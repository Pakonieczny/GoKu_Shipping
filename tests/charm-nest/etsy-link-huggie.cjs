// "This listing also has a Huggie option so what I'm asking for is only the Necklace option" (DOVEHUG, 13 Oct 2026; Paul, 18:09 UTC, Etsy listing 1816178147, card BEADY DOVE 4), OFFLINE:
// the optional rule `leaveHuggie` of the op listingEtsySkuWrite (netlify/functions/_charmNestEtsyLink.js), on the in-memory Firestore and Storage (tests/charm-nest/_sandboxFakes.cjs) and a
// FAKE ETSY (the same rules as etsy-link-families.cjs: it escapes the text it serves, wants decimal prices, refuses a partial *_on_property set, refuses a SKU that varies over a property its
// sku_on_property does not name, gives every product a new id on each PUT, records every request). NO token, NO network: this file cannot reach Etsy at all.
//   REAL (read-only Etsy reads of 13 Oct 2026): the dove and the lion each have 12 products and ONE SKU on all of them (DOVE 4, CUTE LION) before the plain press; the dove's order lines name the options Metal Choice (514)
//   and Charm Type (513); Paul himself says the dove listing has a Huggie option. STAND-INS (marked): the grid itself (tests/charm-nest/etsy-link-families-data.json: 4 metals x { Necklace CHARM, Huggie CHARM SET,
//   CHARM + Engraving }, its option ids 5000... and all of the lion's ids are made up; the other ten dove ids are unproven), prices, stock, readiness, the *_on_property lists (the real ones are unread; every variant is
//   run), the made-up old SKUs of the "different old SKU" case, and whether the LION has any Huggie product at all (never seen: both cases are run).
// It proves: with the rule the 8 Necklace / Engraving products carry the key and the 4 Huggie products carry their OLD SKU (first safety copy per product, else the entry's stated old SKU,
// never anything else; a product with neither is left alone and reported); exactly ONE PUT; only `sku` changes (and sku_on_property is widened ONLY when the planned SKUs differ where the listing
// declares no variation: Etsy's own rule); a NEW safety copy that never overwrites the first; a second press is "already"; a listing without Huggie values is written exactly as without the rule;
// every request without the flag is exactly what it was.
//   node tests/charm-nest/etsy-link-huggie.cjs
'use strict';
const assert = require('assert'), path = require('path');
const fnDir = path.join(__dirname, '../../netlify/functions');
process.env.SHOP_ID = '987654'; process.env.CLIENT_ID = 'test-key'; process.env.CLIENT_SECRET = 'test-secret'; delete process.env.EDIT_PASSCODE;
const Fk = require('./_sandboxFakes.cjs');
const { store, blobs, writes, fileWrites, db, bucket } = Fk;
const FV = Fk.admin.firestore.FieldValue;
const Mod = require(path.join(fnDir, '_charmNestEtsyLink.js'));
require(path.join(__dirname, '../../charm-nest-etsy-link.js')); const Page = globalThis.CharmNestEtsyLink;
const DATA = require('./etsy-link-families-data.json');
const clone = x => JSON.parse(JSON.stringify(x));

/* ═══ the fake Etsy (as in etsy-link-families.cjs) ═══ */
const esc = s => String(s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;').replace(/'/g, '&#39;');
const unesc = s => String(s).replace(/&quot;/g, '"').replace(/&#39;/g, "'").replace(/&lt;/g, '<').replace(/&gt;/g, '>').replace(/&amp;/g, '&');
const money = n => ({ amount: Math.round(n * 100), divisor: 100, currency_code: 'USD' });
const etsyErr = (status, error) => Object.assign(new Error('Etsy API ' + status + ': ' + (error || '')), { status, data: { error: error || 'HTTP ' + status } });
const propOrder = products => { const o = []; for (const p of products) for (const v of p.property_values) if (!o.includes(v.property_id)) o.push(v.property_id); return o; };
const EVERY = [];
function sim(LID, inventory, o) {
  o = o || {};
  const S = { stored: clone(inventory), calls: [], puts: [], seq: 900000000000, lastGet: null, rejected: [] };
  const serve = () => ({ count: S.stored.products.length,
    products: S.stored.products.map(p => ({ product_id: p.product_id, sku: esc(p.sku), is_deleted: !!p.is_deleted,
      property_values: p.property_values.map(v => Object.assign({ property_id: v.property_id }, v.property_name == null ? {} : { property_name: esc(v.property_name) }, { scale_id: v.scale_id, value_ids: v.value_ids.slice(), values: v.values.map(esc) })),
      offerings: p.offerings.map(f => Object.assign({ offering_id: f.offering_id, price: money(f.price), quantity: f.quantity, is_enabled: f.is_enabled, is_deleted: false }, f.readiness_state_id != null ? { readiness_state_id: f.readiness_state_id } : {})) })),
    price_on_property: S.stored.price_on_property.slice(), quantity_on_property: S.stored.quantity_on_property.slice(), sku_on_property: S.stored.sku_on_property.slice() });
  function applyPut(b) {
    const bad = m => { S.rejected.push(m); throw etsyErr(400, m); };
    if (!b || !Array.isArray(b.products) || !b.products.length) bad('products required');
    for (const p of b.products) {
      if (typeof p.sku !== 'string' || p.sku.length > 32) bad('sku must be a string of at most 32 characters');
      if (!Array.isArray(p.offerings) || p.offerings.length !== 1) bad('exactly one offering per product');
      const f = p.offerings[0];
      if (typeof f.price !== 'number' || !(f.price >= 0.2)) bad('price must be a decimal number');
      if (!Number.isInteger(f.quantity) || f.quantity < 0) bad('quantity must be a whole number');
      if (typeof f.is_enabled !== 'boolean') bad('is_enabled must be a boolean');
    }
    const total = propOrder(b.products).length, lists = ['price_on_property', 'quantity_on_property', 'sku_on_property'];
    if (total && lists.some(f => (b[f] || []).length === total)) for (const f of lists) { const n = (b[f] || []).length; if (n > 0 && n < total) bad(f + ': unsupported number of property IDs. Supports only zero or all ' + total + ' variation properties'); }
    for (const [f, key] of [['price_on_property', 'price'], ['sku_on_property', 'sku']]) {
      const decl = new Set((b[f] || []).map(Number)), seen = new Map();
      for (const p of b.products) {
        const g = p.property_values.filter(v => decl.has(v.property_id)).map(v => v.property_id + ':' + v.values.join('/')).sort().join('|'), val = key === 'sku' ? p.sku : p.offerings[0].price;
        if (seen.has(g) && seen.get(g) !== val) bad(f + ' does not cover how ' + key + ' varies');
        seen.set(g, val);
      }
    }
    S.stored = { products: b.products.map((p, i) => { const f = p.offerings[0]; return { product_id: ++S.seq, sku: o.skuTransform ? o.skuTransform(p.sku, i) : p.sku, is_deleted: false,
      property_values: p.property_values.map(v => Object.assign({ property_id: v.property_id }, v.property_name == null ? {} : { property_name: v.property_name }, { scale_id: v.scale_id, value_ids: v.value_ids.slice(), values: v.values.slice() })),
      offerings: [Object.assign({ offering_id: ++S.seq, price: f.price, quantity: f.quantity, is_enabled: f.is_enabled }, f.readiness_state_id != null ? { readiness_state_id: f.readiness_state_id } : {})] }; }),
      price_on_property: b.price_on_property.slice(), quantity_on_property: b.quantity_on_property.slice(), sku_on_property: b.sku_on_property.slice() };
  }
  async function handle(method, p, q, b) {
    const call = { method, path: p, query: q, body: b === undefined ? undefined : clone(b) };
    S.calls.push(call); EVERY.push(call); if (method === 'PUT') S.puts.push(call);
    if (o.fail) { const e = o.fail(call); if (e) throw e; }
    const m = /^\/listings\/(\d+)(\/inventory)?$/.exec(p);
    if (!m || m[1] !== LID) throw etsyErr(404, 'no such listing');
    if (!m[2] && method === 'GET') return { listing_id: +m[1], state: 'active' };
    if (m[2] && method === 'GET') { S.lastGet = serve(); return clone(S.lastGet); }
    if (m[2] && method === 'PUT') { assert.deepStrictEqual(q, { legacy: false }); applyPut(b); return serve(); }
    throw etsyErr(405, 'unexpected request');
  }
  return { S, api: { get: (p, q) => handle('GET', p, q), put: (p, q, b) => handle('PUT', p, q, b) } };
}

/* ═══ the dove: the REAL 12 products (ids, option names and texts), stand-in offerings ═══ */
const KEY = 'BEADY DOVE 4', OLD = 'DOVE 4', LID = '1816178147';
const D = DATA.dove, LION = DATA.lion;
const CFG = { dove: { lid: '1816178147', key: 'BEADY DOVE 4', old: 'DOVE 4', data: D, slug: 'BEADY_DOVE_4' }, lion: { lid: '1801980518', key: 'BEADY CUTE LION', old: 'CUTE LION', data: LION, slug: 'BEADY_CUTE_LION' } };
const isHug = p => p.property_values.some(v => v.values.some(x => /huggie/i.test(x)));
function dove(o) {   // the listing as Etsy holds it: every product carries `sku(p, i)`; the lists are STAND-INS (the real ones are unread)
  o = o || {}; const G = o.cfg || CFG.dove, DD = G.data;
  const inv = { products: DD.products.map((p, i) => ({ product_id: p.product_id, sku: o.sku ? o.sku(p, i) : G.old, is_deleted: false,
      property_values: p.property_values.map(v => ({ property_id: v.property_id, property_name: v.property_name, scale_id: v.scale_id, value_ids: v.value_ids.slice(), values: v.values.slice() })),
      offerings: [{ offering_id: p.product_id * 10, price: o.flat ? 30 : o.priceByMetal ? 28 + ['Sterling Silver', 'Gold Filled', 'Rose Gold Filled', '14k Solid Gold'].indexOf(p.property_values[0].values[0]) * 4 : 28 + (i % 4) * 4 + (i % 7), quantity: i === 5 ? 0 : 999, is_enabled: i !== 5, readiness_state_id: 7001 }] })),   // STAND-INS: prices (per product, or per metal when the listing declares price over the metal only), stock, one disabled product
    price_on_property: (o.price || [514, 513]).slice(), quantity_on_property: (o.qty || []).slice(), sku_on_property: (o.skuOn || []).slice() };
  if (o.drop) inv.products = inv.products.filter((p, i) => !o.drop.includes(i));
  if (o.noHuggie) inv.products.forEach(p => p.property_values.forEach(v => { v.values = v.values.map(x => (/huggie/i.test(x) ? 'Stud earrings SET' : x)); }));   // (a lion whose third Charm Type is not a huggie: no value names one)
  return inv;
}
let tick = 0, maxCalls = 0;
const masterEntry = sku => ({ sku, masterHash: 'm'.repeat(8), masterName: 'MASTER SKU_LIVING_MV_2026-0914.ai', widthPt: 33.29, heightPt: 25.74, holes: 1, engravable: true, aiPath: 'charmnest/master/' + sku.replace(/\W+/g, '_') + '.ai' });
function world(inv, o) {
  o = o || {}; const G = o.cfg || CFG.dove, LID = G.lid, KEY = G.key, OLD = G.old;
  store.clear(); blobs.clear(); writes.length = 0; fileWrites.length = 0;
  const E = sim(LID, inv, o.sim);
  store.set('Charm_Master_Index/' + KEY, masterEntry(KEY)); store.set('Charm_Master_Index/' + OLD, masterEntry(OLD));
  if (o.mirror !== false) store.set('EtsyMail_Listings/' + LID, { state: 'active', title: 'a listing' });
  const mod = Mod.make({ db, FV, bucket, etsy: E.api, meter: { bumpSimple() {}, flushNow: async () => {} }, now: () => 1760000000000 + (++tick), timeoutMs: o.timeoutMs });
  const W = { E, S: E.S, mod, rec: () => store.get('Charm_Custom_Saves/link-' + LID), link: () => (store.get('Charm_Custom_Saves/link-' + LID) || {}).link,
    run: async body => { const n0 = E.S.calls.length, r = await mod.listingEtsySkuWrite(body); const n = E.S.calls.length - n0; maxCalls = Math.max(maxCalls, n); assert.strictEqual(n, r.calls, 'the answer counts exactly the Etsy calls made'); return r; },
    plain: () => W.run({ listingId: LID, sku: KEY, by: 'Paul' }),                                    // the press as it always was (what the one-press run sent at 04:06 UTC)
    rule: old => W.run(Object.assign({ listingId: LID, sku: KEY, by: 'Paul', leaveHuggie: true }, old === undefined ? { huggieOldSku: OLD } : old ? { huggieOldSku: old } : {})) };
  return W;
}
const skus = W => W.S.stored.products.map(p => p.sku);
const bySku = (W, f) => W.S.stored.products.filter(f).map(p => p.sku);
const noSku = b => { const c = clone(b); c.products.forEach(p => delete p.sku); delete c.sku_on_property; return c; };
const readAsPut = get => ({ products: get.products.map(p => { const f = p.offerings[0]; return { sku: '', property_values: p.property_values.map(v => Object.assign({ property_id: v.property_id }, v.property_name == null ? {} : { property_name: unesc(v.property_name) }, { scale_id: v.scale_id, value_ids: v.value_ids, values: v.values.map(unesc) })),
    offerings: [Object.assign({ price: f.price.amount / f.price.divisor, quantity: f.is_enabled ? f.quantity : 0, is_enabled: f.is_enabled }, f.readiness_state_id != null ? { readiness_state_id: f.readiness_state_id } : {})] }; }),
  price_on_property: get.price_on_property, quantity_on_property: get.quantity_on_property, sku_on_property: get.sku_on_property });
const T = [], test = (name, fn) => T.push([name, fn]);

/* ═══ 0 · what the 12 products are ═══ */
test('the STAND-IN dove grid (12 products in the family\'s shape): Metal Choice (514) x Charm Type (513) = 4 metals x {Necklace CHARM, Huggie CHARM SET, CHARM + Engraving}; exactly 4 name a Huggie', () => {
  assert.strictEqual(D.listingId, LID); assert.strictEqual(D.products.length, 12);
  const metals = [...new Set(D.products.map(p => p.property_values.find(v => v.property_id === 514).values[0]))], types = [...new Set(D.products.map(p => p.property_values.find(v => v.property_id === 513).values[0]))];
  assert.deepStrictEqual(metals.sort(), ['14k Solid Gold', 'Gold Filled', 'Rose Gold Filled', 'Sterling Silver']); assert.deepStrictEqual(types.sort(), ['CHARM + Engraving', 'Huggie CHARM SET', 'Necklace CHARM']);
  assert.strictEqual(new Set(D.products.map(p => p.property_values.map(v => v.values[0]).join('|'))).size, 12, 'one product per metal x type');
  assert.deepStrictEqual(D.products.filter(isHug).map(p => p.property_values.find(v => v.property_id === 513).values[0]), Array(4).fill('Huggie CHARM SET'));
});

/* ═══ 1 · the real situation: the plain press of 04:06 UTC put the key on all 12; now the rule ═══ */
for (const [label, skuOn] of [['no variation declared (one SKU everywhere: sku_on_property [])', []], ['SKU declared over the metal only (sku_on_property [514])', [514]], ['SKU declared over both options (sku_on_property [514, 513])', [514, 513]]]) {
  test('after the plain press (all 12 carry BEADY DOVE 4), the rule puts the 4 Huggie products back to DOVE 4 and keeps the key on the 8 others: ONE PUT, only `sku` changes, a NEW safety copy; ' + label, async () => {
    const W = world(dove({ skuOn }));
    const first = await W.plain();   // the real op, the plain request: exactly the 04:06 press
    assert.strictEqual(first.ok, true, JSON.stringify(first)); assert.strictEqual(first.calls, 2); assert.deepStrictEqual(new Set(skus(W)), new Set([KEY]), 'all 12 carry the key (the incident)'); assert(!('huggie' in first));
    const firstCopy = first.beforePath; assert.strictEqual(firstCopy, 'charmnest/custom-saves/' + LID + '/link-BEADY_DOVE_4-before.json');
    const firstBytes = blobs.get(firstCopy).buf.toString('utf8'); assert(JSON.parse(firstBytes).products.every(p => p.sku === OLD), 'the first copy is the listing as it was: every product DOVE 4');
    const n0 = W.S.calls.length, puts0 = W.S.puts.length, before = clone(W.S.stored), files0 = fileWrites.length;
    let atPut = null; const realPut = W.E.api.put; W.E.api.put = async (p, q, b) => { const l = W.link(); atPut = { state: l.state, path: l.beforePath, there: !!(l.beforePath && blobs.has(l.beforePath)), saves: fileWrites.length - files0 }; return realPut(p, q, b); };
    const r = await W.rule();
    assert.strictEqual(r.ok, true, JSON.stringify(r)); assert.strictEqual(r.state, 'linked'); assert.strictEqual(r.products, 12); assert.strictEqual(r.verified, true); assert.strictEqual(r.calls, 2, 'inventory + PUT (the mirror knows the state)');
    assert(!r.already, 'it wrote'); assert.deepStrictEqual(r.huggie, { products: 4, keyed: 8, restored: [{ sku: OLD, n: 4 }], left: 0 });
    assert.strictEqual(W.S.calls.length - n0, 2); assert.strictEqual(W.S.puts.length - puts0, 1, 'exactly ONE PUT'); assert(W.S.calls.slice(n0).every(c => /^\/listings\/1816178147\/inventory$/.test(c.path)), 'only this listing\'s inventory');
    // who carries what, product by product (the real option texts)
    const now = W.S.stored.products;
    assert.deepStrictEqual(now.filter(p => !isHug(p)).map(p => p.sku), Array(8).fill(KEY), 'the 8 Necklace CHARM and CHARM + Engraving products carry BEADY DOVE 4');
    assert.deepStrictEqual(now.filter(isHug).map(p => p.sku), Array(4).fill(OLD), 'the 4 Huggie CHARM SET products are back on DOVE 4');
    assert.deepStrictEqual(now.map(p => p.property_values.map(v => v.values[0]).join('|')), before.products.map(p => p.property_values.map(v => v.values[0]).join('|')), 'same products in the same order');
    // only `sku` changed (and the SKU declaration only when Etsy needs it)
    const put = W.S.puts[puts0].body, get = W.S.lastGet;   // (lastGet: the read of THIS run)
    assert.deepStrictEqual(noSku(put), noSku(readAsPut(get)), 'the PUT body equals the read with sku removed from both');
    assert.deepStrictEqual(put.products.map(p => p.sku), get.products.map((p, i) => (isHug(p) ? OLD : KEY)), 'each product\'s sku in the body');
    const all = [514, 513];
    assert.deepStrictEqual(put.sku_on_property.slice().sort(), all.slice().sort(), 'sku_on_property: every property when the SKUs differ over Charm Type (declared [] / [514] / [514,513] -> [514,513])');
    assert.strictEqual(W.S.rejected.length, 0, 'the fake Etsy refused nothing');
    assert.deepStrictEqual(stripIds(W.S.stored).products.map(p => ({ pv: p.property_values, o: p.offerings })), stripIds(before).products.map(p => ({ pv: p.property_values, o: p.offerings })), 'Etsy holds the same options, prices, stock, enabled state and readiness');
    // the safety copies: the first untouched, a NEW one written and noted BEFORE the PUT
    assert(atPut && atPut.state === 'writing' && atPut.there && atPut.saves === 1, 'the new copy was saved and noted on the record before the PUT left: ' + JSON.stringify(atPut));
    assert.strictEqual(r.beforePath, 'charmnest/custom-saves/' + LID + '/link-BEADY_DOVE_4-before-2.json'); assert.strictEqual(blobs.get(firstCopy).buf.toString('utf8'), firstBytes, 'the FIRST copy is byte for byte what it was');
    assert.deepStrictEqual(fileWrites.slice(files0).map(f => f.path), [r.beforePath], 'one file written, the new copy'); assert.deepStrictEqual(JSON.parse(blobs.get(r.beforePath).buf.toString('utf8')), get, 'the new copy is this run\'s read, whole');
    const link = W.link(); assert.strictEqual(link.state, 'linked'); assert.strictEqual(link.leaveHuggie, true); assert.deepStrictEqual(link.huggie, r.huggie); assert.deepStrictEqual(link.backups, [firstCopy, r.beforePath]); assert(!('runningUntil' in link));
    // the SKU table the page reads shows the new state (not one key everywhere)
    const tab = store.get('Charm_Listing_Skus/' + LID); assert(tab && tab.uni === undefined, 'no single SKU any more: ' + JSON.stringify(tab).slice(0, 160));
    // the words
    assert.strictEqual(Page.wording(r, LID, KEY).text, 'Done: BEADY DOVE 4 is on the 8 products of listing 1816178147 that are not Huggie. 4 Huggie products put back to DOVE 4. The listing as it was is kept.');
    // a second press with the rule: "already", answered from the record, no Etsy request, no write
    const n1 = W.S.calls.length, w1 = writes.length, again = await W.rule();
    assert.strictEqual(again.ok, true); assert.strictEqual(again.calls, 0); assert.strictEqual(W.S.calls.length, n1); assert.strictEqual(writes.length, w1); assert.strictEqual(fileWrites.length, files0 + 1); assert.deepStrictEqual(again.huggie, r.huggie);
    assert.strictEqual(Page.wording(Object.assign({}, again, { already: true }), LID, KEY).text, 'Already there: the 8 products of listing 1816178147 that are not Huggie carry BEADY DOVE 4, and the 4 Huggie products do not. Nothing was changed.');
    // ... and so does a PLAIN press (it can never put the key back on the Huggie products): the record answers, no Etsy request
    const plain = await W.plain(); assert.strictEqual(plain.ok, true); assert.strictEqual(plain.calls, 0); assert.deepStrictEqual(plain.huggie, r.huggie); assert.strictEqual(W.S.puts.length, puts0 + 1); assert.deepStrictEqual(bySku(W, isHug), Array(4).fill(OLD));
    assert.match(Page.wording(plain, LID, KEY).text, /that are not Huggie/);
  });
}
const stripIds = inv => { const c = clone(inv); for (const p of c.products) { delete p.product_id; for (const f of p.offerings) delete f.offering_id; } return c; };

test('"Already there" with no record: the listing is read (1 request: the inventory), nothing is written, and the record it leaves answers the next press with 0 requests', async () => {
  const W = world(dove({ sku: p => (isHug(p) ? OLD : KEY), skuOn: [514, 513] }));   // somebody set it by hand: the 8 carry the key, the 4 their old SKU
  const r = await W.rule();
  assert.strictEqual(r.ok, true, JSON.stringify(r)); assert.strictEqual(r.already, true); assert.strictEqual(r.calls, 1); assert.strictEqual(W.S.puts.length, 0, 'no PUT'); assert.strictEqual(fileWrites.length, 0, 'no safety copy'); assert.deepStrictEqual(r.huggie, { products: 4, keyed: 8, restored: [], left: 0 });
  assert.strictEqual(W.link().leaveHuggie, true);
  const again = await W.rule(); assert.strictEqual(again.calls, 0); assert.strictEqual(W.S.calls.length, 1);
  assert.strictEqual(Page.wording(r, LID, KEY).text, 'Already there: the 8 products of listing 1816178147 that are not Huggie carry BEADY DOVE 4, and the 4 Huggie products do not. Nothing was changed.');
});
test('a rule press on a listing a plain press already did, whose record says linked, READS the listing (the record is not trusted) - even when that earlier record used up its three tries', async () => {
  const W = world(dove({ skuOn: [514, 513] })); await W.plain();
  const rec = W.rec(); rec.link.attempts = 3; store.set('Charm_Custom_Saves/link-' + LID, rec);   // (a long history of tries)
  const r = await W.rule(); assert.strictEqual(r.ok, true, JSON.stringify(r)); assert.strictEqual(r.calls, 2); assert.strictEqual(W.S.puts.length, 2); assert.deepStrictEqual(bySku(W, isHug), Array(4).fill(OLD));
});

/* ═══ 2 · which old SKU a Huggie product goes back to ═══ */
test('a Huggie product whose first safety copy holds a DIFFERENT old SKU goes back to THAT value; the others to the entry\'s own stated SKU or their own old value', async () => {
  const OLDS = { 'Sterling Silver': 'DOVE 4 SS', 'Gold Filled': 'DOVE 4', 'Rose Gold Filled': 'DOVE 4 ROSE', '14k Solid Gold': OLD };   // MADE-UP old SKUs of the Huggie products per metal
  const W = world(dove({ skuOn: [514, 513], sku: p => (isHug(p) ? OLDS[p.property_values.find(v => v.property_id === 514).values[0]] : OLD) }));
  for (const k of ['DOVE 4 SS', 'DOVE 4 ROSE']) store.set('Charm_Master_Index/' + k, masterEntry(k));
  const first = await W.plain(); assert.strictEqual(first.ok, true, JSON.stringify(first)); assert.deepStrictEqual(new Set(skus(W)), new Set([KEY]));
  const r = await W.rule('DOVE 4 STATED'); assert.strictEqual(r.ok, true, JSON.stringify(r));
  assert.deepStrictEqual(W.S.stored.products.filter(isHug).map(p => [p.property_values.find(v => v.property_id === 514).values[0], p.sku]).sort(), Object.entries(OLDS).sort(), 'each Huggie product is back on ITS old SKU from the first copy; the stated SKU was used for none');
  assert.deepStrictEqual(r.huggie.restored, [{ sku: 'DOVE 4 ROSE', n: 1 }, { sku: 'DOVE 4 SS', n: 1 }, { sku: OLD, n: 2 }].sort((a, b) => b.n - a.n || (a.sku < b.sku ? -1 : 1))); assert.strictEqual(r.huggie.left, 0);
  assert.deepStrictEqual(bySku(W, p => !isHug(p)), Array(8).fill(KEY));
});
test('no first safety copy at all: the entry\'s own stated old SKU is used (never another value); a Huggie product with neither is LEFT ALONE and reported', async () => {
  // stated old SKU only
  let W = world(dove({ sku: () => KEY, skuOn: [514, 513] }));   // everything carries the key; no record, no copy (e.g. set on Etsy by hand)
  let r = await W.rule(); assert.strictEqual(r.ok, true, JSON.stringify(r)); assert.deepStrictEqual(bySku(W, isHug), Array(4).fill(OLD), 'back to the STATED old SKU'); assert.deepStrictEqual(r.huggie, { products: 4, keyed: 8, restored: [{ sku: OLD, n: 4 }], left: 0 });
  // neither: nothing is invented
  W = world(dove({ sku: () => KEY, skuOn: [514, 513] }));
  r = await W.rule(''); assert.strictEqual(r.ok, false, JSON.stringify(r)); assert.strictEqual(r.reason, 'huggie-unknown'); assert.strictEqual(r.calls, 1); assert.strictEqual(W.S.puts.length, 0, 'no PUT'); assert.strictEqual(fileWrites.length, 0, 'no safety copy: nothing was going to change');
  assert.deepStrictEqual(new Set(skus(W)), new Set([KEY]), 'every product exactly as it was'); assert.match(r.text, /no earlier SKU is known/); assert.match(r.details[0], /4 Huggie products carry BEADY DOVE 4/);
  assert.match(Page.wording(r, LID, KEY).text, /^Huggie products carry the SKU and no earlier SKU is known for them/);
  // part known, part not: the known ones go back, the other one stays and is reported (the first copy lacks one product)
  W = world(dove({ skuOn: [514, 513] })); const first = await W.plain(); assert.strictEqual(first.ok, true);
  const copy = JSON.parse(blobs.get(first.beforePath).buf.toString('utf8')); copy.products = copy.products.filter(p => !(isHug(p) && p.property_values.some(v => v.values[0] === 'Rose Gold Filled'))); blobs.get(first.beforePath).buf = Buffer.from(JSON.stringify(copy));
  r = await W.rule(''); assert.strictEqual(r.ok, true, JSON.stringify(r)); assert.deepStrictEqual(r.huggie, { products: 4, keyed: 8, restored: [{ sku: OLD, n: 3 }], left: 1 });
  assert.deepStrictEqual(W.S.stored.products.filter(isHug).map(p => [p.property_values.find(v => v.property_id === 514).values[0], p.sku]).sort(), [['14k Solid Gold', OLD], ['Gold Filled', OLD], ['Rose Gold Filled', KEY], ['Sterling Silver', OLD]], 'the one with nothing known is left exactly as it was');
  assert.match(Page.wording(r, LID, KEY).text, /1 Huggie product still carries BEADY DOVE 4: no earlier SKU is known for it, so it was left alone/);
  // a second press: the record answers; the left one is still reported
  const again = await W.rule(''); assert.strictEqual(again.calls, 0); assert.strictEqual(again.huggie.left, 1);
});
test('a Huggie product that does NOT carry the key is never touched (whatever it carries); a copy that says the product carried the KEY is not an old SKU', async () => {
  const W = world(dove({ skuOn: [514, 513], sku: p => (isHug(p) ? OLD : 'DOVE 4 NECK') }));
  store.set('Charm_Master_Index/DOVE 4 NECK', masterEntry('DOVE 4 NECK'));
  let r = await W.rule(); assert.strictEqual(r.ok, true, JSON.stringify(r)); assert.deepStrictEqual(bySku(W, isHug), Array(4).fill(OLD), 'untouched'); assert.deepStrictEqual(bySku(W, p => !isHug(p)), Array(8).fill(KEY)); assert.deepStrictEqual(r.huggie, { products: 4, keyed: 8, restored: [], left: 0 });
  // a first copy that holds the KEY for a Huggie product (it carried the key before our first write): the stated SKU is used, not the key
  const W2 = world(dove({ skuOn: [514, 513], sku: p => KEY })); const f = await W2.plain(); assert.strictEqual(f.already, true, 'every product carried the key: nothing to write for the plain press');
  r = await W2.rule(); assert.strictEqual(r.ok, true, JSON.stringify(r)); assert.deepStrictEqual(bySku(W2, isHug), Array(4).fill(OLD));
});

/* ═══ 3 · a listing WITHOUT Huggie values behaves exactly as without the rule ═══ */
test('a listing with no Huggie value: the rule on and the rule off send the SAME PUT, give the same record and files; the answer has no `huggie`', async () => {
  const NOH = () => dove({ skuOn: [], sku: () => 'OTHER 1', drop: [1, 4, 7, 10] });   // 8 products, none names a Huggie (the stand-in drops the 4 Huggie products)
  const A = world(NOH()), a = await A.plain(); const Ab = A.S.puts.map(p => p.body), Ar = clone(A.rec().link), Af = fileWrites.map(f => f.path);
  const B = world(NOH()), b = await B.rule(); const Bb = B.S.puts.map(p => p.body), Br = clone(B.rec().link), Bf = fileWrites.map(f => f.path);
  assert.strictEqual(a.ok, true); assert.strictEqual(b.ok, true, JSON.stringify(b)); assert.deepStrictEqual(Bb, Ab, 'the same PUT body'); assert.deepStrictEqual(Bf, Af, 'the same files'); assert(!('huggie' in b), 'no `huggie` in the answer');
  const strip = o => { const c = clone(o); delete c.leaveHuggie; delete c.at; return c; };
  assert.deepStrictEqual(strip(Br), strip(Ar), 'the same record, apart from the rule\'s own mark'); assert.deepStrictEqual(Object.assign({}, b), Object.assign({}, a), 'the same answer');
  assert.deepStrictEqual(B.S.puts[0].body.products.map(p => p.sku), Array(8).fill(KEY)); assert.deepStrictEqual(B.S.puts[0].body.sku_on_property, [], 'no widening when every SKU is the same');
});
test('a listing with no Huggie value, whatever lists it declares (none, a partial SKU list beside a full price list, full, nothing varying): the rule sends the SAME PUT as the plain press', async () => {
  for (const [skuOn, price, flat] of [[[], [514, 513]], [[514], [514, 513]], [[513], [514, 513]], [[514, 513], [514, 513]], [[], []]]) {
    const mk = () => dove({ skuOn, price, flat: price.length === 0, sku: () => 'OTHER 1', drop: [1, 4, 7, 10] });
    const A = world(mk()), a = await A.plain(), B = world(mk()), b = await B.rule();
    assert.strictEqual(a.ok, true, JSON.stringify([skuOn, price, a])); assert.strictEqual(b.ok, true, JSON.stringify([skuOn, price, b])); assert.deepStrictEqual(B.S.puts.map(x => x.body), A.S.puts.map(x => x.body), 'same PUT for lists ' + JSON.stringify([skuOn, price])); assert.deepStrictEqual(b, a);
  }
});
test('a listing whose Huggie value is spelled in other ways: Huggie / huggies / HUGGY in any case count; "Huggins" and "Hug" do not', async () => {
  const inv = dove({ skuOn: [514, 513], sku: () => KEY }); const names = ['HUGGIE hoops', 'the huggies set', 'Huggy Charm', 'Huggins Charm'];
  inv.products.slice(0, 4).forEach((p, i) => { p.property_values[1].values = [names[i]]; p.property_values[1].value_ids = [8000 + i]; });
  const W = world(inv); const r = await W.rule(); assert.strictEqual(r.ok, true, JSON.stringify(r));
  assert.strictEqual(r.huggie.products, 6, '3 spelled-out Huggie values among the first four ("Huggins" is none) plus the 3 real Huggie CHARM SET products among the others');
  assert.strictEqual(W.S.stored.products[3].sku, KEY, '"Huggins Charm" is not a huggie: it keeps the key'); assert.strictEqual(W.S.stored.products[0].sku, OLD);
});
test('only Huggie products on the listing: refused, nothing sent', async () => {
  const W = world(dove({ skuOn: [514, 513], sku: () => KEY, drop: [0, 2, 3, 5, 6, 8, 9, 11] })); const r = await W.rule();
  assert.strictEqual(r.ok, false); assert.strictEqual(r.reason, 'only-huggie'); assert.strictEqual(W.S.puts.length, 0); assert.strictEqual(fileWrites.length, 0); assert.strictEqual(r.calls, 1);
});

/* ═══ 4 · the fresh-listing run (nothing was ever written): the Huggie products are not touched at all ═══ */
test('a listing no press ever touched (all 12 on DOVE 4): the rule puts the key on the 8 and leaves the 4 Huggie products on DOVE 4; the first copy is the original', async () => {
  const W = world(dove({ skuOn: [] })); const r = await W.rule();
  assert.strictEqual(r.ok, true, JSON.stringify(r)); assert.strictEqual(W.S.puts.length, 1); assert.deepStrictEqual(bySku(W, p => !isHug(p)), Array(8).fill(KEY)); assert.deepStrictEqual(bySku(W, isHug), Array(4).fill(OLD));
  assert.deepStrictEqual(r.huggie, { products: 4, keyed: 8, restored: [], left: 0 }); assert.strictEqual(r.beforePath, 'charmnest/custom-saves/' + LID + '/link-BEADY_DOVE_4-before.json');
  assert.strictEqual(Page.wording(r, LID, KEY).text, 'Done: BEADY DOVE 4 is on the 8 products of listing 1816178147 that are not Huggie. The 4 Huggie products were left alone. The listing as it was is kept.');
});

/* ═══ 5 · the stops stay ═══ */
test('Etsy keeps ONE Huggie product on the key: verify failure, ONE PUT, the new safety copy kept, no undo, never sent again unchanged', async () => {
  const W = world(dove({ skuOn: [514, 513] }), { sim: { skuTransform: (s, i) => (s === OLD && i === 10 ? KEY : s) } });
  await W.plain(); const puts0 = W.S.puts.length;
  const r = await W.rule(); assert.strictEqual(r.ok, false); assert.strictEqual(r.reason, 'verify'); assert(r.details.some(x => /SKU/.test(x)), JSON.stringify(r.details)); assert.strictEqual(W.S.puts.length - puts0, 1); assert(blobs.has(r.beforePath));
  const n0 = W.S.calls.length, again = await W.rule(); assert.strictEqual(again.reason, 'verify'); assert.strictEqual(again.calls, 0); assert.strictEqual(W.S.calls.length, n0);
});
test('Etsy refuses the PUT (403 / 400): nothing changes, ONE PUT, no loop; the 403 says it needs listings_w; the second try after re-authorising writes once', async () => {
  let deny = true;
  const W = world(dove({ skuOn: [514, 513] }), { sim: { fail: c => (c.method === 'PUT' && deny && W.S.puts.length >= 2 ? etsyErr(403, 'no') : null) } });
  await W.plain(); const before = clone(W.S.stored);
  let r = await W.rule(); assert.strictEqual(r.ok, false); assert.strictEqual(r.reason, 'scope'); assert.strictEqual(r.needs, 'listings_w'); assert.strictEqual(W.S.puts.length, 2, 'one PUT for the rule run'); assert.deepStrictEqual(W.S.stored, before, 'nothing changed on Etsy');
  deny = false; r = await W.rule(); assert.strictEqual(r.ok, true, JSON.stringify(r)); assert.deepStrictEqual(bySku(W, isHug), Array(4).fill(OLD));
});
test('the PUT landed but Etsy did not answer in time (504): "unverified"; the next press reads the listing, sees the planned SKUs, checks it against the new safety copy and is linked - NO second PUT', async () => {
  let cut = true;
  const W = world(dove({ skuOn: [514, 513] }), { sim: { fail: c => null } });
  await W.plain();
  const realPut = W.E.api.put; W.E.api.put = async (p, q, b) => { const out = await realPut(p, q, b); if (cut) { cut = false; throw etsyErr(504, 'gateway timeout'); } return out; };
  const puts0 = W.S.puts.length; let r = await W.rule(); assert.strictEqual(r.ok, false); assert.strictEqual(r.state, 'unverified', JSON.stringify(r)); assert.strictEqual(W.S.puts.length - puts0, 1);
  const again = await W.rule(); assert.strictEqual(again.ok, true, JSON.stringify(again)); assert.strictEqual(again.verified, true); assert.strictEqual(W.S.puts.length - puts0, 1, 'no second PUT'); assert.deepStrictEqual(again.huggie, { products: 4, keyed: 8, restored: [{ sku: OLD, n: 4 }], left: 0 });
  assert.deepStrictEqual(bySku(W, isHug), Array(4).fill(OLD));
});
test('the request is checked before anything is read: a bad stated old SKU is refused with 0 Etsy requests; `leaveHuggie` must be exactly true; a stated SKU without the rule is ignored', async () => {
  const W = world(dove({ skuOn: [514, 513], sku: () => KEY }));
  for (const old of ['DOVE  4', 'DOVE 4 ', 'x'.repeat(33), KEY, 'DOVE–4']) { const r = await W.run({ listingId: LID, sku: KEY, by: 'Paul', leaveHuggie: true, huggieOldSku: old }); assert.strictEqual(r.reason, 'sku-shape', JSON.stringify(old) + JSON.stringify(r)); assert.strictEqual(r.calls, 0); }
  assert.strictEqual(W.S.calls.length, 0); assert.strictEqual(writes.length, 0);
  // "true" (a string) is not the flag: the plain behaviour (everything already carries the key)
  const r1 = await W.run({ listingId: LID, sku: KEY, by: 'Paul', leaveHuggie: 'true', huggieOldSku: OLD }); assert.strictEqual(r1.already, true); assert(!('huggie' in r1)); assert.strictEqual(W.S.puts.length, 0);
  const r2 = await W.run({ listingId: LID, sku: KEY, by: 'Paul', huggieOldSku: OLD }); assert.strictEqual(r2.already, true); assert.strictEqual(W.S.puts.length, 0, 'the Huggie products were not touched by a request without the flag');
  const sb = await W.run({ listingId: LID, sku: KEY, by: 'Paul', leaveHuggie: true, huggieOldSku: OLD, sandbox: true }); assert.strictEqual(sb.reason, 'sandbox'); assert.strictEqual(sb.calls, 0);
});
test('without the flag the op writes the key on EVERY product, Huggie included, exactly as before (the rule is opt-in)', async () => {
  const W = world(dove({ skuOn: [514, 513] })); const r = await W.plain();
  assert.strictEqual(r.ok, true); assert.deepStrictEqual(new Set(skus(W)), new Set([KEY])); assert.strictEqual(W.S.puts.length, 1); assert(!('huggie' in r));
  assert.strictEqual(Page.wording(r, LID, KEY).text, 'Done: BEADY DOVE 4 is now on all 12 products of listing 1816178147.');
});

/* ═══ 6 · Etsy's own rule about SKUs that differ ═══ */
test('sku_on_property: widened to EVERY property only when the planned SKUs differ where the listing declares no variation; price/quantity lists widened with it as Etsy demands; nothing else moves', async () => {
  // declared over Charm Type already: nothing is widened
  let W = world(dove({ skuOn: [513], price: [514], priceByMetal: true })); await W.plain(); let r = await W.rule(); assert.strictEqual(r.ok, true, JSON.stringify(r));
  assert.deepStrictEqual(W.S.puts[1].body.sku_on_property, [513], 'declared over Charm Type: left as it is'); assert.strictEqual(W.S.rejected.length, 0);
  // declared over the metal only, price over the metal only: widened to both, price widened with it
  W = world(dove({ skuOn: [514], price: [514], priceByMetal: true })); await W.plain(); r = await W.rule(); assert.strictEqual(r.ok, true, JSON.stringify(r));
  assert.deepStrictEqual(W.S.puts[1].body.sku_on_property, [514, 513]); assert.deepStrictEqual(W.S.puts[1].body.price_on_property, [514, 513], 'the partial price list is widened with it (Etsy accepts zero or all)'); assert.strictEqual(W.S.rejected.length, 0);
  // nothing declared anywhere (a listing with ONE price and ONE SKU): widened to both for the SKU only, price stays []
  W = world(dove({ skuOn: [], price: [], flat: true })); await W.plain(); r = await W.rule(); assert.strictEqual(r.ok, true, JSON.stringify(r));
  assert.deepStrictEqual(W.S.puts[1].body.sku_on_property, [514, 513]); assert.deepStrictEqual(W.S.puts[1].body.price_on_property, []); assert.deepStrictEqual(W.S.puts[1].body.quantity_on_property, []); assert.strictEqual(W.S.rejected.length, 0);
  assert.deepStrictEqual(bySku(W, isHug), Array(4).fill(OLD));
});

/* ═══ 6b · the LION (1801980518, BEADY CUTE LION, old SKU CUTE LION): the rule is on its entry too; whether it has Huggie products was never seen, so BOTH cases are proven ═══ */
const lionWorld = (o) => world(dove(Object.assign({ cfg: CFG.lion, skuOn: [514, 513] }, o)), { cfg: CFG.lion });
const lionRule = (W, old) => W.run(Object.assign({ listingId: CFG.lion.lid, sku: CFG.lion.key, by: 'Paul', leaveHuggie: true }, old === undefined ? { huggieOldSku: CFG.lion.old } : old ? { huggieOldSku: old } : {}));
const lionPlain = W => W.run({ listingId: CFG.lion.lid, sku: CFG.lion.key, by: 'Paul' });
test('LION without any Huggie product: the rule is a STRICT NO-OP after the plain press - one inventory read, no PUT, no safety copy, the plain press\'s own words "Already there", its record kept as it was; the next press answers from the record with no request', async () => {
  const W = lionWorld({ noHuggie: true }); const LL = CFG.lion.lid;
  const first = await lionPlain(W); assert.strictEqual(first.ok, true, JSON.stringify(first)); assert.strictEqual(first.calls, 2);
  assert.deepStrictEqual(new Set(skus(W)), new Set([CFG.lion.key])); const files0 = fileWrites.length, puts0 = W.S.puts.length, stored0 = clone(W.S.stored), rec0 = clone(W.rec().link);
  const todayAgain = await lionPlain(W);   // what the plain op answers today to a second press (the record, no Etsy request)
  assert.strictEqual(todayAgain.calls, 0); const todayWords = Page.wording(Object.assign({}, todayAgain, { already: true }), LL, CFG.lion.key).text; assert.strictEqual(todayWords, 'Already there: all 12 products of listing 1801980518 carry BEADY CUTE LION. Nothing was changed.');
  const n0 = W.S.calls.length, r = await lionRule(W);
  assert.strictEqual(r.ok, true, JSON.stringify(r)); assert.strictEqual(r.already, true, 'answers "already"'); assert.strictEqual(r.calls, 1, 'ONE Etsy request: the inventory'); assert.strictEqual(W.S.calls.length - n0, 1); assert.strictEqual(W.S.calls[n0].method, 'GET');
  assert.strictEqual(W.S.puts.length, puts0, 'no PUT'); assert.strictEqual(fileWrites.length, files0, 'no safety copy'); assert.deepStrictEqual(W.S.stored, stored0, 'Etsy untouched'); assert(!('huggie' in r), 'no `huggie` in the answer');
  assert.strictEqual(r.products, 12); assert.strictEqual(r.beforePath, first.beforePath, 'the first press\'s copy is still the one named');
  assert.strictEqual(Page.wording(r, LL, CFG.lion.key).text, todayWords, 'the very words the batch shows today: "' + todayWords + '"');
  const rec1 = W.rec().link, strip = o => { const c = clone(o); delete c.leaveHuggie; delete c.at; delete c.calls; delete c.runningUntil; delete c.already; return c; };
  assert.strictEqual(rec1.state, 'linked'); assert.strictEqual(rec1.leaveHuggie, true); assert.deepStrictEqual(rec1, Object.assign({}, rec0, { leaveHuggie: true }), 'the record is EXACTLY what the plain press left, plus the rule\'s mark'); void strip; assert.strictEqual(rec1.already, rec0.already, 'its `already` flag is as it was (the single press says what it always said)');
  const n1 = W.S.calls.length, again = await lionRule(W); assert.strictEqual(again.calls, 0); assert.strictEqual(again.ok, true); assert.strictEqual(W.S.calls.length, n1); assert(!('huggie' in again));
  // batch-style: calls 0 and not `already` -> the page counts it as already there with the same words
  assert.strictEqual(Page.wording(Object.assign({}, again, { already: true }), LL, CFG.lion.key).text, todayWords);
  const plain2 = await lionPlain(W); assert.strictEqual(plain2.calls, 0); assert.deepStrictEqual(Object.assign({}, plain2), Object.assign({}, todayAgain), 'a plain press after it answers exactly as it did before the rule ran');
});
test('LION without a Huggie product and no earlier press at all (everything on CUTE LION): the key goes on all 12 exactly as the plain press does it; same PUT, same record, same files', async () => {
  const A = lionWorld({ noHuggie: true }), a = await lionPlain(A), B = lionWorld({ noHuggie: true }), b = await lionRule(B);
  assert.strictEqual(a.ok, true); assert.strictEqual(b.ok, true, JSON.stringify(b)); assert.deepStrictEqual(B.S.puts.map(x => x.body), A.S.puts.map(x => x.body)); assert.deepStrictEqual(b, a); assert.deepStrictEqual(skus(B), Array(12).fill(CFG.lion.key));
});
test('LION WITH Huggie products (4 of 12, the family\'s shape): the plain press put BEADY CUTE LION on all 12; the rule puts the 4 back to CUTE LION and says so', async () => {
  const W = lionWorld(); const LL = CFG.lion.lid; await lionPlain(W); assert.deepStrictEqual(new Set(skus(W)), new Set([CFG.lion.key]));
  const puts0 = W.S.puts.length, r = await lionRule(W);
  assert.strictEqual(r.ok, true, JSON.stringify(r)); assert(!r.already); assert.strictEqual(r.calls, 2); assert.strictEqual(W.S.puts.length - puts0, 1, 'ONE PUT'); assert.deepStrictEqual(r.huggie, { products: 4, keyed: 8, restored: [{ sku: 'CUTE LION', n: 4 }], left: 0 });
  assert.deepStrictEqual(bySku(W, isHug), Array(4).fill('CUTE LION')); assert.deepStrictEqual(bySku(W, p => !isHug(p)), Array(8).fill(CFG.lion.key)); assert.strictEqual(W.S.rejected.length, 0);
  assert.strictEqual(Page.wording(r, LL, CFG.lion.key).text, 'Done: BEADY CUTE LION is on the 8 products of listing 1801980518 that are not Huggie. 4 Huggie products put back to CUTE LION. The listing as it was is kept.');
  const again = await lionRule(W); assert.strictEqual(again.calls, 0); assert.deepStrictEqual(again.huggie, r.huggie);
});

/* ═══ 7 · what the page says ═══ */
test('the panel\'s words: the single press keeps every old line; the rule\'s answers have their own (plural and singular)', () => {
  const W = (r) => Page.wording(r, LID, KEY).text;
  assert.strictEqual(W({ ok: true, products: 12, verified: true }), 'Done: BEADY DOVE 4 is now on all 12 products of listing 1816178147.');
  assert.strictEqual(W({ ok: true, already: true, products: 12 }), 'Already there: all 12 products of listing 1816178147 carry BEADY DOVE 4. Nothing was changed.');
  const h = (o) => W(Object.assign({ ok: true, products: 12, huggie: { products: 4, keyed: 8, restored: [{ sku: OLD, n: 4 }], left: 0 } }, o));
  assert.strictEqual(h({ beforePath: 'x' }), 'Done: BEADY DOVE 4 is on the 8 products of listing 1816178147 that are not Huggie. 4 Huggie products put back to DOVE 4. The listing as it was is kept.');
  assert.strictEqual(W({ ok: true, products: 3, beforePath: 'x', huggie: { products: 1, keyed: 2, restored: [{ sku: 'A B', n: 1 }], left: 0 } }), 'Done: BEADY DOVE 4 is on the 2 products of listing 1816178147 that are not Huggie. 1 Huggie product put back to A B. The listing as it was is kept.');
  assert.strictEqual(W({ ok: true, already: true, products: 3, huggie: { products: 1, keyed: 1, restored: [], left: 0 } }), 'Already there: the 1 product of listing 1816178147 that is not Huggie carries BEADY DOVE 4, and the 1 Huggie product does not. Nothing was changed.');
  assert.strictEqual(W({ ok: true, products: 12, huggie: { products: 4, keyed: 8, restored: [{ sku: OLD, n: 3 }, { sku: 'DOVE 4 SS', n: 1 }], left: 0 } }), 'Done: BEADY DOVE 4 is on the 8 products of listing 1816178147 that are not Huggie. 3 Huggie products put back to DOVE 4 and 1 Huggie product put back to DOVE 4 SS.');
  assert.strictEqual(W({ ok: false, text: 'the listing is draft on Etsy', retry: true }), 'The listing is draft on Etsy.');
});

let finished = false;
process.on('exit', () => { if (!finished) { console.error('etsy-link-huggie: the run ended before the last case'); process.exitCode = 1; } });
(async () => {
  const warn = console.warn, err = console.error; console.warn = () => {}; console.error = () => {};
  let n = 0;
  try {
    for (const [name, fn] of T) {
      try { await fn(); n++; process.stdout.write('  ok  ' + name.slice(0, 170) + '\n'); }
      catch (e) { console.warn = warn; console.error = err; console.error('FAIL ' + name + '\n' + (e && e.stack || e)); finished = true; process.exit(1); }
    }
  } finally { console.warn = warn; console.error = err; }
  assert(maxCalls <= 3, 'no run made more than 3 Etsy requests (' + maxCalls + ')');
  const shape = c => c.method + ' ' + c.path.replace(/\/listings\/\d+/, '/listings/{id}') + (c.query ? ' ' + JSON.stringify(c.query) : '');
  assert.deepStrictEqual([...new Set(EVERY.map(shape))].sort(), ['GET /listings/{id}/inventory {"legacy":false}', 'PUT /listings/{id}/inventory {"legacy":false}'], 'every request of every case was GET inventory or PUT inventory of the one (fake) listing');
  finished = true;
  console.log('etsy-link-huggie OK: ' + n + ' cases, ' + EVERY.length + ' FAKE Etsy requests, the most in any one run: ' + maxCalls + ', real Etsy requests: 0');
  process.exit(0);
})().catch(e => { console.error(e); finished = true; process.exit(1); });
