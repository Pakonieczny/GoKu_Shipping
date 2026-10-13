// "Replace each set of etsy SKU's with the provided charm repo SKU" (ETSYFAM, 13 Oct 2026; Paul, 02:57 UTC), OFFLINE, on the REAL products of the listings he asked about:
// netlify/functions/_charmNestEtsyLink.js (op listingEtsySkuWrite) on the in-memory Firestore and Storage (tests/charm-nest/_sandboxFakes.cjs) and a FAKE ETSY (the same rules as
// tests/charm-nest/etsy-link.cjs: it escapes the text it serves, wants decimal prices, refuses a partial *_on_property set, refuses a SKU that varies over a property its
// sku_on_property does not name, gives every product a new id on each PUT, records every request). NO token, NO network: this file cannot reach Etsy at all.
//   REAL inputs (etsy-link-families-data.json, from the read-only Etsy reads of 13 Oct 2026): the product ids, the SKU of each product, the Etsy option names, ids and texts of
//   bike 1734130419 (20 x Sports 27-Bike + 20 x Sports 27-Bike-CO), monkey 1745415868 (40 + 20 x ...-CO), wolf 4494182906 and chameleon 4494184284 (40 blank + 20 placeholders),
//   dove 1816178147 and lion 1801980518 (12 products, one SKU each). STAND-INS (marked in the data): prices, stock, readiness, which product is disabled, the *_on_property lists
//   (the SKU follows the metal), the chameleon's option and value ids, the bar's 816 products (12 x 68 options, one SKU "BEADY"), a cat and a swan shape.
// What it proves for every one of them: the press writes the plain key on EVERY product whatever they carry (two SKUs, blanks, placeholders), exactly one PUT, at most 3 Etsy
// requests, only the `sku` field changes (the body equals the read with sku removed from both), the old SKUs are in the safety copy written BEFORE the PUT and on the record, the
// answer and the words name what was replaced, the second press makes no Etsy call, an echo that differs in ONE product is a verify failure with the copy kept, 401/403 = needs
// listings_w and nothing changed, a bad key is refused with 0 Etsy calls, and a listing with ONE SKU keeps exactly its words and its answer.
//   node tests/charm-nest/etsy-link-families.cjs
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

/* ═══ the fake Etsy (as in etsy-link.cjs, for any listing id) ═══ */
const esc = s => String(s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;').replace(/'/g, '&#39;');
const unesc = s => String(s).replace(/&quot;/g, '"').replace(/&#39;/g, "'").replace(/&lt;/g, '<').replace(/&gt;/g, '>').replace(/&amp;/g, '&');
const money = n => ({ amount: Math.round(n * 100), divisor: 100, currency_code: 'USD' });
const etsyErr = (status, error) => Object.assign(new Error('Etsy API ' + status + ': ' + (error || '')), { status, data: { error: error || 'HTTP ' + status } });
const propOrder = products => { const o = []; for (const p of products) for (const v of p.property_values) if (!o.includes(v.property_id)) o.push(v.property_id); return o; };
const EVERY = [];
function sim(LID, inventory, o) {
  o = o || {};
  const S = { stored: clone(inventory), calls: [], puts: [], seq: 900000000000, lastGet: null };
  const serve = () => ({ count: S.stored.products.length,
    products: S.stored.products.map(p => ({ product_id: p.product_id, sku: esc(p.sku), is_deleted: !!p.is_deleted,
      property_values: p.property_values.map(v => Object.assign({ property_id: v.property_id }, v.property_name == null ? {} : { property_name: esc(v.property_name) }, { scale_id: v.scale_id, value_ids: v.value_ids.slice(), values: v.values.map(esc) })),
      offerings: p.offerings.map(f => Object.assign({ offering_id: f.offering_id, price: money(f.price), quantity: f.quantity, is_enabled: f.is_enabled, is_deleted: false }, f.readiness_state_id != null ? { readiness_state_id: f.readiness_state_id } : {})) })),
    price_on_property: S.stored.price_on_property.slice(), quantity_on_property: S.stored.quantity_on_property.slice(), sku_on_property: S.stored.sku_on_property.slice() });
  function applyPut(b) {
    const bad = m => { throw etsyErr(400, m); };
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
const stripIds = inv => { const c = clone(inv); for (const p of c.products) { delete p.product_id; for (const f of p.offerings) delete f.offering_id; } return c; };

/* ═══ the listings: real products + stand-in offerings ═══ */
function inventoryOf(d) {
  return { products: d.products.map((p, i) => ({ product_id: p.product_id, sku: p.sku, is_deleted: false,
      property_values: p.property_values.map(v => ({ property_id: v.property_id, property_name: v.property_name == null ? v.property_name : unesc(v.property_name), scale_id: v.scale_id, value_ids: v.value_ids.slice(), values: v.values.map(unesc) })),   // (stored as plain text; the fake serves it escaped, as Etsy does)
      offerings: [{ offering_id: p.product_id * 10, price: 28 + (i % 4) * 4 + (i % 7), quantity: i === 5 ? 0 : 999, is_enabled: i !== 5, readiness_state_id: 7001 }] })),   // STAND-INS: prices, stock, one disabled product, readiness
    price_on_property: d.price_on_property.slice(), quantity_on_property: d.quantity_on_property.slice(), sku_on_property: d.sku_on_property.slice() };
}
/* the bar (816 products = 12 metal options x 68 length/font options, one SKU "BEADY": the app's stored SKU table) and a cat and a swan shape: stand-in option texts */
function gridData(listingId, key, sku, metals, lengths, names) {
  const products = []; let id = 29000000000;
  metals.forEach((m, i) => lengths.forEach((n, j) => products.push({ product_id: ++id, sku: typeof sku === 'function' ? sku(i, j) : sku, property_values: [
    { property_id: 514, property_name: names[0], scale_id: null, value_ids: [55000000000 + i], values: [m] }, { property_id: 513, property_name: names[1], scale_id: null, value_ids: [57000000000 + j], values: [n] }] })));
  return { listingId, key, price_on_property: [514, 513], quantity_on_property: [], sku_on_property: [], products };
}
const BAR = gridData('1830130714', 'CURB', 'BEADY', Array.from({ length: 12 }, (_, i) => 'GOLD·' + i), Array.from({ length: 68 }, (_, j) => '16"·FONT' + j), ['METAL - ENGRAVING', 'LENGTH / FONT / ALIGNMENT']);
const CAT = gridData('1801964156', 'CAT 1 - SITTING CAT', 'CAT 1- Sitting Cat', ['Sterling Silver', 'Gold Filled', 'Rose Gold Filled', '14k Solid Gold'], ['Necklace CHARM', 'Huggie CHARM SET', 'CHARM + Engraving'], ['Metal Choice', 'Charm Type']);
const SWAN = gridData('1700763608', 'BEADY SWAN', 'Swan_3608', ['GF 14/20', 'SS', '14K Gold'], ['16"', '18"', '20"'], ['Metal', 'Chain length']);
const L = Object.assign({ bar: BAR, cat: CAT, swan: SWAN }, DATA);

let tick = 0, maxCalls = 0;
const masterEntry = sku => ({ sku, masterHash: 'm'.repeat(8), masterName: 'MASTER SKU_LIVING_MV_2026-0914.ai', widthPt: 33.29, heightPt: 25.74, holes: 1, engravable: true, aiPath: 'charmnest/master/' + sku.replace(/\W+/g, '_') + '.ai' });
function world(name, o) {
  o = o || {}; const d = L[name], LID = d.listingId;
  store.clear(); blobs.clear(); writes.length = 0; fileWrites.length = 0;
  const E = sim(LID, o.inventory || inventoryOf(d), o.sim);
  store.set('Charm_Master_Index/' + d.key, masterEntry(d.key)); store.set('Charm_Master_Index/OTHER_ONE', masterEntry('OTHER_ONE'));
  store.set('EtsyMail_Listings/' + LID, { state: 'active', title: 'a listing' });
  const mod = Mod.make({ db, FV, bucket, etsy: E.api, meter: { bumpSimple() {}, flushNow: async () => {} }, now: () => 1760000000000 + (++tick), timeoutMs: o.timeoutMs });
  const W = { d, LID, E, S: E.S, mod, rec: () => store.get('Charm_Custom_Saves/link-' + LID), link: () => (store.get('Charm_Custom_Saves/link-' + LID) || {}).link,
    run: async body => { const n0 = E.S.calls.length, r = await mod.listingEtsySkuWrite(body || { listingId: LID, sku: d.key, by: 'Paul' }); const n = E.S.calls.length - n0; maxCalls = Math.max(maxCalls, n); assert.strictEqual(n, r.calls, 'the answer counts exactly the Etsy calls made'); return r; } };
  return W;
}
const countBy = products => { const m = new Map(); for (const p of products) { const k = p.sku.toUpperCase(); m.set(k, (m.get(k) || { sku: p.sku, n: 0 })); m.get(k).n++; } return [...m.values()].sort((a, b) => b.n - a.n || (a.sku < b.sku ? -1 : a.sku > b.sku ? 1 : 0)); };

const T = [], test = (name, fn) => T.push([name, fn]);

/* the whole contract of one press, written from the GET answer alone and apart from the module */
async function pressAndProve(name, expectWords) {
  const W = world(name), d = W.d, N = d.products.length, groups = countBy(d.products), K = d.key;
  let atPut = null;
  const realPut = W.E.api.put; W.E.api.put = async (p, q, b) => { const l = W.link(); atPut = { state: l.state, path: l.beforePath, blob: l.beforePath && blobs.has(l.beforePath) ? blobs.get(l.beforePath).buf.toString('utf8') : null, saves: fileWrites.length }; return realPut(p, q, b); };
  const r = await W.run();
  assert.strictEqual(r.ok, true, name + ' ' + JSON.stringify(r)); assert.strictEqual(r.state, 'linked'); assert.strictEqual(r.products, N); assert.strictEqual(r.sku, K); assert.strictEqual(r.listingId, d.listingId); assert.strictEqual(r.verified, true);
  assert.strictEqual(r.calls, 2, 'inventory + PUT (the mirror knows the state)'); assert.strictEqual(W.S.puts.length, 1, 'exactly ONE PUT');
  assert(W.S.calls.every(c => /^\/listings\/\d+(\/inventory)?$/.test(c.path) && c.path.startsWith('/listings/' + d.listingId)), 'only this listing, only GET listing / GET inventory / PUT inventory');
  // what the products carried is said, with the counts, only when it was more than one SKU
  if (groups.length > 1) { assert.deepStrictEqual(r.replaced, groups.slice(0, 12)); assert(!r.replacedMore); } else { assert(!('replaced' in r) && !('replacedMore' in r), name + ' one SKU: no `replaced`: ' + JSON.stringify(r.replaced)); }
  // the PUT: the read with the sku field changed on EVERY product and nothing else (sku removed from both: equal; the lists only widened, never changed in value)
  const put = W.S.puts[0].body, get = W.S.lastGet;
  assert.strictEqual(put.products.length, N); assert(put.products.every(p => p.sku === K), 'every product takes the plain key');
  const noSku = b => { const c = clone(b); c.products.forEach(p => delete p.sku); delete c.sku_on_property; return c; };
  const read = { products: get.products.map(p => { const f = p.offerings[0]; return { sku: '', property_values: p.property_values.map(v => Object.assign({ property_id: v.property_id }, v.property_name == null ? {} : { property_name: unesc(v.property_name) }, { scale_id: v.scale_id, value_ids: v.value_ids, values: v.values.map(unesc) })),
    offerings: [Object.assign({ price: f.price.amount / f.price.divisor, quantity: f.is_enabled ? f.quantity : 0, is_enabled: f.is_enabled }, f.readiness_state_id != null ? { readiness_state_id: f.readiness_state_id } : {})] }; }),
    price_on_property: get.price_on_property, quantity_on_property: get.quantity_on_property, sku_on_property: get.sku_on_property };
  assert.deepStrictEqual(noSku(put), noSku(read), name + ': the body equals the read with sku removed from both');
  const total = new Set(get.products.flatMap(p => p.property_values.map(v => v.property_id))).size, a = get.sku_on_property.slice().sort(), b = put.sku_on_property.slice().sort();
  assert(JSON.stringify(a) === JSON.stringify(b) || (a.length > 0 && a.length < total && b.length === total), name + ': sku_on_property is as read, or widened to every property with no value changed: ' + JSON.stringify([a, b]));
  // what Etsy then stores differs from before in the SKUs (and that widened list) only
  const was = stripIds(inventoryOf(d)), now = stripIds(W.S.stored); was.products.forEach(p => { p.sku = K; }); delete was.sku_on_property; delete now.sku_on_property;
  assert.deepStrictEqual(now, was, name + ': Etsy stores the same listing with the key on every product');
  // the safety copy: written and noted BEFORE the PUT, the whole read, every old SKU in it, never overwritten
  assert(atPut, 'the PUT happened'); assert.strictEqual(atPut.state, 'writing', 'the record said "writing" when the PUT left'); assert.strictEqual(atPut.saves, 1, 'one file written before the PUT'); assert.strictEqual(atPut.path, r.beforePath);
  const saved = JSON.parse(atPut.blob); assert.deepStrictEqual(saved, get, 'the safety copy is the GET answer, whole');
  assert.deepStrictEqual(saved.products.map(p => unesc(p.sku)), d.products.map(p => p.sku), 'every old SKU, product by product, is in the safety copy');
  assert.deepStrictEqual(fileWrites.map(f => f.path), [r.beforePath], 'nothing else was written to Storage');
  const link = W.link(); assert.strictEqual(link.state, 'linked'); assert.strictEqual(link.products, N); assert.strictEqual(link.attempts, 1); assert(!('runningUntil' in link));
  if (groups.length > 1) assert.deepStrictEqual(link.replaced, r.replaced, 'the record says what was replaced');
  assert.deepStrictEqual(store.get('Charm_Listing_Skus/' + d.listingId).uni, K, 'the SKU table the page reads shows the one key');
  // the words
  const words = Page.wording(r, d.listingId, K).text; assert.strictEqual(words, expectWords, name + ' words');
  // a second press: no Etsy call, no write, the same answer and words
  const n0 = W.S.calls.length, w0 = writes.length, again = await W.run();
  assert.strictEqual(again.ok, true); assert.strictEqual(again.state, 'linked'); assert.strictEqual(again.calls, 0); assert.strictEqual(W.S.calls.length, n0); assert.strictEqual(writes.length, w0); assert.strictEqual(fileWrites.length, 1);
  assert.strictEqual(Page.wording(again, d.listingId, K).text, expectWords, name + ' second press: the same words');
  return { W, r };
}

/* ═══ 1 · the listings with several SKUs: every product takes the plain key ═══ */
test('bike 1734130419 (SPORTS 27): 20 x Sports 27-Bike + 20 x Sports 27-Bike-CO -> all 40 products take SPORTS 27 (no -CO twin)', async () => {
  const { r } = await pressAndProve('bike', 'Done: SPORTS 27 is now on all 40 products of listing 1734130419. It replaced 20 x Sports 27-Bike and 20 x Sports 27-Bike-CO; the listing as it was is kept.');
  assert.deepStrictEqual(r.replaced, [{ sku: 'Sports 27-Bike', n: 20 }, { sku: 'Sports 27-Bike-CO', n: 20 }]);
});
test('monkey 1745415868 (BEADY MONKEY HEAR NO EVIL3): 40 x Hear No Evil Monkey + 20 x ...-CO -> all 60 products take the key', async () => {
  const { r } = await pressAndProve('monkey', 'Done: BEADY MONKEY HEAR NO EVIL3 is now on all 60 products of listing 1745415868. It replaced 40 x Hear No Evil Monkey and 20 x Hear No Evil Monkey-CO; the listing as it was is kept.');
  assert.deepStrictEqual(r.replaced, [{ sku: 'Hear No Evil Monkey', n: 40 }, { sku: 'Hear No Evil Monkey-CO', n: 20 }]);
});
test('wolf 4494182906 (WOLF_67948): 40 blank + the shop\'s five placeholder SKUs (4 each) -> all 60 products take the key', async () => {
  await pressAndProve('wolf', 'Done: WOLF_67948 is now on all 60 products of listing 4494182906. It replaced 40 x (no SKU), 4 x CO-10kSolidCh, 4 x CO-14kSolidCh, 4 x CO-GoldCharmO, 4 x CO-RoseCharmO and 4 x CO-SilverChar; the listing as it was is kept.');
});
test('chameleon 4494184284 (CHAMELEON_28151): 40 blank + five placeholders -> all 60 products take the key', async () => {
  await pressAndProve('chameleon', 'Done: CHAMELEON_28151 is now on all 60 products of listing 4494184284. It replaced 40 x (no SKU), 4 x CO-10kSolidCh, 4 x CO-14kSolidCh, 4 x CO-GoldCharmO, 4 x CO-RoseCharmO and 4 x CO-SilverChar; the listing as it was is kept.');
});

/* ═══ 2 · the listings with ONE SKU: exactly what they always had ═══ */
test('dove 1816178147, lion 1801980518, bar 1830130714 (816 products), a cat shape and a swan shape: one SKU on every product -> the old words, no `replaced`, the old answer', async () => {
  await pressAndProve('dove', 'Done: BEADY DOVE 4 is now on all 12 products of listing 1816178147.');
  await pressAndProve('lion', 'Done: BEADY CUTE LION is now on all 12 products of listing 1801980518.');
  const t0 = Date.now(); await pressAndProve('bar', 'Done: CURB is now on all 816 products of listing 1830130714.'); assert(Date.now() - t0 < 15000, 'the 816-product write is quick offline: ' + (Date.now() - t0) + ' ms');
  await pressAndProve('cat', 'Done: CAT 1 - SITTING CAT is now on all 12 products of listing 1801964156.');
  await pressAndProve('swan', 'Done: BEADY SWAN is now on all 9 products of listing 1700763608.');
});

/* ═══ 3 · the stops stay ═══ */
test('a key that is not a master SKU (or not a shape Etsy keeps) is refused with 0 Etsy calls on the two-SKU listings; the "-CO" twin is no master and is never derived', async () => {
  for (const name of ['bike', 'monkey', 'wolf', 'chameleon']) {
    const W = world(name); writes.length = 0;
    for (const [sku, reason] of [[W.d.key + '-CO', 'sku-not-in-master'], [W.d.key.toLowerCase(), 'sku-shape'], [W.d.key + ' ', 'sku-shape'], [W.d.key + ' 2', 'sku-not-in-master'], ['Sports 27-Bike', 'sku-shape'], ['SPORTS 27-BIKE', 'sku-not-in-master']]) {
      const r = await W.run({ listingId: W.LID, sku, by: 'Paul' }); assert.strictEqual(r.reason, reason, name + ' ' + sku + ' ' + JSON.stringify(r)); assert.strictEqual(r.calls, 0);
    }
    assert.strictEqual(W.S.calls.length, 0, name + ': no Etsy call'); assert.strictEqual(writes.length, 0); assert.strictEqual(fileWrites.length, 0);
    for (const body of [{ listingId: W.LID, sku: W.d.key, by: 'Paul', sandbox: true }]) { const r = await W.run(body); assert.strictEqual(r.reason, 'sandbox'); assert.strictEqual(r.calls, 0); }
    assert.strictEqual(W.S.calls.length, 0);
  }
});
test('401/403 on the PUT of a two-SKU listing: "needs listings_w", ONE PUT, nothing changed on Etsy, the old SKUs still in the safety copy; the next press (after re-authorising) replaces them once', async () => {
  for (const name of ['bike', 'wolf']) {
    let deny = true;
    const W = world(name, { sim: { fail: c => (c.method === 'PUT' && deny ? etsyErr(403, 'not allowed') : null) } });
    const r = await W.run(); assert.strictEqual(r.ok, false); assert.strictEqual(r.reason, 'scope'); assert.strictEqual(r.needs, 'listings_w'); assert.strictEqual(r.calls, 2); assert.strictEqual(W.S.puts.length, 1, 'one PUT, no loop');
    assert.deepStrictEqual(W.S.stored.products.map(p => p.sku), W.d.products.map(p => p.sku), 'nothing changed on Etsy: every product still carries its own SKU');
    const saved = JSON.parse(blobs.get(r.beforePath).buf.toString('utf8')); assert.deepStrictEqual(saved.products.map(p => unesc(p.sku)), W.d.products.map(p => p.sku), 'the old SKUs are in the safety copy');
    deny = false; const again = await W.run(); assert.strictEqual(again.ok, true, JSON.stringify(again)); assert.strictEqual(W.S.puts.length, 2); assert(again.replaced && again.replaced.length >= 2, 'the second try still says what it replaced'); assert(W.S.stored.products.every(p => p.sku === W.d.key));
  }
});
test('Etsy\'s echo is compared PER PRODUCT: ONE product handed back with another SKU is a verify failure with the safety copy kept, ONE PUT, no undo, never sent again unchanged', async () => {
  for (const name of ['bike', 'monkey']) {
    const W = world(name, { sim: { skuTransform: (s, i) => (i === 33 ? s + 'X' : s) } }); const r = await W.run();
    assert.strictEqual(r.ok, false); assert.strictEqual(r.reason, 'verify'); assert(r.details.some(x => /SKU/.test(x)), JSON.stringify(r.details)); assert.strictEqual(W.S.puts.length, 1); assert(blobs.has(r.beforePath));
    const saved = JSON.parse(blobs.get(r.beforePath).buf.toString('utf8')); assert.deepStrictEqual(saved.products.map(p => unesc(p.sku)), W.d.products.map(p => p.sku));
    assert.strictEqual(W.link().state, 'failed'); const n0 = W.S.calls.length, again = await W.run(); assert.strictEqual(again.reason, 'verify'); assert.strictEqual(again.calls, 0); assert.strictEqual(W.S.calls.length, n0);
  }
});
test('a listing that already carries the key on every product (a person set it, or an earlier try landed) is "already": 1 Etsy call, no safety copy, no PUT, and no `replaced`', async () => {
  for (const name of ['bike', 'wolf', 'dove']) {
    const d = L[name], inv = inventoryOf(d); inv.products.forEach(p => { p.sku = d.key; });
    const W = world(name, { inventory: inv }); const r = await W.run(); assert.strictEqual(r.ok, true); assert.strictEqual(r.already, true); assert.strictEqual(r.calls, 1); assert.strictEqual(W.S.puts.length, 0); assert.strictEqual(fileWrites.length, 0); assert(!('replaced' in r));
    assert.strictEqual(Page.wording(r, d.listingId, d.key).text, 'Already there: all ' + d.products.length + ' products of listing ' + d.listingId + ' carry ' + d.key + '. Nothing was changed.');
  }
});
test('part-way: some products already carry the key and the rest carry other SKUs -> all of them end on the key (the key counts as one of the groups)', async () => {
  const d = L.bike, inv = inventoryOf(d); inv.products.slice(0, 10).forEach(p => { p.sku = d.key; });
  const W = world('bike', { inventory: inv }); const r = await W.run(); assert.strictEqual(r.ok, true, JSON.stringify(r));
  assert.deepStrictEqual(r.replaced, [{ sku: 'Sports 27-Bike-CO', n: 20 }, { sku: 'Sports 27-Bike', n: 10 }, { sku: 'SPORTS 27', n: 10 }].sort((a, b) => b.n - a.n || (a.sku < b.sku ? -1 : 1)));
  assert(W.S.stored.products.every(p => p.sku === d.key));
});

let finished = false;
process.on('exit', () => { if (!finished) { console.error('etsy-link-families: the run ended before the last case'); process.exitCode = 1; } });
(async () => {
  const warn = console.warn, err = console.error; console.warn = () => {}; console.error = () => {};
  let n = 0;
  try {
    for (const [name, fn] of T) {
      try { await fn(); n++; process.stdout.write('  ok  ' + name.slice(0, 150) + '\n'); }
      catch (e) { console.warn = warn; console.error = err; console.error('FAIL ' + name + '\n' + (e && e.stack || e)); finished = true; process.exit(1); }
    }
  } finally { console.warn = warn; console.error = err; }
  assert(maxCalls <= 3, 'no run made more than 3 Etsy requests (' + maxCalls + ')');
  const shape = c => c.method + ' ' + c.path.replace(/\/listings\/\d+/, '/listings/{id}') + (c.query ? ' ' + JSON.stringify(c.query) : '');
  assert.deepStrictEqual([...new Set(EVERY.map(shape))].sort(), ['GET /listings/{id}/inventory {"legacy":false}', 'PUT /listings/{id}/inventory {"legacy":false}'], 'every Etsy request of every case was GET inventory or PUT inventory of the one listing (' + EVERY.length + ' requests)');
  finished = true;
  console.log('etsy-link-families OK: ' + n + ' cases, ' + EVERY.length + ' fake Etsy requests, the most in any one run: ' + maxCalls + ', real Etsy requests: 0');
  process.exit(0);
})().catch(e => { console.error(e); finished = true; process.exit(1); });
