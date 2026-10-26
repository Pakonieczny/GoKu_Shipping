// "Put a master charm's SKU on an existing Etsy listing" (SWANSKU, 12 Oct 2026; Paul: "Update this etsy listing SKU to match this Charm in our repo"), OFFLINE:
// netlify/functions/_charmNestEtsyLink.js (op listingEtsySkuWrite) on an in-memory Firestore and Storage (tests/charm-nest/_sandboxFakes.cjs) and a FAKE ETSY that
// behaves like the real one where it matters for this write (the same fake as tests/charm-nest/save-etsy.cjs: it escapes the text it serves, 16" arrives as 16&quot;,
// stores what it is sent, gives every product a new id on each PUT, wants decimal prices, refuses a partial *_on_property set, refuses a SKU that varies over a
// property its sku_on_property does not name, and records every request). NO real token, NO network: the one end-to-end case runs the real etsyFetch against a fake
// node-fetch and a made-up token. Etsy's inventory PUT replaces the WHOLE inventory, so every case below is a way of not harming a listing:
//   · every product takes the SKU ("BEADY SWAN", with its space) and EVERYTHING else goes back as read: a listing with two options (3 metals x 3 lengths, 16" entity-escaped,
//     a disabled product, readiness ids), blank SKUs, one product with no options, escaped option text and names, the *_on_property lists;
//   · a listing whose products carry SEVERAL different SKUs is REPLACED (ETSYFAM, 13 Oct 2026, Paul: "Replace each set of etsy SKU's with the provided charm repo SKU"): every product
//     takes the SKU, the answer names what was replaced, the before-copy keeps the old SKUs; one that already carries the SKU is "already", no write;
//   · the SKU must be EXACTLY a master SKU (not blocked, not in the sandbox) and one Etsy keeps as written (32 characters, no space at an end or two in a row);
//     Etsy's own echo that changed the SKU is recorded as verify with the before-copy, one PUT, no undo;
//   · the before-copy is in Storage and on the record BEFORE the PUT, never overwritten; if it cannot be written or noted, NOTHING is sent; a body that fails the self-check is not sent;
//   · 401/403 = "needs listings_w" with no retry loop (one PUT per press); 429, 5xx, timeouts, a refused body: each a plain reason; a refused body is not sent again unchanged;
//   · a double press sends once (lease while in flight, nothing after linked), an unconfirmed PUT is checked by reading first, a listing changed by someone else meanwhile is left alone;
//   · call counts: 2 with the mirror's state, 3 without, never 4; the op is behind the repo "modify" token.
//   node tests/charm-nest/etsy-link.cjs
'use strict';
const assert = require('assert'), path = require('path'), Module = require('module');
const fnDir = path.join(__dirname, '../../netlify/functions');
process.env.SHOP_ID = '987654'; process.env.CLIENT_ID = 'test-key'; process.env.CLIENT_SECRET = 'test-secret'; delete process.env.EDIT_PASSCODE;
const Fk = require('./_sandboxFakes.cjs');
const { store, blobs, writes, fileWrites, db, bucket } = Fk;
const FV = Fk.admin.firestore.FieldValue;
const Mod = require(path.join(fnDir, '_charmNestEtsyLink.js'));
require(path.join(__dirname, '../../charm-nest-etsy-link.js')); const Page = globalThis.CharmNestEtsyLink;   // (the page's own wording: pure functions, no DOM needed)
const clone = x => JSON.parse(JSON.stringify(x));
const LID = '1718000002', SKU = 'BEADY SWAN', OLD = 'Swan_3608';

/* ═══ the fake Etsy (as in save-etsy.cjs; one addition: skuTransform, Etsy keeping a different SKU than it was sent) ═══ */
/* ═══ the fake Etsy ═══ */
const esc = s => String(s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;').replace(/'/g, '&#39;');
const unesc = s => String(s).replace(/&quot;/g, '"').replace(/&#39;/g, "'").replace(/&lt;/g, '<').replace(/&gt;/g, '>').replace(/&amp;/g, '&');   // (written here, apart from the module: one level, & last)
const money = n => ({ amount: Math.round(n * 100), divisor: 100, currency_code: 'USD' });
const etsyErr = (status, error) => Object.assign(new Error('Etsy API ' + status + ': ' + (error || '')), { status, data: { error: error || 'HTTP ' + status } });
const propOrder = products => { const o = []; for (const p of products) for (const v of p.property_values) if (!o.includes(v.property_id)) o.push(v.property_id); return o; };

/* an inventory as Etsy STORES it (raw text, decimal prices); rows: { sku, pv:[[propertyId, name, text]], price, qty, on, ready (null = none), deleted } */
function mkInv(rows, lists) {
  const ids = new Map(); let next = 5000, pid = 100;
  const vid = (p, t) => { const k = p + '|' + t; if (!ids.has(k)) ids.set(k, next++); return ids.get(k); };
  return Object.assign({
    products: rows.map(r => { const id = pid++; return { product_id: id, sku: r.sku, is_deleted: !!r.deleted,
      property_values: (r.pv || []).map(([p, name, text]) => ({ property_id: p, property_name: name, scale_id: null, value_ids: [vid(p, text)], values: [text] })),
      offerings: [Object.assign({ offering_id: id * 10, price: r.price == null ? 25 : r.price, quantity: r.qty == null ? 5 : r.qty, is_enabled: r.on !== false }, r.ready === null ? {} : { readiness_state_id: r.ready || 7001 })] }; }),
    price_on_property: [], quantity_on_property: [], sku_on_property: [] }, lists || {});
}
function sim(inventory, o) {
  o = o || {};
  const S = { stored: clone(inventory), state: o.state || 'active', calls: [], puts: [], gets: [], seq: 900000, lastGet: null };
  const every = sim.every;
  const serve = () => ({ count: S.stored.products.length,
    products: S.stored.products.map(p => Object.assign({ product_id: p.product_id, sku: o.escapeSku === false ? p.sku : esc(p.sku), is_deleted: !!p.is_deleted,
      property_values: p.property_values.map(v => Object.assign({ property_id: v.property_id }, v.property_name == null ? {} : { property_name: esc(v.property_name) }, { scale_id: v.scale_id, value_ids: v.value_ids.slice(), values: v.values.map(esc) })),
      offerings: p.offerings.map(f => Object.assign({ offering_id: f.offering_id, price: money(f.price), quantity: f.quantity, is_enabled: f.is_enabled, is_deleted: false }, f.readiness_state_id != null ? { readiness_state_id: f.readiness_state_id } : {})) })),
    price_on_property: S.stored.price_on_property.slice(), quantity_on_property: S.stored.quantity_on_property.slice(), sku_on_property: S.stored.sku_on_property.slice(),
    ...(S.stored.readiness_state_on_property ? { readiness_state_on_property: S.stored.readiness_state_on_property.slice() } : {}) });
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
      if (o.needReadiness && f.readiness_state_id == null) bad('readiness_state_id is required');
      for (const v of p.property_values) if (!Array.isArray(v.values) || !Array.isArray(v.value_ids)) bad('property values malformed');
    }
    const total = propOrder(b.products).length, lists = ['price_on_property', 'quantity_on_property', 'sku_on_property'];
    if (total && lists.some(f => (b[f] || []).length === total)) for (const f of lists) { const n = (b[f] || []).length; if (n > 0 && n < total) bad(f + ': unsupported number of property IDs. Supports only zero or all ' + total + ' variation properties'); }
    for (const [f, key] of [['price_on_property', 'price'], ['sku_on_property', 'sku']]) {   // a field may differ only between products that differ on a property the list names
      const decl = new Set((b[f] || []).map(Number)), seen = new Map();
      for (const p of b.products) {
        const g = p.property_values.filter(v => decl.has(v.property_id)).map(v => v.property_id + ':' + v.values.join('/')).sort().join('|'), val = key === 'sku' ? p.sku : p.offerings[0].price;
        if (seen.has(g) && seen.get(g) !== val) bad(f + ' does not cover how ' + key + ' varies');
        seen.set(g, val);
      }
    }
    S.stored = { products: b.products.map(p => { const f = p.offerings[0]; return { product_id: ++S.seq, sku: o.skuTransform ? o.skuTransform(p.sku) : p.sku, is_deleted: false,   // (new ids on every PUT)
      property_values: p.property_values.map(v => Object.assign({ property_id: v.property_id }, v.property_name == null ? {} : { property_name: v.property_name }, { scale_id: v.scale_id, value_ids: v.value_ids.slice(), values: v.values.slice() })),
      offerings: [Object.assign({ offering_id: ++S.seq, price: f.price, quantity: f.quantity, is_enabled: f.is_enabled && !(f.quantity > 0 && false) }, f.readiness_state_id != null ? { readiness_state_id: f.readiness_state_id } : {})] }; }),
      price_on_property: b.price_on_property.slice(), quantity_on_property: b.quantity_on_property.slice(), sku_on_property: b.sku_on_property.slice(), ...(b.readiness_state_on_property ? { readiness_state_on_property: b.readiness_state_on_property.slice() } : {}) };
    for (const p of S.stored.products) for (const f of p.offerings) if (!f.is_enabled && f.quantity > 0) f.is_enabled = true;   // (Etsy: stock on a disabled offering switches it on again)
    if (o.mutate) o.mutate(S.stored);
  }
  async function handle(method, p, q, b) {
    const call = { method, path: p, query: q, body: b === undefined ? undefined : clone(b) };
    S.calls.push(call); every.push(call); if (method === 'PUT') S.puts.push(call); else S.gets.push(call);
    if (o.during) await o.during(call);
    if (o.hang && o.hang(call)) await new Promise(() => {});
    const fail = o.fail && o.fail(call); if (fail) throw fail;
    const m = /^\/listings\/(\d+)(\/inventory)?$/.exec(p);
    if (!m || m[1] !== LID) throw etsyErr(404, 'no such listing');
    if (!m[2] && method === 'GET') return { listing_id: +m[1], state: S.state };
    if (m[2] && method === 'GET') { S.lastGet = serve(); if (o.shape) o.shape(S.lastGet); return clone(S.lastGet); }
    if (m[2] && method === 'PUT') {
      assert.deepStrictEqual(q, { legacy: false }, 'the inventory is always written with legacy=false');
      applyPut(b);
      const e = o.echo, out = e === 'empty' ? {} : typeof e === 'function' ? e(serve()) : serve();
      S.lastEcho = clone(out); return out;
    }
    throw etsyErr(405, 'unexpected request');
  }
  return { S, api: { get: (p, q) => handle('GET', p, q), put: (p, q, b) => handle('PUT', p, q, b) } };
}
sim.every = [];   // every request any fake Etsy of this file received
const stripIds = inv => { const c = clone(inv); for (const p of c.products) { delete p.product_id; for (const f of p.offerings) delete f.offering_id; } return c; };

const kinds = W => W.S.calls.map(c => c.method + ' ' + c.path.replace(LID, '{id}'));
const noWrites = () => { assert.strictEqual(writes.length, 0, 'no document written: ' + JSON.stringify(writes)); assert.strictEqual(fileWrites.length, 0, 'no file written'); };
/* what the PUT must be, written from the GET answer alone and apart from the module */
function expectedBody(get, isGroup, newSku) {
  return { products: get.products.filter(p => !p.is_deleted).map(p => { const f = p.offerings[0]; return { sku: isGroup(p) ? newSku : unesc(p.sku),
    property_values: p.property_values.map(v => Object.assign({ property_id: v.property_id }, v.property_name == null ? {} : { property_name: unesc(v.property_name) }, { scale_id: v.scale_id, value_ids: v.value_ids, values: v.values.map(unesc) })),
    offerings: [Object.assign({ price: f.price.amount / f.price.divisor, quantity: f.is_enabled ? f.quantity : 0, is_enabled: f.is_enabled }, f.readiness_state_id != null ? { readiness_state_id: f.readiness_state_id } : {})] }; }),
    price_on_property: get.price_on_property, quantity_on_property: get.quantity_on_property, sku_on_property: get.sku_on_property };
}


/* ═══ the world of one case: the master, the mirror, the module on fakes ═══ */
let tick = 0, maxCalls = 0;
const masterEntry = sku => ({ sku, masterHash: 'm'.repeat(8), masterName: 'MASTER SKU_LIVING_MV_2026-0914.ai', widthPt: 33.29, heightPt: 25.74, holes: 1, engravable: true, aiPath: 'charmnest/master/' + sku.replace(/\W+/g, '_') + '.ai' });
function world(o) {
  o = o || {};
  store.clear(); blobs.clear(); writes.length = 0; fileWrites.length = 0;
  const inv = o.inventory || beady(), E = sim(inv, o.sim);
  for (const s of ['BEADY SWAN', 'SWAN_77741', 'HUGGIE HOOPS- SWAN', 'OTHER_ONE']) store.set('Charm_Master_Index/' + s, masterEntry(s));
  for (const [k, v] of Object.entries(o.master || {})) store.set('Charm_Master_Index/' + k, v);
  if (o.mirror !== false) store.set('EtsyMail_Listings/' + LID, { state: o.mirror || 'active', title: 'Swan charm necklace' });
  const bumps = [];
  const mod = Mod.make({ db: o.db || db, FV, bucket: o.bucket || bucket, etsy: E.api, meter: { bumpSimple: n => bumps.push(n), flushNow: async () => {} }, now: () => 1760000000000 + (++tick), timeoutMs: o.timeoutMs, maxCalls: o.maxCalls });
  const rec = () => store.get('Charm_Custom_Saves/link-' + LID);
  const W = { E, S: E.S, mod, bumps, inv, rec, link: () => (rec() || {}).link,
    run: async body => { const n0 = E.S.calls.length, r = await W.mod.listingEtsySkuWrite(body || { listingId: LID, sku: SKU, by: 'Paul' }); const n = E.S.calls.length - n0; maxCalls = Math.max(maxCalls, n); assert.strictEqual(n, r.calls, 'the answer counts exactly the Etsy calls made: ' + JSON.stringify(r)); return r; } };
  return W;
}

/* the listings the cases use */
const metals = ['GF 14/20', 'SS', '14K Gold'], lengths = ['16"', '18"', '20"'];
const beady = (sku) => mkInv(metals.flatMap((m, i) => lengths.map((n, j) => ({ sku: sku == null ? OLD : sku, pv: [[513, 'Metal', m], [514, 'Chain length', n]], price: 20 + i * 10 + j * 2, qty: (i + j) % 4 === 0 ? 0 : 3 + j, on: (i + j) % 4 !== 0, ready: j % 2 ? 7001 : 7002 }))), { price_on_property: [513, 514] });
const byMetal = () => mkInv(metals.flatMap((m, i) => lengths.map((n, j) => ({ sku: ['SWAN_GF', 'SWAN_SS', 'SWAN_14K'][i], pv: [[513, 'Metal', m], [514, 'Chain length', n]], price: 20 + i * 10 + j * 2 }))), { price_on_property: [513, 514], sku_on_property: [513] });
const T = [], test = (name, fn) => T.push([name, fn]);

/* the PUT is the GET answer with every product's SKU changed, and what Etsy then stores differs from what it stored only in those SKUs */
async function sameExceptSku(W, sku) {
  const r = await W.run(); assert.strictEqual(r.ok, true, JSON.stringify(r));
  assert.strictEqual(W.S.puts.length, 1, 'exactly one PUT');
  const put = W.S.puts[0].body, want = expectedBody(W.S.lastGet, () => true, sku);
  assert.deepStrictEqual(put, want, 'the PUT body equals the GET answer except every product\'s sku');
  const exp = clone(W.inv); exp.products.forEach(p => { if (!p.is_deleted) p.sku = sku; });
  assert.deepStrictEqual(stripIds(W.S.stored), stripIds(exp), 'the listing as Etsy stores it differs only in the SKUs');
  return r;
}

/* ═══ 1 · the write: everything but the SKU goes back as read ═══ */
test('two options (3 metals x 3 lengths, 16" entity-escaped, a disabled product, readiness ids): every product takes "BEADY SWAN", nothing else moves; 2 calls with the mirror', async () => {
  const W = world();
  assert.strictEqual(W.inv.products.length, 9);
  const r = await sameExceptSku(W, SKU);
  assert.strictEqual(r.state, 'linked'); assert.strictEqual(r.calls, 2); assert.strictEqual(r.sku, SKU); assert.strictEqual(r.listingId, LID); assert.strictEqual(r.products, 9); assert.strictEqual(r.verified, true);
  assert.deepStrictEqual(W.S.calls.map(c => c.method + ' ' + c.path.replace(LID, '{id}')), ['GET /listings/{id}/inventory', 'PUT /listings/{id}/inventory']);
  assert.deepStrictEqual(W.S.calls.map(c => c.query), [{ legacy: false }, { legacy: false }]);
  const put = W.S.puts[0].body;
  assert(put.products.every(p => p.sku === SKU), 'the space stays in the SKU'); assert(put.products.some(p => p.offerings[0].is_enabled === false && p.offerings[0].quantity === 0), 'disabled products are still disabled at zero stock');
  assert(put.products.some(p => p.offerings[0].readiness_state_id === 7002) && put.products.some(p => p.offerings[0].readiness_state_id === 7001), 'readiness ids kept per product');
  assert(put.products.some(p => p.property_values[1].values[0] === '16"') && !JSON.stringify(put).includes('&quot;'), 'option text goes back as plain text (16", not 16&quot;)');
  assert.deepStrictEqual(put.price_on_property, [513, 514]);
  assert.strictEqual(r.productIds.length, 9); assert.deepStrictEqual(r.productIds, W.S.stored.products.map(p => String(p.product_id)), 'the ids are the ones Etsy gave after the write');
  assert.strictEqual(W.bumps.length, 2); assert(W.bumps.every(n => n === 'custom.skuLink'), 'every Etsy call is counted once as custom.skuLink');
  const d = W.rec(); assert.strictEqual(d.kind, 'listingSkuLink'); assert.strictEqual(d.listingId, LID); assert.strictEqual(d.sku, SKU); assert.strictEqual(d.by, 'Paul');
  assert.strictEqual(d.link.state, 'linked'); assert.strictEqual(d.link.calls, 2); assert.strictEqual(d.link.verified, true); assert.strictEqual(d.link.attempts, 1); assert(!('runningUntil' in d.link), 'the lease is released'); assert.strictEqual(d.link.products, 9);
  assert.deepStrictEqual(store.get('Charm_Listing_Skus/' + LID), { at: store.get('Charm_Listing_Skus/' + LID).at, n: 9, uni: SKU }, 'the SKU table the page reads shows the one SKU');
  assert(!/name|email|address|buyer/i.test(JSON.stringify(d)), 'no buyer data on the record');
});
test('blank SKUs (a listing with none, or blanks beside one value): every product takes the SKU', async () => {
  const blank = mkInv([{ sku: '', pv: [[513, 'Metal', 'GF']], price: 20 }, { sku: '', pv: [[513, 'Metal', 'SS']], price: 22 }, { sku: '', pv: [[513, 'Metal', 'RG']], price: 24 }], { price_on_property: [513] });
  const W = world({ inventory: blank }); await sameExceptSku(W, SKU);
  const some = mkInv([{ sku: '', pv: [[513, 'Metal', 'GF']], price: 20 }, { sku: OLD, pv: [[513, 'Metal', 'SS']], price: 22 }, { sku: OLD.toLowerCase(), pv: [[513, 'Metal', 'RG']], price: 24 }], { price_on_property: [513], sku_on_property: [513] });
  const W2 = world({ inventory: some }); await sameExceptSku(W2, SKU);
  assert.deepStrictEqual(W2.S.puts[0].body.sku_on_property, [513], 'the lists are left as Etsy sent them');
});
test('a single product with no options; no mirror: GET listing first, 3 calls and never 4', async () => {
  const W = world({ inventory: mkInv([{ sku: OLD }]), mirror: false });
  const r = await sameExceptSku(W, SKU); assert.strictEqual(r.calls, 3);
  assert.deepStrictEqual(W.S.calls.map(c => c.method + ' ' + c.path.replace(LID, '{id}')), ['GET /listings/{id}', 'GET /listings/{id}/inventory', 'PUT /listings/{id}/inventory']);
});
test('option text, names and the old SKU are unescaped ONCE (Etsy stores what it is sent and escapes on the way out)', async () => {
  const inv = mkInv([{ sku: "ACORN'S LEAF", pv: [[513, 'Tom\'s "Metal"', 'Gold & Silver']], price: 30 }, { sku: "ACORN'S LEAF", pv: [[513, 'Tom\'s "Metal"', '16"']], price: 31 }, { sku: "ACORN'S LEAF", pv: [[513, 'Tom\'s "Metal"', '18&quot;']], price: 32 }, { sku: "ACORN'S LEAF", pv: [[513, 'Tom\'s "Metal"', 'a < b']], price: 33 }],
    { price_on_property: [513] });
  for (const escapeSku of [true, false]) {
    const W = world({ inventory: inv, sim: { escapeSku } }), r = await W.run(); assert.strictEqual(r.ok, true, JSON.stringify(r));
    const put = W.S.puts[0].body;
    assert.deepStrictEqual(put.products.map(p => p.property_values[0].values[0]), ['Gold & Silver', '16"', '18&quot;', 'a < b'], 'one pass: the stored 18&quot; (served as 18&amp;quot;) goes back as 18&quot;, not 18"');
    assert.strictEqual(put.products[0].property_values[0].property_name, 'Tom\'s "Metal"'); assert(put.products.every(p => p.sku === SKU));
    assert.deepStrictEqual(stripIds(W.S.stored).products.map(p => p.property_values[0].values[0]), ['Gold & Silver', '16"', '18&quot;', 'a < b'], 'what Etsy stores is unchanged text');
  }
});
test('the *_on_property lists: a partial one is widened as the proxy does; readiness-by-property kept when Etsy sends one; a deleted product is not sent', async () => {
  const partial = mkInv([{ sku: OLD, pv: [[513, 'Metal', 'GF'], [514, 'Chain length', '16"']], price: 20 }, { sku: OLD, pv: [[513, 'Metal', 'GF'], [514, 'Chain length', '18"']], price: 22 }, { sku: OLD, pv: [[513, 'Metal', 'SS'], [514, 'Chain length', '16"']], price: 24 }, { sku: OLD, pv: [[513, 'Metal', 'SS'], [514, 'Chain length', '18"']], price: 26 }],
    { price_on_property: [513, 514], sku_on_property: [513] });
  partial.readiness_state_on_property = [514]; partial.products.push({ product_id: 999, sku: 'GONE', is_deleted: true, property_values: [], offerings: [{ offering_id: 1, price: 5, quantity: 1, is_enabled: true }] });
  const W = world({ inventory: partial }), r = await W.run(); assert.strictEqual(r.ok, true, JSON.stringify(r));
  const put = W.S.puts[0].body;
  assert.deepStrictEqual([put.price_on_property, put.quantity_on_property, put.sku_on_property], [[513, 514], [], [513, 514]], 'sku_on_property widened to every property, the empty list left alone');
  assert.deepStrictEqual(put.readiness_state_on_property, [514]); assert.strictEqual(put.products.length, 4, 'the deleted product is not in the body'); assert(put.products.every(p => p.sku === SKU));
  const W2 = world(); await W2.run(); assert(!('readiness_state_on_property' in W2.S.puts[0].body));
});
test('the self-check stops a body that differs in anything else: nothing is sent, no safety copy, no price or stock ever moves', async () => {
  const W = world({ sim: { shape: g => { g.products[3].offerings[0].price = null; } } });
  const r = await W.run(); assert.strictEqual(r.ok, false); assert.strictEqual(r.reason, 'internal'); assert.strictEqual(W.S.puts.length, 0); assert.strictEqual(fileWrites.length, 0, 'no safety copy either');
  assert(r.details && r.details.some(x => /price unreadable/.test(x)), JSON.stringify(r));
  const W2 = world({ sim: { shape: g => { g.products[2].offerings = []; } } }); const r2 = await W2.run(); assert.strictEqual(r2.reason, 'internal'); assert.strictEqual(W2.S.puts.length, 0);
});


/* ═══ EDITSKU (Paul, 26 Oct 2026): the order window's Edit SKU writes ONE listing, and only the products that carry the line's Etsy SKU ═══ */
test('Edit SKU (onlySku): only the products of THIS listing that carry the line\'s Etsy SKU take the new one, the rest keep theirs; another SKU of it later is read live; an SKU it does not carry changes nothing', async () => {
  const W = world({ inventory: byMetal() }), r = await W.run({ listingId: LID, sku: SKU, onlySku: 'swan_ss', by: 'Paul' });
  assert.strictEqual(r.ok, true, JSON.stringify(r)); assert.strictEqual(r.state, 'linked'); assert.strictEqual(W.S.puts.length, 1, 'exactly one PUT');
  const skus = stripIds(W.S.stored).products.map(p => p.sku);
  assert.deepStrictEqual(skus.filter(x => x === SKU).length, 3, 'the 3 SS products take it'); assert.deepStrictEqual(skus.filter(x => x === 'SWAN_GF').length, 3); assert.deepStrictEqual(skus.filter(x => x === 'SWAN_14K').length, 3, 'GF and 14K keep their own');
  assert(W.S.calls.every(c => c.path.startsWith('/listings/' + LID)), 'only this listing is read and written: ' + W.S.calls.map(c => c.path).join(', '));
  assert(blobs.has(r.beforePath), 'the listing as it was is kept');
  assert.strictEqual(r.changed, 3, 'the answer says how many products changed');
  // a later Edit SKU of the same listing's GF line: the record says linked, but the listing is read again and only the GF products change
  const g = await W.run({ listingId: LID, sku: SKU, onlySku: 'SWAN_GF', by: 'Paul' }); assert.strictEqual(g.ok, true, JSON.stringify(g)); assert.strictEqual(W.S.puts.length, 2);
  assert.deepStrictEqual(stripIds(W.S.stored).products.map(p => p.sku).filter(x => x === 'SWAN_14K').length, 3, '14K still kept'); assert.strictEqual(stripIds(W.S.stored).products.filter(p => p.sku === SKU).length, 6);
  // the same edit again (an older order of the listing still has the old SKU): nothing left to change, no PUT
  const a = await W.run({ listingId: LID, sku: SKU, onlySku: 'SWAN_GF', by: 'Paul' }); assert.strictEqual(a.ok, true, JSON.stringify(a)); assert.strictEqual(W.S.puts.length, 2, 'no PUT');
  // an Etsy SKU the listing does not carry: nothing is written
  const F = world({ inventory: byMetal() }), n = await F.run({ listingId: LID, sku: SKU, onlySku: 'NOT_ON_IT', by: 'Paul' }); assert.strictEqual(n.ok, false); assert.strictEqual(n.reason, 'from-sku-missing', JSON.stringify(n)); assert.strictEqual(F.S.puts.length, 0, 'no PUT');
});

/* ═══ 2 · what is on the listing today ═══ */
test('products that carry SEVERAL different SKUs are REPLACED (Paul, 13 Oct): every product takes the SKU, only the sku field moves, the answer names what was replaced, the before-copy keeps the old SKUs, 2 calls', async () => {
  const W = world({ inventory: byMetal() }), r = await W.run();
  assert.strictEqual(r.ok, true, JSON.stringify(r)); assert.strictEqual(r.state, 'linked'); assert.strictEqual(r.calls, 2); assert.strictEqual(r.products, 9); assert.strictEqual(r.verified, true);
  assert.deepStrictEqual(r.replaced, [{ sku: 'SWAN_14K', n: 3 }, { sku: 'SWAN_GF', n: 3 }, { sku: 'SWAN_SS', n: 3 }], 'what the products carried, biggest group first (ties by name)'); assert(!('replacedMore' in r));
  assert.strictEqual(W.S.puts.length, 1, 'exactly one PUT'); const put = W.S.puts[0].body;
  assert(put.products.every(p => p.sku === SKU), 'every product takes the SKU');
  // the PUT is the read with only the sku field changed: with the sku removed from both they are equal (the lists may only be widened, as they always were)
  const noSku = b => { const c = clone(b); c.products.forEach(p => delete p.sku); delete c.sku_on_property; return c; };
  assert.deepStrictEqual(noSku(put), noSku(expectedBody(W.S.lastGet, () => true, SKU)), 'body = read, sku aside');
  assert.deepStrictEqual(put.sku_on_property, [513, 514], 'sku_on_property [513] widened to every property (Etsy refuses a partial list once any list names them all); no value changes');
  assert.deepStrictEqual(stripIds(W.S.stored).products.map(p => p.sku), Array(9).fill(SKU)); assert.strictEqual(W.S.stored.products.length, 9);
  // the old SKUs are in the safety copy, which was written before the PUT, and on the record
  const saved = JSON.parse(blobs.get(r.beforePath).buf.toString('utf8')); assert.deepStrictEqual(saved, W.S.lastGet, 'the before-copy is the whole read');
  assert.deepStrictEqual([...new Set(saved.products.map(p => p.sku))].sort(), ['SWAN_14K', 'SWAN_GF', 'SWAN_SS'], 'the three old SKUs are in the safety copy');
  const l = W.link(); assert.strictEqual(l.state, 'linked'); assert.deepStrictEqual(l.replaced, r.replaced); assert(!('replacedMore' in l));
  assert.strictEqual(Page.wording(r, LID, SKU).text, 'Done: BEADY SWAN is now on all 9 products of listing ' + LID + '. It replaced 3 x SWAN_14K, 3 x SWAN_GF and 3 x SWAN_SS; the listing as it was is kept.');
  // a second press: no Etsy call, the same answer with the same words
  const n0 = W.S.calls.length, again = await W.run(); assert.strictEqual(again.ok, true); assert.strictEqual(again.calls, 0); assert.strictEqual(W.S.calls.length, n0); assert.deepStrictEqual(again.replaced, r.replaced);
  assert.strictEqual(Page.wording(again, LID, SKU).text, Page.wording(r, LID, SKU).text);
  // a SKU a person set on some products (the target on a few, the old one on the rest): both sets are replaced; the products already carrying the target stay as they are
  const inv = beady(); inv.products.slice(0, 4).forEach(p => { p.sku = SKU; });
  const W2 = world({ inventory: inv }), r2 = await W2.run(); assert.strictEqual(r2.ok, true, JSON.stringify(r2)); assert.deepStrictEqual(r2.replaced, [{ sku: OLD, n: 5 }, { sku: SKU, n: 4 }]); assert(W2.S.stored.products.every(p => p.sku === SKU));
  // blanks and several real SKUs together: the blank is its own group, written ""
  const mix = mkInv([{ sku: '', pv: [[513, 'Metal', 'GF']] }, { sku: 'A_ONE', pv: [[513, 'Metal', 'SS']] }, { sku: 'B_TWO', pv: [[513, 'Metal', 'RG']] }, { sku: 'b_two', pv: [[513, 'Metal', 'XX']] }], { price_on_property: [513], sku_on_property: [513] });
  const W3 = world({ inventory: mix }), r3 = await W3.run(); assert.strictEqual(r3.ok, true, JSON.stringify(r3)); assert.deepStrictEqual(r3.replaced, [{ sku: 'B_TWO', n: 2 }, { sku: '', n: 1 }, { sku: 'A_ONE', n: 1 }].sort((a, b) => b.n - a.n || (a.sku < b.sku ? -1 : 1)), 'case set aside: B_TWO and b_two are one group');
  assert.strictEqual(W3.S.puts.length, 1); assert(W3.S.stored.products.every(p => p.sku === SKU));
  assert(/\(no SKU\)/.test(Page.wording(r3, LID, SKU).text), Page.wording(r3, LID, SKU).text);
});
test('one SKU (or none) on every product: the answer is exactly what it always was (no `replaced`, no new words); a deleted product does not count as a second SKU', async () => {
  const W = world(), r = await W.run(); assert.strictEqual(r.ok, true); assert(!('replaced' in r) && !('replacedMore' in r), JSON.stringify(r)); assert(!('replaced' in W.link()));
  assert.strictEqual(Page.wording(r, LID, SKU).text, 'Done: BEADY SWAN is now on all 9 products of listing ' + LID + '.');
  const inv = beady(); inv.products.push({ product_id: 998, sku: 'GONE_ONE', is_deleted: true, property_values: [], offerings: [{ offering_id: 1, price: 5, quantity: 1, is_enabled: true }] });
  const D = world({ inventory: inv }), d = await D.run(); assert.strictEqual(d.ok, true); assert(!('replaced' in d), 'a deleted product is not a SKU the listing carries');
  const blank = mkInv([{ sku: '', pv: [[513, 'Metal', 'GF']] }, { sku: '', pv: [[513, 'Metal', 'SS']] }], { price_on_property: [513] });
  const B = world({ inventory: blank }), b = await B.run(); assert.strictEqual(b.ok, true); assert(!('replaced' in b));
  // more than 12 different SKUs: the first 12 groups and the number of products in the rest
  const many = mkInv(Array.from({ length: 15 }, (_, i) => ({ sku: 'MANY_' + String(i + 1).padStart(2, '0'), pv: [[513, 'Metal', 'V' + i]] })).concat([{ sku: 'MANY_01', pv: [[513, 'Metal', 'V99']] }]), { price_on_property: [513], sku_on_property: [513] });
  const M = world({ inventory: many }), m = await M.run(); assert.strictEqual(m.ok, true, JSON.stringify(m)); assert.strictEqual(m.replaced.length, 12); assert.deepStrictEqual(m.replaced[0], { sku: 'MANY_01', n: 2 }); assert.strictEqual(m.replacedMore, 3, 'the three smaller groups');
  assert.strictEqual(M.link().replacedMore, 3); assert(/ and 3 x other SKUs;/.test(Page.wording(m, LID, SKU).text), Page.wording(m, LID, SKU).text);
});
test('already on Etsy (a person set it): no write, no safety copy, no PUT', async () => {
  const W = world({ inventory: beady(SKU) }), r = await W.run();
  assert.strictEqual(r.ok, true); assert.strictEqual(r.state, 'linked'); assert.strictEqual(r.already, true); assert.strictEqual(r.calls, 1); assert.strictEqual(W.S.puts.length, 0); assert.strictEqual(fileWrites.length, 0);
  assert.strictEqual(W.link().state, 'linked'); assert.strictEqual(W.link().already, true);
});
test('refused before any Etsy call: not active (the mirror), sandbox, bad requests; a missing listing; a listing with no products', async () => {
  const W = world({ mirror: 'draft' }), r = await W.run(); assert.strictEqual(r.reason, 'listing-state'); assert.strictEqual(r.text, 'the listing is draft on Etsy'); assert.strictEqual(r.calls, 0); assert.strictEqual(W.S.calls.length, 0);
  const G = world({ mirror: false, sim: { state: 'expired' } }), g = await G.run(); assert.strictEqual(g.reason, 'listing-state'); assert.strictEqual(g.calls, 1, 'one GET of the listing, then stop'); assert.strictEqual(G.S.puts.length, 0);
  const S = world(); writes.length = 0;
  for (const body of [{ listingId: LID, sku: SKU, sandbox: true }, { listingId: LID, sku: SKU, sandbox: '1' }]) { const s = await S.run(body); assert.strictEqual(s.reason, 'sandbox'); assert.strictEqual(s.calls, 0); }
  assert.strictEqual(S.S.calls.length, 0); assert.strictEqual(writes.length, 0); assert.strictEqual(fileWrites.length, 0);
  for (const body of [null, {}, { listingId: LID }, { sku: SKU }, { listingId: 'abc', sku: SKU }, { listingId: '12', sku: SKU }, { listingId: LID, sku: 42 }, { listingId: LID, sku: '' }]) { const b = await S.mod.listingEtsySkuWrite(body); assert.strictEqual(b.reason, 'bad-request', JSON.stringify(body)); assert.strictEqual(b.calls, 0); }
  assert.strictEqual(S.S.calls.length, 0);
  const N = world({ mirror: false, sim: { fail: c => (c.path === '/listings/' + LID ? etsyErr(404, 'gone') : null) } }), n = await N.run(); assert.strictEqual(n.reason, 'listing-gone'); assert.strictEqual(N.S.puts.length, 0);
  const E = world({ inventory: { products: [], price_on_property: [], quantity_on_property: [], sku_on_property: [] } }), e = await E.run(); assert.strictEqual(e.reason, 'no-products'); assert.strictEqual(E.S.puts.length, 0);
});
test('an Etsy listing link is a listing: Paul\'s link with its transaction id works; the id inside it is the one written', async () => {
  const link = 'https://www.etsy.com/listing/' + LID + '/swan-charm-necklace-gold-swan-bird?transaction_id=5247484637';
  assert.strictEqual(Mod.internals.listingIdOf(link), LID); assert.strictEqual(Mod.internals.listingIdOf(' ' + LID + ' '), LID); assert.strictEqual(Mod.internals.listingIdOf('https://www.etsy.com/uk/listing/' + LID), LID);
  assert.strictEqual(Mod.internals.listingIdOf('https://www.etsy.com/shop/Foo'), ''); assert.strictEqual(Mod.internals.listingIdOf('12'), '');
  const W = world(), r = await W.run({ listingId: link, sku: SKU, by: 'Paul' }); assert.strictEqual(r.ok, true, JSON.stringify(r)); assert.strictEqual(r.listingId, LID);
  assert(W.S.calls.every(c => c.path.startsWith('/listings/' + LID)));
});

/* ═══ 3 · the SKU: exactly the master's, and one Etsy keeps as written ═══ */
test('the master must hold EXACTLY that SKU: another spelling, a look-alike, a design that is not there, a shell without a SKU, a blocked entry: refused, no Etsy call, nothing written', async () => {
  const W = world({ master: { 'BLOCKED ONE': Object.assign(masterEntry('BLOCKED ONE'), { blocked: 'also in another master file' }), 'SHELL ONLY': { upAngle: 10 } } }); writes.length = 0;
  for (const [sku, reason] of [['BEADY_SWAN', 'sku-not-in-master'], ['BEADY SWAN 2', 'sku-not-in-master'], ['SWAN', 'sku-not-in-master'], ['SHELL ONLY', 'sku-not-in-master'], ['BLOCKED ONE', 'sku-blocked'], ['beady swan', 'sku-shape'], ['Beady Swan', 'sku-shape']]) {
    const r = await W.run({ listingId: LID, sku, by: 'Paul' }); assert.strictEqual(r.reason, reason, sku + ' -> ' + JSON.stringify(r)); assert.strictEqual(r.calls, 0);
  }
  assert.strictEqual(W.S.calls.length, 0); assert.strictEqual(writes.length, 0); assert.strictEqual(fileWrites.length, 0);
  assert.strictEqual(Object.keys(store.get('Charm_Master_Index/BEADY SWAN')).length, Object.keys(masterEntry('x')).length, 'the master entry is never touched');
});
test('a master SKU that Etsy might trim or refuse is stopped before any call: a space at an end, two spaces, a tab, a non-breaking space, more than 32 characters; exactly 32 is fine', async () => {
  const odd = { 'TRAILING SPACE ': masterEntry('TRAILING SPACE '), 'DOUBLE  SPACE': masterEntry('DOUBLE  SPACE'), 'TAB\tONE': masterEntry('TAB\tONE'), ['X'.repeat(33)]: masterEntry('X'.repeat(33)), ['X'.repeat(32)]: masterEntry('X'.repeat(32)) };
  const W = world({ master: odd });
  for (const sku of ['TRAILING SPACE ', ' LEADING', 'DOUBLE  SPACE', 'TAB\tONE', 'NBSP ONE', 'X'.repeat(33), 'CAFEÉ']) {
    const r = await W.run({ listingId: LID, sku, by: 'Paul' }); assert.strictEqual(r.reason, 'sku-shape', JSON.stringify(sku)); assert.strictEqual(r.calls, 0); assert(/Etsy would not keep that SKU/.test(r.text) && r.details.length === 1, r.text);
  }
  assert.strictEqual(W.S.calls.length, 0); assert.strictEqual(fileWrites.length, 0);
  const ok = await W.run({ listingId: LID, sku: 'X'.repeat(32), by: 'Paul' }); assert.strictEqual(ok.ok, true, JSON.stringify(ok)); assert.strictEqual(W.S.puts[0].body.products[0].sku.length, 32);
});
test('CATSKU: three master names read alike ("CAT 1 - SITTING CAT", "CAT 1- SITTING CAT", "... (HUGGIE)"): only the EXACT key is written, on all 12 products (4 metals x 3 Charm Types), nothing else moves; long-dash look-alikes are refused; an Etsy that hands back the old spelling is a verify failure', async () => {
  const CAT = 'CAT 1 - SITTING CAT', TWIN = 'CAT 1- SITTING CAT', HUG = 'CAT 1- SITTING CAT (HUGGIE)', OLDC = 'CAT 1- Sitting Cat';
  assert(/^[\x20-\x7e]+$/.test(CAT) && CAT.length === 19 && CAT.charCodeAt(5) === 0x20 && CAT.charCodeAt(6) === 0x2d && CAT.charCodeAt(7) === 0x20, 'the key is plain ASCII: a hyphen-minus with a space on each side');
  const master = { [CAT]: masterEntry(CAT), [TWIN]: masterEntry(TWIN), [HUG]: masterEntry(HUG) };
  const grid = (sku) => mkInv(['Sterling Silver', 'Gold Filled', 'Rose Gold Filled', '14k Solid Gold'].flatMap((m, i) => ['Necklace CHARM', 'Huggie CHARM SET', 'CHARM + Engraving'].map((t, j) => ({ sku, pv: [[514, 'Metal Choice', m], [513, 'Charm Type', t]], price: [12, 18, 22, 48][i] + [0, 9, 4][j], qty: i === 3 && j === 2 ? 0 : 999, on: !(i === 3 && j === 2) }))), { price_on_property: [514, 513] });
  const W = world({ inventory: grid(OLDC), master });
  assert.strictEqual(W.inv.products.length, 12);
  const r = await W.run({ listingId: LID, sku: CAT, by: 'Paul' }); assert.strictEqual(r.ok, true, JSON.stringify(r));
  assert.strictEqual(r.products, 12); assert.strictEqual(r.calls, 2); assert.strictEqual(r.sku, CAT);
  assert.strictEqual(W.S.puts.length, 1); assert.deepStrictEqual(W.S.puts[0].body, expectedBody(W.S.lastGet, () => true, CAT), 'the PUT is the GET answer with every sku the exact key');
  assert(W.S.puts[0].body.products.every(p => p.sku === CAT), 'the key is sent with its spaces and its hyphen as stored');
  const exp = clone(W.inv); exp.products.forEach(p => { p.sku = CAT; }); assert.deepStrictEqual(stripIds(W.S.stored), stripIds(exp), 'what Etsy then stores differs from before only in the SKUs');
  assert.deepStrictEqual(store.get('Charm_Listing_Skus/' + LID).uni, CAT); assert.strictEqual(W.link().products, 12);
  assert(!writes.some(w => /Charm_Master_Index/.test(w.path)), 'no master entry is ever written');
  assert.strictEqual(r.beforePath, 'charmnest/custom-saves/' + LID + '/link-CAT_1_SITTING_CAT-before.json', 'the safety copy is named by the SKU with its punctuation set aside (both look-alikes would give this name: a second copy never overwrites the first)');
  assert.strictEqual(Page.wording(r, LID, CAT).text, 'Done: CAT 1 - SITTING CAT is now on all 12 products of listing ' + LID + '.', 'the words Paul is told to expect');
  // the other look-alike names are real masters too, so the op takes them when asked: the card's chip always sends ITS key. Spellings that are no master (en dash, em dash, minus sign, two spaces, lower case) never leave
  const Z = world({ master });
  for (const [sku, reason] of [['CAT 1 \u2013 SITTING CAT', 'sku-shape'], ['CAT 1 \u2014 SITTING CAT', 'sku-shape'], ['CAT 1 \u2212 SITTING CAT', 'sku-shape'], ['CAT 1  -  SITTING CAT', 'sku-shape'], ['cat 1 - sitting cat', 'sku-shape'], ['CAT 1 - SITTING CAT ', 'sku-shape'], ['CAT 1-SITTING CAT', 'sku-not-in-master'], ['CAT 1 -SITTING CAT', 'sku-not-in-master'], ['CAT 1 - SITTING CAT (HUGGIE)', 'sku-not-in-master']]) {
    const z = await Z.run({ listingId: LID, sku, by: 'Paul' }); assert.strictEqual(z.reason, reason, JSON.stringify(sku) + ' -> ' + JSON.stringify(z)); assert.strictEqual(z.calls, 0);
  }
  assert.strictEqual(Z.S.calls.length, 0, 'no Etsy call for any of them');
  // Etsy hands back the old spelling (or any other text): the comparison is exact, so it is a verify failure with the safety copy; one PUT, no undo
  for (const tr of [s => s.replace(' - ', '- '), s => s.replace(' - ', ' \u2013 '), s => s.replace(' - ', '-')]) {
    const V = world({ inventory: grid(OLDC), master, sim: { skuTransform: tr } }), v = await V.run({ listingId: LID, sku: CAT, by: 'Paul' });
    assert.strictEqual(v.reason, 'verify'); assert.strictEqual(V.S.puts.length, 1); assert(blobs.has(v.beforePath)); assert(v.details.some(x => /SKU/.test(x)), JSON.stringify(v.details));
  }
  // the products carry two SKUs (the huggie products under their own name): both are replaced by the exact key, the old ones named and kept in the safety copy
  const mixed = grid(OLDC); mixed.products.forEach(p => { if (p.property_values[1].values[0] === 'Huggie CHARM SET') p.sku = HUG; }); mixed.sku_on_property = [513];
  const M = world({ inventory: mixed, master }), m = await M.run({ listingId: LID, sku: CAT, by: 'Paul' }); assert.strictEqual(m.ok, true, JSON.stringify(m)); assert.deepStrictEqual(m.replaced, [{ sku: OLDC, n: 8 }, { sku: HUG, n: 4 }]); assert.strictEqual(M.S.puts.length, 1); assert(M.S.stored.products.every(p => p.sku === CAT));
  // every product already carries the exact key: no write
  const A = world({ inventory: grid(CAT), master }), a = await A.run({ listingId: LID, sku: CAT, by: 'Paul' }); assert.strictEqual(a.already, true); assert.strictEqual(A.S.puts.length, 0);
});
test('Etsy keeps a DIFFERENT SKU than it was sent (it trims, or drops a space): recorded as verify with the before-copy, ONE PUT, no undo, and never sent again unchanged', async () => {
  for (const tr of [s => s.replace(/ /g, ''), s => s.toLowerCase(), s => s.slice(0, 5)]) {
    const W = world({ sim: { skuTransform: tr } }), r = await W.run();
    assert.strictEqual(r.ok, false); assert.strictEqual(r.reason, 'verify'); assert.strictEqual(r.retry, false); assert(/did not keep the new SKU as it was sent/.test(r.text), r.text); assert(r.details.some(x => /SKU/.test(x)), JSON.stringify(r.details));
    assert.strictEqual(W.S.puts.length, 1, 'one PUT'); assert.strictEqual(r.calls, 2); assert(blobs.has(r.beforePath), 'the before-copy is named and kept: ' + r.beforePath);
    assert.strictEqual(W.link().state, 'failed'); assert.strictEqual(W.link().reason, 'verify');
    const n0 = W.S.calls.length, again = await W.run(); assert.strictEqual(again.reason, 'verify'); assert.strictEqual(again.calls, 0); assert.strictEqual(W.S.calls.length, n0, 'no Etsy call on a press after a verify failure');
  }
});

/* ═══ 4 · the safety copy first ═══ */
test('the before-copy (the exact GET answer) is in Storage and noted on the record BEFORE the PUT', async () => {
  let atPut = null;
  const W = world();
  const realPut = W.E.api.put; W.E.api.put = async (p, q, b) => { const l = W.link(); atPut = { state: l.state, path: l.beforePath, hash: l.beforeHash, blob: l.beforePath && blobs.has(l.beforePath) ? blobs.get(l.beforePath).buf.toString('utf8') : null, saves: fileWrites.length, running: l.runningUntil }; return realPut(p, q, b); };
  const mod = Mod.make({ db, FV, bucket, etsy: W.E.api, meter: { bumpSimple() {} }, now: () => 1760000000000 + (++tick) });
  const r = await mod.listingEtsySkuWrite({ listingId: LID, sku: SKU, by: 'Paul' }); assert.strictEqual(r.ok, true, JSON.stringify(r));
  assert(atPut, 'the PUT happened'); assert.strictEqual(atPut.state, 'writing', 'the record said "writing" when the PUT left'); assert(atPut.running > 0, 'and held the lease');
  assert.strictEqual(atPut.path, 'charmnest/custom-saves/' + LID + '/link-BEADY_SWAN-before.json'); assert.strictEqual(r.beforePath, atPut.path);
  assert.strictEqual(atPut.saves, 1, 'one file written before the PUT');
  assert.deepStrictEqual(JSON.parse(atPut.blob), W.S.lastGet, 'the saved copy is the GET answer, whole');
  assert.strictEqual(require('crypto').createHash('sha256').update(atPut.blob).digest('hex'), atPut.hash);
  const l = W.link(); assert.strictEqual(l.beforePath, atPut.path); assert.strictEqual(l.attempts, 1); assert.deepStrictEqual(l.backups, [atPut.path]); assert.strictEqual(l.products, 9);
  assert.deepStrictEqual(fileWrites.map(f => f.path), [atPut.path], 'nothing else was written to Storage');
});
test('if the before-copy cannot be written, kept, or noted on the record, NOTHING is sent', async () => {
  const brokenBucket = how => ({ name: 'x', file: p => { const f = bucket.file(p); return Object.assign({}, f, { save: async (...a) => { if (how === 'throws') throw new Error('storage is down'); if (how === 'lies') return; return f.save(...a); } }); } });
  for (const how of ['throws', 'lies']) {
    const W = world({ bucket: brokenBucket(how) }), r = await W.run();
    assert.strictEqual(r.ok, false); assert.strictEqual(r.reason, 'backup-failed'); assert.strictEqual(r.text, 'the safety copy could not be saved'); assert.strictEqual(r.retry, true);
    assert.strictEqual(W.S.puts.length, 0, how + ': no PUT'); assert.strictEqual(W.link().state, 'failed'); assert.strictEqual(W.link().reason, 'backup-failed');
  }
  // the record cannot be updated to "writing"
  const flaky = Object.assign({}, db, { collection: c => { const col = db.collection(c); return Object.assign({}, col, { doc: id => { const d = col.doc(id); return Object.assign({}, d, { update: async data => { if (data && data.link && data.link.state === 'writing') throw new Error('firestore is down'); return d.update(data); } }); } }); } });
  const W = world({ db: flaky }), r = await W.run(); assert.strictEqual(r.reason, 'backup-failed'); assert.strictEqual(W.S.puts.length, 0, 'the copy is not noted on the record: no PUT');
});
test('a before-copy is never overwritten: a second attempt writes -before-2.json and the first stays as it was', async () => {
  const W = world({ sim: { fail: c => (c.method === 'PUT' && W.S.puts.length === 1 ? etsyErr(504, 'gateway timeout') : null) } });
  const r1 = await W.run(); assert.strictEqual(r1.state, 'unverified'); const first = r1.beforePath, firstText = blobs.get(first).buf.toString('utf8');
  const r2 = await W.run(); assert.strictEqual(r2.ok, true, JSON.stringify(r2)); assert.strictEqual(r2.beforePath, first.replace('-before.json', '-before-2.json'));
  assert.strictEqual(blobs.get(first).buf.toString('utf8'), firstText, 'the first copy is untouched'); assert.deepStrictEqual(W.link().backups, [first, r2.beforePath]); assert.strictEqual(W.link().attempts, 2);
});

/* ═══ 5 · what Etsy answers ═══ */
test('401/403 on the PUT: "needs listings_w", ONE PUT per press, no loop; after a re-authorisation the next press writes once', async () => {
  let deny = true;
  const W = world({ sim: { fail: c => (c.method === 'PUT' && deny ? etsyErr(403, 'not allowed') : null) } });
  const r = await W.run(); assert.strictEqual(r.ok, false); assert.strictEqual(r.reason, 'scope'); assert.strictEqual(r.needs, 'listings_w'); assert.strictEqual(r.retry, true); assert.strictEqual(r.calls, 2); assert(/listings_w/.test(r.text));
  assert.strictEqual(W.S.puts.length, 1, 'one PUT, no retry loop'); assert.strictEqual(W.link().state, 'failed'); assert.strictEqual(W.link().putSent, false, 'Etsy refused it: nothing was applied'); assert.strictEqual(W.S.stored.products[0].sku, OLD);
  deny = false; const again = await W.run(); assert.strictEqual(again.ok, true, JSON.stringify(again)); assert.strictEqual(W.S.puts.length, 2, 'one PUT for each press'); assert.strictEqual(again.beforePath, r.beforePath.replace('-before.json', '-before-2.json'));
  const G = world({ sim: { fail: c => (c.path.endsWith('/inventory') && c.method === 'GET' ? etsyErr(401, 'bad token') : null) } }), g = await G.run(); assert.strictEqual(g.reason, 'scope'); assert.strictEqual(g.calls, 1); assert.strictEqual(G.S.puts.length, 0); assert.strictEqual(fileWrites.length, 0);
});
test('Etsy errors: each a plain reason, the right state and retry; a refused body is recorded and NOT sent again unchanged', async () => {
  const put = (status, error) => ({ sim: { fail: c => (c.method === 'PUT' ? etsyErr(status, error) : null) } });
  const cases = [[429, 'slow down', 'busy', true, 'failed'], [404, 'gone', 'listing-gone', false, 'failed'], [400, 'products malformed', 'rejected', false, 'failed'], [500, 'oops', 'etsy-down', true, 'failed'], [502, 'bad gateway', 'etsy-down', true, 'unverified'], [504, 'timeout', 'etsy-down', true, 'unverified']];
  for (const [status, error, reason, retry, state] of cases) {
    const W = world(put(status, error)), r = await W.run();
    assert.strictEqual(r.reason, reason, status + ' ' + JSON.stringify(r)); assert.strictEqual(r.retry, retry, String(status)); assert.strictEqual(r.state, state, String(status)); assert.strictEqual(W.S.puts.length, 1); assert.strictEqual(r.calls, 2);
    assert.strictEqual(W.S.stored.products[0].sku, OLD, 'nothing applied');
    if (reason === 'rejected') {
      assert.deepStrictEqual(r.details, ['products malformed']); const n0 = W.S.calls.length, again = await W.run();
      assert.strictEqual(again.reason, 'rejected'); assert.strictEqual(again.calls, 0); assert.strictEqual(W.S.calls.length, n0, 'a refused body is not sent again unchanged');
    }
  }
  const D = world({ sim: { fail: c => (c.method === 'GET' && c.path.endsWith('/inventory') ? Object.assign(new Error('socket hang up'), { code: 'ECONNRESET' }) : null) } }), d = await D.run(); assert.strictEqual(d.reason, 'offline'); assert.strictEqual(D.S.puts.length, 0);
});
test('timeouts: a hung GET is "Etsy did not answer" with no PUT; a hung PUT is "unverified" and the next press only looks first', async () => {
  const G = world({ timeoutMs: 40, sim: { hang: c => c.method === 'GET' && c.path.endsWith('/inventory') } }), g = await G.run(); assert.strictEqual(g.reason, 'etsy-down'); assert.strictEqual(G.S.puts.length, 0);
  const P = world({ timeoutMs: 40, sim: { hang: c => c.method === 'PUT' } }), p = await P.run(); assert.strictEqual(p.state, 'unverified'); assert.strictEqual(p.reason, 'etsy-down'); assert.strictEqual(P.S.puts.length, 1); assert.strictEqual(P.link().putSent, true);
});
test('unverified: the PUT landed but Etsy gave no whole inventory back: the next press reads first, finds the SKU there, verifies it against the before-copy and sends NO second PUT', async () => {
  const W = world({ sim: { echo: 'empty' } }), r = await W.run(); assert.strictEqual(r.state, 'unverified'); assert.strictEqual(r.reason, 'unverified'); assert.strictEqual(W.S.puts.length, 1); assert.strictEqual(W.S.stored.products[0].sku, SKU);
  const n0 = W.S.calls.length, again = await W.run();
  assert.strictEqual(again.ok, true, JSON.stringify(again)); assert.strictEqual(again.verified, true); assert.strictEqual(again.calls, 1, 'one GET of the inventory, nothing else'); assert.strictEqual(W.S.calls.length - n0, 1); assert.strictEqual(W.S.puts.length, 1, 'no second PUT');
  assert.strictEqual(W.link().state, 'linked');
});
test('unverified: the PUT did not land (504): the next press sends once more ONLY because the listing is exactly as the before-copy was; three tries is the end', async () => {
  const W = world({ sim: { fail: c => (c.method === 'PUT' ? etsyErr(504, 'gateway timeout') : null) } });
  const a = await W.run(); assert.strictEqual(a.state, 'unverified'); const b = await W.run(); assert.strictEqual(b.state, 'unverified'); assert.strictEqual(b.calls, 2, 'inventory + PUT (no listing GET: an earlier try already looked)'); const c = await W.run(); assert.strictEqual(c.state, 'unverified');
  assert.strictEqual(W.S.puts.length, 3); assert.strictEqual(W.link().attempts, 3);
  const n0 = W.S.calls.length, d = await W.run(); assert.strictEqual(d.reason, 'attempts'); assert.strictEqual(d.calls, 0); assert.strictEqual(W.S.calls.length, n0);
});
test('changed by someone else after an unconfirmed try: the listing is neither as before nor as wanted: nothing is sent', async () => {
  const W = world({ sim: { fail: c => (c.method === 'PUT' ? etsyErr(504, 'gateway timeout') : null) } });
  await W.run(); W.S.stored.products[0].offerings[0].price += 3;   // a person changed a price on Etsy meanwhile
  const r = await W.run(); assert.strictEqual(r.ok, false); assert.strictEqual(r.reason, 'changed'); assert.strictEqual(r.calls, 1); assert.strictEqual(W.S.puts.length, 1, 'no second PUT');
  const V = world({ sim: { fail: c => (c.method === 'PUT' ? etsyErr(504, 'gateway timeout') : null) } });
  await V.run(); V.S.stored.products.forEach(p => { p.sku = 'SET_BY_HAND'; });   // a person set another SKU meanwhile: the listing is not as before
  const v = await V.run(); assert.strictEqual(v.reason, 'changed'); assert.strictEqual(V.S.puts.length, 1);
});
test('verify: an answer that is not what was sent is recorded with the details and the before-copy, with ONE PUT, no undo, and nothing sent again', async () => {
  const cases = {
    'a price moved': { mutate: s => { s.products[0].offerings[0].price += 5; }, text: /not what was sent/, detail: /price/ },
    'stock appeared': { mutate: s => { s.products[1].offerings[0].quantity = 99; }, text: /not what was sent/, detail: /quantity/ }
  };
  for (const [what, c] of Object.entries(cases)) {
    const W = world({ sim: { mutate: c.mutate } }), r = await W.run();
    assert.strictEqual(r.ok, false, what); assert.strictEqual(r.reason, 'verify', what + JSON.stringify(r)); assert(c.text.test(r.text), r.text); assert(r.details.some(x => c.detail.test(x)), what + ' ' + JSON.stringify(r.details));
    assert.strictEqual(W.S.puts.length, 1, what + ': ONE PUT, no undo'); assert(blobs.has(r.beforePath), 'the before-copy is kept'); assert.strictEqual(W.link().state, 'failed'); assert.strictEqual(W.link().reason, 'verify');
    const n0 = W.S.calls.length, again = await W.run(); assert.strictEqual(again.reason, 'verify'); assert.strictEqual(W.S.calls.length, n0, what + ': a later press makes no Etsy call');
  }
});
test('a product dropped by Etsy after the PUT: the echo has the wrong count (unverified); the next press reads the listing, compares it with the before-copy (verify), sends NO second PUT', async () => {
  const W = world({ sim: { mutate: s => { s.products.pop(); } } }), r = await W.run(); assert.strictEqual(r.state, 'unverified'); assert.strictEqual(W.S.puts.length, 1);
  const again = await W.run(); assert.strictEqual(again.ok, false); assert.strictEqual(again.reason, 'verify'); assert(again.details.some(x => /Missing/.test(x)), JSON.stringify(again.details)); assert.strictEqual(W.S.puts.length, 1, 'no second PUT'); assert.strictEqual(again.calls, 1);
});
test('the cap is a counter in the code: a run that would need a fourth call stops before it (here the cap is lowered to 2: no PUT)', async () => {
  const W = world({ mirror: false, maxCalls: 2 }), r = await W.run(); assert.strictEqual(r.ok, false); assert.strictEqual(r.calls, 2); assert.strictEqual(W.S.puts.length, 0);
  const X = world({ mirror: false, maxCalls: 99 }), x = await X.run(); assert.strictEqual(x.ok, true); assert.strictEqual(x.calls, 3, 'the code never lets a run make more than 3');
});

/* ═══ 6 · a double press sends once ═══ */
test('while the PUT is in flight the record holds a lease: a second press is refused with no Etsy call and no second PUT', async () => {
  let second = null, leaseSeen = null;
  const W = world({ sim: { during: async c => { if (c.method !== 'PUT') return; leaseSeen = W.link(); const n0 = W.S.calls.length; second = await W.mod.listingEtsySkuWrite({ listingId: LID, sku: SKU, by: 'Paul' }); second.extra = W.S.calls.length - n0; } } });
  const r = await W.run(); assert.strictEqual(r.ok, true, JSON.stringify(r));
  assert.strictEqual(leaseSeen.state, 'writing'); assert(leaseSeen.runningUntil > leaseSeen.at + 1000, 'the "writing" note still holds the lease');
  assert.strictEqual(second.ok, false); assert.strictEqual(second.reason, 'running'); assert.strictEqual(second.calls, 0); assert.strictEqual(second.extra, 0);
  assert.strictEqual(W.S.puts.length, 1); assert(!('runningUntil' in W.link()), 'released with the last write');
});
test('after "linked" a second press makes no Etsy call and no write; an expired lease is not a lease; another master SKU for the same listing is a new record that keeps a note of the old', async () => {
  const W = world(); const first = await W.run(); assert.strictEqual(first.ok, true);
  const callsBefore = W.S.calls.length; writes.length = 0; fileWrites.length = 0;
  const again = await W.run();
  assert.strictEqual(again.ok, true); assert.strictEqual(again.state, 'linked'); assert.strictEqual(again.calls, 0); assert.deepStrictEqual(again.productIds, first.productIds); assert.strictEqual(again.sku, SKU);
  assert.strictEqual(W.S.calls.length, callsBefore); assert.strictEqual(writes.length, 0); assert.strictEqual(fileWrites.length, 0);
  const L = world(); L.rec(); store.set('Charm_Custom_Saves/link-' + LID, { kind: 'listingSkuLink', listingId: LID, sku: SKU, link: { state: 'pending', runningUntil: Date.now() + 5e6 } });
  const rl = await L.run(); assert.strictEqual(rl.reason, 'running'); assert.strictEqual(rl.calls, 0); assert.strictEqual(L.S.calls.length, 0);
  const X = world(); store.set('Charm_Custom_Saves/link-' + LID, { kind: 'listingSkuLink', listingId: LID, sku: SKU, link: { state: 'pending', runningUntil: 5 } }); assert.strictEqual((await X.run()).ok, true);
  const Y = world(); await Y.run(); const other = await Y.run({ listingId: LID, sku: 'OTHER_ONE', by: 'Paul' });
  assert.strictEqual(other.ok, true, JSON.stringify(other)); assert.strictEqual(Y.rec().sku, 'OTHER_ONE'); assert.strictEqual(Y.rec().earlier.length, 1); assert.strictEqual(Y.rec().earlier[0].sku, SKU); assert.strictEqual(Y.rec().earlier[0].state, 'linked');
});

/* ═══ 7 · the door ═══ */
test('wiring: the op is behind the repo "modify" token; locked it answers 401 and touches nothing; unlocked it writes through the real etsyFetch with a made-up token (sandbox refused, 403 = needs listings_w)', async () => {
  let current = null; const requests = [];
  const res = (status, body) => ({ ok: status >= 200 && status < 300, status, headers: { get: () => undefined }, json: async () => body, text: async () => (typeof body === 'string' ? body : JSON.stringify(body)) });
  const fakeFetch = async (url, opt) => {
    const u = new URL(url); opt = opt || {};
    requests.push({ host: u.host, path: u.pathname, search: u.search, method: opt.method || 'GET', auth: opt.headers && opt.headers.Authorization, key: opt.headers && opt.headers['x-api-key'], body: opt.body });
    if (u.host !== 'api.etsy.com' || !u.pathname.startsWith('/v3/application/')) return res(500, 'unexpected host');
    if (!current) return res(500, 'no world');
    const p = u.pathname.slice('/v3/application'.length), q = u.searchParams.has('legacy') ? { legacy: u.searchParams.get('legacy') === 'true' } : undefined;
    try { return res(200, (opt.method === 'PUT') ? await current.api.put(p, q, JSON.parse(opt.body)) : await current.api.get(p, q)); }
    catch (e) { return res(e.status || 500, e.data || String(e.message)); }
  };
  const realLoad = Module._load;
  Fk.install();
  const installed = Module._load;
  Module._load = function (req, ...rest) {
    if (req === 'node-fetch') return fakeFetch;
    if (/[\/]_etsyApiMeter(\.js)?$/.test(req)) return { bump: () => ({ failNet() {}, fromHttp() {}, ok() {} }), bumpSimple() {}, flushNow: async () => {} };
    return installed.call(this, req, ...rest);
  };
  try {
    const lib = require(path.join(fnDir, 'charmNestLibrary.js')), RA = require(path.join(fnDir, '_charmNestRepoAuth.js')), stamp = require('./_repoToken.cjs');
    assert.strictEqual(lib.ops.listingEtsySkuWrite, Mod.ops.listingEtsySkuWrite, 'the Library\'s op table carries this module\'s op');
    assert.deepStrictEqual(RA.requirement('charmNestLibrary', 'listingEtsySkuWrite', {}), { op: 'listingEtsySkuWrite', need: 'modify' }, 'a repo "modify" op');
    const send = b => lib.handler({ httpMethod: 'POST', headers: {}, body: JSON.stringify(b), queryStringParameters: {} }).then(r => ({ status: r.statusCode, body: JSON.parse(r.body) }));
    const post = async b => send(await stamp('charmNestLibrary', b));
    const W = world(); current = W.E;
    store.set('config/etsyOauth', { access_token: 'made-up-token', refresh_token: 'made-up-refresh', expires_at_ms: Date.now() + 3600e3 });
    writes.length = 0;
    const body = { op: 'listingEtsySkuWrite', listingId: LID, sku: SKU, by: 'Paul' };
    let r = await send(body);
    assert.strictEqual(r.status, 401); assert.strictEqual(r.body.error, 'repo-locked'); assert.strictEqual(r.body.need, 'modify'); assert.strictEqual(requests.length, 0, 'locked: no Etsy call'); assert.strictEqual(fileWrites.length, 0);
    assert(!writes.some(w => /Charm_Custom_Saves/.test(w.path)), 'locked: no record written');
    r = await send(Object.assign({}, body, { repoToken: 'not-a-token' })); assert.strictEqual(r.status, 401); assert.strictEqual(requests.length, 0);
    r = await post(Object.assign({}, body, { sandbox: true }));
    assert.strictEqual(r.status, 200); assert.strictEqual(r.body.ok, false); assert.strictEqual(r.body.reason, 'sandbox'); assert.strictEqual(requests.length, 0);
    r = await post(body);
    assert.strictEqual(r.status, 200, JSON.stringify(r.body)); assert.strictEqual(r.body.ok, true, JSON.stringify(r.body)); assert.strictEqual(r.body.state, 'linked'); assert.strictEqual(r.body.calls, 2);
    assert.deepStrictEqual(requests.map(q => q.method + ' ' + q.path + q.search), ['GET /v3/application/listings/' + LID + '/inventory?legacy=false', 'PUT /v3/application/listings/' + LID + '/inventory?legacy=false'], 'the real etsyFetch, the right URLs');
    assert(requests.every(q => q.host === 'api.etsy.com' && q.auth === 'Bearer made-up-token' && q.key === 'test-key:test-secret'), 'only the made-up token and key');
    assert.deepStrictEqual(JSON.parse(requests[1].body), expectedBody(W.S.lastGet, () => true, SKU), 'the body that went over the wire');
    assert(W.S.stored.products.every(p => p.sku === SKU));
    r = await post(body); assert.strictEqual(r.body.state, 'linked'); assert.strictEqual(r.body.calls, 0); assert.strictEqual(requests.length, 2, 'again: no Etsy call');
    // a refusal from Etsy through the real helper keeps its status: 403 is "needs listings_w"
    const W2 = world({ sim: { fail: c => (c.method === 'PUT' ? etsyErr(403, 'not allowed') : null) } }); current = W2.E; requests.length = 0;
    r = await post(body);
    assert.strictEqual(r.status, 200); assert.strictEqual(r.body.reason, 'scope'); assert.strictEqual(r.body.needs, 'listings_w'); assert.strictEqual(requests.length, 2);
  } finally { Module._load = realLoad; }
});

let finished = false;
process.on('exit', () => { if (!finished) { console.error('etsy-link: the run ended before the last case (a promise that never settled?)'); process.exitCode = 1; } });
(async () => {
  const warn = console.warn, err = console.error; console.warn = () => {}; console.error = () => {};
  let n = 0;
  try {
    for (const [name, fn] of T) {
      let timer;
      try { await Promise.race([fn(), new Promise((_, rej) => { timer = setTimeout(() => rej(new Error('the case did not finish in 20 s')), 20000); })]); n++; process.stdout.write('  ok  ' + name.slice(0, 110) + '\n'); }
      catch (e) { console.warn = warn; console.error = err; console.error('FAIL ' + name + '\n' + (e && e.stack || e)); finished = true; process.exit(1); }
      finally { clearTimeout(timer); }
    }
  } finally { console.warn = warn; console.error = err; }
  assert(maxCalls <= 3, 'no run made more than 3 Etsy calls (' + maxCalls + ')');
  const shape = c => c.method + ' ' + c.path.replace(LID, '{id}') + (c.query ? ' ' + JSON.stringify(c.query) : '');
  const seen = [...new Set(sim.every.map(shape))].sort();
  assert.deepStrictEqual(seen, ['GET /listings/{id}', 'GET /listings/{id}/inventory {"legacy":false}', 'PUT /listings/{id}/inventory {"legacy":false}'], 'every Etsy request of every case was GET listing, GET inventory or PUT inventory of the one listing (' + sim.every.length + ' requests)');
  finished = true;
  console.log('etsy-link OK: ' + n + ' cases, ' + sim.every.length + ' fake Etsy requests, the most in any one run: ' + maxCalls);
  process.exit(0);
})().catch(e => { console.error(e); finished = true; process.exit(1); });
