// The Etsy step of "save a custom design to the charm repository, give it a SKU, link it on Etsy" (plans/save-to-repo-1011/CONTRACT.md section 5),
// OFFLINE: netlify/functions/_charmNestEtsySku.js on an in-memory Firestore and Storage (tests/charm-nest/_sandboxFakes.cjs) and a FAKE ETSY.
// The fake Etsy behaves like the real one where it matters for this write: it escapes the text it serves (16" arrives as 16&quot;), stores what it is sent,
// gives every product a new id on each PUT, wants prices as decimals, refuses a partial *_on_property set once one list covers every property, refuses a
// SKU that varies over a property its sku_on_property does not name, and answers every call with a recorded request. NO real token, NO network: the one
// end-to-end section runs the real etsyFetch (_etsyMailEtsy.js) against a fake node-fetch and a made-up token in the fake config/etsyOauth.
// It proves (Etsy's inventory PUT replaces the WHOLE inventory, so every line below is a way of not harming a listing):
//   · what is sent is what was read, except the `sku` of the group (the products that shared the bought product's SKU): a deep compare of the PUT body with
//     the GET answer for a single product, 32 products sharing one SKU, a subset sharing one, distinct SKUs; prices, quantities, enabled states, readiness ids,
//     option text, the *_on_property lists, disabled products still at zero stock; and the fake Etsy's STORED listing afterwards equals the stored listing
//     before, field for field, except those SKUs (so nothing was escaped twice, no id, price or stock moved);
//   · a listing whose *_on_property is partial is widened exactly as etsyUpdateListingInventoryProxy.js does (that file's own sanitizeForPut / normalizeOnProperty
//     run against this module on 300 random inventories, so the copy cannot drift);
//   · the before-copy (the exact GET answer) is in Storage and noted on the save record BEFORE the PUT; if either fails, no PUT is sent; a before-copy is never overwritten;
//   · nothing is sent when: the listing is not active, the SKU on Etsy changed, the new SKU is on another product, the save record is missing / not at the repository
//     step / a rehearsal, the request is a sandbox one; a second call after "linked" makes no Etsy call and no write;
//   · a different answer from Etsy (price moved, SKU not kept, a product added) is recorded as failed/verify with the details and the path of the before-copy, with
//     exactly ONE PUT and no undo; an answer that is not a whole inventory is "unverified" and its retry is one GET and no PUT;
//   · call counts: 2 with the mirror's state, 3 without, never 4; only GET listing, GET inventory, PUT inventory, always ?legacy=false on the inventory;
//   · 401/403 = "needs listings_w", 429, 5xx, timeout, no connection, 404, a refused body: each a plain reason, the right state, the right retry;
//   · the Charm_Listing_Skus table is refreshed from the answer; the alias record is noted; the lease refuses a second run while one is in flight.
//   node tests/charm-nest/save-etsy.cjs
'use strict';
const assert = require('assert'), path = require('path'), fs = require('fs'), Module = require('module');
const fnDir = path.join(__dirname, '../../netlify/functions');
process.env.SHOP_ID = '987654'; process.env.CLIENT_ID = 'test-key'; process.env.CLIENT_SECRET = 'test-secret'; delete process.env.EDIT_PASSCODE;
const Fk = require('./_sandboxFakes.cjs');
const { store, blobs, writes, fileWrites, db, bucket } = Fk;
const FV = Fk.admin.firestore.FieldValue;
const Mod = require(path.join(fnDir, '_charmNestEtsySku.js'));
const Canon = require(path.join(fnDir, '_etsyInventoryCanonical.js'));
const ListingSkus = require(path.join(fnDir, '_charmNestListingSkus.js'));
const clone = x => JSON.parse(JSON.stringify(x));
const LID = '1718000001', NEW = 'ACORN_LEAF_0001', OLD = "ACORN'S LEAF";

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
    S.stored = { products: b.products.map(p => { const f = p.offerings[0]; return { product_id: ++S.seq, sku: p.sku, is_deleted: false,   // (new ids on every PUT)
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

/* ═══ the world of one test: a save record, the mirror, the alias, the module on fakes ═══ */
let tick = 0, maxCalls = 0;
function world(o) {
  o = o || {};
  store.clear(); blobs.clear(); writes.length = 0; fileWrites.length = 0;
  const rid = o.rid || 'R1', inv = o.inventory || mkInv([{ sku: OLD }]);
  const E = sim(inv, o.sim), bought = o.bought || inv.products[0];
  if (o.doc !== null) store.set('Charm_Custom_Saves/' + rid, Object.assign({ rid, listingId: LID, sku: NEW, oldSku: OLD, productId: String(bought.product_id), optionPairs: [], group: { mode: 'all', productIds: [] }, title: 'Acorn leaf charm', design: { name: 'Acorn.ai', kind: 'ai', hash: 'h'.repeat(8), bytes: 1000 },
    masterHash: 'm'.repeat(8), aiPath: 'charmnest/master/custom/' + NEW + '.ai', thumbPath: 'charmnest/master/custom/' + NEW + '.png', by: 'Paul', at: 1760000000000, reused: false, phase: 'repo', etsy: { state: 'pending' }, rehearsal: false }, o.doc));
  if (o.mirror !== false) store.set('EtsyMail_Listings/' + LID, { state: o.mirror || 'active', title: 'Acorn leaf' });
  store.set('Charm_Sku_Aliases/' + LID, { bySku: { [OLD]: NEW }, by: 'Paul', keep: { rid, sku: NEW, at: 1, by: 'Paul' } });
  const bumps = [];
  const mod = Mod.make({ db: o.db || db, FV, bucket: o.bucket || bucket, etsy: E.api, meter: { bumpSimple: n => bumps.push(n), flushNow: async () => {} }, now: () => 1760000000000 + (++tick), timeoutMs: o.timeoutMs });
  const W = { E, S: E.S, mod, bumps, rid, inv, doc: () => store.get('Charm_Custom_Saves/' + rid), etsyDoc: () => (store.get('Charm_Custom_Saves/' + rid) || {}).etsy,
    run: async body => { const n0 = E.S.calls.length, r = await mod.customEtsySkuWrite(body || { rid }); const n = E.S.calls.length - n0; maxCalls = Math.max(maxCalls, n); assert.strictEqual(n, r.calls, 'the answer counts exactly the Etsy calls made'); return r; } };
  return W;
}
const kinds = W => W.S.calls.map(c => c.method + ' ' + c.path.replace(LID, '{id}'));
const noWrites = () => { assert.strictEqual(writes.length, 0, 'no document written: ' + JSON.stringify(writes)); assert.strictEqual(fileWrites.length, 0, 'no file written'); };
/* what the PUT must be, written from the GET answer alone and apart from the module */
function expectedBody(get, isGroup, newSku) {
  return { products: get.products.filter(p => !p.is_deleted).map(p => { const f = p.offerings[0]; return { sku: isGroup(p) ? newSku : unesc(p.sku),
    property_values: p.property_values.map(v => Object.assign({ property_id: v.property_id }, v.property_name == null ? {} : { property_name: unesc(v.property_name) }, { scale_id: v.scale_id, value_ids: v.value_ids, values: v.values.map(unesc) })),
    offerings: [Object.assign({ price: f.price.amount / f.price.divisor, quantity: f.is_enabled ? f.quantity : 0, is_enabled: f.is_enabled }, f.readiness_state_id != null ? { readiness_state_id: f.readiness_state_id } : {})] }; }),
    price_on_property: get.price_on_property, quantity_on_property: get.quantity_on_property, sku_on_property: get.sku_on_property };
}

/* the listings the cases use */
const metals = ['GF 14/20', 'SS', 'RG 14/20', '14K Gold'], lengths = [14, 15, 16, 17, 18, 20, 22, 24];
const thirtyTwo = () => mkInv(metals.flatMap((m, i) => lengths.map((n, j) => ({ sku: 'CUSTOM_6673', pv: [[513, 'Metal', m], [514, 'Chain length', n + '"']], price: 20 + i * 10 + j, qty: (i + j) % 5 === 0 ? 0 : 3 + j, on: (i + j) % 5 !== 0, ready: j % 2 ? 7001 : 7002 }))), { price_on_property: [513, 514] });
const subset = () => mkInv([
  { sku: 'CUSTOM_6673', pv: [[513, 'Metal', 'GF 14/20'], [514, 'Chain length', '16"']], price: 30 }, { sku: 'CUSTOM_6673', pv: [[513, 'Metal', 'GF 14/20'], [514, 'Chain length', '18"']], price: 32 },
  { sku: 'FOX_HEAD_4821', pv: [[513, 'Metal', 'SS'], [514, 'Chain length', '16"']], price: 28 }, { sku: 'FOX_HEAD_4822', pv: [[513, 'Metal', 'SS'], [514, 'Chain length', '18"']], price: 31, qty: 0, on: false },
  { sku: '', pv: [[513, 'Metal', 'RG 14/20'], [514, 'Chain length', '16"']], price: 40 }, { sku: 'ZOE\'S & CO', pv: [[513, 'Metal', 'RG 14/20'], [514, 'Chain length', '18"']], price: 44 }
], { price_on_property: [513, 514], sku_on_property: [513, 514] });
const distinct = () => mkInv([{ sku: 'OLD_ONE', pv: [[513, 'Metal', 'GF']], price: 20 }, { sku: 'OLD_TWO', pv: [[513, 'Metal', 'SS']], price: 22 }, { sku: 'OLD_THREE', pv: [[513, 'Metal', 'RG']], price: 24 }, { sku: 'OLD_FOUR', pv: [[513, 'Metal', 'YG']], price: 26 }],
  { price_on_property: [513], sku_on_property: [513] });
const partial = () => mkInv([   // sku_on_property names one of the two properties, price_on_property both: Etsy's GET returns this; its PUT refuses it unless widened
  { sku: 'SKU_A', pv: [[513, 'Metal', 'GF'], [514, 'Chain length', '16"']], price: 20 }, { sku: 'SKU_A', pv: [[513, 'Metal', 'GF'], [514, 'Chain length', '18"']], price: 22 },
  { sku: 'SKU_B', pv: [[513, 'Metal', 'SS'], [514, 'Chain length', '16"']], price: 24 }, { sku: 'SKU_B', pv: [[513, 'Metal', 'SS'], [514, 'Chain length', '18"']], price: 26 }
], { price_on_property: [513, 514], sku_on_property: [513] });

const T = [], test = (name, fn) => T.push([name, fn]);

/* ═══ 1 · the body is what was read, except the group's SKU ═══ */
async function sameExceptSku(W, isGroup, sku, expectIds) {
  const r = await W.run();
  assert.strictEqual(r.ok, true, JSON.stringify(r));
  assert.strictEqual(W.S.puts.length, 1, 'exactly one PUT');
  const put = W.S.puts[0].body, want = expectedBody(W.S.lastGet, isGroup, sku);
  assert.deepStrictEqual(put, want, 'the PUT body equals the GET answer except the group\'s sku');
  assert.deepStrictEqual(Object.keys(put).sort(), Object.keys(want).sort());
  // and what Etsy now STORES equals what it stored: only those SKUs differ, no id aside (new on every PUT), nothing escaped twice, no price or stock moved
  const before = W.inv, after = W.S.stored, exp = clone(before);
  exp.products.forEach((p, i) => { if (isGroup(W.S.lastGet.products[i])) p.sku = sku; });
  assert.deepStrictEqual(stripIds(after), stripIds(exp), 'the listing as Etsy stores it differs only in the group\'s SKUs');
  return r;
}
test('a single product: body equals the read, 2 calls with the mirror, record linked', async () => {
  const W = world();
  const r = await sameExceptSku(W, () => true, NEW);
  assert.strictEqual(r.state, 'linked'); assert.strictEqual(r.calls, 2); assert.strictEqual(r.sku, NEW); assert.strictEqual(r.verified, true);
  assert.deepStrictEqual(kinds(W), ['GET /listings/{id}/inventory', 'PUT /listings/{id}/inventory']);
  assert.deepStrictEqual(W.S.calls.map(c => c.query), [{ legacy: false }, { legacy: false }]);
  assert.deepStrictEqual(r.productIds, [String(W.S.stored.products[0].product_id)], 'the ids are the ones Etsy gave after the write');
  assert.strictEqual(W.bumps.length, 2); assert(W.bumps.every(n => n === 'custom.skuWrite'), 'every Etsy call is counted once as custom.skuWrite');
  const e = W.etsyDoc(); assert.strictEqual(e.state, 'linked'); assert.strictEqual(e.calls, 2); assert.strictEqual(e.verified, true); assert.deepStrictEqual(e.productIds, r.productIds); assert(!('runningUntil' in e), 'the lease is released');
});
test('32 products sharing one SKU: all 32 change, nothing else', async () => {
  const W = world({ inventory: thirtyTwo(), doc: { oldSku: 'CUSTOM_6673', group: { mode: 'all', productIds: [] } } });
  assert.strictEqual(W.inv.products.length, 32);
  const r = await sameExceptSku(W, () => true, NEW);
  assert.strictEqual(r.productIds.length, 32); assert.strictEqual(r.calls, 2);
  const put = W.S.puts[0].body;
  assert(put.products.every(p => p.sku === NEW));
  assert(put.products.some(p => p.offerings[0].is_enabled === false && p.offerings[0].quantity === 0), 'disabled products are still disabled at zero stock');
  assert(W.inv.products.some(p => p.offerings[0].is_enabled === false), 'the fixture has disabled products');
  assert(put.products.some(p => p.offerings[0].readiness_state_id === 7002) && put.products.some(p => p.offerings[0].readiness_state_id === 7001), 'readiness ids kept per product');
  assert(put.products.some(p => p.property_values[1].values[0] === '16"'), 'option text goes back as plain text (16", not 16&quot;)');
  assert.deepStrictEqual(put.price_on_property, [513, 514]);
  // the table the page reads: every live product shares one SKU, so it is a single SKU
  assert.deepStrictEqual(store.get('Charm_Listing_Skus/' + LID), { at: store.get('Charm_Listing_Skus/' + LID).at, n: 32, uni: NEW });
});
test('a subset sharing the SKU: only those products change; the others (a blank SKU, an escaped one, a disabled one) go back as they were', async () => {
  const inv = subset(), W = world({ inventory: inv, doc: { oldSku: 'CUSTOM_6673', group: { mode: 'some', productIds: [] } } });
  const r = await sameExceptSku(W, p => unesc(p.sku) === 'CUSTOM_6673', NEW);
  assert.deepStrictEqual(W.S.puts[0].body.products.map(p => p.sku), [NEW, NEW, 'FOX_HEAD_4821', 'FOX_HEAD_4822', '', "ZOE'S & CO"]);
  assert.strictEqual(r.productIds.length, 2);
  assert.strictEqual(W.S.puts[0].body.products[3].offerings[0].quantity, 0); assert.strictEqual(W.S.puts[0].body.products[3].offerings[0].is_enabled, false);
  const t = store.get('Charm_Listing_Skus/' + LID); assert(Array.isArray(t.products) && t.products.length === 6 && t.products.filter(p => p.sku === NEW).length === 2, 'the SKU table shows the two products with the new SKU');
});
test('distinct SKUs: only the bought product changes', async () => {
  const inv = distinct(), W = world({ inventory: inv, bought: inv.products[2], doc: { oldSku: 'OLD_THREE', group: { mode: 'some', productIds: [] } } });
  await sameExceptSku(W, p => p.sku === 'OLD_THREE', NEW);
  assert.deepStrictEqual(W.S.puts[0].body.products.map(p => p.sku), ['OLD_ONE', 'OLD_TWO', NEW, 'OLD_FOUR']);
});
test('the bought product is found by its option pairs when its id has changed (Etsy gives new ids on every write)', async () => {
  const inv = distinct(), W = world({ inventory: inv, bought: inv.products[1], doc: { oldSku: 'OLD_TWO', productId: '55', optionPairs: ['513:' + inv.products[1].property_values[0].value_ids[0]], group: { mode: 'some' } } });
  await sameExceptSku(W, p => p.sku === 'OLD_TWO', NEW);
  const W2 = world({ inventory: inv, bought: inv.products[1], doc: { oldSku: 'OLD_TWO', productId: '', optionPairs: [['513', String(inv.products[1].property_values[0].value_ids[0])]], group: { mode: 'some' } } });
  await sameExceptSku(W2, p => p.sku === 'OLD_TWO', NEW);
  const W3 = world({ inventory: inv, bought: inv.products[1], doc: { oldSku: 'OLD_TWO', productId: '55', optionPairs: [], group: { mode: 'some' } } });
  const r = await W3.run(); assert.strictEqual(r.reason, 'product-missing'); assert.strictEqual(W3.S.puts.length, 0);
  const W4 = world({ inventory: inv, bought: inv.products[1], doc: { oldSku: 'OLD_TWO', productId: '', optionPairs: ['513:424242'], group: { mode: 'some' } } });
  assert.strictEqual((await W4.run()).reason, 'product-missing'); assert.strictEqual(W4.S.puts.length, 0);
});
test('a listing whose price_on_property is partial is widened exactly as the proxy does, and Etsy takes it', async () => {
  const inv = partial(), W = world({ inventory: inv, doc: { oldSku: 'SKU_A', group: { mode: 'some' } } });
  const r = await W.run(); assert.strictEqual(r.ok, true, JSON.stringify(r));
  const put = W.S.puts[0].body;
  assert.deepStrictEqual([put.price_on_property, put.quantity_on_property, put.sku_on_property], [[513, 514], [], [513, 514]], 'sku_on_property widened to every property, the empty list left alone');
  assert.deepStrictEqual(put.products.map(p => p.sku), [NEW, NEW, 'SKU_B', 'SKU_B']);
  assert.deepStrictEqual(put.products.map(p => p.offerings[0].price), [20, 22, 24, 26]);
  // the fake Etsy refuses the unwidened body (this is what the proxy's comment describes)
  const raw = clone(W.S.puts[0].body); raw.sku_on_property = [513];
  const Z = sim(inv); await assert.rejects(() => Z.api.put('/listings/' + LID + '/inventory', { legacy: false }, raw), /unsupported number of property IDs/);
});
test('the readiness-by-property list is kept when Etsy sends one, absent when it does not; a deleted product is not sent', async () => {
  const inv = subset(); inv.readiness_state_on_property = [514]; inv.products.push({ product_id: 999, sku: 'GONE', is_deleted: true, property_values: [], offerings: [{ offering_id: 1, price: 5, quantity: 1, is_enabled: true }] });
  const W = world({ inventory: inv, doc: { oldSku: 'CUSTOM_6673', group: { mode: 'some' } } });
  const r = await W.run(); assert.strictEqual(r.ok, true, JSON.stringify(r));
  assert.deepStrictEqual(W.S.puts[0].body.readiness_state_on_property, [514]);
  assert.strictEqual(W.S.puts[0].body.products.length, 6, 'the deleted product is not in the body');
  const W2 = world(); await W2.run(); assert(!('readiness_state_on_property' in W2.S.puts[0].body));
});
test('option text, names and the SKUs of products that are not changed are unescaped ONCE (Etsy stores what it is sent and escapes on the way out)', async () => {
  const inv = mkInv([{ sku: OLD, pv: [[513, 'Tom\'s "Metal"', 'Gold & Silver']], price: 30 }, { sku: 'ZOE\'S', pv: [[513, 'Tom\'s "Metal"', '16"']], price: 31 }, { sku: 'ODD', pv: [[513, 'Tom\'s "Metal"', '18&quot;']], price: 32 }, { sku: '<B>', pv: [[513, 'Tom\'s "Metal"', 'a < b']], price: 33 }],
    { price_on_property: [513], sku_on_property: [513] });
  for (const escapeSku of [true, false]) {
    const W = world({ inventory: inv, sim: { escapeSku }, doc: { group: { mode: 'some' } } });
    const r = await W.run(); assert.strictEqual(r.ok, true, JSON.stringify(r));
    const put = W.S.puts[0].body;
    assert.deepStrictEqual(put.products.map(p => p.property_values[0].values[0]), ['Gold & Silver', '16"', '18&quot;', 'a < b'], 'one pass: the stored 18&quot; (served as 18&amp;quot;) goes back as 18&quot;, not 18"');
    assert.strictEqual(put.products[0].property_values[0].property_name, 'Tom\'s "Metal"');
    assert.deepStrictEqual(put.products.map(p => p.sku), [NEW, escapeSku ? "ZOE'S" : "ZOE'S", 'ODD', '<B>']);
    assert.deepStrictEqual(stripIds(W.S.stored).products.map(p => [p.sku, p.property_values[0].values[0]]), [[NEW, 'Gold & Silver'], ["ZOE'S", '16"'], ['ODD', '18&quot;'], ['<B>', 'a < b']], 'what Etsy stores is unchanged text');
  }
});

/* ═══ 2 · the same rules as the proxy's (its own code, run against this module's) ═══ */
test('sanitizing and widening are the proxy\'s: its own sanitizeForPut / normalizeOnProperty against this module on 300 random inventories', async () => {
  const file = path.join(fnDir, 'etsyUpdateListingInventoryProxy.js'), src = fs.readFileSync(file, 'utf8') + '\nmodule.exports.__t = { sanitizeForPut, normalizeOnProperty };';
  const fakeRequire = req => req === './etsyRateLimiter' ? { etsyFetch: async () => { throw new Error('no network in this test'); } } : require(path.join(fnDir, req));
  const m = { exports: {} }; new Function('require', 'module', 'exports', 'process', src)(fakeRequire, m, m.exports, process);
  const proxy = m.exports.__t;
  let seed = 7; const rnd = n => { seed = (seed * 1103515245 + 12345) % 2147483648; return seed % n; };
  const pick = a => a[rnd(a.length)];
  for (let t = 0; t < 300; t++) {
    const nProps = rnd(3), props = [513, 514].slice(0, nProps), rows = [];
    const vals = props.map(p => Array.from({ length: 1 + rnd(3) }, (_, i) => 'v' + p + '_' + i));
    const combos = props.length === 0 ? [[]] : props.length === 1 ? vals[0].map(a => [a]) : vals[0].flatMap(a => vals[1].map(b => [a, b]));
    for (const c of combos) rows.push({ sku: pick(['', 'A1', 'B2', 'CUSTOM_6673']), pv: c.map((v, i) => [props[i], 'Prop' + props[i], v]), price: pick([20, 20.5, 33.33, 99]), qty: pick([0, 1, 7]), on: pick([true, true, false]), ready: pick([null, 7001, 7002]) });
    const base = mkInv(rows, { price_on_property: props.filter(() => rnd(2)), quantity_on_property: props.filter(() => rnd(3) === 0), sku_on_property: props.filter(() => rnd(2)) });
    // as Etsy would serve it: Money objects for some prices, plain decimals for others
    const served = clone(base); served.products.forEach(p => p.offerings.forEach(f => { f.price = rnd(2) ? money(f.price) : f.price; }));
    const group = served.products.filter(() => rnd(3) === 0), gset = new Set(group);
    const theirs = proxy.sanitizeForPut(served, proxy.normalizeOnProperty(served, Canon.productPropertyOrder(served)), null);
    theirs.products.forEach((p, i) => { if (gset.has(served.products[i])) p.sku = 'NEWSKU_1'; });
    const mine = Mod.internals.buildPutBody(served, served.products, gset, 'NEWSKU_1');
    assert.deepStrictEqual(clone(mine), clone(theirs), 'case ' + t);
    assert.deepStrictEqual(Mod.internals.selfCheck(served, served.products, mine, gset, 'NEWSKU_1'), [], 'the self-check passes the proxy-identical body, case ' + t);
  }
});
test('the self-check stops a body that differs in anything else (it would be sent otherwise)', async () => {
  const inv = clone(subset()); inv.products.forEach(p => p.offerings.forEach(f => { f.price = money(f.price); }));
  const live = inv.products, gset = new Set(live.filter(p => p.sku === 'CUSTOM_6673')), good = Mod.internals.buildPutBody(inv, live, gset, NEW), check = b => Mod.internals.selfCheck(inv, live, b, gset, NEW);
  assert.deepStrictEqual(check(good), []);
  const mutants = {
    'a price': b => { b.products[2].offerings[0].price += 1; }, 'a quantity': b => { b.products[0].offerings[0].quantity += 1; }, 'stock on a disabled offering': b => { b.products[3].offerings[0].quantity = 1; },
    'an enabled state': b => { b.products[0].offerings[0].is_enabled = false; }, 'a readiness id': b => { b.products[1].offerings[0].readiness_state_id = 1; }, 'a lost readiness id': b => { delete b.products[1].offerings[0].readiness_state_id; },
    'an option value': b => { b.products[4].property_values[1].values[0] = '17"'; }, 'an option id': b => { b.products[4].property_values[1].property_id = 9; }, 'value ids': b => { b.products[4].property_values[0].value_ids = []; },
    'an option dropped': b => { b.products[4].property_values.pop(); }, 'a product dropped': b => { b.products.pop(); }, 'a product added': b => { b.products.push(clone(b.products[0])); },
    'a product reordered': b => { b.products.reverse(); }, 'a SKU outside the group': b => { b.products[2].sku = NEW; }, 'a SKU of the group left': b => { b.products[0].sku = 'CUSTOM_6673'; },
    'a price list': b => { b.price_on_property = []; }, 'a sku list': b => { b.sku_on_property = [513]; }, 'a quantity list': b => { b.quantity_on_property = [513, 514]; }, 'a stray field': b => { b.shipping_profile_id = 1; },
    'a price as money': b => { b.products[0].offerings[0].price = money(30); }, 'a missing offering': b => { b.products[0].offerings = []; }
  };
  for (const [what, fn] of Object.entries(mutants)) { const b = clone(good); fn(b); assert(check(b).length > 0, 'a body with ' + what + ' must be refused'); }
  const bad = clone(inv); bad.products[0].offerings[0].price = null;
  assert(Mod.internals.selfCheck(bad, bad.products, Mod.internals.buildPutBody(bad, bad.products, new Set(), NEW), new Set(), NEW).some(x => /price unreadable/.test(x)), 'an unreadable price is never sent');
});

/* ═══ 3 · the before-copy first ═══ */
test('the before-copy (the exact GET answer) is in Storage and noted on the record BEFORE the PUT', async () => {
  let atPut = null;
  const W = world({ sim: { } });
  const realPut = W.E.api.put; W.E.api.put = async (p, q, b) => { const e = W.etsyDoc(); atPut = { state: e.state, path: e.beforePath, hash: e.beforeHash, blob: e.beforePath && blobs.has(e.beforePath) ? blobs.get(e.beforePath).buf.toString('utf8') : null, saves: fileWrites.length }; return realPut(p, q, b); };
  const mod = Mod.make({ db, FV, bucket, etsy: W.E.api, meter: { bumpSimple() {} }, now: () => 1760000000000 + (++tick) });
  const r = await mod.customEtsySkuWrite({ rid: 'R1' }); assert.strictEqual(r.ok, true, JSON.stringify(r));
  assert(atPut, 'the PUT happened'); assert.strictEqual(atPut.state, 'writing', 'the record said "writing" when the PUT left');
  assert.strictEqual(atPut.path, 'charmnest/custom-saves/' + LID + '/R1-before.json');
  assert.strictEqual(atPut.saves, 1, 'one file written before the PUT');
  assert.deepStrictEqual(JSON.parse(atPut.blob), W.S.lastGet, 'the saved copy is the GET answer, whole');
  assert.strictEqual(require('crypto').createHash('sha256').update(atPut.blob).digest('hex'), atPut.hash);
  const e = W.etsyDoc(); assert.strictEqual(e.beforePath, atPut.path); assert.strictEqual(e.attempts, 1); assert.deepStrictEqual(e.backups, [atPut.path]); assert.strictEqual(e.products, 1);
  assert.deepStrictEqual(e.groupBefore, [{ id: String(W.inv.products[0].product_id), sku: 'ACORN&#39;S LEAF' }], 'the group\'s ids and old SKUs, as Etsy served them, on the record');
  assert.deepStrictEqual(fileWrites.map(f => f.path), [atPut.path], 'nothing else was written to Storage');
});
test('if the before-copy cannot be written, kept, or noted on the record, NOTHING is sent', async () => {
  const brokenBucket = how => ({ name: 'x', file: p => { const f = bucket.file(p); return Object.assign({}, f, { save: async (...a) => { if (how === 'throws') throw new Error('storage is down'); if (how === 'lies') return; return f.save(...a); } }); } });
  for (const how of ['throws', 'lies']) {
    const W = world({ bucket: brokenBucket(how) }), r = await W.run();
    assert.strictEqual(r.ok, false); assert.strictEqual(r.reason, 'backup-failed'); assert.strictEqual(r.text, 'the safety copy could not be saved'); assert.strictEqual(r.retry, true);
    assert.strictEqual(W.S.puts.length, 0, how + ': no PUT'); assert.strictEqual(W.etsyDoc().state, 'failed'); assert.strictEqual(W.etsyDoc().reason, 'backup-failed');
  }
  // the record cannot be updated to "writing"
  const dbFail = { collection: c => ({ doc: id => { const ref = db.collection(c).doc(id); return Object.assign({}, ref, { update: async d => { if (d && d.etsy && d.etsy.state === 'writing') throw new Error('firestore is down'); return ref.update(d); } }); } }), runTransaction: fn => db.runTransaction(fn) };
  const W = world({ db: dbFail }), r = await W.run();
  assert.strictEqual(r.reason, 'backup-failed'); assert.strictEqual(W.S.puts.length, 0, 'no PUT when the record cannot be written');
});
test('a before-copy is never overwritten: a second attempt writes -before-2.json and the first stays as it was', async () => {
  const W = world({ sim: { fail: c => (c.method === 'PUT' && W.S.puts.length === 1 ? etsyErr(403, 'scope') : null) } });
  const r1 = await W.run(); assert.strictEqual(r1.reason, 'scope');
  const first = blobs.get('charmnest/custom-saves/' + LID + '/R1-before.json').buf.toString('utf8');
  const r2 = await W.run(); assert.strictEqual(r2.ok, true, JSON.stringify(r2));
  assert.strictEqual(blobs.get('charmnest/custom-saves/' + LID + '/R1-before.json').buf.toString('utf8'), first, 'the first before-copy is untouched');
  assert(blobs.has('charmnest/custom-saves/' + LID + '/R1-before-2.json'));
  assert.deepStrictEqual(W.etsyDoc().backups, ['charmnest/custom-saves/' + LID + '/R1-before.json', 'charmnest/custom-saves/' + LID + '/R1-before-2.json']); assert.strictEqual(W.etsyDoc().attempts, 2);
  // even with the record's counters lost, an existing name is skipped
  const W2 = world(); blobs.set('charmnest/custom-saves/' + LID + '/R1-before.json', { buf: Buffer.from('{"older":true}'), contentType: 'application/json' });
  assert.strictEqual((await W2.run()).ok, true); assert.strictEqual(blobs.get('charmnest/custom-saves/' + LID + '/R1-before.json').buf.toString(), '{"older":true}'); assert(blobs.has('charmnest/custom-saves/' + LID + '/R1-before-2.json'));
});

/* ═══ 4 · call counts ═══ */
test('calls: 2 with the mirror\'s state, 3 without (GET listing first), never 4', async () => {
  const a = world(); assert.strictEqual((await a.run()).calls, 2); assert.strictEqual(a.S.calls.length, 2);
  const b = world({ mirror: false }); const rb = await b.run(); assert.strictEqual(rb.calls, 3); assert.deepStrictEqual(kinds(b), ['GET /listings/{id}', 'GET /listings/{id}/inventory', 'PUT /listings/{id}/inventory']);
  assert.strictEqual(b.bumps.length, 3);
  // an echo that is not a whole inventory without the mirror: still 3, no fourth call to read it back
  const c = world({ mirror: false, sim: { echo: 'empty' } }); const rc = await c.run(); assert.strictEqual(rc.state, 'unverified'); assert.strictEqual(rc.calls, 3); assert.strictEqual(c.S.calls.length, 3);
  // a mirror doc with no state is the same as no mirror
  const d = world({ mirror: false }); store.set('EtsyMail_Listings/' + LID, { title: 'x' }); assert.strictEqual((await d.run()).calls, 3);
});
test('a listing that is not active is refused before any inventory call (mirror or GET)', async () => {
  for (const state of ['inactive', 'draft', 'expired', 'sold_out', 'removed', 'ACTIVE_NOT']) {
    const W = world({ mirror: state }), r = await W.run();
    assert.strictEqual(r.ok, false); assert.strictEqual(r.reason, 'listing-state'); assert.strictEqual(r.text, 'the listing is ' + state.toLowerCase().replace(/[^a-z_ ]/g, '') + ' on Etsy'); assert.strictEqual(r.calls, 0);
    assert.strictEqual(W.S.calls.length, 0, state + ': Etsy not asked at all'); assert.strictEqual(fileWrites.length, 0);
    assert.strictEqual(W.etsyDoc().state, 'failed'); assert.strictEqual(W.etsyDoc().reason, 'listing-state');
  }
  const W = world({ mirror: false, sim: { state: 'draft' } }), r = await W.run();
  assert.strictEqual(r.reason, 'listing-state'); assert.strictEqual(r.text, 'the listing is draft on Etsy'); assert.strictEqual(r.calls, 1); assert.deepStrictEqual(kinds(W), ['GET /listings/{id}']);
  const M = world({ mirror: false, sim: { state: 'active' } }); assert.strictEqual((await M.run()).ok, true);
});

/* ═══ 5 · nothing is sent when Etsy is not as the order left it ═══ */
test('changed: the SKU on Etsy is no longer the old one', async () => {
  const inv = mkInv([{ sku: 'SOMEONE_ELSE_1' }]), W = world({ inventory: inv, doc: { group: { mode: 'unknown', productIds: [] } } });
  const r = await W.run();
  assert.strictEqual(r.reason, 'changed'); assert.strictEqual(r.text, 'the SKU on Etsy changed since the order was read'); assert.strictEqual(r.retry, false); assert.strictEqual(r.calls, 1);
  assert.strictEqual(W.S.puts.length, 0); assert.strictEqual(fileWrites.length, 0, 'no before-copy for a write that did not happen'); assert.strictEqual(W.etsyDoc().reason, 'changed');
  // the old SKU matches whatever the case or the escaping
  const W2 = world({ inventory: mkInv([{ sku: "acorn's leaf " }]) }); assert.strictEqual((await W2.run()).ok, true);
  // the group changed in size since the preview ("all" then, "some" now)
  const W3 = world({ inventory: subset(), doc: { oldSku: 'CUSTOM_6673', group: { mode: 'all', productIds: [] } } }); const r3 = await W3.run(); assert.strictEqual(r3.reason, 'changed'); assert.strictEqual(W3.S.puts.length, 0);
  const W4 = world({ inventory: subset(), doc: { oldSku: 'CUSTOM_6673', group: { mode: 'unknown', productIds: [] } } }); assert.strictEqual((await W4.run()).ok, true, 'an unknown group is decided from the live inventory');
});
test('sku-in-use: the new SKU is already on another product of the listing', async () => {
  const inv = distinct(); inv.products[3].sku = NEW;
  const W = world({ inventory: inv, bought: inv.products[0], doc: { oldSku: 'OLD_ONE', group: { mode: 'some' } } }), r = await W.run();
  assert.strictEqual(r.reason, 'sku-in-use'); assert.strictEqual(r.text, 'that SKU is already on another product of the listing'); assert.strictEqual(W.S.puts.length, 0); assert.strictEqual(fileWrites.length, 0);
  assert.strictEqual(r.retry, false);
});
test('product-missing: the product bought is gone, or the listing has no products', async () => {
  const W = world({ inventory: mkInv([{ sku: OLD }]), doc: { productId: '4242' } }); const r = await W.run(); assert.strictEqual(r.reason, 'product-missing'); assert.strictEqual(r.text, 'the product bought is no longer on the listing'); assert.strictEqual(W.S.puts.length, 0);
  const W2 = world({ inventory: Object.assign(mkInv([{ sku: OLD }]), { products: [] }), bought: { product_id: 100 } }); assert.strictEqual((await W2.run()).reason, 'product-missing');
});
test('already on Etsy (a person set it, or an earlier run landed): linked, no write, and an old SKU left on another product stops it', async () => {
  const W = world({ inventory: mkInv([{ sku: NEW }]) }), r = await W.run();
  assert.strictEqual(r.ok, true); assert.strictEqual(r.state, 'linked'); assert.strictEqual(r.calls, 1); assert.strictEqual(W.S.puts.length, 0); assert.strictEqual(fileWrites.length, 0); assert.strictEqual(r.verified, false);
  const inv = subset(); inv.products[0].sku = NEW;
  const W2 = world({ inventory: inv, doc: { oldSku: 'CUSTOM_6673', group: { mode: 'some' } } }); const r2 = await W2.run(); assert.strictEqual(r2.reason, 'changed'); assert.strictEqual(W2.S.puts.length, 0);
});

/* ═══ 6 · the saved record decides ═══ */
test('refused without a saved record at the repository step; sandbox and rehearsal never; bad requests; nothing read from Etsy, nothing written', async () => {
  const cases = [
    ['no record', { doc: null }, { rid: 'R1' }, 'no-save'], ['not at the repository step', { doc: { phase: 'sku' } }, { rid: 'R1' }, 'not-saved'], ['no phase', { doc: { phase: undefined } }, { rid: 'R1' }, 'not-saved'],
    ['a rehearsal', { doc: { rehearsal: true } }, { rid: 'R1' }, 'rehearsal'], ['sandbox true', {}, { rid: 'R1', sandbox: true }, 'sandbox'], ['sandbox 1', {}, { rid: 'R1', sandbox: 1 }, 'sandbox'], ['sandbox "1"', {}, { rid: 'R1', sandbox: '1' }, 'sandbox'],
    ['sandbox "true"', {}, { rid: 'R1', sandbox: 'true' }, 'sandbox'], ['no rid', {}, {}, 'bad-request'], ['a rid with a slash', {}, { rid: 'a/b' }, 'bad-request'], ['a huge rid', {}, { rid: 'x'.repeat(200) }, 'bad-request'],
    ['a lower-case sku', { doc: { sku: 'acorn_leaf' } }, { rid: 'R1' }, 'bad-save'], ['a sku with a space', { doc: { sku: 'ACORN LEAF' } }, { rid: 'R1' }, 'bad-save'], ['a sku over 32', { doc: { sku: 'A'.repeat(33) } }, { rid: 'R1' }, 'bad-save'],
    ['no sku', { doc: { sku: '' } }, { rid: 'R1' }, 'bad-save'], ['a listing id that is not digits', { doc: { listingId: '12/34' } }, { rid: 'R1' }, 'bad-save']
  ];   // (KEEP B5: old = new is no refusal any more: the listing IS linked, see below)
  for (const [what, w, body, reason] of cases) {
    const W = world(w);
    if (w.doc && 'phase' in w.doc && w.doc.phase === undefined) delete store.get('Charm_Custom_Saves/R1').phase;
    writes.length = 0; fileWrites.length = 0;
    const r = await W.run(body);
    assert.strictEqual(r.ok, false, what); assert.strictEqual(r.reason, reason, what + ': ' + JSON.stringify(r)); assert.strictEqual(r.calls, 0, what); assert.strictEqual(W.S.calls.length, 0, what + ': no Etsy call'); noWrites();
    assert(!('error' in r), 'an expected refusal has no `error` key (the Library would answer HTTP 400)');
  }
  { // KEEP B5: the line already carried the design's own SKU (the shop's SKUs have the shape the plan makes): nothing to change on Etsy, the listing IS linked, no call, recorded, idempotent
    const W = world({ doc: { oldSku: NEW } }); writes.length = 0;
    let r = await W.run({ rid: 'R1' });
    assert.strictEqual(r.ok, true); assert.strictEqual(r.state, 'linked'); assert.strictEqual(r.noop, true); assert.strictEqual(r.calls, 0); assert.strictEqual(W.S.calls.length, 0, 'no Etsy call');
    assert.strictEqual(store.get('Charm_Custom_Saves/R1').etsy.state, 'linked'); assert.strictEqual(store.get('Charm_Custom_Saves/R1').etsy.noop, true);
    r = await W.run({ rid: 'R1' }); assert.strictEqual(r.ok, true); assert.strictEqual(r.noop, true); assert.strictEqual(W.S.calls.length, 0, 'again: still no call');
  }
  const tx = world(); tx.mod = Mod.make({ db: { collection: () => ({ doc: () => ({}) }), runTransaction: async () => { throw new Error('firestore is down'); } }, FV, bucket, etsy: tx.E.api, meter: { bumpSimple() {} }, now: Date.now });
  const r = await tx.mod.customEtsySkuWrite({ rid: 'R1' }); assert.strictEqual(r.reason, 'internal'); assert.strictEqual(tx.S.calls.length, 0);
  // the real wiring refuses the sandbox without building anything (no Firebase, no Etsy helper is loaded for it)
  const viaOps = await Mod.ops.customEtsySkuWrite({ rid: 'R1', sandbox: true }); assert.strictEqual(viaOps.ok, false); assert.strictEqual(viaOps.reason, 'sandbox'); assert.strictEqual(viaOps.calls, 0); noWrites();
});
test('a second call after "linked" makes no Etsy call and no write; a run in flight is refused; an expired lease is not', async () => {
  const W = world(); const first = await W.run(); assert.strictEqual(first.ok, true);
  const callsBefore = W.S.calls.length; writes.length = 0; fileWrites.length = 0;
  const again = await W.run();
  assert.strictEqual(again.ok, true); assert.strictEqual(again.state, 'linked'); assert.strictEqual(again.calls, 0); assert.deepStrictEqual(again.productIds, first.productIds); assert.strictEqual(again.sku, NEW);
  assert.strictEqual(W.S.calls.length, callsBefore); noWrites();
  const L = world(); store.get('Charm_Custom_Saves/R1').etsy = { state: 'pending', runningUntil: Date.now() + 5e6 };
  const rl = await L.run(); assert.strictEqual(rl.reason, 'running'); assert.strictEqual(rl.calls, 0); assert.strictEqual(L.S.calls.length, 0); noWrites();
  const X = world(); store.get('Charm_Custom_Saves/R1').etsy = { state: 'pending', runningUntil: 5 }; assert.strictEqual((await X.run()).ok, true);
});

/* ═══ 7 · what Etsy answers ═══ */
test('while the PUT is in flight the record holds a lease: a second press is refused with no Etsy call and no second PUT', async () => {
  let second = null, leaseSeen = null;
  const W = world({ sim: { during: async c => { if (c.method !== 'PUT') return; leaseSeen = W.etsyDoc(); const n0 = W.S.calls.length; second = await W.mod.customEtsySkuWrite({ rid: 'R1' }); second.extra = W.S.calls.length - n0; } } });
  const r = await W.run(); assert.strictEqual(r.ok, true, JSON.stringify(r));
  assert.strictEqual(leaseSeen.state, 'writing'); assert(leaseSeen.runningUntil > leaseSeen.at + 1000, 'the "writing" note still holds the lease (on the test\'s clock)');
  assert.strictEqual(second.ok, false); assert.strictEqual(second.reason, 'running'); assert.strictEqual(second.calls, 0); assert.strictEqual(second.extra, 0, 'the second press made no Etsy call (the PUT itself was counted before it started)') ;
  assert.strictEqual(W.S.puts.length, 1); assert(!('runningUntil' in W.etsyDoc()), 'released with the last write');
});
test('verify: an answer that is not what was sent is recorded with the details and the before-copy, with ONE PUT and no undo', async () => {
  const cases = {
    'a price moved': { mutate: s => { s.products[0].offerings[0].price += 5; }, text: /not what was sent/, detail: /price/ },
    'the SKU was not kept': { mutate: s => { s.products[0].sku = OLD; }, text: /did not keep the new SKU/, detail: /SKU/ },
    'stock appeared': { mutate: s => { s.products[0].offerings[0].quantity = 99; }, text: /not what was sent/, detail: /quantity/ },
    'switched off': { mutate: s => { s.products[0].offerings[0].is_enabled = false; }, text: /not what was sent/, detail: /enabled/ },
    'readiness changed': { mutate: s => { s.products[0].offerings[0].readiness_state_id = 1; }, text: /not what was sent/, detail: /readiness/ },
    'a list changed': { mutate: s => { s.sku_on_property = [999]; }, text: /not what was sent/, detail: /sku_on_property/ },
    'an option text double-escaped': { mutate: s => { s.products[0].property_values[0].values[0] = 'GF &amp; 14'; }, inv: mkInv([{ sku: OLD, pv: [[513, 'Metal', 'GF & 14']] }], { price_on_property: [513], sku_on_property: [513] }), text: /not what was sent/, detail: /option text/ }
  };
  for (const [what, c] of Object.entries(cases)) {
    const W = world({ inventory: c.inv, sim: { mutate: c.mutate } }), r = await W.run();
    assert.strictEqual(r.ok, false, what); assert.strictEqual(r.state, 'failed', what); assert.strictEqual(r.reason, 'verify', what + ': ' + JSON.stringify(r)); assert(c.text.test(r.text), what + ': ' + r.text);
    assert(r.details && r.details.some(x => c.detail.test(x)), what + ': ' + JSON.stringify(r.details)); assert.strictEqual(r.retry, false);
    assert.strictEqual(W.S.puts.length, 1, what + ': one PUT, no second, no undo'); assert.strictEqual(r.beforePath, 'charmnest/custom-saves/' + LID + '/R1-before.json'); assert(blobs.has(r.beforePath));
    const e = W.etsyDoc(); assert.strictEqual(e.state, 'failed'); assert.strictEqual(e.reason, 'verify'); assert(e.details.length > 0); assert.strictEqual(e.beforePath, r.beforePath);
    // pressing Retry later: one GET to look, never a PUT, however the listing now stands
    const calls0 = W.S.calls.length, again = await W.run();
    assert.strictEqual(again.ok, false, what); assert.strictEqual(again.reason, 'verify', what); assert.strictEqual(W.S.puts.length, 1, what + ': a retry sends no PUT'); assert.strictEqual(W.S.calls.length - calls0, 1, what + ': a retry is one GET');
  }
});
test('a product that appears on Etsy after the PUT: the echo has the wrong count (unverified), the retry reads the listing and compares it with the before-copy (verify, no second PUT)', async () => {
  const W = world({ sim: { mutate: s => { s.products.push(Object.assign(clone(s.products[0]), { product_id: 5555, property_values: [{ property_id: 513, property_name: 'Metal', scale_id: null, value_ids: [1], values: ['X'] }] })); } } });
  const r = await W.run(); assert.strictEqual(r.state, 'unverified'); assert.strictEqual(W.S.puts.length, 1);
  const again = await W.run(); assert.strictEqual(again.reason, 'verify'); assert.strictEqual(again.state, 'failed'); assert(again.details.some(x => /Unexpected combination/.test(x)), JSON.stringify(again.details)); assert.strictEqual(W.S.puts.length, 1); assert.strictEqual(again.calls, 1);
});
test('unverified: an echo that is not a whole inventory; the retry is one GET and no PUT, and links when the SKU is there', async () => {
  for (const mirror of [true, false]) {
    for (const echo of ['empty', s => ({ products: s.products.map(p => ({ product_id: p.product_id })) }), s => Object.assign({}, s, { products: [] })]) {
      const W = world({ mirror: mirror ? 'active' : false, sim: { echo } }), r = await W.run();
      assert.strictEqual(r.ok, false); assert.strictEqual(r.state, 'unverified'); assert.strictEqual(r.reason, 'unverified'); assert.strictEqual(r.retry, true); assert.strictEqual(r.calls, mirror ? 2 : 3);
      assert.strictEqual(W.etsyDoc().state, 'unverified'); assert.strictEqual(W.S.puts.length, 1);
      assert.strictEqual(store.has('Charm_Listing_Skus/' + LID), false, 'a table is not refreshed from an answer that was not read');
      const g0 = W.S.gets.length, again = await W.run();
      assert.strictEqual(again.ok, true, JSON.stringify(again)); assert.strictEqual(again.state, 'linked'); assert.strictEqual(again.calls, 1, 'the retry is one GET'); assert.strictEqual(W.S.gets.length - g0, 1); assert.strictEqual(W.S.puts.length, 1, 'and no PUT');
      assert.strictEqual(again.verified, true, 'the live listing was compared with the before-copy');
      assert.strictEqual(W.etsyDoc().state, 'linked'); assert.strictEqual(store.get('Charm_Listing_Skus/' + LID).uni, NEW, 'the table is refreshed from the retry\'s GET');
    }
  }
  // the retry finds the SKU NOT there (the PUT never landed): the normal sequence again, one more backup, one PUT
  const W = world({ sim: { echo: 'empty', fail: c => null } }); const r1 = await W.run(); assert.strictEqual(r1.state, 'unverified');
  W.S.stored = clone(W.inv);   // (as if Etsy never applied it)
  const r2 = await W.run(); assert.strictEqual(r2.state, 'unverified'); assert.strictEqual(W.S.puts.length, 2); assert(blobs.has('charmnest/custom-saves/' + LID + '/R1-before-2.json'));
  // the retry finds the listing changed in other ways since the PUT: not linked, said plainly
  const V = world({ sim: { echo: 'empty' } }); await V.run(); V.S.stored.products[0].offerings[0].price = 99;
  const rv = await V.run(); assert.strictEqual(rv.reason, 'verify'); assert.strictEqual(V.S.puts.length, 1);
});
test('Etsy errors: each a plain reason, the right state and retry; 4xx is not applied, a timeout or 502-504 after the PUT may be', async () => {
  const E = (status, error) => etsyErr(status, error);
  const cases = [
    // [what, which call fails, error, reason, state, retry, needs]
    ['GET inventory 401', 'GET inv', E(401), 'scope', 'failed', true, 'listings_w'], ['GET inventory 403', 'GET inv', E(403), 'scope', 'failed', true, 'listings_w'], ['PUT 403', 'PUT', E(403), 'scope', 'failed', true, 'listings_w'], ['PUT 401', 'PUT', E(401), 'scope', 'failed', true, 'listings_w'],
    ['GET listing 403', 'GET listing', E(403), 'scope', 'failed', true, 'listings_w'], ['token refresh refused', 'GET inv', new Error('Etsy token refresh failed: 401 {"error":"invalid_grant"}'), 'scope', 'failed', true, 'listings_w'], ['token not seeded', 'GET inv', new Error('Etsy OAuth not seeded at config/etsyOauth. Run etsyMailSeedTokens first.'), 'scope', 'failed', true, 'listings_w'],
    ['token refresh 500', 'GET inv', new Error('Etsy token refresh failed: 500 oops'), 'etsy-down', 'failed', true],
    ['GET 429', 'GET inv', E(429), 'busy', 'failed', true], ['PUT 429', 'PUT', E(429), 'busy', 'failed', true], ['GET 500', 'GET inv', E(500), 'etsy-down', 'failed', true], ['PUT 500', 'PUT', E(500), 'etsy-down', 'failed', true],
    ['PUT 503', 'PUT', E(503), 'etsy-down', 'unverified', true], ['PUT 504', 'PUT', E(504), 'etsy-down', 'unverified', true], ['GET 503', 'GET inv', E(503), 'etsy-down', 'failed', true],
    ['GET inventory 404', 'GET inv', E(404), 'listing-gone', 'failed', false], ['GET listing 404', 'GET listing', E(404), 'listing-gone', 'failed', false], ['PUT 404', 'PUT', E(404), 'listing-gone', 'failed', false],
    ['PUT 400', 'PUT', E(400, 'sku is too long'), 'rejected', 'failed', false], ['PUT 409', 'PUT', E(409, 'conflict'), 'rejected', 'failed', false], ['GET no connection', 'GET inv', new Error('getaddrinfo ENOTFOUND api.etsy.com'), 'offline', 'failed', true], ['PUT no connection', 'PUT', new Error('socket hang up'), 'offline', 'unverified', true]
  ];
  const TEXT = { scope: 'Etsy refused the change. Paul needs to re-authorise Etsy with listings_w', busy: 'Etsy is busy, try again in a minute', 'etsy-down': 'Etsy did not answer', offline: 'could not reach Etsy', 'listing-gone': 'Etsy no longer has this listing', rejected: 'Etsy did not accept the change' };
  for (const [what, which, err, reason, state, retry, needs] of cases) {
    const W = world({ mirror: which === 'GET listing' ? false : 'active', sim: { fail: c => ((which === 'PUT' && c.method === 'PUT') || (which === 'GET inv' && c.path.endsWith('/inventory') && c.method === 'GET') || (which === 'GET listing' && !c.path.endsWith('/inventory')) ? err : null) } });
    const r = await W.run();
    assert.strictEqual(r.ok, false, what); assert.strictEqual(r.reason, reason, what + ': ' + JSON.stringify(r)); assert.strictEqual(r.state, state, what); assert.strictEqual(r.retry, retry, what); assert.strictEqual(r.text, TEXT[reason], what);
    if (needs) assert.strictEqual(r.needs, needs, what); else assert(!('needs' in r), what);
    if (reason === 'rejected') assert(r.details && r.details.length, 'Etsy\'s own words are kept');
    assert(W.S.puts.length <= 1, what + ': never a second PUT'); assert.strictEqual(W.etsyDoc().state, state, what); assert.strictEqual(W.etsyDoc().reason, reason, what);
    if (which !== 'PUT') assert.strictEqual(W.S.puts.length, 0, what + ': nothing was sent'); else assert.strictEqual(r.beforePath, 'charmnest/custom-saves/' + LID + '/R1-before.json', what + ': the before-copy exists for the retry');
    assert(!('runningUntil' in W.etsyDoc()), what + ': lease released');
  }
  // 401/403 then fixed: the retry is a whole new run with a new before-copy (checked in the before-copy test), ending linked
  const W = world({ sim: { fail: c => (c.method === 'PUT' && W.S.puts.length === 1 ? etsyErr(403) : null) } });
  assert.strictEqual((await W.run()).reason, 'scope'); const ok = await W.run(); assert.strictEqual(ok.ok, true); assert.strictEqual(W.S.puts.length, 2);
});
test('timeouts: 8 s per call by default (here shortened); a hung PUT is "unverified" and its retry only looks', async () => {
  const W = world({ timeoutMs: 40, sim: { hang: c => c.method === 'PUT' && W.S.puts.length === 1 } });
  const t0 = Date.now(), r = await W.run();
  assert.strictEqual(r.ok, false); assert.strictEqual(r.reason, 'etsy-down'); assert.strictEqual(r.state, 'unverified'); assert.strictEqual(r.text, 'Etsy did not answer'); assert.strictEqual(r.retry, true); assert(Date.now() - t0 < 2000);
  assert.strictEqual(W.etsyDoc().state, 'unverified'); assert(blobs.has(r.beforePath));
  const g = await W.run(); assert.strictEqual(g.ok, true, JSON.stringify(g)); assert.strictEqual(g.calls, 2, 'the retry skipped the listing-state check and read the inventory, then wrote');
  assert.strictEqual(W.S.puts.length, 2, 'the listing still had the old SKU (the hung PUT never landed), so the retry wrote again from a fresh before-copy'); assert(blobs.has('charmnest/custom-saves/' + LID + '/R1-before-2.json'));
  const H = world({ timeoutMs: 40, sim: { hang: c => c.method === 'GET' && c.path.endsWith('/inventory') } }); const rh = await H.run();
  assert.strictEqual(rh.reason, 'etsy-down'); assert.strictEqual(rh.state, 'failed'); assert.strictEqual(H.S.puts.length, 0);
  // the default is 8 s
  assert.strictEqual(fs.readFileSync(path.join(fnDir, '_charmNestEtsySku.js'), 'utf8').match(/TIMEOUT_MS = (\d+)/)[1], '8000');
});
test('the body is checked before it leaves: an unreadable price, quantity or offering means no backup, no PUT', async () => {
  const shapes = {
    'a price Etsy did not give': g => { g.products[0].offerings[0].price = { amount: null, divisor: 100 }; },
    'a quantity that is not a number': g => { g.products[0].offerings[0].quantity = 'many'; },
    'a product with no offering': g => { g.products[0].offerings = []; },
    'a price of nothing': g => { g.products[0].offerings[0].price = money(0); }
  };
  for (const [what, shape] of Object.entries(shapes)) {
    const W = world({ sim: { shape } }), r = await W.run();
    assert.strictEqual(r.ok, false, what); assert.strictEqual(r.reason, 'internal', what + ': ' + JSON.stringify(r)); assert.strictEqual(r.retry, false); assert.strictEqual(W.S.puts.length, 0, what); assert.strictEqual(fileWrites.length, 0, what + ': nothing written to Storage'); assert(r.details && r.details.length, what);
  }
});
test('the cap is a counter in the code: a run that would need a fourth call stops before it', async () => {
  for (const cap of [1, 2]) {
    const W = world({ mirror: false }), mod = Mod.make({ db, FV, bucket, etsy: W.E.api, meter: { bumpSimple() {} }, now: Date.now, maxCalls: cap });
    const r = await mod.customEtsySkuWrite({ rid: 'R1' });
    assert.strictEqual(r.ok, false); assert.strictEqual(r.reason, 'internal'); assert.strictEqual(r.calls, cap); assert.strictEqual(W.S.calls.length, cap); assert.strictEqual(W.S.puts.length, 0, 'a PUT is never the call that went over');
  }
  const W = world({ mirror: false }), up = Mod.make({ db, FV, bucket, etsy: W.E.api, meter: { bumpSimple() {} }, now: Date.now, maxCalls: 50 });
  assert.strictEqual((await up.customEtsySkuWrite({ rid: 'R1' })).calls, 3, 'a test knob can lower the cap, never raise it');
});
test('the cap of 3 Etsy calls: the full path without the mirror uses exactly 3', async () => {
  const W = world({ mirror: false, sim: { echo: 'empty' } }); let n = 0;
  const api = { get: (...a) => { n++; return W.E.api.get(...a); }, put: (...a) => { n++; return W.E.api.put(...a); } };
  const mod = Mod.make({ db, FV, bucket, etsy: api, meter: { bumpSimple() {} }, now: Date.now });
  const r = await mod.customEtsySkuWrite({ rid: 'R1' }); assert.strictEqual(r.calls, 3); assert.strictEqual(n, 3);
});

/* ═══ 8 · the page's table, the alias, the meter ═══ */
test('on linked: Charm_Listing_Skus is rebuilt from the read-back, the alias note added without touching its answer, the meter flushed', async () => {
  let flushed = 0;
  const W = world({ inventory: subset(), doc: { oldSku: 'CUSTOM_6673', group: { mode: 'some' } } });
  store.set('Charm_Listing_Skus/' + LID, { at: 1, n: 99, uni: 'STALE' });
  const mod = Mod.make({ db, FV, bucket, etsy: W.E.api, meter: { bumpSimple() {}, flushNow: async () => { flushed++; } }, now: () => 1760000000000 + (++tick) });
  const r = await mod.customEtsySkuWrite({ rid: 'R1' }); assert.strictEqual(r.ok, true, JSON.stringify(r));
  assert.strictEqual(flushed, 1, 'the call counters are flushed once');
  const t = store.get('Charm_Listing_Skus/' + LID);
  assert.deepStrictEqual(t, ListingSkus.toStored(ListingSkus.compact(W.S.lastEcho, t.at)), 'exactly the table the page\'s own lookup would have made from that answer (SKUs as Etsy serves them)');
  assert.strictEqual(t.n, 6); assert.deepStrictEqual(t.products.map(p => p.sku), [NEW, NEW, 'FOX_HEAD_4821', 'FOX_HEAD_4822', '', 'ZOE&#39;S &amp; CO'], 'one row per product with the SKU Etsy now has (the page sees it at once)');
  assert.deepStrictEqual(t.products.map(p => p.id), W.S.stored.products.map(p => String(p.product_id)), 'with the ids Etsy now has');
  assert.strictEqual(t.uni, undefined); assert(Array.isArray(t.products[0].pv) && t.products[0].pv.every(x => typeof x === 'string'), 'stored as strings (no array in an array)');
  const a = store.get('Charm_Sku_Aliases/' + LID);
  assert.deepStrictEqual(a.bySku, { [OLD]: NEW }, 'the alias answer is untouched'); assert.strictEqual(a.keep.rid, 'R1'); assert.strictEqual(a.keep.sku, NEW); assert.strictEqual(a.keep.by, 'Paul');
  assert.deepStrictEqual({ state: a.keep.etsy.state, rid: a.keep.etsy.rid, sku: a.keep.etsy.sku }, { state: 'linked', rid: 'R1', sku: NEW });
  // no alias record: none is created
  const N = world(); store.delete('Charm_Sku_Aliases/' + LID); assert.strictEqual((await N.run()).ok, true); assert.strictEqual(store.has('Charm_Sku_Aliases/' + LID), false);
  // the rest of the save record is untouched
  const d = W.doc(); assert.strictEqual(d.phase, 'repo'); assert.strictEqual(d.sku, NEW); assert.strictEqual(d.rehearsal, false); assert.strictEqual(d.aiPath, 'charmnest/master/custom/' + NEW + '.ai');
});
test('a failing side write after the SKU is on Etsy never turns the answer into a failure', async () => {
  const flaky = { collection: c => ({ doc: id => { const ref = db.collection(c).doc(id); return Object.assign({}, ref, { set: async (...a) => { if (c === 'Charm_Listing_Skus') throw new Error('down'); return ref.set(...a); }, update: async d => { if (c === 'Charm_Sku_Aliases') throw new Error('down'); return ref.update(d); } }); }, }), runTransaction: fn => db.runTransaction(fn) };
  const w = world({ db: flaky }); const warn = console.warn; console.warn = () => {};
  try { const r = await w.run(); assert.strictEqual(r.ok, true); assert.strictEqual(r.state, 'linked'); assert.strictEqual(w.etsyDoc().state, 'linked'); } finally { console.warn = warn; }
});

/* ═══ 9 · the real wiring: charmNestLibrary → ops → the real etsyFetch against a fake node-fetch and a made-up token ═══ */
test('wiring: the Library answers the op (sandbox refused, no record refused, then a full write through the real etsyFetch with a made-up token)', async () => {
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
    const lib = require(path.join(fnDir, 'charmNestLibrary.js'));
    assert.strictEqual(lib.ops.customEtsySkuWrite, Mod.ops.customEtsySkuWrite, 'the Library\'s op table carries this module\'s op');
    const post = b => lib.handler({ httpMethod: 'POST', headers: {}, body: JSON.stringify(b), queryStringParameters: {} }).then(r => ({ status: r.statusCode, body: JSON.parse(r.body) }));
    const W = world(); current = W.E;
    store.set('config/etsyOauth', { access_token: 'made-up-token', refresh_token: 'made-up-refresh', expires_at_ms: Date.now() + 3600e3 });
    writes.length = 0;
    let r = await post({ op: 'customEtsySkuWrite', rid: 'R1', sandbox: true });
    assert.strictEqual(r.status, 200); assert.strictEqual(r.body.ok, false); assert.strictEqual(r.body.reason, 'sandbox'); assert.strictEqual(requests.length, 0); assert.strictEqual(writes.length, 0);
    r = await post({ op: 'customEtsySkuWrite', rid: 'NOPE' });
    assert.strictEqual(r.status, 200); assert.strictEqual(r.body.reason, 'no-save'); assert.strictEqual(requests.length, 0);
    r = await post({ op: 'customEtsySkuWrite', rid: 'R1' });
    assert.strictEqual(r.status, 200, JSON.stringify(r.body)); assert.strictEqual(r.body.ok, true, JSON.stringify(r.body)); assert.strictEqual(r.body.state, 'linked'); assert.strictEqual(r.body.calls, 2);
    assert.deepStrictEqual(requests.map(q => q.method + ' ' + q.path + q.search), ['GET /v3/application/listings/' + LID + '/inventory?legacy=false', 'PUT /v3/application/listings/' + LID + '/inventory?legacy=false'], 'the real etsyFetch, the right URLs');
    assert(requests.every(q => q.host === 'api.etsy.com' && q.auth === 'Bearer made-up-token' && q.key === 'test-key:test-secret'), 'only the made-up token and key');
    assert.deepStrictEqual(JSON.parse(requests[1].body), expectedBody(W.S.lastGet, () => true, NEW), 'the body that went over the wire');
    assert.strictEqual(W.S.stored.products[0].sku, NEW);
    r = await post({ op: 'customEtsySkuWrite', rid: 'R1' });
    assert.strictEqual(r.body.state, 'linked'); assert.strictEqual(r.body.calls, 0); assert.strictEqual(requests.length, 2, 'again: no Etsy call');
    // a refusal from Etsy through the real helper keeps its status: 403 is "needs listings_w"
    const W2 = world({ sim: { fail: c => (c.method === 'PUT' ? etsyErr(403, 'not allowed') : null) } }); current = W2.E; requests.length = 0;
    r = await post({ op: 'customEtsySkuWrite', rid: 'R1' });
    assert.strictEqual(r.status, 200); assert.strictEqual(r.body.reason, 'scope'); assert.strictEqual(r.body.needs, 'listings_w'); assert.strictEqual(requests.length, 2);
  } finally { Module._load = realLoad; }
});

let finished = false;
process.on('exit', () => { if (!finished) { console.error('save-etsy: the run ended before the last case (a promise that never settled?)'); process.exitCode = 1; } });
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
  console.log('save-etsy OK: ' + n + ' cases, ' + sim.every.length + ' fake Etsy requests, the most in any one run: ' + maxCalls);
  process.exit(0);
})().catch(e => { console.error(e); finished = true; process.exit(1); });
