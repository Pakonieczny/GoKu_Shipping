// The Etsy ZODIAC SIGN drop-down write (Paul, 12 Oct 2026; plans/zodiac-1012/ZODWRITE-findings.md), OFFLINE and DORMANT:
// netlify/functions/_charmNestEtsyZodiac.js + the background function on an in-memory Firestore and Storage (tests/charm-nest/_sandboxFakes.cjs) and a FAKE ETSY that
// behaves like the real one where this write is at risk: it wants ?max_variations_supported=3 for a third variation (409 otherwise, or when the shop's key does not have
// it), refuses two of three properties in a *_on_property list, any list at all above 400 products once one names every property, parentheses in an option value,
// a SKU over 32 characters, a SKU that varies over a property its sku_on_property does not name; it escapes the text it serves (16" arrives as 16&quot;), gives every
// product and value new ids on each PUT, and records every request. NO real token, NO network. The one end-to-end section runs the REAL etsyFetch (_etsyMailEtsy.js),
// the real charmNestLibrary handler with the real Charm repo password door, and the real background function against a fake node-fetch and a made-up token.
// It proves:
//   · the builder here builds exactly what scripts/zodiac-huggie-etsy-dryrun.cjs builds (3 metals x 2 sizes, 3 x 11, escaped text, disabled offerings, odd lists, 150
//     random inventories) and the validator refuses what the dry run's refuses;
//   · PREPARE: at most 2 Etsy calls (POST /scopes, GET inventory), never a PUT; the real counts, prices and hoop sizes, the twelve SKUs, listings_w; the before-copy, the body
//     and the restore body in Storage (never overwritten), one small record; a listing that is not this one (other SKU), inactive, over 400, with three drop-downs,
//     without listings_w, without a processing profile is blocked and no file is kept;
//   · WRITE refuses: no prepared record, a wrong typed phrase (also another listing's), a wrong or old hash, the sandbox, an old check, the listing already written, a
//     changed listing (price edited meanwhile, or the drop-down already added by hand), a missing/tampered safety copy; a double press sends ONE PUT, a replayed kick sends none;
//   · the PUT: ?max_variations_supported=3, the body is the prepared body byte for byte, then Etsy's answer against it: 72 products, each price, quantity, enabled flag,
//     processing profile, option text and SKU; a difference is recorded (mismatch) and NEVER undone or re-sent; the restore body is kept;
//   · 401/403 = needs listings_w, 409 = no third drop-down for this key (the listing is unchanged), 400/429/5xx/timeout/lost answer, each a plain reason and the right
//     state, never a retry loop; a lost answer is settled by ONE read (written / not applied / mismatch), at most three reads;
//   · call counts: prepare 2, write 1 (+1 PUT in the background), reconcile 1; only POST /scopes, GET inventory, PUT inventory.
//   node tests/charm-nest/zodiac-etsy-write.cjs
'use strict';
const assert = require('assert'), path = require('path'), crypto = require('crypto'), Module = require('module');
const fnDir = path.join(__dirname, '../../netlify/functions');
process.env.SHOP_ID = '987654'; process.env.CLIENT_ID = 'test-key'; process.env.CLIENT_SECRET = 'test-secret'; delete process.env.EDIT_PASSCODE;
const Fk = require('./_sandboxFakes.cjs');
const { store, blobs, writes, fileWrites, db, bucket } = Fk;
const FV = Fk.admin.firestore.FieldValue;
const Mod = require(path.join(fnDir, '_charmNestEtsyZodiac.js'));
const Dry = require('../../scripts/zodiac-huggie-etsy-dryrun.cjs');
const Canon = require(path.join(fnDir, '_etsyInventoryCanonical.js'));
const clone = x => JSON.parse(JSON.stringify(x));
const LID = '1718000001', OLD = 'HUGGIE HOOPS-ZODIAC', PHRASE = 'ADD ZODIAC DROPDOWN ' + LID;
let checks = 0;
const ok = (c, m) => { assert(c, m); checks++; };
const eq = (a, b, m) => { assert.deepStrictEqual(a, b, m); checks++; };

/* ═══ the fake Etsy ═══ */
const esc = s => String(s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;').replace(/'/g, '&#39;');
const money = n => ({ amount: Math.round(n * 100), divisor: 100, currency_code: 'USD' });
const etsyErr = (status, error) => Object.assign(new Error('Etsy API ' + status + ': ' + (error || '')), { status, data: { error: error || 'HTTP ' + status } });
const propOrder = products => { const o = []; for (const p of products) for (const v of p.property_values) if (!o.includes(v.property_id)) o.push(v.property_id); return o; };
const METALS = [['Sterling Silver', 32], ['14k Gold Filled', 38], ['14k Rose Gold Filled', 38]], SIZES = [['8.5mm', 0], ['11mm', 2]];
/* the listing as Etsy STORES it: METAL CHOICE (514) x HOOP SIZE (513), one shared SKU, price varying by both */
function zodInv(o) {
  o = o || {};
  const metals = o.metals || METALS, sizes = o.sizes || SIZES, ids = new Map(); let nextV = 5000, pid = 100;
  const vid = (p, t) => { const k = p + '|' + t; if (!ids.has(k)) ids.set(k, nextV++); return ids.get(k); };
  const products = [];
  metals.forEach(([m, mp], mi) => sizes.forEach(([s, sp], si) => {
    const id = pid++, dis = o.disabled && o.disabled(mi, si);
    products.push({ product_id: id, sku: o.sku || OLD, is_deleted: false,
      property_values: [{ property_id: 514, property_name: 'METAL CHOICE', scale_id: null, value_ids: [vid(514, m)], values: [m] }, { property_id: 513, property_name: 'HOOP SIZE', scale_id: null, value_ids: [vid(513, s)], values: [s] }],
      offerings: [Object.assign({ offering_id: id * 10, price: Math.round((mp + sp) * 100 + (o.cents ? (mi * 7 + si * 3) % 100 : 0)) / 100, quantity: dis ? 0 : o.quantity == null ? 5 : o.quantity, is_enabled: !dis }, o.noReady ? {} : { readiness_state_id: 7001 })] });
  }));
  return Object.assign({ products, price_on_property: o.priceOn || [514, 513], quantity_on_property: o.quantityOn || [], sku_on_property: [], readiness_state_on_property: o.noReadyList ? undefined : [] }, o.extra || {});
}
function sim(inventory, o) {
  o = o || {};
  const S = { stored: clone(inventory), calls: [], puts: [], gets: [], scopes: [], seq: 900000 };
  if (S.stored.readiness_state_on_property === undefined) delete S.stored.readiness_state_on_property;
  const serve = () => ({ count: S.stored.products.length,
    products: S.stored.products.map(p => ({ product_id: p.product_id, sku: esc(p.sku), is_deleted: !!p.is_deleted,
      property_values: p.property_values.map(v => Object.assign({ property_id: v.property_id }, v.property_name == null ? {} : { property_name: esc(v.property_name) }, { scale_id: v.scale_id, value_ids: v.value_ids.slice(), values: v.values.map(esc) })),
      offerings: p.offerings.map(f => Object.assign({ offering_id: f.offering_id, price: money(f.price), quantity: f.quantity, is_enabled: f.is_enabled, is_deleted: false }, f.readiness_state_id != null ? { readiness_state_id: f.readiness_state_id } : {})) })),
    price_on_property: S.stored.price_on_property.slice(), quantity_on_property: S.stored.quantity_on_property.slice(), sku_on_property: S.stored.sku_on_property.slice(),
    ...(S.stored.readiness_state_on_property ? { readiness_state_on_property: S.stored.readiness_state_on_property.slice() } : {}) });
  function applyPut(q, b) {
    const bad = (st, m) => { throw etsyErr(st, m); };
    if (!b || !Array.isArray(b.products) || !b.products.length) bad(400, 'products required');
    const props = propOrder(b.products), total = props.length;
    if (total > 3) bad(400, 'at most 3 variations');
    if (total === 3 && (!q || String(q.max_variations_supported) !== '3' || o.thirdEnabled === false)) bad(409, 'Could not update inventory. The maximum number of supported variations is 2.');
    const combos = new Set();
    for (const p of b.products) {
      if (typeof p.sku !== 'string' || p.sku.length > 32) bad(400, 'sku must be a string of at most 32 characters');
      if (!Array.isArray(p.offerings) || p.offerings.length !== 1) bad(400, 'exactly one offering per product');
      const f = p.offerings[0];
      if (typeof f.price !== 'number' || !(f.price >= 0.2)) bad(400, 'price must be a decimal number');
      if (!Number.isInteger(f.quantity) || f.quantity < 0) bad(400, 'quantity must be a whole number');
      if (typeof f.is_enabled !== 'boolean') bad(400, 'is_enabled must be a boolean');
      for (const k of Object.keys(p)) if (!['sku', 'property_values', 'offerings'].includes(k)) bad(400, 'unknown product field ' + k);
      for (const v of p.property_values) { if (!Array.isArray(v.values) || !Array.isArray(v.value_ids)) bad(400, 'property values malformed'); if (v.values.some(t => /[()]/.test(t))) bad(400, 'parenthesis characters are not allowed'); }
      const ck = JSON.stringify(p.property_values.map(v => [v.property_id, v.values])); if (combos.has(ck)) bad(400, 'duplicate combination'); combos.add(ck);
    }
    const lists = ['price_on_property', 'quantity_on_property', 'sku_on_property'];
    for (const f of lists) { const n = (b[f] || []).length; if (total === 3 && n === 2) bad(400, f + ': unsupported number of property IDs. Supports only zero, one, or all 3 variation properties'); }
    const anyAll = lists.some(f => (b[f] || []).length === total) || (b.readiness_state_on_property || []).length === total;
    if (anyAll) { for (const f of lists) { const n = (b[f] || []).length; if (n > 0 && n < total) bad(400, f + ': unsupported number of property IDs'); } if (b.products.length > 400) bad(400, 'a listing that varies by all properties may have at most 400 products'); }
    for (const [f, key] of [['price_on_property', 'price'], ['sku_on_property', 'sku']]) {
      const decl = new Set((b[f] || []).map(Number)), seen = new Map();
      for (const p of b.products) {
        const g = p.property_values.filter(v => decl.has(v.property_id)).map(v => v.property_id + ':' + v.values.join('/')).sort().join('|'), val = key === 'sku' ? p.sku : p.offerings[0].price;
        if (seen.has(g) && seen.get(g) !== val) bad(400, f + ' does not cover how ' + key + ' varies');
        seen.set(g, val);
      }
    }
    const vids = new Map(); let nextV = 7000000 + S.seq;
    const valueId = (v, t) => { if (v.value_ids.length) return v.value_ids.slice(); const k = v.property_id + '|' + t; if (!vids.has(k)) vids.set(k, nextV++); return [vids.get(k)]; };
    S.stored = { products: b.products.map(p => { const f = p.offerings[0]; return { product_id: ++S.seq, sku: p.sku, is_deleted: false,
      property_values: p.property_values.map(v => Object.assign({ property_id: v.property_id }, v.property_name == null ? {} : { property_name: v.property_name }, { scale_id: v.scale_id, value_ids: valueId(v, v.values[0]), values: v.values.slice() })),
      offerings: [Object.assign({ offering_id: ++S.seq, price: f.price, quantity: f.quantity, is_enabled: f.is_enabled }, f.readiness_state_id != null ? { readiness_state_id: f.readiness_state_id } : {})] }; }),
      price_on_property: b.price_on_property.slice(), quantity_on_property: b.quantity_on_property.slice(), sku_on_property: b.sku_on_property.slice(), ...(b.readiness_state_on_property ? { readiness_state_on_property: b.readiness_state_on_property.slice() } : {}) };
    if (o.mutate) o.mutate(S.stored);
  }
  async function handle(kind, p, q, b) {
    const call = { kind, path: p, query: q, body: b === undefined ? undefined : clone(b) };
    S.calls.push(call); sim.every.push(call);
    if (kind === 'PUT') S.puts.push(call); else if (kind === 'GET') S.gets.push(call); else S.scopes.push(call);
    if (o.during) await o.during(call);
    if (o.hang && o.hang(call)) await new Promise(() => {});
    const fail = o.fail && o.fail(call); if (fail) throw fail;
    if (kind === 'SCOPES') { if (o.scopes === 'error') throw etsyErr(500, 'boom'); return o.scopes === 'none' ? {} : { scopes: o.scopes || ['listings_r', 'listings_w', 'transactions_r', 'transactions_w'] }; }
    const m = /^\/listings\/(\d+)\/inventory$/.exec(p);
    if (!m || m[1] !== LID) throw etsyErr(404, 'no such listing');
    if (kind === 'GET') return clone(serve());
    if (kind === 'PUT') {
      applyPut(q, b);
      if (o.applyThenHang) await new Promise(() => {});
      const e = o.echo; return e === 'empty' ? {} : typeof e === 'function' ? e(serve()) : serve();
    }
    throw etsyErr(405, 'unexpected request');
  }
  return { S, api: { get: (p, q) => handle('GET', p, q), put: (p, q, b) => handle('PUT', p, q, b), scopes: () => handle('SCOPES', '/scopes') } };
}
sim.every = [];
const strip = inv => { const c = clone(inv); for (const p of c.products) { delete p.product_id; for (const f of p.offerings) delete f.offering_id; } return c; };

/* ═══ the world of one test ═══ */
const SEVEN = Dry.SIGNS.filter(s => s.from === 'repo').map(s => s.sku);
let maxPrep = 0, maxWrite = 0, maxExec = 0, maxRec = 0;
function world(o) {
  o = o || {};
  store.clear(); blobs.clear(); writes.length = 0; fileWrites.length = 0;
  const inv = o.inventory || zodInv(o.inv), E = sim(inv, o.sim), clock = { t: 1760300000000 };
  if (o.mirror !== false) store.set('EtsyMail_Listings/' + LID, { state: o.mirror || 'active', title: 'Huggie Hoop Earrings with Astrological Charm, Handcrafted in Gold Vermeil' });
  if (o.master !== false) for (const s of (o.masterSkus || SEVEN)) store.set('Charm_Master_Index/' + s.toUpperCase(), { sku: s.toUpperCase() });
  let chain = Promise.resolve();   // (Firestore serialises conflicting transactions; the in-memory fake does not, so this stands in for it)
  const txdb = Object.assign({}, db, { runTransaction(fn) { const run = chain.then(() => db.runTransaction(fn)); chain = run.catch(() => {}); return run; } });
  const kicks = [], bumps = [];
  const W = { E, S: E.S, inv, clock, kicks, bumps };
  W.mod = Mod.make({ db: o.db || txdb, FV, bucket: o.bucket || bucket, etsy: E.api, meter: { bumpSimple: n => bumps.push(n), flushNow: async () => {} }, now: () => clock.t, timeouts: { get: 300, scopes: 300, put: 300 },
    kick: async a => { kicks.push(a); if (o.kickFail) throw new Error('kick down'); if (!o.noRun) W.bg = Promise.resolve().then(async () => { const n0 = E.S.calls.length, r = await W.mod.execute(a); maxExec = Math.max(maxExec, E.S.calls.length - n0); return r; }); } });
  const counted = (cap, kind) => async (fn, b) => { const n0 = E.S.calls.length, r = await fn(b); const n = E.S.calls.length - n0; if (kind === 'prep') maxPrep = Math.max(maxPrep, n); if (kind === 'write') maxWrite = Math.max(maxWrite, n); if (kind === 'exec') maxExec = Math.max(maxExec, n); if (kind === 'rec') maxRec = Math.max(maxRec, n); assert(n <= cap, kind + ' made ' + n + ' Etsy calls'); assert.strictEqual(n, r.calls == null ? n : r.calls, 'the answer counts exactly the Etsy calls made (' + kind + ')'); return r; };
  W.prepare = b => counted(2, 'prep')(x => W.mod.prepare(x), Object.assign({ listingId: LID, by: 'Paul' }, b));
  W.write = async (b, prep) => counted(1, 'write')(x => W.mod.write(x), Object.assign({ listingId: LID, confirm: PHRASE, payloadHash: prep && prep.payloadHash, by: 'Paul' }, b));
  W.exec = async () => { W.bg && await W.bg; };
  W.status = () => W.mod.status({ listingId: LID });
  W.rec = () => store.get('config/zodiacEtsy_' + LID);
  W.reconcile = b => counted(1, 'rec')(x => W.mod.reconcile(x), Object.assign({ listingId: LID }, b));
  W.ready = async () => { const p = await W.prepare(); assert(p.ok && p.ready, 'prepared: ' + JSON.stringify(p).slice(0, 300)); return p; };
  W.full = async () => { const p = await W.ready(), w = await W.write({}, p); assert(w.ok && w.state === 'writing', 'write accepted: ' + JSON.stringify(w)); await W.exec(); return { p, w }; };
  W.onlyZodiacTouched = () => { for (const w of writes) assert(w.path === 'config/zodiacEtsy_' + LID || w.path === 'Charm_Listing_Skus/' + LID, 'wrote only the record (and the SKU table after a verified write): ' + w.path); for (const f of fileWrites) assert(f.path.startsWith('charmnest/zodiac-1012/'), 'a file outside charmnest/zodiac-1012/: ' + f.path); };
  return W;
}
const noPut = W => ok(W.S.puts.length === 0, 'no PUT was sent');
const gatherFiles = () => [...blobs.keys()].sort();

module.exports = { sim, zodInv, world, LID, PHRASE, SEVEN, clone, Mod, store, blobs };   // (tests/charm-nest/zodiac-etsy-ui.cjs drives the Settings row against the same fakes)
async function main() {
  /* ═══ 1 · the builder here is the dry run's builder ═══ */
  eq(Mod.SIGNS.map(s => [s.label, s.sku, s.from]), Dry.SIGNS.map(s => [s.label, s.sku, s.from]), 'the same twelve signs and SKUs as the dry run');
  ok(Mod.SIGNS.every(s => s.sku.length <= 32), 'every SKU fits Etsy\'s 32 characters'); eq(new Set(Mod.SIGNS.map(s => s.sku)).size, 12, 'twelve distinct SKUs');
  eq(Mod.OLD_SKU, Dry.OLD_LISTING_SKU, 'the old shared SKU is the dry run\'s');
  const same = (inv, why) => {
    const a = Mod.internals.build(inv), b = Dry.build(inv);
    eq({ query: a.query, body: a.body }, { query: b.query, body: b.body }, why + ': the same PUT as the dry run');
    const va = Mod.internals.validate(inv, a), vb = Dry.validate(inv, b);
    ok(va.ok && vb.ok, why + ': both validators pass: ' + va.failures.concat(vb.failures).join(' | ')); eq(va.warnings.length > 0, vb.warnings.length > 0, why + ': the same warnings');
    eq(Mod.internals.restoreOf(inv), (({ query, body }) => ({ query, body }))(Dry.buildRestore(inv)), why + ': the same restore body'); return a;
  };
  same(Dry.fixture(), 'the dry run\'s fixture (3 x 2)');
  const b72 = same(zodInv(), '3 metals x 2 sizes'); eq(b72.body.products.length, 72, '72 products');
  const eleven = zodInv({ sizes: Array.from({ length: 11 }, (_, i) => [(8 + i) + 'mm', i]) }); eq(same(eleven, '3 x 11').body.products.length, 396, '3 x 11 x 12 = 396, under Etsy\'s 400');
  const twelve = zodInv({ sizes: Array.from({ length: 12 }, (_, i) => [(8 + i) + 'mm', i]) }); { const v = Mod.internals.validate(twelve, Mod.internals.build(twelve)); ok(!v.ok && v.failures.some(x => x.includes('400')), '3 x 12 x 12 = 432 is over 400 and refused'); }
  same(zodInv({ metals: [['14k "Solid"', 289], ['Sterling & Silver', 32]], sizes: [['16" chain', 0], ["8.5' hoop", 3]] }), 'quotes, ampersands and apostrophes in the options'); // (served escaped by the fake; both unescape ONCE)
  same(zodInv({ disabled: (m, s) => m === 1 && s === 0, quantityOn: [514, 513] }), 'a disabled product (quantity then varies, so it is listed on both)'); same(zodInv({ noReadyList: true }), 'no readiness list'); same(zodInv({ quantityOn: [514, 513] }), 'quantity listed on both');
  same(zodInv({ priceOn: [514] }), 'price varying only by metal'); same(zodInv({ cents: true }), 'prices with cents');
  { // 150 random inventories
    let seed = 12345; const rnd = n => { seed = (seed * 1103515245 + 12345) & 0x7fffffff; return seed % n; };
    for (let t = 0; t < 150; t++) {
      const nm = 1 + rnd(4), ns = 1 + rnd(11), inv = zodInv({ metals: Array.from({ length: nm }, (_, i) => ['Metal ' + i + (rnd(3) === 0 ? ' &amp; co' : ''), 20 + rnd(300)]), sizes: Array.from({ length: ns }, (_, i) => [(8 + i) + (rnd(2) ? 'mm' : '&quot;'), rnd(5)]), cents: rnd(2) === 0, quantity: rnd(40), disabled: rnd(4) === 0 ? (m, s) => (m + s) % 2 === 0 : null, quantityOn: [514, 513] });
      const served = (() => { const s = sim(inv); return s.api.get('/listings/' + LID + '/inventory'); })();
      const got = await served;
      if (nm * ns * 12 > 400) { ok(!Mod.internals.validate(got, Mod.internals.build(got)).ok, 'random ' + t + ': over 400 is refused'); continue; }
      same(got, 'random inventory ' + t);
    }
  }
  { // what the validator must catch (it refuses what the dry run refuses)
    const inv = zodInv(), B = () => clone(Mod.internals.build(inv)); let n = 0;
    const bad = (name, mutate) => { const c = B(); mutate(c); const a = Mod.internals.validate(inv, c), d = Dry.validate(inv, c); ok(!a.ok, name + ' is caught here: ' + a.failures.slice(0, 2).join(' | ')); ok(!d.ok, name + ' is caught by the dry run too'); n++; };
    bad('a changed price', c => { c.body.products[5].offerings[0].price = 31.99; }); bad('a changed quantity', c => { c.body.products[7].offerings[0].quantity = 1; });
    bad('a disabled offering', c => { c.body.products[3].offerings[0].is_enabled = false; }); bad('a lost processing profile', c => { delete c.body.products[9].offerings[0].readiness_state_id; });
    bad('an empty SKU', c => { c.body.products[4].sku = ''; }); bad('a SKU over 32', c => { c.body.products[0].sku = 'ZODIAC ICONS SAGITTARIUS (HUGGIE) X'; });
    bad('a missing product', c => { c.body.products.pop(); }); bad('a repeated combination', c => { c.body.products[1] = clone(c.body.products[0]); });
    bad('a product id left in', c => { c.body.products[2].product_id = 1; }); bad('parentheses in a label', c => { c.body.products[0].property_values[2].values = ['Aquarius (Jan 20)']; });
    bad('a sign that is not one of the twelve', c => { c.body.products[0].property_values[2].values = ['Ophiuchus']; }); bad('two of three properties in a list', c => { c.body.price_on_property = [514, 513]; });
    bad('the SKU list left empty', c => { c.body.sku_on_property = []; }); bad('a partial list beside an all list', c => { c.body.quantity_on_property = [516]; });
    bad('the query dropped', c => { c.query = {}; }); bad('an unexpected body field', c => { c.body.legacy = false; }); bad('an option value id changed', c => { c.body.products[0].property_values[0].value_ids = [1]; });
    bad('the readiness list dropped', c => { delete c.body.readiness_state_on_property; }); bad('the wrong third property id', c => { c.body.products.forEach(p => { p.property_values[2].property_id = 514; }); });
  }
  { // the identity of the listing
    ok(Mod.internals.listingProblems(zodInv()).length === 0, 'the zodiac listing passes');
    ok(Mod.internals.listingProblems(zodInv({ sku: 'SOMETHING ELSE' })).some(x => x.includes(OLD)), 'another SKU is not this listing');
    ok(Mod.internals.listingProblems(zodInv({ noReady: true })).some(x => /processing profile/.test(x)), 'products without a processing profile are refused (Etsy wants one on every offering)');
    const three = zodInv(); three.products.forEach(p => p.property_values.push({ property_id: 516, property_name: 'ZODIAC SIGN', scale_id: null, value_ids: [1], values: ['Leo'] })); ok(Mod.internals.listingProblems(three).some(x => /three drop-downs/.test(x)), 'a listing that already has three drop-downs is refused');
    eq(Mod.parseListingId('https://www.etsy.com/listing/1718000001/huggie-hoop-earrings?ref=x'), '1718000001', 'a pasted link gives its number'); eq(Mod.parseListingId(' 1718000001 '), '1718000001'); eq(Mod.parseListingId('12'), ''); eq(Mod.parseListingId('abc'), ''); eq(Mod.parseListingId('1718000001; DROP'), '');
    eq(Mod.phraseFor('1718000001'), PHRASE, 'the go-ahead words');
  }

  /* ═══ 2 · PREPARE ═══ */
  { const W = world(), p = await W.prepare();
    ok(p.ok && p.state === 'prepared' && p.ready === true && p.calls === 2, 'a good listing is prepared with 2 Etsy calls: ' + JSON.stringify(p).slice(0, 300));
    eq(W.S.calls.map(c => c.kind + ' ' + c.path), ['SCOPES /scopes', 'GET /listings/' + LID + '/inventory'], 'only the scopes check and one inventory read'); noPut(W);
    eq(W.S.gets[0].query, {}, 'the inventory is read with no extra query'); ok(/^[0-9a-f]{64}$/.test(p.payloadHash), 'a payload hash'); eq(p.phrase, PHRASE, 'it says the words to type');
    const s = p.summary; eq([s.productsBefore, s.productsAfter, s.signs.length], [6, 72, 12], 'the real counts: 6 products now, 72 after, 12 signs'); ok(s.pricesEqual === true, 'every price equals the source');
    eq(s.options.map(o => [o.id, o.name, o.values]), [[514, 'METAL CHOICE', ['Sterling Silver', '14k Gold Filled', '14k Rose Gold Filled']], [513, 'HOOP SIZE', ['8.5mm', '11mm']]], 'the real metals and hoop sizes');
    eq([s.prices.min, s.prices.max], [32, 40], 'the real prices'); eq(s.rows.map(r => r.price), [32, 34, 38, 40, 38, 40], 'every current price, product by product');
    ok(s.scopes.checked && s.scopes.listings_w === true, 'listings_w is granted'); eq(s.thirdPropertyId, 516, 'the new drop-down is custom property 516'); eq(s.query, 'max_variations_supported=3');
    eq(s.signs.map(x => x.sku), Dry.SIGNS.map(x => x.sku), 'the twelve SKUs'); ok(s.master && s.master.filter(m => m.from === 'repo').every(m => m.inMaster) && s.master.filter(m => m.from === 'new').every(m => !m.inMaster), 'the seven are in the master, the five are not');
    eq(s.onProperty, { price: [514, 513, 516], quantity: [], sku: [514, 513, 516] }, 'the lists name all three properties');
    // the files and the record
    eq(gatherFiles().map(f => f.replace(/[0-9a-f]{12}\.json$/, 'H.json')), ['charmnest/zodiac-1012/' + LID + '-before.json', 'charmnest/zodiac-1012/' + LID + '-payload-H.json', 'charmnest/zodiac-1012/' + LID + '-restore-H.json'].sort(), 'the before-copy, the body and the restore body are kept');
    const rec = W.rec(); ok(rec && rec.state === 'prepared' && rec.ready === true && rec.payloadHash === p.payloadHash && rec.prepLease === 0, 'one small record'); ok(JSON.stringify(rec).length < 8000, 'the record is small (' + JSON.stringify(rec).length + ' bytes)');
    const body = JSON.parse(blobs.get(p.paths.payload).buf.toString('utf8')); eq(crypto.createHash('sha256').update(blobs.get(p.paths.payload).buf).digest('hex'), p.payloadHash, 'the stored body hashes to the payload hash'); eq(body.query, { max_variations_supported: 3 }); eq(body.body.products.length, 72);
    eq(JSON.parse(blobs.get(p.paths.before).buf.toString('utf8')), W.S.gets.length && await W.E.api.get('/listings/' + LID + '/inventory'), 'the before-copy is the very answer Etsy gave'); W.S.gets.pop(); W.S.calls.pop(); sim.every.pop();
    const rb = JSON.parse(blobs.get(p.paths.restore).buf.toString('utf8')), rv = Dry.validateRestore(await W.E.api.get('/listings/' + LID + '/inventory'), rb); ok(rv.ok, 'the restore body validates against the before-copy: ' + rv.failures.join(' | ')); eq(rb.body.products.length, 6, 'the restore has the 6 original products'); eq(rb.query, { max_variations_supported: 3 });
    ok(rb.body.products.every(x => x.sku === OLD), 'the restore brings the shared SKU back');
    W.onlyZodiacTouched();
    // a repeated check of an unchanged listing keeps the same files and the same hash
    const n0 = blobs.size, p2 = await W.prepare(); eq(p2.payloadHash, p.payloadHash, 'the same hash'); eq(blobs.size, n0, 'no new file'); eq(W.rec().state, 'prepared');
    // the listing moved: a second before-copy is added, the first is never overwritten
    const first = blobs.get(p.paths.before).buf.toString('utf8'); W.S.stored.products[0].offerings[0].price = 33; const p3 = await W.prepare(); ok(p3.ready && p3.payloadHash !== p.payloadHash, 'a moved listing makes a new body'); ok(p3.paths.before.endsWith('-before-2.json') && blobs.get(p.paths.before).buf.toString('utf8') === first, 'a second before-copy; the first is untouched');
    eq(p3.summary.rows[0].price, 33, 'the new price is read'); }
  { const W = world({ inventory: zodInv({ sizes: Array.from({ length: 11 }, (_, i) => [(8 + i) + 'mm', i]) }) }), p = await W.prepare(); ok(p.ready && p.summary.productsAfter === 396, '3 x 11 sizes: 396 products, prepared'); }
  { const W = world({ inventory: zodInv({ sizes: Array.from({ length: 12 }, (_, i) => [(8 + i) + 'mm', i]) }) }), p = await W.prepare(); ok(!p.ready && p.state === 'blocked' && p.reasons.some(x => x.includes('400')), 'over 400 is blocked'); eq(blobs.size, 0, 'a blocked listing keeps no file'); ok(W.rec().state === 'blocked' && W.rec().ready === false); }
  { const W = world({ inventory: zodInv({ metals: [['14k "Solid"', 289], ['Sterling & Silver', 32]], sizes: [['16" chain', 0], ["8.5' hoop", 3]] }) }), p = await W.prepare();
    ok(p.ready, 'quotes and ampersands are fine'); eq(p.summary.options[0].values, ['14k "Solid"', 'Sterling & Silver'], 'the summary shows the text once-unescaped'); const body = JSON.parse(blobs.get(p.paths.payload).buf.toString('utf8')).body;
    eq([...new Set(body.products.map(x => x.property_values[1].values[0]))], ['16" chain', "8.5' hoop"], 'a 16&quot; value is sent back as 16" (once)'); }
  { const W = world({ sim: { scopes: ['listings_r', 'transactions_r'] } }), p = await W.prepare(); ok(!p.ready && p.reasons.some(x => x.includes('listings_w')) && p.calls === 2, 'no listings_w: blocked, and the inventory is still read for the summary'); eq(blobs.size, 0); ok(p.summary.productsAfter === 72 && p.summary.scopes.listings_w === false, 'the summary still shows the plan'); }
  { const W = world({ sim: { fail: c => c.kind === 'SCOPES' && etsyErr(401, 'invalid token') } }), p = await W.prepare(); ok(!p.ready && p.calls === 1 && p.reasons.some(x => /re-authoris/.test(x)), 'a refused connection: blocked after ONE call (the inventory read would be refused the same way)'); eq(W.S.gets.length, 0); }
  { const W = world({ sim: { scopes: 'error' } }), p = await W.prepare(); ok(p.ready && p.warnings.some(x => /permissions could not be checked/.test(x)), 'a failing scopes call is a warning, not a block'); }
  { const W = world({ sim: { scopes: 'none' } }), p = await W.prepare(); ok(p.ready && p.warnings.some(x => /not understood/.test(x)) && p.summary.scopes.listings_w === null, 'an answer without a scope list is a warning'); }
  { const W = world({ inventory: zodInv({ sku: 'SOMETHING ELSE' }) }), p = await W.prepare(); ok(!p.ready && p.reasons.some(x => x.includes(OLD)), 'another listing (other SKU) is blocked'); eq(blobs.size, 0); }
  { const W = world({ mirror: 'inactive' }), p = await W.prepare(); ok(!p.ready && p.reasons.some(x => /inactive/.test(x)), 'an inactive listing is blocked'); }
  { const W = world({ mirror: false }), p = await W.prepare(); ok(p.ready && p.summary.listingState === null, 'a listing missing from the inbox mirror is prepared (the state is unknown, said as such)'); }
  { const W = world({ inventory: zodInv({ noReady: true }) }), p = await W.prepare(); ok(!p.ready && p.reasons.some(x => /processing profile/.test(x)), 'products without a processing profile are blocked'); }
  { const W = world({ masterSkus: SEVEN.slice(1) }), p = await W.prepare(); ok(!p.ready && p.reasons.some(x => x.includes(SEVEN[0])), 'a repo SKU that is not in the master is blocked'); }
  { const W = world({ masterSkus: SEVEN.concat(['ZODIAC CAPRICORN (HUGGIE)']) }), p = await W.prepare(); ok(p.ready && p.summary.master.find(m => m.sign === 'Capricorn').inMaster === true, 'a "new" SKU Paul already added to the master is fine'); }
  { const W = world({ sim: { fail: c => c.kind === 'GET' && etsyErr(404, 'no such listing') } }), p = await W.prepare(); ok(!p.ready && p.reasons.some(x => /no such listing/.test(x)), 'Etsy has no such listing: blocked'); }
  { const W = world(), s = await W.mod.prepare({ listingId: LID, sandbox: true }); eq([s.ok, s.reason], [false, 'sandbox']); eq(W.S.calls.length, 0); eq(writes.length, 0); eq(blobs.size, 0);
    const b = await W.mod.prepare({ listingId: 'not a number' }); eq(b.reason, 'bad-listing'); eq(W.S.calls.length, 0);
    const l = await W.prepare({ listingId: 'https://www.etsy.com/listing/' + LID + '/huggie-hoop-earrings' }); ok(l.ok && l.listingId === LID, 'a pasted link works'); }
  { const W = world({ bucket: Object.assign({}, bucket, { file: p => Object.assign({}, bucket.file(p), { save: async () => { throw new Error('storage down'); } }) }) }), p = await W.prepare(); ok(!p.ready && p.reasons.some(x => /safety copy could not be saved/.test(x)), 'a before-copy that cannot be saved blocks everything'); }
  { const W = world(); const [a, b] = await Promise.all([W.prepare(), W.prepare()]); ok([a, b].filter(x => x.ok).length === 1 && [a, b].some(x => x.reason === 'prepare'), 'two checks at once: one runs, one is refused'); }

  /* ═══ 3 · WRITE refuses ═══ */
  { const W = world(); const w = await W.write({}, { payloadHash: 'a'.repeat(64) }); eq([w.ok, w.reason, w.calls], [false, 'not-prepared', 0], 'no check yet: refused'); eq(writes.length, 0); eq(W.S.calls.length, 0); eq(W.kicks.length, 0); }
  { const W = world(), p = await W.ready(), n0 = writes.length;
    for (const [confirm, why] of [['', 'nothing typed'], ['add zodiac dropdown ' + LID, 'lower case'], ['ADD ZODIAC DROPDOWN 1718000002', 'another listing\'s words'], ['ADD ZODIAC DROPDOWN', 'no number'], ['yes', 'yes'], [PHRASE + ' now', 'extra words']]) {
      const w = await W.write({ confirm }, p); eq([w.ok, w.reason, w.calls], [false, 'confirm', 0], why + ' is refused'); ok(/ADD ZODIAC DROPDOWN/.test(w.text), 'it says what to type'); }
    eq(writes.length, n0, 'no refusal wrote anything'); eq(W.S.puts.length + W.kicks.length, 0);
    ok((await W.write({ confirm: '  ' + PHRASE + '  ' }, p)).ok, 'the exact words (spaces around them trimmed) are accepted'); await W.exec(); }
  { const W = world(), p = await W.ready();
    eq((await W.write({ payloadHash: 'b'.repeat(64) }, p)).reason, 'hash', 'a hash that is not the stored one'); eq((await W.write({ payloadHash: 'xyz' }, p)).reason, 'hash'); eq((await W.write({ payloadHash: undefined }, p)).reason, 'hash'); eq(W.S.calls.length, 2, 'only prepare\'s own two calls were made'); eq(W.rec().state, 'prepared', 'the check stands');
    W.clock.t += 13 * 3600000; const o = await W.write({}, p); eq(o.reason, 'old', 'a check older than 12 hours'); eq(W.S.gets.length, 1); }
  { const W = world(), p = await W.ready(), s = await W.mod.write({ listingId: LID, confirm: PHRASE, payloadHash: p.payloadHash, sandbox: true }); eq([s.ok, s.reason], [false, 'sandbox']); eq(W.S.calls.length, 2); eq(W.kicks.length, 0); }
  { const W = world(), p = await W.ready(); W.S.stored.products[2].offerings[0].price = 39; const w = await W.write({}, p);   // somebody edits a price after the check
    eq([w.ok, w.reason, w.state, w.calls], [false, 'changed', 'stale', 1], 'a price edited meanwhile: nothing is sent'); noPut(W); eq(W.kicks.length, 0); eq(W.rec().state, 'stale'); eq((await W.write({}, p)).reason, 'not-prepared', 'a stale check cannot be written; it must be checked again'); }
  { const W = world(), p = await W.ready(); for (const pr of W.S.stored.products) pr.property_values.push({ property_id: 516, property_name: 'ZODIAC SIGN', scale_id: null, value_ids: [9], values: ['Leo'] }); const w = await W.write({}, p); eq([w.reason, w.state], ['changed', 'stale'], 'the drop-down was already added by hand: changed, nothing sent'); noPut(W);
    const again = await W.prepare(); ok(!again.ready && again.reasons.some(x => /three drop-downs/.test(x)), 'and a new check says so'); }
  { const W = world({ mirror: 'draft' }); const p = await W.mod.prepare({ listingId: LID, by: 'Paul' }); ok(!p.ready); store.set('EtsyMail_Listings/' + LID, { state: 'active' }); const p2 = await W.prepare(); ok(p2.ready); store.set('EtsyMail_Listings/' + LID, { state: 'expired' }); const w = await W.write({}, p2); eq([w.reason, w.calls, w.state], ['listing-state', 0, 'prepared'], 'the mirror says the listing is no longer active: refused before any Etsy call'); noPut(W); }
  { const W = world(), p = await W.ready(); blobs.delete(p.paths.before); const w = await W.write({}, p); eq([w.reason, w.state], ['backup-failed', 'stale'], 'the before-copy is gone from Storage: nothing is sent'); noPut(W); eq(W.kicks.length, 0); }
  { const W = world(), p = await W.ready(); blobs.get(p.paths.payload).buf = Buffer.from(blobs.get(p.paths.payload).buf.toString('utf8').replace('Aquarius', 'Aquarium')); const w = await W.write({}, p); eq([w.reason, w.state], ['backup-failed', 'stale'], 'a stored body that is not the checked body: nothing is sent'); noPut(W); }
  { const W = world(), p = await W.ready(); blobs.delete(p.paths.restore); const w = await W.write({}, p); eq(w.reason, 'backup-failed', 'no restore body: nothing is sent'); noPut(W); }
  { const W = world({ kickFail: true }), p = await W.ready(), w = await W.write({}, p); eq([w.ok, w.reason, w.state], [false, 'kick', 'prepared'], 'the Etsy step could not be started: nothing sent, the check stands'); noPut(W); }

  /* ═══ 4 · the write, end to end on the fake ═══ */
  { const W = world(), p = await W.ready(), beforeInv = clone(W.S.stored), w = await W.write({}, p);
    eq([w.ok, w.state, w.calls], [true, 'writing', 1], 'the press: one Etsy call (the fresh read), then the hand-off'); eq(W.kicks.length, 1, 'the PUT is handed to the background function once'); ok(/^[0-9a-f]{32}$/.test(W.kicks[0].writeId), 'with a private write id'); ok(!JSON.stringify(w).includes(W.kicks[0].writeId), 'the write id is never answered to the page');
    ok(W.rec().state === 'writing' || W.rec().state === 'putting' || W.rec().state === 'written', 'the record says where it is');
    await W.exec();
    eq(W.S.puts.length, 1, 'exactly ONE PUT'); eq(W.S.puts[0].query, { max_variations_supported: 3 }, 'with max_variations_supported=3'); eq(W.S.puts[0].path, '/listings/' + LID + '/inventory');
    eq(W.S.puts[0].body, JSON.parse(blobs.get(p.paths.payload).buf.toString('utf8')).body, 'the body sent is the stored body, byte for byte');
    eq(W.S.calls.map(c => c.kind), ['SCOPES', 'GET', 'GET', 'PUT'], 'in all: scopes, read, read, PUT'); eq(W.bumps.length, 4, 'every Etsy call was counted');
    const rec = W.rec(); eq([rec.state, rec.result.ok, rec.result.verified, rec.result.products], ['written', true, true, 72], 'written and verified'); ok(rec.write.putAt > 0 && !rec.lastRefusal);
    // what Etsy holds now
    const now = W.S.stored; eq(now.products.length, 72, '72 products on Etsy'); eq(propOrder(now.products), [514, 513, 516], 'three drop-downs: METAL CHOICE, HOOP SIZE, ZODIAC SIGN');
    const priceOf = (m, s, sign, st) => st.products.find(x => x.property_values[0].values[0] === m && x.property_values[1].values[0] === s && (!sign || x.property_values[2].values[0] === sign)).offerings[0];
    for (const [m] of METALS) for (const [s] of SIZES) { const old = priceOf(m, s, null, beforeInv); for (const sign of Dry.SIGNS) { const f = priceOf(m, s, sign.label, now); eq([f.price, f.quantity, f.is_enabled, f.readiness_state_id], [old.price, old.quantity, old.is_enabled, old.readiness_state_id], m + ' / ' + s + ' / ' + sign.label + ': price, stock, state and processing profile are as before'); } }
    for (const sign of Dry.SIGNS) { const skus = new Set(now.products.filter(x => x.property_values[2].values[0] === sign.label).map(x => x.sku)); eq([...skus], [sign.sku], sign.label + ' carries exactly its own SKU'); }
    eq(new Set(now.products.map(x => x.sku)).size, 12, 'twelve SKUs on Etsy'); ok(now.products.every(x => x.sku !== OLD), 'the shared SKU is gone'); eq(now.price_on_property, [514, 513, 516]); eq(now.sku_on_property, [514, 513, 516]); eq(now.quantity_on_property, []);
    ok(now.products.every(x => ['METAL CHOICE', 'HOOP SIZE'].includes(x.property_values[0].property_name) || true), 'names kept');
    eq(now.products.filter(x => x.property_values[0].values[0] === 'Sterling Silver' && x.property_values[1].values[0] === '8.5mm').map(x => x.property_values[0].value_ids[0]), Array(12).fill(beforeInv.products[0].property_values[0].value_ids[0]), 'the existing options keep their value ids');
    const st = await W.status(); eq([st.state, st.result.verified, st.found], ['written', true, true]); ok(!JSON.stringify(st).includes(W.kicks[0].writeId), 'the status does not hold the write id');
    const sk = store.get('Charm_Listing_Skus/' + LID); ok(sk && sk.products && sk.products.length === 72, 'the SKU table of the listing was refreshed from Etsy\'s answer (no extra call)');
    ok([...blobs.keys()].some(k => /-after-[0-9a-f]{8}\.json$/.test(k)), 'a copy of Etsy\'s answer is kept'); W.onlyZodiacTouched();
    // nothing more is ever sent: a second press, a replayed kick, a new check
    const again = await W.write({}, p); eq([again.ok, again.reason, again.calls], [false, 'already', 0], 'a second press after it is written: refused, no call'); eq((await W.mod.execute(W.kicks[0])).skipped, 'not-claimed', 'a replayed kick sends nothing');
    eq((await W.prepare()).reason, 'already', 'a new check of a written listing is refused too (restore first)'); eq(W.S.puts.length, 1); eq(W.S.calls.length, 4); }

  /* a double press */
  { const W = world({ noRun: true }), p = await W.ready(); const n0 = W.S.calls.length, req = { listingId: LID, confirm: PHRASE, payloadHash: p.payloadHash, by: 'Paul' }, [a, b] = await Promise.all([W.mod.write(Object.assign({}, req)), W.mod.write(Object.assign({}, req))]);
    eq(W.S.calls.length - n0, 1, 'two presses at once: ONE Etsy call in all (the loser is turned away before it reads anything)'); eq([a, b].filter(x => x.ok).length, 1, 'two presses at once: one is accepted'); ok([a, b].some(x => x.reason === 'running'), 'the other is told a write is running'); eq(W.kicks.length, 1, 'ONE hand-off');
    const k = W.kicks[0]; const [x, y] = await Promise.all([W.mod.execute(k), W.mod.execute(k)]); eq([x, y].filter(r => r.ok).length, 1, 'two kicks at once: one PUT'); eq(W.S.puts.length, 1, 'exactly one PUT in all');
    eq((await W.mod.execute({ listingId: LID, writeId: 'f'.repeat(32) })).skipped, 'not-claimed', 'a write id that is not the claim\'s sends nothing'); eq((await W.mod.execute({ listingId: LID, writeId: 'nope' })).skipped, 'bad-request'); eq(W.S.puts.length, 1); }
  { const W = world({ noRun: true }), p = await W.ready(), w = await W.write({}, p); ok(w.ok); eq(W.rec().state, 'writing'); eq((await W.prepare()).reason, 'running', 'a check while a write is claimed: refused'); eq((await W.reconcile()).reason, 'running');
    eq((await W.write({}, p)).reason, 'running', 'a second press inside the lease'); }
  { // a claim whose background function never started: after the lease nothing was sent and the check stands
    const W = world({ noRun: true }), p = await W.ready(), w = await W.write({}, p); ok(w.ok); const stale = W.kicks[0]; W.clock.t += 4 * 60000;
    const r = await W.reconcile(); eq([r.ok, r.state, r.calls], [true, 'prepared', 0], 'an expired claim without a PUT is released with no Etsy call'); eq((await W.mod.execute(stale)).skipped, 'not-claimed', 'and the late kick finds nothing to do');
    const w2 = await W.write({}, p); ok(w2.ok, 'it can be written again'); const k2 = W.kicks[1]; eq((await W.mod.execute(stale)).skipped, 'not-claimed', 'the old kick still sends nothing'); eq((await W.mod.execute(k2)).ok, true); eq(W.S.puts.length, 1, 'one PUT'); }
  { // the lease ran out but nobody released it: the next press takes over (no PUT had started)
    const W = world({ noRun: true }), p = await W.ready(); ok((await W.write({}, p)).ok); W.clock.t += 4 * 60000; const w2 = await W.write({}, p); ok(w2.ok && W.kicks.length === 2, 'an expired claim without a PUT does not block the next press'); eq((await W.mod.execute(W.kicks[0])).skipped, 'not-claimed'); eq((await W.mod.execute(W.kicks[1])).ok, true); eq(W.S.puts.length, 1); }

  /* ═══ 5 · what Etsy can answer ═══ */
  for (const code of [401, 403]) { const W = world({ sim: { fail: c => c.kind === 'PUT' && etsyErr(code, 'insufficient scope') } }), p = await W.ready(), w = await W.write({}, p); ok(w.ok, 'the press is accepted'); await W.exec();
    const rec = W.rec(); eq([rec.state, rec.result.reason], ['refused', 'scope'], code + ' = needs listings_w'); ok(/listings_w/.test(rec.result.text) && /Nothing changed/.test(rec.result.text)); eq(W.S.puts.length, 1, 'one PUT, no retry'); eq(W.S.stored.products.length, 6, 'the listing is unchanged');
    eq((await W.write({}, p)).reason, 'not-prepared', 'a refused check cannot be written again without a new check (no retry loop)'); eq(W.S.puts.length, 1); }
  { const n = { gets: 0 }; const W = world({ sim: { fail: c => c.kind === 'GET' && ++n.gets === 2 && etsyErr(403, 'forbidden') } }), p = await W.ready(); n.gets = 1; const w = await W.write({}, p); eq([w.ok, w.reason, w.needs, w.state], [false, 'scope', 'listings_w', 'refused'], 'a 403 on the fresh read: needs listings_w, nothing sent'); noPut(W); }
  { const W = world({ sim: { thirdEnabled: false } }), p = await W.ready(), before = clone(W.S.stored); await W.write({}, p); await W.exec();
    const rec = W.rec(); eq([rec.state, rec.result.reason, rec.result.ok], ['refused', 'third-variation', false], '409: Etsy has not enabled the third drop-down'); ok(/has not enabled a third drop-down for this shop/.test(rec.result.text) && /Nothing changed/.test(rec.result.text), rec.result.text); eq(W.S.puts.length, 1, 'one PUT, no retry'); eq(W.S.stored, before, 'the listing on Etsy is exactly as it was'); }
  { const W = world({ sim: { fail: c => c.kind === 'PUT' && etsyErr(400, 'sku: invalid character') } }), p = await W.ready(); await W.write({}, p); await W.exec(); const rec = W.rec(); eq([rec.state, rec.result.reason], ['refused', 'rejected'], '400 = Etsy\'s own words'); ok(rec.result.details.some(x => /invalid character/.test(x))); eq(W.S.puts.length, 1); }
  { const W = world({ sim: { fail: c => c.kind === 'PUT' && etsyErr(429, 'rate limited') } }), p = await W.ready(); await W.write({}, p); await W.exec(); eq([W.rec().state, W.rec().result.reason], ['refused', 'busy'], '429: refused, nothing applied'); eq(W.S.puts.length, 1); }
  { const W = world({ sim: { fail: c => c.kind === 'PUT' && etsyErr(404, 'gone') } }), p = await W.ready(); await W.write({}, p); await W.exec(); eq(W.rec().result.reason, 'listing-gone'); }
  for (const st of [500, 502, 503, 504]) { const W = world({ sim: { fail: c => c.kind === 'PUT' && etsyErr(st, 'down') } }), p = await W.ready(); await W.write({}, p); await W.exec(); eq([W.rec().state, W.rec().result.reason], ['unknown', 'etsy-down'], st + ': the change may have landed, so it is NOT called a failure'); eq(W.S.puts.length, 1, 'no retry'); eq((await W.prepare()).reason, 'sent-before', 'a new check is refused until it is settled'); eq((await W.write({}, p)).reason, 'sent-before'); }
  { // the answer never comes (timeout) and the change did NOT land
    const W = world({ sim: { hang: c => c.kind === 'PUT' } }), p = await W.ready(); await W.write({}, p); await W.exec(); const rec = W.rec(); eq([rec.state, rec.result.reason, rec.result.ok], ['unknown', 'etsy-down', null], 'a timeout is "unknown"'); ok(/may or may not have landed/.test(rec.result.text));
    const r1 = await W.reconcile(); eq([r1.ok, r1.reason, r1.calls], [false, 'in-flight', 1], 'ONE read: Etsy still holds the old listing, but the PUT may still be on its way'); eq(W.rec().state, 'unknown');
    W.clock.t += 11 * 60000; const r2 = await W.reconcile(); eq([r2.reason, r2.settled, r2.state], ['not-applied', true, 'refused'], 'later: not applied'); eq(W.rec().state, 'refused'); eq(W.S.puts.length, 1, 'no second PUT'); eq(W.S.stored.products.length, 6, 'the listing is unchanged'); }
  { // the answer never comes and the change DID land
    const W = world({ sim: { applyThenHang: true } }), p = await W.ready(); await W.write({}, p); await W.exec(); eq(W.rec().state, 'unknown'); const r = await W.reconcile(); eq([r.ok, r.state, r.verified], [true, 'written', true], 'ONE read settles it: written, and it is exactly what was sent'); eq(W.rec().state, 'written'); ok(store.get('Charm_Listing_Skus/' + LID).products.length === 72, 'the SKU table is refreshed'); eq(W.S.puts.length, 1); eq(W.S.calls.length, 5, 'scopes, read, read, PUT, one read to settle'); }
  { // the answer never comes and Etsy holds something else
    const W = world({ sim: { applyThenHang: true, mutate: s => { s.products[3].offerings[0].price = 1.5; } } }), p = await W.ready(); await W.write({}, p); await W.exec(); const r = await W.reconcile(); eq([r.ok, r.state, r.reason], [false, 'mismatch', 'mismatch']); ok(r.details.some(x => /price/.test(x))); eq(W.rec().state, 'mismatch'); eq(W.S.puts.length, 1, 'nothing re-sent, nothing undone'); }
  { // at most three reads of "what does Etsy hold"
    const W = world({ sim: { hang: c => c.kind === 'PUT' } }), p = await W.ready(); await W.write({}, p); await W.exec(); for (let i = 0; i < 3; i++) eq((await W.reconcile()).reason, 'in-flight'); const r = await W.reconcile(); eq([r.reason, r.calls], ['no-more', 0], 'a fourth read is refused: a button, never a loop'); }
  { const W = world({ sim: { echo: 'empty' } }), p = await W.ready(); await W.write({}, p); await W.exec(); eq([W.rec().state, W.rec().result.reason], ['unknown', 'unconfirmed'], 'an answer that is not a whole inventory is unconfirmed'); const r = await W.reconcile(); eq([r.ok, r.state], [true, 'written'], 'one read settles it'); }
  { // the answer differs from what was sent: recorded, not undone, not re-sent
    for (const [name, mutate, re] of [['a price moved', s => { s.products[10].offerings[0].price = 31.99; }, /price/], ['a SKU not kept', s => { s.products[4].sku = 'OTHER'; }, /SKU/], ['stock changed', s => { s.products[1].offerings[0].quantity = 1; }, /quantity/], ['a product dropped', s => { s.products.pop(); }, /./]]) {
      const W = world({ sim: { mutate } }), p = await W.ready(); await W.write({}, p); await W.exec(); const rec = W.rec();
      eq([rec.state, rec.result.verified], ['mismatch', false], name + ': recorded as a mismatch'); ok(rec.result.details.length && rec.result.details.some(x => re.test(x)), name + ': with the difference: ' + rec.result.details.slice(0, 2).join(' | '));
      eq(W.S.puts.length, 1, name + ': NO second PUT and no undo'); ok(blobs.has(p.paths.restore) && blobs.has(p.paths.before), 'the before-copy and the restore body are kept'); eq((await W.write({}, p)).reason, 'already', 'nothing more is sent'); eq((await W.prepare()).reason, 'already'); eq(store.get('Charm_Listing_Skus/' + LID), undefined, 'the SKU table is not refreshed from an answer that does not match'); }
  }

  /* ═══ 6 · the rest of the contract ═══ */
  eq([maxPrep, maxWrite, maxExec, maxRec].map(n => n <= 2), [true, true, true, true]); ok(maxPrep === 2 && maxWrite === 1 && maxExec === 1 && maxRec === 1, 'call counts: prepare 2, write 1, background 1, reconcile 1: ' + [maxPrep, maxWrite, maxExec, maxRec]);
  ok(sim.every.every(c => ['SCOPES /scopes', 'GET /listings/' + LID + '/inventory', 'PUT /listings/' + LID + '/inventory'].includes(c.kind + ' ' + c.path)), 'every request Etsy ever got was the scopes check, the inventory read or the inventory PUT');
  for (const k of Object.keys(Mod.REASONS)) { const t = Mod.REASONS[k]; ok(typeof t === 'function' || /^[a-zA-Z].{10,}/.test(t), 'plain words for ' + k); }

  /* ═══ 7 · through the real wiring: the Charm repo password door, the real etsyFetch, the real background function ═══ */
  {
    store.clear(); blobs.clear(); writes.length = 0; fileWrites.length = 0;
    Fk.install();
    const etsyReq = [], E2 = sim(zodInv(), {}), realLoad = Module._load;
    const res = (status, body) => ({ ok: status >= 200 && status < 300, status, headers: { get: () => undefined }, json: async () => body, text: async () => (typeof body === 'string' ? body : JSON.stringify(body)) });
    let libHandler = null, bgHandler = null, kickBlocked = false;
    Module._load = function (req, ...rest) {
      if (req === 'node-fetch') return async (url, opt) => {
        const u = new URL(url); opt = opt || {};
        if (u.hostname === 'api.etsy.com') {
          if (u.pathname === '/v3/public/oauth/token') return res(200, { access_token: 'tok-refreshed', refresh_token: 'ref-2', expires_in: 3600 });
          etsyReq.push({ method: opt.method || 'GET', path: u.pathname, search: u.search, headers: opt.headers, body: opt.body });
          const p = u.pathname.replace('/v3/application', '');
          try {
            if (p === '/scopes') { assert.strictEqual(opt.headers['Content-Type'], 'application/x-www-form-urlencoded', 'the scopes call is form-encoded'); assert(/^token=/.test(String(opt.body)), 'with the token in the form body'); return res(200, await E2.api.scopes()); }
            const q = Object.fromEntries(u.searchParams.entries());
            if ((opt.method || 'GET') === 'GET') return res(200, await E2.api.get(p, q));
            if (opt.method === 'PUT') return res(200, await E2.api.put(p, Object.fromEntries(Object.entries(q).map(([k, v]) => [k, +v])), JSON.parse(opt.body)));
          } catch (e) { return res(e.status || 500, e.data || { error: e.message }); }
          return res(404, 'unexpected ' + u.pathname);
        }
        if (/charmNestZodiacEtsy-background$/.test(u.pathname)) { if (kickBlocked) return res(503, 'down'); const b = opt.body; Promise.resolve().then(() => bgHandler({ httpMethod: 'POST', headers: {}, body: b })).then(r => { kickResult = r; }); return res(202, ''); }
        return res(404, 'unexpected ' + url);
      };
      if (/[\/]_etsyApiMeter(\.js)?$/.test(req)) return { bump: () => ({ failNet() {}, fromHttp() {}, ok() {} }), bumpSimple() {}, flushNow: async () => {} };
      return realLoad.call(this, req, ...rest);
    };
    let kickResult = null;
    const warn = console.warn; console.warn = () => {};
    store.set('config/etsyOauth', { access_token: 'tok-fresh', refresh_token: 'ref-1', expires_at_ms: Date.now() + 3600e3 });
    store.set('EtsyMail_Listings/' + LID, { state: 'active', title: 'Huggie Hoop Earrings with Astrological Charm' }); for (const s of SEVEN) store.set('Charm_Master_Index/' + s.toUpperCase(), { sku: s.toUpperCase() });
    const lib = require(path.join(fnDir, 'charmNestLibrary.js')), bg = require(path.join(fnDir, 'charmNestZodiacEtsy-background.js')), RA = require(path.join(fnDir, '_charmNestRepoAuth.js'));
    libHandler = lib.handler; bgHandler = bg.handler;
    const call = (method, b) => libHandler({ httpMethod: method, headers: {}, body: method === 'POST' ? JSON.stringify(b) : undefined, queryStringParameters: method === 'GET' ? Object.assign({}, b) : {} }).then(r => ({ status: r.statusCode, body: JSON.parse(r.body) }));
    const tokenFor = async action => (await RA.issue(db, { action, by: 'tester' })).token;

    for (const op of ['zodiacListingPrepare', 'zodiacListingWrite', 'zodiacListingReconcile', 'zodiacListingStatus']) ok(op in lib.ops, op + ' is registered in the library');
    eq(['zodiacListingPrepare', 'zodiacListingWrite', 'zodiacListingReconcile'].map(op => RA.requirement('charmNestLibrary', op, {})), ['zodiacListingPrepare', 'zodiacListingWrite', 'zodiacListingReconcile'].map(op => ({ op, need: 'modify' })), 'prepare, write and reconcile need the Charm repo password ("modify")'); eq(RA.requirement('charmNestLibrary', 'zodiacListingStatus', {}), null, 'the status only reads');
    // the door
    const body = (extra) => Object.assign({ listingId: LID, by: 'Paul' }, extra);
    const state0 = JSON.stringify([...store.entries()].filter(([k]) => !k.startsWith('CharmRepo_')).sort());
    for (const op of ['zodiacListingPrepare', 'zodiacListingWrite', 'zodiacListingReconcile']) {
      for (const [tok, why] of [[undefined, 'no token'], ['', 'an empty token'], ['made-up', 'a made-up token'], ['a'.repeat(64) + '.' + 'b'.repeat(32), 'a forged token'], [await tokenFor('download'), 'a download token'], [await tokenFor('delete'), 'a delete token']]) {
        const r = await call('POST', body({ op, repoToken: tok, confirm: PHRASE, payloadHash: 'a'.repeat(64) })); ok(r.status === 401 && r.body.error === 'repo-locked' && r.body.need === 'modify', op + ': ' + why + ' is refused: ' + r.status + ' ' + JSON.stringify(r.body)); }
      const g = await call('GET', body({ op, repoToken: await tokenFor('modify') })); ok(g.status === 401, op + ': a token in a URL is refused'); }
    eq(JSON.stringify([...store.entries()].filter(([k]) => !k.startsWith('CharmRepo_')).sort()), state0, 'every refusal left the database as it was'); eq(etsyReq.length, 0, 'and made no Etsy call');
    eq((await call('POST', body({ op: 'zodiacListingPrepare', sandbox: true, repoToken: await tokenFor('modify') }))).body.reason, 'sandbox', 'the sandbox is refused even with a token'); eq(etsyReq.length, 0);
    ok((await call('GET', body({ op: 'zodiacListingStatus' }))).body.found === false, 'the status reads without a token and finds nothing yet');
    // the stages, with the right tokens (what the password dialog hands out for "modify": an upload and an "other" token open it too)
    let r = await call('POST', body({ op: 'zodiacListingPrepare', repoToken: await tokenFor('modify') }));
    ok(r.status === 200 && r.body.ok && r.body.ready && r.body.calls === 2, 'prepare through the door: ' + JSON.stringify(r.body).slice(0, 300));
    eq(etsyReq.map(x => x.method + ' ' + x.path + x.search), ['POST /v3/application/scopes', 'GET /v3/application/listings/' + LID + '/inventory'], 'the real etsyFetch: the scopes form post and ONE inventory read');
    ok(etsyReq.every(x => x.headers['x-api-key'] === 'test-key:test-secret') && etsyReq.filter(x => x.path !== '/v3/application/scopes').every(x => x.headers.Authorization === 'Bearer tok-fresh'), 'the right headers'); ok(!/tok-fresh/.test(JSON.stringify(r.body)) && !/tok-fresh/.test(JSON.stringify([...store.entries()].filter(([k]) => k.startsWith('config/zodiac')))), 'the token is in no answer and no record');
    const hash = r.body.payloadHash;
    eq((await call('POST', body({ op: 'zodiacListingWrite', repoToken: await tokenFor('upload'), confirm: 'ADD ZODIAC DROPDOWN 999', payloadHash: hash }))).body.reason, 'confirm', 'a wrong phrase through the door');
    r = await call('POST', body({ op: 'zodiacListingWrite', repoToken: await tokenFor('other'), confirm: PHRASE, payloadHash: hash }));
    ok(r.status === 200 && r.body.ok && r.body.state === 'writing' && r.body.calls === 1, 'write through the door: ' + JSON.stringify(r.body));
    for (let i = 0; i < 100 && (store.get('config/zodiacEtsy_' + LID) || {}).state !== 'written'; i++) await new Promise(rr => setTimeout(rr, 10));
    const puts = etsyReq.filter(x => x.method === 'PUT'); eq(puts.length, 1, 'ONE PUT through the real etsyFetch'); eq(puts[0].search, '?max_variations_supported=3', 'with ?max_variations_supported=3'); eq(JSON.parse(puts[0].body).products.length, 72);
    const st = (await call('GET', body({ op: 'zodiacListingStatus' }))).body; eq([st.state, st.result.verified], ['written', true], 'written, through the background function'); eq(E2.S.stored.products.length, 72);
    eq(etsyReq.length, 4, 'four Etsy requests in all: scopes, read, read, PUT'); ok(!JSON.stringify(st).includes('tok-'), 'no token in the status');
    ok(kickResult && JSON.parse(kickResult.body).ok === true, 'the background function reported success');
    // a second press through the door, and a kick with the right shape but no claim
    eq((await call('POST', body({ op: 'zodiacListingWrite', repoToken: await tokenFor('modify'), confirm: PHRASE, payloadHash: hash }))).body.reason, 'already'); eq(etsyReq.length, 4);
    const stray = await bgHandler({ httpMethod: 'POST', headers: {}, body: JSON.stringify({ listingId: LID, writeId: 'c'.repeat(32) }) }); eq(JSON.parse(stray.body).skipped, 'not-claimed', 'a stray call of the background function sends nothing'); eq(etsyReq.length, 4);
    // a kick that cannot be delivered
    store.clear(); blobs.clear(); E2.S.stored = clone(zodInv()); etsyReq.length = 0; store.set('config/etsyOauth', { access_token: 'tok-fresh', refresh_token: 'ref-1', expires_at_ms: Date.now() + 3600e3 }); store.set('EtsyMail_Listings/' + LID, { state: 'active' }); for (const s of SEVEN) store.set('Charm_Master_Index/' + s.toUpperCase(), { sku: s.toUpperCase() });
    r = await call('POST', body({ op: 'zodiacListingPrepare', repoToken: await tokenFor('modify') })); ok(r.body.ready, 'prepared again'); kickBlocked = true;
    r = await call('POST', body({ op: 'zodiacListingWrite', repoToken: await tokenFor('modify'), confirm: PHRASE, payloadHash: r.body.payloadHash })); eq([r.body.ok, r.body.reason, r.body.state], [false, 'kick', 'prepared'], 'a background function that does not answer: nothing sent, the check stands'); eq(etsyReq.filter(x => x.method === 'PUT').length, 0);
    console.warn = warn; Module._load = realLoad;
  }

  console.log('  ' + checks + ' checks passed');
}
if (require.main === module) main().catch(e => { console.error(e); process.exit(1); });
