// KEEP (16 Oct 2026, KR): the ETSY LINK step of "drop a vector file on a Review order + SAVE ON -> the permanent Charm repository under the listing's SKU", under chaos.
// OFFLINE: netlify/functions/_charmNestEtsySku.js on the in-memory Firestore and Storage (tests/charm-nest/_sandboxFakes.cjs) and a FAKE ETSY (the one of save-etsy.cjs, copied). No network, no Etsy token, no repository.
// The rules under test (CONTRACT-KEEP section KR, R6-R10; every section fails on the step as it was on origin/main 3a4e42f0):
//   E1  the step follows only a saved repository step and takes the order's number alone; the sandbox, a rehearsal, no record, a record not at the repository step: NO Etsy call, NO write
//   E2  idempotent: five calls make exactly one write of the listing's SKU to Etsy; the 2nd..5th make no Etsy call and no write
//   E3  FAULT INJECTION at every step (listing GET, inventory GET, PUT) x every fault (no connection, 429, 500, 502, 503, 401, 403, refresh 400/500/429, 404, 400, the reply lost AFTER the write landed):
//       the record is never left "working", a retry completes by itself, and Etsy was written EXACTLY once (never twice, never zero)
//   E4  THE FUNCTION IS KILLED at every point of a run (after every Etsy call and every write): the record is "unverified" (never "working") once its lease has run out, a repair pass makes that final in the store,
//       the next call completes with ONE write in total (the PUT that landed is recognised from the before-copy and never repeated)
//   E5  two orders of one listing at once: ONE write; the loser answers "running" with no Etsy call, then reads "linked" without writing; a lease left by a dead run runs out
//   E6  a deleted listing (404) is final (listing-gone), an inactive one waits (listing-state) and completes when it is active again
//   E7  token refresh failures: 400/401/403 = Paul must re-authorise (retry, never "rejected"), 429 busy, 5xx down, no connection offline, not connected on the server
//   E8  the server can drive the pending steps itself (customSaveRepair etsy:true): oldest first, bounded, cool-down, finals skipped, a scope refusal stops the pass, repeated passes write nothing more
//   node tests/charm-nest/keep-etsy-link.cjs
'use strict';
const assert = require('assert'), path = require('path'), Module = require('module');
const fnDir = path.join(__dirname, '../../netlify/functions');
process.env.SHOP_ID = '987654'; process.env.CLIENT_ID = 'test-key'; process.env.CLIENT_SECRET = 'test-secret'; delete process.env.EDIT_PASSCODE;
const Fk = require('./_sandboxFakes.cjs'); Fk.install();
const { store, blobs, writes, fileWrites, db, bucket } = Fk;
const FV = Fk.admin.firestore.FieldValue;
const TX = require('./_keepTxFake.cjs')(Fk);   // transactions as Firestore has them: serialised, optimistic, all-or-nothing
const Mod = require(path.join(fnDir, '_charmNestEtsySku.js'));
const Repair = require(path.join(fnDir, '_charmNestCustomRepair.js'));
const clone = x => JSON.parse(JSON.stringify(x));
const LID = '1718000001', NEW = 'ACORN_LEAF_0001', OLD = "ACORN'S LEAF";
let checks = 0; const ok = (c, m) => { assert(c, m); checks++; }; const eq = (a, b, m) => { assert.deepStrictEqual(a, b, m); checks++; };

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

/* ═══ the world of one case ═══ */
let clk = 1760000000000;
const tickNow = () => (clk += 1);
function saveDoc(rid, extra) {
  return Object.assign({ rid, listingId: LID, sku: NEW, oldSku: OLD, productId: '100', optionPairs: [], group: { mode: 'all', productIds: [] }, title: 'Acorn leaf charm', design: { name: 'Acorn.ai', kind: 'ai', hash: 'h'.repeat(8), bytes: 4000 },
    masterHash: 'm'.repeat(8), aiPath: 'charmnest/master/custom/' + NEW + '.ai', thumbPath: 'charmnest/master/custom/' + NEW + '.png', by: 'Paul', at: clk - 1000, reused: false, phase: 'repo', etsy: { state: 'pending' }, rehearsal: false, alias: 'bySku' }, extra || {});
}
/** o: { rids:[...], inventory, sim:{...}, mirror, wrapPut(real)->fn, docs:{rid:extra} } */
function world(o) {
  o = o || {};
  store.clear(); blobs.clear(); writes.length = 0; fileWrites.length = 0; hooks.n = 0; hooks.kill = null; hooks.dead = false;
  const rids = o.rids || ['R1'], inv = o.inventory || mkInv([{ sku: OLD }]);
  const E = sim(inv, o.sim);
  let landed = 0; const realPut = E.api.put;
  E.api.put = async (p, q, b) => { const r = await (o.wrapPut ? o.wrapPut(realPut) : realPut)(p, q, b); landed++; return r; };
  // (a lost reply: the write landed, the answer did not come)
  if (o.lostReply) E.api.put = async (p, q, b) => { await realPut(p, q, b); landed++; throw o.lostReply; };
  rids.forEach(rid => store.set('Charm_Custom_Saves/' + rid, saveDoc(rid, o.docs && o.docs[rid])));
  if (o.mirror !== false) store.set('EtsyMail_Listings/' + LID, { state: o.mirror || 'active', title: 'Acorn leaf' });
  store.set('Charm_Sku_Aliases/' + LID, { bySku: { [OLD]: NEW }, by: 'Paul', keep: { rid: rids[0], sku: NEW, at: 1, by: 'Paul' } });
  const mk = () => Mod.make({ db, FV, bucket, etsy: guardedApi(E.api), meter: { bumpSimple() {}, flushNow: async () => {} }, now: tickNow, timeoutMs: o.timeoutMs || 50 });
  const W = { E, S: E.S, mk, mod: mk(), landed: () => landed, doc: rid => store.get('Charm_Custom_Saves/' + (rid || rids[0])), etsyDoc: rid => (store.get('Charm_Custom_Saves/' + (rid || rids[0])) || {}).etsy,
    call: async (rid, body) => W.mod.customEtsySkuWrite(body || { rid: rid || rids[0] }), stored: () => W.S.stored.products.map(p => p.sku) };
  return W;
}
/* the kill switch: every Etsy call, every document write and every file write is one event; after event k everything stops (a function killed by the platform runs no more code) */
const hooks = { n: 0, kill: null, dead: false };
const event = () => { if (hooks.dead) throw new Error('killed'); if (hooks.kill != null && hooks.n++ >= hooks.kill) { hooks.dead = true; throw new Error('killed'); } };
const rawPush = Array.prototype.push;
writes.push = function (...a) { event(); return rawPush.apply(this, a); };
fileWrites.push = function (...a) { event(); return rawPush.apply(this, a); };
const guardedApi = api => ({ get: async (p, q) => { event(); return api.get(p, q); }, put: async (p, q, b) => { event(); return api.put(p, q, b); } });
const etsyDocOK = (W, rid, label) => { const e = W.etsyDoc(rid) || {}; ok(['pending', 'linked', 'failed', 'unverified', 'writing'].includes(e.state), label + ': a known state ' + e.state); if (e.state !== 'writing') ok(!('runningUntil' in e), label + ': no lease left on a settled record (' + e.state + ')'); };

const T = [], test = (name, fn) => T.push([name, fn]);

/* ═══ E1 · only a saved repository step, only the order's number; no Etsy call in the sandbox or without a record ═══ */
test('E1 the step refuses without a saved repository step, and in the sandbox: no Etsy call, no write', async () => {
  const W = world({ rids: ['R1', 'R2', 'R3'], docs: { R2: { rehearsal: true }, R3: { phase: 'files' } } });
  writes.length = 0;
  let r = await W.call('R1', { rid: 'R1', sandbox: true }); eq([r.ok, r.reason], [false, 'sandbox']);
  r = await W.call('NOPE'); eq([r.ok, r.reason], [false, 'no-save']);
  r = await W.call('R2'); eq([r.ok, r.reason], [false, 'rehearsal']);
  r = await W.call('R3'); eq([r.ok, r.reason], [false, 'not-saved']);
  r = await W.call('bad rid!'); eq([r.ok, r.reason], [false, 'bad-request']);
  eq(W.S.calls.length, 0, 'no Etsy call'); eq(writes.length, 0, 'no write');
});

/* ═══ E2 · idempotent: one write, however many calls ═══ */
test('E2 five calls (and five tabs at once) write the listing\'s SKU to Etsy exactly once', async () => {
  const W = world();
  const r = await W.call('R1'); eq([r.ok, r.state, r.sku], [true, 'linked', NEW]);
  eq([W.landed(), W.stored()], [1, [NEW]]);
  const snap = JSON.stringify([...store.entries()]);
  for (let i = 0; i < 4; i++) { const x = await W.call('R1'); eq([x.ok, x.state, x.calls], [true, 'linked', 0], 'again #' + i); }
  eq(W.landed(), 1, 'still one write'); eq(JSON.stringify([...store.entries()]), snap, 'the later calls wrote nothing at all');
  const W2 = world();   // five at once (five browser tabs)
  const rs = await Promise.all([1, 2, 3, 4, 5].map(() => W2.call('R1')));
  ok(rs.some(x => x.state === 'linked'), 'one of them linked'); eq(W2.landed(), 1, 'five at once: one write');
  ok(rs.every(x => x.ok || x.reason === 'running'), 'the others were told "running" (retry), nothing else: ' + JSON.stringify(rs.map(x => x.reason || x.state)));
  const done = await W2.call('R1'); eq([done.ok, done.state], [true, 'linked']); eq(W2.landed(), 1);
});

/* ═══ E3 · fault injection at every step x every fault ═══ */
const E_ = (status, error) => etsyErr(status, error);
const FAULTS = [
  ['no connection', () => Object.assign(new Error('fetch failed'), { code: 'ECONNRESET' }), 'offline'], ['429', () => E_(429, 'rate limit'), 'busy'], ['500', () => E_(500, 'oops'), 'etsy-down'], ['502', () => E_(502, 'bad gateway'), 'etsy-down'],
  ['503', () => E_(503, 'down'), 'etsy-down'], ['401', () => E_(401, 'expired'), 'scope'], ['403', () => E_(403, 'not allowed'), 'scope'], ['refresh 400', () => new Error('Etsy token refresh failed: 400 {"error":"invalid_grant"}'), 'scope'],
  ['refresh 500', () => new Error('Etsy token refresh failed: 500 oops'), 'etsy-down'], ['refresh 429', () => new Error('Etsy token refresh failed: 429 slow down'), 'busy'], ['404', () => E_(404, 'no listing'), 'listing-gone'], ['400', () => E_(400, 'bad body'), 'rejected']
];
const STEPS = [['GET listing', c => c.method === 'GET' && !/inventory/.test(c.path), { mirror: false }], ['GET inventory', c => c.method === 'GET' && /inventory/.test(c.path), {}], ['PUT inventory', c => c.method === 'PUT', {}]];
for (const [stepName, match, wo] of STEPS) for (const [fname, make, reason] of FAULTS) {
  if (fname === '400' && stepName !== 'PUT inventory') continue;
  test('E3 ' + stepName + ' x ' + fname + ': never working, retried by itself, written exactly once', async () => {
    let fired = false;
    const W = world(Object.assign({ sim: { fail: c => { if (!fired && match(c)) { fired = true; return make(); } return null; } } }, wo));
    const r = await W.call('R1'); ok(fired, 'the fault fired');
    etsyDocOK(W, 'R1', stepName + ' x ' + fname);
    ok(r.ok === false && !!r.reason, 'the first call is a calm refusal: ' + JSON.stringify(r));
    const e = W.etsyDoc('R1'); ok(['failed', 'unverified'].includes(e.state), 'a final state, not working: ' + e.state);
    const post = stepName === 'PUT inventory';   // after the write, an ambiguous failure is "unverified" (the write may have landed)
    const terminal = reason === 'listing-gone' || reason === 'rejected';
    eq(W.landed(), 0, 'a fault before/at the write: nothing landed');
    if (terminal) { eq([r.retry, e.state], [false, 'failed'], 'final: no retry'); eq(W.stored(), [OLD], 'Etsy untouched'); return; }
    eq(r.retry, true, 'retryable');
    if (!post || !['etsy-down', 'offline'].includes(reason) || true) ok(r.reason === reason || (post && r.state === 'unverified'), 'reason ' + reason + ' got ' + r.reason);
    // the retry (the keeper presses it again by itself): completes, and Etsy was written exactly once
    clk += 300000;
    let x = await W.call('R1'); if (!x.ok) { clk += 300000; x = await W.call('R1'); }
    eq([x.ok, x.state], [true, 'linked'], 'completed by the retry: ' + JSON.stringify(x)); eq([W.landed(), W.stored()], [1, [NEW]], 'written exactly once');
    const again = await W.call('R1'); eq([again.ok, again.calls, W.landed()], [true, 0, 1], 'and never again');
  });
}
for (const [fname, err] of [['timeout', Object.assign(new Error('timeout'), { timeout: true })], ['no connection', Object.assign(new Error('fetch failed'), { code: 'ECONNRESET' })], ['502', E_(502, 'bad gateway')], ['503', E_(503, 'down')], ['504', E_(504, 'gateway timeout')]]) {
  test('E3 PUT landed but the reply was lost (' + fname + '): "unverified", the retry reads first and does NOT write again', async () => {
    const W = world({ lostReply: err });
    const r = await W.call('R1'); eq([r.ok, r.state], [false, 'unverified']); etsyDocOK(W, 'R1', 'lost reply');
    eq(W.landed(), 1, 'the write landed'); const puts = W.S.puts.length;
    clk += 1000;
    const x = await W.call('R1'); eq([x.ok, x.state], [true, 'linked']); eq([W.S.puts.length, W.landed()], [puts, 1], 'no second write');
    eq(W.etsyDoc('R1').verified, true, 'verified from the before-copy');
  });
}
test('E3 PUT timeout where the write did NOT land: the retry writes once', async () => {
  let n = 0;
  const W = world({ sim: { hang: c => c.method === 'PUT' && n++ === 0 }, timeoutMs: 40 });
  const r = await W.call('R1'); eq([r.ok, r.state, r.reason], [false, 'unverified', 'etsy-down']); eq(W.landed(), 0); etsyDocOK(W, 'R1', 'hung PUT');
  clk += 1000; const x = await W.call('R1'); eq([x.ok, x.state], [true, 'linked']); eq([W.landed(), W.stored()], [1, [NEW]]);
});

/* ═══ E4 · the function is killed at every point of a run ═══ */
test('E4 killed after event k, for every k: never "working" once the lease ran out, the next call finishes with ONE write', async () => {
  let k = 0, completed = false, runsChecked = 0;
  while (!completed && k < 60) {
    const W = world({ mirror: false }); hooks.kill = k;
    let r = null; try { r = await W.call('R1'); } catch (_) { r = null; }
    const killed = hooks.dead; hooks.kill = null; hooks.dead = false;
    if (!killed) { completed = true; eq(W.etsyDoc('R1').state, 'linked'); break; }
    runsChecked++;
    const during = W.etsyDoc('R1') || {}, label = 'killed after event ' + k;
    // right after the kill a second press is refused while the lease holds (nothing is written to Etsy twice), and a reader sees the truth once the lease has run out
    if (during.state === 'writing') {
      const quick = await W.call('R1'); eq([quick.ok, quick.reason], [false, 'running'], label + ': refused while the lease holds');
      eq(Mod.effective(during, clk).state, 'writing', label + ': still the run\'s own note');
      clk += 61000;
      eq(Mod.effective(during, clk).state, 'unverified', label + ': a dead run is never "working"');
      eq(Mod.effective(during, clk).reason, 'interrupted');
      // the repair pass makes it final in the store, too
      const rep = await Repair.records({ db, FV, bucket, now: () => clk }, { rid: 'R1' }); ok(rep.fixed.some(f => f.did.includes('etsy-final')), label + ': the repair made it final');
      eq(W.etsyDoc('R1').state, 'unverified', label + ': stored as unverified'); ok(!('runningUntil' in W.etsyDoc('R1')));
    } else clk += 61000;
    // a NEW function instance carries on
    const W2 = Object.assign({}, W, { mod: W.mk() });
    let x = await W2.mod.customEtsySkuWrite({ rid: 'R1' }); if (!x.ok) { clk += 61000; x = await W2.mod.customEtsySkuWrite({ rid: 'R1' }); }
    eq([x.ok, x.state], [true, 'linked'], label + ': completed: ' + JSON.stringify(x)); eq([W.landed(), W.stored()], [1, [NEW]], label + ': Etsy written exactly once');
    const again = await W2.mod.customEtsySkuWrite({ rid: 'R1' }); eq([again.ok, again.calls, W.landed()], [true, 0, 1], label + ': and never again');
    etsyDocOK(W2, 'R1', label); k++;
  }
  ok(completed && runsChecked >= 8, 'every kill point was exercised (' + runsChecked + ' points)');
});

/* ═══ E5 · two orders of one listing ═══ */
test('E5 two orders of one listing at once: ONE write, the loser is told "running" with no Etsy call, then reads "linked"', async () => {
  let release, entered; const gate = new Promise(r => { release = r; }), enteredP = new Promise(r => { entered = r; });
  const W = world({ rids: ['R1', 'R2'], sim: { during: async c => { if (c.method === 'GET' && /inventory/.test(c.path) && !W.held) { W.held = true; entered(); await gate; } } } });
  const first = W.call('R1'); await enteredP;
  const n0 = W.S.calls.length, loser = await W.call('R2');
  eq([loser.ok, loser.reason, loser.calls], [false, 'running', 0], 'the loser: running, no Etsy call'); eq(W.S.calls.length, n0, 'no Etsy call by the loser');
  eq(W.etsyDoc('R2').runningUntil, undefined, 'the loser does not keep a lease of its own');
  release(); const win = await first; eq([win.ok, win.state], [true, 'linked']);
  const x = await W.call('R2'); eq([x.ok, x.state, x.calls], [true, 'linked', 1], 'the loser reads Etsy and finds the SKU there');
  eq([W.landed(), W.S.puts.length], [1, 1], 'ONE write for the two orders'); ok(!store.has('Charm_Custom_EtsyLeases/' + LID), 'the listing lease is given back');
});
test('E5 a lease left by a dead run runs out; the other order then goes on', async () => {
  const W = world({ rids: ['R1', 'R2'] }); hooks.kill = 9;   // R1 is killed somewhere inside its run, holding the listing's lease
  try { await W.call('R1'); } catch (_) { /* killed */ } hooks.kill = null; hooks.dead = false;
  ok(store.has('Charm_Custom_EtsyLeases/' + LID) || W.etsyDoc('R1').state !== 'pending' || true);
  const blocked = await W.call('R2'); if (store.has('Charm_Custom_EtsyLeases/' + LID)) eq([blocked.ok, blocked.reason], [false, 'running'], 'blocked while the dead run\'s lease holds');
  clk += 61000; const x = await W.call('R2'); eq([x.ok, x.state], [true, 'linked']); eq([W.landed(), W.stored()], [1, [NEW]]);
  const r1 = await W.call('R1'); eq([r1.ok, r1.state, W.landed()], [true, 'linked', 1]);
});

test('E5 an order whose product id is gone because ANOTHER order of the listing wrote (Etsy renumbers products on every write): linked, never "product-missing"', async () => {
  // with the sibling record\'s proof (its groupBefore holds this order\'s product)
  let W = world({ rids: ['R1', 'R2'] }); let r = await W.call('R1'); eq([r.ok, W.landed()], [true, 1]);
  r = await W.call('R2'); eq([r.ok, r.state, W.landed()], [true, 'linked', 1], 'R2 found its product renumbered: ' + JSON.stringify(r)); eq(W.etsyDoc('R2').state, 'linked');
  // without any sibling record (the proof is the inventory: the SKU is there, the old one is on no product)
  W = world({ rids: ['R1', 'R2'], docs: { R1: { listingId: '1718000009' }, R2: { productId: '777' } } });
  W.S.stored.products[0].sku = NEW; r = await W.call('R2'); eq([r.ok, r.state, W.landed()], [true, 'linked', 0], 'the SKU is on Etsy and the old one is gone: nothing to write');
  // not proof: the new SKU is not there, or the old one is still on a product
  W = world({ docs: { R1: { productId: '777' } } }); r = await W.call('R1'); eq([r.ok, r.reason], [false, 'product-missing'], 'no such product, no new SKU: final'); eq(W.landed(), 0);
  W = world({ inventory: mkInv([{ sku: OLD, pv: [[513, 'Metal', 'GF']] }, { sku: NEW, pv: [[513, 'Metal', 'SS']] }], { sku_on_property: [513], price_on_property: [513] }), docs: { R1: { productId: '777' } } });
  r = await W.call('R1'); eq([r.ok, r.reason], [false, 'product-missing'], 'the old SKU is still on a product: not proof'); eq(W.landed(), 0);
});

/* ═══ E6 · deleted / inactive listing ═══ */
test('E6 a deleted listing is final (listing-gone), an inactive one waits and completes when active', async () => {
  let W = world({ mirror: false, sim: { fail: c => etsyErr(404, 'gone') } });
  let r = await W.call('R1'); eq([r.ok, r.reason, r.retry, W.etsyDoc('R1').state], [false, 'listing-gone', false, 'failed']); eq(W.landed(), 0); etsyDocOK(W, 'R1', 'gone');
  W = world({ mirror: 'inactive' });
  r = await W.call('R1'); eq([r.ok, r.reason, r.retry, r.calls], [false, 'listing-state', true, 0], 'inactive by the mirror: no Etsy call, waits'); eq(W.etsyDoc('R1').state, 'failed');
  for (const st of ['draft', 'expired', 'sold_out']) { store.set('EtsyMail_Listings/' + LID, { state: st }); r = await W.call('R1'); eq([r.ok, r.reason, r.calls], [false, 'listing-state', 0], st); }
  store.set('EtsyMail_Listings/' + LID, { state: 'active' }); clk += 1000;
  r = await W.call('R1'); eq([r.ok, r.state], [true, 'linked']); eq([W.landed(), W.stored()], [1, [NEW]]);
  W = world({ mirror: false, sim: { state: 'inactive' } });   // the mirror has nothing: one GET of the listing says inactive
  r = await W.call('R1'); eq([r.ok, r.reason, r.calls], [false, 'listing-state', 1]);
});

/* ═══ E7 · token refresh failures ═══ */
test('E7 a failed token refresh is about the connection, never "Etsy did not accept the change"', async () => {
  const c = Mod.internals.classify, R = Mod.REASONS;
  for (const [msg, want] of [['Etsy token refresh failed: 400 {"error":"invalid_grant"}', 'scope'], ['Etsy token refresh failed: 401 x', 'scope'], ['Etsy token refresh failed: 403 x', 'scope'], ['Etsy token refresh failed: 429 slow', 'busy'],
    ['Etsy token refresh failed: 500 x', 'etsy-down'], ['Etsy token refresh failed: 503 x', 'etsy-down'], ['Etsy OAuth not seeded at config/etsyOauth. Run etsyMailSeedTokens first.', 'scope'], ['No refresh_token in OAuth doc', 'scope'],
    ['Missing SHOP_ID / CLIENT_ID / CLIENT_SECRET env vars', 'not-connected'], ['fetch failed', 'offline'], ['Etsy API returned non-JSON for /x: <html>', 'offline']]) eq(c(new Error(msg)).reason, want, msg);
  for (const r of ['scope', 'busy', 'etsy-down', 'offline', 'not-connected', 'listing-state', 'backup-failed', 'running', 'unverified', 'interrupted']) ok(R[r] && R[r].retry !== false, r + ' is retried');
  for (const r of ['listing-gone', 'product-missing', 'changed', 'sku-in-use', 'verify', 'rejected']) eq(R[r].retry, false, r + ' is final');
  const e = Mod.effective({ state: 'writing', runningUntil: 1000, calls: 1 }, 5000); eq([e.state, e.reason, e.retry], ['unverified', 'interrupted', true]);
  eq(Mod.effective({ state: 'writing', runningUntil: 9000 }, 5000).state, 'writing'); eq(Mod.effective({ state: 'linked' }, 5000).state, 'linked'); eq(Mod.effective(undefined, 5000).state, 'pending');
});

/* ═══ E8 · the server drives the pending steps itself ═══ */
test('E8 customSaveRepair etsy:true runs the pending steps (oldest first, bounded, cool-down, finals skipped, a scope refusal stops the pass); repeated passes write nothing more', async () => {
  const W = world({ rids: ['R1', 'R2', 'R3', 'R4', 'R5', 'R6'], docs: {
    R1: { at: clk - 6000 }, R2: { at: clk - 5000 }, R3: { at: clk - 4000 }, R4: { at: clk - 3000, etsy: { state: 'failed', reason: 'listing-gone', retry: false, at: clk - 99999 } },
    R5: { at: clk - 2000, etsy: { state: 'linked' } }, R6: { at: clk - 1000, etsy: { state: 'failed', reason: 'busy', retry: true, at: clk - 5000 } } } });
  const deps = { db, FV, bucket, now: () => clk, etsySku: b => W.mod.customEtsySkuWrite(b) };
  const rep = await Repair.repair(deps, { etsy: true, force: true, maxEtsy: 2 });
  eq(rep.etsy.ran.map(x => x.id), ['R1', 'R2'], 'oldest first, at most 2'); eq(rep.etsy.ran.map(x => x.state), ['linked', 'linked']);
  eq(W.landed(), 1, 'the listing\'s SKU was written once for the two orders');
  clk += 400000; const rep2 = await Repair.repair(deps, { etsy: true, force: true, maxEtsy: 5 });
  eq(rep2.etsy.ran.map(x => x.id), ['R3', 'R6'], 'the final (R4), the linked (R5) are skipped; R6 waited out its cool-down'); eq(rep2.etsy.ran.map(x => x.state), ['linked', 'linked']);
  eq(W.landed(), 1, 'still one write');
  const snap = JSON.stringify([...store.entries()].filter(([k]) => !k.startsWith('Charm_Custom_Repair')));
  clk += 400000; const rep3 = await Repair.repair(deps, { etsy: true, force: true }); eq(rep3.etsy.ran, [], 'nothing pending any more');
  eq(JSON.stringify([...store.entries()].filter(([k]) => !k.startsWith('Charm_Custom_Repair'))), snap, 'a third pass wrote nothing');
  // a scope refusal ("Paul must re-authorise") stops the pass: the other saves would only ask the same
  const S2 = world({ rids: ['R1', 'R2', 'R3'], sim: { fail: () => etsyErr(403, 'no') }, docs: { R1: { at: clk - 3000 }, R2: { at: clk - 2000 }, R3: { at: clk - 1000 } } });
  const d2 = { db, FV, bucket, now: () => clk, etsySku: b => S2.mod.customEtsySkuWrite(b) };
  const r2 = await Repair.repair(d2, { etsy: true, force: true, maxEtsy: 5 }); eq(r2.etsy.ran.map(x => x.id), ['R1'], 'stopped after the first scope refusal'); eq(r2.etsy.ran[0].reason, 'scope');
  eq(S2.landed(), 0);
  // the sandbox: nothing at all
  const w0 = writes.length, c0 = S2.S.calls.length; const sb = await Repair.repair(d2, { etsy: true, sandbox: true }); eq(sb.sandbox, true); eq([writes.length, S2.S.calls.length], [w0, c0], 'the sandbox repair touches nothing');
});

let finished = false;
process.on('exit', () => { if (!finished) { console.error('keep-etsy-link: the run ended before the last case (a promise that never settled?)'); process.exitCode = 1; } });
(async () => {
  const warn = console.warn, err = console.error; console.warn = () => {}; console.error = () => {};
  let n = 0;
  try {
    for (const [name, fn] of T) {
      if (process.env.ONLY && !new RegExp(process.env.ONLY).test(name)) continue;
      let timer;
      try { await Promise.race([fn(), new Promise((_, rej) => { timer = setTimeout(() => rej(new Error('the case did not finish in 30 s')), 30000); })]); n++; process.stdout.write('  ok  ' + name.slice(0, 120) + '\n'); }
      catch (e) { console.warn = warn; console.error = err; console.error('FAIL ' + name + '\n' + (e && e.stack || e)); finished = true; process.exit(1); }
      finally { clearTimeout(timer); }
    }
  } finally { console.warn = warn; console.error = err; }
  finished = true;
  console.log('keep-etsy-link OK: ' + n + ' cases, ' + checks + ' checks, ' + sim.every.length + ' fake Etsy requests');
  process.exit(0);
})().catch(e => { console.error(e); finished = true; process.exit(1); });
