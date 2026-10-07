// Cost of the Etsy Pricing console's store (Firebase cost emergency, FC19), measured with tests/cost/meter.cjs on an in-memory Firestore.
//
//   node tests/cost/fc19-pricing-store.cjs     prints the before/after table; exits 1 when an answer differs or a budget is exceeded
//
// Fixture: 4,700 EtsyPricing_Listings documents, 2,000 of them batched (a batched listing carries original_inventory, about 21 KB, and its
// snapshot hash); one EtsyPricing_Runs document with 4,700 queued ids (a full-catalogue run). Sizes are the code's own field shapes; the 21 KB is the figure written
// in etsyPricingStore.js (measured there on a real 12-metal x 4-length matrix).  Nothing is sent to Etsy or the real Firestore: the batch
// worker's HTTP calls are a recorder, its pricing scheme and rate limiter are stubs.
//
//   getAll        before: whole collection, every call              after: cold = masked list, then ONE read per call (changes only)
//   getRun        before: the run document with its 4,700 ids       after: without ids
//   batch loop    before: whole listing + whole run per listing     after: the seven fields / the two flags
'use strict';
const path = require('path'), assert = require('assert');
const root = path.join(__dirname, '../..');
const meter = require('./meter.cjs');
const fn = f => path.join(root, 'netlify/functions', f);
const say = s => process.stdout.write(s + '\n');
const kb = n => (n / 1024).toFixed(0) + ' KB';
const mb = n => (n / 1048576).toFixed(2) + ' MB';

const N = 4700, BATCHED = 2000;
const COLL = 'EtsyPricing_Listings';
const HEAVY = ['original_inventory', 'original_snapshot_hash'];
const CONSOLE = ['chain_type', 'chain_set', 'engraving', 'engrave_set', 'batched', 'last_batch', 'batch_blocked', 'health', 'scanned', 'approval', 'last_save', 'title', 'original_saved', 'queue_id', 'category', 'listing_kind', 'updated_at'];
const hex = (n, s) => { let o = ''; for (let i = 0; o.length < n; i++) o += ((s * 2654435761 + i * 40503) >>> 0).toString(16); return o.slice(0, n); };

function inventory(seedN) {                       // about 21 KB of JSON in the shape Etsy returns
  const products = [];
  for (let p = 0; p < 46; p++) products.push({
    product_id: 9000000000 + seedN * 100 + p, sku: 'SKU-' + seedN + '-' + p, is_deleted: false,
    offerings: [{ offering_id: 5000000000 + seedN * 100 + p, price: { amount: 3969 + p, divisor: 100, currency_code: 'USD' }, quantity: 999, is_enabled: true, is_deleted: false }],
    property_values: [
      { property_id: 200, property_name: 'Primary color', scale_id: null, value_ids: [1000 + p], values: ['Sterling Silver ' + p] },
      { property_id: 513, property_name: 'Length', scale_id: null, value_ids: [2000 + (p % 5)], values: [(14 + 2 * (p % 5)) + ' inches'] }]
  });
  return { products, price_on_property: [200], quantity_on_property: [], sku_on_property: [] };
}
const INV_BYTES = Buffer.byteLength(JSON.stringify(inventory(1)));

function listingDoc(i, batched) {
  const d = { chain_type: i % 3 ? 'regular' : 'beady', chain_set: true, engraving: i % 4 !== 0, engrave_set: true, batched,
    title: 'Personalized Initial Charm Necklace in Sterling Silver, Gold Filled and Rose Gold, Gift for Her ' + i, scanned: batched, updated_at: 1.79e12 + i };
  if (batched) Object.assign(d, {
    last_batch: { at: 1.79e12 + i, ok: true }, last_save: { at: 1.79e12 + i, verified: true },
    health: { error_count: 0, warning_count: 1, product_count: 60, min_price: 39.69, max_price: 61.5 },
    approval: { mode: 'updated', at: 1.79e12 + i, hash: hex(64, i) }, original_saved: true,
    original_snapshot_hash: hex(64, i + 7), original_inventory: inventory(i) });
  return d;
}
function fixture() {
  const seed = {};
  for (let i = 0; i < N; i++) seed[COLL + '/' + (1000000000 + i)] = listingDoc(i, i < BATCHED);
  const ids = []; for (let i = 0; i < N; i++) ids.push(String(1000000000 + i));        // a full-catalogue run
  seed['EtsyPricing_Runs/run1'] = { status: 'running', ids, total: ids.length, done: 3, ok: 3, fail: 0, current: '#1000002003', errors: ['x'.repeat(180), 'y'.repeat(180)], stop: false, paused: false, consec_fail: 0, started_by: 'manual', created_at: 1.79e12, updated_at: 1.79e12 };
  return seed;
}
const pick = (o, fields) => { const out = {}; for (const f of fields) if (o[f] !== undefined) out[f] = o[f]; return out; };
const ev = (body) => ({ httpMethod: 'POST', body: JSON.stringify(body) });
const call = async (h, body) => { const r = await h.handler(ev(body)); assert.equal(r.statusCode, 200, JSON.stringify(body) + ' -> ' + r.body.slice(0, 200)); return JSON.parse(r.body); };

/* what the code did before FC19, copied here as the reference for "the answer did not change" */
async function legacyGetAll(db) {
  const snap = await db.collection(COLL).get();
  const docs = {};
  snap.forEach(d => { const o = d.data() || {}; for (const k of HEAVY) delete o[k]; docs[d.id] = o; });
  return { docs, count: Object.keys(docs).length };
}

(async () => {
  const m = meter.create();
  m.db.seed(fixture());
  m.install();
  process.env.URL = 'https://example.test';
  const store = require(fn('etsyPricingStore.js'));
  const realNow = Date.now; let skew = 0; Date.now = () => realNow() + skew;
  const rows = [];
  const row = (label, d) => { rows.push([label, d.reads + (d.aggs || 0), d.bytes, d.writes]); return d; };

  say('fixture: ' + N + ' listings (' + BATCHED + ' batched, original_inventory ' + kb(INV_BYTES) + ' each), run document with ' + N + ' ids');

  /* ── BEFORE: the old getAll (reference copy), through the meter ── */
  const a0 = m.snapshot();
  const legacy = await m.op('before.getAll', () => legacyGetAll(m.db));
  const before = row('BEFORE getAll (every call)', m.since(a0));

  /* ── AFTER: cold call, a second call with nothing changed, calls after changes ── */
  const a1 = m.snapshot();
  const cold = await m.op('after.getAll.cold', () => call(store, { action: 'getAll' }));
  const coldCost = row('AFTER getAll, first call on an instance', m.since(a1));
  const a2 = m.snapshot();
  const warm = await m.op('after.getAll.warm', () => call(store, { action: 'getAll' }));
  const warmCost = row('AFTER getAll, nothing changed', m.since(a2));

  // the answer is the old answer, restricted to the fields the console reads; the heavy fields are absent
  assert.equal(cold.count, N); assert.equal(Object.keys(cold.docs).length, N);
  for (const id of Object.keys(legacy.docs)) assert.deepEqual(cold.docs[id], pick(legacy.docs[id], CONSOLE), 'doc ' + id + ' differs from the old answer');
  assert.deepEqual(warm.docs, cold.docs, 'a call with nothing changed answers the same');
  for (const d of Object.values(cold.docs)) for (const k of HEAVY) assert.equal(d[k], undefined, 'heavy field leaked: ' + k);
  // and everything the old answer carried that the console does NOT read is only fields nothing in the repo writes
  for (const d of Object.values(legacy.docs)) for (const k of Object.keys(d)) assert(CONSOLE.includes(k), 'the old answer carried an unlisted field: ' + k);
  assert(warm.asOf >= cold.asOf);

  /* ── a console writes (set) and the batch worker writes: the next getAll shows them, at the price of the changed documents ── */
  await call(store, { action: 'set', id: '1000004000', patch: { batched: false, chain_type: 'beady' } });
  await call(store, { action: 'set', id: '1000000005', patch: { title: 'Renamed', engraving: false } });
  skew += 40;
  const a3 = m.snapshot();
  const after2 = await m.op('after.getAll.changed', () => call(store, { action: 'getAll' }));
  const changedCost = row('AFTER getAll, 2 listings changed', m.since(a3));
  assert.equal(after2.docs['1000004000'].chain_type, 'beady'); assert.equal(after2.docs['1000004000'].batched, false);
  assert.equal(after2.docs['1000000005'].title, 'Renamed'); assert.equal(after2.docs['1000000005'].engraving, false);
  assert.equal(after2.docs['1000000005'].original_inventory, undefined, 'a changed batched listing still does not drag its snapshot along');
  assert.equal(after2.docs['1000000006'].title, cold.docs['1000000006'].title);
  assert.equal(after2.count, N);
  // equal to a fresh full masked read of the current database state
  const truth = await legacyGetAll(m._raw);
  for (const id of Object.keys(truth.docs)) assert.deepEqual(after2.docs[id], pick(truth.docs[id], CONSOLE), 'after changes, ' + id + ' differs from the database');

  /* ── a polling console: since ── */
  const a4 = m.snapshot();
  const d0 = await call(store, { action: 'getAll', since: after2.asOf });
  const sinceIdle = row('AFTER getAll { since }, nothing new', m.since(a4));
  assert(d0.delta === true && d0.asOf >= after2.asOf);
  assert(Object.keys(d0.docs).length <= 3, 'only the listings inside the 15 s margin come back again');
  await call(store, { action: 'set', id: '1000000100', patch: { last_batch: { at: 1, ok: false, error: 'boom' } } });
  const a5 = m.snapshot();
  const d1 = await call(store, { action: 'getAll', since: d0.asOf });
  const sinceOne = row('AFTER getAll { since }, 1 listing changed', m.since(a5));
  assert.equal(d1.docs['1000000100'].last_batch.error, 'boom');
  skew += 60000;    // the margin (15 s before the caller's asOf) returns the last few writes once more; the poll after that is empty
  const d2 = await call(store, { action: 'getAll', since: d1.asOf });
  assert(Object.keys(d2.docs).length <= 4);
  skew += 60000;
  const d3 = await call(store, { action: 'getAll', since: d2.asOf });
  assert.equal(Object.keys(d3.docs).length, 0, 'a quiet minute later the delta is empty');
  // a bad or absurd since never breaks the answer
  const dBad = await call(store, { action: 'getAll', since: 'nope' }); assert.equal(dBad.count, N);
  const dFuture = await call(store, { action: 'getAll', since: realNow() + 10 * 86400000 }); assert(dFuture.delta === true);

  /* ── the old answer on request (full:true), and the safety net: a full re-read after 30 minutes ── */
  const full = await call(store, { action: 'getAll', full: true });
  for (const id of Object.keys(legacy.docs)) if (!['1000004000', '1000000005', '1000000100'].includes(id)) assert.deepEqual(full.docs[id], legacy.docs[id], 'full:true must equal the old answer for ' + id);
  skew += 31 * 60000;
  const a6 = m.snapshot();
  await call(store, { action: 'getAll' });
  const ttl = row('AFTER getAll, 31 minutes later (full re-read)', m.since(a6));
  assert(ttl.reads >= N, 'the instance re-reads the whole list after 30 minutes');

  /* ── ten consoles arriving together on a cold instance share ONE full read ── */
  delete require.cache[require.resolve(fn('etsyPricingStore.js'))];
  const store2 = require(fn('etsyPricingStore.js'));
  const a7 = m.snapshot();
  const many = await Promise.all(Array.from({ length: 10 }, () => call(store2, { action: 'getAll' })));
  const burst = row('AFTER 10 simultaneous cold getAll', m.since(a7));
  assert(burst.reads < N + 40, 'ten simultaneous callers cost one full read plus ten small deltas, not ten full reads: ' + burst.reads);
  for (const r of many) assert.equal(r.count, N);

  /* ── a document written without updated_at is found by the full read and kept (the safety net, nothing assumed beyond it) ── */
  m.db.seed({ [COLL + '/1000009999']: { chain_set: true, engrave_set: true, batched: false, title: 'No stamp' } });
  delete require.cache[require.resolve(fn('etsyPricingStore.js'))];
  const store3 = require(fn('etsyPricingStore.js'));
  const noStamp = await call(store3, { action: 'getAll' });
  assert.equal(noStamp.docs['1000009999'].title, 'No stamp'); assert.equal(noStamp.count, N + 1);

  /* ── if the changes query cannot run (index switched off), the answer is still right: the masked list, read whole ── */
  m.queryGuard = q => { if (q.filters.some(([f, op]) => f === 'updated_at' && op === '>')) throw Object.assign(new Error('9 FAILED_PRECONDITION: no index'), { code: 9 }); };
  delete require.cache[require.resolve(fn('etsyPricingStore.js'))];
  const store4 = require(fn('etsyPricingStore.js'));
  const warnSaved = console.warn; console.warn = () => {};
  const b0 = m.snapshot();
  const brk1 = await call(store4, { action: 'getAll' });
  const brk2 = await call(store4, { action: 'getAll' });
  const brkSince = await call(store4, { action: 'getAll', since: brk2.asOf });
  console.warn = warnSaved; m.queryGuard = undefined;
  assert.equal(brk1.count, N + 1); assert.deepEqual(brk2.docs, brk1.docs); assert.equal(brkSince.delta, undefined, 'no usable changes query: the whole list, not a delta'); assert.equal(brkSince.count, N + 1);
  assert(m.since(b0).reads >= 3 * N, 'each call reads the masked list whole in that case');
  assert(m.since(b0).bytes < 3 * 3 * 1024 * 1024, 'and still never the snapshots');

  /* ── getRun / activeRun: the run document without its 4,700 ids ── */
  const a8 = m.snapshot();
  const runOld = await m.op('before.getRun', async () => { const s = await m.db.collection('EtsyPricing_Runs').doc('run1').get(); return s.data(); });
  const runBefore = row('BEFORE getRun (one poll)', m.since(a8));
  const a9 = m.snapshot();
  const runNew = await m.op('after.getRun', () => call(store, { action: 'getRun', run_id: 'run1' }));
  const runAfter = row('AFTER getRun (one poll)', m.since(a9));
  const expectRun = Object.assign({}, runOld); delete expectRun.ids;
  assert.deepEqual(runNew.run, expectRun, 'getRun answers the same minus ids'); assert.equal(runNew.run_id, 'run1');
  const act = await call(store, { action: 'activeRun' });
  assert.deepEqual(act.run, expectRun); assert.equal(act.run_id, 'run1');
  const miss = await store.handler(ev({ action: 'getRun', run_id: 'nope' })); assert.equal(miss.statusCode, 404);
  const dup = await store.handler(ev({ action: 'startRun', ids: ['1'] })); assert.equal(dup.statusCode, 409); assert.equal(JSON.parse(dup.body).run_id, 'run1');

  /* ── the batch worker, 6 listings (3 batched before, 3 not), against the same database: what it reads per listing ── */
  const scheme = fn('_etsyPricingScheme.js'), limiter = fn('etsyRateLimiter.js');
  const stub = (p, exports) => { require.cache[require.resolve(p)] = { id: p, filename: p, loaded: true, exports }; };
  const plan = () => ({ rows: [{ sku: 'a' }] });
  stub(scheme, { CANON_ORDER: [], CHARM_ONLY_METALS: [], NO_CHAIN_VALUE: '', ENGRAVE_INSTRUCTIONS: '', REGULAR_PRICES: {}, BEADY_FLAT_PRICES: {}, BEADY_SOLID_BY_LENGTH: {}, CHARM_ONLY_PRICE_POOLS: {}, CHARM_LISTING_PRICES: {},
    isNoChainVal: () => false, parseLen: () => 0, titleCaseOpt: s => s, firstOffering: () => null, deep: x => x,
    priceFor: () => 1, planStandardRebuild: (products) => (products && products[0] && products[0].fail ? { error: 'No metal dropdown found' } : plan()), planCharmListingRebuild: plan, planStudRebuild: plan,
    normalizeChainType: v => (v === 'beady' ? 'beady' : v === 'regular' ? 'regular' : null), listingKindFor: () => null });
  stub(limiter, { etsyFetch: async () => { throw new Error('no Etsy call in a test'); } });
  const sent = [];
  global.fetch = async (url, o) => {
    sent.push(String(url));
    const u = new URL(url, 'https://example.test'), id = u.searchParams.get('listingId');
    const ok = body => ({ ok: true, status: 200, text: async () => JSON.stringify(body) });
    if (u.pathname.endsWith('etsyListingInventoryDetailProxy')) return ok({ snapshot_hash: 'h' + id, inventory: { products: [{ fail: id === '1000000002' }] } });
    if (u.pathname.endsWith('etsyUpdateListingInventoryProxy')) return ok({ verified: true, fresh: { snapshot_hash: 'h2', pricing_health: { error_count: 0, warning_count: 0, product_count: 1, min_price: 1, max_price: 2 } }, previous_inventory: { products: [] }, previous_snapshot_hash: 'prev' });
    throw new Error('unexpected fetch ' + url);
  };
  const ids6 = ['1000000000', '1000000001', '1000000002', '1000000003', '1000002500', '1000002501'];
  const seed2 = { 'EtsyPricing_Config/etsyOauth': { access_token: 'tok', refresh_token: 'r', expires_at: realNow() + 3600000 + skew },
                  'EtsyPricing_Runs/run2': { status: 'queued', ids: ids6, total: 6, done: 0, ok: 0, fail: 0, current: '', errors: [], stop: false, paused: false, consec_fail: 0, created_at: 1, updated_at: 1 } };
  m.db.seed(seed2);
  delete require.cache[require.resolve(fn('etsyPricingBatch-background.js'))];
  const batch = require(fn('etsyPricingBatch-background.js'));
  const a10 = m.snapshot();
  const out = await m.op('after.batch6', () => batch.handler({ body: JSON.stringify({ run_id: 'run2' }) }));
  const batchAfter = m.since(a10);
  assert.equal(out.body, 'done');
  const end = m.db.dump();
  assert.equal(end['EtsyPricing_Runs/run2'].status, 'done'); assert.equal(end['EtsyPricing_Runs/run2'].ok, 5); assert.equal(end['EtsyPricing_Runs/run2'].fail, 1); assert.equal(end['EtsyPricing_Runs/run2'].blocked, 1);
  assert.equal(end['EtsyPricing_Runs/run2'].errors.length, 1); assert(/NEEDS ATTENTION #1000000002/.test(end['EtsyPricing_Runs/run2'].errors[0]));
  assert.equal(end[COLL + '/1000000002'].batch_blocked.reason, 'plan_refused');
  assert.equal(end[COLL + '/1000002500'].batched, true);
  assert.equal(end[COLL + '/1000000000'].original_inventory.products.length, 46, 'a listing that already has its snapshot keeps it, untouched');
  assert.equal(end[COLL + '/1000002500'].original_saved, true, 'a first batch saves the snapshot');
  assert.equal(end[COLL + '/1000002500'].original_snapshot_hash, 'prev');
  say('batch worker, 6 listings (3 with a snapshot): ' + batchAfter.reads + ' reads, ' + kb(batchAfter.bytes) + ' read in total, ' + batchAfter.writes + ' writes');
  assert(batchAfter.bytes < 12 * 1024, 'the batch loop reads no snapshots and no id list: ' + batchAfter.bytes + ' bytes');
  Date.now = realNow; m.uninstall();

  /* ── table ── */
  say('');
  say('call'.padEnd(52) + 'reads'.padStart(8) + 'bytes'.padStart(12) + 'writes'.padStart(8));
  for (const [l, r, b, w] of rows) say(l.padEnd(52) + String(r).padStart(8) + String(b).padStart(12) + String(w).padStart(8));

  /* ── per hour for one console during a batch: getAll on load, then every 4th poll of a 3.5 s loop = every 14 s; getRun every 3.5 s ── */
  const hr = (d, n) => meter.perHour(d, n);
  const getAllPolls = 3600 / 14, runPolls = 3600 / 3.5;
  const hb = hr(before, getAllPolls), ha = hr(sinceOne, getAllPolls), hrb = hr(runBefore, runPolls), hra = hr(runAfter, runPolls);
  say('');
  say('one console open during a batch (' + BATCHED + ' listings already batched):');
  say('  getAll every 14 s   before ' + Math.round(hb.reads).toLocaleString('en') + ' reads, ' + (hb.bytes / 1073741824).toFixed(2) + ' GiB, ' + hb.usd.toFixed(3) + ' USD an hour');
  say('                      after  ' + Math.round(ha.reads).toLocaleString('en') + ' reads, ' + (ha.bytes / 1073741824).toFixed(4) + ' GiB, ' + ha.usd.toFixed(4) + ' USD an hour   (page asking { since })');
  say('  getRun every 3.5 s  before ' + Math.round(hrb.reads).toLocaleString('en') + ' reads, ' + (hrb.bytes / 1048576).toFixed(0) + ' MiB, ' + hrb.usd.toFixed(4) + ' USD an hour');
  say('                      after  ' + Math.round(hra.reads).toLocaleString('en') + ' reads, ' + (hra.bytes / 1048576).toFixed(0) + ' MiB, ' + hra.usd.toFixed(4) + ' USD an hour');

  /* ── budgets ── */
  meter.assertMax(coldCost, { reads: N + 4, bytes: 3 * 1024 * 1024 }, 'cold getAll: one masked read of the collection');
  meter.assertMax(warmCost, { reads: 2, bytes: 1 }, 'getAll with nothing changed is one read');
  meter.assertMax(changedCost, { reads: 8, bytes: 6 * 1024 }, 'getAll after 2 changes reads those documents only');
  meter.assertMax(sinceIdle, { reads: 6, bytes: 8 * 1024 }, 'since with nothing new');
  meter.assertMax(sinceOne, { reads: 6, bytes: 8 * 1024 }, 'since with one change');
  meter.assertMax(runAfter, { reads: 1, bytes: 2 * 1024 }, 'getRun without ids');
  assert(before.bytes > BATCHED * INV_BYTES, 'the fixture really reproduces the old cost (snapshots read in full)');
  assert(runBefore.bytes > 50 * 1024);
  console.log('\nfc19 pricing store: answers identical, budgets met');
})().catch(e => { console.error(e); process.exitCode = 1; });
