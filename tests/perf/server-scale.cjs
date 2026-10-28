// PERFORMANCE AT PRODUCTION SCALE, the server side (Paul, 26 Oct 2026: "I don't see anything for how fast things actually happen
// and the performance of the overall platform once it's being used at scale with tens of thousands of orders in live and historical").
// The REAL Netlify handlers (charmNestLibrary, firebaseOrders, employeeEfficiency, the order timeline) over the repo's in-memory
// Firestore (tests/charm-nest/bridge-server.cjs fakeAdmin), seeded with a shop's history: 30,000 historical orders on their sheets
// and sets (laser done), their pool rows, 300,000 timeline events, half a year of Station_Activity and Efficiency_Daily for six
// people, cancelled orders, 3,000 open orders. Each operation is timed and its Firestore document reads counted (the fake bills
// as Firestore does: one per document a query returns or a get reads), and its answer measured. Budgets:
//   an interactive read under 1 s and a bounded number of reads (never a read that grows with ALL history when it needs a page
//   or a day), every list paged (a cursor / more:true), no answer over 1 MB, a long job inside its function's time limit (10 s)
//   with a continuation.
//   timeout 300 node tests/perf/server-scale.cjs        (PERF_TABLE=1 prints the table only, no assertion failure exit)
// No network, no credentials: the fakes only.
'use strict';
const path = require('path'), assert = require('assert');
const root = path.join(__dirname, '../..'), fnDir = path.join(root, 'netlify/functions');
const { makeState, fakeAdmin } = require(path.join(root, 'tests/charm-nest/bridge-server.cjs'));

const N_ORDERS = +process.env.PERF_ORDERS || 30000, ORDERS_PER_SHEET = 8, SHEETS_PER_SET = 3, N_OPEN = 3000, N_CANCELLED = 600;
const N_TIMELINE = +process.env.PERF_TIMELINE || 300000, PEOPLE = ['Paul K', 'Ana_M', 'Tess Welder', 'Marco R', 'Bea', 'Lina'], DAYS = 180, EV_PER_DAY = 60;
const PASS = 'perf-pass-1234', DAY0 = Date.UTC(2026, 3, 1, 15), DAYMS = 86400000;
const dayOf = t => new Date(t).toISOString().slice(0, 10);

// ── the store: one collection's entries per query (st.collEntries), so the fake's own scan of 500k documents does not swamp the time
const st = makeState();
{ const idx = new Map(), cOf = k => k.slice(0, k.lastIndexOf('/')), set = st.docs.set, del = st.docs.delete;
  st.docs.set = (k, v) => { st.writes = (st.writes || 0) + 1; const c = cOf(k); (idx.get(c) || idx.set(c, new Map()).get(c)).set(k, v); return set(k, v); };
  st.docs.delete = k => { const m = idx.get(cOf(k)); if (m) m.delete(k); return del(k); };
  st.collEntries = c => (idx.get(c) || new Map()).entries(); }
const admin = fakeAdmin(st, 'http://localhost');
/* (the fake has startAfter only: startAt(x), inclusive, on one numeric orderBy is startAfter just past x the other way)
   a count() is billed as Firestore bills an aggregation, one read per 1,000 index entries (at least one), not one per document as the fake's get() */
{ const db = admin.firestore(), wrap = (q, dir) => new Proxy(q, { get(t, k) { const v = t[k]; if (k === 'startAt') return x => wrap(t.startAfter(typeof x === 'number' ? (dir === 'desc' ? x + 1e-6 : x - 1e-6) : x), dir); if (k === 'orderBy') return (f, d) => wrap(v.call(t, f, d), d || 'asc'); if (k === 'count') return () => ({ get: async () => { const res = await t.count().get(), n = res.data().count; st.reads = (st.reads || 0) - n + Math.max(1, Math.ceil(n / 1000)); return res; } }); if (['where', 'orderBy', 'limit', 'startAfter', 'select'].includes(k)) return (...a) => wrap(v.apply(t, a), dir); return typeof v === 'function' ? v.bind(t) : v; } }), coll = db.collection;
  db.collection = c => wrap(coll(c));
  db.doc = p => { const i = p.lastIndexOf('/'); return db.collection(p.slice(0, i)).doc(p.slice(i + 1)); }; }   // (firebase-admin's db.doc(path), which the fake leaves out)
const Module = require('module'), realLoad = Module._load;
/* a FAKE Etsy: the shop's 1,100 newest receipts are new (above the 30,000 mirrored), 4-6 KB each; it answers getShopReceipts by offset/limit, newest
   first, in 300 ms a page, as the real one may (no network, no token: _etsyMailEtsy's token and the API meter are stubs) */
const NEW_ORDERS = 1100, etsy = { calls: 0 }, ETSY_TEXT = 'Handmade personalized initial charm, stamped and polished in our studio. ';
const newReceipt = i => { const id = 3500000000 + 40000 + i, t = Math.floor(Date.now() / 1000) - (NEW_ORDERS - i) * 120; return { receipt_id: id, receipt_type: 0, seller_user_id: 1, buyer_user_id: 900000 + i, name: 'Buyer ' + i, buyer_email: 'b' + i + '@x.test', status: i % 40 ? 'Paid' : 'Canceled', is_paid: true, is_shipped: false, is_canceled: !(i % 40), message_from_buyer: '', created_timestamp: t, updated_timestamp: t, create_timestamp: t, update_timestamp: t, grandtotal: { amount: 4200, divisor: 100, currency_code: 'USD' }, transactions: [0, 1].map(j => ({ transaction_id: id * 10 + j, title: ETSY_TEXT.repeat(4), description: ETSY_TEXT.repeat(10), listing_id: 1718000 + (i % 40), sku: 'SKU-' + (i % 40), quantity: 1, variations: [{ formatted_name: 'Letter', formatted_value: 'A' }], price: { amount: 2100, divisor: 100, currency_code: 'USD' }, created_timestamp: t })) }; };
const NEWEST = Array.from({ length: NEW_ORDERS }, (_, i) => newReceipt(NEW_ORDERS - 1 - i));
async function fakeEtsy(url, init) {
  const u = new URL(String(url)); etsy.calls++;
  const reply = (status, body) => ({ ok: status < 300, status, headers: { get: () => null }, json: async () => body, text: async () => JSON.stringify(body) });
  if (/oauth\/token/.test(u.pathname)) return reply(200, { access_token: 'tok', refresh_token: 'ref', expires_in: 3600 });
  await new Promise(ok => setTimeout(ok, 300));
  const off = +u.searchParams.get('offset') || 0, lim = +u.searchParams.get('limit') || 25, since = +u.searchParams.get('min_last_modified') || 0;
  const all = NEWEST.filter(x => x.updated_timestamp >= since);
  return reply(200, { count: all.length, results: JSON.parse(JSON.stringify(all.slice(off, off + lim))) });
}
const meterToken = { failNet() {}, fromHttp() {}, ok() {} }, fakeMeter = { bump: () => meterToken, bumpSimple() {}, flushNow: async () => {}, wrapHandler: h => h };
Module._load = function (req, ...rest) { if (req === 'firebase-admin' || /[\/]firebaseAdmin(\.js)?$/.test(req) || req === './firebaseAdmin') return admin; if (req === 'node-fetch') return fakeEtsy; if (/[\/]_etsyApiMeter(\.js)?$/.test(req)) return fakeMeter; if (/[\/]_etsyMailEtsy(\.js)?$/.test(req)) return { getValidEtsyAccessToken: async () => 'tok', etsyFetch: async (p, o) => (await fakeEtsy('https://api.etsy.com/v3/application' + p, o)).json() }; return realLoad.call(this, req, ...rest); };
process.env.SHOP_ID = '987654'; process.env.CLIENT_ID = 'test-key'; process.env.CLIENT_SECRET = 'test-secret';
process.env.EDIT_PASSCODE = PASS; process.env.CHARM_NEST_DELETE_CODE = '975311';
global.fetch = async () => { throw new Error('no network in the perf test'); };
const put = (c, id, d) => st.docs.set(c + '/' + id, d);

function seed() {
  const t0 = Date.now();
  let o = 0, sheetN = 0, setN = 0;
  const orderId = i => String(3500000000 + i);
  // historical orders: sheets of 8 orders, sets of 3 sheets, all laser done; their pool rows (2 lines an order)
  while (o < N_ORDERS) {
    const setId = 'set-h' + setN, ids = [], at = DAY0 + Math.floor(o / (N_ORDERS / DAYS)) * DAYMS, day = dayOf(at);
    for (let k = 0; k < SHEETS_PER_SET && o < N_ORDERS; k++) {
      const id = 'sheet-h' + sheetN++, orders = []; for (let j = 0; j < ORDERS_PER_SHEET && o < N_ORDERS; j++) orders.push(orderId(o++));
      ids.push(id);
      const charms = orders.flatMap((x, j) => [0, 1].map(l => ({ id: id + ':' + j + l, hash: 'h' + ((j * 7 + l) % 500), name: 'Charm ' + j, order: x })));
      put('Charm_Nest_Sheets', id, { id, sheetId: id, metal: k % 2 ? 'silver' : 'gold', day, setId, runId: 'run-h' + Math.floor(setN / 5), status: 'complete', archived: false, draft: false, orders, listings: ['17180' + (sheetN % 90)], charmCount: charms.length, placedCount: charms.length, charms, poolIds: charms.map(c => c.id), laserDoneAt: at + 3600000, laserDoneBy: 'Paul K', processSeals: [{ id: 'laserDone-1', how: 'laserDone', at: at + 3600000, by: 'Paul K' }], createdAt: at, updatedAt: at + 3600000 });
      for (const c of charms) put('Charm_Pool', c.id, { poolId: c.id, orderId: c.order, sheetId: id, setId, status: 'placed', lineKey: c.order + '_1', transactionId: '9' + c.order, day, updatedAt: at });
    }
    put('Charm_Nest_Sets', setId, { setId, seq: setN + 1, day, runId: 'run-h' + Math.floor(setN / 5), name: 'Set-' + (setN + 1), sheetIds: ids, status: 'complete', laserDoneAt: at + 3600000, laserDoneBy: 'Paul K', processSeals: [{ id: 'laserDone-1', how: 'laserDone', at: at + 3600000, by: 'Paul K' }], committedAt: at, createdAt: at, updatedAt: at + 3600000 });
    if (setN % 5 === 0) put('Charm_Nest_Runs', 'run-h' + setN / 5, { runId: 'run-h' + setN / 5, day, step: 'done', status: 'complete', mode: 'auto', createdAt: at, updatedAt: at + 1 });
    setN++;
  }
  // open orders: the Etsy mirror's open receipts and their pool rows, not yet placed
  const now = Date.now();
  for (let i = 0; i < N_OPEN; i++) { const id = orderId(N_ORDERS + i); put('EtsyMail_Receipts', id, { receipt_id: +id, status: 'Paid', is_shipped: false, created_timestamp: Math.floor(now / 1000) - i * 60, updated_timestamp: Math.floor(now / 1000) - i * 60, buyer_name: 'Buyer ' + i, raw: { receipt_id: +id, status: 'Paid', is_shipped: false, transactions: [{ transaction_id: 1, sku: 'SKU-1', quantity: 1 }] } }); put('Charm_Pool', id + '_1', { poolId: id + '_1', orderId: id, status: 'open', lineKey: id + '_1', updatedAt: now - i }); }
  // historical receipts (shipped) and the cancelled ones, with their cancel records
  for (let i = 0; i < N_ORDERS; i++) { const id = orderId(i), cancelled = i % Math.floor(N_ORDERS / N_CANCELLED) === 0, at = DAY0 + Math.floor(i / (N_ORDERS / DAYS)) * DAYMS; put('EtsyMail_Receipts', id, { receipt_id: +id, status: cancelled ? 'Canceled' : 'Completed', is_shipped: !cancelled, created_timestamp: Math.floor(at / 1000), updated_timestamp: Math.floor(at / 1000), buyer_name: 'B' + i, raw: { receipt_id: +id, status: cancelled ? 'Canceled' : 'Completed', is_shipped: !cancelled, transactions: [] } }); if (cancelled) put('Charm_Nest_Cancelled', id, { orderId: id, by: 'Etsy', source: 'etsy', etsyStatus: 'Canceled', cancelledAt: at, createdAt: at }); }
  // the timeline: events spread over the history (10 an order), the newest orders' too
  const Timeline = require(path.join(fnDir, '_orderTimeline.js')), types = ['placed', 'labelPrinted', 'scan', 'laserDone', 'sorted', 'welded', 'assembled', 'shipped', 'scan', 'scan'];
  for (let i = 0; i < N_TIMELINE; i++) { const n = Math.floor(i / 10) % N_ORDERS, at = DAY0 + Math.floor(n / (N_ORDERS / DAYS)) * DAYMS + (i % 10) * 600000; const c = Timeline.clean({ orderId: orderId(n), type: types[i % 10], at: Math.min(at, now - 1000), by: PEOPLE[i % 6], id: 'e' + i, station: 'shipping' }, { source: 'station' }); if (c) put('Order_Timeline', c.key, Object.assign(c.doc, { createdAt: c.doc.at })); }
  // half a year of station activity for six people, and their daily rollups (as _stationActivity writes them)
  const SA = require(path.join(fnDir, '_stationActivity.js')), FV = { increment: v => v, serverTimestamp: () => now }, stations = ['shipping', 'assembly', 'welding', 'sorter', 'design'];
  for (let d = 0; d < DAYS; d++) for (const [pi, person] of PEOPLE.entries()) {
    const evs = [];
    for (let e = 0; e < EV_PER_DAY; e++) { const at = now - (DAYS - d) * DAYMS + e * 300000 + pi * 1000; const c = SA.clean({ id: `a${d}-${pi}-${e}`, station: stations[pi % 5], action: e % 3 ? 'complete' : 'scan', person, orderId: orderId((d * 170 + e) % N_ORDERS), parts: 2, orders: 1, at, sincePrevMs: 300000, device: stations[pi % 5] }, now, ''); if (c && c.doc) { evs.push(c.doc); put(SA.ACT, c.doc.id, c.doc); } }
    if (evs.length) { const day = evs[0].day, patch = SA.rollupPatch(FV, null, day, evs[0].person, evs, ''); put(SA.DAILY, SA.rollupId(day, evs[0].person), JSON.parse(JSON.stringify(patch))); }
  }
  put('config', 'editPasscode', { passcode: PASS });
  // production's Current flag (CURRENT_MARK) is in place: the open sheets carry current: true; 20 open sheets of today
  put('Charm_Nest_Rev', 'currentFlag', { v: 1 });
  put('Brites_Orders', 'Employee Numbers', Object.fromEntries(PEOPLE.map((p, i) => [String(111111 * (i + 1)), p])));
  for (let i = 0; i < 20; i++) put('Charm_Nest_Sheets', 'sheet-open' + i, { id: 'sheet-open' + i, metal: 'gold', day: dayOf(now), setId: 'set-open' + (i >> 2), runId: 'run-open', status: 'complete', archived: false, current: true, orders: [orderId(N_ORDERS + i)], charms: [], createdAt: now, updatedAt: now });
  // a sandbox copy to wipe: 40,000 records over its families
  for (let i = 0; i < 40000; i++) put(['Sandbox_Charm_Pool', 'Sandbox_Charm_Nest_Sheets', 'Sandbox_Order_Timeline', 'Sandbox_Charm_Nest_Sets'][i % 4], 'sb' + i, { orderId: orderId(i % 5000), day: dayOf(now), updatedAt: now });
  put('Sandbox_Charm_Nest_Sandbox', 'current', { at: now, orders: 5000 });
  return Date.now() - t0;
}

const results = [];
async function measure(name, budget, fn) {
  if (process.env.PERF_TRACE && name.includes(process.env.PERF_TRACE)) { let last = Date.now(); st.tick = w => { const t = Date.now(); console.log('   ', t - last, 'ms before', w); last = t; }; } else st.tick = null;
  const r0 = st.reads || 0, w0 = st.writes || 0, t0 = Date.now(); let res, err = null;
  try { res = await fn(); } catch (e) { err = e; }
  const ms = Date.now() - t0, reads = (st.reads || 0) - r0, writes = (st.writes || 0) - w0, bytes = res && res.body != null ? Buffer.byteLength(String(res.body)) : res ? Buffer.byteLength(JSON.stringify(res)) : 0;
  let body = null; try { body = res && res.body != null ? JSON.parse(res.body) : res; } catch (_) { body = null; }
  const row = { name, ms, reads, writes, bytes, status: res && res.statusCode, budget, err: err ? String(err.message || err).slice(0, 200) : body && body.error ? String(body.error).slice(0, 200) : '', body };
  row.ok = !row.err && (row.status == null || row.status === 200) && ms <= budget.ms && reads <= budget.reads && bytes <= (budget.bytes || 1e6) && (budget.writes == null || writes <= budget.writes) && (!budget.paged || budget.paged(body));
  results.push(row); return body;
}

(async () => {
  const seedMs = seed();
  console.log(`seeded ${st.docs.size} documents in ${seedMs} ms`);
  const Lib = require(path.join(fnDir, 'charmNestLibrary.js')), Orders = require(path.join(fnDir, 'firebaseOrders.js')), Eff = require(path.join(fnDir, 'employeeEfficiency.js'));
  const stamp = require(path.join(root, 'tests/charm-nest/_repoToken.cjs')), lib = async b => Lib.handler({ httpMethod: 'POST', headers: { 'x-edit-passcode': PASS }, queryStringParameters: {}, body: JSON.stringify(/^(sandboxReset|purgeHistory|sandboxPullOrders)$/.test(b.op) ? await stamp('charmNestLibrary', b, admin.firestore()) : b) });
  const eff = b => Eff.handler({ httpMethod: 'POST', headers: { 'x-nf-client-connection-ip': '10.0.0.9' }, body: JSON.stringify(Object.assign({ key: PASS }, b)) });
  const door = (b, h) => Orders.handler({ httpMethod: 'POST', headers: Object.assign({ 'x-nf-client-connection-ip': '10.0.0.8', origin: 'https://goldenspike.app' }, h || {}), queryStringParameters: {}, body: JSON.stringify(b) });
  const doorGet = q => Orders.handler({ httpMethod: 'GET', headers: { 'x-nf-client-connection-ip': '10.0.0.8', origin: 'https://goldenspike.app' }, queryStringParameters: q, body: '' });
  /* (FAKE: the in-memory store answers a query by scanning its whole collection, where Firestore walks an index: a query over the 300,000
      timeline events costs the fake ~0.3 s each, Firestore milliseconds. The few ops that query Order_Timeline at this size are held to their
      reads and to the function's 10 s, not to 1 s of the fake's scanning: SCAN) */
  const SCAN = { ms: 9500 },
        I = { ms: 1000, reads: 1500 }, paged = k => b => !!b && (b[k] != null || b.next !== undefined || b.more !== undefined || b.cursor !== undefined);
  const mid = String(3500000000 + Math.floor(N_ORDERS / 2)), today = dayOf(Date.now());

  await measure('Library listSheets (Current)', I, () => lib({ op: 'listSheets', excludeDone: true }));
  // (a part of the sheet list keeps half the 1 MB budget as headroom, the rest follows its cursor: every sheet comes, none twice)
  const ls = await measure('Library listSheets (all, first page)', { ...I, bytes: 600000, paged: b => !!b && Array.isArray(b.sheets) && !!b.next }, () => lib({ op: 'listSheets', limit: 300 }));
  if (ls && ls.next) { const more = await measure('Library listSheets (all, next part)', { ...I, bytes: 600000 }, () => lib({ op: 'listSheets', cursor: ls.next })); const ids = ls.sheets.concat((more && more.sheets) || []).map(x => x.id); if (new Set(ids).size !== ids.length || ids.length !== 300 && !(more && more.next)) results.push({ name: 'listSheets parts: every sheet once', ok: false, err: ids.length + ' ids over two parts' }); }
  await measure('Library history (first page)', I, () => lib({ op: 'history' }));
  await measure('Library history (activity)', I, () => lib({ op: 'history', sort: 'activity' }));
  await measure('Library setList', I, () => lib({ op: 'setList' }));
  await measure('Library laserDoneList sheets', I, () => lib({ op: 'laserDoneList', kind: 'sheets' }));
  await measure('Library laserDoneList sets', I, () => lib({ op: 'laserDoneList', kind: 'sets' }));
  await measure('Library laserDoneList (activity)', I, () => lib({ op: 'laserDoneList', sort: 'activity' }));
  await measure('Library findSheets (one order)', { ms: 1000, reads: 200 }, () => lib({ op: 'findSheets', q: mid }));
  await measure('Library runList', I, () => lib({ op: 'runList' }));
  await measure('Library cancelList', I, () => lib({ op: 'cancelList' }));
  await measure('Library poolList (one order)', I, () => lib({ op: 'poolList', orderId: mid }));
  await measure('Order timeline (one order)', { ...SCAN, reads: 300 }, () => lib({ op: 'timelineGet', orderId: mid }));
  await measure('Library sandboxStatus (light)', { ms: 1000, reads: 10 }, () => lib({ op: 'sandboxStatus', light: true }));
  await measure('Library sandboxStatus (sandbox, counts)', { ms: 1000, reads: 2000 }, () => lib({ op: 'sandboxStatus', sandbox: true }));
  await measure('Library cancelSweep (dry run)', { ms: 9000, reads: 40000 }, () => lib({ op: 'cancelSweep', dryRun: true }));
  await measure('Efficiency overview (day)', { ...SCAN, reads: 1500 }, () => eff({ op: 'overview' }));
  await measure('Efficiency overview (30 days)', { ...SCAN, reads: 1500 }, () => eff({ op: 'overview', days: 30 }));
  await measure('Efficiency person (month)', I, () => eff({ op: 'person', name: 'Ana_M', range: 'month' }));
  await measure('Efficiency person (year)', { ms: 1000, reads: 2500 }, () => eff({ op: 'person', name: 'Ana_M', range: 'year' }));
  await measure('Efficiency personOrders', I, () => eff({ op: 'personOrders', name: 'Ana_M' }));
  await measure('Efficiency orders', I, () => eff({ op: 'orders', orderId: mid }));
  await measure('Station pinLogin', { ms: 1000, reads: 20 }, () => door({ pinLogin: '222222' }));
  await measure('Station admin door', { ms: 1000, reads: 20 }, () => door({ stationAdmin: 'Ana_M' }));
  // the long jobs: inside the function's 10 s, with a continuation (more: true) until done
  for (let i = 0, more = true; more && i < 30; i++) { const b = await measure('Library sandboxReset (wipe) call ' + (i + 1), { ms: 9500, reads: 60000 }, () => lib({ op: 'sandboxReset', sandbox: true })); more = !!(b && b.more); if (i === 29 && more) results.push({ name: 'sandboxReset never finished', ok: false, err: 'still more after 30 calls' }); }
  /* the pulls of new orders at scale: the sandbox's pull of the 1,000 newest Etsy orders (Paul's number), and production's receipts mirror bringing
     1,100 new receipts into the 30,000 already mirrored; the pull inside the 10 s of a function, the mirror inside the 30 s a scheduled function lives (it stops starting pages at its own RUN_BUDGET_MS) */
  put('config', 'etsyOauth', { access_token: 'tok', refresh_token: 'ref', expires_at_ms: Date.now() + 3600000 });   // (a throwaway token in the fake store: the fake Etsy takes any)
  put('config', 'charmNestSandboxPull', { v: 1, limit: 1000, by: 'Paul', at: Date.now() });
  const pull = await measure('Sandbox pull of 1,000 new orders', { ms: 9500, reads: 200, writes: 200 }, () => lib({ op: 'sandboxPullOrders', sandbox: true, startId: 'perf-pull-000001', by: 'Paul K' }));
  if (pull && !(pull.ok && pull.count === 1000)) results.push({ name: 'sandbox pull brought 1,000', ok: false, err: JSON.stringify(pull).slice(0, 200) });
  put('EtsyMail_Config', 'receiptsMirrorState', { lastSyncTimestamp: Math.floor(Date.now() / 1000) - 3 * 86400, nextEligibleAtMs: 0 });
  const Mirror = require(path.join(fnDir, 'etsyMailReceiptsMirrorCron.js'));
  const mir = await measure('Production receipts mirror, 1,100 new over 30,000', { ms: 25000, reads: 1500, writes: 1500 }, () => Mirror.handler({}));
  const mirrored = NEWEST.filter(x => st.doc('EtsyMail_Receipts', String(x.receipt_id))).length;
  if (mirrored !== NEW_ORDERS) results.push({ name: 'mirror brought every new receipt', ok: false, err: mirrored + ' of ' + NEW_ORDERS + ' ' + JSON.stringify(mir).slice(0, 200) });
  await measure('Library purgeHistory (check)', { ms: 2000, reads: 60000 }, () => lib({ op: 'purgeHistory', code: '975311', check: true }));

  /* FEED (Paul, 28 Oct 2026: "fewer cloud calls"; plan: phase 2, one changes feed per computer): every write names what it touched in the feed's four
     documents (charmNestLibrary.js FEED_SHARDS: the Library's own, pool rows, timelines, cancels / designs / maps), one bumpRev per op. Contention: 10 writes a
     second for 60 s (CONTENTION_SECS) across the kinds, the ten of each second sent at once, from ten computers, with transactions run as Firestore runs
     them (st.atomic: a commit that lost to another is run again) and every read and commit taking a few milliseconds, so the bumps race. Not one person's
     write may fail; every write moves its shards' counts; one question (getChanges) names every write's keys. Then the feed's documents refuse every
     write for 10 s: the persons' writes still answer, and the feed sends the readers of what it lost to their own question (p:* and the like). */
  {
    const secs = +process.env.CONTENTION_SECS || 60, rate = 10, total = secs * rate, open = i => String(3500000000 + N_ORDERS + (i % N_OPEN));
    const write = i => [
      () => ({ op: 'poolUpdate', poolIds: [`${open(i)}_12345_1`], patch: { note: 'c' + i } }),
      () => ({ op: 'putSheet', sheet: { id: 'sheet-open' + (i % 20), perfNote: i } }),
      () => ({ op: 'customPut', key: `${open(i)}_12345`, receiptId: open(i), how: 'button', by: 'Paul K' }),
      () => ({ op: 'reviewMarkPut', rid: open(i), action: i % 2 ? 'mark' : 'resolve', by: 'Ana_M' }),
      () => ({ op: 'timelineAdd', events: [{ orderId: open(i), type: 'note', at: Date.now(), by: 'Bea', text: 'contention ' + i, id: 'cont-' + i }] }),
      () => ({ op: 'cancelFates', orderId: open(i), fates: [], by: 'Paul K' })
    ][i % 6]();
    const parts = n => String(n).split('.').map(Number), first = JSON.parse((await lib({ op: 'getChanges' })).body), before = JSON.parse(JSON.stringify(Lib.feedStats));
    st.atomic = true; st.txRetries = 0; st.tick = () => new Promise(r => setTimeout(r, 2 + Math.random() * 8));
    const r0 = st.reads || 0, w0 = st.writes || 0, t0 = Date.now(), pending = [];
    // PUSH (28 Oct 2026): every write that moves the feed also raises Charm_Nest_Push/counter (_charmNestPush.js: one write in flight, a dirty flag, a gap); its writes are counted here
    const Push = require(path.join(fnDir, '_charmNestPush.js')), cWrites = [], setP = st.docs.set, pushBefore = JSON.parse(JSON.stringify(Push.stats)); let cN0 = (st.doc('Charm_Nest_Push', 'counter') || {}).n || 0;
    st.docs.set = (k, v) => { if (k === 'Charm_Nest_Push/counter') cWrites.push(Date.now()); return setP.call(st.docs, k, v); };
    // (the counter takes 40 ms to write, as a Firestore write does: the fake answers at once, and a write that takes no time cannot be shared)
    const dbP = admin.firestore(), colP = dbP.collection; dbP.collection = c => (/Charm_Nest_Push$/.test(c) ? { doc: id => { const d = colP(c).doc(id); return { set: async (data, o) => { await new Promise(r => setTimeout(r, 40)); return d.set(data, o); } }; } } : colP(c));
    for (let i = 0; i < total; i++) { const due = t0 + Math.floor(i / rate) * 1000 - Date.now(); if (due > 0) await new Promise(r => setTimeout(r, due)); pending.push(lib(write(i)).then(x => x.statusCode, e => String(e))); }
    const codes = await Promise.all(pending), ms = Date.now() - t0;
    st.docs.set = setP; dbP.collection = colP;
    const peak = cWrites.reduce((m, t, i) => { let j = i; while (j < cWrites.length && cWrites[j] < t + 1000) j++; return Math.max(m, j - i); }, 0), cN = ((st.doc('Charm_Nest_Push', 'counter') || {}).n || 0) - cN0;
    const pushNote = `counter writes/s ${(cWrites.length / secs).toFixed(2)} (peak ${peak} in any second; counter n +${cN}; shared ${Push.stats.shared - pushBefore.shared} of ${Push.stats.asked - pushBefore.asked} nudges, failed ${Push.stats.failed - pushBefore.failed})`;
    const after = Lib.feedStats, failed = after.fallbacks - before.fallbacks + after.lost - before.lost, retried = after.retried - before.retried;
    const perShard = Object.fromEntries(Object.entries(after.shards).map(([k, v]) => [k, +((v - (before.shards[k] || 0)) / secs).toFixed(1)]));
    const ch = JSON.parse((await lib({ op: 'getChanges', since: first.n })).body), moved = parts(ch.n).map((x, j) => x - parts(first.n)[j]), bad = codes.filter(c => c !== 200).length;
    const named = k => Array.isArray(ch.keys) && ch.keys.includes(k);
    const pushOk = cWrites.length > 0 && cN === cWrites.length && Push.stats.failed === pushBefore.failed && peak <= 8 && cWrites.length / secs <= 5;   // (the counter: well under the ten writes a second that arrive (one write in flight, the rest share the next), none lost, none failed)
    const ok = pushOk && bad === 0 && failed === 0 && !ch.reset && named('p:' + open(0)) && named('o:' + open(4)) && named('c:' + open(3) + '_review') && named('x:' + open(5)) && named('s:sheet-open1') && moved.every(x => x > 0);
    results.push({ name: `FEED contention: ${rate} writes/s for ${secs} s across kinds (person's writes failed ${bad}; bumps failed ${failed}, retried ${retried}, transactions run again ${st.txRetries || 0}; shard writes/s ${JSON.stringify(perShard)}; ${pushNote}; counts +${moved.join('/')})`, ms, reads: (st.reads || 0) - r0, writes: (st.writes || 0) - w0, bytes: 0, ok, err: ok ? '' : `non-200 ${bad}, bumps failed ${failed}, reset ${!!ch.reset}, keys ${(ch.keys || []).length}` });
    // the feed's documents refuse every write: best-effort, never the person's write
    const feedDoc = k => /^(Charm_Nest_Rev\/(library|feed-[pox])|Charm_Nest_Push\/counter)$/.test(k), set0 = st.docs.set;   // (PUSH: the counter refuses with them)
    st.docs.set = (k, v) => { if (feedDoc(k)) throw new Error('UNAVAILABLE: the feed\'s document refuses (test)'); return set0.call(st.docs, k, v); };
    const b1 = JSON.parse(JSON.stringify(Lib.feedStats)), from = JSON.parse((await lib({ op: 'getChanges' })).body).n, t1 = Date.now(), refused = [];
    for (let i = 0; i < 20; i++) refused.push(lib(write(i)).then(x => x.statusCode, e => String(e)));
    const codes2 = await Promise.all(refused), ms2 = Date.now() - t1;
    st.docs.set = set0; st.atomic = false; st.tick = null;
    const lost = Lib.feedStats.lost - b1.lost, bad2 = codes2.filter(c => c !== 200).length;
    await lib({ op: 'poolUpdate', poolIds: [`${open(0)}_12345_1`], patch: { note: 'after' } });   // (the feed's documents take writes again: a count that moved without its list)
    const ch2 = JSON.parse((await lib({ op: 'getChanges', since: from })).body);
    const ok2 = bad2 === 0 && lost > 0 && !!ch2.keys && ch2.keys.includes('p:*');
    results.push({ name: `FEED best-effort: the feed's documents refusing every write, 20 writes of persons all answered (${20 - bad2}/20, ${lost} bumps lost); after, the pool rows' readers sent to their own question (p:*)`, ms: ms2, reads: 0, writes: 0, bytes: 0, ok: ok2, err: ok2 ? '' : `non-200 ${bad2}, lost ${lost}, keys ${JSON.stringify(ch2.keys)}` });
  }


  console.log('\n' + ['operation', 'ms', 'reads', 'writes', 'KB', 'ok', 'note'].join(' | '));
  for (const r of results) console.log([r.name, r.ms, r.reads, r.writes, r.bytes != null ? (r.bytes / 1024).toFixed(1) : '', r.ok ? 'PASS' : 'FAIL', r.err || (r.ok ? '' : `budget ${r.budget.ms} ms / ${r.budget.reads} reads`)].join(' | '));
  Module._load = realLoad;
  const bad = results.filter(r => !r.ok);
  if (process.env.PERF_TABLE !== '1') assert.strictEqual(bad.length, 0, 'over budget at scale: ' + bad.map(r => r.name).join(', '));
  console.log(bad.length ? `\n${bad.length} over budget` : '\nserver-scale: every operation within its budget');
  process.exit(0);
})().catch(e => { console.error(e); process.exit(1); });
