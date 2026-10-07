// Reads, bytes and writes per CALL of the generic doors the open pages knock on (Firebase cost emergency, FC14), measured with tests/cost/meter.cjs
// on an in-memory Firestore seeded the way production is shaped, and projected per open page per hour from the idle call rates that
// tests/cost/fc14-idle-pages.cjs measures in a real browser.
//
//   node tests/cost/fc14-doors.cjs          prints the table; exits 1 when a budget below is exceeded
//
// No network, no secret, no Etsy call, no paid call. The product code under test is the real netlify/functions/*.js, loaded through the
// meter's firebaseAdmin hook.
'use strict';
const path = require('path'), assert = require('assert');
const root = path.join(__dirname, '../..');
const meter = require('./meter.cjs');
const fn = f => path.join(root, 'netlify/functions', f);
const say = s => process.stdout.write(s + '\n');

process.env.ETSYMAIL_EXTENSION_SECRET = 'meter-secret';         // (a made-up value for this test only)
process.env.CONTEXT = 'dev';

const m = meter.create();
const db = m.db, Timestamp = meter.Timestamp;
const T = ms => Timestamp.fromMillis(ms);
const NOW = Date.now();

/* ── production-shaped fixtures ── */
const seed = {};
// the Etsy call meter's document (EtsyMail_Config/etsyApiCounters): 34 call sites × 5 buckets + a timestamp
const sites = {}; for (let i = 0; i < 34; i++) sites['helper.site' + i] = { attempt: 982 + i, ok: 980, failHttp: 0, fail429: 2, failNet: 0, lastAttemptAt: T(NOW - i * 1000), last60s: i % 7 };
seed['EtsyMail_Config/etsyApiCounters'] = { day: '2026-10-07', grandTotal: 33000, sites, updatedAt: T(NOW) };
seed['EtsyApi_Config/usage'] = { day: '2026-10-07', apps: { 'pricing-console': { count: 412, since: NOW - 3600e3 }, 'design-station': { count: 90, since: NOW - 3600e3 } }, etsy: { limit_per_day: 5000, remaining_today: 3120, reported_at: NOW - 60e3 }, qps: { max: 2 } };
// the inbox: 2,000 threads of ~3 KB (searchable text, summaries) and one thread with 40 messages
const txt = 'lorem ipsum dolor sit amet '.repeat(100);
for (let i = 0; i < 2000; i++) seed['EtsyMail_Threads/etsy_conv_' + i] = { customerName: 'Buyer ' + i, status: i % 3 ? 'auto_replied' : 'pending_human_review', searchableText: txt, summary: txt.slice(0, 600), updatedAt: T(NOW - i * 60e3), lastMessageAt: T(NOW - i * 60e3) };
for (let i = 0; i < 40; i++) seed['EtsyMail_Threads/etsy_conv_1/messages/m' + i] = { text: txt.slice(0, 400), direction: i % 2 ? 'inbound' : 'outbound', timestamp: T(NOW - (40 - i) * 3600e3), createdAt: T(NOW - (40 - i) * 3600e3) };
// the shared lock table of the Design Stations: 300 tombstones and 12 live locks, 8 sorter claims
for (let i = 0; i < 300; i++) seed['Design_RealTime_Selected_Orders/' + (1000000 + i)] = { selected: false, selectedBy: null, page: null, at: T(NOW - (i + 1) * 3600e3), expireAt: new Date(NOW + 20 * 86400e3) };
for (let i = 0; i < 12; i++) seed['Design_RealTime_Selected_Orders/' + (2000000 + i)] = { selected: true, selectedBy: 'client-' + i, page: 'design', at: T(NOW - i * 60e3) };
for (let i = 0; i < 8; i++) seed['Design_RealTime_Selected_Orders/' + (3000000 + i)] = { claimed: true, claimedBy: 'sorter', claimRun: 'run1', claimAt: T(NOW - i * 60e3), at: T(NOW - i * 60e3) };
// completed orders: the whole history of the ledger (3,000 ids here; production has more)
for (let i = 0; i < 3000; i++) seed['Design_Completed Orders/' + (5000000 + i)] = { completedAt: T(NOW - i * 3600e3), by: 'x' };
seed['config/stationAdmins'] = { names: ['Paul K'] };
db.seed(seed);
m.install();

const proxy = require(fn('firestoreProxy.js')), orders = require(fn('firebaseOrders.js')), usage = require(fn('etsyApiUsage.js')), gate = require(fn('authGate.js'));
const H = { 'x-etsymail-secret': 'meter-secret' };
const get = (h, q, headers) => h.handler({ httpMethod: 'GET', headers: headers || {}, queryStringParameters: q || {}, multiValueQueryStringParameters: {}, body: null });
const post = (h, b) => h.handler({ httpMethod: 'POST', headers: { 'x-nf-client-connection-ip': '203.0.113.9' }, queryStringParameters: {}, body: JSON.stringify(b) });

const rows = [];
async function one(name, perHourShown, call, budget) {
  const a = m.snapshot();
  const res = await m.op(name, call);
  const d = m.since(a);
  rows.push({ name, reads: d.reads + d.aggs, bytes: d.bytes, writes: d.writes, perHour: perHourShown });
  if (budget) meter.assertMax(d, budget, name);
  return res;
}

(async () => {
  // 1 · the Inbox API meter panel (etsy-mail-1.html), before: every 3 s = 1,200/h per open tab; after: 0 hidden, 300 to 1,200 shown
  let r = await one('inbox meter: firestoreProxy get etsyApiCounters', 1200, () => get(proxy, { op: 'get', coll: 'EtsyMail_Config', id: 'etsyApiCounters' }, H), { reads: 1, bytes: 9000 });
  assert.strictEqual(JSON.parse(r.body).exists, true);
  // 2 · inbox thread-list delta (nothing new) and open-thread message delta (nothing new)
  await one('inbox list delta since=now, 0 docs (60 s poll)', 60, () => get(proxy, { op: 'list', coll: 'EtsyMail_Threads', orderBy: 'updatedAt,desc', since: String(NOW + 1000), limit: '200' }, H), { reads: 1 });
  await one('inbox open thread: listSub messages delta, 0 docs (10 s poll)', 360, () => get(proxy, { op: 'listSub', coll: 'EtsyMail_Threads', id: 'etsy_conv_1', sub: 'messages', orderBy: 'createdAt,asc', since: String(NOW + 1000), sinceField: 'createdAt', limit: '200' }, H), { reads: 1 });
  await one('inbox open thread: get thread doc (every 3rd tick, 30 s)', 120, () => get(proxy, { op: 'get', coll: 'EtsyMail_Threads', id: 'etsy_conv_1' }, H), { reads: 1 });
  // 3 · what a page load of the inbox costs once: the first full list (limit 200, a thread is ~3 KB) and a full message read
  await one('inbox boot: list threads limit 200 (once per load)', 0, () => get(proxy, { op: 'list', coll: 'EtsyMail_Threads', orderBy: 'updatedAt,desc', limit: '200' }, H));
  await one('inbox boot: listSub messages limit 500 (once per thread opened)', 0, () => get(proxy, { op: 'listSub', coll: 'EtsyMail_Threads', id: 'etsy_conv_1', sub: 'messages', orderBy: 'timestamp,asc', limit: '500' }, H));
  // 4 · the dead `counts` op (nothing in the repo calls it): what one call would cost
  await one('firestoreProxy counts op (unused; one call)', 0, () => get(proxy, { op: 'counts', coll: 'EtsyMail_Threads', groupBy: 'status' }, H));
  // 5 · the Design Station lock poll
  await one('design.html lock poll: firebaseOrders?rtSince (nothing new)', 310, () => get(orders, { rtSince: String(NOW + 1000) }));
  await one('design.html lock poll: firebaseOrders?rtSince (2 changes)', 0, () => { db.seed({ 'Design_RealTime_Selected_Orders/9000001': { selected: true, selectedBy: 'c', page: 'design', at: T(NOW + 2000) }, 'Design_RealTime_Selected_Orders/9000002': { selected: false, at: T(NOW + 3000) } }); return get(orders, { rtSince: String(NOW + 1500) }); });
  await one('design pages boot: firebaseOrders?rt=1 (all locks and claims)', 0, () => get(orders, { rt: '1' }));
  await one('design pages boot: firebaseOrders?designCompleted=1 (whole ledger, ids only)', 0, () => get(orders, { designCompleted: '1' }));
  // 6 · the usage widget of the Pricing console
  await one('etsy-pricing: etsyApiUsage (15 s)', 240, () => get(usage, { app: 'pricing-console' }), { reads: 1, bytes: 2000 });
  // 7 · station session beat (every 5 min per signed-in page) and live beat (every 30 s while an order is open)
  const sid = 'assembly-1-FIXTURE1';
  const start = { id: sid, event: 'start', station: 'assembly', device: 'assembly-1', person: 'Test Person', computerId: 'comp-fixture-1', at: Date.now(), sentAt: Date.now() };
  await one('session start (once per sign-in)', 0, () => post(orders, { session: start }));
  await one('session beat (every 5 min per signed-in page)', 12, () => post(orders, { session: Object.assign({}, start, { event: 'beat', at: Date.now(), sentAt: Date.now() }) }), { reads: 3, writes: 1 });
  const live = { v: 1, event: 'work', station: 'assembly', device: 'assembly-1', person: 'Test Person', computer: 'comp-fixture-1', session: sid, startAt: Date.now(), sentAt: Date.now(), order: { kind: 'order', rid: '1234567890', orderNumber: '1234567890', title: '', customer: '', scannedAt: Date.now(), pieces: [], pieceCount: 1, note: '' } };
  await post(orders, { live });
  await one('live beat (every 30 s while an order is open; at most 20 per order)', 0, () => post(orders, { live: Object.assign({}, live, { event: 'beat' }) }));
  // 8 · the passcode door: no Firestore at all
  await one('authGate GET (design pages, once per load)', 0, () => get(gate, {}), { reads: 0, writes: 0 });

  say('\ncall                                                               reads   bytes writes  calls/h (open page)  reads/h   KB/h');
  for (const r of rows) say(r.name.padEnd(66) + String(r.reads).padStart(6) + String(r.bytes).padStart(8) + String(r.writes).padStart(7) + String(r.perHour || '').padStart(10) + String(r.perHour ? r.reads * r.perHour : '').padStart(16) + String(r.perHour ? Math.round(r.bytes * r.perHour / 1024) : '').padStart(8));
  m.uninstall();
  say('\nok');
})().catch(e => { say('FAILED ' + (e && e.stack || e)); process.exit(1); });
