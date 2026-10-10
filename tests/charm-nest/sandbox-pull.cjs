// The sandbox always pulls the 250 NEWEST Etsy orders when it starts (Paul, 10 Oct 2026) — op sandboxPullOrders
// (netlify/functions/_charmNestSandboxPull.js), served by etsySandbox.js and played by the sandboxStream op.
// Offline: a fake Etsy (the receipts endpoint and the token endpoint, every request recorded), an in-memory Firestore that
// REFUSES nested arrays (as Firestore does) and an in-memory Storage, against the REAL charmNestLibrary, etsySandbox and
// _etsyMailEtsy (production's own token helper) code. No network, no live record. It proves:
//   · one pull = exactly 3 Etsy calls (offsets 0/100/200, limits 100/100/50, newest first, any status, the key and token
//     production uses) and no other Etsy request at all; the answer says so (calls 3, callsToday, capLeft);
//   · the 250 are stored as one file + the pointer Charm_Sandbox/current (with the start id as the retry marker); the
//     previous set (an old snapshot, an old pull, its stream) is gone first; sheets and the like are not touched;
//   · the stream plays only the open ones, oldest first; shipped / cancelled / unpaid receipts are kept but never listed;
//   · the same start id again returns the same set with 0 new calls; a busy claim, a used start id, a bad id answer plainly;
//   · the daily cap (6 pulls) refuses with no Etsy call and leaves the sandbox EMPTY; a failed pull (Etsy down, a page
//     failing, a limit, a missing token) leaves it EMPTY, counts exactly the calls it made, and never falls back;
//   · the old Sep 17 snapshot (a pointer without source "etsy-pull") is no set, and a warm emulator never serves a wiped set.
//   node tests/charm-nest/sandbox-pull.cjs
'use strict';
const path = require('path'), assert = require('assert');
const refuseNestedArrays = require('./_noNestedArrays.cjs');
const fnDir = path.join(__dirname, '../../netlify/functions');
process.env.SHOP_ID = '987654'; process.env.CLIENT_ID = 'test-key'; process.env.CLIENT_SECRET = 'test-secret'; delete process.env.EDIT_PASSCODE;

/* ── in-memory Firestore (refuses nested arrays) and Storage ── */
const store = new Map(), blobs = new Map();
class TS { constructor(ms) { this.ms = ms; } toMillis() { return this.ms; } }
const SERVER_TS = { __sts: true }, DEL = { __del: true };
const FieldValue = { serverTimestamp: () => SERVER_TS, delete: () => DEL, increment: n => ({ __inc: n }) };
const plain = v => !!v && typeof v === 'object' && !Array.isArray(v) && !(v instanceof TS) && v !== SERVER_TS && v !== DEL && v.__inc === undefined;
const clone = v => Array.isArray(v) ? v.map(clone) : v === SERVER_TS ? new TS(Date.now()) : (v && v.__inc !== undefined) ? v.__inc : plain(v) ? Object.fromEntries(Object.entries(v).filter(([, x]) => x !== DEL).map(([k, x]) => [k, clone(x)])) : v;
const idOf = key => key.slice(key.lastIndexOf('/') + 1);
const snap = (key, d) => ({ id: idOf(key), exists: !!d, data: () => (d ? clone(d) : undefined), get: f => (d ? d[f] : undefined) });
function docRef(key) {
  return { id: idOf(key), path: key,
    async get() { return snap(key, store.get(key)); },
    async set(data, o) { refuseNestedArrays(data, key); store.set(key, o && o.merge ? Object.assign({}, store.get(key) || {}, clone(data)) : clone(data)); },
    async delete() { store.delete(key); } };
}
const db = {
  collection: c => ({ doc: id => docRef(c + '/' + id), where: () => ({ select: () => ({ get: async () => ({ docs: [], empty: true }) }) }), count: () => ({ get: async () => ({ data: () => ({ count: [...store.keys()].filter(k => k.startsWith(c + '/') && !k.slice(c.length + 1).includes('/')).length }) }) }), async get() { const docs = [...store.keys()].filter(k => k.startsWith(c + '/') && !k.slice(c.length + 1).includes('/')).map(k => snap(k, store.get(k))); return { size: docs.length, docs, empty: !docs.length }; } }),
  doc: p => docRef(p),
  async getAll(...refs) { return Promise.all(refs.map(r => r.get())); },
  async runTransaction(fn) {
    const ops = [], tx = { get: r => r.get(), set: (r, d, o) => ops.push(() => r.set(d, o)), delete: r => ops.push(() => r.delete()) };
    const out = await fn(tx); for (const o of ops) await o(); return out;
  }
};
const bucket = { name: 'test-bucket', file(p) { return { name: p,
  async save(buf) { blobs.set(p, Buffer.from(buf)); }, async exists() { return [blobs.has(p)]; },
  async download() { if (!blobs.has(p)) throw new Error('no blob ' + p); return [blobs.get(p)]; },
  async delete(o) { if (!blobs.has(p) && !(o && o.ignoreNotFound)) throw new Error('no blob ' + p); blobs.delete(p); } }; },
  async getFiles(q = {}) { assert(q.autoPaginate === false && q.maxResults > 0, 'files are listed a page at a time'); return [[...blobs.keys()].filter(k => k.startsWith(q.prefix || '')).sort().slice(0, q.maxResults).map(n => this.file(n)), null]; } };
const admin = { firestore: Object.assign(() => db, { FieldValue, Timestamp: Object.assign(TS, { fromMillis: ms => new TS(ms) }) }), storage: () => ({ bucket: () => bucket }) };

/* ── a fake Etsy: 320 receipts, newest = highest number; every request recorded; the token endpoint too ── */
const DAY = 86400, T0 = Math.floor(Date.now() / 1000) - 400 * 3600;
const SHOP = Array.from({ length: 320 }, (_, i) => {
  const rid = 3600000000 + i, created = T0 + i * 3600, ship = created + 7 * DAY;
  const extra = i % 10 === 0 ? { is_shipped: true, status: 'Completed' } : i % 37 === 0 ? { status: 'Canceled' } : i % 53 === 0 ? { is_paid: false, status: 'Payment Processing' } : {};
  return Object.assign({ receipt_id: rid, order_number: rid, name: 'Buyer ' + rid, status: 'Paid', is_paid: true, is_shipped: false, created_timestamp: created, create_timestamp: created, expected_ship_date: ship,
    transactions: [{ transaction_id: rid * 10 + 1, receipt_id: rid, listing_id: 1718000 + (i % 9), sku: `BR-TST-0${1 + (i % 6)}`, title: 'Initial charm', quantity: 1, created_timestamp: created, expected_ship_date: ship,
      variations: [{ property_id: 513, value_id: 1, formatted_name: 'Metal', formatted_value: i % 2 ? 'Sterling Silver' : '14k Gold Filled' }, { property_id: 54, value_id: 2, formatted_name: 'Personalization', formatted_value: 'ANNA' }] }] }, extra);
});
const NEWEST = SHOP.slice().sort((a, b) => b.created_timestamp - a.created_timestamp).slice(0, 250);
const etsy = { calls: [], oauth: 0, failAt: null, throwAt: null, status: null, now: () => Date.now() };
const res = (status, body, headers = {}) => ({ ok: status >= 200 && status < 300, status, headers: { get: k => headers[String(k).toLowerCase()] }, json: async () => body, text: async () => (typeof body === 'string' ? body : JSON.stringify(body)) });
async function fakeFetch(url, init = {}) {
  const u = new URL(url);
  if (u.pathname === '/v3/public/oauth/token') { etsy.oauth++; return res(200, { access_token: 'tok-refreshed', refresh_token: 'ref-2', expires_in: 3600 }); }
  const m = u.pathname.match(/^\/v3\/application\/shops\/987654\/receipts$/);
  etsy.calls.push({ path: u.pathname, q: Object.fromEntries(u.searchParams), auth: init.headers && init.headers.Authorization, key: init.headers && init.headers['x-api-key'] });
  if (!m) return res(404, 'unexpected request ' + u.pathname);
  const offset = +u.searchParams.get('offset'), limit = +u.searchParams.get('limit');
  if (etsy.throwAt === offset) throw new Error('socket hang up');
  if (etsy.failAt === offset) return res(etsy.status || 503, etsy.statusBody || 'Service Unavailable');
  return res(200, { count: SHOP.length, results: SHOP.slice().sort((a, b) => b.created_timestamp - a.created_timestamp).slice(offset, offset + limit) });
}
const meterCalls = [];
const meterToken = { failNet() {}, fromHttp() {}, ok() {} };
const fakeMeter = { bump: id => { meterCalls.push(id); return meterToken; }, bumpSimple: id => meterCalls.push(id), flushNow: async () => {} };

const Module = require('module'), realLoad = Module._load;
Module._load = function (req, ...rest) {
  if (req === 'node-fetch') return fakeFetch;
  if (req === 'firebase-admin' || req === './firebaseAdmin' || /[\/]firebaseAdmin(\.js)?$/.test(req)) return admin;
  if (/[\/]_etsyApiMeter(\.js)?$/.test(req)) return fakeMeter;
  return realLoad.call(this, req, ...rest);
};
global.fetch = async () => { throw new Error('no network in this test'); };
const lib = require(path.join(fnDir, 'charmNestLibrary.js')), emulator = require(path.join(fnDir, 'etsySandbox.js'));
const post = b => lib.handler({ httpMethod: 'POST', headers: {}, body: JSON.stringify(b) }).then(r => ({ status: r.statusCode, body: JSON.parse(r.body) }));
const pullOp = (startId, extra) => post(Object.assign({ op: 'sandboxPullOrders', sandbox: true, startId, by: 'tester' }, extra || {}));
const listed = async () => (await emulator.handler({ httpMethod: 'GET', queryStringParameters: { fn: 'listOpenOrders' } }).then(r => JSON.parse(r.body))).results;
const emu = q => emulator.handler({ httpMethod: 'GET', queryStringParameters: q }).then(r => ({ status: r.statusCode, body: JSON.parse(r.body) }));
const setToken = (ms, name = 'tok-fresh') => store.set('config/etsyOauth', { access_token: name, refresh_token: 'ref-1', expires_at_ms: Date.now() + ms });
const resetEtsy = () => { etsy.calls.length = 0; etsy.oauth = 0; etsy.failAt = null; etsy.throwAt = null; etsy.status = null; etsy.statusBody = null; };
const wipe = () => { store.delete('Charm_Sandbox/current'); store.delete('Charm_Sandbox/stream'); for (const k of [...blobs.keys()]) if (k.startsWith('charmnest/sandbox/') && !k.startsWith('charmnest/sandbox/master/')) blobs.delete(k); };   // (what Reset does to this family; the budget stays)
const todayKey = () => new Date().toISOString().slice(0, 10);
const empty = () => !store.has('Charm_Sandbox/current') && !store.has('Charm_Sandbox/stream') && ![...blobs.keys()].some(k => k.startsWith('charmnest/sandbox/orders-pull/'));
const ledger = () => store.get('Charm_Sandbox/pulls');
const openOf = list => list.filter(r => r.is_paid !== false && !r.is_shipped && !/cancel/i.test(r.status));

(async () => {
  setToken(3600e3);
  // ── before any pull: an old snapshot (no source) is no set, an old pull and its stream are what the new pull replaces ──
  blobs.set('charmnest/sandbox/orders-2026-09-17.json', Buffer.from(JSON.stringify({ at: 1, count: 1, receipts: [Object.assign({}, SHOP[5], { receipt_id: 4100000001 })] })));
  store.set('Charm_Sandbox/current', { path: 'charmnest/sandbox/orders-2026-09-17.json', count: 1, at: 1, takenBy: 'old' });
  blobs.set('charmnest/sandbox/orders-pull/old-pull.json', Buffer.from('{}'));
  blobs.set('charmnest/sandbox/master/keep.ai', Buffer.from('master'));
  store.set('Charm_Sandbox/stream', { on: true, v: 2, seed: 1, snapshotPath: 'charmnest/sandbox/orders-2026-09-17.json', tick: 4, total: 1 });
  store.set('Sandbox_Charm_Nest_Sheets/sheet-1', { id: 'sheet-1', orders: ['4100000001'] });
  store.set('Charm_Nest_Sheets/prod-1', { id: 'prod-1' });
  assert.strictEqual((await listed()).length, 0, 'the old snapshot (no source) lists nothing');
  assert.strictEqual((await emu({ fn: 'status' })).body.count, 0, 'and the emulator holds no orders for it');
  let st = await post({ op: 'sandboxStatus', sandbox: true, light: true }); assert.strictEqual(st.body.snapshot, null, 'the status shows no set for an old snapshot');
  assert.strictEqual((await post({ op: 'sandboxStream', action: 'ensure', sandbox: true })).status, 409, 'no stream without a pulled set');
  const put = await post({ op: 'sandboxPut', sandbox: true, path: 'charmnest/sandbox/x.json' }); assert(put.status === 410 && /retired/.test(put.body.error), 'Take a sandbox snapshot is retired: ' + JSON.stringify(put.body));

  // ── refusals that touch nothing ──
  let r = await post({ op: 'sandboxPullOrders', startId: 'start-aaaaaaaa' }); assert.strictEqual(r.status, 403, 'production never pulls'); assert(store.has('Charm_Sandbox/current'));
  r = await pullOp('short'); assert(r.status === 400 && r.body.reason === 'bad' && r.body.empty === false, 'a start id is required'); assert(store.has('Charm_Sandbox/current'), 'a bad request keeps what is there');
  assert.strictEqual(etsy.calls.length, 0);

  // ── pull 1: exactly 3 Etsy calls ──
  r = await pullOp('start-0001-aaaa', { seed: 777, speed: 50 });
  assert.strictEqual(r.status, 200, JSON.stringify(r.body).slice(0, 300));
  const a = r.body;
  assert.deepStrictEqual(etsy.calls.map(c => [c.q.offset, c.q.limit]).sort(), [['0', '100'], ['100', '100'], ['200', '50']], 'pages of 100, 100 and 50');
  assert(etsy.calls.every(c => c.path === '/v3/application/shops/987654/receipts' && c.q.sort_on === 'created' && c.q.sort_order === 'desc' && !('was_shipped' in c.q) && !('status' in c.q) && !('was_canceled' in c.q) && c.auth === 'Bearer tok-fresh' && c.key === 'test-key:test-secret'), 'newest first, any status, production\'s token and key');
  assert.strictEqual(etsy.calls.length, 3, 'three Etsy calls and nothing else'); assert.strictEqual(etsy.oauth, 0, 'a fresh token is not refreshed');
  assert.deepStrictEqual(meterCalls.splice(0), ['sandbox.receiptsPage', 'sandbox.receiptsPage', 'sandbox.receiptsPage'], 'the inbox meter sees the 3 calls');
  assert(a.ok && !a.already && a.calls === 3 && a.tokenRefreshes === 0 && a.count === 250 && a.source === 'etsy-pull' && a.label === '250 newest Etsy orders' && a.startId === 'start-0001-aaaa', JSON.stringify(a).slice(0, 400));
  const wantOpen = openOf(NEWEST).length;
  assert(wantOpen < 250 && wantOpen > 150 && a.open === wantOpen && a.closed === 250 - wantOpen && a.closedBy.shipped + a.closedBy.canceled + a.closedBy.unpaid === a.closed && a.closedBy.shipped > 0 && a.closedBy.canceled > 0 && a.closedBy.unpaid > 0, 'open / closed counts: ' + JSON.stringify([a.open, a.closed, a.closedBy]));
  assert(a.callsToday === 3 && a.pullsToday === 1 && a.cap === 6 && a.capLeft === 5 && a.callsCap === 20, 'the budget in the answer: ' + JSON.stringify([a.callsToday, a.pullsToday, a.cap, a.capLeft]));
  assert(a.pulledAt > 0 && a.newestAt === NEWEST[0].created_timestamp * 1000 && a.oldestAt === NEWEST[249].created_timestamp * 1000 && a.stream && a.stream.seed === 777 && a.stream.total === wantOpen && a.stream.tick === 0, 'newest and oldest, and the stream started for the set: ' + JSON.stringify(a.stream));
  // the previous set is gone, the new one stands, the rest is untouched
  const cur = store.get('Charm_Sandbox/current');
  assert(cur.source === 'etsy-pull' && cur.startId === 'start-0001-aaaa' && cur.path === a.path && cur.count === 250 && cur.open === wantOpen, 'the pointer is the marker');
  assert(!blobs.has('charmnest/sandbox/orders-2026-09-17.json') && !blobs.has('charmnest/sandbox/orders-pull/old-pull.json'), 'the old snapshot and the old pull are deleted');
  assert([...blobs.keys()].filter(k => k.startsWith('charmnest/sandbox/orders-pull/')).length === 1 && blobs.has(a.path), 'one file holds the set');
  assert(blobs.has('charmnest/sandbox/master/keep.ai') && store.has('Sandbox_Charm_Nest_Sheets/sheet-1') && store.has('Charm_Nest_Sheets/prod-1'), 'master files, sandbox sheets (the page wipes those) and production are not touched by the pull');
  const file = JSON.parse(blobs.get(a.path).toString('utf8'));
  assert.deepStrictEqual(file.receipts, NEWEST, 'the 250 newest, as Etsy returned them, transactions included, newest first');
  assert(store.get('Charm_Sandbox/stream').snapshotPath === a.path && store.get('Charm_Sandbox/stream').seed === 777, 'the stream plays this set');
  assert(ledger().pulls === 1 && ledger().calls === 3 && ledger().day === todayKey() && ledger().claim === null && ledger().starts[0].id === 'start-0001-aaaa', 'the ledger counts the pull: ' + JSON.stringify(ledger()));
  st = await post({ op: 'sandboxStatus', sandbox: true }); assert(st.body.snapshot.source === 'etsy-pull' && st.body.snapshot.open === wantOpen && st.body.budget.capLeft === 5 && st.body.budget.callsToday === 3, 'status shows the pull and the budget');
  assert(!JSON.stringify(a).includes('tok-fresh') && !JSON.stringify(a).includes('test-secret'), 'no token or secret in an answer');

  // ── the stream plays the open ones, oldest first; the closed ones are kept but never listed ──
  assert.strictEqual((await listed()).length, 0, 'a new stream lists nothing yet');
  const openIds = openOf(NEWEST).sort((x, y) => x.created_timestamp - y.created_timestamp).map(x => String(x.receipt_id)), seen = [];
  for (let i = 0; i < 4; i++) { const t = await post({ op: 'sandboxStream', action: 'tick', sandbox: true, expect: store.get('Charm_Sandbox/stream').simNow }); assert(t.body.advanced, 'a step'); }
  const lst = await listed(); seen.push(...lst.map(x => String(x.receipt_id)).reverse());
  assert(seen.length >= 8 && seen.length <= 20 && seen.every((id, i) => id === openIds[i]), 'the first orders in are the OLDEST open ones, in order: ' + seen.slice(0, 5));
  assert(lst.every(x => x.transactions.length === 1 && x.transactions[0].variations.length === 2), 'each streamed order has its transactions');
  const closedId = String(NEWEST.find(x => x.is_shipped).receipt_id);
  assert(!seen.includes(closedId), 'a shipped order is never listed');
  const one = await emu({ fn: 'etsyOrderProxy', orderId: closedId }); assert.strictEqual(one.status, 404, 'with the stream playing, an order that has not come is not found (the stream decides)');

  // ── the same start again: the same set, no new call ──
  const before = etsy.calls.length;
  r = await pullOp('start-0001-aaaa', { seed: 999 });
  assert(r.status === 200 && r.body.already === true && r.body.calls === 0 && r.body.count === 250 && r.body.pulledAt === a.pulledAt && r.body.path === a.path && r.body.pullsToday === 1 && r.body.callsToday === 3, 'a retry returns the stored set: ' + JSON.stringify(r.body).slice(0, 300));
  assert.strictEqual(etsy.calls.length, before, 'a retry makes no Etsy call'); assert(r.body.stream && r.body.stream.seed === 777 && r.body.stream.tick === 4, 'and resumes the same stream, not a new one');
  assert.strictEqual(ledger().pulls, 1, 'a retry costs nothing');

  // ── a pull in flight (a live claim) is not doubled, and what is there is kept; a stale claim is taken over ──
  store.set('Charm_Sandbox/pulls', Object.assign({}, ledger(), { claim: { startId: 'start-0002-bbbb', at: Date.now() } }));
  r = await pullOp('start-0002-bbbb'); assert(r.status === 409 && r.body.reason === 'busy' && r.body.empty === false && /already pulling/.test(r.body.error), 'busy: ' + JSON.stringify(r.body)); assert(store.has('Charm_Sandbox/current') && etsy.calls.length === before, 'busy keeps the set and calls nothing');
  store.set('Charm_Sandbox/pulls', Object.assign({}, ledger(), { claim: { startId: 'start-0002-bbbb', at: Date.now() - 120000 } }));

  // ── a new start replaces the set completely (the old file goes, the stream starts over) ──
  const oldPath = a.path; resetEtsy();
  r = await pullOp('start-0002-bbbb', { seed: 5 });
  assert(r.status === 200 && !r.body.already && r.body.calls === 3 && r.body.pullsToday === 2 && r.body.callsToday === 6 && r.body.capLeft === 4 && r.body.path !== oldPath, 'start 2: ' + JSON.stringify(r.body).slice(0, 200));
  assert(!blobs.has(oldPath) && [...blobs.keys()].filter(k => k.startsWith('charmnest/sandbox/orders-pull/')).length === 1, 'the previous set is replaced completely');
  assert(r.body.stream.seed === 5 && r.body.stream.tick === 0, 'a new stream for the new set');
  // a warm emulator never answers a wiped set from memory
  await post({ op: 'sandboxStream', action: 'tick', sandbox: true }); assert((await listed()).length > 0, 'the new set plays');
  wipe(); assert.strictEqual((await listed()).length, 0, 'after a wipe the warm emulator lists nothing (not from its memory)'); assert.strictEqual((await emu({ fn: 'status' })).body.count, 0);
  // the same start after a wipe is refused without a call; a new one pulls
  resetEtsy();
  r = await pullOp('start-0002-bbbb'); assert(r.status === 409 && r.body.reason === 'startUsed' && etsy.calls.length === 0, 'the same start after a wipe: ' + JSON.stringify(r.body)); assert.strictEqual(ledger().pulls, 2);

  // ── failures leave the sandbox EMPTY, count what they spent, and never fall back ──
  const seedSet = async () => { const k = await pullOp('start-seed-' + Math.random().toString(36).slice(2, 8)); assert.strictEqual(k.status, 200); resetEtsy(); return k.body; };
  // Etsy down on the first page: one call
  store.set('Charm_Sandbox/pulls', Object.assign({}, ledger(), { pulls: 2, calls: 6 }));
  await seedSet(); const L0 = ledger().pulls, C0 = ledger().calls; store.set('Charm_Sandbox/pulls', Object.assign({}, ledger(), { pulls: 2, calls: 6 }));
  etsy.failAt = 0; etsy.status = 503;
  r = await pullOp('start-down-0003');
  assert(r.status === 502 && r.body.ok === false && r.body.reason === 'etsy' && r.body.empty === true && r.body.calls === 1 && /Etsy did not answer \(HTTP 503\)/.test(r.body.error) && !/\n/.test(r.body.error), 'Etsy down: ' + JSON.stringify(r.body));
  assert(empty(), 'the sandbox is EMPTY after a failed pull'); assert.strictEqual(etsy.calls.length, 1, 'page one failed: the other two pages were not asked');
  assert.strictEqual(ledger().calls, 7, 'the one call counted'); assert.strictEqual(ledger().pulls, 3, 'the attempt counted'); assert.strictEqual(ledger().claim, null, 'the claim is released');
  // a retry of the same start is allowed (it did not succeed) and counted
  resetEtsy(); r = await pullOp('start-down-0003'); assert(r.status === 200 && r.body.calls === 3 && r.body.pullsToday === 4 && r.body.callsToday === 10, 'the retry of a failed start pulls: ' + JSON.stringify([r.status, r.body.calls, r.body.pullsToday, r.body.callsToday]));
  // a later page fails: the pages asked are counted, nothing is kept
  resetEtsy(); etsy.failAt = 100; etsy.status = 500;
  r = await pullOp('start-page-0004'); assert(r.status === 502 && r.body.reason === 'etsy' && r.body.calls === 3 && r.body.empty && empty(), 'page 2 failing: ' + JSON.stringify(r.body));
  assert.strictEqual(ledger().pulls, 5); assert.strictEqual(ledger().calls, 13);
  // the network drops
  resetEtsy(); etsy.throwAt = 0; r = await pullOp('start-net-00005'); assert(r.status === 502 && r.body.reason === 'etsy' && r.body.calls === 1 && /network error/.test(r.body.error) && empty(), 'a dropped connection: ' + JSON.stringify(r.body));
  // Etsy's daily limit
  store.set('Charm_Sandbox/pulls', Object.assign({}, ledger(), { pulls: 1, calls: 3 }));
  resetEtsy(); etsy.failAt = 0; etsy.status = 429; etsy.statusBody = 'You have exceeded your daily rate limit'; r = await pullOp('start-lim-00006'); assert(r.status === 502 && r.body.reason === 'limit' && /daily limit/.test(r.body.error) && empty(), 'the daily limit: ' + JSON.stringify(r.body));
  // the token: a refresh is made when it is due (and counted), a missing token fails with no Etsy data call and gives the slot back
  resetEtsy(); store.set('Charm_Sandbox/pulls', { day: todayKey(), pulls: 0, calls: 0, tokenRefreshes: 0, starts: [], claim: null });
  setToken(-60000, 'tok-stale'); delete require.cache[require.resolve(path.join(fnDir, '_etsyMailEtsy.js'))];   // (a cold instance: production's helper keeps the token it saw in memory too)
  r = await pullOp('start-tok-00007');
  assert(r.status === 200 && r.body.calls === 3 && r.body.tokenRefreshes === 1 && etsy.oauth === 1 && etsy.calls.every(c => c.auth === 'Bearer tok-refreshed') && store.get('config/etsyOauth').access_token === 'tok-refreshed', 'a due token is refreshed by production\'s own helper: ' + JSON.stringify(r.body).slice(0, 200));
  resetEtsy(); store.delete('config/etsyOauth');
  r = await pullOp('start-tok-00008'); assert(r.status === 503 && r.body.reason === 'token' && r.body.calls === 0 && r.body.empty && empty() && etsy.calls.length === 0 && /sign-in/.test(r.body.error), 'no token: ' + JSON.stringify(r.body));
  assert.strictEqual(ledger().pulls, 1, 'a pull that never reached Etsy gives its slot back');
  setToken(3600e3);

  // ── the daily cap: six pulls, then a plain refusal that leaves the sandbox empty and asks Etsy nothing ──
  resetEtsy(); store.set('Charm_Sandbox/pulls', { day: todayKey(), pulls: 5, calls: 15, tokenRefreshes: 0, starts: [{ id: 'old-start-0001', at: 1, ok: true, calls: 3 }], claim: null });
  r = await pullOp('start-six-00009'); assert(r.status === 200 && r.body.pullsToday === 6 && r.body.capLeft === 0 && r.body.callsToday === 18, 'the sixth pull of the day: ' + JSON.stringify([r.status, r.body.pullsToday, r.body.capLeft]));
  resetEtsy();
  r = await pullOp('start-seven-0010');
  assert(r.status === 429 && r.body.reason === 'cap' && r.body.empty === true && r.body.calls === 0 && r.body.capLeft === 0 && r.body.error === "Today's 6 sandbox pulls from Etsy are used up, so the sandbox stays empty until tomorrow (UTC midnight)." && /T00:00:00\.000Z$/.test(r.body.resetAt) && Date.parse(r.body.resetAt) > Date.now(), 'the cap: ' + JSON.stringify(r.body));
  assert(empty() && etsy.calls.length === 0, 'the cap leaves the sandbox empty and asks Etsy nothing');
  assert.strictEqual(ledger().pulls, 6, 'a refused pull is not counted');
  // after a wipe the budget is still spent (Reset cannot be used to get round the cap)
  wipe(); assert.strictEqual(ledger().pulls, 6); r = await pullOp('start-eight-0011'); assert.strictEqual(r.body.reason, 'cap');
  // a new UTC day starts the count over
  store.set('Charm_Sandbox/pulls', Object.assign({}, ledger(), { day: '2000-01-01' }));
  r = await pullOp('start-nine-00012'); assert(r.status === 200 && r.body.pullsToday === 1 && r.body.callsToday === 3 && r.body.capLeft === 5, 'a new day: ' + JSON.stringify([r.status, r.body.pullsToday]));

  // ── the whole run asked Etsy for receipts and nothing else ──
  console.log('sandbox pull OK: 3 Etsy calls per pull, ' + wantOpen + ' of 250 open, cap 6/day');
  Module._load = realLoad;
})().catch(e => { console.error(e); process.exit(1); });
