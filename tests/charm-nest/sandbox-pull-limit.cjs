// Settings → "Newest Etsy orders the sandbox pulls" (Paul, 11 Oct 2026: "adjust the # of newest listings the Sandbox test pulls from Etsy
// after a purge of Sandbox records"). The SERVER half, offline: an in-memory Firestore and Storage (tests/charm-nest/_sandboxFakes.cjs), a
// FAKE Etsy (every request recorded) and the REAL charmNestLibrary, _charmNestSandboxPull and _charmNestRepoAuth code. No network, no live record.
// It proves:
//   · nothing set = 250 exactly as before: the status says so (limit 250, not set), a pull is 3 Etsy calls (offsets 0/100/200, limits 100/100/50);
//   · the limits: 10 to 500, a number outside is held to the nearest with one plain line, a non-number is refused and writes nothing; a value
//     hand-written outside the limits (or not a number) is held / ignored at read time, so the limits hold whatever is stored;
//   · the write is PROTECTED: no token, a forged token, a token for another kind of action, a single-use delete token, a GET: all refused with
//     "repo-locked" and nothing written; a token the password dialog would have given ("modify", also "upload" and "other") is accepted;
//   · the number is a setting, not sandbox data: the wipe (sandboxReset, run to its end) and the registry leave it alone, and nothing pulls by itself
//     after the wipe (the sandbox stays empty until a pull is asked for, and no Etsy call is made);
//   · a pull uses the number: 120 = 2 calls (100 + 20), 30 = 1 call, 500 = 5 calls; the answer says what was asked for (want), the ledger counts the
//     calls made, and since NOLIMIT (Paul, 13 Oct 2026: "I don't want any of these limits") there is NO daily cap on pulls or Etsy calls: the status and the
//     answers carry no cap (null), and a pull of 500 orders repeated 10 times in a day is never refused (the old gate stopped at 20 calls / 6 pulls, so after 4).
//   node tests/charm-nest/sandbox-pull-limit.cjs
'use strict';
const path = require('path'), assert = require('assert'), Module = require('module');
const fnDir = path.join(__dirname, '../../netlify/functions');
process.env.SHOP_ID = '987654'; process.env.CLIENT_ID = 'test-key'; process.env.CLIENT_SECRET = 'test-secret'; delete process.env.EDIT_PASSCODE;
const Fk = require('./_sandboxFakes.cjs'); Fk.install();
const { store, blobs, writes, db } = Fk;
const Families = require('../../charm-nest-sandbox-families.js');

/* ── a fake Etsy: 520 receipts, newest = highest number; every request recorded ── */
const DAY = 86400, T0 = Math.floor(Date.now() / 1000) - 600 * 3600;
const SHOP = Array.from({ length: 520 }, (_, i) => {
  const rid = 3700000000 + i, created = T0 + i * 3600;
  return { receipt_id: rid, order_number: rid, name: 'Buyer ' + rid, status: 'Paid', is_paid: true, is_shipped: i % 11 === 0, created_timestamp: created, create_timestamp: created, expected_ship_date: created + 7 * DAY,
    transactions: [{ transaction_id: rid * 10 + 1, receipt_id: rid, listing_id: 1718000 + (i % 9), sku: `BR-TST-0${1 + (i % 6)}`, title: 'Initial charm', quantity: 1, created_timestamp: created, expected_ship_date: created + 7 * DAY, variations: [] }] };
});
const NEWEST = n => SHOP.slice().sort((a, b) => b.created_timestamp - a.created_timestamp).slice(0, n);
const etsy = { calls: [] };
const res = (status, body) => ({ ok: status >= 200 && status < 300, status, headers: { get: () => undefined }, json: async () => body, text: async () => (typeof body === 'string' ? body : JSON.stringify(body)) });
async function fakeFetch(url) {
  const u = new URL(url);
  if (u.pathname === '/v3/public/oauth/token') return res(200, { access_token: 'tok-refreshed', refresh_token: 'ref-2', expires_in: 3600 });
  etsy.calls.push({ path: u.pathname, offset: +u.searchParams.get('offset'), limit: +u.searchParams.get('limit') });
  if (!/^\/v3\/application\/shops\/987654\/receipts$/.test(u.pathname)) return res(404, 'unexpected request ' + u.pathname);
  const offset = +u.searchParams.get('offset'), limit = +u.searchParams.get('limit');
  return res(200, { count: SHOP.length, results: NEWEST(SHOP.length).slice(offset, offset + limit) });
}
const meterToken = { failNet() {}, fromHttp() {}, ok() {} };
const fakeMeter = { bump: () => meterToken, bumpSimple() {}, flushNow: async () => {} };
const realLoad = Module._load;
Module._load = function (req, ...rest) {
  if (req === 'node-fetch') return fakeFetch;
  if (/[\/]_etsyApiMeter(\.js)?$/.test(req)) return fakeMeter;
  return realLoad.call(this, req, ...rest);
};
const warnings = [], realWarn = console.warn; console.warn = (...a) => { warnings.push(a.map(String).join(' ')); };
const lib = require(path.join(fnDir, 'charmNestLibrary.js')), Pull = require(path.join(fnDir, '_charmNestSandboxPull.js')), RA = require(path.join(fnDir, '_charmNestRepoAuth.js'));

const call = (method, b, qs) => lib.handler({ httpMethod: method, headers: {}, body: method === 'POST' ? JSON.stringify(b) : undefined, queryStringParameters: method === 'GET' ? Object.assign({}, b) : (qs || {}) }).then(r => ({ status: r.statusCode, body: JSON.parse(r.body || '{}') }));
const post = b => call('POST', b);
const repoStamp = require('./_repoToken.cjs');   // (the Settings buttons' ops, pull and reset, carry the standing unlock the door issues on the same in-memory Firestore)
const pullOp = async (startId, extra) => post(await repoStamp('charmNestLibrary', Object.assign({ op: 'sandboxPullOrders', sandbox: true, startId, by: 'tester' }, extra || {})));
const tokenFor = async action => (await RA.issue(db, { action, by: 'tester' })).token;   // what repoUnlock hands out after the right password (the password itself is the door's own test)
const setOp = async (limit, action = 'modify') => post({ op: 'sandboxPullLimitSet', limit, by: 'Paul', repoToken: await tokenFor(action) });
const CFG = 'config/charmNestSandboxPull';
const setToken = () => store.set('config/etsyOauth', { access_token: 'tok-fresh', refresh_token: 'ref-1', expires_at_ms: Date.now() + 3600e3 });
const todayKey = () => new Date().toISOString().slice(0, 10);
const ledger = () => store.get('Charm_Sandbox/pulls');
const offsets = () => etsy.calls.map(c => [c.offset, c.limit]);
const empty = () => !store.has('Charm_Sandbox/current') && !store.has('Charm_Sandbox/stream') && ![...blobs.keys()].some(k => k.startsWith('charmnest/sandbox/orders-pull/'));
const limitOf = async () => (await post({ op: 'sandboxStatus', sandbox: true })).body.pullLimit;

(async () => {
  setToken();

  /* ═══ 1 · nothing set: 250, exactly as today ═══ */
  let lim = await limitOf();
  assert(lim && lim.limit === 250 && lim.isDefault === true && lim.isSet === false && lim.def === 250 && lim.min === 10 && lim.max === 500 && lim.perPage === 100 && lim.callsPerPull === 3 && lim.pullsPerDay === null && lim.callsCap === null && lim.pullsCap === null, 'no document = 250: ' + JSON.stringify(lim));
  assert(!store.has(CFG), 'reading the number writes nothing');
  let r = await pullOp('start-default-01');
  assert(r.status === 200 && r.body.count === 250 && r.body.want === 250 && r.body.calls === 3, 'the default pull brings 250 with 3 calls: ' + JSON.stringify(r.body).slice(0, 200));
  assert.deepStrictEqual(offsets(), [[0, 100], [100, 100], [200, 50]], 'pages of 100, 100 and 50, as before');
  assert.strictEqual(store.get('Charm_Sandbox/current').want, 250, 'the pointer remembers what was asked for');
  assert.strictEqual(ledger().calls, 3);

  /* ═══ 2 · the limits (pure, then through the server) ═══ */
  for (const [x, v, held] of [[10, 10, false], [11, 11, false], [250, 250, false], [500, 500, false], [5, 10, true], [-40, 10, true], [0, 10, true], [501, 500, true], [99999, 500, true], ['300', 300, false], [' 40 ', 40, false], [12.6, 13, false], [9.4, 10, true]])
    assert.deepStrictEqual(Pull.clampWant(x), { value: v, clamped: held }, 'clampWant(' + JSON.stringify(x) + ')');
  for (const x of ['abc', '', '  ', null, undefined, NaN, Infinity, true, {}, []]) assert.strictEqual(Pull.clampWant(x), null, 'not a number: ' + JSON.stringify(x));
  assert.deepStrictEqual([Pull.MIN_WANT, Pull.MAX_WANT, Pull.WANT], [10, 500, 250]);
  // the Etsy cost of each end: 10 = 1 call, 100 = 1, 101 = 2, 250 = 3, 500 = 5; the daily 20 calls / 6 pulls still allow 4 of the biggest
  assert.deepStrictEqual([10, 100, 101, 250, 300, 301, 500].map(Pull.pagesFor), [1, 1, 2, 3, 3, 4, 5]);
  assert.deepStrictEqual(Pull.planFor(250), [[0, 100], [100, 100], [200, 50]]); assert.deepStrictEqual(Pull.planFor(120), [[0, 100], [100, 20]]); assert.deepStrictEqual(Pull.planFor(30), [[0, 30]]);
  assert.strictEqual(Pull.planFor(500).length, 5); assert(Pull.planFor(500).every(([o, l], i) => o === i * 100 && l === 100));

  /* ═══ 3 · the write is protected ═══ */
  const before = JSON.stringify([...store.entries()].filter(([k]) => !k.startsWith('CharmRepo_Tokens/')).sort());
  const refused = async (b, why, method) => {
    const out = await call(method || 'POST', b);
    assert(out.status === 401 && out.body.error === 'repo-locked' && out.body.need === 'modify', why + ': ' + out.status + ' ' + JSON.stringify(out.body));
    assert(!store.has(CFG), why + ': nothing was written'); return out;
  };
  await refused({ op: 'sandboxPullLimitSet', limit: 100 }, 'no token');
  await refused({ op: 'sandboxPullLimitSet', limit: 100, repoToken: '' }, 'an empty token');
  await refused({ op: 'sandboxPullLimitSet', limit: 100, repoToken: 'not-a-token' }, 'a made-up token');
  await refused({ op: 'sandboxPullLimitSet', limit: 100, repoToken: 'a'.repeat(64) + '.' + 'b'.repeat(32) }, 'a well-shaped forged token (no matching record)');
  await refused({ op: 'sandboxPullLimitSet', limit: 100, repoToken: await tokenFor('download') }, 'a download token');
  await refused({ op: 'sandboxPullLimitSet', limit: 100, repoToken: await tokenFor('delete') }, 'a delete token');
  await refused({ op: 'sandboxPullLimitSet', limit: 100, repoToken: await tokenFor('history') }, 'a history token');
  { const t = await tokenFor('modify'); await refused({ op: 'sandboxPullLimitSet', limit: 100, repoToken: t }, 'a token in a GET (a token never travels in a URL)', 'GET'); }
  { const t = await tokenFor('modify'); RA.deps.now = () => Date.now() + 20 * 60000; await refused({ op: 'sandboxPullLimitSet', limit: 100, repoToken: t }, 'an expired token'); RA.deps.now = () => Date.now(); RA.forget(); }
  assert.strictEqual(JSON.stringify([...store.entries()].filter(([k]) => !k.startsWith('CharmRepo_Tokens/')).sort()), before, 'every refusal left the database as it was');
  assert.strictEqual((await limitOf()).limit, 250, 'still 250');
  // a sandbox-flagged and a production-flagged call are both behind the same door
  { const out = await post({ op: 'sandboxPullLimitSet', limit: 100, sandbox: true }); assert.strictEqual(out.status, 401, 'the sandbox flag does not open it'); }

  // the right token: what the dialog gets for "modify" (an upload token and an "other" token open it too, as the lock's own table says)
  r = await setOp(120);
  assert(r.status === 200 && r.body.ok && r.body.limit === 120 && r.body.clamped === false && r.body.note === '' && r.body.isSet === true && r.body.callsPerPull === 2 && r.body.pullsPerDay === null, 'a modify token writes: ' + JSON.stringify(r.body));
  assert.strictEqual(store.get(CFG).limit, 120); assert.strictEqual(store.get(CFG).v, 1); assert.strictEqual(store.get(CFG).by, 'Paul');
  assert.deepStrictEqual(Object.keys(store.get(CFG)).sort(), ['at', 'by', 'limit', 'updatedAt', 'v'], 'one small flat document');
  assert.strictEqual((await setOp(130, 'upload')).status, 200, 'an upload token opens it'); assert.strictEqual((await setOp(140, 'other')).status, 200, 'an "other" token opens it');
  assert.strictEqual(store.get(CFG).limit, 140);
  // the same token again is fine for a reusable kind (the dialog keeps it for a few minutes)
  { const t = await tokenFor('modify'); assert.strictEqual((await post({ op: 'sandboxPullLimitSet', limit: 150, repoToken: t })).status, 200); assert.strictEqual((await post({ op: 'sandboxPullLimitSet', limit: 160, repoToken: t })).status, 200); }
  assert.strictEqual(store.get(CFG).limit, 160);

  // the server holds the number to the limits and says so in one plain line
  r = await setOp(5000); assert(r.status === 200 && r.body.limit === 500 && r.body.clamped === true && r.body.note === 'Held to 500: the most one pull may bring (5 Etsy calls).' && r.body.callsPerPull === 5 && r.body.pullsPerDay === null, 'over: ' + JSON.stringify(r.body));
  assert.strictEqual(store.get(CFG).limit, 500);
  r = await setOp(3); assert(r.status === 200 && r.body.limit === 10 && r.body.clamped === true && r.body.note === 'Held to 10: the fewest one pull may bring.' && r.body.callsPerPull === 1, 'under: ' + JSON.stringify(r.body));
  assert.strictEqual(store.get(CFG).limit, 10);
  r = await setOp('77'); assert(r.status === 200 && r.body.limit === 77 && r.body.clamped === false, 'a numeric string is taken');
  for (const bad of ['abc', '', null, undefined, {}, true]) { const out = await setOp(bad); assert(out.status === 400 && out.body.reason === 'bad' && /whole number from 10 to 500/.test(out.body.error), 'a non-number is refused: ' + JSON.stringify(bad) + ' ' + JSON.stringify(out.body)); assert.strictEqual(store.get(CFG).limit, 77, 'and nothing changed'); }
  // a value written by hand outside the limits, or not a number, still cannot break them
  store.set(CFG, { v: 1, limit: 100000 }); lim = await limitOf(); assert(lim.limit === 500 && lim.isSet === true, 'hand-written 100000 is read as 500');
  store.set(CFG, { v: 1, limit: -3 }); assert.strictEqual((await limitOf()).limit, 10);
  store.set(CFG, { v: 1, limit: 'many' }); lim = await limitOf(); assert(lim.limit === 250 && lim.isSet === false, 'a stored non-number is no setting: 250');
  store.set(CFG, { v: 1, limit: 90, by: 'Paul', at: 1760000000000 });

  /* ═══ 4 · a pull uses the number ═══ */
  const pulls = [];   // [limit, startId] → the answer
  const run = async (limit, id) => {
    store.set(CFG, { v: 1, limit }); etsy.calls.length = 0;
    const got = await pullOp(id); pulls.push([limit, got]); return got;
  };
  r = await run(120, 'start-limit-120a'); assert(r.status === 200 && r.body.count === 120 && r.body.want === 120 && r.body.calls === 2, '120 = two calls: ' + JSON.stringify(r.body).slice(0, 160));
  assert.deepStrictEqual(offsets(), [[0, 100], [100, 20]]); assert.deepStrictEqual(JSON.parse(blobs.get(r.body.path).buf.toString('utf8')).receipts.map(x => x.receipt_id), NEWEST(120).map(x => x.receipt_id), 'the 120 newest, newest first');
  assert(store.get('Charm_Sandbox/current').count === 120 && store.get('Charm_Sandbox/current').want === 120 && r.body.open < 120 && r.body.stream && r.body.stream.total === r.body.open, 'the stream plays the open ones of those 120');
  r = await run(30, 'start-limit-30aa'); assert(r.status === 200 && r.body.count === 30 && r.body.calls === 1, '30 = one call'); assert.deepStrictEqual(offsets(), [[0, 30]]);
  assert.strictEqual(JSON.parse(blobs.get(r.body.path).buf.toString('utf8')).receipts.length, 30);
  // same start again: the stored set, no call, whatever the number says now
  store.set(CFG, { v: 1, limit: 400 }); etsy.calls.length = 0; r = await pullOp('start-limit-30aa'); assert(r.status === 200 && r.body.already === true && r.body.count === 30 && r.body.want === 30 && etsy.calls.length === 0, 'a retry of the same start is the stored set: ' + JSON.stringify(r.body).slice(0, 120));
  // 500 = five calls, and the daily budget counts them
  store.set('Charm_Sandbox/pulls', { day: todayKey(), pulls: 0, calls: 0, tokenRefreshes: 0, starts: [], claim: null });
  r = await run(500, 'start-limit-500a'); assert(r.status === 200 && r.body.count === 500 && r.body.want === 500 && r.body.calls === 5 && r.body.callsToday === 5 && r.body.pullsToday === 1, '500 = five calls: ' + JSON.stringify(r.body).slice(0, 200));
  assert.deepStrictEqual(offsets().sort((a, b) => a[0] - b[0]), [[0, 100], [100, 100], [200, 100], [300, 100], [400, 100]]); assert.strictEqual(ledger().calls, 5);
  assert.deepStrictEqual(JSON.parse(blobs.get(r.body.path).buf.toString('utf8')).receipts.map(x => x.receipt_id), NEWEST(500).map(x => x.receipt_id), 'the 500 newest');
  // a missing document is 250 exactly as before
  store.delete(CFG); etsy.calls.length = 0; r = await pullOp('start-limit-none1'); assert(r.status === 200 && r.body.count === 250 && r.body.calls === 3, 'no setting = 250'); assert.deepStrictEqual(offsets(), [[0, 100], [100, 100], [200, 50]]);
  // a shop with fewer orders than asked brings what it has
  { const keep = SHOP.splice(60); store.set(CFG, { v: 1, limit: 200 }); etsy.calls.length = 0; r = await pullOp('start-limit-short1'); SHOP.push(...keep);
    assert(r.status === 200 && r.body.count === 60 && r.body.want === 200 && r.body.calls === 2, 'a small shop: ' + JSON.stringify(r.body).slice(0, 160)); assert.deepStrictEqual(offsets(), [[0, 100], [100, 100]], 'every page is asked at once (SBXPULL): the second one comes back empty'); }

  /* ── NOLIMIT (13 Oct 2026): no daily cap. The old gate refused a 500 pull at 16 of 20 calls and any pull at 6 of 6; neither is a reason to stop now, and the day's ledger only counts ── */
  store.set(CFG, { v: 1, limit: 500 });
  store.set('Charm_Sandbox/pulls', { day: todayKey(), pulls: 3, calls: 16, tokenRefreshes: 0, starts: [], claim: null });   // (the old gate: 16 of 20 calls used, 500 needs 5)
  etsy.calls.length = 0; r = await pullOp('start-cap-calls01');
  assert(r.status === 200 && r.body.ok && r.body.count === 500 && r.body.calls === 5 && r.body.callsToday === 21 && r.body.pullsToday === 4 && etsy.calls.length === 5 && !empty(), 'a 500 pull at 16 calls used goes through: ' + JSON.stringify(r.body).slice(0, 200));
  assert.strictEqual(ledger().pulls, 4, 'and is counted'); assert.strictEqual(ledger().calls, 21);
  store.set(CFG, { v: 1, limit: 10 }); store.set('Charm_Sandbox/pulls', { day: todayKey(), pulls: 6, calls: 18, tokenRefreshes: 0, starts: [], claim: null });   // (the old gate: 6 of 6 pulls)
  etsy.calls.length = 0; r = await pullOp('start-cap-pulls01');
  assert(r.status === 200 && r.body.ok && r.body.count === 10 && r.body.pullsToday === 7 && r.body.callsToday === 19 && etsy.calls.length === 1 && !empty(), 'the seventh pull of the day goes through, whatever the number: ' + JSON.stringify(r.body).slice(0, 200));
  // the answers and the status carry no cap at all (null), only the counts
  for (const x of [r.body, (await post({ op: 'sandboxStatus', sandbox: true })).body.budget]) assert(x.cap === null && x.capLeft === null && x.callsCap === null && x.pullsToday === 7 && x.callsToday === 19, 'no cap in the budget: ' + JSON.stringify(x));
  // a pull of 500 orders, ten times in one day: every one goes through (the old gate refused the fifth, at 20 calls), each is 5 Etsy calls, and the ledger counts them all
  store.set(CFG, { v: 1, limit: 500 }); store.set('Charm_Sandbox/pulls', { day: todayKey(), pulls: 0, calls: 0, tokenRefreshes: 0, starts: [], claim: null }); etsy.calls.length = 0;
  for (let i = 1; i <= 10; i++) {
    r = await pullOp('start-500x10-' + String(i).padStart(2, '0'));
    assert(r.status === 200 && r.body.ok && r.body.reason === undefined && r.body.count === 500 && r.body.calls === 5 && r.body.pullsToday === i && r.body.callsToday === 5 * i, `pull ${i} of 500 orders is not refused: ` + JSON.stringify(r.body).slice(0, 200));
    assert(!empty() && r.body.empty !== true, `pull ${i}: the sandbox holds its 500`);
  }
  assert.strictEqual(etsy.calls.length, 50, 'ten pulls, five Etsy calls each, none held back'); assert.strictEqual(ledger().pulls, 10); assert.strictEqual(ledger().calls, 50);
  assert.deepStrictEqual(JSON.parse(blobs.get(store.get('Charm_Sandbox/current').path).buf.toString('utf8')).receipts.map(x => x.receipt_id), NEWEST(500).map(x => x.receipt_id), 'and the last one holds the 500 newest');

  /* ═══ 5 · the number is a setting: the wipe leaves it, and the sandbox stays empty after a reset until a pull is asked for ═══ */
  store.set('Charm_Sandbox/pulls', { day: todayKey(), pulls: 0, calls: 0, tokenRefreshes: 0, starts: [], claim: null });
  r = await setOp(180); assert.strictEqual(r.status, 200);
  const cfgBefore = Fk.canon(store.get(CFG));
  r = await pullOp('start-wipe-00001'); assert(r.status === 200 && r.body.count === 180 && r.body.calls === 2);
  store.set('Sandbox_Charm_Pool/p1_1', { poolId: 'p1_1', orderId: 'p1', state: 'pooled' }); store.set('Sandbox_Charm_Nest_Sheets/s1', { id: 's1', orders: ['p1'] }); store.set('Sandbox_Charm_Custom_Orders/p1_1', { key: 'p1_1', receiptId: 'p1' });
  assert(store.has('Charm_Sandbox/current') && store.has('Charm_Sandbox/stream') && [...blobs.keys()].some(k => k.startsWith('charmnest/sandbox/orders-pull/')), 'the sandbox holds a pulled set and its stream');
  assert(Families.isProtected('firestore', CFG) && !Families.server().some(f => f.store === 'doc' && f.collection === 'config' && (f.docs || []).includes('charmNestSandboxPull')), 'the registry keeps the number and clears no config document of that name');
  const callsBefore = etsy.calls.length, wr = writes.length;
  let w = await post(await repoStamp('charmNestLibrary', { op: 'sandboxReset', sandbox: true })), n = 1; while (w.body.more && n < 50) { w = await post(await repoStamp('charmNestLibrary', { op: 'sandboxReset', sandbox: true })); n++; }
  assert(w.status === 200 && w.body.ok && w.body.more === false, 'the wipe ran to its end: ' + JSON.stringify(w.body));
  assert(empty() && ![...store.keys()].some(k => k.startsWith('Sandbox_')), 'nothing of the sandbox is left');
  assert(store.has(CFG) && Fk.canon(store.get(CFG)) === cfgBefore, "the wipe left Paul's number byte-identical");
  assert(!writes.slice(wr).some(x => x.path === CFG), 'and never wrote or deleted it');
  assert(store.has('Charm_Sandbox/pulls'), 'the pull budget stays as well');
  lim = await limitOf(); assert(lim.limit === 180 && lim.isSet === true, "the status still reads Paul's number after the wipe: " + JSON.stringify(lim));
  assert.strictEqual(etsy.calls.length, callsBefore, 'the wipe made no Etsy call'); assert(empty(), 'and brought no orders back: the sandbox waits for Start');
  assert.strictEqual((await post({ op: 'sandboxStatus', sandbox: true, light: true })).body.snapshot, null, 'no order set after the reset');
  // Start after the reset: the pull brings the number Paul set
  etsy.calls.length = 0; r = await pullOp('start-wipe-00002'); assert(r.status === 200 && r.body.count === 180 && r.body.want === 180 && r.body.calls === 2, 'the pull after the reset brings 180: ' + JSON.stringify(r.body).slice(0, 160));

  const leaked = JSON.stringify([...store.entries()].filter(([k]) => k.startsWith('config/') || k.startsWith('Charm_Sandbox/')));
  assert(!/repoToken|password|tok-fresh/.test(JSON.stringify(store.get(CFG))), 'no token or secret in the setting');
  assert(!leaked.includes('test-secret'), 'no Etsy secret stored');
  console.warn = realWarn; Module._load = realLoad;
  console.log(`sandbox pull limit OK: default 250 (3 calls), held to ${Pull.MIN_WANT}-${Pull.MAX_WANT} (up to ${Pull.pagesFor(Pull.MAX_WANT)} calls), write needs the repo token, survives the wipe, ${pulls.length + 4} pulls checked`);
})().catch(e => { console.warn = realWarn; console.error(e); process.exit(1); });
