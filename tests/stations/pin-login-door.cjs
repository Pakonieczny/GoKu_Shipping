// The Employee Number login door (firebaseOrders {pinLogin} + _stationPinLogin.js): the server looks the typed number up and
// answers with the name only; the roster (Brites_Orders/"Employee Numbers", one field per person: number -> name) never leaves.
// Fakes only: Firestore is a Map with transactions; the clock is faked and the failure delay is recorded, not waited for.
// Every PIN here is made up when this test runs and is never written anywhere or put in a message: the checks assert
// booleans only, so a failure cannot print one.
//   1 · a known number answers { ok, name } and nothing else; the name is as stored (case, accents, punctuation) with the
//       spaces tidied; no other number or name comes back, and the number is not echoed
//   2 · an unknown number answers { ok:false } after a delay, the same as a record whose value is not a usable name
//   3 · malformed input (not six digits, not text, a huge body, a body that is not JSON) is refused with 400/413 before
//       the roster is read, and a number in a URL (GET) is never looked up
//   4 · guessing: 10 wrong tries from one address start a one-minute lockout (a right number is refused too), another address
//       is unaffected, and the address works again after the minute; 40 requests a minute from one address do the same;
//       a burst sent at once cannot slip past a lockout that starts while it is in flight
//   5 · everybody together: 30 wrong tries in a minute lock all addresses, across instances (the count is in Firestore) and
//       recover after the minute; a right number never adds to a count
//   6 · fails open: a Firestore failure around the counters never keeps a right number out; a missing roster or a failed
//       read is an error WITHOUT `ok` (so a page may fall back); the sandbox flag does not give a fresh counter
//   7 · no PIN in any response, any log line, or any stored document (the counter holds a window, a count, a lockout time)
//   NODE_PATH=… node tests/stations/pin-login-door.cjs
'use strict';
const path = require('path'), assert = require('assert'), Module = require('module');
const root = path.join(__dirname, '../..');

/* ── the fake Firestore ── */
const docs = new Map(), reads = new Map();
let failLimiter = false, failRoster = false;
const clone = v => (v === undefined ? v : JSON.parse(JSON.stringify(v)));
const ref = p => ({ path: p, id: p.split('/').pop(), get: async () => snap(p) });
const snap = p => {
  reads.set(p, (reads.get(p) || 0) + 1);
  if (failLimiter && p.startsWith('Station_PinLogin/')) throw new Error('limiter store is down');
  if (failRoster && p.startsWith('Brites_Orders/Employee')) throw new Error('roster store is down');
  return { exists: docs.has(p), data: () => clone(docs.get(p)) };
};
const fakeDb = {
  collection: c => ({ doc: id => ref(c + '/' + id) }),
  runTransaction: async fn => {
    const w = [];
    const out = await fn({ get: r => r.get(), set: (r, d) => { if (failLimiter && r.path.startsWith('Station_PinLogin/')) throw new Error('limiter store is down'); w.push([r.path, d]); } });
    for (const [p, d] of w) docs.set(p, clone(d));
    return out;
  }
};
const fakeAdmin = { firestore: Object.assign(() => fakeDb, { FieldValue: { serverTimestamp: () => 'ts', delete: () => null } }) };
const realLoad = Module._load;
Module._load = function (req, ...rest) { if (/[\/]firebaseAdmin(\.js)?$/.test(req)) return fakeAdmin; return realLoad.call(this, req, ...rest); };
const door = require(path.join(root, 'netlify/functions/firebaseOrders.js'));
const P = require(path.join(root, 'netlify/functions/_stationPinLogin.js'));
Module._load = realLoad;

/* ── what was printed: every console line is kept so the PINs can be searched for ── */
const logs = [];
for (const k of ['log', 'info', 'warn', 'error', 'debug']) console[k] = (...a) => { logs.push(a.map(x => (x && x.stack) || (typeof x === 'string' ? x : JSON.stringify(x))).join(' ')); };
const say = (...a) => process.stdout.write(a.join(' ') + '\n');

/* ── made-up people ── */
const used = new Set();
const fakePin = () => { for (;;) { const p = String(100000 + Math.floor(Math.random() * 900000)); if (!/^(\d)\1{5}$/.test(p) && !used.has(p)) { used.add(p); return p; } } };
const PEOPLE = [['Giovanna', fakePin()], ['Anna', fakePin()], ['Michael', fakePin()], ['Ivy', fakePin()],
  ['  Zoë   O’Neil-Ñandú \n', fakePin()], ['giovanna c.', fakePin()]];
const NAME_OF = Object.fromEntries(PEOPLE.map(([n, p]) => [p, n]));
const [GIO, ANNA, MICH, IVY, ODD, LOWER] = PEOPLE.map(x => x[1]);
const BROKEN = { digits: fakePin(), empty: fakePin(), blank: fakePin(), num: fakePin(), obj: fakePin() };
const NOBODY = fakePin();
const seesPin = text => [...used].some(p => String(text).includes(p));
const resetRoster = () => {
  const roster = Object.fromEntries(PEOPLE.map(([n, p]) => [p, n]));
  roster[BROKEN.digits] = '123456'; roster[BROKEN.empty] = ''; roster[BROKEN.blank] = '   '; roster[BROKEN.num] = 42; roster[BROKEN.obj] = { n: 'x' };
  docs.set('Brites_Orders/Employee Numbers', roster);
};

/* ── the clock and the waiting ── */
let now = Date.parse('2026-10-03T12:00:00Z');
Date.now = () => now;
const slept = [];
P.deps.sleep = async ms => { slept.push(ms); now += ms; };
P.deps.rand = () => 0.5;
const advance = ms => { now += ms; };

const bodies = [];                                                  // every response body ever returned
let ipN = 0;
const fresh = () => '198.51.100.' + (++ipN);
const call = async (pinLogin, { ip = fresh(), query, raw, method = 'POST', headers } = {}) => {
  const event = { httpMethod: method, headers: Object.assign({ 'x-nf-client-connection-ip': ip }, headers || {}), queryStringParameters: query || {},
    body: method === 'POST' ? (raw != null ? raw : JSON.stringify({ pinLogin })) : null };
  const r = await door.handler(event);
  bodies.push(r.body);
  let b = {}; try { b = JSON.parse(r.body); } catch (_) {}
  return { status: r.statusCode, body: b, raw: r.body, headers: r.headers || {} };
};
const rosterReads = () => reads.get('Brites_Orders/Employee Numbers') || 0;
const reset = () => { docs.clear(); reads.clear(); slept.length = 0; P.reset(); failLimiter = false; failRoster = false; resetRoster(); now += 3600e3; };
const results = [];
const check = async (name, fn) => { try { await fn(); results.push([name, true]); say('  ok   ' + name); } catch (e) { results.push([name, false]); say('  FAIL ' + name + ': ' + String(e && e.message).replace(/\d{6}/g, '######')); } };

(async () => {
  /* 1 · a known number */
  await check('1 a known number answers { ok, name } and nothing else; the name as stored, spaces tidied', async () => {
    reset();
    const r = await call(GIO);
    assert.strictEqual(r.status, 200); assert.deepStrictEqual(Object.keys(r.body).sort(), ['name', 'ok']);
    assert.strictEqual(r.body.ok, true); assert.strictEqual(r.body.name, 'Giovanna');
    assert.strictEqual((await call(ODD)).body.name, 'Zoë O’Neil-Ñandú', 'case, accents and punctuation stay; spaces and line ends are tidied');
    assert.strictEqual((await call(LOWER)).body.name, 'giovanna c.', 'a lower-case name stays lower case');
    assert.strictEqual((await call(ANNA)).body.name, 'Anna');
    assert(!seesPin(r.raw), 'the response echoes a number');
    for (const [n, p] of PEOPLE) {                                                 // each number gives its own person, and only that person
      const x = await call(p);
      assert.strictEqual(x.body.name, n.replace(/\s+/g, ' ').trim()); assert.deepStrictEqual(Object.keys(x.body).sort(), ['name', 'ok']);
    }
    assert.strictEqual(slept.length, 0, 'a right number is answered at once');
    assert.strictEqual(docs.has('Station_PinLogin/limits'), false, 'a right number adds to no count');
    assert.strictEqual(r.headers['Cache-Control'], 'no-store');
  });

  /* 2 · an unknown number */
  await check('2 an unknown number is { ok:false } after a delay; a broken record looks the same', async () => {
    reset();
    const a = await call(NOBODY);
    assert.strictEqual(a.status, 200); assert.deepStrictEqual(Object.keys(a.body).sort(), ['error', 'ok']); assert.strictEqual(a.body.ok, false);
    assert(!seesPin(a.raw), 'the refusal echoes a number');
    assert(slept.length === 1 && slept[0] >= 400, 'a wrong try waits about 0.4 s: ' + slept.join());
    for (const k of Object.keys(BROKEN)) {
      const b = await call(BROKEN[k]);
      assert.deepStrictEqual(b.body, a.body, 'a record with a ' + k + ' value is answered like an unknown number');
    }
    assert(!JSON.stringify(a.body).includes('Giovanna'), 'no name leaks on a refusal');
  });

  /* 3 · malformed input */
  await check('3 malformed input is refused before the roster is read; a number in a URL is never looked up', async () => {
    reset();
    const bad = ['', '12345', '1234567', 'abcdef', '12 456', ' ' + GIO.slice(1), GIO + ' ', GIO.slice(0, 5) + 'x', '١٢٣٤٥٦', 123456, Number(GIO), null, true, [], [GIO], { n: GIO }];
    for (const v of bad) {
      const r = await call(v);
      assert.strictEqual(r.status, 400, 'a malformed value gives 400'); assert.strictEqual(r.body.ok, false);
      assert(!seesPin(r.raw));
    }
    assert.strictEqual(rosterReads(), 0, 'the roster was never read for them');
    assert.strictEqual(slept.length, 0);
    const big = await call(GIO, { raw: JSON.stringify({ pinLogin: GIO, pad: 'x'.repeat(2000) }) });
    assert.strictEqual(big.status, 413); assert.strictEqual(big.body.ok, false);
    const before = logs.length;
    const junk = await call(null, { raw: '{"pinLogin": "' + GIO + '", oops' });
    assert.strictEqual(junk.status, 400); assert.strictEqual(junk.body.ok, false);
    assert(!logs.slice(before).some(seesPin), 'a body that is not JSON puts nothing from it in the log');
    // a number sent in a URL is not a login: the query is ignored, nothing is looked up, no name is returned
    const get = await call(null, { method: 'GET', query: { pinLogin: GIO } });
    assert(!get.raw.includes('Giovanna') && !seesPin(get.raw), 'a GET never answers a login');
    assert.strictEqual(rosterReads(), 0);
    // an unknown field next to others is not a login: pinLogin must be present
    const other = await call(null, { raw: JSON.stringify({ pin: GIO }) });
    assert(!other.raw.includes('Giovanna') && other.body.ok === undefined, 'only { pinLogin } is a login');
  });

  /* 4 · one address guessing */
  await check('4 ten wrong tries from one address lock it for a minute; others are unaffected; it recovers', async () => {
    reset();
    const A = '203.0.113.7', B = '203.0.113.8';
    for (let i = 1; i <= 10; i++) { const r = await call(fakePin(), { ip: A }); assert.strictEqual(r.status, 200); assert.strictEqual(r.body.ok, false, 'try ' + i + ' is answered no'); }
    const locked = await call(GIO, { ip: A });
    assert.strictEqual(locked.status, 429); assert.deepStrictEqual(locked.body, { ok: false, tooMany: true, error: 'Too many tries, wait a minute' });
    assert(Number(locked.headers['Retry-After']) >= 1 && Number(locked.headers['Retry-After']) <= 60);
    assert(!seesPin(locked.raw));
    assert.strictEqual((await call(NOBODY, { ip: A })).status, 429, 'a wrong number is refused too');
    assert.strictEqual((await call(GIO, { ip: B })).body.name, 'Giovanna', 'another address still signs in');
    advance(30000);
    assert.strictEqual((await call(GIO, { ip: A })).status, 429, 'still locked half way');
    advance(31000);
    const back = await call(GIO, { ip: A });
    assert.strictEqual(back.status, 200); assert.strictEqual(back.body.name, 'Giovanna', 'the address works again after the minute');
    for (let i = 1; i <= 9; i++) assert.strictEqual((await call(fakePin(), { ip: A })).status, 200);
    assert.strictEqual((await call(GIO, { ip: A })).status, 200, 'the count started clean: nine wrong tries do not lock');
    // a quiet address starts clean: 9 wrong tries, a minute, 9 more
    P.reset(); const C = '203.0.113.9';
    for (let i = 0; i < 9; i++) await call(fakePin(), { ip: C });
    advance(61000);
    for (let i = 0; i < 9; i++) assert.strictEqual((await call(fakePin(), { ip: C })).status, 200);
    assert.strictEqual((await call(GIO, { ip: C })).status, 200, 'wrong tries a minute apart never add up');
  });
  await check('4b forty requests a minute from one address lock it (right numbers too), and it recovers', async () => {
    reset(); const A = '203.0.113.20';
    for (let i = 1; i <= 40; i++) assert.strictEqual((await call(GIO, { ip: A })).status, 200, 'request ' + i);
    assert.strictEqual((await call(GIO, { ip: A })).status, 429);
    advance(61000);
    assert.strictEqual((await call(GIO, { ip: A })).status, 200);
  });
  await check('4c thirty right numbers a minute (a shop signing in) are never locked or counted', async () => {
    reset(); const A = '203.0.113.21';
    for (let i = 1; i <= 30; i++) assert.strictEqual((await call([GIO, ANNA, MICH, IVY][i % 4], { ip: A })).status, 200);
    assert.strictEqual(docs.has('Station_PinLogin/limits'), false);
  });

  await check('4d a burst cannot slip past a lockout that starts while it is in flight (right numbers included)', async () => {
    reset(); const A = '203.0.113.22';
    const burst = await Promise.all(Array.from({ length: 30 }, (_, i) => call(i % 5 === 4 ? GIO : fakePin(), { ip: A })));
    const answered = burst.filter(r => r.status === 200 && r.body.ok === false).length;
    assert(answered <= 10, 'at most ten wrong tries of one burst are answered no: ' + answered);
    assert(burst.every(r => r.status === 200 || r.status === 429), 'the rest are told to wait');
    assert(burst.filter(r => r.status === 429).length >= 15, 'most of the burst is told to wait');
    assert(!burst.some(r => seesPin(r.raw)));
    advance(61000);
    assert.strictEqual((await call(GIO, { ip: A })).body.name, 'Giovanna', 'the address works again after the minute');
  });

  /* 5 · everybody together */
  await check('5 thirty wrong tries in a minute lock everybody, across instances, and recover', async () => {
    reset();
    for (let i = 1; i <= 30; i++) { const r = await call(fakePin(), { ip: fresh() }); assert.strictEqual(r.status, 200, 'wrong try ' + i + ' is answered'); }
    const lim = docs.get('Station_PinLogin/limits');
    assert(lim && lim.fails === 30 && lim.lockedUntil > now, 'the shared counter shows the lockout');
    assert.deepStrictEqual(Object.keys(lim).sort(), ['fails', 'lockedUntil', 'windowStart'], 'the counter holds a window, a count and a time: nothing else');
    assert(!seesPin(JSON.stringify(lim)));
    const a = await call(GIO, { ip: fresh() });
    assert.strictEqual(a.status, 429); assert.strictEqual(a.body.error, 'Too many tries, wait a minute');
    P.reset();                                                                     // another instance (cold): the shared count still holds
    assert.strictEqual((await call(GIO, { ip: fresh() })).status, 429, 'a cold instance is locked too');
    advance(62000);
    P.reset();
    const ok = await call(GIO, { ip: fresh() });
    assert.strictEqual(ok.status, 200); assert.strictEqual(ok.body.name, 'Giovanna', 'everybody can sign in again after the minute');
    for (let i = 0; i < 5; i++) assert.strictEqual((await call(fakePin(), { ip: fresh() })).status, 200);
    assert.strictEqual(docs.get('Station_PinLogin/limits').fails, 5, 'the count began again in a new window');
    assert.strictEqual((await call(GIO, { ip: fresh() })).status, 200);
  });
  await check('5b this instance alone also locks at thirty wrong tries when the shared count cannot be read', async () => {
    reset(); failLimiter = true;
    for (let i = 1; i <= 30; i++) assert.strictEqual((await call(fakePin(), { ip: fresh() })).status, 200);
    assert.strictEqual((await call(GIO, { ip: fresh() })).status, 429, 'the in-memory count still protects');
    advance(62000);
    assert.strictEqual((await call(GIO, { ip: fresh() })).status, 200);
  });

  /* 6 · fails open, errors, sandbox */
  await check('6 a failing counter store never keeps a right number out; wrong numbers are still answered', async () => {
    reset(); failLimiter = true;
    const r = await call(GIO); assert.strictEqual(r.status, 200); assert.strictEqual(r.body.name, 'Giovanna');
    const w = await call(NOBODY); assert.strictEqual(w.status, 200); assert.strictEqual(w.body.ok, false);
    assert(!logs.some(l => /Giovanna/.test(l) && seesPin(l)));
  });
  await check('6b a missing or unreadable roster is an error WITHOUT ok (a page may fall back), never a refusal', async () => {
    reset(); docs.delete('Brites_Orders/Employee Numbers');
    const m = await call(GIO); assert.strictEqual(m.status, 503); assert.strictEqual(m.body.ok, undefined); assert(!seesPin(m.raw));
    resetRoster(); failRoster = true;
    const e = await call(GIO); assert.strictEqual(e.status, 500); assert.strictEqual(e.body.ok, undefined); assert(!seesPin(e.raw), 'the error text carries no number');
  });
  await check('6c the sandbox flag reads the real roster and shares the counters', async () => {
    reset(); const A = '203.0.113.30';
    const s = await call(GIO, { ip: A, query: { sandbox: '1' } }); assert.strictEqual(s.body.name, 'Giovanna');
    for (let i = 0; i < 10; i++) await call(fakePin(), { ip: A, query: { sandbox: i % 2 ? '1' : undefined } });
    assert.strictEqual((await call(GIO, { ip: A })).status, 429, 'switching the flag does not give a fresh count');
    assert.strictEqual(reads.get('Sandbox_Brites_Orders/Employee Numbers') || 0, 0);
  });

  /* 7 · no PIN anywhere */
  await check('7 no PIN in any response, log line or stored document', async () => {
    const stored = JSON.stringify([...docs].filter(([k]) => !k.startsWith('Brites_Orders/')));
    assert(!seesPin(stored), 'a stored document holds a number');
    assert(!bodies.some(b => seesPin(b)), 'a response held a number');
    assert(!logs.some(l => seesPin(l)), 'a log line held a number');
    assert(logs.length > 0, 'the lockouts were logged (without a number)');
  });

  const failed = results.filter(r => !r[1]).length;
  say(failed ? failed + ' check(s) FAILED' : 'pin-login door: all ' + results.length + ' checks passed');
  process.exit(failed ? 1 : 0);
})().catch(e => { say('ERROR ' + String(e && e.stack || e).replace(/\d{6}/g, '######')); process.exit(1); });
