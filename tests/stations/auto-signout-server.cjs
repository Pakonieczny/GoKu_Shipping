// The server's half of the automatic sign-out and the Admin list (plans/stations-round2/api.md "AD2"):
//   netlify/functions/_stationAdmins.js, _stationAutoSignout.js, stationSessionsSweepCron.js, the {stationAdmin} and {session} doors of
//   firebaseOrders.js, and the readers that apply the rules (the live board, the person page's attendance).
// Everything offline: Firestore is an in-memory fake (typed fields, where/orderBy/limit, transactions, counted writes), the clock is faked,
// every person is invented and no PIN is used anywhere.
//   1 · the 17:00 Toronto instant, with daylight saving (fall back 1 Nov 2026, spring forward 14 Mar 2027)
//   2 · the Admin list: fallback when the document is missing, name matching, the 60 s cache, a failed read, the door (one name, { ok, admin },
//       never the list, rate limited with counters of its own, 503 without `ok` when it cannot be read)
//   3 · the session door: lastInputAt kept (server clock, never backwards), a beat that reports 10 minutes without input ends the session at the
//       last input ("idle"), an idle or closing end keeps the last input (not raised to the last beat), a wrong computer clock is undone, a dead
//       page is ended at its last input (non-Admin) or at its last beat (Admin, or a page that never reported input), Admin exempt, sandbox apart
//   4 · the readers and the sweep: idle end time = last input, a dead page, a fresh beat is never ended for input it has not reported yet,
//       Admin exempt, 17:00 with and without recent input, daylight saving days, idempotent (one write per session, an ended session never
//       rewritten, nothing deleted), two people at one station, real and Sandbox_ never mixed, an unreadable list leaves sessions alone
//   5 · the attendance: the day ends at the last input, `endedBy` says idle or closing, nothing is estimated
//   node tests/stations/auto-signout-server.cjs
'use strict';
const path = require('path'), assert = require('assert'), Module = require('module');
const root = path.join(__dirname, '../..');

/* ── fake Firestore ── */
class Ts { constructor(m) { this.m = m; } toMillis() { return this.m; } static fromMillis(m) { return new Ts(m); } }
const val = v => v instanceof Ts ? v.m : v;
const kind = v => v instanceof Ts ? 'ts' : v === null ? 'null' : typeof v;
const clone = v => v instanceof Ts ? v : Array.isArray(v) ? v.map(clone) : v && typeof v === 'object' ? Object.fromEntries(Object.entries(v).map(([k, x]) => [k, clone(x)])) : v;
function store() {
  const colls = new Map(), reads = [], writes = [], failing = new Set();
  const data = n => { if (!colls.has(n)) colls.set(n, new Map()); return colls.get(n); };
  const down = n => { if (failing.has(n)) throw Object.assign(new Error('14 UNAVAILABLE: synthetic outage of ' + n), { code: 14 }); };
  function query(name, filters, order, lim) {
    return {
      where: (f, op, v) => query(name, filters.concat([[f, op, v]]), order, lim),
      orderBy: (f, d) => query(name, filters, [f, d || 'asc'], lim),
      limit: n => query(name, filters, order, n),
      get: async () => {
        down(name);
        let docs = [...data(name)].map(([id, d]) => ({ id, d }));
        for (const [f, op, v] of filters) docs = docs.filter(({ d }) => {
          const x = d[f]; if (x === undefined || kind(x) !== kind(v)) return false;
          const a = val(x), b = val(v);
          return op === '==' ? a === b : op === '>=' ? a >= b : op === '>' ? a > b : op === '<' ? a < b : op === '<=' ? a <= b : false;
        });
        if (order) { const [f, dir] = order; docs = docs.filter(({ d }) => d[f] !== undefined).sort((p, q) => (val(p.d[f]) < val(q.d[f]) ? -1 : val(p.d[f]) > val(q.d[f]) ? 1 : 0) * (dir === 'desc' ? -1 : 1)); }
        if (lim != null) docs = docs.slice(0, lim);
        reads.push({ name, n: docs.length, filters: filters.map(f => f[0] + f[1]) });
        return { docs: docs.map(({ id, d }) => ({ id, data: () => clone(d) })), size: docs.length, empty: !docs.length };
      }
    };
  }
  const ref = (name, id) => ({ id, name,
    get: async () => { down(name); reads.push({ name, doc: id }); const d = data(name).get(id); return { exists: !!d, id, data: () => clone(d) }; },
    set: async (v, o) => { writes.push({ name, id, how: o && o.merge ? 'merge' : 'set', keys: Object.keys(v) }); data(name).set(id, o && o.merge ? Object.assign({}, data(name).get(id) || {}, clone(v)) : clone(v)); } });
  const db = {
    collection: name => Object.assign(query(name, [], null, null), { doc: id => ref(name, id) }),
    runTransaction: async fn => fn({ get: r => r.get(), set: (r, v, o) => r.set(v, o) })
  };
  return { db, put: (name, id, d) => data(name).set(id, clone(d)), get: (name, id) => data(name).get(id), all: name => [...data(name)].map(([id, d]) => Object.assign({ _id: id }, d)),
    reads, writes, writesTo: (name, id) => writes.filter(w => w.name === name && (id == null || w.id === id)), count: name => data(name).size,
    fail: n => failing.add(n), heal: n => failing.delete(n), clear: () => { reads.length = 0; writes.length = 0; } };
}
let cur = store();
const dbNow = { collection: n => cur.db.collection(n), runTransaction: f => cur.db.runTransaction(f) };       // (what the modules captured at load follows the store of the moment)
const fakeAdmin = { firestore: Object.assign(() => dbNow, { FieldValue: { serverTimestamp: () => 'ts', increment: n => ({ __inc: n }), delete: () => null }, Timestamp: Ts }) };
const realLoad = Module._load;
Module._load = function (req, ...rest) { if (/[\/]firebaseAdmin(\.js)?$/.test(req)) return fakeAdmin; return realLoad.call(this, req, ...rest); };
const door = require(path.join(root, 'netlify/functions/firebaseOrders.js'));
const eff = require(path.join(root, 'netlify/functions/employeeEfficiency.js'));
const cron = require(path.join(root, 'netlify/functions/stationSessionsSweepCron.js'));
const EP = require(path.join(root, 'netlify/functions/_editPasscode.js'));
Module._load = realLoad;
const Admins = require(path.join(root, 'netlify/functions/_stationAdmins.js'));
const AS = require(path.join(root, 'netlify/functions/_stationAutoSignout.js'));
const ATT = require(path.join(root, 'netlify/functions/_employeeAttendance.js'));
const PIN_DOOR = require(path.join(root, 'netlify/functions/_stationPinLogin.js'));

const PASS = 'synthetic-pass-9f3k';
process.env.EDIT_PASSCODE = PASS;
const logs = [], keepLog = (...a) => logs.push(a.map(x => typeof x === 'string' ? x : JSON.stringify(x)).join(' '));
console.warn = console.log = console.error = console.info = keepLog;
const say = (...a) => process.stdout.write(a.join(' ') + '\n');
const Z = iso => Date.parse(iso);
const MIN = 60000;
let NOW = Z('2026-10-05T13:00:00Z');                       // 09:00 on Monday 5 Oct 2026 in New York and Toronto (EDT, UTC-4)
Date.now = () => NOW;
const at = (iso) => { NOW = Z(iso); return NOW; };
let ipN = 0;
const fresh = () => {
  cur = store(); Admins.forget(dbNow); Admins.reset(); AS.resetSweep(); PIN_DOOR.reset();
  eff._t.cacheOf(dbNow).memo.clear(); EP.resetCache();
  return cur;
};
const ipOf = () => '198.51.100.' + (++ipN % 250);

/* the session door, as a station page speaks to it */
const sess = (o) => Object.assign({ id: 'welding__weld-1__Tess_Welder__t1', event: 'beat', person: 'Tess Welder', station: 'sorting', device: 'sorting-1', computerId: 'pc-TESTAAAA',
  computerLabel: 'Sorting sorting-1 · TEST', at: NOW, sentAt: NOW }, o);       // (the default-policy station: 10 minutes. Welding and Laser have their own rows, checked in part 6)
async function post(body, o = {}) {
  const r = await door.handler({ httpMethod: 'POST', headers: { 'x-nf-client-connection-ip': o.ip || ipOf() }, queryStringParameters: o.sandbox ? { sandbox: '1' } : {}, body: o.raw != null ? o.raw : JSON.stringify(body) });
  let b = {}; try { b = JSON.parse(r.body || '{}'); } catch (_) {}
  return { status: r.statusCode, body: b, raw: r.body, headers: r.headers || {} };
}
const session = (o, opt) => post({ session: sess(o) }, opt);
const doc = (id, o = {}) => cur.get((o.sandbox ? 'Sandbox_' : '') + 'Station_Sessions', id);
/* a session as the door stored it, for the readers and the sweep */
const seed = (id, o) => cur.put((o.sandbox ? 'Sandbox_' : '') + 'Station_Sessions', id, Object.assign({ id, person: 'Tess Welder', employeeId: '', station: 'sorting', device: 'sorting-1', computerId: 'pc-TESTAAAA', computerLabel: '',
  startAt: Z('2026-10-05T13:00:00Z'), lastSeenAt: Z('2026-10-05T13:00:00Z'), endAt: null, endReason: null, minutes: 0 }, o.d || {}));
const live = async (o = {}) => {
  eff._t.cacheOf(dbNow).memo.clear();
  const r = await eff._t.handle({ httpMethod: 'POST', headers: { 'x-nf-client-connection-ip': '203.0.113.' + (++ipN % 250) }, body: JSON.stringify({ op: 'live', key: PASS, sandbox: !!o.sandbox }) }, dbNow);
  return { status: r.statusCode, body: JSON.parse(r.body || '{}') };
};
const onNow = out => out.body.signedIn.map(x => x.name).sort();
const results = [];
const check = async (name, fn) => { try { await fn(); results.push([name, true]); say('  ok   ' + name); } catch (e) { results.push([name, false]); say('  FAIL ' + name + ': ' + String((e && e.message) || e).replace(/\n+/g, ' | ').slice(0, 700) + ' @' + String((e && e.stack) || '').split('\n').filter(l => /auto-signout-server\.cjs:\d+/.test(l)).slice(0, 1).map(l => l.replace(/.*cjs:/, '')).join('')); } };
const iso = ms => new Date(ms).toISOString();

(async () => {
  /* 1 · the closing instant */
  await check('1 17:00 Toronto, with daylight saving: the instant is right on every kind of day', async () => {
    const C = t => iso(AS.closingInstant(Z(t)));
    assert.strictEqual(C('2026-10-05T13:00:00Z'), '2026-10-05T21:00:00.000Z', 'EDT: 17:00 is 21:00Z');
    assert.strictEqual(C('2026-10-06T02:00:00Z'), '2026-10-05T21:00:00.000Z', '22:00 on 5 Oct is still the 5th in Toronto');
    assert.strictEqual(C('2026-10-31T13:00:00Z'), '2026-10-31T21:00:00.000Z', 'the day before the clocks go back (EDT)');
    assert.strictEqual(C('2026-11-01T12:00:00Z'), '2026-11-01T22:00:00.000Z', 'the day the clocks go back (EST after 2:00): 17:00 is 22:00Z');
    assert.strictEqual(C('2026-11-01T05:30:00Z'), '2026-11-01T22:00:00.000Z', 'the first hour of that day (still EDT) belongs to the same day');
    assert.strictEqual(C('2026-11-02T14:00:00Z'), '2026-11-02T22:00:00.000Z', 'EST');
    assert.strictEqual(C('2027-03-13T14:00:00Z'), '2027-03-13T22:00:00.000Z', 'the day before the clocks go forward (EST)');
    assert.strictEqual(C('2027-03-14T14:00:00Z'), '2027-03-14T21:00:00.000Z', 'the day the clocks go forward: 17:00 EDT is 21:00Z');
    assert.strictEqual(C('2027-03-14T06:30:00Z'), '2027-03-14T21:00:00.000Z', 'before the change on that day');
    assert.strictEqual(C('2027-03-15T14:00:00Z'), '2027-03-15T21:00:00.000Z');
    // the same wall clock as New York, so the "midnight" cap and 17:00 never disagree about the day
    assert.strictEqual(AS.nyMidnightAfter(Z('2026-11-01T14:00:00Z')) - Z('2026-11-01T14:00:00Z'), 15 * 3600e3, 'midnight after 09:00 EST is 15 h later (5:00Z next day)');
  });

  /* 2 · the Admin list */
  await check('2 the Admin list: fallback when missing, names matched like one person, cached 60 s, failure kept apart', async () => {
    const s = fresh();
    assert.deepStrictEqual([...Admins.DEFAULT_ADMINS], ['Paul K', 'Paul'], 'the documented fallback');
    for (const n of ['Paul K', 'paul k', 'PAUL  K', 'Paul_K', 'Paul K.', ' Paul\tK ', 'Paul', 'paul', 'Paul 482915', 'Pául K']) assert.strictEqual(await Admins.isAdmin(dbNow, n), true, 'is an admin: ' + n);
    for (const n of ['Paula', 'Paul Kay', 'Pauline', 'Michael V.', 'K', '', '123456', 'Paul L', null, undefined, 'Paul-K-Smith']) assert.strictEqual(await Admins.isAdmin(dbNow, n), false, 'is not: ' + n);
    assert.strictEqual(s.reads.filter(r => r.name === 'config').length, 1, 'one read of the list for all of those (kept 60 s)');
    // the document: its names replace the fallback; a stored name with odd spelling still matches
    s.put('config', 'stationAdmins', { names: ['Giovanna C.', 'ana m', 7, null, '482915'], note: 'x' });
    assert.strictEqual(await Admins.isAdmin(dbNow, 'Paul K'), true, 'still the old list inside 60 s');
    at('2026-10-05T13:01:01Z');
    assert.strictEqual(await Admins.isAdmin(dbNow, 'Paul K'), false, 'the stored list replaces the fallback: Paul K is not on it any more');
    assert.strictEqual(await Admins.isAdmin(dbNow, 'Giovanna C.'), true); assert.strictEqual(await Admins.isAdmin(dbNow, 'Ana_M'), true, 'ana m = Ana_M'); assert.strictEqual(await Admins.isAdmin(dbNow, 'Giovanna'), false, 'a bare first name is not Giovanna C.');
    // an empty list, a list of non-names, a missing field: the fallback again
    for (const d of [{ names: [] }, { names: [1, 2, ''] }, { other: 1 }, { names: 'Paul K' }]) { s.put('config', 'stationAdmins', d); Admins.forget(dbNow); assert.strictEqual(await Admins.isAdmin(dbNow, 'Paul K'), true, 'fallback for ' + JSON.stringify(d)); }
    // a failed read: unknown (null) with nothing kept; the last list when one was kept
    Admins.forget(dbNow); s.fail('config');
    assert.strictEqual(await Admins.isAdmin(dbNow, 'Paul K'), null, 'unknown, never a guess');
    s.heal('config'); s.put('config', 'stationAdmins', { names: ['Zed Z'] }); at('2026-10-05T13:02:00Z');
    assert.strictEqual(await Admins.isAdmin(dbNow, 'Zed Z'), true);
    s.fail('config'); at('2026-10-05T13:04:00Z');
    assert.strictEqual(await Admins.isAdmin(dbNow, 'Zed Z'), true, 'the list kept from before the outage is used');
    assert.strictEqual(await Admins.isAdmin(dbNow, 'Paul K'), false);
    s.heal('config');
  });

  await check('2b the door { stationAdmin }: one name in, { ok, admin } out; never the list, never a PIN; rate limited; 503 without ok', async () => {
    const s = fresh(); at('2026-10-05T13:00:00Z');
    s.put('config', 'stationAdmins', { names: ['Paul K', 'Dana Admin'] });
    let r = await post({ stationAdmin: 'Paul K' });
    assert.strictEqual(r.status, 200); assert.deepStrictEqual(r.body, { ok: true, admin: true }); assert.strictEqual(r.headers['Cache-Control'], 'no-store');
    assert.deepStrictEqual((await post({ stationAdmin: 'paul_k' })).body, { ok: true, admin: true });
    assert.deepStrictEqual((await post({ stationAdmin: 'Marco R.' })).body, { ok: true, admin: false });
    assert.deepStrictEqual((await post({ stationAdmin: '482915' })).body, { ok: true, admin: false }, 'digits are a PIN, never a person');
    r = await post({ stationAdmin: 'Dana Admin 482915' });
    assert.deepStrictEqual(r.body, { ok: true, admin: true }); assert(!r.raw.includes('482915') && !r.raw.includes('Dana') && !r.raw.includes('Paul'), 'nothing of the name, the list or a number comes back');
    for (const bad of [123, null, ['Paul K'], { n: 'Paul K' }, '', '   ', 'x'.repeat(201)]) { const x = await post({ stationAdmin: bad }); assert.strictEqual(x.status, 400, 'refused: ' + JSON.stringify(bad).slice(0, 30)); assert.strictEqual(x.body.ok, false); }
    r = await post({ stationAdmin: 'Paul K', pad: 'x'.repeat(1100) }); assert.strictEqual(r.status, 413);
    { const g = await door.handler({ httpMethod: 'GET', headers: { 'x-nf-client-connection-ip': ipOf() }, queryStringParameters: { stationAdmin: 'Paul K' } }); assert(!/"admin"/.test(g.body || ''), 'a GET is not the door: it never answers { admin }'); }
    // the list is not in any answer, and the door never writes
    assert.strictEqual(s.writes.length, 0, 'read only: nothing was written');
    // rate limit: 40 requests a minute from one address, then a one-minute lockout; another address and the PIN door are not affected
    Admins.reset(); const ip = '192.0.2.77';
    for (let i = 0; i < 40; i++) assert.strictEqual((await post({ stationAdmin: 'Paul K' }, { ip })).status, 200);
    r = await post({ stationAdmin: 'Paul K' }, { ip });
    assert.strictEqual(r.status, 429); assert.strictEqual(r.body.tooMany, true); assert.strictEqual(r.body.ok, false); assert(Number(r.headers['Retry-After']) >= 1);
    assert.strictEqual((await post({ stationAdmin: 'Paul K' }, { ip })).status, 429, 'still locked');
    assert.strictEqual((await post({ stationAdmin: 'Paul K' }, { ip: '192.0.2.78' })).status, 200, 'another address is unaffected');
    const pin = await post({ pinLogin: '12' }, { ip }); assert.strictEqual(pin.status, 400, 'the PIN door has its own counters (a bad try there is a 400, not locked out)');
    at('2026-10-05T13:01:05Z');
    assert.strictEqual((await post({ stationAdmin: 'Paul K' }, { ip })).status, 200, 'the address works again after the minute');
    // an unreadable list with none kept: 503 and NO ok (the page then treats the person as not an Admin)
    Admins.forget(dbNow); s.fail('config');
    r = await post({ stationAdmin: 'Paul K' }); assert.strictEqual(r.status, 503); assert(!('ok' in r.body), 'no ok field'); assert(!('admin' in r.body));
    s.heal('config');
    assert(!logs.join('\n').includes('482915'), 'no PIN in any log line');
  });

  /* 3 · the session door */
  await check('3a a beat that reports 10 minutes without input ends the session at the LAST INPUT ("idle"); fresh input keeps it', async () => {
    const s = fresh(); at('2026-10-05T13:00:00Z');
    let r = await session({ event: 'start', lastInputAt: NOW });
    assert.strictEqual(r.status, 200); assert.strictEqual(r.body.ended, false);
    let d = doc('welding__weld-1__Tess_Welder__t1');
    assert.strictEqual(d.admin, false, 'the server sets admin at the start'); assert.strictEqual(d.lastInputAt, NOW); assert.strictEqual(d.endAt, null);
    at('2026-10-05T13:05:00Z'); r = await session({ lastInputAt: Z('2026-10-05T13:02:00Z') });
    assert.strictEqual(r.body.ended, false); d = doc('welding__weld-1__Tess_Welder__t1'); assert.strictEqual(d.lastInputAt, Z('2026-10-05T13:02:00Z'));
    at('2026-10-05T13:10:00Z'); r = await session({ lastInputAt: Z('2026-10-05T13:02:00Z') });
    assert.strictEqual(r.body.ended, false, '8 minutes without input: still in');
    at('2026-10-05T13:13:00Z'); r = await session({ lastInputAt: Z('2026-10-05T13:02:00Z') });
    assert.strictEqual(r.body.ended, true); assert.strictEqual(r.body.endReason, 'idle'); assert.strictEqual(r.body.endAt, Z('2026-10-05T13:02:00Z'), 'the end is the last input, not 13:13');
    d = doc('welding__weld-1__Tess_Welder__t1');
    assert.strictEqual(d.endAt, Z('2026-10-05T13:02:00Z')); assert.strictEqual(d.endReason, 'idle'); assert.strictEqual(d.minutes, 2); assert.strictEqual(d.lastSeenAt, NOW, 'the last beat is kept as it was');
    // never backwards: an older lastInputAt cannot pull the stored one back
    fresh(); at('2026-10-05T13:00:00Z');
    await session({ event: 'start', lastInputAt: NOW }); at('2026-10-05T13:04:00Z'); await session({ lastInputAt: Z('2026-10-05T13:03:00Z') });
    at('2026-10-05T13:08:00Z'); await session({ lastInputAt: Z('2026-10-05T13:01:00Z') });
    assert.strictEqual(doc('welding__weld-1__Tess_Welder__t1').lastInputAt, Z('2026-10-05T13:03:00Z'), 'never moves backwards');
    // never in the future, never before the start
    at('2026-10-05T13:09:00Z'); await session({ lastInputAt: Z('2026-10-05T19:00:00Z') });
    assert.strictEqual(doc('welding__weld-1__Tess_Welder__t1').lastInputAt, NOW, 'a future input is "now"');
  });

  await check('3b an end the page sends with idle or closing keeps the last input; any other reason is raised to the last beat', async () => {
    const s = fresh(); at('2026-10-05T13:00:00Z');
    const id1 = 'welding__weld-1__A_Tester__t1', id2 = 'welding__weld-1__B_Tester__t2', id3 = 'welding__weld-1__C_Tester__t3', id4 = 'welding__weld-1__D_Tester__t4';
    for (const [id, p] of [[id1, 'A Tester'], [id2, 'B Tester'], [id3, 'C Tester'], [id4, 'D Tester']]) await session({ id, person: p, event: 'start', lastInputAt: NOW });
    at('2026-10-05T13:10:00Z');
    for (const [id, p] of [[id1, 'A Tester'], [id2, 'B Tester'], [id3, 'C Tester'], [id4, 'D Tester']]) await session({ id, person: p, lastInputAt: Z('2026-10-05T13:09:00Z') });
    at('2026-10-05T13:20:00Z');
    for (const [id, p] of [[id1, 'A Tester'], [id2, 'B Tester'], [id3, 'C Tester'], [id4, 'D Tester']]) await session({ id, person: p, lastInputAt: Z('2026-10-05T13:14:00Z') });
    at('2026-10-05T13:23:00Z');
    let r = await session({ id: id1, person: 'A Tester', event: 'end', reason: 'idle', at: Z('2026-10-05T13:14:00Z'), lastInputAt: Z('2026-10-05T13:14:00Z') });
    assert.strictEqual(r.body.endAt, Z('2026-10-05T13:14:00Z')); assert.strictEqual(r.body.endReason, 'idle'); assert.strictEqual(doc(id1).minutes, 14);
    r = await session({ id: id2, person: 'B Tester', event: 'end', reason: 'closing', at: Z('2026-10-05T13:14:00Z') });
    assert.strictEqual(r.body.endAt, Z('2026-10-05T13:14:00Z')); assert.strictEqual(r.body.endReason, 'closing');
    r = await session({ id: id3, person: 'C Tester', event: 'end', reason: 'signOut', at: Z('2026-10-05T13:14:00Z') });
    assert.strictEqual(r.body.endAt, Z('2026-10-05T13:20:00Z'), 'a person\'s own sign-out is raised to the last beat, as before'); assert.strictEqual(r.body.endReason, 'signOut');
    // not before an input the server already knows, not before the start, not after now, nothing sent = the known last input
    r = await session({ id: id4, person: 'D Tester', event: 'end', reason: 'idle', at: Z('2026-10-05T13:05:00Z') });
    assert.strictEqual(r.body.endAt, Z('2026-10-05T13:14:00Z'), 'never earlier than the last input already reported (13:14)');
    for (const [reason, atT, want] of [['idle', Z('2026-10-05T18:00:00Z'), NOW], ['idle', undefined, Z('2026-10-05T13:14:00Z')], ['idle', 5, Z('2026-10-05T13:14:00Z')], ['closing', 'soon', Z('2026-10-05T13:14:00Z')]]) {
      fresh(); at('2026-10-05T13:00:00Z'); await session({ event: 'start', lastInputAt: NOW }); at('2026-10-05T13:10:00Z'); await session({ lastInputAt: Z('2026-10-05T13:09:00Z') }); at('2026-10-05T13:20:00Z'); await session({ lastInputAt: Z('2026-10-05T13:14:00Z') }); at('2026-10-05T13:23:00Z');
      const x = await session({ event: 'end', reason, at: atT, lastInputAt: undefined }); assert.strictEqual(x.body.endAt, want, `${reason} at ${atT}`);
    }
    // a wrong computer clock is undone: the page is 3 minutes ahead, so its "13:14" is the server's 13:11
    fresh(); at('2026-10-05T13:00:00Z'); await session({ event: 'start', lastInputAt: NOW, sentAt: NOW + 180000, at: NOW + 180000 });
    at('2026-10-05T13:20:00Z'); r = await session({ event: 'end', reason: 'idle', at: Z('2026-10-05T13:14:00Z') + 180000, lastInputAt: Z('2026-10-05T13:14:00Z') + 180000, sentAt: Z('2026-10-05T13:20:00Z') + 180000 });
    assert.strictEqual(r.body.endAt, Z('2026-10-05T13:14:00Z'), 'the page\'s clock is corrected by what the server saw');
    assert.strictEqual(doc('welding__weld-1__Tess_Welder__t1').startAt, Z('2026-10-05T13:00:00Z'));
    // ... and a page that is only 4 seconds off counts as right (under 10 s is no skew)
    fresh(); at('2026-10-05T13:00:00Z'); await session({ event: 'start', lastInputAt: NOW }); at('2026-10-05T13:10:00Z'); await session({ lastInputAt: Z('2026-10-05T13:09:00Z') }); at('2026-10-05T13:20:00Z'); await session({ lastInputAt: Z('2026-10-05T13:14:00Z') });
    r = await session({ event: 'end', reason: 'idle', at: Z('2026-10-05T13:14:00Z') + 4000, sentAt: Z('2026-10-05T13:20:00Z') + 4000 }); assert.strictEqual(r.body.endAt, Z('2026-10-05T13:14:04Z'));
  });

  await check('3c a page silent for 15 minutes: non-Admin with a known last input ends "idle" at it; Admin, or a page that never reported input, ends "closed" at the last beat', async () => {
    fresh(); at('2026-10-05T13:00:00Z');
    const ids = { a: 'welding__weld-1__Dead_Dan__t1', b: 'welding__weld-1__Paul_K__t2', c: 'welding__weld-1__Legacy_Lee__t3' };
    await session({ id: ids.a, person: 'Dead Dan', event: 'start', lastInputAt: NOW });
    await session({ id: ids.b, person: 'Paul K', event: 'start', lastInputAt: NOW });
    await session({ id: ids.c, person: 'Legacy Lee', event: 'start' });              // an old page: no lastInputAt, no sentAt
    assert.strictEqual(doc(ids.b).admin, true, 'Paul K is an Admin (the fallback list)'); assert.strictEqual(doc(ids.c).admin, false); assert(!('lastInputAt' in doc(ids.c)), 'nothing is guessed for an old page');
    at('2026-10-05T13:05:00Z');
    await session({ id: ids.a, person: 'Dead Dan', lastInputAt: Z('2026-10-05T13:03:00Z') }); await session({ id: ids.b, person: 'Paul K', lastInputAt: Z('2026-10-05T13:00:00Z') }); await session({ id: ids.c, person: 'Legacy Lee' });
    at('2026-10-05T13:30:00Z');                                                        // 25 minutes of nothing, then a beat arrives
    const ra = await session({ id: ids.a, person: 'Dead Dan', lastInputAt: Z('2026-10-05T13:29:00Z') });
    assert.strictEqual(ra.body.ended, true); assert.strictEqual(ra.body.endReason, 'idle'); assert.strictEqual(ra.body.endAt, Z('2026-10-05T13:03:00Z'), 'ended at the last input the server knew, not 13:05 and not 13:30');
    const rb = await session({ id: ids.b, person: 'Paul K', lastInputAt: Z('2026-10-05T13:00:00Z') });
    assert.strictEqual(rb.body.endReason, 'closed'); assert.strictEqual(rb.body.endAt, Z('2026-10-05T13:05:00Z'), 'an Admin: the old rule, at the last beat');
    const rc = await session({ id: ids.c, person: 'Legacy Lee' });
    assert.strictEqual(rc.body.endReason, 'closed'); assert.strictEqual(rc.body.endAt, Z('2026-10-05T13:05:00Z'), 'a page that never reported input: the old rule');
  });

  await check('3d Admin is exempt from idle and closing; a non-Admin is not; the midnight end is unchanged', async () => {
    fresh(); at('2026-10-05T13:00:00Z');
    await session({ id: 'welding__weld-1__Paul_K__t9', person: 'Paul K', event: 'start', lastInputAt: NOW });
    await session({ id: 'welding__weld-1__Ivy_Y__t8', person: 'Ivy Y', event: 'start', lastInputAt: NOW });
    for (const t of ['13:05', '13:10', '13:15', '13:20']) { at(`2026-10-05T${t}:00Z`); await session({ id: 'welding__weld-1__Paul_K__t9', person: 'Paul K', lastInputAt: Z('2026-10-05T13:00:00Z') }); }
    assert.strictEqual(doc('welding__weld-1__Paul_K__t9').endAt, null, 'an Admin with no input for 20 minutes stays signed in');
    // 17:00 and later for an Admin too: a beat at 17:05 with input from 09:00 changes nothing
    const keep = (id, lastSeenAt) => cur.put('Station_Sessions', id, Object.assign({}, doc(id), { lastSeenAt }));       // (the page beat all day: only the last beat matters here)
    keep('welding__weld-1__Paul_K__t9', Z('2026-10-05T21:00:00Z'));
    at('2026-10-05T21:05:00Z'); let r = await session({ id: 'welding__weld-1__Paul_K__t9', person: 'Paul K', lastInputAt: Z('2026-10-05T13:00:00Z') });
    assert.strictEqual(r.body.ended, false, 'no closing for an Admin');
    // the client's idle end for an Admin that could not prove it (offline) is still recorded as the page says
    // midnight New York still ends everybody: a beat after midnight ends the Admin at the midnight
    keep('welding__weld-1__Paul_K__t9', Z('2026-10-06T03:58:00Z'));
    at('2026-10-06T04:01:00Z'); r = await session({ id: 'welding__weld-1__Paul_K__t9', person: 'Paul K', lastInputAt: NOW });
    assert.strictEqual(r.body.endReason, 'midnight'); assert.strictEqual(r.body.endAt, Z('2026-10-06T04:00:00Z'));
    assert.strictEqual(doc('welding__weld-1__Ivy_Y__t8').endAt, null, '(the other session has not beaten, so the door did not touch it)');
    // a name added to the list later is exempt at once
    fresh(); at('2026-10-05T13:00:00Z'); await session({ id: 'welding__weld-1__Newly_Named__t1', person: 'Newly Named', event: 'start', lastInputAt: NOW });
    assert.strictEqual(doc('welding__weld-1__Newly_Named__t1').admin, false);
    cur.put('config', 'stationAdmins', { names: ['Newly Named'] }); Admins.forget(dbNow); keep('welding__weld-1__Newly_Named__t1', Z('2026-10-05T13:25:00Z')); at('2026-10-05T13:30:00Z');
    r = await session({ id: 'welding__weld-1__Newly_Named__t1', person: 'Newly Named', lastInputAt: Z('2026-10-05T13:00:00Z') });
    assert.strictEqual(r.body.ended, false, 'on the list now: exempt although the document said admin:false');
  });

  await check('3e the sandbox is its own store: the same beat there ends only the Sandbox_ session', async () => {
    const s = fresh(); at('2026-10-05T13:00:00Z');
    await session({ event: 'start', lastInputAt: NOW }); await session({ event: 'start', lastInputAt: NOW }, { sandbox: true });
    at('2026-10-05T13:20:00Z');
    const rs = await session({ lastInputAt: Z('2026-10-05T13:01:00Z') }, { sandbox: true });
    assert.strictEqual(rs.body.endReason, 'idle');
    assert.strictEqual(doc('welding__weld-1__Tess_Welder__t1', { sandbox: true }).endReason, 'idle'); assert.strictEqual(doc('welding__weld-1__Tess_Welder__t1').endAt, null, 'production untouched');
    assert.strictEqual(s.count('Station_Sessions'), 1); assert.strictEqual(s.count('Sandbox_Station_Sessions'), 1);
  });

  await check('3f an ended session is never rewritten: a later beat or end writes nothing', async () => {
    const s = fresh(); at('2026-10-05T13:00:00Z');
    await session({ event: 'start', lastInputAt: NOW }); at('2026-10-05T13:20:00Z'); await session({ lastInputAt: Z('2026-10-05T13:02:00Z') });
    const id = 'welding__weld-1__Tess_Welder__t1', before = JSON.stringify(doc(id)); s.clear();
    for (const o of [{ lastInputAt: NOW }, { event: 'end', reason: 'signOut', at: NOW }, { event: 'end', reason: 'closing', at: NOW }, { event: 'start', lastInputAt: NOW }]) {
      const r = await session(o); assert.strictEqual(r.body.ended, true); assert.strictEqual(r.body.endReason, 'idle'); at(iso(NOW + MIN));
    }
    assert.strictEqual(s.writesTo('Station_Sessions').length, 0, 'not one write'); assert.strictEqual(JSON.stringify(doc(id)), before);
  });

  /* 4 · the readers and the sweep */
  await check('4a the live board: a dead page ends at its last input, a fresh beat is left alone, an Admin and an old page keep the old rule', async () => {
    const s = fresh(); at('2026-10-05T14:30:00Z');                                    // 10:30
    const T = h => Z('2026-10-05T' + h + ':00Z');
    seed('s-idle', { d: { person: 'Ivy Y', station: 'sorting', device: 'sorting-1', startAt: T('13:00'), lastSeenAt: T('14:28'), lastInputAt: T('14:10') } });        // page alive, 18 minutes without input
    seed('s-dead', { d: { person: 'Ana M', station: 'assembly', device: 'assembly-2', startAt: T('13:00'), lastSeenAt: T('14:05'), lastInputAt: T('14:00') } });      // page died 25 minutes ago
    seed('s-admin', { d: { person: 'Paul K', station: 'shipping', device: 'shipping-1', startAt: T('13:00'), lastSeenAt: T('14:05'), lastInputAt: T('13:00') } });     // an Admin whose page died
    seed('s-fresh', { d: { person: 'Michael V', station: 'sorting', device: 'sorting-2', startAt: T('13:00'), lastSeenAt: T('14:27'), lastInputAt: T('14:22') } });    // beating, input 8 minutes ago
    seed('s-wait', { d: { person: 'Waiting Wes', station: 'design', device: 'design-1', startAt: T('13:00'), lastSeenAt: T('14:20'), lastInputAt: T('14:12') } });     // last beat 10 minutes ago, input 18 minutes ago: the page may have news
    seed('s-old', { d: { person: 'Old Timer', station: 'design', device: 'design', startAt: T('13:00'), lastSeenAt: T('14:25') } });                                 // an old page: no input reported
    const out = await live();
    assert.strictEqual(out.status, 200, JSON.stringify(out.body).slice(0, 300));
    assert.deepStrictEqual(onNow(out), ['Michael V.', 'Old Timer', 'Waiting Wes'], 'on now: the beating page, the old page, and the one whose beat is still inside its window');
    let d = doc('s-idle'); assert.strictEqual(d.endAt, T('14:10')); assert.strictEqual(d.endReason, 'idle'); assert.strictEqual(d.minutes, 70);
    d = doc('s-dead'); assert.strictEqual(d.endAt, T('14:00')); assert.strictEqual(d.endReason, 'idle'); assert.strictEqual(d.minutes, 60);
    d = doc('s-admin'); assert.strictEqual(d.endAt, T('14:05')); assert.strictEqual(d.endReason, 'closed', 'an Admin: only the old rule');
    for (const id of ['s-fresh', 's-wait', 's-old']) assert.strictEqual(doc(id).endAt, null, id + ' stays open');
    assert.strictEqual(s.writesTo('Station_Sessions').length, 3, 'one write per session ended, nothing else');
    for (const w of s.writesTo('Station_Sessions')) assert.deepStrictEqual(w.keys.sort(), ['endAt', 'endReason', 'minutes'], 'only the end is written');
    assert.strictEqual(doc('s-idle').lastSeenAt, T('14:28'), 'the last beat is kept'); assert.strictEqual(doc('s-idle').person, 'Ivy Y');
    // 10 minutes later the waiting page has still said nothing: it was silent 20 minutes, so it is ended at its last input
    at('2026-10-05T14:40:00Z'); s.clear();
    const out2 = await live();
    assert.deepStrictEqual(onNow(out2), ['Michael V.'], 'Waiting Wes is gone (idle at his last input); the old page was quiet 15 minutes (closed at its last beat, as every reader already showed)');
    assert.strictEqual(doc('s-wait').endAt, T('14:12')); assert.strictEqual(doc('s-wait').endReason, 'idle');
  });

  await check('4b 17:00 Toronto: no input in the 10 minutes before it ends "closing" at the last input; input inside the window stays and idle runs from it', async () => {
    const s = fresh(); at('2026-10-05T21:03:00Z');                                    // 17:03
    const T = h => Z('2026-10-05T' + h + ':00Z');
    seed('c-closing', { d: { person: 'Closing Cora', station: 'sorting', device: 'sorting-1', startAt: T('13:00'), lastSeenAt: T('21:02'), lastInputAt: T('20:45') } });      // last beat 17:02, last input 16:45
    seed('c-recent', { d: { person: 'Recent Rae', station: 'sorting', device: 'sorting-2', startAt: T('13:00'), lastSeenAt: T('21:02'), lastInputAt: T('20:55') } });        // input at 16:55: inside the window
    seed('c-edge', { d: { person: 'Edge Ed', station: 'assembly', device: 'assembly-1', startAt: T('13:00'), lastSeenAt: T('21:02'), lastInputAt: T('20:50') } });           // 16:50 exactly: ten minutes is no input in the last ten
    seed('c-before', { d: { person: 'Before Bea', station: 'assembly', device: 'assembly-2', startAt: T('13:00'), lastSeenAt: T('20:52'), lastInputAt: T('20:45') } });      // last beat 16:52: the page has not spoken since
    seed('c-admin', { d: { person: 'Paul K', station: 'shipping', device: 'shipping-1', startAt: T('13:00'), lastSeenAt: T('21:02'), lastInputAt: T('20:00') } });
    seed('c-after', { d: { person: 'After Abe', station: 'design', device: 'design-1', startAt: T('21:30'), lastSeenAt: T('21:36'), lastInputAt: T('21:31') } });
    let out = await live();
    assert.strictEqual(doc('c-closing').endReason, 'closing'); assert.strictEqual(doc('c-closing').endAt, T('20:45'), 'at her last input (16:45), not 17:00 and not 17:03');
    assert.strictEqual(doc('c-edge').endReason, 'closing'); assert.strictEqual(doc('c-edge').endAt, T('20:50'));
    assert.strictEqual(doc('c-recent').endAt, null, 'input at 16:55 stays at 17:00');
    assert.strictEqual(doc('c-before').endAt, null, 'a page whose last beat was before 17:00 is not decided yet: the server waits for its word');
    assert.strictEqual(doc('c-admin').endAt, null, 'an Admin is not signed out at 17:00'); assert.strictEqual(doc('c-after').endAt, null);
    assert.deepStrictEqual(onNow(out), ['After Abe', 'Before Bea', 'Paul K.', 'Recent Rae']);
    // 17:20: Rae has not beaten since 17:02 (18 minutes: a dead page): idle from her last input, 16:55 (not "closing": she had input in the window)
    at('2026-10-05T21:20:00Z'); out = await live();
    assert.strictEqual(doc('c-recent').endReason, 'idle'); assert.strictEqual(doc('c-recent').endAt, T('20:55'));
    // Bea's page died at 16:52: idle at 16:45, not closing (it never reported at or after 17:00)
    assert.strictEqual(doc('c-before').endReason, 'idle'); assert.strictEqual(doc('c-before').endAt, T('20:45'));
    // After Abe signed in at 17:30, after the closing instant: only idle ever applies
    at('2026-10-05T21:50:00Z'); out = await live(); assert.strictEqual(doc('c-after').endAt, null, '5 minutes of beat vs input: still in');
    at('2026-10-05T22:00:00Z'); out = await live(); assert.strictEqual(doc('c-after').endReason, 'idle'); assert.strictEqual(doc('c-after').endAt, T('21:31'));
    assert.strictEqual(doc('c-admin').endReason, 'closed', 'the Admin\'s page died at 17:02: the old rule, at its last beat'); assert.strictEqual(doc('c-admin').endAt, T('21:02'));
  });

  await check('4c the door applies 17:00 at the first beat after it', async () => {
    fresh(); at('2026-10-05T13:00:00Z');
    const ids = ['closing', 'stays', 'idleonly'].map(n => 'welding__weld-1__' + n + '__t1');
    await session({ id: ids[0], person: 'Cora Closing', event: 'start', lastInputAt: NOW }); await session({ id: ids[1], person: 'Stan Stays', event: 'start', lastInputAt: NOW });
    for (const id of ids.slice(0, 2)) cur.put('Station_Sessions', id, Object.assign({}, doc(id), { lastSeenAt: Z('2026-10-05T20:52:00Z') }));       // (they beat all day)
    at('2026-10-05T20:57:00Z'); await session({ id: ids[0], person: 'Cora Closing', lastInputAt: Z('2026-10-05T20:49:00Z') }); await session({ id: ids[1], person: 'Stan Stays', lastInputAt: Z('2026-10-05T20:56:00Z') });
    assert.strictEqual(doc(ids[0]).endAt, null, 'at 16:57 she was 8 minutes idle: still in');
    at('2026-10-05T21:02:00Z');
    let r = await session({ id: ids[0], person: 'Cora Closing', lastInputAt: Z('2026-10-05T20:49:00Z') });
    assert.strictEqual(r.body.endReason, 'closing'); assert.strictEqual(r.body.endAt, Z('2026-10-05T20:49:00Z'));
    r = await session({ id: ids[1], person: 'Stan Stays', lastInputAt: Z('2026-10-05T21:01:00Z') });
    assert.strictEqual(r.body.ended, false, 'input a minute ago: stays');
  });

  await check('4d daylight saving days: 17:00 is 22:00Z after the clocks go back and 21:00Z before; the reverse in spring', async () => {
    const run = async (dayIso, B, L, want, label) => {
      fresh(); at(dayIso + 'T12:00:00Z'); seed('x', { d: { person: 'Dst Dora', station: 'sorting', device: 'sorting-1', startAt: Z(dayIso + 'T13:00:00Z'), lastSeenAt: Z(dayIso + 'T' + B + ':00Z'), lastInputAt: Z(dayIso + 'T' + L + ':00Z') } });
      at(dayIso + 'T' + B + ':00Z'); NOW += 30000;
      await AS.settle({ db: dbNow, now: NOW }, [Object.assign({ id: 'x' }, cur.get('Station_Sessions', 'x'))].map(r => r));
      const d = cur.get('Station_Sessions', 'x');
      assert.strictEqual(d.endReason, want, label + ' ' + d.endReason); assert.strictEqual(d.endAt, Z(dayIso + 'T' + L + ':00Z'), label + ' ends at the last input');
    };
    await run('2026-10-31', '21:01', '20:49', 'closing', 'EDT, 17:00 = 21:00Z');                       // last beat 17:01 EDT, input 16:49
    await run('2026-10-31', '20:59', '20:45', 'idle', 'EDT, a beat at 16:59 is before closing');
    await run('2026-11-01', '22:02', '21:50', 'closing', 'EST, 17:00 = 22:00Z (input 16:50 exactly)');
    await run('2026-11-01', '22:02', '21:51', 'idle', 'EST, input 16:51 is inside the window: idle from it (11 minutes at the beat)');
    await run('2026-11-01', '21:02', '20:00', 'idle', 'EST, 21:02Z is 16:02: before closing');            // (if the code used EDT it would say closing)
    await run('2027-03-13', '22:01', '21:49', 'closing', 'EST, 17:00 = 22:00Z');
    await run('2027-03-14', '21:01', '20:49', 'closing', 'EDT after the spring change: 17:00 = 21:00Z');
    await run('2027-03-14', '20:59', '20:40', 'idle', 'EDT: 20:59Z is 16:59');
  });

  await check('4e idempotent: one write per session, an ended session is never rewritten, nothing is deleted', async () => {
    const s = fresh(); at('2026-10-05T15:00:00Z');
    const T = h => Z('2026-10-05T' + h + ':00Z');
    seed('i-1', { d: { person: 'Ivy Y', station: 'sorting', startAt: T('13:00'), lastSeenAt: T('14:00'), lastInputAt: T('13:50') } });
    seed('i-done', { d: { person: 'Done Dee', station: 'sorting', startAt: T('13:00'), lastSeenAt: T('13:10'), lastInputAt: T('13:05'), endAt: T('13:30'), endReason: 'signOut', minutes: 30 } });
    const noteBefore = JSON.stringify(cur.get('Station_Sessions', 'i-done'));
    for (let i = 0; i < 3; i++) { await live(); AS.resetSweep(); await AS.sweep({ db: dbNow, now: NOW + i * 1000, force: true }); at(iso(NOW + 5000)); }
    assert.strictEqual(s.writesTo('Station_Sessions', 'i-1').length, 1, 'ended once, however many reads and sweeps look');
    assert.strictEqual(s.writesTo('Station_Sessions', 'i-done').length, 0, 'an ended session is not touched'); assert.strictEqual(JSON.stringify(cur.get('Station_Sessions', 'i-done')), noteBefore);
    assert.strictEqual(cur.get('Station_Sessions', 'i-1').endAt, T('13:50')); assert.strictEqual(cur.get('Station_Sessions', 'i-1').endReason, 'idle');
    assert.strictEqual(s.count('Station_Sessions'), 2, 'nothing deleted'); assert(!s.writes.some(w => w.how !== 'merge'), 'every write is a merge of the end only');
    // the end is re-checked on the fresh document: a beat that arrived meanwhile wins
    seed('i-race', { d: { person: 'Race Ray', station: 'sorting', startAt: T('13:00'), lastSeenAt: T('14:00'), lastInputAt: T('13:50') } });
    const row = Object.assign({ id: 'i-race' }, cur.get('Station_Sessions', 'i-race'));            // the reader loaded this...
    cur.put('Station_Sessions', 'i-race', Object.assign({}, cur.get('Station_Sessions', 'i-race'), { lastSeenAt: NOW - 20000, lastInputAt: NOW - 30000 }));   // ...and the page beat before the write
    s.clear(); AS.resetSweep();
    await AS.settle({ db: dbNow, now: NOW }, [row]);
    assert.strictEqual(s.writesTo('Station_Sessions', 'i-race').length, 0, 'the fresh document says the person is working: no write'); assert.strictEqual(cur.get('Station_Sessions', 'i-race').endAt, null);
    assert.strictEqual(row.endAt, T('13:50'), '(the stale row the reader holds is still patched for its own answer)');
  });

  await check('4f two people at one page (a default-policy station): each session is decided on its own, input at the page counts for both', async () => {
    const s = fresh(); at('2026-10-05T14:21:00Z');
    const T = h => Z('2026-10-05T' + h + ':00Z');
    const two = (L) => { seed('w-weld', { d: { person: 'Tess Welder', task: 'welding', startAt: T('13:00'), lastSeenAt: T('14:20'), lastInputAt: L } }); seed('w-match', { d: { person: 'Marco R', task: 'matching', startAt: T('13:30'), lastSeenAt: T('14:20'), lastInputAt: L } }); seed('w-other', { d: { person: 'Ivy Y', station: 'sorting', device: 'sorting-1', startAt: T('13:00'), lastSeenAt: T('14:20'), lastInputAt: T('14:19') } }); };
    two(T('14:18'));
    let out = await live();
    assert.deepStrictEqual(onNow(out), ['Ivy Y.', 'Marco R.', 'Tess Welder'], 'both are in while the page has input'); assert.strictEqual(s.writesTo('Station_Sessions').length, 0);
    fresh(); at('2026-10-05T14:21:00Z'); two(T('14:00'));
    out = await live();
    assert.deepStrictEqual(onNow(out), ['Ivy Y.'], 'no input at the page for 20 minutes: both go, the other station is untouched');
    for (const id of ['w-weld', 'w-match']) { assert.strictEqual(doc(id).endAt, T('14:00')); assert.strictEqual(doc(id).endReason, 'idle'); }
    assert.strictEqual(cur.writesTo('Station_Sessions', 'w-weld').length, 1); assert.strictEqual(cur.writesTo('Station_Sessions', 'w-match').length, 1); assert.strictEqual(doc('w-other').endAt, null);
    assert.strictEqual(doc('w-weld').task, 'welding', 'the task stays on the session');
    // through the door: two sessions of one page, both beating with the page's input
    fresh(); at('2026-10-05T13:00:00Z');
    const a = 'welding__weld-1__Tess_Welder__welding__t1', b = 'welding__weld-1__Marco_R__matching__t2';
    await session({ id: a, person: 'Tess Welder', task: 'welding', event: 'start', lastInputAt: NOW }); await session({ id: b, person: 'Marco R', task: 'matching', event: 'start', lastInputAt: NOW });
    at('2026-10-05T13:10:00Z'); await session({ id: a, person: 'Tess Welder', task: 'welding', lastInputAt: Z('2026-10-05T13:09:00Z') }); await session({ id: b, person: 'Marco R', task: 'matching', lastInputAt: Z('2026-10-05T13:09:00Z') });
    at('2026-10-05T13:20:00Z'); await session({ id: a, person: 'Tess Welder', task: 'welding', lastInputAt: Z('2026-10-05T13:19:00Z') }); await session({ id: b, person: 'Marco R', task: 'matching', lastInputAt: Z('2026-10-05T13:19:00Z') });
    assert.strictEqual(doc(a).endAt, null); assert.strictEqual(doc(b).endAt, null);
    at('2026-10-05T13:35:00Z');
    const ra = await session({ id: a, person: 'Tess Welder', task: 'welding', lastInputAt: Z('2026-10-05T13:19:00Z') }), rb = await session({ id: b, person: 'Marco R', task: 'matching', lastInputAt: Z('2026-10-05T13:19:00Z') });
    assert.strictEqual(ra.body.endAt, Z('2026-10-05T13:19:00Z')); assert.strictEqual(rb.body.endAt, Z('2026-10-05T13:19:00Z')); assert.strictEqual(ra.body.endReason, 'idle');
  });

  await check('4g the sweep: every open session of the real and the Sandbox_ store, each on its own; the cron answers 200 and is cheap when called twice', async () => {
    const s = fresh(); at('2026-10-05T15:00:00Z');
    const T = h => Z('2026-10-05T' + h + ':00Z');
    seed('r-stale', { d: { person: 'Real Rita', station: 'sorting', startAt: T('13:00'), lastSeenAt: T('14:00'), lastInputAt: T('13:40') } });
    seed('r-live', { d: { person: 'Live Lou', station: 'sorting', startAt: T('13:00'), lastSeenAt: T('14:58'), lastInputAt: T('14:57') } });
    seed('b-stale', { sandbox: true, d: { person: 'Box Bo', station: 'sorting', startAt: T('13:00'), lastSeenAt: T('14:00'), lastInputAt: T('13:30') } });
    seed('b-live', { sandbox: true, d: { person: 'Box Live', station: 'sorting', startAt: T('13:00'), lastSeenAt: T('14:58'), lastInputAt: T('14:57') } });
    seed('z-zombie', { d: { person: 'Zombie Zed', station: 'sorting', startAt: Z('2026-10-02T13:00:00Z'), lastSeenAt: Z('2026-10-02T14:00:00Z') } });      // days old, an old page: ended "closed" at its last beat, as the readers always showed it
    seed('e-ended', { d: { person: 'Ended Ed', station: 'sorting', startAt: T('13:00'), lastSeenAt: T('13:30'), endAt: T('13:30'), endReason: 'signOut', minutes: 30 } });
    const r = await cron.handler({ httpMethod: 'POST', headers: {}, body: '{}' });
    assert.strictEqual(r.statusCode, 200); const b = JSON.parse(r.body); assert.strictEqual(b.ok, true);
    assert.strictEqual(b.stores.real.open, 3, 'the query only returns sessions that are not ended'); assert.strictEqual(b.stores.sandbox.open, 2);
    assert.strictEqual(doc('r-stale').endAt, T('13:40')); assert.strictEqual(doc('r-stale').endReason, 'idle'); assert.strictEqual(doc('r-live').endAt, null);
    assert.strictEqual(doc('b-stale', { sandbox: true }).endAt, T('13:30')); assert.strictEqual(doc('b-stale', { sandbox: true }).endReason, 'idle'); assert.strictEqual(doc('b-live', { sandbox: true }).endAt, null);
    assert.strictEqual(doc('z-zombie').endAt, Z('2026-10-02T14:00:00Z')); assert.strictEqual(doc('z-zombie').endReason, 'closed');
    assert(!cur.get('Station_Sessions', 'b-stale') && !cur.get('Sandbox_Station_Sessions', 'r-stale'), 'the two stores never mix');
    assert.strictEqual(s.writesTo('Station_Sessions', 'e-ended').length, 0);
    assert.deepStrictEqual(s.reads.filter(x => x.filters).map(x => x.name + ':' + x.filters.join(',')), ['Station_Sessions:endAt==', 'Sandbox_Station_Sessions:endAt=='], 'one single-field equality query per store: no index to build');
    const r2 = await cron.handler({ httpMethod: 'POST', headers: {}, body: '{}' });
    assert.strictEqual(JSON.parse(r2.body).skipped, true, 'a second call inside a minute does nothing');
    // the file declares its own schedule, literally, as scripts/build-netlify.cjs reads it
    const src = require('fs').readFileSync(path.join(root, 'netlify/functions/stationSessionsSweepCron.js'), 'utf8');
    assert(/^exports\.config\s*=\s*(\{[\s\S]*?\});/m.test(src), 'a literal in-file config'); assert(/schedule:\s*"\*\/5 \* \* \* \*"/.test(src), 'every 5 minutes');
    const entries = require(path.join(root, 'scripts/netlify-function-entries.json'));
    assert(entries.endpoints.includes('stationSessionsSweepCron.js') && entries.modules.includes('_stationAdmins.js') && entries.modules.includes('_stationAutoSignout.js'), 'classified in the function manifest');
  });

  await check('4h an unreadable Admin list leaves every session alone (nobody is ended on a read error); once it can be read they are decided', async () => {
    const s = fresh(); at('2026-10-05T15:00:00Z');
    const T = h => Z('2026-10-05T' + h + ':00Z');
    seed('u-1', { d: { person: 'Unknown Una', station: 'sorting', startAt: T('13:00'), lastSeenAt: T('14:00'), lastInputAt: T('13:40') } });
    seed('u-2', { d: { person: 'Flagged Fay', station: 'sorting', startAt: T('13:00'), lastSeenAt: T('14:00'), lastInputAt: T('13:40'), admin: false } });
    seed('u-3', { d: { person: 'Boss Bea', station: 'sorting', startAt: T('13:00'), lastSeenAt: T('14:00'), lastInputAt: T('13:40'), admin: true } });
    s.fail('config');
    const sw = await AS.sweep({ db: dbNow, now: NOW, force: true });
    assert.strictEqual(sw.stores.real.skipped, 1, 'the one with no stored flag is skipped');
    assert.strictEqual(doc('u-1').endAt, null, 'left open'); assert.strictEqual(doc('u-2').endReason, 'idle', 'a stored admin:false is enough to be decided'); assert.strictEqual(doc('u-3').endReason, 'closed', 'a stored admin:true is exempt from idle: only the old rule');
    s.heal('config'); AS.resetSweep(); at('2026-10-05T15:02:00Z');
    await AS.sweep({ db: dbNow, now: NOW, force: true });
    assert.strictEqual(doc('u-1').endReason, 'idle'); assert.strictEqual(doc('u-1').endAt, T('13:40'));
    // the list changes: a stored admin:false whose name is on the list now is exempt; a stored admin:true whose name was removed stays exempt
    fresh(); at('2026-10-05T15:00:00Z');
    seed('v-1', { d: { person: 'Flagged Fay', startAt: T('13:00'), lastSeenAt: T('14:00'), lastInputAt: T('13:40'), admin: false } });
    seed('v-2', { d: { person: 'Boss Bea', startAt: T('13:00'), lastSeenAt: T('14:00'), lastInputAt: T('13:40'), admin: true } });
    cur.put('config', 'stationAdmins', { names: ['Flagged Fay'] });
    await AS.sweep({ db: dbNow, now: NOW, force: true });
    assert.strictEqual(doc('v-1').endReason, 'closed', 'on the list now: only the old rule'); assert.strictEqual(doc('v-2').endReason, 'closed', 'stored admin:true: only the old rule');
  });

  await check('4i settledSnap hands a reader the same shape, patched; the reader\'s own maths sees the end even when the write fails', async () => {
    const s = fresh(); at('2026-10-05T15:00:00Z');
    const T = h => Z('2026-10-05T' + h + ':00Z');
    seed('p-1', { d: { person: 'Patch Pat', startAt: T('13:00'), lastSeenAt: T('14:00'), lastInputAt: T('13:40') } }); seed('p-2', { d: { person: 'Open Olly', startAt: T('13:00'), lastSeenAt: T('14:59'), lastInputAt: T('14:58') } });
    const snap = await cur.db.collection('Station_Sessions').where('startAt', '>=', T('00:00')).orderBy('startAt', 'desc').limit(10).get();
    const brokenDb = { collection: n => cur.db.collection(n), runTransaction: async () => { throw new Error('write failed'); } };
    const out = await AS.settledSnap({ db: brokenDb, prefix: '', now: NOW }, snap);
    const by = Object.fromEntries(out.docs.map(d => [d.id, d.data()]));
    assert.strictEqual(by['p-1'].endAt, T('13:40')); assert.strictEqual(by['p-1'].endReason, 'idle'); assert.strictEqual(by['p-1'].minutes, 40); assert.strictEqual(by['p-2'].endAt, null);
    assert.strictEqual(by['p-1'].person, 'Patch Pat', 'every other field is as stored'); assert.strictEqual(out.docs.length, 2); assert.strictEqual(typeof out.docs[0].data, 'function');
    assert.strictEqual(doc('p-1').endAt, null, 'the write failed: the stored document is as it was (a later look tries again)');
  });

  /* 5 · attendance */
  await check('5 the person page: an idle or closing day ends at the last input, `endedBy` says why, nothing is estimated, days off are unchanged', async () => {
    const s = fresh(); at('2026-10-05T18:00:00Z');
    const T = h => Z('2026-10-05T' + h + ':00Z');
    seed('a-idle', { d: { person: 'Tess Welder', startAt: T('13:00'), lastSeenAt: T('15:35'), lastInputAt: T('15:30') } });
    seed('a-teammate', { d: { person: 'Marco R', startAt: T('13:00'), lastSeenAt: T('15:35'), lastInputAt: T('15:30') } });
    const ctx = { db: dbNow, admin: { firestore: { Timestamp: Ts } }, now: NOW, today: '2026-10-05' };
    let a = await ATT.attendance(ctx, { name: 'Tess Welder', from: '2026-10-05', to: '2026-10-05' });
    assert.strictEqual(a.ok, true); let c = a.calendar.find(x => x.day === '2026-10-05');
    assert.strictEqual(c.endedBy, 'idle'); assert.strictEqual(c.lastOut, T('15:30'), 'Out = the last input'); assert.strictEqual(c.signedMs, 150 * MIN, '2 h 30 m, not 2 h 35');
    assert(!c.estimated, 'a normal sign-out, no estimate'); assert.strictEqual(c.state, 'worked');
    // the same person's day when the page said closing
    fresh(); at('2026-10-05T21:10:00Z');
    seed('a-closing', { d: { person: 'Cora Closing', startAt: T('13:00'), lastSeenAt: T('21:02'), lastInputAt: T('20:45') } }); seed('a-team2', { d: { person: 'Marco R', startAt: T('13:00'), lastSeenAt: T('21:02'), lastInputAt: T('21:01') } });
    a = await ATT.attendance({ db: dbNow, admin: { firestore: { Timestamp: Ts } }, now: NOW, today: '2026-10-05' }, { name: 'Cora Closing', from: '2026-10-05', to: '2026-10-05' });
    c = a.calendar.find(x => x.day === '2026-10-05'); assert.strictEqual(c.endedBy, 'closing'); assert.strictEqual(c.lastOut, T('20:45')); assert.strictEqual(c.signedMs, 465 * MIN);
    // a signOut and a midnight keep their old words
    fresh(); at('2026-10-06T14:00:00Z');
    seed('o-1', { d: { person: 'Sam S', startAt: T('13:00'), lastSeenAt: T('15:00'), endAt: T('15:00'), endReason: 'signOut', minutes: 120 } }); seed('o-2', { d: { person: 'Mia M', startAt: T('13:00'), lastSeenAt: T('15:00'), endAt: T('15:00'), endReason: 'signOut', minutes: 120 } });
    a = await ATT.attendance({ db: dbNow, admin: { firestore: { Timestamp: Ts } }, now: NOW, today: '2026-10-06' }, { name: 'Sam S', from: '2026-10-05', to: '2026-10-05' });
    assert.strictEqual(a.calendar.find(x => x.day === '2026-10-05').endedBy, 'signOut');
    // the rule table for the portal's words
    assert.strictEqual(AS.END_TEXT.idle, 'Signed out after 10 minutes without input'); assert.strictEqual(AS.END_TEXT.closing, 'Signed out at 5:00 pm');
    assert.deepStrictEqual([...AS.END_REASONS], ['signOut', 'midnight', 'switched', 'closed', 'idle', 'closing']);
  });

  /* 6 · per-station sign-out (Paul, 6 Oct 2026 20:10 UTC, plan.md "Addendum 2"): Welding only at 17:00 Toronto, Laser after 1 hour (30 minutes from 17:00), the rest as built */
  const T = h => Z('2026-10-05T' + h + ':00Z');                                    // Monday 5 Oct 2026, EDT: 09:00 = 13:00Z, 17:00 = 21:00Z
  const row = (id, station, person, d) => seed(id, { d: Object.assign({ station, person, device: station === 'welding' ? 'weld-1' : station === 'laser' ? 'charm-nest-1' : station + '-1' }, station === 'welding' ? { task: 'welding' } : {}, d) });
  const state = id => { const d = cur.get('Station_Sessions', id); return d.endAt ? d.endReason + '@' + iso(d.endAt).slice(11, 19) : 'open'; };
  const names = out => out.body.signedIn.map(x => x.name).sort();

  await check('6a the policy table: the documented one, frozen, the same for every station key, and equal to the page\'s table in station-session.js', async () => {
    const want = { default: { idleMin: 10, idleMinAfter17: 10, closeAt17: 'idleWindow' }, welding: { idleMin: 0, idleMinAfter17: 0, closeAt17: 'always' }, laser: { idleMin: 60, idleMinAfter17: 30, closeAt17: 'idleWindow' } };
    assert.deepStrictEqual(JSON.parse(JSON.stringify(AS.POLICY)), want, 'the table of the addendum');
    assert(Object.isFrozen(AS.POLICY) && Object.isFrozen(AS.POLICY.welding), 'frozen');
    const P = require(path.join(root, 'netlify/functions/_stationSignoutPolicy.js'));
    for (const k of ['sorting', 'assembly', 'shipping', 'design', 'sorter', 'qr', 'inbox', '', 'default', 'constructor', '__proto__', 'toString', 'hasOwnProperty', 'nonsense']) assert.deepStrictEqual(P.copyOf(k), want.default, 'a station with no row of its own is the default: ' + JSON.stringify(k));
    assert.deepStrictEqual(P.copyOf(null), want.default); assert.deepStrictEqual(P.copyOf(undefined), want.default);
    assert.deepStrictEqual(P.copyOf('welding'), want.welding); assert.deepStrictEqual(P.copyOf(' Welding '), want.welding, 'case and spaces do not matter'); assert.deepStrictEqual(P.copyOf('laser'), want.laser);
    // the page's table, read out of its source (StationSession.policy answers a copy of the station's row of this literal): the two can never drift apart
    const src = require('fs').readFileSync(path.join(root, 'station-session.js'), 'utf8');
    const at0 = src.search(/\bconst\s+POLICY\s*=\s*\{/);
    assert(at0 >= 0, 'station-session.js has no `const POLICY = {` table (AD3)');
    let i = src.indexOf('{', at0), depth = 0, j = i;
    for (; j < src.length; j++) { if (src[j] === '{') depth++; else if (src[j] === '}' && --depth === 0) break; }
    const client = require('vm').runInNewContext('(' + src.slice(i, j + 1) + ')');
    assert.deepStrictEqual(JSON.parse(JSON.stringify(client)), JSON.parse(JSON.stringify(AS.POLICY)), 'the page and the server carry the same table, value for value');
    assert(/policy\s*[:(]/.test(src) || /function\s+policy\b/.test(src), 'the page exposes StationSession.policy');
  });

  await check('6b Welding: no idle sign-out at all (6 hours without input, a page silent for 6 hours, two people), the board keeps them', async () => {
    const s = fresh(); at('2026-10-05T19:00:00Z');                                   // 15:00, before 17:00
    row('w-live', 'welding', 'Tess Welder', { startAt: T('13:00'), lastSeenAt: NOW - 60000, lastInputAt: T('13:00') });        // the page beats, nobody touched it for 6 hours
    row('w-dead', 'welding', 'Dead Dana', { startAt: T('13:00'), lastSeenAt: T('13:00'), lastInputAt: T('13:00') });          // the page went quiet at 09:00
    row('w-match', 'welding', 'Marco R', { task: 'matching', startAt: T('14:00'), lastSeenAt: NOW - 120000, lastInputAt: T('14:00') });
    row('w-noinput', 'welding', 'Old Page', { startAt: T('13:00'), lastSeenAt: T('13:00') });                                 // a page that never reported input
    row('s-ctl', 'sorting', 'Ctl Cat', { startAt: T('13:00'), lastSeenAt: NOW - 60000, lastInputAt: T('13:00') });             // the control: a default station, 6 hours idle
    const out = await live();
    assert.strictEqual(out.status, 200);
    assert.deepStrictEqual(names(out), ['Dead Dana', 'Marco R.', 'Old Page', 'Tess Welder'], 'every Welding person is still signed in; the sorting control is not');
    for (const id of ['w-live', 'w-dead', 'w-match', 'w-noinput']) assert.strictEqual(state(id), 'open', id);
    assert.strictEqual(state('s-ctl'), 'idle@13:00:00', 'the control ends idle at its last input');
    assert.strictEqual(s.writesTo('Station_Sessions').length, 1, 'only the control was written');
    // the hours of a quiet Welding page keep counting while it is signed in (a reader never closes it at its last beat)
    const wl = out.body.stations.find(x => x.key === 'welding');
    assert(wl && wl.people.length === 4, 'the Welding card lists the four people');
    // decide() itself: 20 hours of no input before 17:00 is still nothing (a session of an earlier hour, read at 16:59:59)
    const d0 = { startAt: T('13:00'), lastSeenAt: T('20:59'), lastInputAt: T('13:00'), station: 'welding' };
    assert.strictEqual(AS.decide(d0, Z('2026-10-05T20:59:59Z'), false), null);
    assert.strictEqual(AS.decide(Object.assign({}, d0, { station: 'sorting' }), Z('2026-10-05T20:59:59Z'), false).endReason, 'idle', '(the same row at a default station is long idle)');
    // through the door: a beat after six quiet hours is the same session carrying on
    fresh(); at('2026-10-05T13:00:00Z');
    const id = 'welding__weld-1__Tess_Welder__welding__t1';
    await session({ id, station: 'welding', device: 'weld-1', task: 'welding', event: 'start', lastInputAt: NOW });
    at('2026-10-05T19:00:00Z'); let r = await session({ id, station: 'welding', device: 'weld-1', task: 'welding', lastInputAt: T('13:00') });
    assert.strictEqual(r.body.ended, false, 'six quiet hours, then a beat: still the same session'); assert.strictEqual(doc(id).lastSeenAt, NOW); assert.strictEqual(doc(id).endAt, null);
    at('2026-10-05T20:59:59Z'); r = await session({ id, station: 'welding', device: 'weld-1', task: 'welding', lastInputAt: T('13:00') });
    assert.strictEqual(r.body.ended, false, '16:59:59');
  });

  await check('6c Welding: every person ends at 17:00 Toronto SHARP (closing), whatever the last input; 16:59 stays; a page read at 18:30; never rewritten', async () => {
    const s = fresh();
    const mk = () => {
      row('c-a', 'welding', 'Anna Weld', { startAt: T('13:00'), lastSeenAt: T('20:58'), lastInputAt: T('13:00') });                          // beating, no input since 09:00
      row('c-b', 'welding', 'Ben Weld', { startAt: T('13:00'), lastSeenAt: T('20:59'), lastInputAt: Z('2026-10-05T20:59:30Z') });           // input half a minute before 17:00
      row('c-m', 'welding', 'Mia Match', { task: 'matching', startAt: T('14:00'), lastSeenAt: T('20:55'), lastInputAt: T('20:30') });       // the second person at the same page, the other task
      row('c-out', 'welding', 'Out Early', { startAt: T('13:00'), lastSeenAt: T('20:30'), lastInputAt: T('20:30'), endAt: T('20:30'), endReason: 'signOut', minutes: 450 });   // signed himself out at 16:30
    };
    mk(); at('2026-10-05T20:59:59Z');
    let out = await live();
    assert.deepStrictEqual(names(out), ['Anna Weld', 'Ben Weld', 'Mia Match'], '16:59:59: everybody still in'); assert.strictEqual(s.writesTo('Station_Sessions').length, 0);
    at('2026-10-05T21:00:00Z'); out = await live();
    for (const id of ['c-a', 'c-b', 'c-m']) assert.strictEqual(state(id), 'closing@21:00:00', id + ': 17:00:00 sharp, not the last input');
    assert.strictEqual(cur.get('Station_Sessions', 'c-a').minutes, 480); assert.strictEqual(cur.get('Station_Sessions', 'c-m').minutes, 420);
    assert.strictEqual(state('c-out'), 'signOut@20:30:00', 'a person who signed out by hand is not touched');
    assert.deepStrictEqual(names(out), [], 'nobody is signed in at Welding after 17:00');
    for (const w of s.writesTo('Station_Sessions')) assert.deepStrictEqual(w.keys.sort(), ['endAt', 'endReason', 'minutes'], 'only the end is written');
    assert.strictEqual(s.writesTo('Station_Sessions').length, 3);
    // read at 18:30 for a page that last beat at 16:55 and then slept: the same answer, ended at 17:00 of that day
    fresh(); row('d-1', 'welding', 'Sleepy Sam', { startAt: T('13:00'), lastSeenAt: T('20:55'), lastInputAt: T('20:50') }); at('2026-10-05T22:30:00Z');
    await live(); assert.strictEqual(state('d-1'), 'closing@21:00:00', 'a page read at 18:30 is ended at 17:00 (not at its last beat, not at 18:30)');
    // idempotent: again and again, by readers and by the sweep: one write each, nothing rewritten, nothing deleted
    for (let k = 0; k < 3; k++) { await live(); AS.resetSweep(); await AS.sweep({ db: dbNow, now: NOW + k * 1000, force: true }); at(iso(NOW + 5000)); }
    assert.strictEqual(cur.writesTo('Station_Sessions', 'd-1').length, 1, 'ended once'); assert.strictEqual(cur.count('Station_Sessions'), 1);
    // a session that began AFTER 17:00 is never due at 17:00 (the next one is tomorrow's, the midnight comes first): it stays through the evening and ends at the midnight
    fresh(); row('e-1', 'welding', 'Late Lou', { startAt: T('21:30'), lastSeenAt: T('22:25'), lastInputAt: T('21:30') }); at('2026-10-05T22:30:00Z');
    await live(); assert.strictEqual(state('e-1'), 'open', 'signed in at 17:30: no idle sign-out, no 17:00');
    at('2026-10-06T03:59:59Z'); AS.resetSweep(); await AS.sweep({ db: dbNow, now: NOW, force: true }); assert.strictEqual(state('e-1'), 'open');
    at('2026-10-06T04:01:00Z'); AS.resetSweep(); await AS.sweep({ db: dbNow, now: NOW, force: true }); assert.strictEqual(state('e-1'), 'midnight@04:00:00', 'New York midnight ends everybody');
    // through the door: a beat after 17:00 ends the session at 17:00; a page that loads after 17:00 names 17:00 as its end and keeps it (a reload's input is later)
    fresh(); at('2026-10-05T13:00:00Z');
    const id = 'welding__weld-1__Ivo_Weld__welding__t1', id2 = 'welding__weld-1__Uma_Weld__matching__t2', id3 = 'welding__weld-1__Ida_Weld__welding__t3';
    for (const [i, p, t] of [[id, 'Ivo Weld', 'welding'], [id2, 'Uma Weld', 'matching'], [id3, 'Ida Weld', 'welding']]) await session({ id: i, person: p, station: 'welding', device: 'weld-1', task: t, event: 'start', lastInputAt: NOW });
    at('2026-10-05T20:55:00Z'); for (const [i, p, t] of [[id, 'Ivo Weld', 'welding'], [id2, 'Uma Weld', 'matching'], [id3, 'Ida Weld', 'welding']]) await session({ id: i, person: p, station: 'welding', device: 'weld-1', task: t, lastInputAt: T('20:50') });
    at('2026-10-05T21:03:00Z');
    let r = await session({ id, person: 'Ivo Weld', station: 'welding', device: 'weld-1', task: 'welding', lastInputAt: Z('2026-10-05T21:02:30Z') });
    assert.strictEqual(r.body.ended, true); assert.strictEqual(r.body.endReason, 'closing'); assert.strictEqual(r.body.endAt, T('21:00'), 'a beat at 17:03 ends the session at 17:00 sharp');
    r = await session({ id: id2, person: 'Uma Weld', station: 'welding', device: 'weld-1', task: 'matching', event: 'end', reason: 'closing', at: T('21:00'), lastInputAt: Z('2026-10-05T21:03:00Z') });
    assert.strictEqual(r.body.endAt, T('21:00'), 'the page\'s closing end, with an input after 17:00 in the same request: still 17:00 sharp'); assert.strictEqual(r.body.endReason, 'closing');
    r = await session({ id: id3, person: 'Ida Weld', station: 'welding', device: 'weld-1', task: 'welding', event: 'end', reason: 'signOut', at: Z('2026-10-05T21:03:00Z') });
    assert.strictEqual(r.body.endReason, 'signOut', 'an explicit sign-out (a person\'s own, or the page leaving) is stored as it always was'); assert.strictEqual(r.body.endAt, Z('2026-10-05T21:03:00Z'));
    fresh(); at('2026-10-05T13:00:00Z'); await session({ id, person: 'Ivo Weld', station: 'welding', device: 'weld-1', task: 'welding', event: 'start', lastInputAt: NOW });
    at('2026-10-05T14:20:00Z'); r = await session({ id, person: 'Ivo Weld', station: 'welding', device: 'weld-1', task: 'welding', event: 'end', reason: 'idle', at: T('14:05'), lastInputAt: T('14:05') });
    assert.strictEqual(r.body.endReason, 'idle'); assert.strictEqual(r.body.endAt, T('14:05'), 'an end the page itself names is not second-guessed');
  });

  await check('6d Laser: 60 minutes before 17:00 (59:59 stays, 60:00 ends, "idle", end = last input), 10 minutes of nothing never ends it', async () => {
    fresh(); at('2026-10-05T13:00:00Z');
    const LID = n => `laser__charm-nest-1__${n}__t1`, L0 = T('13:30');
    const lsess = (n, o) => session(Object.assign({ id: LID(n), person: n.replace('_', ' '), station: 'laser', device: 'charm-nest-1' }, o));
    for (const n of ['Lena_A', 'Liam_B', 'Lola_C']) await lsess(n, { event: 'start', lastInputAt: NOW });
    at('2026-10-05T13:30:00Z'); for (const n of ['Lena_A', 'Liam_B', 'Lola_C']) await lsess(n, { lastInputAt: L0 });
    at('2026-10-05T13:45:00Z'); let r = await lsess('Lola_C', { lastInputAt: L0 });
    assert.strictEqual(r.body.ended, false, '15 minutes without input is nothing for Laser (the default would be out at 10)');
    at('2026-10-05T14:29:59Z'); r = await lsess('Lena_A', { lastInputAt: L0 });
    assert.strictEqual(r.body.ended, false, '59:59 without input: stays'); assert.strictEqual(doc(LID('Lena_A')).endAt, null);
    at('2026-10-05T14:30:00Z'); r = await lsess('Liam_B', { lastInputAt: L0 });
    assert.strictEqual(r.body.ended, true, '60:00 without input: out'); assert.strictEqual(r.body.endReason, 'idle'); assert.strictEqual(r.body.endAt, L0, 'ended at the last input, not at 14:30');
    assert.strictEqual(doc(LID('Liam_B')).minutes, 30);
    // the same limit through the readers (the page is awake: its beat says how long it has had no input)
    const s = fresh(); at('2026-10-05T14:29:59Z');
    row('r-59', 'laser', 'Rae Fifty', { startAt: T('13:00'), lastSeenAt: NOW, lastInputAt: L0 }); row('r-60', 'laser', 'Rob Sixty', { startAt: T('13:00'), lastSeenAt: NOW, lastInputAt: Z('2026-10-05T13:29:59Z') });
    let out = await live(); assert.deepStrictEqual(names(out), ['Rae Fifty']); assert.strictEqual(state('r-59'), 'open'); assert.strictEqual(state('r-60'), 'idle@13:29:59');
    // the old 10-minute marks do nothing for a Laser person: 12 minutes, 25 minutes, 45 minutes
    fresh(); at('2026-10-05T14:30:00Z'); for (const [n, m] of [['a', 12], ['b', 25], ['c', 45]]) row('k-' + n, 'laser', 'Kay ' + n.toUpperCase(), { startAt: T('13:00'), lastSeenAt: NOW, lastInputAt: NOW - m * 60000 });
    out = await live(); assert.strictEqual(out.body.signedIn.length, 3); assert.strictEqual(cur.writesTo('Station_Sessions').length, 0);
    // the Sorter app with no role (Admin) or with the Design role is the default: 10 minutes
    fresh(); at('2026-10-05T14:30:00Z'); row('x-1', 'sorter', 'Sorter Sue', { startAt: T('13:00'), lastSeenAt: NOW, lastInputAt: NOW - 15 * 60000 }); row('x-2', 'design', 'Design Dee', { startAt: T('13:00'), lastSeenAt: NOW, lastInputAt: NOW - 15 * 60000 });
    await live(); assert.strictEqual(state('x-1').slice(0, 4), 'idle'); assert.strictEqual(state('x-2').slice(0, 4), 'idle');
  });

  await check('6e Laser from 17:00 Toronto: 30 minutes; last input 16:25 is out at 17:00 (closing, ended 16:25), 16:45 stays and is out at 17:15 (closing, ended 16:45); 15:50 was out at 16:50 (idle)', async () => {
    const run = async (L, B, now, station = 'laser') => { fresh(); row('x', station, 'Lisa Laser', { startAt: T('13:00'), lastSeenAt: B, lastInputAt: L }); at(iso(now)); await live(); return state('x'); };
    assert.strictEqual(await run(T('20:25'), T('21:00'), T('21:00') + 5000), 'closing@20:25:00', 'the page beat at 17:00 with last input 16:25: out, ended at 16:25');
    assert.strictEqual(await run(T('20:30'), T('21:00'), T('21:00') + 5000), 'closing@20:30:00', 'exactly 30 minutes: out');
    assert.strictEqual(await run(Z('2026-10-05T20:30:01Z'), T('21:00'), T('21:00') + 5000), 'open', '29:59: stays');
    assert.strictEqual(await run(T('20:45'), T('21:00'), T('21:00') + 5000), 'open', 'input at 16:45 stays at 17:00');
    assert.strictEqual(await run(T('20:45'), Z('2026-10-05T21:14:59Z'), Z('2026-10-05T21:15:04Z')), 'open', '29:59 after 16:45: stays');
    assert.strictEqual(await run(T('20:45'), T('21:15'), T('21:15') + 5000), 'closing@20:45:00', 'out at 17:15, ended at the last input 16:45');
    assert.strictEqual(await run(T('19:50'), T('20:50'), T('20:50') + 5000), 'idle@19:50:00', 'last input 15:50: out at 16:50 (idle, 60 minutes), ended at 15:50');
    assert.strictEqual(await run(T('19:50'), Z('2026-10-05T20:49:59Z'), Z('2026-10-05T20:50:04Z')), 'open', '16:49:59 beat: not yet');
    assert.strictEqual(await run(T('20:00'), T('21:00'), T('21:00') + 5000), 'closing@20:00:00', 'last input 16:00: the 60 minutes end at exactly 17:00 and the limit from 17:00 is 30: closing');
    assert.strictEqual(await run(Z('2026-10-05T19:59:59Z'), Z('2026-10-05T20:59:59Z'), Z('2026-10-05T21:00:01Z') - 1000), 'idle@19:59:59', 'last input 15:59:59: 60 minutes pass at 16:59:59, before 17:00: idle');
    // a session that began after 17:00: the limit is the 30 minutes from the start
    assert.strictEqual(await run(T('21:30'), Z('2026-10-05T21:59:59Z'), T('22:00')), 'open'); assert.strictEqual(await run(T('21:30'), T('22:00'), T('22:00') + 5000), 'closing@21:30:00', 'signed in at 17:30, no input: out after 30 minutes (closing), ended at the last input');
    // the rule table, at the function: reason and end for a grid (what decide says is what every reader and the sweep do)
    for (const [L, B, want] of [['20:25', '21:00', 'closing@20:25'], ['20:45', '21:14', null], ['20:45', '21:15', 'closing@20:45'], ['19:50', '20:50', 'idle@19:50'], ['19:51', '20:50', null], ['20:00', '20:59', null], ['20:00', '21:00', 'closing@20:00']]) {
      const d = AS.decide({ startAt: T('13:00'), lastSeenAt: T(B), lastInputAt: T(L), station: 'laser' }, T(B) + 1000, false);
      assert.strictEqual(d ? d.endReason + '@' + iso(d.endAt).slice(11, 16) : null, want, `decide L ${L} B ${B}`);
    }
    // the same row at a default station (10 minutes): unchanged (closing at 17:00 when the last input is 16:50 or before)
    assert.strictEqual(await run(T('20:25'), T('21:00'), T('21:00') + 5000, 'sorting'), 'closing@20:25:00'); assert.strictEqual(await run(T('20:55'), T('21:02'), T('21:03'), 'sorting'), 'open');
  });

  await check('6f a dead page: Laser ends only when its limit has passed since the last beat (60 minutes, 30 counted from 17:00), Welding at 17:00; never at 15 minutes', async () => {
    const dead = async (station, d, now, extra = {}) => { fresh(); row('x', station, station === 'welding' ? 'Dead Welder' : 'Dead Laser', Object.assign({ startAt: T('13:00') }, d)); at(iso(now)); const out = await live(); return [state('x'), names(out).length]; };
    // Laser: last beat 12:00 (16:00Z), last input 11:55
    const ld = { lastSeenAt: T('16:00'), lastInputAt: T('15:55') };
    assert.deepStrictEqual(await dead('laser', ld, T('16:15') + 1000), ['open', 1], '15 minutes of silence: still signed in (the default would have closed it)');
    assert.deepStrictEqual(await dead('laser', ld, T('16:30')), ['open', 1], '30 minutes');
    assert.deepStrictEqual(await dead('laser', ld, Z('2026-10-05T16:59:59Z')), ['open', 1], '59:59 since the last beat');
    assert.deepStrictEqual(await dead('laser', ld, T('17:00')), ['idle@15:55:00', 0], '60:00 since the last beat: ended at its last input');
    // near 17:00: last beat 16:40, last input 16:35 -> the limit from 17:00 is 30 minutes counted from the beat: 17:10
    const l2 = { lastSeenAt: T('20:40'), lastInputAt: T('20:35') };
    assert.deepStrictEqual(await dead('laser', l2, Z('2026-10-05T21:09:59Z')), ['open', 1]); assert.deepStrictEqual(await dead('laser', l2, T('21:10')), ['closing@20:35:00', 0], 'closing: the sign-out falls after 17:00');
    // no input ever reported: ended "closed" at its last beat once the limit has passed
    const l3 = { lastSeenAt: T('16:00') };
    assert.deepStrictEqual(await dead('laser', l3, T('16:30')), ['open', 1]); assert.deepStrictEqual(await dead('laser', l3, T('17:00')), ['closed@16:00:00', 0]);
    // Welding: last beat 10:00, no input since: signed in until 17:00 sharp
    const wd = { lastSeenAt: T('14:00'), lastInputAt: T('14:00') };
    assert.deepStrictEqual(await dead('welding', wd, T('16:00')), ['open', 1]); assert.deepStrictEqual(await dead('welding', wd, Z('2026-10-05T20:59:59Z')), ['open', 1]);
    assert.deepStrictEqual(await dead('welding', wd, T('21:00')), ['closing@21:00:00', 0]); assert.deepStrictEqual(await dead('welding', wd, T('22:30')), ['closing@21:00:00', 0]);
    assert.deepStrictEqual(await dead('welding', { lastSeenAt: T('14:00') }, T('22:30')), ['closing@21:00:00', 0], 'a page that never reported input is ended at 17:00 too');
    // the control: a default station's dead page is ended at its last input after 15 minutes, as before
    assert.deepStrictEqual(await dead('assembly', { lastSeenAt: T('16:00'), lastInputAt: T('15:55') }, T('16:15')), ['idle@15:55:00', 0]);
    // Admin keeps the old rule on every station: a quiet page is "closed" at its last beat after 15 minutes; no 17:00, no idle while it beats
    fresh(); row('a-1', 'welding', 'Paul K', { startAt: T('13:00'), lastSeenAt: T('14:00'), lastInputAt: T('13:00') }); row('a-2', 'welding', 'Paul K', { task: 'matching', startAt: T('13:00'), lastSeenAt: T('22:25'), lastInputAt: T('13:00') });
    at('2026-10-05T22:30:00Z'); await live(); assert.strictEqual(state('a-1'), 'closed@14:00:00'); assert.strictEqual(state('a-2'), 'open', 'an Admin whose page beats is not signed out at 17:00');
    fresh(); row('a-3', 'laser', 'Paul K', { startAt: T('13:00'), lastSeenAt: T('16:00'), lastInputAt: T('15:55') }); at('2026-10-05T16:20:00Z'); await live(); assert.strictEqual(state('a-3'), 'closed@16:00:00', 'Admin: the old rule');
    // an unreadable Admin list: Welding and Laser sessions are left alone like every other
    fresh(); row('u-1', 'welding', 'Unk Wen', { startAt: T('13:00'), lastSeenAt: T('13:05') }); row('u-2', 'laser', 'Unk Lee', { startAt: T('13:00'), lastSeenAt: T('13:05'), lastInputAt: T('13:00') });
    cur.fail('config'); at('2026-10-05T22:30:00Z'); const sw = await AS.sweep({ db: dbNow, now: NOW, force: true });
    assert.strictEqual(sw.stores.real.skipped, 2); assert.strictEqual(state('u-1'), 'open'); assert.strictEqual(state('u-2'), 'open');
    cur.heal('config'); AS.resetSweep(); await AS.sweep({ db: dbNow, now: NOW + 1000, force: true }); assert.strictEqual(state('u-1'), 'closing@21:00:00'); assert.strictEqual(state('u-2'), 'closing@13:00:00'.replace('closing', 'idle'));
  });

  await check('6g the door: a beat after a long silence is the same session for Laser (inside its limit) and Welding; the default still closes it', async () => {
    fresh(); at('2026-10-05T13:00:00Z');
    const ids = { l: 'laser__charm-nest-1__Lia_L__t1', w: 'welding__weld-1__Wil_W__welding__t1', d: 'sorting__sorting-1__Dev_D__t1' };
    const body = (k, o) => Object.assign({ id: ids[k], person: { l: 'Lia L', w: 'Wil W', d: 'Dev D' }[k], station: { l: 'laser', w: 'welding', d: 'sorting' }[k], device: { l: 'charm-nest-1', w: 'weld-1', d: 'sorting-1' }[k] }, k === 'w' ? { task: 'welding' } : {}, o);
    for (const k of ['l', 'w', 'd']) await session(body(k, { event: 'start', lastInputAt: NOW }));
    at('2026-10-05T13:40:00Z');                                                         // 40 quiet minutes, then the pages wake and beat with fresh input
    const r = {}; for (const k of ['l', 'w', 'd']) r[k] = await session(body(k, { lastInputAt: NOW }));
    assert.strictEqual(r.l.body.ended, false, 'Laser: the same session carries on'); assert.strictEqual(r.w.body.ended, false, 'Welding: the same session carries on');
    assert.strictEqual(r.d.body.ended, true); assert.strictEqual(r.d.body.endReason, 'idle', 'the default: ended at its last input (10 minutes), as before');
    assert.strictEqual(doc(ids.l).lastSeenAt, NOW); assert.strictEqual(doc(ids.l).startAt, T('13:00'), 'one session, the same start (a sheet clock is not restarted)');
    // after the Laser limit has passed in silence, the beat does not bring it back
    at('2026-10-05T15:00:00Z'); const r2 = await session(body('l', { lastInputAt: Z('2026-10-05T13:40:00Z') }));
    assert.strictEqual(r2.body.ended, true); assert.strictEqual(r2.body.endReason, 'idle'); assert.strictEqual(r2.body.endAt, Z('2026-10-05T13:40:00Z'));
  });

  await check('6h daylight saving days: Welding ends at 17:00 local (22:00Z after 1 Nov, 21:00Z before it and after 14 Mar); Laser\'s 17:00 window follows', async () => {
    for (const [day, c] of [['2026-10-31', '21:00'], ['2026-11-01', '22:00'], ['2026-11-02', '22:00'], ['2027-03-13', '22:00'], ['2027-03-14', '21:00'], ['2027-03-15', '21:00']]) {
      const Zd = h => Z(`${day}T${h}:00Z`), startH = c === '21:00' ? '13:00' : '14:00', before = Z(`${day}T${c}:00Z`) - 1000;
      fresh(); row('w', 'welding', 'Dst Wade', { startAt: Zd(startH), lastSeenAt: before - 60000, lastInputAt: Zd(startH) }); at(iso(before)); await live(); assert.strictEqual(state('w'), 'open', day + ' 16:59:59');
      at(`${day}T${c}:00Z`); await live(); assert.strictEqual(state('w'), `closing@${c}:00`, day + ' 17:00 local');
      // Laser: last input 35 minutes before 17:00 local, beat at 17:00 local: out (closing); last input 15 minutes before: stays
      const cm = Z(`${day}T${c}:00Z`);
      fresh(); row('l1', 'laser', 'Dst Lana', { startAt: Zd(startH), lastSeenAt: cm, lastInputAt: cm - 35 * 60000 }); row('l2', 'laser', 'Dst Lena', { startAt: Zd(startH), lastSeenAt: cm, lastInputAt: cm - 15 * 60000 });
      at(iso(cm + 5000)); await live(); assert.strictEqual(state('l1'), 'closing@' + iso(cm - 35 * 60000).slice(11, 19), day + ' laser 35 minutes'); assert.strictEqual(state('l2'), 'open', day + ' laser 15 minutes');
    }
  });

  await check('6i the sweep and the sandbox: Welding and Laser end by the same rules in each store alone; nothing is deleted; an Admin is not touched', async () => {
    const s = fresh(); at('2026-10-05T21:05:00Z');
    row('rw', 'welding', 'Real Wren', { startAt: T('13:00'), lastSeenAt: T('13:05'), lastInputAt: T('13:00') });
    row('rl', 'laser', 'Real Lars', { startAt: T('13:00'), lastSeenAt: T('20:30'), lastInputAt: T('20:30') });
    row('rp', 'welding', 'Paul K', { startAt: T('13:00'), lastSeenAt: T('21:04'), lastInputAt: T('13:00') });
    seed('bw', { sandbox: true, d: { station: 'welding', task: 'matching', device: 'weld-1', person: 'Box Wren', startAt: T('13:00'), lastSeenAt: T('13:05'), lastInputAt: T('13:00') } });
    seed('bl', { sandbox: true, d: { station: 'laser', device: 'charm-nest-1', person: 'Box Lars', startAt: T('13:00'), lastSeenAt: T('21:04'), lastInputAt: T('21:03') } });
    const r = await cron.handler({ httpMethod: 'POST', headers: {}, body: '{}' }); assert.strictEqual(r.statusCode, 200);
    assert.strictEqual(state('rw'), 'closing@21:00:00'); assert.strictEqual(state('rl'), 'closing@20:30:00'); assert.strictEqual(state('rp'), 'open', 'an Admin is exempt from 17:00');
    const sb = id => { const d = cur.get('Sandbox_Station_Sessions', id); return d.endAt ? d.endReason + '@' + iso(d.endAt).slice(11, 19) : 'open'; };
    assert.strictEqual(sb('bw'), 'closing@21:00:00'); assert.strictEqual(sb('bl'), 'open', 'a Sandbox_ Laser person with input a minute ago stays');
    assert(!cur.get('Station_Sessions', 'bw') && !cur.get('Sandbox_Station_Sessions', 'rw'), 'the two stores never mix');
    assert.strictEqual(s.count('Station_Sessions'), 3); assert.strictEqual(s.count('Sandbox_Station_Sessions'), 2, 'nothing deleted');
    for (const w of s.writes) assert.deepStrictEqual(w.keys.sort(), ['endAt', 'endReason', 'minutes']);
  });

  await check('6j the portal\'s words and hours: Welding ends at 17:00, Laser at its last input; endedText says what happened at that station', async () => {
    const att = async (name, now = Z('2026-10-05T23:30:00Z')) => { const a = await ATT.attendance({ db: dbNow, admin: { firestore: { Timestamp: Ts } }, now, today: '2026-10-05' }, { name, from: '2026-10-05', to: '2026-10-05' }); assert.strictEqual(a.ok, true); return a.calendar.find(x => x.day === '2026-10-05'); };
    fresh(); at('2026-10-05T23:30:00Z');
    row('h-w', 'welding', 'Wanda Welds', { startAt: T('13:00'), lastSeenAt: T('20:55'), lastInputAt: T('20:50') });
    row('h-wo', 'welding', 'Wendy Out', { startAt: T('13:00'), lastSeenAt: T('18:00'), lastInputAt: T('18:00'), endAt: T('18:10'), endReason: 'signOut', minutes: 310 });
    row('h-li', 'laser', 'Lena Idle', { startAt: T('13:00'), lastSeenAt: T('17:00'), lastInputAt: T('16:00') });          // idle 60 minutes at 16:00Z+60
    row('h-lc', 'laser', 'Lola Closing', { startAt: T('13:00'), lastSeenAt: T('21:00'), lastInputAt: T('20:25') });
    row('h-di', 'sorting', 'Dina Default', { startAt: T('13:00'), lastSeenAt: T('15:00'), lastInputAt: T('14:50') });
    let c = await att('Wanda Welds'); assert.strictEqual(c.endedBy, 'closing'); assert.strictEqual(c.endedText, 'Signed out at 5:00 pm'); assert.strictEqual(c.lastOut, T('21:00'), 'Out = 17:00 sharp'); assert.strictEqual(c.signedMs, 480 * MIN, 'eight hours: the day ends at 17:00');
    c = await att('Wendy Out'); assert.strictEqual(c.endedBy, 'signOut'); assert(!('endedText' in c), 'an explicit sign-out has no auto wording'); assert.strictEqual(c.lastOut, T('18:10'), 'her own sign-out stands');
    c = await att('Lena Idle'); assert.strictEqual(c.endedBy, 'idle'); assert.strictEqual(c.endedText, 'Signed out after 1 hour without input'); assert.strictEqual(c.lastOut, T('16:00'), 'Laser: Out = the last input'); assert.strictEqual(c.signedMs, 180 * MIN);
    c = await att('Lola Closing'); assert.strictEqual(c.endedBy, 'closing'); assert.strictEqual(c.endedText, 'Signed out at 5:00 pm after 30 minutes without input'); assert.strictEqual(c.lastOut, T('20:25'));
    c = await att('Dina Default'); assert.strictEqual(c.endedBy, 'idle'); assert.strictEqual(c.endedText, 'Signed out after 10 minutes without input'); assert.strictEqual(c.lastOut, T('14:50'));
    // the words themselves, and the Sign-ins window's short labels
    for (const [stn, reason, text, pill] of [['laser', 'idle', 'Signed out after 1 hour without input', '1 hour without input'], ['laser', 'closing', 'Signed out at 5:00 pm after 30 minutes without input', '5:00 pm · 30 min without input'],
      ['welding', 'closing', 'Signed out at 5:00 pm', '5:00 pm'], ['sorting', 'idle', 'Signed out after 10 minutes without input', '10 min without input'], ['sorting', 'closing', 'Signed out at 5:00 pm', '5:00 pm'], ['inbox', 'idle', 'Signed out after 10 minutes without input', '10 min without input'], ['design', 'closing', 'Signed out at 5:00 pm', '5:00 pm']]) {
      assert.strictEqual(AS.endText(stn, reason), text, stn + ' ' + reason); assert.strictEqual(AS.endPill(stn, reason), pill, stn + ' ' + reason + ' pill');
    }
    assert.strictEqual(AS.endText('welding', 'idle'), 'Signed out after 10 minutes without input', '(Welding has no idle sign-out; a page that is not up to date and ended one itself keeps the default words, which are what that page did)');
    assert.strictEqual(AS.endText('laser', 'signOut'), 'Signed out'); assert.strictEqual(AS.endPill('laser', 'signOut'), '');
  });

  const failed = results.filter(r => !r[1]);
  say(`\n${results.length - failed.length}/${results.length} passed`);
  if (failed.length) { say('FAILED: ' + failed.map(f => f[0]).join('; ')); process.exit(1); }
})().catch(e => { say('CRASH ' + ((e && e.stack) || e)); process.exit(1); });
