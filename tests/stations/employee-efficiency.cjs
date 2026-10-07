// The Employee efficiency console's read function (netlify/functions/employeeEfficiency.js). Offline: Firestore is an
// in-memory fake (typed fields, where/orderBy/limit, Timestamp), the clock is faked, every passcode here is synthetic.
//   1 · gate: keyless / wrong key 401 with no data, valid key 200, Firestore-held passcode, no passcode 403, 429 after
//       ten wrong keys, GET refused, the passcode never in a response or a log, config/editPasscode not read when the env wins
//   2 · day boundary (New York midnight), a session across midnight splits by day, default day is the New York day
//   3 · rollups + sessions merged: totals, rate, seconds per scan, on/out, name variants merged, no digit-only names
//   4 · delta cursor: only new events in the feed, 5 s cache, the shape stays complete
//   5 · partial fallback: sessions + seals only (no rollups), empty store, a failing collection, both failing
//   6 · sandbox separation, the person op, the orders op, response shapes, read sizes
//   node tests/stations/employee-efficiency.cjs
'use strict';
require(require('path').join(__dirname, '../../netlify/functions/_activityKinds.js')).NO_THROUGHPUT.clear();   // this suite uses 'welding' as a plain fixture station for the generic arithmetic: the Welding station's own rule (not counted in throughput, R2 of stations round 2) is tested in welding-portal.cjs

const path = require('path'), assert = require('assert'), Module = require('module');
const root = path.join(__dirname, '../..');
const clone = x => x == null ? x : JSON.parse(JSON.stringify(x));

/* ── fake Firestore ── */
class Ts { constructor(m) { this.m = m; } toMillis() { return this.m; } static fromMillis(m) { return new Ts(m); } }
const kind = v => v instanceof Ts ? 'ts' : typeof v === 'number' ? 'num' : typeof v === 'string' ? 'str' : 'other';
const val = v => v instanceof Ts ? v.m : v;
function fakeStore() {
  const colls = new Map(), reads = [], writes = [], failing = new Set();
  const data = n => { if (!colls.has(n)) colls.set(n, new Map()); return colls.get(n); };
  const keep = v => v instanceof Ts ? v : Array.isArray(v) ? v.map(keep) : v && typeof v === 'object' ? Object.fromEntries(Object.entries(v).map(([k, x]) => [k, keep(x)])) : v;
  function query(name, filters, order, lim) {
    return {
      where: (f, op, v) => query(name, filters.concat([[f, op, v]]), order, lim),
      orderBy: (f, d) => query(name, filters, [f, d || 'asc'], lim),
      limit: n => query(name, filters, order, n),
      get: async () => {
        if (failing.has(name)) throw Object.assign(new Error('14 UNAVAILABLE: synthetic outage of ' + name), { code: 14 });
        let docs = [...data(name)].map(([id, d]) => ({ id, d }));
        for (const [f, op, v] of filters) docs = docs.filter(({ d }) => {
          const x = d[f]; if (x === undefined || kind(x) !== kind(v)) return false;
          const a = val(x), b = val(v);
          return op === '==' ? a === b : op === '>=' ? a >= b : op === '>' ? a > b : op === '<' ? a < b : op === '<=' ? a <= b : false;
        });
        if (order) { const [f, dir] = order; docs = docs.filter(({ d }) => d[f] !== undefined).sort((p, q) => (val(p.d[f]) < val(q.d[f]) ? -1 : val(p.d[f]) > val(q.d[f]) ? 1 : 0) * (dir === 'desc' ? -1 : 1)); }
        if (lim != null) docs = docs.slice(0, lim);
        reads.push({ name, n: docs.length, filters: filters.map(f => f[0] + f[1]), order: order && order[0] });
        return { docs: docs.map(({ id, d }) => ({ id, data: () => keep(d) })), size: docs.length, empty: !docs.length };
      }
    };
  }
  const db = { collection: name => Object.assign(query(name, [], null, null), {
    doc: id => ({ id,
      get: async () => { reads.push({ name, doc: id }); if (failing.has(name)) throw new Error('14 UNAVAILABLE'); const d = data(name).get(id); return { exists: !!d, data: () => keep(d) }; },
      set: async v => { writes.push([name, id]); data(name).set(id, keep(v)); },
      create: async v => { writes.push([name, id]); if (data(name).has(id)) throw Object.assign(new Error('6 ALREADY_EXISTS'), { code: 6 }); data(name).set(id, keep(v)); } }) }) };
  return { db, put: (name, id, d) => data(name).set(id, keep(d)), reads, writes, fail: n => failing.add(n), heal: n => failing.delete(n), count: n => data(name).size, colls,
    readsOf: name => reads.filter(r => r.name === name) };
}

/* ── the module under test, over the fake admin ── */
const fakeAdmin = { firestore: Object.assign(() => ({}), { Timestamp: Ts, FieldValue: { serverTimestamp: () => 'ts' } }) };
const realLoad = Module._load;
Module._load = function (req, ...rest) { if (/[\/]firebaseAdmin(\.js)?$/.test(req)) return fakeAdmin; return realLoad.call(this, req, ...rest); };
const mod = require(path.join(root, 'netlify/functions/employeeEfficiency.js'));
const EP = require(path.join(root, 'netlify/functions/_editPasscode.js'));
Module._load = realLoad;
const T = mod._t;

const PASS = 'synthetic-pass-9f3k', PASS2 = 'synthetic-firestore-pass-7q';
process.env.EDIT_PASSCODE = PASS;
const logs = [], keepLog = (...a) => logs.push(a.map(x => typeof x === 'string' ? x : JSON.stringify(x)).join(' '));
console.warn = console.log = console.error = console.info = keepLog;      // (the test's own output goes through say())
const say = (...a) => process.stdout.write(a.join(' ') + '\n');
const bodies = [];
const realNow = Date.now; let NOW = Date.parse('2026-10-03T14:00:00Z');       // 10:00 on 3 Oct in New York (EDT)
Date.now = () => NOW;
const MID3 = Date.parse('2026-10-03T04:00:00Z'), MID2 = Date.parse('2026-10-02T04:00:00Z');   // New York midnights
const Z = iso => Date.parse(iso);
let ipN = 0;
async function call(st, body, o = {}) {
  const r = await T.handle({ httpMethod: o.method || 'POST', headers: { 'x-nf-client-connection-ip': o.ip || '203.0.113.' + (++ipN) }, body: JSON.stringify(Object.assign({ op: 'overview', key: PASS }, body)) }, st.db);
  bodies.push(r.body);
  return { status: r.statusCode, headers: r.headers, body: JSON.parse(r.body || '{}') };
}
const fresh = () => { EP.resetCache(); return fakeStore(); };
const tick = ms => { NOW += ms; };

/* ── builders ── */
const sess = (id, person, station, start, o = {}) => ({ id, person, station, device: station + '-1', computerId: 'pc-' + id.slice(0, 8).padEnd(8, 'X'), computerLabel: '', startAt: start, lastSeenAt: o.last == null ? start : o.last, endAt: o.end == null ? null : o.end, endReason: o.reason || null, minutes: 0 });
const roll = (day, person, stations, hours, touched, firstAt, lastAt) => ({ day, person, v: 1, events: 10, firstAt, lastAt, stations, hours, touched });
const stat = o => Object.assign({ scans: 0, scanParts: 0, completes: 0, parts: 0, orders: 0, prints: 0, rejects: 0, errors: 0, notes: 0, undos: 0, undoParts: 0, undoOrders: 0, activeMs: 0, idleMs: 0 }, o);
const ev = (id, person, station, action, at, o = {}) => ({ id, station, device: station + '-1', computer: 'pc-AAAA', session: '', person, action, orderId: o.orderId || '', line: '', sku: '', parts: o.parts || 0, orders: o.orders || 0, detail: o.detail || '', at, seq: 1, sincePrevMs: o.since || 0,
  ts: Ts.fromMillis(o.server || at), serverAt: o.server || at, day: o.day || T.nyDay(at), hour: '00', v: 1 });
const seal = (id, orderId, type, by, station, at, milestone) => ({ orderId, type, at, by, source: 'station', station, device: station + '-1', milestone: !!milestone, text: 'x' });

/* ── 1 · the gate ── */
async function gate() {
  const st = fresh();
  st.put('Efficiency_Daily', '2026-10-03__Tess Welder', roll('2026-10-03', 'Tess Welder', { welding: stat({ parts: 5 }) }, {}, {}, 1, 2));
  for (const [what, body] of [['no key', { key: undefined }], ['empty key', { key: '' }], ['wrong key', { key: 'nope' }], ['a number', { key: 12345 }], ['the key of a prefix', { key: PASS.slice(0, -1) }], ['a longer key', { key: PASS + 'x' }]]) {
    const r = await call(st, body);
    assert.strictEqual(r.status, 401, what);
    assert.deepStrictEqual(r.body, { ok: false, error: 'unauthorized' }, what + ': no data');
  }
  assert.strictEqual(st.reads.length, 0, 'a refused request reads nothing');
  let r = await call(st, {});
  assert.strictEqual(r.status, 200); assert.strictEqual(r.body.ok, true);
  assert.strictEqual((await call(st, { key: '  ' + PASS + ' ' })).status, 200, 'surrounding spaces are trimmed (as the Ads console does)');
  assert.strictEqual((await call(st, {}, { method: 'GET' })).status, 405, 'GET (a key in a URL) is refused');
  assert.strictEqual((await T.handle({ httpMethod: 'OPTIONS', headers: {}, body: '' }, st.db)).statusCode, 204);
  assert.strictEqual((await call(st, { op: 'drop' })).status, 400, 'an unknown op, with a good key');
  assert.strictEqual((await call(st, { op: 'drop', key: 'bad' })).status, 401, 'an unknown op with a bad key is 401 first');
  // ten wrong keys a minute from one address
  const ip = '198.51.100.7';
  for (let i = 0; i < 10; i++) assert.strictEqual((await call(st, { key: 'bad' + i }, { ip })).status, 401);
  assert.strictEqual((await call(st, { key: 'bad' }, { ip })).status, 429, 'the eleventh');
  assert.strictEqual((await call(st, { key: PASS }, { ip })).status, 429, 'even the right key, for that minute');
  assert.strictEqual((await call(st, {})).status, 200, 'another address is unaffected');
  tick(61000);
  assert.strictEqual((await call(st, { key: PASS }, { ip })).status, 200, 'and a minute later it is open again');
  assert.strictEqual(st.readsOf('config').filter(r => r.doc === 'editPasscode').length, 0, 'EDIT_PASSCODE wins: config/editPasscode is not read');
  assert(st.readsOf('config').every(r => r.doc === 'employeeAliases') && st.readsOf('config').length <= 2, 'the alias doc is read after the gate, cached for a minute');
  // the passcode kept in Firestore (no env)
  delete process.env.EDIT_PASSCODE;
  const s2 = fresh(); s2.put('config', 'editPasscode', { passcode: PASS2 });
  assert.strictEqual((await call(s2, { key: PASS })).status, 401, 'the old env value is not accepted');
  assert.strictEqual((await call(s2, { key: PASS2 })).status, 200, 'the Firestore passcode opens it');
  await call(s2, { key: PASS2 }); await call(s2, { key: 'x' });
  assert.strictEqual(s2.readsOf('config').filter(r => r.doc === 'editPasscode').length, 1, 'read once, cached for the minute');
  const s3 = fresh(); s3.put('config', 'editPasscode', { passcode: '' });
  const r3 = await call(s3, { key: 'anything' });
  assert.strictEqual(r3.status, 403); assert.strictEqual(r3.body.code, 'EDIT_PASSCODE_NOT_SET'); assert.strictEqual(r3.body.people, undefined);
  const s4 = fresh(); s4.fail('config');
  assert.strictEqual((await call(s4, { key: 'anything' })).status, 403, 'a Firestore error is closed, not open');
  process.env.EDIT_PASSCODE = PASS; EP.resetCache();
  say('gate: 401 no data (no/empty/wrong/number/prefix/longer), 200 valid and trimmed, GET 405, bad op 400, 429 after ten wrong, Firestore passcode (one cached read), 403 unset/error, env wins without a read');
}

/* ── 2 · day boundary ── */
async function dayBoundary() {
  const st = fresh();
  NOW = Z('2026-10-03T02:00:00Z');                                  // 22:00 on 2 Oct in New York, already 3 Oct in UTC
  let r = await call(st, {});
  assert.strictEqual(r.body.day, '2026-10-02', 'the default day is the New York day, not the UTC day');
  assert.strictEqual(T.nyDay(Z('2026-10-03T03:59:59Z')), '2026-10-02'); assert.strictEqual(T.nyDay(Z('2026-10-03T04:00:00Z')), '2026-10-03');
  assert.strictEqual(T.nyMidnight('2026-10-03'), MID3);
  assert.strictEqual(T.nyMidnight('2026-11-02') - T.nyMidnight('2026-11-01'), 25 * 3600e3, 'the fall-back day is 25 hours');
  assert.strictEqual(T.nyMidnight('2026-03-09') - T.nyMidnight('2026-03-08'), 23 * 3600e3, 'the spring-forward day is 23 hours');
  assert.deepStrictEqual(T.clip(Z('2026-10-03T03:00:00Z'), Z('2026-10-03T05:00:00Z'), '2026-10-01', '2026-10-04').map(c => [c.day, (c.e - c.s) / 60000]), [['2026-10-02', 60], ['2026-10-03', 60]], 'a session across midnight splits 60 / 60');
  assert.deepStrictEqual(T.clip(Z('2026-10-03T03:00:00Z'), MID3, '2026-10-01', '2026-10-04').map(c => c.day), ['2026-10-02'], 'one that ends at midnight puts nothing on the next day');
  assert.strictEqual(T.covered([[0, 60000], [30000, 90000], [200000, 260000]]), 150000, 'an overlap counts once');
  // a legacy session that crosses midnight (the server now ends it at midnight), seen after both days
  NOW = Z('2026-10-03T14:00:00Z');
  st.put('Station_Sessions', 'night-1', sess('night-1', 'Nina Night', 'sorting', Z('2026-10-03T03:00:00Z'), { end: Z('2026-10-03T05:00:00Z'), last: Z('2026-10-03T05:00:00Z'), reason: 'signOut' }));
  const d2 = (await call(st, { day: '2026-10-02' })).body, d3 = (await call(st, { day: '2026-10-03' })).body;
  assert.strictEqual(d2.people[0].totals.signedInMin, 60, 'the 2nd gets the hour before midnight'); assert.strictEqual(d2.people[0].lastOut, MID3, 'and ends at midnight');
  assert.strictEqual(d3.people[0].totals.signedInMin, 60, 'the 3rd gets the hour after'); assert.strictEqual(d3.people[0].firstIn, MID3, 'from midnight');
  assert.strictEqual(d3.people[0].lastOut, Z('2026-10-03T05:00:00Z'));
  const w = (await call(st, { day: '2026-10-03', days: 7 })).body;
  assert.strictEqual(w.people[0].totals.signedInMin, 120, 'seven days: both days'); assert.strictEqual(w.people[0].inDay, '2026-10-03', 'in/out belong to the latest day in');
  assert.strictEqual((await call(st, { day: '2099-01-01' })).body.day, '2026-10-03', 'a future day is today');
  assert.strictEqual((await call(st, { day: '03/10/2026' })).status, 400, 'a bad day');
  assert.strictEqual((await call(st, { day: '2026-02-31' })).status, 400, 'an impossible day');
  assert.strictEqual((await call(st, { days: 3 })).body.days, 7); assert.strictEqual((await call(st, { days: 99 })).body.days, 30); assert.strictEqual((await call(st, { days: 0 })).body.days, 1);
  assert.strictEqual(w.business.trend.length, 14, 'a trend of 14 days'); assert.strictEqual(w.business.trend[13].day, '2026-10-03'); assert.strictEqual(w.business.trend[0].day, '2026-09-20');
  assert.strictEqual((await call(st, { day: '2026-10-03', days: 30 })).body.business.trend.length, 30, 'the 30-day range is its own trend');
  // an open session is counted to now, and a quiet one stops at its last beat
  const s2 = fresh(); NOW = Z('2026-10-03T14:00:00Z');
  s2.put('Station_Sessions', 'live-1', sess('live-1', 'Liv Live', 'welding', Z('2026-10-03T13:00:00Z'), { last: Z('2026-10-03T13:58:00Z') }));
  s2.put('Station_Sessions', 'stale-1', sess('stale-1', 'Stan Stale', 'assembly', Z('2026-10-03T12:00:00Z'), { last: Z('2026-10-03T12:30:00Z') }));
  s2.put('Station_Sessions', 'stale-2', sess('stale-2', 'Wes Weld', 'welding', Z('2026-10-03T12:00:00Z'), { last: Z('2026-10-03T12:30:00Z') }));       // (Welding has no idle sign-out: a quiet page is signed in until 17:00 Toronto, AD4)
  const o = (await call(s2, {})).body, by = n => o.people.find(p => p.name === n);
  assert.strictEqual(by('Liv Live').totals.signedInMin, 60, 'open: to now'); assert.strictEqual(by('Liv Live').status, 'on');
  assert.strictEqual(by('Stan Stale').totals.signedInMin, 30, 'quiet for 15 minutes: closed at the last beat'); assert.strictEqual(by('Stan Stale').status, 'out');
  assert.strictEqual(by('Stan Stale').lastOut, Z('2026-10-03T12:30:00Z'));
  assert.strictEqual(by('Wes Weld').totals.signedInMin, 120, 'a quiet Welding page is still signed in (to now) before 17:00'); assert.strictEqual(by('Wes Weld').status, 'on');
  say('day boundary: NY default day, DST day lengths, 60/60 split, midnight-ending span, overlap once, future/bad days, trend length, open/stale sessions');
}

/* ── 3 · rollups + sessions ── */
function dataset() {
  const st = fresh(); NOW = Z('2026-10-03T14:00:00Z');
  const T0 = Z('2026-10-03T12:00:00Z');                                  // 08:00 New York
  st.put('Station_Sessions', 'weld-t1', sess('weld-t1', 'Tess Welder', 'welding', T0, { last: Z('2026-10-03T13:58:00Z') }));
  st.put('Station_Sessions', 'ray-1', sess('ray-1', 'Ray Welder', 'welding', Z('2026-10-03T11:00:00Z'), { end: Z('2026-10-03T13:00:00Z'), last: Z('2026-10-03T13:00:00Z'), reason: 'signOut' }));
  st.put('Station_Sessions', 'pin-1', sess('pin-1', '123456', 'welding', T0, { last: Z('2026-10-03T13:59:00Z') }));
  st.put('Station_Sessions', 'ann-1', sess('ann-1', 'Ann Assembler', 'assembly', Z('2026-10-03T11:00:00Z'), { last: Z('2026-10-03T13:50:00Z') }));
  st.put('Efficiency_Daily', '2026-10-03__Tess Welder', roll('2026-10-03', 'Tess Welder', {
    welding: stat({ scans: 40, scanParts: 40, completes: 4, parts: 60, orders: 4, prints: 2, rejects: 1, errors: 1, undos: 1, undoParts: 5, activeMs: 3600000, idleMs: 1200000, firstAt: T0 + 5 * 60000, lastAt: T0 + 115 * 60000 }) },
    { '08': { parts: 20, scans: 15, by: { welding: { parts: 20, scans: 15 } } }, '09': { parts: 40, scans: 25, by: { welding: { parts: 40, scans: 25 } } } },
    { 3521000001: { welding: true }, 3521000002: { welding: true }, 3521000003: { welding: true }, 3521000004: { welding: true } }, T0 + 5 * 60000, T0 + 115 * 60000));
  st.put('Efficiency_Daily', '2026-10-03__ray  welder', roll('2026-10-03', 'ray  welder', { welding: stat({ scans: 10, scanParts: 10, completes: 2, parts: 20, orders: 2, activeMs: 1800000, idleMs: 0, firstAt: Z('2026-10-03T11:10:00Z'), lastAt: Z('2026-10-03T12:50:00Z') }) },
    { '07': { parts: 20, scans: 10, by: { welding: { parts: 20, scans: 10 } } } }, { 3521000004: { welding: true }, 3521000005: { welding: true } }, Z('2026-10-03T11:10:00Z'), Z('2026-10-03T12:50:00Z')));
  st.put('Efficiency_Daily', '2026-10-03__123456', roll('2026-10-03', '123456', { welding: stat({ parts: 999 }) }, {}, {}, 1, 2));
  st.put('Efficiency_Daily', '2026-10-02__Tess Welder', roll('2026-10-02', 'Tess Welder', { welding: stat({ parts: 30, scans: 5, scanParts: 5, completes: 3, orders: 3, activeMs: 1800000 }) }, { '10': { parts: 30, scans: 5, by: { welding: { parts: 30, scans: 5 } } } }, { 3520000001: { welding: true } }, 1, 2));
  st.put('Order_Timeline', '3521000010~assembled~a', seal('a', '3521000010', 'assembled', 'Ann Assembler', 'assembly', Z('2026-10-03T13:00:00Z'), true));
  st.put('Order_Timeline', '3521000011~scan~a', seal('b', '3521000011', 'scan', 'Ann Assembler', 'assembly', Z('2026-10-03T13:05:00Z'), false));
  st.put('Order_Timeline', '3521000012~arrived~a', seal('c', '3521000012', 'arrived', 'Etsy', 'sorter', Z('2026-10-03T13:05:00Z'), true));
  const e1 = ev('weld-1_AAAA_1_1', 'Tess Welder', 'welding', 'scan', T0 + 100 * 60000, { orderId: '3521000001', parts: 1, since: 20000 });
  const e2 = ev('weld-1_AAAA_2_2', 'Tess Welder', 'welding', 'complete', T0 + 101 * 60000, { orderId: '3521000001', parts: 10, orders: 1, since: 60000 });
  const e3 = ev('weld-1_BBBB_1_3', 'ray welder', 'welding', 'complete', T0 + 102 * 60000, { orderId: '3521000005', parts: 4, orders: 1, since: 30000 });
  st.put('Station_Activity', e1.id, e1); st.put('Station_Activity', e2.id, e2); st.put('Station_Activity', e3.id, e3);
  return st;
}
async function merge() {
  const st = dataset();
  const r = await call(st, {});
  assert.strictEqual(r.status, 200); const b = r.body;
  assert.deepStrictEqual(b.people.map(p => p.name).sort(), ['Ann Assembler', 'Ray Welder', 'Tess Welder'], 'one Ray (case/space variants merged), no digit-only name');
  assert(!JSON.stringify(b).includes('123456') && !JSON.stringify(b).includes('999'), 'a digits-only name is never data');
  const by = n => b.people.find(p => p.name === n), tess = by('Tess Welder'), ray = by('Ray Welder'), ann = by('Ann Assembler');
  assert.strictEqual(b.people[0].status, 'on', 'people who are on come first');
  // Tess: rollup + a live session
  assert.strictEqual(tess.status, 'on'); assert.strictEqual(tess.onSince, Z('2026-10-03T12:00:00Z')); assert.deepStrictEqual(tess.nowAt, ['welding']);
  assert.strictEqual(tess.firstIn, Z('2026-10-03T12:00:00Z')); assert.strictEqual(tess.lastOut, null, 'no lock-out while on'); assert.strictEqual(tess.inDay, '2026-10-03');
  assert.strictEqual(tess.source, 'events');
  assert.deepStrictEqual(tess.totals, { parts: 55, scanParts: 40, scans: 40, orders: 4, rejects: 1, errors: 1, activeMin: 60, idleMin: 20, signedInMin: 120, rate: 55, secPerScan: 90 }, 'net parts (60-5), parts per ACTIVE hour, seconds per scan');
  assert.deepStrictEqual(tess.stations, [{ station: 'welding', minutes: 120, parts: 55, scanParts: 40, scans: 40, completes: 4, prints: 2, orders: 4 }]);
  assert.strictEqual(tess.perHour.length, 24); assert.strictEqual(tess.perHour[8], 20); assert.strictEqual(tess.perHour[9], 40); assert.strictEqual(tess.perHour.reduce((a, c) => a + c, 0), 60);
  assert.deepStrictEqual(tess.orders, [{ orderId: '3521000001', stations: ['welding'], parts: 10, lastAt: Z('2026-10-03T12:00:00Z') + 101 * 60000 }], 'her order from the events');
  // Ray: merged variants, signed out
  assert.strictEqual(ray.status, 'out'); assert.strictEqual(ray.onSince, null); assert.strictEqual(ray.firstIn, Z('2026-10-03T11:00:00Z')); assert.strictEqual(ray.lastOut, Z('2026-10-03T13:00:00Z'));
  assert.strictEqual(ray.totals.signedInMin, 120); assert.strictEqual(ray.totals.parts, 20); assert.strictEqual(ray.totals.rate, 40); assert.strictEqual(ray.totals.secPerScan, 180);
  assert.strictEqual(ray.orders[0].orderId, '3521000005');
  // Ann: no rollup -> the seals (orders and scans, never parts)
  assert.strictEqual(ann.source, 'seals'); assert.strictEqual(ann.totals.parts, 0); assert.strictEqual(ann.totals.scans, 1); assert.strictEqual(ann.totals.orders, 2); assert.strictEqual(ann.totals.signedInMin, 180);
  assert.strictEqual(ann.stations[0].completes, 1); assert.strictEqual(ann.perHour[9], 1, 'seals count order steps by hour (13:00Z = 09:00)'); assert.strictEqual(ann.status, 'on');
  assert.deepStrictEqual(b.sources, { events: true, seals: true, sessions: true });
  assert(b.notes.some(n => /seals/.test(n)), 'the seals are labelled'); assert.strictEqual(b.partial, undefined, 'events exist: not partial');
  // business: distinct orders, per station, now, trend
  assert.deepStrictEqual(b.business.totals, { parts: 75, scans: 51, orders: 7, people: 3 }, 'orders are distinct (3521000004 is both Tess and Ray)');
  const ws = b.business.stations.find(s => s.station === 'welding'), as = b.business.stations.find(s => s.station === 'assembly');
  assert.deepStrictEqual(ws, { station: 'welding', parts: 75, scans: 50, orders: 5, peopleNow: ['Tess Welder'] });
  assert.deepStrictEqual(as.peopleNow, ['Ann Assembler']); assert.strictEqual(as.orders, 2);
  assert.deepStrictEqual(b.business.stations.map(s => s.station).slice(0, 4), ['sorting', 'welding', 'assembly', 'shipping'], 'the four production stations are always listed');
  assert.deepStrictEqual(Object.keys(b.business.perHour).sort(), ['assembly', 'welding'], 'per-station arrays only where there is data');
  assert.strictEqual(b.business.perHour.welding[8], 20); assert.strictEqual(b.business.perHour.welding[7], 20); assert.strictEqual(b.business.perHour.welding.length, 24);
  const tr = b.business.trend, last = tr[tr.length - 1], prev = tr[tr.length - 2];
  assert.deepStrictEqual(last, { day: '2026-10-03', parts: 75, orders: 7, people: 3, source: 'events' });
  assert.deepStrictEqual(prev, { day: '2026-10-02', parts: 30, orders: 1, people: 1, source: 'events' });
  assert.strictEqual(tr[0].source, 'none');
  // seven days adds yesterday's rollup
  const w = (await call(st, { days: 7 })).body, t7 = w.people.find(p => p.name === 'Tess Welder');
  assert.strictEqual(t7.totals.parts, 85); assert.strictEqual(t7.totals.orders, 5); assert.strictEqual(t7.perHour[10], 30); assert.strictEqual(t7.totals.activeMin, 90);
  assert.strictEqual(t7.inDay, '2026-10-03');
  // yesterday on its own: nobody is "on" in a past day
  const y = (await call(st, { day: '2026-10-02' })).body;
  assert.strictEqual(y.people.length, 1); assert.strictEqual(y.people[0].status, 'out'); assert.strictEqual(y.people[0].totals.parts, 30); assert.strictEqual(y.people[0].totals.signedInMin, 0);
  say('merge: rollup + sessions totals, net parts, rate, sec/scan, on/out, variants merged, no digit names, seals for a person without a rollup, distinct business orders, trend, 7 days, past day');
}

/* ── 4 · delta cursor and the cache ── */
async function delta() {
  const st = dataset();
  const a = (await call(st, {})).body;
  assert.strictEqual(a.delta, false); assert.strictEqual(a.feed.length, 3, 'a full answer carries the newest events'); assert.strictEqual(a.feed[0].person, 'Ray Welder', 'newest first, under the person\'s one display name');
  assert.deepStrictEqual(Object.keys(a.feed[0]).sort(), ['action', 'at', 'id', 'orderId', 'parts', 'person', 'station']);
  assert(/^\d+~.+/.test(a.cursor));
  const dataReads = () => st.reads.filter(r => r.name !== 'Station_Rev').length;      // (the revision probe, FC5: one tiny document, is not data)
  const reads = dataReads(); await call(st, {}); await call(st, {}); await call(st, { after: a.cursor });
  assert.strictEqual(dataReads(), reads, 'inside 5 seconds nothing is read again');
  tick(6000);
  const none = (await call(st, { after: a.cursor })).body;
  assert.strictEqual(none.delta, true); assert.deepStrictEqual(none.feed, [], 'nothing new: an empty feed'); assert.strictEqual(none.cursor, a.cursor);
  assert.strictEqual(none.people.length, a.people.length, 'the rest of the answer is still complete');
  const E4 = ev('weld-1_AAAA_3_4', 'Tess Welder', 'welding', 'complete', NOW - 1000, { orderId: '3521000002', parts: 7, orders: 1, since: 30000, server: NOW - 500 });
  st.put('Station_Activity', E4.id, E4);
  st.put('Efficiency_Daily', '2026-10-03__Tess Welder', roll('2026-10-03', 'Tess Welder', { welding: stat({ scans: 40, scanParts: 40, completes: 5, parts: 67, orders: 5, undos: 1, undoParts: 5, activeMs: 3660000, idleMs: 1200000 }) }, { '08': { parts: 27, by: { welding: { parts: 27 } } } }, { 3521000001: { welding: true }, 3521000002: { welding: true } }, 1, 2));
  tick(6000);
  const d = (await call(st, { after: a.cursor })).body;
  assert.deepStrictEqual(d.feed.map(f => f.id), [E4.id], 'only the new event');
  assert.strictEqual(d.cursor, NOW - 6000 - 500 + '~' + E4.id, 'the cursor moves to it');
  assert.strictEqual(d.people.find(p => p.name === 'Tess Welder').totals.parts, 62, 'and the people reflect the new rollup (67 - 5)');
  assert.strictEqual(d.people.find(p => p.name === 'Tess Welder').orders.length, 2, 'her orders list gained the new order');
  tick(6000);
  assert.deepStrictEqual((await call(st, { after: d.cursor })).body.feed, [], 'caught up');
  const full = (await call(st, {})).body; assert.strictEqual(full.feed[0].id, E4.id);
  assert.strictEqual((await call(st, { after: 'garbage' })).body.delta, false, 'a bad cursor is a normal answer'); assert.strictEqual((await call(st, { after: 12 })).body.delta, false);
  // the instance keeps the newest events: a top-up reads only a few documents, not the window again
  const act = st.readsOf('Station_Activity'); assert.strictEqual(act[0].order, 'ts'); assert(act.slice(1).every(r => r.filters.includes('ts>=') && r.n <= 3), 'later reads are top-ups');
  say('delta: complete shape each poll, feed only newer than the cursor, cursor moves, 5 s cache reads nothing, top-ups read a few events');
}

/* ── 5 · partial fallbacks ── */
async function partial() {
  // sessions + seals only: the foundation data has not started
  const st = fresh(); NOW = Z('2026-10-03T14:00:00Z');
  st.put('Station_Sessions', 's1', sess('s1', 'Sam Sorter', 'sorting', Z('2026-10-03T12:00:00Z'), { last: Z('2026-10-03T13:55:00Z') }));
  st.put('Station_Sessions', 's2', sess('s2', 'Wendy Welder', 'welding', Z('2026-10-02T13:00:00Z'), { end: Z('2026-10-02T17:00:00Z'), last: Z('2026-10-02T17:00:00Z'), reason: 'signOut' }));
  st.put('Order_Timeline', 'a', seal('a', '3521000020', 'sorted', 'Sam Sorter', 'sorting', Z('2026-10-03T12:30:00Z'), true));
  st.put('Order_Timeline', 'b', seal('b', '3521000021', 'sorted', 'sam  sorter', 'sorting', Z('2026-10-03T12:40:00Z'), true));
  st.put('Order_Timeline', 'c', seal('c', '3521000021', 'labelPrinted', 'Sam Sorter', 'sorting', Z('2026-10-03T12:41:00Z'), false));
  st.put('Order_Timeline', 'd', seal('d', '3521000021', 'scan', 'Sam Sorter', 'sorting', Z('2026-10-03T12:20:00Z'), false));
  st.put('Order_Timeline', 'e', seal('e', '3521000099', 'sorted', '654321', 'sorting', Z('2026-10-03T12:20:00Z'), true));
  st.put('Order_Timeline', 'f', seal('f', '3521000098', 'queued', 'Sam Sorter', 'sorting', Z('2026-10-03T12:20:00Z'), false));
  st.put('Order_Timeline', 'g', seal('g', '3521000020', 'welded', 'Wendy Welder', 'welding', Z('2026-10-02T15:00:00Z'), true));
  let r = await call(st, { days: 7 }); const b = r.body;
  assert.strictEqual(r.status, 200); assert.strictEqual(b.partial, true); assert.deepStrictEqual(b.sources, { events: false, seals: true, sessions: true });
  assert(b.notes.some(n => /not been recorded/.test(n)), 'says the events have not started');
  const sam = b.people.find(p => p.name === 'Sam Sorter'), wen = b.people.find(p => p.name === 'Wendy Welder');
  assert.strictEqual(sam.source, 'seals'); assert.strictEqual(sam.totals.orders, 2); assert.strictEqual(sam.totals.parts, 0); assert.strictEqual(sam.totals.scans, 1); assert.strictEqual(sam.stations[0].prints, 1);
  assert.strictEqual(sam.totals.signedInMin, 120); assert.strictEqual(sam.status, 'on'); assert(!JSON.stringify(b).includes('654321'), 'a digits-only seal name is dropped');
  assert.strictEqual(wen.source, 'seals'); assert.strictEqual(wen.totals.signedInMin, 240); assert.strictEqual(wen.status, 'out');
  assert.strictEqual(b.business.totals.parts, 0); assert.strictEqual(b.business.totals.orders, 2, 'order 3521000020 was sorted by Sam and welded by Wendy: it counts once');
  assert.deepStrictEqual(b.business.trend.slice(-2).map(t => t.source), ['seals', 'seals']);
  assert.deepStrictEqual(b.feed, [], 'no events: an empty feed, not an error'); assert.strictEqual(b.cursor, '0~');
  // an empty store: still a complete, honest answer
  const e = fresh(); const er = await call(e, {});
  assert.strictEqual(er.status, 200); assert.deepStrictEqual(er.body.people, []); assert.strictEqual(er.body.partial, true); assert.deepStrictEqual(er.body.sources, { events: false, seals: false, sessions: false });
  assert.strictEqual(er.body.business.trend.length, 14); assert.strictEqual(er.body.business.stations.length, 4);
  // events exist but one source fails: partial with errors, the rest still shown
  const f = dataset(); f.fail('Station_Sessions'); const fr = (await call(f, {})).body;
  assert.strictEqual(fr.partial, true); assert(fr.errors.some(x => /^sessions: /.test(x))); assert(fr.people.find(p => p.name === 'Tess Welder').totals.parts === 55, 'rollups still shown'); assert.strictEqual(fr.people[0].totals.signedInMin, 0);
  const g = dataset(); g.fail('Efficiency_Daily'); const gr = (await call(g, {})).body;
  assert.strictEqual(gr.partial, true); assert(gr.errors.some(x => /^rollups: /.test(x))); assert(gr.people.length >= 2, 'sessions still shown');
  assert.strictEqual(gr.sources.events, false);
  const h = dataset(); h.fail('Efficiency_Daily'); h.fail('Station_Sessions'); const hr = await call(h, {});
  assert.strictEqual(hr.status, 503); assert.strictEqual(hr.body.ok, false); assert.strictEqual(hr.body.people, undefined);
  const k = dataset(); k.fail('Order_Timeline'); const kr = (await call(k, {})).body;
  assert(kr.errors.some(x => /^seals: /.test(x)) && kr.partial === true && kr.people.length === 3, 'a failing seals read leaves the rest');
  const ne = dataset(); ne.fail('Station_Activity'); const nr = (await call(ne, {})).body;
  assert(nr.errors.some(x => /^events: /.test(x)) && nr.feed.length === 0 && nr.people.length === 3, 'a failing events read leaves the rest');
  say('partial: sessions + seals only (labelled, parts 0), empty store, one source failing (rollups / sessions / seals / events), both failing 503');
}

/* ── 6 · sandbox, person, orders, shapes, sizes ── */
async function rest() {
  const st = dataset();
  st.put('Sandbox_Station_Sessions', 'sb-1', sess('sb-1', 'Sandy Sandbox', 'welding', Z('2026-10-03T13:00:00Z'), { last: Z('2026-10-03T13:55:00Z') }));
  st.put('Sandbox_Efficiency_Daily', '2026-10-03__Sandy Sandbox', Object.assign(roll('2026-10-03', 'Sandy Sandbox', { welding: stat({ parts: 500, completes: 5, orders: 5, activeMs: 3600000 }) }, {}, { 3529999999: { welding: true } }, 1, 2), { sandbox: true }));
  const se = ev('sb_AAAA_1_1', 'Sandy Sandbox', 'welding', 'complete', NOW - 5000, { orderId: '3529999999', parts: 500, orders: 1 }); se.sandbox = true; st.put('Sandbox_Station_Activity', se.id, se);
  const prod = (await call(st, {})).body, sand = (await call(st, { sandbox: true })).body;
  assert(!prod.people.some(p => p.name === 'Sandy Sandbox') && !JSON.stringify(prod).includes('3529999999'), 'production never shows the sandbox');
  assert.deepStrictEqual(sand.people.map(p => p.name), ['Sandy Sandbox'], 'the sandbox shows only its own'); assert.strictEqual(sand.people[0].totals.parts, 500); assert.strictEqual(sand.feed.length, 1);
  assert(!JSON.stringify(sand).includes('Tess Welder'), 'and none of production');
  assert.strictEqual((await call(st, { sandbox: '1' })).body.people.length, 1);
  assert.strictEqual((await call(st, { op: 'orders', orderId: '3529999999', sandbox: true })).body.steps.length, 1); assert.strictEqual((await call(st, { op: 'orders', orderId: '3529999999' })).body.steps.length, 0);

  // shapes
  const o = prod, isNum = v => typeof v === 'number' && Number.isFinite(v);
  assert.deepStrictEqual(Object.keys(o).sort(), ['business', 'cursor', 'day', 'days', 'delta', 'feed', 'now', 'notes', 'ok', 'people', 'sources'].sort());
  assert(isNum(o.now) && o.day === '2026-10-03' && o.days === 1 && typeof o.cursor === 'string');
  for (const p of o.people) {
    assert.deepStrictEqual(Object.keys(p).sort(), ['firstIn', 'inDay', 'lastOut', 'name', 'nowAt', 'onSince', 'orders', 'perHour', 'source', 'stations', 'status', 'totals'].sort());
    assert(p.status === 'on' || p.status === 'out'); assert.strictEqual(p.perHour.length, 24); assert(p.perHour.every(isNum));
    assert.deepStrictEqual(Object.keys(p.totals).sort(), ['activeMin', 'errors', 'idleMin', 'orders', 'parts', 'rate', 'rejects', 'scanParts', 'scans', 'secPerScan', 'signedInMin']); assert(Object.values(p.totals).every(isNum));
    assert(p.orders.length <= 30); for (const s of p.stations) { assert.deepStrictEqual(Object.keys(s).sort(), ['completes', 'minutes', 'orders', 'parts', 'prints', 'scanParts', 'scans', 'station']); assert(Object.values(s).every(v => typeof v === 'string' || isNum(v))); }
  }
  assert.deepStrictEqual(Object.keys(o.business).sort(), ['perHour', 'stations', 'totals', 'trend']);
  assert.deepStrictEqual(Object.keys(o.business.totals).sort(), ['orders', 'parts', 'people', 'scans']);
  for (const t of o.business.trend) assert.deepStrictEqual(Object.keys(t).sort(), ['day', 'orders', 'parts', 'people', 'source']);
  for (const s of o.business.stations) assert.deepStrictEqual(Object.keys(s).sort(), ['orders', 'parts', 'peopleNow', 'scans', 'station']);
  assert(!JSON.stringify(o).includes(PASS), 'no passcode in an answer');

  // person
  const p = (await call(st, { op: 'person', name: 'tess  WELDER', days: 3 })).body;
  assert.strictEqual(p.ok, true); assert.strictEqual(p.name, 'Tess Welder'); assert.strictEqual(p.from, '2026-10-01'); assert.strictEqual(p.to, '2026-10-03');
  assert.deepStrictEqual(p.days.map(d => d.day), ['2026-10-01', '2026-10-02', '2026-10-03'], 'every day, oldest first');
  assert.deepStrictEqual(p.days.map(d => d.source), ['none', 'events', 'events']); assert.strictEqual(p.days[0].parts, 0); assert.strictEqual(p.days[1].parts, 30); assert.strictEqual(p.days[2].parts, 55);
  assert.strictEqual(p.days[2].firstIn, Z('2026-10-03T12:00:00Z')); assert.strictEqual(p.days[2].lastOut, null); assert.strictEqual(p.days[2].signedInMin, 120); assert.strictEqual(p.days[2].stations[0].station, 'welding');
  assert.strictEqual(p.totals.parts, 85); assert.strictEqual(p.totals.orders, 5); assert.strictEqual(p.days[1].perHour[10], 30);
  assert.strictEqual((await call(st, { op: 'person', name: 'Nobody Here' })).body.days.every(d => d.source === 'none'), true, 'an unknown name: empty days');
  assert.strictEqual((await call(st, { op: 'person', name: '' })).status, 400); assert.strictEqual((await call(st, { op: 'person', name: '123456' })).status, 400, 'a PIN is not a name');
  assert.strictEqual((await call(st, { op: 'person', name: 'Tess Welder', days: 500 })).body.days.length, 62, 'at most 62 days');

  // orders: events at welding, a seal at assembly, a seal at welding that the events replace
  const O = '3521000001', t0 = Z('2026-10-03T12:00:00Z');
  st.put('Order_Timeline', O + '~sorted~s1', seal('s1', O, 'sorted', 'Sam Sorter', 'sorting', t0 - 3600000, true));
  st.put('Order_Timeline', O + '~welded~w1', seal('w1', O, 'welded', 'Tess Welder', 'welding', t0 + 101 * 60000, true));
  st.put('Order_Timeline', O + '~assembled~a1', seal('a1', O, 'assembled', 'Ann Assembler', 'assembly', t0 + 3 * 3600000, true));
  st.put('Order_Timeline', O + '~arrived~e1', seal('e1', O, 'arrived', 'Etsy', 'sorter', t0 - 7200000, true));
  const or = (await call(st, { op: 'orders', orderId: ' ' + O + ' ' })).body;
  assert.strictEqual(or.ok, true); assert.strictEqual(or.orderId, O);
  assert.deepStrictEqual(or.steps.map(s => [s.station, s.person, s.source]), [['sorting', 'Sam Sorter', 'seals'], ['welding', 'Tess Welder', 'events'], ['assembly', 'Ann Assembler', 'seals']], 'oldest first, Etsy is not a person, events replace a station\'s seals');
  const wel = or.steps[1];
  assert.deepStrictEqual([wel.firstAt, wel.lastAt, wel.workMs, wel.scans, wel.completes, wel.parts], [t0 + 100 * 60000, t0 + 101 * 60000, 80000, 1, 1, 10]);
  assert.strictEqual(wel.waitMs, t0 + 100 * 60000 - (t0 - 3600000), 'the time between the sorting step ending and welding starting'); assert.strictEqual(or.steps[0].waitMs, 0);
  assert.strictEqual(or.steps[2].waitMs, t0 + 3 * 3600000 - (t0 + 101 * 60000));
  assert.strictEqual(or.totals.firstAt, t0 - 3600000); assert.strictEqual(or.totals.spanMs, 4 * 3600000); assert.strictEqual(or.totals.people, 3); assert.strictEqual(or.totals.stations, 3);
  assert.deepStrictEqual(or.sources, { events: true, seals: true }); assert.deepStrictEqual(or.events.map(e => e.at), or.events.map(e => e.at).sort((a, b) => a - b));
  assert(or.events.every(e => ['at', 'person', 'station', 'device', 'action', 'parts', 'detail', 'source'].every(k => k in e)));
  assert.deepStrictEqual((await call(st, { op: 'orders', orderId: '3590000000' })).body.steps, []); assert.strictEqual((await call(st, { op: 'orders', orderId: 'abc' })).status, 400);
  const fo = dataset(); fo.fail('Station_Activity'); fo.fail('Order_Timeline'); assert.strictEqual((await call(fo, { op: 'orders', orderId: O })).status, 503);

  // sizes: one overview reads a handful of queries, not collections
  const sz = dataset(); await call(sz, {});
  const by = {}; for (const r of sz.reads) { by[r.name] = by[r.name] || { q: 0, docs: 0 }; by[r.name].q++; by[r.name].docs += r.n || 0; }
  assert.strictEqual(sz.reads.filter(r => r.name === 'Efficiency_Daily' && r.filters.length === 2).length, 1, 'one rollup query for the whole window'); assert.strictEqual(by.Efficiency_Daily.q, 2, '(and one for the day the events began)'); assert.strictEqual(by.Station_Sessions.q, 4, 'a cold overview with the trend reads the sessions in two parts (the final older part, kept 10 minutes; the last two days, kept 5 s), each in both time forms (milliseconds + Firestore times): the same documents as before, and a poll then reads only the recent part');
  assert.strictEqual(by.Station_Activity.q, 1); assert(by.Order_Timeline.q <= 10, 'seals only for the days before the events began, and today for Ann (at most 10 days)');
  const steady = dataset(); steady.put('Efficiency_Daily', '2026-09-01__Tess Welder', roll('2026-09-01', 'Tess Welder', { welding: stat({ parts: 1 }) }, {}, {}, 1, 2)); await call(steady, {});
  assert.strictEqual(steady.readsOf('Order_Timeline').length, 1, 'once the events have run for weeks: seals only for the day with a person who has no rollup (today, Ann)');
  assert.strictEqual(steady.reads.filter(r => r.name === 'Efficiency_Daily' && r.order === 'day').length, 1, 'the day the events began: one read');
  const big = fakeStore(); for (let i = 0; i < 1600; i++) big.put('Station_Sessions', 'big-' + i, sess('big-' + i, 'P' + (i % 40), 'welding', Z('2026-10-03T05:00:00Z') + i * 1000, { end: Z('2026-10-03T05:00:00Z') + i * 1000 + 600000, reason: 'signOut' }));
  EP.resetCache(); const bg = (await call(big, {})).body;
  assert.strictEqual(bg.partial, true); assert(bg.notes.some(n => /cut at their size limit: sessions/.test(n)), 'a read that hit its cap says so');
  assert(big.reads.filter(r => r.name === 'Station_Sessions').every(r => r.n <= 1501));
  // a later day with nothing worked after events began reads no seals
  const q = dataset(); await call(q, { days: 30 }); const sealQ = q.readsOf('Order_Timeline').length;
  assert(sealQ <= 10, 'at most 10 days of seals are ever read');
  say('sandbox separation, shapes (overview/person/orders), person days, order steps + waits, read counts and caps');
}


/* ── 7 · one person under several spellings: folded names, the alias map, overlapping sessions ── */
async function aliases() {
  const T0 = Z('2026-10-03T12:00:00Z');
  const st = fresh(); NOW = Z('2026-10-03T16:00:00Z');
  // the seeded alias: "Giovanna C." (PIN stations) and "Giovanna" (inbox) are one person; the two pages overlap 30 minutes
  st.put('Station_Sessions', 'g-1', sess('g-1', 'Giovanna C.', 'sorting', T0, { end: T0 + 3600000, last: T0 + 3600000, reason: 'signOut' }));
  st.put('Station_Sessions', 'g-2', sess('g-2', 'Giovanna', 'inbox', T0 + 1800000, { end: T0 + 5400000, last: T0 + 5400000, reason: 'signOut' }));
  st.put('Efficiency_Daily', '2026-10-03__Giovanna C.', roll('2026-10-03', 'Giovanna C.', { sorting: stat({ scans: 10, scanParts: 10, completes: 5, parts: 30, orders: 5, activeMs: 1200000 }) }, { '08': { parts: 30, by: { sorting: { parts: 30 } } } }, { 3521000100: { sorting: true } }, T0, T0 + 3600000));
  st.put('Efficiency_Daily', '2026-10-03__Giovanna', roll('2026-10-03', 'Giovanna', { inbox: stat({ scans: 4, scanParts: 0, completes: 2, parts: 0, orders: 2, activeMs: 600000 }) }, { '08': { parts: 0, by: {} } }, { 3521000100: { inbox: true }, 3521000101: { inbox: true } }, T0 + 1800000, T0 + 5400000));
  const e1 = ev('g_AAAA_1_1', 'Giovanna C.', 'sorting', 'complete', T0 + 3000000, { orderId: '3521000100', parts: 30, orders: 1 }), e2 = ev('g_BBBB_1_2', 'Giovanna', 'inbox', 'complete', T0 + 3200000, { orderId: '3521000101', orders: 1 });
  st.put('Station_Activity', e1.id, e1); st.put('Station_Activity', e2.id, e2);
  let b = (await call(st, {})).body;
  assert.deepStrictEqual(b.people.map(p => p.name), ['Giovanna'], 'one person, under the canonical display name');
  const g = b.people[0];
  assert.strictEqual(g.totals.signedInMin, 90, 'the 30 overlapping minutes are counted once (60 + 60 - 30)');
  assert.deepStrictEqual(g.stations.map(s => [s.station, s.minutes]).sort(), [['inbox', 60], ['sorting', 60]], 'per-station minutes stay per page');
  assert.strictEqual(g.totals.parts, 30); assert.strictEqual(g.totals.scans, 14); assert.strictEqual(g.totals.activeMin, 30); assert.strictEqual(g.totals.orders, 1, 'distinct orders across both spellings; 3521000101 was seen only at the inbox: a customer conversation is not an order worked (the person page and the calendar count the same way)');
  assert.strictEqual(g.firstIn, T0); assert.strictEqual(g.lastOut, T0 + 5400000);
  assert.deepStrictEqual(b.feed.map(f => f.person), ['Giovanna', 'Giovanna'], 'the feed uses the one display name');
  assert.deepStrictEqual(g.orders.map(o => o.orderId).sort(), ['3521000100', '3521000101']);
  assert.strictEqual((await call(st, { op: 'person', name: 'giovanna c.', days: 1 })).body.days[0].signedInMin, 90, 'asking by either spelling');
  assert.strictEqual((await call(st, { op: 'person', name: 'GIOVANNA', days: 1 })).body.name, 'Giovanna');
  const ord = (await call(st, { op: 'orders', orderId: '3521000100' })).body; assert.deepStrictEqual(ord.steps.map(x => x.person), ['Giovanna']);
  // the Firestore alias doc adds more; accents and spaces fold with no alias at all; it is read, never written
  const s2 = fresh();
  s2.put('config', 'employeeAliases', { 'José Pérez': ['Jose P.', 'Pepe'], 'Not A Name': ['123456'], '654321': ['Zed'], broken: 'x' });
  s2.put('Station_Sessions', 'j-1', sess('j-1', 'Jose P.', 'welding', T0, { end: T0 + 1800000, last: T0 + 1800000, reason: 'signOut' }));
  s2.put('Station_Sessions', 'j-2', sess('j-2', 'pepe', 'assembly', T0 + 3600000, { end: T0 + 4500000, last: T0 + 4500000, reason: 'signOut' }));
  s2.put('Station_Sessions', 'j-3', sess('j-3', 'JOSE  PEREZ', 'welding', T0 + 7200000, { end: T0 + 7800000, last: T0 + 7800000, reason: 'signOut' }));
  s2.put('Station_Sessions', 'z-1', sess('z-1', 'Zoë  Müller', 'sorting', T0, { end: T0 + 600000, last: T0 + 600000, reason: 'signOut' }));
  s2.put('Station_Sessions', 'z-2', sess('z-2', 'ZOE MULLER', 'sorting', T0 + 1200000, { end: T0 + 1800000, last: T0 + 1800000, reason: 'signOut' }));
  s2.put('Station_Sessions', 'z-3', sess('z-3', 'Zed', 'sorting', T0, { end: T0 + 600000, last: T0 + 600000, reason: 'signOut' }));
  b = (await call(s2, {})).body;
  assert.deepStrictEqual(b.people.map(p => p.name).sort(), ['José Pérez', 'Zed', 'Zoë  Müller'.replace('  ', ' ')].sort(), 'aliases from the doc; accents and case fold; a digits-only entry never becomes a person');
  const jose = b.people.find(p => p.name === 'José Pérez');
  assert.strictEqual(jose.totals.signedInMin, 30 + 15 + 10, 'three spellings, three sessions, one person'); assert.deepStrictEqual(jose.stations.map(s => s.station).sort(), ['assembly', 'welding']);
  assert.strictEqual(b.people.find(p => p.name === 'Zoë Müller').totals.signedInMin, 20, 'Zoë Müller / ZOE MULLER fold together without an alias');
  assert(!JSON.stringify(b).includes('123456') && !JSON.stringify(b).includes('654321'), 'no digits-only name anywhere');
  assert.strictEqual(s2.writes.length, 0, 'the alias doc is never written (nothing is written)');
  const before = s2.readsOf('config').length; tick(1000); await call(s2, {}); assert.strictEqual(s2.readsOf('config').length, before, 'the alias doc is cached for a minute');
  tick(61000); await call(s2, {}); assert.strictEqual(s2.readsOf('config').length, before + 1, 'and read again after it');
  // a failing alias read: the seeded aliases still apply, and it says so
  const s3 = fresh(); s3.put('Station_Sessions', 'g-1', sess('g-1', 'Giovanna C.', 'sorting', T0, { end: T0 + 600000, last: T0 + 600000, reason: 'signOut' }));
  s3.fail('config'); const fb = (await call(s3, {}));
  assert.strictEqual(fb.status, 200, 'the passcode comes from the environment: the alias read fails alone');
  assert.deepStrictEqual(fb.body.people.map(p => p.name), ['Giovanna']); assert.strictEqual(fb.body.partial, true); assert(fb.body.errors.some(x => /^aliases: /.test(x)));
  assert.strictEqual(T.fold('  Zoë   MÜLLER '), 'zoe muller');
  say('aliases: seeded Giovanna merge, overlap counted once with per-page minutes, doc aliases, accents/case/space fold, no digit names, never written, cached a minute, failing read is partial');
}

/* ── 8 · names: the PIN list's underscore style and the typed style are one person; the screen shows a nice name ── */
async function names() {
  const same = ['Michael_V', 'Michael V', 'Michael V.', 'michael v.', 'MICHAEL  V', ' michael_v. ', 'Michael__V', 'MICHAEL_V', 'Michael _ V.'];
  assert.strictEqual(new Set(same.map(T.fold)).size, 1, 'underscore, period, case and spacing do not make a second person');
  assert.strictEqual(T.fold('Michael_V'), 'michael v');
  assert.strictEqual(new Set(["O'Brien", 'OBRIEN', 'O’Brien'].map(T.fold)).size, 1, 'an apostrophe is not a word break');
  const apart = ['Michael V.', 'Michael T.', 'Michael', 'Michael Vega', 'Michelle_R', 'Ivy_Y', 'Ana_M', 'Empress D.', 'Paul_K', 'Giovanna C.', 'Giovanna'];
  assert.strictEqual(new Set(apart.map(T.fold)).size, apart.length, 'a different initial (or none) is a different person: Michael V. is not Michael T. or Michael');
  const seed = T.buildAliases(null);
  assert.strictEqual(seed.map.get(T.fold('Giovanna C.')), T.fold('Giovanna'), 'Giovanna C. still joins Giovanna, through the built-in alias');
  assert.strictEqual(seed.map.get(T.fold('giovanna_c')), T.fold('Giovanna'), '... however it is spelled'); assert.strictEqual(seed.display.get(T.fold('Giovanna')), 'Giovanna');
  const custom = T.buildAliases({ 'Mike V.': ['Michael_V'], 'Shelly_R': ['Michelle R.'] });
  assert.strictEqual(custom.map.get(T.fold('Michael V.')), T.fold('Mike V.'), 'an alias entry folds like a name: "Michael_V" catches "Michael V."');
  assert.strictEqual(custom.map.get(T.fold('MICHELLE_R')), T.fold('Shelly_R'));
  assert.strictEqual(custom.display.get(T.fold('Shelly_R')), 'Shelly_R', 'an alias keeps its own spelling for the screen, underscore and all');
  const nice = { 'Michael_V': 'Michael V.', 'Ana_M': 'Ana M.', 'Paul_K': 'Paul K.', 'Michelle_R': 'Michelle R.', 'Ivy_Y': 'Ivy Y.', 'Giovanna C.': 'Giovanna C.', 'Empress D.': 'Empress D.',
    'Michael V.': 'Michael V.', 'Paul K': 'Paul K.', 'michael v': 'Michael V.', 'MICHAEL  V': 'Michael V.', 'ana_m': 'Ana M.', 'Giovanna': 'Giovanna', 'Shell': 'Shell', 'SHELL': 'Shell',
    'Zoë Müller': 'Zoë Müller', 'McDonald_J': 'McDonald J.', "O'BRIEN": "O'Brien", 'Mary-Jane K': 'Mary-Jane K.', 'JJ': 'JJ', 'Michael V. Smith': 'Michael V. Smith', 'Mary J Blige': 'Mary J. Blige' };
  for (const [raw, want] of Object.entries(nice)) assert.strictEqual(T.niceName(raw), want, 'the screen shows "' + raw + '" as "' + want + '"');
  assert.strictEqual(T.niceName('Michael_V'), T.niceName(T.niceName('Michael_V')), 'tidying twice changes nothing');
  // through the reader: sessions of the PIN-list spelling and the typed spelling are one person with a nice name; nothing is written
  const T0 = Z('2026-10-03T12:00:00Z'), st = fresh(); NOW = Z('2026-10-03T16:00:00Z');
  st.put('Station_Sessions', 'm-1', sess('m-1', 'Michael_V', 'welding', T0, { end: T0 + 3600000, last: T0 + 3600000, reason: 'signOut' }));
  st.put('Station_Sessions', 'm-2', sess('m-2', 'Michael V.', 'design', T0 + 1800000, { end: T0 + 5400000, last: T0 + 5400000, reason: 'signOut' }));
  st.put('Station_Sessions', 'm-3', sess('m-3', 'Michael T.', 'welding', T0, { end: T0 + 600000, last: T0 + 600000, reason: 'signOut' }));
  st.put('Station_Sessions', 'm-4', sess('m-4', 'Ana_M', 'assembly', T0, { end: T0 + 600000, last: T0 + 600000, reason: 'signOut' }));
  const b = (await call(st, {})).body;
  assert.deepStrictEqual(b.people.map(p => p.name).sort(), ['Ana M.', 'Michael T.', 'Michael V.'], 'Michael_V + Michael V. are one Michael V.; Michael T. is another person; Ana_M shows as Ana M.');
  assert.strictEqual(b.people.find(p => p.name === 'Michael V.').totals.signedInMin, 90, '60 + 60 - 30 minutes of overlap');
  for (const spelling of ['Michael_V', 'michael v', 'MICHAEL V.']) assert.strictEqual((await call(st, { op: 'person', name: spelling, days: 1 })).body.days[0].signedInMin, 90, 'asking as "' + spelling + '"');
  assert.strictEqual((await call(st, { op: 'person', name: 'ana m', days: 1 })).body.name, 'Ana M.');
  assert.strictEqual(st.writes.length, 0, 'nothing is written');
  say('names: Michael_V / Michael V. / michael v. / MICHAEL  V are one person, a different initial is not, aliases fold the same way, nice names for the screen (Michael V., Ana M., Paul K.), the alias spelling wins');
}

(async () => {
  try { await gate(); await dayBoundary(); await merge(); await delta(); await partial(); await rest(); await aliases(); await names(); }
  finally { Date.now = realNow; }
  const all = logs.concat(bodies).join('\n');
  for (const s of [PASS, PASS2]) assert(!all.includes(s), 'a passcode appeared in a response or a log line');
  say('no passcode in any response or log line (' + bodies.length + ' responses, ' + logs.length + ' log lines)');
  say('OK');
})().catch(e => { Date.now = realNow; process.stdout.write('FAIL ' + (e && e.stack || e) + '\n'); process.exit(1); });
