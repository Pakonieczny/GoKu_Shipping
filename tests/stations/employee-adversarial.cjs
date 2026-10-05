// An attempt to BREAK the employee portal's server side, offline (ET1). The real functions (employeeEfficiency.js with its helpers
// _employeeProfile / _employeeAttendance / _employeeIssues / _stationLive, and the station doors of firebaseOrders.js with the rollup
// writer _stationActivity.js) run over an in-memory Firestore with typed fields, where/orderBy/limit/select, transactions, increments,
// outages per collection, a fake clock, a synthetic passcode, and the invented shop of tests/charm-nest/employee-profile-seed.cjs.
// Every check below was a way to break it that FAILED once; the fix and this test arrived together.
//   1 · the gate: every op with every odd key (0 reads), the rate limit, locked, GET/OPTIONS/413, the passcode never in an answer or a log
//   2 · prototype keys (ops, ranges, stations, names, rollup station keys), alias loops, input abuse (limits, cursors, search text, dates)
//   3 · privacy: a PIN in a name through the three station doors and in old stored names, 6-digit numbers in free text, Employee Numbers
//       never read, a PIN-like order number refused, sandbox and real never mixed (people, orders, live, thumbnails, poisoned flags)
//   4 · odd data: New York midnight, daylight-saving days (1 Nov 25 h, 8 Mar 23 h), signed in and did nothing, two people at one station,
//       one person under three spellings, 0 and 300 pieces, missing pictures, an order across midnight
//   5 · outages: every source failing in turn gives 200 + partial:true + a named error, unknown numbers are null (never 0) on the person page
//   6 · cost: documented read budgets, nothing read twice, polls are cheap, a 100-person shop answers in bounded reads
//   7 · consistency: person totals = series = order list = overview; live counts = overview station counts
//   8 · fuzz: garbage documents in every collection, fixed seeds, never a 5xx
//   node tests/stations/employee-adversarial.cjs
'use strict';
const path = require('path'), assert = require('assert'), Module = require('module');
const root = path.join(__dirname, '../..');
const S = require('../charm-nest/employee-profile-seed.cjs');

/* ── fake Firestore ── */
class Ts { constructor(m) { this.m = m; } toMillis() { return this.m; } static fromMillis(m) { return new Ts(m); } }
const kind = v => v instanceof Ts ? 'ts' : typeof v === 'number' ? 'num' : typeof v === 'string' ? 'str' : typeof v === 'boolean' ? 'bool' : 'other';
const val = v => v instanceof Ts ? v.m : v;
const isPlain = v => v && typeof v === 'object' && !Array.isArray(v) && !(v instanceof Ts) && v.__inc == null;
function applyDoc(prev, data, merge) {                     // set({merge}) merges nested maps, increment adds
  const out = merge && prev ? Object.assign({}, prev) : {};
  for (const [k, v] of Object.entries(data)) {
    if (v && v.__inc != null) out[k] = (Number(out[k]) || 0) + v.__inc;
    else if (isPlain(v)) out[k] = applyDoc(merge && isPlain(out[k]) ? out[k] : null, v, merge);
    else out[k] = v;
  }
  return out;
}
function fakeStore() {
  const colls = new Map(), reads = [], writes = [], failing = new Set();
  const data = n => { if (!colls.has(n)) colls.set(n, new Map()); return colls.get(n); };
  const keep = v => v instanceof Ts ? v : Array.isArray(v) ? v.map(keep) : v && typeof v === 'object' ? Object.fromEntries(Object.entries(v).map(([k, x]) => [k, keep(x)])) : v;
  const down = name => Object.assign(new Error('14 UNAVAILABLE: synthetic outage of ' + name), { code: 14 });
  function query(name, filters, order, lim, sel) {
    return {
      where: (f, op, v) => query(name, filters.concat([[f, op, v]]), order, lim, sel),
      orderBy: (f, d) => query(name, filters, [f, d || 'asc'], lim, sel),
      limit: n => query(name, filters, order, n, sel),
      select: (...f) => query(name, filters, order, lim, f),
      get: async () => {
        if (failing.has(name)) throw down(name);
        let docs = [...data(name)].map(([id, d]) => ({ id, d }));
        for (const [f, op, v] of filters) docs = docs.filter(({ d }) => {
          const x = d[f]; if (x === undefined || kind(x) !== kind(v)) return false;
          const a = val(x), b = val(v);
          return op === '==' ? a === b : op === '>=' ? a >= b : op === '>' ? a > b : op === '<' ? a < b : op === '<=' ? a <= b : false;
        });
        if (order) { const [f, dir] = order; docs = docs.filter(({ d }) => d[f] !== undefined).sort((p, q) => (val(p.d[f]) < val(q.d[f]) ? -1 : val(p.d[f]) > val(q.d[f]) ? 1 : 0) * (dir === 'desc' ? -1 : 1)); }
        if (lim != null) docs = docs.slice(0, lim);
        reads.push({ name, n: Math.max(1, docs.length), filters: filters.map(f => f[0] + f[1]), vals: filters.map(f => val(f[2])), ts: filters.some(f => f[2] instanceof Ts), order: order && order[0] });
        return { docs: docs.map(({ id, d }) => ({ id, data: () => keep(sel ? Object.fromEntries(Object.entries(d).filter(([k]) => sel.includes(k))) : d) })), size: docs.length, empty: !docs.length };
      }
    };
  }
  const ref = (name, id) => ({ id, name,
    get: async () => { reads.push({ name, doc: id, n: 1 }); if (failing.has(name)) throw down(name); const d = data(name).get(id); return { exists: !!d, id, data: () => keep(d) }; },
    set: async (v, o) => { writes.push([name, id, 'set']); data(name).set(id, applyDoc(data(name).get(id), keep(v), !!(o && o.merge))); },
    update: async v => { writes.push([name, id, 'update']); if (!data(name).has(id)) throw Object.assign(new Error('5 NOT_FOUND'), { code: 5 }); data(name).set(id, Object.assign({}, data(name).get(id), keep(v))); },
    create: async v => { writes.push([name, id, 'create']); data(name).set(id, keep(v)); } });
  const db = { collection: name => Object.assign(query(name, [], null, null, null), { doc: id => ref(name, id) }),
    getAll: async (...a) => Promise.all(a.filter(x => x && x.get).map(r => r.get())),
    runTransaction: async fn => fn({ get: r => r.get(), getAll: (...rs) => Promise.all(rs.map(r => r.get())), set: (r, v, o) => r.set(v, o), create: (r, v) => r.create(v) }) };
  return { db, put: (name, id, d) => data(name).set(id, keep(d)), get: (name, id) => data(name).get(id), all: name => [...data(name)].map(([id, d]) => Object.assign({ _id: id }, d)), reads, writes, fail: n => failing.add(n), heal: n => failing.delete(n), count: n => data(n).size, colls,
    readsOf: name => reads.filter(r => r.name === name), docsOf: name => reads.filter(r => r.name === name).reduce((n, r) => n + r.n, 0), docsRead: () => reads.reduce((n, r) => n + r.n, 0), clear: () => { reads.length = 0; writes.length = 0; } };
}

/* ── the functions under test, over one switchable fake handle (the station doors write through it) ── */
let curDb = null;
const dbNow = { collection: n => curDb.collection(n), getAll: (...a) => curDb.getAll(...a), runTransaction: f => curDb.runTransaction(f) };
const fakeAdmin = { firestore: Object.assign(() => dbNow, { Timestamp: Ts, FieldValue: { serverTimestamp: () => 'ts', increment: n => ({ __inc: n }), delete: () => null } }) };
const realLoad = Module._load;
Module._load = function (req, ...rest) { if (/[\/]firebaseAdmin(\.js)?$/.test(req)) return fakeAdmin; return realLoad.call(this, req, ...rest); };
const eff = require(path.join(root, 'netlify/functions/employeeEfficiency.js'));
const door = require(path.join(root, 'netlify/functions/firebaseOrders.js'));
const EP = require(path.join(root, 'netlify/functions/_editPasscode.js'));
const LIVE = require(path.join(root, 'netlify/functions/_stationLive.js'));
Module._load = realLoad;
const T = eff._t;

const PASS = 'synthetic-pass-et1-7k';
const PIN = '482915', PIN2 = '271828';                     // synthetic "login numbers", loud: they must never come back out
process.env.EDIT_PASSCODE = PASS;
const logs = [], keepLog = (...a) => logs.push(a.map(x => typeof x === 'string' ? x : JSON.stringify(x)).join(' '));
console.warn = console.log = console.error = console.info = keepLog;
const T0 = Date.now ? process.hrtime.bigint() : 0n;
const say = (...a) => process.stdout.write(a.join(' ') + (/^\d ·/.test(String(a[0])) ? `   [${(Number(process.hrtime.bigint() - T0) / 1e9).toFixed(0)} s so far]` : '') + '\n');
let NOW = S.NOW; Date.now = () => NOW;
const setNow = n => { NOW = n; }, tick = m => { NOW += m; };
let ipN = 0;
const bodies = [];
async function call(st, body, o = {}) {
  curDb = st.db;
  const r = await T.handle({ httpMethod: o.method || 'POST', headers: { 'x-nf-client-connection-ip': o.ip || '203.0.113.' + (++ipN % 250) }, body: o.raw != null ? o.raw : JSON.stringify(o.nokey ? body : Object.assign({ key: PASS }, body)) }, st.db);
  if (!r || typeof r.body !== 'string') return { status: r && r.statusCode, body: {}, raw: '', weird: true };
  bodies.push(r.body);
  let j = {}; try { j = JSON.parse(r.body || '{}'); } catch (e) { j = { _unparsable: true }; }
  return { status: r.statusCode, size: r.body.length, body: j, raw: r.body };
}
async function post(st, payload, o = {}) {                 // a station door: open, no passcode
  curDb = st.db;
  const r = await door.handler({ httpMethod: 'POST', headers: { 'x-nf-client-connection-ip': '198.51.100.' + (1 + (++ipN % 200)) }, queryStringParameters: o.sandbox ? { sandbox: '1' } : {}, body: JSON.stringify(payload) });
  return { status: r.statusCode, body: JSON.parse(r.body || '{}') };
}
let TEMPLATE = null;                                          // the seeded shop is built once; every test gets its own store (cold caches) over copies of its collections
function build() {
  EP.resetCache(); NOW = S.NOW;
  if (!TEMPLATE) { const t = fakeStore(); TEMPLATE = { st: t, truth: S.seed((c, id, d) => t.put(c, id, d)) }; }
  const st = fakeStore(); for (const [n, m] of TEMPLATE.st.colls) st.colls.set(n, new Map(m));
  curDb = st.db; return { st, truth: TEMPLATE.truth };
}
function empty() { EP.resetCache(); NOW = S.NOW; const st = fakeStore(); curDb = st.db; return st; }
const person = (st, name, range, extra) => call(st, Object.assign({ op: 'person', name, range }, extra || {}));
const ordersOf = (st, name, extra) => call(st, Object.assign({ op: 'personOrders', name }, extra || {}));
const ok = (cond, msg) => { assert(cond, msg); };
const noFive = (r, what) => ok(r.status < 500 && !r.weird, `${what}: status ${r.status}`);
const statOf = o => Object.assign({ scans: 0, scanParts: 0, completes: 0, parts: 0, orders: 0, prints: 0, rejects: 0, errors: 0, notes: 0, undos: 0, undoParts: 0, undoOrders: 0, activeMs: 0, idleMs: 0, firstAt: 0, lastAt: 0 }, o);
const nyAt = S.nyAt, TODAY = S.TODAY;
const sessionDoc = (id, who, station, startAt, endAt, extra) => Object.assign({ id, person: who, station, device: station + '-1', computerId: 'pc-' + id.replace(/\W/g, '').slice(0, 8).toUpperCase().padEnd(8, 'X'), computerLabel: '', startAt, lastSeenAt: endAt || NOW - 60000, endAt: endAt || null, endReason: endAt ? 'signOut' : null, minutes: 0 }, extra || {});
const rollDoc = (day, who, stations, rids, extra) => Object.assign({ day, person: who, v: 1, events: 10, firstAt: nyAt(day, 9, 5), lastAt: nyAt(day, 14, 50), stations, hours: { '10': { parts: 1, scans: 1, undoParts: 0, by: {} } }, touched: Object.fromEntries((rids || []).map(r => [r, Object.fromEntries(Object.keys(stations).map(s => [s, true]))])) }, extra || {});
const rids = (base, n) => Array.from({ length: n }, (_, i) => String(base + i));
const protoClean = () => { assert.strictEqual(Object.keys(Object.prototype).length, 0, 'Object.prototype was polluted'); assert.strictEqual(({}).polluted, undefined); };

(async () => {
  const t0 = process.hrtime.bigint();

  /* ═══ 1 · the gate ═══ */
  say('1 · the gate');
  {
    const { st } = build();
    const ops = [undefined, 'overview', 'person', 'personOrders', 'orders', 'live', 'bogus', '__proto__', 'constructor'];
    const keys = [undefined, '', ' ', 'x', 0, 123456, [], {}, [PASS], { toString: 1 }, { key: PASS }, null, true, false, PASS.toUpperCase(), PASS.slice(0, -1), PASS + 'x', 'Bearer ' + PASS];
    let n = 0;
    for (const op of ops) for (const key of keys) {
      const r = await call(st, { op, name: 'Giovanna', range: 'week', orderId: '3521000007', key }, { nokey: true }); n++;
      ok(r.status === 401 && r.body.error === 'unauthorized' && Object.keys(r.body).sort().join() === 'error,ok', `op ${op} key ${JSON.stringify(key)}: ${r.status} ${r.raw.slice(0, 80)}`);
    }
    ok(st.reads.length === 0 && st.writes.length === 0, 'a refused call reads and writes nothing: ' + st.reads.length);
    say('  ' + n + ' op x key combinations: all 401, 0 documents read');
    // a wrong key is never echoed back
    const guess = 'a-secret-guess-0xBEEF';
    const g = await call(st, { op: 'overview', key: guess }, { nokey: true }); ok(g.status === 401 && !g.raw.includes(guess));
    // the shared comparer: an object with a non-callable toString, or no prototype, is a wrong key, not an exception
    ok(EP.sameSecret({ toString: 1 }, PASS) === false && EP.sameSecret(Object.create(null), PASS) === false && EP.sameSecret([], PASS) === false && EP.sameSecret(PASS, PASS) === true && EP.sameSecret(' ' + PASS + ' ', PASS) === true, 'sameSecret');
    // ten wrong keys a minute from one address: then even the right key waits
    const lim = empty();
    for (let i = 0; i < 10; i++) { const r = await call(lim, { op: 'overview', key: 'guess' + i }, { nokey: true, ip: '192.0.2.9' }); ok(r.status === 401); }
    let r = await call(lim, { op: 'overview', days: 1 }, { ip: '192.0.2.9' }); ok(r.status === 429 && !r.raw.includes(PASS), 'locked after ten: ' + r.status);
    r = await call(lim, { op: 'overview', days: 1 }, { ip: '192.0.2.10' }); ok(r.status === 200, 'another address is not locked');
    tick(61000); r = await call(lim, { op: 'overview', days: 1 }, { ip: '192.0.2.9' }); ok(r.status === 200, 'a minute later the address may try again');
    // locked while the passcode is unset: 403, and no data read
    const lk = empty(); delete process.env.EDIT_PASSCODE; EP.resetCache(); lk.put('config', 'editPasscode', { passcode: '' });
    r = await call(lk, { op: 'overview' }); ok(r.status === 403 && r.body.code === 'EDIT_PASSCODE_NOT_SET' && lk.reads.every(x => x.name === 'config'), 'locked: ' + r.status);
    process.env.EDIT_PASSCODE = PASS; EP.resetCache();
    // methods and size, before any data is touched
    const m = empty();
    r = await call(m, {}, { method: 'GET' }); ok(r.status === 405);
    r = await call(m, {}, { method: 'OPTIONS' }); ok(r.status === 204);
    r = await call(m, {}, { raw: 'x'.repeat(9000) }); ok(r.status === 413 && m.reads.length === 0, 'too large: ' + r.status);
    r = await call(m, {}, { raw: '{not json' }); ok(r.status === 401, 'unparsable body is an unauthorized call: ' + r.status);
    for (const raw of ['null', '[]', '"x"', '12', 'true']) { r = await call(m, {}, { raw }); ok(r.status === 401 && !r.weird, 'body ' + raw + ': ' + r.status); }
    // an unknown op, with the key: 400, and a prototype method is not an op
    for (const op of ['bogus', '__proto__', 'constructor', 'toString', 'hasOwnProperty', 'valueOf', 'prototype', 'isPrototypeOf', 'op']) { r = await call(m, { op }); ok(r.status === 400 && r.body.error === 'unknown op', `op ${op}: ${r.status}`); }
    for (const op of [5, [], {}, null, true]) { r = await call(m, { op }); ok(r.status === 400 || r.status === 200, `op ${JSON.stringify(op)}: ${r.status}`); }
    protoClean();
    say('  rate limit (10 a minute per address), 403 locked, 405, 204, 413, odd bodies, prototype method names as ops: ok');
  }

  /* ═══ 2 · prototype keys, alias loops, input abuse ═══ */
  say('2 · prototype keys, alias loops, input abuse');
  {
    const { st } = build();
    const base = await ordersOf(st, 'Giovanna', { limit: 5 });
    for (const range of ['__proto__', 'constructor', 'toString', 'hasOwnProperty', ['week'], ['__proto__'], { from: '__proto__', to: 'x' }, { __proto__: { from: '2026-10-01', to: '2026-10-02' } }, null, true, 7, '', 'WEEK', ' week']) {
      const r = await person(st, 'Giovanna', range); noFive(r, 'range ' + JSON.stringify(range)); ok(r.status === 200 || r.status === 400, 'range ' + JSON.stringify(range));
      if (r.status === 200 && r.body.range !== undefined) ok(['day', 'week', 'month', 'quarter', 'year', 'custom'].includes(r.body.range), 'range answered: ' + r.body.range);   // (no range given: the older answer, which has none)
    }
    for (const station of ['__proto__', 'constructor', 'toString', 'hasOwnProperty', 5, [], {}]) {
      const r = await ordersOf(st, 'Giovanna', { station, limit: 5 }); ok(r.status === 200 && r.body.total === base.body.total, 'a prototype name is not a station filter: ' + JSON.stringify(station) + ' ' + r.body.total);
    }
    for (const name of ['__proto__', 'constructor', 'toString', 'hasOwnProperty', 'a/b/c', '../../etc', 'Giovanna/../Paul', 'x'.repeat(5000), 'Ünïcödé 名前 🙂', '\u0000\u0001Giovanna', 'Giovanna\n', '<script>alert(1)</script>', "O'Brien", '%2e%2e', '.', '..', '{"$ne":1}', 'Robert\'); DROP TABLE--', 'ａｂｃ Ｄ']) {
      for (const op of ['person', 'personOrders']) { const r = await call(st, { op, name, range: 'week' }); noFive(r, op + ' name ' + JSON.stringify(name).slice(0, 30)); ok(r.status === 200 || r.status === 400); if (r.status === 200) ok(typeof r.body.name === 'string' && r.body.name.length <= 80, 'name is cut to 80'); }
    }
    protoClean();
    // rollup documents that name stations after prototype members, and numbers that are not numbers
    const p = empty();
    p.put('Efficiency_Daily', `${TODAY}__Proto Tester`, rollDoc(TODAY, 'Proto Tester', { constructor: statOf({ parts: 999 }), __proto__: statOf({ parts: 999 }), toString: statOf({ parts: 999 }), welding: statOf({ parts: 7, completes: 3, scans: 4, orders: 3 }), 'Bad Station!': statOf({ parts: 999 }), x: 5, y: null }, rids(9100000001, 3)));
    p.put('Station_Sessions', 'sp1', sessionDoc('sp1', 'Proto Tester', 'welding', nyAt(TODAY, 9, 0), null));
    const o = await call(p, { op: 'overview', days: 1 });
    ok(o.status === 200 && !/"station":"(constructor|__proto__|toString|Bad Station!)"/.test(o.raw) && o.body.business.totals.parts === 7, 'overview ignores prototype stations: ' + o.status + ' ' + o.raw.slice(0, 300));
    const pp = await person(p, 'Proto Tester', 'day'); ok(pp.status === 200 && pp.body.kpis.parts.value === 7 && !/constructor|__proto__/.test(JSON.stringify(pp.body.stations)), 'person ignores them too: ' + pp.body.kpis.parts.value);
    protoClean();
    // alias loops and aliases onto nothing: one person, no hang, no 5xx
    const a = empty();
    a.put('config', 'employeeAliases', { 'Alpha One': ['Beta Two'], 'Beta Two': ['Alpha One'], 'Cee Three': ['Dee Four', 'Eee Five', 'Cee Three'], constructor: ['__proto__'], toString: 12, [PIN]: ['x'], 'Self Same': ['Self Same'] });
    a.put('Efficiency_Daily', `${TODAY}__Alpha One`, rollDoc(TODAY, 'Alpha One', { welding: statOf({ parts: 10, completes: 5, orders: 5, scans: 5 }) }, rids(9200000001, 5)));
    a.put('Efficiency_Daily', `${TODAY}__Beta Two`, rollDoc(TODAY, 'Beta Two', { welding: statOf({ parts: 4, completes: 2, orders: 2, scans: 2 }) }, rids(9200000101, 2)));
    a.put('Efficiency_Daily', `${TODAY}__Dee Four`, rollDoc(TODAY, 'Dee Four', { assembly: statOf({ parts: 3, completes: 1, orders: 1, scans: 1 }) }, rids(9200000201, 1)));
    a.put('Efficiency_Daily', `${TODAY}__Eee Five`, rollDoc(TODAY, 'Eee Five', { assembly: statOf({ parts: 2, completes: 1, orders: 1, scans: 1 }) }, rids(9200000301, 1)));
    const ao = await call(a, { op: 'overview', days: 1 });
    ok(ao.status === 200 && ao.body.people.length === 2, 'two loops are two people: ' + ao.body.people.map(x => x.name));
    ok(ao.body.people.find(x => x.totals.parts === 14) && ao.body.people.find(x => x.totals.parts === 5), 'A and B are one (14 parts), D and E join C (5): ' + JSON.stringify(ao.body.people.map(x => [x.name, x.totals.parts])));
    for (const nm of ['Alpha One', 'Beta Two', 'alpha one', 'BETA_TWO']) { const r = await person(a, nm, 'day'); ok(r.status === 200 && r.body.kpis.parts.value === 14, `${nm}: ${r.body.kpis && r.body.kpis.parts.value}`); }
    for (const nm of ['Cee Three', 'Dee Four', 'Eee Five']) { const r = await person(a, nm, 'day'); ok(r.status === 200 && r.body.kpis.parts.value === 5, `${nm}: ${r.body.kpis && r.body.kpis.parts.value}`); }
    ok(!ao.raw.includes(PIN), 'an alias that is a number is never shown');
    protoClean();
    say('  prototype names as ops, ranges, stations, person names and rollup station keys; alias loops: ok');

    // numbers: limits, cursors, search text, dates
    const { st: s2 } = build();
    for (const limit of [0, 1, -5, 1e9, NaN, 'abc', '7', null, 1.9, [3], {}, 1e308, -Infinity, '1e9', ' 5 ']) {
      const r = await ordersOf(s2, 'Giovanna', { limit }); ok(r.status === 200 && r.body.orders.length >= 1 && r.body.orders.length <= 100, `limit ${JSON.stringify(limit)}: ${r.body.orders && r.body.orders.length}`);
    }
    for (const cursor of ['o999999', 'o0', 'o-5', 'o1e9', 'garbage', 'o25;DROP', '__proto__', 'o25 ', 'O25', 'o' + '9'.repeat(40), { a: 1 }, ['o5'], 12, 'o07']) {
      const r = await ordersOf(s2, 'Giovanna', { limit: 5, cursor }); ok(r.status === 200 && r.body.total === base.body.total && r.body.orders.length <= 5, `cursor ${JSON.stringify(cursor).slice(0, 20)}`);
      ok(r.body.next === null || /^o\d{1,6}$/.test(r.body.next), 'next cursor is well formed: ' + r.body.next);
    }
    const bigQ = [{ q: 'x'.repeat(1000) }, { q: '.*' }, { q: '(' }, { q: '[' }, { q: '\\' }, { q: '$^' }, { q: '(?<=a)' }, { q: 'a|b' }, { q: '%' }, { q: '\u0000' }, { q: '<b>' }, { q: 'constructor' }, { q: '__proto__' }, { q: 'a b c d e f g h i j k l m n o p q r s t' }, { q: '10/3' }, { q: '99/99' }, { q: '2026-99-99' }, { q: PIN }, { q: '123456 jane' }, { q: 'oct 99' }, { q: '0 oct' }, { q: ['jane'] }, { q: { a: 1 } }, { q: 5 }];
    for (const b of bigQ) { const r = await ordersOf(s2, 'Giovanna', Object.assign({ limit: 3 }, b)); ok(r.status === 200 && Array.isArray(r.body.orders), 'q ' + JSON.stringify(b.q).slice(0, 20)); if (typeof b.q === 'string' && b.q.length > 3) ok(!r.raw.includes(b.q), 'the search text is not echoed back: ' + JSON.stringify(b.q).slice(0, 20)); }
    for (const day of ['0001-01-01', '0099-12-31', '0100-01-01', '1969-12-31', '1970-01-01', '2000-02-29', '2100-01-01', '9999-12-31', '2026-02-29', '2026-10-05T10:00', ' 2026-10-05', 20261005, [], {}, '٢٠٢٦-١٠-٠٥', '2026-10-5', '2026-10-05\n']) {
      s2.clear(); const r = await person(s2, 'Giovanna', 'week', { day }); ok(r.status === 200 || r.status === 400, `day ${JSON.stringify(day)}: ${r.status}`); ok(s2.docsRead() < 2500, `day ${JSON.stringify(day)} read ${s2.docsRead()}`);
      if (r.status === 200) ok(r.body.to <= TODAY, `a far-future day is cut to today: ${r.body.to}`);
    }
    for (const [from, to] of [['0001-01-01', '0001-01-02'], ['1970-01-01', '1970-12-31'], ['2026-10-05', '2026-10-05'], ['2027-01-01', '2027-02-01'], ['2026-10-06', '2026-10-06'], ['2026-10-05', '2026-09-01'], ['1900-01-01', '2026-10-05'], ['2026-02-30', '2026-03-01'], [5, 6], [null, null], ['2026-10-01', null]]) {
      s2.clear(); const r = await person(s2, 'Giovanna', { from, to }); ok(r.status === 200 || r.status === 400, `range ${from}..${to}: ${r.status}`); ok(s2.docsRead() < 3500, `range ${from}..${to} read ${s2.docsRead()}`);
      const r2 = await ordersOf(s2, 'Giovanna', { from, to }); ok(r2.status === 200 || r2.status === 400, `orders ${from}..${to}: ${r2.status}`);
    }
    { const r = await person(s2, 'Giovanna', { from: '2026-10-05', to: '2026-09-01' }); ok(r.status === 400 && /after/.test(r.body.error), 'from after to: 400'); }
    for (const body of [{ days: 1e9 }, { days: -3 }, { days: 'x' }, { days: NaN }, { day: '2999-01-01' }, { after: 'x' }, { after: '~' }, { after: '-1~a' }, { after: '1e999~a' }, { after: '9999999999999999~z' }, { after: 5 }, { after: { a: 1 } }, { trend: 'no' }, { sandbox: [] }, { sandbox: 1 }, { sandbox: 0 }, { sandbox: {} }]) {
      s2.clear(); const r = await call(s2, Object.assign({ op: 'overview' }, body)); ok(r.status === 200 && Array.isArray(r.body.people) && Array.isArray(r.body.feed), 'overview ' + JSON.stringify(body)); ok(s2.docsRead() < 4000, 'overview ' + JSON.stringify(body) + ' read ' + s2.docsRead());
      ok(r.body.people.length <= 60 && r.body.feed.length <= 200, 'answers are cut to their limits');
    }
    for (const body of [{ day: '0001-01-01' }, { day: '0050-01-01' }, { day: [] }, { day: 5 }]) { const r = await call(s2, Object.assign({ op: 'overview' }, body)); ok(r.status === 400, 'overview bad day ' + JSON.stringify(body) + ': ' + r.status); }
    for (const days of [0, -1, 1e9, NaN, 'x', 62, 63, [5], {}]) { const r = await call(s2, { op: 'person', name: 'Giovanna', days }); ok(r.status === 200 && r.body.days.length >= 1 && r.body.days.length <= 62, `legacy days ${JSON.stringify(days)}: ${r.body.days && r.body.days.length}`); }
    for (const body of [{ issuesLimit: 0 }, { issuesLimit: -5 }, { issuesLimit: 1e9 }, { issuesLimit: 'x' }, { issuesCursor: 'garbage' }, { issuesCursor: '9999999999999~zzz' }, { issuesCursor: '5~' + 'z'.repeat(1000) }, { issuesCursor: ['a'] }, { issuesCursor: { a: 1 } }, { crossCheck: 'no' }, { compare: 'false' }, { compare: 0 }]) {
      const r = await person(s2, 'Giovanna', 'month', body); ok(r.status === 200 && r.body.issues && Array.isArray(r.body.issues.items) && r.body.issues.items.length <= 100, 'issues ' + JSON.stringify(body).slice(0, 40));
    }
    for (const orderId of ['', 'abc', '35.21e6', '../x', '3521000007; drop', {}, null, [], 5, [3521000007], '0', '9'.repeat(100)]) { const r = await call(s2, { op: 'orders', orderId }); noFive(r, 'orders ' + JSON.stringify(orderId).slice(0, 20)); ok([200, 400].includes(r.status)); }
    protoClean();
    say('  limits, cursors, 1,000-character and regex-looking searches, dates 0001..9999, ranges, overview and issues paging, legacy days, order numbers: no 5xx, bounded reads');
  }

  /* ═══ 3 · privacy ═══ */
  say('3 · privacy');
  {
    // a PIN typed into a name field: dropped by every station door and again by every reader
    const st = empty();
    const ev = (who, extra) => Object.assign({ id: 'evt-' + (++ipN) + '-' + NOW, station: 'welding', device: 'weld-1', person: who, action: 'complete', orderId: '3521999001', parts: 2, orders: 1, at: NOW - 5000, seq: ipN, sincePrevMs: 1000, detail: 'ok' }, extra);
    for (const nm of [`Paul ${PIN}`, 'Paul 4821', 'Pa 48 29 15', `Paul${PIN}7`, `${PIN} Paul`, 'Paul ٤٨٢٩١٥', 'Paul ４８２９１５']) {
      const r = await post(st, { activity: [ev(nm)] }); ok(r.status === 200 && r.body.written === 1, 'activity door ' + nm + ': ' + JSON.stringify(r.body));
    }
    for (const nm of [PIN, '48 29 15', '..', '', '   ']) { const r = await post(st, { activity: [ev(nm)] }); ok(r.body.written === 0 && r.body.refused === 1, 'activity door refuses a name that is only digits or punctuation: ' + JSON.stringify(nm) + ' ' + JSON.stringify(r.body)); }
    ok(st.all('Station_Activity').every(d => !/\d{4}/.test(d.person) && /\p{L}/u.test(d.person)) && st.all('Efficiency_Daily').every(d => !/\d{4}/.test(d.person) && !/\d{4}/.test(d._id.split('__')[1])), 'no stored activity or rollup name carries digits: ' + st.all('Efficiency_Daily').map(d => d._id) + ' | ' + JSON.stringify(st.all('Station_Activity').map(d => d.person)));
    for (const nm of [`Paul ${PIN}`, 'Pa 48 29 15']) { const r = await post(st, { session: { event: 'start', id: 'sess-' + (++ipN) + 'zz', person: nm, station: 'welding', device: 'weld-1', computerId: 'pc-ABCDEF01' } }); ok(r.status === 200, 'session door ' + nm); }
    for (const nm of [PIN, '48 29 15', '123456 789']) { const r = await post(st, { session: { event: 'start', id: 'sess-' + (++ipN) + 'yy', person: nm, station: 'welding', device: 'weld-1', computerId: 'pc-ABCDEF01' } }); ok(r.status === 400, 'session door refuses a digits-only name: ' + nm + ' ' + r.status); }
    ok(st.all('Station_Sessions').every(d => !/\d{4}/.test(d.person)), 'no stored session name carries digits');
    // the free text a station sends: a 6-digit number is a possible login number, so it is never stored or shown
    const nowWork = { v: 1, event: 'work', station: 'welding', device: 'weld-1', person: `Paul ${PIN}`, order: { kind: 'order', rid: '3521000777', scannedAt: NOW, note: `pin ${PIN} and 48 29 15`, customer: `Sam ${PIN}`, pieces: [{ id: 'abc_1', label: `x ${PIN}`, sku: 'AB-12' }] } };
    const lw = await post(st, { live: nowWork }); ok(lw.status === 200);
    const lv = await call(st, { op: 'live' });
    ok(lv.status === 200 && !lv.raw.includes(PIN) && !/\d{4}[^"]*"/.test(JSON.stringify(lv.body.stations.map(s => s.people))), 'the live board never shows a PIN: ' + JSON.stringify(lv.body.stations.filter(s => s.people.length).map(s => s.people)));
    const dtl = await post(st, { activity: [ev('Tess Welder', { detail: `typed ${PIN} then ok`, sku: PIN, line: PIN })] });
    ok(st.all('Station_Activity').filter(d => d.person === 'Tess Welder').every(d => !JSON.stringify(d).includes(PIN)), 'a digits-only sku or line is blanked, a 6-digit number in a detail becomes [#]: ' + JSON.stringify(st.all('Station_Activity').filter(d => d.person === 'Tess Welder')));
    // old documents written BEFORE the doors dropped digits still answer without them
    const old = empty();
    old.put('Efficiency_Daily', `${TODAY}__Old Name ${PIN}`, rollDoc(TODAY, `Old Name ${PIN}`, { welding: statOf({ parts: 6, completes: 3, orders: 3, scans: 3 }) }, rids(9300000001, 3)));
    old.put('Station_Sessions', 'old1', sessionDoc('old1', `Old Name ${PIN}`, 'welding', nyAt(TODAY, 9, 0), null));
    old.put('Station_Live', 'welding__x__Old', { id: 'welding__x__Old', v: 1, station: 'welding', device: PIN, person: `Old Name ${PIN}`, state: 'working', kind: 'order', rid: PIN, orderNumber: `No ${PIN}`, customer: `Sam ${PIN}`, title: PIN, note: `call ${PIN}`, beatAt: NOW, eventAt: NOW, sinceAt: NOW, scannedAt: NOW, pieces: [] });
    old.put('Station_Activity', 'old-e1', { id: 'old-e1', station: 'welding', device: PIN, person: `Old Name ${PIN}`, action: 'complete', orderId: '9300000001', parts: 2, orders: 1, detail: `see ${PIN}`, at: nyAt(TODAY, 10, 0), seq: 1, sincePrevMs: 0, ts: nyAt(TODAY, 10, 0), serverAt: nyAt(TODAY, 10, 0), day: TODAY, hour: '10', v: 1 });
    for (const body of [{ op: 'overview', days: 7 }, { op: 'live' }, { op: 'person', name: `Old Name ${PIN}`, range: 'week' }, { op: 'person', name: 'Old Name', range: 'week' }, { op: 'personOrders', name: 'Old Name' }, { op: 'orders', orderId: '9300000001' }]) {
      const r = await call(old, body); ok(r.status === 200 && !r.raw.includes(PIN), `${body.op}: an old stored name leaks ${PIN}`);
    }
    ok((await person(old, `Old Name ${PIN}`, 'week')).body.kpis.parts.value === 6 && (await person(old, 'Old Name', 'week')).body.kpis.parts.value === 6, 'the old and the new spelling are one person');
    // a PIN-like order number is not an order
    for (const orderId of [PIN, '1234', '12345678']) { const r = await call(old, { op: 'orders', orderId }); ok(r.status === 400 && r.body.error === 'that is not an order number' && !r.raw.includes(orderId), 'orderId ' + orderId + ': ' + r.status); }
    for (const nm of [PIN, '48 29 15', '٤٨٢٩١٥']) for (const op of ['person', 'personOrders']) { const r = await call(old, { op, name: nm, range: 'week' }); ok(r.status === 400 && !r.raw.includes(PIN), `${op} name ${nm}: ${r.status}`); }
    for (const nm of ['Paul 4821', `Paul ${PIN}`, 'Pa 48 29 15']) for (const op of ['person', 'personOrders']) { const r = await call(old, { op, name: nm, range: 'week' }); ok(r.status === 200 && !/\d{4}/.test(r.body.name) && !r.raw.includes(PIN), `${op} name ${nm} is echoed without its digits: ${r.body.name}`); }
    // the Employee Numbers document is never read, whatever is asked, and its numbers never come back
    const en = build().st; en.put('Brites_Orders', 'Employee Numbers', { 'Giovanna C.': PIN2, 'Michael_V': PIN, list: [PIN2, PIN] });
    for (const body of [{ op: 'overview', days: 30 }, { op: 'live' }, { op: 'person', name: 'Giovanna', range: 'quarter' }, { op: 'person', name: 'Michael V.', range: 'month' }, { op: 'personOrders', name: 'Giovanna', q: 'jane' }, { op: 'orders', orderId: '3521000007' }, { op: 'person', name: 'Employee Numbers', range: 'week' }, { op: 'personOrders', name: PIN2 }, { op: 'person', name: 'Brites_Orders', range: 'week' }]) {
      const r = await call(en, body); ok(r.status < 500 && !r.raw.includes(PIN2) && !r.raw.includes(PIN), `${body.op}: Employee Numbers leaked`);
    }
    ok(en.reads.every(r => r.name !== 'Brites_Orders'), 'nothing reads Brites_Orders: ' + [...new Set(en.reads.map(r => r.name))]);
    say('  PIN in a name (activity, session, live doors and old stored names), 6-digit numbers in free text, PIN-like order numbers, Employee Numbers never read: ok');
  }
  {
    // sandbox and real never mix
    const { st } = build();
    const sbNames = ['Sandy Tester'], realOnly = ['Ana M.', 'Michael V.', 'Ivy Y.', 'Empress D.', 'Paul K.'];
    const all = async (sandbox) => {
      const o = {}; const sb = sandbox ? { sandbox: true } : {};
      o.overview = await call(st, Object.assign({ op: 'overview', days: 7 }, sb)); o.live = await call(st, Object.assign({ op: 'live' }, sb));
      o.person = await call(st, Object.assign({ op: 'person', name: 'Sandy Tester', range: 'week' }, sb)); o.personG = await call(st, Object.assign({ op: 'person', name: 'Giovanna', range: 'week' }, sb));
      o.orders = await call(st, Object.assign({ op: 'personOrders', name: 'Giovanna', limit: 100 }, sb)); o.ord = await call(st, Object.assign({ op: 'orders', orderId: '3521000007' }, sb));
      o.sandy = await call(st, Object.assign({ op: 'personOrders', name: 'Sandy Tester', limit: 100 }, sb));
      return o;
    };
    const real = await all(false), box = await all(true);
    ok(real.overview.body.people.every(p => !sbNames.includes(p.name)) && !real.person.body.found && !real.sandy.body.found && !real.live.raw.includes('Sandy'), 'the real answers never show the sandbox people');
    ok(real.live.body.mode === 'real' && box.live.body.mode === 'sandbox' && real.personG.body.mode === 'real' && box.person.body.mode === 'sandbox' && real.orders.body.mode === 'real' && box.orders.body.mode === 'sandbox', 'modes are named');
    ok(box.overview.body.people.every(p => !realOnly.includes(p.name)) && box.overview.body.people.length === 2, 'the sandbox shows only its own people: ' + box.overview.body.people.map(p => p.name));
    ok(box.person.body.found && box.person.body.kpis.parts.value === 3000, 'sandbox Sandy: 3 days x 1000 loud parts: ' + box.person.body.kpis.parts.value);
    ok(box.personG.body.kpis.parts.value === 3000 && real.personG.body.kpis.parts.value === 248, `Giovanna in the sandbox (${box.personG.body.kpis.parts.value}) is not Giovanna in the shop (${real.personG.body.kpis.parts.value})`);
    ok(!box.orders.raw.includes('Jane Smith') && box.orders.body.orders.every(o => !o.info && o.customer === '' && o.thumbUrl === ''), 'the sandbox has no receipts, customers or pictures');
    ok(box.ord.body.steps.length === 0 && real.ord.body.steps.length === 1, 'an order of the shop is not in the sandbox, and the other way round');
    ok(real.overview.body.business.totals.parts === 1057 && box.overview.body.business.totals.parts === 6000, `totals: shop ${real.overview.body.business.totals.parts}, sandbox ${box.overview.body.business.totals.parts}`);
    // order numbers that exist on both sides: the picture and the buyer of each side stay on that side (the live board once cached the first side asked)
    const x = empty();
    x.put('Design_Order_Archive', '3521000555', { buyer: { name: 'Real Buyer' }, items: [{ transactionId: '1', sku: 'ZZ-1', mirrorUrl: 'https://real.example.com/real-photo.jpg' }] });
    x.put('Sandbox_Design_Order_Archive', '3521000555', { buyer: { name: 'Sandbox Buyer' }, items: [{ transactionId: '1', sku: 'ZZ-1', mirrorUrl: 'https://sandbox.example.com/sb-photo.jpg' }] });
    x.put('Charm_Master_Index', 'ZZ-1', { thumbUrl: 'https://catalog.example.com/zz-1-thumb.png' });      // (the design catalog is shared reference data: the Sandbox has no copy of it, the same SKU has the same design in both)
    const W = who => ({ v: 1, event: 'work', station: 'welding', device: 'weld-1', person: who, order: { kind: 'order', rid: '3521000555', scannedAt: NOW, pieces: [{ id: 'a_1', label: 'x', sku: 'ZZ-1' }] } });
    await post(x, { live: W('Real Person') }); await post(x, { live: W('Sandy Tester') }, { sandbox: true });
    for (const order of [[true, false], [false, true]]) {
      for (const sandbox of order) { tick(1000); const r = await call(x, { op: 'live', sandbox }); const c = r.body.stations.find(s => s.key === 'welding').current;
        ok(c.length === 1 && c[0].customer === (sandbox ? 'Sandbox Buyer' : 'Real Buyer') && c[0].photoUrl.includes(sandbox ? 'sb-photo' : 'real-photo') && c[0].thumbUrl.includes('catalog.example.com') && !r.raw.includes(sandbox ? 'real.example.com' : 'sandbox.example.com'), `live ${sandbox ? 'sandbox' : 'real'} after ${order[0] ? 'sandbox' : 'real'}: ${JSON.stringify(c.map(z => [z.person, z.customer]))}`); }
    }
    // a document in the wrong store, or with a wrong flag, never crosses over
    const f = empty();
    f.put('Efficiency_Daily', `${TODAY}__Poison Real`, rollDoc(TODAY, 'Poison Real', { welding: statOf({ parts: 500, completes: 1, orders: 1, scans: 1 }) }, ['9400000001'], { sandbox: true }));
    f.put('Sandbox_Efficiency_Daily', `${TODAY}__Poison Box`, rollDoc(TODAY, 'Poison Box', { welding: statOf({ parts: 600, completes: 1, orders: 1, scans: 1 }) }, ['9400000002']));
    f.put('Efficiency_Daily', `${TODAY}__Fine Real`, rollDoc(TODAY, 'Fine Real', { welding: statOf({ parts: 5, completes: 1, orders: 1, scans: 1 }) }, ['9400000003']));
    f.put('Sandbox_Efficiency_Daily', `${TODAY}__Fine Box`, rollDoc(TODAY, 'Fine Box', { welding: statOf({ parts: 6, completes: 1, orders: 1, scans: 1 }) }, ['9400000004'], { sandbox: true }));
    f.put('Station_Live', 'welding__weld-1__Poison Real', { id: 'welding__weld-1__Poison Real', v: 1, station: 'welding', device: 'weld-1', person: 'Poison Real', state: 'working', kind: 'order', rid: '9400000001', beatAt: NOW, eventAt: NOW, sinceAt: NOW, sandbox: true, pieces: [] });
    f.put('Sandbox_Station_Live', 'welding__weld-2__Poison Box', { id: 'welding__weld-2__Poison Box', v: 1, station: 'welding', device: 'weld-2', person: 'Poison Box', state: 'working', kind: 'order', rid: '9400000002', beatAt: NOW, eventAt: NOW, sinceAt: NOW, pieces: [] });
    const fr = await call(f, { op: 'overview', days: 1 }), fb = await call(f, { op: 'overview', days: 1, sandbox: true });
    ok(fr.body.people.map(p => p.name).join() === 'Fine Real' && fb.body.people.map(p => p.name).join() === 'Fine Box', `a flag that disagrees with the store keeps a document out: ${fr.body.people.map(p => p.name)} / ${fb.body.people.map(p => p.name)}`);
    const lr = await call(f, { op: 'live' }), lb = await call(f, { op: 'live', sandbox: true });
    ok(!lr.raw.includes('Poison') && !lb.raw.includes('Poison'), 'the live board drops a live document whose flag disagrees with its store');
    say('  sandbox and real: people, totals, orders, receipts, live board, pictures, buyers, poisoned flags: never mixed');
  }

  /* ═══ 4 · odd data ═══ */
  say('4 · odd data');
  {
    // daylight saving through the real doors: events and sign-ins around the repeated hour (1 Nov, 25 h) and the missing hour (8 Mar, 23 h)
    const dst = empty();
    let seq = 0;
    const evAt = async (t, extra) => { setNow(t + 2000); const r = await post(dst, { activity: [Object.assign({ id: 'evt-dst-' + (++seq) + '-' + t, station: 'welding', device: 'weld-1', person: 'Dst Tester', action: 'complete', orderId: String(3521100000 + seq), parts: 2, orders: 1, at: t, seq, sincePrevMs: 1000, detail: '' }, extra)] }); ok(r.status === 200 && r.body.written === 1, 'event at ' + new Date(t).toISOString() + ': ' + JSON.stringify(r.body)); };
    for (const t of [Date.UTC(2026, 10, 1, 3, 59, 59), Date.UTC(2026, 10, 1, 4, 0, 0), Date.UTC(2026, 10, 1, 5, 30), Date.UTC(2026, 10, 1, 6, 30), Date.UTC(2026, 10, 2, 4, 59, 59), Date.UTC(2026, 10, 2, 5, 0, 0)]) await evAt(t);   // 23:59:59 EDT Oct 31, 00:00 EDT, 01:30 EDT, 01:30 EST, 23:59:59 EST Nov 1, 00:00 EST Nov 2
    ok(dst.get('Efficiency_Daily', '2026-10-31__Dst Tester').stations.welding.parts === 2 && dst.get('Efficiency_Daily', '2026-11-01__Dst Tester').stations.welding.parts === 8 && dst.get('Efficiency_Daily', '2026-11-02__Dst Tester').stations.welding.parts === 2, 'rollups by New York day: 2 / 8 / 2');
    ok(dst.get('Efficiency_Daily', '2026-11-01__Dst Tester').hours['01'].parts === 4, 'both 01:30s (EDT and EST) fall in hour 01 of 1 Nov: ' + JSON.stringify(dst.get('Efficiency_Daily', '2026-11-01__Dst Tester').hours));
    dst.put('Station_Sessions', 'dst-a', sessionDoc('dst-a', 'Dst Tester', 'welding', Date.UTC(2026, 10, 1, 2, 0), Date.UTC(2026, 10, 1, 8, 0)));       // 22:00 EDT Oct 31 -> 03:00 EST Nov 1
    dst.put('Station_Sessions', 'dst-b', sessionDoc('dst-b', 'Dst Tester', 'welding', Date.UTC(2026, 10, 1, 13, 0), Date.UTC(2026, 10, 1, 21, 0)));      // 08:00 -> 16:00 EST
    setNow(Date.UTC(2026, 10, 3, 20, 0));
    let p = (await call(dst, { op: 'person', name: 'Dst Tester', range: { from: '2026-10-30', to: '2026-11-02' } })).body;
    const sg = d => p.series.find(s => s.day === d);
    ok(!p.partial && sg('2026-10-31').parts === 2 && sg('2026-11-01').parts === 8 && sg('2026-11-02').parts === 2, 'person series by day: ' + JSON.stringify(p.series.map(s => [s.day, s.parts])));
    ok(sg('2026-10-31').signedMs === 2 * 3600000 - 1000 || sg('2026-10-31').signedMs === 2 * 3600000, 'the night session: 2 h on 31 Oct (22:00 to midnight): ' + sg('2026-10-31').signedMs / 3600000);
    ok(sg('2026-11-01').signedMs === 12 * 3600000, 'the same session is 4 real hours on the 25-hour day (00:00 EDT to 03:00 EST), plus 8 h: ' + sg('2026-11-01').signedMs / 3600000);
    ok(p.hours.find(h => h.hour === 1).parts === 4 && p.hours.reduce((n, h) => n + h.parts, 0) === 12, 'hours of the day add up to every event: ' + JSON.stringify(p.hours.filter(h => h.parts)));
    const po = (await call(dst, { op: 'personOrders', name: 'Dst Tester', from: '2026-10-30', to: '2026-11-02', limit: 50 })).body;
    ok(po.total === 6 && po.orders.filter(o => o.day === '2026-11-01').length === 4, 'orders by day: ' + JSON.stringify(po.orders.map(o => o.day)));
    const ov1 = (await call(dst, { op: 'overview', days: 1, day: '2026-11-01' })).body, ov4 = (await call(dst, { op: 'overview', days: 4, day: '2026-11-02' })).body;
    ok(ov1.business.totals.parts === 8 && ov4.business.totals.parts === 12 && ov4.business.totals.orders === 6, `overview: the 25-hour day ${ov1.business.totals.parts} parts, four days ${ov4.business.totals.parts} parts / ${ov4.business.totals.orders} orders`);
    // sign-outs and heartbeats across midnight are capped at the New York midnight, however long that day is
    for (const [label, startUTC, nowUTC, cap] of [['fall back, 00:30 EDT', Date.UTC(2026, 10, 1, 4, 30), Date.UTC(2026, 10, 2, 12, 0), Date.UTC(2026, 10, 2, 5, 0)], ['spring forward, 00:30 EST', Date.UTC(2026, 2, 8, 5, 30), Date.UTC(2026, 2, 9, 12, 0), Date.UTC(2026, 2, 9, 4, 0)], ['after the repeated hour', Date.UTC(2026, 10, 1, 7, 30), Date.UTC(2026, 10, 2, 12, 0), Date.UTC(2026, 10, 2, 5, 0)], ['23:50 the night before', Date.UTC(2026, 10, 1, 3, 50), Date.UTC(2026, 10, 1, 12, 0), Date.UTC(2026, 10, 1, 4, 0)]]) {
      const sx = empty(); setNow(startUTC);
      const base = { id: 'sess-' + label.replace(/\W/g, '').slice(0, 8) + 'x1', person: 'Dst Tester', station: 'welding', device: 'weld-1', computerId: 'pc-DSTTEST01' };
      await post(sx, { session: Object.assign({ event: 'start' }, base) }); setNow(nowUTC);
      const r = await post(sx, { session: Object.assign({ event: 'end', reason: 'signOut', at: nowUTC }, base) });
      ok(r.body.endAt === cap && r.body.endReason === 'midnight', `${label}: the sign-out is closed at New York midnight (${new Date(r.body.endAt).toISOString()})`);
    }
    // spring forward through the person page: a 00:30 EST to 12:00 EDT shift is 10.5 real hours, all on 8 Mar
    const sp = empty(); setNow(Date.UTC(2026, 2, 8, 7, 0, 2));
    for (const t of [Date.UTC(2026, 2, 8, 6, 59, 59), Date.UTC(2026, 2, 8, 7, 0, 0), Date.UTC(2026, 2, 9, 3, 59, 59), Date.UTC(2026, 2, 9, 4, 0, 0)]) { setNow(t + 2000); await post(sp, { activity: [{ id: 'evt-sp-' + t, station: 'welding', device: 'weld-1', person: 'Dst Tester', action: 'complete', orderId: String(3521200000 + (t % 1000)), parts: 3, orders: 1, at: t, seq: 1, sincePrevMs: 1000, detail: '' }] }); }
    sp.put('Station_Sessions', 'sp-1', sessionDoc('sp-1', 'Dst Tester', 'welding', Date.UTC(2026, 2, 8, 5, 30), Date.UTC(2026, 2, 8, 16, 0)));
    setNow(Date.UTC(2026, 2, 10, 20, 0));
    p = (await call(sp, { op: 'person', name: 'Dst Tester', range: { from: '2026-03-07', to: '2026-03-09' } })).body;
    ok(sg('2026-03-08').parts === 9 && sg('2026-03-08').signedMs === 10.5 * 3600000 && sg('2026-03-09').parts === 3, 'spring forward: ' + JSON.stringify(p.series.map(s => [s.day, s.parts, s.signedMs / 3600000])));
    ok(p.hours.find(h => h.hour === 1).parts === 3 && p.hours.find(h => h.hour === 3).parts === 3 && p.hours.find(h => h.hour === 2).parts === 0, 'hour 02 does not exist on 8 Mar: ' + JSON.stringify(p.hours.filter(h => h.parts)));
    say('  1 Nov (25 h) and 8 Mar (23 h): events by day and hour, sign-in time, orders, overview, sign-out caps at midnight: ok');
  }
  {
    // an order scanned before midnight and finished after it; two people at one station; one person under three spellings; sign-in only; 0 and 300 pieces
    const st = empty();
    const A = 'Night Owl', d1 = '2026-10-01', d2 = '2026-10-02';
    const putEv = (id, who, action, at, o) => st.put('Station_Activity', id, Object.assign({ id, station: 'welding', device: 'weld-1', person: who, action, orderId: '9500000001', parts: 0, orders: 0, detail: '', at, seq: 1, sincePrevMs: 60000, ts: at, serverAt: at, day: T.nyDay(at), hour: '00', v: 1 }, o));
    const t1 = nyAt(d1, 23, 58), t2 = nyAt(d2, 0, 3);
    putEv('no-1', A, 'scan', t1, { parts: 3 }); putEv('no-2', A, 'complete', t2, { parts: 3, orders: 1 });
    st.put('Efficiency_Daily', `${d1}__${A}`, rollDoc(d1, A, { welding: statOf({ scans: 1, scanParts: 3, activeMs: 60000, firstAt: t1, lastAt: t1 }) }, ['9500000001'], { firstAt: t1, lastAt: t1 }));
    st.put('Efficiency_Daily', `${d2}__${A}`, rollDoc(d2, A, { welding: statOf({ completes: 1, parts: 3, orders: 1, activeMs: 60000, firstAt: t2, lastAt: t2 }) }, ['9500000001'], { firstAt: t2, lastAt: t2 }));
    st.put('Station_Sessions', 'no-s', sessionDoc('no-s', A, 'welding', nyAt(d1, 22, 0), nyAt(d2, 1, 0)));
    const wk = await person(st, A, { from: d1, to: d2 }), olist = await ordersOf(st, A, { from: d1, to: d2 }), ov = await call(st, { op: 'overview', days: 2, day: d2 });
    const row = olist.body.orders[0];
    ok(wk.body.kpis.orders.value === 1 && olist.body.total === 1 && ov.body.business.totals.orders === 1, `an order across midnight is ONE order in every total: person ${wk.body.kpis.orders.value}, list ${olist.body.total}, overview ${ov.body.business.totals.orders}`);
    ok(wk.body.kpis.parts.value === 3 && row.parts === 3 && ov.body.business.totals.parts === 3, 'and its pieces count once (finished on the second day)');
    ok(row.day === d2, 'the list shows it on the day of its last action: ' + row.day);
    ok(wk.body.series.find(s => s.day === d1).signedMs === 2 * 3600000 && wk.body.series.find(s => s.day === d2).signedMs === 3600000, 'a 22:00 to 01:00 sign-in is 2 h and 1 h on the two days');

    // two people at one station
    const tw = empty();
    for (const [who, dev, parts, n] of [['Twin One', 'weld-1', 30, 10], ['Twin Two', 'weld-2', 50, 20]]) {
      tw.put('Efficiency_Daily', `${TODAY}__${who}`, rollDoc(TODAY, who, { welding: statOf({ parts, completes: n, orders: n, scans: n + 2, activeMs: 3600000, firstAt: nyAt(TODAY, 9, 5), lastAt: nyAt(TODAY, 14, 50) }) }, rids(9600000001 + (who === 'Twin Two' ? 1000 : 0), n)));
      tw.put('Station_Sessions', 'tw-' + dev, sessionDoc('tw-' + dev, who, 'welding', nyAt(TODAY, 9, 0), null, { device: dev }));
    }
    const twO = (await call(tw, { op: 'overview', days: 1 })).body, twL = (await call(tw, { op: 'live' })).body, ws = twO.business.stations.find(s => s.station === 'welding'), wl = twL.stations.find(s => s.key === 'welding');
    ok(ws.parts === 80 && ws.orders === 30 && ws.peopleNow.length === 2 && twO.business.totals.people === 2, 'overview: both people count at the station: ' + JSON.stringify(ws));
    ok(wl.counts.partsToday === 80 && wl.counts.ordersToday === 30 && wl.people.length === 2 && twL.signedIn.length === 2, 'live board: same numbers, both people: ' + JSON.stringify(wl.counts));
    for (const [who, parts] of [['Twin One', 30], ['Twin Two', 50]]) { const r = (await person(tw, who, 'day')).body; ok(r.kpis.parts.value === parts && r.kpis.orders.value === (parts === 30 ? 10 : 20), who + ' has only his own pieces: ' + r.kpis.parts.value + ' / ' + r.kpis.orders.value); ok(r.kpis.signedHours.value > 5 && r.kpis.signedHours.value < 6.1, who + ' signed time is his own, not doubled: ' + r.kpis.signedHours.value); }

    // one person, three spellings
    const sp3 = empty();
    for (const [nm, parts, n, base] of [['Mary Jo', 10, 4, 9700000000], ['mary jo', 20, 6, 9700001000], ['MARY_JO', 30, 8, 9700002000]]) sp3.put('Efficiency_Daily', `${TODAY}__${nm}`, rollDoc(TODAY, nm, { welding: statOf({ parts, completes: n, orders: n, scans: n }) }, rids(base, n)));
    sp3.put('Station_Sessions', 'mj1', sessionDoc('mj1', 'Mary Jo', 'welding', nyAt(TODAY, 9, 0), null)); sp3.put('Station_Sessions', 'mj2', sessionDoc('mj2', 'MARY_JO', 'welding', nyAt(TODAY, 9, 30), null, { computerId: 'pc-MJTWO001' }));
    const mo = (await call(sp3, { op: 'overview', days: 1 })).body;
    ok(mo.people.length === 1 && mo.people[0].totals.parts === 60 && mo.business.totals.people === 1, 'one person, 60 parts: ' + JSON.stringify(mo.people.map(x => [x.name, x.totals.parts])));
    for (const nm of ['Mary Jo', 'mary_jo', 'MARY JO']) { const r = (await person(sp3, nm, 'day')).body; ok(r.kpis.parts.value === 60 && r.spellings.length === 3 && r.kpis.signedHours.value < 6.1, `${nm}: ${r.kpis.parts.value} parts, ${r.spellings.length} spellings, ${r.kpis.signedHours.value} h`); }
    ok((await ordersOf(sp3, 'mary jo', { limit: 100 })).body.total === 18, 'order list: 18 orders under the three spellings');

    // signed in and did nothing; a person with no logs at all
    const sg = build().st;
    sg.put('Station_Sessions', 'idle-1', sessionDoc('idle-1', 'Idle Ivan', 'welding', nyAt(TODAY, 8, 0), null)); sg.put('Station_Sessions', 'idle-2', sessionDoc('idle-2', 'Idle Ivan', 'welding', nyAt('2026-10-01', 8, 0), nyAt('2026-10-01', 16, 0)));
    const id = await person(sg, 'Idle Ivan', 'week'), nobody = await person(sg, 'Nobody Here', 'week');
    ok(id.status === 200 && id.body.found && id.body.kpis.signedHours.value > 14 && id.body.kpis.parts.value === null && id.body.kpis.orders.value === null, `signed in, nothing logged: ${id.body.kpis.signedHours.value} h, pieces ${id.body.kpis.parts.value} (null: the rollup says nothing was logged, which is not the same as "made none")`);
    ok(id.body.kpis.partsPerActiveHour.value === null && id.body.kpis.partsPerSignedHour.value === null && id.body.kpis.secPerOrderMean.value === null, 'no rate is divided by zero: ' + JSON.stringify([id.body.kpis.partsPerActiveHour.value, id.body.kpis.partsPerSignedHour.value, id.body.kpis.secPerOrderMean.value]));
    ok(nobody.status === 200 && nobody.body.found === false && nobody.body.kpis.parts.value === null && nobody.body.kpis.signedHours.value === null, 'no logs at all: unknown, not zero: ' + JSON.stringify([nobody.body.found, nobody.body.kpis.parts.value, nobody.body.kpis.signedHours.value]));
    const idO = (await call(sg, { op: 'overview', days: 7 })).body; ok(idO.people.find(x => x.name === 'Idle Ivan') && idO.people.find(x => x.name === 'Idle Ivan').source === 'sessions', 'overview lists the person who only signed in');

    // 0 pieces, 300 pieces, a missing picture
    const bp = empty();
    const rb = [['9800000001', 0], ['9800000002', 300], ['9800000003', 2]];
    bp.put('Efficiency_Daily', `${TODAY}__Big Batch`, rollDoc(TODAY, 'Big Batch', { assembly: statOf({ parts: 302, completes: 3, orders: 3, scans: 3 }) }, rb.map(x => x[0])));
    rb.forEach(([rid, q], i) => bp.put('Station_Activity', 'bb-' + i, { id: 'bb-' + i, station: 'assembly', device: 'assembly-1', person: 'Big Batch', action: 'complete', orderId: rid, parts: q, orders: 1, detail: '', at: nyAt(TODAY, 10, i), seq: i, sincePrevMs: 60000, ts: nyAt(TODAY, 10, i), serverAt: nyAt(TODAY, 10, i), day: TODAY, hour: '10', v: 1 }));
    bp.put('EtsyMail_Receipts', '9800000002', { receipt_id: '9800000002', buyer_name: 'Bulk Buyer', raw: { name: 'Bulk Buyer', transactions: [{ transaction_id: 1, listing_id: 5550001, sku: 'BULK-1', title: 'Bulk charm ' + 'x'.repeat(400), quantity: 300 }] } });
    bp.put('EtsyMail_Receipts', '9800000003', { receipt_id: '9800000003', buyer_name: 'Pic Less', raw: { name: 'Pic Less', transactions: [{ transaction_id: 2, listing_id: 5550002, sku: 'NOPIC', title: 'No picture', quantity: 2 }] } });
    bp.put('Etsy_Listing_Image_Cache', '5550001', { images: [{ rank: 1, url_570xN: 'https://i.etsystatic.com/fake/bulk.jpg' }] });
    const bo = (await ordersOf(bp, 'Big Batch', { limit: 10 })).body, by = rid => bo.orders.find(o => o.rid === rid);
    ok(bo.total === 3 && !bo.partial && by('9800000001').parts === 0 && by('9800000002').parts === 300 && by('9800000003').parts === 2, 'orders of 0, 300 and 2 pieces: ' + JSON.stringify(bo.orders.map(o => [o.rid, o.parts])));
    ok(by('9800000002').piecesCount === 300 && by('9800000002').pieces.length === 12 && by('9800000002').info === true && by('9800000002').thumbUrl.startsWith('https://'), '300 pieces: the true count, 12 listed, with the picture: ' + by('9800000002').piecesCount + '/' + by('9800000002').pieces.length);
    ok(by('9800000003').info === true && by('9800000003').thumbUrl === '' && by('9800000003').pieces.length === 2 && by('9800000003').pieces.every(p => p.thumbUrl === ''), 'a missing picture is an empty thumbUrl, not an error, and the order is not partial');
    ok(by('9800000001').info === false && by('9800000001').piecesCount === 0 && by('9800000001').customer === '', 'no receipt: no pieces, no customer, info false');
    ok(by('9800000002').pieces.every(p => p.label.length <= 60 || p.label === p.sku) && JSON.stringify(by('9800000002')).length < 6000, 'a long title does not bloat the row');
    const bs = (await ordersOf(bp, 'Big Batch', { q: 'bulk' })).body; ok(bs.total === 1 && bs.orders[0].rid === '9800000002', 'search finds the customer');
    const bk = (await person(bp, 'Big Batch', 'day')).body; ok(bk.kpis.parts.value === 302 && bk.kpis.orders.value === 3, 'person: 302 pieces in 3 orders: ' + bk.kpis.parts.value);
    say('  across midnight, two at one station, three spellings, signed in only, no logs, 0 / 300 pieces, missing pictures: ok');
  }

  /* ═══ 5 · outages ═══ */
  say('5 · outages');
  {
    const cols = ['Efficiency_Daily', 'Station_Sessions', 'Station_Activity', 'EtsyMail_Receipts', 'config', 'Station_Live', 'Order_Timeline', 'Charm_Master_Index', 'Design_Order_Archive', 'Etsy_Listing_Image_Cache', 'EtsyMail_Listings'];
    const bodiesFor = [{ op: 'person', name: 'Giovanna', range: 'week' }, { op: 'person', name: 'Giovanna', range: 'month' }, { op: 'overview', days: 1 }, { op: 'overview', days: 7 }, { op: 'live' }, { op: 'personOrders', name: 'Giovanna', limit: 5 }, { op: 'personOrders', name: 'Giovanna', q: 'jane', limit: 5 }, { op: 'orders', orderId: '3521000007' }];
    let n = 0, partial = 0, unavailable = 0;
    for (const c of cols) for (const b of bodiesFor) {
      const { st } = build();
      await post(st, { live: { v: 1, event: 'work', station: 'welding', device: 'weld-1', person: 'Giovanna C.', order: { kind: 'order', rid: '3521000777', scannedAt: NOW, pieces: [{ id: 'a_1', label: 'x', sku: 'CH-MOON-GF', listingId: '901001' }] } } });
      st.put('Charm_Master_Index', 'CH-MOON-GF', { thumbUrl: 'https://example.com/moon.png' }); st.put('Design_Order_Archive', '3521000777', { buyer: { name: 'Dana Q' }, items: [{ transactionId: '1', sku: 'CH-MOON-GF', mirrorUrl: 'https://example.com/a.jpg' }] });
      st.fail(c); st.clear();
      const r = await call(st, b); n++;
      ok(r.status === 200 || r.status === 503, `${c} down, ${JSON.stringify(b).slice(0, 40)}: status ${r.status}`);
      if (r.status === 503) { unavailable++; ok(r.body.ok === false && Array.isArray(r.body.errors) && r.body.errors.length, 'a 503 names what failed'); continue; }
      const touched = st.reads.some(x => x.name === c);
      if (touched) { ok(r.body.partial === true && Array.isArray(r.body.errors) && r.body.errors.length > 0, `${c} was read, failed, and the answer is not partial: ${b.op} ${JSON.stringify(b).slice(0, 50)}`); partial++; }
    }
    say(`  ${n} cases (each of ${cols.length} sources down for ${bodiesFor.length} calls): ${partial} partial answers with a named error, ${unavailable} 503s, no 5xx, no silent success`);
    // unknown is null, never 0, on the person page
    const lost = async (c, extra) => { const { st } = build(); st.fail(c); return (await call(st, Object.assign({ op: 'person', name: 'Giovanna', range: 'week' }, extra || {}))).body; };
    let p = await lost('Efficiency_Daily');
    ok(p.partial && p.kpis.parts.value === null && p.kpis.orders.value === null && p.kpis.scans.value === null && p.kpis.signedHours.value > 0 && p.series.every(s => s.parts === null || s.parts === undefined) && p.calendar.every(c => c.parts === null || c.parts === undefined || c.state === 'unknown'), 'no rollups: pieces, orders and scans are null, sign-in time is real: ' + JSON.stringify([p.kpis.parts.value, p.kpis.orders.value, p.kpis.signedHours.value]));
    p = await lost('Station_Sessions');
    ok(p.partial && p.kpis.signedHours.value === null && p.kpis.parts.value === 248 && p.kpis.orders.value === 98, 'no sessions: signed time is null, pieces are real: ' + JSON.stringify([p.kpis.signedHours.value, p.kpis.parts.value]));
    p = await lost('Station_Activity');
    ok(p.partial && p.kpis.parts.value === 248 && p.kpis.signedHours.value === 38 && p.issues.total === null, 'no events: the issues block is null, the rest is real: ' + p.issues.total);
    p = await lost('config');
    ok(p.partial && p.errors.some(e => /^aliases:/.test(e)) && p.kpis.parts.value === 248, 'no alias list: partial, named, same numbers');
    // a source that heals comes back at once (a failed read is not remembered)
    const h = build().st; h.fail('Efficiency_Daily'); const bad = await person(h, 'Giovanna', 'week'); h.heal('Efficiency_Daily'); const good = await person(h, 'Giovanna', 'week');
    ok(bad.body.partial && !good.body.partial && good.body.kpis.parts.value === 248, 'healed: the next call is whole');
    const hp = build().st; hp.fail('Etsy_Listing_Image_Cache'); const a1 = await ordersOf(hp, 'Giovanna', { limit: 5 }); hp.heal('Etsy_Listing_Image_Cache'); hp.clear(); const a2 = await ordersOf(hp, 'Giovanna', { limit: 5 });
    ok(a1.body.partial && a1.body.errors.includes('pictures: unreadable') && !a2.body.partial && a2.body.orders.some(o => o.thumbUrl), 'pictures that could not be read are partial, and are read again (not kept for 30 minutes) once the source is back');
    // the live board during an outage: counts are null (unknown), not 0
    const lv = build().st; lv.fail('Efficiency_Daily'); const lr = (await call(lv, { op: 'live' })).body;
    ok(lr.partial && lr.stations.every(s => s.counts.partsToday === null && s.counts.ordersToday === null && s.counts.scansToday === null), 'live counts are null when today could not be read: ' + JSON.stringify(lr.stations[0].counts));
    // the overview says what happened: an outage is not "no activity yet"
    const ovd = build().st; ovd.fail('Efficiency_Daily'); const od = (await call(ovd, { op: 'overview', days: 7 })).body;
    ok(od.partial && od.errors.some(e => /^rollups:/.test(e)) && od.notes.some(x => /could not be read/.test(x)) && !od.notes.some(x => /have not been recorded/.test(x)), 'an outage is named as an outage: ' + JSON.stringify(od.notes));
    say('  unknown numbers are null (person page, live board), heals at once, the overview says "could not be read" and not "no activity yet": ok');
  }

  /* ═══ 6 · cost ═══ */
  say('6 · cost');
  {
    const G = 'Giovanna';
    const dayReadTwice = st => {
      const seen = new Map(); let rollDup = 0, evDup = 0, sessDup = 0;
      for (const r of st.readsOf('Efficiency_Daily').filter(r => r.filters.length === 2)) for (let d = r.vals[0]; d <= r.vals[1]; d = S.addDays(d, 1)) { seen.set(d, (seen.get(d) || 0) + 1); }
      for (const n of seen.values()) if (n > 1) rollDup++;
      const ev = new Map(); for (const r of st.readsOf('Station_Activity').filter(r => r.filters.length === 2 && r.order == null)) { const k = r.vals.join('|'); ev.set(k, (ev.get(k) || 0) + 1); }
      for (const n of ev.values()) if (n > 1) evDup++;
      const ss = new Map(); for (const r of st.readsOf('Station_Sessions').filter(r => r.filters.length >= 2)) { const k = r.filters.join(',') + r.vals.join('|') + (r.ts ? 'ts' : 'ms'); ss.set(k, (ss.get(k) || 0) + 1); }      // (a session range is asked twice on purpose: once as numbers, once as Firestore times)
      for (const n of ss.values()) if (n > 1) sessDup++;
      return { rollDup, evDup, sessDup };
    };
    let { st } = build();
    await person(st, G, 'week'); const cold = st.docsRead();
    ok(cold < 900, 'a cold week and the week before, with attendance and issues: ' + cold + ' documents (documented: 727 for the profile alone, +E10 order look-ups)');
    ok(st.readsOf('Efficiency_Daily').length <= 3 && st.readsOf('config').length <= 2, 'rollups in at most 3 range queries, config at most 2 reads: ' + st.readsOf('Efficiency_Daily').length + '/' + st.readsOf('config').length);
    let d = dayReadTwice(st); ok(d.rollDup === 0 && d.evDup === 0 && d.sessDup === 0, 'no day, person-day or session range is read twice: ' + JSON.stringify(d));
    st.clear(); await person(st, G, 'week'); ok(st.docsRead() === 0, 'the same answer again: 0 documents, ' + st.docsRead());
    tick(6500); st.clear(); await person(st, G, 'week'); ok(st.docsRead() < 40 && st.docsOf('Station_Activity') === 0, 'a live week 6.5 s later: today only, ' + st.docsRead());
    st.clear(); await person(st, 'Ana M.', 'week'); ok(st.docsOf('Efficiency_Daily') === 0 && st.docsOf('Station_Sessions') === 0 && st.docsRead() < 700, 'another person: no rollup and no session read: ' + st.docsRead());
    st.clear(); await person(st, G, 'month'); d = dayReadTwice(st); ok(d.rollDup === 0 && d.evDup === 0, 'a month after a week: the days of the week are not read again: ' + JSON.stringify(d) + ' ' + st.docsRead());
    ({ st } = build()); await person(st, G, 'year'); ok(st.docsRead() < 1700 && st.readsOf('Efficiency_Daily').length <= 8, 'a cold year and the year before: ' + st.docsRead() + ' documents, ' + st.readsOf('Efficiency_Daily').length + ' rollup queries'); d = dayReadTwice(st); ok(d.rollDup === 0 && d.evDup === 0, 'a year: nothing twice ' + JSON.stringify(d));
    ({ st } = build()); await ordersOf(st, G, { limit: 25 }); ok(st.docsRead() < 600, 'the first page of orders: ' + st.docsRead());
    st.clear(); await ordersOf(st, G, { limit: 25, cursor: 'o25' }); ok(st.docsRead() < 200 && st.docsOf('Efficiency_Daily') === 0, 'the next page: ' + st.docsRead());
    tick(6000); st.clear(); await ordersOf(st, G, { limit: 25 }); ok(st.docsRead() < 15 && st.docsOf('EtsyMail_Receipts') === 0, 'a poll of the first page 6 s later: ' + st.docsRead());
    ({ st } = build()); await call(st, { op: 'overview', days: 1, trend: false }); ok(st.docsRead() < 700, 'overview of a day, cold: ' + st.docsRead());
    for (let i = 0; i < 3; i++) { tick(5100); st.clear(); await call(st, { op: 'overview', days: 1, trend: false }); ok(st.docsRead() < 40, 'overview poll every 5 s: ' + st.docsRead()); }
    ({ st } = build()); await call(st, { op: 'live' }); ok(st.docsRead() < 40, 'live board cold: ' + st.docsRead());
    st.clear(); await call(st, { op: 'live' }); await call(st, { op: 'live' }); ok(st.docsRead() === 0, 'live polls inside 5 s read nothing: ' + st.docsRead());
    st.clear(); for (let i = 0; i < 200; i++) { tick(3000); await call(st, { op: 'live' }); }
    ok(st.docsRead() / 200 < 6 && st.reads.length / 200 < 3, 'a live poll every 3 s averages ' + (st.docsRead() / 200).toFixed(1) + ' documents and ' + (st.reads.length / 200).toFixed(1) + ' reads');
    for (const op of [{ op: 'person', name: G, range: 'week' }, { op: 'overview', days: 7 }, { op: 'personOrders', name: G, limit: 5 }]) { const w = build().st; await call(w, op); ok(w.writes.length === 0, op.op + ' writes nothing'); }
    // a 100-person shop: 8 days of sign-ins, rollups and events
    EP.resetCache(); NOW = S.NOW; const big = fakeStore(); curDb = big.db;
    const STS = ['sorting', 'welding', 'assembly', 'shipping'];
    for (let q = 0; q < 100; q++) {
      const who = 'Worker' + String.fromCharCode(65 + (q % 26)) + String.fromCharCode(65 + Math.floor(q / 26)) + ' T.', station = STS[q % 4];
      for (let k = 0; k < 8; k++) {
        const day = S.addDays(TODAY, -k); if (S.dow(day) === 0 || S.dow(day) === 6) continue;
        const t0 = nyAt(day, 8, q % 30), isToday = k === 0, ends = isToday ? NOW - 60000 : nyAt(day, 16, 0), touched = {};
        big.put('Station_Sessions', `s-${day}-${q}`, sessionDoc(`s-${day}-${q}`, who, station, t0, isToday ? null : ends, { computerId: 'pc-' + q }));
        for (let i = 0; i < 12; i++) {
          const rid = String(3521000000 + q * 100000 + k * 100 + i); touched[rid] = { [station]: true }; const at = t0 + (i + 1) * 600000;
          big.put('Station_Activity', `e-${q}-${k}-${i}`, { id: `e-${q}-${k}-${i}`, station, device: station + '-1', person: who, action: i % 2 ? 'complete' : 'scan', orderId: rid, parts: 2, orders: i % 2 ? 1 : 0, at, seq: i, sincePrevMs: 600000, ts: at + 1000, serverAt: at + 1000, day, hour: '09', v: 1 });
        }
        big.put('Efficiency_Daily', `${day}__${who}`, rollDoc(day, who, { [station]: statOf({ scans: 6, scanParts: 12, completes: 6, parts: 12, orders: 6, activeMs: 3600000, firstAt: t0, lastAt: ends }) }, Object.keys(touched).filter((_, i) => i % 2 === 1)));
      }
    }
    // (each worker finishes 6 orders and 12 pieces a day; 100 workers x 5 weekdays in the last 7 days)
    let r = await call(big, { op: 'overview', days: 7 });
    ok(r.status === 200 && r.size < 120000 && r.body.people.length === 60 && r.body.business.totals.people === 100 && r.body.business.totals.parts === 100 * 5 * 12, `100 people, a week: ${r.status}, ${r.size} bytes, ${r.body.people.length} listed of ${r.body.business.totals.people}, ${r.body.business.totals.parts} pieces`);
    ok(r.body.partial === true && r.body.notes.some(x => /people/.test(x)), 'the 60-person cut is named, never silent');
    big.clear(); tick(5100); r = await call(big, { op: 'overview', days: 1, trend: false }); ok(r.status === 200 && r.size < 120000 && big.docsRead() < 1200, '100 people, a day (poll): ' + big.docsRead() + ' documents, ' + r.size + ' bytes');
    big.clear(); r = await call(big, { op: 'live' }); ok(r.status === 200 && r.size < 40000 && big.docsRead() < 450, '100 people, live board: ' + big.docsRead() + ' documents, ' + r.size + ' bytes');
    big.clear(); r = await call(big, { op: 'person', name: 'WorkerAA T.', range: 'week' }); ok(r.status === 200 && r.size < 150000 && r.body.kpis.parts.value === 60 && big.docsRead() < 3200, '100 people, one person page: ' + big.docsRead() + ' documents, ' + r.size + ' bytes, ' + r.body.kpis.parts.value + ' pieces');
    big.clear(); r = await call(big, { op: 'personOrders', name: 'WorkerAA T.', limit: 25 }); ok(r.status === 200 && r.body.total === 36 && big.docsRead() < 800, '100 people, order list: ' + big.docsRead());
    say('  documented budgets hold; nothing read twice; polls are cheap; 100-person shop: bounded reads and bytes (the 60-person cut is named)');
  }

  /* ═══ 7 · consistency ═══ */
  say('7 · consistency');
  {
    const { st } = build();
    const names = ['Giovanna', 'Michael V.', 'Ana M.', 'Ivy Y.', 'Empress D.', 'Paul K.'];
    let checked = 0;
    for (const [range, days, day] of [['week', 7, TODAY], ['month', 30, TODAY], ['week', 7, '2026-10-02'], ['day', 1, '2026-10-01'], ['day', 1, TODAY]]) {
      const ov = (await call(st, { op: 'overview', days, day })).body, from = S.addDays(day, -(days - 1));
      for (const n of names) {
        const p = (await person(st, n, range, { day })).body, o = ov.people.find(x => x.name === n);
        const ord = []; let cursor = '';
        for (let i = 0; i < 60; i++) { const r = (await call(st, { op: 'personOrders', name: n, from, to: day, limit: 100, cursor })).body; ord.push(...r.orders); if (!r.next) break; cursor = r.next; }
        const sParts = p.series.reduce((a, s) => a + (s.parts || 0), 0), sOrders = p.series.reduce((a, s) => a + (s.orders || 0), 0);
        const listParts = ord.reduce((a, r) => a + r.parts, 0), why = `${n} ${range} to ${day}`;
        const known = p.kpis.parts.value !== null;
        if (known) {
          ok(p.kpis.parts.value === sParts && p.kpis.parts.value === listParts, `${why}: pieces ${p.kpis.parts.value} / series ${sParts} / order list ${listParts}`);
          ok(p.kpis.orders.value === sOrders && p.kpis.orders.value === ord.length, `${why}: orders ${p.kpis.orders.value} / series ${sOrders} / order list ${ord.length}`);
          ok(!o || (o.totals.parts === p.kpis.parts.value && o.totals.orders === p.kpis.orders.value), `${why}: overview ${o && o.totals.parts}/${o && o.totals.orders}`);
          ok(p.stations.reduce((a, s) => a + s.parts, 0) === p.kpis.parts.value, `${why}: stations add up to the pieces`);
          ok(o ? Math.abs(p.kpis.signedHours.value - Math.round(o.totals.signedInMin / 6) / 10) <= 0.11 : true, `${why}: signed time`);
          // E9's calendar is known only from the day sign-in logging began (2 Oct 2026): from there the same pieces and orders
          const cal = p.calendar.filter(c => c.day >= '2026-10-02');
          ok(cal.reduce((a, c) => a + (c.parts || 0), 0) === (p.series.filter(s => s.day >= '2026-10-02').reduce((a, s) => a + (s.parts || 0), 0)), `${why}: calendar pieces from 2 Oct`);
        } else ok(sParts === 0 && ord.length === 0 && (!o || o.totals.parts === 0), `${why}: an inbox-only person has no pieces anywhere (profile null, series and overview 0)`);
        checked++;
      }
    }
    // the live board and the overview of the day count the same things
    const live = (await call(st, { op: 'live' })).body, ov = (await call(st, { op: 'overview', days: 1 })).body;
    for (const s of live.stations) { const o = ov.business.stations.find(x => x.station === s.key); if (!o) { ok(s.counts.partsToday === 0 && s.counts.ordersToday === 0 && s.counts.scansToday === 0, s.key + ' (not in the overview) counts nothing'); continue; }
      ok(s.counts.partsToday === o.parts && s.counts.ordersToday === o.orders && s.counts.scansToday === o.scans && JSON.stringify(s.people.slice().sort()) === JSON.stringify(o.peopleNow.slice().sort()), `${s.key}: live ${JSON.stringify(s.counts)} people ${s.people} vs overview ${o.parts}/${o.orders}/${o.scans} ${o.peopleNow}`); }
    ok(JSON.stringify(live.signedIn.map(x => x.name).sort()) === JSON.stringify(ov.people.filter(x => x.status === 'on').map(x => x.name).sort()), 'signed in now: the live board and the overview name the same people');
    say(`  ${checked} person x window combinations: person = series = order list = overview = stations; live = overview: ok`);
  }

  /* ═══ 8 · fuzz ═══ */
  say('8 · fuzz');
  {
    let total = 0;
    for (const seed0 of [12345, 7, 2026]) {
      let seed = seed0; const rnd = () => { seed = (seed * 1664525 + 1013904223) >>> 0; return seed / 4294967296; };
      const pick = a => a[Math.floor(rnd() * a.length)];
      const junk = () => pick([null, undefined, '', 'x', `Paul ${PIN}`, PIN, 0, -1, 1e15, NaN, Infinity, -Infinity, true, [], {}, [1, 2], { a: 1 }, '2026-10-05', 1791226800000, '1791226800000', 'o9', '__proto__', { __proto__: null }, 'a'.repeat(300)]);
      const garbage = (depth = 0) => { const k = rnd(); if (depth > 2 || k < 0.5) return junk(); if (k < 0.75) { const o = {}; for (let i = 0; i < 1 + Math.floor(rnd() * 4); i++) o[pick(['parts', 'orders', 'scans', 'stations', 'welding', 'hours', '9', 'by', 'touched', 'firstAt', 'lastAt', 'day', 'person', 'pieces', 'id', '3521000007', 'activeMs', 'undoParts'])] = garbage(depth + 1); return o; } const a = []; for (let i = 0; i < Math.floor(rnd() * 4); i++) a.push(garbage(depth + 1)); return a; };
      const { st } = build();
      const colls = ['Efficiency_Daily', 'Station_Sessions', 'Station_Activity', 'Station_Live', 'EtsyMail_Receipts'];
      for (let i = 0; i < 300; i++) {
        const c = pick(colls);
        st.put(c, `fz-${i}`, { day: pick(['2026-10-05', '2026-10-04', garbage(), '2026-10-01']), person: pick(['Giovanna C.', 'Fuzz Tester', garbage(), `Paul ${PIN}`]), station: pick(['welding', 'assembly', 'inbox', garbage()]), device: garbage(), action: pick(['scan', 'complete', 'undo', garbage()]),
          at: pick([NOW - 1000, garbage()]), ts: pick([NOW - 1000, garbage()]), serverAt: garbage(), startAt: pick([NOW - 3600000, garbage()]), lastSeenAt: garbage(), endAt: pick([null, garbage()]), beatAt: pick([NOW, garbage()]), state: pick(['working', 'idle', garbage()]), rid: pick(['3521000555', garbage()]), kind: garbage(),
          stations: garbage(), hours: garbage(), touched: garbage(), pieces: garbage(), events: garbage(), firstAt: garbage(), lastAt: garbage(), orderId: pick(['3521000007', garbage()]), parts: garbage(), orders: garbage(), detail: garbage(), buyer: garbage(), raw: garbage(), images: garbage(), items: garbage(), sandbox: pick([undefined, true, false, garbage()]) });
      }
      st.put('config', 'employeeAliases', { A: ['B'], B: ['A'], Giovanna: garbage(), C: ['D', 'E', 'C'], [PIN]: ['x'], constructor: ['__proto__'], toString: 12 });
      st.put('config', 'employeeSchedule', { closedDays: garbage(), openDays: garbage(), closedWeekdays: garbage() });
      const list = [{ op: 'overview', days: 1 }, { op: 'overview', days: 7 }, { op: 'overview', days: 30 }, { op: 'live' }, { op: 'live', sandbox: true }, { op: 'overview', sandbox: true, days: 7 }, { op: 'person', name: 'Giovanna', range: 'month' }, { op: 'person', name: 'Fuzz Tester', range: 'week' }, { op: 'person', name: 'Giovanna', days: 14 }, { op: 'personOrders', name: 'Giovanna', q: 'jane' }, { op: 'personOrders', name: 'Fuzz Tester' }, { op: 'orders', orderId: '3521000555' }, { op: 'orders', orderId: '3521000007' }, { op: 'person', name: 'Giovanna', range: 'year', sandbox: true }, { op: 'person', name: 'A', range: 'week' }, { op: 'person', name: 'B', range: 'week' }];
      for (const b of list) { st.clear(); tick(60000); const r = await call(st, b); total++; ok(r.status < 500 && !r.weird, `seed ${seed0} ${JSON.stringify(b)}: ${r.status} ${r.raw.slice(0, 100)}`); ok(!r.raw.includes(PIN), `seed ${seed0} ${JSON.stringify(b)} leaks a number`); }
    }
    say(`  ${total} calls over 3 seeds of 300 garbage documents each (and garbage alias and schedule documents): no 5xx, no PIN`);
  }

  /* ═══ the end: the passcode never left ═══ */
  ok(!bodies.some(b => b.includes(PASS)), 'the passcode is in no answer');
  ok(!logs.some(l => l.includes(PASS)), 'the passcode is in no log');
  protoClean();
  say(`\nall adversarial checks passed (${logs.length} log lines, ${bodies.length} answers, ${(Number(process.hrtime.bigint() - t0) / 1e9).toFixed(1)} s)`);
})().catch(e => { process.stdout.write('FAILED: ' + (e && e.stack || e) + '\n'); process.exit(1); });
