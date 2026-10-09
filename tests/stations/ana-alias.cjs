// Ana_M and Anna are ONE person (Paul, 9 Oct 2026: "Ana_M and Anna are the same person. Moving forward record both under Ana_M and
// standardize her login under Ana_M for all applications"). Offline: Firestore is an in-memory fake, every number here is made up
// when the test runs and is never printed (the checks compare booleans and names only).
//   1 · the table: Anna, Anns, "Ana M.", ana_m (any case, spacing, accent) are Ana_M; another person ("Anna K.", "Ana P.", "Ana", Michael_V, Paul K) is untouched
//   2 · the login door: a record that says "Anna", "Ana M." or "Ana_M" answers { ok, name: "Ana_M" }; a record "Giovanna C." or "Michael_V" is answered as stored
//   3 · WRITE: the activity door stores person Ana_M (and the rollup id day__Ana_M), the session door stores a session of Ana_M, the live board and the order
//       timeline carry Ana_M, the laser sheet key is one key; Giovanna C. and Ivy_Y are NOT folded at write time (they are merged when read only)
//   4 · READ: stored records named Anna (a rollup, a session, an event) and named Ana_M are ONE person in the console (overview, person by any spelling,
//       the feed, an order's steps), shown as "Ana M." like every console name; Anns joins her too; the sign-ins window's rows say Ana_M;
//       Giovanna C. still joins Giovanna and Ivy Y. joins Ivy; Michael_V and Michael T. stay apart; nothing is written; the roster lists her once
//   node tests/stations/ana-alias.cjs
'use strict';
const path = require('path'), assert = require('assert'), Module = require('module');
const root = path.join(__dirname, '../..');
const K = require(path.join(root, 'netlify/functions/_activityKinds.js'));
const say = s => process.stdout.write(s + '\n');

/* ── fake Firestore (typed fields, where / orderBy / limit, transactions) ── */
class Ts { constructor(m) { this.m = m; } toMillis() { return this.m; } static fromMillis(m) { return new Ts(m); } }
const kind = v => v instanceof Ts ? 'ts' : typeof v === 'number' ? 'num' : typeof v === 'string' ? 'str' : 'other';
const val = v => v instanceof Ts ? v.m : v;
const keep = v => v instanceof Ts ? v : Array.isArray(v) ? v.map(keep) : v && typeof v === 'object' ? Object.fromEntries(Object.entries(v).map(([k, x]) => [k, keep(x)])) : v;
function fakeStore() {
  const colls = new Map(), writes = [];
  const data = n => { if (!colls.has(n)) colls.set(n, new Map()); return colls.get(n); };
  function query(name, filters, order, lim) {
    return {
      where: (f, op, v) => query(name, filters.concat([[f, op, v]]), order, lim),
      orderBy: (f, d) => query(name, filters, [f, d || 'asc'], lim),
      limit: n => query(name, filters, order, n),
      select: () => query(name, filters, order, lim),
      get: async () => {
        let docs = [...data(name)].map(([id, d]) => ({ id, d }));
        for (const [f, op, v] of filters) docs = docs.filter(({ d }) => {
          const x = d[f]; if (x === undefined || kind(x) !== kind(v)) return false;
          const a = val(x), b = val(v);
          return op === '==' ? a === b : op === '>=' ? a >= b : op === '>' ? a > b : op === '<' ? a < b : op === '<=' ? a <= b : false;
        });
        if (order) { const [f, dir] = order; docs = docs.filter(({ d }) => d[f] !== undefined).sort((p, q) => (val(p.d[f]) < val(q.d[f]) ? -1 : val(p.d[f]) > val(q.d[f]) ? 1 : 0) * (dir === 'desc' ? -1 : 1)); }
        if (lim != null) docs = docs.slice(0, lim);
        return { docs: docs.map(({ id, d }) => ({ id, data: () => keep(d) })), size: docs.length, empty: !docs.length };
      }
    };
  }
  const refOf = (name, id) => ({ id, path: name + '/' + id,
    get: async () => { const d = data(name).get(id); return { exists: !!d, data: () => keep(d), id }; },
    set: async (v, o) => { writes.push([name, id]); data(name).set(id, o && o.merge && data(name).get(id) ? Object.assign({}, data(name).get(id), keep(v)) : keep(v)); },
    update: async v => { if (!data(name).has(id)) throw Object.assign(new Error('5 NOT_FOUND'), { code: 5 }); writes.push([name, id]); data(name).set(id, Object.assign({}, data(name).get(id), keep(v))); },
    create: async v => { writes.push([name, id]); data(name).set(id, keep(v)); } });
  const db = {
    collection: name => Object.assign(query(name, [], null, null), { doc: id => refOf(name, id) }),
    runTransaction: async fn => {
      const w = [];
      const out = await fn({ get: r => r.get(), set: (r, d, o) => w.push([r, d, o]), update: (r, d) => w.push([r, d, { merge: true }]) });
      for (const [r, d, o] of w) await r.set(d, o);
      return out;
    }
  };
  return { db, put: (name, id, d) => data(name).set(id, keep(d)), get: (name, id) => data(name).get(id), writes, colls, all: name => [...data(name)].map(([id, d]) => Object.assign({ _id: id }, d)) };
}
const S = fakeStore();
const fakeAdmin = { firestore: Object.assign(() => S.db, { Timestamp: Ts, FieldValue: { serverTimestamp: () => 'ts', increment: n => n, delete: () => null } }) };
const realLoad = Module._load;
Module._load = function (req, ...rest) { if (/[\/]firebaseAdmin(\.js)?$/.test(req)) return fakeAdmin; return realLoad.call(this, req, ...rest); };
const door = require(path.join(root, 'netlify/functions/firebaseOrders.js'));
const reader = require(path.join(root, 'netlify/functions/employeeEfficiency.js'));
const attendance = require(path.join(root, 'netlify/functions/_employeeAttendance.js'));
const EP = require(path.join(root, 'netlify/functions/_editPasscode.js'));
Module._load = realLoad;
const PIN = require(path.join(root, 'netlify/functions/_stationPinLogin.js'));
const ACT = require(path.join(root, 'netlify/functions/_stationActivity.js'));
const TL = require(path.join(root, 'netlify/functions/_orderTimeline.js'));
const LIVE = require(path.join(root, 'netlify/functions/_stationLive.js'));
const LASER = require(path.join(root, 'netlify/functions/_laserSheetTime.js'));
const T = reader._t;

const PASS = 'synthetic-ana-pass-4m8c';
process.env.EDIT_PASSCODE = PASS;
const logs = [], keepLog = (...a) => logs.push(a.map(x => typeof x === 'string' ? x : JSON.stringify(x)).join(' '));
console.warn = console.log = console.error = console.info = keepLog;
const realNow = Date.now; let NOW = Date.parse('2026-10-09T19:00:00Z');         // 15:00 on 9 Oct in New York (EDT)
Date.now = () => NOW;
const Z = iso => Date.parse(iso);
const eq = (a, b, m) => assert.deepStrictEqual(a, b, m);
let ipN = 0;
const call = async (st, body) => {
  EP.resetCache(); T.cacheOf(st.db).memo.clear(); T.cacheOf(st.db).recent.clear();
  const r = await T.handle({ httpMethod: 'POST', headers: { 'x-nf-client-connection-ip': '203.0.113.' + (++ipN % 250) }, body: JSON.stringify(Object.assign({ op: 'overview', key: PASS }, body)) }, st.db);
  assert.strictEqual(r.statusCode, 200, String(r.body).slice(0, 200));
  return JSON.parse(r.body);
};

(async () => {
  /* ═══ 1 · the table ═══ */
  for (const n of ['Anna', 'ANNA', ' anna ', 'Anns', 'ANNS', 'Ana_M', 'ana_m', 'Ana M.', 'ana m', 'Ána_M']) eq(K.loginName(n), 'Ana_M', 'one login name: ' + JSON.stringify(n));
  for (const n of ['Anna K.', 'Ana P.', 'Ana', 'Annabel', 'Michael_V', 'Paul K', 'Giovanna C.', 'Ivy_Y', 'Ivy Y.', 'Michael T.']) eq(K.loginName(n), n, 'another person is untouched at write time: ' + n);
  eq([K.personName('Anna'), K.personName('Anns'), K.personName('Giovanna C.'), K.personName('ivy y'), K.personName('Michael_V'), K.personName('Anna K.')], ['Ana_M', 'Ana_M', 'Giovanna', 'Ivy', 'Michael_V', 'Anna K.'], 'the read-side table: Anna and Anns are Ana_M; Giovanna C. and Ivy Y. as before; others as given');
  assert.deepStrictEqual(Object.keys(K.PEOPLE_ALIASES), ['Giovanna', 'Ana_M', 'Ivy'], 'the roster of the console: Giovanna, Ana_M, Ivy (Anna is no second person)');
  assert.strictEqual(K.loginName(undefined), undefined); assert.strictEqual(K.loginName(null), null);

  /* ═══ 2 · the login door ═══ */
  PIN.deps.sleep = async () => {};
  const used = new Set(), pin = () => { for (;;) { const p = String(100000 + Math.floor(Math.random() * 900000)); if (!used.has(p) && !/^(\d)\1{5}$/.test(p)) { used.add(p); return p; } } };
  const people = { 'Anna': pin(), 'Ana M.': pin(), 'Ana_M': pin(), 'Giovanna C.': pin(), 'Michael_V': pin() };
  S.put('Brites_Orders', 'Employee Numbers', Object.fromEntries(Object.entries(people).map(([n, p]) => [p, n])));
  let ip = 0;
  const login = async p => { const r = await PIN.pinLogin(S.db, { headers: { 'x-nf-client-connection-ip': '198.51.100.' + (++ip) }, body: '{}' }, p); return { status: r.statusCode, body: r.body }; };
  for (const [n, p] of Object.entries(people)) {
    const r = await login(p);
    assert.strictEqual(r.status === 200 && r.body.ok === true, true, 'the door knows the record ' + n);
    eq(r.body.name, /^Ana|^Anna$/.test(n) ? 'Ana_M' : n, 'the record "' + n + '" logs in as ' + (/^Ana|^Anna$/.test(n) ? 'Ana_M' : n));
    eq(Object.keys(r.body).sort(), ['name', 'ok'], 'name and ok, nothing else');
    assert.strictEqual(JSON.stringify(r).includes(p), false, 'the number is not echoed');
  }
  assert.strictEqual(S.get('Brites_Orders', 'Employee Numbers')[people['Anna']], 'Anna', 'the stored record is never changed');

  /* ═══ 3 · write ═══ */
  const mk = (id, person, station = 'sorting', device = 'sorting-1') => ({ id, station, device, person, action: 'scan', at: NOW, orderId: '3521000900', parts: 1 });
  for (const n of ['Anna', 'Anns', 'Ana M.', 'ANA_M']) { const r = ACT.clean(mk('sorting-1_AAAA_1_1700000000000', n), NOW, ''); assert.strictEqual(!r.refused && r.doc.person === 'Ana_M', true, 'the activity door stores Ana_M for ' + n); }
  eq(ACT.rollupId('2026-10-09', ACT.clean(mk('sorting-1_AAAA_2_1700000000000', 'Anna'), NOW, '').doc.person), '2026-10-09__Ana_M', 'the rollup is day__Ana_M');
  for (const n of ['Anna K.', 'Giovanna C.', 'Ivy_Y', 'Michael_V']) eq(ACT.clean(mk('sorting-1_AAAA_3_1700000000000', n), NOW, '').doc.person, n, 'the other people are stored as sent: ' + n);
  eq(ACT.clean(Object.assign(mk('weld-1_AAAA_4_1700000000000', 'Anna', 'welding', 'weld-1'), { unattributed: true }), NOW, '').doc.person, 'Unattributed', 'nobody stays nobody');
  eq(ACT.clean(mk('sorting-1_AAAA_5_1700000000000', 'Anna 482915'), NOW, '').doc.person, 'Ana_M', 'a PIN that slipped in after the name is dropped, then the name is folded');

  const post = body => door.handler({ httpMethod: 'POST', headers: { 'x-nf-client-connection-ip': '192.0.2.' + (++ipN % 250) }, queryStringParameters: {}, body: JSON.stringify(body) }).then(r => ({ status: r.statusCode, body: JSON.parse(r.body || '{}') }));
  const sess = (id, person, extra) => Object.assign({ id, event: 'start', person, station: 'sorting', device: 'sorting-1', computerId: 'pc-ANAMERGE1', at: NOW }, extra || {});
  eq((await post({ session: sess('sess-ana-1009-a', 'Anna') })).status, 200);
  eq((await post({ session: sess('sess-ana-1009-b', 'Ana M.', { station: 'welding', device: 'weld-1', task: 'welding' }) })).status, 200);
  eq((await post({ session: sess('sess-ana-1009-c', 'Michael_V', { computerId: 'pc-ANAMERGE2' }) })).status, 200);
  eq(['sess-ana-1009-a', 'sess-ana-1009-b', 'sess-ana-1009-c'].map(i => S.get('Station_Sessions', i).person), ['Ana_M', 'Ana_M', 'Michael_V'], 'the session door stores Ana_M for Anna and Ana M.; Michael_V as sent');
  // the end of a session found by its id still works (the person is not part of the key)
  eq((await post({ session: { id: 'sess-ana-1009-a', event: 'end', person: 'Anna', station: 'sorting', device: 'sorting-1', computerId: 'pc-ANAMERGE1', reason: 'signOut', at: NOW + 60000 } })).status, 200);
  assert.strictEqual(S.get('Station_Sessions', 'sess-ana-1009-a').endAt > 0, true, 'the session ended');

  const live = person => ({ event: 'idle', station: 'sorting', device: 'sorting-1', person });
  eq((await LIVE.write(S.db, fakeAdmin.firestore.FieldValue, live('Anna'), {}))[0], 200);
  assert.strictEqual(!!S.get('Station_Live', 'sorting__sorting-1__Ana_M') && !S.get('Station_Live', 'sorting__sorting-1__Anna'), true, 'the live board document is Ana_M\'s');
  const tl = TL.clean({ orderId: '3521000900', type: 'sorted', by: 'Anna', station: 'sorting', source: 'station' }, { source: 'station' });
  eq(tl.doc.by, 'Ana_M', 'the order timeline event says Ana_M'); eq(TL.clean({ orderId: '3521000900', type: 'sorted', by: 'Etsy' }).doc.by, 'Etsy'); eq(TL.clean({ orderId: '3521000900', type: 'sorted', by: 'Michael V.' }).doc.by, 'Michael V.');
  eq([LASER.personKey('Anna'), LASER.personKey('anna'), LASER.personKey('Ana_M'), LASER.personKey('Michael_V')], ['ana m', 'ana m', 'ana m', 'michael v'], 'the laser sheet clock is one key for her (and a key stored as "anna" reads as hers)');

  /* ═══ 4 · read: records stored under Anna are Ana_M's, nothing is rewritten ═══ */
  const st = fakeStore(); NOW = Z('2026-10-09T19:36:00Z');
  const D = '2026-10-09', T0 = Z('2026-10-09T13:19:00Z');                     // 9:19 AM in New York
  const stat = o => Object.assign({ scans: 0, scanParts: 0, completes: 0, parts: 0, orders: 0, prints: 0, rejects: 0, errors: 0, notes: 0, undos: 0, undoParts: 0, undoOrders: 0, activeMs: 0, idleMs: 0 }, o);
  const roll = (person, stations, touched) => ({ day: D, person, v: 1, events: 10, firstAt: T0, lastAt: T0 + 3600000, stations, hours: { '09': { parts: 3, by: {} } }, touched });
  const sess1 = (id, person, station, start, o = {}) => ({ id, person, station, device: station + '-1', computerId: 'pc-' + id.slice(0, 8).padEnd(8, 'X'), computerLabel: '', startAt: start, lastSeenAt: o.last == null ? start : o.last, endAt: o.end == null ? null : o.end, endReason: o.reason || null, minutes: 0 });
  // Paul's screenshot: "Anna" at Sorting 9:19 to 9:52 (ended by 10 minutes without input), "Ana_M" at Weld 2:19 PM to now, and an older "Anns" session
  st.put('Station_Sessions', 'anna-s1', sess1('anna-s1', 'Anna', 'sorting', T0, { end: T0 + 33 * 60000, last: T0 + 43 * 60000, reason: 'idle' }));
  st.put('Station_Sessions', 'anam-w1', sess1('anam-w1', 'Ana_M', 'welding', Z('2026-10-09T18:19:00Z'), { last: NOW }));
  st.put('Station_Sessions', 'anns-s1', sess1('anns-s1', 'Anns', 'assembly', Z('2026-10-09T11:00:00Z'), { end: Z('2026-10-09T11:20:00Z'), last: Z('2026-10-09T11:20:00Z'), reason: 'signOut' }));
  st.put('Station_Sessions', 'giov-s1', sess1('giov-s1', 'Giovanna C.', 'assembly', T0, { end: T0 + 1800000, last: T0 + 1800000, reason: 'signOut' }));
  st.put('Station_Sessions', 'mich-s1', sess1('mich-s1', 'Michael_V', 'shipping', T0, { end: T0 + 1800000, last: T0 + 1800000, reason: 'signOut' }));
  st.put('Station_Sessions', 'mict-s1', sess1('mict-s1', 'Michael T.', 'shipping', T0, { end: T0 + 1800000, last: T0 + 1800000, reason: 'signOut' }));
  st.put('Efficiency_Daily', D + '__Anna', roll('Anna', { sorting: stat({ scans: 4, scanParts: 4, completes: 3, parts: 7, orders: 3, activeMs: 600000 }) }, { 3521000901: { sorting: true }, 3521000902: { sorting: true }, 3521000903: { sorting: true } }));
  st.put('Efficiency_Daily', D + '__Ana_M', roll('Ana_M', { welding: stat({ scans: 2, scanParts: 2, matched: 2, activeMs: 60000 }) }, { 3521000904: { welding: true } }));
  st.put('Efficiency_Daily', D + '__Anns', roll('Anns', { assembly: stat({ scans: 1, scanParts: 1, completes: 1, parts: 2, orders: 1, activeMs: 60000 }) }, { 3521000905: { assembly: true } }));
  st.put('Efficiency_Daily', D + '__Giovanna C.', roll('Giovanna C.', { assembly: stat({ scans: 1, scanParts: 1, completes: 1, parts: 5, orders: 1, activeMs: 60000 }) }, { 3521000906: { assembly: true } }));
  st.put('Efficiency_Daily', D + '__Michael_V', roll('Michael_V', { shipping: stat({ scans: 1, scanParts: 1, completes: 1, parts: 4, orders: 1, activeMs: 60000 }) }, { 3521000907: { shipping: true } }));
  const ev1 = { id: 'sorting-1_AAAA_1_1', station: 'sorting', device: 'sorting-1', computer: 'pc-AAAA', session: '', person: 'Anna', action: 'complete', orderId: '3521000901', line: '', sku: '', parts: 7, orders: 1, detail: '', at: T0 + 60000, seq: 1, sincePrevMs: 0, ts: Ts.fromMillis(T0 + 60000), serverAt: T0 + 60000, day: D, hour: '09', v: 1 };
  st.put('Station_Activity', ev1.id, ev1);
  const shape = () => [...st.colls.keys()].filter(n => st.colls.get(n).size).map(n => [n, [...st.colls.get(n).keys()].sort().join('|')]);
  const before = shape();
  const a = await call(st, { day: D });
  const names = a.people.map(p => p.name);
  assert.strictEqual(names.filter(n => /^Ana/.test(n)).length, 1, 'one Ana in the console: ' + names.join(', '));
  assert.strictEqual(names.includes('Ana M.') && !names.includes('Anna') && !names.includes('Anns') && !names.includes('Ana_M'), true, 'she is shown as Ana M., like every console name (no Anna, no Anns)');
  const ana = a.people.find(p => p.name === 'Ana M.');
  eq([ana.totals.parts, ana.totals.scans], [9, 7 - 0], 'her parts and scans are the sum of Anna, Ana_M and Anns (7 + 2 parts; 4 + 2 + 1 scans)');
  eq(ana.stations.map(s => s.station).sort(), ['assembly', 'sorting', 'welding'], 'her stations: Sorting from the Anna records, Welding from Ana_M, Assembly from Anns');
  eq(ana.totals.signedInMin, 33 + 20 + Math.round((NOW - Z('2026-10-09T18:19:00Z')) / 60000), 'the sign-ins add up (Anna 33 min at Sorting, Anns 20, Ana_M since 2:19 PM): one row of time, not three');
  assert.strictEqual(a.people.some(p => p.name === 'Giovanna') && a.people.some(p => p.name === 'Michael V.') && a.people.some(p => p.name === 'Michael T.'), true, 'Giovanna C. joins Giovanna; the two Michaels stay apart');
  eq(a.people.find(p => p.name === 'Giovanna').totals.parts, 5);
  assert.strictEqual(a.feed.every(f => f.person !== 'Anna'), true, 'the feed says Ana M.'); eq(a.feed.filter(f => f.person === 'Ana M.').length, 1);
  for (const spelling of ['Anna', 'Ana_M', 'Ana M.', 'ana m', 'ANNS']) { const p = await call(st, { op: 'person', name: spelling, day: D, days: 1 }); eq([p.name, p.totals.parts], ['Ana M.', 9], 'asking for "' + spelling + '" gives the same person'); }
  const ord = await call(st, { op: 'orders', orderId: '3521000901' });
  eq(ord.steps.map(s => s.person), ['Ana M.'], "an order Anna worked shows Ana M. as its person");
  // the roster: she is listed (not signed in) when nobody of her signed in; once, never as a second name
  const empty = fakeStore(); const r0 = await call(empty, { day: D, roster: true });
  eq(r0.absent, ['Giovanna', 'Ana M.', 'Ivy'], 'the always-listed names: Giovanna, Ana M. once (not Anna), Ivy');
  eq((await call(st, { day: D, roster: true })).absent, ['Ivy'], 'with her records of the day (stored as Anna, Ana_M, Anns) she is not listed as absent');
  // the attendance reader agrees with the console reader
  const al = attendance._t.buildAliases(null), key = attendance._t.keyFn(al);
  eq([key('Anna'), key('Ana_M'), key('Anns'), key('Ana M.')].every(k => k === key('Ana_M')), true, 'the attendance reader folds the same way'); assert.strictEqual(al.display.get(key('Anna')), 'Ana M.');
  // the sign-ins window's rows (charmNestLibrary sessionRow) say Ana_M for a session stored as Anna
  eq(K.personName('Anna'), 'Ana_M', 'the sign-ins window lists the Anna session under Ana_M (as the screenshot lists Ana_M)');
  eq(shape(), before, 'nothing was written or deleted by any read (the stored records still say Anna)');
  assert.strictEqual(st.writes.length, 0, 'zero writes');
  assert.strictEqual(logs.join('\n').includes(PASS), false, 'no passcode in a log');
  Date.now = realNow;
  say('ana-alias: the table, the login door (Anna, Ana M., Ana_M -> Ana_M), write (activity, session, live, timeline, laser key), read (one Ana M. in the console from Anna/Ana_M/Anns records, any spelling, roster, sign-ins), others untouched, nothing written');
  say('OK');
})().catch(e => { Date.now = realNow; process.stdout.write('FAIL ' + (e && e.stack || e) + '\n'); process.exit(1); });
