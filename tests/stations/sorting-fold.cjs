// ONE Sorting station (Paul, 6 Oct 2026, stations round 2, worker PB1): the Sorter app (station key "sorter") and the QR Printer page
// (station key "qr") are devices of the Sorting station. What was stored under those keys stays exactly as it is (history is permanent);
// the portal folds the keys WHEN IT READS, through displayStation(key) (server: netlify/functions/_activityKinds.js; client mirror:
// EfficiencyStations.displayStation). Offline: Firestore is an in-memory fake, the clock is faked, every passcode and name is invented.
//   1 · displayStation: sorter and qr are sorting, every other key (laser, design, welding, inbox ...) is itself; server, stations module and console agree
//   2 · the Overview, the person page (both forms), the order list, one order, the issues and the live board read OLD events, rollups,
//       sessions, seals and live documents stored under sorter and qr: they appear under Sorting only, the totals are the sum of the
//       stations, a person at the Sorter app and at a sorting page is one person (time counted once, listed once), an order finished at
//       both pages is one order, no answer names a Sorter or QR Printer station anywhere
//   3 · a Sorter-app session of a Laser or Design person (stored as laser / design, device charm-nest-1) is NOT Sorting's
//   4 · history untouched: every stored document is the same after all the reads, and there were no writes
//   5 · the client models (EfficiencyStations.norm, Efficiency.norm / normLive / normOrder) fold an older answer too
//   6 · in a real browser: the Stations board of an answer that still carries Sorter and QR Printer rows draws ONE Sorting card (no Sorter or QR Printer card)
//   JSDOM_DIR=<...>/node_modules node tests/stations/sorting-fold.cjs     (PW_DIR=<playwright node_modules>, CHROMIUM=<chrome> for part 6)
'use strict';
const fs = require('fs'), path = require('path'), assert = require('assert'), http = require('http'), Module = require('module');
const root = path.join(__dirname, '../..');
const clone = x => x == null ? x : JSON.parse(JSON.stringify(x));

/* ── fake Firestore (typed fields, where / orderBy / limit, Timestamp; every write is recorded) ── */
class Ts { constructor(m) { this.m = m; } toMillis() { return this.m; } static fromMillis(m) { return new Ts(m); } }
const kind = v => v instanceof Ts ? 'ts' : typeof v === 'number' ? 'num' : typeof v === 'string' ? 'str' : 'other';
const val = v => v instanceof Ts ? v.m : v;
function fakeStore() {
  const colls = new Map(), reads = [], writes = [];
  const data = n => { if (!colls.has(n)) colls.set(n, new Map()); return colls.get(n); };
  const keep = v => v instanceof Ts ? v : Array.isArray(v) ? v.map(keep) : v && typeof v === 'object' ? Object.fromEntries(Object.entries(v).map(([k, x]) => [k, keep(x)])) : v;
  function query(name, filters, order, lim) {
    return {
      where: (f, op, v) => query(name, filters.concat([[f, op, v]]), order, lim),
      orderBy: (f, d) => query(name, filters, [f, d || 'asc'], lim),
      limit: n => query(name, filters, order, n),
      get: async () => {
        let docs = [...data(name)].map(([id, d]) => ({ id, d }));
        for (const [f, op, v] of filters) docs = docs.filter(({ d }) => {
          const x = d[f]; if (x === undefined || kind(x) !== kind(v)) return false;
          const a = val(x), b = val(v);
          return op === '==' ? a === b : op === '>=' ? a >= b : op === '>' ? a > b : op === '<' ? a < b : op === '<=' ? a <= b : false;
        });
        if (order) { const [f, dir] = order; docs = docs.filter(({ d }) => d[f] !== undefined).sort((p, q) => (val(p.d[f]) < val(q.d[f]) ? -1 : val(p.d[f]) > val(q.d[f]) ? 1 : 0) * (dir === 'desc' ? -1 : 1)); }
        if (lim != null) docs = docs.slice(0, lim);
        reads.push({ name, n: docs.length });
        return { docs: docs.map(({ id, d }) => ({ id, data: () => keep(d) })), size: docs.length, empty: !docs.length };
      }
    };
  }
  const db = { collection: name => Object.assign(query(name, [], null, null), {
    doc: id => ({ id,
      get: async () => { reads.push({ name, doc: id }); const d = data(name).get(id); return { exists: !!d, data: () => keep(d) }; },
      set: async v => { writes.push([name, id, 'set']); data(name).set(id, keep(v)); },
      update: async v => { writes.push([name, id, 'update']); data(name).set(id, Object.assign({}, data(name).get(id), keep(v))); },
      create: async v => { writes.push([name, id, 'create']); data(name).set(id, keep(v)); } }) }) };
  const dump = () => JSON.stringify([...colls].filter(([, m]) => m.size).sort((a, b) => (a[0] < b[0] ? -1 : 1)).map(([n, m]) => [n, [...m].sort((a, b) => (a[0] < b[0] ? -1 : 1))]));
  return { db, put: (name, id, d) => data(name).set(id, keep(d)), reads, writes, dump, colls };
}

/* ── the modules under test, over the fake admin ── */
const fakeAdmin = { firestore: Object.assign(() => ({}), { Timestamp: Ts, FieldValue: { serverTimestamp: () => 'ts' } }) };
const realLoad = Module._load;
Module._load = function (req, ...rest) { if (/[\/]firebaseAdmin(\.js)?$/.test(req)) return fakeAdmin; return realLoad.call(this, req, ...rest); };
const eff = require(path.join(root, 'netlify/functions/employeeEfficiency.js'));
const EP = require(path.join(root, 'netlify/functions/_editPasscode.js'));
const K = require(path.join(root, 'netlify/functions/_activityKinds.js'));
Module._load = realLoad;
const T = eff._t;

const PASS = 'synthetic-pass-9f3k';
process.env.EDIT_PASSCODE = PASS;
const logs = [], keepLog = (...a) => logs.push(a.map(x => typeof x === 'string' ? x : JSON.stringify(x)).join(' '));
console.warn = console.log = console.error = console.info = keepLog;
const say = (...a) => process.stdout.write(a.join(' ') + '\n');
const NOW = Date.parse('2026-10-05T15:00:00Z');                     // 11:00 on Monday 5 Oct in New York (EDT)
Date.now = () => NOW;
const Z = iso => Date.parse(iso);
const DAY = '2026-10-05', YDAY = '2026-10-04';
let ipN = 0;
async function ask(st, body) {
  const r = await T.handle({ httpMethod: 'POST', headers: { 'x-nf-client-connection-ip': '203.0.113.' + (++ipN % 250) }, body: JSON.stringify(Object.assign({ op: 'overview', key: PASS }, body)) }, st.db);
  assert.strictEqual(r.statusCode, 200, `${body.op || 'overview'}: ${r.body}`);
  return JSON.parse(r.body);
}

/* ── the invented shop: everything the Sorter app and the QR Printer ever stored, next to what Sorting stored ── */
const stat = o => Object.assign({ scans: 0, scanParts: 0, completes: 0, parts: 0, orders: 0, prints: 0, rejects: 0, errors: 0, notes: 0, undos: 0, undoParts: 0, undoOrders: 0, activeMs: 0, idleMs: 0 }, o);
const O = n => '35210000' + String(n).padStart(2, '0');                // order ids
const sess = (id, person, station, device, start, o = {}) => ({ id, person, station, device, computerId: 'pc-' + id.padEnd(8, 'X'), computerLabel: '', startAt: start, lastSeenAt: o.last == null ? start : o.last, endAt: o.end == null ? null : o.end, endReason: o.reason || null, minutes: 0 });
const ev = (id, person, station, device, action, at, o = {}) => ({ id, station, device, computer: 'pc-AAAA', session: '', person, action, orderId: o.orderId || '', line: '', sku: '', parts: o.parts || 0, orders: o.orders || 0, detail: o.detail || '', at, seq: 1, sincePrevMs: o.since || 0,
  ts: Ts.fromMillis(at), serverAt: at, day: o.day || DAY, hour: '10', v: 1 });
function seed(st) {
  // rollups (what the activity door keeps): Ana worked the Sorter app (sorter), the QR Printer (qr) and a sorting page (sorting) the same day
  st.put('Efficiency_Daily', `${DAY}__Ana M.`, { day: DAY, person: 'Ana M.', v: 1, events: 14, firstAt: Z('2026-10-05T14:05:00Z'), lastAt: Z('2026-10-05T14:55:00Z'),
    stations: {
      sorter: stat({ scans: 4, scanParts: 8, completes: 3, parts: 6, orders: 3, prints: 3, activeMs: 600000, firstAt: Z('2026-10-05T14:05:00Z'), lastAt: Z('2026-10-05T14:30:00Z') }),
      qr: stat({ prints: 5, activeMs: 120000, firstAt: Z('2026-10-05T14:31:00Z'), lastAt: Z('2026-10-05T14:40:00Z') }),
      sorting: stat({ scans: 6, scanParts: 12, completes: 2, parts: 4, orders: 2, prints: 2, activeMs: 300000, firstAt: Z('2026-10-05T14:41:00Z'), lastAt: Z('2026-10-05T14:55:00Z') }) },
    hours: { 10: { parts: 10, undoParts: 0, scans: 10, by: { sorter: { parts: 6, undoParts: 0, scans: 4 }, sorting: { parts: 4, undoParts: 0, scans: 6 } } } },
    touched: { [O(1)]: { sorter: true, sorting: true }, [O(2)]: { qr: true }, [O(3)]: { sorting: true } } });
  st.put('Efficiency_Daily', `${DAY}__Ben R.`, { day: DAY, person: 'Ben R.', v: 1, events: 9, firstAt: Z('2026-10-05T13:05:00Z'), lastAt: Z('2026-10-05T13:50:00Z'),
    stations: { sorting: stat({ scans: 3, scanParts: 6, completes: 5, parts: 10, orders: 5, prints: 5, activeMs: 900000, firstAt: Z('2026-10-05T13:05:00Z'), lastAt: Z('2026-10-05T13:50:00Z') }) },
    hours: { 9: { parts: 10, undoParts: 0, scans: 3, by: { sorting: { parts: 10, undoParts: 0, scans: 3 } } } },
    touched: { [O(11)]: { sorting: true }, [O(12)]: { sorting: true }, [O(13)]: { sorting: true }, [O(14)]: { sorting: true }, [O(15)]: { sorting: true } } });
  st.put('Efficiency_Daily', `${DAY}__Cy L.`, { day: DAY, person: 'Cy L.', v: 1, events: 3, firstAt: Z('2026-10-05T14:20:00Z'), lastAt: Z('2026-10-05T14:45:00Z'),      // a Laser person working in the Sorter app: stored as laser
    stations: { laser: stat({ completes: 2, parts: 7, activeMs: 200000, firstAt: Z('2026-10-05T14:20:00Z'), lastAt: Z('2026-10-05T14:45:00Z') }) },
    hours: { 10: { parts: 7, undoParts: 0, scans: 0, by: { laser: { parts: 7, undoParts: 0, scans: 0 } } } }, touched: {} });
  st.put('Efficiency_Daily', `${YDAY}__Dee Q.`, { day: YDAY, person: 'Dee Q.', v: 1, events: 2, firstAt: Z('2026-10-04T13:05:00Z'), lastAt: Z('2026-10-04T13:40:00Z'),
    stations: { sorter: stat({ completes: 1, parts: 2, orders: 1, activeMs: 60000 }), qr: stat({ prints: 1, activeMs: 30000 }) }, hours: {}, touched: { [O(21)]: { sorter: true, qr: true } } });
  // sessions: Ana is signed in at the Sorter app AND a sorting page at once (they overlap from 10:30); Cy is the Laser person at the Sorter app
  st.put('Station_Sessions', 's1', sess('s1', 'Ana M.', 'sorter', 'charm-nest-1', Z('2026-10-05T14:00:00Z'), { last: NOW - 30000 }));
  st.put('Station_Sessions', 's2', sess('s2', 'Ana M.', 'sorting', 'sorting-1', Z('2026-10-05T14:30:00Z'), { last: NOW - 20000 }));
  st.put('Station_Sessions', 's3', sess('s3', 'Ben R.', 'sorting', 'sorting-2', Z('2026-10-05T13:00:00Z'), { last: NOW - 60000 }));
  st.put('Station_Sessions', 's4', sess('s4', 'Cy L.', 'laser', 'charm-nest-1', Z('2026-10-05T14:15:00Z'), { last: NOW - 15000 }));
  st.put('Station_Sessions', 's5', sess('s5', 'Dee Q.', 'qr', 'qr-printer', Z('2026-10-04T13:00:00Z'), { last: Z('2026-10-04T14:00:00Z'), end: Z('2026-10-04T14:00:00Z'), reason: 'signOut' }));
  // single events (what the feed and the order page read)
  st.put('Station_Activity', 'e1xxxxxx', ev('e1xxxxxx', 'Ana M.', 'sorter', 'charm-nest-1', 'complete', Z('2026-10-05T14:20:00Z'), { orderId: O(1), parts: 3, orders: 1, detail: 'Complete Order', since: 60000 }));
  st.put('Station_Activity', 'e2xxxxxx', ev('e2xxxxxx', 'Ana M.', 'sorting', 'sorting-1', 'print', Z('2026-10-05T14:42:00Z'), { orderId: O(1), detail: 'sticker', since: 30000 }));
  st.put('Station_Activity', 'e3xxxxxx', ev('e3xxxxxx', 'Ana M.', 'qr', 'qr-printer', 'print', Z('2026-10-05T14:35:00Z'), { orderId: O(2), detail: 'label', since: 20000 }));
  st.put('Station_Activity', 'e4xxxxxx', ev('e4xxxxxx', 'Ben R.', 'sorting', 'sorting-2', 'complete', Z('2026-10-05T13:40:00Z'), { orderId: O(11), parts: 2, orders: 1, detail: 'sticker', since: 40000 }));
  // a seal of an order that only the Sorter app ever touched (an old order, before activity events), by a person who has a rollup
  st.put('Order_Timeline', 'sealxxx1', { orderId: O(99), type: 'labelPrinted', at: Z('2026-10-05T14:10:00Z'), by: 'Dee Q.', source: 'station', station: 'sorter', device: 'charm-nest-1', milestone: false, text: 'x' });
  // live documents: Ana's current order at the Sorter app, Cy's sheet at the laser (same page, the Laser role)
  st.put('Station_Live', 'sorter__charm-nest-1__Ana M.', { id: 'sorter__charm-nest-1__Ana M.', v: 1, station: 'sorter', device: 'charm-nest-1', computer: '', session: '', person: 'Ana M.', sinceAt: Z('2026-10-05T14:00:00Z'), beatAt: NOW - 5000, eventAt: NOW - 5000, state: 'working',
    kind: 'order', rid: O(1), orderNumber: O(1), customer: 'Sam P.', scannedAt: NOW - 120000, pieces: [], pieceCount: 2, note: '', title: '' });
  st.put('Station_Live', 'laser__charm-nest-1__Cy L.', { id: 'laser__charm-nest-1__Cy L.', v: 1, station: 'laser', device: 'charm-nest-1', computer: '', session: '', person: 'Cy L.', sinceAt: Z('2026-10-05T14:15:00Z'), beatAt: NOW - 5000, eventAt: NOW - 5000, state: 'working',
    kind: 'sheet', rid: '', orderNumber: '', customer: '', scannedAt: NOW - 300000, pieces: [], pieceCount: 0, note: '', title: 'GF Sheet 1' });
}

/** every string anywhere in an answer, with its key: the walk behind "nowhere" */
function walk(x, fn, k) { if (Array.isArray(x)) x.forEach(v => walk(v, fn, k)); else if (x && typeof x === 'object') for (const [kk, v] of Object.entries(x)) { fn(kk, v, 'key'); walk(v, fn, kk); } else fn(k, x, 'val'); }
function noSorterOrQr(answer, what) {
  walk(answer, (k, v, t) => {
    if (t === 'key') assert(k !== 'sorter', `${what}: a map keyed "${k}"`);
    if (t === 'key' && k === 'qr') { /* ("qr" is also the order's own QR code: { text } or a string: never a station's counters) */ }
    else if (typeof v === 'string' && /^(station|stationKey|key)$/.test(k || '')) assert(!['sorter', 'qr'].includes(v), `${what}: ${k} = "${v}"`);
    if (t === 'val' && typeof v === 'string' && k === 'stationLabel') assert(!/^(sorter|qr printer|qr labels?|qr)$/i.test(v), `${what}: label "${v}"`);
  });
  walk(answer, (k, v, t) => { if (t === 'key' && k === 'qr') return; });
  (function station(x) { if (Array.isArray(x)) x.forEach(station); else if (x && typeof x === 'object') { if (typeof x.key === 'string' && typeof x.label === 'string') assert(!/^(sorter|qr printer|qr labels?|qr)$/i.test(x.label), `${what}: a station labelled "${x.label}"`); if (typeof x.station === 'string' && typeof x.label === 'string') assert(!/^(sorter|qr printer|qr labels?|qr)$/i.test(x.label), `${what}: a station row labelled "${x.label}"`); } if (x && typeof x === 'object' && !Array.isArray(x)) for (const [kk, v] of Object.entries(x)) { if (kk === 'qr' && v && typeof v === 'object' && !('text' in v)) assert.fail(`${what}: a station map keyed "qr"`); station(v); } })(answer);
  // (a station list is of display stations: no entry of a `stations` array of strings is sorter or qr either)
  walk(answer, (k, v, t) => { if (t === 'val' && k === 'stations' && Array.isArray(v)) v.forEach(s => assert(!['sorter', 'qr'].includes(s), `${what}: stations contains ${s}`)); });
  walk(answer, (k, v, t) => { if (t === 'key' && k === 'stations' && Array.isArray(v)) v.forEach(s => typeof s === 'string' && assert(!['sorter', 'qr'].includes(s), `${what}: stations contains ${s}`)); });
}

/* ── 1 · the one rule ── */
const KEYS = ['sorter', 'qr', 'sorting', 'laser', 'design', 'welding', 'assembly', 'shipping', 'inbox', '', null, undefined, 'constructor', 'toString', '__proto__', 'Sorter', 'QR', 'qr ', 7];
function rule() {
  assert.deepStrictEqual(KEYS.map(K.displayStation), ['sorting', 'sorting', 'sorting', 'laser', 'design', 'welding', 'assembly', 'shipping', 'inbox', '', '', '', 'constructor', 'toString', '__proto__', 'Sorter', 'QR', 'qr ', '7']);
  assert.deepStrictEqual(Object.keys(K.STATION_FOLD).sort(), ['qr', 'sorter'], 'only the keys sorter and qr fold: a Sorter-app session of a Laser or Design person is stored as laser or design and never folds');
  assert.deepStrictEqual(K.storedStations('sorting'), ['sorting', 'sorter', 'qr']); assert.deepStrictEqual(K.storedStations('qr'), ['sorting', 'sorter', 'qr']); assert.deepStrictEqual(K.storedStations('laser'), ['laser']);
  assert.throws(() => { K.STATION_FOLD.laser = 'sorting'; }, TypeError, 'the map is frozen');
  say('1 displayStation: sorter and qr are sorting, everything else is itself (laser and design too), odd keys are safe, the map is frozen');
}

/* ── 2 · the server reads ── */
async function reads() {
  const st = fakeStore(); EP.resetCache(); seed(st);
  const before = st.dump();
  const answers = {};

  // the Overview of today
  const ov = answers.ov = await ask(st, { op: 'overview', day: DAY, days: 1, trend: false });
  const bs = Object.fromEntries(ov.business.stations.map(s => [s.station, s]));
  assert.deepStrictEqual(ov.business.stations.map(s => s.station).filter(k => k === 'sorter' || k === 'qr'), [], 'the Overview lists no Sorter or QR Printer station');
  assert.strictEqual(bs.sorting.parts, (6 + 0 + 4) + 10, 'Sorting = Ana at the Sorter app + the QR Printer + a sorting page, and Ben: the sum of what was stored under all three keys');
  assert.strictEqual(bs.sorting.scans, (4 + 0 + 6) + 3);
  assert.strictEqual(bs.sorting.orders, 3 + 5, 'orders are counted by id: the order Ana touched at the Sorter app and at a sorting page is ONE order');
  assert.deepStrictEqual(bs.sorting.peopleNow, ['Ana M.', 'Ben R.'], 'Ana is at two pages of Sorting: one name, once');
  assert.strictEqual(bs.laser.parts, 7); assert.deepStrictEqual(bs.laser.peopleNow, ['Cy L.'], 'a Laser person at the Sorter app is at Laser');
  assert.strictEqual(ov.business.totals.parts, bs.sorting.parts + bs.laser.parts, 'the business total is the sum of the stations');
  assert.strictEqual(ov.business.totals.parts, ov.people.reduce((n, p) => n + p.totals.parts, 0), 'and of the people');
  assert.strictEqual(ov.business.totals.scans, 13);
  assert.deepStrictEqual(Object.keys(ov.business.perHour).sort(), ['laser', 'sorting'], 'the by-hour lines have no Sorter or QR Printer series');
  assert.strictEqual(ov.business.perHour.sorting[10], 10); assert.strictEqual(ov.business.perHour.sorting[9], 10);
  const ana = ov.people.find(p => p.name === 'Ana M.');
  assert.deepStrictEqual(ana.stations.map(s => s.station), ['sorting'], 'Ana has one Sorting row');
  assert.strictEqual(ana.stations[0].parts, 10); assert.strictEqual(ana.stations[0].scans, 10); assert.strictEqual(ana.stations[0].prints, 10); assert.strictEqual(ana.stations[0].completes, 5);
  assert.strictEqual(ana.stations[0].orders, 3, 'three orders at Sorting (the Sorter app finished 3, the sorting page 2: the same orders are not added)');
  assert.strictEqual(ana.stations[0].minutes, 60, 'time at Sorting is the union of the Sorter app (10:00 to now) and the sorting page (10:30 to now): 60 minutes, not 90');
  assert.deepStrictEqual(ana.nowAt, ['sorting']); assert.strictEqual(ana.status, 'on');
  assert.deepStrictEqual(ana.orders.map(o => [o.orderId, o.stations.join()]).sort(), [[O(1), 'sorting'], [O(2), 'sorting']], 'the orders of her events name Sorting once each (order 1 was at the Sorter app and at a sorting page)');
  assert.deepStrictEqual(ov.people.find(p => p.name === 'Cy L.').stations.map(s => s.station), ['laser']);
  assert.strictEqual(ov.business.totals.orders, 8, 'distinct orders worked today (the shop total)');
  assert.deepStrictEqual([...new Set(ov.feed.map(f => f.station))], ['sorting'], 'the feed says Sorting for events stored as sorter, qr and sorting');
  assert.strictEqual(ov.feed.length, 4);
  noSorterOrQr(ov, 'overview');

  // the same day and the day before, over 7 days (yesterday's Dee: stored as sorter and qr only)
  const wk = answers.wk = await ask(st, { op: 'overview', day: DAY, days: 7, trend: true });
  const dee = wk.people.find(p => p.name === 'Dee Q.');
  assert.deepStrictEqual(dee.stations.map(s => s.station), ['sorting'], 'old history under sorter and qr shows as Sorting alone');
  assert.strictEqual(dee.stations[0].parts, 2); assert.strictEqual(dee.stations[0].prints, 1);
  assert.strictEqual(dee.stations[0].orders, 1, 'one order, touched at both old stations');
  noSorterOrQr(wk, 'overview, 7 days');

  // one person, both forms
  const p1 = answers.p1 = await ask(st, { op: 'person', name: 'Ana M.', day: DAY, days: 3 });
  const today = p1.days.find(d => d.day === DAY);
  assert.deepStrictEqual(today.stations.map(s => s.station), ['sorting']); assert.strictEqual(today.stations[0].parts, 10);
  assert.strictEqual(p1.totals.parts, 10);
  noSorterOrQr(p1, 'person');
  const p2 = answers.p2 = await ask(st, { op: 'person', name: 'Ana M.', range: 'week', day: DAY });
  assert.deepStrictEqual(p2.stations.map(s => s.station), ['sorting'], 'the employee page has one Sorting row');
  assert.strictEqual(p2.stations[0].label, 'Sorting'); assert.strictEqual(p2.stations[0].parts, 10);
  assert.strictEqual(p2.stations[0].orders, 3);
  assert.strictEqual(Math.round(p2.stations[0].shareParts), 100, 'the share of her pieces: all of them Sorting');
  noSorterOrQr(p2, 'employee page');
  const p3 = answers.p3 = await ask(st, { op: 'person', name: 'Dee Q.', range: 'week', day: DAY });
  assert.deepStrictEqual(p3.stations.map(s => s.station), ['sorting']); assert.strictEqual(p3.stations[0].parts, 2);
  noSorterOrQr(p3, 'employee page of an old sorter-only person');

  // the order list of a person, filters and search by the old words
  const ol = answers.ol = await ask(st, { op: 'personOrders', name: 'Ana M.', from: YDAY, to: DAY, limit: 50 });
  assert.deepStrictEqual(ol.orders.map(r => r.rid).sort(), [O(1), O(2), O(3)]);
  for (const r of ol.orders) assert.deepStrictEqual(r.stations, ['sorting'], 'each order was at Sorting');
  noSorterOrQr(ol, 'order list');
  const olf = await ask(st, { op: 'personOrders', name: 'Ana M.', from: YDAY, to: DAY, limit: 50, station: 'sorter' });
  assert.deepStrictEqual(olf.orders.map(r => r.rid).sort(), [O(1), O(2), O(3)], 'an old filter "sorter" is the Sorting filter');
  const olq = await ask(st, { op: 'personOrders', name: 'Ana M.', from: YDAY, to: DAY, limit: 50, q: 'sorter' });
  assert.deepStrictEqual(olq.orders.map(r => r.rid).sort(), [O(1), O(2), O(3)], 'the word "sorter" still finds the orders of Sorting');
  assert.strictEqual((await ask(st, { op: 'personOrders', name: 'Ana M.', from: YDAY, to: DAY, limit: 50, station: 'qr' })).orders.length, 3, 'so does an old "qr" filter');

  // one order: the Sorter app's step and the sorting page's step are one Sorting step; an order only the old seals know
  const o1 = answers.o1 = await ask(st, { op: 'orders', orderId: O(1) });
  assert.strictEqual(o1.steps.length, 1); assert.strictEqual(o1.steps[0].station, 'sorting'); assert.strictEqual(o1.steps[0].person, 'Ana M.');
  assert.strictEqual(o1.steps[0].completes, 1); assert.strictEqual(o1.steps[0].prints, 1); assert.deepStrictEqual([...new Set(o1.events.map(e => e.station))], ['sorting']);
  assert.strictEqual(o1.totals.stations, 1);
  noSorterOrQr(o1, 'one order');
  const o99 = answers.o99 = await ask(st, { op: 'orders', orderId: O(99) });
  assert.deepStrictEqual(o99.steps.map(s => s.station), ['sorting'], 'a seal stored at the sorter station is a Sorting step');
  noSorterOrQr(o99, 'one order, seal only');

  // the live board
  const lv = answers.lv = await ask(st, { op: 'live' });
  assert.deepStrictEqual(lv.stations.map(s => s.key), ['sorting', 'welding', 'assembly', 'shipping', 'design', 'laser', 'inbox'], 'the board: no Sorter card, no QR Printer card; Inbox stays');
  const sorting = lv.stations.find(s => s.key === 'sorting'), laser = lv.stations.find(s => s.key === 'laser');
  assert.strictEqual(sorting.label, 'Sorting'); assert.strictEqual(sorting.state, 'working');
  assert.deepStrictEqual(sorting.people.slice().sort(), ['Ana M.', 'Ben R.'], 'each person once');
  assert.deepStrictEqual(sorting.devices.map(d => [d.device, d.label, d.state, d.person]), [['sorting-1', 'Sorting 1', 'idle', 'Ana M.'], ['sorting-2', 'Sorting 2', 'idle', 'Ben R.'], ['charm-nest-1', 'Sorter (nesting)', 'working', 'Ana M.'], ['qr-printer', 'QR Printer', 'offline', '']],
    'Sorting\'s pages: two sorting computers, the Sorter app (nesting) and the QR Printer page');
  assert.strictEqual(sorting.current.length, 1); assert.strictEqual(sorting.current[0].person, 'Ana M.'); assert.strictEqual(sorting.current[0].rid, O(1));
  assert.strictEqual(sorting.current[0].device, 'charm-nest-1'); assert.strictEqual(sorting.current[0].deviceLabel, 'Sorter (nesting)'); assert.strictEqual(sorting.current[0].id, 'sorting__charm-nest-1__Ana M.');
  assert.deepStrictEqual(sorting.counts, { partsToday: 20, ordersToday: 8, scansToday: 13 }, 'today at Sorting: the sum of the Sorter app, the QR Printer and the sorting pages; an order is counted once');
  assert.deepStrictEqual(lv.signedIn.map(p => [p.name, p.stationKey]), [['Ben R.', 'sorting'], ['Ana M.', 'sorting'], ['Cy L.', 'laser']], 'Signed in now: Ana at the Sorter app and at a sorting page is ONE row');
  assert.strictEqual(lv.signedIn.length, 3, 'the count of people on');
  assert.strictEqual(laser.state, 'working'); assert.deepStrictEqual(laser.people, ['Cy L.']); assert.strictEqual(laser.current.length, 1); assert.strictEqual(laser.current[0].title, 'GF Sheet 1');
  assert.deepStrictEqual(laser.devices.map(d => [d.device, d.label, d.state, d.person]), [['charm-nest-1', 'Sorter app', 'working', 'Cy L.']], 'a Laser person at the Sorter app is at Laser (page: Sorter app), not at Sorting');
  assert(!sorting.people.includes('Cy L.'), 'and not a person of Sorting');
  noSorterOrQr(lv, 'the live board');
  say('2 server reads: Overview (day, week), person (both forms), order list (+ old filter and word), one order, live board: Sorting only; totals = sum; one person once; time and orders counted once');

  /* 4 · history untouched */
  assert.strictEqual(st.writes.length, 0, 'no write of any kind');
  assert(st.dump() === before, 'every stored document is exactly as it was');
  assert.deepStrictEqual(Object.keys(JSON.parse(JSON.stringify(st.colls.get('Efficiency_Daily').get(`${DAY}__Ana M.`).stations))).sort(), ['qr', 'sorter', 'sorting'], 'Ana\'s stored rollup still has its sorter and qr counters');
  assert.strictEqual(st.colls.get('Station_Sessions').get('s1').station, 'sorter'); assert.strictEqual(st.colls.get('Station_Sessions').get('s5').station, 'qr'); assert.strictEqual(st.colls.get('Station_Activity').get('e3xxxxxx').station, 'qr'); assert.strictEqual(st.colls.get('Order_Timeline').get('sealxxx1').station, 'sorter');
  say('4 history untouched: no writes; sessions, events, rollups and the seal still carry sorter and qr');

  /* 3 · Laser and Design at the Sorter app do not fold */
  assert.strictEqual(bs.laser.parts, 7); assert.strictEqual(laser.devices[0].device, 'charm-nest-1');
  const dst = fakeStore(); EP.resetCache();
  dst.put('Station_Sessions', 'd1', sess('d1', 'Di D.', 'design', 'charm-nest-1', Z('2026-10-05T14:00:00Z'), { last: NOW - 10000 }));
  dst.put('Station_Sessions', 'd2', sess('d2', 'Di D.', 'sorter', 'charm-nest-1', Z('2026-10-05T14:00:00Z'), { end: Z('2026-10-05T14:20:00Z'), reason: 'signOut' }));   // (an old Sorter-app session of the same person: stored as sorter)
  dst.put('Efficiency_Daily', `${DAY}__Di D.`, { day: DAY, person: 'Di D.', v: 1, events: 4, firstAt: Z('2026-10-05T14:00:00Z'), lastAt: Z('2026-10-05T14:50:00Z'), stations: { design: stat({ completes: 2, parts: 3, orders: 2, activeMs: 120000 }), sorter: stat({ completes: 1, parts: 1, orders: 1, activeMs: 60000 }) }, hours: {}, touched: { [O(31)]: { design: true }, [O(32)]: { design: true }, [O(33)]: { sorter: true } } });
  const dlv = await ask(dst, { op: 'live' });
  const dd = dlv.stations.find(s => s.key === 'design'), ds = dlv.stations.find(s => s.key === 'sorting');
  assert.deepStrictEqual(dd.people, ['Di D.']); assert.deepStrictEqual(dd.devices.map(d => [d.device, d.label, d.state]).filter(x => x[0] === 'charm-nest-1'), [['charm-nest-1', 'Sorter app', 'idle']], 'a Design person at the Sorter app is at Design');
  assert.deepStrictEqual(ds.people, [], 'nobody at Sorting: a session stored as design is never Sorting\'s'); assert.deepStrictEqual(dlv.signedIn.map(p => [p.name, p.stationKey]), [['Di D.', 'design']]);
  assert.deepStrictEqual(ds.counts, { partsToday: 1, ordersToday: 1, scansToday: 0 }, 'and only what was stored under sorter counts for Sorting');
  assert.deepStrictEqual(dd.counts, { partsToday: 3, ordersToday: 2, scansToday: 0 });
  const dov = await ask(dst, { op: 'overview', day: DAY, days: 1, trend: false });
  assert.deepStrictEqual(dov.people[0].stations.map(s => [s.station, s.parts]).sort(), [['design', 3], ['sorting', 1]], 'Design and Sorting stay two stations for one person');
  say('3 Laser and Design at the Sorter app: stored as laser / design, shown on their own card (page "Sorter app"), never folded into Sorting');
  return answers;
}

/* ── 5 · the client models ── */
async function client() {
  let JSDOM; for (const d of [process.env.JSDOM_DIR, path.join(root, 'node_modules')].filter(Boolean)) { try { ({ JSDOM } = require(path.join(d, 'jsdom'))); break; } catch (_) {} }
  if (!JSDOM) { try { ({ JSDOM } = require('jsdom')); } catch (_) { say('5 – no jsdom: the client model checks were not run'); return; } }
  const dom = new JSDOM('<!doctype html><html><body></body></html>', { runScripts: 'outside-only', url: 'http://127.0.0.1/' });
  const w = dom.window;
  w.eval(fs.readFileSync(path.join(root, 'charm-nest-efficiency-stations.js'), 'utf8'));
  w.eval(fs.readFileSync(path.join(root, 'charm-nest-efficiency.js'), 'utf8'));
  const ES = w.EfficiencyStations, E = w.Efficiency;
  assert(ES && typeof ES.displayStation === 'function' && E && typeof E.displayStation === 'function');
  for (const k of KEYS) { assert.strictEqual(ES.displayStation(k), K.displayStation(k), 'the stations module agrees with the server for ' + String(k)); assert.strictEqual(E.displayStation(k), K.displayStation(k), 'the console agrees with the server for ' + String(k)); }
  assert.strictEqual(E.api.fmt.stName('sorter'), 'Sorting'); assert.strictEqual(E.api.fmt.stName('qr'), 'Sorting'); assert.strictEqual(E.api.fmt.stName('laser'), 'Laser');

  const old = oldLive(NOW);
  const J = x => JSON.parse(JSON.stringify(x));                      // (the models are built inside the jsdom window: plain data of another realm)
  const M = J(ES.norm(old));
  assert.deepStrictEqual(M.stations.map(s => s.key), ['sorting', 'laser', 'inbox'], 'the board model: one Sorting card, no Sorter, no QR Printer');
  const so = M.stations[0];
  assert.strictEqual(so.label, 'Sorting'); assert.deepStrictEqual(so.people.map(p => p.name).sort(), ['Ana M.', 'Ben R.', 'Dana S.'], 'every person of the three, each once');
  assert.deepStrictEqual(so.current.map(c => [c.person, c.station]).sort(), [['Ana M.', 'sorting'], ['Ben R.', 'sorting']]);
  assert.deepStrictEqual(so.counts, { parts: 31, orders: 13, scans: 7 }, 'counts add up');
  assert.strictEqual(so.state, 'working'); assert.strictEqual(so.lastEventAt, NOW - 1000);
  assert.deepStrictEqual(so.devices.map(d => d.device).sort(), ['charm-nest-1', 'qr-printer', 'sorting-1']);
  assert.deepStrictEqual(M.signedIn.map(p => [p.name, p.stationKey]), [['Ana M.', 'sorting'], ['Dana S.', 'sorting'], ['Cy L.', 'laser']], 'Signed in: Ana (at the Sorter app and at a sorting page) once');
  const L = J(E.normLive(old));
  assert.deepStrictEqual(L.stations.map(s => s.key), ['sorting', 'laser', 'inbox'], 'the console\'s live model');
  assert.deepStrictEqual(L.signedIn.map(p => p.name), ['Ana M.', 'Dana S.', 'Cy L.']); assert.deepStrictEqual(L.current.map(c => c.station).sort(), ['sorting', 'sorting']);
  assert.deepStrictEqual(L.stations[0].counts, { partsToday: 31, ordersToday: 13 });
  const O1 = E.norm({ day: DAY, days: 1, people: [{ name: 'Ana M.', status: 'on', nowAt: ['sorter', 'sorting', 'qr'], stations: [{ station: 'sorter', minutes: 20, parts: 6, scans: 4, orders: 3 }, { station: 'sorting', minutes: 30, parts: 4, scans: 6, orders: 2 }, { station: 'qr', minutes: 5, prints: 5 }],
    totals: { parts: 10, scans: 10 }, orders: [{ orderId: O(1), stations: ['sorter', 'sorting'], parts: 3 }], perHour: new Array(24).fill(0) }],
    business: { totals: { parts: 10, scans: 10, orders: 3 }, stations: [{ station: 'sorter', parts: 6, scans: 4, orders: 3, peopleNow: ['Ana M.'] }, { station: 'sorting', parts: 4, scans: 6, orders: 2, peopleNow: ['Ana M.'] }, { station: 'qr', parts: 0, scans: 0, orders: 0, peopleNow: [] }], perHour: { sorter: new Array(24).fill(1), sorting: new Array(24).fill(2) } },
    feed: [{ id: 'f1', at: NOW, person: 'Ana M.', station: 'qr', action: 'print' }] });
  const pa = J(O1.people[0]);
  assert.deepStrictEqual(pa.nowAt, ['sorting']); assert.deepStrictEqual(pa.stations.map(s => s.station), ['sorting']); assert.strictEqual(pa.stations[0].minutes, 55); assert.strictEqual(pa.stations[0].parts, 10);
  assert.deepStrictEqual(pa.orders[0].stations, ['sorting']);
  assert.deepStrictEqual(J([...O1.biz.stations.keys()]), ['sorting'], 'the console\'s station rows: Sorting only'); assert.strictEqual(O1.biz.stations.get('sorting').parts, 10); assert.deepStrictEqual(J(O1.biz.stations.get('sorting').now), ['Ana M.']);
  assert.strictEqual(O1.biz.stations.get('sorting').hours[5], 3, 'the by-hour series of the two keys are added'); assert.strictEqual(O1.feed[0].station, 'sorting');
  const OR = E.normOrder({ orderId: O(1), steps: [{ station: 'sorter', person: 'Ana M.' }, { station: 'qr', person: 'Ana M.' }] });
  assert.deepStrictEqual(J(OR.steps.map(s => s.station)), ['sorting', 'sorting']);
  w.close();
  say('5 client models: the displayStation of the server, the stations module and the console agree; the board model, the console\'s live model and Overview model fold an older answer (one Sorting card, one person once)');
}

/** an answer the way the live board sounded before this change: a Sorter card, a QR Printer card, people at two pages of one station */
function oldLive(now) {
  const mk = (rid, person, device) => ({ id: `x__${device}__${person}`, person, device, deviceLabel: device, kind: 'order', rid, orderNumber: rid, customer: 'Sam P.', title: '', scannedAt: now - 120000, beatAt: now - 5000, qr: { text: rid }, pieces: [], pieceCount: 1, note: '' });
  return { ok: true, at: now, mode: 'real', stations: [
    { key: 'sorting', label: 'Sorting', state: 'idle', people: ['Dana S.', 'Ana M.'], current: [], devices: [{ device: 'sorting-1', label: 'Sorting 1', state: 'idle', person: 'Dana S.', since: now - 3600e3 }], lastEventAt: now - 600000, counts: { partsToday: 10, ordersToday: 4, scansToday: 3 } },
    { key: 'laser', label: 'Laser', state: 'idle', people: ['Cy L.'], current: [], devices: [], lastEventAt: null, counts: { partsToday: 0, ordersToday: 0, scansToday: 0 } },
    { key: 'sorter', label: 'Sorter', state: 'working', people: ['Ana M.', 'Ben R.'], current: [mk('3521000001', 'Ana M.', 'charm-nest-1'), mk('3521000002', 'Ben R.', 'charm-nest-1')], devices: [{ device: 'charm-nest-1', label: 'Sorter', state: 'working', person: 'Ana M.', since: now - 3600e3 }], lastEventAt: now - 1000, counts: { partsToday: 20, ordersToday: 8, scansToday: 4 } },
    { key: 'qr', label: 'QR Printer', state: 'offline', people: [], current: [], devices: [{ device: 'qr-printer', label: 'QR Printer', state: 'offline', person: '', since: 0 }], lastEventAt: null, counts: { partsToday: 1, ordersToday: 1, scansToday: 0 } },
    { key: 'inbox', label: 'Inbox', state: 'offline', people: [], current: [], devices: [], lastEventAt: null, counts: { partsToday: 0, ordersToday: 0, scansToday: 0 } }],
    signedIn: [{ name: 'Ana M.', stationKey: 'sorter', device: 'charm-nest-1', since: now - 3600e3, lastSeenAt: now - 30000 }, { name: 'Ana M.', stationKey: 'sorting', device: 'sorting-1', since: now - 1800e3, lastSeenAt: now - 20000 },
      { name: 'Dana S.', stationKey: 'sorting', since: now - 3000e3, lastSeenAt: now - 10000 }, { name: 'Cy L.', stationKey: 'laser', since: now - 2000e3, lastSeenAt: now - 10000 }] };
}

/* ── 6 · a real browser ── */
async function browser() {
  const pwDir = process.env.PW_DIR || (fs.existsSync(path.join(root, 'node_modules/playwright-core')) ? path.join(root, 'node_modules') : '/opt/node22/lib/node_modules/playwright/node_modules');
  let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { say('6 – no playwright-core: the browser check was not run'); return; }
  const exe = process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome';
  if (!fs.existsSync(exe)) { say('6 – no Chromium: the browser check was not run'); return; }
  const srv = http.createServer((req, res) => {
    const u = new URL(req.url, 'http://x');
    if (u.pathname === '/') { res.setHeader('content-type', 'text/html'); return res.end('<!doctype html><meta charset="utf-8"><body style="margin:0;background:#f4f1ec"><div id="host" style="width:1100px"></div>'); }
    if (u.pathname === '/stations.js') { res.setHeader('content-type', 'application/javascript'); return res.end(fs.readFileSync(path.join(root, 'charm-nest-efficiency-stations.js'))); }
    res.statusCode = 404; res.end();
  });
  await new Promise(r => srv.listen(0, '127.0.0.1', r));
  const base = `http://127.0.0.1:${srv.address().port}`;
  const browser = await chromium.launch({ executablePath: exe, args: ['--no-sandbox'] });
  try {
    const page = await browser.newPage({ viewport: { width: 1200, height: 900 } }), errs = [];
    page.on('pageerror', e => errs.push(e.message));
    await page.route(() => true, r => (new URL(r.request().url()).hostname === '127.0.0.1' ? r.continue() : r.abort()));
    await page.goto(base + '/'); await page.addScriptTag({ url: base + '/stations.js' });
    // the board is fed an OLD-style answer (a Sorter card and a QR Printer card) by the module's own source hook: no network, no passcode
    await page.evaluate(ans => { window.EfficiencyStations.options.source = async () => ans; window.EfficiencyStations.mount(document.getElementById('host'), { own: true, onPerson: false }); }, oldLive(Date.now()));
    await page.waitForSelector('.esSt', { timeout: 15000 });
    const out = await page.evaluate(() => ({ keys: [...document.querySelectorAll('.esSt')].map(e => e.dataset.key), names: [...document.querySelectorAll('.esStName')].map(e => e.textContent.trim()),
      people: [...document.querySelectorAll('.esSt[data-key="sorting"] .esPeople *')].map(e => e.textContent.trim()).filter(Boolean), cards: document.querySelectorAll('.esSt[data-key="sorting"] .esCard').length, sum: document.querySelector('.esSum').textContent, body: document.body.innerText }));
    assert.deepStrictEqual(out.keys, ['sorting', 'laser', 'inbox'], 'the board draws one Sorting card');
    assert.deepStrictEqual(out.names, ['Sorting', 'Laser', 'Inbox']); assert(!/\bSorter\b|QR Printer/.test(out.names.join(' ')), 'no Sorter card and no QR Printer card');
    assert.strictEqual(out.cards, 2, 'the two orders in hand at the Sorter app are on the Sorting card');
    assert(/3 people on/.test(out.sum) || /3 people/.test(out.sum), 'the summary counts Ana once: ' + out.sum);
    assert.deepStrictEqual(errs, [], 'no page error');
    say('6 browser: an answer that still has Sorter and QR Printer rows draws one Sorting card (' + out.names.join(', ') + '), "' + out.sum + '"');
  } finally { await browser.close(); srv.close(); }
}

(async () => {
  rule();
  await reads();
  await client();
  await browser();
  assert(!logs.some(l => l.includes(PASS)), 'the passcode is in no log line');
  say('sorting-fold: all checks passed');
})().catch(e => { process.stdout.write('FAILED: ' + (e && e.stack || e) + '\n'); process.exit(1); });
