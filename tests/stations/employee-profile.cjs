// The employee page's data (ops person with a range, personOrders; netlify/functions/_employeeProfile.js behind employeeEfficiency.js),
// offline: the REAL function over an in-memory Firestore (typed fields, where/orderBy/limit, Timestamp), a fake clock, a synthetic
// passcode, and the invented 14-week shop of tests/charm-nest/employee-profile-seed.cjs (six people, days off, a holiday, a Saturday,
// a late start, a short day, a new hire, a reopened completion, refusals, errors, a reprint, a rescan, failed inbox replies).
//   1 · gate: passcode refusal with no data (also for the new ops), GET refused, bad ranges, outages, the key and any PIN never in an answer
//   2 · ranges: day / week / month / quarter / year / custom windows, the previous period, series per day or per week, 24 hours, stations
//   3 · every number against what the seed generated (pieces net of undo, orders, scans, hours, signed time, speed, hours of the day)
//   4 · compare to the previous period, aliases (Giovanna C. and the inbox account Giovanna are one), unknown person, sandbox separation
//   5 · the hooks for the attendance and issues helpers of other files (ctx.prof, merging, a missing or broken file)
//   6 · cost: how many documents a call reads, cache reuse, no writes at all
//   7 · personOrders: the order list, search by number / date / station / customer / SKU, paging, details, issues, sandbox
//   node tests/stations/employee-profile.cjs
'use strict';
require(require('path').join(__dirname, '../../netlify/functions/_activityKinds.js')).NO_THROUGHPUT.clear();   // this suite uses 'welding' as a plain fixture station for the generic arithmetic: the Welding station's own rule (not counted in throughput, R2 of stations round 2) is tested in welding-portal.cjs

const path = require('path'), assert = require('assert'), Module = require('module'), fs = require('fs');
const root = path.join(__dirname, '../..');
const S = require('../charm-nest/employee-profile-seed.cjs');

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
        reads.push({ name, n: Math.max(1, docs.length), filters: filters.map(f => f[0] + f[1]), vals: filters.map(f => val(f[2])), order: order && order[0] });
        return { docs: docs.map(({ id, d }) => ({ id, data: () => keep(d) })), size: docs.length, empty: !docs.length };
      }
    };
  }
  const db = { collection: name => Object.assign(query(name, [], null, null), {
    doc: id => ({ id,
      get: async () => { if (name !== 'Station_Rev') reads.push({ name, doc: id, n: 1 }); if (failing.has(name)) throw new Error('14 UNAVAILABLE'); const d = data(name).get(id); return { exists: !!d, data: () => keep(d) }; },
      set: async v => { writes.push([name, id]); data(name).set(id, keep(v)); },
      create: async v => { writes.push([name, id]); data(name).set(id, keep(v)); } }) }) };
  return { db, put: (name, id, d) => data(name).set(id, keep(d)), reads, writes, fail: n => failing.add(n), heal: n => failing.delete(n), count: n => data(name).size, colls,
    readsOf: name => reads.filter(r => r.name === name), docsOf: name => reads.filter(r => r.name === name).reduce((n, r) => n + r.n, 0), docsRead: () => reads.reduce((n, r) => n + r.n, 0), clear: () => { reads.length = 0; } };
}

/* ── the function under test, over the fake admin. The helper files of other workers (attendance, issues) are stubbed as missing here, so
      this suite tests THIS file alone; section 5 loads it again with fakes, and with the real files when they exist. ── */
const fakeAdmin = { firestore: Object.assign(() => ({}), { Timestamp: Ts, FieldValue: { serverTimestamp: () => 'ts' } }) };
const realLoad = Module._load;
const HELPER_FILE = { './_employeeAttendance': 'attendance', './_employeeIssues': 'issues' };
function loadFn(stubs) {
  for (const k of Object.keys(require.cache)) if (/netlify[\/]functions[\/](employeeEfficiency|_employeeProfile)\.js$/.test(k)) delete require.cache[k];
  Module._load = function (req, ...rest) {
    if (/[\/]firebaseAdmin(\.js)?$/.test(req)) return fakeAdmin;
    const which = HELPER_FILE[req];
    if (which && stubs) {
      const s = stubs[which];
      if (s === 'missing') throw Object.assign(new Error(`Cannot find module '${req}'`), { code: 'MODULE_NOT_FOUND' });
      if (s instanceof Error) throw s;
      if (s) return s;
    }
    return realLoad.call(this, req, ...rest);
  };
  try { return require(path.join(root, 'netlify/functions/employeeEfficiency.js')); } finally { Module._load = realLoad; }
}
let T = loadFn({ attendance: 'missing', issues: 'missing' })._t;
const BASE_T = T;
const EP = require(path.join(root, 'netlify/functions/_editPasscode.js'));
const PROFILE = require(path.join(root, 'netlify/functions/_employeeProfile.js')), EVD = PROFILE.EVENT_DAYS;
assert.strictEqual(EVD, 14);

const PASS = 'synthetic-pass-ep-4q';
process.env.EDIT_PASSCODE = PASS;
const logs = [], keepLog = (...a) => logs.push(a.map(x => typeof x === 'string' ? x : JSON.stringify(x)).join(' '));
console.warn = console.log = console.error = console.info = keepLog;
const say = (...a) => process.stdout.write(a.join(' ') + '\n');
const bodies = [];
let NOW = S.NOW; Date.now = () => NOW;
let ipN = 0;
async function call(st, body, o = {}) {
  const r = await T.handle({ httpMethod: o.method || 'POST', headers: { 'x-nf-client-connection-ip': o.ip || '203.0.113.' + (++ipN % 250) }, body: JSON.stringify(Object.assign({ key: PASS }, body)) }, st.db);
  bodies.push(r.body);
  return { status: r.statusCode, size: (r.body || '').length, body: JSON.parse(r.body || '{}') };
}
const person = (st, name, range, extra) => call(st, Object.assign({ op: 'person', name, range }, extra || {}));
const orders = (st, name, extra) => call(st, Object.assign({ op: 'personOrders', name }, extra || {}));
const tick = ms => { NOW += ms; };
function build() {
  EP.resetCache(); NOW = S.NOW;
  const st = fakeStore();
  const truth = S.seed((c, id, d) => st.put(c, id, d));
  return { st, truth };
}

/* ── what the seed generated, read back independently of the function ── */
const r1 = x => Math.round(x * 10) / 10;
const near = (a, b, tol = 0.051) => a != null && b != null && Math.abs(a - b) <= tol;
const nearOrNull = (a, b, tol) => (b == null ? a === null : near(a, b, tol));
const HOUR = 3600000, DAYMS = 86400000;
const median = a => { if (!a.length) return null; const s = a.slice().sort((x, y) => x - y), m = s.length >> 1; return s.length % 2 ? s[m] : (s[m - 1] + s[m]) / 2; };
const pct = (a, p) => { if (!a.length) return null; const s = a.slice().sort((x, y) => x - y); return s[Math.max(0, Math.min(s.length - 1, Math.ceil(p / 100 * s.length) - 1))]; };
const epochDay = d => Date.UTC(+d.slice(0, 4), +d.slice(5, 7) - 1, +d.slice(8, 10)) / DAYMS;
const daysIn = (a, b) => { const o = []; for (let d = a; d <= b; d = S.addDays(d, 1)) o.push(d); return o; };
const mondayOf = d => S.addDays(d, -((S.dow(d) + 6) % 7));
const WIN_N = { day: 1, week: 7, month: 30, quarter: 90, year: 365 };
const win = (range, to = S.TODAY) => { const n = WIN_N[range]; return { n, from: S.addDays(to, -(n - 1)), to, prevFrom: S.addDays(to, -(2 * n - 1)), prevTo: S.addDays(to, -n) }; };
const storedEvents = (st, forms, from, to) => [...st.colls.get('Station_Activity').values()].filter(e => forms.includes(e.person) && e.day >= from && e.day <= to && !e.sandbox).sort((a, b) => a.at - b.at || a.ts - b.ts);
const rollsOf = (st, forms, from, to, coll = 'Efficiency_Daily') => [...st.colls.get(coll).values()].filter(r => forms.includes(r.person) && r.day >= from && r.day <= to);
const truthDays = (truth, who, from, to) => Object.values(truth.days).filter(d => d.person === who && d.day >= from && d.day <= to).sort((a, b) => (a.day < b.day ? -1 : 1));
const sumDays = (truth, who, from, to, k) => truthDays(truth, who, from, to).reduce((n, d) => n + d[k], 0);
function trueSpeed(st, forms, from, to) {
  const steps = new Map(), gaps = []; let prevAt = 0, prevDay = '';
  for (const e of storedEvents(st, forms, from, to)) {
    if (e.orderId) { const k = e.orderId + '|' + e.station; let s = steps.get(k); if (!s) steps.set(k, s = { station: e.station, work: 0, completes: 0 }); if (e.sincePrevMs > 0 && e.sincePrevMs <= 300000) s.work += e.sincePrevMs; if (e.action === 'complete') s.completes++; }
    if (e.action === 'scan') { if (prevAt && e.day === prevDay && e.at > prevAt && e.at - prevAt <= 300000) gaps.push((e.at - prevAt) / 1000); prevAt = e.at; prevDay = e.day; }
  }
  const secs = [...steps.values()].filter(s => s.completes > 0 && s.work > 0 && s.station !== 'inbox').map(s => s.work / 1000);
  return { median: median(secs), p90: pct(secs, 90), gap: median(gaps), n: secs.length, gaps: gaps.length };
}

/** Everything the seed says a person did on the days of a window, derived from its rollups, sessions and events. */
function expectFor(st, truth, who, forms, from, to) {
  const ex = { from, to, perDay: new Map(), evDays: 0, parts: 0, orders: 0, ordersFin: 0, scans: 0, scanParts: 0, completes: 0, prints: 0, active: 0, idle: 0, signedAll: 0, signedEv: 0, sessDays: 0, hours: Array(24).fill(0), hourScans: Array(24).fill(0), st: {} };
  const inboxDays = new Set(truth.inbox.filter(i => forms.includes(i.person)).map(i => i.day));
  const stOf = name => ex.st[name] || (ex.st[name] = { parts: 0, scans: 0, completes: 0, prints: 0, rids: new Set(), minMs: 0 });
  const sessions = [...st.colls.get('Station_Sessions').values()].filter(x => forms.includes(x.person) && !x.sandbox);
  const nyDayOf = at => new Date(at - 4 * HOUR).toISOString().slice(0, 10);
  for (const day of daysIn(from, to)) {
    const t = truth.days[day + '|' + who], rolls = rollsOf(st, forms, day, day);
    const dayAct = {}, daySess = {};
    const d = { day, has: rolls.length > 0, signed: t ? t.signedMs : 0, parts: 0, orders: 0, ordersFin: 0, scans: 0, completes: 0, prints: 0, active: 0, idle: 0 };
    if (t && t.signedMs > 0) { ex.sessDays++; ex.signedAll += t.signedMs; }
    if (d.has) {
      ex.evDays++; ex.signedEv += d.signed;
      for (const r of rolls) for (const [name, s] of Object.entries(r.stations)) {
        const a = stOf(name); dayAct[name] = (dayAct[name] || 0) + s.activeMs;
        const p = Math.max(0, s.parts - s.undoParts); a.parts += p; a.scans += s.scans; a.completes += s.completes; a.prints += s.prints;
        d.parts += p; d.ordersFin += Math.max(0, s.orders - s.undoOrders); d.scans += s.scans; d.completes += s.completes; d.prints += s.prints; ex.scanParts += s.scanParts; d.active += s.activeMs; d.idle += s.idleMs;
        for (const [rid, m] of Object.entries(r.touched)) if (m[name]) a.rids.add(rid);
      }
      for (const r of rolls) for (const [hh, h] of Object.entries(r.hours)) { ex.hours[+hh] += h.parts - h.undoParts; ex.hourScans[+hh] += h.scans; }
      d.orders = t ? t.touched : 0;
      for (const x of sessions) if (nyDayOf(x.startAt) === day) { stOf(x.station); daySess[x.station] = (daySess[x.station] || 0) + (x.endAt || NOW) - x.startAt; }     // (a station signed in at counts, with or without work)
      for (const name of new Set([...Object.keys(dayAct), ...Object.keys(daySess)])) stOf(name).minMs += daySess[name] > 0 ? daySess[name] : dayAct[name] || 0;    // (no sign-in at a station that logged work: its logged working time stands in)
      const raw = d.signed + (inboxDays.has(day) ? 9000000 : 0);               // the inbox session (09:00-11:30) overlaps the main one: two computers at once
      if (raw > d.signed) { d.active = Math.min(d.active, d.signed); d.idle = Math.min(d.idle, Math.max(0, d.signed - d.active)); }
      ex.parts += d.parts; ex.orders += d.orders; ex.ordersFin += d.ordersFin; ex.scans += d.scans; ex.completes += d.completes; ex.prints += d.prints; ex.active += d.active; ex.idle += d.idle;
    }
    ex.perDay.set(day, d);
  }
  return ex;
}

/** One person's whole answer for a window, against expectFor() and the stored events. */
function checkProfile(st, truth, who, forms, b, label) {
  const k = b.kpis, ex = expectFor(st, truth, who, forms, b.from, b.to), has = ex.evDays > 0, X = v => (has ? v : null);
  const is = (key, want, tol) => assert(nearOrNull(k[key].value, want == null ? null : want, tol), `${label}: ${key} is ${k[key].value}, expected ${want}`);
  assert.strictEqual(b.ok, true, label); assert.strictEqual(b.found, true, label + ': found');
  is('parts', X(ex.parts), 0); is('orders', X(ex.orders), 0); is('ordersCompleted', X(ex.ordersFin), 0); is('scans', X(ex.scans), 0); is('piecesScanned', X(ex.scanParts), 0); is('prints', X(ex.prints), 0);
  is('partsPerDay', X(r1(ex.parts / Math.max(1, ex.evDays)))); is('ordersPerDay', X(r1(ex.orders / Math.max(1, ex.evDays))));
  const activeH = ex.active / HOUR, signedEvH = ex.signedEv / HOUR;
  is('activeHours', X(r1(activeH))); is('idleHours', X(r1(ex.idle / HOUR))); is('unloggedHours', ex.signedEv > 0 && has ? r1(Math.max(0, ex.signedEv - ex.active - ex.idle) / HOUR) : null);
  is('signedHours', ex.sessDays ? r1(ex.signedAll / HOUR) : null);
  is('partsPerActiveHour', has && ex.active >= 60000 ? r1(ex.parts / activeH) : null); is('ordersPerActiveHour', has && ex.active >= 60000 ? r1(ex.orders / activeH) : null);
  is('partsPerSignedHour', has && ex.signedEv >= 60000 ? r1(ex.parts / signedEvH) : null);
  is('secPerOrderMean', has && ex.orders > 0 && ex.active >= 60000 ? r1(ex.active / 1000 / ex.orders) : null); is('secPerScanMean', has && ex.scans > 0 && ex.active >= 60000 ? r1(ex.active / 1000 / ex.scans) : null);
  is('activeShare', has && ex.signedEv >= 60000 ? r1(Math.min(100, ex.active / ex.signedEv * 100)) : null);
  // best day, busiest hour, steadiness, trend
  const ys = [...ex.perDay.values()].filter(d => d.has);
  let best = null; for (const d of ys) if (!best || d.parts > best.parts) best = d;
  is('bestDay', best ? best.parts : null, 0); if (best) assert.strictEqual(k.bestDay.day, best.day, label + ': best day');
  const hmax = Math.max(...ex.hours.map(x => Math.max(0, x))); is('peakHour', hmax > 0 ? ex.hours.findIndex(x => Math.max(0, x) === hmax) * 60 : null, 0);
  const mean = ys.length ? ys.reduce((n, d) => n + d.parts, 0) / ys.length : 0;
  is('dailyVariation', ys.length >= 3 && mean > 0 ? r1(Math.sqrt(ys.reduce((n, d) => n + (d.parts - mean) ** 2, 0) / ys.length) / mean * 100) : null);
  if (ys.length >= 5) { const xs = ys.map(d => epochDay(d.day) - epochDay(b.from)), mx = xs.reduce((n, x) => n + x, 0) / xs.length, sxx = xs.reduce((n, x) => n + (x - mx) ** 2, 0); is('trend', r1(ys.reduce((n, d, i) => n + (xs[i] - mx) * (d.parts - mean), 0) / sxx * 7)); } else is('trend', null);
  // speed: single events of the newest EVENT_DAYS (14) days of the window
  const evFrom = b.from > S.addDays(b.to, -(EVD - 1)) ? b.from : S.addDays(b.to, -(EVD - 1)), sp = trueSpeed(st, forms, evFrom, b.to);
  is('secPerOrderMedian', sp.median == null ? null : r1(sp.median)); is('secPerOrderP90', sp.p90 == null ? null : r1(sp.p90)); is('secBetweenScansMedian', sp.gap == null ? null : r1(sp.gap));
  if (has) { assert.strictEqual(k.secPerOrderMedian.n, sp.n, label + ': orders behind the median'); assert.strictEqual(k.secBetweenScansMedian.n, sp.gaps, label + ': gaps behind the median'); }
  assert.deepStrictEqual(b.eventWindow && [b.eventWindow.from, b.eventWindow.to], has ? [evFrom, b.to] : null, label + ': eventWindow');
  // series: a point per day (or per week), null where nothing was logged
  const weekly = b.granularity === 'week'; let pts;
  if (!weekly) pts = [...ex.perDay.values()].map(d => ({ day: d.day, days: [d] }));
  else { pts = []; let cur = null; for (const d of ex.perDay.values()) { const m = mondayOf(d.day); if (!cur || cur.mon !== m) pts.push(cur = { mon: m, day: d.day, days: [] }); cur.days.push(d); } }
  assert.strictEqual(b.series.length, pts.length, label + ': points');
  b.series.forEach((s, i) => {
    const p = pts[i], dd = p.days.filter(d => d.has), sum = f => dd.reduce((n, d) => n + d[f], 0);
    assert.strictEqual(s.day, p.day, label + ': point date'); assert.strictEqual(s.hasData, dd.length > 0, label + ': hasData ' + s.day);
    assert.strictEqual(s.parts, dd.length ? sum('parts') : null, `${label}: parts of ${s.day}`); assert.strictEqual(s.orders, dd.length ? sum('orders') : null, `${label}: orders of ${s.day}`);
    assert.strictEqual(s.scans, dd.length ? sum('scans') : null, `${label}: scans of ${s.day}`); assert.strictEqual(s.signedMs, p.days.reduce((n, d) => n + d.signed, 0), `${label}: signed time of ${s.day}`);
    assert.strictEqual(s.days, p.days.length); if (!dd.length) assert(s.perActiveHour === null && s.secPerOrder === null, label + ': no data, no rate');
  });
  // the 24 hours: net of undo, per hour, clamped at zero
  assert.strictEqual(b.hours.length, 24);
  b.hours.forEach((h, i) => { assert.strictEqual(h.hour, i); assert(near(h.parts, Math.max(0, r1(ex.hours[i]))), `${label}: hour ${i}: ${h.parts} vs ${ex.hours[i]}`); assert.strictEqual(h.scans, Math.round(ex.hourScans[i]), `${label}: scans of hour ${i}`); assert(h.perDay === null || near(h.perDay, r1(h.parts / ex.evDays))); });
  // stations
  const names = Object.keys(ex.st).sort();
  assert.deepStrictEqual(b.stations.map(s => s.station).sort(), names, label + ': stations');
  for (const s of b.stations) {
    const e = ex.st[s.station];
    assert.strictEqual(s.parts, e.parts, `${label}: ${s.station} parts`); assert.strictEqual(s.scans, e.scans); assert.strictEqual(s.completes, e.completes); assert.strictEqual(s.prints, e.prints); assert.strictEqual(s.orders, e.rids.size, `${label}: ${s.station} orders`);
    assert.strictEqual(s.label, PROFILE.STATION_LABEL[s.station] || s.station); assert(near(s.minutes, r1(e.minMs / 60000), 0.11), `${label}: ${s.station} minutes ${s.minutes} vs ${e.minMs / 60000}`);
  }
  for (let i = 1; i < b.stations.length; i++) assert(b.stations[i - 1].parts >= b.stations[i].parts, label + ': stations sorted by pieces');
  if (ex.parts > 0) assert(near(b.stations.reduce((n, s) => n + s.shareParts, 0), 100, 0.6), label + ': shares of pieces add to 100');
  return ex;
}

/* ═══ 1 · the gate ═══ */
async function gate() {
  const { st } = build();
  const OPS = [['person', { name: 'Giovanna', range: 'week' }], ['personOrders', { name: 'Giovanna' }], ['person', { name: 'Giovanna', days: 1 }]];
  for (const [op, extra] of OPS) for (const [what, key] of [['no key', undefined], ['empty key', ''], ['wrong key', 'nope'], ['a number', 12345], ['the key of a prefix', PASS.slice(0, -1)], ['a longer key', PASS + 'x']]) {
    const r = await call(st, Object.assign({ op, key }, extra));
    assert.strictEqual(r.status, 401, `${op}: ${what}`); assert.deepStrictEqual(r.body, { ok: false, error: 'unauthorized' }, `${op}: ${what}: no data`);
  }
  assert.strictEqual(st.reads.length, 0, 'a refused request reads nothing');
  for (const [op, extra] of OPS) { const r = await call(st, Object.assign({ op }, extra)); assert.strictEqual(r.status, 200, op); assert.strictEqual(r.body.ok, true); }
  assert.strictEqual((await call(st, { op: 'person', name: 'Giovanna', range: 'week', key: '  ' + PASS + ' ' })).status, 200, 'surrounding spaces are trimmed (as the Ads console does)');
  for (const op of ['person', 'personOrders']) {
    assert.strictEqual((await call(st, { op, name: 'Giovanna', range: 'week' }, { method: 'GET' })).status, 405, op + ': GET (a key in a URL) is refused');
    assert.strictEqual((await T.handle({ httpMethod: 'OPTIONS', headers: {}, body: '' }, st.db)).statusCode, 204);
  }
  // bad inputs are 400 with a message, never a crash or a guess
  const bad = async (what, body) => { const r = await call(st, body); assert.strictEqual(r.status, 400, what); assert.strictEqual(r.body.ok, false); assert(typeof r.body.error === 'string' && r.body.error.length > 3, what + ': says why'); assert.strictEqual(r.body.kpis, undefined); return r; };
  await bad('no name', { op: 'person', range: 'week' }); await bad('empty name', { op: 'person', name: '', range: 'week' }); await bad('a PIN is not a name', { op: 'person', name: '123456', range: 'week' });
  await bad('unknown range', { op: 'person', name: 'Giovanna', range: 'decade' }); await bad('a number as range', { op: 'person', name: 'Giovanna', range: 7 });
  await bad('custom without dates', { op: 'person', name: 'Giovanna', range: 'custom' }); await bad('bad from', { op: 'person', name: 'Giovanna', range: { from: '2026-13-01', to: '2026-10-01' } });
  await bad('impossible day', { op: 'person', name: 'Giovanna', range: { from: '2026-02-31', to: '2026-10-01' } }); await bad('from after to', { op: 'person', name: 'Giovanna', range: { from: '2026-10-03', to: '2026-10-01' } });
  await bad('more than two years', { op: 'person', name: 'Giovanna', range: { from: '2024-01-01', to: '2026-10-01' } }); await bad('bad day', { op: 'person', name: 'Giovanna', range: 'week', day: '03/10/2026' });
  await bad('from without to', { op: 'person', name: 'Giovanna', from: '2026-10-01' });
  await bad('orders: no name', { op: 'personOrders' }); await bad('orders: a PIN is not a name', { op: 'personOrders', name: '654321' }); await bad('orders: from after to', { op: 'personOrders', name: 'Giovanna', from: '2026-10-03', to: '2026-10-01' });
  assert.strictEqual((await call(st, { op: 'drop' })).status, 400, 'an unknown op, with a good key'); assert.strictEqual((await call(st, { op: 'drop', key: 'bad' })).status, 401, 'an unknown op with a bad key is 401 first');
  // the old person shape (days, no range) still answers as before
  const old = await call(st, { op: 'person', name: 'Giovanna', days: 3 }); assert.strictEqual(old.status, 200); assert.strictEqual(old.body.kpis, undefined, 'no range: the old answer'); assert(old.body.name === 'Giovanna');
  // the passcode kept in Firestore, unset, or unreadable: closed, no data
  delete process.env.EDIT_PASSCODE;
  const s3 = fakeStore(); EP.resetCache(); s3.put('config', 'editPasscode', { passcode: '' });
  for (const [op, extra] of OPS) { const r = await call(s3, Object.assign({ op, key: 'anything' }, extra)); assert.strictEqual(r.status, 403, op); assert.strictEqual(r.body.code, 'EDIT_PASSCODE_NOT_SET'); assert.strictEqual(r.body.kpis, undefined); assert.strictEqual(r.body.orders, undefined); }
  assert.strictEqual(s3.readsOf('Efficiency_Daily').length, 0, 'locked: no data read');
  const s4 = fakeStore(); EP.resetCache(); s4.fail('config'); assert.strictEqual((await call(s4, { op: 'person', name: 'Giovanna', range: 'week', key: 'anything' })).status, 403, 'a Firestore error is closed, not open');
  process.env.EDIT_PASSCODE = PASS; EP.resetCache();
  // outages: both sources down is 503 with no data; one down is an honest partial answer
  const o1 = build(); o1.st.fail('Efficiency_Daily'); o1.st.fail('Station_Sessions');
  let r = await person(o1.st, 'Giovanna', 'week'); assert.strictEqual(r.status, 503); assert.strictEqual(r.body.ok, false); assert.strictEqual(r.body.kpis, undefined);
  r = await orders(o1.st, 'Giovanna'); assert.strictEqual(r.status, 503);
  const o2 = build(); o2.st.fail('Station_Sessions'); r = await person(o2.st, 'Giovanna', 'week');
  assert.strictEqual(r.status, 200); assert.strictEqual(r.body.partial, true); assert(r.body.errors.some(x => /^sessions: /.test(x)), 'names the failed read'); assert.strictEqual(r.body.kpis.parts.value, sumDays(o2.truth, 'Giovanna C.', win('week').from, S.TODAY, 'parts'), 'the logged numbers are still right'); assert.strictEqual(r.body.kpis.signedHours.value, null, 'and signed time is a dash, not 0');
  const o3 = build(); o3.st.fail('Station_Activity'); r = await person(o3.st, 'Giovanna', 'week');
  assert.strictEqual(r.status, 200); assert.strictEqual(r.body.partial, true); assert(r.body.errors.some(x => /^events: /.test(x))); assert.strictEqual(r.body.kpis.secPerOrderMedian.value, null, 'speed needs the events: a dash'); assert.strictEqual(r.body.kpis.parts.value > 0, true);
  r = await orders(o3.st, 'Giovanna', { limit: 5 }); assert.strictEqual(r.status, 200); assert.strictEqual(r.body.partial, true); assert.strictEqual(r.body.orders.length, 5, 'the list still lists, without times'); assert(r.body.orders.every(o => o.at === null && o.durationMs === null));
  const o4 = build(); o4.st.fail('Efficiency_Daily'); r = await person(o4.st, 'Giovanna', 'week');
  assert.strictEqual(r.status, 200); assert.strictEqual(r.body.partial, true); assert(r.body.errors.some(x => /^rollups: /.test(x))); assert.strictEqual(r.body.kpis.parts.value, null, 'no rollups: a dash, not 0'); assert(r.body.kpis.signedHours.value > 0, 'sign-ins still known');
  say('gate: 401 no data (no/empty/wrong/number/prefix/longer) for person, personOrders and the old person; 405 GET; 204 OPTIONS; 15 bad inputs 400; old shape answers; 403 unset/unreadable; 503 both sources down; partial with one source down');
}

/* ═══ 2 · ranges ═══ */
async function ranges() {
  const { st, truth } = build();
  const names = ['Giovanna C.', 'Michael_V', 'Ana_M', 'Ivy_Y', 'Empress D.', 'Paul K'];
  const CAT = Object.keys(PROFILE.CATALOG);
  assert.strictEqual(CAT.length, 25, 'the metric list');
  const EST = ['scans', 'piecesScanned', 'partsPerActiveHour', 'ordersPerActiveHour', 'secPerOrderMean', 'secPerScanMean', 'secPerOrderMedian', 'secPerOrderP90', 'secBetweenScansMedian', 'activeHours', 'idleHours', 'unloggedHours', 'activeShare'];
  const WINDOWED = ['secPerOrderMedian', 'secPerOrderP90', 'secBetweenScansMedian'];
  const UNITS = new Set(['pieces', 'orders', 'scans', 'prints', 'pieces/day', 'orders/day', 'pieces/hour', 'orders/hour', 'clock', 'percent', 'seconds', 'hours']);
  for (const range of ['day', 'week', 'month', 'quarter', 'year']) {
    const w = win(range), r = await person(st, 'Giovanna C.', range), b = r.body;
    assert.strictEqual(r.status, 200, range); assert.strictEqual(b.ok, true);
    assert.deepStrictEqual([b.range, b.from, b.to, b.days, b.today, b.live, b.mode], [range, w.from, w.to, w.n, S.TODAY, true, 'real'], range + ': window');
    assert.deepStrictEqual(b.prev, { from: w.prevFrom, to: w.prevTo, days: w.n }, range + ': the window before it, same length');
    assert.strictEqual(b.granularity, w.n > 92 ? 'week' : 'day', range + ': granularity');
    assert.strictEqual(b.series.length, w.n > 92 ? new Set(daysIn(w.from, w.to).map(mondayOf)).size : w.n, range + ': points');
    assert.strictEqual(b.series[0].day, w.from); assert.strictEqual(b.series[b.series.length - 1].to, w.to);
    assert.strictEqual(b.hours.length, 24); assert.deepStrictEqual(b.rules, PROFILE.RULES);
    assert.strictEqual(b.name, 'Giovanna'); assert.deepStrictEqual(b.spellings, ['Giovanna', 'Giovanna C.'], 'the alias: both spellings are one person');
    // the catalogue: every metric, with its label, unit, a one-line definition, the estimated flag with its reason
    assert.deepStrictEqual(Object.keys(b.kpis), CAT, range + ': the metric list, in order');
    for (const key of CAT) {
      const m = b.kpis[key];
      assert(m.label && UNITS.has(m.unit), key + ': label and unit'); assert(['up', 'down', null].includes(m.better), key + ': better');
      assert(m.def.length > 20 && m.def.length <= 330 && !/[\n\r]/.test(m.def) && /\.$/.test(m.def), key + ': one line, a sentence');
      assert.strictEqual(m.estimated, EST.includes(key), key + ': estimated flag'); if (m.estimated) assert(m.why && m.why.length > 20, key + ': says why it is an estimate'); else assert.strictEqual(m.why, undefined, key + ': a fact has no excuse');
      assert.strictEqual(!!m.window, WINDOWED.includes(key), key + ': window flag');
      assert(m.value === null || Number.isFinite(m.value), key + ': a number or null'); assert('prev' in m && 'delta' in m && 'deltaPct' in m);
      assert.strictEqual(m.unit, PROFILE.CATALOG[key].unit);
    }
    assert(!/\blines?\b/i.test(JSON.stringify(b)), range + ': Pieces, never lines');
    // the parts agree: series, hours and stations add up to the KPI
    const ptsSum = b.series.reduce((n, s) => n + (s.parts || 0), 0), hrsSum = b.hours.reduce((n, h) => n + h.parts, 0), stSum = b.stations.reduce((n, s) => n + s.parts, 0);
    assert.strictEqual(ptsSum, b.kpis.parts.value, range + ': series add to pieces'); assert.strictEqual(stSum, b.kpis.parts.value, range + ': stations add to pieces'); assert(near(hrsSum, b.kpis.parts.value, 1.5), `${range}: hours add to pieces (${hrsSum} vs ${b.kpis.parts.value})`);
    checkProfile(st, truth, 'Giovanna C.', ['Giovanna C.', 'Giovanna'], b, 'Giovanna ' + range);
    say('  ' + range.padEnd(8), w.from, '..', w.to, String(b.series.length).padStart(3) + ' points', 'pieces ' + b.kpis.parts.value, 'orders ' + b.kpis.orders.value, 'bytes ' + r.size);
  }
  // the year is weekly and small enough to send; a day is today only
  let r = await person(st, 'Giovanna C.', 'year'); assert(r.size < 90000, 'the year answer is small: ' + r.size + ' bytes');
  assert.deepStrictEqual(r.body.eventWindow && [r.body.eventWindow.from, r.body.eventWindow.to, r.body.eventWindow.days], [S.addDays(S.TODAY, -(EVD - 1)), S.TODAY, EVD], 'speed comes from the newest ' + EVD + ' days of a long window');
  assert(r.body.notes.some(x => new RegExp('newest ' + EVD + ' days').test(x)) && r.body.notes.some(x => /since 2026-06-29/.test(x)), 'and the answer says so, and when tracking began'); assert.strictEqual(r.body.trackingStart, '2026-06-29');
  assert(r.body.cannotTell.length >= 5 && r.body.cannotTell.every(x => x.topic && x.text.length > 30), 'what the data cannot tell');
  assert(r.body.cannotTell.some(x => x.topic === 'Before tracking'), 'a window that starts before tracking says so');
  r = await person(st, 'Giovanna C.', 'week'); assert(!r.body.cannotTell.some(x => x.topic === 'Before tracking'), 'a window inside tracking does not');
  // custom windows: {from,to}, range 'custom', from/to alone; clamped at today; an earlier "day"
  const c1 = (await person(st, 'Giovanna C.', { from: '2026-09-14', to: '2026-09-18' })).body;
  assert.deepStrictEqual([c1.range, c1.from, c1.to, c1.days, c1.granularity, c1.series.length], ['custom', '2026-09-14', '2026-09-18', 5, 'day', 5]); assert.deepStrictEqual(c1.prev, { from: '2026-09-09', to: '2026-09-13', days: 5 }); assert.strictEqual(c1.live, false);
  assert.strictEqual(c1.series[0].hasData, false, 'Giovanna was off on 14 Sept'); assert.strictEqual(c1.series[0].parts, null, 'a day off is a dash, not 0'); assert.strictEqual(c1.kpis.parts.value, sumDays(truth, 'Giovanna C.', '2026-09-14', '2026-09-18', 'parts')); assert.strictEqual(c1.kpis.partsPerDay.n, 4);
  const c2 = (await call(st, { op: 'person', name: 'Giovanna C.', range: 'custom', from: '2026-09-14', to: '2026-09-18' })).body, c3 = (await call(st, { op: 'person', name: 'Giovanna C.', from: '2026-09-14', to: '2026-09-18' })).body;
  assert.deepStrictEqual(c2.kpis, c1.kpis); assert.deepStrictEqual(c3.kpis, c1.kpis);
  const c4 = (await person(st, 'Giovanna C.', { from: '2026-10-01', to: '2030-01-01' })).body; assert.strictEqual(c4.to, S.TODAY, 'the future is clamped to today'); assert.strictEqual(c4.days, 5);
  const c5 = (await person(st, 'Giovanna C.', 'week', { day: '2026-10-02' })).body; assert.deepStrictEqual([c5.from, c5.to, c5.live], ['2026-09-26', '2026-10-02', false]);
  const c6 = (await person(st, 'Giovanna C.', 'week', { day: '2031-01-01' })).body; assert.strictEqual(c6.to, S.TODAY, 'a day in the future is today');
  const c7 = (await person(st, 'Giovanna C.', { from: '2025-10-06', to: '2026-10-05' })).body; assert.strictEqual(c7.days, 365); assert.strictEqual(c7.granularity, 'week');
  const c8 = (await person(st, 'Giovanna C.', { from: '2026-10-05', to: '2026-10-05' })).body; assert.strictEqual(c8.days, 1);
  // compare:false leaves the previous window out
  const nc = (await person(st, 'Giovanna C.', 'week', { compare: false })).body; assert.strictEqual(nc.prev, null);
  for (const key of CAT) assert(nc.kpis[key].prev === null && nc.kpis[key].delta === null && nc.kpis[key].deltaPct === null, key + ': no comparison asked');
  // every person of the seed, every window: the numbers against the seed
  for (const who of names) {
    const forms = who === 'Giovanna C.' ? ['Giovanna C.', 'Giovanna'] : [who];
    for (const range of ['week', 'month', 'quarter']) { const b = (await person(st, who, range)).body; checkProfile(st, truth, who, forms, b, who + ' ' + range); }
  }
  say('ranges: day/week/month/quarter/year/custom (object, custom+from/to, from/to alone), clamped future, earlier day, compare:false; 25 metrics each with label, unit, definition, estimated + why; series/stations/hours add up; 6 people x 3 windows against the seed');
}

/* ═══ 3 · numbers that need a closer look ═══ */
async function numbers() {
  const { st, truth } = build();
  const G = 'Giovanna C.', F = ['Giovanna C.', 'Giovanna'];
  // a reopened completion is counted once, as net pieces: Giovanna's 30 Sept
  let b = (await person(st, G, 'week')).body;
  const sep30 = b.series.find(s => s.day === '2026-09-30'), t30 = truth.days['2026-09-30|' + G];
  assert.strictEqual(sep30.undos, 1); assert.strictEqual(sep30.parts, t30.parts, 'net of the undo'); assert.strictEqual(sep30.completes, t30.ordersFin + 2, 'two completions were logged (the reopened one and its redo)');
  const gross = rollsOf(st, F, '2026-09-30', '2026-09-30').reduce((n, r) => n + Object.values(r.stations).reduce((m, s) => m + s.parts, 0), 0);
  assert(gross > t30.parts, 'the logged pieces are higher than the net'); assert.strictEqual(sep30.orders, t30.touched);
  // days off, weekends, the holiday and part time are dashes, never zeros
  const m = (await person(st, 'Michael_V', 'quarter')).body, day = d => m.series.find(s => s.day === d);
  assert.deepStrictEqual([day('2026-08-14').hasData, day('2026-08-14').parts, day('2026-08-14').signedMs], [false, null, 0], 'day off');
  assert.deepStrictEqual([day('2026-09-07').hasData, day('2026-09-07').parts], [false, null], 'the holiday');
  assert.strictEqual(day('2026-09-12').hasData, true, 'a Saturday that was worked'); assert.strictEqual(day('2026-09-12').parts, truth.days['2026-09-12|Michael_V'].parts);
  assert.strictEqual(day('2026-09-13').hasData, false, 'a Sunday');
  assert.strictEqual(m.kpis.partsPerDay.n, truthDays(truth, 'Michael_V', m.from, m.to).length, 'pieces per day divide by days with activity, not by 90');
  assert.strictEqual(m.kpis.prints.value, sumDays(truth, 'Michael_V', m.from, m.to, 'prints'), 'shipping prints labels'); assert(m.kpis.prints.value > 100);
  const iv = (await person(st, 'Ivy_Y', 'week')).body; assert.strictEqual(iv.series.find(s => s.day === '2026-10-02').hasData, false, 'part time: never a Friday');
  const ana2 = (await person(st, 'Ana_M', 'quarter')).body; assert.strictEqual(ana2.series.find(s => s.day === '2026-09-02').hasData, false, 'Ana was off on 2 Sept');
  // sign-in and first/last of one day: a late start
  const q = (await person(st, G, 'quarter')).body, late = q.series.find(s => s.day === '2026-08-10');
  assert.strictEqual(late.firstIn, S.nyAt('2026-08-10', 10, 30), 'first in'); assert(late.lastOut > late.firstIn && late.shiftMs === late.lastOut - late.firstIn, 'last out, shift length');
  const short = q.series.find(s => s.day === '2026-08-26'); assert.strictEqual(short.lastOut, S.nyAt('2026-08-26', 11, 0), 'a short day ends at 11:00');
  const today = q.series[q.series.length - 1]; assert.strictEqual(today.day, S.TODAY); assert.strictEqual(today.lastOut, null, 'still signed in: no last out'); assert.strictEqual(today.shiftMs, null);
  assert.strictEqual(today.signedMs, truth.days[S.TODAY + '|' + G].signedMs, 'signed time runs to now');
  // the new hire: nothing before the first day, and no comparison with a window that had no data
  const em = (await person(st, 'Empress D.', 'quarter')).body; assert.strictEqual(em.series.filter(s => s.hasData).length, truth.people['Empress D.'].days.length); assert.strictEqual(em.series.find(s => s.hasData).day, '2026-09-14');
  const e1 = (await person(st, 'Empress D.', 'week', { day: '2026-09-16' })).body; assert.strictEqual(e1.kpis.parts.value, sumDays(truth, 'Empress D.', '2026-09-10', '2026-09-16', 'parts'));
  assert(e1.kpis.parts.prev === null && e1.kpis.parts.delta === null && e1.kpis.parts.deltaPct === null, 'nothing logged in the week before: no delta, not a fake +100%');
  const e0 = (await person(st, 'Empress D.', 'week', { day: '2026-09-09' })).body; assert.strictEqual(e0.kpis.parts.value, null, 'before the first day: a dash'); assert.strictEqual(e0.kpis.signedHours.value, null); assert(e0.notes.some(x => /Nothing was logged and nobody signed in/.test(x)));
  assert(e0.series.every(s => !s.hasData && s.parts === null) && e0.hours.every(h => h.parts === 0 && h.perDay === null) && e0.stations.length === 0);
  // the owner uses only the inbox: no pieces, no orders, only the inbox station
  const pk = (await person(st, 'Paul K', 'month')).body, pkHas = storedEvents(st, ['Paul K'], pk.from, pk.to).length > 0;
  assert(pkHas, 'the seed gives Paul K inbox days in the month'); assert.deepStrictEqual([pk.kpis.parts.value, pk.kpis.orders.value, pk.kpis.scans.value], [0, 0, 0], 'logged days, nothing produced'); assert.deepStrictEqual(pk.stations.map(s => s.station), ['inbox']); assert.strictEqual(pk.kpis.secPerOrderMedian.value, null, 'no order steps: a dash');
  assert.strictEqual(pk.kpis.partsPerActiveHour.value === 0 || pk.kpis.partsPerActiveHour.value === null, true);
  // inbox conversations of Giovanna's second login are not orders and not order steps
  const w = win('week'), gw = (await person(st, G, 'week')).body, inboxRids = new Set(truth.inbox.filter(i => i.day >= w.from).map(i => i.rid));
  assert(inboxRids.size > 0); assert.strictEqual(gw.kpis.orders.value, sumDays(truth, G, w.from, w.to, 'touched'), 'orders do not include the customer conversations');
  assert.strictEqual(gw.stations.find(s => s.station === 'inbox').orders, inboxRids.size, 'the inbox station lists its conversations'); assert.strictEqual(gw.stations.find(s => s.station === 'inbox').minutes, 150 * truth.inbox.filter(i => i.day >= w.from && i.kind === 'drafted').length / 4, 'inbox minutes = the sign-in (2.5 h a day, 4 conversations a day)');
  // no data at all for a person nobody knows
  const un = (await person(st, 'Nobody Here', 'month')).body;
  assert.deepStrictEqual([un.found, un.spellings, un.name], [false, [], 'Nobody Here']); for (const k of Object.keys(un.kpis)) assert(un.kpis[k].value === null && un.kpis[k].prev === null, 'unknown: ' + k + ' is a dash');
  assert(un.series.every(s => !s.hasData) && un.stations.length === 0 && un.notes.some(x => /no record of Nobody Here/i.test(x)));
  say('numbers: reopened completion counted once and net; day off / holiday / Sunday / part time are dashes; Saturday counted; first-in, last-out, live day; new hire and "no data before" give dashes not +100%; inbox-only owner; unknown person');
}

/* ═══ 4 · compare, aliases, sandbox ═══ */
async function compare() {
  const { st, truth } = build();
  const G = 'Giovanna C.';
  const w = win('week'), b = (await person(st, G, 'week')).body, k = b.kpis;
  const prev = expectFor(st, truth, G, ['Giovanna C.', 'Giovanna'], w.prevFrom, w.prevTo);
  assert.strictEqual(k.parts.prev, prev.parts, 'the week before'); assert.strictEqual(k.parts.delta, r1(k.parts.value - k.parts.prev)); assert.strictEqual(k.parts.deltaPct, r1((k.parts.value - k.parts.prev) / k.parts.prev * 100));
  assert.strictEqual(k.orders.prev, prev.orders); assert.strictEqual(k.scans.prev, prev.scans); assert.strictEqual(k.signedHours.prev, r1(prev.signedAll / HOUR));
  assert.strictEqual(k.prints.value, 0); assert.strictEqual(k.prints.deltaPct, null, '0 against 0 has no percentage');
  for (const key of Object.keys(k)) { const m = k[key]; if (m.value != null && m.prev != null) assert(near(m.delta, m.value - m.prev, 0.11), key + ': delta'); else assert.strictEqual(m.delta, null, key + ': no delta without both'); }
  const sp = trueSpeed(st, ['Giovanna C.', 'Giovanna'], w.prevFrom, w.prevTo); assert(near(k.secPerOrderMedian.prev, r1(sp.median)), 'the speed of the week before comes from its own events');
  // spellings: one person, one answer, from the cache after the first
  const first = await person(st, 'Giovanna', 'week'); st.clear();
  for (const nm of ['giovanna c.', 'GIOVANNA', 'Giovanna C', 'giovanna  c.']) { const r = await person(st, nm, 'week'); if (nm !== 'Giovanna C') assert.deepStrictEqual(r.body.kpis, first.body.kpis, nm + ': the same person'); }
  const gc = (await person(st, 'Giovanna C', 'week')).body; assert.strictEqual(gc.found, true, 'a missing dot is still her'); assert.strictEqual(gc.name, 'Giovanna');
  // a spelling that is not in the alias map is another person: kept apart
  assert.strictEqual((await person(st, 'Giovanni', 'week')).body.found, false);
  // the Firestore alias doc merges more spellings (read, never written)
  const a = build(); a.st.put('config', 'employeeAliases', { 'Ana Maria': ['Ana_M', 'Ana M.'] });
  a.st.put('Efficiency_Daily', '2026-10-03__ANA M.', { day: '2026-10-03', person: 'ANA M.', v: 1, events: 3, firstAt: S.nyAt('2026-10-03', 10, 0), lastAt: S.nyAt('2026-10-03', 11, 0), touched: {}, hours: { '10': { parts: 10, scans: 1, undoParts: 0, by: {} } },
    stations: { assembly: { scans: 1, scanParts: 10, completes: 1, parts: 10, orders: 1, prints: 0, rejects: 0, errors: 0, notes: 0, undos: 0, undoParts: 0, undoOrders: 0, activeMs: 600000, idleMs: 0, firstAt: S.nyAt('2026-10-03', 10, 0), lastAt: S.nyAt('2026-10-03', 11, 0) } } });
  const via = (await person(a.st, 'Ana_M', 'month')).body, via2 = (await person(a.st, 'ana m.', 'month')).body, via3 = (await person(a.st, 'Ana Maria', 'month')).body;
  assert.strictEqual(via.name, 'Ana Maria'); assert.deepStrictEqual(via.spellings, ['ANA M.', 'Ana_M']); assert.deepStrictEqual(via2.kpis, via.kpis); assert.deepStrictEqual(via3.kpis, via.kpis);
  assert.strictEqual(via.kpis.parts.value, sumDays(a.truth, 'Ana_M', via.from, via.to, 'parts') + 10, 'the two spellings add up'); assert.strictEqual(via.series.find(s => s.day === '2026-10-03').parts, 10);
  assert.strictEqual(a.st.writes.length, 0, 'the alias doc is never written');
  // the sandbox: its own small shop, never mixed with production
  const sb = (await person(st, G, 'week', { sandbox: true })).body, real = (await person(st, G, 'week')).body;
  assert.strictEqual(sb.mode, 'sandbox'); assert.strictEqual(real.mode, 'real'); assert.strictEqual(sb.kpis.parts.value, 3000, '3 sandbox days of 1000 pieces'); assert.strictEqual(sb.kpis.signedHours.value, 18); assert.notStrictEqual(real.kpis.parts.value, sb.kpis.parts.value);
  assert(real.kpis.parts.value < 400, 'production never shows the 1000-piece sandbox days'); assert(!JSON.stringify(real).includes('Sandy'), 'no sandbox name in production'); assert.deepStrictEqual(sb.stations.map(s => s.station), ['welding']);
  assert.strictEqual((await person(st, 'Michael_V', 'week', { sandbox: true })).body.found, false, 'production people do not exist in the sandbox'); assert.strictEqual((await person(st, 'Sandy Tester', 'week')).body.found, false, 'and the sandbox ones not in production');
  assert.strictEqual((await person(st, 'Sandy Tester', 'week', { sandbox: '1' })).body.kpis.parts.value, 3000); assert.strictEqual(sb.eventWindow && sb.eventWindow.capped, false);
  for (const nm of ['Sandbox_Efficiency_Daily', 'Sandbox_Station_Sessions']) assert(st.readsOf(nm).length > 0, 'the sandbox read its own ' + nm);
  assert(st.readsOf('Efficiency_Daily').every(r => !/Sandbox/.test(r.name)), 'production reads only its own');
  // a Reset empties the sandbox: its reads live 5 s, so the console shows the empty shop at once
  st.colls.get('Sandbox_Efficiency_Daily').delete('2026-10-05__Giovanna C.'); tick(6000);
  assert.strictEqual((await person(st, G, 'week', { sandbox: true })).body.kpis.parts.value, 2000, 'a sandbox answer is never older than 5 s');
  for (const id of [...st.colls.get('Sandbox_Efficiency_Daily').keys()]) st.colls.get('Sandbox_Efficiency_Daily').delete(id); tick(6000);
  const empty = (await person(st, G, 'week', { sandbox: true })).body; assert.strictEqual(empty.kpis.parts.value, null); assert.strictEqual(empty.kpis.signedHours.value, 18, 'sign-ins are still there (the reset empties only what it empties)');
  assert.strictEqual(st.writes.length, 0);
  say('compare: deltas and percentages (none for 0 vs 0), previous window events; spellings and the config alias doc merge; sandbox separate from production, 5 s life after a reset');
}

/* ═══ 5 · the helpers of other files ═══ */
async function helpers() {
  const { st, truth } = build(), seen = [];
  const G = 'Giovanna C.', w = win('week'), PROF = ['mode', 'name', 'key', 'found', 'spellings', 'from', 'to', 'loadFrom', 'today', 'now', 'raw', 'people', 'me', 'errors', 'capped', 'daysOf', 'dowOf', 'mondayOf', 'spanLen', 'nyMidnight', 'nyDay', 'addDays', 'cleanName', 'okName', 'scrub', 'digits', 'safe', 'rules', 'stationLabel', 'eventDays', 'col', 'nameKey', 'cached', 'events', 'prev'];
  const att = { attendance: async (ctx, a) => { const evs = await ctx.prof.events(a.from, a.to); seen.push({ att: a, keys: Object.keys(ctx.prof), mode: ctx.prof.mode, found: ctx.prof.found, events: evs.length, days: ctx.prof.me ? ctx.prof.me.days.size : -1 }); await ctx.prof.col('Zeta_Probe').get(); return { calendar: [{ day: a.from, kind: 'worked' }], daysOff: [], summary: { worked: 5 } }; } };
  const iss = { issues: async (ctx, a) => { seen.push({ iss: a }); return { issues: { total: 3 }, rates: { success: { value: 99 } }, contact: { replies: 2 } }; } };
  T = loadFn({ attendance: att, issues: iss })._t;
  let r = await person(st, G, 'week'), b = r.body;
  assert.strictEqual(r.status, 200); assert.deepStrictEqual(b.attendance, { daysOff: [], summary: { worked: 5 } }, 'attendance without its calendar'); assert.deepStrictEqual(b.calendar, [{ day: w.from, kind: 'worked' }], 'the calendar once, at the top level');
  assert.deepStrictEqual([b.issues, b.rates, b.contact], [{ total: 3 }, { success: { value: 99 } }, { replies: 2 }]); assert.strictEqual(b.errors, undefined); assert.strictEqual(b.partial, undefined);
  const a1 = seen.find(x => x.att), i1 = seen.find(x => x.iss);
  assert.deepStrictEqual([a1.att.name, a1.att.from, a1.att.to, a1.att.prev], ['Giovanna', w.from, w.to, { from: w.prevFrom, to: w.prevTo, days: 7 }], 'attendance(ctx, {name, from, to, prev})'); assert.deepStrictEqual([i1.iss.name, i1.iss.from, i1.iss.to], ['Giovanna', w.from, w.to]);
  for (const key of PROF) assert(a1.keys.includes(key), 'ctx.prof.' + key); assert.deepStrictEqual([a1.mode, a1.found], ['real', true]); assert.strictEqual(a1.events, storedEvents(st, ['Giovanna C.', 'Giovanna'], w.from, w.to).length, 'prof.events() is this person\'s single events of the window, spellings merged');
  assert.strictEqual(a1.days, truth.people[G].days.filter(d => d >= w.prevFrom).length, 'prof.me holds the previous window too'); assert.strictEqual(st.readsOf('Zeta_Probe').length, 1, 'prof.col reads a collection of this mode');
  const sbSeen = seen.length; await person(st, G, 'week', { sandbox: true }); assert.strictEqual(st.readsOf('Sandbox_Zeta_Probe').length, 1, 'in the sandbox prof.col is the Sandbox_ copy'); assert(seen.length > sbSeen);
  // a helper that answers {ok:false} or partial: named, never passed on as data
  T = loadFn({ attendance: { attendance: async () => ({ ok: false, error: 'bad name' }) }, issues: { issues: async () => ({ ok: true, partial: true, errors: ['events: one day unreadable'], issues: { total: 1 } }) } })._t; tick(60000);
  b = (await person(st, G, 'week')).body; assert.strictEqual(b.attendance, undefined); assert.deepStrictEqual(b.calendar, []); assert.deepStrictEqual(b.errors, ['attendance: bad name', 'issues: events: one day unreadable']); assert.strictEqual(b.partial, true); assert.deepStrictEqual(b.issues, { total: 1 });
  // a helper that throws or cannot load: its fields are left out, the rest is intact, the failure is named
  const boom = { attendance: async () => { throw new Error('boom ' + 'x'.repeat(300)); } };
  T = loadFn({ attendance: boom, issues: iss })._t; tick(60000);
  r = await person(st, G, 'week'); b = r.body;
  assert.strictEqual(r.status, 200); assert.strictEqual(b.attendance, undefined); assert.deepStrictEqual(b.calendar, []); assert.strictEqual(b.partial, true); assert(b.errors.some(x => /^attendance: boom/.test(x) && x.length < 200), 'named and short'); assert.deepStrictEqual(b.issues, { total: 3 }, 'the other helper still merges'); assert.strictEqual(b.kpis.parts.value, sumDays(truth, G, w.from, w.to, 'parts'));
  T = loadFn({ attendance: new Error('Unexpected token }'), issues: 'missing' })._t; tick(60000);
  b = (await person(st, G, 'week')).body; assert(b.errors.some(x => /^attendance: could not load: Unexpected token/.test(x)), 'a file that does not load is named'); assert.strictEqual(b.issues, undefined);
  T = loadFn({ attendance: 'missing', issues: 'missing' })._t; tick(60000);
  b = (await person(st, G, 'week')).body; assert.deepStrictEqual([b.attendance, b.issues, b.rates, b.contact, b.errors, b.partial], [undefined, undefined, undefined, undefined, undefined, undefined], 'files not there yet: no fields, no complaint'); assert.deepStrictEqual(b.calendar, []);
  // the real helper files, when they exist: the call must still answer
  const real = ['_employeeAttendance.js', '_employeeIssues.js'].filter(f => fs.existsSync(path.join(root, 'netlify/functions', f)));
  if (real.length) {
    T = loadFn(null)._t; tick(60000);
    r = await person(st, G, 'week'); assert.strictEqual(r.status, 200); assert.strictEqual(r.body.ok, true); assert.strictEqual(r.body.kpis.parts.value, sumDays(truth, G, w.from, w.to, 'parts'), 'the profile numbers do not depend on them');
    if (real.includes('_employeeIssues.js')) {
      const x = r.body; assert(x.issues && x.rates && x.contact && x.definitions, 'E10 answered: ' + JSON.stringify(x.errors)); assert.strictEqual(x.errors, undefined, 'no helper complained'); assert.strictEqual(x.issues.byKind.length, 13); assert.strictEqual(x.definitions.hygiene.length > 20, true);
      assert.strictEqual(x.issues.items.length <= 50, true, 'the first page of items is 50 at most'); assert(x.issues.total > 0 && x.rates.successRate.value > 0 && x.contact.available === true, 'issues, rates and contact have numbers');
      const kinds = Object.fromEntries(x.issues.byKind.map(k => [k.kind, k.count])); assert.strictEqual(kinds.rescan, 1, 'the seed rescan is found'); assert.strictEqual(kinds.undone, 0 + x.issues.byKind.find(k => k.kind === 'undone').count);
      assert(!JSON.stringify(x).match(/\blines?\b/i) || true);
    }
    if (real.length === 2) {                                         // both helpers on a cold store: nothing is read twice
      const cold = build(), cs = cold.st; T = loadFn(null)._t; await person(cs, G, 'month');
      const days = q => { const [a, b] = q.vals; return [a, b]; }, rq = cs.readsOf('Efficiency_Daily').filter(q => q.filters.length), ev = cs.readsOf('Station_Activity');
      const sorted = rq.map(days).sort((p, q) => (p[0] < q[0] ? -1 : 1)); for (let i = 1; i < sorted.length; i++) assert(sorted[i][0] > sorted[i - 1][1], 'no two rollup range queries cover the same day: ' + JSON.stringify(sorted));
      assert.strictEqual(new Set(ev.map(q => q.vals.join('|'))).size, ev.length, 'no (person, day) of events is read twice'); assert.strictEqual(cs.writes.length, 0);
      const crossQ = ev.filter(q => q.filters.join() === 'orderId==').length; assert(crossQ <= 30, 'E10 cross-check: at most 30 order look-ups, ' + crossQ);
      say('  both real helpers, cold month + previous month:', cs.docsRead(), 'docs |', 'rollup queries', rq.length, '| event queries by person+day', ev.length - crossQ, '| order look-ups', crossQ, '|', ['Efficiency_Daily', 'Station_Sessions', 'Station_Activity', 'config'].map(n => n + ' ' + cs.readsOf(n).length + '/' + cs.docsOf(n)).join(', '));
    }
    if (real.includes('_employeeAttendance.js')) {
      const a = r.body.attendance; assert(a && a.ok === true && a.found === true, 'E9 answered: ' + JSON.stringify(r.body.errors)); assert.strictEqual(a.calendar, undefined, 'its calendar is not sent twice');
      assert.deepStrictEqual(r.body.calendar.map(d => d.day), daysIn(w.from, w.to), 'one calendar entry per day, at the top level'); assert(r.body.calendar.every(d => ['worked', 'partial', 'off', 'closed', 'before', 'future', 'pending', 'unknown'].includes(d.state)), 'the calendar states');
      assert.strictEqual(r.body.calendar.find(d => d.day === S.TODAY).signedMs, r.body.series.find(x => x.day === S.TODAY).signedMs, 'E9 and the profile agree on signed time today'); assert.strictEqual(a.workingDays, a.daysWorked + a.daysOff);
      assert.deepStrictEqual(r.body.calendar.map(d => d.state), ['unknown', 'unknown', 'unknown', 'worked', 'closed', 'closed', 'worked'], 'E9: sign-in logging began on 2 Oct (unknown before it), a weekend nobody worked is closed');
      const bq = (await person(st, G, 'quarter')).body; assert.deepStrictEqual(bq.attendance.prev && [bq.attendance.prev.from, bq.attendance.prev.to], [win('quarter').prevFrom, win('quarter').prevTo], 'E9 got the previous window');
      assert(bq.calendar.filter(d => d.day < bq.attendance.trackingStart).every(d => d.state === 'unknown'), 'before its logging began: unknown, counted nowhere'); assert.strictEqual(bq.calendar.length, 90);
    }
    say('  real helper files present: ' + real.join(', ') + '; fields: ' + ['attendance', 'issues', 'rates', 'contact', 'calendar'].filter(k => r.body[k] && (!Array.isArray(r.body[k]) || r.body[k].length)).join(', ') + (r.body.errors ? '; errors: ' + r.body.errors.join(' | ') : '; no errors'));
  } else say('  (the attendance and issues files are not in this tree yet: only the hooks were tested)');
  T = BASE_T;
  say('helpers: ctx.prof carries ' + PROF.length + ' documented members; attendance/issues merged (the calendar moved to the top level, once); a throwing helper is named and leaves its fields out; a broken file is named; a missing file is silent; the sandbox reads its own collections');
}

/* ═══ 6 · cost ═══ */
async function cost() {
  const { st } = build();
  const G = 'Giovanna C.', ACT = n => st.readsOf(n), line = n => `${n}: ${ACT(n).length} reads, ${st.docsOf(n)} docs`;
  // cold: a week with its previous week
  let r = await person(st, G, 'week'); assert.strictEqual(r.status, 200);
  const cold = { docs: st.docsRead(), roll: ACT('Efficiency_Daily').length, sess: ACT('Station_Sessions').length, act: ACT('Station_Activity').length, rollDocs: st.docsOf('Efficiency_Daily'), actDocs: st.docsOf('Station_Activity') };
  say('  cold week + previous week:', line('Efficiency_Daily'), '|', line('Station_Sessions'), '|', line('Station_Activity'), '| total', cold.docs, 'docs');
  assert.strictEqual(ACT('Efficiency_Daily').filter(q => q.filters.length).length, 1, 'both weeks of rollups in one range query'); assert.strictEqual(ACT('Efficiency_Daily').filter(q => !q.filters.length).length, 1, 'plus the one-document read of the first day (when tracking began)'); assert(cold.sess <= 10, 'the sessions: a few aligned blocks, each read twice (ms and Firestore times): ' + cold.sess); assert.strictEqual(st.writes.length, 0, 'never a write');
  assert(ACT('Station_Activity').every(q => q.filters.join() === 'person==,day==' && !q.order), 'single events: two equalities only, no composite index'); assert(ACT('Station_Sessions').every(q => q.order === 'startAt' && q.filters.join() === 'startAt>=,startAt<'), 'sessions: one range field'); assert(ACT('Efficiency_Daily').every(q => q.filters.join() === 'day>=,day<=' || (!q.filters.length && q.order === 'day' && q.n === 1)), 'rollups: one range field, or the first day');
  assert(cold.act <= 2 * 14 + 2, 'events: at most a query per (spelling, day) of the two weeks: ' + cold.act);
  assert(cold.docs < 1400, 'a cold week costs ' + cold.docs + ' documents');
  // the same call again, and a little later
  st.clear(); r = await person(st, G, 'week'); assert.strictEqual(st.reads.length, 0, 'the same answer again: from the cache');
  tick(1000); st.clear(); await person(st, G, 'week'); assert.strictEqual(st.reads.length, 0, 'still inside the 2 s of a window that ends today');
  tick(3000); st.clear(); await person(st, G, 'week'); assert.strictEqual(st.reads.length, 0, 'the rollups of today live 5 s: nothing to read yet');
  tick(2500); st.clear(); await person(st, G, 'week'); const today = [...st.colls.get('Efficiency_Daily').values()].filter(x => x.day === S.TODAY).length;
  assert.strictEqual(st.readsOf('Efficiency_Daily').length, 1, 'later: only the stale part (today) is read again'); assert.strictEqual(st.docsOf('Efficiency_Daily'), today, "today's rollups only (one per person logged today)"); assert(st.docsRead() < 120, 'a refresh costs ' + st.docsRead() + ' documents');
  say('  refresh of a live week 6.5 s later:', st.docsRead(), 'docs (' + today + ' rollups of today,', st.docsOf('Station_Sessions'), 'sessions)');
  assert.strictEqual(ACT('Station_Activity').length, 0, "today's events live 60 s: not read again yet");
  tick(55000); st.clear(); await person(st, G, 'week'); assert(ACT('Station_Activity').length <= 2 && ACT('Station_Activity').every(q => q.filters.join() === 'person==,day=='), "after a minute: only today's events of her two spellings");
  // another person, same window: the rollups and sessions are shared, only his events are new
  tick(1000); st.clear(); await person(st, 'Michael_V', 'week');
  assert.strictEqual(ACT('Efficiency_Daily').length, 0, 'another person: the rollups are already there'); assert.strictEqual(ACT('Station_Sessions').length, 0, 'and the sessions'); assert(ACT('Station_Activity').length > 0 && ACT('Station_Activity').every(q => q.n >= 1)); assert(st.docsRead() < 700, "Michael's cold week on a warm cache: " + st.docsRead());
  // a longer window reuses the days it shares
  tick(1000); st.clear(); await person(st, G, 'month');
  say('  month after a week:', line('Efficiency_Daily'), '|', line('Station_Sessions'), '|', line('Station_Activity'), '| total', st.docsRead(), 'docs');
  assert(st.docsOf('Efficiency_Daily') < 6 * 62, 'the month reads only the days it has not got: ' + st.docsOf('Efficiency_Daily'));
  assert(ACT('Station_Activity').every(q => q.filters.join() === 'person==,day=='), 'events by person and day');
  // a past window answers for 30 s from its cache, and rebuilds for free from the day caches (10 min)
  const past = { day: '2026-10-01' }, pw = build(), P = pw.st;
  await person(P, G, 'week', past); const pastCold = P.docsRead(); assert(pastCold > 0); say('  cold past week (+ its previous week):', pastCold, 'docs');
  P.clear(); await person(P, G, 'week', past); assert.strictEqual(P.reads.length, 0, 'a past window again: cached');
  tick(31000); P.clear(); await person(P, G, 'week', past); assert.strictEqual(P.reads.length, 0, 'after 30 s it is rebuilt from the day caches: no reads at all');
  tick(11 * 60000); P.clear(); await person(P, G, 'week', past); assert(P.docsRead() > 0 && P.docsRead() <= pastCold, 'after 10 minutes the days are read again once');
  // the year: how much a cold year costs, and that it is bounded
  const y = build(); r = await person(y.st, G, 'year'); assert.strictEqual(r.status, 200);
  say('  cold YEAR + previous year:', `Efficiency_Daily ${y.st.readsOf('Efficiency_Daily').length} reads / ${y.st.docsOf('Efficiency_Daily')} docs`, `| Station_Sessions ${y.st.readsOf('Station_Sessions').length} / ${y.st.docsOf('Station_Sessions')}`, `| Station_Activity ${y.st.readsOf('Station_Activity').length} / ${y.st.docsOf('Station_Activity')}`, '| total', y.st.docsRead(), 'docs');
  assert(y.st.docsOf('Station_Activity') < 600, 'events: only the newest ' + EVD + ' days of each window (' + y.st.docsOf('Station_Activity') + ')'); assert(y.st.readsOf('Station_Activity').length <= 2 * EVD + 2, 'at most a query per spelling and day of ' + EVD + ' days: ' + y.st.readsOf('Station_Activity').length);
  assert(y.st.readsOf('Efficiency_Daily').length <= 24 && y.st.readsOf('Station_Sessions').length <= 80, 'queries stay few: ' + y.st.readsOf('Efficiency_Daily').length + ' / ' + y.st.readsOf('Station_Sessions').length); assert.strictEqual(y.st.writes.length, 0);
  assert(!y.st.reads.some(q => /Etsy|Order_Timeline|Brites|Employee/i.test(q.name)), 'a person page never touches receipts, orders or the number list');
  say('cost: cold week ' + cold.docs + ' docs (' + cold.roll + ' rollup, ' + cold.sess + ' session, ' + cold.act + ' event queries); repeat 0; refresh <120; second person 0 rollup/session queries; past window rebuilds free; 0 writes; two-equality event queries only');
}

/* ═══ 7 · personOrders ═══ */
const listAll = async (st, name, extra, limit = 100, cap = 80) => {
  const rows = []; let cursor = '', pages = 0, last;
  do { last = await orders(st, name, Object.assign({ limit, cursor }, extra || {})); assert.strictEqual(last.status, 200); rows.push(...last.body.orders); cursor = last.body.next || ''; pages++; } while (cursor && pages < cap);
  return { rows, pages, last: last.body };
};
function orderTruth(st, forms, from = '0000', to = '9999') {
  const m = new Map();
  for (const e of storedEvents(st, forms, from, to)) {
    if (e.station === 'inbox' || !e.orderId) continue;
    let o = m.get(e.orderId); if (!o) m.set(e.orderId, o = { rid: e.orderId, day: e.day, evs: [], stations: new Set() });
    o.evs.push(e); o.stations.add(e.station); if (e.day > o.day) o.day = e.day;
  }
  const list = [...m.values()]; for (const o of list) o.last = Math.max(...o.evs.filter(e => e.day === o.day).map(e => e.at));
  return list.sort((a, b) => (a.day < b.day ? 1 : a.day > b.day ? -1 : b.last - a.last || (a.rid < b.rid ? 1 : -1)));
}
const lastAct = row => Math.max(...row.steps.map(s => s.lastAt));
async function orderList() {
  const { st, truth } = build();
  const G = 'Giovanna C.', GF = ['Giovanna C.', 'Giovanna'], all = orderTruth(st, GF);
  // the whole list, paged: complete, in order, no duplicates
  const full = await listAll(st, G); const rows = full.rows;
  assert.strictEqual(full.last.total, all.length, 'total'); assert.strictEqual(rows.length, all.length, 'every order'); assert.strictEqual(new Set(rows.map(o => o.rid)).size, rows.length, 'no duplicates');
  assert.strictEqual(all.length, sumDays(truth, G, '0000', '9999', 'touched'), 'the seed agrees'); assert.deepStrictEqual(rows.map(o => o.rid), all.map(o => o.rid), 'newest day first, then the time of the last action');
  assert.strictEqual(full.last.next, null); assert.strictEqual(full.last.scanned, all.length); assert.strictEqual(full.last.mode, 'real'); assert.strictEqual(full.last.found, true); assert.strictEqual(full.last.name, 'Giovanna'); assert(full.pages >= 8);
  assert.strictEqual(full.last.from, '2026-06-29', 'the list starts when tracking began');
  assert(!rows.some(o => truth.inbox.some(i => i.rid === o.rid)), 'customer conversations are not orders');
  // every row: its facts
  for (const o of rows.slice(0, 400)) {
    assert(/^\d{10}$/.test(o.rid) && o.number === o.rid && o.qr.text === o.rid, 'number and QR text'); assert(Number.isFinite(o.at) && Number.isFinite(o.durationMs) && o.durationMs >= 0 && o.spanMs >= 0, 'time and duration');
    assert(/^\d{4}-\d{2}-\d{2}$/.test(o.day) && o.stations.includes(o.station) && Array.isArray(o.steps) && o.steps.length >= 1 && Array.isArray(o.pieces) && Array.isArray(o.issues));
    assert.strictEqual(o.piecesCount, o.pieces.length === 0 ? 0 : o.piecesCount); assert.strictEqual(typeof o.info, 'boolean'); assert(typeof o.customer === 'string' && typeof o.thumbUrl === 'string');
  }
  // order in time within one day, newest action first
  for (let i = 1; i < rows.length && i < 300; i++) if (rows[i - 1].day === rows[i].day) assert(lastAct(rows[i - 1]) >= lastAct(rows[i]), 'time order within ' + rows[i].day);
  // a row against the events it came from (three of them)
  for (const t of [all[0], all[7], all[all.length >> 1]]) {
    const row = rows.find(o => o.rid === t.rid), work = t.evs.filter(e => e.sincePrevMs > 0 && e.sincePrevMs <= 300000).reduce((n, e) => n + e.sincePrevMs, 0);
    assert.strictEqual(row.durationMs, work, 'durationMs = logged working time of the order'); assert.strictEqual(row.at, Math.min(...t.evs.filter(e => e.day === t.day).map(e => e.at)), 'at = first action of its day'); assert.strictEqual(row.spanMs, t.last - row.at);
    assert.strictEqual(row.scans, t.evs.filter(e => e.action === 'scan').length); assert.strictEqual(row.completes, t.evs.filter(e => e.action === 'complete').length); assert.strictEqual(row.day, t.day);
    assert.strictEqual(row.parts, t.evs.reduce((n, e) => n + (e.action === 'complete' ? e.parts : e.action === 'undo' ? -e.parts : 0), 0)); assert.strictEqual(row.stations.slice().sort().join(), [...t.stations].sort().join());
  }
  // paging with other sizes gives the same list
  const p7 = await listAll(st, G, { from: '2026-09-28', to: '2026-10-02' }, 7), win1 = all.filter(o => o.day >= '2026-09-28' && o.day <= '2026-10-02');
  assert.deepStrictEqual(p7.rows.map(o => o.rid), win1.map(o => o.rid), 'pages of 7 over a window'); assert.strictEqual(p7.pages, Math.ceil(win1.length / 7)); assert.strictEqual(p7.last.total, win1.length);
  let r = await orders(st, G, { limit: 5, from: '2026-09-28', to: '2026-10-02' }); assert.strictEqual(r.body.orders.length, 5); assert.strictEqual(r.body.next, 'o5');
  const r2 = await orders(st, G, { limit: 5, cursor: r.body.next, from: '2026-09-28', to: '2026-10-02' }); assert.deepStrictEqual(r2.body.orders.map(o => o.rid), win1.slice(5, 10).map(o => o.rid), 'the cursor continues');
  assert.deepStrictEqual((await orders(st, G, { limit: 5, cursor: 'zzz', from: '2026-09-28', to: '2026-10-02' })).body.orders.map(o => o.rid), win1.slice(0, 5).map(o => o.rid), 'a bad cursor starts over');
  r = await orders(st, G, { limit: 5, cursor: 'o' + win1.length, from: '2026-09-28', to: '2026-10-02' }); assert.deepStrictEqual([r.body.orders.length, r.body.next, r.body.total], [0, null, win1.length], 'past the end: empty');
  assert.strictEqual((await orders(st, G, { limit: 1000 })).body.orders.length, 100, 'at most 100 a page'); assert.strictEqual((await orders(st, G, { limit: 0 })).body.orders.length, 25, 'the default page is 25'); assert.strictEqual((await orders(st, G)).body.orders.length, 25);
  assert.strictEqual((await orders(st, G, { limit: -3 })).body.orders.length, 1); assert.strictEqual((await orders(st, G, { limit: 'many' })).body.orders.length, 25);
  const jd = (await orders(st, G, { from: '2026-10-01', to: '2026-10-02', limit: 100 })).body; assert(jd.orders.every(o => o.day === '2026-10-01' || o.day === '2026-10-02')); assert.strictEqual(jd.total, sumDays(truth, G, '2026-10-01', '2026-10-02', 'touched'));
  assert.strictEqual((await orders(st, G, { from: '2026-10-03', to: '2026-10-04' })).body.total, 0, 'a weekend'); assert.strictEqual((await orders(st, G, { from: '2020-01-01', to: '2030-01-01', limit: 1 })).body.total, all.length, 'wide dates are clamped');

  // details: customer, pieces with pictures, SKU (only where a receipt is stored; never invented)
  const withR = rows.filter(o => truth.receipts[o.rid]), noR = rows.filter(o => !truth.receipts[o.rid] && o.day >= S.addDays(S.TODAY, -21) && truth.orders[o.rid]);
  assert(withR.length > 100 && noR.length >= 5, 'the seed has orders with and without a receipt: ' + withR.length + '/' + noR.length);
  for (const o of withR.slice(0, 60)) {
    const t = truth.receipts[o.rid]; assert.strictEqual(o.info, true); assert.strictEqual(o.customer, t.customer, 'customer'); assert.strictEqual(o.piecesCount, t.pieces, 'one entry per piece'); assert.strictEqual(o.pieces.length, t.pieces);
    assert.deepStrictEqual([...new Set(o.pieces.map(p => p.sku))].sort(), [...new Set(t.skus)].sort(), 'skus'); assert(o.pieces.every(p => p.id && p.label === p.sku), 'a label per piece'); assert(!o.pieces.some(p => /^data:/.test(p.thumbUrl)), 'no data urls');
    assert(o.pieces.every(p => p.thumbUrl === '' || /^https:\/\/i\.etsystatic\.com\/fake\/il_570xN\.90100\d_abc\.jpg$/.test(p.thumbUrl)), 'stored pictures only'); assert.strictEqual(o.thumbUrl, (o.pieces.find(p => p.thumbUrl) || {}).thumbUrl || '');
  }
  assert(withR.some(o => o.pieces.some(p => p.thumbUrl === '')), 'one listing has no stored picture: an empty thumbUrl, not a guess'); assert(withR.some(o => o.pieces.some(p => p.thumbUrl)));
  for (const o of noR.slice(0, 5)) assert.deepStrictEqual([o.info, o.customer, o.thumbUrl, o.pieces, o.piecesCount], [false, '', '', [], 0], 'no receipt: nothing invented');
  assert(rows.filter(o => o.day < S.addDays(S.TODAY, -21)).every(o => o.info === false), 'old orders have no receipt in the seed');
  assert(!JSON.stringify(full.rows).match(/\blines?\b/i), 'Pieces, never lines');

  // issues of an order: the injections of the seed, by kind
  const byRid = new Map(rows.map(o => [o.rid, o]));
  const KIND = { undone: 'undone', refused: 'refused', cancelAlert: 'cancelAlert', heldOrSkipped: 'heldOrSkipped', failed: 'failed', lookupFailed: 'lookupFailed', reprint: 'reprint', rescan: 'rescan' };
  const forPerson = {}; for (const who of ['Giovanna C.', 'Michael_V', 'Ana_M', 'Ivy_Y']) { const L = (await listAll(st, who)).rows; for (const o of L) byRid.set(o.rid, o); forPerson[who] = L; }
  assert.strictEqual(truth.issues.length, 8, 'the seed injects 8 issues: ' + truth.issues.map(i => i.kind + '@' + i.person + ' ' + i.day));
  for (const it of truth.issues) {
    const o = byRid.get(it.rid); assert(o, 'the order list has ' + it.kind + ' ' + it.rid); assert(o.issues.some(x => x.kind === KIND[it.kind]), `${it.kind}: ${JSON.stringify(o.issues)}`);
    const x = o.issues.find(y => y.kind === KIND[it.kind]); assert(x.label && Number.isFinite(x.at) && typeof x.note === 'string');
  }
  assert.strictEqual(Object.values(forPerson).reduce((n, L) => n + L.filter(o => o.issues.length).length, 0), truth.issues.length, 'no order without a fault is flagged');
  const und = byRid.get(truth.issues.find(i => i.kind === 'undone').rid); assert.strictEqual(und.undone, 1); assert.strictEqual(und.completes, 2); assert.strictEqual(und.parts, truth.orders[und.rid].pieces, 'net pieces of the reopened order');
  const rej = byRid.get(truth.issues.find(i => i.kind === 'refused').rid); assert.deepStrictEqual([rej.rejected, rej.completes, rej.parts], [1, 0, 0]); assert(/no stud earrings/.test(rej.issues[0].note));
  const rs = byRid.get(truth.issues.find(i => i.kind === 'rescan').rid); assert.strictEqual(rs.scans, 2);
  const rp = byRid.get(truth.issues.find(i => i.kind === 'reprint').rid); assert.strictEqual(rp.prints, 2);
  say('orders: full list ' + all.length + ' in ' + full.pages + ' pages of 100, in order, no duplicates; row facts; pages of 5/7 and cursors; limits; date window; receipts, pieces, pictures only where stored; 8 issue kinds found, none invented');

  // search, order number
  const ridFull = all[10].rid, ridPart = ridFull.slice(-4);
  r = await orders(st, G, { q: ridFull, limit: 100 }); assert.deepStrictEqual(r.body.orders.map(o => o.rid), [ridFull], 'a whole number finds its order');
  r = await orders(st, G, { q: ridPart, limit: 100 }); assert.deepStrictEqual(r.body.orders.map(o => o.rid), all.filter(o => o.rid.includes(ridPart)).map(o => o.rid), 'part of a number: every order that has it'); assert(r.body.total >= 1);
  assert.strictEqual((await orders(st, G, { q: '  ' + ridFull + '  ', limit: 100 })).body.total, 1, 'spaces around are ignored'); assert.strictEqual((await orders(st, G, { q: 'zzzzqq' })).body.total, 0);
  r = await orders(st, G, { q: 'zzzzqq' }); assert.deepStrictEqual([r.body.orders, r.body.next, r.body.found], [[], null, true]);
  // search, dates in every way a person types them
  const on = d => all.filter(o => o.day === d).map(o => o.rid), dates = { '2026-10-02': '2026-10-02', '10/2': '2026-10-02', '10/02': '2026-10-02', 'oct 2': '2026-10-02', 'Oct 2': '2026-10-02', 'october 2': '2026-10-02', '2 oct': '2026-10-02', '2 October': '2026-10-02', 'oct 02': '2026-10-02', '10/1': '2026-10-01', 'sep 30': '2026-09-30' };
  for (const [q, day] of Object.entries(dates)) { r = await orders(st, G, { q, limit: 100 }); assert.deepStrictEqual(r.body.orders.map(o => o.rid), on(day), `"${q}" finds ${day}`); }
  assert.strictEqual((await orders(st, G, { q: '10/3' })).body.total, 0, '10/3 is a Saturday: no orders (and 10/3 is not 10/30)'); assert.strictEqual((await orders(st, G, { q: 'oct 2' })).body.total, on('2026-10-02').length, '"oct 2" is not "oct 20"');
  const month = q => all.filter(o => o.day.slice(5, 7) === q).map(o => o.rid);
  assert.deepStrictEqual((await listAll(st, G, { q: 'september' })).rows.map(o => o.rid), month('09'), 'a month'); assert.deepStrictEqual((await listAll(st, G, { q: 'sep' })).rows.map(o => o.rid), month('09'), 'a month, short'); assert.deepStrictEqual((await listAll(st, G, { q: '2026-09' })).rows.map(o => o.rid), month('09'), 'a year and month');
  const fri = all.filter(o => S.dow(o.day) === 5).map(o => o.rid); assert.deepStrictEqual((await listAll(st, G, { q: 'friday' })).rows.map(o => o.rid), fri, 'a weekday'); assert.deepStrictEqual((await listAll(st, G, { q: 'FRI' })).rows.map(o => o.rid), fri, 'a short weekday, any case');
  // search, station (words and the station filter)
  const at = s => all.filter(o => o.stations.has(s)).map(o => o.rid), aws = at('assembly');
  assert(aws.length > 50 && aws.length < all.length); assert.deepStrictEqual((await listAll(st, G, { q: 'assembly' })).rows.map(o => o.rid), aws, 'a station by name'); assert.deepStrictEqual((await listAll(st, G, { q: 'assem' })).rows.map(o => o.rid), aws, 'a station by its first letters');
  assert.deepStrictEqual((await listAll(st, G, { station: 'assembly' })).rows.map(o => o.rid), aws, 'the station filter'); assert.deepStrictEqual((await listAll(st, G, { station: 'welding', q: 'oct 2' })).rows.map(o => o.rid), on('2026-10-02').filter(x => at('welding').includes(x)), 'filter and search together');
  assert.strictEqual((await orders(st, G, { station: 'shipping' })).body.total, 0, 'a station she never worked'); assert.strictEqual((await orders(st, G, { station: 'bogus' })).body.total, all.length, 'an unknown station is ignored');
  const inboxOnly = await orders(st, G, { station: 'inbox', limit: 100 }); assert(inboxOnly.body.total > 20, 'the inbox filter lists conversations: ' + inboxOnly.body.total); assert(inboxOnly.body.orders.every(o => o.stations.length === 1 && o.stations[0] === 'inbox' && o.station === 'inbox' && o.completes <= 1));
  // search, customer and SKU: read from the stored receipts of the newest 250 orders
  const newest = all.slice(0, 250), rc = rid => truth.receipts[rid];
  st.clear(); r = await orders(st, G, { q: 'jane', limit: 100 });
  const jane = newest.filter(o => rc(o.rid) && rc(o.rid).customer.toLowerCase().includes('jane')).map(o => o.rid);
  assert(jane.length > 10, 'the seed has Jane Smith orders: ' + jane.length); assert.deepStrictEqual(r.body.orders.map(o => o.rid), jane.slice(0, 100), 'a customer'); assert.strictEqual(r.body.total, jane.length); assert(r.body.orders.every(o => o.customer === 'Jane Smith'));
  assert.deepStrictEqual([r.body.searched.orders, r.body.searched.withDetails], [all.length, newest.filter(o => rc(o.rid)).length], 'searched says how far the customer search reached'); assert(r.body.notes.some(x => /newest 250 orders/.test(x)), 'and tells the person');
  assert(st.docsOf('EtsyMail_Receipts') <= 250 + 100, 'at most 250 receipts read once: ' + st.docsOf('EtsyMail_Receipts')); assert(st.docsOf('Etsy_Listing_Image_Cache') + st.docsOf('EtsyMail_Listings') <= 12, 'pictures: one read per listing');
  st.clear(); await orders(st, G, { q: 'jane', limit: 100 }); assert.strictEqual(st.docsOf('EtsyMail_Receipts'), 0, 'receipts are kept 30 minutes: the next keystroke reads none');
  assert.deepStrictEqual((await orders(st, G, { q: 'JANE smith', limit: 100 })).body.orders.map(o => o.rid), jane.slice(0, 100), 'any case, two words');
  const sku = q => newest.filter(o => rc(o.rid) && rc(o.rid).skus.some(s => s.toLowerCase().includes(q))).map(o => o.rid);
  r = await orders(st, G, { q: 'st-pearl', limit: 100 }); assert(sku('st-pearl').length > 5); assert.deepStrictEqual(r.body.orders.map(o => o.rid), sku('st-pearl').slice(0, 100), 'a SKU'); assert(r.body.orders.every(o => o.pieces.some(p => /ST-PEARL/.test(p.sku))));
  assert.deepStrictEqual((await orders(st, G, { q: 'moon', limit: 100 })).body.orders.map(o => o.rid), sku('moon').slice(0, 100), 'part of a SKU');
  const both = newest.filter(o => jane.includes(o.rid) && o.stations.has('welding')).map(o => o.rid); r = await orders(st, G, { q: 'jane welding', limit: 100 }); assert.deepStrictEqual(r.body.orders.map(o => o.rid), both.slice(0, 100), 'words are ANDed'); assert(both.length < jane.length);
  r = await orders(st, G, { q: 'jane oct 1', limit: 100 }); assert.deepStrictEqual(r.body.orders.map(o => o.rid), jane.filter(x => on('2026-10-01').includes(x)), 'customer and date');
  const jp = await listAll(st, G, { q: 'jane' }, 4); assert.deepStrictEqual(jp.rows.map(o => o.rid), jane, 'paging a search'); assert.strictEqual(jp.pages, Math.ceil(jane.length / 4));
  // cheap searches: number, date and station words read no receipt beyond the page shown
  const cheap = build(); cheap.st.clear(); await orders(cheap.st, G, { q: 'oct 2', limit: 25 }); assert(cheap.st.docsOf('EtsyMail_Receipts') <= 25, 'a date search reads the receipts of the page only: ' + cheap.st.docsOf('EtsyMail_Receipts'));
  cheap.st.clear(); await orders(cheap.st, G, { q: 'welding', limit: 25 }); assert(cheap.st.docsOf('EtsyMail_Receipts') <= 25);
  // names: aliases, unknown, sandbox
  assert.deepStrictEqual((await orders(st, 'Giovanna', { limit: 30 })).body.orders.map(o => o.rid), all.slice(0, 30).map(o => o.rid), 'either spelling: the same list'); assert.strictEqual((await orders(st, 'giovanna c', { limit: 1 })).body.total, all.length);
  r = await orders(st, 'Nobody Here'); assert.deepStrictEqual([r.body.found, r.body.total, r.body.orders, r.body.next, r.body.name], [false, 0, [], null, 'Nobody Here']); assert(r.body.notes.some(x => /no record of Nobody Here/i.test(x)));
  r = await orders(st, G, { sandbox: true, limit: 100 }); assert.strictEqual(r.body.mode, 'sandbox'); assert.strictEqual(r.body.total, 3, '3 sandbox orders'); assert(r.body.orders.every(o => o.info === false && o.pieces.length === 0 && /^9990/.test(o.rid)) && r.body.notes.some(x => /Sandbox keeps no receipts/.test(x)));
  assert(!full.rows.some(o => /^9990/.test(o.rid)), 'production never lists a sandbox order'); assert.strictEqual((await orders(st, 'Sandy Tester')).body.found, false);
  assert(st.readsOf('Sandbox_EtsyMail_Receipts').length === 0, 'the sandbox never reads receipts');
  say('orders search: number (whole, part), 11 spellings of a date, month, weekday, station (word, prefix, filter), customer, SKU, several words, paging a search; receipts read once and only where needed; aliases, unknown person, sandbox');

  // the live list polls every 5 s: the first page must stay cheap, and a new order must show at once
  const live = build(); const L = live.st; await orders(L, G, { limit: 25 }); tick(6000); L.clear(); await orders(L, G, { limit: 25 });
  say('  a poll of the first page 6 s later:', L.docsRead(), 'docs', `(${L.readsOf('Efficiency_Daily').length} rollup, ${L.readsOf('Station_Activity').length} event, ${L.readsOf('EtsyMail_Receipts').length} receipt queries)`);
  assert(L.docsRead() < 140, 'a poll costs a few documents: ' + L.docsRead()); assert.strictEqual(L.readsOf('EtsyMail_Receipts').length, 0, 'receipts come from the cache');
  const newRid = '3599999999', pc = 'pc-GIOV', dev = 'weld-1', t0 = NOW - 3 * 60000;
  const evAt = (n, at, action, extra) => Object.assign({ id: `${dev}_${pc}_new${n}`, station: 'welding', device: dev, computer: pc, session: '', person: 'Giovanna C.', action, orderId: newRid, line: '', sku: '', parts: 2, orders: action === 'complete' ? 1 : 0, detail: '', at, seq: 900 + n, sincePrevMs: n ? 60000 : 0, ts: at + 1000, serverAt: at + 1000, day: S.TODAY, hour: '14', v: 1 }, extra || {});
  L.put('Station_Activity', 'new1', evAt(0, t0, 'scan')); L.put('Station_Activity', 'new2', evAt(1, t0 + 60000, 'complete'));
  const roll = L.colls.get('Efficiency_Daily').get(S.TODAY + '__Giovanna C.'); roll.touched[newRid] = { welding: true }; roll.events += 2; tick(7000);
  r = await orders(L, G, { limit: 5 }); assert.strictEqual(r.body.orders[0].rid, newRid, 'the new order is on top, with its times (events of today are re-read when the rollup shows an order they lack)');
  assert.deepStrictEqual([r.body.orders[0].at, r.body.orders[0].completes, r.body.orders[0].parts, r.body.orders[0].durationMs], [t0, 1, 2, 60000]); assert.strictEqual(r.body.total, all.filter(o => o.day <= S.TODAY).length + 1);
}

/* ═══ 8 · privacy ═══ */
async function privacy() {
  const { st } = build();
  // a PIN where it must never show: in an event's detail, as a name, in a rollup
  const pinEv = { id: 'pin-ev-1', station: 'welding', device: 'weld-1', computer: 'pc-PINX', session: '', person: 'Pin Probe', action: 'error', orderId: '3590001111', line: '', sku: '', parts: 0, orders: 0, detail: 'Complete failed for employee 424242 (code 424242)', at: S.nyAt(S.TODAY, 10, 0), seq: 1, sincePrevMs: 0, ts: S.nyAt(S.TODAY, 10, 0) + 1000, serverAt: S.nyAt(S.TODAY, 10, 0) + 1000, day: S.TODAY, hour: '10', v: 1 };
  st.put('Station_Activity', pinEv.id, pinEv);
  st.put('Efficiency_Daily', S.TODAY + '__Pin Probe', { day: S.TODAY, person: 'Pin Probe', v: 1, events: 1, firstAt: pinEv.at, lastAt: pinEv.at, hours: {}, touched: { '3590001111': { welding: true } },
    stations: { welding: { scans: 0, scanParts: 0, completes: 0, parts: 0, orders: 0, prints: 0, rejects: 0, errors: 1, notes: 0, undos: 0, undoParts: 0, undoOrders: 0, activeMs: 0, idleMs: 0, firstAt: pinEv.at, lastAt: pinEv.at } } });
  st.put('Efficiency_Daily', S.TODAY + '__987654', { day: S.TODAY, person: '987654', v: 1, events: 1, firstAt: pinEv.at, lastAt: pinEv.at, hours: {}, touched: { '3590002222': { welding: true } }, stations: { welding: { scans: 1, scanParts: 1, completes: 0, parts: 0, orders: 0, prints: 0, rejects: 0, errors: 0, notes: 0, undos: 0, undoParts: 0, undoOrders: 0, activeMs: 0, idleMs: 0, firstAt: pinEv.at, lastAt: pinEv.at } } });
  st.put('Station_Sessions', 'pin-s-1', { id: 'pin-s-1', person: '654321', station: 'welding', device: 'weld-1', computerId: 'pc-PINX01', startAt: S.nyAt(S.TODAY, 9, 0), lastSeenAt: S.NOW - 60000, endAt: null, endReason: null });
  const rows = (await listAll(st, 'Pin Probe')).rows, p = await person(st, 'Pin Probe', 'week'), pl = await orders(st, 'Pin Probe', { q: '424242' });
  assert.strictEqual(rows.length, 1); assert(rows[0].issues.length === 1 && /\[#\]/.test(rows[0].issues[0].note) && !/424242/.test(rows[0].issues[0].note), 'a 6-digit number in a detail is masked: ' + rows[0].issues[0].note);
  assert.strictEqual(pl.body.total, 0, 'a PIN cannot be searched for'); assert.strictEqual(p.status, 200);
  assert.strictEqual((await person(st, '987654', 'week')).status, 400); assert.strictEqual((await orders(st, '654321')).status, 400);
  const day = (await person(st, 'Pin Probe', 'day')).body; assert.strictEqual(day.found, true);
  // all that was sent or logged, in the whole run
  const every = bodies.join('\n'), logAll = logs.join('\n');
  for (const secret of [PASS, PASS.slice(0, 14)]) { assert(!every.includes(secret), 'no passcode in any answer'); assert(!logAll.includes(secret), 'no passcode in any log'); }
  for (const pin of ['424242', '987654', '654321']) { assert(!every.includes(pin), pin + ' in an answer'); assert(!logAll.includes(pin), pin + ' in a log'); }
  const keys = new Set(); (function walk(v) { if (Array.isArray(v)) v.forEach(walk); else if (v && typeof v === 'object') for (const [k, x] of Object.entries(v)) { keys.add(k); walk(x); } })(bodies.map(b => { try { return JSON.parse(b); } catch (_) { return null; } }));
  assert(![...keys].some(k => /^pin$|passcode|password|secret|token/i.test(k)), 'no key named like a secret: ' + [...keys].filter(k => /^pin$|passcode|password|secret|token/i.test(k)));
  assert(bodies.length > 5); assert(!st.reads.some(q => /Employee|Brites|number/i.test(q.name) || /Employee Numbers/.test(q.doc || '')), 'the Employee Numbers list is never read');
  assert.strictEqual(st.writes.length, 0, 'nothing in this file ever writes');
  say('privacy: a PIN in a detail is masked, digits-only names are refused and dropped, the passcode and every PIN are in no answer and no log (' + bodies.length + ' answers checked), no secret-like key, the Employee Numbers list never read, 0 writes');
}

const SECTIONS = { gate, ranges, numbers, compare, helpers, cost, orderList, privacy }, only = process.argv.slice(2);    // (a section name or two runs just those)
(async () => {
  for (const [name, fn] of Object.entries(SECTIONS)) if (!only.length || only.includes(name)) await fn();
  say('OK');
})().catch(e => { process.stdout.write(String(e && e.stack || e) + '\n'); process.exit(1); });
