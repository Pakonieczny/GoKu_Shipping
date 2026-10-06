// Days worked, days off, short days, late starts, shifts and streaks (netlify/functions/_employeeAttendance.js, plans/employee-hr/api.md "E9").
// Offline: Firestore is an in-memory fake (typed fields, where/orderBy/limit, Timestamp), the clock is faked, every person is invented.
//   multi-month shop (2 Oct to 21 Dec 2026): weekends, a day nobody worked, a person absent three working days, a half day, a midnight
//   auto sign-out, days before logging began, alias and spelling merge, a sandbox that never mixes, empty data, no PIN in any answer,
//   a Saturday the team did work, a Saturday it did not, today (pending), reads (rollups + sessions only, cached), the optional schedule.
//   node tests/stations/employee-attendance.cjs
'use strict';
const path = require('path'), assert = require('assert'), Module = require('module');
const root = path.join(__dirname, '../..');
const M = require(path.join(root, 'netlify/functions/_employeeAttendance.js'));
const T = M._t;

/* ── fake Firestore (the same small shape the other efficiency tests use) ── */
class Ts { constructor(m) { this.m = m; } toMillis() { return this.m; } static fromMillis(m) { return new Ts(m); } }
const kind = v => v instanceof Ts ? 'ts' : typeof v === 'number' ? 'num' : typeof v === 'string' ? 'str' : 'other';
const val = v => v instanceof Ts ? v.m : v;
function fakeStore() {
  const colls = new Map(), reads = [], failing = new Set();
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
        reads.push({ name, n: docs.length, filters: filters.map(f => f[0] + f[1]) });
        return { docs: docs.map(({ id, d }) => ({ id, data: () => keep(d) })), size: docs.length, empty: !docs.length };
      }
    };
  }
  const db = { collection: name => Object.assign(query(name, [], null, null), {
    doc: id => ({ id,
      get: async () => { reads.push({ name, doc: id }); if (failing.has(name)) throw new Error('14 UNAVAILABLE'); const d = data(name).get(id); return { exists: !!d, data: () => keep(d) }; } }) }) };
  return { db, put: (name, id, d) => data(name).set(id, keep(d)), reads, fail: n => failing.add(n), heal: n => failing.delete(n),
    readsOf: name => reads.filter(r => r.name === name), docs: name => [...data(name).values()].map(keep) };
}
const admin = { firestore: { Timestamp: Ts } };
/* the real console function over the fake admin: the `person` op calls attendance() with its own prepared context (section 15) */
const fakeAdmin = { firestore: Object.assign(() => ({}), { Timestamp: Ts, FieldValue: { serverTimestamp: () => 'ts' } }) };
const realLoad = Module._load;
Module._load = function (req, ...rest) { if (/[\/]firebaseAdmin(\.js)?$/.test(req)) return fakeAdmin; return realLoad.call(this, req, ...rest); };
const EFF = require(path.join(root, 'netlify/functions/employeeEfficiency.js'));
const EP = require(path.join(root, 'netlify/functions/_editPasscode.js'));
Module._load = realLoad;
const PASS = 'synthetic-pass-9f3k';
process.env.EDIT_PASSCODE = PASS;

const say = (...a) => process.stdout.write(a.join(' ') + '\n');
const realNow = Date.now;
const at = (day, hm) => { const [h, m] = hm.split(':').map(Number); return T.nyMidnight(day) + (h * 60 + m) * 60000; };
const TODAY = '2026-12-21';                                        // a Monday
let NOW = at(TODAY, '10:00');
Date.now = () => NOW;
const HOUR = 3600000, MIN = 60000;

/* ── builders ── */
const PIN = '482913';                                              // a made-up PIN: it must never appear in any answer
let seq = 0;
const sess = (person, station, start, o = {}) => ({ id: 's' + (++seq), person, employeeId: PIN, station, device: station + '-1', computerId: 'pc-' + String(seq).padStart(8, 'X'), computerLabel: '',
  startAt: start, lastSeenAt: o.last == null ? (o.end == null ? start : o.end) : o.last, endAt: o.end == null ? null : o.end, endReason: o.reason || null, minutes: 0 });
const roll = (day, person, first, last, o = {}) => ({ day, person, v: 1, events: o.events == null ? 12 : o.events, firstAt: first, lastAt: last,
  stations: { welding: { scans: 10, scanParts: 10, completes: 4, parts: o.parts == null ? 40 : o.parts, orders: 3, prints: 1, rejects: 0, errors: 0, notes: 0, undos: 1, undoParts: o.undo == null ? 4 : o.undo, undoOrders: 0, activeMs: 3 * HOUR, idleMs: HOUR, firstAt: first, lastAt: last } },
  touched: { [day.replace(/-/g, '') + '1']: { welding: true }, [day.replace(/-/g, '') + '2']: { welding: true }, [day.replace(/-/g, '') + '3']: { welding: true } } });
const weekday = d => T.weekdayOf(d), isWeekday = d => weekday(d) >= 1 && weekday(d) <= 5;
const days = (a, b) => { const o = []; for (let d = a; d <= b; d = T.addDays(d, 1)) o.push(d); return o; };

function shop(st, prefix = '') {
  const S = (id, d) => st.put(prefix + 'Station_Sessions', id, d), R = (day, person, d) => st.put(prefix + 'Efficiency_Daily', day + '__' + person, Object.assign(d, prefix ? { sandbox: true } : {}));
  const regular = (day, person, o = {}) => {
    S('s-' + day + '-' + person, sess(person, 'welding', at(day, o.in || '08:00'), { end: at(day, o.out || '16:30'), reason: 'signOut' }));
    R(day, person, roll(day, person, at(day, '08:05'), at(day, '16:20')));
  };
  return { S, R, regular };
}
const TEAM = ['Tess Welder', 'Raj Packer', 'Mia Sorter', 'Leo Assembler', 'Zoe Design'];
const OFF = new Set(['Tess Welder|2026-10-14', 'Tess Welder|2026-10-15', 'Tess Welder|2026-11-04', 'Zoe Design|2026-12-05']);
const SPECIAL = new Set(['Tess Welder|2026-10-21', 'Tess Welder|2026-10-22', 'Tess Welder|2026-11-09', 'Tess Welder|2026-11-10', 'Tess Welder|2026-11-17', 'Tess Welder|2026-12-03', 'Tess Welder|2026-12-04', 'Tess Welder|2026-12-14']);
const HOLIDAY = '2026-11-11', NEWT = 'Newt Hire', NEWT_FIRST = '2026-11-02';

function realShop() {
  const st = fakeStore(), { S, R, regular } = shop(st);
  for (const d of days('2026-10-02', '2026-12-18')) {
    if (d === HOLIDAY) continue;
    const sat = weekday(d) === 6;
    if (!isWeekday(d) && !(sat && d === '2026-12-05') && !(sat && d === '2026-10-17')) continue;
    for (const p of TEAM) {
      if (OFF.has(p + '|' + d) || SPECIAL.has(p + '|' + d)) continue;
      if (d === '2026-10-17' && p !== 'Mia Sorter') continue;                    // a quiet Saturday: only Mia (and Paul K at the inbox)
      if (d === '2026-12-05' && p === 'Zoe Design') continue;
      regular(d, p);
    }
    if (d >= NEWT_FIRST && isWeekday(d)) regular(d, NEWT);
  }
  S('paul-sat', sess('Paul K', 'inbox', at('2026-10-17', '10:00'), { end: at('2026-10-17', '14:00'), reason: 'signOut' }));
  // Tess: the special days
  S('t-half', sess('Tess Welder', 'welding', at('2026-10-21', '08:00'), { end: at('2026-10-21', '11:30'), reason: 'signOut' }));                  // a half day: 3.5 h
  R('2026-10-21', 'Tess Welder', roll('2026-10-21', 'Tess Welder', at('2026-10-21', '08:05'), at('2026-10-21', '11:20')));
  S('t-mid', sess('Tess Welder', 'welding', at('2026-10-22', '08:05'), { end: at('2026-10-23', '00:00'), reason: 'midnight' }));                   // signed out by the clock at midnight
  R('2026-10-22', 'Tess Welder', roll('2026-10-22', 'Tess Welder', at('2026-10-22', '08:10'), at('2026-10-22', '16:20')));
  S('t-alias', sess('Tess', 'inbox', at('2026-11-09', '08:00'), { end: at('2026-11-09', '16:30'), reason: 'signOut' }));                            // another spelling (alias)
  R('2026-11-09', 'Tess_Welder', roll('2026-11-09', 'Tess_Welder', at('2026-11-09', '08:05'), at('2026-11-09', '16:20')));                         // underscore spelling
  S('t-late', sess('Tess Welder', 'welding', at('2026-11-10', '10:15'), { end: at('2026-11-10', '16:30'), reason: 'signOut' }));                  // late
  R('2026-11-10', 'Tess Welder', roll('2026-11-10', 'Tess Welder', at('2026-11-10', '10:20'), at('2026-11-10', '16:20')));
  S('t-caps', sess('TESS  WELDER', 'welding', at('2026-11-17', '08:00'), { end: at('2026-11-17', '16:30'), reason: 'signOut' }));                  // case and spacing
  R('2026-11-17', 'TESS  WELDER', roll('2026-11-17', 'TESS  WELDER', at('2026-11-17', '08:05'), at('2026-11-17', '16:20')));
  R('2026-12-03', 'Tess Welder', roll('2026-12-03', 'Tess Welder', at('2026-12-03', '08:10'), at('2026-12-03', '15:55')));                         // work recorded, no sign-in at all
  S('t-lost', sess('Tess Welder', 'welding', at('2026-12-04', '08:10'), { end: at('2026-12-05', '00:00'), reason: 'midnight' }));                  // midnight sign-out, nothing recorded
  S('t-blip', sess('Tess Welder', 'welding', at('2026-12-14', '08:00'), { end: at('2026-12-14', '08:10'), reason: 'signOut' }));                  // ten minutes, then gone
  // before logging began: ignored (these days are 'unknown')
  S('old-1', sess('Raj Packer', 'welding', at('2026-09-30', '08:00'), { end: at('2026-09-30', '16:00'), reason: 'signOut' }));
  S('old-2', sess('Tess Welder', 'welding', at('2026-09-30', '08:00'), { end: at('2026-09-30', '16:00'), reason: 'signOut' }));
  // names made only of digits are never people (they would be PINs)
  S('pin-a', sess(PIN, 'welding', at('2026-12-13', '09:00'), { end: at('2026-12-13', '15:00'), reason: 'signOut' }));
  S('pin-b', sess('913482', 'welding', at('2026-12-13', '09:00'), { end: at('2026-12-13', '15:00'), reason: 'signOut' }));
  R('2026-12-13', PIN, roll('2026-12-13', PIN, at('2026-12-13', '09:05'), at('2026-12-13', '14:00')));
  // a sandbox-marked rollup that landed in the real collection never counts (it would make 14 Oct a day worked)
  st.put('Efficiency_Daily', '2026-10-14__Tess Welder', Object.assign(roll('2026-10-14', 'Tess Welder', at('2026-10-14', '08:05'), at('2026-10-14', '16:20')), { sandbox: true }));
  // today: the others are in (open sessions with a fresh beat), Tess is not
  for (const p of TEAM.concat([NEWT])) if (p !== 'Tess Welder') {
    S('live-' + p, sess(p, 'welding', at(TODAY, '08:00'), { last: NOW - MIN }));
    R(TODAY, p, roll(TODAY, p, at(TODAY, '08:05'), NOW - 2 * MIN, { parts: 12, undo: 0 }));
  }
  // the sandbox: a small rehearsal that must never mix with the real shop
  const sb = shop(st, 'Sandbox_');
  for (const d of ['2026-12-07', '2026-12-08']) { sb.regular(d, 'Tess Welder'); sb.regular(d, 'Mia Sorter'); }
  sb.S('sb-sat', sess('Tess Welder', 'welding', at('2026-12-12', '09:00'), { end: at('2026-12-12', '13:00'), reason: 'signOut' }));
  return st;
}
const ctxOf = (st, o = {}) => Object.assign({ db: st.db, admin, now: NOW, today: TODAY }, o);
const by = (r, d) => r.calendar.find(c => c.day === d);
const att = (st, name, from, to, o) => M.attendance(ctxOf(st, o), { name, from, to });

(async () => {
  const st = realShop();
  const FROM = '2026-09-28';

  /* ── 0 · the clock: New York days, daylight saving, week days ── */
  const Z = iso => Date.parse(iso);
  assert.strictEqual(T.nyDay(Z('2026-10-03T03:59:59Z')), '2026-10-02'); assert.strictEqual(T.nyDay(Z('2026-10-03T04:00:00Z')), '2026-10-03', 'EDT midnight is 04:00 UTC');
  assert.strictEqual(T.nyDay(Z('2026-11-02T04:59:59Z')), '2026-11-01'); assert.strictEqual(T.nyDay(Z('2026-11-02T05:00:00Z')), '2026-11-02', 'EST midnight is 05:00 UTC');
  assert.strictEqual(T.nyMidnight('2026-11-02') - T.nyMidnight('2026-11-01'), 25 * HOUR, 'the fall-back day is 25 hours'); assert.strictEqual(T.nyMidnight('2027-03-15') - T.nyMidnight('2027-03-14'), 23 * HOUR, 'the spring day is 23');
  assert.strictEqual(T.nyMinutes(Z('2026-10-20T12:30:00Z')), 8 * 60 + 30, '08:30 EDT'); assert.strictEqual(T.nyMinutes(Z('2026-12-10T13:30:00Z')), 8 * 60 + 30, '08:30 EST');
  assert.strictEqual(T.nyMinutes(Z('2026-11-01T14:00:00Z')), 9 * 60, 'on the 25-hour day the clock says 09:00 (not ten hours after midnight)');
  assert.strictEqual(T.nyMinutes(Z('2027-03-14T14:00:00Z')), 10 * 60, 'and on the 23-hour day 10:00');
  assert.strictEqual(T.weekdayOf('2026-10-02'), 5, 'a Friday'); assert.strictEqual(T.weekdayOf('2026-10-04'), 0, 'a Sunday'); assert.strictEqual(T.weekdayOf('2026-10-05'), 1);
  assert.strictEqual(T.addDays('2028-02-28', 1), '2028-02-29'); assert.strictEqual(T.addDays('2026-12-31', 1), '2027-01-01'); assert.strictEqual(T.addDays('2026-03-01', -1), '2026-02-28');
  say('clock: New York midnights (EDT and EST), 23 and 25 hour days, minutes on the clock across the change, week days, month and year ends');

  /* ── 1 · the shape ── */
  const r = await att(st, 'Tess Welder', FROM, TODAY);
  assert.strictEqual(r.ok, true); assert.strictEqual(r.mode, 'real'); assert.strictEqual(r.found, true); assert.strictEqual(r.name, 'Tess Welder');
  assert.strictEqual(r.from, FROM); assert.strictEqual(r.to, TODAY); assert.strictEqual(r.today, TODAY);
  assert.strictEqual(r.calendar.length, 85, 'one entry per day, 28 Sep to 21 Dec');
  for (const k of ['workingDays', 'daysWorked', 'daysOff', 'extraDays', 'lateDays', 'shortDays', 'avgShiftMs', 'medianStart', 'medianEnd', 'streaks', 'byWeekday', 'definitions', 'estimated', 'rules', 'states', 'notes'])
    assert(k in r, 'has ' + k);
  for (const c of r.calendar) for (const k of ['day', 'state', 'signedMs', 'firstIn', 'lastOut', 'parts', 'orders', 'others']) assert(k in c, c.day + ' has ' + k);
  assert(r.calendar.every(c => ['late', 'short', 'extra', 'estimated'].every(k => c[k] === undefined || c[k] === true) && (c.lengthKnown === undefined || c.lengthKnown === false)), 'flags are present only when true');
  assert(r.calendar.filter(c => c.late).length === 1 && r.calendar.filter(c => c.short).length === 2, 'the flags that are set');
  assert(r.calendar.every((c, i) => i === 0 || c.day > r.calendar[i - 1].day), 'oldest first');
  assert(r.calendar.every(c => ['worked', 'partial', 'off', 'closed', 'future', 'pending', 'before', 'unknown'].includes(c.state)), 'only the documented states');
  assert.strictEqual(r.states.length, 8); assert(r.states.every(s => s.state && s.label && s.def));
  for (const [k, v] of Object.entries(M.DEFINITIONS)) assert(typeof v === 'string' && v.length > 20 && v.length < 420, 'definition ' + k);
  for (const k of ['workingDay', 'daysOff', 'daysWorked', 'shortDays', 'lateDays', 'avgShiftMs', 'medianStart', 'medianEnd', 'streaks', 'byWeekday', 'estimated', 'notAttendance']) assert(r.definitions[k], 'defines ' + k);
  assert(/logged sign-ins and logged work, not from a time clock/.test(r.definitions.notAttendance) && /phone scan is credited to the desktop/.test(r.definitions.notAttendance) && /work without signing in/.test(r.definitions.notAttendance), 'the honest sentence');
  assert(!/\blines?\b/i.test(JSON.stringify(r.definitions) + JSON.stringify(r.states)), "'Pieces', never 'lines'");
  say('shape: 85 calendar days, every field, 8 documented states, a plain definition for each field, the honest sentence');

  /* ── 2 · days before logging began, weekends, a day nobody worked ── */
  for (const d of ['2026-09-28', '2026-09-29', '2026-09-30', '2026-10-01']) { const c = by(r, d); assert.strictEqual(c.state, 'unknown', d); assert(/before sign-in logging began/i.test(c.note), 'a note'); assert.strictEqual(c.signedMs, 0); }
  assert.strictEqual(r.trackingStart, '2026-10-02');
  assert.strictEqual(by(r, '2026-10-02').state, 'worked', 'the first logged day counts');
  assert(!r.calendar.some(c => c.state === 'off' && c.day < '2026-10-02'), 'never a day off before logging');
  for (const d of ['2026-10-03', '2026-10-04', '2026-10-10', '2026-12-12', '2026-12-13']) assert.strictEqual(by(r, d).state, 'closed', 'weekend ' + d + ' (even 13 Dec, where only digit-only names were "in")');
  assert.strictEqual(by(r, HOLIDAY).state, 'closed', 'a weekday nobody worked is closed, not a day off');
  assert.strictEqual(by(r, HOLIDAY).others, 0);
  assert.strictEqual((await att(st, 'Raj Packer', FROM, TODAY)).calendar.find(c => c.day === HOLIDAY).state, 'closed', 'closed for everybody');
  // 17 Oct: only Mia and Paul K came on a Saturday: two people in, but the team does not usually work Saturdays, so it is not a working day
  assert.strictEqual(by(r, '2026-10-17').state, 'closed', 'a quiet Saturday is closed for the people who were not in');
  const mia = await att(st, 'Mia Sorter', FROM, TODAY);
  assert.strictEqual(by(mia, '2026-10-17').state, 'worked'); assert.strictEqual(by(mia, '2026-10-17').extra, true, 'Mia worked a day the team did not');
  assert.strictEqual(mia.extraDays, 2, '17 Oct and 5 Dec'); assert.strictEqual(mia.workingDays, mia.daysWorked + mia.daysOff, 'extra days are not working days');
  // 5 Dec: four of six came on a Saturday. The team does not usually work Saturdays, so it is not a working day: those who came have an extra day, nobody is off.
  assert.strictEqual(by(r, '2026-12-05').state, 'worked'); assert.strictEqual(by(r, '2026-12-05').extra, true, 'an extra Saturday');
  const zoe = await att(st, 'Zoe Design', FROM, TODAY);
  assert.strictEqual(by(zoe, '2026-12-05').state, 'closed', 'Zoe did not come on a Saturday the team does not usually work: not a day off'); assert.strictEqual(zoe.daysOff, 0); assert.strictEqual(zoe.extraDays, 0);
  assert.deepStrictEqual(r.rules.usualWeekdays, [1, 2, 3, 4, 5], 'the usual working weekdays are learned: Monday to Friday');
  say('unknown before 2 Oct (never off), weekends and a nobody-worked weekday closed, Saturdays are extra days (Mia, Tess), never days off (Zoe)');

  /* ── 3 · absent three working days ── */
  for (const d of ['2026-10-14', '2026-10-15', '2026-11-04']) {
    const c = by(r, d); assert.strictEqual(c.state, 'off', d); assert.strictEqual(c.signedMs, 0); assert.strictEqual(c.firstIn, null); assert.strictEqual(c.parts, null); assert(c.others >= 4, 'others were in');
  }
  assert.strictEqual(by(r, '2026-10-14').state, 'off', 'a sandbox-marked rollup in the real collection did not make 14 Oct a day worked');
  assert.strictEqual(r.daysOff, 3);
  const workingList = days('2026-10-02', '2026-12-18').filter(d => (isWeekday(d) && d !== HOLIDAY) || d === '2026-12-05');
  assert.strictEqual(workingList.length, 56, '55 weekdays and the extra Saturday');
  assert.strictEqual(r.workingDays, 55); assert.strictEqual(r.daysWorked, 52); assert.strictEqual(r.workingDays, r.daysWorked + r.daysOff, 'working = worked + off');
  assert.strictEqual(r.extraDays, 1, '5 Dec'); assert.strictEqual(r.attendanceRate, Math.round(1000 * 52 / 55) / 10);
  say('three days off found (14 Oct, 15 Oct, 4 Nov), 55 working days = 52 worked + 3 off, plus 1 extra Saturday');

  /* ── 4 · half day, late, short ── */
  const half = by(r, '2026-10-21');
  assert.strictEqual(half.state, 'partial'); assert.strictEqual(half.short, true); assert.strictEqual(half.signedMs, 3.5 * HOUR);
  assert.strictEqual(r.rules.usualShiftMs, 8.5 * HOUR, 'her own usual shift is 8.5 hours'); assert.strictEqual(r.rules.shortBelowMs, 4.25 * HOUR, 'short = under half of it');
  assert.strictEqual(r.rules.shortBasis, 'own median shift');
  const blip = by(r, '2026-12-14');
  assert.strictEqual(blip.state, 'partial', 'ten minutes signed in is a short day, not a day off'); assert.strictEqual(blip.signedMs, 10 * MIN);
  assert.strictEqual(r.shortDays, 2); assert.strictEqual(r.shortDays, r.calendar.filter(c => c.state === 'partial').length);
  assert.strictEqual(by(r, '2026-11-10').late, true, 'first in at 10:15, usual start 08:00'); assert.strictEqual(by(r, '2026-11-10').state, 'worked');
  assert.strictEqual(r.rules.usualStart, 8 * 60); assert.strictEqual(r.rules.usualStartText, '8:00 AM');
  assert.strictEqual(r.lateDays, 1); assert.strictEqual(r.calendar.filter(c => c.late).length, 1);
  assert(!by(r, '2026-10-22').late, '08:05 is not late');
  say('half day (3.5 h of a usual 8.5 h) and a ten-minute sign-in are short; 10:15 against a usual 08:00 is late; 08:05 is not');

  /* ── 5 · the midnight auto sign-out, estimates ── */
  const mid = by(r, '2026-10-22');
  assert.strictEqual(mid.endedBy, 'midnight'); assert.strictEqual(mid.estimated, true); assert(mid.lengthKnown !== false);
  assert.strictEqual(mid.lastOut, at('2026-10-22', '16:20'), 'cut back to her last recorded action, not midnight');
  assert.strictEqual(mid.signedMs, at('2026-10-22', '16:20') - at('2026-10-22', '08:05'), '8 h 15 m, not a 16 hour shift');
  assert.strictEqual(mid.state, 'worked');
  const rec = by(r, '2026-12-03');
  assert.strictEqual(rec.state, 'worked'); assert.strictEqual(rec.estimated, true); assert.strictEqual(rec.endedBy, 'activity');
  assert.strictEqual(rec.signedMs, at('2026-12-03', '15:55') - at('2026-12-03', '08:10'), 'no sign-in: the span of the recorded work'); assert.strictEqual(rec.firstIn, at('2026-12-03', '08:10'));
  const lost = by(r, '2026-12-04');
  assert.strictEqual(lost.state, 'worked', 'signed in, nothing recorded after: worked, never short'); assert.strictEqual(lost.lengthKnown, false);
  assert.strictEqual(lost.signedMs, null); assert.strictEqual(lost.lastOut, null); assert.strictEqual(lost.estimated, true); assert(!lost.short); assert(!lost.late);
  assert(r.estimated.any && r.estimated.days.includes('2026-10-22') && r.estimated.days.includes('2026-12-03') && r.estimated.days.includes('2026-12-04'));
  for (const f of ['avgShiftMs', 'medianShiftMs', 'medianEnd', 'shortDays']) { assert.strictEqual(r.estimated.fields[f].estimated, true, f + ' depends on estimates'); assert(r.estimated.fields[f].why.length > 20); }
  for (const f of ['daysOff', 'workingDays', 'daysWorked']) assert.strictEqual(r.estimated.fields[f].estimated, false, f + ' does not');
  assert(r.notes.some(n => /midnight/.test(n)) && r.notes.some(n => /length is unknown/.test(n)) && r.notes.some(n => /not tracked/.test(n)));
  // averages leave out the unknown-length day; the midnight day is in at 8 h 15 m, so the average is near a normal shift
  assert(r.avgShiftMs > 7 * HOUR && r.avgShiftMs < 8.6 * HOUR, 'average shift is a plausible number of hours (' + r.avgShiftMs / HOUR + ')');
  assert.strictEqual(r.medianShiftMs, 8.5 * HOUR);
  assert.strictEqual(r.medianStart, 8 * 60); assert.strictEqual(r.medianStartText, '8:00 AM');
  assert.strictEqual(r.medianEnd, 16 * 60 + 30); assert.strictEqual(r.medianEndText, '4:30 PM');
  say('midnight sign-out cut to the last action (8 h 15 m), activity-only day estimated, nothing-recorded day is worked with unknown length, averages and medians sensible');

  /* ── 6 · aliases and spellings ── */
  const stA = realShop(); stA.put('config', 'employeeAliases', { 'Tess Welder': ['Tess'] });
  const ra = await att(stA, 'Tess Welder', FROM, TODAY);
  const al = by(ra, '2026-11-09');
  assert.strictEqual(al.state, 'worked', 'sessions as "Tess" and a rollup as "Tess_Welder" are one person'); assert.strictEqual(al.signedMs, 8.5 * HOUR); assert.strictEqual(al.parts, 36, 'pieces net of undo'); assert.strictEqual(al.orders, 3);
  assert.strictEqual(by(ra, '2026-11-17').state, 'worked', 'case and spacing ("TESS  WELDER")'); assert.strictEqual(ra.daysOff, 3);
  assert.strictEqual((await att(stA, 'Tess', FROM, TODAY)).daysOff, 3, 'asking by the alias gives the same person');
  assert.strictEqual((await att(stA, 'tess_welder', FROM, TODAY)).daysWorked, 52, 'asking by another spelling too');
  const noAlias = await att(realShop(), 'Tess Welder', FROM, TODAY);
  assert.strictEqual(by(noAlias, '2026-11-09').state, 'worked', 'the underscore spelling still merges without any alias');
  assert.strictEqual(by(noAlias, '2026-11-09').signedMs, at('2026-11-09', '16:20') - at('2026-11-09', '08:05'), 'but the "Tess" sessions are another person without the alias, so only her recorded work shows');
  assert.strictEqual(by(noAlias, '2026-11-09').estimated, true);
  const cust = await M.attendance({ rollups: realShop().docs('Efficiency_Daily'), sessions: realShop().docs('Station_Sessions'), rowsFrom: '2026-10-02', now: NOW, today: TODAY, keyOf: n => n.split(/[ _]/)[0].toLowerCase() }, { name: 'Tess', from: FROM, to: TODAY });
  assert.strictEqual(cust.found, true, 'a caller-supplied key function decides who is one person');
  say('alias doc, underscore and capitals merge; any spelling asks for the same person; a supplied key function works');

  /* ── 7 · people who started later ── */
  const newt = await att(st, NEWT, FROM, TODAY);
  assert.strictEqual(newt.firstDay, NEWT_FIRST);
  for (const d of ['2026-10-05', '2026-10-14', '2026-10-30']) assert.strictEqual(by(newt, d).state, 'before', d + ' is before their first day, not a day off');
  assert.strictEqual(by(newt, '2026-10-10').state, 'closed'); assert.strictEqual(newt.daysOff, 0); assert.strictEqual(newt.workingDays, newt.daysWorked);
  say('a new starter has no days off before their first day');

  /* ── 8 · today, the future, streaks, weekdays ── */
  assert.strictEqual(by(r, TODAY).state, 'pending', 'today, not in yet: never a day off'); assert.strictEqual(by(r, TODAY).others, 5);
  const ahead = await att(st, 'Tess Welder', '2026-12-17', '2026-12-25');
  assert.deepStrictEqual(ahead.calendar.map(c => c.state), ['worked', 'worked', 'closed', 'closed', 'pending', 'future', 'future', 'future', 'future'], '17 to 25 Dec: weekends closed, today pending, then the future');
  const inToday = await M.attendance(ctxOf(st, { now: at(TODAY, '10:00') }), { name: 'Raj Packer', from: TODAY, to: TODAY });
  assert.strictEqual(inToday.calendar[0].state, 'worked', 'in today'); assert.strictEqual(inToday.calendar[0].signedMs, 2 * HOUR); assert.strictEqual(inToday.calendar[0].lastOut, null, 'still in: no last out'); assert.strictEqual(inToday.calendar[0].endedBy, 'open');
  assert.strictEqual(inToday.workingDays, 1); assert.strictEqual(inToday.daysWorked, 1); assert.strictEqual(inToday.avgShiftMs, null, 'a day still running is left out of the shift average');
  assert.strictEqual(inToday.medianStart, 8 * 60); assert.strictEqual(inToday.medianEnd, null);
  // streaks, worked out here from the list of days Tess was in
  const seq1 = workingList.map(d => ['2026-10-14', '2026-10-15', '2026-11-04'].includes(d) ? 0 : 1);
  let cur = 0, best = 0; for (const x of seq1) { cur = x ? cur + 1 : 0; best = Math.max(best, cur); }
  assert.deepStrictEqual([r.streaks.current, r.streaks.best], [cur, best]); assert(cur > 30 && best === cur, 'her current run since 4 Nov is her best (' + cur + ')');
  // a one-day view still knows the streak (the 90 days before are read to learn it)
  const one = await att(st, 'Tess Welder', '2026-12-18', '2026-12-18');
  assert.strictEqual(one.calendar.length, 1); assert.strictEqual(one.streaks.current, cur); assert(one.rules.usualShiftMs === 8.5 * HOUR, 'usual shift learned from the days before');
  assert.strictEqual(one.workingDays, 1);
  const wk = await att(st, 'Tess Welder', '2026-10-12', '2026-10-18');
  assert.strictEqual(wk.byWeekday.length, 7); assert.deepStrictEqual(wk.byWeekday.map(w => w.label), ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']);
  assert.strictEqual(wk.byWeekday[2].daysOff, 1, 'Wednesday 14 Oct'); assert.strictEqual(wk.byWeekday[3].daysOff, 1, 'Thursday 15 Oct'); assert.strictEqual(wk.byWeekday[0].daysWorked, 1);
  assert.strictEqual(wk.byWeekday[5].workingDays, 0, 'the quiet Saturday is not a working day'); assert.strictEqual(wk.workingDays, 5); assert.strictEqual(wk.daysWorked, 3);
  const wholeWd = r.byWeekday; assert.strictEqual(wholeWd[2].daysOff, 2, 'Wednesdays: 14 Oct and 4 Nov'); assert.strictEqual(wholeWd[3].daysOff, 1); assert.strictEqual(wholeWd[5].workingDays, 0, 'Saturday is not a working day'); assert.strictEqual(wholeWd[5].extraDays, 1, 'but she worked one'); assert.strictEqual(wholeWd[6].workingDays, 0);
  assert.strictEqual(wholeWd.reduce((n, w) => n + w.workingDays, 0), r.workingDays); assert.strictEqual(wholeWd.reduce((n, w) => n + w.daysOff, 0), r.daysOff);
  say('today pending, the future, still-in today, streaks (' + cur + ' current = best), a one-day view keeps the streak and the learned shift, byWeekday adds up');

  /* ── 9 · the schedule (optional) ── */
  const sched = await att(st, 'Tess Welder', FROM, TODAY, { schedule: { closedDays: ['2026-10-14', '2026-10-13'], openDays: ['2026-10-24'], closedWeekdays: [] } });
  assert.strictEqual(by(sched, '2026-10-14').state, 'closed', 'a configured closed day is not a day off'); assert.strictEqual(by(sched, '2026-10-13').state, 'worked'); assert.strictEqual(by(sched, '2026-10-13').extra, true, 'worked a configured closed day: extra');
  assert.strictEqual(by(sched, '2026-10-24').state, 'off', 'a configured open day that nobody worked is a day off for the person who was not in');
  assert.strictEqual(sched.daysOff, 3, '3 - 14 Oct + 24 Oct'); assert.strictEqual(sched.rules.schedule, true);
  const stS = realShop(); stS.put('config', 'employeeSchedule', { closedDays: ['2026-11-04'], closedWeekdays: [0, 6] });
  const rs = await att(stS, 'Tess Welder', FROM, TODAY);
  assert.strictEqual(by(rs, '2026-11-04').state, 'closed'); assert.strictEqual(rs.daysOff, 2);
  assert.strictEqual(by(rs, '2026-12-05').state, 'worked'); assert.strictEqual(by(rs, '2026-12-05').extra, true, 'a closed weekday by config: working it is extra');
  const stW = realShop(); stW.put('config', 'employeeSchedule', { closedWeekdays: [0, 3] });
  assert.strictEqual(by(await att(stW, 'Tess Welder', FROM, TODAY), '2026-10-14').state, 'closed', 'Wednesday closed by config: her Wednesday absence is not a day off');
  assert.strictEqual((await att(stS, 'Zoe Design', FROM, TODAY)).daysOff, 0, 'and Zoe is not off on a configured closed Saturday');
  say('schedule: closed days, open days and closed weekdays from ctx or config/employeeSchedule');

  /* ── 10 · sandbox never mixes ── */
  const sbr = await att(st, 'Tess Welder', '2026-12-01', TODAY, { mode: 'sandbox' });
  assert.strictEqual(sbr.mode, 'sandbox'); assert.strictEqual(sbr.workingDays, 2, 'sandbox: only the two rehearsal days'); assert.strictEqual(sbr.daysWorked, 2); assert.strictEqual(sbr.daysOff, 0);
  assert.strictEqual(by(sbr, '2026-12-12').extra, true, 'the lone sandbox Saturday'); assert.strictEqual(sbr.extraDays, 1);
  assert.strictEqual(by(sbr, '2026-12-03').state, 'closed', 'the real 3 Dec work is not in the sandbox');
  const real2 = await att(st, 'Tess Welder', '2026-12-01', TODAY);
  assert.strictEqual(real2.mode, 'real'); assert.strictEqual(by(real2, '2026-12-12').state, 'closed', 'the sandbox Saturday is not in the real answer'); assert.strictEqual(real2.extraDays, 1, 'only the real 5 Dec');
  assert.strictEqual((await att(st, 'Tess Welder', '2026-12-01', TODAY, { prefix: 'Sandbox_' })).mode, 'sandbox', 'prefix works as mode');
  assert.strictEqual((await att(st, 'Tess Welder', '2026-12-01', TODAY, { sandbox: true })).mode, 'sandbox', 'sandbox:true works as mode');
  assert.strictEqual((await att(st, 'Raj Packer', '2026-12-01', TODAY, { mode: 'sandbox' })).found, false, 'Raj is not in the sandbox');
  say('sandbox: only sandbox rows, real unaffected, by mode, prefix or sandbox flag');

  /* ── 11 · empty and failing data, bad requests ── */
  const empty = fakeStore();
  const e0 = await att(empty, 'Tess Welder', '2026-09-30', '2026-10-12');
  assert.strictEqual(e0.ok, true); assert.strictEqual(e0.found, false); assert.strictEqual(e0.firstDay, null);
  assert.deepStrictEqual([e0.workingDays, e0.daysWorked, e0.daysOff, e0.lateDays, e0.shortDays, e0.avgShiftMs, e0.medianStart, e0.medianEnd, e0.attendanceRate], [0, 0, 0, 0, 0, null, null, null, null]);
  assert.deepStrictEqual([e0.streaks.current, e0.streaks.best], [0, 0]);
  assert.strictEqual(by(e0, '2026-09-30').state, 'unknown'); assert.strictEqual(by(e0, '2026-10-07').state, 'closed', 'nobody was in: closed, not off');
  say('empty data: ok, found false, zeros and nulls, no crash');
  const e1 = await M.attendance({ db: empty.db, now: at('2026-10-12', '12:00') }, { name: 'Tess Welder', from: '2026-09-30', to: '2026-10-12' });
  assert.deepStrictEqual(e1.calendar.map(c => c.state), ['unknown', 'unknown'].concat(new Array(10).fill('closed'), ['pending']), 'with nobody in, every logged day is closed, today pending');
  assert(e1.notes.some(n => /No sign-in or recorded work was found/.test(n)));
  const failA = fakeStore(); failA.fail('Efficiency_Daily'); failA.fail('Station_Sessions');
  const f0 = await att(failA, 'Tess Welder', FROM, TODAY);
  assert.strictEqual(f0.ok, false); assert.strictEqual(f0.unavailable, true); assert(f0.errors.length === 2);
  const failB = realShop(); failB.fail('Efficiency_Daily');
  const f1 = await att(failB, 'Tess Welder', '2026-12-01', TODAY);
  assert.strictEqual(f1.ok, true); assert.strictEqual(f1.partial, true); assert(/rollups/.test(f1.errors[0])); assert(f1.notes.some(n => /could not be read/.test(n)));
  assert.strictEqual((await att(st, PIN, FROM, TODAY)).ok, false, 'a digits-only name is a PIN, never a person');
  assert.strictEqual((await att(st, '', FROM, TODAY)).ok, false); assert.strictEqual((await att(st, '12 34 56', FROM, TODAY)).ok, false);
  assert.strictEqual((await att(st, 'Tess Welder', '2026-13-01', TODAY)).ok, false, 'a bad day'); assert.strictEqual((await att(st, 'Tess Welder', TODAY, FROM)).ok, false, 'from after to');
  assert.strictEqual((await M.attendance(ctxOf(st), { name: 'Tess Welder', from: undefined, to: TODAY })).ok, false);
  const wide = await att(st, 'Tess Welder', '2025-01-01', TODAY);
  assert.strictEqual(wide.calendar.length, 366, 'cut to 366 days'); assert.strictEqual(wide.from, '2025-12-21'); assert(wide.notes.some(n => /last 366 days/.test(n)));
  assert(wide.calendar.filter(c => c.state === 'unknown').length >= 270, 'a year back is mostly not tracked');
  assert.strictEqual(wide.daysOff, 3); assert.strictEqual(wide.workingDays, 55);
  say('empty data, one or both reads failing, bad names and days, a 366-day cut');

  /* ── 12 · rows handed in give the same answer as rows read ── */
  const fixed = realShop();
  const viaDb = await att(fixed, 'Tess Welder', FROM, TODAY);
  const viaRows = await M.attendance({ rollups: fixed.docs('Efficiency_Daily'), sessions: fixed.docs('Station_Sessions'), rowsFrom: '2026-10-02', now: NOW, today: TODAY }, { name: 'Tess Welder', from: FROM, to: TODAY });
  assert.deepStrictEqual(viaRows.calendar, viaDb.calendar, 'handed-in rows: the same calendar');
  assert.deepStrictEqual([viaRows.workingDays, viaRows.daysOff, viaRows.shortDays, viaRows.lateDays, viaRows.avgShiftMs, viaRows.streaks.current], [viaDb.workingDays, viaDb.daysOff, viaDb.shortDays, viaDb.lateDays, viaDb.avgShiftMs, viaDb.streaks.current]);
  const viaFns = await M.attendance({ loadRollups: async () => fixed.docs('Efficiency_Daily'), loadSessions: async () => ({ rows: fixed.docs('Station_Sessions') }), now: NOW, today: TODAY }, { name: 'Tess Welder', from: FROM, to: TODAY });
  assert.deepStrictEqual(viaFns.calendar, viaDb.calendar, 'loader functions: the same calendar');
  const noReach = await M.attendance({ rollups: fixed.docs('Efficiency_Daily'), sessions: fixed.docs('Station_Sessions'), now: NOW, today: TODAY }, { name: 'Tess Welder', from: '2026-12-14', to: '2026-12-18' });
  assert.strictEqual(noReach.rules.shortBasis, 'fallback', 'arrays with no stated reach: learn from the period only (five days here, so the fallback limit)');
  assert.strictEqual(by(noReach, '2026-12-14').state, 'partial');
  say('handed-in rows and loader functions give the same calendar as reading the collections');

  /* ── 13 · reads: rollups and sessions only, once, cached ── */
  const cost = realShop(), c1 = await att(cost, 'Tess Welder', FROM, TODAY);
  const names = new Set(cost.reads.map(x => x.name));
  assert.deepStrictEqual([...names].sort(), ['Efficiency_Daily', 'Station_Sessions', 'config'], 'only rollups, sessions and the two config documents');
  assert.strictEqual(cost.readsOf('Station_Activity').length + cost.readsOf('Order_Timeline').length, 0, 'never a single event, never a seal');
  assert.strictEqual(cost.readsOf('Efficiency_Daily').length, 2, 'two range queries for the rollups: the days that are over, and today');
  assert(cost.readsOf('Station_Sessions').length <= 4, 'two range queries for the sessions (twice each, for Firestore times)');
  assert(cost.readsOf('Efficiency_Daily').every(x => x.n < 400 && x.n > 0), 'each part is a day range of a few hundred documents at most');
  assert(cost.readsOf('config').every(x => ['employeeAliases', 'employeeSchedule', 'stationAdmins'].includes(x.doc)) && cost.readsOf('config').length <= 3, 'the two config documents, plus the Admin list the auto sign-out of stale sessions reads (kept a minute)');
  const readsBefore = cost.reads.length;
  await att(cost, 'Raj Packer', FROM, TODAY); await att(cost, 'Tess Welder', FROM, TODAY);
  assert.strictEqual(cost.reads.length, readsBefore, 'a second look within the cache life reads nothing more');
  NOW += 20000; Date.now = () => NOW;
  await M.attendance(ctxOf(cost, { now: NOW }), { name: 'Tess Welder', from: FROM, to: TODAY });
  assert.strictEqual(cost.readsOf('Efficiency_Daily').length, 3, 'after 5 seconds only today is read again (one rollups query), not the 80 days before');
  assert(cost.readsOf('Station_Sessions').length <= 6 && cost.readsOf('Station_Sessions').length > 0, 'and only today\'s sessions');
  assert(cost.readsOf('Station_Sessions').slice(-2).every(x => x.n <= 12), "today's sessions are a handful of documents");
  const dayRange = fakeStore(); shop(dayRange).regular('2026-12-01', 'Tess Welder');
  await M.attendance({ db: dayRange.db, now: at('2026-12-02', '10:00') }, { name: 'Tess Welder', from: '2026-12-01', to: '2026-12-01' });
  const rr = dayRange.readsOf('Efficiency_Daily')[0];
  assert.deepStrictEqual(rr.filters, ['day>=', 'day<='], 'a single-field day range: no composite index'); assert.strictEqual(dayRange.readsOf('Station_Sessions')[0].filters.join(), 'startAt>=,startAt<');
  assert.strictEqual(dayRange.readsOf('Efficiency_Daily').length, 1, 'a window of days that are over is one range query'); assert.strictEqual(dayRange.readsOf('Station_Sessions').length, 1);
  say('reads: rollups and sessions by day range (days over: 10 min cache; today: 5 s), two config docs; nothing per event');

  /* ── 14 · the window before: prev and delta ── */
  const pw = await M.attendance(ctxOf(st), { name: 'Tess Welder', from: '2026-11-02', to: '2026-11-08', prev: { from: '2026-10-26', to: '2026-11-01' } });
  assert.deepStrictEqual([pw.workingDays, pw.daysWorked, pw.daysOff], [5, 4, 1], 'the week of 4 Nov: one day off');
  assert.deepStrictEqual([pw.prev.from, pw.prev.to, pw.prev.days, pw.prev.workingDays, pw.prev.daysOff], ['2026-10-26', '2026-11-01', 7, 5, 0]);
  assert.strictEqual(pw.delta.daysOff, 1); assert.strictEqual(pw.delta.workingDays, 0); assert.strictEqual(pw.delta.daysWorked, -1); assert.strictEqual(pw.delta.attendanceRate, -20);
  assert.strictEqual(pw.prev.lateDays, 0); assert.strictEqual(pw.prev.attendanceRate, 100);
  const pw2 = await M.attendance(ctxOf(st), { name: 'Tess Welder', from: '2026-12-07', to: '2026-12-13', prev: { from: '2026-11-30', to: '2026-12-06' } });
  assert.strictEqual(pw2.prev.extraDays, 1, 'the extra Saturday of the week before'); assert.strictEqual(pw2.delta.extraDays, -1); assert.strictEqual(pw2.delta.daysOff, 0);
  assert.strictEqual(pw2.prev.avgShiftMs, null === pw2.prev.avgShiftMs ? null : pw2.prev.avgShiftMs); assert(pw2.prev.avgShiftMs > 7 * HOUR, 'the week before has an average shift');
  for (const bad of [{ from: '2026-11-05', to: '2026-11-20' }, { from: '2026-11-01', to: '2026-10-01' }, { from: 'x', to: 'y' }, null]) assert.strictEqual((await M.attendance(ctxOf(st), { name: 'Tess Welder', from: '2026-11-02', to: '2026-11-08', prev: bad })).prev, undefined, 'a window that overlaps or is not a window is ignored');
  const pw3 = await M.attendance(ctxOf(st), { name: 'Tess Welder', from: '2026-12-07', to: '2026-12-13' });
  assert.strictEqual(pw3.prev, undefined); assert.strictEqual(pw3.delta, undefined, 'no prev asked: no prev answered');
  // the same numbers in the METRIC shape of the employee page
  const MK = ['workingDays', 'daysWorked', 'daysOff', 'extraDays', 'shortDays', 'lateDays', 'attendanceRate', 'avgShiftHours', 'medianStart', 'medianEnd', 'currentStreak', 'longestStreak'];
  assert.deepStrictEqual(Object.keys(pw.metrics), MK);
  for (const k of MK) { const m = pw.metrics[k]; for (const f of ['label', 'unit', 'value', 'prev', 'delta', 'deltaPct', 'better', 'def', 'estimated']) assert(f in m, k + ' has ' + f); assert(typeof m.label === 'string' && typeof m.def === 'string' && m.def.length > 15, k + ' label and def'); assert(['days', 'percent', 'hours', 'clock'].includes(m.unit), k + ' unit'); }
  assert.deepStrictEqual([pw.metrics.daysOff.value, pw.metrics.daysOff.prev, pw.metrics.daysOff.delta, pw.metrics.daysOff.better, pw.metrics.daysOff.unit], [1, 0, 1, 'down', 'days']);
  assert.strictEqual(pw.metrics.daysOff.deltaPct, null, 'a percent change from zero is null'); assert.strictEqual(pw.metrics.daysWorked.deltaPct, -20); assert.strictEqual(pw.metrics.daysWorked.better, 'up');
  assert.strictEqual(pw.metrics.attendanceRate.value, 80); assert.strictEqual(pw.metrics.attendanceRate.prev, 100); assert.strictEqual(pw.metrics.attendanceRate.delta, -20); assert.strictEqual(pw.metrics.attendanceRate.unit, 'percent');
  assert.strictEqual(pw.metrics.avgShiftHours.unit, 'hours'); assert(pw.metrics.avgShiftHours.value > 7 && pw.metrics.avgShiftHours.value < 9); assert.strictEqual(pw.metrics.medianStart.unit, 'clock'); assert.strictEqual(pw.metrics.medianStart.value, 480);
  assert.strictEqual(pw.metrics.currentStreak.prev, null); assert.strictEqual(pw.metrics.currentStreak.value, pw.streaks.current);
  assert.strictEqual(r.metrics.avgShiftHours.estimated, true); assert(/estimated length/.test(r.metrics.avgShiftHours.why), 'an estimated metric says why'); assert.strictEqual(r.metrics.daysOff.estimated, false);
  assert.strictEqual(r.metrics.daysOff.prev, null); assert.strictEqual(r.metrics.daysOff.n, 55);
  assert.strictEqual(newt.metrics.daysOff.value, 0); assert.strictEqual(e0.metrics.avgShiftHours.value, null, 'a number that cannot be known is null, never a fake zero'); assert.strictEqual(e0.metrics.attendanceRate.value, null);
  say('prev: the same counts for the window before, and their differences; a bad or overlapping prev is ignored; METRIC shape (label, unit, value, prev, delta, better, def, estimated)');

  /* ── 15 · through the real person op (the employee page's data) ── */
  EP.resetCache();
  let ip = 0;
  const person = async (store, body) => { const x = await EFF._t.handle({ httpMethod: 'POST', headers: { 'x-nf-client-connection-ip': '203.0.113.' + (++ip) }, body: JSON.stringify(Object.assign({ op: 'person', key: PASS, name: 'Tess Welder' }, body)) }, store.db); return { status: x.statusCode, body: JSON.parse(x.body || '{}') }; };
  const quarter = await person(st, { range: 'quarter' });
  assert.strictEqual(quarter.status, 200); assert.strictEqual(quarter.body.ok, true);
  assert(quarter.body.attendance && quarter.body.attendance.ok === true, 'the person answer carries attendance');
  assert.strictEqual(quarter.body.attendance.calendar, undefined, 'the calendar is sent once, at the top level (E4)'); assert(Array.isArray(quarter.body.calendar), 'and calendar');
  const qa = quarter.body.attendance;
  assert.strictEqual(qa.from, '2026-09-23'); assert.strictEqual(qa.to, TODAY); assert.strictEqual(quarter.body.calendar.length, 90);
  assert.deepStrictEqual([qa.workingDays, qa.daysWorked, qa.daysOff, qa.extraDays, qa.shortDays, qa.lateDays], [55, 52, 3, 1, 2, 1], 'the same numbers as reading the collections directly');
  assert.strictEqual(quarter.body.calendar.filter(c => c.state === 'unknown').length, 9, '23 Sep to 1 Oct are not tracked'); assert(qa.prev && qa.delta, 'compare is on by default: prev and delta');
  assert.strictEqual(qa.prev.days, 90);
  const week = await person(st, { range: 'week', compare: false });
  const wa = week.body.attendance;
  assert.strictEqual(week.body.calendar.length, 7); assert.strictEqual(wa.rules.usualShiftMs, 8.5 * HOUR, 'a week asked for: her usual shift is learned from the 90 days before, read here');
  assert.strictEqual(wa.rules.usualStart, 480); assert.strictEqual(wa.prev, undefined, 'compare:false: no prev');
  assert.strictEqual(week.body.calendar[week.body.calendar.length - 1].state, 'pending', 'today'); assert.strictEqual(wa.streaks.current, 32);
  const sbp = await person(st, { range: 'month', sandbox: true });
  assert.strictEqual(sbp.body.attendance.mode, 'sandbox'); assert.strictEqual(sbp.body.attendance.daysWorked, 2, 'sandbox through the real op: only the rehearsal'); assert.strictEqual(sbp.body.attendance.extraDays, 1);
  const noone = await person(st, { range: 'week', name: 'Nobody Here' });
  assert.strictEqual(noone.body.found, false); assert.strictEqual(noone.body.attendance.found, false); assert.strictEqual(noone.body.attendance.daysOff, 0, 'a name nobody has used is never "off"');
  const dayOnly = await person(st, { range: 'day', day: '2026-10-14' });
  assert.strictEqual(dayOnly.body.calendar.length, 1); assert.strictEqual(dayOnly.body.calendar[0].state, 'off'); assert.strictEqual(dayOnly.body.attendance.daysOff, 1);
  const bytes = JSON.stringify(quarter.body).length;
  assert(bytes < 400000, 'the quarter answer is a sensible size (' + bytes + ' bytes)');
  say('through the real person op: attendance and calendar in the answer, the same numbers, prev and delta, the 90-day learning read on top of a week, sandbox, an unknown name, a one-day view');

  /* ── 16 · no PIN anywhere ── */
  const all = JSON.stringify([r, ra, newt, mia, zoe, sbr, e0, wide, wk, one, pw, pw2, quarter.body, week.body, sbp.body, noone.body, dayOnly.body]);
  assert(!all.includes(PIN), 'the PIN never appears'); assert(!all.includes('913482')); assert(!/employeeId/.test(all), 'the employeeId field is never copied');
  assert(!/passcode|editpass/i.test(all), 'no passcode word in any answer');
  for (const x of [r, ra, newt, zoe]) assert.strictEqual(typeof x.name, 'string');
  assert(!JSON.stringify(r).includes('Paul K') && !JSON.stringify(r).includes('Raj'), "other people's names are never in a person's answer");
  assert(!JSON.stringify(quarter.body.attendance).includes('Raj') && !JSON.stringify(quarter.body.attendance).includes('Mia'), "nor in the attendance part of the person op");
  say('no PIN, no employeeId, no passcode and no other person in any answer');

  Date.now = realNow;
  say('OK employee-attendance');
})().catch(e => { Date.now = realNow; console.error(e); process.exit(1); });
