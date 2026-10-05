// A fake `person` (with a range), `personOrders` and `live` (plans/employee-hr/api.md: E4, E1, E2) for the employee page's tests and screenshots
// (charm-nest-efficiency-person.js). FAKE DATA ONLY: two invented people, invented customers and order numbers, drawn pictures as data
// addresses, a fake passcode. No network, no Firestore.
//   const F = require('./efficiency-person-fixture.cjs'); const fx = F.make(); fx.answer(body) → { status, json }
//   People: 'Ana M.' (signed in at Welding now, holding an order) and 'Ben R.' (not signed in yet today, last seen yesterday).
//   Any other name is "found:false" (nothing logged under it).
//   fx.state.calls logs { op, name, range, day, from, to, compare, q, cursor } of every call (never the key)
//   fx.state.fail = 2 fails the next two calls with 503 · fx.setDelay(fn(body) → ms) · fx.setLive('working' | 'idle' | 'out')
const OF = require('./efficiency-orders-fixture.cjs');
const KEY = OF.KEY;
const HR = 3600000, DAY = 86400000;
const NYF = new Intl.DateTimeFormat('en-CA', { timeZone: 'America/New_York', year: 'numeric', month: '2-digit', day: '2-digit' });
const NYH = new Intl.DateTimeFormat('en-US', { timeZone: 'America/New_York', hourCycle: 'h23', hour: 'numeric' });
const ymd = t => NYF.format(new Date(t));
const dayMs = d => Date.parse(d + 'T12:00:00Z');
const addDays = (d, n) => new Date(dayMs(d) + n * DAY).toISOString().slice(0, 10);
const diff = (a, b) => Math.round((dayMs(b) - dayMs(a)) / DAY);
const dow = d => new Date(dayMs(d)).getUTCDay();
const mondayOf = d => addDays(d, -((dow(d) + 6) % 7));
const rnd = s => { let h = 2166136261; for (let i = 0; i < s.length; i++) { h ^= s.charCodeAt(i); h = Math.imul(h, 16777619); } return ((h >>> 0) % 100000) / 100000; };
const r1 = v => Math.round(v * 10) / 10, r2 = v => Math.round(v * 100) / 100;
function midnight(day) { let t = dayMs(day) - 12 * HR + 4 * HR; for (let i = 0; i < 6 && ymd(t - 1) === day; i++) t -= HR; for (let i = 0; i < 6 && ymd(t) !== day; i++) t += HR; return t; }
const STATIONS = [['welding', 'Welding'], ['sorting', 'Sorting'], ['assembly', 'Assembly'], ['shipping', 'Shipping']];
const ALL9 = [['sorting', 'Sorting'], ['welding', 'Welding'], ['assembly', 'Assembly'], ['shipping', 'Shipping'], ['design', 'Design'], ['laser', 'Laser'], ['sorter', 'Sorter'], ['qr', 'QR Printer'], ['inbox', 'Inbox']];
const WEIGHTS = [.07, .13, .14, .12, .06, .12, .14, .13, .09];                                    // 8:00 .. 16:00
const RANGES = { day: 1, week: 7, month: 30, quarter: 90, year: 365 };
const median = a => { if (!a.length) return null; const s = a.slice().sort((x, y) => x - y), m = s.length >> 1; return s.length % 2 ? s[m] : (s[m - 1] + s[m]) / 2; };
const p90 = a => { if (!a.length) return null; const s = a.slice().sort((x, y) => x - y); return s[Math.min(s.length - 1, Math.floor(s.length * 0.9))]; };
const sum = (a, f) => a.reduce((n, x) => n + f(x), 0);

function make(opts = {}) {
  const t0 = Date.now(), base = opts.now || Date.now();
  const now = () => base + (Date.now() - t0);
  const st = { calls: [], fail: 0, key: opts.key || KEY, delay: null, live: 'working', mode: 'real' };
  const today = ymd(base), firstDay = addDays(today, -200), trackingStart = addDays(today, -250), nowH = () => +NYH.format(new Date(now()));
  const orders = OF.make({ now: base, key: st.key, count: opts.orders == null ? 60 : opts.orders });
  const PEOPLE = {
    'ana m.': { name: 'Ana M.', key: 'ana m', salt: 0, signedInToday: true },
    'ben r.': { name: 'Ben R.', key: 'ben r', salt: 3, signedInToday: false }
  };

  /* ── one day of one person ── */
  function worked(P, day, live, short) {
    const r = s => rnd(P.key + day + s), mid = midnight(day);
    let firstIn = mid + (8 * 60 + Math.floor(r('f') * 50)) * 60000, signed = (short ? 2.4 + r('s') * 1.2 : 6.6 + r('s') * 1.6) * HR, lastOut = firstIn + signed + 40 * 60000;
    if (live) { firstIn = Math.max(mid + 60000, base - 3.2 * HR); signed = base - firstIn; lastOut = 0; }   // (fixed at the start of the run, so a figure never moves under a test)
    let parts = Math.round((short ? .4 : 1) * (74 + r('p') * 64)); if (live) parts = Math.round(parts * signed / (7.5 * HR));
    const ord = Math.max(1, Math.round(parts / (2.1 + r('o')))), scans = Math.round(parts * (1.9 + r('c') * .6)), completes = Math.round(ord * .93), prints = Math.round(ord * .55);
    const active = signed * (.56 + r('a') * .25), idle = (signed - active) * .7;
    const hours = Array(24).fill(0), hourScans = Array(24).fill(0);
    if (live) { const h1 = +NYH.format(new Date(base)), h0 = Math.max(0, h1 - 3), n = h1 - h0 + 1; for (let h = h0; h <= h1; h++) { hours[h] = Math.round(parts / n); hourScans[h] = Math.round(scans / n); } }
    else WEIGHTS.forEach((w, i) => { hours[8 + i] = Math.round(parts * w); hourScans[8 + i] = Math.round(scans * w); });
    const wel = .42 + r('m') * .2, srt = .3, asm = 1 - wel - srt, split = { welding: wel, sorting: srt, assembly: asm };
    const late = !live && firstIn - mid > (8 * 60 + 35) * 60000;
    return { state: short ? 'partial' : 'worked', parts, orders: ord, scans, completes, prints, rejects: r('j') < .16 ? 1 : 0, errors: r('e') < .1 ? 1 : 0, undos: r('u') < .14 ? 1 : 0,
      activeMs: Math.round(active), idleMs: Math.round(idle), signedMs: Math.round(signed), firstIn, lastOut, hours, hourScans, split, late, short, mid, live };
  }
  function dayRec(P, day) {
    if (day > today) return null;
    if (day < trackingStart) return { state: 'unknown', note: 'Sign-in logging had not begun on this day.' };
    const k = diff(firstDay, day); if (k < 0) return { state: 'before' };
    const w = dow(day);
    if (day === today) return P.signedInToday ? worked(P, day, true, false) : { state: 'pending' };
    if (w === 0 || w === 6) return { state: 'closed' };
    if ((k + P.salt) % 11 === 6) return { state: 'off' };
    return worked(P, day, false, (k + P.salt) % 9 === 4);
  }

  /* ── a window of days ── */
  function windowOf(b) {
    let to = /^\d{4}-\d{2}-\d{2}$/.test(b.day || '') ? b.day : today; if (to > today) to = today;
    if (b.range && typeof b.range === 'object') { const f = b.range.from, t = b.range.to; to = t > today ? today : t; return { from: f, to, days: diff(f, to) + 1 }; }
    const n = RANGES[b.range] || 7; return { from: addDays(to, -(n - 1)), to, days: n };
  }
  function stats(P, w) {
    const days = []; for (let d = w.from; d <= w.to; d = addDays(d, 1)) days.push({ day: d, rec: dayRec(P, d) });
    const wk = days.filter(x => x.rec && (x.rec.state === 'worked' || x.rec.state === 'partial'));
    const S = { days, wk, worked: wk.length, off: days.filter(x => x.rec && x.rec.state === 'off').length };
    for (const k of ['parts', 'orders', 'scans', 'completes', 'prints', 'rejects', 'errors', 'undos', 'activeMs', 'idleMs', 'signedMs']) S[k] = wk.length ? sum(wk, x => x.rec[k]) : null;
    S.hours = Array.from({ length: 24 }, (_, h) => ({ hour: h, parts: wk.length ? sum(wk, x => x.rec.hours[h]) : null, scans: wk.length ? sum(wk, x => x.rec.hourScans[h]) : null }));
    S.best = wk.length ? wk.reduce((a, x) => (x.rec.parts > a.parts ? { day: x.day, parts: x.rec.parts } : a), { day: '', parts: -1 }) : null;
    S.peak = wk.length ? S.hours.reduce((a, h) => (h.parts > a.parts ? h : a), { hour: 0, parts: -1 }).hour : null;
    S.spo = wk.map(x => x.rec.activeMs / 1000 / x.rec.orders);
    S.start = wk.length ? sum(wk, x => (x.rec.firstIn - x.rec.mid) / 60000) / wk.length : null;
    S.end = wk.filter(x => x.rec.lastOut).length ? sum(wk.filter(x => x.rec.lastOut), x => (x.rec.lastOut - x.rec.mid) / 60000) / wk.filter(x => x.rec.lastOut).length : null;
    return S;
  }
  const M = (label, unit, v, pv, better, def, extra) => { v = v == null ? null : v; pv = pv == null ? null : pv; const delta = v != null && pv != null ? r2(v - pv) : null; return Object.assign({ label, unit, value: v, prev: pv, delta, deltaPct: delta != null && pv ? r1(delta / Math.abs(pv) * 100) : null, better, def, estimated: false }, extra || {}); };
  const div = (a, b) => (a != null && b ? a / b : null);
  function kpis(S, Q) {
    const f = (fn) => [fn(S), Q ? fn(Q) : null];
    const mm = (key, label, unit, better, def, fn, extra) => { const [v, pv] = f(fn); return [key, M(label, unit, v == null ? null : r1(v), pv == null ? null : r1(pv), better, def, extra)]; };
    const hrs = s => (s.activeMs == null ? null : s.activeMs / HR);
    return Object.fromEntries([
      mm('parts', 'Pieces finished', 'pieces', 'up', 'Pieces finished at a station, net of undos.', s => s.parts, { n: S.parts }),
      mm('orders', 'Orders', 'orders', 'up', 'Distinct orders worked on.', s => s.orders),
      mm('ordersCompleted', 'Orders completed', 'orders', 'up', 'Orders finished at a station.', s => s.completes),
      mm('scans', 'Scans', 'scans', 'up', 'Scan actions logged under this name.', s => s.scans, { estimated: true, why: 'A phone scan is credited to the desktop that receives it.' }),
      mm('prints', 'Labels printed', 'prints', null, 'Labels printed.', s => s.prints),
      mm('partsPerDay', 'Pieces per day worked', 'pieces/day', 'up', 'Pieces divided by days worked.', s => div(s.parts, s.worked)),
      mm('ordersPerDay', 'Orders per day worked', 'orders/day', 'up', 'Orders divided by days worked.', s => div(s.orders, s.worked)),
      mm('partsPerActiveHour', 'Pieces per active hour', 'pieces/hour', 'up', 'Pieces divided by active hours.', s => div(s.parts, hrs(s))),
      mm('partsPerSignedHour', 'Pieces per signed-in hour', 'pieces/hour', 'up', 'Pieces divided by signed-in hours.', s => div(s.parts, s.signedMs == null ? null : s.signedMs / HR)),
      mm('bestDay', 'Best day', 'pieces', 'up', 'The day with the most parts.', s => (s.best ? s.best.parts : null), { day: S.best ? S.best.day : undefined }),
      mm('peakHour', 'Busiest hour', 'clock', null, 'The hour with the most parts.', s => (s.peak == null ? null : s.peak * 60)),
      mm('secPerOrderMean', 'Working seconds per order', 'seconds', 'down', 'Active seconds divided by orders.', s => div(s.activeMs == null ? null : s.activeMs / 1000, s.orders)),
      mm('secPerScanMean', 'Average per scan', 'seconds', 'down', 'Active seconds divided by scans.', s => div(s.activeMs == null ? null : s.activeMs / 1000, s.scans)),
      mm('secPerOrderMedian', 'Median per order', 'seconds', 'down', 'The middle time for one order.', s => median(s.spo), { estimated: true, why: 'Worked out from each day\'s active time and orders.', n: S.spo.length }),
      mm('secPerOrderP90', 'Slowest 10%', 'seconds', 'down', 'Nine in ten orders were faster than this.', s => p90(s.spo)),
      mm('secBetweenScansMedian', 'Between scans', 'seconds', 'down', 'The middle gap between scans.', s => { const a = div(s.activeMs == null ? null : s.activeMs / 1000, s.scans); return a == null ? null : a * .8; }),
      mm('signedHours', 'Signed in', 'hours', null, 'Time signed in at a station.', s => (s.signedMs == null ? null : s.signedMs / HR)),
      mm('activeHours', 'Active', 'hours', 'up', 'Time between actions under 5 minutes apart.', hrs),
      mm('idleHours', 'Idle', 'hours', null, 'Gaps over 5 minutes while signed in.', s => (s.idleMs == null ? null : s.idleMs / HR)),
      mm('unloggedHours', 'Unlogged', 'hours', null, 'Signed in with no action logged.', s => (s.signedMs == null ? null : Math.max(0, s.signedMs - s.activeMs - s.idleMs) / HR)),
      mm('activeShare', 'Active vs signed in', 'percent', 'up', 'Active time as a share of signed-in time.', s => div(s.activeMs, s.signedMs) == null ? null : s.activeMs / s.signedMs * 100),
      mm('avgStart', 'Usual start', 'clock', null, 'Average first sign-in on days worked.', s => s.start),
      mm('avgEnd', 'Usual finish', 'clock', null, 'Average last sign-out on days worked.', s => s.end)
    ]);
  }
  function seriesOf(S, w) {
    const pt = (day, to, days, list) => {
      const wk = list.filter(x => x.rec && (x.rec.state === 'worked' || x.rec.state === 'partial')), has = wk.length > 0, g = k => (has ? sum(wk, x => x.rec[k]) : null);
      const act = g('activeMs'), sg = g('signedMs'), parts = g('parts'), ord = g('orders'), one = wk.length === 1 && days === 1 ? wk[0].rec : null;
      return { day, to, days, workedDays: wk.length, hasData: has, parts, orders: ord, scans: g('scans'), completes: g('completes'), prints: g('prints'), rejects: g('rejects'), errors: g('errors'), undos: g('undos'), activeMs: act, idleMs: g('idleMs'), signedMs: sg,
        perActiveHour: has ? r1(parts / (act / HR)) : null, perSignedHour: has ? r1(parts / (sg / HR)) : null, secPerOrder: has ? r1(act / 1000 / ord) : null, firstIn: one ? one.firstIn : null, lastOut: one ? one.lastOut || null : null, shiftMs: one ? one.signedMs : null };
    };
    if (w.days <= 92) return { granularity: 'day', series: S.days.map(x => pt(x.day, x.day, 1, [x])) };
    const out = [], mons = new Map(); for (const x of S.days) { const m = mondayOf(x.day), k = m < w.from ? w.from : m; if (!mons.has(k)) mons.set(k, []); mons.get(k).push(x); }
    for (const [k, list] of mons) out.push(pt(k, list[list.length - 1].day, list.length, list));
    return { granularity: 'week', series: out };
  }
  function calendarOf(P, S) {
    return S.days.filter(x => x.rec).map(x => { const c = x.rec, o = { day: x.day, state: c.state }; if (c.note) o.note = c.note; if (c.state === 'worked' || c.state === 'partial') Object.assign(o, { signedMs: c.signedMs, activeMs: c.activeMs, firstIn: c.firstIn, lastOut: c.lastOut || null, parts: c.parts, orders: c.orders, late: c.late, short: c.short, others: 4 }); return o; });
  }
  /** E9's block: metrics (METRICs), streaks, the average shift, the honest sentence. (The calendar is its own top-level field.) */
  function attendanceOf(P, S, Q) {
    const rate = s => { const t = s.worked + s.off; return t ? r1(s.worked / t * 100) : null; };
    let cur = 0; for (let d = today; d >= trackingStart; d = addDays(d, -1)) { const c = dayRec(P, d); if (!c || c.state === 'closed' || c.state === 'before' || c.state === 'unknown') continue; if (c.state === 'worked' || c.state === 'partial') cur++; else if (c.state !== 'pending') break; }
    let best = 0, run = 0; for (const x of S.days) { const c = x.rec; if (!c || c.state === 'closed' || c.state === 'before' || c.state === 'unknown') continue; if (c.state === 'worked' || c.state === 'partial') { run++; best = Math.max(best, run); } else if (c.state !== 'pending') run = 0; }
    const lateN = s => s.wk.filter(x => x.rec.late).length, shortN = s => s.wk.filter(x => x.rec.short).length, shiftH = s => (s.worked ? s.signedMs / s.worked / HR : null);
    const mid = s => (s.start == null ? null : Math.round(s.start)), end = s => (s.end == null ? null : Math.round(s.end));
    const m = (label, unit, v, pv, better, def, extra) => M(label, unit, v == null ? null : r1(v), pv == null ? null : r1(pv), better, def, extra);
    return { ok: true, found: S.worked > 0, from: S.days[0].day, to: S.days[S.days.length - 1].day, trackingStart, firstDay,
      metrics: {
        workingDays: m('Working days', 'days', S.worked + S.off, Q ? Q.worked + Q.off : null, null, 'Days the team worked and this person could have worked.', { estimated: false }),
        daysWorked: m('Days worked', 'days', S.worked, Q ? Q.worked : null, 'up', 'Working days with any sign-in or recorded work.'),
        daysOff: m('Days off', 'days', S.off, Q ? Q.off : null, 'down', 'Working days on which the person never signed in and recorded nothing.'),
        extraDays: m('Extra days', 'days', 0, Q ? 0 : null, null, 'Days present that are not working days.'),
        shortDays: m('Short days', 'days', shortN(S), Q ? shortN(Q) : null, 'down', 'Signed in for less than half of the person\'s own median shift.', { estimated: true, why: 'A day that ended at the midnight sign-out used the last recorded action.' }),
        lateDays: m('Late starts', 'days', lateN(S), Q ? lateN(Q) : null, 'down', 'First sign-in more than 30 minutes after the person\'s own usual start.'),
        attendanceRate: m('Attendance', 'percent', rate(S), Q ? rate(Q) : null, 'up', 'Days worked as a share of working days.'),
        avgShiftHours: m('Average shift', 'hours', shiftH(S), Q ? shiftH(Q) : null, null, 'Time signed in per finished day worked.', { estimated: true, why: 'Days that ended at the midnight sign-out are cut back to the last action.' }),
        medianStart: m('Usual start', 'clock', mid(S), Q ? mid(Q) : null, null, 'Middle time of the first sign-in.'),
        medianEnd: m('Usual finish', 'clock', end(S), Q ? end(Q) : null, null, 'Middle time of the last sign-out.', { estimated: true, why: 'Some finishes are the last recorded action.' }),
        currentStreak: m('Current streak', 'days', cur, null, 'up', 'Days worked in a row up to the end of the period.'),
        longestStreak: m('Longest streak', 'days', best, null, null, 'The most days worked in a row.')
      },
      avgShiftMs: S.worked ? Math.round(S.signedMs / S.worked) : null, streaks: { current: cur, best },
      definitions: { notAttendance: 'These numbers come from logged sign-ins and logged work, not from a time clock: a day off here means not logged in, not proof of absence.' },
      estimated: { any: true, days: [], fields: { avgShiftMs: { estimated: true, why: 'Days that ended at the midnight sign-out are cut back.' }, shortDays: { estimated: true, why: 'Same.' }, workingDays: { estimated: false, rule: 'Decided by the team-size rule, not measured.' }, daysOff: { estimated: false, rule: 'Decided by the team-size rule, not measured.' } } }, notes: [] };
  }
  /** E10's 13 kinds. Counted from the rollup counters on the days from `countersFrom` on only (a dash before), the two cross-checked kinds from the newest orders only. */
  const K13 = [['undone', 'Completion undone', 'own', 'undos'], ['reprint', 'Label reprinted', 'own', .2], ['rescan', 'Scanned again', 'own', .15], ['heldOrSkipped', 'Held or skipped', 'own', .1], ['reopenedLater', 'Reopened later by someone else', 'order', 'win'], ['cameBack', 'Order came back', 'order', 'win'],
    ['unknownSku', 'Piece not in the files', 'order', .05], ['qaFlag', 'Flagged for the team', 'order', .05], ['cancelAlert', 'Cancelled order met', 'order', .04], ['refused', 'Refused', 'order', 'rejects'], ['lookupFailed', 'Lookup failed', 'system', .06], ['failed', 'Error shown', 'system', 'errors'], ['replyFailed', 'Reply failed', 'system', .03]];
  const NOTES = { undone: 'A completion was undone.', reprint: 'The label was printed again.', rescan: 'Scanned again.', heldOrSkipped: 'The order was held or skipped.', reopenedLater: 'Reopened later by another person.', cameBack: 'The order came back to an earlier station.', unknownSku: 'No picture files for this piece.', qaFlag: 'Flagged for the team.', cancelAlert: 'The customer cancelled.', refused: 'Scratched on the back', lookupFailed: 'The Etsy lookup failed.', failed: 'Scan did not match the order', replyFailed: 'The reply was not sent.' };
  const countersFrom = addDays(today, -120);
  function issuesOf(P, S, Q, w) {
    const per = (day, kind, spec) => { const c = dayRec(P, day); if (!c || !(c.state === 'worked' || c.state === 'partial')) return 0; if (typeof spec === 'string') return spec === 'win' ? 0 : c[spec]; return rnd(P.key + day + kind) < spec ? 1 : 0; };
    const countOver = (days, kind, spec) => days.reduce((n, x) => n + per(x.day, kind, spec), 0);
    const range = S.days, nd = range.length;
    const evFrom = addDays(w.to, -30), evDays = range.filter(x => x.day >= evFrom);
    const rows = K13.map(([kind, label, attribution, spec]) => {
      const win = spec === 'win', exact = spec === 'undos', counted = exact || win ? range : range.filter(x => x.day >= countersFrom), dc = counted.filter(x => x.rec).length;
      let count = null;
      if (win) { const fin = evDays.filter(x => x.rec && x.rec.completes).slice(-30); count = fin.length ? fin.filter(x => rnd(P.key + x.day + kind) < .2).length : null; }
      else if (counted.length) count = countOver(counted, kind, spec);
      const covered = win ? 'window' : exact ? 'range' : counted.length < nd ? 'range-partial' : 'range';
      const prevCount = Q && !win && (exact || Q.days.every(x => x.day >= countersFrom)) ? countOver(Q.days, kind, spec) : null;
      return { kind, label, count, definition: NOTES[kind], def: NOTES[kind], how: 'Counted from the logged ' + label.toLowerCase() + ' actions.', attribution, coverage: covered, estimated: win, why: win ? ['Only the person\'s newest finished orders were looked at.'] : [], daysCounted: counted.length, daysActive: dc, complete: counted.length === nd,
        per100Orders: count != null && S.orders ? r1(count / S.orders * 100) : null, prev: prevCount, delta: count != null && prevCount != null ? count - prevCount : null, deltaPct: null, better: attribution === 'own' ? 'down' : null, checked: win ? { orders: Math.min(30, evDays.filter(x => x.rec && x.rec.completes).length), of: 30 } : undefined };
    });
    const items = [];
    for (const x of evDays) for (const k of rows) if (k.count != null && k.coverage !== 'window') { const spec = K13.find(q => q[0] === k.kind)[3]; if (per(x.day, k.kind, spec) && x.day >= (spec === 'undos' ? evFrom : countersFrom)) { const o = orders.orders[Math.floor(rnd(P.key + x.day + k.kind + 'o') * orders.orders.length)]; items.push({ id: x.day + k.kind, at: x.rec.firstIn + 2 * HR + rows.indexOf(k) * 90000, day: x.day, rid: o.rid, number: o.number, station: STATIONS[Math.floor(rnd(x.day + k.kind) * 3)][0], kind: k.kind, label: k.label, attribution: k.attribution, note: NOTES[k.kind], source: 'events' }); } }
    items.sort((a, b) => b.at - a.at);
    const nzr = rows.filter(k => k.count != null), total = S.wk.length && nzr.length ? sum(nzr, k => k.count) : null, by = a => (nzr.length ? sum(nzr.filter(k => k.attribution === a), k => k.count) : null);
    const counters = range.filter(x => x.day >= countersFrom && x.rec).length;
    return { total, own: by('own'), system: by('system'), order: by('order'), per100Orders: total != null && S.orders ? r1(total / S.orders * 100) : null, daysCounted: S.wk.length ? counters || null : null, daysActive: S.wk.length, countersFrom, complete: counters === range.filter(x => x.rec).length,
      prevTotal: Q && Q.wk.length && Q.days.every(x => x.day >= countersFrom) ? sum(K13.filter(k => k[3] !== 'win'), k => countOver(Q.days, k[0], k[3])) : null,
      byKind: rows, byDay: S.wk.map(x => ({ day: x.day, total: sum(K13.filter(k => k[3] !== 'win'), k => per(x.day, k[0], k[3])), source: 'counters' })), items: items.slice(0, 40), itemsTotal: items.length, itemsCapped: items.length > 40, next: null };
  }
  function ratesOf(S, Q) {
    const pc = (n, d) => (d ? { p: n / d * 100, n, d } : null);
    const R = (key, label, better, def, fn, cov) => { const v = fn(S), pv = Q ? fn(Q) : null, o = M(label, 'percent', v == null ? null : r1(v.p), pv == null ? null : r1(pv.p), better, def, { coverage: cov || 'range', key }); if (v) { o.numerator = o.num = v.n; o.denominator = o.den = v.d; } o.definition = def; o.why = cov === 'window' ? ['Only the person\'s own presses are seen.'] : []; o.estimated = cov === 'window'; o.daysCounted = S.wk.length; o.daysActive = S.wk.length; return o; };
    return {
      firstPass: R('firstPass', 'First-pass rate', 'up', 'Of the orders finished at a station, the share with no undo, reopen or reprint by the person afterwards.', s => (s.orders ? pc(s.orders - Math.min(s.orders, s.undos + s.rejects), s.orders) : null), 'window'),
      reworkRate: R('reworkRate', 'Rework rate', 'down', 'Undo presses divided by complete presses.', s => (s.completes ? pc(s.undos, s.completes) : null)),
      successRate: R('successRate', 'Error-free actions', 'up', 'The share of logged actions that did not end in an error on screen.', s => (s.scans ? pc(s.scans - s.errors, s.scans) : null)),
      failureRate: R('failureRate', 'Error rate', 'down', 'The share of logged actions that ended in an error on screen.', s => (s.scans ? pc(s.errors, s.scans) : null)),
      holdRate: R('holdRate', 'Hold and cancel rate', null, 'Refusals, holds and flags per 100 orders handled.', s => (s.orders ? pc(s.rejects, s.orders) : null)),
      reprintRate: R('reprintRate', 'Reprint rate', 'down', 'Labels printed again divided by all label prints.', s => (s.prints ? pc(Math.round(s.prints * .04), s.prints) : null)),
      rescanRate: R('rescanRate', 'Repeat scan rate', 'down', 'Scans marked again divided by all scans.', s => (s.scans ? pc(Math.round(s.scans * .02), s.scans) : null))
    };
  }
  function contactOf(P, S, Q) {
    const c = s => { if (!s || !s.wk.length) return null; const sent = sum(s.wk, x => 3 + Math.floor(rnd(P.key + x.day + 'm') * 7)), failed = sum(s.wk, x => (rnd(P.key + x.day + 'x') < .05 ? 1 : 0)), delivered = sent - failed; return { drafted: sent + 2, sent, delivered, unconfirmed: 0, failed, refused: 0, edited: Math.round(sent * .4), aiSentUnchanged: Math.round(sent * .5), deliveryRate: r1(delivered / (delivered + failed) * 100), failureRate: r1(failed / (delivered + failed) * 100), editedShare: 44.4, medianFirstReplyMs: (22 + (s.wk.length % 9)) * 60000, meanFirstReplyMs: (30 + (s.wk.length % 7)) * 60000, conversationsDone: Math.round(sent * .8), reopened: 1, reopenRate: 1.2 }; };
    const a = c(S), b = c(Q);
    const LAB = { drafted: 'Replies drafted', sent: 'Replies sent', delivered: 'Replies delivered', unconfirmed: 'Replies unconfirmed', failed: 'Replies failed', refused: 'Replies refused', edited: 'AI drafts edited', aiSentUnchanged: 'AI drafts sent unchanged', deliveryRate: 'Delivery rate', failureRate: 'Failure rate', editedShare: 'Drafts edited', medianFirstReplyMs: 'Time to first reply (middle)', meanFirstReplyMs: 'Time to first reply (average)', conversationsDone: 'Conversations done', reopened: 'Conversations reopened', reopenRate: 'Reopen rate' };
    const UNIT = { deliveryRate: 'percent', failureRate: 'percent', editedShare: 'percent', reopenRate: 'percent', medianFirstReplyMs: 'ms', meanFirstReplyMs: 'ms' }, BETTER = { deliveryRate: 'up', failureRate: 'down', failed: 'down', refused: 'down', reopenRate: 'down', reopened: 'down' };
    const metrics = {};
    for (const k of Object.keys(LAB)) { const def = LAB[k] + ' (a plain sentence).', o = M(LAB[k], UNIT[k] || 'count', a ? a[k] : null, b && k !== 'medianFirstReplyMs' ? b[k] : null, BETTER[k] || null, def, { key: k, definition: def, coverage: 'counted', daysCounted: S.wk.length, daysActive: S.wk.length }); metrics[k] = o; }
    return Object.assign({ available: !!a, source: 'counters', daysCounted: S.wk.length, daysActive: S.wk.length, firstReplyCount: S.wk.length, metrics }, a || {});
  }
  const CANNOT = [{ topic: 'Phone scans', text: 'A phone scan is credited to the desktop signed in at that station, so it counts for that desktop\'s person.' }, { topic: 'Breaks', text: 'A break and waiting for work look the same in the log: neither has an action.' }];

  /* ── the answers ── */
  function person(b) {
    const P = PEOPLE[String(b.name || '').toLowerCase()], w = windowOf(b), compare = b.compare !== false, pw = compare ? { from: addDays(w.from, -w.days), to: addDays(w.from, -1), days: w.days } : null;
    const head = { ok: true, now: now(), mode: st.mode, name: P ? P.name : String(b.name || ''), range: b.range, from: w.from, to: w.to, days: w.days, today, live: w.to === today, prev: pw, trackingStart, rules: { teamMinPeople: 2, minSignedMin: 15, shortFraction: 0.5, lateAfterMin: 30, activeGapMin: 5 } };
    if (!P) return Object.assign(head, { found: false, spellings: [], granularity: 'day', firstDay: null, eventWindow: null, kpis: {}, series: [], hours: [], stations: [], calendar: [], cannotTell: CANNOT, notes: ['Nothing has been logged under this name yet.'] });
    const S = stats(P, w), Q = pw ? stats(P, pw) : null, se = seriesOf(S, w), tot = S.parts, sig = S.signedMs;
    const stationRows = STATIONS.map(([k, label]) => { const parts = S.wk.length ? Math.round(sum(S.wk, x => x.rec.parts * (x.rec.split[k] || 0))) : null; return { station: k, label, parts, orders: parts == null ? null : Math.round(parts / 2.3), scans: parts == null ? null : Math.round(parts * 2.1), completes: parts == null ? null : Math.round(parts / 2.5), prints: parts == null ? null : Math.round(parts / 4), minutes: S.wk.length ? Math.round(sum(S.wk, x => x.rec.signedMs / 60000 * (x.rec.split[k] || 0))) : null, shareParts: tot ? r1(parts / tot * 100) : null, shareMinutes: sig ? r1(S.wk.length ? sum(S.wk, x => x.rec.signedMs / 60000 * (x.rec.split[k] || 0)) / (sig / 60000) * 100 : 0) : null, perActiveHour: S.activeMs && parts != null ? r1(parts / (S.activeMs / HR * (S.wk.length ? sum(S.wk, x => (x.rec.split[k] || 0)) / S.wk.length : 0))) : null }; }).filter(x => x.parts).sort((a, b) => b.parts - a.parts);
    return Object.assign(head, { found: true, spellings: [P.name], granularity: se.granularity, firstDay, eventWindow: w.days > 31 ? { from: addDays(w.to, -30), to: w.to, days: 31, capped: false } : { from: w.from, to: w.to, days: w.days, capped: false },
      kpis: kpis(S, Q), series: se.series, hours: S.hours.map(h => ({ hour: h.hour, parts: h.parts, scans: h.scans, perDay: S.worked && h.parts != null ? r1(h.parts / S.worked) : null })), stations: stationRows,
      calendar: calendarOf(P, S), attendance: attendanceOf(P, S, Q), issues: issuesOf(P, S, Q, w), rates: ratesOf(S, Q), contact: contactOf(P, S, Q), cannotTell: CANNOT, notes: w.days > 31 ? ['Single-event figures use the newest 31 days of this window.'] : [] });
  }
  /** The console's `live` read (api.md, E2 section 4), for whoever is signed in: Ana M. at Welding, with the first of the orders in hand. */
  function live() {
    const o = orders.orders[0], mode = st.live, at = now(), who = st.who || 'Ana M.';
    const cur = mode === 'working' ? [{ id: 'welding__welding-1__' + who, person: who, device: 'welding-1', deviceLabel: 'Welding 1', kind: 'order', rid: o.rid, orderNumber: o.number, customer: o.customer, title: 'Order ' + o.number, scannedAt: base - 95000, beatAt: at - 2000, note: '', thumbUrl: o.thumbUrl, vectorUrl: '', photoUrl: '', qr: { text: o.rid }, pieces: o.pieces.slice(0, 24), pieceCount: o.pieces.length }] : [];
    return { ok: true, at, mode: st.mode, day: today, keepAliveMs: 30000, staleMs: 180000,
      stations: ALL9.map(([key, label]) => ({ key, label, state: key === 'welding' && mode !== 'out' ? (mode === 'working' ? 'working' : 'idle') : 'offline', people: key === 'welding' && mode !== 'out' ? [who] : [], current: key === 'welding' ? cur : [], devices: [], lastEventAt: at - 5000, counts: { partsToday: 12, ordersToday: 5, scansToday: 30 } })),
      signedIn: mode === 'out' ? [] : [{ name: who, stationKey: 'welding', device: 'welding-1', since: at - 3 * HR, lastSeenAt: at - 2000 }] };
  }
  function answer(body) {
    st.calls.push({ op: body.op, name: body.name, range: body.range, day: body.day, from: body.from, to: body.to, compare: body.compare, q: body.q, cursor: body.cursor, limit: body.limit, sandbox: body.sandbox });
    if (body.key !== st.key) return { status: 401, json: { ok: false, error: 'unauthorized' } };
    if (st.fail > 0) { st.fail--; return { status: 503, json: { ok: false, error: 'both reads failed' } }; }
    if (body.op === 'person') return { status: 200, json: person(body) };
    if (body.op === 'live') return { status: 200, json: live() };
    if (body.op === 'personOrders') { if (PEOPLE[String(body.name || '').toLowerCase()]) orders.state.person = PEOPLE[String(body.name).toLowerCase()].name; return orders.answer(body); }   // (E8's fixture knows one person: every invented person here shares its list)
    return { status: 400, json: { ok: false, error: 'bad op' } };
  }
  return { answer, state: st, orders, person, live, today, firstDay, setLive(m, who) { st.live = m; st.who = who || ''; }, setDelay(f) { st.delay = f; }, delayFor(body) { return st.delay ? +st.delay(body) || 0 : 0; }, ymd, addDays };
}
module.exports = { make, KEY, ymd, addDays };
