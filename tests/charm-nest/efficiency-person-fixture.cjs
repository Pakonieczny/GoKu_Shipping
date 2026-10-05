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
  const today = ymd(base), firstDay = addDays(today, -300), nowH = () => +NYH.format(new Date(now()));
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
      mm('parts', 'Parts', 'pieces', 'up', 'Pieces finished at a station, net of undos.', s => s.parts, { n: S.parts }),
      mm('orders', 'Orders', 'orders', 'up', 'Distinct orders worked on.', s => s.orders),
      mm('ordersCompleted', 'Orders completed', 'orders', 'up', 'Orders finished at a station.', s => s.completes),
      mm('scans', 'Scans', 'scans', 'up', 'Scan actions logged under this name.', s => s.scans, { estimated: true, why: 'A phone scan is credited to the desktop that receives it.' }),
      mm('prints', 'Labels printed', 'prints', null, 'Labels printed.', s => s.prints),
      mm('partsPerDay', 'Parts per day worked', 'pieces/day', 'up', 'Parts divided by days worked.', s => div(s.parts, s.worked)),
      mm('ordersPerDay', 'Orders per day worked', 'orders/day', 'up', 'Orders divided by days worked.', s => div(s.orders, s.worked)),
      mm('partsPerActiveHour', 'Parts per active hour', 'pieces/hour', 'up', 'Parts divided by active hours.', s => div(s.parts, hrs(s))),
      mm('partsPerSignedHour', 'Parts per signed-in hour', 'pieces/hour', 'up', 'Parts divided by signed-in hours.', s => div(s.parts, s.signedMs == null ? null : s.signedMs / HR)),
      mm('bestDay', 'Best day', 'pieces', 'up', 'The day with the most parts.', s => (s.best ? s.best.parts : null), { day: S.best ? S.best.day : undefined }),
      mm('peakHour', 'Busiest hour', 'clock', null, 'The hour with the most parts.', s => (s.peak == null ? null : s.peak * 60)),
      mm('secPerOrderMean', 'Average per order', 'seconds', 'down', 'Active seconds divided by orders.', s => div(s.activeMs == null ? null : s.activeMs / 1000, s.orders)),
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
    return S.days.filter(x => x.rec).map(x => { const c = x.rec, o = { day: x.day, state: c.state }; if (c.state === 'worked' || c.state === 'partial') Object.assign(o, { signedMs: c.signedMs, activeMs: c.activeMs, firstIn: c.firstIn, lastOut: c.lastOut || null, parts: c.parts, orders: c.orders, late: c.late, short: c.short, others: 4 }); return o; });
  }
  function attendanceOf(P, S, Q) {
    const count = (s, st) => (s ? s.days.filter(x => x.rec && x.rec.state === st).length : 0), wd = s => s.days.filter(x => x.rec && x.rec.state === 'off' && dow(x.day) > 0 && dow(x.day) < 7).length;
    const worked = s => s.worked, rate = s => { const t = s.worked + s.off; return t ? r1(s.worked / t * 100) : null; };
    let cur = 0; for (let d = today; d >= firstDay; d = addDays(d, -1)) { const c = dayRec(P, d); if (!c || c.state === 'closed') continue; if (c.state === 'worked' || c.state === 'partial') cur++; else if (c.state !== 'pending') break; }
    let best = 0, run = 0; for (const x of S.days) { const c = x.rec; if (!c || c.state === 'closed') continue; if (c.state === 'worked' || c.state === 'partial') { run++; best = Math.max(best, run); } else if (c.state !== 'pending') run = 0; }
    const m = (label, unit, v, pv, better, def) => M(label, unit, v, pv, better, def);
    const lateN = s => s.wk.filter(x => x.rec.late).length, shortN = s => s.wk.filter(x => x.rec.short).length;
    return { metrics: {
      daysWorked: m('Days worked', 'days', worked(S), Q ? worked(Q) : null, 'up', 'Days signed in at a station.'),
      daysOff: m('Days off', 'days', S.off, Q ? Q.off : null, 'down', 'Team working days with no sign-in.'),
      weekdayDaysOff: m('Weekday days off', 'days', wd(S), Q ? wd(Q) : null, 'down', 'Days off that fell Monday to Friday.'),
      lateDays: m('Late starts', 'days', lateN(S), Q ? lateN(Q) : null, 'down', 'First sign-in more than 30 minutes after the team\'s usual start.'),
      shortDays: m('Short days', 'days', shortN(S), Q ? shortN(Q) : null, 'down', 'Signed in for under half of what the team did.'),
      attendanceRate: m('Attendance', 'percent', rate(S), Q ? rate(Q) : null, 'up', 'Days worked as a share of the team\'s working days.'),
      currentStreak: m('Current streak', 'days', cur, null, 'up', 'Working days in a row, up to today.'),
      longestStreak: m('Longest streak', 'days', best, null, null, 'The most working days in a row in this period.')
    }, avgShiftMs: S.worked ? Math.round(S.signedMs / S.worked) : null };
  }
  const KINDS = [['rejected', 'Piece rejected', 'order', 'rejects', 'A piece was rejected at a station.'], ['error', 'Error shown', 'system', 'errors', 'The station showed an error while this person worked.'], ['undone', 'Completion undone', 'own', 'undos', 'A completion was undone.']];
  function issuesOf(P, S, Q, w) {
    const items = [], by = KINDS.map(([kind, label, attribution, key, def]) => ({ kind, label, attribution, key, def, count: S.wk.length ? sum(S.wk, x => x.rec[key]) : 0, prevCount: Q && Q.wk.length ? sum(Q.wk, x => x.rec[key]) : null }));
    for (const x of S.wk) for (const k of by) if (x.rec[k.key]) { const o = orders.orders[Math.floor(rnd(P.key + x.day + k.kind) * orders.orders.length)]; items.push({ at: x.rec.firstIn + 2 * HR + by.indexOf(k) * 600000, day: x.day, rid: o.rid, number: o.number, kind: k.kind, label: k.label, station: STATIONS[Math.floor(rnd(x.day + k.kind) * 3)][0], note: k.kind === 'rejected' ? 'Scratched on the back' : k.kind === 'error' ? 'Scan did not match the order' : '' }); }
    items.sort((a, b) => b.at - a.at); const total = sum(by, k => k.count), ord = S.orders;
    const byKind = by.filter(k => k.count).map(k => ({ kind: k.kind, label: k.label, count: k.count, per100Orders: ord ? r1(k.count / ord * 100) : null, coverage: w.days > 31 ? 'window' : 'range', attribution: k.attribution, def: k.def, how: 'Counted from the logged ' + k.label.toLowerCase() + ' actions.', estimated: false }));
    const pt = Q && Q.wk.length ? sum(by, k => k.prevCount || 0) : null;
    return { total: S.wk.length ? total : null, own: S.wk.length ? by.filter(k => k.attribution === 'own').reduce((n, k) => n + k.count, 0) : null, system: S.wk.length ? by.filter(k => k.attribution === 'system').reduce((n, k) => n + k.count, 0) : null, per100Orders: ord ? r1(total / ord * 100) : null, prevTotal: pt,
      byKind, items: items.slice(0, 40), itemsCapped: items.length > 40 };
  }
  function ratesOf(S, Q) {
    const R = (label, better, def, fn) => { const v = fn(S), pv = Q ? fn(Q) : null; return M(label, 'percent', v == null ? null : r1(v), pv == null ? null : r1(pv), better, def); };
    const fp = s => (s.orders ? (s.orders - Math.min(s.orders, s.rejects + s.errors + s.undos)) / s.orders * 100 : null);
    const rev = s => (s.completes ? s.undos / s.completes * 100 : null), rej = s => (s.orders ? s.rejects / s.orders * 100 : null);
    const out = { firstPassRate: R('First-pass success', 'up', 'Orders with no reject, error or undo, as a share of orders.', fp), reversalRate: R('Reversed work', 'down', 'Completions that were undone, as a share of completions.', rev), rejectRate: R('Rejected', 'down', 'Rejects as a share of orders.', rej) };
    if (S.orders) { out.firstPassRate.num = S.orders - Math.min(S.orders, S.rejects + S.errors + S.undos); out.firstPassRate.den = S.orders; out.reversalRate.num = S.undos; out.reversalRate.den = S.completes; out.rejectRate.num = S.rejects; out.rejectRate.den = S.orders; }
    return out;
  }
  function contactOf(P, S, Q) {
    const c = s => { if (!s || !s.wk.length) return null; const sent = sum(s.wk, x => 3 + Math.floor(rnd(P.key + x.day + 'm') * 7)), failed = sum(s.wk, x => (rnd(P.key + x.day + 'x') < .05 ? 1 : 0)); return { sent, failed, delivered: sent - failed, drafted: sent + 2, edited: Math.round(sent * .4), first: 22 + (s.wk.length % 9) }; };
    const a = c(S), b = c(Q); const m = (label, unit, k, better, def) => M(label, unit, a ? a[k] : null, b ? b[k] : null, better, def);
    return { available: true, source: 'Inbox activity', metrics: {
      repliesSent: m('Replies sent', 'count', 'sent', 'up', 'Replies sent to customers from the Inbox.'), repliesDelivered: m('Replies delivered', 'count', 'delivered', 'up', 'Replies Etsy confirmed.'), repliesFailed: m('Replies failed', 'count', 'failed', 'down', 'Replies that did not go through.'),
      repliesDrafted: m('Drafts made', 'count', 'drafted', null, 'AI drafts asked for.'), repliesEdited: m('AI drafts edited', 'count', 'edited', null, 'Drafts changed before sending.'), timeToFirstReplyMin: m('First reply', 'minutes', 'first', 'down', 'The middle time to a first reply.')
    } };
  }
  const CANNOT = [{ topic: 'Phone scans', text: 'A phone scan is credited to the desktop signed in at that station, so it counts for that desktop\'s person.' }, { topic: 'Breaks', text: 'A break and waiting for work look the same in the log: neither has an action.' }];

  /* ── the answers ── */
  function person(b) {
    const P = PEOPLE[String(b.name || '').toLowerCase()], w = windowOf(b), compare = b.compare !== false, pw = compare ? { from: addDays(w.from, -w.days), to: addDays(w.from, -1), days: w.days } : null;
    const head = { ok: true, now: now(), mode: st.mode, name: P ? P.name : String(b.name || ''), range: b.range, from: w.from, to: w.to, days: w.days, today, live: w.to === today, prev: pw, trackingStart: addDays(today, -250), rules: { teamMinPeople: 2, minSignedMin: 15, shortFraction: 0.5, lateAfterMin: 30, activeGapMin: 5 } };
    if (!P) return Object.assign(head, { found: false, spellings: [], granularity: 'day', firstDay: null, eventWindow: null, kpis: {}, series: [], hours: [], stations: [], calendar: [], cannotTell: CANNOT, notes: ['Nothing has been logged under this name yet.'] });
    const S = stats(P, w), Q = pw ? stats(P, pw) : null, se = seriesOf(S, w), tot = S.parts, sig = S.signedMs;
    const stationRows = STATIONS.map(([k, label]) => { const parts = S.wk.length ? Math.round(sum(S.wk, x => x.rec.parts * (x.rec.split[k] || 0))) : null; return { station: k, label, parts, orders: parts == null ? null : Math.round(parts / 2.3), scans: parts == null ? null : Math.round(parts * 2.1), completes: parts == null ? null : Math.round(parts / 2.5), prints: parts == null ? null : Math.round(parts / 4), minutes: S.wk.length ? Math.round(sum(S.wk, x => x.rec.signedMs / 60000 * (x.rec.split[k] || 0))) : null, shareParts: tot ? r1(parts / tot * 100) : null, shareMinutes: sig ? r1(S.wk.length ? sum(S.wk, x => x.rec.signedMs / 60000 * (x.rec.split[k] || 0)) / (sig / 60000) * 100 : 0) : null, perActiveHour: S.activeMs && parts != null ? r1(parts / (S.activeMs / HR * (S.wk.length ? sum(S.wk, x => (x.rec.split[k] || 0)) / S.wk.length : 0))) : null }; }).filter(x => x.parts).sort((a, b) => b.parts - a.parts);
    return Object.assign(head, { found: true, spellings: [P.name], granularity: se.granularity, firstDay, eventWindow: w.days > 31 ? { from: addDays(w.to, -30), to: w.to, days: 31, capped: false } : { from: w.from, to: w.to, days: w.days, capped: false },
      kpis: kpis(S, Q), series: se.series, hours: S.hours.map(h => ({ hour: h.hour, parts: h.parts, scans: h.scans, perDay: S.worked && h.parts != null ? r1(h.parts / S.worked) : null })), stations: stationRows,
      calendar: calendarOf(P, S), attendance: attendanceOf(P, S, Q), issues: issuesOf(P, S, Q, w), rates: ratesOf(S, Q), contact: contactOf(P, S, Q), cannotTell: CANNOT, notes: w.days > 31 ? ['Single-event figures use the newest 31 days of this window.'] : [] });
  }
  /** The console's `live` read (api.md, E2 section 4), for whoever is signed in: Ana M. at Welding, with the first of the orders in hand. */
  function live() {
    const o = orders.orders[0], mode = st.live, at = now();
    const cur = mode === 'working' ? [{ id: 'welding__welding-1__Ana M.', person: 'Ana M.', device: 'welding-1', deviceLabel: 'Welding 1', kind: 'order', rid: o.rid, orderNumber: o.number, customer: o.customer, title: 'Order ' + o.number, scannedAt: base - 95000, beatAt: at - 2000, note: '', thumbUrl: o.thumbUrl, vectorUrl: '', photoUrl: '', qr: { text: o.rid }, pieces: o.pieces.slice(0, 24), pieceCount: o.pieces.length }] : [];
    return { ok: true, at, mode: st.mode, day: today, keepAliveMs: 30000, staleMs: 180000,
      stations: ALL9.map(([key, label]) => ({ key, label, state: key === 'welding' && mode !== 'out' ? (mode === 'working' ? 'working' : 'idle') : 'offline', people: key === 'welding' && mode !== 'out' ? ['Ana M.'] : [], current: key === 'welding' ? cur : [], devices: [], lastEventAt: at - 5000, counts: { partsToday: 12, ordersToday: 5, scansToday: 30 } })),
      signedIn: mode === 'out' ? [] : [{ name: 'Ana M.', stationKey: 'welding', device: 'welding-1', since: at - 3 * HR, lastSeenAt: at - 2000 }] };
  }
  function answer(body) {
    st.calls.push({ op: body.op, name: body.name, range: body.range, day: body.day, from: body.from, to: body.to, compare: body.compare, q: body.q, cursor: body.cursor, limit: body.limit, sandbox: body.sandbox });
    if (body.key !== st.key) return { status: 401, json: { ok: false, error: 'unauthorized' } };
    if (st.fail > 0) { st.fail--; return { status: 503, json: { ok: false, error: 'both reads failed' } }; }
    if (body.op === 'person') return { status: 200, json: person(body) };
    if (body.op === 'live') return { status: 200, json: live() };
    if (body.op === 'personOrders') return orders.answer(body);
    return { status: 400, json: { ok: false, error: 'bad op' } };
  }
  return { answer, state: st, orders, person, live, today, firstDay, setLive(m) { st.live = m; }, setDelay(f) { st.delay = f; }, delayFor(body) { return st.delay ? +st.delay(body) || 0 : 0; }, ymd, addDays };
}
module.exports = { make, KEY, ymd, addDays };
