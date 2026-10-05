/*  netlify/functions/_employeeAttendance.js
 *  ─────────────────────────────────────────────────────────────────────────────
 *  Days worked, days off, short days, late starts, shift lengths and streaks of ONE person (Paul, 5 Oct 2026: "statistics
 *  such as performance, days off, based on not being logged in ..."). A helper for the employee page's `person` op; it is
 *  not an endpoint and has no gate of its own: whoever calls it has already passed the manager passcode.
 *
 *      const { attendance } = require("./_employeeAttendance");
 *      const result = await attendance(ctx, { name, from, to });     // from, to = New York days "YYYY-MM-DD"
 *
 *  WHAT IT READS (never single events, never anything that grows with the number of events):
 *      Efficiency_Daily  one range query over the days  (the rollups the stations keep: first/last action, pieces, orders)
 *      Station_Sessions  one range query over the days  (sign-ins: start, last beat, end, why it ended)
 *      config/employeeAliases (names that are one person), config/employeeSchedule (optional shop calendar): one small
 *      document each, kept a minute.
 *  and only the days it needs: from the older of `from` and 89 days before `to` (so a person's "usual" shift can be
 *  learned even for a one-day view), never before the day logging began, never later than today.
 *
 *  THE CONTEXT. Anything with these members works (all optional except as noted):
 *      ctx.db           a Firestore handle: used to read the two collections above when the rows are not handed in
 *      ctx.prefix       "" (real) or "Sandbox_"  (or ctx.mode "real" | "sandbox", or ctx.sandbox === true): never mixed
 *      ctx.admin        the firebase-admin module (only for Firestore Timestamps in the sessions query)
 *      ctx.now, ctx.today   a clock for tests; default Date.now() and its New York day
 *      ctx.rollups      the Efficiency_Daily documents as stored, an array (or ctx.loadRollups(fromDay, toDay) -> array | {docs})
 *      ctx.sessions     the Station_Sessions documents as stored, an array (or ctx.loadSessions(fromMs, toMs) -> array | {rows})
 *      ctx.rowsFrom     with arrays: the first day the arrays cover (default `from`; the learning window shrinks to it)
 *      ctx.keyOf(name)  one string per person (aliases and spelling merged); else ctx.aliases ({display, map} as
 *                       employeeEfficiency.js builds it), else config/employeeAliases read here
 *      ctx.schedule     { closedDays, openDays, closedWeekdays } (see below); else config/employeeSchedule when ctx.db is set
 *      ctx.trackingStart  the first day sign-in logging counts from (default 2026-10-02)
 *
 *  THE ANSWER and every rule behind it: plans/employee-hr/api.md ("E9: attendance"); every field also carries its own
 *  plain-words line in `definitions`. Honest by construction: the data shows LOGGED sign-ins and work, not true attendance.
 *  No PIN, passcode or digits-only name is ever read into the answer (the session's employeeId field is never touched).
 *  ───────────────────────────────────────────────────────────────────────────── */
"use strict";

/* ── rules (all in one place; every one is echoed in the answer under `rules`) ── */
const RULES = Object.freeze({
  teamMinPeople: 2,          // a working day needs at least this many people in (signed in or recorded work)
  minSignedMin: 15,          // someone counts as "in" for the team after this long signed in (or after any recorded work)
  shortFraction: 0.5,        // short = under this share of the person's own median shift
  shortFallbackMin: 120,     // ... or under this many minutes until the person has minShiftsForUsual shifts
  shortFloorMin: 30,         // the short limit is never lower than this
  minShiftsForUsual: 5,      // shifts needed before a person's own "usual" start and shift are trusted
  lateAfterMin: 30,          // late = first sign-in later than the usual start by more than this
  baselineDays: 90,          // learn usual values from this many days ending at `to` (at least)
  usualWeekdayShare: 0.5     // a weekday is a usual working day when at least this share of its past days had a team
});
const LOGGING_START = "2026-10-02";           // sign-in logging and activity rollups began on this New York day
const MAX_DAYS = 366, MAX_FUTURE_DAYS = 62, MAX_DAY_LIST = 800;
const DAY_MS = 86400000, GONE_MS = 15 * 60000;
const COL = { rollup: "Efficiency_Daily", sessions: "Station_Sessions" };
const LIM = { rollups: 6000, sessions: 9000 };
const TTL_LIVE = 5000, TTL_PAST = 10 * 60000, TTL_CONFIG = 60000;
const DAY_RE = /^\d{4}-\d{2}-\d{2}$/;
const WEEKDAYS = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"];
const WEEK_ORDER = [1, 2, 3, 4, 5, 6, 0];     // Monday first

/* ── plain-words definitions (shown in hovers; keep them short and true) ── */
const DEFINITIONS = Object.freeze({
  notAttendance: "These numbers come from logged sign-ins and logged work, not from a time clock: a phone scan is credited to the desktop's signed-in person, and someone can work without signing in, so a day off here means not logged in, not proof of absence.",
  workingDay: "A working day is a New York day on which the team worked: at least 2 people signed in or recorded work, on a day the team usually works (Monday to Friday, and Saturday or Sunday only once the team has been working it regularly). Weekends and holidays the team did not work are closed, never days off; a person who works one gets it as an extra day.",
  workingDays: "How many working days there are in the period. Days still to come, today before the person is in, days before the person's first record and days before logging began are left out.",
  daysWorked: "Working days on which the person signed in or recorded any work. A short day still counts as worked.",
  daysOff: "Working days on which the person never signed in and recorded nothing.",
  extraDays: "Days the person signed in or recorded work on a day that is not a working day (a Saturday, a holiday, a day with fewer than 2 people in). They are not counted as working days and never as days off.",
  shortDays: "Working days on which the person was signed in for less than half of their own usual shift (their median). Until 5 shifts are logged, less than 2 hours.",
  lateDays: "Working days on which the first sign-in came more than 30 minutes after the person's own usual start (their median first sign-in). Needs 5 logged shifts.",
  attendanceRate: "Days worked divided by working days, as a percent.",
  avgShiftMs: "Average time signed in on the days worked. Days still running and days whose length is unknown are left out.",
  medianShiftMs: "The middle shift length: half of the days were shorter, half longer.",
  medianStart: "The middle first sign-in time of the days worked: half of the days began earlier, half later.",
  medianEnd: "The middle last sign-out time of the finished days worked. After the midnight auto sign-out the end is taken as the person's last recorded action.",
  streaks: "Days worked in a row. Closed days are skipped and do not break a streak; a day off does. Current is the streak up to the end of the period; best is the longest in the days looked at (the period plus up to 90 days before it).",
  byWeekday: "The same counts for each day of the week, to show patterns. A weekday that is always off is probably not that person's day (part time), not an absence.",
  estimated: "An estimate is used when the system signed the person out at midnight (their time is then cut back to their last recorded action), or when there was no sign-in and the time comes from recorded actions only.",
  signedMs: "Time signed in that day; two computers at once count once.",
  firstIn: "The first sign-in of the day (or the first recorded action, if that came first).",
  lastOut: "The last sign-out of the day, or the last recorded action after the midnight auto sign-out. Empty while still signed in or when it cannot be told.",
  parts: "Pieces finished that day (anything undone taken off), from the activity record. Empty when nothing was recorded.",
  orders: "Orders worked that day, from the activity record. Empty when nothing was recorded.",
  others: "How many other people were in that day."
});
const STATES = Object.freeze([
  { state: "worked", label: "Worked", counted: "worked", def: "Signed in or recorded work on a working day." },
  { state: "partial", label: "Short day", counted: "worked", def: "Signed in on a working day, but for less than half of the person's own usual shift (until 5 shifts are logged: under 2 hours)." },
  { state: "off", label: "Day off", counted: "off", def: "A working day on which the person never signed in and recorded nothing." },
  { state: "closed", label: "Closed", counted: null, def: "The team did not work this day (weekend or holiday). Never a day off." },
  { state: "pending", label: "Today", counted: null, def: "Today, and the person is not in yet. Not counted until the day is over." },
  { state: "future", label: "Not yet", counted: null, def: "A day still to come." },
  { state: "before", label: "Not started", counted: null, def: "Earlier than the person's first logged day. Not counted." },
  { state: "unknown", label: "Not tracked", counted: null, def: "Before sign-in logging began (2 Oct 2026). It cannot be told whether anyone worked, so it is never a day off." }
]);

/* ── small helpers ── */
const num = v => Number.isFinite(+v) ? +v : 0;
const ms = v => v == null ? 0 : typeof v.toMillis === "function" ? v.toMillis() : v instanceof Date ? v.getTime() : Number.isFinite(+v) ? +v : 0;
const isObj = v => !!v && typeof v === "object" && !Array.isArray(v);
const r1 = x => Math.round(x * 10) / 10;
const cleanName = v => String(v == null ? "" : v).replace(/[\u0000-\u001f\u007f]/g, " ").replace(/\s+/g, " ").trim().slice(0, 80);
const okName = n => !!n && /\p{L}/u.test(n);                     // a name with no letter ("123456") is a PIN, never a person
const fold = n => cleanName(n).normalize("NFD").replace(/[̀-ͯ]/g, "").toLowerCase()
  .replace(/['‘’`´]/g, "").replace(/[^\p{L}\p{N}]+/gu, " ").trim();
function niceName(raw) {
  const s = cleanName(raw).replace(/_+/g, " ").replace(/\s+/g, " ").trim();
  if (!s) return "";
  let words = s.split(" ");
  if (/\p{L}{3}/u.test(s) && (s === s.toLowerCase() || s === s.toUpperCase()))
    words = words.map(w => w.toLowerCase().replace(/(^|[-'’.])(\p{L})/gu, (_, a, b) => a + b.toUpperCase()));
  if (words.length > 1) words = words.map(w => /^\p{L}$/u.test(w) ? w + "." : w);
  return words.join(" ");
}
function bestForm(forms) {
  let best = "", score = -1;
  for (const [form, n] of forms) { const v = n * 2 + (form !== form.toLowerCase() && form !== form.toUpperCase() ? 1 : 0); if (v > score || (v === score && form < best)) { best = form; score = v; } }
  return best;
}
const BUILTIN_ALIASES = { "Giovanna": ["Giovanna C."] };     // the same seed the console uses
function buildAliases(extra) {
  const display = new Map(), map = new Map();
  const add = (name, list) => {
    const d = cleanName(name); if (!okName(d)) return;
    const ck = fold(d); if (!display.has(ck)) display.set(ck, d);
    if (!map.has(ck)) map.set(ck, ck);
    for (const a of Array.isArray(list) ? list.slice(0, 50) : []) { const an = cleanName(a); if (okName(an)) map.set(fold(an), ck); }
  };
  for (const [k, v] of Object.entries(BUILTIN_ALIASES)) add(k, v);
  if (isObj(extra)) for (const [k, v] of Object.entries(extra).slice(0, 200)) add(k, v);
  return { display, map };
}
function keyFn(aliases) {
  return name => {
    let k = fold(name); const m = aliases && aliases.map;
    if (m) for (let i = 0; i < 3 && m.has(k) && m.get(k) !== k; i++) k = m.get(k);
    return k;
  };
}
function median(a) {
  if (!a.length) return null;
  const s = a.slice().sort((x, y) => x - y), m = s.length >> 1;
  return s.length % 2 ? s[m] : (s[m - 1] + s[m]) / 2;
}
const mean = a => a.length ? a.reduce((n, v) => n + v, 0) / a.length : null;
const clockText = min => { if (min == null) return null; const t = Math.round(min), h = Math.floor(t / 60) % 24, m = t % 60; return `${h % 12 || 12}:${String(m).padStart(2, "0")} ${h < 12 ? "AM" : "PM"}`; };

/* ── New York days (the shop's midnight, daylight saving included) ── */
const partsFmt = new Intl.DateTimeFormat("en-US", { timeZone: "America/New_York", hourCycle: "h23", year: "numeric", month: "numeric", day: "numeric", hour: "numeric", minute: "numeric", second: "numeric" });
function nyParts(t) { const o = {}; for (const p of partsFmt.formatToParts(new Date(t))) if (p.type !== "literal") o[p.type] = +p.value; return o; }
const pad = n => String(n).padStart(2, "0");
const nyDay = t => { const p = nyParts(t); return `${p.year}-${pad(p.month)}-${pad(p.day)}`; };
const nyMinutes = t => { const p = nyParts(t); return (p.hour % 24) * 60 + p.minute; };
const midnights = new Map();
function nyMidnight(day) {
  let t = midnights.get(day); if (t != null) return t;
  const [y, m, d] = day.split("-").map(Number), want = Date.UTC(y, m - 1, d); t = want;
  for (let i = 0; i < 3; i++) { const p = nyParts(t); t += want - Date.UTC(p.year, p.month - 1, p.day, p.hour % 24, p.minute, p.second); }
  if (midnights.size > 900) midnights.clear();
  midnights.set(day, t); return t;
}
const addDays = (day, n) => { const [y, m, d] = day.split("-").map(Number); return new Date(Date.UTC(y, m - 1, d + n)).toISOString().slice(0, 10); };
const validDay = s => typeof s === "string" && DAY_RE.test(s) && addDays(s, 0) === s;
const weekdayOf = day => { const [y, m, d] = day.split("-").map(Number); return new Date(Date.UTC(y, m - 1, d)).getUTCDay(); };
function dayList(from, to) { const out = []; for (let d = from, i = 0; d <= to && i < MAX_DAY_LIST; d = addDays(d, 1), i++) out.push(d); return out; }
const daysBetween = (a, b) => Math.round((Date.parse(b + "T00:00:00Z") - Date.parse(a + "T00:00:00Z")) / DAY_MS);

/* Milliseconds covered by [s, e] pairs, an overlap counted once (one person at two computers). */
function covered(spans) {
  let total = 0, a = -Infinity, z = -Infinity;
  for (const [s, e] of spans.slice().sort((x, y) => x[0] - y[0])) { if (s > z) { if (z > a) total += z - a; a = s; z = e; } else z = Math.max(z, e); }
  if (z > a) total += z - a;
  return total;
}
/** One sign-in session as a span of time. An open session whose last beat is 15+ minutes old ended at that beat. */
function spanOf(s, now) {
  const start = ms(s.startAt); if (!(start > 0)) return null;
  const last = Math.max(start, ms(s.lastSeenAt) || start);
  let end = ms(s.endAt), live = false;
  if (!end) { if (now - last >= GONE_MS) end = last; else { end = now; live = true; } }
  end = Math.min(end, now); if (end < start) end = start;
  return { start, end, live, reason: String(s.endReason || "") };
}
/** The pieces of a span inside the New York days from..to. `auto` = the piece ran into the midnight auto sign-out. */
function clip(sp, from, to) {
  const out = []; let d = nyDay(sp.start);
  for (let i = 0; i < 45; i++) {
    const ds = nyMidnight(d), de = nyMidnight(addDays(d, 1)), a = Math.max(sp.start, ds), z = Math.min(sp.end, de);
    if (d >= from && d <= to && (i === 0 ? z >= a : z > a)) out.push({ day: d, s: a, e: z, live: sp.live && z >= sp.end, auto: !sp.live && (z >= de - 1000 || (sp.reason === "midnight" && z >= sp.end)) });
    if (sp.end <= de || d >= to) break;
    d = addDays(d, 1);
  }
  return out;
}

/* ── the person-days ──────────────────────────────────────────────────────── */
const newPD = day => ({ day, act: false, events: 0, first: 0, last: 0, parts: 0, orders: 0, touched: new Set(), spans: [] });

/** Rows → Map(personKey → Map(day → person-day)), spellings seen per person. Rows before `start` or outside `win` are ignored. */
function ingest(rows, keyOf, now, win, sandbox) {
  const by = new Map(), forms = new Map();
  const get = (name, day) => {
    const key = keyOf(name); if (!key) return null;
    let f = forms.get(key); if (!f) forms.set(key, f = new Map()); f.set(name, (f.get(name) || 0) + 1);
    let m = by.get(key); if (!m) by.set(key, m = new Map());
    let pd = m.get(day); if (!pd) m.set(day, pd = newPD(day));
    return pd;
  };
  for (const x of rows.rollups || []) {
    if (!isObj(x) || !validDay(x.day) || x.day < win.from || x.day > win.to || !!x.sandbox !== sandbox) continue;
    const name = cleanName(x.person); if (!okName(name)) continue;
    let fa = ms(x.firstAt), la = ms(x.lastAt), net = 0, ord = 0, any = num(x.events) > 0;
    for (const v of Object.values(isObj(x.stations) ? x.stations : {})) {
      if (!isObj(v)) continue;
      any = true;
      net += Math.max(0, num(v.parts) - num(v.undoParts)); ord += Math.max(0, num(v.orders) - num(v.undoOrders));
      const f = ms(v.firstAt), l = ms(v.lastAt);
      if (f > 0 && (!fa || f < fa)) fa = f;
      if (l > la) la = l;
    }
    if (!any && !(fa > 0)) continue;                              // an empty rollup is no activity
    const pd = get(name, x.day); if (!pd) continue;
    pd.act = true; pd.events += Math.max(0, num(x.events)); pd.parts += net; pd.orders += ord;
    if (fa > 0 && (!pd.first || fa < pd.first)) pd.first = fa;
    if (la > pd.last) pd.last = la;
    if (isObj(x.touched)) for (const id of Object.keys(x.touched)) { const o = String(id).replace(/\D/g, ""); if (o) pd.touched.add(o); }
  }
  for (const s of rows.sessions || []) {
    if (!isObj(s)) continue;
    const name = cleanName(s.person); if (!okName(name)) continue;
    const sp = spanOf(s, now); if (!sp) continue;
    for (const c of clip(sp, win.from, win.to)) { const pd = get(name, c.day); if (pd) pd.spans.push(c); }
  }
  return { by, forms };
}

/** One person-day made ready: signed time (a midnight auto sign-out cut back to the last recorded action), first in, last out. */
function finish(pd, rules) {
  const spans = pd.spans.slice().sort((a, b) => a.s - b.s);
  let est = false, known = true, live = false, firstS = Infinity, lastE = 0, midnight = false;
  const iv = [];
  for (const sp of spans) {
    let e = sp.e;
    if (sp.live) live = true;
    else if (sp.auto) {
      midnight = true; est = true;
      if (pd.last >= sp.s) e = Math.min(pd.last, sp.e);          // signed out by the clock: the day ended at the last recorded action
      else { e = sp.s; known = false; }                           // nothing was recorded after signing in: the length cannot be told
    }
    iv.push([sp.s, e]);
    if (sp.s < firstS) firstS = sp.s;
    if (e > lastE) lastE = e;
  }
  const hasSession = spans.length > 0;
  let signed = covered(iv);
  if (!hasSession && pd.act) { signed = pd.first > 0 && pd.last >= pd.first ? pd.last - pd.first : 0; est = true; }   // recorded work, no sign-in: the span of the work
  const firstIn = Math.min(firstS, pd.first > 0 ? pd.first : Infinity);
  pd.present = hasSession || pd.act;
  pd.signedMs = known ? signed : (signed > 0 ? signed : null);
  pd.lengthKnown = known;
  pd.est = est;
  pd.firstIn = Number.isFinite(firstIn) ? firstIn : null;
  pd.startFromWork = !hasSession && pd.first > 0;
  pd.lastOut = live || !known ? null : (Math.max(lastE, pd.last) || null);
  pd.endedBy = live ? "open" : !hasSession ? (pd.act ? "activity" : null) : !known ? "midnight" : midnight ? "midnight" : "signOut";
  pd.live = live;
  pd.teamPresent = pd.act || (pd.signedMs || 0) >= rules.minSignedMin * 60000;
  pd.partsOut = pd.act ? pd.parts : null;
  pd.ordersOut = pd.act ? (pd.touched.size || pd.orders) : null;
  return pd;
}

/* ── the schedule (optional config/employeeSchedule) ── */
function cleanSchedule(raw) {
  const days = v => new Set((Array.isArray(v) ? v : []).filter(validDay).slice(0, 800));
  const wd = (Array.isArray(raw && raw.closedWeekdays) ? raw.closedWeekdays : []).map(Number).filter(n => Number.isInteger(n) && n >= 0 && n <= 6);
  return { closed: days(raw && raw.closedDays), open: days(raw && raw.openDays), closedWeekdays: new Set(wd), configured: !!raw };
}

/* ── the computation (pure: rows in, answer out) ──────────────────────────── */
/**
 * @param m { target, display, found?, from, to, today, now, rows:{rollups,sessions}, keyOf, schedule, trackingStart,
 *            baseFrom, sandbox, mode, forms? }
 */
function compute(m) {
  const rules = Object.assign({}, RULES, m.rules || {});
  const { from, to, today } = m;
  const trackingStart = validDay(m.trackingStart) ? m.trackingStart : LOGGING_START;
  const sched = m.schedule instanceof Object && m.schedule.closed instanceof Set ? m.schedule : cleanSchedule(m.schedule);
  const baseFrom = m.baseFrom && m.baseFrom < from ? m.baseFrom : from;
  const readFrom = baseFrom > trackingStart ? baseFrom : trackingStart;          // nothing before logging began is used
  const readTo = to < today ? to : today;
  const { by, forms } = ingest(m.rows, m.keyOf, m.now, { from: readFrom, to: readTo }, !!m.sandbox);
  for (const mp of by.values()) for (const pd of mp.values()) finish(pd, rules);

  // who was in, per day (everybody, the person included)
  const team = new Map();
  for (const mp of by.values()) for (const [d, pd] of mp) if (pd.teamPresent) team.set(d, (team.get(d) || 0) + 1);
  // learn the team's rhythm from the days that are over
  const yesterday = addDays(today, -1);
  const learnDays = readFrom <= (to < yesterday ? to : yesterday) ? dayList(readFrom, to < yesterday ? to : yesterday) : [];
  const occ = [0, 0, 0, 0, 0, 0, 0], good = [0, 0, 0, 0, 0, 0, 0];
  for (const d of learnDays) {
    if (sched.closed.has(d)) continue;
    const w = weekdayOf(d); occ[w]++; if ((team.get(d) || 0) >= rules.teamMinPeople) good[w]++;
  }
  // Monday to Friday are usual working days (a weekday with fewer than 2 people in is simply closed); Saturday and Sunday become usual
  // only when the team has really been working them (at least half of at least two of them had a team in)
  const usual = [0, 1, 2, 3, 4, 5, 6].map(w => sched.closedWeekdays.size ? !sched.closedWeekdays.has(w)
    : w >= 1 && w <= 5 ? true : occ[w] >= 2 && good[w] / occ[w] >= rules.usualWeekdayShare);
  const teamDay = d => {
    if (sched.open.has(d)) return true;
    if (sched.closed.has(d) || sched.closedWeekdays.has(weekdayOf(d))) return false;
    return usual[weekdayOf(d)] && (team.get(d) || 0) >= rules.teamMinPeople;      // a day the team does not usually work is never a working day
  };

  const mine = by.get(m.target) || new Map();
  let firstDay = null;
  for (const d of dayList(readFrom, readTo)) { const pd = mine.get(d); if (pd && pd.present) { firstDay = d; break; } }
  // the person's own usual shift and start, from the finished working days they were in
  const shifts = [], starts = [];
  for (const d of learnDays) {
    const pd = mine.get(d);
    if (!pd || !pd.present || !pd.lengthKnown || !(pd.signedMs >= rules.minSignedMin * 60000) || !teamDay(d)) continue;
    shifts.push(pd.signedMs); if (pd.firstIn) starts.push(nyMinutes(pd.firstIn));
  }
  const trusted = shifts.length >= rules.minShiftsForUsual;
  const usualShiftMs = trusted ? median(shifts) : null, usualStartMin = trusted && starts.length >= rules.minShiftsForUsual ? Math.round(median(starts)) : null;
  const shortBelowMs = trusted ? Math.max(rules.shortFloorMin * 60000, rules.shortFraction * usualShiftMs) : rules.shortFallbackMin * 60000;

  // the state of every day, baseline included (streaks use it)
  const all = [];
  for (const d of dayList(baseFrom, to)) {
    const pd = mine.get(d) || null, c = { day: d, state: "", signedMs: 0, firstIn: null, lastOut: null, parts: null, orders: null, others: null,
      late: false, short: false, extra: false, estimated: false, lengthKnown: true, endedBy: null, _startFromWork: false };
    if (d > today) { c.state = "future"; all.push(c); continue; }
    if (d < trackingStart) { c.state = "unknown"; c.note = "Before sign-in logging began (" + trackingStart + ")."; all.push(c); continue; }
    const isToday = d === today, present = !!(pd && pd.present), tDay = teamDay(d);
    c.others = Math.max(0, (team.get(d) || 0) - (pd && pd.teamPresent ? 1 : 0));
    if (present) {
      c.signedMs = pd.signedMs; c.firstIn = pd.firstIn; c.lastOut = pd.lastOut; c.parts = pd.partsOut; c.orders = pd.ordersOut;
      c.estimated = pd.est; c.endedBy = pd.endedBy; c.lengthKnown = pd.lengthKnown; c._startFromWork = pd.startFromWork;
      if (tDay || isToday) {
        c.state = "worked";
        if (!isToday && pd.lengthKnown && (pd.signedMs || 0) < shortBelowMs) { c.state = "partial"; c.short = true; }
        if (usualStartMin != null && pd.firstIn && nyMinutes(pd.firstIn) > usualStartMin + rules.lateAfterMin) c.late = true;
      } else { c.state = "worked"; c.extra = true; }
    } else if (isToday && !sched.closed.has(d)) c.state = "pending";
    else if (!tDay) c.state = "closed";
    else if (firstDay == null || d < firstDay) c.state = "before";
    else c.state = "off";
    all.push(c);
  }

  // streaks over everything looked at
  let cur = 0, best = 0;
  for (const c of all) {
    if (c.state === "worked" || c.state === "partial") { cur++; if (cur > best) best = cur; }
    else if (c.state === "off") cur = 0;
  }
  const inRange = all.filter(c => c.day >= from);
  const calendar = inRange.map(c => { const o = Object.assign({}, c); delete o._startFromWork; return o; });

  // the counts, over the period asked for
  let workingDays = 0, daysWorked = 0, daysOff = 0, extraDays = 0, shortDays = 0, lateDays = 0;
  const shiftList = [], startList = [], endList = [], estDays = [];
  let estShift = 0, estEnd = 0, estStart = 0, estShort = 0;
  const wd = WEEKDAYS.map((label, dow) => ({ dow, label, workingDays: 0, daysWorked: 0, daysOff: 0, extraDays: 0, shortDays: 0, lateDays: 0, shifts: [] }));
  for (const c of inRange) {
    const row = wd[weekdayOf(c.day)];
    if (c.state === "worked" || c.state === "partial") {
      if (c.extra) { extraDays++; row.extraDays++; if (c.estimated && estDays.length < 60) estDays.push(c.day); continue; }
      workingDays++; daysWorked++; row.workingDays++; row.daysWorked++;
      if (c.estimated && estDays.length < 60) estDays.push(c.day);
      if (c.short) { shortDays++; row.shortDays++; }
      if (c.late) { lateDays++; row.lateDays++; }
      if (c.estimated) estShort++;
      if (c.firstIn) { startList.push(nyMinutes(c.firstIn)); if (c._startFromWork) estStart++; }
      if (c.day !== today) {
        if (c.lastOut) { endList.push(nyMinutes(c.lastOut)); if (c.estimated) estEnd++; }
        if (c.lengthKnown !== false && c.signedMs != null) { shiftList.push(c.signedMs); row.shifts.push(c.signedMs); if (c.estimated) estShift++; }
      }
    } else if (c.state === "off") { workingDays++; daysOff++; row.workingDays++; row.daysOff++; }
  }
  const byWeekday = WEEK_ORDER.map(i => {
    const w = wd[i];
    return { dow: w.dow, label: w.label, workingDays: w.workingDays, daysWorked: w.daysWorked, daysOff: w.daysOff, extraDays: w.extraDays, shortDays: w.shortDays, lateDays: w.lateDays,
      avgShiftMs: w.shifts.length ? Math.round(mean(w.shifts)) : null, offShare: w.workingDays ? r1(100 * w.daysOff / w.workingDays) : null };
  });
  const avg = shiftList.length ? Math.round(mean(shiftList)) : null, med = shiftList.length ? Math.round(median(shiftList)) : null;
  const mStart = startList.length ? Math.round(median(startList)) : null, mEnd = endList.length ? Math.round(median(endList)) : null;

  const found = !!firstDay;
  const note = [];
  if (calendar.some(c => c.state === "unknown")) note.push(`Days before ${trackingStart} are shown as not tracked: sign-in logging had not begun, so nobody is counted absent on them.`);
  if (!found) note.push("No sign-in or recorded work was found for this name in the days looked at.");
  if (estDays.length) note.push(`${estDays.length} day${estDays.length === 1 ? "" : "s"} use an estimate: the system signs everybody out at midnight, so that time is cut back to the last recorded action.`);
  const unknownLen = calendar.filter(c => c.lengthKnown === false).length;
  if (unknownLen) note.push(`${unknownLen} day${unknownLen === 1 ? "" : "s"} ended with the midnight auto sign-out and nothing recorded after signing in, so their length is unknown and they are left out of shift averages and never counted as short.`);

  const fld = (estimated, why) => ({ estimated: !!estimated, why: estimated ? why : "" });
  const estimatedBlock = {
    any: estDays.length > 0, days: estDays,
    fields: {
      workingDays: { estimated: false, why: "", rule: "Decided by the team-size rule in definitions.workingDay: it is a rule, not a measurement." },
      daysWorked: fld(false), daysOff: { estimated: false, why: "", rule: "Decided by the team-size rule in definitions.workingDay: it is a rule, not a measurement." },
      shortDays: fld(estShort > 0, "Some days used an estimated length (midnight auto sign-out or recorded actions only), and the limit is learned from the person's own past shifts."),
      lateDays: fld(estStart > 0, "Some days had no sign-in, so the first recorded action stands in for the start."),
      avgShiftMs: fld(estShift > 0, "Some days use an estimated length (cut back to the last recorded action after the midnight auto sign-out, or the span of recorded actions when there was no sign-in)."),
      medianShiftMs: fld(estShift > 0, "Some days use an estimated length (cut back to the last recorded action after the midnight auto sign-out, or the span of recorded actions when there was no sign-in)."),
      medianStart: fld(estStart > 0, "Some days had no sign-in, so the first recorded action stands in."),
      medianEnd: fld(estEnd > 0, "Some days ended with the midnight auto sign-out or had no sign-in, so the last recorded action stands in for the end."),
      streaks: fld(false)
    }
  };
  const rulesOut = { teamMinPeople: rules.teamMinPeople, minSignedMin: rules.minSignedMin, shortFraction: rules.shortFraction,
    shortFallbackMin: rules.shortFallbackMin, minShiftsForUsual: rules.minShiftsForUsual, lateAfterMin: rules.lateAfterMin, baselineDays: rules.baselineDays,
    usualWeekdays: [1, 2, 3, 4, 5, 6, 0].filter(w => usual[w]),
    usualShiftMs: usualShiftMs == null ? null : Math.round(usualShiftMs), usualStart: usualStartMin, usualStartText: clockText(usualStartMin), shiftsLearnedFrom: shifts.length,
    shortBelowMs: Math.round(shortBelowMs), shortBasis: trusted ? "own median shift" : "fallback", schedule: sched.configured };
  const display = (m.aliases && m.aliases.display && m.aliases.display.get(m.target)) || (forms.get(m.target) ? niceName(bestForm(forms.get(m.target))) : "") || m.display || "";
  return {
    ok: true, mode: m.mode || (m.sandbox ? "sandbox" : "real"), name: display, found, from, to, today, trackingStart, firstDay,
    calendar,
    workingDays, daysWorked, daysOff, extraDays, lateDays, shortDays,
    attendanceRate: workingDays ? r1(100 * daysWorked / workingDays) : null,
    avgShiftMs: avg, medianShiftMs: med,
    medianStart: mStart, medianStartText: clockText(mStart), medianEnd: mEnd, medianEndText: clockText(mEnd),
    streaks: { current: cur, best, from: baseFrom < trackingStart ? trackingStart : baseFrom, to: readTo },
    byWeekday, rules: rulesOut, states: STATES, definitions: DEFINITIONS, estimated: estimatedBlock, notes: note
  };
}

/* ── loading ──────────────────────────────────────────────────────────────── */
const memos = new WeakMap();
function memoOf(ctx) {
  if (!ctx.db || typeof ctx.db !== "object") return null;
  let m = memos.get(ctx.db); if (!m) memos.set(ctx.db, m = new Map());
  return m;
}
/** A shared, time-limited read; a failed read is forgotten at once. */
function cached(ctx, key, ttl, fn) {
  const memo = memoOf(ctx);
  ttl = Math.min(ttl, ctx.life == null ? Infinity : ctx.life);
  if (!memo || !(ttl > 0)) return Promise.resolve().then(fn);
  const hit = memo.get(key), t = Date.now();
  if (hit && t - hit.at < ttl) return hit.p;
  const entry = { at: t, p: Promise.resolve().then(fn) };
  memo.set(key, entry);
  entry.p.catch(() => { if (memo.get(key) === entry) memo.delete(key); });
  if (memo.size > 200) for (const [k, e] of memo) if (t - e.at > TTL_PAST) memo.delete(k);
  return entry.p;
}
const col = (ctx, name) => ctx.db.collection((ctx.prefix || "") + name);
const failText = e => String((e && (e.message || e.code)) || e || "failed").slice(0, 160);
const safe = (p, label) => Promise.resolve(p).then(value => ({ ok: true, value }), e => ({ ok: false, label, error: failText(e) }));

/** The days a..z as reads: the days that are over (kept 10 minutes, shared by every viewer) and today (kept 5 seconds), so a
 *  window that ends today re-reads only today's few documents each time. `first` = the part that starts the window. */
function splitDays(a, z, today) {
  if (z < today) return [{ a, z, ttl: TTL_PAST, first: true }];
  const out = [], y = addDays(today, -1);
  if (a <= y) out.push({ a, z: y, ttl: TTL_PAST, first: true });
  out.push({ a: a > today ? a : today, z, ttl: TTL_LIVE, first: a > y });
  return out;
}
function readRollups(ctx, a, z, ttl) {
  return cached(ctx, `roll|${ctx.prefix || ""}|${a}|${z}`, ttl, async () => {
    const snap = await col(ctx, COL.rollup).where("day", ">=", a).where("day", "<=", z).limit(LIM.rollups + 1).get();
    return { rows: snap.docs.slice(0, LIM.rollups).map(d => d.data() || {}), capped: snap.docs.length > LIM.rollups };
  });
}
function readSessions(ctx, fromMs, toMs, ttl) {
  return cached(ctx, `sess|${ctx.prefix || ""}|${fromMs}|${toMs}`, ttl, async () => {
    const TS = ctx.admin && ctx.admin.firestore && ctx.admin.firestore.Timestamp, ranges = [[fromMs, toMs]];
    if (TS && typeof TS.fromMillis === "function") ranges.push([TS.fromMillis(fromMs), TS.fromMillis(toMs)]);     // times are ms or Firestore times; a range matches one kind
    const snaps = await Promise.all(ranges.map(([x, y]) => col(ctx, COL.sessions).where("startAt", ">=", x).where("startAt", "<", y).orderBy("startAt", "desc").limit(LIM.sessions + 1).get()));
    const seen = new Set(), rows = []; let capped = false;
    for (const s of snaps) { if (s.docs.length > LIM.sessions) capped = true; for (const d of s.docs.slice(0, LIM.sessions)) if (!seen.has(d.id)) { seen.add(d.id); rows.push(Object.assign({ id: d.id }, d.data() || {})); } }
    return { rows, capped };
  });
}
/** Reads the days a..z in its parts and puts the rows together. */
async function readParts(ctx, a, z, today, kind) {
  const parts = await Promise.all(splitDays(a, z, today).map(p => kind === "rollups"
    ? readRollups(ctx, p.a, p.z, p.ttl)
    : readSessions(ctx, p.first ? nyMidnight(p.a) - DAY_MS : nyMidnight(p.a), nyMidnight(addDays(p.z, 1)), p.ttl)));   // (the first part also looks a day back for a session begun before it)
  const seen = new Set(), rows = [];
  for (const p of parts) for (const r of p.rows) { if (r && r.id) { if (seen.has(r.id)) continue; seen.add(r.id); } rows.push(r); }
  return { rows, capped: parts.some(p => p.capped) };
}
async function loadAliases(ctx) {
  if (typeof ctx.keyOf === "function") return { keyOf: name => String(ctx.keyOf(name) || ""), aliases: ctx.aliases || null, error: "" };
  if (ctx.aliases && ctx.aliases.map instanceof Map) return { keyOf: keyFn(ctx.aliases), aliases: ctx.aliases, error: "" };
  let aliases = buildAliases(null), error = "";
  if (ctx.db) {
    const r = await safe(cached(ctx, `aliases|${ctx.prefix || ""}`, TTL_CONFIG, async () => { const s = await ctx.db.collection("config").doc("employeeAliases").get(); return s.exists ? s.data() : null; }), "aliases");
    if (r.ok) aliases = buildAliases(r.value); else error = "aliases: " + r.error;
  }
  return { keyOf: keyFn(aliases), aliases, error };
}
async function loadSchedule(ctx) {
  if (ctx.schedule !== undefined) return { schedule: cleanSchedule(await ctx.schedule), error: "" };
  if (!ctx.db) return { schedule: cleanSchedule(null), error: "" };
  const r = await safe(cached(ctx, `schedule|${ctx.prefix || ""}`, TTL_CONFIG, async () => { const s = await ctx.db.collection("config").doc("employeeSchedule").get(); return s.exists ? (s.data() || {}) : null; }), "schedule");
  return r.ok ? { schedule: cleanSchedule(r.value), error: "" } : { schedule: cleanSchedule(null), error: "schedule: " + r.error };
}
const rowsOf = (v, key) => Array.isArray(v) ? v : v && Array.isArray(v[key]) ? v[key] : v && Array.isArray(v.docs) ? v.docs.map(d => typeof d.data === "function" ? d.data() : d) : [];

/** The rows for the days a..z: handed in, loaded by the caller's own functions, or read here. Says what could not be read. */
async function loadRows(ctx, a, z, today) {
  const out = { rollups: [], sessions: [], errors: [], capped: [], failed: 0, handedIn: false };
  const tasks = [], wantMs = [nyMidnight(a) - DAY_MS, nyMidnight(addDays(z, 1))];
  const take = (p, label, put) => tasks.push(safe(p, label).then(r => { if (r.ok) put(r.value); else { out.errors.push(label + ": " + r.error); out.failed++; } }));
  if (Array.isArray(ctx.rollups)) { out.rollups = ctx.rollups; out.handedIn = true; }
  else if (typeof ctx.loadRollups === "function") take(Promise.resolve().then(() => ctx.loadRollups(a, z)), "rollups", v => { out.rollups = rowsOf(v, "rows"); });
  else if (ctx.db) take(readParts(ctx, a, z, today, "rollups"), "rollups", v => { out.rollups = v.rows; if (v.capped) out.capped.push("rollups"); });
  else { out.errors.push("rollups: no source"); out.failed++; }
  if (Array.isArray(ctx.sessions)) { out.sessions = ctx.sessions; out.handedIn = true; }
  else if (typeof ctx.loadSessions === "function") take(Promise.resolve().then(() => ctx.loadSessions(wantMs[0], wantMs[1])), "sessions", v => { out.sessions = rowsOf(v, "rows"); });
  else if (ctx.db) take(readParts(ctx, a, z, today, "sessions"), "sessions", v => { out.sessions = v.rows; if (v.capped) out.capped.push("sessions"); });
  else { out.errors.push("sessions: no source"); out.failed++; }
  await Promise.all(tasks);
  return out;
}

/* ── the entry ────────────────────────────────────────────────────────────── */
/**
 * attendance(ctx, { name, from, to }) -> Promise<answer>
 * An answer with ok:false is returned (never thrown) for a bad request or when neither collection could be read.
 */
async function attendance(ctx, args) {
  ctx = ctx || {}; args = args || {};
  const now = Number.isFinite(+ctx.now) && +ctx.now > 0 ? +ctx.now : Date.now(), today = validDay(ctx.today) ? ctx.today : nyDay(now);
  const name = cleanName(args.name);
  if (!okName(name)) return { ok: false, error: "name required" };
  if (!validDay(args.from) || !validDay(args.to)) return { ok: false, error: "from and to must be days like 2026-10-05" };
  let from = args.from, to = args.to; const notes = [];
  if (from > to) return { ok: false, error: "from is after to" };
  const maxTo = addDays(today, MAX_FUTURE_DAYS);
  if (to > maxTo) to = maxTo;
  if (daysBetween(from, to) + 1 > MAX_DAYS) { from = addDays(to, -(MAX_DAYS - 1)); notes.push(`The period was cut to the last ${MAX_DAYS} days.`); }
  const sandbox = ctx.sandbox === true || ctx.mode === "sandbox" || ctx.prefix === "Sandbox_";
  ctx = Object.assign({}, ctx, { prefix: sandbox ? "Sandbox_" : "", life: ctx.life != null ? ctx.life : (sandbox ? TTL_LIVE : Infinity) });
  const trackingStart = validDay(ctx.trackingStart) ? ctx.trackingStart : LOGGING_START;
  // the days to read: the period, and enough before it to learn the person's usual shift (never before logging began or after today)
  const readTo = to < today ? to : today;
  let baseFrom = addDays(readTo, -(RULES.baselineDays - 1)); if (baseFrom > from) baseFrom = from;
  if (baseFrom < addDays(to, -(MAX_DAYS - 1))) baseFrom = addDays(to, -(MAX_DAYS - 1));
  const readFrom = baseFrom < trackingStart ? trackingStart : baseFrom;
  const [al, sc] = await Promise.all([loadAliases(ctx), loadSchedule(ctx)]);
  const key = al.keyOf(name);
  let rows = { rollups: [], sessions: [], errors: [], capped: [], failed: 0, handedIn: false };
  if (readFrom <= readTo) rows = await loadRows(ctx, readFrom, readTo, today);
  // rows handed in as arrays cover only what their owner says (rowsFrom, else just the period): never learn from days they may lack
  if (rows.handedIn) baseFrom = validDay(ctx.rowsFrom) && ctx.rowsFrom < from ? ctx.rowsFrom : from;
  if (rows.failed >= 2) return { ok: false, unavailable: true, error: "the data could not be read just now", errors: rows.errors };
  const answer = compute({ target: key, display: niceName(name), from, to, today, now, rows, keyOf: al.keyOf, aliases: al.aliases, schedule: sc.schedule, trackingStart, baseFrom, sandbox, mode: sandbox ? "sandbox" : "real" });
  const errors = rows.errors.concat(al.error ? [al.error] : [], sc.error ? [sc.error] : []);
  if (notes.length) answer.notes = notes.concat(answer.notes);
  if (rows.capped.length) answer.notes.push("Some lists were cut at their size limit: " + rows.capped.join(", ") + ". Counts may be too low.");
  if (errors.length || rows.capped.length) { answer.partial = true; if (errors.length) { answer.errors = errors; answer.notes.push("Some data could not be read just now, so these numbers may be incomplete."); } }
  return answer;
}

module.exports = { attendance, RULES, LOGGING_START, DEFINITIONS, STATES, _t: { compute, ingest, finish, clip, spanOf, covered, fold, niceName, buildAliases, keyFn, nyDay, nyMidnight, nyMinutes, addDays, weekdayOf, median, cleanSchedule, clockText } };
