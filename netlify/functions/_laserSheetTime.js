/*  netlify/functions/_laserSheetTime.js
 *  How long each cut sheet took the Laser person (Paul, 6 Oct 2026: "the time it took to complete this sheet must be calculated on
 *  the elapsed time since most recent login or if login is continuous then since the last sheet has been marked as completed").
 *
 *  THE RULE (plans/stations-round2/plan.md R7; api.md section LS1)
 *    time of a sheet = the moment it was marked completed - START
 *    START = the LATER of (that person's most recent sign-in as Laser, that person's previous standing sheet completion)
 *    - per person: two Laser people have their own clocks;
 *    - never earlier than the sign-in: an idle, 5 pm, midnight or manual sign-out and a new sign-in restarts the clock;
 *    - a completion with no Laser session (Admin, no role, a Design session) has NO time: it is stored as unknown, never invented.
 *    - "had a Laser session" = the sign-in was still OPEN by the stations' own rules at that moment (_stationAutoSignout.decide, the very rules the board and
 *      the sweep apply: Laser is signed out after 60 minutes without input, 30 from 17:00, at the LAST INPUT, and a quiet page is not closed after 15 minutes).
 *      So a Laser person who starts a long cut and leaves the page alone, a sleeping computer, a page whose beats did not arrive: the sign-in still covers the sheet
 *      while the station keeps it open. A sign-out the rules already made (idle at the last input, 5 pm, midnight, switching role) ends the clock; a new sign-in restarts it.
 *
 *  THE RECORD: Laser_Sheet_Times/{sheetId}__{at} (Sandbox_Laser_Sheet_Times in the sandbox), one document per sheet completion, written
 *  once by op laserDone (charmNestLibrary.js) from the server's own clock, the Station_Sessions documents and the earlier records
 *  here. The page's own figure (charm-nest-laser-time.js) is sent along and cross-checked: when the two differ the SERVER's figure is
 *  kept and the document says so. A record is never edited, recomputed or deleted; taking a completion back writes a second kind of
 *  document (laserSheetUndone) and the readers leave the undone completion out. No Etsy call, no model call, no PIN: the person is the
 *  NAME only.
 *
 *  Everything below that touches Firestore takes `db` and `prefix` as arguments; the pure rule (computeStart, pickSession, pickPrevious,
 *  summarize) has no I/O and is what the test checks against the page's copy. */
"use strict";

const AutoSignout = require("./_stationAutoSignout");     // the stations' sign-out rules (pure at load: no Firestore until a call): the one place that says when a Laser sign-in ended
const COLL = "Laser_Sheet_Times", SESSIONS = "Station_Sessions";
const DONE = "laserSheetDone", UNDONE = "laserSheetUndone";
const SESSION_GONE_MS = 15 * 60000;            // a page with no beat for this long is "quiet": most stations close it, the Laser station keeps it until its own limit (stillOpen)
const LOOKBACK_MS = 26 * 3600e3;               // a Laser sign-in older than this cannot cover a completion: everybody is signed out at midnight (26 h covers the longest day)
const TOLERANCE_ABS_S = 10, TOLERANCE_REL = 0.05;   // the page's figure and the server's agree within 10 s or 5 %
const MAX_SECONDS = 24 * 3600;
const LIMIT = { sessions: 1500, prior: 800, range: 4000, rows: 400 };
const TZ = "America/New_York";

/* ── small helpers (the same cleaning as the stations' doors) ── */
const str = (v, n) => String(v == null ? "" : v).replace(/[\u0000-\u001f\u007f]/g, " ").replace(/\s+/g, " ").trim().slice(0, n);
const num = v => { const x = Number(v); return Number.isFinite(x) ? x : 0; };
const ms = v => v == null ? 0 : typeof v.toMillis === "function" ? v.toMillis() : v instanceof Date ? v.getTime() : Number.isFinite(+v) ? +v : 0;
const noPin = s => (s.match(/\p{Nd}/gu) || []).length >= 4 ? s.replace(/\p{Nd}+/gu, " ").replace(/\s+/g, " ").trim() : s;
/** The name as a station keeps it (digits runs of four or more are a PIN that slipped in and are dropped), or "" when it is nobody. */
const cleanName = v => { const s = noPin(str(v, 200)).slice(0, 80); return /\p{L}/u.test(s) ? s : ""; };
/** One person however the login spelled the name: strip accents, case-fold, drop apostrophes, every other punctuation mark is a space ("Michael_V" = "Michael V."). */
const personKey = v => cleanName(v).normalize("NFD").replace(/[̀-ͯ]/g, "").toLowerCase().replace(/['‘’`´]/g, "").replace(/[^\p{L}\p{N}]+/gu, " ").trim();

let nyFmt = null;
try { nyFmt = new Intl.DateTimeFormat("en-US", { timeZone: TZ, hourCycle: "h23", year: "numeric", month: "2-digit", day: "2-digit", hour: "2-digit", minute: "2-digit", second: "2-digit" }); } catch (_) {}
function nyDay(t) {
  if (nyFmt) { const o = {}; for (const p of nyFmt.formatToParts(new Date(t))) o[p.type] = p.value; return `${o.year}-${o.month}-${o.day}`; }
  return new Date(t - 5 * 3600e3).toISOString().slice(0, 10);
}

/* ═════════════════════════ THE RULE (pure) ═════════════════════════ */

/** { startAt, startedFrom, seconds } for a completion at `at` (ms) given the Laser sign-in `loginAt` and the previous standing completion `prevAt` (ms or 0/null).
 *  No sign-in = unknown (seconds null). The start is the later of the two, never before the sign-in; a tie counts as the sign-in. */
function computeStart(at, loginAt, prevAt) {
  at = num(at); loginAt = num(loginAt); prevAt = num(prevAt);
  if (!(at > 0) || !(loginAt > 0)) return { startAt: null, startedFrom: "unknown", seconds: null };
  const fromPrev = prevAt > loginAt && prevAt <= at;
  const startAt = fromPrev ? prevAt : Math.min(loginAt, at);
  return { startAt, startedFrom: fromPrev ? "previousSheet" : "login", seconds: Math.max(0, Math.min(MAX_SECONDS, Math.round((at - startAt) / 1000))) };
}

/** Was this OPEN Laser sign-in (no end stored yet) still the person's at the moment `at`? By the stations' own rules (_stationAutoSignout.decide, the ones the board and the sweep
 *  apply to the same rows): not when the rules had already ended it by then (Laser: 60 minutes without input before 17:00, 30 from 17:00, at the last input; an Admin's row keeps the
 *  old 15 quiet minutes) and not past the New York midnight after it started. A page that is merely quiet (a long cut, a sleeping computer, beats that did not arrive) is still signed in
 *  while the station keeps it open, so the sheet is timed from the sign-in. */
function stillOpen(s, startAt, at) {
  if (at >= AutoSignout.nyMidnightAfter(startAt)) return false;
  const d = AutoSignout.decide({ startAt, lastSeenAt: ms(s.lastSeenAt), lastInputAt: ms(s.lastInputAt), station: "laser" }, at, s.admin === true);
  return !d || d.endAt >= at - 1000;
}

/** The Laser session of this person that covers the moment `at`: the most recent sign-in as Laser at or before it that had not ended
 *  (or ended at or after it); an open one counts while the stations' rules still keep it open (stillOpen). rows are Station_Sessions documents as stored. null for none. */
function pickSession(rows, key, at) {
  if (!key || !(at > 0)) return null;
  let best = null;
  for (const s of rows || []) {
    if (!s || personKey(s.person) !== key) continue;
    if (s.station !== "laser" && s.role !== "laser") continue;          // (the Sorter app's session of a Laser person: station "laser", LD1; role "laser" is read too)
    const startAt = ms(s.startAt); if (!(startAt > 0) || startAt > at) continue;
    const endAt = ms(s.endAt);
    if (endAt > 0) { if (endAt < at - 1000) continue; }
    else if (!stillOpen(s, startAt, at)) continue;                      // ended by the rules before this moment (idle at its last input, 5 pm, midnight) though the end is not stored yet
    if (!best || startAt > best.startAt) best = { id: str(s.id, 100), startAt, device: str(s.device, 40) };
  }
  return best;
}

/** The previous STANDING completion by this person before `at`, inside the sign-in (docs are Laser_Sheet_Times documents): the latest `at`. null for none. */
function pickPrevious(docs, key, at, loginAt) {
  const undone = undoneSet(docs);
  let best = null;
  for (const d of docs || []) {
    if (!d || d.kind !== DONE || d.personKey !== key) continue;
    const t = ms(d.at); if (!(t > 0) || t >= at || t < loginAt) continue;
    if (undone.has(`${d.sheetId}|${t}`)) continue;
    if (!best || t > best.at) best = { at: t, sheetId: str(d.sheetId, 100) };
  }
  return best;
}
const undoneSet = docs => { const s = new Set(); for (const d of docs || []) if (d && d.kind === UNDONE && d.sheetId && ms(d.was) > 0) s.add(`${d.sheetId}|${ms(d.was)}`); return s; };
/** The standing completions of a list of documents (undone ones left out), newest first. */
function standing(docs) {
  const undone = undoneSet(docs);
  return (docs || []).filter(d => d && d.kind === DONE && ms(d.at) > 0 && !undone.has(`${d.sheetId}|${ms(d.at)}`)).sort((a, b) => ms(b.at) - ms(a.at) || (a.sheetId < b.sheetId ? -1 : 1));
}
/** { sheets, timed, unknown, avgSec, fastestSec, slowestSec, totalSec, pieces, orders } of standing rows. Only a row with `seconds` counts for the times; none = null, never 0. */
function summarize(rows) {
  const out = { sheets: 0, timed: 0, unknown: 0, avgSec: null, fastestSec: null, slowestSec: null, totalSec: 0, pieces: 0, orders: 0 };
  for (const r of rows || []) {
    out.sheets++; out.pieces += Math.max(0, num(r.pieces)); out.orders += Math.max(0, num(r.orders));
    const s = r.seconds; if (s == null || !Number.isFinite(+s)) { out.unknown++; continue; }
    out.timed++; out.totalSec += +s;
    out.fastestSec = out.fastestSec == null ? +s : Math.min(out.fastestSec, +s); out.slowestSec = out.slowestSec == null ? +s : Math.max(out.slowestSec, +s);
  }
  if (out.timed) out.avgSec = Math.round(out.totalSec / out.timed);
  return out;
}

/** What the page said, cleaned: { role, session, loginAgo, prevAgo, prevKnown, seconds, startedFrom } or null (nothing usable). The page sends AGES in ms before the press
 *  (loginAgo, prevAgo), never clock times, so a wrong clock on that computer cannot spoil a figure. */
function cleanClient(c) {
  if (!c || typeof c !== "object" || Array.isArray(c)) return null;
  const age = v => { const x = Number(v); return Number.isFinite(x) && x >= 0 && x <= LOOKBACK_MS ? Math.round(x) : null; };
  const sec = c.seconds == null ? null : Number.isFinite(+c.seconds) && +c.seconds >= 0 && +c.seconds <= MAX_SECONDS ? Math.round(+c.seconds) : null;
  return { role: c.role === "laser" ? "laser" : "", session: /^[\w.:-]{8,100}$/.test(String(c.session || "")) ? String(c.session) : "", loginAgo: age(c.loginAgo), prevAgo: age(c.prevAgo), prevKnown: c.prevKnown !== false,
    seconds: sec, startedFrom: c.startedFrom === "previousSheet" ? "previousSheet" : c.startedFrom === "login" ? "login" : "" };
}
const agree = (a, b) => Math.abs(a - b) <= Math.max(TOLERANCE_ABS_S, TOLERANCE_REL * Math.max(a, b));
const fromWord = f => (f === "login" ? "the sign-in" : f === "previousSheet" ? "the previous sheet" : "");

/** The figure of one completion action at the server's moment `at`. `ctx` = { session, prev } read from the store (session: pickSession's answer or null; prev: pickPrevious's
 *  answer or null), `client` = cleanClient's answer or null. Returns { seconds, startedFrom, startAt, loginAt, prevAt, prevSheetId, session, source, verified, clientSeconds, disagree, note }.
 *  The server's own figure always wins; the page's is kept beside it. */
function decide(at, ctx, client) {
  const out = { seconds: null, startedFrom: "unknown", startAt: null, loginAt: null, prevAt: null, prevSheetId: "", session: "", source: "none", verified: false, clientSeconds: client && client.seconds != null ? client.seconds : null, disagree: false, note: "" };
  const prev = ctx && ctx.prev || null, said = client && client.role === "laser" && client.seconds != null;
  if (ctx && ctx.session) {
    const r = computeStart(at, ctx.session.startAt, prev && prev.at);
    Object.assign(out, { seconds: r.seconds, startedFrom: r.startedFrom, startAt: r.startAt, loginAt: ctx.session.startAt, prevAt: prev ? prev.at : null, prevSheetId: prev ? prev.sheetId : "", session: ctx.session.id, source: "server", verified: true });
    if (said && !agree(client.seconds, r.seconds)) {
      // a page that did not know the previous sheet (another computer completed it) cannot be expected to agree: that is a note, not a disagreement
      const unaware = client.prevKnown === false && r.startedFrom === "previousSheet";
      out.disagree = !unaware;
      out.note = `the page said ${client.seconds} s${client.startedFrom ? " from " + fromWord(client.startedFrom) : ""}${unaware ? " (it did not know the previous sheet)" : ""}; the server's ${r.seconds} s from the sessions was kept`;
    }
    return out;
  }
  // The server found no Laser session record (the page's start write was lost or has not arrived): a page that says it is a Laser sign-in keeps its own sign-in time (as an age before
  // this press), and the previous sheet is still checked on the server. It is flagged unverified. With no such claim the time is unknown.
  if (client && client.role === "laser" && client.loginAgo != null) {
    const loginAt = at - client.loginAgo, r = computeStart(at, loginAt, prev && prev.at);
    Object.assign(out, { seconds: r.seconds, startedFrom: r.startedFrom, startAt: r.startAt, loginAt, prevAt: prev ? prev.at : null, prevSheetId: prev ? prev.sheetId : "", session: client.session || "", source: "client", verified: false,
      note: "no Laser session record was found on the server: the page's sign-in time was used" });
    if (said && !agree(client.seconds, r.seconds)) { out.disagree = true; out.note += `; the page said ${client.seconds} s`; }
  }
  return out;
}

/* ═════════════════════════ THE STORE ═════════════════════════ */

const col = (db, prefix, name) => db.collection((prefix || "") + name);

/** The Laser sign-ins of the last day, as stored (one range on startAt, no composite index); the person is picked in memory. */
async function readSessions(db, prefix, at) {
  const snap = await col(db, prefix, SESSIONS).where("startAt", ">=", at - LOOKBACK_MS).limit(LIMIT.sessions).get();
  return snap.docs.map(d => Object.assign({ id: d.id }, d.data() || {}));
}
/** The completion and undo records from `fromMs` on (one range on `at`, newest first, so a cap drops the OLDEST), up to `untilMs` when given. */
async function readSince(db, prefix, fromMs, limit, untilMs) {
  let q = col(db, prefix, COLL).where("at", ">=", fromMs);
  if (untilMs) q = q.where("at", "<", untilMs);
  const snap = await q.orderBy("at", "desc").limit(limit || LIMIT.prior).get();
  return snap.docs.map(d => Object.assign({ _id: d.id }, d.data() || {}));
}
/** The session and the previous standing completion of one person at `at`: { session, prev }. Never throws: a failed read is "none" and the caller says so (`readError`). */
async function context(db, prefix, by, at) {
  const key = personKey(by), out = { key, session: null, prev: null };
  if (!key) return out;
  try {
    out.session = pickSession(await readSessions(db, prefix, at), key, at);
    // the previous sheet is read for the whole lookback window too: the client may have a sign-in the server does not know (decide() above)
    out.prev = pickPrevious(await readSince(db, prefix, at - LOOKBACK_MS), key, at, out.session ? out.session.startAt : at - LOOKBACK_MS);
  } catch (e) { out.readError = String((e && e.message) || e).slice(0, 160); }
  return out;
}

/** Writes the documents once (an existing id is left exactly as it is). Returns how many were new. */
async function writeOnce(db, FV, prefix, docs) {
  if (!docs.length) return 0;
  return db.runTransaction(async tx => {
    const refs = docs.map(d => col(db, prefix, COLL).doc(d._id));
    const snaps = await tx.getAll(...refs);
    let n = 0;
    docs.forEach((d, i) => { if (snaps[i] && snaps[i].exists) return; const x = Object.assign({}, d); delete x._id; tx.set(refs[i], Object.assign(x, { createdAt: FV.serverTimestamp() })); n++; });
    return n;
  });
}

/** The records of one laserDone press that marked sheets completed. `marks` = [{ sheetId, sheet, setId, setSeq, metal, pieces, orders }], `at` the server's moment,
 *  `by` the name, `client` the page's cleaned figure (or null). Reads the sessions and the earlier completions, decides once, writes once.
 *  Returns { sheets:[{ sheetId, seconds, startedFrom, source, disagree }], figure } and never throws (a failed write is logged and the press stands as it was). */
async function recordDone(db, FV, prefix, o) {
  const marks = (o.marks || []).filter(m => m && m.sheetId);
  if (!marks.length) return { sheets: [], figure: null };
  const at = o.at, by = cleanName(o.by), ctx = await context(db, prefix, by, at), figure = decide(at, ctx, o.client || null);
  if (ctx.readError) figure.note = (figure.note ? figure.note + "; " : "") + "the sessions could not be read: " + ctx.readError;
  const together = marks.length, each = figure.seconds == null ? null : Math.round(figure.seconds / together);
  const docs = marks.map(m => ({
    _id: `${m.sheetId}__${at}`, kind: DONE, v: 1, id: `${m.sheetId}__${at}`, sheetId: m.sheetId, sheet: str(m.sheet, 80), setId: str(m.setId, 100), setSeq: num(m.setSeq) || null, metal: str(m.metal, 20),
    person: by, personKey: personKey(by), at, day: nyDay(at), device: str(o.device, 40), via: str(o.via, 24),
    seconds: each, actionSeconds: figure.seconds, together, startedFrom: figure.startedFrom, startAt: figure.startAt, loginAt: figure.loginAt, session: figure.session, prevAt: figure.prevAt, prevSheetId: figure.prevSheetId,
    pieces: Math.max(0, Math.round(num(m.pieces))), orders: Math.max(0, Math.round(num(m.orders))),
    source: figure.source, verified: figure.verified, clientSeconds: figure.clientSeconds, disagree: figure.disagree, note: str(figure.note, 300) }));
  try { await writeOnce(db, FV, prefix, docs); }
  catch (e) { console.warn("[laserSheetTime] not recorded:", (e && e.message) || e); }
  return { sheets: docs.map(d => ({ sheetId: d.sheetId, seconds: d.seconds, startedFrom: d.startedFrom, source: d.source, disagree: d.disagree })), figure: Object.assign({}, figure, { each, together }) };
}

/** A completion taken back: one marker per completion (`was` = that completion's time), never an edit of the record. */
async function recordUndone(db, FV, prefix, o) {
  const docs = (o.marks || []).filter(m => m && m.sheetId && num(m.was) > 0).map(m => ({ _id: `undo__${m.sheetId}__${num(m.was)}`, kind: UNDONE, v: 1, id: `undo__${m.sheetId}__${num(m.was)}`, sheetId: m.sheetId, was: num(m.was), by: cleanName(o.by), at: o.at, day: nyDay(o.at) }));
  try { return await writeOnce(db, FV, prefix, docs); }
  catch (e) { console.warn("[laserSheetTime] undo not recorded:", (e && e.message) || e); return 0; }
}

/** A person's newest standing completion of the last 26 hours, for the page (the previous sheet after a reload or on another computer). */
async function lastFor(db, prefix, by, now) {
  const key = personKey(by); if (!key) return null;
  const rows = standing(await readSince(db, prefix, now - LOOKBACK_MS)).filter(d => d.personKey === key);
  const d = rows[0]; if (!d) return null;
  return { at: ms(d.at), sheetId: d.sheetId, sheet: d.sheet || "", seconds: d.seconds == null ? null : d.seconds, startedFrom: d.startedFrom || "unknown" };
}

/* ═════════════════════════ THE READERS (the portal) ═════════════════════════ */

/** One row as a reader shows it (no ids of other kinds, no personKey). */
const rowOf = d => ({ at: ms(d.at), sheetId: str(d.sheetId, 100), sheet: str(d.sheet, 80), setId: str(d.setId, 100), person: str(d.person, 80), seconds: d.seconds == null ? null : Math.max(0, Math.round(num(d.seconds))),
  startedFrom: d.startedFrom === "login" || d.startedFrom === "previousSheet" ? d.startedFrom : "unknown", pieces: Math.max(0, Math.round(num(d.pieces))), orders: Math.max(0, Math.round(num(d.orders))), together: Math.max(1, Math.round(num(d.together)) || 1),
  source: d.source === "server" || d.source === "client" ? d.source : "none", disagree: !!d.disagree, day: str(d.day, 10) || nyDay(ms(d.at)) });

/** The standing completions with `at` in [fromMs, toMs): { rows (newest first, as rowOf), capped }. One range on `at`; the query reaches two days past the window so the
 *  undo marker of a completion near its end is read too. */
async function readRange(db, prefix, fromMs, toMs) {
  const docs = await readSince(db, prefix, fromMs, LIMIT.range + 1, toMs + 2 * 86400e3);
  const capped = docs.length > LIMIT.range;
  return { rows: standing(docs.slice(0, LIMIT.range)).filter(d => ms(d.at) < toMs).map(rowOf), capped };
}

/* ── days (the shop's New York days are strings; this is only calendar arithmetic on them) ── */
const DAY_RE = /^\d{4}-\d{2}-\d{2}$/;
const addDays = (day, n) => { const [y, m, d] = day.split("-").map(Number); return new Date(Date.UTC(y, m - 1, d + n)).toISOString().slice(0, 10); };
const validDay = s => DAY_RE.test(String(s)) && addDays(String(s), 0) === String(s);
const diffDays = (a, b) => Math.round((Date.parse(b + "T12:00:00Z") - Date.parse(a + "T12:00:00Z")) / 86400e3);
const mondayOf = d => addDays(d, -((new Date(d + "T12:00:00Z").getUTCDay() + 6) % 7));

/** Series of a window: a bucket per day up to 92 days, Monday weeks beyond: [{ day, to, days, sheets, timed, avgSec, fastestSec, slowestSec }]. A bucket with no sheet is left out (nothing known). */
function seriesOf(rows, from, to) {
  const weekly = diffDays(from, to) + 1 > 92, by = new Map();
  for (const r of rows) { const b = weekly ? mondayOf(r.day) : r.day; if (!by.has(b)) by.set(b, []); by.get(b).push(r); }
  return [...by.keys()].sort().map(b => {
    const s = summarize(by.get(b)), last = weekly ? addDays(b, 6) : b;
    return { day: b < from ? from : b, to: last > to ? to : last, days: weekly ? 7 : 1, sheets: s.sheets, timed: s.timed, avgSec: s.avgSec, fastestSec: s.fastestSec, slowestSec: s.slowestSec };
  });
}

/** op laserSheets (employeeEfficiency): one person's sheets in a window. H = { json, nyMidnight, nameKeyOf, display, cleanName, okName } from employeeEfficiency.js. */
async function opSheets(ctx, body, H) {
  const name = H.cleanName(body.name);
  if (!H.okName(name)) return H.json(400, { ok: false, error: "name required" });
  const r = body.range;
  let to = body.day == null || body.day === "" ? ctx.today : String(body.day), from = "";
  if (r && typeof r === "object") { from = String(r.from || ""); to = String(r.to || to); }
  else { const n = { day: 1, week: 7, month: 30, quarter: 90, year: 365 }[r] || Math.max(1, Math.min(366, Math.floor(num(body.days)) || 30)); if (validDay(to)) from = addDays(to, -(n - 1)); }
  if (!validDay(to) || (from && !validDay(from))) return H.json(400, { ok: false, error: "day must be YYYY-MM-DD" });
  if (to > ctx.today) to = ctx.today;
  if (!from || from > to) return H.json(400, { ok: false, error: "the window is not valid" });
  if (diffDays(from, to) > 365) from = addDays(to, -365);
  const fromMs = H.nyMidnight(from), toMs = H.nyMidnight(addDays(to, 1));
  const want = H.nameKeyOf(ctx, name);
  const got = await readRange(ctx.db, ctx.prefix, fromMs, toMs);
  const mine = got.rows.filter(x => H.nameKeyOf(ctx, x.person) === want);
  const totals = summarize(mine);
  const out = { ok: true, now: ctx.now, name: H.display(name), from, to, days: diffDays(from, to) + 1, found: mine.length > 0, spellings: [...new Set(mine.map(x => x.person))].slice(0, 8),
    sheets: mine.slice(0, LIMIT.rows), totals, series: seriesOf(mine, from, to), notes: [] };
  if (mine.length > LIMIT.rows) out.notes.push(`The list shows the newest ${LIMIT.rows} sheets; the figures count all ${mine.length}.`);
  if (mine.some(x => x.seconds == null)) out.notes.push("A sheet marked completed with no Laser sign-in has no time: it is listed, and left out of the average, fastest and slowest.");
  if (got.capped) { out.partial = true; out.notes.push("The window held more records than one read takes; the oldest are left out."); }
  return H.json(200, out);
}

/** The Laser station's block of op live: { day, today, last } for everybody, today (New York). Null when the collection cannot be read (the card then says nothing). */
async function liveBlock(ctx, H) {
  const fromMs = H.nyMidnight(ctx.today);
  const got = await H.cached(ctx, `lasersheets|${ctx.prefix}|${ctx.today}`, 15000, () => readRange(ctx.db, ctx.prefix, fromMs, fromMs + 26 * 3600e3));
  const rows = got.rows.filter(r => r.day === ctx.today), t = summarize(rows), last = rows[0] || null;
  return { day: ctx.today, today: { sheets: t.sheets, timed: t.timed, avgSec: t.avgSec, fastestSec: t.fastestSec, slowestSec: t.slowestSec },
    last: last ? { at: last.at, person: H.display(last.person), sheet: last.sheet, sheetId: last.sheetId, seconds: last.seconds, startedFrom: last.startedFrom, pieces: last.pieces, orders: last.orders, together: last.together } : null };
}

module.exports = { COLL, DONE, UNDONE, SESSION_GONE_MS, LOOKBACK_MS, personKey, cleanName, computeStart, stillOpen, pickSession, pickPrevious, standing, summarize, cleanClient, decide, agree,
  context, recordDone, recordUndone, lastFor, readRange, readSince, rowOf, seriesOf, opSheets, liveBlock, nyDay, addDays, validDay, diffDays };
