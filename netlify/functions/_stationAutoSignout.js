/*  netlify/functions/_stationAutoSignout.js
 *  The server's half of the automatic sign-out (Paul, 6 Oct 2026: "Auto log-out all employees other than myself (Admin) unless you are seeing
 *  activity on a given station ... Also log everyone out of their station automatically at 5:00 pm Toronto time unless you detect the same
 *  activity threshold"). The page does it itself (station-session.js, AD1); this is the same rule for a page that died or never said so, so
 *  that it holds when a computer is switched off, a tab crashes or a browser sleeps.
 *
 *  PER STATION (Paul, 6 Oct 2026 20:10 UTC, plans/stations-round2/plan.md "Addendum 2"): the numbers live in ONE table, _stationSignoutPolicy.js, the very
 *  same table the page carries (station-session.js POLICY; a test compares them). default = the rules below, 10 minutes. WELDING: no idle sign-out at
 *  all; at 17:00 America/Toronto every Welding person is signed out, end time 17:00 sharp, reason "closing", whatever their last input (a session that
 *  began after 17:00 waits for the New York midnight). LASER: 60 minutes without input before 17:00 (reason "idle"), 30 minutes from 17:00 on (reason
 *  "closing"); the end is always the last input. The 15 minute "closed" rule (a page with no beat) does not end a Welding or a Laser session early: a
 *  silent Laser page is ended when its limit has passed since its last beat (60 minutes; 30 minutes counted from 17:00), a Welding one at 17:00.
 *  An explicit end the page itself sends (Sign out, switching person, pagehide) is not automatic and is stored as it always was.
 *  THE RULES  (a session = one document of Station_Sessions; `B` its last beat, `L` its last input; both server clock, ms; the default station's numbers)
 *    · An ended session is never touched, never rewritten, nothing is ever deleted.
 *    · Unknown whether the person is an Admin (the list unreadable and no stored flag): left open, decided on a later look.
 *    · Admin (stored `admin: true`, or the list says so now): exempt from the idle and the 5 pm rule. Only the old rule: a page silent for
 *      15 minutes ended "closed" at its last beat ("midnight" when that beat is past the session's own New York midnight).
 *    · Non-Admin without `lastInputAt` (an old page): the same old rule.
 *    · Non-Admin with `lastInputAt`: ended when the page itself reported 10 minutes or more without input at its last beat (B - L >= 10 min),
 *      or when the page is dead (no beat for 15 minutes). The end time is L, the last input, never the time anybody noticed. Reason "idle".
 *    · Reason "closing" instead of "idle": the session started before 17:00 America/Toronto, its last beat is at or after 17:00 and L is at or
 *      before 16:50 (first seen at or after 17:00 with no input in the 10 minutes before it). Anyone with input inside those 10 minutes stays,
 *      and the idle rule runs from their last input. The end time is L (never after 17:00).
 *    A session is decided only from what the server KNOWS, so a live page is never ended for input it has not reported yet: a page whose last
 *    beat is fresh may stay "signed in" for up to one beat (5 minutes); the server waits for the page's own word.
 *  WHERE IT RUNS  on read (settle(): the live op, the person and attendance ops, the overview, the sorter's Sign-ins list), in the beat door
 *    (firebaseOrders {session}), and by stationSessionsSweepCron every 5 minutes (sweep()).
 *  WRITES  one transaction per session, only while it is still open, re-checked on the fresh document (a beat that arrived meanwhile wins);
 *    endAt, endReason and minutes only.
 *  Contract: /mnt/project-files/plans/stations-round2/api.md ("AD2"). Nothing here calls Etsy. */
"use strict";
const Admins = require("./_stationAdmins");
const Policy = require("./_stationSignoutPolicy");

const COLL = "Station_Sessions";
const IDLE_MS = 10 * 60000, CLOSED_MS = 15 * 60000;
const CLOSING_ZONE = "America/Toronto", CLOSING_HOUR = 17, NY_ZONE = "America/New_York";
const READ_WRITE_WINDOW_MS = 3 * 86400e3;       // a reader writes the end of a session that started in the last 3 days (older ones are only patched in its answer; the sweep does them)
const MAX_WRITES = 40, SWEEP_WRITES = 100, SWEEP_READ = 300, RETRY_MS = 30000;
const END_REASONS = Object.freeze(["signOut", "midnight", "switched", "closed", "idle", "closing"]);
/* what each end means for the people reading it (all but midnight are a person's own or a normal end: hours stop at endAt, nothing is estimated) */
const END_TEXT = Object.freeze({ signOut: "Signed out", midnight: "Signed out at midnight", switched: "Another person signed in", closed: "Page closed",
  idle: "Signed out after 10 minutes without input", closing: "Signed out at 5:00 pm" });

/* time and chance are replaceable so a test runs in no time */
const deps = { now: () => Date.now() };

const ms = v => v == null ? 0 : typeof v.toMillis === "function" ? v.toMillis() : v instanceof Date ? v.getTime() : Number.isFinite(+v) ? +v : 0;

/* ── zones ── */
const fmts = new Map();
function zoneParts(zone, t) {
  let f = fmts.get(zone);
  if (!f) { try { f = new Intl.DateTimeFormat("en-US", { timeZone: zone, hourCycle: "h23", year: "numeric", month: "2-digit", day: "2-digit", hour: "2-digit", minute: "2-digit", second: "2-digit" }); } catch (_) { f = null; } fmts.set(zone, f); }
  if (f) { const o = {}; for (const p of f.formatToParts(new Date(t))) o[p.type] = p.value; return { y: +o.year, m: +o.month, d: +o.day, h: +o.hour % 24, mi: +o.minute, s: +o.second }; }
  const e = new Date(t - 5 * 3600e3);                                  // no zone data: Eastern Standard Time
  return { y: e.getUTCFullYear(), m: e.getUTCMonth() + 1, d: e.getUTCDate(), h: e.getUTCHours(), mi: e.getUTCMinutes(), s: e.getUTCSeconds() };
}
const offsetOf = (zone, x) => { const p = zoneParts(zone, x); return Date.UTC(p.y, p.m - 1, p.d, p.h, p.mi, p.s) - Math.floor(x / 1000) * 1000; };
/** the first New York midnight after t (ms): a session never outlives it (the same as firebaseOrders') */
function nyMidnightAfter(t) {
  const p = zoneParts(NY_ZONE, t), wall = Date.UTC(p.y, p.m - 1, p.d + 1);
  let u = wall - offsetOf(NY_ZONE, t); u = wall - offsetOf(NY_ZONE, u);
  return u > t ? u : t + 86400e3;
}
/** 17:00 in Toronto on the Toronto day of t (ms); daylight saving by the zone (the same wall clock as New York) */
function closingInstant(t) {
  const p = zoneParts(CLOSING_ZONE, t), wall = Date.UTC(p.y, p.m - 1, p.d, CLOSING_HOUR, 0, 0);
  let u = wall - offsetOf(CLOSING_ZONE, t); u = wall - offsetOf(CLOSING_ZONE, u);
  return u;
}

/* ── clocks of the pages ── */
/** How far a page's clock is from the server's: the server's now minus the clock the page stamped on its request (`sentAt`), in whole seconds.
 *  Under 10 s counts as none, and so does a value that is not a time or is over 36 hours off (the same correction as the live door's). */
function skewOf(sentAt, now) {
  const t = Number(sentAt);
  if (!Number.isFinite(t) || t < 1e12) return 0;
  const d = now - t;
  return Math.abs(d) < 10000 || Math.abs(d) > 36 * 3600e3 ? 0 : Math.round(d / 1000) * 1000;
}
/** A moment the page reports (ms, its clock) as the server's clock, or 0 when it is not a time. Never later than `now`. */
function pageTime(t, skew, now) {
  const x = Number(t);
  if (!Number.isFinite(x) || x < 1e12) return 0;
  return Math.min(Math.round(x + skew), now);
}

/* ── the decision ── */
/** minutes as the sessions keep them: one decimal */
const minutesOf = (start, end) => Math.max(0, Math.round((end - start) / 6000) / 10);

/**
 * What to do with an OPEN session, from what is known. `s` = { startAt, lastSeenAt, lastInputAt?, endAt?, station? } (ms or Firestore times);
 * `adm` = true (Admin) | false | null (unknown). Returns null (leave it) or { endAt, endReason, rule }.
 * `rule` says which rule decided it: "idle" | "closing" | "closed" | "midnight".
 * The station's row in _stationSignoutPolicy.js says which numbers apply (no `station`, or one with no row of its own: the default).
 */
function decide(s, now, adm) {
  const start = ms(s && s.startAt);
  if (!(start > 0) || ms(s.endAt)) return null;                                      // not a session, or already ended: never rewritten
  const B = Math.max(start, ms(s.lastSeenAt) || start), cap = nyMidnightAfter(start), C = closingInstant(start);
  const P = Policy.of(s.station);
  if (adm === null) return null;
  // the old rule: a page silent for 15 minutes ended "closed" at its last beat ("midnight" when that beat is past the session's own midnight)
  const silent = () => ({ endAt: Math.min(B, cap), endReason: B > cap ? "midnight" : "closed", rule: B > cap ? "midnight" : "closed" });
  if (adm === true) return now - B >= CLOSED_MS ? silent() : null;                   // an Admin: only the old rule, whatever the station
  if (P.closeAt17 === "always" && start < C && now >= C) return { endAt: C, endReason: "closing", rule: "closing" };      // Welding: 17:00 sharp, whatever the last input
  if (!Policy.hasIdle(P)) return now >= cap ? { endAt: cap, endReason: "midnight", rule: "midnight" } : null;        // no idle rule: only the midnight is left (a Welding session that began after 17:00)
  const Lraw = ms(s.lastInputAt), dead = now >= Policy.deadAt(P, start, B, C, cap);
  if (!(Lraw > 0)) return dead ? silent() : null;                                    // a page that never reported input: its last beat, once its station's silence has passed
  const L = Math.min(Math.max(Lraw, start), B);
  if (!dead && Policy.dueAt(P, L, C) > B) return null;                               // the page spoke recently and its own report is inside the limit: stays
  if (L > cap) return { endAt: cap, endReason: "midnight", rule: "midnight" };
  // "closing" or "idle"? A limit that changes at 17:00 (Laser) is "closing" when the sign-out falls at or after 17:00 and "idle" before it. The default's single
  // limit is "closing" only when the server first hears at or after 17:00 from a person with no input in the window before it, else "idle" (Rule B, as built).
  const closing = Policy.limitChangesAt17(P) ? Policy.dueAt(P, L, C) >= C : start < C && C <= B && L <= C - P.idleMinAfter17 * 60000;
  return { endAt: L, endReason: closing ? "closing" : "idle", rule: closing ? "closing" : "idle" };
}

/** Does the readers' own derived "closed" (a page silent for 15 minutes counted as ended at its last beat) NOT apply to this stored session? True when its station keeps a quiet
 *  page open (Laser inside its limit, Welding before 17:00): the person is still signed in until the rules above end the session. `row` as stored (station, startAt, lastSeenAt). */
function keptOpen(row, now) {
  const start = ms(row && row.startAt); if (!(start > 0)) return false;
  const B = Math.max(start, ms(row.lastSeenAt) || start);
  return Policy.keptOpen(row.station, start, B, closingInstant(start), nyMidnightAfter(start), now == null ? deps.now() : now);
}
/** What a sign-out by the rules is called, in plain words, for a station and a reason (the portal's wording: the person page's Out row); "" for any other reason */
const endText = (station, reason) => Policy.endText(station, reason) || END_TEXT[reason] || "";
/** the same as a short label for a pill (the sorter's Sign-ins window) */
const endPill = (station, reason) => Policy.endPill(station, reason);

/** true | false | null for a stored session row and the loaded list */
function adminState(row, list) {
  const stored = row && row.admin === true ? true : row && row.admin === false ? false : null;
  if (stored === true) return true;
  if (list && list.ok) return list.keys.has(Admins.keyOf(row && row.person));
  return stored === false ? false : null;
}

/* ── ending, one session ── */
const recent = new Map();                                             // id -> time we last tried to end it (this instance)
function tried(key, now) {
  const t = recent.get(key);
  if (t && now - t < RETRY_MS) return true;
  recent.delete(key); recent.set(key, now);
  while (recent.size > 2000) recent.delete(recent.keys().next().value);
  return false;
}
/** Ends one session if it is still open and the rules still say so on the fresh document. Resolves "ended" | "ended-by-then" | "stays" | "gone" | "error". */
async function endOne(db, coll, id, now) {
  try {
    const ref = db.collection(coll).doc(id);
    const list = await Admins.load(db);
    return await db.runTransaction(async tx => {
      const snap = await tx.get(ref);
      if (!snap || !snap.exists) return "gone";
      const v = snap.data() || {};
      if (ms(v.endAt)) return "ended-by-then";
      const d = decide(v, now, adminState(v, list));
      if (!d) return "stays";
      tx.set(ref, { endAt: d.endAt, endReason: d.endReason, minutes: minutesOf(ms(v.startAt), d.endAt) }, { merge: true });
      return "ended";
    });
  } catch (e) {
    console.warn("[autoSignout] could not end a session:", String((e && (e.message || e.code)) || e).slice(0, 120));
    return "error";
  }
}

/**
 * Applies the rules to session rows a reader just loaded. `ctx` = { db, prefix?, now?, write?: boolean (default true), window?: ms, max?: number };
 * `rows` = plain objects as stored, with their document id as `id`. Rows the rules end are PATCHED in place (endAt, endReason, minutes), so the
 * reader's own maths is right even when a write fails; each such session is also ended in Firestore (once: a transaction re-checks it).
 * Never throws. Returns { checked, ended, written, skipped, errors }.
 */
async function settle(ctx, rows) {
  const out = { checked: 0, ended: 0, written: 0, skipped: 0, errors: 0 };
  try {
    const now = ctx.now || deps.now(), prefix = ctx.prefix || "";
    const open = (Array.isArray(rows) ? rows : []).filter(r => r && typeof r === "object" && r.id && !ms(r.endAt) && ms(r.startAt) > 0);
    if (!open.length) return out;
    const list = await Admins.load(ctx.db);
    const win = ctx.window == null ? READ_WRITE_WINDOW_MS : ctx.window, todo = [];
    for (const r of open) {
      out.checked++;
      const adm = adminState(r, list);
      if (adm === null) { out.skipped++; continue; }
      const d = decide(r, now, adm);
      if (!d) continue;
      r.endAt = d.endAt; r.endReason = d.endReason; r.minutes = minutesOf(ms(r.startAt), d.endAt);
      out.ended++;
      if (ctx.write !== false && now - ms(r.startAt) <= win) todo.push(String(r.id));
    }
    const max = ctx.max == null ? MAX_WRITES : ctx.max, ids = todo.filter(id => !tried(prefix + "|" + id, now)).slice(0, max);
    for (let i = 0; i < ids.length; i += 5) {
      const got = await Promise.all(ids.slice(i, i + 5).map(id => endOne(ctx.db, prefix + COLL, id, now)));
      for (const g of got) { if (g === "ended") out.written++; else if (g === "error") out.errors++; }
    }
  } catch (e) {
    out.errors++;
    console.warn("[autoSignout] settle:", String((e && (e.message || e.code)) || e).slice(0, 120));
  }
  return out;
}

/**
 * A query snapshot of sessions after settle(): the same shape (docs[] with id, ref and data()), the open sessions the rules end carrying their
 * end. For a reader whose own code walks `snap.docs` and `d.data()`; the rest of its code does not change. Never throws.
 */
async function settledSnap(ctx, snap) {
  const docs = (snap && Array.isArray(snap.docs)) ? snap.docs : [];
  try {
    const rows = docs.map(d => Object.assign({ id: d.id }, d.data() || {}));
    await settle(ctx, rows);
    return Object.assign({}, snap, { docs: docs.map((d, i) => ({ id: d.id, ref: d.ref, exists: true, data: () => rows[i] })) });
  } catch (e) { return snap; }
}

/* ── the sweep (stationSessionsSweepCron): every open session of a store, ended or left as the rules say ── */
let sweptAt = 0;
/**
 * { db, now?, prefixes?: ["", "Sandbox_"], force? }. One query per store (sessions that are not ended: a single-field equality, no index
 * to build), at most SWEEP_READ read and SWEEP_WRITES written a run. A run within a minute of the last one in this instance is skipped
 * (a scheduled function answers 403 to a direct call; this only keeps a stray call cheap). Never throws.
 */
async function sweep(opts) {
  const db = opts.db, now = opts.now || deps.now(), prefixes = opts.prefixes || ["", "Sandbox_"];
  if (!opts.force && now - sweptAt < 60000) return { skipped: true };
  sweptAt = now;
  const stores = {};
  for (const prefix of prefixes) {
    const key = prefix ? "sandbox" : "real";
    try {
      const snap = await db.collection(prefix + COLL).where("endAt", "==", null).limit(SWEEP_READ).get();
      const rows = snap.docs.map(d => Object.assign({ id: d.id }, d.data() || {}));
      const r = await settle({ db, prefix, now, window: Infinity, max: SWEEP_WRITES }, rows);
      stores[key] = Object.assign({ open: rows.length }, r);
    } catch (e) {
      stores[key] = { error: String((e && (e.message || e.code)) || e).slice(0, 120) };
      console.warn("[autoSignout] sweep of", key, "failed:", stores[key].error);
    }
  }
  return { stores };
}
function resetSweep() { sweptAt = 0; recent.clear(); }

module.exports = { COLL, IDLE_MS, CLOSED_MS, CLOSING_ZONE, CLOSING_HOUR, END_REASONS, END_TEXT, POLICY: Policy.POLICY, policyOf: Policy.of, policy: Policy.copyOf, endText, endPill, keptOpen, decide, adminState, settle, settledSnap, sweep, endOne,
  closingInstant, nyMidnightAfter, skewOf, pageTime, minutesOf, resetSweep, deps };
