/*  netlify/functions/_stationSignoutPolicy.js
 *  Who is signed out automatically, and when, station by station (Paul, 6 Oct 2026, 20:10 UTC: "Do not auto log out users from the Laser
 *  Cutting or Welding ... 1. For Stud Welding only auto log out users after 5pm Toronto time. 2. For Laser Cutting only auto-logout users
 *  after 1 hr of inactivity instead of the typical 10 min. Also after auto log out users after 5pm Toronto time if no activity for at least
 *  30 min after 5 pm").
 *
 *  ONE TABLE. station-session.js (the page, AD3) carries the very same table value for value in its `POLICY`; tests/stations/auto-signout-server.cjs
 *  reads that table out of the page's source and asserts the two are equal, so they can never drift apart. Change a number here and the page's
 *  must change with it (the test fails until it does). Admin is exempt from everything in it (never listed); midnight New York ends everybody.
 *
 *    idleMin          minutes without input that sign a non-Admin person out BEFORE 17:00 Toronto of the day (reason "idle"); 0 = no idle rule
 *    idleMinAfter17   the same limit from 17:00 Toronto of the day on; 0 = none
 *    closeAt17        "idleWindow": at 17:00 a person with no input in the previous `idleMinAfter17` minutes is signed out at their LAST INPUT (closing)
 *                     "always":     every person signed in at 17:00 is signed out at 17:00 SHARP (closing), whatever their last input
 *
 *  A station with no row of its own (and "") gets `default`. The keys are the session's stored `station`: the Sorter app's Laser person is
 *  station "laser", its Design person "design", a person with no role "sorter" (default); the Welding page is "welding" for both tasks.
 *
 *  Pure: no Firestore, no clock, no zone arithmetic (the caller passes C, the closing instant). Contract: plans/stations-round2/api.md "AD4". */
"use strict";

const MIN = 60000, CLOSED_MIN = 15;     // a page with no beat for 15 minutes is "dead" (the old `closed` rule), unless its station's limit is longer (see deadAt)

const POLICY = Object.freeze({
  default: Object.freeze({ idleMin: 10, idleMinAfter17: 10, closeAt17: "idleWindow" }),   // Sorting, Assembly, Shipping, Design apps, Inbox, Sorter app (Admin / no role / Design role), qr, sorter
  welding: Object.freeze({ idleMin: 0, idleMinAfter17: 0, closeAt17: "always" }),         // station welding (weld-1; every task, every person)
  laser: Object.freeze({ idleMin: 60, idleMinAfter17: 30, closeAt17: "idleWindow" })      // station laser (the Sorter app with the Laser role)
});

/** the station's row (a station of no row of its own, "" or a name like "constructor": `default`) */
function of(station) {
  const k = String(station == null ? "" : station).trim().toLowerCase();
  return k !== "default" && Object.prototype.hasOwnProperty.call(POLICY, k) ? POLICY[k] : POLICY.default;
}
/** a copy of the station's row (what the page's StationSession.policy(station) answers) */
const copyOf = station => Object.assign({}, of(station));

const hasIdle = P => P.idleMin > 0 || P.idleMinAfter17 > 0;
/** true when the limit takes over at 17:00 with a number of its own (Laser: 60 then 30), false when 17:00 only brings the window of the one limit (the default) */
const limitChangesAt17 = P => P.idleMinAfter17 !== P.idleMin;

/**
 * The first moment (ms) a person whose last input, or last sign of life, was `x` is due for the IDLE rule: Infinity for never.
 * Before 17:00 (`C`, the closing instant of the session's Toronto day) the limit is idleMin; from 17:00 on it is idleMinAfter17.
 * Laser, last input 16:25: 16:25 + 60 is past 17:00, so the answer is the later of 17:00 and 16:25 + 30 = 17:00. Last input 16:45: 17:15.
 * Last input 15:50: 16:50 (before 17:00). The default (10 and 10) is always x + 10 minutes.
 */
function dueAt(P, x, C) {
  let t = Infinity;
  if (P.idleMin > 0 && x + P.idleMin * MIN < C) t = x + P.idleMin * MIN;
  if (P.idleMinAfter17 > 0) t = Math.min(t, Math.max(C, x + P.idleMinAfter17 * MIN));
  return t;
}
/**
 * The first moment (ms) a page that has not beaten since `B` counts as dead, so that the SERVER may end its session:
 *   the old `closed` window (15 minutes) or, when the station's limit is longer, the moment its limit has passed since that beat
 *   (Laser: 60 minutes, 30 from 17:00; so a sleeping computer or a closed tab does not end a Laser session early),
 *   Welding (no idle rule): never by silence; its end is 17:00 (closeAt17 "always"), or the midnight for a session that began after 17:00.
 * `start` and `C` as in dueAt; `cap` = the New York midnight after the start.
 */
function deadAt(P, start, B, C, cap) {
  if (!hasIdle(P)) return P.closeAt17 === "always" && start < C ? C : cap;
  return Math.max(B + CLOSED_MIN * MIN, dueAt(P, B, C));
}
/** Is a session whose page has been silent for 15 minutes still open by its station's rule at `now`? (Laser inside its limit, Welding before 17:00.) Readers skip their derived "closed" for it. For a station with the default window it is false whenever the old 15 minutes have passed. */
function keptOpen(station, start, B, C, cap, now) { return now < deadAt(of(station), start, B, C, cap); }

/* ── the words ── */
/** "10 minutes", "1 hour", "2 hours", "90 minutes" */
function span(min) { const n = Math.round(min); return n >= 60 && n % 60 === 0 ? (n / 60 === 1 ? "1 hour" : `${n / 60} hours`) : `${n} minute${n === 1 ? "" : "s"}`; }
/** the short form for a pill: "10 min", "1 hour" */
function spanShort(min) { const n = Math.round(min); return n >= 60 && n % 60 === 0 ? (n / 60 === 1 ? "1 hour" : `${n / 60} hours`) : `${n} min`; }
/**
 * What a sign-out by the rules is called for the people reading it, by station and end reason ("" for any other reason):
 *   default   idle "Signed out after 10 minutes without input"           closing "Signed out at 5:00 pm"
 *   laser     idle "Signed out after 1 hour without input"               closing "Signed out at 5:00 pm after 30 minutes without input"
 *   welding   closing "Signed out at 5:00 pm"                            (there is no idle sign-out)
 */
function endText(station, reason) {
  const P = of(station);
  if (reason === "idle") return P.idleMin > 0 ? `Signed out after ${span(P.idleMin)} without input` : "";
  if (reason === "closing") return P.closeAt17 === "idleWindow" && limitChangesAt17(P) && P.idleMinAfter17 > 0 ? `Signed out at 5:00 pm after ${span(P.idleMinAfter17)} without input` : "Signed out at 5:00 pm";
  return "";
}
/** the same as a short label (the Sign-ins window's pill): "10 min without input", "1 hour without input", "5:00 pm", "5:00 pm · 30 min without input" */
function endPill(station, reason) {
  const P = of(station);
  if (reason === "idle") return P.idleMin > 0 ? `${spanShort(P.idleMin)} without input` : "";
  if (reason === "closing") return P.closeAt17 === "idleWindow" && limitChangesAt17(P) && P.idleMinAfter17 > 0 ? `5:00 pm · ${spanShort(P.idleMinAfter17)} without input` : "5:00 pm";
  return "";
}

module.exports = { POLICY, MIN, CLOSED_MIN, of, copyOf, hasIdle, limitChangesAt17, dueAt, deadAt, keptOpen, endText, endPill, span };
