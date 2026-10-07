/*  netlify/functions/_employeeRev.js
 *  The employee data REVISION (Firebase cost emergency, 7 Oct 2026, FC5). The Employee console polls every few seconds (the live board every
 *  3 s, the overview every 10 s, the person pages every 15 s) and every poll used to re-read today's sessions, rollups and live documents even
 *  when nothing had happened. Reads must follow CHANGES, not time: one tiny document counts the changes, the readers look at it first and
 *  keep what they already read while it has not moved.
 *
 *      Station_Rev/employee   { act, ses, live, at }       (production only; the Sandbox is small, emptied by Reset, and never uses it)
 *          act   a Station_Activity event / Efficiency_Daily rollup was written          (_stationActivity.add)
 *          ses   a Station_Sessions document was started or ended (a person signed in, out, or the rules ended it)
 *                (firebaseOrders {session} start / end / a beat that ended it, _stationAutoSignout endOne)
 *          live  a Station_Live document got a new order or went idle  (_stationLive.write work / idle; a keep-alive beat is NOT a change)
 *
 *  afterWrite(db, prefix, kinds)   called by the writers AFTER their own commit; one set(merge) with increments; never throws, never fails a
 *                                  write (a lost bump only means the readers refresh at their maximum age instead).
 *  read(db, now)                   what the readers compare: { ok, act, ses, live } or { ok: false } (also while the document does not exist yet); kept 1 s in the instance, one read shared
 *                                  by concurrent callers; any failure is { ok: false } and the readers behave exactly as before this file.
 *  The revision is only ever a reason to skip a read: every reader still re-reads at its maximum age (live documents 45 s, sessions 1 minute, rollups and events 2 minutes),
 *  so a bump that never arrived is a delay of seconds, not a stale screen. Nothing here reads or writes anything else.  */
"use strict";
const COLL = "Station_Rev", DOC = "employee", KINDS = ["act", "ses", "live"];
const MEMO_MS = 1000;

const memos = new WeakMap();
function memoOf(db) { let m = memos.get(db); if (!m) memos.set(db, m = { at: 0, p: null, v: null }); return m; }

/** Count a change. kinds: some of "act" | "ses" | "live". Production only (prefix ""). Resolves true when written; never throws. */
async function afterWrite(db, prefix, kinds, FieldValue) {
  try {
    if (prefix || !db) return false;
    let FV = FieldValue;                                               // (the writer's own FieldValue; else the shared admin, when it can be loaded)
    if (!FV) { try { FV = require("./firebaseAdmin").firestore.FieldValue; } catch (_) { return false; } }
    if (!FV || typeof FV.increment !== "function") return false;
    const patch = { at: Date.now() };
    for (const k of kinds || []) if (KINDS.includes(k)) patch[k] = FV.increment(1);
    if (Object.keys(patch).length < 2) return false;
    await db.collection(COLL).doc(DOC).set(patch, { merge: true });
    const m = memoOf(db); m.at = 0;                                    // (this instance sees its own change at once)
    return true;
  } catch (e) {
    console.warn("[employeeRev] a change could not be counted (readers fall back to their maximum age):", String((e && (e.message || e.code)) || e).slice(0, 120));
    return false;
  }
}

/** The revision now: { ok: true, act, ses, live } or { ok: false } (not readable, or no change counted yet). Never throws. */
function read(db, now) {
  const m = memoOf(db), t = now == null ? Date.now() : now;
  if (m.p && t - m.at < MEMO_MS) return m.p;
  const p = (async () => {
    try {
      const snap = await db.collection(COLL).doc(DOC).get();
      if (!snap || !snap.exists) return { ok: false, missing: true };      // (no writer has counted a change yet, or its writes fail: unknown, so the readers keep their short lifetimes)
      const d = snap.data() || {};
      const n = v => (Number.isFinite(+v) ? +v : 0);
      return { ok: true, act: n(d.act), ses: n(d.ses), live: n(d.live) };
    } catch (_) { return { ok: false }; }
  })();
  m.at = t; m.p = p;
  p.then(r => { if (!r.ok && !r.missing && m.p === p) { m.p = null; m.at = 0; } });   // (a failed read is not remembered; a document not there yet is, for the second)
  return p;
}

module.exports = { COLL, DOC, KINDS, afterWrite, read };
