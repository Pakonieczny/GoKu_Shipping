/*  netlify/functions/_charmNestLease.js — one holder at a time for a cloud task (cloud plan v2, section 1.1 and 1.3; build step 1a, E3, C7).
 *  Used ONLY by the cloud driver's lease (Charm_Nest_Drive/lease) and the worker task documents. The world build keeps its own lease,
 *  _charmNestWorldLease.js, which this file does not touch and does not replace (E3: two world builds must never run at once).
 *
 *  The lease document is passed in (a DocumentReference). It holds { nonce, state: dispatched | running | done | failed, at, by, startedAt, beatAt,
 *  endedAt, again, error }.
 *    dispatch(db, ref, by)     a single-use nonce when the lease is free (no holder, the holder ended, or the holder is stale); otherwise the holder is
 *                              told to run again when it ends (again) and no nonce is given.
 *    begin(db, ref, nonce, o)  the holder of that nonce starts (state running). o.check(tx, lease) runs inside the same transaction, after the lease is
 *                              read and before anything is written: it reads what it needs and answers { ok: false, why } (nothing is written) or
 *                              { ok: true, writes: [[ref, data, opts]], info } (written in the same commit).
 *    renew(db, ref, nonce, o)  the holder says it is alive (beatAt, when the heartbeat is on) and may save o.patch with it; o.check as in begin.
 *    end(db, ref, nonce, ...)  the holder is done; answers { again } (a dispatch was asked while it ran).
 *  Stale: a dispatched lease nobody began after DISPATCH_STALE_MS; a running lease older than START_STALE_MS (14 min, as _charmNestWorldLease.js; v1's
 *  14.5 min left no margin under Netlify's 15), or, ONLY when the heartbeat is on for it, one whose beat is older than BEAT_STALE_MS (5 min).
 *  The heartbeat is opt-in (o.heartbeat: true): it is never on by default.                                                                       */
"use strict";
const crypto = require("crypto");
const BEAT_STALE_MS = 5 * 60000, START_STALE_MS = 14 * 60000, DISPATCH_STALE_MS = 2 * 60000;

/** Whether the lease d is held by a live holder at time now. */
function held(d, now) {
  if (!d || !d.nonce) return false;
  if (d.state === "dispatched") return now - (+d.at || 0) < DISPATCH_STALE_MS;
  if (d.state !== "running") return false;
  const started = +d.startedAt || +d.at || 0;
  if (now - started >= START_STALE_MS) return false;
  if (d.heartbeat && now - (+d.beatAt || started) >= BEAT_STALE_MS) return false;
  return true;
}
const stale = (d, now) => !!(d && d.nonce && (d.state === "dispatched" || d.state === "running") && !held(d, now));

async function dispatch(db, ref, by, o = {}) {
  const now = Date.now(), nonce = crypto.randomBytes(12).toString("hex");
  return db.runTransaction(async tx => {
    const s = await tx.get(ref), d = s.exists ? s.data() || {} : {};
    if (held(d, now)) { tx.set(ref, { again: true, againBy: String(by || "").slice(0, 60), againAt: now }, { merge: true }); return { ok: true, queued: true, state: d.state }; }
    tx.set(ref, { nonce, state: "dispatched", at: now, by: String(by || "").slice(0, 60), again: false, startedAt: null, beatAt: null, endedAt: null, error: null, heartbeat: !!o.heartbeat, ...(stale(d, now) ? { tookFrom: { nonce: d.nonce, state: d.state, at: +d.at || 0 } } : {}) }, { merge: true });
    return { ok: true, nonce };
  });
}

async function begin(db, ref, nonce, o = {}) {
  const now = Date.now();
  return db.runTransaction(async tx => {
    const s = await tx.get(ref), d = s.exists ? s.data() || {} : {};
    if (!nonce || d.nonce !== nonce || d.state !== "dispatched") return null;
    const c = o.check ? await o.check(tx, d) : { ok: true };
    if (!c || c.ok === false) return { refused: true, why: (c && c.why) || "refused" };
    const hb = o.heartbeat == null ? !!d.heartbeat : !!o.heartbeat;
    tx.set(ref, Object.assign({ state: "running", startedAt: now, heartbeat: hb }, hb ? { beatAt: now } : {}, o.patch || {}), { merge: true });
    for (const [r, data, opts] of c.writes || []) tx.set(r, data, opts || {});
    return { ok: true, by: d.by || "", startedAt: now, info: c.info || null };
  });
}

async function renew(db, ref, nonce, o = {}) {
  const now = Date.now();
  return db.runTransaction(async tx => {
    const s = await tx.get(ref), d = s.exists ? s.data() || {} : {};
    if (!nonce || d.nonce !== nonce || d.state !== "running") return { ok: false, lost: true };
    const c = o.check ? await o.check(tx, d) : { ok: true };
    if (!c || c.ok === false) return { ok: false, why: (c && c.why) || "refused" };
    tx.set(ref, Object.assign({}, d.heartbeat ? { beatAt: now } : {}, o.patch || {}), { merge: true });
    for (const [r, data, opts] of c.writes || []) tx.set(r, data, opts || {});
    return { ok: true, info: c.info || null };
  });
}

async function end(db, ref, nonce, result, error) {
  const now = Date.now();
  return db.runTransaction(async tx => {
    const s = await tx.get(ref), d = s.exists ? s.data() || {} : {};
    if (!nonce || d.nonce !== nonce) return { again: false, lost: true };
    tx.set(ref, { state: error ? "failed" : "done", endedAt: now, error: error ? String(error).slice(0, 300) : null, result: result || null }, { merge: true });
    return { again: !!d.again };
  });
}

async function state(ref) { const s = await ref.get(); return s.exists ? s.data() : null; }

module.exports = { dispatch, begin, renew, end, state, held, stale, BEAT_STALE_MS, START_STALE_MS, DISPATCH_STALE_MS };
