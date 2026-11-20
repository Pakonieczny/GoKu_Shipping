/*  netlify/functions/_charmNestWorldLease.js — one build of the cloud world at a time (_charmNestWorld.js).
 *  Charm_World/lease { nonce, state: dispatched | running | done | failed, at, by, startedAt, endedAt, result, error }.
 *  dispatch(): takes the lease (a transaction: none held, or the one held is done, failed or older than STALE_MS) and gives a single-use nonce to the
 *  background function; the background function runs only with that nonce, once (begin()). A build asked while one runs is remembered (`again`) and
 *  dispatched by the one running when it ends, so a change is never left out.                                                                   */
"use strict";
const crypto = require("crypto");
const WORLD = "Charm_World", STALE_MS = 14 * 60000, DISPATCH_STALE_MS = 2 * 60000;

async function dispatch(db, by, opts = {}) {
  const ref = db.collection(WORLD).doc("lease"), now = Date.now(), nonce = crypto.randomBytes(12).toString("hex");
  const out = await db.runTransaction(async tx => {
    const s = await tx.get(ref), d = s.exists ? s.data() || {} : {};
    const busy = (d.state === "running" && now - (+d.startedAt || +d.at || 0) < STALE_MS) || (d.state === "dispatched" && now - (+d.at || 0) < DISPATCH_STALE_MS);
    if (busy) { tx.set(ref, { again: true, againBy: String(by || "").slice(0, 60), againAt: now }, { merge: true }); return { ok: true, queued: true, state: d.state }; }
    tx.set(ref, { nonce, state: "dispatched", at: now, by: String(by || "").slice(0, 60), sweep: !!opts.sweep, again: false, startedAt: null, endedAt: null, error: null });
    return { ok: true, nonce };
  });
  return out;
}

async function begin(db, nonce) {
  const ref = db.collection(WORLD).doc("lease"), now = Date.now();
  return db.runTransaction(async tx => {
    const s = await tx.get(ref), d = s.exists ? s.data() || {} : {};
    if (!nonce || d.nonce !== nonce || d.state !== "dispatched") return null;
    tx.set(ref, { state: "running", startedAt: now }, { merge: true });
    return { sweep: !!d.sweep, by: d.by || "" };
  });
}

async function end(db, nonce, result, error) {
  const ref = db.collection(WORLD).doc("lease"), now = Date.now();
  return db.runTransaction(async tx => {
    const s = await tx.get(ref), d = s.exists ? s.data() || {} : {};
    if (d.nonce !== nonce) return { again: false };
    tx.set(ref, { state: error ? "failed" : "done", endedAt: now, error: error ? String(error).slice(0, 300) : null, result: result || null }, { merge: true });
    return { again: !!d.again };
  });
}

async function state(db) { const s = await db.collection(WORLD).doc("lease").get(); return s.exists ? s.data() : null; }

module.exports = { dispatch, begin, end, state, WORLD };
