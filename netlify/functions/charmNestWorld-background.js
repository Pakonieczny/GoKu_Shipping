/*  netlify/functions/charmNestWorld-background.js — builds the cloud world (_charmNestWorld.js) with Netlify's 15-minute background budget.
 *  Started only by charmNestWorldCron (every 2 minutes) or charmNestLibrary op worldBuild, each with the single-use nonce of the lease
 *  (_charmNestWorldLease.js): a call without it does nothing. When it ends, the changes feed names w:world, so every page reads the new world.
 *  A build asked for while this one ran is started again at its end.                                                                          */
"use strict";
const admin = require("./firebaseAdmin");
const { parseBody } = require("./_charmNestAuth");
const Lease = require("./_charmNestWorldLease");
const db = admin.firestore();

async function kick(nonce) {
  const fetch = require("node-fetch"), origin = process.env.URL || process.env.DEPLOY_PRIME_URL || "https://goldenspike.app";
  try { await fetch(`${String(origin).replace(/\/+$/, "")}/.netlify/functions/charmNestWorld-background`, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ nonce }) }); }
  catch (e) { console.warn("[world] could not start the next build:", e && e.message); }
}

exports.handler = async (event) => {
  const body = parseBody(event), nonce = String(body.nonce || "");
  const lease = await Lease.begin(db, nonce).catch(e => { console.warn("[world] lease:", e && e.message); return null; });
  if (!lease) return { statusCode: 200, body: "not this build's turn" };
  let result = null, error = null;
  try {
    const Lib = require("./charmNestLibrary");
    result = await require("./_charmNestWorld").build({ db, admin, Lib, sweep: lease.sweep, log: t => console.log(t), budgetMs: 12 * 60000 });
    if (!result.skipped) { try { if (typeof Lib.worldBump === "function") await Lib.worldBump(); } catch (e) { console.warn("[world] feed not raised:", e && e.message); } }
    console.log("[world] built", JSON.stringify({ live: result.live, writes: result.writes, states: result.states, errors: result.errors }).slice(0, 800));
  } catch (e) { error = (e && e.message) || String(e); console.error("[world] build failed:", error); }
  const brief = result ? { at: +result.at || Date.now(), skipped: !!result.skipped, live: +result.live || 0, writes: +result.writes || 0, errors: (result.errors || []).length } : null;
  const ended = await Lease.end(db, nonce, brief, error).catch(() => ({ again: false }));
  // (a change made while this build ran is built now; at most one build every 30 s: this one waits out what is left of the gap, inside its own 15 minutes)
  if (ended.again) { const since = Date.now() - (lease.startedAt || Date.now()); if (since < Lease.MIN_GAP_MS) await new Promise(r => setTimeout(r, Lease.MIN_GAP_MS - since)); const next = await Lease.dispatch(db, "again").catch(() => null); if (next && next.nonce) await kick(next.nonce); }
  return { statusCode: 200, body: error ? "failed" : "built" };
};
