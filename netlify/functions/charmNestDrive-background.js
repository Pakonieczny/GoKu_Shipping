/*  netlify/functions/charmNestDrive-background.js — one pass of the cloud driver (_charmNestDrive.js; cloud plan v2 §1.1, build step 2) with Netlify's 15-minute
 *  background budget (the pass keeps to 12). Started only by charmNestDriveCron, driveSoon (charmNestLibrary.js) or a pass that asks for another, each with the
 *  single-use nonce of the driver's lease (Charm_Nest_Drive/lease, _charmNestLease.js): a call without it does nothing. DORMANT: while the drive switch is absent or
 *  "off" the lease refuses the pass and nothing is written. The driver is given Lib.call and Lib.cloudOps only, through the library module (never its ops table).
 *  Its steps and pass items are the build steps' unit modules (_charmNestSteps.js; hard-parts §0.1).                                                                   */
"use strict";
const { parseBody } = require("./_charmNestAuth");

/* PASSNOW (23 Nov 2026, items 21 and 22: a press waited 61 s): a worker that found no pass running left its request for the next one and started none, so it waited for the cron, up to a minute. It asks for a pass now (driveSoon: never while the drive is off; one that runs already is asked to go again). */
const passFor = async (r, what) => { if (!r || !r.again || r.again === "the run is closed") return; try { await require("./charmNestLibrary").driveSoon(what + " waits"); } catch (e) { console.warn("[drive] no pass asked:", (e && e.message) || e); } };

exports.handler = async (event) => {
  const body = parseBody(event), nonce = String(body.nonce || "");
  /* APPROVENOW: the file of one approval, asked by engraveApprove with the approval's own single-use key: written here, beside the driver's pass and under ITS lease (every write is fenced on it), never waiting on the pass's own work */
  if (!nonce && body.engrave && body.key) {
    try { const r = await require("./_charmEngraveFiles").nowTask({ db: require("./firebaseAdmin").firestore(), Lib: require("./charmNestLibrary"), taskId: String(body.engrave).slice(0, 220), key: String(body.key).slice(0, 80), say: t => console.log(t) }); console.log("[engrave now]", String(body.engrave).slice(0, 120), JSON.stringify(r).slice(0, 300)); await passFor(r, "engrave"); }
    catch (e) { console.error("[engrave now] failed:", (e && e.message) || e); }
    return { statusCode: 200, body: "done" };
  }
  /* CMDFAST: one person's request (a Change SKU, a piece made again), asked by runCmdPut with the request's own key: done here, beside the pass and under its lease */
  if (!nonce && body.cmd && body.runId && body.key) {
    try { const r = await require("./_charmNestCmdUnit").nowRun({ db: require("./firebaseAdmin").firestore(), Lib: require("./charmNestLibrary"), runId: String(body.runId).slice(0, 80), id: String(body.cmd).slice(0, 40), key: String(body.key).slice(0, 80), say: t => console.log(t) }); console.log("[cmd now]", String(body.cmd).slice(0, 40), JSON.stringify(r).slice(0, 300)); await passFor(r, "cmd"); }
    catch (e) { console.error("[cmd now] failed:", (e && e.message) || e); }
    return { statusCode: 200, body: "done" };
  }
  if (!nonce) return { statusCode: 200, body: "not this pass's turn" };
  const admin = require("./firebaseAdmin"), db = admin.firestore(), Drive = require("./_charmNestDrive"), Lease = require("./_charmNestLease");
  let out = null;
  try {
    const Lib = require("./charmNestLibrary"), Steps = require("./_charmNestSteps");   // (the real units of the build steps that have landed: hard-parts §0.1)
    out = await Drive.pass({ db, Lib: { call: Lib.call, cloudOps: Lib.cloudOps }, nonce, budgetMs: 12 * 60000, log: t => console.log(t), steps: Steps.steps, passItems: Steps.passItems, shadow: Steps.shadow });
    console.log("[drive] pass", JSON.stringify(out).slice(0, 800));
  } catch (e) { console.error("[drive] pass failed:", (e && e.message) || e); }
  // (work is left, or a change was asked while it ran: the next pass, at once)
  if (out && out.again) { const next = await Lease.dispatch(db, db.collection("Charm_Nest_Drive").doc("lease"), "again", { heartbeat: true }).catch(() => null); if (next && next.nonce) await Drive.kick(next.nonce); }
  return { statusCode: 200, body: out && out.ok ? "passed" : "not passed" };
};
