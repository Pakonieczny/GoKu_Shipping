/*  netlify/functions/charmNestDrive-background.js — one pass of the cloud driver (_charmNestDrive.js; cloud plan v2 §1.1, build step 2) with Netlify's 15-minute
 *  background budget (the pass keeps to 12). Started only by charmNestDriveCron, driveSoon (charmNestLibrary.js) or a pass that asks for another, each with the
 *  single-use nonce of the driver's lease (Charm_Nest_Drive/lease, _charmNestLease.js): a call without it does nothing. DORMANT: while the drive switch is absent or
 *  "off" the lease refuses the pass and nothing is written. The driver is given Lib.call and Lib.cloudOps only, through the library module (never its ops table).      */
"use strict";
const { parseBody } = require("./_charmNestAuth");

exports.handler = async (event) => {
  const body = parseBody(event), nonce = String(body.nonce || "");
  if (!nonce) return { statusCode: 200, body: "not this pass's turn" };
  const admin = require("./firebaseAdmin"), db = admin.firestore(), Drive = require("./_charmNestDrive"), Lease = require("./_charmNestLease");
  let out = null;
  try {
    const Lib = require("./charmNestLibrary");
    out = await Drive.pass({ db, Lib: { call: Lib.call, cloudOps: Lib.cloudOps }, nonce, budgetMs: 12 * 60000, log: t => console.log(t) });
    console.log("[drive] pass", JSON.stringify(out).slice(0, 800));
  } catch (e) { console.error("[drive] pass failed:", (e && e.message) || e); }
  // (work is left, or a change was asked while it ran: the next pass, at once)
  if (out && out.again) { const next = await Lease.dispatch(db, db.collection("Charm_Nest_Drive").doc("lease"), "again", { heartbeat: true }).catch(() => null); if (next && next.nonce) await Drive.kick(next.nonce); }
  return { statusCode: 200, body: out && out.ok ? "passed" : "not passed" };
};
