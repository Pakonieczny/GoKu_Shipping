/*  netlify/functions/charmNestSheet-background.js — THE ONE SHEET WRITER (cloud plan v2 §1.1 b, build step 7a): one sheet's files (.ai, labelled proof, nest report, preview), its checks
 *  and automatic overlap fixes, its files in Storage and its record (_charmNestFinish.js), with Netlify's 15-minute background budget (each invocation keeps to 12; a job that needs more
 *  goes on in the next one from its saved step). Started only by the cloud driver's files lane (op finishSheet with start), or by itself when its job goes on, each time with the
 *  single-use nonce of the sheet's task lease (Charm_Nest_Finish/<sheetId>, _charmNestLease, heartbeat on): a call without it does nothing.
 *  DORMANT: while Charm_Nest_Open/drive is absent or "off" (or "files" is not in its cloudSteps, or a computer carries the sheet's run) the lease refuses to begin and nothing is written.
 *  The writer is given Lib.call only (the cloud's in-process, fenced call of charmNestLibrary), never its ops table. When a job ends it kicks the driver (driveSoon "task"). */
"use strict";
const { parseBody } = require("./_charmNestAuth");

exports.handler = async (event) => {
  const body = parseBody(event), sheetId = String(body.sheetId || "").replace(/[^\w\-]/g, "").slice(0, 80), nonce = String(body.nonce || "").replace(/[^\w]/g, "").slice(0, 64);
  if (!sheetId || !nonce) return { statusCode: 200, body: "not this task's turn" };
  const admin = require("./firebaseAdmin"), db = admin.firestore(), Finish = require("./_charmNestFinish"), Lease = require("./_charmNestLease"), Ops = require("./_charmNestFinishOps");
  let out = null, Lib = null;
  try {
    Lib = require("./charmNestLibrary");
    out = await Finish.run({ db, Lib: { call: Lib.call }, bucket: admin.storage().bucket(), sheetId, nonce, budgetMs: 12 * 60000, log: t => console.log(t) });
    console.log("[finish] " + sheetId, JSON.stringify(out).slice(0, 800));
  } catch (e) { console.error("[finish] " + sheetId + " failed:", (e && e.message) || e); }
  // (its job goes on, or it was asked again while it ran: the next invocation, at once, on a new nonce of the same task)
  if (out && out.again) {
    const next = await Lease.dispatch(db, db.collection(Finish.FINISH).doc(sheetId), "again", { heartbeat: true }).catch(() => null);
    if (next && next.nonce) await Ops.kickWriter(sheetId, next.nonce);
  }
  // (a job ended, done or not: the driver looks at the run again, the overflow, the sets and what else waited on this sheet; never while the drive is off)
  if (out && out.ended && Lib && typeof Lib.driveSoon === "function") await Lib.driveSoon("task").catch(() => null);
  return { statusCode: 200, body: out && out.ok ? "written" : "not written" };
};
