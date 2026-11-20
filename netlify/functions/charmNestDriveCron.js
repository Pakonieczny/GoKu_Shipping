/*  netlify/functions/charmNestDriveCron.js — every minute, with no page open: the cloud driver's safety net (cloud plan v2 §1.1, build step 2). DORMANT: with
 *  Charm_Nest_Open/drive absent or in mode "off" it reads that one document and nothing else. In "standby" or "on" it keeps the cloud's carrier of the run alive (its
 *  only write) and starts a pass of the driver (charmNestDrive-background.js) when something is to be done. It only dispatches; it never starts a world build.
 *  Declared here and in netlify.toml, like every other cron.                                                                                                    */
"use strict";
exports.handler = async function () {
  try {
    const admin = require("./firebaseAdmin"), db = admin.firestore(), Drive = require("./_charmNestDrive");
    const out = await Drive.tick({ db, kick: Drive.kick });
    if (!out.off) console.log("[drive] tick", JSON.stringify({ why: out.why, dispatched: out.dispatched, queued: !!out.queued, kept: !!out.kept, reads: out.reads }));
    return { statusCode: 200, body: JSON.stringify({ ok: true, off: !!out.off, dispatched: !!out.dispatched }) };
  } catch (e) {
    console.error("[charmNestDriveCron] failed:", e && e.message);
    return { statusCode: 500, body: JSON.stringify({ ok: false, error: String((e && e.message) || e).slice(0, 160) }) };
  }
};
exports.config = { schedule: "* * * * *" };
