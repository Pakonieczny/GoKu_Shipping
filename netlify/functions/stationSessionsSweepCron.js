/*  netlify/functions/stationSessionsSweepCron.js
 *
 *  Scheduled sweep of the stations' open sign-in sessions (Paul, 6 Oct 2026: auto sign-out of everyone but the Admin after 10 minutes
 *  without input and at 5:00 pm Toronto time). The page ends its own session; this ends the ones whose page died or never said so, with the
 *  same honest end: the time of the person's LAST INPUT, never the time of this run. The rules are in _stationAutoSignout.js and are the
 *  same ones the readers (the live board, the person page, the sorter's Sign-ins list) and the session door apply; this only makes sure a
 *  session is ended in Firestore even when nobody is looking.
 *
 *  What it does, every 5 minutes: per store (real, Sandbox_) one query for the sessions that are not ended (single-field equality: no index
 *  to build), at most 300 read and 100 ended per run; every end is one transaction that re-checks the fresh document, so a beat that arrived
 *  meanwhile wins. Admins are exempt from the idle and 5 pm rules. Each station follows its own row of the one policy table
 *  (_stationSignoutPolicy.js; Addendum 2): Welding is ended only at 17:00 Toronto (17:00 sharp, "closing"), Laser after 60 minutes without input
 *  (30 from 17:00), and a quiet page does not end either of them early. It never deletes anything, never rewrites an ended session, never calls
 *  Etsy and never touches another collection.
 *
 *  The schedule is declared in this file (in-file config, kept literal by scripts/build-netlify.cjs); netlify.toml is not changed.
 *  A scheduled function answers 403 to a direct call, so it cannot be tried by hand: tests/stations/auto-signout-server.cjs runs sweep()
 *  against a fake clock and a fake Firestore.  Contract: /mnt/project-files/plans/stations-round2/api.md ("AD2"). */
"use strict";
const admin = require("./firebaseAdmin");
const db = admin.firestore();
const AutoSignout = require("./_stationAutoSignout");

exports.handler = async function () {
  try {
    const out = await AutoSignout.sweep({ db });
    console.log("[stationSessionsSweepCron]", JSON.stringify(out));
    return { statusCode: 200, body: JSON.stringify(Object.assign({ ok: true }, out)) };
  } catch (e) {
    console.error("[stationSessionsSweepCron] failed:", e && e.message);
    return { statusCode: 500, body: JSON.stringify({ ok: false, error: String((e && e.message) || e).slice(0, 160) }) };
  }
};

exports.config = {
  schedule: "*/5 * * * *"
};
