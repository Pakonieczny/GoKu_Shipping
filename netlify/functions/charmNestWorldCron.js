/*  netlify/functions/charmNestWorldCron.js — every 2 minutes, with no page open: the cloud world is built again (charmNestWorld-background.js,
 *  _charmNestWorld.js). It only takes the lease and starts the background build; a build still running is left alone (it starts the next one
 *  itself when it ends). Declared here and in netlify.toml, like every other cron.                                                              */
"use strict";
exports.handler = async function () {
  try {
    // (NIGHTQUIET, Paul 25 Nov 2026: 9 PM to 7 AM in Toronto, nobody works: the world is built every 15 minutes instead of every 2; a person's change still builds it at once, charmNestLibrary worldSoon)
    if (require("./_charmNestDrive").nightNow(Date.now()) && new Date().getUTCMinutes() % 15 > 1) return { statusCode: 200, body: JSON.stringify({ ok: true, skipped: "night" }) };
    const admin = require("./firebaseAdmin"), db = admin.firestore(), Lease = require("./_charmNestWorldLease");
    const got = await Lease.dispatch(db, "cron");
    if (!got.nonce) return { statusCode: 200, body: JSON.stringify({ ok: true, skipped: got.state || "busy" }) };
    const fetch = require("node-fetch"), origin = process.env.URL || process.env.DEPLOY_PRIME_URL || "https://goldenspike.app";
    const r = await fetch(`${String(origin).replace(/\/+$/, "")}/.netlify/functions/charmNestWorld-background`, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ nonce: got.nonce }) });
    return { statusCode: 200, body: JSON.stringify({ ok: r.status < 400, dispatched: r.status }) };
  } catch (e) {
    console.error("[charmNestWorldCron] failed:", e && e.message);
    return { statusCode: 500, body: JSON.stringify({ ok: false, error: String((e && e.message) || e).slice(0, 160) }) };
  }
};
exports.config = { schedule: "*/2 * * * *" };
