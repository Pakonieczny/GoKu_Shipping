/*  netlify/functions/etsyMailSyncCron.js
 *
 *  Scheduled companion to etsyMailSync-background.
 *
 *  Netlify has two distinct function types:
 *    - Scheduled functions: max 30 seconds runtime, run on cron
 *    - Background functions: max 15 minutes runtime, invoked via HTTP
 *
 *  We can't schedule a background function directly (a schedule on a
 *  -background.js file would invoke it with the 30-sec scheduled runtime,
 *  which isn't enough for any real sync work).
 *
 *  So this tiny scheduled function just POSTs to the background function.
 *  It completes in <1 sec. The background function then runs for up to
 *  15 min with the full sync logic.
 *
 *  Checks syncState first — if backfillInProgress is false AND no
 *  incremental sync is due (lastSyncCompletedAt < 25 min ago), skip this
 *  invocation to avoid wasted work. That keeps Firestore costs down when
 *  there's nothing to do.
 *
 *  Schedule configured in netlify.toml:
 *    [functions."etsyMailSyncCron"]
 *      schedule = "*\u002F5 * * * *"   # every 5 minutes
 *
 *  Running every 5 min gives fast resume after a paused backfill and
 *  reasonably fresh incremental syncs during normal operation.
 */

const fetch = require("node-fetch");
const admin = require("./firebaseAdmin");

const db = admin.firestore();

async function readSyncState() {
  const snap = await db.collection("EtsyMail_Config").doc("syncState").get();
  return snap.exists ? snap.data() : null;
}

exports.handler = async (event) => {
  try {
    const state = await readSyncState();

    // Decide whether to kick off a sync this tick.
    let shouldRun = false;
    let reason = "";

    if (!state) {
      shouldRun = false;
      reason = "no syncState doc yet (waiting for first manual trigger)";
    } else if (state.backfillInProgress) {
      shouldRun = true;
      reason = `backfill in progress (window ${state.backfillCompletedWindows || 0})`;
    } else if (state.lastSyncInProgress) {
      // An invocation is currently running — don't double-trigger
      shouldRun = false;
      reason = "sync already in progress";
    } else {
      // Incremental cadence: every 25 min
      const lastMs = state.lastSyncCompletedAt && state.lastSyncCompletedAt.toMillis
        ? state.lastSyncCompletedAt.toMillis()
        : 0;
      const ageMinutes = (Date.now() - lastMs) / 60000;
      if (lastMs === 0 || ageMinutes >= 25) {
        shouldRun = true;
        reason = `incremental sync due (last completed ${Math.round(ageMinutes)} min ago)`;
      } else {
        shouldRun = false;
        reason = `last sync ${Math.round(ageMinutes)} min ago — not due yet`;
      }
    }

    console.log(`etsyMailSyncCron: shouldRun=${shouldRun} (${reason})`);

    if (!shouldRun) {
      return { statusCode: 200, body: JSON.stringify({ skipped: true, reason }) };
    }

    // Build the site origin. Netlify sets URL for production and DEPLOY_URL
    // for deploy previews. Fall back to constructing from site metadata.
    const siteOrigin = process.env.URL || process.env.DEPLOY_URL;
    if (!siteOrigin) {
      console.error("Missing URL and DEPLOY_URL env vars — cannot determine site origin");
      return { statusCode: 500, body: "No site origin available" };
    }

    const targetUrl = `${siteOrigin}/.netlify/functions/etsyMailSync-background`;
    console.log(`etsyMailSyncCron: invoking ${targetUrl}`);

    // Fire-and-forget POST. Don't await the response body since the
    // background function returns 202 immediately anyway.
    const resp = await fetch(targetUrl, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ cronTriggered: true })
    });

    console.log(`etsyMailSyncCron: invocation returned ${resp.status}`);

    return {
      statusCode: 200,
      body: JSON.stringify({
        triggered: true,
        reason,
        invocationStatus: resp.status
      })
    };

  } catch (err) {
    console.error("etsyMailSyncCron error:", err);
    return { statusCode: 500, body: JSON.stringify({ error: err.message }) };
  }
};
