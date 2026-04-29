/*  netlify/functions/etsyMailGmailCron.js
 *
 *  Scheduled trigger for the Gmail watcher. Mirrors etsyMailSyncCron.js
 *  exactly — small scheduled function that POSTs to the long-running
 *  background function.
 *
 *  Why two files for one job:
 *    Netlify has two distinct function types with different runtime caps:
 *      - Scheduled functions   → 30 sec ceiling, run on cron
 *      - Background functions  → 15 min ceiling, invoked via HTTP only
 *    A schedule attached to a -background.js file would invoke it under
 *    the 30-sec scheduled budget. So we use a tiny scheduled poker that
 *    completes in <1 sec, which then POSTs to the background fn.
 *
 *  ═══ CADENCE ══════════════════════════════════════════════════════════
 *
 *  Schedule configured in netlify.toml:
 *    [functions."etsyMailGmailCron"]
 *      schedule = "* * * * *"   # every 1 minute
 *
 *  1-min cadence is intentional. Etsy notification emails arrive within
 *  ~30s of the customer hitting Send on Etsy; a 1-min poll keeps the
 *  end-to-end Etsy → inbox latency under ~90 seconds, which is the
 *  product floor for "feels like real-time" auto-reply. The Etsy receipts
 *  sync uses 5-min cadence because it's polling for completed receipts,
 *  not new messages, where minutes-old data is fine.
 *
 *  ═══ THROTTLING ═══════════════════════════════════════════════════════
 *
 *  Reads syncState first. If a sync is already in progress, skip — never
 *  double-trigger (background-fn is idempotent on the watermark, but
 *  parallel runs would compete for the same Gmail quota and waste
 *  invocations). Otherwise runs.
 *
 *  We do NOT impose an "at least N min since last completed" gate like
 *  etsyMailSyncCron does. Gmail polling is cheap (one users.messages.list
 *  call returns nothing if no new mail) and the value of low latency
 *  here is high.
 */

"use strict";

const fetch = require("node-fetch");
const admin = require("./firebaseAdmin");

const db = admin.firestore();

const SYNC_STATE_DOC = "EtsyMail_Config/gmailSyncState";

async function readSyncState() {
  const snap = await db.doc(SYNC_STATE_DOC).get();
  return snap.exists ? snap.data() : null;
}

exports.handler = async (event) => {
  try {
    const state = await readSyncState();

    let shouldRun = true;
    let reason = "";

    if (state && state.lastSyncInProgress) {
      // A previous invocation is still running. Skip — the in-flight one
      // will catch any new messages, and forcing parallel runs causes
      // duplicate Gmail reads + wasted Netlify invocations.
      shouldRun = false;
      reason = "sync already in progress";
    } else {
      reason = "due (1-min cadence)";
    }

    console.log(`etsyMailGmailCron: shouldRun=${shouldRun} (${reason})`);

    if (!shouldRun) {
      return { statusCode: 200, body: JSON.stringify({ skipped: true, reason }) };
    }

    // Build the site origin. Netlify sets URL for production and
    // DEPLOY_URL for deploy previews. Same convention as
    // etsyMailSyncCron.js + etsyMailListingCreatorCron.js.
    const siteOrigin = process.env.URL
                    || process.env.DEPLOY_URL
                    || process.env.NETLIFY_BASE_URL;
    if (!siteOrigin) {
      console.error("Missing URL/DEPLOY_URL env vars — cannot determine site origin");
      return { statusCode: 500, body: "No site origin available" };
    }

    const targetUrl = `${siteOrigin}/.netlify/functions/etsyMailGmail-background`;
    console.log(`etsyMailGmailCron: invoking ${targetUrl}`);

    // Fire-and-forget POST. Netlify returns 202 immediately for background
    // functions, so awaiting the body would just block on a no-op.
    const resp = await fetch(targetUrl, {
      method : "POST",
      headers: { "Content-Type": "application/json" },
      body   : JSON.stringify({ cronTriggered: true })
    });

    console.log(`etsyMailGmailCron: invocation returned ${resp.status}`);

    return {
      statusCode: 200,
      body: JSON.stringify({
        triggered       : true,
        reason,
        invocationStatus: resp.status
      })
    };

  } catch (err) {
    console.error("etsyMailGmailCron error:", err);
    return { statusCode: 500, body: JSON.stringify({ error: err.message }) };
  }
};
