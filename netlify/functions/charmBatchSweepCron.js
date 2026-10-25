/*  netlify/functions/charmBatchSweepCron.js
 *
 *  Scheduled trigger for the Listing Generator batch sweep. Fires the
 *  `batch_sweep` kind on geminiImageProxy-background, which (with a 15-minute
 *  background budget):
 *
 *    1. Refreshes the Gemini state of every un-collected batch record in
 *       Firestore (ListingGenerator1Batches),
 *    2. AUTO-COLLECTS every batch that has reached JOB_STATE_SUCCEEDED —
 *       downloading the result images into Firebase Storage and writing
 *       manifests — with NO browser tab required,
 *    3. Resumes any charm_batch_orchestrate run whose self-chain was
 *       dropped (no progress update in 20+ minutes).
 *
 *  This makes the entire Charm Maker batch pipeline independent of the
 *  operator's browser: submit, walk away, results land on their own.
 *
 *  The sweep itself holds an overlap guard in Firestore
 *  (LG1_Config/batchSweep.runningSince), so a tick that fires while a
 *  previous sweep is still running is a cheap no-op.
 *
 *  Schedule lives in netlify.toml (same pattern as etsyMailGmailCron) —
 *  every 10 minutes. (Cron string spelled out to avoid closing this
 *  comment: asterisk-slash-10, then four asterisks, space-separated.)
 *    [functions."charmBatchSweepCron"]
 *      schedule = "<asterisk>/10 * * * *"
 */

"use strict";

const fetch = require("node-fetch");

exports.handler = async () => {
  const siteOrigin = process.env.URL
                  || process.env.DEPLOY_URL
                  || process.env.NETLIFY_BASE_URL;
  if (!siteOrigin) {
    console.error("charmBatchSweepCron: missing URL/DEPLOY_URL env vars — cannot determine site origin");
    return { statusCode: 500, body: "no site origin" };
  }

  const targetUrl = `${String(siteOrigin).replace(/\/+$/, "")}/.netlify/functions/geminiImageProxy-background`;

  // Background functions accept with a 202 immediately; the sweep then
  // runs with the 15-minute background budget. Fire-and-forget. A refused or
  // failed dispatch is tried once more, and reported as a failure (it used to
  // return 200 whatever happened, so a lost run left no trace for 10 minutes).
  let lastError = null;
  for (let attempt = 0; attempt < 2; attempt++) {
    try {
      const resp = await fetch(targetUrl, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ kind: "batch_sweep", cronTriggered: true }),
      });
      console.log(`charmBatchSweepCron: invocation returned ${resp.status}`);
      if (resp.status < 400) return { statusCode: 200, body: JSON.stringify({ ok: true, dispatched: resp.status }) };
      lastError = `HTTP ${resp.status}`;
    } catch (err) {
      lastError = String(err && err.message);
      console.error("charmBatchSweepCron: dispatch failed:", lastError);
    }
    if (attempt === 0) await new Promise((resolve) => setTimeout(resolve, 3000));
  }
  return { statusCode: 500, body: JSON.stringify({ ok: false, error: lastError }) };
};
