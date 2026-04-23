/*  netlify/functions/etsyMailSync.js
 *
 *  Lightweight sync control endpoint. Two actions:
 *
 *  GET /.netlify/functions/etsyMailSync?action=status
 *      → No auth. Returns current sync state + collection counts.
 *        Use this to poll progress during a background sync.
 *
 *  POST /.netlify/functions/etsyMailSync?action=trigger
 *      body: { mode: "full" | "incremental", daysBack?: 730 }
 *      → Auth via X-EtsyMail-Secret. Invokes etsyMailSync-background and
 *        returns immediately with { invoked: true }. Check status endpoint
 *        to see progress/completion.
 *
 *  The actual work runs in etsyMailSync-background.js (15-min timeout).
 *  Scheduled invocations go directly to the background function via
 *  netlify.toml — they don't go through this trigger.
 */

const fetch = require("node-fetch");
const admin = require("./firebaseAdmin");
const { requireExtensionAuth, CORS } = require("./_etsyMailAuth");

const db = admin.firestore();

function json(statusCode, body) {
  return {
    statusCode,
    headers: { ...CORS, "Content-Type": "application/json" },
    body: JSON.stringify(body)
  };
}

function tsToIso(v) {
  if (!v) return null;
  if (v.toMillis) return new Date(v.toMillis()).toISOString();
  if (v instanceof Date) return v.toISOString();
  return null;
}

async function runStatus() {
  const stateSnap = await db.collection("EtsyMail_Config").doc("syncState").get();
  const state = stateSnap.exists ? stateSnap.data() : null;
  const customersAgg = await db.collection("EtsyMail_Customers").count().get();
  return {
    lastSyncInProgress    : state ? !!state.lastSyncInProgress : false,
    lastSyncCompletedAt   : state ? tsToIso(state.lastSyncCompletedAt) : null,
    lastSyncStartedAt     : state ? tsToIso(state.lastSyncStartedAt)   : null,
    lastSyncMode          : state ? (state.lastSyncMode || null) : null,
    lastSyncReceiptsCount : state ? (state.lastSyncReceiptsCount || 0) : 0,  // receipts SCANNED this invocation
    lastSyncCustomersCount: state ? (state.lastSyncCustomersCount || 0) : 0,
    lastSyncPagesFetched  : state ? (state.lastSyncPagesFetched || 0) : 0,
    lastSyncHitOffsetCap  : state ? !!state.lastSyncHitOffsetCap : false,
    lastSyncDurationMs    : state ? (state.lastSyncDurationMs || null) : null,
    lastSyncProgress      : state ? (state.lastSyncProgress || null) : null,
    lastSyncError         : state ? (state.lastSyncError || null) : null,
    lastSyncErrorAt       : state ? tsToIso(state.lastSyncErrorAt) : null,
    backfillInProgress    : state ? !!state.backfillInProgress : false,
    backfillCompletedWindows: state ? (state.backfillCompletedWindows || 0) : 0,
    backfillReceiptsTotal : state ? (state.backfillReceiptsTotal || 0) : 0,
    customersCount        : customersAgg.data().count || 0
  };
}

// Invoke the background function. We call it via HTTP on the same Netlify
// site — Netlify routes the -background.js function correctly and returns
// 202 immediately. We don't await the actual work.
async function triggerBackgroundSync(event, mode, daysBack) {
  // Derive the site origin from the incoming request
  const host = event.headers["x-forwarded-host"] || event.headers.host;
  const proto = event.headers["x-forwarded-proto"] || "https";
  const siteOrigin = `${proto}://${host}`;
  const url = `${siteOrigin}/.netlify/functions/etsyMailSync-background`;

  const res = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ mode, daysBack })
  });

  // Netlify returns 202 Accepted for background functions. Some edge cases
  // may return other 2xx codes too.
  return { invoked: res.status >= 200 && res.status < 300, invocationStatus: res.status };
}

exports.handler = async (event) => {
  if (event.httpMethod === "OPTIONS") {
    return { statusCode: 204, headers: CORS, body: "" };
  }

  const qs = event.queryStringParameters || {};
  const action = qs.action || "status";

  try {
    if (action === "status") {
      const body = await runStatus();
      return json(200, body);
    }

    if (action === "trigger") {
      const auth = requireExtensionAuth(event);
      if (!auth.ok) return auth.response;

      let mode = "incremental";
      let daysBack = 730;
      if (event.httpMethod === "POST" && event.body) {
        try {
          const body = JSON.parse(event.body);
          if (body.mode) mode = body.mode === "full" ? "full" : "incremental";
          if (body.daysBack) daysBack = Math.max(1, parseInt(body.daysBack, 10));
        } catch {}
      }
      if (qs.mode) mode = qs.mode === "full" ? "full" : "incremental";
      if (qs.daysBack) daysBack = Math.max(1, parseInt(qs.daysBack, 10));

      const result = await triggerBackgroundSync(event, mode, daysBack);
      return json(202, {
        ok: true,
        mode,
        daysBack,
        ...result,
        nextStep: "Poll /.netlify/functions/etsyMailSync?action=status to see progress"
      });
    }

    return json(400, { error: `Unknown action: ${action}. Use action=status or action=trigger.` });
  } catch (err) {
    console.error("etsyMailSync error:", err);
    return json(500, { error: err.message || "Unknown error" });
  }
};
