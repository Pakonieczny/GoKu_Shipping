/*  netlify/functions/etsyMailGmailConfig.js
 *
 *  Owner-gated endpoint for the inbox UI's Gmail Watcher panel. Two ops:
 *
 *    GET  ?op=get
 *      → Returns { enabled, updatedAt, updatedBy, syncState }
 *        Callable with the X-EtsyMail-Secret only — no role check on read.
 *
 *    POST { op:"set", enabled: bool, actor: "<operator name>" }
 *      → Owner-only. Writes EtsyMail_Config/gmailWatcher.enabled and
 *        appends an audit row. The cron picks up the new flag value within
 *        ≤1 minute (its next tick).
 *
 *  Why a dedicated endpoint instead of just using firestoreProxy:
 *    - Centralizes the role check (firestoreProxy enforces it too, but
 *      this gives the UI a clean single-purpose call site).
 *    - Returns a combined view of the toggle state AND the watcher's
 *      sync state in one round trip — the inbox panel needs both.
 *    - Audit row uses a watcher-specific eventType so operators can
 *      filter the audit log for "who toggled the watcher when."
 *
 *  Auth model is identical to other operator-config endpoints:
 *    - X-EtsyMail-Secret required for both ops
 *    - Set additionally requires actor in EtsyMail_Operators with
 *      role="owner". Operators with role="operator" can READ the panel
 *      but the toggle button is disabled for them.
 */

"use strict";

const admin = require("./firebaseAdmin");
const { CORS, requireExtensionAuth } = require("./_etsyMailAuth");
const { requireOwner, logUnauthorized } = require("./_etsyMailRoles");

const db = admin.firestore();
const FV = admin.firestore.FieldValue;

const WATCHER_CFG_DOC  = "EtsyMail_Config/gmailWatcher";
const SYNC_STATE_DOC   = "EtsyMail_Config/gmailSyncState";
const OAUTH_DOC_PATH   = "config/gmailOauth";
const AUDIT_COLL       = "EtsyMail_Audit";

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

async function readWatcherConfig() {
  const snap = await db.doc(WATCHER_CFG_DOC).get();
  if (!snap.exists) {
    return { enabled: false, updatedAt: null, updatedBy: null };
  }
  const d = snap.data();
  return {
    enabled  : !!d.enabled,
    updatedAt: tsToIso(d.updatedAt),
    updatedBy: d.updatedBy || null
  };
}

async function readSyncSnapshot() {
  const [stateSnap, oauthSnap] = await Promise.all([
    db.doc(SYNC_STATE_DOC).get(),
    db.doc(OAUTH_DOC_PATH).get()
  ]);
  const state = stateSnap.exists ? stateSnap.data() : null;
  const oauth = oauthSnap.exists ? oauthSnap.data() : null;
  return {
    oauthSeeded         : !!oauth,
    oauthEmailAddress   : oauth ? (oauth.emailAddress || null) : null,
    lastSyncCompletedAt : state ? tsToIso(state.lastSyncCompletedAt) : null,
    lastSyncInProgress  : state ? !!state.lastSyncInProgress : false,
    lastSyncMode        : state ? (state.lastSyncMode || null) : null,
    lastSyncMessagesScanned: state ? (state.lastSyncMessagesScanned || 0) : 0,
    lastSyncJobsEnqueued: state ? (state.lastSyncJobsEnqueued || 0) : 0,
    lastSyncErrors      : state ? (state.lastSyncErrors || 0) : 0,
    lastSyncError       : state ? (state.lastSyncError || null) : null
  };
}

exports.handler = async (event) => {
  if (event.httpMethod === "OPTIONS") return { statusCode: 204, headers: CORS, body: "" };

  // X-EtsyMail-Secret on every call (read AND write).
  const auth = requireExtensionAuth(event);
  if (!auth.ok) return auth.response;

  const qs = event.queryStringParameters || {};
  const op = (qs.op || "").toLowerCase() ||
             (event.httpMethod === "POST" ? safeBodyOp(event) : null) ||
             "get";

  try {
    if (op === "get") {
      const [cfg, sync] = await Promise.all([readWatcherConfig(), readSyncSnapshot()]);
      return json(200, { ok: true, ...cfg, syncState: sync });
    }

    if (op === "set") {
      if (event.httpMethod !== "POST") return json(405, { error: "POST required for op=set" });

      let body = {};
      try { body = JSON.parse(event.body || "{}"); }
      catch { return json(400, { error: "Invalid JSON body" }); }

      const { enabled, actor } = body;
      if (typeof enabled !== "boolean") {
        return json(400, { error: "Field 'enabled' must be a boolean" });
      }
      if (!actor || typeof actor !== "string") {
        return json(400, { error: "Field 'actor' (operator name) is required" });
      }

      // Owner-only — toggling the watcher affects every operator.
      const owner = await requireOwner(actor);
      if (!owner.ok) {
        await logUnauthorized({
          actor,
          eventType: "gmail_watcher_toggle_unauthorized",
          payload  : { attemptedEnabled: enabled, reason: owner.reason }
        });
        return json(403, { error: "Owner role required", reason: owner.reason });
      }

      // Persist the flag.
      await db.doc(WATCHER_CFG_DOC).set({
        enabled,
        updatedAt: FV.serverTimestamp(),
        updatedBy: actor
      }, { merge: true });

      // Audit.
      await db.collection(AUDIT_COLL).add({
        threadId : null,
        draftId  : null,
        eventType: enabled ? "gmail_watcher_enabled" : "gmail_watcher_disabled",
        actor,
        payload  : { enabled },
        createdAt: FV.serverTimestamp()
      }).catch(()=>{});

      const [cfg, sync] = await Promise.all([readWatcherConfig(), readSyncSnapshot()]);
      return json(200, { ok: true, ...cfg, syncState: sync });
    }

    return json(400, { error: `Unknown op '${op}'. Use op=get or op=set.` });

  } catch (err) {
    console.error("etsyMailGmailConfig error:", err);
    return json(500, { error: err.message || "Unknown error" });
  }
};

// Small helper to peek at body.op without parsing twice on the GET path.
function safeBodyOp(event) {
  if (!event.body) return null;
  try {
    const b = JSON.parse(event.body);
    return b && typeof b.op === "string" ? b.op.toLowerCase() : null;
  } catch { return null; }
}
