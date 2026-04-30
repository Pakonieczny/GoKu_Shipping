/*  netlify/functions/etsyMailPurgeInbox.js
 *
 *  Owner-gated DESTRUCTIVE endpoint that wipes the inbox to a clean
 *  slate. Used after testing/migration when the inbox accumulated junk
 *  threads and the operator wants to start fresh.
 *
 *  WIPES:
 *    - EtsyMail_Threads     — every thread doc (conversation list)
 *    - EtsyMail_Jobs        — every job (queued/claimed/completed/failed)
 *    - EtsyMail_Audit       — every audit row (optional, default true)
 *
 *  RESETS:
 *    - EtsyMail_Config/gmailSyncState.lastInternalDateMs → Date.now()
 *      (so the watcher doesn't re-process any history)
 *
 *  PRESERVES:
 *    - config/gmailOauth                       (Gmail OAuth tokens)
 *    - config/etsyOauth                        (Etsy OAuth tokens)
 *    - EtsyMail_Config/* other than gmailSyncState (watcher toggle,
 *      autoPipeline, etc.)
 *    - EtsyMail_Operators                      (operator/owner roles)
 *    - EtsyMail_Listings, EtsyMail_Customers etc. — anything not
 *      listed above
 *
 *  Hard requirement: caller must include `confirm: "WIPE"` in the body.
 *  This is a footgun guard — owner role + confirm token both required.
 *
 *  Usage:
 *    POST /.netlify/functions/etsyMailPurgeInbox
 *    Body: { actor: "Paul K", confirm: "WIPE", purgeAudit: true }
 *
 *  Returns: { ok, threadsDeleted, jobsDeleted, auditDeleted, watermarkResetTo }
 */

"use strict";

const admin = require("./firebaseAdmin");
const { CORS, requireExtensionAuth } = require("./_etsyMailAuth");
const { requireOwner } = require("./_etsyMailRoles");

const db = admin.firestore();
const FV = admin.firestore.FieldValue;

const BATCH_SIZE = 400;   // Firestore batch limit is 500; leave headroom

function json(statusCode, body) {
  return {
    statusCode,
    headers: { ...CORS, "Content-Type": "application/json" },
    body: JSON.stringify(body)
  };
}

async function deleteCollection(collName) {
  let total = 0;
  // Loop in batches so a 1000-doc collection doesn't fail.
  while (true) {
    const snap = await db.collection(collName).limit(BATCH_SIZE).get();
    if (snap.empty) break;

    const batch = db.batch();
    snap.docs.forEach(doc => batch.delete(doc.ref));
    await batch.commit();
    total += snap.size;

    // If we got fewer than the batch size, that was the last page.
    if (snap.size < BATCH_SIZE) break;
  }
  return total;
}

exports.handler = async (event) => {
  if (event.httpMethod === "OPTIONS") return { statusCode: 204, headers: CORS, body: "" };
  if (event.httpMethod !== "POST") return json(405, { error: "POST required" });

  const auth = requireExtensionAuth(event);
  if (!auth.ok) return auth.response;

  let body = {};
  try { body = JSON.parse(event.body || "{}"); }
  catch { return json(400, { error: "Invalid JSON body" }); }

  const { actor, confirm, purgeAudit = true } = body;
  if (!actor) return json(400, { error: "actor required" });
  if (confirm !== "WIPE") {
    return json(400, {
      error  : "confirm must equal the literal string 'WIPE' to acknowledge data loss",
      example: { actor, confirm: "WIPE", purgeAudit: true }
    });
  }

  const owner = await requireOwner(actor);
  if (!owner.ok) return json(403, { error: "Owner role required", reason: owner.reason });

  const result = {
    threadsDeleted   : 0,
    jobsDeleted      : 0,
    auditDeleted     : 0,
    watermarkResetTo : null
  };

  try {
    // ── Threads ─────────────────────────────────────────────────────
    result.threadsDeleted = await deleteCollection("EtsyMail_Threads");

    // ── Jobs ────────────────────────────────────────────────────────
    result.jobsDeleted = await deleteCollection("EtsyMail_Jobs");

    // ── Audit (optional but defaults on) ────────────────────────────
    if (purgeAudit) {
      result.auditDeleted = await deleteCollection("EtsyMail_Audit");
    }

    // ── Reset Gmail watermark so future watcher runs don't re-scan ──
    const nowMs = Date.now();
    await db.doc("EtsyMail_Config/gmailSyncState").set({
      lastInternalDateMs: nowMs,
      lastSyncCompletedAt: FV.serverTimestamp(),
      lastSyncMessagesScanned: 0,
      lastSyncJobsEnqueued: 0,
      lastSyncErrors: 0,
      lastSyncError: null,
      lastSyncMode: "purge_reset",
      lastSyncInProgress: false,
      resetBy: actor,
      resetAt: FV.serverTimestamp()
    }, { merge: true });
    result.watermarkResetTo = new Date(nowMs).toISOString();

    // ── Audit the purge itself ──────────────────────────────────────
    // (in a freshly-created audit collection if we just wiped the old)
    await db.collection("EtsyMail_Audit").add({
      threadId : null,
      eventType: "inbox_purged",
      actor,
      payload  : result,
      createdAt: FV.serverTimestamp()
    }).catch(() => {});

    return json(200, { ok: true, ...result });
  } catch (err) {
    console.error("etsyMailPurgeInbox error:", err);
    return json(500, { error: err.message, partial: result });
  }
};
