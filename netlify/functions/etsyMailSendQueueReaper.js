/*  netlify/functions/etsyMailSendQueueReaper.js
 *
 *  v1.4 — Send-queue reaper.
 *
 *  ═══ PURPOSE ════════════════════════════════════════════════════════
 *
 *  Drafts that have been enqueued for send (status="queued") or claimed
 *  by an extension (status="sending") can get stranded if:
 *
 *    - The operator's browser is closed (no extension to peek the queue)
 *    - The extension claimed a draft but the tab died before clicking
 *      Send (sendStage="pre_click", stale heartbeat)
 *    - The extension clicked Send but the tab died before a confirmation
 *      signal (sendStage="post_click", stale heartbeat) — the message
 *      may or may not have reached Etsy
 *
 *  The existing peek/claim paths in etsyMailDraftSend.js already handle
 *  these on demand — but only when the extension actually peeks/claims.
 *  If the extension is offline for hours/days, the queue grows
 *  unbounded and the parent threads sit at queued_for_auto_send (Auto-
 *  Reply folder, animated "sending…" pill) indefinitely.
 *
 *  This reaper is the safety net: runs even when no extension is around.
 *  Sweeps drafts past the staleness threshold, marks them failed, and
 *  demotes any queued_for_auto_send parent thread to pending_human_
 *  review so operators see them.
 *
 *  ═══ STALENESS THRESHOLDS ══════════════════════════════════════════
 *
 *  status=queued  + queuedAt > MAX_CLAIM_LOOKBACK_MIN (30 min)
 *      → mark failed (QUEUED_EXPIRED), demote thread.
 *
 *  status=sending + sendStage=pre_click + sendHeartbeatAt > 60s old
 *      → mark failed (CLAIM_ABANDONED), demote thread.
 *      (Extension claimed but never clicked Send. Re-claiming is safe
 *       since the message hasn't reached Etsy.)
 *
 *  status=sending + sendStage=post_click + sendHeartbeatAt > 60s old
 *      → mark failed (STRANDED_POST_CLICK), demote thread.
 *      (Extension clicked Send but never confirmed. Re-sending would
 *       risk a duplicate. Operator MUST verify on Etsy.)
 *
 *  ═══ INVOCATION ════════════════════════════════════════════════════
 *
 *  Scheduled every 5 minutes — more aggressive than the pipeline-claim
 *  reaper because stranded sends are customer-facing (a thread stuck
 *  on "sending…" delays operator visibility).
 *
 *  Manual POST also supported (with auth) for one-shot recovery.
 */

const admin = require("./firebaseAdmin");
const { CORS, requireExtensionAuth } = require("./_etsyMailAuth");
const {
  demoteThreadInTxn,
  isStaleQueued,
  isStaleHeartbeat,
  MAX_CLAIM_LOOKBACK_MIN
} = require("./etsyMailDraftSend");

const db = admin.firestore();
const FV = admin.firestore.FieldValue;

const DRAFTS_COLL = "EtsyMail_Drafts";
const AUDIT_COLL  = "EtsyMail_Audit";

// Cap reaps per run. Same defensive ceiling as the pipeline reaper —
// during a major incident we don't want one cron tick chewing through
// thousands of docs.
const MAX_REAP_PER_RUN = 200;

exports.config = { schedule: "*/5 * * * *" };

function json(statusCode, body) {
  return {
    statusCode,
    headers: { ...CORS, "Content-Type": "application/json" },
    body: JSON.stringify(body)
  };
}

async function audit(threadId, draftId, eventType, payload) {
  await db.collection(AUDIT_COLL).add({
    threadId, draftId, eventType,
    actor    : "system:sendQueueReaper",
    payload,
    createdAt: FV.serverTimestamp()
  });
}

/** Reap one stale draft inside a transaction. Idempotent — second run
 *  on an already-reaped draft is a no-op. */
async function reapDraft(draftRef, kind) {
  return await db.runTransaction(async (tx) => {
    const snap = await tx.get(draftRef);
    if (!snap.exists) return { reaped: false, reason: "draft_gone" };
    const d = snap.data();

    // Re-check staleness inside the txn — extension might have
    // peek'd and processed the draft between our query and the txn.
    if (d.status === "queued") {
      if (!isStaleQueued(d.queuedAt)) {
        return { reaped: false, reason: "queued_not_yet_stale" };
      }
    } else if (d.status === "sending") {
      if (!isStaleHeartbeat(d.sendHeartbeatAt)) {
        return { reaped: false, reason: "sending_heartbeat_fresh" };
      }
    } else {
      // Already terminal (failed/sent/etc.) — nothing to reap.
      return { reaped: false, reason: "already_terminal", currentStatus: d.status };
    }

    // Pick failure code + decision reason based on the draft's stage
    let sendErrorCode, sendError, decisionReason;
    if (d.status === "queued") {
      sendErrorCode  = "QUEUED_EXPIRED";
      sendError      = `Expired by reaper — queued more than ${MAX_CLAIM_LOOKBACK_MIN} minutes (extension may be offline)`;
      decisionReason = "human_review_after_queued_expired";
    } else if (d.sendStage === "post_click") {
      sendErrorCode  = "STRANDED_POST_CLICK";
      sendError      = "Send was clicked but never confirmed. Verify on Etsy whether the message went through before re-sending.";
      decisionReason = "human_review_after_stranded_post_click";
    } else {
      sendErrorCode  = "CLAIM_ABANDONED";
      sendError      = "Extension claimed the draft but never clicked Send (heartbeat stale). Safe to re-send.";
      decisionReason = "human_review_after_claim_abandoned";
    }

    tx.set(draftRef, {
      status          : "failed",
      sendError,
      sendErrorCode,
      sendHeartbeatAt : FV.serverTimestamp(),
      updatedAt       : FV.serverTimestamp()
    }, { merge: true });

    // Demote parent thread (no-op if it's no longer queued_for_auto_send)
    const threadStatusUpdate = await demoteThreadInTxn(tx, d.threadId, decisionReason);

    return {
      reaped: true,
      threadId: d.threadId,
      sendErrorCode,
      threadStatusUpdate,
      ageMs: kind === "queued"
        ? (d.queuedAt ? Date.now() - d.queuedAt.toMillis() : null)
        : (d.sendHeartbeatAt ? Date.now() - d.sendHeartbeatAt.toMillis() : null)
    };
  });
}

exports.handler = async (event) => {
  if (event.httpMethod === "OPTIONS") {
    return { statusCode: 200, headers: CORS, body: "ok" };
  }

  const isScheduledInvocation =
    !!(event.body && event.body.includes("scheduled-event")) ||
    (event.headers && event.headers["x-netlify-event"] === "schedule");

  if (!isScheduledInvocation && event.httpMethod) {
    const auth = requireExtensionAuth(event);
    if (!auth.ok) return auth.response;
  }

  const tStart = Date.now();
  let totalReaped = 0;
  let totalScanned = 0;
  let totalSkipped = 0;
  const failures = [];

  try {
    // ── Pass 1: stale `queued` drafts ────────────────────────────
    // Drafts with status=queued whose queuedAt is past the threshold.
    // Single-field index on `status` (already exists for the peek
    // path); we filter queuedAt in memory.
    const queuedSnap = await db.collection(DRAFTS_COLL)
      .where("status", "==", "queued")
      .limit(MAX_REAP_PER_RUN * 2)
      .get();
    totalScanned += queuedSnap.size;

    for (const doc of queuedSnap.docs) {
      if (totalReaped >= MAX_REAP_PER_RUN) break;
      const d = doc.data();
      if (!isStaleQueued(d.queuedAt)) { totalSkipped++; continue; }
      try {
        const r = await reapDraft(doc.ref, "queued");
        if (r.reaped) {
          totalReaped++;
          await audit(r.threadId, doc.id, "draft_queue_expired_by_reaper", {
            sendErrorCode: r.sendErrorCode,
            ageMs        : r.ageMs,
            threadStatusUpdate: r.threadStatusUpdate
          });
        } else {
          totalSkipped++;
        }
      } catch (e) {
        failures.push({ draftId: doc.id, error: e.message });
      }
    }

    // ── Pass 2: stale `sending` drafts ───────────────────────────
    if (totalReaped < MAX_REAP_PER_RUN) {
      const sendingSnap = await db.collection(DRAFTS_COLL)
        .where("status", "==", "sending")
        .limit(MAX_REAP_PER_RUN * 2)
        .get();
      totalScanned += sendingSnap.size;

      for (const doc of sendingSnap.docs) {
        if (totalReaped >= MAX_REAP_PER_RUN) break;
        const d = doc.data();
        if (!isStaleHeartbeat(d.sendHeartbeatAt)) { totalSkipped++; continue; }
        try {
          const r = await reapDraft(doc.ref, "sending");
          if (r.reaped) {
            totalReaped++;
            await audit(r.threadId, doc.id, "draft_send_reaped", {
              sendErrorCode: r.sendErrorCode,
              sendStage    : d.sendStage,
              ageMs        : r.ageMs,
              threadStatusUpdate: r.threadStatusUpdate
            });
          } else {
            totalSkipped++;
          }
        } catch (e) {
          failures.push({ draftId: doc.id, error: e.message });
        }
      }
    }

    const summary = {
      success    : true,
      reaped     : totalReaped,
      scanned    : totalScanned,
      skipped    : totalSkipped,
      failures   : failures.length,
      failureLog : failures.slice(0, 10),  // cap response size
      durationMs : Date.now() - tStart,
      ranAt      : new Date().toISOString()
    };

    if (totalReaped > 0 || failures.length > 0) {
      console.log("sendQueueReaper:", JSON.stringify(summary));
    }

    return json(200, summary);
  } catch (err) {
    console.error("sendQueueReaper error:", err);
    return json(500, { error: err.message || String(err) });
  }
};
