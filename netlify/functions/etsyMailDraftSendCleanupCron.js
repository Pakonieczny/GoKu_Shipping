/*  netlify/functions/etsyMailDraftSendCleanupCron.js
 *
 *  Scheduled rescue for M5 sends that got stranded. An extension can
 *  crash mid-send — tab closed, laptop lid shut, service worker evicted —
 *  leaving a draft stuck in status="sending" forever. Nobody else can
 *  claim it because claim requires status=queued OR stale-heartbeat.
 *
 *  This cron runs every few minutes, finds drafts where:
 *    - status == "sending"
 *    - sendHeartbeatAt is older than STALE_HEARTBEAT_MS
 *  …and resets them to "queued" (or "failed" if they've already hit the
 *  retry ceiling). Next peek from any extension picks them back up.
 *
 *  Scheduling: configured in netlify.toml:
 *      [functions."etsyMailDraftSendCleanupCron"]
 *      schedule = "every 3 minutes"
 *
 *  Runtime envelope: 30 seconds (Netlify scheduled-function cap). This
 *  query is cheap (indexed where + small limit) so 30s is plenty.
 *
 *  Telemetry: writes a summary entry to EtsyMail_Audit each run so the
 *  operator UI can surface "N stranded sends recovered today".
 */

const admin = require("./firebaseAdmin");

const db = admin.firestore();
const FV = admin.firestore.FieldValue;

const DRAFTS_COLL = "EtsyMail_Drafts";
const AUDIT_COLL  = "EtsyMail_Audit";

const STALE_HEARTBEAT_MS = 60 * 1000;   // must match etsyMailDraftSend.js
const MAX_SEND_ATTEMPTS  = 3;
const SCAN_LIMIT         = 50;          // one pass; if more, next tick catches them

exports.handler = async () => {
  const started = Date.now();
  const cutoffMs = started - STALE_HEARTBEAT_MS;
  const cutoffTs = admin.firestore.Timestamp.fromMillis(cutoffMs);

  let scanned = 0, requeued = 0, failed = 0, skipped = 0;
  const actions = [];

  try {
    // Query: all drafts currently "sending". We then filter by heartbeat
    // client-side because Firestore won't combine inequality filters on
    // different fields without a composite index (and the cost of
    // scanning all "sending" drafts is tiny — there are rarely >50).
    const snap = await db.collection(DRAFTS_COLL)
      .where("status", "==", "sending")
      .limit(SCAN_LIMIT)
      .get();

    scanned = snap.size;
    if (!scanned) {
      return {
        statusCode: 200,
        body: JSON.stringify({ ok: true, scanned: 0, elapsedMs: Date.now() - started })
      };
    }

    for (const doc of snap.docs) {
      const data = doc.data();
      const hbTs = data.sendHeartbeatAt;
      const hbMs = hbTs && hbTs.toMillis ? hbTs.toMillis() : 0;
      if (hbMs >= cutoffMs) {
        // Still alive — extension heartbeating normally. Leave it.
        skipped++;
        continue;
      }

      // Transaction per doc: race-safe against a late heartbeat arriving
      // at the same moment (unlikely but possible).
      try {
        await db.runTransaction(async (tx) => {
          const fresh = await tx.get(doc.ref);
          if (!fresh.exists) return;
          const d = fresh.data();
          if (d.status !== "sending") return;
          const hb = d.sendHeartbeatAt;
          const hbms = hb && hb.toMillis ? hb.toMillis() : 0;
          if (hbms >= cutoffMs) return;  // heartbeat landed between queries

          // v0.9.1 #2/#3: stranded with sendStage=post_click is the
          // dangerous case. The Send button was clicked; we don't know
          // if the message went out. Re-clicking would risk a duplicate.
          // Mark failed for manual operator verification — never requeue.
          if (d.sendStage === "post_click") {
            tx.set(doc.ref, {
              status        : "failed",
              sendError     : `Stranded after Send was clicked (${Math.round((Date.now() - hbms) / 1000)}s ago). Verify on Etsy whether the message went through before re-sending.`,
              sendErrorCode : "STRANDED_POST_CLICK",
              updatedAt     : FV.serverTimestamp()
            }, { merge: true });
            actions.push({ draftId: doc.id, action: "failed_post_click", attempts: d.sendAttempts || 0 });
            failed++;
            return;
          }

          const attempts = d.sendAttempts || 0;
          if (attempts >= MAX_SEND_ATTEMPTS) {
            tx.set(doc.ref, {
              status        : "failed",
              sendError     : `Stranded (no heartbeat for ${Math.round((Date.now() - hbms) / 1000)}s) — retry budget exhausted`,
              sendErrorCode : "STRANDED_EXHAUSTED",
              updatedAt     : FV.serverTimestamp()
            }, { merge: true });
            actions.push({ draftId: doc.id, action: "failed", attempts });
            failed++;
          } else {
            tx.set(doc.ref, {
              status        : "queued",
              sendSessionId : null,
              sendClaimedAt : null,
              sendError     : `Previous attempt stranded (no heartbeat for ${Math.round((Date.now() - hbms) / 1000)}s) — requeued for retry`,
              sendErrorCode : "STRANDED_REQUEUED",
              updatedAt     : FV.serverTimestamp()
            }, { merge: true });
            actions.push({ draftId: doc.id, action: "requeued", attempts });
            requeued++;
          }
        });
      } catch (e) {
        console.warn(`cleanup txn error for ${doc.id}:`, e.message);
      }
    }

    // Audit summary (only if we did something)
    if (requeued || failed) {
      try {
        await db.collection(AUDIT_COLL).add({
          threadId  : null,
          draftId   : null,
          eventType : "send_cleanup_cron",
          actor     : "cron",
          payload   : {
            scanned, requeued, failed, skipped,
            actions, elapsedMs: Date.now() - started
          },
          createdAt : FV.serverTimestamp()
        });
      } catch (e) {
        console.warn("audit write failed:", e.message);
      }
    }

    return {
      statusCode: 200,
      body: JSON.stringify({
        ok: true, scanned, requeued, failed, skipped,
        elapsedMs: Date.now() - started
      })
    };

  } catch (err) {
    console.error("etsyMailDraftSendCleanupCron error:", err);
    return {
      statusCode: 500,
      body: JSON.stringify({ error: err.message, scanned, requeued, failed, skipped })
    };
  }
};
