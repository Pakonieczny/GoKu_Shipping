/*  netlify/functions/etsyMailAutoPipelineReaper.js
 *
 *  v1.3 — Stale-claim reaper.
 *
 *  ═══ PURPOSE ════════════════════════════════════════════════════════
 *
 *  The auto-pipeline (etsyMailAutoPipeline-background.js) atomically
 *  claims a thread by setting:
 *      lastAutoDecision   = "in_progress"
 *      lastAutoDecisionAt = <serverTimestamp>
 *
 *  When the pipeline finishes successfully (auto_send or human_review),
 *  it overwrites those fields. When it crashes mid-run — Lambda timeout,
 *  Anthropic API hang, network blip, OOM — the in_progress marker is
 *  left orphaned and the thread shows as "AI thinking..." indefinitely
 *  in the operator UI.
 *
 *  This reaper runs on a 10-minute cron, finds threads with stale
 *  in_progress markers (older than 5 minutes), and clears them. The
 *  thread is left at pending_human_review (where the claim transaction
 *  put it) so it's already visible in the operator's Needs Review
 *  folder. We DO NOT auto-retry the pipeline — recovery is manual via
 *  the AI Draft button. If a real new inbound arrives after the reaper
 *  clears the marker, the snapshot trigger will fire the pipeline
 *  fresh.
 *
 *  ═══ INVOCATION ════════════════════════════════════════════════════
 *
 *  Netlify scheduled function. Two ways to enable the schedule:
 *
 *  (a) In-code config (this file's `exports.config`). Works on Netlify
 *      runtime v2+ without any extra config or dependency.
 *
 *  (b) netlify.toml (preferred for visibility):
 *
 *        [[scheduled.functions]]
 *          name = "etsyMailAutoPipelineReaper"
 *          cron = "*​/10 * * * *"
 *
 *  Schedule: every 10 minutes. Reaper itself is fast (< 1 sec for
 *  typical inboxes — single Firestore query, batched updates).
 *
 *  Manual invocation is also supported:
 *      POST /.netlify/functions/etsyMailAutoPipelineReaper
 *      (X-EtsyMail-Secret required)
 *
 *  Useful for testing or one-shot recovery after a known incident.
 *
 *  ═══ AUDIT ══════════════════════════════════════════════════════════
 *
 *  Each reaped thread gets an audit entry:
 *      eventType: "auto_pipeline_stale_claim_recovered"
 *      payload:   { staleForMs, previousDecision, threadStatus }
 *
 *  Operators can grep the audit log to find recurring patterns
 *  (specific customer triggering crashes, time-of-day correlations,
 *  etc.).
 */

const admin = require("./firebaseAdmin");
const { CORS, requireExtensionAuth } = require("./_etsyMailAuth");

const db = admin.firestore();
const FV = admin.firestore.FieldValue;

const THREADS_COLL = "EtsyMail_Threads";
const AUDIT_COLL   = "EtsyMail_Audit";

// A claim is "stale" once this much time has passed without a finalize.
// 5 minutes is generous — the auto-pipeline typically completes in
// 10-60 seconds; >2 minutes of in_progress almost always means death.
// We picked 5 to err on the side of NOT clobbering a slow-but-still-
// running pipeline.
const STALE_CLAIM_THRESHOLD_MS = 5 * 60 * 1000;

// Hard cap on how many threads to reap per run. If something is
// catastrophically wrong (e.g., Anthropic outage causing every
// pipeline to hang), we don't want a single reaper invocation to
// chew through 10,000 threads. 200 is enough for any realistic
// queue depth at our cadence.
const MAX_REAP_PER_RUN = 200;

// In-code schedule config. Netlify runtime reads this on deploy and
// registers the cron. (Comment in the file header explains the
// alternate netlify.toml path.)
exports.config = { schedule: "*/10 * * * *" };

function json(statusCode, body) {
  return {
    statusCode,
    headers: { ...CORS, "Content-Type": "application/json" },
    body: JSON.stringify(body)
  };
}

async function writeAudit(threadId, payload) {
  await db.collection(AUDIT_COLL).add({
    threadId,
    draftId  : null,
    eventType: "auto_pipeline_stale_claim_recovered",
    actor    : "system:reaper",
    payload,
    createdAt: FV.serverTimestamp()
  });
}

/** Reap one thread inside a transaction so we don't clobber a pipeline
 *  that finished between our query and our update.
 *
 *  Returns:
 *    { reaped: true, ageMs }      — claim was stale, we cleared it
 *    { reaped: false, reason }    — pipeline finished or claim wasn't stale anymore
 */
async function reapOne(threadRef) {
  return await db.runTransaction(async (tx) => {
    const snap = await tx.get(threadRef);
    if (!snap.exists) return { reaped: false, reason: "thread_gone" };
    const data = snap.data() || {};

    if (data.lastAutoDecision !== "in_progress") {
      // Pipeline finished between query and txn — nothing to do.
      return { reaped: false, reason: "no_longer_in_progress", currentDecision: data.lastAutoDecision };
    }

    const claimedAtMs = data.lastAutoDecisionAt && data.lastAutoDecisionAt.toMillis
      ? data.lastAutoDecisionAt.toMillis() : 0;
    const ageMs = Date.now() - claimedAtMs;
    if (ageMs < STALE_CLAIM_THRESHOLD_MS) {
      // Claim isn't actually stale yet — the query window slid past
      // the threshold but by the time we got the txn lock, the claim
      // is younger than the threshold. Leave it alone.
      return { reaped: false, reason: "not_yet_stale", ageMs };
    }

    // Reap. Clear the in_progress marker. Reset
    // lastAutoProcessedInboundAt so a future snapshot trigger CAN
    // re-run the pipeline naturally — without this, the idempotency
    // lock would keep the thread permanently un-processable.
    //
    // Thread status: leave it at whatever the claim set it to
    // (typically pending_human_review). The thread is already in the
    // operator's Needs Review folder — they can take action.
    tx.update(threadRef, {
      lastAutoDecision           : "stale_claim_recovered",
      lastAutoDecisionAt         : FV.serverTimestamp(),
      lastAutoProcessedInboundAt : null,
      aiDraftStatus              : data.aiDraftStatus === "ready" ? "ready" : "none",
      updatedAt                  : FV.serverTimestamp()
    });

    return {
      reaped: true,
      ageMs,
      previousStatus: data.status || null,
      hadDraft      : !!data.latestDraftId
    };
  });
}

exports.handler = async (event) => {
  // Manual invocation path
  if (event.httpMethod === "OPTIONS") {
    return { statusCode: 200, headers: CORS, body: "ok" };
  }
  // Netlify scheduled functions invoke with a special body shape; we
  // don't need to inspect it. For manual POST/GET invocations, require
  // the secret. Scheduled invocations skip the auth check (Netlify's
  // scheduler doesn't send the header, and is itself the authority).
  const isScheduledInvocation = !!(event.body && event.body.includes("scheduled-event"))
    || (event.headers && event.headers["x-netlify-event"] === "schedule");

  if (!isScheduledInvocation && event.httpMethod) {
    const auth = requireExtensionAuth(event);
    if (!auth.ok) return auth.response;
  }

  const tStart = Date.now();
  const cutoffMs = Date.now() - STALE_CLAIM_THRESHOLD_MS;

  try {
    // Single-field query — uses the existing single-field index, no
    // composite index required. We filter the lastAutoDecisionAt
    // condition in memory after the query. For typical inboxes the
    // result set of in_progress threads is small (usually 0-5), so
    // pulling them all is cheap.
    const snap = await db.collection(THREADS_COLL)
      .where("lastAutoDecision", "==", "in_progress")
      .limit(MAX_REAP_PER_RUN * 2)   // pull extra in case some are not-yet-stale
      .get();

    let candidates = [];
    snap.forEach(doc => {
      const data = doc.data() || {};
      const claimedAtMs = data.lastAutoDecisionAt && data.lastAutoDecisionAt.toMillis
        ? data.lastAutoDecisionAt.toMillis() : 0;
      if (claimedAtMs <= cutoffMs) {
        candidates.push({ id: doc.id, ref: doc.ref, ageMs: Date.now() - claimedAtMs });
      }
    });

    // Cap reaps per run
    if (candidates.length > MAX_REAP_PER_RUN) {
      candidates = candidates.slice(0, MAX_REAP_PER_RUN);
    }

    let reapedCount = 0;
    let skippedCount = 0;
    for (const c of candidates) {
      try {
        const result = await reapOne(c.ref);
        if (result.reaped) {
          reapedCount++;
          await writeAudit(c.id, {
            staleForMs       : c.ageMs,
            previousStatus   : result.previousStatus,
            hadDraft         : result.hadDraft,
            staleThresholdMs : STALE_CLAIM_THRESHOLD_MS
          });
        } else {
          skippedCount++;
        }
      } catch (e) {
        console.warn("reapOne failed for", c.id, e.message);
        skippedCount++;
      }
    }

    const summary = {
      success      : true,
      scanned      : snap.size,
      candidates   : candidates.length,
      reaped       : reapedCount,
      skipped      : skippedCount,
      durationMs   : Date.now() - tStart,
      ranAt        : new Date().toISOString(),
      thresholdMs  : STALE_CLAIM_THRESHOLD_MS
    };

    if (reapedCount > 0) {
      console.log("autoPipelineReaper:", JSON.stringify(summary));
    }

    return json(200, summary);
  } catch (err) {
    console.error("autoPipelineReaper error:", err);
    return json(500, { error: err.message || String(err) });
  }
};
