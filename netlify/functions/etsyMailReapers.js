/*  netlify/functions/etsyMailReapers.js
 *
 *  v2.4 — Consolidated reaper. Replaces three separate cron functions
 *  with a single endpoint that runs all three reaper passes on each
 *  invocation:
 *
 *    1. AUTO_PIPELINE_CLAIMS — clear stale `in_progress` markers from
 *       crashed pipeline runs (was: etsyMailAutoPipelineReaper).
 *    2. SEND_QUEUE          — fail/abandon stranded queued + sending
 *       drafts (was: etsyMailSendQueueReaper).
 *    3. SALES_FUNNELS       — mark abandoned sales conversations
 *       (was: etsyMailSalesReaper).
 *
 *  ═══ WHY ONE FILE ════════════════════════════════════════════════════
 *
 *  Previously three separate scheduled functions, each on a different
 *  cron cadence (5 min / 5 min / 6 h). Consolidating reduces deploy
 *  surface and audit noise. Each pass is independently bounded
 *  (MAX_REAP_PER_RUN_*) and short-circuits when there's nothing to do,
 *  so running all three on the most aggressive cadence (5 min) costs
 *  ~one indexed Firestore query per reaper-with-zero-work — negligible.
 *
 *  Sales-funnel scan would otherwise run 72× more often (every 5 min vs
 *  every 6 h). To keep query volume sane, the sales-funnel pass uses an
 *  internal time-gate (lastSalesScanAt in EtsyMail_Config/reaperState)
 *  so it ACTUALLY runs only once per SALES_SCAN_INTERVAL_MS. The other
 *  two reapers run on every invocation as before.
 *
 *  ═══ INVOCATION ════════════════════════════════════════════════════
 *
 *  Scheduled cron:        netlify.toml schedule (every 5 minutes)
 *  Manual full sweep:     POST /.netlify/functions/etsyMailReapers
 *  Manual single pass:    POST { op: "auto_pipeline" | "send_queue" | "sales_funnels" }
 *  Force sales pass now:  POST { op: "sales_funnels", force: true }
 *
 *  Manual invocations require X-EtsyMail-Secret. Scheduled invocations
 *  bypass auth (Netlify scheduler is the authority).
 */

const admin = require("./firebaseAdmin");
const { CORS, requireExtensionAuth, isScheduledInvocation } = require("./_etsyMailAuth");
const {
  demoteThreadInTxn,
  isStaleQueued,
  isStaleHeartbeat,
  MAX_CLAIM_LOOKBACK_MIN
} = require("./etsyMailDraftSend");

const db = admin.firestore();
const FV = admin.firestore.FieldValue;

// ─── Collections ────────────────────────────────────────────────────────
const THREADS_COLL = "EtsyMail_Threads";
const DRAFTS_COLL  = "EtsyMail_Drafts";
const SALES_COLL   = "EtsyMail_SalesContext";
const AUDIT_COLL   = "EtsyMail_Audit";
const CONFIG_COLL  = "EtsyMail_Config";

// ─── Auto-pipeline reaper config ───────────────────────────────────────
// A claim is "stale" once this much time has passed without a finalize.
// 5 minutes is generous — the auto-pipeline typically completes in
// 10-60 seconds; >2 minutes of in_progress almost always means death.
const STALE_CLAIM_THRESHOLD_MS  = 5 * 60 * 1000;
const MAX_REAP_PER_RUN_PIPELINE = 200;

// ─── Send-queue reaper config ──────────────────────────────────────────
const MAX_REAP_PER_RUN_SEND     = 200;

// ─── Sales-funnel reaper config ────────────────────────────────────────
const ABANDON_AFTER_DAYS  = parseInt(process.env.ETSYMAIL_SALES_ABANDON_AFTER_DAYS || "7", 10);
const MAX_THREADS_PER_RUN = parseInt(process.env.ETSYMAIL_SALES_REAPER_MAX_THREADS || "200", 10);
// Run the sales-funnel scan at most every 6 hours. Stored in
// EtsyMail_Config/reaperState.lastSalesScanAt (millis). The other two
// reapers run on every invocation; only sales is gated, because its
// query (lastTurnAt < threshold) returns the most candidates and
// running it every 5 minutes wastes Firestore reads.
const SALES_SCAN_INTERVAL_MS = 6 * 60 * 60 * 1000;

// Stages that are eligible for sales-funnel abandonment. pending_close_approval
// is NOT in this list — those threads are deals waiting on operator
// approval, not stalled customer conversations.
const REAPABLE_STAGES = new Set(["discovery", "spec", "quote", "revision"]);

// ─── Helpers ───────────────────────────────────────────────────────────

function json(statusCode, body) {
  return {
    statusCode,
    headers: { ...CORS, "Content-Type": "application/json" },
    body: JSON.stringify(body)
  };
}

async function writeAudit(threadId, draftId, eventType, payload, actor = "system:reapers", outcome = "success", ruleViolations = []) {
  try {
    await db.collection(AUDIT_COLL).add({
      threadId, draftId, eventType, actor, payload,
      createdAt: FV.serverTimestamp(),
      outcome, ruleViolations
    });
  } catch (e) {
    console.warn("reapers audit write failed:", e.message);
  }
}

// ═══════════════════════════════════════════════════════════════════════
//  Pass 1 — Auto-pipeline stale-claim reaper
// ═══════════════════════════════════════════════════════════════════════
//
// The auto-pipeline atomically claims a thread by setting:
//   lastAutoDecision   = "in_progress"
//   lastAutoDecisionAt = <serverTimestamp>
// When the pipeline finishes successfully it overwrites those fields.
// When it crashes mid-run — Lambda timeout, Anthropic API hang, network
// blip, OOM — the in_progress marker is left orphaned and the thread
// shows as "AI thinking..." indefinitely in the operator UI.
//
// This pass finds threads with stale in_progress markers (older than 5
// minutes), and clears them. The thread is left at pending_human_review
// so it's visible in the operator's Needs Review folder.

async function reapStaleClaim(threadRef) {
  return await db.runTransaction(async (tx) => {
    const snap = await tx.get(threadRef);
    if (!snap.exists) return { reaped: false, reason: "thread_gone" };
    const data = snap.data() || {};

    if (data.lastAutoDecision !== "in_progress") {
      return { reaped: false, reason: "no_longer_in_progress", currentDecision: data.lastAutoDecision };
    }

    const claimedAtMs = data.lastAutoDecisionAt && data.lastAutoDecisionAt.toMillis
      ? data.lastAutoDecisionAt.toMillis() : 0;
    const ageMs = Date.now() - claimedAtMs;
    if (ageMs < STALE_CLAIM_THRESHOLD_MS) {
      return { reaped: false, reason: "not_yet_stale", ageMs };
    }

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

async function runAutoPipelinePass() {
  const tStart = Date.now();
  const cutoffMs = Date.now() - STALE_CLAIM_THRESHOLD_MS;

  const snap = await db.collection(THREADS_COLL)
    .where("lastAutoDecision", "==", "in_progress")
    .limit(MAX_REAP_PER_RUN_PIPELINE * 2)
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

  if (candidates.length > MAX_REAP_PER_RUN_PIPELINE) {
    candidates = candidates.slice(0, MAX_REAP_PER_RUN_PIPELINE);
  }

  let reapedCount = 0;
  let skippedCount = 0;
  for (const c of candidates) {
    try {
      const result = await reapStaleClaim(c.ref);
      if (result.reaped) {
        reapedCount++;
        await writeAudit(c.id, null, "auto_pipeline_stale_claim_recovered", {
          staleForMs       : c.ageMs,
          previousStatus   : result.previousStatus,
          hadDraft         : result.hadDraft,
          staleThresholdMs : STALE_CLAIM_THRESHOLD_MS
        });
      } else {
        skippedCount++;
      }
    } catch (e) {
      console.warn("reapStaleClaim failed for", c.id, e.message);
      skippedCount++;
    }
  }

  return {
    pass         : "auto_pipeline",
    scanned      : snap.size,
    candidates   : candidates.length,
    reaped       : reapedCount,
    skipped      : skippedCount,
    durationMs   : Date.now() - tStart,
    thresholdMs  : STALE_CLAIM_THRESHOLD_MS
  };
}

// ═══════════════════════════════════════════════════════════════════════
//  Pass 2 — Send-queue reaper
// ═══════════════════════════════════════════════════════════════════════
//
// Drafts that have been enqueued (status=queued) or claimed (status=
// sending) by an extension can get stranded if the operator's browser
// is closed, the tab dies pre-click, or the tab dies post-click. The
// existing peek/claim paths in etsyMailDraftSend.js handle these on
// demand — but only when the extension actually peeks. If the extension
// is offline for hours/days, the queue grows unbounded.
//
// Staleness:
//   queued + queuedAt > MAX_CLAIM_LOOKBACK_MIN (30 min)
//     → mark failed (QUEUED_EXPIRED), demote thread.
//   sending + pre_click + heartbeat > 60s old
//     → mark failed (CLAIM_ABANDONED), demote thread. Safe to re-send.
//   sending + post_click + heartbeat > 60s old
//     → mark sent_unverified (STRANDED_POST_CLICK), demote thread. Operator
//       MUST verify on Etsy before taking any further action. Never blindly re-send.

async function reapStaleDraft(draftRef, kind) {
  return await db.runTransaction(async (tx) => {
    const snap = await tx.get(draftRef);
    if (!snap.exists) return { reaped: false, reason: "draft_gone" };
    const d = snap.data();

    // Re-check staleness inside the txn
    if (d.status === "queued") {
      if (!isStaleQueued(d.queuedAt)) {
        return { reaped: false, reason: "queued_not_yet_stale" };
      }
    } else if (d.status === "sending") {
      if (!isStaleHeartbeat(d.sendHeartbeatAt)) {
        return { reaped: false, reason: "sending_heartbeat_fresh" };
      }
    } else {
      return { reaped: false, reason: "already_terminal", currentStatus: d.status };
    }

    let sendErrorCode, sendError, decisionReason;
    let terminalStatus = "failed";   // default — failed sends
    let setSentAt      = false;      // sent_unverified should also stamp sentAt
    if (d.status === "queued") {
      sendErrorCode  = "QUEUED_EXPIRED";
      sendError      = `Expired by reaper — queued more than ${MAX_CLAIM_LOOKBACK_MIN} minutes (extension may be offline)`;
      decisionReason = "human_review_after_queued_expired";
    } else if (d.sendStage === "post_click") {
      // v2.6 fix: STRANDED_POST_CLICK is NOT a failure — the extension
      // typed the message AND clicked Etsy's Send button. The "stranded"
      // part means we just don't have a confirmation toast/signal. The
      // message almost always WAS delivered (Etsy's Send is reliable),
      // so we use `sent_unverified` semantics:
      //   - status: sent_unverified  (not "failed")
      //   - sentAt: now              (so the UI's optimistic message
      //     insert fires, putting the just-sent text into the thread
      //     view immediately instead of leaving the operator wondering)
      //   - thread → human_review    (so the operator can verify)
      // Treating this as `failed` was the prior bug: the operator saw
      // a red error banner and re-sent, creating duplicate messages.
      sendErrorCode  = "STRANDED_POST_CLICK";
      sendError      = "Send was clicked. Etsy didn't return a confirmation signal within the timeout — verify on Etsy that the message went through. (Most likely it did; this status just means we couldn't auto-confirm.)";
      decisionReason = "human_review_after_stranded_post_click";
      terminalStatus = "sent_unverified";
      setSentAt      = true;
    } else {
      sendErrorCode  = "CLAIM_ABANDONED";
      sendError      = "Extension claimed the draft but never clicked Send (heartbeat stale). Safe to re-send.";
      decisionReason = "human_review_after_claim_abandoned";
    }

    const draftPatch = {
      status          : terminalStatus,
      sendError,
      sendErrorCode,
      sendHeartbeatAt : FV.serverTimestamp(),
      updatedAt       : FV.serverTimestamp()
    };
    if (setSentAt) draftPatch.sentAt = FV.serverTimestamp();
    tx.set(draftRef, draftPatch, { merge: true });

    const threadStatusUpdate = await demoteThreadInTxn(tx, d.threadId, decisionReason);

    return {
      reaped: true,
      threadId: d.threadId,
      sendErrorCode,
      threadStatusUpdate,
      sendStage: d.sendStage,
      ageMs: kind === "queued"
        ? (d.queuedAt ? Date.now() - d.queuedAt.toMillis() : null)
        : (d.sendHeartbeatAt ? Date.now() - d.sendHeartbeatAt.toMillis() : null)
    };
  });
}

async function runSendQueuePass() {
  const tStart = Date.now();
  let totalReaped = 0;
  let totalScanned = 0;
  let totalSkipped = 0;
  const failures = [];

  // ── Pass 2a: stale `queued` drafts ────────────────────────────
  const queuedSnap = await db.collection(DRAFTS_COLL)
    .where("status", "==", "queued")
    .limit(MAX_REAP_PER_RUN_SEND * 2)
    .get();
  totalScanned += queuedSnap.size;

  for (const doc of queuedSnap.docs) {
    if (totalReaped >= MAX_REAP_PER_RUN_SEND) break;
    const d = doc.data();
    if (!isStaleQueued(d.queuedAt)) { totalSkipped++; continue; }
    try {
      const r = await reapStaleDraft(doc.ref, "queued");
      if (r.reaped) {
        totalReaped++;
        await writeAudit(r.threadId, doc.id, "draft_queue_expired_by_reaper", {
          sendErrorCode: r.sendErrorCode,
          ageMs        : r.ageMs,
          threadStatusUpdate: r.threadStatusUpdate
        }, "system:sendQueueReaper");
      } else {
        totalSkipped++;
      }
    } catch (e) {
      failures.push({ draftId: doc.id, error: e.message });
    }
  }

  // ── Pass 2b: stale `sending` drafts ───────────────────────────
  if (totalReaped < MAX_REAP_PER_RUN_SEND) {
    const sendingSnap = await db.collection(DRAFTS_COLL)
      .where("status", "==", "sending")
      .limit(MAX_REAP_PER_RUN_SEND * 2)
      .get();
    totalScanned += sendingSnap.size;

    for (const doc of sendingSnap.docs) {
      if (totalReaped >= MAX_REAP_PER_RUN_SEND) break;
      const d = doc.data();
      if (!isStaleHeartbeat(d.sendHeartbeatAt)) { totalSkipped++; continue; }
      try {
        const r = await reapStaleDraft(doc.ref, "sending");
        if (r.reaped) {
          totalReaped++;
          await writeAudit(r.threadId, doc.id, "draft_send_reaped", {
            sendErrorCode: r.sendErrorCode,
            sendStage    : r.sendStage,
            ageMs        : r.ageMs,
            threadStatusUpdate: r.threadStatusUpdate
          }, "system:sendQueueReaper");
        } else {
          totalSkipped++;
        }
      } catch (e) {
        failures.push({ draftId: doc.id, error: e.message });
      }
    }
  }

  return {
    pass       : "send_queue",
    scanned    : totalScanned,
    reaped     : totalReaped,
    skipped    : totalSkipped,
    failures   : failures.length,
    failureLog : failures.slice(0, 10),
    durationMs : Date.now() - tStart
  };
}

// ═══════════════════════════════════════════════════════════════════════
//  Pass 3 — Sales-funnel abandonment reaper
// ═══════════════════════════════════════════════════════════════════════
//
// Detects sales threads where SalesContext.stage is in [discovery, spec,
// quote, revision] and lastTurnAt is older than ABANDON_AFTER_DAYS days.
// Marks them abandoned in both SalesContext and the parent thread.

async function isSalesModeEnabled() {
  try {
    const doc = await db.collection(CONFIG_COLL).doc("autoPipeline").get();
    if (!doc.exists) return false;
    return doc.data().salesModeEnabled === true;
  } catch (e) {
    console.warn("salesReaper: config read failed:", e.message);
    return false;
  }
}

/** Read the sales-pass time gate. Returns true iff it's been longer
 *  than SALES_SCAN_INTERVAL_MS since the last sales scan, OR the gate
 *  doc is missing (first run). */
async function shouldRunSalesPass() {
  try {
    const doc = await db.collection(CONFIG_COLL).doc("reaperState").get();
    if (!doc.exists) return true;
    const lastMs = doc.data().lastSalesScanAt && doc.data().lastSalesScanAt.toMillis
      ? doc.data().lastSalesScanAt.toMillis() : 0;
    return (Date.now() - lastMs) >= SALES_SCAN_INTERVAL_MS;
  } catch (e) {
    console.warn("salesReaper: gate read failed (proceeding):", e.message);
    return true;
  }
}

async function markSalesPassRan() {
  try {
    await db.collection(CONFIG_COLL).doc("reaperState").set({
      lastSalesScanAt: FV.serverTimestamp()
    }, { merge: true });
  } catch (e) {
    console.warn("salesReaper: gate write failed:", e.message);
  }
}

async function reapAbandonedSalesThread(threadId, thresholdMs) {
  const ctxRef    = db.collection(SALES_COLL).doc(threadId);
  const threadRef = db.collection(THREADS_COLL).doc(threadId);

  return await db.runTransaction(async (tx) => {
    const ctxSnap = await tx.get(ctxRef);
    if (!ctxSnap.exists) return { reaped: false, reason: "context_missing" };
    const ctx = ctxSnap.data() || {};

    if (!REAPABLE_STAGES.has(ctx.stage)) {
      return { reaped: false, reason: "stage_not_reapable", stage: ctx.stage };
    }

    const lastTurnMs = ctx.lastTurnAt && ctx.lastTurnAt.toMillis ? ctx.lastTurnAt.toMillis() : 0;
    if (lastTurnMs >= thresholdMs) {
      return { reaped: false, reason: "fresh", lastTurnMs, thresholdMs };
    }

    tx.set(ctxRef, {
      stage      : "abandoned",
      abandonedAt: FV.serverTimestamp(),
      lastSalesAgentBlockReason: null
    }, { merge: true });

    tx.set(threadRef, {
      status   : "sales_abandoned",
      salesStage: "abandoned",
      updatedAt: FV.serverTimestamp()
    }, { merge: true });

    return {
      reaped: true,
      fromStage: ctx.stage,
      lastTurnAtMs: lastTurnMs
    };
  });
}

// v0.9.47 — Sales abandonment removed from system policy. The
// sales-funnel reaper pass returns immediately without touching
// any threads. The auto_pipeline and send_queue passes are
// unaffected. Operators manually archive stale sales threads from
// the Sales — Active folder.
const SALES_FUNNEL_PASS_DISABLED = true;

async function runSalesFunnelPass({ force = false } = {}) {
  const tStart = Date.now();

  if (SALES_FUNNEL_PASS_DISABLED) {
    return {
      pass: "sales_funnels",
      skipped: true,
      reason: "abandonment_removed_from_policy",
      durationMs: Date.now() - tStart
    };
  }

  if (!(await isSalesModeEnabled())) {
    return { pass: "sales_funnels", skipped: true, reason: "sales_mode_disabled", durationMs: Date.now() - tStart };
  }
  if (!force && !(await shouldRunSalesPass())) {
    return { pass: "sales_funnels", skipped: true, reason: "interval_gated", intervalMs: SALES_SCAN_INTERVAL_MS, durationMs: Date.now() - tStart };
  }

  const thresholdMs = Date.now() - (ABANDON_AFTER_DAYS * 24 * 60 * 60 * 1000);
  const thresholdTs = admin.firestore.Timestamp.fromMillis(thresholdMs);

  let snap;
  try {
    snap = await db.collection(SALES_COLL)
      .where("lastTurnAt", "<", thresholdTs)
      .orderBy("lastTurnAt", "asc")
      .limit(MAX_THREADS_PER_RUN)
      .get();
  } catch (e) {
    if (/index/i.test(e.message)) {
      console.error("salesReaper: composite index required.", e.message);
      await writeAudit(null, null, "sales_reaper_index_missing", { error: e.message }, "system:salesReaper", "failure", ["MISSING_FIRESTORE_INDEX"]);
      return { pass: "sales_funnels", error: "Missing Firestore index — see function logs", needsIndex: true, durationMs: Date.now() - tStart };
    }
    throw e;
  }

  // Always mark the gate, even if scan was empty — the gate's purpose
  // is "we did the work", not "we found something". Doing it before the
  // per-thread loop means a partial-failure run still updates the gate
  // (we don't want a single bad thread re-running the entire scan in
  // 5 min).
  await markSalesPassRan();

  if (snap.empty) {
    return { pass: "sales_funnels", scanned: 0, reaped: 0, durationMs: Date.now() - tStart };
  }

  let reapedCount = 0;
  const reapedThreads = [];
  const skipped = [];

  for (const doc of snap.docs) {
    const threadId = doc.id;
    const ctxData = doc.data() || {};

    if (!REAPABLE_STAGES.has(ctxData.stage)) {
      skipped.push({ threadId, reason: "stage_not_reapable", stage: ctxData.stage });
      continue;
    }

    try {
      const result = await reapAbandonedSalesThread(threadId, thresholdMs);
      if (result.reaped) {
        reapedCount++;
        const ageDays = Math.round((Date.now() - result.lastTurnAtMs) / (24 * 60 * 60 * 1000));
        reapedThreads.push({ threadId, fromStage: result.fromStage, lastTurnAtMs: result.lastTurnAtMs, ageDays });
        await writeAudit(threadId, null, "sales_abandoned", {
          fromStage      : result.fromStage,
          lastTurnAtMs   : result.lastTurnAtMs,
          ageDays,
          abandonAfterDays: ABANDON_AFTER_DAYS
        }, "system:salesReaper");
      } else {
        skipped.push({ threadId, reason: result.reason });
      }
    } catch (e) {
      console.warn(`salesReaper: thread ${threadId} reap failed:`, e.message);
      skipped.push({ threadId, reason: "transaction_error", error: e.message });
    }
  }

  if (reapedCount > 0 || snap.size >= MAX_THREADS_PER_RUN) {
    await writeAudit(null, null, "sales_reaper_scan_complete", {
      scanned         : snap.size,
      reaped          : reapedCount,
      capacityHit     : snap.size >= MAX_THREADS_PER_RUN,
      abandonAfterDays: ABANDON_AFTER_DAYS,
      thresholdMs,
      reapedSample    : reapedThreads.slice(0, 10),
      durationMs      : Date.now() - tStart
    }, "system:salesReaper");
  }

  return {
    pass       : "sales_funnels",
    scanned    : snap.size,
    reaped     : reapedCount,
    skipped    : skipped.length,
    capacityHit: snap.size >= MAX_THREADS_PER_RUN,
    reapedThreads,
    durationMs : Date.now() - tStart
  };
}

// ═══════════════════════════════════════════════════════════════════════
//  Handler
// ═══════════════════════════════════════════════════════════════════════

exports.handler = async (event) => {
  if (event.httpMethod === "OPTIONS") {
    return { statusCode: 200, headers: CORS, body: "ok" };
  }

  const scheduled = isScheduledInvocation(event);

  if (!scheduled && event.httpMethod) {
    const auth = requireExtensionAuth(event);
    if (!auth.ok) return auth.response;
  }

  // Optional body: `{ op: "auto_pipeline" | "send_queue" | "sales_funnels", force?: bool }`
  // for targeted manual sweeps. Default is to run all three.
  let body = {};
  if (event.body) {
    try { body = JSON.parse(event.body); } catch { body = {}; }
  }
  const op    = body.op || null;
  const force = body.force === true;

  const tStart = Date.now();
  const results = {};
  const errors  = [];

  try {
    if (!op || op === "auto_pipeline") {
      try { results.autoPipeline = await runAutoPipelinePass(); }
      catch (e) { errors.push({ pass: "auto_pipeline", error: e.message }); console.error("autoPipeline pass:", e); }
    }
    if (!op || op === "send_queue") {
      try { results.sendQueue = await runSendQueuePass(); }
      catch (e) { errors.push({ pass: "send_queue", error: e.message }); console.error("sendQueue pass:", e); }
    }
    if (!op || op === "sales_funnels") {
      try { results.salesFunnels = await runSalesFunnelPass({ force }); }
      catch (e) { errors.push({ pass: "sales_funnels", error: e.message }); console.error("salesFunnels pass:", e); }
    }

    const totalReaped =
        ((results.autoPipeline && results.autoPipeline.reaped) || 0)
      + ((results.sendQueue    && results.sendQueue.reaped)    || 0)
      + ((results.salesFunnels && results.salesFunnels.reaped) || 0);

    const summary = {
      success    : errors.length === 0,
      ranOp      : op || "all",
      totalReaped,
      results,
      errors,
      durationMs : Date.now() - tStart,
      ranAt      : new Date().toISOString()
    };

    if (totalReaped > 0 || errors.length > 0) {
      console.log("etsyMailReapers:", JSON.stringify(summary));
    }

    return json(errors.length === 0 ? 200 : 207, summary);

  } catch (err) {
    console.error("etsyMailReapers unhandled error:", err);
    return json(500, { error: err.message || String(err), durationMs: Date.now() - tStart });
  }
};

// Exports for tests / manual debugging.
module.exports.runAutoPipelinePass         = runAutoPipelinePass;
module.exports.runSendQueuePass            = runSendQueuePass;
module.exports.runSalesFunnelPass          = runSalesFunnelPass;
module.exports.reapStaleClaim              = reapStaleClaim;
module.exports.reapStaleDraft              = reapStaleDraft;
module.exports.reapAbandonedSalesThread    = reapAbandonedSalesThread;
module.exports.REAPABLE_STAGES             = Array.from(REAPABLE_STAGES);
module.exports.STALE_CLAIM_THRESHOLD_MS    = STALE_CLAIM_THRESHOLD_MS;
module.exports.SALES_SCAN_INTERVAL_MS      = SALES_SCAN_INTERVAL_MS;
