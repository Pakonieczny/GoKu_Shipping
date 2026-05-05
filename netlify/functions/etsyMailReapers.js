/*  netlify/functions/etsyMailReapers.js
 *
 *  v2.5 — Consolidated reaper. Runs four reaper passes on each
 *  invocation:
 *
 *    1. AUTO_PIPELINE_CLAIMS — clear stale `in_progress` markers from
 *       crashed pipeline runs (was: etsyMailAutoPipelineReaper).
 *    2. SEND_QUEUE          — fail/abandon stranded queued + sending
 *       drafts (was: etsyMailSendQueueReaper).
 *    3. SALES_FUNNELS       — mark abandoned sales conversations
 *       (was: etsyMailSalesReaper).
 *    4. GMAIL_SCRAPE        — recover stuck Gmail-watcher → scrape jobs
 *       (v2.5; closes the loop on threads that get stranded at
 *       customerName="Unknown" because the Chrome extension wasn't
 *       running, crashed mid-claim, or scraped an Etsy page that no
 *       longer rendered the customer name).
 *
 *  ═══ WHY ONE FILE ════════════════════════════════════════════════════
 *
 *  Previously three separate scheduled functions, each on a different
 *  cron cadence (5 min / 5 min / 6 h). Consolidating reduces deploy
 *  surface and audit noise. Each pass is independently bounded
 *  (MAX_REAP_PER_RUN_*) and short-circuits when there's nothing to do,
 *  so running all passes on the most aggressive cadence (5 min) costs
 *  ~one indexed Firestore query per reaper-with-zero-work — negligible.
 *
 *  Sales-funnel scan would otherwise run 72× more often (every 5 min vs
 *  every 6 h). To keep query volume sane, the sales-funnel pass uses an
 *  internal time-gate (lastSalesScanAt in EtsyMail_Config/reaperState)
 *  so it ACTUALLY runs only once per SALES_SCAN_INTERVAL_MS. The other
 *  passes run on every invocation.
 *
 *  v3.1 — sub-sweep D: defensive Unicode unmangler. Repairs threads
 *  where the Chrome scraper stored literal `\uXXXX` escape sequences
 *  in customerName / subject (e.g. "Caitr\u00edona" instead of
 *  "Caitríona") by JSON-stringifying its inputs upstream. Snapshot
 *  ingest now decodes on the way in; this sub-sweep cleans rows that
 *  predate that fix. Idempotent — once a row is clean, it's skipped
 *  on every subsequent pass.
 *
 *  ═══ v2.5 ADDITION — gmail_scrape pass ═══════════════════════════════
 *
 *  v2.5.1 NOTE — INDEX-FREE QUERIES: All sub-sweep queries here use
 *  single-field equality only. Multi-field inequality + orderBy queries
 *  would be more efficient (Firestore could prune server-side) but they
 *  require composite indexes that have to be provisioned out-of-band in
 *  Firebase. Without those indexes the queries throw and the entire
 *  pass silently fails — exactly the symptom that surfaced in the
 *  field. We over-fetch and filter client-side instead; the volumes
 *  involved (claimed scrape jobs, detected_from_gmail threads,
 *  customerName="Unknown" threads) are all small enough that the
 *  client-side filter is fine.
 *
 *  Three sub-sweeps, run in order on every invocation:
 *
 *    Sub-sweep A — Stuck claimed scrape jobs
 *      EtsyMail_Jobs where jobType=="scrape" AND status=="claimed" AND
 *      claimedAt < now - SCRAPE_STUCK_CLAIM_MS. Revert to "queued" so the
 *      extension picks them up next poll. After MAX_SCRAPE_ATTEMPTS the
 *      job goes to "failed" instead, breaking any infinite-retry loop.
 *
 *    Sub-sweep B — detected_from_gmail threads with no live job
 *      EtsyMail_Threads where status=="detected_from_gmail" AND createdAt
 *      < now - SCRAPE_DETECTED_GRACE_MS. For each:
 *        (1) If customerName is still "Unknown", try to fill it from the
 *            email subject (Etsy notification subjects always carry the
 *            buyer's name — see extractCustomerNameFromSubject). This is
 *            independent of the job recovery; it makes the inbox useful
 *            even when the extension is permanently down.
 *        (2) Look up the deterministic gmail_<gmailMessageId> job: if
 *            missing or "failed", enqueue a fresh job; if "queued"/
 *            "claimed"/"succeeded", leave alone. After MAX_SCRAPE_ATTEMPTS
 *            the thread is tagged "scrape_exhausted" for operator follow-up.
 *
 *    Sub-sweep C — Successful scrape but customerName=="Unknown"
 *      EtsyMail_Threads where customerName=="Unknown" AND status is
 *      post-scrape (etsy_scraped, ai_drafted, etc) AND
 *      _unknownRetryAttempted!==true AND lastSyncedAt is older than
 *      SCRAPE_UNKNOWN_GRACE_MS. Two-step recovery:
 *        (1) Try the subject-fill first (cheap, no extension needed).
 *            If it succeeds, consume the one-shot guard and skip the
 *            rescrape — the thread is now labeled correctly.
 *        (2) Otherwise, set _unknownRetryAttempted=true under a
 *            transaction BEFORE enqueueing the rescrape job, so two
 *            reaper invocations racing can never produce duplicate
 *            retries. One rescrape per thread, ever.
 *
 *  The subject-fill paths in (B1) and (C1) are the high-impact recovery:
 *  Etsy notification emails always carry the customer's name in the
 *  subject ("Re: Etsy Conversation with <NAME>"), so a thread can be
 *  labeled correctly without any Etsy roundtrip — useful when the
 *  Chrome extension is offline, the operator's session has expired, or
 *  Etsy's DOM has shifted out from under the scraper.
 *
 *  Why this lives here, not in a standalone reaper file:
 *    The "consolidated reaper" pattern is the existing convention in
 *    this codebase (see "WHY ONE FILE" above). A separate
 *    etsyMailGmailScrapeReaper.js would mean another scheduled
 *    function, another netlify.toml entry, another set of audit-row
 *    actor strings to filter on. The gmail_scrape pass costs one
 *    indexed query per sub-sweep when idle — same as every other pass
 *    here — so consolidation is essentially free.
 *
 *  ═══ INVOCATION ════════════════════════════════════════════════════
 *
 *  Scheduled cron:        netlify.toml schedule (every 5 minutes)
 *  Manual full sweep:     POST /.netlify/functions/etsyMailReapers
 *  Manual single pass:    POST { op: "auto_pipeline" | "send_queue" |
 *                                    "sales_funnels" | "gmail_scrape" }
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
// v2.5 — Jobs collection used by the gmail_scrape pass below.
const JOBS_COLL    = "EtsyMail_Jobs";

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

// ─── Gmail-scrape reaper config (v2.5) ─────────────────────────────────
// A scrape job is "stuck claimed" if the extension grabbed it but never
// progressed. Real scrapes finish in seconds; we wait 5 min before
// declaring the worker dead. Well past the extension's 20s claim-poll
// cycle, so a healthy-but-slow worker won't be reaped.
const SCRAPE_STUCK_CLAIM_MS = 5 * 60 * 1000;

// Grace period after a thread is created at "detected_from_gmail"
// before sub-sweep B starts re-enqueueing for it. Gives the extension
// first dibs — its claim-poll runs every 20s, so 3 min is generous.
const SCRAPE_DETECTED_GRACE_MS = 3 * 60 * 1000;

// Grace period after a successful scrape that left customerName=
// "Unknown" before sub-sweep C retries. Lets any in-flight follow-up
// writes settle and avoids racing the snapshot endpoint's commit.
const SCRAPE_UNKNOWN_GRACE_MS = 2 * 60 * 1000;

// Mirrors MAX_ATTEMPTS in etsyMailJobs.js. If they ever diverge, jobs
// could get stuck in a loop where the reaper keeps requeueing past the
// extension's max-attempts threshold. Keep aligned.
const MAX_SCRAPE_ATTEMPTS = 3;

// Defense-in-depth caps per invocation. The 30s scheduled-function
// budget is plenty for these numbers; the cap is mainly to prevent a
// misconfig (e.g. an entire day of jobs all stuck) from blowing the
// budget on one tick. Anything not handled this tick gets handled the
// next.
const MAX_SCRAPE_REAP_PER_RUN = 50;

// Statuses that mean "the scrape did happen, but the result might still
// have customerName=Unknown" — sub-sweep C only retries threads that
// are already past the initial detection step.
const SCRAPE_POST_STATUSES = [
  "etsy_scraped",
  "ai_drafted",
  "needs_review",
  "auto_replied",
  "replied",
  "closed"
];

/**
 * Extract the customer name from an Etsy notification email subject.
 *
 * Source of truth: this is a copy of the same-named function in
 * etsyMailGmail-background.js. Duplicated here (rather than imported)
 * because background-function modules aren't a stable import surface
 * in Netlify, and the function is small + rarely changes. If you edit
 * one copy, edit the other — they should stay in lockstep.
 *
 * Used by sub-sweep C to skip the rescrape entirely when the email
 * subject already carries the buyer's name. Empirically that's true
 * for >99% of "Unknown" threads in this system, since Etsy's
 * notification emails always include the customer name in the subject.
 *
 * Returns the cleaned name, or null if the subject doesn't match the
 * expected "Etsy Conversation with <NAME>" pattern.
 */
function extractCustomerNameFromSubject(subject) {
  if (!subject || typeof subject !== "string") return null;
  let s = subject.replace(/^\s*(?:re|fwd|fw)\s*:\s*/gi, "").trim();
  const m = s.match(/Etsy Conversation with[\s\u00A0]+(.+)$/i);
  if (!m) return null;
  let name = m[1];
  name = name.replace(/\s+about\s+.*$/i, "");
  const comma = name.indexOf(",");
  if (comma !== -1) name = name.slice(0, comma);
  name = name.replace(/\s+/g, " ").trim();
  if (!name) return null;
  if (name.length > 200) name = name.slice(0, 200).trim();
  return name;
}

/**
 * v3.1 — Defensive decoder for JSON-stringified Unicode escape sequences.
 *
 * Source of truth: this is a copy of the same-named function in
 * etsyMailSnapshot.js. See that file's comment for the full rationale —
 * tl;dr the Chrome scraper is round-tripping non-ASCII customer names
 * and subjects through JSON.stringify, producing literal `\uXXXX`
 * escape sequences instead of the actual characters.
 *
 * Snapshot ingest now decodes on the way in. This copy is used by
 * sub-sweep D below to repair existing mangled rows that landed before
 * the snapshot fix was deployed.
 *
 * Edit both copies in lockstep if the rule changes.
 */
function unmangleEscapedUnicode(s) {
  if (typeof s !== "string" || s.length === 0) return s;
  if (s.indexOf("\\u") === -1) return s;
  // Surrogate-pair pass first (astral-plane code points like emoji).
  let out = s.replace(
    /\\u([dD][89aAbB][0-9a-fA-F]{2})\\u([dD][c-fC-F][0-9a-fA-F]{2})/g,
    (_m, hi, lo) => {
      const high = parseInt(hi, 16);
      const low  = parseInt(lo, 16);
      try {
        return String.fromCodePoint(((high - 0xD800) << 10) + (low - 0xDC00) + 0x10000);
      } catch {
        return _m;
      }
    }
  );
  // Then single-BMP escapes — but only for code points >= 0x80, so we
  // don't accidentally "decode" a literal backslash-u-ASCII pair from
  // an unrelated docstring or template field.
  out = out.replace(/\\u([0-9a-fA-F]{4})/g, (m, hex) => {
    const cp = parseInt(hex, 16);
    if (cp < 0x80) return m;
    try {
      return String.fromCharCode(cp);
    } catch {
      return m;
    }
  });
  return out;
}

/**
 * Returns true if the input is a string that contains at least one
 * `\uXXXX` escape sequence representing a non-ASCII code point — i.e.
 * a string that the unmangler would actually change.
 */
function hasMangledEscapes(s) {
  if (typeof s !== "string" || s.length === 0) return false;
  if (s.indexOf("\\u") === -1) return false;
  // Quick check: any \uHHHH where HH >= 80 (non-ASCII)?
  return /\\u(?:00[89a-fA-F]|0[1-9a-fA-F][0-9a-fA-F]|[1-9a-fA-F][0-9a-fA-F]{2})/i.test(s);
}

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

async function runSalesFunnelPass({ force = false } = {}) {
  const tStart = Date.now();

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
//  Pass 4 — Gmail-watcher → scrape-job recovery (v2.5)
// ═══════════════════════════════════════════════════════════════════════
//
// Three sub-sweeps that together close the loop on threads stranded at
// customerName="Unknown" by the Gmail-watcher → extension-scraper
// pipeline. See file header for the full failure-mode taxonomy. Each
// sub-sweep is independently bounded and idempotent; running this pass
// twice in quick succession cannot produce duplicate work.

// Centralized "is this job alive?" check used by sub-sweep B so any
// future caller (manual-rescrape endpoint, etc.) shares the rule.
function isLiveScrapeJob(jobData) {
  if (!jobData) return false;
  return ["queued", "claimed", "succeeded"].includes(jobData.status);
}

/**
 * Try to fill customerName from the email subject already stored on
 * the thread. Used by sub-sweeps B and C as a fast-path before falling
 * back to a rescrape — Etsy notification subjects always carry the
 * buyer's name in plain text, so when the watcher has stored that
 * subject we don't need the extension to scrape anything.
 *
 * Transactional so two reaper invocations racing each other can never
 * both fill the same thread (the second one sees customerName already
 * populated and aborts).
 *
 * Returns:
 *   { filled: true,  name }  — customerName patched, audit row written
 *   { filled: false, reason: "no_subject" | "subject_unparseable"
 *                            | "already_named" | "tx_aborted" }
 */
async function tryFillCustomerNameFromSubject(threadRef, threadData) {
  const subject = threadData && threadData.subject;
  if (!subject) return { filled: false, reason: "no_subject" };

  const parsed = extractCustomerNameFromSubject(subject);
  if (!parsed) return { filled: false, reason: "subject_unparseable" };

  // Transactional check-and-fill. If another caller raced us and the
  // thread is no longer at "Unknown", we abort without overwriting.
  let outcome;
  try {
    outcome = await db.runTransaction(async (tx) => {
      const fresh = await tx.get(threadRef);
      if (!fresh.exists) return { filled: false, reason: "tx_aborted" };
      const data = fresh.data() || {};
      const isPlaceholder =
           !data.customerName
        || data.customerName === "Unknown"
        || data.customerName === "";
      if (!isPlaceholder) return { filled: false, reason: "already_named" };

      tx.update(threadRef, {
        customerName            : parsed,
        customerNameFromSubject : true,
        updatedAt               : FV.serverTimestamp()
      });
      return { filled: true, name: parsed };
    });
  } catch (err) {
    console.warn(`[gmail-scrape-reaper] fill-from-subject tx failed for ${threadRef.id}:`, err.message);
    return { filled: false, reason: "tx_aborted" };
  }

  if (outcome.filled) {
    await writeAudit(
      threadRef.id,
      null,
      "thread_customer_name_filled_from_subject",
      { customerName: parsed, subject },
      "system:reapers:gmailScrape"
    );
  }
  return outcome;
}

/**
 * Sub-sweep A — Stuck claimed scrape jobs.
 * Reverts to "queued" so the extension picks them up next poll.
 * After MAX_SCRAPE_ATTEMPTS the job goes to "failed" instead of
 * looping. Transactional check-and-flip prevents racing reapers from
 * double-requeueing.
 */
async function reapStuckScrapeClaims() {
  const stuckCutoffMs = Date.now() - SCRAPE_STUCK_CLAIM_MS;

  // Single-field equality query — no composite index required. We
  // intentionally do NOT add the claimedAt < cutoff filter here, even
  // though that would prune the candidate set, because combining
  // jobType+status+claimedAt requires a composite index in Firestore.
  // Without that index the query throws and the entire pass silently
  // fails (caught by the outer try/catch in runGmailScrapePass, but the
  // operator never sees that error). Filtering claimedAt client-side
  // costs us nothing — claimed scrape jobs are always rare relative to
  // queued/succeeded ones, so the result set is small.
  //
  // We over-fetch (5x the per-run cap) to ensure we have enough
  // candidates after client-side filtering.
  const snap = await db.collection(JOBS_COLL)
    .where("jobType", "==", "scrape")
    .where("status",  "==", "claimed")
    .limit(MAX_SCRAPE_REAP_PER_RUN * 5)
    .get();

  if (snap.empty) return { requeued: 0, exhausted: 0 };

  let requeued  = 0;
  let exhausted = 0;
  let processed = 0;

  for (const docSnap of snap.docs) {
    if (processed >= MAX_SCRAPE_REAP_PER_RUN) break;

    const data = docSnap.data() || {};
    const claimedAtMs = data.claimedAt && data.claimedAt.toMillis
      ? data.claimedAt.toMillis() : 0;
    // Client-side staleness filter — see comment above on why this isn't
    // in the query.
    if (!claimedAtMs || claimedAtMs > stuckCutoffMs) continue;

    processed++;
    const ref = docSnap.ref;
    try {
      const outcome = await db.runTransaction(async (tx) => {
        const fresh = await tx.get(ref);
        if (!fresh.exists) return { action: "gone" };
        const fdata = fresh.data() || {};
        if (fdata.status !== "claimed") return { action: "no_longer_claimed" };

        // Re-check staleness inside the tx — claimedAt might have been
        // updated between the query and now (worker submitted a heartbeat).
        const fClaimedAtMs = fdata.claimedAt && fdata.claimedAt.toMillis
          ? fdata.claimedAt.toMillis() : 0;
        if (Date.now() - fClaimedAtMs < SCRAPE_STUCK_CLAIM_MS) {
          return { action: "no_longer_stuck" };
        }

        const attempts = fdata.attempts || 0;
        if (attempts >= MAX_SCRAPE_ATTEMPTS) {
          // Don't loop — escalate to failed and let the operator look.
          tx.update(ref, {
            status   : "failed",
            lastError: `Stuck in 'claimed' past ${SCRAPE_STUCK_CLAIM_MS}ms with no heartbeat; attempts=${attempts} reached MAX_SCRAPE_ATTEMPTS`,
            updatedAt: FV.serverTimestamp()
          });
          return { action: "exhausted", attempts, threadId: fdata.threadId };
        }

        tx.update(ref, {
          status    : "queued",
          claimedBy : null,
          claimedAt : null,
          // attempts left as-is — claimNextJob's tx in etsyMailJobs.js
          // will increment it on next claim.
          lastError : `Reaped stuck claim after ${SCRAPE_STUCK_CLAIM_MS}ms (worker died?)`,
          updatedAt : FV.serverTimestamp()
        });
        return { action: "requeued", attempts, threadId: fdata.threadId };
      });

      if (outcome.action === "requeued") {
        requeued++;
        await writeAudit(outcome.threadId, null, "scrape_job_reaped_stuck_claim",
          { jobId: ref.id, attempts: outcome.attempts },
          "system:reapers:gmailScrape"
        );
      } else if (outcome.action === "exhausted") {
        exhausted++;
        await writeAudit(outcome.threadId, null, "scrape_job_exhausted",
          { jobId: ref.id, attempts: outcome.attempts, reason: "stuck_claim_max_attempts" },
          "system:reapers:gmailScrape"
        );
      }
    } catch (err) {
      console.warn(`[gmail-scrape-reaper] subsweep A tx failed for ${ref.id}:`, err.message);
    }
  }

  return { requeued, exhausted };
}

/**
 * Sub-sweep B — detected_from_gmail threads with no live job.
 *
 * Two recoveries happen here, in order:
 *
 *   1. Subject-fill (cheap, no extension needed).
 *      If the thread is still showing customerName="Unknown" but has a
 *      parseable email subject, populate the customer name from the
 *      subject. This is independent of whether the scrape ever runs —
 *      it just makes the inbox useful immediately. Threads that never
 *      get scraped (extension permanently down) still display the
 *      buyer's name instead of "Unknown".
 *
 *   2. Job re-enqueue (only if no live job is in flight).
 *      Either no job ever got created (extension wasn't running and
 *      the watcher's enqueue silently lost the doc somehow), or the
 *      prior job is in "failed" status. Either way, enqueue a fresh
 *      scrape job using the deterministic gmail_<msgId> id (matches
 *      the watcher's pattern, preserves idempotency for any concurrent
 *      watcher tick).
 *
 * The two recoveries are independent — a thread can have its name
 * filled from subject AND a fresh scrape job queued in the same tick.
 * The eventual scrape will overwrite customerName with the real Etsy
 * value, but the customerNameFromSubject flag advertises the
 * provenance so we know which value is the placeholder.
 */
async function reapDetectedThreadsWithoutLiveJobs() {
  const graceMs = SCRAPE_DETECTED_GRACE_MS;
  const graceCutoffMs = Date.now() - graceMs;

  // Single-field equality query — no composite index required. We
  // intentionally do NOT add `createdAt < cutoff` + `orderBy createdAt`
  // here because that requires a composite index in Firestore. Without
  // the index the query would throw and the entire pass would silently
  // fail. We over-fetch and apply the grace-period cutoff client-side.
  //
  // Volume-wise this is fine: detected_from_gmail is a transient
  // status (advances to etsy_scraped on first successful scrape), so
  // the result set is bounded by however many threads are mid-pipeline
  // at any given moment. The MAX_SCRAPE_REAP_PER_RUN * 5 over-fetch
  // gives us plenty of headroom.
  const snap = await db.collection(THREADS_COLL)
    .where("status", "==", "detected_from_gmail")
    .limit(MAX_SCRAPE_REAP_PER_RUN * 5)
    .get();

  if (snap.empty) return { requeued: 0, exhausted: 0, skippedAlive: 0, namesFilledFromSubject: 0 };

  let requeued     = 0;
  let exhausted    = 0;
  let skippedAlive = 0;
  let namesFilledFromSubject = 0;
  let processed    = 0;

  for (const threadSnap of snap.docs) {
    if (processed >= MAX_SCRAPE_REAP_PER_RUN) break;

    const thread          = threadSnap.data() || {};
    const threadId        = thread.threadId || threadSnap.id;
    const gmailMessageId  = thread.gmailMessageId;
    const conversationUrl = thread.etsyConversationUrl;

    // Step 1 — Fill customerName from subject if it's still "Unknown".
    // This runs BEFORE the grace-period check so newly-detected threads
    // get labeled instantly even within the grace window. Cheap (one tx,
    // no Etsy roundtrip), independent of the job-queue recovery below.
    // Even if the rest of this loop iteration bails out, the name is now
    // visible in the inbox.
    if (!thread.customerName || thread.customerName === "Unknown" || thread.customerName === "") {
      const fillResult = await tryFillCustomerNameFromSubject(threadSnap.ref, thread);
      if (fillResult.filled) {
        namesFilledFromSubject++;
        // Update the in-memory copy so the rest of this iteration sees
        // the new name (downstream branches read thread.customerName for
        // audit-payload purposes).
        thread.customerName = fillResult.name;
        thread.customerNameFromSubject = true;
      }
    }

    // Apply the grace-period cutoff client-side (see query comment).
    // We only consider threads created MORE than SCRAPE_DETECTED_GRACE_MS
    // ago — gives the extension first dibs on freshly-detected threads.
    const createdAtMs = thread.createdAt && thread.createdAt.toMillis
      ? thread.createdAt.toMillis() : 0;
    if (!createdAtMs || createdAtMs > graceCutoffMs) continue;

    processed++;

    // Without a conversation URL we have nothing to scrape. Tag and
    // skip — operator must investigate.
    if (!conversationUrl) {
      await threadSnap.ref.set({
        riskFlags: FV.arrayUnion("scrape_no_conversation_url"),
        updatedAt: FV.serverTimestamp()
      }, { merge: true });
      await writeAudit(threadId, null, "scrape_reaper_no_conversation_url", {},
        "system:reapers:gmailScrape");
      continue;
    }

    // Match the watcher's deterministic id so we don't create a parallel
    // job alongside a queued/claimed one we're not seeing yet.
    const jobId = gmailMessageId
      ? `gmail_${gmailMessageId}`
      : `rescrape_${threadId}_${Date.now()}`;
    const jobRef     = db.collection(JOBS_COLL).doc(jobId);
    const jobDocSnap = await jobRef.get();
    const jobData    = jobDocSnap.exists ? jobDocSnap.data() : null;

    if (isLiveScrapeJob(jobData)) {
      // Healthy job in flight — let it cook. Sub-sweep A handles stuck
      // claims separately.
      skippedAlive++;
      continue;
    }

    // Either no job exists or it's "failed". Decide based on attempts.
    const priorAttempts = jobData ? (jobData.attempts || 0) : 0;
    if (priorAttempts >= MAX_SCRAPE_ATTEMPTS) {
      exhausted++;
      // Tag the thread so the inbox UI can surface "scrape exhausted —
      // manual rescrape needed". Don't change status — the thread is
      // still legitimately at detected_from_gmail.
      await threadSnap.ref.set({
        riskFlags: FV.arrayUnion("scrape_exhausted"),
        updatedAt: FV.serverTimestamp()
      }, { merge: true });
      await writeAudit(threadId, null, "scrape_job_exhausted",
        { jobId, attempts: priorAttempts, reason: "detected_thread_max_attempts" },
        "system:reapers:gmailScrape"
      );
      continue;
    }

    // Re-enqueue. set with merge:false on the deterministic id resets
    // the doc cleanly — no leftover claimedBy/claimedAt from a prior
    // failed attempt.
    await jobRef.set({
      jobId,
      jobType : "scrape",
      status  : "queued",
      threadId,
      payload : {
        etsyConversationUrl: conversationUrl,
        source             : "reapers:gmailScrape",
        gmailMessageId     : gmailMessageId || null,
        gmailThreadId      : thread.gmailThreadId || null,
        rescrape           : true
      },
      attempts       : priorAttempts,   // preserve history; claim tx increments
      claimedBy      : null,
      claimedAt      : null,
      lastError      : jobData ? (jobData.lastError || null) : null,
      lastHeartbeatAt: null,
      result         : null,
      createdAt      : jobData && jobData.createdAt ? jobData.createdAt : FV.serverTimestamp(),
      updatedAt      : FV.serverTimestamp(),
      reapedAt       : FV.serverTimestamp()
    }, { merge: false });

    requeued++;
    await writeAudit(threadId, null, "scrape_job_reaped_detected_orphan",
      {
        jobId,
        priorJobStatus  : jobData ? jobData.status : "missing",
        priorAttempts,
        gmailMessageId  : gmailMessageId || null,
        conversationUrl
      },
      "system:reapers:gmailScrape"
    );
  }

  return { requeued, exhausted, skippedAlive, namesFilledFromSubject };
}

/**
 * Sub-sweep C — Successful scrape but customerName=="Unknown".
 *
 * Two paths, in order:
 *
 *   1. Subject-fill (cheap, no extension needed).
 *      Try to populate customerName from the email subject already on
 *      the thread. Etsy notification subjects always carry the buyer's
 *      name. If the fill succeeds, we're done — no rescrape is queued.
 *
 *   2. One-shot rescrape (only if subject-fill failed/skipped).
 *      The _unknownRetryAttempted flag is set transactionally BEFORE
 *      the job is enqueued, so two reaper invocations racing each
 *      other can never produce duplicate retries. After this single
 *      retry the thread either gets filled in by the rescrape, or it
 *      stays "Unknown" and the operator is on their own — we
 *      explicitly do NOT loop further automatic retries here (the
 *      user's call when wiring this up: "Yes — try once more, then
 *      leave alone").
 */
async function reapUnknownAfterScrape() {
  const cutoffMs = Date.now() - SCRAPE_UNKNOWN_GRACE_MS;

  // Firestore can't combine an `==` on customerName with a `not-in` on
  // status efficiently without a composite index. Query on the most
  // selective single field (customerName=="Unknown") and filter status
  // / grace / one-shot guard client-side. "Unknown" threads should be
  // rare in steady state, so the result set stays small even at scale.
  const snap = await db.collection(THREADS_COLL)
    .where("customerName", "==", "Unknown")
    .limit(MAX_SCRAPE_REAP_PER_RUN * 2)   // overfetch; will filter
    .get();

  if (snap.empty) return { retried: 0, skipped: 0, namesFilledFromSubject: 0 };

  let retried = 0;
  let skipped = 0;
  let namesFilledFromSubject = 0;

  for (const threadSnap of snap.docs) {
    if (retried >= MAX_SCRAPE_REAP_PER_RUN) break;

    const thread   = threadSnap.data() || {};
    const threadId = thread.threadId || threadSnap.id;

    // Subject-fill is the cheap, no-Etsy-roundtrip path and should run
    // for ANY thread still showing customerName="Unknown" — including
    // detected_from_gmail threads (sub-sweep B's primary domain). The
    // two passes are independently safe (the tryFillCustomerNameFromSubject
    // tx aborts if customerName is no longer "Unknown"), so running both
    // just gives us belt-and-suspenders coverage. The status filter
    // below only gates the rescrape branch, not the subject-fill branch.
    const fillResult = await tryFillCustomerNameFromSubject(threadSnap.ref, thread);
    if (fillResult.filled) {
      namesFilledFromSubject++;
      // Mark the one-shot guard so we don't reconsider this thread for
      // a rescrape on a future tick — the name is now correct.
      await threadSnap.ref.set({
        _unknownRetryAttempted   : true,
        _unknownRetryAttemptedAt : FV.serverTimestamp(),
        updatedAt                : FV.serverTimestamp()
      }, { merge: true });
      continue;
    }

    // Below this point we're considering the rescrape path, which is
    // the one-shot retry. Apply all the rescrape-specific guards:

    // One-shot guard. The transactional flip below is the authoritative
    // gate; this is just a fast-path skip for already-attempted threads
    // so we don't burn budget on transactions that would no-op.
    if (thread._unknownRetryAttempted === true) {
      skipped++;
      continue;
    }

    // Threads still at detected_from_gmail are sub-sweep B's job — let
    // it handle the rescrape there to keep the deterministic gmail_<id>
    // job-id semantics consistent.
    if (!SCRAPE_POST_STATUSES.includes(thread.status)) {
      skipped++;
      continue;
    }

    // Honor the post-scrape grace window so we don't race a snapshot
    // commit that's about to fill in customerName legitimately.
    const lastSyncedMs = thread.lastSyncedAt && thread.lastSyncedAt.toMillis
      ? thread.lastSyncedAt.toMillis() : 0;
    if (lastSyncedMs && lastSyncedMs > cutoffMs) {
      skipped++;
      continue;
    }

    const conversationUrl = thread.etsyConversationUrl;
    if (!conversationUrl) {
      // Can't rescrape without a URL — mark the guard so we stop
      // re-evaluating this thread on every reaper tick.
      await threadSnap.ref.set({
        _unknownRetryAttempted: true,
        riskFlags             : FV.arrayUnion("unknown_no_conversation_url"),
        updatedAt             : FV.serverTimestamp()
      }, { merge: true });
      skipped++;
      continue;
    }

    // Set the one-shot guard transactionally — if another reaper tick
    // got here first and already flipped it, abort without enqueueing.
    let didClaim = false;
    try {
      didClaim = await db.runTransaction(async (tx) => {
        const fresh = await tx.get(threadSnap.ref);
        if (!fresh.exists) return false;
        const data = fresh.data() || {};
        if (data._unknownRetryAttempted === true) return false;
        if (data.customerName !== "Unknown") return false;   // already filled in
        tx.update(threadSnap.ref, {
          _unknownRetryAttempted   : true,
          _unknownRetryAttemptedAt : FV.serverTimestamp(),
          updatedAt                : FV.serverTimestamp()
        });
        return true;
      });
    } catch (err) {
      console.warn(`[gmail-scrape-reaper] subsweep C guard tx failed for ${threadId}:`, err.message);
      continue;
    }

    if (!didClaim) {
      skipped++;
      continue;
    }

    // Fresh job id (unique per retry) so we don't collide with the
    // already-succeeded gmail_<msgId> doc — that doc's history stays
    // intact for the audit trail.
    const jobId = `rescrape_${threadId}_${Date.now()}`;
    await db.collection(JOBS_COLL).doc(jobId).set({
      jobId,
      jobType : "scrape",
      status  : "queued",
      threadId,
      payload : {
        etsyConversationUrl: conversationUrl,
        source             : "reapers:gmailScrape:unknown-retry",
        gmailMessageId     : thread.gmailMessageId || null,
        gmailThreadId      : thread.gmailThreadId  || null,
        rescrape           : true,
        reason             : "customerName=Unknown after first scrape"
      },
      attempts       : 0,
      claimedBy      : null,
      claimedAt      : null,
      lastError      : null,
      lastHeartbeatAt: null,
      result         : null,
      createdAt      : FV.serverTimestamp(),
      updatedAt      : FV.serverTimestamp(),
      reapedAt       : FV.serverTimestamp()
    }, { merge: false });

    retried++;
    await writeAudit(threadId, null, "scrape_job_reaped_unknown_retry",
      {
        jobId,
        priorStatus    : thread.status,
        gmailMessageId : thread.gmailMessageId || null,
        conversationUrl
      },
      "system:reapers:gmailScrape"
    );
  }

  return { retried, skipped, namesFilledFromSubject };
}

/**
 * Sub-sweep D — Unmangle JSON-escaped Unicode in stored thread fields.
 *
 * One-shot data-repair pass. The Chrome scraper had a bug where some
 * non-ASCII customer names + subjects arrived as literal `\uXXXX`
 * escape sequences instead of the actual character ("Caitríona" stored
 * as the 13-character string "Caitr\u00edona"). The snapshot endpoint
 * now decodes on ingest, but threads that were already created before
 * that fix landed still carry the mangled values.
 *
 * This sweep finds those threads and fixes them in place. It's
 * intentionally cheap: a single equality-free query is impossible
 * (Firestore can't filter on "string contains substring") so we walk
 * recently-active threads and only update the ones whose customerName
 * or subject contain literal `\uXXXX` escape sequences.
 *
 * Bounded by SCRAPE_UNMANGLE_BATCH so it doesn't hog the 30s function
 * budget. Repeated runs across reaper ticks gradually clean the whole
 * collection — once it's clean, every subsequent run is a no-op.
 */
async function reapMangledUnicodeFields() {
  // Walk recently-updated threads first — the operator is most likely
  // to be looking at those, so fixing them first gives the fastest
  // perceived improvement. We page through up to BATCH * 4 candidates
  // per tick and only update the ones that actually need it.
  const BATCH = MAX_SCRAPE_REAP_PER_RUN;
  const snap = await db.collection(THREADS_COLL)
    .orderBy("updatedAt", "desc")
    .limit(BATCH * 4)
    .get();

  if (snap.empty) return { fixed: 0, scanned: 0 };

  let fixed = 0;
  let scanned = 0;

  for (const threadSnap of snap.docs) {
    if (fixed >= BATCH) break;
    scanned++;
    const data = threadSnap.data() || {};

    // Check the two fields we know the scraper mangles. Skipping any
    // field that doesn't actually have a `\uXXXX` non-ASCII escape
    // means clean threads are skipped after a single property read —
    // very cheap.
    const nameMangled    = hasMangledEscapes(data.customerName);
    const subjectMangled = hasMangledEscapes(data.subject);
    const senderMangled  = hasMangledEscapes(data.lastSenderName);
    if (!nameMangled && !subjectMangled && !senderMangled) continue;

    const patch = {};
    if (nameMangled) {
      patch.customerName = unmangleEscapedUnicode(data.customerName);
    }
    if (subjectMangled) {
      patch.subject = unmangleEscapedUnicode(data.subject);
    }
    if (senderMangled) {
      patch.lastSenderName = unmangleEscapedUnicode(data.lastSenderName);
    }
    patch.updatedAt = FV.serverTimestamp();
    patch.unicodeUnmangledAt = FV.serverTimestamp();

    try {
      await threadSnap.ref.set(patch, { merge: true });
      fixed++;
      await writeAudit(
        threadSnap.id,
        null,
        "thread_unicode_unmangled",
        {
          fieldsRepaired: Object.keys(patch).filter(k => k !== "updatedAt" && k !== "unicodeUnmangledAt"),
          // Truncate originals to 80 chars so we don't bloat audit rows.
          originalCustomerName: nameMangled ? String(data.customerName).slice(0, 80) : null,
          originalSubject     : subjectMangled ? String(data.subject).slice(0, 80) : null
        },
        "system:reapers:gmailScrape"
      );
    } catch (err) {
      console.warn(`[gmail-scrape-reaper] subsweep D unmangle failed for ${threadSnap.id}:`, err.message);
    }
  }

  return { fixed, scanned };
}

/**
 * Top-level entry for the gmail_scrape pass. Runs four sub-sweeps in
 * order and returns a combined summary. Each sub-sweep wraps its own
 * try/catch so a partial failure on one doesn't block the others.
 */
async function runGmailScrapePass() {
  const tStart = Date.now();
  const summary = {
    pass                       : "gmail_scrape",
    stuckClaimsRequeued        : 0,
    stuckClaimsExhausted       : 0,
    detectedThreadsRequeued    : 0,
    detectedThreadsExhausted   : 0,
    detectedThreadsSkippedAlive: 0,
    detectedNamesFilledFromSubject: 0,
    unknownThreadsRetried      : 0,
    unknownThreadsSkipped      : 0,
    unknownNamesFilledFromSubject : 0,
    mangledUnicodeFixed        : 0,
    mangledUnicodeScanned      : 0,
    reaped                     : 0,    // for the consolidated totalReaped
    subErrors                  : [],
    durationMs                 : 0
  };

  try {
    const a = await reapStuckScrapeClaims();
    summary.stuckClaimsRequeued  = a.requeued;
    summary.stuckClaimsExhausted = a.exhausted;
  } catch (e) {
    summary.subErrors.push({ subsweep: "A_stuck_claims", error: e.message });
    console.error("[gmail-scrape-reaper] subsweep A failed:", e);
  }

  try {
    const b = await reapDetectedThreadsWithoutLiveJobs();
    summary.detectedThreadsRequeued        = b.requeued;
    summary.detectedThreadsExhausted       = b.exhausted;
    summary.detectedThreadsSkippedAlive    = b.skippedAlive;
    summary.detectedNamesFilledFromSubject = b.namesFilledFromSubject || 0;
  } catch (e) {
    summary.subErrors.push({ subsweep: "B_detected_orphans", error: e.message });
    console.error("[gmail-scrape-reaper] subsweep B failed:", e);
  }

  try {
    const c = await reapUnknownAfterScrape();
    summary.unknownThreadsRetried        = c.retried;
    summary.unknownThreadsSkipped        = c.skipped;
    summary.unknownNamesFilledFromSubject = c.namesFilledFromSubject || 0;
  } catch (e) {
    summary.subErrors.push({ subsweep: "C_unknown_retry", error: e.message });
    console.error("[gmail-scrape-reaper] subsweep C failed:", e);
  }

  try {
    const d = await reapMangledUnicodeFields();
    summary.mangledUnicodeFixed   = d.fixed;
    summary.mangledUnicodeScanned = d.scanned;
  } catch (e) {
    summary.subErrors.push({ subsweep: "D_unmangle", error: e.message });
    console.error("[gmail-scrape-reaper] subsweep D failed:", e);
  }

  // Roll up "reaped" for the handler's totalReaped tally. Counts every
  // action that produced a downstream effect: job requeued, job
  // exhausted-and-marked-failed, one-shot retry enqueued, customerName
  // backfilled from the email subject, OR a row repaired by the
  // unmangler. Does NOT count skipped-alive (healthy in-flight work).
  summary.reaped =
      summary.stuckClaimsRequeued
    + summary.stuckClaimsExhausted
    + summary.detectedThreadsRequeued
    + summary.detectedThreadsExhausted
    + summary.unknownThreadsRetried
    + summary.detectedNamesFilledFromSubject
    + summary.unknownNamesFilledFromSubject
    + summary.mangledUnicodeFixed;

  summary.durationMs = Date.now() - tStart;
  return summary;
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

  // Optional body: `{ op: "auto_pipeline" | "send_queue" | "sales_funnels" |
  //                       "gmail_scrape", force?: bool }`
  // for targeted manual sweeps. Default is to run all passes.
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
    if (!op || op === "gmail_scrape") {
      try { results.gmailScrape = await runGmailScrapePass(); }
      catch (e) { errors.push({ pass: "gmail_scrape", error: e.message }); console.error("gmailScrape pass:", e); }
    }

    const totalReaped =
        ((results.autoPipeline && results.autoPipeline.reaped) || 0)
      + ((results.sendQueue    && results.sendQueue.reaped)    || 0)
      + ((results.salesFunnels && results.salesFunnels.reaped) || 0)
      + ((results.gmailScrape  && results.gmailScrape.reaped)  || 0);

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
module.exports.runGmailScrapePass          = runGmailScrapePass;
module.exports.reapStaleClaim              = reapStaleClaim;
module.exports.reapStaleDraft              = reapStaleDraft;
module.exports.reapAbandonedSalesThread    = reapAbandonedSalesThread;
module.exports.reapStuckScrapeClaims       = reapStuckScrapeClaims;
module.exports.reapDetectedThreadsWithoutLiveJobs = reapDetectedThreadsWithoutLiveJobs;
module.exports.reapUnknownAfterScrape      = reapUnknownAfterScrape;
module.exports.tryFillCustomerNameFromSubject = tryFillCustomerNameFromSubject;
module.exports.extractCustomerNameFromSubject = extractCustomerNameFromSubject;
module.exports.reapMangledUnicodeFields    = reapMangledUnicodeFields;
module.exports.unmangleEscapedUnicode      = unmangleEscapedUnicode;
module.exports.hasMangledEscapes           = hasMangledEscapes;
module.exports.REAPABLE_STAGES             = Array.from(REAPABLE_STAGES);
module.exports.STALE_CLAIM_THRESHOLD_MS    = STALE_CLAIM_THRESHOLD_MS;
module.exports.SALES_SCAN_INTERVAL_MS      = SALES_SCAN_INTERVAL_MS;
module.exports.SCRAPE_STUCK_CLAIM_MS       = SCRAPE_STUCK_CLAIM_MS;
module.exports.SCRAPE_DETECTED_GRACE_MS    = SCRAPE_DETECTED_GRACE_MS;
module.exports.SCRAPE_UNKNOWN_GRACE_MS     = SCRAPE_UNKNOWN_GRACE_MS;
