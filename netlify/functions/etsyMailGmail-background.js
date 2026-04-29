/*  netlify/functions/etsyMailGmail-background.js
 *
 *  M6 Gmail-watcher — polls a Gmail inbox for Etsy notification emails,
 *  extracts the embedded Etsy conversation link, and enqueues a `scrape`
 *  job in EtsyMail_Jobs. The existing Chrome extension picks up that job
 *  on its next poll, opens the Etsy tab, scrapes the conversation, and
 *  POSTs the snapshot — closing the loop with ZERO changes to the
 *  scraper, the snapshot ingest, or the extension itself.
 *
 *  ═══ THE LOOP ═════════════════════════════════════════════════════════
 *
 *    Etsy → email → Gmail
 *                    │
 *                    ▼  this fn (every minute)
 *                etsyMailGmail-background
 *                    │  (1) extract conversation link from email body
 *                    │  (2) upsert EtsyMail_Threads doc with status
 *                    │      "detected_from_gmail" + gmailMessageId
 *                    │  (3) enqueue EtsyMail_Jobs doc { jobType: "scrape",
 *                    │      payload: { etsyConversationUrl } }
 *                    ▼
 *                EtsyMail_Jobs (queued)
 *                    │
 *                    ▼  Chrome extension polls every 20s
 *                background.js (extension)
 *                    │  claims job → opens tab → runs content scraper
 *                    ▼
 *                etsyMailSnapshot (existing endpoint)
 *                    │  advances detected_from_gmail → etsy_scraped
 *                    │  triggers etsyMailAutoPipeline-background
 *                    ▼
 *                EtsyMail_Threads (status: etsy_scraped, message stored)
 *                    │
 *                    ▼  auto-reply pipeline + inbox UI
 *
 *  Every step except this file already exists. Status advance, dedup
 *  (by contentHash), audit logging, image mirroring, AI draft, and
 *  send queue are all handled by the existing pipeline.
 *
 *  ═══ INVOCATION ═══════════════════════════════════════════════════════
 *
 *    Scheduled  : etsyMailGmailCron.js (every 1 minute)
 *    Manual     : etsyMailGmail.js?action=trigger
 *    Direct test: POST /.netlify/functions/etsyMailGmail-background
 *
 *  Optional body: { mode: "incremental" | "full", windowDays?: 7, query?: "..." }
 *
 *  ═══ ENV VARS ═════════════════════════════════════════════════════════
 *
 *    GMAIL_CLIENT_ID, GMAIL_CLIENT_SECRET   — required for token refresh
 *    GMAIL_QUERY (optional)                 — overrides the default Gmail
 *                                             search filter. Default:
 *                                             from:notify@etsy.com
 *    GMAIL_INITIAL_WINDOW_DAYS (optional)   — how far back to look on the
 *                                             very first run. Default 7.
 *
 *  ═══ STATE ═══════════════════════════════════════════════════════════
 *
 *  EtsyMail_Config/gmailSyncState  — single doc, fields:
 *    { lastSyncInProgress, lastSyncStartedAt, lastSyncCompletedAt,
 *      lastSyncMode, lastSyncMessagesScanned, lastSyncJobsEnqueued,
 *      lastSyncSkipped, lastSyncError, lastSyncErrorAt,
 *      lastInternalDateMs            // newest message's Gmail receive time
 *    }
 *
 *  Watermark is a Gmail-side internalDate (ms epoch). Each poll asks Gmail
 *  for `after:<seconds>` — Gmail's `after:` operator takes Unix seconds.
 *  We never look backwards through history, only forward from the
 *  watermark, so the cost-per-tick stays bounded regardless of mailbox
 *  size.
 */

"use strict";

const fetch = require("node-fetch");
const admin = require("./firebaseAdmin");
const {
  listMessages,
  getMessage,
  extractEtsyConversationLink,
  summarizeMessage
} = require("./_etsyMailGmail");

const db = admin.firestore();
const FV = admin.firestore.FieldValue;

// ─── Collections / config doc paths ──────────────────────────────────────
const THREADS_COLL = "EtsyMail_Threads";
const JOBS_COLL    = "EtsyMail_Jobs";
const AUDIT_COLL   = "EtsyMail_Audit";
const SYNC_STATE_DOC = "EtsyMail_Config/gmailSyncState";   // 2-segment path

// ─── Tuning constants ────────────────────────────────────────────────────

// Default Gmail search filter. Picks up Etsy's "new message" notification
// emails. Operators can override via GMAIL_QUERY env var if their inbox
// uses a label-based filter or a different sender list.
//
// NB: this is COMBINED with `after:<seconds>` from the watermark before
// being sent to Gmail. So the env var should NOT include an after: clause.
const DEFAULT_GMAIL_QUERY = "from:notify@etsy.com";

// On the very first run (no watermark in syncState), don't pull all-time
// history — limit to the last N days. Operators can override per-request
// or via env var. 7d is generous enough to catch backlog, conservative
// enough to avoid burning the 15-min budget on years of email.
const DEFAULT_INITIAL_WINDOW_DAYS = parseInt(
  process.env.GMAIL_INITIAL_WINDOW_DAYS || "7", 10
);

// Per-page Gmail listing size. 100 is the API's max.
const PAGE_SIZE = 100;

// Cap pages per invocation as defense-in-depth — a misconfigured query
// (e.g. accidentally `from:gmail.com`) shouldn't burn the budget.
const MAX_PAGES_PER_INVOCATION = 50;

// Stop 13 min into the 15-min envelope so we have time to flush state.
const MAX_INVOCATION_MS = 13 * 60 * 1000;

// Yield between Gmail message fetches so we don't trip the per-user
// concurrency limit (250 quota units/sec, getMessage costs 5 units).
const FETCH_DELAY_MS = 50;

// ─── Helpers ─────────────────────────────────────────────────────────────
const sleep = (ms) => new Promise(r => setTimeout(r, ms));

async function readSyncState() {
  const snap = await db.doc(SYNC_STATE_DOC).get();
  return snap.exists ? snap.data() : null;
}

async function writeSyncState(patch) {
  // serverTimestamp on updatedAt every time so observers see fresh state.
  await db.doc(SYNC_STATE_DOC).set(
    { ...patch, updatedAt: FV.serverTimestamp() },
    { merge: true }
  );
}

async function writeAudit({ threadId, eventType, actor, payload }) {
  try {
    await db.collection(AUDIT_COLL).add({
      threadId : threadId || null,
      draftId  : null,
      eventType,
      actor    : actor || "system:gmail-watcher",
      payload  : payload || {},
      createdAt: FV.serverTimestamp()
    });
  } catch (err) {
    // Audit failures are non-fatal — never let an audit write block the
    // happy path. Log and move on.
    console.warn("audit write failed:", err.message);
  }
}

/**
 * Build the Gmail search query for this invocation. Combines the static
 * filter (env var or default) with the watermark — Gmail's `after:`
 * operator wants Unix seconds.
 *
 * Returns { q, watermarkSec } so the caller can log + advance state.
 */
function buildQuery({ baseQuery, lastInternalDateMs, windowDays }) {
  let watermarkSec;
  if (lastInternalDateMs && lastInternalDateMs > 0) {
    // +1 second to skip the message at the boundary (we already processed it)
    watermarkSec = Math.floor(lastInternalDateMs / 1000) + 1;
  } else {
    // No watermark → look back windowDays (initial run / re-seed scenario)
    const days = windowDays > 0 ? windowDays : DEFAULT_INITIAL_WINDOW_DAYS;
    watermarkSec = Math.floor((Date.now() - days * 86400 * 1000) / 1000);
  }
  const q = `${baseQuery} after:${watermarkSec}`;
  return { q, watermarkSec };
}

// ─── Thread upsert ───────────────────────────────────────────────────────
//
// Pattern matches etsyMailThreads.js action:create — same field shape, so
// the inbox UI and downstream pipeline see exactly the same thread doc
// whether it was created by Gmail detection or the snapshot endpoint.
//
// Three cases:
//   1. Thread doesn't exist        → CREATE with status "detected_from_gmail"
//   2. Thread exists, no gmailId   → PATCH to attach gmail metadata
//   3. Thread exists, has gmailId  → no-op (already linked from a prior poll)
//
// We never advance status here. The snapshot endpoint owns status
// transitions ("detected_from_gmail" → "etsy_scraped"); leaving status
// alone on the upsert means the snapshot endpoint's existing state
// machine just works.

async function upsertThreadFromGmail({
  conversationId,
  conversationUrl,
  gmailMessageId,
  gmailThreadId,
  internalDateMs,
  customerName,
  customerEmail,
  subject
}) {
  const threadId = `etsy_conv_${conversationId}`;
  const ref = db.collection(THREADS_COLL).doc(threadId);
  const now = FV.serverTimestamp();
  const gmailReceivedAt = internalDateMs
    ? admin.firestore.Timestamp.fromMillis(internalDateMs)
    : null;

  const snap = await ref.get();

  if (!snap.exists) {
    // CREATE — mirror the field shape of etsyMailThreads.js action:create
    // so the inbox UI sees exactly what it expects. Initial status is
    // "detected_from_gmail" — the snapshot endpoint will advance to
    // "etsy_scraped" on first successful scrape (logic that already
    // exists in etsyMailSnapshot.js line 125).
    const initial = {
      threadId,
      etsyConversationId  : conversationId,
      etsyConversationUrl : conversationUrl,
      gmailMessageId,
      gmailThreadId,
      gmailReceivedAt,
      customerName        : customerName || "Unknown",
      customerEmail       : customerEmail || null,
      etsyUsername        : null,
      linkedOrderId       : null,
      linkedListingIds    : [],
      status              : "detected_from_gmail",
      category            : null,
      confidence          : null,
      needsHumanReview    : true,
      aiDraftStatus       : "none",
      latestDraftId       : null,
      lastInboundAt       : gmailReceivedAt,
      lastOutboundAt      : null,
      lastSyncedAt        : null,
      lastScrapedDomHash  : null,
      assignedTo          : null,
      tags                : [],
      riskFlags           : [],
      messageCount        : 0,
      unread              : true,
      lastReadAt          : null,
      subject             : subject || null,
      createdAt           : now,
      updatedAt           : now,
      // M3 buyer metadata fields — populated by snapshot's first scrape
      buyerUserId         : null,
      buyerPeopleUrl      : null,
      buyerAvatarUrl      : null,
      buyerIsRepeatBuyer  : false
    };
    await ref.set(initial, { merge: false });
    await writeAudit({
      threadId,
      eventType: "thread_created",
      actor    : "system:gmail",
      payload  : { source: "gmail", gmailMessageId, gmailThreadId, hasInitialText: false }
    });
    return { threadId, action: "created" };
  }

  // UPDATE path. If thread already has THIS exact gmailMessageId, skip —
  // we've already processed this email (idempotency guard for SW retries).
  const existing = snap.data() || {};
  if (existing.gmailMessageId === gmailMessageId) {
    return { threadId, action: "skipped_already_linked" };
  }

  // Patch only the gmail-related fields. Don't touch status, customerName,
  // etc. — the snapshot endpoint and operator UI may have refined those.
  const patch = {
    gmailMessageId,
    gmailThreadId,
    gmailReceivedAt,
    updatedAt: now
  };
  // Bring etsyConversationUrl up to date if missing (older threads created
  // before Gmail integration may have null URL).
  if (!existing.etsyConversationUrl) patch.etsyConversationUrl = conversationUrl;

  await ref.set(patch, { merge: true });
  await writeAudit({
    threadId,
    eventType: "thread_gmail_linked",
    actor    : "system:gmail",
    payload  : { gmailMessageId, gmailThreadId, previousGmailMessageId: existing.gmailMessageId || null }
  });
  return { threadId, action: "linked" };
}

// ─── Job enqueue ─────────────────────────────────────────────────────────
//
// Pattern matches the EtsyMail_Jobs schema consumed by etsyMailJobs.js
// (op:claim) — same fields, same status lifecycle, same payload shape.
// Deterministic doc id `gmail_<gmailMessageId>` makes re-enqueues from
// SW retries idempotent without needing a transaction.

async function enqueueScrapeJob({ threadId, conversationUrl, gmailMessageId, gmailThreadId }) {
  const jobId = `gmail_${gmailMessageId}`;
  const ref = db.collection(JOBS_COLL).doc(jobId);

  // Idempotency: if a job with this id already exists in any non-failed
  // state, skip. We only want to re-enqueue if the prior attempt failed
  // hard (status="failed" with no further retries).
  const existing = await ref.get();
  if (existing.exists) {
    const data = existing.data() || {};
    const skippableStatuses = ["queued", "claimed", "succeeded"];
    if (skippableStatuses.includes(data.status)) {
      return { jobId, action: "skipped_existing", existingStatus: data.status };
    }
    // status === "failed" → fall through and re-queue
  }

  await ref.set({
    jobId,
    jobType   : "scrape",
    status    : "queued",
    threadId  : threadId || null,
    payload   : {
      etsyConversationUrl: conversationUrl,
      source             : "gmail",
      gmailMessageId,
      gmailThreadId
    },
    attempts       : 0,
    claimedBy      : null,
    claimedAt      : null,
    lastError      : null,
    lastHeartbeatAt: null,
    result         : null,
    createdAt      : FV.serverTimestamp(),
    updatedAt      : FV.serverTimestamp()
  }, { merge: false });

  return { jobId, action: "enqueued" };
}

// ─── Main loop ───────────────────────────────────────────────────────────

async function runIncremental({ invocationStartMs, mode, query, windowDays }) {
  const state = await readSyncState();
  const lastInternalDateMs = state && state.lastInternalDateMs ? state.lastInternalDateMs : 0;

  // mode="full" wipes the watermark for this invocation only; the windowDays
  // cap still applies so we don't accidentally pull all-time history. Useful
  // when an operator has changed the GMAIL_QUERY filter and wants a backfill.
  const effectiveWatermark = mode === "full" ? 0 : lastInternalDateMs;

  const baseQuery = (query && query.trim()) || process.env.GMAIL_QUERY || DEFAULT_GMAIL_QUERY;
  const { q, watermarkSec } = buildQuery({
    baseQuery,
    lastInternalDateMs: effectiveWatermark,
    windowDays
  });

  console.log(`[gmail-watcher] running mode=${mode} q="${q}"`);

  let pageToken = null;
  let pagesFetched = 0;
  let messagesScanned = 0;
  let jobsEnqueued = 0;
  let threadsCreated = 0;
  let threadsLinked = 0;
  let skippedNoLink = 0;
  let skippedAlreadyProcessed = 0;
  let errors = 0;
  let newestInternalDateMs = lastInternalDateMs;

  // PAGINATE through Gmail until exhaustion or budget. We process each
  // message inside the same page loop so that if we hit the time cap the
  // watermark advances incrementally — next invocation picks up where
  // we left off without re-doing work.
  while (true) {
    if (Date.now() - invocationStartMs > MAX_INVOCATION_MS) {
      console.log("[gmail-watcher] hit invocation budget, stopping");
      break;
    }
    if (pagesFetched >= MAX_PAGES_PER_INVOCATION) {
      console.log("[gmail-watcher] hit MAX_PAGES_PER_INVOCATION, stopping");
      break;
    }

    let listResp;
    try {
      listResp = await listMessages({ q, pageToken, maxResults: PAGE_SIZE });
    } catch (err) {
      console.error("[gmail-watcher] listMessages failed:", err.message);
      errors++;
      throw err;   // fatal — let the outer handler record the error in state
    }
    pagesFetched++;

    const stubs = Array.isArray(listResp.messages) ? listResp.messages : [];
    if (stubs.length === 0 && pageToken == null) {
      // First page came back empty — nothing new since watermark
      console.log("[gmail-watcher] no new messages");
      break;
    }

    // Process messages oldest-first WITHIN the page so the watermark
    // advances monotonically. Gmail returns newest-first, so reverse.
    const orderedStubs = stubs.slice().reverse();

    for (const stub of orderedStubs) {
      if (Date.now() - invocationStartMs > MAX_INVOCATION_MS) break;
      messagesScanned++;

      let full;
      try {
        full = await getMessage(stub.id, { format: "full" });
      } catch (err) {
        console.warn(`[gmail-watcher] getMessage(${stub.id}) failed:`, err.message);
        errors++;
        continue;
      }

      const summary = summarizeMessage(full);

      // Track newest internalDate for watermark advance — even messages
      // we skip count, otherwise we'd reprocess them on every poll.
      if (summary.internalDateMs && summary.internalDateMs > newestInternalDateMs) {
        newestInternalDateMs = summary.internalDateMs;
      }

      // EXTRACT — find the Etsy conversation link in the body
      const link = extractEtsyConversationLink(full);
      if (!link || !link.conversationId) {
        skippedNoLink++;
        // Audit each miss so operators can spot if Etsy changes their
        // email format and we're silently dropping all of them.
        await writeAudit({
          threadId : null,
          eventType: "gmail_message_skipped_no_link",
          actor    : "system:gmail",
          payload  : {
            gmailMessageId : summary.gmailMessageId,
            subject        : summary.subject,
            from           : summary.from
          }
        });
        await sleep(FETCH_DELAY_MS);
        continue;
      }

      // UPSERT thread
      let upsertResult;
      try {
        upsertResult = await upsertThreadFromGmail({
          conversationId : link.conversationId,
          conversationUrl: link.conversationUrl,
          gmailMessageId : summary.gmailMessageId,
          gmailThreadId  : summary.gmailThreadId,
          internalDateMs : summary.internalDateMs,
          customerName   : null,                     // populated by scrape
          customerEmail  : null,                     // could parse "From:" if useful
          subject        : summary.subject
        });
      } catch (err) {
        console.warn(`[gmail-watcher] upsertThread failed for ${summary.gmailMessageId}:`, err.message);
        errors++;
        continue;
      }

      if (upsertResult.action === "created") threadsCreated++;
      else if (upsertResult.action === "linked") threadsLinked++;
      else if (upsertResult.action === "skipped_already_linked") {
        skippedAlreadyProcessed++;
        // Already linked — don't enqueue another scrape job. The thread
        // is already in the pipeline. Watermark still advances so we
        // don't re-fetch this message next tick.
        await sleep(FETCH_DELAY_MS);
        continue;
      }

      // ENQUEUE scrape job → existing extension picks this up next poll
      try {
        const jobResult = await enqueueScrapeJob({
          threadId       : upsertResult.threadId,
          conversationUrl: link.conversationUrl,
          gmailMessageId : summary.gmailMessageId,
          gmailThreadId  : summary.gmailThreadId
        });
        if (jobResult.action === "enqueued") {
          jobsEnqueued++;
          await writeAudit({
            threadId : upsertResult.threadId,
            eventType: "scrape_job_enqueued",
            actor    : "system:gmail",
            payload  : {
              jobId          : jobResult.jobId,
              gmailMessageId : summary.gmailMessageId,
              conversationUrl: link.conversationUrl
            }
          });
        }
      } catch (err) {
        console.warn(`[gmail-watcher] enqueueJob failed for ${summary.gmailMessageId}:`, err.message);
        errors++;
      }

      await sleep(FETCH_DELAY_MS);
    }

    pageToken = listResp.nextPageToken || null;
    if (!pageToken) break;
  }

  // Persist the new watermark + counters. Always write — even on a no-op
  // run, the lastSyncCompletedAt advance is what the cron uses to
  // throttle subsequent triggers.
  await writeSyncState({
    lastSyncMode             : mode,
    lastSyncStartedAt        : admin.firestore.Timestamp.fromMillis(invocationStartMs),
    lastSyncCompletedAt      : FV.serverTimestamp(),
    lastSyncDurationMs       : Date.now() - invocationStartMs,
    lastSyncMessagesScanned  : messagesScanned,
    lastSyncJobsEnqueued     : jobsEnqueued,
    lastSyncThreadsCreated   : threadsCreated,
    lastSyncThreadsLinked    : threadsLinked,
    lastSyncSkippedNoLink    : skippedNoLink,
    lastSyncSkippedAlreadyProcessed: skippedAlreadyProcessed,
    lastSyncErrors           : errors,
    lastSyncPagesFetched     : pagesFetched,
    lastSyncQuery            : q,
    lastSyncWatermarkSec     : watermarkSec,
    lastInternalDateMs       : newestInternalDateMs,
    lastSyncInProgress       : false,
    // Clear the prior error if this run succeeded
    ...(errors === 0 ? { lastSyncError: null, lastSyncErrorAt: null } : {})
  });

  return {
    messagesScanned,
    jobsEnqueued,
    threadsCreated,
    threadsLinked,
    skippedNoLink,
    skippedAlreadyProcessed,
    errors,
    pagesFetched,
    newestInternalDateMs
  };
}

// ─── Entry ───────────────────────────────────────────────────────────────
exports.handler = async (event) => {
  const invocationStartMs = Date.now();

  if (!process.env.GMAIL_CLIENT_ID || !process.env.GMAIL_CLIENT_SECRET) {
    console.error("[gmail-watcher] GMAIL_CLIENT_ID / GMAIL_CLIENT_SECRET not set");
    return { statusCode: 500, body: "Missing GMAIL_CLIENT_ID / GMAIL_CLIENT_SECRET env vars" };
  }

  // Parse params from body or query string. The cron passes nothing; the
  // status/trigger endpoint may pass mode/query/windowDays.
  let mode = "incremental";
  let query = null;
  let windowDays = null;
  try {
    if (event.body) {
      const b = JSON.parse(event.body);
      if (b.mode === "full") mode = "full";
      if (typeof b.query === "string") query = b.query;
      if (typeof b.windowDays === "number") windowDays = b.windowDays;
    }
    if (event.queryStringParameters) {
      const qs = event.queryStringParameters;
      if (qs.mode === "full") mode = "full";
      if (qs.query) query = qs.query;
      if (qs.windowDays) windowDays = parseInt(qs.windowDays, 10);
    }
  } catch {}

  try {
    await writeSyncState({
      lastSyncInProgress: true,
      lastSyncStartedAt : admin.firestore.Timestamp.fromMillis(invocationStartMs),
      lastSyncMode      : mode,
      lastSyncError     : null,
      lastSyncErrorAt   : null
    });

    const summary = await runIncremental({ invocationStartMs, mode, query, windowDays });

    console.log("[gmail-watcher] complete:", JSON.stringify(summary));
    return { statusCode: 200, body: JSON.stringify({ ok: true, ...summary }) };

  } catch (err) {
    console.error("[gmail-watcher] fatal:", err);
    try {
      await writeSyncState({
        lastSyncInProgress: false,
        lastSyncError     : err.message || String(err),
        lastSyncErrorAt   : FV.serverTimestamp()
      });
    } catch {}
    // Re-throw so Netlify marks the invocation as failed (visible in
    // function logs). The next cron tick will retry.
    throw err;
  }
};
