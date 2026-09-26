/*  netlify/functions/etsyMailListingCreatorCron.js
 *
 *  Scheduled trigger for the custom-listing-creator pipeline. Polls
 *  EtsyMail_Threads every minute looking for threads in the "queued"
 *  state, claims each via a Firestore transaction, and fires the
 *  background worker.
 *
 *  Pipeline state-machine (lives on the thread doc as customListingStatus):
 *
 *      [no value]
 *           │
 *           ▼  (sales agent on customer_accepted=true)
 *      "queued" ────────────────────────────────────────────┐
 *           │                                                │
 *           ▼  (this cron, atomic claim)                     │
 *      "creating"                                            │
 *           │                                                │
 *           ├──► "created"  (worker success — terminal)      │
 *           ├──► "failed"   (worker terminal failure;        │
 *           │                needsOperatorReview=true)       │
 *           └──► "queued"  ───────────────────────────────────┘
 *                          (worker retryable failure)
 *
 *  Why query on customListingStatus == "queued" instead of customerAccepted:
 *    customerAccepted is a *sticky* field — once the sales agent sets it,
 *    it stays true forever (the agent doesn't reset on subsequent turns).
 *    Querying on it means scanning every historical accepted thread on
 *    every cron tick, and once enough completed history accumulates the
 *    asc-ordered LIMIT-bound query starves the actual new threads at the
 *    back of the list. customListingStatus is owned end-to-end by this
 *    pipeline so it has clean states with no missing-vs-null ambiguity.
 *
 *  Cool-down:
 *    We wait 30 seconds after acceptance (customerAcceptedAt) before
 *    acting. Gives the customer a moment to retract; the sales agent
 *    can flip customerAccepted back to false on the next inbound (and
 *    the worker's loadThreadData re-checks this at fire time).
 *
 *  Stuck-flow recovery:
 *    A thread stuck in "creating" longer than RECOVERY_TIMEOUT_MS gets
 *    reclaimed by tryClaim — guards against the bg fn crashing without
 *    ever calling markFailure(). A separate "stuck sweep" query picks
 *    these up because they're not in "queued" anymore.
 *
 *  Source of truth: CUSTOM_LISTING_AUTOMATION_SPEC.md §3
 *                   (with the state-machine refinement noted in SETUP.md)
 */

"use strict";

const fetch = require("node-fetch");
const admin = require("./firebaseAdmin");
const { isScheduledInvocation } = require("./_etsyMailAuth");

const db = admin.firestore();
const FV = admin.firestore.FieldValue;

const THREADS_COLL = "EtsyMail_Threads";

const ACCEPTANCE_COOLDOWN_MS = 30 * 1000;        // 30s grace for customer retraction
const RECOVERY_TIMEOUT_MS    = 20 * 60 * 1000;   // re-claim "creating" if older than 20min.
                                                 // Netlify's bg fn hard ceiling is 15 minutes,
                                                 // so 20 guarantees we only reclaim after the
                                                 // worker is provably dead — never racing a
                                                 // still-running invocation (which would cause
                                                 // double-listing).
const MAX_PER_RUN            = 10;               // throttle: at most 10 listings per minute
const QUEUED_FETCH_SIZE      = 25;               // queued candidates per tick
const STUCK_FETCH_SIZE       = 10;               // stuck-recovery candidates per tick
const MAX_RETRYABLE_ERRORS   = 8;                // audit fix F7: then a person decides
const REPLY_CHECK_AFTER_MS   = 5 * 60 * 1000;    // audit fix F4: look at the link message after 5 min
const REPLY_STUCK_AFTER_MS   = 45 * 60 * 1000;   // ... and call it undelivered after 45 min in the queue
const REPLY_LEGACY_AFTER_MS  = 3 * 24 * 60 * 60 * 1000; // older records: mark once, quietly (no folder change)

function functionsBase() {
  return process.env.URL
      || process.env.DEPLOY_URL
      || process.env.NETLIFY_BASE_URL
      || "http://localhost:8888";
}

/** Atomically claim a thread for listing creation (or clean up stale state).
 *  Pre-condition: caller has already pre-filtered on a query that returned
 *  this doc as eligible (status="queued" or stuck "creating").
 *  Returns true if THIS caller now owns the slot, false if state moved.
 *  All checks are inside the transaction so concurrent crons (e.g. an
 *  overlapping run) can't both claim the same thread.
 *
 *  Cleanup side-effects (when the claim is rejected):
 *    - customListingId already set + status not "created"
 *        → flip status to "created" (re-acceptance edge: the agent flipped
 *          status back to "queued" but the listing already exists).
 *    - customerAccepted is now false + status was "queued" or "creating"
 *        → flip status to "retracted" so the cron stops surfacing this
 *          thread. (Customer pivoted before the cool-down expired or
 *          while the worker was running.) Without this, the thread sits
 *          in "queued" forever as cosmetic dead-weight in the dashboard. */
async function tryClaim(threadRef, opts = {}) {
  const { allowReclaimStuck = false } = opts;
  return db.runTransaction(async (tx) => {
    const snap = await tx.get(threadRef);
    if (!snap.exists) return false;
    const d = snap.data();
    const status = d.customListingStatus;

    // Cleanup case 1: re-acceptance after a fully-completed prior run.
    // The agent overwrote status to "queued" on a fresh acceptance turn,
    // but customListingStatus was already "created" — i.e., the listing
    // pipeline finished. Reset back to "created" so cron stops surfacing
    // this thread.
    //
    // NOTE: customListingId existence is NO LONGER a sufficient signal
    // for "already done" — v4.3 persists customListingId immediately
    // after createDraftListing, so it's set mid-flow. The reliable
    // terminal signal is customListingStatus === "created" OR the
    // thread-level status === "sales_completed".
    // Audit fix F19 — this cleanup used to sit AFTER the guard below, which
    // returns first for every sales_completed thread, so it never ran: such
    // a thread stayed "queued" for ever at the head of the FIFO query and
    // took one of the MAX_PER_RUN slots on every tick.
    if (status === "queued" && d.status === "sales_completed") {
      tx.update(threadRef, {
        customListingStatus: "created",
        updatedAt          : FV.serverTimestamp()
      });
      return false;
    }
    if (status === "created" || d.status === "sales_completed") {
      // shouldn't happen — cron query filters on status="queued" — but
      // defense in depth.
      return false;
    }

    // Cleanup case 2: customer retracted between agent's "queued" write
    // and now. v4.3 — with the durable-acceptance fix in the sales agent,
    // customerAccepted no longer auto-flips false on follow-up turns, so
    // the only way to get here is an OPERATOR manually flipping
    // customerAccepted=false in the dashboard (a legitimate intervention).
    // We honor that and clean up the queue.
    if (!d.customerAccepted) {
      if (status === "queued" || status === "creating") {
        tx.update(threadRef, {
          customListingStatus     : "retracted",
          customListingRetractedAt: FV.serverTimestamp(),
          updatedAt               : FV.serverTimestamp()
        });
      }
      return false;
    }

    // Standard / stuck-recovery claim paths.
    if (status === "queued") {
      // v4.3 — RACE PROTECTION. The sales-agent's durable-acceptance fix
      // (v4.3, SalesAgent §4.3) leaves customerAccepted alone on
      // non-acceptance turns, but a customer who *re-affirms* acceptance
      // mid-flow ("yes please proceed!") is a legitimate acceptance turn,
      // and the agent will write customListingStatus="queued" on top of
      // the "creating" state set by the cron's prior claim. Without this
      // freshness check, the cron's next tick would see status="queued"
      // and fire a SECOND worker while the first is still running,
      // causing a duplicate listing.
      //
      // markFailure clears customListingStartedAt with FV.delete() on
      // exit, so a fresh startedAt unambiguously means "worker still
      // in-flight". An old startedAt + status=queued means failed-
      // retryable (markFailure ran), and we want to claim immediately.
      const startedAt = d.customListingStartedAt;
      const startedMs = (startedAt && startedAt.toMillis) ? startedAt.toMillis() : 0;
      if (startedMs && (Date.now() - startedMs) < RECOVERY_TIMEOUT_MS) {
        return false;   // worker still in-flight — let it finish
      }
      // Audit fix F7 — back off after retryable failures (1, 2, 4, 8,
      // then every 15 min) and hand over to a person after 8, instead of
      // re-running Etsy + Claude calls every minute.
      const errCount = Number(d.customListingErrorCount || 0);
      const errAtMs  = (d.customListingErrorAt && d.customListingErrorAt.toMillis) ? d.customListingErrorAt.toMillis() : 0;
      if (errCount >= MAX_RETRYABLE_ERRORS) {
        tx.update(threadRef, {
          customListingStatus      : "failed",
          needsOperatorReview      : true,
          needsOperatorReviewReason: `listing_creation_failed: gave up after ${errCount} attempts — ${String(d.customListingError || "unknown error").slice(0, 200)}`,
          updatedAt                : FV.serverTimestamp()
        });
        return false;
      }
      if (errCount > 0 && errAtMs) {
        const waitMs = Math.min(15, Math.pow(2, errCount - 1)) * 60 * 1000;
        if (Date.now() - errAtMs < waitMs) return false;   // still backing off
      }
      // Fall through to claim (fresh queue OR retryable failure).
    } else if (status === "creating" && allowReclaimStuck) {
      // Stuck-flow recovery: only reclaim if the prior attempt has clearly
      // wedged. The cron writes customListingStartedAt at every claim, so
      // an old timestamp = no progress for at least RECOVERY_TIMEOUT_MS.
      // The 20-min threshold is past Netlify's 15-min bg fn hard ceiling,
      // so we only reclaim threads whose worker is provably dead — never
      // racing a still-running invocation.
      const startedAt = d.customListingStartedAt;
      const startedMs = (startedAt && startedAt.toMillis) ? startedAt.toMillis() : 0;
      const ageMs     = Date.now() - startedMs;
      if (ageMs < RECOVERY_TIMEOUT_MS) return false;
      // Audit fix F5 — a "Custom listing" button run also sets "creating"
      // but stamps customListingManualRequestedAt, not customListingStartedAt.
      // On a finished thread the OLD startedAt made it look stuck, and the
      // automatic worker then marked it "created" mid-run.
      const manualAt = d.customListingManualRequestedAt;
      const manualMs = (manualAt && manualAt.toMillis) ? manualAt.toMillis() : 0;
      if (manualMs && (Date.now() - manualMs) < RECOVERY_TIMEOUT_MS) return false;
      // else: stuck — fall through and reclaim. The worker's resume logic
      // (v4.3) will pick up where the prior attempt left off using the
      // mid-flow persistence markers (customListingId, customListingImagesAt).
    } else {
      // "created" / "failed" / "retracted" / unset — not eligible.
      return false;
    }

    tx.update(threadRef, {
      customListingStatus    : "creating",
      customListingStartedAt : FV.serverTimestamp(),
      customListingAttempts  : FV.increment(1),
      updatedAt              : FV.serverTimestamp()
    });
    return true;
  });
}

async function fireBackgroundWorker(threadId) {
  const url = `${functionsBase()}/.netlify/functions/etsyMailListingCreator-background`;
  // Fire-and-forget: Netlify returns 202 immediately for -background fns.
  // We don't await the actual work — the bg fn writes its own state.
  try {
    const res = await fetch(url, {
      method : "POST",
      headers: {
        "Content-Type": "application/json",
        ...(process.env.ETSYMAIL_EXTENSION_SECRET
          ? { "X-EtsyMail-Secret": process.env.ETSYMAIL_EXTENSION_SECRET }
          : {})
      },
      body: JSON.stringify({ threadId })
    });
    if (res.status !== 202 && !res.ok) {
      const text = await res.text().catch(() => "");
      console.error(`[listingCreatorCron] bg invoke ${threadId} returned ${res.status}: ${text.slice(0, 200)}`);
    }
  } catch (e) {
    console.error(`[listingCreatorCron] bg invoke ${threadId} failed:`, e.message);
    // Don't bubble — the next cron tick will retry via the recovery timeout
    // (the claim we just made will look "stuck" after RECOVERY_TIMEOUT_MS).
  }
}

/** Standard pull: threads in "queued" state past the cool-down.
 *  Uses index (customListingStatus ASC, customerAcceptedAt ASC). */
async function fetchQueued() {
  const cutoffTs = admin.firestore.Timestamp.fromMillis(Date.now() - ACCEPTANCE_COOLDOWN_MS);
  return db.collection(THREADS_COLL)
    .where("customListingStatus", "==", "queued")
    .where("customerAcceptedAt",  "<=", cutoffTs)
    .orderBy("customerAcceptedAt", "asc")   // FIFO: oldest queued first
    .limit(QUEUED_FETCH_SIZE)
    .get();
}

/** Stuck-flow sweep: threads stuck in "creating" past the recovery timeout.
 *  Uses index (customListingStatus ASC, customListingStartedAt ASC). */
async function fetchStuck() {
  const stuckCutoff = admin.firestore.Timestamp.fromMillis(Date.now() - RECOVERY_TIMEOUT_MS);
  return db.collection(THREADS_COLL)
    .where("customListingStatus",    "==", "creating")
    .where("customListingStartedAt", "<=", stuckCutoff)
    .orderBy("customListingStartedAt", "asc")
    .limit(STUCK_FETCH_SIZE)
    .get();
}

/** Audit fix F4 — the listing-link message is enqueued by the worker and then
 *  nobody records whether it reached the customer (customListingReplyStatus
 *  stays "queued" forever). Settle it from the draft slot: sent -> "sent",
 *  failed/replaced/stuck -> "failed" + Needs Review so a person re-sends the
 *  link. Firestore only (one equality query + one draft read per thread);
 *  never touches Etsy. */
async function settleListingReplies() {
  const out = { checked: 0, sent: 0, failed: 0 };
  const snap = await db.collection(THREADS_COLL)
    .where("customListingReplyStatus", "==", "queued")
    .limit(20)
    .get();
  const tsMs = v => (v && v.toMillis) ? v.toMillis() : 0;
  for (const doc of snap.docs) {
    const t = doc.data() || {};
    // Older records may lack customListingReplyQueuedAt; fall back to the
    // other timestamps the worker writes, so no record can sit in this
    // query forever and starve newer ones.
    const queuedMs = tsMs(t.customListingReplyQueuedAt) || tsMs(t.customListingReplyEnqueuedAt) ||
                     tsMs(t.customListingSentAt) || tsMs(t.customListingCreatedAt);
    if (queuedMs && Date.now() - queuedMs < REPLY_CHECK_AFTER_MS) continue;
    if (!queuedMs || Date.now() - queuedMs > REPLY_LEGACY_AFTER_MS) {
      // Records from before this check existed: take them out of the query
      // without moving the thread or bumping updatedAt (no rail re-sort).
      await doc.ref.set({ customListingReplyStatus: "unverified_legacy" }, { merge: true });
      continue;
    }
    out.checked++;
    const url = String(t.customListingUrl || "");
    const draftId = t.customListingReplyDraftId || ("draft_" + doc.id);
    const ds = await db.collection("EtsyMail_Drafts").doc(draftId).get();
    const d = ds.exists ? ds.data() : null;
    const norm = s => String(s || "").replace(/\s+/g, " ").trim();
    const ours = !!d && !!url && norm(d.text).includes(url);
    let outcome = null, why = null;
    if (ours && ["sent", "sent_text_only", "sent_unverified"].includes(d.status)) outcome = d.status === "sent_unverified" ? "sent_unverified" : "sent";
    else if (ours && d.status === "failed") { outcome = "failed"; why = d.sendErrorCode || "SEND_FAILED"; }
    else if (!ours) {
      // The one-per-thread draft slot was reused (a later AI or operator
      // reply). Before calling the link lost, look for it among the latest
      // messages scraped from Etsy. Optimistic copies are written at
      // enqueue time, so they prove nothing and are ignored.
      let seen = false;
      if (url) {
        const ms = await db.collection(THREADS_COLL).doc(doc.id).collection("messages")
          .orderBy("timestamp", "desc").limit(30).get();
        seen = ms.docs.some(m => {
          const x = m.data() || {};
          return x.direction === "outbound" && !x.localOptimistic && String(x.text || "").includes(url);
        });
      }
      if (seen) outcome = "sent";
      else { outcome = "failed"; why = d ? "REPLACED_BEFORE_SEND" : "DRAFT_MISSING"; }
    }
    else if (Date.now() - queuedMs > REPLY_STUCK_AFTER_MS) { outcome = "failed"; why = "STILL_" + String(d.status || "unknown").toUpperCase(); }
    if (!outcome) continue;
    const patch = { customListingReplyStatus: outcome, customListingReplySettledAt: FV.serverTimestamp(), updatedAt: FV.serverTimestamp() };
    if (outcome === "failed") {
      Object.assign(patch, {
        customListingReplyError  : why,
        status                   : "pending_human_review",
        needsOperatorReview      : true,
        needsOperatorReviewReason: `listing_link_not_delivered: ${why} — check the conversation and re-send ${url || "the listing link"} if it is missing`
      });
      out.failed++;
    } else out.sent++;
    await doc.ref.set(patch, { merge: true });
    await db.collection("EtsyMail_Audit").add({
      threadId: doc.id, draftId, eventType: outcome === "failed" ? "custom_listing_link_not_delivered" : "custom_listing_link_delivered",
      actor: "system:listingCreatorCron", payload: { outcome, why, draftStatus: d ? d.status : null }, createdAt: FV.serverTimestamp()
    });
  }
  return out;
}

exports.handler = async function (event) {
  const tStart = Date.now();
  const isScheduled = isScheduledInvocation(event || {});

  try {
    // Run both queries in parallel so a single tick can pick up new work
    // AND recover anything stuck from a prior crash.
    const [queuedSnap, stuckSnap] = await Promise.all([
      fetchQueued(),
      fetchStuck().catch(e => {
        // Stuck sweep failure is non-fatal — the queued path still runs.
        console.warn("[listingCreatorCron] stuck sweep failed:", e.message);
        return { docs: [], size: 0, empty: true };
      })
    ]);

    // Audit fix F4 — settle listing-link deliveries (Firestore only, non-fatal).
    const replies = await settleListingReplies().catch(e => {
      console.warn("[listingCreatorCron] reply settle failed:", e.message);
      return null;
    });
    if (replies && (replies.sent || replies.failed)) console.log("[listingCreatorCron] listing links:", JSON.stringify(replies));

    const queuedDocs = queuedSnap.empty ? [] : queuedSnap.docs;
    const stuckDocs  = stuckSnap.empty  ? [] : stuckSnap.docs;

    // Merge + dedupe (a thread shouldn't be in both, but defense in depth).
    const seen = new Set();
    const merged = [];
    for (const d of [...queuedDocs, ...stuckDocs]) {
      if (seen.has(d.id)) continue;
      seen.add(d.id);
      merged.push(d);
      if (merged.length >= MAX_PER_RUN) break;
    }

    if (!merged.length) {
      return {
        statusCode: 200,
        body: JSON.stringify({
          ok: true, queued: queuedDocs.length, stuck: stuckDocs.length,
          claimed: 0, elapsedMs: Date.now() - tStart, scheduled: isScheduled
        })
      };
    }

    // Claim + invoke each, in parallel. Each step is independent.
    const results = await Promise.all(merged.map(async (doc) => {
      const threadId  = doc.id;
      const isStuck   = doc.data().customListingStatus === "creating";
      try {
        const claimed = await tryClaim(doc.ref, { allowReclaimStuck: isStuck });
        if (!claimed) return { threadId, claimed: false, reason: isStuck ? "no-longer-stuck" : "no-longer-queued" };
        await fireBackgroundWorker(threadId);
        return { threadId, claimed: true, fired: true, recovered: isStuck };
      } catch (e) {
        console.error(`[listingCreatorCron] error processing ${threadId}:`, e.message);
        return { threadId, claimed: false, error: e.message };
      }
    }));

    const claimed   = results.filter(r => r.claimed).length;
    const recovered = results.filter(r => r.recovered).length;
    const errored   = results.filter(r => r.error).length;

    console.log(
      `[listingCreatorCron] queued=${queuedDocs.length} stuck=${stuckDocs.length} ` +
      `claimed=${claimed} recovered=${recovered} errored=${errored} elapsedMs=${Date.now() - tStart}`
    );

    return {
      statusCode: 200,
      body: JSON.stringify({
        ok: true,
        queued    : queuedDocs.length,
        stuck     : stuckDocs.length,
        claimed,
        recovered,
        errored,
        elapsedMs : Date.now() - tStart,
        scheduled : isScheduled,
        results
      })
    };

  } catch (err) {
    console.error("[listingCreatorCron] fatal:", err);
    return {
      statusCode: 500,
      body: JSON.stringify({ ok: false, error: err.message, elapsedMs: Date.now() - tStart })
    };
  }
};

// In-file schedule (works alongside or instead of netlify.toml).
exports.config = {
  schedule: "* * * * *"   // every minute
};
