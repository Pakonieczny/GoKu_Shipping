/*  netlify/functions/etsyMailThreads.js
 *
 *  Thread CRUD for the EtsyMail automation system.
 *  Mirrors the style of firebaseOrders.js: Admin SDK, CORS preamble, single
 *  handler that dispatches on method + query params.
 *
 *  Collections (see FIRESTORE_SCHEMA.md):
 *    EtsyMail_Threads/{threadId}
 *    EtsyMail_Threads/{threadId}/messages/{messageId}
 *    EtsyMail_Audit/{eventId}
 *    EtsyMail_Jobs/{jobId}
 *
 *  Supported operations (Milestone 1):
 *    GET  ?list=1&status=...&limit=...      → list threads
 *    GET  ?threadId=...                     → fetch single thread + messages
 *    GET  ?counts=1                         → left-rail counts by status
 *    POST body:{ action:'create', ... }     → create new thread (manual or Gmail)
 *    POST body:{ action:'patch',  threadId, fields } → partial update
 *    POST body:{ action:'appendMessage', threadId, message } → append to messages subcollection
 *    POST body:{ action:'markRead', threadId } → sets lastReadAt, unread=false
 *    POST body:{ action:'setStatus', threadId, status, reason? } → state transition + audit
 *    POST body:{ action:'enqueueJob', threadId, jobType, payload } → write EtsyMail_Jobs doc
 */

const admin = require("./firebaseAdmin");
const { requireExtensionAuth } = require("./_etsyMailAuth");
const db    = admin.firestore();
const FV    = admin.firestore.FieldValue;

const THREADS_COLL  = "EtsyMail_Threads";
const AUDIT_COLL    = "EtsyMail_Audit";
const JOBS_COLL     = "EtsyMail_Jobs";

// v1.2: include X-EtsyMail-Secret in allowed headers (CORS preflight)
// because the handler now enforces requireExtensionAuth. The inbox UI
// forwards the secret on every api() call; the snapshot/extension
// forwards it from env. Calls without the header now 401.
const CORS = {
  "Access-Control-Allow-Origin" : "*",
  "Access-Control-Allow-Headers": "Content-Type,Authorization,X-EtsyMail-Secret",
  "Access-Control-Allow-Methods": "GET,POST,OPTIONS"
};

const VALID_STATUSES = new Set([
  "detected_from_gmail",
  "pending_etsy_scrape",
  "etsy_scraped",
  "pending_order_enrichment",
  "ready_for_ai",
  "draft_ready",
  "pending_human_review",
  "approved_for_send",
  "auto_send_eligible",
  "queued_for_auto_send",    // v1.2 — AI passed all gates, draft enqueued, awaiting Etsy send confirmation
  "auto_replied",            // v1.0 — AI auto-sent AND Etsy confirmed delivery
  "send_in_progress",
  "sent",
  "hold_uncertain",          // legacy — retained for backward compat
  "hold_missing_order",      // legacy
  "hold_login_required",     // legacy
  "failed_scrape",
  "failed_send",
  // v4.3 — sales-agent lifecycle. The agent already writes "sales_active"
  // (line 1538) and treats "sales_completed" / "sales_abandoned" as
  // terminal (TERMINAL_THREAD_STATUSES). Those statuses were missing from
  // VALID_STATUSES, which prevented the dashboard from filtering on them
  // (the ?list=1&status=... endpoint validates against this set). The
  // dashboard's "Completed Sales" menu queries status=sales_completed.
  "sales_active",
  "sales_completed",
  "sales_abandoned",
  "archived"
]);

// ─── v2.4: Search support (folded in from former etsyMailSearch.js) ─────
//
// Each thread doc carries a denormalized `searchableText` field populated
// by etsyMailSnapshot.js: lowercased, normalized concatenation of customer
// name / Etsy username / subject / linked order id, plus the most recent
// ~6KB of message body. We load the most recent N threads ordered by
// updatedAt desc, run a substring match in memory, return same shape as
// firestoreProxy `op:list` so the UI can drop them into existing render
// path with no changes.
//
// At 500 threads × 6KB = ~3MB transferred per search. In-memory cache
// (15s TTL, keyed by query+limit+status) absorbs rapid keystrokes from
// the inbox UI's debounced search input. When the inbox grows beyond
// ~5K threads, swap for Algolia / Typesense / Meilisearch.

// Cap how many threads we'll lazy-backfill per request. Each backfill
// is one subcollection read + one write — too many in parallel trips
// Firestore quota / function timeout.
const MAX_BACKFILLS_PER_REQUEST = 50;

// Same normalizer the snapshot uses, kept in sync. Lowercase + collapse
// runs of whitespace + trim.
function normalizeSearchText(text = "") {
  return String(text).toLowerCase().replace(/\s+/g, " ").trim();
}

const _searchCache = new Map();
const SEARCH_CACHE_TTL_MS = 15 * 1000;
const MAX_CACHE_ENTRIES = 100;

/* Convert Firestore doc data to JSON-safe form, turning Timestamps into
 * {_ts: true, ms: <millis>} markers — same shape firestoreProxy uses, so
 * the inbox doesn't need a separate code path for search results. */
function serializeForSearch(value) {
  if (value === null || typeof value !== "object") return value;
  if (value && typeof value.toDate === "function" && typeof value.toMillis === "function") {
    return { _ts: true, ms: value.toMillis() };
  }
  if (Array.isArray(value)) return value.map(serializeForSearch);
  const out = {};
  for (const k of Object.keys(value)) out[k] = serializeForSearch(value[k]);
  return out;
}

/** Trim the heaviest internal-only field from search results. v1.6: keep
 *  `searchableText` so the UI can run further per-keystroke local
 *  filtering on the result set without an extra round trip; only drop
 *  the larger raw `searchableMessageText`. */
function trimSearchResultDoc(data) {
  const { searchableMessageText, ...rest } = data;
  return rest;
}

function gcSearchCache() {
  if (_searchCache.size <= MAX_CACHE_ENTRIES) return;
  const cutoff = Date.now() - SEARCH_CACHE_TTL_MS;
  for (const [k, v] of _searchCache.entries()) {
    if (v.at < cutoff) _searchCache.delete(k);
  }
  while (_searchCache.size > MAX_CACHE_ENTRIES) {
    const oldest = _searchCache.keys().next().value;
    _searchCache.delete(oldest);
  }
}

/** Run the full-text search. Extracted so the GET handler can dispatch
 *  to it on `?search=1`. Returns the same response shape as the former
 *  etsyMailSearch endpoint. */
async function runThreadSearch({ q, limit, statusList }) {
  const cacheKey = q + "|" + limit + "|" + statusList.sort().join(",");
  const cached = _searchCache.get(cacheKey);
  if (cached && (Date.now() - cached.at) < SEARCH_CACHE_TTL_MS) {
    return {
      docs   : cached.docs,
      q,
      count  : cached.docs.length,
      scanned: cached.scanned,
      cached : true
    };
  }

  // Query strategy mirrors fetchThreadListNow's composite-index avoidance:
  //   - status-filtered: single-field where, no orderBy (auto-index)
  //   - unfiltered:      orderBy by updatedAt (single-field auto-index)
  // Sort happens client-side anyway.
  let firestoreQuery = db.collection(THREADS_COLL);
  if (statusList.length === 1) {
    firestoreQuery = firestoreQuery.where("status", "==", statusList[0]).limit(limit);
  } else if (statusList.length > 1) {
    // Firestore `in` supports up to 10 values.
    firestoreQuery = firestoreQuery.where("status", "in", statusList).limit(limit);
  } else {
    firestoreQuery = firestoreQuery.orderBy("updatedAt", "desc").limit(limit);
  }

  const snap = await firestoreQuery.get();

  const matches = [];
  let backfilled = 0;
  const backfillPromises = [];

  snap.forEach(doc => {
    const data = doc.data() || {};
    const haystack = (data.searchableText || "").toLowerCase();

    if (haystack) {
      // Fast path: searchableText already populated.
      if (haystack.includes(q)) {
        matches.push({ id: doc.id, ...serializeForSearch(trimSearchResultDoc(data)) });
      }
      return;
    }

    // Lazy backfill for threads scraped before searchableText was
    // populated (or threads with no new messages since). Also surface
    // metadata-only matches immediately so the user sees something
    // even before the body text is read.
    const metaOnly = [
      data.customerName, data.etsyUsername, data.subject, data.linkedOrderId
    ].some(v => v && String(v).toLowerCase().includes(q));
    if (metaOnly) {
      matches.push({ id: doc.id, ...serializeForSearch(trimSearchResultDoc(data)) });
    }

    if (backfilled >= MAX_BACKFILLS_PER_REQUEST) return;
    backfilled++;

    backfillPromises.push((async () => {
      try {
        const msgsSnap = await doc.ref.collection("messages")
          .orderBy("timestamp", "desc")
          .limit(50)
          .get();
        const allText = msgsSnap.docs
          .map(d => normalizeSearchText((d.data() || {}).text || ""))
          .filter(Boolean)
          .reverse()
          .join(" ");
        const SEARCHABLE_MAX = 6000;
        const truncated = allText.length > SEARCHABLE_MAX
          ? allText.slice(allText.length - SEARCHABLE_MAX)
          : allText;
        const meta = [
          data.customerName, data.etsyUsername, data.subject, data.linkedOrderId
        ].map(s => normalizeSearchText(String(s || ""))).filter(Boolean).join(" ");
        const searchableText = (meta + " " + truncated).trim();

        await doc.ref.set({
          searchableText,
          searchableMessageText: truncated
        }, { merge: true });

        if (searchableText.includes(q) && !metaOnly) {
          const enriched = { ...data, searchableText, searchableMessageText: truncated };
          matches.push({ id: doc.id, ...serializeForSearch(trimSearchResultDoc(enriched)) });
        }
      } catch (e) {
        console.warn("etsyMailThreads search: backfill failed for", doc.id, "—", e.message);
      }
    })());
  });

  if (backfillPromises.length > 0) {
    await Promise.all(backfillPromises);
  }

  _searchCache.set(cacheKey, { docs: matches, at: Date.now(), scanned: snap.size });
  gcSearchCache();

  return {
    docs    : matches,
    q,
    count   : matches.length,
    scanned : snap.size,
    backfilled,
    cached  : false,
    maxedOut: snap.size >= limit
  };
}

function json(statusCode, body) {
  return { statusCode, headers: CORS, body: JSON.stringify(body) };
}
function bad(msg, code = 400) { return json(code, { error: msg }); }
function ok(body)             { return json(200, { success: true, ...body }); }

async function writeAudit({ threadId = null, draftId = null, eventType, actor = "system:api", payload = {} }) {
  await db.collection(AUDIT_COLL).add({
    threadId, draftId, eventType, actor, payload,
    createdAt: FV.serverTimestamp()
  });
}

exports.handler = async (event) => {
  if (event.httpMethod === "OPTIONS") {
    return { statusCode: 200, headers: CORS, body: "ok" };
  }

  // v1.2: gate every op behind the shared secret. Same approach as
  // etsyMailDraftSend. Inbox forwards the secret from localStorage on
  // every api() call. If env is unset, requireExtensionAuth allows
  // through (dev mode) and logs a loud warning.
  const auth = requireExtensionAuth(event);
  if (!auth.ok) return auth.response;

  try {
    const method = event.httpMethod;
    const qs     = event.queryStringParameters || {};

    /* ──────────────────────────── GET ──────────────────────────── */
    if (method === "GET") {

      /* ?search=1&q=...&limit=...&status=...
       * Full-text search across threads. v2.4: folded in from former
       * etsyMailSearch.js. The standalone /etsyMailSearch endpoint is
       * preserved as a thin shim that delegates back to this same
       * handler — see etsyMailSearch.js. */
      if (qs.search === "1") {
        const q = String(qs.q || "").trim().toLowerCase();
        const limit = Math.min(Math.max(parseInt(qs.limit || "500", 10), 1), 2000);
        const statusRaw = qs.status ? String(qs.status).trim() : "";
        const statusList = statusRaw
          ? statusRaw.split(",").map(s => s.trim()).filter(Boolean).slice(0, 10)  // Firestore `in` cap
          : [];

        // Guard: queries shorter than 2 chars would scan everything for
        // "a". The inbox UI also debounces and only fires for q.length>=2,
        // but defense in depth.
        if (q.length < 2) {
          return ok({ docs: [], q, count: 0, scanned: 0 });
        }

        try {
          const result = await runThreadSearch({ q, limit, statusList });
          return ok(result);
        } catch (err) {
          console.error("threads search error:", err);
          return json(500, { error: err.message || String(err) });
        }
      }

      /* ?counts=1 → counts by status (for left-rail badges) */
      if (qs.counts === "1") {
        const snap   = await db.collection(THREADS_COLL).select("status").get();
        const counts = {};
        snap.forEach(d => {
          const s = (d.data() || {}).status || "unknown";
          counts[s] = (counts[s] || 0) + 1;
        });
        return ok({ counts });
      }

      /* ?threadId=... → single thread + messages */
      if (qs.threadId) {
        const threadId = String(qs.threadId);
        const tRef = db.collection(THREADS_COLL).doc(threadId);
        const tSnap = await tRef.get();
        if (!tSnap.exists) return json(404, { success: false, notFound: true });

        const mSnap = await tRef.collection("messages").orderBy("timestamp", "asc").limit(500).get();
        const messages = mSnap.docs.map(d => ({ id: d.id, ...d.data() }));

        return ok({ thread: { id: tSnap.id, ...tSnap.data() }, messages });
      }

      /* ?list=1 → list threads, optionally filtered by status.
       * v4.3: ?list=1&completedSales=1 returns the "Completed Sales" menu
       * (threads where the listing-creator worker called markSuccess).
       * Filter is the existence of `salesCompletedAt` rather than
       * `status == "sales_completed"`, because subsequent post-purchase
       * chatter (e.g., "thanks, when will it ship?") routes through
       * etsyMailAutoPipeline → finalizeThread, which overwrites status
       * to "auto_replied" / "pending_human_review" / "queued_for_auto_send".
       * `salesCompletedAt` is written ONCE by the worker and is never
       * touched by any other code path, so it gives stable menu
       * membership regardless of post-sale activity.
       *
       * Firestore's orderBy implicitly excludes docs where the field is
       * missing, so this query also acts as an existence filter.
       */
      if (qs.list === "1") {
        const limit = Math.min(parseInt(qs.limit || "100", 10), 500);

        let q = db.collection(THREADS_COLL);

        if (qs.completedSales === "1") {
          // "Completed Sales" menu — stable membership via salesCompletedAt
          q = q.orderBy("salesCompletedAt", "desc").limit(limit);
        } else {
          const statusFilter = qs.status;
          if (statusFilter && VALID_STATUSES.has(statusFilter)) {
            q = q.where("status", "==", statusFilter);
          }
          q = q.orderBy("updatedAt", "desc").limit(limit);
        }

        const snap = await q.get();
        const threads = snap.docs.map(d => ({ id: d.id, ...d.data() }));
        return ok({ threads });
      }

      return bad("GET requires ?list=1, ?threadId=..., or ?counts=1");
    }

    /* ──────────────────────────── POST ──────────────────────────── */
    if (method === "POST") {
      let body = {};
      try { body = JSON.parse(event.body || "{}"); }
      catch { return bad("Invalid JSON body"); }

      const action = body.action;
      if (!action) return bad("Missing action");

      /* ---------- create ---------- */
      if (action === "create") {
        const {
          threadId: suppliedId,
          etsyConversationId = null,
          etsyConversationUrl = null,
          gmailMessageId      = null,
          gmailThreadId       = null,
          customerName        = "Unknown",
          customerEmail       = null,
          etsyUsername        = null,
          linkedOrderId       = null,
          subject             = null,
          initialText         = null,
          source              = "manual",          // 'manual' | 'gmail' | 'extension'
          status              = "detected_from_gmail"
        } = body;

        if (!VALID_STATUSES.has(status)) return bad(`Invalid status '${status}'`);

        const threadId = suppliedId
          || (etsyConversationId ? `etsy_conv_${etsyConversationId}` : `tmp_${Date.now()}_${Math.random().toString(36).slice(2,8)}`);

        const threadRef = db.collection(THREADS_COLL).doc(threadId);
        const now = FV.serverTimestamp();

        const threadDoc = {
          threadId,
          etsyConversationId,
          etsyConversationUrl,
          gmailMessageId,
          gmailThreadId,
          gmailReceivedAt      : null,
          customerName,
          customerEmail,
          etsyUsername,
          linkedOrderId,
          linkedListingIds     : [],
          status,
          category             : null,
          confidence           : null,
          needsHumanReview     : true,
          aiDraftStatus        : "none",
          latestDraftId        : null,
          lastInboundAt        : initialText ? now : null,
          lastOutboundAt       : null,
          lastSyncedAt         : null,
          lastScrapedDomHash   : null,
          assignedTo           : null,
          tags                 : [],
          riskFlags            : [],
          messageCount         : initialText ? 1 : 0,
          unread               : !!initialText,
          lastReadAt           : null,
          subject,
          createdAt            : now,
          updatedAt            : now
        };

        const batch = db.batch();
        batch.set(threadRef, threadDoc, { merge: false });

        if (initialText) {
          const msgRef = threadRef.collection("messages").doc();
          batch.set(msgRef, {
            source          : source === "gmail" ? "gmail" : "etsy",
            direction       : "inbound",
            senderName      : customerName || "Customer",
            senderRole      : "customer",
            timestamp       : now,
            text            : String(initialText),
            normalizedText  : String(initialText).toLowerCase().replace(/\s+/g, " ").trim(),
            contentHash     : null,  // filled by first scrape with real timestamp
            imageUrls       : [],
            storageImagePaths: [],
            attachmentUrls  : [],
            createdAt       : now
          });
        }

        await batch.commit();
        await writeAudit({
          threadId,
          eventType: "thread_created",
          actor    : `system:${source}`,
          payload  : { source, hasInitialText: !!initialText }
        });

        return ok({ threadId });
      }

      /* ---------- patch ---------- */
      if (action === "patch") {
        const { threadId, fields } = body;
        if (!threadId) return bad("Missing threadId");
        if (!fields || typeof fields !== "object") return bad("Missing fields object");

        const allowed = [
          "customerName", "customerEmail", "etsyUsername", "linkedOrderId",
          "linkedListingIds", "category", "confidence", "needsHumanReview",
          "aiDraftStatus", "latestDraftId", "assignedTo", "tags", "riskFlags",
          "subject", "etsyConversationId", "etsyConversationUrl",
          "gmailMessageId", "gmailThreadId"
        ];
        const update = { updatedAt: FV.serverTimestamp() };
        for (const k of Object.keys(fields)) {
          if (allowed.includes(k)) update[k] = fields[k];
        }
        if (Object.keys(update).length === 1) return bad("No allowed fields in patch");

        await db.collection(THREADS_COLL).doc(threadId).set(update, { merge: true });
        return ok({ threadId, patched: Object.keys(update).filter(k => k !== "updatedAt") });
      }

      /* ---------- appendMessage ---------- */
      if (action === "appendMessage") {
        const { threadId, message } = body;
        if (!threadId) return bad("Missing threadId");
        if (!message || typeof message !== "object") return bad("Missing message object");

        const {
          source        = "staff",
          direction     = "outbound",
          senderName    = "Staff",
          senderRole    = "staff",
          text          = "",
          imageUrls     = [],
          attachmentUrls = []
        } = message;

        const normalizedText = String(text).toLowerCase().replace(/\s+/g, " ").trim();
        const tRef = db.collection(THREADS_COLL).doc(threadId);
        const now  = FV.serverTimestamp();

        const batch = db.batch();
        const msgRef = tRef.collection("messages").doc();
        batch.set(msgRef, {
          source, direction, senderName, senderRole,
          timestamp: now,
          text, normalizedText,
          contentHash: null,
          imageUrls, storageImagePaths: [], attachmentUrls,
          createdAt: now
        });

        const threadPatch = {
          messageCount: FV.increment(1),
          updatedAt   : now
        };
        if (direction === "inbound") {
          threadPatch.lastInboundAt = now;
          threadPatch.unread = true;
        } else {
          threadPatch.lastOutboundAt = now;
        }
        batch.set(tRef, threadPatch, { merge: true });

        await batch.commit();
        await writeAudit({
          threadId,
          eventType: "message_appended",
          actor    : `system:${source}`,
          payload  : { direction, senderName }
        });

        return ok({ threadId, messageId: msgRef.id });
      }

      /* ---------- markRead ---------- */
      if (action === "markRead") {
        const { threadId } = body;
        if (!threadId) return bad("Missing threadId");
        await db.collection(THREADS_COLL).doc(threadId).set({
          unread    : false,
          lastReadAt: FV.serverTimestamp(),
          updatedAt : FV.serverTimestamp()
        }, { merge: true });
        return ok({ threadId });
      }

      /* ---------- setStatus ---------- */
      if (action === "setStatus") {
        const { threadId, status, reason = null, actor = "system:api" } = body;
        if (!threadId) return bad("Missing threadId");
        if (!VALID_STATUSES.has(status)) return bad(`Invalid status '${status}'`);

        const tRef = db.collection(THREADS_COLL).doc(threadId);
        const snap = await tRef.get();
        if (!snap.exists) return json(404, { success: false, notFound: true });

        const prev = (snap.data() || {}).status || null;

        // ── v1.4: Manual auto_replied attribution ──────────────────
        // The auto_replied status is reserved for AI-completed sends
        // (set by etsyMailDraftSend.complete after Etsy confirms
        // delivery). When an operator manually moves a thread here
        // via the move-to dropdown, we MUST distinguish it from a
        // real AI auto-reply — otherwise the "AI handled rate"
        // metric is polluted with operator decisions.
        //
        // Rule: any setStatus → auto_replied is automatically marked
        // with manuallyMovedToAutoReplied=true plus actor + timestamp.
        // The only path that creates a "real" auto_replied is
        // etsyMailDraftSend.complete, which doesn't go through this
        // endpoint.
        const patch = { status, updatedAt: FV.serverTimestamp() };
        if (status === "auto_replied") {
          patch.manuallyMovedToAutoReplied = true;
          patch.manualMoveActor            = actor;
          patch.manualMoveAt               = FV.serverTimestamp();
          patch.manualMoveReason           = reason || null;
          patch.manualMoveFromStatus       = prev;
          // Clear AI confidence/decision attribution so reporting
          // doesn't incorrectly credit the AI for this thread.
          patch.lastAutoDecision           = "manually_moved_to_auto_replied";
          patch.lastAutoDecisionAt         = FV.serverTimestamp();
        } else if (prev === "auto_replied" || prev === "queued_for_auto_send") {
          // Moving OUT of an AI-handled status — clear the manual flag
          // so a future re-entry isn't haunted by stale provenance.
          patch.manuallyMovedToAutoReplied = FV.delete();
          patch.manualMoveActor            = FV.delete();
          patch.manualMoveAt               = FV.delete();
          patch.manualMoveReason           = FV.delete();
          patch.manualMoveFromStatus       = FV.delete();
        }

        await tRef.set(patch, { merge: true });
        await writeAudit({
          threadId,
          eventType: "status_changed",
          actor,
          payload: {
            from: prev, to: status, reason,
            manualMoveFlagged: status === "auto_replied"
          }
        });
        return ok({
          threadId, from: prev, to: status,
          manuallyMovedToAutoReplied: status === "auto_replied" ? true : null
        });
      }

      /* ---------- enqueueJob ---------- */
      if (action === "enqueueJob") {
        const { threadId, jobType, payload = {} } = body;
        if (!threadId) return bad("Missing threadId");
        if (!jobType)  return bad("Missing jobType");

        const jobRef = db.collection(JOBS_COLL).doc();
        const now    = FV.serverTimestamp();
        await jobRef.set({
          jobId     : jobRef.id,
          threadId,
          jobType,
          status    : "queued",
          claimedBy : null,
          claimedAt : null,
          attempts  : 0,
          lastError : null,
          payload,
          createdAt : now,
          updatedAt : now
        });
        await writeAudit({
          threadId,
          eventType: "job_enqueued",
          actor    : "system:api",
          payload  : { jobType, jobId: jobRef.id }
        });
        return ok({ jobId: jobRef.id });
      }

      return bad(`Unknown action '${action}'`);
    }

    return json(405, { error: "Method Not Allowed" });

  } catch (err) {
    console.error("etsyMailThreads error:", err);
    return json(500, { error: err.message || String(err) });
  }
};
