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
const db    = admin.firestore();
const FV    = admin.firestore.FieldValue;

const THREADS_COLL  = "EtsyMail_Threads";
const AUDIT_COLL    = "EtsyMail_Audit";
const JOBS_COLL     = "EtsyMail_Jobs";

const CORS = {
  "Access-Control-Allow-Origin" : "*",
  "Access-Control-Allow-Headers": "Content-Type,Authorization",
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
  "send_in_progress",
  "sent",
  "hold_uncertain",
  "hold_missing_order",
  "hold_login_required",
  "failed_scrape",
  "failed_send",
  "archived"
]);

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

  try {
    const method = event.httpMethod;
    const qs     = event.queryStringParameters || {};

    /* ──────────────────────────── GET ──────────────────────────── */
    if (method === "GET") {

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

      /* ?list=1 → list threads, optionally filtered by status */
      if (qs.list === "1") {
        const statusFilter = qs.status;
        const limit        = Math.min(parseInt(qs.limit || "100", 10), 500);

        let q = db.collection(THREADS_COLL);
        if (statusFilter && VALID_STATUSES.has(statusFilter)) {
          q = q.where("status", "==", statusFilter);
        }
        // order by lastInboundAt desc, falling back to updatedAt
        q = q.orderBy("updatedAt", "desc").limit(limit);

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
        await tRef.set({ status, updatedAt: FV.serverTimestamp() }, { merge: true });
        await writeAudit({
          threadId,
          eventType: "status_changed",
          actor,
          payload: { from: prev, to: status, reason }
        });
        return ok({ threadId, from: prev, to: status });
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
