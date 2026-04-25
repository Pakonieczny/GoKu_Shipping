/*  netlify/functions/etsyMailDraftSend.js
 *
 *  M5 send-pipeline orchestrator. Because the Etsy API has no public
 *  endpoint to send a conversation message, M5 routes sends through the
 *  Chrome extension, which scripts Etsy's own compose-and-send DOM flow
 *  on the operator's browser. This function is the coordination layer.
 *
 *  Flow:
 *    [inbox UI]      POST op=enqueue  →  writes EtsyMail_Drafts/{draftId}
 *                                        with status=queued, attachments[]
 *    [inbox UI]      GET  op=status   →  polls EtsyMail_Drafts/{draftId}
 *
 *    [extension]     POST op=peek     →  "is there a queued draft for the
 *                                        Etsy thread this tab has open?"
 *    [extension]     POST op=claim    →  atomic claim via Firestore txn;
 *                                        flips status queued → sending
 *    [extension]     POST op=heartbeat → keeps the claim fresh during the
 *                                        DOM-scripting phase (≤60s stale
 *                                        before the cleanup cron reclaims)
 *    [extension]     POST op=complete → flips status sending → sent
 *    [extension]     POST op=fail     → flips status sending → failed,
 *                                        with { error, partial } detail
 *
 *  Draft document schema (EtsyMail_Drafts/{draftId}):
 *    {
 *      draftId            : "draft_etsy_conv_1651714855",
 *      threadId           : "etsy_conv_1651714855",
 *      etsyConversationUrl: "https://www.etsy.com/your/conversations/1651714855",
 *      text               : "Hi Karrie! Thanks for your order…",
 *      status             : "draft" | "queued" | "sending" | "sent" | "sent_text_only" | "failed",
 *      attachments        : [
 *        {
 *          attachmentId : "att_abc123",
 *          type         : "image" | "listing" | "tracking_image",
 *          // for image/tracking_image:
 *          storagePath  : "etsymail/drafts/.../att_abc123.png",
 *          proxyUrl     : "/.netlify/functions/etsyMailImage?path=...",
 *          contentType  : "image/png",
 *          bytes        : 12345,
 *          filename     : "screenshot.png",
 *          // for listing:
 *          listingId    : "1234567890",
 *          listingUrl   : "https://www.etsy.com/listing/1234567890",
 *          listingTitle : "Sterling Silver Cardinal Charm",
 *          thumbnail    : "...",
 *          // for tracking_image specifically:
 *          trackingCode : "9400...",
 *          carrier      : "USPS"
 *        },
 *        ...
 *      ],
 *      // Populated by the AI draft generator (M4); preserved here so the
 *      // draft doc is self-contained.
 *      generatedByAI      : true,
 *      aiModel            : "claude-opus-4-7",
 *      aiReasoning        : "...",
 *      aiActiveQuestion   : "...",
 *      // Lifecycle
 *      createdBy          : "Paul_K",
 *      createdAt          : Timestamp,
 *      queuedAt           : Timestamp,
 *      sentAt             : Timestamp,
 *      // Send coordination
 *      sendSessionId      : "ext_abc123",  // extension instance that claimed it
 *      sendClaimedAt      : Timestamp,
 *      sendHeartbeatAt    : Timestamp,     // refreshed every ~5s during send
 *      sendAttempts       : 2,             // 3rd try before giving up
 *      sendError          : string | null,
 *      sendPartialSuccess : true | false,  // sent text but images failed
 *      updatedAt          : Timestamp
 *    }
 *
 *  Auth:
 *    - enqueue + status ops: no secret required (same-origin inbox).
 *    - peek, claim, heartbeat, complete, fail: require X-EtsyMail-Secret
 *      (extension-invoked). Enforced per-op below.
 */

const admin = require("./firebaseAdmin");
const { requireExtensionAuth, CORS } = require("./_etsyMailAuth");

const db = admin.firestore();
const FV = admin.firestore.FieldValue;

const DRAFTS_COLL = "EtsyMail_Drafts";
const AUDIT_COLL  = "EtsyMail_Audit";
const CONFIG_COLL = "EtsyMail_Config";          // v0.9.1: kill-switch lives here

const MAX_SEND_ATTEMPTS      = 3;
const STALE_HEARTBEAT_MS     = 60 * 1000;       // 60s: if no heartbeat, treat claim as abandoned
const MAX_CLAIM_LOOKBACK_MIN = 30;              // ignore draft docs older than 30 min from peek

// Kill-switch cache: avoid one Firestore read per peek/claim. The cache
// invalidates after 15 seconds, so flipping the switch in Firestore takes
// at most 15s + the next peek interval to take effect across all tabs.
let _killSwitchCache = { value: null, fetchedAt: 0 };
const KILL_SWITCH_CACHE_MS = 15 * 1000;

function json(statusCode, body) {
  return { statusCode, headers: CORS, body: JSON.stringify(body) };
}
function bad(msg, code = 400) { return json(code, { error: msg }); }
function ok(payload) { return json(200, { success: true, ...payload }); }

/** Read the global send-disabled flag from EtsyMail_Config/global.
 *  Returns { disabled, reason, by }. Cached for KILL_SWITCH_CACHE_MS to
 *  avoid hot-pathing Firestore on every peek/claim. */
async function getKillSwitch() {
  if (Date.now() - _killSwitchCache.fetchedAt < KILL_SWITCH_CACHE_MS && _killSwitchCache.value) {
    return _killSwitchCache.value;
  }
  try {
    const snap = await db.collection(CONFIG_COLL).doc("global").get();
    const data = snap.exists ? snap.data() : {};
    const value = {
      disabled : !!data.sendDisabled,
      reason   : data.sendDisabledReason || null,
      by       : data.sendDisabledBy     || null,
      at       : data.sendDisabledAt     ? data.sendDisabledAt.toMillis() : null
    };
    _killSwitchCache = { value, fetchedAt: Date.now() };
    return value;
  } catch (e) {
    console.warn("killSwitch fetch failed:", e.message);
    // Fail-open: if we can't read the doc, allow sends. Better than
    // hard-failing the whole pipeline if Firestore has a transient issue.
    return { disabled: false, reason: null, by: null, at: null };
  }
}

async function audit(threadId, draftId, eventType, actor, payload) {
  try {
    await db.collection(AUDIT_COLL).add({
      threadId : threadId || null,
      draftId  : draftId  || null,
      eventType,
      actor    : actor || "system",
      payload  : payload || {},
      createdAt: FV.serverTimestamp()
    });
  } catch (e) {
    console.warn("audit write failed:", e.message);
  }
}

function serializeDoc(snap) {
  if (!snap.exists) return null;
  const data = snap.data();
  const out  = { id: snap.id };
  for (const [k, v] of Object.entries(data)) {
    if (v && typeof v.toMillis === "function") {
      out[k] = { _ts: true, ms: v.toMillis() };
    } else {
      out[k] = v;
    }
  }
  return out;
}

function isStaleHeartbeat(hbTs) {
  if (!hbTs) return true;
  const ms = hbTs.toMillis ? hbTs.toMillis() : Number(hbTs);
  if (!ms) return true;
  return (Date.now() - ms) > STALE_HEARTBEAT_MS;
}

/** True if a queued draft has been sitting too long to safely send.
 *  Operators expect that clicking Send sends NOW — if a queued draft
 *  was forgotten about for hours and an Etsy tab opens later, sending
 *  yesterday's draft today is the wrong behavior. */
function isStaleQueued(queuedAtTs) {
  if (!queuedAtTs) return false;   // no queuedAt = treat as fresh (defensive)
  const ms = queuedAtTs.toMillis ? queuedAtTs.toMillis() : Number(queuedAtTs);
  if (!ms) return false;
  return (Date.now() - ms) > (MAX_CLAIM_LOOKBACK_MIN * 60 * 1000);
}

/** Normalize an attachments array for persistence. Strips sentinels,
 *  validates required fields per type, and ensures attachmentId is set. */
function normalizeAttachments(raw) {
  if (!Array.isArray(raw)) return [];
  const out = [];
  for (const a of raw) {
    if (!a || typeof a !== "object") continue;
    const type = a.type;
    if (type !== "image" && type !== "listing" && type !== "tracking_image") continue;

    const common = {
      attachmentId: a.attachmentId || ("att_" + Math.random().toString(36).slice(2, 12)),
      type
    };
    if (type === "image") {
      // Operator-uploaded images need both: storagePath identifies the
      // bucket object, proxyUrl is what the extension fetches.
      if (!a.storagePath || !a.proxyUrl) continue;
      out.push({
        ...common,
        storagePath : String(a.storagePath),
        proxyUrl    : String(a.proxyUrl),
        contentType : a.contentType || "image/png",
        bytes       : Number(a.bytes) || null,
        filename    : a.filename || null
      });
    } else if (type === "tracking_image") {
      // Tracking images only need proxyUrl — storagePath isn't always
      // populated by the M4 snapshot path, and the extension only
      // needs the proxyUrl to fetch bytes anyway. Dropping these on
      // null storagePath was silently breaking image attachment for
      // every send that included a tracking image.
      if (!a.proxyUrl) continue;
      out.push({
        ...common,
        proxyUrl      : String(a.proxyUrl),
        storagePath   : a.storagePath || null,   // optional, kept for forensics
        contentType   : a.contentType || "image/png",
        bytes         : Number(a.bytes) || null,
        filename      : a.filename || null,
        trackingCode  : a.trackingCode || null,
        carrier       : a.carrier      || null,
        trackingStatus: a.trackingStatus || null
      });
    } else if (type === "listing") {
      if (!a.listingId) continue;
      out.push({
        ...common,
        listingId   : String(a.listingId),
        listingUrl  : a.listingUrl  || `https://www.etsy.com/listing/${a.listingId}`,
        listingTitle: a.listingTitle || null,
        thumbnail   : a.thumbnail    || null,
        price       : a.price        || null
      });
    }
  }
  return out;
}

exports.handler = async (event) => {
  if (event.httpMethod === "OPTIONS") {
    return { statusCode: 200, headers: CORS, body: "ok" };
  }

  // ── Auth: ALL ops now require X-EtsyMail-Secret ────────────────────
  // v0.9.1 (#1): the inbox subdomain is publicly reachable, so any op
  // could be hit by an outside party who finds it. Every op now requires
  // the same secret the extension sends. The inbox forwards it from
  // localStorage('etsymail_secret'). If ETSYMAIL_EXTENSION_SECRET env
  // var is unset, requireExtensionAuth falls back to passthrough (dev).
  const auth = requireExtensionAuth(event);
  if (!auth.ok) return auth.response;

  // ── GET ops ────────────────────────────────────────────────────────
  if (event.httpMethod === "GET") {
    const qs = event.queryStringParameters || {};
    const op = qs.op;
    if (!op) return bad("Missing op");

    /* ── status ──
     *  Inbox polls this while waiting for the extension to send. */
    if (op === "status") {
      const { draftId } = qs;
      if (!draftId) return bad("Missing draftId");
      const snap = await db.collection(DRAFTS_COLL).doc(String(draftId)).get();
      if (!snap.exists) return json(404, { error: "Draft not found", draftId });
      return ok({ draft: serializeDoc(snap) });
    }

    /* ── killswitch_status ──
     *  Inbox polls this to render the kill-switch banner. Cheap. */
    if (op === "killswitch_status") {
      const ks = await getKillSwitch();
      return ok({ killSwitch: ks });
    }

    return bad(`Unknown GET op '${op}'`);
  }

  if (event.httpMethod !== "POST") {
    return json(405, { error: "Method Not Allowed" });
  }

  let body = {};
  try { body = JSON.parse(event.body || "{}"); }
  catch { return bad("Invalid JSON body"); }

  const { op } = body;
  if (!op) return bad("Missing op");

  // Note: per-op auth gating removed in v0.9.1 — ALL ops authed at top.

  try {
    /* ── enqueue (inbox → server) ─────────────────────────────────
     *  Called when the operator clicks "Send via Etsy".
     *  Input:
     *    { op:"enqueue", threadId, etsyConversationUrl, text,
     *      attachments:[...], employeeName, aiMeta:{...}?,
     *      force?:bool }     // v0.9.1 #6: needed to overwrite a draft
     *                        //          queued by a different operator
     *  Output: { draftId, status:"queued" } */
    if (op === "enqueue") {
      // v0.9.1 #8: kill-switch — global send disable
      const ks = await getKillSwitch();
      if (ks.disabled) {
        return json(503, {
          error      : "Send pipeline disabled by operator",
          errorCode  : "SEND_DISABLED",
          killSwitch : ks
        });
      }
      const {
        threadId,
        etsyConversationUrl,
        text,
        attachments  = [],
        employeeName = null,
        aiMeta       = null,
        force        = false       // v0.9.1 #6: explicit overwrite of another operator's queued draft
      } = body;

      if (!threadId || !/^etsy_conv_\d+$/.test(String(threadId))) {
        return bad("threadId must match etsy_conv_<digits>");
      }
      // The conversation URL must match a route the extension's content
      // script recognizes. Mirrors the patterns in content-sender.js's
      // extractConversationId() so a URL the extension can read is also
      // a URL we accept on enqueue. Catches:
      //   /your/conversations/<id>
      //   /conversations/<id>
      //   /your/messages/buyer/<id>
      //   /your/messages/thread/<id>
      //   /messages/<id>
      const URL_RE = /^https:\/\/(www\.)?etsy\.com\/(?:your\/conversations|conversations|your\/messages\/(?:buyer|thread)|messages)\/\d+/;
      if (!etsyConversationUrl || !URL_RE.test(etsyConversationUrl)) {
        return bad("etsyConversationUrl must be an Etsy conversation URL");
      }
      const cleanText = String(text || "").trim();
      const normalized = normalizeAttachments(attachments);

      if (!cleanText && !normalized.length) {
        return bad("Draft must have text or at least one attachment");
      }

      // Deterministic draftId per thread: prevents stacked queued drafts
      // for the same thread, and makes peek a single doc-get not a query.
      const draftId = "draft_" + threadId;
      const ref     = db.collection(DRAFTS_COLL).doc(draftId);

      // Transaction: enqueuing overwrites any prior queued/sending state
      // with clear audit trail. If currently sending, operator must wait
      // — return 409 so the UI can show a graceful message.
      const result = await db.runTransaction(async (tx) => {
        const snap = await tx.get(ref);
        const prev = snap.exists ? snap.data() : null;
        if (prev && (prev.status === "sending")) {
          return { conflict: true, prevStatus: prev.status };
        }

        // v0.9.1 #6: block second-operator queue overwrites
        // If another operator already queued this draft, refuse silently
        // unless the current operator passed force:true. The inbox catches
        // the 409 and shows a confirm dialog.
        if (prev && prev.status === "queued" && !force) {
          const prevOperator = prev.createdBy || null;
          const thisOperator = employeeName    || null;
          if (prevOperator && thisOperator && prevOperator !== thisOperator) {
            return {
              ownerConflict : true,
              prevOperator,
              thisOperator,
              prevQueuedAt  : prev.queuedAt ? prev.queuedAt.toMillis() : null
            };
          }
        }

        const payload = {
          draftId,
          threadId,
          etsyConversationUrl,
          text               : cleanText,
          attachments        : normalized,
          status             : "queued",
          createdBy          : employeeName || (prev && prev.createdBy) || null,
          // Preserve AI metadata if this was originally an AI draft
          generatedByAI      : (aiMeta && aiMeta.generatedByAI) != null
            ? !!aiMeta.generatedByAI
            : (prev && prev.generatedByAI) || false,
          aiModel            : (aiMeta && aiMeta.model)            || (prev && prev.aiModel) || null,
          aiReasoning        : (aiMeta && aiMeta.reasoning)        || (prev && prev.aiReasoning) || null,
          aiActiveQuestion   : (aiMeta && aiMeta.activeQuestion)   || (prev && prev.aiActiveQuestion) || null,
          // Lifecycle
          queuedAt           : FV.serverTimestamp(),
          updatedAt          : FV.serverTimestamp(),
          // Reset send-coordination state
          sendSessionId      : null,
          sendClaimedAt      : null,
          sendHeartbeatAt    : null,
          sendAttempts       : 0,
          sendError          : null,
          sendErrorCode      : null,
          sendPartialSuccess : false,
          sendStage          : "pre_click",  // v0.9.1 #2/#3: send-boundary state
          sentAt             : null
        };
        if (!snap.exists) payload.createdAt = FV.serverTimestamp();
        tx.set(ref, payload, { merge: true });
        return { conflict: false, payload };
      });

      if (result.conflict) {
        return json(409, {
          error      : `Draft is currently ${result.prevStatus}; wait for it to finish or fail`,
          errorCode  : "DRAFT_BUSY",
          draftId,
          prevStatus : result.prevStatus
        });
      }
      if (result.ownerConflict) {
        return json(409, {
          error         : `Draft is queued by ${result.prevOperator}. Send 'force:true' to overwrite.`,
          errorCode     : "QUEUE_OWNER_CONFLICT",
          draftId,
          prevOperator  : result.prevOperator,
          thisOperator  : result.thisOperator,
          prevQueuedAt  : result.prevQueuedAt
        });
      }

      await audit(threadId, draftId, "draft_enqueued", employeeName || "operator", {
        textLength   : cleanText.length,
        attachmentCount: normalized.length,
        attachmentTypes: normalized.map(a => a.type)
      });

      return ok({
        draftId,
        status      : "queued",
        threadId,
        attachmentCount: normalized.length,
        pollUrl     : `/.netlify/functions/etsyMailDraftSend?op=status&draftId=${encodeURIComponent(draftId)}`
      });
    }

    /* ── cancel (inbox → server) ──────────────────────────────────
     *  Operator clicks "Cancel send" before the extension claims it.
     *  Only allowed while status === "queued" — if already sending, too late. */
    if (op === "cancel") {
      const { draftId } = body;
      if (!draftId) return bad("Missing draftId");
      const ref = db.collection(DRAFTS_COLL).doc(String(draftId));
      const result = await db.runTransaction(async (tx) => {
        const snap = await tx.get(ref);
        if (!snap.exists) return { notFound: true };
        const prev = snap.data();
        if (prev.status !== "queued") return { badState: prev.status };
        tx.set(ref, {
          status    : "draft",
          queuedAt  : null,
          updatedAt : FV.serverTimestamp()
        }, { merge: true });
        return { ok: true, threadId: prev.threadId };
      });
      if (result.notFound) return json(404, { error: "Draft not found" });
      if (result.badState) return json(409, { error: `Cannot cancel — draft is ${result.badState}` });
      await audit(result.threadId, draftId, "draft_cancelled", "operator", {});
      return ok({ draftId, status: "draft" });
    }

    /* ── peek (extension → server) ────────────────────────────────
     *  Extension content script asks: "is there a queued draft for
     *  the thread this Etsy tab is on?" Read-only, no claim.
     *  Input: { op:"peek", threadId }
     *  Output: { queued: true, draft: {...} } | { queued: false } */
    if (op === "peek") {
      const { threadId } = body;
      if (!threadId) return bad("Missing threadId");

      // v0.9.1 #8: kill-switch — peek returns no work if disabled
      const ks = await getKillSwitch();
      if (ks.disabled) {
        return ok({ queued: false, killSwitch: ks });
      }

      // Deterministic draft id keeps this a doc-get, not a query.
      const draftId = "draft_" + threadId;
      const snap = await db.collection(DRAFTS_COLL).doc(draftId).get();
      if (!snap.exists) return ok({ queued: false });
      const d = snap.data();

      if (d.status === "queued") {
        // Stale queued draft — operator clicked Send hours ago and
        // forgot. Don't surface it to the extension; instead expire
        // it so the inbox UI sees `failed` on next status poll.
        if (isStaleQueued(d.queuedAt)) {
          try {
            await db.collection(DRAFTS_COLL).doc(draftId).set({
              status        : "failed",
              sendError     : `Expired — queued more than ${MAX_CLAIM_LOOKBACK_MIN} minutes`,
              sendErrorCode : "QUEUED_EXPIRED",
              updatedAt     : FV.serverTimestamp()
            }, { merge: true });
            await audit(d.threadId, draftId, "draft_queue_expired", "peek", {
              ageMin: Math.round((Date.now() - d.queuedAt.toMillis()) / 60000)
            });
          } catch (e) { console.warn("expire stale queued failed:", e.message); }
          return ok({ queued: false, currentStatus: "failed" });
        }
        return ok({ queued: true, draft: serializeDoc(snap) });
      }

      if (d.status !== "queued") {
        // v0.9.1 #2/#3: stale-sending drafts are NEVER auto-reclaimable
        // if sendStage === "post_click" — clicking Send a second time
        // would cause a duplicate message. The cleanup cron handles
        // this case by marking failed (STRANDED_POST_CLICK), which
        // requires manual operator review on Etsy.
        if (d.status === "sending" && isStaleHeartbeat(d.sendHeartbeatAt)) {
          if (d.sendStage === "post_click") {
            // Don't surface; cleanup cron will fail it for manual review.
            return ok({ queued: false, currentStatus: "sending", stranded: true, postClick: true });
          }
          return ok({ queued: true, stale: true, draft: serializeDoc(snap) });
        }
        return ok({ queued: false, currentStatus: d.status });
      }
    }

    /* ── claim (extension → server) ───────────────────────────────
     *  Atomic. Only one extension wins a given queued draft.
     *  Input: { op:"claim", draftId, sessionId, workerId }
     *  Output: { draft, prevStatus } */
    if (op === "claim") {
      const { draftId, sessionId, workerId } = body;
      if (!draftId || !sessionId) return bad("Missing draftId or sessionId");

      // v0.9.1 #8: kill-switch — refuse to claim if disabled
      const ks = await getKillSwitch();
      if (ks.disabled) {
        return json(503, { error: "Send pipeline disabled", errorCode: "SEND_DISABLED", killSwitch: ks });
      }

      const ref = db.collection(DRAFTS_COLL).doc(String(draftId));
      const result = await db.runTransaction(async (tx) => {
        const snap = await tx.get(ref);
        if (!snap.exists) return { notFound: true };
        const prev = snap.data();

        // Reject stale queued — paired with peek's expiration logic
        // so a slow extension claim can't beat the expiration sweep.
        if (prev.status === "queued" && isStaleQueued(prev.queuedAt)) {
          tx.set(ref, {
            status        : "failed",
            sendError     : `Expired — queued more than ${MAX_CLAIM_LOOKBACK_MIN} minutes`,
            sendErrorCode : "QUEUED_EXPIRED",
            updatedAt     : FV.serverTimestamp()
          }, { merge: true });
          return { expired: true };
        }

        // v0.9.1 #2/#3: never re-claim a stranded post-click draft.
        // The Send button was clicked; we don't know if the message went
        // through. Re-clicking would risk a duplicate. Mark failed for
        // manual operator review on Etsy.
        const staleSending = prev.status === "sending" && isStaleHeartbeat(prev.sendHeartbeatAt);
        if (staleSending && prev.sendStage === "post_click") {
          tx.set(ref, {
            status        : "failed",
            sendError     : "Tab died after clicking Etsy's Send button. Verify on Etsy whether the message went through before re-sending.",
            sendErrorCode : "STRANDED_POST_CLICK",
            updatedAt     : FV.serverTimestamp()
          }, { merge: true });
          return { strandedPostClick: true };
        }

        // Accept queued, or sending-but-stale-pre-click (extension died before Send)
        if (prev.status !== "queued" && !staleSending) {
          return { taken: true, currentStatus: prev.status };
        }

        // Retry guard
        const nextAttempts = (prev.sendAttempts || 0) + 1;
        if (nextAttempts > MAX_SEND_ATTEMPTS) {
          tx.set(ref, {
            status       : "failed",
            sendError    : `Exceeded ${MAX_SEND_ATTEMPTS} attempts`,
            sendAttempts : nextAttempts,
            updatedAt    : FV.serverTimestamp()
          }, { merge: true });
          return { exhausted: true, attempts: nextAttempts };
        }

        tx.set(ref, {
          status         : "sending",
          sendSessionId  : String(sessionId),
          sendWorkerId   : workerId || null,
          sendClaimedAt  : FV.serverTimestamp(),
          sendHeartbeatAt: FV.serverTimestamp(),
          sendAttempts   : nextAttempts,
          sendError      : null,
          sendErrorCode  : null,
          sendStage      : "pre_click",   // v0.9.1 #2/#3: reset on every claim
          updatedAt      : FV.serverTimestamp()
        }, { merge: true });
        return { ok: true, data: prev, attempts: nextAttempts };
      });

      if (result.notFound) return json(404, { error: "Draft not found" });
      if (result.expired)  return json(410, { error: `Draft expired — was queued > ${MAX_CLAIM_LOOKBACK_MIN} min`, errorCode: "QUEUED_EXPIRED" });
      if (result.strandedPostClick) {
        return json(410, {
          error     : "Previous attempt clicked Send and went silent. Manual review required on Etsy.",
          errorCode : "STRANDED_POST_CLICK"
        });
      }
      if (result.taken)    return json(409, { error: `Draft already ${result.currentStatus}`, currentStatus: result.currentStatus });
      if (result.exhausted) return json(410, { error: "Draft exhausted retry budget", attempts: result.attempts });

      // Return the full draft payload the extension needs to execute the send.
      const freshSnap = await ref.get();
      await audit(result.data.threadId, draftId, "draft_claimed", workerId || "extension", {
        sessionId, attempt: result.attempts
      });
      return ok({ draft: serializeDoc(freshSnap), attempts: result.attempts });
    }

    /* ── heartbeat (extension → server) ───────────────────────────
     *  Refresh sendHeartbeatAt. Extension calls this every 5s while
     *  actively scripting Etsy's compose. Input must include sessionId
     *  so stolen claims are rejected. */
    if (op === "heartbeat") {
      const { draftId, sessionId, progress } = body;
      if (!draftId || !sessionId) return bad("Missing draftId or sessionId");

      const ref = db.collection(DRAFTS_COLL).doc(String(draftId));
      const result = await db.runTransaction(async (tx) => {
        const snap = await tx.get(ref);
        if (!snap.exists) return { notFound: true };
        const prev = snap.data();
        if (prev.status !== "sending") return { badState: prev.status };
        if (prev.sendSessionId !== sessionId) return { notYours: true, owner: prev.sendSessionId };

        const patch = {
          sendHeartbeatAt: FV.serverTimestamp(),
          updatedAt      : FV.serverTimestamp()
        };
        if (progress && typeof progress === "object") {
          patch.sendProgress = {
            phase       : progress.phase || null,
            stepLabel   : progress.stepLabel || null,
            attachmentsUploaded: Number(progress.attachmentsUploaded) || 0,
            attachmentsTotal   : Number(progress.attachmentsTotal)    || 0,
            ts          : Date.now()
          };
        }
        tx.set(ref, patch, { merge: true });
        return { ok: true };
      });
      if (result.notFound) return json(404, { error: "Draft not found" });
      if (result.badState) return json(409, { error: `Draft is ${result.badState}` });
      if (result.notYours) return json(403, { error: "Heartbeat from wrong session", owner: result.owner });
      return ok({ heartbeat: Date.now() });
    }

    /* ── mark_clicked (extension → server) ─────────────────────────
     *  v0.9.1 #2/#3: extension calls this immediately BEFORE clicking
     *  Etsy's Send button. The atomic flip from sendStage="pre_click"
     *  to "post_click" tells the cleanup cron and the claim path that
     *  any future stranding must NOT auto-requeue — clicking again
     *  would risk a duplicate send.
     *
     *  This must be a synchronous round-trip BEFORE the click. If the
     *  call fails, the extension MUST NOT click Send (treats as a
     *  retryable failure with errorCode=MARK_CLICKED_FAILED).
     *
     *  Input: { op:"mark_clicked", draftId, sessionId } */
    if (op === "mark_clicked") {
      const { draftId, sessionId } = body;
      if (!draftId || !sessionId) return bad("Missing draftId or sessionId");

      const ref = db.collection(DRAFTS_COLL).doc(String(draftId));
      const result = await db.runTransaction(async (tx) => {
        const snap = await tx.get(ref);
        if (!snap.exists) return { notFound: true };
        const prev = snap.data();
        if (prev.status !== "sending") return { badState: prev.status };
        if (prev.sendSessionId !== sessionId) return { notYours: true };
        tx.set(ref, {
          sendStage      : "post_click",
          sendHeartbeatAt: FV.serverTimestamp(),
          updatedAt      : FV.serverTimestamp()
        }, { merge: true });
        return { ok: true, threadId: prev.threadId };
      });
      if (result.notFound) return json(404, { error: "Draft not found" });
      if (result.badState) return json(409, { error: `Draft is ${result.badState}` });
      if (result.notYours) return json(403, { error: "mark_clicked from wrong session" });
      await audit(result.threadId, draftId, "draft_mark_clicked", sessionId, {});
      return ok({ stage: "post_click", at: Date.now() });
    }

    /* ── complete (extension → server) ────────────────────────────
     *  Extension confirms the send succeeded.
     *  Input: { draftId, sessionId, partial?, sentText?, unverified?, imagesSent? } */
    if (op === "complete") {
      const {
        draftId, sessionId,
        partial = false, sentText = true,
        unverified = false,    // v0.9.1 #4: 12s timeout with no positive signal
        imagesSent = 0, imagesTotal = 0,
        listingsSent = 0, listingsTotal = 0,
        etsyMessageId = null,
        note = null
      } = body;
      if (!draftId || !sessionId) return bad("Missing draftId or sessionId");

      const ref = db.collection(DRAFTS_COLL).doc(String(draftId));
      const result = await db.runTransaction(async (tx) => {
        const snap = await tx.get(ref);
        if (!snap.exists) return { notFound: true };
        const prev = snap.data();
        if (prev.sendSessionId !== sessionId) return { notYours: true };

        // v0.9.1 #4: terminal status reflects what we actually know.
        //   - partial=true   → sent_text_only (text confirmed, images failed)
        //   - unverified=true → sent_unverified (clicked Send, no positive
        //                       signal within 12s; needs operator verification)
        //   - else            → sent (toast or composer-cleared confirmation)
        let finalStatus = "sent";
        if (partial)         finalStatus = "sent_text_only";
        else if (unverified) finalStatus = "sent_unverified";

        tx.set(ref, {
          status             : finalStatus,
          sentAt             : FV.serverTimestamp(),
          sendPartialSuccess : !!partial,
          sendUnverified     : !!unverified,
          sendImagesSent     : Number(imagesSent)     || 0,
          sendImagesTotal    : Number(imagesTotal)    || 0,
          sendListingsSent   : Number(listingsSent)   || 0,
          sendListingsTotal  : Number(listingsTotal)  || 0,
          sendTextSent       : !!sentText,
          sendNote           : note || null,
          etsyMessageId      : etsyMessageId || null,
          sendHeartbeatAt    : FV.serverTimestamp(),
          updatedAt          : FV.serverTimestamp()
        }, { merge: true });
        return { ok: true, threadId: prev.threadId, status: finalStatus };
      });
      if (result.notFound) return json(404, { error: "Draft not found" });
      if (result.notYours) return json(403, { error: "Complete from wrong session" });
      await audit(result.threadId, draftId, "draft_sent", sessionId, {
        partial, unverified, imagesSent, imagesTotal, listingsSent, listingsTotal, note
      });
      return ok({ draftId, status: result.status });
    }

    /* ── fail (extension → server) ────────────────────────────────
     *  Extension reports a failure. Supports requeue-if-retryable.
     *  Input: { draftId, sessionId, error, retry?:boolean, errorCode? } */
    if (op === "fail") {
      const {
        draftId, sessionId,
        error = "unknown error",
        errorCode = null,
        retry = false
      } = body;
      if (!draftId || !sessionId) return bad("Missing draftId or sessionId");

      const ref = db.collection(DRAFTS_COLL).doc(String(draftId));
      const result = await db.runTransaction(async (tx) => {
        const snap = await tx.get(ref);
        if (!snap.exists) return { notFound: true };
        const prev = snap.data();
        if (prev.sendSessionId !== sessionId) return { notYours: true };

        const attempts = prev.sendAttempts || 0;
        const willRetry = retry && attempts < MAX_SEND_ATTEMPTS;

        const patch = {
          sendError      : String(error).slice(0, 1000),
          sendErrorCode  : errorCode || null,
          sendHeartbeatAt: FV.serverTimestamp(),
          updatedAt      : FV.serverTimestamp()
        };
        if (willRetry) {
          patch.status        = "queued";
          patch.sendSessionId = null;
          patch.sendClaimedAt = null;
          // Keep attempts; next claim increments.
        } else {
          patch.status        = "failed";
        }
        tx.set(ref, patch, { merge: true });
        return { ok: true, threadId: prev.threadId, requeued: willRetry, attempts };
      });
      if (result.notFound) return json(404, { error: "Draft not found" });
      if (result.notYours) return json(403, { error: "Fail from wrong session" });
      await audit(result.threadId, draftId, result.requeued ? "draft_send_requeued" : "draft_send_failed", sessionId, {
        error, errorCode, attempts: result.attempts
      });
      return ok({
        draftId,
        status   : result.requeued ? "queued" : "failed",
        requeued : result.requeued,
        attempts : result.attempts
      });
    }

    /* ── kill_switch_set (ops → server) ──────────────────────────
     *  Toggle the global send-disabled flag. Authenticated like every
     *  other op. v0.9.1 #8.
     *
     *  Input: { op:"kill_switch_set", disabled:bool, reason?, by? }
     *  Output: { killSwitch: { disabled, reason, by, at } } */
    if (op === "kill_switch_set") {
      const { disabled, reason = null, by = null } = body;
      if (typeof disabled !== "boolean") return bad("disabled must be a boolean");
      const ref = db.collection(CONFIG_COLL).doc("global");
      await ref.set({
        sendDisabled       : !!disabled,
        sendDisabledReason : disabled ? (reason || "manually disabled") : null,
        sendDisabledBy     : disabled ? (by     || "operator")          : null,
        sendDisabledAt     : disabled ? FV.serverTimestamp()            : null,
        updatedAt          : FV.serverTimestamp()
      }, { merge: true });
      // Invalidate cache immediately
      _killSwitchCache = { value: null, fetchedAt: 0 };
      await audit(null, null, disabled ? "kill_switch_enabled" : "kill_switch_disabled", by || "operator", { reason });
      return ok({ killSwitch: { disabled, reason, by, at: Date.now() } });
    }

    return bad(`Unknown op '${op}'`);

  } catch (err) {
    console.error("etsyMailDraftSend error:", err);
    return json(500, { error: err.message || String(err) });
  }
};
