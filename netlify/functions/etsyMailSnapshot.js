/*  netlify/functions/etsyMailSnapshot.js
 *
 *  Ingest endpoint for the Chrome extension's Etsy thread scrapes.
 *
 *  Flow:
 *    1. Extension scrapes an Etsy conversation and POSTs a structured snapshot.
 *    2. This function finds (or creates) a thread doc keyed by etsyConversationId.
 *    3. For each scraped message, it dedupes by contentHash and writes only new ones.
 *    4. It updates the thread's lastSyncedAt, lastScrapedDomHash, customer/username if newly learned,
 *       and advances status detected_from_gmail → etsy_scraped.
 *    5. Writes an audit event.
 *
 *  POST body shape (from extension):
 *    {
 *      scrapedAt: <ms>,
 *      etsyConversationId: <string>,
 *      etsyConversationUrl: <string>,
 *      threadDomHash: <sha1 of full DOM>,
 *      participants: [{ name, etsyUsername, role }, ...],
 *      subject: <string or null>,
 *      messages: [
 *        {
 *          senderName, senderRole,      // role: 'customer' | 'staff'
 *          timestampMs,                  // ms since epoch
 *          text,
 *          imageUrls: [<etsy CDN urls>], // to be mirrored
 *          attachmentUrls: [...],
 *          contentHash: <sha1(senderName + timestampMs + normalizedText)>,
 *          domSelector: <optional debug>
 *        }, ...
 *      ],
 *      session: { etsyLoggedIn: <bool>, etsyUsername: <string or null> }
 *    }
 */

const admin = require("./firebaseAdmin");
const { requireExtensionAuth, CORS } = require("./_etsyMailAuth");
const db  = admin.firestore();
const FV  = admin.firestore.FieldValue;

const THREADS_COLL = "EtsyMail_Threads";
const AUDIT_COLL   = "EtsyMail_Audit";

function json(statusCode, body) { return { statusCode, headers: CORS, body: JSON.stringify(body) }; }
function bad(msg, code = 400)    { return json(code, { error: msg }); }

async function writeAudit({ threadId, eventType, actor, payload }) {
  await db.collection(AUDIT_COLL).add({
    threadId: threadId || null,
    draftId : null,
    eventType,
    actor   : actor || "system:extension",
    payload : payload || {},
    createdAt: FV.serverTimestamp()
  });
}

function normalize(text = "") {
  return String(text).toLowerCase().replace(/\s+/g, " ").trim();
}

function pickCustomer(participants) {
  if (!Array.isArray(participants)) return null;
  return participants.find(p => p && p.role === "customer") || null;
}

exports.handler = async (event) => {
  if (event.httpMethod === "OPTIONS") return { statusCode: 200, headers: CORS, body: "ok" };
  if (event.httpMethod !== "POST")     return json(405, { error: "Method Not Allowed" });

  const auth = requireExtensionAuth(event);
  if (!auth.ok) return auth.response;

  let body = {};
  try { body = JSON.parse(event.body || "{}"); }
  catch { return bad("Invalid JSON"); }

  const {
    etsyConversationId,
    etsyConversationUrl,
    threadDomHash,
    scrapedAt,
    participants = [],
    subject = null,
    messages = [],
    session = {}
  } = body;

  if (!etsyConversationId) return bad("Missing etsyConversationId");
  if (!Array.isArray(messages)) return bad("messages must be an array");

  const threadId = `etsy_conv_${etsyConversationId}`;
  const tRef = db.collection(THREADS_COLL).doc(threadId);

  try {
    // ─── 1) Load or create the thread doc ───
    const tSnap = await tRef.get();
    const now = FV.serverTimestamp();
    const customer = pickCustomer(participants);

    let threadExisted = tSnap.exists;
    let currentStatus = tSnap.exists ? (tSnap.data().status || null) : null;

    const threadPatch = {
      etsyConversationId,
      etsyConversationUrl: etsyConversationUrl || null,
      lastScrapedDomHash : threadDomHash || null,
      lastSyncedAt       : now,
      updatedAt          : now
    };
    if (subject) threadPatch.subject = subject;
    if (customer) {
      if (customer.name)          threadPatch.customerName = customer.name;
      if (customer.etsyUsername)  threadPatch.etsyUsername = customer.etsyUsername;
      // NEW — buyer metadata useful for M3 customer panel
      if (customer.peopleUrl)     threadPatch.buyerPeopleUrl = customer.peopleUrl;
      if (customer.avatarUrl)     threadPatch.buyerAvatarUrl = customer.avatarUrl;
      if (customer.buyerUserId)   threadPatch.buyerUserId = String(customer.buyerUserId);
      if (typeof customer.isRepeatBuyer === "boolean") {
        threadPatch.buyerIsRepeatBuyer = customer.isRepeatBuyer;
      }
    }

    // Advance status on first successful scrape
    const advanceable = ["detected_from_gmail", "pending_etsy_scrape", null, undefined];
    if (advanceable.includes(currentStatus)) {
      threadPatch.status = "etsy_scraped";
    }

    if (!threadExisted) {
      // Create fresh
      const initial = {
        threadId,
        etsyConversationId,
        etsyConversationUrl : etsyConversationUrl || null,
        gmailMessageId      : null,
        gmailThreadId       : null,
        gmailReceivedAt     : null,
        customerName        : (customer && customer.name) || "Unknown",
        customerEmail       : null,
        etsyUsername        : (customer && customer.etsyUsername) || null,
        linkedOrderId       : null,
        linkedListingIds    : [],
        status              : "etsy_scraped",
        category            : null,
        confidence          : null,
        needsHumanReview    : true,
        aiDraftStatus       : "none",
        latestDraftId       : null,
        lastInboundAt       : null,
        lastOutboundAt      : null,
        lastSyncedAt        : now,
        lastScrapedDomHash  : threadDomHash || null,
        assignedTo          : null,
        tags                : [],
        riskFlags           : [],
        messageCount        : 0,
        unread              : true,
        lastReadAt          : null,
        subject             : subject || null,
        createdAt           : now,
        updatedAt           : now
      };
      await tRef.set(initial, { merge: false });
    } else {
      await tRef.set(threadPatch, { merge: true });
    }

    // ─── 2) Dedupe + upsert messages ───
    // We fetch existing hashes AND their current timestamps so we can UPDATE
    // a stored message's timestamp if the scraper now provides a better one
    // (e.g., scraper v0.3+ extracts real per-message Date: headers that
    // earlier scrapes missed).
    const existingSnap = await tRef.collection("messages").select("contentHash", "timestamp").limit(2000).get();
    const existingByHash = new Map();   // hash → { docId, currentTsMs }
    existingSnap.forEach(d => {
      const data = d.data() || {};
      if (data.contentHash) {
        const currentTsMs = data.timestamp && typeof data.timestamp.toMillis === "function"
          ? data.timestamp.toMillis()
          : null;
        existingByHash.set(data.contentHash, { docId: d.id, currentTsMs });
      }
    });

    let newest_inbound_ms  = null;
    let newest_outbound_ms = null;
    let newestAny_ms       = null;
    const toInsert = [];
    const toUpdate = [];

    // Rough heuristic: a "scrape-time fallback" timestamp is one within a
    // few seconds of scrapedAt. If the existing stored timestamp looks like
    // a fallback AND the new one doesn't, update it.
    const scrapeTimeMs = typeof scrapedAt === "number" ? scrapedAt : Date.now();
    const FALLBACK_WINDOW_MS = 120 * 1000;  // 2 minutes
    function looksLikeFallbackTs(tsMs) {
      return tsMs != null && Math.abs(tsMs - scrapeTimeMs) < FALLBACK_WINDOW_MS;
    }

    for (const m of messages) {
      if (!m || !m.contentHash) continue;

      const direction = m.senderRole === "staff" ? "outbound" : "inbound";
      const ts = typeof m.timestampMs === "number" ? m.timestampMs : null;
      if (ts != null) {
        newestAny_ms = Math.max(newestAny_ms || 0, ts);
        if (direction === "inbound")  newest_inbound_ms  = Math.max(newest_inbound_ms  || 0, ts);
        if (direction === "outbound") newest_outbound_ms = Math.max(newest_outbound_ms || 0, ts);
      }

      const existing = existingByHash.get(m.contentHash);
      if (existing) {
        // Candidate for timestamp update: we have a new ts, it's different,
        // and either we stored nothing or what we stored looks like a fallback.
        if (ts != null) {
          const stored = existing.currentTsMs;
          const storedLooksFallback = looksLikeFallbackTs(stored);
          const newLooksFallback    = looksLikeFallbackTs(ts);
          const storedMissingOrBad  = stored == null || storedLooksFallback;
          if (storedMissingOrBad && !newLooksFallback && stored !== ts) {
            toUpdate.push({
              docId: existing.docId,
              patch: {
                timestamp : admin.firestore.Timestamp.fromMillis(ts),
                updatedAt : now
              }
            });
          }
        }
        continue;   // already exists, don't re-insert
      }

      // Sanitize listing cards — accept only expected fields, drop anything weird
      const listingCards = Array.isArray(m.listingCards)
        ? m.listingCards.map(c => ({
            listingId        : String(c.listingId || ""),
            listingUrl       : String(c.listingUrl || ""),
            title            : String(c.title || ""),
            thumbnailUrl     : String(c.thumbnailUrl || ""),
            priceText        : String(c.priceText || ""),
            originalPriceText: String(c.originalPriceText || ""),
            shippingText    : String(c.shippingText || "")
          })).filter(c => c.listingId && c.listingUrl)
        : [];

      toInsert.push({
        source            : "etsy",
        direction,
        senderName        : m.senderName || "Unknown",
        senderRole        : m.senderRole || "customer",
        timestamp         : ts ? admin.firestore.Timestamp.fromMillis(ts) : now,
        text              : m.text || "",
        normalizedText    : normalize(m.text),
        contentHash       : m.contentHash,
        messageType       : m.messageType || "text",     // "text" | "image" | future types
        imageUrls         : Array.isArray(m.imageUrls) ? m.imageUrls : [],
        thumbnailUrls     : Array.isArray(m.thumbnailUrls) ? m.thumbnailUrls : [],
        listingCards,                                    // NEW: structured Etsy listing previews
        storageImagePaths : [],
        storageMirrorState: Array.isArray(m.imageUrls) && m.imageUrls.length ? "pending" : "none",
        attachmentUrls    : Array.isArray(m.attachmentUrls) ? m.attachmentUrls : [],
        etsyDomSelector   : m.domSelector || null,
        createdAt         : now
      });
    }

    // Write inserts (new messages)
    let writtenCount = 0;
    for (let i = 0; i < toInsert.length; i += 400) {
      const batch = db.batch();
      const chunk = toInsert.slice(i, i + 400);
      for (const m of chunk) {
        const mRef = tRef.collection("messages").doc(`etsy_${m.contentHash}`);
        batch.set(mRef, m, { merge: false });
      }
      await batch.commit();
      writtenCount += chunk.length;
    }

    // Write timestamp updates (existing messages with better timestamps now available)
    let updatedCount = 0;
    for (let i = 0; i < toUpdate.length; i += 400) {
      const batch = db.batch();
      const chunk = toUpdate.slice(i, i + 400);
      for (const u of chunk) {
        const mRef = tRef.collection("messages").doc(u.docId);
        batch.set(mRef, u.patch, { merge: true });
      }
      await batch.commit();
      updatedCount += chunk.length;
    }

    // ─── 3) Update thread tail timestamps + message count ───
    if (writtenCount > 0) {
      const tailPatch = { updatedAt: now };
      tailPatch.messageCount = FV.increment(writtenCount);
      if (newest_inbound_ms != null) {
        tailPatch.lastInboundAt = admin.firestore.Timestamp.fromMillis(newest_inbound_ms);
        tailPatch.unread = true;
      }
      if (newest_outbound_ms != null) {
        tailPatch.lastOutboundAt = admin.firestore.Timestamp.fromMillis(newest_outbound_ms);
      }

      // ─── v1.3: image_attached risk flag ──────────────────────────
      // If any of the newly-inserted messages carry images, mark the
      // thread so the inbox UI's "with image" filter can find it.
      // arrayUnion is idempotent — re-marking an already-marked thread
      // is a no-op. We don't bother removing the flag if all images
      // get deleted later because that's exceedingly rare and the
      // UX cost of a stale flag is low.
      const anyImageMessage = toInsert.some(m =>
        (Array.isArray(m.imageUrls) && m.imageUrls.length > 0) ||
        m.messageType === "image"
      );
      if (anyImageMessage) {
        tailPatch.riskFlags = FV.arrayUnion("image_attached");
      }

      // ─── v1.3: searchableText denormalized field ────────────────
      // Maintains a lowercased, normalized concatenation of:
      //   - thread metadata (customer name, etsy username, subject)
      //   - the message bodies of recent messages (incremental: we
      //     append newly-inserted message text to the existing field
      //     and truncate from the front to keep the most recent ~6KB)
      //
      // This is what etsyMailSearch.js queries for substring matches.
      // Keeping it on the thread doc means search is a single
      // collection scan, no subcollection joins.
      //
      // The 6KB cap protects against runaway growth — a thread with
      // hundreds of messages would otherwise grow unbounded. Recent
      // messages are most relevant to search, so dropping oldest
      // first is the right trade-off.
      const newTextChunks = toInsert
        .map(m => normalize(m.text))
        .filter(Boolean);

      // v1.10: searchableText must exist on every thread for the inbox
      // to search message bodies. Pre-v1.10 it was only built/updated
      // when new messages arrived — threads scraped once before this
      // logic existed, OR threads that haven't received new activity
      // since v1.3, never got the field. Search would silently miss
      // them.
      //
      // Now: rebuild searchableText whenever EITHER:
      //   (a) new messages arrived (incremental — append + truncate, fast)
      //   (b) the field is missing on the existing thread doc
      //       (one-time backfill from the messages subcollection)
      //
      // Case (b) is a single subcollection read per thread, runs at most
      // once per thread (next scrape sees the field populated and skips).
      const prevSnap = (tSnap && tSnap.data && tSnap.data()) || {};
      const hasField = !!prevSnap.searchableText;
      const SEARCHABLE_MAX = 6000;

      const buildMeta = (truncatedBody) => {
        const metaParts = [
          threadPatch.customerName  || prevSnap.customerName  || "",
          threadPatch.etsyUsername  || prevSnap.etsyUsername  || "",
          threadPatch.subject       || prevSnap.subject       || "",
          prevSnap.linkedOrderId    || ""
        ].map(s => normalize(String(s))).filter(Boolean);
        return (metaParts.join(" ") + " " + truncatedBody).trim();
      };

      if (newTextChunks.length > 0) {
        // Case (a): incremental update
        const prevMessageText = prevSnap.searchableMessageText || "";
        const combined = (prevMessageText + " " + newTextChunks.join(" ")).trim();
        const truncated = combined.length > SEARCHABLE_MAX
          ? combined.slice(combined.length - SEARCHABLE_MAX)
          : combined;
        tailPatch.searchableMessageText = truncated;
        tailPatch.searchableText = buildMeta(truncated);
      } else if (!hasField) {
        // Case (b): one-time backfill. Read the existing messages
        // subcollection (most recent 50, plenty for the 6KB cap) and
        // build the field from scratch. After this scrape the field
        // exists and the snapshot returns to incremental updates.
        try {
          const msgsSnap = await tRef.collection("messages")
            .orderBy("timestamp", "desc")
            .limit(50)
            .get();
          const allText = msgsSnap.docs
            .map(d => normalize((d.data() || {}).text || ""))
            .filter(Boolean)
            .reverse()                // back to chronological order
            .join(" ");
          const truncated = allText.length > SEARCHABLE_MAX
            ? allText.slice(allText.length - SEARCHABLE_MAX)
            : allText;
          tailPatch.searchableMessageText = truncated;
          tailPatch.searchableText = buildMeta(truncated);
        } catch (backfillErr) {
          console.warn("snapshot: searchableText backfill failed for", threadId, "—", backfillErr.message);
          // Fall back to metadata-only so at least metadata search works
          tailPatch.searchableText = buildMeta("");
        }
      }

      await tRef.set(tailPatch, { merge: true });
    }

    // ─── 4) Session / login-required detection ───
    // v1.2: This MUST run BEFORE the auto-pipeline trigger. If Etsy is
    // logged out, we know the send pipeline can't deliver — pushing the
    // thread to Needs Review and skipping the AI call avoids burning
    // an Opus call for a draft we can't actually send.
    //
    // Status: route to pending_human_review (the v1.1+ visible folder).
    // The legacy hold_login_required is no longer in the rail.
    const etsyLoggedOut = session && session.etsyLoggedIn === false;
    if (etsyLoggedOut) {
      await tRef.set({
        status   : "pending_human_review",
        updatedAt: now
      }, { merge: true });
      await writeAudit({
        threadId,
        eventType: "held",
        actor: "system:extension",
        payload: { reason: "etsy_login_required" }
      });
    }

    // ─── 5) Trigger auto-reply pipeline ─────────────────────────
    // If a new inbound message landed AND the Etsy session is logged in,
    // fire the auto-reply pipeline as a Netlify -background function.
    // It:
    //   - generates an AI draft via etsyMailDraftReply
    //   - reads the AI's self-rated confidence
    //   - applies deterministic veto rules (refund, cancel, legal, etc.)
    //   - either auto-enqueues for send (high confidence + no vetoes)
    //     OR routes the thread to "Needs review" (low confidence,
    //     vetoed, or kill-switch active)
    //
    // Why -background: the AI draft step takes 10-60 seconds with Opus
    // 4.7 + tool calls. Netlify's standard 10s function timeout is too
    // tight; the -background suffix unlocks 15 minutes and decouples
    // the response from completion (Netlify returns 202 immediately).
    //
    // We AWAIT the fetch (with a 5-second AbortSignal) so the snapshot
    // function doesn't return before the trigger has been dispatched.
    // Netlify -background returns 202 within ~50-200ms typically; the
    // 5s timeout is generous safety. Any error is swallowed — the
    // scrape ingest must succeed independently of auto-reply.
    //
    // The on/off flag and confidence threshold live in
    // EtsyMail_Config/autoPipeline (read by the pipeline itself, cached
    // 15s). Snapshot stays dumb — every new inbound triggers, and the
    // pipeline decides whether to act.
    const hasNewInbound = writtenCount > 0 && newest_inbound_ms != null;
    if (hasNewInbound && !etsyLoggedOut) {
      const baseUrl = process.env.URL
                   || process.env.DEPLOY_URL
                   || "http://localhost:8888";
      const headers = { "Content-Type": "application/json" };
      if (process.env.ETSYMAIL_EXTENSION_SECRET) {
        headers["X-EtsyMail-Secret"] = process.env.ETSYMAIL_EXTENSION_SECRET;
      }
      // AbortSignal.timeout requires Node 18+. All Netlify Functions
      // run on Node 18+ by default, but fall back gracefully if absent
      // (older bundlers can be missing the static method).
      const signal = (typeof AbortSignal !== "undefined" && AbortSignal.timeout)
        ? AbortSignal.timeout(5000)
        : undefined;
      try {
        const res = await fetch(`${baseUrl}/.netlify/functions/etsyMailAutoPipeline-background`, {
          method : "POST",
          headers,
          body   : JSON.stringify({
            threadId,
            employeeName: "system:auto-pipeline"
          }),
          signal
        });
        // 202 = Netlify accepted the background invocation. 200 is fine
        // too (e.g., if someone runs the function synchronously in dev).
        // Anything else is a smoke signal.
        if (res.status !== 202 && !res.ok) {
          console.warn("autoPipeline trigger non-2xx:", res.status, threadId);
        }
      } catch (e) {
        console.warn("autoPipeline trigger failed:", e.message, threadId);
        // Don't propagate — scrape ingest must succeed independently.
      }
    }

    // ─── 5) Audit ───
    await writeAudit({
      threadId,
      eventType: threadExisted ? "scrape_succeeded" : "thread_created_from_scrape",
      actor    : "system:extension",
      payload  : {
        newMessageCount      : writtenCount,
        updatedMessageCount  : updatedCount,
        totalMessagesScraped : messages.length,
        threadDomHash        : threadDomHash || null,
        scrapedAt            : scrapedAt || null
      }
    });

    // ─── 6) Collect image mirror jobs (if any) ───
    // Return a list of {messageId, imageUrls} pairs the extension can pass
    // to etsyMailMirrorImage, one call per image. Keeps Storage uploads out
    // of this hot path.
    const imagesToMirror = [];
    for (const m of toInsert) {
      if (!m.imageUrls || !m.imageUrls.length) continue;
      imagesToMirror.push({
        messageDocId: `etsy_${m.contentHash}`,
        imageUrls   : m.imageUrls
      });
    }

    return json(200, {
      success         : true,
      threadId,
      threadExisted,
      newMessages     : writtenCount,
      updatedMessages : updatedCount,
      totalScanned    : messages.length,
      imagesToMirror
    });

  } catch (err) {
    console.error("etsyMailSnapshot error:", err);
    await writeAudit({
      threadId: threadId,
      eventType: "scrape_failed",
      actor: "system:extension",
      payload: { error: err.message }
    }).catch(()=>{});
    return json(500, { error: err.message || String(err) });
  }
};
