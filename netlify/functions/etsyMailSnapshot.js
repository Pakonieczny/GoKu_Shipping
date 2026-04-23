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
      await tRef.set(tailPatch, { merge: true });
    }

    // ─── 4) Session / login-required detection ───
    if (session && session.etsyLoggedIn === false) {
      await tRef.set({ status: "hold_login_required", updatedAt: now }, { merge: true });
      await writeAudit({
        threadId,
        eventType: "held",
        actor: "system:extension",
        payload: { reason: "etsy_login_required" }
      });
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
