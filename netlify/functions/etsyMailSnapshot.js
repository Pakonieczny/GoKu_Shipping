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

    // ─── 2) Dedupe + write new messages ───
    // Pull existing message contentHashes so we only write new ones.
    const existingSnap = await tRef.collection("messages").select("contentHash").limit(2000).get();
    const existingHashes = new Set();
    existingSnap.forEach(d => {
      const h = (d.data() || {}).contentHash;
      if (h) existingHashes.add(h);
    });

    let newest_inbound_ms = null;
    let newest_outbound_ms = null;
    let newestAny_ms = null;
    const toWrite = [];

    for (const m of messages) {
      if (!m || !m.contentHash) continue;
      if (existingHashes.has(m.contentHash)) continue;

      const direction = m.senderRole === "staff" ? "outbound" : "inbound";
      const ts = typeof m.timestampMs === "number" ? m.timestampMs : null;
      if (ts != null) {
        newestAny_ms = Math.max(newestAny_ms || 0, ts);
        if (direction === "inbound")  newest_inbound_ms  = Math.max(newest_inbound_ms  || 0, ts);
        if (direction === "outbound") newest_outbound_ms = Math.max(newest_outbound_ms || 0, ts);
      }

      toWrite.push({
        source            : "etsy",
        direction,
        senderName        : m.senderName || "Unknown",
        senderRole        : m.senderRole || "customer",
        timestamp         : ts ? admin.firestore.Timestamp.fromMillis(ts) : now,
        text              : m.text || "",
        normalizedText    : normalize(m.text),
        contentHash       : m.contentHash,
        imageUrls         : Array.isArray(m.imageUrls) ? m.imageUrls : [],
        storageImagePaths : [],                                    // filled later by image mirror
        storageMirrorState: Array.isArray(m.imageUrls) && m.imageUrls.length ? "pending" : "none",
        attachmentUrls    : Array.isArray(m.attachmentUrls) ? m.attachmentUrls : [],
        etsyDomSelector   : m.domSelector || null,
        createdAt         : now
      });
    }

    // Batch-write new messages (chunks of 400 to stay under Firestore batch limit)
    let writtenCount = 0;
    for (let i = 0; i < toWrite.length; i += 400) {
      const batch = db.batch();
      const chunk = toWrite.slice(i, i + 400);
      for (const m of chunk) {
        const mRef = tRef.collection("messages").doc(`etsy_${m.contentHash}`);
        batch.set(mRef, m, { merge: false });
      }
      await batch.commit();
      writtenCount += chunk.length;
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
    for (const m of toWrite) {
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
