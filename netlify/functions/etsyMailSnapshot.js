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
const align = require("./_etsyMailThreadAlign");
const { removeCopies, loadStoredForAlign, sweepPage } = require("./_etsyMailMessageCopies");
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

/**
 * Inbox layouts: is the customer waiting on us, and the row previews.
 * Pure (no Firestore), so tests can call it directly.
 *
 *   prev             the thread doc as read before this scrape ({} if new);
 *                    timestamps may be Firestore Timestamps or plain ms
 *   newestInboundMs  newest customer message time in this scrape (or null)
 *   newestOutboundMs newest staff message time in this scrape (or null)
 *   inboundTs        every customer message time in this scrape
 *   newestInText     text of the newest customer message in this scrape
 *   newestOutText    text of the newest staff message in this scrape
 *
 * Returns { awaitingReplySinceMs?, clearAwaiting?, lastInboundPreview?,
 * lastOutboundPreview? }. awaitingReplySince marks the first customer
 * message we have not answered: set when the customer spoke last, cleared
 * when a staff reply is scraped (this includes replies typed on Etsy).
 */
const AWAIT_SKEW_MS    = 120000;          // lastOperatorReplyAt is server time; message times come from Etsy's page
const AWAIT_MAX_AGE_MS = 30 * 86400000;   // don't resurrect old back-filled conversations
function awaitMsOf(v) {
  if (v && typeof v.toMillis === "function") return v.toMillis();
  if (typeof v === "number" && Number.isFinite(v)) return v;
  return 0;
}
function previewText(s) {
  return String(s == null ? "" : s).replace(/\s+/g, " ").trim().slice(0, 160);
}
function computeAwaitingState({ prev = {}, newestInboundMs = null, newestOutboundMs = null,
                                inboundTs = [], newestInText = null, newestOutText = null,
                                nowMs = Date.now() } = {}) {
  const p = prev || {};
  const out = {};
  const lastOurs   = Math.max(awaitMsOf(p.lastOutboundAt), newestOutboundMs || 0, awaitMsOf(p.lastOperatorReplyAt) - AWAIT_SKEW_MS);
  const lastTheirs = Math.max(awaitMsOf(p.lastInboundAt), newestInboundMs || 0);
  if (lastTheirs > 0 && lastTheirs >= lastOurs) {           // customer spoke last (a tie counts as waiting)
    // An archived thread was handled: only a customer message we had not
    // seen before reopens it, never a re-scrape of the old ones.
    const archived = p.status === "archived";
    const seenIn   = awaitMsOf(p.lastInboundAt);
    const open     = (Array.isArray(inboundTs) ? inboundTs : [])
      .filter(t => typeof t === "number" && t >= lastOurs && (!archived || t > seenIn));
    // A scrape may hold only the newest messages: the customer message
    // already stored as the latest one is unanswered too.
    if (!archived && seenIn > 0 && seenIn >= lastOurs) open.push(seenIn);
    const firstOpen = open.length ? open.reduce((a, b) => Math.min(a, b)) : (archived ? 0 : lastTheirs);
    const cur = awaitMsOf(p.awaitingReplySince);
    // Old back-filled conversations are left alone: the customer's newest
    // message must be recent.
    if (firstOpen > 0 && (!cur || cur < lastOurs) && nowMs - lastTheirs < AWAIT_MAX_AGE_MS) {
      out.awaitingReplySinceMs = firstOpen;
    }
  } else if (p.awaitingReplySince) {
    out.clearAwaiting = true;                               // a staff reply was scraped
  }
  if (newestInText != null && newestInboundMs != null) out.lastInboundPreview = previewText(newestInText);
  if (newestOutText != null && newestOutboundMs != null) out.lastOutboundPreview = previewText(newestOutText);
  return out;
}
exports.computeAwaitingState = computeAwaitingState;

function pickCustomer(participants) {
  if (!Array.isArray(participants)) return null;
  return participants.find(p => p && p.role === "customer") || null;
}

/**
 * v3.1 — Defensive decoder for JSON-stringified Unicode escape sequences.
 *
 * Some upstream code path (in the Chrome scraper, by the look of the
 * field shape — names + subjects only) is calling JSON.stringify on
 * scraped strings and storing the result, which turns "Caitríona" into
 * the literal six-character string `Caitr\u00edona` (a real backslash,
 * then "u00ed", then "ona"). Operators see the literal escape sequence
 * in the inbox UI instead of the accented character.
 *
 * We can't reach into the extension to fix it at the source, so we
 * decode defensively here at the ingest boundary. Behavior:
 *
 *   - Input has no backslash-u sequences → returned unchanged.
 *   - Each `\uXXXX` matched is replaced with the actual character it
 *     represents, BUT only when the resulting code point is >= 0x80
 *     (non-ASCII). This avoids "decoding" sequences that are real
 *     backslash content (`a literal \u0041` from a documentation
 *     string, etc.) and only fixes the bug we're actually seeing,
 *     which is non-ASCII characters that got round-tripped through
 *     JSON.stringify.
 *   - Surrogate pairs (`\uD83D\uDE00` = 😀) are joined and decoded as a
 *     single code point so emoji and astral-plane characters survive.
 *   - Idempotent: running it twice on a clean string is a no-op.
 *
 * Returns the input unchanged for non-string types, null, or undefined.
 */
function unmangleEscapedUnicode(s) {
  if (typeof s !== "string" || s.length === 0) return s;
  // Fast path — no `\u` sequences, nothing to do.
  if (s.indexOf("\\u") === -1) return s;

  // Two-pass approach:
  //   Pass 1 — surrogate pairs `\uHHHH\uHHHH` where the first is a
  //            high surrogate (D800-DBFF) and the second a low
  //            surrogate (DC00-DFFF). These represent astral-plane
  //            code points (e.g. emoji) and must be decoded together.
  //   Pass 2 — single `\uHHHH` escapes for non-ASCII BMP characters.
  let out = s.replace(
    /\\u([dD][89aAbB][0-9a-fA-F]{2})\\u([dD][c-fC-F][0-9a-fA-F]{2})/g,
    (_m, hi, lo) => {
      const high = parseInt(hi, 16);
      const low  = parseInt(lo, 16);
      try {
        return String.fromCodePoint(((high - 0xD800) << 10) + (low - 0xDC00) + 0x10000);
      } catch {
        return _m;   // leave as-is on any parse failure
      }
    }
  );
  out = out.replace(/\\u([0-9a-fA-F]{4})/g, (m, hex) => {
    const cp = parseInt(hex, 16);
    // Only decode non-ASCII. Below 0x80 we leave the literal alone —
    // it might be intentional content (e.g. a docstring showing JSON
    // syntax). Empirically the bug only produces non-ASCII escapes
    // because ASCII characters don't get JSON-escaped in the first place.
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
 * Walk an object/array and apply unmangleEscapedUnicode to every string
 * field. Used to clean the `participants` array before storing.
 * Recursion-safe (won't follow circular references — uses a Set).
 */
function unmangleObjectStrings(obj, _seen) {
  if (obj == null) return obj;
  if (typeof obj === "string") return unmangleEscapedUnicode(obj);
  if (typeof obj !== "object") return obj;
  if (!_seen) _seen = new Set();
  if (_seen.has(obj)) return obj;
  _seen.add(obj);
  if (Array.isArray(obj)) {
    return obj.map(v => unmangleObjectStrings(v, _seen));
  }
  const out = {};
  for (const k of Object.keys(obj)) {
    out[k] = unmangleObjectStrings(obj[k], _seen);
  }
  return out;
}

// Etsy page-change alarm. A scrape that reads no messages, only blank
// ones, none with a time, or nothing matching the stored copy of a known
// thread means Etsy changed its page (or signed the extension out).
// Each scrape updates EtsyMail_Config/scrapeHealth; the inbox shows a red
// line after three bad scrapes in a row.
function scrapeProblem(body, messages, ctx) {
  const d = body.diagnostics || {};
  if (body.session && body.session.etsyLoggedIn === false) return "Etsy signed the extension out";
  const real = messages.filter(Boolean);
  if (!real.length) {
    const why = Array.isArray(d.domMissReasons) && d.domMissReasons[0];
    return why ? String(why).slice(0, 120) : "No messages read from the conversation page";
  }
  const blank = real.filter(m => !String(m.text || "").trim()
    && !(Array.isArray(m.imageUrls) && m.imageUrls.length) && m.messageType !== "image").length;
  if (blank * 2 > real.length) return "Messages came in without their text";
  if (real.length >= 3 && real.every(m => typeof m.timestampMs !== "number")) return "Messages came in without times";
  if (ctx && ctx.storedCount >= 3 && real.length >= 3 && ctx.matched === 0) return "Scraped messages matched none of the stored ones";
  return null;
}

async function recordScrapeHealth(problem, threadId) {
  const now = Date.now();
  const patch = problem
    ? { lastAtMs: now, lastBadAtMs: now, lastBadReason: problem, lastBadThreadId: threadId || null, consecutiveBad: FV.increment(1) }
    : { lastAtMs: now, lastOkAtMs: now, consecutiveBad: 0 };
  await db.collection("EtsyMail_Config").doc("scrapeHealth").set(patch, { merge: true });
}

exports.handler = async (event) => {
  if (event.httpMethod === "OPTIONS") return { statusCode: 200, headers: CORS, body: "ok" };
  if (event.httpMethod !== "POST")     return json(405, { error: "Method Not Allowed" });

  const auth = requireExtensionAuth(event);
  if (!auth.ok) return auth.response;

  let body = {};
  try { body = JSON.parse(event.body || "{}"); }
  catch { return bad("Invalid JSON"); }

  // v3.1 — Defensive unmangle of every string field BEFORE we destructure.
  // The Chrome scraper has a known bug where some non-ASCII customer
  // names + subjects arrive as literal `\uXXXX` escape sequences (six
  // characters) instead of the actual Unicode character (one character).
  // unmangleObjectStrings walks the body recursively and decodes any
  // such sequence found in any string field. Idempotent on clean data.
  // See unmangleEscapedUnicode above for the full rationale.
  body = unmangleObjectStrings(body);

  // v3.2 — threadExists lookup. Lets the Chrome scraper ask, before it
  // commits to scrolling through a long Etsy conversation, "do we
  // already have this thread?" If yes, the scraper skips Phase 1
  // (which triggers Etsy's load-more to pull the entire history) and
  // sends only the bubbles that are visible without any scrolling.
  // Those visible bubbles always include the newest message (Etsy's
  // conversation page opens at the bottom of the thread). The snapshot
  // ingest's content-based dedup (senderRole + normalizedText +
  // tsMinute) handles any overlap between visible-already-known
  // bubbles and genuinely new ones, even if multiple new messages
  // arrived between scrapes.
  //
  // For a fresh thread we've never seen, this op returns exists:false
  // and the scraper falls back to its existing full Phase 1 scroll.
  //
  // v3.4 — Also return exists:false when the thread DOC exists but has
  // no messages (messageCount === 0 or null). This handles the "clean
  // rescrape" workflow where an operator wiped a polluted thread's
  // messages subcollection via deleteSub but the parent doc remained.
  // Without this, the scraper would see exists:true, do an incremental
  // scrape, and only capture the visible bottom of the thread — leaving
  // the upper history permanently missing.
  //
  // Returns: { exists }
  if (body.op === "threadExists") {
    const convId = body.etsyConversationId;
    if (!convId) return bad("threadExists requires etsyConversationId");
    const threadId = `etsy_conv_${convId}`;
    try {
      const tSnap = await db.collection(THREADS_COLL).doc(threadId).get();
      if (!tSnap.exists) return json(200, { exists: false });
      const data = tSnap.data() || {};
      const mc = typeof data.messageCount === "number" ? data.messageCount : null;
      // Empty thread → treat as not existing so the scraper does a full pass
      if (mc === 0 || mc === null) {
        return json(200, { exists: false, reason: "thread_doc_present_but_empty" });
      }
      return json(200, { exists: true });
    } catch (err) {
      console.error("threadExists failed:", err);
      // Fail open so the scraper falls back to its full-scrape path
      // rather than blocking on a transient backend error.
      return json(200, { exists: false, error: err.message });
    }
  }

  // Clean copies left by the old time-based dedupe, a page of threads per
  // call: { op: "dedupeSweep", startAfter, limit, dryRun }. Returns the
  // cursor for the next page and what was (or would be) removed.
  if (body.op === "dedupeSweep") {
    const limit = Math.max(1, Math.min(40, Number(body.limit) || 20));
    const dryRun = body.dryRun !== false;
    const r = await sweepPage({ startAfter: body.startAfter || null, limit, dryRun, deadline: Date.now() + 20000 });
    return json(200, { success: true, dryRun, ...r });
  }

  const {
    etsyConversationId,
    etsyConversationUrl,
    threadDomHash,
    scrapedAt,
    participants = [],
    subject = null,
    messages = [],
    session = {},
    // v0.9.38 — Etsy's new in-thread heading. Optional; older scrapers
    // and odd thread types (no related order) may omit it.
    conversationHeading = null
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

    // v0.9.38 — Persist Etsy's conversation-heading info if the scraper
    // surfaced it. Per-field rather than blob so missing fields don't
    // clobber previously-captured ones (e.g. if Etsy briefly removes
    // the order link from the heading, we keep the last-seen orderId).
    if (conversationHeading && typeof conversationHeading === "object") {
      if (conversationHeading.orderId) {
        threadPatch.etsyOrderId = String(conversationHeading.orderId);
      }
      if (conversationHeading.categoryBadge) {
        threadPatch.etsyHeadingBadge = String(conversationHeading.categoryBadge);
      }
      if (conversationHeading.title) {
        threadPatch.etsyHeadingTitle = String(conversationHeading.title);
      }
      if (conversationHeading.viewOrderUrl) {
        threadPatch.etsyViewOrderUrl = String(conversationHeading.viewOrderUrl);
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
        updatedAt           : now,
        // v4.3.16 — Buyer metadata. Previously these fields were only
        // written via threadPatch (the `merge: true` path for existing
        // threads), so on a FIRST scrape — the precise moment when we
        // most need them — they were dropped. Now mirror them into
        // the initial doc so they're present from creation.
        buyerUserId         : (customer && customer.buyerUserId) ? String(customer.buyerUserId) : null,
        buyerPeopleUrl      : (customer && customer.peopleUrl) || null,
        buyerAvatarUrl      : (customer && customer.avatarUrl) || null,
        buyerIsRepeatBuyer  : !!(customer && customer.isRepeatBuyer),
        // v0.9.38 — Mirror Etsy's conversation-heading metadata into
        // initial-create too so first-scrape threads carry it. Older
        // scrapers send conversationHeading=null and these stay null.
        etsyOrderId         : (conversationHeading && conversationHeading.orderId)
                              ? String(conversationHeading.orderId) : null,
        etsyHeadingBadge    : (conversationHeading && conversationHeading.categoryBadge) || null,
        etsyHeadingTitle    : (conversationHeading && conversationHeading.title) || null,
        etsyViewOrderUrl    : (conversationHeading && conversationHeading.viewOrderUrl) || null
      };
      await tRef.set(initial, { merge: false });
    } else {
      await tRef.set(threadPatch, { merge: true });
    }

    // ─── 2) Match the scrape against what is stored, then insert ───
    // By message order and content, never by time (see
    // _etsyMailThreadAlign.js): the scraper stamps the top of a partial
    // scrape with the next day's time, and the old minute-based dedupe
    // stored those messages again on every scrape. Only scraped messages
    // (source "etsy") are matched: our own "just sent" stand-in is one doc
    // per thread, rewritten by every send, so Etsy's copy must be stored.
    const stored = await loadStoredForAlign(tRef);
    const existingIds = stored.ids;
    const storedEtsy = stored.etsy;
    const storedOther = stored.other;

    const scrapeMode = (body.diagnostics && body.diagnostics.scrapeMode) || null;
    const incoming = messages.map(m => ({
      fp  : m ? align.fingerprint(m) : "",
      tsMs: m && typeof m.timestampMs === "number" ? m.timestampMs : null
    }));
    const scrapeTimeMs = typeof scrapedAt === "number" ? scrapedAt : Date.now();
    const aligned = align.alignScrape(storedEtsy, incoming, { scrapedAt: scrapeTimeMs });
    const scrapeIssue = scrapeProblem(body, messages, {
      storedCount: threadExisted ? storedEtsy.length : 0,
      matched: aligned.matchOf.filter(j => j >= 0).length
    });

    // Copies stored by the old dedupe: proven by this scrape, or found by
    // the pattern the old bug left (findLegacyDuplicates).
    const dupIds = new Set(aligned.duplicates);
    try {
      for (const id of align.findLegacyDuplicates(storedEtsy, storedOther)) dupIds.add(id);
    } catch (e) { console.warn("[snapshot] legacy duplicate scan skipped:", e.message); }

    let newest_inbound_ms  = null;
    let newest_outbound_ms = null;
    let newestAny_ms       = null;
    const toInsert = [];
    const toUpdate = [];
    // Inbox layouts: customer message times and newest texts in this scrape
    // (see computeAwaitingState).
    const inboundTs = [];
    let newestInText = null, newestInTs = -1, newestOutText = null, newestOutTs = -1;
    const usedIds = new Set();

    for (let i = 0; i < messages.length; i++) {
      const m = messages[i];
      if (!m || !incoming[i].fp) continue;

      const direction = m.senderRole === "staff" ? "outbound" : "inbound";
      const j = aligned.matchOf[i];
      // A matched message keeps its stored time; a new one gets a time
      // between its neighbours.
      const ts = j >= 0 ? aligned.sorted[j].tsMs : aligned.newTs[i];
      if (ts != null) {
        newestAny_ms = Math.max(newestAny_ms || 0, ts);
        if (direction === "inbound")  newest_inbound_ms  = Math.max(newest_inbound_ms  || 0, ts);
        if (direction === "outbound") newest_outbound_ms = Math.max(newest_outbound_ms || 0, ts);
        try {
          const shown = m.text || ((Array.isArray(m.imageUrls) && m.imageUrls.length) || m.messageType === "image" ? "Sent a photo" : "");
          if (direction === "inbound") {
            inboundTs.push(ts);
            if (ts >= newestInTs) { newestInTs = ts; newestInText = shown; }
          } else if (ts >= newestOutTs) { newestOutTs = ts; newestOutText = shown; }
        } catch (_) { /* previews are optional */ }
      }
      if (j >= 0) continue;   // already stored

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

      // The scraper's hash counts positions inside this scrape, so a partial
      // scrape can repeat the id of an older message: never overwrite one.
      const baseId = `etsy_${String(m.contentHash || require("crypto").createHash("sha1").update(incoming[i].fp).digest("hex")).replace(/\//g, "_")}`;
      let docId = baseId, k = 2;
      while (existingIds.has(docId) || usedIds.has(docId)) docId = `${baseId}_${k++}`;
      usedIds.add(docId);

      toInsert.push({
        docId,
        source            : "etsy",
        direction,
        senderName        : m.senderName || "Unknown",
        senderRole        : m.senderRole || "customer",
        timestamp         : admin.firestore.Timestamp.fromMillis(ts != null ? ts : scrapeTimeMs),
        text              : m.text || "",
        normalizedText    : normalize(m.text),
        contentHash       : m.contentHash || null,
        messageType       : m.messageType || "text",     // "text" | "image" | future types
        imageUrls         : Array.isArray(m.imageUrls) ? m.imageUrls : [],
        thumbnailUrls     : Array.isArray(m.thumbnailUrls) ? m.thumbnailUrls : [],
        listingCards,                                    // NEW: structured Etsy listing previews
        storageImagePaths : [],
        storageMirrorState: Array.isArray(m.imageUrls) && m.imageUrls.length ? "pending" : "none",
        attachmentUrls    : Array.isArray(m.attachmentUrls) ? m.attachmentUrls : [],
        etsyDomSelector   : m.domSelector || null,
        // Time read off the page vs placed between its neighbours.
        timestampSource   : (ts != null && ts === incoming[i].tsMs) ? "page" : "placed",
        createdAt         : now
      });
    }

    // Write inserts (new messages)
    let writtenCount = 0;
    for (let i = 0; i < toInsert.length; i += 400) {
      const batch = db.batch();
      const chunk = toInsert.slice(i, i + 400);
      for (const m of chunk) {
        const { docId, ...data } = m;
        batch.set(tRef.collection("messages").doc(docId), data, { merge: false });
      }
      await batch.commit();
      writtenCount += chunk.length;
    }
    const updatedCount = toUpdate.length;

    // Remove the extra copies. Each is kept in EtsyMail_MessageArchive first,
    // so nothing is lost if one was a real repeat.
    let duplicatesRemoved = 0;
    try { duplicatesRemoved = await removeCopies(tRef, threadId, Array.from(dupIds)); }
    catch (e) { console.warn("[snapshot] duplicate cleanup skipped:", e.message); }
    // A partial scrape that shares nothing with what is stored: messages
    // above the visible part may be missing. The extension answers with a
    // full scrape.
    const needsFullScrape = aligned.gap && scrapeMode !== "full";

    // ─── 3) Update thread tail timestamps + message count ───
    if (writtenCount > 0) {
      const tailPatch = { updatedAt: now };
      tailPatch.messageCount = FV.increment(writtenCount);
      if (newest_inbound_ms != null) {
        tailPatch.lastInboundAt = admin.firestore.Timestamp.fromMillis(newest_inbound_ms);
      }
      // Unread only when a customer message we had not stored arrives.
      if (toInsert.some(m => m.direction === "inbound")) tailPatch.unread = true;
      if (newest_outbound_ms != null) {
        tailPatch.lastOutboundAt = admin.firestore.Timestamp.fromMillis(newest_outbound_ms);
      }

      // Inbox layouts: waiting state + row previews, in this same write.
      // Guarded so a bug here can never stop the tail patch itself.
      try {
        const st = computeAwaitingState({
          prev            : tSnap.exists ? (tSnap.data() || {}) : {},
          newestInboundMs : newest_inbound_ms,
          newestOutboundMs: newest_outbound_ms,
          inboundTs,
          newestInText,
          newestOutText,
          nowMs           : Date.now()
        });
        if (st.awaitingReplySinceMs) tailPatch.awaitingReplySince = admin.firestore.Timestamp.fromMillis(st.awaitingReplySinceMs);
        else if (st.clearAwaiting)   tailPatch.awaitingReplySince = FV.delete();
        if (st.lastInboundPreview  != null) tailPatch.lastInboundPreview  = st.lastInboundPreview;
        if (st.lastOutboundPreview != null) tailPatch.lastOutboundPreview = st.lastOutboundPreview;
      } catch (e) { console.warn("[snapshot] awaiting/preview calc skipped:", e.message); }

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

      // Charm Sorter questions (_etsyMailOrderLink.js): the customer's reply reaches the sorter in
      // the same moment it reaches the inbox. Best-effort and time-boxed; never fails the scrape.
      try {
        const orderLink = require("./_etsyMailOrderLink");
        const threadNow = Object.assign({}, tSnap.exists ? tSnap.data() : {}, threadPatch);
        const fresh = toInsert.map(m => ({
          id        : m.docId,
          direction : m.direction,
          senderName: m.senderName,
          tsMs      : m.timestamp && typeof m.timestamp.toMillis === "function" ? m.timestamp.toMillis() : Date.now(),
          text      : m.text,
          hasImages : m.imageUrls.length > 0
        }));
        await Promise.race([
          orderLink.onThreadMessages(threadId, threadNow, fresh),
          new Promise(resolve => setTimeout(resolve, 4000))
        ]);
      } catch (e) {
        console.warn("orderLink hook failed (non-fatal):", e.message);
      }
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
    // Why -background: the AI draft step takes 10-60 seconds with Sonnet
    // 4.6 + tool calls. Netlify's standard 10s function timeout is too
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
    const hasNewInbound = toInsert.some(m => m.direction === "inbound");
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
        duplicatesRemoved,
        needsFullScrape,
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
        messageDocId: m.docId,
        imageUrls   : m.imageUrls
      });
    }

    // ─── 7) Trigger per-buyer order sync (v4.4.1 — fire-and-forget) ───
    // Closes the gap where manual scrapes left brand-new customers (or
    // recently-active ones) without an EtsyMail_Customers doc until the
    // next scheduled etsyMailSync cron. Triggering directly off the
    // snapshot here makes manual and auto-pipeline paths equivalent:
    // both refresh the customer's order data immediately after a scrape.
    //
    // v4.4.1 — Also pass receiptId+threadId so sync-background's new
    // targeted-hydrate path (ensureReceiptMirroredById) can pull a
    // specific receipt directly from Etsy if it's not yet in the mirror.
    // Without this, a help-request thread on a brand-new order ends up
    // with "No purchase history" in the customer panel because:
    //   - the mirror cron hasn't picked the receipt up yet, AND
    //   - the buyer-sync's mirror query returns 0 results, AND
    //   - the customer doc gets written with orderCount=0 (or worse, not
    //     at all when buyerUserId never came through on the scrape).
    // Passing receiptId resolves buyerUserId from the receipt itself if
    // the scrape didn't capture it, and guarantees the specific order
    // the customer is writing about is present in the mirror before the
    // aggregation runs.
    //
    // Async-invoke into etsyMailSync-background via Netlify's background
    // dispatch URL. We do NOT await — order sync is independent of the
    // snapshot's primary responsibility (saving messages) and shouldn't
    // block the response. Errors are logged but never bubbled up.
    const buyerForSync = (customer && customer.buyerUserId) ? String(customer.buyerUserId) : null;

    // Pull a structured order ID from the scraped conversation heading
    // (Etsy's "Help request" / "Help with order n.°" banner). Falls back
    // to the field that just got written to the thread doc.
    const receiptForSync =
         (body && body.conversationHeading && body.conversationHeading.orderId)
      || threadPatch.etsyOrderId
      || null;
    const receiptIdValid = receiptForSync && /^\d+$/.test(String(receiptForSync))
      ? String(receiptForSync) : null;

    // Fire the trigger whenever we have EITHER signal. With only
    // buyerUserId: standard mirror-aggregation path. With only receiptId:
    // targeted hydrate resolves buyer from the receipt, then aggregates.
    // With both: belt-and-suspenders, the hydrate confirms the receipt
    // and may correct a stale buyerUserId.
    if (buyerForSync || receiptIdValid) {
      const fnHost = process.env.URL || process.env.DEPLOY_PRIME_URL || null;
      if (fnHost) {
        const syncUrl = `${fnHost}/.netlify/functions/etsyMailSync-background`;
        const syncBody = { mode: "buyer", threadId };
        if (buyerForSync)   syncBody.buyerUserId = buyerForSync;
        if (receiptIdValid) syncBody.receiptId   = receiptIdValid;

        require("node-fetch")(syncUrl, {
          method : "POST",
          headers: { "Content-Type": "application/json" },
          body   : JSON.stringify(syncBody)
        }).catch(err => {
          console.warn(
            `buyer sync trigger failed for buyerUserId=${buyerForSync || "(none)"} receiptId=${receiptIdValid || "(none)"}:`,
            err.message || err
          );
        });
        console.log(
          `[snapshot] queued buyer sync — buyerUserId=${buyerForSync || "(unresolved)"}` +
          ` receiptId=${receiptIdValid || "(none)"} threadId=${threadId}`
        );
      } else {
        console.warn(
          `[snapshot] no fnHost (URL/DEPLOY_PRIME_URL) — skipping buyer sync trigger ` +
          `(buyerUserId=${buyerForSync || "(none)"}, receiptId=${receiptIdValid || "(none)"})`
        );
      }
    } else {
      console.log(
        `[snapshot] no buyer-sync signal available for thread ${threadId} ` +
        `(no buyerUserId from scrape, no orderId in conversation heading) — skipping trigger`
      );
    }

    await recordScrapeHealth(scrapeIssue, threadId)
      .catch(e => console.warn("[snapshot] scrape health not recorded:", e.message));

    return json(200, {
      success         : true,
      threadId,
      threadExisted,
      newMessages     : writtenCount,
      updatedMessages : updatedCount,
      duplicatesRemoved,
      needsFullScrape,
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
