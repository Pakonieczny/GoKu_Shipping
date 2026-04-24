/*  netlify/functions/etsyMailDraftReply.js
 *
 *  M4 — AI-assisted draft reply generator. v2 rewrite.
 *
 *  ═══ WHAT CHANGED FROM v1 ═══
 *
 *  - Full conversation history fed as alternating user/assistant turns,
 *    NOT as a pasted transcript string. This is the single biggest
 *    quality lever for "does this sound like the actual staff replying":
 *    Claude understands conversation boundaries natively when messages
 *    arrive in their real roles.
 *  - Images passed as native image blocks (base64) from Firebase Storage,
 *    so Claude can actually see the photos the customer sent.
 *  - Shared listing cards inlined into message text so Claude knows
 *    which products were discussed.
 *  - Shop enrichment: real Etsy shop metadata (policies, announcement,
 *    sections) pulled via Etsy API once every 24h and merged into the
 *    system prompt alongside the Firestore config.
 *  - Tool-use loop: Claude can call lookup_order_tracking and
 *    lookup_order_details to resolve "where's my order?" questions
 *    against live Etsy data. compose_draft_reply is the terminal tool
 *    that ends the loop (cleaner than JSON parsing).
 *  - Mode: added "follow_up" for re-engaging stalled custom-order prospects.
 *  - Model: Opus 4.7 with effort:"high". No temperature/top_p/top_k
 *    (not supported on 4.7). Adaptive thinking on by default.
 *  - Uses shared _etsyMailAnthropic.js client (same HTTP pattern as
 *    claudeCodeProxy-background.js in this repo).
 *
 *  ═══ REQUEST ═══
 *
 *  POST body:
 *    {
 *      threadId     : "etsy_conv_1651714855",   // required
 *      mode         : "initial" | "revise" | "follow_up",  // default "initial"
 *      currentDraft : "...",                     // for revise
 *      instructions : "...",                     // operator guidance
 *      employeeName : "Paul_K",                  // for signature
 *      includeImages: true                       // default true
 *    }
 *
 *  ═══ RESPONSE ═══
 *
 *  {
 *    success, draftId, text, reasoning, suggestedListings,
 *    referencedReceiptIds: ["4040875933"],       // receipts the AI actually looked up
 *    toolCalls: [{name, input, durationMs, ...}], // for audit UI
 *    tokensUsed: { input, output, cacheRead, cacheCreate },
 *    model, mode, durationMs, iterations
 *  }
 *
 *  ═══ ENV VARS ═══
 *
 *  ANTHROPIC_API_KEY              required
 *  ETSYMAIL_AI_MODEL              optional; default claude-opus-4-7
 *  ETSYMAIL_AI_EFFORT             optional; default "high"
 *  ETSYMAIL_AI_MAX_TOKENS         optional; default 12000 (Opus 4.7 counts
 *                                 thinking + response + tool-use ALL
 *                                 against max_tokens; at effort:"high"
 *                                 with multimodal input and a 2-3 tool
 *                                 loop, 5000 gets tight. 12000 leaves
 *                                 generous headroom while capping runaway.)
 *  ETSYMAIL_EXTENSION_SECRET      gates this endpoint
 *  SHOP_ID / CLIENT_ID / CLIENT_SECRET  for Etsy API tool calls
 */

const admin = require("./firebaseAdmin");
const { CORS } = require("./_etsyMailAuth");
const { runToolLoop } = require("./_etsyMailAnthropic");
const {
  getShop,
  getShopSections,
  getShopReceiptFull,
  getShopReceiptShipments
} = require("./_etsyMailEtsy");

const db     = admin.firestore();
const bucket = admin.storage().bucket();
const FV     = admin.firestore.FieldValue;

// ─── Collections ────────────────────────────────────────────────────────
const THREADS_COLL   = "EtsyMail_Threads";
const CUSTOMERS_COLL = "EtsyMail_Customers";
const DRAFTS_COLL    = "EtsyMail_Drafts";
const AUDIT_COLL     = "EtsyMail_Audit";
const CONFIG_COLL    = "EtsyMail_Config";

// ─── Model config ────────────────────────────────────────────────────────
// Opus 4.7 default. effort:"high" per operator request.
//
// IMPORTANT: On Opus 4.7, max_tokens is a hard ceiling on thinking tokens +
// response tokens + tool-use tokens COMBINED (per Anthropic's 4.7 docs —
// this is a change from 4.6 where thinking had its own budget). At
// effort:"high" with multimodal input and a 2-3 tool call loop, 5000 is
// tight; 12000 gives comfortable headroom without uncapping spend.
const AI_MODEL     = process.env.ETSYMAIL_AI_MODEL    || "claude-opus-4-7";
const AI_EFFORT    = process.env.ETSYMAIL_AI_EFFORT   || "high";
const AI_MAX_TOKENS = parseInt(process.env.ETSYMAIL_AI_MAX_TOKENS || "12000", 10);

// ─── Context-building caps ───────────────────────────────────────────────
// How many of the most-recent messages to include in the conversation
// history. 40 covers the vast majority of threads; older than that gets
// summarized in a (N earlier messages omitted) note.
const MESSAGE_HISTORY_LIMIT = 40;

// Hard cap on characters per message (guards against pasted mega-blobs).
const PER_MESSAGE_CHAR_CAP = 4000;

// Max images to embed across the whole conversation. Each Anthropic image
// block costs roughly 1500+ tokens depending on dimensions; 15 is a
// reasonable ceiling for cost + latency.
const MAX_IMAGES_TOTAL = 15;

// Max iterations in the tool-use loop. 6 covers: initial think → look up
// tracking → think → look up details → compose. Higher → rare multi-order
// scenarios. Caps hallucinated loops.
const MAX_TOOL_ITERATIONS = 6;

// Shop enrichment cache TTL — refresh shop metadata from Etsy API at most
// once every 24h. Stale cache is still used (sync refresh happens only
// when the doc is older than this).
const SHOP_ENRICHMENT_TTL_MS = 24 * 60 * 60 * 1000;

// ─── HTTP helpers ────────────────────────────────────────────────────────
function json(statusCode, body) {
  return { statusCode, headers: { ...CORS, "Content-Type": "application/json" }, body: JSON.stringify(body) };
}
function bad(msg, code = 400) { return json(code, { error: msg }); }

// ─── Firestore loaders ───────────────────────────────────────────────────

async function loadThread(threadId) {
  const snap = await db.collection(THREADS_COLL).doc(threadId).get();
  return snap.exists ? { id: snap.id, ...snap.data() } : null;
}

async function loadMessages(threadId, limit) {
  // Fetch the last N messages chronologically. We fetch +1 so we can
  // tell the model when older messages were elided.
  const snap = await db.collection(THREADS_COLL).doc(threadId)
    .collection("messages")
    .orderBy("timestamp", "desc")
    .limit(limit + 1)
    .get();
  const all = snap.docs.map(d => ({ id: d.id, ...d.data() }));
  const hasMore = all.length > limit;
  const kept = all.slice(0, limit).reverse();  // → chronological
  return { messages: kept, hasMore, elidedCount: hasMore ? (snap.size - limit) : 0 };
}

async function loadCustomer(buyerUserId) {
  if (!buyerUserId) return null;
  const snap = await db.collection(CUSTOMERS_COLL).doc(String(buyerUserId)).get();
  return snap.exists ? { id: snap.id, ...snap.data() } : null;
}

async function loadPromptConfig() {
  const snap = await db.collection(CONFIG_COLL).doc("aiPromptConfig").get();
  return snap.exists ? snap.data() : null;
}

// ─── Shop enrichment cache ───────────────────────────────────────────────
// Fetches shop metadata + sections from Etsy API, caches in Firestore
// for 24h. Stale cache is still returned — refresh happens fire-and-forget
// so draft latency is never blocked by shop enrichment.
//
// If the Etsy API call fails entirely and there's no cache, returns null
// and the prompt builder falls back to Firestore-only config.

async function getShopEnrichment() {
  const ref = db.collection(CONFIG_COLL).doc("shopEnrichment");
  const snap = await ref.get();
  const cached = snap.exists ? snap.data() : null;

  const ageMs = cached && cached.cachedAt && cached.cachedAt.toMillis
    ? Date.now() - cached.cachedAt.toMillis()
    : Infinity;

  if (cached && ageMs < SHOP_ENRICHMENT_TTL_MS) {
    return cached;
  }

  // Cache is stale or missing — refresh synchronously if missing,
  // fire-and-forget if we have stale cache.
  const refresh = refreshShopEnrichment(ref).catch(e => {
    console.warn("[shopEnrichment] refresh failed:", e.message);
    return null;
  });

  if (cached) {
    // Return stale cache immediately; refresh in background.
    refresh.then(() => {});   // no-op, just don't await
    return cached;
  }

  // No cache at all — wait for the refresh to complete
  return await refresh;
}

async function refreshShopEnrichment(ref) {
  const [shop, sections] = await Promise.all([
    getShop().catch(e => { console.warn("getShop failed:", e.message); return null; }),
    getShopSections().catch(e => { console.warn("getShopSections failed:", e.message); return []; })
  ]);

  if (!shop && !sections.length) return null;

  const payload = {
    shopName          : (shop && shop.shop_name)     || null,
    shopTitle         : (shop && shop.title)          || null,
    announcement      : (shop && shop.announcement)   || null,
    saleMessage       : (shop && shop.sale_message)   || null,
    digitalSaleMessage: (shop && shop.digital_sale_message) || null,
    policyWelcome     : (shop && shop.policy_welcome)      || null,
    policyPayment     : (shop && shop.policy_payment)      || null,
    policyShipping    : (shop && shop.policy_shipping)     || null,
    policyRefunds     : (shop && shop.policy_refunds)      || null,
    policyAdditional  : (shop && shop.policy_additional)   || null,
    policySellerInfo  : (shop && shop.policy_seller_info)  || null,
    sections          : (sections || []).map(s => ({
      sectionId : s.shop_section_id,
      title     : s.title,
      rank      : s.rank,
      activeListingCount: s.active_listing_count
    })),
    currency          : (shop && shop.currency_code) || null,
    cachedAt          : FV.serverTimestamp()
  };

  await ref.set(payload, { merge: false });
  return payload;
}

// ─── Message → Anthropic content mapper ──────────────────────────────────
// Converts a Firestore message doc into an Anthropic message content
// array. Handles text, mirrored images (via Firebase Storage base64),
// and shared listing cards (inlined as text references).

function clip(s, max) {
  if (!s) return "";
  s = String(s);
  if (s.length <= max) return s;
  return s.slice(0, max) + " [… truncated]";
}

function mimeFromStoragePath(path) {
  const ext = (path.split(".").pop() || "").toLowerCase();
  switch (ext) {
    case "jpg":
    case "jpeg": return "image/jpeg";
    case "png" : return "image/png";
    case "gif" : return "image/gif";
    case "webp": return "image/webp";
    default    : return "image/jpeg";
  }
}

/** Fetch image bytes from Firebase Storage and return an Anthropic image
 *  block. Returns null on any failure (missing file, too large, etc.) —
 *  caller handles the null by skipping the block. */
async function storageImageBlock(storagePath) {
  try {
    const file = bucket.file(storagePath);
    const [exists] = await file.exists();
    if (!exists) return null;
    const [meta] = await file.getMetadata().catch(() => [{}]);
    const [buf]  = await file.download();
    // Anthropic's image block size limit is 5 MB base64-encoded. Skip anything over 4MB raw as a safety margin.
    if (buf.length > 4 * 1024 * 1024) {
      console.warn(`[storageImageBlock] skipping ${storagePath} — too large (${buf.length} bytes)`);
      return null;
    }
    return {
      type  : "image",
      source: {
        type      : "base64",
        media_type: (meta && meta.contentType) || mimeFromStoragePath(storagePath),
        data      : buf.toString("base64")
      }
    };
  } catch (e) {
    console.warn(`[storageImageBlock] failed for ${storagePath}: ${e.message}`);
    return null;
  }
}

/** Format one message's listing-card / link context as a text note
 *  appended to the message body. */
function formatMessageContextSuffix(m) {
  const parts = [];
  if (Array.isArray(m.listingCards) && m.listingCards.length) {
    parts.push(
      "[Shared listing cards in this message:\n" +
      m.listingCards.slice(0, 6).map(c =>
        `  • ${c.title || "(untitled)"} — listing ${c.listingId || "?"}` +
        (c.priceText ? ` — ${c.priceText}` : "") +
        (c.listingUrl ? ` — ${c.listingUrl}` : "")
      ).join("\n") +
      "]"
    );
  }
  if (Array.isArray(m.imageUrls) && m.imageUrls.length && !(Array.isArray(m.storageImagePaths) && m.storageImagePaths.length)) {
    // Image was attached but never mirrored to Storage — we can't embed it
    parts.push(`[${m.imageUrls.length} image attachment(s) on this message — not available to view]`);
  }
  return parts.length ? "\n\n" + parts.join("\n\n") : "";
}

/** Convert one Firestore message doc into the content array for an
 *  Anthropic message turn. budget is the remaining image budget for
 *  the conversation (decremented as images are attached).
 *
 *  IMPORTANT: Anthropic's API only allows `image` content blocks in
 *  USER turns, not assistant turns. Staff messages (which become
 *  assistant turns) must describe their images textually, not embed
 *  them. This matches our intent anyway — the AI is role-playing as
 *  the staff, so it doesn't need to "see" what previous staff sent,
 *  only know that an image was sent. */
async function messageToContent(m, imageBudget, includeImages, role) {
  const content = [];
  const canEmbedImages = role === "user";   // Anthropic: images only in user turns

  // Images first — they visually anchor the message. Only mirror-stored ones
  // (we have the bytes); unmirrored imageUrls get a text note instead.
  // Only for USER (customer) messages.
  if (canEmbedImages && includeImages && Array.isArray(m.storageImagePaths) && m.storageImagePaths.length && imageBudget.remaining > 0) {
    for (const sp of m.storageImagePaths) {
      if (imageBudget.remaining <= 0) break;
      const block = await storageImageBlock(sp);
      if (block) {
        content.push(block);
        imageBudget.remaining -= 1;
        imageBudget.attached += 1;
      }
    }
  }

  // Text body — always present even if empty (model needs to see the turn)
  const text = clip(m.text || "", PER_MESSAGE_CHAR_CAP);
  let suffix = formatMessageContextSuffix(m);

  // For assistant turns (staff messages) that HAD images: add a text note
  // since we can't embed them. Same convention as unmirrored-but-present
  // images in the suffix formatter, but always applied regardless of mirror state.
  if (!canEmbedImages && Array.isArray(m.imageUrls) && m.imageUrls.length) {
    const imgCount = m.imageUrls.length;
    const imgNote = `\n\n[${imgCount} image attachment${imgCount > 1 ? "s" : ""} sent with this staff message — not embedded (assistant-turn restriction). Reference them textually if needed.]`;
    // Only add if the suffix didn't already mention the unmirrored note for the same images
    if (!suffix.includes("image attachment")) suffix += imgNote;
  }

  const headerDate = tsToDateStr(m.timestamp) || tsToDateStr(m.createdAt);
  const header = headerDate ? `[${headerDate}] ` : "";
  const body = `${header}${text || "(no text)"}` + suffix;
  content.push({ type: "text", text: body });

  return content;
}

function tsToDateStr(ts) {
  if (!ts) return "";
  const ms = (ts && typeof ts.toMillis === "function") ? ts.toMillis()
           : (typeof ts === "number") ? ts
           : null;
  if (!ms) return "";
  const d = new Date(ms);
  return d.toISOString().slice(0, 16).replace("T", " ");   // YYYY-MM-DD HH:MM
}

// ─── Build the full messages array for the API call ──────────────────────
// Maps Firestore messages → alternating user/assistant Anthropic turns.
// Customer messages are "user"; staff messages are "assistant" (so the
// model sees its own prior outputs as its own, picking up style/voice).
//
// Consecutive same-role messages are CONCATENATED within a single turn —
// the Anthropic API requires strictly alternating roles. So two customer
// messages back-to-back become one user turn with two content arrays
// merged. Same for staff.

async function buildConversationMessages(messages, elidedCount, hasMore, includeImages) {
  const imageBudget = { remaining: MAX_IMAGES_TOTAL, attached: 0 };
  const turns = [];
  let currentRole = null;
  let currentContent = [];

  // Preamble user turn — tells model about elided history if any
  if (hasMore && elidedCount > 0) {
    turns.push({
      role: "user",
      content: [{ type: "text", text: `[CONVERSATION CONTEXT: ${elidedCount} older messages have been omitted. What follows are the ${messages.length} most recent messages in chronological order.]` }]
    });
  }

  for (const m of messages) {
    const role = m.senderRole === "staff" ? "assistant" : "user";

    if (currentRole && currentRole !== role) {
      // Flush current turn
      turns.push({ role: currentRole, content: currentContent });
      currentContent = [];
    }
    currentRole = role;

    const msgContent = await messageToContent(m, imageBudget, includeImages, role);
    // Separator between merged messages of same role
    if (currentContent.length) {
      currentContent.push({ type: "text", text: "---" });
    }
    currentContent.push(...msgContent);
  }
  if (currentRole) {
    turns.push({ role: currentRole, content: currentContent });
  }

  // Anthropic requires messages[] to start with role=user. If our first
  // turn is "assistant" (rare — thread started with a staff message),
  // prepend a synthetic user framing turn.
  if (turns.length && turns[0].role !== "user") {
    turns.unshift({
      role: "user",
      content: [{ type: "text", text: "[Earlier context — this thread opens with a staff-side message.]" }]
    });
  }

  return { turns, imagesAttached: imageBudget.attached };
}

// ─── System prompt builder ───────────────────────────────────────────────

const DEFAULT_SYSTEM_PROMPT = `
You are composing replies AS A HUMAN member of the CustomBrites customer
service team. You are NOT an AI assistant speaking to a customer — you
ARE the shop's reply voice. Customers must never suspect your replies
were drafted by AI.

About CustomBrites:
- Etsy shop specializing in handmade charm jewelry
- Nature-themed: birds (cardinals especially), bees, butterflies,
  caterpillars, and similar pieces
- Materials: sterling silver, gold-filled, 14K solid gold
- Charm sizes: huggie-size, standard, add-on sets
- Custom combinations are a core part of the business

Voice:
- Warm, personal, concise
- First-name basis if known
- Replies stay under ~150 words unless detail is genuinely needed
- Never promise specific ship dates — always give a range
- Reference order history naturally where it adds value ("thanks for
  being a repeat buyer", etc.)
- Don't over-apologize; one acknowledgment is enough
- If unsure about a product detail, gently suggest the customer check
  the listing rather than guess
`.trim();

function buildSystemPromptText(config, shopEnrichment, employeeName) {
  let sys = (config && config.systemPrompt) || DEFAULT_SYSTEM_PROMPT;

  // Conversation-boundary instruction — critical for accurate replies
  sys += `

CONVERSATION INTERPRETATION RULES — APPLY TO EVERY DRAFT:

1. IDENTIFY THE ACTIVE QUESTION. A long thread may contain multiple
   conversations over time. Before composing, determine:
      - What is the most recent QUESTION or REQUEST the customer has
        made that is still OPEN?
      - What parts of the history are resolved ("already shipped",
        "refund issued", "custom finalized") and should be treated
        as CONTEXT only, not re-answered?
      - Are there conflicting statements across time? Trust the most
        recent and treat older contradictions as resolved unless
        the customer explicitly reopens them.

2. ONE CONVERSATION PER DRAFT. Don't re-answer old resolved questions.
   Don't reference information from three months ago unless the
   customer explicitly references it. The customer opens the thread
   and sees YOUR reply; they're thinking about what they just asked,
   not what they asked six weeks ago.

3. IDENTIFY THE ORDER BEING DISCUSSED. If the customer asks about
   their order, figure out WHICH order:
      - Recent conversational context (references to specific items,
        receipt numbers, ship dates) usually pins it down
      - If ambiguous, look at the customer's recent receipts — most
        likely the most recent unshipped one for tracking questions,
        most recent shipped one for delivery questions
      - When STILL unclear, it's acceptable to politely ask which
        order — BUT only when you genuinely can't narrow it down
      - Use the lookup_order_details tool to pull full order info
        when you need to reference specific items or personalization

4. TRACKING QUESTIONS. When a customer asks "where's my order?",
   "hasn't arrived", "tracking number please":
      - Call lookup_order_tracking with the relevant receiptId
      - If tracking exists AND the order shipped, share it warmly
      - If order is paid but not shipped, acknowledge we're still
        making it, and give a realistic time range
      - If no data comes back, DON'T fabricate — say "let me pull
        up your tracking details and get back to you within a few
        hours" (operator will handle from there)

5. SALES CONVERSION for custom / large / high-intent conversations:
      - When a customer is asking about a custom piece or a larger
        order, engage like a craftsperson who's genuinely excited to
        make it. Ask clarifying questions that help narrow the design.
      - When a customer has shown interest but the conversation has
        stalled (follow_up mode), gently re-engage: confirm you're
        still available, ask if they've had time to think, offer to
        put something together.
      - NEVER pressure. Always soft. An Etsy customer who feels
        pressured will disappear; one who feels cared-for will return.

6. HUMAN TONE HYGIENE:
      - Don't use corporate-speak ("per our policy", "as per the
        agreement", "we strive to...") — this screams support-bot
      - Don't over-structure with bullet lists or headers — talk
        like a person in an email, with paragraphs
      - One or two sentences per paragraph is plenty
      - Don't start with "I understand your concern" or similar
        canned empathy openers
      - Contractions are fine (we're, you're, it's)

`.trim();

  // Firestore-configured shop policies
  if (config && Array.isArray(config.shopPolicies) && config.shopPolicies.length) {
    sys += "\n\n--- SHOP POLICIES (from shop config) ---\n" +
      config.shopPolicies.map(p => `- ${String(p).trim()}`).join("\n");
  }

  if (config && config.toneGuidelines) {
    sys += "\n\n--- TONE GUIDELINES ---\n" + String(config.toneGuidelines).trim();
  }

  // Etsy-sourced shop enrichment
  if (shopEnrichment) {
    const shopLines = [];
    if (shopEnrichment.shopName) shopLines.push(`Shop name: ${shopEnrichment.shopName}`);
    if (shopEnrichment.announcement) shopLines.push(`Current announcement: ${shopEnrichment.announcement}`);
    if (shopEnrichment.policyShipping) shopLines.push(`Shipping policy (from Etsy):\n${clip(shopEnrichment.policyShipping, 1200)}`);
    if (shopEnrichment.policyRefunds)  shopLines.push(`Returns policy (from Etsy):\n${clip(shopEnrichment.policyRefunds, 1200)}`);
    if (shopEnrichment.policyAdditional) shopLines.push(`Additional policies (from Etsy):\n${clip(shopEnrichment.policyAdditional, 1200)}`);
    if (shopLines.length) sys += "\n\n--- LIVE SHOP INFO FROM ETSY ---\n" + shopLines.join("\n\n");
  }

  // Signature
  const sigTemplate = (config && config.signatureTemplate) || "Best,\n{employeeName}\nCustomBrites";
  const sig = sigTemplate.replace(/\{employeeName\}/g, employeeName || "CustomBrites");
  sys += `\n\n--- SIGNATURE TO USE ---\n${sig}`;

  // Tool-use instructions
  sys += `

--- TOOL USE ---

You have four tools:
  - lookup_order_tracking(receiptId) — returns tracking code, carrier,
    ship date, delivery status for a specific order. Use this whenever
    the customer asks about tracking/where their order is/has it shipped.
  - lookup_order_details(receiptId) — returns the full order: items,
    personalization, variations, totals, buyer address. Use this when
    you need to reference specific items or check personalization.
  - generate_tracking_image(trackingCode) — generates a branded visual
    tracking timeline image that will be attached to your reply. Use
    this when the customer is asking where their package is and seeing
    the scan history would help. Call AFTER lookup_order_tracking so you
    pass the correct tracking code. The carrier (USPS vs Chit Chats) is
    auto-detected. You will naturally reference the attached image in
    your reply (e.g., "I've pulled up the tracking for you below").
  - compose_draft_reply(...) — THE TERMINAL TOOL. Call this exactly
    ONCE when you've completed all lookups and are ready to commit the
    reply. This ends the draft generation.

Workflow:
  1. Read the customer context + conversation history
  2. Identify the active question (per rules above)
  3. If the question is about tracking/shipping/an order detail, call
     lookup_order_tracking and/or lookup_order_details as needed
  4. If a visual tracking timeline would genuinely help the customer
     (they're anxious about a package, asking "where is it?", or the
     tracking has unusual detail worth showing), call
     generate_tracking_image with the tracking code from step 3
  5. Call compose_draft_reply with the final text + reasoning +
     referenced receiptIds + any listing suggestions

When you've generated a tracking image, mention it naturally in your
reply — "I've attached the current tracking details below" — and then
explain what the customer should understand from the timeline (the
scan events, expected delivery, what's normal). Don't just dump the
image; contextualize it.

Do NOT emit prose replies directly — only via compose_draft_reply.
Reasoning in plain text is fine between tool calls (Claude's adaptive
thinking handles that); just make sure the final action is
compose_draft_reply.
`.trim();

  return sys;
}

// ─── Context preamble — customer + thread + mode ─────────────────────────
// We add one extra "user" turn at the START of the conversation that
// gives the model the customer context, order history, and mode
// instructions. This lives ABOVE the real conversation history so the
// model has scene-setting before it reads the messages themselves.

function buildContextPreamble({ thread, customer, mode, currentDraft, instructions, employeeName }) {
  const sections = [];

  sections.push(`--- THREAD METADATA ---`);
  sections.push(`Thread ID: ${thread.id}`);
  sections.push(`Subject: ${thread.subject || "(none)"}`);
  sections.push(`Current status: ${thread.status || "unknown"}`);
  if (thread.etsyConversationUrl) sections.push(`Etsy URL: ${thread.etsyConversationUrl}`);

  sections.push(`\n--- CUSTOMER CONTEXT ---`);
  sections.push(`Display name: ${thread.customerName || "(unknown)"}`);
  if (thread.etsyUsername) sections.push(`Etsy username: ${thread.etsyUsername}`);

  if (customer) {
    sections.push(`Orders in last 2 years: ${customer.orderCount || 0}, ${customer.currency || "USD"} ${Number(customer.totalSpent || 0).toFixed(2)} total spent`);
    const first = tsToDateStr(customer.firstOrderAt);
    const last  = tsToDateStr(customer.lastOrderAt);
    if (first) sections.push(`First order: ${first}`);
    if (last)  sections.push(`Most recent order: ${last}`);

    if (Array.isArray(customer.recentReceipts) && customer.recentReceipts.length) {
      sections.push(`\nRecent receipts (newest first — use these receiptIds with your lookup tools):`);
      for (const r of customer.recentReceipts.slice(0, 10)) {
        const od = tsToDateStr(r.orderedAt);
        const statusBits = [];
        if (r.isShipped) statusBits.push("shipped");
        else if (r.isPaid) statusBits.push("paid, not yet shipped");
        else statusBits.push("unpaid");
        sections.push(
          `  • receiptId=${r.receiptId} — ${od} — ${customer.currency || "USD"} ${Number(r.grandTotal || 0).toFixed(2)} — ${statusBits.join(", ")}`
        );
      }
    }
  } else {
    sections.push(`(No cached purchase history. May be a first-time buyer, or last ordered >2 years ago.)`);
  }

  // Mode-specific instruction
  sections.push(`\n--- DRAFT MODE: ${mode.toUpperCase()} ---`);
  if (mode === "initial") {
    sections.push(`Compose a fresh reply to the most recent customer message in the thread below.`);
  } else if (mode === "revise") {
    sections.push(`Revise the existing draft the operator has in the composer. Keep the core message but incorporate the instructions below if provided.`);
    if (currentDraft && currentDraft.trim()) {
      sections.push(`\nCurrent draft text:\n"""\n${clip(currentDraft, 4000)}\n"""`);
    }
  } else if (mode === "follow_up") {
    sections.push(`This is a FOLLOW-UP draft. The previous operator reply didn't receive a response, and enough time has passed that a gentle re-engagement feels natural. Tone: warm, low-pressure, ask one clear question that makes it easy for the customer to reply. Do NOT repeat info already covered in the thread.`);
  }

  if (instructions && instructions.trim()) {
    sections.push(`\n--- OPERATOR INSTRUCTIONS ---\n${clip(instructions, 1500)}`);
  }

  sections.push(`\n--- WHAT FOLLOWS ---
The conversation history is delivered as alternating user (customer) and
assistant (CustomBrites staff) turns. Staff-sent images/listings are
visible alongside the customer's. Read the full history, identify the
active question per the rules in the system prompt, do any tracking or
order lookups you need, and finish by calling compose_draft_reply.

Operator signing the reply: ${employeeName || "(unspecified — use default signature)"}`);

  return sections.join("\n");
}

// ─── Tool specs + executors ──────────────────────────────────────────────

const TOOL_SPECS = [
  {
    name: "lookup_order_tracking",
    description: "Look up the current tracking status, carrier, tracking code, and shipping date for a specific Etsy order. Use this when the customer asks about where their order is, if it has shipped, or for a tracking number. The receiptId MUST be from the customer's cached order history — pick the most likely order being discussed.",
    input_schema: {
      type: "object",
      properties: {
        receiptId: {
          type: "string",
          description: "The Etsy receipt ID (numeric string) for the order in question. Pick from the customer's Recent receipts list in context."
        }
      },
      required: ["receiptId"]
    }
  },
  {
    name: "lookup_order_details",
    description: "Look up the full details of a specific Etsy order: what items were purchased, personalization text, variations, totals, shipping address. Use this when you need to reference specific items or check what a customer personalized on their order.",
    input_schema: {
      type: "object",
      properties: {
        receiptId: {
          type: "string",
          description: "The Etsy receipt ID (numeric string) from the customer's Recent receipts list."
        }
      },
      required: ["receiptId"]
    }
  },
  {
    name: "generate_tracking_image",
    description: "Generate a branded visual tracking timeline image for a specific tracking number. Use this when the customer is asking where their package is, or when seeing the scan history would help answer their question. The carrier (USPS or Chit Chats) is auto-detected from the tracking number format. Returns an imageUrl that can be referenced in your draft reply as an attachment. Prefer calling this AFTER lookup_order_tracking has returned a tracking code, so you pass the correct code.",
    input_schema: {
      type: "object",
      properties: {
        trackingCode: {
          type: "string",
          description: "The tracking code to generate a timeline image for. Must be one of: a USPS label number (12/15/20/22/26 digits), a Chit Chats shipment ID (10 alphanumeric chars), or a UPU S10 international code (format: LX123456789NL)."
        }
      },
      required: ["trackingCode"]
    }
  },
  {
    name: "compose_draft_reply",
    description: "Emit the final reply text that will be shown to the operator. Call this EXACTLY ONCE at the end of your reasoning/tool-use process. This ends the draft generation.",
    input_schema: {
      type: "object",
      properties: {
        text: {
          type: "string",
          description: "The reply text, including the signature. This is what the operator will see in the composer."
        },
        reasoning: {
          type: "string",
          description: "2-4 sentences: what you identified as the active question, which order (if relevant) you pinned it to, and why your reply says what it says."
        },
        referencedReceiptIds: {
          type: "array",
          items: { type: "string" },
          description: "Array of receiptIds you actually looked up while drafting. Empty array if none."
        },
        suggestedListings: {
          type: "array",
          description: "Optional. Listings that would make sense to attach to this reply (e.g., a specific cardinal charm the customer asked about). Only include if you're confident the listing exists based on prior conversation.",
          items: {
            type: "object",
            properties: {
              listingId: { type: "string" },
              title    : { type: "string" },
              reason   : { type: "string" }
            },
            required: ["listingId", "title"]
          }
        },
        activeQuestion: {
          type: "string",
          description: "One-sentence statement of the customer's current open question, as you understood it."
        }
      },
      required: ["text", "reasoning", "referencedReceiptIds"]
    }
  }
];

function buildToolExecutors(ctx) {
  return {
    lookup_order_tracking: async (input) => {
      const receiptId = String(input.receiptId || "").trim();
      if (!receiptId || !/^\d+$/.test(receiptId)) {
        return { error: "receiptId must be a numeric string" };
      }

      // Validate: receiptId should belong to this customer
      const recentIds = new Set(
        ((ctx.customer && ctx.customer.recentReceipts) || []).map(r => String(r.receiptId))
      );
      if (recentIds.size && !recentIds.has(receiptId)) {
        return {
          error: "receiptId does not match any receipt in the customer's cached order history",
          receiptId,
          availableReceiptIds: Array.from(recentIds)
        };
      }

      const data = await getShopReceiptShipments(receiptId);
      return data;
    },

    lookup_order_details: async (input) => {
      const receiptId = String(input.receiptId || "").trim();
      if (!receiptId || !/^\d+$/.test(receiptId)) {
        return { error: "receiptId must be a numeric string" };
      }
      const recentIds = new Set(
        ((ctx.customer && ctx.customer.recentReceipts) || []).map(r => String(r.receiptId))
      );
      if (recentIds.size && !recentIds.has(receiptId)) {
        return {
          error: "receiptId does not match any receipt in the customer's cached order history",
          receiptId,
          availableReceiptIds: Array.from(recentIds)
        };
      }

      const receipt = await getShopReceiptFull(receiptId);
      // Slim down to the fields the model needs (the full payload is huge
      // and wastes tokens)
      const tx = Array.isArray(receipt.transactions) ? receipt.transactions : [];
      return {
        receiptId    : String(receiptId),
        orderedAt    : receipt.created_timestamp ? new Date(receipt.created_timestamp * 1000).toISOString() : null,
        buyerName    : receipt.name || null,
        buyerMessage : receipt.message_from_buyer || null,
        isPaid       : !!receipt.is_paid,
        isShipped    : !!receipt.is_shipped,
        grandTotal   : receipt.grandtotal && (Number(receipt.grandtotal.amount) / Math.pow(10, receipt.grandtotal.divisor || 2)) || null,
        currency     : receipt.grandtotal && receipt.grandtotal.currency_code || null,
        shippingAddress: {
          firstLine : receipt.first_line   || null,
          secondLine: receipt.second_line  || null,
          city      : receipt.city         || null,
          state     : receipt.state        || null,
          zip       : receipt.zip          || null,
          country   : receipt.country_iso  || null
        },
        items: tx.map(t => ({
          listingId      : t.listing_id,
          title          : t.title,
          quantity       : t.quantity,
          price          : t.price && (Number(t.price.amount) / Math.pow(10, t.price.divisor || 2)) || null,
          personalization: t.personalization || t.transaction_personalization || null,
          variations     : Array.isArray(t.variations) ? t.variations.map(v => ({
            property: v.formatted_name  || v.property_value || null,
            value   : v.formatted_value || null
          })) : []
        })),
        // Shipments so the model has tracking if it asked for details
        // instead of tracking specifically
        isShippedStatus: !!receipt.is_shipped
      };
    },

    // compose_draft_reply is the TERMINAL tool. Returning __terminal:true
    // tells runToolLoop to break out after processing this batch — the
    // model doesn't get another API call (saves cost + prevents it from
    // looping / producing a second draft).
    compose_draft_reply: async (input) => {
      return { __terminal: true, received: true, composed: true };
    },

    // Generate a branded tracking-timeline image.
    //
    // Architecture:
    //   - Calls the snapshot endpoint (fast; creates a job doc, fires the
    //     background function, returns a jobId within ~1 sec)
    //   - On cache hit: endpoint returns inline data (no job needed)
    //   - On cache miss: endpoint returns { jobId, status: "pending" }
    //     and the background function does the slow work (up to 15 min)
    //
    // Either way, the AI's tool call completes in <2 seconds. The UI polls
    // EtsyMail_TrackingJobs/{jobId} for the final image.
    generate_tracking_image: async (input) => {
      const trackingCode = String(input.trackingCode || "").trim();
      if (!trackingCode) {
        return { error: "trackingCode is required" };
      }

      const fetch = require("node-fetch");

      // Build the self-URL for our own snapshot endpoint. In Netlify prod,
      // process.env.URL is the site's canonical URL.
      const baseUrl = process.env.URL ||
                      process.env.DEPLOY_PRIME_URL ||
                      process.env.NETLIFY_SITE_URL ||
                      "https://etsy-mail-1.goldenspike.app";
      const endpoint = `${baseUrl.replace(/\/$/, "")}/.netlify/functions/etsyMailTrackingSnapshot`;

      let res, body;
      try {
        res = await fetch(endpoint, {
          method : "POST",
          headers: { "Content-Type": "application/json" },
          body   : JSON.stringify({ trackingCode }),
          timeout: 9000
        });
        const text = await res.text();
        try { body = JSON.parse(text); }
        catch { body = { error: `Non-JSON response: ${text.slice(0, 300)}` }; }
      } catch (e) {
        return { error: `Tracking snapshot call failed: ${e.message}`, trackingCode };
      }

      if (!res.ok) {
        return {
          error       : body.error || `Tracking snapshot returned ${res.status}`,
          code        : body.code || null,
          trackingCode
        };
      }

      // Record the job reference so the UI can render a placeholder + poll
      if (ctx.trackingImages) {
        ctx.trackingImages.push({
          trackingCode     : body.trackingCode,
          jobId            : body.jobId || null,
          status           : body.status || "pending",    // "pending" | "ready"
          inline           : body.inline === true,         // cache hit flag
          carrier          : body.carrier || null,
          carrierDisplay   : body.carrierDisplay || null,
          statusText       : body.statusText || null,
          statusKey        : body.statusKey || null,
          estimatedDelivery: body.estimatedDelivery || null,
          destination      : body.destination || null,
          imageUrl         : body.imageUrl || null,        // null unless inline=true
          imageStoragePath : body.imageStoragePath || null,
          imageWidth       : body.imageWidth || null,
          imageHeight      : body.imageHeight || null,
          eventCount       : (body.events || []).length,
          latestEvent      : (body.events || [])[0] || null
        });
      }

      // Return a compact summary to the model so it can reference the
      // tracking image naturally in its draft.
      //
      // On cache hit (inline=true): model has full tracking data to cite
      // On cache miss: model just knows an image is being generated — it
      //   should reference it as "the tracking details attached below"
      //   without pretending to know specifics it hasn't seen.
      if (body.inline) {
        return {
          success            : true,
          imageGenerated     : true,
          trackingCode       : body.trackingCode,
          carrier            : body.carrierDisplay,
          status             : body.statusText,
          statusKey          : body.statusKey,
          estimatedDelivery  : body.estimatedDelivery,
          destination        : body.destination,
          eventCount         : (body.events || []).length,
          latestEventTitle   : (body.events || [])[0]?.title || null,
          latestEventLocation: (body.events || [])[0]?.location || null,
          latestEventAt      : (body.events || [])[0]?.at || null,
          cached             : true
        };
      } else {
        return {
          success         : true,
          imageGenerating : true,
          trackingCode    : body.trackingCode,
          jobId           : body.jobId,
          note            : "The tracking image is being generated in the background (typically 5-30 sec). Reference it in your reply as 'the tracking details attached below' — the operator's UI will display it as soon as it's ready."
        };
      }
    }
  };
}

// ─── Audit + draft persistence ──────────────────────────────────────────

async function writeAudit({ threadId, draftId, eventType, actor = "system:draftReply", payload = {} }) {
  try {
    await db.collection(AUDIT_COLL).add({
      threadId: threadId || null,
      draftId : draftId || null,
      eventType,
      actor,
      payload,
      createdAt: FV.serverTimestamp()
    });
  } catch (e) {
    console.warn("audit write failed:", e.message);
  }
}

// ─── Main handler ───────────────────────────────────────────────────────

exports.handler = async (event) => {
  if (event.httpMethod === "OPTIONS") return { statusCode: 200, headers: CORS, body: "ok" };
  if (event.httpMethod !== "POST")     return json(405, { error: "Method Not Allowed" });

  // NOTE: No requireExtensionAuth here by design — this endpoint is called
  // from the operator inbox (browser, same-origin) which does NOT send the
  // X-EtsyMail-Secret header. Matches the pattern used by etsyMailOrder.js.
  // The inbox itself is operator-only. If you need to harden this later,
  // either add the secret to the inbox's api() helper, or add a CORS origin
  // allowlist check here.

  let body = {};
  try { body = JSON.parse(event.body || "{}"); }
  catch { return bad("Invalid JSON body"); }

  const {
    threadId,
    mode          = "initial",
    currentDraft  = null,
    instructions  = null,
    employeeName  = null,
    includeImages = true
  } = body;

  if (!threadId) return bad("Missing threadId");
  if (!["initial", "revise", "follow_up"].includes(mode)) {
    return bad("mode must be 'initial' | 'revise' | 'follow_up'");
  }

  const tStart = Date.now();
  let draftId = `draft_${threadId}`;

  try {
    // ─── 1. Load all context in parallel ────────────────────────────
    const [thread, promptConfig, shopEnrichment] = await Promise.all([
      loadThread(threadId),
      loadPromptConfig(),
      getShopEnrichment().catch(e => {
        console.warn("[shopEnrichment]", e.message);
        return null;
      })
    ]);

    if (!thread) return json(404, { error: "Thread not found", threadId });

    const [{ messages, hasMore, elidedCount }, customer] = await Promise.all([
      loadMessages(threadId, MESSAGE_HISTORY_LIMIT),
      loadCustomer(thread.buyerUserId)
    ]);

    if (!messages.length) return bad("Thread has no messages to reply to");

    // ─── 2. Build the Anthropic message array ──────────────────────
    // Context preamble first (as a user turn), then the real conversation.
    const preambleText = buildContextPreamble({
      thread, customer, mode, currentDraft, instructions, employeeName
    });
    const { turns: convTurns, imagesAttached } = await buildConversationMessages(
      messages, elidedCount, hasMore, includeImages
    );

    // Merge preamble into the first user turn (must start with role:user)
    const initialMessages = [
      { role: "user", content: [{ type: "text", text: preambleText }] },
      ...convTurns
    ];

    // ─── 3. Build system prompt ────────────────────────────────────
    const system = buildSystemPromptText(promptConfig, shopEnrichment, employeeName);

    // ─── 4. Run the tool-use loop ──────────────────────────────────
    const toolContext = {
      thread,
      customer,
      trackingImages: []   // collected by generate_tracking_image executor
    };
    const toolExecutors = buildToolExecutors(toolContext);

    let loopResult;
    try {
      loopResult = await runToolLoop({
        model         : AI_MODEL,
        maxTokens     : AI_MAX_TOKENS,
        system,
        initialMessages,
        toolSpecs     : TOOL_SPECS,
        toolExecutors,
        toolContext,
        effort        : AI_EFFORT,
        useThinking   : true,
        maxIterations : MAX_TOOL_ITERATIONS
      });
    } catch (e) {
      await db.collection(THREADS_COLL).doc(threadId).set({
        aiDraftStatus: "failed",
        updatedAt    : FV.serverTimestamp()
      }, { merge: true }).catch(()=>{});
      await writeAudit({
        threadId, eventType: "ai_draft_failed",
        payload: { error: e.message, mode }
      });
      return json(502, { error: `AI call failed: ${e.message}` });
    }

    const durationMs = Date.now() - tStart;

    // ─── 5. Extract the final reply from compose_draft_reply tool call ──
    // Look for the compose_draft_reply call in the tool-call log.
    const composeCall = loopResult.toolCalls.find(tc => tc.name === "compose_draft_reply");
    let parsed;
    let parsedOk = false;

    if (composeCall && composeCall.input && typeof composeCall.input.text === "string") {
      parsed = {
        text                : composeCall.input.text.trim(),
        reasoning           : String(composeCall.input.reasoning || "").trim(),
        referencedReceiptIds: Array.isArray(composeCall.input.referencedReceiptIds) ? composeCall.input.referencedReceiptIds.map(String) : [],
        suggestedListings   : Array.isArray(composeCall.input.suggestedListings) ? composeCall.input.suggestedListings : [],
        activeQuestion      : String(composeCall.input.activeQuestion || "").trim()
      };
      parsedOk = Boolean(parsed.text);
    }

    if (!parsedOk) {
      // Fallback — model produced text but never called compose_draft_reply.
      // Extract the last text content block as the reply.
      const finalContent = Array.isArray(loopResult.finalResponse.content) ? loopResult.finalResponse.content : [];
      const lastText = finalContent.filter(b => b.type === "text").map(b => b.text).join("\n\n").trim();
      parsed = {
        text                : lastText || "(Model finished without producing a draft. Try again.)",
        reasoning           : "(Model did not call compose_draft_reply — using last text block as reply.)",
        referencedReceiptIds: loopResult.toolCalls
          .filter(tc => tc.name === "lookup_order_tracking" || tc.name === "lookup_order_details")
          .map(tc => String((tc.input && tc.input.receiptId) || "")).filter(Boolean),
        suggestedListings   : [],
        activeQuestion      : ""
      };
      parsedOk = false;
    }

    // Sanitize suggestedListings
    parsed.suggestedListings = parsed.suggestedListings
      .filter(s => s && typeof s === "object")
      .map(s => ({
        listingId: String(s.listingId || "").trim(),
        title    : String(s.title     || "").trim(),
        reason   : String(s.reason    || "").trim()
      }))
      .filter(s => s.listingId && s.title)
      .slice(0, 5);

    // ─── 6. Persist draft ──────────────────────────────────────────
    const draftRef = db.collection(DRAFTS_COLL).doc(draftId);
    const now = FV.serverTimestamp();

    // Audit-friendly tool call log (strip large response payloads)
    const toolCallLog = loopResult.toolCalls.map(tc => ({
      name       : tc.name,
      input      : tc.input,
      error      : tc.error,
      durationMs : tc.durationMs,
      // For non-terminal tools, include a slim version of the output
      outputPreview: tc.name === "compose_draft_reply" ? null :
        (typeof tc.output === "object" && tc.output !== null
          ? { ...tc.output, _truncated: false }
          : tc.output)
    }));

    const usage = loopResult.usage || {};
    const trackingImages = Array.isArray(toolContext.trackingImages) ? toolContext.trackingImages : [];

    // Build attachments array: any generated tracking images become attachments
    // the operator can include when sending the reply.
    const attachments = trackingImages.map(img => ({
      type          : "tracking_image",
      trackingCode  : img.trackingCode,
      carrier       : img.carrier,
      carrierDisplay: img.carrierDisplay,
      status        : img.status,
      statusKey     : img.statusKey,
      imageUrl      : img.imageUrl,
      imageStoragePath: img.imageStoragePath,
      imageWidth    : img.imageWidth,
      imageHeight   : img.imageHeight,
      queuedForSend : true,  // default: include when operator sends
      addedAt       : new Date().toISOString()
    }));

    const draftDoc = {
      draftId,
      threadId,
      status                : "draft",
      text                  : parsed.text,
      reasoning             : parsed.reasoning,
      activeQuestion        : parsed.activeQuestion,
      suggestedListings     : parsed.suggestedListings,
      referencedReceiptIds  : parsed.referencedReceiptIds,
      attachments,
      trackingImages,
      generatedByAI         : true,
      aiModel               : AI_MODEL,
      aiEffort              : AI_EFFORT,
      aiMode                : mode,
      aiInstructions        : instructions || null,
      aiParsedOk            : parsedOk,
      aiIncludedImages      : imagesAttached,
      aiToolCalls           : toolCallLog,
      aiTokensInput         : usage.input_tokens                || 0,
      aiTokensOutput        : usage.output_tokens               || 0,
      aiTokensCacheRead     : usage.cache_read_input_tokens     || 0,
      aiTokensCacheCreate   : usage.cache_creation_input_tokens || 0,
      aiDurationMs          : durationMs,
      aiIterations          : loopResult.toolCalls.length,
      createdBy             : employeeName || null,
      createdAt             : now,
      updatedAt             : now
    };
    await draftRef.set(draftDoc, { merge: false });

    // ─── 7. Update thread ──────────────────────────────────────────
    await db.collection(THREADS_COLL).doc(threadId).set({
      latestDraftId: draftId,
      aiDraftStatus: "ready",
      updatedAt    : now
    }, { merge: true });

    // ─── 8. Audit ─────────────────────────────────────────────────
    await writeAudit({
      threadId, draftId,
      eventType: mode === "revise" ? "ai_draft_revised"
                : mode === "follow_up" ? "ai_draft_follow_up"
                : "ai_draft_generated",
      actor    : employeeName ? `operator:${employeeName}` : "system:draftReply",
      payload  : {
        model              : AI_MODEL,
        effort             : AI_EFFORT,
        mode,
        parsedOk,
        activeQuestion     : parsed.activeQuestion,
        tokensInput        : draftDoc.aiTokensInput,
        tokensOutput       : draftDoc.aiTokensOutput,
        tokensCacheRead    : draftDoc.aiTokensCacheRead,
        tokensCacheCreate  : draftDoc.aiTokensCacheCreate,
        durationMs,
        messageCount       : messages.length,
        hadMoreMessages    : hasMore,
        elidedMessageCount : elidedCount,
        imagesAttached,
        hasCustomerContext : !!customer,
        referencedReceiptIds: parsed.referencedReceiptIds,
        toolCallCount      : loopResult.toolCalls.length,
        toolCallNames      : loopResult.toolCalls.map(tc => tc.name)
      }
    });

    // ─── 9. Respond ───────────────────────────────────────────────
    return json(200, {
      success            : true,
      draftId,
      text               : parsed.text,
      reasoning          : parsed.reasoning,
      activeQuestion     : parsed.activeQuestion,
      referencedReceiptIds: parsed.referencedReceiptIds,
      suggestedListings  : parsed.suggestedListings,
      trackingImages,
      attachments,
      toolCalls          : toolCallLog,
      tokensUsed         : {
        input       : draftDoc.aiTokensInput,
        output      : draftDoc.aiTokensOutput,
        cacheRead   : draftDoc.aiTokensCacheRead,
        cacheCreate : draftDoc.aiTokensCacheCreate,
        total       : draftDoc.aiTokensInput + draftDoc.aiTokensOutput
      },
      model              : AI_MODEL,
      effort             : AI_EFFORT,
      parsedOk,
      mode,
      iterations         : loopResult.toolCalls.length,
      imagesAttached,
      durationMs
    });

  } catch (err) {
    console.error("etsyMailDraftReply error:", err);
    await writeAudit({
      threadId, eventType: "ai_draft_failed",
      payload: { error: err.message, mode }
    }).catch(()=>{});
    return json(500, { error: err.message || String(err) });
  }
};
