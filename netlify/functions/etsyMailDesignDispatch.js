/**
 *  etsyMailDesignDispatch — v1.0.1
 *
 *  Hooks the inbox's Staff Reply card into Brites Messages (the
 *  internal design-team chat backed by Brites_Orders/{orderId}/messages
 *  in the same Firebase project).
 *
 *  Triggered when the operator flips the "Send to Design" toggle on
 *  before clicking Send via Etsy. The Etsy send proceeds independently
 *  through the existing queue; this function runs in parallel and is
 *  fire-and-forget from the UI's perspective. A failure here never
 *  blocks the Etsy send.
 *
 *  Flow:
 *    1. Validate session (operator role).
 *    2. Load the thread to learn the linked Etsy order ID.
 *    3. Pull the last few customer-side messages for context so Haiku
 *       can write a useful condensed summary.
 *    4. Ask Haiku (claude-haiku-4-5) to extract the design-team-
 *       relevant details from the operator's draft.
 *    5. Write the condensed text to
 *       Brites_Orders/{orderId}/messages/{auto-id} with the same shape
 *       the existing /design-message UI writes.
 *    6. Return the order ID, the condensed text, and a doc ref so the
 *       UI can show "Sent to Design" feedback.
 *
 *  If the thread has no linked etsyOrderId (e.g. pre-purchase inquiry),
 *  this function returns ok:false with a clear reason — the toggle in
 *  the UI should be disabled in that case, but we double-check here.
 */

"use strict";

const admin = require("./firebaseAdmin");
const { CORS } = require("./_etsyMailAuth");
const { requireSession } = require("./_etsyMailRoles");
const { callClaudeRaw } = require("./_etsyMailAnthropic");

const db = admin.firestore();
const FV = admin.firestore.FieldValue;

const THREADS_COLL       = "EtsyMail_Threads";
const BRITES_ORDERS_COLL = "Brites_Orders";

const HAIKU_MODEL = "claude-haiku-4-5-20251001";

// Cap how much we send to Haiku — design-team context wants enough
// back-and-forth to detect the current conversation arc and any
// agreed-upon spec changes, but not the whole thread history. ~20
// messages typically covers the active arc plus a small lead-in
// from the prior arc so Haiku can see the boundary.
const RECENT_MESSAGES_FOR_CONTEXT = 20;
const MAX_MESSAGE_CHARS = 1200;
const MAX_DRAFT_CHARS   = 3000;
const HAIKU_MAX_OUTPUT_TOKENS = 600;

function ok(body)  { return { statusCode: 200, headers: { "Content-Type": "application/json", ...CORS }, body: JSON.stringify({ success: true, ...body }) }; }
function bad(msg, code = 400) { return { statusCode: code, headers: { "Content-Type": "application/json", ...CORS }, body: JSON.stringify({ success: false, error: msg }) }; }

/**
 * Loads recent messages from both directions, with image URL arrays,
 * so Haiku can (a) detect the current conversation arc, (b) identify
 * design-proof images the customer accepted, and (c) pull agreed-
 * upon spec details from the back-and-forth — not just the operator's
 * latest draft.
 *
 * Returns oldest-first so the transcript reads naturally top to bottom.
 *
 * v1.0.1 — CRITICAL FIX: field name was wrong in the original.
 *
 * etsyMailSnapshot.js writes messages with field `timestamp` (a
 * Firestore Timestamp object via admin.firestore.Timestamp.fromMillis).
 * The original dispatcher queried `orderBy("timestampMs", "desc")`,
 * which Firestore handles by RETURNING ZERO DOCUMENTS because no doc
 * has a `timestampMs` field. The result: Haiku always saw "no prior
 * messages in window" and had to summarize from the operator's draft
 * alone — explaining the "wrong information passed to production"
 * complaint, since Haiku couldn't see what was actually discussed/
 * agreed/imaged earlier in the arc.
 *
 * The defensive read of `m.timestamp` handles both the canonical case
 * (Firestore Timestamp with .toMillis) and any future shape changes
 * (raw millis or seconds+nanoseconds objects) without crashing.
 */
async function loadRecentThreadMessages(threadId, limit = RECENT_MESSAGES_FOR_CONTEXT) {
  const snap = await db.collection(THREADS_COLL).doc(threadId)
    .collection("messages")
    .orderBy("timestamp", "desc")
    .limit(limit)
    .get();
  const out = [];
  snap.forEach(d => {
    const m = d.data() || {};
    const text = String(m.text || "").trim();
    const imageUrls = Array.isArray(m.imageUrls)
      ? m.imageUrls.filter(u => typeof u === "string" && u)
      : [];
    // Skip messages with neither text nor images — nothing for Haiku
    // to reason about and they're often scrape artifacts.
    if (!text && imageUrls.length === 0) return;
    const isOutbound = m.direction === "outbound" || m.senderRole === "staff";

    // v1.0.1 — Defensive timestamp extraction. The admin SDK
    // deserializes Firestore Timestamp fields into objects with a
    // .toMillis() method. Older docs (pre-snapshot-v3) may have a
    // raw timestampMs millis field. Fall back to 0 only if both fail.
    let whenMs = 0;
    if (m.timestamp && typeof m.timestamp.toMillis === "function") {
      whenMs = m.timestamp.toMillis();
    } else if (typeof m.timestampMs === "number") {
      whenMs = m.timestampMs;
    } else if (m.timestamp && typeof m.timestamp.seconds === "number") {
      // Cross-runtime defensive: if a doc has been read in a way that
      // returns the raw {seconds, nanoseconds} shape, handle it too.
      whenMs = m.timestamp.seconds * 1000 + Math.floor((m.timestamp.nanoseconds || 0) / 1e6);
    }

    out.push({
      when      : whenMs,
      direction : isOutbound ? "staff" : "customer",
      senderName: m.senderName || (isOutbound ? "Staff" : "Customer"),
      text      : text.slice(0, MAX_MESSAGE_CHARS),
      imageUrls,
    });
  });
  return out.reverse(); // oldest → newest
}

/**
 * Format the recent thread for Haiku as a transcript with timestamps
 * and explicit image markers. Each image gets a stable [IMG#N] tag so
 * Haiku can reference its URL in the JSON response without having to
 * echo back long Firebase Storage URLs verbatim.
 *
 * Returns { transcript, imageIndex } where imageIndex maps
 * "IMG1" → url, "IMG2" → url, etc. for the dispatcher to resolve.
 */
function formatThreadForHaiku(messages) {
  const imageIndex = new Map();
  let imgCounter = 0;
  const lines = [];
  for (const m of (messages || [])) {
    const when = m.when
      ? new Date(m.when).toISOString().replace("T", " ").slice(0, 16) + " UTC"
      : "(no time)";
    const who = m.direction === "staff" ? "Staff" : "Customer";
    const imageRefs = [];
    for (const url of (m.imageUrls || [])) {
      imgCounter += 1;
      const key = `IMG${imgCounter}`;
      imageIndex.set(key, url);
      imageRefs.push(`[${key}]`);
    }
    let body = (m.text || "").trim();
    if (imageRefs.length) {
      body = body
        ? `${body}  (attached: ${imageRefs.join(", ")})`
        : `(image attachment: ${imageRefs.join(", ")})`;
    }
    if (body) lines.push(`[${who} @ ${when}] ${body}`);
  }
  return { transcript: lines.join("\n\n"), imageIndex };
}

/**
 * Asks Haiku to (a) detect the current conversation arc, (b) extract
 * the agreed-upon production-relevant details from it, and (c) identify
 * which design-proof image — if any — the customer accepted.
 *
 * Output is a JSON object the dispatcher uses to decide what to write
 * to Brites Messages. Arc-detection logic mirrors the intent-classifier
 * approach so the design summary never bleeds in details from an
 * unrelated prior order on the same thread.
 */
async function condenseForDesign({ operatorDraft, threadMessages, orderId, customerName }) {
  const { transcript, imageIndex } = formatThreadForHaiku(threadMessages);
  const draftCapped = String(operatorDraft || "").slice(0, MAX_DRAFT_CHARS);

  const systemPrompt = [
    "You are a dispatcher for a custom-jewelry shop's design/production team.",
    "You read a customer-service conversation and produce ONE compact summary",
    "of the LATEST active arc — the substance the design team needs to make",
    "or modify a piece. The output is consumed in real time by the design",
    "team's internal chat (Brites Messages), so it must be telegraphic,",
    "factual, and free of CS pleasantries.",
    "",
    "═══ ARC IDENTIFICATION — THE CRITICAL JUDGMENT ═══════════════════",
    "",
    "Customer threads can span months or years. A single thread can hold a",
    "2023 order, a 2024 thank-you, and a 2026 custom request stacked",
    "together. Your output must describe ONLY the CURRENT active arc —",
    "what the customer wants RIGHT NOW based on the most recent back-and-",
    "forth.",
    "",
    "Walk backwards from the most recent message until you hit a natural",
    "arc boundary: a long quiet gap (weeks/months), a settled 'thanks!'",
    "with no follow-up, a topic pivot ('actually, also...'), or the start",
    "of the visible transcript. Everything before that boundary is",
    "context, not signal — do NOT pull spec details from an old arc into",
    "the current summary. If unsure where the arc starts, prefer the",
    "narrower interpretation (more recent messages only).",
    "",
    "═══ WHAT TO EXTRACT (anything production/fulfillment needs to know) ══",
    "",
    "OVERALL PHILOSOPHY. The operator made a deliberate choice to send",
    "this to design/production. That means the operator believes there is",
    "SOMETHING in this reply the team needs to see. Your job is to find",
    "it — not to second-guess whether it qualifies. If the customer is",
    "requesting any change to their order, asking for any action on it,",
    "or confirming any spec, that's what you surface. The strict omit-by-",
    "default rule applies to FIELDS THAT WEREN'T RAISED, not to the",
    "decision of whether to summarize at all.",
    "",
    "Production/fulfillment-relevant content falls into two buckets:",
    "",
    "─── BUCKET A: New-piece specs (when building from scratch) ───────",
    "",
    "Include each field only when it was raised in the arc. If not",
    "discussed, omit it entirely — silence means 'not part of this arc'.",
    "  - Engraving: exact text in quotes (verbatim, character-for-",
    "    character).",
    "  - Charm/family: necklace / earring / bracelet / charm / etc.",
    "  - Size: numeric size or descriptor (e.g. '9-10mm', 'small',",
    "    '18 inch chain').",
    "  - Metal: gold filled / sterling / 14k / etc.",
    "  - Chain length / style.",
    "",
    "NO PLACEHOLDERS for new-piece specs. Never output 'Size: TBD',",
    "'Metal: TBD', 'Chain: TBD', or any equivalent. If a field wasn't",
    "discussed, it doesn't appear in the summary at all.",
    "",
    "─── BUCKET B: Order modifications and fulfillment changes ────────",
    "",
    "These are explicit customer-requested changes to an EXISTING order.",
    "All of these are production/fulfillment-relevant and MUST be",
    "surfaced when the customer is requesting them (or the operator is",
    "confirming the change):",
    "  - Engraving change on an existing order (new text, different",
    "    layout, removed engraving, etc.)",
    "  - Spec swap on an existing order (metal change, size change,",
    "    chain length change, charm style change, etc.)",
    "  - Shipping address change — the customer asking to update where",
    "    the order ships. State the new address verbatim. Note that",
    "    shipping addresses are only relevant when the customer is",
    "    actively requesting a CHANGE; addresses that just appear",
    "    incidentally in a message (e.g., in a signature) are not.",
    "  - Order cancellation or hold request.",
    "  - Rush request on an existing order (\"can you ship sooner\",",
    "    \"need it by X\").",
    "  - Add-on to an unstarted order (additional piece, second charm,",
    "    etc.).",
    "",
    "When the customer explicitly raises any of the above:",
    "  - Lead with what's being CHANGED (\"Shipping address update:\",",
    "    \"Engraving change:\", etc.)",
    "  - State the new value verbatim.",
    "  - If the existing-order reference is to a specific order number,",
    "    include it.",
    "",
    "─── ADDITIONAL FLAGS (only when explicitly raised) ───────────────",
    "",
    "  - Rush production (only if the word 'rush' or equivalent urgency",
    "    appears — never default to 'no rush').",
    "  - Deadline date (date only, never as a promise — only if the",
    "    customer named a specific date).",
    "",
    "─── FORBIDDEN — never include these ──────────────────────────────",
    "",
    "  - Pleasantries, thanks, signoffs",
    "  - Delivery-window discussion / shipping ETAs from the shop side",
    "    (\"we'll ship in 4-5 days\") — these are ops-side timing, not a",
    "    production input.",
    "  - Sales/quoting math, prices, taxes",
    "  - Speculation about future or follow-up orders ('customer may",
    "    order more later', 'might do another piece', etc.) — even when",
    "    the customer said it.",
    "  - Customer mood or emotion ('excited', 'happy', 'frustrated')",
    "  - Backstory or context not directly tied to a spec or change",
    "    (\"this is for her birthday\", \"she's been wanting one for",
    "    months\", etc.)",
    "  - Editorial flags like '(not actionable yet)', '(low priority)'",
    "  - Workflow meta-commentary about who decides what: phrases like",
    "    'operator's call on layout', 'designer's choice', 'use your",
    "    judgment'. State the SPEC, not who made the decision. If the",
    "    operator committed to a layout, that committed layout IS the",
    "    spec — full stop. If something genuinely remains open for the",
    "    designer, state it as a direct instruction: 'Font: designer",
    "    discretion' is fine; 'Operator's call on font' is editorial",
    "    commentary about the workflow.",
    "",
    "STILL-DISCUSSING — if a field was raised in the arc but not yet",
    "settled, mention it plainly: 'metal still being discussed' or",
    "'size pending customer choice'. Only include this when the customer",
    "or operator actually brought the field up. If neither raised it,",
    "omit it entirely — don't add 'TBD' as a default reminder.",
    "",
    "═══ FORMAT — TELEGRAPHIC, LABEL-STYLE ═══════════════════════════",
    "",
    "Target: 15-40 words. The design team is scanning a queue. Use",
    "label-style fragments, not full sentences. Periods between fields,",
    "not commas or conjunctions.",
    "",
    "GOOD output (notice — only fields that were actually raised):",
    "  Necklace charm. Engraving: 'Maryland Black Bears 2026 NAHL",
    "  Champions'.",
    "",
    "  Bracelet, sterling, 7 inch. Engraving: 'Mom 2024' on inside.",
    "",
    "  Figure skater necklace. Engraving on back of boot: 'My Dream'",
    "  split across two lines — 'My' on top (narrower part) /",
    "  'Dream' below. Make as large as possible.",
    "",
    "  Modify order: swap 14k charm to gold filled, same engraving",
    "  ('Sarah'). Rush requested — needs by May 30.",
    "",
    "  Shipping address update for order #4072468345: 513 Mount Vernon",
    "  Rd, Greer, SC 29651. Update before fulfillment.",
    "",
    "  Engraving change on order #4123: from 'My Dream' to a heart",
    "  symbol. Order still in paid-not-shipped state.",
    "",
    "  Order hold request: customer asking to pause order #4099 while",
    "  she reconsiders the metal choice.",
    "",
    "BAD output (rejected — placeholder padding):",
    "  ✗ 'Necklace charm. Engraving: \"Mom 2024\". Size: TBD. Metal: TBD.'",
    "    Problem: Size and Metal were never raised in the arc. Adding",
    "    them as TBD placeholders is forbidden — silence equals 'not",
    "    part of this arc'.",
    "",
    "BAD output (rejected — verbose, speculative, padded):",
    "  ✗ 'Customer confirmed engraving \"Maryland Black Bears 2026 NAHL",
    "    Champions\" on the necklace charm — size and metal still to",
    "    be selected from line sheet. No rush. Customer mentioned",
    "    possible follow-up order after seeing first piece (not",
    "    actionable yet).'",
    "    Problems: 'Customer confirmed' is filler, 'No rush' was never",
    "    asked about, 'follow-up order' is forbidden speculation,",
    "    '(not actionable yet)' is editorial flagging, and size/metal",
    "    weren't raised in this arc so they shouldn't appear at all.",
    "",
    "  ✗ 'The customer would like a beautiful necklace with the words",
    "    \"Mom\" engraved on it in gold.'",
    "    Problems: 'beautiful', 'would like' are pleasantries.",
    "    Should be: 'Necklace charm, gold filled. Engraving: \"Mom\".'",
    "",
    "The operator just typed a new reply that's about to be sent. Treat",
    "it as the most recent message in the arc — its content may confirm,",
    "modify, or extend the customer's prior asks.",
    "",
    "═══ IMAGE ACCEPTANCE — WHICH PROOF DID THEY APPROVE? ═════════════",
    "",
    "The shop often sends design-proof images as attachments. The design",
    "team needs to know which proof was accepted so they make the right",
    "piece. The transcript marks images with [IMG1], [IMG2], etc.",
    "",
    "Look at staff-sent images and the customer's subsequent reaction.",
    "Signals of ACCEPTANCE:",
    "  - explicit approval (Yes! / Approved / Love it / Perfect)",
    "  - direct confirmation (That's the one / Looks great)",
    "  - go-ahead language (Let's go with that / Ready to order)",
    "",
    "Signals of NON-ACCEPTANCE — leave acceptedImage null:",
    "  - revision request (Can you make X bigger / Try a different font)",
    "  - rejection (Not quite right / Can you try again)",
    "  - no clear response (customer asked something else after, or",
    "    didn't reply, or kept negotiating)",
    "  - the image is from an old arc that has since closed",
    "",
    "When ONE staff-attached image was clearly accepted in the current",
    "arc, return its tag in acceptedImage (e.g. 'IMG3'). When multiple",
    "images were sent and only one accepted, pick that one. When in",
    "doubt — leave it null. A false-positive attachment misleads the",
    "design team more than a missing one.",
    "",
    "═══ ACK-ONLY SHORTCUT ════════════════════════════════════════════",
    "",
    "If the latest arc is purely an acknowledgment with no actionable",
    "details for design (e.g. answering a tracking question, replying",
    "to praise, sending a refund update), say so plainly in the summary",
    "and set hasDesignAction: false.",
    "",
    "═══ OUTPUT — JSON ONLY ═══════════════════════════════════════════",
    "",
    "Respond with ONLY a JSON object, no preamble, no markdown fences:",
    "{",
    '  "summary": "<15-40 word label-style spec, periods between fields>",',
    '  "hasDesignAction": true | false,',
    '  "acceptedImage": "IMG3" | null,',
    '  "acceptedImageReason": "<MAX 12 WORDS — why this image, or null>"',
    "}",
    "",
    "Brevity is mandatory. Every word that isn't a production spec is",
    "noise that costs the design team scanning time. If a field wasn't",
    "discussed in the arc, omit it — silence equals 'not applicable'.",
  ].join("\n");

  const userMessage = [
    `Order #: ${orderId || "(unknown)"}`,
    `Customer: ${customerName || "(unknown)"}`,
    "",
    "═══ RECENT THREAD (oldest at top, newest at bottom) ═══",
    transcript || "(no prior messages in window)",
    "",
    "═══ OPERATOR'S REPLY (just typed, about to be sent to customer) ═══",
    draftCapped,
    "",
    "Produce the JSON now.",
  ].join("\n");

  const resp = await callClaudeRaw({
    model       : HAIKU_MODEL,
    maxTokens   : HAIKU_MAX_OUTPUT_TOKENS,
    system      : systemPrompt,
    messages    : [{ role: "user", content: userMessage }],
    useThinking : false,
  });

  const blocks = Array.isArray(resp && resp.content) ? resp.content : [];
  const textBlocks = blocks.filter(b => b && b.type === "text" && typeof b.text === "string");
  const raw = textBlocks.map(b => b.text).join("").trim();
  if (!raw) throw new Error("Haiku returned no text content");

  // Strip accidental markdown fences if Haiku ever decides to wrap.
  const cleaned = raw
    .replace(/^```(?:json)?\s*/i, "")
    .replace(/\s*```\s*$/i, "")
    .trim();

  let parsed;
  try {
    parsed = JSON.parse(cleaned);
  } catch (e) {
    console.warn("[designDispatch] non-JSON Haiku output:", cleaned.slice(0, 400));
    throw new Error("Haiku returned non-JSON output");
  }

  // Empty-summary handling. When Haiku returns no summary text, the
  // operator's deliberate toggle action is still respected — we don't
  // silently drop the dispatch just because Haiku didn't find anything
  // to extract. The operator chose to send this; that's a signal that
  // something in the reply belongs in front of production/design,
  // even if Haiku's pattern-matching missed it.
  //
  // Fallback: pass the operator's own reply through verbatim (capped),
  // so the production team sees the message and can decide for
  // themselves whether it's actionable. We log a warning so we can
  // tune the prompt later if this case becomes common.
  let summary = String(parsed.summary || "").trim();
  if (!summary) {
    const draftSnippet = String(operatorDraft || "").trim().slice(0, 600);
    summary = draftSnippet
      ? "Operator dispatched: " + draftSnippet
      : "Operator triggered design dispatch with empty reply.";
    console.warn("[designDispatch] Haiku returned empty summary; falling back to operator draft");
  }

  // Resolve acceptedImage tag → URL via the index we built.
  let acceptedImageUrl = null;
  const tag = (typeof parsed.acceptedImage === "string") ? parsed.acceptedImage.trim() : null;
  if (tag && imageIndex.has(tag)) acceptedImageUrl = imageIndex.get(tag);
  // Safety net: if Haiku returned a raw URL instead of a tag, accept it
  // only if it appears in our imageIndex (prevents Haiku from inventing
  // URLs that didn't exist in the thread).
  if (!acceptedImageUrl && tag && tag.startsWith("http")) {
    for (const url of imageIndex.values()) {
      if (url === tag) { acceptedImageUrl = url; break; }
    }
  }

  return {
    summary,
    hasDesignAction    : parsed.hasDesignAction !== false, // default true
    acceptedImageUrl,
    acceptedImageReason: String(parsed.acceptedImageReason || "").trim() || null,
  };
}

exports.handler = async (event) => {
  if (event.httpMethod === "OPTIONS") return { statusCode: 200, headers: CORS, body: "ok" };
  if (event.httpMethod !== "POST")    return bad("Method not allowed", 405);

  // Session auth — operator-initiated action, must be a known user.
  const auth = await requireSession(event);
  if (!auth.ok) return bad(auth.error || "Unauthorized", 401);
  const employeeName = String(auth.user?.displayName || auth.user?.username || "Operator");

  let body;
  try { body = JSON.parse(event.body || "{}"); }
  catch (_) { return bad("Invalid JSON body"); }

  const threadId   = String(body.threadId   || "").trim();
  const draftText  = String(body.draftText  || "").trim();
  const senderName = String(body.employeeName || employeeName).trim();

  if (!threadId)  return bad("Missing threadId");
  if (!draftText) return bad("Missing draftText");

  // v1.0.1 — Entry-time audit log. The original audit was at the END
  // (after success), meaning if the function crashed early (Haiku
  // timeout, Firestore transient on the load, etc.) we had NO record
  // the operator even tried. Result: "messages not arriving" felt
  // mysterious because the only paper trail was the dispatched
  // message itself.
  //
  // Now we write a "started" audit row IMMEDIATELY on entry, best-
  // effort (silent catch). Every attempt leaves a trace. The success
  // audit at the end still fires with the dispatched message IDs;
  // having both lets you correlate "started but never finished" rows
  // with the actual failure to see where the dispatch died.
  const dispatchStartedAt = Date.now();
  try {
    await db.collection("EtsyMail_Audit").add({
      eventType : "design_dispatch_started",
      threadId,
      actor     : senderName,
      payload   : {
        draftLength    : draftText.length,
        draftPreview   : draftText.slice(0, 120),
      },
      createdAt : FV.serverTimestamp(),
    });
  } catch (_) { /* best-effort */ }

  // ─── 1) Load thread to learn the order ID and customer name ──────
  const tSnap = await db.collection(THREADS_COLL).doc(threadId).get();
  if (!tSnap.exists) return bad("Thread not found", 404);
  const thread = tSnap.data() || {};

  const orderId = thread.etsyOrderId ? String(thread.etsyOrderId).trim() : null;
  if (!orderId) {
    // No linked order — Brites_Orders is keyed by order ID, so we
    // genuinely have nowhere to write. The UI should pre-disable the
    // toggle in this case; this is the server-side double-check.
    return bad("Thread has no linked etsyOrderId — cannot route to Brites Messages", 422);
  }

  const customerName = (thread.participants || []).find(p => p && p.role === "customer")?.displayName
    || thread.customerName
    || "(unknown customer)";

  // ─── 2) Pull recent thread messages (both directions, with images) ─
  let threadMessages = [];
  try {
    threadMessages = await loadRecentThreadMessages(threadId);
  } catch (e) {
    // Non-fatal — Haiku can still produce a useful summary from just
    // the operator's draft, though arc detection will be weaker.
    console.warn(`[designDispatch] failed to load recent messages for ${threadId}: ${e.message}`);
  }

  // v1.0.1 — Loud warning when the thread-context load returns nothing.
  // The original timestampMs-vs-timestamp bug caused this to be silently
  // empty on EVERY dispatch, and there was no signal anywhere — Haiku
  // just got "(no prior messages in window)" forever and hallucinated.
  //
  // After the v1.0.1 fix this should be non-empty for any thread that's
  // been scraped at least once. If you ever see this warning post-fix,
  // it means either (a) the field name changed again, or (b) the
  // thread genuinely has no messages yet (legitimately fresh).
  if (threadMessages.length === 0) {
    console.warn(`[designDispatch] thread ${threadId} produced ZERO context messages — Haiku will summarize from operator draft only. If this thread has visible messages, the messages subcollection field schema may have changed.`);
  }

  // ─── 3) Condense via Haiku (arc-aware + image-aware) ─────────────
  let dispatch;
  try {
    dispatch = await condenseForDesign({
      operatorDraft : draftText,
      threadMessages,
      orderId,
      customerName,
    });
  } catch (e) {
    console.error(`[designDispatch] Haiku failed for thread ${threadId}:`, e.message);
    return bad(`Summarization failed: ${e.message}`, 502);
  }
  const { summary: condensed, acceptedImageUrl, acceptedImageReason, hasDesignAction } = dispatch;

  // ─── 4) Write to Brites_Orders/{orderId}/messages ────────────────
  //
  // The existing design-message.html writes directly to the messages
  // subcollection without ensuring the parent doc exists. Firestore
  // tolerates this — the parent becomes a "ghost" doc visible only by
  // direct ID lookup. That's fine for design-message's UX (operator
  // types the exact order # into a search box), but if the design
  // team's other tooling lists orders via `collection().get()`, ghost
  // docs are invisible.
  //
  // To make every dispatched message reliably surface in any list-
  // style view, we materialize the parent doc with merge:true BEFORE
  // the subcollection write. We do NOT seed product/customer/quote
  // fields — those belong to intake. We only add message-tracking
  // metadata so listing + sorting works.
  //
  // When Haiku identified an accepted design proof, we ALSO write a
  // SECOND message holding just the image URL. design-message.html
  // renders text and image as separate bubbles (text OR image per
  // doc, never both) — so two writes match the existing UI shape.
  //
  // v1.0.1 — Wrap each write in a retry loop. Firestore can return
  // transient UNAVAILABLE / DEADLINE_EXCEEDED / INTERNAL errors under
  // load; without retry, one of those = lost message + 502 to client.
  // The original symptom "sometimes messages don't arrive" almost
  // certainly included one or more transient failures of this kind.
  //
  // Retry policy: 3 attempts, backoff 250ms → 750ms → 2250ms (3x).
  // Total worst-case latency added: ~3.5s. Acceptable given dispatch
  // is fire-and-forget UX-wise (operator already saw "Sent ✓").
  // Only retry on transient codes; permanent errors (PERMISSION_DENIED,
  // INVALID_ARGUMENT) fail fast since retrying won't help.
  const TRANSIENT_ERROR_CODES = new Set([
    "unavailable", "deadline-exceeded", "internal", "aborted",
    "resource-exhausted", "cancelled"
  ]);
  async function withRetry(label, fn) {
    let lastErr;
    for (let attempt = 1; attempt <= 3; attempt++) {
      try {
        return await fn();
      } catch (e) {
        lastErr = e;
        const code = (e && (e.code || "")).toString().toLowerCase();
        const isTransient = TRANSIENT_ERROR_CODES.has(code);
        if (!isTransient || attempt === 3) {
          console.warn(`[designDispatch] ${label} attempt ${attempt} failed (code=${code || "?"}); ${isTransient ? "out of retries" : "non-transient"}: ${e.message}`);
          throw e;
        }
        const backoffMs = 250 * Math.pow(3, attempt - 1);
        console.warn(`[designDispatch] ${label} attempt ${attempt} transient (${code}); retrying in ${backoffMs}ms: ${e.message}`);
        await new Promise(r => setTimeout(r, backoffMs));
      }
    }
    throw lastErr; // unreachable but quiets linters
  }

  let textWriteRef;
  let imageWriteRef = null;
  try {
    const orderRef = db.collection(BRITES_ORDERS_COLL).doc(orderId);

    await withRetry("orderRef.set", () => orderRef.set({
      orderId            : orderId,
      lastMessageAt      : FV.serverTimestamp(),
      lastMessageBy      : senderName,
      lastMessageSource  : "etsymail_design_dispatch",
      customerName       : customerName,
    }, { merge: true }));

    // Text message — the condensed substance.
    textWriteRef = await withRetry("messages.add (text)", () => orderRef
      .collection("messages")
      .add({
        text       : condensed,
        senderName : senderName,
        senderRole : "staff",
        timestamp  : FV.serverTimestamp(),
        source     : "etsymail_design_dispatch",
        sourceThreadId : threadId,
        sourceOriginalLength : draftText.length,
        hasDesignAction,
      }));

    // Image message — only when Haiku confidently identified an
    // accepted design proof in the current arc.
    if (acceptedImageUrl) {
      imageWriteRef = await withRetry("messages.add (image)", () => orderRef
        .collection("messages")
        .add({
          imageUrl   : acceptedImageUrl,
          text       : "Approved design proof",
          senderName : senderName,
          senderRole : "staff",
          timestamp  : FV.serverTimestamp(),
          source     : "etsymail_design_dispatch_image",
          sourceThreadId : threadId,
          acceptedImageReason : acceptedImageReason || null,
        }));
    }
  } catch (e) {
    console.error(`[designDispatch] Firestore write failed for order ${orderId} after retries:`, e.message);
    // v1.0.1 — Write a failure-audit row so the operator can correlate
    // their "I clicked Send to Design and nothing showed up" complaint
    // with a concrete server-side failure. Best-effort — if even this
    // audit write fails, we still 502 the client.
    try {
      await db.collection("EtsyMail_Audit").add({
        eventType : "design_dispatch_write_failed",
        threadId,
        etsyOrderId: orderId,
        actor     : senderName,
        payload   : {
          errorMessage   : e.message || String(e),
          errorCode      : (e && e.code) || null,
          condensedLength: (condensed || "").length,
          acceptedImageUrl: acceptedImageUrl || null,
          elapsedMs      : Date.now() - dispatchStartedAt,
        },
        createdAt : FV.serverTimestamp(),
      });
    } catch (_) { /* best-effort */ }
    return bad(`Write to Brites_Orders failed: ${e.message}`, 502);
  }

  // ─── 5) Audit so we can correlate later ──────────────────────────
  try {
    await db.collection("EtsyMail_Audit").add({
      eventType : "design_dispatch",
      threadId,
      etsyOrderId: orderId,
      actor: senderName,
      payload: {
        condensedLength    : condensed.length,
        originalLength     : draftText.length,
        britesMessageId    : textWriteRef.id,
        britesImageMessageId: imageWriteRef ? imageWriteRef.id : null,
        acceptedImageUrl   : acceptedImageUrl || null,
        hasDesignAction,
      },
      createdAt: FV.serverTimestamp(),
    });
  } catch (_) { /* audit write is best-effort */ }

  return ok({
    orderId,
    britesMessageId     : textWriteRef.id,
    britesImageMessageId: imageWriteRef ? imageWriteRef.id : null,
    condensed,
    customerName,
    acceptedImageUrl    : acceptedImageUrl || null,
    acceptedImageReason : acceptedImageReason || null,
    hasDesignAction,
  });
};
