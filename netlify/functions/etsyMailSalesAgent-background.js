/*  netlify/functions/etsyMailSalesAgent-background.js
 *
 *  v4.0 — Unified sales agent (background function).
 *
 *  ═══ WHY BACKGROUND ═══
 *
 *  The synchronous version of this function was hitting Netlify's 26-second
 *  sync invocation cap on every Opus call with thinking enabled, producing
 *  intermittent 504 gateway timeouts that caused drafts to land but never
 *  enqueue (the auto-pipeline gave up at the timeout). Converting to a
 *  background function (15-minute cap) fixes the timeout class entirely.
 *
 *  The auto-pipeline now fires the agent and polls EtsyMail_Drafts for
 *  the agent's write rather than awaiting the response. See callers in
 *  etsyMailAutoPipeline-background.js.
 *
 *  ═══ WHAT THE AGENT DOES ═══
 *
 *  Custom Brites sells three custom product families:
 *    • huggie    (Custom Huggie Charm earrings, sold as set of 2)
 *    • necklace  (Custom Necklace Charm + optional chain)
 *    • stud      (Custom Stud Earrings — pair, single, or mismatched)
 *  Every customer selection is a code (1A, 2B, 3C, etc.). The resolver
 *  validates each code, sums prices, applies bulk-tier discount, and
 *  returns the exact total. The AI cannot invent a price.
 *
 *  ═══ NO STAGES ═════════════════════════════════════════════════════════
 *
 *    discovery                                spec
 *        |                                     |
 *        v                                     v
 *      [spec] ─── back-edge ───> [discovery]   [quote]
 *                                                |  ^
 *                                                v  |
 *                                        [revision] (loop)
 *                                                |
 *                                                v
 *                              [pending_close_approval]   <-- Step 3 picks up here
 *
 *  Off-ramps from any stage: human_review, abandoned.
 *  Server enforces transitions against STAGE_FLOW.canSkipTo.
 *  AI cannot skip stages it isn't allowed to skip.
 *
 *  ═══ REQUEST ═══════════════════════════════════════════════════════════
 *
 *  POST {
 *    threadId               : "etsy_conv_...",                      // required
 *    latestInboundText      : "<last customer message text>",
 *    latestInboundAttachments: [{ url: "<https://...>" }, ...],     // optional
 *    customerHistory        : { isRepeat, orderCount, lifetimeValueUsd },
 *    intentClassification   : "sales_lead",                         // from auto-pipeline
 *    intentConfidence       : 0.85,
 *    employeeName           : "system:auto-pipeline" | "Paul_K"
 *  }
 *
 *  ═══ RESPONSE ══════════════════════════════════════════════════════════
 *
 *  {
 *    success                    : true,
 *    stage                      : "spec" | "quote" | ...,
 *    draftId                    : "draft_etsy_conv_...",
 *    confidence                 : 0.85,
 *    ready_for_human_approval   : false,
 *    draft_custom_order_listing : null | {...},   // present at close stage
 *    quoteValidation            : null | {...},
 *    isNeedsReviewHandoff       : true | false,   // v2.1
 *    durationMs                 : 12345
 *  }
 *
 *  ═══ KEY DESIGN DECISIONS ══════════════════════════════════════════════
 *
 *  1. **Option-sheet resolver replaces band pricing.** resolveQuote is
 *     the single source of truth for prices. Customer selections are
 *     codes (1A, 2B, ...); each code maps to a fixed price; sum, discount,
 *     done. No more min/max bands; no more "AI quoted within range".
 *
 *  2. **Direct imports for sibling helpers.** resolveQuote (option resolver),
 *     searchListings, searchCollateral imported as JS functions —
 *     no HTTP round-trips. Same pattern Step 1 established.
 *
 *  3. **Prompts: file-system source of seed, Firestore override.**
 *     EtsyMail_SalesPrompts/{stage} is the operator-editable override.
 *     Falls back to prompts/sales/{stage}.md (bundled via netlify.toml's
 *     included_files). Hard-fail if neither present (Step 1 fix #1 pattern).
 *
 *  4. **Drafts are STORED, not ENQUEUED.** status:"draft" — operator
 *     manually clicks Send in the UI. Step 3 introduces enqueue-on-approval
 *     for the close artifact only.
 *
 *  5. **Soft escalation on Quote-row codes.** If the resolver returns
 *     escalations[] populated (a code is priceQuote:true), the agent
 *     gathers everything else, then composes a Needs Review synopsis
 *     and routes the thread to pending_human_review. The synopsis
 *     becomes the draft body (operator-facing); the customer-facing
 *     reply is preserved on the draft as customerFacingReplyDraft.
 *
 *  6. **validateQuotedPriceIfPresent re-runs the resolver server-side.**
 *     The AI's quoted_total_usd MUST match the resolver's total within
 *     1 cent. Mismatch → escalation. Quote-row at validation time →
 *     escalation. Hallucinated prices cannot reach the customer.
 *
 *  7. **Vision input.** Inbound image attachments flow into the agent's
 *     user content as Anthropic image blocks (URL form). Etsy CDN URLs
 *     are publicly fetchable; Anthropic's API accepts them.
 *
 *  8. **Audit shape (canonical).**
 *     { threadId, draftId, eventType, actor, payload,
 *       createdAt: serverTimestamp(),
 *       outcome  : "success" | "failure" | "blocked",
 *       ruleViolations: [...] }
 *
 *  ═══ ENV VARS ══════════════════════════════════════════════════════════
 *
 *    ANTHROPIC_API_KEY             required
 *    ETSYMAIL_EXTENSION_SECRET     gates this endpoint
 *    ETSYMAIL_SALES_MODEL          override; default claude-opus-4-7
 *    ETSYMAIL_SALES_EFFORT         override; default "high"
 *    ETSYMAIL_SALES_MAX_TOKENS     override; default 6000
 */

const fs   = require("fs");
const path = require("path");

const admin = require("./firebaseAdmin");
const { CORS, requireExtensionAuth } = require("./_etsyMailAuth");
const { runToolLoop } = require("./_etsyMailAnthropic");
// ─── In-bundle module imports — guarded with try/catch ────────────────
//
// These modules are all part of the Step 2.3 bundle and should always be
// present in a correct deployment. The try/catch guards serve two purposes:
//
//  (a) Graceful degradation during partial/phased rollouts where not every
//      function file has been deployed yet. Without the guard, a MODULE_NOT_
//      FOUND error on ANY of these prevents the entire salesAgent module from
//      loading — which means EVERY request fails with a cryptic 502, not just
//      the specific tool that needed that module.
//
//  (b) Protection against syntax errors or missing native dependencies in a
//      sibling module crashing this module at load time. Quarantines the blast
//      radius to the specific tool(s) that depend on the broken module.
//
// Tool executors check for null and return a structured error that the AI
// model can reason about and escalate from, rather than throwing.

let searchListings = null;
try {
  ({ searchListings } = require("./etsyMailListingsCatalog"));
} catch (e) {
  console.warn("salesAgent: etsyMailListingsCatalog not loadable — search_shop_listings tool will return graceful empty.", e.message);
}

// v2.1 — Direct import of the deterministic option-sheet resolver.
// Replaces v2.0's band-pricing engine (etsyMailSalesPricing).
// resolveQuote is load-critical for the sales agent (without it the
// agent cannot quote prices). If it fails to load, the resolveQuote
// tool returns a RESOLVER_UNAVAILABLE error and the agent must escalate
// to human review rather than hallucinating a price.
let resolveQuote = null;
let loadOptionSheet = null;
try {
  ({ resolveQuote, loadSheet: loadOptionSheet } = require("./etsyMailOptionResolver"));
} catch (e) {
  console.error("salesAgent: etsyMailOptionResolver not loadable — resolveQuote / get_option_sheet tools will be unavailable. Sales quoting cannot proceed.", e.message);
}

// v2.3 — Direct import of the listing URL parser + lookup. Used for the
// new lookup_listing_by_url tool exposed to the agent at every stage.
// Etsy-API-first lookup with cache fallback; see etsyMailListingsCatalog
// header for the resolution hierarchy.
// v2.4 — These helpers were folded into etsyMailListingsCatalog (was a
// separate etsyMailListingLookup.js); the import path moved but the
// surface is identical.
let lookupListingByUrl = null;
let lookupListingById  = null;
try {
  ({ lookupListingByUrl, lookupListingById } = require("./etsyMailListingsCatalog"));
} catch (e) {
  console.warn("salesAgent: etsyMailListingsCatalog (lookup helpers) not loadable — listing lookup tools will return graceful errors.", e.message);
}

// Step 2.5 — collateral search. Imported directly; if Step 2.5 hasn't
// been deployed yet (etsyMailCollateral.js missing), the require will
// throw at module load. Guard around it so the agent still works
// without collateral.
let searchCollateral = null;
try {
  ({ searchCollateral } = require("./etsyMailCollateral"));
} catch (e) {
  console.warn("salesAgent: etsyMailCollateral not loadable — get_collateral tool will return graceful empty.", e.message);
}

const db = admin.firestore();
const FV = admin.firestore.FieldValue;

// ─── Collections ────────────────────────────────────────────────────────
const SALES_COLL    = "EtsyMail_SalesContext";
const PROMPTS_COLL  = "EtsyMail_SalesPrompts";
const THREADS_COLL  = "EtsyMail_Threads";
const DRAFTS_COLL   = "EtsyMail_Drafts";
const AUDIT_COLL    = "EtsyMail_Audit";
const CONFIG_COLL   = "EtsyMail_Config";

// ─── Model config ───────────────────────────────────────────────────────
const AI_MODEL          = process.env.ETSYMAIL_SALES_MODEL || "claude-opus-4-7";
// AI_EFFORT controls how much "thinking" budget Anthropic's Opus 4.7
// allocates to each sales-agent turn. Allowed values per Anthropic API:
//   low | medium | high | xhigh | max
//
// "high" is a sensible default for sales-mode: the funnel state machine
// rewards careful spec extraction and option-resolver use, which benefit
// from extra reasoning over the cheaper "medium" tier. Override per-deploy
// via the ETSYMAIL_SALES_EFFORT env var.
//
// IMPORTANT: only the five allowed values above will work. Any other
// value (e.g. legacy "balanced") will cause Anthropic to 400 every call
// with `output_config.effort: Input should be 'low'/'medium'/'high'/...`.
// We validate at module load to fail fast rather than per-request.
const _ALLOWED_EFFORTS = new Set(["low", "medium", "high", "xhigh", "max"]);
const _RAW_EFFORT = process.env.ETSYMAIL_SALES_EFFORT || "high";
const AI_EFFORT = _ALLOWED_EFFORTS.has(_RAW_EFFORT) ? _RAW_EFFORT : "high";
if (_RAW_EFFORT !== AI_EFFORT) {
  console.warn(`salesAgent: ETSYMAIL_SALES_EFFORT='${_RAW_EFFORT}' is not a valid effort — falling back to '${AI_EFFORT}'. Allowed: ${[..._ALLOWED_EFFORTS].join(", ")}`);
}
const AI_MAX_TOKENS     = parseInt(process.env.ETSYMAIL_SALES_MAX_TOKENS || "6000", 10);
const MAX_TOOL_ITERATIONS = 8;

// ─── Config cache (matches Step 1 pattern) ──────────────────────────────
let _cfgCache = { value: null, fetchedAt: 0 };
const CFG_CACHE_MS = 15 * 1000;

async function getConfig() {
  if (_cfgCache.value && (Date.now() - _cfgCache.fetchedAt < CFG_CACHE_MS)) {
    return _cfgCache.value;
  }
  let value = {
    salesModeEnabled       : false,
    salesAutoEngage        : false,
    salesPilotThreadIds    : [],
    listingsMirrorEnabled  : false,
    intentClassifierEnabled: false
  };
  try {
    const doc = await db.collection(CONFIG_COLL).doc("autoPipeline").get();
    if (doc.exists) {
      const d = doc.data() || {};
      value = {
        salesModeEnabled       : d.salesModeEnabled === true,
        salesAutoEngage        : d.salesAutoEngage === true,
        salesPilotThreadIds    : Array.isArray(d.salesPilotThreadIds) ? d.salesPilotThreadIds : [],
        listingsMirrorEnabled  : d.listingsMirrorEnabled === true,
        intentClassifierEnabled: d.intentClassifierEnabled === true
      };
    }
    _cfgCache = { value, fetchedAt: Date.now() };
  } catch (e) {
    console.warn("salesAgent: config fetch failed:", e.message);
  }
  return value;
}

// ─── State (no stages) ─────────────────────────────────────────────────
//
// v4.0: Stage decomposition has been removed. The agent runs ONE prompt
// per inbound, decides what to do based on conversation state, and uses
// any tool it needs (including resolveQuote at the moment it judges the
// customer is ready). What the old stage system tracked — accumulated
// spec, quote history, last resolver result — is still persisted on
// SalesContext, but the agent no longer thinks in stage names.
//
// engageable means: this thread is in a state where the agent should
// run on the next inbound. terminal means: the customer accepted the
// quote / the order was placed / a human took over. The agent doesn't
// run on terminal threads.
//
// v4.3.1 — `pending_human_review` is intentionally NOT in this set, even
// though the agent itself can write that status when it decides a turn
// needs help. "Needs review on this turn" ≠ "conversation is over". When
// the customer replies later — possibly with the very information the
// agent was missing — the agent must re-engage and produce a fresh
// draft. Including pending_human_review here caused a regression where
// every subsequent inbound was silently skipped, leaving threads parked
// in pending_human_review with no draft for the operator to review.
//
// Truly terminal states:
//   sales_completed — listing-creator worker ran markSuccess (the sale
//                     closed; subsequent customer messages route to the
//                     standard customer-service draft pipeline, not the
//                     sales agent)
//   sales_abandoned — reaper concluded the lead is dead

const TERMINAL_THREAD_STATUSES = new Set([
  "sales_completed",
  "sales_abandoned"
]);

// ─── Prompt loading ────────────────────────────────────────────────────
//
// v4.0: ONE prompt at EtsyMail_SalesPrompts/sales. No stage decomposition.
// Edited via Settings → Agent Prompts in the dashboard.

const SALES_PROMPT_DOC_ID = "sales";

async function loadSalesPrompt() {
  try {
    const doc = await db.collection(PROMPTS_COLL).doc(SALES_PROMPT_DOC_ID).get();
    if (!doc.exists) {
      return {
        ok: false,
        error: `Sales prompt missing. Expected ${PROMPTS_COLL}/${SALES_PROMPT_DOC_ID} ` +
               `with field "systemPrompt". Upload it via Settings → Agent Prompts.`
      };
    }
    const d = doc.data() || {};
    const sp = typeof d.systemPrompt === "string" ? d.systemPrompt : "";
    if (sp.length < 100) {
      return {
        ok: false,
        error: `Sales prompt is too short (${sp.length} chars). Likely a placeholder.`
      };
    }
    return { ok: true, prompt: sp };
  } catch (e) {
    return { ok: false, error: `Sales prompt load failed: ${e.message}` };
  }
}

// ─── Sales context load/init ───────────────────────────────────────────

async function loadOrInitSalesContext(threadId) {
  const ref = db.collection(SALES_COLL).doc(threadId);
  const doc = await ref.get();
  if (doc.exists) {
    return { ...doc.data(), _ref: ref, _isNew: false };
  }
  const init = {
    threadId,
    stage             : "discovery",
    accumulatedSpec   : {},
    missingInputs     : [],
    quoteHistory      : [],
    itemsProposed     : [],
    itemsAccepted     : [],
    totalQuotedUsd    : null,
    discountAppliedPct: 0,
    operatorOverrides : [],
    createdAt         : FV.serverTimestamp(),
    lastTurnAt        : FV.serverTimestamp(),
    lastAdvancedAt    : FV.serverTimestamp(),
    abandonedAt       : null,
    lastSalesAgentBlockReason: null
  };
  await ref.set(init);
  return { ...init, _ref: ref, _isNew: true };
}

// ─── Audit helper (canonical Step 2 shape) ─────────────────────────────

async function writeAudit({ threadId = null, draftId = null, eventType,
                            actor = "sales-agent", payload = {},
                            outcome = "success", ruleViolations = [] }) {
  try {
    await db.collection(AUDIT_COLL).add({
      threadId, draftId, eventType, actor, payload,
      createdAt: FV.serverTimestamp(),
      outcome, ruleViolations
    });
  } catch (e) {
    console.warn("salesAgent audit write failed:", e.message);
  }
}

// ─── Sales-speed / context helpers ───────────────────────────────────

function isCustomerVisibleUrl(url) {
  return typeof url === "string"
    && /^https?:///i.test(url)
    && !/REPLACE_WITH_PUBLIC_URL/i.test(url)
    && !/example.com/i.test(url);
}

function normalizeAttachment(raw, source = "thread") {
  if (!raw) return null;
  const url = typeof raw === "string"
    ? raw
    : (raw.url || raw.proxyUrl || raw.imageUrl || raw.attachmentUrl || raw.href || "");
  if (!isCustomerVisibleUrl(url)) return null;
  return {
    url,
    source,
    type: raw.type || (raw.contentType && /^image//i.test(raw.contentType) ? "image" : "file"),
    contentType: raw.contentType || null,
    filename: raw.filename || raw.name || null
  };
}

function mergeAttachments(...sets) {
  const out = [];
  const seen = new Set();
  for (const set of sets) {
    if (!Array.isArray(set)) continue;
    for (const raw of set) {
      const att = normalizeAttachment(raw, raw && raw.source ? raw.source : "thread");
      if (!att || seen.has(att.url)) continue;
      seen.add(att.url);
      out.push(att);
      if (out.length >= 12) return out;
    }
  }
  return out;
}

function compactAttachmentList(atts) {
  return (Array.isArray(atts) ? atts : []).slice(0, 12).map(a => ({
    url: a.url,
    type: a.type || "file",
    source: a.source || "thread",
    filename: a.filename || null
  }));
}

async function loadRecentThreadMessages(threadId, limit = 12) {
  try {
    const snap = await db.collection(THREADS_COLL).doc(threadId)
      .collection("messages")
      .orderBy("timestamp", "desc")
      .limit(Math.max(1, Math.min(limit, 30)))
      .get();
    const rows = [];
    for (const d of snap.docs) {
      const m = d.data() || {};
      const text = String(m.text || "").trim();
      const imageUrls = Array.isArray(m.imageUrls) ? m.imageUrls : [];
      const attachmentUrls = Array.isArray(m.attachmentUrls) ? m.attachmentUrls : [];
      if (!text && imageUrls.length === 0 && attachmentUrls.length === 0) continue;
      rows.push({
        id: d.id,
        direction: m.direction || null,
        text: text.slice(0, 900),
        hasAttachments: imageUrls.length + attachmentUrls.length > 0,
        attachmentCount: imageUrls.length + attachmentUrls.length,
        timestamp: m.timestamp && typeof m.timestamp.toMillis === "function" ? m.timestamp.toMillis() : null
      });
    }
    return rows.reverse();
  } catch (e) {
    console.warn("salesAgent: recent thread message load failed:", e.message);
    return [];
  }
}

function recentOutboundTextsFromMessages(messages) {
  return (Array.isArray(messages) ? messages : [])
    .filter(m => m && m.direction === "outbound" && m.text)
    .slice(-4)
    .map(m => String(m.text).trim())
    .filter(Boolean);
}

function normalizeForRepeat(s) {
  return String(s || "")
    .toLowerCase()
    .replace(/https?:\/\/\S+/g, "")
    .replace(/[^a-z0-9$]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function splitSentencesLight(s) {
  return String(s || "")
    .split(/(?<=[.!?])\s+/)
    .map(x => x.trim())
    .filter(Boolean);
}

function applySalesReplyGuard(reply, priorOutboundTexts = []) {
  const raw = String(reply || "").trim();
  if (!raw) return raw;
  const priorSentenceSet = new Set();
  for (const prior of priorOutboundTexts) {
    for (const sent of splitSentencesLight(prior)) {
      const n = normalizeForRepeat(sent);
      if (n.length >= 32) priorSentenceSet.add(n);
    }
  }
  if (!priorSentenceSet.size) return raw;

  const kept = [];
  const priorJoined = Array.from(priorSentenceSet).join(" | ");
  for (const sent of splitSentencesLight(raw)) {
    const n = normalizeForRepeat(sent);
    const capabilityReset = /custom .*charm.*photo.*absolutely.*(do|possible|make)/i.test(sent)
      && /custom .*charm.*photo.*absolutely.*(do|possible|make)/i.test(priorJoined);
    if (n.length >= 32 && (priorSentenceSet.has(n) || capabilityReset)) continue;
    kept.push(sent);
  }
  return kept.length ? kept.join(" ") : raw;
}

function inferFamilyFromTextAndContext(text, salesCtx = {}) {
  const spec = salesCtx.accumulatedSpec || {};
  if (spec.family === "huggie" || spec.family === "necklace" || spec.family === "stud") return spec.family;
  const t = String(text || "").toLowerCase();
  if (/huggie|huggy|hoop/.test(t)) return "huggie";
  if (/necklace|chain|pendant/.test(t)) return "necklace";
  if (/stud|studs|earring/.test(t)) return "stud";
  if (/charm/.test(t) && /chain|necklace|pendant/.test(t)) return "necklace";
  return null;
}

function compactOption(option) {
  if (!option || typeof option !== "object") return null;
  return {
    code: option.code || null,
    label: option.label || null,
    size: option.size || null,
    metal: option.metal || null,
    chainStyle: option.chainStyle || null,
    length: option.length || null,
    hoopSize: option.hoopSize || null,
    priceUsd: typeof option.priceUsd === "number" ? option.priceUsd : null,
    priceQuote: option.priceQuote === true,
    priceNotAvailable: option.priceNotAvailable === true,
    explainer: option.explainer || null
  };
}

function compactOptionSheetForAi(sheet) {
  if (!sheet || !Array.isArray(sheet.sections)) return null;
  return {
    family: sheet.family || null,
    displayName: sheet.displayName || sheet.family || null,
    unitOfMeasure: sheet.unitOfMeasure || null,
    sections: sheet.sections.map(sec => ({
      sectionId: sec.sectionId,
      name: sec.name,
      instruction: sec.instruction || null,
      required: sec.required === true,
      dependencies: sec.dependencies || null,
      options: Array.isArray(sec.options) ? sec.options.map(compactOption).filter(Boolean) : [],
      bulkSavings: Array.isArray(sec.bulkSavings) ? sec.bulkSavings : []
    }))
  };
}

async function prefetchLineSheetCollateral({ latestInboundText, salesCtx, recentThreadMessages = null }) {
  // v3.1: line-sheet URL is made available to the agent whenever the
  // family is inferable, regardless of customer phrasing. The DECISION to
  // send is the agent's, based on its read of the conversation. The
  // previous regex-gated prefetch was a rigid pattern-match that only
  // fired on specific phrases like "what options" — meaning the agent
  // had nothing to send for customers who never used those magic words.
  // The agent's prompt now teaches WHEN to send (judgment); this code
  // just guarantees the URL is always there when the agent decides yes.
  //
  // v4.3.11: also scan the recent thread message history (not just the
  // current inbound + SalesContext). On round-2+ resets, SalesContext's
  // accumulatedSpec was wiped, so family inference would fail even when
  // family is obvious from the prior conversation. Without this widening,
  // round-2 customers who say "do you have a pricing sheet?" get no
  // prefetched line sheet because inferFamilyFromTextAndContext returns
  // null on the wiped SalesContext. We try the current message first,
  // then fall back to scanning recent messages until family is found.
  let family = inferFamilyFromTextAndContext(latestInboundText, salesCtx);
  if (!family && Array.isArray(recentThreadMessages)) {
    for (let i = recentThreadMessages.length - 1; i >= 0; i--) {
      const m = recentThreadMessages[i];
      if (!m || !m.text) continue;
      family = inferFamilyFromTextAndContext(m.text, {});  // empty ctx — text-only inference
      if (family) break;
    }
  }
  if (!family || !searchCollateral) return [];
  try {
    // v4.3.12 — Don't pass `category` as an exact-match filter. Operator-
    // created collateral entries have friendly display-name categories
    // ("Custom Necklace Charm", "Custom Huggie Hoop Charm Earrings"),
    // not the agent's family enum ("necklace", "huggie", "stud"). The
    // searchCollateral implementation does .where("category","==",..)
    // which means exact match; the family value never matched a display
    // name, so prefetch returned empty for every operator-uploaded sheet.
    //
    // Instead: search by kind="line_sheet" with the family as a keyword.
    // searchCollateral's keyword scoring matches on name/description/
    // keywords fields, which essentially always contain the family word
    // (a "Necklace Charm" sheet has "necklace" in the name).
    const result = await searchCollateral({
      kind     : "line_sheet",
      keywords : [family],
      limit    : 5
    });
    const matches = Array.isArray(result && result.matches) ? result.matches : [];
    // Filter to entries that actually look like a match for this family
    // (defense in depth — keyword scoring isn't a hard filter, so an
    // unrelated line sheet with vague text could rank high). We keep
    // only matches whose name OR keywords mention the family.
    const familyRx = new RegExp("\\b" + family + "\\b", "i");
    const familyMatches = matches.filter(m => {
      if (!m) return false;
      if (familyRx.test(String(m.name || ""))) return true;
      if (Array.isArray(m.keywords) && m.keywords.some(k => familyRx.test(String(k)))) return true;
      if (familyRx.test(String(m.description || ""))) return true;
      return false;
    });
    return familyMatches.filter(m => m && isCustomerVisibleUrl(m.url)).slice(0, 3);
  } catch (e) {
    console.warn("salesAgent: line-sheet collateral prefetch failed:", e.message);
    return [];
  }
}

// ─── Tool executors ────────────────────────────────────────────────────

function buildToolExecutors({ threadId, salesCtx, customerHistory, cfg }) {
  return {
    search_shop_listings: async ({ query, limit = 8 }) => {
      if (!cfg.listingsMirrorEnabled) {
        return {
          error: "Listings mirror is disabled — operator must enable it in Settings.",
          note : "Reply without referencing specific listings."
        };
      }
      if (!searchListings) {
        return {
          error: "Listings catalog module is not available in this deployment.",
          note : "Reply without referencing specific listings."
        };
      }
      try {
        const result = await searchListings(String(query || ""), limit);
        if (result && result.error) return result;
        return {
          query,
          matches    : result.matches || [],
          count      : result.count || 0,
          totalScored: result.totalScored || 0
        };
      } catch (e) {
        return { error: `search_shop_listings failed: ${e.message}`, query };
      }
    },

    resolveQuote: async ({ family, selectedCodes, quantity,
                            wantsRush = false,
                            includeShippingSummary = false }) => {
      // v2.2 — pass through rush + shipping summary flags. wantsRush
      // triggers the family's rush production policy ($15 flat fee, qty
      // cap, hard-escalate with Quote-row codes). includeShippingSummary
      // attaches a read-only shipping-upgrade range for the AI to
      // reference verbatim (no commitment to a specific number).

      // Guard: if module failed to load at startup, return a structured
      // error so the AI escalates to human review rather than hallucinating
      // a price. This is treated the same as a resolver throw (see catch).
      if (!resolveQuote) {
        const unavailResult = {
          success: false,
          reason : "RESOLVER_UNAVAILABLE",
          customerMessage: "Our pricing system is temporarily unavailable. A team member will follow up with your quote shortly."
        };
        await writeAudit({
          threadId,
          eventType: "option_quote_failed",
          actor: "system:salesAgent",
          payload: { family, selectedCodes, quantity, wantsRush, reason: "RESOLVER_UNAVAILABLE" },
          outcome: "failure",
          ruleViolations: ["RESOLVER_UNAVAILABLE"]
        });
        return unavailResult;
      }

      try {
        const result = await resolveQuote({
          family, selectedCodes, quantity,
          wantsRush: wantsRush === true,
          includeShippingSummary: includeShippingSummary === true
        });
        salesCtx._lastResolverResult = result;

        // Direct-import calls bypass etsyMailOptionResolver's HTTP handler,
        // so the sales agent writes the canonical quote audit row here.
        await writeAudit({
          threadId,
          eventType: result && result.success ? "option_quote_resolved" : "option_quote_failed",
          actor: "system:salesAgent",
          payload: {
            family,
            selectedCodes: Array.isArray(selectedCodes) ? selectedCodes : [],
            quantity,
            wantsRush: wantsRush === true,
            includeShippingSummary: includeShippingSummary === true,
            total: result && typeof result.total === "number" ? result.total : null,
            reason: result && result.reason ? result.reason : null,
            escalations: result && Array.isArray(result.escalations) ? result.escalations : []
          },
          outcome: result && result.success ? "success" : "blocked",
          ruleViolations: result && result.success ? [] : [result && result.reason ? result.reason : "QUOTE_FAILED"]
        });

        return result;
      } catch (e) {
        await writeAudit({
          threadId,
          eventType: "option_quote_failed",
          actor: "system:salesAgent",
          payload: {
            family,
            selectedCodes: Array.isArray(selectedCodes) ? selectedCodes : [],
            quantity,
            wantsRush: wantsRush === true,
            includeShippingSummary: includeShippingSummary === true,
            reason: "RESOLVER_ERROR",
            error: e.message
          },
          outcome: "failure",
          ruleViolations: ["RESOLVER_ERROR"]
        });
        return {
          success: false,
          reason: "RESOLVER_ERROR",
          error: e.message
        };
      }
    },

    request_photo: async ({ reason }) => {
      // Tool exists primarily so the agent SIGNALS this need explicitly,
      // and the audit log shows when the agent thinks a photo is required.
      // The reply text itself does the asking; this is metadata only.
      return { ok: true, reason: String(reason || "") };
    },

    request_dimensions: async ({ what }) => {
      return { ok: true, what: String(what || "") };
    },

    // v2.3 — Etsy listing URL lookup. Customer pasted a listing URL?
    // The agent calls this to fetch authoritative data direct from Etsy's
    // API. Returns title, price, description excerpt, image URL, state
    // (active/sold-out/etc), shop ownership flag (notOurShop:true means
    // a competitor's listing). Cache-fallback path activates only if
    // Etsy's API call failed.
    lookup_listing_by_url: async ({ url }) => {
      if (!lookupListingByUrl) {
        return { found: false, reason: "LOOKUP_UNAVAILABLE", error: "Listing lookup module is not available in this deployment." };
      }
      try {
        const result = await lookupListingByUrl({ url, threadId });
        // Surface the result as a flat object the AI can read easily.
        // The full listing data is nested under .listing on success.
        return result;
      } catch (e) {
        return { found: false, reason: "LOOKUP_ERROR", error: e.message };
      }
    },

    get_option_sheet: async ({ family }) => {
      const fam = String(family || "").toLowerCase().trim();
      if (!["huggie", "necklace", "stud"].includes(fam)) {
        return { success: false, reason: "UNKNOWN_FAMILY", family };
      }
      if (!loadOptionSheet) {
        return { success: false, reason: "OPTION_SHEET_UNAVAILABLE" };
      }
      try {
        const sheet = await loadOptionSheet(fam);
        if (!sheet || sheet.active === false) {
          return { success: false, reason: "UNKNOWN_FAMILY", family: fam };
        }
        return { success: true, sheet: compactOptionSheetForAi(sheet) };
      } catch (e) {
        return { success: false, reason: "OPTION_SHEET_ERROR", error: e.message };
      }
    },

    get_collateral: async ({ category, kind, keywords }) => {
      if (!searchCollateral) {
        return {
          matches: [],
          note: "Collateral retrieval is not yet deployed."
        };
      }
      try {
        const result = await searchCollateral({
          category: category ? String(category) : undefined,
          kind    : kind     ? String(kind)     : undefined,
          keywords: Array.isArray(keywords) ? keywords : undefined,
          limit   : 5
        });
        if (result && Array.isArray(result.matches)) {
          result.matches = result.matches.filter(m => m && isCustomerVisibleUrl(m.url));
          result.count = result.matches.length;
        }
        return result;
      } catch (e) {
        return { matches: [], error: e.message };
      }
    }
  };
}

// ─── Tool specs per stage ──────────────────────────────────────────────

const TOOL_SPEC_SEARCH_LISTINGS = {
  name: "search_shop_listings",
  description: "Search the shop's active Etsy catalog. Returns title, priceUsd, primary image URL, and listing URL for matching items. Use to confirm the shop sells something the customer references, or to suggest a related/upsell item.",
  input_schema: {
    type: "object",
    properties: {
      query: { type: "string", description: "Product name, material, color, or other search term." },
      limit: { type: "integer", minimum: 1, maximum: 25 }
    },
    required: ["query"]
  }
};

const TOOL_SPEC_REQUEST_PHOTO = {
  name: "request_photo",
  description: "Signal that you need a photo from the customer (inspiration, recipient sizing, reference). Your reply text should ask for the photo naturally — this tool just records the request.",
  input_schema: {
    type: "object",
    properties: { reason: { type: "string", description: "Short reason for the audit log." } },
    required: ["reason"]
  }
};

const TOOL_SPEC_REQUEST_DIMENSIONS = {
  name: "request_dimensions",
  description: "Signal that you need specific measurements (ring size, chain length, etc.). Your reply text should ask for them naturally.",
  input_schema: {
    type: "object",
    properties: { what: { type: "string", description: "What dimension(s) you're asking for." } },
    required: ["what"]
  }
};

const TOOL_SPEC_RESOLVE_QUOTE = {
  name: "resolveQuote",
  description: "Compute the EXACT price for a Custom Brites custom order using the line-sheet option resolver. This is the ONLY way to quote a price — you MUST call this before stating any total. Returns the itemized line items, per-piece subtotal, bulk-tier discount, optional rush production fee, optional shipping summary, and final total. If a Quote-row code is in selectedCodes, the resolver returns escalations[] populated; you must then escalate to Needs Review (advance_stage:'human_review') and compose a needs_review_synopsis. Not Available codes return success:false with reason:'NOT_AVAILABLE' and a customer-facing message you should relay verbatim. RUSH PRODUCTION ($15 per order, gets to 2-3 days vs standard 4-5): pass wantsRush:true ONLY when the customer has expressed deadline pressure or asked about speeding things up. Rush + Quote-row code together returns reason:'RUSH_BLOCKED_BY_QUOTE_ROW' (operator must approve). Rush over qtyMaxForRush returns reason:'RUSH_QTY_OVER_CAP'. SHIPPING SUMMARY: pass includeShippingSummary:true only when the customer has asked about shipping speed or has expressed urgency — returns a read-only price range and fastest-days text you may quote verbatim, but you must NEVER bind to a specific shipping cost (the customer picks at Etsy checkout).",
  input_schema: {
    type: "object",
    properties: {
      family: {
        type: "string",
        enum: ["huggie", "necklace", "stud"],
        description: "The product family. Must match the family the customer is ordering."
      },
      selectedCodes: {
        type: "array",
        items: { type: "string" },
        description: "Line-sheet option codes the customer has chosen, e.g. ['1F','2B','3A']. Each code MUST come from the family's line sheet. Codes are case-insensitive."
      },
      quantity: {
        type: "integer",
        minimum: 1,
        description: "Number of pieces. For huggies, 1 piece = 1 set of 2 charms. For necklaces, 1 piece = 1 charm (with optional chain). For studs, 1 piece = 1 set (pair, single, or mismatched pair)."
      },
      wantsRush: {
        type: "boolean",
        description: "Set to true ONLY when the customer has expressed deadline pressure or asked about rush/expedited production. Adds the $15 flat per-order rush production fee, switches production timing from 4-5 days to 2-3 days. Capped at 10 pieces per order. Hard-escalates if any selectedCode is a Quote-row. Default false."
      },
      includeShippingSummary: {
        type: "boolean",
        description: "Set to true when the customer has asked about shipping speed, mentioned a tight deadline, or you're proactively offering a complete speed-up package alongside rush production. Attaches a read-only shippingSummary object with a USD price range (e.g. '$5.00-$22.00') and a fastest-days text. Use it verbatim in your reply — never bind to a specific shipping cost. Customer picks at Etsy checkout. Default false."
      }
    },
    required: ["family", "selectedCodes", "quantity"]
  }
};

const TOOL_SPEC_GET_OPTION_SHEET = {
  name: "get_option_sheet",
  description: "Fetch the current option sheet for a product family, including sections, codes, descriptions, prices, Quote-row flags, not-available flags, dependencies, and unit-of-measure. Use this whenever the customer asks what options are available, gives natural-language specs that need code mapping, or you need to verify codes before quoting.",
  input_schema: {
    type: "object",
    properties: {
      family: {
        type: "string",
        enum: ["huggie", "necklace", "stud"],
        description: "The product family whose line sheet you need."
      }
    },
    required: ["family"]
  }
};

const TOOL_SPEC_GET_COLLATERAL = {
  name: "get_collateral",
  description: "Retrieve operator-curated collateral (line sheets, product cards, lookbooks, image sets, terms/care/material guides) by category. Returns URLs you can reference in your reply. Useful categories: 'huggie', 'necklace', 'stud', 'metals_education', 'aftercare'.",
  input_schema: {
    type: "object",
    properties: {
      category: { type: "string", description: "The category to search within (e.g., 'huggie', 'metals_education')." },
      kind    : {
        type: "string",
        enum: ["line_sheet", "product_card", "lookbook", "image_set", "terms"],
        description: "Optional. Filter by collateral kind."
      },
      keywords: { type: "array", items: { type: "string" }, description: "Optional. Extra keyword filter terms." }
    },
    required: ["category"]
  }
};

// v2.3 — Etsy listing URL lookup tool. Available at every stage so the
// agent can resolve a customer-pasted URL whenever it appears (initial
// inquiry, mid-spec when the customer says "make it like this one",
// revision when they want to compare with another listing). Returns
// authoritative data from Etsy's API (with cache fallback).
const TOOL_SPEC_LOOKUP_LISTING_BY_URL = {
  name: "lookup_listing_by_url",
  description: "Fetch the full data for a specific Etsy listing when the customer has pasted or referenced a listing URL. Recognizes all Etsy URL formats: canonical (etsy.com/listing/12345), with locale prefix (etsy.com/uk/listing/12345), with slug or query params, and the seller's internal /your/listings/ URLs. Etsy short links (etsy.me/...) are detected but NOT auto-resolved (returns SHORT_LINK_UNRESOLVED — ask the customer for the full URL). Returns authoritative data direct from Etsy's API including title, price, description excerpt, primary image URL, listing state, shop ownership, and customizability flag. The result includes notOurShop:true if the listing belongs to a competitor (in which case acknowledge but pivot to your own offerings) and isActive:false if the listing is sold-out, expired, or draft (in which case mention the listing is no longer available and offer to make something similar via custom order).",
  input_schema: {
    type: "object",
    properties: {
      url: {
        type: "string",
        description: "The full URL the customer pasted, or any URL that might be an Etsy listing. Pass it raw — the parser handles slugs, query strings, locale prefixes, etc."
      }
    },
    required: ["url"]
  }
};

function buildToolSpecs() {
  // v4.0: all tools available always. The agent decides what to call
  // based on conversation state, not on a stage gate.
  return [
    TOOL_SPEC_SEARCH_LISTINGS,
    TOOL_SPEC_GET_OPTION_SHEET,
    TOOL_SPEC_LOOKUP_LISTING_BY_URL,
    TOOL_SPEC_REQUEST_PHOTO,
    TOOL_SPEC_REQUEST_DIMENSIONS,
    TOOL_SPEC_RESOLVE_QUOTE,
    TOOL_SPEC_GET_COLLATERAL
  ];
}

// ─── Defensive JSON parse (mirrors intentClassifier pattern) ───────────
//
// v2.6 — Hardened to recover from three additional failure modes Opus
// occasionally emits:
//   1. Markdown fences with leading prose ("Sure, here's the JSON: ```json{...}")
//   2. Truncated output where the closing braces/brackets got cut off
//      (model hit max_tokens mid-emit). We close any open structures.
//   3. Trailing commas before } or ] (technically invalid JSON but
//      LLMs emit them constantly).
//
// Order: try strict parse → strip fences → extract first {...} block
// → repair truncation → repair trailing commas → final attempt.

function tryParseJson(rawText) {
  if (!rawText || typeof rawText !== "string") return null;
  let text = rawText.trim();

  // Step 1: strip markdown fences (handles ```json\n... and bare ```)
  if (text.startsWith("```")) {
    text = text.replace(/^```(?:json|JSON)?\s*\n?/i, "").replace(/\s*```\s*$/, "");
  }
  try { return JSON.parse(text); } catch {}

  // Step 2: find the first { and walk the structure. We need to find
  // EITHER a balanced block that parses, OR the remaining text from { to
  // end (so we can attempt repair on truncation).
  const start = text.indexOf("{");
  if (start === -1) return null;

  // Scan the structure once, tracking open depth + bracket depth.
  // If we find a balanced terminus, try parsing that slice. Otherwise
  // we'll repair the open structure below.
  let depth = 0;     // braces
  let arrDepth = 0;  // brackets
  let inStr = false;
  let esc   = false;
  let balancedEnd = -1;

  for (let i = start; i < text.length; i++) {
    const c = text[i];
    if (esc) { esc = false; continue; }
    if (c === "\\") { esc = true; continue; }
    if (c === '"') { inStr = !inStr; continue; }
    if (inStr) continue;
    if      (c === "{") depth++;
    else if (c === "}") { depth--; if (depth === 0 && arrDepth === 0) { balancedEnd = i; break; } }
    else if (c === "[") arrDepth++;
    else if (c === "]") arrDepth--;
  }

  // Step 3: if we found a clean balanced block, try parsing.
  if (balancedEnd !== -1) {
    const slice = text.slice(start, balancedEnd + 1);
    try { return JSON.parse(slice); } catch {}
    // Balanced but didn't parse — likely a trailing comma. Try repair.
    try { return JSON.parse(repairTrailingCommas(slice)); } catch {}
  }

  // Step 4: truncated. Take everything from the first { to end of text,
  // close any open string, then close all open arrays + braces in order.
  let repair = text.slice(start);
  if (inStr) repair += '"';                                  // close runaway string
  while (arrDepth-- > 0) repair += "]";
  while (depth--    > 0) repair += "}";
  try { return JSON.parse(repair); } catch {}
  try { return JSON.parse(repairTrailingCommas(repair)); } catch {}

  return null;
}

/** Strip trailing commas before `}` or `]` — common LLM output bug.
 *  Has to skip commas that are inside strings, since a string value of
 *  "foo," followed by `}` is legal. */
function repairTrailingCommas(s) {
  let out = "";
  let inStr = false, esc = false;
  for (let i = 0; i < s.length; i++) {
    const c = s[i];
    if (esc) { out += c; esc = false; continue; }
    if (c === "\\") { out += c; esc = true; continue; }
    if (c === '"') { inStr = !inStr; out += c; continue; }
    if (inStr) { out += c; continue; }
    if (c === ",") {
      // Look ahead past whitespace to see if next non-ws char is } or ]
      let j = i + 1;
      while (j < s.length && /\s/.test(s[j])) j++;
      if (j < s.length && (s[j] === "}" || s[j] === "]")) continue;  // drop comma
    }
    out += c;
  }
  return out;
}

// ─── Validate AI's chosen quote (post-loop server-side gate) ──────────

async function validateQuotedPriceIfPresent({ threadId, parsed, salesCtx }) {
  // v2.1 — Option-sheet validation. The AI's quoted_total_usd MUST
  // exactly match (within 1 cent for rounding) the resolver's total
  // for the same family + selectedCodes + quantity. If the AI claims
  // a price that doesn't match the resolver, that's a hallucination
  // and we hard-escalate.

  const quotedTotalUsd = (typeof parsed.quoted_total_usd === "number")
    ? parsed.quoted_total_usd
    : (parsed.draft_custom_order_listing && typeof parsed.draft_custom_order_listing.totalUsd === "number")
      ? parsed.draft_custom_order_listing.totalUsd
      : null;

  if (typeof quotedTotalUsd !== "number" || quotedTotalUsd <= 0) {
    // No price stated this turn (e.g. discovery, spec, or close stage
    // when no quote needs to be re-stated). Skip validation.
    return { skip: true };
  }

  // Pull family + selectedCodes + quantity from the cached resolver
  // result first (most reliable — it's exactly what the AI just used)
  // or from the parsed output's items_quoted.
  // v2.2 — also pull the wantsRush flag the AI used. If the AI quoted a
  // rush total ($X + $15), we must re-validate with wantsRush:true or
  // we'll see a $15 drift and falsely block the quote.
  let family, selectedCodes, quantity;
  let wantsRush = false;
  const lrr = salesCtx._lastResolverResult;
  if (lrr && lrr.success) {
    family        = lrr.family;
    quantity      = lrr.quantity;
    selectedCodes = (lrr.lineItems || []).map(li => li.code).filter(Boolean);
    // Rush was applied if and only if the cached result has a rush object.
    wantsRush     = lrr.rush !== null && lrr.rush !== undefined;
  }
  if ((!family || !Array.isArray(selectedCodes) || selectedCodes.length === 0) && parsed.items_quoted) {
    family        = parsed.items_quoted.family || family;
    selectedCodes = parsed.items_quoted.selectedCodes || selectedCodes;
    quantity      = parsed.items_quoted.quantity      || quantity;
    // Fall back to a rush flag on items_quoted if the AI included one
    if (typeof parsed.items_quoted.wantsRush === "boolean") wantsRush = parsed.items_quoted.wantsRush;
  }

  if (!family || !Array.isArray(selectedCodes) || selectedCodes.length === 0 || !quantity) {
    return {
      valid: false,
      reason: "VALIDATE_ITEMS_UNKNOWN",
      detail: "Could not determine family/selectedCodes/quantity to re-validate against the resolver."
    };
  }

  // Re-run the resolver server-side with the SAME rush flag the AI used.
  // If anything is off (price mismatch, unknown code, not-available code,
  // rush blocked), the validator returns invalid.
  if (!resolveQuote) {
    // Module failed to load at startup — cannot re-validate. Treat as
    // invalid so the pipeline escalates to human review rather than
    // auto-sending an unvalidated price.
    return {
      valid: false,
      reason: "RESOLVER_UNAVAILABLE",
      detail: "Option resolver module is not loaded — quote cannot be server-side validated."
    };
  }
  const fresh = await resolveQuote({ family, selectedCodes, quantity, wantsRush });

  if (!fresh.success) {
    return {
      valid: false,
      reason: "RESOLVER_REJECTED_AT_VALIDATION",
      resolverReason: fresh.reason,
      resolverDetail: fresh
    };
  }

  // Hard escalation: any escalation signal at validation time means
  // the AI tried to commit to a final price while a Quote-row code is
  // pending. The AI should have routed to human_review at quote stage;
  // catching it here is the last line of defense.
  if (Array.isArray(fresh.escalations) && fresh.escalations.length > 0) {
    return {
      valid: false,
      reason: "QUOTE_ROW_NOT_RESOLVED",
      escalations: fresh.escalations,
      resolverTotal: fresh.total
    };
  }

  // Penny-tolerance check on the price itself. The resolver is
  // deterministic; any drift between the AI's stated total and the
  // resolver's is either a hallucination or a stale cache.
  const drift = Math.abs(quotedTotalUsd - fresh.total);
  if (drift > 0.01) {
    return {
      valid: false,
      reason: "QUOTED_PRICE_MISMATCH",
      quotedPrice: quotedTotalUsd,
      resolverTotal: fresh.total,
      drift
    };
  }

  return {
    valid: true,
    family,
    selectedCodes,
    quantity,
    resolverTotal: fresh.total
  };
}

// ─── Build initial messages for the agent ──────────────────────────────

function buildInitialMessages({ contextSummary, latestInboundText, referenceAttachments }) {
  const safeRefAttachments = compactAttachmentList(referenceAttachments);
  const userContent = [
    {
      type: "text",
      text: [
        "═══ Sales context ═══",
        JSON.stringify(contextSummary, null, 2),
        "",
        "═══ Latest customer message ═══",
        String(latestInboundText || "(no text)").slice(0, 6000)
      ].join("\n")
    }
  ];

  if (safeRefAttachments.length) {
    userContent.push({
      type: "text",
      text: "Customer-provided reference attachments retained from this thread (shown as image blocks where possible):\n" +
            safeRefAttachments.map((a, i) => String(i + 1) + ". " + (a.filename || a.type || "attachment") + ": " + a.url).join("\n")
    });
  }

  let imgCount = 0;
  for (const att of safeRefAttachments) {
    if (imgCount >= 4) break;
    if (att && isCustomerVisibleUrl(att.url)) {
      userContent.push({
        type  : "image",
        source: { type: "url", url: att.url }
      });
      imgCount++;
    }
  }

  return [{ role: "user", content: userContent }];
}
// ─── Main handler ──────────────────────────────────────────────────────

exports.handler = async (event) => {
  if (event.httpMethod === "OPTIONS") {
    return { statusCode: 200, headers: CORS, body: "ok" };
  }
  if (event.httpMethod !== "POST") {
    return { statusCode: 405, headers: CORS, body: JSON.stringify({ error: "Method Not Allowed" }) };
  }

  const auth = requireExtensionAuth(event);
  if (!auth.ok) return auth.response;

  let body = {};
  try { body = JSON.parse(event.body || "{}"); }
  catch { return { statusCode: 400, headers: CORS, body: JSON.stringify({ error: "Invalid JSON body" }) }; }

  const {
    threadId,
    latestInboundText,
    latestInboundAttachments = [],
    threadReferenceAttachments = [],
    referencedListings       = [],     // v2.3 — pre-fetched from auto-pipeline
    customerHistory          = {},
    intentClassification     = null,
    intentConfidence         = null,
    employeeName             = "system:auto-pipeline"
  } = body;

  if (!threadId) {
    return { statusCode: 400, headers: CORS, body: JSON.stringify({ error: "threadId required" }) };
  }

  const tStart = Date.now();
  const cfg = await getConfig();

  // ── Master enable + pilot allow-list ──
  if (!cfg.salesModeEnabled) {
    return { statusCode: 403, headers: CORS,
             body: JSON.stringify({ error: "Sales mode is disabled in config" }) };
  }
  if (cfg.salesPilotThreadIds.length > 0 && !cfg.salesPilotThreadIds.includes(threadId)) {
    return { statusCode: 403, headers: CORS,
             body: JSON.stringify({ error: "Thread not in pilot allow-list",
                                     pilotListLength: cfg.salesPilotThreadIds.length }) };
  }

  try {
    // v4.3 — TERMINAL THREAD GUARD. Defense in depth against routing bugs
    // and manual re-invocations. The autoPipeline's path (a) STATEFUL keys
    // off ACTIVE_SALES_STAGES from SalesContext, but if SalesContext was
    // never reset on completion (older threads, or a worker that crashed
    // before resetting it), a follow-up message could still land here. Any
    // thread already in a terminal sales status is, by definition, done —
    // we don't want to generate fresh draft replies that pollute a
    // completed conversation. Let the standard customer-service draft
    // pipeline (etsyMailDraftReply) handle the follow-up instead.
    //
    // We also check salesCompletedAt: thread.status can be overwritten by
    // autoPipeline's finalizeThread on subsequent generic replies (it
    // writes "auto_replied" / "pending_human_review" / "queued_for_auto_send"),
    // so a thread whose sale completed weeks ago might no longer have
    // status="sales_completed". salesCompletedAt is written ONCE by the
    // worker and never touched by other code paths — a reliable
    // "ever completed?" signal.
    // v4.3.15 — Hoist thread data so it's available for contextSummary
    // below. Without this hoist, the agent's contextSummary couldn't
    // surface listing-pipeline fields (customListingStatus,
    // customerAccepted, customListingId etc.) and the addendum's
    // edge-case guidance about "where is my listing?" would reference
    // fields the agent can't see.
    let threadDocData = null;
    try {
      const tSnap = await db.collection(THREADS_COLL).doc(threadId).get();
      if (tSnap.exists) {
        threadDocData = tSnap.data() || {};
        const ts = threadDocData.status;
        const everCompleted = !!threadDocData.salesCompletedAt;
        if (TERMINAL_THREAD_STATUSES.has(ts) || everCompleted) {
          console.log(`[salesAgent] thread ${threadId} is terminal (status=${ts}, everCompleted=${everCompleted}), skipping`);
          return { statusCode: 200, headers: CORS,
                   body: JSON.stringify({
                     ok: true, skipped: true,
                     reason: TERMINAL_THREAD_STATUSES.has(ts) ? "terminal_thread_status" : "sale_already_completed",
                     status: ts,
                     everCompleted
                   }) };
        }
      }
    } catch (e) {
      // Read failure is non-fatal — fall through to normal processing.
      // Better to occasionally over-process than under-process.
      console.warn(`[salesAgent] terminal-status check failed for ${threadId}:`, e.message);
    }

    // ── Load or init the sales context ──
    const salesCtx = await loadOrInitSalesContext(threadId);
    // v4.0: stage is no longer read or referenced. Kept only in
    // SalesContext init for backward compatibility with older sales
    // contexts that still have it.

    // Carry customer reference photos/files across the whole sales thread.
    // This prevents the agent from asking for a photo that was already sent
    // earlier in the same Etsy conversation.
    const referenceAttachments = mergeAttachments(
      Array.isArray(latestInboundAttachments)
        ? latestInboundAttachments.map(a => ({ ...a, source: "latest_inbound" })) : [],
      Array.isArray(threadReferenceAttachments)
        ? threadReferenceAttachments.map(a => ({ ...a, source: "thread_history" })) : [],
      Array.isArray(salesCtx.referenceAttachments)
        ? salesCtx.referenceAttachments.map(a => ({ ...a, source: a.source || "sales_context" })) : []
    );

    const recentThreadMessages = await loadRecentThreadMessages(threadId, 12);
    const priorOutboundTexts = recentOutboundTextsFromMessages(recentThreadMessages);
    const recommendedCollateral = await prefetchLineSheetCollateral({
      latestInboundText, salesCtx, recentThreadMessages
    });

    // ── Load the unified sales prompt ──
    const promptLoad = await loadSalesPrompt();
    if (!promptLoad.ok) {
      await writeAudit({
        threadId, eventType: "sales_agent_prompt_unavailable",
        payload: { error: promptLoad.error },
        outcome: "failure"
      });
      return { statusCode: 503, headers: CORS,
               body: JSON.stringify({ error: promptLoad.error,
                                       errorCode: "SALES_PROMPT_NOT_AVAILABLE" }) };
    }

    // ── Build tools + executors ──
    const toolSpecs     = buildToolSpecs();
    const toolExecutors = buildToolExecutors({ threadId, salesCtx, customerHistory, cfg });

    // ── Build initial messages ──
    // v2.3 — Compact `referencedListings` for the agent's context. The
    // auto-pipeline pre-fetched these via etsyMailListingLookup before
    // the agent ran. We pass through only the fields the AI cares
    // about so the context window doesn't bloat.
    const compactReferencedListings = (Array.isArray(referencedListings) ? referencedListings : [])
      .map(r => {
        if (!r || !r.found) {
          return {
            url: r && r.url ? r.url : null,
            found: false,
            reason: (r && r.reason) || "UNKNOWN"
          };
        }
        const li = r.listing || {};
        return {
          url           : r.url,
          found         : true,
          listingId     : r.listingId,
          source        : r.source,
          notOurShop    : !!r.notOurShop,
          isActive      : !!r.isActive,
          title         : li.title || null,
          priceUsd      : li.priceUsd ?? null,
          state         : li.state || null,
          quantity      : li.quantity ?? null,
          listingUrl    : li.listingUrl || null,
          primaryImageUrl : li.primaryImageUrl || null,
          descriptionShort: li.descriptionShort
                            ? li.descriptionShort.slice(0, 400) : null,
          tags          : Array.isArray(li.tags) ? li.tags.slice(0, 8) : [],
          materials     : Array.isArray(li.materials) ? li.materials.slice(0, 6) : [],
          isCustomizable: li.isCustomizable
        };
      });

    const contextSummary = {
      // v4.0: no stage. The agent reads accumulatedSpec, quoteHistory,
      // lastResolverResult, recentThreadMessages and decides what's next.
      accumulatedSpec    : salesCtx.accumulatedSpec || {},
      quoteHistory       : (salesCtx.quoteHistory || []).slice(-3),
      lastResolverResult : (salesCtx._lastResolverResult && salesCtx._lastResolverResult.success)
                            ? {
                                family   : salesCtx._lastResolverResult.family,
                                total    : salesCtx._lastResolverResult.total,
                                quantity : salesCtx._lastResolverResult.quantity,
                                bulkTier : salesCtx._lastResolverResult.bulkTier,
                                escalations: salesCtx._lastResolverResult.escalations || []
                              }
                            : null,
      // v2.3 — Etsy listing URLs the customer pasted, pre-resolved.
      // Empty array if the customer didn't paste any.
      referencedListings : compactReferencedListings,
      recentThreadMessages,
      previousOutboundReplies: priorOutboundTexts.slice(-3),
      referenceAttachments: compactAttachmentList(referenceAttachments),
      hasReferenceImage: referenceAttachments.length > 0,
      recommendedCollateral,
      customerHistory    : {
        isRepeat          : !!(customerHistory && customerHistory.isRepeat),
        orderCount        : (customerHistory && customerHistory.orderCount) || 0,
        lifetimeValueUsd  : (customerHistory && customerHistory.lifetimeValueUsd) || 0
      },
      intentClassification, intentConfidence,
      // v4.3.15 — Listing-pipeline state visible to the agent. Without
      // these, the addendum's edge-case guidance ("check
      // thread.customListingStatus") referenced fields the agent
      // couldn't see — the agent had no way to know whether a listing
      // had been created, was in progress, or had never been attempted.
      // When a customer asks "where is my listing?" the agent needs
      // these values to decide whether to (a) fire the pipeline now
      // because nothing is in flight, (b) reassure the customer
      // because creation is already running, or (c) escalate because
      // something is genuinely broken.
      thread: (() => {
        const t = threadDocData || {};
        return {
          customerAccepted        : t.customerAccepted === true,
          customerAcceptedAt      : (t.customerAcceptedAt && t.customerAcceptedAt.toMillis)
                                      ? t.customerAcceptedAt.toMillis() : null,
          customListingStatus     : t.customListingStatus || null,
          customListingId         : t.customListingId || null,
          customListingUrl        : t.customListingUrl || null,
          customListingSentAt     : (t.customListingSentAt && t.customListingSentAt.toMillis)
                                      ? t.customListingSentAt.toMillis() : null,
          salesCompletedAt        : (t.salesCompletedAt && t.salesCompletedAt.toMillis)
                                      ? t.salesCompletedAt.toMillis() : null,
          salesRound              : Number(t.salesRound || 1)
        };
      })()
    };
    const initialMessages = buildInitialMessages({
      contextSummary, latestInboundText, referenceAttachments
    });

    // ── Run the tool loop ──
    //
    // v4.3.12 — Behavioral addendum to the operator's prompt, narrowly
    // focused on line-sheet sends. The operator's prompt at
    // EtsyMail_SalesPrompts/sales teaches the agent how to run the
    // funnel; this addendum codifies a SPECIFIC delivery policy that
    // the operator asked for and that prompt-only solutions kept
    // failing on.
    //
    // The bug we're patching:
    //   When the customer asked "do you have a pricing sheet?", the
    //   agent's reply was a 200-word recitation of every option and
    //   price ("Charm size + metal: 9-10mm, 11-12mm, 14mm, or 1 inch,
    //   each in Sterling Silver, 14k Gold Filled..."). That is the
    //   exact OPPOSITE of what was asked for. The customer wanted
    //   the line-sheet IMAGE, not a wall of text.
    //
    // The fix has TWO parts that must work together:
    //   1. Prompt addendum: tell the agent NOT to recite options,
    //      and to instead emit `attach_line_sheet: true` in its JSON
    //      output, with a SHORT, OPTION-FREE one-line reply.
    //   2. Code-level attachment construction (later in this file):
    //      when parsed.attach_line_sheet is true, build an `image`
    //      attachment record from recommendedCollateral[0] and write
    //      it to the draft's attachments[]. The Chrome extension's
    //      send loop will then upload the line-sheet image alongside
    //      the message text.
    //
    // We APPEND (don't replace) the operator's prompt so any further
    // operator edits in Settings → Agent Prompts continue to work.
    const lineSheetEagernessAddendum = `

# LINE-SHEET POLICY (system addendum — overrides any conflicting prompt guidance)

## When to send the line sheet

When the customer signals interest in pricing or options — even WEAKLY — your default action is to send the line sheet for that family. Examples of triggering signals:
- "do you have a pricing sheet"
- "what are my options"
- "what sizes / metals / chains do you offer"
- "what's available"
- "can I see options"
- "how much for [a different config]"
- ANY question that asks about choices, prices for variants, or what's possible

The ONE thing you may need before sending the line sheet is which family (necklace, stud earring, or huggie hoop earring). If conversation context already tells you this — a prior round, an earlier message in this round, an explicit listing reference — USE that, don't re-ask.

## How to send the line sheet — CRITICAL

When you decide to send the line sheet, you MUST do BOTH of the following in your JSON output:

1. **Set the structured signal**: include \`"attach_line_sheet": true\` as a top-level field in your JSON. The system reads this and attaches the actual line-sheet IMAGE FILE (from operator-uploaded collateral) to your draft message. The customer sees the image inline in their Etsy conversation. This is the entire mechanism — the image attaches automatically; you do not need to embed any URL.

2. **Keep reply text SHORT and OPTION-FREE**. Your \`reply\` field must be a single short sentence inviting the customer to look at the attached sheet and tell you what they want. NO PRICES. NO LISTS. NO OPTIONS RECITED IN TEXT. The image carries that information. Examples of CORRECT reply text:
   - "Here's our necklace charm line sheet — take a look and let me know which size, metal, chain, and length you'd like."
   - "Attached is our huggie hoop line sheet. Pick the configuration you want and I'll quote it."
   - "Sending over the stud earring line sheet — let me know which option works for you."

Examples of INCORRECT reply text (DO NOT do this — these are exactly the failure mode being patched):
   - ❌ "Here's the rundown for necklace charms. Charm size + metal: 9-10mm, 11-12mm, 14mm..." (reciting the sheet in text)
   - ❌ Any reply that includes prices like "$24" or "$45" before the customer has chosen options
   - ❌ Any reply that lists chain types, metals, lengths, or bulk discounts in prose
   - ❌ Embedding the line sheet URL in the text — the IMAGE attaches automatically, no URL needed

The customer will SEE the line sheet image rendered in their Etsy conversation. Trust the attachment to convey the information; your job in the reply text is just to invite them to look at it and respond.

## When NOT to send the line sheet

- The customer has already given you all the spec codes and is ready to lock in (no choosing happening). Then proceed to quote with resolveQuote.
- The customer has explicitly declined to see options ("just quote me X").
- A line-sheet collateral image is not available for the family (you can tell by checking context.summary.recommendedCollateral — if empty, fall back to asking which family or to a brief verbal exchange).

## Edge case: collateral genuinely unavailable

If the customer wants pricing/options but no line-sheet collateral exists for their family in context.summary.recommendedCollateral, do NOT default to reciting prices. Instead, set ready_for_human_approval: true and produce a needs_review_synopsis explaining that the operator should send the line sheet manually. This keeps the experience consistent — customer sees an image OR a human-attended message, never a wall of pricing text.
`.trim();

    // v4.3.15 — A second addendum: structural acceptance signal must
    // match reply text. The single most damaging failure mode in the
    // agent is when its REPLY TEXT verbally accepts a quote ("Got it,
    // we'll send the custom listing your way") but the JSON output's
    // `customer_accepted` field stays false. The downstream listing-
    // creator pipeline only fires when customer_accepted=true, so the
    // customer reads a promise that never gets kept and asks "where
    // is my listing?" minutes later.
    //
    // Joanna's banana-charm round 2 hit this: across THREE consecutive
    // AI turns the model said "we'll send the custom listing your way"
    // with customer_accepted=false. The thread doc has no
    // customListingStatus, no customerAccepted, no customListingId —
    // direct-fire never claimed because the structural flag was never
    // flipped. Customer is stuck waiting indefinitely.
    //
    // The fix needs to be a hard, structural-level rule (not just
    // "consider setting it"), because the model is reliably emitting
    // friendly acceptance language without the structured signal. We
    // bind the two together: if your reply commits to creating /
    // sending a custom listing, customer_accepted MUST be true.
    const acceptanceSyncAddendum = `

# CUSTOMER_ACCEPTED STRUCTURAL RULE (system addendum)

When you write reply text that commits to sending a custom listing — phrases like:
- "we'll send the custom listing your way"
- "sending you the listing now"
- "I'll send the listing through"
- "got it, we'll get the listing over to you"
- "creating the listing for you"
- ANY language that promises a listing is being created or sent

— you MUST set \`"customer_accepted": true\` in your JSON output. These are not separate concerns. The structured \`customer_accepted\` field is what TRIGGERS the listing creation pipeline. If you say "we'll send the listing" without setting \`customer_accepted: true\`, the listing pipeline never runs and the customer waits indefinitely. They will then ask "where is my listing?" — at which point the system will be in a stuck state requiring operator intervention.

When to set \`customer_accepted: true\`:
- Customer has clearly accepted a specific quote (says "yes", "sounds good", "let's do it", "ok let's go", "great", "no rush option", "send the listing", "send the link", etc.) AFTER you've quoted them with all spec codes resolved.
- A complete spec exists in lastResolverResult (family + line items + total).
- You're about to commit to sending the listing in your reply text.

When NOT to set \`customer_accepted: true\`:
- Customer is still asking questions or comparing options.
- Customer accepted but specs are incomplete (no resolved quote, codes unclear).
- Customer asked about something else (turnaround, shipping, etc.) without confirming the order.
- You're declining or asking for clarification.

Edge case — customer asks "where is my listing?" or similar follow-up after a previous unkept promise:
- This means a previous turn promised to send the listing but customer_accepted was not set, OR the listing pipeline failed.
- Check thread.customListingStatus in your context. If missing or null AND the spec is locked AND the price is accepted, set \`customer_accepted: true\` NOW so the pipeline fires this turn. Reply text should reassure the customer the listing is coming through and avoid additional language about checking with the team.
- If thread.customListingStatus exists and shows "creating" or similar in-flight state, don't set customer_accepted again — just write a brief reassurance ("the listing is generating now, you'll have it in a minute"). The pipeline is already running.
- Only escalate to ready_for_human_approval if you genuinely cannot determine what to do — for example if there's a real payment / refund / cancellation question entangled with the listing-status question.

This rule is non-negotiable. Reply text and customer_accepted must agree.
`.trim();

    // v0.9.18 — Three additional addendums responding to specific
    // failure modes the operator flagged in production drafts. Each
    // is a structural-level rule, not a soft suggestion, because
    // prompt-only soft language has reliably failed to prevent the
    // problem (see the existing acceptanceSyncAddendum precedent).

    // ── (1) attach_line_sheet ↔ reply text sync ─────────────────────
    // Mirror the customer_accepted sync rule for attach_line_sheet.
    // The damaging failure mode: agent writes reply text saying
    // "see the attached line sheet" / "take a look at the attached
    // necklace line sheet" but emits attach_line_sheet:false. The
    // line-sheet image is never attached, the customer sees a
    // promise with nothing fulfilling it. The operator UI now blocks
    // such drafts client-side, but the prompt still needs to bind
    // the two emissions together so the agent doesn't strand drafts
    // in a not-sendable state.
    const attachLineSheetSyncAddendum = `

# ATTACH_LINE_SHEET STRUCTURAL RULE (system addendum)

If your reply text mentions "the attached line sheet", "see the attached", "I've attached", "take a look at the attached", "here's our line sheet", "attached menu", "attached options sheet", or ANY phrasing that tells the customer a line sheet / menu / options sheet is attached to this message, you MUST set \`"attach_line_sheet": true\` in your JSON output.

These are not separate concerns. The structured \`attach_line_sheet\` field is what makes the line-sheet IMAGE actually attach to the draft. If you write "see the attached line sheet" in your reply but emit \`attach_line_sheet: false\`, the customer receives a message with a broken promise and the operator UI will refuse to send the draft — both auto-pipeline and manual Send via Etsy are gated on this.

The reverse holds: if you emit \`attach_line_sheet: true\`, your reply text must invite the customer to look at the attachment. Don't set the flag and then write text that doesn't reference the sheet.

When you genuinely have nothing to attach (e.g. no line-sheet collateral exists for the family), do NOT use phrasing that promises an attachment. Either ask one clarifying question to nail down the family first, or set ready_for_human_approval:true with a synopsis explaining the operator should send the line sheet manually.
`.trim();

    // ── (2) Don't restate what staff already said in the thread ─────
    // The damaging failure mode: a CustomBrites operator just sent a
    // helpful, complete reply (e.g. "There is a drop-down arrow to
    // add notes on the listing — no worries if you missed it"), the
    // customer responds, and the agent's draft restates almost
    // verbatim what the operator already wrote. Two agents talking
    // past each other; customer reads it as a bug.
    const noRestateStaffAddendum = `

# NO RESTATING STAFF MESSAGES (system addendum)

Read the conversation thread before composing. The customer can see every prior message in this conversation, including ones from staff (CustomBrites). If a staff member already answered the customer's question, gave them an option, or made a commitment in this thread, do NOT repeat that answer back at them in different words. Doing so reads as if you didn't read the thread, and it confuses the customer about who they're actually talking to.

Specifically:
- If the customer's most recent message is a follow-up to a staff message ("I see it now", "got it", "thanks", "ok"), respond to the follow-up. Don't re-explain what staff already explained.
- If the customer is asking a NEW question after a staff answer, address the new question only. Don't preface with a recap of the staff answer.
- If the customer is asking for confirmation of something staff just confirmed ("so it'll be 15 inches?"), confirm crisply ("Yes, 15 inches confirmed.") and move forward — don't paraphrase the staff message at length.

The signal that you're about to restate staff: your draft contains a sentence that closely paraphrases something a staff message in the thread already said. When you catch yourself doing this, delete it and write only what advances the conversation from where it actually is.
`.trim();

    // ── (3) Move-forward / close-the-deal bias ─────────────────────
    // The damaging failure mode: customer is clearly ready to buy
    // (clear specs, urgency signals, "let's do it" energy), and the
    // agent asks ANOTHER clarifying question or restates the spec
    // back at them when it could have computed a quote and offered
    // to send the custom listing. The customer's enthusiasm rots
    // while the conversation stalls.
    const moveForwardAddendum = `

# MOVE-FORWARD BIAS (system addendum)

When a customer signals readiness to buy — explicit confirmations, urgency about timing, "let's do it" energy, or simply having given you every input you need — your job is to advance the conversation toward a quote and a custom listing, not to ask another reassurance question.

Operating principles:
- If you have the family + every required code + a quantity, call \`resolveQuote\` and quote. Don't ask "are you sure?" or restate the spec back. The customer told you what they want; act on it.
- If the customer is asking for confirmation of something already in scope ("so you'll do 15 inches?"), confirm and move forward. Don't open new questions you didn't need answered.
- If you are missing exactly ONE input that would unlock a quote, ask only for that one input. Don't bundle it with restating what you already know.
- If the customer has accepted a previously-quoted price (see CUSTOMER ACCEPTANCE rules), set customer_accepted:true and confirm warmly in one short sentence. The downstream automation creates the listing; you don't promise a timeline.

When NOT to move forward:
- A required input is genuinely missing AND the customer hasn't given you something to work with (no implied default, no prior turn).
- The resolver returned escalations[] and you must escalate.
- The customer has explicitly slowed the conversation ("let me think about it", "I need to check with my partner").

In every other case, the bias is forward. Don't manufacture clarifying questions to feel safe; that's the failure mode this rule exists to prevent.
`.trim();

    // ── (4) Rush eagerness — JUDGMENT, not a sticky rule ──────────
    // Per operator policy correction (v0.9.21): rush production is
    // ONLY available via a custom Etsy listing. There is no "tick a
    // box at checkout" path on standard listings. Earlier wording
    // that suggested "add it before checkout" was incorrect and is
    // replaced here.
    //
    // The flow when a customer wants rush:
    //   1. Identify the base listing they want (or design they want).
    //      Prefer to find it from the thread (a pasted listing URL,
    //      a referenced item, an established spec from earlier turns).
    //      If you can't find it, ask one short clarifying question to
    //      get the listing URL or the design intent.
    //   2. Compute a quote that includes the $15 rush fee plus any
    //      other custom requests (resolveQuote with wantsRush:true,
    //      or for a non-standard build, escalate).
    //   3. State the quote and confirm.
    //   4. On acceptance (customer_accepted:true), the downstream
    //      pipeline generates a custom Etsy listing with the rush
    //      already priced in. The customer checks out THAT listing,
    //      not the original one.
    //
    // Surfacing rush is still a judgment call about live urgency.
    const rushEagernessAddendum = `

# RUSH OFFER — READ THE LIVE CONVERSATION, NOT THE WHOLE THREAD

The base playbook tells you to surface rush production ($15, 2-3 days vs 4-5) when the customer has expressed urgency or named a deadline, AND to suppress it during early discovery (before specs are chosen). When those rules collide on a single turn, use judgment, don't mechanically apply one or the other.

## What "live urgency" looks like

Read the active exchange — the customer's most recent few messages and the immediate context they're operating in. Look for signals that the customer is presently eager, worried about timing, or working toward a real deadline that's still in scope:

- The customer just named a date or event that's still ahead and tied to the current question ("for my mom's birthday May 14", "wedding next month", "graduation").
- The customer expressed worry about timing in this exchange ("hope it gets here in time", "cutting it close", "will it arrive before...").
- The customer used urgency words in the active conversation ("rush", "asap", "soon", "in a hurry").
- The customer or staff just discussed delivery timing in the immediately preceding turns and the customer is still on that topic — i.e. the deadline is genuinely live, not a fragment of a past conversation.

If any of these are present and the customer seems eager OR worried about getting the piece in time, offering rush is helpful regardless of whether they've chosen specs yet. Don't suppress the FYI just because the conversation is technically in discovery mode.

## What does NOT count as live urgency

- A deadline mentioned in an EARLIER, self-contained conversation in the same thread (e.g. a Mother's-Day question from three weeks ago, now closed) when the current conversation is about something different. Threads can carry many separate conversations over time. A past deadline doesn't bind a present conversation.
- A staff message from the past that referenced timing for a different question.
- Generic shop content about shipping or production windows in passing — that's reference, not a live deadline.
- The customer explicitly saying "no rush" or "whenever" in this exchange. Respect it.

## HOW rush actually works — IMPORTANT, this corrects earlier wording

Rush is NOT a "tick a box at checkout" option on standard Etsy listings. Customers cannot add rush themselves to an existing listing. The ONLY mechanism for rush is a CUSTOM Etsy listing that we generate, with the $15 rush fee priced in. So when you mention rush, your reply must NOT say "add it at checkout" or "you can select rush before placing the order" or anything that implies the customer can opt in via the standard purchase path. Those phrasings are incorrect and confusing.

When the customer wants rush, the path is:
1. Find the base listing or design they're ordering. If the customer pasted a listing URL earlier in the thread, that's the base. If they mentioned a piece by description, identify it. If neither is clear, ask one short question to nail it down ("which listing are you looking at, or do you have a link?").
2. Compute a quote that includes the rush fee, plus any custom requests, via resolveQuote with wantsRush:true.
3. State the total. Make it clear the price already includes rush.
4. On acceptance, the system generates a custom Etsy listing that captures the rush. The customer checks out THAT listing — not the original.

## How to mention rush when you do (the offer phrasings)

Brief, optional, FYI-toned. One sentence appended to your substantive answer; the rush mention rides along, it's not the focus. Use phrasings consistent with the rest of the playbook:

- "Just in case it'd help with the timing, we offer expedited production for $15 which cuts production time from 4 to 5 days down to 2 to 3 days."
- "If you'd like to speed things up, we offer expedited production for $15 which gets you to 2 to 3 days instead of 4 to 5."
- "Heads up, we also offer a $15 rush option that drops production to 2 to 3 days if the timing is tight."

When the customer accepts rush, transition to the custom-listing flow described above. Don't add language like "just check the rush box at checkout" — there is no such box.

Avoid (these read pushy or presumptuous, or describe a mechanism that doesn't exist):
- "Given you're in a rush, we offer..." (presumes their state)
- "Since you mentioned [date], the rush option is..." (restates their words)
- "Highly recommend the rush option for this!" (sounds like an upsell)
- "Just add the rush option at checkout" (this mechanism does not exist)
- "You can pick rush before placing the order" (this mechanism does not exist)

## Don't repeat the offer

If rush has already been offered earlier in THIS active conversation (by you or by staff), don't offer it again. The customer can come back to it.
`.trim();

    // ── (5) Product family disambiguation + dimensions questions ───
    // The damaging failure mode this addresses:
    //
    // Customer asked "size of the cross charm FOR NECKLACE (not
    // huggie), compared to a coin." The customer's wording was
    // explicit and parenthesized — they pre-disambiguated for the
    // agent. Despite that, the agent provided huggie-scale
    // dimensions (7mm × 5.4mm, "about the size of the date stamped
    // on a penny") — apparently inferring family from the listing
    // title's word "Tiny" rather than from the customer's explicit
    // "for necklace (not huggie)" signal.
    //
    // Two corrections needed:
    //   a) Family is whatever the customer said it is. Title
    //      adjectives ("Tiny", "Small", "Mini") don't override an
    //      explicit family signal.
    //   b) "How big is it / size / dimensions" is a sizes question.
    //      The line sheet shows size references visually and is the
    //      safest answer, plus it surfaces other size options the
    //      customer hadn't considered. Made-up specific mm
    //      measurements are NOT a substitute for it.
    const familyDisambiguationAddendum = `

# PRODUCT FAMILY DISAMBIGUATION

Custom Brites makes three SEPARATE product families, each with its own size scale:

- **Necklace charms** — the larger pendants worn on a chain
- **Huggie charm earrings** — the much smaller charms that dangle from a huggie hoop
- **Stud earrings** — small studs worn directly in the ear

A "cross charm" exists in more than one of these families with very different sizes. A customer asking about the necklace cross charm and a customer asking about the huggie cross charm are asking about physically different products. Conflating them produces a wrong answer the customer will catch.

## How to lock in family

The customer's explicit statement of family is authoritative. When they write something like:
- "the cross charm FOR NECKLACE"
- "for the necklace (not huggie)"
- "the necklace version"
- "the huggie one"
- "stud earrings, not necklaces"

— that IS the family. Use it. Do NOT override it based on:
- A word like "Tiny" / "Small" / "Mini" in a listing title (these don't determine family; a tiny necklace charm is still a necklace charm).
- An assumption about which family the customer "probably" meant.
- The fact that a listing card thumbnail looks small.

When the customer has NOT specified family explicitly, fall back in this order:
1. Listing reference (if a listing URL was pasted, the listing's category usually pins family — necklace listings, huggie listings, and stud listings are distinct).
2. Earlier turns in the active conversation that established family.
3. Ask one short clarifying question. ("Is this for a necklace charm or a huggie charm?")

## Dimensions / "how big" questions

When a customer asks how big a charm is, what size it is, how it compares to something, or otherwise asks for physical dimensions, the best answer is the line sheet for the relevant family. The line sheet shows the actual sizes visually with scale references — that's what these questions are really asking for, and it's the source of truth the agent shouldn't try to substitute with from-memory mm figures.

So: on a sizes question, set \`attach_line_sheet: true\` for the family the customer is asking about and write a short reply inviting them to look at the sheet. If you also want to give a one-line ballpark to anchor expectations ("the smallest necklace charm is roughly X" / "they range from Y to Z"), only do so when you have a high-confidence source for the number — context-pulled listing dimensions, a prior turn that established it, or sales playbook size bands. Never fabricate specific mm dimensions to sound helpful. A sentence saying "the line sheet shows the actual sizes side by side" is more useful than a wrong measurement.

This is also the moment the line sheet doubles as an upsell: a customer asking about ONE size sees the full range, and many of them upsize or add to their order after seeing it.

## When the customer's family contradicts the listing they pasted

If the customer pastes a listing for, say, a huggie cross charm but explicitly asks about the necklace version, the customer is right and the listing is wrong-or-misread. Acknowledge the necklace context, send the necklace line sheet, and (briefly, if it helps) point out you have a separate necklace listing for the cross charm. Don't just answer with the huggie-listing's dimensions because that's what the URL pointed at.
`.trim();

    // ── (6) No fake follow-through promises ─────────────────────────
    // The damaging failure mode this addresses:
    //
    // Customer designed a custom graduation charm with engraving
    // text, sent dimensions and material confirmation, said they'd
    // come back later. Agent replied "we'll reference this
    // conversation when you're ready." That's a promise we don't
    // honor — there's no system that pings an operator weeks later
    // to remind them about a pending conversation. The agent
    // committed to a future human action it cannot trigger or
    // guarantee. The customer comes back, finds nothing prepared,
    // experience is poor.
    //
    // The right move on a custom-design turn is one of:
    //   a) Compute a quote and offer the custom listing now (price
    //      is statable, design fits within standard options).
    //   b) Send the family's line sheet for any genuinely missing
    //      info (size, finish, etc.) so the customer can finalize.
    //   c) Escalate to operator review (set ready_for_human_approval
    //      true). An operator IS a real follow-through path.
    //
    // What the agent must not do is commit to a future action that
    // requires unprompted human attention without flagging the
    // thread for review.
    const noFakeFollowthroughPromisesAddendum = `

# NO FAKE FOLLOW-THROUGH PROMISES (system addendum, structural rule)

The agent cannot promise anything that requires future human action without flagging the thread for review. The system does not automatically notify operators "this customer is going to come back next week — be ready." If you write a reply that implies someone on our side will remember, watch, or proactively re-engage on this thread, you have created a promise the system cannot keep, and the customer will return to find nothing prepared.

This is a hard rule, not a soft preference.

## Forbidden — never write any of these or anything similar

- "We'll reference this conversation when you're ready"
- "We'll have everything queued up for when you come back"
- "Just message us when ready and we'll pull it together"
- "We'll keep your specs on file"
- "We'll remember this for next time"
- "We'll be here when you're ready"
- "Reach back out and we'll have it ready for you"
- "We'll watch for your reply"
- "I'll set this aside for you"
- Any phrasing that asks the customer to come back later AND implies our side has prepared or will prepare anything in advance

## Permitted promises (because the agent itself delivers them in this turn)

You CAN promise things you are providing right now, in this same reply:
- A quote stated in the reply (resolveQuote was called, the price is in the message).
- A line sheet that's actually attached to this reply (attach_line_sheet:true and the line sheet image is there).
- A custom Etsy listing reference, IF the listing has been generated and is part of this reply's attachments. You may say "the custom listing is attached" only when it actually is.
- Tracking information you've looked up and included.
- An explicit operator follow-up, IF you set \`ready_for_human_approval: true\`. That flag flags the thread for an operator to handle, which is a real follow-through path. Saying "the team will follow up" is honest when it accompanies the flag.

## What to do on a "I'll get back to you later" turn

When the customer signals they're going to come back later and asks a procedural question ("do I need to write anything on the order?", "how do I order this?"), do one of:

1. **Provide the custom listing now.** If the customer has given you enough to act on (a clear design, dimensions, material, engraving text) and the build fits standard line-sheet options, compute the quote with resolveQuote and proceed toward custom_listing creation (customer_accepted:true on acceptance). The custom listing is the artifact that captures the spec — not a promise to remember.

2. **Send the line sheet for missing info.** If there's specific information still needed before a quote is possible (chain length, finish choice, etc.), set attach_line_sheet:true for the right family and write a short note inviting them to finalize when ready. The line sheet is concrete; the customer has it whenever they want to come back.

3. **Escalate genuinely-custom work.** If the design is outside the standard line sheet (unusual shape, novel concept, dimensions outside our normal range) and you cannot quote it, set ready_for_human_approval:true with a complete needs_review_synopsis. The reply text can honestly say "the team will review this and follow up with a custom quote" — because an operator will, and they'll see this thread in the review queue.

In none of those three cases do you say "we'll reference this conversation later." Either the artifact is in this reply, or the thread is flagged for a human, or the customer is being asked for a specific clarifying input.

## When the customer says "I'll come back next week" and there's nothing to capture

Sometimes the customer is genuinely just saying they're not ready to commit yet, and there's no meaningful action to take this turn. In that case, a short acknowledgment is fine — but it must NOT promise advance preparation. Examples that are OK:

- "Sounds good. The line sheet stays valid; whenever you're ready to lock in specs, just send through."
- "Take your time. When you're ready, message back with the size and finish you'd like and we'll proceed."

Both put the next-action ball in the customer's court. Neither implies our side is preparing or watching anything.
`.trim();

    // ── (7) Existing-order context detection ────────────────────────
    // The damaging failure mode this addresses:
    //
    // CustomBrites previously messaged the customer asking to confirm
    // a spec ("which disc size do you want for your silver circle
    // duck charm? 14, 12, 10mm"). Customer responded "12mm". Agent
    // restarted discovery with "is this a necklace charm, huggie, or
    // stud?" — even though the customer has a recent paid order for
    // exactly that piece, and the staff question pinned it.
    //
    // The agent needs to recognize when the active conversation is a
    // continuation of an existing-order spec confirmation, and act
    // accordingly: confirm the spec, advance, do NOT relitigate
    // basics that the existing order already establishes.
    const existingOrderContextAddendum = `

# EXISTING-ORDER CONTEXT DETECTION (system addendum)

A thread can be a sales discovery conversation OR a continuation of an existing-order spec confirmation. These need different handling, and the cue is in the conversation itself, not in the thread label.

## Signals that you are in an existing-order context

Any of the following, especially when several co-occur:
- The customer has a recent PAID order in their order history visible in context, with item descriptions that match what the conversation is about.
- The IMMEDIATELY PRECEDING staff message asked the customer to confirm a specific detail of their order ("which disc size do you want", "which length did you choose", "can you confirm the engraving spelling").
- The customer's current message is a brief, direct answer to that staff question ("12mm", "20 inch", "yes that's correct").
- The conversation thread is short, recent, and the topic is a spec the order would need.

When these signals line up, the customer is NOT in early discovery. The customer is finalizing an existing purchase. Your reply should reflect that.

## What to do in an existing-order context

- Confirm the spec the customer just gave you ("Got it, 12mm noted, we'll proceed with that on your order.").
- Do NOT restart discovery questions whose answers are already implied by the existing order. If the order is for a "silver circle duck charm" and the customer is confirming disc size, the family (necklace charm vs huggie vs stud) is already pinned by the listing they bought — don't ask "is this for a necklace, huggie, or stud earring?".
- Use lookup_order_details(receiptId) when you need to confirm the listing's family or other specs that aren't already in the visible message context.
- Don't recompute prices. They already paid. The spec confirmation is the only outstanding action.
- If the customer's answer to the spec question is ambiguous or you genuinely need another detail to fulfill, ask the one specific thing — but ground it in the existing order's terms ("got it, 12mm noted — and on the chain length, want to keep the 16-inch we have on file or change it?"), not in fresh-discovery language.

## What NOT to do

Restarting discovery on an existing order looks like a bug to the customer — they think we don't know who they are or what they bought. Specifically don't:
- Ask which family (necklace / huggie / stud) when the order's listing already says it.
- Re-pitch the line sheet on a closed-spec confirmation.
- Treat the customer's brief reply as a fresh inquiry needing full intake.
- Compute a new quote — they've paid, no quote needed.

If the conversation is genuinely ambiguous (you can't tell whether it's about an existing order or a new purchase), ask one short clarifying question instead of restarting either path. ("Quick check — is this for the order you placed last week, or a new piece?")
`.trim();

    // ── (8) Extended collateral library ─────────────────────────────
    // The agent now has access to four distinct collateral kinds, not
    // just line sheets. Each has its own attach flag in the JSON
    // output, and the system attaches whatever subset the agent
    // requests in a single Etsy reply (multiple chips). Operator bias:
    // attach MORE rather than less when the customer would benefit
    // from the information; people like to be informed.
    //
    // Damaging failure mode this addendum prevents: the agent says
    // "the line sheet shows how it sits on the chest" — which is
    // false. The line sheet doesn't show fit. We have a separate
    // fit-reference collateral for that. The agent must pick the
    // RIGHT artifact for the question, and may stack multiple.
    const extendedCollateralAddendum = `

# EXTENDED COLLATERAL LIBRARY (system addendum)

You can attach four distinct kinds of visual collateral to your reply. Each has its own structured flag in your JSON output. You may set ANY subset (or all four) on a single reply — each becomes a separate image attachment in the same Etsy message.

## The four kinds

**1. Line sheet — \`attach_line_sheet: true\`**
A visual sheet showing the available styles, sizes, codes, and prices for a product family (necklace charms, huggie charms, stud earrings). This is what you send for: family-shopping questions, sizing questions, "what options do you have for X", catalog browsing, dimension comparisons within a family. The line sheet shows the products themselves at scale; it does NOT show how a piece is worn or how it sits on a person.

**2. Fit reference — \`attach_fit_reference: true\`**
A visual showing how a necklace sits on the body — chain length comparisons (16", 18", 20", etc.) shown on a neck model. This is what you send when the customer asks "how does it fit", "how does it sit on the chest", "how long is 18 inches really", "I'm petite, will it look right", "where does it hang", "how low does it sit." Do NOT claim the line sheet shows fit — it doesn't. Use the fit reference for fit questions.

**3. Metal comparison — \`attach_metal_comparison: true\`**
A visual showing the side-by-side differences between Gold Filled vs Gold Plated vs 14k Solid Gold. Send this when the customer asks about metal options, gold types, "what's the difference between gold filled and solid gold", "is it real gold", "will it tarnish", "which gold is best", price-vs-quality comparisons across metals, allergy or skin-reaction concerns about metal type, or otherwise needs to choose between metals.

**4. Care instructions — \`attach_care_instructions: true\`**
A visual showing how to care for and clean fine custom jewellery. Send this proactively on most custom-order conversations — once the customer has accepted a quote or is finalizing a purchase, including care instructions in the same message helps them get the most life out of the piece. Also send when the customer asks how to clean, store, wear, or maintain jewellery; when they mention worry about tarnish or damage; or when the order is for a high-value piece (solid gold, large bulk orders).

## Bias: attach MORE rather than less

When more than one kind would help, attach more than one. Operator policy is that customers like to be informed and learn. Examples of stacked attachments:

- Customer asks "I'm petite, would 18 inches sit too long?" with a question about gold filled vs solid gold pricing → attach **fit_reference + metal_comparison** in one reply. Two chips, one message.
- Customer accepts a quote on a custom necklace → attach **care_instructions** in the acceptance reply. They've just bought; help them keep the piece looking good.
- Customer asks for line sheet and also asks about gold types → attach **line_sheet + metal_comparison**.
- Customer has a tight deadline and is choosing between two designs → attach **line_sheet + fit_reference** so they can see size AND fit at once and decide faster.

Do NOT stack collateral the customer doesn't need. Sending care instructions on a tracking inquiry is noise. Use judgment.

## Don't misattribute

Each artifact shows one thing well. If the line sheet doesn't show fit, don't say it does — attach the fit reference instead. Never write "the line sheet shows how it sits on the chest" or "the line sheet shows the difference between metals" — those claims are false. The line sheet shows products at scale only. Fit and metal differences live in their own artifacts.

## When a kind is requested but missing

If you set a flag but no collateral exists for that kind, the system logs the miss and the other attached kinds still go out. The reply text should not promise something that didn't attach — see the line-sheet promise rule for the same principle. If you're not sure all the collateral you want is available, write the reply to invite-without-promising ("here's our line sheet — happy to share more if helpful") so a missing chip doesn't leave a broken promise.
`.trim();

    // Concatenate. Keep a clear separator so the addendum is visible
    // in any prompt-debugging output without being mistaken for
    // operator-edited content.
    const fullSystemPrompt = String(promptLoad.prompt || "").trim()
      + "\n\n---\n\n"
      + lineSheetEagernessAddendum
      + "\n\n---\n\n"
      + acceptanceSyncAddendum
      + "\n\n---\n\n"
      + attachLineSheetSyncAddendum
      + "\n\n---\n\n"
      + noRestateStaffAddendum
      + "\n\n---\n\n"
      + moveForwardAddendum
      + "\n\n---\n\n"
      + rushEagernessAddendum
      + "\n\n---\n\n"
      + familyDisambiguationAddendum
      + "\n\n---\n\n"
      + noFakeFollowthroughPromisesAddendum
      + "\n\n---\n\n"
      + existingOrderContextAddendum
      + "\n\n---\n\n"
      + extendedCollateralAddendum;

    let loopResult;
    try {
      loopResult = await runToolLoop({
        model         : AI_MODEL,
        maxTokens     : AI_MAX_TOKENS,
        system        : fullSystemPrompt,
        initialMessages,
        toolSpecs,
        toolExecutors,
        toolContext   : { threadId, salesCtx },
        effort        : AI_EFFORT,
        useThinking   : true,
        maxIterations : MAX_TOOL_ITERATIONS
      });
    } catch (e) {
      await writeAudit({
        threadId, eventType: "sales_agent_call_failed",
        payload: { error: e.message }, outcome: "failure"
      });
      return { statusCode: 502, headers: CORS,
               body: JSON.stringify({ error: `AI call failed: ${e.message}` }) };
    }

    // ── Extract final text ──
    const finalText = (loopResult.finalResponse && Array.isArray(loopResult.finalResponse.content)
      ? loopResult.finalResponse.content
      : []
    )
      .filter(b => b && b.type === "text")
      .map(b => b.text || "")
      .join("\n")
      .trim();

    // ── Parse JSON (defensive) ──
    const parsed = tryParseJson(finalText);
    if (!parsed) {
      await writeAudit({
        threadId, eventType: "sales_agent_unparseable_output",
        payload: { rawPreview: finalText.slice(0, 500),
                   toolCalls: (loopResult.toolCalls || []).map(t => t.name) },
        outcome: "failure"
      });
      // Escalate to human review — we have a half-formed reply we can't trust.
      await db.collection(THREADS_COLL).doc(threadId).set({
        status: "pending_human_review",
        lastSalesAgentBlockReason: "unparseable_output",
        updatedAt: FV.serverTimestamp()
      }, { merge: true });
      return { statusCode: 500, headers: CORS,
               body: JSON.stringify({
                 error: "AI output not parseable JSON",
                 rawPreview: finalText.slice(0, 500),
                 escalated: true
               }) };
    }

    // ── Deterministic reply cleanup before validation/persistence ──
    // Removes repeated capability confirmations from prior outbound turns while
    // preserving the AI's structured quote fields.
    if (typeof parsed.reply === "string") {
      parsed.reply = applySalesReplyGuard(parsed.reply, priorOutboundTexts);
    }

    // v4.3.15 — Acceptance-signal consistency backstop.
    //
    // The prompt addendum tells the agent that reply text and
    // customer_accepted must agree, but model adherence to soft rules
    // is imperfect. Joanna's banana-charm round 2 hit this exact case
    // THREE turns in a row: the model wrote "we'll send the custom
    // listing your way" while emitting customer_accepted=false. The
    // listing pipeline never fired because direct-fire reads the
    // structured field, not the prose.
    //
    // We don't auto-flip customer_accepted=true (that would risk false
    // positives — the model might use commitment-shaped language in
    // a hypothetical or recap context). Instead we flag the
    // inconsistency: log the audit event, route the turn to operator
    // review with a clear synopsis. The operator sees the conflict,
    // can either edit the draft to fire the listing manually, or fix
    // the conversation themselves. Better than silently shipping a
    // broken promise.
    //
    // The detection is deliberately strict — looks for first-person
    // sender-side commitments ("we'll send", "I'll send", "sending
    // your way") in close proximity to "listing" or "link". Generic
    // mentions of "the listing" in a question or recap context don't
    // trigger.
    if (typeof parsed.reply === "string" && parsed.customer_accepted !== true) {
      const replyLower = parsed.reply.toLowerCase();
      const commitmentPatterns = [
        /\b(we'll|we will|i'll|i will)\s+send(?:ing)?\b[^.?!]{0,50}\b(listing|link)\b/,
        /\b(sending|sent)\s+(?:you\s+)?(?:the\s+)?(?:custom\s+)?(?:listing|link)\b[^.?!]{0,50}\b(your way|now|through|over|across)\b/,
        /\bget(?:ting)?\s+the\s+(?:custom\s+)?listing\s+(?:over|to)\s+you\b/,
        /\b(creating|generating|making)\s+the\s+(?:custom\s+)?listing\s+(?:for you|now)\b/
      ];
      const hit = commitmentPatterns.find(rx => rx.test(replyLower));
      if (hit) {
        console.warn(
          `[salesAgent] Acceptance-signal inconsistency for ${threadId}: ` +
          `reply text commits to sending a listing but customer_accepted=false. ` +
          `Pattern matched: ${hit}. Routing to operator review.`
        );
        await writeAudit({
          threadId, eventType: "sales_agent_acceptance_inconsistency",
          payload: {
            replyExcerpt: parsed.reply.slice(0, 250),
            patternMatched: String(hit),
            originalCustomerAccepted: !!parsed.customer_accepted
          },
          outcome: "blocked",
          ruleViolations: ["acceptance_signal_inconsistency"]
        });
        // Force human review for this turn. The operator sees the draft
        // with a clear synopsis explaining the inconsistency and can
        // decide whether to (a) tick customer_accepted and re-fire the
        // pipeline, or (b) rewrite the reply to remove the commitment.
        parsed.ready_for_human_approval = true;
        parsed.needs_review_synopsis =
          `ACCEPTANCE-SIGNAL INCONSISTENCY\n\n` +
          `The AI's reply commits to sending a custom listing, but the structured ` +
          `acceptance signal was not set, so the listing pipeline did NOT fire. ` +
          `Without operator action the customer will wait indefinitely.\n\n` +
          `AI's reply (excerpt): "${parsed.reply.slice(0, 200)}${parsed.reply.length > 200 ? "..." : ""}"\n\n` +
          `Operator action: review the conversation. If the customer has indeed accepted ` +
          `a finalized quote, manually trigger listing creation (set customerAccepted=true ` +
          `and customListingStatus="queued" on the thread) OR send the listing through ` +
          `the standard manual flow. If the customer has not yet accepted, edit the ` +
          `reply to remove the commitment language before sending.`;
      }
    }

    // ── Validate quote if present (server-side gate) ──
    const quoteValidation = await validateQuotedPriceIfPresent({ threadId, parsed, salesCtx });
    if (quoteValidation && quoteValidation.valid === false && !quoteValidation.skip) {
      await writeAudit({
        threadId, eventType: "sales_agent_quote_invalid",
        payload: {
          reason         : quoteValidation.reason,
          allowedMin     : quoteValidation.allowedMin,
          allowedMax     : quoteValidation.allowedMax,
          rejectedQuote  : parsed.quoted_total_usd
                         || (parsed.draft_custom_order_listing && parsed.draft_custom_order_listing.totalUsd),
          violations     : quoteValidation.violations || null
        },
        outcome: "blocked",
        ruleViolations: [quoteValidation.reason]
      });
      await db.collection(THREADS_COLL).doc(threadId).set({
        status: "pending_human_review",
        lastSalesAgentBlockReason: quoteValidation.reason,
        updatedAt: FV.serverTimestamp()
      }, { merge: true });
      await salesCtx._ref.set({
        lastSalesAgentBlockReason: quoteValidation.reason,
        lastTurnAt: FV.serverTimestamp()
      }, { merge: true });
      return { statusCode: 422, headers: CORS,
               body: JSON.stringify({
                 error: "Quote validation failed — escalated to human review",
                 reason: quoteValidation.reason,
                 allowedMin: quoteValidation.allowedMin,
                 allowedMax: quoteValidation.allowedMax,
                 escalated: true
               }) };
    }

    // ── Detect escalation/handoff signals ──
    //
    // v4.0: no stages, no transitions. The agent expresses outcome via
    // two flags:
    //   - ready_for_human_approval / needs_review_synopsis populated:
    //     operator must look at this thread before customer reply ships
    //     (Quote-row escalations, RUSH_BLOCKED_BY_QUOTE_ROW, etc.)
    //   - advance_stage === "abandoned": customer pivoted out of sales
    //     (e.g., asking for support on an old order). Thread routes to
    //     the operator's normal inbox, not the sales rail.
    //   - advance_stage === "human_review": legacy off-ramp, kept for
    //     backward compat. Same effect as ready_for_human_approval.
    const rawAdvance = parsed.advance_stage || null;
    const wantsHumanReview =
      rawAdvance === "human_review" ||
      !!parsed.ready_for_human_approval;
    const isAbandoned = rawAdvance === "abandoned";

    // ── Compose draft + persist ──
    const draftId = "draft_" + threadId;

    // v2.1 — Needs Review handoff. When the AI sets needs_review_synopsis,
    // the draft is flagged for operator review. The synopsis is written to
    // `needsReviewSynopsis` on the draft (operator-facing field, surfaced
    // in the Needs Review sidebar) and into the audit log.
    //
    // v0.9.22 BUGFIX (Image A): the draft `text` field MUST be the
    // customer-facing reply (or empty), never the operator synopsis. The
    // earlier behavior pushed the synopsis into `text`, which meant the
    // operator opened the thread, saw the staff-reply textarea pre-filled
    // with raw debug content like "ACCEPTANCE-SIGNAL INCONSISTENCY ...
    // Operator action: review the conversation. If the customer has indeed
    // accepted ..." That's debugging output, not a draft. The operator
    // either had to delete it before composing or could accidentally send
    // it. Both are wrong.
    //
    // Correct surfaces:
    //   - Staff reply textarea (`text`)   — customer-facing reply only
    //   - Needs Review sidebar / panel    — `needsReviewSynopsis`
    //   - Audit log                       — full synopsis + payload
    //
    // We detect Needs Review handoff via either:
    //   1. parsed.needs_review_synopsis is a non-empty string, OR
    //   2. parsed.advance_stage === "human_review" (legacy path)
    const isNeedsReviewHandoff =
      (typeof parsed.needs_review_synopsis === "string" && parsed.needs_review_synopsis.trim().length > 50)
      || parsed.advance_stage === "human_review";

    const customerFacingReply = (typeof parsed.reply === "string" && parsed.reply.trim())
      ? parsed.reply.trim()
      : "";

    // Draft body: ALWAYS the customer-facing reply (or empty). The
    // synopsis lives only in `needsReviewSynopsis` on the draft doc.
    const replyText = customerFacingReply;

    const aiConfidence = (typeof parsed.confidence === "number" && parsed.confidence >= 0 && parsed.confidence <= 1)
      ? parsed.confidence : 0.5;

    // ─── v4.3.12: line-sheet attachment construction ──────────────────
    //
    // When the agent decides to send the line sheet (parsed.attach_line_sheet
    // === true), construct an image-attachment record from the first
    // attachable collateral match in recommendedCollateral. "Attachable"
    // means the collateral was uploaded through op:"upload" (so it has
    // storagePath + contentType), not just registered as an external URL.
    // The resulting attachment record matches the same shape that
    // etsyMailDraftAttachment writes for operator drag-and-drop uploads,
    // so the existing draft-send pipeline (and the Chrome extension's
    // image-injection step) handles it without modification.
    //
    // If the agent set attach_line_sheet:true but no attachable collateral
    // exists for the family, we don't blow up — the draft saves with no
    // attachment, the prompt addendum's edge-case guidance kicks in
    // (agent should set ready_for_human_approval in that case anyway),
    // and the audit log records the miss for operator visibility.
    // ─── v0.9.22: multi-kind collateral attachment construction ───────
    //
    // The agent can now attach any subset of these collateral kinds
    // by emitting matching boolean flags on its JSON output:
    //
    //    attach_line_sheet      : line sheet for the recommended family
    //    attach_fit_reference   : how a necklace sits on the body
    //    attach_metal_comparison: gold filled vs gold plated vs solid
    //    attach_care_instructions: jewellery care guide
    //
    // Each kind looks up the FIRST attachable collateral entry of that
    // kind (in `recommendedCollateral`, falling back to a category-wide
    // search if absent) and produces one image attachment. Multiple
    // kinds → multiple chips in the operator UI, all delivered to the
    // customer in one Etsy message.
    //
    // The legacy `attach_line_sheet:true` path is preserved and
    // unchanged in semantics; it's just one entry in the new table.
    //
    // If a kind is requested but no usable collateral exists for it,
    // we don't blow up — the missing kind is logged in
    // collateralAttachInfo[] for operator visibility, the other kinds
    // still attach, and the audit log records the misses.
    const COLLATERAL_KINDS_REQUESTED = [
      { flag: "attach_line_sheet",       kind: "line_sheet",       label: "line sheet" },
      { flag: "attach_fit_reference",    kind: "fit_reference",    label: "fit reference"  },
      { flag: "attach_metal_comparison", kind: "metal_comparison", label: "metal comparison" },
      { flag: "attach_care_instructions",kind: "care_instructions",label: "care instructions" }
    ];

    let attachmentsToWrite = [];
    let collateralAttachInfo = [];   // for audit, one entry per kind requested
    // back-compat: legacy code/audits read `lineSheetAttachInfo`. Kept
    // populated for the line-sheet kind specifically.
    let lineSheetAttachInfo = null;

    // Helper: find first attachable collateral matching a kind. Tries
    // recommendedCollateral first (which is the family-relevant subset
    // the agent was told about), then falls back to a global search by
    // kind so a kind like "metal_comparison" or "care_instructions"
    // — which is family-independent — still resolves even when the
    // family-scoped recommendation list didn't include it.
    async function findAttachableForKind(kind) {
      const recList = Array.isArray(recommendedCollateral) ? recommendedCollateral : [];
      const recHit = recList.find(c => c && c.kind === kind && c.storagePath && c.uploadedContentType);
      if (recHit) return recHit;
      // Fallback: ask the collateral search for any active entry of this
      // kind, regardless of category. Family-independent kinds
      // (metal_comparison, care_instructions, fit_reference) will
      // typically be stored without a category or under a generic
      // category like "general" / "_all".
      try {
        const { searchCollateral } = require("./etsyMailCollateral");
        const result = await searchCollateral({ kind, limit: 5 });
        const matches = (result && Array.isArray(result.matches)) ? result.matches : [];
        // Pick the first attachable: must have storagePath (uploaded
        // file, not just URL reference) AND its kind must match exactly.
        // searchCollateral can return scored matches that don't match
        // kind exactly when kind score is just a boost not a filter,
        // so verify here.
        const exactKindHit = matches.find(c =>
          c && c.kind === kind && c.storagePath && c.uploadedContentType
        );
        if (exactKindHit) return exactKindHit;
      } catch {}
      return null;
    }

    for (const { flag, kind, label } of COLLATERAL_KINDS_REQUESTED) {
      if (parsed[flag] !== true) continue;
      const hit = await findAttachableForKind(kind);
      if (hit) {
        const synthId = "att_collateral_" + (hit.id || Math.random().toString(36).slice(2, 10));
        const ct = hit.uploadedContentType;
        attachmentsToWrite.push({
          attachmentId : synthId,
          type         : "image",
          storagePath  : hit.storagePath,
          proxyUrl     : "/.netlify/functions/etsyMailImage?path=" + encodeURIComponent(hit.storagePath),
          contentType  : ct,
          bytes        : typeof hit.uploadedSizeBytes === "number" ? hit.uploadedSizeBytes : null,
          filename     : hit.uploadedFilename || ((hit.name || kind) + "." + ((ct.split("/")[1] || "png"))),
          source       : "collateral",
          collateralId : hit.id || null,
          collateralName : hit.name || null,
          collateralKind : hit.kind || kind
        });
        const info = {
          kind, label,
          decided: true, attached: true,
          collateralId: hit.id || null,
          collateralName: hit.name || null
        };
        collateralAttachInfo.push(info);
        if (kind === "line_sheet") lineSheetAttachInfo = info;
      } else {
        const info = {
          kind, label,
          decided: true, attached: false,
          reason: "no_active_collateral_for_kind"
        };
        collateralAttachInfo.push(info);
        if (kind === "line_sheet") lineSheetAttachInfo = info;
        console.warn(`salesAgent: ${flag}=true but no attachable ${label} collateral for thread ${threadId}`);
      }
    }
    // ──────────────────────────────────────────────────────────────────

    await db.collection(DRAFTS_COLL).doc(draftId).set({
      draftId,
      threadId,
      text                  : replyText,
      attachments           : attachmentsToWrite,
      // v0.9.18 — Mirror attachments into draftAttachments so the
      // operator UI's hydrateComposerFromDraft (which historically
      // read draftAttachments) sees the line-sheet chip and renders
      // it above the staff reply textarea, just like a drag-dropped
      // image. Belt-and-suspenders pairing with the UI-side merge
      // fix in v0.9.18 of etsy-mail-1.html: either patch alone makes
      // the chip appear; both together also self-heals older drafts
      // and keeps the data path predictable for any future surface
      // (e.g. a mobile UI) that reads either field.
      draftAttachments      : attachmentsToWrite,
      referenceAttachments  : compactAttachmentList(referenceAttachments),
      status                : "draft",     // NEVER "queued" in Step 2
      generatedByAI         : true,
      generatedBySalesAgent : true,
      aiConfidence,
      aiReasoning           : String(parsed.reasoning || "").slice(0, 1000),
      aiNeedsPhoto          : !!parsed.needs_photo,
      aiMissingInputs       : Array.isArray(parsed.missing_inputs) ? parsed.missing_inputs.slice(0, 12) : [],
      aiCollateralReferenced: Array.isArray(parsed.collateral_referenced) ? parsed.collateral_referenced : [],
      aiRecommendedCollateral: recommendedCollateral,
      readyForHumanApproval : !!parsed.ready_for_human_approval,
      // v4.1 — customer_accepted is the signal for the downstream
      // listing-creator automation. The agent sets it true ONLY on the
      // turn the customer explicitly accepts a previously-quoted price.
      // The future automation watches the draft (or a pubsub on this
      // field) and creates the Etsy custom listing.
      // v4.2 — customerAcceptedAt mirrors the timestamp written on the
      // thread doc (used by the listing-creator cron's cool-down). Only
      // emitted on the turn customer_accepted flips true so we don't
      // clobber a prior acceptance timestamp on later turns.
      // v4.3 — DURABILITY: never auto-flip customerAccepted from true to
      // false on a follow-up turn. The previous behavior wrote
      // `customerAccepted: !!parsed.customer_accepted` on every turn,
      // which meant a harmless follow-up like "btw can you ship by
      // Friday?" (where parsed.customer_accepted is naturally false on
      // that turn) would WIPE a valid prior acceptance, then the cron's
      // retraction-cleanup branch would lock the thread into "retracted"
      // and the listing would never get created. Fix: only emit the
      // acceptance fields on actual acceptance turns. Retraction is now
      // operator-driven (manual flip in the dashboard) — much rarer than
      // the false-retraction failure mode this prevents.
      ...(parsed.customer_accepted ? {
        customerAccepted   : true,
        customerAcceptedAt : FV.serverTimestamp()
      } : {}),
      draftCustomOrderListing: parsed.draft_custom_order_listing || null,
      // v2.1 — Needs Review handoff fields
      isNeedsReviewHandoff       : isNeedsReviewHandoff,
      needsReviewSynopsis        : isNeedsReviewHandoff ? (parsed.needs_review_synopsis || null) : null,
      customerFacingReplyDraft   : isNeedsReviewHandoff ? customerFacingReply : null,
      // v2.1 — preserve resolver result on the draft for forensics + Step 3 hand-off
      resolverResult        : (salesCtx._lastResolverResult && salesCtx._lastResolverResult.success)
                              ? salesCtx._lastResolverResult : null,
      createdBy             : "sales-agent",
      createdAt             : FV.serverTimestamp(),
      updatedAt             : FV.serverTimestamp(),
      // Send-state fields kept null — the operator's manual Send action
      // sets these via the existing etsyMailDraftSend.enqueue path.
      sendSessionId         : null,
      sendClaimedAt         : null,
      sendHeartbeatAt       : null,
      sendAttempts          : 0,
      sendError             : null,
      sentAt                : null
    }, { merge: true });

    // ── Persist sales context updates ──
    const ctxUpdates = {
      accumulatedSpec : { ...(salesCtx.accumulatedSpec || {}), ...(parsed.extracted_spec || {}) },
      referenceAttachments: compactAttachmentList(referenceAttachments),
      lastCustomerFacingReply: customerFacingReply,
      lastTurnAt      : FV.serverTimestamp(),
      lastSalesAgentBlockReason: null   // clear any prior block reason on a successful turn
    };
    if (salesCtx._lastResolverResult) {
      ctxUpdates._lastResolverResult = salesCtx._lastResolverResult;
    }
    // Record quote in history if one was produced
    const quotedTotal = (typeof parsed.quoted_total_usd === "number") ? parsed.quoted_total_usd
                     : (parsed.draft_custom_order_listing && typeof parsed.draft_custom_order_listing.totalUsd === "number")
                       ? parsed.draft_custom_order_listing.totalUsd : null;
    if (typeof quotedTotal === "number") {
      const lrr = salesCtx._lastResolverResult;
      ctxUpdates.quoteHistory = FV.arrayUnion({
        at         : Date.now(),
        total      : quotedTotal,
        validated  : true,
        // v2.1 — record the resolver result instead of the band. This is
        // what the sales card UI reads for the "selected codes / bulk tier
        // / escalations" display.
        resolverResult: (lrr && lrr.success)
          ? {
              family        : lrr.family,
              quantity      : lrr.quantity,
              total         : lrr.total,
              perPieceAfterModifier: lrr.perPieceAfterModifier,
              subtotal      : lrr.subtotal,
              discountAmount: lrr.discountAmount,
              bulkTier      : lrr.bulkTier,
              escalations   : lrr.escalations || [],
              rush          : lrr.rush || null,
              shippingSummary: lrr.shippingSummary || null,
              lineItems     : (lrr.lineItems || []).map(li => ({
                code: li.code, label: li.label,
                priceUsd: li.priceUsd ?? null,
                priceQuote: !!li.priceQuote,
                isModifier: !!li.isModifier
              }))
            }
          : null,
        hadEscalations: !!(lrr && Array.isArray(lrr.escalations) && lrr.escalations.length)
      });
      ctxUpdates.totalQuotedUsd = quotedTotal;
    }
    await salesCtx._ref.set(ctxUpdates, { merge: true });

    // ── Update parent thread status ──
    // v4.0: simpler thread status mapping. The agent has finished a turn;
    // the thread sits in "sales_active" until escalation, abandonment,
    // or completion (handled by Step 3 close-listing flow elsewhere).
    let threadStatus;
    if (wantsHumanReview) {
      threadStatus = "pending_human_review";
    } else if (isAbandoned) {
      threadStatus = "sales_abandoned";
    } else {
      threadStatus = "sales_active";
    }

    await db.collection(THREADS_COLL).doc(threadId).set({
      status        : threadStatus,
      // v4.3.2 — On an acceptance turn, the AI's customer-facing reply
      // is going to be thrown away anyway: the listing-creator worker
      // sends its own message with the listing URL ("Here's the custom
      // listing for your necklace: <url>"). Marking the draft status as
      // "skipped_acceptance" lets the dashboard distinguish a useful
      // pending draft from a stale one, and avoids confusing the
      // operator with a Send-via-Etsy button on text that won't be sent.
      aiDraftStatus : parsed.customer_accepted ? "skipped_acceptance" : "ready",
      latestDraftId : draftId,
      aiConfidence,
      readyForHumanApproval: !!parsed.ready_for_human_approval,
      // v4.1 — customer_accepted signal for downstream listing-creator
      // automation. Mirrored on both draft and thread so a worker can
      // query either collection. quotedTotal mirrored for the same
      // reason — the automation needs the price without re-reading the
      // draft.
      // v4.3 — DURABILITY: see the long comment on the draft write
      // above. Acceptance fields are only written on actual acceptance
      // turns. Once customerAccepted is true on the thread, follow-up
      // turns leave it alone — the worker reads stable state.
      ...(parsed.customer_accepted ? {
        customerAccepted   : true,
        acceptedQuoteUsd   : (typeof quotedTotal === "number") ? quotedTotal : null,
        acceptedQuoteFamily: (salesCtx._lastResolverResult
                                && salesCtx._lastResolverResult.success
                                && typeof salesCtx._lastResolverResult.family === "string")
                                  ? salesCtx._lastResolverResult.family
                                  : null,
        customerAcceptedAt  : FV.serverTimestamp(),
        // customListingStatus: "queued" — explicit sentinel that the
        // cron's primary query keys off. Only written on acceptance, so
        // a follow-up turn doesn't overwrite a "creating" / "created"
        // state set later by the cron / worker.
        customListingStatus : "queued"
      } : {}),
      // lastResolverResult updates on every turn — it reflects the agent's
      // current understanding of what the customer wants. The locked
      // version of the spec at acceptance time is captured implicitly via
      // acceptedQuoteUsd and acceptedQuoteFamily above; the worker reads
      // both when it fires, so a customer changing one detail mid-stream
      // doesn't change the locked price or family.
      lastResolverResult   : (salesCtx._lastResolverResult && salesCtx._lastResolverResult.success)
                              ? salesCtx._lastResolverResult : null,
      updatedAt     : FV.serverTimestamp()
    }, { merge: true });

    // v4.3.2 — DIRECT-FIRE the listing-creator worker on acceptance turns.
    // The cron remains a safety net (recovers stuck/dropped invocations
    // every minute), but waiting up to 60s for the cron's next tick when
    // we already know the thread is ready is a needless delay.
    //
    // RACE PROTECTION (v4.3.3): we MUST atomically claim the thread
    // before firing the worker. The thread doc was just written above
    // with customListingStatus="queued" but no customListingStartedAt.
    // If we fired the worker without claiming, the next cron tick (up
    // to 60s later) would see status="queued" + missing startedAt and
    // claim it itself — firing a SECOND worker while the first is
    // still running. Two workers on the same thread would create
    // duplicate listings.
    //
    // The atomic claim mirrors etsyMailListingCreatorCron's tryClaim
    // semantics: transition status from "queued" to "creating" and set
    // customListingStartedAt. Once the cron sees a fresh startedAt
    // (< RECOVERY_TIMEOUT_MS old) on a "creating" thread, its race-
    // protection logic correctly skips.
    //
    // If the claim fails for any reason (Firestore txn conflict, the
    // status moved unexpectedly, env vars missing), we silently let
    // the cron handle it on its next tick — the worst-case is the
    // 60s delay that existed before this optimization.
    if (parsed.customer_accepted) {
      try {
        const siteUrl = process.env.URL || process.env.DEPLOY_URL;
        const secret  = process.env.ETSYMAIL_EXTENSION_SECRET;
        if (!siteUrl || !secret) {
          console.warn(`[salesAgent] direct-fire skipped (missing URL or secret env). Cron will pick this up.`);
        } else {
          // Atomic claim — same shape as cron's tryClaim, scoped to the
          // simpler "fresh acceptance, queued, no prior startedAt" case
          // since we just wrote queued on this same code path.
          const threadRef = db.collection(THREADS_COLL).doc(threadId);
          const claimed = await db.runTransaction(async (tx) => {
            const snap = await tx.get(threadRef);
            if (!snap.exists) return false;
            const d = snap.data();
            // Only claim if exactly the state we just wrote: queued + accepted +
            // no prior startedAt. Anything else means another mutator (cron,
            // operator, prior worker) has modified the thread; let cron handle it.
            if (d.customListingStatus !== "queued") return false;
            if (!d.customerAccepted)                return false;
            if (d.customListingStartedAt)           return false;
            tx.update(threadRef, {
              customListingStatus    : "creating",
              customListingStartedAt : FV.serverTimestamp(),
              customListingAttempts  : FV.increment(1),
              updatedAt              : FV.serverTimestamp()
            });
            return true;
          });

          if (!claimed) {
            console.warn(`[salesAgent] direct-fire claim lost for ${threadId} (cron will handle).`);
          } else {
            // Fire-and-forget. Don't await the response (background fns
            // return 202 immediately, but we don't want to block the
            // agent's response on even that round-trip).
            fetch(`${siteUrl}/.netlify/functions/etsyMailListingCreator-background`, {
              method : "POST",
              headers: {
                "Content-Type": "application/json",
                "X-EtsyMail-Secret": secret
              },
              body: JSON.stringify({ threadId })
            }).catch(e => console.warn(`[salesAgent] direct-fire worker fetch failed (non-fatal, claim already set; cron's stuck-sweep will recover after RECOVERY_TIMEOUT_MS): ${e.message}`));
            console.log(`[salesAgent] claimed and direct-fired listing worker for ${threadId}`);
          }
        }
      } catch (e) {
        console.warn(`[salesAgent] direct-fire wrapper failed (non-fatal): ${e.message}`);
      }
    }

    // ── Audit ──
    await writeAudit({
      threadId, draftId,
      eventType: "sales_agent_turn",
      payload: {
        threadStatus,
        confidence   : aiConfidence,
        toolCalls    : (loopResult.toolCalls || []).map(t => ({
                          name: t.name,
                          durationMs: t.durationMs,
                          error: t.error || null
                       })),
        quotedTotal  : quotedTotal || null,
        readyForHumanApproval: !!parsed.ready_for_human_approval,
        customerAccepted: !!parsed.customer_accepted,
        rawAdvance,
        referenceAttachmentCount: referenceAttachments.length,
        // v4.3.12 — line-sheet attach diagnostics. null when the agent
        // didn't try to attach; populated when it did so the operator
        // can audit the decision and see whether it actually went out.
        lineSheetAttach: lineSheetAttachInfo,
        // v0.9.22 — multi-kind collateral attach diagnostics. One entry
        // per kind the agent requested. Empty array when no kinds were
        // requested.
        collateralAttach: collateralAttachInfo,
        usage        : loopResult.usage || null
      }
    });

    return {
      statusCode: 200,
      headers: { ...CORS, "Content-Type": "application/json" },
      body: JSON.stringify({
        success                    : true,
        threadStatus,
        draftId,
        confidence                 : aiConfidence,
        ready_for_human_approval   : !!parsed.ready_for_human_approval,
        draft_custom_order_listing : parsed.draft_custom_order_listing || null,
        quoteValidation            : quoteValidation || null,
        toolCalls                  : (loopResult.toolCalls || []).map(t => ({ name: t.name, error: t.error || null })),
        durationMs                 : Date.now() - tStart
      })
    };

  } catch (err) {
    console.error("salesAgent unhandled error:", err);
    await writeAudit({
      threadId, eventType: "sales_agent_unhandled_error",
      payload: { error: err.message, stack: err.stack ? err.stack.slice(0, 1000) : null },
      outcome: "failure"
    });
    return { statusCode: 500, headers: CORS,
             body: JSON.stringify({ error: err.message || String(err) }) };
  }
};
