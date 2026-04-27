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

const TERMINAL_THREAD_STATUSES = new Set([
  "sales_completed",
  "sales_abandoned",
  "pending_human_review"
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

async function prefetchLineSheetCollateral({ latestInboundText, salesCtx }) {
  // v3.1: line-sheet URL is made available to the agent whenever the
  // family is inferable, regardless of customer phrasing. The DECISION to
  // send is the agent's, based on its read of the conversation. The
  // previous regex-gated prefetch was a rigid pattern-match that only
  // fired on specific phrases like "what options" — meaning the agent
  // had nothing to send for customers who never used those magic words.
  // The agent's prompt now teaches WHEN to send (judgment); this code
  // just guarantees the URL is always there when the agent decides yes.
  const family = inferFamilyFromTextAndContext(latestInboundText, salesCtx);
  if (!family || !searchCollateral) return [];
  try {
    const result = await searchCollateral({ category: family, kind: "line_sheet", limit: 3 });
    const matches = Array.isArray(result && result.matches) ? result.matches : [];
    return matches.filter(m => m && isCustomerVisibleUrl(m.url)).slice(0, 3);
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
    const recommendedCollateral = await prefetchLineSheetCollateral({ latestInboundText, salesCtx });

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
      intentClassification, intentConfidence
    };
    const initialMessages = buildInitialMessages({
      contextSummary, latestInboundText, referenceAttachments
    });

    // ── Run the tool loop ──
    let loopResult;
    try {
      loopResult = await runToolLoop({
        model         : AI_MODEL,
        maxTokens     : AI_MAX_TOKENS,
        system        : promptLoad.prompt,
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
    // the draft body becomes the synopsis (operator-facing, NOT customer-
    // facing). The operator at Custom Brites sees the synopsis in the
    // Needs Review folder and can either:
    //   (a) edit + send a customer-facing reply themselves, or
    //   (b) provide the missing quote and let the agent resume.
    //
    // The customer-facing `reply` from the AI (e.g. "I'm checking with
    // the team and I'll get back to you shortly") is preserved on the
    // draft as `customerFacingReplyDraft` so the operator can use it as
    // a starting point.
    //
    // We detect Needs Review handoff via either:
    //   1. parsed.needs_review_synopsis is a non-empty string, OR
    //   2. parsed.advance_stage === "human_review" (legacy path)
    const isNeedsReviewHandoff =
      (typeof parsed.needs_review_synopsis === "string" && parsed.needs_review_synopsis.trim().length > 50)
      || parsed.advance_stage === "human_review";

    const customerFacingReply = (typeof parsed.reply === "string" && parsed.reply.trim())
      ? parsed.reply.trim()
      : "(The AI did not produce a reply for this turn — operator review needed.)";

    // Draft body: synopsis if handoff, else customer-facing reply.
    const replyText = isNeedsReviewHandoff && typeof parsed.needs_review_synopsis === "string" && parsed.needs_review_synopsis.trim().length > 50
      ? parsed.needs_review_synopsis.trim()
      : customerFacingReply;

    const aiConfidence = (typeof parsed.confidence === "number" && parsed.confidence >= 0 && parsed.confidence <= 1)
      ? parsed.confidence : 0.5;

    await db.collection(DRAFTS_COLL).doc(draftId).set({
      draftId,
      threadId,
      text                  : replyText,
      attachments           : [],
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
      customerAccepted      : !!parsed.customer_accepted,
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
      aiDraftStatus : "ready",
      latestDraftId : draftId,
      aiConfidence,
      readyForHumanApproval: !!parsed.ready_for_human_approval,
      // v4.1 — customer_accepted signal for downstream listing-creator
      // automation. Mirrored on both draft and thread so a worker can
      // query either collection. quotedTotal mirrored for the same
      // reason — the automation needs the price without re-reading the
      // draft.
      customerAccepted     : !!parsed.customer_accepted,
      acceptedQuoteUsd     : (!!parsed.customer_accepted && typeof quotedTotal === "number") ? quotedTotal : null,
      lastResolverResult   : (salesCtx._lastResolverResult && salesCtx._lastResolverResult.success)
                              ? salesCtx._lastResolverResult : null,
      updatedAt     : FV.serverTimestamp()
    }, { merge: true });

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
