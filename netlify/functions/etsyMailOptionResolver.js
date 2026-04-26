/*  netlify/functions/etsyMailOptionResolver.js
 *
 *  v2.2 — Option Sheet Resolver with rush production + shipping summary.
 *
 *  ═══ WHAT THIS DOES ═══════════════════════════════════════════════════
 *
 *  Customer says: "I want 1F + 2B + 3A, qty 5, can you rush it?"
 *  Resolver does: validates each code is real, looks up prices, applies
 *                 stud-set math (single = 60%, mismatched = +$5), sums
 *                 per-piece subtotal, multiplies by quantity, applies the
 *                 bulk-tier discount, optionally adds rush production
 *                 fee ($15 flat per order), returns a fully itemized
 *                 result PLUS a shipping summary the AI can mention.
 *
 *  ═══ FOUR OPS ═══════════════════════════════════════════════════════════
 *
 *  POST { op: "resolveQuote", family, selectedCodes, quantity, wantsRush?,
 *         includeShippingSummary? }
 *      AI tool path. Returns:
 *        { success:true, family, lineItems, perPieceSubtotal, quantity,
 *          subtotal, bulkTier, discountAmount, rush?, total, currency,
 *          escalations, shippingSummary? }
 *
 *      `rush` populated only when wantsRush:true AND the family permits.
 *      Hard-escalates if wantsRush:true with any Quote-row code present
 *      (per Custom Brites policy — operator must confirm rush + custom
 *      pricing together).
 *
 *      `shippingSummary` populated only when includeShippingSummary:true.
 *      Read-only summary derived from EtsyMail_ShippingUpgradesCache;
 *      gives the AI a price range and fastest-days text it can mention
 *      verbatim. Never binds to a specific shipping cost — Etsy checkout
 *      shows that to the customer.
 *
 *  POST { op: "getSheet", family }
 *      Returns the full option sheet for one family. UI reads this.
 *
 *  POST { op: "listFamilies" }
 *      Returns all available families.
 *
 *  POST { op: "validateCode", family, code }
 *      Single-code lookup. Lightweight check.
 *
 *  POST { op: "putSheet", family, sheet }  — owner-only, used by import
 *
 *  ═══ EXPORTED HELPER ══════════════════════════════════════════════════
 *
 *    module.exports.resolveQuote({ family, selectedCodes, quantity,
 *                                  wantsRush, includeShippingSummary })
 *      Direct-import path for etsyMailSalesAgent. Same pattern as
 *      Step 1's searchListings.
 *
 *  ═══ FIRESTORE SHAPE ══════════════════════════════════════════════════
 *
 *    EtsyMail_OptionSheets/{family}            ← option sheets
 *    EtsyMail_ShippingUpgradesCache/current    ← shipping cache (Step 2.2)
 *
 *  ═══ ENV VARS ══════════════════════════════════════════════════════════
 *
 *    ETSYMAIL_EXTENSION_SECRET     gates this endpoint
 */

const admin = require("./firebaseAdmin");
const { CORS, requireExtensionAuth } = require("./_etsyMailAuth");
const { requireOwner, logUnauthorized } = require("./_etsyMailRoles");

// v2.2 — direct-import shipping summary helper. Try-around so the
// resolver still works if etsyMailShippingSync hasn't been deployed yet
// (graceful degradation, same pattern as the collateral guard).
let getShippingUpgradesCache = null;
let summarizeShippingForAi   = null;
try {
  const shipMod = require("./etsyMailShippingSync");
  getShippingUpgradesCache = shipMod.getShippingUpgradesCache;
  summarizeShippingForAi   = shipMod.summarizeShippingForAi;
} catch (e) {
  console.warn("optionResolver: etsyMailShippingSync not loadable — shippingSummary will be unavailable.", e.message);
}

const db = admin.firestore();
const FV = admin.firestore.FieldValue;

const SHEETS_COLL = "EtsyMail_OptionSheets";
const AUDIT_COLL  = "EtsyMail_Audit";

// In-memory cache for option sheets. Sheets change rarely; cache invalidates
// on owner-only writes (the writes call invalidateSheetCache below).
const SHEET_CACHE_MS = 60 * 1000;
const _sheetCache = new Map();   // family → { value, fetchedAt }

// ─── Helpers ────────────────────────────────────────────────────────────

function json(statusCode, body) {
  return { statusCode, headers: { ...CORS, "Content-Type": "application/json" }, body: JSON.stringify(body) };
}
function bad(msg, code = 400) { return json(code, { error: msg }); }
function ok(body)             { return json(200, { ...body }); }

async function writeAudit({ threadId = null, draftId = null, eventType,
                            actor = "system:optionResolver", payload = {},
                            outcome = "success", ruleViolations = [] }) {
  try {
    await db.collection(AUDIT_COLL).add({
      threadId, draftId, eventType, actor, payload,
      createdAt: FV.serverTimestamp(),
      outcome, ruleViolations
    });
  } catch (e) {
    console.warn("optionResolver audit write failed:", e.message);
  }
}

function r2(n) { return Math.round(n * 100) / 100; }

function invalidateSheetCache(family) {
  if (family) _sheetCache.delete(family);
  else _sheetCache.clear();
}

// ─── Sheet load ────────────────────────────────────────────────────────

async function loadSheet(family) {
  if (!family || typeof family !== "string") return null;
  const cached = _sheetCache.get(family);
  if (cached && (Date.now() - cached.fetchedAt < SHEET_CACHE_MS)) return cached.value;
  try {
    const doc = await db.collection(SHEETS_COLL).doc(family).get();
    if (!doc.exists) {
      _sheetCache.set(family, { value: null, fetchedAt: Date.now() });
      return null;
    }
    const sheet = doc.data();
    _sheetCache.set(family, { value: sheet, fetchedAt: Date.now() });
    return sheet;
  } catch (e) {
    console.warn(`loadSheet(${family}) failed:`, e.message);
    return null;
  }
}

// Index a sheet's options by code for O(1) lookup. Stud section 2 (Choose
// Your Set) has codes that are MODIFIERS, not standalone options — they're
// indexed too but flagged.
function indexSheetCodes(sheet) {
  const idx = new Map();
  if (!sheet || !Array.isArray(sheet.sections)) return idx;
  for (const section of sheet.sections) {
    if (!Array.isArray(section.options)) continue;
    for (const option of section.options) {
      if (!option.code) continue;
      idx.set(String(option.code).toUpperCase(), {
        sectionId  : section.sectionId,
        sectionName: section.name,
        required   : section.required === true,
        isAutomatic: section.isAutomatic === true,
        option
      });
    }
  }
  return idx;
}

// Pick the bulk tier matching `quantity`. Each sheet has exactly one
// bulkSavings section (auto-applied).
function pickBulkTier(sheet, quantity) {
  for (const section of (sheet.sections || [])) {
    if (!Array.isArray(section.bulkSavings)) continue;
    for (const tier of section.bulkSavings) {
      const minOk = quantity >= tier.minQty;
      const maxOk = tier.maxQty === null || tier.maxQty === undefined || quantity <= tier.maxQty;
      if (minOk && maxOk) return tier;
    }
  }
  return null;
}

// ─── The resolver ──────────────────────────────────────────────────────

/** Resolve a list of selected codes into a fully itemized quote.
 *
 *  Inputs:
 *    family                  — "huggie" | "necklace" | "stud"
 *    selectedCodes           — array of code strings, e.g. ["1F", "2B", "3A"]
 *    quantity                — integer ≥ 1
 *    wantsRush               — boolean (optional, default false). If true,
 *                              add the family's rush production fee to the
 *                              total. Hard-fails if the rush policy
 *                              forbids it (qty over cap, or any Quote-row
 *                              code present per hardEscalateWithQuoteRow).
 *    includeShippingSummary  — boolean (optional, default false). If true,
 *                              attach a shippingSummary object derived
 *                              from EtsyMail_ShippingUpgradesCache. Does
 *                              not affect the math; it's a read-only
 *                              summary the AI can mention to the customer.
 *
 *  Returns:
 *    { success: true,  ... }           on full success
 *    { success: false, reason, ... }   on hard failure
 *
 *  Soft-escalation: if any code is a priceQuote row, the resolver
 *  STILL completes the partial quote (sums what it can) and returns
 *  with `escalations[]` populated. The agent then composes a Needs
 *  Review handoff per the synopsis spec.
 *
 *  Hard failure cases:
 *    UNKNOWN_FAMILY, UNKNOWN_CODE, INVALID_QUANTITY, NOT_AVAILABLE,
 *    REQUIRED_SECTION_MISSING, DEPENDENT_SECTION_MISSING_PARENT,
 *    NO_BULK_TIER_FOR_QUANTITY, RUSH_NOT_AVAILABLE,
 *    RUSH_QTY_OVER_CAP, RUSH_BLOCKED_BY_QUOTE_ROW
 */
async function resolveQuote({ family, selectedCodes, quantity,
                              wantsRush = false,
                              includeShippingSummary = false }) {
  if (!family || typeof family !== "string") {
    return { success: false, reason: "UNKNOWN_FAMILY" };
  }
  if (!Array.isArray(selectedCodes) || selectedCodes.length === 0) {
    return { success: false, reason: "NO_CODES_SELECTED" };
  }
  const qty = parseInt(quantity, 10);
  if (!Number.isFinite(qty) || qty < 1) {
    return { success: false, reason: "INVALID_QUANTITY", quantity };
  }

  const sheet = await loadSheet(family);
  if (!sheet || sheet.active === false) {
    return { success: false, reason: "UNKNOWN_FAMILY", family };
  }

  const codeIndex = indexSheetCodes(sheet);
  const lineItems = [];
  const escalations = [];
  const notAvailable = [];
  const unknownCodes = [];

  // Track which sections have been "covered" so we can validate required
  // sections + dependencies AFTER the loop.
  const coveredSections = new Set();
  // Stud section 2 modifiers are tracked separately (they don't add a
  // line item; they transform section 1's price).
  const studSetModifier = { type: null, pct: null, amountUsd: 0 };

  for (const rawCode of selectedCodes) {
    const code = String(rawCode || "").toUpperCase().trim();
    if (!code) continue;
    const entry = codeIndex.get(code);
    if (!entry) {
      unknownCodes.push(code);
      continue;
    }
    coveredSections.add(entry.sectionId);
    const opt = entry.option;

    // ─── Not-available code: hard fail. Caller must re-prompt. ───────
    if (opt.priceNotAvailable === true) {
      notAvailable.push({
        code,
        section: entry.sectionName,
        message: opt.notAvailableMessage ||
                 `The selection ${code} is not available. Please choose an alternative.`
      });
      continue;
    }

    // ─── Stud section 2 (Choose Your Set) — modifier, not a line item ─
    // Family-specific path: only stud has this.
    if (family === "stud" && entry.sectionId === 2 && opt.modifier) {
      const m = opt.modifier;
      if (m.type === "asIs") {
        studSetModifier.type = "asIs";
      } else if (m.type === "percentOfPair") {
        studSetModifier.type = "percentOfPair";
        studSetModifier.pct = m.pct;
      } else if (m.type === "addToTotal") {
        studSetModifier.type = "addToTotal";
        studSetModifier.amountUsd = m.amountUsd;
      }
      lineItems.push({
        code,
        sectionId: entry.sectionId,
        sectionName: entry.sectionName,
        label: opt.label,
        priceUsd: 0,                            // modifier, not standalone
        modifierApplied: m,
        explainer: opt.explainer || null,
        isModifier: true
      });
      continue;
    }

    // ─── Quote row: soft-escalation. Record it but don't hard-fail. ──
    if (opt.priceQuote === true) {
      escalations.push({
        code,
        section: entry.sectionName,
        sectionId: entry.sectionId,
        reason: "PRICE_QUOTE_REQUIRED",
        details: opt
      });
      lineItems.push({
        code,
        sectionId: entry.sectionId,
        sectionName: entry.sectionName,
        label: optLabelFor(opt),
        priceUsd: null,                         // unknown — quote required
        priceQuote: true,
        priceRange: opt.priceRange || null,
        explainer: opt.explainer || null
      });
      continue;
    }

    // ─── Normal priced option ────────────────────────────────────────
    lineItems.push({
      code,
      sectionId: entry.sectionId,
      sectionName: entry.sectionName,
      label: optLabelFor(opt),
      priceUsd: opt.priceUsd,
      explainer: opt.explainer || null
    });
  }

  // Hard fails BEFORE summing
  if (unknownCodes.length) {
    return {
      success: false,
      reason: "UNKNOWN_CODE",
      unknownCodes,
      hint: `Selected code(s) ${unknownCodes.join(", ")} are not in the ${family} option sheet. Re-prompt the customer with the valid codes for that section.`
    };
  }
  if (notAvailable.length) {
    return {
      success: false,
      reason: "NOT_AVAILABLE",
      notAvailable,
      hint: "Re-prompt the customer with the suggested alternatives in notAvailableMessage."
    };
  }

  // Required-section check
  const missingRequired = [];
  for (const section of (sheet.sections || [])) {
    if (section.required !== true) continue;
    if (section.isAutomatic === true) continue;   // bulkSavings auto-picks
    if (!coveredSections.has(section.sectionId)) {
      missingRequired.push({ sectionId: section.sectionId, name: section.name });
    }
  }
  if (missingRequired.length) {
    return {
      success: false,
      reason: "REQUIRED_SECTION_MISSING",
      missingRequired,
      hint: "Ask the customer to choose an option from each required section."
    };
  }

  // Dependent-section check
  for (const section of (sheet.sections || [])) {
    if (!section.dependencies || !section.dependencies.requires) continue;
    const requires = String(section.dependencies.requires);
    // Format: "section3:any" → requires section 3 to be covered
    const m = /^section(\d+):any$/.exec(requires);
    if (m) {
      const parentId = parseInt(m[1], 10);
      const parentCovered = coveredSections.has(parentId);
      const childCovered  = coveredSections.has(section.sectionId);
      if (childCovered && !parentCovered) {
        return {
          success: false,
          reason: "DEPENDENT_SECTION_MISSING_PARENT",
          dependentSection: { sectionId: section.sectionId, name: section.name },
          parentSection: { sectionId: parentId },
          hint: `Section ${section.sectionId} (${section.name}) requires a selection in section ${parentId} first.`
        };
      }
      // If parent isn't covered AND child isn't either, that's fine —
      // optional dependent skipped naturally.
    }
  }

  // ─── Per-piece subtotal calc ─────────────────────────────────────────
  // Sum priced (non-modifier, non-quote) line items.
  let perPiecePriced = 0;
  for (const li of lineItems) {
    if (li.priceQuote) continue;       // unknown — not summed
    if (li.isModifier) continue;       // stud-set modifier, applied next
    perPiecePriced += li.priceUsd || 0;
  }
  perPiecePriced = r2(perPiecePriced);

  // Apply stud-set modifier (only for stud family).
  let perPieceAfterModifier = perPiecePriced;
  let modifierExplainer = null;
  if (family === "stud") {
    if (studSetModifier.type === "percentOfPair") {
      // 60% of pair price applies to the WHOLE per-piece total
      // (charm price + any other priced items).
      perPieceAfterModifier = r2(perPiecePriced * (studSetModifier.pct / 100));
      modifierExplainer = `Single Stud: ${studSetModifier.pct}% of pair price`;
    } else if (studSetModifier.type === "addToTotal") {
      perPieceAfterModifier = r2(perPiecePriced + studSetModifier.amountUsd);
      modifierExplainer = `Mismatched Pair: pair price + $${studSetModifier.amountUsd}`;
    } else {
      modifierExplainer = "Pair (default)";
    }
  }

  // ─── Quantity and bulk discount ──────────────────────────────────────
  const subtotal = r2(perPieceAfterModifier * qty);
  const tier = pickBulkTier(sheet, qty);
  if (!tier) {
    return {
      success: false,
      reason: "NO_BULK_TIER_FOR_QUANTITY",
      quantity: qty,
      hint: "Sheet's bulk-tier ranges don't cover this quantity. Verify bulkSavings ranges in the option sheet."
    };
  }
  const discountAmount = r2(subtotal * (tier.discountPct / 100));
  const subtotalAfterDiscount = r2(subtotal - discountAmount);

  // ─── Rush production (v2.2) ──────────────────────────────────────────
  // Per-order flat fee, regardless of quantity. Capped by qty per family
  // policy. Hard-escalates with Quote-row codes per Custom Brites rule:
  // an operator must confirm rush + custom pricing together — the AI
  // can't promise both autonomously.
  let rush = null;
  if (wantsRush === true) {
    const policy = sheet.rushProduction || null;
    if (!policy || policy.available !== true) {
      return {
        success: false,
        reason : "RUSH_NOT_AVAILABLE",
        family,
        hint   : "This product family does not currently offer rush production."
      };
    }
    if (typeof policy.qtyMaxForRush === "number" && qty > policy.qtyMaxForRush) {
      return {
        success: false,
        reason : "RUSH_QTY_OVER_CAP",
        family,
        quantity: qty,
        rushQtyMax: policy.qtyMaxForRush,
        hint: `Rush production is only available for orders of ${policy.qtyMaxForRush} pieces or fewer. This order is ${qty}; either reduce the quantity or accept standard production.`
      };
    }
    if (policy.hardEscalateWithQuoteRow === true && escalations.length > 0) {
      return {
        success: false,
        reason : "RUSH_BLOCKED_BY_QUOTE_ROW",
        family,
        escalations,
        hint   : "Rush production combined with a custom-quoted item requires operator approval. Hard-escalate to Needs Review with a synopsis."
      };
    }
    rush = {
      requested: true,
      feeUsd                    : policy.feeUsd,
      feeStructure              : policy.feeStructure || "per_order",
      productionDaysStandardMin : policy.productionDaysStandardMin || null,
      productionDaysStandardMax : policy.productionDaysStandardMax || null,
      productionDaysRushMin     : policy.productionDaysRushMin     || null,
      productionDaysRushMax     : policy.productionDaysRushMax     || null,
      customerFacingDescription : policy.customerFacingDescription || null
    };
  }

  // Final total: subtotal after bulk discount + rush fee (if any)
  const total = r2(subtotalAfterDiscount + (rush ? rush.feeUsd : 0));

  // ─── Shipping summary (v2.2) ─────────────────────────────────────────
  // Read-only summary pulled from EtsyMail_ShippingUpgradesCache. Does
  // not affect the math; the AI uses it to mention shipping options
  // verbatim ("expedited shipping is available at checkout, typically
  // $X.XX-$Y.YY"). Customer picks the actual upgrade at Etsy checkout.
  let shippingSummary = null;
  if (includeShippingSummary === true && getShippingUpgradesCache && summarizeShippingForAi) {
    try {
      const cache = await getShippingUpgradesCache();
      shippingSummary = summarizeShippingForAi(cache);
    } catch (e) {
      // Don't fail the whole quote because shipping cache had a hiccup.
      console.warn("resolveQuote: shippingSummary unavailable:", e.message);
      shippingSummary = { available: false, reason: "CACHE_READ_FAILED" };
    }
  }

  return {
    success: true,
    family,
    familyDisplayName: sheet.displayName || family,
    unitOfMeasure: sheet.unitOfMeasure || null,
    lineItems,
    perPiecePriced,
    perPieceAfterModifier,
    modifierExplainer,
    quantity: qty,
    subtotal,
    bulkTier: { code: tier.code, label: tier.label, discountPct: tier.discountPct },
    discountAmount,
    subtotalAfterDiscount,
    rush,                                          // null or { feeUsd, feeStructure, ... }
    total,                                         // = subtotalAfterDiscount + (rush ? rush.feeUsd : 0)
    currency: "USD",
    escalations,                                   // non-empty = soft-escalation
    requiresNeedsReviewHandoff: escalations.length > 0,
    shippingSummary                                // null or { rangeText, anyUpgrades, fastestDaysText, available }
  };
}

// Best-effort label for a line item — different sections have different
// shape (charm size+metal, hoop+metal, chain+metal, simple label).
function optLabelFor(opt) {
  if (opt.label) return opt.label;
  if (opt.size && opt.metal)        return `${opt.size} ${opt.metal}`;
  if (opt.hoopSize && opt.metal)    return `${opt.hoopSize} hoop ${opt.metal}`;
  if (opt.chainStyle && opt.metal)  return `${opt.chainStyle} ${opt.metal}`;
  if (opt.length)                   return `${opt.length}`;
  if (opt.chainStyle)               return opt.chainStyle;
  return opt.code || "(unlabeled)";
}

// ─── Handler ───────────────────────────────────────────────────────────

exports.handler = async (event) => {
  if (event.httpMethod === "OPTIONS") {
    return { statusCode: 200, headers: CORS, body: "ok" };
  }
  if (event.httpMethod !== "POST") {
    return json(405, { error: "Method Not Allowed" });
  }

  const auth = requireExtensionAuth(event);
  if (!auth.ok) return auth.response;

  let body = {};
  try { body = JSON.parse(event.body || "{}"); }
  catch { return bad("Invalid JSON body"); }

  const op = body.op;
  if (!op) return bad("op required");

  try {
    if (op === "resolveQuote") {
      const {
        family, selectedCodes, quantity, threadId,
        wantsRush, includeShippingSummary
      } = body;
      const result = await resolveQuote({
        family, selectedCodes, quantity,
        wantsRush: wantsRush === true,
        includeShippingSummary: includeShippingSummary === true
      });

      // Audit: every quote computation, success or failure. Used for
      // pricing forensics if a customer disputes a number.
      await writeAudit({
        threadId: threadId || null,
        eventType: result.success ? "option_quote_resolved" : "option_quote_failed",
        actor: "system:optionResolver",
        payload: { family, selectedCodes, quantity, wantsRush: wantsRush === true,
                   includeShippingSummary: includeShippingSummary === true, result },
        outcome: result.success ? "success" : "blocked",
        ruleViolations: result.success ? [] : [result.reason || "UNKNOWN"]
      });

      return ok(result);
    }

    if (op === "getSheet") {
      const sheet = await loadSheet(body.family);
      if (!sheet) return json(404, { success: false, reason: "UNKNOWN_FAMILY", family: body.family });
      return ok({ success: true, sheet });
    }

    if (op === "listFamilies") {
      const snap = await db.collection(SHEETS_COLL).limit(50).get();
      const families = [];
      snap.forEach(d => {
        const data = d.data() || {};
        families.push({
          id: d.id,
          family: data.family || d.id,
          displayName: data.displayName || d.id,
          active: data.active !== false,
          sectionCount: Array.isArray(data.sections) ? data.sections.length : 0
        });
      });
      return ok({ success: true, families });
    }

    if (op === "validateCode") {
      const { family, code } = body;
      if (!family || !code) return bad("family and code required");
      const sheet = await loadSheet(family);
      if (!sheet) return ok({ found: false, reason: "UNKNOWN_FAMILY" });
      const idx = indexSheetCodes(sheet);
      const entry = idx.get(String(code).toUpperCase());
      if (!entry) return ok({ found: false, reason: "UNKNOWN_CODE" });
      return ok({
        found: true,
        code: String(code).toUpperCase(),
        sectionId: entry.sectionId,
        sectionName: entry.sectionName,
        option: entry.option
      });
    }

    if (op === "putSheet") {
      // Owner-only: write a complete option sheet doc. Used for bulk
      // import from the seed JSON, or future "edit sheet" UI.
      const ownerCheck = await requireOwner(body.actor);
      if (!ownerCheck.ok) {
        await logUnauthorized({
          actor: body.actor,
          eventType: "option_sheet_put_unauthorized",
          payload: { family: body.family, reason: ownerCheck.reason }
        });
        return json(403, { error: "Owner role required", reason: ownerCheck.reason });
      }
      const { family, sheet } = body;
      if (!family || !sheet) return bad("family and sheet required");
      if (sheet.family && sheet.family !== family) {
        return bad("sheet.family must match top-level family field");
      }
      // Stamp metadata
      const toWrite = {
        ...sheet,
        family,
        lastUpdatedBy: body.actor,
        updatedAt: FV.serverTimestamp()
      };
      await db.collection(SHEETS_COLL).doc(family).set(toWrite, { merge: false });
      invalidateSheetCache(family);
      await writeAudit({
        eventType: "option_sheet_put",
        actor: body.actor,
        payload: { family }
      });
      return ok({ success: true, family });
    }

    /* ─── v2.5: Multi-family upload from seed-file shape ──────────
     * Owner-only. Accepts the same JSON the operator would have
     * edited locally and run through `seeds/import_seeds.js` —
     * the multi-family wrapper:
     *   { _meta?, huggie: {...}, necklace: {...}, stud: {...} }
     *
     * Top-level `_meta` (and any other key whose value is not an
     * object) is ignored. Every other top-level key is treated as a
     * family name and its value as a sheet. Each family is validated
     * BEFORE any write happens — partial imports would leave Firestore
     * in a half-updated state with no way to roll back.
     *
     * Reply shape:
     *   { success, written: ["huggie","necklace","stud"], skipped: ["_meta"] }
     *
     * If any family fails validation, the response is 422 with
     * `{ error, family, reason }` and NOTHING is written. The seed
     * script's import is one-shot; this UI path mirrors that
     * atomicity so an operator never ends up with two families
     * matching the new file and one matching the old. */
    if (op === "putSheets") {
      const ownerCheck = await requireOwner(body.actor);
      if (!ownerCheck.ok) {
        await logUnauthorized({
          actor: body.actor,
          eventType: "option_sheets_put_unauthorized",
          payload: { reason: ownerCheck.reason }
        });
        return json(403, { error: "Owner role required", reason: ownerCheck.reason });
      }
      const { sheets } = body;
      if (!sheets || typeof sheets !== "object" || Array.isArray(sheets)) {
        return bad("sheets must be an object keyed by family name");
      }

      // Phase 1 — partition + validate every family before any write.
      const candidates = []; // [{ family, sheet }]
      const skipped    = []; // top-level keys we deliberately ignore
      for (const [key, val] of Object.entries(sheets)) {
        // Ignore _meta and any non-object top-level entry. The seed
        // file's _meta block carries human-readable notes about the
        // bulk-tier matrix etc.; persisting it as a sheet would create
        // a phantom family the resolver would index against.
        if (key.startsWith("_") || !val || typeof val !== "object" || Array.isArray(val)) {
          skipped.push(key);
          continue;
        }
        if (val.family && val.family !== key) {
          return json(422, {
            error: "Family mismatch",
            family: key,
            reason: `top-level key '${key}' but sheet.family is '${val.family}'`
          });
        }
        if (!Array.isArray(val.sections) || val.sections.length === 0) {
          return json(422, {
            error: "Sheet missing sections",
            family: key,
            reason: "sheet.sections must be a non-empty array"
          });
        }
        candidates.push({ family: key, sheet: val });
      }
      if (candidates.length === 0) {
        return bad("No valid sheets found in payload (every top-level key was skipped)");
      }

      // Phase 2 — write each family, stamp metadata, invalidate cache.
      // We use a batch write so all families land atomically. The
      // seed-script path uses individual `set()` calls because the
      // script can retry per-family; the UI path benefits more from
      // atomicity than retry-ability.
      const batch = db.batch();
      for (const { family, sheet } of candidates) {
        const toWrite = {
          ...sheet,
          family,
          lastUpdatedBy: body.actor,
          updatedAt: FV.serverTimestamp()
        };
        batch.set(db.collection(SHEETS_COLL).doc(family), toWrite, { merge: false });
      }
      await batch.commit();

      // Cache invalidation must run AFTER the commit so a concurrent
      // resolveQuote during the write can't repopulate the cache from
      // the old doc.
      for (const { family } of candidates) invalidateSheetCache(family);

      await writeAudit({
        eventType: "option_sheets_put_bulk",
        actor: body.actor,
        payload: {
          written: candidates.map(c => c.family),
          skipped
        }
      });

      return ok({
        success: true,
        written: candidates.map(c => c.family),
        skipped
      });
    }

    return bad(`Unknown op '${op}'`);

  } catch (err) {
    console.error("optionResolver error:", err);
    return json(500, { error: err.message || String(err), op });
  }
};

// Direct-import path for sibling functions (etsyMailSalesAgent and
// future Step 3's etsyMailCustomOrderDraft). Same pattern as Step 1's
// searchListings + Step 2's computeQuoteBand.
module.exports.resolveQuote        = resolveQuote;
module.exports.loadSheet           = loadSheet;
module.exports.indexSheetCodes     = indexSheetCodes;
module.exports.invalidateSheetCache = invalidateSheetCache;
