/**
 * _etsyMailVetoes.js — deterministic auto-send safety vetoes.
 *
 * Shared by:
 *   - etsyMailAutoPipeline-background.js  (support + sales auto-send decision)
 *   - etsyMailDraftReply.js               (autoSendBlockers returned to the inbox,
 *                                          so the manual "AI Draft" auto-send
 *                                          obeys the same bright lines)
 *
 * Pure functions. No Firestore, no network, no Etsy calls.
 *
 * The first eleven patterns are moved verbatim from
 * etsyMailAutoPipeline-background.js (v1.2). The "…2" patterns and
 * non_english_sensitive are additions: plain phrasings the originals miss
 * ("I want my money returned", "it came in pieces", "the package never
 * showed up", "please stop the order", "send it to my work address").
 * False positives only cost an auto-send (the reply waits for a person).
 */
"use strict";

// ─── v1.2: Deterministic veto rules ─────────────────────────────────────
//
// Self-rated AI confidence is not enough for high-stakes scenarios. Even
// a model that scores its own draft at 0.95 should NOT auto-send if the
// inbound mentions a refund, a chargeback, legal escalation, etc. These
// rules are the bright-line safety net.
//
// Pattern matching is intentionally conservative — false positives push
// to human review (cheap, just wastes one auto-send opportunity); false
// negatives push to auto-send (expensive, can damage customer trust).
// When in doubt, add the pattern.
//
// Patterns are case-insensitive and word-boundary anchored. Tested
// against both the latest inbound text (highest signal) AND the AI's
// outbound draft text (catches drafts that say "I'll process your
// refund" even when the inbound was cagey).
const DETERMINISTIC_VETO_PATTERNS = [
  // ── Money-sensitive ──────────────────────────────────────────────
  { id: "refund",      pattern: /\b(refund|chargeback|dispute|money\s*back|return\s+(this|the|my)\s+(item|order|product)|process\s+(?:a|the|my)\s+refund)\b/i,
    reason: "refund/return language" },
  { id: "cancel",      pattern: /\b(cancel\s+(?:my|the|this)\s+(?:order|purchase)|cancellation\s+(?:request|policy)|cancel\s+(?:and|&)\s+refund)\b/i,
    reason: "cancellation request" },

  // ── Legal / escalation ───────────────────────────────────────────
  { id: "legal",       pattern: /\b(lawsuit|sue\s*you|small\s+claims|legal\s+action|attorney|consult\s+(?:my\s+)?lawyer|file\s+a\s+case)\b/i,
    reason: "legal escalation" },
  { id: "complaint",   pattern: /\b(BBB|Better\s+Business\s+Bureau|file\s+a\s+complaint|complaint\s+with\s+Etsy|report\s+(?:you|this\s+shop|seller))\b/i,
    reason: "formal complaint" },
  { id: "fraud",       pattern: /\b(scammer|scammed|fraudulent|fraud\s+(?:case|alert)|theft|stolen\s+(?:my|the))\b/i,
    reason: "fraud accusation" },

  // ── Order data integrity ─────────────────────────────────────────
  { id: "address",     pattern: /\b(change\s+(?:my|the)\s+(?:shipping\s+)?address|wrong\s+address|different\s+address|update\s+(?:my\s+)?address|ship\s+to\s+(?:a\s+)?different)\b/i,
    reason: "address change" },
  { id: "personalize", pattern: /\b(change\s+(?:the\s+)?(?:name|spelling|engraving|personalization|customization|wording)|wrong\s+name|misspelled|spelled\s+wrong|spell(?:ing|ed)?\s+it\s+wrong)\b/i,
    reason: "personalization correction" },

  // ── Damage / replacement ─────────────────────────────────────────
  { id: "damaged",     pattern: /\b(damaged|broken|defective|cracked|shattered|arrived\s+broken|wrong\s+item\s+received|received\s+the\s+wrong)\b/i,
    reason: "damage/wrong-item claim" },
  { id: "missing",     pattern: /\b(missing\s+(?:item|piece|part)|never\s+(?:received|arrived|came)|hasn't\s+(?:arrived|come)|never\s+got\s+(?:my|it|the))\b/i,
    reason: "non-delivery claim" },
  { id: "replace",     pattern: /\b(send\s+(?:me\s+)?(?:another|a\s+replacement|a\s+new\s+one)|replacement\s+(?:order|piece|item)|reship)\b/i,
    reason: "replacement request" },

  // ── Custom orders / deals ────────────────────────────────────────
  { id: "custom",      pattern: /\b(custom\s+order|customize\b|customise\b|special\s+request|can\s+you\s+make\s+(?:me\s+)?a|bulk\s+order|wholesale\b|discount\s+code|coupon\s+code)\b/i,
    reason: "custom-order or discount inquiry" },

  // ── Additions (audit 2026-09) ────────────────────────────────────
  { id: "refund2",     pattern: /\b(money\s+returned|want\s+(?:my\s+)?money|give\s+(?:me\s+)?(?:my\s+)?money|reimburse(?:d|ment)?|charge\s+back)\b/i,
    reason: "refund/return language" },
  { id: "cancel2",     pattern: /\b(stop\s+(?:the|my|this)\s+order|don['\u2019]?t\s+(?:make|ship|send)\s+(?:it|the\s+order|my\s+order)|cancel\s+(?:it|this|that)|call\s+off\s+(?:the|my)\s+order)\b/i,
    reason: "cancellation request" },
  { id: "damaged2",    pattern: /\b(came\s+in\s+pieces|fell\s+apart|(?:charm|clasp|chain|stone|it)\s+(?:fell\s+off|snapped|broke)|arrived\s+(?:bent|crushed|open|scratched)|wrong\s+(?:item|size|colou?r|charm|necklace|metal|product))\b/i,
    reason: "damage/wrong-item claim" },
  { id: "missing2",    pattern: /\b(never\s+showed\s+up|hasn['\u2019]?t\s+shown\s+up|still\s+(?:hasn['\u2019]?t|haven['\u2019]?t|not)\s+(?:arrived|received|come|gotten|shown\s+up)|didn['\u2019]?t\s+(?:arrive|receive)|not\s+(?:yet\s+)?received|lost\s+in\s+the\s+mail|(?:marked|says|shows)\s+(?:as\s+)?delivered\s+but)\b/i,
    reason: "non-delivery claim" },
  { id: "personalize2", pattern: /\b(wrong\s+(?:initial|letter|name|date|spelling|engraving)|typo\s+(?:in|on)|engrav(?:ed|ing)\s+(?:is|was)\s+wrong|was\s+supposed\s+to\s+(?:say|read|be\s+spelled|be\s+engraved))\b/i,
    reason: "personalization correction" },
  { id: "address2",    pattern: /\b((?:send|ship)\s+(?:it\s+)?to\s+(?:my|a|the)\s+(?:work|office|new|other|parents?|mom|dad)\b|(?:new|updated|correct)\s+(?:shipping\s+)?address|i\s+(?:just\s+)?moved)\b/i,
    reason: "address change" },
  // Money / cancel / damage words in the languages this shop sees most.
  // Letter-class boundaries instead of \b so accented words match.
  { id: "non_english_sensitive", pattern: /(?<![A-Za-z\u00C0-\u00FF])(reembolso|reembolsar|devoluci[o\u00F3]n|cancelar|remboursement|rembourser|annuler|endommag[\u00E9e]e?|r[\u00FCu]ckerstattung|erstattung|stornieren|besch[\u00E4a]digt|kaputt|rimborso|annullare|danneggiat[oa]|devolu[c\u00E7][a\u00E3]o|danificad[oa]|terugbetaling|annuleren|beschadigd)(?![A-Za-z\u00C0-\u00FF])/i,
    reason: "refund/cancel/damage language (non-English)" }
];

/** Run all veto patterns against given text. Returns array of triggered
 *  veto IDs + reasons. Empty array = clean.
 *
 *  excludePatternIds: array of pattern IDs to skip. Used by the sales-
 *  agent auto-send path to skip the "custom" pattern, since sales mode
 *  exists specifically to handle custom-order inquiries — applying that
 *  veto to a sales-agent draft would block 100% of sales auto-sends. */
function runVetoPatterns(text, excludePatternIds = []) {
  if (!text || typeof text !== "string") return [];
  const skipSet = new Set(excludePatternIds);
  const hits = [];
  for (const v of DETERMINISTIC_VETO_PATTERNS) {
    if (skipSet.has(v.id)) continue;
    if (v.pattern.test(text)) hits.push({ id: v.id, reason: v.reason });
  }
  return hits;
}


// Order tools whose returned { error } means the draft was written without
// the data it needed (the loop only records thrown errors in tc.error).
const ORDER_DATA_TOOLS = new Set(["lookup_order_tracking", "lookup_order_details", "generate_tracking_image"]);

/** Apply all deterministic safety checks. Returns { vetoed, reasons }.
 *  Same contract as the pipeline's v1.2 function, plus: an order tool that
 *  RETURNED an error (outputPreview.error / output.error) also counts. */
function applyDeterministicVetoes({ inboundText, draftText, draftToolCalls, excludePatternIds = [] }) {
  const reasons = [];

  const inboundHits = runVetoPatterns(inboundText, excludePatternIds);
  for (const h of inboundHits) reasons.push("inbound_" + h.id + ": " + h.reason);

  const outboundHits = runVetoPatterns(draftText, excludePatternIds);
  for (const h of outboundHits) reasons.push("outbound_" + h.id + ": " + h.reason);

  const returnedError = (tc) => {
    if (!ORDER_DATA_TOOLS.has(tc.name)) return false;
    const out = (tc.outputPreview && typeof tc.outputPreview === "object") ? tc.outputPreview
              : (tc.output && typeof tc.output === "object") ? tc.output : null;
    return !!(out && out.error);
  };
  const toolErrors = (Array.isArray(draftToolCalls) ? draftToolCalls : [])
    .filter(tc => tc && tc.name !== "compose_draft_reply" && (tc.error || returnedError(tc)));
  if (toolErrors.length) {
    reasons.push("tool_call_failed: " + toolErrors.map(tc => tc.name).join(","));
  }

  return { vetoed: reasons.length > 0, reasons };
}

/** Text of every customer message the shop has NOT answered yet: the inbound
 *  messages newer than the latest real outbound, oldest first.
 *
 *  Replaces "latest inbound only" for vetoes. A burst like
 *  "it arrived broken" + "also, love the colour!" used to be judged on the
 *  second message alone. Answered messages are still excluded, so a refund
 *  question the shop already handled in round 1 does not veto round 2
 *  (the v4.3.7 concern).
 *
 *  Optimistic ghosts (localOptimistic) are skipped rather than treated as
 *  an answer: a ghost re-stamped to "now" must not hide the newest
 *  inbound messages.
 *
 *  @param {object[]} messagesNewestFirst  message docs, newest first
 *  @returns {string|null}
 */
function unansweredInboundText(messagesNewestFirst, { maxMessages = 10, maxChars = 4000 } = {}) {
  const texts = [];
  for (const m of (Array.isArray(messagesNewestFirst) ? messagesNewestFirst : [])) {
    if (!m) continue;
    if (m.direction === "outbound") {
      if (m.localOptimistic === true) continue;
      break;
    }
    if (m.direction !== "inbound") continue;
    const t = String(m.text || "").trim();
    if (t) texts.push(t);
    if (texts.length >= maxMessages) break;
  }
  if (!texts.length) return null;
  return texts.reverse().join("\n---\n").slice(-maxChars);
}

module.exports = {
  DETERMINISTIC_VETO_PATTERNS,
  runVetoPatterns,
  applyDeterministicVetoes,
  unansweredInboundText,
};
