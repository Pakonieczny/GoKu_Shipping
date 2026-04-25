/*  netlify/functions/etsyMailAutoPipeline-background.js
 *
 *  v1.0 — Auto-reply pipeline orchestrator (BACKGROUND function).
 *
 *  ═══ WHY -BACKGROUND ════════════════════════════════════════════════════
 *
 *  Netlify's `-background` suffix unlocks a 15-minute timeout (vs 10s for
 *  regular synchronous functions) and decouples invocation from response.
 *  This pipeline calls etsyMailDraftReply (Opus 4.7 + tool loop, 10-60s
 *  typical) and may also call etsyMailDraftSend.enqueue, so the standard
 *  10s budget is too tight. The trade-off: callers get a 202 immediately
 *  with no body, so any return data we'd want must be persisted (we
 *  write everything to Firestore and the audit trail).
 *
 *  ═══ PURPOSE ════════════════════════════════════════════════════════════
 *
 *  When a new inbound message lands in a thread, this function:
 *
 *    1. Generates an AI draft via etsyMailDraftReply (mode="initial").
 *       The compose_draft_reply tool now self-rates `confidence` and
 *       `difficulty` on every draft.
 *
 *    2. Inspects the rating against the configured confidence threshold:
 *
 *       confidence ≥ threshold  →  AUTO_SEND
 *           - Enqueue the draft via etsyMailDraftSend (op="enqueue")
 *           - Set thread status = "auto_replied"
 *           - The Chrome extension picks up the queued draft on its
 *             next peek and sends it via Etsy's compose flow (same path
 *             as a manual "Send via Etsy" click).
 *
 *       confidence < threshold  →  HUMAN_REVIEW
 *           - Leave the draft in EtsyMail_Drafts/{draftId} (status=draft)
 *           - Set thread status = "pending_human_review"
 *           - Operator finds it in the Needs Review folder with the AI's
 *             rating + reasoning visible on open.
 *
 *  Idempotency: each thread tracks lastAutoProcessedInboundAt (millis).
 *  If lastInboundAt > lastAutoProcessedInboundAt, the pipeline runs once
 *  for that inbound. We do NOT auto-reply to outbound or scraper-only
 *  updates. We do NOT auto-reply if the kill-switch is on. We do NOT
 *  auto-reply if a draft for this thread is already in-flight (queued
 *  or sending).
 *
 *  ═══ INVOCATION ═════════════════════════════════════════════════════════
 *
 *  Two invocation paths:
 *
 *  (a) Background trigger — fire-and-forget POST from etsyMailSnapshot.js
 *      right after a new inbound message lands. Netlify returns 202 to
 *      the caller immediately and runs this function asynchronously up
 *      to 15 minutes.
 *
 *  (b) Direct (operator) — POST to this endpoint with { threadId } from
 *      the inbox to manually re-run the pipeline (backfills, overrides).
 *      Same 202-and-go semantics; check the thread doc for the result.
 *
 *  ═══ REQUEST ════════════════════════════════════════════════════════════
 *
 *  POST body:
 *    {
 *      threadId         : "etsy_conv_1651714855",   // required
 *      confidenceThreshold: 0.80,                   // optional override
 *      employeeName     : "system:auto-pipeline",   // optional signature
 *      forceRerun       : false,                    // bypass idempotency
 *      dryRun           : false                     // generate but don't act
 *    }
 *
 *  ═══ RESPONSE ═══════════════════════════════════════════════════════════
 *
 *  Netlify always returns 202 (Accepted) immediately for -background
 *  functions, regardless of what we put in the response. We still write
 *  a structured response body in case Netlify changes that semantics or
 *  for local testing where this can be invoked synchronously.
 *
 *  Real "result" lives in Firestore:
 *    - thread.status: "auto_replied" or "pending_human_review"
 *    - thread.lastAutoDecision: "auto_send" | "human_review" | ...
 *    - thread.aiConfidence / thread.aiDifficulty: the rating
 *    - draft doc updated with text + rating
 *    - audit doc with eventType "auto_pipeline_*"
 *
 *  ═══ ENV VARS ═══════════════════════════════════════════════════════════
 *
 *  ETSYMAIL_EXTENSION_SECRET       gates direct-invocation auth
 *  URL / DEPLOY_URL                Netlify-provided base for inter-fn calls
 *
 *  All operational config (enabled flag, confidence threshold) lives in
 *  Firestore at EtsyMail_Config/autoPipeline so operators can tune it
 *  from the inbox UI without redeploying. See getAutoPipelineConfig().
 */

const admin = require("./firebaseAdmin");
const { CORS, requireExtensionAuth } = require("./_etsyMailAuth");

const db = admin.firestore();
const FV = admin.firestore.FieldValue;

const THREADS_COLL = "EtsyMail_Threads";
const DRAFTS_COLL  = "EtsyMail_Drafts";
const AUDIT_COLL   = "EtsyMail_Audit";
const CONFIG_COLL  = "EtsyMail_Config";

// ─── Hard-coded fallback defaults ─────────────────────────────────────
// Used only when EtsyMail_Config/autoPipeline doesn't exist yet (first
// deploy). Once the doc is written, these are ignored — the doc is the
// single source of truth and the inbox UI is the only knob that matters.
const FALLBACK_THRESHOLD = 0.80;
const FALLBACK_ENABLED   = true;

// Don't auto-reply to messages older than this when re-processing.
// Protects against accidental backfills auto-sending to old threads.
const MAX_INBOUND_AGE_MS = 7 * 24 * 60 * 60 * 1000;   // 7 days

// ─── Auto-pipeline config — Firestore-backed, cached 15s ──────────────
// Same pattern as the kill-switch in etsyMailDraftSend.js. The cache
// lives in module scope so warm-container invocations share it; cold
// starts re-fetch (acceptable, ~1 round-trip).
let _autoCfgCache = { value: null, fetchedAt: 0 };
const AUTO_CFG_CACHE_MS = 15 * 1000;

async function getAutoPipelineConfig() {
  if (_autoCfgCache.value && (Date.now() - _autoCfgCache.fetchedAt < AUTO_CFG_CACHE_MS)) {
    return _autoCfgCache.value;
  }
  let value = { enabled: FALLBACK_ENABLED, threshold: FALLBACK_THRESHOLD, source: "fallback" };
  try {
    const doc = await db.collection(CONFIG_COLL).doc("autoPipeline").get();
    if (doc.exists) {
      const d = doc.data() || {};
      const t = typeof d.threshold === "number" ? d.threshold : FALLBACK_THRESHOLD;
      value = {
        enabled  : d.enabled !== false,             // default true if doc exists but unset
        threshold: Math.max(0, Math.min(1, t)),
        updatedBy: d.updatedBy || null,
        updatedAt: d.updatedAt && d.updatedAt.toMillis ? d.updatedAt.toMillis() : null,
        source   : "firestore"
      };
    }
    _autoCfgCache = { value, fetchedAt: Date.now() };
  } catch (e) {
    console.warn("autoPipeline: config fetch failed:", e.message);
  }
  return value;
}

function json(statusCode, body) {
  return { statusCode, headers: { ...CORS, "Content-Type": "application/json" }, body: JSON.stringify(body) };
}
function bad(msg, code = 400) { return json(code, { error: msg }); }
function ok(body)             { return json(200, { success: true, ...body }); }

async function writeAudit({ threadId, draftId = null, eventType, actor = "system:autoPipeline", payload = {} }) {
  await db.collection(AUDIT_COLL).add({
    threadId, draftId, eventType, actor, payload,
    createdAt: FV.serverTimestamp()
  });
}

/** Read the kill-switch from EtsyMail_Config/global.sendDisabled.
 *
 *  v1.5: this used to read EtsyMail_Config/killSwitch.disabled, which
 *  was a SEPARATE doc from the one etsyMailDraftSend reads/writes
 *  (EtsyMail_Config/global.sendDisabled). The split caused the auto-
 *  pipeline to decide "kill-switch off, proceed" while the sender
 *  refused with SEND_DISABLED — leaving threads in queued_for_auto_send
 *  with confusing audit reasons like "enqueue failed: SEND_DISABLED".
 *
 *  Now both readers share one source of truth. The kill_switch_set op
 *  in etsyMailDraftSend writes to /global, and the inbox UI's killswitch
 *  banner polls the same doc.
 *
 *  If on, we MUST skip the auto-send branch (still safe to draft + route
 *  to review). */
async function getKillSwitch() {
  try {
    const doc = await db.collection(CONFIG_COLL).doc("global").get();
    if (!doc.exists) return { disabled: false };
    const d = doc.data() || {};
    return {
      disabled: !!d.sendDisabled,
      reason  : d.sendDisabledReason || null,
      by      : d.sendDisabledBy     || null,
      at      : d.sendDisabledAt && d.sendDisabledAt.toMillis ? d.sendDisabledAt.toMillis() : null
    };
  } catch (e) {
    console.warn("autoPipeline: killSwitch fetch failed:", e.message);
    return { disabled: false };
  }
}

/** Resolve the base URL for inter-function calls. Netlify provides
 *  `URL` in the env at runtime; locally we default to localhost. */
function functionsBase() {
  return process.env.URL
      || process.env.DEPLOY_URL
      || process.env.NETLIFY_BASE_URL
      || "http://localhost:8888";
}

/** POST to a sibling Netlify function. Forwards the extension secret so
 *  endpoints that require it (etsyMailDraftReply, etsyMailDraftSend in
 *  some ops) accept the call. */
async function callFunction(name, body) {
  const url = `${functionsBase()}/.netlify/functions/${name}`;
  const headers = { "Content-Type": "application/json" };
  if (process.env.ETSYMAIL_EXTENSION_SECRET) {
    headers["X-EtsyMail-Secret"] = process.env.ETSYMAIL_EXTENSION_SECRET;
  }
  const res = await fetch(url, {
    method: "POST",
    headers,
    body: JSON.stringify(body || {})
  });
  const text = await res.text();
  let data = {};
  try { data = JSON.parse(text); } catch { data = { raw: text }; }
  if (!res.ok) {
    const err = new Error(data.error || `${name} ${res.status}`);
    err.status = res.status;
    err.data = data;
    throw err;
  }
  return data;
}

/** Inspect whether the most recent message in a thread is an inbound
 *  message that we haven't yet auto-processed. Returns one of:
 *    { ok: true, inboundMs, ageMs }
 *    { ok: false, reason: "..." }
 *
 *  Pure read-only — used by `claimThread` inside a transaction and by
 *  callers that want to peek without claiming.
 */
function evaluateEligibility(thread, options = {}) {
  if (!thread) return { ok: false, reason: "thread not found" };

  const status = thread.status || "";

  // ── v1.2: archived threads CAN be auto-processed ─────────────────
  // If a customer replies to an archived conversation, the new inbound
  // is the customer re-engaging — exactly when fresh AI handling helps.
  // We removed the archived-bail and instead include "archived" in
  // HIDDEN_INTAKE_STATUSES below, which causes the claim transaction to
  // surface the thread back to pending_human_review during processing.
  // The pipeline then runs normally; if AI confidence is high, the
  // thread goes to Auto-Reply. Operators can re-archive afterward.

  // The "sent" status means the operator manually sent a reply. If the
  // customer follows up, we want the AI to take a fresh look — but the
  // status will have been bumped by the next inbound scrape, so this
  // gate only fires when sent is truly the latest event.
  if (status === "sent" && !options.forceRerun) {
    return { ok: false, reason: "manual send is the latest action; not re-replying without forceRerun" };
  }

  const inboundMs = thread.lastInboundAt && thread.lastInboundAt.toMillis
    ? thread.lastInboundAt.toMillis() : null;
  if (!inboundMs) return { ok: false, reason: "no inbound message on thread" };

  // Idempotency: if we've already processed an inbound at or after this
  // timestamp, skip. forceRerun bypasses.
  const lastProcessedMs = thread.lastAutoProcessedInboundAt && thread.lastAutoProcessedInboundAt.toMillis
    ? thread.lastAutoProcessedInboundAt.toMillis() : 0;
  if (!options.forceRerun && lastProcessedMs >= inboundMs) {
    return { ok: false, reason: "already auto-processed this inbound" };
  }

  // Don't auto-reply to ancient messages (operator backfill safety)
  const ageMs = Date.now() - inboundMs;
  if (ageMs > MAX_INBOUND_AGE_MS && !options.forceRerun) {
    return { ok: false, reason: `inbound too old (${Math.round(ageMs / 86400000)}d) for auto-reply` };
  }

  // If the latest message is outbound (operator just replied manually),
  // there's nothing to respond to.
  const outboundMs = thread.lastOutboundAt && thread.lastOutboundAt.toMillis
    ? thread.lastOutboundAt.toMillis() : 0;
  if (outboundMs > inboundMs) {
    return { ok: false, reason: "latest message is outbound; nothing to reply to" };
  }

  return { ok: true, inboundMs, ageMs };
}

/** Statuses that a thread can be in when the pipeline first sees it but
 *  that aren't visible in the operator inbox folders during processing.
 *  The atomic claim upgrades them to pending_human_review so the thread
 *  is always visible during processing.
 *
 *  v1.2: `archived` is included here. A new inbound on an archived
 *  thread is the customer re-engaging — bring it back into the active
 *  queue. Operator can re-archive after the AI handles it. */
const HIDDEN_INTAKE_STATUSES = new Set([
  "detected_from_gmail",
  "pending_etsy_scrape",
  "etsy_scraped",
  "pending_order_enrichment",
  "ready_for_ai",
  "draft_ready",        // legacy folder removed in v1.1
  "sent",               // legacy folder removed in v1.1 (only re-routed on new inbound)
  "hold_uncertain",     // legacy hold
  "hold_missing_order", // legacy hold
  "hold_login_required",// legacy hold
  "failed_scrape",
  "failed_send",
  "archived"            // v1.2: re-engage on new customer inbound
]);

/** Atomically claim a thread for auto-processing. Combines the eligibility
 *  check and the "in_progress" marker in one Firestore transaction so two
 *  rapid back-to-back snapshots can't both trigger an Opus call for the
 *  same inbound.
 *
 *  Side-effects (on successful claim):
 *    - Sets `lastAutoProcessedInboundAt` to the inbound timestamp
 *      (this is the idempotency lock — subsequent calls see it and skip).
 *    - Upgrades thread status from any HIDDEN_INTAKE_STATUS to
 *      pending_human_review so the thread is immediately visible while
 *      the AI call runs. The pipeline can later upgrade to auto_replied.
 *    - Writes lastAutoDecision="in_progress" / lastAutoDecisionAt so the
 *      operator's UI can show "AI thinking…" if we add that affordance.
 *
 *  Returns the same shape as evaluateEligibility, plus { claimed: true }
 *  on success and { claimed: false } when the lock was already held.
 */
async function claimThread(threadRef, options = {}) {
  return await db.runTransaction(async (tx) => {
    const snap = await tx.get(threadRef);
    if (!snap.exists) return { ok: false, claimed: false, reason: "thread not found" };
    const data = snap.data() || {};

    const elig = evaluateEligibility(data, options);
    if (!elig.ok) return { ok: false, claimed: false, reason: elig.reason, status: data.status };

    // Decide what status to claim with. If the thread is currently in a
    // hidden intake state, surface it to Needs Review so it's visible to
    // operators during the (potentially 60-second) AI run. If it's already
    // in auto_replied or pending_human_review, leave it — those are
    // user-visible states; we'll either keep or upgrade after the AI call.
    const currentStatus = data.status || "";
    const claimStatus = HIDDEN_INTAKE_STATUSES.has(currentStatus)
      ? "pending_human_review"
      : currentStatus;

    tx.update(threadRef, {
      lastAutoProcessedInboundAt: data.lastInboundAt,        // the lock
      status                    : claimStatus,
      lastAutoDecision          : "in_progress",
      lastAutoDecisionAt        : FV.serverTimestamp(),
      updatedAt                 : FV.serverTimestamp()
    });

    return {
      ok: true, claimed: true,
      inboundMs    : elig.inboundMs,
      ageMs        : elig.ageMs,
      previousStatus: currentStatus,
      claimStatus
    };
  });
}

/** Check whether a draft for this thread is already in flight (queued,
 *  sending, or recently sent). If so, skip — don't double-send. */
async function isDraftInFlight(threadId) {
  const draftId = "draft_" + threadId;
  const snap = await db.collection(DRAFTS_COLL).doc(draftId).get();
  if (!snap.exists) return null;
  const d = snap.data() || {};
  if (d.status === "queued" || d.status === "sending") return d.status;
  return null;
}

/** Mark a thread as auto-processed at a specific inbound timestamp +
 *  set its operator-facing status. Used by all decision branches.
 *
 *  Note: `lastAutoProcessedInboundAt` was already set by `claimThread`
 *  at the start of the run; we re-set it here defensively in case the
 *  finalize timestamp differs (it shouldn't, but doc-merging is cheap). */
async function finalizeThread(threadId, { newStatus, inboundMs, decision, draftId, aiConfidence, aiDifficulty }) {
  const patch = {
    status                       : newStatus,
    lastAutoProcessedInboundAt   : admin.firestore.Timestamp.fromMillis(inboundMs),
    lastAutoDecision             : decision,
    lastAutoDecisionAt           : FV.serverTimestamp(),
    aiConfidence                 : aiConfidence,
    aiDifficulty                 : aiDifficulty,
    aiDraftStatus                : draftId ? "ready" : "none",
    updatedAt                    : FV.serverTimestamp()
  };
  // Only write latestDraftId when we actually have one. Avoids
  // overwriting a previous valid draftId with null when the pipeline
  // skips AI generation (e.g., when the autoPipeline is disabled).
  if (draftId) patch.latestDraftId = draftId;
  await db.collection(THREADS_COLL).doc(threadId).set(patch, { merge: true });
}

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
    reason: "custom-order or discount inquiry" }
];

/** Run all veto patterns against given text. Returns array of triggered
 *  veto IDs + reasons. Empty array = clean. */
function runVetoPatterns(text) {
  if (!text || typeof text !== "string") return [];
  const hits = [];
  for (const v of DETERMINISTIC_VETO_PATTERNS) {
    if (v.pattern.test(text)) hits.push({ id: v.id, reason: v.reason });
  }
  return hits;
}

/** Fetch the most-recent inbound message text from a thread's messages
 *  subcollection. Used to run veto checks against what the customer
 *  actually said, not against thread metadata.
 *
 *  Returns null if no inbound exists (shouldn't happen since the pipeline
 *  only runs when one was just written, but defensive). */
async function loadLatestInboundText(threadId) {
  try {
    const snap = await db.collection(THREADS_COLL).doc(threadId)
      .collection("messages")
      .where("direction", "==", "inbound")
      .orderBy("timestamp", "desc")
      .limit(1)
      .get();
    if (snap.empty) return null;
    const m = snap.docs[0].data();
    return String(m.text || "").slice(0, 4000);   // cap for regex perf
  } catch (e) {
    console.warn("loadLatestInboundText failed:", e.message);
    return null;
  }
}

/** Apply all deterministic safety checks. Returns { vetoed, reasons }.
 *  Combines:
 *    - inbound message regex matches
 *    - outbound draft regex matches (catches AI drafts that promise
 *      things even when the inbound was cagey)
 *    - tool-call errors in the AI's draft (lookup failed → AI is
 *      working with incomplete data → don't auto-send)
 */
function applyDeterministicVetoes({ inboundText, draftText, draftToolCalls }) {
  const reasons = [];

  const inboundHits = runVetoPatterns(inboundText);
  for (const h of inboundHits) reasons.push("inbound_" + h.id + ": " + h.reason);

  const outboundHits = runVetoPatterns(draftText);
  for (const h of outboundHits) reasons.push("outbound_" + h.id + ": " + h.reason);

  // Tool-call errors: if the AI tried to look up an order or tracking
  // number and the call errored, the AI is either working with stale
  // info or flat-out hallucinating. Don't trust the draft.
  const toolErrors = (Array.isArray(draftToolCalls) ? draftToolCalls : [])
    .filter(tc => tc && tc.error && tc.name !== "compose_draft_reply");
  if (toolErrors.length) {
    reasons.push("tool_call_failed: " + toolErrors.map(tc => tc.name).join(","));
  }

  return { vetoed: reasons.length > 0, reasons };
}

exports.handler = async (event) => {
  if (event.httpMethod === "OPTIONS") {
    return { statusCode: 200, headers: CORS, body: "ok" };
  }
  if (event.httpMethod !== "POST") {
    return json(405, { error: "Method Not Allowed" });
  }

  // Auth: require the same shared secret as other extension endpoints.
  // The function is invoked by etsyMailSnapshot internally (which has
  // the secret in env) and can also be called by an operator from the
  // inbox (browser forwards the secret from localStorage).
  const auth = requireExtensionAuth(event);
  if (!auth.ok) return auth.response;

  let body = {};
  try { body = JSON.parse(event.body || "{}"); }
  catch { return bad("Invalid JSON body"); }

  const {
    threadId,
    confidenceThreshold,
    employeeName = "system:auto-pipeline",
    forceRerun   = false,
    dryRun       = false
  } = body;

  if (!threadId) return bad("Missing threadId");

  // Single-source-of-truth config: Firestore at EtsyMail_Config/autoPipeline.
  const autoCfg = await getAutoPipelineConfig();

  // Per-call override via request body (lets operators dry-run a specific
  // threshold), otherwise use the Firestore config.
  const threshold = (typeof confidenceThreshold === "number" && confidenceThreshold >= 0 && confidenceThreshold <= 1)
    ? confidenceThreshold
    : autoCfg.threshold;

  const tStart = Date.now();
  const threadRef = db.collection(THREADS_COLL).doc(threadId);

  try {
    // ─── 1. Atomic claim ─────────────────────────────────────────
    // Combines eligibility check + lock acquisition into one Firestore
    // transaction. After this, the thread:
    //   - has lastAutoProcessedInboundAt set (idempotency lock)
    //   - is in pending_human_review if it was at a hidden intake status
    //     (so it's visible during the AI run, not stuck in "All" only)
    //   - has lastAutoDecision="in_progress" for observability
    //
    // If the lock is already held, the second caller bails here — no
    // wasted Opus calls in race scenarios.
    const claim = await claimThread(threadRef, { forceRerun });
    if (!claim.ok) {
      await writeAudit({
        threadId, eventType: "auto_pipeline_skipped",
        payload: { reason: claim.reason, status: claim.status }
      });
      return ok({
        threadId, decision: "skipped",
        skipReason: claim.reason,
        durationMs: Date.now() - tStart
      });
    }

    // Don't double-act if a previous run is still queued/sending. We
    // check this AFTER the claim — the claim itself doesn't know about
    // draft state, only thread state.
    const inFlight = await isDraftInFlight(threadId);
    if (inFlight && !forceRerun) {
      // v1.4: explicitly unwind the claim's in_progress marker.
      // The claim transaction set lastAutoDecision="in_progress"; if
      // we just return here, the thread shows "AI thinking..." forever
      // until the stale-claim reaper kicks in 5+ minutes later. Set a
      // terminal decision so the UI reflects reality immediately.
      //
      // Thread status was already moved to a visible folder by the
      // claim if needed, so no status change is required — just clear
      // the in_progress flag.
      await db.collection(THREADS_COLL).doc(threadId).set({
        lastAutoDecision           : "skipped_draft_in_flight",
        lastAutoDecisionAt         : FV.serverTimestamp(),
        lastAutoProcessedInboundAt : null,    // allow re-trigger when in-flight clears
        updatedAt                  : FV.serverTimestamp()
      }, { merge: true });
      await writeAudit({
        threadId, eventType: "auto_pipeline_skipped",
        payload: { reason: `draft already ${inFlight}`, claimUnwound: true }
      });
      return ok({
        threadId, decision: "skipped",
        skipReason: `draft already ${inFlight}`,
        durationMs: Date.now() - tStart
      });
    }

    // ─── 2. Generate AI draft ────────────────────────────────────
    // We always generate the draft, even when the auto-pipeline is
    // disabled in config. "Disabled" means "don't auto-send" — it
    // doesn't mean "don't think". An operator opening a Needs Review
    // thread should still see the AI's suggested reply alongside its
    // confidence score, ready to edit and send manually.
    //
    // The slow step — typically 10-60 seconds with Opus 4.7 + tool calls.
    let draftResp;
    try {
      draftResp = await callFunction("etsyMailDraftReply", {
        threadId,
        mode         : "initial",
        employeeName : employeeName || "system:auto-pipeline"
      });
    } catch (err) {
      await writeAudit({
        threadId, eventType: "auto_pipeline_failed",
        payload: { stage: "draft_generation", error: err.message }
      });
      // Thread is already in pending_human_review (from the claim) —
      // just record the failure on the thread doc so the UI shows it.
      await db.collection(THREADS_COLL).doc(threadId).set({
        aiDraftStatus     : "failed",
        lastAutoDecision  : "failed",
        lastAutoDecisionAt: FV.serverTimestamp(),
        updatedAt         : FV.serverTimestamp()
      }, { merge: true });
      return json(500, { error: "Draft generation failed: " + err.message, threadId });
    }

    const aiConfidence = (typeof draftResp.aiConfidence === "number") ? draftResp.aiConfidence
                       : (typeof draftResp.confidence   === "number") ? draftResp.confidence
                       : 0;
    const aiDifficulty = (typeof draftResp.aiDifficulty === "number") ? draftResp.aiDifficulty
                       : (typeof draftResp.difficulty   === "number") ? draftResp.difficulty
                       : null;
    const draftId      = draftResp.draftId || ("draft_" + threadId);
    const text         = draftResp.text || "";

    // ─── 3. Pipeline disabled? Route to human review with the draft ──
    // We DID generate the draft (above), we just don't auto-send it.
    if (!autoCfg.enabled) {
      await finalizeThread(threadId, {
        newStatus    : "pending_human_review",
        inboundMs    : claim.inboundMs,
        decision     : "human_review_pipeline_disabled",
        draftId,
        aiConfidence,
        aiDifficulty
      });
      await writeAudit({
        threadId, draftId,
        eventType: "auto_pipeline_routed_to_review",
        actor    : employeeName,
        payload  : {
          reason: "auto-pipeline disabled in config",
          aiConfidence, aiDifficulty,
          previousStatus: claim.previousStatus
        }
      });
      return ok({
        threadId,
        decision   : "human_review",
        reason     : "auto-pipeline disabled in config",
        aiConfidence,
        aiDifficulty,
        draftId,
        text,
        durationMs : Date.now() - tStart
      });
    }

    // ─── 4. Deterministic safety vetoes ──────────────────────────
    // Even with confidence ≥ threshold, certain customer requests must
    // never auto-send: refunds, cancellations, legal escalation,
    // damaged-item claims, address changes, custom orders, tool-call
    // errors. The veto check runs against the latest INBOUND text
    // (what the customer actually said) and the OUTBOUND draft (catches
    // AI drafts that promise refunds even when the inbound was cagey).
    const inboundText = await loadLatestInboundText(threadId);
    const veto = applyDeterministicVetoes({
      inboundText,
      draftText      : text,
      draftToolCalls : draftResp.toolCalls
    });

    // ─── 5. Branch: auto-send vs human review ────────────────────
    const meetsThreshold = aiConfidence >= threshold;

    // Kill-switch: if global send is paused, we can still draft, but
    // can't auto-send. Force the route to human review with a clear note.
    const ks = await getKillSwitch();

    const decision = (meetsThreshold && !veto.vetoed && !ks.disabled && !dryRun)
      ? "auto_send"
      : "human_review";

    if (decision === "auto_send") {
      // Enqueue via the existing send pipeline. The Chrome extension
      // picks it up on its next peek (same path as a manual click).
      const tSnap = await threadRef.get();
      const thread = tSnap.exists ? tSnap.data() : {};
      const etsyConversationUrl = thread.etsyConversationUrl
        || ("https://www.etsy.com/your/conversations/"
           + (thread.etsyConversationId || threadId.replace("etsy_conv_", "")));

      try {
        // v1.5: atomic enqueue + thread finalize. Pass the finalize
        // patch as primitive fields; the enqueue op writes both the
        // draft AND the thread status in ONE Firestore transaction.
        // Pre-v1.5 this was two sequential writes — if the second
        // failed mid-flight, the draft would be queued (extension
        // sends it) but the thread would still be at pending_human_
        // review from the claim. The final folder placement was
        // wrong even though the customer got the right reply.
        await callFunction("etsyMailDraftSend", {
          op                  : "enqueue",
          threadId,
          etsyConversationUrl,
          text,
          attachments         : Array.isArray(draftResp.attachments) ? draftResp.attachments : [],
          employeeName,
          aiMeta              : {
            generatedByAI : true,
            model         : draftResp.model || null,
            reasoning     : draftResp.reasoning || null,
            activeQuestion: draftResp.activeQuestion || null,
            confidence    : aiConfidence,
            difficulty    : aiDifficulty
          },
          force               : true,
          parentThreadFinalizePatch : {
            threadId,
            newStatus    : "queued_for_auto_send",
            inboundMs    : claim.inboundMs,
            decision     : "auto_send_enqueued",
            aiConfidence,
            aiDifficulty
          }
        });
      } catch (err) {
        // If enqueue fails, fall back to human review — never silently
        // drop the AI's work. Thread is already at pending_human_review
        // from the claim; we just record the fallback.
        //
        // Note: because the enqueue txn is atomic, this catch block
        // means BOTH the draft enqueue AND the thread finalize were
        // rolled back — the thread is still at the claim's status
        // (pending_human_review). Calling finalizeThread here writes
        // the operator-facing fallback metadata (decision reason, etc.)
        // without changing the user-visible folder.
        await writeAudit({
          threadId, draftId, eventType: "auto_pipeline_enqueue_failed",
          payload: { error: err.message, errorCode: err.data && err.data.errorCode }
        });
        await finalizeThread(threadId, {
          newStatus    : "pending_human_review",
          inboundMs    : claim.inboundMs,
          decision     : "human_review_after_enqueue_failure",
          draftId,
          aiConfidence,
          aiDifficulty
        });
        return ok({
          threadId,
          decision    : "human_review",
          fallbackReason: "enqueue failed: " + err.message,
          aiConfidence,
          aiDifficulty,
          threshold,
          draftId,
          text,
          durationMs  : Date.now() - tStart
        });
      }

      // v1.5: thread was already promoted to queued_for_auto_send
      // atomically inside the enqueue transaction above. No separate
      // finalizeThread call here — pre-v1.5 there was, and a failure
      // between the two writes was the bug we just fixed. The audit
      // entry below still gets written.

      await writeAudit({
        threadId, draftId,
        eventType: "auto_pipeline_auto_sent",
        actor    : employeeName,
        payload  : {
          aiConfidence, aiDifficulty, threshold,
          model     : draftResp.model || null,
          textChars : text.length,
          attachmentCount: Array.isArray(draftResp.attachments) ? draftResp.attachments.length : 0
        }
      });

      return ok({
        threadId,
        decision  : "auto_send",
        aiConfidence,
        aiDifficulty,
        threshold,
        draftId,
        text,
        durationMs: Date.now() - tStart
      });
    }

    // ─── human_review branch ────────────────────────────────────
    // Determine the reason in priority order: vetoes first (most
    // important to surface to operators), then kill-switch, then
    // dryRun, then plain confidence-below-threshold.
    let fallbackReason;
    if (veto.vetoed) {
      fallbackReason = "deterministic veto: " + veto.reasons.join("; ");
    } else if (ks.disabled) {
      fallbackReason = "kill-switch active; not auto-sending";
    } else if (dryRun) {
      fallbackReason = "dryRun=true";
    } else {
      fallbackReason = `confidence ${aiConfidence.toFixed(2)} below threshold ${threshold.toFixed(2)}`;
    }

    await finalizeThread(threadId, {
      newStatus    : "pending_human_review",
      inboundMs    : claim.inboundMs,
      decision     : veto.vetoed ? "human_review_vetoed" : "human_review",
      draftId,
      aiConfidence,
      aiDifficulty
    });

    await writeAudit({
      threadId, draftId,
      eventType: "auto_pipeline_routed_to_review",
      actor    : employeeName,
      payload  : {
        reason     : fallbackReason,
        aiConfidence, aiDifficulty, threshold,
        vetoes     : veto.reasons,
        killSwitchDisabled: ks.disabled,
        model      : draftResp.model || null
      }
    });

    return ok({
      threadId,
      decision    : "human_review",
      reason      : fallbackReason,
      vetoes      : veto.reasons,
      aiConfidence,
      aiDifficulty,
      threshold,
      draftId,
      text,
      durationMs  : Date.now() - tStart
    });
  } catch (err) {
    console.error("etsyMailAutoPipeline error:", err);
    await writeAudit({
      threadId, eventType: "auto_pipeline_failed",
      payload: { error: err.message }
    }).catch(() => {});
    return json(500, { error: err.message || String(err) });
  }
};
