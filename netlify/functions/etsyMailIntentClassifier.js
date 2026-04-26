/*  netlify/functions/etsyMailIntentClassifier.js
 *
 *  v2.0 Step 1 — Intent classifier
 *
 *  Classifies a single inbound customer message into one of five buckets:
 *    support | sales_lead | post_purchase | spam | unclear
 *
 *  ═══ DESIGN NOTES ════════════════════════════════════════════════════════
 *
 *  Model: claude-haiku-4-5-20251001 — cheap, fast. Classification doesn't
 *  need Opus accuracy and we want sub-second turnaround so the auto-pipeline
 *  doesn't drag. No thinking blocks (haiku family doesn't benefit and costs
 *  more tokens with them on).
 *
 *  Output shape: strict JSON, parsed defensively. Bad model output is NOT
 *  a fatal error for the caller — we return { classification: "unclear",
 *  confidence: 0, parseError: ... } so the auto-pipeline can keep going.
 *
 *  Cache: per-thread, 24h, in EtsyMail_IntentClassifications/{threadId}.
 *  The auto-pipeline calls this once per inbound; if the same thread gets
 *  another classify request within 24h we return the cached result. Force
 *  re-classification with `force: true`. The cache lifetime matters less
 *  than the fact that classification is idempotent — the SAME message
 *  yields the SAME classification (Haiku temperature is effectively 0 for
 *  short structured outputs).
 *
 *  v2.0 Step 2 forward-compat: the cache doc shape is what the sales-lead
 *  router will read in etsyMailAutoPipeline-background.js. Don't break it.
 *  Step 2 may add fields (e.g., classifierVersion, signals_v2) — additive
 *  only.
 *
 *  ═══ REQUEST ════════════════════════════════════════════════════════════
 *
 *  POST {
 *    threadId    : "etsy_conv_1651714855",     // required
 *    messageText : "do you make custom...",     // required
 *    force       : false,                        // optional; bypass cache
 *    actor       : "system:auto-pipeline"        // optional, for audit
 *  }
 *
 *  ═══ RESPONSE ═══════════════════════════════════════════════════════════
 *
 *  {
 *    success        : true,
 *    threadId       : "...",
 *    classification : "sales_lead",
 *    confidence     : 0.85,
 *    signals        : ["custom request", "asks for quote"],
 *    reasoning      : "Customer asks if you can make a custom necklace.",
 *    cached         : false,                    // true if served from cache
 *    classifiedAt   : 1714080000000,
 *    model          : "claude-haiku-4-5-20251001",
 *    parseError     : null                      // string if model output
 *                                                // didn't parse and we
 *                                                // fell back to "unclear"
 *  }
 *
 *  ═══ ENV VARS ═══════════════════════════════════════════════════════════
 *
 *  ANTHROPIC_API_KEY              required (also used by draftReply)
 *  ETSYMAIL_INTENT_MODEL          optional; default claude-haiku-4-5-20251001
 *  ETSYMAIL_EXTENSION_SECRET      gates this endpoint (same as siblings)
 */

const fs = require("fs");
const path = require("path");

const admin = require("./firebaseAdmin");
const { CORS, requireExtensionAuth } = require("./_etsyMailAuth");
const { callClaudeRaw } = require("./_etsyMailAnthropic");

const db = admin.firestore();
const FV = admin.firestore.FieldValue;

// ─── Collections ────────────────────────────────────────────────────────
const CACHE_COLL   = "EtsyMail_IntentClassifications";
const THREADS_COLL = "EtsyMail_Threads";
const AUDIT_COLL   = "EtsyMail_Audit";

// ─── Model config ───────────────────────────────────────────────────────
// Haiku 4.5 — cheapest current model. Classification is structured output
// so even smaller models would work, but Haiku 4.5 is the smallest in the
// 4.x family that supports the same API shape as the rest of the system.
const INTENT_MODEL = process.env.ETSYMAIL_INTENT_MODEL || "claude-haiku-4-5-20251001";
const INTENT_MAX_TOKENS = 300;     // ~5x the expected output, leaves room for
                                    // the model to ramble if it does
const CACHE_TTL_MS = 24 * 60 * 60 * 1000;  // 24h
const MESSAGE_TEXT_CAP = 4000;     // hard cap on input length so a giant
                                    // pasted email doesn't blow tokens

// Five canonical categories. The model is instructed to pick exactly one;
// anything else falls back to "unclear".
const VALID_CATEGORIES = new Set([
  "support", "sales_lead", "post_purchase", "spam", "unclear"
]);

// ─── System prompt — loaded from prompts/intent_classifier.md ───────────
//
// The prompt file is bundled into the Netlify function by setting
// `included_files = ["netlify/functions/prompts/**"]` in netlify.toml.
// Without that toml line, esbuild bundles only the .js sources and the
// readFileSync below FAILS — fast and loud, not silently degraded.
//
// Why we deliberately do NOT fall back to a shorter inline prompt: the
// fallback would silently downgrade classification accuracy without the
// operator noticing. Every classification across the deployment lifetime
// would be stuck on the worse prompt. Hard-failing means the
// misconfiguration shows up as `intent_classify_failed` audit events on
// the FIRST classify call, which is loud and obvious. Deploy → first
// inbound → audit log shows the issue → fix the toml → redeploy.
//
// We try-load lazily on first use rather than at module-scope so the
// function CAN still serve OPTIONS (CORS preflight) and the handler can
// return a useful 503 response with a specific errorCode.
let _SYSTEM_PROMPT = null;
let _PROMPT_LOAD_ERROR = null;
function loadSystemPrompt() {
  if (_SYSTEM_PROMPT) return { ok: true, prompt: _SYSTEM_PROMPT };
  if (_PROMPT_LOAD_ERROR) return { ok: false, error: _PROMPT_LOAD_ERROR };
  try {
    const p = path.join(__dirname, "prompts", "intent_classifier.md");
    _SYSTEM_PROMPT = fs.readFileSync(p, "utf8");
    if (!_SYSTEM_PROMPT || _SYSTEM_PROMPT.length < 100) {
      // File exists but is suspiciously empty/truncated — treat as missing.
      _SYSTEM_PROMPT = null;
      _PROMPT_LOAD_ERROR = `prompts/intent_classifier.md is empty or truncated (${_SYSTEM_PROMPT ? _SYSTEM_PROMPT.length : 0} bytes)`;
      console.error("[intentClassifier] " + _PROMPT_LOAD_ERROR);
      return { ok: false, error: _PROMPT_LOAD_ERROR };
    }
    return { ok: true, prompt: _SYSTEM_PROMPT };
  } catch (e) {
    _PROMPT_LOAD_ERROR =
      "Could not load prompts/intent_classifier.md (" + e.message + "). " +
      "Verify netlify.toml [functions] block has " +
      "included_files = [\"netlify/functions/prompts/**\"] then redeploy.";
    console.error("[intentClassifier] " + _PROMPT_LOAD_ERROR);
    return { ok: false, error: _PROMPT_LOAD_ERROR };
  }
}

// ─── Helpers ────────────────────────────────────────────────────────────

function json(statusCode, body) {
  return { statusCode, headers: { ...CORS, "Content-Type": "application/json" }, body: JSON.stringify(body) };
}
function bad(msg, code = 400) { return json(code, { error: msg }); }
function ok(body)             { return json(200, { success: true, ...body }); }

async function writeAudit({ threadId, eventType, actor = "system:intentClassifier", payload = {} }) {
  // Match v1.10 canonical audit shape used by every other function in this
  // codebase: { threadId, draftId, eventType, actor, payload, createdAt }.
  // Step 2's pricing/agent code will continue to use this same shape; new
  // optional fields (outcome, ruleViolations) ride inside `payload` so the
  // top-level schema never breaks.
  try {
    await db.collection(AUDIT_COLL).add({
      threadId : threadId || null,
      draftId  : null,
      eventType,
      actor,
      payload  : payload || {},
      createdAt: FV.serverTimestamp()
    });
  } catch (e) {
    console.warn("audit write failed (non-fatal):", e.message);
  }
}

/** Pull the cached classification if it's still fresh.
 *  Returns null on cache miss / stale / parse error. */
async function readCache(threadId, messageText = "") {
  try {
    const snap = await db.collection(CACHE_COLL).doc(threadId).get();
    if (!snap.exists) return null;
    const d = snap.data() || {};
    const at = d.classifiedAt && d.classifiedAt.toMillis ? d.classifiedAt.toMillis() : 0;
    if (!at || Date.now() - at > CACHE_TTL_MS) return null;
    if (!VALID_CATEGORIES.has(d.classification)) return null;

    // v2.3 hardening: cache is keyed by thread, but classification is
    // based on a single inbound message. If the customer sends a new
    // message in the same thread within 24h, the old classification must
    // not be reused or sales leads can be misrouted as stale support.
    const currentPrefix = String(messageText || "").slice(0, 200);
    if (d.inputHashPrefix && d.inputHashPrefix !== currentPrefix) return null;

    return {
      classification: d.classification,
      confidence    : typeof d.confidence === "number" ? d.confidence : 0,
      signals       : Array.isArray(d.signals) ? d.signals : [],
      reasoning     : d.reasoning || "",
      classifiedAt  : at,
      model         : d.model || null
    };
  } catch (e) {
    console.warn("intent cache read failed:", e.message);
    return null;
  }
}

/** Write the classification to BOTH:
 *    1. EtsyMail_IntentClassifications/{threadId}  (canonical, full doc)
 *    2. EtsyMail_Threads/{threadId} (denormalized fields for thread-list
 *       rendering and Step 2 routing — readers shouldn't have to do a
 *       second collection lookup just to draw a badge or decide where to
 *       route a sales-lead).
 *
 *  Both writes happen sequentially. If the canonical write succeeds and
 *  the thread-doc denormalize fails, the thread badge will be stale until
 *  the next classify run, but the canonical record is correct. Acceptable
 *  failure mode.
 */
async function writeResult(threadId, result, messageText) {
  const nowTs = FV.serverTimestamp();
  // 1. Canonical
  await db.collection(CACHE_COLL).doc(threadId).set({
    threadId,
    classification: result.classification,
    confidence    : result.confidence,
    signals       : result.signals,
    reasoning     : result.reasoning,
    model         : result.model,
    // Hash of message text (prefix 200 chars) so we can later detect when
    // the source message changed — useful for Step 2 if we want to
    // invalidate cache when the customer sends a new message.
    inputHashPrefix: String(messageText || "").slice(0, 200),
    classifiedAt  : nowTs,
    updatedAt     : nowTs
  }, { merge: true });

  // 2. Denormalize onto thread doc for fast list-row rendering and for
  // Step 2's salesAutoEngage router (which reads classification + confidence
  // off the thread doc as part of its routing decision).
  try {
    await db.collection(THREADS_COLL).doc(threadId).set({
      intentClassification: result.classification,
      intentConfidence    : result.confidence,
      intentSignals       : result.signals,
      intentClassifiedAt  : nowTs,
      updatedAt           : nowTs
    }, { merge: true });
  } catch (e) {
    console.warn("intent denormalize on thread doc failed:", e.message);
  }
}

/** Defensive JSON parse — strips markdown fences, leading/trailing text,
 *  picks out the first balanced { } block. The model SHOULDN'T emit any
 *  of that, but defensive parsing makes us robust to a 1-in-10000 stray
 *  chain-of-thought leak.
 *
 *  Returns the parsed object, or null if no parseable JSON found. */
function tryParseJson(rawText) {
  if (!rawText || typeof rawText !== "string") return null;
  let text = rawText.trim();

  // Strip ```json ... ``` fences if present
  if (text.startsWith("```")) {
    text = text.replace(/^```(?:json)?\s*/i, "").replace(/\s*```\s*$/, "");
  }

  // Try direct parse first
  try { return JSON.parse(text); } catch {}

  // Fall back: find first { ... matching } in the string
  const start = text.indexOf("{");
  if (start === -1) return null;

  let depth = 0;
  let inString = false;
  let escape = false;
  for (let i = start; i < text.length; i++) {
    const c = text[i];
    if (escape) { escape = false; continue; }
    if (c === "\\") { escape = true; continue; }
    if (c === '"') { inString = !inString; continue; }
    if (inString) continue;
    if (c === "{") depth++;
    else if (c === "}") {
      depth--;
      if (depth === 0) {
        const candidate = text.slice(start, i + 1);
        try { return JSON.parse(candidate); } catch { return null; }
      }
    }
  }
  return null;
}

/** Validate + coerce a parsed model output into the canonical shape.
 *  Anything malformed degrades gracefully to "unclear" with confidence 0
 *  and a parseError annotation. The pipeline keeps moving. */
function coerceClassification(parsed, parseError) {
  const fallback = {
    classification: "unclear",
    confidence    : 0,
    signals       : [],
    reasoning     : parseError ? `Classifier output unparseable: ${parseError}` : "Classifier returned no usable output.",
    parseError    : parseError || "no_output"
  };
  if (!parsed || typeof parsed !== "object") return fallback;

  const cls = String(parsed.classification || "").toLowerCase().trim();
  if (!VALID_CATEGORIES.has(cls)) {
    return { ...fallback, parseError: `unknown_category: ${cls || "(empty)"}`,
             reasoning: `Model returned non-canonical category '${cls}'.` };
  }

  let conf = Number(parsed.confidence);
  if (!Number.isFinite(conf)) conf = 0;
  conf = Math.max(0, Math.min(1, conf));

  let signals = parsed.signals;
  if (!Array.isArray(signals)) signals = [];
  signals = signals.slice(0, 6).map(s => String(s).slice(0, 120));

  const reasoning = String(parsed.reasoning || "").slice(0, 500);

  return {
    classification: cls,
    confidence    : conf,
    signals,
    reasoning,
    parseError    : null
  };
}

// ─── Core classify call ────────────────────────────────────────────────

async function classifyMessage(messageText) {
  const truncated = String(messageText || "").slice(0, MESSAGE_TEXT_CAP);
  if (!truncated.trim()) {
    return {
      classification: "unclear",
      confidence    : 0.95,
      signals       : ["empty message"],
      reasoning     : "Inbound message text was empty after trimming.",
      parseError    : null,
      model         : INTENT_MODEL
    };
  }

  const promptLoad = loadSystemPrompt();
  if (!promptLoad.ok) {
    // Hard fail — no silent fallback. The handler converts this into a
    // 503 with errorCode "PROMPT_NOT_BUNDLED" so the operator sees the
    // misconfiguration immediately in the audit log.
    const err = new Error(promptLoad.error);
    err.code = "PROMPT_NOT_BUNDLED";
    throw err;
  }

  const resp = await callClaudeRaw({
    model      : INTENT_MODEL,
    maxTokens  : INTENT_MAX_TOKENS,
    system     : promptLoad.prompt,
    messages   : [{ role: "user", content: truncated }],
    useThinking: false   // haiku 4.5 — no adaptive thinking, save tokens
  });

  // Pull the first text block out of content[]
  const textBlocks = (resp.content || []).filter(b => b && b.type === "text");
  const rawText = textBlocks.map(b => b.text || "").join("").trim();

  let parsed = tryParseJson(rawText);
  let parseError = parsed ? null : "json_parse_failed";

  const coerced = coerceClassification(parsed, parseError);
  return { ...coerced, model: INTENT_MODEL, rawText: rawText.slice(0, 500) };
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

  const { threadId, messageText, force = false, actor = "system:intentClassifier" } = body;
  if (!threadId)    return bad("Missing threadId");
  if (!messageText) return bad("Missing messageText");

  const tStart = Date.now();

  try {
    // ── Cache check ──
    if (!force) {
      const cached = await readCache(threadId, messageText);
      if (cached) {
        return ok({
          threadId,
          ...cached,
          cached    : true,
          durationMs: Date.now() - tStart
        });
      }
    }

    // ── Classify ──
    const result = await classifyMessage(messageText);

    // ── Persist ──
    await writeResult(threadId, result, messageText);

    // ── Audit ──
    await writeAudit({
      threadId,
      eventType: "intent_classified",
      actor,
      payload  : {
        classification: result.classification,
        confidence    : result.confidence,
        signals       : result.signals,
        model         : result.model,
        forced        : !!force,
        parseError    : result.parseError || null,
        durationMs    : Date.now() - tStart
      }
    });

    return ok({
      threadId,
      classification: result.classification,
      confidence    : result.confidence,
      signals       : result.signals,
      reasoning     : result.reasoning,
      cached        : false,
      classifiedAt  : Date.now(),
      model         : result.model,
      parseError    : result.parseError || null,
      durationMs    : Date.now() - tStart
    });

  } catch (err) {
    console.error("intentClassifier error:", err);
    const isPromptMissing = err && err.code === "PROMPT_NOT_BUNDLED";
    await writeAudit({
      threadId,
      eventType: "intent_classify_failed",
      actor,
      payload  : {
        error    : err.message || String(err),
        errorCode: err && err.code ? err.code : null
      }
    });
    return json(isPromptMissing ? 503 : 500, {
      error    : err.message || String(err),
      errorCode: err && err.code ? err.code : null,
      threadId
    });
  }
};
