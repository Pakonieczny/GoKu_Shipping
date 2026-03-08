/* netlify/functions/geminiCodeProxy-background.js */
/* ═══════════════════════════════════════════════════════════════════
   TRANCHED AI PIPELINE — v3.0 (Self-Chaining)
   ─────────────────────────────────────────────────────────────────
   Each invocation handles ONE unit of work then chains to itself
   for the next, staying well under Netlify's 15-min limit.

   Invocation 0  ▸  "plan"    — Gemini planner model creates tranches, saves state
   Invocation 1–N ▸ "tranche" — Gemini executor model executes one tranche,
                     saves accumulated files, chains to next tranche
   Final          ▸ Writes ai_response.json so the frontend picks
                     up the completed build.

   All intermediate state lives in Firebase so each invocation is
   stateless and can reconstruct context from the pipeline file.
   ═══════════════════════════════════════════════════════════════════ */

const fetch = require("node-fetch");
const admin = require("./firebaseAdmin");

/* ── Gemini API config ────────────────────────────────────────── */
/*
  Gemini compatibility notes:
  - Gemini 3 / 3.1 models use thinkingLevel.
  - Gemini 2.5 models use thinkingBudget.
  - responseMimeType is supported for strict JSON planner output.
  - streamGenerateContent uses SSE with alt=sse.
*/
const GEMINI_API_BASE = "https://generativelanguage.googleapis.com/v1beta/models";

// Keep planner on Pro-class model for deep planning.
const GEMINI_DEFAULT_PLANNER_MODEL =
  process.env.GEMINI_GAME_PLANNER_MODEL ||
  process.env.GEMINI_MODEL ||
  "gemini-3.1-pro-preview";

// Keep executor on Flash for speed/cost, unless overridden.
const GEMINI_DEFAULT_EXECUTOR_MODEL =
  process.env.GEMINI_GAME_EXECUTOR_MODEL ||
  "gemini-3-flash-preview";

const GEMINI_DEFAULT_REASONING_EFFORT = "medium"; // logical internal setting, mapped to thinkingLevel/Budget
const GEMINI_DEFAULT_TEXT_VERBOSITY = "low";      // Gemini has no direct verbosity API knob; applied through prompt steering
const GEMINI_DEFAULT_MAX_OUTPUT_TOKENS = 24000;

const GEMINI_DEFAULT_PLANNER_REASONING_EFFORT = "medium";
const GEMINI_DEFAULT_PLANNER_TEXT_VERBOSITY = "medium";
const GEMINI_DEFAULT_PLANNER_MAX_OUTPUT_TOKENS = 32000;

const GEMINI_DEFAULT_EXECUTOR_REASONING_EFFORT = "medium";
const GEMINI_DEFAULT_EXECUTOR_TEXT_VERBOSITY = "low";
const GEMINI_DEFAULT_EXECUTOR_MAX_OUTPUT_TOKENS = 24000;

const GEMINI_HTTP_TIMEOUT_MS = Number(process.env.GEMINI_HTTP_TIMEOUT_MS || 1500000);
const GEMINI_PLANNER_HTTP_TIMEOUT_MS = Number(process.env.GEMINI_PLANNER_HTTP_TIMEOUT_MS || 1500000);
const GEMINI_CHAIN_ACCEPT_TIMEOUT_MS = Number(process.env.GEMINI_CHAIN_ACCEPT_TIMEOUT_MS || 1500000);
const GEMINI_PROGRESS_STREAM_FLUSH_MS = Number(process.env.GEMINI_PROGRESS_STREAM_FLUSH_MS || 8000);
const GEMINI_PROGRESS_STREAM_MIN_CHARS = Number(process.env.GEMINI_PROGRESS_STREAM_MIN_CHARS || 1200);
const GEMINI_PROGRESS_EVENTS_LIMIT = Number(process.env.GEMINI_PROGRESS_EVENTS_LIMIT || 80);
const GEMINI_PROGRESS_STREAM_PREVIEW_LIMIT = Number(process.env.GEMINI_PROGRESS_STREAM_PREVIEW_LIMIT || 4000);

/* ── helper: normalize multimodal parts for Gemini ───────────── */
function normalizeGeminiContentBlocks(userContent) {
  const blocks = Array.isArray(userContent)
    ? userContent
    : [{ type: "text", text: String(userContent || "") }];

  const normalized = [];

  for (const block of blocks) {
    if (!block) continue;

    if (block.type === "text") {
      normalized.push({ text: String(block.text || "") });
      continue;
    }

    if (block.type === "image") {
      const mediaType = block.source?.media_type || "image/png";
      const base64Data = block.source?.data;
      if (!base64Data) continue;
      normalized.push({
        inlineData: {
          mimeType: mediaType,
          data: base64Data
        }
      });
      continue;
    }

    if (block.type === "audio") {
      const mediaType = block.source?.media_type || "audio/mpeg";
      const base64Data = block.source?.data;
      if (!base64Data) continue;
      normalized.push({
        inlineData: {
          mimeType: mediaType,
          data: base64Data
        }
      });
      continue;
    }

    if (block.type === "video") {
      const mediaType = block.source?.media_type || "video/mp4";
      const base64Data = block.source?.data;
      if (!base64Data) continue;
      normalized.push({
        inlineData: {
          mimeType: mediaType,
          data: base64Data
        }
      });
      continue;
    }

    if (block.text && typeof block.text === "string") {
      normalized.push({ text: block.text });
      continue;
    }

    if (block.inlineData?.data && block.inlineData?.mimeType) {
      normalized.push({
        inlineData: {
          data: block.inlineData.data,
          mimeType: block.inlineData.mimeType
        }
      });
    }
  }

  return normalized;
}

function extractGeminiText(data) {
  if (!data || typeof data !== "object") return "";

  if (typeof data.text === "string" && data.text.trim()) {
    return data.text.trim();
  }

  const candidates = Array.isArray(data.candidates) ? data.candidates : [];
  const textParts = [];

  for (const candidate of candidates) {
    const parts = Array.isArray(candidate?.content?.parts) ? candidate.content.parts : [];
    for (const part of parts) {
      if (typeof part?.text === "string") {
        textParts.push(part.text);
      }
    }
  }

  return textParts.join("").trim();
}

function buildOutputBudgetBreakdown(usage) {
  const outputTokens = Number(usage?.output_tokens || 0);
  const reasoningTokens = Number(usage?.output_tokens_details?.reasoning_tokens || 0);
  const safeReasoningTokens = Math.max(0, Math.min(outputTokens, reasoningTokens));
  const restOutputTokens = Math.max(0, outputTokens - safeReasoningTokens);

  return {
    reasoning_tokens: safeReasoningTokens,
    rest_output_tokens: restOutputTokens
  };
}

function mapGeminiUsage(usageMetadata) {
  if (!usageMetadata || typeof usageMetadata !== "object") return null;

  const inputTokens = Number(usageMetadata.promptTokenCount || 0);
  const outputTokens = Number(usageMetadata.candidatesTokenCount || 0);
  const totalTokens = Number(
    usageMetadata.totalTokenCount ||
    (inputTokens + outputTokens)
  );
  const reasoningTokens = Number(usageMetadata.thoughtsTokenCount || 0);

  return {
    input_tokens: inputTokens,
    output_tokens: outputTokens,
    total_tokens: totalTokens,
    input_tokens_details: {
      cached_content_tokens: Number(usageMetadata.cachedContentTokenCount || 0),
      tool_use_prompt_tokens: Number(usageMetadata.toolUsePromptTokenCount || 0),
      prompt_tokens_details: usageMetadata.promptTokensDetails || null,
      cache_tokens_details: usageMetadata.cacheTokensDetails || null,
      tool_use_prompt_tokens_details: usageMetadata.toolUsePromptTokensDetails || null
    },
    output_tokens_details: {
      reasoning_tokens: reasoningTokens,
      candidates_tokens_details: usageMetadata.candidatesTokensDetails || null
    },
    output_budget: {
      reasoning_tokens: reasoningTokens,
      rest_output_tokens: Math.max(0, outputTokens - reasoningTokens)
    }
  };
}

function mapVerbosityInstruction(verbosity) {
  const value = String(verbosity || GEMINI_DEFAULT_TEXT_VERBOSITY || "medium").toLowerCase();
  if (value === "low") {
    return "Keep your wording tight and efficient. Do not add extra prose beyond what the required format needs.";
  }
  if (value === "high") {
    return "Be detailed and explicit where needed, while still obeying the required output format exactly.";
  }
  return "Be moderately detailed, but avoid filler and unnecessary narration.";
}

function mapGeminiThinkingConfig(model, effort) {
  const m = String(model || "").toLowerCase();
  const e = String(effort || GEMINI_DEFAULT_REASONING_EFFORT || "medium").toLowerCase();

  if (m.startsWith("gemini-3")) {
    const level =
      e === "low" ? "low" :
      e === "high" ? "high" :
      "medium";

    return {
      thinkingConfig: {
        thinkingLevel: level
      }
    };
  }

  if (m.startsWith("gemini-2.5")) {
    const budget =
      e === "low" ? 2048 :
      e === "high" ? 16384 :
      8192;

    return {
      thinkingConfig: {
        thinkingBudget: budget
      }
    };
  }

  return {};
}

async function fetchJsonWithTimeout(url, options = {}, timeoutMs = GEMINI_HTTP_TIMEOUT_MS) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), Math.max(1000, Number(timeoutMs) || GEMINI_HTTP_TIMEOUT_MS));

  try {
    return await fetch(url, { ...options, signal: controller.signal });
  } catch (error) {
    if (error && error.name === "AbortError") {
      throw new Error(`Request timed out after ${Math.round((Math.max(1000, Number(timeoutMs) || GEMINI_HTTP_TIMEOUT_MS)) / 1000)}s`);
    }
    throw error;
  } finally {
    clearTimeout(timer);
  }
}

async function parseGeminiStreamResponse(res, callbacks = {}) {
  const bodyStream = res.body;
  if (!bodyStream) {
    throw new Error("Gemini API returned no response body for stream.");
  }

  const decoder = new TextDecoder("utf-8");
  let buffer = "";
  let fullText = "";
  let finalResponse = null;
  let lastTextResponse = null;
  let eventCount = 0;
  let parseErrorCount = 0;
  const rawEvents = [];

  async function processEventBlock(block) {
    const lines = String(block || "").split(/\r?\n/);
    const dataLines = [];

    for (const line of lines) {
      if (line.startsWith("data:")) {
        dataLines.push(line.slice(5).trimStart());
      }
    }

    if (!dataLines.length) return;

    const dataStr = dataLines.join("\n").trim();
    if (!dataStr || dataStr === "[DONE]") return;

    eventCount += 1;
    if (rawEvents.length < 8) rawEvents.push(dataStr.slice(0, 1200));

    let data;
    try {
      data = JSON.parse(dataStr);
    } catch {
      parseErrorCount += 1;
      return;
    }

    if (callbacks.onEvent) {
      await callbacks.onEvent(data);
    }

    const eventText = extractGeminiText(data);
    if (eventText) {
      if (!fullText) {
        fullText = eventText;
      } else if (eventText !== fullText) {
        if (eventText.startsWith(fullText)) {
          fullText = eventText;
        } else {
          fullText += eventText;
        }
      }

      lastTextResponse = data;
      if (callbacks.onTextDelta) {
        await callbacks.onTextDelta(eventText, fullText, data);
      }
    }

    finalResponse = data;
  }

  for await (const chunk of bodyStream) {
    buffer += decoder.decode(chunk, { stream: true });
    const parts = buffer.split(/\r?\n\r?\n/);
    buffer = parts.pop() || "";

    for (const part of parts) {
      await processEventBlock(part);
    }
  }

  if (buffer.trim()) {
    await processEventBlock(buffer);
  }

  return {
    data: lastTextResponse || finalResponse,
    text: String(fullText || "").trim(),
    diagnostics: {
      eventCount,
      parseErrorCount,
      rawEvents
    }
  };
}

async function callGemini(apiKey, {
  model,
  maxTokens,
  system,
  userContent,
  effort,
  verbosity,
  timeoutMs,
  stream = true,
  jsonMode = false,
  onEvent,
  onTextDelta
}) {
  const resolvedMaxTokens = Number(maxTokens || GEMINI_DEFAULT_MAX_OUTPUT_TOKENS);
  const verbosityInstruction = mapVerbosityInstruction(verbosity);
  const thinkingPatch = mapGeminiThinkingConfig(model, effort);

  const body = {
    systemInstruction: {
      parts: [
        {
          text: `${String(system || "").trim()}\n\nOUTPUT STYLE REQUIREMENT:\n${verbosityInstruction}`
        }
      ]
    },
    contents: [
      {
        role: "user",
        parts: normalizeGeminiContentBlocks(userContent)
      }
    ],
    generationConfig: {
      maxOutputTokens: resolvedMaxTokens,
      temperature: 0.2,
      candidateCount: 1,
      ...(jsonMode ? { responseMimeType: "application/json" } : {}),
      ...thinkingPatch
    }
  };

  const encodedModel = encodeURIComponent(String(model || "").trim());
  const endpoint = stream
    ? `${GEMINI_API_BASE}/${encodedModel}:streamGenerateContent?alt=sse&key=${encodeURIComponent(apiKey)}`
    : `${GEMINI_API_BASE}/${encodedModel}:generateContent?key=${encodeURIComponent(apiKey)}`;

  const res = await fetchJsonWithTimeout(endpoint, {
    method: "POST",
    headers: {
      "Content-Type": "application/json"
    },
    body: JSON.stringify(body)
  }, timeoutMs);

  if (!res.ok) {
    const raw = await res.text();
    let data = null;
    try {
      data = raw ? JSON.parse(raw) : null;
    } catch (_) {}
    throw new Error(data?.error?.message || `Gemini API error (${res.status})`);
  }

  if (stream) {
    const streamed = await parseGeminiStreamResponse(res, { onEvent, onTextDelta });
    const data = streamed.data;
    const responseText =
      String(streamed.text || "").trim() ||
      extractGeminiText(data);

    if (!responseText) {
      const err = new Error(
        `Empty response from Gemini (stream events=${streamed?.diagnostics?.eventCount || 0}, parseErrors=${streamed?.diagnostics?.parseErrorCount || 0})`
      );
      err.phase = "gemini_stream";
      err.details = JSON.stringify(streamed?.diagnostics || {}).slice(0, 4000);
      throw err;
    }

    return {
      text: responseText,
      usage: mapGeminiUsage(data?.usageMetadata)
    };
  }

  const raw = await res.text();
  let data = null;
  try {
    data = raw ? JSON.parse(raw) : null;
  } catch (parseError) {
    throw new Error(`Gemini API returned non-JSON response (${res.status}): ${raw.slice(0, 500)}`);
  }

  const responseText = extractGeminiText(data);
  if (!responseText) throw new Error("Empty response from Gemini");

  return {
    text: responseText,
    usage: mapGeminiUsage(data?.usageMetadata)
  };
}

function ensureLiveProgress(progress) {
  if (!progress || typeof progress !== "object") return null;
  if (!progress.live || typeof progress.live !== "object") {
    progress.live = {
      stage: null,
      model: null,
      label: null,
      detail: null,
      streamingText: "",
      streamBytes: 0,
      events: [],
      updatedAt: Date.now()
    };
  }
  if (!Array.isArray(progress.live.events)) progress.live.events = [];
  return progress.live;
}

function trimStreamingPreview(text, maxChars = GEMINI_PROGRESS_STREAM_PREVIEW_LIMIT) {
  const value = String(text || "");
  if (value.length <= maxChars) return value;
  return value.slice(-maxChars);
}

function appendLiveEvent(progress, event) {
  const live = ensureLiveProgress(progress);
  const item = {
    id: `${Date.now()}_${Math.random().toString(36).slice(2, 8)}`,
    time: Date.now(),
    type: event?.type || "info",
    label: event?.label || "Update",
    detail: event?.detail || "",
    stage: event?.stage || live.stage || null,
    model: event?.model || live.model || null
  };
  live.events.push(item);
  if (live.events.length > GEMINI_PROGRESS_EVENTS_LIMIT) {
    live.events = live.events.slice(-GEMINI_PROGRESS_EVENTS_LIMIT);
  }
  live.updatedAt = Date.now();
  return item;
}

function updateLiveState(progress, patch = {}) {
  const live = ensureLiveProgress(progress);
  Object.assign(live, patch || {});
  live.updatedAt = Date.now();
  return live;
}

function createProgressTelemetry(bucket, projectPath, progress) {
  let lastPersistAt = 0;
  let lastStreamPersistAt = 0;
  let lastStreamPersistLen = 0;
  let pendingPersistPromise = null;

  async function persist(force = false) {
    const now = Date.now();
    const minGapMs = force ? 2000 : 8000;
    if (now - lastPersistAt < minGapMs) return;
    if (pendingPersistPromise) return pendingPersistPromise;

    pendingPersistPromise = (async () => {
      await saveProgress(bucket, projectPath, progress);
      lastPersistAt = Date.now();
    })();

    try {
      await pendingPersistPromise;
    } finally {
      pendingPersistPromise = null;
    }
  }

  return {
    async event(label, detail = "", options = {}) {
      appendLiveEvent(progress, { label, detail, ...options });
      if (options?.patch) updateLiveState(progress, options.patch);
      await persist(false);
    },

    async patch(patch = {}, force = false) {
      updateLiveState(progress, patch);
      await persist(force);
    },

    async stream(delta, aggregateText, options = {}) {
      const live = updateLiveState(progress, {
        stage: options.stage || progress.live?.stage || null,
        label: options.label || progress.live?.label || null,
        detail: options.detail || progress.live?.detail || null,
        model: options.model || progress.live?.model || null,
        streamingText: trimStreamingPreview(aggregateText),
        streamBytes: Number(progress.live?.streamBytes || 0) + String(delta || "").length
      });

      const now = Date.now();
      const currentLen = String(live.streamingText || "").length;
      const shouldPersist =
        (now - lastStreamPersistAt >= GEMINI_PROGRESS_STREAM_FLUSH_MS) ||
        (currentLen - lastStreamPersistLen >= GEMINI_PROGRESS_STREAM_MIN_CHARS);

      if (shouldPersist) {
        await persist(false);
        lastStreamPersistAt = now;
        lastStreamPersistLen = currentLen;
      }
    },

    async clearStream() {
      updateLiveState(progress, { streamingText: "", streamBytes: 0 });
      await persist(false);
    }
  };
}

/* ── helper: strip markdown fences and prose to extract JSON ─── */
function stripFences(text) {
  let cleaned = String(text || "")
    .replace(/^```json\s*/i, "")
    .replace(/^```\s*/i, "")
    .replace(/\s*```$/i, "")
    .trim();

  const firstBrace = cleaned.indexOf("{");
  const lastBrace = cleaned.lastIndexOf("}");
  if (firstBrace > 0 && lastBrace > firstBrace) {
    cleaned = cleaned.substring(firstBrace, lastBrace + 1);
  }

  return cleaned.trim();
}

const REQUIRED_TRANCHE_VALIDATION_BLOCK = `VALIDATION MANIFEST RULE (copy this block verbatim into EVERY tranche prompt you generate):
---
MANDATORY VALIDATION MANIFEST: Every file you output MUST contain a machine-readable
manifest block embedded as a comment near the top of the file, using these exact markers:

VALIDATION_MANIFEST_START
{
  "file": "<exact file path e.g. models/2>",
  "systems": [
    { "id": "<snake_case_system_id>", "keywords": ["keyword1", "keyword2"], "notes": "what this file implements for this system" }
  ]
}
VALIDATION_MANIFEST_END

Rules the validator enforces — your output will be REJECTED if you break them:
1. List ONLY systems you actually implement in that specific file with real executable code.
2. Each listed system MUST have nearby executable code evidence (function, class, event handler,
   loop, conditional, assignment) that uses at least one of the declared keywords.
3. Comments, strings, and variable names alone are NOT sufficient evidence.
4. Do NOT omit the markers — a file without VALIDATION_MANIFEST_START / VALIDATION_MANIFEST_END
   will fail validation and trigger an automatic repair pass.
5. This same marker format applies to EVERY file type, including json/assets.json.
   For json/assets.json, place the manifest inside a leading /* ... */ block comment at the very top,
   then put the valid JSON content immediately after it.
---`;

function countOccurrences(haystack, needle) {
  if (!needle) return 0;
  return String(haystack || "").split(needle).length - 1;
}

function assertTranchePromptHasRequiredManifestBlock(tranche, index) {
  const prompt = String(tranche?.prompt || "").replace(/\r\n/g, "\n").trim();
  const label = `tranche ${index + 1}${tranche?.name ? ` (${tranche.name})` : ""}`;

  if (!prompt) {
    throw new Error(`Pre-execution tranche manifest assertion failed for ${label}: prompt is empty.`);
  }

  const occurrenceCount = countOccurrences(prompt, REQUIRED_TRANCHE_VALIDATION_BLOCK);
  if (occurrenceCount !== 1) {
    throw new Error(`Pre-execution tranche manifest assertion failed for ${label}: expected exactly 1 verbatim validation block, found ${occurrenceCount}.`);
  }

  const requiredFragments = [
    "VALIDATION MANIFEST RULE (copy this block verbatim into EVERY tranche prompt you generate):",
    "MANDATORY VALIDATION MANIFEST: Every file you output MUST contain a machine-readable",
    "VALIDATION_MANIFEST_START",
    `"file": "<exact file path e.g. models/2>"`,
    `"systems": [`,
    `"id": "<snake_case_system_id>"`,
    `"keywords": ["keyword1", "keyword2"]`,
    `"notes": "what this file implements for this system"`,
    "VALIDATION_MANIFEST_END",
    "4. Do NOT omit the markers",
    "5. This same marker format applies to EVERY file type, including json/assets.json."
  ];

  const missingFragments = requiredFragments.filter(fragment => !prompt.includes(fragment));
  if (missingFragments.length) {
    throw new Error(`Pre-execution tranche manifest assertion failed for ${label}: missing required manifest block fragment(s): ${missingFragments.join(" | ")}`);
  }

  return true;
}

function enforceTrancheValidationBlock(plan) {
  if (!plan || !Array.isArray(plan.tranches)) {
    throw new Error("Planner output is missing tranches.");
  }

  const failures = [];
  plan.tranches = plan.tranches.map((tranche, index) => {
    const normalized = tranche && typeof tranche === "object" ? { ...tranche } : {};
    const prompt = String(normalized.prompt || "").trim();

    if (!prompt) {
      failures.push(`tranche ${index + 1}: empty prompt`);
      return normalized;
    }

    if (!prompt.includes(REQUIRED_TRANCHE_VALIDATION_BLOCK)) {
      normalized.prompt = `${prompt}

${REQUIRED_TRANCHE_VALIDATION_BLOCK}`;
    }

    try {
      assertTranchePromptHasRequiredManifestBlock(normalized, index);
    } catch (error) {
      failures.push(error.message);
    }

    return normalized;
  });

  if (failures.length) {
    throw new Error(`Deterministic tranche manifest assertion failed: ${failures.join("; ")}`);
  }

  return plan;
}

/* ── helper: parse tranche executor delimiter-format responses ── */
function parseDelimitedResponse(text) {
  const files = [];
  const fileRegex = /===FILE_START:\s*([^\n]+?)\s*===\n([\s\S]*?)===FILE_END:\s*\1\s*===/g;

  let match;
  while ((match = fileRegex.exec(text)) !== null) {
    const path = match[1].trim();
    const content = match[2];
    if (path && content !== undefined) {
      files.push({ path, content });
    }
  }

  const msgMatch = String(text || "").match(/===MESSAGE===\n([\s\S]*?)===END_MESSAGE===/);
  const message = msgMatch ? msgMatch[1].trim() : "Tranche completed.";

  if (files.length === 0) {
    try {
      const parsed = JSON.parse(stripFences(text));
      if (parsed && Array.isArray(parsed.updatedFiles)) {
        console.warn("Executor used JSON format instead of delimiter format — parsed as fallback.");
        return parsed;
      }
    } catch (_) {}
    return null;
  }

  return { updatedFiles: files, message };
}

/* ── helper: save progress to Firebase ───────────────────────── */
async function saveProgress(bucket, projectPath, progress) {
  if (progress && typeof progress === "object") {
    progress.updatedAt = Date.now();
    ensureLiveProgress(progress);
  }

  await bucket.file(`${projectPath}/ai_progress.json`).save(
    JSON.stringify(progress),
    { contentType: "application/json", resumable: false }
  );
}

/* ── helper: save ai_response.json with freshness metadata ───── */
async function saveAiResponse(bucket, projectPath, allUpdatedFiles, meta = {}) {
  const payload = {
    jobId: meta.jobId || "unknown",
    timestamp: Date.now(),
    trancheIndex: meta.trancheIndex !== undefined ? meta.trancheIndex : null,
    totalTranches: meta.totalTranches || null,
    status: meta.status || "checkpoint",
    message: meta.message || "",
    updatedFiles: allUpdatedFiles || []
  };

  await bucket.file(`${projectPath}/ai_response.json`).save(
    JSON.stringify(payload),
    { contentType: "application/json", resumable: false }
  );
}

/* ── helper: save pipeline state to Firebase ─────────────────── */
async function savePipelineState(bucket, projectPath, state) {
  await bucket.file(`${projectPath}/ai_pipeline_state.json`).save(
    JSON.stringify(state),
    { contentType: "application/json", resumable: false }
  );
}

/* ── helper: load pipeline state from Firebase ───────────────── */
async function loadPipelineState(bucket, projectPath) {
  const file = bucket.file(`${projectPath}/ai_pipeline_state.json`);
  const [exists] = await file.exists();
  if (!exists) return null;
  const [content] = await file.download();
  return JSON.parse(content.toString());
}

/* ── helper: check kill switch ───────────────────────────────── */
async function checkKillSwitch(bucket, projectPath, jobId) {
  try {
    const activeJobFile = bucket.file(`${projectPath}/ai_active_job.json`);
    const [exists] = await activeJobFile.exists();

    if (exists) {
      const [content] = await activeJobFile.download();
      const activeData = JSON.parse(content.toString());

      if (activeData.jobId && activeData.jobId !== jobId) {
        return { killed: true, reason: "superseded", newJobId: activeData.jobId };
      }

      if (activeData.cancelled) {
        return { killed: true, reason: "cancelled" };
      }
    }
  } catch (e) {}

  return { killed: false };
}

/* ── helper: self-chain — invoke this function again ─────────── */
async function chainToSelf(payload) {
  const siteUrl = process.env.URL || process.env.DEPLOY_URL || "";
  const chainUrl = `${siteUrl}/.netlify/functions/geminiCodeProxy-background`;

  console.log(`CHAIN → next step: mode=${payload.mode}, tranche=${payload.nextTranche ?? "n/a"} → ${chainUrl}`);

  try {
    const res = await fetchJsonWithTimeout(chainUrl, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload)
    }, GEMINI_CHAIN_ACCEPT_TIMEOUT_MS);

    const responseText = await res.text().catch(() => "");
    console.log(`Chain response status: ${res.status}`);

    if (!res.ok) {
      const err = new Error(`Self-chain failed: HTTP ${res.status} ${res.statusText || ""}`.trim());
      err.status = res.status;
      err.phase = "self_chain";
      err.responseBody = responseText;
      throw err;
    }
  } catch (err) {
    console.error("Chain invocation failed:", err.message);
    if (!err.phase) err.phase = "self_chain";
    throw err;
  }
}

function buildSerializableErrorPayload(error, context = {}) {
  const payload = {
    error: error?.message || String(error || "Unknown error"),
    name: error?.name || "Error",
    stack: typeof error?.stack === "string" ? error.stack : null,
    context: context || {},
    timestamp: new Date().toISOString()
  };

  if (error && typeof error === "object") {
    if (typeof error.status === "number") payload.status = error.status;
    if (typeof error.statusCode === "number") payload.statusCode = error.statusCode;
    if (typeof error.phase === "string") payload.phase = error.phase;
    if (typeof error.details === "string") payload.details = error.details;
    if (error.responseBody != null) payload.responseBody = String(error.responseBody).slice(0, 12000);
    if (error.raw != null) payload.raw = String(error.raw).slice(0, 12000);
  }

  return payload;
}

/* ═══════════════════════════════════════════════════════════════ */
exports.handler = async (event) => {
  let projectPath = null;
  let bucket = null;
  let jobId = null;

  try {
    bucket = admin.storage().bucket(
      process.env.FIREBASE_STORAGE_BUCKET || "gokudatabase.firebasestorage.app"
    );
  } catch (bucketInitErr) {
    console.error("CRITICAL: Firebase bucket initialization failed:", bucketInitErr);
    return {
      statusCode: 500,
      body: JSON.stringify({ error: "Firebase init failed: " + bucketInitErr.message })
    };
  }

  try {
    console.log("[geminiCodeProxy] Handler invoked. body length:", event.body ? event.body.length : 0);

    if (!event.body) throw new Error("Missing request body");

    const parsedBody = JSON.parse(event.body);
    projectPath = parsedBody.projectPath;
    jobId = parsedBody.jobId;

    console.log(`[geminiCodeProxy] projectPath=${projectPath} jobId=${jobId}`);

    if (!projectPath) throw new Error("Missing projectPath");
    if (!jobId) throw new Error("Missing jobId");

    const apiKey = process.env.GEMINI_API_KEY;
    if (!apiKey) throw new Error("Missing GEMINI_API_KEY — set this env var in Netlify dashboard");

    console.log(`[geminiCodeProxy] API key present (length ${apiKey.length}). Bucket ready.`);

    const mode = parsedBody.mode || "plan";
    const nextTranche = parsedBody.nextTranche || 0;

    /* ════════════════════════════════════════════════════════════
       MODE: "plan"
       ════════════════════════════════════════════════════════════ */
    if (mode === "plan") {
      const requestFile = bucket.file(`${projectPath}/ai_request.json`);
      const [content] = await requestFile.download();
      const { prompt, files, selectedAssets, inlineImages } = JSON.parse(content.toString());

      if (!prompt) throw new Error("Missing instructions inside payload");

      let fileContext = "Here are the current project files:\n\n";
      if (files) {
        for (const [path, fileContent] of Object.entries(files)) {
          fileContext += `--- FILE: ${path} ---\n${fileContent}\n\n`;
        }
      }

      const mediaBlocks = [];

      if (selectedAssets && Array.isArray(selectedAssets) && selectedAssets.length > 0) {
        let assetContext = "\n\nThe user has designated the following files for use. Their relative paths in the project are:\n";

        for (const asset of selectedAssets) {
          assetContext += `- ${asset.path}\n`;

          const isSupportedMedia =
            (asset.type && (asset.type.startsWith("image/") || asset.type.startsWith("audio/") || asset.type.startsWith("video/"))) ||
            (asset.name && asset.name.match(/\.(png|jpe?g|webp|mp3|wav|ogg|mp4|webm)$/i));

          if (isSupportedMedia) {
            try {
              const assetRes = await fetch(asset.url);
              if (!assetRes.ok) throw new Error(`Failed to fetch: ${assetRes.statusText}`);
              const arrayBuffer = await assetRes.arrayBuffer();
              const base64Data = Buffer.from(arrayBuffer).toString("base64");

              let mime = asset.type;
              if (!mime) {
                const lower = String(asset.name || "").toLowerCase();
                if (lower.endsWith(".png")) mime = "image/png";
                else if (lower.endsWith(".jpg") || lower.endsWith(".jpeg")) mime = "image/jpeg";
                else if (lower.endsWith(".webp")) mime = "image/webp";
                else if (lower.endsWith(".mp3")) mime = "audio/mpeg";
                else if (lower.endsWith(".wav")) mime = "audio/wav";
                else if (lower.endsWith(".ogg")) mime = "audio/ogg";
                else if (lower.endsWith(".mp4")) mime = "video/mp4";
                else if (lower.endsWith(".webm")) mime = "video/webm";
                else mime = "application/octet-stream";
              }

              if (mime.startsWith("image/")) {
                mediaBlocks.push({ type: "image", source: { type: "base64", media_type: mime, data: base64Data } });
              } else if (mime.startsWith("audio/")) {
                mediaBlocks.push({ type: "audio", source: { type: "base64", media_type: mime, data: base64Data } });
              } else if (mime.startsWith("video/")) {
                mediaBlocks.push({ type: "video", source: { type: "base64", media_type: mime, data: base64Data } });
              }
            } catch (fetchErr) {
              console.error(`Failed to fetch designated media asset ${asset.name}:`, fetchErr);
            }
          } else {
            assetContext += `  (Note: ${asset.name} is a non-inline binary/model file. Reference it by path in code.)\n`;
          }
        }

        fileContext += assetContext;
      }

      if (inlineImages && Array.isArray(inlineImages) && inlineImages.length > 0) {
        for (const img of inlineImages) {
          if (img.data && img.mimeType && img.mimeType.startsWith("image/")) {
            mediaBlocks.push({
              type: "image",
              source: {
                type: "base64",
                media_type: img.mimeType,
                data: img.data
              }
            });
          }
        }
      }

      const progress = {
        jobId: jobId,
        status: "planning",
        planningStartTime: Date.now(),
        planningEndTime: null,
        planningAnalysis: "",
        totalTranches: 0,
        currentTranche: -1,
        tranches: [],
        tokenUsage: {
          planning: null,
          tranches: [],
          totals: {
            input_tokens: 0,
            output_tokens: 0,
            reasoning_tokens: 0,
            rest_output_tokens: 0
          }
        },
        finalMessage: null,
        error: null,
        completedTime: null,
        live: {
          stage: "planning",
          model: GEMINI_DEFAULT_PLANNER_MODEL,
          label: "Planning request queued",
          detail: "Preparing Game Intelligence + Expert Planning call...",
          streamingText: "",
          streamBytes: 0,
          events: [],
          updatedAt: Date.now()
        }
      };

      await saveProgress(bucket, projectPath, progress);
      const progressTelemetry = createProgressTelemetry(bucket, projectPath, progress);

      await progressTelemetry.event(
        "Planning job started",
        "Preparing Gemini planning request with full project context.",
        { stage: "planning", model: GEMINI_DEFAULT_PLANNER_MODEL }
      );

      const planningSystem = `You are an expert game development architect and AI pipeline planner.

Your job: analyze the user's game build/modification request and split it into sequential, self-contained TRANCHES that can be executed one at a time by a coding AI.

RULES FOR SPLITTING:
1. Each tranche should focus on 1-2 closely related concerns (e.g., "physics + movement", "UI + scoring", "pipe spawning + scrolling").
2. Tranches MUST be ordered by dependency — later tranches build on earlier ones.
3. Each tranche prompt must be FULLY SELF-CONTAINED: include all the context, rules, and specifics the coding AI needs without referencing other tranches by name.
4. Preserve ALL technical details, variable names, slot layouts, exact code snippets, and architecture rules from the original prompt in the relevant tranche(s). Do NOT summarize or lose any detail.
5. If the prompt is simple enough (minor change, single concern), use just 1 tranche.
6. For complex game builds, use 3-7 tranches. Never exceed 8.
7. Each tranche should describe what FILES it expects to create or modify.
8. The FIRST tranche should always set up the foundational scaffold that later tranches build upon.
9. The LAST tranche should handle polish, edge cases, and integration glue.

CRITICAL FILE NAMING RULES (include in every tranche prompt):
- The main logic file is named "2" (NOT "WorldController.js"), located in "models/" folder.
- The main HTML file is named "23" (NOT "document.html"), located in "models/" folder.
- "assets.json" is in the "json/" folder.

NOTE: Do NOT include validation manifest blocks in the tranche prompts you generate. Those are injected automatically server-side.

You must respond ONLY with a valid JSON object. No markdown, no code fences, no preamble.

{
  "analysis": "Brief 1-2 sentence analysis of the overall request complexity and your splitting strategy.",
  "tranches": [
    {
      "name": "Short Name",
      "description": "2-3 sentence description of what this tranche accomplishes.",
      "expertAgents": ["agent_id_1", "agent_id_2"],
      "phase": 1,
      "dependencies": [],
      "qualityCriteria": ["Criterion 1", "Criterion 2"],
      "prompt": "THE COMPLETE, SELF-CONTAINED PROMPT for the coding AI. Include all relevant technical details.",
      "expectedFiles": ["models/2", "models/23"]
    }
  ]
}`;

      const planningUserContent = [
        {
          type: "text",
          text: `${fileContext}\n\n=== FULL USER REQUEST (analyze and split into tranches) ===\n${prompt}\n=== END USER REQUEST ===`
        },
        ...mediaBlocks
      ];

      console.log(`[geminiCodeProxy] STAGE 0: Calling Gemini planner — model=${GEMINI_DEFAULT_PLANNER_MODEL} maxTokens=${GEMINI_DEFAULT_PLANNER_MAX_OUTPUT_TOKENS} timeout=${Math.round(GEMINI_PLANNER_HTTP_TIMEOUT_MS / 1000)}s job=${jobId}`);

      await progressTelemetry.event(
        "Planning request sent",
        `Model ${GEMINI_DEFAULT_PLANNER_MODEL} with ${GEMINI_DEFAULT_PLANNER_REASONING_EFFORT} reasoning and ${GEMINI_DEFAULT_PLANNER_TEXT_VERBOSITY} verbosity.`,
        {
          stage: "planning",
          model: GEMINI_DEFAULT_PLANNER_MODEL,
          patch: {
            stage: "planning",
            model: GEMINI_DEFAULT_PLANNER_MODEL,
            label: "Waiting for planner response",
            detail: `Timeout ${Math.round(GEMINI_PLANNER_HTTP_TIMEOUT_MS / 1000)}s • max output ${GEMINI_DEFAULT_PLANNER_MAX_OUTPUT_TOKENS}`
          }
        }
      );

      let plannerSawFirstToken = false;
      const planResult = await callGemini(apiKey, {
        model: GEMINI_DEFAULT_PLANNER_MODEL,
        maxTokens: GEMINI_DEFAULT_PLANNER_MAX_OUTPUT_TOKENS,
        effort: GEMINI_DEFAULT_PLANNER_REASONING_EFFORT,
        verbosity: GEMINI_DEFAULT_PLANNER_TEXT_VERBOSITY,
        timeoutMs: GEMINI_PLANNER_HTTP_TIMEOUT_MS,
        system: planningSystem,
        userContent: planningUserContent,
        stream: true,
        jsonMode: true,
        onEvent: async () => {
          await progressTelemetry.patch(
            {
              stage: "planning",
              model: GEMINI_DEFAULT_PLANNER_MODEL,
              label: "Planner response created",
              detail: "Gemini accepted the planning request."
            },
            true
          );
        },
        onTextDelta: async (delta, aggregateText) => {
          if (!plannerSawFirstToken) {
            plannerSawFirstToken = true;
            await progressTelemetry.event(
              "Planner streaming live output",
              "Realtime planner text is now flowing into the AI Context Window.",
              {
                stage: "planning",
                model: GEMINI_DEFAULT_PLANNER_MODEL,
                patch: {
                  stage: "planning",
                  model: GEMINI_DEFAULT_PLANNER_MODEL,
                  label: "Planner is reasoning live",
                  detail: "Streaming planning text from Gemini..."
                }
              }
            );
          }

          await progressTelemetry.stream(delta, aggregateText, {
            stage: "planning",
            model: GEMINI_DEFAULT_PLANNER_MODEL,
            label: "Planner is reasoning live",
            detail: "Streaming planning text from Gemini..."
          });
        }
      });

      await progressTelemetry.clearStream(true);

      await progressTelemetry.event(
        "Planning response received",
        "Planner finished streaming. Validating tranche plan JSON...",
        {
          stage: "planning",
          model: GEMINI_DEFAULT_PLANNER_MODEL,
          patch: {
            stage: "planning",
            model: GEMINI_DEFAULT_PLANNER_MODEL,
            label: "Validating planner output",
            detail: "Parsing returned JSON plan..."
          }
        }
      );

      if (planResult.usage) {
        progress.tokenUsage.planning = planResult.usage;
        progress.tokenUsage.totals.input_tokens += planResult.usage.input_tokens || 0;
        progress.tokenUsage.totals.output_tokens += planResult.usage.output_tokens || 0;
        progress.tokenUsage.totals.reasoning_tokens += planResult.usage.output_budget?.reasoning_tokens || 0;
        progress.tokenUsage.totals.rest_output_tokens += planResult.usage.output_budget?.rest_output_tokens || 0;
        await saveProgress(bucket, projectPath, progress);
      }

      let plan;
      try {
        plan = JSON.parse(stripFences(planResult.text));
      } catch (e) {
        let recovered = null;
        try {
          const cleaned = stripFences(planResult.text);
          const lastBrace = cleaned.lastIndexOf("}");
          if (lastBrace > 0) {
            for (let i = lastBrace; i > 0; i--) {
              if (cleaned[i] !== "}") continue;
              try {
                const candidate = JSON.parse(cleaned.substring(0, i + 1) + "]}");
                if (candidate?.tranches?.length > 0) {
                  recovered = candidate;
                  break;
                }
              } catch (_) {}
            }
          }
        } catch (_) {}

        if (!recovered || !Array.isArray(recovered.tranches) || recovered.tranches.length === 0) {
          throw new Error("Failed to parse planning output as JSON: " + e.message);
        }

        console.warn(`Planning JSON was truncated — recovered ${recovered.tranches.length} tranche(s) via repair.`);
        plan = recovered;
      }

      if (!plan.tranches || !Array.isArray(plan.tranches) || plan.tranches.length === 0) {
        throw new Error("Planner returned zero tranches.");
      }

      plan = enforceTrancheValidationBlock(plan);

      await progressTelemetry.event(
        "Planning JSON validated",
        `Created ${plan.tranches.length} tranche(s).`,
        { stage: "planning", model: GEMINI_DEFAULT_PLANNER_MODEL }
      );

      progress.status = "executing";
      progress.planningEndTime = Date.now();
      progress.planningAnalysis = plan.analysis || "";
      progress.totalTranches = plan.tranches.length;
      progress.currentTranche = 0;
      progress.tranches = plan.tranches.map((t, i) => ({
        index: i,
        name: t.name,
        description: t.description,
        expertAgents: t.expertAgents || [],
        phase: t.phase || 0,
        dependencies: t.dependencies || [],
        qualityCriteria: t.qualityCriteria || [],
        prompt: t.prompt,
        expectedFiles: t.expectedFiles || [],
        status: "pending",
        startTime: null,
        endTime: null,
        message: null,
        filesUpdated: []
      }));

      await progressTelemetry.patch(
        {
          stage: "executing",
          model: GEMINI_DEFAULT_EXECUTOR_MODEL,
          label: `Plan locked: ${plan.tranches.length} tranche(s)`,
          detail: "Chaining into tranche execution...",
          streamingText: "",
          streamBytes: 0
        },
        true
      );

      console.log(`Plan created: ${plan.tranches.length} tranches.`);

      const pipelineState = {
        jobId,
        projectPath,
        progress,
        accumulatedFiles: files ? { ...files } : {},
        allUpdatedFiles: [],
        mediaBlocks,
        totalTranches: plan.tranches.length
      };

      await savePipelineState(bucket, projectPath, pipelineState);

      await chainToSelf({
        projectPath,
        jobId,
        mode: "tranche",
        nextTranche: 0
      });

      return {
        statusCode: 200,
        body: JSON.stringify({ success: true, chained: true, phase: "planning_complete" })
      };
    }

    /* ════════════════════════════════════════════════════════════
       MODE: "tranche"
       ════════════════════════════════════════════════════════════ */
    if (mode === "tranche") {
      const killCheck = await checkKillSwitch(bucket, projectPath, jobId);

      if (killCheck.killed) {
        if (killCheck.reason === "superseded") {
          console.log(`Job ${jobId} superseded by ${killCheck.newJobId}. Terminating chain.`);
          return { statusCode: 200, body: JSON.stringify({ success: true, superseded: true }) };
        }

        if (killCheck.reason === "cancelled") {
          console.log("Cancellation signal detected — aborting chain.");
          const state = await loadPipelineState(bucket, projectPath);

          if (state) {
            const activeJobFile = bucket.file(`${projectPath}/ai_active_job.json`);
            await activeJobFile.delete().catch(() => {});
            state.progress.status = "cancelled";
            state.progress.finalMessage = `Pipeline cancelled by user after ${nextTranche} tranche(s).`;
            state.progress.completedTime = Date.now();
            await saveProgress(bucket, projectPath, state.progress);

            if (state.allUpdatedFiles.length > 0) {
              await saveAiResponse(bucket, projectPath, state.allUpdatedFiles, {
                jobId: state.jobId,
                trancheIndex: nextTranche,
                totalTranches: state.totalTranches,
                status: "cancelled",
                message: `Pipeline cancelled. ${state.allUpdatedFiles.length} file(s) were updated before cancellation.`
              });
            }
          }

          return { statusCode: 200, body: JSON.stringify({ success: true, cancelled: true }) };
        }
      }

      const state = await loadPipelineState(bucket, projectPath);
      if (!state) throw new Error("Pipeline state not found in Firebase. Chain broken.");

      const { progress, accumulatedFiles, allUpdatedFiles, mediaBlocks } = state;
      const tranche = progress.tranches[nextTranche];

      if (!tranche) throw new Error(`Tranche ${nextTranche} not found in pipeline state.`);

      const progressTelemetry = createProgressTelemetry(bucket, projectPath, progress);

      progress.currentTranche = nextTranche;
      progress.tranches[nextTranche].status = "in_progress";
      progress.tranches[nextTranche].startTime = Date.now();
      await saveProgress(bucket, projectPath, progress);

      console.log(`TRANCHE ${nextTranche + 1}/${progress.totalTranches}: ${tranche.name} (Job ${jobId})`);

      const executionSystem = `You are an expert game development AI.
The user will provide project files and a focused modification request (one tranche of a larger build).

You must respond using DELIMITER FORMAT only. Do NOT use JSON. Do NOT use markdown code blocks.

For each file you update or create, output it like this:

===FILE_START: path/to/filename===
...complete raw file content here, exactly as it should be saved...
===FILE_END: path/to/filename===

After all files, add a message block:

===MESSAGE===
A detailed explanation of what you implemented in this tranche, including specific functions, variables, and logic you added or changed.
===END_MESSAGE===

EXAMPLE (two files updated):
===FILE_START: models/2===
// full JS content here
===FILE_END: models/2===

===FILE_START: models/23===
<!DOCTYPE html>...full HTML here...
===FILE_END: models/23===

===MESSAGE===
Added physics body initialization and collision handler registration.
===END_MESSAGE===

CRITICAL RULES:
- Only include files that actually need to be changed or created.
- The main logic file is named "2" in the "models" folder. Never use "WorldController.js".
- The main HTML file is named "23" in the "models" folder. Never use "document.html".
- "assets.json" is in the "json" folder.
- Always output the COMPLETE file content for each updated file — not patches or diffs.
- Build upon the existing file contents provided. Do NOT discard or overwrite work from prior tranches.
- If the file already has functions, variables, or structures from prior tranches, KEEP THEM ALL and add your new code alongside them.
- The delimiter lines (===FILE_START:=== etc.) must appear exactly as shown, on their own lines.

MANDATORY VALIDATION MANIFEST (your output will be REJECTED without this):
Every file you output MUST contain a machine-readable manifest block embedded as a comment
near the top of the file content, using these exact markers on their own lines:

VALIDATION_MANIFEST_START
{
  "file": "<exact file path matching the FILE_START delimiter, e.g. models/2>",
  "systems": [
    { "id": "<snake_case_system_id>", "keywords": ["keyword1", "keyword2"], "notes": "what this file implements for this system" }
  ]
}
VALIDATION_MANIFEST_END

Enforcement rules — the downstream validator will REJECT your file and trigger a repair pass if:
1. The VALIDATION_MANIFEST_START / VALIDATION_MANIFEST_END block is missing from any output file.
2. A declared system has no nearby executable code evidence (function body, class method,
   event handler, loop, conditional, assignment) that uses at least one of its declared keywords.
3. You declare a system that only appears in comments, strings, or variable names — not in logic.
4. The manifest JSON is malformed or unparseable.

Correct approach:
- After implementing each system in real code, add its entry to the manifest.
- Use keywords that literally appear in your function/variable/event names for that system.
- Only list systems you genuinely implement in THIS file — not aspirational or planned ones.
- For models/2 (JS): embed the manifest inside a block comment /* VALIDATION_MANIFEST_START ... VALIDATION_MANIFEST_END */
- For models/23 (HTML): embed the manifest inside an HTML comment <!-- VALIDATION_MANIFEST_START ... VALIDATION_MANIFEST_END -->
- For json/assets.json: use the exact same VALIDATION_MANIFEST_START / VALIDATION_MANIFEST_END block inside a leading /* ... */ comment at the very top, then place the valid JSON body immediately after the comment.`;

      let trancheFileContext = "Here are the current project files (includes all output from prior tranches — you MUST preserve all existing code):\n\n";
      for (const [path, fileContent] of Object.entries(accumulatedFiles)) {
        trancheFileContext += `--- FILE: ${path} ---\n${fileContent}\n\n`;
      }

      assertTranchePromptHasRequiredManifestBlock(tranche, nextTranche);

      const trancheUserContent = [
        {
          type: "text",
          text: `${trancheFileContext}\n\n=== TRANCHE ${nextTranche + 1} of ${progress.totalTranches}: "${tranche.name}" ===\n\n${tranche.prompt}\n\n=== END TRANCHE INSTRUCTIONS ===\n\nIMPORTANT: You are working on tranche ${nextTranche + 1} of ${progress.totalTranches}. The project files above contain ALL work from prior tranches. You MUST preserve all existing code and ADD your changes on top. Output the COMPLETE updated file contents.`
        },
        ...(mediaBlocks || [])
      ];

      let trancheResponseObj;

      await progressTelemetry.event(
        `Tranche ${nextTranche + 1} started`,
        tranche.name || `Executing tranche ${nextTranche + 1}.`,
        {
          stage: "executing",
          model: GEMINI_DEFAULT_EXECUTOR_MODEL,
          patch: {
            stage: "executing",
            model: GEMINI_DEFAULT_EXECUTOR_MODEL,
            label: `Executing tranche ${nextTranche + 1}/${progress.totalTranches}`,
            detail: tranche.name || tranche.description || "Running executor...",
            currentTranche: nextTranche
          }
        }
      );

      let executorSawFirstToken = false;

      try {
        trancheResponseObj = await callGemini(apiKey, {
          model: GEMINI_DEFAULT_EXECUTOR_MODEL,
          maxTokens: GEMINI_DEFAULT_EXECUTOR_MAX_OUTPUT_TOKENS,
          effort: GEMINI_DEFAULT_EXECUTOR_REASONING_EFFORT,
          verbosity: GEMINI_DEFAULT_EXECUTOR_TEXT_VERBOSITY,
          timeoutMs: GEMINI_HTTP_TIMEOUT_MS,
          system: executionSystem,
          userContent: trancheUserContent,
          stream: true,
          jsonMode: false,
          onEvent: async () => {
            await progressTelemetry.patch(
              {
                stage: "executing",
                model: GEMINI_DEFAULT_EXECUTOR_MODEL,
                label: `Tranche ${nextTranche + 1}/${progress.totalTranches} response created`,
                detail: tranche.name || tranche.description || "Executor accepted by Gemini.",
                currentTranche: nextTranche
              },
              true
            );
          },
          onTextDelta: async (delta, aggregateText) => {
            if (!executorSawFirstToken) {
              executorSawFirstToken = true;
              await progressTelemetry.event(
                `Tranche ${nextTranche + 1} streaming live output`,
                "Realtime executor text is now flowing into the AI Context Window.",
                {
                  stage: "executing",
                  model: GEMINI_DEFAULT_EXECUTOR_MODEL,
                  patch: {
                    stage: "executing",
                    model: GEMINI_DEFAULT_EXECUTOR_MODEL,
                    label: `Streaming tranche ${nextTranche + 1}/${progress.totalTranches}`,
                    detail: tranche.name || tranche.description || "Receiving streamed executor output...",
                    currentTranche: nextTranche
                  }
                }
              );
            }

            await progressTelemetry.stream(delta, aggregateText, {
              stage: "executing",
              model: GEMINI_DEFAULT_EXECUTOR_MODEL,
              label: `Streaming tranche ${nextTranche + 1}/${progress.totalTranches}`,
              detail: tranche.name || tranche.description || "Receiving streamed executor output...",
              currentTranche: nextTranche
            });
          }
        });

        await progressTelemetry.clearStream(true);

        await progressTelemetry.event(
          `Tranche ${nextTranche + 1} response received`,
          "Parsing streamed executor payload and merging files...",
          {
            stage: "executing",
            model: GEMINI_DEFAULT_EXECUTOR_MODEL,
            patch: {
              stage: "executing",
              model: GEMINI_DEFAULT_EXECUTOR_MODEL,
              label: `Parsing tranche ${nextTranche + 1}/${progress.totalTranches}`,
              detail: "Validating delimiter blocks and file payloads...",
              currentTranche: nextTranche
            }
          }
        );
      } catch (err) {
        progress.tranches[nextTranche].status = "error";
        progress.tranches[nextTranche].endTime = Date.now();
        progress.tranches[nextTranche].message = `Error: ${err.message}`;

        await progressTelemetry.event(
          `Tranche ${nextTranche + 1} failed`,
          err.message,
          {
            stage: "executing",
            model: GEMINI_DEFAULT_EXECUTOR_MODEL,
            type: "error",
            patch: {
              label: `Tranche ${nextTranche + 1} failed`,
              detail: err.message,
              currentTranche: nextTranche
            }
          }
        );

        console.error(`Tranche ${nextTranche + 1} failed:`, err.message);

        state.progress = progress;
        await savePipelineState(bucket, projectPath, state);

        if (allUpdatedFiles.length > 0) {
          await saveAiResponse(bucket, projectPath, allUpdatedFiles, {
            jobId: jobId,
            trancheIndex: nextTranche,
            totalTranches: progress.totalTranches,
            status: "checkpoint",
            message: `Checkpoint after tranche ${nextTranche + 1} error-skip. ${allUpdatedFiles.length} file(s) so far.`
          });
        }

        if (nextTranche + 1 < progress.totalTranches) {
          await chainToSelf({ projectPath, jobId, mode: "tranche", nextTranche: nextTranche + 1 });
          return { statusCode: 200, body: JSON.stringify({ success: true, chained: true, phase: `tranche_${nextTranche}_error_skipped` }) };
        }
      }

      if (trancheResponseObj) {
        if (trancheResponseObj.usage) {
          progress.tokenUsage.tranches[nextTranche] = trancheResponseObj.usage;
          progress.tokenUsage.totals.input_tokens += trancheResponseObj.usage.input_tokens || 0;
          progress.tokenUsage.totals.output_tokens += trancheResponseObj.usage.output_tokens || 0;
          progress.tokenUsage.totals.reasoning_tokens += trancheResponseObj.usage.output_budget?.reasoning_tokens || 0;
          progress.tokenUsage.totals.rest_output_tokens += trancheResponseObj.usage.output_budget?.rest_output_tokens || 0;
          progress.tranches[nextTranche].tokenUsage = trancheResponseObj.usage;
        }

        const trancheResult = parseDelimitedResponse(trancheResponseObj.text);

        if (!trancheResult) {
          progress.tranches[nextTranche].status = "error";
          progress.tranches[nextTranche].endTime = Date.now();
          progress.tranches[nextTranche].message = "Executor returned no recognisable file delimiters or valid JSON fallback.";

          await progressTelemetry.event(
            `Tranche ${nextTranche + 1} parse failure`,
            "Executor returned no delimiter payload. Skipping to keep pipeline alive.",
            {
              stage: "executing",
              model: GEMINI_DEFAULT_EXECUTOR_MODEL,
              type: "warn",
              patch: {
                label: `Tranche ${nextTranche + 1} parse failure`,
                detail: "No valid delimiter output returned.",
                currentTranche: nextTranche
              }
            }
          );

          console.error(`Tranche ${nextTranche + 1} produced no parseable output.`);
          console.error("Raw response (first 500 chars):", trancheResponseObj.text.slice(0, 500));

          state.progress = progress;
          await savePipelineState(bucket, projectPath, state);

          if (allUpdatedFiles.length > 0) {
            await saveAiResponse(bucket, projectPath, allUpdatedFiles, {
              jobId: jobId,
              trancheIndex: nextTranche,
              totalTranches: progress.totalTranches,
              status: "checkpoint",
              message: `Checkpoint after tranche ${nextTranche + 1} parse-error skip. ${allUpdatedFiles.length} file(s) so far.`
            });
          }

          if (nextTranche + 1 < progress.totalTranches) {
            await chainToSelf({ projectPath, jobId, mode: "tranche", nextTranche: nextTranche + 1 });
            return { statusCode: 200, body: JSON.stringify({ success: true, chained: true, phase: `tranche_${nextTranche}_parse_error` }) };
          }
        }

        if (trancheResult) {
          const trancheFilesUpdated = [];

          if (trancheResult.updatedFiles && Array.isArray(trancheResult.updatedFiles)) {
            for (const file of trancheResult.updatedFiles) {
              accumulatedFiles[file.path] = file.content;
              trancheFilesUpdated.push(file.path);

              const existingIdx = allUpdatedFiles.findIndex(f => f.path === file.path);
              if (existingIdx >= 0) {
                allUpdatedFiles[existingIdx] = file;
              } else {
                allUpdatedFiles.push(file);
              }
            }
          }

          progress.tranches[nextTranche].status = "complete";
          progress.tranches[nextTranche].endTime = Date.now();
          progress.tranches[nextTranche].message = trancheResult.message || "Tranche completed.";
          progress.tranches[nextTranche].filesUpdated = trancheFilesUpdated;

          await progressTelemetry.event(
            `Tranche ${nextTranche + 1} merged`,
            `${trancheFilesUpdated.length} file(s) updated: ${trancheFilesUpdated.join(", ") || "none"}`,
            {
              stage: "executing",
              model: GEMINI_DEFAULT_EXECUTOR_MODEL,
              type: "success",
              patch: {
                label: `Tranche ${nextTranche + 1}/${progress.totalTranches} complete`,
                detail: trancheResult.message || `${trancheFilesUpdated.length} file(s) updated.`,
                currentTranche: nextTranche
              }
            }
          );

          console.log(`Tranche ${nextTranche + 1} complete: ${trancheFilesUpdated.length} files updated.`);

          if (allUpdatedFiles.length > 0) {
            await saveAiResponse(bucket, projectPath, allUpdatedFiles, {
              jobId: jobId,
              trancheIndex: nextTranche,
              totalTranches: progress.totalTranches,
              status: "checkpoint",
              message: `Checkpoint after tranche ${nextTranche + 1}/${progress.totalTranches}: ${trancheResult.message || "completed."}`
            });
          }
        }
      }

      state.progress = progress;
      state.accumulatedFiles = accumulatedFiles;
      state.allUpdatedFiles = allUpdatedFiles;
      await savePipelineState(bucket, projectPath, state);

      if (nextTranche + 1 < progress.totalTranches) {
        await chainToSelf({
          projectPath,
          jobId,
          mode: "tranche",
          nextTranche: nextTranche + 1
        });

        return {
          statusCode: 200,
          body: JSON.stringify({ success: true, chained: true, phase: `tranche_${nextTranche}_complete` })
        };
      }

      const summaryParts = progress.tranches
        .filter(t => t.status === "complete")
        .map((t) => `Tranche ${t.index + 1} — ${t.name}: ${t.message}`);

      const finalMessage = summaryParts.join("\n\n") || "Build completed.";

      await saveAiResponse(bucket, projectPath, allUpdatedFiles, {
        jobId: jobId,
        trancheIndex: progress.totalTranches - 1,
        totalTranches: progress.totalTranches,
        status: "final",
        message: finalMessage
      });

      progress.status = "complete";
      updateLiveState(progress, {
        stage: "complete",
        model: GEMINI_DEFAULT_EXECUTOR_MODEL,
        label: "Pipeline complete",
        detail: `Updated ${allUpdatedFiles.length} file(s) across ${progress.tranches.filter(tr => tr.status === "complete").length} tranche(s).`,
        streamingText: "",
        streamBytes: 0
      });

      appendLiveEvent(progress, {
        type: "success",
        label: "Pipeline complete",
        detail: `All tranche work finished. ${allUpdatedFiles.length} file(s) are ready in ai_response.json.`,
        stage: "complete",
        model: GEMINI_DEFAULT_EXECUTOR_MODEL
      });

      const t = progress.tokenUsage.totals;
      progress.finalMessage = `Build complete: ${allUpdatedFiles.length} file(s) updated across ${progress.tranches.filter(tr => tr.status === "complete").length} tranche(s). Tokens: ${t.input_tokens} in / ${t.output_tokens} out (${t.reasoning_tokens} reasoning, ${t.rest_output_tokens} rest-of-output).`;
      progress.completedTime = Date.now();

      await saveProgress(bucket, projectPath, progress);

      console.log(`Total tokens — input: ${t.input_tokens}, output: ${t.output_tokens}, reasoning: ${t.reasoning_tokens}, rest_output: ${t.rest_output_tokens}`);

      try { await bucket.file(`${projectPath}/ai_pipeline_state.json`).delete(); } catch (e) {}
      try { await bucket.file(`${projectPath}/ai_request.json`).delete(); } catch (e) {}

      return {
        statusCode: 200,
        body: JSON.stringify({ success: true, phase: "complete" })
      };
    }

    throw new Error(`Unknown mode: ${mode}`);
  } catch (error) {
    console.error("Gemini Code Proxy Background Error:", error);

    const errorPayload = buildSerializableErrorPayload(error, {
      projectPath,
      jobId: jobId || "unknown"
    });

    try {
      if (projectPath && bucket) {
        await bucket.file(`${projectPath}/ai_error.json`).save(
          JSON.stringify(errorPayload, null, 2),
          { contentType: "application/json", resumable: false }
        );

        try {
          await saveProgress(bucket, projectPath, {
            jobId: jobId || "unknown",
            status: "error",
            error: errorPayload.error,
            errorDetails: errorPayload.details || errorPayload.responseBody || null,
            errorPhase: errorPayload.phase || null,
            completedTime: Date.now()
          });
        } catch (e2) {}
      }
    } catch (e) {
      console.error("CRITICAL: Failed to write error to Firebase.", e);
    }

    return {
      statusCode: 500,
      body: JSON.stringify({
        error: errorPayload.error,
        phase: errorPayload.phase || null,
        details: errorPayload.details || errorPayload.responseBody || null
      })
    };
  }
};