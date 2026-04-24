/*  netlify/functions/_etsyMailAnthropic.js
 *
 *  Shared Anthropic API client for all EtsyMail AI features.
 *
 *  This file mirrors the exact integration pattern used by
 *  claudeCodeProxy-background.js in this same repo, for consistency:
 *    - Same env var: ANTHROPIC_API_KEY
 *    - Same endpoint + headers (incl. prompt-caching beta)
 *    - Same Opus 4.7 handling (adaptive thinking, output_config.effort,
 *      NO temperature/top_p/top_k/budget_tokens — all 400 on 4.7)
 *    - Same overload/rate-limit retry loop (5 attempts, exponential
 *      backoff with jitter, same status-code + message heuristics)
 *
 *  The one deliberate addition: a tool-use loop helper
 *  (`runToolLoop`) since EtsyMail's draft generator needs Claude to be
 *  able to call server-side tools like lookup_order_tracking and pick a
 *  terminal "compose_draft_reply" tool. claudeCodeProxy doesn't use
 *  tools so this piece is new; the HTTP client underneath is identical.
 */

const fetch = require("node-fetch");

// ─── Config ──────────────────────────────────────────────────────────────
const ANTHROPIC_URL     = "https://api.anthropic.com/v1/messages";
const ANTHROPIC_VERSION = "2023-06-01";
const ANTHROPIC_BETA    = "prompt-caching-2024-07-31";

// Retry constants — identical to claudeCodeProxy-background.js
const CLAUDE_OVERLOAD_MAX_RETRIES  = 5;
const CLAUDE_OVERLOAD_BASE_DELAY_MS = 1250;
const CLAUDE_OVERLOAD_MAX_DELAY_MS  = 12000;

// ─── Helpers ─────────────────────────────────────────────────────────────

function sleep(ms) { return new Promise(resolve => setTimeout(resolve, ms)); }

function computeClaudeRetryDelayMs(attempt) {
  const exponentialDelay = Math.min(
    CLAUDE_OVERLOAD_BASE_DELAY_MS * Math.pow(2, Math.max(0, attempt - 1)),
    CLAUDE_OVERLOAD_MAX_DELAY_MS
  );
  const jitter = Math.floor(Math.random() * 700);
  return exponentialDelay + jitter;
}

function isClaudeOverloadError(status, message = "") {
  const normalized = String(message || "").toLowerCase();
  if ([429, 500, 502, 503, 504, 529].includes(Number(status))) return true;
  if (
    normalized.includes("econnreset")     ||
    normalized.includes("econnrefused")   ||
    normalized.includes("etimedout")      ||
    normalized.includes("enotfound")      ||
    normalized.includes("socket hang up") ||
    normalized.includes("network error")  ||
    normalized.includes("fetch failed")
  ) return true;
  return (
    normalized.includes("overloaded")            ||
    normalized.includes("overload")              ||
    normalized.includes("rate limit")            ||
    normalized.includes("too many requests")     ||
    normalized.includes("capacity")              ||
    normalized.includes("temporarily unavailable")
  );
}

/** Convert a plain string system prompt into the structured cache-eligible
 *  block shape the Anthropic prompt-caching beta expects. If `system` is
 *  already an array of blocks, pass through unchanged. Matches the pattern
 *  from claudeCodeProxy-background.js::buildSystemBlocks. */
function buildSystemBlocks(system) {
  if (!system) return undefined;
  if (Array.isArray(system)) return system;
  if (typeof system === "string") {
    // Cache the entire system prompt as one ephemeral block. For EtsyMail
    // the system prompt is 2-5 KB of shop policies — big enough that cache
    // hits save meaningful input tokens on subsequent drafts in the same
    // thread. Opus 4.7 cache TTL is ~5 min by default.
    return [{ type: "text", text: system, cache_control: { type: "ephemeral" } }];
  }
  return undefined;
}

// ─── Core single-call client ─────────────────────────────────────────────

/** Make ONE request to /v1/messages with the retry loop around overload
 *  errors. Returns the raw response JSON so callers can inspect
 *  content blocks, usage, stop_reason, etc. Throws on non-retryable
 *  errors or after max retries exhausted.
 *
 *  @param {object} opts
 *  @param {string} opts.model        e.g. "claude-opus-4-7"
 *  @param {number} opts.maxTokens
 *  @param {string|object[]} opts.system
 *  @param {object[]} opts.messages   Anthropic message array
 *  @param {object[]} [opts.tools]    Tool definitions for tool-use
 *  @param {string} [opts.effort]     "low" | "medium" | "high" | "xhigh" | "max"
 *  @param {boolean} [opts.useThinking]  Opus 4.7 only; default true
 *  @param {number} [opts.budgetTokens]  Only applied to pre-4.7 models
 */
async function callClaudeRaw({
  model,
  maxTokens,
  system,
  messages,
  tools,
  effort,
  useThinking = true,
  budgetTokens
}) {
  const apiKey = process.env.ANTHROPIC_API_KEY;
  if (!apiKey) throw new Error("Missing ANTHROPIC_API_KEY");

  const systemBlocks = buildSystemBlocks(system);

  const body = {
    model,
    max_tokens: maxTokens,
    messages
  };
  if (systemBlocks) body.system = systemBlocks;
  if (Array.isArray(tools) && tools.length) body.tools = tools;

  const isOpus47 = model && model.startsWith("claude-opus-4-7");

  if (isOpus47) {
    // Opus 4.7: adaptive thinking only; temperature/top_p/top_k/budget_tokens all 400.
    if (useThinking) body.thinking = { type: "adaptive" };
    if (effort)      body.output_config = { effort };
  } else {
    // Pre-4.7: legacy budget_tokens path
    if (budgetTokens) body.thinking = { type: "enabled", budget_tokens: budgetTokens };
    if (effort)       body.output_config = { effort };
  }

  const headers = {
    "Content-Type"     : "application/json",
    "x-api-key"        : apiKey,
    "anthropic-version": ANTHROPIC_VERSION,
    "anthropic-beta"   : ANTHROPIC_BETA
  };

  let lastError = null;

  for (let attempt = 1; attempt <= CLAUDE_OVERLOAD_MAX_RETRIES; attempt++) {
    try {
      const res = await fetch(ANTHROPIC_URL, {
        method : "POST",
        headers,
        body   : JSON.stringify(body)
      });

      const rawText = await res.text();
      let data = null;

      if (rawText) {
        try { data = JSON.parse(rawText); }
        catch (parseErr) {
          const e = new Error(`Claude returned non-JSON response: ${parseErr.message}`);
          e.status = res.status;
          e.rawText = rawText;
          e.isRetryableOverload = isClaudeOverloadError(res.status, rawText);
          throw e;
        }
      }

      if (!res.ok) {
        const errMsg = (data && data.error && data.error.message) || `Claude API error (${res.status})`;
        const e = new Error(errMsg);
        e.status = res.status;
        e.data = data;
        e.isRetryableOverload = isClaudeOverloadError(res.status, errMsg);
        throw e;
      }

      // Success — caller consumes the raw response
      console.log(
        `[callClaude] model=${model} effort=${effort || "none"} stop_reason=${data.stop_reason} ` +
        `input_tokens=${data.usage?.input_tokens || 0} output_tokens=${data.usage?.output_tokens || 0} ` +
        `cache_read=${data.usage?.cache_read_input_tokens || 0} cache_create=${data.usage?.cache_creation_input_tokens || 0} ` +
        `content_blocks=${Array.isArray(data.content) ? data.content.length : 0}`
      );
      return data;

    } catch (err) {
      const status = err && err.status ? err.status : null;
      const retryable = Boolean(err && err.isRetryableOverload) ||
                        isClaudeOverloadError(status, (err && err.message) || "");
      lastError = err;

      if (!retryable || attempt >= CLAUDE_OVERLOAD_MAX_RETRIES) throw err;

      const delayMs = computeClaudeRetryDelayMs(attempt);
      console.warn(
        `[callClaude] retrying after overload/rate-limit ` +
        `(attempt ${attempt}/${CLAUDE_OVERLOAD_MAX_RETRIES}, model=${model}, status=${status || "n/a"}, delay=${delayMs}ms): ${err.message}`
      );
      await sleep(delayMs);
    }
  }

  throw lastError || new Error("Claude request failed after retries");
}

// ─── Tool-use loop ───────────────────────────────────────────────────────

/** Run a multi-turn tool-use loop with Claude.
 *
 *  Claude returns `stop_reason: "tool_use"` when it wants to call a tool.
 *  We execute the tool, append the result as a user turn containing a
 *  tool_result block, and call Claude again. Repeat until Claude produces
 *  a natural end_turn or hits the iteration cap.
 *
 *  The caller defines tools in `toolSpecs` (Anthropic's shape) and a
 *  matching `toolExecutors` map of name → async function(input, ctx).
 *
 *  Returns { finalResponse, transcript, toolCalls, usage }.
 *    - finalResponse is the last API response (the one that ended the loop)
 *    - transcript is the full messages[] array (useful for audit/debug)
 *    - toolCalls is an array of {name, input, output, durationMs, error}
 *    - usage is aggregated across all calls (sum of input + output tokens)
 *
 *  @param {object} opts
 *  @param {string} opts.model
 *  @param {number} opts.maxTokens
 *  @param {string|object[]} opts.system
 *  @param {object[]} opts.initialMessages
 *  @param {object[]} opts.toolSpecs
 *  @param {object}   opts.toolExecutors  name → async fn(input, ctx) → result
 *  @param {object}   [opts.toolContext]  passed as 2nd arg to each executor
 *  @param {string}   [opts.effort]
 *  @param {boolean}  [opts.useThinking]
 *  @param {number}   [opts.maxIterations] default 6
 */
async function runToolLoop({
  model,
  maxTokens,
  system,
  initialMessages,
  toolSpecs,
  toolExecutors,
  toolContext = null,
  effort,
  useThinking = true,
  maxIterations = 6
}) {
  const messages = [...initialMessages];
  const toolCalls = [];
  const aggUsage = {
    input_tokens                : 0,
    output_tokens               : 0,
    cache_read_input_tokens     : 0,
    cache_creation_input_tokens : 0
  };
  let finalResponse = null;

  for (let iter = 1; iter <= maxIterations; iter++) {
    const response = await callClaudeRaw({
      model, maxTokens, system, messages,
      tools: toolSpecs, effort, useThinking
    });

    // Aggregate usage
    if (response.usage) {
      aggUsage.input_tokens                += response.usage.input_tokens                || 0;
      aggUsage.output_tokens               += response.usage.output_tokens               || 0;
      aggUsage.cache_read_input_tokens     += response.usage.cache_read_input_tokens     || 0;
      aggUsage.cache_creation_input_tokens += response.usage.cache_creation_input_tokens || 0;
    }

    finalResponse = response;

    // Append assistant turn to transcript so next iteration sees full history.
    const assistantContent = Array.isArray(response.content) ? response.content : [];
    messages.push({ role: "assistant", content: assistantContent });

    // If Claude didn't request a tool call, we're done.
    if (response.stop_reason !== "tool_use") break;

    // Gather every tool_use block and execute each one; Claude may emit
    // multiple tool calls in one turn.
    const toolUseBlocks = assistantContent.filter(b => b && b.type === "tool_use");
    if (!toolUseBlocks.length) break;  // defensive

    const toolResultBlocks = [];
    let terminalCalled = false;  // If any executor returns { __terminal: true }
                                  // we stop after processing this batch — no
                                  // further API call. This lets callers signal
                                  // a "compose final output" terminal tool
                                  // without the model burning another round-trip.

    for (const tu of toolUseBlocks) {
      const executor = toolExecutors[tu.name];
      const started  = Date.now();
      let output;
      let errMsg = null;

      if (!executor) {
        errMsg = `No executor registered for tool '${tu.name}'`;
        output = { error: errMsg };
      } else {
        try {
          output = await executor(tu.input || {}, toolContext);
        } catch (e) {
          errMsg = e.message || String(e);
          output = { error: errMsg };
        }
      }
      const durationMs = Date.now() - started;

      // Terminal-tool convention: executor returns { __terminal: true, ... }
      if (output && typeof output === "object" && output.__terminal === true) {
        terminalCalled = true;
      }

      toolCalls.push({
        name: tu.name,
        input: tu.input,
        output,
        durationMs,
        error: errMsg
      });

      // tool_result content is a string per Anthropic docs. We stringify
      // the output object so the model can read structured data.
      toolResultBlocks.push({
        type        : "tool_result",
        tool_use_id : tu.id,
        content     : typeof output === "string" ? output : JSON.stringify(output),
        is_error    : Boolean(errMsg)
      });
    }

    // Append user turn with all tool_result blocks
    messages.push({ role: "user", content: toolResultBlocks });

    // If a terminal tool fired, stop here without another API call.
    if (terminalCalled) break;
  }

  return {
    finalResponse,
    transcript: messages,
    toolCalls,
    usage: aggUsage
  };
}

module.exports = {
  callClaudeRaw,
  runToolLoop,
  isClaudeOverloadError,
  buildSystemBlocks,
  sleep
};
