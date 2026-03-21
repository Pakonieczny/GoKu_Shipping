/* netlify/functions/claudeCodeProxy-background.js */
/* ═══════════════════════════════════════════════════════════════════
   TRANCHED AI PIPELINE — v5.2 (+ Spec Validation Patch Loop)
   ─────────────────────────────────────────────────────────────────
   Each invocation handles ONE unit of work then chains to itself
   for the next, staying well under Netlify's 15-min limit.

   Invocation 0    ▸  "plan"    — Spec Validation Gate (3 Sonnet calls)
                       runs first, then Opus 4.6 creates a dependency-
                       ordered, 6.3-centered tranche plan + hardening batch.
                       Gate FAIL writes ai_error.json with structured issues
                       and halts before Opus fires.
   Invocation 1–N  ▸  "tranche" — Sonnet 4.6 executes one tranche,
                       saves accumulated files, chains to next tranche.
   Correction loop ▸  "fix"     — Used only for objective retryable
                       validation failures.
   Final           ▸  Writes ai_response.json for frontend pickup.

   SPEC VALIDATION GATE (runs in "plan" mode before Opus):
   ─────────────────────────────────────────────────────────
   Call 1 — Extract  : Sonnet reads Master Prompt, produces 6-8 custom
                       simulation scenarios specific to this game's mechanics.
   Call 2 — Simulate : Sonnet traces each scenario through the spec rules
                       literally, documents findings in plain text.
   Call 3 — Review   : Sonnet classifies findings as PASS/FAIL JSON.
   On PASS  → continues to Opus planning as normal.
   On FAIL  → writes ai_error.json { validationFailed:true, issues:[...] }
              and returns without invoking Opus. Frontend renders issues
              in the tranche panel so the user can fix the Master Prompt.
   On error → validation is skipped with a warning; Opus proceeds.

   Recovery policy:
   - 0 retries for soft/advisory findings.
   - 1 retry max for narrow objective hard failures when the repair is surgical.
   - 2 retries only for parser/envelope failures or truly critical scaffold/runtime issues.
   - Everything else is deferred into a single end-stage hardening batch.

   All intermediate state lives in Firebase so each invocation is
   stateless and can reconstruct context from the pipeline file.
   ═══════════════════════════════════════════════════════════════════ */

const fetch = require("node-fetch");
const admin = require("./firebaseAdmin");

const RETRY_POLICY = Object.freeze({
  parser_envelope: 2,
  critical_runtime: 2  // retained for fix-mode retryBudget fallback
});

const HARDENING_BATCH_NAME = "End-Stage Hardening Batch";
const HARDENING_BATCH_KIND = "hardening_batch";


/* ─── SCAFFOLD + SDK INSTRUCTION BUNDLE: fetched from Firebase ───
   All project-level instruction files live under:
     ${projectPath}/ai_system_instructions/

   We classify them into:
   - scaffold: immutable game foundation / structural rules
   - sdk: engine reference / API facts / certainty fallback
   - other: additional instruction docs (treated as sdk-side supplemental context)
*/

function classifyInstructionFile(fileName = "", content = "") {
  const lowerName = String(fileName || "").toLowerCase();
  const lowerContent = String(content || "").toLowerCase();

  // SDK check runs FIRST — filename is unambiguous and must not be overridden
  // by the content-based scaffold check (SDK docs often mention "scaffold" and "immutable")
  if (
    lowerName.includes("engine_reference") ||
    lowerName.includes("engine-reference") ||
    lowerName.includes("engine reference") ||
    lowerName.includes("sdk") ||
    lowerContent.includes("cherry3d engine reference") ||
    lowerContent.includes("platform invariants")
  ) {
    return "sdk";
  }

  if (
    lowerName.includes("scaffold") ||
    (lowerContent.includes("scaffold") && lowerContent.includes("immutable"))
  ) {
    return "scaffold";
  }

  return "other";
}

async function fetchInstructionBundle(bucket, projectPath) {
  try {
    const folder = `${projectPath}/ai_system_instructions`;
    const [files] = await bucket.getFiles({ prefix: folder + "/" });
    if (!files || files.length === 0) {
      console.warn(`fetchInstructionBundle: no files found at ${folder}/`);
      return {
        scaffoldText: "",
        sdkText: "",
        combinedText: "",
        scaffoldCount: 0,
        sdkCount: 0,
        otherCount: 0
      };
    }

    files.sort((a, b) => a.name.localeCompare(b.name));
    const parts = await Promise.all(
      files.map(async (file) => {
        const [fileContent] = await file.download();
        const content = fileContent.toString("utf8");
        return {
          fileName: file.name.split("/").pop(),
          content,
          kind: classifyInstructionFile(file.name.split("/").pop(), content)
        };
      })
    );

    const scaffoldDocs = parts.filter(p => p.kind === "scaffold");
    const sdkDocs = parts.filter(p => p.kind === "sdk");
    const otherDocs = parts.filter(p => p.kind === "other");

    const formatDocs = (docs) => docs.map(doc =>
      `--- ${doc.fileName} ---\n${doc.content}`
    ).join("\n\n");

    const scaffoldText = formatDocs(scaffoldDocs);
    const sdkText = formatDocs([...sdkDocs, ...otherDocs]);

    const sections = [];
    if (scaffoldText) {
      sections.push(`=== IMMUTABLE CHERRY3D SCAFFOLD ===\n${scaffoldText}`);
    }
    if (sdkText) {
      sections.push(`=== CHERRY3D SDK / ENGINE REFERENCE ===\n${sdkText}`);
    }

    const combinedText = sections.join("\n\n");
    console.log(
      `fetchInstructionBundle: loaded ${files.length} file(s) ` +
      `(scaffold=${scaffoldDocs.length}, sdk=${sdkDocs.length}, other=${otherDocs.length})`
    );

    return {
      scaffoldText,
      sdkText,
      combinedText,
      scaffoldCount: scaffoldDocs.length,
      sdkCount: sdkDocs.length,
      otherCount: otherDocs.length
    };
  } catch (err) {
    console.error("fetchInstructionBundle failed:", err.message);
    return {
      scaffoldText: "",
      sdkText: "",
      combinedText: "",
      scaffoldCount: 0,
      sdkCount: 0,
      otherCount: 0
    };
  }
}

function assertInstructionBundle(bundle, phaseLabel = "Pipeline") {
  if (!bundle?.scaffoldText) {
    throw new Error(`${phaseLabel}: immutable Scaffold missing from ai_system_instructions/.`);
  }
  if (!bundle?.sdkText) {
    throw new Error(`${phaseLabel}: SDK / Engine Reference missing from ai_system_instructions/.`);
  }
}

/* ── Load approved Asset Roster from Firebase (if present) ──────
   Returns a formatted context block string, or empty string if no
   roster was approved for this run.                               */
async function loadApprovedRosterBlock(bucket, projectPath) {
  try {
    const rosterFile = bucket.file(`${projectPath}/ai_asset_roster_approved.json`);
    const [exists] = await rosterFile.exists();
    if (!exists) return "";
    const [content] = await rosterFile.download();
    const r = JSON.parse(content.toString());
    if (!r._meta?.approved) return "";

    const objs = (r.primitiveObjects || []).map(a =>
      `  - ${a.assetName} (from ${a.sourceRosterDocument}): ${a.intendedRole || ""}`
    ).join("\n");
    const texs = (r.textureAssets || []).map(a =>
      `  - ${a.assetName} (from ${a.sourceRosterDocument}): ${a.intendedUsage || ""}`
    ).join("\n");
    const staged = (r.stagedAssets || []).map(a =>
      `  - ${a.assetName} → ${a.stagedPath}`
    ).join("\n");
    const vn = r.visualDirectionNotes || {};
    const sf = r._meta?.stagedFolder || "";

    return `\n\n═══════════════════════════════════════════════════════════
APPROVED GAME-SPECIFIC ASSET ROSTER — FIRST-CLASS COMPANION DOCUMENT
Authority equal to the Master Prompt and all reference images.
All tranche planning and execution MUST use these approved assets.
═══════════════════════════════════════════════════════════

GAME INTERPRETATION:
${r.gameInterpretationSummary || ""}

APPROVED PRIMITIVE OBJECTS (${(r.primitiveObjects||[]).length}):
${objs || "  (none)"}

APPROVED TEXTURE ASSETS (${(r.textureAssets||[]).length}):
${texs || "  (none)"}

STAGED ASSET FOLDER: ${sf}
STAGED FILES (Firebase paths — use these in models/2 and models/23):
${staged || "  (none extracted)"}

ASSETS.JSON: Staged assets are registered under the "staged_roster" key.
Use those manifest keys for all asset references in models/2 and models/23.

VISUAL DIRECTION:
  Color Direction:    ${vn.colorDirection || "N/A"}
  Material Style:     ${vn.materialStyle || "N/A"}
  Realism Level:      ${vn.realismLevel || "N/A"}
  Environmental Tone: ${vn.environmentalTone || "N/A"}
  Surface Treatment:  ${vn.surfaceTreatment || "N/A"}
  FX Relevance:       ${vn.fxRelevance || "N/A"}

TRANCHE DESIGN & EXECUTION REQUIREMENT:
1. Tranche Design MUST plan explicitly around these approved assets.
2. Every tranche touching rendered content, obstacles, environment, or scene
   objects MUST incorporate the relevant approved assets from this roster.
3. Textures and materials MUST follow the Visual Direction notes above.
4. Reference staged files by their Firebase staged paths or assets.json keys.
5. Color direction and surface treatment must be consistent throughout all tranches.
═══════════════════════════════════════════════════════════`;
  } catch (e) {
    console.warn("[ROSTER] Could not load approved roster:", e.message);
    return "";
  }
}

/* ── DYNAMIC_ARCHITECTURE_JSON_SCHEMA — REMOVED ─────────────
   Architect pass has been merged into single-pass planner.
   No intermediate architecture spec is generated. ────────── */


/* ── Hardening batch helpers ─────────────────────────────────── */

function isHardeningBatchTranche(tranche = {}) {
  return Boolean(
    tranche.kind === HARDENING_BATCH_KIND ||
    tranche.isHardeningBatch ||
    String(tranche.name || "").toLowerCase().includes("hardening")
  );
}

function formatHardeningQueue(items = []) {
  if (!items.length) return "No queued hardening items.";
  return items.map((item, idx) => [
    `ITEM ${idx + 1}`,
    `  Tranche : ${item.trancheIndex !== undefined ? item.trancheIndex + 1 : "n/a"} — ${item.trancheName || "Unknown tranche"}`,
    `  Lane    : ${item.lane || "soft"}`,
    `  File    : ${item.file || "unknown"}`,
    `  Pattern : ${item.pattern || item.kind || "General hardening"}`,
    `  Detail  : ${item.message || item.note || "No detail provided."}`
  ].join('\n')).join('\n\n');
}

function buildHardeningBatchUserText({ progress, accumulatedFiles, tranche, modelAnalysis }) {
  const queuedItems = Array.isArray(progress?.hardeningQueue) ? progress.hardeningQueue : [];
  let text = '';
  text += `=== HARDENING BATCH CONTEXT ===\n`;
  text += `You are resolving deferred tranche findings in one end-stage batch.\n`;
  text += `This batch exists to clean up queued advisories and unresolved objective issues without redoing earlier tranches.\n\n`;
  text += `=== QUEUED HARDENING ITEMS ===\n${formatHardeningQueue(queuedItems)}\n=== END QUEUED HARDENING ITEMS ===\n\n`;
  text += `=== HARDENING BATCH MANIFEST ===\n`;
  text += `Name: ${tranche.name}\n`;
  text += `Purpose: ${tranche.purpose || tranche.description || 'Resolve queued issues in a single final pass.'}\n`;
  text += `Visible Result: ${tranche.visibleResult || 'Project remains runnable with queued issues resolved.'}\n`;
  text += `Safety Checks:\n${(tranche.safetyChecks || []).map((s, i) => `  ${i + 1}. ${s}`).join('\n') || '  1. Preserve all working systems and only fix queued items.'}\n`;
  text += `=== END HARDENING BATCH MANIFEST ===\n\n`;
  text += `=== CURRENT ACCUMULATED FILES ===\n`;
  for (const [pathName, fileContent] of Object.entries(accumulatedFiles || {})) {
    text += `\n--- FILE: ${pathName} ---\n${fileContent}\n`;
  }
  text += `\n=== END CURRENT ACCUMULATED FILES ===\n\n`;
  if (Array.isArray(modelAnalysis) && modelAnalysis.length > 0) {
    text += `=== THREE.JS MODEL ANALYSIS ===\n${JSON.stringify(modelAnalysis, null, 2)}\n=== END THREE.JS MODEL ANALYSIS ===\n\n`;
  }
  text += `Resolve the queued hardening items with the minimum safe edits required. Preserve all existing working code. Output complete updated file contents only for the files you changed.`;
  return text;
}

/* ── helper: call Claude API ─────────────────────────────────── */
const CLAUDE_OVERLOAD_MAX_RETRIES = 5;
const CLAUDE_OVERLOAD_BASE_DELAY_MS = 1250;
const CLAUDE_OVERLOAD_MAX_DELAY_MS = 12000;

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

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
  return (
    normalized.includes("overloaded") ||
    normalized.includes("overload") ||
    normalized.includes("rate limit") ||
    normalized.includes("too many requests") ||
    normalized.includes("capacity") ||
    normalized.includes("temporarily unavailable")
  );
}

async function callClaude(apiKey, { model, maxTokens, system, userContent, effort, budgetTokens }) {
  const body = {
    model,
    max_tokens: maxTokens,
    system,
    messages: [{ role: "user", content: userContent }]
  };

  if (budgetTokens) {
    body.thinking = { type: "enabled", budget_tokens: budgetTokens };
  }

  if (effort) {
    body.output_config = { effort };
  }

  let lastError = null;

  for (let attempt = 1; attempt <= CLAUDE_OVERLOAD_MAX_RETRIES; attempt++) {
    try {
      const res = await fetch("https://api.anthropic.com/v1/messages", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "x-api-key": apiKey,
          "anthropic-version": "2023-06-01"
        },
        body: JSON.stringify(body)
      });

      const rawText = await res.text();
      let data = null;

      if (rawText) {
        try {
          data = JSON.parse(rawText);
        } catch (parseErr) {
          if (!res.ok) {
            throw new Error(`Claude API error (${res.status}) with non-JSON body: ${rawText.slice(0, 500)}`);
          }
          throw new Error(`Failed to parse Claude response JSON: ${parseErr.message}`);
        }
      }

      if (!res.ok) {
        const apiMessage = data?.error?.message || `Claude API error (${res.status})`;
        const err = new Error(apiMessage);
        err.status = res.status;
        err.isRetryableOverload = isClaudeOverloadError(res.status, apiMessage);
        throw err;
      }

      const responseText = data?.content?.find(b => b.type === "text")?.text;
      if (!responseText) {
        throw new Error("Empty response from Claude");
      }

      return {
        text: responseText,
        usage: data?.usage || null
      };
    } catch (err) {
      const status = Number(err?.status || 0);
      const retryable =
        Boolean(err?.isRetryableOverload) ||
        isClaudeOverloadError(status, err?.message);

      lastError = err;

      if (!retryable || attempt >= CLAUDE_OVERLOAD_MAX_RETRIES) {
        throw err;
      }

      const delayMs = computeClaudeRetryDelayMs(attempt);
      console.warn(
        `[callClaude] retrying Claude request after overload/rate-limit ` +
        `(attempt ${attempt}/${CLAUDE_OVERLOAD_MAX_RETRIES}, model=${model}, status=${status || "n/a"}, delay=${delayMs}ms): ${err.message}`
      );
      await sleep(delayMs);
    }
  }

  throw lastError || new Error("Claude request failed after retries");
}

/* ── helper: strip markdown fences and prose to extract JSON ─── */
/* Used ONLY for the planning phase (Opus), which outputs pure metadata
   strings — no embedded code — so JSON is safe there.               */
function stripFences(text) {
  let cleaned = text
    .replace(/^```json\s*/i, "")
    .replace(/^```\s*/i, "")
    .replace(/\s*```$/i, "")
    .trim();

  const firstBrace = cleaned.indexOf('{');
  const lastBrace = cleaned.lastIndexOf('}');
  if (firstBrace > 0 && lastBrace > firstBrace) {
    cleaned = cleaned.substring(firstBrace, lastBrace + 1);
  }

  return cleaned.trim();
}

function safeJsonParse(text, label) {
  try {
    return JSON.parse(stripFences(text));
  } catch (error) {
    throw new Error(`Failed to parse ${label} output as JSON: ${error.message}`);
  }
}

/* ── buildArchitectureSpecBlock — REMOVED ─────────────────
   No longer needed. Single-pass planner embeds game-specific
   rules directly in each tranche prompt. ─────────────────── */

const REQUIRED_TRANCHE_VALIDATION_BLOCK = `
VALIDATION + RECOVERY CONTRACT:
- Design tranche prompts so tranche success is judged first by visibleResult + safetyChecks.
- Do NOT plan tranches that depend on stylistic perfection or one preferred coding style to pass.
- Objective scaffold/runtime mistakes may be retried, but only under the runtime policy:
  • 0 retries for soft/advisory findings.
  • 1 retry max for narrow objective hard failures when the repair is obviously surgical.
  • 2 retries only for parser/envelope failures or truly critical scaffold/runtime issues.
  • Everything else is deferred into one end-stage hardening batch.
- Always make the final tranche a single end-stage hardening batch anchored to Section 8 so deferred findings are resolved in one pass.`;

function countOccurrences(haystack, needle) {
  if (!needle) return 0;
  return String(haystack || '').split(needle).length - 1;
}

function assertTranchePromptHasRequiredManifestBlock(tranche, index) {
  if (!tranche || typeof tranche.prompt !== 'string' || !tranche.prompt.trim()) {
    throw new Error(`Planner tranche ${index + 1} is missing a prompt.`);
  }
  return true;
}

function normalizeArray(value, fallback = []) {
  if (Array.isArray(value)) return value.filter(Boolean);
  if (value === undefined || value === null || value === '') return [...fallback];
  return [value].filter(Boolean);
}

function appendSyntheticHardeningTranche(plan) {
  const tranches = Array.isArray(plan?.tranches) ? plan.tranches : [];
  if (tranches.some(isHardeningBatchTranche)) return plan;

  const maxPhase = tranches.reduce((max, tranche) => Math.max(max, Number(tranche?.phase || 0)), 0);
  tranches.push({
    kind: HARDENING_BATCH_KIND,
    isHardeningBatch: true,
    name: HARDENING_BATCH_NAME,
    description: 'Single deferred batch that resolves queued soft findings and unresolved objective issues after the functional tranches are complete.',
    anchorSections: ['8.1', '8.3'],
    purpose: 'Resolve the queued hardening backlog in one final pass without redoing earlier tranches.',
    systemsTouched: ['cross-system hardening', 'final acceptance', 'deferred validation cleanup'],
    filesTouched: ['models/2', 'models/23'],
    visibleResult: 'The project remains runnable and any queued hardening items are resolved in one final pass.',
    safetyChecks: [
      'Preserve all already-working tranche output.',
      'Fix queued hardening items with the smallest safe edits.',
      'Do not regress gameplay, HUD, lifecycle, or restart behavior while hardening.'
    ],
    expertAgents: ['integration', 'qa'],
    phase: maxPhase + 1,
    dependencies: tranches.map((tr, idx) => tr?.name || `Tranche ${idx + 1}`),
    qualityCriteria: [
      'Queued hardening items are resolved without regressions.',
      'The final code remains scaffold-compliant and runnable.'
    ],
    prompt: 'Synthetic hardening batch — runtime will inject the deferred hardening queue.',
    expectedFiles: ['models/2', 'models/23']
  });

  plan.tranches = tranches;
  return plan;
}

function enforceTrancheValidationBlock(plan) {
  const rawTranches = Array.isArray(plan?.tranches) ? plan.tranches : [];
  plan.tranches = rawTranches.map((tranche, index) => {
    const expectedFiles = normalizeArray(tranche.expectedFiles || tranche.filesTouched, ['models/2', 'models/23']);
    return {
      kind: tranche.kind || 'build',
      name: tranche.name || `Tranche ${index + 1}`,
      description: tranche.description || tranche.purpose || `Implement tranche ${index + 1}.`,
      anchorSections: normalizeArray(tranche.anchorSections, ['6.3']),
      purpose: tranche.purpose || tranche.description || `Implement tranche ${index + 1}.`,
      systemsTouched: normalizeArray(tranche.systemsTouched, ['gameplay']),
      filesTouched: normalizeArray(tranche.filesTouched, expectedFiles),
      visibleResult: tranche.visibleResult || tranche.description || `Tranche ${index + 1} produces a runnable incremental result.`,
      safetyChecks: normalizeArray(tranche.safetyChecks, tranche.qualityCriteria || ['Leave the project runnable after this tranche.']),
      expertAgents: normalizeArray(tranche.expertAgents, []),
      phase: Number(tranche.phase || 0),
      dependencies: normalizeArray(tranche.dependencies, []),
      qualityCriteria: normalizeArray(tranche.qualityCriteria, []),
      prompt: String(tranche.prompt || '').trim(),
      expectedFiles,
      isHardeningBatch: Boolean(tranche.isHardeningBatch || tranche.kind === HARDENING_BATCH_KIND)
    };
  });

  return appendSyntheticHardeningTranche(plan);
}

/* ── helper: parse tranche executor delimiter-format responses ── */
/* Tranche executors output raw file content between delimiters,
   completely bypassing JSON escaping. This eliminates the entire
   class of "Unexpected non-whitespace character after JSON" errors
   that occur when Claude embeds code inside a JSON string field.

   Expected format from the executor:
     ===FILE_START: models/2===
     ...raw file content, zero escaping needed...
     ===FILE_END: models/2===

     ===MESSAGE===
     Changelog text here
     ===END_MESSAGE===
*/
function parseDelimitedResponse(text) {
  const files = [];

  // Extract all FILE_START / FILE_END blocks
  const fileRegex = /===FILE_START:\s*([^\n]+?)\s*===\n([\s\S]*?)===FILE_END:\s*\1\s*===/g;
  let match;
  while ((match = fileRegex.exec(text)) !== null) {
    const path = match[1].trim();
    const content = match[2]; // preserve exactly — no trimming
    if (path && content !== undefined) {
      files.push({ path, content });
    }
  }

  // Extract message block
  const msgMatch = text.match(/===MESSAGE===\n([\s\S]*?)===END_MESSAGE===/);
  const message = msgMatch ? msgMatch[1].trim() : "Tranche completed.";

  // If no delimiters found at all, fall back to JSON for backwards compat
  if (files.length === 0) {
    try {
      const parsed = JSON.parse(stripFences(text));
      if (parsed && Array.isArray(parsed.updatedFiles)) {
        console.warn("Executor used JSON format instead of delimiter format — parsed as fallback.");
        return parsed;
      }
    } catch (_) { /* ignore */ }
    // Return empty-handed; caller will treat as a skippable parse error
    return null;
  }

  return { updatedFiles: files, message };
}

/* ── helper: save progress to Firebase ───────────────────────── */
async function saveProgress(bucket, projectPath, progress) {
  await bucket.file(`${projectPath}/ai_progress.json`).save(
    JSON.stringify(progress),
    { contentType: "application/json", resumable: false }
  );
}

/* ── helper: save ai_response.json with freshness metadata ───── */
/* Called after every successful tranche merge (checkpoint), on
   cancellation, and at final completion so the frontend always has
   the best available snapshot and can verify payload freshness.    */
async function saveAiResponse(bucket, projectPath, allUpdatedFiles, meta = {}) {
  const payload = {
    jobId:         meta.jobId        || "unknown",
    timestamp:     Date.now(),
    trancheIndex:  meta.trancheIndex !== undefined ? meta.trancheIndex : null,
    totalTranches: meta.totalTranches || null,
    status:        meta.status       || "checkpoint", // "checkpoint" | "cancelled" | "final"
    message:       meta.message      || "",
    updatedFiles:  allUpdatedFiles   || []
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
  } catch (e) { /* no active job file = continue safely */ }
  return { killed: false };
}

/* ── helper: self-chain — invoke this function again ─────────── */
async function chainToSelf(payload) {
  const siteUrl = process.env.URL || process.env.DEPLOY_URL || "";
  const chainUrl = `${siteUrl}/.netlify/functions/claudeCodeProxy-background`;

  console.log(`CHAIN → next step: mode=${payload.mode}, tranche=${payload.nextTranche ?? "n/a"} → ${chainUrl}`);

  try {
    const res = await fetch(chainUrl, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload)
    });
    // Background functions return 202 immediately — we don't wait.
    console.log(`Chain response status: ${res.status}`);
  } catch (err) {
    console.error("Chain invocation failed:", err.message);
    throw new Error(`Self-chain failed: ${err.message}`);
  }
}

/* ═══════════════════════════════════════════════════════════════
   SPEC VALIDATION GATE — three Sonnet calls before Opus planning
   ═══════════════════════════════════════════════════════════════

   Call 1 (Extract)  : reads the Master Prompt and identifies the
     game's actual mechanics, producing 6-8 custom simulation
     scenarios tailored to THIS game. Generic enough to work for
     any genre; never hard-codes Fish_Hunt fields.

   Call 2 (Simulate) : traces each scenario through the spec rules
     literally. Documents TRACE / RESULT / ISSUE for each.

   Call 3 (Review)   : classifies simulation findings as PASS/FAIL
     and emits a structured JSON issues list.

   All three calls use claude-sonnet-4-20250514.
   Extended thinking is disabled for these calls (no budgetTokens)
   so they stay fast and cheap — under ~20 seconds total.
   ═══════════════════════════════════════════════════════════════ */

/* ── Known engine constraints injected into Call 2 ──────────────
   These are scaffold-level facts that the Master Prompt author
   should not have to write — they apply to every Cherry3D game. */
const SCAFFOLD_VALIDATION_CONSTRAINTS = `
KNOWN ENGINE CONSTRAINTS (Cherry3D scaffold v8.1):
These apply to every game regardless of what the Master Prompt says.
Factor them into your simulations where relevant.

1. OBJECT POOLING: Any spawn/despawn cycle MUST use ScenePool.
   A count cap alone does not prevent WASM object accumulation.
   If the spec has no pooling mechanism, note objectAccumulationRisk.

2. ROOT OBJECT ROTATION: .rotation/.rotate on a root scene object
   is a silent no-op. Directional characters must use scale flip.
   If the spec describes characters that face a travel direction
   with no mechanism stated, note it as a risk.

3. CHILD RIGIDBODY POSITION: A RigidBody added as a child of a
   mesh returns local-space coords from getMotionState(). Top-down
   games must track player position by integrating velocity on the
   main thread — not by reading the child RB.

4. TOP-DOWN CAMERA: mat4.lookAt with a vertical camera produces a
   degenerate matrix. The scaffold handles this automatically —
   no spec action needed, but note it if relevant to the game.
`;

/* ── Call 1: Extract game-specific simulation scenarios ─────── */
function buildExtractionPrompt(masterPrompt) {
  return `You are a game logic analyst. Read the Master Game Prompt below \
and identify the game's core mechanics that could contain logical errors \
before any code is written.

Produce a JSON array of 6-8 simulation scenarios tailored specifically \
to THIS game. Each scenario must be concrete and traceable — it must \
have a specific setup, a specific spec rule to apply, and a question \
that has a definite numerical or boolean answer.

Cover these four areas for every game:

1. START STATE VIABILITY
   Can the player do anything meaningful in the very first seconds?
   Is there a condition at the exact start value that might be impossible?

2. FIRST INTERACTION CORRECTNESS
   What happens when the player performs the primary action for the
   first time? Does the formula or condition produce the correct result?

3. PROGRESSION FORMULA BEHAVIOUR
   Does the score/growth/currency formula produce smooth progression
   or explosive/broken jumps at representative values?

4. STATE TRANSITION COMPLETENESS
   Do all UI state transitions (death → modal, shop open → close,
   pause → resume, restart) leave the game in a clean defined state?

Also check: does the spec describe a spawn/despawn cycle? If so,
include a scenario that checks whether the spec explicitly requires
object pooling (not just a count cap).

MASTER GAME PROMPT:
${masterPrompt}

Respond with ONLY a valid JSON array. No markdown fences, no preamble.

[
  {
    "id": "SIM-01",
    "area": "start state viability",
    "setup": "exact starting conditions from the spec",
    "specRule": "the relevant rule to quote verbatim from the spec",
    "question": "the specific concrete question to answer",
    "expectedBehaviour": "what correct gameplay looks like here"
  }
]`;
}

/* ── Call 2: Simulate the scenarios against the spec ─────────── */
function buildSimulationPrompt(masterPrompt, scenarios) {
  const scenarioBlock = scenarios.map(s =>
`${s.id} — ${String(s.area || '').toUpperCase()}
  Setup:    ${s.setup}
  Rule:     find and quote verbatim: "${s.specRule}"
  Question: ${s.question}
  Expected: ${s.expectedBehaviour}

  TRACE:  [apply the rule literally, step by step]
  RESULT: [the specific outcome — a number, a state, a behaviour]
  ISSUE:  "none" OR precise description of the problem found`
  ).join('\n\n');

  return `You are a game logic validator. You have been given a Master \
Game Prompt and a set of simulation scenarios tailored to this specific \
game. Trace each scenario through the spec rules literally and document \
exactly what you find.

Do NOT write code. Do NOT summarise the spec. Apply every rule exactly \
as written. If a rule says "strictly less than", apply strictly less than.

${SCAFFOLD_VALIDATION_CONSTRAINTS}

MASTER GAME PROMPT:
${masterPrompt}

SIMULATIONS TO RUN:
${scenarioBlock}

Do not skip any simulation. Do not add simulations not listed above.`;
}

/* ── Call 3: Classify simulation findings as PASS / FAIL ─────── */
function buildReviewPrompt(simulationDoc, scenarios) {
  const simIds = scenarios.map(s => s.id).join(', ');
  return `You are a spec review classifier. Read the simulation document \
below and classify each finding. Do NOT re-run simulations. Do NOT \
introduce new reasoning. ONLY classify what the simulation document \
already found.

Simulation IDs that were run: ${simIds}

SIMULATION DOCUMENT:
${simulationDoc}

Respond with ONLY a valid JSON object. No markdown fences, no preamble.

{
  "result": "PASS" or "FAIL",
  "summary": "one sentence describing the overall finding",
  "issues": [
    {
      "id": "SIM-XX",
      "severity": "CRITICAL" or "HIGH" or "MEDIUM",
      "rule": "the spec rule that is broken, quoted verbatim",
      "description": "precise description of the problem",
      "recommendation": "minimum spec change that fixes this"
    }
  ],
  "passedSimulations": ["SIM-01", "SIM-03"],
  "failedSimulations": ["SIM-02"],
  "objectAccumulationRisk": true or false,
  "startStatePlayable": true or false
}

Classification rules:
- result is FAIL if ANY issue is CRITICAL or HIGH
- result is FAIL if startStatePlayable is false
- result is PASS only if all issues are MEDIUM or lower AND startStatePlayable is true
- startStatePlayable is false if any start-state simulation found the
  player cannot perform the primary action at the initial game state
- objectAccumulationRisk is true if any simulation found a spawn/despawn
  cycle with no pooling requirement stated in the spec
- issues array is empty if all simulations passed cleanly`;
}

/* ── stripArrayFences — strips markdown fences, finds first JSON array ── */
function stripArrayFences(text) {
  let cleaned = text
    .replace(/^```json\s*/i, '')
    .replace(/^```\s*/i, '')
    .replace(/\s*```$/i, '')
    .trim();
  const firstBracket = cleaned.indexOf('[');
  const lastBracket  = cleaned.lastIndexOf(']');
  if (firstBracket >= 0 && lastBracket > firstBracket) {
    cleaned = cleaned.substring(firstBracket, lastBracket + 1);
  }
  return cleaned.trim();
}

/* ── Call 4: Patch — adds missing rules to the Master Prompt ─── */
/* Only called for MEDIUM-severity issues. Never changes an         */
/* existing rule — only adds what is missing.                       */
function buildPatchPrompt(masterPrompt, mediumIssues) {
  const issueBlock = mediumIssues.map((issue, i) =>
`Gap ${i + 1} (${issue.id}):
  Problem:     ${issue.description}
  Missing from: ${issue.rule || 'spec — rule not found or underspecified'}
  Add this:    ${issue.recommendation}`
  ).join('\n\n');

  return `You are a game spec editor. You have been given a Master Game \
Prompt and a list of MEDIUM severity spec gaps — rules that are missing \
or underspecified. Your job is to add the missing rules.

STRICT CONSTRAINTS:
- Add ONLY what is needed to resolve the listed gaps.
- Do NOT change any existing rule, formula, condition, or threshold.
- Do NOT invent new gameplay mechanics.
- Do NOT restructure or reformat the existing prompt.
- Append new rules in the same numbered-list style already used.
- Output the COMPLETE updated Master Prompt — every existing line intact.

ORIGINAL MASTER PROMPT:
${masterPrompt}

SPEC GAPS TO RESOLVE:
${issueBlock}

Output the complete updated Master Prompt with the missing rules added.`;
}

/* ── Run one full validation pass (Calls 1-2-3) ─────────────── */
/* Extracted so the retry loop can call it cleanly.               */
async function runSingleValidationPass(apiKey, masterPrompt, scenarios, bucket, projectPath, attempt, imageBlocks = []) {
  const imageValidationPreamble = imageBlocks.length > 0
    ? `\n\nREFERENCE IMAGES: ${imageBlocks.length} game reference image(s) are attached. When evaluating object/entity depth or complexity, treat visual evidence in these images as authoritative. If an image shows more depth, detail, or object complexity than the spec text describes, classify the discrepancy as MEDIUM severity rather than HIGH — the spec may intentionally be terse while the image defines the true target.\n`
    : '';

  // Call 2: Simulate
  const simResult = await callClaude(apiKey, {
    model:       'claude-sonnet-4-20250514',
    maxTokens:   8000,
    system:      'You are a game logic validator. Be precise and literal.',
    userContent: [
      { type: 'text', text: imageValidationPreamble + buildSimulationPrompt(masterPrompt, scenarios) },
      ...imageBlocks
    ]
  });
  const simulationDoc = simResult.text;

  try {
    await bucket.file(`${projectPath}/ai_validation_simulation${attempt > 0 ? `_patch${attempt}` : ''}.txt`)
      .save(simulationDoc, { contentType: 'text/plain', resumable: false });
  } catch (e) { /* non-fatal */ }

  // Call 3: Review
  const reviewResult = await callClaude(apiKey, {
    model:       'claude-sonnet-4-20250514',
    maxTokens:   6000,
    system:      'You are a spec review classifier. Respond only with a valid JSON object.',
    userContent: [
      { type: 'text', text: buildReviewPrompt(simulationDoc, scenarios) },
      ...imageBlocks
    ]
  });
  const reviewData = JSON.parse(stripFences(reviewResult.text));

  try {
    await bucket.file(`${projectPath}/ai_validation_review${attempt > 0 ? `_patch${attempt}` : ''}.json`)
      .save(JSON.stringify(reviewData, null, 2), { contentType: 'application/json', resumable: false });
  } catch (e) { /* non-fatal */ }

  return { simulationDoc, reviewData };
}

/* ── Main validation gate orchestrator — with patch retry loop ── */
async function runSpecValidationGate(apiKey, masterPrompt, progress, bucket, projectPath, jobId, imageBlocks = []) {
  console.log(`[VALIDATION] Starting spec validation gate for job ${jobId}`);

  const MAX_PATCH_ATTEMPTS = 2;

  // Update progress so the frontend shows a validating state
  progress.status = 'validating';
  progress.validationStartTime = Date.now();
  await saveProgress(bucket, projectPath, progress);

  // ── Call 1: Extract scenarios (runs once — same scenarios for all passes) ──
  console.log('[VALIDATION] Call 1: extracting game-specific scenarios...');
  let scenarios;
  try {
    const extractResult = await callClaude(apiKey, {
      model:       'claude-sonnet-4-20250514',
      maxTokens:   3000,
      system:      'You are a game logic analyst. Respond only with a valid JSON array.',
      userContent: [
        { type: 'text', text: buildExtractionPrompt(masterPrompt) },
        ...imageBlocks
      ]
    });
    scenarios = JSON.parse(stripArrayFences(extractResult.text));
    if (!Array.isArray(scenarios) || scenarios.length === 0) throw new Error('Empty scenario array');
    console.log(`[VALIDATION] Extracted ${scenarios.length} scenario(s): ${scenarios.map(s => s.id).join(', ')}`);
  } catch (e) {
    console.warn(`[VALIDATION] Call 1 failed (${e.message}) — skipping validation, proceeding to planning`);
    progress.status = 'planning';
    progress.validationSkipped = true;
    progress.validationSkipReason = e.message;
    await saveProgress(bucket, projectPath, progress);
    return { passed: true, skipped: true, activePrompt: masterPrompt };
  }

  try {
    await bucket.file(`${projectPath}/ai_validation_scenarios.json`)
      .save(JSON.stringify(scenarios, null, 2), { contentType: 'application/json', resumable: false });
  } catch (e) { /* non-fatal */ }

  progress.validationScenarios = scenarios.map(s => s.id);
  progress.validationCall1Done = true;
  await saveProgress(bucket, projectPath, progress);

  // ── Patch retry loop — Calls 2 + 3 (+ optional Call 4 patch) ────────────
  let activePrompt   = masterPrompt;
  let patchAttempt   = 0;
  let allPatchHistory = [];  // accumulates every patch attempt for the UI

  for (let pass = 0; pass <= MAX_PATCH_ATTEMPTS; pass++) {

    const isRetry = pass > 0;
    console.log(`[VALIDATION] ${isRetry ? `Patch attempt ${pass}/${MAX_PATCH_ATTEMPTS}:` : 'Initial pass:'} running Calls 2+3...`);

    if (isRetry) {
      progress.validationPatchAttempt = pass;
      progress.validationPatchStatus  = 'simulating';
      await saveProgress(bucket, projectPath, progress);
    } else {
      progress.validationCall2Done = false;
      progress.validationCall3Done = false;
      await saveProgress(bucket, projectPath, progress);
    }

    // ── Calls 2 + 3 ────────────────────────────────────────────────────────
    let simulationDoc, reviewData;
    try {
      ({ simulationDoc, reviewData } = await runSingleValidationPass(
        apiKey, activePrompt, scenarios, bucket, projectPath, pass, imageBlocks
      ));
    } catch (e) {
      console.warn(`[VALIDATION] Calls 2/3 failed on pass ${pass} (${e.message}) — skipping, proceeding to planning`);
      progress.status = 'planning';
      progress.validationSkipped = true;
      progress.validationSkipReason = e.message;
      await saveProgress(bucket, projectPath, progress);
      return { passed: true, skipped: true, activePrompt };
    }

    progress.validationCall2Done  = true;
    progress.validationCall3Done  = true;
    progress.validationResult     = reviewData.result;
    progress.validationSummary    = reviewData.summary;
    progress.validationIssues     = reviewData.issues || [];
    progress.validationEndTime    = Date.now();
    await saveProgress(bucket, projectPath, progress);

    console.log(`[VALIDATION] Pass ${pass} result: ${reviewData.result} — ${reviewData.summary}`);

    // ── PASS ────────────────────────────────────────────────────────────────
    if (reviewData.result === 'PASS') {
      progress.status = 'planning';
      progress.validationActivePromptPatched = activePrompt !== masterPrompt;
      await saveProgress(bucket, projectPath, progress);

      // If the prompt was patched, persist the patched version so Opus uses it
      // and preserve the original for the user's reference
      if (activePrompt !== masterPrompt) {
        try {
          await bucket.file(`${projectPath}/ai_validation_original_prompt.txt`)
            .save(masterPrompt, { contentType: 'text/plain', resumable: false });
          await bucket.file(`${projectPath}/ai_validation_patched_prompt.txt`)
            .save(activePrompt, { contentType: 'text/plain', resumable: false });
          console.log(`[VALIDATION] Patched prompt saved. Original preserved.`);
        } catch (e) { /* non-fatal */ }
      }

      return {
        passed:                 true,
        result:                 'PASS',
        summary:                reviewData.summary,
        issues:                 [],
        passedSimulations:      reviewData.passedSimulations || [],
        failedSimulations:      [],
        objectAccumulationRisk: reviewData.objectAccumulationRisk,
        startStatePlayable:     reviewData.startStatePlayable,
        scenarios,
        simulationDoc,
        activePrompt,              // ← caller uses this for Opus, not the original
        wasPatched:             activePrompt !== masterPrompt,
        patchCount:             pass,
        patchHistory:           allPatchHistory
      };
    }

    // ── FAIL — check severity split ─────────────────────────────────────────
    const issues      = reviewData.issues || [];
    const hardIssues  = issues.filter(i => i.severity === 'CRITICAL' || i.severity === 'HIGH');
    const mediumIssues = issues.filter(i => i.severity === 'MEDIUM');

    // Hard failures always halt immediately — never attempt to patch
    if (hardIssues.length > 0) {
      console.log(`[VALIDATION] Hard FAIL (${hardIssues.length} CRITICAL/HIGH) — halting, no auto-patch`);
      progress.status = 'validating';
      await saveProgress(bucket, projectPath, progress);
      return {
        passed:                 false,
        hardStop:               true,
        result:                 'FAIL',
        summary:                reviewData.summary,
        issues,
        passedSimulations:      reviewData.passedSimulations || [],
        failedSimulations:      reviewData.failedSimulations || [],
        objectAccumulationRisk: reviewData.objectAccumulationRisk,
        startStatePlayable:     reviewData.startStatePlayable,
        scenarios,
        simulationDoc,
        activePrompt,
        patchHistory:           allPatchHistory
      };
    }

    // ── MEDIUM-only FAIL — attempt patch if budget remains ──────────────────
    if (pass >= MAX_PATCH_ATTEMPTS) {
      // Budget exhausted
      console.log(`[VALIDATION] Patch budget exhausted after ${pass} attempt(s) — halting`);
      progress.status = 'validating';
      await saveProgress(bucket, projectPath, progress);
      return {
        passed:                 false,
        budgetExhausted:        true,
        result:                 'FAIL',
        summary:                reviewData.summary,
        issues,
        passedSimulations:      reviewData.passedSimulations || [],
        failedSimulations:      reviewData.failedSimulations || [],
        objectAccumulationRisk: reviewData.objectAccumulationRisk,
        startStatePlayable:     reviewData.startStatePlayable,
        scenarios,
        simulationDoc,
        activePrompt,
        patchHistory:           allPatchHistory
      };
    }

    // ── Call 4: Patch ────────────────────────────────────────────────────────
    patchAttempt = pass + 1;
    console.log(`[VALIDATION] MEDIUM-only fail. Running Call 4 (patch attempt ${patchAttempt})...`);
    progress.validationPatchAttempt = patchAttempt;
    progress.validationPatchStatus  = 'patching';
    await saveProgress(bucket, projectPath, progress);

    let patchedPrompt;
    try {
      const patchResult = await callClaude(apiKey, {
        model:       'claude-sonnet-4-20250514',
        maxTokens:   masterPrompt.length > 20000 ? 16000 : 8000,
        system:      'You are a game spec editor. Output only the updated Master Prompt.',
        userContent: [{ type: 'text', text: buildPatchPrompt(activePrompt, mediumIssues) }]
      });
      patchedPrompt = patchResult.text.trim();
      if (!patchedPrompt || patchedPrompt.length < activePrompt.length * 0.8) {
        throw new Error('Patch produced a truncated or empty prompt');
      }
    } catch (e) {
      console.warn(`[VALIDATION] Call 4 patch failed (${e.message}) — halting validation`);
      progress.status = 'validating';
      await saveProgress(bucket, projectPath, progress);
      return {
        passed:  false,
        result:  'FAIL',
        summary: reviewData.summary,
        issues,
        passedSimulations:      reviewData.passedSimulations || [],
        failedSimulations:      reviewData.failedSimulations || [],
        objectAccumulationRisk: reviewData.objectAccumulationRisk,
        startStatePlayable:     reviewData.startStatePlayable,
        scenarios,
        simulationDoc,
        activePrompt,
        patchHistory: allPatchHistory
      };
    }

    // Save patch artifact
    allPatchHistory.push({ attempt: patchAttempt, issues: mediumIssues.map(i => i.id) });
    try {
      await bucket.file(`${projectPath}/ai_validation_patched_prompt_${patchAttempt}.txt`)
        .save(patchedPrompt, { contentType: 'text/plain', resumable: false });
    } catch (e) { /* non-fatal */ }

    progress.validationPatchStatus  = 'retrying';
    progress.validationPatchHistory = allPatchHistory;
    await saveProgress(bucket, projectPath, progress);

    activePrompt = patchedPrompt;
    console.log(`[VALIDATION] Patch ${patchAttempt} applied (${patchedPrompt.length} chars). Re-running validation...`);
  }

  // Should not reach here — loop exits via return inside
  return { passed: false, result: 'FAIL', activePrompt };
}

/* ═══════════════════════════════════════════════════════════════ */
exports.handler = async (event) => {
  let projectPath = null;
  let bucket = null;
  let jobId = null;

  try {
    if (!event.body) throw new Error("Missing request body");

    const parsedBody = JSON.parse(event.body);
    projectPath = parsedBody.projectPath;
    jobId = parsedBody.jobId;

    if (!projectPath) throw new Error("Missing projectPath");
    if (!jobId) throw new Error("Missing jobId");

    const apiKey = process.env.ANTHROPIC_API_KEY;
    if (!apiKey) throw new Error("Missing ANTHROPIC_API_KEY");

    bucket = admin.storage().bucket(process.env.FIREBASE_STORAGE_BUCKET || "gokudatabase.firebasestorage.app");

    // ── Determine mode: "plan" / "tranche" ──────────────────────
    const mode = parsedBody.mode || "plan";
    const nextTranche = parsedBody.nextTranche || 0;

    // ══════════════════════════════════════════════════════════════
    //  MODE: "plan" — First invocation, do planning then chain
    // ══════════════════════════════════════════════════════════════
    if (mode === "plan") {

      // ── 1. Download the request payload from Firebase ─────────
      const requestFile = bucket.file(`${projectPath}/ai_request.json`);
      const [content] = await requestFile.download();
      const { prompt, files, selectedAssets, inlineImages, modelAnalysis } = JSON.parse(content.toString());
      if (!prompt) throw new Error("Missing instructions inside payload");

      // ── 2. Spec Validation Gate ───────────────────────────────
      // Runs three Sonnet calls against the Master Prompt before
      // Opus planning starts. On FAIL, writes ai_error.json with
      // structured issues and halts without invoking Opus.
      // On any internal error the gate is skipped so a bad day
      // at the API doesn't block every game build.
      const earlyProgress = {
        jobId,
        status: 'validating',
        validationStartTime: Date.now()
      };
      await saveProgress(bucket, projectPath, earlyProgress);

      // Build imageBlocks early so validation gate can use them
      const earlyImageBlocks = [];
      if (inlineImages && Array.isArray(inlineImages)) {
        for (const img of inlineImages) {
          if (img.data && img.mimeType && img.mimeType.startsWith('image/')) {
            earlyImageBlocks.push({ type: 'image', source: { type: 'base64', media_type: img.mimeType, data: img.data } });
          }
        }
      }

      const validationResult = await runSpecValidationGate(
        apiKey, prompt, earlyProgress, bucket, projectPath, jobId, earlyImageBlocks
      );

      if (!validationResult.passed && !validationResult.skipped) {
        // Write structured error so the frontend polling loop picks it up
        await bucket.file(`${projectPath}/ai_error.json`).save(
          JSON.stringify({
            error:            `Spec validation FAILED — ${validationResult.issues.length} issue(s) must be resolved in the Master Prompt before code generation can proceed.`,
            jobId,
            validationFailed:    true,
            hardStop:            validationResult.hardStop        || false,
            budgetExhausted:     validationResult.budgetExhausted || false,
            summary:             validationResult.summary,
            issues:              validationResult.issues,
            passedSimulations:   validationResult.passedSimulations,
            failedSimulations:   validationResult.failedSimulations,
            objectAccumulationRisk: validationResult.objectAccumulationRisk,
            startStatePlayable:     validationResult.startStatePlayable,
            patchHistory:        validationResult.patchHistory || []
          }),
          { contentType: 'application/json', resumable: false }
        );
        console.log(`[VALIDATION] FAILED — halting pipeline. Issues: ${validationResult.issues.map(i => i.id).join(', ')}`);
        return { statusCode: 200, body: JSON.stringify({ success: false, validationFailed: true }) };
      }

      console.log(`[VALIDATION] ${validationResult.skipped ? 'SKIPPED (error in gate)' : 'PASSED'} — proceeding to Opus planning`);

      // Use the active prompt (patched or original) for all subsequent pipeline calls
      // If patched, the patched version is already saved in Firebase for the user's reference
      const effectivePrompt = (validationResult.activePrompt) || prompt;

      // ── 3. Build file context string ──────────────────────────
      let fileContext = "Here are the current project files:\n\n";
      if (files) {
        for (const [path, fileContent] of Object.entries(files)) {
          fileContext += `--- FILE: ${path} ---\n${fileContent}\n\n`;
        }
      }

      // ── 3. Build multi-modal content blocks ───────────────────
      const imageBlocks = [];

      if (selectedAssets && Array.isArray(selectedAssets) && selectedAssets.length > 0) {
        let assetContext = "\n\nThe user has designated the following files for use. Their relative paths in the project are:\n";
        for (const asset of selectedAssets) {
          assetContext += `- ${asset.path}\n`;
          const isSupportedImage =
            (asset.type && asset.type.startsWith("image/")) ||
            (asset.name && asset.name.match(/\.(png|jpe?g|webp)$/i));

          if (isSupportedImage) {
            try {
              const assetRes = await fetch(asset.url);
              if (!assetRes.ok) throw new Error(`Failed to fetch: ${assetRes.statusText}`);
              const arrayBuffer = await assetRes.arrayBuffer();
              const base64Data = Buffer.from(arrayBuffer).toString("base64");
              let mime = asset.type;
              if (!mime || !mime.startsWith("image/")) {
                if (asset.name.endsWith(".png")) mime = "image/png";
                else if (asset.name.endsWith(".jpg") || asset.name.endsWith(".jpeg")) mime = "image/jpeg";
                else if (asset.name.endsWith(".webp")) mime = "image/webp";
                else mime = "image/png";
              }
              imageBlocks.push({ type: "image", source: { type: "base64", media_type: mime, data: base64Data } });
            } catch (fetchErr) {
              console.error(`Failed to fetch visual asset ${asset.name}:`, fetchErr);
            }
          } else {
            assetContext += `  (Note: ${asset.name} is a non-image file. Reference it by path in code.)\n`;
          }
        }
        fileContext += assetContext;
      }

      if (modelAnalysis && Array.isArray(modelAnalysis) && modelAnalysis.length > 0) {
        fileContext += `\n\n=== THREE.JS MODEL ANALYSIS ===\n${JSON.stringify(modelAnalysis, null, 2)}\n`;
      }

      if (inlineImages && Array.isArray(inlineImages) && inlineImages.length > 0) {
        for (const img of inlineImages) {
          if (img.data && img.mimeType && img.mimeType.startsWith("image/")) {
            imageBlocks.push({ type: "image", source: { type: "base64", media_type: img.mimeType, data: img.data } });
          }
        }
      }

      // ══════════════════════════════════════════════════════════
      //  SINGLE-PASS PLANNING (Opus 4.6)
      //  Reads Master Prompt + Engine Reference + files directly.
      //  No intermediate architecture spec. No re-synthesis.
      //  Outputs tranche plan with rules embedded in each prompt.
      // ══════════════════════════════════════════════════════════

      // ── Fetch Scaffold + SDK instruction bundle ──
      const instructionBundle = await fetchInstructionBundle(bucket, projectPath);
      assertInstructionBundle(instructionBundle, "PLAN");

      // ── Load approved Asset Roster (if one was approved for this run) ──
      const approvedRosterBlock = await loadApprovedRosterBlock(bucket, projectPath);
      if (approvedRosterBlock) {
        console.log("[PLAN] Approved Asset Roster loaded — will be injected into planning and all tranche prompts.");
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
          totals: { input_tokens: 0, output_tokens: 0 }
        },
        finalMessage: null,
        error: null,
        completedTime: null,
        hardeningQueue: [],
        finalHardeningSummary: null
      };
      await saveProgress(bucket, projectPath, progress);

      const planningSystem = `You are an expert game development planner for the Cherry3D engine.

Your job: read the user's request, the existing project files, and the instruction bundle below. Then split the build into sequential, self-contained TRANCHES that can be executed one at a time by a coding AI.

${instructionBundle.combinedText}

INSTRUCTION PRECEDENCE:
1. The Cherry3D Scaffold is the immutable foundation for all future games on this platform.
2. The SDK / Engine Reference is complementary. Use it for engine facts, API details, certainty gaps, threading rules, property paths, and anti-pattern avoidance.
3. If both instruction layers apply to the same topic, the Scaffold wins for architecture, lifecycle shape, immutable sections, required state fields, and build sequencing.
4. The SDK wins for engine-level invariants and implementation facts not explicitly overridden by the Scaffold.
5. Never plan tranches that delete, replace, bypass, or work around an immutable scaffold block. Adapt the requested game to the scaffold.
6. REFERENCE IMAGES (if attached): Any images attached to this request are first-class game design inputs with authority equal to the Master Prompt. They define the intended visual style, layout, object types, and complexity level. Where the image and the text spec diverge, treat the image as the authoritative definition of what must be built. Every tranche that involves visual elements, entities, or layouts must reconcile against the attached images.

PLANNING RULES:
1. Section 6.3 is the center of gravity. Every core gameplay tranche must anchor to one or more 6.3 subsection(s), and the tranche order must follow dependency reality rather than raw document order.
2. Plan the build like a house: foundation before controls, controls before authored playfield shell, shell before gameplay loop, gameplay loop before progression/HUD, progression before feedback/polish, then final hardening.
3. Each tranche prompt must be FULLY SELF-CONTAINED — embed the exact game-specific rules, variable names, slot layouts, code snippets, and pitfall warnings from the user's request that are relevant to that tranche. Do NOT summarize away critical implementation details.
4. ALWAYS split large or complex tranches into A/B/C sub-tranches. There is no hard cap on tranche count — use as many as needed. If in doubt, split.
5. Keep tranche scope TIGHT: each tranche should implement ONE subsystem or ONE cohesive set of closely-related functions. A tranche that touches more than ~150-200 lines of new/changed code is too large and MUST be split further.
6. Every tranche must declare: kind, anchorSections, purpose, systemsTouched, filesTouched, visibleResult, safetyChecks, expectedFiles, dependencies, expertAgents, phase, and qualityCriteria.
7. The FIRST tranche must establish scaffold-compliant foundations: preserve immutable scaffold sections, extend existing factories/hooks, create materials/world build, wire shared state safely, and establish STATIC collision surfaces where required.
8. Do NOT instruct the executor to remove immutable scaffold fields/blocks or invent a replacement lifecycle when the scaffold already defines one.
9. If the scaffold already provides a section (camera stage, UI hookup, particle emitter factory, instance parent pattern, input handler shape, etc.), the tranche must explicitly extend that section instead of replacing it.
10. When the user's request contains code examples (updateInput, syncPlayerSharedMemory, ghost AI, etc.), embed those exact code examples in the relevant tranche prompts — do not paraphrase them.
11. The final tranche must be a single end-stage hardening batch anchored to Section 8 so deferred findings can be resolved in one pass.
12. SPLIT AGGRESSIVELY. More tranches = smaller context per AI call = fewer timeouts and higher quality. Never merge tranches to reduce count. A game with 6 systems should produce at least 8-12 tranches (foundation + one per system + polish + hardening). If in doubt, split into an A and B sub-tranche.
13. Target ~100-150 lines of new or changed code per tranche. Any tranche prompt that describes more than 2 distinct systems, or more than ~3 new functions, is too large and must be split.

${REQUIRED_TRANCHE_VALIDATION_BLOCK}

You must respond ONLY with a valid JSON object. No markdown, no code fences, no preamble.

{
  "analysis": "Brief planning analysis describing how you decomposed the build and why.",
  "tranches": [
    {
      "kind": "build",
      "name": "Short Name",
      "description": "2-3 sentence description of what this tranche accomplishes.",
      "anchorSections": ["6.3.1"],
      "purpose": "Why this tranche exists in the build order.",
      "systemsTouched": ["player controller", "shared state"],
      "filesTouched": ["models/2", "models/23"],
      "visibleResult": "What the user can observe working after this tranche.",
      "safetyChecks": ["Hard requirements this tranche must satisfy before moving on."],
      "expertAgents": ["agent_id_1", "agent_id_2"],
      "phase": 1,
      "dependencies": [],
      "qualityCriteria": ["Criterion 1", "Criterion 2"],
      "prompt": "THE COMPLETE, SELF-CONTAINED PROMPT for the coding AI. Embed exact game-specific rules, code examples, and pitfall warnings from the user's request. Do NOT repeat the full instruction docs, but ensure the tranche is scaffold-compliant and never violates immutable scaffold sections.",
      "expectedFiles": ["models/2", "models/23"]
    }
  ]
}`;

      const planningUserContent = [
        { type: "text", text: `${fileContext}${approvedRosterBlock}

=== FULL USER REQUEST ===
${effectivePrompt}
=== END USER REQUEST ===` },
        ...imageBlocks
      ];

      console.log(`PLANNING: Single-pass Opus 4.6 for Job ${jobId}...`);
      const planResult = await callClaude(apiKey, {
        model: "claude-opus-4-6",
        maxTokens: 100000,
        budgetTokens: 23000,
        effort: "high",
        system: planningSystem,
        userContent: planningUserContent
      });

      if (planResult.usage) {
        progress.tokenUsage.planning = planResult.usage;
        progress.tokenUsage.totals.input_tokens += planResult.usage.input_tokens || 0;
        progress.tokenUsage.totals.output_tokens += planResult.usage.output_tokens || 0;
        await saveProgress(bucket, projectPath, progress);
      }

      let plan = safeJsonParse(planResult.text, "planning");

      if (!plan.tranches || !Array.isArray(plan.tranches) || plan.tranches.length === 0) {
        throw new Error("Planner returned zero tranches.");
      }

      plan = enforceTrancheValidationBlock(plan);

      // Update progress with plan
      progress.status = "executing";
      progress.planningEndTime = Date.now();
      progress.planningAnalysis = plan.analysis || "";
      progress.totalTranches = plan.tranches.length;
      progress.currentTranche = 0;
      progress.tranches = plan.tranches.map((t, i) => ({
        index: i,
        kind: t.kind || 'build',
        isHardeningBatch: Boolean(t.isHardeningBatch || t.kind === HARDENING_BATCH_KIND),
        name: t.name,
        description: t.description,
        anchorSections: t.anchorSections || [],
        purpose: t.purpose || t.description || '',
        systemsTouched: t.systemsTouched || [],
        filesTouched: t.filesTouched || t.expectedFiles || [],
        visibleResult: t.visibleResult || '',
        safetyChecks: t.safetyChecks || [],
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
        filesUpdated: [],
        validationRetryCount: 0,
        executionRetryCount: 0,
        retryBudget: 0
      }));
      await saveProgress(bucket, projectPath, progress);

      console.log(`Plan created: ${plan.tranches.length} tranches.`);

      // ── Save pipeline state for chained invocations ──────────
      const pipelineState = {
        jobId,
        projectPath,
        progress,
        accumulatedFiles: files ? { ...files } : {},
        allUpdatedFiles: [],
        imageBlocks,
        modelAnalysis: Array.isArray(modelAnalysis) ? modelAnalysis : [],
        totalTranches: plan.tranches.length,
        approvedRosterBlock   // ← propagated to every tranche execution
      };
      await savePipelineState(bucket, projectPath, pipelineState);

      // ── Chain to first tranche ───────────────────────────────
      await chainToSelf({
        projectPath,
        jobId,
        mode: "tranche",
        nextTranche: 0
      });

      return { statusCode: 200, body: JSON.stringify({ success: true, chained: true, phase: "planning_complete" }) };
    }

    // ══════════════════════════════════════════════════════════════
    //  MODE: "tranche" — Execute one tranche, then chain to next
    // ══════════════════════════════════════════════════════════════
    if (mode === "tranche") {

      // ── Kill switch check ────────────────────────────────────
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
                jobId:         state.jobId,
                trancheIndex:  nextTranche,
                totalTranches: state.totalTranches,
                status:        "cancelled",
                message:       `Pipeline cancelled. ${state.allUpdatedFiles.length} file(s) were updated before cancellation.`
              });
            }
          }
          return { statusCode: 200, body: JSON.stringify({ success: true, cancelled: true }) };
        }
      }

      // ── Load pipeline state ──────────────────────────────────
      const state = await loadPipelineState(bucket, projectPath);
      if (!state) throw new Error("Pipeline state not found in Firebase. Chain broken.");

      const { progress, accumulatedFiles, allUpdatedFiles, imageBlocks, modelAnalysis, approvedRosterBlock = "" } = state;
      const tranche = progress.tranches[nextTranche];

      // ── Fetch Scaffold + SDK instruction bundle ──
      const instructionBundle = await fetchInstructionBundle(bucket, projectPath);
      assertInstructionBundle(instructionBundle, "TRANCHE");

      if (!tranche) throw new Error(`Tranche ${nextTranche} not found in pipeline state.`);

      const isHardeningBatch = isHardeningBatchTranche(tranche);
      if (isHardeningBatch && (!progress.hardeningQueue || progress.hardeningQueue.length === 0)) {
        progress.currentTranche = nextTranche;
        progress.tranches[nextTranche].status = "complete";
        progress.tranches[nextTranche].startTime = progress.tranches[nextTranche].startTime || Date.now();
        progress.tranches[nextTranche].endTime = Date.now();
        progress.tranches[nextTranche].message = "No queued hardening items — final hardening batch skipped to save tokens.";
        progress.finalHardeningSummary = "Hardening batch skipped because no deferred items were queued.";
        await saveProgress(bucket, projectPath, progress);

        state.progress = progress;
        await savePipelineState(bucket, projectPath, state);

        if (nextTranche + 1 < progress.totalTranches) {
          await chainToSelf({ projectPath, jobId, mode: "tranche", nextTranche: nextTranche + 1 });
          return { statusCode: 200, body: JSON.stringify({ success: true, chained: true, phase: `tranche_${nextTranche}_hardening_skipped` }) };
        }

        const summaryParts = progress.tranches
          .filter(t => t.status === "complete")
          .map((t) => `Tranche ${t.index + 1} — ${t.name}: ${t.message}`);
        if (progress.finalHardeningSummary) {
          summaryParts.push(`Final hardening: ${progress.finalHardeningSummary}`);
        }
        const finalMessage = summaryParts.join("\n\n") || "Build completed.";

        await saveAiResponse(bucket, projectPath, allUpdatedFiles, {
          jobId:         jobId,
          trancheIndex:  progress.totalTranches - 1,
          totalTranches: progress.totalTranches,
          status:        "final",
          message:       finalMessage
        });

        progress.status = "complete";
        const t = progress.tokenUsage.totals;
        progress.finalMessage = `Build complete: ${allUpdatedFiles.length} file(s) updated across ${progress.tranches.filter(tr => tr.status === "complete").length} tranche(s). Tokens: ${t.input_tokens} in / ${t.output_tokens} out.`;
        progress.completedTime = Date.now();
        await saveProgress(bucket, projectPath, progress);

        try { await bucket.file(`${projectPath}/ai_pipeline_state.json`).delete(); } catch (e) {}
        try { await bucket.file(`${projectPath}/ai_request.json`).delete(); } catch (e) {}

        return { statusCode: 200, body: JSON.stringify({ success: true, phase: "complete" }) };
      }

      // ── Mark tranche as in-progress ──────────────────────────
      progress.currentTranche = nextTranche;
      progress.tranches[nextTranche].status = "in_progress";
      progress.tranches[nextTranche].startTime = progress.tranches[nextTranche].startTime || Date.now();
      await saveProgress(bucket, projectPath, progress);

      console.log(`TRANCHE ${nextTranche + 1}/${progress.totalTranches}: ${tranche.name} (Job ${jobId})`);

      // IMPORTANT: Executors use DELIMITER FORMAT, NOT JSON.
      // Embedding raw JS/HTML code inside JSON string fields causes frequent
      // parse failures because LLMs miss-escape quotes, backslashes, and
      // newlines. Delimiters require zero escaping and are completely robust.
      const executionSystem = `You are an expert game development AI.
The user will provide project files and a focused modification request (one tranche of a larger build).

${instructionBundle.combinedText}

INSTRUCTION PRECEDENCE:
- The Cherry3D Scaffold is the immutable foundation. Treat it as the required base architecture.
- The SDK / Engine Reference is complementary. Use it whenever engine/API certainty is needed.
- If both apply, the Scaffold wins for architecture/lifecycle/state shape, and the SDK wins for engine facts and anti-pattern avoidance.
- Never delete, replace, or work around an immutable scaffold section. Extend inside it.
- REFERENCE IMAGES (if attached): Any images attached to this tranche are first-class game design inputs with authority equal to the Master Prompt. They define the intended visual appearance, entity types, layout geometry, and interaction model. When implementing this tranche, reconcile your output against the attached images — if your code would produce something visually inconsistent with an attached image, that is a defect. Visual Reconciliation is a required quality criterion for every tranche that touches rendered content.

Do not re-state the instruction docs — just apply them. This pipeline uses a tiered recovery policy: soft findings are deferred, surgical objective failures may get one retry, and only parser/envelope or truly critical scaffold/runtime issues can consume two retries.
Write it correctly the first time so the tranche can move forward without rework.

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

OUTPUT RULES:
- Only include files that actually need to be changed or created.
- Always output the COMPLETE file content for each updated file — not patches or diffs.
- Build upon the existing file contents provided. Do NOT discard or overwrite work from prior tranches.
- If the file already has functions, variables, or structures from prior tranches, KEEP THEM ALL and add your new code alongside them.
- The delimiter lines (===FILE_START:=== etc.) must appear exactly as shown, on their own lines.
- If the scaffold already defines the correct place for a system (camera stage, UI hookup, particle factory, instance parent pattern, input handler, lifecycle block), implement inside that existing scaffold section.
- Do NOT replace scaffold-owned state fields with renamed alternatives unless the tranche explicitly requires preserving both and safely extending them.
- Do NOT invent custom lifecycle blocks when the scaffold already supplies one.

VALIDATOR STATUS:
- Validation manifest requirements are temporarily disabled.
- Do NOT add VALIDATION_MANIFEST blocks unless another pipeline stage explicitly requires them.
- Focus on correct delimiter output, complete file content, scaffold compliance, and working runtime logic.`;



      // Build file context from accumulated state
      let trancheFileContext = "Here are the current project files (includes all output from prior tranches — you MUST preserve all existing code):\n\n";
      for (const [path, fileContent] of Object.entries(accumulatedFiles)) {
        trancheFileContext += `--- FILE: ${path} ---\n${fileContent}\n\n`;
      }

      if (Array.isArray(modelAnalysis) && modelAnalysis.length > 0) {
        trancheFileContext += `=== THREE.JS MODEL ANALYSIS ===\n${JSON.stringify(modelAnalysis, null, 2)}\n\n`;
      }

      assertTranchePromptHasRequiredManifestBlock(tranche, nextTranche);

      const rosterPrefix = approvedRosterBlock
        ? `=== APPROVED GAME-SPECIFIC ASSET ROSTER ===\n${approvedRosterBlock}\n=== END ASSET ROSTER ===\n\n`
        : "";

      const trancheUserText = isHardeningBatch
        ? buildHardeningBatchUserText({ progress, accumulatedFiles, tranche, modelAnalysis })
        : `${rosterPrefix}${trancheFileContext}

=== TRANCHE ${nextTranche + 1} of ${progress.totalTranches}: "${tranche.name}" ===

${tranche.prompt}

=== END TRANCHE INSTRUCTIONS ===

IMPORTANT: You are working on tranche ${nextTranche + 1} of ${progress.totalTranches}. The project files above contain ALL work from prior tranches. You MUST preserve all existing code and ADD your changes on top. Output the COMPLETE updated file contents.`;

      const trancheUserContent = [
        {
          type: "text",
          text: trancheUserText
        },
        ...(imageBlocks || [])
      ];

      let trancheResponseObj;
      try {
        trancheResponseObj = await callClaude(apiKey, {
          model: "claude-sonnet-4-6",
          maxTokens: 128000,
          budgetTokens: 10000,
          effort: "high",
          system: executionSystem,
          userContent: trancheUserContent
        });
      } catch (err) {
        progress.tranches[nextTranche].status = "error";
        progress.tranches[nextTranche].endTime = Date.now();
        progress.tranches[nextTranche].message = `Error: ${err.message}`;
        await saveProgress(bucket, projectPath, progress);
        console.error(`Tranche ${nextTranche + 1} failed:`, err.message);

        // Save state and chain to next tranche (skip this one)
        state.progress = progress;
        await savePipelineState(bucket, projectPath, state);

        // Checkpoint ai_response.json with whatever was accumulated so far
        if (allUpdatedFiles.length > 0) {
          await saveAiResponse(bucket, projectPath, allUpdatedFiles, {
            jobId:         jobId,
            trancheIndex:  nextTranche,
            totalTranches: progress.totalTranches,
            status:        "checkpoint",
            message:       `Checkpoint after tranche ${nextTranche + 1} error-skip. ${allUpdatedFiles.length} file(s) so far.`
          });
        }

        if (nextTranche + 1 < progress.totalTranches) {
          await chainToSelf({ projectPath, jobId, mode: "tranche", nextTranche: nextTranche + 1 });
          return { statusCode: 200, body: JSON.stringify({ success: true, chained: true, phase: `tranche_${nextTranche}_error_skipped` }) };
        }
        // Fall through to finalization if last tranche
      }

      // ── Process tranche response (if we got one) ─────────────
      if (trancheResponseObj) {
        // Record token usage
        if (trancheResponseObj.usage) {
          progress.tokenUsage.tranches[nextTranche] = trancheResponseObj.usage;
          progress.tokenUsage.totals.input_tokens += trancheResponseObj.usage.input_tokens || 0;
          progress.tokenUsage.totals.output_tokens += trancheResponseObj.usage.output_tokens || 0;
          progress.tranches[nextTranche].tokenUsage = trancheResponseObj.usage;
        }

        // Parse using delimiter format — no JSON escaping issues possible
        const trancheResult = parseDelimitedResponse(trancheResponseObj.text);
        if (!trancheResult) {
          const parseRetryBudget = RETRY_POLICY.parser_envelope;
          const currentParseRetry = progress.tranches[nextTranche].executionRetryCount || 0;

          console.error(`Tranche ${nextTranche + 1} produced no parseable output.`);
          console.error("Raw response (first 500 chars):", trancheResponseObj.text.slice(0, 500));

          if (currentParseRetry < parseRetryBudget) {
            const nextReplay = currentParseRetry + 1;
            progress.tranches[nextTranche].status = "retrying";
            progress.tranches[nextTranche].executionRetryCount = nextReplay;
            progress.tranches[nextTranche].retryBudget = parseRetryBudget;
            progress.tranches[nextTranche].message = `Parser/envelope issue detected — execution replay ${nextReplay}/${parseRetryBudget} queued.`;
            await saveProgress(bucket, projectPath, progress);

            state.progress = progress;
            await savePipelineState(bucket, projectPath, state);

            await chainToSelf({ projectPath, jobId, mode: "tranche", nextTranche });
            return { statusCode: 200, body: JSON.stringify({ success: true, chained: true, phase: `tranche_${nextTranche}_parse_retry_${nextReplay}` }) };
          }

          progress.tranches[nextTranche].status = "error";
          progress.tranches[nextTranche].endTime = Date.now();
          progress.tranches[nextTranche].message = `Executor returned no recognisable file delimiters after ${parseRetryBudget} parser/envelope retries.`;
          await saveProgress(bucket, projectPath, progress);
          console.error(`Tranche ${nextTranche + 1} produced no parseable output.`);
          console.error("Raw response (first 500 chars):", trancheResponseObj.text.slice(0, 500));

          state.progress = progress;
          await savePipelineState(bucket, projectPath, state);

          if (allUpdatedFiles.length > 0) {
            await saveAiResponse(bucket, projectPath, allUpdatedFiles, {
              jobId:         jobId,
              trancheIndex:  nextTranche,
              totalTranches: progress.totalTranches,
              status:        "checkpoint",
              message:       `Checkpoint after tranche ${nextTranche + 1} parser/envelope exhaustion. ${allUpdatedFiles.length} file(s) so far.`
            });
          }

          if (nextTranche + 1 < progress.totalTranches) {
            await chainToSelf({ projectPath, jobId, mode: "tranche", nextTranche: nextTranche + 1 });
            return { statusCode: 200, body: JSON.stringify({ success: true, chained: true, phase: `tranche_${nextTranche}_parse_error` }) };
          }
          // Fall through to finalization
        }

        if (trancheResult) {

          // Merge tranche output into accumulated files
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

          // Update progress: tranche complete
          progress.tranches[nextTranche].status = "complete";
          progress.tranches[nextTranche].endTime = Date.now();
          progress.tranches[nextTranche].message = trancheResult.message || "Tranche completed.";
          progress.tranches[nextTranche].filesUpdated = trancheFilesUpdated;
          await saveProgress(bucket, projectPath, progress);

          console.log(`Tranche ${nextTranche + 1} complete: ${trancheFilesUpdated.length} files updated.`);

          // ── Checkpoint ai_response.json after every successful merge ──
          // This ensures the frontend always has the latest snapshot even if
          // a later tranche or finalization step fails.
          if (allUpdatedFiles.length > 0) {
            await saveAiResponse(bucket, projectPath, allUpdatedFiles, {
              jobId:         jobId,
              trancheIndex:  nextTranche,
              totalTranches: progress.totalTranches,
              status:        "checkpoint",
              message:       `Checkpoint after tranche ${nextTranche + 1}/${progress.totalTranches}: ${trancheResult.message || "completed."}`
            });
          }
        }
      }

      // ── Save updated pipeline state ──────────────────────────
      state.progress = progress;
      state.accumulatedFiles = accumulatedFiles;
      state.allUpdatedFiles = allUpdatedFiles;
      await savePipelineState(bucket, projectPath, state);

      // ── Chain to next tranche OR finalize ─────────────────────
      if (nextTranche + 1 < progress.totalTranches) {
        await chainToSelf({
          projectPath,
          jobId,
          mode: "tranche",
          nextTranche: nextTranche + 1
        });
        return { statusCode: 200, body: JSON.stringify({ success: true, chained: true, phase: `tranche_${nextTranche}_complete` }) };
      }

      // ══════════════════════════════════════════════════════════
      //  FINAL — All tranches done, assemble and save response
      // ══════════════════════════════════════════════════════════

      const summaryParts = progress.tranches
        .filter(t => t.status === "complete")
        .map((t) => `Tranche ${t.index + 1} — ${t.name}: ${t.message}`);

      const finalMessage = summaryParts.join("\n\n") || "Build completed.";

      await saveAiResponse(bucket, projectPath, allUpdatedFiles, {
        jobId:         jobId,
        trancheIndex:  progress.totalTranches - 1,
        totalTranches: progress.totalTranches,
        status:        "final",
        message:       finalMessage
      });

      progress.status = "complete";
      const t = progress.tokenUsage.totals;
      const deferredCount = Array.isArray(progress.hardeningQueue) ? progress.hardeningQueue.length : 0;
      progress.finalMessage = `Build complete: ${allUpdatedFiles.length} file(s) updated across ${progress.tranches.filter(tr => tr.status === "complete").length} tranche(s). Tokens: ${t.input_tokens} in / ${t.output_tokens} out.${deferredCount ? ` Deferred items still queued: ${deferredCount}.` : ''}`;
      progress.completedTime = Date.now();
      await saveProgress(bucket, projectPath, progress);

      console.log(`Total tokens — input: ${t.input_tokens}, output: ${t.output_tokens}`);

      // Clean up pipeline state and request files
      try { await bucket.file(`${projectPath}/ai_pipeline_state.json`).delete(); } catch (e) {}
      try { await bucket.file(`${projectPath}/ai_request.json`).delete(); } catch (e) {}

      return { statusCode: 200, body: JSON.stringify({ success: true, phase: "complete" }) };
    }


        // NOTE: "patch_issue" mode has been moved to the synchronous function
    // netlify/functions/claudeCodePatch.js — it cannot return an inline
    // HTTP response from a background function (Netlify returns 202 immediately).

    throw new Error(`Unknown mode: ${mode}`);

  } catch (error) {
    console.error("Claude Code Proxy Background Error:", error);
    try {
      if (projectPath && bucket) {
        await bucket.file(`${projectPath}/ai_error.json`).save(
          JSON.stringify({ error: error.message }),
          { contentType: "application/json", resumable: false }
        );
        try {
          await saveProgress(bucket, projectPath, {
            jobId: jobId || "unknown",
            status: "error",
            error: error.message,
            completedTime: Date.now()
          });
        } catch (e2) {}
      }
    } catch (e) {
      console.error("CRITICAL: Failed to write error to Firebase.", e);
    }

    return { statusCode: 500, body: JSON.stringify({ error: error.message }) };
  }
};