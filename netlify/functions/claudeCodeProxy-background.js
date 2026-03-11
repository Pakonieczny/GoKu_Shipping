/* netlify/functions/claudeCodeProxy-background.js */
/* ═══════════════════════════════════════════════════════════════════
   TRANCHED AI PIPELINE — v3.0 (Self-Chaining)
   ─────────────────────────────────────────────────────────────────
   Each invocation handles ONE unit of work then chains to itself
   for the next, staying well under Netlify's 15-min limit.

   Invocation 0  ▸  "plan"  — Opus 4.6 plans tranches, saves state
   Invocation 1–N ▸ "tranche" — Sonnet 4.6 executes one tranche,
                     saves accumulated files, chains to next tranche
   Final          ▸ Writes ai_response.json so the frontend picks
                     up the completed build.

   All intermediate state lives in Firebase so each invocation is
   stateless and can reconstruct context from the pipeline file.
   ═══════════════════════════════════════════════════════════════════ */

const fetch = require("node-fetch");
const admin = require("./firebaseAdmin");


const HARD_ENVIRONMENT_CONSTRAINTS = `HARD ENVIRONMENT CONSTRAINTS (platform facts — never reinterpret):
1. Output format must use delimiter blocks exactly:
   ===FILE_START: path=== ... ===FILE_END: path===
   followed by ===MESSAGE=== ... ===END_MESSAGE===
2. Valid target files are models/2, models/23, and json/assets.json.
3. The main logic file is models/2. Never rename it to WorldController.js.
4. The main HTML file is models/23. Never rename it to document.html.
5. Preserve accumulated code from prior tranches. Output complete file contents, never diffs.
6. Respect the platform's staged execution model: one planning pass followed by sequential tranche execution.
7. Heavy engine/physics setup must respect deferred readiness patterns and isReady polling when the SDK requires it.
8. The validation manifest block is mandatory in every emitted file.
9. Rigidbody rbPosition is a LOCAL offset from its parent object. Use [0,0,0] unless you need intentional displacement from the visual mesh center. Passing world coordinates causes POSITION DOUBLING because the engine compounds parent transform + child offset.
10. Every surface that must block a DYNAMIC body needs its own STATIC rigidbody — including the floor. A visual-only mesh provides zero collision resistance; DYNAMIC actors will fall through it.
11. File 2 (models/2) and file 23 (models/23) run in SEPARATE window contexts. window.* globals set in one file do NOT exist in the other. All cross-file communication must use DOM element references obtained from Module.ProjectManager.getObject(id).DOMElement, or boolean flag polling read each frame.
12. After any HTML button click or modal dismissal, set pointer-events:none on the overlay root so input events reach the engine canvas. Never call blur() or focus() on engine-managed elements — this breaks the engine's keyboard capture pipeline. Always add a document.addEventListener('keydown') fallback that writes directly to gameState for movement input.
13. Use the full property path rb.RigidBody.controls.setFloat / setInt / addInputHandler / getMotionState. The shorthand rb.controls is a SILENT NO-OP in most configurations — no error is thrown, but nothing happens.
14. Do NOT use controls.getFloat() or controls.getInt() for reading position or state back from the physics thread — these are unreliable and may always return 0. Instead use rb.RigidBody.getMotionState() for position/velocity, and compute derived state (tileCentered, wallBlocked) on the main thread from position data.
15. After spawning any DYNAMIC rigidbody actor, verify within the first 3 render frames that the visual mesh position tracks the physics body position. If it does not auto-sync, add explicit sync every frame: obj.position = [motionState.position[0], y, motionState.position[2]].
16. For grid-based games, actor collision bounding boxes must provide at least 15% clearance relative to tile size (e.g., half-extent 0.35 for 1.0-unit tiles). Tighter tolerances cause actors to wedge into adjacent walls at corners.
17. Tile-centering corrections in physics updateInput must ONLY apply to the axis perpendicular to movement. Correcting the movement axis will fight the movement velocity and can produce exact cancellation, freezing the actor.`;

const DYNAMIC_ARCHITECTURE_JSON_SCHEMA = `{
  "summary": "2-4 sentence synthesis of the exact game architecture required for THIS request.",
  "gameType": "Short label for the requested game/class of interaction.",
  "stateModel": [
    "Specific state domains and canonical state variables that must exist for this game."
  ],
  "actorModel": [
    {
      "actor": "actor_name",
      "role": "what it does",
      "representation": "entity/data structure/UI element",
      "motion": "dynamic|kinematic|static|logical_only|n/a",
      "rules": ["critical behavior or collision rules"]
    }
  ],
  "systems": [
    {
      "name": "system name",
      "purpose": "what it must do",
      "sdkBindings": ["specific SDK APIs/patterns that must be used or avoided"],
      "implementationRules": ["strict dynamic rules for this request only"],
      "antiPatterns": ["what would break the design if done naively"]
    }
  ],
  "invariants": ["cross-system laws that may not be violated"],
  "failureGuards": ["checks/orderings needed to avoid known failure modes"],
  "trancheHints": ["how the builder should sequence implementation work"],
  "failureModes": [
    {
      "symptom": "what the developer or player would observe at runtime (e.g., actor at wrong position, input dead, UI not updating)",
      "cause": "underlying engine/API/context issue that produces this symptom",
      "prevention": "specific implementation pattern the generated code MUST use to avoid this failure"
    }
  ]
}`;

/* ── helper: call Claude API ─────────────────────────────────── */
async function callClaude(apiKey, { model, maxTokens, system, userContent, effort, budgetTokens }) {
  const body = {
    model,
    max_tokens: maxTokens,
    thinking: { type: "enabled", budget_tokens: budgetTokens || 10000 },
    system,
    messages: [{ role: "user", content: userContent }]
  };

  if (effort) {
    body.output_config = { effort };
  }

  const res = await fetch("https://api.anthropic.com/v1/messages", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "x-api-key": apiKey,
      "anthropic-version": "2023-06-01"
    },
    body: JSON.stringify(body)
  });

  const data = await res.json();
  if (!res.ok) throw new Error(data.error?.message || `Claude API error (${res.status})`);

  const responseText = data.content?.find(b => b.type === "text")?.text;
  if (!responseText) throw new Error("Empty response from Claude");

  return {
    text: responseText,
    usage: data.usage || null
  };
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

function buildArchitectureSpecBlock(spec) {
  if (!spec) return "";
  const pretty = typeof spec === "string" ? spec : JSON.stringify(spec, null, 2);
  return `
╔═════════════ DYNAMIC GAME ARCHITECTURE SPEC (BINDING) ═════════════╗
║ These rules were synthesized from the user's request, contract,   ║
║ existing files, and SDK context. They define the game-specific    ║
║ architecture for THIS build only. Do not replace them with        ║
║ generic engine advice or hardcoded one-size-fits-all rules.       ║
╚════════════════════════════════════════════════════════════════════╝
${pretty}
`;
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
  return String(haystack || '').split(needle).length - 1;
}

function assertTranchePromptHasRequiredManifestBlock(tranche, index) {
  const prompt = String(tranche?.prompt || '').replace(/\r\n/g, '\n').trim();
  const label = `tranche ${index + 1}${tranche?.name ? ` (${tranche.name})` : ''}`;

  if (!prompt) {
    throw new Error(`Pre-execution tranche manifest assertion failed for ${label}: prompt is empty.`);
  }

  const occurrenceCount = countOccurrences(prompt, REQUIRED_TRANCHE_VALIDATION_BLOCK);
  if (occurrenceCount !== 1) {
    throw new Error(`Pre-execution tranche manifest assertion failed for ${label}: expected exactly 1 verbatim validation block, found ${occurrenceCount}.`);
  }

  const requiredFragments = [
    'VALIDATION MANIFEST RULE (copy this block verbatim into EVERY tranche prompt you generate):',
    'MANDATORY VALIDATION MANIFEST: Every file you output MUST contain a machine-readable',
    'VALIDATION_MANIFEST_START',
    '"file": "<exact file path e.g. models/2>"',
    '"systems": [',
    '"id": "<snake_case_system_id>"',
    '"keywords": ["keyword1", "keyword2"]',
    '"notes": "what this file implements for this system"',
    'VALIDATION_MANIFEST_END',
    '4. Do NOT omit the markers',
    '5. This same marker format applies to EVERY file type, including json/assets.json.'
  ];

  const missingFragments = requiredFragments.filter(fragment => !prompt.includes(fragment));
  if (missingFragments.length) {
    throw new Error(`Pre-execution tranche manifest assertion failed for ${label}: missing required manifest block fragment(s): ${missingFragments.join(' | ')}`);
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
    throw new Error(`Deterministic tranche manifest assertion failed: ${failures.join('; ')}`);
  }

  return plan;
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

    // ── Determine mode: "plan" (initial) or "tranche" (chained) ──
    const mode = parsedBody.mode || "plan";
    const nextTranche = parsedBody.nextTranche || 0;

    // ══════════════════════════════════════════════════════════════
    //  MODE: "plan" — First invocation, do planning then chain
    // ══════════════════════════════════════════════════════════════
    if (mode === "plan") {

      // ── 1. Download the request payload from Firebase ─────────
      const requestFile = bucket.file(`${projectPath}/ai_request.json`);
      const [content] = await requestFile.download();
      const { prompt, files, selectedAssets, inlineImages, gameContract } = JSON.parse(content.toString());
      if (!prompt) throw new Error("Missing instructions inside payload");

      // ── 2. Build file context string ──────────────────────────
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

      if (inlineImages && Array.isArray(inlineImages) && inlineImages.length > 0) {
        for (const img of inlineImages) {
          if (img.data && img.mimeType && img.mimeType.startsWith("image/")) {
            imageBlocks.push({ type: "image", source: { type: "base64", media_type: img.mimeType, data: img.data } });
          }
        }
      }

      // ══════════════════════════════════════════════════════════
      //  STAGE 0 — PLANNING (Opus 4.6, adaptive, high)
      // ══════════════════════════════════════════════════════════
      const progress = {
        jobId: jobId,
        status: "planning",
        planningStartTime: Date.now(),
        planningEndTime: null,
        planningAnalysis: "",
        architectureSpec: null,
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
        completedTime: null
      };
      await saveProgress(bucket, projectPath, progress);

      const architectSystem = `You are the ARCHITECT pass in a two-pass game generation pipeline.

Your job is NOT to write code and NOT to split tranches yet.
Read the user's request, existing files, optional game contract, and any SDK guidance present in the supplied context.
Synthesize the GAME-SPECIFIC architecture laws that the later builder/planner must obey.

${HARD_ENVIRONMENT_CONSTRAINTS}

Separate platform facts from game rules:
- Environment constraints stay hardcoded and are already provided above.
- Dynamic architecture rules must be inferred fresh for THIS exact game.
- Do NOT invent rigid one-size-fits-all requirements such as a universal actor table, fixed physics model, or generic grid math unless this request truly requires them.
- Only specify motion types, actor categories, camera laws, grid laws, or state machines when supported by the request/contract/SDK context.
- Resolve conflicts in favor of the user's game contract and the SDK-compatible implementation path.

Return ONLY valid JSON matching this schema:
${DYNAMIC_ARCHITECTURE_JSON_SCHEMA}`;

      const architectUserContent = [
        { type: "text", text: `${fileContext}

=== OPTIONAL GAME CONTRACT ===
${gameContract || 'None provided.'}
=== END GAME CONTRACT ===

=== FULL USER REQUEST (derive game-specific architecture laws) ===
${prompt}
=== END USER REQUEST ===` },
        ...imageBlocks
      ];

      console.log(`STAGE 0A: Architect pass with Claude Sonnet 4.6 for Job ${jobId}...`);
      const architectResult = await callClaude(apiKey, {
        model: "claude-sonnet-4-6",
        maxTokens: 32000,
        budgetTokens: 12000,
        effort: "high",
        system: architectSystem,
        userContent: architectUserContent
      });

      if (architectResult.usage) {
        progress.tokenUsage.planning = architectResult.usage;
        progress.tokenUsage.totals.input_tokens += architectResult.usage.input_tokens || 0;
        progress.tokenUsage.totals.output_tokens += architectResult.usage.output_tokens || 0;
        await saveProgress(bucket, projectPath, progress);
      }

      const architectureSpec = safeJsonParse(architectResult.text, "architect");
      progress.architectureSpec = architectureSpec;
      await saveProgress(bucket, projectPath, progress);

      const architectureSpecBlock = buildArchitectureSpecBlock(architectureSpec);
      const planningSystem = `You are the BUILDER-PLANNER pass in a two-pass game generation pipeline.

Your job: use the hard environment constraints plus the dynamic architecture spec to split the user's request into sequential, self-contained TRANCHES that can be executed one at a time by a coding AI.

${HARD_ENVIRONMENT_CONSTRAINTS}
${architectureSpecBlock}

PLANNING RULES:
1. Each tranche should focus on 1-2 closely related concerns.
2. Tranches MUST be ordered by dependency — later tranches build on earlier ones.
3. Each tranche prompt must be FULLY SELF-CONTAINED.
4. Preserve exact technical details, variable names, slot layouts, code snippets, and SDK usage requirements from the source material.
5. If the request is simple enough, use 1 tranche. Otherwise use the minimum count that preserves correctness.
6. Each tranche must declare expectedFiles, dependencies, expertAgents, phase, and qualityCriteria.
7. The FIRST tranche should establish the exact foundations required by the architecture spec.
8. The LAST tranche should handle integration, edge cases, and polish.
9. Do NOT inject generic rules that contradict the architecture spec. If a rule is game-specific, ground it in the architecture spec.
10. In every tranche prompt, clearly separate HARD ENVIRONMENT CONSTRAINTS from GAME-SPECIFIC ARCHITECTURE RULES.

${REQUIRED_TRANCHE_VALIDATION_BLOCK}

You must respond ONLY with a valid JSON object. No markdown, no code fences, no preamble.

{
  "analysis": "Brief planning analysis describing how the architecture spec shapes the tranche sequence.",
  "tranches": [
    {
      "name": "Short Name",
      "description": "2-3 sentence description of what this tranche accomplishes.",
      "expertAgents": ["agent_id_1", "agent_id_2"],
      "phase": 1,
      "dependencies": [],
      "qualityCriteria": ["Criterion 1", "Criterion 2"],
      "prompt": "THE COMPLETE, SELF-CONTAINED PROMPT for the coding AI. It must embed the relevant architecture rules for this tranche.",
      "expectedFiles": ["models/2", "models/23"]
    }
  ]
}`;

      const planningUserContent = [
        { type: "text", text: `${fileContext}

=== OPTIONAL GAME CONTRACT ===
${gameContract || 'None provided.'}
=== END GAME CONTRACT ===

=== DYNAMIC ARCHITECTURE SPEC ===
${JSON.stringify(architectureSpec, null, 2)}
=== END DYNAMIC ARCHITECTURE SPEC ===

=== FULL USER REQUEST (plan tranche execution) ===
${prompt}
=== END USER REQUEST ===` },
        ...imageBlocks
      ];

      console.log(`STAGE 0B: Builder-plan pass with Opus 4.6 for Job ${jobId}...`);
      const planResult = await callClaude(apiKey, {
        model: "claude-opus-4-6",
        maxTokens: 128000,
        budgetTokens: 25000,
        effort: "high",
        system: planningSystem,
        userContent: planningUserContent
      });

      if (planResult.usage) {
        progress.tokenUsage.planning = {
          input_tokens: (progress.tokenUsage.planning?.input_tokens || 0) + (planResult.usage.input_tokens || 0),
          output_tokens: (progress.tokenUsage.planning?.output_tokens || 0) + (planResult.usage.output_tokens || 0)
        };
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
      progress.planningAnalysis = [
        architectureSpec?.summary ? `ARCHITECT SUMMARY:\n${architectureSpec.summary}` : "",
        plan.analysis ? `BUILDER PLAN SUMMARY:\n${plan.analysis}` : ""
      ].filter(Boolean).join("\n\n");
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
        gameContract: gameContract || null,
        architectureSpec,
        totalTranches: plan.tranches.length
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

      const { progress, accumulatedFiles, allUpdatedFiles, imageBlocks, gameContract, architectureSpec } = state;
      const tranche = progress.tranches[nextTranche];

      if (!tranche) throw new Error(`Tranche ${nextTranche} not found in pipeline state.`);

      // ── Mark tranche as in-progress ──────────────────────────
      progress.currentTranche = nextTranche;
      progress.tranches[nextTranche].status = "in_progress";
      progress.tranches[nextTranche].startTime = Date.now();
      await saveProgress(bucket, projectPath, progress);

      console.log(`TRANCHE ${nextTranche + 1}/${progress.totalTranches}: ${tranche.name} (Job ${jobId})`);

      // IMPORTANT: Executors use DELIMITER FORMAT, NOT JSON.
      // Embedding raw JS/HTML code inside JSON string fields causes frequent
      // parse failures because LLMs miss-escape quotes, backslashes, and
      // newlines. Delimiters require zero escaping and are completely robust.
      const executionSystem = `You are an expert game development AI.
The user will provide project files and a focused modification request (one tranche of a larger build).

${HARD_ENVIRONMENT_CONSTRAINTS}

CHERRY3D SDK ERRATA — VERIFIED CORRECTIONS (apply these over any conflicting SDK guidance):

ERRATUM 1: controls.getFloat() / getInt() are UNRELIABLE for reading data back from the physics thread.
They may always return 0. For position readback, use rb.RigidBody.getMotionState().position.
For derived state like tileCentered or wallBlocked, compute them on the main thread from position + walkable tile data.

ERRATUM 2: The correct property path is rb.RigidBody.controls, NOT rb.controls.
Using rb.controls.setFloat() is a SILENT NO-OP — no error thrown, but nothing happens.
Always use: gameState.playerBody.RigidBody.controls.setFloat(slot, value)
Always use: gameState.playerBody.RigidBody.addInputHandler(updateInput)
Always use: gameState.playerBody.RigidBody.getMotionState()

ERRATUM 3: DYNAMIC bodies do NOT always auto-sync visual mesh position to physics position.
After spawning any DYNAMIC actor, add explicit visual sync in the render loop:
  const ms = playerBody.RigidBody.getMotionState();
  playerObj.position = [ms.position[0], 0.5, ms.position[2]];

ERRATUM 4: The HTML overlay object ID is NOT guaranteed to be '25'.
Always use fallback discovery: try getObject('25'), then scan IDs 20-30 probing for a known DOM element
(e.g., querySelector('#startButton')) to find the actual overlay root.

ERRATUM 5: File 2 (models/2) and file 23 (models/23) run in SEPARATE window contexts.
window.* globals set in file 23 are NOT accessible from file 2. All UI communication must use
DOM element references from Module.ProjectManager.getObject(id).DOMElement.
Wire button click listeners directly on queried DOM elements in file 2.
Add boolean flag polling (_startRequested / _restartRequested) as a cross-context fallback.
After modal dismissal, set pointer-events:none on the overlay root.
Never call blur()/focus() — it breaks engine keyboard input capture.
Always add document.addEventListener('keydown') fallback for movement input.

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



      // Build file context from accumulated state
      let trancheFileContext = "Here are the current project files (includes all output from prior tranches — you MUST preserve all existing code):\n\n";
      for (const [path, fileContent] of Object.entries(accumulatedFiles)) {
        trancheFileContext += `--- FILE: ${path} ---\n${fileContent}\n\n`;
      }

      assertTranchePromptHasRequiredManifestBlock(tranche, nextTranche);

      const trancheUserContent = [
        {
          type: "text",
          text: `${trancheFileContext}\n\n=== TRANCHE ${nextTranche + 1} of ${progress.totalTranches}: "${tranche.name}" ===\n\n${buildArchitectureSpecBlock(architectureSpec)}${tranche.prompt}\n\n=== END TRANCHE INSTRUCTIONS ===\n\nIMPORTANT: You are working on tranche ${nextTranche + 1} of ${progress.totalTranches}. The project files above contain ALL work from prior tranches. You MUST preserve all existing code and ADD your changes on top. Output the COMPLETE updated file contents.`
        },
        ...(imageBlocks || [])
      ];

      let trancheResponseObj;
      try {
        trancheResponseObj = await callClaude(apiKey, {
          model: "claude-sonnet-4-6",
          maxTokens: 128000,
          budgetTokens: 30000,
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
          progress.tranches[nextTranche].status = "error";
          progress.tranches[nextTranche].endTime = Date.now();
          progress.tranches[nextTranche].message = "Executor returned no recognisable file delimiters or valid JSON fallback.";
          await saveProgress(bucket, projectPath, progress);
          console.error(`Tranche ${nextTranche + 1} produced no parseable output.`);
          console.error("Raw response (first 500 chars):", trancheResponseObj.text.slice(0, 500));

          state.progress = progress;
          await savePipelineState(bucket, projectPath, state);

          // Checkpoint ai_response.json with whatever was accumulated so far
          if (allUpdatedFiles.length > 0) {
            await saveAiResponse(bucket, projectPath, allUpdatedFiles, {
              jobId:         jobId,
              trancheIndex:  nextTranche,
              totalTranches: progress.totalTranches,
              status:        "checkpoint",
              message:       `Checkpoint after tranche ${nextTranche + 1} parse-error skip. ${allUpdatedFiles.length} file(s) so far.`
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
      progress.finalMessage = `Build complete: ${allUpdatedFiles.length} file(s) updated across ${progress.tranches.filter(tr => tr.status === "complete").length} tranche(s). Tokens: ${t.input_tokens} in / ${t.output_tokens} out.`;
      progress.completedTime = Date.now();
      await saveProgress(bucket, projectPath, progress);

      console.log(`Total tokens — input: ${t.input_tokens}, output: ${t.output_tokens}`);

      // Clean up pipeline state and request files
      try { await bucket.file(`${projectPath}/ai_pipeline_state.json`).delete(); } catch (e) {}
      try { await bucket.file(`${projectPath}/ai_request.json`).delete(); } catch (e) {}

      return { statusCode: 200, body: JSON.stringify({ success: true, phase: "complete" }) };
    }

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