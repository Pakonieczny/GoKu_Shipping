/* netlify/functions/claudeCodeProxy-background.js */
/* ═══════════════════════════════════════════════════════════════════
   TRANCHED AI PIPELINE — v4.1 (Anti-Pattern Correction Loop)
   ─────────────────────────────────────────────────────────────────
   Each invocation handles ONE unit of work then chains to itself
   for the next, staying well under Netlify's 15-min limit.

   Invocation 0    ▸  "plan"    — Opus 4.6 plans tranches directly
                       from Master Prompt + Engine Reference + files.
   Invocation 1–N  ▸  "tranche" — Sonnet 4.6 executes one tranche,
                       saves accumulated files, chains to next tranche
   Correction loop ▸  "fix"     — On FATAL anti-pattern violation,
                       chains back to the SAME tranche index with the
                       violation report injected. Up to
                       MAX_ANTIPATTERN_RETRIES attempts before skip.
   Final           ▸  Writes ai_response.json for frontend pickup.

   Anti-pattern validators run after each tranche. FATAL violations
   trigger an automatic correction pass instead of silent skip.
   Progress object carries antiPatternRetryCount + antiPatternReport
   so the frontend UI can show exactly what was detected and fixed.

   All intermediate state lives in Firebase so each invocation is
   stateless and can reconstruct context from the pipeline file.
   ═══════════════════════════════════════════════════════════════════ */

const fetch = require("node-fetch");
const admin = require("./firebaseAdmin");

const MAX_ANTIPATTERN_RETRIES = 2;   // correction attempts per tranche before skip


/* ─── ENGINE REFERENCE: fetched from Firebase at runtime ────────
   The Engine Reference is stored as one or more files under the
   project's  ai_system_instructions/  folder in Firebase Storage.
   fetchSystemInstructions() loads them all, concatenates them in
   lexicographic filename order, and returns the combined text.
   This is injected verbatim into every planning and executor
   system prompt. To update the Engine Reference, upload a new
   version of the file to ai_system_instructions/ — no code change
   needed. ── */

async function fetchSystemInstructions(bucket, projectPath) {
  try {
    const folder = `${projectPath}/ai_system_instructions`;
    const [files] = await bucket.getFiles({ prefix: folder + "/" });
    if (!files || files.length === 0) {
      console.warn(`fetchSystemInstructions: no files found at ${folder}/`);
      return "";
    }
    // Sort lexicographically so multi-file ordering is deterministic
    files.sort((a, b) => a.name.localeCompare(b.name));
    const parts = await Promise.all(
      files.map(async (file) => {
        const [fileContent] = await file.download();
        return fileContent.toString("utf8");
      })
    );
    const combined = parts.join("\n\n");
    console.log(`fetchSystemInstructions: loaded ${files.length} file(s), ${combined.length} chars.`);
    return combined;
  } catch (err) {
    console.error("fetchSystemInstructions failed:", err.message);
    return "";
  }
}

/* ─── ANTI-PATTERN VALIDATORS ──────────────────────────────────
   Engine-agnostic. Tests apply to any Cherry3D game — no game-
   specific function names, UI object names, or floor conventions.
   FATAL patterns trigger automatic re-execution of the tranche
   with an explicit correction prompt. ──────────────────────────── */

const ANTI_PATTERNS = [
  {
    // §3 — rb.RigidBody.controls is the correct path.
    // rb.controls.setFloat/setInt is a silent no-op on every Cherry3D game.
    name: "Silent no-op controls path",
    test: (code) => {
      return code.split('\n').some(line => {
        const t = line.trim();
        if (t.startsWith('//') || t.startsWith('*')) return false;
        // Flag .controls.set* NOT preceded by .RigidBody
        return /(?<!RigidBody\.controls)\.controls\.set(Float|Int)\s*\(/.test(line) &&
               !/RigidBody\.controls\.set(Float|Int)/.test(line);
      });
    },
    message: "FATAL: Found .controls.setFloat/setInt without .RigidBody. prefix — silent no-op. Use body.RigidBody.controls.setFloat/setInt.",
    severity: "FATAL"
  },
  {
    // §4 — controls.getFloat/getInt always return 0. Use getMotionState().
    name: "Unreliable physics readback",
    test: (code) => {
      return code.split('\n').some(line => {
        const t = line.trim();
        if (t.startsWith('//') || t.startsWith('*')) return false;
        return /\.controls\.get(Float|Int)\s*\(/.test(line);
      });
    },
    message: "FATAL: Found controls.getFloat()/getInt() for readback — always returns 0. Use body.RigidBody.getMotionState() instead.",
    severity: "FATAL"
  },
  {
    // §8 — file 2 and file 23 have isolated window contexts.
    // Any window.XYZ.method() call from file 2 silently fails for any game.
    name: "Cross-context window global call",
    test: (code) => {
      return code.split('\n').some(line => {
        const t = line.trim();
        if (t.startsWith('//') || t.startsWith('*')) return false;
        // Match window.<Anything>.<method>() — property reads (no parens) are OK
        return /window\.[A-Za-z_$][\w$]*\.[A-Za-z_$][\w$]*\s*\(/.test(line);
      });
    },
    message: "FATAL: Found window.<object>.<method>() call from file 2. Files 2 and 23 have isolated window contexts — use Module.ProjectManager.getObject(id).DOMElement for all cross-file DOM access.",
    severity: "FATAL"
  },
  {
    // §5 — rbPosition is a LOCAL offset from the parent mesh.
    // Any non-zero value doubles the intended world position.
    name: "Non-zero rbPosition",
    test: (code) => {
      const rbCalls = code.match(/createRigidBody\([^)]+\)/g) || [];
      return rbCalls.some(call => {
        const arrays = call.match(/\[([^\]]*)\]/g);
        if (!arrays || arrays.length < 2) return false;
        const lastArray = arrays[arrays.length - 1];
        return lastArray && !/\[\s*0\s*,\s*0\s*,\s*0\s*\]/.test(lastArray);
      });
    },
    message: "FATAL: Found createRigidBody with non-[0,0,0] rbPosition — causes position doubling. rbPosition is a LOCAL offset from the parent; always use [0,0,0].",
    severity: "FATAL"
  },
  {
    // §6 — any blocking surface without a STATIC rigidbody is invisible to physics.
    // Detects createObject/createInstance blocks that set up a surface mesh but
    // have no accompanying STATIC rigidbody anywhere nearby (within 1500 chars).
    name: "Surface mesh without STATIC rigidbody",
    test: (code) => {
      // Find every createRigidBody call and collect their motion types
      const rbTypes = [];
      const rbRegex = /createRigidBody\s*\([^)]*['"]?(STATIC|KINEMATIC|DYNAMIC)['"]?[^)]*\)/gi;
      let m;
      while ((m = rbRegex.exec(code)) !== null) {
        rbTypes.push({ idx: m.index, type: m[1].toUpperCase() });
      }
      const hasStatic = rbTypes.some(r => r.type === 'STATIC');
      // If the file creates rigidbodies but none are STATIC, flag it.
      // Only trigger when the file has enough rigidbodies to indicate a full scene build
      // (avoids false positives on partial tranche files).
      return rbTypes.length >= 3 && !hasStatic;
    },
    message: "FATAL: File creates rigidbodies but none are STATIC. Every floor, wall, or blocking surface needs a STATIC rigidbody or DYNAMIC actors will fall through with no error.",
    severity: "FATAL"
  },
  {
    // §14, §17 — Camera must be set every frame in onRender BEFORE any isReady/isDead
    // guards. Module.controls.position and .target are the ONLY reliable paths.
    // Module.camera and scene.camera are forbidden — they are unreliable.
    name: "Camera not set correctly every frame in onRender",
    test: (code) => {
      // Check for forbidden camera API paths (Module.camera / scene.camera)
      const hasBadCamPath = code.split('\n').some(line => {
        const t = line.trim();
        if (t.startsWith('//') || t.startsWith('*')) return false;
        return /Module\.camera\b/.test(line) || /\bscene\.camera\b/.test(line);
      });
      if (hasBadCamPath) return true;

      // Only inspect files that define onRender (skip partial tranche files)
      if (!/\bonRender\b/.test(code) || !/\bonInit\b/.test(code)) return false;

      // Extract onRender body using balanced-brace walk (robust against nested blocks)
      const fnIdx = code.search(/\bfunction\s+onRender\s*\(|\bonRender\s*[:=]\s*function\s*\(|\bonRender\s*[:=]\s*\(/);
      if (fnIdx === -1) return false;
      const openIdx = code.indexOf('{', fnIdx);
      if (openIdx === -1) return false;
      let depth = 1, pos = openIdx + 1;
      while (pos < code.length && depth > 0) {
        if (code[pos] === '{') depth++;
        else if (code[pos] === '}') depth--;
        pos++;
      }
      const body = code.slice(openIdx + 1, pos - 1);

      // Flag if Module.controls.position/.target is never set inside onRender
      const camIdx = body.search(/Module\.controls\.(position|target)/);
      if (camIdx === -1) return true;

      // Flag if camera is set AFTER an isReady/isDead/gameOver guard
      const guardIdx = body.search(/if\s*\([\s\S]{0,60}(?:isReady|isDead|gameOver|gameState\.(?:isReady|isDead|gameOver|paused))/);
      return guardIdx !== -1 && guardIdx < camIdx;
    },
    message: "FATAL: Camera (Module.controls.position / .target) must be set every frame in onRender BEFORE any isReady/isDead guards, and NEVER via Module.camera or scene.camera — those are unreliable paths.",
    severity: "FATAL"
  },
  {
    // §14 — onRender must return true. A falsy return breaks the engine render loop.
    name: "onRender missing return true",
    test: (code) => {
      // Only test files that define onRender
      if (!/\bonRender\b/.test(code)) return false;
      // Locate onRender definition
      const fnIdx = code.search(/\bfunction\s+onRender\s*\(|\bonRender\s*[:=]\s*function\s*\(|\bonRender\s*[:=]\s*\(/);
      if (fnIdx === -1) return false;
      const openIdx = code.indexOf('{', fnIdx);
      if (openIdx === -1) return false;
      // Walk balanced braces to extract full function body
      let depth = 1, pos = openIdx + 1;
      while (pos < code.length && depth > 0) {
        if (code[pos] === '{') depth++;
        else if (code[pos] === '}') depth--;
        pos++;
      }
      const body = code.slice(openIdx, pos);
      return !/return\s+true/.test(body);
    },
    message: "FATAL: onRender must always return true. A missing or falsy return breaks the engine render loop.",
    severity: "FATAL"
  },
  {
    // §13 — KINEMATIC bodies require BOTH a visual AND a collider update every frame.
    // obj.position updates the visual mesh only.
    // RigidBody.set([{prop:"setPosition",...}]) updates the collider only.
    // Omitting either half causes silent visual/collider desync.
    name: "KINEMATIC dual-update incomplete",
    test: (code) => {
      if (!/KINEMATIC/.test(code)) return false;
      const hasSetPosition = /["']setPosition["']/.test(code);
      const hasObjPosition = /\.position\s*=\s*\[/.test(code);
      // Flag when one side of the dual-update is present but the other is absent
      return hasSetPosition !== hasObjPosition;
    },
    message: "FATAL: KINEMATIC body requires BOTH obj.position=[x,y,z] (visual mesh) AND RigidBody.set([{prop:'setPosition',value:[x,y,z]}]) (collider) every frame. Omitting either half causes silent visual/collider desync.",
    severity: "FATAL"
  },
  {
    // §11 — setLinearVelocity is a SILENT NO-OP on KINEMATIC bodies.
    // KINEMATIC actors must be moved via setPosition (collider) + obj.position (visual).
    // Use DYNAMIC motionType if velocity-driven movement is required.
    name: "KINEMATIC setLinearVelocity silent no-op",
    test: (code) => {
      if (!/KINEMATIC/.test(code)) return false;
      return code.split('\n').some(line => {
        const t = line.trim();
        if (t.startsWith('//') || t.startsWith('*')) return false;
        return /setLinearVelocity/.test(line);
      });
    },
    message: "FATAL: setLinearVelocity is a SILENT NO-OP on KINEMATIC bodies — no error is thrown but the actor will not move. Use setPosition (collider) + obj.position (visual) for KINEMATIC, or switch to DYNAMIC if velocity control is needed.",
    severity: "FATAL"
  }
];

function runAntiPatternValidation(files) {
  const violations = [];
  for (const file of files) {
    if (!file.path || !file.content) continue;
    // Only validate JS/HTML files
    if (!file.path.includes('models/')) continue;
    
    for (const pattern of ANTI_PATTERNS) {
      // Reset regex lastIndex for global patterns
      let triggered = false;
      try {
        triggered = pattern.test(file.content);
      } catch (e) {
        console.warn(`Anti-pattern check "${pattern.name}" threw:`, e.message);
      }
      if (triggered) {
        violations.push({
          file: file.path,
          pattern: pattern.name,
          message: pattern.message,
          severity: pattern.severity
        });
      }
    }
  }
  return violations;
}

/* ── DYNAMIC_ARCHITECTURE_JSON_SCHEMA — REMOVED ─────────────
   Architect pass has been merged into single-pass planner.
   No intermediate architecture spec is generated. ────────── */

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

/* ── buildArchitectureSpecBlock — REMOVED ─────────────────
   No longer needed. Single-pass planner embeds game-specific
   rules directly in each tranche prompt. ─────────────────── */

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

    // ── Determine mode: "plan" / "tranche" / "fix" ──────────────
    // "fix" re-runs the same tranche index with a correction prompt
    const mode = parsedBody.mode || "plan";
    const nextTranche = parsedBody.nextTranche || 0;
    const fixAttempt  = parsedBody.fixAttempt  || 0;  // 1-based, 0 means not a fix pass

    // ══════════════════════════════════════════════════════════════
    //  MODE: "plan" — First invocation, do planning then chain
    // ══════════════════════════════════════════════════════════════
    if (mode === "plan") {

      // ── 1. Download the request payload from Firebase ─────────
      const requestFile = bucket.file(`${projectPath}/ai_request.json`);
      const [content] = await requestFile.download();
      const { prompt, files, selectedAssets, inlineImages } = JSON.parse(content.toString());
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
      //  SINGLE-PASS PLANNING (Opus 4.6)
      //  Reads Master Prompt + Engine Reference + files directly.
      //  No intermediate architecture spec. No re-synthesis.
      //  Outputs tranche plan with rules embedded in each prompt.
      // ══════════════════════════════════════════════════════════

      // ── Fetch Engine Reference from ai_system_instructions/ ──
      const engineReference = await fetchSystemInstructions(bucket, projectPath);
      if (!engineReference) {
        console.warn("PLAN: Engine Reference not found in ai_system_instructions/ — proceeding without it.");
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
        completedTime: null
      };
      await saveProgress(bucket, projectPath, progress);

      const planningSystem = `You are an expert game development planner for the Cherry3D engine.

Your job: read the user's request, the existing project files, and the Engine Reference below. Then split the build into sequential, self-contained TRANCHES that can be executed one at a time by a coding AI.

${engineReference}

PLANNING RULES:
1. Each tranche should focus on 1-2 closely related concerns.
2. Tranches MUST be ordered by dependency — later tranches build on earlier ones.
3. Each tranche prompt must be FULLY SELF-CONTAINED — it must embed the exact game-specific rules, variable names, slot layouts, code snippets, and pitfall warnings from the user's request that are relevant to that tranche. Do NOT summarize or abstract — copy the exact technical details.
4. If the request is simple enough, use 1 tranche. Otherwise use the minimum count that preserves correctness.
5. Each tranche must declare expectedFiles, dependencies, expertAgents, phase, and qualityCriteria.
6. The FIRST tranche should establish foundations: factories, materials, maze parsing, floor+walls with STATIC rigidbodies.
7. The LAST tranche should handle integration, edge cases, and polish.
8. Engine invariants from the ENGINE REFERENCE above are already in the executor's system prompt. Do NOT restate them in tranche prompts. Only embed game-specific rules.
9. When the user's request contains code examples (updateInput, syncPlayerSharedMemory, ghost AI, etc.), embed those exact code examples in the relevant tranche prompts — do not paraphrase them.

${REQUIRED_TRANCHE_VALIDATION_BLOCK}

You must respond ONLY with a valid JSON object. No markdown, no code fences, no preamble.

{
  "analysis": "Brief planning analysis describing how you decomposed the build and why.",
  "tranches": [
    {
      "name": "Short Name",
      "description": "2-3 sentence description of what this tranche accomplishes.",
      "expertAgents": ["agent_id_1", "agent_id_2"],
      "phase": 1,
      "dependencies": [],
      "qualityCriteria": ["Criterion 1", "Criterion 2"],
      "prompt": "THE COMPLETE, SELF-CONTAINED PROMPT for the coding AI. Embed exact game-specific rules, code examples, and pitfall warnings from the user's request. Do NOT embed engine-level rules — those are in the executor's system prompt.",
      "expectedFiles": ["models/2", "models/23"]
    }
  ]
}`;

      const planningUserContent = [
        { type: "text", text: `${fileContext}

=== FULL USER REQUEST ===
${prompt}
=== END USER REQUEST ===` },
        ...imageBlocks
      ];

      console.log(`PLANNING: Single-pass Opus 4.6 for Job ${jobId}...`);
      const planResult = await callClaude(apiKey, {
        model: "claude-opus-4-6",
        maxTokens: 128000,
        budgetTokens: 30000,
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

      const { progress, accumulatedFiles, allUpdatedFiles, imageBlocks } = state;
      const tranche = progress.tranches[nextTranche];

      // ── Fetch Engine Reference from ai_system_instructions/ ──
      const engineReference = await fetchSystemInstructions(bucket, projectPath);
      if (!engineReference) {
        console.warn("TRANCHE: Engine Reference not found in ai_system_instructions/ — proceeding without it.");
      }

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

${engineReference}

The ENGINE REFERENCE above is the single authoritative source for all platform invariants.
Do not re-state them — just apply them. Your output will be automatically scanned by
anti-pattern validators that enforce the rules in the Engine Reference. Write it correctly
the first time.

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
          text: `${trancheFileContext}\n\n=== TRANCHE ${nextTranche + 1} of ${progress.totalTranches}: "${tranche.name}" ===\n\n${tranche.prompt}\n\n=== END TRANCHE INSTRUCTIONS ===\n\nIMPORTANT: You are working on tranche ${nextTranche + 1} of ${progress.totalTranches}. The project files above contain ALL work from prior tranches. You MUST preserve all existing code and ADD your changes on top. Output the COMPLETE updated file contents.`
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
          // ── Anti-pattern validation ──────────────────────────────
          if (trancheResult.updatedFiles && Array.isArray(trancheResult.updatedFiles)) {
            const violations    = runAntiPatternValidation(trancheResult.updatedFiles);
            const fatalViolations = violations.filter(v => v.severity === "FATAL");

            if (fatalViolations.length > 0) {
              // ── Build human-readable violation report ─────────────
              const violationLines = fatalViolations.map((v, i) =>
                `VIOLATION ${i + 1} — ${v.pattern}\n  File   : ${v.file}\n  Detail : ${v.message}`
              ).join('\n\n');

              const currentRetry = (progress.tranches[nextTranche].antiPatternRetryCount || 0);
              const violationSummary = fatalViolations.map(v => `[${v.file}] ${v.message}`).join('\n');

              console.error(`Tranche ${nextTranche + 1} FAILED anti-pattern validation (attempt ${currentRetry + 1}/${MAX_ANTIPATTERN_RETRIES}):\n${violationSummary}`);

              if (currentRetry < MAX_ANTIPATTERN_RETRIES) {
                // ── Schedule a correction pass ──────────────────────
                const nextAttempt = currentRetry + 1;

                progress.tranches[nextTranche].status              = "fixing";
                progress.tranches[nextTranche].antiPatternRetryCount = nextAttempt;
                progress.tranches[nextTranche].antiPatternReport   = violationLines;
                progress.tranches[nextTranche].antiPatternViolations = fatalViolations.map(v => ({
                  file: v.file, pattern: v.pattern, message: v.message
                }));
                progress.tranches[nextTranche].fixAttempt          = nextAttempt;
                // Keep startTime so the timer keeps running
                progress.tranches[nextTranche].endTime             = null;
                progress.tranches[nextTranche].message             = `⚠ ${fatalViolations.length} FATAL violation(s) detected — correction pass ${nextAttempt}/${MAX_ANTIPATTERN_RETRIES} queued.`;
                await saveProgress(bucket, projectPath, progress);

                // Persist state with the REJECTED files so fix mode can reference them
                state.progress                                     = progress;
                state.rejectedTranche                              = {
                  index:      nextTranche,
                  files:      trancheResult.updatedFiles,
                  violations: fatalViolations,
                  report:     violationLines
                };
                await savePipelineState(bucket, projectPath, state);

                if (allUpdatedFiles.length > 0) {
                  await saveAiResponse(bucket, projectPath, allUpdatedFiles, {
                    jobId, trancheIndex: nextTranche, totalTranches: progress.totalTranches,
                    status:  "checkpoint",
                    message: `Anti-pattern violations detected in tranche ${nextTranche + 1}. Correction pass ${nextAttempt}/${MAX_ANTIPATTERN_RETRIES} starting.`
                  });
                }

                // Chain to fix mode for the SAME tranche
                await chainToSelf({ projectPath, jobId, mode: "fix", nextTranche, fixAttempt: nextAttempt });
                return { statusCode: 200, body: JSON.stringify({ success: true, chained: true, phase: `tranche_${nextTranche}_fix_queued_attempt_${nextAttempt}` }) };

              } else {
                // ── Retries exhausted — skip and continue ───────────
                console.error(`Tranche ${nextTranche + 1} exhausted ${MAX_ANTIPATTERN_RETRIES} correction attempt(s). Skipping.`);
                progress.tranches[nextTranche].status    = "error";
                progress.tranches[nextTranche].endTime   = Date.now();
                progress.tranches[nextTranche].message   = `Anti-pattern correction failed after ${MAX_ANTIPATTERN_RETRIES} attempt(s).\n${violationSummary}`;
                await saveProgress(bucket, projectPath, progress);

                state.progress = progress;
                await savePipelineState(bucket, projectPath, state);

                if (allUpdatedFiles.length > 0) {
                  await saveAiResponse(bucket, projectPath, allUpdatedFiles, {
                    jobId, trancheIndex: nextTranche, totalTranches: progress.totalTranches,
                    status:  "checkpoint",
                    message: `Tranche ${nextTranche + 1} skipped after ${MAX_ANTIPATTERN_RETRIES} failed correction attempts.`
                  });
                }

                if (nextTranche + 1 < progress.totalTranches) {
                  await chainToSelf({ projectPath, jobId, mode: "tranche", nextTranche: nextTranche + 1 });
                  return { statusCode: 200, body: JSON.stringify({ success: true, chained: true, phase: `tranche_${nextTranche}_antipattern_exhausted` }) };
                }
                trancheResult.updatedFiles = [];   // don't merge
              }

            } else if (violations.length > 0) {
              // Warnings — log but don't reject
              const warnSummary = violations.map(v => `[${v.file}] ${v.message}`).join('\n');
              console.warn(`Tranche ${nextTranche + 1} anti-pattern WARNINGS:\n${warnSummary}`);
            }
          }

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


    // ══════════════════════════════════════════════════════════════
    //  MODE: "fix" — Re-run ONE tranche with violation report injected
    //  Triggered automatically when anti-pattern validation fails.
    //  Chains back to "tranche" mode on success, or skips on exhaustion.
    // ══════════════════════════════════════════════════════════════
    if (mode === "fix") {

      // ── Kill switch check ────────────────────────────────────
      const killCheck = await checkKillSwitch(bucket, projectPath, jobId);
      if (killCheck.killed) {
        return { statusCode: 200, body: JSON.stringify({ success: true, superseded: killCheck.reason === "superseded" }) };
      }

      // ── Load pipeline state ──────────────────────────────────
      const state = await loadPipelineState(bucket, projectPath);
      if (!state) throw new Error("Pipeline state not found in Firebase. Fix mode chain broken.");

      const { progress, accumulatedFiles, allUpdatedFiles, imageBlocks, rejectedTranche } = state;
      const tranche = progress.tranches[nextTranche];

      if (!tranche) throw new Error(`Tranche ${nextTranche} not found in pipeline state (fix mode).`);
      if (!rejectedTranche || rejectedTranche.index !== nextTranche) {
        throw new Error(`No rejected tranche data for tranche ${nextTranche}. Fix mode cannot proceed.`);
      }

      // ── Fetch Engine Reference ───────────────────────────────
      const engineReference = await fetchSystemInstructions(bucket, projectPath);

      console.log(`FIX MODE: Tranche ${nextTranche + 1} — attempt ${fixAttempt}/${MAX_ANTIPATTERN_RETRIES} (Job ${jobId})`);

      // ── Update UI: show "fixing" status with violation details ─
      progress.tranches[nextTranche].status  = "fixing";
      progress.tranches[nextTranche].message = `🔧 Correction pass ${fixAttempt}/${MAX_ANTIPATTERN_RETRIES} in progress — rewriting to fix ${rejectedTranche.violations.length} violation(s)...`;
      await saveProgress(bucket, projectPath, progress);

      // ── Build the violation-aware correction system prompt ────
      const correctionSystem = `You are an expert Cherry3D game developer performing a TARGETED CORRECTION.
A previous generation pass produced code with FATAL engine violations that will cause silent runtime failures.
You must rewrite ONLY the offending logic to fix every listed violation. Preserve all other code exactly.

${engineReference}

The ENGINE REFERENCE above is the authoritative source. The violations below are EXACT matches
against that reference. Fix them precisely — do not introduce new code unrelated to the violations.

RESPONSE FORMAT: Use delimiter format only — no JSON, no markdown code blocks.
===FILE_START: path===
...complete corrected file...
===FILE_END: path===
===MESSAGE===
Summary of exactly what was fixed and why each violation occurred.
===END_MESSAGE===`;

      // ── Build the user content: rejected files + violation report ─
      const violationReport = rejectedTranche.report;
      const violatingFiles  = rejectedTranche.files;

      let correctionUserText = `=== VIOLATION REPORT ===\nThe following FATAL anti-pattern violations were detected in your previous output:\n\n${violationReport}\n\n=== END VIOLATION REPORT ===\n\n`;
      correctionUserText += `=== REJECTED FILES (your previous output — fix these) ===\n`;
      for (const f of violatingFiles) {
        correctionUserText += `\n--- FILE: ${f.path} ---\n${f.content}\n`;
      }
      correctionUserText += `\n=== END REJECTED FILES ===\n\n`;
      correctionUserText += `=== CURRENT ACCUMULATED FILES (prior tranches — do NOT touch these) ===\n`;
      for (const [path, fileContent] of Object.entries(accumulatedFiles)) {
        // Skip the paths that are being corrected to avoid confusion
        if (violatingFiles.some(f => f.path === path)) continue;
        correctionUserText += `\n--- FILE: ${path} ---\n${fileContent}\n`;
      }
      correctionUserText += `\n=== END ACCUMULATED FILES ===\n\n`;
      correctionUserText += `=== ORIGINAL TRANCHE INSTRUCTIONS ===\n${tranche.prompt}\n=== END TRANCHE INSTRUCTIONS ===\n\n`;
      correctionUserText += `Fix every violation listed in the VIOLATION REPORT above. Output the complete corrected file(s).`;

      // ── Call Claude for the correction ──────────────────────
      let fixResponseObj;
      try {
        fixResponseObj = await callClaude(apiKey, {
          model:        "claude-sonnet-4-6",
          maxTokens:    128000,
          budgetTokens: 20000,
          effort:       "high",
          system:       correctionSystem,
          userContent:  [{ type: "text", text: correctionUserText }, ...(imageBlocks || [])]
        });
      } catch (err) {
        console.error(`Fix pass ${fixAttempt} failed with API error:`, err.message);
        progress.tranches[nextTranche].status  = "error";
        progress.tranches[nextTranche].endTime = Date.now();
        progress.tranches[nextTranche].message = `Correction pass ${fixAttempt} API error: ${err.message}`;
        await saveProgress(bucket, projectPath, progress);
        state.progress = progress;
        await savePipelineState(bucket, projectPath, state);
        if (nextTranche + 1 < progress.totalTranches) {
          await chainToSelf({ projectPath, jobId, mode: "tranche", nextTranche: nextTranche + 1 });
        }
        return { statusCode: 200, body: JSON.stringify({ success: false, phase: `fix_${nextTranche}_api_error` }) };
      }

      // Track token usage for this fix pass
      if (fixResponseObj.usage) {
        progress.tokenUsage.totals.input_tokens  += fixResponseObj.usage.input_tokens  || 0;
        progress.tokenUsage.totals.output_tokens += fixResponseObj.usage.output_tokens || 0;
        if (!progress.tranches[nextTranche].fixTokenUsage) progress.tranches[nextTranche].fixTokenUsage = [];
        progress.tranches[nextTranche].fixTokenUsage.push(fixResponseObj.usage);
      }

      // ── Parse fix response ───────────────────────────────────
      const fixResult = parseDelimitedResponse(fixResponseObj.text);
      if (!fixResult || !fixResult.updatedFiles || fixResult.updatedFiles.length === 0) {
        console.error(`Fix pass ${fixAttempt}: no parseable output from correction pass.`);
        progress.tranches[nextTranche].status  = "error";
        progress.tranches[nextTranche].endTime = Date.now();
        progress.tranches[nextTranche].message = `Correction pass ${fixAttempt} produced no output.`;
        await saveProgress(bucket, projectPath, progress);
        state.progress = progress;
        await savePipelineState(bucket, projectPath, state);
        if (nextTranche + 1 < progress.totalTranches) {
          await chainToSelf({ projectPath, jobId, mode: "tranche", nextTranche: nextTranche + 1 });
        }
        return { statusCode: 200, body: JSON.stringify({ success: false, phase: `fix_${nextTranche}_no_output` }) };
      }

      // ── Re-validate the corrected files ──────────────────────
      const revalidation   = runAntiPatternValidation(fixResult.updatedFiles);
      const remainingFatal = revalidation.filter(v => v.severity === "FATAL");

      if (remainingFatal.length > 0) {
        // Still failing after this correction pass
        const remainingSummary = remainingFatal.map(v => `[${v.file}] ${v.message}`).join('\n');
        console.warn(`Fix pass ${fixAttempt} — ${remainingFatal.length} violation(s) remain.`);

        if (fixAttempt < MAX_ANTIPATTERN_RETRIES) {
          // Queue another fix pass
          const nextAttempt = fixAttempt + 1;
          const remainingReport = remainingFatal.map((v, i) =>
            `VIOLATION ${i + 1} — ${v.pattern}\n  File   : ${v.file}\n  Detail : ${v.message}`
          ).join('\n\n');

          progress.tranches[nextTranche].antiPatternRetryCount  = nextAttempt;
          progress.tranches[nextTranche].antiPatternReport      = remainingReport;
          progress.tranches[nextTranche].antiPatternViolations  = remainingFatal.map(v => ({ file: v.file, pattern: v.pattern, message: v.message }));
          progress.tranches[nextTranche].fixAttempt             = nextAttempt;
          progress.tranches[nextTranche].status                 = "fixing";
          progress.tranches[nextTranche].message                = `⚠ ${remainingFatal.length} violation(s) remain after pass ${fixAttempt} — correction pass ${nextAttempt}/${MAX_ANTIPATTERN_RETRIES} queued.`;
          await saveProgress(bucket, projectPath, progress);

          state.progress       = progress;
          state.rejectedTranche = {
            index: nextTranche, files: fixResult.updatedFiles,
            violations: remainingFatal, report: remainingReport
          };
          await savePipelineState(bucket, projectPath, state);

          await chainToSelf({ projectPath, jobId, mode: "fix", nextTranche, fixAttempt: nextAttempt });
          return { statusCode: 200, body: JSON.stringify({ success: true, chained: true, phase: `fix_${nextTranche}_retry_${nextAttempt}` }) };

        } else {
          // Exhausted all correction attempts
          progress.tranches[nextTranche].status  = "error";
          progress.tranches[nextTranche].endTime = Date.now();
          progress.tranches[nextTranche].message = `Correction failed after ${MAX_ANTIPATTERN_RETRIES} attempt(s). Violations persist: ${remainingSummary}`;
          await saveProgress(bucket, projectPath, progress);
          state.progress = progress;
          state.rejectedTranche = null;
          await savePipelineState(bucket, projectPath, state);

          if (allUpdatedFiles.length > 0) {
            await saveAiResponse(bucket, projectPath, allUpdatedFiles, {
              jobId, trancheIndex: nextTranche, totalTranches: progress.totalTranches,
              status: "checkpoint",
              message: `Tranche ${nextTranche + 1} skipped — corrections exhausted after ${MAX_ANTIPATTERN_RETRIES} attempt(s).`
            });
          }
          if (nextTranche + 1 < progress.totalTranches) {
            await chainToSelf({ projectPath, jobId, mode: "tranche", nextTranche: nextTranche + 1 });
          }
          return { statusCode: 200, body: JSON.stringify({ success: false, phase: `fix_${nextTranche}_exhausted` }) };
        }
      }

      // ── All violations resolved — merge corrected files ───────
      console.log(`Fix pass ${fixAttempt} SUCCEEDED for tranche ${nextTranche + 1}. All violations resolved.`);

      const fixFilesUpdated = [];
      for (const file of fixResult.updatedFiles) {
        accumulatedFiles[file.path] = file.content;
        fixFilesUpdated.push(file.path);
        const existingIdx = allUpdatedFiles.findIndex(f => f.path === file.path);
        if (existingIdx >= 0) { allUpdatedFiles[existingIdx] = file; }
        else                  { allUpdatedFiles.push(file); }
      }

      progress.tranches[nextTranche].status       = "complete";
      progress.tranches[nextTranche].endTime      = Date.now();
      progress.tranches[nextTranche].filesUpdated = fixFilesUpdated;
      progress.tranches[nextTranche].message      = `✅ Fixed in ${fixAttempt} correction pass(es). ${fixResult.message || ""}`;
      // Clear rejected tranche from state
      state.rejectedTranche = null;
      await saveProgress(bucket, projectPath, progress);

      state.progress        = progress;
      state.accumulatedFiles = accumulatedFiles;
      state.allUpdatedFiles  = allUpdatedFiles;
      await savePipelineState(bucket, projectPath, state);

      await saveAiResponse(bucket, projectPath, allUpdatedFiles, {
        jobId, trancheIndex: nextTranche, totalTranches: progress.totalTranches,
        status: "checkpoint",
        message: `Tranche ${nextTranche + 1} corrected and merged after ${fixAttempt} fix pass(es).`
      });

      // ── Chain to next tranche ────────────────────────────────
      if (nextTranche + 1 < progress.totalTranches) {
        await chainToSelf({ projectPath, jobId, mode: "tranche", nextTranche: nextTranche + 1 });
        return { statusCode: 200, body: JSON.stringify({ success: true, chained: true, phase: `fix_${nextTranche}_complete` }) };
      }

      // ── Last tranche was the one being fixed — finalize ──────
      const summaryParts = progress.tranches
        .filter(t => t.status === "complete")
        .map(t => `Tranche ${t.index + 1} — ${t.name}: ${t.message}`);

      await saveAiResponse(bucket, projectPath, allUpdatedFiles, {
        jobId, trancheIndex: progress.totalTranches - 1, totalTranches: progress.totalTranches,
        status: "final", message: summaryParts.join("\n\n") || "Build completed with corrections."
      });

      progress.status       = "complete";
      progress.finalMessage = `Build complete with corrections: ${allUpdatedFiles.length} file(s). Tokens: ${progress.tokenUsage.totals.input_tokens} in / ${progress.tokenUsage.totals.output_tokens} out.`;
      progress.completedTime = Date.now();
      await saveProgress(bucket, projectPath, progress);

      try { await bucket.file(`${projectPath}/ai_pipeline_state.json`).delete(); } catch (e) {}
      try { await bucket.file(`${projectPath}/ai_request.json`).delete(); }        catch (e) {}

      return { statusCode: 200, body: JSON.stringify({ success: true, phase: "complete_via_fix" }) };
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