/* netlify/functions/claudeCodeProxy-background.js */
/* ═══════════════════════════════════════════════════════════════════
   TRANCHED AI PIPELINE — v5.2 (+ Spec Validation Patch Loop)
   ─────────────────────────────────────────────────────────────────
   Each invocation handles ONE unit of work then chains to itself
   for the next, staying well under Netlify's 15-min limit.

   Invocation 0    ▸  "plan"    — Spec Validation Gate (3 Sonnet calls)
                       runs first, then Opus 4.6 creates a dependency-
                       ordered, contract-driven tranche plan.
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
                       simulation scenarios specific to this game's mechanics and current prompt layout.
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

   All intermediate state lives in Firebase so each invocation is
   stateless and can reconstruct context from the pipeline file.
   ═══════════════════════════════════════════════════════════════════ */

const fetch = require("node-fetch");
const admin = require("./firebaseAdmin");

const RETRY_POLICY = Object.freeze({
  parser_envelope: 2,
  critical_runtime: 2  // retained for fix-mode retryBudget fallback
});



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
    lowerName.includes("binding_law") ||
    lowerName.includes("binding-law") ||
    lowerName.includes("case_law") ||
    lowerName.includes("case-law") ||
    lowerName.includes("pattern_library") ||
    lowerName.includes("pattern-library") ||
    lowerContent.includes("working shipped games are the law") ||
    lowerContent.includes("binding law + pattern library") ||
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
      sections.push(`=== BINDING CHERRY3D SCAFFOLD LAW ===\n${scaffoldText}`);
    }
    if (sdkText) {
      sections.push(`=== SUBORDINATE CHERRY3D SDK / ENGINE NOTES ===\n${sdkText}`);
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
    throw new Error(`${phaseLabel}: binding Scaffold law missing from ai_system_instructions/.`);
  }
  if (!bundle?.sdkText) {
    throw new Error(`${phaseLabel}: subordinate SDK / Engine Notes missing from ai_system_instructions/.`);
  }
}

function flattenAssetsManifestEntries(entries) {
  const flat = [];
  for (const entry of Array.isArray(entries) ? entries : []) {
    if (!entry || typeof entry !== "object") continue;
    flat.push(entry);
    if (Array.isArray(entry.children) && entry.children.length > 0) {
      flat.push(...flattenAssetsManifestEntries(entry.children));
    }
  }
  return flat;
}

async function loadAssetsManifestIndex(bucket, projectPath) {
  try {
    const manifestFile = bucket.file(`${projectPath}/json/assets.json`);
    const [exists] = await manifestFile.exists();
    if (!exists) return new Map();
    const [content] = await manifestFile.download();
    const parsed = JSON.parse(content.toString());
    const manifestRoot = Array.isArray(parsed)
      ? parsed
      : Object.values(parsed || {}).find(v => Array.isArray(v)) || [];
    const flat = flattenAssetsManifestEntries(manifestRoot);
    const index = new Map();
    for (const entry of flat) {
      if (!entry?.title) continue;
      index.set(String(entry.title).toLowerCase(), {
        key: entry.key != null ? String(entry.key) : "",
        type: entry.type || "",
        title: entry.title
      });
    }
    return index;
  } catch (e) {
    console.warn("[ROSTER] Could not load assets.json for manifest annotation:", e.message);
    return new Map();
  }
}

function resolveRosterRole(asset) {
  return asset?.intendedRole || asset?.intendedUsage || asset?.selectionRationale || asset?.matchedRequirement || "";
}

function formatRosterNumber(value, digits = 3) {
  const num = Number(value);
  return Number.isFinite(num) ? num.toFixed(digits) : "N/A";
}

function buildRosterObjectContract(asset, stagedMeta, manifestMeta) {
  const geometry = asset?.geometryAnalysis || stagedMeta?.geometryAnalysis || null;
  const stagedPath = stagedMeta?.stagedPath || asset?.stagedPath || "(not staged)";
  const colormapFile = asset?.colormapFile || stagedMeta?.colormapFile || null;
  const colormapPath = asset?.colormapStagedPath || stagedMeta?.colormapStagedPath || null;
  const colormapConfidence = asset?.colormapConfidence || stagedMeta?.colormapConfidence || (colormapFile ? "HIGH" : "NONE");
  const uvNote = geometry?.uvMapping?.uvWrappingNote || "NOT AVAILABLE";
  const colormapManifestKey = asset?.colormapManifestKey || stagedMeta?.colormapManifestKey || "";
  const slotCount = Number(asset?.slotCount ?? stagedMeta?.slotCount ?? asset?.meshCount ?? stagedMeta?.meshCount ?? 0);
  const manifestKey = manifestMeta?.key ? `"${manifestMeta.key}"` : "(unresolved)";
  const sourceDoc = asset?.sourceRosterDocument || "Unknown source";

  if (!geometry) {
    return `  ┌─ [${asset.assetName}]
  │  Source:       ${sourceDoc}
  │  Role:         ${resolveRosterRole(asset)}
  │  Manifest key: ${manifestKey}
  │  Staged path:  ${stagedPath}
  │
  │  GEOMETRY CONTRACT: NOT AVAILABLE — scale and position conservatively;
  │    use scale [1,1,1] and position.y = 0 as safe defaults.
  │  TEXTURE CONTRACT:
  │    Colormap file: ${colormapFile || "NOT AVAILABLE"}
  │    Colormap path: ${colormapPath || "NOT AVAILABLE"}
  │    Colormap key:  ${colormapManifestKey || "NOT AVAILABLE"}
  │    Slot count:    ${slotCount || "NOT AVAILABLE"}
  │    Mesh count:    ${(asset?.meshCount != null ? asset.meshCount : (stagedMeta?.meshCount != null ? stagedMeta.meshCount : "NOT AVAILABLE"))}
  └─`;
  }

  const size = geometry?.geometry?.size || {};
  const placement = geometry?.placement || {};
  const scale = geometry?.scale || {};
  const origin = geometry?.origin || {};
  const scaleVec = Array.isArray(scale.suggestedGameScaleVec)
    ? `[${scale.suggestedGameScaleVec.map(v => formatRosterNumber(v, 6)).join(", ")}]`
    : "[N/A]";
  const assignmentLine = (colormapPath || colormapManifestKey)
    ? `defineMaterial('mat_<asset>', 255, 255, 255, 0.5, 0.0, '${colormapManifestKey || 'RESOLVED_COLORMAP_KEY_REQUIRED'}'); then apply that registered material key across slots 0..${Math.max(0, ((slotCount || 1) - 1))} via gameState._applyMat / slot-safe scaffold logic. material_file must contain the registered material key, never the raw staged path. Default to createInstance with a registered instance parent, but if working-game law proves per-object visual overrides do not survive instancing for that pool, use createObject consistently instead.`
    : "NOT AVAILABLE";

  return `  ┌─ [${asset.assetName}]
  │  Source:       ${sourceDoc}
  │  Role:         ${resolveRosterRole(asset)}
  │  Manifest key: ${manifestKey}
  │  Staged path:  ${stagedPath}
  │
  │  GEOMETRY CONTRACT (measured values — use these directly in code):
  │    Bounding box:   W=${formatRosterNumber(size.x)}  H=${formatRosterNumber(size.y)}  D=${formatRosterNumber(size.z)}  (authored unit: ${scale.authoredUnit || "unknown"})
  │    Origin class:   ${origin.classification || "N/A"}
  │    Dominant axis:  ${placement.dominantAxis || "N/A"}
  │    Forward hint:   ${placement.forwardHint || "N/A"}
  │
  │  PLACEMENT CONTRACT (copy these values verbatim into tranche code):
  │    position.y for floor placement:    ${formatRosterNumber(placement.floorY, 6)}
  │    position.y for vertical centering: ${formatRosterNumber(placement.centerY, 6)}
  │    position.x centering correction:   ${formatRosterNumber(placement.centerOffsetX, 6)}
  │    position.z centering correction:   ${formatRosterNumber(placement.centerOffsetZ, 6)}
  │    Suggested scale (largest dim → 1): ${formatRosterNumber(scale.suggestedGameScale, 6)}
  │    Scale vector:                      ${scaleVec}
  │    Scale warning:                     ${scale.scaleWarning || "null"}
  │
  │  TEXTURE CONTRACT:
  │    Colormap file:    ${colormapFile || "NOT AVAILABLE"}
  │    Colormap path:    ${colormapPath || "NOT AVAILABLE"}
  │    Colormap key:     ${colormapManifestKey || "NOT AVAILABLE"}
  │    Colormap conf.:   ${colormapConfidence}
  │    UV mapping:       ${uvNote}
  │    Slot count:       ${slotCount || "NOT AVAILABLE"}
  │    Mesh count:       ${(asset?.meshCount != null ? asset.meshCount : (stagedMeta?.meshCount != null ? stagedMeta.meshCount : "NOT AVAILABLE"))}
  │    Assignment:       ${assignmentLine}
  └─`;
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

    const manifestIndex = await loadAssetsManifestIndex(bucket, projectPath);
    const stagedIndex = new Map(
      (r.stagedAssets || [])
        .filter(a => a?.assetName)
        .map(a => [String(a.assetName).toLowerCase(), a])
    );

    const objs = (r.objects3d || []).map(a => {
      const manifestMeta = manifestIndex.get(String(a.assetName || "").toLowerCase());
      const stagedMeta = stagedIndex.get(String(a.assetName || "").toLowerCase());
      return buildRosterObjectContract(a, stagedMeta, manifestMeta);
    }).join("\n");

    const particleTextures = (r.textureAssets || []).filter(a => a.particleEffectTarget);

    const texsParticle = particleTextures.map(a => {
      const manifestMeta = manifestIndex.get(String(a.assetName || "").toLowerCase());
      return `  - ${a.assetName} (from ${a.sourceRosterDocument}) → particleEffectTarget: "${a.particleEffectTarget}" | ${resolveRosterRole(a)} | manifest key: ${manifestMeta?.key ? `"${manifestMeta.key}"` : "(unresolved)"}`;
    }).join("\n");

    const staged = (r.stagedAssets || []).map(a => {
      const manifestMeta = manifestIndex.get(String(a.assetName || "").toLowerCase());
      const colormapSuffix = a.colormapStagedPath ? ` | colormap: ${a.colormapStagedPath}` : "";
      return `  - ${a.assetName} → ${a.stagedPath}${manifestMeta?.key ? ` | manifest key: "${manifestMeta.key}"` : ""}${colormapSuffix}`;
    }).join("\n");
    const vn = r.visualDirectionNotes || {};
    const sf = r._meta?.stagedFolder || "";

    return `

═══════════════════════════════════════════════════════════
APPROVED GAME-SPECIFIC ASSET ROSTER — FIRST-CLASS COMPANION DOCUMENT
Authority equal to the Master Prompt and all reference images.
All tranche planning and execution MUST use these approved assets.
═══════════════════════════════════════════════════════════

GAME INTERPRETATION:
${r.gameInterpretationSummary || ""}

APPROVED 3D OBJECTS (${(r.objects3d||[]).length}):
${objs || "  (none)"}

APPROVED PARTICLE EFFECT TEXTURES (${particleTextures.length}) — Foundation-B MUST populate PARTICLE_TEX_PATHS (staged paths) and gameState.particleTextureIds (manifest keys):
${texsParticle || "  (none)"}

STAGED ASSET FOLDER: ${sf}
STAGED FILES (Firebase paths — use these in models/2 and models/23):
${staged || "  (none extracted)"}

ASSETS.JSON MANIFEST LOCATIONS (after frontend copy + sync):
- Approved 3D objects register as children of the Models folder, key "15".
- Approved particle textures register at root level with their own assigned numeric keys.
- The per-asset manifest keys are resolved above — use those exact keys for all asset references in models/2 and models/23.

VISUAL DIRECTION:
  Color Direction:    ${vn.colorDirection || "N/A"}
  Material Style:     ${vn.materialStyle || "N/A"}
  Realism Level:      ${vn.realismLevel || "N/A"}
  Environmental Tone: ${vn.environmentalTone || "N/A"}
  Surface Treatment:  ${vn.surfaceTreatment || "N/A"}
  FX Relevance:       ${vn.fxRelevance || "N/A"}

TRANCHE DESIGN & EXECUTION REQUIREMENT:
1. Tranche Design MUST plan explicitly around these approved assets.
2. Every tranche touching rendered content, obstacles, environment, or scene objects MUST incorporate the relevant approved assets from this roster.
3. Visual Direction notes above govern color, material, and FX treatment throughout all tranches.
4. Reference staged files by their Firebase staged paths or assets.json keys.
5. Color direction and surface treatment must be consistent throughout all tranches.
6. PARTICLE TEXTURE REGISTRY: A Foundation-B sub-tranche MUST be planned immediately after Foundation-A. Its job is to populate BOTH (a) PARTICLE_TEX_PATHS keyed by particleEffectTarget using the exact staged Firebase paths from the Approved Asset Roster block, and (b) gameState.particleTextureIds keyed by particleEffectTarget using the exact assets.json manifest keys. This tranche must complete before any particle template or emitter tranche.
7. Every approved particle effect texture used by a particle billboard or sphere MUST be applied at the particle template level: registerParticleTemplate(... extraData: { material_file: PARTICLE_TEX_PATHS[effectName] }) or an equivalent direct particle slot material_file assignment. Populating gameState.particleTextureIds alone is not sufficient. Particle textures are a separate workflow from non-primitive scene-object defineMaterial/_applyMat contracts.
8. 3D OBJECT REGISTRY: Every tranche touching visible scene content MUST branch cleanly between the five Cherry3D system primitives (cube, cylinder, sphere, plane, planevertical) and non-primitive approved roster 3D objects. If the object is intentionally one of those five primitives, skip external scan/roster geometry-texture-slotCount enforcement for that object and use primitive-safe logic only. For every other visible gameplay object, you MUST use an approved roster 3D object via gameState.objectids and the resolved assets.json manifest keys surfaced above. Using a Cherry3D primitive as a visible gameplay object when a roster asset covers that role is a tranche execution defect.
9. GEOMETRY CONTRACTS surfaced above are arithmetic, not suggestions. When present, copy the exact placement and scale values into planning and execution prompts without paraphrase.
10. TEXTURE CONTRACTS surfaced above are mandatory for non-primitive roster 3D objects. Use the scaffold material-registry path: define a registered material whose albedo_texture is the resolved numeric colormap manifest key from assets.json, then apply that material key across all valid slots using gameState._applyMat / registerObjectContract-safe logic. material_file must hold the registered material key, never a raw staged path.
11. SLOT CONTRACT: slotCount is the primary hard loop bound for material application (fallback to meshCount only if slotCount is unavailable).
12. TEXTURED INSTANCE PATH: For textured non-primitive scene objects, register an instance parent and use createInstance as the default path. If working-game law proves that per-object visual overrides do not survive instancing for that pool, use createObject consistently for that pool instead of mixing createInstance and createObject.
═══════════════════════════════════════════════════════════`;
  } catch (e) {
    console.warn("[ROSTER] Could not load approved roster:", e.message);
    return "";
  }
}

/* ── DYNAMIC_ARCHITECTURE_JSON_SCHEMA — REMOVED ─────────────
   Architect pass has been merged into single-pass planner.
   No intermediate architecture spec is generated. ────────── */



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
  // Network-level transient failures — no HTTP status code, matched by message
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

async function callClaude(apiKey, { model, maxTokens, system, userContent, effort, budgetTokens }) {
  const body = {
    model,
    max_tokens: maxTokens,
    system,
    messages: [{ role: "user", content: userContent }]
  };

  const headers = {
    "Content-Type": "application/json",
    "x-api-key": apiKey,
    "anthropic-version": "2023-06-01"
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
        headers,
        body: JSON.stringify(body)
      });

      const rawText = await res.text();
      let data = null;

      if (rawText) {
        try {
          data = JSON.parse(rawText);
        } catch (parseErr) {
          const parseError = new Error(`Claude returned non-JSON response: ${parseErr.message}`);
          parseError.status = res.status;
          parseError.rawText = rawText;
          parseError.isRetryableOverload = isClaudeOverloadError(res.status, rawText);
          throw parseError;
        }
      }

      if (!res.ok) {
        const errMsg = data?.error?.message || `Claude API error (${res.status})`;
        const err = new Error(errMsg);
        err.status = res.status;
        err.data = data;
        err.isRetryableOverload = isClaudeOverloadError(res.status, errMsg);
        throw err;
      }

      const textBlock = data?.content?.find(block => block.type === "text")?.text;
      if (!textBlock) {
        throw new Error("Claude response missing text block");
      }

      return { text: textBlock, usage: data?.usage || null };
    } catch (err) {
      const status = err?.status || null;
      const retryable = Boolean(err?.isRetryableOverload) || isClaudeOverloadError(status, err?.message || "");
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
  • 2 retries only for parser/envelope failures or truly critical scaffold/runtime issues.`;



function buildMasterPromptLayoutGuidance(masterPrompt = "") {
  const prompt = String(masterPrompt || "");
  const hasNewStructuredLayout =
    /#\s*1\.\s*SESSION DECISIONS/i.test(prompt) &&
    /#\s*2\.\s*GAME IDENTITY/i.test(prompt) &&
    /#\s*3\.\s*IMPLEMENTATION CONTRACT/i.test(prompt);
  const hasLegacy63Layout = /\b6\.3(\.\d+)?\b/.test(prompt);

  if (hasNewStructuredLayout) {
    return `MASTER PROMPT LAYOUT DETECTED:
- Section 1 = session decisions / fixed run constraints.
- Section 2 = game identity / fantasy / win-loss / session loop.
- Section 3.x = implementation contract. Treat this as the highest-authority gameplay + technical contract for movement, camera, initialization, overlay, lifecycle, ownership, and exact variables.
- Section 4.x = synopsis matrix. Use 4.1+ for mechanics/rules, world/object inventory, VFX, colours/audio, and authored game content requirements.
- Section 5 = runtime registry / exact names / counts / materials / particle keys / pools.
- Section 6 = author-provided tranche plan. Treat it as advisory sequencing guidance only. Preserve dependency reality, safety, and execution size even if you refine or split it.
- Section 7 = validation contract / hard-fail conditions / non-negotiable outcome checks.
- When this layout is present, NEVER force legacy 6.3 anchors. Use the ACTUAL section numbers from this prompt in anchorSections (for example: 3.1, 3.3, 3.4, 4.1, 4.2, 4.3, 5, 7).
- Sections 3, 4, 5, and 7 are authoritative. Sections 1 and 2 provide context. Section 6 informs sequencing but does not override dependency reality.`;
  }

  if (hasLegacy63Layout) {
    return `MASTER PROMPT LAYOUT DETECTED:
- Legacy 6.3-style structure is present.
- Use the actual 6.3 subsection numbers surfaced by the prompt in anchorSections when they exist.
- Still preserve dependency reality over raw document order.`;
  }

  return `MASTER PROMPT LAYOUT DETECTED:
- No canonical legacy or new layout markers were found.
- Infer the prompt's real section hierarchy from its headings and subheadings.
- Use the ACTUAL headings/subheadings present in the prompt for anchorSections.
- Never invent 6.3 anchors when the prompt does not use them.`;
}

function countOccurrences(haystack, needle) {
  if (!needle) return 0;
  return String(haystack || '').split(needle).length - 1;
}

function parseRosterContractValue(block, pattern) {
  const match = String(block || "").match(pattern);
  return match && match[1] ? String(match[1]).trim() : "";
}

function parseApprovedRosterContracts(approvedRosterBlock = "") {
  const contracts = [];
  const blockRegex = /┌─ \[([^\]]+)\]([\s\S]*?)└─/g;
  let match;

  while ((match = blockRegex.exec(String(approvedRosterBlock || ""))) !== null) {
    const assetName = String(match[1] || "").trim();
    const block = match[0];
    const texturePath = parseRosterContractValue(block, /Colormap path:\s+([^\n]+)/i);
    const scaleWarning = parseRosterContractValue(block, /Scale warning:\s+([^\n]+)/i);
    const dominantAxis = parseRosterContractValue(block, /Dominant axis:\s+([^\n]+)/i);

    contracts.push({
      assetName,
      geometryAvailable: /GEOMETRY CONTRACT \(measured values/i.test(block),
      dominantAxis,
      floorY: parseRosterContractValue(block, /position\.y for floor placement:\s+([^\n]+)/i),
      centerOffsetX: parseRosterContractValue(block, /position\.x centering correction:\s+([^\n]+)/i),
      centerOffsetZ: parseRosterContractValue(block, /position\.z centering correction:\s+([^\n]+)/i),
      suggestedGameScale: parseRosterContractValue(block, /Suggested scale \(largest dim → 1\):\s+([^\n]+)/i),
      scaleVector: parseRosterContractValue(block, /Scale vector:\s+([^\n]+)/i),
      scaleWarning,
      texturePath: /^(NOT AVAILABLE|null|\(not staged\))$/i.test(texturePath) ? "" : texturePath,
      colormapManifestKey: (() => {
        const key = parseRosterContractValue(block, /Colormap key:\s+([^\n]+)/i);
        return /^(NOT AVAILABLE|null|\(unresolved\))$/i.test(key) ? "" : key;
      })(),
      slotCount: Number(parseRosterContractValue(block, /Slot count:\s+([^\n]+)/i) || 0),
      meshCount: Number(parseRosterContractValue(block, /Mesh count:\s+([^\n—]+)/i) || 0)
    });
  }

  return contracts;
}

function promptMentionsAsset(prompt, assetName = "") {
  const promptText = String(prompt || "").toLowerCase();
  const exact = String(assetName || "").trim().toLowerCase();
  const base = exact.replace(/\.[a-z0-9]+$/i, "");
  if (!exact && !base) return false;
  if (exact && promptText.includes(exact)) return true;
  if (base && promptText.includes(base)) return true;
  return false;
}

function buildContractPromptReview(progress, approvedRosterBlock = "") {
  const contracts = parseApprovedRosterContracts(approvedRosterBlock);
  const tranches = Array.isArray(progress?.tranches) ? progress.tranches : [];
  const items = [];
  let reviewedTranches = 0;
  let issueCount = 0;

  tranches.forEach((tranche, index) => {
    const prompt = String(tranche?.prompt || "");
    const referencedAssets = contracts.filter(contract => promptMentionsAsset(prompt, contract.assetName));
    const warnings = [];

    if (referencedAssets.length > 0) {
      reviewedTranches += 1;
    }

    referencedAssets.forEach((contract) => {
      const missing = [];

      if (contract.geometryAvailable) {
        [
          ["floorY", contract.floorY],
          ["centerOffsetX", contract.centerOffsetX],
          ["centerOffsetZ", contract.centerOffsetZ],
          ["suggestedGameScale", contract.suggestedGameScale],
          ["scaleVector", contract.scaleVector],
          ["dominantAxis", contract.dominantAxis]
        ].forEach(([label, value]) => {
          if (value && !prompt.includes(value)) {
            missing.push(`${label}=${value}`);
          }
        });

        if (contract.scaleWarning && contract.scaleWarning.toLowerCase() !== "null" && !prompt.includes(contract.scaleWarning)) {
          missing.push(`scaleWarning=${contract.scaleWarning}`);
        }
      }

      if (contract.texturePath && !prompt.includes(contract.texturePath)) {
        missing.push(`colormapPath=${contract.texturePath}`);
      }
      if (contract.colormapManifestKey && !prompt.includes(contract.colormapManifestKey)) {
        missing.push(`colormapKey=${contract.colormapManifestKey}`);
      }

      if (missing.length > 0) {
        warnings.push(`${contract.assetName}: missing prompt-carried contract values -> ${missing.join(" | ")}`);
      }
    });

    tranche.contractPromptReviewWarnings = warnings;
    tranche.contractPromptReviewStatus = warnings.length > 0 ? "warning" : (referencedAssets.length > 0 ? "ok" : "not_applicable");

    if (warnings.length > 0) {
      issueCount += warnings.length;
    }

    items.push({
      trancheIndex: index,
      trancheName: tranche?.name || `Tranche ${index + 1}`,
      status: tranche.contractPromptReviewStatus,
      assets: referencedAssets.map(contract => contract.assetName),
      warnings
    });
  });

  const summary = issueCount > 0
    ? `Informational tranche prompt contract review: ${issueCount} possible omission(s) across ${reviewedTranches} reviewed tranche(s). Build continues; review the UI log and tranche cards later.`
    : `Informational tranche prompt contract review: no obvious missing carried contract values were detected across ${reviewedTranches} reviewed tranche(s).`;

  return {
    status: "informational",
    generatedAt: Date.now(),
    reviewedTranches,
    issueCount,
    summary,
    items
  };
}


function escapeRegex(value = "") {
  return String(value || "").replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function buildDeterministicContractAppendixForPrompt(prompt, contracts = []) {
  const referencedAssets = contracts.filter(contract => promptMentionsAsset(prompt, contract.assetName));
  if (referencedAssets.length === 0) return "";
  if (String(prompt || "").includes("=== DETERMINISTIC ROSTER CONTRACT CARRY-THROUGH (AUTO-INJECTED) ===")) {
    return "";
  }

  const lines = referencedAssets.map((contract) => {
    const geometryLines = contract.geometryAvailable ? [
      `  floorY=${contract.floorY || "N/A"}`,
      `  centerOffsetX=${contract.centerOffsetX || "N/A"}`,
      `  centerOffsetZ=${contract.centerOffsetZ || "N/A"}`,
      `  suggestedGameScale=${contract.suggestedGameScale || "N/A"}`,
      `  scaleVector=${contract.scaleVector || "N/A"}`,
      `  dominantAxis=${contract.dominantAxis || "N/A"}`,
      `  scaleWarning=${contract.scaleWarning || "null"}`
    ] : [
      `  geometryContract=NOT AVAILABLE`
    ];

    const slotCount = contract.slotCount || contract.meshCount || 0;
    const textureLines = (contract.texturePath || contract.colormapManifestKey)
      ? [
          `  colormapPath=${contract.texturePath || 'NOT AVAILABLE'}`,
          `  colormapKey=${contract.colormapManifestKey || 'NOT AVAILABLE'}`,
          `  slotCount=${slotCount}`,
          `  meshCount=${contract.meshCount || 0}`,
          `  assetClass=EXTERNAL_NON_PRIMITIVE_SCANNED_OBJECT`,
          `  textureAssignment=Define a registered material whose albedo_texture uses the resolved numeric colormap manifest key ${contract.colormapManifestKey || 'REQUIRED'}, then apply that registered material key across EVERY valid slot N from 0 to ${Math.max(0, (slotCount || 1) - 1)} (${slotCount || 1} slot(s) total) via gameState._applyMat or equivalent slot-safe scaffold logic. material_file must contain the registered material key, never the staged path. Default to createInstance with a registered instance parent, but if working-game law proves per-object visual overrides do not survive instancing for that pool, use createObject consistently instead. Skip this workflow only for the five Cherry3D system primitives (cube, cylinder, sphere, plane, planevertical).`
        ]
      : [
          `  colormapPath=NOT AVAILABLE`,
          `  slotCount=${slotCount}`,
          `  meshCount=${contract.meshCount || 0}`
        ];

    return [
      `- ${contract.assetName}`,
      ...geometryLines,
      ...textureLines
    ].join("\n");
  }).join("\n");

  return `

=== DETERMINISTIC ROSTER CONTRACT CARRY-THROUGH (AUTO-INJECTED) ===
For every asset already named in this tranche, the following roster contract values are mandatory and must be copied verbatim into the emitted code and audit-trail comments. Do not paraphrase, round, or omit them.
${lines}
=== END DETERMINISTIC ROSTER CONTRACT CARRY-THROUGH ===`;
}

function injectDeterministicContractsIntoPlan(plan, approvedRosterBlock = "") {
  const contracts = parseApprovedRosterContracts(approvedRosterBlock);
  const rawTranches = Array.isArray(plan?.tranches) ? plan.tranches : [];
  plan.tranches = rawTranches.map((tranche) => {
    const basePrompt = String(tranche?.prompt || "").trim();
    const appendix = buildDeterministicContractAppendixForPrompt(basePrompt, contracts);
    const prompt = appendix ? `${basePrompt}${appendix}` : basePrompt;
    return {
      ...tranche,
      originalPrompt: tranche?.originalPrompt || basePrompt,
      prompt,
      contractCarryThroughInjected: Boolean(appendix),
      contractCarryThroughAssets: contracts
        .filter(contract => promptMentionsAsset(basePrompt, contract.assetName))
        .map(contract => contract.assetName)
    };
  });
  return plan;
}

function buildContractCodeReviewForTranche(tranche, updatedFiles, approvedRosterBlock = "") {
  const contracts = parseApprovedRosterContracts(approvedRosterBlock);
  const prompt = String(tranche?.prompt || "");
  const combinedCode = Array.isArray(updatedFiles)
    ? updatedFiles.map(file => String(file?.content || "")).join("\n\n")
    : "";
  const referencedAssets = contracts.filter(contract => promptMentionsAsset(prompt, contract.assetName));
  const warnings = [];

  referencedAssets.forEach((contract) => {
    const assetPattern = escapeRegex(contract.assetName);
    const basePattern = escapeRegex(String(contract.assetName || "").replace(/\.[a-z0-9]+$/i, ""));
    const placementAuditPresent = new RegExp(`\\[(?:${assetPattern}|${basePattern})\\]\\s+placement contract applied`, "i").test(combinedCode);
    const textureAuditPresent = new RegExp(`(?:${assetPattern}|${basePattern}).{0,120}applied colormap|applied colormap.{0,120}(?:${assetPattern}|${basePattern})`, "is").test(combinedCode);
    const missing = [];

    if (contract.geometryAvailable) {
      if (!placementAuditPresent) missing.push("placementAuditTrail");
      if (contract.floorY && !combinedCode.includes(contract.floorY)) missing.push(`floorY=${contract.floorY}`);
      if (contract.centerOffsetX && !combinedCode.includes(contract.centerOffsetX)) missing.push(`centerOffsetX=${contract.centerOffsetX}`);
      if (contract.centerOffsetZ && !combinedCode.includes(contract.centerOffsetZ)) missing.push(`centerOffsetZ=${contract.centerOffsetZ}`);
      const hasScaleValue = (contract.scaleVector && combinedCode.includes(contract.scaleVector)) || (contract.suggestedGameScale && combinedCode.includes(contract.suggestedGameScale));
      if (!hasScaleValue) missing.push(`scaleVector|suggestedGameScale=${contract.scaleVector || contract.suggestedGameScale || "N/A"}`);
    }

    if (contract.texturePath || contract.colormapManifestKey) {
      if (!textureAuditPresent) missing.push("textureAuditTrail");
      if (contract.texturePath && !combinedCode.includes(contract.texturePath)) missing.push(`colormapPath=${contract.texturePath}`);
      if (contract.colormapManifestKey && !combinedCode.includes(contract.colormapManifestKey)) missing.push(`colormapKey=${contract.colormapManifestKey}`);

      const hasRegisteredMaterial = /defineMaterial\s*\(/i.test(combinedCode);
      if (!hasRegisteredMaterial) missing.push("defineMaterial(...)");

      const hasSafeApply = /_applyMat\s*\(/.test(combinedCode) || /material_file\s*[:=]/i.test(combinedCode);
      if (!hasSafeApply) missing.push("registeredMaterialApplication");

      const hasCreateInstance = /createInstance\s*\(/.test(combinedCode);
      const hasCreateObjectException = /createObject\s*\(/.test(combinedCode) && /(per-object visual override|instance parent[^\n]{0,80}unsafe|createobject consistently|instancing does not respect)/i.test(combinedCode);
      if (!hasCreateInstance && !hasCreateObjectException) {
        missing.push("createInstance(...)|explicitCreateObjectException");
      }

      const slotCount = contract.slotCount || contract.meshCount || 0;
      if (slotCount > 1 && !/_applyMat\s*\(/.test(combinedCode)) {
        let missedSlots = [];
        for (let slot = 0; slot < slotCount; slot++) {
          const slotPattern = new RegExp(`data\\[['"]${slot}['"]\\]\\.material_file`, "i");
          if (!slotPattern.test(combinedCode)) {
            missedSlots.push(slot);
          }
        }
        if (missedSlots.length > 0) {
          missing.push(`slotCoverageMissing=[${missedSlots.join(",")}] (expected all slots 0-${slotCount - 1} to have registered-material coverage)`);
        }
      }
    }

    if (missing.length > 0) {
      warnings.push(`${contract.assetName}: emitted code missing contract evidence -> ${missing.join(" | ")}`);
    }
  });

  return {
    status: warnings.length > 0 ? "warning" : (referencedAssets.length > 0 ? "ok" : "not_applicable"),
    assets: referencedAssets.map(contract => contract.assetName),
    warnings
  };
}

function summarizeContractCodeReview(progress) {
  const tranches = Array.isArray(progress?.tranches) ? progress.tranches : [];
  let reviewedTranches = 0;
  let issueCount = 0;
  const items = tranches.map((tranche, index) => {
    const warnings = Array.isArray(tranche?.contractCodeReviewWarnings) ? tranche.contractCodeReviewWarnings : [];
    const assets = Array.isArray(tranche?.contractCodeReviewAssets) ? tranche.contractCodeReviewAssets : [];
    if (assets.length > 0) reviewedTranches += 1;
    if (warnings.length > 0) issueCount += warnings.length;
    return {
      trancheIndex: index,
      trancheName: tranche?.name || `Tranche ${index + 1}`,
      status: tranche?.contractCodeReviewStatus || (assets.length > 0 ? "ok" : "not_applicable"),
      assets,
      warnings
    };
  });

  const summary = issueCount > 0
    ? `Informational tranche code contract review: ${issueCount} possible omission(s) across ${reviewedTranches} reviewed tranche(s).`
    : `Informational tranche code contract review: no obvious missing contract evidence was detected across ${reviewedTranches} reviewed tranche(s).`;

  return {
    status: "informational",
    generatedAt: Date.now(),
    reviewedTranches,
    issueCount,
    summary,
    items
  };
}

function selectNextSequentialTranche(progress, preferredIndex = null) {
  const tranches = Array.isArray(progress?.tranches) ? progress.tranches : [];
  const pendingIndices = tranches
    .map((_, index) => index)
    .filter(index => !isTrancheTerminalStatus(tranches[index]?.status));

  if (pendingIndices.length === 0) {
    return { ready: false, done: true, index: null, reason: "all tranches are complete" };
  }

  if (
    Number.isInteger(preferredIndex) &&
    preferredIndex >= 0 &&
    preferredIndex < tranches.length &&
    !isTrancheTerminalStatus(tranches[preferredIndex]?.status)
  ) {
    return { ready: true, done: false, index: preferredIndex };
  }

  return { ready: true, done: false, index: pendingIndices[0] };
}

function normalizeArray(value, fallback = []) {
  if (Array.isArray(value)) return value.filter(Boolean);
  if (value === undefined || value === null || value === '') return [...fallback];
  return [value].filter(Boolean);
}

function enforceTrancheValidationBlock(plan) {
  const rawTranches = Array.isArray(plan?.tranches) ? plan.tranches : [];
  plan.tranches = rawTranches.map((tranche, index) => {
    const expectedFiles = normalizeArray(tranche.expectedFiles || tranche.filesTouched, ['models/2', 'models/23']);
    return {
      kind: tranche.kind || 'build',
      name: tranche.name || `Tranche ${index + 1}`,
      description: tranche.description || tranche.purpose || `Implement tranche ${index + 1}.`,
      anchorSections: normalizeArray(tranche.anchorSections, ['prompt_contract']),
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
      expectedFiles
    };
  });

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

   All three calls use claude-sonnet-4-6.
   Thinking is omitted for these calls; use explicit effort only
   so they stay fast and cheap — under ~20 seconds total.
   ═══════════════════════════════════════════════════════════════ */

/* ── Known engine constraints injected into Call 2 ──────────────
   These are scaffold-level facts that the Master Prompt author
   should not have to write — they apply to every Cherry3D game. */
const SCAFFOLD_VALIDATION_CONSTRAINTS = `
KNOWN ENGINE CONSTRAINTS (Cherry3D scaffold v19):
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

5. COORDINATE SPACE COLLISION: Any angle or position used in a
   collision test must be compared in the same coordinate space.
   If the spec stores child attachment angles in local space but
   compares them directly against world-space angles, note a
   coordinateSpaceCollisionRisk.

6. ENGINE AUTO-ROTATION FROM VELOCITY: Cherry3D can apply visual
   rotation from positional delta. Any object that must keep a fixed
   orientation while moving, flying, or orbiting needs an explicit
   per-frame rotate overwrite. If the spec omits that correction,
   note an autoRotationRisk.

7. INSTANCE PARENT FRUSTUM CULLING: Instanced children inherit
   visibility from the instance parent's bounding box. Hiding the
   instance parent by scaling it to near-zero can make every child
   invisible. Instance parents should be parked off-screen instead.
   If the spec uses near-zero parent scale to hide instance roots,
   note an instancingCullRisk.

8. DOM OVERLAY INPUT OWNERSHIP: The raw overlayRoot provided by the
   engine must remain untouched. Pointer-event toggling belongs on the
   inner game-root element (gameState._gameRootEl), not overlayRoot.
   If a spec implies disabling input on the platform wrapper itself,
   note an overlayInputOwnershipRisk.

9. TEXTURED ROSTER OBJECT CONTRACT: For non-primitive textured scene
   objects, Cherry3D expects the scaffold material-registry path:
   defineMaterial(..., albedo_texture=<resolved numeric colormap key>),
   then apply that registered material key to the object's valid slots.
   material_file must hold the registered material key, not a raw file
   path. The working texture path for these objects is createInstance
   with a registered instance parent. If the spec implies raw-path
   material_file writes for scene objects or relies on plain createObject
   for textured roster geometry, note a texturedObjectContractRisk.

10. CHILD RIGIDBODY LOCAL-SPACE POSITION (Non-Negotiable 13): A RigidBody
   attached as a child of a visual parent operates in local-space. Its
   rbPosition must stay [0,0,0] unless a deliberate local offset is truly
   intended. Passing world-space coordinates into a child rbPosition causes
   POSITION DOUBLING. If the spec passes world coordinates to a child
   rigidbody position, note a childRbPositionDoublingRisk.

11. DYNAMIC VISUAL AUTO-SYNC (Non-Negotiable 16): DYNAMIC visuals do NOT
   always auto-sync with their rigidbody. If the spec moves a DYNAMIC
   actor and expects the visual follows without explicit getMotionState()
   mirroring, note a dynamicVisualDriftRisk. The scaffold provides
   syncDynamicVisualFromRigidBody() for this purpose.

12. TILE-CENTERING AXIS LAW (Non-Negotiable 19): Snap / tile-centering
   correction may ONLY adjust the perpendicular (non-movement) axis.
   For a game moving along Z, only X may be corrected. For a game moving
   along X, only Z may be corrected. If a spec applies centering
   correction to the same axis the player is moving along, note a
   tileSnapAxisViolationRisk.

13. SHARED WASM ASSET CAP (Non-Negotiable 21): The WASM engine enforces one
   hard instance cap per asset globally. Two ScenePools sharing the same
   asset ID and instance parent compete against that single cap. If the spec
   describes two separate pool types using the same geometry for the same
   role (e.g. road tiles and rail tiles both using the same roadStraight
   asset), their combined addObject calls can exceed the cap mid-gameplay
   → OOB crash. Note a sharedAssetCapRisk. They must be declared as a
   single aliased pool with one maxInstances cap.

14. PARTICLE TEMPLATE CROSS-SESSION LEAK (Non-Negotiable 20): Every particle
   template registered in onInit is a live WASM scene object. The engine's
   info worker continues posting position/state updates to their handles
   after session end. If a game registers custom particle templates (any
   ptex_* or game-specific templates beyond particleBillboard/particleSphere)
   but has no explicit teardown mechanism for ALL of them in the session-end
   path, note a particleTemplateleakRisk. Templates must be removed via the
   gameState.particleTemplates registry loop in onDestroy — never via a
   hand-written key list.

15. ASSET READINESS RACE (Non-Negotiable 22): onInit fires before the WASM
   engine has necessarily finished loading every project asset. Calling
   registerInstanceParent or registerParticleTemplate with an asset ID that
   isn't loaded yet dereferences a null pointer → OOB. If the spec registers
   a large number of instance parents or particle templates at startup with
   no readiness check or retry mechanism, note an assetReadinessRaceRisk.
   Burst emitter creation must be deferred past the retry flush via
   _createBurstEmitters() so particlesettings.object is never null.
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

${buildMasterPromptLayoutGuidance(masterPrompt)}

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

${buildMasterPromptLayoutGuidance(masterPrompt)}

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
- Preserve the existing prompt layout and heading hierarchy.
- Insert each missing rule into the most relevant existing section/subsection when that section already exists (for example 3.x, 4.x, 5, or 7). Only append at the end when no relevant section exists.
- Keep the author's section numbering / heading style intact.
- Output the COMPLETE updated Master Prompt — every existing line intact except for the minimal inserted additions.

${buildMasterPromptLayoutGuidance(masterPrompt)}

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
    model:       'claude-sonnet-4-6',
    maxTokens:   8000,
    effort:      'low',
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
    model:       'claude-sonnet-4-6',
    maxTokens:   6000,
    effort:      'low',
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

  // ── TEMPORARY DISABLE — set to false to re-enable sim 0–8 validation ──
  const VALIDATION_ENABLED = false;
  if (!VALIDATION_ENABLED) {
    console.warn(`[VALIDATION] DISABLED — skipping all sim validations, proceeding directly to planning`);
    progress.status = 'planning';
    progress.validationSkipped = true;
    progress.validationSkipReason = 'Validation temporarily disabled via VALIDATION_ENABLED flag';
    await saveProgress(bucket, projectPath, progress);
    return { passed: true, skipped: true, activePrompt: masterPrompt };
  }

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
      model:       'claude-sonnet-4-6',
      maxTokens:   3000,
      effort:      'low',
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
        model:       'claude-sonnet-4-6',
        maxTokens:   masterPrompt.length > 20000 ? 16000 : 8000,
        effort:      'low',
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
        contractPromptReview: null
      };
      await saveProgress(bucket, projectPath, progress);

      const planningSystem = `You are an expert game development planner for the Cherry3D engine.

Your job: read the user's request, the existing project files, and the instruction bundle below. Then split the build into sequential, self-contained TRANCHES that can be executed one at a time by a coding AI.

${instructionBundle.combinedText}

INSTRUCTION PRECEDENCE:
1. The Cherry3D Scaffold is the binding codified law extracted from shipped working games.
2. The SDK / Engine Notes are subordinate. Use them only for engine facts, API details, threading rules, property paths, and anti-pattern avoidance when the scaffold is silent.
3. If both instruction layers apply to the same topic, the Scaffold wins for architecture, lifecycle shape, immutable sections, required state fields, build sequencing, UI ownership, movement authority, materials/textures, particles, and scene mutation rails.
4. Never elevate the SDK into a parallel lawbook. It fills certainty gaps only where the scaffold does not already settle the issue.
5. Never plan tranches that delete, replace, bypass, or work around an immutable scaffold block. Adapt the requested game to the scaffold.
6. Pick one lawful pattern family per subsystem and preserve it through planning, execution, validation, and repair. Do not silently switch families mid-pipeline.
7. REFERENCE IMAGES (if attached): Any images attached to this request are first-class game design inputs with authority equal to the Master Prompt. They define the intended visual style, layout, object types, and complexity level. Where the image and the text spec diverge, treat the image as the authoritative definition of what must be built. Every tranche that involves visual elements, entities, or layouts must reconcile against the attached images.

PLANNING RULES:
1. The Master Prompt's actual contract sections are the center of gravity. Read the real heading structure first, then anchor every core gameplay tranche to the prompt's actual authoritative sections/subsections. If the prompt uses the new layout, prioritize Sections 3.x, 4.x, 5, and 7. If it uses a legacy layout, use those real legacy section numbers. Never invent 6.3 anchors when the prompt does not contain them.
2. Plan the build like a house: foundation before controls, controls before authored playfield shell, shell before gameplay loop, gameplay loop before progression/HUD, progression before feedback/polish.
3. Each tranche prompt must be FULLY SELF-CONTAINED — embed the exact game-specific rules, variable names, slot layouts, code snippets, and pitfall warnings from the user's request that are relevant to that tranche. Do NOT summarize away critical implementation details.
4. ALWAYS split large or complex tranches into A/B/C sub-tranches. There is no hard cap on tranche count — use as many as needed. If in doubt, split.
5. Keep tranche scope TIGHT: each tranche should implement ONE subsystem or ONE cohesive set of closely-related functions. If a tranche exceeds its active tranche-budget window (1-5: ~175-225 lines, 6-10: ~120-170 lines, 11+: ~80-130 lines), it is too large and MUST be split further.
6. Every tranche must declare: kind, anchorSections, purpose, systemsTouched, filesTouched, visibleResult, safetyChecks, expectedFiles, dependencies, expertAgents, phase, and qualityCriteria.
7. The FIRST tranche must establish scaffold-compliant foundations: preserve immutable scaffold sections, extend existing factories/hooks, create materials/world build, wire shared state safely, and establish STATIC collision surfaces where required.
8. Do NOT instruct the executor to remove immutable scaffold fields/blocks or invent a replacement lifecycle when the scaffold already defines one.
9. If the scaffold already provides a section (camera stage, UI hookup, particle emitter factory, instance parent pattern, input handler shape, etc.), the tranche must explicitly extend that section instead of replacing it. If a subsystem requires a lawful pattern choice (for example physics_driven vs direct_integration, createInstance default vs explicit createObject exception, or sphere-burst vs billboard-trail particles), the tranche prompt must name that family explicitly and prohibit mixing.
10. When the user's request contains code examples (updateInput, syncPlayerSharedMemory, ghost AI, etc.), embed those exact code examples in the relevant tranche prompts — do not paraphrase them.
11. Make tranche count DYNAMIC. The number of tranches must be an output of true dependency order, subsystem complexity, and execution safety — never a preset target. A simple game may need only 7-9 tranches; a larger game may need more. Depth, detail, and density come from the game requirements, not from inflating tranche count. Merge naturally related work when it remains safely within the active line-budget window, and split only when dependency, risk, or execution size requires it.
12. Tranches 1-5: target ~175-225 lines of new or changed code per tranche. Any tranche prompt that describes more than 2 distinct systems, or more than ~3 new functions, is too large and must be split.
13. Tranches 6-10: target ~120-170 lines of new or changed code per tranche. If scope expands beyond one subsystem, split further before execution.
14. LATE-PHASE TIGHTENING (tranches 11 and beyond): As the codebase grows, complexity compounds. For any tranche planned at position 11 or later, cut the line budget to ~80-130 lines of new or changed code, limit scope to ONE function or ONE tightly-coupled pair of functions, and prefer A/B sub-tranche splits over any grouping. If the system being implemented at tranche 11+ would require touching more than one section of models/2, it must be split into sub-tranches.
15. If an Approved Asset Roster is present, you MUST populate gameState.objectids with every roster asset before the five Cherry3D system primitives. Roster assets are mandatory for all non-primitive visual game objects. The five Cherry3D system primitives (cube, cylinder, sphere, plane, planevertical) are reserved for primitive-authored visuals, particle system internals, and invisible collision geometry. If a visual object is intentionally one of those five primitives, the tranche prompt must say so explicitly and MUST skip external scan/roster GEOMETRY CONTRACT, TEXTURE CONTRACT, and SLOT CONTRACT enforcement for that object. For every other rendered visual element, the prompt field MUST explicitly name the approved roster asset to use by its resolved objectids manifest key from the Approved Asset Roster block. Using a Cherry3D primitive as a visible gameplay object when a roster asset covers that role is a planning defect.
16. If the Approved Asset Roster contains particle texture entries (particleEffectTarget set), you MUST plan a Foundation-B sub-tranche immediately after Foundation-A. Foundation-B has two jobs: populate PARTICLE_TEX_PATHS keyed by particleEffectTarget using the exact staged Firebase paths surfaced in the Approved Asset Roster block, and populate gameState.particleTextureIds keyed by particleEffectTarget using the exact assets.json manifest keys. Every tranche that registers particle templates or creates particle billboards / spheres MUST declare Foundation-B as a dependency and MUST name the exact particleEffectTarget keys it uses. A tranche plan that lists particle textures in the roster but never populates PARTICLE_TEX_PATHS for template-time material_file assignment is a planning defect.
17. PARTICLE TEMPLATE APPLICATION RULE: Particle textures are applied differently from non-primitive scene-object colormaps. The tranche that registers particle templates MUST explicitly wire each approved effect texture into registerParticleTemplate(... extraData: { material_file: PARTICLE_TEX_PATHS[effectName] }) or equivalent direct particle-slot material_file assignment. Do NOT route particle textures through defineMaterial/_applyMat unless the scaffold section being modified explicitly does so for particles.
18. GEOMETRY CONTRACT ENFORCEMENT: For every approved roster asset that has a GEOMETRY CONTRACT in the roster block, the tranche whose job is to spawn or position that asset MUST embed the exact numerical values from that contract into its prompt field. Copy floorY, centerOffsetX, centerOffsetZ, scale vector, dominant axis, and any scale warning verbatim. Do NOT paraphrase, estimate, or omit them.
19. TEXTURE CONTRACT ENFORCEMENT: This rule applies ONLY to non-primitive approved roster 3D objects. For every such asset that has a TEXTURE CONTRACT with a non-null colormap path / resolved colormap manifest key, the tranche that creates that object MUST plan to define a registered material whose albedo_texture uses the resolved numeric colormap manifest key from assets.json, then apply that registered material key across every valid slot N from 0 to slotCount-1 (fallback meshCount only if slotCount is unavailable) using gameState._applyMat or equivalent slot-safe scaffold logic. material_file must contain the registered material key, never the raw colormap path. Textured scene objects should default to createInstance with a registered instance parent, but if working-game law proves that per-object visual overrides do not survive instancing for that pool, use createObject consistently instead. Cherry3D system primitives skip this external texture-contract workflow. Using defineMaterial() color alone when a colormap is available is a planning defect.
20. SCALE CORRECTION AWARENESS: If an asset's GEOMETRY CONTRACT includes scaleWarning = "LARGE SCALE CORRECTION NEEDED", the tranche prompt MUST explicitly note this and include the suggestedGameScale as the baseline. The executor must apply this baseline before any game-specific size adjustment.
21. SOURCE PRECEDENCE: When the Approved Game-Specific Asset Roster and the raw THREE.JS MODEL ANALYSIS both mention the same asset, treat the roster block as authoritative. Use the raw model analysis only as supporting reference context; never let it override a roster contract value.
22. SLOT CONTRACT: This rule applies ONLY to non-primitive approved roster 3D objects. Every such asset has a slotCount in its TEXTURE CONTRACT (fallback meshCount only if slotCount is unavailable), and the tranche that applies textures MUST cover EVERY valid slot N from 0 to slotCount-1 via gameState._applyMat or equivalent slot-safe scaffold logic. If explicit per-slot assignment is used, material_file must contain the registered material key for every valid slot. Applying only slot 0 when slotCount > 1 is a crash-inducing defect. Applying to a slot index >= slotCount crashes the engine. Cherry3D system primitives skip this slotCount workflow entirely. The slotCount value is a hard constraint, not a suggestion. Embed the exact slotCount value in the tranche prompt so the executor knows the precise loop bounds.
23. HTML UI PLACEMENT RULE: For any tranche that creates or modifies visible HTML UI / HUD / overlay elements in models/23 or localUI, NEVER place UI in the top-left or top-right corners of the screen. Prefer top-center, bottom-center, or clearly inset side placements instead. Any UI that sits near the left or right edge MUST be pulled inward toward the center with visible padding / inset margin rather than hugging the screen edge. Corner-hugging or edge-hugging HUD placement is a planning defect.
24. CHILD RIGIDBODY LOCAL-SPACE POSITION (Non-Negotiable 13): When a RigidBody is attached as a child of a visual object, rbPosition MUST stay [0,0,0] unless a deliberate local offset is truly intended. World-space coordinates passed into a child rbPosition cause POSITION DOUBLING. Every tranche that attaches a RigidBody as a child must carry this constraint explicitly in its prompt.
25. DYNAMIC VISUAL AUTO-SYNC (Non-Negotiable 16): DYNAMIC visuals do NOT auto-sync with their rigidbody in all cases. If a tranche creates DYNAMIC bodies and the visual must track them, the tranche prompt MUST explicitly require either (a) getMotionState() position mirroring every frame in Stage 3, or (b) the scaffold helper syncDynamicVisualFromRigidBody(visualObj, rigidbodyObj). Assuming auto-sync is a planning defect.
26. TILE-CENTERING AXIS LAW (Non-Negotiable 19): Any snap / tile-centering correction may ONLY adjust the non-movement (perpendicular) axis. For a game moving forward along Z, only X may be snapped. For a game moving along X, only Z may be snapped. Any tranche implementing lane-snap, tile-center, or perpendicular correction MUST embed this constraint and use the scaffold helper computePerpendicularCorrection(movementAxis, currentPos, targetPos, gain). Correcting the movement axis itself is a hard defect.
27. ENGINE AUTO-ROTATION FROM VELOCITY (Rule 35): The Cherry3D engine automatically infers and applies a visual rotation to ANY object whose position changes between frames. Any object that must hold a fixed orientation during flight (projectiles, thrown objects, orbiters) must set obj.rotate = [0,0,0] EVERY frame inside its flight-update function — setting it once at spawn is not sufficient. Any tranche spawning or moving such objects MUST embed this requirement explicitly.
28. KINEMATIC DUAL UPDATE (Non-Negotiable 15): KINEMATIC actors moved manually require a dual update — both the visual obj.position AND the collider setPosition must be updated together. The scaffold provides setKinematicDualPose(visualObj, rigidbodyObj, position) for this. Any tranche moving a KINEMATIC actor must explicitly plan the dual update.
29. STATIC COLLISION FOR FLOORS (Non-Negotiable 14): Every floor, track, or ground surface that must block a DYNAMIC actor requires a STATIC rigidbody. A visual-only mesh provides zero collision resistance. Any tranche building ground geometry must explicitly plan a STATIC rigidbody for each blocking surface.

30. PARTICLE TEMPLATE TEARDOWN (Non-Negotiable 20): onDestroy MUST remove every particle template via the gameState.particleTemplates registry loop — never via a hand-written key list. Every template registered through registerParticleTemplate() is automatically tracked and covered by that loop. Any tranche that registers game-specific particle templates (beyond the two scaffold defaults) must declare teardown coverage in its safetyChecks. A tranche that modifies onDestroy must not introduce or preserve a hand-written particle key list — that pattern is FORBIDDEN.

31. SHARED ASSET POOL UNIFICATION (Non-Negotiable 21): If two pool types use the same asset ID and the same instance parent, they MUST be declared as one ScenePool with a single maxInstances cap, aliased to both variable names. Declaring two separate ScenePool instances for one shared WASM asset is FORBIDDEN. canAllocate() MUST be checked before every new addObject call for any capped pool — if canAllocate() returns false, skip allocation entirely rather than calling addObject. In onDestroy (both normal and page-unload paths), every ScenePool MUST call pool.reset() — NOT pool.purge(). purge() parks objects by writing obj.position to handles that are freed after WASM teardown → OOB. reset() is a JS-only handle drop and is always safe in onDestroy.

32. ASSET READINESS AND BURST EMITTER DEFERRAL (Non-Negotiable 22): All burst emitter creation MUST be placed inside a named _createBurstEmitters() function. _createBurstEmitters() is called immediately after the registration retry flush if no particle template retries were queued, or deferred as a queueBuildStep if retries were needed — ensuring particlesettings.object is never null when a burst emitter is created. Any tranche that creates burst emitters must plan them inside _createBurstEmitters() and must not call createParticleEmitter() for burst emitters before the retry flush is confirmed complete. Direct Module.ProjectManager.addObject calls for instance parents or particle templates outside of registerInstanceParent / registerParticleTemplate are FORBIDDEN.

${buildMasterPromptLayoutGuidance(effectivePrompt)}

${REQUIRED_TRANCHE_VALIDATION_BLOCK}

You must respond ONLY with a valid JSON object. No markdown, no code fences, no preamble.

{
  "analysis": "Brief planning analysis describing how you decomposed the build and why.",
  "tranches": [
    {
      "kind": "build",
      "name": "Short Name",
      "description": "2-3 sentence description of what this tranche accomplishes.",
      "anchorSections": ["3.1", "4.1"],
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
        { type: "text", text: `${approvedRosterBlock}${fileContext}

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
      plan = injectDeterministicContractsIntoPlan(plan, approvedRosterBlock);

      // Update progress with plan
      progress.status = "executing";
      progress.planningEndTime = Date.now();
      progress.planningAnalysis = plan.analysis || "";
      progress.totalTranches = plan.tranches.length;
      progress.currentTranche = 0;
      progress.tranches = plan.tranches.map((t, i) => ({
        index: i,
        kind: t.kind || 'build',
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
        retryBudget: 0,
        originalPrompt: t.originalPrompt || t.prompt,
        contractCarryThroughInjected: Boolean(t.contractCarryThroughInjected),
        contractCarryThroughAssets: t.contractCarryThroughAssets || [],
        contractPromptReviewWarnings: [],
        contractPromptReviewStatus: "not_applicable",
        contractCodeReviewWarnings: [],
        contractCodeReviewStatus: "not_applicable",
        contractCodeReviewAssets: []
      }));
      progress.contractPromptReview = buildContractPromptReview(progress, approvedRosterBlock);
      progress.contractCodeReview = summarizeContractCodeReview(progress);
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
        approvedRosterBlock,   // ← propagated to every tranche execution
        contractPromptReview: progress.contractPromptReview,
        contractCodeReview: progress.contractCodeReview
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
- The Cherry3D Scaffold is the binding codified law extracted from shipped working games. Treat it as the required base architecture.
- The SDK / Engine Notes are subordinate. Use them only when the scaffold is silent and engine/API certainty is needed.
- If both apply, the Scaffold wins for architecture, lifecycle, state shape, movement authority, UI ownership, materials/textures, particles, and scene mutation rails.
- Never elevate the SDK into a parallel authority. It fills certainty gaps only where the scaffold does not already settle the issue.
- Never delete, replace, or work around an immutable scaffold section. Extend inside it.
- Pick one lawful pattern family per subsystem and preserve it through the tranche. Do not silently switch families mid-implementation.
- REFERENCE IMAGES (if attached): Any images attached to this tranche are first-class game design inputs with authority equal to the Master Prompt. They define the intended visual appearance, entity types, layout geometry, and interaction model. When implementing this tranche, reconcile your output against the attached images — if your code would produce something visually inconsistent with an attached image, that is a defect. Visual Reconciliation is a required quality criterion for every tranche that touches rendered content.

Do not re-state the instruction docs — just apply them. Write it correctly the first time so the tranche can move forward without rework.

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
- 3D OBJECT ENFORCEMENT: If an Approved Asset Roster is present and contains objects3d entries, every visible gameplay object introduced or modified in this tranche MUST branch cleanly between the five Cherry3D system primitives (cube, cylinder, sphere, plane, planevertical) and non-primitive approved roster assets. If the object is intentionally one of those five primitives, state that explicitly in code comments and skip external scan/roster geometry-texture-meshCount enforcement for that object. For every other visible gameplay object, you MUST use a roster asset via gameState.objectids and the resolved assets.json manifest keys surfaced in the roster block. Using a Cherry3D primitive as a visible gameplay object when a roster asset covers that role is a defect. Those primitives may otherwise only be used for primitive-authored visuals, particle internals, and invisible collision geometry.
- PARTICLE TEXTURE ENFORCEMENT: Approved particle textures follow the scaffold particle-template path, not the non-primitive scene-object material-registry path. If this tranche IS Foundation-B, your first job is to populate BOTH PARTICLE_TEX_PATHS[effectName] = '<staged Firebase path>' and gameState.particleTextureIds[effectName] = '<manifest key>' for every approved particleEffectTarget from the roster block. In any later tranche that registers particle templates or creates particle billboards / spheres, you MUST assign the texture at the template/object slot itself via registerParticleTemplate(... extraData: { material_file: PARTICLE_TEX_PATHS[effectName] }) or an equivalent direct particle data['0'].material_file assignment. Declaring gameState.particleTextureIds without wiring material_file onto the particle template/object is a defect.
- PLACEMENT MATH AUDIT TRAIL: When placing any roster asset that has a GEOMETRY CONTRACT, include a comment block immediately above the position and scale assignments in the emitted code using the contract values, e.g. // [assetName] placement contract applied: floorY=[v] origin=[class] scale=[s,s,s]. Its absence is a detectable defect.
- TEXTURE ASSIGNMENT AUDIT TRAIL: This applies ONLY to non-primitive approved roster 3D objects. When creating any such asset that has a TEXTURE CONTRACT with a non-null colormap path / resolved colormap manifest key, include a comment immediately above the material/setup block noting the applied colormap key and meshCount, define a registered material whose albedo_texture uses that numeric manifest key, and apply that registered material key across every valid slot using gameState._applyMat or equivalent slot-safe scaffold logic. material_file must contain the registered material key, never the raw staged path. Cherry3D system primitives skip this external texture-contract audit trail.
- MESH COUNT CONTRACT (CRASH PREVENTION): This applies ONLY to non-primitive approved roster 3D objects. Every such roster asset carries a meshCount in its TEXTURE CONTRACT. You MUST cover EVERY valid slot N from 0 to meshCount-1 via gameState._applyMat or equivalent slot-safe scaffold logic. If explicit per-slot assignment is used, material_file must carry the registered material key for every valid slot. Assigning only data['0'] when meshCount > 1 leaves untextured mesh slots and CRASHES the engine. Assigning to a slot index >= meshCount also CRASHES the engine. Cherry3D system primitives skip this meshCount workflow entirely. Use a loop or explicit per-slot assignments — never assume a single-slot assignment covers a multi-mesh object. The meshCount value is provided verbatim in the DETERMINISTIC ROSTER CONTRACT CARRY-THROUGH block for this tranche; treat it as a hard loop bound.
- HTML UI PLACEMENT RULE: When this tranche creates or updates visible HTML UI / HUD / overlay elements in models/23 or localUI, NEVER place any UI element in the top-left or top-right corner of the screen. It is always better to move UI slightly inward toward the screen center rather than hugging the left or right edges. Prefer top-center, bottom-center, or clearly inset side placements. Any element near the left or right edge must use an intentional inset margin so it reads as center-biased, not edge-anchored. Top-left / top-right corner placement and hard edge-hugging placement are defects.
- CHILD RIGIDBODY LOCAL-SPACE POSITION (Non-Negotiable 13): When attaching a RigidBody as a child of a visual object, rbPosition MUST stay [0,0,0] unless a deliberate local offset is truly intended. The engine resolves world position through the parent visual transform. Passing world-space coordinates into a child rbPosition causes POSITION DOUBLING — the object appears twice as far from the origin as expected. This is a hard defect with no runtime warning.
- DYNAMIC VISUAL AUTO-SYNC (Non-Negotiable 16): DYNAMIC visuals do NOT always auto-sync with their rigidbody. If this tranche moves a DYNAMIC actor, the visual MUST be explicitly mirrored every frame in Stage 3 of onRender via getMotionState() position readback or the scaffold helper syncDynamicVisualFromRigidBody(visualObj, rigidbodyObj). Assuming the engine will auto-sync the visual is a defect. If the visual ever drifts or freezes while the physics body moves, add the explicit mirror.
- TILE-CENTERING AXIS LAW (Non-Negotiable 19): Any snap / tile-centering / perpendicular correction may ONLY adjust the non-movement axis. If the player moves along Z, only X may be corrected. If the player moves along X, only Z may be corrected. Correcting the movement axis itself stalls the player. Use the scaffold helper computePerpendicularCorrection(movementAxis, currentPos, targetPos, gain) — it enforces this law internally. Never write a manual snap that touches the movement axis.
- ENGINE AUTO-ROTATION FROM VELOCITY (Rule 35): The Cherry3D engine automatically applies visual rotation to ANY object whose position changes between frames. Any object that must hold a fixed orientation during flight (projectiles, thrown objects, balls, knives) MUST set obj.rotate = [0,0,0] EVERY frame inside its flight-update function. Setting rotation once at spawn is not sufficient — the engine overwrites it each frame. For objects orbiting a rotating parent, compute worldAngle = localAngle + parentAngle each frame and set obj.rotate accordingly.
- STATIC COLLISION FOR FLOORS (Non-Negotiable 14): Every floor, track, or ground surface that must block a DYNAMIC actor requires a STATIC rigidbody. Visual-only geometry provides zero collision resistance. Use createStaticGroundBody() or createRigidBody() with motionType='STATIC'.
- KINEMATIC DUAL UPDATE (Non-Negotiable 15): KINEMATIC actors moved manually require a dual update — visual obj.position AND collider setPosition must be updated together every move. Use the scaffold helper setKinematicDualPose(visualObj, rigidbodyObj, position). Updating only one side leaves the collider desynced from the visual.
- PARTICLE TEMPLATE TEARDOWN (Non-Negotiable 20): If this tranche modifies onDestroy, it MUST tear down particle templates via the gameState.particleTemplates registry loop — never via a hand-written key list. The loop covers every key registered through registerParticleTemplate() automatically. Adding or preserving a hard-coded particle key list in onDestroy is a FORBIDDEN pattern.
- SHARED ASSET POOL UNIFICATION (Non-Negotiable 21): Two ScenePools sharing the same asset ID and instance parent MUST be declared as one ScenePool with a single maxInstances cap, aliased to both variable names. In onDestroy (both normal and page-unload paths), call pool.reset() on every ScenePool — NOT pool.purge(). purge() writes obj.position to WASM handles that are freed after teardown → OOB. reset() is JS-only and always safe. canAllocate() MUST be checked before every new addObject call for any capped pool; if false, skip the allocation entirely.
- BURST EMITTER DEFERRAL (Non-Negotiable 22): All burst emitter creation MUST live inside a named _createBurstEmitters() function. Call it immediately after the registration retry flush when no particle template retries were queued, or defer it via queueBuildStep when retries were needed. Never call createParticleEmitter() for burst emitters inline before the retry flush completes — particlesettings.object will be null if the template registration failed. Direct Module.ProjectManager.addObject calls for instance parents or particle templates outside of registerInstanceParent / registerParticleTemplate are FORBIDDEN.

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

      if (progress.contractPromptReview?.issueCount > 0 && Array.isArray(progress.tranches?.[nextTranche]?.contractPromptReviewWarnings) && progress.tranches[nextTranche].contractPromptReviewWarnings.length > 0) {
        console.warn(`[CONTRACT REVIEW][informational] Tranche ${nextTranche + 1}: ${progress.tranches[nextTranche].contractPromptReviewWarnings.join(" || ")}`);
      }

      const rosterPrefix = approvedRosterBlock
        ? `=== APPROVED GAME-SPECIFIC ASSET ROSTER ===\n${approvedRosterBlock}\n=== END ASSET ROSTER ===\n\n`
        : "";

      const trancheUserText = `${rosterPrefix}${trancheFileContext}

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
          maxTokens: 100000,
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

          const contractCodeReview = buildContractCodeReviewForTranche(progress.tranches[nextTranche], trancheResult.updatedFiles, approvedRosterBlock);
          progress.tranches[nextTranche].contractCodeReviewWarnings = contractCodeReview.warnings;
          progress.tranches[nextTranche].contractCodeReviewStatus = contractCodeReview.status;
          progress.tranches[nextTranche].contractCodeReviewAssets = contractCodeReview.assets;
          progress.contractCodeReview = summarizeContractCodeReview(progress);

          if (contractCodeReview.warnings.length > 0) {
            console.warn(`[CONTRACT CODE REVIEW][informational] Tranche ${nextTranche + 1}: ${contractCodeReview.warnings.join(" || ")}`);
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
      state.contractCodeReview = progress.contractCodeReview;
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