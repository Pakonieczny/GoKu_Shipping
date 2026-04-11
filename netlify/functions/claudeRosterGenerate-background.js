/* netlify/functions/claudeRosterGenerate-background.js */
/* ═══════════════════════════════════════════════════════════════════
   GAME-SPECIFIC ASSET ROSTER GENERATION — v6.0 (Phase 1 Only)
   ─────────────────────────────────────────────────────────────────
   Background Netlify function (suffix -background = 15-min timeout).
   Returns 202 immediately. Writes result to Firebase when done.
   Frontend polls ai_asset_roster_phase1.json to detect completion.

   Flow:
     1. Read global CSV from game-generator-1/projects/BASE_Files/asset_3d_objects/
        reorganized_assets_manifest.csv to extract the live category list.
     2. Read Master Prompt + inline images from ai_request.json.
     3. PHASE 1 — Claude analyzes the game prompt + gameplay reference
        images and produces a structured list of required particle effects
        and 3D objects. Each 3D object requirement includes 2 to 6
        rankedCategories chosen from the live CSV category list, sorted
        by likelihoodPercent from highest to lowest.
     4. Save the Phase 1 payload as ai_asset_roster_phase1.json.
     5. Frontend collects one user reference image per required 3D object.
     6. claudeRosterStageAB-background reads the CSV again, filters assets
        by rankedCategories / suggestedCategories, then runs Stage A/B
        on the filtered pool using the top valid categories by score.

   Global asset paths (shared across all projects):
     CSV:  game-generator-1/projects/BASE_Files/asset_3d_objects/reorganized_assets_manifest.csv
     Zips: game-generator-1/projects/BASE_Files/asset_3d_objects/{asset_name}.zip

   Request body: { projectPath, jobId }
   Response:     202 Accepted (background function — no body)
   ═══════════════════════════════════════════════════════════════════ */

const fetch = require("node-fetch");
const admin = require("./firebaseAdmin");

const GLOBAL_ASSET_CSV_PATH = "game-generator-1/projects/BASE_Files/asset_3d_objects/reorganized_assets_manifest.csv";
const MIN_SUGGESTED_CATS  = 2;
const MAX_SUGGESTED_CATS  = 6;
const AVATARS_ZIP_PATH_DEFAULT = "game-generator-1/projects/BASE_Files/asset_3d_objects/Avatars.zip";

/* ─── Retry helpers ──────────────────────────────────────────────── */
function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

function computeRetryDelay(attempt) {
  const BASE = 1250, MAX = 12000;
  return Math.min(BASE * Math.pow(2, Math.max(0, attempt - 1)), MAX) + Math.floor(Math.random() * 700);
}

function isOverload(status, msg = "") {
  const m = String(msg).toLowerCase();
  if ([429, 500, 502, 503, 504, 529].includes(Number(status))) return true;
  if (
    m.includes("econnreset")     ||
    m.includes("econnrefused")   ||
    m.includes("etimedout")      ||
    m.includes("enotfound")      ||
    m.includes("socket hang up") ||
    m.includes("network error")  ||
    m.includes("fetch failed")
  ) return true;
  return m.includes("overloaded")        ||
         m.includes("rate limit")        ||
         m.includes("too many requests") ||
         m.includes("capacity")          ||
         m.includes("temporarily unavailable");
}

async function callClaude(apiKey, { model, maxTokens, system, userContent }) {
  const MAX_RETRIES = 5;
  const body = {
    model,
    max_tokens: maxTokens,
    system,
    messages: [{ role: "user", content: userContent }]
  };
  let last;
  for (let i = 1; i <= MAX_RETRIES; i++) {
    try {
      const res = await fetch("https://api.anthropic.com/v1/messages", {
        method:  "POST",
        headers: {
          "Content-Type":      "application/json",
          "x-api-key":         apiKey,
          "anthropic-version": "2023-06-01"
        },
        body: JSON.stringify(body)
      });
      const raw  = await res.text();
      const data = raw ? JSON.parse(raw) : null;
      if (!res.ok) {
        const msg = data?.error?.message || `Claude error (${res.status})`;
        const err = Object.assign(new Error(msg), {
          status: res.status,
          isRetryableOverload: isOverload(res.status, msg)
        });
        throw err;
      }
      const text = data?.content?.find(b => b.type === "text")?.text;
      if (!text) throw new Error("Empty response from Claude");
      return { text, usage: data?.usage || null };
    } catch (err) {
      last = err;
      if (!err.isRetryableOverload && !isOverload(err.status, err.message)) throw err;
      if (i >= MAX_RETRIES) throw err;
      await sleep(computeRetryDelay(i));
    }
  }
  throw last;
}

/* ─── CSV parsing ──────────────────────────────────────────────── */
function parseCsvRows(csvText) {
  const rows = [];
  let row = [];
  let field = '';
  let i = 0;
  let inQuotes = false;

  while (i < csvText.length) {
    const ch = csvText[i];

    if (inQuotes) {
      if (ch === '"') {
        if (csvText[i + 1] === '"') {
          field += '"';
          i += 2;
          continue;
        }
        inQuotes = false;
        i += 1;
        continue;
      }
      field += ch;
      i += 1;
      continue;
    }

    if (ch === '"') {
      inQuotes = true;
      i += 1;
      continue;
    }
    if (ch === ',') {
      row.push(field);
      field = '';
      i += 1;
      continue;
    }
    if (ch === '\n') {
      row.push(field);
      rows.push(row);
      row = [];
      field = '';
      i += 1;
      continue;
    }
    if (ch === '\r') {
      i += 1;
      continue;
    }

    field += ch;
    i += 1;
  }

  if (field.length > 0 || row.length > 0) {
    row.push(field);
    rows.push(row);
  }

  return rows.filter(r => r.some(cell => String(cell || '').trim() !== ''));
}

/* ─── Parse CSV text → unique sorted category list ──────────────── */
function parseCategoriesFromCsv(csvText) {
  const rows = parseCsvRows(csvText);
  if (rows.length === 0) throw new Error('CSV is empty');

  const header = rows[0].map(h => h.trim().toLowerCase());
  const catIdx = header.indexOf('new_category');
  if (catIdx === -1) throw new Error("CSV missing 'new_category' column");

  const categories = new Set();
  for (let i = 1; i < rows.length; i++) {
    const cat = (rows[i][catIdx] || '').trim();
    if (cat) categories.add(cat);
  }
  return [...categories].sort();
}

/* ─── Utilities ──────────────────────────────────────────────────── */
function stripFences(text) {
  let t = text
    .replace(/^```json\s*/i, "").replace(/^```\s*/i, "").replace(/\s*```$/i, "").trim();
  const a = t.indexOf("{"), b = t.lastIndexOf("}");
  if (a > 0 && b > a) t = t.substring(a, b + 1);
  return t.trim();
}

function clampLikelihoodPercent(value, fallback = 50) {
  const n = Number(value);
  if (!Number.isFinite(n)) return Math.max(0, Math.min(100, Math.round(fallback)));
  return Math.max(0, Math.min(100, Math.round(n)));
}

function buildLegacyRankedCategories(rawCategories) {
  if (!Array.isArray(rawCategories)) return [];
  return rawCategories.map((category, index) => ({
    category: String(category || "").trim(),
    likelihoodPercent: Math.max(1, 100 - (index * 5))
  }));
}

function normalizeSuggestedCategoryRanking(obj = {}) {
  const rawRanked = Array.isArray(obj.rankedCategories) ? obj.rankedCategories : [];
  const rankedSource = rawRanked.length > 0 ? rawRanked : buildLegacyRankedCategories(obj.suggestedCategories);
  const normalized = [];
  const seen = new Set();

  for (let index = 0; index < rankedSource.length; index++) {
    const entry = rankedSource[index];
    const category = String(
      typeof entry === "string"
        ? entry
        : (entry?.category || entry?.name || "")
    ).trim();
    if (!category) continue;

    const dedupeKey = category.toLowerCase();
    if (seen.has(dedupeKey)) continue;
    seen.add(dedupeKey);

    normalized.push({
      category,
      likelihoodPercent: clampLikelihoodPercent(
        typeof entry === "string" ? 100 - (index * 5) : entry?.likelihoodPercent,
        100 - (index * 5)
      )
    });
  }

  normalized.sort((a, b) => {
    if (b.likelihoodPercent !== a.likelihoodPercent) return b.likelihoodPercent - a.likelihoodPercent;
    return a.category.localeCompare(b.category);
  });

  const rankedCategories = normalized.slice(0, MAX_SUGGESTED_CATS);
  return {
    rankedCategories,
    suggestedCategories: rankedCategories.map(entry => entry.category)
  };
}


function buildMasterPromptLayoutGuidance(masterPrompt = "") {
  const prompt = String(masterPrompt || "");
  const hasNewStructuredLayout =
    /#\s*1\.\s*SESSION DECISIONS/i.test(prompt) &&
    /#\s*2\.\s*GAME IDENTITY/i.test(prompt) &&
    /#\s*3\.\s*IMPLEMENTATION CONTRACT/i.test(prompt);
  const hasLegacy63Layout = /\b6\.3(\.\d+)?\b/.test(prompt);

  if (hasNewStructuredLayout) {
    return `MASTER PROMPT LAYOUT DETECTED:
- Section 3.x = implementation contract for movement, camera, init, UI, lifecycle, and exact variable constraints.
- Section 4.x = mechanics/rules, object inventory, and VFX requirements.
- Section 5 = runtime registry with exact object/material/particle names and counts.
- Section 6 = author tranche plan. Useful context, but do not infer extra required assets from sequencing text alone.
- Section 7 = validation contract. Use it to confirm must-exist visible systems.
- For asset discovery, Sections 3, 4, 5, and 7 outweigh descriptive fluff.`;
  }

  if (hasLegacy63Layout) {
    return `MASTER PROMPT LAYOUT DETECTED:
- Legacy 6.3-style structure is present.
- Read the actual gameplay, object, VFX, and validation sections directly from that layout.`;
  }

  return `MASTER PROMPT LAYOUT DETECTED:
- No canonical layout markers were found.
- Infer the authoritative sections from the prompt's real headings and use them for asset discovery.`;
}


function detectRoadPipelineSettings(masterPrompt = "", existing = null) {
  const lower = String(masterPrompt || "").toLowerCase();
  const hasAny = (...patterns) => patterns.some(pattern => pattern.test(lower));

  const existingValue = (existing && typeof existing === "object") ? existing : {};

  let gameType = "other";
  if (hasAny(
    /\b(racing|race car|drift|time trial|lap timer|checkpoint racing)\b/,
    /\b(track|circuit|racetrack|raceway|road course)\b/,
    /\b(laps|finish line|starting grid|pit lane)\b/
  )) {
    gameType = "racing";
  } else if (hasAny(
    /\b(side[ -]?scroller|side[ -]?scrolling|side view|side-view|runner)\b/,
    /\b(vehicle|truck|car|bike|buggy|motorcycle|tank)\b/,
    /\b(terrain|ground traversal|hill climb|slope|road strip|terrain strip)\b/
  )) {
    gameType = "sidescroller_terrain";
  } else if (hasAny(
    /\b(platformer|platforming|run and jump|jump between platforms)\b/,
    /\b(ground traversal|terrain|ground piece|platform route|ramp)\b/
  )) {
    gameType = "platformer";
  }

  const roadExclusionFlag = gameType !== "other" || hasAny(
    /\b(road section|track segment|terrain strip|ground piece|pre-built ground|prebuilt ground)\b/,
    /\b(track layout|terrain layout|road pipeline|road\.zip)\b/
  );

  if (existingValue.roadPipelineUserOverride === true) {
    return {
      ...existingValue,
      gameType,
      source: "user_override_preserved_v1"
    };
  }

  return {
    ...existingValue,
    gameType,
    roadExclusionFlag,
    source: "prompt_heuristic_v4"
  };
}

function buildVariantGroupWarnings(objects3d = []) {
  const groups = new Map();
  for (const obj of (objects3d || [])) {
    const group = String(obj?.variantGroup || "").trim().toLowerCase();
    if (!group || group === "unique") continue;
    if (!groups.has(group)) groups.set(group, 0);
    groups.set(group, groups.get(group) + 1);
  }

  return Array.from(groups.entries())
    .filter(([, count]) => count === 1)
    .map(([group]) => group)
    .sort();
}

function buildPhase1Prompt(masterPrompt, categoryList, roadPipeline = {}) {
  const catBlock = categoryList.map(c => `  - ${c}`).join("\n");
  const roadClause = roadPipeline?.roadExclusionFlag
    ? `
ROAD-FIRST TERRAIN CLAUSE (Road.zip pipeline is active)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Road.zip is the PRIMARY source of road layout, drivable path geometry, track sections, ramps,
turns, bumps, and any authored road-piece assembly. Cherry3D primitives are ALSO allowed and
expected, but only as COMPLIMENTARY terrain fill around the assembled road layout.

DO NOT include road sections, terrain strips, ground pieces, track segments, roadside filler
planes, cliff filler blocks, embankment shells, or any equivalent structural ground asset in
your objects3d list. Those are handled by the road + primitive terrain pipeline, not by the
asset roster.

When Road.zip is active, the system will:
  - assemble the actual road using Road.zip pieces as the authoritative primary building blocks
  - add Cherry3D primitive terrain beside, under, and around those road pieces to complete the
    remaining gameplay terrain
  - use only hidden .primitives keys 4-14 for that complimentary terrain work
  - never use deprecated model primitive keys 17, 18, 21, 34, or 35
  - keep primitive terrain adjacent to and connected with the placed road pieces rather than
    floating separately or replacing the road itself

The following ARE still sourced as objects3d from the asset library (request these as normal):
  - Props placed ON TOP of the terrain: trees, bushes, rocks, boulders, grass tufts
  - Vegetation: any plant, shrub, foliage scatter prop
  - Structural props that sit in the scene but are not the ground surface itself:
    buildings, ruins, walls, fences, crates, barrels, vehicles, lamps, signs
  - Collectibles and non-character hazards
  - Any decorative or gameplay prop that has distinct shape and is not the terrain shell

In short: Road.zip builds the road. Cherry3D primitives stitch in the surrounding terrain.
The props sitting on that finished terrain shell come from the asset library as normal.
`
    : `
PRIMITIVE TERRAIN CLAUSE (Road.zip pipeline is NOT active for this game)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
All terrain structure — meaning the ground floor, terrain floor tiles, mountain body geometry,
cliff face geometry, hill shapes, sloped ground planes, raised platforms, and any other
structural ground-volume pieces — MUST be built exclusively from Cherry3D system primitives
(cube, square, plane, sphere, cylinder, capsule, cone, torus, torusknot, tetrahedron, icosahedron).
Those primitive-authored terrain pieces must resolve only through the hidden .primitives manifest
keys 4-14; deprecated model primitive keys 17, 18, 21, 34, and 35 must never be requested.
These geometry types are already available in the engine and require no external assets.

DO NOT request terrain floor pieces, ground tiles, terrain strips, mountain meshes, cliff
body OBJs, hill geometry, slope meshes, or any equivalent structural ground asset as objects3d
entries. They will not be sourced from the asset library. The terrain build tranche will
construct them procedurally using the Cherry3D primitives.

The following ARE still sourced as objects3d from the asset library (request these as normal):
  - Props placed ON TOP of the terrain: trees, bushes, rocks, boulders, grass tufts
  - Vegetation: any plant, shrub, foliage scatter prop
  - Structural props that sit in the scene but are not the ground surface itself:
    buildings, ruins, walls, fences, crates, barrels, vehicles, lamps, signs
  - Collectibles and non-character hazards
  - Any decorative or gameplay prop that has distinct shape and is not the terrain floor

In short: the terrain SHELL (floor, slopes, volumes) is primitives-only.
The props SITTING ON the terrain shell come from the asset library as normal.
`;

  return `You are a game visual requirements analyst. Your ONLY job in this phase is to study the game description and produce a structured list of every particle effect, prop/scene 3D object, and avatar/character requirement the game needs.

DO NOT reference any asset files, filenames, or packs. You have not seen them yet.
DO NOT include surface textures or road/terrain sections (these are handled by separate pipelines).
Character-role requirements MUST be emitted only in avatarRequirements, never in objects3d.
AvatarRequirements are for any visible controllable or animated character role such as player avatar, enemy, NPC, boss, companion, crowd human, creature, or animal performer.
Objects3d are only for non-character props, scenery, hazards, pickups, vehicles used as props, and environment pieces.
When the prompt contains a structured contract layout, prefer extracting requirements from the implementation contract, mechanics/object inventory, registry, and validation sections rather than from tranche sequencing text.
For visible gameplay objects, prefer authored non-primitive scene objects when the game calls for rich silhouettes or recognizable props; primitives should only be implied when the game truly wants primitive-authored visuals, particle internals, or invisible collision geometry.
Be specific and visual in your descriptions — describe what each thing looks like, its size relative to the scene, its motion characteristics, and the gameplay moment it appears in.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
UI EXCLUSION RULE — HARD PROHIBITION
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
The following are NEVER 3D objects. Do not include any of them in objects3d under any circumstances, regardless of how the game prompt describes them:

BANNED — character roles in objects3d (these belong in avatarRequirements):
  - Player avatar, hero, driver, rider, pilot, gunner
  - Enemies, NPCs, bosses, companions, pets, crowd performers
  - Any visible animated character, creature, humanoid, animal, or monster that acts like an actor rather than a prop


BANNED — 2D interface elements (all handled by file 23 HTML pipeline):
  - Health bars, HP bars, life bars, stamina bars
  - Power bars, energy meters, fuel gauges, charge indicators
  - Ammo counters, bullet displays, reload indicators
  - Score displays, point counters, combo meters
  - Timer displays, countdown clocks
  - Minimap, radar, compass overlays
  - Pause menus, settings screens, main menus
  - Start screens, game over screens, victory screens
  - Button graphics, icon overlays, cursor graphics
  - Stage clear panels, reward modals, shop interfaces
  - Tutorial overlays, control hint displays
  - Any flat panel, quad, or plane used purely as a 2D display surface
  - Any object whose primary function is to show text, numbers, or 2D art to the player

If the master prompt describes any of the above as visual elements, treat them as UI pipeline items and exclude them silently. Do not mention them in your output.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
ENVIRONMENTAL VARIETY MANDATE
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
A game environment built from one tree, one rock, and one bush is visually flat and unconvincing. You MUST request multiple variants of each prop class that the game environment requires.

Apply these minimum counts whenever the game environment calls for that prop class:

VEGETATION:
  Trees → minimum 3 variants.
  Bushes / shrubs → minimum 2 variants.
  Ground cover / grass tufts → minimum 2 variants if used as scatter props.

ROCKS / GEOLOGICAL:
  Rocks / boulders → minimum 3 variants.
  Cliff faces / rock formations → minimum 2 variants if the game has cliff scenery.

STRUCTURES / ARCHITECTURE:
  Buildings → minimum 2 variants.
  Walls / fences / barriers → minimum 2 variants.
  Ruins / debris pieces → minimum 3 variants.

VEHICLES (background / destructible):
  Any vehicle class used as scenery or obstacle → minimum 2 variants.

CONTAINERS / CRATES:
  Any crate, barrel, or container class → minimum 2 variants.

STREET / ENVIRONMENT FURNITURE:
  Lamps, signs, benches, bins, etc. → minimum 2 variants per furniture class.

ENEMIES / CHARACTERS:
  Each distinct enemy type counts as one variant — do not artificially inflate enemy counts.

EXCEPTION: Unique hero/player characters, boss objects, and named single gameplay props are exempt from minimum counts.
EXCEPTION: If the game's visual style is deliberately minimalist, reduce minimum counts to 2 per class instead of 3.

${roadClause}
${buildMasterPromptLayoutGuidance(masterPrompt)}

MASTER GAME PROMPT:
${masterPrompt}

AVAILABLE 3D ASSET CATEGORIES:
The asset library is organised into the following categories. For each 3D object requirement you identify, you MUST return rankedCategories as an array of 2 to 6 category guesses from this exact list, sorted from highest to lowest likelihoodPercent.
- Always include at least the 2 most probable categories.
- Lean on the side of caution and include additional plausible categories rather than fewer when the object could realistically appear in adjacent or overlapping folders.
- Prefer 4 to 6 ranked categories when there are multiple credible locations.
- likelihoodPercent must be an integer from 0 to 100 and should reflect relative likelihood for this object requirement. It is used for ranking and filtering, so the highest-confidence categories must come first.
- Do not invent categories outside this list.

${catBlock}

Respond ONLY with a valid JSON object. No markdown, no fences, no preamble.

{
  "gameInterpretationSummary": "2-3 sentence description of the game type, environment, visual style, and key visual requirements.",
  "particleEffects": [
    {
      "name": "short_snake_case_identifier",
      "visualDescription": "What this effect looks like visually — shape, density, spread, color tone, scale",
      "behaviorDescription": "How it moves and behaves — duration, velocity, burst vs continuous",
      "triggerMoment": "When in gameplay this effect fires"
    }
  ],
  "objects3d": [
    {
      "name": "short_snake_case_identifier",
      "visualDescription": "What this prop looks like — shape, silhouette, style, approximate scale",
      "gameplayRole": "What this prop does in the game — obstacle, collectible, environment piece, hazard, etc.",
      "variantGroup": "The prop class this belongs to, e.g. 'tree', 'rock', 'building', 'vehicle', 'unique'",
      "rankedCategories": [
        { "category": "Primary_Category/Sub_Category", "likelihoodPercent": 94 },
        { "category": "Secondary_Category/Sub_Category", "likelihoodPercent": 83 }
      ]
    }
  ],
  "avatarRequirements": [
    {
      "name": "short_snake_case_identifier",
      "visualDescription": "Silhouette, species/type, outfit/armor, approximate scale",
      "gameplayRole": "player_avatar | enemy | npc | boss | companion | crowd",
      "characterType": "humanoid | creature | vehicle | robot | animal | other",
      "gameplayFunction": "What this character does mechanically — attacks, collects, guards, etc.",
      "animationNeeds": ["idle", "walk", "attack_or_action"],
      "importance": "required | optional",
      "selectionPriority": 1,
      "textureStyle": "Brief material/texture style note"
    }
  ]
}`;
}

/* ═══════════════════════════════════════════════════════════════════ */
exports.handler = async (event) => {
  let projectPath = null;
  let bucket      = null;
  let jobId       = null;

  const err400 = msg => ({ statusCode: 400, body: msg });
  const err500 = msg => ({ statusCode: 500, body: msg });

  try {
    if (!event.body) return { statusCode: 400, body: "" };

    const body = JSON.parse(event.body);
    jobId = body.jobId;
    projectPath = body.projectPath;
    if (!projectPath || !jobId) return { statusCode: 400, body: "" };

    const apiKey = process.env.ANTHROPIC_API_KEY;
    if (!apiKey) throw new Error("Missing ANTHROPIC_API_KEY");

    bucket = admin.storage().bucket(
      process.env.FIREBASE_STORAGE_BUCKET || "gokudatabase.firebasestorage.app"
    );

    console.log(`[ROSTER-GEN] Starting for project ${projectPath}, job ${jobId}`);

    // ── 1. Load global CSV and extract live category list ────────────────
    console.log(`[ROSTER-GEN] Loading global asset CSV from ${GLOBAL_ASSET_CSV_PATH}`);
    const csvFile = bucket.file(GLOBAL_ASSET_CSV_PATH);
    const [csvExists] = await csvFile.exists();
    if (!csvExists) throw new Error(`Global asset CSV not found at ${GLOBAL_ASSET_CSV_PATH}`);
    const [csvBuffer] = await csvFile.download();
    const categoryList = parseCategoriesFromCsv(csvBuffer.toString("utf8"));
    console.log(`[ROSTER-GEN] CSV loaded: ${categoryList.length} unique categories found`);

    // ── 2. Load Master Prompt + inline images from ai_request.json ──────
    const requestFile = bucket.file(`${projectPath}/ai_request.json`);
    const [reqExists] = await requestFile.exists();
    if (!reqExists) return err400("ai_request.json not found. Submit prompt first.");
    const [reqContent] = await requestFile.download();
    const { prompt: masterPrompt, inlineImages = [], roadPipeline: requestRoadPipeline = null, avatarPipeline = null } = JSON.parse(reqContent.toString());
    if (!masterPrompt) return err400("No prompt found in ai_request.json");

    // ── 3. Build reference image blocks ─────────────────────────────────
    const refImageBlocks = [];
    for (const img of inlineImages) {
      if (img.data && img.mimeType && img.mimeType.startsWith("image/")) {
        refImageBlocks.push({
          type:   "image",
          source: { type: "base64", media_type: img.mimeType, data: img.data }
        });
      }
    }
    if (refImageBlocks.length > 0) {
      console.log(`[ROSTER-GEN] Loaded ${refImageBlocks.length} reference image(s)`);
    }

    // ── 4. Phase 1 — Game Visual Needs Analysis ──────────────────────────
    console.log("[ROSTER-GEN] Phase 1: analyzing game visual requirements...");

    const imagePreamble = refImageBlocks.length > 0
      ? `\nREFERENCE IMAGES: ${refImageBlocks.length} gameplay reference image(s) are attached. ` +
        `They carry authority equal to the Master Prompt. Use them to infer visual style, ` +
        `environment type, entity types, color palette, and particle FX requirements.\n\n`
      : "";

    const roadPipeline = detectRoadPipelineSettings(masterPrompt, requestRoadPipeline);
    const phase1Result = await callClaude(apiKey, {
      model:       "claude-sonnet-4-20250514",
      maxTokens:   16000,
      system:      "You are a game visual requirements analyst. Respond only with a valid JSON object. No markdown, no fences, no preamble.",
      userContent: [
        { type: "text", text: imagePreamble + buildPhase1Prompt(masterPrompt, categoryList, roadPipeline) },
        ...refImageBlocks
      ]
    });

    let phase1;
    try {
      phase1 = JSON.parse(stripFences(phase1Result.text));
    } catch (e) {
      const tokenInfo = phase1Result.usage
        ? `(used ${phase1Result.usage.output_tokens} output tokens — limit was 16000)`
        : "(token usage unavailable)";
      const likelyTruncated = (phase1Result.usage?.output_tokens ?? 0) >= 15900;
      console.error(
        `[ROSTER-GEN] Phase 1 JSON parse failed ${tokenInfo}` +
        (likelyTruncated ? " — response appears TRUNCATED, increase maxTokens further." : " — malformed JSON from model.")
      );
      console.error("[ROSTER-GEN] Response head:", phase1Result.text.slice(0, 300));
      console.error("[ROSTER-GEN] Response tail:", phase1Result.text.slice(-300));
      return err500(`Phase 1 returned unparseable JSON: ${e.message}`);
    }

    phase1.avatarRequirements = Array.isArray(phase1.avatarRequirements) ? phase1.avatarRequirements : [];

    // Validate rankedCategories: enforce minimum 2, maximum 6, and derive legacy suggestedCategories
    for (const obj of (phase1.objects3d || [])) {
      obj.variantGroup = String(obj.variantGroup || 'unique').trim() || 'unique';
      const { rankedCategories, suggestedCategories } = normalizeSuggestedCategoryRanking(obj);
      obj.rankedCategories = rankedCategories;
      obj.suggestedCategories = suggestedCategories;

      // Warn about unknown categories (StageAB will also filter these out)
      for (const entry of rankedCategories) {
        if (!categoryList.includes(entry.category)) {
          console.warn(
            `[ROSTER-GEN] Object "${obj.name}" suggested unknown category "${entry.category}" ` +
            `(${entry.likelihoodPercent}%) — will be ignored in filter`
          );
        }
      }

      // Enforce minimum of 2
      if (rankedCategories.length === 0) {
        console.warn(`[ROSTER-GEN] Object "${obj.name}" returned no rankedCategories — will be skipped in StageAB unless Phase 1 is retried with valid categories`);
      } else if (rankedCategories.length < MIN_SUGGESTED_CATS) {
        console.warn(
          `[ROSTER-GEN] Object "${obj.name}" returned only ${rankedCategories.length} ranked category ` +
          `— minimum is ${MIN_SUGGESTED_CATS}. StageAB will skip this object until at least 2 valid categories are supplied.`
        );
      }
    }

    console.log(
      `[ROSTER-GEN] Phase 1 complete: ` +
      `${(phase1.particleEffects || []).length} particle effect(s), ` +
      `${(phase1.objects3d || []).length} 3D object(s), ${(phase1.avatarRequirements || []).length} avatar requirement(s) identified`
    );
    for (const obj of (phase1.objects3d || [])) {
      const rankingSummary = (obj.rankedCategories || [])
        .map(entry => `${entry.category} (${entry.likelihoodPercent}%)`)
        .join(", ");
      console.log(
        `[ROSTER-GEN]   "${obj.name}" → ranked categories: ` +
        `${rankingSummary || "NONE (StageAB will skip until categories are supplied)"}`
      );
    }

    // ── 5. Write Phase 1 result + category list to Firebase ─────────────
    const phase1Payload = {
      phase1,
      categoryList,         // pass live list to StageAB so it doesn't re-parse CSV
      roadPipeline,
      avatarPipeline: {
        zipPath: avatarPipeline?.zipPath || AVATARS_ZIP_PATH_DEFAULT
      },
      variantGroupWarnings: buildVariantGroupWarnings(phase1.objects3d || []),
      jobId,
      generatedAt:          Date.now(),
      masterPromptSnippet:  masterPrompt.slice(0, 120)
    };

    await bucket.file(`${projectPath}/ai_asset_roster_phase1.json`).save(
      JSON.stringify(phase1Payload, null, 2),
      { contentType: "application/json", resumable: false }
    );

    console.log(
      `[ROSTER-GEN] Phase 1 written to Firebase. ` +
      `Objects: ${(phase1.objects3d || []).length}, ` +
      `Avatars: ${(phase1.avatarRequirements || []).length}, ` +
      `Particles: ${(phase1.particleEffects || []).length}. ` +
      `Waiting for user reference images.`
    );

    return { statusCode: 202, body: "" };

  } catch (error) {
    console.error("[ROSTER-GEN] Unhandled error:", error);
    if (bucket && projectPath) {
      try {
        await bucket.file(`${projectPath}/ai_asset_roster_error.json`).save(
          JSON.stringify({ error: error.message, failedAt: Date.now(), stage: "phase1", jobId: jobId || null }),
          { contentType: "application/json", resumable: false }
        );
      } catch (e) { /* non-fatal */ }
    }
    return { statusCode: 202, body: "" };
  }
};
