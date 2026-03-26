/* netlify/functions/claudeRosterGenerate-background.js */
/* ═══════════════════════════════════════════════════════════════════
   GAME-SPECIFIC ASSET ROSTER GENERATION — v6.0 (Phase 1 Only)
   ─────────────────────────────────────────────────────────────────
   Background Netlify function (suffix -background = 15-min timeout).
   Returns 202 immediately. Writes result to Firebase when done.
   Frontend polls ai_asset_roster_phase1.json to detect completion.

   Flow:
     1. Read global CSV from BASE_Files(template)/asset_3d_objects/
        reorganized_assets_manifest.csv to extract the live category list.
     2. Read Master Prompt + inline images from ai_request.json.
     3. PHASE 1 — Claude analyzes the game prompt + gameplay reference
        images and produces a structured list of required particle effects
        and 3D objects. Each 3D object requirement includes up to 3
        suggestedCategories chosen from the live CSV category list.
     4. Save the Phase 1 payload as ai_asset_roster_phase1.json.
     5. Frontend collects one user reference image per required 3D object.
     6. claudeRosterStageAB-background reads the CSV again, filters assets
        by suggestedCategories, then runs Stage A/B on the filtered pool.

   Global asset paths (shared across all projects):
     CSV:  BASE_Files(template)/asset_3d_objects/reorganized_assets_manifest.csv
     Zips: BASE_Files(template)/asset_3d_objects/{asset_name}.zip

   Request body: { projectPath, jobId }
   Response:     202 Accepted (background function — no body)
   ═══════════════════════════════════════════════════════════════════ */

const fetch = require("node-fetch");
const admin = require("./firebaseAdmin");

const GLOBAL_ASSET_CSV_PATH = "BASE_Files(template)/asset_3d_objects/reorganized_assets_manifest.csv";

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

function buildPhase1Prompt(masterPrompt, categoryList) {
  const catBlock = categoryList.map(c => `  - ${c}`).join("\n");

  return `You are a game visual requirements analyst. Your ONLY job in this phase is to study the game description and produce a structured list of every particle effect and 3D object the game needs.

DO NOT reference any asset files, filenames, or packs. You have not seen them yet.
DO NOT include surface textures, UI elements, or anything other than particle effects and 3D objects.
Be specific and visual in your descriptions — describe what each thing looks like, its size relative to the scene, its motion characteristics, and the gameplay moment it appears in.

MASTER GAME PROMPT:
${masterPrompt}

AVAILABLE 3D ASSET CATEGORIES:
The asset library is organised into the following categories. For each 3D object requirement you identify, you MUST assign between 2 and 3 categories from this exact list. Always provide a second category — assets are sometimes miscategorised or exist in related folders. A third category is optional but encouraged when there is a plausible alternate location.

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
      "visualDescription": "What this object looks like — shape, silhouette, style, approximate scale",
      "gameplayRole": "What this object does in the game — obstacle, collectible, environment piece, character, etc.",
      "suggestedCategories": ["Primary_Category/Sub_Category", "Secondary_Category/Sub_Category"]
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
    const { prompt: masterPrompt, inlineImages = [] } = JSON.parse(reqContent.toString());
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

    const phase1Result = await callClaude(apiKey, {
      model:       "claude-sonnet-4-20250514",
      maxTokens:   4000,
      system:      "You are a game visual requirements analyst. Respond only with a valid JSON object. No markdown, no fences, no preamble.",
      userContent: [
        { type: "text", text: imagePreamble + buildPhase1Prompt(masterPrompt, categoryList) },
        ...refImageBlocks
      ]
    });

    let phase1;
    try {
      phase1 = JSON.parse(stripFences(phase1Result.text));
    } catch (e) {
      console.error("[ROSTER-GEN] Phase 1 JSON parse failed:", phase1Result.text.slice(0, 500));
      return err500(`Phase 1 returned unparseable JSON: ${e.message}`);
    }

    // Validate suggestedCategories: enforce minimum 2, maximum 3
    for (const obj of (phase1.objects3d || [])) {
      if (!Array.isArray(obj.suggestedCategories)) obj.suggestedCategories = [];

      // Clamp to max 3
      obj.suggestedCategories = obj.suggestedCategories.slice(0, 3);

      // Warn about unknown categories (StageAB will also filter these out)
      for (const cat of obj.suggestedCategories) {
        if (!categoryList.includes(cat)) {
          console.warn(`[ROSTER-GEN] Object "${obj.name}" suggested unknown category "${cat}" — will be ignored in filter`);
        }
      }

      // Enforce minimum of 2
      if (obj.suggestedCategories.length === 0) {
        console.warn(`[ROSTER-GEN] Object "${obj.name}" returned no suggestedCategories — will scan full library`);
      } else if (obj.suggestedCategories.length === 1) {
        console.warn(`[ROSTER-GEN] Object "${obj.name}" returned only 1 suggestedCategory — minimum is 2. StageAB will scan only that category; consider retrying Phase 1 if match quality is poor.`);
      }
    }

    console.log(
      `[ROSTER-GEN] Phase 1 complete: ` +
      `${(phase1.particleEffects || []).length} particle effect(s), ` +
      `${(phase1.objects3d || []).length} 3D object(s) identified`
    );
    for (const obj of (phase1.objects3d || [])) {
      console.log(`[ROSTER-GEN]   "${obj.name}" → categories: ${(obj.suggestedCategories || []).join(", ") || "NONE (full scan)"}`);
    }

    // ── 5. Write Phase 1 result + category list to Firebase ─────────────
    const phase1Payload = {
      phase1,
      categoryList,         // pass live list to StageAB so it doesn't re-parse CSV
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
