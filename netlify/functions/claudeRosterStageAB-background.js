/* netlify/functions/claudeRosterStageAB-background.js */
/* ═══════════════════════════════════════════════════════════════════
   ASSET ROSTER — STAGE A/B VISUAL MATCHING — v6.0
   ─────────────────────────────────────────────────────────────────
   Background Netlify function (suffix -background = 15-min timeout).
   Returns 202 immediately. Writes result to Firebase when done.
   Frontend polls ai_asset_roster_pending.json to detect completion.

   Key change from v5: CSV-driven category pre-filtering.
   ─────────────────────────────────────────────────────────────────
   Flow:
     1. Read Phase 1 result from ai_asset_roster_phase1.json.
        Phase 1 includes suggestedCategories (up to 3) per 3D object.
     2. Read user reference images from ai_roster_ref_images.json.
     3. Read global CSV from game-generator-1/projects/BASE_Files/asset_3d_objects/
        reorganized_assets_manifest.csv → build assetName→category map.
     4. Scan ONLY the zip files whose asset_name maps to one of the
        suggestedCategories for each requirement. If no valid CSV-backed
        categories are suggested, skip object search for that requirement
        rather than falling back to the full library.
     5. STAGE A — image-vs-image batch scan on the filtered asset pool.
        Particles: text description vs thumbnails (unchanged).
        3D Objects: user reference image vs filtered thumbnails.
     6. STAGE B — per-requirement final visual pick (unchanged).
     7. Assemble final roster, enforce limits, save pending.json.

   Global asset paths (shared across all projects):
     CSV:  game-generator-1/projects/BASE_Files/asset_3d_objects/reorganized_assets_manifest.csv
     Zips: game-generator-1/projects/BASE_Files/asset_3d_objects/{asset_name}.zip

   Request body: { projectPath, jobId }
   Response:     202 Accepted (background function — no body)
   ═══════════════════════════════════════════════════════════════════ */

const fetch  = require("node-fetch");
const admin  = require("./firebaseAdmin");
const JSZip  = require("jszip");

/* ─── Constants ──────────────────────────────────────────────────── */
const GLOBAL_ASSET_BASE    = "game-generator-1/projects/BASE_Files/asset_3d_objects";
const GLOBAL_ASSET_CSV     = `${GLOBAL_ASSET_BASE}/reorganized_assets_manifest.csv`;

const CLAUDE_MAX_RETRIES   = 5;
const CLAUDE_BASE_DELAY_MS = 1250;
const CLAUDE_MAX_DELAY_MS  = 12000;

const MAX_OBJ_ASSETS       = 25;
const MAX_PNG_ASSETS       = 50;
const IMAGES_PER_BATCH     = 50;
const MAX_SUGGESTED_CATS   = 3;   // hard cap — Phase 1 enforces this too

/* ─── Retry helpers ──────────────────────────────────────────────── */
function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

function computeRetryDelay(attempt) {
  return Math.min(
    CLAUDE_BASE_DELAY_MS * Math.pow(2, Math.max(0, attempt - 1)),
    CLAUDE_MAX_DELAY_MS
  ) + Math.floor(Math.random() * 700);
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
  const body = {
    model,
    max_tokens: maxTokens,
    system,
    messages: [{ role: "user", content: userContent }]
  };
  let last;
  for (let i = 1; i <= CLAUDE_MAX_RETRIES; i++) {
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
      if (i >= CLAUDE_MAX_RETRIES) throw err;
      await sleep(computeRetryDelay(i));
    }
  }
  throw last;
}

/* ─── CSV parsing ────────────────────────────────────────────────── */
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

// Returns { map: Map<assetName (lowercase), category>, categories: Set<category> }
function parseCsvIndex(csvText) {
  const rows = parseCsvRows(csvText);
  if (rows.length === 0) throw new Error('CSV is empty');

  const header = rows[0].map(h => h.trim().toLowerCase());
  const nameIdx = header.indexOf('asset_name');
  const catIdx  = header.indexOf('new_category');
  if (nameIdx === -1 || catIdx === -1) {
    throw new Error("CSV missing 'asset_name' or 'new_category' column");
  }

  const map = new Map();
  const categories = new Set();
  for (let i = 1; i < rows.length; i++) {
    const name = (rows[i][nameIdx] || '').trim().toLowerCase();
    const cat  = (rows[i][catIdx]  || '').trim();
    if (name && cat) {
      map.set(name, cat);
      categories.add(cat);
    }
  }

  return { map, categories };
}

/* ─── Utilities ──────────────────────────────────────────────────── */
function stripFences(text) {
  let t = text
    .replace(/^```json\s*/i, "").replace(/^```\s*/i, "").replace(/\s*```$/i, "").trim();
  const a = t.indexOf("{"), b = t.lastIndexOf("}");
  if (a > 0 && b > a) t = t.substring(a, b + 1);
  return t.trim();
}

function chunkArray(arr, size) {
  const out = [];
  for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size));
  return out;
}

/* ─── Enforce hard selection limits ─────────────────────────────── */
function enforceHardLimits(roster) {
  if (!roster) return roster;
  if (Array.isArray(roster.objects3d) && roster.objects3d.length > MAX_OBJ_ASSETS) {
    console.warn(`[ROSTER-AB] Trimming objects3d from ${roster.objects3d.length} to ${MAX_OBJ_ASSETS}`);
    roster.objects3d = roster.objects3d.slice(0, MAX_OBJ_ASSETS);
  }
  if (Array.isArray(roster.textureAssets) && roster.textureAssets.length > MAX_PNG_ASSETS) {
    console.warn(`[ROSTER-AB] Trimming textureAssets from ${roster.textureAssets.length} to ${MAX_PNG_ASSETS}`);
    roster.textureAssets = roster.textureAssets.slice(0, MAX_PNG_ASSETS);
  }
  if (roster.coverageSummary) {
    roster.coverageSummary.totalObjects3d  = (roster.objects3d    || []).length;
    roster.coverageSummary.totalTextures   = (roster.textureAssets || []).length;
    roster.coverageSummary.limitsRespected =
      roster.coverageSummary.totalObjects3d <= MAX_OBJ_ASSETS &&
      roster.coverageSummary.totalTextures  <= MAX_PNG_ASSETS;
  }
  return roster;
}

/* ─── Stage A prompt: particle text-vs-image batch scan ─────────── */
function buildStageAParticlePrompt(requirements) {
  const reqList = requirements.map((r, i) =>
    `  ${i + 1}. ${r.name}: ${r.visualDescription}` +
    (r.behaviorDescription ? ` — ${r.behaviorDescription}` : "")
  ).join("\n");

  return `You are a game asset visual screener. You will be shown a batch of particle effect texture thumbnail images.
Your job is to identify which images are plausible visual candidates for any of the particle effect requirements listed below.
Cast a wide net — include anything that could plausibly match, even loosely, but still respect whether the requirement reads more like a burst / impact / spark versus a trail / smoke / lingering streak.

PARTICLE EFFECT REQUIREMENTS:
${reqList}

The images in this batch are numbered sequentially starting at 1.
For each image, list which requirement numbers (1-based) it could satisfy. Use an empty array if none.

Respond ONLY with a valid JSON object. No markdown, no fences, no preamble.

{
  "matches": [
    { "imageIndex": 1, "matchesRequirements": [1, 3] },
    { "imageIndex": 2, "matchesRequirements": [] },
    { "imageIndex": 3, "matchesRequirements": [2] }
  ]
}`;
}

/* ─── Stage A prompt: 3D object image-vs-image batch scan ───────── */
function buildStageAObjectRefImagePrompt(requirementName, gameplayRole) {
  return `You are a game asset visual screener matching 3D object thumbnails against a user-provided reference image.

The FIRST image attached is the user's reference image for the requirement:
  Name: ${requirementName}
  Gameplay role: ${gameplayRole || "not specified"}

The remaining images (numbered 1, 2, 3... in your response) are candidate thumbnails from the 3D object library.
Your job: identify which library thumbnails are visually similar enough to the reference image to be a plausible match.
Cast a wide net — include anything that shares the general shape, style, or object category, but prefer candidates that look like final visible gameplay objects rather than generic placeholder geometry.

Respond ONLY with a valid JSON object. No markdown, no fences, no preamble.

{
  "matches": [
    { "imageIndex": 1, "matchesReference": true },
    { "imageIndex": 2, "matchesReference": false },
    { "imageIndex": 3, "matchesReference": true }
  ]
}`;
}

/* ─── Stage B prompt: particle text-based final pick ────────────── */
function buildStageBParticlePrompt(requirementName, requirementDesc, candidates, gameInterpretation) {
  return `GAME CONTEXT:
${gameInterpretation}

You are making the final asset selection for a game. You have been given thumbnail images of candidate particle texture assets. Pick the single best visual match for the requirement below.

REQUIREMENT:
Name: ${requirementName}
Description: ${requirementDesc}
Type: Particle Effect Texture

CANDIDATE THUMBNAILS (images attached in order):
${candidates.map((c, i) => `  Image ${i + 1}: ${c.assetFile} (${c.sourceZip})`).join("\n")}

SELECTION RULES:
- Judge purely by visual appearance vs the requirement description.
- Consider shape silhouette, density, edge softness, color tone, and whether the texture reads more like a burst / impact / spark versus a trail / smoke / lingering streak.
- Pick exactly one winner. State which image number you chose and why.

Respond ONLY with a valid JSON object. No markdown, no fences, no preamble.

{
  "requirementName": "${requirementName}",
  "imageNumberChosen": 1,
  "visualSelectionRationale": "What you saw in the thumbnail that matched the requirement"
}`;
}

/* ─── Stage B prompt: 3D object image-vs-image final pick ───────── */
function buildStageBObjectRefImagePrompt(requirementName, gameplayRole, candidates, gameInterpretation) {
  return `GAME CONTEXT:
${gameInterpretation}

You are making the final 3D object asset selection. The FIRST image is the user's reference image showing what the object should look like. The remaining images are candidate library thumbnails.

REQUIREMENT:
Name: ${requirementName}
Gameplay role: ${gameplayRole || "not specified"}

CANDIDATE THUMBNAILS (images 2 onwards, numbered starting at 1 in your response):
${candidates.map((c, i) => `  Image ${i + 1}: ${c.objFile} (${c.sourceZip})`).join("\n")}

SELECTION RULES:
- The reference image (first image) is the target appearance.
- Pick the candidate thumbnail that most closely resembles the reference in shape, silhouette, style, object category, and final in-game readability.
- Prefer richer authored objects over obvious placeholder or low-detail geometry when both satisfy the role.
- Pick exactly one winner. State which candidate image number (1-based, not counting the reference) you chose and why.
- Do NOT infer or default a colormap filename here. Colormap resolution is handled later by the extract stage from the selected asset's zip contents.

Respond ONLY with a valid JSON object. No markdown, no fences, no preamble.

{
  "requirementName": "${requirementName}",
  "imageNumberChosen": 1,
  "visualSelectionRationale": "What made this thumbnail most similar to the reference image"
}`;
}

/* ═══════════════════════════════════════════════════════════════════ */
exports.handler = async (event) => {
  let projectPath = null;
  let bucket      = null;
  let jobId       = null;

  const err400 = msg => ({ statusCode: 400, body: msg });

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

    console.log(`[ROSTER-AB] Starting Stage A/B for project ${projectPath}, job ${jobId}`);

    // ── 1. Load Phase 1 result ───────────────────────────────────────────
    const phase1File = bucket.file(`${projectPath}/ai_asset_roster_phase1.json`);
    const [p1Exists] = await phase1File.exists();
    if (!p1Exists) return err400("ai_asset_roster_phase1.json not found. Run Phase 1 first.");
    const [p1Content] = await phase1File.download();
    const p1Payload   = JSON.parse(p1Content.toString());
    const { phase1 }  = p1Payload;
    if (!phase1) return err400("No phase1 data in ai_asset_roster_phase1.json");

    const particleReqs      = phase1.particleEffects || [];
    const objectReqs        = phase1.objects3d       || [];
    const gameInterpretation = phase1.gameInterpretationSummary || "";

    console.log(`[ROSTER-AB] Phase 1 loaded: ${particleReqs.length} particle req(s), ${objectReqs.length} object req(s)`);

    // ── 2. Load user reference images ───────────────────────────────────
    const refImagesFile = bucket.file(`${projectPath}/ai_roster_ref_images.json`);
    const [refExists]   = await refImagesFile.exists();
    if (!refExists) return err400("ai_roster_ref_images.json not found. Frontend must upload user reference images first.");
    const [refContent]      = await refImagesFile.download();
    const { objects: userRefImages = [] } = JSON.parse(refContent.toString());

    const refImageByName = new Map();
    for (const img of userRefImages) {
      if (img.requirementName && img.b64 && img.mimeType) {
        refImageByName.set(img.requirementName.toLowerCase(), img);
      }
    }
    console.log(`[ROSTER-AB] User reference images loaded: ${refImageByName.size} object(s) have reference images`);

    // ── 3. Load global CSV → asset_name → category map ──────────────────
    console.log(`[ROSTER-AB] Loading global asset CSV from ${GLOBAL_ASSET_CSV}`);
    const csvFile = bucket.file(GLOBAL_ASSET_CSV);
    const [csvExists] = await csvFile.exists();
    if (!csvExists) throw new Error(`Global asset CSV not found at ${GLOBAL_ASSET_CSV}`);
    const [csvBuffer] = await csvFile.download();
    const { map: assetCategoryMap, categories: knownCategories } = parseCsvIndex(csvBuffer.toString("utf8"));
    console.log(`[ROSTER-AB] CSV loaded: ${assetCategoryMap.size} asset entries`);

    // ── 4. Build per-requirement allowed category sets ───────────────────
    // Map<requirementName, Set<category>> — empty Set means "skip object search"
    const reqCategoryFilter = new Map();
    for (const req of objectReqs) {
      const cats = (req.suggestedCategories || []).slice(0, MAX_SUGGESTED_CATS);
      // Only keep categories that actually exist in the CSV
      const validCats = new Set(cats.filter(c => {
        const known = knownCategories.has(c);
        if (!known) console.warn(`[ROSTER-AB] Req "${req.name}": unknown category "${c}" — ignoring`);
        return known;
      }));
      reqCategoryFilter.set(req.name, validCats);
      if (validCats.size > 0) {
        console.log(`[ROSTER-AB] Req "${req.name}": filtering to ${validCats.size} category(s): ${[...validCats].join(", ")}`);
      } else {
        console.warn(`[ROSTER-AB] Req "${req.name}": no valid categories — skipping object search for this requirement`);
      }
    }

    // ── 5. Scan particle zip files (project-local, unchanged) ───────────
    const particleAssets = []; // { assetFile, b64, mimeType, sourceZip }
    {
      const particlePrefix = `${projectPath}/asset_particle_textures/`;
      let particleFiles;
      try {
        [particleFiles] = await bucket.getFiles({ prefix: particlePrefix });
      } catch (e) {
        console.warn(`[ROSTER-AB] Could not list particle folder: ${e.message}`);
        particleFiles = [];
      }
      const particleZips = (particleFiles || []).filter(f => f.name.toLowerCase().endsWith(".zip"));
      console.log(`[ROSTER-AB] Particle zips found: ${particleZips.length}`);

      for (const zipFile of particleZips) {
        const sourceZip = zipFile.name.split("/").pop();
        try {
          const [zipBuffer] = await zipFile.download();
          const zip = await JSZip.loadAsync(zipBuffer);
          let added = 0;
          for (const entryPath of Object.keys(zip.files)) {
            if (zip.files[entryPath].dir) continue;
            const base  = entryPath.split("/").pop();
            const lower = base.toLowerCase();
            if (base.startsWith("._")) continue;
            if (![".png", ".jpg", ".jpeg", ".webp"].some(e => lower.endsWith(e))) continue;
            const blob     = await zip.files[entryPath].async("nodebuffer");
            const mimeType = lower.endsWith(".png") ? "image/png" : "image/jpeg";
            particleAssets.push({ assetFile: base, b64: blob.toString("base64"), mimeType, sourceZip });
            added++;
          }
          console.log(`[ROSTER-AB] Particle zip ${sourceZip}: ${added} asset(s) indexed`);
        } catch (e) {
          console.warn(`[ROSTER-AB] Could not process particle zip ${sourceZip}: ${e.message}`);
        }
      }
    }

    // ── 6. Scan global 3D object mega-zips, tagged with CSV category ────
    //
    // Zip structure (derived from CSV new_category column):
    //   {TopLevel}.zip / {SubCategory} / {asset_name} / {asset_name}.obj
    //                                                  / {asset_name}.jpg  ← thumbnail
    //                                                  / colormap.jpg
    //
    // CSV new_category = "Architecture_Modular/Floors_Stairs_Pillars"
    //   → zip file:      Architecture_Modular.zip
    //   → internal path: Floors_Stairs_Pillars/{asset_name}/
    //
    // Top-level zip names are derived dynamically from the CSV — adding a
    // 5th zip requires no code changes, just updating the CSV and uploading.
    //
    // Strategy: load each mega-zip ONCE, index ALL assets inside it tagged
    // with their full CSV category. Stage A filters the in-memory array
    // per-requirement — no repeat zip downloads per requirement.
    //
    // objectAssets: { objFile, thumbFile, b64, mimeType, sourceZip, assetName, category }

    console.log(`[ROSTER-AB] Scanning global 3D object mega-zips from ${GLOBAL_ASSET_BASE}/`);
    const objectAssets = [];
    {
      // Derive unique top-level zip names from CSV categories dynamically.
      // "Architecture_Modular/Floors_Stairs_Pillars" → "Architecture_Modular"
      const topLevelZipNames = new Set();
      for (const cat of assetCategoryMap.values()) {
        const topLevel = cat.split("/")[0];
        if (topLevel) topLevelZipNames.add(topLevel);
      }
      console.log(`[ROSTER-AB] Top-level zips derived from CSV: ${[...topLevelZipNames].join(", ")}`);

      for (const zipName of topLevelZipNames) {
        const zipPath = `${GLOBAL_ASSET_BASE}/${zipName}.zip`;
        const zipFile = bucket.file(zipPath);
        const [zipExists] = await zipFile.exists();
        if (!zipExists) {
          console.warn(`[ROSTER-AB] Mega-zip not found: ${zipPath} — skipping`);
          continue;
        }

        console.log(`[ROSTER-AB] Loading mega-zip: ${zipName}.zip`);
        let zip;
        try {
          const [zipBuffer] = await zipFile.download();
          zip = await JSZip.loadAsync(zipBuffer);
        } catch (e) {
          console.warn(`[ROSTER-AB] Could not load ${zipName}.zip: ${e.message} — skipping`);
          continue;
        }

        // Group zip entries by "SubCategory/asset_name" folder key.
        // Internal path: {SubCategory}/{asset_name}/{filename}
        // Map< "SubCategory/asset_name" → { subCategory, assetFolder, objEntry, thumbEntry } >
        const assetFolderMap = new Map();

        // Detect whether the zip has a redundant root folder matching the zip name.
        // e.g. Architecture_Modular.zip may contain:
        //   Architecture_Modular/Modular_Blocks_Panels/fountain-center/file  ← extra level
        // OR the expected:
        //   Modular_Blocks_Panels/fountain-center/file                        ← direct
        // We detect this by checking if parts[0] matches zipName (case-insensitive).
        // If so, we shift the index offset by 1.
        const zipNameLower = zipName.toLowerCase();
        let depthOffset = 0;
        for (const entryPath of Object.keys(zip.files)) {
          if (zip.files[entryPath].dir) continue;
          const p = entryPath.split("/");
          if (p.length >= 1 && p[0].toLowerCase() === zipNameLower) {
            depthOffset = 1;
          }
          break; // only need to check first file
        }
        if (depthOffset > 0) {
          console.log(`[ROSTER-AB] Mega-zip ${zipName}.zip has redundant root folder — adjusting depth offset`);
        }

        for (const entryPath of Object.keys(zip.files)) {
          if (zip.files[entryPath].dir) continue;
          const parts = entryPath.split("/");
          if (parts.length < 3 + depthOffset) continue; // need SubCategory/asset_name/file
          const subCategory = parts[0 + depthOffset];
          const assetFolder = parts[1 + depthOffset];
          const fileName    = parts[parts.length - 1];
          const fileLower   = fileName.toLowerCase();
          if (fileName.startsWith("._")) continue;

          const folderKey = `${subCategory}/${assetFolder}`;
          if (!assetFolderMap.has(folderKey)) {
            assetFolderMap.set(folderKey, { subCategory, assetFolder, objEntry: null, thumbEntry: null });
          }
          const entry = assetFolderMap.get(folderKey);

          if (fileLower.endsWith(".obj") && !entry.objEntry) {
            entry.objEntry = { entryPath, fileName };
          } else if (
            !fileLower.includes("colormap") &&
            [".png", ".jpg", ".jpeg", ".webp"].some(e => fileLower.endsWith(e)) &&
            !entry.thumbEntry
          ) {
            entry.thumbEntry = { entryPath, fileName, fileLower };
          }
        }

        // Build objectAssets from folder map
        let added = 0;
        for (const [folderKey, entry] of assetFolderMap) {
          if (!entry.objEntry) continue;
          if (!entry.thumbEntry) {
            console.warn(`[ROSTER-AB] ${zipName}.zip/${folderKey}: no thumbnail — skipping`);
            continue;
          }

          // Verify asset exists in CSV
          const assetNameLower = entry.assetFolder.toLowerCase();
          const csvCategory    = assetCategoryMap.get(assetNameLower);
          if (!csvCategory) {
            console.warn(`[ROSTER-AB] ${zipName}.zip/${folderKey}: "${entry.assetFolder}" not in CSV — skipping`);
            continue;
          }

          try {
            const blob = await zip.files[entry.thumbEntry.entryPath].async("nodebuffer");
            const b64  = blob.toString("base64");
            if (!b64) continue;
            const mimeType = entry.thumbEntry.fileLower.endsWith(".png") ? "image/png" : "image/jpeg";
            objectAssets.push({
              objFile:   entry.objEntry.fileName,
              thumbFile: entry.thumbEntry.fileName,
              b64,
              mimeType,
              sourceZip: `${zipName}.zip`,
              assetName: entry.assetFolder,
              category:  csvCategory          // canonical category from CSV
            });
            added++;
          } catch (e) {
            console.warn(`[ROSTER-AB] ${zipName}.zip/${folderKey}: thumbnail read failed — ${e.message}`);
          }
        }

        console.log(`[ROSTER-AB] Mega-zip ${zipName}.zip: ${added} asset(s) indexed`);
      }
    }
    console.log(`[ROSTER-AB] Asset library ready: ${particleAssets.length} particle textures, ${objectAssets.length} 3D objects`);

    // ── 7. Stage A — Visual Library Scan ────────────────────────────────
    const particleCandidates = new Map(particleReqs.map(r => [r.name, []]));
    const objectCandidates   = new Map(objectReqs.map(r   => [r.name, []]));

    // Stage A: particles (unchanged — no category filter needed)
    async function runStageAParticleBatches() {
      if (particleReqs.length === 0 || particleAssets.length === 0) return;
      const batches = chunkArray(particleAssets, IMAGES_PER_BATCH);
      console.log(`[ROSTER-AB] Stage A particles: ${particleAssets.length} assets → ${batches.length} batch(es)`);

      for (let b = 0; b < batches.length; b++) {
        const batch       = batches[b];
        const imageBlocks = batch.map(asset => ({
          type:   "image",
          source: { type: "base64", media_type: asset.mimeType, data: asset.b64 }
        }));

        let batchResult;
        try {
          batchResult = await callClaude(apiKey, {
            model:       "claude-sonnet-4-20250514",
            maxTokens:   2000,
            system:      "You are a game asset visual screener. Respond only with a valid JSON object. No markdown, no fences, no preamble.",
            userContent: [
              { type: "text", text: buildStageAParticlePrompt(particleReqs) },
              ...imageBlocks
            ]
          });
        } catch (e) {
          console.warn(`[ROSTER-AB] Stage A particle batch ${b + 1} failed: ${e.message} — skipping`);
          continue;
        }

        let parsed;
        try { parsed = JSON.parse(stripFences(batchResult.text)); }
        catch (e) {
          console.warn(`[ROSTER-AB] Stage A particle batch ${b + 1} parse failed — skipping`);
          continue;
        }

        for (const match of (parsed.matches || [])) {
          const imgIdx = (match.imageIndex || 1) - 1;
          const asset  = batch[imgIdx];
          if (!asset) continue;
          for (const reqIdx of (match.matchesRequirements || [])) {
            const req = particleReqs[reqIdx - 1];
            if (!req) continue;
            const candidates = particleCandidates.get(req.name);
            if (!candidates) continue;
            if (!candidates.some(c => c.assetFile === asset.assetFile)) {
              candidates.push(asset);
            }
          }
        }
      }
    }

    // Stage A: 3D objects — category-filtered image-vs-image
    async function runStageAObjectsImageVsImage() {
      if (objectReqs.length === 0 || objectAssets.length === 0) return;

      for (const req of objectReqs) {
        const refImg = refImageByName.get(req.name.toLowerCase());
        if (!refImg) {
          console.warn(`[ROSTER-AB] No reference image for object "${req.name}" — will be unmatched`);
          continue;
        }

        // Apply category filter — never fall back to the full library when categories fail.
        const allowedCats = reqCategoryFilter.get(req.name) || new Set();
        if (allowedCats.size === 0) {
          console.warn(`[ROSTER-AB] Stage A object "${req.name}": no valid CSV-backed categories — skipping search`);
          continue;
        }

        const filteredAssets = objectAssets.filter(a => allowedCats.has(a.category));

        console.log(
          `[ROSTER-AB] Stage A object "${req.name}": ` +
          `${filteredAssets.length} assets after category filter ` +
          `(${[...allowedCats].join(", ")})`
        );

        if (filteredAssets.length === 0) {
          console.warn(`[ROSTER-AB] Stage A object "${req.name}": 0 assets in searched categories — skipping search`);
          continue;
        }

        const refBlock = {
          type:   "image",
          source: { type: "base64", media_type: refImg.mimeType, data: refImg.b64 }
        };

        const batches    = chunkArray(filteredAssets, IMAGES_PER_BATCH);
        const candidates = objectCandidates.get(req.name);
        console.log(`[ROSTER-AB] Stage A object "${req.name}": ${filteredAssets.length} assets → ${batches.length} batch(es)`);

        for (let b = 0; b < batches.length; b++) {
          const batch       = batches[b];
          const thumbBlocks = batch.map(asset => ({
            type:   "image",
            source: { type: "base64", media_type: asset.mimeType, data: asset.b64 }
          }));

          let batchResult;
          try {
            batchResult = await callClaude(apiKey, {
              model:       "claude-sonnet-4-20250514",
              maxTokens:   2000,
              system:      "You are a game asset visual screener. Respond only with a valid JSON object. No markdown, no fences, no preamble.",
              userContent: [
                { type: "text", text: buildStageAObjectRefImagePrompt(req.name, req.gameplayRole) },
                refBlock,
                ...thumbBlocks
              ]
            });
          } catch (e) {
            console.warn(`[ROSTER-AB] Stage A object "${req.name}" batch ${b + 1} failed: ${e.message} — skipping`);
            continue;
          }

          let parsed;
          try { parsed = JSON.parse(stripFences(batchResult.text)); }
          catch (e) {
            console.warn(`[ROSTER-AB] Stage A object "${req.name}" batch ${b + 1} parse failed — skipping`);
            continue;
          }

          for (const match of (parsed.matches || [])) {
            if (!match.matchesReference) continue;
            const imgIdx = (match.imageIndex || 1) - 1;
            const asset  = batch[imgIdx];
            if (!asset) continue;
            if (!candidates.some(c => c.objFile === asset.objFile)) {
              candidates.push(asset);
            }
          }
        }

        console.log(`[ROSTER-AB] Stage A object "${req.name}": ${candidates.length} candidate(s) found`);
      }
    }

    // Run particle and object Stage A scans concurrently
    await Promise.all([
      runStageAParticleBatches(),
      runStageAObjectsImageVsImage()
    ]);

    console.log("[ROSTER-AB] Stage A complete");

    // ── 8. Stage B — Per-Requirement Final Visual Pick ───────────────────
    console.log("[ROSTER-AB] Stage B: per-requirement final visual selection...");

    async function runStageBParticle(req) {
      const candidates = particleCandidates.get(req.name) || [];
      if (candidates.length === 0) {
        console.warn(`[ROSTER-AB] Stage B particle: no candidates for "${req.name}" — unmatched`);
        return null;
      }
      const imageBlocks = candidates.map(c => ({
        type:   "image",
        source: { type: "base64", media_type: c.mimeType, data: c.b64 }
      }));
      const desc = req.visualDescription + (req.behaviorDescription ? ` — ${req.behaviorDescription}` : "");
      let result;
      try {
        result = await callClaude(apiKey, {
          model:       "claude-sonnet-4-20250514",
          maxTokens:   1000,
          system:      "You are a visual asset selection specialist. Respond only with a valid JSON object. No markdown, no fences, no preamble.",
          userContent: [
            { type: "text", text: buildStageBParticlePrompt(req.name, desc, candidates, gameInterpretation) },
            ...imageBlocks
          ]
        });
      } catch (e) {
        console.warn(`[ROSTER-AB] Stage B particle failed for "${req.name}": ${e.message} — using first candidate`);
        return { requirementName: req.name, selectedAsset: candidates[0], visualSelectionRationale: `Fallback: ${e.message}`, colormapFile: null };
      }
      let parsed;
      try { parsed = JSON.parse(stripFences(result.text)); }
      catch (e) { parsed = { imageNumberChosen: 1, visualSelectionRationale: "Fallback: parse error" }; }
      const chosenIdx = Math.min((parsed.imageNumberChosen || 1) - 1, candidates.length - 1);
      return { requirementName: req.name, selectedAsset: candidates[chosenIdx], visualSelectionRationale: parsed.visualSelectionRationale || "", colormapFile: null };
    }

    async function runStageBObject(req) {
      const candidates = objectCandidates.get(req.name) || [];
      const refImg     = refImageByName.get(req.name.toLowerCase());

      if (candidates.length === 0) {
        console.warn(`[ROSTER-AB] Stage B object: no candidates for "${req.name}" — unmatched`);
        return null;
      }

      const refBlock = refImg ? {
        type:   "image",
        source: { type: "base64", media_type: refImg.mimeType, data: refImg.b64 }
      } : null;

      const thumbBlocks = candidates.map(c => ({
        type:   "image",
        source: { type: "base64", media_type: c.mimeType, data: c.b64 }
      }));

      const userContent = refBlock
        ? [{ type: "text", text: buildStageBObjectRefImagePrompt(req.name, req.gameplayRole, candidates, gameInterpretation) }, refBlock, ...thumbBlocks]
        : [{ type: "text", text: buildStageBObjectRefImagePrompt(req.name, req.gameplayRole, candidates, gameInterpretation) }, ...thumbBlocks];

      let result;
      try {
        result = await callClaude(apiKey, {
          model:       "claude-sonnet-4-20250514",
          maxTokens:   1000,
          system:      "You are a visual asset selection specialist. Respond only with a valid JSON object. No markdown, no fences, no preamble.",
          userContent
        });
      } catch (e) {
        console.warn(`[ROSTER-AB] Stage B object failed for "${req.name}": ${e.message} — using first candidate`);
        return { requirementName: req.name, selectedAsset: candidates[0], visualSelectionRationale: `Fallback: ${e.message}`, colormapFile: null };
      }

      let parsed;
      try { parsed = JSON.parse(stripFences(result.text)); }
      catch (e) { parsed = { imageNumberChosen: 1, visualSelectionRationale: "Fallback: parse error" }; }

      const chosenIdx = Math.min((parsed.imageNumberChosen || 1) - 1, candidates.length - 1);
      return {
        requirementName:          req.name,
        selectedAsset:            candidates[chosenIdx],
        visualSelectionRationale: parsed.visualSelectionRationale || "",
        colormapFile:             null
      };
    }

    const [particleResults, objectResults] = await Promise.all([
      Promise.all(particleReqs.map(r => runStageBParticle(r))),
      Promise.all(objectReqs.map(r   => runStageBObject(r)))
    ]);

    console.log(
      `[ROSTER-AB] Stage B complete: ${particleResults.filter(Boolean).length} particle selections, ` +
      `${objectResults.filter(Boolean).length} object selections`
    );

    // ── 9. Assemble final roster ─────────────────────────────────────────
    function assembleParticleAsset(stageBResult, phase1Req) {
      if (!stageBResult) return null;
      const asset = stageBResult.selectedAsset;
      return {
        assetName:            asset.assetFile,
        sourceZip:            asset.sourceZip,
        intendedUsage:        `Particle effect: ${stageBResult.requirementName}`,
        particleEffectTarget: stageBResult.requirementName,
        matchedRequirement:   stageBResult.requirementName,
        selectionRationale:   stageBResult.visualSelectionRationale,
        thumbnailB64:         asset.b64,
        thumbnailMime:        asset.mimeType
      };
    }

    function assembleObjectAsset(stageBResult, phase1Req) {
      if (!stageBResult) return null;
      const asset = stageBResult.selectedAsset;
      const p1    = phase1Req || {};
      return {
        assetName:          asset.objFile,
        thumbFile:          asset.thumbFile,
        sourceZip:          asset.sourceZip,
        category:           asset.category || null,
        intendedRole:       p1.gameplayRole || p1.visualDescription || stageBResult.requirementName || "",
        matchedRequirement: stageBResult.requirementName,
        selectionRationale: stageBResult.visualSelectionRationale,
        colormapFile:       null,
        colormapConfidence: "PENDING_EXTRACT",
        thumbnailB64:       asset.b64,
        thumbnailMime:      asset.mimeType
      };
    }

    const phase1ParticleMap = new Map(particleReqs.map(r => [r.name, r]));
    const phase1ObjectMap   = new Map(objectReqs.map(r   => [r.name, r]));

    const textureAssets = particleResults
      .filter(Boolean)
      .map(r => assembleParticleAsset(r, phase1ParticleMap.get(r.requirementName)))
      .filter(Boolean);

    const objects3d = objectResults
      .filter(Boolean)
      .map(r => assembleObjectAsset(r, phase1ObjectMap.get(r.requirementName)))
      .filter(Boolean);

    const matchedParticleNames = new Set(textureAssets.map(a => a.matchedRequirement));
    const matchedObjectNames   = new Set(objects3d.map(a     => a.matchedRequirement));

    const unmatchedRequirements = [
      ...particleReqs.filter(r => !matchedParticleNames.has(r.name)).map(r => ({
        requirementName: r.name, type: "particle_effect", reason: "No visual candidates found in Stage A"
      })),
      ...objectReqs.filter(r => !matchedObjectNames.has(r.name)).map(r => ({
        requirementName: r.name, type: "object_3d",
        reason: ((reqCategoryFilter.get(r.name) || new Set()).size === 0)
          ? "No valid CSV-backed categories were available for this requirement; Stage A object search was skipped"
          : "No visual candidates found in the searched categories during Stage A",
        categoriesSearched: [...(reqCategoryFilter.get(r.name) || [])]
      }))
    ];

    const roster = {
      documentTitle:             "Game-Specific Asset Roster",
      gameInterpretationSummary: gameInterpretation,
      objects3d,
      textureAssets,
      unmatchedRequirements,
      coverageSummary: {
        totalObjects3d:  objects3d.length,
        totalTextures:   textureAssets.length,
        totalUnmatched:  unmatchedRequirements.length,
        limitsRespected: objects3d.length <= MAX_OBJ_ASSETS && textureAssets.length <= MAX_PNG_ASSETS,
        coverageNotes:   `${objects3d.length} objects (category-filtered, image-matched) and ${textureAssets.length} particle textures selected.`
      },
      visualDirectionNotes: {}
    };

    roster._phase1Analysis = phase1;
    enforceHardLimits(roster);

    roster._meta = {
      jobId,
      generatedAt:         Date.now(),
      totalObjectAssets:   objectAssets.length,
      totalParticleAssets: particleAssets.length,
      refImagesUsed:       refImageByName.size,
      csvEntriesLoaded:    assetCategoryMap.size,
      approved:            false
    };

    // ── 10. Save pending roster to Firebase ──────────────────────────────
    await bucket.file(`${projectPath}/ai_asset_roster_pending.json`).save(
      JSON.stringify(roster, null, 2),
      { contentType: "application/json", resumable: false }
    );

    console.log(
      `[ROSTER-AB] Complete. Objects: ${objects3d.length}, ` +
      `Textures: ${textureAssets.length}, ` +
      `Unmatched: ${unmatchedRequirements.length}, ` +
      `RefImages used: ${refImageByName.size}, ` +
      `CSV entries: ${assetCategoryMap.size}`
    );

    return { statusCode: 202, body: "" };

  } catch (error) {
    console.error("[ROSTER-AB] Unhandled error:", error);
    if (bucket && projectPath) {
      try {
        await bucket.file(`${projectPath}/ai_asset_roster_error.json`).save(
          JSON.stringify({ error: error.message, failedAt: Date.now(), stage: "stageAB", jobId: jobId || null }),
          { contentType: "application/json", resumable: false }
        );
      } catch (e) { /* non-fatal */ }
    }
    return { statusCode: 202, body: "" };
  }
};
