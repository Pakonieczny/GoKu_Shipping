/* netlify/functions/claudeRosterStageAB-background.js */
/* ═══════════════════════════════════════════════════════════════════
   ASSET ROSTER — STAGE A/B VISUAL MATCHING
   ─────────────────────────────────────────────────────────────────
   Background Netlify function (suffix -background = 15-min timeout).
   Returns 202 immediately. Writes result to Firebase when done.
   Frontend polls ai_asset_roster_pending.json to detect completion.

   Called AFTER claudeRosterGenerate-background.js (Phase 1) and AFTER
   the user has dropped reference images for each 3D object via the
   frontend modal. The frontend writes those images to Firebase as
   ai_roster_ref_images.json before calling this function.

   Flow:
     1. Read Phase 1 result from ai_asset_roster_phase1.json
     2. Read user reference images from ai_roster_ref_images.json
        Format: { objects: [{ requirementName, mimeType, b64 }] }
     3. Scan asset_particle_textures/ and asset_3d_objects/ for .zip files
        (same zip scanning as claudeRosterGenerate-background.js)
     4. STAGE A — Visual library scan:
        - Particles: compare zip thumbnails against Phase 1 text descriptions (unchanged)
        - 3D Objects: compare zip thumbnails against the user's reference image
          for each object (image-vs-image instead of text-vs-image)
     5. STAGE B — Per-requirement final visual pick (unchanged logic)
     6. Assemble final roster, enforce limits, save ai_asset_roster_pending.json

   Request body: { projectPath, jobId }
   Response:     202 Accepted (background function — no body)
   ═══════════════════════════════════════════════════════════════════ */

const fetch = require("node-fetch");
const admin = require("./firebaseAdmin");
const JSZip = require("jszip");

/* ─── Constants ──────────────────────────────────────────────────── */
const CLAUDE_OVERLOAD_MAX_RETRIES   = 5;
const CLAUDE_OVERLOAD_BASE_DELAY_MS = 1250;
const CLAUDE_OVERLOAD_MAX_DELAY_MS  = 12000;

const MAX_OBJ_ASSETS   = 25;
const MAX_PNG_ASSETS   = 50;
const IMAGES_PER_BATCH = 50;

/* ─── Retry helpers ──────────────────────────────────────────────── */
function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

function computeRetryDelay(attempt) {
  return Math.min(
    CLAUDE_OVERLOAD_BASE_DELAY_MS * Math.pow(2, Math.max(0, attempt - 1)),
    CLAUDE_OVERLOAD_MAX_DELAY_MS
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
  return m.includes("overloaded")            ||
         m.includes("rate limit")            ||
         m.includes("too many requests")     ||
         m.includes("capacity")              ||
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
  for (let i = 1; i <= CLAUDE_OVERLOAD_MAX_RETRIES; i++) {
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
      if (i >= CLAUDE_OVERLOAD_MAX_RETRIES) throw err;
      await sleep(computeRetryDelay(i));
    }
  }
  throw last;
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
  if (Array.isArray(roster.objects3d)) {
    if (roster.objects3d.length > MAX_OBJ_ASSETS) {
      console.warn(`[ROSTER-AB] Trimming objects3d from ${roster.objects3d.length} to ${MAX_OBJ_ASSETS}`);
      roster.objects3d = roster.objects3d.slice(0, MAX_OBJ_ASSETS);
    }
  }
  if (Array.isArray(roster.textureAssets)) {
    if (roster.textureAssets.length > MAX_PNG_ASSETS) {
      console.warn(`[ROSTER-AB] Trimming textureAssets from ${roster.textureAssets.length} to ${MAX_PNG_ASSETS}`);
      roster.textureAssets = roster.textureAssets.slice(0, MAX_PNG_ASSETS);
    }
  }
  if (roster.coverageSummary) {
    roster.coverageSummary.totalObjects3d = (roster.objects3d || []).length;
    roster.coverageSummary.totalTextures  = (roster.textureAssets || []).length;
    roster.coverageSummary.limitsRespected =
      roster.coverageSummary.totalObjects3d <= MAX_OBJ_ASSETS &&
      roster.coverageSummary.totalTextures  <= MAX_PNG_ASSETS;
  }
  return roster;
}

/* ─── Stage A prompt: particle text-vs-image batch scan ─────────── */
// Particles are still matched using Phase 1 text descriptions (unchanged).
function buildStageAParticlePrompt(requirements) {
  const reqList = requirements.map((r, i) =>
    `  ${i + 1}. ${r.name}: ${r.visualDescription}` +
    (r.behaviorDescription ? ` — ${r.behaviorDescription}` : "")
  ).join("\n");

  return `You are a game asset visual screener. You will be shown a batch of particle effect texture thumbnail images.
Your job is to identify which images are plausible visual candidates for any of the particle effect requirements listed below.
Cast a wide net — include anything that could plausibly match, even loosely.

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
// The first image in every Claude call for object Stage A batches is the
// user's reference image for this specific requirement. The remaining images
// are the zip thumbnail candidates. Claude scores each thumbnail for visual
// similarity to the reference image.
function buildStageAObjectRefImagePrompt(requirementName, gameplayRole) {
  return `You are a game asset visual screener matching 3D object thumbnails against a user-provided reference image.

The FIRST image attached is the user's reference image for the requirement:
  Name: ${requirementName}
  Gameplay role: ${gameplayRole || "not specified"}

The remaining images (numbered 1, 2, 3... in your response) are candidate thumbnails from the 3D object library.
Your job: identify which library thumbnails are visually similar enough to the reference image to be a plausible match.
Cast a wide net — include anything that shares the general shape, style, or object category.

Respond ONLY with a valid JSON object. No markdown, no fences, no preamble.

{
  "matches": [
    { "imageIndex": 1, "matchesReference": true },
    { "imageIndex": 2, "matchesReference": false },
    { "imageIndex": 3, "matchesReference": true }
  ]
}`;
}

/* ─── Stage B prompt: particle text-based final pick (unchanged) ─── */
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
- Consider shape silhouette, density, edge softness, color tone.
- Pick exactly one winner. State which image number you chose and why.

Respond ONLY with a valid JSON object. No markdown, no fences, no preamble.

{
  "requirementName": "${requirementName}",
  "imageNumberChosen": 1,
  "visualSelectionRationale": "What you saw in the thumbnail that matched the requirement"
}`;
}

/* ─── Stage B prompt: 3D object image-vs-image final pick ───────── */
// The first image is the user's reference. Remaining images are candidates.
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
- Pick the candidate thumbnail that most closely resembles the reference in shape, silhouette, style, and object category.
- Pick exactly one winner. State which candidate image number (1-based, not counting the reference) you chose and why.
- Include a colormapFile field — default "colormap.jpg" unless you have evidence of a different filename.

Respond ONLY with a valid JSON object. No markdown, no fences, no preamble.

{
  "requirementName": "${requirementName}",
  "imageNumberChosen": 1,
  "visualSelectionRationale": "What made this thumbnail most similar to the reference image",
  "colormapFile": "colormap.jpg"
}`;
}

/* ═══════════════════════════════════════════════════════════════════ */
exports.handler = async (event) => {
  let projectPath = null;
  let bucket      = null;

  const err400 = msg => ({ statusCode: 400, body: msg });
  const err500 = msg => ({ statusCode: 500, body: msg });

  try {
    if (!event.body) return { statusCode: 400, body: "" };

    const body = JSON.parse(event.body);
    const { jobId } = body;
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
    const { phase1, gameInterpretation: storedGameInterpretation } = JSON.parse(p1Content.toString());
    if (!phase1) return err400("No phase1 data found in ai_asset_roster_phase1.json");

    const particleReqs = phase1.particleEffects || [];
    const objectReqs   = phase1.objects3d       || [];
    const gameInterpretation = phase1.gameInterpretationSummary || storedGameInterpretation || "";

    console.log(`[ROSTER-AB] Phase 1 loaded: ${particleReqs.length} particle req(s), ${objectReqs.length} object req(s)`);

    // ── 2. Load user reference images for 3D objects ─────────────────────
    // Written by the frontend after the user completes the modal.
    // Format: { objects: [{ requirementName, mimeType, b64 }] }
    const refImagesFile = bucket.file(`${projectPath}/ai_roster_ref_images.json`);
    const [refExists]   = await refImagesFile.exists();
    if (!refExists) return err400("ai_roster_ref_images.json not found. Frontend must upload user reference images first.");
    const [refContent] = await refImagesFile.download();
    const { objects: userRefImages = [] } = JSON.parse(refContent.toString());

    // Build lookup: requirementName (lowercase) → { mimeType, b64 }
    const refImageByName = new Map();
    for (const img of userRefImages) {
      if (img.requirementName && img.b64 && img.mimeType) {
        refImageByName.set(img.requirementName.toLowerCase(), img);
      }
    }
    console.log(`[ROSTER-AB] User reference images loaded: ${refImageByName.size} object(s) have reference images`);

    // ── 3. Scan zip files and build asset libraries ──────────────────────
    const ASSET_FOLDERS = [
      { prefix: `${projectPath}/asset_particle_textures/`, packType: "particle_texture" },
      { prefix: `${projectPath}/asset_3d_objects/`,        packType: "3d_object"        }
    ];

    const particleAssets = []; // { assetFile, b64, mimeType, sourceZip }
    const objectAssets   = []; // { objFile, thumbFile, b64, mimeType, sourceZip }

    for (const { prefix, packType } of ASSET_FOLDERS) {
      let folderFiles;
      try {
        [folderFiles] = await bucket.getFiles({ prefix });
      } catch (e) {
        console.warn(`[ROSTER-AB] Could not list ${prefix}: ${e.message}`);
        continue;
      }

      const zipFiles = (folderFiles || []).filter(f => f.name.toLowerCase().endsWith(".zip"));
      console.log(`[ROSTER-AB] ${packType}: found ${zipFiles.length} zip(s) in ${prefix}`);

      for (const zipFile of zipFiles) {
        const sourceZip = zipFile.name.split("/").pop();
        try {
          const [zipBuffer] = await zipFile.download();
          const zip = await JSZip.loadAsync(zipBuffer);

          if (packType === "particle_texture") {
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

          } else {
            // 3D object pack: pair each .obj with its thumbnail
            const imagesByStem = new Map();
            for (const entryPath of Object.keys(zip.files)) {
              if (zip.files[entryPath].dir) continue;
              const base  = entryPath.split("/").pop();
              const lower = base.toLowerCase();
              if (base.startsWith("._")) continue;
              if (![".png", ".jpg", ".jpeg", ".webp"].some(e => lower.endsWith(e))) continue;
              const stem = lower.replace(/\.[^.]+$/, "");
              imagesByStem.set(stem, { entryPath, base, lower });
            }

            let added = 0;
            for (const entryPath of Object.keys(zip.files)) {
              if (zip.files[entryPath].dir) continue;
              const base  = entryPath.split("/").pop();
              const lower = base.toLowerCase();
              if (!lower.endsWith(".obj")) continue;

              const stem     = lower.replace(/\.obj$/, "");
              const imgEntry = imagesByStem.get(stem) || null;

              if (!imgEntry) {
                console.warn(`[ROSTER-AB] ${sourceZip}/${base}: matching same-stem thumbnail not found — skipping`);
                continue;
              }
              if (imgEntry.lower.includes("colormap")) {
                console.warn(`[ROSTER-AB] ${sourceZip}/${base}: same-stem image is a colormap, not a thumbnail — skipping`);
                continue;
              }

              const blob = await zip.files[imgEntry.entryPath].async("nodebuffer");
              const b64 = blob.toString("base64");
              const mimeType = imgEntry.lower.endsWith(".png") ? "image/png" : "image/jpeg";
              const thumbFile = imgEntry.base;

              if (!b64) {
                console.warn(`[ROSTER-AB] ${sourceZip}/${base}: thumbnail could not be read — skipping`);
                continue;
              }

              objectAssets.push({ objFile: base, thumbFile, b64, mimeType, sourceZip });
              added++;
            }
            console.log(`[ROSTER-AB] Object zip ${sourceZip}: ${added} obj asset(s) indexed`);
          }
        } catch (e) {
          console.warn(`[ROSTER-AB] Could not process zip ${sourceZip}: ${e.message}`);
        }
      }
    }

    console.log(`[ROSTER-AB] Asset library ready: ${particleAssets.length} particle textures, ${objectAssets.length} 3D objects`);

    // ── 4. Stage A — Visual Library Scan ────────────────────────────────
    const particleCandidates = new Map(particleReqs.map(r => [r.name, []]));
    const objectCandidates   = new Map(objectReqs.map(r   => [r.name, []]));

    // ── Stage A: particles — text-description-based (unchanged) ─────────
    async function runStageAParticleBatches() {
      if (particleReqs.length === 0 || particleAssets.length === 0) return;
      const batches = chunkArray(particleAssets, IMAGES_PER_BATCH);
      console.log(`[ROSTER-AB] Stage A particles: ${particleAssets.length} assets → ${batches.length} batch(es)`);

      for (let b = 0; b < batches.length; b++) {
        const batch = batches[b];
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

    // ── Stage A: 3D objects — image-vs-image (new) ──────────────────────
    // For each object requirement that has a user reference image:
    //   Send reference image + batches of zip thumbnails to Claude.
    //   Claude flags which thumbnails visually match the reference.
    // For requirements without a user reference image: falls through to
    //   zero candidates → unmatched (UI blocks this case, but defensive here).
    async function runStageAObjectsImageVsImage() {
      if (objectReqs.length === 0 || objectAssets.length === 0) return;
      const batches = chunkArray(objectAssets, IMAGES_PER_BATCH);

      for (const req of objectReqs) {
        const refImg = refImageByName.get(req.name.toLowerCase());
        if (!refImg) {
          console.warn(`[ROSTER-AB] No reference image for object "${req.name}" — will be unmatched`);
          continue;
        }

        const refBlock = {
          type:   "image",
          source: { type: "base64", media_type: refImg.mimeType, data: refImg.b64 }
        };

        const candidates = objectCandidates.get(req.name);
        console.log(`[ROSTER-AB] Stage A objects "${req.name}": scanning ${objectAssets.length} assets in ${batches.length} batch(es) against reference image`);

        for (let b = 0; b < batches.length; b++) {
          const batch = batches[b];
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
                refBlock,       // first image: user's reference
                ...thumbBlocks  // remaining images: zip thumbnails
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
            const imgIdx = (match.imageIndex || 1) - 1; // 0-based into batch
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

    // ── 5. Stage B — Per-Requirement Final Visual Pick ───────────────────
    console.log("[ROSTER-AB] Stage B: per-requirement final visual selection...");

    // Stage B: particles — text-based (unchanged)
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
        return {
          requirementName: req.name,
          selectedAsset: candidates[0],
          visualSelectionRationale: `Fallback: Stage B API error — ${e.message}`,
          colormapFile: null
        };
      }

      let parsed;
      try { parsed = JSON.parse(stripFences(result.text)); }
      catch (e) {
        parsed = { imageNumberChosen: 1, visualSelectionRationale: "Fallback: parse error" };
      }

      const chosenIdx = Math.min((parsed.imageNumberChosen || 1) - 1, candidates.length - 1);
      return {
        requirementName:          req.name,
        selectedAsset:            candidates[chosenIdx],
        visualSelectionRationale: parsed.visualSelectionRationale || "",
        colormapFile:             null
      };
    }

    // Stage B: 3D objects — image-vs-image (new)
    // Reference image is prepended as the first image in the call.
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
        return {
          requirementName: req.name,
          selectedAsset: candidates[0],
          visualSelectionRationale: `Fallback: Stage B API error — ${e.message}`,
          colormapFile: "colormap.jpg"
        };
      }

      let parsed;
      try { parsed = JSON.parse(stripFences(result.text)); }
      catch (e) {
        parsed = { imageNumberChosen: 1, visualSelectionRationale: "Fallback: parse error", colormapFile: "colormap.jpg" };
      }

      const chosenIdx = Math.min((parsed.imageNumberChosen || 1) - 1, candidates.length - 1);
      return {
        requirementName:          req.name,
        selectedAsset:            candidates[chosenIdx],
        visualSelectionRationale: parsed.visualSelectionRationale || "",
        colormapFile:             parsed.colormapFile || "colormap.jpg"
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

    // ── 6. Assemble final roster ─────────────────────────────────────────
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
        intendedRole:       p1.gameplayRole || p1.visualDescription || stageBResult.requirementName || "",
        matchedRequirement: stageBResult.requirementName,
        selectionRationale: stageBResult.visualSelectionRationale,
        colormapFile:       stageBResult.colormapFile || "colormap.jpg",
        colormapConfidence: stageBResult.colormapFile ? "HIGH" : "MEDIUM",
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
        requirementName: r.name, type: "object_3d", reason: "No visual candidates found in Stage A"
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
        coverageNotes:   `${objects3d.length} objects (image-matched) and ${textureAssets.length} particle textures selected.`
      },
      visualDirectionNotes: {}
    };

    roster._phase1Analysis = phase1;
    enforceHardLimits(roster);

    roster._meta = {
      jobId,
      generatedAt:        Date.now(),
      totalObjectAssets:  objectAssets.length,
      totalParticleAssets: particleAssets.length,
      refImagesUsed:      refImageByName.size,
      approved:           false
    };

    // ── 7. Save pending roster to Firebase ──────────────────────────────
    await bucket.file(`${projectPath}/ai_asset_roster_pending.json`).save(
      JSON.stringify(roster, null, 2),
      { contentType: "application/json", resumable: false }
    );

    console.log(
      `[ROSTER-AB] Complete. Objects: ${objects3d.length}, ` +
      `Textures: ${textureAssets.length}, ` +
      `Unmatched: ${unmatchedRequirements.length}, ` +
      `RefImages used: ${refImageByName.size}`
    );

    return { statusCode: 202, body: "" };

  } catch (error) {
    console.error("[ROSTER-AB] Unhandled error:", error);
    if (bucket && projectPath) {
      try {
        await bucket.file(`${projectPath}/ai_asset_roster_error.json`).save(
          JSON.stringify({ error: error.message, failedAt: Date.now(), jobId, stage: "stageAB" }),
          { contentType: "application/json", resumable: false }
        );
      } catch (e) { /* non-fatal */ }
    }
    return { statusCode: 202, body: "" };
  }
};
