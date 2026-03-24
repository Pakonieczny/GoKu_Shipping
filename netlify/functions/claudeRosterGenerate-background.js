/* netlify/functions/claudeRosterGenerate-background.js */
/* ═══════════════════════════════════════════════════════════════════
   GAME-SPECIFIC ASSET ROSTER GENERATION — v5.0 (Visual-Only Pipeline)
   ─────────────────────────────────────────────────────────────────
   Background Netlify function (suffix -background = 15-min timeout).
   Returns 202 immediately. Writes result to Firebase when done.
   Frontend polls ai_asset_roster_pending.json to detect completion.

   Flow:
     1. Read Master Prompt + inline images from ai_request.json
     2. Scan asset_particle_textures/ and asset_3d_objects/ for .zip files.
        For each zip:
          - Particle packs: extract every image as
            { assetFile, b64, mimeType, sourceZip }
          - 3D object packs: for each .obj, find same-stem image in the zip
            as its thumbnail. Fall back to first non-colormap image in the
            pack if no same-stem match.
            Store as { objFile, thumbFile, b64, mimeType, sourceZip }
        No .docx files are read. Selection is 100% visual.
     3. PHASE 1 — Claude analyzes the game prompt + reference images
        and produces a structured list of required particle effects
        and 3D objects (visual descriptions only, no asset names).
     4. STAGE A — Visual library scan (one pass, all requirements at once).
        Batches of IMAGES_PER_BATCH images are sent to Claude with all
        Phase 1 requirements. Claude tags each image against whichever
        requirement(s) it is a plausible candidate for.
        Result: per-requirement candidate lists.
     5. STAGE B — Per-requirement final visual pick.
        For each requirement, send its Stage A candidates to Claude and
        get a single winner. All requirements run in parallel.
     6. Assemble final roster. Winning thumbnails are embedded for UI display.
     7. Validate hard limits (enforceHardLimits).
     8. Save roster as ai_asset_roster_pending.json in Firebase.

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
const IMAGES_PER_BATCH = 50;  // Stage A: images per Claude call
const DEBUG_PARTICLE_STAGE_A = true; // Temporary debugging for particle texture Stage A

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
      console.warn(`[ROSTER] Trimming objects3d from ${roster.objects3d.length} to ${MAX_OBJ_ASSETS}`);
      roster.objects3d = roster.objects3d.slice(0, MAX_OBJ_ASSETS);
    }
  }
  if (Array.isArray(roster.textureAssets)) {
    if (roster.textureAssets.length > MAX_PNG_ASSETS) {
      console.warn(`[ROSTER] Trimming textureAssets from ${roster.textureAssets.length} to ${MAX_PNG_ASSETS}`);
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

/* ─── Phase 1 prompt: game visual needs analysis ─────────────────── */
function buildPhase1Prompt(masterPrompt) {
  return `You are a game visual requirements analyst. Your ONLY job in this phase is to study the game description and produce a structured list of every particle effect and 3D object the game needs.

DO NOT reference any asset files, filenames, or packs. You have not seen them yet.
DO NOT include surface textures, UI elements, or anything other than particle effects and 3D objects.
Be specific and visual in your descriptions — describe what each thing looks like, its size relative to the scene, its motion characteristics, and the gameplay moment it appears in.

MASTER GAME PROMPT:
${masterPrompt}

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
      "gameplayRole": "What this object does in the game — obstacle, collectible, environment piece, character, etc."
    }
  ]
}`;
}

/* ─── Stage A prompt: visual batch scan ─────────────────────────── */
/*
   Sent once per batch of IMAGES_PER_BATCH images.
   Claude sees all requirements and all images simultaneously and tags
   each image index against whichever requirements it could satisfy.
   Images matching nothing are tagged with an empty array.
*/
function buildStageAPrompt(requirements, isParticle) {
  const type    = isParticle ? "particle effect texture" : "3D object";
  const reqList = requirements.map((r, i) =>
    `  ${i + 1}. ${r.name}: ${r.visualDescription}` +
    (r.behaviorDescription ? ` — ${r.behaviorDescription}` : "") +
    (r.gameplayRole        ? ` — ${r.gameplayRole}`        : "")
  ).join("\n");

  return `You are a game asset visual screener. You will be shown a batch of ${type} thumbnail images.
Your job is to identify which images are plausible visual candidates for any of the game requirements listed below.
Cast a wide net — include anything that could plausibly match, even loosely. It is better to include a marginal candidate than to miss a good one.

GAME REQUIREMENTS (${type}):
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

/* ─── Stage B prompt: per-requirement final visual pick ─────────── */
function buildStageBPrompt(requirementName, requirementDesc, candidates, isParticle, gameInterpretation) {
  const type = isParticle ? "Particle Effect Texture" : "3D Object";
  return `GAME CONTEXT:
${gameInterpretation}

You are making the final asset selection for a game. You have been given thumbnail images of candidate assets. Pick the single best visual match for the requirement below.

REQUIREMENT:
Name: ${requirementName}
Description: ${requirementDesc}
Type: ${type}

CANDIDATE THUMBNAILS (images attached in order):
${candidates.map((c, i) => `  Image ${i + 1}: ${isParticle ? c.assetFile : c.objFile} (${c.sourceZip})`).join("\n")}

SELECTION RULES:
- Judge purely by visual appearance vs the requirement description.
- For particle textures: consider shape silhouette, density, edge softness, color tone.
- For 3D objects: consider overall shape, silhouette, style, and gameplay role fit.
- Pick exactly one winner. State which image number you chose and why.
- For 3D objects only: include a colormapFile field — default "colormap.jpg" unless you have evidence of a different filename.

Respond ONLY with a valid JSON object. No markdown, no fences, no preamble.

{
  "requirementName": "${requirementName}",
  "imageNumberChosen": 1,
  "visualSelectionRationale": "What you saw in the thumbnail that matched the requirement",
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

    console.log(`[ROSTER-GEN] Starting for project ${projectPath}, job ${jobId}`);

    // ── 1. Load Master Prompt + inline images from ai_request.json ──────
    const requestFile = bucket.file(`${projectPath}/ai_request.json`);
    const [reqExists] = await requestFile.exists();
    if (!reqExists) return err400("ai_request.json not found. Submit prompt first.");
    const [reqContent] = await requestFile.download();
    const { prompt: masterPrompt, inlineImages = [] } = JSON.parse(reqContent.toString());
    if (!masterPrompt) return err400("No prompt found in ai_request.json");

    // ── 2. Build reference image blocks ─────────────────────────────────
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

    // ── 3. Scan zip files and build asset libraries ──────────────────────
    /*
       Particle packs:  every image → { assetFile, b64, mimeType, sourceZip }
       3D object packs: for each .obj, pair with same-stem image in zip.
                        Fall back to first non-colormap image if no stem match.
                        → { objFile, thumbFile, b64, mimeType, sourceZip }
    */
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
        console.warn(`[ROSTER-GEN] Could not list ${prefix}: ${e.message}`);
        continue;
      }

      const zipFiles = (folderFiles || []).filter(f => f.name.toLowerCase().endsWith(".zip"));
      console.log(`[ROSTER-GEN] ${packType}: found ${zipFiles.length} zip(s) in ${prefix}`);

      for (const zipFile of zipFiles) {
        const sourceZip = zipFile.name.split("/").pop();
        try {
          const [zipBuffer] = await zipFile.download();
          const zip = await JSZip.loadAsync(zipBuffer);

          if (packType === "particle_texture") {
            // ── Every image file in the zip is a particle asset ──────────
            let added = 0;
            for (const entryPath of Object.keys(zip.files)) {
              if (zip.files[entryPath].dir) continue;
              const base  = entryPath.split("/").pop();
              const lower = base.toLowerCase();
              if (base.startsWith("._")) continue; // macOS AppleDouble metadata — not real images
              if (![".png", ".jpg", ".jpeg", ".webp"].some(e => lower.endsWith(e))) continue;

              const blob     = await zip.files[entryPath].async("nodebuffer");
              const mimeType = lower.endsWith(".png") ? "image/png" : "image/jpeg";
              particleAssets.push({ assetFile: base, b64: blob.toString("base64"), mimeType, sourceZip });
              added++;
            }
            console.log(`[ROSTER-GEN] Particle zip ${sourceZip}: ${added} asset(s) indexed`);
            if (DEBUG_PARTICLE_STAGE_A && added > 0) {
              const sampleNames = particleAssets
                .filter(a => a.sourceZip === sourceZip)
                .slice(-Math.min(10, added))
                .map(a => a.assetFile);
              console.log(`[ROSTER-GEN][DEBUG] Particle zip ${sourceZip} sample indexed assets: ${sampleNames.join(", ")}`);
            }

          } else {
            // ── 3D object pack: pair each .obj with its thumbnail ────────
            // Build stem → image entry map for all images in this zip
            const imagesByStem = new Map();
            const allImages    = [];
            for (const entryPath of Object.keys(zip.files)) {
              if (zip.files[entryPath].dir) continue;
              const base  = entryPath.split("/").pop();
              const lower = base.toLowerCase();
              if (base.startsWith("._")) continue; // macOS AppleDouble metadata — not real images
              if (![".png", ".jpg", ".jpeg", ".webp"].some(e => lower.endsWith(e))) continue;
              const stem = lower.replace(/\.[^.]+$/, "");
              imagesByStem.set(stem, { entryPath, base, lower });
              allImages.push({ entryPath, base, lower });
            }

            // Pack-level fallback: first non-colormap image in the zip
            const fallbackEntry = allImages.find(img => !img.lower.includes("colormap")) || null;
            let fallbackB64  = null;
            let fallbackMime = "image/jpeg";
            if (fallbackEntry) {
              const blob  = await zip.files[fallbackEntry.entryPath].async("nodebuffer");
              fallbackB64 = blob.toString("base64");
              fallbackMime = fallbackEntry.lower.endsWith(".png") ? "image/png" : "image/jpeg";
            } else {
              console.warn(`[ROSTER-GEN] ${sourceZip}: no non-colormap image found for fallback thumbnail`);
            }

            // Process each .obj — read its paired image at the same time
            let added = 0;
            for (const entryPath of Object.keys(zip.files)) {
              if (zip.files[entryPath].dir) continue;
              const base  = entryPath.split("/").pop();
              const lower = base.toLowerCase();
              if (!lower.endsWith(".obj")) continue;

              const stem     = lower.replace(/\.obj$/, "");
              const imgEntry = imagesByStem.get(stem) || null;

              let b64, mimeType, thumbFile;
              if (imgEntry && !imgEntry.lower.includes("colormap")) {
                // Same-stem non-colormap image found — use it directly
                const blob = await zip.files[imgEntry.entryPath].async("nodebuffer");
                b64        = blob.toString("base64");
                mimeType   = imgEntry.lower.endsWith(".png") ? "image/png" : "image/jpeg";
                thumbFile  = imgEntry.base;
              } else {
                // Fall back to pack-level non-colormap thumbnail
                b64       = fallbackB64;
                mimeType  = fallbackMime;
                thumbFile = fallbackEntry ? fallbackEntry.base : null;
              }

              if (!b64) {
                console.warn(`[ROSTER-GEN] ${sourceZip}/${base}: no thumbnail available — skipping`);
                continue;
              }

              objectAssets.push({ objFile: base, thumbFile, b64, mimeType, sourceZip });
              added++;
            }
            console.log(`[ROSTER-GEN] Object zip ${sourceZip}: ${added} obj asset(s) indexed`);
          }
        } catch (e) {
          console.warn(`[ROSTER-GEN] Could not process zip ${sourceZip}: ${e.message}`);
        }
      }
    }

    console.log(`[ROSTER-GEN] Asset library ready: ${particleAssets.length} particle textures, ${objectAssets.length} 3D objects`);
    if (DEBUG_PARTICLE_STAGE_A && particleAssets.length > 0) {
      const particleZipCounts = Array.from(
        particleAssets.reduce((m, a) => {
          m.set(a.sourceZip, (m.get(a.sourceZip) || 0) + 1);
          return m;
        }, new Map()).entries()
      );
      console.log("[ROSTER-GEN][DEBUG] Particle asset counts by zip:", particleZipCounts);
      console.log("[ROSTER-GEN][DEBUG] First 25 particle assets:", particleAssets.slice(0, 25).map(a => ({ assetFile: a.assetFile, sourceZip: a.sourceZip, mimeType: a.mimeType, b64Length: a.b64?.length || 0 })));
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
        { type: "text", text: imagePreamble + buildPhase1Prompt(masterPrompt) },
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
    console.log(`[ROSTER-GEN] Phase 1 complete: ${(phase1.particleEffects || []).length} particle effect(s), ${(phase1.objects3d || []).length} 3D object(s) identified`);

    const particleReqs = phase1.particleEffects || [];
    const objectReqs   = phase1.objects3d       || [];
    if (DEBUG_PARTICLE_STAGE_A) {
      console.log("[ROSTER-GEN][DEBUG] Particle requirements:", particleReqs.map((r, i) => ({
        index: i + 1,
        name: r.name,
        visualDescription: r.visualDescription,
        behaviorDescription: r.behaviorDescription || ""
      })));
    }

    // ── 5. Stage A — Visual Library Scan ────────────────────────────────
    /*
       One pass through ALL assets of each type.
       Each batch call sends IMAGES_PER_BATCH images + all requirements.
       Claude returns which requirement indices each image matches.
       Batches run sequentially per asset type; both types run concurrently.

       Result maps:
         particleCandidates: Map<requirementName, asset[]>
         objectCandidates:   Map<requirementName, asset[]>
    */
    console.log("[ROSTER-GEN] Stage A: visual library scan...");

    const particleCandidates = new Map(particleReqs.map(r => [r.name, []]));
    const objectCandidates   = new Map(objectReqs.map(r   => [r.name, []]));

    async function runStageABatches(assets, requirements, candidateMap, isParticle) {
      if (requirements.length === 0 || assets.length === 0) return;

      const batches   = chunkArray(assets, IMAGES_PER_BATCH);
      const assetType = isParticle ? "particle" : "object";
      console.log(`[ROSTER-GEN] Stage A ${assetType}: ${assets.length} assets → ${batches.length} batch(es) of up to ${IMAGES_PER_BATCH}`);

      for (let b = 0; b < batches.length; b++) {
        const batch = batches[b];
        console.log(`[ROSTER-GEN] Stage A ${assetType} batch ${b + 1}/${batches.length} (${batch.length} images)`);
        if (DEBUG_PARTICLE_STAGE_A && isParticle) {
          console.log(`[ROSTER-GEN][DEBUG] Stage A particle batch ${b + 1} asset files:`, batch.map((asset, idx) => ({
            imageIndex: idx + 1,
            assetFile: asset.assetFile,
            sourceZip: asset.sourceZip,
            mimeType: asset.mimeType,
            b64Length: asset.b64?.length || 0
          })));
          console.log(`[ROSTER-GEN][DEBUG] Stage A particle batch ${b + 1} requirements:`, requirements.map((req, idx) => ({
            requirementIndex: idx + 1,
            name: req.name,
            visualDescription: req.visualDescription,
            behaviorDescription: req.behaviorDescription || ""
          })));
        }

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
              { type: "text", text: buildStageAPrompt(requirements, isParticle) },
              ...imageBlocks
            ]
          });
        } catch (e) {
          console.warn(`[ROSTER-GEN] Stage A ${assetType} batch ${b + 1} failed: ${e.message} — skipping`);
          continue;
        }

        if (DEBUG_PARTICLE_STAGE_A && isParticle) {
          console.log(`[ROSTER-GEN][DEBUG] Stage A particle batch ${b + 1} raw response:`, (batchResult.text || "").slice(0, 4000));
        }

        let parsed;
        try {
          parsed = JSON.parse(stripFences(batchResult.text));
        } catch (e) {
          console.warn(`[ROSTER-GEN] Stage A ${assetType} batch ${b + 1} parse failed — skipping: ${e.message}`);
          continue;
        }

        if (DEBUG_PARTICLE_STAGE_A && isParticle) {
          console.log(`[ROSTER-GEN][DEBUG] Stage A particle batch ${b + 1} parsed matches:`, parsed.matches || []);
        }

        // Map Claude's 1-based image indices back to assets in this batch
        for (const match of (parsed.matches || [])) {
          const imgIdx = (match.imageIndex || 1) - 1; // 0-based
          const asset  = batch[imgIdx];
          if (!asset) continue;

          for (const reqIdx of (match.matchesRequirements || [])) {
            const req = requirements[reqIdx - 1]; // 0-based
            if (!req) continue;
            const candidates = candidateMap.get(req.name);
            if (!candidates) continue;

            // Deduplicate by asset filename
            const key = isParticle ? asset.assetFile : asset.objFile;
            if (!candidates.some(c => (isParticle ? c.assetFile : c.objFile) === key)) {
              candidates.push(asset);
            }
          }
        }
      }

      // Log Stage A hit counts per requirement
      for (const [reqName, candidates] of candidateMap) {
        console.log(`[ROSTER-GEN] Stage A ${assetType} "${reqName}": ${candidates.length} candidate(s)`);
        if (DEBUG_PARTICLE_STAGE_A && isParticle) {
          console.log(`[ROSTER-GEN][DEBUG] Stage A particle "${reqName}" candidate asset files:`, candidates.map(c => c.assetFile));
        }
      }
    }

    // Run particle and object Stage A scans concurrently
    await Promise.all([
      runStageABatches(particleAssets, particleReqs, particleCandidates, true),
      runStageABatches(objectAssets,   objectReqs,   objectCandidates,   false)
    ]);

    console.log("[ROSTER-GEN] Stage A complete");
    if (DEBUG_PARTICLE_STAGE_A) {
      const particleStageASummary = Array.from(particleCandidates.entries()).map(([reqName, candidates]) => ({
        reqName,
        count: candidates.length,
        assetFiles: candidates.map(c => c.assetFile)
      }));
      console.log("[ROSTER-GEN][DEBUG] Particle Stage A summary:", particleStageASummary);
    }

    // ── 6. Stage B — Per-Requirement Final Visual Pick ───────────────────
    /*
       For each requirement, send its Stage A candidates to Claude for a
       single final pick. All requirements run in parallel.
       Requirements with no Stage A candidates become unmatched.
    */
    console.log("[ROSTER-GEN] Stage B: per-requirement final visual selection...");

    const gameInterpretation = phase1.gameInterpretationSummary || "";

    async function runStageB(requirementName, requirementDesc, candidates, isParticle) {
      if (candidates.length === 0) {
        console.warn(`[ROSTER-GEN] Stage B: no candidates for "${requirementName}" — unmatched`);
        return null;
      }

      const imageBlocks = candidates.map(c => ({
        type:   "image",
        source: { type: "base64", media_type: c.mimeType, data: c.b64 }
      }));

      let result;
      try {
        result = await callClaude(apiKey, {
          model:       "claude-sonnet-4-20250514",
          maxTokens:   1000,
          system:      "You are a visual asset selection specialist. Respond only with a valid JSON object. No markdown, no fences, no preamble.",
          userContent: [
            { type: "text", text: buildStageBPrompt(requirementName, requirementDesc, candidates, isParticle, gameInterpretation) },
            ...imageBlocks
          ]
        });
      } catch (e) {
        console.warn(`[ROSTER-GEN] Stage B failed for "${requirementName}": ${e.message} — using first candidate`);
        return {
          requirementName,
          selectedAsset:            candidates[0],
          imageNumberChosen:        1,
          visualSelectionRationale: `Fallback: Stage B API error — ${e.message}`,
          colormapFile:             isParticle ? null : "colormap.jpg"
        };
      }

      let parsed;
      try {
        parsed = JSON.parse(stripFences(result.text));
      } catch (e) {
        console.warn(`[ROSTER-GEN] Stage B parse failed for "${requirementName}" — using first candidate`);
        parsed = { imageNumberChosen: 1, visualSelectionRationale: "Fallback: parse error", colormapFile: "colormap.jpg" };
      }

      const chosenIdx = Math.min((parsed.imageNumberChosen || 1) - 1, candidates.length - 1);
      const chosen    = candidates[chosenIdx];

      return {
        requirementName,
        selectedAsset:            chosen,
        imageNumberChosen:        parsed.imageNumberChosen || 1,
        visualSelectionRationale: parsed.visualSelectionRationale || "",
        colormapFile:             parsed.colormapFile || (isParticle ? null : "colormap.jpg")
      };
    }

    const particleDescMap = new Map(particleReqs.map(r => [
      r.name,
      r.visualDescription + (r.behaviorDescription ? ` — ${r.behaviorDescription}` : "")
    ]));
    const objectDescMap = new Map(objectReqs.map(r => [
      r.name,
      r.visualDescription + (r.gameplayRole ? ` — ${r.gameplayRole}` : "")
    ]));

    const [particleResults, objectResults] = await Promise.all([
      Promise.all(particleReqs.map(r =>
        runStageB(r.name, particleDescMap.get(r.name) || "", particleCandidates.get(r.name) || [], true)
      )),
      Promise.all(objectReqs.map(r =>
        runStageB(r.name, objectDescMap.get(r.name) || "", objectCandidates.get(r.name) || [], false)
      ))
    ]);

    console.log(
      `[ROSTER-GEN] Stage B complete: ${particleResults.filter(Boolean).length} particle selections, ` +
      `${objectResults.filter(Boolean).length} object selections`
    );

    // ── 7. Assemble final roster ─────────────────────────────────────────
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
      gameInterpretationSummary: phase1.gameInterpretationSummary || "",
      objects3d,
      textureAssets,
      unmatchedRequirements,
      coverageSummary: {
        totalObjects3d:  objects3d.length,
        totalTextures:   textureAssets.length,
        totalUnmatched:  unmatchedRequirements.length,
        limitsRespected: objects3d.length <= MAX_OBJ_ASSETS && textureAssets.length <= MAX_PNG_ASSETS,
        coverageNotes:   `${objects3d.length} objects and ${textureAssets.length} particle textures selected via visual-only pipeline.`
      },
      visualDirectionNotes: {}
    };

    roster._phase1Analysis = phase1;
    enforceHardLimits(roster);

    roster._meta = {
      jobId,
      generatedAt:         Date.now(),
      totalParticleAssets: particleAssets.length,
      totalObjectAssets:   objectAssets.length,
      imageCount:          refImageBlocks.length,
      approved:            false
    };

    // ── 8. Save pending roster to Firebase ──────────────────────────────
    await bucket.file(`${projectPath}/ai_asset_roster_pending.json`).save(
      JSON.stringify(roster, null, 2),
      { contentType: "application/json", resumable: false }
    );

    console.log(
      `[ROSTER-GEN] Complete. Objects: ${(roster.objects3d || []).length}, ` +
      `Textures: ${(roster.textureAssets || []).length}, ` +
      `Unmatched: ${(roster.unmatchedRequirements || []).length}`
    );

    return { statusCode: 202, body: "" };

  } catch (error) {
    console.error("[ROSTER-GEN] Unhandled error:", error);
    if (bucket && projectPath) {
      try {
        await bucket.file(`${projectPath}/ai_asset_roster_error.json`).save(
          JSON.stringify({ error: error.message, failedAt: Date.now() }),
          { contentType: "application/json", resumable: false }
        );
      } catch (e) { /* non-fatal */ }
    }
    return { statusCode: 202, body: "" };
  }
};
