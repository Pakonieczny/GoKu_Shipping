/* netlify/functions/claudeRosterGenerate-background.js */
/* ═══════════════════════════════════════════════════════════════════
   GAME-SPECIFIC ASSET ROSTER GENERATION — v4.0 (Two-Phase)
   ─────────────────────────────────────────────────────────────────
   Background Netlify function (suffix -background = 15-min timeout).
   Returns 202 immediately. Writes result to Firebase when done.
   Frontend polls ai_asset_roster_pending.json to detect completion.

   Flow:
     1. Read Master Prompt + inline images from ai_request.json
     2. Scan asset_particle_textures/ and asset_3d_objects/ for .docx
        and .zip files (co-located in the same folder per pack)
     3. PHASE 1 — Claude analyzes the game prompt + reference images
        and produces a structured list of required particle effects
        and 3D objects (visual descriptions only, no asset names)
     4. PHASE 2A — Claude reads all docx AI descriptions (text only)
        and for each Phase 1 requirement shortlists 8-12 plausible
        candidate assets. Wide net, no visual check yet.
     5. PHASE 2B — For each requirement, the shortlisted candidate
        thumbnails are built from the co-located asset zip files and sent
        to Claude as vision inputs alongside the reference game images.
        Claude makes the final pick purely by visual match.
        Runs all requirements in parallel.
     6. Thumbnails of selected assets are embedded in the roster JSON
        so the UI can display them in the review panel.
     7. Validate hard limits (enforceHardLimits)
     8. Save roster as ai_asset_roster_pending.json in Firebase

   Request body: { projectPath, jobId }
   Response:     202 Accepted (background function — no body)
   ═══════════════════════════════════════════════════════════════════ */

const fetch  = require("node-fetch");
const admin  = require("./firebaseAdmin");
const JSZip  = require("jszip");

/* ─── Retry helpers (mirrors claudeCodeProxy-background.js) ────── */
const CLAUDE_OVERLOAD_MAX_RETRIES   = 5;
const CLAUDE_OVERLOAD_BASE_DELAY_MS = 1250;
const CLAUDE_OVERLOAD_MAX_DELAY_MS  = 12000;

const MAX_OBJ_ASSETS = 25;
const MAX_PNG_ASSETS = 50;

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

function computeRetryDelay(attempt) {
  return Math.min(
    CLAUDE_OVERLOAD_BASE_DELAY_MS * Math.pow(2, Math.max(0, attempt - 1)),
    CLAUDE_OVERLOAD_MAX_DELAY_MS
  ) + Math.floor(Math.random() * 700);
}

function isOverload(status, msg = "") {
  const m = String(msg).toLowerCase();
  if ([429,500,502,503,504,529].includes(Number(status))) return true;
  if (
    m.includes("econnreset")     ||
    m.includes("econnrefused")   ||
    m.includes("etimedout")      ||
    m.includes("enotfound")      ||
    m.includes("socket hang up") ||
    m.includes("network error")  ||
    m.includes("fetch failed")
  ) return true;
  return m.includes("overloaded") || m.includes("rate limit") ||
         m.includes("too many requests") || m.includes("capacity") ||
         m.includes("temporarily unavailable");
}

async function callClaude(apiKey, { model, maxTokens, system, userContent }) {
  const body = { model, max_tokens: maxTokens, system,
                 messages: [{ role: "user", content: userContent }] };
  let last;
  for (let i = 1; i <= CLAUDE_OVERLOAD_MAX_RETRIES; i++) {
    try {
      const res  = await fetch("https://api.anthropic.com/v1/messages", {
        method: "POST",
        headers: { "Content-Type": "application/json",
                   "x-api-key": apiKey, "anthropic-version": "2023-06-01" },
        body: JSON.stringify(body)
      });
      const raw  = await res.text();
      const data = raw ? JSON.parse(raw) : null;
      if (!res.ok) {
        const msg = data?.error?.message || `Claude error (${res.status})`;
        const err = Object.assign(new Error(msg), { status: res.status,
          isRetryableOverload: isOverload(res.status, msg) });
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

/* ─── Extract plain text from a docx buffer using JSZip ─────── */
/* Reads word/document.xml and strips all XML tags. Crude but
   reliable for content that's all normal paragraphs / tables.   */
async function extractDocxText(buffer) {
  try {
    const zip  = await JSZip.loadAsync(buffer);
    const xml  = zip.file("word/document.xml");
    if (!xml) return "(empty docx)";
    const xmlText = await xml.async("string");
    // Replace paragraph and line-break tags with newlines, strip all other tags
    return xmlText
      .replace(/<w:br[^>]*\/>/gi, "\n")
      .replace(/<\/w:p>/gi, "\n")
      .replace(/<[^>]+>/g, " ")
      .replace(/&amp;/g, "&").replace(/&lt;/g, "<").replace(/&gt;/g, ">")
      .replace(/&quot;/g, '"').replace(/&#x[0-9A-Fa-f]+;/g, " ")
      .replace(/[ \t]+/g, " ")
      .replace(/\n{3,}/g, "\n\n")
      .trim();
  } catch (e) {
    return `(could not parse docx: ${e.message})`;
  }
}

/* ─── Strip JSON fences ──────────────────────────────────────── */
function stripFences(text) {
  let t = text
    .replace(/^```json\s*/i, "").replace(/^```\s*/i, "").replace(/\s*```$/i, "").trim();
  const a = t.indexOf("{"), b = t.lastIndexOf("}");
  if (a > 0 && b > a) t = t.substring(a, b + 1);
  return t.trim();
}

/* ─── Enforce hard selection limits ─────────────────────────── */
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
    roster.coverageSummary.totalTextures   = (roster.textureAssets || []).length;
    roster.coverageSummary.limitsRespected =
      roster.coverageSummary.totalObjects3d <= MAX_OBJ_ASSETS &&
      roster.coverageSummary.totalTextures   <= MAX_PNG_ASSETS;
  }
  return roster;
}


/* ─── Phase 1 prompt: game visual needs analysis ─────────────── */
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

/* ─── Phase 2A prompt: text-only shortlist ───────────────────── */
function buildPhase2APrompt(phase1Result, particleDocsBlock, objectDocsBlock) {
  return `You are a game asset shortlisting specialist. For each visual requirement, identify the best 8-12 candidate assets from the catalogs using ONLY the AI text descriptions. Do not make final picks yet — cast a wide net of plausible matches.

RULES:
- For each particle effect requirement, list 8-12 candidate filenames from the PARTICLE TEXTURE CATALOG whose descriptions are plausibly relevant.
- For each 3D object requirement, list 8-12 candidate filenames from the 3D OBJECT CATALOG whose descriptions are plausibly relevant.
- Use ONLY the AI-oriented description and category text to judge relevance. Do NOT use filenames as a criterion.
- Include candidates that are similar, adjacent, or loosely relevant — err on the side of inclusion.
- assetName and sourceRosterDocument must be copied exactly as shown in the catalog.

GAME VISUAL REQUIREMENTS:
${JSON.stringify(phase1Result, null, 2)}

PARTICLE TEXTURE CATALOG:
${particleDocsBlock || "(No particle texture packs found)"}

3D OBJECT CATALOG:
${objectDocsBlock || "(No 3D object packs found)"}

Respond ONLY with a valid JSON object. No markdown, no fences, no preamble.

{
  "particleCandidates": [
    {
      "requirementName": "name from particleEffects in phase 1",
      "candidates": [
        { "assetName": "exact_filename.png", "sourceRosterDocument": "ExactDocxName.docx", "descriptionMatch": "one sentence why this description is relevant" }
      ]
    }
  ],
  "objectCandidates": [
    {
      "requirementName": "name from objects3d in phase 1",
      "candidates": [
        { "assetName": "exact_filename.obj", "sourceRosterDocument": "ExactDocxName.docx", "descriptionMatch": "one sentence why this description is relevant" }
      ]
    }
  ]
}`;
}

/* ─── Phase 2B prompt: visual final selection ────────────────── */
function buildPhase2BPrompt(requirementName, requirementDesc, candidateLabels, isParticle, gameInterpretation, imagePreamble) {
  return `${imagePreamble}You are making the final asset selection for a game. You have been given thumbnail images of candidate assets. Your job is to pick the single best match for the requirement below based on what you can see in the thumbnails.

GAME CONTEXT:
${gameInterpretation}

REQUIREMENT:
Name: ${requirementName}
Description: ${requirementDesc}
Type: ${isParticle ? 'Particle Effect Texture' : '3D Object'}

CANDIDATE THUMBNAILS:
The images attached (in order) correspond to these candidates:
${candidateLabels.map((c, i) => `  Image ${i + 1}: ${c.assetName} (${c.sourceRosterDocument})`).join('\n')}

SELECTION RULES:
- Judge purely by visual appearance of the thumbnail vs the requirement description.
- For particle textures: consider the shape silhouette, density, edge softness, and whether it matches the effect type.
- For 3D objects: consider the overall shape, silhouette, and whether it matches the intended role.
- Pick exactly one winner. State which image number you chose and why.
- For 3D objects only: include a colormapFile field. Default to "colormap.jpg" unless the candidate clearly indicates a different color texture filename.

Respond ONLY with a valid JSON object. No markdown, no fences, no preamble.

{
  "requirementName": "${requirementName}",
  "selectedAssetName": "exact_filename_from_candidates",
  "selectedSourceRosterDocument": "ExactDocxName.docx",
  "imageNumberChosen": 1,
  "visualSelectionRationale": "What you saw in the thumbnail that matched the requirement",
  "colormapFile": "colormap.jpg"
}`;
}

/* ─── Build thumbnail catalog from asset zip files ───────────── */
/*
   Previous approach: extracted thumbnails from inside the .docx by trying
   to pair Word image blips with filename text cells by positional index.
   This was unreliable — Word XML interleaves decorative/header images with
   content images, so the index assumption breaks and the catalog came out
   empty or mismatched, causing 0 Phase 2B hits.

   New approach: the .zip co-located with each .docx already contains the
   actual asset files (the .png textures and .obj models). For particle
   packs, the zip contains image assets directly, so each image is used as
   its own thumbnail. For 3D object packs, NEVER use the colormap image as
   the visual stand-in. Instead, use the second available image in the zip
   folder as the shared thumbnail stand-in for every .obj in that pack.

   Keys are lowercase asset filenames, exactly matching what Claude is
   instructed to output in Phase 2A — so Phase 2B lookups will actually hit.
*/
async function buildThumbnailCatalogFromZips(folderData) {
  const catalog = new Map();

  for (const { packType, docxResults, zipFileIndex } of folderData) {
    for (const { baseName: docxBaseName } of docxResults) {
      const zipEntry = zipFileIndex.get(docxBaseName);
      if (!zipEntry || !zipEntry._zipBuffer) continue;

      try {
        const zip = await JSZip.loadAsync(zipEntry._zipBuffer);

        if (packType === "particle_texture") {
          // Each .png in the zip IS an asset — use it directly as its own thumbnail
          for (const entryPath of Object.keys(zip.files)) {
            if (zip.files[entryPath].dir) continue;
            const base  = entryPath.split("/").pop();
            const lower = base.toLowerCase();
            if (![".png",".jpg",".jpeg",".webp"].some(e => lower.endsWith(e))) continue;

            const blob     = await zip.files[entryPath].async("nodebuffer");
            const mimeType = lower.endsWith(".png") ? "image/png" : "image/jpeg";
            catalog.set(lower, { b64: blob.toString("base64"), mimeType, sourceDoc: docxBaseName });
          }

        } else {
          // 3D object pack — NEVER use the colormap image as the visual stand-in.
          // Use the second available image in the zip as the shared thumbnail for
          // every .obj in this pack. If fewer than two images exist, leave the
          // thumbnail null and let downstream fuzzy / fallback handling decide.
          const imageEntries = Object.keys(zip.files)
            .filter(p => {
              if (zip.files[p].dir) return false;
              const l = p.split("/").pop().toLowerCase();
              return [".png",".jpg",".jpeg",".webp"].some(e => l.endsWith(e));
            })
            .sort((a, b) => a.localeCompare(b, undefined, { sensitivity: "base", numeric: true }));

          const standInEntry = imageEntries.length >= 2 ? imageEntries[1] : null;

          let thumbB64   = null;
          let thumbMime  = "image/jpeg";
          if (standInEntry) {
            const blob = await zip.files[standInEntry].async("nodebuffer");
            thumbB64   = blob.toString("base64");
            thumbMime  = standInEntry.toLowerCase().endsWith(".png") ? "image/png" : "image/jpeg";
          } else {
            console.warn(`[ROSTER-GEN] ${docxBaseName}: 3D object zip has fewer than 2 images; no stand-in thumbnail assigned`);
          }

          // Register every .obj filename in this pack against the shared stand-in thumb
          for (const entryPath of Object.keys(zip.files)) {
            if (zip.files[entryPath].dir) continue;
            const base  = entryPath.split("/").pop();
            if (!base.toLowerCase().endsWith(".obj")) continue;
            catalog.set(base.toLowerCase(), {
              b64:       thumbB64,
              mimeType:  thumbMime,
              sourceDoc: docxBaseName
            });
          }
        }

        console.log(`[ROSTER-GEN] Thumbnail catalog after ${docxBaseName}: ${catalog.size} entries`);
      } catch (e) {
        console.warn(`[ROSTER-GEN] Could not build thumbnails from zip for ${docxBaseName}: ${e.message}`);
      }
    }
  }

  return catalog;
}

/* ═══════════════════════════════════════════════════════════════ */
exports.handler = async (event) => {
  let projectPath = null;
  let bucket = null;

  try {
    if (!event.body) return { statusCode: 400, body: '' };

    const body = JSON.parse(event.body);
    const { jobId } = body;
    projectPath = body.projectPath;
    if (!projectPath || !jobId) return { statusCode: 400, body: '' };

    const apiKey = process.env.ANTHROPIC_API_KEY;
    if (!apiKey) throw new Error("Missing ANTHROPIC_API_KEY");

    bucket = admin.storage().bucket(
      process.env.FIREBASE_STORAGE_BUCKET || "gokudatabase.firebasestorage.app"
    );

    console.log(`[ROSTER-GEN] Starting for project ${projectPath}, job ${jobId}`);

    // ── 1. Load Master Prompt + inline images from ai_request.json ──
    const requestFile = bucket.file(`${projectPath}/ai_request.json`);
    const [reqExists] = await requestFile.exists();
    if (!reqExists) return err400("ai_request.json not found. Submit prompt first.");
    const [reqContent] = await requestFile.download();
    const { prompt: masterPrompt, inlineImages = [] } = JSON.parse(reqContent.toString());
    if (!masterPrompt) return err400("No prompt found in ai_request.json");

    // ── 2. Load .docx and .zip files from both asset folders ─────────
    const ASSET_FOLDERS = [
      { prefix: `${projectPath}/asset_particle_textures/`, packType: "particle_texture" },
      { prefix: `${projectPath}/asset_3d_objects/`,        packType: "3d_object"        }
    ];

    const folderData = [];

    for (const { prefix, packType } of ASSET_FOLDERS) {
      let folderFiles;
      try {
        [folderFiles] = await bucket.getFiles({ prefix });
      } catch (e) {
        console.warn(`[ROSTER-GEN] Could not list ${prefix}: ${e.message}`);
        folderData.push({ packType, docxResults: [], zipFileIndex: new Map() });
        continue;
      }

      // ── 2a. Extract text from every .docx in this folder ──
      const docxFiles = (folderFiles || []).filter(f => f.name.toLowerCase().endsWith(".docx"));
      console.log(`[ROSTER-GEN] ${packType}: found ${docxFiles.length} docx file(s) in ${prefix}`);

      const docxResults = await Promise.all(docxFiles.map(async (docFile) => {
        const baseName = docFile.name.split("/").pop();
        try {
          const [buf] = await docFile.download();
          const text  = await extractDocxText(buf);
          return { baseName, text };
        } catch (e) {
          console.warn(`[ROSTER-GEN] Could not read ${baseName}: ${e.message}`);
          return { baseName, text: `(Could not extract text: ${e.message})` };
        }
      }));

      // ── 2b. Index filenames from every .zip in this same folder ──
      const zipFileIndex = new Map();
      const zipFiles = (folderFiles || []).filter(f => f.name.toLowerCase().endsWith(".zip"));

      await Promise.all(zipFiles.map(async (zipFile) => {
        const zipBaseName  = zipFile.name.split("/").pop();
        const docxBaseName = zipBaseName.replace(/\.zip$/i, ".docx");
        try {
          const [zipBuffer] = await zipFile.download();
          const zip = await JSZip.loadAsync(zipBuffer);
          const objFiles   = [];
          const imageFiles = [];
          for (const entryPath of Object.keys(zip.files)) {
            if (zip.files[entryPath].dir) continue;
            const base  = entryPath.split("/").pop();
            const lower = base.toLowerCase();
            if (lower.endsWith(".obj")) objFiles.push(base);
            else if ([".png",".jpg",".jpeg",".webp"].some(e => lower.endsWith(e))) imageFiles.push(base);
          }
          objFiles.sort();
          imageFiles.sort();
          zipFileIndex.set(docxBaseName, { objFiles, imageFiles, _zipBuffer: zipBuffer });
          console.log(`[ROSTER-GEN] Indexed ${zipBaseName} (${packType}): ${objFiles.length} obj, ${imageFiles.length} image(s)`);
        } catch (e) {
          console.warn(`[ROSTER-GEN] Could not index zip ${zipBaseName}: ${e.message}`);
        }
      }));

      folderData.push({ packType, docxResults, zipFileIndex });
    }

    // ── 3. Build catalog blocks for Phase 2 ─────────────────────────
    // Each catalog block = docx text content + exact available filenames
    // from the co-located zip. Filenames are listed for output accuracy
    // only — matching must be driven by AI descriptions.
    function buildCatalogBlock(folderEntry) {
      return folderEntry.docxResults.map(d => {
        const fileList = (() => {
          if (!folderEntry.zipFileIndex.has(d.baseName)) return "";
          const { objFiles, imageFiles } = folderEntry.zipFileIndex.get(d.baseName);
          const lines = [];
          if (objFiles.length   > 0) lines.push(`EXACT FILENAMES IN ZIP (use verbatim for assetName — do NOT use as matching criterion):\n${objFiles.map(f => `  - ${f}`).join("\n")}`);
          if (imageFiles.length > 0) lines.push(`EXACT FILENAMES IN ZIP (use verbatim for assetName — do NOT use as matching criterion):\n${imageFiles.map(f => `  - ${f}`).join("\n")}`);
          return lines.join("\n\n");
        })();
        return `\n=== PACK: ${d.baseName} ===\n${d.text}${fileList ? "\n\n" + fileList : ""}\n=== END: ${d.baseName} ===\n`;
      }).join("");
    }

    const particleFolderData = folderData.find(f => f.packType === "particle_texture") || { docxResults: [], zipFileIndex: new Map() };
    const objectFolderData   = folderData.find(f => f.packType === "3d_object")        || { docxResults: [], zipFileIndex: new Map() };
    const particleDocsBlock  = buildCatalogBlock(particleFolderData);
    const objectDocsBlock    = buildCatalogBlock(objectFolderData);

    // ── 4. Build image content blocks ───────────────────────────────
    const imageBlocks = [];
    for (const img of inlineImages) {
      if (img.data && img.mimeType && img.mimeType.startsWith("image/")) {
        imageBlocks.push({
          type: "image",
          source: { type: "base64", media_type: img.mimeType, data: img.data }
        });
      }
    }
    if (imageBlocks.length > 0) {
      console.log(`[ROSTER-GEN] Loaded ${imageBlocks.length} reference image(s)`);
    }
    const imagePreamble = imageBlocks.length > 0
      ? `\nREFERENCE IMAGES: ${imageBlocks.length} gameplay reference image(s) are attached. ` +
        `They carry authority equal to the Master Prompt. Use them to infer visual style, ` +
        `environment type, entity types, color palette, and particle FX requirements.\n\n`
      : "";

    // ── 5. Phase 1 — Game Visual Needs Analysis ──────────────────────
    console.log("[ROSTER-GEN] Phase 1: analyzing game visual requirements...");
    const phase1Result = await callClaude(apiKey, {
      model:      "claude-sonnet-4-20250514",
      maxTokens:  4000,
      system:     "You are a game visual requirements analyst. Respond only with a valid JSON object. No markdown, no fences, no preamble.",
      userContent: [
        { type: "text", text: imagePreamble + buildPhase1Prompt(masterPrompt) },
        ...imageBlocks
      ]
    });

    let phase1;
    try {
      phase1 = JSON.parse(stripFences(phase1Result.text));
    } catch (e) {
      console.error("[ROSTER-GEN] Phase 1 JSON parse failed:", phase1Result.text.slice(0, 500));
      return err500(`Phase 1 returned unparseable JSON: ${e.message}`);
    }
    console.log(`[ROSTER-GEN] Phase 1 complete: ${(phase1.particleEffects||[]).length} particle effect(s), ${(phase1.objects3d||[]).length} 3D object(s) identified`);

    // ── 6. Phase 2A — Text-Only Shortlist ────────────────────────────
    console.log("[ROSTER-GEN] Phase 2A: text-based candidate shortlisting...");
    const phase2AResult = await callClaude(apiKey, {
      model:     "claude-sonnet-4-20250514",
      maxTokens: 8000,
      system:    "You are a game asset shortlisting specialist. Respond only with a valid JSON object. No markdown, no fences, no preamble.",
      userContent: [
        { type: "text", text: buildPhase2APrompt(phase1, particleDocsBlock, objectDocsBlock) }
      ]
    });

    let phase2A;
    try {
      phase2A = JSON.parse(stripFences(phase2AResult.text));
    } catch (e) {
      console.error("[ROSTER-GEN] Phase 2A JSON parse failed:", phase2AResult.text.slice(0, 500));
      return err500(`Phase 2A returned unparseable JSON: ${e.message}`);
    }
    console.log(`[ROSTER-GEN] Phase 2A complete: ${(phase2A.particleCandidates||[]).length} particle groups, ${(phase2A.objectCandidates||[]).length} object groups`);

    // ── 7. Build thumbnail catalog from zip files ─────────────────────
    // Uses the zip buffers already downloaded in step 2 — no extra network
    // calls needed.  Keys are lowercase asset filenames that exactly match
    // what Claude outputs in Phase 2A, so Phase 2B lookups will resolve.
    console.log("[ROSTER-GEN] Building thumbnail catalog from zip files...");
    const thumbnailCatalog = await buildThumbnailCatalogFromZips(folderData);
    console.log(`[ROSTER-GEN] Thumbnail catalog ready: ${thumbnailCatalog.size} entries`);

    // ── 8. Phase 2B — Visual Final Selection ─────────────────────────
    // For each requirement group, send candidate thumbnails derived from the
    // co-located asset zip files to Claude vision and get the final pick.
    // Run all groups in parallel.
    console.log("[ROSTER-GEN] Phase 2B: visual final selection...");

    const gameInterpretation = phase1.gameInterpretationSummary || "";

    async function runPhase2B(requirementName, requirementDesc, candidates, isParticle) {
      // Resolve thumbnails for each candidate
      const resolved = candidates.map(c => ({
        ...c,
        thumb: thumbnailCatalog.get(c.assetName.toLowerCase()) || null
      })).filter(c => c.thumb !== null);

      if (resolved.length === 0) {
        // Phase 2A candidates didn't match any real catalog keys.
        // This usually means Claude used slightly wrong filenames.
        // Fallback: fuzzy-scan the thumbnail catalog for any key that shares
        // meaningful tokens with the requirement name or any candidate name.
        console.warn(`[ROSTER-GEN] Phase 2B: no thumbnails for "${requirementName}" — attempting fuzzy catalog fallback`);

        const reqTokens = requirementName.toLowerCase().replace(/_/g, " ").split(/\s+/);
        const candidateTokens = candidates.flatMap(c =>
          c.assetName.toLowerCase().replace(/[_\-\.]/g, " ").split(/\s+/)
        );
        const allTokens = [...new Set([...reqTokens, ...candidateTokens])].filter(t => t.length > 2);

        let bestKey   = null;
        let bestScore = 0;
        for (const [key] of thumbnailCatalog) {
          const score = allTokens.filter(t => key.includes(t)).length;
          if (score > bestScore) { bestScore = score; bestKey = key; }
        }

        if (bestKey) {
          const hit = thumbnailCatalog.get(bestKey);
          console.log(`[ROSTER-GEN] Fuzzy fallback for "${requirementName}": using "${bestKey}" (score ${bestScore})`);
          return {
            requirementName,
            selectedAssetName:            bestKey,
            selectedSourceRosterDocument: hit.sourceDoc,
            imageNumberChosen:            1,
            visualSelectionRationale:     `Fuzzy fallback — no exact thumbnail match for Phase 2A candidates; selected by keyword similarity to requirement`
          };
        }

        // Absolute last resort — first entry in catalog
        const firstKey = thumbnailCatalog.keys().next().value;
        if (firstKey) {
          const hit = thumbnailCatalog.get(firstKey);
          return {
            requirementName,
            selectedAssetName:            firstKey,
            selectedSourceRosterDocument: hit.sourceDoc,
            imageNumberChosen:            1,
            visualSelectionRationale:     `Last-resort fallback — no thumbnail or keyword match found`
          };
        }

        return null; // catalog is completely empty
      }

      const thumbImageBlocks = resolved.map(c => ({
        type: "image",
        source: { type: "base64", media_type: c.thumb.mimeType, data: c.thumb.b64 }
      }));

      // Include reference game images for context
      const refImageBlocks = imageBlocks.slice(0, 2); // max 2 ref images to save tokens
      const refPreamble = refImageBlocks.length > 0
        ? `\nREFERENCE GAME IMAGES: ${refImageBlocks.length} game reference image(s) are attached first, followed by the candidate thumbnails.\n\n`
        : "";

      const result = await callClaude(apiKey, {
        model:     "claude-sonnet-4-20250514",
        maxTokens: 1000,
        system:    "You are a visual asset selection specialist. Respond only with a valid JSON object. No markdown, no fences, no preamble.",
        userContent: [
          { type: "text", text: buildPhase2BPrompt(requirementName, requirementDesc, resolved, isParticle, gameInterpretation, refPreamble) },
          ...refImageBlocks,
          ...thumbImageBlocks
        ]
      });

      try {
        return JSON.parse(stripFences(result.text));
      } catch (e) {
        console.warn(`[ROSTER-GEN] Phase 2B parse failed for ${requirementName}: ${e.message}`);
        // Fall back to first resolved candidate
        return {
          requirementName,
          selectedAssetName:            resolved[0].assetName,
          selectedSourceRosterDocument: resolved[0].sourceRosterDocument,
          imageNumberChosen:            1,
          visualSelectionRationale:     "Fallback: parse error on visual selection",
          colormapFile:                 isParticle ? null : "colormap.jpg"
        };
      }
    }

    // Build requirement descriptions map from Phase 1
    const particleDescMap = new Map((phase1.particleEffects || []).map(e => [e.name, e.visualDescription + " — " + (e.behaviorDescription || "")]));
    const objectDescMap   = new Map((phase1.objects3d       || []).map(e => [e.name, e.visualDescription + " — " + (e.gameplayRole || "")]));

    // Run all Phase 2B calls in parallel
    const [particleResults, objectResults] = await Promise.all([
      Promise.all((phase2A.particleCandidates || []).map(group =>
        runPhase2B(group.requirementName, particleDescMap.get(group.requirementName) || "", group.candidates || [], true)
      )),
      Promise.all((phase2A.objectCandidates || []).map(group =>
        runPhase2B(group.requirementName, objectDescMap.get(group.requirementName) || "", group.candidates || [], false)
      ))
    ]);

    console.log(`[ROSTER-GEN] Phase 2B complete: ${particleResults.filter(Boolean).length} particle selections, ${objectResults.filter(Boolean).length} object selections`);

    // ── 9. Assemble final roster from Phase 2B results ────────────────
    // Map Phase 2B winners back to full asset entries, embed thumbnails
    function assembleAsset(phase2bResult, isParticle, phase1Req) {
      if (!phase2bResult) return null;
      const thumb = thumbnailCatalog.get(phase2bResult.selectedAssetName.toLowerCase());
      const p1    = phase1Req || {};

      if (isParticle) {
        return {
          assetName:              phase2bResult.selectedAssetName,
          sourceRosterDocument:   phase2bResult.selectedSourceRosterDocument,
          intendedUsage:          `Particle effect: ${phase2bResult.requirementName}`,
          particleEffectTarget:   phase2bResult.requirementName,
          matchedRequirement:     phase2bResult.requirementName,
          selectionRationale:     phase2bResult.visualSelectionRationale,
          thumbnailB64:           thumb ? thumb.b64 : null,
          thumbnailMime:          thumb ? thumb.mimeType : null
        };
      } else {
        return {
          assetName:              phase2bResult.selectedAssetName,
          sourceRosterDocument:   phase2bResult.selectedSourceRosterDocument,
          intendedRole:           p1.gameplayRole || p1.visualDescription || phase2bResult.requirementName || "",
          matchedRequirement:     phase2bResult.requirementName,
          selectionRationale:     phase2bResult.visualSelectionRationale,
          colormapFile:           phase2bResult.colormapFile || "colormap.jpg",
          colormapConfidence:     phase2bResult.colormapFile ? "HIGH" : "MEDIUM",
          thumbnailB64:           thumb ? thumb.b64 : null,
          thumbnailMime:          thumb ? thumb.mimeType : null
        };
      }
    }

    const phase1ParticleMap = new Map((phase1.particleEffects || []).map(e => [e.name, e]));
    const phase1ObjectMap   = new Map((phase1.objects3d       || []).map(e => [e.name, e]));

    const textureAssets    = particleResults.filter(Boolean).map(r => assembleAsset(r, true,  phase1ParticleMap.get(r.requirementName))).filter(Boolean);
    const objects3d = objectResults.filter(Boolean).map(r  => assembleAsset(r, false, phase1ObjectMap.get(r.requirementName))).filter(Boolean);

    // Determine unmatched requirements
    const matchedParticleNames = new Set(textureAssets.map(a => a.matchedRequirement));
    const matchedObjectNames   = new Set(objects3d.map(a => a.matchedRequirement));
    const unmatchedRequirements = [
      ...(phase1.particleEffects || []).filter(e => !matchedParticleNames.has(e.name)).map(e => ({
        requirementName: e.name, type: "particle_effect", reason: "No candidates found or thumbnail unavailable"
      })),
      ...(phase1.objects3d || []).filter(e => !matchedObjectNames.has(e.name)).map(e => ({
        requirementName: e.name, type: "object_3d", reason: "No candidates found or thumbnail unavailable"
      }))
    ];

    const vn = {};
    const roster = {
      documentTitle:            `Game-Specific Asset Roster`,
      gameInterpretationSummary: phase1.gameInterpretationSummary || "",
      objects3d,
      textureAssets,
      unmatchedRequirements,
      coverageSummary: {
        totalObjects3d: objects3d.length,
        totalTextures:   textureAssets.length,
        totalUnmatched:  unmatchedRequirements.length,
        limitsRespected: objects3d.length <= MAX_OBJ_ASSETS && textureAssets.length <= MAX_PNG_ASSETS,
        coverageNotes:   `${objects3d.length} objects and ${textureAssets.length} particle textures matched via visual confirmation.`
      },
      visualDirectionNotes: vn
    };

    // Attach phase 1 analysis for UI display
    roster._phase1Analysis = phase1;
    enforceHardLimits(roster);

    // Collect all docx names seen across both folders for metadata
    const allDocxNames = folderData.flatMap(f => f.docxResults.map(d => d.baseName));

    roster._meta = {
      jobId,
      generatedAt:        Date.now(),
      availableRosterDocs: allDocxNames,
      imageCount:         imageBlocks.length,
      approved:           false
    };

    // ── 7. Save pending roster to Firebase ───────────────────────────
    await bucket.file(`${projectPath}/ai_asset_roster_pending.json`).save(
      JSON.stringify(roster, null, 2),
      { contentType: "application/json", resumable: false }
    );

    console.log(
      `[ROSTER-GEN] Complete. Objects: ${(roster.objects3d||[]).length}, ` +
      `Textures: ${(roster.textureAssets||[]).length}, ` +
      `Unmatched: ${(roster.unmatchedRequirements||[]).length}`
    );

    return { statusCode: 202, body: '' };

  } catch (error) {
    console.error("[ROSTER-GEN] Unhandled error:", error);
    if (bucket && projectPath) {
      try {
        await bucket.file(`${projectPath}/ai_asset_roster_error.json`).save(
          JSON.stringify({ error: error.message, failedAt: Date.now() }),
          { contentType: "application/json", resumable: false }
        );
      } catch(e) { /* non-fatal */ }
    }
    return { statusCode: 202, body: '' };
  }
};


