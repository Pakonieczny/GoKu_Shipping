/* netlify/functions/claudeRosterGenerate-background.js */
/* ═══════════════════════════════════════════════════════════════════
   GAME-SPECIFIC ASSET ROSTER GENERATION — v5.1 (Phase 1 Only)
   ─────────────────────────────────────────────────────────────────
   Background Netlify function (suffix -background = 15-min timeout).
   Returns 202 immediately. Writes result to Firebase when done.
   Frontend polls ai_asset_roster_phase1.json to detect completion.

   Flow:
     1. Read Master Prompt + inline images from ai_request.json.
     2. PHASE 1 — Claude analyzes the game prompt + gameplay reference
        images and produces a structured list of required particle effects
        and 3D objects (visual descriptions only, no asset names).
     3. Save the Phase 1 payload as ai_asset_roster_phase1.json.
     4. Frontend collects one user reference image per required 3D object.
     5. claudeRosterStageAB-background performs the single zip-library scan
        and completes Stage A/B matching.

   Request body: { projectPath, jobId }
   Response:     202 Accepted (background function — no body)
   ═══════════════════════════════════════════════════════════════════ */

const fetch = require("node-fetch");
const admin = require("./firebaseAdmin");

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
  return m.includes("overloaded")            ||
         m.includes("rate limit")            ||
         m.includes("too many requests")     ||
         m.includes("capacity")              ||
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

/* ─── Utilities ──────────────────────────────────────────────────── */
function stripFences(text) {
  let t = text
    .replace(/^```json\s*/i, "").replace(/^```\s*/i, "").replace(/\s*```$/i, "").trim();
  const a = t.indexOf("{"), b = t.lastIndexOf("}");
  if (a > 0 && b > a) t = t.substring(a, b + 1);
  return t.trim();
}
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

    // ── 3. Phase 1 — Game Visual Needs Analysis ──────────────────────────
    // Phase 1 is intentionally scan-free. Zip-library scanning now happens
    // only once, inside claudeRosterStageAB-background, after the frontend
    // collects one user reference image per required 3D object.
    // Phase 1 runs here and writes ai_asset_roster_phase1.json to Firebase.
    // The frontend polls for that file, shows the reference image drop modal,
    // then calls claudeRosterStageAB-background to run Stage A/B with the
    // user-provided reference images driving object matching.
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

    // ── 4. Write Phase 1 result to Firebase for frontend to poll ─────────
    // The frontend will show the reference image modal, collect one image per
    // 3D object, upload them as ai_roster_ref_images.json, then call
    // claudeRosterStageAB-background to run Stage A/B.
    const phase1Payload = {
      phase1,
      jobId,
      generatedAt: Date.now(),
      masterPromptSnippet: masterPrompt.slice(0, 120)
    };

    await bucket.file(`${projectPath}/ai_asset_roster_phase1.json`).save(
      JSON.stringify(phase1Payload, null, 2),
      { contentType: "application/json", resumable: false }
    );

    console.log(`[ROSTER-GEN] Phase 1 written to Firebase. Objects: ${(phase1.objects3d || []).length}, Particles: ${(phase1.particleEffects || []).length}. Waiting for user reference images.`);

    return { statusCode: 202, body: "" };

  } catch (error) {
    console.error("[ROSTER-GEN] Unhandled error:", error);
    if (bucket && projectPath) {
      try {
        await bucket.file(`${projectPath}/ai_asset_roster_error.json`).save(
          JSON.stringify({ error: error.message, failedAt: Date.now(), jobId, stage: "phase1" }),
          { contentType: "application/json", resumable: false }
        );
      } catch (e) { /* non-fatal */ }
    }
    return { statusCode: 202, body: "" };
  }
};
