/* netlify/functions/claudeRosterGenerate-background.js */
/* ═══════════════════════════════════════════════════════════════════
   GAME-SPECIFIC ASSET ROSTER GENERATION — v2.0
   ─────────────────────────────────────────────────────────────────
   Background Netlify function (suffix -background = 15-min timeout).
   Returns 202 immediately. Writes result to Firebase when done.
   Frontend polls ai_asset_roster_pending.json to detect completion.

   Flow:
     1. Read Master Prompt + inline images from ai_request.json
     2. Read all .docx text content from asset_rosters/ in Firebase
        (parallel downloads)
     3. Call Claude Sonnet 4.6 with all three inputs to generate
        a game-specific Asset Roster (max 25 obj / 50 png)
     4. Validate hard limits
     5. Save roster as ai_asset_roster_pending.json in Firebase
        (frontend polls for this file to detect completion)

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
  if (Array.isArray(roster.primitiveObjects)) {
    if (roster.primitiveObjects.length > MAX_OBJ_ASSETS) {
      console.warn(`[ROSTER] Trimming primitiveObjects from ${roster.primitiveObjects.length} to ${MAX_OBJ_ASSETS}`);
      roster.primitiveObjects = roster.primitiveObjects.slice(0, MAX_OBJ_ASSETS);
    }
  }
  if (Array.isArray(roster.textureAssets)) {
    if (roster.textureAssets.length > MAX_PNG_ASSETS) {
      console.warn(`[ROSTER] Trimming textureAssets from ${roster.textureAssets.length} to ${MAX_PNG_ASSETS}`);
      roster.textureAssets = roster.textureAssets.slice(0, MAX_PNG_ASSETS);
    }
  }
  if (roster.coverageSummary) {
    roster.coverageSummary.totalPrimitives = (roster.primitiveObjects || []).length;
    roster.coverageSummary.totalTextures   = (roster.textureAssets || []).length;
    roster.coverageSummary.limitsRespected =
      roster.coverageSummary.totalPrimitives <= MAX_OBJ_ASSETS &&
      roster.coverageSummary.totalTextures   <= MAX_PNG_ASSETS;
  }
  return roster;
}

/* ─── Build the Claude prompt ────────────────────────────────── */
function buildRosterPrompt(masterPrompt, rosterDocsBlock) {
  return `You are a game asset selection specialist. Your job is to analyze the requested game and select the most appropriate available assets from the available Asset Rosters.

HARD LIMITS — YOU MUST RESPECT THESE ABSOLUTELY:
- Maximum ${MAX_OBJ_ASSETS} unique primitive object assets (.obj files)
- Maximum ${MAX_PNG_ASSETS} unique texture assets (.png files)
- Do NOT exceed these limits under any circumstances

ANALYSIS TASK:
Study the Master Game Prompt and any attached gameplay reference images carefully.
Infer the game's specific requirements including:
- Genre and gameplay type
- Environment type (indoor/outdoor/space/abstract/etc.)
- Obstacle types and props needed
- Primitive object needs (shapes, structures, platforms)
- Texture and material needs
- Scene composition and object density
- Color direction and visual style
- Realism vs stylization level
- FX relevance
- Gameplay readability needs
- Environmental tone

AVAILABLE ASSET ROSTERS:
The following Asset Roster documents describe the available primitives and textures in each pack.
Each document corresponds to a .zip file of the same name in the Asset_Packs folder.

${rosterDocsBlock}

SELECTION TASK:
Based on your analysis of the requested game, select the most appropriate assets from the above rosters.

SELECTION RULES:
- Choose assets based on genuine relevance to the requested gameplay, scene, and visual style
- Do NOT choose assets randomly
- Prioritize assets that directly serve the game's environment, obstacles, and visual identity
- Ensure selected textures are compatible with the selected objects
- Ensure coverage of all major scene elements (ground, walls/obstacles, props, effects)
- Ensure visual consistency across selections
- Stay within the hard limits above

MASTER GAME PROMPT:
${masterPrompt}

OUTPUT REQUIREMENTS:
Respond ONLY with a valid JSON object. No markdown fences, no preamble.

{
  "documentTitle": "Game-Specific Asset Roster — [brief game name]",
  "gameInterpretationSummary": "2-3 sentence description of the game type, environment, visual style, and key asset requirements inferred from the Master Prompt and reference images.",
  "primitiveObjects": [
    {
      "assetName": "filename.obj",
      "sourceRosterDocument": "RosterDocumentName.docx",
      "intendedRole": "What this object does in the scene or gameplay",
      "selectionRationale": "Why this specific asset was chosen for this game"
    }
  ],
  "textureAssets": [
    {
      "assetName": "filename.png",
      "sourceRosterDocument": "RosterDocumentName.docx",
      "intendedUsage": "Which surfaces or materials this texture applies to",
      "selectionRationale": "Why this specific texture was chosen for this game"
    }
  ],
  "coverageSummary": {
    "totalPrimitives": 0,
    "totalTextures": 0,
    "limitsRespected": true,
    "coverageNotes": "Brief note on what scene elements are covered by the selection"
  },
  "visualDirectionNotes": {
    "colorDirection": "Primary and accent color palette direction",
    "materialStyle": "Material aesthetic (realistic, stylized, hand-painted, PBR, etc.)",
    "realismLevel": "Photorealistic / Semi-realistic / Stylized / Cartoon",
    "environmentalTone": "Mood and atmosphere (dark, bright, gritty, clean, etc.)",
    "surfaceTreatment": "How surfaces should look (worn, pristine, organic, industrial, etc.)",
    "fxRelevance": "What FX assets are relevant and why, or 'None required'"
  }
}`;
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

    // ── 2. Load all .docx files from asset_rosters/ — parallel ─────
    const rosterFolder = `${projectPath}/asset_rosters`;
    const [rosterFiles] = await bucket.getFiles({ prefix: rosterFolder + "/" });
    const docxFiles = (rosterFiles || []).filter(f =>
      f.name.toLowerCase().endsWith(".docx")
    );

    console.log(`[ROSTER-GEN] Found ${docxFiles.length} roster docx file(s)`);

    // Download and extract all docx files in parallel
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

    const rosterDocNames = docxResults.map(d => d.baseName);
    let rosterDocsBlock = docxResults.map(d =>
      `\n\n=== ROSTER DOCUMENT: ${d.baseName} ===\n${d.text}\n=== END: ${d.baseName} ===\n`
    ).join("");

    if (rosterDocsBlock.trim() === "") {
      rosterDocsBlock = "(No Asset Roster documents found in Asset_Rosters folder. Claude should generate a generic asset roster based on the game requirements only, without referencing specific files.)";
    }

    // ── 3. Build image content blocks ───────────────────────────────
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
      console.log(`[ROSTER-GEN] Loaded ${imageBlocks.length} reference image(s) for context`);
    }

    const imagePreamble = imageBlocks.length > 0
      ? `\nREFERENCE IMAGES: ${imageBlocks.length} gameplay reference image(s) are attached. ` +
        `They carry authority equal to the Master Prompt. Use them to infer the visual style, ` +
        `environment type, entity types, color palette, and asset requirements.\n\n`
      : "";

    // ── 4. Call Claude Sonnet 4.6 ────────────────────────────────────
    console.log("[ROSTER-GEN] Calling Claude Sonnet 4.6 for roster generation...");
    const rosterResult = await callClaude(apiKey, {
      model:      "claude-sonnet-4-20250514",
      maxTokens:  8000,
      system:     "You are a game asset selection specialist. Respond only with a valid JSON object. No markdown, no fences, no preamble.",
      userContent: [
        { type: "text", text: imagePreamble + buildRosterPrompt(masterPrompt, rosterDocsBlock) },
        ...imageBlocks
      ]
    });

    // ── 5. Parse and validate ────────────────────────────────────────
    let roster;
    try {
      roster = JSON.parse(stripFences(rosterResult.text));
    } catch (e) {
      console.error("[ROSTER-GEN] Failed to parse Claude response as JSON:", rosterResult.text.slice(0, 500));
      return err500(`Claude returned unparseable roster JSON: ${e.message}`);
    }

    roster = enforceHardLimits(roster);

    // Attach metadata
    roster._meta = {
      jobId,
      generatedAt: Date.now(),
      availableRosterDocs: rosterDocNames,
      imageCount: imageBlocks.length,
      approved: false
    };

    // ── 6. Save pending roster to Firebase ───────────────────────────
    await bucket.file(`${projectPath}/ai_asset_roster_pending.json`).save(
      JSON.stringify(roster, null, 2),
      { contentType: "application/json", resumable: false }
    );

    console.log(
      `[ROSTER-GEN] Complete. Primitives: ${(roster.primitiveObjects||[]).length}, ` +
      `Textures: ${(roster.textureAssets||[]).length}`
    );

    // Background function — frontend polls ai_asset_roster_pending.json
    return { statusCode: 202, body: '' };

  } catch (error) {
    console.error("[ROSTER-GEN] Unhandled error:", error);
    // Write error sentinel to Firebase so the frontend poll can detect failure
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


