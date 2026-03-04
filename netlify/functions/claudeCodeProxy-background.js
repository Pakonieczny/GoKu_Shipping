/* netlify/functions/claudeCodeProxy-background.js */
/* ═══════════════════════════════════════════════════════════════════
   TRANCHED AI PIPELINE — v2.0
   ─────────────────────────────────────────────────────────────────
   Stage 0  ▸  Opus 4.6 (adaptive, high)  → Analyzes the prompt and
              splits it into self-contained tranches in real time.
   Stage 1–N ▸ Sonnet 4.6 (adaptive, high) → Executes each tranche
              sequentially, each building on the accumulated output
              of all prior tranches.
   Progress  ▸ Written to Firebase after every tranche so the
              frontend can poll and render live status.
   ═══════════════════════════════════════════════════════════════════ */

const fetch = require("node-fetch");
const admin = require("./firebaseAdmin");

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
function stripFences(text) {
  // Remove markdown fences
  let cleaned = text
    .replace(/^```json\s*/i, "")
    .replace(/^```\s*/i, "")
    .replace(/\s*```$/i, "")
    .trim();

  // If it doesn't start with {, find the first { and last } to extract the JSON object
  const firstBrace = cleaned.indexOf('{');
  const lastBrace = cleaned.lastIndexOf('}');
  if (firstBrace > 0 && lastBrace > firstBrace) {
    cleaned = cleaned.substring(firstBrace, lastBrace + 1);
  }

  return cleaned.trim();
}

/* ── helper: save progress to Firebase ───────────────────────── */
async function saveProgress(bucket, projectPath, progress) {
  await bucket.file(`${projectPath}/ai_progress.json`).save(
    JSON.stringify(progress),
    { contentType: "application/json", resumable: false }
  );
}

/* ═══════════════════════════════════════════════════════════════ */
exports.handler = async (event) => {
  let projectPath = null;
  let bucket = null;

  try {
    if (!event.body) throw new Error("Missing request body");

    ({ projectPath } = JSON.parse(event.body));
    if (!projectPath) throw new Error("Missing projectPath");

    const apiKey = process.env.ANTHROPIC_API_KEY;
    if (!apiKey) throw new Error("Missing ANTHROPIC_API_KEY");

    bucket = admin.storage().bucket(process.env.FIREBASE_STORAGE_BUCKET || "gokudatabase.firebasestorage.app");

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
    //  STAGE 0 — PLANNING (Opus 4.6, adaptive, high)
    // ══════════════════════════════════════════════════════════
    const progress = {
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

    const planningSystem = `You are an expert game development architect and AI pipeline planner.

Your job: analyze the user's game build/modification request and split it into sequential, self-contained TRANCHES that can be executed one at a time by a coding AI.

RULES FOR SPLITTING:
1. Each tranche should focus on 1-2 closely related concerns (e.g., "physics + movement", "UI + scoring", "pipe spawning + scrolling").
2. Tranches MUST be ordered by dependency — later tranches build on earlier ones.
3. Each tranche prompt must be FULLY SELF-CONTAINED: include all the context, rules, and specifics the coding AI needs without referencing other tranches by name.
4. Preserve ALL technical details, variable names, slot layouts, exact code snippets, and architecture rules from the original prompt in the relevant tranche(s). Do NOT summarize or lose any detail.
5. If the prompt is simple enough (minor change, single concern), use just 1 tranche.
6. For complex game builds, use 3-7 tranches. Never exceed 8.
7. Each tranche should describe what FILES it expects to create or modify.
8. The FIRST tranche should always set up the foundational scaffold that later tranches build upon.
9. The LAST tranche should handle polish, edge cases, and integration glue.

CRITICAL FILE NAMING RULES (include in every tranche prompt):
- The main logic file is named "2" (NOT "WorldController.js"), located in "models/" folder.
- The main HTML file is named "23" (NOT "document.html"), located in "models/" folder.
- "assets.json" is in the "json/" folder.

You must respond ONLY with a valid JSON object. No markdown, no code fences, no preamble.

{
  "analysis": "Brief 1-2 sentence analysis of the overall request complexity and your splitting strategy.",
  "tranches": [
    {
      "name": "Short Name",
      "description": "2-3 sentence description of what this tranche accomplishes.",
      "prompt": "THE COMPLETE, SELF-CONTAINED PROMPT for the coding AI. Include all relevant technical details.",
      "expectedFiles": ["models/2", "models/23"]
    }
  ]
}`;

    const planningUserContent = [
      { type: "text", text: `${fileContext}\n\n=== FULL USER REQUEST (analyze and split into tranches) ===\n${prompt}\n=== END USER REQUEST ===` },
      ...imageBlocks
    ];

    console.log("STAGE 0: Planning with Opus 4.6...");
    const planResult = await callClaude(apiKey, {
      model: "claude-opus-4-6",
      maxTokens: 64000,
      budgetTokens: 20000,
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

    let plan;
    try {
      plan = JSON.parse(stripFences(planResult.text));
    } catch (e) {
      throw new Error("Failed to parse planning output as JSON: " + e.message);
    }

    if (!plan.tranches || !Array.isArray(plan.tranches) || plan.tranches.length === 0) {
      throw new Error("Planner returned zero tranches.");
    }

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

    // ══════════════════════════════════════════════════════════
    //  STAGES 1–N — EXECUTION (Sonnet 4.6, adaptive, high)
    // ══════════════════════════════════════════════════════════

    const accumulatedFiles = files ? { ...files } : {};
    const allUpdatedFiles = [];

    const executionSystem = `You are an expert game development AI.
The user will provide project files and a focused modification request (one tranche of a larger build).
You must respond ONLY with a valid JSON object. Do not use markdown code blocks.

The JSON format must be EXACTLY:
{
  "message": "A detailed explanation of what you implemented in this tranche, including specific functions, variables, and logic you added or changed.",
  "updatedFiles": [
    { "path": "folder/filename.ext", "content": "THE_ENTIRE_UPDATED_FILE_CONTENT" }
  ]
}

CRITICAL RULES:
- Only include files in 'updatedFiles' that actually need to be changed or created.
- The main logic file is named "2" in the "models" folder. Never output "WorldController.js".
- The main HTML file is named "23" in the "models" folder. Never output "document.html".
- "assets.json" is in the "json" folder.
- Always output the COMPLETE file content for each updated file — not patches or diffs.
- Build upon the existing file contents provided to you. Do NOT discard or overwrite work from prior tranches. You are adding to and extending the existing code.
- If the file already has functions, variables, or structures from prior tranches, KEEP THEM ALL and add your new code alongside them.`;

    for (let i = 0; i < plan.tranches.length; i++) {
      const tranche = plan.tranches[i];

      // Check for cancellation signal before starting each tranche
      try {
        const cancelFile = bucket.file(`${projectPath}/ai_cancel.json`);
        const [exists] = await cancelFile.exists();
        if (exists) {
          console.log("Cancellation signal detected — aborting pipeline.");
          await cancelFile.delete().catch(() => {});
          progress.status = "cancelled";
          progress.finalMessage = `Pipeline cancelled by user after ${i} tranche(s).`;
          progress.completedTime = Date.now();
          await saveProgress(bucket, projectPath, progress);

          // Write a partial response with whatever we have so far
          if (allUpdatedFiles.length > 0) {
            const partialResponse = {
              message: `Pipeline cancelled. ${allUpdatedFiles.length} file(s) were updated before cancellation.`,
              updatedFiles: allUpdatedFiles
            };
            await bucket.file(`${projectPath}/ai_response.json`).save(
              JSON.stringify(partialResponse),
              { contentType: "application/json", resumable: false }
            );
          }
          return { statusCode: 200, body: JSON.stringify({ success: true, cancelled: true }) };
        }
      } catch (e) { /* no cancel file = continue */ }

      // Update progress: tranche starting
      progress.currentTranche = i;
      progress.tranches[i].status = "in_progress";
      progress.tranches[i].startTime = Date.now();
      await saveProgress(bucket, projectPath, progress);

      console.log(`TRANCHE ${i + 1}/${plan.tranches.length}: ${tranche.name}`);

      // Build file context from accumulated state
      let trancheFileContext = "Here are the current project files (includes all output from prior tranches — you MUST preserve all existing code):\n\n";
      for (const [path, fileContent] of Object.entries(accumulatedFiles)) {
        trancheFileContext += `--- FILE: ${path} ---\n${fileContent}\n\n`;
      }

      const trancheUserContent = [
        {
          type: "text",
          text: `${trancheFileContext}\n\n=== TRANCHE ${i + 1} of ${plan.tranches.length}: "${tranche.name}" ===\n\n${tranche.prompt}\n\n=== END TRANCHE INSTRUCTIONS ===\n\nIMPORTANT: You are working on tranche ${i + 1} of ${plan.tranches.length}. The project files above contain ALL work from prior tranches. You MUST preserve all existing code and ADD your changes on top. Output the COMPLETE updated file contents.`
        },
        ...imageBlocks
      ];

      let trancheResponseObj;
      try {
        trancheResponseObj = await callClaude(apiKey, {
          model: "claude-sonnet-4-6",
          maxTokens: 100000,
          budgetTokens: 24000,
          effort: "high",
          system: executionSystem,
          userContent: trancheUserContent
        });
      } catch (err) {
        progress.tranches[i].status = "error";
        progress.tranches[i].endTime = Date.now();
        progress.tranches[i].message = `Error: ${err.message}`;
        await saveProgress(bucket, projectPath, progress);
        console.error(`Tranche ${i + 1} failed:`, err.message);
        continue;
      }

      // Record tranche token usage
      if (trancheResponseObj.usage) {
        progress.tokenUsage.tranches[i] = trancheResponseObj.usage;
        progress.tokenUsage.totals.input_tokens += trancheResponseObj.usage.input_tokens || 0;
        progress.tokenUsage.totals.output_tokens += trancheResponseObj.usage.output_tokens || 0;
        progress.tranches[i].tokenUsage = trancheResponseObj.usage;
        await saveProgress(bucket, projectPath, progress);
      }

      let trancheResult;
      try {
        trancheResult = JSON.parse(stripFences(trancheResponseObj.text));
      } catch (e) {
        progress.tranches[i].status = "error";
        progress.tranches[i].endTime = Date.now();
        progress.tranches[i].message = `Failed to parse JSON response: ${e.message}`;
        await saveProgress(bucket, projectPath, progress);
        console.error(`Tranche ${i + 1} JSON parse failed:`, e.message);
        continue;
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
      progress.tranches[i].status = "complete";
      progress.tranches[i].endTime = Date.now();
      progress.tranches[i].message = trancheResult.message || "Tranche completed.";
      progress.tranches[i].filesUpdated = trancheFilesUpdated;
      await saveProgress(bucket, projectPath, progress);

      console.log(`Tranche ${i + 1} complete: ${trancheFilesUpdated.length} files updated.`);
    }

    // ══════════════════════════════════════════════════════════
    //  FINAL — Assemble and save response
    // ══════════════════════════════════════════════════════════

    const summaryParts = progress.tranches
      .filter(t => t.status === "complete")
      .map((t, i) => `Tranche ${t.index + 1} — ${t.name}: ${t.message}`);

    const finalResponse = {
      message: summaryParts.join("\n\n") || "Build completed.",
      updatedFiles: allUpdatedFiles
    };

    await bucket.file(`${projectPath}/ai_response.json`).save(
      JSON.stringify(finalResponse),
      { contentType: "application/json", resumable: false }
    );

    progress.status = "complete";
    const t = progress.tokenUsage.totals;
    progress.finalMessage = `Build complete: ${allUpdatedFiles.length} file(s) updated across ${progress.tranches.filter(t => t.status === "complete").length} tranche(s). Tokens: ${t.input_tokens} in / ${t.output_tokens} out.`;
    progress.completedTime = Date.now();
    await saveProgress(bucket, projectPath, progress);

    console.log(`Total tokens — input: ${t.input_tokens}, output: ${t.output_tokens}`);

    try { await requestFile.delete(); } catch (e) {}

    return { statusCode: 200, body: JSON.stringify({ success: true }) };

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