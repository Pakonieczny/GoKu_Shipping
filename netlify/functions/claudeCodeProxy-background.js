/* netlify/functions/claudeCodeProxy-background.js */
const fetch = require("node-fetch");
const admin = require("./firebaseAdmin");

exports.handler = async (event) => {
  try {
    if (!event.body) {
      throw new Error("Missing request body");
    }

    // 1. We only receive the projectPath from the frontend
    const { projectPath } = JSON.parse(event.body);
    if (!projectPath) throw new Error("Missing projectPath details");

    const apiKey = process.env.ANTHROPIC_API_KEY;
    if (!apiKey) throw new Error("Missing ANTHROPIC_API_KEY");

    // 2. Connect to Firebase and download the massive payload file
    const bucket = admin.storage().bucket(process.env.FIREBASE_STORAGE_BUCKET || "gokudatabase.firebasestorage.app");
    const requestFile = bucket.file(`${projectPath}/ai_request.json`);
    const [content] = await requestFile.download();

    // 3. Unpack the full data directly from Firebase memory
    const { prompt, files, selectedAssets, inlineImages } = JSON.parse(content.toString());

    if (!prompt) throw new Error("Missing instructions inside payload");

    // --- Build file context string ---
    let fileContext = "Here are the current project files:\n\n";
    if (files) {
      for (const [path, fileContent] of Object.entries(files)) {
        fileContext += `--- FILE: ${path} ---\n${fileContent}\n\n`;
      }
    }

    const systemInstruction = `You are an expert game development AI. 
The user will provide project files and a modification request.
You must respond ONLY with a valid JSON object. Do not use markdown code blocks like \`\`\`json.

The JSON format must be EXACTLY:
{
  "message": "A short, 1-2 sentence explanation of what you changed.",
  "updatedFiles": [
    { "path": "folder/filename.ext", "content": "THE_ENTIRE_UPDATED_FILE_CONTENT" }
  ]
}
Only include files in 'updatedFiles' that actually need to be changed.`;

    // --- Build the content array for Claude's messages API ---
    // Claude uses a content array where each element is a content block.
    // Images are passed as image source blocks; text is a text block.
    const userContentBlocks = [];

    // Primary text block: system context + file context + user prompt
    userContentBlocks.push({
      type: "text",
      text: fileContext + "\nUser Request: " + prompt
    });

    // --- Inject Multi-Modal Assets SAFELY ---
    if (selectedAssets && Array.isArray(selectedAssets) && selectedAssets.length > 0) {
      let assetContext = "\n\nThe user has designated the following files for you to use. Their relative paths in the project are:\n";

      for (const asset of selectedAssets) {
        assetContext += `- ${asset.path}\n`;

        const isSupportedImage =
          (asset.type && asset.type.startsWith('image/')) ||
          (asset.name && asset.name.match(/\.(png|jpe?g|webp)$/i));

        // Claude supports image media types: image/jpeg, image/png, image/gif, image/webp
        // Audio/video are NOT supported as inline data by Claude — reference by path only
        if (isSupportedImage) {
          try {
            const assetRes = await fetch(asset.url);
            if (!assetRes.ok) throw new Error(`Failed to fetch media: ${assetRes.statusText}`);
            const arrayBuffer = await assetRes.arrayBuffer();
            const base64Data = Buffer.from(arrayBuffer).toString('base64');

            let mime = asset.type;
            if (!mime || !mime.startsWith('image/')) {
              if (asset.name.endsWith('.png')) mime = 'image/png';
              else if (asset.name.endsWith('.jpg') || asset.name.endsWith('.jpeg')) mime = 'image/jpeg';
              else if (asset.name.endsWith('.webp')) mime = 'image/webp';
              else mime = 'image/png';
            }

            userContentBlocks.push({
              type: "image",
              source: {
                type: "base64",
                media_type: mime,
                data: base64Data
              }
            });
          } catch (fetchErr) {
            console.error(`Failed to fetch visual asset ${asset.name}:`, fetchErr);
          }
        } else {
          assetContext += `  (Note: ${asset.name} is a non-image file. Use the path provided above to reference it in code.)\n`;
        }
      }

      // Append asset context to the first text block
      userContentBlocks[0].text += assetContext;
    }

    // --- Inject Dragged-and-Dropped Images ---
    // Claude supports: image/jpeg, image/png, image/gif, image/webp
    if (inlineImages && Array.isArray(inlineImages) && inlineImages.length > 0) {
      for (const img of inlineImages) {
        if (img.data && img.mimeType && img.mimeType.startsWith('image/')) {
          userContentBlocks.push({
            type: "image",
            source: {
              type: "base64",
              media_type: img.mimeType,
              data: img.data
            }
          });
        }
      }
    }

    // --- Call Claude API ---
    const body = {
      model: "claude-opus-4-6",        // Opus 4.6 = max reasoning, 128K output tokens
      max_tokens: 100000,               // MUST be high enough for thinking + response at max effort.
                                       // At effort:"max" with adaptive thinking, Claude can burn thousands
                                       // of tokens on reasoning alone — 16K was too low, leaving 0 for
                                       // the text response block (causing "Empty response from Claude").
      thinking: { type: "adaptive" },  // Recommended for Opus 4.6: Claude decides when/how much to reason
      output_config: { effort: "max" },// effort lives inside output_config (top-level is invalid)
      system: systemInstruction,
      messages: [
        {
          role: "high",
          content: userContentBlocks
        }
      ]
    };

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
    if (!res.ok) throw new Error(data.error?.message || "Claude API error");

    // Claude returns content as an array of blocks; grab the first text block
    const responseText = data.content?.find(block => block.type === "text")?.text;
    if (!responseText) throw new Error("Empty response from Claude");

    // Strip any accidental markdown fences Claude may have added
    const cleanResponse = responseText
      .replace(/^```json\s*/i, '')
      .replace(/^```\s*/i, '')
      .replace(/\s*```$/i, '')
      .trim();

    // Save AI output to Firebase
    await bucket.file(`${projectPath}/ai_response.json`).save(cleanResponse, {
      contentType: "application/json",
      resumable: false
    });

    // Clean up the request payload since we are done with it
    try { await requestFile.delete(); } catch (e) {}

    return { statusCode: 200, body: JSON.stringify({ success: true }) };

  } catch (error) {
    console.error("Claude Code Proxy Background Error:", error);
    try {
      if (event.body) {
        const { projectPath } = JSON.parse(event.body);
        if (projectPath) {
          const bucket = admin.storage().bucket(process.env.FIREBASE_STORAGE_BUCKET || "gokudatabase.firebasestorage.app");
          await bucket.file(`${projectPath}/ai_error.json`).save(
            JSON.stringify({ error: error.message }),
            { contentType: "application/json" }
          );
        }
      }
    } catch (e) {
      console.error("CRITICAL: Failed to write error to Firebase.", e);
    }

    return { statusCode: 500, body: JSON.stringify({ error: error.message }) };
  }
};