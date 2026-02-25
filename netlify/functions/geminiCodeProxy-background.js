/* netlify/functions/geminiCodeProxy-background.js */
const fetch = require("node-fetch");
const admin = require("./firebaseAdmin");

exports.handler = async (event) => {
  try {
    const { prompt, files, projectPath, selectedAssets } = JSON.parse(event.body);
    const apiKey = process.env.GEMINI_API_KEY;

    if (!apiKey) throw new Error("Missing GEMINI_API_KEY");
    if (!prompt || !projectPath) throw new Error("Missing instructions or project details");

    let fileContext = "Here are the current project files:\n\n";
    for (const [path, content] of Object.entries(files)) {
      fileContext += `--- FILE: ${path} ---\n${content}\n\n`;
    }

    const systemInstruction = `
      You are an expert game development AI. 
      The user will provide project files and a modification request.
      You must respond ONLY with a valid JSON object. Do not use markdown code blocks like \`\`\`json.
      
      The JSON format must be EXACTLY:
      {
        "message": "A short, 1-2 sentence explanation of what you changed.",
        "updatedFiles": [
          { "path": "folder/filename.ext", "content": "THE_ENTIRE_UPDATED_FILE_CONTENT" }
        ]
      }
      Only include files in 'updatedFiles' that actually need to be changed.
    `;

    // Initialize the parts array with text
    const parts = [{ text: systemInstruction + fileContext + "\nUser Request: " + prompt }];

    // --- NEW: Inject Multi-Modal Assets ---
    if (selectedAssets && selectedAssets.length > 0) {
        let assetContext = "\n\nThe user has also provided the following media assets for you to use. Their relative paths in the project are:\n";
        
        for (const asset of selectedAssets) {
            assetContext += `- ${asset.path} \n`;
            
            try {
                // Fetch the asset buffer from Firebase Storage URL
                const assetRes = await fetch(asset.url);
                const arrayBuffer = await assetRes.arrayBuffer();
                const base64Data = Buffer.from(arrayBuffer).toString('base64');
                
                // Append the file directly to Gemini's vision/audio context
                parts.push({
                    inlineData: {
                        data: base64Data,
                        mimeType: asset.type || "image/png"
                    }
                });
            } catch (fetchErr) {
                console.error(`Failed to fetch asset ${asset.name}:`, fetchErr);
            }
        }
        // Add the path context text so the AI knows how to code the paths
        parts[0].text += assetContext;
    }

    const body = {
      contents: [{ role: "user", parts: parts }],
      generationConfig: {
        responseMimeType: "application/json", 
        temperature: 0.2 
      }
    };

    const url = `https://generativelanguage.googleapis.com/v1beta/models/gemini-3-pro-preview:generateContent`;
    
    const res = await fetch(url, {
      method: "POST",
      headers: { "Content-Type": "application/json", "x-goog-api-key": apiKey },
      body: JSON.stringify(body),
    });

    const data = await res.json();
    if (!res.ok) throw new Error(data.error?.message || "Gemini API error");

    const responseText = data.candidates?.[0]?.content?.parts?.[0]?.text;
    if (!responseText) throw new Error("Empty response from Gemini");

    const bucket = admin.storage().bucket(process.env.FIREBASE_STORAGE_BUCKET || "gokudatabase.firebasestorage.app");
    await bucket.file(`${projectPath}/ai_response.json`).save(responseText, {
        contentType: "application/json",
        resumable: false
    });

  } catch (error) {
    console.error("Code Proxy Background Error:", error);
    try {
        const { projectPath } = JSON.parse(event.body);
        const bucket = admin.storage().bucket(process.env.FIREBASE_STORAGE_BUCKET || "gokudatabase.firebasestorage.app");
        await bucket.file(`${projectPath}/ai_error.json`).save(JSON.stringify({ error: error.message }), { 
            contentType: "application/json" 
        });
    } catch(e) {}
  }
};