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

    const parts = [{ text: systemInstruction + fileContext + "\nUser Request: " + prompt }];

    // --- Inject Multi-Modal Assets SAFELY ---
    if (selectedAssets && selectedAssets.length > 0) {
        let assetContext = "\n\nThe user has designated the following files for you to use. Their relative paths in the project are:\n";
        
        for (const asset of selectedAssets) {
            assetContext += `- ${asset.path} \n`;
            
            // Filter out 3D models and unsupported files from inlineData
            const isSupportedMedia = 
                asset.type.startsWith('image/') || 
                asset.type.startsWith('audio/') || 
                asset.type.startsWith('video/') ||
                asset.name.match(/\.(png|jpe?g|webp|mp3|wav|ogg)$/i);

            if (isSupportedMedia) {
                try {
                    const assetRes = await fetch(asset.url);
                    const arrayBuffer = await assetRes.arrayBuffer();
                    const base64Data = Buffer.from(arrayBuffer).toString('base64');
                    
                    // Fallback mime type if undefined
                    let mime = asset.type;
                    if (!mime) {
                        if (asset.name.endsWith('.png')) mime = 'image/png';
                        else if (asset.name.endsWith('.jpg') || asset.name.endsWith('.jpeg')) mime = 'image/jpeg';
                        else if (asset.name.endsWith('.mp3')) mime = 'audio/mp3';
                        else mime = 'image/png'; // generic fallback
                    }

                    parts.push({
                        inlineData: {
                            data: base64Data,
                            mimeType: mime
                        }
                    });
                } catch (fetchErr) {
                    console.error(`Failed to fetch visual asset ${asset.name}:`, fetchErr);
                }
            } else {
                // It's a 3D model (.obj, .glb, etc). Just add a text note about it.
                assetContext += `  (Note: ${asset.name} is a 3D model/binary file. Use the path provided above to load it in your code.)\n`;
            }
        }
        
        // Add the path context text so the AI knows the exact strings to write
        parts[0].text += assetContext;
    }

    const body = {
      contents: [{ role: "user", parts: parts }],
      generationConfig: {
        responseMimeType: "application/json", 
        temperature: 0.2 
      }
    };

    const url = `https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-pro:generateContent`;

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