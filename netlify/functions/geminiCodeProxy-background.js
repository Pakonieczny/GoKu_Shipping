/* netlify/functions/geminiCodeProxy-background.js */
const fetch = require("node-fetch");
const admin = require("./firebaseAdmin");

exports.handler = async (event) => {
  try {
    if (!event.body) {
      throw new Error("Missing request body");
    }
    
    // 1. We only receive the projectPath from the frontend now
    const { projectPath } = JSON.parse(event.body);
    if (!projectPath) throw new Error("Missing projectPath details");

    const apiKey = process.env.GEMINI_API_KEY;
    if (!apiKey) throw new Error("Missing GEMINI_API_KEY");

    // 2. Connect to Firebase and download the massive payload file
    const bucket = admin.storage().bucket(process.env.FIREBASE_STORAGE_BUCKET || "gokudatabase.firebasestorage.app");
    const requestFile = bucket.file(`${projectPath}/ai_request.json`);
    const [content] = await requestFile.download();
    
    // 3. Unpack the full data directly from Firebase memory
    const { prompt, files, selectedAssets, inlineImages } = JSON.parse(content.toString());

    if (!prompt) throw new Error("Missing instructions inside payload");

    let fileContext = "Here are the current project files:\n\n";
    if (files) {
        for (const [path, fileContent] of Object.entries(files)) {
          fileContext += `--- FILE: ${path} ---\n${fileContent}\n\n`;
        }
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
    if (selectedAssets && Array.isArray(selectedAssets) && selectedAssets.length > 0) {
        let assetContext = "\n\nThe user has designated the following files for you to use. Their relative paths in the project are:\n";
        
        for (const asset of selectedAssets) {
            assetContext += `- ${asset.path} \n`;
            
            const isSupportedMedia = 
                (asset.type && (asset.type.startsWith('image/') || asset.type.startsWith('audio/') || asset.type.startsWith('video/'))) ||
                (asset.name && asset.name.match(/\.(png|jpe?g|webp|mp3|wav|ogg)$/i));

            if (isSupportedMedia) {
                try {
                    const assetRes = await fetch(asset.url);
                    if (!assetRes.ok) throw new Error(`Failed to fetch media from url: ${assetRes.statusText}`);
                    const arrayBuffer = await assetRes.arrayBuffer();
                    const base64Data = Buffer.from(arrayBuffer).toString('base64');
                    
                    let mime = asset.type;
                    if (!mime) {
                        if (asset.name.endsWith('.png')) mime = 'image/png';
                        else if (asset.name.endsWith('.jpg') || asset.name.endsWith('.jpeg')) mime = 'image/jpeg';
                        else if (asset.name.endsWith('.mp3')) mime = 'audio/mp3';
                        else mime = 'image/png'; 
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
                assetContext += `  (Note: ${asset.name} is a 3D model/binary file. Use the path provided above to load it in your code.)\n`;
            }
        }
        
        parts[0].text += assetContext;
    }

    // --- Inject Dragged-and-Dropped Images ---
    if (inlineImages && Array.isArray(inlineImages) && inlineImages.length > 0) {
        for (const img of inlineImages) {
            if (img.data && img.mimeType) {
                parts.push({
                    inlineData: {
                        data: img.data,
                        mimeType: img.mimeType
                    }
                });
            }
        }
    }

    const body = {
      contents: [{ role: "user", parts: parts }],
      generationConfig: {
        responseMimeType: "application/json", 
        temperature: 0.2 
      }
    };

    const url = `https://generativelanguage.googleapis.com/v1beta/models/gemini-3-flash-preview:generateContent`;

    const res = await fetch(url, {
      method: "POST",
      headers: { "Content-Type": "application/json", "x-goog-api-key": apiKey },
      body: JSON.stringify(body),
    });

    const data = await res.json();
    if (!res.ok) throw new Error(data.error?.message || "Gemini API error");

    const responseText = data.candidates?.[0]?.content?.parts?.[0]?.text;
    if (!responseText) throw new Error("Empty response from Gemini");

    // Save AI output to Firebase
    await bucket.file(`${projectPath}/ai_response.json`).save(responseText, {
        contentType: "application/json",
        resumable: false
    });
    
    // Clean up the request payload since we are done with it
    try { await requestFile.delete(); } catch(e) {}

    return { statusCode: 200, body: JSON.stringify({ success: true }) };

  } catch (error) {
    console.error("Code Proxy Background Error:", error);
    try {
        if (event.body) {
            const { projectPath } = JSON.parse(event.body);
            if (projectPath) {
                const bucket = admin.storage().bucket(process.env.FIREBASE_STORAGE_BUCKET || "gokudatabase.firebasestorage.app");
                await bucket.file(`${projectPath}/ai_error.json`).save(JSON.stringify({ error: error.message }), { 
                    contentType: "application/json" 
                });
            }
        }
    } catch(e) {
        console.error("CRITICAL: Failed to write error to Firebase.", e);
    }
    
    return { statusCode: 500, body: JSON.stringify({ error: error.message }) };
  }
};