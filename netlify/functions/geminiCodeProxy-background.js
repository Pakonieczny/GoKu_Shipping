/* netlify/functions/geminiCodeProxy-background.js */
const fetch = require("node-fetch");
const admin = require("./firebaseAdmin");

exports.handler = async (event) => {
  // Background functions only return a 202 to the browser immediately. 
  // We must handle all logic and save the result to Firebase Storage.
  try {
    const { prompt, files, projectPath } = JSON.parse(event.body);
    const apiKey = process.env.GEMINI_API_KEY;

    if (!apiKey) throw new Error("Missing GEMINI_API_KEY");
    if (!prompt || !projectPath) throw new Error("Missing instructions or project details");

    // Construct the context payload for Gemini
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

    const body = {
      contents: [{ role: "user", parts: [{ text: systemInstruction + fileContext + "\nUser Request: " + prompt }] }],
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

    // Save the success payload directly to Firebase Storage
    const bucket = admin.storage().bucket(process.env.FIREBASE_STORAGE_BUCKET || "gokudatabase.firebasestorage.app");
    await bucket.file(`${projectPath}/ai_response.json`).save(responseText, {
        contentType: "application/json",
        resumable: false
    });

  } catch (error) {
    console.error("Code Proxy Background Error:", error);
    // Write an error file so the frontend polling knows to stop and show an error
    try {
        const { projectPath } = JSON.parse(event.body);
        const bucket = admin.storage().bucket(process.env.FIREBASE_STORAGE_BUCKET || "gokudatabase.firebasestorage.app");
        await bucket.file(`${projectPath}/ai_error.json`).save(JSON.stringify({ error: error.message }), { 
            contentType: "application/json" 
        });
    } catch(e) {}
  }
};