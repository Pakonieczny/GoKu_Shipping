// netlify/functions/cropAndSend.js

const fetch = require("node-fetch");
const FormData = require("form-data");

/**
 * Expects POST body like:
 * {
 *   "fileBase64": "<base64>",
 *   "fileName": "cropped_image_X.jpg"
 *   "vectorStoreId": "<optional>"
 * }
 *
 * Then:
 *  1) Upload the file to OpenAI /v1/files
 *  2) "create" a new store or attach the file to an existing store in myVectorStore.js
 */
exports.handler = async function(event) {
  try {
    if (!event.body || event.body.trim() === "") {
      throw new Error("No JSON body provided.");
    }
    const payload = JSON.parse(event.body);
    const { fileBase64, fileName, vectorStoreId } = payload;

    if (!fileBase64 || !fileBase64.trim()) {
      throw new Error("Missing 'fileBase64'.");
    }
    if (!fileName) {
      throw new Error("Missing 'fileName'.");
    }

    const apiKey = process.env.OPENAI_API_KEY;
    if (!apiKey) {
      throw new Error("Missing OPENAI_API_KEY environment variable.");
    }

    // Convert base64 -> buffer
    const buffer = Buffer.from(fileBase64, "base64");
    let contentType = "image/jpeg";
    if (fileName.toLowerCase().endsWith(".png")) {
      contentType = "image/png";
    } else if (fileName.toLowerCase().endsWith(".gif")) {
      contentType = "image/gif";
    }

    const form = new FormData();
    form.append("file", buffer, { filename: fileName, contentType });
    form.append("purpose", "fine-tune"); // or 'embeddings'

    // Upload to /v1/files
    const openAiResp = await fetch("https://api.openai.com/v1/files", {
      method: "POST",
      headers: {
        "Authorization": `Bearer ${apiKey}`,
        ...form.getHeaders()
      },
      body: form
    });
    const openAiRespText = await openAiResp.text();
    let openAiData;
    try {
      openAiData = JSON.parse(openAiRespText);
    } catch {
      openAiData = { error: openAiRespText };
    }
    if (!openAiResp.ok) {
      throw new Error(
        `OpenAI file upload failed (status ${openAiResp.status}): ` +
        JSON.stringify(openAiData)
      );
    }
    const fileId = openAiData.id;
    if (!fileId) {
      throw new Error("OpenAI upload succeeded but no 'id' was returned.");
    }

    // Next => attach that file ID to a new or existing store
    // For simplicity, we do "create" a new store
    const storePayload = {
      action: "create",
      file_ids: [fileId],
      name: "My Images Vector Store"
    };
    if (vectorStoreId) {
      // if you want to attach to an existing store, adapt logic here
    }

    // Call local netlify function "myVectorStore"
    const storeResp = await fetch("/.netlify/functions/myVectorStore", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(storePayload)
    });
    const storeRespText = await storeResp.text();
    let storeData;
    try {
      storeData = JSON.parse(storeRespText);
    } catch {
      storeData = { error: storeRespText };
    }
    if (!storeResp.ok) {
      throw new Error(
        `myVectorStore step failed (status ${storeResp.status}): ` +
        JSON.stringify(storeData)
      );
    }

    // Return success
    return {
      statusCode: 200,
      body: JSON.stringify({
        status: "ok",
        fileId,
        vectorResult: storeData
      })
    };
  } catch (err) {
    console.error("Error in cropAndSend.js:", err);
    return {
      statusCode: 500,
      body: JSON.stringify({ error: err.message })
    };
  }
};