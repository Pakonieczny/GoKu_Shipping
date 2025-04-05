// netlify/functions/cropAndSendLocal.js

const fs = require("fs");
const fetch = require("node-fetch");
const FormData = require("form-data");
// If you want actual cropping, install "sharp" or "jimp":
// const sharp = require("sharp");

exports.handler = async function(event) {
  try {
    if (!event.body) {
      throw new Error("No JSON body provided.");
    }
    const payload = JSON.parse(event.body);

    // 'localPath' => path to the .jpg in /tmp
    // 'fileName' => final name in OpenAI
    // 'vectorStoreId' => optional store ID
    const { localPath, fileName, vectorStoreId } = payload;
    if (!localPath) throw new Error("Missing 'localPath'.");
    if (!fileName) throw new Error("Missing 'fileName'.");

    const apiKey = process.env.OPENAI_API_KEY;
    if (!apiKey) {
      throw new Error("Missing OPENAI_API_KEY environment variable.");
    }

    // 1) Verify the file exists in /tmp
    if (!fs.existsSync(localPath)) {
      throw new Error(`File not found: ${localPath}`);
    }

    // 2) Read the file data
    let buffer = fs.readFileSync(localPath);

    // 3) (Optional) Crop with "sharp" if you have bounding box data
    // e.g. if payload.cropRect => {x, y, width, height}
    /*
      if (payload.cropRect) {
        const { x, y, width, height } = payload.cropRect;
        buffer = await sharp(buffer)
          .extract({ left: x, top: y, width, height })
          .toBuffer();
      }
    */

    // 4) Upload to OpenAI /v1/files
    let contentType = "image/jpeg";
    if (fileName.toLowerCase().endsWith(".png")) {
      contentType = "image/png";
    } else if (fileName.toLowerCase().endsWith(".gif")) {
      contentType = "image/gif";
    }

    const form = new FormData();
    form.append("file", buffer, { filename: fileName, contentType });
    // Set purpose to "embeddings" or "fine-tune" as needed
    form.append("purpose", "fine-tune");

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
    if (!fileId) throw new Error("OpenAI upload succeeded but no 'id' returned.");

    // 5) Attach the file ID to a vector store (or create a new one)
    let storePayload = {
      action: "create",
      file_ids: [fileId],
      name: "My Server-Side Images Vector Store"
    };
    if (vectorStoreId) {
      // If you prefer to attach to an existing store, define that logic:
      // e.g. storePayload = { action: "attach", store_id: vectorStoreId, file_ids: [fileId] };
    }

    // local call to myVectorStore
    const storeResp = await fetch("/.netlify/functions/myVectorStore", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(storePayload)
    });
    if (!storeResp.ok) {
      const storeErr = await storeResp.text();
      throw new Error(`myVectorStore error: ${storeResp.status} => ${storeErr}`);
    }
    const storeData = await storeResp.json();

    return {
      statusCode: 200,
      body: JSON.stringify({
        status: "ok",
        fileId,
        storeData
      })
    };
  } catch (err) {
    console.error("cropAndSendLocal error:", err);
    return {
      statusCode: 500,
      body: JSON.stringify({ error: err.message })
    };
  }
};