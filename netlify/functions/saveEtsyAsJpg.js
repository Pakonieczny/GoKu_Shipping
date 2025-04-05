// netlify/functions/saveEtsyAsJpg.js

const fs = require("fs");
const path = require("path");
const fetch = require("node-fetch");

exports.handler = async function(event) {
  try {
    // Parse ?imageUrl=... from query params
    const { imageUrl } = event.queryStringParameters || {};
    if (!imageUrl) {
      return {
        statusCode: 400,
        body: JSON.stringify({ error: "Missing ?imageUrl=..." })
      };
    }

    // 1) Fetch image from Etsy (server-side => no CORS issue)
    const resp = await fetch(imageUrl);
    if (!resp.ok) {
      return {
        statusCode: resp.status,
        body: JSON.stringify({ error: `Failed to fetch image: ${resp.status}` })
      };
    }
    const buffer = await resp.buffer();

    // 2) Write it to /tmp as .jpg
    const fileName = `etsy_${Date.now()}.jpg`;
    const filePath = path.join("/tmp", fileName);
    fs.writeFileSync(filePath, buffer);

    // 3) Return path to the caller
    return {
      statusCode: 200,
      body: JSON.stringify({
        status: "ok",
        savedPath: filePath,
        message: `Saved Etsy image to /tmp as ${fileName}`
      })
    };
  } catch (err) {
    console.error("saveEtsyAsJpg error:", err);
    return {
      statusCode: 500,
      body: JSON.stringify({ error: err.message })
    };
  }
};