// netlify/functions/imageProxy.js
const fetch = require("node-fetch");

exports.handler = async function(event, context) {
  const { url } = event.queryStringParameters;
  if (!url) {
    return {
      statusCode: 400,
      body: "Missing 'url' query parameter"
    };
  }
  try {
    // Fetch the image from the provided URL
    const response = await fetch(url);
    const contentType = response.headers.get("content-type") || "application/octet-stream";
    const buffer = await response.buffer();

    return {
      statusCode: 200,
      headers: {
        "Content-Type": contentType,
        "Access-Control-Allow-Origin": "*" // Allow cross-origin requests
      },
      // Return the image as a base64-encoded string
      body: buffer.toString("base64"),
      isBase64Encoded: true
    };
  } catch (error) {
    return {
      statusCode: 500,
      body: JSON.stringify({ error: error.toString() })
    };
  }
};