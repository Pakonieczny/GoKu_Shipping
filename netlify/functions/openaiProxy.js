const fetch = require("node-fetch");

// CORS so the in-grid editor on britesjewelry.com can call this cross-origin.
// (The Listing Generator runs same-origin on goldenspike.app and didn't need this.)
const CORS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "Content-Type, Authorization",
  "Access-Control-Allow-Methods": "POST, OPTIONS",
  "Content-Type": "application/json"
};

exports.handler = async (event, context) => {
  // Answer the browser's CORS preflight before anything else.
  if (event.httpMethod === "OPTIONS") {
    return { statusCode: 200, headers: CORS, body: "" };
  }
  try {
    console.log("openaiProxy received event:", event);

    // Parse the incoming payload
    let payload = JSON.parse(event.body);

    // Model: default to gpt-4o-mini (keeps existing callers unchanged), but allow
    // a caller to request a specific model via "model_override" (e.g. Smart Match
    // uses gpt-5.4-mini). model_override is stripped before forwarding to OpenAI.
    payload.model = payload.model_override || "gpt-4o-mini";
    delete payload.model_override;

    // If the payload does not already contain a "messages" array,
    // and an "image" field is provided, then construct the proper messages array.
    if (!payload.messages) {
      if (payload.image) {
        // Use the provided prompt if it exists; otherwise, default to a standard prompt.
        const promptText = payload.prompt || "Describe this image.";
        // Use the provided detail level or default to "high"
        const detail = payload.detail || "high";

        // Construct the required messages array.
        payload.messages = [
          {
            role: "user",
            content: [
              { type: "text", text: promptText },
              {
                type: "image_url",
                image_url: {
                  url: payload.image,
                  detail: detail
                }
              }
            ]
          }
        ];
      } else {
        throw new Error("Missing required parameter: messages or image");
      }
    }

    // Remove top-level keys that are not expected by the API.
    delete payload.prompt;
    delete payload.image;
    delete payload.detail;

    console.log("Final payload to be sent:", JSON.stringify(payload, null, 2));

    // Retrieve the OpenAI API key from the environment.
    const apiKey = process.env.OPENAI_API_KEY;
    if (!apiKey) {
      throw new Error("Missing OPENAI_API_KEY environment variable");
    }

    // Define the OpenAI endpoint for chat completions (vision-capable)
    const endpoint = "https://api.openai.com/v1/chat/completions";
    console.log("Forwarding request to OpenAI endpoint:", endpoint);

    // Forward the request exactly as built.
    const response = await fetch(endpoint, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Authorization": `Bearer ${apiKey}`
      },
      body: JSON.stringify(payload)
    });

    const data = await response.json();
    if (!response.ok) {
      console.error("OpenAI API error:", data);
      return {
        statusCode: response.status,
        headers: CORS,
        body: JSON.stringify({ error: data })
      };
    }

    console.log("OpenAI API response:", data);
    return {
      statusCode: 200,
      headers: CORS,
      body: JSON.stringify(data)
    };
  } catch (error) {
    console.error("Error in openaiProxy function:", error);
    return {
      statusCode: 500,
      headers: CORS,
      body: JSON.stringify({ error: error.message })
    };
  }
};