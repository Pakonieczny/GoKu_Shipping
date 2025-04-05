const fetch = require("node-fetch");

exports.handler = async (event, context) => {
  try {
    console.log("openaiProxy received event:", event);

    // Parse the incoming payload.
    const inputPayload = JSON.parse(event.body);

    // Force the model to "gpt-4o-mini" regardless of what the client sent.
    inputPayload.model = "gpt-4o-mini";

    // If no "messages" array is provided but an "image" field exists, construct a messages array.
    if (!inputPayload.messages) {
      if (inputPayload.image) {
        // Use the provided prompt if available; otherwise, default to a standard prompt.
        const promptText = inputPayload.prompt || "What's in this image?";
        // Use the provided detail level, or default to "auto" (you may choose "low" or "high" based on your needs).
        const detail = inputPayload.detail || "auto";

        // Construct the messages array as specified by the OpenAI vision documentation.
        inputPayload.messages = [
          {
            role: "user",
            content: [
              { type: "text", text: promptText },
              {
                type: "image_url",
                image_url: { url: inputPayload.image, detail: detail }
              }
            ]
          }
        ];
      } else {
        // If neither messages nor an image is provided, throw an error.
        throw new Error("Missing required parameter: 'messages' or 'image'");
      }
    }

    // (Optional) Log the final payload for debugging.
    console.log("Forwarding payload:", JSON.stringify(inputPayload, null, 2));

    // Retrieve the OpenAI API key from the environment.
    const apiKey = process.env.OPENAI_API_KEY;
    if (!apiKey) {
      throw new Error("Missing OPENAI_API_KEY environment variable");
    }

    // Define the endpoint for chat completions (vision-capable requests).
    const endpoint = "https://api.openai.com/v1/chat/completions";
    console.log("Forwarding request to OpenAI endpoint:", endpoint);

    // Forward the request with the complete payload.
    const response = await fetch(endpoint, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Authorization": `Bearer ${apiKey}`
      },
      body: JSON.stringify(inputPayload)
    });

    const data = await response.json();

    if (!response.ok) {
      console.error("OpenAI API error:", data);
      return {
        statusCode: response.status,
        body: JSON.stringify({ error: data })
      };
    }

    console.log("OpenAI API response:", data);
    return {
      statusCode: 200,
      body: JSON.stringify(data)
    };
  } catch (error) {
    console.error("Error in openaiProxy function:", error);
    return {
      statusCode: 500,
      body: JSON.stringify({ error: error.message })
    };
  }
};