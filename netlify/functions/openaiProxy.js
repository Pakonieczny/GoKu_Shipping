const fetch = require("node-fetch");

exports.handler = async (event, context) => {
  try {
    console.log("openaiProxy received event:", event);

    // Parse the payload from the request body
    const payload = JSON.parse(event.body);

    // If "messages" is missing but a "prompt" is provided, use it to create a message
    if (!payload.messages) {
      if (payload.prompt) {
        payload.messages = [{ role: "user", content: payload.prompt }];
      } else {
        throw new Error("Missing required parameter: 'messages'");
      }
    }

    // Remove unsupported keys before forwarding the request
    if (payload.image) {
      delete payload.image;
    }
    if (payload.prompt) {
      delete payload.prompt;
    }

    // Force the model to "gpt-4o-latest"
    payload.model = "gpt-4o-mini";

    // Retrieve the API key from environment variables
    const apiKey = process.env.OPENAI_API_KEY;
    if (!apiKey) {
      throw new Error("Missing OPENAI_API_KEY environment variable");
    }

    // Define the OpenAI endpoint for chat completions
    const endpoint = "https://api.openai.com/v1/chat/completions";
    console.log("Forwarding request to OpenAI endpoint:", endpoint);

    // Forward the request to OpenAI's API with the cleaned payload
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