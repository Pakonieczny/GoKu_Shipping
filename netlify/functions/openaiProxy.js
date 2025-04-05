const fetch = require("node-fetch");

exports.handler = async (event, context) => {
  try {
    console.log("openaiProxy received event:", event);

    // Parse the incoming payload.
    // The payload should already follow the vision guidelines, for example:
    // {
    //   "model": "some-model", // this will be overridden,
    //   "messages": [{
    //     "role": "user",
    //     "content": [
    //       { "type": "text", "text": "What's in this image?" },
    //       {
    //         "type": "image_url",
    //         "image_url": {
    //           "url": "https://upload.wikimedia.org/wikipedia/commons/thumb/d/dd/Gfp-wisconsin-madison-the-nature-boardwalk.jpg/2560px-Gfp-wisconsin-madison-the-nature-boardwalk.jpg",
    //           "detail": "high"
    //         }
    //       }
    //     ]
    //   }],
    //   "max_tokens": 300
    // }
    const payload = JSON.parse(event.body);

    // Force the model to "gpt-4o-mini"
    payload.model = "gpt-4o-mini";

    // Log the final payload (for debugging purposes)
    console.log("Forwarding payload:", JSON.stringify(payload, null, 2));

    // Retrieve the OpenAI API key from environment variables
    const apiKey = process.env.OPENAI_API_KEY;
    if (!apiKey) {
      throw new Error("Missing OPENAI_API_KEY environment variable");
    }

    // Define the endpoint for chat completions (which supports vision inputs)
    const endpoint = "https://api.openai.com/v1/chat/completions";
    console.log("Forwarding request to OpenAI endpoint:", endpoint);

    // Forward the request exactly as received
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