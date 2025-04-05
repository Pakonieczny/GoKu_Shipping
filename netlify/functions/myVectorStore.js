// netlify/functions/myVectorStore.js

const fetch = require("node-fetch");

exports.handler = async function(event) {
  try {
    const apiKey = process.env.OPENAI_API_KEY;
    if (!apiKey) {
      throw new Error("Missing OPENAI_API_KEY environment variable");
    }

    // Parse the incoming request body
    if (!event.body || event.body.trim() === "") {
      throw new Error("No JSON body in request");
    }
    const payload = JSON.parse(event.body);

    // Determine action: "create" or "query"
    // You can add more, e.g. "list", "delete", etc.
    let action = "query";
    if (payload.action === "create") {
      action = "create";
    } else if (payload.action === "query") {
      action = "query";
    }

    // Helper to call OpenAI with the correct headers
    async function openAIRequest(url, method = "GET", bodyObj = null) {
      const headers = {
        "Authorization": `Bearer ${apiKey}`,
        "Content-Type": "application/json"
      };
      const options = {
        method,
        headers
      };
      if (bodyObj) {
        options.body = JSON.stringify(bodyObj);
      }
      const resp = await fetch(url, options);
      if (!resp.ok) {
        const errData = await resp.text();
        throw new Error(`OpenAI error (status ${resp.status}): ${errData}`);
      }
      return await resp.json();
    }

    // Base endpoint for vector stores
    const baseUrl = "https://api.openai.com/v1/vector_stores";

    if (action === "create") {
      // We need a "file_ids" array in the payload, plus optional "name"
      if (!payload.file_ids || !Array.isArray(payload.file_ids) || payload.file_ids.length === 0) {
        throw new Error("No file_ids array provided for create action");
      }
      const dataToSend = {
        name: payload.name || "My Vector Store",
        file_ids: payload.file_ids
      };
      // POST to /v1/vector_stores
      console.log("Creating vector store with:", dataToSend);
      const result = await openAIRequest(baseUrl, "POST", dataToSend);
      return {
        statusCode: 200,
        body: JSON.stringify(result)
      };

    } else if (action === "query") {
      // Query an existing store => we expect "store_id" and "query" in payload
      const storeId = payload.store_id;
      if (!storeId) {
        throw new Error("Missing 'store_id' for query action");
      }
      const queryText = payload.query || "";
      const topK = payload.topK || 10;

      // POST /v1/vector_stores/{storeId}/search
      const searchUrl = `${baseUrl}/${storeId}/search`;
      const bodyObj = {
        query: queryText,
        max_num_results: topK,
        rewrite_query: false
      };
      console.log("Querying store:", storeId, "with:", bodyObj);
      const result = await openAIRequest(searchUrl, "POST", bodyObj);
      return {
        statusCode: 200,
        body: JSON.stringify(result)
      };

    } else {
      // Unknown action
      return {
        statusCode: 400,
        body: JSON.stringify({
          error: "Unknown action. Provide 'action' of 'create' or 'query'."
        })
      };
    }

  } catch (err) {
    console.error("Exception in myVectorStore.js:", err);
    return {
      statusCode: 500,
      body: JSON.stringify({ error: err.message })
    };
  }
};