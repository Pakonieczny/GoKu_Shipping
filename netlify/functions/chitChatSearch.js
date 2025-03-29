// netlify/functions/chitChatSearch.js
const fetch = require("node-fetch");

exports.handler = async function(event, context) {
  try {
    const query = event.queryStringParameters.q;
    console.log("chitChatSearch: Received query:", query);
    if (!query) {
      console.error("chitChatSearch: Missing query parameter 'q'");
      return { statusCode: 400, body: JSON.stringify({ error: "Missing query parameter 'q'" }) };
    }
    
    const clientId = process.env.CHIT_CHATS_CLIENT_ID;
    const accessToken = process.env.CHIT_CHATS_ACCESS_TOKEN;
    console.log("chitChatSearch: Using clientId:", clientId);
    if (!clientId || !accessToken) {
      console.error("chitChatSearch: Missing environment variables.");
      return { statusCode: 500, body: JSON.stringify({ error: "Missing CHIT_CHATS_CLIENT_ID or CHIT_CHATS_ACCESS_TOKEN" }) };
    }
    
    // Option B: Use a dedicated search endpoint.
    // For example, the endpoint might be:
    // https://chitchats.com/api/v1/clients/<YOUR_CLIENT_ID>/orders/search?query=<search_term>
    const apiUrl = `https://chitchats.com/api/v1/clients/${clientId}/orders/search?query=${encodeURIComponent(query)}`;
    console.log("chitChatSearch Option B: Calling API URL:", apiUrl);
    
    const response = await fetch(apiUrl, {
      method: "GET",
      headers: {
        "Authorization": `Bearer ${accessToken}`,
        "Content-Type": "application/json"
      }
    });
    
    console.log("chitChatSearch Option B: API response status:", response.status);
    const rawText = await response.text();
    console.log("chitChatSearch Option B: Raw response text:", rawText);
    
    let data;
    try {
      data = JSON.parse(rawText);
    } catch (err) {
      console.error("chitChatSearch Option B: Error parsing JSON:", err);
      data = { raw: rawText };
    }
    
    console.log("chitChatSearch Option B: API response data:", data);
    return { statusCode: response.status, body: JSON.stringify(data) };
  } catch (error) {
    console.error("chitChatSearch Option B: Caught error:", error);
    return { statusCode: 500, body: JSON.stringify({ error: error.message }) };
  }
};