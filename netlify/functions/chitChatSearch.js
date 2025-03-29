// netlify/functions/chitChatSearch.js
const fetch = require("node-fetch");

exports.handler = async function(event, context) {
  try {
    // Retrieve the search query parameter.
    const query = event.queryStringParameters.q;
    console.log("chitChatSearch: Received query:", query);
    if (!query) {
      console.error("chitChatSearch: Missing query parameter 'q'");
      return { 
        statusCode: 400, 
        body: JSON.stringify({ error: "Missing query parameter 'q'" })
      };
    }
    
    // Retrieve your Chit Chats API credentials from environment variables.
    const clientId = process.env.CHIT_CHATS_CLIENT_ID;
    const accessToken = process.env.CHIT_CHATS_ACCESS_TOKEN;
    console.log("chitChatSearch: Using clientId:", clientId);
    if (!clientId || !accessToken) {
      console.error("chitChatSearch: Missing CHIT_CHATS_CLIENT_ID or CHIT_CHATS_ACCESS_TOKEN in environment variables.");
      return {
        statusCode: 500,
        body: JSON.stringify({ error: "Missing CHIT_CHATS_CLIENT_ID or CHIT_CHATS_ACCESS_TOKEN" })
      };
    }
    
    // Construct the API URL using an alternate endpoint pattern.
    // This new URL uses: 
    // https://chitchats.com/api/v1/orders/search?client_id=<clientId>&query=<search_term>
    const apiUrl = `https://chitchats.com/api/v1/orders/search?client_id=${clientId}&query=${encodeURIComponent(query)}`;
    console.log("chitChatSearch: Calling API URL:", apiUrl);
    
    // Make the API call using the stored credentials.
    const response = await fetch(apiUrl, {
      method: "GET",
      headers: {
        "Authorization": `Bearer ${accessToken}`,
        "Content-Type": "application/json"
      }
    });
    
    console.log("chitChatSearch: API response status:", response.status);
    const rawText = await response.text();
    console.log("chitChatSearch: Raw response text:", rawText);
    
    let data;
    try {
      data = JSON.parse(rawText);
    } catch (err) {
      console.error("chitChatSearch: Error parsing JSON:", err);
      data = { raw: rawText };
    }
    
    console.log("chitChatSearch: API response data:", data);
    
    return {
      statusCode: response.status,
      body: JSON.stringify(data)
    };
  } catch (error) {
    console.error("chitChatSearch: Caught error:", error);
    return {
      statusCode: 500,
      body: JSON.stringify({ error: error.message })
    };
  }
};