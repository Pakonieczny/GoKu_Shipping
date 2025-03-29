// netlify/functions/chitChatSearch.js
const fetch = require("node-fetch");

exports.handler = async function(event, context) {
  try {
    // Retrieve the search query parameter
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
    
    // Construct the correct API URL.
    // According to the docs, the endpoint is:
    // https://chitchats.com/api/v1/clients/<YOUR_CLIENT_ID>/orders?search=<query>
    const apiUrl = `https://chitchats.com/api/v1/clients/${clientId}/orders?search=${encodeURIComponent(query)}`;
    console.log("chitChatSearch: Calling API URL:", apiUrl);
    
    // Make the API call.
    const response = await fetch(apiUrl, {
      method: "GET",
      headers: {
        "Authorization": `Bearer ${accessToken}`,
        "Content-Type": "application/json"
      }
    });
    
    console.log("chitChatSearch: API response status:", response.status);
    const data = await response.json();
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