// netlify/functions/chitChatSearch.js
const fetch = require("node-fetch");

exports.handler = async function(event, context) {
  try {
    const query = event.queryStringParameters.q;
    console.log("chitChatSearch: Received query:", query);
    if (!query) {
      console.error("chitChatSearch: Missing query parameter 'q'");
      return { 
        statusCode: 400, 
        body: JSON.stringify({ error: "Missing query parameter 'q'" })
      };
    }
    
    const clientId = process.env.CHIT_CHATS_CLIENT_ID;
    const accessToken = process.env.CHIT_CHATS_ACCESS_TOKEN;
    console.log("chitChatSearch: Using clientId:", clientId);
    if (!clientId || !accessToken) {
      console.error("chitChatSearch: Missing environment variables");
      return {
        statusCode: 500,
        body: JSON.stringify({ error: "Missing CHIT_CHATS_CLIENT_ID or CHIT_CHATS_ACCESS_TOKEN" })
      };
    }
    
    // Construct the Chit Chats API URL – adjust the endpoint as per docs.
    const apiUrl = `https://api.chitchat.com/v1/orders?search=${encodeURIComponent(query)}`;
    console.log("chitChatSearch: Calling API URL:", apiUrl);
    
    const response = await fetch(apiUrl, {
      method: "GET",
      headers: {
        "Authorization": `Bearer ${accessToken}`,
        "Content-Type": "application/json",
        "x-api-key": clientId
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