// netlify/functions/chitChatSearch.js
const fetch = require("node-fetch");

exports.handler = async function(event, context) {
  try {
    // Retrieve the search query parameter
    const query = event.queryStringParameters.q;
    if (!query) {
      return { 
        statusCode: 400, 
        body: JSON.stringify({ error: "Missing query parameter 'q'" })
      };
    }

    // Retrieve your Chit Chats API credentials from environment variables.
    const clientId = process.env.CHIT_CHATS_CLIENT_ID;
    const accessToken = process.env.CHIT_CHATS_ACCESS_TOKEN;
    if (!clientId || !accessToken) {
      return {
        statusCode: 500,
        body: JSON.stringify({ error: "Missing CHIT_CHATS_CLIENT_ID or CHIT_CHATS_ACCESS_TOKEN in environment variables." })
      };
    }

    // Construct the Chit Chats API URL for order search.
    // (According to https://chitchats.com/docs/api/v1, adjust the endpoint as needed.)
    const apiUrl = `https://chitchats.com/api/v1/orders?search=${encodeURIComponent(query)}`;

    // Make the API call using the stored credentials.
    const response = await fetch(apiUrl, {
      method: "GET",
      headers: {
        "Authorization": `Bearer ${accessToken}`,
        "Content-Type": "application/json",
        "x-api-key": clientId
      }
    });
    const data = await response.json();
    
    return {
      statusCode: response.status,
      body: JSON.stringify(data)
    };
  } catch (error) {
    console.error("Error in chitChatSearch:", error);
    return {
      statusCode: 500,
      body: JSON.stringify({ error: error.message })
    };
  }
};