// netlify/functions/testChitChats.js
const fetch = require("node-fetch");

exports.handler = async function(event, context) {
  try {
    const clientId = process.env.CHIT_CHATS_CLIENT_ID;
    const accessToken = process.env.CHIT_CHATS_ACCESS_TOKEN;
    if (!clientId || !accessToken) {
      return {
        statusCode: 500,
        body: JSON.stringify({ error: "Missing CHIT_CHATS_CLIENT_ID or CHIT_CHATS_ACCESS_TOKEN" })
      };
    }
    
    // Use the shipments endpoint as a simple test.
    const apiUrl = `https://chitchats.com/api/v1/clients/${clientId}/shipments?limit=1&page=1`;
    console.log("testChitChats: Calling API URL:", apiUrl);
    
    const response = await fetch(apiUrl, {
      method: "GET",
      headers: {
        "Authorization": `Bearer ${accessToken}`,
        "Content-Type": "application/json"
      }
    });
    
    console.log("testChitChats: API response status:", response.status);
    const data = await response.json();
    console.log("testChitChats: API response data:", data);
    
    return {
      statusCode: response.status,
      body: JSON.stringify({ success: true, data })
    };
  } catch (error) {
    console.error("testChitChats: Caught error:", error);
    return {
      statusCode: 500,
      body: JSON.stringify({ error: error.message })
    };
  }
};