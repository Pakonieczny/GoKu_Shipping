// netlify/functions/testChitChats.js
const fetch = require("node-fetch");

exports.handler = async function(event, context) {
  try {
    const clientId = process.env.CHIT_CHATS_CLIENT_ID;
    const accessToken = process.env.CHIT_CHATS_ACCESS_TOKEN;
    if (!clientId || !accessToken) {
      return {
        statusCode: 500,
        body: JSON.stringify({ success: false, error: "Missing CHIT_CHATS_CLIENT_ID or CHIT_CHATS_ACCESS_TOKEN" })
      };
    }
    
    // Use the shipments endpoint as a test.
    const apiUrl = `https://chitchats.com/api/v1/clients/${clientId}/shipments?limit=1&page=1`;
    console.log("testChitChats: Calling API URL:", apiUrl);
    
    const response = await fetch(apiUrl, {
      method: "GET",
      headers: {
        "Authorization": `Bearer ${accessToken}`,
        "Content-Type": "application/json"
      }
    });
    
    const data = await response.json();
    console.log("testChitChats: API response status:", response.status);
    console.log("testChitChats: API response data:", data);
    
    if (response.status === 200) {
      return {
        statusCode: 200,
        body: JSON.stringify({ success: true, data })
      };
    } else {
      return {
        statusCode: response.status,
        body: JSON.stringify({ success: false, error: data.error || data })
      };
    }
  } catch (error) {
    console.error("testChitChats: Caught error:", error);
    return {
      statusCode: 500,
      body: JSON.stringify({ success: false, error: error.message })
    };
  }
};