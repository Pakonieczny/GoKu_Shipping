// netlify/functions/testChitChats.js
const fetch = require("node-fetch");

exports.handler = async function(event, context) {
  try {
    const clientId = process.env.CHIT_CHATS_CLIENT_ID;
    const accessToken = process.env.CHIT_CHATS_ACCESS_TOKEN;
    
    // Debug logging to verify credentials (remove in production)
    console.log("testChitChats: Access token used:", accessToken);
    console.log("testChitChats: Authorization header:", `Bearer ${accessToken}`);
    
    if (!clientId || !accessToken) {
      return {
        statusCode: 500,
        body: JSON.stringify({ success: false, error: "Missing CHIT_CHATS_CLIENT_ID or CHIT_CHATS_ACCESS_TOKEN" })
      };
    }
    
    // Construct the shipments endpoint URL
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
    const rawText = await response.text();
    console.log("testChitChats: Raw response text:", rawText);
    
    let data;
    try {
      data = JSON.parse(rawText);
    } catch (err) {
      console.error("testChitChats: Error parsing JSON:", err);
      data = { raw: rawText };
    }
    
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