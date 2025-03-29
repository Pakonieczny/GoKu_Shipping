// netlify/functions/testChitChats.js
const fetch = require("node-fetch");

exports.handler = async function(event, context) {
  try {
    // Retrieve credentials from environment variables
    const clientId = process.env.CHIT_CHATS_CLIENT_ID;
    const accessToken = process.env.CHIT_CHATS_ACCESS_TOKEN;
    // Use the CHIT_CHATS_BASE_URL if provided; default to staging URL for testing
    const baseUrl = process.env.CHIT_CHATS_BASE_URL || "https://staging.chitchats.com/api/v1";
    
    // Debug logging to verify credentials and header
    console.log("testChitChats: Using clientId:", clientId);
    console.log("testChitChats: Access token used:", accessToken);
    console.log("testChitChats: Authorization header:", `Bearer ${accessToken}`);
    
    if (!clientId || !accessToken) {
      return {
        statusCode: 500,
        body: JSON.stringify({ success: false, error: "Missing CHIT_CHATS_CLIENT_ID or CHIT_CHATS_ACCESS_TOKEN" })
      };
    }
    
    // Construct the shipments endpoint URL with status=ready, limit and page parameters
    const apiUrl = `${baseUrl}/clients/${clientId}/shipments?status=ready&limit=100&page=1`;
    console.log("testChitChats: Full API URL:", apiUrl);
    
    // Make the API call
    const response = await fetch(apiUrl, {
      method: "GET",
      headers: {
        "Authorization": `Bearer ${accessToken}`,
        "Content-Type": "application/json; charset=utf-8"
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