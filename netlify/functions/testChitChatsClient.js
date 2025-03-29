// netlify/functions/testChitChatsClient.js
const fetch = require("node-fetch");

exports.handler = async function(event, context) {
  try {
    const clientId = process.env.CHIT_CHATS_CLIENT_ID;
    const accessToken = process.env.CHIT_CHATS_ACCESS_TOKEN;
    // Use the staging URL as default for testing
    const baseUrl = process.env.CHIT_CHATS_BASE_URL || "https://staging.chitchats.com/api/v1";
    
    // Debug logging to verify credentials
    console.log("testChitChatsClient: Using clientId:", clientId);
    console.log("testChitChatsClient: Access token used:", accessToken);
    console.log("testChitChatsClient: Authorization header:", `Bearer ${accessToken}`);
    
    if (!clientId || !accessToken) {
      return {
        statusCode: 500,
        body: JSON.stringify({ success: false, error: "Missing CHIT_CHATS_CLIENT_ID or CHIT_CHATS_ACCESS_TOKEN" })
      };
    }
    
    // Construct the API URL using the staging base URL and client ID.
    const apiUrl = `${baseUrl}/clients/${clientId}`;
    console.log("testChitChatsClient: Full API URL:", apiUrl);
    
    const response = await fetch(apiUrl, {
      method: "GET",
      headers: {
        "Authorization": `Bearer ${accessToken}`,
        "Content-Type": "application/json"
      }
    });
    
    console.log("testChitChatsClient: API response status:", response.status);
    const rawText = await response.text();
    console.log("testChitChatsClient: Raw response text:", rawText);
    
    let data;
    try {
      data = JSON.parse(rawText);
    } catch (err) {
      console.error("testChitChatsClient: Error parsing JSON:", err);
      data = { raw: rawText };
    }
    
    console.log("testChitChatsClient: API response data:", data);
    
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
    console.error("testChitChatsClient: Caught error:", error);
    return {
      statusCode: 500,
      body: JSON.stringify({ success: false, error: error.message })
    };
  }
};