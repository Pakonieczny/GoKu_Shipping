// etsyOrderProxy.js
const fetch = require("node-fetch");

exports.handler = async function(event, context) {
  try {
    // Retrieve orderId from query parameters.
    const orderId = event.queryStringParameters.orderId;
    // Retrieve access token from request headers.
    const accessToken = event.headers['access-token'] || event.headers['Access-Token'];
    if (!orderId) {
      return { 
        statusCode: 400, 
        body: JSON.stringify({ error: "Missing orderId parameter" }) 
      };
    }
    if (!accessToken) {
      return { 
        statusCode: 400, 
        body: JSON.stringify({ error: "Missing access token" }) 
      };
    }
    
    // Retrieve your shop ID and CLIENT_ID from environment variables.
    const shopId = process.env.SHOP_ID;
    const clientId = process.env.CLIENT_ID;
    if (!shopId) {
      return {
        statusCode: 500,
        body: JSON.stringify({ error: "Missing SHOP_ID environment variable" })
      };
    }
    if (!clientId) {
      return {
        statusCode: 500,
        body: JSON.stringify({ error: "Missing CLIENT_ID environment variable" })
      };
    }
    
    // Construct the Etsy receipts URL (which represents order details).
    const url = `/v3/application/shops/${shopId}/receipts/${receiptId}/transactions?includes=personalization`;
    
    // Make the GET request to Etsy with the required headers.
    const response = await fetch(etsyUrl, {
      method: "GET",
      headers: {
        "Authorization": `Bearer ${accessToken}`,
        "Content-Type": "application/json",
        "x-api-key": clientId  // Include your CLIENT_ID as the API key.
      }
    });
    
    const data = await response.json();
    return {
      statusCode: response.status,
      body: JSON.stringify(data)
    };
    
  } catch (error) {
    return {
      statusCode: 500,
      body: JSON.stringify({ error: error.message })
    };
  }
};