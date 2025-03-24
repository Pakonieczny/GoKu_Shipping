const fetch = require('node-fetch');

exports.handler = async function(event, context) {
  try {
    // Expect an orderId to be provided via query parameters
    const orderId = event.queryStringParameters.orderId;
    // Retrieve the access token from headers
    const accessToken = event.headers['access-token'] || event.headers['Access-Token'];
    // Retrieve your shop ID from environment variables
    const shopId = process.env.SHOP_ID;
    
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
    if (!shopId) {
      return { 
        statusCode: 400, 
        body: JSON.stringify({ error: "Missing SHOP_ID environment variable" }) 
      };
    }
    
    // Construct the Etsy API URL for pulling order (receipt) information.
    // Etsy API v3 uses receipts to represent order details.
    const etsyUrl = `https://api.etsy.com/v3/application/shops/${shopId}/receipts/${orderId}`;
    
    const response = await fetch(etsyUrl, {
      method: "GET",
      headers: {
        "Authorization": `Bearer ${accessToken}`,
        "Content-Type": "application/json"
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