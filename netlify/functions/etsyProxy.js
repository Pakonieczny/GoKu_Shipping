 const fetch = require('node-fetch');
 const { getValidEtsyAccessToken } = require("./lib/etsyAuth");

exports.handler = async function(event, context) {
  try {
    const listingId = event.queryStringParameters.listingId;

    if (!listingId) {
      return { statusCode: 400, body: JSON.stringify({ error: "Missing listingId parameter" }) };
    }

    const etsyUrl = `https://api.etsy.com/v3/application/listings/${listingId}`;
     const accessToken = await getValidEtsyAccessToken();
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
    return { statusCode: 500, body: JSON.stringify({ error: error.message }) };
  }
};