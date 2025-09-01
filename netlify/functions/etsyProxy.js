const { etsyFetch } = require("./_shared/etsyRateLimiter");

exports.handler = async function(event, context) {
  try {
    const listingId = event.queryStringParameters.listingId;
    const accessToken = event.headers['access-token'] || event.headers['Access-Token'];
    if (!listingId) {
      return { statusCode: 400, body: JSON.stringify({ error: "Missing listingId parameter" }) };
    }
    if (!accessToken) {
      return { statusCode: 400, body: JSON.stringify({ error: "Missing access token" }) };
    }
    const clientId = process.env.CLIENT_ID;
    const etsyUrl  = `https://openapi.etsy.com/v3/application/listings/${listingId}`;
    const response = await etsyFetch(etsyUrl, {
      method: "GET",
      headers: {
        "Authorization": `Bearer ${accessToken}`,
        "x-api-key": clientId,
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