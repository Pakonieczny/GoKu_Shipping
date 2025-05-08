// trackOrderProxy.js  – proxy → Etsy
const fetch = require("node-fetch");

exports.handler = async (event) => {
  try {
    const { receiptId, tracking, carrier } = JSON.parse(event.body || "{}");
    const token  = event.headers["access-token"];
    const shopId = process.env.SHOP_ID;       // put your shop ID in Netlify env
    const apiKey = process.env.CLIENT_ID;     // Etsy “Client ID”

    if (!receiptId || !tracking || !carrier)
      return { statusCode:400, body:JSON.stringify({error:"Missing fields"}) };

    const url = `https://openapi.etsy.com/v3/application/shops/${shopId}` +
                `/receipts/${receiptId}/tracking`;

    const resp = await fetch(url, {
      method:"POST",
      headers:{
        "Authorization":`Bearer ${token}`,
        "x-api-key":apiKey,
        "Content-Type":"application/x-www-form-urlencoded"
      },
      body:new URLSearchParams({ tracking_code:tracking, carrier_name:carrier })
    });

    return { statusCode:resp.status, body:await resp.text() };
  } catch (err) {
    return { statusCode:500, body:JSON.stringify({error:err.message}) };
  }
};