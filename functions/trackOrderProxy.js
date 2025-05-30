// trackOrderProxy.js  — proxy → Etsy
const fetch = require("node-fetch");

exports.handler = async event => {
  try {
    const { receiptId, tracking, carrier } = JSON.parse(event.body || "{}");
    const token  = event.headers["access-token"] || "";
    const shopId = process.env.SHOP_ID;     // set in Netlify env
    const apiKey = process.env.CLIENT_ID;   // Etsy “Client ID”

    /* basic validation */
    if (!receiptId || !tracking || !carrier)
      return { statusCode: 400,
               body: JSON.stringify({ error: "Missing receiptId / tracking / carrier" }) };

    /* build Etsy endpoint */
    const url = `https://openapi.etsy.com/v3/application/shops/${shopId}` +
                `/receipts/${receiptId}/tracking`;

    /* Etsy wants JSON in v3 */
    const body = JSON.stringify({
      tracking_code : tracking,
      carrier_name  : carrier,
      notify_buyer  : true,   // emails buyer
      send_bcc      : true    // emails seller
    });

    const etsyResp = await fetch(url, {
      method : "POST",
      headers: {
        "Authorization": `Bearer ${token}`,
        "x-api-key"    : apiKey,
        "Content-Type" : "application/json"
      },
      body
    });

    /* pass Etsy’s raw status back so the client can act on 401/403 */
    return {
      statusCode: etsyResp.status,
      body      : await etsyResp.text()
    };
  } catch (err) {
    return { statusCode: 500,
             body: JSON.stringify({ error: err.message }) };
  }
};