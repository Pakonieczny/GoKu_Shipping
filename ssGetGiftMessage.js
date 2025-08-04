// netlify/functions/ssGetGiftMessage.js
const fetch = require("node-fetch");              // Netlify ▸ Node 18 runtime

exports.handler = async (event) => {
  const orderNumber = event.queryStringParameters?.orderNumber;
  if (!orderNumber)
    return { statusCode: 400, body: "Missing orderNumber" };

  // ── ShipStation auth (Basic Auth = key:secret, Base64-encoded) ──
  const { SS_API_KEY, SS_API_SECRET } = process.env;
  const auth = Buffer.from(`${SS_API_KEY}:${SS_API_SECRET}`).toString("base64");

  const url = `https://ssapi.shipstation.com/orders?orderNumber=${encodeURIComponent(orderNumber)}`;

  try {
    const resp = await fetch(url, { headers: { Authorization: `Basic ${auth}` } });
    if (!resp.ok)
      return { statusCode: resp.status, body: await resp.text() };

    const json = await resp.json();
    const order = json.orders?.[0];
    if (!order)
      return { statusCode: 404, body: "Order not found" };

    return {
      statusCode: 200,
      body: JSON.stringify({
        giftMessage: order.giftMessage ?? "",
        giftFrom   : order.giftMessageFrom ?? order.billTo?.name ?? ""
      })
    };
  } catch (err) {
    return { statusCode: 500, body: err.message };
  }
};