// ssGetOrderAddress.js — read-only ShipStation lookup by orderNumber
// Env vars required in Netlify:
//   SS_API_KEY, SS_API_SECRET

const fetch = require("node-fetch");

exports.handler = async (event) => {
  try {
    const orderNumber = (event.queryStringParameters?.orderNumber || "").trim();
    if (!orderNumber) {
      return { statusCode: 400, body: JSON.stringify({ error: "Missing orderNumber" }) };
    }

    const key    = process.env.SS_API_KEY || "";
    const secret = process.env.SS_API_SECRET || "";
    if (!key || !secret) {
      return { statusCode: 500, body: JSON.stringify({ error: "ShipStation credentials not configured" }) };
    }

    const baseURL = "https://ssapi.shipstation.com";
    const headers = {
      Authorization: "Basic " + Buffer.from(`${key}:${secret}`).toString("base64")
    };

    // 1) Look up order by its orderNumber (your Etsy receiptId)
    const listURL = `${baseURL}/orders?orderNumber=${encodeURIComponent(orderNumber)}`;
    const listResp = await fetch(listURL, { headers });
    if (!listResp.ok) {
      const text = await listResp.text();
      return { statusCode: listResp.status, body: text };
    }
    const orders = await listResp.json();

    const order = Array.isArray(orders) ? orders[0] : (orders?.orders?.[0] || null);
    if (!order) {
      return { statusCode: 404, body: JSON.stringify({ error: "Order not found" }) };
    }

       const {
         orderId,
         orderNumber: on,
         shipTo = null,
         gift = false,
         giftMessage = "",
         customerNotes = "",
         carrierCode = "",
         serviceCode = "",
         shippingAmount = null,
         items = []
       } = order;

      return {
        statusCode: 200,
        headers: { "Content-Type": "application/json" },
         body: JSON.stringify({
           orderId,
           orderNumber: on,
           shipTo,
           gift,
           giftMessage,
           customerNotes,
           carrierCode,
           serviceCode,
           shippingAmount,
           items: (Array.isArray(items) ? items : []).map(i => ({
             name: i.name,
             sku: i.sku || null,
             quantity: Number(i.quantity || 1),
             unitPrice: typeof i.unitPrice === "number" ? i.unitPrice : Number(i.unitPrice || 0),
             originCountry: (i.productCountryOfOrigin || "").toUpperCase() || null,
             hsCode: i.productHarmonizedCode || null
           }))
         })
      };
  } catch (err) {
    return { statusCode: 500, body: JSON.stringify({ error: err.message }) };
  }
};