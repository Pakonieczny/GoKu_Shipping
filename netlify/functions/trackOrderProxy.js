// trackOrderProxy.js  — proxy → ShipStation → Etsy
// Requires two Netlify env-vars:
//   SS_API_KEY    = your ShipStation “API Key”
//   SS_API_SECRET = the matching “API Secret”

const fetch = require("node-fetch");

exports.handler = async event => {
  try {
    /* Front-end sends { receiptId, tracking, carrier } — keep names intact */
    const {
      receiptId:   orderNumber,
      tracking:    trackingNumber,
      carrier:     carrierCode,
      shipDate
    } = JSON.parse(event.body || "{}");

    /* Basic validation */
    if (!orderNumber || !trackingNumber || !carrierCode) {
      return {
        statusCode: 400,
        body: JSON.stringify({ error: "Missing orderNumber / trackingNumber / carrierCode" })
      };
    }

    /* ShipStation V1 uses Basic auth: base64(key:secret) */
    const auth = Buffer.from(
      `${process.env.SS_API_KEY}:${process.env.SS_API_SECRET}`
    ).toString("base64");
    const headers = { Authorization: `Basic ${auth}` };
    const baseURL = "https://ssapi.shipstation.com";

    /* 1️⃣  Find the ShipStation order that matches the Etsy receiptId */
    const lookupURL = `${baseURL}/orders?orderNumber=${encodeURIComponent(orderNumber)}`;
    const lookupResp = await fetch(lookupURL, { headers });

    if (!lookupResp.ok) {
      return { statusCode: lookupResp.status, body: await lookupResp.text() };
    }

    const { orders } = await lookupResp.json();
    const orderId = orders?.[0]?.orderId;

    if (!orderId) {
      return { statusCode: 404, body: "Order not found in ShipStation" };
    }

    /* 2️⃣  Mark the order as shipped (ShipStation notifies Etsy automatically) */
    const markResp = await fetch(`${baseURL}/orders/markasshipped`, {
      method: "POST",
      headers: { ...headers, "Content-Type": "application/json" },
       body: JSON.stringify((() => {
         const payload = {
           orderId,
           trackingNumber,
           shipDate: shipDate || new Date().toISOString().slice(0, 10),
           notifyCustomer: true,
           notifySalesChannel: true
         };
         // Map UI "ChitChats" → ShipStation 'other' w/ name
         if ((carrierCode || "").toLowerCase() === "chitchats") {
           payload.carrierCode = "other";
           payload.carrierName = "Chit Chats";
         } else {
           payload.carrierCode = carrierCode;
         }
         return payload;
       })())
    });

    return { statusCode: markResp.status, body: await markResp.text() };
  } catch (err) {
    return { statusCode: 500, body: JSON.stringify({ error: err.message }) };
  }
};