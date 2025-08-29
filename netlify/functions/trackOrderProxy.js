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

    // Build a storeId → marketplaceName map so we can prefer Etsy orders
      async function loadStoreMap(headers, baseURL) {
        try {
          const r = await fetch(`${baseURL}/stores`, { headers });
          if (!r.ok) return new Map();
          const stores = await r.json();
          const m = new Map();
          for (const s of stores || []) m.set(s.storeId, String(s.marketplaceName || "").toLowerCase());
          return m;
        } catch {
          return new Map();
        }
      }

    /* 1️⃣  Find the ShipStation order that matches the Etsy receiptId */
    const lookupURL = `${baseURL}/orders?orderNumber=${encodeURIComponent(orderNumber)}`;
    const lookupResp = await fetch(lookupURL, { headers });

    if (!lookupResp.ok) {
      return { statusCode: lookupResp.status, body: await lookupResp.text() };
    }

    const { orders } = await lookupResp.json();
    if (!orders || !orders.length) {
      return { statusCode: 404, body: "Order not found in ShipStation" };
    }

    // Prefer the Etsy store’s copy if multiples exist
    const storeMap = await loadStoreMap(headers, baseURL);
    const etsyOrder = orders.find(o => /etsy/.test(storeMap.get(o.storeId) || ""));
    const chosen    = etsyOrder || orders[0];
    const orderId   = chosen?.orderId;

    if (!orderId) {
      return { statusCode: 404, body: "Order not found in ShipStation (no Etsy match)" };
    }

    /* 2️⃣  Mark the order as shipped (ShipStation notifies Etsy automatically) */
    const markResp = await fetch(`${baseURL}/orders/markasshipped`, {
      method: "POST",
      headers: { ...headers, "Content-Type": "application/json" },
      body: JSON.stringify({
        orderId,
        carrierCode,
        trackingNumber,
        shipDate: shipDate || new Date().toISOString().slice(0, 10),
        notifyCustomer: true,
        notifySalesChannel: true
      })
    });

    return { statusCode: markResp.status, body: await markResp.text() };
  } catch (err) {
    return { statusCode: 500, body: JSON.stringify({ error: err.message }) };
  }
};