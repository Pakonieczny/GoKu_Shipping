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
        orderStatus = "",
        shipTo = null,
        gift = false,
        giftMessage = "",
        customerNotes = "",
        carrierCode = "",
        serviceCode = "",
        shippingAmount = null,
        createDate = null,   // ← used for dateOrdered
        orderDate = null,    // ← fallback
        shipByDate = null,   // ← used for shipDate
        shipDate = null,     // ← sometimes present on order
        items = []
      } = order;

      // 🔎 Also fetch shipments so the UI can show tracking + shipped date
      let shipments   = [];
      let shippedDate = shipDate || null; // prefer order.shipDate if present
      try {
        const sUrl  = `${baseURL}/shipments?orderId=${encodeURIComponent(orderId)}`;
        const sResp = await fetch(sUrl, { headers });
        if (sResp.ok) {
          const sJson = await sResp.json();
          const raw   = Array.isArray(sJson.shipments) ? sJson.shipments : [];
          shipments   = raw.map(x => ({
            carrier_name : x.carrierCode || "",
            tracking_code: x.trackingNumber || "",
            shipDate     : x.shipDate || x.createDate || null
          }));
          if (!shippedDate && raw.length) {
            shippedDate = raw[0].shipDate || raw[0].createDate || null;
          }
        }
      } catch (_) { /* non-fatal */ }

      return {
        statusCode: 200,
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          orderId,
          orderNumber: on,
          orderStatus,
          shipTo,
          gift,
          giftMessage,
          customerNotes,
          carrierCode,
          serviceCode,
          shippingAmount,
          // 🗓️ feed the UI
          createDate: createDate || orderDate || null,
          shipByDate: shipByDate || null,
          shippedDate,
          // 🖼️ include image URL hints from SS
          items: (Array.isArray(items) ? items : []).map(i => ({
            name         : i.name,
            sku          : i.sku || null,
            quantity     : Number(i.quantity || 1),
            unitPrice    : typeof i.unitPrice === "number" ? i.unitPrice : Number(i.unitPrice || 0),
            originCountry: (i.productCountryOfOrigin || "").toUpperCase() || null,
            hsCode       : i.productHarmonizedCode || null,
            imageUrl     : i.imageUrl || i.imageURL || i.thumbnailUrl || null
          })),
          shipments
        })
      };
  } catch (err) {
    return { statusCode: 500, body: JSON.stringify({ error: err.message }) };
  }
};