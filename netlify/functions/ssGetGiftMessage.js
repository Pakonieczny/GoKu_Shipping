// netlify/functions/ssGetGiftMessage.js
// Node 18 runtime (CommonJS)

const fetch = require("node-fetch");   // bundled by Netlify

exports.handler = async (event) => {
  const orderNumber = (event.queryStringParameters?.orderNumber || "").trim();
  if (!orderNumber)
    return { statusCode: 400, body: "Missing orderNumber" };

  // ── ShipStation Basic-Auth ──
  const SS_API_KEY = (process.env.SS_API_KEY || "").trim();
  const SS_API_SECRET = (process.env.SS_API_SECRET || "").trim();
  if (!SS_API_KEY || !SS_API_SECRET) {
    return {
      statusCode: 503,
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ code: "SHIPSTATION_NOT_CONFIGURED", error: "ShipStation connection is not configured. Check SS_API_KEY and SS_API_SECRET in Netlify." })
    };
  }
  const AUTH = "Basic " + Buffer.from(`${SS_API_KEY}:${SS_API_SECRET}`).toString("base64");
  const base = "https://ssapi.shipstation.com";

  // helper: query ShipStation endpoint, return first order or null
  const query = async (url) => {
    const r = await fetch(url, { headers: { Authorization: AUTH } });
    if (r.status === 404) return null;           // nothing found at this URL
    if (!r.ok) {
      const err = new Error(
        r.status === 401 || r.status === 403
          ? "ShipStation rejected the connection. Check the V1 API key, secret, and account API access."
          : r.status === 429
            ? "ShipStation request limit reached. Please try again shortly."
            : `ShipStation lookup failed (HTTP ${r.status}). Please try again.`
      );
      err.statusCode = r.status;
      throw err;
    }
    const j = await r.json();
    return j.orders?.[0] || null;
  };

  try {
    // 1️⃣  Try orderNumber field first, then fall back to customerOrderId
    let order =
      await query(`${base}/orders?orderNumber=${encodeURIComponent(orderNumber)}`) ||
      await query(`${base}/orders?customerOrderId=${encodeURIComponent(orderNumber)}`);

    // 2️⃣  Final fallback: advancedsearch (matches partials, prefixes, etc.)
    if (!order) {
      order = await query(
        `${base}/orders/advancedsearch?orderNumber=${encodeURIComponent(orderNumber)}&page=1&pageSize=1`
      );
    }

    if (!order)
      return { statusCode: 404, body: "Order not found in ShipStation" };

    return {
      statusCode: 200,
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        giftMessage: order.giftMessage?.trim() || "",
        giftFrom:
          order.giftMessageFrom?.trim() ||
          order.billTo?.name?.trim() ||
          ""
      })
    };
  } catch (err) {
    console.error("[ssGetGiftMessage] lookup failed", err.statusCode || 502);
    return {
      statusCode: err.statusCode || 502,
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ error: err.statusCode ? err.message : "Unable to reach ShipStation. Please try again." })
    };
  }
};
