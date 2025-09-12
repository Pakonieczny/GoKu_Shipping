// netlify/functions/etsyOrderProxy.js  — DROP-IN REPLACEMENT
// Returns buyer selections via transactions endpoint.
// Env vars expected: SHOP_ID, CLIENT_ID
// Header expected from client: "access-token: <OAuth Bearer>"
// Optional: "Authorization: Bearer <token>"

const fetch = require("node-fetch");

exports.handler = async function (event) {
  try {
    // ---- Inputs & Env ----------------------------------------------------
    const receiptId = (event.queryStringParameters?.orderId || "").trim(); // Etsy receipt_id
    if (!receiptId) return json(400, { error: "Missing orderId (receipt_id)" });

    // Accept multiple header spellings + Authorization
    const headerToken =
      event.headers["access-token"] ||
      event.headers["Access-Token"] ||
      event.headers["authorization"] ||
      event.headers["Authorization"];

    const accessToken = headerToken
      ? headerToken.replace(/^Bearer\s+/i, "")
      : "";

    if (!accessToken) return json(401, { error: "Missing access token" });

    const shopId = process.env.SHOP_ID;
    const apiKey = process.env.CLIENT_ID; // Etsy calls this the Keystring/client id
    if (!shopId || !apiKey) {
      return json(500, { error: "Server missing SHOP_ID or CLIENT_ID" });
    }

    // ---- Etsy v3: transactions by receipt -------------------------------
    // Keep domain consistent with your existing codebase.
    const base = "https://openapi.etsy.com/v3/application";
    const url = `${base}/shops/${encodeURIComponent(
      shopId
    )}/receipts/${encodeURIComponent(receiptId)}/transactions?limit=100`;

    const resp = await fetch(url, {
      method: "GET",
      headers: {
        "x-api-key": apiKey,
        Authorization: `Bearer ${accessToken}`,
        Accept: "application/json",
      },
    });

    // Non-2xx: bubble up details to help debugging
    if (!resp.ok) {
      const detail = await safeText(resp);
      return json(resp.status, {
        error: "Etsy API error",
        status: resp.status,
        detail: detail || resp.statusText,
      });
    }

    const data = await resp.json();
    const transactions = Array.isArray(data?.results) ? data.results : [];

    // Normalize shape for front-end use (variations included)
    const out = {
      receipt_id: receiptId,
      transactions: transactions.map((t) => ({
        transaction_id: t.transaction_id,
        listing_id: t.listing_id,
        product_id: t.product_id,
        title: t.title,
        quantity: t.quantity,
        price: t.price,
        is_gift: t.is_gift,
        personalization: t.personalization,
        variations: t.variations,               // ← buyer’s chosen options (formatted_name/value)
        selected_variations: t.selected_variations, // sometimes present
        product_data: t.product_data,           // contains property_values in some flows
        expected_ship_date: t.expected_ship_date,
      })),
      // Optional: keep raw for diagnostics (comment out if you prefer lean payloads)
      raw: data,
    };

    return json(200, out);
  } catch (err) {
    return json(500, { error: "Proxy failure", detail: String(err?.message || err) });
  }
};

function json(status, body) {
  return {
    statusCode: status,
    headers: {
      "Content-Type": "application/json",
      "Access-Control-Allow-Origin": "*",
    },
    body: JSON.stringify(body),
  };
}

async function safeText(resp) {
  try {
    return await resp.text();
  } catch {
    return "";
  }
}