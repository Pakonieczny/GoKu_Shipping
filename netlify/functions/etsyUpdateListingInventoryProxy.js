// netlify/functions/etsyUpdateListingInventoryProxy.js
const fetch = require("node-fetch");

exports.handler = async (event) => {
  try {
    if (event.httpMethod !== "POST") {
      return { statusCode: 405, body: "Method Not Allowed" };
    }

    const accessToken = event.headers["access-token"] || event.headers["Access-Token"];
    const clientId    = process.env.CLIENT_ID;
    if (!accessToken) return { statusCode: 400, body: JSON.stringify({ error: "Missing access token" }) };
    if (!clientId)    return { statusCode: 500, body: JSON.stringify({ error: "Missing CLIENT_ID" }) };

    const body = JSON.parse(event.body || "{}");
    const listingId = body.listing_id;
    const items     = Array.isArray(body.items) ? body.items : [];

    if (!listingId) return { statusCode: 400, body: JSON.stringify({ error: "Missing listing_id" }) };
    if (!items.length) return { statusCode: 400, body: JSON.stringify({ error: "No items provided" }) };

    // 1) Fetch current inventory (we PUT the full document back with SKU edits)
    const getUrl = `https://openapi.etsy.com/v3/application/listings/${listingId}/inventory`;
    const getResp = await fetch(getUrl, {
      headers: {
        Authorization: `Bearer ${accessToken}`,
        "x-api-key": clientId,
        "Content-Type": "application/json"
      }
    });
    const inv = await getResp.json();
    if (!getResp.ok) {
      return { statusCode: getResp.status, body: JSON.stringify(inv) };
    }

    const byId = new Map(items.map(i => [Number(i.product_id), String(i.sku || "").trim()]));

    // 2) Apply SKU changes
    const products = (inv.products || []).map(p => {
      const pid = Number(p.product_id);
      if (byId.has(pid)) {
        p.sku = byId.get(pid);
      }
      return p;
    });

    // 3) PUT back to Etsy
    const putUrl = `https://openapi.etsy.com/v3/application/listings/${listingId}/inventory`;
    const putResp = await fetch(putUrl, {
      method: "PUT",
      headers: {
        Authorization: `Bearer ${accessToken}`,
        "x-api-key": clientId,
        "Content-Type": "application/json"
      },
      body: JSON.stringify({ products })
    });
    const result = await putResp.json();
    if (!putResp.ok) {
      return { statusCode: putResp.status, body: JSON.stringify(result) };
    }

    return { statusCode: 200, body: JSON.stringify({ ok: true, listing_id: listingId }) };
  } catch (err) {
    return { statusCode: 500, body: JSON.stringify({ error: err.message }) };
  }
};