// netlify/functions/listOpenOrders.js
// Returns ALL open receipts (unfulfilled orders) for your shop.
// Walks through Etsy's cursor-based pagination until next_cursor === null.

const fetch = require("node-fetch");

exports.handler = async (event) => {
  try {
    /* 1) access token must come from front-end header */
    const accessToken =
      event.headers["access-token"] || event.headers["Access-Token"];
    if (!accessToken) {
      return {
        statusCode: 400,
        body: JSON.stringify({ error: "Missing access-token header" })
      };
    }

    /* 2) required env vars */
    const SHOP_ID    = process.env.SHOP_ID;
    const CLIENT_ID  = process.env.CLIENT_ID;  // Etsy app keystring
    if (!SHOP_ID || !CLIENT_ID) {
      return {
        statusCode: 500,
        body: JSON.stringify({ error: "Missing SHOP_ID or CLIENT_ID env var" })
      };
    }

    /* 3) loop through pages until next_cursor == null */
    const allReceipts = [];
    let cursor = null;

    do {
      const qs = new URLSearchParams({
        status: "open",
        sort_on: "created",
        sort_order: "desc",
        limit: "100"              // max allowed; pagination removes the cap
      });
      if (cursor) qs.append("cursor", cursor);

      const url =
        `https://api.etsy.com/v3/application/shops/${SHOP_ID}/receipts?` +
        qs.toString();

      const resp = await fetch(url, {
        method:  "GET",
        headers: {
          "Authorization": `Bearer ${accessToken}`,
          "x-api-key":     CLIENT_ID,
          "Content-Type":  "application/json"
        }
      });

      if (!resp.ok) {
        const errText = await resp.text();
        return { statusCode: resp.status, body: errText };
      }

      const data = await resp.json();
      if (Array.isArray(data.results)) allReceipts.push(...data.results);
      cursor = (data.pagination || {}).next_cursor || null;

    } while (cursor);

    return {
      statusCode: 200,
      body: JSON.stringify({ results: allReceipts })
    };

  } catch (err) {
    return { statusCode: 500, body: JSON.stringify({ error: err.message }) };
  }
};