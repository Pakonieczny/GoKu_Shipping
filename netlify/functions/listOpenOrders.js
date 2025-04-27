/**
 * listOpenOrders.js  –  returns EVERY open receipt for your shop.
 * Works by walking Etsy's offset-based pagination until next_offset === null.
 */

const fetch = require("node-fetch");

exports.handler = async (event) => {
  try {
    /* 1.  OAuth token from front-end header */
    const accessToken =
      event.headers["access-token"] || event.headers["Access-Token"];
    if (!accessToken) {
      return {
        statusCode: 400,
        body: JSON.stringify({ error: "Missing access-token header" })
      };
    }

    /* 2.  Required env vars */
    const SHOP_ID   = process.env.SHOP_ID;      // numeric ID of your Etsy shop
    const CLIENT_ID = process.env.CLIENT_ID;    // Etsy app key string
    if (!SHOP_ID || !CLIENT_ID) {
      return {
        statusCode: 500,
        body: JSON.stringify({ error: "Missing SHOP_ID or CLIENT_ID env var" })
      };
    }

    /* 3.  Loop through pages using offset pagination */
    const allReceipts = [];
    let offset = Number(event.queryStringParameters.offset || 0);  // ← updated

    do {
      const qs = new URLSearchParams({
        status     : "open",
        limit      : "100",    // page size
        offset     : offset.toString(),
        sort_on    : "created",
        sort_order : "desc"
      });

      const url =
        `https://api.etsy.com/v3/application/shops/${SHOP_ID}/receipts?` +
        qs.toString();

      const resp = await fetch(url, {
        method: "GET",
        headers: {
          Authorization : `Bearer ${accessToken}`,
          "x-api-key"   : CLIENT_ID,
          "Content-Type": "application/json"
        }
      });

      if (!resp.ok) {
        const txt = await resp.text();
        return { statusCode: resp.status, body: txt };
      }

      const data = await resp.json();
      if (Array.isArray(data.results)) allReceipts.push(...data.results);

      /* ---- find next offset (Etsy returns null when done) ---- */
      const next = (data.pagination || {}).next_offset;
      offset = next === null || next === undefined ? null : next;

    } while (offset !== null);

    /* 4.  Return the merged list */
    return {
      statusCode: 200,
      body: JSON.stringify({ results: allReceipts })
    };

  } catch (err) {
    return { statusCode: 500, body: JSON.stringify({ error: err.message }) };
  }
};