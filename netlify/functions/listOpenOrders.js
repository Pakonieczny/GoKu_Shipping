/**
 * listOpenOrders.js  –  returns EVERY open receipt for your shop.
 * Works by walking Etsy's offset-based pagination until next_offset === null
 * when offset==0, but fetches ONE page when an explicit offset is sent.
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

    // ====== first requested offset comes from browser (UPDATED) ======
    let offset        = Number(event.queryStringParameters.offset || 0);
    const firstOffset = offset;                 // remember what the browser asked for

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

      /* Always stop after FIRST page — paging is handled by the browser */
      offset = null;

      // ====== break after ONE page when browser passed an explicit offset ======
      if (firstOffset !== 0) offset = null;

      /* 4.  Return Etsy’s payload UNCHANGED so pagination.next_offset is preserved */
      return { statusCode: 200, body: JSON.stringify(data) };

    } while (offset !== null);

    /* (The loop now exits after the return above; this block is unreachable
       but kept to preserve your original structure.) */
    return {
      statusCode: 200,
      body: JSON.stringify({ results: allReceipts })
    };

  } catch (err) {
    return { statusCode: 500, body: JSON.stringify({ error: err.message }) };
  }
};