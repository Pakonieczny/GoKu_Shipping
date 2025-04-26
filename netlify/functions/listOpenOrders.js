// /netlify/functions/listOpenOrders.js
const fetch = require("node-fetch");

exports.handler = async (event) => {
  try {
    /* — 1) auth token from front-end — */
    const accessToken =
      event.headers["access-token"] || event.headers["Access-Token"];
    if (!accessToken) {
      return {
        statusCode: 400,
        body: JSON.stringify({ error: "Missing access token header" })
      };
    }

    /* — 2) env vars you already set for other functions — */
    const SHOP_ID    = process.env.SHOP_ID;     // your Etsy shop numeric ID
    const CLIENT_ID  = process.env.CLIENT_ID;   // Etsy app keystring
    if (!SHOP_ID || !CLIENT_ID) {
      return {
        statusCode: 500,
        body: JSON.stringify({ error: "Missing SHOP_ID or CLIENT_ID env var" })
      };
    }

    /* — 3) call Etsy Receipts endpoint for OPEN orders — */
    const etsyUrl =
      `https://api.etsy.com/v3/application/shops/${SHOP_ID}/receipts` +
      `?status=open&limit=100&sort_on=created&sort_order=desc`;

    const resp = await fetch(etsyUrl, {
      method:  "GET",
      headers: {
        "Authorization": `Bearer ${accessToken}`,
        "x-api-key":     CLIENT_ID,
        "Content-Type":  "application/json"
      }
    });

    const data = await resp.json();
    return { statusCode: resp.status, body: JSON.stringify(data) };

  } catch (err) {
    return { statusCode: 500, body: JSON.stringify({ error: err.message }) };
  }
};