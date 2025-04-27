/**
 * listOpenOrders.js – returns **exactly** 100 “Paid” & **un-shipped** receipts
 * per call (except the last page). `offset` now refers to the *filtered* list.
 */
const fetch = require("node-fetch");

exports.handler = async (event) => {
  try {
    /* 1 – OAuth & env-vars -------------------------------------------------- */
    const accessToken =
      event.headers["access-token"] || event.headers["Access-Token"];
    if (!accessToken) {
      return { statusCode: 400, body: JSON.stringify({ error: "Missing access-token header" }) };
    }

    const SHOP_ID   = process.env.SHOP_ID;   // numeric
    const CLIENT_ID = process.env.CLIENT_ID; // app key
    if (!SHOP_ID || !CLIENT_ID) {
      return { statusCode: 500, body: JSON.stringify({ error: "Missing SHOP_ID or CLIENT_ID env var" }) };
    }

    /* 2 – pagination logic -------------------------------------------------- */
    const PAGE_SIZE      = 100;                               // always 100
    const filteredOffset = Number(event.queryStringParameters.offset || 0); // within filtered list

    let apiOffset   = 0;          // Etsy’s raw offset
    let allFiltered = [];         // collected “Paid & un-shipped”
    let nextApiOff  = null;       // remember next_offset from Etsy

    while (allFiltered.length < filteredOffset + PAGE_SIZE) {
      /* build Etsy URL ----------------------------------------------------- */
      const qs = new URLSearchParams({
        status     : "open",
        limit      : "100",
        offset     : apiOffset.toString(),
        sort_on    : "created",
        sort_order : "desc"
      });
      const url = `https://api.etsy.com/v3/application/shops/${SHOP_ID}/receipts?${qs}`;

      /* fetch one raw page -------------------------------------------------- */
      const resp = await fetch(url, {
        method  : "GET",
        headers : {
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
      nextApiOff = data?.pagination?.next_offset ?? null;

      /* keep only Paid & not-shipped --------------------------------------- */
      const filtered = (data.results || []).filter(
        (r) => r.status === "Paid" && r.is_shipped === false
      );
      allFiltered.push(...filtered);

      /* stop if Etsy has no more pages ------------------------------------- */
      if (nextApiOff === null) break;
      apiOffset = nextApiOff;
    }

    /* 3 – slice out this page & build response ----------------------------- */
    const pageSlice   = allFiltered.slice(filteredOffset, filteredOffset + PAGE_SIZE);
    const moreExists  =
      allFiltered.length > filteredOffset + PAGE_SIZE || nextApiOff !== null;

    const payload = {
      results    : pageSlice,
      pagination : { next_offset: moreExists ? filteredOffset + PAGE_SIZE : null }
    };

    return { statusCode: 200, body: JSON.stringify(payload) };
  } catch (err) {
    return { statusCode: 500, body: JSON.stringify({ error: err.message }) };
  }
};