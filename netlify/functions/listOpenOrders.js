/**
 * listOpenOrders.js  –  returns open receipts ordered by OLDEST ship date.
 * Pagination is performed AFTER sorting:
 *   offset=0  →  oldest 100        (ships soonest)
 *   offset=100→  next-oldest 100   …
 */
const fetch = require("node-fetch");

exports.handler = async (event) => {
  try {
    /* 1.  OAuth token */
    const accessToken =
      event.headers["access-token"] || event.headers["Access-Token"];
    if (!accessToken) {
      return { statusCode: 400, body: JSON.stringify({ error: "Missing access-token header" }) };
    }

    /* 2.  Required env vars */
    const SHOP_ID   = process.env.SHOP_ID;
    const CLIENT_ID = process.env.CLIENT_ID;
    if (!SHOP_ID || !CLIENT_ID) {
      return { statusCode: 500, body: JSON.stringify({ error: "Missing SHOP_ID or CLIENT_ID env var" }) };
    }

    /* 3.  figure out which slice the browser is asking for */
    const browserOffset = Number(event.queryStringParameters.offset || 0);

    /* 4.  pull **all** open receipts once (offset-walk) */
    const allReceipts = [];
    let offset = 0;
    do {
      const qs = new URLSearchParams({
        status       : "open",
        was_paid     : "true",
        was_shipped  : "false",
        was_canceled : "false",
        limit        : "100",
        offset       : offset.toString(),
        sort_on      : "created",      // any order is fine – we’ll sort later
        sort_order   : "desc"
      });

      const url = `https://api.etsy.com/v3/application/shops/${SHOP_ID}/receipts?${qs}`;
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
      allReceipts.push(...(data.results || []));
      offset = data.pagination?.next_offset ?? null;
    } while (offset !== null);

    /* 5.  add a numeric ship-timestamp to every receipt */
    allReceipts.forEach(r => {
      let ts = null;
      if (Array.isArray(r.transactions) && r.transactions[0]?.expected_ship_date) {
        ts = r.transactions[0].expected_ship_date;
      }
      if (!ts) ts = r.dispatch_date || r.ship_by_date || 0;
      r.__shipTS = ts;         // 0 for “unknown”
    });

    /* 6.  sort oldest-shipping → newest-shipping (0’s go last) */
    allReceipts.sort((a, b) => {
      if (a.__shipTS === 0 && b.__shipTS === 0) return 0;
      if (a.__shipTS === 0) return  1;  // push “no date” to bottom
      if (b.__shipTS === 0) return -1;
      return a.__shipTS - b.__shipTS;
    });

    /* 7.  slice the exact 100-row page the browser wants */
    const page = allReceipts.slice(browserOffset, browserOffset + 100);

    /* 8.  wrap like Etsy’s payload so front-end stays unchanged */
    const payload = {
      results    : page,
      pagination : {
        next_offset : (browserOffset + 100 < allReceipts.length) ? browserOffset + 100 : null
      }
    };
    return { statusCode: 200, body: JSON.stringify(payload) };

  } catch (err) {
    return { statusCode: 500, body: JSON.stringify({ error: err.message }) };
  }
};