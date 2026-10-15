// etsyOrderProxy.js  – DROP-IN REPLACEMENT
const fetch = require("node-fetch");

exports.handler = async function (event) {
  try {
    /* ------------------------------------------------------------------
     * 1.  INPUTS & ENV
     * ------------------------------------------------------------------ */
    const orderId     = event.queryStringParameters?.orderId;          // Etsy “receipt_id”
    const headers=Object.fromEntries(Object.entries(event.headers || {}).map(([k,v])=>[k.toLowerCase(),v]));
    const accessToken = headers["access-token"];
    const shopId      = process.env.SHOP_ID;
    const clientId    = process.env.CLIENT_ID;
    const clientSecret = process.env.CLIENT_SECRET || process.env.ETSY_SHARED_SECRET;

    if (!orderId)      return { statusCode: 400, body: JSON.stringify({ error: "Missing orderId parameter" }) };
    if (!accessToken)  return { statusCode: 400, body: JSON.stringify({ error: "Missing access token" }) };
    if (!shopId)       return { statusCode: 500, body: JSON.stringify({ error: "Missing SHOP_ID environment variable" }) };
    if (!clientId)     return { statusCode: 500, body: JSON.stringify({ error: "Missing CLIENT_ID environment variable" }) };
    if (!clientSecret) return { statusCode: 500, body: JSON.stringify({ error: "Missing CLIENT_SECRET (Etsy shared secret) environment variable" }) };

    /* ------------------------------------------------------------------
     * 2.  BUILD URL  — one call returns receipt + transactions
     * ------------------------------------------------------------------ */
    const etsyUrl =
      `https://api.etsy.com/v3/application/shops/${shopId}` +
      `/receipts/${orderId}?includes=` +
      [
        "Transactions",
        "Transactions.personalization",
        "Transactions.variations"       // ← ensure buyer selections are returned
      ].join(",");

    /* ------------------------------------------------------------------
     * 3.  MAKE REQUEST
     * ------------------------------------------------------------------ */
    const response = await fetch(etsyUrl, {
      method: "GET",
      headers: {
        Authorization: `Bearer ${accessToken}`,
        // Etsy now requires keystring:shared_secret
        "x-api-key": `${clientId}:${clientSecret}`,
        "Content-Type": "application/json"
      }
    });

    // A body that is not JSON (an HTML page from Etsy's edge, a cut body) keeps Etsy's own status: a 429 must still pause the
    // station and a 404 must still mean "gone", where this used to answer 500 for all of them; a 200 that was cut is a failure.
    let payload = null; try { payload = await response.json(); } catch (_) {}
    if (payload === null) return { statusCode: response.ok ? 502 : response.status, headers: {"Content-Type":"application/json","Cache-Control":"no-store",...(response.headers?.get?.("retry-after") ? {"Retry-After":response.headers.get("retry-after")} : {})}, body: JSON.stringify({ error: "etsy_error", status: response.status, body: "Etsy's answer could not be read" }) };
    // Etsy v3 returns the receipt directly; the station and sandbox share an
    // explicit receipt/transactions envelope. Preserve root fields for older callers.
    const receipt = payload?.receipt || (payload?.receipt_id ? payload : null);
    const normalized = response.ok && receipt
      ? { ...payload, receipt, transactions: payload.transactions || receipt.transactions || [] }
      : payload;
    return {
      statusCode: response.status,
      headers: {"Content-Type":"application/json","Cache-Control":"no-store",...(response.headers?.get?.("retry-after") ? {"Retry-After":response.headers.get("retry-after")} : {})},
      body: JSON.stringify(normalized)
    };

  } catch (err) {
    return { statusCode: 500, body: JSON.stringify({ error: err.message }) };
  }
};