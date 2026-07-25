/*  netlify/functions/etsyApiUsage.js
 *
 *  GET /.netlify/functions/etsyApiUsage?app=<appId>
 *
 *  Returns the verified Etsy API usage snapshot rendered by the App / Key /
 *  QPS widget in the Listing Generator and the Pricing Console.
 *
 *  This endpoint is FREE — it reads Firestore counters that real Etsy calls
 *  already populated. It never calls Etsy. Poll it as often as you like;
 *  the consoles poll every 15s.
 *
 *  Response contract (consumed verbatim by both consoles):
 *    {
 *      ok                  : true,
 *      verified            : true,      // false ⇒ UI must show "Unavailable"
 *      count               : 412,       // this app's calls today
 *      count_since         : 1750...,   // ms — when tracking for today began
 *      budget              : 2500,      // 50% of Etsy's per-key daily quota
 *      max_qps             : 2.1,
 *      qps_cap             : 2.5,
 *      etsy_limit_per_day  : 5000,      // from Etsy's own headers, or null
 *      etsy_remaining_today: 3120,      // from Etsy's own headers, or null
 *      etsy_reported_at    : 1750...,   // ms — header freshness, or null
 *      server_time         : 1750...
 *    }
 *
 *  `verified` is the contract's whole point: the consoles refuse to display
 *  anything without it, so a degraded read can never be mistaken for real
 *  numbers. On failure we return verified:false with the reason rather than
 *  a zeroed-out payload that would look like "no usage today".
 *
 *  The `app` query param namespaces the count. Defaults are chosen so an
 *  un-parameterised call from either console still lands somewhere sane:
 *    ?app=pricing-console     → the Etsy Pricing Console
 *    ?app=listing-generator   → the Listing Generator
 *
 *  A caller that passes no `app` is bucketed as "unattributed" rather than
 *  defaulting into a named app — an un-parameterised client must never
 *  silently inflate another console's budget.
 */

"use strict";

const usage = require("./_etsyApiUsage");

const CORS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "Content-Type, Accept",
  "Access-Control-Allow-Methods": "GET, OPTIONS",
};

const json = (statusCode, body) => ({
  statusCode,
  headers: {
    "Content-Type": "application/json",
    // Usage changes every few seconds — never let a CDN or browser serve a
    // stale reading as if it were current.
    "Cache-Control": "no-store, max-age=0",
    ...CORS,
  },
  body: JSON.stringify(body),
});

exports.handler = async (event) => {
  if (event.httpMethod === "OPTIONS") return { statusCode: 204, headers: CORS, body: "" };
  if (event.httpMethod !== "GET") {
    return json(405, { ok: false, verified: false, error: "Method not allowed" });
  }

  try {
    const q = event.queryStringParameters || {};
    const appId = String(q.app || "unattributed").trim() || "unattributed";

    const snapshot = await usage.readUsage(appId);

    // readUsage() already returns verified:false + error on a store failure;
    // pass it straight through so the console can show the real reason.
    return json(snapshot.ok === false ? 503 : 200, snapshot);
  } catch (e) {
    return json(503, {
      ok: false,
      verified: false,
      error: "API statistics unavailable: " + (e && e.message ? e.message : e),
      server_time: Date.now(),
    });
  }
};
