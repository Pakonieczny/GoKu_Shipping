/* netlify/functions/_etsyMailCarriers/usps.js
 *
 * USPS tracking driver — fetches tracking events from tools.usps.com via
 * Apify's synchronous run-sync-get-dataset-items API.
 *
 * Why Apify?
 *   USPS's tracking page is JavaScript-heavy and behind bot-detection. Plain
 *   server-side fetch() gets an empty body. A real browser is needed, but
 *   bundling Chromium inside Netlify Functions exceeds the 50 MB bundle
 *   limit and has ongoing maintenance burden. Apify handles Chromium +
 *   proxy rotation + CAPTCHA bypass for us.
 *
 * Cost:
 *   ~$0.002-0.01 per lookup with the free $5/mo credit. For the CustomBrites
 *   volume (~22/day, ~660/mo) we stay inside the free tier indefinitely.
 *
 * Which Apify actor:
 *   substantial_sponge/usps-tracking is maintained and reliable.
 *   Its input shape: { trackingNumbers: ["4206..."] }
 *   Its output shape (one dataset item per tracking number):
 *     {
 *       trackingNumber: "4206...",
 *       status: "In Transit",
 *       statusCategory: "In Transit",
 *       expectedDelivery: "2026-04-27T21:00:00",
 *       lastUpdate: "...",
 *       events: [
 *         {
 *           status: "Arrived at USPS Regional Origin Facility",
 *           date: "April 23, 2026",
 *           time: "8:02 pm",
 *           location: "NORTHWEST ROCHESTER NY DISTRIBUTION CENTER"
 *         },
 *         ...
 *       ]
 *     }
 *
 *   If that actor is unavailable or breaks, swap ACTOR_ID below for another
 *   USPS-tracking actor (there are several; see apify.com/store).
 *
 * Env vars required:
 *   APIFY_API_TOKEN   The API token from apify.com → Integrations → API tokens
 *
 * Optional env vars:
 *   APIFY_USPS_ACTOR_ID   Override the default actor (default: substantial_sponge~usps-tracking)
 *   APIFY_TIMEOUT_SEC     Max seconds to wait for Apify run (default: 60)
 */

const fetch = require("node-fetch");

const DEFAULT_ACTOR  = "substantial_sponge~usps-tracking";
const APIFY_BASE     = "https://api.apify.com/v2";
const DEFAULT_TIMEOUT = 60;

const ACTOR_ID       = process.env.APIFY_USPS_ACTOR_ID || DEFAULT_ACTOR;
const APIFY_TOKEN    = process.env.APIFY_API_TOKEN || "";
const RUN_TIMEOUT_SEC = Number(process.env.APIFY_TIMEOUT_SEC) || DEFAULT_TIMEOUT;

/**
 * Call the Apify actor synchronously and wait for results.
 * Uses run-sync-get-dataset-items which blocks until the actor finishes
 * and returns the dataset directly (simpler than run + poll + fetch dataset).
 */
async function callApifyActor(trackingCode) {
  if (!APIFY_TOKEN) {
    throw new Error("APIFY_API_TOKEN env var is required for USPS tracking lookup");
  }

  const url = `${APIFY_BASE}/acts/${ACTOR_ID}/run-sync-get-dataset-items` +
              `?token=${encodeURIComponent(APIFY_TOKEN)}` +
              `&timeout=${RUN_TIMEOUT_SEC}` +
              `&format=json`;

  const body = { trackingNumbers: [trackingCode] };

  let res;
  try {
    res = await fetch(url, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
      // Netlify sync functions have a 10 s timeout. Cap Apify wait below that
      // so we can return a helpful error if it times out.
      // (Apify's own timeout is RUN_TIMEOUT_SEC; this is the HTTP client timeout.)
      timeout: 9000
    });
  } catch (e) {
    const err = new Error(`Apify call failed: ${e.message}`);
    err.code = "APIFY_NETWORK";
    throw err;
  }

  const rawText = await res.text();

  if (!res.ok) {
    const err = new Error(`Apify returned ${res.status}: ${rawText.slice(0, 500)}`);
    err.code = "APIFY_ERROR";
    err.status = res.status;
    throw err;
  }

  let items;
  try {
    items = JSON.parse(rawText);
  } catch (e) {
    const err = new Error(`Apify response was not JSON: ${rawText.slice(0, 500)}`);
    err.code = "APIFY_BAD_JSON";
    throw err;
  }

  if (!Array.isArray(items) || items.length === 0) {
    const err = new Error("Apify returned no results");
    err.code = "APIFY_NO_RESULTS";
    throw err;
  }

  return items[0];
}

/** Normalize a USPS event date/time pair into an ISO timestamp. */
function parseEventDate(dateStr, timeStr) {
  if (!dateStr) return null;
  // "April 23, 2026" + "8:02 pm"  →  "April 23, 2026 8:02 pm"
  const combined = `${dateStr}${timeStr ? " " + timeStr : ""}`;
  const parsed = new Date(combined);
  if (isNaN(parsed.getTime())) return null;
  return parsed.toISOString();
}

/** Convert USPS status text into a normalized machine-friendly key. */
function normalizeStatusKey(status) {
  const s = String(status || "").toLowerCase();
  if (s.includes("delivered"))        return "delivered";
  if (s.includes("out for delivery")) return "out_for_delivery";
  if (s.includes("in transit"))       return "in_transit";
  if (s.includes("pre-shipment") || s.includes("label"))  return "pre_shipment";
  if (s.includes("return"))           return "returned";
  if (s.includes("exception") || s.includes("alert"))     return "exception";
  return "in_transit";   // safe default
}

/**
 * Look up USPS tracking for a label.
 *
 * @param {string} trackingCode  The USPS tracking number
 * @returns {Promise<object>}    Normalized tracking result
 */
async function lookup(trackingCode) {
  const raw = await callApifyActor(trackingCode);

  // Apify actor response may use slightly different field names across versions.
  // Handle the most likely variations defensively.
  const rawEvents = raw.events || raw.trackingEvents || raw.history || [];
  const events = (Array.isArray(rawEvents) ? rawEvents : [])
    .map((e) => ({
      at       : parseEventDate(e.date, e.time) || e.dateTime || e.timestamp || null,
      title    : e.status || e.event || e.description || e.title || "Scan",
      subtitle : e.subtitle || null,
      location : e.location || e.place || null,
      status   : e.statusCode || e.code || null
    }))
    .filter((e) => e.title);   // drop empty rows

  // Sort events newest-first for display (USPS site shows them this way)
  events.sort((a, b) => {
    const ta = a.at ? new Date(a.at).getTime() : 0;
    const tb = b.at ? new Date(b.at).getTime() : 0;
    return tb - ta;
  });

  const status        = raw.status || raw.statusCategory || "In Transit";
  const statusKey     = normalizeStatusKey(status);
  const estDelivery   = raw.expectedDelivery || raw.estimatedDelivery || raw.deliveryDate || null;
  const resolvedAt    = statusKey === "delivered"
    ? (events.find(e => /delivered/i.test(e.title))?.at || null)
    : null;

  return {
    carrier          : "usps",
    carrierDisplay   : "USPS",
    trackingCode,
    status,
    statusKey,
    estimatedDelivery: estDelivery,
    destination      : raw.destination || null,
    origin           : raw.origin || null,
    shipDate         : raw.shipDate || events[events.length - 1]?.at || null,
    resolvedAt,
    events,
    raw
  };
}

module.exports = { lookup };
