/* netlify/functions/_etsyMailCarrierUsps.js
 *
 * USPS tracking driver — uses the Howlers' Multi-Carrier Package Tracking
 * actor on Apify. This actor auto-detects the carrier, handles USPS + UPS
 * + FedEx in one API, and returns a normalized response shape.
 *
 * Why this actor (vs. the previous substantial_sponge one):
 *   - substantial_sponge/usps-tracking is officially flagged
 *     "Under maintenance" on Apify and fails on real tracking codes
 *   - This one is 5.0-rated, actively maintained, 28 users
 *   - Supports multi-carrier so we can retire separate UPS/FedEx drivers
 *     if we ever add them
 *   - Has demoMode for testing without burning credits
 *
 * Actor details:
 *   Full ID: alizarin_refrigerator-owner/multi-carrier-package-tracking-usps-ups-fedex
 *   Short ID: nrtmiXkuzd6UGsfQp
 *   Pricing: $0.01 per result
 *   Free-tier fit: ~500 lookups/month fits within $5 Apify credit
 *
 * Actor output shape (from the published README):
 *   {
 *     trackingNumber : "1Z999AA10123456784",
 *     carrier        : "UPS" | "USPS" | "FedEx",
 *     status         : "Delivered" | "In Transit" | ...,
 *     statusDetail   : "DELIVERED",
 *     estimatedDelivery: ISO string | null,
 *     deliveredDate  : ISO string | null,
 *     lastUpdate     : ISO string,
 *     location       : "Austin, TX 78701 US",
 *     events: [
 *       {
 *         timestamp: ISO string,
 *         status   : "Delivered",
 *         location : "Austin, TX 78701 US",
 *         details  : "Left at front door"
 *       },
 *       ...
 *     ]
 *   }
 *
 * Tracking-code quirks we handle:
 *   USPS's modern labels can be 12/15/20/22/26/30/34 digits. The actor's
 *   carrier-detection docs mention "starts with 94/92/93 + 20 digits" —
 *   it may not recognize 34-digit IMpb codes out of the box. When we see
 *   a label >22 digits, we try the full code first; if the actor returns
 *   an "unknown carrier" or "not found" error, we retry with the last
 *   22 digits (which is typically the USPS tracking-number subset inside
 *   the larger IMpb).
 *
 * Env vars required:
 *   APIFY_API_TOKEN   The API token from apify.com → Integrations → API tokens
 *
 * Optional env vars:
 *   APIFY_USPS_ACTOR_ID   Override the default actor (default: alizarin_refrigerator-owner~multi-carrier-package-tracking-usps-ups-fedex)
 *   APIFY_TIMEOUT_SEC     Max Apify actor run seconds (default: 180)
 *   APIFY_HTTP_TIMEOUT_MS HTTP client timeout in ms (default: 600000 = 10 min)
 *   APIFY_DEMO_MODE       If "1" or "true", call the actor with demoMode:true
 *                         (returns sample data without consuming credits — useful
 *                         for initial smoke testing before real invocations)
 */

const fetch = require("node-fetch");

const DEFAULT_ACTOR   = "alizarin_refrigerator-owner~multi-carrier-package-tracking-usps-ups-fedex";
const APIFY_BASE      = "https://api.apify.com/v2";
const DEFAULT_TIMEOUT = 180;
const DEFAULT_HTTP_TIMEOUT_MS = 10 * 60 * 1000;   // 10 min; background func has 15 min budget

const ACTOR_ID        = process.env.APIFY_USPS_ACTOR_ID || DEFAULT_ACTOR;
const APIFY_TOKEN     = process.env.APIFY_API_TOKEN || "";
const RUN_TIMEOUT_SEC = Number(process.env.APIFY_TIMEOUT_SEC) || DEFAULT_TIMEOUT;
const HTTP_TIMEOUT_MS = Number(process.env.APIFY_HTTP_TIMEOUT_MS) || DEFAULT_HTTP_TIMEOUT_MS;
const DEMO_MODE       = /^(1|true|yes)$/i.test(process.env.APIFY_DEMO_MODE || "");

/**
 * Call the Apify actor with a specific tracking code.
 * Returns the first dataset item, or throws with error code.
 */
async function callApifyActor(trackingCode) {
  if (!APIFY_TOKEN) {
    throw Object.assign(
      new Error("APIFY_API_TOKEN env var is required for USPS tracking"),
      { code: "APIFY_NO_TOKEN" }
    );
  }

  const url = `${APIFY_BASE}/acts/${ACTOR_ID}/run-sync-get-dataset-items` +
              `?token=${encodeURIComponent(APIFY_TOKEN)}` +
              `&timeout=${RUN_TIMEOUT_SEC}` +
              `&format=json`;

  const body = {
    trackingNumbers: [trackingCode],
    demoMode       : DEMO_MODE
  };

  console.log(`[usps] calling Apify actor with code=${trackingCode} demoMode=${DEMO_MODE}`);

  let res;
  try {
    res = await fetch(url, {
      method : "POST",
      headers: { "Content-Type": "application/json" },
      body   : JSON.stringify(body),
      timeout: HTTP_TIMEOUT_MS
    });
  } catch (e) {
    throw Object.assign(new Error(`Apify network error: ${e.message}`),
      { code: "APIFY_NETWORK" });
  }

  const rawText = await res.text();

  if (!res.ok) {
    throw Object.assign(
      new Error(`Apify returned ${res.status}: ${rawText.slice(0, 500)}`),
      { code: "APIFY_ERROR", status: res.status }
    );
  }

  let items;
  try { items = JSON.parse(rawText); }
  catch {
    throw Object.assign(
      new Error(`Apify response was not JSON: ${rawText.slice(0, 500)}`),
      { code: "APIFY_BAD_JSON" }
    );
  }

  if (!Array.isArray(items) || items.length === 0) {
    throw Object.assign(new Error("Apify returned empty dataset"),
      { code: "APIFY_NO_RESULTS" });
  }

  console.log(`[usps] Apify returned ${items.length} item(s), first item keys:`,
    Object.keys(items[0] || {}).join(","));

  return items[0];
}

/** Normalize the actor's status text into a machine-friendly key. */
function normalizeStatusKey(status, statusDetail) {
  const s = String(status || statusDetail || "").toLowerCase();
  if (s.includes("delivered"))        return "delivered";
  if (s.includes("out for delivery")) return "out_for_delivery";
  if (s.includes("in transit") || s.includes("in_transit")) return "in_transit";
  if (s.includes("departed") || s.includes("arrived") || s.includes("origin")) return "in_transit";
  if (s.includes("pre-shipment") || s.includes("label")) return "pre_shipment";
  if (s.includes("return"))           return "returned";
  if (s.includes("exception") || s.includes("alert") || s.includes("fail")) return "exception";
  return "in_transit";
}

/** Map the Multi-Carrier actor's response to our normalized tracking shape. */
function shapeActorResult(raw, requestedCode) {
  const rawEvents = Array.isArray(raw.events) ? raw.events : [];

  const events = rawEvents
    .map(e => ({
      at       : e.timestamp || e.date || e.dateTime || null,
      title    : e.status || e.event || e.description || "Scan",
      subtitle : e.details || e.detail || null,
      location : e.location || e.place || null,
      status   : e.statusCode || e.code || null
    }))
    .filter(e => e.title);

  // Sort newest first
  events.sort((a, b) => {
    const ta = a.at ? new Date(a.at).getTime() : 0;
    const tb = b.at ? new Date(b.at).getTime() : 0;
    return tb - ta;
  });

  const carrierUpper = String(raw.carrier || "USPS").toUpperCase();
  const statusKey = normalizeStatusKey(raw.status, raw.statusDetail);
  const resolvedAt = raw.deliveredDate ||
                     (statusKey === "delivered"
                       ? events.find(e => /delivered/i.test(e.title))?.at
                       : null);

  return {
    carrier          : "usps",   // feature slot name — UI shows carrierDisplay
    carrierDisplay   : carrierUpper,
    trackingCode     : requestedCode,
    status           : raw.status || "In Transit",
    statusKey,
    estimatedDelivery: raw.estimatedDelivery || null,
    destination      : raw.location || null,
    origin           : raw.origin || null,
    shipDate         : raw.shipDate || (events[events.length - 1]?.at || null),
    resolvedAt,
    events,
    raw
  };
}

/**
 * Look up USPS tracking for a label.
 *
 * Strategy:
 *   1. Try the full tracking code first (typical USPS 22/26 digit format)
 *   2. If that returns empty/not-found AND the code is 23+ digits,
 *      retry with the last 22 digits (IMpb tracking-number subset)
 *
 * @param {string} trackingCode
 * @returns {Promise<object>}  Normalized tracking result
 */
async function lookup(trackingCode) {
  const code = String(trackingCode || "").trim();
  if (!code) {
    throw Object.assign(new Error("Missing tracking code"), { code: "INVALID_INPUT" });
  }

  // Build ordered attempt list
  const attempts = [code];
  if (/^\d{23,}$/.test(code) && code.slice(-22) !== code) {
    attempts.push(code.slice(-22));
  }

  let lastError;
  for (const attempt of attempts) {
    try {
      const raw = await callApifyActor(attempt);

      // Actor may return { error: "..." } or { notFound: true } for bad codes
      if (raw.error || raw.notFound) {
        console.log(`[usps] Actor reported not-found for ${attempt}: ${raw.error || "notFound"}`);
        lastError = Object.assign(
          new Error(raw.error || `Tracking not found: ${attempt}`),
          { code: "NOT_FOUND" }
        );
        continue;
      }

      // Ensure the result has events or a recognizable status
      if (!raw.events && !raw.status && !raw.carrier) {
        console.log(`[usps] Actor returned empty-ish result for ${attempt}`);
        lastError = Object.assign(
          new Error(`Actor returned empty result for ${attempt}`),
          { code: "APIFY_NO_RESULTS" }
        );
        continue;
      }

      console.log(`[usps] success for code ${attempt} — carrier=${raw.carrier} status=${raw.status}`);
      return shapeActorResult(raw, code);    // always echo caller's original code
    } catch (e) {
      lastError = e;
      // For infrastructure errors, don't bother retrying with a different
      // tracking-code truncation — the issue isn't the code.
      if (e.code === "APIFY_NETWORK" || e.code === "APIFY_NO_TOKEN" ||
          e.code === "APIFY_BAD_JSON") {
        throw e;
      }
      // For APIFY_ERROR (400/5xx from actor) we DO retry with truncation,
      // since the actor might have rejected the code format.
      console.log(`[usps] attempt failed (${e.code}: ${e.message}), trying next`);
    }
  }

  throw lastError || Object.assign(
    new Error(`All tracking lookup attempts failed for ${code}`),
    { code: "NOT_FOUND" }
  );
}

module.exports = { lookup };
