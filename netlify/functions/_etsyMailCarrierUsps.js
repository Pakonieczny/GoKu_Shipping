/* netlify/functions/_etsyMailCarrierUsps.js
 *
 * USPS tracking driver — powered by 17TRACK API v2.2.
 *
 * Why 17TRACK (not Apify):
 *   - substantial_sponge/usps-tracking is flagged "Under maintenance" on Apify
 *   - alizarin_refrigerator-owner/multi-carrier-package-tracking wraps the
 *     dead USPS Web Tools API (shut down January 2026) and asks for
 *     credentials we can't obtain
 *   - 17TRACK has a real free tier (100 tracks/mo), proper uptime SLA,
 *     handles all USPS label formats including 34-digit IMpb
 *
 * 17TRACK is a 2-step async API:
 *   1. POST /track/v2.2/register  — subscribe a tracking number
 *   2. POST /track/v2.2/gettrackinfo  — retrieve current scan events
 *
 * First-time registrations populate within seconds-to-minutes. We poll
 * gettrackinfo up to 3 times over ~30 sec. The background function gives
 * us 15 min of runway, so this is comfortable.
 *
 * Once a tracking number is registered, subsequent gettrackinfo calls
 * return data immediately (subject to 17TRACK's background refresh cycle).
 * This means our cache layer absorbs most of the latency — cold fetches
 * are ~5-30 sec, warm fetches (already-registered numbers) are ~200ms.
 *
 * API reference:
 *   https://asset.17track.net/api/document/v2.2_en/index.html
 *
 * Env vars required:
 *   SEVENTEEN_TRACK_API_KEY  Your API access key from the 17TRACK console
 *
 * Env vars optional:
 *   SEVENTEEN_TRACK_BASE_URL   Override base URL (default: https://api.17track.net/track/v2.2)
 *   SEVENTEEN_TRACK_POLLS      Max gettrackinfo polls (default: 3)
 *   SEVENTEEN_TRACK_POLL_DELAY_MS  Delay between polls in ms (default: 10000)
 */

const fetch = require("node-fetch");

const DEFAULT_BASE_URL = "https://api.17track.net/track/v2.2";
const BASE_URL    = (process.env.SEVENTEEN_TRACK_BASE_URL || DEFAULT_BASE_URL).replace(/\/+$/, "");
const API_KEY     = process.env.SEVENTEEN_TRACK_API_KEY || "";
const MAX_POLLS   = Number(process.env.SEVENTEEN_TRACK_POLLS) || 3;
const POLL_DELAY  = Number(process.env.SEVENTEEN_TRACK_POLL_DELAY_MS) || 10000;

// USPS carrier code in 17TRACK's catalog. 21051 is their primary USPS code.
// We pass auto_detection:true as a safety net so mis-routed numbers still
// resolve (e.g. if a merchant accidentally paired a UPS label with USPS).
const USPS_CARRIER_CODE = 21051;

function headers() {
  return {
    "Content-Type": "application/json",
    "17token"     : API_KEY
  };
}

/** Low-level 17TRACK API call. Returns parsed JSON or throws. */
async function call17Track(path, payload) {
  if (!API_KEY) {
    throw Object.assign(
      new Error("SEVENTEEN_TRACK_API_KEY env var is required for USPS tracking"),
      { code: "SEVENTEEN_NO_TOKEN" }
    );
  }

  const url = `${BASE_URL}${path}`;
  console.log(`[17track] POST ${path} payload=${JSON.stringify(payload).slice(0, 200)}`);

  let res;
  try {
    res = await fetch(url, {
      method : "POST",
      headers: headers(),
      body   : JSON.stringify(payload),
      timeout: 15000
    });
  } catch (e) {
    throw Object.assign(new Error(`17TRACK network error: ${e.message}`),
      { code: "SEVENTEEN_NETWORK" });
  }

  const text = await res.text();

  if (!res.ok) {
    throw Object.assign(
      new Error(`17TRACK HTTP ${res.status}: ${text.slice(0, 400)}`),
      { code: "SEVENTEEN_HTTP_ERROR", status: res.status }
    );
  }

  let json;
  try { json = JSON.parse(text); }
  catch {
    throw Object.assign(
      new Error(`17TRACK returned non-JSON: ${text.slice(0, 400)}`),
      { code: "SEVENTEEN_BAD_JSON" }
    );
  }

  // 17TRACK wraps everything in { code, data }. code:0 is OK; anything else
  // may still contain per-item errors inside data.errors or data.rejected.
  if (json.code !== 0 && json.code !== undefined) {
    console.warn(`[17track] API returned code=${json.code}`, json);
  }

  return json;
}

/** Register a tracking number with 17TRACK (subscribes it for monitoring). */
async function registerTrackingNumber(trackingCode) {
  const payload = [{
    number       : trackingCode,
    carrier      : USPS_CARRIER_CODE,
    auto_detection: true
  }];

  const res = await call17Track("/register", payload);

  const rejected = res?.data?.rejected || [];
  const accepted = res?.data?.accepted || [];

  // Already-registered is fine — 17TRACK returns a rejection like
  //   "The registration information of ... already exists"
  // We treat that as success (the number is subscribed, we can fetch info).
  for (const r of rejected) {
    const msg = r.error?.message || "";
    if (/already exists|already registered/i.test(msg)) {
      console.log(`[17track] ${trackingCode} already registered, proceeding`);
      return { accepted: true, alreadyRegistered: true };
    }
  }

  if (accepted.length === 0 && rejected.length > 0) {
    const r = rejected[0];
    throw Object.assign(
      new Error(`17TRACK rejected registration: ${r.error?.message || "unknown"}`),
      { code: r.error?.code === -18010012 ? "INVALID_TRACKING" : "SEVENTEEN_REJECTED" }
    );
  }

  return { accepted: accepted.length > 0, alreadyRegistered: false };
}

/** Fetch tracking info for a registered number. */
async function getTrackInfo(trackingCode) {
  const payload = [{ number: trackingCode, carrier: USPS_CARRIER_CODE }];
  const res = await call17Track("/gettrackinfo", payload);

  const accepted = res?.data?.accepted || [];
  const rejected = res?.data?.rejected || [];

  if (rejected.length > 0 && accepted.length === 0) {
    const r = rejected[0];
    const msg = r.error?.message || "rejected";
    // If the number hasn't finished registering yet, 17TRACK returns a
    // "not registered" error. Caller will retry.
    if (/not register|not registered/i.test(msg)) {
      return null;   // caller should retry
    }
    throw Object.assign(
      new Error(`17TRACK gettrackinfo rejected: ${msg}`),
      { code: "SEVENTEEN_REJECTED" }
    );
  }

  return accepted[0] || null;
}

/**
 * Register-then-poll. Returns the first non-empty track_info or null after
 * MAX_POLLS attempts.
 */
async function registerAndFetch(trackingCode) {
  await registerTrackingNumber(trackingCode);

  let attempt = 0;
  let lastResult = null;
  while (attempt < MAX_POLLS) {
    attempt++;
    console.log(`[17track] gettrackinfo attempt ${attempt}/${MAX_POLLS}`);
    const item = await getTrackInfo(trackingCode);
    if (item && hasUsableData(item)) {
      console.log(`[17track] got usable data on attempt ${attempt}`);
      return item;
    }
    lastResult = item;
    if (attempt < MAX_POLLS) {
      await new Promise(r => setTimeout(r, POLL_DELAY));
    }
  }

  console.log(`[17track] all ${MAX_POLLS} attempts returned no usable data`);
  return lastResult;
}

/** Does this response contain scan events we can render? */
function hasUsableData(item) {
  if (!item) return false;
  // 17TRACK v2.2 puts events under track_info.tracking.providers[n].events
  // but older schemas use track.z{0,1,2}.[n].z or similar. Check both.
  const events = extractEvents(item);
  if (events.length > 0) return true;
  // Also accept entries that at least have a latest_event / status even if
  // the full event list isn't yet present
  const ti = item.track_info || item.track || {};
  if (ti.latest_event_info || ti.latest_event?.description) return true;
  if (ti.latest_status || ti.latest_status?.status) return true;
  return false;
}

/** Extract scan events from the 17TRACK response (defensive across versions). */
function extractEvents(item) {
  const events = [];

  // v2.2 schema: data.accepted[0].track_info.tracking.providers[0].events[]
  const providers = item?.track_info?.tracking?.providers || [];
  for (const p of providers) {
    for (const e of (p.events || [])) {
      events.push({
        at       : e.time_utc || e.time_iso || e.time_raw?.date || null,
        title    : e.description || e.stage || "Scan",
        subtitle : null,
        location : formatLocation(e.address) || e.location || null,
        status   : e.sub_status || e.stage || null
      });
    }
  }

  // Older v2 fallback: data.accepted[0].track.z1[] (events) / z0 origin / z2 destination
  if (events.length === 0 && item.track) {
    const arrays = [item.track.z0, item.track.z1, item.track.z2]
      .filter(Array.isArray).flat();
    for (const e of arrays) {
      events.push({
        at       : e.a || e.time_utc || null,   // event timestamp
        title    : e.z || e.c || e.description || "Scan",
        subtitle : null,
        location : e.b || e.location || null,
        status   : e.d || null
      });
    }
  }

  // Sort newest first
  events.sort((a, b) => {
    const ta = a.at ? new Date(a.at).getTime() : 0;
    const tb = b.at ? new Date(b.at).getTime() : 0;
    return tb - ta;
  });

  return events;
}

/** Format 17TRACK address object into a location string. */
function formatLocation(addr) {
  if (!addr || typeof addr !== "object") return null;
  const parts = [addr.city, addr.state, addr.postal_code, addr.country]
    .filter(Boolean);
  return parts.length > 0 ? parts.join(", ") : null;
}

/** Map 17TRACK's main status codes to our normalized status keys. */
function normalizeStatusKey(latestStatus) {
  // 17TRACK v2.2 returns latest_status.status: "Delivered", "InTransit",
  // "Exception", "Expired", "Undelivered", "Alert", "NotFound",
  // "Pickup", "InfoReceived"
  const raw = String(latestStatus || "").toLowerCase();
  if (raw.includes("deliver") && !raw.includes("un")) return "delivered";
  if (raw.includes("undeliver")) return "exception";
  if (raw.includes("out for delivery") || raw.includes("outfordelivery")) return "out_for_delivery";
  if (raw.includes("transit")) return "in_transit";
  if (raw.includes("pickup")) return "in_transit";
  if (raw.includes("inforeceived") || raw.includes("info received") || raw.includes("pre-shipment")) return "pre_shipment";
  if (raw.includes("exception") || raw.includes("alert")) return "exception";
  if (raw.includes("expired")) return "exception";
  if (raw.includes("return")) return "returned";
  return "in_transit";
}

/** Shape the 17TRACK response into our normalized tracking result. */
function shapeResult(item, requestedCode) {
  const ti = item?.track_info || {};
  const latest = ti.latest_status || ti.latestStatus || {};
  const milestone = ti.milestone || [];
  const timeEstimated = ti.time_metrics?.estimated_delivery_date || null;
  const shipmentInfo = ti.shipping_info?.shipper_address || {};
  const destInfo = ti.shipping_info?.recipient_address || {};
  const providers = ti.tracking?.providers || [];
  const primaryProvider = providers[0] || {};
  const carrierName = primaryProvider.provider?.name || "USPS";

  const events = extractEvents(item);

  const statusText = latest.sub_status_descr || latest.status || item.track?.e || "In Transit";
  const statusKey  = normalizeStatusKey(latest.status || item.track?.e);

  // Try to find a delivery event timestamp
  const deliveredEvent = events.find(e => /deliver/i.test(e.title));
  const resolvedAt = statusKey === "delivered" && deliveredEvent ? deliveredEvent.at : null;

  return {
    carrier          : "usps",
    carrierDisplay   : carrierName.toUpperCase().includes("USPS") ? "USPS" : carrierName,
    trackingCode     : requestedCode,
    status           : statusText,
    statusKey,
    estimatedDelivery: timeEstimated?.from || timeEstimated?.to || timeEstimated || null,
    destination      : formatLocation(destInfo) || null,
    origin           : formatLocation(shipmentInfo) || null,
    shipDate         : events.length > 0 ? events[events.length - 1].at : null,
    resolvedAt,
    events,
    raw: item   // keep full response for debugging
  };
}

/**
 * Look up USPS tracking for a label.
 *
 * @param {string} trackingCode
 * @returns {Promise<object>}  Normalized tracking result
 */
async function lookup(trackingCode) {
  const code = String(trackingCode || "").trim();
  if (!code) {
    throw Object.assign(new Error("Missing tracking code"), { code: "INVALID_INPUT" });
  }

  console.log(`[17track] looking up ${code}`);

  const item = await registerAndFetch(code);

  if (!item || !hasUsableData(item)) {
    throw Object.assign(
      new Error(`No tracking events found for ${code} after ${MAX_POLLS} polls`),
      { code: "NOT_FOUND" }
    );
  }

  const shaped = shapeResult(item, code);
  console.log(`[17track] ${code} → ${shaped.carrierDisplay} ${shaped.status} (${shaped.events.length} events)`);
  return shaped;
}

module.exports = { lookup };
