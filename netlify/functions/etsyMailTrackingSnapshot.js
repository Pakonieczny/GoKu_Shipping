/* netlify/functions/etsyMailTrackingSnapshot.js
 *
 * HTTP endpoint for the tracking-image generator.
 * Thin wrapper around _etsyMailTracking.js's snapshot() function.
 *
 *   POST /.netlify/functions/etsyMailTrackingSnapshot
 *   Body: { trackingCode: string, carrierHint?: string, forceRefresh?: boolean }
 *
 *   GET /.netlify/functions/etsyMailTrackingSnapshot?code=<tracking>
 *     (Equivalent; supports manual testing via URL.)
 *
 * Response (200): see snapshot() return shape in _etsyMailTracking.js
 *
 * Error responses map snapshot() error codes to HTTP statuses:
 *   UNKNOWN_CARRIER, INVALID_INPUT                    → 400
 *   NOT_FOUND, APIFY_NO_RESULTS                       → 404
 *   APIFY_ERROR, APIFY_NETWORK                        → 502
 *   RENDER_FAILED, UPLOAD_FAILED                      → 500
 *   (anything else)                                   → 500
 */

const { snapshot } = require("./_etsyMailTracking");

const CORS = {
  "Access-Control-Allow-Origin" : "*",
  "Access-Control-Allow-Headers": "Content-Type, Authorization, X-EtsyMail-Secret",
  "Access-Control-Allow-Methods": "POST, GET, OPTIONS"
};

const json = (statusCode, body) => ({
  statusCode,
  headers: { ...CORS, "Content-Type": "application/json" },
  body   : JSON.stringify(body)
});

const STATUS_FOR_CODE = {
  INVALID_INPUT   : 400,
  UNKNOWN_CARRIER : 400,
  NOT_FOUND       : 404,
  APIFY_NO_RESULTS: 404,
  APIFY_BAD_JSON  : 502,
  APIFY_ERROR     : 502,
  APIFY_NETWORK   : 502,
  RENDER_FAILED   : 500,
  UPLOAD_FAILED   : 500
};

exports.handler = async (event) => {
  if (event.httpMethod === "OPTIONS") {
    return { statusCode: 200, headers: CORS, body: "ok" };
  }

  let trackingCode, carrierHint, forceRefresh;

  if (event.httpMethod === "GET") {
    const q = event.queryStringParameters || {};
    trackingCode = String(q.trackingCode || q.code || "").trim();
    carrierHint  = String(q.carrier || "").trim().toLowerCase();
    forceRefresh = q.refresh === "1" || q.refresh === "true";
  } else if (event.httpMethod === "POST") {
    let body;
    try { body = JSON.parse(event.body || "{}"); }
    catch { return json(400, { error: "Invalid JSON body" }); }
    trackingCode = String(body.trackingCode || body.code || "").trim();
    carrierHint  = String(body.carrierHint || body.carrier || "").trim().toLowerCase();
    forceRefresh = Boolean(body.forceRefresh);
  } else {
    return json(405, { error: "Method not allowed" });
  }

  if (!trackingCode) {
    return json(400, { error: "Missing trackingCode" });
  }

  try {
    const result = await snapshot(trackingCode, { carrierHint, forceRefresh });
    return json(200, result);
  } catch (e) {
    const status = STATUS_FOR_CODE[e.code] || 500;
    console.error(`[trackingSnapshot] ${e.code || "ERROR"}: ${e.message}`);
    return json(status, {
      error       : e.message || "Tracking snapshot failed",
      code        : e.code || "INTERNAL",
      trackingCode
    });
  }
};
