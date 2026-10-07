/* netlify/functions/etsyMailTrackingImage.js
 *
 * Image proxy for tracking screenshots.
 *
 * Serves the tracking PNG from Firebase Storage through our own origin
 * so COEP: require-corp doesn't block the <img> tag. No caching on this
 * endpoint — Firebase Storage is the single source of truth, and we want
 * the browser to always reflect the latest version.
 *
 * Usage:
 *   GET /.netlify/functions/etsyMailTrackingImage?trackingCode=<code>
 */

const admin  = require("./firebaseAdmin");
const fetch  = require("node-fetch");

const db = admin.firestore();

const CORS = {
  "Access-Control-Allow-Origin" : "*",
  "Access-Control-Allow-Methods": "GET, OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type"
};

// Always-fresh caching strategy. Firebase Storage is authoritative; any
// time the tracking snapshot refreshes, the PNG is overwritten there, and
// the browser should pull the new one immediately. No browser cache, no
// CDN cache. Trade a tiny bit of latency for zero stale-image headaches.
// A picture a browser holds is asked about every time (If-None-Match against the doc's version, above): same freshness as before,
// but an unchanged picture costs one small read and no Storage download.
const REVALIDATE_HEADERS = { "Cache-Control": "private, no-cache" };
const NO_CACHE_HEADERS = {
  "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
  "Pragma"       : "no-cache",
  "Expires"      : "0"
};

exports.handler = async (event) => {
  if (event.httpMethod === "OPTIONS") {
    return { statusCode: 200, headers: CORS, body: "ok" };
  }

  if (event.httpMethod !== "GET") {
    return {
      statusCode: 405,
      headers: { ...CORS, "Content-Type": "application/json" },
      body: JSON.stringify({ error: "Method not allowed" })
    };
  }

  const q = event.queryStringParameters || {};
  const trackingCode = String(q.trackingCode || q.code || "").trim();

  if (!trackingCode) {
    return {
      statusCode: 400,
      headers: { ...CORS, "Content-Type": "application/json" },
      body: JSON.stringify({ error: "Missing trackingCode" })
    };
  }

  // Look up the Firebase URL from the cache doc (only that field: the rest of the doc is the tracking events, not needed here)
  let firebaseUrl, etag = null;
  try {
    const [snap] = await db.getAll(db.collection("EtsyMail_TrackingCache").doc(trackingCode), { fieldMask: ["imageUrl"] });
    // The picture is overwritten whenever the snapshot is refreshed, and the cache doc is written right after, so the doc's own
    // update time names the picture. A page that already holds that version is told so (304) and nothing is read from Storage.
    if (snap.exists && snap.updateTime) etag = '"tc-' + snap.updateTime.seconds + "." + snap.updateTime.nanoseconds + '"';
    if (etag && (event.headers?.["if-none-match"] || event.headers?.["If-None-Match"]) === etag) {
      return { statusCode: 304, headers: { ...CORS, ...REVALIDATE_HEADERS, ETag: etag }, body: "" };
    }
    if (!snap.exists) {
      return {
        statusCode: 404,
        headers: { ...CORS, ...NO_CACHE_HEADERS, "Content-Type": "application/json" },
        body: JSON.stringify({ error: "Tracking image not found — not cached yet" })
      };
    }
    firebaseUrl = snap.data().imageUrl;
    if (!firebaseUrl) {
      return {
        statusCode: 404,
        headers: { ...CORS, ...NO_CACHE_HEADERS, "Content-Type": "application/json" },
        body: JSON.stringify({ error: "Cache entry has no imageUrl" })
      };
    }
  } catch (e) {
    return {
      statusCode: 500,
      headers: { ...CORS, ...NO_CACHE_HEADERS, "Content-Type": "application/json" },
      body: JSON.stringify({ error: `Firestore lookup failed: ${e.message}` })
    };
  }

  // Fetch the image from Firebase Storage (server-to-server, so no CORS)
  let res, buffer;
  try {
    res = await fetch(firebaseUrl, { timeout: 15000 });
    if (!res.ok) {
      return {
        statusCode: res.status,
        headers: { ...CORS, ...NO_CACHE_HEADERS, "Content-Type": "application/json" },
        body: JSON.stringify({ error: `Firebase Storage returned ${res.status}` })
      };
    }
    buffer = await res.buffer();
  } catch (e) {
    return {
      statusCode: 502,
      headers: { ...CORS, ...NO_CACHE_HEADERS, "Content-Type": "application/json" },
      body: JSON.stringify({ error: `Failed to fetch from Storage: ${e.message}` })
    };
  }

  // Send the bytes back with the version they were read at: the browser keeps them and asks again with that version.
  return {
    statusCode: 200,
    headers: {
      ...CORS,
      ...(etag ? { ...REVALIDATE_HEADERS, ETag: etag } : NO_CACHE_HEADERS),
      "Content-Type"                 : "image/png",
      "Cross-Origin-Resource-Policy" : "cross-origin"
    },
    body           : buffer.toString("base64"),
    isBase64Encoded: true
  };
};
