/* netlify/functions/etsyMailTrackingImage.js
 *
 * Image proxy for tracking screenshots.
 *
 * Problem this solves:
 *   Firebase Storage serves images with a public download token, but it
 *   sends neither CORS (Access-Control-Allow-Origin) nor CORP
 *   (Cross-Origin-Resource-Policy) headers. Our site sets
 *   Cross-Origin-Embedder-Policy: require-corp, which means cross-origin
 *   resources must explicitly opt-in via CORP. Without CORP, the browser
 *   blocks embedding OR a direct fetch() → blob approach.
 *
 * Solution:
 *   Fetch the Firebase Storage image from Netlify, stream it back to the
 *   browser with a same-origin response. Since this endpoint lives on
 *   etsy-mail-1.goldenspike.app (same origin as the inbox), COEP doesn't
 *   apply. We add Cache-Control so Netlify's CDN caches it, keeping latency
 *   and function invocation count down.
 *
 * Usage:
 *   GET /.netlify/functions/etsyMailTrackingImage?trackingCode=<code>
 *     → 302 redirect to Firebase URL (uses the cache doc's stored URL)
 *   GET /.netlify/functions/etsyMailTrackingImage?trackingCode=<code>&mode=proxy
 *     → streams the PNG bytes directly (bypasses CORS)
 *
 * We use mode=proxy by default for COEP compatibility.
 */

const admin  = require("./firebaseAdmin");
const fetch  = require("node-fetch");

const db = admin.firestore();

const CORS = {
  "Access-Control-Allow-Origin" : "*",
  "Access-Control-Allow-Methods": "GET, OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type"
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

  // Look up the Firebase URL from the cache doc
  let firebaseUrl;
  try {
    const snap = await db.collection("EtsyMail_TrackingCache").doc(trackingCode).get();
    if (!snap.exists) {
      return {
        statusCode: 404,
        headers: { ...CORS, "Content-Type": "application/json" },
        body: JSON.stringify({ error: "Tracking image not found — not cached yet" })
      };
    }
    firebaseUrl = snap.data().imageUrl;
    if (!firebaseUrl) {
      return {
        statusCode: 404,
        headers: { ...CORS, "Content-Type": "application/json" },
        body: JSON.stringify({ error: "Cache entry has no imageUrl" })
      };
    }
  } catch (e) {
    return {
      statusCode: 500,
      headers: { ...CORS, "Content-Type": "application/json" },
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
        headers: { ...CORS, "Content-Type": "application/json" },
        body: JSON.stringify({ error: `Firebase Storage returned ${res.status}` })
      };
    }
    buffer = await res.buffer();
  } catch (e) {
    return {
      statusCode: 502,
      headers: { ...CORS, "Content-Type": "application/json" },
      body: JSON.stringify({ error: `Failed to fetch from Storage: ${e.message}` })
    };
  }

  // Stream the bytes back with same-origin-friendly headers
  return {
    statusCode: 200,
    headers: {
      ...CORS,
      "Content-Type"                 : "image/png",
      "Cross-Origin-Resource-Policy" : "cross-origin",
      "Cache-Control"                : "public, max-age=900"   // 15 min CDN cache
    },
    body           : buffer.toString("base64"),
    isBase64Encoded: true
  };
};
