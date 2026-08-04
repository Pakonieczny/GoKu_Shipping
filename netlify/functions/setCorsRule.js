/**
 * Netlify Function  →  GET /.netlify/functions/setCorsRule
 *
 * Re-applies the bucket's CORS configuration on demand.
 *
 * WHY THIS FILE CHANGED
 * ---------------------
 * The previous version kept its own hard-coded origin list containing exactly
 * one entry — shipping-1 — and setCorsConfiguration REPLACES the config rather
 * than merging into it. Because this is an unauthenticated GET, a single hit
 * from anyone (a link preview, a crawler, a bookmarked "one-shot" URL that was
 * never deleted) silently revoked Storage access for every other station until
 * the next firebaseAdmin cold start happened to put it back. That is a very
 * quiet way to break image uploads across the shop.
 *
 * It now applies the same configuration firebaseAdmin.js owns, so the worst
 * this endpoint can do is re-assert the correct rule. Idempotent, and useful
 * when you want the rule refreshed without waiting for a cold start.
 */
const admin = require("./firebaseAdmin");

exports.handler = async () => {
  try {
    await admin.applyBucketCors();
    return {
      statusCode: 200,
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        success: true,
        bucket : admin.DEFAULT_BUCKET,
        origins: admin.CORS_ORIGINS
      })
    };
  } catch (err) {
    console.error("CORS update failed:", err);
    return {
      statusCode: 500,
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ success: false, error: String((err && err.message) || err) })
    };
  }
};
