/*  netlify/functions/etsyMailImage.js
 *
 *  Image redirector for mirrored Etsy message images.
 *
 *  The mirror function (etsyMailMirrorImage) uploads Etsy CDN images to
 *  Firebase Storage at paths like:
 *     etsymail/etsy_conv_12345/etsy_<contentHash>/<imgHash>.jpg
 *
 *  Those paths aren't directly fetchable — they need a signed URL. Rather
 *  than storing signed URLs on message docs (they expire), this endpoint
 *  signs on demand and 302-redirects the browser.
 *
 *  Usage (from the inbox UI):
 *     GET /.netlify/functions/etsyMailImage?path=etsymail/.../hash.jpg
 *
 *  Response: 302 Found with Location header set to a fresh signed URL
 *  valid for 10 minutes. Browser follows the redirect and loads the image.
 *
 *  Security:
 *    - Path must begin with "etsymail/" — prevents arbitrary bucket access
 *    - No auth required; image paths are content-hashed and non-guessable
 *    - The signed URL itself expires quickly (10 min) to limit URL sharing
 */

const admin = require("./firebaseAdmin");

const bucket = admin.storage().bucket();

// Browser-level CORS; images can be embedded cross-origin
const CORS = {
  "Access-Control-Allow-Origin" : "*",
  "Access-Control-Allow-Methods": "GET, OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type",
  "Access-Control-Max-Age"      : "86400"
};

// Allowed path prefix — prevents requests for arbitrary bucket paths
const PATH_PREFIX = "etsymail/";

exports.handler = async (event) => {
  if (event.httpMethod === "OPTIONS") {
    return { statusCode: 204, headers: CORS, body: "" };
  }
  if (event.httpMethod !== "GET") {
    return {
      statusCode: 405,
      headers: { ...CORS, "Content-Type": "application/json" },
      body: JSON.stringify({ error: "Method Not Allowed" })
    };
  }

  try {
    const qs = event.queryStringParameters || {};
    const rawPath = qs.path || "";

    if (!rawPath) {
      return {
        statusCode: 400,
        headers: { ...CORS, "Content-Type": "application/json" },
        body: JSON.stringify({ error: "Missing 'path' query parameter" })
      };
    }

    // Prevent path traversal and unauthorized prefixes
    if (!rawPath.startsWith(PATH_PREFIX)) {
      return {
        statusCode: 403,
        headers: { ...CORS, "Content-Type": "application/json" },
        body: JSON.stringify({ error: "Path must start with 'etsymail/'" })
      };
    }
    if (rawPath.includes("..") || rawPath.includes("\\")) {
      return {
        statusCode: 400,
        headers: { ...CORS, "Content-Type": "application/json" },
        body: JSON.stringify({ error: "Invalid path" })
      };
    }

    const file = bucket.file(rawPath);

    // Check existence quickly — avoids signing URLs that would 404 anyway
    const [exists] = await file.exists();
    if (!exists) {
      return {
        statusCode: 404,
        headers: { ...CORS, "Content-Type": "application/json" },
        body: JSON.stringify({ error: "Image not found", path: rawPath })
      };
    }

    const [signedUrl] = await file.getSignedUrl({
      action : "read",
      expires: Date.now() + 10 * 60 * 1000   // 10 minutes
    });

    return {
      statusCode: 302,
      headers: {
        ...CORS,
        Location     : signedUrl,
        "Cache-Control": "public, max-age=300"  // let browsers cache redirect briefly
      },
      body: ""
    };

  } catch (err) {
    console.error("etsyMailImage error:", err);
    return {
      statusCode: 500,
      headers: { ...CORS, "Content-Type": "application/json" },
      body: JSON.stringify({ error: err.message || "Unknown error" })
    };
  }
};
