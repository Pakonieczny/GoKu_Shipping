/*  netlify/functions/etsyMailOrder.js
 *
 *  Live Etsy receipt fetcher for the inbox customer panel.
 *
 *  When an operator clicks an order in the customer panel, the inbox calls:
 *    GET /.netlify/functions/etsyMailOrder?receiptId=4040875933
 *
 *  This endpoint:
 *    1. Reads the seeded OAuth token from Firestore (config/etsyOauth)
 *    2. Auto-refreshes if within 2 min of expiry
 *    3. Calls Etsy's getShopReceipt endpoint with transactions + variations
 *    4. Returns the raw receipt JSON to the client
 *
 *  Unlike your existing etsyOrderProxy.js, this does NOT require the client
 *  to pass an access-token header — the inbox (different subdomain) doesn't
 *  have those tokens in its localStorage. Server-side token management only.
 *
 *  No auth on this endpoint because:
 *    - It's read-only
 *    - The receiptId must match a receipt from OUR shop (Etsy enforces this
 *      server-side via the shops/{shop_id}/receipts/{receipt_id} path)
 *    - Callers already need to know a specific receipt ID
 *
 *  If you want to tighten this later, add requireExtensionAuth or a CORS
 *  origin allowlist check.
 */

const fetch = require("node-fetch");
const admin = require("./firebaseAdmin");

const db = admin.firestore();
const FV = admin.firestore.FieldValue;

const SHOP_ID       = process.env.SHOP_ID;
const CLIENT_ID     = process.env.CLIENT_ID;
const CLIENT_SECRET = process.env.CLIENT_SECRET || process.env.ETSY_SHARED_SECRET;

const OAUTH_DOC_PATH = "config/etsyOauth";
const TOKEN_REFRESH_BUFFER_MS = 2 * 60 * 1000;

const CORS = {
  "Access-Control-Allow-Origin" : "*",
  "Access-Control-Allow-Headers": "Content-Type",
  "Access-Control-Allow-Methods": "GET,OPTIONS"
};

function json(statusCode, body) {
  return {
    statusCode,
    headers: { ...CORS, "Content-Type": "application/json" },
    body: JSON.stringify(body)
  };
}

// OAuth token helpers — identical pattern to etsyMailSync-background
async function refreshEtsyToken(oldRefreshToken) {
  const res = await fetch("https://api.etsy.com/v3/public/oauth/token", {
    method: "POST",
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
    body: new URLSearchParams({
      grant_type: "refresh_token",
      client_id: CLIENT_ID,
      refresh_token: oldRefreshToken
    })
  });
  if (!res.ok) {
    const body = await res.text();
    throw new Error(`Etsy token refresh failed: ${res.status} ${body}`);
  }
  const data = await res.json();
  const expires_at = Date.now() + Math.max(0, (data.expires_in - 120)) * 1000;
  await db.doc(OAUTH_DOC_PATH).set({
    access_token : data.access_token,
    refresh_token: data.refresh_token || oldRefreshToken,
    expires_at,
    updatedAt    : FV.serverTimestamp()
  }, { merge: true });
  return data.access_token;
}

async function getValidEtsyAccessToken() {
  const snap = await db.doc(OAUTH_DOC_PATH).get();
  if (!snap.exists) throw new Error(`Etsy OAuth not seeded at ${OAUTH_DOC_PATH}`);
  const tok = snap.data();
  if (!tok.refresh_token) throw new Error("No refresh_token in OAuth doc");
  const expiresAt = typeof tok.expires_at === "number" ? tok.expires_at : 0;
  if (!tok.access_token || expiresAt - Date.now() < TOKEN_REFRESH_BUFFER_MS) {
    return await refreshEtsyToken(tok.refresh_token);
  }
  return tok.access_token;
}

exports.handler = async (event) => {
  if (event.httpMethod === "OPTIONS") {
    return { statusCode: 204, headers: CORS, body: "" };
  }
  if (event.httpMethod !== "GET") {
    return json(405, { error: "Method Not Allowed" });
  }

  const qs = event.queryStringParameters || {};
  const receiptId = qs.receiptId;

  if (!receiptId) return json(400, { error: "Missing receiptId query parameter" });
  if (!/^\d+$/.test(String(receiptId))) {
    return json(400, { error: "receiptId must be numeric" });
  }

  if (!SHOP_ID || !CLIENT_ID || !CLIENT_SECRET) {
    return json(500, { error: "Missing SHOP_ID / CLIENT_ID / CLIENT_SECRET env vars" });
  }

  try {
    const accessToken = await getValidEtsyAccessToken();

    // Step 1 — fetch the receipt with its transactions.
    const url =
      `https://api.etsy.com/v3/application/shops/${SHOP_ID}` +
      `/receipts/${receiptId}?includes=` +
      ["Transactions", "Transactions.personalization", "Transactions.variations"].join(",");

    const res = await fetch(url, {
      headers: {
        Authorization: `Bearer ${accessToken}`,
        "x-api-key": `${CLIENT_ID}:${CLIENT_SECRET}`,
        "Content-Type": "application/json"
      }
    });

    const payload = await res.json();
    if (!res.ok) {
      return json(res.status, { error: (payload && payload.error) || `Etsy API ${res.status}`, details: payload });
    }

    // Step 2 — enrich each transaction with a listing thumbnail + listing URL.
    //
    // Etsy's getShopReceipt doesn't include listing images in the transaction
    // payload (despite supporting various `includes`, images aren't one of the
    // options for transactions). We fetch them in parallel from
    // getListingImages for each unique listing_id in the receipt.
    //
    // Typical receipt has 1-5 line items → 1-5 parallel image fetches. At ~200ms
    // each with parallelism, this adds ~200-400ms to the modal open.
    //
    // We pick the FIRST image (index 0) for each listing — the primary product
    // photo. If getListingImages fails for a listing (e.g. listing was deleted
    // since the purchase), we fall back to null and the UI shows a placeholder.
    const transactions = Array.isArray(payload.transactions) ? payload.transactions : [];
    const uniqueListingIds = Array.from(new Set(
      transactions.map(t => t.listing_id).filter(id => id != null)
    ));

    const imageUrlByListingId = {};
    if (uniqueListingIds.length) {
      await Promise.all(uniqueListingIds.map(async (listingId) => {
        try {
          const imgRes = await fetch(
            `https://api.etsy.com/v3/application/listings/${listingId}/images`,
            {
              headers: {
                Authorization: `Bearer ${accessToken}`,
                "x-api-key": `${CLIENT_ID}:${CLIENT_SECRET}`,
                "Content-Type": "application/json"
              }
            }
          );
          if (!imgRes.ok) return;  // skip on failure; UI handles gracefully
          const imgData = await imgRes.json();
          const results = Array.isArray(imgData.results) ? imgData.results : [];
          if (!results.length) return;
          // Prefer a reasonably-sized thumbnail. Etsy's listing-image object has
          // url_75x75, url_170x135, url_224xN, url_340x270, url_570xN, url_fullxfull.
          // 170x135 is ideal for a modal row thumbnail.
          const img = results[0];
          imageUrlByListingId[listingId] =
            img.url_170x135 ||
            img.url_224xN ||
            img.url_75x75 ||
            img.url_340x270 ||
            img.url_570xN ||
            img.url_fullxfull ||
            null;
        } catch (err) {
          // Don't let one failed image break the whole modal
          console.warn(`Image fetch failed for listing ${listingId}:`, err.message);
        }
      }));
    }

    // Step 3 — attach thumbnail + listingUrl to each transaction.
    for (const t of transactions) {
      if (t.listing_id) {
        t.thumbnail_url = imageUrlByListingId[t.listing_id] || null;
        t.listing_url = `https://www.etsy.com/listing/${t.listing_id}`;
      } else {
        t.thumbnail_url = null;
        t.listing_url = null;
      }
    }

    return json(200, payload);

  } catch (err) {
    console.error("etsyMailOrder error:", err);
    return json(500, { error: err.message || "Unknown error" });
  }
};
