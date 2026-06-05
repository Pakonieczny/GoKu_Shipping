// netlify/functions/shopifyEditor.js
// ---------------------------------------------------------------------------
// Secure proxy between the Brites listing-editor UI (a hidden page on
// britesjewelry.com) and the Shopify Admin API.
//
// The Shopify Admin token lives ONLY here, as a Netlify environment variable,
// and is never exposed to the browser. Every request must carry the correct
// passcode (also an env var) or it is rejected — that is the "only me" gate.
//
// Required Netlify environment variables
// --------------------------------------
//   SHOPIFY_STORE        e.g. "britesjewelry.myshopify.com"  (the .myshopify domain, NOT the .com)
//   SHOPIFY_ADMIN_TOKEN  Admin API access token from your custom app  (starts with "shpat_")
//   EDIT_PASSCODE        a secret you choose; the UI must send it on every call
//   SHOPIFY_API_VERSION  optional; defaults to "2024-10"
//
// Custom app scopes needed (Shopify admin > Settings > Apps > Develop apps):
//   read_products, write_products   (covers title, variant prices, images,
//                                    collects/collection membership, metafields)
// ---------------------------------------------------------------------------

const fetch = require("node-fetch");

const API_VERSION = process.env.SHOPIFY_API_VERSION || "2024-10";

// Only allow the editor page on your own store to call this.
const ALLOWED_ORIGINS = [
  "https://britesjewelry.com",
  "https://www.britesjewelry.com"
];
function corsHeaders(origin) {
  const allow = ALLOWED_ORIGINS.includes(origin) ? origin : ALLOWED_ORIGINS[0];
  return {
    "Access-Control-Allow-Origin": allow,
    "Access-Control-Allow-Headers": "Content-Type, X-Edit-Passcode",
    "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
    "Content-Type": "application/json"
  };
}

// Thin wrapper around the Shopify Admin REST API.
async function shopify(path, method = "GET", body) {
  const store = process.env.SHOPIFY_STORE;
  const token = process.env.SHOPIFY_ADMIN_TOKEN;
  const url = `https://${store}/admin/api/${API_VERSION}/${path}`;
  const res = await fetch(url, {
    method,
    headers: {
      "X-Shopify-Access-Token": token,
      "Content-Type": "application/json"
    },
    body: body ? JSON.stringify(body) : undefined
  });
  const text = await res.text();
  let data;
  try { data = text ? JSON.parse(text) : {}; } catch { data = { raw: text }; }
  return { ok: res.ok, status: res.status, data, link: res.headers.get("link") };
}

// Pull the cursor for the next page out of Shopify's Link header.
function nextPageInfo(link) {
  if (!link) return null;
  const m = link.match(/page_info=([^&>]+)>;\s*rel="next"/);
  return m ? m[1] : null;
}

exports.handler = async function (event) {
  const origin  = event.headers.origin || event.headers.Origin || "";
  const headers = corsHeaders(origin);

  // CORS pre-flight
  if (event.httpMethod === "OPTIONS") {
    return { statusCode: 200, headers, body: "ok" };
  }

  // ── Auth gate: passcode must match the env var ──────────────────────────
  const passcode = event.headers["x-edit-passcode"] || event.headers["X-Edit-Passcode"];
  if (!process.env.EDIT_PASSCODE || passcode !== process.env.EDIT_PASSCODE) {
    return { statusCode: 401, headers, body: JSON.stringify({ error: "Unauthorized" }) };
  }

  try {
    const q      = event.queryStringParameters || {};
    const isPost = event.httpMethod === "POST";
    const body   = isPost ? JSON.parse(event.body || "{}") : {};
    const action = q.action || body.action;

    switch (action) {

      /* ───────────────────────── READS ───────────────────────── */

      // Paginated product list. UI loops, passing back `next` until null.
      case "listProducts": {
        const limit    = q.limit || 100;
        const pageInfo = q.page_info;
        const path = pageInfo
          ? `products.json?limit=${limit}&page_info=${pageInfo}`
          : `products.json?limit=${limit}`;
        const r = await shopify(path);
        return { statusCode: r.status, headers, body: JSON.stringify({
          products: r.data.products || [],
          next: nextPageInfo(r.link)
        }) };
      }

      // All collections, split into manual (custom) vs automated (smart).
      // Smart collections include `rules` + `disjunctive` so the UI can compute
      // membership and know exactly which tag/field governs each collection.
      case "listCollections": {
        const custom = await shopify(`custom_collections.json?limit=250&fields=id,title,handle`);
        const smart  = await shopify(`smart_collections.json?limit=250`); // full objects -> includes rules
        return { statusCode: 200, headers, body: JSON.stringify({
          custom: custom.data.custom_collections || [],
          smart: (smart.data.smart_collections || []).map(c => ({
            id: c.id, title: c.title, handle: c.handle,
            disjunctive: c.disjunctive,   // true = match ANY rule, false = match ALL
            rules: c.rules                // [{column, relation, condition}, ...]
          }))
        }) };
      }

      // Which manual collections a product currently belongs to.
      case "productCollects": {
        const pid = q.product_id || body.product_id;
        const r = await shopify(`collects.json?product_id=${pid}&limit=250&fields=id,collection_id,product_id`);
        return { statusCode: r.status, headers, body: JSON.stringify({ collects: r.data.collects || [] }) };
      }

      /* ───────────────────────── WRITES ──────────────────────── */

      case "updateTitle": {
        const { product_id, title } = body;
        const r = await shopify(`products/${product_id}.json`, "PUT",
          { product: { id: product_id, title } });
        return { statusCode: r.status, headers, body: JSON.stringify(r.data) };
      }

      // Update the levers that drive SMART-collection membership.
      // body: { product_id, tags?: "a, b, c", product_type?: "Necklaces" }
      // tags is the FULL comma-separated tag list (Shopify replaces all tags),
      // so the UI sends existing tags +/- the one being toggled.
      case "updateProductFields": {
        const { product_id, tags, product_type } = body;
        const product = { id: product_id };
        if (typeof tags === "string")        product.tags = tags;
        if (typeof product_type === "string") product.product_type = product_type;
        const r = await shopify(`products/${product_id}.json`, "PUT", { product });
        return { statusCode: r.status, headers, body: JSON.stringify(r.data) };
      }

      // body.variants = [{ id, price }, ...]
      case "updateVariantPrices": {
        const results = [];
        for (const v of (body.variants || [])) {
          const r = await shopify(`variants/${v.id}.json`, "PUT",
            { variant: { id: v.id, price: String(v.price) } });
          results.push({ id: v.id, ok: r.ok, status: r.status });
        }
        return { statusCode: 200, headers, body: JSON.stringify({ results }) };
      }

      // Promote an existing image to primary (position 1).
      case "setPrimaryImage": {
        const { product_id, image_id } = body;
        const r = await shopify(`products/${product_id}/images/${image_id}.json`, "PUT",
          { image: { id: image_id, position: 1 } });
        return { statusCode: r.status, headers, body: JSON.stringify(r.data) };
      }

      // Add a product to a MANUAL collection.
      case "addCollect": {
        const { product_id, collection_id } = body;
        const r = await shopify(`collects.json`, "POST",
          { collect: { product_id, collection_id } });
        return { statusCode: r.status, headers, body: JSON.stringify(r.data) };
      }

      // Remove a product from a MANUAL collection (needs the collect id).
      case "removeCollect": {
        const { collect_id } = body;
        const r = await shopify(`collects/${collect_id}.json`, "DELETE");
        return { statusCode: r.status, headers, body: JSON.stringify({ ok: r.ok }) };
      }

      // Save image zoom/pan as a product metafield (non-destructive). Upserts.
      case "setFraming": {
        const { product_id, scale, offsetX, offsetY } = body;
        const value = JSON.stringify({ scale, offsetX, offsetY });
        const list = await shopify(`products/${product_id}/metafields.json?namespace=card&key=framing`);
        const existing = (list.data.metafields || [])[0];
        const r = existing
          ? await shopify(`metafields/${existing.id}.json`, "PUT",
              { metafield: { id: existing.id, type: "json", value } })
          : await shopify(`products/${product_id}/metafields.json`, "POST",
              { metafield: { namespace: "card", key: "framing", type: "json", value } });
        return { statusCode: r.status, headers, body: JSON.stringify(r.data) };
      }

      default:
        return { statusCode: 400, headers, body: JSON.stringify({ error: "Unknown action: " + action }) };
    }
  } catch (err) {
    return { statusCode: 500, headers, body: JSON.stringify({ error: err.message }) };
  }
};
