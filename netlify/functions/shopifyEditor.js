// netlify/functions/shopifyEditor.js
// ---------------------------------------------------------------------------
// Secure proxy between the Brites in-grid editor (on britesjewelry.com) and the
// Shopify Admin GraphQL API.
//
// 2026 model:
//   - Auth: client-credentials grant. We hold the app's Client ID + Secret as
//     env vars and exchange them for a short-lived token (auto-refreshed ~24h).
//     No shpat_ token, nothing in the browser.
//   - API: GraphQL Admin API (REST product endpoints are retired for new apps).
//
// The editor's action names + request/response shapes are unchanged, so the
// theme snippet does not need any edits.
//
// Required Netlify environment variables:
//   SHOPIFY_STORE          e.g. "britesjewelry.myshopify.com"
//   SHOPIFY_CLIENT_ID      Client ID from your Dev Dashboard app
//   SHOPIFY_CLIENT_SECRET  Client secret from your Dev Dashboard app
//   EDIT_PASSCODE          a secret you choose; the UI must send it on every call
//   SHOPIFY_API_VERSION    optional; defaults to "2025-10"
// ---------------------------------------------------------------------------

const fetch = require("node-fetch");

const API_VERSION = process.env.SHOPIFY_API_VERSION || "2025-10";

const ALLOWED_ORIGINS = ["https://britesjewelry.com", "https://www.britesjewelry.com"];
function corsHeaders(origin) {
  const allow = ALLOWED_ORIGINS.includes(origin) ? origin : ALLOWED_ORIGINS[0];
  return {
    "Access-Control-Allow-Origin": allow,
    "Access-Control-Allow-Headers": "Content-Type, X-Edit-Passcode",
    "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
    "Content-Type": "application/json"
  };
}

/* ---- client-credentials token (cached across warm invocations) ---- */
let _token = null, _tokenExp = 0;
async function getToken() {
  if (_token && Date.now() < _tokenExp - 60000) return _token;
  const store = process.env.SHOPIFY_STORE;
  const res = await fetch(`https://${store}/admin/oauth/access_token`, {
    method: "POST",
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
    body: new URLSearchParams({
      grant_type: "client_credentials",
      client_id: process.env.SHOPIFY_CLIENT_ID,
      client_secret: process.env.SHOPIFY_CLIENT_SECRET
    })
  });
  const text = await res.text();
  if (!res.ok) throw new Error("Token request failed (" + res.status + "): " + text);
  const data = JSON.parse(text);
  _token = data.access_token;
  _tokenExp = Date.now() + (data.expires_in || 86399) * 1000;
  return _token;
}

/* ---- GraphQL helper ---- */
async function gql(query, variables) {
  const store = process.env.SHOPIFY_STORE;
  const token = await getToken();
  const res = await fetch(`https://${store}/admin/api/${API_VERSION}/graphql.json`, {
    method: "POST",
    headers: { "X-Shopify-Access-Token": token, "Content-Type": "application/json" },
    body: JSON.stringify({ query, variables: variables || {} })
  });
  const data = await res.json();
  if (!res.ok) throw new Error("GraphQL HTTP " + res.status);
  if (data.errors && data.errors.length) throw new Error("GraphQL: " + JSON.stringify(data.errors));
  return data.data;
}

/* ---- shape a GraphQL product into the REST-like form the editor expects ---- */
function shapeProduct(node) {
  const optPos = {};
  (node.options || []).forEach(o => { optPos[o.name] = o.position; });
  const variants = ((node.variants && node.variants.edges) || []).map(e => {
    const v = e.node; const out = { id: v.id, price: v.price };
    (v.selectedOptions || []).forEach(so => { const p = optPos[so.name]; if (p) out["option" + p] = so.value; });
    return out;
  });
  const images = ((node.media && node.media.edges) || [])
    .map(e => (e.node && e.node.image) ? { id: e.node.id, src: e.node.image.url } : null)
    .filter(Boolean);
  return {
    id: node.id, title: node.title, handle: node.handle,
    product_type: node.productType, tags: (node.tags || []).join(", "),
    options: (node.options || []).map(o => ({ name: o.name, position: o.position })),
    variants, images
  };
}

exports.handler = async function (event) {
  const origin = event.headers.origin || event.headers.Origin || "";
  const headers = corsHeaders(origin);
  const reply = (status, obj) => ({ statusCode: status, headers, body: JSON.stringify(obj) });

  if (event.httpMethod === "OPTIONS") return { statusCode: 200, headers, body: "ok" };

  const passcode = event.headers["x-edit-passcode"] || event.headers["X-Edit-Passcode"];
  if (!process.env.EDIT_PASSCODE || passcode !== process.env.EDIT_PASSCODE)
    return reply(401, { error: "Unauthorized" });

  try {
    const q = event.queryStringParameters || {};
    const isPost = event.httpMethod === "POST";
    const body = isPost ? JSON.parse(event.body || "{}") : {};
    const action = q.action || body.action;

    switch (action) {

      /* ---------- READS ---------- */

      case "listCollections": {
        const d = await gql(`query {
          collections(first: 250) { edges { node {
            id title handle
            ruleSet { appliedDisjunctively rules { column relation condition } }
          } } }
        }`);
        const smart = [], custom = [];
        (d.collections.edges || []).forEach(e => {
          const n = e.node;
          if (n.ruleSet) {
            smart.push({
              id: n.id, title: n.title, handle: n.handle,
              disjunctive: n.ruleSet.appliedDisjunctively,
              rules: (n.ruleSet.rules || []).map(r => ({
                column: String(r.column).toLowerCase(),
                relation: String(r.relation).toLowerCase(),
                condition: r.condition
              }))
            });
          } else {
            custom.push({ id: n.id, title: n.title, handle: n.handle });
          }
        });
        return reply(200, { smart, custom });
      }

      // Single product, full data (by handle). Used when you open a card.
      case "getProduct": {
        const handle = q.handle || body.handle;
        const d = await gql(`query($q: String!) {
          products(first: 1, query: $q) { edges { node {
            id title handle productType tags
            options { name position }
            media(first: 50) { edges { node { ... on MediaImage { id image { url } } } } }
            variants(first: 100) { edges { node { id price selectedOptions { name value } } } }
          } } }
        }`, { q: "handle:" + handle });
        const node = (d.products.edges[0] || {}).node;
        if (!node) return reply(404, { error: "Product not found" });
        return reply(200, { product: shapeProduct(node) });
      }

      // Catalog scan for price presets (on demand). Small page size to respect
      // GraphQL cost limits; the UI loops using `next`.
      case "listProducts": {
        const cursor = q.page_info || null;
        const d = await gql(`query($cursor: String) {
          products(first: 40, after: $cursor) { edges { node {
            id title handle productType tags
            options { name position }
            variants(first: 20) { edges { node { id price selectedOptions { name value } } } }
          } } pageInfo { hasNextPage endCursor } }
        }`, { cursor });
        const products = (d.products.edges || []).map(e => shapeProduct(e.node));
        const next = d.products.pageInfo.hasNextPage ? d.products.pageInfo.endCursor : null;
        return reply(200, { products, next });
      }

      /* ---------- WRITES ---------- */

      case "updateTitle": {
        const d = await gql(`mutation($p: ProductUpdateInput!) {
          productUpdate(product: $p) { product { id } userErrors { field message } }
        }`, { p: { id: body.product_id, title: body.title } });
        const ue = d.productUpdate.userErrors;
        return ue.length ? reply(400, { error: ue[0].message }) : reply(200, { ok: true });
      }

      // Smart-collection levers: tags (full list) and/or product type.
      case "updateProductFields": {
        const p = { id: body.product_id };
        if (typeof body.tags === "string")
          p.tags = body.tags.split(",").map(t => t.trim()).filter(Boolean);
        if (typeof body.product_type === "string") p.productType = body.product_type;
        const d = await gql(`mutation($p: ProductUpdateInput!) {
          productUpdate(product: $p) { product { id } userErrors { field message } }
        }`, { p });
        const ue = d.productUpdate.userErrors;
        return ue.length ? reply(400, { error: ue[0].message }) : reply(200, { ok: true });
      }

      // body.variants = [{ id, price }]. We look up each variant's product, then
      // bulk-update per product (bulk update requires the productId).
      case "updateVariantPrices": {
        const variants = body.variants || [];
        const ids = variants.map(v => v.id);
        const nd = await gql(`query($ids: [ID!]!) {
          nodes(ids: $ids) { ... on ProductVariant { id product { id } } }
        }`, { ids });
        const prodOf = {};
        (nd.nodes || []).forEach(n => { if (n && n.product) prodOf[n.id] = n.product.id; });
        const groups = {};
        variants.forEach(v => {
          const pid = prodOf[v.id]; if (!pid) return;
          (groups[pid] = groups[pid] || []).push({ id: v.id, price: String(v.price) });
        });
        const results = [];
        for (const pid of Object.keys(groups)) {
          const d = await gql(`mutation($pid: ID!, $vars: [ProductVariantsBulkInput!]!) {
            productVariantsBulkUpdate(productId: $pid, variants: $vars) {
              productVariants { id } userErrors { field message }
            }
          }`, { pid, vars: groups[pid] });
          const ok = d.productVariantsBulkUpdate.userErrors.length === 0;
          groups[pid].forEach(v => results.push({ id: v.id, ok }));
        }
        return reply(200, { results });
      }

      // Promote an existing image to primary (move it to position 0).
      case "setPrimaryImage": {
        const d = await gql(`mutation($id: ID!, $moves: [MoveInput!]!) {
          productReorderMedia(id: $id, moves: $moves) { job { id } mediaUserErrors { field message } }
        }`, { id: body.product_id, moves: [{ id: body.image_id, newPosition: "0" }] });
        const ue = d.productReorderMedia.mediaUserErrors;
        return ue.length ? reply(400, { error: ue[0].message }) : reply(200, { ok: true });
      }

      // Copy an image into this product by URL. Shopify fetches the URL and
      // creates an independent MediaImage on the target product (a true copy).
      // Returns immediately with the new media id; the file finishes processing
      // on Shopify's side a moment later (status PROCESSING -> READY).
      case "addImage": {
        const src = body.src;
        if (!src) return reply(400, { error: "Missing image src" });
        const d = await gql(`mutation($pid: ID!, $media: [CreateMediaInput!]!) {
          productCreateMedia(productId: $pid, media: $media) {
            media { ... on MediaImage { id status image { url } } }
            mediaUserErrors { field message }
          }
        }`, { pid: body.product_id, media: [{ originalSource: src, mediaContentType: "IMAGE", alt: body.alt || "" }] });
        const ue = d.productCreateMedia.mediaUserErrors;
        if (ue.length) return reply(400, { error: ue[0].message });
        const m = (d.productCreateMedia.media || [])[0] || {};
        return reply(200, { ok: true, image: { id: m.id, src: (m.image && m.image.url) || src, status: m.status || "PROCESSING" } });
      }

      // Reorder a product's media to exactly match body.image_ids (the full
      // desired order). First id becomes the primary image. Async on Shopify's
      // side (returns a job) but takes effect within a few seconds.
      case "reorderImages": {
        const ids = body.image_ids || [];
        if (!ids.length) return reply(400, { error: "No image order supplied" });
        const moves = ids.map((id, i) => ({ id, newPosition: String(i) }));
        const d = await gql(`mutation($id: ID!, $moves: [MoveInput!]!) {
          productReorderMedia(id: $id, moves: $moves) { job { id } mediaUserErrors { field message } }
        }`, { id: body.product_id, moves });
        const ue = d.productReorderMedia.mediaUserErrors;
        return ue.length ? reply(400, { error: ue[0].message }) : reply(200, { ok: true });
      }

      // Permanently remove one or more media from a product.
      case "deleteImage": {
        const ids = body.image_ids || (body.image_id ? [body.image_id] : []);
        if (!ids.length) return reply(400, { error: "No image id supplied" });
        const d = await gql(`mutation($mediaIds: [ID!]!, $productId: ID!) {
          productDeleteMedia(mediaIds: $mediaIds, productId: $productId) {
            deletedMediaIds mediaUserErrors { field message }
          }
        }`, { mediaIds: ids, productId: body.product_id });
        const ue = d.productDeleteMedia.mediaUserErrors;
        return ue.length ? reply(400, { error: ue[0].message }) : reply(200, { ok: true, deleted: d.productDeleteMedia.deletedMediaIds });
      }

      // Save image zoom/pan as a product metafield (non-destructive, upsert).
      case "setFraming": {
        const value = JSON.stringify({ scale: body.scale, offsetX: body.offsetX, offsetY: body.offsetY });
        const d = await gql(`mutation($mf: [MetafieldsSetInput!]!) {
          metafieldsSet(metafields: $mf) { userErrors { field message } }
        }`, { mf: [{ ownerId: body.product_id, namespace: "card", key: "framing", type: "json", value }] });
        const ue = d.metafieldsSet.userErrors;
        return ue.length ? reply(400, { error: ue[0].message }) : reply(200, { ok: true });
      }

      // Permanently delete an entire product (and its variants + media).
      case "deleteProduct": {
        const d = await gql(`mutation($input: ProductDeleteInput!) {
          productDelete(input: $input) { deletedProductId userErrors { field message } }
        }`, { input: { id: body.product_id } });
        const ue = d.productDelete.userErrors;
        return ue.length ? reply(400, { error: ue[0].message }) : reply(200, { ok: true, deleted: d.productDelete.deletedProductId });
      }

      default:
        return reply(400, { error: "Unknown action: " + action });
    }
  } catch (err) {
    return reply(500, { error: err.message });
  }
};
