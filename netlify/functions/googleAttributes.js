// netlify/functions/googleAttributes.js
// ---------------------------------------------------------------------------
// SELF-RUNNING, idempotent bulk-fixer for the Google Shopping RECOMMENDED
// attributes that Merchant Center reports missing across the Brites catalog:
//
//   • color      — Missing on 145,092 offers (100%). PER-VARIANT, derived from
//                  the metal option: Sterling Silver -> "Silver",
//                  any *Gold*/*Filled* -> "Gold", any *Rose* -> "Rose Gold".
//                  Written as a VARIANT metafield.
//   • age_group  — Missing on 86,429 offers. Constant "adult" (Google's
//                  recommended value for non-children's products).
//   • gender     — Missing on 86,429 offers. Constant, default "unisex".
//
// All three are written into the `mm-google-shopping` metafield namespace — the
// SAME namespace the Shopify "Google & YouTube" channel reads and the SAME one
// applySiteFixes.setCustomLabels already uses for custom_label_0/1. So this
// piggybacks on the existing, proven feed pipeline. No Google API required.
//
// Architecture is intentionally identical to applySiteFixes.js: client-creds
// token, retrying gql(), cursor-paginated catalog walk, time budget, progress
// in Firestore (Brites_Editor_Meta/googleAttributesState), idempotent re-runs.
//
// Deploy: add to netlify.toml and let it run hourly; it resumes from the saved
// cursor each run and no-ops once the whole catalog is processed:
//
//   [functions."googleAttributes"]
//     schedule = "@hourly"
//
// HTTP (diagnostics / manual kick) — no auth; idempotent feed writes only:
//   GET  ...?run=now            -> one immediate run (live writes)
//   GET  ...?run=now&drain=1    -> one longer run (for fast backfill)
//   POST { action, dryRun?, cursor?, drain? }
//        actions: "run" (default dryRun=true), "status"
//
// Env: SHOPIFY_STORE, SHOPIFY_CLIENT_ID, SHOPIFY_CLIENT_SECRET,
//      SHOPIFY_API_VERSION?      (default "2025-10")
//      GMC_AGE_GROUP?            (default "adult")
//      GMC_GENDER_DEFAULT?       (default "unisex")
//
// NOTE on color & the native channel: the Google & YouTube channel auto-maps a
// variant option literally named "Color" to the color attribute. Your option is
// "Metal Choice", so we set the color explicitly via the variant metafield here
// (third-party feed apps and the feed pipeline read mm-google-shopping.color).
// If color does not surface in the feed after this runs, the guaranteed fallback
// is a one-line Merchant Center feed rule mapping the metal option -> color;
// see the deploy notes shipped alongside this file.
// ---------------------------------------------------------------------------

let _fb = null;
function fb() {
  if (_fb !== null) return _fb;
  try {
    const admin = require("./firebaseAdmin");
    _fb = { admin, db: admin.firestore(), FV: admin.firestore.FieldValue };
  } catch (e) { console.error("[googleAttributes] Firebase unavailable:", e.message); _fb = false; }
  return _fb;
}

const fetch = require("node-fetch");
const API_VERSION = process.env.SHOPIFY_API_VERSION || "2025-10";
const NS          = "mm-google-shopping";
const AGE_GROUP   = process.env.GMC_AGE_GROUP || "adult";
const GENDER      = process.env.GMC_GENDER_DEFAULT || "unisex";

/* ---- token + gql: identical pattern to applySiteFixes.js / shopifyEditor.js ---- */
let _token = null, _tokenExp = 0;
async function getToken() {
  if (_token && Date.now() < _tokenExp - 60000) return _token;
  const res = await fetch(`https://${process.env.SHOPIFY_STORE}/admin/oauth/access_token`, {
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
async function gql(query, variables, _attempt) {
  const token = await getToken();
  try {
    const res = await fetch(`https://${process.env.SHOPIFY_STORE}/admin/api/${API_VERSION}/graphql.json`, {
      method: "POST",
      headers: { "X-Shopify-Access-Token": token, "Content-Type": "application/json" },
      body: JSON.stringify({ query, variables: variables || {} })
    });
    if (res.status >= 500) throw new Error("GraphQL HTTP " + res.status);
    const data = await res.json();
    if (!res.ok) throw new Error("GraphQL HTTP " + res.status);
    if (data.errors && data.errors.length) throw new Error("GraphQL: " + JSON.stringify(data.errors));
    return data.data;
  } catch (e) {
    const msg = String((e && e.message) || e);
    const transient = /ECONNRESET|ETIMEDOUT|socket hang up|network|fetch failed|EAI_AGAIN|GraphQL HTTP 5\d\d|THROTTLED|throttl/i.test(msg);
    const attempt = _attempt || 0;
    if (transient && attempt < 2) { await new Promise(r => setTimeout(r, 350 * (attempt + 1))); return gql(query, variables, attempt + 1); }
    throw e;
  }
}

/* ================= color derivation ================= */
// Map a metal/option value to a Google color value. Returns null when we can't
// tell (we never fabricate a color — a missing color is better than a wrong one).
function metalToColor(value) {
  const v = String(value || "").toLowerCase();
  if (!v) return null;
  if (/\brose\b|rose ?gold/.test(v)) return "Rose Gold";
  if (/gold|filled|vermeil/.test(v)) return "Gold";   // 14k gold filled, 14k solid gold
  if (/silver|sterling/.test(v))     return "Silver";
  if (/platinum/.test(v))            return "Platinum";
  return null;
}
// Pick the colour for a variant from its selected options. Prefer an option that
// is clearly about metal/material/colour; otherwise scan every option value.
function variantColor(variant) {
  const opts = (variant && variant.selectedOptions) || [];
  const named = opts.find(o => /metal|material|colou?r|finish/i.test(o.name || ""));
  if (named) { const c = metalToColor(named.value); if (c) return c; }
  for (const o of opts) { const c = metalToColor(o.value); if (c) return c; }
  return null;
}

/* ================= one catalog page ================= */
// Page size is constrained by Shopify's 1000-point query-cost cap: a nested
// products×variants query costs roughly 2 + products×(2 + variants×2), so
// 6 products × 50 variants ≈ 614 points — safely under the cap, with each
// variant still becoming a colour metafield. Brites products top out around
// 32 variants, so first:50 captures every variant.
async function processPage(dryRun, cursor) {
  const d = await gql(`query($after: String) {
    products(first: 6, after: $after) {
      pageInfo { hasNextPage endCursor }
      edges { node {
        id title
        variants(first: 50) { edges { node { id selectedOptions { name value } } } }
      } }
    } }`, { after: cursor || null });

  const edges = d.products.edges;
  const metafields = [];
  let colorSet = 0, colorSkipped = 0;
  const sample = [];

  for (const e of edges) {
    const p = e.node;
    // product-level constants
    metafields.push({ ownerId: p.id, namespace: NS, key: "age_group", type: "single_line_text_field", value: AGE_GROUP });
    metafields.push({ ownerId: p.id, namespace: NS, key: "gender",    type: "single_line_text_field", value: GENDER });
    // per-variant colour
    const vedges = (p.variants && p.variants.edges) || [];
    for (const ve of vedges) {
      const color = variantColor(ve.node);
      if (color) {
        metafields.push({ ownerId: ve.node.id, namespace: NS, key: "color", type: "single_line_text_field", value: color });
        colorSet++;
      } else { colorSkipped++; }
    }
    if (sample.length < 5) sample.push({ title: p.title, variants: vedges.length });
  }

  if (!dryRun && metafields.length) {
    for (let i = 0; i < metafields.length; i += 25) {
      const r = await gql(`mutation($metafields: [MetafieldsSetInput!]!) {
        metafieldsSet(metafields: $metafields) { userErrors { field message } } }`,
        { metafields: metafields.slice(i, i + 25) });
      const ue = r.metafieldsSet.userErrors;
      if (ue.length) return { error: ue[0].message, cursor };
    }
  }

  return {
    products: edges.length, colorSet, colorSkipped, sample,
    done: !d.products.pageInfo.hasNextPage,
    cursor: d.products.pageInfo.endCursor
  };
}

/* ================= state ================= */
const STATE_DOC = "googleAttributesState";
function baseState() { return { cursor: null, done: false, productsProcessed: 0, colorSet: 0, runs: 0 }; }
async function loadState() {
  const f = fb();
  if (!f) return baseState();
  try {
    const snap = await f.db.collection("Brites_Editor_Meta").doc(STATE_DOC).get();
    return snap.exists ? Object.assign(baseState(), snap.data()) : baseState();
  } catch (e) { return baseState(); }
}
async function saveState(state) {
  const f = fb();
  if (!f) return;
  try { await f.db.collection("Brites_Editor_Meta").doc(STATE_DOC).set(state); } catch (e) {}
}

/* ================= automatic runner ================= */
async function autoRun(opts) {
  opts = opts || {};
  const started = Date.now();
  const budget  = opts.drain ? 840000 : 8000;   // drain = 14 min, for the one-time backfill when deployed as a -background function; incremental saves make any earlier timeout harmless
  const dryRun  = !!opts.dryRun;
  const state   = await loadState();
  state.runs = (state.runs || 0) + 1;
  state.lastRunAt = new Date().toISOString();
  const log = [];

  if (state.done && !opts.force) return { status: "all attributes set — nothing to do", state };
  if (opts.force) { state.done = false; state.cursor = opts.cursor || null; }

  let cursor = (opts.cursor !== undefined ? opts.cursor : state.cursor) || null;
  let pages = 0;
  try {
    while (Date.now() - started < budget) {
      const r = await processPage(dryRun, cursor);
      if (r.error) { log.push({ error: r.error }); break; }
      state.productsProcessed = (state.productsProcessed || 0) + r.products;
      state.colorSet = (state.colorSet || 0) + r.colorSet;
      cursor = r.cursor; pages++;
      state.cursor = cursor;
      if (!dryRun) await saveState(state);   // incremental save: a timeout never loses progress
      if (r.done) { state.done = true; break; }
      if (dryRun) break;                     // a dry run inspects one page only
    }
  } catch (e) { log.push({ error: String(e.message || e) }); }

  state.cursor = cursor;
  if (state.done) state.completedAt = new Date().toISOString();
  if (!dryRun) await saveState(state);
  log.push({ pagesThisRun: pages, productsProcessed: state.productsProcessed, colorSet: state.colorSet, done: !!state.done });
  return { status: state.done ? "ALL ATTRIBUTES SET" : "in progress — continues next scheduled run", log, state };
}

/* ================= handler (scheduled + HTTP) ================= */
exports.handler = async (event) => {
  const headers = {
    "Access-Control-Allow-Origin": "https://britesjewelry.com",
    "Access-Control-Allow-Headers": "Content-Type",
    "Access-Control-Allow-Methods": "POST, OPTIONS",
    "Content-Type": "application/json"
  };
  if (event.httpMethod === "OPTIONS") return { statusCode: 204, headers };

  // Netlify scheduled invocation -> fully automatic run, no auth, no input.
  const scheduled = !!(event.headers && (event.headers["x-nf-event"] === "schedule" || event.isScheduled));
  if (scheduled) {
    try {
      const out = await autoRun();
      console.log("[googleAttributes] scheduled run:", JSON.stringify(out.status));
      return { statusCode: 200, headers, body: JSON.stringify(out) };
    } catch (e) {
      return { statusCode: 500, headers, body: JSON.stringify({ ok: false, error: String(e.message || e) }) };
    }
  }

  // Browser trigger: ?run=now runs immediately; add &drain=1 for a longer backfill pass.
  const q = event.queryStringParameters || {};
  if (event.httpMethod === "GET" && q.run === "now") {
    try {
      const out = await autoRun({ drain: q.drain === "1" });
      return { statusCode: 200, headers, body: JSON.stringify(out, null, 2) };
    } catch (e) {
      return { statusCode: 500, headers, body: JSON.stringify({ ok: false, error: String(e.message || e) }) };
    }
  }

  try {
    const b = JSON.parse(event.body || "{}");
    const dryRun = b.dryRun !== false;   // dry-run unless explicitly false
    let result;
    switch (b.action) {
      case "run":
        result = await autoRun({ dryRun, drain: !!b.drain, force: !!b.force, cursor: b.cursor });
        break;
      case "status": {
        const state = await loadState();
        const preview = await processPage(true, state.cursor);   // dry preview of the next page
        result = { state, nextPagePreview: { products: preview.products, colorSet: preview.colorSet, colorSkipped: preview.colorSkipped, sample: preview.sample, done: preview.done } };
        break;
      }
      default:
        return { statusCode: 400, headers, body: JSON.stringify({ error: "unknown action", actions: ["run", "status"] }) };
    }
    return { statusCode: 200, headers, body: JSON.stringify({ ok: true, action: b.action, dryRun, result }, null, 1) };
  } catch (e) {
    return { statusCode: 500, headers, body: JSON.stringify({ ok: false, error: String(e.message || e) }) };
  }
};
