// netlify/functions/mcOfferAudit.js
// ---------------------------------------------------------------------------
// READ-ONLY diagnostic for the July 2026 "totals keep climbing" question.
//
//   MC + the Google & YouTube app both report ~160K offers across 2 markets
//   (US, CA) = ~80K per market. The catalog was believed to be ~64.6K
//   variants. This function gets the EXACT counts straight from Shopify so
//   we know which of two worlds we're in:
//
//     A) variants ≈ 80K  -> nothing is broken. The catalog itself grew
//        (option/variant additions from bulk editing), 80K x 2 markets
//        legitimately exceeds the 150K Shopping-ads quota, and the fix is a
//        capacity decision (trim variants / single market / quota request).
//     B) variants ≈ 65K  -> the app is submitting ~15K stale offers per
//        market (deleted variant IDs never withdrawn) and needs a resync.
//
// HTTP (zero-auth, idempotent, same pattern as googleAttributes):
//   GET ...?run=now          -> counts + verdict (fast; uses count queries)
//   GET ...?run=now&deep=1   -> adds a paginated walk of ACTIVE products to
//                               break variant counts down by product status
//                               and flag the 20 most variant-heavy products
//                               (slow: ~2-4 min on a 4K-product catalog).
//
// Env: SHOPIFY_STORE, SHOPIFY_CLIENT_ID, SHOPIFY_CLIENT_SECRET,
//      SHOPIFY_API_VERSION? (default "2025-10"). Scopes: read_products only.
// ---------------------------------------------------------------------------
"use strict";

const API_VERSION = process.env.SHOPIFY_API_VERSION || "2025-10";
const MARKETS = 2;           // US + CA (post-UK-removal)
const QUOTA = 150000;        // Shopping-ads offer quota outside CSS
const APP_TOTAL_SEEN = 160128; // app Overview reading, Jul 5 2026 (reference only)

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
  _tokenExp = Date.now() + (Number(data.expires_in || 3600) * 1000);
  return _token;
}

async function gql(query, variables, _attempt) {
  const store = process.env.SHOPIFY_STORE;
  const token = await getToken();
  const res = await fetch(`https://${store}/admin/api/${API_VERSION}/graphql.json`, {
    method: "POST",
    headers: { "Content-Type": "application/json", "X-Shopify-Access-Token": token },
    body: JSON.stringify({ query, variables: variables || {} })
  });
  const body = await res.json().catch(() => ({}));
  if (res.status === 429 || (body.errors || []).some(e => (e.extensions || {}).code === "THROTTLED")) {
    if ((_attempt || 0) < 4) { await new Promise(r => setTimeout(r, 1500 * ((_attempt || 0) + 1))); return gql(query, variables, (_attempt || 0) + 1); }
  }
  if (body.errors && body.errors.length) throw new Error("GraphQL: " + JSON.stringify(body.errors).slice(0, 500));
  return body.data;
}

/* ------------------------------ counts ------------------------------ */

// One count query. `productVariantsCount`/`productsCount` return { count,
// precision }; precision "AT_LEAST" means the true number is >= count (the
// API caps exact counting on very large sets) — we surface that flag.
async function counts() {
  const d = await gql(`
    query {
      productsTotal:    productsCount { count precision }
      productsActive:   productsCount(query: "status:active")   { count precision }
      productsDraft:    productsCount(query: "status:draft")    { count precision }
      productsArchived: productsCount(query: "status:archived") { count precision }
      productsPublished: productsCount(query: "status:active AND published_status:published") { count precision }
      variantsTotal:    productVariantsCount { count precision }
      variantsActive:   productVariantsCount(query: "product_status:active") { count precision }
      variantsDraft:    productVariantsCount(query: "product_status:draft") { count precision }
      variantsArchived: productVariantsCount(query: "product_status:archived") { count precision }
    }`);
  const n = k => ({ count: (d[k] || {}).count ?? null, precision: (d[k] || {}).precision || null });
  return {
    products: { total: n("productsTotal"), active: n("productsActive"), draft: n("productsDraft"), archived: n("productsArchived"), activePublished: n("productsPublished") },
    variants: { total: n("variantsTotal"), active: n("variantsActive"), draft: n("variantsDraft"), archived: n("variantsArchived") }
  };
}

/* --------------------------- deep walk (opt) --------------------------- */

// Paginated pass over ACTIVE products: exact variant totals independent of
// the count API, plus the heaviest products (where option bloat lives).
async function deepWalk(budgetMs, startCursor) {
  const t0 = Date.now();
  let cursor = startCursor || null, products = 0, variants = 0, truncated = false;
  const heavy = []; // { title, handle, v }
  for (;;) {
    if (Date.now() - t0 > budgetMs) { truncated = true; break; }
    const d = await gql(`
      query($cursor: String) {
        products(first: 100, after: $cursor, query: "status:active") {
          edges { node { title handle variantsCount { count } } }
          pageInfo { hasNextPage endCursor }
        }
      }`, { cursor });
    const edges = (d.products && d.products.edges) || [];
    for (const e of edges) {
      const v = ((e.node.variantsCount || {}).count) || 0;
      products += 1; variants += v;
      if (heavy.length < 20 || v > heavy[heavy.length - 1].v) {
        heavy.push({ title: e.node.title, handle: e.node.handle, v });
        heavy.sort((a, b) => b.v - a.v);
        if (heavy.length > 20) heavy.pop();
      }
    }
    cursor = d.products.pageInfo.endCursor;
    if (!d.products.pageInfo.hasNextPage) { cursor = null; break; }
  }
  return { products, variants, truncated, resumeCursor: truncated ? cursor : null, top20ByVariants: heavy, ms: Date.now() - t0 };
}

/* ------------------------------ verdict ------------------------------ */

function verdict(c) {
  const vActive = (c.variants.active.count != null) ? c.variants.active.count : c.variants.total.count;
  if (vActive == null) return { verdict: "counts unavailable — run with &deep=1 for the paginated walk" };
  const expected = vActive * MARKETS;
  const perMarketSeen = Math.round(APP_TOTAL_SEEN / MARKETS);
  const gapPerMarket = perMarketSeen - vActive;
  const overQuota = expected - QUOTA;
  let call;
  if (Math.abs(gapPerMarket) <= vActive * 0.03) {
    call = "WORLD A — the app's offer count matches the real variant count. Nothing is stale; " +
      "the catalog itself has ~" + vActive.toLocaleString() + " variants, so " + MARKETS + " markets = " +
      expected.toLocaleString() + " offers" +
      (overQuota > 0 ? " (" + overQuota.toLocaleString() + " OVER the 150K quota). Fix = capacity decision: trim variants, drop to one market, or request a quota increase." : " (under quota).");
  } else if (gapPerMarket > 0) {
    call = "WORLD B — the app is submitting ~" + gapPerMarket.toLocaleString() + " MORE offers per market than " +
      "variants exist. Stale/orphaned offers; fix = force a clean resync in the Google & YouTube app " +
      "(toggle Product sync off/on once, off-hours) or wait out the 30-day offer expiry.";
  } else {
    call = "Variant count EXCEEDS app offers per market by ~" + Math.abs(gapPerMarket).toLocaleString() +
      " — app backfill still in progress; totals will keep RISING toward " + expected.toLocaleString() + ".";
  }
  return {
    activeVariants: vActive,
    markets: MARKETS,
    expectedOffers: expected,
    appTotalSeen: APP_TOTAL_SEEN,
    appPerMarket: perMarketSeen,
    gapPerMarket,
    quota: QUOTA,
    overQuotaBy: Math.max(0, expected - QUOTA),
    verdict: call
  };
}

/* ------------------------------ handler ------------------------------ */

const HEADERS = { "Content-Type": "application/json", "Access-Control-Allow-Origin": "*" };
const out = (status, obj) => ({ statusCode: status, headers: HEADERS, body: JSON.stringify(obj, null, 2) });

exports.handler = async (event) => {
  try {
    const q = (event && event.queryStringParameters) || {};
    if (q.run !== "now") {
      return out(200, {
        usage: "GET ?run=now  |  GET ?run=now&deep=1 (adds per-product walk + top-20 variant-heavy products)",
        readOnly: true
      });
    }
    const c = await counts();
    const res = { counts: c, analysis: verdict(c) };
    const capped = c.variants.active.precision === "AT_LEAST";
    if (capped) res.analysis = { note: "variant count API capped at 10K (precision AT_LEAST) — verdict requires the deep walk. Run ?run=now&deep=1; if deep.truncated, re-run with &cursor=<deep.resumeCursor> and sum the 'variants' fields.", quota: QUOTA, appTotalSeen: APP_TOTAL_SEEN };
    if (q.deep) {
      res.deep = await deepWalk(22000, q.cursor || null);
      if (!res.deep.truncated && !q.cursor) {
        // full single-pass walk -> compute the real verdict from measured variants
        res.analysis = verdict({ variants: { active: { count: res.deep.variants, precision: "EXACT" }, total: { count: res.deep.variants } }, products: c.products });
      }
    }
    return out(200, res);
  } catch (e) {
    return out(500, { error: String(e && e.message || e) });
  }
};
