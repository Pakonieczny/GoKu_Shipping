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
// PRODUCT IDENTIFIERS (for Product Ratings): during the same catalog walk this
// also captures each product's representative SKU + barcode and writes them to
// Firestore at  Brites_ProductIds/{handle} = { sku, gtin, title, updatedAt }.
// productReviewsFeed.js reads that collection to emit GTIN / SKU / Brand+MPN on
// each review, which is what clears Merchant Center's "Missing or invalid
// product_id" warning so star ratings can serve. Identifier writes happen on
// every live run (covering new products going forward); for a fast one-time
// backfill of the existing catalog use idsOnly mode (skips the metafield writes
// entirely — just reads products and writes the id docs):
//
//   ...?run=now&drain=1&force=1&idsonly=1   (deploy as -background for the 14-min budget)
//
// AUTO-COVERAGE OF NEW PRODUCTS: once the initial backfill is complete, each
// scheduled run instead performs a "recent-changes sweep" — it scans products
// updated since the last sweep (sortKey UPDATED_AT) and writes BOTH attributes
// and identifier docs for them. So products added/edited in Shopify get fully
// populated automatically within the hour, with no manual re-run. Watermark is
// stored in Brites_Editor_Meta/googleAttributesState.lastSweepAt.
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
//   GET  ...?run=now                         -> one immediate run (live writes)
//   GET  ...?run=now&drain=1                  -> one longer run (fast backfill)
//   GET  ...?run=now&force=1                  -> re-walk the whole catalog again
//   GET  ...?run=now&force=1&idsonly=1&drain=1-> fast id-only backfill (no metafield writes)
//   POST { action, dryRun?, cursor?, drain?, force?, idsOnly? }
//        actions: "run" (default dryRun=true), "status"
//
// Env: SHOPIFY_STORE, SHOPIFY_CLIENT_ID, SHOPIFY_CLIENT_SECRET,
//      SHOPIFY_API_VERSION?      (default "2025-10")
//      GMC_AGE_GROUP?            (default "adult")
//      GMC_GENDER_DEFAULT?       (default "unisex")
//      GMC_WRITE_CONCURRENCY?    (default 5 — parallel metafield writes per page)
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
const WRITE_CONCURRENCY = Number(process.env.GMC_WRITE_CONCURRENCY || 5);   // parallel metafield writes per page; throttle-retry in gql() handles backpressure
const PRODUCT_IDS_COLLECTION = "Brites_ProductIds";   // {handle} -> { sku, gtin, title, updatedAt }; read by productReviewsFeed.js

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

/* ---- small concurrency-limited runner (no deps) ---- */
async function runWithConcurrency(tasks, limit) {
  let i = 0;
  const n = Math.min(limit, tasks.length);
  const workers = new Array(n).fill(0).map(async () => {
    while (i < tasks.length) {
      const idx = i++;            // single-threaded JS: i++ is atomic
      await tasks[idx]();
    }
  });
  await Promise.all(workers);
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

/* ================= product-identifier write ================= */
// Persist one representative SKU + barcode per product to Firestore so the
// reviews feed can match reviews -> products. Non-fatal: an identifier-write
// failure must never break the attribute pass.
async function writeProductIds(idRows) {
  const f = fb();
  if (!f || !idRows.length) return 0;
  const batch = f.db.batch();
  let n = 0;
  for (const row of idRows) {
    if (!row.handle) continue;
    if (!row.sku && !row.barcode) continue;     // nothing useful to match on -> skip (feed falls back to brand)
    batch.set(
      f.db.collection(PRODUCT_IDS_COLLECTION).doc(row.handle),
      { sku: row.sku || "", gtin: row.barcode || "", title: row.title || "", updatedAt: new Date().toISOString() },
      { merge: true }
    );
    n++;
  }
  if (!n) return 0;
  try { await batch.commit(); return n; }
  catch (e) { console.error("[googleAttributes] product-id write failed:", e && e.message); return 0; }
}

/* ================= process a batch of product edges ================= */
// Shared by the full-catalog walk (processPage) and the recent-changes sweep.
// Writes attribute metafields (unless idsOnly) and the Brites_ProductIds docs.
async function processEdges(edges, dryRun, idsOnly) {
  const metafields = [];
  const idRows = [];
  let colorSet = 0, colorSkipped = 0;
  const sample = [];

  for (const e of edges) {
    const p = e.node;
    const vedges = (p.variants && p.variants.edges) || [];

    // representative identifiers: first non-empty sku + first non-empty barcode
    let repSku = "", repBarcode = "";
    for (const ve of vedges) {
      const v = ve.node || {};
      if (!repSku && v.sku && String(v.sku).trim()) repSku = String(v.sku).trim();
      if (!repBarcode && v.barcode && String(v.barcode).trim()) repBarcode = String(v.barcode).trim();
      if (repSku && repBarcode) break;
    }
    idRows.push({ handle: p.handle, title: p.title, sku: repSku, barcode: repBarcode });

    if (!idsOnly) {
      // product-level constants
      metafields.push({ ownerId: p.id, namespace: NS, key: "age_group", type: "single_line_text_field", value: AGE_GROUP });
      metafields.push({ ownerId: p.id, namespace: NS, key: "gender",    type: "single_line_text_field", value: GENDER });
      // per-variant colour
      for (const ve of vedges) {
        const color = variantColor(ve.node);
        if (color) {
          metafields.push({ ownerId: ve.node.id, namespace: NS, key: "color", type: "single_line_text_field", value: color });
          colorSet++;
        } else { colorSkipped++; }
      }
    }
    if (sample.length < 5) sample.push({ title: p.title, handle: p.handle, variants: vedges.length, sku: repSku, gtin: repBarcode });
  }

  if (!dryRun && !idsOnly && metafields.length) {
    // Split into Shopify's 25-per-call limit, then write the chunks in parallel
    // (capped). gql()'s THROTTLED retry self-paces if we outrun the rate limit.
    const chunks = [];
    for (let i = 0; i < metafields.length; i += 25) chunks.push(metafields.slice(i, i + 25));
    let firstError = null;
    await runWithConcurrency(chunks.map(chunk => async () => {
      if (firstError) return;                       // stop issuing new writes after an error
      const r = await gql(`mutation($metafields: [MetafieldsSetInput!]!) {
        metafieldsSet(metafields: $metafields) { userErrors { field message } } }`,
        { metafields: chunk });
      const ue = r.metafieldsSet.userErrors;
      if (ue.length && !firstError) firstError = ue[0].message;
    }), WRITE_CONCURRENCY);
    if (firstError) return { error: firstError, colorSet, colorSkipped, sample, idsWritten: 0 };
  }

  // identifier docs are written on every live run (both modes)
  let idsWritten = 0;
  if (!dryRun) idsWritten = await writeProductIds(idRows);

  return { colorSet, colorSkipped, idsWritten, sample };
}

/* ================= one catalog page (full walk) ================= */
// Page size is constrained by Shopify's 1000-point query-cost cap: a nested
// products×variants query costs roughly 2 + products×(2 + variants×2), so
// 6 products × 50 variants ≈ 614 points — safely under the cap, with each
// variant still becoming a colour metafield. Brites products top out around
// 32 variants, so first:50 captures every variant. (sku/barcode are scalar
// fields and add ~0 to the query cost.)
//
// idsOnly: skip ALL metafield writes; only read products and persist their
// SKU/barcode identifier docs. Used for the fast one-time id backfill.
async function processPage(dryRun, cursor, idsOnly) {
  const d = await gql(`query($after: String) {
    products(first: 6, after: $after) {
      pageInfo { hasNextPage endCursor }
      edges { node {
        id title handle
        variants(first: 50) { edges { node { id selectedOptions { name value } sku barcode } } }
      } }
    } }`, { after: cursor || null });

  const r = await processEdges(d.products.edges, dryRun, idsOnly);
  if (r.error) return { error: r.error, cursor };   // leave cursor un-advanced → page retries next run (idempotent)
  return {
    products: d.products.edges.length, colorSet: r.colorSet, colorSkipped: r.colorSkipped,
    idsWritten: r.idsWritten, sample: r.sample,
    done: !d.products.pageInfo.hasNextPage,
    cursor: d.products.pageInfo.endCursor
  };
}

/* ================= state ================= */
const STATE_DOC = "googleAttributesState";
function baseState() { return { cursor: null, done: false, productsProcessed: 0, colorSet: 0, idsWritten: 0, runs: 0, lastSweepAt: null }; }
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
  const idsOnly = !!opts.idsOnly;
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
      const r = await processPage(dryRun, cursor, idsOnly);
      if (r.error) { log.push({ error: r.error }); break; }
      state.productsProcessed = (state.productsProcessed || 0) + r.products;
      state.colorSet = (state.colorSet || 0) + r.colorSet;
      state.idsWritten = (state.idsWritten || 0) + (r.idsWritten || 0);
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
  log.push({ pagesThisRun: pages, productsProcessed: state.productsProcessed, colorSet: state.colorSet, idsWritten: state.idsWritten, idsOnly, done: !!state.done });
  return { status: state.done ? (idsOnly ? "ALL PRODUCT IDS WRITTEN" : "ALL ATTRIBUTES SET") : "in progress — continues next scheduled run", log, state };
}

/* ================= recent-changes sweep (auto-covers new/edited products) =================
   Runs on the schedule once the initial backfill is complete. Scans products
   updated since the watermark (state.lastSweepAt), oldest-changed first, and
   writes BOTH attributes and identifier docs. The watermark advances to the
   newest product processed, so it resumes correctly across runs and never
   misses or permanently skips a product. Idempotent: re-touching a product
   just re-writes the same values. */
async function sweepRecent(opts) {
  opts = opts || {};
  const started = Date.now();
  const budget  = opts.drain ? 840000 : 8000;
  const state   = await loadState();
  state.runs = (state.runs || 0) + 1;
  state.lastRunAt = new Date().toISOString();
  const log = [];

  // Watermark: last sweep, else when the backfill completed, else 2 days back.
  const sinceTs = state.lastSweepAt || state.completedAt || new Date(Date.now() - 2 * 864e5).toISOString();
  const sinceQ  = new Date(sinceTs).toISOString().replace(/\.\d{3}Z$/, "Z");

  let cursor = null, hasNext = true, pages = 0, processed = 0, idsWritten = 0, colorSet = 0;
  let maxUpdated = sinceTs;
  try {
    while (hasNext && Date.now() - started < budget) {
      const d = await gql(`query($after: String, $q: String) {
        products(first: 6, after: $after, query: $q, sortKey: UPDATED_AT) {
          pageInfo { hasNextPage endCursor }
          edges { node {
            id title handle updatedAt
            variants(first: 50) { edges { node { id selectedOptions { name value } sku barcode } } }
          } }
        } }`, { after: cursor, q: `updated_at:>${sinceQ}` });

      // Skip any product not strictly newer than the watermark (query is a backstop).
      const edges = (d.products.edges || []).filter(e => !e.node.updatedAt || e.node.updatedAt > sinceTs);
      if (edges.length) {
        const r = await processEdges(edges, false, false);   // full: attributes + identifiers
        if (r.error) { log.push({ error: r.error }); break; }
        processed += edges.length; idsWritten += r.idsWritten; colorSet += r.colorSet;
        for (const e of edges) { if (e.node.updatedAt && e.node.updatedAt > maxUpdated) maxUpdated = e.node.updatedAt; }
      }
      cursor = d.products.pageInfo.endCursor;
      hasNext = d.products.pageInfo.hasNextPage;
      pages++;
    }
  } catch (e) { log.push({ error: String(e.message || e) }); }

  // Advance the watermark: to the newest product we processed, or to "now" when
  // nothing changed (so we don't re-scan the same empty window next time).
  state.lastSweepAt = processed > 0 ? maxUpdated : new Date(started).toISOString();
  state.lastSweepRunAt = new Date().toISOString();
  state.lastSweepProcessed = processed;
  state.idsWritten = (state.idsWritten || 0) + idsWritten;
  await saveState(state);

  log.push({ swept: processed, idsWritten, colorSet, pages, caughtUp: !hasNext, watermark: state.lastSweepAt });
  return { status: "sweep complete", log, state };
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
      // Until the initial backfill finishes, keep walking the full catalog.
      // After that, switch to the recent-changes sweep to auto-cover new/edited products.
      const st  = await loadState();
      const out = st.done ? await sweepRecent() : await autoRun();
      console.log("[googleAttributes] scheduled run:", JSON.stringify(out.status));
      return { statusCode: 200, headers, body: JSON.stringify(out) };
    } catch (e) {
      return { statusCode: 500, headers, body: JSON.stringify({ ok: false, error: String(e.message || e) }) };
    }
  }

  // Browser trigger: ?run=now runs immediately. &drain=1 longer pass; &force=1
  // re-walks the whole catalog; &idsonly=1 writes only the product-id docs.
  const q = event.queryStringParameters || {};
  if (event.httpMethod === "GET" && q.run === "now") {
    try {
      const out = (q.sweep === "1")
        ? await sweepRecent({ drain: q.drain === "1" })
        : await autoRun({ drain: q.drain === "1", force: q.force === "1", idsOnly: q.idsonly === "1" });
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
        result = await autoRun({ dryRun, drain: !!b.drain, force: !!b.force, idsOnly: !!b.idsOnly, cursor: b.cursor });
        break;
      case "sweep":
        result = await sweepRecent({ drain: !!b.drain });
        break;
      case "status": {
        const state = await loadState();
        const preview = await processPage(true, state.cursor);   // dry preview of the next page
        result = { state, nextPagePreview: { products: preview.products, colorSet: preview.colorSet, colorSkipped: preview.colorSkipped, sample: preview.sample, done: preview.done } };
        break;
      }
      default:
        return { statusCode: 400, headers, body: JSON.stringify({ error: "unknown action", actions: ["run", "sweep", "status"] }) };
    }
    return { statusCode: 200, headers, body: JSON.stringify({ ok: true, action: b.action, dryRun, result }, null, 1) };
  } catch (e) {
    return { statusCode: 500, headers, body: JSON.stringify({ ok: false, error: String(e.message || e) }) };
  }
};
