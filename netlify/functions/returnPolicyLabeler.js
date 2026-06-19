// netlify/functions/returnPolicyLabeler.js
// ---------------------------------------------------------------------------
// SELF-RUNNING, idempotent tagger that stamps the Google Shopping
// return_policy_label attribute onto products that fall under your
// NON-RETURNABLE exception policy in Merchant Center.
//
// WHAT GETS LABELED — earrings only.
//   Earrings are inherently non-returnable (hygiene), so they're labeled
//   `non-returnable`, which makes Merchant Center apply the matching exception
//   return policy to them instead of the default 30-day policy.
//
// WHAT DOES NOT GET LABELED — personalized / engravable products.
//   Personalization on this store is an OPTIONAL add-on (an engraving / line-
//   item field) on products that are also sold plain. A product-level feed
//   label can't express "non-returnable only if the buyer personalized it" —
//   labeling the whole product would wrongly block returns on the plain
//   version and mismatch the product page. So personalized items intentionally
//   stay returnable in the feed; that exclusion lives in the written refund
//   policy and is enforced at return time. (If you ever add listings that are
//   inherently custom-only, tell me and I'll add a rule for those.)
//
// Detection (earring if EITHER is true):
//   • productType matches /earring/i, OR
//   • the product belongs to a collection whose handle matches /earring/i
//     (your main "earrings" collection; theme sub-collections are covered by
//     productType).
//
// Writes product-level metafield mm-google-shopping.return_policy_label — the
// same namespace the Google & YouTube channel reads (and the same pipeline as
// googleAttributes.js / setCustomLabels), so it flows to BOTH feed labels.
//
// The label VALUE must exactly match the exception-policy label you created in
// Merchant Center. Default "non-returnable"; override with GMC_RETURN_LABEL.
//
// Deploy: add to netlify.toml (a non-stacking minute, e.g. :45) and let it run;
// it paginates the catalog (cursor in Firestore Brites_Editor_Meta/
// returnPolicyLabelState), resumes each run, and no-ops once complete.
//
//   [functions."returnPolicyLabeler"]
//     schedule = "45 * * * *"
//
// HTTP (no auth; idempotent writes only):
//   GET  ...?run=now            -> one immediate run
//   GET  ...?run=now&drain=1    -> long run (for a -background copy backfill)
//   POST { action, dryRun?, drain?, force?, cursor? }
//        actions: "run" (default dryRun=true), "status"
//
// Env: SHOPIFY_STORE, SHOPIFY_CLIENT_ID, SHOPIFY_CLIENT_SECRET,
//      SHOPIFY_API_VERSION?       (default "2025-10")
//      GMC_RETURN_LABEL?          (default "non-returnable")
//      GMC_WRITE_CONCURRENCY?     (default 5)
// ---------------------------------------------------------------------------

let _fb = null;
function fb() {
  if (_fb !== null) return _fb;
  try {
    const admin = require("./firebaseAdmin");
    _fb = { admin, db: admin.firestore(), FV: admin.firestore.FieldValue };
  } catch (e) { console.error("[returnPolicyLabeler] Firebase unavailable:", e.message); _fb = false; }
  return _fb;
}

const fetch = require("node-fetch");
const API_VERSION       = process.env.SHOPIFY_API_VERSION || "2025-10";
const NS                = "mm-google-shopping";
const RETURN_LABEL      = process.env.GMC_RETURN_LABEL || "non-returnable";
const WRITE_CONCURRENCY = Number(process.env.GMC_WRITE_CONCURRENCY || 5);

/* ---- token + gql: identical pattern to googleAttributes.js / applySiteFixes.js ---- */
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

/* ================= earring detection ================= */
const EARRING_RE = /earring/i;
function isEarring(p) {
  if (EARRING_RE.test(p.productType || "")) return true;
  const cols = (p.collections && p.collections.edges) || [];
  for (const c of cols) if (EARRING_RE.test((c.node && c.node.handle) || "")) return true;
  return false;
}

/* ================= one catalog page ================= */
// products(first:15) × collections(first:20) ≈ 362 query-cost points — well
// under Shopify's 1000 cap. return_policy_label is product-level (earrings are
// non-returnable regardless of variant), so no per-variant fan-out here.
async function processPage(dryRun, cursor) {
  const d = await gql(`query($after: String) {
    products(first: 15, after: $after) {
      pageInfo { hasNextPage endCursor }
      edges { node {
        id title productType
        collections(first: 20) { edges { node { handle } } }
      } }
    } }`, { after: cursor || null });

  const edges = d.products.edges;
  const metafields = [];
  let labeled = 0;
  const sample = [];

  for (const e of edges) {
    const p = e.node;
    if (isEarring(p)) {
      metafields.push({ ownerId: p.id, namespace: NS, key: "return_policy_label",
                        type: "single_line_text_field", value: RETURN_LABEL });
      labeled++;
      if (sample.length < 5) sample.push({ title: p.title, productType: p.productType || "(none)" });
    }
  }

  if (!dryRun && metafields.length) {
    const chunks = [];
    for (let i = 0; i < metafields.length; i += 25) chunks.push(metafields.slice(i, i + 25));
    let firstError = null;
    await runWithConcurrency(chunks.map(chunk => async () => {
      if (firstError) return;
      const r = await gql(`mutation($metafields: [MetafieldsSetInput!]!) {
        metafieldsSet(metafields: $metafields) { userErrors { field message } } }`,
        { metafields: chunk });
      const ue = r.metafieldsSet.userErrors;
      if (ue.length && !firstError) firstError = ue[0].message;
    }), WRITE_CONCURRENCY);
    if (firstError) return { error: firstError, cursor };
  }

  return {
    products: edges.length, labeled, sample,
    done: !d.products.pageInfo.hasNextPage,
    cursor: d.products.pageInfo.endCursor
  };
}

/* ================= state ================= */
const STATE_DOC = "returnPolicyLabelState";
function baseState() { return { cursor: null, done: false, productsScanned: 0, labeled: 0, runs: 0 }; }
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
  const budget  = opts.drain ? 840000 : 8000;   // drain = 14 min for a -background backfill; incremental saves make any earlier timeout harmless
  const dryRun  = !!opts.dryRun;
  const state   = await loadState();
  state.runs = (state.runs || 0) + 1;
  state.lastRunAt = new Date().toISOString();
  const log = [];

  if (state.done && !opts.force) return { status: "all earrings labeled — nothing to do", state };
  if (opts.force) { state.done = false; state.cursor = opts.cursor || null; }

  let cursor = (opts.cursor !== undefined ? opts.cursor : state.cursor) || null;
  let pages = 0;
  try {
    while (Date.now() - started < budget) {
      const r = await processPage(dryRun, cursor);
      if (r.error) { log.push({ error: r.error }); break; }
      state.productsScanned = (state.productsScanned || 0) + r.products;
      state.labeled = (state.labeled || 0) + r.labeled;
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
  log.push({ pagesThisRun: pages, productsScanned: state.productsScanned, labeled: state.labeled, done: !!state.done });
  return { status: state.done ? "ALL EARRINGS LABELED" : "in progress — continues next scheduled run", log, state };
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
      console.log("[returnPolicyLabeler] scheduled run:", JSON.stringify(out.status));
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
        result = { state, nextPagePreview: { products: preview.products, labeled: preview.labeled, sample: preview.sample, done: preview.done } };
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
