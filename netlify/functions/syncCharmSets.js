// netlify/functions/syncCharmSets.js
// ---------------------------------------------------------------------------
// SELF-RUNNING charm-set relationship sync. Writes each product's curated
// "complete the set" partners to metafield brites.set (JSON), which the PDP
// bundle module reads first (falling back to live search only for products
// without curated data).
//
// The CHARM_SETS data below was generated offline from the full catalog
// export: every title was reduced to a normalized "charm signature" (form
// words, metals, and marketing words stripped), products sharing a signature
// across DIFFERENT jewelry formats become a set, and deliberate duplicate
// listings (same charm + same format, alternate primary photo / altered
// title) were collapsed to one canonical listing per format — 943 products
// across 389 charm families, 1,486 duplicates excluded. Full duplicate audit:
// data/charm_set_duplicates_report.csv in the implementation package.
//
// Schedule (netlify.toml):
//   [functions."syncCharmSets"]
//     schedule = "@hourly"
// Time-budgeted; resumes via Firestore cursor (Brites_Editor_Meta/charmSetsState)
// and goes dormant when complete. Optional HTTP: POST {action:"run"} or
// {action:"verifyVisual", handle:"..."} with X-Edit-Passcode.
//
// VISUAL VERIFICATION lives in verifyCharmSets-background.js (OpenAI vision,
// piggybacking the OPENAI_API_KEY your proxies already use). It walks each
// family's actual product photos and prunes any partner that doesn't show the
// same charm — see that file for the full pipeline.
// ---------------------------------------------------------------------------

const fetch = require("node-fetch");
let _fb = null;
function fb() {
  if (_fb !== null) return _fb;
  try { const admin = require("./firebaseAdmin");
    _fb = { admin, db: admin.firestore() };
  } catch (e) { _fb = false; }
  return _fb;
}
const API_VERSION = process.env.SHOPIFY_API_VERSION || "2025-10";
let _token = null, _tokenExp = 0;
async function getToken() {
  if (_token && Date.now() < _tokenExp - 60000) return _token;
  const res = await fetch(`https://${process.env.SHOPIFY_STORE}/admin/oauth/access_token`, {
    method: "POST", headers: { "Content-Type": "application/x-www-form-urlencoded" },
    body: new URLSearchParams({ grant_type: "client_credentials",
      client_id: process.env.SHOPIFY_CLIENT_ID, client_secret: process.env.SHOPIFY_CLIENT_SECRET })
  });
  const t = await res.text();
  if (!res.ok) throw new Error("token " + res.status + ": " + t);
  const d = JSON.parse(t); _token = d.access_token; _tokenExp = Date.now() + (d.expires_in || 86399) * 1000;
  return _token;
}
async function gql(q, v) {
  const res = await fetch(`https://${process.env.SHOPIFY_STORE}/admin/api/${API_VERSION}/graphql.json`, {
    method: "POST", headers: { "X-Shopify-Access-Token": await getToken(), "Content-Type": "application/json" },
    body: JSON.stringify({ query: q, variables: v || {} })
  });
  const d = await res.json();
  if (d.errors && d.errors.length) throw new Error(JSON.stringify(d.errors));
  return d.data;
}

const { CHARM_SETS, REMOVED_HANDLES, DATA_VERSION } = require("./charmSetsData");
const HANDLES = Object.keys(CHARM_SETS).concat(REMOVED_HANDLES || []);
// REMOVED_HANDLES get value "[]" — clearing sets the verifier disproved.

async function runBatch() {
  const started = Date.now();
  const f = fb();
  let state = { idx: 0, done: false };
  if (f) {
    try { const s = await f.db.collection("Brites_Editor_Meta").doc("charmSetsState").get();
      if (s.exists) state = s.data(); } catch (e) {}
  }
  // New verified data? Restart the walk from zero automatically.
  if (state.dataVersion !== DATA_VERSION) state = { idx: 0, done: false, dataVersion: DATA_VERSION };
  if (state.done) return { status: "charm sets fully synced — dormant", version: DATA_VERSION, written: state.idx };

  while (state.idx < HANDLES.length && Date.now() - started < 7000) {
    const batch = HANDLES.slice(state.idx, state.idx + 20);
    const q = batch.map((h, j) => `p${j}: productByHandle(handle: ${JSON.stringify(h)}) { id }`).join("\n");
    const d = await gql(`query { ${q} }`);
    const metafields = [];
    batch.forEach((h, j) => {
      if (d["p" + j]) metafields.push({
        ownerId: d["p" + j].id, namespace: "brites", key: "set",
        type: "json", value: JSON.stringify(CHARM_SETS[h] || [])
      });
    });
    if (metafields.length) {
      const r = await gql(`mutation($m: [MetafieldsSetInput!]!) {
        metafieldsSet(metafields: $m) { userErrors { field message } } }`, { m: metafields });
      const ue = r.metafieldsSet.userErrors;
      if (ue.length) throw new Error("metafieldsSet: " + ue[0].message);
    }
    state.idx += batch.length;
  }
  state.done = state.idx >= HANDLES.length;
  if (f) { try { await f.db.collection("Brites_Editor_Meta").doc("charmSetsState").set(state); } catch (e) {} }
  return { status: state.done ? "ALL CHARM SETS SYNCED" : "in progress — resumes next run",
           progress: state.idx + " / " + HANDLES.length };
}

exports.handler = async function (event) {
  const headers = { "Access-Control-Allow-Origin": "https://britesjewelry.com",
    "Access-Control-Allow-Headers": "Content-Type, X-Edit-Passcode",
    "Access-Control-Allow-Methods": "GET, POST, OPTIONS", "Content-Type": "application/json" };
  if (event.httpMethod === "OPTIONS") return { statusCode: 204, headers };
  const scheduled = !!(event.headers && (event.headers["x-nf-event"] === "schedule" || event.isScheduled));
  if (!scheduled) {
    const pass = (event.headers && (event.headers["x-edit-passcode"] || event.headers["X-Edit-Passcode"])) || "";
    if (pass !== process.env.EDIT_PASSCODE) return { statusCode: 401, headers, body: JSON.stringify({ error: "bad passcode" }) };
  }
  try {
    let result;
    const b = event.httpMethod === "POST" ? JSON.parse(event.body || "{}") : {};
    result = await runBatch();
    return { statusCode: 200, headers, body: JSON.stringify({ ok: true, result }, null, 1) };
  } catch (e) {
    return { statusCode: 500, headers, body: JSON.stringify({ ok: false, error: String(e.message || e) }) };
  }
};
