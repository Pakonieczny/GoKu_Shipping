// netlify/functions/googleAdsAutopilot.js
// ─────────────────────────────────────────────────────────────────────────────
// Brites "Ad Autopilot" — shared engine library (require()-able, NOT scheduled).
//
// This is a logic module in the same spirit as charmSetsData.js / firebaseAdmin.js:
// the thin Netlify entrypoints (googleAdsAutopilotKick.js, *-background.js) require()
// this file and call into it. Keeping all Google Ads logic here means the cron
// entry and the 15-min worker stay tiny and the whole engine lives in one place.
//
// REUSES existing infra — adds no parallel stack:
//   • require("./firebaseAdmin")      → Firestore state/ledger/queues (same as every fn)
//   • require("node-fetch")           → same HTTP client as the rest of the repo
//   • OpenAI direct call              → identical shape to verifyCharmSets-background.js
//                                        (OPENAI_API_KEY, gpt-5/o-series param branch)
//   • EDIT_PASSCODE / URL / SITE_NAME → same env conventions
//
// NEW because nothing to append to: the repo's "Google" code (googleAttributes.js)
// only writes mm-google-shopping *metafields* for the feed app — there is no Google
// Ads API client, OAuth, or developer-token path anywhere. That surface is built here.
//
// Google Ads API: REST, v24 by default (v20 sunset 2026-06-10; v21→Aug, v22→Oct).
// GADS_API_VERSION makes the version a one-line env bump, not a code change.
//
// Safety model (see SPEC): global kill switch + hard spend ceiling + anomaly
// circuit-breaker + approval queue + dry-run/validateOnly. Mutations only ever
// leave this module through mutate()/mutateAll()/uploadConversions(), which all
// honour control().dryRun and the spend ceiling.
// ─────────────────────────────────────────────────────────────────────────────

const fetch = require("node-fetch");

/* ============================ Firebase (shared) ============================ */
let _fb = null;
function fb() {
  if (_fb !== null) return _fb;
  try {
    const admin = require("./firebaseAdmin");
    _fb = { admin, db: admin.firestore(), FV: admin.firestore.FieldValue };
  } catch (e) {
    console.error("[gads] Firebase unavailable:", e && e.message);
    _fb = false;
  }
  return _fb;
}

const COL = {
  control:   "Brites_GAds_Control",     // doc "control": enabled, dryRun, caps, autoApprove
  state:     "Brites_GAds_State",        // doc "cycle": cursors, lastRun timestamps
  metrics:   "Brites_GAds_Metrics",      // daily snapshots (time-series, auto-id)
  ledger:    "Brites_GAds_Ledger",       // every mutation, with experiment ids (auto-id)
  approvals: "Brites_GAds_Approvals",    // pending creative/budget ops (auto-id)
  calendar:  "Brites_GAds_Calendar",     // event×collection config (doc per collection)
  occasions: "Brites_GAds_Occasions",    // per-occasion memory (uses + attributed performance)
  convQueue: "Brites_GAds_ConvQueue",    // offline conversions waiting for upload (auto-id)
  convAdj:   "Brites_GAds_ConvAdjQueue", // conversion adjustments (refunds/retractions) waiting for upload (auto-id)
  orderLog:  "Brites_GAds_OrderLog",     // EVERY Shopify order outcome (captured/skipped) + attribution, for the log + organic intelligence (auto-id)
  kwCache:   "Brites_GAds_KwCache"       // cached Keyword Planner results keyed by seed-set+geo (TTL), to spare API quota
};

/* ============================ Config / control ============================ */
const ENV = process.env;
const V          = ENV.GADS_API_VERSION || "v24";
const BASE       = `https://googleads.googleapis.com/${V}`;
const CID        = (ENV.GADS_CUSTOMER_ID || "").replace(/\D/g, "");        // Brites account
const LOGIN_CID  = (ENV.GADS_LOGIN_CUSTOMER_ID || CID).replace(/\D/g, ""); // manager (MCC)
const DEV_TOKEN  = ENV.GADS_DEVELOPER_TOKEN || "";
const GEN_MODEL  = ENV.GADS_GEN_MODEL || "gpt-5.5";                       // text generation
const CURRENCY   = ENV.GADS_CURRENCY || "USD";

// The store's nine homepage collections (handle ↔ title) — drives the Draft Bench picker.
const COLLECTIONS = [
  { handle: "gifts-for-teachers", title: "Teachers" },
  { handle: "gifts-for-nurses-doctors", title: "Nurses & Doctors" },
  { handle: "animal-lovers", title: "Animal Lovers" },
  { handle: "bird-lovers", title: "Bird Lovers" },
  { handle: "beach-ocean", title: "Beach & Ocean" },
  { handle: "cop", title: "Sports & Athletics" },
  { handle: "floral-flower-lovers", title: "Floral & Flower Lovers" },
  { handle: "celestial", title: "Celestial" },
  { handle: "bar-engraved", title: "Personalized" }
];
const OCCASIONS = [
  "Evergreen gifting", "Teacher Appreciation Week", "Nurses Week", "Mother's Day", "Father's Day",
  "Graduation", "Back to School", "Valentine's Day", "Christmas", "Memorial / Sympathy", "Birthday / Zodiac"
];

// Control defaults; Firestore Brites_GAds_Control/control overrides these live.
const DEFAULT_CONTROL = {
  enabled: false,                 // master switch — OFF until you turn it on
  dryRun: true,                   // compute + queue + validateOnly, never apply — ON until proven
  maxDailyBudgetTotal: Number(ENV.GADS_MAX_DAILY_BUDGET_TOTAL || 100), // hard ceiling, account ccy
  maxBudgetStepPct: 20,           // largest single budget move per cycle
  budgetMoveApprovalPct: 20,      // budget moves above this % need human approval
  autoApproveVettedTemplates: true,
  targetRoas: Number(ENV.GADS_TARGET_ROAS || 0),  // 0 = don't auto-tune tROAS
  smartBidding: Number(ENV.GADS_TARGET_ROAS || 0) > 0,  // false = Manual CPC (capped) · true = Smart Bidding (Max Conversion Value, no CPC cap)
  minConvForTargetTune: 30,       // Smart Bidding volume floor before nudging targets
  learningCooldownDays: 7,        // don't restructure a campaign changed within N days
  anomalySpendMultiple: 2.5       // yesterday spend > N× trailing avg ⇒ trip breaker
};

async function control() {
  const f = fb();
  let c = { ...DEFAULT_CONTROL };
  if (f) {
    try {
      const s = await f.db.collection(COL.control).doc("control").get();
      if (s.exists) c = { ...c, ...s.data() };
    } catch (e) {}
  }
  // env hard ceiling always wins as an upper bound even if Firestore says higher
  c.maxDailyBudgetTotal = Math.min(c.maxDailyBudgetTotal, Number(ENV.GADS_MAX_DAILY_BUDGET_TOTAL || c.maxDailyBudgetTotal));
  return c;
}

/* ============================ OAuth token (cached) ============================ */
let _tok = null, _tokExp = 0;
async function mintToken() {
  if (_tok && Date.now() < _tokExp - 60000) return _tok;
  const res = await fetch("https://oauth2.googleapis.com/token", {
    method: "POST",
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
    body: new URLSearchParams({
      client_id: ENV.GADS_CLIENT_ID || "",
      client_secret: ENV.GADS_CLIENT_SECRET || "",
      refresh_token: ENV.GADS_REFRESH_TOKEN || "",
      grant_type: "refresh_token"
    })
  });
  const data = await res.json().catch(() => ({}));
  if (!res.ok || !data.access_token) {
    throw new Error("[gads] OAuth token error: " + (data.error_description || data.error || res.status));
  }
  _tok = data.access_token;
  _tokExp = Date.now() + (data.expires_in || 3600) * 1000;
  return _tok;
}

function adsHeaders(token, loginCidOverride) {
  const h = {
    "Authorization": "Bearer " + token,
    "developer-token": DEV_TOKEN,
    "Content-Type": "application/json"
  };
  // Default: manager (MCC) login-customer-id. Override with a specific CID, or `false` to omit the
  // header entirely (used by the Keyword Planner auth-path fallback).
  const lc = (loginCidOverride === undefined) ? LOGIN_CID : loginCidOverride;
  if (lc) h["login-customer-id"] = String(lc).replace(/\D/g, "");
  return h;
}

/* ============================ GAQL read (paged) ============================ */
async function gaql(query) {
  const token = await mintToken();
  const out = [];
  let pageToken = undefined;
  do {
    const res = await fetch(`${BASE}/customers/${CID}/googleAds:search`, {
      method: "POST",
      headers: adsHeaders(token),
      body: JSON.stringify(pageToken ? { query, pageToken } : { query })
    });
    const data = await res.json().catch(() => ({}));
    if (!res.ok) throw new Error("[gads] search failed: " + JSON.stringify(data).slice(0, 600));
    (data.results || []).forEach(r => out.push(r));
    pageToken = data.nextPageToken;
  } while (pageToken);
  return out;
}

/* ============================ Mutations (gated) ============================ */
// Per-service mutate, e.g. service="campaignBudgets", "adGroupCriteria", "campaigns".
async function mutate(service, operations, { ctrl, label = "", validateOnly = null } = {}) {
  ctrl = ctrl || (await control());
  const vo = validateOnly == null ? !!ctrl.dryRun : validateOnly;
  const token = await mintToken();
  const body = { operations, partialFailure: true };
  if (vo) body.validateOnly = true;
  const res = await fetch(`${BASE}/customers/${CID}/${service}:mutate`, {
    method: "POST", headers: adsHeaders(token), body: JSON.stringify(body)
  });
  const data = await res.json().catch(() => ({}));
  await ledger({ kind: "mutate", service, label, validateOnly: vo, count: operations.length,
                 ok: res.ok, error: res.ok ? null : JSON.stringify(data).slice(0, 800),
                 partialFailure: data.partialFailureError || null });
  if (!res.ok) throw new Error(`[gads] ${service}:mutate failed: ` + JSON.stringify(data).slice(0, 600));
  return data;
}

// Atomic cross-resource mutate (build a whole campaign in one transaction with
// temp resource names). operations = [{ campaignBudgetOperation:{...} }, ...].
async function mutateAll(mutateOperations, { ctrl, label = "", validateOnly = null } = {}) {
  ctrl = ctrl || (await control());
  const vo = validateOnly == null ? !!ctrl.dryRun : validateOnly;
  const token = await mintToken();
  const body = { mutateOperations, partialFailure: false };
  if (vo) body.validateOnly = true;
  const res = await fetch(`${BASE}/customers/${CID}/googleAds:mutate`, {
    method: "POST", headers: adsHeaders(token), body: JSON.stringify(body)
  });
  const data = await res.json().catch(() => ({}));
  await ledger({ kind: "mutateAll", label, validateOnly: vo, count: mutateOperations.length,
                 ok: res.ok, error: res.ok ? null : JSON.stringify(data).slice(0, 800) });
  if (!res.ok) throw new Error("[gads] googleAds:mutate failed: " + JSON.stringify(data).slice(0, 600));
  return data;
}

/* ============================ Offline conversions ============================ */
// Producer contract: anything (a Shopify order webhook, or your existing order
// pipeline) calls enqueueConversion(...) to drop a row in Brites_GAds_ConvQueue.
// This engine drains it into Google so Smart Bidding optimises on REAL revenue.
async function enqueueConversion({ gclid, gbraid, wbraid, value, currency, orderId, conversionDateTime }) {
  const f = fb(); if (!f) return false;
  if (!gclid && !gbraid && !wbraid) return false; // no click id ⇒ unattributable
  if (orderId) { // dedup: Shopify retries webhooks; one conversion per order
    try { const ex = await f.db.collection(COL.convQueue).where("orderId", "==", orderId).limit(1).get(); if (!ex.empty) return { duplicate: true, orderId }; } catch (e) {}
  }
  await f.db.collection(COL.convQueue).add({
    gclid: gclid || null, gbraid: gbraid || null, wbraid: wbraid || null,
    value: Number(value) || 0, currency: currency || CURRENCY,
    orderId: orderId || null, refundedTotal: 0,
    conversionDateTime: conversionDateTime || gAdsTime(new Date()),
    uploaded: false, createdAt: f.FV.serverTimestamp()
  });
  return { enqueued: true, orderId: orderId || null };
}

async function uploadConversions({ ctrl, limit = 500 } = {}) {
  ctrl = ctrl || (await control());
  const f = fb(); if (!f) return { uploaded: 0 };
  const action = ENV.GADS_CONVERSION_ACTION; // customers/CID/conversionActions/NNN
  if (!action) return { uploaded: 0, skipped: "GADS_CONVERSION_ACTION not set" };
  const snap = await f.db.collection(COL.convQueue).where("uploaded", "==", false).limit(limit).get();
  if (snap.empty) return { uploaded: 0 };
  const docs = []; const conversions = [];
  snap.forEach(d => {
    const x = d.data(); docs.push(d.ref);
    const c = { conversionAction: action, conversionDateTime: x.conversionDateTime,
                conversionValue: x.value, currencyCode: x.currency, orderId: x.orderId || undefined };
    if (x.gclid) c.gclid = x.gclid; else if (x.gbraid) c.gbraid = x.gbraid; else if (x.wbraid) c.wbraid = x.wbraid;
    conversions.push(c);
  });
  const token = await mintToken();
  const body = { conversions, partialFailure: true };
  if (ctrl.dryRun) body.validateOnly = true;
  const res = await fetch(`${BASE}/customers/${CID}:uploadClickConversions`, {
    method: "POST", headers: adsHeaders(token), body: JSON.stringify(body)
  });
  const data = await res.json().catch(() => ({}));
  await ledger({ kind: "uploadConversions", count: conversions.length, validateOnly: !!ctrl.dryRun,
                 ok: res.ok, error: res.ok ? null : JSON.stringify(data).slice(0, 600),
                 partialFailure: data.partialFailureError || null });
  if (res.ok && !ctrl.dryRun) {
    const batch = f.db.batch();
    docs.forEach(ref => batch.update(ref, { uploaded: true, uploadedAt: f.FV.serverTimestamp() }));
    await batch.commit();
  }
  return { uploaded: res.ok && !ctrl.dryRun ? conversions.length : 0, validateOnly: !!ctrl.dryRun };
}

/* ---- Conversion adjustments (refunds → retraction / restatement) ---- */
// Shopify refunds/create → recordRefund() looks up the original conversion (by orderId),
// tracks cumulative refund, and queues a RETRACTION (fully refunded) or RESTATEMENT
// (partial — new net value). Keeps Google Ads ROAS honest so Smart Bidding and the
// recommendation engine don't optimize toward revenue that was handed back.
async function enqueueConversionAdjustment({ orderId, gclid, adjustmentType, restatementValue, currency, adjustmentDateTime }) {
  const f = fb(); if (!f) return false;
  if (!orderId && !gclid) return false;
  await f.db.collection(COL.convAdj).add({
    orderId: orderId || null, gclid: gclid || null,
    adjustmentType: adjustmentType || "RETRACTION",
    restatementValue: restatementValue != null ? Number(restatementValue) : null,
    currency: currency || CURRENCY,
    adjustmentDateTime: adjustmentDateTime || gAdsTime(new Date()),
    uploaded: false, createdAt: f.FV.serverTimestamp()
  });
  return true;
}

async function recordRefund({ orderId, refundAmount, when }) {
  const f = fb(); if (!f || !orderId) return { ok: false, reason: "no orderId" };
  let orig = null;
  try { const q = await f.db.collection(COL.convQueue).where("orderId", "==", orderId).limit(1).get(); q.forEach(d => { orig = Object.assign({ ref: d.ref }, d.data()); }); } catch (e) {}
  if (!orig) return { ok: true, skipped: "no matching ad-attributed conversion for this order" };
  const refundedSoFar = (Number(orig.refundedTotal) || 0) + (Number(refundAmount) || 0);
  const newValue = Math.max(0, (Number(orig.value) || 0) - refundedSoFar);
  const full = newValue <= 0.005;
  await enqueueConversionAdjustment({
    orderId, gclid: orig.gclid || null,
    adjustmentType: full ? "RETRACTION" : "RESTATEMENT",
    restatementValue: full ? null : newValue,
    currency: orig.currency, adjustmentDateTime: when || gAdsTime(new Date())
  });
  try { await orig.ref.update({ refundedTotal: refundedSoFar }); } catch (e) {}
  return { ok: true, adjustmentType: full ? "RETRACTION" : "RESTATEMENT", newValue };
}

async function uploadConversionAdjustments({ ctrl, limit = 500 } = {}) {
  ctrl = ctrl || (await control());
  const f = fb(); if (!f) return { uploaded: 0 };
  const action = ENV.GADS_CONVERSION_ACTION;
  if (!action) return { uploaded: 0, skipped: "GADS_CONVERSION_ACTION not set" };
  const snap = await f.db.collection(COL.convAdj).where("uploaded", "==", false).limit(limit).get();
  if (snap.empty) return { uploaded: 0 };
  const docs = []; const adjustments = [];
  snap.forEach(d => {
    const x = d.data(); docs.push(d.ref);
    const a = { conversionAction: action, adjustmentType: x.adjustmentType, adjustmentDateTime: x.adjustmentDateTime, orderId: x.orderId || undefined };
    if (!x.orderId && x.gclid) a.gclidDateTimePair = { gclid: x.gclid, conversionDateTime: x.adjustmentDateTime };
    if (x.adjustmentType === "RESTATEMENT" && x.restatementValue != null) a.restatementValue = { adjustedValue: x.restatementValue, currencyCode: x.currency };
    adjustments.push(a);
  });
  const token = await mintToken();
  const body = { conversionAdjustments: adjustments, partialFailure: true };
  if (ctrl.dryRun) body.validateOnly = true;
  const res = await fetch(`${BASE}/customers/${CID}:uploadConversionAdjustments`, { method: "POST", headers: adsHeaders(token), body: JSON.stringify(body) });
  const data = await res.json().catch(() => ({}));
  await ledger({ kind: "uploadConversionAdjustments", count: adjustments.length, validateOnly: !!ctrl.dryRun, ok: res.ok, error: res.ok ? null : JSON.stringify(data).slice(0, 600), partialFailure: data.partialFailureError || null });
  if (res.ok && !ctrl.dryRun) { const batch = f.db.batch(); docs.forEach(ref => batch.update(ref, { uploaded: true, uploadedAt: f.FV.serverTimestamp() })); await batch.commit(); }
  return { uploaded: res.ok && !ctrl.dryRun ? adjustments.length : 0, validateOnly: !!ctrl.dryRun };
}

/* ---- Conversion-tracking health (the 3-way connection's vital sign) ---- */
// Verifies the Shopify → Google Ads → app loop is actually live: account tracking
// status, enabled conversion actions, recent recorded conversions, upload-queue depth,
// and last upload. `validated` gates whether recommendations may trust ROAS history.
async function conversionHealth({ force } = {}) {
  const f = fb();
  if (f && !force) {
    try { const s = await f.db.collection(COL.state).doc("conv_health").get(); if (s.exists) { const x = s.data(); if (x.at && (Date.now() - x.at) < 15 * 60 * 1000 && x.data) return x.data; } } catch (e) {}
  }
  const out = { status: "UNKNOWN", actionConfigured: !!ENV.GADS_CONVERSION_ACTION, actionId: ENV.GADS_CONVERSION_ACTION || null,
    actions: [], recentConversions: null, queueDepth: null, adjQueueDepth: null, lastUpload: null,
    healthy: false, validated: false, reasons: [], at: Date.now() };
  try {
    const r = await gaql(`SELECT customer.conversion_tracking_setting.conversion_tracking_status FROM customer`);
    const cs = r[0] && r[0].customer && r[0].customer.conversionTrackingSetting;
    if (cs && cs.conversionTrackingStatus) out.status = cs.conversionTrackingStatus;
  } catch (e) { out.reasons.push("status check failed: " + String(e.message).slice(0, 70)); }
  try {
    const rows = await gaql(`SELECT conversion_action.id, conversion_action.name, conversion_action.status, conversion_action.type, conversion_action.category FROM conversion_action`);
    out.actions = rows.map(r => ({ id: String(r.conversionAction.id), name: r.conversionAction.name, status: r.conversionAction.status, type: r.conversionAction.type, category: r.conversionAction.category }));
  } catch (e) { out.reasons.push("conversion-action list failed: " + String(e.message).slice(0, 70)); }
  try {
    const tz = await _accountTz();
    const end = _acctDateYmd(tz, 0), start = _acctDateYmd(tz, -29 * 86400000);
    const r = await gaql(`SELECT metrics.conversions, metrics.all_conversions FROM customer WHERE segments.date BETWEEN '${start}' AND '${end}'`);
    out.recentConversions = r[0] && r[0].metrics ? Number(r[0].metrics.conversions || 0) : 0;
  } catch (e) {}
  if (f) {
    try { const q = await f.db.collection(COL.convQueue).where("uploaded", "==", false).limit(500).get(); out.queueDepth = q.size; } catch (e) {}
    try { const q = await f.db.collection(COL.convAdj).where("uploaded", "==", false).limit(500).get(); out.adjQueueDepth = q.size; } catch (e) {}
    try { const lg = await f.db.collection(COL.ledger).orderBy("at", "desc").limit(50).get(); let found = null; lg.forEach(d => { const x = d.data(); if (!found && x.kind === "uploadConversions") found = { at: x.at && x.at.toMillis ? x.at.toMillis() : null, count: x.count, ok: x.ok }; }); out.lastUpload = found; } catch (e) {}
  }
  const enabledAction = out.actions.some(a => a.status === "ENABLED");
  out.healthy = !!(out.actionConfigured && enabledAction && out.status && out.status !== "NOT_CONVERSION_TRACKED" && out.status !== "UNKNOWN");
  out.validated = !!(out.healthy && Number(out.recentConversions) > 0);
  if (!out.actionConfigured) out.reasons.push("GADS_CONVERSION_ACTION env var is not set");
  if (!enabledAction && out.actions.length === 0) out.reasons.push("no conversion actions found in the Google Ads account");
  else if (!enabledAction) out.reasons.push("no ENABLED conversion action (create/enable an Import 'from clicks' action)");
  if (out.status === "NOT_CONVERSION_TRACKED") out.reasons.push("account status is NOT_CONVERSION_TRACKED");
  if (out.healthy && Number(out.recentConversions) === 0) out.reasons.push("tracking is configured but no conversions recorded in 30d yet");
  if (f) { try { await f.db.collection(COL.state).doc("conv_health").set({ data: out, at: Date.now() }); } catch (e) {} }
  return out;
}

/* ===================== Store order log + organic intelligence ===================== */
// Every Shopify order (ad-attributed or not) is logged here. Orders that DIDN'T come from a
// Google ad click still teach us what's selling — we mine that to inform future ad campaigns.

async function recordOrderEvent(ev) {
  const f = fb(); if (!f) return false;
  const items = Array.isArray(ev.items)
    ? ev.items.map(it => ({ title: String(it.title || "").slice(0, 160), qty: Number(it.qty) || 1 })).filter(it => it.title).slice(0, 25)
    : (Array.isArray(ev.products) ? ev.products.map(t => ({ title: String(t).slice(0, 160), qty: 1 })).filter(it => it.title).slice(0, 25) : []);
  const itemCount = ev.itemCount != null ? (Number(ev.itemCount) || 0) : items.reduce((a, b) => a + (b.qty || 1), 0);
  const row = {
    orderId: ev.orderId || null, value: Number(ev.value) || 0, currency: ev.currency || CURRENCY,
    source: ev.source || null, medium: ev.medium || null, campaign: ev.campaign || null,
    hasClickId: !!ev.gclid, captured: !!ev.captured, reason: ev.reason || null,
    items, itemCount, products: items.map(i => i.title),
    handle: ev.handle || null, at: f.FV.serverTimestamp(), ts: ev.ts || Date.now()
  };
  try { await f.db.collection(COL.orderLog).add(row); return true; } catch (e) { return false; }
}

// Most recent order outcomes for the console log.
async function recentOrders({ limit = 25 } = {}) {
  const f = fb(); if (!f) return [];
  try {
    const q = await f.db.collection(COL.orderLog).orderBy("ts", "desc").limit(Math.min(250, limit)).get();
    const out = []; q.forEach(d => { const x = d.data(); out.push({ id: d.id, orderId: x.orderId, value: x.value, currency: x.currency, source: x.source, medium: x.medium, campaign: x.campaign, captured: x.captured, hasClickId: x.hasClickId, reason: x.reason, items: x.items || ((x.products || []).map(t => ({ title: t, qty: 1 }))), itemCount: x.itemCount != null ? x.itemCount : ((x.products || []).length), handle: x.handle, ts: x.ts }); });
    return out;
  } catch (e) { return []; }
}

// Aggregate organic (non-ad) demand from the order log: revenue split, top products, top sources.
async function storeSignals({ days = 30, max = 500 } = {}) {
  const f = fb(); if (!f) return null;
  const since = Date.now() - days * 86400000;
  let rows = [];
  try {
    const q = await f.db.collection(COL.orderLog).orderBy("ts", "desc").limit(max).get();
    q.forEach(d => { const x = d.data(); if ((x.ts || 0) >= since) rows.push(x); });
  } catch (e) { return null; }
  const out = { days, orders: rows.length, adOrders: 0, organicOrders: 0, adRevenue: 0, organicRevenue: 0,
    topProducts: [], topSources: [] };
  const prod = {}, src = {};
  rows.forEach(x => {
    const v = Number(x.value) || 0; const isAd = !!x.hasClickId;
    if (isAd) { out.adOrders++; out.adRevenue += v; } else { out.organicOrders++; out.organicRevenue += v; }
    (x.products || []).forEach(p => { const k = String(p).trim(); if (!k) return; (prod[k] = prod[k] || { name: k, orders: 0, revenue: 0, ad: 0, organic: 0 }); prod[k].orders++; prod[k].revenue += v / Math.max(1, (x.products || []).length); isAd ? prod[k].ad++ : prod[k].organic++; });
    const sk = ((x.source || "direct") + " / " + (x.medium || "none")).toLowerCase();
    (src[sk] = src[sk] || { source: sk, orders: 0, revenue: 0 }); src[sk].orders++; src[sk].revenue += v;
  });
  out.adRevenue = +out.adRevenue.toFixed(2); out.organicRevenue = +out.organicRevenue.toFixed(2);
  out.topProducts = Object.values(prod).map(p => ({ ...p, revenue: +p.revenue.toFixed(2) })).sort((a, b) => b.orders - a.orders || b.revenue - a.revenue).slice(0, 10);
  out.topSources = Object.values(src).map(s => ({ ...s, revenue: +s.revenue.toFixed(2) })).sort((a, b) => b.orders - a.orders).slice(0, 8);
  return out;
}

// One-time / on-demand backfill: pull the most recent Shopify orders into the order log so the
// intelligence panel is populated immediately, without waiting for new webhook orders. Records
// to the log ONLY (never re-uploads conversions to Google Ads — that would risk double-counting
// stale/organic data). Idempotent: orders already in the log are skipped.
async function backfillOrders({ limit = 100 } = {}) {
  const f = fb(); if (!f) throw new Error("no firestore");
  const want = Math.min(250, Math.max(1, limit | 0));
  const FULL = `{ orders(first: ${want}, sortKey: CREATED_AT, reverse: true) { edges { node {
      id name createdAt
      totalPriceSet { shopMoney { amount currencyCode } }
      customAttributes { key value }
      customerJourneySummary { firstVisit { landingPage utmParameters { source medium campaign } } }
      lineItems(first: 25) { edges { node { title quantity } } } } } } }`;
  const MIN = `{ orders(first: ${want}, sortKey: CREATED_AT, reverse: true) { edges { node {
      id name createdAt
      totalPriceSet { shopMoney { amount currencyCode } }
      customAttributes { key value }
      lineItems(first: 25) { edges { node { title quantity } } } } } } }`;
  let d;
  try { d = await shopifyGql(FULL); }
  catch (e) { d = await shopifyGql(MIN); } // customer-journey field/scope unavailable → still backfill
  const edges = (d && d.orders && d.orders.edges) || [];

  const existing = new Set();
  try { const q = await f.db.collection(COL.orderLog).get(); q.forEach(x => { const o = x.data(); if (o.orderId) existing.add(String(o.orderId)); }); } catch (e) {}

  const rows = [];
  edges.forEach(e => {
    const n = (e && e.node) || {};
    const orderId = String(n.name || n.id || "").replace(/^gid:\/\/shopify\/Order\//, "");
    if (!orderId || existing.has(orderId)) return;
    existing.add(orderId);
    const money = n.totalPriceSet && n.totalPriceSet.shopMoney;
    const value = Number(money && money.amount) || 0;
    const currency = (money && money.currencyCode) || CURRENCY;
    const attrs = n.customAttributes || [];
    const ga = k => { const m = attrs.find(a => String(a.key || "").toLowerCase() === k); return (m && m.value) || null; };
    let gclid = ga("gclid"), gbraid = ga("gbraid"), wbraid = ga("wbraid");
    const fv = (n.customerJourneySummary && n.customerJourneySummary.firstVisit) || {};
    let source = (fv.utmParameters && fv.utmParameters.source) || ga("utm_source");
    let medium = (fv.utmParameters && fv.utmParameters.medium) || ga("utm_medium");
    let campaign = (fv.utmParameters && fv.utmParameters.campaign) || ga("utm_campaign");
    let handle = null;
    if (fv.landingPage) { try { const u = new URL(fv.landingPage, "https://x.invalid");
      gclid = gclid || u.searchParams.get("gclid"); gbraid = gbraid || u.searchParams.get("gbraid"); wbraid = wbraid || u.searchParams.get("wbraid");
      source = source || u.searchParams.get("utm_source"); medium = medium || u.searchParams.get("utm_medium"); campaign = campaign || u.searchParams.get("utm_campaign");
      const mm = (u.pathname || "").match(/\/products\/([^\/?#]+)/); if (mm) handle = mm[1];
    } catch (x) {} }
    const items = (((n.lineItems && n.lineItems.edges) || []).map(li => ({ title: (li.node && li.node.title) || "", qty: Number(li.node && li.node.quantity) || 1 })).filter(it => it.title)).slice(0, 25);
    const itemCount = items.reduce((a, b) => a + (b.qty || 1), 0);
    const clickId = gclid || gbraid || wbraid || null;
    const captured = !!clickId;
    const reason = captured ? "captured — Google ad click (backfill)"
      : (campaign === "sag_organic" ? "organic — free Google listing (sag_organic)"
         : (source ? `non-ad — ${source}/${medium || "none"}` : "organic / no Google click id"));
    rows.push({ orderId, value, currency, source: source || null, medium: medium || null, campaign: campaign || null,
      hasClickId: captured, captured, reason, items, itemCount, products: items.map(i => i.title), handle: handle || null,
      ts: n.createdAt ? Date.parse(n.createdAt) : Date.now(), backfill: true });
  });

  let added = 0;
  for (let i = 0; i < rows.length; i += 400) {
    const batch = f.db.batch();
    rows.slice(i, i + 400).forEach(r => { const ref = f.db.collection(COL.orderLog).doc(); batch.set(ref, Object.assign({ at: f.FV.serverTimestamp() }, r)); added++; });
    await batch.commit();
  }
  return { ok: true, fetched: edges.length, added, skipped: edges.length - added };
}

async function clearOrderLog({ keep = 1000 } = {}) {
  const f = fb(); if (!f) return { deleted: 0 };
  try {
    const q = await f.db.collection(COL.orderLog).orderBy("ts", "desc").get();
    const docs = q.docs || []; let deleted = 0;
    for (let i = keep; i < docs.length; i += 400) {
      const batch = f.db.batch(); docs.slice(i, i + 400).forEach(d => { batch.delete(d.ref); deleted++; });
      await batch.commit();
    }
    return { deleted };
  } catch (e) { return { deleted: 0, error: e.message }; }
}

/* ============================ Ledger / approvals ============================ */
async function ledger(entry) {
  const f = fb(); if (!f) return;
  try { await f.db.collection(COL.ledger).add({ ...entry, at: f.FV.serverTimestamp() }); } catch (e) {}
}

// Clear the activity ledger. With {keep:N}, deletes all but the N most-recent
// entries (used for automatic bounding); with no args, deletes everything.
async function clearLedger({ keep } = {}) {
  const f = fb(); if (!f) return { ok: false, deleted: 0, error: "no firestore" };
  let deleted = 0, kept = 0;
  try {
    const col = f.db.collection(COL.ledger);
    const snap = (keep && keep > 0) ? await col.orderBy("at", "desc").get() : await col.get();
    const docs = []; snap.forEach(d => docs.push(d));
    let batch = f.db.batch(), n = 0;
    for (let i = 0; i < docs.length; i++) {
      if (keep && keep > 0 && i < keep) { kept++; continue; }
      batch.delete(docs[i].ref); n++; deleted++;
      if (n >= 400) { await batch.commit(); batch = f.db.batch(); n = 0; }
    }
    if (n > 0) await batch.commit();
    return { ok: true, deleted, kept };
  } catch (e) { return { ok: false, deleted, error: e.message }; }
}

async function enqueueApproval(item) {
  // item: { type:'creative'|'budget'|'negatives'|'keywords'|'pmax', summary, payload, experimentId, vetted }
  const f = fb(); if (!f) return null;
  const ref = await f.db.collection(COL.approvals).add({
    ...item, status: "PENDING", createdAt: f.FV.serverTimestamp()
  });
  return ref.id;
}

// Apply one approved queue item by replaying its stored payload through the right mutate.
// Defensive heal for payloads frozen before the text_guidelines fix: the API rejects
// Campaign.text_guidelines (free-text messaging_restrictions). Strip it from any campaign
// create/update op so drafts approved under the old builder can apply after the fix.
function sanitizeOps(ops) {
  if (!Array.isArray(ops)) return ops;
  ops.forEach(op => {
    if (!op) return;
    const c = (op.campaignOperation && (op.campaignOperation.create || op.campaignOperation.update)) ||
              op.create || op.update;
    if (c && typeof c === "object") {
      delete c.text_guidelines; delete c.textGuidelines;
      // Legacy schedule fields: Campaign uses startDateTime/endDateTime ("yyyyMMdd HH:MM:SS"),
      // not startDate/endDate. Migrate any draft queued before this fix so it applies cleanly.
      if (c.startDate != null) { const v = _toGAdsDateTime(c.startDate, "00:00:00"); if (v) c.startDateTime = v; delete c.startDate; }
      if (c.endDate != null)   { const v = _toGAdsDateTime(c.endDate, "23:59:59"); if (v) c.endDateTime = v; delete c.endDate; }
      if (c.start_date != null){ const v = _toGAdsDateTime(c.start_date, "00:00:00"); if (v && !c.startDateTime) c.startDateTime = v; delete c.start_date; }
      if (c.end_date != null)  { const v = _toGAdsDateTime(c.end_date, "23:59:59"); if (v && !c.endDateTime) c.endDateTime = v; delete c.end_date; }
    }
  });
  return ops;
}
async function applyApproval(id, ctrl) {
  ctrl = ctrl || (await control());
  const f = fb(); if (!f) throw new Error("no firestore");
  const ref = f.db.collection(COL.approvals).doc(id);
  const snap = await ref.get();
  if (!snap.exists) throw new Error("approval not found");
  const it = snap.data();
  if (it.status !== "APPROVED") throw new Error("approval not in APPROVED state");
  const p = it.payload || {};
  const vo = !!ctrl.dryRun;
  // Creation-time ceiling guard: a new-campaign draft must fit under the daily-budget ceiling.
  // Trim its budget to the remaining headroom (vs enabled campaigns), or refuse if there's none.
  let _ctrim = null;
  if (p.mutateOperations && Number(ctrl.maxDailyBudgetTotal) > 0) {
    const bOp = p.mutateOperations.find(o => o && o.campaignBudgetOperation && o.campaignBudgetOperation.create && o.campaignBudgetOperation.create.amountMicros != null);
    if (bOp) {
      const want = fromMicros(bOp.campaignBudgetOperation.create.amountMicros);
      const current = await _enabledBudgetTotal();
      const headroom = Number(ctrl.maxDailyBudgetTotal) - current;
      if (want > headroom + 0.001) {
        if (headroom >= 1) {
          const trimmed = +headroom.toFixed(2);
          bOp.campaignBudgetOperation.create.amountMicros = micros(trimmed);
          _ctrim = { from: want, to: trimmed, ceiling: Number(ctrl.maxDailyBudgetTotal), current: +current.toFixed(2) };
        } else {
          throw new Error(`Can't launch under your ceiling: enabled campaigns already use ${CURRENCY}${current.toFixed(2)}/day of your ${CURRENCY}${Number(ctrl.maxDailyBudgetTotal).toFixed(2)}/day cap. Raise the ceiling (Controls) or pause/trim a campaign first.`);
        }
      }
    }
  }
  if (p.service && p.operations) await mutate(p.service, sanitizeOps(p.operations), { ctrl, label: "approval:" + it.type });
  else if (p.mutateOperations)   await mutateAll(sanitizeOps(p.mutateOperations), { ctrl, label: "approval:" + it.type });
  if (!vo && _ctrim) { try { await ledger({ kind: "ceilingTrim", from: _ctrim.from, to: _ctrim.to, ceiling: _ctrim.ceiling }); } catch (e) {} }
  // Only flip to APPLIED when it actually ran; a dry-run only validated, so leave it APPROVED.
  if (!vo) await ref.update({ status: "APPLIED", appliedAt: f.FV.serverTimestamp() });
  return true;
}
// Re-apply every approval stuck in APPROVED (approved but its apply errored). The sanitizer
// above removes the dead field, so these now create cleanly. Honors dry-run.
async function retryStuckApprovals(ctrl) {
  ctrl = ctrl || (await control());
  const f = fb(); if (!f) return { tried: 0, applied: 0, failed: [] };
  const st = await f.db.collection(COL.approvals).where("status", "==", "APPROVED").limit(25).get();
  const ids = []; st.forEach(d => ids.push(d.id));
  let applied = 0; const failed = [];
  for (const id of ids) {
    try { await applyApproval(id, ctrl); if (!ctrl.dryRun) applied++; }
    catch (e) { failed.push({ id, error: String((e && e.message) || e).slice(0, 300) }); }
  }
  return { tried: ids.length, applied, failed, dryRun: !!ctrl.dryRun };
}

/* ============================ Helpers ============================ */
function micros(v) { return Math.round(Number(v) * 1e6); }
function fromMicros(m) { return (Number(m) || 0) / 1e6; }
function clampHeadline(s) { return String(s).slice(0, 30); }   // RSA headline ≤30
function clampDescription(s) { return String(s).slice(0, 90); } // RSA description ≤90
function cleanAdText(s) { return String(s == null ? "" : s).replace(/[\p{So}\p{Sk}\p{Extended_Pictographic}\u2190-\u21FF\u27F0-\u27FF\u2900-\u297F\u2B00-\u2BFF]/gu, "").replace(/\s{2,}/g, " ").trim(); } // strip prohibited symbols/emoji
function gAdsTime(d) {
  // "yyyy-MM-dd HH:mm:ss+00:00"
  const p = n => String(n).padStart(2, "0");
  return `${d.getUTCFullYear()}-${p(d.getUTCMonth()+1)}-${p(d.getUTCDate())} ` +
         `${p(d.getUTCHours())}:${p(d.getUTCMinutes())}:${p(d.getUTCSeconds())}+00:00`;
}
function daysUntil(mmdd, now = new Date()) {
  const [m, d] = mmdd.split("-").map(Number);
  let t = new Date(Date.UTC(now.getUTCFullYear(), m - 1, d));
  if (t < now) t = new Date(Date.UTC(now.getUTCFullYear() + 1, m - 1, d));
  return Math.round((t - now) / 86400000);
}

/* ===================== Brand-safety guardrails (your edge) ===================== */
// Generated copy is filtered HERE before it is ever queued — every headline/description
// runs through brandSafe() and the char clamps, so unsafe or off-tone lines never reach
// a campaign. (An earlier build also tried to stamp Campaign.text_guidelines server-side,
// but that field shape is rejected by the API, so brand-safety is enforced client-side only.)
const BRAND = {
  // never let AI write these into emotional/memorial/medical-adjacent copy
  termExclusions: (ENV.GADS_TERM_EXCLUSIONS ||
    "cure,heal disease,medical advice,guaranteed,miracle,cheap,discount diva,clearance," +
    "death,grief discount,cremation deal").split(",").map(s => s.trim()).filter(Boolean).slice(0, 25),
  messagingRestrictions: (ENV.GADS_MESSAGING_RULES ||
    "Keep sympathy and memorial language gentle and respectful; never use urgency, pressure, or sales hype on grief or loss themes.|" +
    "Do not make medical, health, or therapeutic claims about jewelry.|" +
    "Emotional appeals must feel sincere and personal, never exploitative.|" +
    "Always sound handcrafted and premium, never bargain-bin.").split("|").map(s => s.trim()).filter(Boolean).slice(0, 40)
};
function brandSafe(text) {
  const t = String(text).toLowerCase();
  return !BRAND.termExclusions.some(x => x && t.includes(x.toLowerCase()));
}
function textGuidelinesOp() {
  return { termExclusions: BRAND.termExclusions, messagingRestrictions: BRAND.messagingRestrictions };
}

/* ===================== OpenAI generation (repo convention) ===================== */
async function openaiJSON(prompt, { maxTokens = 1400, effort = "low" } = {}) {
  const model = GEN_MODEL;
  const payload = { model, messages: [{ role: "user", content: prompt }] };
  // NOTE: for gpt-5 / o* reasoning models, max_completion_tokens INCLUDES hidden reasoning
  // tokens — with effort "high" the reasoning share grows, so budget generously or long JSON
  // outputs get truncated mid-array.
  if (/^(gpt-5|o\d)/.test(model)) { payload.max_completion_tokens = maxTokens; payload.reasoning_effort = effort; }
  else { payload.max_tokens = Math.min(maxTokens, 900); payload.temperature = 0.8; }
  const res = await fetch("https://api.openai.com/v1/chat/completions", {
    method: "POST",
    headers: { "Content-Type": "application/json", Authorization: "Bearer " + (ENV.OPENAI_API_KEY || "") },
    body: JSON.stringify(payload)
  });
  const data = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error("[gads] OpenAI: " + ((data.error && data.error.message) || res.status));
  const choice = (data.choices || [])[0] || {};
  const raw = ((choice.message || {}).content || "");
  const cleaned = raw.replace(/```json|```/g, "").trim();
  try { return JSON.parse(cleaned); } catch (e) {}
  // Truncated output (finish_reason "length") is the usual culprit — salvage what parses:
  // cut back to the last complete object and close the brackets, so a near-complete
  // opportunities array isn't thrown away wholesale. If even that fails, THROW a descriptive
  // error instead of returning null: a silent null upstream is how "every re-scan shows the
  // same stale list" happened.
  const salvaged = _salvageJson(cleaned);
  if (salvaged) return salvaged;
  throw new Error("[gads] OpenAI returned unparseable JSON (finish_reason: " + (choice.finish_reason || "?") + ", " + cleaned.length + " chars)");
}

// Best-effort repair of truncated JSON: trim to the last complete value, then close
// any brackets/braces that are still open (string-aware). Returns parsed object or null.
function _salvageJson(s) {
  if (!s || s[0] !== "{") return null;
  for (let cut = s.length; cut > 1; ) {
    const j = Math.max(s.lastIndexOf("}", cut - 1), s.lastIndexOf("]", cut - 1));
    if (j < 0) return null;
    let candidate = s.slice(0, j + 1).replace(/,\s*$/, "");
    let open = [], inStr = false, esc = false;
    for (let i = 0; i < candidate.length; i++) {
      const ch = candidate[i];
      if (inStr) { if (esc) esc = false; else if (ch === "\\") esc = true; else if (ch === '"') inStr = false; continue; }
      if (ch === '"') inStr = true;
      else if (ch === "{" || ch === "[") open.push(ch);
      else if (ch === "}" || ch === "]") open.pop();
    }
    if (!inStr) {
      const close = open.reverse().map(c => c === "{" ? "}" : "]").join("");
      try { return JSON.parse(candidate + close); } catch (e) {}
    }
    cut = j;
  }
  return null;
}

// Build event-tailored RSA copy for a collection, on-brand and brand-safe.
async function generateRSAAssets(coll, event) {
  const proof = coll.reviewProof || "thousands of 5-star reviews";
  const heroes = (coll.heroProducts || []).slice(0, 6).join("; ");
  const prompt =
`You write Google Search ad copy for Brites, a handcrafted personalized charm-jewelry brand.
Voice: warm, sincere, premium, gift-and-emotion led — never bargain or hypey.
Collection: "${coll.title}" (${coll.handle}). Bestsellers: ${heroes || "n/a"}.
Social proof you may reference: ${proof}.
Occasion/emotion focus: ${event ? event.label + " — " + (event.angle || "") : "evergreen gifting"}.
Hard rules:
- 15 headlines, each ≤30 characters. 4 descriptions, each ≤90 characters.
- 4 sitelink texts (≤25 chars) with 1-line descriptions, 6 callouts (≤25 chars).
- Avoid these terms entirely: ${BRAND.termExclusions.join(", ")}.
- ${BRAND.messagingRestrictions.join(" ")}
Return ONLY JSON: {"headlines":[],"descriptions":[],"sitelinks":[{"text":"","desc":""}],"callouts":[]}`;
  const j = await openaiJSON(prompt, { maxTokens: 1500 });
  if (!j) return null;
  const out = {
    headlines: (j.headlines || []).map(clampHeadline).filter(brandSafe).slice(0, 15),
    descriptions: (j.descriptions || []).map(clampDescription).filter(brandSafe).slice(0, 4),
    sitelinks: (j.sitelinks || []).filter(s => brandSafe(s.text) && brandSafe(s.desc || ""))
                 .map(s => ({ text: String(s.text).slice(0, 25), desc: String(s.desc || "").slice(0, 35) })).slice(0, 4),
    callouts: (j.callouts || []).map(s => String(s).slice(0, 25)).filter(brandSafe).slice(0, 6)
  };
  // RSA minimums: 3 headlines, 2 descriptions
  if (out.headlines.length < 3 || out.descriptions.length < 2) return null;
  return out;
}

/* ===================== Event calendar (seed-on-empty) ===================== */
const SEED_CALENDAR = {
  // handle : { title, peaks:[{label, date 'MM-DD', leadDays, angle}], heroProducts, reviewProof }
  "gifts-for-teachers": { title: "Gifts for Teachers", reviewProof: "4.9★ from thousands of buyers",
    peaks: [{ label: "Teacher Appreciation Week", date: "05-04", leadDays: 21, angle: "thank the teacher who shaped them" },
            { label: "Back to School", date: "08-15", leadDays: 21, angle: "a keepsake for a new school year" },
            { label: "End of Year Thank-You", date: "06-01", leadDays: 18, angle: "say thank you as the year closes" }] },
  "gifts-for-nurses-doctors": { title: "Nurses & Doctors", reviewProof: "loved by thousands of caregivers",
    peaks: [{ label: "Nurses Week", date: "05-06", leadDays: 21, angle: "honor the ones who care for us" },
            { label: "Doctors' Day", date: "03-30", leadDays: 18, angle: "a thank-you they can keep" }] },
  "animal-lovers": { title: "Animal Lovers", reviewProof: "thousands of 5-star reviews",
    peaks: [{ label: "Mother's Day", date: "05-11", leadDays: 21, angle: "for the animal-lover mom" },
            { label: "Christmas", date: "12-25", leadDays: 35, angle: "a charm of their favorite creature" }] },
  "celestial": { title: "Celestial", reviewProof: "thousands of happy customers",
    peaks: [{ label: "Birthday / Zodiac", date: "01-01", leadDays: 0, angle: "their sign, in gold (evergreen)" }] },
  "bar-engraved": { title: "Personalized", reviewProof: "made-to-order keepsakes, 4.9★",
    peaks: [{ label: "Mother's Day", date: "05-11", leadDays: 21, angle: "their name, engraved forever" },
            { label: "Valentine's Day", date: "02-14", leadDays: 21, angle: "a personal piece, just for them" }] }
};

async function loadCalendar() {
  const f = fb(); if (!f) return SEED_CALENDAR;
  const cref = f.db.collection(COL.calendar);
  const snap = await cref.get();
  if (snap.empty) { // seed once so it's editable in the console going forward
    const batch = f.db.batch();
    Object.entries(SEED_CALENDAR).forEach(([h, v]) => batch.set(cref.doc(h), v));
    await batch.commit();
    return SEED_CALENDAR;
  }
  const out = {}; snap.forEach(d => out[d.id] = { handle: d.id, ...d.data() });
  return out;
}

// Which (collection, peak) pairs are within their lead window right now?
async function dueEvents(now = new Date()) {
  const cal = await loadCalendar();
  const due = [];
  Object.values(cal).forEach(coll => {
    (coll.peaks || []).forEach(pk => {
      if (!pk.date) return;
      const dleft = daysUntil(pk.date, now);
      if (pk.leadDays > 0 && dleft <= pk.leadDays && dleft >= Math.max(0, pk.leadDays - 3)) {
        due.push({ coll: { handle: coll.handle, title: coll.title, heroProducts: coll.heroProducts, reviewProof: coll.reviewProof },
                   event: { label: pk.label, angle: pk.angle, daysLeft: dleft } });
      }
    });
  });
  return due;
}

/* ===================== Build a Search campaign (atomic) ===================== */
// Returns mutateOperations[] for googleAds:mutate. Creates budget→campaign→adgroup
// →RSA in one transaction using temp resource names. Gated/queued by the worker.
/* date helpers for campaign scheduling windows (YYYY-MM-DD ↔ Google's YYYYMMDD) */
function _ymd(d) { return d.toISOString().slice(0, 10); }
function _parseYmd(s) { const m = /^(\d{4})-(\d{2})-(\d{2})/.exec(String(s || "").trim()); if (!m) return null; const d = new Date(Date.UTC(+m[1], +m[2] - 1, +m[3])); return isNaN(d.getTime()) ? null : d; }
function _todayUtc() { const t = new Date(); return new Date(Date.UTC(t.getUTCFullYear(), t.getUTCMonth(), t.getUTCDate())); }
function _daysBetween(a, b) { return Math.round((b.getTime() - a.getTime()) / 86400000); }
function gAdsDate(s, clampToday) { let d = _parseYmd(s); if (!d) return null; if (clampToday) { const t = _todayUtc(); if (d < t) d = t; } return _ymd(d).replace(/-/g, ""); }

// Extract a clean YYYY-MM-DD from a Google Ads date/datetime string ("2026-06-29 00:00:00", "20260629 000000", "2026-06-29").
function _dateOnly(s) { if (!s) return null; const m = String(s).match(/(\d{4})-?(\d{2})-?(\d{2})/); return m ? `${m[1]}-${m[2]}-${m[3]}` : null; }

// "Today" in the AD ACCOUNT's timezone (not the server's UTC), as YYYYMMDD — so scheduling
// decisions match how Google Ads evaluates start dates. Falls back to UTC if the lookup fails.
async function _accountTz() {
  try { const r = await gaql(`SELECT customer.time_zone FROM customer LIMIT 1`);
        if (r[0] && r[0].customer && r[0].customer.timeZone) return r[0].customer.timeZone; } catch (e) {}
  return "America/Toronto";
}
// Current wall-clock in the account's timezone (+optional ms offset) as "yyyyMMdd HH:MM:SS".
// Used for scheduling so Google Ads never sees a start_date_time in the past.
function _accountDateTime(tz, offsetMs) {
  const when = new Date(Date.now() + (offsetMs || 0)); const o = {};
  try {
    new Intl.DateTimeFormat("en-CA", { timeZone: tz, hour12: false, year: "numeric", month: "2-digit",
      day: "2-digit", hour: "2-digit", minute: "2-digit", second: "2-digit" })
      .formatToParts(when).forEach(p => { o[p.type] = p.value; });
  } catch (e) {
    o.year = when.getUTCFullYear(); o.month = String(when.getUTCMonth() + 1).padStart(2, "0");
    o.day = String(when.getUTCDate()).padStart(2, "0"); o.hour = String(when.getUTCHours()).padStart(2, "0");
    o.minute = String(when.getUTCMinutes()).padStart(2, "0"); o.second = String(when.getUTCSeconds()).padStart(2, "0");
  }
  const hh = (o.hour === "24") ? "00" : o.hour;   // some environments emit "24" for midnight
  return `${o.year}${o.month}${o.day} ${hh}:${o.minute}:${o.second}`;
}
// Account-timezone calendar date (with optional ms offset) as "YYYY-MM-DD", for segments.date ranges.
function _acctDateYmd(tz, offsetMs) { const s = _accountDateTime(tz, offsetMs || 0); return `${s.slice(0,4)}-${s.slice(4,6)}-${s.slice(6,8)}`; }

// Google Ads campaign schedule fields are startDateTime/endDateTime in "yyyyMMdd HH:MM:SS".
// Accepts a date (YYYY-MM-DD / YYYYMMDD) and appends a time, or passes through an existing datetime.
function _toGAdsDateTime(val, time) {
  if (val == null) return null;
  const s = String(val).trim();
  if (!s) return null;
  if (/\d{1,2}:\d{2}/.test(s)) return s;            // already has a time component
  const ymd = s.replace(/-/g, "");
  if (!/^\d{8}$/.test(ymd)) return null;
  return ymd + " " + time;
}

/* ===================== Keyword research (Google Keyword Planner + AI) =====================
   Two real signals, merged per keyword so EVERY opportunity is individually researched:
   1) Google Keyword Planner (generateKeywordIdeas): real 12-mo avg searches, competition index
      (0-100), and 20th/80th-percentile top-of-page bids. Used whenever the call succeeds.
   2) AI keyword research (from the opportunity model): per-keyword estimated searches, competition,
      CPC range, intent, and head/long-tail class — so cards are still tailored + differentiated
      when Keyword Planner is unavailable. Real Planner data OVERRIDES the estimate per keyword.
   The API error (if any) is captured and surfaced — never swallowed. */
// Keyword Planner permission is checked PER auth path AND per operating customer. Search/mutate
// work via the manager, but generateKeywordIdeas can 403 there. Keyword ideas are market-wide data
// (geo + language), NOT account-specific — so ANY account the user can reach that has Keyword Planner
// enabled returns the same numbers. We transparently try: the client via manager, the client direct,
// the client with no manager header, and finally the MANAGER account itself as the operating customer.
// Whichever returns data is locked in. If all 403, it's a true account-access gap (enable Keyword
// Planner / billing on an account) and we fall back to AI research.
let _kpAttempt = null; // null = undiscovered; else an attempt.key
const _sleep = ms => new Promise(r => setTimeout(r, ms));
const _KP_BACKOFF_MS = Number(ENV.KP_BACKOFF_MS || 1500);
function _kpAttempts() {
  const a = [
    { key: "mcc",  customer: CID,      login: undefined },
    { key: "cid",  customer: CID,      login: CID },
    { key: "none", customer: CID,      login: false }
  ];
  if (LOGIN_CID && LOGIN_CID !== CID) a.push({ key: "mgr", customer: LOGIN_CID, login: LOGIN_CID });
  return a;
}
async function keywordResearch(keywords, geoIds, { langId = "1000" } = {}) {
  const seeds = [...new Set((keywords || []).map(k => String(k).trim().toLowerCase()).filter(Boolean))].slice(0, 20);
  if (!seeds.length) return { ok: false, error: "no seeds", status: null };
  const geos = ((geoIds && geoIds.length) ? geoIds : ["2124"]).map(g => `geoTargetConstants/${String(g).replace(/\D/g, "")}`).filter(g => /\d/.test(g));
  const body = { language: `languageConstants/${langId}`, geoTargetConstants: geos, includeAdultKeywords: false, keywordPlanNetwork: "GOOGLE_SEARCH", keywordSeed: { keywords: seeds } };
  const all = _kpAttempts();
  const attempts = _kpAttempt ? all.filter(a => a.key === _kpAttempt) : all;
  let lastErr = null, lastStatus = null;
  for (const at of attempts) {
    try {
      let res, data;
      for (let n = 0; n < 2; n++) {
        const token = await mintToken();
        res = await fetch(`${BASE}/customers/${at.customer}:generateKeywordIdeas`, { method: "POST", headers: adsHeaders(token, at.login), body: JSON.stringify(body) });
        data = await res.json().catch(() => ({}));
        if (res.status === 429 && n === 0) { await _sleep(_KP_BACKOFF_MS); continue; } // brief backoff on rate limit
        break;
      }
      if (res.ok) {
        _kpAttempt = at.key;
        const ideas = (data.results || []).map(r => {
          const m = r.keywordIdeaMetrics || {};
          return { text: r.text, searches: Number(m.avgMonthlySearches) || 0,
                   competition: m.competition || "UNKNOWN", competitionIndex: m.competitionIndex != null ? Number(m.competitionIndex) : null,
                   low: fromMicros(m.lowTopOfPageBidMicros), high: fromMicros(m.highTopOfPageBidMicros) };
        });
        return { ok: true, ideas, status: res.status, authMode: at.key };
      }
      lastErr = (data.error && (data.error.message || (data.error.details && JSON.stringify(data.error.details)))) || JSON.stringify(data).slice(0, 300);
      lastStatus = res.status;
      // 429 = rate/quota limited but AUTHORIZED. The auth path works; trying the others just burns
      // more quota. Stop and report it as a rate limit (the caller backs off / uses cache).
      if (res.status === 429) { _kpAttempt = at.key; break; }
      if (!/permission|unauthor|USER_PERMISSION|login.customer/i.test(lastErr || "") && res.status !== 403 && res.status !== 401) break;
    } catch (e) { lastErr = e.message; lastStatus = null; }
  }
  return { ok: false, error: lastErr, status: lastStatus, triedModes: attempts.map(a => a.key) };
}
function _median(arr) { const a = (arr || []).filter(x => x != null && !isNaN(x)).sort((x, y) => x - y); if (!a.length) return null; const m = Math.floor(a.length / 2); return a.length % 2 ? a[m] : (a[m - 1] + a[m]) / 2; }
function _tailOf(text) { const w = String(text || "").trim().split(/\s+/).filter(Boolean).length; return w >= 4 ? "LONG" : w === 3 ? "MID" : "HEAD"; }
function _compIdx(level) { const l = String(level || "").toUpperCase(); return l === "HIGH" ? 80 : l === "MEDIUM" ? 50 : l === "LOW" ? 20 : null; }
function _compLabel(idx) { return idx == null ? "UNKNOWN" : idx >= 66 ? "HIGH" : idx >= 33 ? "MEDIUM" : "LOW"; }
// Normalize the model's per-keyword research (objects or bare strings) into the KP idea shape.
function aiKeywordResearch(aiKeywords) {
  return (aiKeywords || []).map(k => {
    if (typeof k === "string") return { text: k, searches: null, competition: "UNKNOWN", competitionIndex: null, low: null, high: null, tail: _tailOf(k), intent: null, real: false };
    if (!k || !k.text) return null;
    const ci = (k.competitionIndex != null) ? Number(k.competitionIndex) : _compIdx(k.competition);
    return { text: String(k.text), searches: (k.searches != null ? Number(k.searches) : null),
      competition: String(k.competition || _compLabel(ci) || "UNKNOWN").toUpperCase(), competitionIndex: ci,
      low: (k.cpcLow != null ? _r2(k.cpcLow) : null), high: (k.cpcHigh != null ? _r2(k.cpcHigh) : null),
      tail: (k.tail ? String(k.tail).toUpperCase() : _tailOf(k.text)), intent: k.intent || null, real: false };
  }).filter(Boolean);
}
// Merge AI keyword research with real Keyword Planner ideas. Real data wins per keyword text.
// Always returns a usable research object so the planner is NEVER stuck on a generic tier guess.
function mergeKeywordResearch(aiKeywords, kpResult) {
  const out = aiKeywordResearch(aiKeywords);
  const byText = {}; out.forEach(k => byText[k.text.toLowerCase()] = k);
  let realCount = 0;
  if (kpResult && kpResult.ok && Array.isArray(kpResult.ideas)) {
    kpResult.ideas.forEach(idea => {
      const key = String(idea.text || "").toLowerCase(); if (!key) return;
      const ex = byText[key];
      const rec = { text: idea.text, searches: idea.searches, competition: idea.competition || _compLabel(idea.competitionIndex),
        competitionIndex: idea.competitionIndex, low: _r2(idea.low) || null, high: _r2(idea.high) || null,
        tail: ex ? ex.tail : _tailOf(idea.text), intent: ex ? ex.intent : null, real: true };
      if (ex) Object.assign(ex, rec); else { out.push(rec); byText[key] = rec; }
      realCount++;
    });
  }
  const withVol = out.filter(k => k.searches != null);
  const searchVolume = withVol.length ? withVol.reduce((a, k) => a + (k.searches || 0), 0) : null;
  let cpcLow = _median(out.map(k => k.low).filter(x => x > 0));
  let cpcHigh = _median(out.map(k => k.high).filter(x => x > 0));
  const competitionIndex = _median(out.map(k => k.competitionIndex).filter(x => x != null));
  // If no bid data anywhere, derive a CPC band from real competition — still differentiated per opp.
  if (!(cpcHigh > 0)) { const ci = competitionIndex != null ? competitionIndex : 45; cpcHigh = _r2(0.55 + (ci / 100) * 2.6); cpcLow = _r2(cpcHigh * 0.45); }
  const longCount = out.filter(k => k.tail === "LONG").length, headCount = out.filter(k => k.tail === "HEAD").length;
  const longTailRatio = out.length ? Math.round(longCount / out.length * 100) : 0;
  out.sort((a, b) => (b.real ? 1 : 0) - (a.real ? 1 : 0) || (b.searches || 0) - (a.searches || 0));
  return { ok: true, source: realCount > 0 ? "google_keyword_planner" : "ai_estimate", realCount,
    keywords: out.slice(0, 12), searchVolume, competitionIndex, cpc: { low: cpcLow, high: cpcHigh },
    longTailRatio, longCount, headCount };
}
// Diagnostic: run one live Keyword Planner call and return the raw outcome (status + error + sample)
// so the actual reason for any failure is visible instead of silently falling back.
async function keywordDiag({ keyword, geo } = {}) {
  const kw = (keyword && String(keyword).trim()) || "name necklace";
  _kpAttempt = null; // force a fresh discovery so the diagnostic tests every path
  const r = await keywordResearch([kw], (geo && geo.length) ? geo : ["2124"]);
  const modeLabel = { mcc: "via manager (login-customer-id = MCC)", cid: "client account direct (login-customer-id = account)", none: "no login-customer-id header", mgr: "manager account as data source (customers/" + LOGIN_CID + ")" };
  // ---- SCAN PROBE: reproduce the opportunity-scan's EXACT keyword path (real defaultCountries geo +
  // the real seed phrases from the most recent scan) so the reason an opp falls back to AI estimates is
  // visible instead of guessed. Read-only. Read scanProbe.verdict for the plain-English diagnosis.
  let scanProbe = null;
  try {
    const ctrl = await control();
    const scanGeo = (Array.isArray(ctrl.defaultCountries) && ctrl.defaultCountries.length) ? ctrl.defaultCountries : ["2124"];
    const geoResolved = scanGeo.map(g => `geoTargetConstants/${String(g).replace(/\D/g, "")}`).filter(g => /\d/.test(g));
    let realSeeds = [];
    try {
      const f2 = fb();
      if (f2) {
        const s = await f2.db.collection(COL.state).doc("opportunities").get();
        const lst = (s.exists && Array.isArray((s.data() || {}).list)) ? s.data().list : [];
        realSeeds = [...new Set(lst.flatMap(o => (o.keywords || []).map(k => String(typeof k === "string" ? k : (k && k.text) || "").toLowerCase())).filter(Boolean))].slice(0, 15);
      }
    } catch (e) {}
    if (!realSeeds.length) realSeeds = [kw];
    const rp = await keywordResearch(realSeeds, scanGeo); // live call: real geo + real scan seeds
    const returned = new Set((rp.ideas || []).map(i => String(i.text || "").toLowerCase()));
    const seedsMatched = realSeeds.filter(s => returned.has(s));
    let verdict;
    if (geoResolved.length === 0) verdict = "GEO BROKEN: defaultCountries resolves to zero valid geoTargetConstants — the Keyword Planner request is malformed.";
    else if (!rp.ok) verdict = "API CALL FAILED with real seeds/geo (status " + (rp.status || "?") + "): " + (rp.error || "unknown") + ".";
    else if (seedsMatched.length === 0) verdict = "CALL OK but 0 of " + realSeeds.length + " opportunity seeds were returned by Keyword Planner (niche/low-volume phrases have no data). Exact-match merge finds nothing, so each opp falls back to AI estimates. " + (rp.ideas || []).length + " RELATED ideas WERE returned (the related-idea fallback now uses these).";
    else verdict = "CALL OK and " + seedsMatched.length + "/" + realSeeds.length + " seeds matched exactly — scan should show live data.";
    scanProbe = { verdict, geoRaw: scanGeo, geoResolved, geoResolvedCount: geoResolved.length,
      liveCallOk: !!rp.ok, status: rp.status || null, error: rp.error || null,
      seedsTested: realSeeds, seedsTestedCount: realSeeds.length,
      relatedIdeasReturned: (rp.ideas || []).length, seedsMatchedExactly: seedsMatched, seedsMatchedCount: seedsMatched.length };
  } catch (e) { scanProbe = { verdict: "probe threw: " + (e && e.message), error: e && e.message }; }
  return { ok: !!r.ok, status: r.status || null, error: r.error || null, ideaCount: (r.ideas || []).length,
    authMode: r.authMode || null, authModeLabel: r.authMode ? modeLabel[r.authMode] : null, triedModes: r.triedModes || (r.authMode ? [r.authMode] : null),
    sample: (r.ideas || []).slice(0, 6).map(i => ({ text: i.text, searches: i.searches, competition: i.competition, competitionIndex: i.competitionIndex, low: _r2(i.low), high: _r2(i.high) })),
    scanProbe,
    request: { endpoint: `customers/${CID}:generateKeywordIdeas`, customerId: CID, loginCustomerId: LOGIN_CID || null, version: V, seed: kw } };
}
// Back-compat wrapper: a real-only research object (used by custom builds, which have no AI seeds).
async function researchOpportunity(seeds, geoIds) {
  const kp = await keywordResearch(seeds, geoIds);
  const merged = mergeKeywordResearch(seeds, kp);
  merged.error = kp.ok ? null : (kp.error || "unavailable");
  return merged;
}

/* ===================== Batched + cached keyword research pool =====================
   Keyword Planner (generateKeywordIdeas) is rate-limited to ~1 request/sec per developer token
   (a SEPARATE limit from the 15,000/day operation quota, and NOT removed by Standard access).
   Firing one call per opportunity in parallel trips it instantly. So instead we:
     1) collect the UNIQUE seed phrases across every opportunity,
     2) serve them from a Firestore cache when fresh (same response over a long time span),
     3) otherwise query Google in SERIAL chunks of <=20 seeds with a small gap between chunks,
        stopping immediately on a 429 (rate limited), and cache whatever we got.
   Returns { ok, ideasByText:{lowercased text -> idea}, status, error, cached }. Callers pull each
   opportunity's own seeds out of the shared pool; seeds not reached (e.g. a mid-batch 429) simply
   fall back to AI estimates for that opportunity. */
function _kwCacheKey(seeds, geoKey) {
  const raw = seeds.slice().sort().join("|") + "@" + geoKey;
  return "kw_" + Buffer.from(raw).toString("base64").replace(/[^a-zA-Z0-9]/g, "").slice(0, 100);
}
async function _kwCacheGet(key) {
  const f = fb(); if (!f) return null;
  try { const d = await f.db.collection(COL.kwCache).doc(key).get();
    if (d.exists) { const x = d.data() || {}; if (x.at && (Date.now() - x.at) < 14 * 86400000) return x.ideasByText || null; } } catch (e) {}
  return null;
}
async function _kwCacheSet(key, ideasByText) {
  const f = fb(); if (!f) return;
  try { await f.db.collection(COL.kwCache).doc(key).set({ at: Date.now(), ideasByText }); } catch (e) {}
}
/* Live scan progress: tiny merges into the opportunities doc so the console can show a
   real progress bar (phase, %, detail) while the background scan runs. Best-effort only —
   a progress write must never break the scan. Cleared (null) when the scan ends either way. */
async function _scanProg(pct, label, detail) {
  const f = fb(); if (!f) return;
  try { await f.db.collection(COL.state).doc("opportunities").set({ progress: { pct: Math.max(0, Math.min(99, Math.round(pct))), label: String(label || "").slice(0, 80), detail: detail ? String(detail).slice(0, 120) : null, at: Date.now() } }, { merge: true }); } catch (e) {}
}
async function keywordResearchPool(seeds, geoIds, { langId = "1000", onChunk = null } = {}) {
  const uniq = [...new Set((seeds || []).map(s => String(s).trim().toLowerCase()).filter(Boolean))];
  if (!uniq.length) return { ok: false, error: "no seeds", status: null, ideasByText: {} };
  const geoKey = ((geoIds && geoIds.length) ? geoIds : ["2124"]).map(g => String(g).replace(/\D/g, "")).sort().join(",");
  const cacheKey = _kwCacheKey(uniq, geoKey);
  const cached = await _kwCacheGet(cacheKey);
  if (cached && Object.keys(cached).length) return { ok: true, ideasByText: cached, status: 200, cached: true };
  const ideasByText = {}; let anyOk = false, lastErr = null, lastStatus = null, brokeEarly = false;
  // Keyword Planner is rate-limited to ~1 request/sec (a SEPARATE limit from Basic/Standard access,
  // and NOT lifted by either). A transient 429 must NOT wipe keyword data for the whole scan, so each
  // chunk is retried with growing backoff before we give up. Only a PERSISTENT 429 (still limited after
  // every retry) stops the pool. A partial (rate-limited) pool is never cached, so a later scan
  // re-queries Google instead of inheriting the gap. Retries live HERE, not stacked inside
  // keywordResearch, so we don't multiply calls and worsen the very rate limit we're absorbing.
  const _CHUNK_RETRY_MS = [0, 3000, 6000]; // attempt 1 immediate, then wait 3s, then 6s before retrying
  for (let i = 0; i < uniq.length; i += 20) {
    const chunk = uniq.slice(i, i + 20);
    if (onChunk) { try { onChunk(Math.floor(i / 20) + 1, Math.ceil(uniq.length / 20), Object.keys(ideasByText).length); } catch (e) {} }
    if (i > 0) await _sleep(_KP_BACKOFF_MS); // serialize between chunks: stay under ~1 request/second
    let r = null;
    for (let a = 0; a < _CHUNK_RETRY_MS.length; a++) {
      if (_CHUNK_RETRY_MS[a]) await _sleep(_CHUNK_RETRY_MS[a]); // let the ~1/sec window clear before retrying
      r = await keywordResearch(chunk, geoIds, { langId }).catch(e => ({ ok: false, error: e && e.message, status: null }));
      if (r.ok || r.status !== 429) break; // success, or a non-rate-limit failure → stop retrying this chunk
    }
    if (r.ok) { anyOk = true; (r.ideas || []).forEach(idea => { const k = String(idea.text || "").toLowerCase(); if (k && !ideasByText[k]) ideasByText[k] = idea; }); }
    else { lastErr = r.error; lastStatus = r.status; if (r.status === 429) { brokeEarly = true; break; } } // still limited after retries → stop, keep what we have
  }
  if (anyOk && !brokeEarly) await _kwCacheSet(cacheKey, ideasByText); // never cache a partial (rate-limited) pool
  return { ok: anyOk, error: anyOk ? null : lastErr, status: lastStatus, ideasByText, cached: false };
}

/* ===================== Campaign planner (research-grounded) =====================
   Every opportunity (scanned or custom) is sized from Google Ads reality for a
   LOW-VOLUME handmade-jewelry advertiser:
   - Bidding: Manual CPC. It has no learning phase, and the max CPC bid is a HARD cap
     on what you pay per click — the lever the owner asked for. (Smart Bidding can't be
     CPC-capped and needs ~15-50 conversions/mo to optimize, which this account can't feed.)
   - Run length >= 21d (default 28; up to 45 around a dated occasion). Jewelry converts at
     ~2%, so a 1-week test yields too few sales to read, and Google needs 1-2+ weeks of
     steady data before performance stabilizes. Occasion campaigns start ~17d pre-peak to ramp.
   - CPC tiers from 2025-26 retail search benchmarks (e-commerce ~$1-3/click, cheaper for
     niche/long-tail), bumped a tier for competitive gifting peaks. All money in CURRENCY.   */
const PLAN_CVR = 0.02;  // jewelry/apparel conversion-rate benchmark (~1.5-3%) — used as a PRIOR, not a constant
const CPC_TIERS = [
  { low: 0.32, target: 0.50, max: 0.70 },  // 0 niche / long-tail themed
  { low: 0.50, target: 0.78, max: 1.05 },  // 1 personalized staple
  { low: 0.75, target: 1.10, max: 1.45 }   // 2 competitive gifting / precious
];
const _TIER_LABEL = ["niche / long-tail", "personalized staple", "competitive gifting"];
function _r2(n) { return Math.round(Number(n) * 100) / 100; }
function _r1(n) { return Math.round(Number(n) * 10) / 10; }

/* ---- Computed conversion rate (replaces the static 2% assumption) ----
   Real account CVR = conversions ÷ clicks from YOUR Google Ads history (120d), shrunk toward the
   2% jewelry benchmark with a Bayesian prior worth 500 clicks. With little history the benchmark
   dominates; as real clicks accumulate, YOUR rate takes over smoothly — no cliff, no tiny-sample
   noise. Clamped to a sane retail band. Cached 12h. Only counts history when conversion tracking
   is validated (otherwise clicks without recorded sales would drag CVR toward zero unfairly). */
const _CVR_PRIOR_CLICKS = 500, _CVR_MIN = 0.004, _CVR_MAX = 0.08;
let _cvrMem = null; // per-invocation memo
async function accountCvr() {
  if (_cvrMem && (Date.now() - _cvrMem.at) < 5 * 60 * 1000) return _cvrMem;
  const f = fb();
  if (f) { try { const d = await f.db.collection(COL.state).doc("cvr").get();
    if (d.exists) { const x = d.data() || {}; if (x.at && (Date.now() - x.at) < 12 * 3600000) { _cvrMem = x; return x; } } } catch (e) {} }
  let clicks = 0, conv = 0, tracked = false;
  try { const h = await conversionHealth({}); tracked = !!(h && h.validated); } catch (e) {}
  if (tracked) {
    try {
      const tz = await _accountTz();
      const rows = await metricsRange({ start: _acctDateYmd(tz, -119 * 86400000), end: _acctDateYmd(tz, 0) });
      (rows || []).forEach(c => { clicks += Number(c.clicks) || 0; conv += Number(c.conv) || 0; });
    } catch (e) {}
  }
  const cvr = Math.max(_CVR_MIN, Math.min(_CVR_MAX,
    (conv + PLAN_CVR * _CVR_PRIOR_CLICKS) / (clicks + _CVR_PRIOR_CLICKS)));
  const source = clicks >= 300
    ? `your account: ${_r2(conv)} sales / ${clicks} clicks (120d), blended with the ${Math.round(PLAN_CVR * 100)}% jewelry benchmark`
    : (tracked ? `${Math.round(PLAN_CVR * 100)}% jewelry benchmark (only ${clicks} tracked clicks so far — your real rate takes over as history builds)`
               : `${Math.round(PLAN_CVR * 100)}% jewelry benchmark (conversion tracking not yet validated)`);
  const out = { cvr: Math.round(cvr * 10000) / 10000, clicks, conv: _r2(conv), source, at: Date.now() };
  if (f) { try { await f.db.collection(COL.state).doc("cvr").set(out); } catch (e) {} }
  _cvrMem = out; return out;
}

/* ---- AI market read (bounded) ----
   The scan model contributes brand/market judgment the raw Google numbers can't: how strongly THIS
   collection × occasion converts for a personalized-charm store, demand direction into the window,
   and the best ad angle. It is applied as a BOUNDED multiplier on the computed CVR (0.75–1.25×) with
   its reasoning surfaced — never as free-form numbers, so it can tilt projections but not fabricate
   them. */
function _mktNorm(m) {
  if (!m || typeof m !== "object") return null;
  let fit = Number(m.fit);
  if (!isFinite(fit)) fit = 1;
  fit = Math.max(0.75, Math.min(1.25, fit));
  const demand = ["rising", "steady", "fading"].indexOf(String(m.demand || "").toLowerCase()) >= 0 ? String(m.demand).toLowerCase() : null;
  return { fit: _r2(fit), fitWhy: String(m.fitWhy || "").slice(0, 120) || null, demand, angle: String(m.angle || "").slice(0, 100) || null };
}
function _cpcTier(title, occasion) {
  const t = String(title || "").toLowerCase(), o = String(occasion || "").toLowerCase();
  let tier = 1;
  if (/(dinosaur|axolotl|mushroom|frog|\bcat\b|\bdog\b|dragon|\bfox\b|\bbee\b|cottagecore|zodiac|astrolog|gamer|anime|kawaii|spooky|niche)/.test(t)) tier = 0;
  else if (/(name|personaliz|custom|initial|birthstone|charm|couple|family|monogram|letter|bracelet|necklace|ring|earring|pendant)/.test(t)) tier = 1;
  if (/(diamond|gold|bridal|engagement|wedding|proposal)/.test(t)) tier = 2;
  if (/(mother'?s day|christmas|valentine|anniversary|wedding|engagement|graduation)/.test(o)) tier = Math.min(2, tier + 1);
  return tier;
}
function _nthWeekdayOfMonth(year, month, weekday, n) {
  const first = new Date(Date.UTC(year, month, 1));
  const day = 1 + ((weekday - first.getUTCDay() + 7) % 7) + (n - 1) * 7;
  return new Date(Date.UTC(year, month, day));
}
// Next upcoming peak date (YYYY-MM-DD) for a known annual occasion, else null (evergreen).
function _nextOccasionPeak(label) {
  const s = String(label || "").toLowerCase(); const now = _todayUtc();
  function pick(make) { let d = make(now.getUTCFullYear()); if (d < now) d = make(now.getUTCFullYear() + 1); return d; }
  if (/mother/.test(s)) return _ymd(pick(y => _nthWeekdayOfMonth(y, 4, 0, 2)));   // 2nd Sun May
  if (/father/.test(s)) return _ymd(pick(y => _nthWeekdayOfMonth(y, 5, 0, 3)));   // 3rd Sun Jun
  if (/valentine/.test(s)) return _ymd(pick(y => new Date(Date.UTC(y, 1, 14))));
  if (/christmas/.test(s)) return _ymd(pick(y => new Date(Date.UTC(y, 11, 25))));
  if (/graduation/.test(s)) return _ymd(pick(y => new Date(Date.UTC(y, 5, 10))));
  if (/back to school/.test(s)) return _ymd(pick(y => new Date(Date.UTC(y, 8, 1))));
  if (/teacher/.test(s)) return _ymd(pick(y => _nthWeekdayOfMonth(y, 4, 2, 1)));  // ~1st Tue May
  if (/nurse/.test(s)) return _ymd(pick(y => new Date(Date.UTC(y, 4, 12))));      // May 12
  return null;
}
// The whole research output for one campaign: CPC cap, daily budget, run window, expected
// outcome, plus plain-language rationale strings the console surfaces on every opportunity.
function planCampaign({ title, occasion, peakDate, ceiling, headroom, smartBidding, research, aov, cvrInfo, market } = {}) {
  const ccy = CURRENCY; const smart = !!smartBidding;
  const tier = _cpcTier(title, occasion);
  const tierLabel = _TIER_LABEL[tier];
  // CPC: REAL Keyword Planner top-of-page bids when we have them, tier heuristic otherwise.
  const R = (research && research.ok && (research.cpc.high > 0 || research.cpc.low > 0)) ? research : null;
  let cpc, cpcSource;
  if (R) {
    const hi = R.cpc.high || (R.cpc.low * 1.6), lo = R.cpc.low || (hi * 0.45);
    cpc = { low: _r2(lo), max: _r2(hi) };
    cpcSource = research.source || "google_keyword_planner";
  } else {
    const t = CPC_TIERS[tier]; cpc = { low: t.low, max: t.max }; cpcSource = "estimate";
  }
  // ---- ONE projection chain. Every number on the card derives from these three inputs. ----
  // (1) Expected PAID cost per click. Top-of-page low/high are the 20th/80th-percentile bids; real
  //     clicks clear between them, so we model the geometric mid of the band (the right average for
  //     skewed price data) — never above the cap on Manual CPC. Projecting off the low bid (old
  //     behavior) overstated clicks; the UI projecting off the cap understated them. This is the fix.
  const eCpcMarket = _r2(Math.sqrt(Math.max(0.05, cpc.low) * Math.max(cpc.low, cpc.max)));
  const eCpc = _r2(smart ? eCpcMarket : Math.min(eCpcMarket, cpc.max));
  // (2) Conversion rate — computed, not assumed: account history shrunk toward the benchmark
  //     (see accountCvr), then tilted by the bounded AI market read for THIS collection × occasion.
  const cvrBase = (cvrInfo && cvrInfo.cvr) || PLAN_CVR;
  const mkt = market || null;
  const cvrFit = mkt ? mkt.fit : 1;
  const cvrUsed = Math.round(Math.max(_CVR_MIN, Math.min(_CVR_MAX, cvrBase * cvrFit)) * 10000) / 10000;
  const cvrSourceText = (cvrInfo && cvrInfo.source) || `${Math.round(PLAN_CVR * 100)}% jewelry benchmark`;
  // (3) Average order value — real store data (passed in), null-safe below.
  const today = _todayUtc();
  const FLOOR = 21, DEFAULT = 28, MAX = 45, LEAD = 17, TAIL = 5;
  const peak = peakDate ? _parseYmd(peakDate) : (occasion ? _parseYmd(_nextOccasionPeak(occasion)) : null);
  let start, end, durBasis;
  if (peak && peak > today) {
    start = new Date(Math.max(today.getTime(), peak.getTime() - LEAD * 86400000));
    end = new Date(Math.max(start.getTime() + FLOOR * 86400000, peak.getTime() + TAIL * 86400000));
    durBasis = `Starts ~${Math.round((peak - start) / 86400000)} days before the ${occasion || "occasion"} peak to ramp and clear Google's 1\u20132 week learning window, then runs through it.`;
  } else {
    start = today; end = new Date(today.getTime() + DEFAULT * 86400000);
    durBasis = `Runs ${DEFAULT} days \u2014 at the modeled ~${(cvrUsed * 100).toFixed(1)}% conversion rate a shorter test produces too few sales to read reliably.`;
  }
  let durationDays = Math.max(FLOOR, Math.min(MAX, _daysBetween(start, end)));
  // Real competition nudges the run length: hotter auctions need more days to gather data.
  if (R && R.competitionIndex != null) {
    const adj = R.competitionIndex > 66 ? 5 : (R.competitionIndex < 33 ? -3 : 0);
    if (adj) { durationDays = Math.max(FLOOR, Math.min(MAX, durationDays + adj)); durBasis += ` Extended for high keyword competition (${Math.round(R.competitionIndex)}/100).`.replace(" Extended", R.competitionIndex > 66 ? " Extended" : " Trimmed"); }
  }
  end = new Date(start.getTime() + durationDays * 86400000);
  const room = headroom != null ? headroom : (ceiling != null ? ceiling : 25);
  const PACE = 9; // target clicks/day for a readable test
  const minDaily = Math.max(5, Math.ceil(4 * eCpc));
  const capDaily = Math.max(minDaily, Math.min(room > 0 ? room : 25, 25));
  let daily = Math.max(minDaily, Math.min(capDaily, Math.round(PACE * eCpc)));
  const noRoom = room > 0 && room < minDaily;
  // ---- projections (all from the same chain) ----
  const clicksPerDay = _r2(daily / eCpc);
  const clicksTotal = Math.round(clicksPerDay * durationDays);
  const conversions = _r2(clicksTotal * cvrUsed);
  const spendTotal = Math.round(daily * durationDays);
  const revenue = (aov && aov > 0) ? _r2(conversions * aov) : null;
  const UNC = 0.3; // ± uncertainty band on the conversion rate → revenue/ROAS bands
  const revenueLow = revenue != null ? _r2(revenue * (1 - UNC)) : null;
  const revenueHigh = revenue != null ? _r2(revenue * (1 + UNC)) : null;
  let expectedRoas = null;
  if (revenue != null && spendTotal > 0) {
    const lo = _r1(revenueLow / spendTotal), hi = _r1(revenueHigh / spendTotal);
    expectedRoas = { low: lo, high: hi, band: lo.toFixed(1) + "\u2013" + hi.toFixed(1) + "x",
      basis: `Computed: projected revenue \u00f7 projected spend, with \u00b1${Math.round(UNC * 100)}% conversion-rate uncertainty \u2014 derived from the same numbers on this card, not an AI guess.` };
  }
  // ---- strategy-aware framing (every string below quotes the SAME chain) ----
  const caveats = [];
  let cpcBasis, budgetBasis, goal, strategy, strategyLabel;
  const _srcName = R ? (cpcSource === "google_keyword_planner" ? "Google Keyword Planner" : "AI keyword research") : "";
  const realNote = R ? `${_srcName}: top-of-page bids ${ccy} ${cpc.low.toFixed(2)}\u2013${cpc.max.toFixed(2)} across ${(R.keywords ? R.keywords.length : "your")} keywords` + (R.searchVolume ? ` (~${R.searchVolume.toLocaleString()} searches/mo` : "") + (R.competitionIndex != null ? `${R.searchVolume ? ", " : " ("}competition ${Math.round(R.competitionIndex)}/100)` : (R.searchVolume ? ")" : "")) + "." : "";
  const eCpcNote = `Projections use a modeled expected paid CPC of ~${ccy} ${eCpc.toFixed(2)} (geometric mid of the bid band \u2014 real clicks clear between the 20th and 80th percentile bids${smart ? "" : ", and you rarely pay your cap"}).`;
  if (smart) {
    strategy = "SMART_BIDDING"; strategyLabel = "Smart Bidding";
    goal = `Maximize conversion value automatically \u2014 no per-click cap`;
    cpcBasis = (R ? realNote + " " : "") + `Smart Bidding sets each bid itself, so the ${ccy} ${cpc.max.toFixed(2)} figure is a reference, NOT a cap \u2014 clicks can cost more, especially while learning. ${eCpcNote}`;
    budgetBasis = `\u2248${Math.round(clicksPerDay)} clicks/day at the modeled ~${ccy} ${eCpc.toFixed(2)} expected CPC. Smart Bidding spends close to the full daily budget; keep it steady (no \u00b1>20% swings) so it doesn't restart learning.`;
    caveats.push(`Smart Bidding chases conversions by setting bids per auction \u2014 great when you have volume, but there is NO max-CPC cap, so cost per click can spike (most during the 1\u20132 week learning phase).`);
    if (conversions < 15) caveats.push(`This budget yields ~${conversions} sales over the run \u2014 below the ~15\u201330/month Google needs to exit learning, so it may keep spending unpredictably. Manual CPC gives a hard cap until volume grows.`);
  } else {
    strategy = "MANUAL_CPC"; strategyLabel = "Manual CPC";
    goal = `Maximize sales within a ${ccy} ${cpc.max.toFixed(2)} max CPC`;
    cpcBasis = R ? `${realNote} Hard cap at the 80th-percentile bid (${ccy} ${cpc.max.toFixed(2)}) so your ad reliably reaches the top without overpaying. ${eCpcNote}`
                 : `${tierLabel} terms (no live Keyword Planner data \u2014 estimate). Retail search clicks run ~$1\u20133; capped at ${ccy} ${cpc.max.toFixed(2)}. ${eCpcNote}`;
    budgetBasis = `\u2248${Math.round(clicksPerDay)} clicks/day at the modeled ~${ccy} ${eCpc.toFixed(2)} expected CPC \u2014 enough traffic to read without burning the ceiling.`;
    caveats.push(`Manual CPC: you never pay more than ${ccy} ${cpc.max.toFixed(2)} per click, and the daily budget caps each day's spend. No learning phase \u2014 but Google won't auto-raise bids to chase a likely sale.`);
    if (conversions < 15) caveats.push(`At this budget you'll gather directional data (~${conversions} sales over the run), short of the ~15\u201330/month Smart Bidding would need \u2014 which is exactly why a hard CPC cap is the safer default here.`);
  }
  caveats.push(`Conversion rate \u2014 modeled at ${(cvrUsed * 100).toFixed(1)}%: ${cvrSourceText}${mkt && mkt.fit !== 1 ? `, tilted \u00d7${mkt.fit} by the AI market read below` : ""}.`);
  if (mkt && (mkt.fitWhy || mkt.fit !== 1)) caveats.push(`AI market read \u2014 \u00d7${mkt.fit} conversion fit${mkt.fitWhy ? `: ${mkt.fitWhy}` : ""}${mkt.demand ? ` (demand ${mkt.demand})` : ""}. Bounded 0.75\u20131.25\u00d7 and applied to the conversion rate \u2014 it tilts the projection, it can't fabricate it.`);
  if (revenue != null) caveats.push(`Projected revenue ~${ccy} ${revenue.toLocaleString()} = ~${conversions} sales \u00d7 ${ccy} ${_r2(aov).toFixed(2)} average order (your real store data). With \u00b1${Math.round(UNC * 100)}% conversion uncertainty: ${ccy} ${revenueLow.toLocaleString()}\u2013${revenueHigh.toLocaleString()}, hence the ${expectedRoas ? expectedRoas.band : ""} expected ROAS (revenue \u00f7 ${ccy} ${spendTotal} spend).`);
  if (noRoom) caveats.push(`Ceiling headroom (${ccy} ${_r2(room)}) is below the ${ccy} ${minDaily} a campaign needs to gather data \u2014 raise the daily ceiling or pause a campaign first.`);
  return {
    currency: ccy, tier, tierLabel, cvr: cvrUsed, smartBidding: smart, capApplies: !smart, researched: !!R, cpcSource,
    cpc: { low: cpc.low, target: eCpc, max: cpc.max, source: cpcSource, basis: cpcBasis },
    duration: { days: durationDays, startDate: _ymd(start), endDate: _ymd(end), basis: durBasis },
    budget: { daily, basis: budgetBasis },
    expected: { clicksPerDay: Math.round(clicksPerDay), clicksTotal, conversions, spendTotal, revenue, revenueLow, revenueHigh, aov: aov || null, searchVolume: R ? R.searchVolume : null, competitionIndex: R ? R.competitionIndex : null },
    expectedRoas,
    // The frontend recomputes on budget/CPC/date edits using EXACTLY these inputs — one engine, two runtimes.
    model: { eCpcMarket, eCpc, cpcLow: cpc.low, cpcHigh: cpc.max, cvrBase, cvrFit, cvr: cvrUsed, cvrSource: cvrSourceText, aov: aov || 0, uncertainty: UNC },
    market: mkt,
    strategy, strategyLabel, goal, caveats
  };
}

// Default campaign-level negative keywords for a premium, made-to-order jewelry store: strip out
// makers, bargain-hunters, repairs, jobs, and competitor-marketplace traffic that won't convert.
// Broad-match negatives exclude the term in any query. Cuts wasted spend → better effective ROAS.
const DEFAULT_NEGATIVES = ["free", "diy", "how to make", "tutorial", "pattern", "cheap", "wholesale",
  "bulk", "supplier", "manufacturer", "repair", "fix", "job", "jobs", "hiring", "salary", "fake",
  "replica", "knockoff", "amazon", "temu", "shein", "wish", "meaning", "definition", "clipart", "svg", "png"];
// Brand callouts — descriptive (not promises), true for Brites, each ≤25 chars.
const BRAND_CALLOUTS = ["Handmade in Canada", "Personalized Charms", "Custom-Made Gifts", "Unique Handmade Designs"];
const _clip = (s, n) => String(s || "").slice(0, n);
// Sitelink + callout + structured-snippet assets. Google: sitelinks alone lift conversions ~15% by
// adding relevant links + ad real estate. All URLs are pages that always exist (collection, homepage,
// Shopify's built-in /collections/all sorts) so nothing 404s. Returned as asset + campaignAsset ops
// with temp resource names, all applied atomically with the campaign.
function buildCampaignAssets(coll, finalUrl, cRes) {
  const ASSET = n => `customers/${CID}/assets/${n}`; const ops = []; let an = -10;
  const short = _clip(coll.title, 16);
  const ALL = "https://britesjewelry.com/collections/all";
  const sitelinks = [
    { linkText: _clip("Shop " + short, 25), d1: "Browse the full collection", d2: "Personalized, made to order", url: finalUrl },
    { linkText: "Best Sellers", d1: "Our most-loved pieces", d2: "Top customer favorites", url: ALL + "?sort_by=best-selling" },
    { linkText: "New Arrivals", d1: "Fresh handmade designs", d2: "Just added to the shop", url: ALL + "?sort_by=created-descending" },
    { linkText: "Personalize a Gift", d1: "Add names, dates & charms", d2: "Made unique, just for them", url: finalUrl }
  ];
  sitelinks.forEach(s => { const a = ASSET(an--); ops.push({ assetOperation: { create: { resourceName: a, finalUrls: [s.url], sitelinkAsset: { linkText: _clip(s.linkText, 25), description1: _clip(s.d1, 35), description2: _clip(s.d2, 35) } } } }); ops.push({ campaignAssetOperation: { create: { asset: a, campaign: cRes, fieldType: "SITELINK" } } }); });
  BRAND_CALLOUTS.forEach(t => { const a = ASSET(an--); ops.push({ assetOperation: { create: { resourceName: a, calloutAsset: { calloutText: _clip(t, 25) } } } }); ops.push({ campaignAssetOperation: { create: { asset: a, campaign: cRes, fieldType: "CALLOUT" } } }); });
  const ss = ASSET(an--); ops.push({ assetOperation: { create: { resourceName: ss, structuredSnippetAsset: { header: "Types", values: ["Necklaces", "Bracelets", "Earrings", "Rings", "Charms"] } } } }); ops.push({ campaignAssetOperation: { create: { asset: ss, campaign: cRes, fieldType: "STRUCTURED_SNIPPET" } } });
  return { ops, summary: { sitelinks: sitelinks.length, callouts: BRAND_CALLOUTS.length, structuredSnippets: 1 } };
}

function buildSearchCampaignOps(coll, event, assets, { dailyBudget, startDate, endDate, countries, maxCpc, smartBidding, targetRoas, negatives, withAssets } = {}) {
  const tag = `${coll.handle}-${(event ? event.label : "evergreen").toLowerCase().replace(/[^a-z0-9]+/g, "-")}`.slice(0, 40);
  const bRes = `customers/${CID}/campaignBudgets/-1`;
  const cRes = `customers/${CID}/campaigns/-2`;
  const agRes = `customers/${CID}/adGroups/-3`;
  const finalUrl = `https://${(ENV.SITE_NAME ? "" : "")}britesjewelry.com/collections/${coll.handle}`;
  const startYmd = gAdsDate(startDate, true);   // clamp start to today-or-later
  const endYmd = gAdsDate(endDate, false);
  // Bidding: Manual CPC by default so the ad-group max CPC bid is a HARD cap on spend/click
  // (no learning phase; predictable budgets). Smart Bidding (Max Conversion Value, optional tROAS)
  // is opt-in via the console toggle (control.smartBidding) or GADS_TARGET_ROAS — note that
  // strategy CANNOT be CPC-capped. The choice is baked into the ops at generate time.
  const capCpc = Number(maxCpc) > 0 ? Number(maxCpc) : 0.80;
  const useSmart = (smartBidding != null) ? !!smartBidding : !!ENV.GADS_TARGET_ROAS;
  const tRoas = Number(targetRoas || ENV.GADS_TARGET_ROAS || 0);
  const bidding = useSmart
    ? { maximizeConversionValue: tRoas > 0 ? { targetRoas: tRoas } : {} }
    : { manualCpc: { enhancedCpcEnabled: false } };
  const ops = [
    { campaignBudgetOperation: { create: {
        resourceName: bRes, name: `BA · ${tag} · ${Date.now()}`,
        amountMicros: micros(dailyBudget), deliveryMethod: "STANDARD", explicitlyShared: false } } },
    { campaignOperation: { create: {
        resourceName: cRes, name: `BA · ${tag}`, status: "PAUSED",      // always start PAUSED
        advertisingChannelType: "SEARCH", campaignBudget: bRes,
        containsEuPoliticalAdvertising: "DOES_NOT_CONTAIN_EU_POLITICAL_ADVERTISING",
        ...(startYmd ? { startDateTime: startYmd + " 00:00:00" } : {}),
        ...(endYmd ? { endDateTime: endYmd + " 23:59:59" } : {}),
        ...bidding,
        networkSettings: { targetGoogleSearch: true, targetSearchNetwork: true, targetContentNetwork: false } } } },
    { adGroupOperation: { create: {
        resourceName: agRes, name: `${coll.title} · ${event ? event.label : "Evergreen"}`,
        campaign: cRes, type: "SEARCH_STANDARD", cpcBidMicros: micros(capCpc) } } },
    { adGroupAdOperation: { create: {
        adGroup: agRes, status: "ENABLED", ad: {
          finalUrls: [finalUrl],
          responsiveSearchAd: {
            headlines: assets.headlines.map(t => ({ text: clampHeadline(cleanAdText(t)) })).filter(h => h.text),
            descriptions: assets.descriptions.map(t => ({ text: clampDescription(cleanAdText(t)) })).filter(d => d.text)
          } } } } }
  ];
  // Keyword themes from collection title + event (phrase match)
  const kws = [coll.title, `${coll.title} gift`, `${coll.title} necklace`,
               event ? `${coll.title} ${event.label}` : null].filter(Boolean);
  kws.forEach((k, i) => ops.push({ adGroupCriterionOperation: { create: {
    adGroup: agRes, status: "ENABLED", keyword: { text: k, matchType: "PHRASE" } } } }));
  // Location targeting — one positive campaign criterion per chosen country. With NO location
  // criteria, Google Ads defaults to "all countries and territories", so we set these explicitly.
  (countries || []).map(x => String(x).replace(/\D/g, "")).filter(Boolean).forEach(gid => {
    ops.push({ campaignCriterionOperation: { create: {
      campaign: cRes, location: { geoTargetConstant: `geoTargetConstants/${gid}` } } } });
  });
  // Negative keywords (campaign level) — exclude non-buyer traffic. Saves spend → lifts real ROAS.
  const negs = (Array.isArray(negatives) ? negatives : DEFAULT_NEGATIVES).map(n => String(n).trim().toLowerCase()).filter(Boolean);
  const negSet = [...new Set(negs)];
  negSet.forEach(n => ops.push({ campaignCriterionOperation: { create: {
    campaign: cRes, negative: true, keyword: { text: n, matchType: "BROAD" } } } }));
  // Sitelink + callout + structured-snippet assets — extra links/real estate that lift CTR & conversions.
  let assetSummary = null;
  if (withAssets !== false) { const ca = buildCampaignAssets(coll, finalUrl, cRes); ca.ops.forEach(o => ops.push(o)); assetSummary = ca.summary; }
  return { ops, tag, finalUrl, negatives: negSet, assetSummary };
}

/* ============================ STAGES ============================ */

// MEASURE: snapshot campaign + asset + search-term performance into Firestore.
async function measure() {
  const f = fb();
  // 1) ALL campaigns (config only, no date segment) — guarantees brand-new / paused /
  //    zero-impression campaigns are included, which a date-segmented query would drop.
  const base = await gaql(
    `SELECT campaign.id, campaign.name, campaign.status, campaign.primary_status, campaign.primary_status_reasons, campaign_budget.resource_name, campaign_budget.amount_micros
     FROM campaign WHERE campaign.status != 'REMOVED'`);
  const byId = {};
  base.forEach(r => {
    byId[r.campaign.id] = {
      id: r.campaign.id, name: r.campaign.name, status: r.campaign.status,
      primaryStatus: r.campaign.primaryStatus || null,
      primaryStatusReasons: r.campaign.primaryStatusReasons || [],
      budget: fromMicros(r.campaignBudget && r.campaignBudget.amountMicros),
      budgetRes: (r.campaignBudget && r.campaignBudget.resourceName) || null,
      cost: 0, conv: 0, value: 0, clicks: 0, impr: 0
    };
  });
  // 2) Metrics for the last 14 days INCLUDING today — Google's LAST_14_DAYS excludes today, so a
  //    campaign that only started serving today would otherwise read $0 spend / 0 conversions no
  //    matter how often you refresh. Window computed in the account's timezone.
  const _mtz = await _accountTz();
  const _mEnd = _acctDateYmd(_mtz, 0), _mStart = _acctDateYmd(_mtz, -13 * 86400000);
  try {
    const met = await gaql(
      `SELECT campaign.id, metrics.cost_micros, metrics.conversions, metrics.conversions_value,
              metrics.clicks, metrics.impressions
       FROM campaign WHERE segments.date BETWEEN '${_mStart}' AND '${_mEnd}' AND campaign.status != 'REMOVED'`);
    met.forEach(r => {
      const c = byId[r.campaign.id]; if (!c) return;
      c.cost += fromMicros(r.metrics.costMicros); c.conv += Number(r.metrics.conversions || 0);
      c.value += Number(r.metrics.conversionsValue || 0); c.clicks += Number(r.metrics.clicks || 0);
      c.impr += Number(r.metrics.impressions || 0);
    });
  } catch (e) {}
  // 3) Schedule window (start/end) — queried separately, with a field-name fallback, so a
  //    naming difference can never blank the whole dashboard. Overlays startDate/endDate.
  for (const [sf, ef, sk, ek] of [
    ["campaign.start_date_time", "campaign.end_date_time", "startDateTime", "endDateTime"],
    ["campaign.start_date", "campaign.end_date", "startDate", "endDate"]
  ]) {
    try {
      const sch = await gaql(`SELECT campaign.id, ${sf}, ${ef} FROM campaign WHERE campaign.status != 'REMOVED'`);
      sch.forEach(r => { const c = byId[r.campaign.id]; if (!c) return;
        c.startDate = _dateOnly(r.campaign[sk]); c.endDate = _dateOnly(r.campaign[ek]); });
      break; // first field-set that works wins
    } catch (e) { /* try the next field naming */ }
  }
  // 4) Current target countries per campaign (positive LOCATION criteria) — isolated so any issue
  //    can't blank the dashboard. Overlays an array of geo IDs onto each campaign record.
  try {
    const loc = await gaql(
      `SELECT campaign.id, campaign_criterion.location.geo_target_constant, campaign_criterion.negative
       FROM campaign_criterion
       WHERE campaign_criterion.type = 'LOCATION' AND campaign_criterion.status != 'REMOVED'`);
    loc.forEach(r => { const c = byId[r.campaign.id]; if (!c) return;
      const cc = r.campaignCriterion || {}; if (cc.negative) return;
      const gid = String(((cc.location && cc.location.geoTargetConstant) || "").split("/").pop() || "");
      if (!gid) return; (c.countries = c.countries || []).push(gid);
    });
  } catch (e) {}
  const snapshot = Object.values(byId);
  if (f) await f.db.collection(COL.metrics).add({ at: f.FV.serverTimestamp(), kind: "campaign14d", snapshot });
  await attributeOccasionsFromSnapshot(snapshot);
  return snapshot;
}

// METRICS for an arbitrary date range — powers the Command Center's per-section calendar pickers.
// Same shape as measure()'s snapshot (per-campaign cost/conv/value/clicks/impr + schedule), but for
// the chosen [start,end] window and WITHOUT writing a Firestore snapshot. Read-only.
async function metricsRange({ start, end } = {}) {
  const tz = await _accountTz();
  let s = _dateOnly(start) || _acctDateYmd(tz, -29 * 86400000);
  let e = _dateOnly(end) || _acctDateYmd(tz, 0);
  if (s > e) { const t = s; s = e; e = t; }
  const base = await gaql(
    `SELECT campaign.id, campaign.name, campaign.status, campaign.primary_status, campaign.primary_status_reasons, campaign_budget.amount_micros
     FROM campaign WHERE campaign.status != 'REMOVED'`);
  const byId = {};
  base.forEach(r => { byId[r.campaign.id] = {
    id: r.campaign.id, name: r.campaign.name, status: r.campaign.status,
    primaryStatus: r.campaign.primaryStatus || null, primaryStatusReasons: r.campaign.primaryStatusReasons || [],
    budget: fromMicros(r.campaignBudget && r.campaignBudget.amountMicros),
    cost: 0, conv: 0, value: 0, clicks: 0, impr: 0 }; });
  try {
    const met = await gaql(
      `SELECT campaign.id, metrics.cost_micros, metrics.conversions, metrics.conversions_value, metrics.clicks, metrics.impressions
       FROM campaign WHERE segments.date BETWEEN '${s}' AND '${e}' AND campaign.status != 'REMOVED'`);
    met.forEach(r => { const c = byId[r.campaign.id]; if (!c) return;
      c.cost += fromMicros(r.metrics.costMicros); c.conv += Number(r.metrics.conversions || 0);
      c.value += Number(r.metrics.conversionsValue || 0); c.clicks += Number(r.metrics.clicks || 0);
      c.impr += Number(r.metrics.impressions || 0); });
  } catch (er) {}
  for (const [sf, ef, sk, ek] of [
    ["campaign.start_date_time", "campaign.end_date_time", "startDateTime", "endDateTime"],
    ["campaign.start_date", "campaign.end_date", "startDate", "endDate"]
  ]) {
    try { const sch = await gaql(`SELECT campaign.id, ${sf}, ${ef} FROM campaign WHERE campaign.status != 'REMOVED'`);
      sch.forEach(r => { const c = byId[r.campaign.id]; if (!c) return; c.startDate = _dateOnly(r.campaign[sk]); c.endDate = _dateOnly(r.campaign[ek]); }); break;
    } catch (e2) {}
  }
  return { snapshot: Object.values(byId), range: { start: s, end: e } };
}
async function pruneAssets({ ctrl, minImpr = 500 } = {}) {
  ctrl = ctrl || (await control());
  const rows = await gaql(
    `SELECT ad_group_ad_asset_view.performance_label, ad_group_ad_asset_view.field_type,
            asset.resource_name, asset.text_asset.text, campaign.name, ad_group.resource_name,
            metrics.impressions
     FROM ad_group_ad_asset_view
     WHERE segments.date DURING LAST_30_DAYS
       AND ad_group_ad_asset_view.performance_label = 'LOW'
       AND ad_group_ad_asset_view.field_type IN ('HEADLINE','DESCRIPTION')`);
  const weak = rows.filter(r => Number(r.metrics.impressions || 0) >= minImpr);
  if (!weak.length) return { flagged: 0, queued: 0 };
  // (replacement copy generation happens at the campaign/collection level on the
  //  next event refresh; here we just surface the weak assets for the operator)
  const byCampaign = {};
  weak.forEach(r => {
    const k = r.campaign.name; (byCampaign[k] = byCampaign[k] || []).push({
      field: r.adGroupAdAssetView.fieldType, text: r.asset.textAsset && r.asset.textAsset.text,
      asset: r.asset.resourceName, impr: Number(r.metrics.impressions || 0) });
  });
  const id = await enqueueApproval({ type: "creative", vetted: false,
    summary: `${weak.length} low-performing RSA assets across ${Object.keys(byCampaign).length} campaigns — review for refresh`,
    payload: { note: "operator-review", weak: byCampaign } });
  return { flagged: weak.length, queued: 1, approvalId: id };
}

// MINE: converting search terms ⇒ exact keywords; expensive zero-conv terms ⇒ negatives.
async function mineSearchTerms({ ctrl, convMin = 1, wasteCost = 8 } = {}) {
  ctrl = ctrl || (await control());
  const rows = await gaql(
    `SELECT search_term_view.search_term, search_term_view.status, campaign.id, campaign.name,
            ad_group.resource_name, metrics.conversions, metrics.cost_micros, metrics.clicks
     FROM search_term_view WHERE segments.date DURING LAST_30_DAYS`);
  const addKw = []; const addNeg = [];
  rows.forEach(r => {
    const term = r.searchTermView.search_term || r.searchTermView.searchTerm;
    if (!term) return;
    const conv = Number(r.metrics.conversions || 0);
    const cost = fromMicros(r.metrics.costMicros);
    const already = (r.searchTermView.status === "ADDED");
    if (conv >= convMin && !already && r.adGroup) {
      addKw.push({ adGroupCriterion: { adGroup: r.adGroup.resourceName, status: "ENABLED",
        keyword: { text: term, matchType: "EXACT" } } });
    } else if (conv === 0 && cost >= wasteCost && r.campaign) {
      addNeg.push({ campaignCriterion: { campaign: `customers/${CID}/campaigns/${r.campaign.id}`,
        negative: true, keyword: { text: term, matchType: "EXACT" } } });
    }
  });
  let queued = 0;
  if (addKw.length) { await enqueueApproval({ type: "keywords", vetted: true,
    summary: `${addKw.length} converting search terms → add as exact keywords`,
    payload: { service: "adGroupCriteria", operations: addKw.map(create => ({ create })) } }); queued++; }
  if (addNeg.length) { await enqueueApproval({ type: "negatives", vetted: true,
    summary: `${addNeg.length} wasteful zero-conversion terms → add as negatives`,
    payload: { service: "campaignCriteria", operations: addNeg.map(create => ({ create })) } }); queued++; }
  return { keywords: addKw.length, negatives: addNeg.length, queued };
}

// REALLOCATE: move budget toward above-ROAS campaigns within the global ceiling.
async function reallocateBudgets({ ctrl } = {}) {
  ctrl = ctrl || (await control());
  const rows = await gaql(
    `SELECT campaign.id, campaign.name, campaign.status, campaign_budget.resource_name,
            campaign_budget.amount_micros, metrics.cost_micros, metrics.conversions_value
     FROM campaign WHERE segments.date DURING LAST_14_DAYS
       AND campaign.status = 'ENABLED' AND campaign.advertising_channel_type IN ('SEARCH','PERFORMANCE_MAX')`);
  const items = rows.map(r => ({
    id: r.campaign.id, name: r.campaign.name,
    budgetRes: r.campaignBudget && r.campaignBudget.resourceName,
    budget: fromMicros(r.campaignBudget && r.campaignBudget.amountMicros),
    cost: fromMicros(r.metrics.costMicros), value: Number(r.metrics.conversionsValue || 0)
  })).filter(x => x.budgetRes && x.budget > 0);
  if (!items.length) return { moves: 0 };
  const target = ctrl.targetRoas || (() => {
    const tc = items.reduce((a, b) => a + b.cost, 0), tv = items.reduce((a, b) => a + b.value, 0);
    return tc > 0 ? tv / tc : 0;
  })();
  const totalBudget = items.reduce((a, b) => a + b.budget, 0);
  const ceiling = ctrl.maxDailyBudgetTotal;
  const stepMax = ctrl.maxBudgetStepPct / 100;
  const ops = []; const moves = [];
  items.forEach(x => {
    const roas = x.cost > 0 ? x.value / x.cost : null;
    if (roas == null) return;
    let factor = 1;
    if (roas >= target * 1.15) factor = 1 + stepMax;        // scale winners up
    else if (roas <= target * 0.6) factor = 1 - stepMax;     // trim losers
    if (factor === 1) return;
    let newBudget = Math.max(1, +(x.budget * factor).toFixed(2));
    moves.push({ campaign: x.name, from: x.budget, to: newBudget, roas: +roas.toFixed(2), target: +target.toFixed(2) });
    ops.push({ update: { resourceName: x.budgetRes, amountMicros: micros(newBudget) }, updateMask: "amount_micros" });
  });
  // enforce ceiling: if proposed sum exceeds cap, scale all proposed-up moves down
  let proposedTotal = items.reduce((a, b) => {
    const mv = moves.find(m => m.campaign === b.name); return a + (mv ? mv.to : b.budget);
  }, 0);
  if (proposedTotal > ceiling) return { moves: 0, blocked: "ceiling", proposedTotal, ceiling };
  if (!ops.length) return { moves: 0 };
  // small total moves auto (within autoApprove), large ones to the queue
  const pctTotalChange = Math.abs(proposedTotal - totalBudget) / Math.max(1, totalBudget) * 100;
  if (pctTotalChange <= ctrl.budgetMoveApprovalPct) {
    await mutate("campaignBudgets", ops, { ctrl, label: "reallocateBudgets" });
    return { moves: ops.length, applied: true, dryRun: !!ctrl.dryRun, detail: moves };
  }
  const id = await enqueueApproval({ type: "budget", vetted: false,
    summary: `Budget reallocation (${pctTotalChange.toFixed(0)}% of total) across ${ops.length} campaigns`,
    payload: { service: "campaignBudgets", operations: ops }, });
  return { moves: ops.length, applied: false, queued: true, approvalId: id, detail: moves };
}

// ANOMALY: trip breaker if yesterday's spend spikes vs trailing average.
async function anomalyCheck({ ctrl } = {}) {
  ctrl = ctrl || (await control());
  const f = fb();
  const y = await gaql(`SELECT metrics.cost_micros FROM customer WHERE segments.date DURING YESTERDAY`);
  const t = await gaql(`SELECT metrics.cost_micros FROM customer WHERE segments.date DURING LAST_14_DAYS`);
  const yCost = fromMicros((y[0] && y[0].metrics.costMicros) || 0);
  const tCost = fromMicros((t[0] && t[0].metrics.costMicros) || 0) / 14;
  const tripped = tCost > 0 && yCost > tCost * ctrl.anomalySpendMultiple;
  if (tripped && f) {
    await f.db.collection(COL.control).doc("control").set(
      { enabled: false, trippedAt: f.FV.serverTimestamp(), tripReason: `spend ${yCost.toFixed(2)} > ${ctrl.anomalySpendMultiple}× avg ${tCost.toFixed(2)}` },
      { merge: true });
  }
  return { yesterday: +yCost.toFixed(2), trailingAvg: +tCost.toFixed(2), tripped };
}

/* ===================== Spend-cap enforcement ===================== */

// Sum of ENABLED campaigns' daily budgets (the budgets that can actually spend right now).
async function _enabledBudgetTotal() {
  try {
    const rows = await gaql(`SELECT campaign.id, campaign_budget.amount_micros FROM campaign WHERE campaign.status = 'ENABLED'`);
    return rows.reduce((s, r) => s + fromMicros(r.campaignBudget && r.campaignBudget.amountMicros), 0);
  } catch (e) { return 0; }
}

// Keep the SUM of enabled campaigns' daily budgets at/under the ceiling by scaling them all down
// proportionally. Catches drift from manual edits or many concurrent launches.
async function enforceBudgetCeiling({ ctrl } = {}) {
  ctrl = ctrl || (await control());
  const ceiling = Number(ctrl.maxDailyBudgetTotal) || 0;
  if (!(ceiling > 0)) return { ok: true, skipped: "no ceiling set" };
  const rows = await gaql(
    `SELECT campaign.id, campaign.name, campaign_budget.resource_name, campaign_budget.amount_micros
     FROM campaign WHERE campaign.status = 'ENABLED'`);
  const items = rows.map(r => ({ id: r.campaign.id, name: r.campaign.name,
      res: r.campaignBudget && r.campaignBudget.resourceName,
      budget: fromMicros(r.campaignBudget && r.campaignBudget.amountMicros) }))
    .filter(x => x.res && x.budget > 0);
  const total = items.reduce((a, b) => a + b.budget, 0);
  if (total <= ceiling + 0.001) return { ok: true, total: +total.toFixed(2), ceiling, withinCeiling: true };
  const factor = ceiling / total, floor = 1;
  const ops = [], moves = [];
  items.forEach(x => {
    const nb = Math.max(floor, +(x.budget * factor).toFixed(2));
    if (Math.abs(nb - x.budget) < 0.01) return;
    moves.push({ campaign: x.name, from: x.budget, to: nb });
    ops.push({ update: { resourceName: x.res, amountMicros: micros(nb) }, updateMask: "amount_micros" });
  });
  if (!ops.length) return { ok: true, total: +total.toFixed(2), ceiling, withinCeiling: false, trimmed: 0 };
  const res = await mutate("campaignBudgets", ops, { ctrl, label: "enforceCeiling" });
  if (res && res.partialFailureError) { const m = (res.partialFailureError.message || "").slice(0, 300); throw new Error(`ceiling trim rejected: ${m}`); }
  await ledger({ kind: "enforceBudgetCeiling", total: +total.toFixed(2), ceiling, trimmed: ops.length, validateOnly: !!ctrl.dryRun });
  return { ok: true, total: +total.toFixed(2), ceiling, trimmed: ops.length, detail: moves, dryRun: !!ctrl.dryRun };
}

// Month-to-date account spend (computed in the account's timezone).
async function _mtdSpend() {
  const tz = await _accountTz();
  const end = _acctDateYmd(tz, 0);
  const start = end.slice(0, 8) + "01"; // first day of the current month, YYYY-MM-01
  const r = await gaql(`SELECT metrics.cost_micros FROM customer WHERE segments.date BETWEEN '${start}' AND '${end}'`);
  return { mtd: fromMicros((r[0] && r[0].metrics && r[0].metrics.costMicros) || 0), start, end };
}

// Hard monthly cap (opt-in): when month-to-date account spend reaches maxMonthlySpend, PAUSE every
// enabled campaign and stop the autopilot. This is the only true hard stop, since Google has no
// native account-level cap. Does nothing unless maxMonthlySpend is set.
async function monthlySpendGuard({ ctrl } = {}) {
  ctrl = ctrl || (await control());
  const limit = Number(ctrl.maxMonthlySpend) || 0;
  if (!(limit > 0)) return { ok: true, skipped: "no monthly cap set" };
  const { mtd, start, end } = await _mtdSpend();
  const pct = +(mtd / limit * 100).toFixed(1);
  if (mtd < limit) return { ok: true, mtd: +mtd.toFixed(2), limit, pct, tripped: false, window: { start, end } };
  let paused = 0;
  try {
    const rows = await gaql(`SELECT campaign.id, campaign.resource_name FROM campaign WHERE campaign.status = 'ENABLED'`);
    const ops = rows.map(r => ({ update: { resourceName: (r.campaign && r.campaign.resourceName) || `customers/${CID}/campaigns/${r.campaign.id}`, status: "PAUSED" }, updateMask: "status" }));
    if (ops.length && !ctrl.dryRun) await mutate("campaigns", ops, { ctrl, label: "monthlyCapPause" });
    paused = ops.length;
  } catch (e) {}
  const f = fb();
  if (f && !ctrl.dryRun) {
    try { await f.db.collection(COL.control).doc("control").set({ enabled: false, trippedAt: f.FV.serverTimestamp(), tripReason: `monthly cap reached: ${CURRENCY}${mtd.toFixed(2)} \u2265 ${CURRENCY}${limit}` }, { merge: true }); } catch (e) {}
  }
  await ledger({ kind: "monthlySpendGuard", mtd: +mtd.toFixed(2), limit, paused, validateOnly: !!ctrl.dryRun });
  return { ok: true, mtd: +mtd.toFixed(2), limit, pct, tripped: true, paused, dryRun: !!ctrl.dryRun };
}

/* ===================== Live Shopify collections ===================== */
// Pulls real collections from the store (same client-credentials pattern the rest
// of the repo uses), cached in Firestore so the Bench loads fast. force=true re-pulls.
let _shTok = null, _shExp = 0;
async function shopifyToken() {
  if (_shTok && Date.now() < _shExp - 60000) return _shTok;
  const store = ENV.SHOPIFY_STORE;
  const res = await fetch(`https://${store}/admin/oauth/access_token`, {
    method: "POST", headers: { "Content-Type": "application/x-www-form-urlencoded" },
    body: new URLSearchParams({ grant_type: "client_credentials",
      client_id: ENV.SHOPIFY_CLIENT_ID, client_secret: ENV.SHOPIFY_CLIENT_SECRET })
  });
  const txt = await res.text();
  if (!res.ok) throw new Error("Shopify token " + res.status + ": " + txt.slice(0, 160));
  const d = JSON.parse(txt); _shTok = d.access_token; _shExp = Date.now() + (d.expires_in || 86399) * 1000;
  return _shTok;
}
async function shopifyGql(query) {
  const store = ENV.SHOPIFY_STORE, ver = ENV.SHOPIFY_API_VERSION || "2025-10", token = await shopifyToken();
  const res = await fetch(`https://${store}/admin/api/${ver}/graphql.json`, {
    method: "POST", headers: { "X-Shopify-Access-Token": token, "Content-Type": "application/json" },
    body: JSON.stringify({ query })
  });
  const d = await res.json().catch(() => ({}));
  if (!res.ok || d.errors) throw new Error("Shopify GQL: " + JSON.stringify(d.errors || res.status).slice(0, 200));
  return d.data;
}
async function fetchShopifyCollections() {
  const out = []; let cursor = null, guard = 0;
  do {
    const after = cursor ? `, after: "${cursor}"` : "";
    const d = await shopifyGql(`{ collections(first: 250, sortKey: TITLE${after}) {
      pageInfo { hasNextPage endCursor } edges { node { title handle } } } }`);
    const edges = (d.collections && d.collections.edges) || [];
    edges.forEach(e => { if (e.node && e.node.handle) out.push({ handle: e.node.handle, title: e.node.title }); });
    const pi = d.collections && d.collections.pageInfo;
    cursor = pi && pi.hasNextPage ? pi.endCursor : null;
  } while (cursor && ++guard < 10);
  return out;
}
async function getCollections({ force } = {}) {
  const f = fb();
  if (f && !force) {
    try {
      const s = await f.db.collection(COL.state).doc("collections").get();
      if (s.exists) { const x = s.data(); if (x.at && (Date.now() - x.at) < 60 * 60 * 1000 && Array.isArray(x.list) && x.list.length) return x.list; }
    } catch (e) {}
  }
  let list;
  try { list = await fetchShopifyCollections(); }
  catch (e) {
    if (f) { try { const s = await f.db.collection(COL.state).doc("collections").get(); if (s.exists && Array.isArray(s.data().list) && s.data().list.length) return s.data().list; } catch (_) {} }
    return COLLECTIONS; // last-resort static fallback
  }
  if (!list.length) return COLLECTIONS;
  if (f) { try { await f.db.collection(COL.state).doc("collections").set({ list, at: Date.now() }); } catch (e) {} }
  return list;
}

/* ===================== Collection profiles (scanned + distilled from real listings) =====================
   The scan used to judge a collection by its TITLE alone; profiling only its top sellers would just
   swap one bias for another (a 200-listing collection is NOT its 5 bestsellers). So each collection
   gets a STRATIFIED sample — up to 30 best-sellers (what proves demand) + 20 newest (where the
   collection is heading), deduped — which is then DISTILLED in code into a motif/type/material
   frequency inventory + price spread. Frequencies across ~50 stratified listings approximate the
   real composition of even a several-hundred-listing collection, and mid-frequency motifs are where
   creative long-tail keywords live. Compact enough to hand the AI EVERY collection's full inventory.
   One paginated Admin GraphQL pass, cached 7 days. Three query variants cover API-version
   differences (productsCount object vs int vs absent); on total failure the scan degrades to
   titles-only exactly as before — never blocks. */
const _TOK_STOP = new Set(["for","the","and","with","a","an","of","in","to","your","my","her","his","our","or","on","by","from","this","that","gift","gifts","personalized","personalised","custom","customized","dainty","tiny","mini","small","cute","handmade","women","men","girls","boys","kids","jewelry","jewellery",
  // description fluff (marketing filler that would pollute the motif inventory)
  "beautiful","perfect","quality","love","made","hand","handcrafted","everyday","piece","pieces","wear","wearing","style","design","designed","comes","makes","great","ideal","special","unique","free","shipping","ships","order","box","packaging","available","choose","select","options","option"]);
const _TOK_TYPE = new Set(["necklace","necklaces","bracelet","bracelets","earrings","earring","ring","rings","anklet","anklets","charm","charms","pendant","pendants","hoop","hoops","stud","studs","chain","chains","choker","keychain","set","sets","brooch","pin"]);
const _TOK_MAT = new Set(["gold","silver","sterling","14k","18k","rose","filled","solid","plated","beady","beaded"]);
const _OPT_MAT_RE = /material|metal|finish/i;
const _OPT_PERS_RE = /engrav|personal|font|initial|letter|photo|birthstone|name|monogram|stamp/i;
/* ===================== Best Sellers live-sales bump (canonical Top-200 list) ==================
   The Top-200 best-sellers list (Brites_Editor_Meta/bestSellers, seeded from the CSV by
   shopifyEditor) is the single source of truth for what a "best seller" is. Membership is FIXED
   by the CSV; ongoing site sales only increment counts and re-rank WITHIN the list. No sales ->
   nothing changes. The order webhook calls bumpBestSellers on every paid order. */
function _bsNorm(x) { return String(x == null ? "" : x).replace(/\s+/g, " ").trim().toLowerCase(); }
// Pure: apply sold line items (sku/title/qty) to the rows; returns { rows, matched }. Ranks are
// recomputed by (CSV orders + live) desc, stable by prior rank. Unmatched items are IGNORED —
// the CSV alone decides membership.
function _bsApplySale(rows, items) {
  const bySku = {}, byTitle = {};
  (rows || []).forEach((r, i) => {
    (r.skus || []).forEach(sk => { const k = _bsNorm(sk); if (k && !(k in bySku)) bySku[k] = i; else if (k && (rows[bySku[k]].rank || 9999) > (r.rank || 9999)) bySku[k] = i; });
    const tk = _bsNorm(r.name); if (tk && !(tk in byTitle)) byTitle[tk] = i; else if (tk && (rows[byTitle[tk]].rank || 9999) > (r.rank || 9999)) byTitle[tk] = i;
  });
  let matched = 0;
  (items || []).forEach(it => {
    if (!it) return;
    const qty = Number(it.qty) || 1;
    let idx = null;
    const sk = _bsNorm(it.sku); if (sk && bySku[sk] != null) idx = bySku[sk];
    if (idx == null) { const tk = _bsNorm(it.title); if (tk && byTitle[tk] != null) idx = byTitle[tk]; }
    if (idx == null) return;
    rows[idx].live = (Number(rows[idx].live) || 0) + qty; matched++;
  });
  if (matched) {
    const order = rows.map((r, i) => [r, i]).sort((a, b) =>
      (((b[0].orders || 0) + (b[0].live || 0)) - ((a[0].orders || 0) + (a[0].live || 0))) || ((a[0].rank || 9999) - (b[0].rank || 9999)) || (a[1] - b[1]));
    order.forEach((x, i) => { x[0].rank = i + 1; });
  }
  return { rows, matched };
}
async function bumpBestSellers(items) {
  const f = fb(); if (!f || !Array.isArray(items) || !items.length) return { matched: 0 };
  const ref = f.db.collection("Brites_Editor_Meta").doc("bestSellers");
  try {
    let matched = 0;
    await f.db.runTransaction(async tx => {
      const snap = await tx.get(ref);
      if (!snap.exists) return; // list not seeded yet (editor seeds it) — never invent one here
      const data = snap.data() || {};
      if (!Array.isArray(data.rows) || !data.rows.length) return;
      const res = _bsApplySale(data.rows, items);
      matched = res.matched;
      if (matched) tx.set(ref, { rows: res.rows, lastSaleAt: Date.now() }, { merge: true });
    });
    return { matched };
  } catch (e) { return { matched: 0, error: e.message }; }
}

/* ===================== Per-listing sales counts (pure units-sold ranking) =====================
   "Top seller" here means ONE thing: how many units that listing has sold — no recency weighting,
   no other criteria. Shopify exposes no per-product sales field, so this aggregates real orders:
   PRIMARY   your Shopify orders (last 365d, cancelled excluded, capped at the most recent ~1,500
             orders to respect API limits — cap is surfaced in the source label when hit),
   FALLBACK  the app's own order log in Firestore (title-keyed, most recent ~1,000 orders),
   LAST      Shopify's BEST_SELLING sort order as fetched (labeled as such — never silently).
   Cached 7 days; ranking is applied client-side to the profiler's candidate pool. */
async function productSalesMap({ force } = {}) {
  const f = fb();
  if (!force && f) { try { const d = await f.db.collection(COL.state).doc("productSales").get();
    if (d.exists) { const x = d.data() || {}; if (x.v === 1 && x.at && (Date.now() - x.at) < 7 * 86400000 && x.byId && Object.keys(x.byId).length) return x; } } catch (e) {} }
  // PRIMARY: the canonical Top-200 best-sellers list (CSV baseline + live site sales). The ads
  // engine must respect the SAME definition of "best seller" as the website.
  try {
    if (f) {
      const d = await f.db.collection("Brites_Editor_Meta").doc("bestSellers").get();
      if (d.exists) {
        const rows = (d.data() || {}).rows;
        if (Array.isArray(rows) && rows.length) {
          const byId0 = {}, byTitle0 = {};
          rows.forEach(r => { const total = (Number(r.orders) || 0) + (Number(r.live) || 0);
            if (r.productId) byId0[r.productId] = Math.max(byId0[r.productId] || 0, total);
            const tk = String(r.name || "").trim().toLowerCase(); if (tk) byTitle0[tk] = Math.max(byTitle0[tk] || 0, total); });
          const out0 = { byId: byId0, byTitle: byTitle0, source: `the Top-200 best-sellers list (CSV baseline + live site sales, ${rows.length} listings)`, orders: null, at: Date.now(), v: 1 };
          if (f) { try { await f.db.collection(COL.state).doc("productSales").set(out0); } catch (e) {} }
          return out0;
        }
      }
    }
  } catch (e) {}
  const since = new Date(Date.now() - 365 * 86400000).toISOString().slice(0, 10);
  const byId = {}, byTitle = {}; let orders = 0, pages = 0, truncated = false;
  try {
    let after = null, more = true;
    while (more && pages < 30) { // 30 pages x 50 = most recent ~1,500 orders, bounded API cost
      pages++;
      const d = await shopifyGql(`{ orders(first: 50${after ? `, after: "${after}"` : ""}, query: "created_at:>=${since} -status:cancelled", sortKey: CREATED_AT, reverse: true) { pageInfo { hasNextPage endCursor } edges { node { lineItems(first: 12) { edges { node { quantity product { id title } } } } } } } }`);
      const conn = (d && d.orders) || {};
      (conn.edges || []).forEach(oe => { orders++;
        ((((oe.node || {}).lineItems || {}).edges) || []).forEach(le => {
          const li = le && le.node; if (!li || !li.product) return;
          const qty = Number(li.quantity) || 1;
          if (li.product.id) byId[li.product.id] = (byId[li.product.id] || 0) + qty;
          const t = String(li.product.title || "").trim().toLowerCase();
          if (t) byTitle[t] = (byTitle[t] || 0) + qty;
        });
      });
      const pi = conn.pageInfo || {};
      more = !!(pi.hasNextPage && pi.endCursor); after = pi.endCursor;
      if (more && pages >= 30) truncated = true;
    }
  } catch (e) {}
  let source = null;
  if (Object.keys(byId).length) {
    source = `units sold in your Shopify orders since ${since} (${orders} orders${truncated ? ", most recent only" : ""})`;
  } else {
    // fallback: the app's own order log (title-keyed)
    try {
      if (f) {
        const q = await f.db.collection(COL.orderLog).orderBy("ts", "desc").limit(1000).get();
        let n = 0;
        q.forEach(doc => { const x = doc.data() || {}; n++;
          const items = Array.isArray(x.items) && x.items.length ? x.items : ((x.products || []).map(t => ({ title: t, qty: 1 })));
          items.forEach(it => { const t = String((it && it.title) || "").trim().toLowerCase(); if (t) byTitle[t] = (byTitle[t] || 0) + (Number(it && it.qty) || 1); });
        });
        if (Object.keys(byTitle).length) source = `units sold in the app's order log (most recent ${n} orders)`;
      }
    } catch (e) {}
  }
  if (!source) return null; // no sales data anywhere -> profiler labels Shopify-sort fallback
  const out = { byId, byTitle, source, orders, at: Date.now(), v: 1 };
  if (f) { try { await f.db.collection(COL.state).doc("productSales").set(out); } catch (e) {} }
  return out;
}
// Units sold for one product: by Shopify id first, then by (lowercased) title.
function _salesOf(p, sm) {
  if (!p || !sm) return 0;
  if (p.id != null && sm.byId && sm.byId[p.id] != null) return Number(sm.byId[p.id]) || 0;
  const t = String(p.title || "").trim().toLowerCase();
  if (t && sm.byTitle && sm.byTitle[t] != null) return Number(sm.byTitle[t]) || 0;
  return 0;
}
// Stable re-rank by pure units sold (desc); without sales data the given order is preserved.
function _rankBySales(prods, sm) {
  if (!sm) return (prods || []).slice();
  return (prods || []).map((p, i) => [p, i]).sort((a, b) => (_salesOf(b[0], sm) - _salesOf(a[0], sm)) || (a[1] - b[1])).map(x => x[0]);
}

// Jewelry TYPE of a product: the explicit Shopify Type field when set (Brites maintains it),
// else inferred from the title. Types are the campaign-relevant axes inside a mixed collection —
// necklaces, beady necklaces, hoop/stud earrings, bracelets and charm-only listings have different
// buyers, materials and price points, so each must be profiled separately.
function _ptypeOf(p) {
  const explicit = String((p && p.productType) || "").trim();
  if (explicit) return explicit.slice(0, 34);
  const t = String((p && p.title) || "").toLowerCase();
  if (/charm only/.test(t)) return /earring/.test(t) ? "Earring Charm Only" : "Necklace Charm Only";
  if (/hoop/.test(t)) return "Hoop Earrings";
  if (/stud/.test(t)) return "Stud Earrings";
  if (/earring/.test(t)) return "Earrings";
  if (/bracelet/.test(t)) return "Bracelet";
  if (/anklet/.test(t)) return "Anklet";
  if (/keychain|key chain/.test(t)) return "Keychain";
  if (/\bring\b/.test(t)) return "Ring";
  if (/bead(y|ed)/.test(t) && /necklace|chain/.test(t)) return "Beady Necklace";
  if (/necklace|pendant/.test(t)) return "Necklace";
  return "Other";
}
// Aggregate PRODUCT OPTION structures across sampled listings: which material tiers the collection
// actually offers (e.g. sterling / 14k gold-filled / SOLID 14k gold) and which personalization
// options exist (engraving, birthstone, photo…) — invisible in titles, decisive for keywords.
function _optSummary(prods) {
  const mats = {}, pers = new Set();
  (prods || []).forEach(p => (p && p.options || []).forEach(o => {
    const nm = String((o && o.name) || "");
    if (_OPT_MAT_RE.test(nm)) (o.values || []).forEach(v => { const k = String(v || "").toLowerCase().trim().slice(0, 30); if (k) mats[k] = (mats[k] || 0) + 1; });
    else if (_OPT_PERS_RE.test(nm)) { const k = nm.toLowerCase().trim().slice(0, 30); if (k) pers.add(k); }
  }));
  return { materials: Object.keys(mats).sort((a, b) => mats[b] - mats[a]).slice(0, 6).map(k => ({ t: k, n: mats[k] })),
           personalization: [...pers].slice(0, 6) };
}
// Median price PER MATERIAL TIER from real variants (top sellers): "solid 14k gold ~$310" is a
// different campaign than "gold filled ~$68" — same collection, different buyer and intent.
function _matPrices(pricedProds) {
  const by = {};
  (pricedProds || []).forEach(p => ((((p || {}).variants || {}).edges) || []).forEach(ve => {
    const v = ve && ve.node; if (!v) return;
    const so = (v.selectedOptions || []).find(s => s && _OPT_MAT_RE.test(String(s.name || "")));
    if (!so) return;
    const key = String(so.value || "").toLowerCase().trim().slice(0, 30);
    const price = Number(v.price);
    if (key && isFinite(price) && price > 0) (by[key] = by[key] || []).push(price);
  }));
  return Object.keys(by).map(k => ({ t: k, price: Math.round(_median(by[k])) }))
    .filter(x => x.price > 0).sort((a, b) => a.price - b.price).slice(0, 5);
}
// Distill listing titles into ranked motif / product-type / material inventories with counts.
function _distill(titles) {
  const motifs = {}, types = {}, mats = {};
  (titles || []).forEach(t => {
    const seen = new Set(); // count each token once per listing so long titles don't dominate
    String(t || "").toLowerCase().split(/[^a-z0-9]+/).forEach(w => {
      if (!w || w.length < 3 || seen.has(w) || _TOK_STOP.has(w)) return;
      seen.add(w);
      if (_TOK_TYPE.has(w)) types[w] = (types[w] || 0) + 1;
      else if (_TOK_MAT.has(w)) mats[w] = (mats[w] || 0) + 1;
      else if (!/^\d+$/.test(w)) motifs[w] = (motifs[w] || 0) + 1;
    });
  });
  const rank = (o, n) => Object.keys(o).sort((a, b) => o[b] - o[a]).slice(0, n).map(k => ({ t: k, n: o[k] }));
  return { motifs: rank(motifs, 12), types: rank(types, 4), mats: rank(mats, 3) };
}
// (price median uses the existing _median helper; sampled prices are already filtered > 0)
async function collectionProfiles({ force, onPage = null } = {}) {
  const f = fb();
  if (!force && f) { try { const d = await f.db.collection(COL.state).doc("collectionProfiles").get();
    if (d.exists) { const x = d.data() || {}; if (x.v === 6 && x.at && (Date.now() - x.at) < 7 * 86400000 && Array.isArray(x.list) && x.list.length) return { list: x.list, at: x.at, salesBasis: x.salesBasis || null }; } } catch (e) {} }
  // Pure units-sold ranking for "top seller" (see productSalesMap). Null -> Shopify-sort fallback, labeled.
  let salesMap = null; try { salesMap = await productSalesMap({}); } catch (e) {}
  const salesBasis = salesMap ? salesMap.source : "Shopify best-selling sort (pure sales-count ranking unavailable this run)";
  /* PHASE 1 — wide, TYPE-AWARE sweep: 50 best-sellers + 20 newest per collection with productType,
     price and OPTION STRUCTURES on every product. Every jewelry type present in the collection is
     seen, counted, priced and materials-profiled — not just whichever type dominates the bestseller
     head. 3 collections/page keeps each query safely under Admin GraphQL cost limits. */
  const P1 = `{ edges { node { id title productType tags priceRangeV2 { minVariantPrice { amount } maxVariantPrice { amount } } options { name values } } } }`;
  const P1F = `{ edges { node { id title productType tags priceRangeV2 { minVariantPrice { amount } maxVariantPrice { amount } } } } }`;
  const Q = (after, variant) => `{ collections(first: 3${after ? `, after: "${after}"` : ""}) { pageInfo { hasNextPage endCursor } edges { node { handle title ${variant === 0 ? "productsCount { count } " : variant === 1 ? "productsCount " : ""}best: products(first: 50, sortKey: BEST_SELLING) ${P1} fresh: products(first: 20, sortKey: CREATED, reverse: true) ${P1F} } } } }`;
  let variant = -1, page = null;
  for (let v = 0; v < 3 && variant < 0; v++) { try { page = await shopifyGql(Q(null, v)); variant = v; } catch (e) {} }
  if (variant < 0 || !page) return null;
  const raw = []; let guard = 0;
  while (page && guard++ < 34) {
    const conn = page.collections || {};
    (conn.edges || []).forEach(e => {
      const n = e.node || {}; if (!n.handle) return;
      raw.push({ handle: n.handle, title: n.title, rawCount: n.productsCount,
        bestP: (((n.best || {}).edges) || []).map(pe => pe && pe.node).filter(Boolean),
        freshP: (((n.fresh || {}).edges) || []).map(pe => pe && pe.node).filter(Boolean) });
    });
    if (onPage) { try { onPage(raw.length); } catch (e) {} }
    const pi = conn.pageInfo || {};
    if (pi.hasNextPage && pi.endCursor && raw.length < 120) { try { page = await shopifyGql(Q(pi.endCursor, variant)); } catch (e) { page = null; } }
    else page = null;
  }
  if (!raw.length) return null;
  /* PHASE 2 — per-TYPE representatives for variant-level pricing: up to 2 top sellers of EACH
     jewelry type in each collection (types ranked by presence, max 10 reps/collection), fetched in
     batched node lookups with variants (price per material tier) + a bounded plain-text
     description. This is what makes "solid 14k gold hoop earrings ~$180" per-type knowledge instead
     of a collection-wide blur. Failures here degrade to Phase-1 data only — never block. */
  const wantIds = []; const repMeta = {};
  raw.forEach(c => {
    c.bestR = _rankBySales(c.bestP, salesMap); // pure units-sold order (falls back to fetched order)
    const byType = {};
    c.bestR.forEach(p => { const ty = _ptypeOf(p); (byType[ty] = byType[ty] || []).push(p); });
    const rankedTypes = Object.keys(byType).sort((a, b) => byType[b].length - byType[a].length);
    let taken = 0;
    rankedTypes.forEach(ty => {
      byType[ty].slice(0, 2).forEach(p => {
        if (taken >= 10 || !p.id) return;
        taken++; wantIds.push(p.id); repMeta[p.id] = { handle: c.handle, type: ty };
      });
    });
  });
  const nodeById = {};
  for (let i = 0; i < wantIds.length; i += 25) {
    const chunk = wantIds.slice(i, i + 25);
    try {
      const d = await shopifyGql(`{ nodes(ids: [${chunk.map(id => `"${id}"`).join(",")}]) { ... on Product { id description(truncateAt: 160) variants(first: 10) { edges { node { price selectedOptions { name value } } } } } } }`);
      ((d && d.nodes) || []).forEach(nd => { if (nd && nd.id) nodeById[nd.id] = nd; });
    } catch (e) { break; } // partial phase-2 is fine — profiles fall back to phase-1 data
  }
  /* Assemble per-collection profiles with a per-TYPE breakdown. */
  const list = raw.map(c => {
    const bestR = c.bestR || _rankBySales(c.bestP, salesMap);
    const seen = new Set(); const sample = [];
    bestR.concat(c.freshP).forEach(p => { const t = String(p.title || "").trim(); if (t && !seen.has(t.toLowerCase())) { seen.add(t.toLowerCase()); sample.push(p); } });
    const prices = []; let lo = null, hi = null;
    sample.forEach(p => { const r = p.priceRangeV2 || {};
      const a = Number(r.minVariantPrice && r.minVariantPrice.amount), b = Number(r.maxVariantPrice && r.maxVariantPrice.amount);
      if (isFinite(a) && a > 0) { lo = lo == null ? a : Math.min(lo, a); prices.push(a); }
      if (isFinite(b) && b > 0) hi = hi == null ? b : Math.max(hi, b); });
    const count = c.rawCount == null ? null : (typeof c.rawCount === "object" ? Number(c.rawCount.count) : Number(c.rawCount));
    // per-type detail: counts + price band from the wide sweep; materials from that type's OPTION
    // structures; per-material prices + descriptions from that type's Phase-2 representatives.
    const byType = {};
    sample.forEach(p => { const ty = _ptypeOf(p); (byType[ty] = byType[ty] || []).push(p); });
    const repsByType = {};
    Object.keys(repMeta).forEach(id => { const m = repMeta[id]; if (m.handle === c.handle && nodeById[id]) (repsByType[m.type] = repsByType[m.type] || []).push(nodeById[id]); });
    const descTexts = [];
    const typesDetail = Object.keys(byType).sort((a, b) => byType[b].length - byType[a].length).slice(0, 8).map(ty => {
      const prods = byType[ty];
      let tlo = null, thi = null; const tPrices = [];
      prods.forEach(p => { const r = p.priceRangeV2 || {};
        const a = Number(r.minVariantPrice && r.minVariantPrice.amount), b = Number(r.maxVariantPrice && r.maxVariantPrice.amount);
        if (isFinite(a) && a > 0) { tlo = tlo == null ? a : Math.min(tlo, a); tPrices.push(a); }
        if (isFinite(b) && b > 0) thi = thi == null ? b : Math.max(thi, b); });
      const opt = _optSummary(prods);
      const reps = repsByType[ty] || [];
      reps.forEach(r => { const d = String(r.description || "").trim(); if (d) descTexts.push(d); });
      const matP = _matPrices(reps);
      const priceBy = {}; matP.forEach(x => priceBy[x.t] = x.price);
      const materials = (opt.materials.length ? opt.materials : matP.map(x => ({ t: x.t, n: 1 })))
        .map(m => ({ t: m.t, n: m.n || 1, price: priceBy[m.t] != null ? priceBy[m.t] : null })).slice(0, 4);
      return { type: ty, n: prods.length, priceLow: tlo != null ? Math.round(tlo) : null,
        priceMed: tPrices.length ? Math.round(_median(tPrices)) : null, priceHigh: thi != null ? Math.round(thi) : null,
        materials, personalization: opt.personalization };
    });
    // Listing TAGS across the whole sample (best + fresh): the merchant's own search terms
    // (styles, recipients, occasions, materials) with frequencies — prime keyword-seed material
    // that titles alone miss.
    const tagCount = {};
    sample.forEach(p => (Array.isArray(p.tags) ? p.tags : []).forEach(t => { const k = String(t).trim(); if (k) tagCount[k] = (tagCount[k] || 0) + 1; }));
    const listingTags = Object.keys(tagCount).sort((a, b) => tagCount[b] - tagCount[a]).slice(0, 10).map(t => ({ t, n: tagCount[t] }));
    const inv = _distill(sample.map(p => p.title).concat(descTexts));
    const persAll = [...new Set(typesDetail.flatMap(t => t.personalization || []))].slice(0, 6);
    const med = _median(prices);
    return { handle: c.handle, title: c.title, count: isFinite(count) ? count : null, sampled: sample.length,
      priceLow: lo != null ? Math.round(lo) : null, priceMed: med != null ? Math.round(med) : null, priceHigh: hi != null ? Math.round(hi) : null,
      motifs: inv.motifs, types: inv.types, mats: inv.mats, listingTags,
      typesDetail, personalization: persAll,
      reps: [bestR[0] && (String(bestR[0].title).trim().slice(0, 60) + (salesMap && _salesOf(bestR[0], salesMap) > 0 ? ` (${_salesOf(bestR[0], salesMap)} sold)` : "")), c.freshP[0] && String(c.freshP[0].title).trim().slice(0, 60)].filter(Boolean) };
  });
  const builtAt = Date.now();
  if (f) { try { await f.db.collection(COL.state).doc("collectionProfiles").set({ list, at: builtAt, v: 6, salesBasis }); } catch (e) {} }
  return { list, at: builtAt, salesBasis };
}
// One compact prompt line per collection: count · price spread · ranked motif inventory (with
// frequencies, so the AI sees the collection's real composition) · types · materials · anchors.
function _profileText(profiles, collections) {
  const byH = {}; (profiles || []).forEach(p => { if (p && p.handle) byH[p.handle] = p; });
  const inv = arr => (arr || []).map(x => `${x.t}(${x.n})`).join(" ");
  return (collections || []).map(c => {
    const p = byH[c.handle];
    if (!p || ((!p.motifs || !p.motifs.length) && (!p.reps || !p.reps.length))) return `- ${c.title}`;
    const bits = [];
    if (p.count != null) bits.push(p.sampled >= p.count ? `${p.count} listings, fully scanned` : `${p.count} listings, ${p.sampled} sampled`);
    else if (p.sampled) bits.push(`${p.sampled} sampled`);
    if (p.priceLow != null && p.priceHigh != null) bits.push(`$${p.priceLow}\u2013$${p.priceHigh}${p.priceMed != null ? ` med $${p.priceMed}` : ""}`);
    const parts = [`- ${c.title}${bits.length ? ` (${bits.join("; ")})` : ""}`];
    if (p.motifs && p.motifs.length) parts.push(`motifs: ${inv(p.motifs)}`);
    if (p.listingTags && p.listingTags.length) parts.push(`listing tags: ${inv(p.listingTags)}`);
    if (p.personalization && p.personalization.length) parts.push(`personalization: ${p.personalization.join(", ")}`);
    if (p.reps && p.reps.length) parts.push(`anchors: ${p.reps.map(s => `"${s}"`).join("; ")}`);
    const head = parts.join(" \u00b7 ");
    // per-jewelry-type breakdown: each type in the collection with its share, price band and
    // material tiers (with real per-tier prices where variants were scanned)
    if (p.typesDetail && p.typesDetail.length) {
      const tline = p.typesDetail.map(t => {
        const mats = (t.materials || []).filter(m => m && m.t).map(m => m.t + (m.price != null ? ` ~$${m.price}` : "")).join(", ");
        const band = (t.priceLow != null && t.priceHigh != null) ? ` $${t.priceLow}\u2013$${t.priceHigh}` : "";
        return `${t.type} \u00d7${t.n}${band}${mats ? ` [${mats}]` : ""}`;
      }).join(" \u00b7 ");
      return head + `\n    types: ${tline}`;
    }
    if (p.types && p.types.length) return head + ` \u00b7 types: ${inv(p.types)}`;
    return head;
  }).join("\n");
}
// Sanitize the model's audience object (free text, bounded lengths, never trusted raw).
function _audNorm(a) {
  if (!a || typeof a !== "object") return null;
  const s = (v, n) => { const t = String(v || "").trim().slice(0, n); return t || null; };
  const out = { buyer: s(a.buyer, 70), recipient: s(a.recipient, 50), motivation: s(a.motivation, 90), searchStyle: s(a.searchStyle, 80) };
  return (out.buyer || out.recipient || out.motivation) ? out : null;
}

/* ===================== Occasion memory + AI suggestions ===================== */
function slugify(s) { return String(s || "").toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/^-|-$/g, "").slice(0, 60); }

// Called when a draft is created — remembers the occasion was used (+ its campaign tag).
async function recordOccasionUse(occasion, handle, tag) {
  const f = fb(); if (!f || !occasion) return;
  const slug = slugify(occasion); const ref = f.db.collection(COL.occasions).doc(slug);
  try {
    const s = await ref.get(); const x = s.exists ? s.data() : { occasion, slug, timesUsed: 0, collections: {}, tags: [], outcome: "untested" };
    x.occasion = occasion; x.timesUsed = (x.timesUsed || 0) + 1; x.lastUsed = f.FV.serverTimestamp();
    x.collections = x.collections || {}; if (handle) { x.collections[handle] = x.collections[handle] || { count: 0 }; x.collections[handle].count++; }
    x.tags = Array.isArray(x.tags) ? x.tags : []; if (tag && x.tags.indexOf(tag) < 0) x.tags.push(tag);
    await ref.set(x, { merge: true });
  } catch (e) {}
}

// Called from measure(): roll real campaign performance back onto each occasion,
// marking success/fail so future suggestions learn from outcomes.
async function attributeOccasionsFromSnapshot(snapshot) {
  const f = fb(); if (!f || !snapshot || !snapshot.length) return;
  const live = {};
  snapshot.forEach(c => { const m = /^BA · (.+)$/.exec(c.name || ""); if (m) live[m[1]] = { cost: +c.cost || 0, conv: +c.conv || 0, value: +c.value || 0 }; });
  if (!Object.keys(live).length) return;
  try {
    const target = (await control()).targetRoas || 2.5;
    const snap = await f.db.collection(COL.occasions).get();
    const batch = f.db.batch(); let any = false;
    snap.forEach(doc => {
      const x = doc.data(); const tags = x.tags || []; let cost = 0, conv = 0, value = 0;
      tags.forEach(t => { if (live[t]) { cost += live[t].cost; conv += live[t].conv; value += live[t].value; } });
      if (cost > 0) {
        const roas = value / cost;
        const outcome = roas >= target ? "success" : (cost >= 20 ? "fail" : "untested");
        batch.set(doc.ref, { agg: { spend: +cost.toFixed(2), conv, value: +value.toFixed(2), roas: +roas.toFixed(2) }, outcome, attributedAt: f.FV.serverTimestamp() }, { merge: true });
        any = true;
      }
    });
    if (any) await batch.commit();
  } catch (e) {}
}

// AI-generated, memory-weighted occasion suggestions for a collection. Cached 12h; force re-rolls.
async function suggestOccasions(handle, { force } = {}) {
  const f = fb(); const cacheKey = "occasions_" + (handle || "global");
  if (f && !force) {
    try {
      const s = await f.db.collection(COL.state).doc(cacheKey).get();
      if (s.exists) { const x = s.data(); if (x.at && (Date.now() - x.at) < 12 * 60 * 60 * 1000 && Array.isArray(x.list) && x.list.length) return x.list; }
    } catch (e) {}
  }
  let memory = [];
  if (f) {
    try { const snap = await f.db.collection(COL.occasions).get(); snap.forEach(d => { const x = d.data(); memory.push({ occasion: x.occasion, timesUsed: x.timesUsed || 0, outcome: x.outcome || "untested", roas: (x.agg && x.agg.roas) || null }); }); } catch (e) {}
  }
  const collTitle = handle ? (await collectionMeta(handle)).title : "the store";
  const dateStr = _acctDateYmd(await _accountTz().catch(() => "America/Toronto"));
  const memText = memory.length
    ? memory.map(m => `- ${m.occasion}: used ${m.timesUsed}x, outcome ${m.outcome}${m.roas ? `, ROAS ${m.roas}x` : ""}`).join("\n")
    : "(no history yet — nothing has run)";
  const prompt =
`Today is ${dateStr}. Plan Google Ads occasions for Brites, a handcrafted personalized charm-jewelry brand (gift- and emotion-led). Target collection: "${collTitle}".
Occasion memory (what we've run and how it did):
${memText}
Suggest 8-12 occasions/events to advertise over the NEXT ~90 DAYS from today, ranked best-first. Favor:
- timely seasonal/gifting moments genuinely upcoming within ~90 days of today,
- occasions fitting this collection's audience,
- occasions memory marks "success" (repeat the winners).
Avoid occasions memory marks "fail" or that are out of season right now. Always include an "Evergreen gifting" option.
Return ONLY JSON: {"occasions":[{"label":"","daysOut":<int>,"recommendation":"push|test|skip","proven":<bool>,"why":"<=90 chars"}]}`;
  let list = null;
  try { const j = await openaiJSON(prompt, { maxTokens: 1000 }); if (j && Array.isArray(j.occasions)) list = j.occasions.filter(o => o && o.label).slice(0, 12); } catch (e) {}
  if (!list || !list.length) list = OCCASIONS.map(o => ({ label: o, daysOut: null, recommendation: "test", proven: false, why: "" }));
  if (!list.some(o => /evergreen/i.test(o.label))) list.unshift({ label: "Evergreen gifting", daysOut: 0, recommendation: "test", proven: false, why: "always-on baseline" });
  if (f) { try { await f.db.collection(COL.state).doc(cacheKey).set({ list, at: Date.now() }); } catch (e) {} }
  return list;
}

/* ===================== Manual campaign enable / pause ===================== */
// Flip a single campaign ENABLED/PAUSED. Same update+updateMask shape as the
// (working) budget reallocation path, on the campaigns service. Honors dry-run.
async function setCampaignStatus(campaignId, status, { ctrl } = {}) {
  ctrl = ctrl || (await control());
  status = String(status || "").toUpperCase();
  if (status !== "ENABLED" && status !== "PAUSED" && status !== "REMOVED") throw new Error("status must be ENABLED, PAUSED, or REMOVED");
  const id = String(campaignId).replace(/\D/g, "");
  if (!id) throw new Error("missing campaign id");
  const resourceName = `customers/${CID}/campaigns/${id}`;
  // REMOVED is a terminal state reached via a remove operation — Google Ads rejects
  // an update of status=REMOVED ("Enum value 'REMOVED' cannot be used"). ENABLED/PAUSED
  // are valid status updates.
  const op = status === "REMOVED"
    ? { remove: resourceName }
    : { update: { resourceName, status }, updateMask: "status" };
  const res = await mutate("campaigns", [op], { ctrl, label: "setStatus:" + status });
  // mutate() uses partialFailure, so an operation Google Ads rejects returns HTTP 200
  // with partialFailureError. Surface it instead of falsely reporting success.
  if (res && res.partialFailureError) {
    const msg = (res.partialFailureError.message || JSON.stringify(res.partialFailureError)).slice(0, 400);
    throw new Error(`Google Ads rejected ${status} for campaign ${id}: ${msg}`);
  }
  return { ok: true, id, status, dryRun: !!ctrl.dryRun };
}

// "Start now": move a scheduled (PENDING) campaign's start date to today in the ACCOUNT'S
// timezone so Google Ads stops treating it as future-dated and lets it begin serving.
// Leaves the end date untouched (the window's end is preserved).
async function startCampaignNow(campaignId, { ctrl } = {}) {
  ctrl = ctrl || (await control());
  const id = String(campaignId).replace(/\D/g, "");
  if (!id) throw new Error("missing campaign id");
  const tz = await _accountTz();
  // Google Ads rejects a start_date_time in the past, and treats it as a full timestamp
  // (not just a date), so "today 00:00:00" fails by midday. Use the account's current
  // wall-clock + a 2-minute buffer (clock skew / processing) → effectively "starts now".
  const dt = _accountDateTime(tz, 2 * 60 * 1000);
  const op = { update: { resourceName: `customers/${CID}/campaigns/${id}`, startDateTime: dt }, updateMask: "start_date_time" };
  const res = await mutate("campaigns", [op], { ctrl, label: "startNow:" + id });
  if (res && res.partialFailureError) {
    const msg = (res.partialFailureError.message || JSON.stringify(res.partialFailureError)).slice(0, 400);
    throw new Error(`Google Ads rejected start-now for campaign ${id}: ${msg}`);
  }
  return { ok: true, id, startDate: `${dt.slice(0, 4)}-${dt.slice(4, 6)}-${dt.slice(6, 8)}`, startDateTime: dt, dryRun: !!ctrl.dryRun };
}

/* ===================== Location / country targeting ===================== */

// Full list of targetable COUNTRIES (geo target constants), cached in Firestore since it's static.
async function listCountries({ force } = {}) {
  const f = fb();
  if (!force && f) {
    try { const d = await f.db.collection(COL.state).doc("countries").get();
          if (d.exists && Array.isArray(d.data().list) && d.data().list.length) return d.data().list; } catch (e) {}
  }
  let list = [];
  try {
    const rows = await gaql(
      `SELECT geo_target_constant.id, geo_target_constant.name, geo_target_constant.country_code
       FROM geo_target_constant
       WHERE geo_target_constant.target_type = 'Country' AND geo_target_constant.status = 'ENABLED'`);
    list = rows.map(r => { const g = r.geoTargetConstant || {}; return { id: String(g.id), name: g.name, code: g.countryCode }; })
               .filter(c => c.id && c.name)
               .sort((a, b) => a.name.localeCompare(b.name));
  } catch (e) {}
  if (f && list.length) { try { await f.db.collection(COL.state).doc("countries").set({ list, at: f.FV.serverTimestamp() }); } catch (e) {} }
  return list;
}

// The country geo IDs a LIVE campaign currently targets (positive location criteria).
async function campaignCountries(campaignId) {
  const id = String(campaignId).replace(/\D/g, ""); if (!id) return [];
  try {
    const rows = await gaql(
      `SELECT campaign_criterion.criterion_id, campaign_criterion.location.geo_target_constant, campaign_criterion.negative
       FROM campaign_criterion
       WHERE campaign.id = ${id} AND campaign_criterion.type = 'LOCATION' AND campaign_criterion.status != 'REMOVED'`);
    return rows.map(r => {
      const cc = r.campaignCriterion || {};
      return { critId: String(cc.criterionId), geoId: String(((cc.location && cc.location.geoTargetConstant) || "").split("/").pop() || ""), negative: !!cc.negative };
    }).filter(x => x.geoId && !x.negative);
  } catch (e) { return []; }
}

// Set the exact set of target countries on a LIVE campaign: removes criteria no longer wanted,
// adds the new ones, leaves unchanged ones in place (so we never needlessly churn the campaign).
async function setCampaignCountries(campaignId, countryIds, { ctrl } = {}) {
  ctrl = ctrl || (await control());
  const id = String(campaignId).replace(/\D/g, ""); if (!id) throw new Error("missing campaign id");
  const want = [...new Set((countryIds || []).map(x => String(x).replace(/\D/g, "")).filter(Boolean))];
  if (!want.length) throw new Error("pick at least one country (a campaign can't target zero locations)");
  const campaignRes = `customers/${CID}/campaigns/${id}`;
  const existing = await campaignCountries(id);
  const have = new Set(existing.map(e => e.geoId));
  const ops = [];
  existing.forEach(e => { if (!want.includes(e.geoId)) ops.push({ campaignCriterionOperation: { remove: `customers/${CID}/campaignCriteria/${id}~${e.critId}` } }); });
  want.forEach(gid => { if (!have.has(gid)) ops.push({ campaignCriterionOperation: { create: { campaign: campaignRes, location: { geoTargetConstant: `geoTargetConstants/${gid}` } } } }); });
  if (!ops.length) return { ok: true, id, countries: want, unchanged: true, dryRun: !!ctrl.dryRun };
  const res = await mutateAll(ops, { ctrl, label: "setCountries:" + id });
  if (res && res.partialFailureError) {
    const msg = (res.partialFailureError.message || JSON.stringify(res.partialFailureError)).slice(0, 400);
    throw new Error(`Google Ads rejected country update for campaign ${id}: ${msg}`);
  }
  return { ok: true, id, countries: want, dryRun: !!ctrl.dryRun };
}

// Rewrite the target countries on a PENDING approval draft (before it's applied), by swapping the
// location criterion ops inside its stored payload. Lets the user choose countries at approval time.
async function setApprovalCountries(approvalId, countryIds) {
  const f = fb(); if (!f) throw new Error("no firestore");
  const ref = f.db.collection(COL.approvals).doc(approvalId);
  const snap = await ref.get(); if (!snap.exists) throw new Error("approval not found");
  const p = (snap.data() || {}).payload || {};
  const want = [...new Set((countryIds || []).map(x => String(x).replace(/\D/g, "")).filter(Boolean))];
  let ops = Array.isArray(p.mutateOperations) ? p.mutateOperations.slice() : [];
  let campRes = null;
  ops.forEach(o => { const c = o && o.campaignOperation && o.campaignOperation.create; if (c && c.resourceName) campRes = c.resourceName; });
  // drop existing positive location criterion ops, then append the chosen ones
  ops = ops.filter(o => { const c = o && o.campaignCriterionOperation && o.campaignCriterionOperation.create; return !(c && c.location && c.location.geoTargetConstant); });
  if (campRes) want.forEach(gid => ops.push({ campaignCriterionOperation: { create: { campaign: campRes, location: { geoTargetConstant: `geoTargetConstants/${gid}` } } } }));
  await ref.set({ payload: { ...p, mutateOperations: ops, countries: want } }, { merge: true });
  return { ok: true, id: approvalId, countries: want };
}

/* ===================== Manual budget control ===================== */
async function campaignBudgetRes(campaignId) {
  const id = String(campaignId).replace(/\D/g, "");
  const rows = await gaql(`SELECT campaign_budget.resource_name FROM campaign WHERE campaign.id = ${id} LIMIT 1`);
  const r = rows[0]; return r && r.campaignBudget && r.campaignBudget.resourceName;
}
async function setCampaignBudget(campaignId, dailyBudget, { ctrl, budgetRes } = {}) {
  ctrl = ctrl || (await control());
  const amt = Number(dailyBudget);
  if (!(amt > 0)) throw new Error("budget must be a positive number");
  if (ctrl.maxDailyBudgetTotal && amt > ctrl.maxDailyBudgetTotal)
    throw new Error(`budget ${CURRENCY}${amt} exceeds your account ceiling ${CURRENCY}${ctrl.maxDailyBudgetTotal}`);
  let res = budgetRes || await campaignBudgetRes(campaignId);
  if (!res) throw new Error("could not resolve this campaign's budget resource");
  const op = { update: { resourceName: res, amountMicros: micros(amt) }, updateMask: "amount_micros" };
  await mutate("campaignBudgets", [op], { ctrl, label: "setBudget:" + amt });
  return { ok: true, id: String(campaignId).replace(/\D/g, ""), budget: amt, dryRun: !!ctrl.dryRun };
}

/* ===================== Per-campaign AI optimization analysis ===================== */
async function latestSnapshotCampaign(campaignId) {
  const f = fb(); if (!f) return null;
  try {
    const mt = await f.db.collection(COL.metrics).orderBy("at", "desc").limit(1).get();
    let snap = null; mt.forEach(d => snap = d.data().snapshot);
    if (!snap) return null;
    const id = String(campaignId).replace(/\D/g, "");
    return snap.find(c => String(c.id) === id) || null;
  } catch (e) { return null; }
}
// Researches one campaign's real metrics and returns a structured optimization read.
// Honest like Google's own recommendations: if there isn't enough data, it says so.
async function analyzeCampaign(campaignId, { force } = {}) {
  const f = fb(); const ctrl = await control();
  const id = String(campaignId).replace(/\D/g, "");
  const cacheKey = "analysis_" + id;
  if (f && !force) {
    try { const s = await f.db.collection(COL.state).doc(cacheKey).get(); if (s.exists) { const x = s.data(); if (x.at && (Date.now() - x.at) < 6 * 60 * 60 * 1000 && x.analysis) return x.analysis; } } catch (e) {}
  }
  const c = await latestSnapshotCampaign(id);
  if (!c) return { score: null, status: "unknown", summary: "No snapshot for this campaign yet — run Measure first.", actions: [], campaignId: id, currency: CURRENCY };
  const roas = c.cost > 0 ? c.value / c.cost : null, ctr = c.impr > 0 ? c.clicks / c.impr * 100 : null, cpa = c.conv > 0 ? c.cost / c.conv : null;
  const target = ctrl.targetRoas || 0, ccy = CURRENCY;
  const enoughData = c.conv >= 15 || c.cost >= 50;
  const _convH = await conversionHealth().catch(() => ({ validated: false, healthy: false }));
  const convNote = _convH.validated ? "" :
    `\nCRITICAL: account conversion tracking is ${_convH.healthy ? "configured but has recorded no sales yet" : "NOT confirmed to be recording sales"}. Any ROAS/CPA above may be undercounted or zero for that reason — treat performance as UNVALIDATED. Do NOT recommend scaling on ROAS; if conversions are 0, prioritize verifying conversion tracking over campaign changes.`;
  const metrics = `status=${c.status}, dailyBudget=${ccy}${c.budget}, spend14d=${ccy}${c.cost}, impressions=${c.impr}, clicks=${c.clicks}, ctr=${ctr == null ? "n/a" : ctr.toFixed(2) + "%"}, conversions=${c.conv}, convValue=${ccy}${c.value}, roas=${roas == null ? "n/a" : roas.toFixed(2) + "x"}, cpa=${cpa == null ? "n/a" : ccy + cpa.toFixed(2)}`;
  const prompt =
`You are a senior Google Ads strategist optimizing a Search campaign for Brites, a handcrafted personalized charm-jewelry brand (gift/emotion-led). Currency ${ccy}. Account target ROAS: ${target || "unset (maximize value)"}. Account daily budget ceiling: ${ccy}${ctrl.maxDailyBudgetTotal}.
Campaign "${c.name}" — last 14 days: ${metrics}.${convNote}
Give an honest optimization assessment. If there isn't enough data to optimize responsibly (Google Smart Bidding generally needs ~15+ conversions), SAY SO and recommend gathering data rather than inventing changes. Otherwise recommend concrete, prioritized actions (budget, bidding, keywords, creative, or status).
Return ONLY JSON:
{"score": <0-100 optimization/health score>,
 "status": "<one of: not serving | learning | limited by budget | underperforming | healthy | scaling | insufficient data>",
 "summary": "<2 plain-language sentences>",
 "actions": [{"title":"<short>","detail":"<why + expected effect, <=140 chars>","type":"<budget|bid|status|keywords|creative|wait>","suggestedBudget": <number in ${ccy} or null>}]}`;
  let out = null;
  try { const j = await openaiJSON(prompt, { maxTokens: 800 }); if (j && j.summary) out = j; } catch (e) {}
  if (!out) {
    out = {
      score: enoughData ? 55 : 25,
      status: c.status === "PAUSED" ? "not serving" : (c.cost > 0 ? (roas != null && target && roas >= target ? "healthy" : "underperforming") : "learning"),
      summary: enoughData ? "Automated read from current metrics (AI analysis unavailable)." : "Not enough conversion data yet to optimize responsibly — let it gather conversions first.",
      actions: c.status === "PAUSED" ? [{ title: "Enable to start", detail: "Campaign is paused — enable it to begin serving and gathering data.", type: "status", suggestedBudget: null }] : []
    };
  }
  out.score = Math.max(0, Math.min(100, Number(out.score) || 0));
  out.actions = Array.isArray(out.actions) ? out.actions.slice(0, 5).map(a => ({
    title: String(a.title || "").slice(0, 70), detail: String(a.detail || "").slice(0, 160),
    type: ["budget", "bid", "status", "keywords", "creative", "wait"].indexOf(a.type) >= 0 ? a.type : "wait",
    suggestedBudget: a.suggestedBudget != null ? Math.max(1, Math.min(ctrl.maxDailyBudgetTotal || 9999, Number(a.suggestedBudget))) : null
  })) : [];
  out.campaignId = id; out.currency = ccy; out.generatedAt = Date.now();
  if (f) { try { await f.db.collection(COL.state).doc(cacheKey).set({ analysis: out, at: Date.now() }); } catch (e) {} }
  return out;
}

/* ===================== Opportunity engine (the planner) ===================== */
// Pulls the store's best-selling products (bounded) so the AI can reference real heroes.
async function fetchTopProducts() {
  const d = await shopifyGql(`{ products(first: 40, sortKey: BEST_SELLING) { edges { node { title handle tags } } } }`);
  return ((d.products && d.products.edges) || []).map(e => ({
    title: e.node.title, handle: e.node.handle,
    tags: Array.isArray(e.node.tags) ? e.node.tags.filter(Boolean) : []
  })).filter(p => p.title);
}

// THE big analysis: cross-reference all collections + best-sellers + calendar + memory
// → a ranked list of fully-specified campaign opportunities (budget, duration, keywords…).
// Cached 12h; force re-rolls. Stored in Firestore for recall.
/* Conversion-likelihood score (0-99) + urgency-blended rank, so the list can be ordered by
   "most likely to convert" with time-critical windows boosted. Inputs are the model\u2019s own
   market judgment (fit, demand), priority, and proven history \u2014 all already on the card. */
function _oppScore(o) {
  const fit = (o.market && Number(o.market.fit)) || 1;
  const pw = { high: 1.25, medium: 1.0, test: 0.8 }[o.priority] || 1.0;
  const dw = { rising: 1.12, steady: 1.0, fading: 0.85 }[(o.market && o.market.demand) || "steady"] || 1.0;
  const prov = o.proven ? 1.2 : 1.0;
  const score = Math.max(5, Math.min(99, Math.round(58 * fit * pw * dw * prov)));
  const uw = (o.daysOut <= 0) ? 1.12 : (o.daysOut <= 3) ? 1.06 : 1.0; // urgency boost
  return { score, rank: Math.round(score * uw * 10) / 10 };
}
async function scanOpportunities({ force, cacheOnly } = {}) {
  const f = fb(); const ctrl = await control();
  if (f && (cacheOnly || !force)) {
    try {
      const s = await f.db.collection(COL.state).doc("opportunities").get();
      if (s.exists) {
        const x = s.data();
        if (cacheOnly) return { opportunities: Array.isArray(x.list) ? x.list : [], scannedAt: x.at || null, scanning: !!x.scanning, lastError: x.lastError || null, lastErrorAt: x.lastErrorAt || null, progress: x.progress || null };
        if (x.at && (Date.now() - x.at) < 12 * 60 * 60 * 1000 && Array.isArray(x.list) && x.list.length) return { opportunities: x.list, scannedAt: x.at };
      } else if (cacheOnly) { return { opportunities: [], scannedAt: null, scanning: false }; }
    } catch (e) { if (cacheOnly) return { opportunities: [], scannedAt: null, scanning: false }; }
  }
  // ---- Scan pipeline wrapped so a failure ANYWHERE records WHY (readable via lastError) and ALWAYS
  // clears the scanning flag. Previously the background caller swallowed the error, leaving
  // scanning:true and stale opportunities on screen forever with no signal as to the cause.
  try {
  await _scanProg(3, "Reading catalog & memory");
  const collections = await getCollections({});
  let products = []; try { products = await fetchTopProducts(); } catch (e) {}
  let memory = [];
  if (f) { try { const snap = await f.db.collection(COL.occasions).get(); snap.forEach(d => { const x = d.data(); memory.push({ occasion: x.occasion, outcome: x.outcome || "untested", roas: (x.agg && x.agg.roas) || null, collections: Object.keys(x.collections || {}) }); }); } catch (e) {} }
  const dateStr = _acctDateYmd(await _accountTz().catch(() => "America/Toronto"));
  const ceiling = ctrl.maxDailyBudgetTotal || 100, ccy = CURRENCY;
  const collText = collections.map(c => c.title).join(", ");
  const prodText = products.length
    ? products.slice(0, 40).map(p => p.title + ((p.tags && p.tags.length) ? ` [tags: ${p.tags.slice(0, 6).join(", ")}]` : "")).join("; ")
    : "(not available)";
  const memText = memory.length ? memory.map(m => `${m.occasion} [${(m.collections || []).join("/")}]: ${m.outcome}${m.roas ? ` ${m.roas}x` : ""}`).join("; ") : "(no history yet — nothing has run)";
  const _convH = await conversionHealth().catch(() => ({ validated: false }));
  const convDirective = _convH.validated
    ? "CONVERSION TRACKING: LIVE and recording sales — ROAS/outcome history is reliable. Weight proven occasions heavily; you may recommend scaling winners."
    : "CONVERSION TRACKING: NOT YET RECORDING SALES — you have NO validated ROAS data. Do NOT label any occasion 'proven'; keep market.fit conservative (0.9-1.1 unless you have a strong product-level reason), keep recommendedDailyBudget at modest test levels, and favor low-risk bets over aggressive spend until conversions flow.";
  // Scan several REAL listings per collection so keywords/audience/fit are grounded in actual
  // products, not collection names. Cached 7d; degrades to titles-only if Shopify is unreachable.
  let profiles = null, profiledAt = null, salesBasis = null;
  await _scanProg(8, "Profiling collections", "50 best-sellers + 20 newest per collection, with listing tags");
  try { const _p = await collectionProfiles({ onPage: n => _scanProg(Math.min(30, 8 + n * 0.6), "Profiling collections", n + " collections scanned") }); if (_p && Array.isArray(_p.list) && _p.list.length) { profiles = _p.list; profiledAt = _p.at; salesBasis = _p.salesBasis; } } catch (e) {}
  await _scanProg(34, "Building the strategy brief", (profiles ? profiles.length + " collection profiles" : "titles only") + " \u00b7 " + products.length + " best-sellers");
  const collBlock = (profiles && profiles.length)
    ? `COLLECTIONS \u2014 each profiled from a STRATIFIED, TYPE-AWARE scan of its real listings (data as of ${_ymd(new Date(profiledAt || Date.now()))}; "top seller" = ${salesBasis || "Shopify best-selling sort"}; refreshed weekly) (50 best-sellers + 20 newest per collection, plus per-type representative variants and descriptions). Each collection line shows: motif inventory with per-motif listing counts \u00b7 listing-tag inventory (the merchant\u2019s own search terms, with counts) \u00b7 personalization options \u00b7 anchors \u00b7 then a "types:" breakdown of every JEWELRY TYPE the collection actually contains (Necklace, Beady Necklace, Hoop/Stud Earrings, Bracelet, Charm Only\u2026) with its share (\u00d7n), price band, and MATERIAL TIERS with real per-tier prices from live variants. Use ALL of it: high-frequency motifs are the collection's identity (head terms); MID-frequency motifs are underexploited long-tail keyword material; the TYPE breakdown tells you which product types to build keywords around and in what proportion \u2014 a collection that is mostly necklaces with some hoop earrings and charm-only listings earns keywords across those types, weighted by share, and NEVER keywords for a type it doesn't contain; MATERIAL TIERS are distinct keyword axes with different buyers and intent ("solid 14k gold X" is a premium keepsake purchase at that tier's real price, "gold filled X" is the affordable tier \u2014 never blur them, never promise a tier, type or price the inventory doesn't show); PERSONALIZATION options (engraving, birthstone, photo\u2026) are high-intent keyword modifiers. Ground every keyword, phrase, audience and fit judgment in this inventory, never in the collection name alone:\n${_profileText(profiles, collections)}`
    : `ALL COLLECTIONS: ${collText}`;
  const prompt =
`Today: ${dateStr}. You are the campaign strategist for Brites, a handcrafted personalized charm-jewelry brand (gift/emotion-led). Currency ${ccy}. Total daily ad ceiling ${ccy} ${ceiling}.
${convDirective}
${collBlock}
TOP-SELLING PRODUCTS: ${prodText}
PAST OCCASION PERFORMANCE (memory): ${memText}
Find the 8-12 best advertising OPPORTUNITIES to act on within the NEXT ~30 DAYS. Do NOT suggest anything whose run window starts more than ~30 days from today — near-term relevance only. Cross-reference upcoming calendar / seasonal gifting moments with the collections and best-sellers that fit them and with past performance. For EACH opportunity return:
- collectionTitle (MUST be exactly one of the collections listed above)
- occasion (the event/emotion to lead with)
- startDate (YYYY-MM-DD: when the campaign should START — a sensible lead time before the occasion's peak so it can ramp; today or later, and within ~30 days)
- endDate (YYYY-MM-DD: when it should STOP — at or shortly after the peak; for "Evergreen gifting" use a ~30-day rolling window from startDate)
- daysOut (int: days from today until startDate; 0 if it should start now)
- priority: "high" (timely + strong fit, or proven winner), "medium" (solid), "test" (speculative)
- recommendedDailyBudget (number in ${ccy}; scale to importance — larger for major gifting events, smaller for niche/evergreen; keep realistic vs the ${ceiling} ceiling)
- market: {"fit": number 0.75-1.25 (multiplier on the store's measured conversion rate for THIS collection \u00d7 occasion: >1.0 when the pairing is gift-urgent, emotionally loaded, or matches proven best-sellers; <1.0 when it's browsy, generic, or a stretch fit; 1.0 when neutral \u2014 be honest, most should sit 0.9-1.1), "fitWhy": <=110 chars grounding the multiplier in THIS store's products/buyers, "demand": "rising"|"steady"|"fading" (search & gifting demand heading into the run window), "angle": <=90 chars the single best-converting ad angle}
  (Do NOT estimate ROAS \u2014 it is computed from real CPC, budget, and store data. Your job is the market judgment the raw numbers can't see.)
- proven (bool: true ONLY if memory shows success for this occasion)
- rationale (<=120 chars: why now, why this collection)
- keywords: an array of 5-8 RESEARCHED keyword objects. MIX broad "head" terms with specific "long-tail" phrases, DRAWN FROM the collection's motif inventory AND ITS LISTING TAGS (the tags are the merchant's own search terms \u2014 styles, recipients, occasions, materials \u2014 and often ARE the phrases shoppers type; fold the relevant ones for this occasion into keyword texts) \u2014 head terms from its high-frequency motifs, long-tail from mid-frequency motifs \u00d7 product types \u00d7 the occasion (a collection with bunny(31) and axolotl(6) earns both "bunny necklace" AND "axolotl charm gift") \u2014 phrased the way the audience below actually searches. DIFFERENTIATE the numbers per keyword and per opportunity (do not reuse the same figures). Each object:
    {"text": phrase a shopper would search,
     "searches": realistic estimated AVERAGE MONTHLY Google searches in the target countries (broad head terms in the hundreds-to-thousands; niche/long-tail 10-300; reflect how popular THIS exact phrase really is),
     "competition": "LOW" | "MEDIUM" | "HIGH" (long-tail/niche usually LOW; broad jewelry/gift terms HIGH),
     "cpcLow": realistic LOW top-of-page CPC in ${ccy}, "cpcHigh": realistic HIGH top-of-page CPC in ${ccy} (2025-26 retail-jewelry search runs ~$0.30-$3.50; long-tail cheaper, broad or gifting-peak terms pricier),
     "intent": "high" (ready to buy) | "medium" | "low",
     "tail": "HEAD" (1-2 words) | "MID" (3 words) | "LONG" (4+ words, specific)}
- keywordStrategy: <=180 chars explaining why THIS keyword mix for THIS collection+occasion (the head vs long-tail balance, buyer intent, and why more or fewer terms)
- keyPhrases (3-4 short emotional ad phrases speaking directly to the audience's motivation)
- audience: {"buyer": <=70 chars WHO is typing the search and paying \u2014 usually the gift-giver, be specific (e.g. "team parents at season end", "moms of teen daughters"), "recipient": <=50 chars who receives it, "motivation": <=90 chars the emotional driver of the purchase, "searchStyle": <=80 chars how THIS buyer actually phrases searches}
INTERPLAY (critical): audience \u00d7 occasion timing \u00d7 motif inventory must agree \u2014 keywords are what THIS buyer types in THIS window for the motifs/types/price band this collection actually contains; market.fit reflects inventory-level fit (price point, motif breadth, giftability), never the collection name alone. If the window is short, weight urgent/ready-to-buy phrasing; if the listings skew premium, weight quality/keepsake phrasing.
Only include opportunities genuinely relevant within ~30 days. Rank best-first (soonest + strongest first). Avoid out-of-season occasions and any memory marks as fail. Return ONLY JSON: {"opportunities":[ ... ]}`;
  let list = null, llmErr = null;
  // Reasoning models spend hidden reasoning tokens FROM max_completion_tokens before emitting any
  // JSON — at effort "high" on this large a prompt, a 9k budget was fully consumed by reasoning
  // alone ("finish_reason: length, 0 chars"). So: a much bigger budget, and if high effort still
  // starves the output, retry once at medium effort (far less reasoning burn) instead of failing.
  const _llmLadder = [{ maxTokens: 24000, effort: "high" }, { maxTokens: 24000, effort: "medium" }];
  let _rungNo = 0;
  for (const _rung of _llmLadder) {
    _rungNo++;
    await _scanProg(_rungNo === 1 ? 38 : 46, "AI strategist reasoning", _rungNo === 1 ? "deep pass (high effort) \u2014 the long step" : "retry at standard effort");
    try {
      const j = await openaiJSON(prompt, _rung);
      if (j && Array.isArray(j.opportunities)) { list = j.opportunities.filter(o => o && o.collectionTitle && o.occasion); llmErr = null; }
      else llmErr = "model returned no opportunities array";
    } catch (e) { llmErr = (e && e.message) || "AI scan failed"; }
    if (list && list.length) break;
  }
  if (!list || !list.length) {
    // The scan FAILED (LLM error or nothing usable) — do not pretend otherwise. Keep the old list
    // and its ORIGINAL timestamp (no `at` bump), record WHY in lastError for the console, and
    // return whatever we previously had so the UI isn't empty.
    let prevList = [], prevAt = null;
    if (f) {
      try {
        const s2 = await f.db.collection(COL.state).doc("opportunities").get();
        if (s2.exists) { const x2 = s2.data(); prevList = Array.isArray(x2.list) ? x2.list : []; prevAt = x2.at || null; }
      } catch (e) {}
      try { await f.db.collection(COL.state).doc("opportunities").set({ scanning: false, lastError: llmErr || "no opportunities returned", lastErrorAt: Date.now(), progress: null }, { merge: true }); } catch (e) {}
    }
    return { opportunities: prevList, scannedAt: prevAt, lastError: llmErr || "no opportunities returned", lastErrorAt: Date.now() };
  }
  const byTitle = {}; collections.forEach(c => byTitle[c.title.toLowerCase()] = c.handle);
  const today0 = _todayUtc();
  let _enabled = 0; try { _enabled = await _enabledBudgetTotal(); } catch (e) {}
  const headroom = Math.max(0, ceiling - _enabled);
  // Real store AOV (from logged Shopify orders) so projected revenue uses YOUR numbers, not a guess.
  let aov = 0; try { const sig = await storeSignals({ days: 120 }); const rev = (sig.adRevenue || 0) + (sig.organicRevenue || 0); if (sig.orders > 0) aov = _r2(rev / sig.orders); } catch (e) {}
  // Computed conversion rate (account history shrunk toward the benchmark) — one fetch, used by every plan.
  let cvrInfo = null; try { cvrInfo = await accountCvr(); } catch (e) {}
  const geoIds = (Array.isArray(ctrl.defaultCountries) && ctrl.defaultCountries.length) ? ctrl.defaultCountries : ["2124"];
  // Real Keyword Planner data — but Keyword Planner is rate-limited to ~1 req/sec, so we do NOT
  // fire one call per opportunity. We collect every opportunity's unique seeds, run ONE batched +
  // cached pool (serial chunks, backoff, stops on 429), then hand each opportunity its own slice.
  const _oppTexts = o => (Array.isArray(o.keywords) ? o.keywords : []).map(k => typeof k === "string" ? k : (k && k.text)).filter(Boolean);
  const allSeeds = [...new Set(list.flatMap(o => _oppTexts(o).map(s => String(s).toLowerCase())))];
  await _scanProg(56, "AI proposed " + list.length + " opportunities", allSeeds.length + " unique keyword seeds to research");
  const pool = await keywordResearchPool(allSeeds, geoIds, { onChunk: (i, n, got) => _scanProg(58 + (i - 1) / Math.max(1, n) * 22, "Google Keyword Planner", "batch " + i + "/" + n + " \u00b7 " + got + " phrases with live data") }).catch(e => ({ ok: false, error: e && e.message, status: null, ideasByText: {} }));
  await _scanProg(82, "Costing & ranking plans", "budgets, CPC caps, projected sales per opportunity");
  list = list.map((o, i) => {
    const t = String(o.collectionTitle).toLowerCase();
    const handle = byTitle[t] || (collections.find(c => c.title.toLowerCase().indexOf(t) >= 0) || {}).handle || null;
    const mem = memory.find(m => m.occasion && String(m.occasion).toLowerCase() === String(o.occasion).toLowerCase());
    // Pull this opportunity's own seeds out of the shared pool; merge real data OVER the model's research.
    // Real data OVER the model's research. Exact-seed matches first (a seed Keyword Planner returned
    // with data). But KP only returns a keyword when it HAS data, so niche/long-tail opportunity seeds
    // usually aren't returned verbatim — leaving an opp with zero real ideas and a silent AI-estimate
    // fallback even though the pool call succeeded. So when there's no exact match, we supplement with
    // the RELATED ideas KP DID return for this opp's DISTINCTIVE terms (its theme words, not the generic
    // "necklace/charm/gift" tokens shared by every opp). Still real market data, correctly scoped.
    let oppIdeas = pool.ok ? _oppTexts(o).map(x => pool.ideasByText[String(x).toLowerCase()]).filter(Boolean) : [];
    if (pool.ok && !oppIdeas.length) {
      const _GEN = new Set(["necklace","necklaces","charm","charms","jewelry","jewellery","pendant","pendants","gift","gifts","for","the","and","with","personalized","personalised","custom","dainty","tiny","mini"]);
      const distinctive = new Set(_oppTexts(o).flatMap(s => String(s).toLowerCase().split(/\s+/)).filter(w => w.length > 2 && !_GEN.has(w)));
      if (distinctive.size) {
        oppIdeas = Object.values(pool.ideasByText).filter(idea => String(idea.text || "").toLowerCase().split(/\s+/).some(t => distinctive.has(t))).slice(0, 20);
      }
    }
    const kpForOpp = pool.ok ? { ok: true, ideas: oppIdeas, status: pool.status } : { ok: false, error: pool.error, status: pool.status };
    const merged = mergeKeywordResearch(o.keywords, kpForOpp);
    merged.error = pool.ok ? null : (pool.error || null);
    merged.strategy = o.keywordStrategy || null;
    const peakDate = o.endDate || o.startDate || _nextOccasionPeak(o.occasion);
    const mkt = _mktNorm(o.market);
    const plan = planCampaign({ title: o.collectionTitle, occasion: o.occasion, peakDate, ceiling, headroom, smartBidding: !!ctrl.smartBidding, research: merged, aov, cvrInfo, market: mkt });
    const startDate = plan.duration.startDate, endDate = plan.duration.endDate, durationDays = plan.duration.days;
    const bud = plan.budget.daily, maxCpc = plan.cpc.max;
    const daysOut = Math.max(0, _daysBetween(today0, _parseYmd(startDate)));
    const kws = merged.keywords.map(k => k.text).slice(0, 8);
    return {
      id: "op" + i, collectionHandle: handle, collectionTitle: o.collectionTitle, occasion: o.occasion,
      startDate, endDate, daysOut, durationDays, maxCpc, plan,
      priority: (["high", "medium", "test"].indexOf(o.priority) >= 0 ? o.priority : "test"),
      recommendedDailyBudget: bud, estTotalSpend: plan.expected.spendTotal,
      expectedRoasBand: plan.expectedRoas ? plan.expectedRoas.band : null, expectedRoas: plan.expectedRoas || null,
      market: mkt, audience: _audNorm(o.audience), proven: !!o.proven,
      rationale: o.rationale || "", keywords: kws, keywordData: merged.keywords,
      research: { source: merged.source, error: merged.error, realCount: merged.realCount,
        searchVolume: merged.searchVolume, competitionIndex: merged.competitionIndex, cpc: merged.cpc,
        longTailRatio: merged.longTailRatio, headCount: merged.headCount, longCount: merged.longCount,
        strategy: merged.strategy, keywordCount: merged.keywords.length },
      keyPhrases: Array.isArray(o.keyPhrases) ? o.keyPhrases.slice(0, 4) : [],
      pastStats: mem && mem.roas ? { roas: mem.roas, outcome: mem.outcome } : null, currency: ccy
    };
  }).filter(o => o.collectionHandle)
    .filter(o => o.daysOut <= 32)   // ~30-day forward window: drop anything that starts too far out
    .map(o => { const sc = _oppScore(o); o.score = sc.score; o.rank = sc.rank; return o; })
    // Default order = the blend the console shows: conversion likelihood boosted by urgency.
    .sort((a, b) => b.rank - a.rank);
  // Persist the fresh list. If THIS write throws (commonly: the doc exceeds Firestore's 1 MiB limit
  // because keywordData/plan bloat the payload), it was previously swallowed — leaving stale data AND a
  // stuck scanning flag. Now a write failure retries with a trimmed payload and is always recorded.
  await _scanProg(96, "Saving " + list.length + " ranked opportunities");
  if (f && list.length) {
    try { await f.db.collection(COL.state).doc("opportunities").set({ list, at: Date.now(), scanning: false, lastError: null, lastErrorAt: null, progress: null }); }
    catch (e) {
      try {
        const slim = list.map(o => { const c = Object.assign({}, o); delete c.keywordData; return c; }); // drop heavy per-keyword metrics
        await f.db.collection(COL.state).doc("opportunities").set({ list: slim, at: Date.now(), scanning: false, lastError: "write trimmed (payload too large): " + (e && e.message), lastErrorAt: Date.now(), progress: null });
      } catch (e2) {
        try { await f.db.collection(COL.state).doc("opportunities").set({ scanning: false, lastError: "WRITE FAILED: " + (e2 && e2.message), lastErrorAt: Date.now(), progress: null }, { merge: true }); } catch (e3) {}
      }
    }
  }
  else if (f) { try { await f.db.collection(COL.state).doc("opportunities").set({ scanning: false, lastError: "all opportunities filtered out (no matching collections / all out of window)", lastErrorAt: Date.now(), progress: null }, { merge: true }); } catch (e) {} }
  return { opportunities: list, scannedAt: Date.now() };
  } catch (scanErr) {
    // ANY failure in the scan pipeline: record it (console-readable via lastError) and ALWAYS clear
    // scanning so the UI stops showing stale data. This is the safety net that was missing.
    if (f) { try { await f.db.collection(COL.state).doc("opportunities").set({ scanning: false, lastError: (scanErr && scanErr.message) || String(scanErr), lastErrorStack: ((scanErr && scanErr.stack) || "").slice(0, 600), lastErrorAt: Date.now(), progress: null }, { merge: true }); } catch (e) {} }
    return { opportunities: [], scannedAt: null, error: (scanErr && scanErr.message) || String(scanErr) };
  }
}

/* ============ Opportunity ↔ approval ↔ campaign reconciliation ============ */
// The join key is the campaign tag: `{handle}-{slug(occasion)}` (sliced to 40),
// identical to what buildSearchCampaignOps stamps onto `BA · {tag}`. So an
// opportunity, its queued/approved approval, and the live campaign all share one tag.
function oppTag(handle, occasion) {
  const lbl = (occasion && occasion !== "Evergreen gifting") ? occasion : "evergreen";
  return `${handle}-${String(lbl).toLowerCase().replace(/[^a-z0-9]+/g, "-")}`.slice(0, 40);
}
function tagFromCampaignName(name) {
  const m = /^BA · (.+)$/.exec(name || ""); return m ? m[1].slice(0, 40) : null;
}
function approvalTag(x) {
  const ops = (x && x.payload && x.payload.mutateOperations) || [];
  for (const op of ops) {
    const c = op.campaignOperation && op.campaignOperation.create;
    if (c && c.name) { const t = tagFromCampaignName(c.name); if (t) return t; }
  }
  return (x && x.tag) || null;
}
const _AP_RANK = { REJECTED: 0, PENDING: 1, APPROVED: 2, APPLIED: 3 };
const _CAMP_RANK = { REMOVED: 0, PAUSED: 1, ENABLED: 2 };
// tag -> { where:"approval"|"campaign", status, approvalId?, campaignId? }
// A live campaign is ground truth and overrides any approval record for that tag.
async function takenTags() {
  const map = {}; const f = fb();
  if (f) {
    try {
      const ap = await f.db.collection(COL.approvals).limit(300).get();
      ap.forEach(d => {
        const x = d.data(); const tag = approvalTag(x); if (!tag) return;
        const cur = map[tag];
        if (!cur || (_AP_RANK[x.status] || 0) >= (_AP_RANK[cur.status] || 0))
          map[tag] = { where: "approval", status: x.status, approvalId: d.id };
      });
    } catch (e) {}
  }
  try {
    const rows = await gaql(`SELECT campaign.id, campaign.name, campaign.status, campaign.primary_status, campaign.primary_status_reasons FROM campaign`);
    const campMap = {};
    rows.forEach(r => {
      const tag = tagFromCampaignName(r.campaign.name); if (!tag) return;
      const cur = campMap[tag]; const st = r.campaign.status;
      if (!cur || (_CAMP_RANK[st] || 0) >= (_CAMP_RANK[cur.status] || 0))
        campMap[tag] = { where: "campaign", status: st, campaignId: r.campaign.id,
          primaryStatus: r.campaign.primaryStatus || null, primaryStatusReasons: r.campaign.primaryStatusReasons || [] };
    });
    Object.keys(campMap).forEach(tag => { map[tag] = campMap[tag]; }); // campaigns override approvals
  } catch (e) {}
  return map;
}
// Opportunities annotated with their current real state, so the UI can split
// "unused" from "already acted on" and never re-suggest a taken campaign.
async function opportunitiesWithStatus({ force, cacheOnly } = {}) {
  const r = await scanOpportunities({ force, cacheOnly });
  let taken = {};
  try { taken = await takenTags(); } catch (e) {}
  // Dates are normalized at SERVE time: an opportunity may have been scanned up to 12h ago (or be
  // a stale list kept after a failed re-scan), so clamp startDate to today, recompute daysOut, and
  // drop anything whose whole window has passed — the console must never suggest starting a
  // campaign in the past.
  const today0 = _todayUtc();
  const todayYmd = _acctDateYmd(await _accountTz().catch(() => "America/Toronto"));
  const opportunities = (r.opportunities || []).map(o => {
    const tag = oppTag(o.collectionHandle, o.occasion);
    const out = Object.assign({}, o, { tag, acted: taken[tag] || null });
    const endD = _parseYmd(out.endDate);
    if (endD && endD < today0) { out._expired = true; return out; }
    const startD = _parseYmd(out.startDate);
    if (startD && startD < today0) {
      out.startDate = todayYmd;
      out.daysOut = 0;
      const days = endD ? (_daysBetween(today0, endD) + 1) : out.durationDays;
      out.durationDays = Math.max(1, days || 1);
      if (out.plan && out.plan.duration) {
        out.plan = Object.assign({}, out.plan, { duration: Object.assign({}, out.plan.duration, { startDate: todayYmd, days: out.durationDays }) });
      }
    } else if (startD) {
      out.daysOut = Math.max(0, _daysBetween(today0, startD));
    }
    return out;
  })
  // Expired windows are dead; archived campaigns are terminal: drop both from every list
  // (not "in use", not re-suggested as "unused"). Live/paused/approval states stay,
  // shown with their actual status.
  .filter(o => !o._expired)
  .filter(o => !(o.acted && o.acted.where === "campaign" && o.acted.status === "REMOVED"));
  return { opportunities, scannedAt: r.scannedAt, scanning: !!r.scanning, taken, lastError: r.lastError || r.error || null, lastErrorAt: r.lastErrorAt || null, progress: r.progress || null };
}

/* ===================== Force-generate (Draft Bench) ===================== */
// Build a real draft for ANY collection × occasion, regardless of calendar date,
// and queue it for approval. This is the live path behind the console's Bench.
async function collectionMeta(handle) {
  const cal = await loadCalendar();
  const fromCal = cal[handle];
  const known = COLLECTIONS.find(c => c.handle === handle);
  const title = (fromCal && fromCal.title) || (known && known.title) ||
                handle.replace(/-/g, " ").replace(/\b\w/g, c => c.toUpperCase());
  return { handle, title, heroProducts: fromCal && fromCal.heroProducts,
           reviewProof: (fromCal && fromCal.reviewProof) || "thousands of 5-star reviews" };
}

async function generateForCollection(handle, eventLabel, budget, { ctrl, startDate, endDate, countries, maxCpc, peakDate, smartBidding } = {}) {
  ctrl = ctrl || (await control());
  if (!handle) return { ok: false, reason: "no collection given" };
  const coll = await collectionMeta(handle);
  const event = (eventLabel && eventLabel !== "Evergreen gifting") ? { label: eventLabel, angle: "" } : null;
  const assets = await generateRSAAssets(coll, event);
  if (!assets) return { ok: false, reason: "generation rejected — copy failed brand-safety or fell under RSA minimums" };
  // Research-grounded plan: gives a custom build the SAME costed treatment as a scanned one —
  // a learning-aware run length, a CPC cap, and a budget that fits the ceiling — even when the
  // console sends nothing but collection + occasion. Explicit values from the caller win.
  let _enabled = 0; try { _enabled = await _enabledBudgetTotal(); } catch (e) {}
  const ceiling = ctrl.maxDailyBudgetTotal || 100;
  const smart = (smartBidding != null) ? !!smartBidding : !!ctrl.smartBidding;
  // Same real treatment as a scanned opportunity: Keyword Planner CPC/demand + real store AOV.
  const _geo = (countries && countries.length) ? countries : ((Array.isArray(ctrl.defaultCountries) && ctrl.defaultCountries.length) ? ctrl.defaultCountries : ["2124"]);
  const _seeds = [coll.title, `${coll.title} gift`, `${coll.title} necklace`, (event ? `${coll.title} ${event.label}` : null)].filter(Boolean);
  let _res = null; try { _res = await researchOpportunity(_seeds, _geo); } catch (e) {}
  let _aov = 0; try { const sig = await storeSignals({ days: 120 }); const rev = (sig.adRevenue || 0) + (sig.organicRevenue || 0); if (sig.orders > 0) _aov = _r2(rev / sig.orders); } catch (e) {}
  let _cvrInfo = null; try { _cvrInfo = await accountCvr(); } catch (e) {}
  const plan = planCampaign({ title: coll.title, occasion: eventLabel, peakDate, ceiling, headroom: Math.max(0, ceiling - _enabled), smartBidding: smart, research: (_res && _res.ok ? _res : null), aov: _aov, cvrInfo: _cvrInfo });
  const dailyBudget = Number(budget) > 0 ? Number(budget) : plan.budget.daily;
  const sDate = startDate || plan.duration.startDate;
  const eDate = endDate || plan.duration.endDate;
  const capCpc = Number(maxCpc) > 0 ? Number(maxCpc) : plan.cpc.max;
  // Default target countries (so a draft never silently launches to "all countries"). Falls back
  // to the saved control default, then Canada (2124) — the brand's home market.
  let cty = (countries && countries.length) ? countries
          : (Array.isArray(ctrl.defaultCountries) && ctrl.defaultCountries.length ? ctrl.defaultCountries : ["2124"]);
  cty = [...new Set(cty.map(x => String(x).replace(/\D/g, "")).filter(Boolean))];
  const { ops, tag, negatives, assetSummary } = buildSearchCampaignOps(coll, event, assets, { dailyBudget, startDate: sDate, endDate: eDate, countries: cty, maxCpc: capCpc, smartBidding: smart, targetRoas: Number(ctrl.targetRoas || 0) });
  await recordOccasionUse(event ? event.label : "Evergreen gifting", coll.handle, tag);
  const win = (sDate && eDate) ? ` (${sDate} → ${eDate}, ${plan.duration.days}d)` : "";
  const bidTxt = smart ? "Smart Bidding (no CPC cap)" : `Manual CPC ≤ ${CURRENCY} ${capCpc.toFixed(2)}/click`;
  const assetTxt = assetSummary ? `, ${assetSummary.sitelinks} sitelinks + ${assetSummary.callouts} callouts` : "";
  const id = await enqueueApproval({
    type: "creative", vetted: false,
    summary: `NEW Search campaign “${tag}”${event ? ` for ${event.label}` : ""}${win} — ${bidTxt}, ${assets.headlines.length} headlines${assetTxt}, ${negatives.length} negatives, starts PAUSED (drafted on the Bench)`,
    payload: { mutateOperations: ops, finalCollection: coll.handle, event: event ? event.label : null, startDate: sDate || null, endDate: eDate || null, countries: cty, maxCpc: capCpc, smartBidding: smart, negatives, assetSummary, plan },
    experimentId: tag
  });
  return { ok: true, approvalId: id, tag, title: coll.title, event: event ? event.label : null,
           budget: dailyBudget, maxCpc: capCpc, smartBidding: smart, startDate: sDate, endDate: eDate, plan, currency: CURRENCY, countries: cty, assets, negatives, assetSummary };
}


async function dashboard() {
  const f = fb(); const ctrl = await control();
  const out = {
    control: ctrl, currency: CURRENCY,
    collections: COLLECTIONS, occasions: OCCASIONS, terms: BRAND.termExclusions,
    pending: [], recentLedger: [], lastMetrics: null, metricsSeries: []
  };
  if (!f) return out;
  try {
    // Equality-only filter needs no composite index; sort newest-first in memory.
    const ap = await f.db.collection(COL.approvals).where("status", "==", "PENDING").limit(50).get();
    const rows = [];
    ap.forEach(d => { const x = d.data(); rows.push({ id: d.id, ...x, _ts: x.createdAt && x.createdAt.toMillis ? x.createdAt.toMillis() : 0, createdAt: undefined }); });
    rows.sort((a, b) => b._ts - a._ts);
    rows.slice(0, 25).forEach(r => { delete r._ts; out.pending.push(r); });
  } catch (e) {}
  out.stuck = [];
  try {
    // APPROVED but not yet APPLIED = apply errored. Surface so the operator can retry.
    const st = await f.db.collection(COL.approvals).where("status", "==", "APPROVED").limit(25).get();
    st.forEach(d => { const x = d.data(); out.stuck.push({ id: d.id, type: x.type, summary: x.summary, vetted: x.vetted, payload: x.payload }); });
  } catch (e) {}
  try {
    const lg = await f.db.collection(COL.ledger).orderBy("at", "desc").limit(20).get();
    lg.forEach(d => { const x = d.data(); out.recentLedger.push({ ...x, at: x.at && x.at.toMillis ? x.at.toMillis() : null }); });
  } catch (e) {}
  try {
    const mt = await f.db.collection(COL.metrics).orderBy("at", "desc").limit(14).get();
    const rows = [];
    mt.forEach(d => { const x = d.data(); rows.push({ at: x.at && x.at.toMillis ? x.at.toMillis() : null, kind: x.kind, snapshot: x.snapshot }); });
    if (rows.length) out.lastMetrics = rows[0].snapshot;
    out.metricsSeries = rows.reverse(); // oldest → newest
  } catch (e) {}
  try { out.conversionHealth = await conversionHealth(); } catch (e) { out.conversionHealth = null; }
  try { out.recentOrders = await recentOrders({ limit: 200 }); } catch (e) { out.recentOrders = []; }
  return out;
}

module.exports = {
  COL, V, CID,
  control, mintToken, gaql, mutate, mutateAll,
  enqueueConversion, uploadConversions, enqueueConversionAdjustment, uploadConversionAdjustments, recordRefund, conversionHealth, gAdsTime,
  recordOrderEvent, recentOrders, storeSignals, clearOrderLog, backfillOrders,
  ledger, clearLedger, enqueueApproval, applyApproval, applyApprovalById: applyApproval, retryStuckApprovals, sanitizeOps,
  generateRSAAssets, buildSearchCampaignOps, buildCampaignAssets, planCampaign, accountCvr, collectionProfiles, productSalesMap, bumpBestSellers, keywordResearch, keywordResearchPool, researchOpportunity, mergeKeywordResearch, keywordDiag, metricsRange, textGuidelinesOp, brandSafe,
  generateForCollection, COLLECTIONS, OCCASIONS,
  getCollections, suggestOccasions, recordOccasionUse,
  scanOpportunities, opportunitiesWithStatus, takenTags, fetchTopProducts, setCampaignStatus, startCampaignNow, setCampaignBudget, analyzeCampaign,
  listCountries, campaignCountries, setCampaignCountries, setApprovalCountries,
  loadCalendar, dueEvents,
  measure, pruneAssets, mineSearchTerms, reallocateBudgets, anomalyCheck,
  enforceBudgetCeiling, monthlySpendGuard,
  dashboard,
  _util: { micros, fromMicros, clampHeadline, clampDescription, gAdsTime, daysUntil }
};
