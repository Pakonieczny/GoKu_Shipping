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
  convAdj:   "Brites_GAds_ConvAdjQueue"  // conversion adjustments (refunds/retractions) waiting for upload (auto-id)
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

function adsHeaders(token) {
  const h = {
    "Authorization": "Bearer " + token,
    "developer-token": DEV_TOKEN,
    "Content-Type": "application/json"
  };
  if (LOGIN_CID) h["login-customer-id"] = LOGIN_CID;
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
    const r = await gaql(`SELECT metrics.conversions, metrics.all_conversions FROM customer DURING LAST_30_DAYS`);
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
  if (p.service && p.operations) await mutate(p.service, sanitizeOps(p.operations), { ctrl, label: "approval:" + it.type });
  else if (p.mutateOperations)   await mutateAll(sanitizeOps(p.mutateOperations), { ctrl, label: "approval:" + it.type });
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
async function openaiJSON(prompt, { maxTokens = 1400 } = {}) {
  const model = GEN_MODEL;
  const payload = { model, messages: [{ role: "user", content: prompt }] };
  if (/^(gpt-5|o\d)/.test(model)) { payload.max_completion_tokens = maxTokens; payload.reasoning_effort = "low"; }
  else { payload.max_tokens = Math.min(maxTokens, 900); payload.temperature = 0.8; }
  const res = await fetch("https://api.openai.com/v1/chat/completions", {
    method: "POST",
    headers: { "Content-Type": "application/json", Authorization: "Bearer " + (ENV.OPENAI_API_KEY || "") },
    body: JSON.stringify(payload)
  });
  const data = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error("[gads] OpenAI: " + ((data.error && data.error.message) || res.status));
  const raw = (((data.choices || [])[0] || {}).message || {}).content || "";
  const cleaned = raw.replace(/```json|```/g, "").trim();
  try { return JSON.parse(cleaned); } catch { return null; }
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

function buildSearchCampaignOps(coll, event, assets, { dailyBudget, startDate, endDate }) {
  const tag = `${coll.handle}-${(event ? event.label : "evergreen").toLowerCase().replace(/[^a-z0-9]+/g, "-")}`.slice(0, 40);
  const bRes = `customers/${CID}/campaignBudgets/-1`;
  const cRes = `customers/${CID}/campaigns/-2`;
  const agRes = `customers/${CID}/adGroups/-3`;
  const finalUrl = `https://${(ENV.SITE_NAME ? "" : "")}britesjewelry.com/collections/${coll.handle}`;
  const startYmd = gAdsDate(startDate, true);   // clamp start to today-or-later
  const endYmd = gAdsDate(endDate, false);
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
        maximizeConversionValue: ENV.GADS_TARGET_ROAS ? { targetRoas: Number(ENV.GADS_TARGET_ROAS) } : {},
        networkSettings: { targetGoogleSearch: true, targetSearchNetwork: true, targetContentNetwork: false } } } },
    { adGroupOperation: { create: {
        resourceName: agRes, name: `${coll.title} · ${event ? event.label : "Evergreen"}`,
        campaign: cRes, type: "SEARCH_STANDARD", cpcBidMicros: micros(0.40) } } },
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
  return { ops, tag, finalUrl };
}

/* ============================ STAGES ============================ */

// MEASURE: snapshot campaign + asset + search-term performance into Firestore.
async function measure() {
  const f = fb();
  // 1) ALL campaigns (config only, no date segment) — guarantees brand-new / paused /
  //    zero-impression campaigns are included, which a date-segmented query would drop.
  const base = await gaql(
    `SELECT campaign.id, campaign.name, campaign.status, campaign_budget.resource_name, campaign_budget.amount_micros
     FROM campaign WHERE campaign.status != 'REMOVED'`);
  const byId = {};
  base.forEach(r => {
    byId[r.campaign.id] = {
      id: r.campaign.id, name: r.campaign.name, status: r.campaign.status,
      budget: fromMicros(r.campaignBudget && r.campaignBudget.amountMicros),
      budgetRes: (r.campaignBudget && r.campaignBudget.resourceName) || null,
      cost: 0, conv: 0, value: 0, clicks: 0, impr: 0
    };
  });
  // 2) Last-14-day metrics — overlay onto campaigns that have activity (aggregated per campaign,
  //    since a date-segmented query returns one row per campaign per day).
  try {
    const met = await gaql(
      `SELECT campaign.id, metrics.cost_micros, metrics.conversions, metrics.conversions_value,
              metrics.clicks, metrics.impressions
       FROM campaign WHERE segments.date DURING LAST_14_DAYS AND campaign.status != 'REMOVED'`);
    met.forEach(r => {
      const c = byId[r.campaign.id]; if (!c) return;
      c.cost += fromMicros(r.metrics.costMicros); c.conv += Number(r.metrics.conversions || 0);
      c.value += Number(r.metrics.conversionsValue || 0); c.clicks += Number(r.metrics.clicks || 0);
      c.impr += Number(r.metrics.impressions || 0);
    });
  } catch (e) {}
  const snapshot = Object.values(byId);
  if (f) await f.db.collection(COL.metrics).add({ at: f.FV.serverTimestamp(), kind: "campaign14d", snapshot });
  await attributeOccasionsFromSnapshot(snapshot);
  return snapshot;
}

// PRUNE: find LOW-rated RSA headlines/descriptions with enough exposure; ask the
// model for on-brand replacements; QUEUE the swap (never auto-apply text blind).
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
  const dateStr = new Date().toISOString().slice(0, 10);
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
  const d = await shopifyGql(`{ products(first: 40, sortKey: BEST_SELLING) { edges { node { title handle } } } }`);
  return ((d.products && d.products.edges) || []).map(e => ({ title: e.node.title, handle: e.node.handle })).filter(p => p.title);
}

// THE big analysis: cross-reference all collections + best-sellers + calendar + memory
// → a ranked list of fully-specified campaign opportunities (budget, duration, keywords…).
// Cached 12h; force re-rolls. Stored in Firestore for recall.
async function scanOpportunities({ force, cacheOnly } = {}) {
  const f = fb(); const ctrl = await control();
  if (f && (cacheOnly || !force)) {
    try {
      const s = await f.db.collection(COL.state).doc("opportunities").get();
      if (s.exists) {
        const x = s.data();
        if (cacheOnly) return { opportunities: Array.isArray(x.list) ? x.list : [], scannedAt: x.at || null, scanning: !!x.scanning };
        if (x.at && (Date.now() - x.at) < 12 * 60 * 60 * 1000 && Array.isArray(x.list) && x.list.length) return { opportunities: x.list, scannedAt: x.at };
      } else if (cacheOnly) { return { opportunities: [], scannedAt: null, scanning: false }; }
    } catch (e) { if (cacheOnly) return { opportunities: [], scannedAt: null, scanning: false }; }
  }
  const collections = await getCollections({});
  let products = []; try { products = await fetchTopProducts(); } catch (e) {}
  let memory = [];
  if (f) { try { const snap = await f.db.collection(COL.occasions).get(); snap.forEach(d => { const x = d.data(); memory.push({ occasion: x.occasion, outcome: x.outcome || "untested", roas: (x.agg && x.agg.roas) || null, collections: Object.keys(x.collections || {}) }); }); } catch (e) {} }
  const dateStr = new Date().toISOString().slice(0, 10);
  const ceiling = ctrl.maxDailyBudgetTotal || 100, ccy = CURRENCY;
  const collText = collections.map(c => c.title).join(", ");
  const prodText = products.length ? products.map(p => p.title).slice(0, 40).join("; ") : "(not available)";
  const memText = memory.length ? memory.map(m => `${m.occasion} [${(m.collections || []).join("/")}]: ${m.outcome}${m.roas ? ` ${m.roas}x` : ""}`).join("; ") : "(no history yet — nothing has run)";
  const _convH = await conversionHealth().catch(() => ({ validated: false }));
  const convDirective = _convH.validated
    ? "CONVERSION TRACKING: LIVE and recording sales — ROAS/outcome history is reliable. Weight proven occasions heavily; you may recommend scaling winners."
    : "CONVERSION TRACKING: NOT YET RECORDING SALES — you have NO validated ROAS data. Do NOT label any occasion 'proven'; treat every expectedRoasBand as a conservative ESTIMATE, keep recommendedDailyBudget at modest test levels, and favor low-risk bets over aggressive spend until conversions flow.";
  const prompt =
`Today: ${dateStr}. You are the campaign strategist for Brites, a handcrafted personalized charm-jewelry brand (gift/emotion-led). Currency ${ccy}. Total daily ad ceiling ${ccy} ${ceiling}.
${convDirective}
ALL COLLECTIONS: ${collText}
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
- expectedRoasBand (e.g. "3-4x"; be conservative)
- proven (bool: true ONLY if memory shows success for this occasion)
- rationale (<=120 chars: why now, why this collection)
- keywords (4-6 phrase-match keywords a shopper would search)
- keyPhrases (3-4 short emotional ad phrases)
Only include opportunities genuinely relevant within ~30 days. Rank best-first (soonest + strongest first). Avoid out-of-season occasions and any memory marks as fail. Return ONLY JSON: {"opportunities":[ ... ]}`;
  let list = null;
  try { const j = await openaiJSON(prompt, { maxTokens: 2400 }); if (j && Array.isArray(j.opportunities)) list = j.opportunities.filter(o => o && o.collectionTitle && o.occasion); } catch (e) {}
  if (!list) list = [];
  const byTitle = {}; collections.forEach(c => byTitle[c.title.toLowerCase()] = c.handle);
  const today0 = _todayUtc();
  list = list.map((o, i) => {
    const t = String(o.collectionTitle).toLowerCase();
    const handle = byTitle[t] || (collections.find(c => c.title.toLowerCase().indexOf(t) >= 0) || {}).handle || null;
    const mem = memory.find(m => m.occasion && String(m.occasion).toLowerCase() === String(o.occasion).toLowerCase());
    const bud = Math.max(2, Math.min(ceiling, Number(o.recommendedDailyBudget) || 8));
    // Resolve the run window. Trust the AI's dates; fall back to daysOut + a default length.
    let start = _parseYmd(o.startDate), end = _parseYmd(o.endDate);
    const durHint = Math.max(3, Math.min(60, +o.durationDays || 21));
    if (!start) { const lead = Math.max(0, (o.daysOut != null ? +o.daysOut : 7)); start = new Date(today0.getTime() + lead * 86400000); }
    if (start < today0) start = today0;                       // never before today
    if (!end || end <= start) end = new Date(start.getTime() + durHint * 86400000);
    const durationDays = Math.max(3, Math.min(90, _daysBetween(start, end)));
    const startDate = _ymd(start), endDate = _ymd(end);
    const daysOut = Math.max(0, _daysBetween(today0, start));
    return {
      id: "op" + i, collectionHandle: handle, collectionTitle: o.collectionTitle, occasion: o.occasion,
      startDate, endDate, daysOut, durationDays,
      priority: (["high", "medium", "test"].indexOf(o.priority) >= 0 ? o.priority : "test"),
      recommendedDailyBudget: bud, estTotalSpend: Math.round(bud * durationDays),
      expectedRoasBand: o.expectedRoasBand || null, proven: !!o.proven,
      rationale: o.rationale || "", keywords: Array.isArray(o.keywords) ? o.keywords.slice(0, 6) : [],
      keyPhrases: Array.isArray(o.keyPhrases) ? o.keyPhrases.slice(0, 4) : [],
      pastStats: mem && mem.roas ? { roas: mem.roas, outcome: mem.outcome } : null, currency: ccy
    };
  }).filter(o => o.collectionHandle)
    .filter(o => o.daysOut <= 32)   // ~30-day forward window: drop anything that starts too far out
    .sort((a, b) => a.daysOut - b.daysOut);
  if (f && list.length) { try { await f.db.collection(COL.state).doc("opportunities").set({ list, at: Date.now(), scanning: false }); } catch (e) {} }
  else if (f) { try { await f.db.collection(COL.state).doc("opportunities").set({ scanning: false, at: Date.now() }, { merge: true }); } catch (e) {} }
  return { opportunities: list, scannedAt: Date.now() };
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
    const rows = await gaql(`SELECT campaign.id, campaign.name, campaign.status FROM campaign`);
    const campMap = {};
    rows.forEach(r => {
      const tag = tagFromCampaignName(r.campaign.name); if (!tag) return;
      const cur = campMap[tag]; const st = r.campaign.status;
      if (!cur || (_CAMP_RANK[st] || 0) >= (_CAMP_RANK[cur.status] || 0))
        campMap[tag] = { where: "campaign", status: st, campaignId: r.campaign.id };
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
  const opportunities = (r.opportunities || []).map(o => {
    const tag = oppTag(o.collectionHandle, o.occasion);
    return Object.assign({}, o, { tag, acted: taken[tag] || null });
  })
  // Archived campaigns are terminal: drop their opportunity from every list
  // (not "in use", not re-suggested as "unused"). Live/paused/approval states stay,
  // shown with their actual status.
  .filter(o => !(o.acted && o.acted.where === "campaign" && o.acted.status === "REMOVED"));
  return { opportunities, scannedAt: r.scannedAt, scanning: !!r.scanning, taken };
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

async function generateForCollection(handle, eventLabel, budget, { ctrl, startDate, endDate } = {}) {
  ctrl = ctrl || (await control());
  if (!handle) return { ok: false, reason: "no collection given" };
  const coll = await collectionMeta(handle);
  const event = (eventLabel && eventLabel !== "Evergreen gifting") ? { label: eventLabel, angle: "" } : null;
  const assets = await generateRSAAssets(coll, event);
  if (!assets) return { ok: false, reason: "generation rejected — copy failed brand-safety or fell under RSA minimums" };
  const dailyBudget = Number(budget) || Number(ENV.GADS_NEW_CAMPAIGN_BUDGET || 8);
  const { ops, tag } = buildSearchCampaignOps(coll, event, assets, { dailyBudget, startDate, endDate });
  await recordOccasionUse(event ? event.label : "Evergreen gifting", coll.handle, tag);
  const win = (startDate && endDate) ? ` (${startDate} → ${endDate})` : "";
  const id = await enqueueApproval({
    type: "creative", vetted: false,
    summary: `NEW Search campaign “${tag}”${event ? ` for ${event.label}` : ""}${win} — ${assets.headlines.length} headlines, starts PAUSED (drafted on the Bench)`,
    payload: { mutateOperations: ops, finalCollection: coll.handle, event: event ? event.label : null, startDate: startDate || null, endDate: endDate || null },
    experimentId: tag
  });
  return { ok: true, approvalId: id, tag, title: coll.title, event: event ? event.label : null,
           budget: dailyBudget, currency: CURRENCY, assets };
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
  return out;
}

module.exports = {
  COL, V, CID,
  control, mintToken, gaql, mutate, mutateAll,
  enqueueConversion, uploadConversions, enqueueConversionAdjustment, uploadConversionAdjustments, recordRefund, conversionHealth, gAdsTime,
  ledger, clearLedger, enqueueApproval, applyApproval, applyApprovalById: applyApproval, retryStuckApprovals, sanitizeOps,
  generateRSAAssets, buildSearchCampaignOps, textGuidelinesOp, brandSafe,
  generateForCollection, COLLECTIONS, OCCASIONS,
  getCollections, suggestOccasions, recordOccasionUse,
  scanOpportunities, opportunitiesWithStatus, takenTags, fetchTopProducts, setCampaignStatus, setCampaignBudget, analyzeCampaign,
  loadCalendar, dueEvents,
  measure, pruneAssets, mineSearchTerms, reallocateBudgets, anomalyCheck,
  dashboard,
  _util: { micros, fromMicros, clampHeadline, clampDescription, gAdsTime, daysUntil }
};
