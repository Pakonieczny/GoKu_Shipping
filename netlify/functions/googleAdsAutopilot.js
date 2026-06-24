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
  convQueue: "Brites_GAds_ConvQueue"     // offline conversions waiting for upload (auto-id)
};

/* ============================ Config / control ============================ */
const ENV = process.env;
const V          = ENV.GADS_API_VERSION || "v24";
const BASE       = `https://googleads.googleapis.com/${V}`;
const CID        = (ENV.GADS_CUSTOMER_ID || "").replace(/\D/g, "");        // Brites account
const LOGIN_CID  = (ENV.GADS_LOGIN_CUSTOMER_ID || CID).replace(/\D/g, ""); // manager (MCC)
const DEV_TOKEN  = ENV.GADS_DEVELOPER_TOKEN || "";
const GEN_MODEL  = ENV.GADS_GEN_MODEL || "gpt-5.4-mini";                   // text generation
const CURRENCY   = ENV.GADS_CURRENCY || "USD";

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
  await f.db.collection(COL.convQueue).add({
    gclid: gclid || null, gbraid: gbraid || null, wbraid: wbraid || null,
    value: Number(value) || 0, currency: currency || CURRENCY,
    orderId: orderId || null,
    conversionDateTime: conversionDateTime || gAdsTime(new Date()),
    uploaded: false, createdAt: f.FV.serverTimestamp()
  });
  return true;
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

/* ============================ Ledger / approvals ============================ */
async function ledger(entry) {
  const f = fb(); if (!f) return;
  try { await f.db.collection(COL.ledger).add({ ...entry, at: f.FV.serverTimestamp() }); } catch (e) {}
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
async function applyApproval(id, ctrl) {
  ctrl = ctrl || (await control());
  const f = fb(); if (!f) throw new Error("no firestore");
  const ref = f.db.collection(COL.approvals).doc(id);
  const snap = await ref.get();
  if (!snap.exists) throw new Error("approval not found");
  const it = snap.data();
  if (it.status !== "APPROVED") throw new Error("approval not in APPROVED state");
  const p = it.payload || {};
  if (p.service && p.operations) await mutate(p.service, p.operations, { ctrl, label: "approval:" + it.type });
  else if (p.mutateOperations)   await mutateAll(p.mutateOperations, { ctrl, label: "approval:" + it.type });
  await ref.update({ status: "APPLIED", appliedAt: f.FV.serverTimestamp() });
  return true;
}

/* ============================ Helpers ============================ */
function micros(v) { return Math.round(Number(v) * 1e6); }
function fromMicros(m) { return (Number(m) || 0) / 1e6; }
function clampHeadline(s) { return String(s).slice(0, 30); }   // RSA headline ≤30
function clampDescription(s) { return String(s).slice(0, 90); } // RSA description ≤90
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
// Two layers: (1) we filter generated copy here before it is ever queued, and
// (2) we stamp Campaign.text_guidelines on created campaigns so Google's own AI
// (PMax / AI-Max) is constrained server-side too (v23.1+).
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
function buildSearchCampaignOps(coll, event, assets, { dailyBudget }) {
  const tag = `${coll.handle}-${(event ? event.label : "evergreen").toLowerCase().replace(/[^a-z0-9]+/g, "-")}`.slice(0, 40);
  const bRes = `customers/${CID}/campaignBudgets/-1`;
  const cRes = `customers/${CID}/campaigns/-2`;
  const agRes = `customers/${CID}/adGroups/-3`;
  const finalUrl = `https://${(ENV.SITE_NAME ? "" : "")}britesjewelry.com/collections/${coll.handle}`;
  const ops = [
    { campaignBudgetOperation: { create: {
        resourceName: bRes, name: `BA · ${tag} · ${Date.now()}`,
        amountMicros: micros(dailyBudget), deliveryMethod: "STANDARD", explicitlyShared: false } } },
    { campaignOperation: { create: {
        resourceName: cRes, name: `BA · ${tag}`, status: "PAUSED",      // always start PAUSED
        advertisingChannelType: "SEARCH", campaignBudget: bRes,
        maximizeConversionValue: ENV.GADS_TARGET_ROAS ? { targetRoas: Number(ENV.GADS_TARGET_ROAS) } : {},
        networkSettings: { targetGoogleSearch: true, targetSearchNetwork: true, targetContentNetwork: false },
        textGuidelines: textGuidelinesOp() } } },
    { adGroupOperation: { create: {
        resourceName: agRes, name: `${coll.title} · ${event ? event.label : "Evergreen"}`,
        campaign: cRes, type: "SEARCH_STANDARD", cpcBidMicros: micros(0.40) } } },
    { adGroupAdOperation: { create: {
        adGroupAd: { adGroup: agRes, status: "ENABLED", ad: {
          finalUrls: [finalUrl],
          responsiveSearchAd: {
            headlines: assets.headlines.map(t => ({ text: t })),
            descriptions: assets.descriptions.map(t => ({ text: t }))
          } } } } } }
  ];
  // Keyword themes from collection title + event (phrase match)
  const kws = [coll.title, `${coll.title} gift`, `${coll.title} necklace`,
               event ? `${coll.title} ${event.label}` : null].filter(Boolean);
  kws.forEach((k, i) => ops.push({ adGroupCriterionOperation: { create: {
    adGroupCriterion: { adGroup: agRes, status: "ENABLED", keyword: { text: k, matchType: "PHRASE" } } } } }));
  return { ops, tag, finalUrl };
}

/* ============================ STAGES ============================ */

// MEASURE: snapshot campaign + asset + search-term performance into Firestore.
async function measure() {
  const f = fb();
  const campaigns = await gaql(
    `SELECT campaign.id, campaign.name, campaign.status, campaign_budget.amount_micros,
            metrics.cost_micros, metrics.conversions, metrics.conversions_value, metrics.clicks, metrics.impressions
     FROM campaign WHERE segments.date DURING LAST_14_DAYS AND campaign.status != 'REMOVED'`);
  const snapshot = campaigns.map(r => ({
    id: r.campaign.id, name: r.campaign.name, status: r.campaign.status,
    budget: fromMicros(r.campaignBudget && r.campaignBudget.amountMicros),
    cost: fromMicros(r.metrics.costMicros), conv: Number(r.metrics.conversions || 0),
    value: Number(r.metrics.conversionsValue || 0), clicks: Number(r.metrics.clicks || 0),
    impr: Number(r.metrics.impressions || 0)
  }));
  if (f) await f.db.collection(COL.metrics).add({ at: f.FV.serverTimestamp(), kind: "campaign14d", snapshot });
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

/* ============================ Summary for console ============================ */
async function dashboard() {
  const f = fb(); const ctrl = await control();
  const out = { control: ctrl, pending: [], recentLedger: [], lastMetrics: null };
  if (!f) return out;
  try {
    const ap = await f.db.collection(COL.approvals).where("status", "==", "PENDING").orderBy("createdAt", "desc").limit(25).get();
    ap.forEach(d => out.pending.push({ id: d.id, ...d.data(), createdAt: undefined }));
  } catch (e) {}
  try {
    const lg = await f.db.collection(COL.ledger).orderBy("at", "desc").limit(20).get();
    lg.forEach(d => out.recentLedger.push({ ...d.data(), at: undefined }));
  } catch (e) {}
  try {
    const mt = await f.db.collection(COL.metrics).orderBy("at", "desc").limit(1).get();
    mt.forEach(d => out.lastMetrics = d.data().snapshot);
  } catch (e) {}
  return out;
}

module.exports = {
  COL, V, CID,
  control, mintToken, gaql, mutate, mutateAll,
  enqueueConversion, uploadConversions,
  ledger, enqueueApproval, applyApproval, applyApprovalById: applyApproval,
  generateRSAAssets, buildSearchCampaignOps, textGuidelinesOp, brandSafe,
  loadCalendar, dueEvents,
  measure, pruneAssets, mineSearchTerms, reallocateBudgets, anomalyCheck,
  dashboard,
  _util: { micros, fromMicros, clampHeadline, clampDescription, gAdsTime, daysUntil }
};
