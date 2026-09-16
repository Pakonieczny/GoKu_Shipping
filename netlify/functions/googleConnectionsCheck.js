// netlify/functions/googleConnectionsCheck.js
// ─────────────────────────────────────────────────────────────────────────────
// Verifies EVERY Google connection this application depends on, in one read-only
// pass, against the live account:
//
//   https://goldenspike.app/.netlify/functions/googleConnectionsCheck
//   …?format=json                     machine-readable
//   …?write=1                         adds a validateOnly mutate probe (Google
//                                     documents validateOnly as non-mutating;
//                                     it is still opt-in, so the default run
//                                     cannot touch the account at all)
//
// What it proves, and what it deliberately does not:
//   · A green row means Google answered. It does not mean an ad is serving.
//   · A skipped row means a credential never reached Google. Skipped is never
//     reported as green.
//   · Policy approval, serving and billing are separate states from API reach.
//
// The catalog lives in _googleConnections.js so tests can assert it stays
// complete as the application grows.
// ─────────────────────────────────────────────────────────────────────────────

const fetch = require("node-fetch");
const C = require("./_googleConnections");
const ENV = process.env;

const V = ENV.GADS_API_VERSION || "v24";
const CID = (ENV.GADS_CUSTOMER_ID || "").replace(/\D/g, "");
const LOGIN = (ENV.GADS_LOGIN_CUSTOMER_ID || "").replace(/\D/g, "");
const MERCHANT = String(ENV.GMC_MERCHANT_ID || ENV.MERCHANT_CENTER_ID || "").replace(/\D/g, "");
const TIMEOUT = 20000;

const present = k => typeof ENV[k] === "string" && ENV[k].trim().length > 0;
const esc = s => String(s == null ? "" : s).replace(/[&<>"]/g, c => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[c]));

// Run probes a few at a time: a verifier must not itself look like an attack.
async function inBatches(items, size, worker) {
  const out = [];
  for (let i = 0; i < items.length; i += size) out.push(...await Promise.all(items.slice(i, i + size).map(worker)));
  return out;
}

async function mintToken(clientId, clientSecret, refreshToken) {
  const res = await fetch("https://oauth2.googleapis.com/token", {
    method: "POST", timeout: TIMEOUT, headers: { "Content-Type": "application/x-www-form-urlencoded" },
    body: new URLSearchParams({ client_id: clientId, client_secret: clientSecret, refresh_token: refreshToken, grant_type: "refresh_token" })
  });
  const data = await res.json().catch(() => ({}));
  if (!res.ok || !data.access_token) throw new Error((data.error || res.status) + " — " + (data.error_description || "check the client id, secret and refresh token"));
  return data.access_token;
}

function adsHeaders(token, customerId) {
  const h = { Authorization: "Bearer " + token, "developer-token": ENV.GADS_DEVELOPER_TOKEN, "Content-Type": "application/json" };
  if (LOGIN) h["login-customer-id"] = LOGIN;
  return h;
}

async function adsPost(token, path, body, version) {
  const res = await fetch("https://googleads.googleapis.com/" + (version || V) + "/" + path, {
    method: "POST", timeout: TIMEOUT, headers: adsHeaders(token), body: JSON.stringify(body)
  });
  const data = await res.json().catch(() => ({}));
  return { res, data };
}

// ── 1. Credentials ──────────────────────────────────────────────────────────
function credentialSection() {
  const rows = [];
  const shape = (key, test, good, bad) => rows.push(present(key) ? (test() ? C.ok(key, good) : C.fail(key, bad)) : C.fail(key, "missing"));

  rows.push(/^v\d+$/.test(V) ? C.ok("GADS_API_VERSION", V) : C.fail("GADS_API_VERSION", V + " — expected a vNN version"));
  shape("GADS_DEVELOPER_TOKEN", () => true, "present (" + (ENV.GADS_DEVELOPER_TOKEN || "").length + " chars)", "missing");
  shape("GADS_CLIENT_ID", () => /\.apps\.googleusercontent\.com$/.test(ENV.GADS_CLIENT_ID), "present", "does not end in .apps.googleusercontent.com");
  shape("GADS_CLIENT_SECRET", () => /^GOCSPX-/.test(ENV.GADS_CLIENT_SECRET), "present (GOCSPX-…)", "not a GOCSPX- secret");
  shape("GADS_REFRESH_TOKEN", () => /^1\/\//.test(ENV.GADS_REFRESH_TOKEN), "present (1//…)", "not a 1// refresh token");
  rows.push(/^\d{10}$/.test(LOGIN) ? C.ok("GADS_LOGIN_CUSTOMER_ID", LOGIN) : C.fail("GADS_LOGIN_CUSTOMER_ID", (LOGIN || "missing") + " — expected 10 digits, no dashes"));
  rows.push(/^\d{10}$/.test(CID) ? C.ok("GADS_CUSTOMER_ID", CID) : C.fail("GADS_CUSTOMER_ID", (CID || "missing") + " — expected 10 digits, no dashes"));
  rows.push(MERCHANT ? C.ok("GMC_MERCHANT_ID", MERCHANT) : C.warn("GMC_MERCHANT_ID", "not set — the Merchant account is discovered from a linked campaign instead"));
  rows.push(present("GMC_REFRESH_TOKEN") ? C.ok("GMC_REFRESH_TOKEN", "present") : C.warn("GMC_REFRESH_TOKEN", "not set — every Merchant Center call is unavailable"));
  rows.push(present("GEMINI_API_KEY") ? C.ok("GEMINI_API_KEY", "present") : C.warn("GEMINI_API_KEY", "not set — image and video generation are unavailable"));
  rows.push(present("SHOPIFY_STORE") ? C.ok("SHOPIFY_STORE", ENV.SHOPIFY_STORE) : C.warn("SHOPIFY_STORE", "not set — the store side cannot be verified"));
  return { title: "Credentials", note: "Shape only. The rows below prove whether Google accepts them.", rows };
}

// ── 2. Google Ads reach and API version currency ────────────────────────────
async function adsAccessSection(token) {
  const rows = [];
  if (!token) return { title: "Google Ads · access", rows: [C.skip("access", "no access token was minted")], served: [] };

  try {
    const res = await fetch("https://googleads.googleapis.com/" + V + "/customers:listAccessibleCustomers", {
      timeout: TIMEOUT, headers: { Authorization: "Bearer " + token, "developer-token": ENV.GADS_DEVELOPER_TOKEN }
    });
    const data = await res.json().catch(() => ({}));
    if (res.ok) {
      const ids = (data.resourceNames || []).map(r => r.split("/")[1]);
      rows.push(C.ok("developer token", "accepted · sees " + ids.length + " account(s)", { accounts: ids }));
      rows.push(ids.includes(CID) || ids.includes(LOGIN)
        ? C.ok("account reachable", CID + " is reachable")
        : C.fail("account reachable", CID + " is not in the accessible list — confirm the manager link was accepted"));
    } else {
      const detail = res.status + " — " + (C.adsErrorCode(data) || JSON.stringify(data).slice(0, 200));
      rows.push(C.fail("developer token", detail, { remedy: C.remedyFor(detail) }));
    }
  } catch (e) { rows.push(C.fail("developer token", e.message)); }

  // Version currency. A version that answers today still sunsets; a version that
  // does not answer is already gone. Probe the neighbours to say which is true.
  const base = Number(String(V).replace(/\D/g, "")) || 0;
  const candidates = [base, base + 1, base + 2].filter(Boolean);
  const served = [];
  for (const n of candidates) {
    try {
      const res = await fetch("https://googleads.googleapis.com/v" + n + "/customers:listAccessibleCustomers", {
        timeout: TIMEOUT, headers: { Authorization: "Bearer " + token, "developer-token": ENV.GADS_DEVELOPER_TOKEN }
      });
      if (res.status !== 404) served.push("v" + n);
    } catch (e) { /* treated as not served */ }
  }
  const newest = served[served.length - 1];
  rows.push(newest && newest !== V
    ? C.warn("API version currency", "configured " + V + "; Google also serves " + served.join(", ") + ". Newer versions carry later sunset dates — move GADS_API_VERSION to " + newest + " once its fields are confirmed below.")
    : served.length ? C.ok("API version currency", "configured " + V + " is the newest version answering for this account")
      : C.fail("API version currency", "no probed version answered"));
  return { title: "Google Ads · access", rows, served };
}


// ── 2b. Which Google APIs this refresh token may actually reach ─────────────
async function scopeSection(token) {
  if (!token) return { title: "OAuth scopes", rows: [C.skip("scopes", "no access token was minted")] };
  let granted = null;
  try {
    const res = await fetch("https://oauth2.googleapis.com/tokeninfo?access_token=" + encodeURIComponent(token), { timeout: TIMEOUT });
    const data = await res.json().catch(() => ({}));
    if (res.ok && typeof data.scope === "string") granted = new Set(data.scope.split(/\s+/).filter(Boolean));
    else return { title: "OAuth scopes", rows: [C.fail("tokeninfo", res.status + " — the granted scopes could not be read")] };
  } catch (e) { return { title: "OAuth scopes", rows: [C.fail("tokeninfo", e.message)] }; }
  const rows = C.REQUIRED_SCOPES.map(s => granted.has(s.scope)
    ? C.ok(s.unlocks, "granted · " + s.scope)
    : C.row(s.unlocks, s.required ? "FAIL" : "warn",
        "not granted to this refresh token · " + s.scope,
        { remedy: "Re-consent this OAuth client with " + s.scope + " and store the new refresh token. A missing scope fails at call time, not at consent time." }));
  rows.push(C.ok("scopes granted", granted.size + " in total"));
  return { title: "OAuth scopes", note: "The Ads refresh token is checked against every scope this application needs. Merchant Center uses its own token where GMC_REFRESH_TOKEN is set.", rows };
}

// ── 3. Every reporting resource ─────────────────────────────────────────────
async function adsResourceSection(token) {
  if (!token || !/^\d{10}$/.test(CID)) return { title: "Google Ads · reporting resources", rows: [C.skip("resources", "no usable Ads credentials")] };
  const rows = await inBatches(C.ADS_RESOURCES, 4, async probe => {
    try {
      const { res, data } = await adsPost(token, "customers/" + CID + "/googleAds:search", { query: probe.query, pageSize: 50 });
      if (res.ok) {
        const n = (data.results || []).length;
        return C.ok(probe.resource, (probe.used ? "" : "available, not yet used · ") + n + " row(s) returned", { used: probe.used, family: probe.family, why: probe.why, rows: n });
      }
      const detail = res.status + " — " + (C.adsErrorCode(data) || JSON.stringify(data).slice(0, 180));
      // An unused capability that the account cannot reach is information, not a defect.
      const status = probe.used ? "FAIL" : "warn";
      return C.row(probe.resource, status, detail, { used: probe.used, family: probe.family, why: probe.why, remedy: C.remedyFor(detail) });
    } catch (e) { return C.row(probe.resource, probe.used ? "FAIL" : "warn", e.message, { used: probe.used, family: probe.family, why: probe.why }); }
  });
  return { title: "Google Ads · reporting resources", note: "Each row is the smallest query that proves the resource is readable. Rows marked “available, not yet used” are Google capabilities this application does not call yet.", rows };
}

// ── 4. Schema: does every field this application queries still exist? ───────
// A version bump is only safe if all of them do, so the whole set is
// introspected in batches rather than a sample.
async function fieldMap(token, version, names) {
  const found = new Map();
  for (let i = 0; i < names.length; i += 50) {
    const chunk = names.slice(i, i + 50);
    const { res, data } = await adsPost(token, "googleAdsFields:search", {
      query: "SELECT name, category, selectable, data_type, selectable_with WHERE name IN (" + chunk.map(n => "'" + n + "'").join(",") + ")"
    }, version);
    if (!res.ok) throw new Error(res.status + " — " + (C.adsErrorCode(data) || JSON.stringify(data).slice(0, 160)));
    for (const r of data.results || []) found.set(r.name, r);
  }
  return found;
}

async function adsSchemaSection(token, served) {
  if (!token) return { title: "Google Ads · field schema", rows: [C.skip("schema", "no access token")] };
  const rows = [], names = C.allQueriedFields();
  let current = null;
  try { current = await fieldMap(token, V, names); }
  catch (e) { return { title: "Google Ads · field schema", rows: [C.fail("field introspection", e.message, { remedy: C.remedyFor(e.message) })] }; }

  const missing = names.filter(n => !current.has(n));
  const unselectable = names.filter(n => current.has(n) && !current.get(n).selectable);
  rows.push(missing.length
    ? C.fail("fields present in " + V, missing.length + " of " + names.length + " missing: " + missing.join(", "))
    : C.ok("fields present in " + V, "all " + names.length + " queried fields exist"));
  if (unselectable.length) rows.push(C.warn("fields selectable in " + V, "present but not selectable: " + unselectable.join(", ")));

  // Named fields the operator asked about keep their own row and explanation.
  for (const f of C.ADS_FIELDS) {
    const found = current.get(f.field);
    rows.push(!found ? C.fail(f.field, "not present in " + V, { why: f.why })
      : found.selectable ? C.ok(f.field, found.category + " · " + found.dataType + " · selectable", { why: f.why })
        : C.warn(f.field, "exists but is not selectable in " + V, { why: f.why }));
  }

  // "Can I split this statistic by mobile versus desktop?" answered per resource.
  const deviceJoins = new Set((current.get("segments.device") || {}).selectableWith || []);
  if (deviceJoins.size) {
    for (const resource of C.DEVICE_SPLIT_WANTED) {
      rows.push(deviceJoins.has(resource)
        ? C.ok("device split · " + resource, "segments.device is selectable with " + resource)
        : C.warn("device split · " + resource, "Google does not allow segments.device on " + resource + " in " + V + ". A mobile/desktop split of this statistic is not available from the API — report it at a level that does support it rather than estimating."));
    }
  } else rows.push(C.skip("device split", "segments.device could not be introspected, so its joins are unknown"));

  // Readiness of every other version Google serves, so a bump is provable.
  for (const version of (served || []).filter(v => v !== V)) {
    try {
      const other = await fieldMap(token, version, names);
      const gone = names.filter(n => !other.has(n));
      rows.push(gone.length
        ? C.warn(version + " readiness", gone.length + " field(s) this application queries do not exist in " + version + ": " + gone.join(", ") + ". Do not set GADS_API_VERSION to " + version + " until these are replaced.")
        : C.ok(version + " readiness", "all " + names.length + " queried fields exist in " + version + " — GADS_API_VERSION can be set to " + version + " safely"));
    } catch (e) { rows.push(C.warn(version + " readiness", "could not be checked: " + e.message)); }
  }

  return { title: "Google Ads · field schema", note: "GoogleAdsFieldService answers for the live account. Every field the catalog queries is checked, in the configured version and in every other version Google serves.", rows };
}

// ── 5. Write capability, without writing ────────────────────────────────────
async function adsWriteSection(token, enabled) {
  if (!enabled) return { title: "Google Ads · write capability", rows: [C.skip("validateOnly mutate", "not run. Add ?write=1 to include it; Google documents validateOnly as non-mutating.")] };
  if (!token || !/^\d{10}$/.test(CID)) return { title: "Google Ads · write capability", rows: [C.skip("validateOnly mutate", "no usable Ads credentials")] };
  try {
    const { res, data } = await adsPost(token, "customers/" + CID + "/googleAds:mutate", {
      validateOnly: true, mutateOperations: [{ campaignBudgetOperation: { create: { name: "connection check " + Date.now(), amountMicros: "1000000", deliveryMethod: "STANDARD" } } }]
    });
    if (res.ok) return { title: "Google Ads · write capability", rows: [C.ok("validateOnly mutate", "accepted — this token may create and update. Nothing was created.")] };
    const detail = res.status + " — " + (C.adsErrorCode(data) || JSON.stringify(data).slice(0, 200));
    return { title: "Google Ads · write capability", rows: [C.fail("validateOnly mutate", detail, { remedy: C.remedyFor(detail) })] };
  } catch (e) { return { title: "Google Ads · write capability", rows: [C.fail("validateOnly mutate", e.message)] }; }
}

// ── 6. Merchant Center ──────────────────────────────────────────────────────
async function merchantSection() {
  const rows = [];
  let token = null;
  if (!present("GMC_REFRESH_TOKEN")) return { title: "Merchant Center", rows: [C.skip("merchant", "GMC_REFRESH_TOKEN is not set — no Merchant call can be attempted")] };
  try {
    token = await mintToken(ENV.GMC_CLIENT_ID || ENV.GADS_CLIENT_ID || "", ENV.GMC_CLIENT_SECRET || ENV.GADS_CLIENT_SECRET || "", ENV.GMC_REFRESH_TOKEN);
    rows.push(C.ok("merchant OAuth", "minted an access token"));
  } catch (e) { return { title: "Merchant Center", rows: [C.fail("merchant OAuth", e.message, { remedy: C.remedyFor(e.message) })] }; }
  if (!MERCHANT) { rows.push(C.warn("merchant account", "no GMC_MERCHANT_ID — set it to probe the account's sub-APIs")); return { title: "Merchant Center", rows }; }

  const probed = await inBatches(C.MERCHANT_PROBES, 3, async probe => {
    try {
      const res = await fetch("https://merchantapi.googleapis.com/" + probe.path(MERCHANT), {
        method: probe.method, timeout: TIMEOUT,
        headers: { Authorization: "Bearer " + token, "Content-Type": "application/json" },
        ...(probe.body ? { body: JSON.stringify(probe.body) } : {})
      });
      const data = await res.json().catch(() => ({}));
      if (res.ok) return C.ok(probe.key, (probe.used ? "" : "available, not yet used · ") + "answered", { used: probe.used, why: probe.why });
      const detail = res.status + " — " + (((data || {}).error || {}).message || "").slice(0, 180);
      return C.row(probe.key, probe.used ? "FAIL" : "warn", detail, { used: probe.used, why: probe.why, remedy: C.remedyFor(detail) });
    } catch (e) { return C.row(probe.key, probe.used ? "FAIL" : "warn", e.message, { used: probe.used, why: probe.why }); }
  });
  return { title: "Merchant Center", note: "Rows marked “available, not yet used” are Merchant capabilities this application does not call yet.", rows: rows.concat(probed) };
}

// ── 7. Everything else Google ───────────────────────────────────────────────
async function otherGoogleSection(token) {
  const rows = [];
  if (present("GEMINI_API_KEY")) {
    try {
      const res = await fetch("https://generativelanguage.googleapis.com/v1beta/models?key=" + encodeURIComponent(ENV.GEMINI_API_KEY) + "&pageSize=200", { timeout: TIMEOUT });
      const data = await res.json().catch(() => ({}));
      if (res.ok) {
        const names = (data.models || []).map(m => String(m.name || "").replace("models/", ""));
        const video = names.filter(n => /veo|video/i.test(n));
        rows.push(C.ok("Gemini models", names.length + " model(s) available"));
        rows.push(video.length ? C.ok("Gemini video models", video.slice(0, 6).join(", ")) : C.warn("Gemini video models", "no video-capable model is visible to this key"));
      } else rows.push(C.fail("Gemini models", res.status + " — " + (((data || {}).error || {}).message || "")));
    } catch (e) { rows.push(C.fail("Gemini models", e.message)); }
  } else rows.push(C.skip("Gemini models", "GEMINI_API_KEY is not set"));

  // Google Ads reports a video asset as attached and serving. It does not report
  // that YouTube rejected, failed to process, or unpublished the same video.
  // The key lives in Firestore so it costs no Netlify environment slot.
  const youtube = await require("./_googleApiKeys").googleApiKeyStatus("youtubeApiKey");
  if (youtube.error && !youtube.key) {
    rows.push(C.skip("YouTube Data API", "the key store could not be read: " + youtube.error));
  } else if (!youtube.key) {
    rows.push(C.skip("YouTube Data API", "optional, not configured. Films upload through Google Ads, which reports their upload state and serving eligibility on its own; this key would only add YouTube-side detail such as a copyright rejection or a privacy change made in YouTube Studio. Add youtubeApiKey to Firestore " + require("./_googleApiKeys").DOC_PATH + " if that is ever wanted."));
  } else if (!token || !/^\d{10}$/.test(CID)) {
    rows.push(C.skip("YouTube Data API", "a key is configured (" + youtube.source + "), but the video IDs come from Google Ads and those credentials are unavailable"));
  } else {
    try {
      const { data } = await adsPost(token, "customers/" + CID + "/googleAds:search", {
        query: "SELECT asset.youtube_video_asset.youtube_video_id FROM asset WHERE asset.type = 'YOUTUBE_VIDEO' LIMIT 50", pageSize: 50
      });
      const ids = (data.results || []).map(r => (((r.asset || {}).youtubeVideoAsset || {}).youtubeVideoId)).filter(Boolean);
      if (!ids.length) rows.push(C.ok("YouTube Data API", "key read from " + youtube.source + "; this account has no YouTube video asset to check"));
      else {
        const state = await require("./_youtubeVideos").videoStatus({ fetch, apiKey: youtube.key, videoIds: ids });
        rows.push(state.unserviceable || state.missing.length
          ? C.fail("YouTube Data API", state.detail + " · " + state.videos.filter(v => !v.serviceable).map(v => v.id + ": " + v.problems.join("; ")).join(" | ").slice(0, 300))
          : C.ok("YouTube Data API", state.detail));
      }
    } catch (e) { rows.push(C.fail("YouTube Data API", e.message)); }
  }

  // Offline conversions: the app's own record, then the Ads-side destination.
  try {
    const admin = require("./firebaseAdmin");
    const doc = await admin.firestore().collection("config").doc("googleAdsDataManager").get();
    rows.push(doc.exists ? C.ok("Data Manager connection", "a sealed credential record is saved") : C.warn("Data Manager connection", "no saved connection — Shopify offline conversions are not being uploaded"));
  } catch (e) { rows.push(C.skip("Data Manager connection", "Firestore unavailable here: " + e.message)); }
  rows.push(present("GADS_CONVERSION_ACTION")
    ? C.ok("conversion action configured", ENV.GADS_CONVERSION_ACTION)
    : C.warn("conversion action configured", "GADS_CONVERSION_ACTION is not set — offline uploads have no destination"));

  if (present("SHOPIFY_STORE")) {
    const store = ENV.SHOPIFY_STORE, api = ENV.SHOPIFY_API_VERSION || "2025-10";
    const token = ENV.SHOPIFY_ACCESS_TOKEN || ENV.SHOPIFY_ADMIN_TOKEN || "";
    if (!token) rows.push(C.skip("Shopify shop", "no Shopify admin token in this environment"));
    else try {
      const res = await fetch("https://" + store + "/admin/api/" + api + "/shop.json", { timeout: TIMEOUT, headers: { "X-Shopify-Access-Token": token } });
      const data = await res.json().catch(() => ({}));
      rows.push(res.ok
        ? C.ok("Shopify shop", (data.shop || {}).myshopify_domain + " · " + (data.shop || {}).currency)
        : C.fail("Shopify shop", res.status + " — " + JSON.stringify(data).slice(0, 160)));
    } catch (e) { rows.push(C.fail("Shopify shop", e.message)); }
  } else rows.push(C.skip("Shopify shop", "SHOPIFY_STORE is not set"));

  return { title: "Other Google services and the store", rows };
}

// ── 8. Creative format policy against Google's published requirements ───────
function formatSection() {
  const policy = require("../../brites-ad-format-policy");
  const rows = [];
  for (const spec of C.REQUIRED_IMAGE_FORMATS) {
    const label = spec.fieldType + " (" + spec.key + ")";
    rows.push(C.ok(label, "ratio " + spec.ratio + " · min " + spec.minWidth + "×" + spec.minHeight +
      " · recommended " + spec.recommendedWidth + "×" + spec.recommendedHeight + " · up to " + spec.maxCount +
      (spec.required ? " · required" : " · optional")));
  }
  const videoKeys = (policy.video.formats || []).map(f => f.key);
  for (const spec of C.REQUIRED_VIDEO_FORMATS) {
    rows.push(videoKeys.includes(spec.key)
      ? C.ok("video · " + spec.key, "exported by the creative policy · " + spec.why)
      : C.fail("video · " + spec.key, "NOT exported by brites-ad-format-policy.js · " + spec.why));
  }
  rows.push(policy.video.seconds >= 10
    ? C.ok("video length", policy.video.seconds + "s meets Google's 10s minimum for Performance Max")
    : C.fail("video length", policy.video.seconds + "s is below Google's 10s minimum"));
  return { title: "Creative requirements", note: "Google's published asset requirements, checked against this application's own creative policy.", rows };
}

// ── Runner ──────────────────────────────────────────────────────────────────
async function run(options) {
  const sections = [credentialSection()];
  let token = null, tokenError = null;
  if (present("GADS_CLIENT_ID") && present("GADS_CLIENT_SECRET") && present("GADS_REFRESH_TOKEN")) {
    try { token = await mintToken(ENV.GADS_CLIENT_ID, ENV.GADS_CLIENT_SECRET, ENV.GADS_REFRESH_TOKEN); }
    catch (e) { tokenError = e.message; }
  } else tokenError = "client id, secret or refresh token is missing";
  sections[0].rows.push(token ? C.ok("OAuth token exchange", "minted an access token")
    : C.fail("OAuth token exchange", tokenError, { remedy: C.remedyFor(tokenError) }));

  const access = await adsAccessSection(token);
  sections.push(access);
  sections.push(await scopeSection(token));
  sections.push(await adsResourceSection(token));
  sections.push(await adsSchemaSection(token, access.served));
  sections.push(await adsWriteSection(token, options.write));
  sections.push(await merchantSection());
  sections.push(await otherGoogleSection(token));
  sections.push(formatSection());

  return { checkedAt: new Date().toISOString(), apiVersion: V, customerId: CID || null, merchantId: MERCHANT || null, summary: C.summarize(sections), sections };
}

function html(result) {
  const dot = { ok: "🟢", FAIL: "🔴", warn: "🟡", skipped: "⚪" };
  const body = result.sections.map(section => {
    const rows = section.rows.map(r =>
      '<tr><td style="padding:6px 10px;vertical-align:top">' + (dot[r.status] || "·") + '</td>' +
      '<td style="padding:6px 10px;font-weight:600;vertical-align:top;white-space:nowrap">' + esc(r.name) + '</td>' +
      '<td style="padding:6px 10px;color:#444;font-family:ui-monospace,monospace;font-size:12px">' + esc(r.detail) +
      (r.why ? '<div style="color:#777;font-family:inherit;font-size:12px;margin-top:3px">' + esc(r.why) + '</div>' : "") +
      (r.remedy ? '<div style="color:#8a4b00;font-family:inherit;font-size:12px;margin-top:3px">→ ' + esc(r.remedy) + '</div>' : "") +
      '</td></tr>').join("");
    return '<h3 style="margin:26px 0 6px;font-weight:600">' + esc(section.title) + '</h3>' +
      (section.note ? '<p style="margin:0 0 8px;color:#666;font-size:13px">' + esc(section.note) + '</p>' : "") +
      '<table style="border-collapse:collapse;width:100%;border:1px solid #eee">' + rows + '</table>';
  }).join("");
  const s = result.summary;
  return '<!doctype html><meta charset="utf-8"><title>Google connections check</title>' +
    '<body style="font-family:-apple-system,Segoe UI,sans-serif;max-width:980px;margin:36px auto;padding:0 16px;color:#1a1a1a">' +
    '<h2 style="font-weight:600;margin:0">Google connections check</h2>' +
    '<p style="font-size:16px">' + s.ok + ' reached · ' + s.failed + ' failed · ' + s.warned + ' need attention · ' + s.skipped + ' not attempted' +
    '<br><span style="color:#666;font-size:13px">API ' + esc(result.apiVersion) + ' · customer ' + esc(result.customerId || "—") + ' · Merchant ' + esc(result.merchantId || "—") + ' · ' + esc(result.checkedAt) + '</span></p>' +
    '<p style="color:#666;font-size:13px;border-left:3px solid #ddd;padding-left:10px">A green row means Google answered this request. It does not mean an ad is serving: policy approval, delivery and billing are separate states. A grey row was never attempted, and is never counted as a pass.</p>' +
    body + '</body>';
}

exports.handler = async (event) => {
  const gate = (ENV.EDIT_PASSCODE || "").trim().replace(/^["']|["']$/g, "");
  const params = (event && event.queryStringParameters) || {};
  if (gate && String(params.key || "").trim() !== gate) {
    return { statusCode: 401, headers: { "Content-Type": "text/plain" }, body: "Add ?key=<EDIT_PASSCODE> to run the Google connections check." };
  }
  let result;
  try { result = await run({ write: String(params.write || "") === "1" }); }
  catch (e) { return { statusCode: 500, headers: { "Content-Type": "application/json" }, body: JSON.stringify({ error: String(e.message || e) }) }; }

  const wantsJson = String(params.format || "") === "json" || !/text\/html/.test(((event.headers || {}).accept) || "");
  return wantsJson
    ? { statusCode: 200, headers: { "Content-Type": "application/json" }, body: JSON.stringify(result, null, 2) }
    : { statusCode: 200, headers: { "Content-Type": "text/html; charset=utf-8" }, body: html(result) };
};

exports.run = run;
