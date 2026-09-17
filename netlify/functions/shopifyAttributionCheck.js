// netlify/functions/shopifyAttributionCheck.js
// ─────────────────────────────────────────────────────────────────────────────
// Verifies the Shopify half of revenue attribution, which is the only part of
// the chain that can be broken right now with nothing turning red:
//
//   https://goldenspike.app/.netlify/functions/shopifyAttributionCheck
//   …?format=json
//
// Read-only: theme assets and webhook registrations, plus the conversion upload
// queue this application already keeps.
// ─────────────────────────────────────────────────────────────────────────────

const fetch = require('node-fetch');
const { shopifyAttribution } = require('./_shopifyAttribution');
const ENV = process.env;
const TIMEOUT = 20000;
const STORE = ENV.SHOPIFY_STORE || '';
const API = ENV.SHOPIFY_API_VERSION || '2025-10';
const esc = s => String(s == null ? '' : s).replace(/[&<>"]/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' }[c]));

async function shopifyToken() {
  if (!STORE) throw new Error('SHOPIFY_STORE is not set.');
  if (!ENV.SHOPIFY_CLIENT_ID || !ENV.SHOPIFY_CLIENT_SECRET) throw new Error('SHOPIFY_CLIENT_ID and SHOPIFY_CLIENT_SECRET are required.');
  const res = await fetch('https://' + STORE + '/admin/oauth/access_token', {
    method: 'POST', timeout: TIMEOUT, headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body: new URLSearchParams({ grant_type: 'client_credentials', client_id: ENV.SHOPIFY_CLIENT_ID, client_secret: ENV.SHOPIFY_CLIENT_SECRET })
  });
  const data = await res.json().catch(() => ({}));
  if (!res.ok || !data.access_token) throw new Error('Shopify OAuth: ' + (data.error_description || data.error || res.status));
  return data.access_token;
}

function requestWith(token) {
  return async (p) => {
    const res = await fetch('https://' + STORE + '/admin/api/' + API + '/' + p, {
      timeout: TIMEOUT, headers: { 'X-Shopify-Access-Token': token, 'Content-Type': 'application/json' }
    });
    const data = await res.json().catch(() => null);
    if (!res.ok || !data) throw new Error('HTTP ' + res.status + (data && data.errors ? ' — ' + JSON.stringify(data.errors).slice(0, 140) : ''));
    return data;
  };
}

// What the store recorded but Google never accepted. These are sales that will
// never appear in campaign metrics, ROAS or the daily charts.
async function queueHealth() {
  const admin = require('./firebaseAdmin');
  const db = admin.firestore();
  const pending = await db.collection('Brites_GAds_ConvQueue').where('uploaded', '==', false).limit(500).get();
  const failed = await db.collection('Brites_GAds_ConvQueue').where('failed', '==', true).limit(50).get();
  const samples = [];
  failed.forEach(d => { const x = d.data(); if (samples.length < 5) samples.push({ orderId: x.orderId || null, value: x.value ?? null, error: String(x.uploadError || '').slice(0, 160) }); });
  return { pending: pending.size, failed: failed.size, samples };
}


// Why Google refused a sale, asked without uploading anything.
//
// A definitively rejected row is never retried, so its reason is frozen at
// whatever was recorded when it failed. This re-runs the live transport's own
// submit path with validateOnly, which builds the identical payload and mutates
// no record: the dry-run branch validates and moves on without touching the
// document. Opt-in via ?diagnose=1, because it is still one request per sale.
async function diagnoseRefusals(limit) {
  if (ENV.GADS_CONVERSION_UPLOAD_API === 'legacy')
    return { checked: 0, reasons: [], detail: 'Uploads are pinned to the legacy service, which Google refuses for accounts not allowlisted for it. That, not a per-sale fault, is the cause.' };

  const service = require('./googleAdsDataManager').createDataManager({
    env: ENV, fetch: require('node-fetch'),
    fb: () => ({ db: require('./firebaseAdmin').firestore() }),
    COL: { convQueue: 'Brites_GAds_ConvQueue' }, ledger: async () => {}
  });
  // dryRun makes every submission validateOnly; retryRejected reaches the rows
  // that were parked as definitively rejected.
  const result = await service.run({ ctrl: { dryRun: true }, limit: limit || 25, retryRejected: true });

  // The same cause repeated fifteen times is one problem, not fifteen.
  const grouped = new Map();
  for (const e of result.errors || []) {
    const key = String(e.error || 'unknown');
    const at = grouped.get(key) || { reason: key, count: 0, orders: [], value: 0 };
    at.count++; if (at.orders.length < 5) at.orders.push(e.orderId);
    grouped.set(key, at);
  }
  const reasons = [...grouped.values()].sort((a, b) => b.count - a.count);
  return {
    checked: (result.validated || 0) + (result.rejected || 0),
    validated: result.validated || 0, reasons,
    detail: reasons.length
      ? reasons.map(r => r.count + '× ' + r.reason).join(' · ').slice(0, 500)
      : (result.validated || 0) + ' refused sale(s) now validate cleanly and would upload on the next retry.'
  };
}

// Google no longer allowlists ConversionUploadService for new integrations and
// refuses its uploads outright. Data Manager is this application's default;
// GADS_CONVERSION_UPLOAD_API=legacy forces the deprecated path. Which one is in
// use decides whether refused sales are a configuration problem or a credential
// one, so the report names it rather than leaving it to be guessed.
async function uploadTransport() {
  const legacy = ENV.GADS_CONVERSION_UPLOAD_API === 'legacy';
  const row = { transport: legacy ? 'legacy ConversionUploadService' : 'Data Manager API', legacy };
  if (legacy) {
    row.detail = 'GADS_CONVERSION_UPLOAD_API is set to "legacy", so uploads use ConversionUploadService — which Google refuses for accounts not allowlisted for it. Remove that variable to use the Data Manager API this application already supports.';
    return row;
  }
  const health = await require('./googleAdsDataManager').createDataManager({
    env: ENV, fetch: require('node-fetch'),
    fb: () => ({ db: require('./firebaseAdmin').firestore() }),
    COL: { convQueue: 'Brites_GAds_ConvQueue' }, ledger: async () => {}
  }).health({ probeScopes: true });
  Object.assign(row, health);
  if (!health.configured) {
    row.detail = 'Data Manager is selected but not authorised: connect its own OAuth credentials before any order can upload.';
    return row;
  }
  // Google documents both scopes as required. A credential missing the
  // companion one still mints a token, so the gap only shows as a refusal at
  // ingest that never mentions scopes.
  const missing = health.missingScopes || [];
  row.detail = (missing.length ? 'The saved credential is missing ' + missing.join(' and ') + '; Google documents both as required. Re-consent this connection with both. · ' : '') +
    health.confirmed + ' confirmed upload(s) to the configured conversion action · ' +
    health.processing + ' processing · ' + health.unknown + ' in an unknown state · ' +
    (health.awaitingAccess || 0) + ' waiting on account access · ' +
    (health.awaitingRetry || 0) + ' refused by a payload since corrected, retrying on the next scheduled upload · ' +
    health.retryable + ' retryable';
  return row;
}

async function run(options) {
  const token = await shopifyToken();
  // Read the queue first: whether orders are arriving decides how an invisible
  // webhook should be reported.
  let queue = null;
  try { queue = await queueHealth(); } catch (e) { queue = { error: String(e.message || e).slice(0, 200) }; }
  const result = await shopifyAttribution({
    request: requestWith(token),
    expectedHost: 'goldenspike.app',
    ordersArriving: !!queue && !queue.error && (queue.pending > 0 || queue.failed > 0)
  });
  result.sections.queue = queue && !queue.error
    ? { id: 'queue', label: 'Conversion upload queue', status: 'available', ...queue }
    : { id: 'queue', label: 'Conversion upload queue', status: 'unavailable', detail: (queue && queue.error) || 'unavailable' };
  const q = result.sections.queue;
  if (q.status === 'available') {
    q.detail = q.failed ? q.failed + ' sale(s) Google refused and ' + q.pending + ' waiting to upload'
      : q.pending + ' conversion(s) waiting to upload; none refused';
    if (q.failed) result.summary.blocking.push(q.failed + ' refused conversion upload(s)');
  } else result.summary.unavailable++;
  result.summary.healthy = result.summary.blocking.length === 0 && result.summary.unavailable === 0;
  try { result.sections.transport = { id: 'transport', label: 'Conversion upload transport', status: 'available', ...await uploadTransport() }; }
  catch (e) { result.sections.transport = { id: 'transport', label: 'Conversion upload transport', status: 'unavailable', detail: String(e.message || e).slice(0, 240) }; }
  const t = result.sections.transport;
  if (t.status === 'available' && (t.legacy || t.configured === false))
    result.summary.blocking.push(t.legacy ? 'uploads still use the deprecated ConversionUploadService' : 'Data Manager is not authorised');
  if (t.status === 'available' && (t.missingScopes || []).length)
    result.summary.blocking.push('the Data Manager credential is missing ' + t.missingScopes.join(' and '));
  result.summary.healthy = result.summary.blocking.length === 0 && result.summary.unavailable === 0;

  if (options && options.diagnose) {
    try { result.sections.refusals = { id: 'refusals', label: 'Why Google refused them', status: 'available', ...await diagnoseRefusals(options.diagnoseLimit) }; }
    catch (e) { result.sections.refusals = { id: 'refusals', label: 'Why Google refused them', status: 'unavailable', detail: String(e.message || e).slice(0, 240) }; }
  }
  const all = Object.values(result.sections);
  result.summary.sections = all.length;
  result.summary.unavailable = all.filter(s => s.status !== 'available').length;
  result.summary.read = all.length - result.summary.unavailable;
  result.summary.healthy = result.summary.blocking.length === 0 && result.summary.unavailable === 0;
  result.store = STORE;
  return result;
}

function html(r) {
  const s = r.summary;
  const rows = Object.values(r.sections).map(sec => {
    const dot = sec.status !== 'available' ? '⚪' : (s.blocking.length && /not installed|never render|missing webhook|refused/.test(sec.detail || '')) ? '🔴' : (sec.problems && sec.problems.length) || (sec.failed) ? '🟡' : '🟢';
    let extra = '';
    if (sec.topics) {
      // When orders are arriving, a webhook this API cannot see is firing, and
      // the topic rows must not contradict the heading that says so.
      const invisibleOnly = /orders are reaching the conversion queue/.test((r.summary.warnings || []).join(' '));
      extra = '<ul style="margin:6px 0 0;padding-left:18px;font-size:12px;color:#555">' + sec.topics.map(t =>
        '<li>' + (t.ok ? '🟢' : invisibleOnly ? '🟡' : '🔴') + ' <b>' + esc(t.topic) + '</b> — ' +
        (t.ok ? 'registered to this app' : invisibleOnly ? 'not visible to this app; one this API cannot see is firing' : 'NOT registered, and no orders are arriving') +
        '<br><span style="color:#777">' + esc(t.why) + '</span></li>').join('') + '</ul>';
    }
    if (sec.reasons && sec.reasons.length) extra = '<ul style="margin:6px 0 0;padding-left:18px;font-size:12px;color:#555">' + sec.reasons.map(r =>
      '<li><b>' + r.count + ' sale(s)</b>' + (r.value ? ' worth ' + r.value.toFixed(2) : '') + ' — ' + esc(r.reason) + '<br><span style="color:#777">' + esc((r.orders || []).join(', ')) + '</span></li>').join('') + '</ul>';
    if (sec.samples && sec.samples.length) extra = '<ul style="margin:6px 0 0;padding-left:18px;font-size:12px;color:#555">' + sec.samples.map(x =>
      '<li>order ' + esc(x.orderId) + ' — ' + esc(x.error) + '</li>').join('') + '</ul>';
    return '<tr><td style="padding:8px 10px;vertical-align:top">' + dot + '</td>' +
      '<td style="padding:8px 10px;font-weight:600;vertical-align:top;white-space:nowrap">' + esc(sec.label) + '</td>' +
      '<td style="padding:8px 10px;color:#444;font-size:13px">' + esc(sec.detail || '') + extra + '</td></tr>';
  }).join('');
  return '<!doctype html><meta charset="utf-8"><title>Shopify attribution check</title>' +
    '<body style="font-family:-apple-system,Segoe UI,sans-serif;max-width:940px;margin:36px auto;padding:0 16px;color:#1a1a1a">' +
    '<h2 style="font-weight:600;margin:0">Shopify attribution check</h2>' +
    '<p style="font-size:16px">' + (s.healthy ? 'Every link in the attribution chain is in place.' : 'Blocking: ' + esc(s.blocking.join(' · '))) +
    (s.warnings && s.warnings.length ? '<br><span style="color:#8a4b00;font-size:13px">' + esc(s.warnings.join(' · ')) + '</span>' : '') +
    '<br><span style="color:#666;font-size:13px">' + esc(r.store) + ' · ' + new Date(r.checkedAt).toISOString() + '</span></p>' +
    '<p style="color:#666;font-size:13px;border-left:3px solid #ddd;padding-left:10px">This is the half of revenue attribution that lives outside Google. When it breaks, paid orders still record — they simply stop carrying the click id, and Google Ads under-reports the revenue it earned.</p>' +
    '<table style="border-collapse:collapse;width:100%;border:1px solid #eee">' + rows + '</table>' +
    '<p style="color:#666;font-size:12px;margin-top:14px">' + esc(r.note) + '</p></body>';
}

exports.handler = async (event) => {
  const gate = (ENV.EDIT_PASSCODE || '').trim().replace(/^["']|["']$/g, '');
  const params = (event && event.queryStringParameters) || {};
  if (gate && String(params.key || '').trim() !== gate) {
    return { statusCode: 401, headers: { 'Content-Type': 'text/plain' }, body: 'Add ?key=<EDIT_PASSCODE> to run the Shopify attribution check.' };
  }
  let result;
  try { result = await run({ diagnose: String(params.diagnose || '') === '1', diagnoseLimit: Number(params.limit) || 25 }); }
  catch (e) { return { statusCode: 200, headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ ok: false, error: String(e.message || e) }, null, 2) }; }
  const wantsJson = String(params.format || '') === 'json' || !/text\/html/.test(((event.headers || {}).accept) || '');
  return wantsJson
    ? { statusCode: 200, headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(result, null, 2) }
    : { statusCode: 200, headers: { 'Content-Type': 'text/html; charset=utf-8' }, body: html(result) };
};
