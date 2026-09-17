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
// A refused row is retried until its attempts are exhausted, and then never
// again — so the reason it failed is frozen at whatever was recorded at the
// time, which until recently was Google's generic "There was a problem with the
// request." validateOnly re-submits the same payload and returns the same
// per-row errors, recording nothing: Google documents it as non-mutating.
//
// Opt-in via ?diagnose=1, because it is still an outbound request per sale.
const CLICK_ID_ORDER = ['gclid', 'gbraid', 'wbraid'];

async function diagnoseRefusals(limit) {
  const action = ENV.GADS_CONVERSION_ACTION;
  if (!action) throw new Error('GADS_CONVERSION_ACTION is not set, so there is no destination to validate against.');
  const cid = (ENV.GADS_CUSTOMER_ID || '').replace(/\D/g, '');
  if (!/^\d{10}$/.test(cid)) throw new Error('GADS_CUSTOMER_ID is not a ten-digit account.');

  const db = require('./firebaseAdmin').firestore();
  const snap = await db.collection('Brites_GAds_ConvQueue').where('failed', '==', true).limit(limit || 25).get();
  const rows = [];
  snap.forEach(d => rows.push(d.data()));
  if (!rows.length) return { checked: 0, reasons: [], detail: 'No refused conversions are on record.' };

  const conversions = [], described = [];
  for (const x of rows) {
    const kind = CLICK_ID_ORDER.find(k => x[k]);
    if (!kind) { described.push({ orderId: x.orderId || null, value: x.value ?? null, reason: 'no Google click id was ever captured for this order' }); continue; }
    const c = { conversionAction: action, conversionDateTime: x.conversionDateTime, conversionValue: x.value, currencyCode: x.currency, orderId: x.orderId || undefined };
    c[kind] = x[kind];
    conversions.push(c);
    described.push({ orderId: x.orderId || null, value: x.value ?? null, clickKind: kind, index: conversions.length - 1 });
  }

  if (conversions.length) {
    const auth = await fetch('https://oauth2.googleapis.com/token', {
      method: 'POST', timeout: TIMEOUT, headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
      body: new URLSearchParams({ client_id: ENV.GADS_CLIENT_ID || '', client_secret: ENV.GADS_CLIENT_SECRET || '', refresh_token: ENV.GADS_REFRESH_TOKEN || '', grant_type: 'refresh_token' })
    });
    const token = await auth.json().catch(() => ({}));
    if (!auth.ok || !token.access_token) throw new Error('Google Ads OAuth: ' + (token.error_description || token.error || auth.status));
    const headers = { Authorization: 'Bearer ' + token.access_token, 'developer-token': ENV.GADS_DEVELOPER_TOKEN, 'Content-Type': 'application/json' };
    const login = (ENV.GADS_LOGIN_CUSTOMER_ID || '').replace(/\D/g, '');
    if (login) headers['login-customer-id'] = login;

    const res = await fetch('https://googleads.googleapis.com/' + (ENV.GADS_API_VERSION || 'v24') + '/customers/' + cid + ':uploadClickConversions', {
      method: 'POST', timeout: TIMEOUT, headers,
      body: JSON.stringify({ conversions, partialFailure: true, validateOnly: true })
    });
    const data = await res.json().catch(() => ({}));
    if (!res.ok) throw new Error('Google Ads refused the validation itself: HTTP ' + res.status + ' ' + JSON.stringify(data).slice(0, 200));
    const errors = indexErrors(data.partialFailureError);
    for (const row of described) {
      if (row.index == null) continue;
      row.reason = errors[row.index] || 'accepted on validation — this sale would upload now';
      delete row.index;
    }
  }

  // The same reason repeated fifteen times is one problem, not fifteen.
  const grouped = new Map();
  for (const row of described) {
    const key = String(row.reason || 'unknown');
    const at = grouped.get(key) || { reason: key, count: 0, orders: [], value: 0 };
    at.count++; at.value += Number(row.value) || 0;
    if (at.orders.length < 5) at.orders.push(row.orderId);
    grouped.set(key, at);
  }
  const reasons = [...grouped.values()].sort((a, b) => b.count - a.count);
  return {
    checked: described.length, reasons,
    detail: reasons.map(r => r.count + '× ' + r.reason).join(' · ').slice(0, 400)
  };
}

// Google's per-row rejections arrive in partialFailureError, addressed by index.
// The errorCode names the cause; the message beside it is usually generic.
function indexErrors(pf) {
  const out = {};
  for (const d of (pf && pf.details) || []) for (const er of d.errors || []) {
    const hit = (((er.location || {}).fieldPathElements) || []).find(e => e && e.fieldName === 'conversions' && e.index != null);
    if (!hit) continue;
    const code = er.errorCode ? Object.keys(er.errorCode).map(k => k + ':' + er.errorCode[k]).join(',') : '';
    const message = String(er.message || '').trim();
    out[Number(hit.index)] = (code && message ? code + ' — ' + message : code || message || 'rejected').slice(0, 300);
  }
  return out;
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
  if (options && options.diagnose) {
    try { result.sections.refusals = { id: 'refusals', label: 'Why Google refused them', status: 'available', ...await diagnoseRefusals(options.diagnoseLimit) }; }
    catch (e) { result.sections.refusals = { id: 'refusals', label: 'Why Google refused them', status: 'unavailable', detail: String(e.message || e).slice(0, 240) }; }
  }
  result.store = STORE;
  return result;
}

function html(r) {
  const s = r.summary;
  const rows = Object.values(r.sections).map(sec => {
    const dot = sec.status !== 'available' ? '⚪' : (s.blocking.length && /not installed|never render|missing webhook|refused/.test(sec.detail || '')) ? '🔴' : (sec.problems && sec.problems.length) || (sec.failed) ? '🟡' : '🟢';
    let extra = '';
    if (sec.topics) extra = '<ul style="margin:6px 0 0;padding-left:18px;font-size:12px;color:#555">' + sec.topics.map(t =>
      '<li>' + (t.ok ? '🟢' : '🔴') + ' <b>' + esc(t.topic) + '</b> — ' + (t.ok ? 'registered' : 'NOT registered to this app') + '<br><span style="color:#777">' + esc(t.why) + '</span></li>').join('') + '</ul>';
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
