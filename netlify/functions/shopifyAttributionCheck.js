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
const fs = require('fs');
const path = require('path');
const { shopifyAttribution } = require('./_shopifyAttribution');
const ENV = process.env;
const TIMEOUT = 20000;
const STORE = ENV.SHOPIFY_STORE || '';
const API = ENV.SHOPIFY_API_VERSION || '2025-10';
const esc = s => String(s == null ? '' : s).replace(/[&<>"]/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' }[c]));

// The version this repository ships, so an older installed copy is visible.
function shippedVersion() {
  try {
    const file = path.join(__dirname, '../../shopify/snippets/brites-gclid-capture.liquid');
    return (fs.readFileSync(file, 'utf8').match(/brites-gclid-capture\/(\d+)/) || [])[1] || null;
  } catch (e) { return null; }
}

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

async function run() {
  const token = await shopifyToken();
  const result = await shopifyAttribution({
    request: requestWith(token),
    expectedHost: 'goldenspike.app',
    expectedVersion: shippedVersion()
  });
  try { result.sections.queue = { id: 'queue', label: 'Conversion upload queue', status: 'available', ...await queueHealth() }; }
  catch (e) { result.sections.queue = { id: 'queue', label: 'Conversion upload queue', status: 'unavailable', detail: String(e.message || e).slice(0, 200) }; }
  const q = result.sections.queue;
  if (q.status === 'available') {
    q.detail = q.failed ? q.failed + ' sale(s) Google refused and ' + q.pending + ' waiting to upload'
      : q.pending + ' conversion(s) waiting to upload; none refused';
    if (q.failed) result.summary.blocking.push(q.failed + ' refused conversion upload(s)');
  } else result.summary.unavailable++;
  result.summary.healthy = result.summary.blocking.length === 0 && result.summary.unavailable === 0;
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
  try { result = await run(); }
  catch (e) { return { statusCode: 200, headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ ok: false, error: String(e.message || e) }, null, 2) }; }
  const wantsJson = String(params.format || '') === 'json' || !/text\/html/.test(((event.headers || {}).accept) || '');
  return wantsJson
    ? { statusCode: 200, headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(result, null, 2) }
    : { statusCode: 200, headers: { 'Content-Type': 'text/html; charset=utf-8' }, body: html(result) };
};
