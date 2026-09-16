// netlify/functions/googleMerchantHealth.js
// ─────────────────────────────────────────────────────────────────────────────
// Read-only Merchant Center health:
//   https://goldenspike.app/.netlify/functions/googleMerchantHealth
//   …?format=json
//
// Answers what googleConnectionsCheck only proved reachable: which offers cannot
// serve and why, whether Google records the advertising link, and whether store
// conversions reach Merchant Center. Every call is a read.
// ─────────────────────────────────────────────────────────────────────────────

const fetch = require('node-fetch');
const { merchantHealth } = require('./_merchantHealth');
const ENV = process.env;
const TIMEOUT = 20000;
const MERCHANT = String(ENV.GMC_MERCHANT_ID || ENV.MERCHANT_CENTER_ID || '').replace(/\D/g, '');
const ADS_CID = (ENV.GADS_CUSTOMER_ID || '').replace(/\D/g, '');
const esc = s => String(s == null ? '' : s).replace(/[&<>"]/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' }[c]));

async function merchantToken() {
  const refresh = String(ENV.GMC_REFRESH_TOKEN || '').trim();
  if (!refresh) throw new Error('GMC_REFRESH_TOKEN is not set, so no Merchant Center call can be made.');
  const res = await fetch('https://oauth2.googleapis.com/token', {
    method: 'POST', timeout: TIMEOUT, headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body: new URLSearchParams({
      client_id: ENV.GMC_CLIENT_ID || ENV.GADS_CLIENT_ID || '', client_secret: ENV.GMC_CLIENT_SECRET || ENV.GADS_CLIENT_SECRET || '',
      refresh_token: refresh, grant_type: 'refresh_token'
    })
  });
  const data = await res.json().catch(() => ({}));
  if (!res.ok || !data.access_token) throw new Error('Merchant OAuth: ' + (data.error_description || data.error || res.status));
  return data.access_token;
}

function requestWith(token) {
  return async (path, method, body) => {
    const res = await fetch('https://merchantapi.googleapis.com/' + path, {
      method: method || 'GET', timeout: TIMEOUT,
      headers: { Authorization: 'Bearer ' + token, 'Content-Type': 'application/json' },
      ...(body ? { body: JSON.stringify(body) } : {})
    });
    const data = await res.json().catch(() => null);
    if (!res.ok || !data) throw new Error((data && data.error && data.error.message) || 'HTTP ' + res.status);
    return data;
  };
}

function html(result) {
  const s = result.summary;
  const rows = Object.values(result.sections).map(section => {
    const dot = section.status === 'available' ? '🟢' : '🟡';
    const extra = section.offers && section.offers.length
      ? '<ul style="margin:6px 0 0;padding-left:18px;color:#555;font-size:12px">' +
        section.offers.slice(0, 20).map(o => '<li><b>' + esc(o.offerId) + '</b> · ' + esc(o.status) + (o.issues.length ? ' — ' + esc(o.issues.map(i => i.description).join('; ')) : '') + '</li>').join('') +
        '</ul>' + (section.truncated ? '<p style="color:#888;font-size:12px">More affected offers exist than are listed.</p>' : '')
      : '';
    return '<tr><td style="padding:8px 10px;vertical-align:top">' + dot + '</td>' +
      '<td style="padding:8px 10px;font-weight:600;vertical-align:top;white-space:nowrap">' + esc(section.label) + '</td>' +
      '<td style="padding:8px 10px;color:#444;font-size:13px">' + esc(section.detail || '') + extra + '</td></tr>';
  }).join('');
  return '<!doctype html><meta charset="utf-8"><title>Merchant Center health</title>' +
    '<body style="font-family:-apple-system,Segoe UI,sans-serif;max-width:940px;margin:36px auto;padding:0 16px;color:#1a1a1a">' +
    '<h2 style="font-weight:600;margin:0">Merchant Center health</h2>' +
    '<p style="font-size:16px">' + (s.healthy ? 'Nothing is blocking offers from serving.' : (s.blocking.length ? 'Blocking: ' + esc(s.blocking.join(' · ')) : 'No blockers found')) +
    (s.unavailable ? '<br><span style="color:#8a4b00;font-size:13px">' + s.unavailable + ' section(s) could not be read, so those questions remain open.</span>' : '') +
    '<br><span style="color:#666;font-size:13px">Merchant ' + esc(result.merchantId) + ' · advertising account ' + esc(result.adsCustomerId || '—') + '</span></p>' +
    '<table style="border-collapse:collapse;width:100%;border:1px solid #eee">' + rows + '</table>' +
    '<p style="color:#666;font-size:12px;margin-top:14px">' + esc(result.note) + '</p></body>';
}

exports.handler = async (event) => {
  const gate = (ENV.EDIT_PASSCODE || '').trim().replace(/^["']|["']$/g, '');
  const params = (event && event.queryStringParameters) || {};
  if (gate && String(params.key || '').trim() !== gate) {
    return { statusCode: 401, headers: { 'Content-Type': 'text/plain' }, body: 'Add ?key=<EDIT_PASSCODE> to run the Merchant Center health check.' };
  }
  let result;
  try {
    if (!MERCHANT) throw new Error('Set GMC_MERCHANT_ID to the numeric Merchant Center account ID.');
    result = await merchantHealth({ request: requestWith(await merchantToken()), merchantId: MERCHANT, adsCustomerId: ADS_CID, offerLimit: params.offers });
  } catch (e) {
    return { statusCode: 200, headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ ok: false, error: String(e.message || e) }, null, 2) };
  }
  const wantsJson = String(params.format || '') === 'json' || !/text\/html/.test(((event.headers || {}).accept) || '');
  return wantsJson
    ? { statusCode: 200, headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(result, null, 2) }
    : { statusCode: 200, headers: { 'Content-Type': 'text/html; charset=utf-8' }, body: html(result) };
};
