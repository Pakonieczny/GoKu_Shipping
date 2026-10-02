// netlify/functions/googleMerchantHealth.js
// ─────────────────────────────────────────────────────────────────────────────
// Read-only Merchant Center health:
//   https://goldenspike.app/.netlify/functions/googleMerchantHealth?key=<passcode>
//   …&format=json
//   …&ads=pending     instead: every complete ad waiting in Approvals, checked the
//                     way its Performance Max plan is checked before publishing —
//                     can its product's exact offers show in its target countries
//                     (approved / pending / disapproved for Shopping ads, stock,
//                     item issues), plus account issues and shipping coverage
//
// Answers what googleConnectionsCheck only proved reachable: which offers cannot
// serve and why, whether Google records the advertising link, and whether store
// conversions reach Merchant Center. Every Google call is a read. Its one write is
// to this app's Firestore: a Merchant account id found from the Google Ads Merchant
// link is recorded in config/googleApiKeys.merchantId while that field is empty.
// Needs the passcode, and refuses outright while no passcode is set (_adsCheckGate.js).
// ─────────────────────────────────────────────────────────────────────────────

const fetch = require('node-fetch');
const { merchantHealth, resolveMerchantId, offerReadiness, sharedReads, pendingAdTargets, countryCodesFor, accountIssueSummary } = require('./_merchantHealth');
const { refuse } = require('./_adsCheckGate');
const ENV = process.env;
const TIMEOUT = 20000;
const ADS_CID = (ENV.GADS_CUSTOMER_ID || '').replace(/\D/g, '');
const LOGIN = (ENV.GADS_LOGIN_CUSTOMER_ID || '').replace(/\D/g, '');
const V = ENV.GADS_API_VERSION || 'v24';
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


async function adsQuery(query) {
  const res = await fetch('https://oauth2.googleapis.com/token', {
    method: 'POST', timeout: TIMEOUT, headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body: new URLSearchParams({ client_id: ENV.GADS_CLIENT_ID || '', client_secret: ENV.GADS_CLIENT_SECRET || '', refresh_token: ENV.GADS_REFRESH_TOKEN || '', grant_type: 'refresh_token' })
  });
  const token = await res.json().catch(() => ({}));
  if (!res.ok || !token.access_token) throw new Error('Google Ads OAuth: ' + (token.error_description || token.error || res.status));
  const headers = { Authorization: 'Bearer ' + token.access_token, 'developer-token': ENV.GADS_DEVELOPER_TOKEN, 'Content-Type': 'application/json' };
  if (LOGIN) headers['login-customer-id'] = LOGIN;
  const search = await fetch('https://googleads.googleapis.com/' + V + '/customers/' + ADS_CID + '/googleAds:search', {
    method: 'POST', timeout: TIMEOUT, headers, body: JSON.stringify({ query })
  });
  const data = await search.json().catch(() => ({}));
  if (!search.ok) throw new Error('Google Ads: HTTP ' + search.status);
  return data.results || [];
}

function requestWith(token) {
  return async (path, method, body) => {
    const res = await fetch('https://merchantapi.googleapis.com/' + path, {
      method: method || 'GET', timeout: TIMEOUT,
      headers: { Authorization: 'Bearer ' + token, 'Content-Type': 'application/json' },
      ...(body ? { body: JSON.stringify(body) } : {})
    });
    const data = await res.json().catch(() => null);
    if (!res.ok || !data) throw Object.assign(new Error((data && data.error && data.error.message) || 'HTTP ' + res.status), { status: res.status });
    return data;
  };
}

// The complete ads waiting in Approvals, one at a time so Merchant reads stay paced; account issues and shipping
// settings are read once for all of them. Fresh reads (no cache): this is the owner's live proof.
async function pendingAdsReadiness(request, merchantId, deps) {
  const db = (deps && deps.db) || require('./firebaseAdmin').firestore();
  const snap = await db.collection('Brites_GAds_Approvals').where('type', '==', 'adDesignSubmission').where('status', '==', 'PENDING').limit(25).get();
  let defaults = [];
  try { const control = await db.collection('Brites_GAds_Control').doc('control').get(); defaults = (control.exists && control.data().defaultCountries) || []; } catch (_) { /* each ad's own countries come first anyway */ }
  const shared = sharedReads(request, merchantId), ads = [];
  for (const ad of pendingAdTargets(snap.docs.map(d => ({ id: d.id, ...d.data() })), defaults)) {
    const place = ad.countryCodes ? { codes: ad.countryCodes, unknown: [] } : countryCodesFor(ad.geoIds);
    const readiness = await offerReadiness({ request, merchantId, offerIds: ad.offerIds, countries: place.codes, unmapped: place.unknown.length, feedLabel: ad.feedLabel, shared, force: true });
    ads.push({ id: ad.id, title: ad.title, offerIds: ad.offerIds, countryCodes: place.codes, feedLabel: ad.feedLabel, styles: ad.styles, readiness });
  }
  const facts = await shared.accountIssues, count = s => ads.filter(a => a.readiness.status === s).length;
  return {
    checkedAt: Date.now(), merchantId, ads,
    accountIssues: facts.error ? { error: facts.error } : { blocking: facts.blocking.length, errors: facts.errors.length, detail: accountIssueSummary(facts) },
    summary: { ads: ads.length, ready: count('ready'), warning: count('warning'), blocked: count('blocked'), unavailable: count('unavailable'),
      healthy: ads.length > 0 && ads.every(a => a.readiness.status === 'ready') && !facts.error && !facts.blocking.length },
    note: 'Read-only. Shopping ads status per country, as Merchant Center reports it now. It does not show whether an eligible offer received traffic.'
  };
}

function adsHtml(result) {
  const s = result.summary, dot = { ready: '🟢', warning: '🟡', unavailable: '🟡', blocked: '🔴' };
  const where = list => (list || []).join(', ') || '—';
  const rows = result.ads.map(ad => {
    const r = ad.readiness, offers = (r.offers || []).map(o => '<li><b>' + esc(o.offerId) + '</b> · ' + esc(o.feedLabel || '') + ' · ' + esc((o.availability || 'availability unknown').toLowerCase().replace(/_/g, ' ')) +
      ' · approved ' + esc(where(o.approved)) + (o.pending.length ? ' · under review ' + esc(where(o.pending)) : '') + (o.disapproved.length ? ' · disapproved ' + esc(where(o.disapproved)) : '') +
      (o.issues.length ? ' — ' + esc(o.issues.map(i => i.description + ' [' + i.severity.toLowerCase() + (i.countries.length ? ' · ' + i.countries.join(', ') : '') + ']').join('; ')) : '') + '</li>').join('');
    return '<tr><td style="padding:8px 10px;vertical-align:top">' + (dot[r.status] || '·') + '</td><td style="padding:8px 10px;vertical-align:top"><b>' + esc(ad.title) + '</b><br><span style="color:#666;font-size:12px">' +
      esc(ad.offerIds.length + ' offer(s) · ' + (ad.countryCodes.length ? ad.countryCodes.join(', ') : 'every country') + (ad.feedLabel ? ' · feed ' + ad.feedLabel : '')) + '</span></td>' +
      '<td style="padding:8px 10px;color:#333;font-size:13px">' + esc(r.message) + (offers ? '<ul style="margin:6px 0 0;padding-left:18px;color:#555;font-size:12px">' + offers + '</ul>' : '') +
      (r.missing && r.missing.length ? '<p style="color:#8a4b00;font-size:12px">Not in Merchant Center: ' + esc(r.missing.join(', ')) + '</p>' : '') + '</td></tr>';
  }).join('');
  return '<!doctype html><meta charset="utf-8"><title>Merchant readiness of waiting ads</title>' +
    '<body style="font-family:-apple-system,Segoe UI,sans-serif;max-width:980px;margin:36px auto;padding:0 16px;color:#1a1a1a">' +
    '<h2 style="font-weight:600;margin:0">Merchant readiness of the complete ads waiting in Approvals</h2>' +
    '<p style="font-size:16px">' + (!s.ads ? 'No complete ad is waiting in Approvals.' : s.healthy ? 'All ' + s.ads + ' waiting ads: every offer can show in every target country, and no account issue stops offers serving.' :
      s.ads + ' waiting ad(s): ' + s.ready + ' ready · ' + s.warning + ' with warnings · ' + s.blocked + ' blocked · ' + s.unavailable + ' not readable') +
    '<br><span style="color:#666;font-size:13px">Account issues: ' + esc(result.accountIssues.detail || 'could not be read (' + result.accountIssues.error + ')') + '</span>' +
    '<br><span style="color:#666;font-size:13px">Merchant ' + esc(result.merchantId) + ' · from ' + esc(result.merchantIdSource || '—') + '</span></p>' +
    '<table style="border-collapse:collapse;width:100%;border:1px solid #eee">' + rows + '</table>' +
    '<p style="color:#666;font-size:12px;margin-top:14px">' + esc(result.note) + '</p></body>';
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
    ((s.attention || []).length ? '<br><span style="color:#8a4b00;font-size:13px">Needs attention: ' + esc(s.attention.join(' · ')) + '</span>' : '') +
    '<br><span style="color:#666;font-size:13px">Merchant ' + esc(result.merchantId) + ' · advertising account ' + esc(result.adsCustomerId || '—') + '</span></p>' +
    '<table style="border-collapse:collapse;width:100%;border:1px solid #eee">' + rows + '</table>' +
    '<p style="color:#666;font-size:12px;margin-top:14px">' + esc(result.note) + '</p></body>';
}

exports.handler = async (event) => {
  // Passcode only (?key=, X-Edit-Passcode or body passcode); refused while no passcode is set.
  const refused = await refuse(event, 'the Merchant Center health check');
  if (refused) return refused;
  const params = (event && event.queryStringParameters) || {}, adsMode = String(params.ads || '') === 'pending';
  let result;
  try {
    const token = await merchantToken();
    const found = await resolveMerchantId({
      env: ENV,
      readConfig: () => require('./_googleApiKeys').storedValue('merchantId'),
      saveConfig: id => require('./_googleApiKeys').saveStoredValueIfEmpty('merchantId', id),
      adsQuery: ADS_CID ? adsQuery : null,
      listAccounts: async () => ((await requestWith(token)('accounts/v1/accounts?pageSize=100', 'GET')).accounts || [])
        .map(a => a.accountId || String(a.name || '').replace(/^accounts\//, ''))
    });
    if (!found.id) throw new Error('No Merchant Center account could be resolved: ' + found.reason);
    result = adsMode
      ? await pendingAdsReadiness(requestWith(token), found.id)
      : await merchantHealth({ request: requestWith(token), merchantId: found.id, adsCustomerId: ADS_CID, offerLimit: params.offers });
    result.merchantIdSource = found.source;
    if (found.saved) result.merchantIdSaved = found.savedTo;
    if (found.notes.length) result.merchantIdNotes = found.notes;
  } catch (e) {
    return { statusCode: 200, headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ ok: false, error: String(e.message || e) }, null, 2) };
  }
  const wantsJson = String(params.format || '') === 'json' || !/text\/html/.test(((event.headers || {}).accept) || '');
  return wantsJson
    ? { statusCode: 200, headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(result, null, 2) }
    : { statusCode: 200, headers: { 'Content-Type': 'text/html; charset=utf-8' }, body: adsMode ? adsHtml(result) : html(result) };
};
exports.pendingAdsReadiness = pendingAdsReadiness;
