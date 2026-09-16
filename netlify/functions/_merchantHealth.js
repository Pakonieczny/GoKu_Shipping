'use strict';

// Read-only Merchant Center health.
//
// Three questions the application could not previously answer:
//   1. What is stopping offers from serving right now (account and per-offer)?
//   2. Does Google's own record of the Google Ads link match the account we
//      advertise from?
//   3. Does Google record a conversion source for the store?
//
// Every call is a GET or a reports:search read. Nothing here mutates.
// https://developers.google.com/merchant/api/reference/rest
//
// `request(path, method, body)` is injected so this module is testable without
// network access, and so the caller owns credentials and timeouts.

const PRODUCT_PAGE = 250;
const MAX_PRODUCT_PAGES = 4;

function text(value, limit) { return String(value == null ? '' : value).slice(0, limit || 300); }

// A section that could not be read is recorded as unavailable with its reason.
// It is never silently dropped and never reported as healthy.
async function section(out, id, label, work) {
  try {
    const value = await work();
    out[id] = { id, label, status: 'available', ...value };
  } catch (error) {
    out[id] = { id, label, status: 'unavailable', detail: text(error && error.message || error, 300) };
  }
  return out[id];
}

async function accountIssues(request, account) {
  const data = await request('accounts/v1/accounts/' + account + '/issues', 'GET');
  const issues = (data.accountIssues || []).map(i => ({
    title: text(i.title, 200),
    severity: i.severity || 'UNKNOWN',
    impactedCountries: (i.impactedDestinations || []).flatMap(d => (d.impacts || []).map(x => x.regionCode)).filter(Boolean).slice(0, 12)
  }));
  const blocking = issues.filter(i => i.severity === 'ERROR' || i.severity === 'CRITICAL');
  return { issues, blocking: blocking.length, detail: issues.length ? blocking.length + ' blocking of ' + issues.length + ' account issue(s)' : 'No account issues reported.' };
}

// Per-offer disapprovals. A disapproved offer serves nothing and says nothing,
// so the exact offer IDs matter more than a count.
async function productIssues(request, account, limit) {
  const rows = [];
  let pageToken = null;
  for (let page = 0; page < MAX_PRODUCT_PAGES; page++) {
    const data = await request('reports/v1/accounts/' + account + '/reports:search', 'POST', {
      query: 'SELECT offer_id, title, aggregated_reporting_context_status, item_issues FROM product_view LIMIT ' + PRODUCT_PAGE,
      pageSize: PRODUCT_PAGE, ...(pageToken ? { pageToken } : {})
    });
    for (const row of data.results || []) rows.push(row.productView || {});
    pageToken = data.nextPageToken;
    if (!pageToken) break;
  }
  const severityOf = issue => ((issue.severity || {}).aggregatedSeverity) || issue.severity || 'UNKNOWN';
  const affected = rows
    .map(p => ({
      offerId: text(p.offerId, 200), title: text(p.title, 160),
      status: p.aggregatedReportingContextStatus || 'UNKNOWN',
      issues: (p.itemIssues || []).map(i => ({
        description: text((i.type || {}).description || (i.type || {}).code, 200),
        severity: severityOf(i),
        resolution: text((i.type || {}).canonicalAttribute ? 'attribute: ' + (i.type || {}).canonicalAttribute : (i.resolution || ''), 120)
      }))
    }))
    .filter(p => p.issues.length || ['NOT_ELIGIBLE_OR_DISAPPROVED', 'PENDING'].includes(p.status));
  const disapproved = affected.filter(p => p.status === 'NOT_ELIGIBLE_OR_DISAPPROVED');
  return {
    scanned: rows.length, affected: affected.length, disapproved: disapproved.length,
    offers: affected.slice(0, limit),
    truncated: affected.length > limit,
    detail: rows.length
      ? disapproved.length + ' offer(s) cannot serve and ' + (affected.length - disapproved.length) + ' carry warnings, of ' + rows.length + ' read'
      : 'No products were returned for this account.'
  };
}

// Google's own record of which Google Ads accounts this Merchant account serves.
async function adsLink(request, account, adsCustomerId) {
  const data = await request('accounts/v1/accounts/' + account + '/relationships', 'GET');
  const links = (data.accountRelationships || []).map(r => ({
    provider: text(r.provider, 120), displayName: text(r.accountIdAlias || r.providerDisplayName, 120)
  }));
  const wanted = String(adsCustomerId || '').replace(/\D/g, '');
  // The relationship names the provider account; match on the digits so a
  // dashed or prefixed form still resolves.
  const matched = wanted ? links.some(l => (l.provider + ' ' + l.displayName).replace(/\D/g, '').includes(wanted)) : null;
  return {
    links, matchedAdvertiser: matched,
    detail: !links.length ? 'Google records no account relationships.'
      : matched === null ? links.length + ' relationship(s); no Google Ads customer ID was supplied to match against.'
        : matched ? 'The advertising account ' + wanted + ' is linked.'
          : 'None of the ' + links.length + ' recorded relationships names the advertising account ' + wanted + '.'
  };
}

async function conversionSources(request, account) {
  const data = await request('conversions/v1/accounts/' + account + '/conversionSources?pageSize=50', 'GET');
  const sources = (data.conversionSources || []).map(s => ({
    name: text(s.name, 200), state: s.state || 'UNKNOWN',
    kind: s.googleAnalyticsLink ? 'GOOGLE_ANALYTICS' : s.merchantCenterDestination ? 'MERCHANT_CENTER_DESTINATION' : 'OTHER'
  }));
  const active = sources.filter(s => s.state === 'ACTIVE');
  return {
    sources, active: active.length,
    detail: sources.length ? active.length + ' active of ' + sources.length + ' conversion source(s)'
      : 'Google records no conversion source for this account, so store conversions are not reaching Merchant Center.'
  };
}

// Settings that decide whether an offer can serve at all, regardless of its feed.
async function servingSettings(request, account) {
  const shipping = await request('accounts/v1/accounts/' + account + '/shippingSettings', 'GET').catch(e => ({ _error: e.message }));
  const returns = await request('accounts/v1/accounts/' + account + '/onlineReturnPolicies?pageSize=10', 'GET').catch(e => ({ _error: e.message }));
  const services = (shipping && shipping.services) || [];
  const policies = (returns && returns.onlineReturnPolicies) || [];
  return {
    shippingServices: services.length, returnPolicies: policies.length,
    shippingError: shipping && shipping._error ? text(shipping._error, 200) : null,
    returnsError: returns && returns._error ? text(returns._error, 200) : null,
    detail: services.length + ' shipping service(s) and ' + policies.length + ' return policy/policies configured'
  };
}

// Which feed owns each product. A Shopify-managed source must not be overwritten.
async function dataSources(request, account) {
  const data = await request('datasources/v1/accounts/' + account + '/dataSources?pageSize=50', 'GET');
  const sources = (data.dataSources || []).map(s => ({
    name: text(s.name, 200), displayName: text(s.displayName, 120),
    input: s.input || 'UNKNOWN',
    primary: !!s.primaryProductDataSource,
    feedLabel: (s.primaryProductDataSource || {}).feedLabel || null,
    contentLanguage: (s.primaryProductDataSource || {}).contentLanguage || null
  }));
  return { sources, detail: sources.length + ' data source(s); ' + sources.filter(s => s.primary).length + ' primary' };
}

async function promotions(request, account) {
  const data = await request('promotions/v1/accounts/' + account + '/promotions?pageSize=25', 'GET');
  const rows = (data.promotions || []).map(p => ({ id: text(p.promotionId, 120), status: (p.promotionStatus || {}).destinationStatuses ? 'REPORTED' : 'UNKNOWN' }));
  return { promotions: rows.length, detail: rows.length ? rows.length + ' promotion(s) configured' : 'No Merchant promotions configured; sale messaging does not reach Shopping surfaces.' };
}

async function autofeed(request, account) {
  const data = await request('accounts/v1/accounts/' + account + '/autofeedSettings', 'GET');
  return { enabled: data.enableProducts === true, detail: data.enableProducts === true ? 'Google is crawling the store directly as a feed source.' : 'Automatic crawling is off; products come from the configured feeds only.' };
}

async function merchantHealth(input) {
  const { request, merchantId, adsCustomerId, offerLimit } = input || {};
  if (typeof request !== 'function') throw new Error('A Merchant API request function is required.');
  if (!/^\d+$/.test(String(merchantId || ''))) throw new Error('A numeric Merchant Center account ID is required.');
  const account = String(merchantId), limit = Number.isFinite(Number(offerLimit)) ? Math.max(1, Math.min(100, Number(offerLimit))) : 25;
  const out = {};
  await section(out, 'accountIssues', 'Account issues', () => accountIssues(request, account));
  await section(out, 'productIssues', 'Offer eligibility', () => productIssues(request, account, limit));
  await section(out, 'adsLink', 'Google Ads link', () => adsLink(request, account, adsCustomerId));
  await section(out, 'conversionSources', 'Conversion sources', () => conversionSources(request, account));
  await section(out, 'servingSettings', 'Shipping and returns', () => servingSettings(request, account));
  await section(out, 'dataSources', 'Feed ownership', () => dataSources(request, account));
  await section(out, 'promotions', 'Promotions', () => promotions(request, account));
  await section(out, 'autofeed', 'Automatic crawling', () => autofeed(request, account));

  const sections = Object.values(out);
  const unavailable = sections.filter(s => s.status === 'unavailable');
  // Only an observed answer can establish health. An unread section leaves the
  // question open rather than assuming the answer is "nothing wrong".
  const blocking = [];
  if (out.accountIssues.status === 'available' && out.accountIssues.blocking) blocking.push(out.accountIssues.blocking + ' account issue(s)');
  if (out.productIssues.status === 'available' && out.productIssues.disapproved) blocking.push(out.productIssues.disapproved + ' offer(s) that cannot serve');
  if (out.conversionSources.status === 'available' && !out.conversionSources.active) blocking.push('no active conversion source');
  if (out.adsLink.status === 'available' && out.adsLink.matchedAdvertiser === false) blocking.push('the advertising account is not linked');

  return {
    checkedAt: Date.now(), merchantId: account, adsCustomerId: adsCustomerId || null,
    sections: out,
    summary: {
      read: sections.length - unavailable.length, unavailable: unavailable.length,
      blocking, healthy: blocking.length === 0 && unavailable.length === 0
    },
    note: 'Read-only. Merchant Center reports eligibility; it does not report whether an eligible offer received traffic.'
  };
}

module.exports = { merchantHealth, accountIssues, productIssues, adsLink, conversionSources, servingSettings, dataSources, promotions, autofeed };
