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

const PRODUCT_PAGE = 1000;
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
  let pageToken = null, complete = false;
  for (let page = 0; page < MAX_PRODUCT_PAGES; page++) {
    // A LIMIT caps the whole result, not a page: it hid every offer after the first 250.
    // Read only offers that are not fully eligible, across the whole catalogue, by page.
    const data = await request('reports/v1/accounts/' + account + '/reports:search', 'POST', {
      query: "SELECT id, offer_id, title, aggregated_reporting_context_status, item_issues FROM product_view WHERE aggregated_reporting_context_status IN ('NOT_ELIGIBLE_OR_DISAPPROVED', 'PENDING', 'ELIGIBLE_LIMITED')",
      pageSize: PRODUCT_PAGE, ...(pageToken ? { pageToken } : {})
    });
    for (const row of data.results || []) rows.push(row.productView || {});
    pageToken = data.nextPageToken;
    if (!pageToken) { complete = true; break; }
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
    .filter(p => p.issues.length || ['NOT_ELIGIBLE_OR_DISAPPROVED', 'PENDING', 'ELIGIBLE_LIMITED'].includes(p.status));
  const disapproved = affected.filter(p => p.status === 'NOT_ELIGIBLE_OR_DISAPPROVED');
  return {
    scanned: rows.length, affected: affected.length, disapproved: disapproved.length,
    offers: affected.slice(0, limit),
    truncated: affected.length > limit || !complete, complete,
    detail: rows.length
      ? disapproved.length + ' offer(s) cannot serve and ' + (affected.length - disapproved.length) + ' are limited or pending' + (complete ? ', across the whole catalogue' : '; more such offers exist than were read')
      : 'No disapproved, limited or pending offers were returned.'
  };
}

// A UTF-8 title that a feed, app or file upload read as Latin-1 or Windows-1252
// shows "â", "Ã©" or "Â" where a dash, quote or accent belongs. Latin-1 also turns
// the rest of a dash into invisible control characters that Google drops, leaving a
// lone "â" (" â " for " – "). Nothing in Google Ads re-encodes a feed title, so the
// fix belongs wherever the title comes from.
const CP1252 = { 0x20ac: 0x80, 0x201a: 0x82, 0x192: 0x83, 0x201e: 0x84, 0x2026: 0x85, 0x2020: 0x86, 0x2021: 0x87, 0x2c6: 0x88, 0x2030: 0x89, 0x160: 0x8a, 0x2039: 0x8b, 0x152: 0x8c, 0x17d: 0x8e,
  0x2018: 0x91, 0x2019: 0x92, 0x201c: 0x93, 0x201d: 0x94, 0x2022: 0x95, 0x2013: 0x96, 0x2014: 0x97, 0x2dc: 0x98, 0x2122: 0x99, 0x161: 0x9a, 0x203a: 0x9b, 0x153: 0x9c, 0x17e: 0x9e, 0x178: 0x9f };
const TITLE_FIX = 'Fix it where the title comes from, not in Google Ads: in Merchant Center open Products, find the offer and check which data source supplies its title. If it is the Shopify Google & YouTube app, correct the product title in Shopify and save so the app resyncs it; if it is a file or Google Sheets feed, correct the text there and set that data source\'s encoding to UTF-8.';
// Lead characters of the sequences a misread English catalogue produces: Â Ã (Latin-1
// symbols and accents), Å Ë (Œ š ˜ …), â (dashes, quotes, bullets), ï (byte-order mark), ð (emoji).
// Other accented capitals stay text, so "CAFÉ”" is never "repaired".
const LEADS = { 0xc2: 1, 0xc3: 1, 0xc5: 1, 0xcb: 1, 0xe2: 2, 0xef: 2, 0xf0: 3 };
function titleProblem(title) {
  const s = String(title == null ? '' : title), byte = ch => { const c = ch ? ch.charCodeAt(0) : 0; return c >= 0x80 && c <= 0xbf ? c : CP1252[c] || 0; };
  let out = '', found = false, exact = true;
  for (let i = 0; i < s.length; i++) {
    const c = s.charCodeAt(i), n = LEADS[c] || 0, bytes = [c];
    while (n && bytes.length <= n && byte(s[i + bytes.length])) bytes.push(byte(s[i + bytes.length]));
    const decoded = n && bytes.length === n + 1 ? Buffer.from(bytes).toString('utf8') : '';
    if (decoded && !decoded.includes('�')) { out += decoded === '﻿' ? '' : decoded; i += n; found = true; continue; }
    // A lone "â" whose control characters were dropped: a spaced dash, or a contraction's apostrophe.
    if (c === 0xe2 && /(^|\s)$/.test(s.slice(0, i)) && /^(\s|$)/.test(s.slice(i + 1))) { out += '–'; found = true; exact = false; continue; }
    if (c === 0xe2 && /[A-Za-z]$/.test(s.slice(0, i)) && /^(s|t|ll|re|ve|d|m)(?![A-Za-zÀ-ɏ])/.test(s.slice(i + 1))) { out += '’'; found = true; exact = false; continue; }
    if (c === 0xe2 && byte(s[i + 1]) >= 0xa0) { i++; found = true; exact = false; continue; }
    if (c === 0xfffd || c === 0xfeff || (c >= 0x80 && c <= 0x9f)) { found = true; exact = false; continue; }
    out += s[i];
  }
  return found ? { problem: 'encoding', title: s, suggested: out.replace(/\s{2,}/g, ' ').trim(), exact,
    detail: 'The title shows garbled characters such as "â" where a dash, quote or accent belongs: it was read with the wrong text encoding before it reached Merchant Center.' } : null;
}
// One report notice for the offers whose feed title is garbled, in the order given.
function garbledTitlesNotice(rows, currency) {
  const list = (rows || []).filter(r => r && r.titleProblem);
  if (!list.length) return null;
  const named = list.slice(0, 3).map(r => r.itemId + ' (shown as "' + text(r.title, 70) + '"' + (Number(r.cost) > 0 ? ', ' + (currency ? currency + ' ' : '') + Number(r.cost).toFixed(2) + ' spent' : '') + '; suggested "' + text(r.titleProblem.suggested, 150) + '")');
  return 'Feed problem: Google shows a garbled title for ' + list.length + ' offer(s) in this report: ' + named.join('; ') + (list.length > 3 ? '; and ' + (list.length - 3) + ' more' : '') + '. Shoppers see these characters in the ads. ' + TITLE_FIX;
}

// Offers that serve with a garbled title. They are not disapproved, so the
// eligibility read above never lists them, yet shoppers see the damage in every ad.
async function titleIssues(request, account, limit) {
  const rows = [];
  let pageToken = null, complete = false;
  for (let page = 0; page < MAX_PRODUCT_PAGES; page++) {
    // Read only titles holding a character that misread UTF-8 leaves behind, then confirm each locally.
    const data = await request('reports/v1/accounts/' + account + '/reports:search', 'POST', {
      query: "SELECT id, offer_id, feed_label, title, aggregated_reporting_context_status FROM product_view WHERE title REGEXP_MATCH '[ÂÃÅËâïð�﻿]'",
      pageSize: PRODUCT_PAGE, ...(pageToken ? { pageToken } : {})
    });
    for (const row of data.results || []) rows.push(row.productView || {});
    pageToken = data.nextPageToken;
    if (!pageToken) { complete = true; break; }
  }
  const affected = rows.map(p => ({ p, problem: titleProblem(p.title) })).filter(x => x.problem).map(({ p, problem }) => ({
    offerId: text(p.offerId, 200), feedLabel: p.feedLabel || null, title: text(p.title, 160), status: p.aggregatedReportingContextStatus || 'UNKNOWN',
    suggestedTitle: text(problem.suggested, 160),
    issues: [{ description: 'Garbled title "' + text(p.title, 90) + '"; suggested "' + text(problem.suggested, 90) + '"', severity: 'WARNING', resolution: TITLE_FIX }]
  }));
  return {
    scanned: rows.length, affected: affected.length, offers: affected.slice(0, limit),
    truncated: affected.length > limit || !complete, complete,
    detail: affected.length
      ? affected.length + ' offer(s) show garbled characters such as "â" in their Google title' + (complete ? '' : ', and more titles exist than were read') + '. ' + TITLE_FIX
      : 'No garbled product titles were found' + (complete ? ' across the whole catalogue.' : ' in the offers read; more titles exist than were read.')
  };
}

// Google's own record of which Google Ads accounts this Merchant account serves.
// Merchant records a relationship per provider — Google Ads, Shopify, Google
// Shopping — and names the provider, never the advertising customer ID. Matching
// on digits therefore reported a live, working link as missing. Which Ads
// account is joined is answered from the Ads side by the product_link resource,
// not from here.
async function adsLink(request, account, adsCustomerId) {
  const data = await request('accounts/v1/accounts/' + account + '/relationships', 'GET');
  const links = (data.accountRelationships || []).map(r => ({
    provider: text(r.provider, 120), displayName: text(r.accountIdAlias || r.providerDisplayName || r.displayName, 120)
  }));
  const named = pattern => links.find(l => pattern.test(l.provider) || pattern.test(l.displayName));
  const googleAds = named(/GOOGLE_ADS|google ads/i), shopify = named(/shopify/i);
  return {
    links, googleAdsLinked: !!googleAds, shopifyLinked: !!shopify,
    detail: !links.length ? 'Google records no account relationships.'
      : (googleAds ? 'Google Ads is linked' : 'NO Google Ads relationship is recorded') +
        (shopify ? '; Shopify is linked' : '; no Shopify relationship is recorded') +
        '. Merchant names the provider, not the advertising customer ID' +
        (adsCustomerId ? ' — confirm ' + adsCustomerId + ' specifically from the Ads side with the product_link resource.' : '.')
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


// Where the Merchant account id comes from, in order of trust. An explicit
// setting beats anything Google reports; among Google's own records, the Ads
// account's Merchant link beats a campaign, and the accounts the Merchant OAuth
// token can list are the last resort. A source that names several different
// accounts is never guessed between: it is reported and the next source is tried.
//   env          GMC_MERCHANT_ID / MERCHANT_CENTER_ID
//   readConfig() Firestore config/googleApiKeys.merchantId (resolves to a string or null)
//   adsQuery(g)  resolves to the rows Google Ads returned
//   listAccounts() resolves to the Merchant account ids the OAuth token can list
// Every step is read-only, and a failing step is noted, not thrown.
const MERCHANT_ID_HELP = 'Add the numeric Merchant Center account id (digits only, shown top right in merchants.google.com) as the Netlify environment variable GMC_MERCHANT_ID, or as the field merchantId in Firestore config/googleApiKeys.';
const digits = v => String(v == null ? '' : v).replace(/\D/g, '');

async function resolveMerchantId(deps) {
  const { env, readConfig, adsQuery, listAccounts } = deps || {};
  const notes = [], seen = [];
  const explicit = digits((env || {}).GMC_MERCHANT_ID || (env || {}).MERCHANT_CENTER_ID);
  if (explicit) return { id: explicit, ids: [explicit], source: 'environment GMC_MERCHANT_ID', notes };
  if (readConfig) {
    try { const v = digits(await readConfig()); if (v) return { id: v, ids: [v], source: 'Firestore config/googleApiKeys.merchantId', notes }; }
    catch (e) { notes.push('Firestore config not readable: ' + String((e && e.message) || e)); }
  }
  const unique = list => [...new Set(list.map(digits).filter(Boolean))];
  const steps = [
    adsQuery && { source: 'the Google Ads product_link (Merchant Center link)', run: async () => unique((await adsQuery("SELECT product_link.merchant_center.merchant_center_id FROM product_link WHERE product_link.type = 'MERCHANT_CENTER'") || [])
      .map(r => (((r.productLink || {}).merchantCenter || {}).merchantCenterId))) },
    adsQuery && { source: 'a linked shopping campaign', run: async () => unique((await adsQuery("SELECT campaign.shopping_setting.merchant_id FROM campaign WHERE campaign.status != 'REMOVED' LIMIT 200") || [])
      .map(r => (((r.campaign || {}).shoppingSetting || {}).merchantId))) },
    listAccounts && { source: 'the accounts the Merchant OAuth token can list', run: async () => unique(await listAccounts() || []) }
  ].filter(Boolean);
  let chosen = null;
  for (const step of steps) {
    // The account list is only a fallback: skip it once anything else has spoken.
    if (step.source.startsWith('the accounts') && (chosen || seen.length)) continue;
    let ids = [];
    try { ids = await step.run(); } catch (e) { notes.push(step.source + ' failed: ' + String((e && e.message) || e)); continue; }
    ids.forEach(i => { if (!seen.includes(i)) seen.push(i); });
    if (ids.length === 1 && !chosen) chosen = { id: ids[0], source: step.source };
    else if (ids.length === 1 && chosen && ids[0] !== chosen.id) notes.push(step.source + ' names ' + ids[0] + ', not ' + chosen.id + '; using ' + chosen.id + ' from ' + chosen.source);
    else if (ids.length > 1) notes.push(step.source + ' names ' + ids.length + ' different Merchant accounts (' + ids.join(', ') + ')');
  }
  if (chosen) return { id: chosen.id, ids: seen, source: chosen.source, notes };
  if (seen.length > 1) return { id: null, ids: seen, notes, reason: 'several Merchant accounts are named (' + seen.join(', ') + ') and none was chosen. Set GMC_MERCHANT_ID (or merchantId in Firestore config/googleApiKeys) to the one this store uses.' };
  return { id: null, ids: seen, notes, reason: 'no Merchant account id could be found: it is not in GMC_MERCHANT_ID, Firestore config/googleApiKeys.merchantId, the Google Ads product_link, any shopping campaign, or the Merchant OAuth account list' + (notes.length ? ' (' + notes.join('; ') + ')' : '') + '. ' + MERCHANT_ID_HELP };
}

async function merchantHealth(input) {
  const { request, merchantId, adsCustomerId, offerLimit } = input || {};
  if (typeof request !== 'function') throw new Error('A Merchant API request function is required.');
  if (!/^\d+$/.test(String(merchantId || ''))) throw new Error('A numeric Merchant Center account ID is required.');
  const account = String(merchantId), limit = Number.isFinite(Number(offerLimit)) ? Math.max(1, Math.min(100, Number(offerLimit))) : 25;
  const out = {};
  await section(out, 'accountIssues', 'Account issues', () => accountIssues(request, account));
  await section(out, 'productIssues', 'Offer eligibility', () => productIssues(request, account, limit));
  await section(out, 'titleIssues', 'Product titles', () => titleIssues(request, account, limit));
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
  if (out.adsLink.status === 'available' && out.adsLink.googleAdsLinked === false) blocking.push('no Google Ads relationship is recorded');
  // Serving but visibly damaged: named beside the blockers, never counted as one.
  const attention = out.titleIssues.status === 'available' && out.titleIssues.affected ? [out.titleIssues.affected + ' offer(s) with a garbled title'] : [];

  return {
    checkedAt: Date.now(), merchantId: account, adsCustomerId: adsCustomerId || null,
    sections: out,
    summary: {
      read: sections.length - unavailable.length, unavailable: unavailable.length,
      blocking, attention, healthy: blocking.length === 0 && unavailable.length === 0
    },
    note: 'Read-only. Merchant Center reports eligibility; it does not report whether an eligible offer received traffic.'
  };
}

module.exports = { merchantHealth, resolveMerchantId, MERCHANT_ID_HELP, accountIssues, productIssues, titleIssues, titleProblem, garbledTitlesNotice, TITLE_FIX, adsLink, conversionSources, servingSettings, dataSources, promotions, autofeed };
