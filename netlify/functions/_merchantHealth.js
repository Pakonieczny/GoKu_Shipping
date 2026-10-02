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

// Google's account issue severities: CRITICAL "causes offers to not serve", ERROR
// "might affect offers (in the future)", SUGGESTION an improvement. Each impacted
// region carries its own severity, so an issue can stop offers in one country only.
// https://developers.google.com/merchant/api/reference/rest/accounts_v1/accounts.issues
function accountIssueFacts(data) {
  const issues = ((data && data.accountIssues) || []).map(i => {
    const severity = String(i.severity || 'SEVERITY_UNSPECIFIED').toUpperCase();
    const impacts = (i.impactedDestinations || []).flatMap(d => (d.impacts || []).map(x => ({ context: d.reportingContext || null, country: String(x.regionCode || '').toUpperCase() || null, severity: String(x.severity || severity).toUpperCase() })));
    const countries = [...new Set(impacts.map(x => x.country).filter(Boolean))], criticalCountries = [...new Set(impacts.filter(x => x.severity === 'CRITICAL').map(x => x.country).filter(Boolean))];
    return { title: text(i.title, 200), severity, critical: severity === 'CRITICAL' || impacts.some(x => x.severity === 'CRITICAL'), countries: countries.slice(0, 30), criticalCountries: criticalCountries.slice(0, 30), detail: text(i.detail, 300) || null, documentation: text(i.documentationUri, 300) || null };
  });
  const blocking = issues.filter(i => i.critical), errors = issues.filter(i => !i.critical && i.severity === 'ERROR');
  return { issues, blocking, errors, suggestions: issues.filter(i => !i.critical && i.severity !== 'ERROR'), more: !!(data && data.nextPageToken) };
}
// A blocking issue that names no country stops offers everywhere.
function blocksCountry(issue, country) {
  if (!issue || !issue.critical) return false;
  const where = issue.criticalCountries.length ? issue.criticalCountries : issue.countries;
  return !where.length || where.includes(String(country || '').toUpperCase());
}
const issueWhere = i => i.countries.length ? ' (' + i.countries.slice(0, 6).map(countryName).join(', ') + (i.countries.length > 6 ? ', …' : '') + ')' : '';
// One sentence for the connections check and the health page.
function accountIssueSummary(facts) {
  if (!facts.issues.length) return 'No account issues: Google reports nothing at account level that stops or limits offers.' + (facts.more ? ' More issues exist than were read.' : '');
  const name = list => list.slice(0, 4).map(i => i.title + issueWhere(i)).join('; ') + (list.length > 4 ? '; and ' + (list.length - 4) + ' more' : '');
  return [facts.blocking.length ? plural(facts.blocking.length, 'account issue stops', 'account issues stop') + ' offers serving: ' + name(facts.blocking) + '.' : 'No account issue stops offers serving.',
    facts.errors.length ? plural(facts.errors.length, 'issue', 'issues') + ' may affect offers: ' + name(facts.errors) + '.' : '',
    facts.suggestions.length ? plural(facts.suggestions.length, 'suggestion', 'suggestions') + ' to improve the account.' : '', facts.more ? 'More issues exist than were read.' : ''].filter(Boolean).join(' ');
}

async function accountIssues(request, account) {
  const facts = accountIssueFacts(await request('accounts/v1/accounts/' + account + '/issues', 'GET'));
  const issues = facts.issues.map(i => ({ title: i.title, severity: i.severity, blocksServing: i.critical, impactedCountries: i.countries.slice(0, 12) }));
  return { issues, blocking: facts.blocking.length, errors: facts.errors.length, detail: accountIssueSummary(facts) };
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
//   saveConfig(id) records an id found from the Google Ads Merchant link in
//                Firestore config/googleApiKeys.merchantId, only while that field
//                is empty, so later runs read it instead of rediscovering it
// Every Google step is read-only, and a failing step is noted, not thrown.
// trusted: the id came from a setting or from the Ads account's own Merchant link.
const MERCHANT_ID_HELP = 'Add the numeric Merchant Center account id (digits only, shown top right in merchants.google.com) as the Netlify environment variable GMC_MERCHANT_ID, or as the field merchantId in Firestore config/googleApiKeys.';
const MERCHANT_ID_DOC = 'Firestore config/googleApiKeys.merchantId';
const digits = v => String(v == null ? '' : v).replace(/\D/g, '');

async function resolveMerchantId(deps) {
  const { env, readConfig, adsQuery, listAccounts, saveConfig } = deps || {};
  const notes = [], seen = [];
  const explicit = digits((env || {}).GMC_MERCHANT_ID || (env || {}).MERCHANT_CENTER_ID);
  if (explicit) return { id: explicit, ids: [explicit], source: 'environment GMC_MERCHANT_ID', trusted: true, notes };
  let configEmpty = false;
  if (readConfig) {
    try { const v = digits(await readConfig()); if (v) return { id: v, ids: [v], source: MERCHANT_ID_DOC, trusted: true, notes }; configEmpty = true; }
    catch (e) { notes.push('Firestore config not readable: ' + String((e && e.message) || e)); }
  }
  const unique = list => [...new Set(list.map(digits).filter(Boolean))];
  const steps = [
    adsQuery && { source: 'the Google Ads product_link (Merchant Center link)', link: true, run: async () => unique((await adsQuery("SELECT product_link.merchant_center.merchant_center_id FROM product_link WHERE product_link.type = 'MERCHANT_CENTER'") || [])
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
    if (ids.length === 1 && !chosen) chosen = { id: ids[0], source: step.source, link: !!step.link };
    else if (ids.length === 1 && chosen && ids[0] !== chosen.id) notes.push(step.source + ' names ' + ids[0] + ', not ' + chosen.id + '; using ' + chosen.id + ' from ' + chosen.source);
    else if (ids.length > 1) notes.push(step.source + ' names ' + ids.length + ' different Merchant accounts (' + ids.join(', ') + ')');
  }
  if (chosen) {
    const found = { id: chosen.id, ids: seen, source: chosen.source, trusted: chosen.link, notes, saved: false };
    // Only the Ads account's own Merchant link is remembered, and only into an empty field that was actually read.
    if (chosen.link && configEmpty && typeof saveConfig === 'function') {
      try { found.saved = !!(await saveConfig(chosen.id)); if (found.saved) found.savedTo = MERCHANT_ID_DOC; }
      catch (e) { notes.push('the Merchant account id could not be saved to ' + MERCHANT_ID_DOC + ': ' + String((e && e.message) || e).slice(0, 200)); }
    }
    return found;
  }
  if (seen.length > 1) return { id: null, ids: seen, notes, reason: 'several Merchant accounts are named (' + seen.join(', ') + ') and none was chosen. Set GMC_MERCHANT_ID (or merchantId in Firestore config/googleApiKeys) to the one this store uses.' };
  return { id: null, ids: seen, notes, reason: 'no Merchant account id could be found: it is not in GMC_MERCHANT_ID, Firestore config/googleApiKeys.merchantId, the Google Ads product_link, any shopping campaign, or the Merchant OAuth account list' + (notes.length ? ' (' + notes.join('; ') + ')' : '') + '. ' + MERCHANT_ID_HELP };
}

// ── Per-ad readiness: can this product's offers show where the ad will run? ──
// Performance Max sells a complete ad's product only through its listing group
// filter's offer IDs, so a disapproved, out-of-stock, unshipped or missing offer
// serves nothing and says nothing. Before such a plan is published this answers,
// for the product's offers and the campaign's target countries:
//   · per offer and country: approved, pending or disapproved for Shopping ads
//     (the reporting context Performance Max product listings serve in), its
//     availability, and Google's item-level issues with their severity;
//   · account issues that stop offers serving in those countries;
//   · whether Merchant Center's shipping settings deliver to each country.
// One batched read per plan: one product_view report for every offer of the
// product at once (exact identity, item issues), products.get for each offer it
// finds (per-country status lives only there; at most 12, four at a time, and a
// rate limit stops further reads instead of retrying), account issues and
// shipping settings. Cached five minutes per offer set. Nothing mutates.
// Only an observed, complete answer blocks: Performance Max is refused when no
// offer can show, or even be under review, in any target country. A question
// that could not be answered is a warning, never a refusal.
const SHOPPING_ADS = 'SHOPPING_ADS', MAX_READY_OFFERS = 12, READ_AT_ONCE = 4, READY_TTL = 5 * 60 * 1000, readyCache = new Map();
// Google's country geo target IDs are 2000 + the ISO 3166-1 numeric code (2840 United States, 2124 Canada).
const GEO_ISO = '036AU,040AT,056BE,076BR,124CA,152CL,156CN,158TW,170CO,203CZ,208DK,246FI,250FR,276DE,300GR,344HK,348HU,352IS,356IN,360ID,372IE,376IL,380IT,392JP,410KR,442LU,458MY,484MX,528NL,554NZ,578NO,604PE,608PH,616PL,620PT,642RO,682SA,702SG,704VN,710ZA,724ES,752SE,756CH,764TH,784AE,792TR,826GB,840US'
  .split(',').reduce((m, x) => { m['2' + x.slice(0, 3)] = x.slice(3); return m; }, {});
const geoCountry = id => GEO_ISO[digits(id)] || null;
// known: { geoId: 'CA' } from the account's own country list, for countries outside the table above.
function countryCodesFor(geoIds, known) {
  const codes = [], unknown = [];
  for (const raw of geoIds || []) { const id = digits(raw), c = String((known && known[id]) || geoCountry(id) || '').toUpperCase(); if (/^[A-Z]{2}$/.test(c)) { if (!codes.includes(c)) codes.push(c); } else if (id) unknown.push(id); }
  return { codes: codes.sort(), unknown };
}
let regionNames = null;
function countryName(code) { try { regionNames = regionNames || new Intl.DisplayNames(['en'], { type: 'region' }); return regionNames.of(String(code)) || String(code); } catch (_) { return String(code); } }
const words = (list, joiner) => list.length <= 1 ? (list[0] || '') : list.slice(0, -1).join(', ') + ' ' + (joiner || 'and') + ' ' + list[list.length - 1];
const placeNames = (codes, joiner) => codes.length ? words(codes.map(countryName), joiner) : 'any country';
const availabilityOf = v => String(v == null ? '' : v).trim().toUpperCase().replace(/[\s-]+/g, '_');
const upper = list => [...new Set((Array.isArray(list) ? list : []).map(c => String(c).toUpperCase()).filter(Boolean))];
const plural = (n, one, many) => n + ' ' + (n === 1 ? one : many);
const rateLimited = e => Number(e && e.status) === 429 || /RESOURCE_EXHAUSTED|quota|rate.?limit|too many requests/i.test(String((e && e.message) || e));

// Reads shared by every ad of one Merchant account: account issues and shipping settings.
function sharedReads(request, account) {
  return {
    accountIssues: request('accounts/v1/accounts/' + account + '/issues', 'GET').then(accountIssueFacts, e => ({ error: text((e && e.message) || e, 200) })),
    shipping: request('accounts/v1/accounts/' + account + '/shippingSettings', 'GET').then(data => ({
      countries: upper(((data && data.services) || []).filter(s => s && s.active !== false).flatMap(s => s.deliveryCountries || []))
    }), e => ({ error: text((e && e.message) || e, 200), missing: Number(e && e.status) === 404 }))
  };
}

// One offer as Google holds it. product: products.get (status per country), row: the product_view report row.
function offerFacts(row, product, inFeed) {
  const status = (product && product.productStatus) || {}, dests = Array.isArray(status.destinationStatuses) ? status.destinationStatuses : [];
  const ads = dests.find(d => d && d.reportingContext === SHOPPING_ADS), attrs = (product && (product.productAttributes || product.attributes)) || {};
  const fromProduct = (Array.isArray(status.itemLevelIssues) ? status.itemLevelIssues : []).filter(i => i && i.severity !== 'NOT_IMPACTED' && (!i.reportingContext || i.reportingContext === SHOPPING_ADS))
    .map(i => ({ description: text(i.description || i.code, 160), severity: String(i.severity || 'UNKNOWN'), countries: upper(i.applicableCountries), attribute: i.attribute || null, resolution: text(i.detail, 200) || null }));
  const fromReport = (row.itemIssues || []).map(i => { const t = i.type || {}, sev = i.severity || {}, ctx = (sev.severityPerReportingContext || []).find(s => s.reportingContext === SHOPPING_ADS) || {};
    return { description: text(t.description || t.code, 160) + (t.canonicalAttribute ? ' (' + t.canonicalAttribute + ')' : ''), severity: String(sev.aggregatedSeverity || 'UNKNOWN'), countries: upper([].concat(ctx.disapprovedCountries || [], ctx.demotedCountries || [])), attribute: t.canonicalAttribute || null, resolution: null }; })
    .filter(i => i.severity !== 'NOT_IMPACTED');
  return { offerId: text(row.offerId, 200), feedLabel: row.feedLabel || null, language: row.languageCode || null, inFeed,
    // full: Google's per-country status was read. partial: the product was read but carries no destination status yet.
    read: product ? (dests.length ? 'full' : 'partial') : 'report', reportStatus: row.aggregatedReportingContextStatus || null,
    availability: availabilityOf(attrs.availability) || null, shoppingAds: ads ? true : dests.length ? false : null,
    approved: upper(ads && ads.approvedCountries), pending: upper(ads && ads.pendingCountries), disapproved: upper(ads && ads.disapprovedCountries),
    shipsTo: upper((Array.isArray(attrs.shipping) ? attrs.shipping : []).map(s => s && s.country)), issues: (product ? fromProduct : fromReport).slice(0, 5) };
}
// Where one offer stands in one country, most decisive reason first.
function offerIn(o, country, blocked) {
  if (!o.inFeed) return 'other feed';
  if (o.read !== 'full') return 'unknown';
  if (o.availability === 'OUT_OF_STOCK') return 'out of stock';
  if (blocked(country)) return 'account issue';
  if (o.shoppingAds === false) return 'not in Shopping ads';
  if (o.approved.includes(country)) return 'eligible';
  if (o.pending.includes(country)) return 'pending';
  if (o.disapproved.includes(country)) return 'disapproved';
  return 'not targeted';
}
const WHY_ORDER = ['out of stock', 'account issue', 'not in Shopping ads', 'disapproved', 'not targeted', 'other feed', 'unknown'];
function whyNot(o, states, ctx) {
  const why = WHY_ORDER.find(s => states.includes(s)) || 'unknown', issue = o.issues.find(i => i.severity === 'DISAPPROVED') || o.issues[0];
  if (why === 'out of stock') return 'out of stock';
  if (why === 'account issue') return 'stopped by the account issue "' + ((ctx.facts && ctx.facts.blocking[0]) || {}).title + '"';
  if (why === 'not in Shopping ads') return 'not set up for Shopping ads, the listings Performance Max serves';
  if (why === 'disapproved') return 'disapproved' + (issue ? ' (' + issue.description + ')' : '');
  if (why === 'not targeted') return 'not set up to show in ' + placeNames(ctx.countries, 'or') + (o.approved.length ? ' (approved for ' + placeNames(o.approved.slice(0, 4)) + ' only)' : '');
  if (why === 'other feed') return 'in the ' + o.feedLabel + ' feed, not the campaign\'s ' + ctx.feedLabel + ' feed';
  return 'its status could not be read';
}

function readinessUnavailable({ offerIds, countryCodes, merchantId, reason, now }) {
  const message = 'Merchant Center: the status of this product\'s offers could not be read (' + text(reason, 220) + '). This does not block Performance Max; Google Ads\' own product status is checked again when you approve.';
  return { status: 'unavailable', block: false, reason: null, detail: text(reason, 300), checkedAt: (now || Date.now)(), merchantId: merchantId || null, offerIds: (offerIds || []).slice(0, 40), countryCodes: countryCodes || [], offers: [], countries: [], lines: [message], message };
}

async function readinessFresh({ request, account, ids, wanted, unmapped, skipped, feedLabel, shared, clock }) {
  const base = { offerIds: ids, countryCodes: wanted, merchantId: account, now: clock };
  const pattern = '(?i)^(' + ids.map(x => x.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')).join('|') + ')$';
  const query = "SELECT id, offer_id, feed_label, language_code, channel, aggregated_reporting_context_status, item_issues FROM product_view WHERE offer_id REGEXP_MATCH '" + pattern.replace(/\\/g, '\\\\').replace(/'/g, "\\'") + "'";
  const reads = shared || sharedReads(request, account);
  let rows = [], pageToken = null, complete = false;
  try {
    for (let page = 0; page < 3; page++) {
      const data = await request('reports/v1/accounts/' + account + '/reports:search', 'POST', { query, pageSize: 100, ...(pageToken ? { pageToken } : {}) });
      for (const r of (data && data.results) || []) rows.push(r.productView || {});
      pageToken = data && data.nextPageToken; if (!pageToken) { complete = true; break; }
    }
  } catch (e) { return readinessUnavailable({ ...base, reason: 'Merchant Center did not list them: ' + text((e && e.message) || e, 180) }); }
  const wantedIds = new Set(ids.map(x => x.toLowerCase())), seen = new Map();
  for (const r of rows) {
    const id = String(r.offerId || '');
    if (!wantedIds.has(id.toLowerCase()) || (r.channel && r.channel !== 'ONLINE') || !/^[a-z]{2,3}(-[A-Za-z]+)?$/.test(r.languageCode || '') || !r.feedLabel) continue;
    seen.set([r.languageCode, r.feedLabel, id].join('~'), r);
  }
  const identities = [...seen.values()], found = new Set(identities.map(r => String(r.offerId).toLowerCase()));
  const missing = complete ? ids.filter(x => !found.has(x.toLowerCase())) : [];
  const inFeed = r => !feedLabel || String(r.feedLabel).toUpperCase() === String(feedLabel).toUpperCase();
  // Only offers this campaign can serve are read; a rate limit ends the reads rather than retrying them.
  const toRead = identities.filter(inFeed), products = new Map(), notes = [];
  let stopped = null;
  for (let i = 0; i < Math.min(toRead.length, MAX_READY_OFFERS) && !stopped; i += READ_AT_ONCE) {
    await Promise.all(toRead.slice(i, Math.min(i + READ_AT_ONCE, MAX_READY_OFFERS)).map(async r => {
      const name = 'accounts/' + account + '/products/' + Buffer.from([r.languageCode, r.feedLabel, r.offerId].join('~')).toString('base64url');
      try { products.set(r, await request('products/v1/' + name, 'GET')); }
      catch (e) { if (rateLimited(e)) stopped = 'Merchant Center asked to slow down'; else notes.push(r.offerId + ': ' + text((e && e.message) || e, 120)); }
    }));
  }
  if (toRead.length > MAX_READY_OFFERS) notes.push('only the first ' + MAX_READY_OFFERS + ' of ' + toRead.length + ' offers were read');
  if (stopped) notes.push(stopped + '; the remaining offers were not read');
  const offers = identities.map(r => offerFacts(r, products.get(r) || null, inFeed(r)));
  const [facts, shipping] = await Promise.all([reads.accountIssues, reads.shipping]);
  const accountFacts = facts && !facts.error ? facts : null, blocked = c => !!accountFacts && accountFacts.blocking.some(i => blocksCountry(i, c));
  // No target countries means every country: judge the countries Google reports for these offers.
  const countries = wanted.length ? wanted : upper(offers.flatMap(o => [].concat(o.approved, o.pending, o.disapproved))).sort();
  const byCountry = countries.map(c => { const states = offers.map(o => offerIn(o, c, blocked));
    return { country: c, name: countryName(c), eligible: states.filter(s => s === 'eligible').length, pending: states.filter(s => s === 'pending').length, states }; });
  const stateOf = (o, c) => c.states[offers.indexOf(o)];
  const showing = byCountry.filter(c => c.eligible), reviewing = byCountry.filter(c => !c.eligible && c.pending), dark = byCountry.filter(c => !c.eligible && !c.pending);
  const eligibleOffers = offers.filter(o => byCountry.some(c => stateOf(o, c) === 'eligible')), idle = offers.filter(o => !eligibleOffers.includes(o));
  const conclusive = complete && !unmapped && !skipped && offers.every(o => !o.inFeed || o.read === 'full') && toRead.length <= MAX_READY_OFFERS && !stopped;
  const block = conclusive && !showing.length && !reviewing.length, total = offers.length + missing.length;
  // Why the given offers cannot show in the given countries, grouped: "2 are disapproved: … (ids)".
  const why = (list, where) => { const groups = new Map(), here = byCountry.filter(c => where.includes(c.country));
    list.forEach(o => { const w = whyNot(o, here.map(c => stateOf(o, c)), { countries: where, feedLabel, facts: accountFacts }); groups.set(w, (groups.get(w) || []).concat(o.offerId)); });
    if (missing.length) groups.set('not in Merchant Center account ' + account, missing);
    return [...groups.entries()].map(([w, ids]) => ids.length === 1 ? ids[0] + ' is ' + w : ids.length + ' are ' + w + ': ' + ids.slice(0, 3).join(', ') + (ids.length > 3 ? ', …' : '')).join('; '); };
  const codes = list => list.map(c => c.country), lines = [];
  const noneText = kind => 'none of this product\'s ' + plural(total, kind, kind + 's') + ' can show in ' + placeNames(countries, 'or') + ', so Performance Max would show nothing. ' +
    (total ? 'Of its offers: ' + why(idle, countries) + '.' : 'Merchant Center has no offer for this product.') + ' Fix this in Merchant Center or in the store feed that supplies the product';
  if (block) lines.push('Merchant Center: ' + noneText('offer') + '.');
  else {
    if (showing.length) lines.push('Merchant Center: ' + (eligibleOffers.length === total ? (total === 1 ? 'the product\'s offer' : 'all ' + total + ' of the product\'s offers') : eligibleOffers.length + ' of ' + total + ' offers') + ' can show in ' + placeNames(codes(showing)) + '.');
    if (dark.length) lines.push((showing.length ? 'None' : 'Merchant Center: no offer') + ' can show in ' + placeNames(codes(dark)) + (showing.length ? '' : ' yet') + ': ' + why(offers, codes(dark)) + '.' + (showing.length ? ' Performance Max shows this product only in ' + placeNames(codes(showing)) + '.' : ''));
    if (reviewing.length) lines.push((showing.length || dark.length ? '' : 'Merchant Center: ') + 'Google is still reviewing this product\'s offers for ' + placeNames(codes(reviewing)) + '; they can show there once approved.');
    if (showing.length && (idle.length || missing.length) && !dark.length) lines.push(plural(idle.length + missing.length, 'other offer', 'other offers') + ' will not show: ' + why(idle, countries) + '.');
    if (!lines.length) lines.push('Merchant Center: no offer of this product could be confirmed to show yet.');
  }
  const accountLines = [];
  if (accountFacts) {
    const hit = accountFacts.blocking.filter(i => !countries.length || countries.some(c => blocksCountry(i, c)));
    if (hit.length && !block) accountLines.push('Merchant Center account issue that stops offers serving: ' + hit.slice(0, 2).map(i => i.title + issueWhere(i)).join('; ') + '.');
    if (accountFacts.errors.length) accountLines.push('Merchant Center account issue that may affect offers: ' + accountFacts.errors.slice(0, 2).map(i => i.title + issueWhere(i)).join('; ') + (accountFacts.errors.length > 2 ? '; and ' + (accountFacts.errors.length - 2) + ' more' : '') + '.');
  } else notes.push('account issues could not be read' + (facts && facts.error ? ' (' + facts.error + ')' : ''));
  // A country with no shipping service explains why its offers cannot show; where an offer shows, shipping evidently works.
  const unshipped = shipping && !shipping.error ? codes(dark).filter(c => !shipping.countries.includes(c) && !offers.some(o => o.shipsTo.includes(c))) : [];
  if (unshipped.length) accountLines.push('Merchant Center shipping settings have no active service to ' + placeNames(unshipped) + '; an offer needs shipping to a country to show there.');
  if (shipping && shipping.error && !shipping.missing) notes.push('shipping settings could not be read');
  if (unmapped) notes.push(plural(unmapped, 'target country', 'target countries') + ' could not be named, so ' + (unmapped === 1 ? 'it was' : 'they were') + ' not checked');
  if (skipped) notes.push(plural(skipped, 'offer ID is', 'offer IDs are') + ' not in a form Merchant Center can be asked about');
  if (!complete) notes.push('Merchant Center listed more matching offers than were read');
  const all = lines.concat(accountLines, notes.length ? ['Not fully checked: ' + notes.join('; ') + '.'] : []);
  return { status: block ? 'blocked' : (dark.length || reviewing.length || idle.length || missing.length || accountLines.length || notes.length ? 'warning' : 'ready'), block, conclusive,
    reason: block ? 'Performance Max was not prepared: ' + noneText('Merchant Center offer') + ', then prepare the plan again. Display campaigns do not use Merchant Center offers and can still be chosen.' : null,
    checkedAt: clock(), merchantId: account, offerIds: ids, countryCodes: wanted, feedLabel: feedLabel || null, eligibleCountries: codes(showing), reviewCountries: codes(reviewing), darkCountries: codes(dark),
    offers: offers.slice(0, MAX_READY_OFFERS).map(o => ({ offerId: o.offerId, feedLabel: o.feedLabel, language: o.language, availability: o.availability, approved: o.approved.slice(0, 20), pending: o.pending.slice(0, 20), disapproved: o.disapproved.slice(0, 20), issues: o.issues.slice(0, 3), read: o.read, inFeed: o.inFeed, eligible: eligibleOffers.includes(o) })),
    missing, countries: byCountry.map(c => ({ country: c.country, name: c.name, eligible: c.eligible, pending: c.pending })),
    accountIssues: accountFacts ? { blocking: accountFacts.blocking.length, errors: accountFacts.errors.length } : null, shippingUncovered: unshipped,
    lines: all, message: all.join(' ') };
}

// input: request(path, method, body), merchantId, offerIds (the product's exact offer IDs), countries (ISO codes;
// empty = every country), unmapped (target countries that could not be named), feedLabel (the campaign's feed
// label, if it has one), shared (sharedReads() reused across ads), force (skip the five-minute cache).
async function offerReadiness(input) {
  const { request, merchantId, offerIds, countries, unmapped = 0, feedLabel = null, shared = null, force = false, now } = input || {};
  const clock = typeof now === 'function' ? now : Date.now;
  if (typeof request !== 'function') throw new Error('A Merchant API request function is required.');
  const account = digits(merchantId), ids = [...new Map((offerIds || []).map(x => String(x || '').trim()).filter(Boolean).map(x => [x.toLowerCase(), x])).values()];
  const wanted = upper(countries).filter(c => /^[A-Z]{2}$/.test(c)).sort(), base = { offerIds: ids, countryCodes: wanted, merchantId: account || null, now: clock };
  if (!account) return readinessUnavailable({ ...base, reason: 'the Merchant Center account could not be resolved' });
  const safe = ids.filter(x => /^[\w.:-]{1,150}$/.test(x));
  if (!safe.length) return readinessUnavailable({ ...base, reason: ids.length ? 'the offer IDs are not in a form Merchant Center can be asked about' : 'this product has no exact Merchant offer ID' });
  const key = JSON.stringify([account, safe.map(x => x.toLowerCase()).sort(), wanted, unmapped, String(feedLabel || '').toUpperCase()]), hit = readyCache.get(key);
  if (!force && hit && clock() - hit.at < READY_TTL) return hit.value;
  const value = readinessFresh({ request, account, ids: safe, wanted, unmapped, skipped: ids.length - safe.length, feedLabel, shared, clock });
  readyCache.set(key, { at: clock(), value });
  if (readyCache.size > 50) readyCache.delete(readyCache.keys().next().value);
  const result = await value;
  // A question that could not be answered is asked again next time, not remembered.
  if (result.status === 'unavailable' && readyCache.get(key) && readyCache.get(key).value === value) readyCache.delete(key);
  return result;
}

function resetReadinessCache() { readyCache.clear(); }

// Publication's last look, from Google Ads' own product status (shopping_product rows as merchantProducts returns
// them). Refuses only when no offer of the product can serve in any target country: returns that reason in plain
// words, or null when at least one offer can serve. isEligible is the engine's own eligibility rule.
function noServableOffer(rows, itemIds, isEligible, countryCodes) {
  const ids = (itemIds || []).map(String).filter(Boolean), wanted = upper(countryCodes);
  if (!ids.length) return null;
  const rowsOf = id => (rows || []).filter(p => p && String(p.itemId).toLowerCase() === id.toLowerCase());
  const servesThere = p => !wanted.length || !upper(p.targetCountries).length || upper(p.targetCountries).some(c => wanted.includes(c));
  if (ids.some(id => rowsOf(id).some(p => isEligible(p) && servesThere(p)))) return null;
  const why = id => { const found = rowsOf(id), p = found.find(isEligible) || found[0]; if (!p) return id + ' is not in Google Ads\' product list from Merchant Center';
    const av = String(p.availability || '').toUpperCase();
    if (isEligible(p)) return id + ' only targets ' + placeNames(upper(p.targetCountries).slice(0, 4));
    if (av && av !== 'IN_STOCK') return id + ' is ' + (av === 'OUT_OF_STOCK' ? 'out of stock' : av.toLowerCase().replace(/_/g, ' '));
    return id + ' is not eligible' + ((p.issues || [])[0] ? ' (' + text(p.issues[0], 120) + ')' : ''); };
  return 'Performance Max was not published: none of this product\'s ' + plural(ids.length, 'Merchant Center offer', 'Merchant Center offers') + ' can serve' + (wanted.length ? ' in ' + placeNames(wanted, 'or') : '') + ' right now, so the campaign would show nothing (' +
    ids.slice(0, 4).map(why).join('; ') + (ids.length > 4 ? '; and ' + (ids.length - 4) + ' more' : '') + '). Fix the offers in Merchant Center, then approve again. Nothing was created.';
}

// The complete ads waiting in Approvals, as the readiness check needs them: the product's exact offer IDs
// (shopify_<market>_<product>_<variant> in the ad's saved context), the target countries (the prepared plan's,
// else the plan choice, else the saved context, else the account defaults) and the feed label.
function pendingAdTargets(approvals, defaultCountries) {
  return (approvals || []).filter(a => a && a.type === 'adDesignSubmission' && a.status === 'PENDING' && !a.deletedAt && !a.archivedAt).map(a => {
    const r = a.designReview || {}, ctx = r.context || {}, prefs = a.submissionPreferences || {}, product = (String(r.productId || '').match(/(\d+)$/) || [])[1] || '';
    const planned = (((a.pipelinePlan || {}).summary || {}).merchant || {}).countryCodes;
    const geo = [prefs.countries, ctx.countries, defaultCountries].find(list => Array.isArray(list) && list.length) || [];
    return { id: a.id || null, title: text(r.productTitle || String(a.summary || '').replace(/^Complete ad · /, ''), 160), productId: product,
      offerIds: [...new Set((ctx.itemIds || []).map(String).filter(id => { const m = id.match(/^shopify_[^_]+_(\d+)_(\d+)$/i); return m && m[1] === product; }))],
      countryCodes: Array.isArray(planned) && planned.length ? upper(planned) : null, geoIds: geo.map(String), feedLabel: ctx.feedLabel || null, styles: Array.isArray(prefs.styles) ? prefs.styles : null };
  });
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
  if (out.accountIssues.status === 'available' && out.accountIssues.blocking) blocking.push(out.accountIssues.blocking + ' account issue(s) that stop offers serving');
  if (out.productIssues.status === 'available' && out.productIssues.disapproved) blocking.push(out.productIssues.disapproved + ' offer(s) that cannot serve');
  if (out.conversionSources.status === 'available' && !out.conversionSources.active) blocking.push('no active conversion source');
  if (out.adsLink.status === 'available' && out.adsLink.googleAdsLinked === false) blocking.push('no Google Ads relationship is recorded');
  // Serving but visibly damaged: named beside the blockers, never counted as one.
  const attention = out.titleIssues.status === 'available' && out.titleIssues.affected ? [out.titleIssues.affected + ' offer(s) with a garbled title'] : [];
  if (out.accountIssues.status === 'available' && out.accountIssues.errors) attention.push(out.accountIssues.errors + ' account issue(s) that may affect offers');

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

module.exports = { merchantHealth, resolveMerchantId, MERCHANT_ID_HELP, MERCHANT_ID_DOC, accountIssues, accountIssueFacts, accountIssueSummary, blocksCountry,
  offerReadiness, readinessUnavailable, resetReadinessCache, sharedReads, noServableOffer, pendingAdTargets, geoCountry, countryCodesFor, countryName, productIssues, titleIssues, titleProblem, garbledTitlesNotice, TITLE_FIX, adsLink, conversionSources, servingSettings, dataSources, promotions, autofeed };
