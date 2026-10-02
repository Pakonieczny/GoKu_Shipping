// Merchant Center health and YouTube video state. Both answer questions Google
// Ads cannot: an offer that cannot serve, and a video Ads reports as attached
// that YouTube has rejected or has not finished processing.
const assert = require('assert/strict'), path = require('path');
const FN = path.resolve(__dirname, '../../netlify/functions');
const { merchantHealth, resolveMerchantId } = require(path.join(FN, '_merchantHealth.js'));
const keys = require(path.join(FN, '_googleApiKeys.js'));
const { videoStatus, seconds, problemsFor } = require(path.join(FN, '_youtubeVideos.js'));
let passed = 0;
const check = (cond, name) => { assert.ok(cond, name); passed++; console.log('PASS', name); };

// A Merchant account with one disapproved offer, no conversion source, and a
// relationship that does not name the advertising account.
function merchantStub(overrides = {}) {
  const seen = [];
  const request = async (p, method, body) => {
    seen.push(p);
    if (overrides[p] instanceof Error) throw overrides[p];
    if (p in overrides) return overrides[p];
    if (/\/issues$/.test(p)) return { accountIssues: [
      { title: 'Missing shipping settings', severity: 'CRITICAL', impactedDestinations: [{ reportingContext: 'SHOPPING_ADS', impacts: [{ regionCode: 'CA', severity: 'CRITICAL' }] }] },
      { title: 'Missing return policy', severity: 'ERROR', impactedDestinations: [{ impacts: [{ regionCode: 'CA' }] }] }] };
    if (/reports:search$/.test(p)) return { results: [
      { productView: { offerId: 'duck-1', title: 'Duck necklace', aggregatedReportingContextStatus: 'ELIGIBLE', itemIssues: [] } },
      { productView: { offerId: 'duck-2', title: 'Duck charm', aggregatedReportingContextStatus: 'NOT_ELIGIBLE_OR_DISAPPROVED', itemIssues: [{ type: { code: 'image_link_broken', description: 'Image cannot be fetched' }, severity: { aggregatedSeverity: 'DISAPPROVED' } }] } }
    ] };
    if (/\/relationships$/.test(p)) return { accountRelationships: [{ provider: 'providers/116881232', providerDisplayName: 'Shopify' }] };
    if (/conversionSources/.test(p)) return { conversionSources: [] };
    if (/shippingSettings$/.test(p)) return { services: [{ serviceName: 'Standard' }] };
    if (/onlineReturnPolicies/.test(p)) return { onlineReturnPolicies: [] };
    if (/dataSources/.test(p)) return { dataSources: [{ name: 'a', displayName: 'Shopify', input: 'API', primaryProductDataSource: { feedLabel: 'CA', contentLanguage: 'en' } }] };
    if (/promotions/.test(p)) return { promotions: [] };
    if (/autofeedSettings$/.test(p)) return { enableProducts: false };
    return {};
  };
  return { request, seen };
}

(async () => {
  const stub = merchantStub();
  const health = await merchantHealth({ request: stub.request, merchantId: '555', adsCustomerId: '1234567890' });

  check(health.sections.productIssues.disapproved === 1, 'an offer that cannot serve is counted');
  check(health.sections.productIssues.offers[0].offerId === 'duck-2', 'the exact offer ID is named, not just a count');
  check(/Image cannot be fetched/.test(JSON.stringify(health.sections.productIssues.offers[0].issues)), 'the reason the offer cannot serve is carried through');
  check(health.sections.productIssues.scanned === 2 && !health.sections.productIssues.offers.some(o => o.offerId === 'duck-1'), 'an eligible offer is not reported as a problem');
  check(health.sections.accountIssues.blocking === 1 && health.sections.accountIssues.errors === 1, 'a blocking account issue is separated from advisory ones');
  check(health.sections.accountIssues.issues.find(i => i.title === 'Missing shipping settings').blocksServing === true && health.sections.accountIssues.issues.find(i => i.title === 'Missing return policy').blocksServing === false,
    'only Google\'s CRITICAL severity ("causes offers to not serve") counts as stopping offers; ERROR "might affect" them');
  check(health.summary.attention.some(a => /may affect offers/.test(a)), 'an issue that may affect offers is named beside the blockers, not counted as one');
  check(health.sections.adsLink.googleAdsLinked === false && health.sections.adsLink.shopifyLinked === true,
    'each provider relationship is judged on its own; a Shopify link is not a Google Ads link');
  check(health.sections.conversionSources.active === 0 && /not reaching Merchant Center/.test(health.sections.conversionSources.detail), 'a missing conversion source is stated plainly');
  check(health.sections.dataSources.sources[0].primary === true, 'feed ownership is reported so a managed source is not overwritten');

  // A disapproval beyond the first page is still found: no LIMIT, only non-eligible offers, every page read.
  const bodies = [];
  const paged = await merchantHealth({ merchantId: '555', adsCustomerId: '1', request: async (p, method, body) => {
    if (!/reports:search$/.test(p) || /REGEXP_MATCH/.test(body.query)) return stub.request(p, method, body);
    bodies.push(body);
    return body.pageToken ? { results: [{ productView: { offerId: 'late-1', aggregatedReportingContextStatus: 'NOT_ELIGIBLE_OR_DISAPPROVED', itemIssues: [] } }] }
      : { results: [{ productView: { offerId: 'early-1', aggregatedReportingContextStatus: 'PENDING', itemIssues: [] } }], nextPageToken: 'next' };
  } });
  check(bodies.length === 2 && bodies.every(b => !/\bLIMIT\b/.test(b.query) && /aggregated_reporting_context_status IN/.test(b.query)), 'the offer scan pages through the catalogue without a LIMIT');
  check(paged.sections.productIssues.disapproved === 1 && paged.sections.productIssues.complete === true && /whole catalogue/.test(paged.sections.productIssues.detail), 'a disapproval after the first page still blocks health');

  const blockers = health.summary.blocking.join(' | ');
  check(/account issue/.test(blockers) && /cannot serve/.test(blockers) && /conversion source/.test(blockers) && /no Google Ads relationship/.test(blockers),
    'every blocker reaches the summary');
  check(health.summary.healthy === false, 'an account with blockers is not reported healthy');

  // A healthy account, and a matching advertising link.
  const good = await merchantHealth({
    request: merchantStub({
      'accounts/v1/accounts/555/issues': { accountIssues: [] },
      'reports/v1/accounts/555/reports:search': { results: [{ productView: { offerId: 'duck-1', aggregatedReportingContextStatus: 'ELIGIBLE', itemIssues: [] } }] },
      'accounts/v1/accounts/555/relationships': { accountRelationships: [{ provider: 'providers/GOOGLE_ADS', providerDisplayName: 'Google Ads' }] },
      'conversions/v1/accounts/555/conversionSources?pageSize=50': { conversionSources: [{ name: 's', state: 'ACTIVE', merchantCenterDestination: {} }] }
    }).request, merchantId: '555', adsCustomerId: '123-456-7890'
  });
  check(good.summary.healthy === true, 'an account with no blockers and every section read is reported healthy');
  check(good.sections.adsLink.googleAdsLinked === true, 'the providers/GOOGLE_ADS relationship is recognised as the Google Ads link');

  // A section that could not be read leaves its question open rather than
  // implying nothing is wrong.
  const partial = await merchantHealth({
    request: merchantStub({ 'accounts/v1/accounts/555/issues': new Error('403 permission denied') }).request,
    merchantId: '555', adsCustomerId: '1234567890'
  });
  check(partial.sections.accountIssues.status === 'unavailable' && /403/.test(partial.sections.accountIssues.detail), 'an unreadable section records its reason');
  check(partial.summary.unavailable === 1 && partial.summary.healthy === false, 'an unread section is never reported as healthy');

  await assert.rejects(() => merchantHealth({ request: stub.request, merchantId: 'abc' }), /numeric Merchant Center account ID/); passed++;
  console.log('PASS a non-numeric Merchant account is refused');

  // ── Merchant account id: discovered from the Ads Merchant link, then recorded once in Firestore ──────────
  const link = async q => /FROM product_link/.test(q) ? [{ productLink: { merchantCenter: { merchantCenterId: '5550001' } } }] : [];
  const saves = [];
  const discovered = await resolveMerchantId({ env: {}, readConfig: async () => null, adsQuery: link, saveConfig: async id => { saves.push(id); return true; } });
  check(discovered.id === '5550001' && discovered.trusted === true && /product_link/.test(discovered.source), 'the Merchant id comes from the Google Ads product_link and is trusted');
  check(discovered.saved === true && saves.length === 1 && saves[0] === '5550001' && /config\/googleApiKeys\.merchantId/.test(discovered.savedTo), 'an id found from the Merchant link is saved to Firestore config/googleApiKeys.merchantId');
  saves.length = 0;
  const stored = await resolveMerchantId({ env: {}, readConfig: async () => 5550001, adsQuery: async () => { throw Error('discovery must not run'); }, saveConfig: async id => { saves.push(id); return true; } });
  check(stored.id === '5550001' && stored.trusted && /Firestore/.test(stored.source) && !saves.length, 'a saved id is read straight from Firestore: no discovery, no second save');
  const unreadable = await resolveMerchantId({ env: {}, readConfig: async () => { throw Error('offline'); }, adsQuery: link, saveConfig: async id => { saves.push(id); return true; } });
  check(unreadable.id === '5550001' && unreadable.saved === false && !saves.length, 'nothing is saved when the Firestore field could not be read (it may not be empty)');
  const fromEnv = await resolveMerchantId({ env: { GMC_MERCHANT_ID: '777' }, readConfig: async () => null, adsQuery: link, saveConfig: async id => { saves.push(id); return true; } });
  check(fromEnv.id === '777' && fromEnv.trusted && !saves.length, 'an environment value wins and is never copied');
  const campaignOnly = await resolveMerchantId({ env: {}, readConfig: async () => null, adsQuery: async q => /FROM campaign/.test(q) ? [{ campaign: { shoppingSetting: { merchantId: '888' } } }] : [], saveConfig: async id => { saves.push(id); return true; } });
  check(campaignOnly.id === '888' && campaignOnly.trusted === false && !saves.length, 'an id only inferred from a shopping campaign is used but not trusted or saved');
  const failedSave = await resolveMerchantId({ env: {}, readConfig: async () => null, adsQuery: link, saveConfig: async () => { throw Error('permission denied'); } });
  check(failedSave.id === '5550001' && failedSave.saved === false && /could not be saved.*permission denied/.test(failedSave.notes.join(' ')), 'a failed save is noted and never stops the check');

  // The key store writes only the numeric merchantId, only while it is empty, merged into the document.
  keys.resetCache();
  const fakeDb = initial => { const doc = { exists: initial != null, value: initial || {} }, writes = [];
    return { doc, writes, db: { doc: () => ({ get: async () => ({ exists: doc.exists, data: () => doc.value }) }),
      runTransaction: async fn => fn({ get: async () => ({ exists: doc.exists, data: () => doc.value }), set: (ref, data, opts) => { writes.push({ data, opts }); doc.exists = true; doc.value = { ...doc.value, ...data }; } }) } }; };
  const blankDoc = fakeDb({ youtubeApiKey: 'AIza-kept' });
  check(await keys.saveStoredValueIfEmpty('merchantId', '5550001', blankDoc) === true && blankDoc.writes.length === 1 && JSON.stringify(blankDoc.writes[0].data) === '{"merchantId":"5550001"}' && blankDoc.writes[0].opts.merge === true && blankDoc.doc.value.youtubeApiKey === 'AIza-kept',
    'an empty merchantId is written as that one numeric field, merged, leaving the other keys untouched');
  const taken = fakeDb({ merchantId: '999' });
  check(await keys.saveStoredValueIfEmpty('merchantId', '5550001', taken) === false && !taken.writes.length && taken.doc.value.merchantId === '999', 'an operator\'s own merchantId is never replaced');
  await assert.rejects(() => keys.saveStoredValueIfEmpty('youtubeApiKey', 'AIza-x', fakeDb({})), /not a value this store records/); passed++;
  await assert.rejects(() => keys.saveStoredValueIfEmpty('merchantId', 'abc', fakeDb({})), /numeric merchantId/); passed++;
  console.log('PASS the key store refuses to write secrets or a non-numeric account id');

  // ── YouTube ───────────────────────────────────────────────────────────────
  check(seconds('PT10S') === 10 && seconds('PT1M30S') === 90 && seconds('rubbish') === null, 'ISO-8601 durations are read, and nonsense is not guessed at');
  check(problemsFor({ uploadStatus: 'processed', privacyStatus: 'unlisted', embeddable: true, seconds: 10 }, 10).length === 0, 'a healthy unlisted ad film reports no problem');
  check(/below the 10s/.test(problemsFor({ uploadStatus: 'processed', privacyStatus: 'unlisted', embeddable: true, seconds: 8 }, 10).join(' ')), 'a film under Google\'s minimum length is flagged');

  const items = {
    dQw4w9WgXcQ: { id: 'dQw4w9WgXcQ', snippet: { title: 'Duck film' }, status: { uploadStatus: 'processed', privacyStatus: 'unlisted', embeddable: true }, contentDetails: { duration: 'PT10S' } },
    kJQP7kiw5Fk: { id: 'kJQP7kiw5Fk', snippet: { title: 'Rejected' }, status: { uploadStatus: 'rejected', rejectionReason: 'copyright', privacyStatus: 'unlisted', embeddable: true }, contentDetails: { duration: 'PT10S' } }
  };
  const ytFetch = async url => ({ ok: true, status: 200, json: async () => ({ items: [...new URL(url).searchParams.get('id').split(',')].filter(id => items[id]).map(id => items[id]) }) });

  const state = await videoStatus({ fetch: ytFetch, apiKey: 'k', videoIds: ['dQw4w9WgXcQ', 'kJQP7kiw5Fk', '9bZkp7q19f0'] });
  check(state.videos.find(v => v.id === 'dQw4w9WgXcQ').serviceable === true, 'a processed unlisted film is serviceable');
  check(state.videos.find(v => v.id === 'kJQP7kiw5Fk').serviceable === false && /rejected/i.test(state.videos.find(v => v.id === 'kJQP7kiw5Fk').problems.join(' ')), 'a rejected film is reported unserviceable with its reason');
  check(state.missing.includes('9bZkp7q19f0'), 'a video Google Ads knows about that YouTube will not return is itself reported');
  check(state.unserviceable === 1 && /cannot serve/.test(state.detail), 'the summary counts films that cannot serve');

  await assert.rejects(() => videoStatus({ fetch: ytFetch, videoIds: ['dQw4w9WgXcQ'] }), /YOUTUBE_API_KEY/); passed++;
  console.log('PASS reading YouTube state without a credential is refused, not guessed');
  const empty = await videoStatus({ fetch: ytFetch, apiKey: 'k', videoIds: ['!!bad id!!'] });
  check(empty.videos.length === 0 && empty.requested === 0, 'a malformed video ID is never sent to YouTube');

  console.log(passed + ' Merchant health and YouTube video-state checks passed.');
  require('./suite-guard.cjs').done();
})().catch(e => { console.error(e); process.exitCode = 1; });
