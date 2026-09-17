// Merchant Center health and YouTube video state. Both answer questions Google
// Ads cannot: an offer that cannot serve, and a video Ads reports as attached
// that YouTube has rejected or has not finished processing.
const assert = require('assert/strict'), path = require('path');
const FN = path.resolve(__dirname, '../../netlify/functions');
const { merchantHealth } = require(path.join(FN, '_merchantHealth.js'));
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
    if (/\/issues$/.test(p)) return { accountIssues: [{ title: 'Missing return policy', severity: 'ERROR', impactedDestinations: [{ impacts: [{ regionCode: 'CA' }] }] }] };
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
  check(health.sections.accountIssues.blocking === 1, 'a blocking account issue is separated from advisory ones');
  check(health.sections.adsLink.googleAdsLinked === false && health.sections.adsLink.shopifyLinked === true,
    'each provider relationship is judged on its own; a Shopify link is not a Google Ads link');
  check(health.sections.conversionSources.active === 0 && /not reaching Merchant Center/.test(health.sections.conversionSources.detail), 'a missing conversion source is stated plainly');
  check(health.sections.dataSources.sources[0].primary === true, 'feed ownership is reported so a managed source is not overwritten');

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
})().catch(e => { console.error(e); process.exitCode = 1; });
