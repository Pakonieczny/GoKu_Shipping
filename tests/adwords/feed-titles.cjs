// A feed title Google received garbled (UTF-8 read as Latin-1 or Windows-1252)
// shows "â" in live ads. Nothing in this app writes feed titles, so the console
// names the offer and the fix at the title's source, and never copies the damage
// into ad text it builds. Offline: stubbed Merchant, Google Ads and AI calls only.
'use strict';
const assert = require('node:assert/strict'), fs = require('node:fs'), path = require('node:path'), vm = require('node:vm');
const FN = path.resolve(__dirname, '../../netlify/functions'), enginePath = path.join(FN, 'googleAdsAutopilot.js');
const M = require(path.join(FN, '_merchantHealth.js'));
let checks = 0;
const check = (cond, name) => { assert.ok(cond, name); checks++; console.log('PASS', name); };

// 1. Detection and repair.
const utf8 = 'Dainty Monogram Necklace – Name Jewelry — Mother’s Day “Gift” • Café';
const cp1252 = new TextDecoder('windows-1252').decode(Buffer.from(utf8, 'utf8')), latin1 = Buffer.from(utf8, 'utf8').toString('latin1');
// The live shape: Latin-1 turned each en dash into "â" plus two control characters, which Google dropped.
const live = 'Dainty Monogram Necklace â Monogram Necklace â Custom Block Monogram Initials Necklace â Name Jewelry';
const liveFixed = 'Dainty Monogram Necklace – Monogram Necklace – Custom Block Monogram Initials Necklace – Name Jewelry';
check(M.titleProblem(cp1252).suggested === utf8 && M.titleProblem(cp1252).exact === true, 'a Windows-1252 misreading is repaired exactly');
check(M.titleProblem(latin1).suggested === utf8 && M.titleProblem(latin1).exact === true, 'a Latin-1 misreading that kept its control characters is repaired exactly');
check(M.titleProblem(live).suggested === liveFixed && M.titleProblem(live).exact === false, 'the live title, whose control characters were dropped, is flagged with a best-effort dash');
check(M.titleProblem('Motherâs Day Necklace').suggested === 'Mother’s Day Necklace', 'a contraction keeps its apostrophe');
check(M.titleProblem('Broken � title').suggested === 'Broken title' && M.titleProblem('﻿Heart charm').suggested === 'Heart charm', 'replacement characters and byte-order marks are flagged and dropped');
for (const clean of [utf8, 'Pâte de verre pendant, château charm, pâté', 'São Paulo necklace, Ñandú charm', 'Crème brûlée charm · 14k Gold Filled / 18" / None', 'CAFÉ” charm', 'Noël’s café® charm', '', null])
  check(M.titleProblem(clean) === null, JSON.stringify(clean) + ' is correct text and is not flagged');

// 2. Merchant health scans the whole catalogue for them, separately from eligibility.
function merchant(titlePages, failTitles) {
  const titleBodies = [];
  const request = async (p, method, body) => {
    if (/reports:search$/.test(p)) {
      if (!/REGEXP_MATCH/.test(body.query)) return { results: [] };
      titleBodies.push(body);
      if (failTitles) throw new Error('Invalid query');
      const page = body.pageToken ? 1 : 0;
      return { results: titlePages[page].map(productView => ({ productView })), ...(page + 1 < titlePages.length ? { nextPageToken: 'page' + (page + 1) } : {}) };
    }
    if (/\/issues$/.test(p)) return { accountIssues: [] };
    if (/\/relationships$/.test(p)) return { accountRelationships: [{ provider: 'providers/GOOGLE_ADS', providerDisplayName: 'Google Ads' }] };
    if (/conversionSources/.test(p)) return { conversionSources: [{ name: 's', state: 'ACTIVE', merchantCenterDestination: {} }] };
    return {};
  };
  return { request, titleBodies };
}
(async () => {
  const pages = [
    [{ offerId: 'shopify_US_1_2', feedLabel: 'US', title: live, aggregatedReportingContextStatus: 'ELIGIBLE' }, { offerId: 'shopify_US_5_6', feedLabel: 'US', title: 'Pâte de verre pendant', aggregatedReportingContextStatus: 'ELIGIBLE' }],
    [{ offerId: 'shopify_CA_3_4', feedLabel: 'CA', title: cp1252, aggregatedReportingContextStatus: 'ELIGIBLE' }]
  ];
  const stub = merchant(pages), health = await M.merchantHealth({ request: stub.request, merchantId: '555', adsCustomerId: '1' }), titles = health.sections.titleIssues;
  check(stub.titleBodies.length === 2 && stub.titleBodies.every(b => /FROM product_view WHERE title REGEXP_MATCH '\[[^\]]*â[^\]]*\]'/.test(b.query) && !/\bLIMIT\b/.test(b.query) && b.pageSize === 1000) && stub.titleBodies[1].pageToken === 'page1', 'the title scan filters on the telltale characters and pages through the whole catalogue');
  check(titles.status === 'available' && titles.scanned === 3 && titles.affected === 2 && titles.complete === true, 'each candidate is confirmed locally; correct accented text is not reported');
  const first = titles.offers.find(o => o.offerId === 'shopify_US_1_2');
  check(first && first.suggestedTitle === liveFixed && first.issues[0].resolution === M.TITLE_FIX && /Garbled title/.test(first.issues[0].description), 'each garbled offer carries its suggested title and the fix');
  check(titles.offers.find(o => o.offerId === 'shopify_CA_3_4').suggestedTitle === utf8, 'a second market\'s garbled title is repaired exactly');
  check(/2 offer\(s\) show garbled characters/.test(titles.detail) && /encoding to UTF-8/.test(titles.detail) && /Shopify Google & YouTube app/.test(titles.detail), 'the section states the count and the exact fix');
  check(health.summary.attention.length === 1 && /2 offer\(s\) with a garbled title/.test(health.summary.attention[0]) && !health.summary.blocking.length && health.summary.healthy === true, 'garbled titles need attention but do not block serving');
  const failed = await M.merchantHealth({ request: merchant(pages, true).request, merchantId: '555', adsCustomerId: '1' });
  check(failed.sections.titleIssues.status === 'unavailable' && /Invalid query/.test(failed.sections.titleIssues.detail) && failed.sections.productIssues.status === 'available', 'a refused title scan is reported as unread without losing the other sections');

  // 3. The performance report names the garbled offer, its spend and the fix.
  const cx = { module: { exports: {} }, exports: {}, require: n => n === 'node-fetch' ? () => { throw Error('network forbidden'); } : require('module').createRequire(enginePath)(n),
    process: { env: { GADS_CUSTOMER_ID: '123' } }, console, Buffer, Date, Intl, Map, Set, URL, setTimeout, clearTimeout };
  vm.createContext(cx);
  vm.runInContext(fs.readFileSync(enginePath, 'utf8') + '\nmodule.exports.feedTitleTest={set:x=>{gaql=x.gaql;_fxRateToUsd=async()=>.75;_fb=false;}};', cx);
  const E = cx.module.exports, campaign = { id: '24033783547', name: 'BA · pmax test', status: 'ENABLED', advertisingChannelType: 'PERFORMANCE_MAX' };
  const row = (itemId, title, costMicros) => ({ campaign, segments: { productItemId: itemId, productTitle: title, productMerchantId: '78', productFeedLabel: 'US', productLanguage: 'en', productChannel: 'ONLINE', productCountry: 'geoTargetConstants/2840' }, metrics: { impressions: 900, clicks: 71, costMicros } });
  E.feedTitleTest.set({ gaql: async q => {
    if (q.includes('customer.time_zone')) return [{ customer: { timeZone: 'America/Toronto', currencyCode: 'CAD' } }];
    if (q.includes('shopping_performance_view')) return q.includes("'PURCHASE'") ? [] : [row('shopify_us_1_2', live, 184750653), row('shopify_us_3_4', 'Duck charm necklace', 20000000)];
    if (q.includes('FROM campaign WHERE') && q.includes('segments.date')) return [{ campaign, segments: { date: '2026-09-10' }, metrics: { impressions: 1800, clicks: 142, costMicros: 204750653 } }];
    return [];
  } });
  const r = await E.dailyStats({ start: '2026-08-30', end: '2026-09-28', campaignId: campaign.id });
  const notice = r.warnings.find(w => /^Feed problem/.test(w)) || '', bad = r.products.find(p => p.itemId === 'shopify_us_1_2'), good = r.products.find(p => p.itemId === 'shopify_us_3_4');
  check(/shopify_us_1_2/.test(notice) && /CAD 184\.75 spent/.test(notice) && notice.includes(liveFixed) && /encoding to UTF-8/.test(notice), 'the performance report names the offer, its spend, the suggested title and the fix');
  check(bad.titleProblem && bad.titleProblem.suggested === liveFixed && good.titleProblem === null && !/shopify_us_3_4/.test(notice), 'only the garbled offer is flagged, on its own row');
  check(bad.title === live && Math.round(bad.cost * 100) === 18475, 'Google\'s title and spend are reported as Google returned them');

  // 4. A new PMax draft never copies the garbled characters into ad text, and says why.
  const ids = ['shopify_US_123456_111111', 'shopify_US_234567_222222'];
  const sandbox = { module: { exports: {} }, process: { env: { GADS_CURRENCY: 'USD' } }, require: require('module').createRequire(enginePath), URL, Intl, Date, console, Buffer, setTimeout, clearTimeout, AbortController };
  vm.createContext(sandbox); vm.runInContext(fs.readFileSync(enginePath, 'utf8'), sandbox);
  let queued = null, aiCalls = 0;
  const offers = ids.map((itemId, i) => ({ itemId, title: i ? cp1252 : live, status: 'ELIGIBLE', availability: 'IN_STOCK', feedLabel: 'US', targetCountries: ['US'], issueDetails: [], customLabels: [] }));
  sandbox.mocks = { merchantCenterId: async () => '1', gaql: async () => { throw Error('No Google call in tests'); }, _assertOpportunityNotDeleted: async () => {}, fb: () => ({ db: { collection: () => ({ doc: () => ({ get: async () => ({ exists: true, data: () => ({}) }) }) }) } }), _pmaxResearchCandidate: () => ({}),
    control: async () => ({ defaultCountries: ['2124', '2840'] }), getCollections: async () => [{ handle: 'names', title: 'Name Jewelry' }], collectionProfiles: async () => ({ list: [] }), merchantProducts: async () => offers,
    discoverPmaxAudienceResource: async () => ({}), listCountries: async () => [{ code: 'CA', id: '2124' }, { code: 'US', id: '2840' }], shopifyGql: async () => ({ nodes: [] }),
    openaiJSON: async () => { aiCalls++; throw Error('No AI call in tests'); }, playbookSlice: async () => ({}), enqueueApproval: async item => { queued = item; return 'draft'; } };
  vm.runInContext(Object.keys(sandbox.mocks).map(k => k + '=mocks.' + k + ';').join('\n'), sandbox);
  await sandbox.module.exports.generatePmaxApproval({ handle: 'names', feedLabel: 'US', itemIds: ids, dailyBudget: 10 });
  const sent = JSON.stringify(queued.payload.mutateOperations);
  check(!/[âÃÂ]/.test(sent) && sent.includes('Dainty Monogram Necklace – Monogram Necklace') && sent.includes('Mother’s Day “Gift”'), 'the ad text and group names Google receives use the repaired titles');
  check(/feed title garbled on shopify_US_123456_111111, shopify_US_234567_222222: fix it at its Merchant source/.test(queued.summary), 'the draft names the garbled offers and where to fix them');
  check(queued.payload.meta.productTitles.every(t => !/â/.test(t)) && aiCalls === 0, 'saved product titles are clean and no AI call was made');
  // One product: the copy writer's fallback uses the repaired title too.
  sandbox.mocks.merchantProducts = async () => [offers[0]]; vm.runInContext('merchantProducts=mocks.merchantProducts;', sandbox);
  await sandbox.module.exports.generatePmaxApproval({ handle: 'names', feedLabel: 'US', itemIds: [ids[0]], dailyBudget: 10 });
  const single = JSON.stringify(queued.payload.mutateOperations);
  check(!/â/.test(single) && single.includes('Dainty Monogram Necklace – Monogram Necklace – Custom Block Monogram Initials'), 'a single-product draft\'s long headline uses the repaired title');

  console.log(checks + ' feed title checks passed.');
  require('./suite-guard.cjs').done();
})().catch(e => { console.error(e); process.exit(1); });
