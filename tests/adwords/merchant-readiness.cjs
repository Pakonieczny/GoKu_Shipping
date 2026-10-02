// Per-ad Merchant readiness: before a complete ad is published as Performance Max, can its product's exact offers
// show in the campaign's target countries? Mocked Merchant API only; nothing here reaches Google.
const assert = require('assert/strict'), fs = require('fs'), vm = require('vm'), path = require('path');
const FN = path.resolve(__dirname, '../../netlify/functions');
const M = require(path.join(FN, '_merchantHealth.js'));
let passed = 0;
const check = (cond, name) => { assert.ok(cond, name); passed++; console.log('PASS', name); };

// A Merchant account. offers: { offerId, feedLabel, approved, pending, disapproved, availability, issues, dest, getError }.
function merchant({ offers = [], issues = [], shipping = ['CA', 'US'], fail = {} } = {}) {
  const calls = [];
  const request = async (p, method, body) => {
    calls.push({ p, method, body });
    for (const [part, error] of Object.entries(fail)) if (p.includes(part)) throw error;
    if (/reports:search$/.test(p)) {
      const source = body.query.match(/REGEXP_MATCH '(.+)'$/)[1].replace(/\\\\/g, '\\').replace('(?i)', ''), re = new RegExp(source, 'i');
      return { results: offers.filter(o => re.test(o.offerId)).map(o => ({ productView: { offerId: o.offerId, feedLabel: o.feedLabel || 'US', languageCode: 'en', channel: 'ONLINE', aggregatedReportingContextStatus: o.status || 'ELIGIBLE', itemIssues: o.reportIssues || [] } })) };
    }
    if (/^products\/v1\/accounts\/\d+\/products\//.test(p)) {
      const [, label, offerId] = Buffer.from(p.split('/products/').pop(), 'base64url').toString('utf8').split('~'), o = offers.find(x => x.offerId === offerId && (x.feedLabel || 'US') === label);
      if (!o) throw Object.assign(new Error('Merchant Center product not found'), { status: 404 });
      if (o.getError) throw o.getError;
      return { name: p.replace('products/v1/', ''), offerId, feedLabel: label, contentLanguage: 'en', productAttributes: { availability: o.availability || 'IN_STOCK', ...(o.shipping ? { shipping: o.shipping } : {}) },
        productStatus: { destinationStatuses: o.dest || [{ reportingContext: 'SHOPPING_ADS', approvedCountries: o.approved || [], pendingCountries: o.pending || [], disapprovedCountries: o.disapproved || [] }, { reportingContext: 'FREE_LISTINGS', approvedCountries: ['US', 'CA'] }], itemLevelIssues: o.issues || [] } };
    }
    if (/\/issues$/.test(p)) return { accountIssues: issues };
    if (/shippingSettings$/.test(p)) return { services: [{ serviceName: 'Standard', active: true, deliveryCountries: shipping }] };
    throw Object.assign(new Error('unexpected ' + p), { status: 404 });
  };
  return { request, calls };
}
const US = 'shopify_US_100_1', CA = 'shopify_CA_100_1', US2 = 'shopify_US_100_2';
const imageIssue = { code: 'image_link_broken', description: 'Image cannot be fetched', severity: 'DISAPPROVED', reportingContext: 'SHOPPING_ADS', applicableCountries: ['CA', 'US'], attribute: 'image link' };
const ready = (m, extra) => M.offerReadiness({ request: m.request, merchantId: '555', offerIds: [US, CA].map(x => x.toLowerCase()), countries: ['CA', 'US'], force: true, ...(extra || {}) });

(async () => {
  // ── Country names and geo IDs ──────────────────────────────────────────────
  check(M.geoCountry('2840') === 'US' && M.geoCountry('geoTargetConstants/2124') === 'CA' && M.geoCountry('2826') === 'GB', 'Google geo target IDs name their countries (2000 + ISO numeric)');
  const named = M.countryCodesFor(['2840', '2124', '9999999', '2999'], { 2999: 'XK' });
  check(named.codes.join() === 'CA,US,XK' && named.unknown.join() === '9999999', 'a country outside the table comes from the account\'s own list, and an unknown one is reported, not guessed');

  // ── 1. Approved everywhere ─────────────────────────────────────────────────
  let m = merchant({ offers: [{ offerId: US, feedLabel: 'US', approved: ['US', 'CA'] }, { offerId: CA, feedLabel: 'CA', approved: ['CA'] }] });
  let r = await ready(m);
  check(r.status === 'ready' && !r.block && r.eligibleCountries.join() === 'CA,US', 'approved, in-stock offers in every target country are ready');
  check(/^Merchant Center: all 2 of the product's offers can show in Canada and United States\.$/.test(r.message), 'the plan says so in plain words: ' + r.message);
  const reports = m.calls.filter(c => /reports:search$/.test(c.p)), gets = m.calls.filter(c => /^products\//.test(c.p));
  check(reports.length === 1 && /offer_id REGEXP_MATCH '\(\?i\)\^\(shopify_us_100_1\|shopify_ca_100_1\)\$'/.test(reports[0].body.query), 'one report read covers every offer of the product, case-insensitively (Ads lowercases offer IDs)');
  check(gets.length === 2 && m.calls.length === 5, 'then one product read per offer found, plus account issues and shipping settings: one batched read per plan');
  check(m.calls.every(c => c.method === 'GET' || (c.method === 'POST' && /reports:search$/.test(c.p))), 'every Merchant call is a GET or a reports:search read');
  check(r.offers.every(o => o.read === 'full' && o.eligible), 'per-country Shopping ads status is read for each offer');

  // Cached briefly: the same offer set within five minutes makes no Merchant call; force reads again.
  const once = merchant({ offers: [{ offerId: US, feedLabel: 'US', approved: ['US'] }] }), usOnly = force => M.offerReadiness({ request: once.request, merchantId: '555', offerIds: [US], countries: ['US'], force });
  await usOnly(false); await usOnly(false); await usOnly(false);
  check(once.calls.length === 4, 'a repeated plan or list refresh within five minutes reuses the one read');
  await usOnly(true);
  check(once.calls.length === 8, 'force (the owner\'s live check) always reads Google again');

  // ── 2. Disapproved in one country only: a warning, never a refusal ───────────
  m = merchant({ offers: [{ offerId: US, feedLabel: 'US', approved: ['US'], disapproved: ['CA'], issues: [{ ...imageIssue, applicableCountries: ['CA'] }] }, { offerId: CA, feedLabel: 'CA', disapproved: ['CA'], issues: [imageIssue] }] });
  r = await ready(m);
  check(!r.block && r.status === 'warning' && r.eligibleCountries.join() === 'US' && r.darkCountries.join() === 'CA', 'an offer disapproved in one country leaves the plan publishable, with a warning');
  check(/1 of 2 offers can show in United States/.test(r.message) && /None can show in Canada/.test(r.message) && /None can show in Canada: 2 are disapproved \(Image cannot be fetched\): shopify_US_100_1, shopify_CA_100_1\./.test(r.message) && /only in United States/.test(r.message),
    'the warning names the country, the reason and what Performance Max will do: ' + r.message);

  // ── 3. Disapproved everywhere: refused, with the reason ─────────────────────
  m = merchant({ offers: [{ offerId: US, feedLabel: 'US', disapproved: ['US', 'CA'], issues: [imageIssue] }, { offerId: CA, feedLabel: 'CA', disapproved: ['CA'], issues: [imageIssue] }] });
  r = await ready(m);
  check(r.block && r.status === 'blocked' && r.conclusive, 'no offer eligible in any target country blocks Performance Max');
  check(/^Performance Max was not prepared: none of this product's 2 Merchant Center offers can show in Canada or United States/.test(r.reason) && /Of its offers: 2 are disapproved \(Image cannot be fetched\): shopify_US_100_1, shopify_CA_100_1\./.test(r.reason) && /Display campaigns/.test(r.reason),
    'the refusal names the offers, the reason and what still works: ' + r.reason);

  // Out of stock everywhere, or missing from Merchant Center: refused too.
  r = await ready(merchant({ offers: [{ offerId: US, feedLabel: 'US', approved: ['US', 'CA'], availability: 'OUT_OF_STOCK' }, { offerId: CA, feedLabel: 'CA', approved: ['CA'], availability: 'out of stock' }] }));
  check(r.block && /2 are out of stock: shopify_US_100_1, shopify_CA_100_1/.test(r.reason), 'approved but out of stock everywhere is refused: an out-of-stock offer serves nothing');
  r = await ready(merchant({ offers: [] }));
  check(r.block && /not in Merchant Center account 555/.test(r.reason) && r.missing.length === 2, 'offers missing from Merchant Center are refused by name');
  r = await ready(merchant({ offers: [{ offerId: US, feedLabel: 'US', dest: [{ reportingContext: 'FREE_LISTINGS', approvedCountries: ['US'] }] }, { offerId: CA, feedLabel: 'CA', dest: [{ reportingContext: 'FREE_LISTINGS', approvedCountries: ['CA'] }] }] }));
  check(r.block && /not set up for Shopping ads/.test(r.reason), 'offers approved only for free listings cannot serve Performance Max');
  r = await ready(merchant({ offers: [{ offerId: US, feedLabel: 'US', approved: ['US'] }] }), { countries: ['GB'], offerIds: [US] });
  check(r.block && /shopify_US_100_1 is not set up to show in United Kingdom \(approved for United States only\)/.test(r.reason), 'an offer approved only outside the target countries is refused, saying where it does show');

  // Still under review: never refused.
  r = await ready(merchant({ offers: [{ offerId: US, feedLabel: 'US', pending: ['US', 'CA'] }, { offerId: CA, feedLabel: 'CA', pending: ['CA'] }] }));
  check(!r.block && r.status === 'warning' && r.reviewCountries.join() === 'CA,US' && /still reviewing/.test(r.message), 'offers still under Google review warn but do not block');

  // ── 4. Account issues ──────────────────────────────────────────────────────
  const critical = (countries) => ({ title: 'Misrepresentation', severity: 'CRITICAL', impactedDestinations: [{ reportingContext: 'SHOPPING_ADS', impacts: countries.map(c => ({ regionCode: c, severity: 'CRITICAL' })) }] });
  const approvedBoth = [{ offerId: US, feedLabel: 'US', approved: ['US', 'CA'] }, { offerId: CA, feedLabel: 'CA', approved: ['CA'] }];
  r = await ready(merchant({ offers: approvedBoth, issues: [critical(['US', 'CA'])] }));
  check(r.block && /stopped by the account issue "Misrepresentation"/.test(r.reason), 'a CRITICAL account issue in every target country blocks, naming the issue');
  r = await ready(merchant({ offers: approvedBoth, issues: [critical(['CA'])] }));
  check(!r.block && r.eligibleCountries.join() === 'US' && /account issue that stops offers serving: Misrepresentation \(Canada\)/.test(r.message), 'a CRITICAL issue in one country leaves the others and says where it stops offers');
  r = await ready(merchant({ offers: approvedBoth, issues: [{ title: 'Missing return policy', severity: 'ERROR', impactedDestinations: [{ impacts: [{ regionCode: 'US' }] }] }] }));
  check(!r.block && r.status === 'warning' && r.eligibleCountries.join() === 'CA,US' && /may affect offers: Missing return policy \(United States\)/.test(r.message), 'an ERROR account issue is a warning only');
  check(M.blocksCountry(M.accountIssueFacts({ accountIssues: [{ title: 'x', severity: 'CRITICAL' }] }).blocking[0], 'US'), 'a CRITICAL issue naming no country stops offers everywhere');

  // ── 5. What cannot be read never blocks ────────────────────────────────────
  r = await ready(merchant({ offers: approvedBoth, fail: { 'reports:search': new Error('PERMISSION_DENIED') } }));
  check(r.status === 'unavailable' && !r.block && /could not be read \(Merchant Center did not list them: PERMISSION_DENIED\)/.test(r.message) && /does not block/.test(r.message), 'an unreadable report is a warning, not a refusal');
  r = await ready(merchant({ offers: [{ offerId: US, feedLabel: 'US', disapproved: ['US', 'CA'] }, { offerId: CA, feedLabel: 'CA', getError: new Error('backend error') }] }));
  check(!r.block && !r.conclusive && /Not fully checked: shopify_CA_100_1: backend error/.test(r.message), 'one offer that could not be read keeps a disapproved sibling from blocking');
  m = merchant({ offers: Array.from({ length: 6 }, (_, i) => ({ offerId: 'shopify_US_100_' + (i + 1), feedLabel: 'US', disapproved: ['US'], getError: i === 0 ? Object.assign(new Error('Quota exceeded'), { status: 429 }) : null })) });
  r = await M.offerReadiness({ request: m.request, merchantId: '555', offerIds: Array.from({ length: 6 }, (_, i) => 'shopify_us_100_' + (i + 1)), countries: ['US'], force: true });
  check(!r.block && /asked to slow down; the remaining offers were not read/.test(r.message) && m.calls.filter(c => /^products\//.test(c.p)).length === 4, 'a rate limit stops further reads instead of retrying, and leaves the plan unblocked');
  m = merchant({ offers: approvedBoth, fail: { 'reports:search': new Error('timeout') } });
  await M.offerReadiness({ request: m.request, merchantId: '555', offerIds: [CA], countries: ['CA'] });
  await M.offerReadiness({ request: m.request, merchantId: '555', offerIds: [CA], countries: ['CA'] });
  check(m.calls.filter(c => /reports:search$/.test(c.p)).length === 2, 'an unanswered read is asked again next time, not cached');
  r = await ready(merchant({ offers: approvedBoth }), { unmapped: 1, countries: ['US'] });
  check(!r.block && /1 target country could not be named/.test(r.message), 'a target country that could not be named is reported as unchecked');

  // ── 6. The campaign's feed label decides which offers it can serve ────────────
  r = await ready(merchant({ offers: [{ offerId: US, feedLabel: 'US', approved: ['US'] }, { offerId: CA, feedLabel: 'CA', approved: ['CA'] }] }), { feedLabel: 'US' });
  check(!r.block && r.eligibleCountries.join() === 'US' && /None can show in Canada: shopify_US_100_1 is not set up to show in Canada/.test(r.message) && /in the CA feed, not the campaign's US feed/.test(r.message), 'an offer in another feed than the campaign\'s is not counted, and that is said');
  r = await ready(merchant({ offers: [{ offerId: CA, feedLabel: 'CA', approved: ['CA'] }] }), { feedLabel: 'US', offerIds: [CA] });
  check(r.block && /in the CA feed, not the campaign's US feed/.test(r.reason), 'a product only in another feed than the campaign\'s is refused');

  // ── 7. Shipping coverage ───────────────────────────────────────────────────
  r = await ready(merchant({ offers: [{ offerId: US, feedLabel: 'US', approved: ['US'] }, { offerId: CA, feedLabel: 'CA' }], shipping: ['US'] }));
  check(!r.block && r.shippingUncovered.join() === 'CA' && /no active service to Canada/.test(r.message), 'a target country without a shipping service is named where no offer can show');

  // ── 8. Publication's last look (Google Ads product status) ─────────────────
  const eligible = p => p.status === 'ELIGIBLE' && p.availability === 'IN_STOCK';
  const live = [{ itemId: 'shopify_us_100_1', status: 'ELIGIBLE', availability: 'IN_STOCK', targetCountries: ['US'] }, { itemId: 'shopify_ca_100_1', status: 'NOT_ELIGIBLE', availability: 'IN_STOCK', issues: ['Image cannot be fetched'] }];
  check(M.noServableOffer(live, [US, CA], eligible, ['US', 'CA']) === null, 'one eligible offer is enough to publish: the others are named in the plan, not refused');
  const refused = M.noServableOffer(live.slice(1).concat({ itemId: 'shopify_us_100_1', status: 'ELIGIBLE', availability: 'OUT_OF_STOCK' }), [US, CA], eligible, ['US', 'CA']);
  check(/^Performance Max was not published: none of this product's 2 Merchant Center offers can serve in United States or Canada right now/.test(refused) && /shopify_US_100_1 is out of stock/.test(refused) && /shopify_CA_100_1 is not eligible \(Image cannot be fetched\)/.test(refused) && /Nothing was created/.test(refused), 'no eligible offer refuses with each offer\'s reason: ' + refused);
  check(/only targets United States/.test(M.noServableOffer(live.slice(0, 1), [US], eligible, ['GB'])) && M.noServableOffer(live.slice(0, 1), [US], eligible, []) === null, 'target countries count when Google reports them');
  check(/is not in Google Ads' product list/.test(M.noServableOffer([], [US], eligible, null)) && M.noServableOffer([], [], eligible, null) === null, 'a missing offer is named; no offers to check refuses nothing');

  // ── 9. The waiting complete ads, as the owner's live check reads them ────────
  const ads = M.pendingAdTargets([
    { id: 'design-review-a', type: 'adDesignSubmission', status: 'PENDING', designReview: { productId: '100', productTitle: 'Duck Silhouette Charm Necklace', context: { itemIds: ['shopify_us_100_1', 'shopify_ca_100_1', 'shopify_us_200_1'], feedLabel: null, countries: ['2124'] } }, submissionPreferences: { countries: ['2840', '2124'], styles: ['pmax'] } },
    { id: 'design-review-b', type: 'adDesignSubmission', status: 'PENDING', summary: 'Complete ad · Gecko Necklace', designReview: { productId: 'gid://shopify/Product/300', context: { itemIds: ['shopify_US_300_9'] } }, pipelinePlan: { summary: { merchant: { countryCodes: ['us'] } } } },
    { id: 'design-review-c', type: 'adDesignSubmission', status: 'PENDING', designReview: { productId: '400', context: { itemIds: ['shopify_US_400_1'] } } },
    { id: 'gone', type: 'adDesignSubmission', status: 'PENDING', deletedAt: 1, designReview: {} }, { id: 'done', type: 'adDesignSubmission', status: 'APPLIED', designReview: {} }, { id: 'other', type: 'pmax', status: 'PENDING' }], ['2826']);
  check(ads.length === 3 && ads[0].offerIds.join() === 'shopify_us_100_1,shopify_ca_100_1' && ads[0].geoIds.join() === '2840,2124' && ads[0].title === 'Duck Silhouette Charm Necklace', 'each waiting ad brings only its own product\'s offers and the countries chosen for its plan');
  check(ads[1].title === 'Gecko Necklace' && ads[1].offerIds.join() === 'shopify_US_300_9' && ads[1].countryCodes.join() === 'US' && ads[2].geoIds.join() === '2826', 'a prepared plan\'s countries come first; the account defaults are the last resort');

  // ── 10. Through the real plan, check and publish code ────────────────────────
  const fixture = path.join(__dirname, 'design-publication.cjs'), source = fs.readFileSync(fixture, 'utf8').split('(async()=>{')[0];
  const ctx = vm.createContext({ require: require('module').createRequire(fixture), __dirname, process, console, Buffer, Date, URL, setTimeout, clearTimeout });
  vm.runInContext(source + '\nthis.factory=engine;this.memoryFactory=memory;', ctx);
  const E = ctx.factory(), f = ctx.memoryFactory(), root = f.db.collection('workspaces').doc('test'), sharp = require('sharp');
  const assets = {};
  for (const [shape, width, height] of [['square', 600, 600], ['landscape', 1200, 628], ['portrait', 600, 750]]) { const bytes = await sharp({ create: { width, height, channels: 3, background: '#ddd' } }).jpeg().toBuffer(), p = 'Brites_GAds_Creative/test/' + shape + '.jpg'; assets[shape] = { path: p, width, height, bytes: bytes.length, hash: E.E.creativeHash(bytes.toString('base64')) }; f.files.set(p, bytes); }
  const copy = { headlines: ['Duck Charm', 'A Playful Duck Necklace', 'A Gift For Duck Lovers'], longHeadlines: ['Give a playful duck charm necklace'], descriptions: ['Shop the duck charm at Brites Jewelry.', 'Choose your favorite metal.'] };
  const w = { context: { itemIds: ['shopify_us_100_1', 'shopify_ca_100_1'], handle: 'ducks', feedLabel: null }, settings: { productId: '100', groupRef: 'g' }, job: { result: { assets } } }; await root.set(w);
  const product = { id: '100', title: 'Duck Silhouette Charm Necklace', url: 'https://britesjewelry.com/products/duck', offerIds: ['shopify_us_100_1', 'shopify_ca_100_1'] };
  const item = { sourceHash: 'source', designReview: { workspaceId: 'test', copy } };
  let saved = 0, gm = merchant({ offers: [{ offerId: US, feedLabel: 'US', disapproved: ['US', 'CA'], issues: [imageIssue] }, { offerId: CA, feedLabel: 'CA', disapproved: ['CA'], issues: [imageIssue] }] });
  E.bind({ fb: () => f, _adDesignWorkspaceRef: () => root, _reportContext: async () => ({ budgetCurrency: 'CAD' }), merchantCenterId: async () => '555', mintMerchantToken: async () => 'merchant-token', _designMerchantRequest: (...a) => gm.request(...a),
    _saveCreativeAsset: async (ws, bytes, name, meta) => { saved++; const p = 'Brites_GAds_Creative/test/' + name + '.jpg'; f.files.set(p, bytes); return { ...meta, path: p, bytes: bytes.length, hash: E.E.creativeHash(bytes.toString('base64')) }; } });
  const R = require(path.join(FN, 'googleAdsCampaignStyles.js')), prepare = (styles, identity) => E.get('_prepareCampaignStyles')({ item, context: { w, product }, choice: R.selection(styles, { pmax: 10, responsive_display: 5 }, ['2840', '2124']), identity });
  M.resetReadinessCache();
  await assert.rejects(() => prepare(['pmax'], 'a'.repeat(64)), /Performance Max was not prepared: none of this product's 2 Merchant Center offers can show in Canada or United States.*Image cannot be fetched/); passed++;
  check(saved === 0, 'a plan whose product cannot show anywhere is refused before any work is done');
  M.resetReadinessCache();
  gm = merchant({ offers: [{ offerId: US, feedLabel: 'US', approved: ['US'], disapproved: ['CA'], issues: [{ ...imageIssue, applicableCountries: ['CA'] }] }, { offerId: CA, feedLabel: 'CA', disapproved: ['CA'], issues: [imageIssue] }] });
  const plan = await prepare(['pmax', 'responsive_display'], 'b'.repeat(64));
  check(plan.summary.merchant.status === 'warning' && plan.summary.merchant.countryCodes.join() === 'CA,US' && /None can show in Canada/.test(plan.summary.note) && /Existing campaigns are unchanged\. Merchant Center:/.test(plan.summary.note),
    'a plan with an offer that cannot show in one country is prepared, and its summary says so in plain words');
  const calls = gm.calls.length;
  const display = await prepare(['responsive_display'], 'c'.repeat(64));
  check(!display.summary.merchant && gm.calls.length === calls, 'a Display-only plan makes no Merchant call: Display does not use Merchant offers');

  // "Check with Google": the validation message carries the plan's Merchant readiness, read again when stale.
  const id = 'design-review-' + 'b'.repeat(32), apRef = f.db.collection('Brites_GAds_Approvals').doc(id), payload = plan.payload, hash = E.E.creativeHash(payload);
  const stale = { ...plan.summary.merchant, checkedAt: Date.now() - 11 * 60000 };
  await apRef.set({ type: 'adDesignSubmission', status: 'PENDING', reviewHash: 'rh', sourceHash: 'source', designReview: { workspaceId: 'test', productId: '100', groupRef: 'g', context: w.context, copy }, payload, pipelinePlan: { ...plan, hash, summary: { ...plan.summary, merchant: stale } } });
  E.bind({ _adDesignPublicationContext: async () => ({ ref: root, w, product }), _adDesignSelectionHash: () => 'source', materializeReviewedCreative: async () => [], mutateAll: async () => ({}), control: async () => ({}) });
  M.resetReadinessCache();
  gm = merchant({ offers: [{ offerId: US, feedLabel: 'US', approved: ['US', 'CA'] }, { offerId: CA, feedLabel: 'CA', approved: ['CA'] }] });
  const checked = await E.E.publishAdDesignSubmission({ id, hash: 'rh', planHash: hash, validateOnly: true });
  check(/^Google accepted the campaign structure.*Merchant Center: all 2 of the product's offers can show in Canada and United States\.$/.test(checked.message) && checked.merchant.status === 'ready' && gm.calls.length === 5,
    'Check with Google reads Merchant readiness again when the plan\'s is older than ten minutes, and says it in plain words');

  // Publication's last look refuses only when no offer can serve.
  let applied = 0;
  E.bind({ applyApproval: async () => { applied++; return { status: 'VALIDATED' }; }, _pmaxIsEligible: p => p.status === 'ELIGIBLE' && p.availability === 'IN_STOCK' });
  await apRef.update({ status: 'PENDING', 'pipelinePlan.summary.merchant': plan.summary.merchant });
  E.bind({ merchantProducts: async () => [{ itemId: 'shopify_us_100_1', status: 'ELIGIBLE', availability: 'IN_STOCK' }, { itemId: 'shopify_ca_100_1', status: 'NOT_ELIGIBLE', availability: 'IN_STOCK', issues: ['Image cannot be fetched'] }] });
  const published = await E.E.publishAdDesignSubmission({ id, hash: 'rh', planHash: hash, confirmed: true });
  check(applied === 1 && /dry-run/.test(published.message), 'one ineligible variant no longer refuses the whole product: Performance Max serves the eligible offer');
  await apRef.update({ status: 'PENDING' });
  E.bind({ merchantProducts: async () => [{ itemId: 'shopify_us_100_1', status: 'ELIGIBLE', availability: 'OUT_OF_STOCK' }, { itemId: 'shopify_ca_100_1', status: 'NOT_ELIGIBLE', availability: 'IN_STOCK', issues: ['Image cannot be fetched'] }] });
  await assert.rejects(() => E.E.publishAdDesignSubmission({ id, hash: 'rh', planHash: hash, confirmed: true }), /none of this product's 2 Merchant Center offers can serve in Canada or United States right now.*out of stock/); passed++;
  check(applied === 1 && (await apRef.get()).data().status === 'PENDING', 'no offer able to serve anywhere refuses publication with each reason, and the approval stays waiting');

  console.log(passed + ' Merchant readiness checks passed.');
  require('./suite-guard.cjs').done();
})().catch(e => { console.error(e); process.exitCode = 1; });
