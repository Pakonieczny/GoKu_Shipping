// Search campaign construction: what the atomic googleAds:mutate create sends to Google.
// Offline only: the engine is loaded in a sandbox and every network/AI dependency is a local fake.
const assert = require('node:assert/strict'), fs = require('node:fs'), path = require('node:path'), vm = require('node:vm');
const filename = path.resolve(__dirname, '../../netlify/functions/googleAdsAutopilot.js');
const ctx = vm.createContext({ module: { exports: {} }, exports: {}, require: require('node:module').createRequire(filename), process: { env: { GADS_CUSTOMER_ID: '123' } }, console, Buffer, Date, Intl, Map, Set, URL, URLSearchParams, setTimeout, clearTimeout });
vm.runInContext(fs.readFileSync(filename, 'utf8'), ctx, { filename });
const E = ctx.module.exports, U = E._util;
let n = 0; const check = (v, m) => { assert.ok(v, m); n++; };
const INVALID = /[,!@%^*()={};~`<>?\\|\[\]]/;
const googleKeyword = t => typeof t === 'string' && t.length > 0 && t.length <= 80 && t.split(' ').length <= 10 && !INVALID.test(t);
const creates = (ops, key) => ops.filter(o => o[key] && o[key].create).map(o => o[key].create);
const ymd = days => new Date(Date.now() + days * 86400000).toISOString().slice(0, 10);
const longOccasion = 'Libra season, October birthdays and Halloween celestial style';
const copy = { headlines: ['Celestial Charm Jewelry', 'Moon And Star Earrings', 'Made To Order For You'], descriptions: ['Handcrafted celestial jewelry, made to order.', 'Choose your metal and your sign.'] };

(async () => {
  // Keyword text Google accepts.
  const kw = ctx._adsKeywordText;
  check(kw('Star Earrings, Gold') === 'star earrings gold', 'commas and capitals are normalized, not sent');
  check(kw('World Teachers’ Day gift') === "world teachers' day gift", 'typographic apostrophes become plain apostrophes');
  check(kw('necklace (gold) | 14k') === 'necklace gold 14k', 'symbols Google rejects are removed');
  check(kw('one two three four five six seven eight nine ten eleven') === null, 'more than 10 words cannot be a keyword');
  check(kw('x'.repeat(81)) === null && kw('') === null, 'over-long or empty text cannot be a keyword');

  // Inventory seeds never glue a descriptive occasion sentence onto a product.
  const profile = { handle: 'celestial', title: 'Celestial', typesDetail: [{ type: 'Earrings' }, { type: 'Necklace' }], motifs: ['moon', 'star', 'zodiac'], mats: ['gold filled'], personalization: ['engraving (optional)'], topProducts: [{ title: 'Moon Stud Earrings', handle: 'moon-stud-earrings', sold: 3 }], sampled: 20 };
  const seeds = ctx._inventoryKeywordSeeds(profile, longOccasion);
  check(seeds.length > 0 && seeds.every(googleKeyword), 'every inventory seed is valid keyword text');
  check(!seeds.some(s => s.includes('libra')), 'a long occasion description is not appended to product seeds');
  check(seeds.includes('engraving moon earrings'), 'parenthesized option notes are dropped from personalization seeds');
  check(ctx._inventoryKeywordSeeds(profile, "Mother's Day").includes("moon earrings mother's day"), 'a short occasion name still forms a seasonal seed');

  // Grounding rejects the stored junk and keeps singular/plural types in one ad group.
  const stored = [
    { text: 'moon stud earrings', real: true, searches: 480 }, { text: 'star stud earrings', real: true, searches: 300 },
    { text: 'zodiac charm necklace', real: true, searches: 900 }, { text: 'star pendant necklace', real: true, searches: 400 },
    { text: 'star earrings libra season, october birthdays and halloween celestial style', source: 'inventory_seed' },
    { text: 'moon earrings for my sister who loves the sky and stars every night', source: 'ai' }
  ];
  const grounded = U.groundKeywordPlan(stored, profile, longOccasion, { min: 4, max: 18 });
  check(grounded.keywords.every(k => googleKeyword(k.text)), 'grounded keywords are all valid Google keyword text');
  check(!grounded.keywords.some(k => k.text.includes('libra')), 'stored occasion-sentence seeds are not reused');
  check(grounded.rejected.some(r => /occasion description/.test(r.reason)) && grounded.rejected.some(r => /not a valid Google keyword/.test(r.reason)), 'rejections say why');
  const plural = U.groundKeywordPlan([{ text: 'birth flower bracelet' }, { text: 'engraved bar bracelets' }, { text: 'initial charm bracelet gold' }, { text: 'birth flower bracelets' }],
    { typesDetail: [{ type: 'Bracelets' }], motifs: ['flower', 'bar', 'initial'], personalization: ['engraved'] }, 'Evergreen gifting', { min: 2, max: 18 });
  check(plural.groups.length === 1 && plural.groups[0].label === 'Bracelets' && ['birth flower bracelet', 'birth flower bracelets', 'initial charm bracelet gold'].every(t => plural.groups[0].keywords.some(k => k.text === t)), '"bracelet" and "bracelets" share one ad group');

  // The atomic create itself.
  const adGroups = [
    { name: 'Earrings · ' + longOccasion, assets: copy, keywords: [{ text: 'moon stud earrings', real: true, searches: 480 }, { text: 'Star Earrings, Gold', intent: 'high' }, { text: 'first aid kit charm earrings' }, { text: 'one two three four five six seven eight nine ten eleven' }] },
    { name: 'Necklace', assets: copy, keywords: [{ text: 'zodiac necklace', real: true, searches: 900 }, { text: 'star necklace', real: true, searches: 400 }] }
  ];
  const built = E.buildSearchCampaignOps({ handle: 'celestial', title: 'Celestial' }, { label: longOccasion }, copy,
    { dailyBudget: 7.555, maxCpc: 1.237, startDate: ymd(5), endDate: ymd(40), countries: ['2124', '2840'], smartBidding: false, negatives: ['kit', 'free', 'diy, crafts', 'kit'], withAssets: false, adGroups });
  const ops = built.ops;
  check(Object.keys(ops[0])[0] === 'campaignBudgetOperation' && Object.keys(ops[1])[0] === 'campaignOperation', 'budget, then campaign, precede everything that references them');
  const budget = creates(ops, 'campaignBudgetOperation')[0], campaign = creates(ops, 'campaignOperation')[0];
  check(budget.amountMicros === 7560000 && budget.amountMicros % 10000 === 0, 'the daily budget is sent in whole cents');
  check(creates(ops, 'adGroupOperation').every(g => g.cpcBidMicros === 1240000), 'the max CPC is sent in whole cents');
  check(campaign.status === 'PAUSED' && campaign.advertisingChannelType === 'SEARCH' && campaign.containsEuPoliticalAdvertising === 'DOES_NOT_CONTAIN_EU_POLITICAL_ADVERTISING', 'campaign starts paused with the required EU political declaration');
  check(campaign.networkSettings.targetGoogleSearch && !campaign.networkSettings.targetSearchNetwork && !campaign.networkSettings.targetContentNetwork && campaign.geoTargetTypeSetting.positiveGeoTargetType === 'PRESENCE', 'Google Search only, people located in the chosen countries');
  check(/^\d{8} 00:00:00$/.test(campaign.startDateTime) && /^\d{8} 23:59:59$/.test(campaign.endDateTime) && campaign.manualCpc && campaign.manualCpc.enhancedCpcEnabled === false, 'future schedule and manual CPC are set');
  const criteria = creates(ops, 'campaignCriterionOperation');
  check(criteria.filter(c => c.language).length === 1 && criteria.find(c => c.language).language.languageConstant === 'languageConstants/1000', 'one English language criterion matches the copy and the research');
  check(criteria.filter(c => c.location).map(c => c.location.geoTargetConstant).join() === 'geoTargetConstants/2124,geoTargetConstants/2840', 'the chosen countries are targeted');
  const keywords = creates(ops, 'adGroupCriterionOperation').map(c => c.keyword);
  check(keywords.length === 5 && keywords.every(k => googleKeyword(k.text)), 'only valid keyword text is sent');
  check(keywords.some(k => k.text === 'star earrings gold' && k.matchType === 'EXACT'), 'a comma keyword is cleaned instead of failing the create');
  check(built.keywordSummary.dropped.length === 1 && built.keywordSummary.measured === 3, 'the summary reports dropped and measured keywords');
  const negatives = criteria.filter(c => c.negative).map(c => c.keyword.text);
  check(!negatives.includes('kit') && negatives.includes('free') && negatives.includes('diy crafts') && negatives.length === 2, 'negatives are valid, deduplicated and never block our own keyword');
  const names = creates(ops, 'adGroupOperation').map(g => g.name);
  check(names[0] === 'Earrings · Libra season, October birthdays and Halloween celestial' && names[0].length <= 70, 'long ad group names end on a whole word');
  const refs = new Set(creates(ops, 'adGroupOperation').map(g => g.resourceName));
  check(creates(ops, 'adGroupCriterionOperation').every(c => refs.has(c.adGroup)) && creates(ops, 'adGroupAdOperation').every(a => refs.has(a.adGroup)), 'every keyword and ad points at an ad group created in the same request');

  // Product splits (googleAdsGroups) still keep only ad-group-level operations.
  const split = E.buildSearchCampaignOps({ handle: 'duck', title: 'Duck necklace' }, null, copy, { dailyBudget: 1, maxCpc: 1, withAssets: false,
    adGroups: [{ name: 'Duck necklace', finalUrl: 'https://britesjewelry.com/products/duck', keywords: ['duck necklace', 'buy duck necklace', 'shop duck necklace', 'duck necklace brites'].map(text => ({ text, matchType: 'EXACT' })), assets: copy }] });
  check(creates(split.ops, 'adGroupCriterionOperation').length === 4 && creates(split.ops, 'adGroupAdOperation')[0].ad.finalUrls[0].endsWith('/products/duck'), 'product-split keywords and landing page are unchanged');

  // Sitelinks, callouts and snippets.
  const assetsFor = (coll, extras) => E.buildCampaignAssets(coll, 'https://britesjewelry.com/collections/' + coll.handle, 'customers/123/campaigns/-2', extras).ops.map(o => o.assetOperation && o.assetOperation.create).filter(Boolean);
  const sports = assetsFor({ handle: 'sports-athletics', title: 'Sports & Athletics' }, { snippetTypes: ['Necklace', 'Earrings', 'Charm', 'Other'], relatedCollections: [{ title: 'Gifts for Nurses & Doctors and Caregivers', handle: 'nurses' }] });
  const links = sports.filter(a => a.sitelinkAsset).map(a => a.sitelinkAsset.linkText);
  check(links[0] === 'Shop Sports & Athletics' && links.every(t => t.length <= 25) && links.includes('Gifts for Nurses &') === false, 'sitelink text ends on a whole word');
  check(sports.find(a => a.structuredSnippetAsset).structuredSnippetAsset.values.join() === 'Necklace,Earrings,Charm', 'the structured snippet never lists "Other"');
  const best = assetsFor({ handle: 'best-sellers', title: 'Best Sellers' }, {}).filter(a => a.sitelinkAsset);
  check(best.length === 1 && best[0].finalUrls[0].endsWith('/best-sellers'), 'the Best Sellers ad does not repeat its own page as a second sitelink');

  // Design Studio Search uses the same builder.
  const studio = ctx.buildDesignStudioSearchCampaignOps(U.designStudioBaseBlueprint(), { dailyBudget: 4.005, startDate: ymd(1), endDate: ymd(90), countries: ['2124'], maxCpc: 1 });
  const studioCriteria = creates(studio.ops, 'campaignCriterionOperation');
  check(studioCriteria.some(c => c.language) && creates(studio.ops, 'adGroupCriterionOperation').every(c => googleKeyword(c.keyword.text)), 'Studio Search is English-targeted with valid keywords');
  check(creates(studio.ops, 'campaignBudgetOperation')[0].amountMicros % 10000 === 0, 'Studio Search budget is in whole cents');

  // Draft generation with fakes: the generator never spends on copy for a date Google would reject.
  let copyCalls = 0, queued = null, prompt = '';
  Object.assign(ctx, {
    fb: () => null, _accountTz: async () => 'America/Toronto', collectionMeta: async handle => ({ handle, title: 'Celestial' }),
    collectionProfiles: async () => ({ list: [profile] }), generateRSAAssets: async () => { copyCalls++; return copy; },
    _enabledBudgetTotal: async () => 0, storeSignals: async () => ({ orders: 0 }), accountCvr: async () => null, _fxRateToUsd: async () => 1,
    researchOpportunity: async () => ({ ok: true, source: 'google_keyword_planner', keywords: stored, cpc: { low: 0.5, high: 1.5 }, competitionIndex: 50 }),
    getCollections: async () => [], recordOccasionUse: async () => {}, enqueueApproval: async item => { queued = item; return 'draft1'; }
  });
  const ctrl = { budgetCurrency: 'CAD', maxDailyBudgetTotal: 100, defaultCountries: ['2124'] };
  const past = await E.generateForCollection('celestial', longOccasion, 7.555, { ctrl, startDate: ymd(1), endDate: ymd(-2), maxCpc: 1.237 });
  check(!past.ok && /in the past/.test(past.reason) && copyCalls === 0, 'a past end date stops before any paid copy request');
  const backwards = await E.generateForCollection('celestial', longOccasion, 7.555, { ctrl, startDate: ymd(20), endDate: ymd(10), maxCpc: 1.237 });
  check(!backwards.ok && /before the start date/.test(backwards.reason) && copyCalls === 0, 'an end before the start stops before any paid copy request');
  const ok = await E.generateForCollection('celestial', longOccasion, 7.555, { ctrl, startDate: ymd(3), endDate: ymd(30), maxCpc: 1.237 });
  check(ok.ok && queued && ok.budget === 7.56 && ok.maxCpc === 1.24, 'the draft reports the cent values Google will store');
  const qOps = queued.payload.mutateOperations;
  check(creates(qOps, 'campaignCriterionOperation').some(c => c.language) && creates(qOps, 'adGroupCriterionOperation').every(c => googleKeyword(c.keyword.text) && !c.keyword.text.includes('libra')), 'the queued create is English-targeted with clean keywords');
  check(/CAD 1\.24\/click/.test(queued.summary) && /with measured Google demand/.test(queued.summary), 'the approval summary uses the account currency and says how many keywords have measured demand');

  // Copy request: sitelinks and callouts come from real store pages, so the model is not asked for them.
  Object.assign(ctx, { playbookSlice: async () => [], playbookText: () => '', openaiJSON: async p => { prompt = p; return { headlines: ['Moon Earrings', 'moon earrings', 'Star Necklace Gifts', 'A'.repeat(31), 'Zodiac Charms'], descriptions: ['Handcrafted to order.', 'Choose your sign.', 'Handcrafted to order.'] }; } });
  const rsa = await E.generateRSAAssets({ handle: 'celestial', title: 'Celestial' }, { label: 'Christmas' }, {});
  check(!/sitelink|callout/i.test(prompt), 'the copy request no longer pays for unused sitelinks and callouts');
  check(rsa.headlines.join('|') === 'Moon Earrings|Star Necklace Gifts|Zodiac Charms' && rsa.descriptions.length === 2 && !('sitelinks' in rsa), 'generated copy is deduplicated and length-checked');

  console.log(`PASS ${n} Search campaign build checks`);
})().catch(e => { console.error(e); process.exit(1); });
