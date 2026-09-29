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
  // A metal or stone on a product type is a generic head, not this collection; a design word keeps it.
  const floral = { typesDetail: [{ type: 'Necklace', materials: [{ t: 'rose gold' }, { t: '14k gold' }] }, { type: 'Earrings', materials: [{ t: 'freshwater pearl' }] }], motifs: ['rose', 'moon'], mats: ['sterling silver', 'gold filled'], topProducts: [{ title: 'Gold Rose Necklace' }] };
  const heads = U.groundKeywordPlan(['gold necklace', 'gold filled necklace', '14k gold hoop earrings', 'personalized gold necklace', 'rose gold necklace', 'sterling silver necklace', 'pearl earrings',
    'gold rose necklace', 'rose necklace', 'sterling silver moon earrings', 'rose gold moon necklace'].map(text => ({ text, real: true, searches: 1000 })), floral, 'Evergreen gifting', { min: 1, max: 18 });
  check(['gold necklace', 'gold filled necklace', '14k gold hoop earrings', 'personalized gold necklace', 'rose gold necklace', 'sterling silver necklace', 'pearl earrings'].every(t => heads.rejected.some(r => r.text === t && /generic material/.test(r.reason))), 'material + product type heads are rejected, whatever their volume');
  check(heads.keywords.filter(k => k.source !== 'inventory_seed').map(k => k.text).join() === 'gold rose necklace,rose necklace,sterling silver moon earrings,rose gold moon necklace', 'a motif keeps the keyword, even when the motif is also a metal colour');
  // Keywords without measured Keyword Planner demand stay usable long-tail keywords, marked unmeasured, and add no evidence.
  const mixed = U.groundKeywordPlan([{ text: 'moon stud earrings', real: true, searches: 480 }, { text: 'star stud earrings', real: true, searches: 0 }, { text: 'zodiac charm necklace', searches: 900 }], profile, 'Evergreen gifting', { min: 1, max: 18 });
  const flag = t => (mixed.keywords.find(k => k.text === t) || {}).measured;
  check(flag('moon stud earrings') === true && flag('star stud earrings') === false && flag('zodiac charm necklace') === false && mixed.keywords.filter(k => k.source === 'inventory_seed').every(k => k.measured === false), 'only Keyword Planner volume marks a keyword measured; estimates and seeds are unmeasured');
  check(mixed.evidence.measured === 1 && mixed.evidence.unmeasured === mixed.keywords.length - 1, 'grounding evidence counts measured and unmeasured keywords separately');
  const measuredOnly = U.groundKeywordPlan([{ text: 'moon stud earrings', real: true, searches: 480 }], profile, 'Evergreen gifting', { min: 1, max: 18 });
  const plusEstimate = U.groundKeywordPlan([{ text: 'moon stud earrings', real: true, searches: 480 }, { text: 'star stud earrings', searches: 5000, source: 'ai' }], profile, 'Evergreen gifting', { min: 1, max: 18 });
  check(plusEstimate.keywords.length === measuredOnly.keywords.length + 1 && plusEstimate.confidence === measuredOnly.confidence && measuredOnly.confidence < 96, 'an unmeasured keyword does not raise grounding confidence');

  // The atomic create itself.
  const adGroups = [
    { name: 'Earrings · ' + longOccasion, assets: copy, keywords: [{ text: 'moon stud earrings', real: true, searches: 480 }, { text: 'Star Earrings, Gold', intent: 'high' }, { text: 'first aid kit charm earrings' }, { text: 'one two three four five six seven eight nine ten eleven' }] },
    { name: 'Necklace', assets: copy, keywords: [{ text: 'zodiac necklace', real: true, searches: 900 }, { text: 'star necklace', real: true, searches: 400 }] }
  ];
  const built = E.buildSearchCampaignOps({ handle: 'celestial', title: 'Celestial' }, { label: longOccasion }, copy,
    { dailyBudget: 7.555, maxCpc: 1.237, startDate: ymd(5), endDate: ymd(40), countries: ['2124', '2840'], smartBidding: false, negatives: ['kit', 'free', 'diy, crafts', 'kit', { text: 'For Free', matchType: 'phrase' }], withAssets: false, adGroups });
  const ops = built.ops;
  check(Object.keys(ops[0])[0] === 'campaignBudgetOperation' && Object.keys(ops[1])[0] === 'campaignOperation', 'budget, then campaign, precede everything that references them');
  const budget = creates(ops, 'campaignBudgetOperation')[0], campaign = creates(ops, 'campaignOperation')[0];
  check(budget.amountMicros === 7560000 && budget.amountMicros % 10000 === 0, 'the daily budget is sent in whole cents');
  check(creates(ops, 'adGroupOperation').every(g => g.cpcBidMicros === 1240000), 'the max CPC is sent in whole cents');
  check(campaign.status === 'PAUSED' && campaign.advertisingChannelType === 'SEARCH' && campaign.containsEuPoliticalAdvertising === 'DOES_NOT_CONTAIN_EU_POLITICAL_ADVERTISING', 'campaign starts paused with the required EU political declaration');
  check(campaign.networkSettings.targetGoogleSearch && !campaign.networkSettings.targetSearchNetwork && !campaign.networkSettings.targetContentNetwork && campaign.geoTargetTypeSetting.positiveGeoTargetType === 'PRESENCE', 'Google Search only, people located in the chosen countries');
  check(campaign.startDateTime === ymd(5) + ' 00:00:00' && campaign.endDateTime === ymd(40) + ' 23:59:59' && campaign.manualCpc && campaign.manualCpc.enhancedCpcEnabled === false, 'future schedule (in "yyyy-MM-dd HH:mm:ss", the layout Google documents and returns) and manual CPC are set');
  const criteria = creates(ops, 'campaignCriterionOperation');
  check(criteria.filter(c => c.language).length === 1 && criteria.find(c => c.language).language.languageConstant === 'languageConstants/1000', 'one English language criterion matches the copy and the research');
  check(criteria.filter(c => c.location).map(c => c.location.geoTargetConstant).join() === 'geoTargetConstants/2124,geoTargetConstants/2840', 'the chosen countries are targeted');
  const keywords = creates(ops, 'adGroupCriterionOperation').map(c => c.keyword);
  check(keywords.length === 5 && keywords.every(k => googleKeyword(k.text)), 'only valid keyword text is sent');
  check(keywords.some(k => k.text === 'star earrings gold' && k.matchType === 'EXACT'), 'a comma keyword is cleaned instead of failing the create');
  check(built.keywordSummary.dropped.length === 1 && built.keywordSummary.measured === 3, 'the summary reports dropped and measured keywords');
  const negatives = criteria.filter(c => c.negative).map(c => c.keyword.text + '|' + c.keyword.matchType);
  check(negatives.join() === 'diy crafts|BROAD,for free|PHRASE' && built.negatives.join() === 'diy crafts,for free', 'negatives are valid, deduplicated, keep their match type and never block our own keyword or a buying search ("free")');

  // The default exclusions, read the way Google matches negatives: broad needs every word in any order,
  // phrase the words together in order. Negatives never match close variants.
  const blocks = (n, q) => n.matchType === 'PHRASE' ? (' ' + q + ' ').includes(' ' + n.text + ' ') : n.matchType === 'EXACT' ? n.text === q : n.text.split(' ').every(w => q.split(' ').includes(w));
  const defaults = Array.from(vm.runInContext('DEFAULT_NEGATIVES', ctx), n => typeof n === 'string' ? { text: n, matchType: 'BROAD' } : { text: n.text, matchType: n.matchType });
  check(defaults.every(n => googleKeyword(n.text) && ['BROAD', 'PHRASE'].includes(n.matchType)) && new Set(defaults.map(n => n.text)).size === defaults.length, 'every default exclusion is valid, unique keyword text with a match type');
  check(['nickel free earrings', 'tarnish free necklace', 'lead free', 'hypoallergenic nickel free', 'free shipping'].every(q => !defaults.some(n => blocks(n, q))), 'no default exclusion blocks "nickel free earrings", "tarnish free necklace", "lead free" or "free shipping"');
  check(['free download charm', 'free patterns', 'earrings for free'].every(q => defaults.some(n => blocks(n, q))) && !defaults.some(n => n.text === 'bulk') && defaults.some(n => n.text === 'cheap' && n.matchType === 'BROAD'),
    'freebie searches stay excluded by phrase; "bulk" (team gifts) stays searchable and "cheap" stays excluded');
  const sent = creates(E.buildSearchCampaignOps({ handle: 'celestial', title: 'Celestial' }, null, copy, { dailyBudget: 5, maxCpc: 1, withAssets: false,
    adGroups: [{ name: 'Earrings', assets: copy, keywords: ['moon stud earrings', 'star stud earrings', 'moon hoop earrings', 'star hoop earrings'].map(text => ({ text })) }] }).ops, 'campaignCriterionOperation').filter(c => c.negative).map(c => c.keyword);
  check(JSON.stringify(sent.map(k => [k.text, k.matchType])) === JSON.stringify(defaults.map(n => [n.text, n.matchType])), 'a campaign built with the defaults sends every exclusion with its match type');
  const necklaces = E.buildSearchCampaignOps({ handle: 'celestial', title: 'Celestial' }, null, copy, { dailyBudget: 5, maxCpc: 1, withAssets: false, negatives: ['earrings', { text: 'free earrings', matchType: 'PHRASE' }, 'shipping', 'free svg'],
    adGroups: [{ name: 'Necklace', assets: copy, keywords: ['zodiac necklace', 'star necklace', 'moon necklace', 'zodiac charm necklace'].map(text => ({ text })) }] });
  check(necklaces.negatives.join() === 'earrings,free svg', 'a product word can still be excluded; "free earrings" and "shipping" would stop buying searches and are left out');
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
  const studioAssets = creates(studio.ops, 'assetOperation');
  check(JSON.stringify(studioAssets.filter(a => a.calloutAsset).map(a => a.calloutAsset.calloutText)) === JSON.stringify(Array.from(vm.runInContext('_STUDIO_CALLOUTS', ctx))) && studioAssets.filter(a => a.calloutAsset).every(a => a.calloutAsset.calloutText.length <= 25), 'Studio callouts are sent whole, never cut mid-word');
  check(studioAssets.find(a => a.structuredSnippetAsset).structuredSnippetAsset.header === 'Service catalog', 'Studio tools are listed under a structured snippet header that fits them');

  // Creative review replaces ad text only: extension assets and their campaign links survive, for Search and PMax.
  const isExtension = o => (o.assetOperation && ['sitelinkAsset', 'calloutAsset', 'structuredSnippetAsset'].some(k => o.assetOperation.create[k])) || !!o.campaignAssetOperation;
  const createdFirst = list => { const made = new Set(); for (const o of list) { const c = Object.values(o)[0].create || {};
    if (['campaign', 'campaignBudget', 'asset', 'assetGroup', 'adGroup', 'parentListingGroupFilter'].some(k => typeof c[k] === 'string' && /[\/~]-\d+$/.test(c[k]) && !made.has(c[k]))) return false;
    if (c.resourceName) made.add(c.resourceName); } return true; };
  const tempNames = list => list.map(o => (Object.values(o)[0].create || {}).resourceName).filter(Boolean);
  const draft = E.buildSearchCampaignOps({ handle: 'celestial', title: 'Celestial' }, null, copy, { dailyBudget: 10, maxCpc: 1, startDate: ymd(2), endDate: ymd(30), countries: ['2124'], smartBidding: false,
    assetExtras: { snippetTypes: ['Necklace', 'Earrings', 'Charm'], relatedCollections: [{ title: 'Star Gifts', handle: 'star-gifts' }] },
    adGroups: [{ name: 'Earrings', assets: copy, keywords: ['moon stud earrings', 'star stud earrings'].map(text => ({ text })) }, { name: 'Necklace', assets: copy, keywords: ['zodiac charm necklace', 'star pendant necklace'].map(text => ({ text })) }] });
  const searchPayload = { mutateOperations: JSON.parse(JSON.stringify(draft.ops)), assetSummary: Object.assign({}, draft.assetSummary) };
  const searchExtensions = JSON.stringify(searchPayload.mutateOperations.filter(isExtension));
  const reviewedSearch = { headlines: ['Moon Stud Earrings', 'Celestial Charm Gifts', 'Made To Order For You'], descriptions: ['Handcrafted moon and star earrings, made to order.', 'Choose your metal and your sign.'] };
  ctx._putCreativeCopy(searchPayload, creates(draft.ops, 'adGroupOperation').map((g, i) => ({ key: 'g' + i, ref: g.resourceName, channel: 'search', copy: reviewedSearch })));
  check(searchPayload.mutateOperations.filter(isExtension).length === 16 && JSON.stringify(searchPayload.mutateOperations.filter(isExtension)) === searchExtensions, 'Search creative review keeps its 3 sitelinks, 4 callouts and snippet, each linked to the campaign');
  check(searchPayload.assetSummary.sitelinks === 3 && searchPayload.assetSummary.callouts === 4 && searchPayload.assetSummary.structuredSnippets === 1, 'the draft extension counts still describe the reviewed operations');
  check(creates(searchPayload.mutateOperations, 'adGroupAdOperation').every(a => a.ad.responsiveSearchAd.headlines.map(h => h.text).join() === reviewedSearch.headlines.join()) && createdFirst(searchPayload.mutateOperations), 'the reviewed Search copy replaces the drafted ad text in a valid atomic order');
  const pmax = ctx.buildPmaxCampaignOps({ handle: 'celestial', title: 'Celestial' }, { dailyBudget: 10, startDate: ymd(2), endDate: ymd(30), merchantId: '999', itemIds: ['shopify_CA_111_222'], countries: ['2124'],
    offerDetails: [{ itemId: 'shopify_CA_111_222', title: 'Moon Stud Earrings', url: 'https://britesjewelry.com/products/moon-stud-earrings' }], relatedCollections: [{ title: 'Star Gifts', handle: 'star-gifts' }] });
  const pmaxPayload = { mutateOperations: JSON.parse(JSON.stringify(pmax.ops)) };
  const pmaxExtensions = JSON.stringify(pmaxPayload.mutateOperations.filter(isExtension));
  const reviewedPmax = { headlines: ['Moon Stud Earrings', 'Celestial Charm Gifts', 'Made To Order', 'Brites Jewelry', 'Moon Earrings Gift', 'Handcrafted Studs', 'Star And Moon Studs', 'Gift For Sky Lovers', 'Little Moon Studs', 'Celestial Studs', 'Moon Charm Studs'],
    longHeadlines: ['Handcrafted moon stud earrings, made to order', 'Moon stud earrings for the sky lover in your life'], descriptions: ['Moon studs, made to order.', 'Handcrafted celestial earrings from Brites.', 'Choose your metal for each moon.', 'A small gift for sky lovers.'] };
  ctx._putCreativeCopy(pmaxPayload, [{ key: 'g0', ref: 'customers/123/assetGroups/-3', channel: 'pmax', copy: reviewedPmax }]);
  check(pmaxPayload.mutateOperations.filter(isExtension).length === 14 && JSON.stringify(pmaxPayload.mutateOperations.filter(isExtension)) === pmaxExtensions, 'PMax creative review keeps its 3 sitelinks and 4 callouts, each linked to the campaign');
  const pmaxText = {}; creates(pmaxPayload.mutateOperations, 'assetOperation').forEach(a => { if (a.textAsset) pmaxText[a.resourceName] = a.textAsset.text; });
  const attached = field => creates(pmaxPayload.mutateOperations, 'assetGroupAssetOperation').filter(l => l.fieldType === field).map(l => pmaxText[l.asset]).join('|');
  check(attached('HEADLINE') === reviewedPmax.headlines.join('|') && attached('LONG_HEADLINE') === reviewedPmax.longHeadlines.join('|') && attached('DESCRIPTION') === reviewedPmax.descriptions.join('|'), 'the PMax asset group carries exactly the reviewed text');
  const pmaxNames = tempNames(pmaxPayload.mutateOperations);
  check(new Set(pmaxNames).size === pmaxNames.length && createdFirst(pmaxPayload.mutateOperations), 'reviewed PMax operations keep unique temporary ids in a valid atomic order');

  // Draft generation with fakes: the generator never spends on copy for a date Google would reject.
  let copyCalls = 0, queued = null, prompt = '';
  Object.assign(ctx, {
    fb: () => null, _accountTz: async () => 'America/Toronto', collectionMeta: async handle => ({ handle, title: 'Celestial' }),
    collectionProfiles: async () => ({ list: [profile] }), generateRSAAssets: async () => { copyCalls++; return copy; },
    _enabledBudgetTotal: async () => 0, storeSignals: async () => ({ orders: 0 }), accountCvr: async () => null, _fxRateToUsd: async () => 1,
    researchOpportunity: async () => ({ ok: true, source: 'google_keyword_planner', keywords: stored.concat([{ text: 'moon necklace', real: true, searches: 90000 }]), cpc: { low: 0.5, high: 1.5 }, competitionIndex: 50 }),
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
  check(queued.summary.includes(`(${ymd(3)} → ${ymd(30)}, 28d)`), 'the summary states the run window Google receives, in inclusive days');
  check(!/headlines/.test(queued.summary) && new RegExp(`${queued.payload.adGroupSummary.length} ad groups? \\(copy set in creative review\\)`).test(queued.summary) && /, 2 sitelinks \+ 4 callouts, /.test(queued.summary),'the summary names the ad groups and extensions and leaves ad copy to creative review');
  check(queued.payload.keywordSummary.measured === 4 && queued.payload.keywordSummary.researched === false && queued.payload.keywordValidation.evidence.measured === 4, 'unmeasured seed keywords are reported as unmeasured, not as researched');
  check(ok.plan.expected.windowSearches === 2080, 'the forecast counts only the measured keywords this draft bids on, not every researched idea');
  const torontoToday = new Intl.DateTimeFormat('en-CA', { timeZone: 'America/Toronto', year: 'numeric', month: '2-digit', day: '2-digit' }).format(new Date());
  await E.generateForCollection('celestial', longOccasion, 7.555, { ctrl, startDate: torontoToday, endDate: ymd(30), maxCpc: 1.237 });
  const upTo = Math.round((Date.parse(ymd(30)) - Date.parse(torontoToday)) / 86400000) + 1;
  check(!creates(queued.payload.mutateOperations, 'campaignOperation')[0].startDateTime && queued.summary.includes(`(starts when enabled, ends ${ymd(30)}, up to ${upTo}d)`), 'a start of today is sent as start-on-enable and summarized that way');
  const paidBefore = copyCalls;
  ctx.researchOpportunity = async () => ({ ok: true, source: 'google_keyword_planner', keywords: stored.slice(0, 3), cpc: { low: 0.5, high: 1.5 }, competitionIndex: 50 });
  const thin = await E.generateForCollection('celestial', longOccasion, 7.555, { ctrl, startDate: ymd(3), endDate: ymd(30), maxCpc: 1.237 });
  check(!thin.ok && /measured demand/.test(thin.reason) && copyCalls === paidBefore, 'too little measured demand stops the draft before any paid copy request');

  // Start dates are judged in the account's time zone, which can be a day ahead of (or behind) UTC.
  // A zone whose date differs from UTC's right now: ahead (UTC+14) from 10:00 UTC, behind (UTC-11) before.
  const acctTz = new Date().getUTCHours() >= 10 ? 'Pacific/Kiritimati' : 'Pacific/Pago_Pago';
  vm.runInContext(`_tzCache = ${JSON.stringify(acctTz)}`, ctx);
  const acctToday = new Intl.DateTimeFormat('en-CA', { timeZone: acctTz, year: 'numeric', month: '2-digit', day: '2-digit' }).format(new Date());
  const acctTomorrow = new Date(Date.parse(acctToday) + 86400000).toISOString().slice(0, 10);
  check(!ctx._campaignScheduleFields(acctToday, ymd(30)).startDateTime && ctx._campaignScheduleFields(acctTomorrow, ymd(30)).startDateTime === acctTomorrow + ' 00:00:00', 'the account’s own today is never sent as a start time; its tomorrow is');
  vm.runInContext('_tzCache = null', ctx);

  // Copy request: sitelinks and callouts come from real store pages, so the model is not asked for them.
  Object.assign(ctx, { playbookSlice: async () => [], playbookText: () => '', openaiJSON: async p => { prompt = p; return { headlines: ['Moon Earrings', 'moon earrings', 'Star Necklace Gifts', 'A'.repeat(31), 'Zodiac Charms'], descriptions: ['Handcrafted to order.', 'Choose your sign.', 'Handcrafted to order.'] }; } });
  const rsa = await E.generateRSAAssets({ handle: 'celestial', title: 'Celestial' }, { label: 'Christmas' }, {});
  check(!/sitelink|callout/i.test(prompt), 'the copy request no longer pays for unused sitelinks and callouts');
  check(rsa.headlines.join('|') === 'Moon Earrings|Star Necklace Gifts|Zodiac Charms' && rsa.descriptions.length === 2 && !('sitelinks' in rsa), 'generated copy is deduplicated and length-checked');

  console.log(`PASS ${n} Search campaign build checks`);
})().catch(e => { console.error(e); process.exit(1); });
