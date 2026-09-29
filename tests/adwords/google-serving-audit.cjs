// Google serving check: what Google holds for a campaign, read back READ-ONLY and put in
// plain words (policy review with Google's reasons, primary status reasons, dates, locations
// and the location option, networks, conversion goals, products and the Merchant link).
// Synthetic data only. The fake Google Ads reader answers GAQL by resource, refuses anything
// that is not a SELECT, and nothing here touches the network.
const assert = require('assert/strict'), fs = require('fs'), path = require('path'), vm = require('vm');
const REPO = path.resolve(__dirname, '../..'), FN = path.join(REPO, 'netlify/functions');
const S = require(path.join(FN, '_googleAdsServing.js'));
let passed = 0;
const check = (cond, name) => { assert.ok(cond, name); passed++; };
const text = f => [f.text, f.reason || '', f.fix || ''].join(' | ');
const has = (r, level, re) => r.findings.some(f => f.level === level && re.test(text(f)));
const fact = (r, label) => (r.facts.find(f => f.label === label) || {}).value || '';

function fakeGoogle(data, { fail = {}, hang = {} } = {}) {
  const queries = [];
  const gaql = async q => {
    queries.push(q);
    assert.match(q.trim(), /^SELECT\s/, 'the serving check only reads');
    const from = (q.match(/\bFROM\s+([a-z_]+)/) || [])[1];
    if (hang[from]) return new Promise(() => {});
    const failure = typeof fail[from] === 'function' ? fail[from](q) : fail[from];
    if (failure) throw (failure instanceof Error ? failure : new Error(failure));
    return JSON.parse(JSON.stringify(data[from] || []));
  };
  return { gaql, queries };
}
const topic = (t, type, extra = {}) => ({ topic: t, type, ...extra });

(async () => {
  // ── 1. A Search campaign with the problems the brief names ───────────────────────────
  const search = {
    campaign: [{ campaign: { id: '101', name: 'nurses', status: 'PAUSED', servingStatus: 'NONE', primaryStatus: 'PAUSED',
      primaryStatusReasons: ['CAMPAIGN_PAUSED', 'BUDGET_CONSTRAINED', 'UNKNOWN'], advertisingChannelType: 'SEARCH', biddingStrategyType: 'MAXIMIZE_CONVERSION_VALUE',
      startDateTime: '2026-08-20 00:00:00', endDateTime: '2026-09-06 23:59:59',
      networkSettings: { targetGoogleSearch: true, targetSearchNetwork: true, targetContentNetwork: true },
      geoTargetTypeSetting: { positiveGeoTargetType: 'PRESENCE_OR_INTEREST' }, missingEuPoliticalAdvertisingDeclaration: true,
      finalUrlSuffix: 'utm_source=google' }, campaignBudget: { amountMicros: '8000000', deliveryMethod: 'STANDARD' } }],
    campaign_criterion: [
      { campaignCriterion: { type: 'LOCATION', location: { geoTargetConstant: 'geoTargetConstants/2840' } } },
      { campaignCriterion: { type: 'LOCATION', location: { geoTargetConstant: 'geoTargetConstants/2250' } } },
      { campaignCriterion: { type: 'KEYWORD', negative: true, keyword: { text: 'free' } } }],
    campaign_conversion_goal: [
      { campaignConversionGoal: { category: 'PURCHASE', origin: 'WEBSITE', biddable: true } },
      { campaignConversionGoal: { category: 'ADD_TO_CART', origin: 'WEBSITE', biddable: true } },
      { campaignConversionGoal: { category: 'BEGIN_CHECKOUT', origin: 'WEBSITE' } }],   // biddable false is omitted in JSON
    conversion_goal_campaign_config: [{ conversionGoalCampaignConfig: { goalConfigLevel: 'CUSTOMER' } }],
    conversion_action: [
      { conversionAction: { id: '1', name: 'Purchase', status: 'ENABLED', type: 'WEBPAGE', category: 'PURCHASE', origin: 'WEBSITE', primaryForGoal: true } },
      { conversionAction: { id: '2', name: 'offline (Upload)', status: 'ENABLED', type: 'UPLOAD_CLICKS', category: 'PURCHASE', origin: 'WEBSITE' } }, // unset = primary
      { conversionAction: { id: '3', name: 'Add To Cart', status: 'ENABLED', type: 'WEBPAGE', category: 'ADD_TO_CART', origin: 'WEBSITE', primaryForGoal: true } },
      { conversionAction: { id: '4', name: 'Begin Checkout', status: 'ENABLED', type: 'WEBPAGE', category: 'BEGIN_CHECKOUT', origin: 'WEBSITE', primaryForGoal: true } },
      { conversionAction: { id: '5', name: 'Page view', status: 'ENABLED', type: 'WEBPAGE', category: 'PAGE_VIEW', origin: 'WEBSITE', primaryForGoal: false } }],
    customer: [{ customer: { id: '123', status: 'ENABLED', autoTaggingEnabled: false, currencyCode: 'CAD', timeZone: 'America/Toronto' } }],
    ad_group: [{ adGroup: { id: '11', name: 'Nurse charms', status: 'ENABLED' } }, { adGroup: { id: '12', name: 'Beach charms', status: 'ENABLED' } }],
    ad_group_ad: [
      { adGroup: { id: '11', name: 'Nurse charms', status: 'ENABLED' }, adGroupAd: { status: 'ENABLED', ad: { id: '9001', finalUrls: ['https://britesjewelry.com/collections/nurse'] },
        policySummary: { approvalStatus: 'DISAPPROVED', reviewStatus: 'REVIEWED', policyTopicEntries: [
          topic('TRADEMARKS_IN_AD_TEXT', 'PROHIBITED', { evidences: [{ textList: { texts: ['Fakebrand'] } }], constraints: [{ countryConstraintList: { countries: [{ countryCriterion: 'geoTargetConstants/2840' }] } }] }),
          topic('SOME_INFO', 'DESCRIPTIVE')] } } },
      { adGroup: { id: '12', name: 'Beach charms', status: 'ENABLED' }, adGroupAd: { status: 'ENABLED', ad: { id: '9002', finalUrls: ['http://example.net/beach'] }, adStrength: 'POOR',
        actionItems: ['Try adding a few more unique headlines'], policySummary: { approvalStatus: 'APPROVED_LIMITED', reviewStatus: 'REVIEWED', policyTopicEntries: [topic('HEALTH_CLAIMS', 'LIMITED')] } } },
      { adGroup: { id: '12', name: 'Beach charms', status: 'ENABLED' }, adGroupAd: { status: 'ENABLED', ad: { id: '9003', finalUrls: ['https://britesjewelry.com/collections/beach'] },
        policySummary: { reviewStatus: 'REVIEW_IN_PROGRESS' }, primaryStatusReasons: ['AD_GROUP_AD_UNDER_REVIEW'] } }],
    ad_group_criterion: [
      { adGroup: { id: '11', status: 'ENABLED' }, adGroupCriterion: { status: 'ENABLED', keyword: { text: 'fakebrand charm' }, approvalStatus: 'DISAPPROVED', disapprovalReasons: ['TRADEMARKS'] } },
      { adGroup: { id: '11', status: 'ENABLED' }, adGroupCriterion: { status: 'ENABLED', keyword: { text: 'nurse charm bracelet gold' }, approvalStatus: 'APPROVED', systemServingStatus: 'RARELY_SERVED' } },
      { adGroup: { id: '11', status: 'ENABLED' }, adGroupCriterion: { status: 'ENABLED', keyword: { text: 'nurse charm' }, approvalStatus: 'APPROVED', primaryStatusReasons: ['AD_GROUP_CRITERION_BELOW_FIRST_PAGE_BID'] } }],
    ad_group_asset: [{ adGroup: { id: '11' }, adGroupAsset: { fieldType: 'AD_IMAGE', status: 'ENABLED', primaryStatusReasons: ['ASSET_DISAPPROVED'] },
      asset: { id: '77', type: 'IMAGE', name: 'nurse square', policySummary: { approvalStatus: 'DISAPPROVED', policyTopicEntries: [topic('IMAGE_QUALITY', 'PROHIBITED')] } } }],
    campaign_asset: [
      { campaignAsset: { fieldType: 'SITELINK', status: 'ENABLED' }, asset: { sitelinkAsset: { linkText: 'Shop charms' }, policySummary: { approvalStatus: 'APPROVED', reviewStatus: 'REVIEW_IN_PROGRESS' } } },
      { campaignAsset: { fieldType: 'CALLOUT', status: 'ENABLED' }, asset: { calloutAsset: { calloutText: 'Free shipping' }, policySummary: { approvalStatus: 'APPROVED_LIMITED', policyTopicEntries: [topic('MISLEADING', 'LIMITED')] } } }]
  };
  let g = fakeGoogle(search);
  let r = await S.auditCampaign({ gaql: g.gaql, customerId: '123', campaignId: '101', channel: 'SEARCH', today: '2026-09-29', shippingCountries: ['2036', '2124', '2826', '2840'], apiVersion: 'v24' });
  check(r.ok && r.readOnly && !r.partial && r.warnings.length === 0, 'a complete read produces a complete, read-only result');
  check(g.queries.every(q => /^SELECT\s/.test(q.trim())) && !g.queries.some(q => /mutate|validate_only/i.test(q)), 'every Google request is a GAQL read');
  check(r.verdict === 'blocked' && /end date \(2026-09-06\) has passed/.test(r.headline), 'a passed end date blocks serving and leads the headline');
  check(has(r, 'block', /end date \(2026-09-06\) has passed.*Set a later end date/), 'the end-date finding says how to fix it');
  check(has(r, 'risk', /presence or interest.*Presence/), 'the presence-or-interest location option is flagged');
  check(has(r, 'risk', /targets location 2250, outside your shipping countries/), 'a location outside the shipping countries is flagged');
  check(has(r, 'risk', /Search partners is on/) && has(r, 'risk', /Display expansion is on/), 'search partners and display expansion are flagged');
  check(has(r, 'risk', /2 purchase actions count: Purchase \(webpage\), offline \(Upload\) \(upload clicks\).*counts it twice and bids too high/), 'two primary purchase actions in a biddable goal are flagged as double counting');
  check(has(r, 'risk', /also optimizes for add to cart.*cheaper actions/) && !has(r, 'risk', /begin checkout|page view/), 'only goals the campaign bids on count; secondary actions and non-biddable goals do not');
  check(has(r, 'risk', /Auto-tagging is off/), 'auto-tagging off is flagged (uploaded sales need the click ID)');
  check(has(r, 'risk', /EU political ads declaration as missing/), "Google's missing EU declaration flag is surfaced");
  check(has(r, 'risk', /1 ad disapproved in "Nurse charms".*trademarks in ad text \(United States\): "Fakebrand"/) && !/some info/.test(JSON.stringify(r.findings)), "a disapproved ad shows Google's topic, where it applies and the quoted text; descriptive topics are left out");
  check(has(r, 'risk', /1 ad limited by policy.*health claims/), 'a limited ad shows its policy topic');
  check(has(r, 'note', /1 ad still under Google review/), 'an ad under review is shown');
  check(has(r, 'note', /Poor ad strength.*Try adding a few more unique headlines/), "Google's own ad-strength action item is passed through");
  check(has(r, 'risk', /1 keyword disapproved: "fakebrand charm".*trademarks/), 'a disapproved keyword shows its disapproval reason');
  check(has(r, 'note', /rarely shown \(low search volume\): "nurse charm bracelet gold"/), 'a rarely served keyword is shown');
  check(has(r, 'risk', /bid below Google's first-page estimate: "nurse charm"/), 'a keyword bid below the first-page estimate is flagged');
  check(has(r, 'risk', /Ad group "Beach charms" has no enabled keyword/), 'an ad group without keywords is flagged');
  check(has(r, 'risk', /1 ad group asset disapproved: image "nurse square".*image quality/), 'a disapproved ad group image shows its policy topic');
  check(has(r, 'note', /1 campaign asset limited by policy: callout "Free shipping".*misleading/) && has(r, 'note', /1 campaign asset still under review/), 'campaign assets show limited and under-review states');
  check(has(r, 'risk', /budget limits how often/) && has(r, 'note', /API version \(v24\) cannot name/), 'primary status reasons are translated, UNKNOWN names the API version');
  check(has(r, 'risk', /example\.net, which is not your store/) && has(r, 'risk', /1 final URL is not a valid https address/), 'landing pages off the store or without https are flagged');
  check(has(r, 'note', /No language is set/) && fact(r, 'Languages') === 'all languages', 'no language targeting is noted');
  check(/United States, location 2250 · also people interested in them/.test(fact(r, 'Locations')) && fact(r, 'Networks') === 'Google Search + search partners + display expansion', 'facts describe locations and networks in plain words');
  check(fact(r, 'Budget') === 'CAD 8.00 a day' && /auto-tagging OFF/.test(fact(r, 'Tracking')) && fact(r, 'Negative keywords') === '1 at campaign level', 'facts describe the budget, tracking and negatives');
  check(r.findings.map(f => f.level).join(',') === r.findings.map(f => f.level).sort((a, b) => ['block', 'risk', 'note'].indexOf(a) - ['block', 'risk', 'note'].indexOf(b)).join(','), 'findings are ordered block, risk, note');
  check(r.counts.block === 1 && r.counts.risk >= 14, 'counts match the findings');
  // campaign and ad_group are segmenting resources of the asset link views: a campaign.id filter must be selected too.
  for (const q of g.queries.filter(q => /FROM (campaign_asset|ad_group_asset)\b/.test(q))) check(/SELECT campaign\.id,/.test(q), 'asset link read selects the campaign.id it filters by');
  check(g.queries.some(q => /FROM ad_group_criterion/.test(q) && /negative = FALSE/.test(q)) && !g.queries.some(q => /FROM (asset_group|shopping_product|product_link)\b/.test(q)), 'a Search check reads keywords and no Performance Max resources');

  // Manual CPC: goals only affect reporting, so goal findings become notes.
  const manual = JSON.parse(JSON.stringify(search)); manual.campaign[0].campaign.biddingStrategyType = 'MANUAL_CPC';
  r = await S.auditCampaign({ gaql: fakeGoogle(manual).gaql, customerId: '123', campaignId: '101', channel: 'SEARCH', today: '2026-09-29' });
  check(has(r, 'note', /2 purchase actions count/) && !has(r, 'risk', /purchase actions count/) && has(r, 'note', /Manual CPC/), 'with manual bids the goal findings are notes');

  // ── 2. A retail Performance Max campaign ──────────────────────────────────────────────
  const ag1 = 'customers/123/assetGroups/1', ag2 = 'customers/123/assetGroups/2';
  const link = (ag, fieldType, extra = {}) => ({ assetGroupAsset: { assetGroup: ag, fieldType, status: 'ENABLED', ...extra }, asset: extra.asset || {} });
  const pmax = {
    campaign: [{ campaign: { id: '77', name: 'pmax-charms-us', status: 'PAUSED', primaryStatus: 'PAUSED', primaryStatusReasons: ['CAMPAIGN_PAUSED', 'HAS_ASSET_GROUPS_DISAPPROVED'],
      advertisingChannelType: 'PERFORMANCE_MAX', biddingStrategyType: 'MAXIMIZE_CONVERSION_VALUE', maximizeConversionValue: { targetRoas: 3.5 },
      startDateTime: '2026-09-12 00:00:00', endDateTime: '2026-10-02 23:59:59', geoTargetTypeSetting: { positiveGeoTargetType: 'PRESENCE' },
      shoppingSetting: { merchantId: '555', feedLabel: 'US' }, containsEuPoliticalAdvertising: 'DOES_NOT_CONTAIN_EU_POLITICAL_ADVERTISING',
      assetAutomationSettings: [{ assetAutomationType: 'FINAL_URL_EXPANSION_TEXT_ASSET_AUTOMATION', assetAutomationStatus: 'OPTED_OUT' }, { assetAutomationType: 'TEXT_ASSET_AUTOMATION', assetAutomationStatus: 'OPTED_OUT' }] },
      campaignBudget: { amountMicros: '13000000' } }],
    campaign_criterion: [
      { campaignCriterion: { type: 'LOCATION', location: { geoTargetConstant: 'geoTargetConstants/2840' } } },
      { campaignCriterion: { type: 'LOCATION', location: { geoTargetConstant: 'geoTargetConstants/2124' } } },
      { campaignCriterion: { type: 'LANGUAGE', language: { languageConstant: 'languageConstants/1000' } } }],
    campaign_conversion_goal: [{ campaignConversionGoal: { category: 'PURCHASE', origin: 'WEBSITE', biddable: true } }],
    conversion_action: [{ conversionAction: { name: 'Purchase', status: 'ENABLED', type: 'WEBPAGE', category: 'PURCHASE', origin: 'WEBSITE', primaryForGoal: true } }],
    customer: [{ customer: { status: 'ENABLED', autoTaggingEnabled: true, currencyCode: 'USD', timeZone: 'America/Toronto' } }],
    campaign_lifecycle_goal: [{ campaignLifecycleGoal: { customerAcquisitionGoalSettings: { optimizationMode: 'BID_HIGHER_FOR_NEW_CUSTOMER' } } }],
    asset_group: [
      { assetGroup: { id: '1', resourceName: ag1, name: 'Charms', status: 'ENABLED', primaryStatus: 'NOT_ELIGIBLE', primaryStatusReasons: ['ASSET_GROUP_DISAPPROVED'], adStrength: 'POOR',
        finalUrls: ['https://britesjewelry.com/collections/charms'], assetCoverage: { adStrengthActionItems: [
          { actionItemType: 'ADD_ASSET', addAssetDetails: { assetFieldType: 'LONG_HEADLINE', assetCount: 2 } },
          { actionItemType: 'ADD_ASSET', addAssetDetails: { assetFieldType: 'YOUTUBE_VIDEO', assetCount: 1, videoAspectRatioRequirement: 'VERTICAL' } }] } } },
      { assetGroup: { id: '2', resourceName: ag2, name: 'Necklaces', status: 'ENABLED', primaryStatus: 'ELIGIBLE', adStrength: 'AVERAGE', finalUrls: ['https://britesjewelry.com/collections/necklaces'] } }],
    asset_group_asset: [
      link(ag1, 'HEADLINE'), link(ag1, 'HEADLINE'), link(ag1, 'LONG_HEADLINE'), link(ag1, 'DESCRIPTION'), link(ag1, 'DESCRIPTION'),
      link(ag1, 'MARKETING_IMAGE'), link(ag1, 'SQUARE_MARKETING_IMAGE'), link(ag1, 'BUSINESS_NAME'),
      link(ag1, 'HEADLINE', { policySummary: { approvalStatus: 'DISAPPROVED', policyTopicEntries: [topic('TRADEMARKS_IN_AD_TEXT', 'PROHIBITED', { evidences: [{ textList: { texts: ['Fakebrand style'] } }] })] },
        primaryStatusReasons: ['ASSET_DISAPPROVED'], asset: { textAsset: { text: 'Fakebrand style charms' } } })],
    asset_group_listing_group_filter: [
      { assetGroupListingGroupFilter: { assetGroup: ag1, type: 'SUBDIVISION' } },
      { assetGroupListingGroupFilter: { assetGroup: ag1, type: 'UNIT_INCLUDED', caseValue: { productItemId: { value: 'shopify_US_1_2' } } } },
      { assetGroupListingGroupFilter: { assetGroup: ag1, type: 'UNIT_INCLUDED', caseValue: { productItemId: { value: 'shopify_US_3_4' } } } },
      { assetGroupListingGroupFilter: { assetGroup: ag1, type: 'UNIT_EXCLUDED', caseValue: { productItemId: {} } } }],
    asset_group_signal: [
      { assetGroupSignal: { assetGroup: ag1, searchTheme: { text: 'fakebrand charms' }, approvalStatus: 'DISAPPROVED', disapprovalReasons: ['TRADEMARKS'] } },
      { assetGroupSignal: { assetGroup: ag1, audience: { audience: 'customers/123/audiences/5' } } }],
    shopping_product: [
      { shoppingProduct: { itemId: 'shopify_us_1_2', status: 'ELIGIBLE', feedLabel: 'US' } },
      { shoppingProduct: { itemId: 'shopify_us_5_6', status: 'NOT_ELIGIBLE', issues: [{ errorCode: 'campaign_paused', description: 'Campaign is paused', adsSeverity: 'ERROR' }] } },
      { shoppingProduct: { itemId: 'shopify_us_7_8', status: 'NOT_ELIGIBLE', issues: [{ errorCode: 'missing_shipping', description: 'Missing shipping information', adsSeverity: 'ERROR' }] } },
      { shoppingProduct: { itemId: 'shopify_us_9_9', status: 'ELIGIBLE_LIMITED', issues: [{ description: 'Restricted in some countries', adsSeverity: 'WARNING' }] } }],
    product_link: [{ productLink: { type: 'MERCHANT_CENTER', merchantCenter: { merchantCenterId: '555' } } }]
  };
  g = fakeGoogle(pmax);
  r = await S.auditCampaign({ gaql: g.gaql, customerId: '123', campaignId: '77', today: '2026-09-29', shippingCountries: ['2124', '2840'], apiVersion: 'v24' });
  check(r.ok && r.channel === 'PERFORMANCE_MAX' && r.verdict === 'attention' && r.counts.block === 0, 'the channel is discovered from the campaign row; no blocking problem here');
  check(g.queries.some(q => q.includes("FROM shopping_product WHERE shopping_product.campaign = 'customers/123/campaigns/77'")), 'products are read in the campaign scope, with their status for this campaign');
  check(!g.queries.some(q => /FROM (ad_group_criterion|ad_group_ad|ad_group)\b/.test(q)), 'a Performance Max check reads no Search resources');
  check(has(r, 'risk', /It ends on 2026-10-02, in 3 days/), 'an end date within a week is flagged');
  check(has(r, 'risk', /from the US feed, but the campaign also targets Canada/), 'a feed label for one country with other targeted countries is flagged');
  check(has(r, 'risk', /Asset group "Charms" is disapproved.*trademarks in ad text: "Fakebrand style"/), "a disapproved asset group shows Google's topic");
  check(has(r, 'risk', /1 asset disapproved in "Charms": headline "Fakebrand style charms"/), 'the disapproved asset itself is named');
  check(has(r, 'risk', /"Charms" has Poor ad strength.*Google asks: add 2 long headlines, add 1 video \(vertical\)/), "Google's ad strength action items are shown as the fix");
  check(has(r, 'note', /"Necklaces" has Average ad strength/) && has(r, 'note', /"Necklaces" has no text or images of its own/), 'a feed-only asset group is explained, not failed');
  check(has(r, 'note', /"Charms" has fewer assets than Google's minimum: 1 logo \(has 0\)/) && !has(r, 'block', /minimum/), "a retail group below the asset minimum is a note naming the missing field type");
  check(has(r, 'risk', /1 asset group has no product filter/), 'an asset group without a product filter in a retail campaign is flagged');
  check(has(r, 'risk', /1 of 4 products cannot show.*Missing shipping information \(1\)/) && !/Campaign is paused/.test(JSON.stringify(r.findings)), 'product issues are shown with the pause-only issue ignored while paused');
  check(has(r, 'note', /1 product can show only in some places.*Restricted in some countries/), 'a limited product is noted with its issue');
  check(has(r, 'risk', /1 of 2 products in the product filter are not in Merchant Center under this campaign's feed: shopify_us_3_4/), 'a filtered product Google does not have is flagged (IDs compared case-insensitively)');
  check(has(r, 'note', /1 search theme disapproved in "Charms".*trademarks/), 'a disapproved search theme shows its reason');
  check(!has(r, 'note', /Final URL expansion is on/) && /final URL expansion off · Google-written text off · image enhancement on \(Google default\)/.test(fact(r, 'Google automation')), 'asset automation facts reflect opt-outs and Google defaults');
  check(fact(r, 'Merchant Center') === '555 · feed US' && /4 in this campaign · 2 eligible once enabled · 1 limited · 1 not eligible/.test(fact(r, 'Products')), 'Merchant Center and product facts are plain');
  check(fact(r, 'New customers') === 'bids higher for new customers' && fact(r, 'Languages') === 'English' && /target ROAS 350%/.test(fact(r, 'Bidding')), 'goal, language and bidding facts are read');
  check(!has(r, 'risk', /Merchant Center 555 is not linked/) && !r.findings.some(f => f.area === 'Goals'), 'a working link and a single purchase goal produce no findings');

  // No products, and the campaign's Merchant Center is not the linked one.
  const unlinked = JSON.parse(JSON.stringify(pmax)); unlinked.shopping_product = []; unlinked.product_link = [{ productLink: { merchantCenter: { merchantCenterId: '999' } } }];
  r = await S.auditCampaign({ gaql: fakeGoogle(unlinked).gaql, customerId: '123', campaignId: '77', channel: 'PERFORMANCE_MAX', today: '2026-09-29' });
  check(r.verdict === 'blocked' && has(r, 'block', /Google finds no products for this campaign \(Merchant Center 555, feed US\)/) && has(r, 'block', /Merchant Center 555 is not linked.*linked: 999/), 'no products and an unlinked Merchant Center block serving');
  check(has(r, 'block', /2 of 2 products in the product filter are not in Merchant Center/), 'when Google has none of the filtered products, that blocks too');

  // Every product not eligible for a real reason, campaign enabled.
  const allBad = JSON.parse(JSON.stringify(pmax)); allBad.campaign[0].campaign.status = 'ENABLED';
  allBad.shopping_product = [{ shoppingProduct: { itemId: 'shopify_us_1_2', status: 'NOT_ELIGIBLE', issues: [{ description: 'Image too small', adsSeverity: 'ERROR' }] } }];
  r = await S.auditCampaign({ gaql: fakeGoogle(allBad).gaql, customerId: '123', campaignId: '77', channel: 'PERFORMANCE_MAX', today: '2026-09-29' });
  check(has(r, 'block', /None of the 1 products can show.*Image too small \(1\)/), 'no eligible product blocks serving, with Google\'s issue');

  // ── 3. Reads that fail become warnings, never errors ─────────────────────────────────
  const withReason = JSON.parse(JSON.stringify(search)); withReason.campaign[0].campaign.primaryStatusReasons.push('MISSING_LOCATION_TARGETING');
  const failing = fakeGoogle(withReason, { fail: {
    campaign: q => /contains_eu_political_advertising/.test(q) ? '[gads] search failed: queryError=UNRECOGNIZED_FIELD · field not found' : null,
    campaign_criterion: '[gads] search failed: queryError=PROHIBITED_RESOURCE_TYPE_IN_FROM_CLAUSE · no' } });
  r = await S.auditCampaign({ gaql: failing.gaql, customerId: '123', campaignId: '101', channel: 'SEARCH', today: '2026-09-29' });
  check(r.ok && r.partial && r.warnings.some(w => /Campaign settings: read with fewer details \(Google Ads: queryError=UNRECOGNIZED_FIELD\)/.test(w)), 'an older API without a field falls back to a reduced campaign read, with a warning');
  check(r.warnings.some(w => /Locations, languages and schedule could not be read \(Google Ads: queryError=PROHIBITED_RESOURCE_TYPE_IN_FROM_CLAUSE\)/.test(w)), 'a failed read is named in the warnings with Google\'s error code only');
  check(has(r, 'risk', /It has no location targeting/) && !has(r, 'risk', /No locations are set/) && !has(r, 'note', /No language is set/), 'with the location read failed, Google\'s own missing-location reason is still shown, and nothing is guessed');
  check(failing.queries.filter(q => /FROM campaign WHERE/.test(q)).length === 2, 'the reduced campaign read is tried once after the full one');
  // A reduced campaign read carries no EU declaration or automation fields: nothing is inferred from their absence.
  const bare = { campaign: [{ campaign: { id: '9', status: 'PAUSED', advertisingChannelType: 'PERFORMANCE_MAX', geoTargetTypeSetting: { positiveGeoTargetType: 'PRESENCE' } }, campaignBudget: { amountMicros: '1000000' } }] };
  let a = S.analyze(bare, { today: '2026-09-29', reduced: ['campaign'] });
  check(!a.findings.some(f => /EU political|Final URL expansion/.test(f.text)) && !a.facts.some(f => f.label === 'Google automation'), 'a reduced read infers no EU declaration or automation state');
  a = S.analyze(bare, { today: '2026-09-29' });
  check(a.findings.some(f => /Final URL expansion is on/.test(f.text)) && /final URL expansion on \(Google default\)/.test(a.facts.find(f => f.label === 'Google automation').value), 'a full read with no automation settings means Google\'s defaults, final URL expansion on');
  const unspecified = JSON.parse(JSON.stringify(bare)); unspecified.campaign[0].campaign.containsEuPoliticalAdvertising = 'UNKNOWN';
  check(S.analyze(unspecified, { today: '2026-09-29' }).findings.some(f => /EU political ads declaration as missing/.test(f.text)), 'an unknown EU declaration is flagged from a full read');

  // The campaign itself unreadable: an answer, not an exception.
  r = await S.auditCampaign({ gaql: fakeGoogle({}, { fail: { campaign: '[gads] search failed: queryError=X' } }).gaql, customerId: '123', campaignId: '5', channel: 'SEARCH' });
  check(r.ok === false && r.verdict === 'unknown' && r.warnings.some(w => /Campaign settings could not be read/.test(w)), 'an unreadable campaign yields an unknown verdict, not an error');

  // Quota exhausted: stop asking Google, say so.
  const quota = Object.assign(new Error('Google Ads request quota is temporarily exhausted.'), { code: 'GADS_QUOTA_EXHAUSTED' });
  const q429 = fakeGoogle({}, { fail: { campaign: quota, campaign_criterion: quota, campaign_conversion_goal: quota, conversion_goal_campaign_config: quota, conversion_action: quota, customer: quota, campaign_lifecycle_goal: quota, campaign_asset: quota } });
  r = await S.auditCampaign({ gaql: q429.gaql, customerId: '123', campaignId: '5', isQuotaError: e => e.code === 'GADS_QUOTA_EXHAUSTED' });
  check(r.ok === false && r.warnings.some(w => /quota is exhausted/.test(w)) && q429.queries.length <= 8, 'a quota error stops further reads and is reported');

  // A read that never answers is cut off at the deadline.
  const slow = fakeGoogle(pmax, { hang: { shopping_product: true } }), t0 = Date.now();
  r = await S.auditCampaign({ gaql: slow.gaql, customerId: '123', campaignId: '77', channel: 'PERFORMANCE_MAX', today: '2026-09-29', deadlineMs: 60 });
  check(Date.now() - t0 < 2000 && r.ok && r.warnings.some(w => /Products in this campaign could not be read \(Google did not answer in time\.\)/.test(w)), 'a read that never answers is cut off at the deadline, the rest is still judged');

  // Missing inputs.
  check((await S.auditCampaign({ gaql: null, campaignId: '1' })).ok === false && (await S.auditCampaign({ gaql: async () => [], campaignId: 'x' })).ok === false, 'missing reader or campaign ID is refused without I/O');
  check(S.localDate('America/Toronto', Date.parse('2026-09-30T02:00:00Z')) === '2026-09-29', 'the account date follows the account time zone');
  const tzOnly = await S.auditCampaign({ gaql: fakeGoogle(search).gaql, customerId: '123', campaignId: '101', channel: 'SEARCH' });
  check(has(tzOnly, 'block', /end date \(2026-09-06\) has passed/), 'without a supplied date the account time zone gives today');

  // ── 4. The engine returns the check with the timeline and never fails because of it ────
  const enginePath = path.join(FN, 'googleAdsAutopilot.js');
  const cx = { module: { exports: {} }, exports: {}, require: n => n === 'node-fetch' ? (async () => { throw Error('network forbidden'); }) : require('module').createRequire(enginePath)(n),
    process: { env: { GADS_CUSTOMER_ID: '123' } }, console, Buffer, Date, Intl, Map, Set, URL, setTimeout, clearTimeout };
  vm.createContext(cx);
  vm.runInContext(fs.readFileSync(enginePath, 'utf8') + '\nmodule.exports.__t={set:v=>{if(v.gaql)gaql=v.gaql;if(v.fb!==undefined)_fb=v.fb;}};', cx, { filename: enginePath });
  const E = cx.module.exports;
  const engineData = JSON.parse(JSON.stringify(search));
  engineData.campaign[0].campaign.startDateTime = '2026-08-20 00:00:00';
  let engineQueries = [];
  const engineGaql = fakeGoogle(engineData).gaql;
  E.__t.set({ fb: false, gaql: async q => { engineQueries.push(q); if (/segments\.date/.test(q)) return []; return engineGaql(q); } });
  let tl = await E.campaignTimeline({ id: '101' });
  check(tl.campaign && tl.campaign.id === '101' && Array.isArray(tl.steps) && tl.serving && tl.serving.ok && tl.serving.verdict === 'blocked', 'campaignTimeline returns the serving check beside the timeline');
  check(tl.serving.apiVersion === 'v24' && tl.serving.readOnly === true && engineQueries.every(q => /^\s*SELECT\s/.test(q)), 'the engine check reports its API version and only reads');
  E.__t.set({ gaql: async q => { if (/campaign\.start_date_time/.test(q) && /FROM campaign WHERE campaign\.id = 101\s*$/.test(q.replace(/\s+/g, ' ').trim()) && !/serving_status/.test(q)) return engineGaql(q); if (/segments\.date/.test(q)) return []; throw new Error('[gads] search failed: queryError=INTERNAL'); } });
  tl = await E.campaignTimeline({ id: '101' });
  check(tl.campaign && tl.campaign.id === '101' && tl.serving && tl.serving.ok === false && tl.serving.verdict === 'unknown', 'a failing serving check leaves the timeline intact');

  // ── 5. Guard: asset link views are filtered only by fields they also select ────────────
  // campaign and ad_group are SEGMENTING resources of campaign_asset and ad_group_asset;
  // Google rejects a WHERE on them that the SELECT lacks (EXPECTED_REFERENCED_FIELD_IN_SELECT_CLAUSE).
  const SEGMENTED = { campaign_asset: ['campaign', 'ad_group'], ad_group_asset: ['campaign', 'ad_group'] };
  let scanned = 0; const offenders = [];
  for (const file of fs.readdirSync(FN).filter(n => n.endsWith('.js'))) {
    const src = fs.readFileSync(path.join(FN, file), 'utf8');
    for (const m of src.matchAll(/\bFROM\s+(campaign_asset|ad_group_asset)\b/g)) {
      const selectAt = src.lastIndexOf('SELECT', m.index);
      if (selectAt < 0 || m.index - selectAt > 3000) continue;
      const select = src.slice(selectAt, m.index), tail = src.slice(m.index, m.index + 900), end = tail.search(/`|"\s*[),;+]/);
      let where = (end > 0 ? tail.slice(0, end) : tail).split(/\bWHERE\b/)[1] || '';
      // Resolve template variables such as ${filter} to their latest definition.
      where = where.replace(/\$\{(\w+)\}/g, (all, name) => { const defs = [...src.slice(0, m.index).matchAll(new RegExp('\\b' + name + '\\s*=\\s*`([^`]*)`', 'g'))]; return defs.length ? defs[defs.length - 1][1] : all; });
      scanned++;
      for (const seg of SEGMENTED[m[1]]) for (const used of new Set([...where.matchAll(new RegExp('\\b' + seg + '\\.([a-z_]+)', 'g'))].map(x => seg + '.' + x[1])))
        if (!new RegExp('\\b' + used.replace('.', '\\.') + '\\b').test(select)) offenders.push(`${file}:${src.slice(0, m.index).split('\n').length} ${m[1]} filtered by ${used} without selecting it`);
    }
  }
  check(scanned >= 6, 'the guard found the asset link queries (' + scanned + ')');
  check(offenders.length === 0, 'every asset link query selects the campaign/ad group fields it filters by' + (offenders.length ? ': ' + offenders.join('; ') : ''));

  // ── 6. The module cannot change the account ──────────────────────────────────────────
  const moduleSource = fs.readFileSync(path.join(FN, '_googleAdsServing.js'), 'utf8');
  check(!/mutate|fetch\s*\(|require\(/.test(moduleSource.replace(/\/\/.*$/gm, '')), 'the serving module issues no mutation, fetch or other I/O of its own');
  check(Object.values(S.buildQueries({ customerId: '1', campaignId: '2', channel: 'PERFORMANCE_MAX' })).flat().concat(Object.values(S.buildQueries({ customerId: '1', campaignId: '2', channel: 'SEARCH' })).flat()).every(q => /^SELECT\s/.test(q)), 'every planned query is a SELECT');
  check(JSON.parse(fs.readFileSync(path.join(REPO, 'scripts/netlify-function-entries.json'), 'utf8')).modules.includes('_googleAdsServing.js'), 'the helper is classified as a module for the Netlify build');

  // ── 7. The console shows it ───────────────────────────────────────────────────────────
  const html = fs.readFileSync(path.join(REPO, 'brites-adwords.html'), 'utf8');
  check(/\+renderServing\(t\.serving\)/.test(html), 'the campaign timeline renders the serving check');
  const reasonLabels = html.match(/var REASON_LABEL=\{[^\n]*\};/)[0];
  check(/BUDGET_CONSTRAINED:/.test(reasonLabels) && /HAS_ASSET_GROUPS_DISAPPROVED:/.test(reasonLabels) && /MISSING_LOCATION_TARGETING:/.test(reasonLabels) && !/CAMPAIGN_BUDGET_LIMITED|BIDDING_STRATEGY_SUGGESTED/.test(reasonLabels), 'status reason labels use the reason names Google actually returns');
  const escSrc = html.match(/function esc\(s\)\{[^\n]*\}/)[0], block = html.slice(html.indexOf('var SERV_LV='), html.indexOf('// Scoped per-campaign creative job'));
  const ui = vm.createContext({}); vm.runInContext(escSrc + '\n' + block, ui);
  const shown = ui.renderServing({ ok: true, verdict: 'blocked', headline: 'Will not serve as intended: <b>x</b>', apiVersion: 'v24', checkedAt: '2026-09-29T12:00:00Z',
    findings: [{ level: 'block', area: 'Dates', text: 'End <img src=x> passed', fix: 'Extend it' }, { level: 'note', area: 'Ads', text: 'Under review', reason: 'r' }], facts: [{ label: 'Budget', value: 'CAD 8.00 a day' }], warnings: ['Keywords could not be read'] });
  check(/Stops it/.test(shown) && /&lt;img src=x&gt;/.test(shown) && !/<img/.test(shown) && /&lt;b&gt;x&lt;\/b&gt;/.test(shown), 'findings render with their level and are HTML-escaped');
  check(/<summary>Notes <span class="xd-n">1<\/span><\/summary>/.test(shown) && /What Google has now/.test(shown) && /CAD 8\.00 a day/.test(shown) && /nothing was changed/.test(shown) && /Not checked: Keywords could not be read/.test(shown), 'notes and facts fold away; unread parts are named');
  check(/Google serving check unavailable: boom/.test(ui.renderServing({ ok: false, error: 'boom' })) && ui.renderServing(null) === '', 'an unavailable check says so and a missing one renders nothing');
  let scripts = 0; for (const m of html.matchAll(/<script\b([^>]*)>([\s\S]*?)<\/script>/gi)) { if (/\bsrc\s*=/.test(m[1])) continue; new vm.Script(m[2]); scripts++; }
  check(scripts >= 1, 'every inline console script still compiles');

  console.log(`google-serving-audit: ${passed} checks passed`);
})().catch(e => { console.error(e); process.exit(1); });
