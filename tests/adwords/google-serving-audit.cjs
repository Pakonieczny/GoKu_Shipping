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

// The quoted ids of a read's `item_id IN (...)` list, un-escaped.
const idsOf = q => [...(q.match(/item_id IN \((.*)\)\s*$/s) || [, ''])[1].matchAll(/'((?:[^'\\]|\\.)*)'/g)].map(m => m[1].replace(/\\(.)/g, '$1'));

function fakeGoogle(data, { fail = {}, hang = {} } = {}) {
  const queries = [];
  const gaql = async q => {
    queries.push(q);
    assert.match(q.trim(), /^SELECT\s/, 'the serving check only reads');
    const from = (q.match(/\bFROM\s+([a-z_]+)/) || [])[1];
    if (hang[from]) return new Promise(() => {});
    const failure = typeof fail[from] === 'function' ? fail[from](q) : fail[from];
    if (failure) throw (failure instanceof Error ? failure : new Error(failure));
    let rows = data[from] || [];
    // Like Google, answer a shopping_product read by item_id only with the ids it names (exact case).
    const named = from === 'shopping_product' && /shopping_product\.item_id IN \(/.test(q) ? idsOf(q) : null;
    if (named) rows = rows.filter(r => named.includes(r.shoppingProduct.itemId));
    const limit = (q.match(/\bLIMIT\s+(\d+)\s*$/) || [])[1];
    if (limit) rows = rows.slice(0, Number(limit));
    return JSON.parse(JSON.stringify(rows));
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
  // Google returns "yyyy-MM-dd HH:mm:ss". A campaign written earlier in the compact layout ("yyyyMMdd HH:mm:ss") must read the same way.
  {
    const older = JSON.parse(JSON.stringify(search)), camp = older.campaign[0].campaign;
    camp.startDateTime = camp.startDateTime.replace(/-/g, ''); camp.endDateTime = camp.endDateTime.replace(/-/g, '');
    check(camp.startDateTime === '20260820 00:00:00' && camp.endDateTime === '20260906 23:59:59', 'the older campaign really carries the compact layout');
    const rc = await S.auditCampaign({ gaql: fakeGoogle(older).gaql, customerId: '123', campaignId: '101', channel: 'SEARCH', today: '2026-09-29', shippingCountries: ['2036', '2124', '2826', '2840'], apiVersion: 'v24' });
    check(fact(r, 'Dates') === 'starts 2026-08-20 · ends 2026-09-06' && fact(rc, 'Dates') === fact(r, 'Dates'), 'the dashed and the compact layout read as the same start and end day');
    check(JSON.stringify(rc.findings) === JSON.stringify(r.findings) && rc.verdict === r.verdict && rc.headline === r.headline, 'and give the same findings, verdict and headline');
  }
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
    // The first row is one of the two offers the product filter above names; the other rows are products the filter leaves out.
    shopping_product: [
      { shoppingProduct: { itemId: 'shopify_US_1_2', status: 'ELIGIBLE', feedLabel: 'US' } },
      { shoppingProduct: { itemId: 'shopify_US_5_6', status: 'NOT_ELIGIBLE', issues: [{ errorCode: 'campaign_paused', description: 'Campaign is paused', adsSeverity: 'ERROR' }] } },
      { shoppingProduct: { itemId: 'shopify_US_7_8', status: 'NOT_ELIGIBLE', issues: [{ errorCode: 'missing_shipping', description: 'Missing shipping information', adsSeverity: 'ERROR' }] } },
      { shoppingProduct: { itemId: 'shopify_US_9_9', status: 'ELIGIBLE_LIMITED', issues: [{ description: 'Restricted in some countries', adsSeverity: 'WARNING' }] } }],
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
  // The filter names two offers, so only those are judged: the three other products Google lists (not eligible, limited) are left out on purpose.
  check(!/Missing shipping|Restricted in some countries|Campaign is paused/.test(JSON.stringify(r.findings)) && !r.findings.some(f => /products? (cannot show|can show)|None of the/.test(f.text)), 'products the product filter leaves out are never counted or named in a finding');
  check(has(r, 'risk', /1 of 2 products in the product filter are not in Merchant Center under this campaign's feed: shopify_us_3_4/), 'a filtered product Google does not have is flagged');
  check(has(r, 'note', /1 search theme disapproved in "Charms".*trademarks/), 'a disapproved search theme shows its reason');
  check(!has(r, 'note', /Final URL expansion is on/) && /final URL expansion off · Google-written text off · image enhancement on \(Google default\)/.test(fact(r, 'Google automation')), 'asset automation facts reflect opt-outs and Google defaults');
  check(fact(r, 'Merchant Center') === '555 · feed US' && fact(r, 'Products') === '2 included · 1 can show · 1 not listed by Google · 3 others left out by the product filter', 'Merchant Center and product facts are plain: what the campaign includes, and how many others its filter leaves out');
  check(fact(r, 'New customers') === 'bids higher for new customers' && fact(r, 'Languages') === 'English' && /target ROAS 350%/.test(fact(r, 'Bidding')), 'goal, language and bidding facts are read');
  check(!has(r, 'risk', /Merchant Center 555 is not linked/) && !r.findings.some(f => f.area === 'Goals'), 'a working link and a single purchase goal produce no findings');

  // The same campaign with an "all products" filter names no offers, so every product Google lists is judged, and no targeted read is made.
  const everything = JSON.parse(JSON.stringify(pmax));
  everything.asset_group_listing_group_filter = [{ assetGroupListingGroupFilter: { assetGroup: ag1, type: 'UNIT_INCLUDED' } }];
  g = fakeGoogle(everything);
  r = await S.auditCampaign({ gaql: g.gaql, customerId: '123', campaignId: '77', today: '2026-09-29', shippingCountries: ['2124', '2840'], apiVersion: 'v24' });
  check(!g.queries.some(q => /item_id IN/.test(q)) && g.queries.filter(q => /FROM shopping_product/.test(q)).length === 1, 'with no offers named in the filter, only the plain products read is made');
  check(has(r, 'risk', /1 of 4 products cannot show.*Missing shipping information \(1\)/) && !/Campaign is paused/.test(JSON.stringify(r.findings)), 'product issues are shown with the pause-only issue ignored while paused');
  check(has(r, 'note', /1 product can show only in some places.*Restricted in some countries/), 'a limited product is noted with its issue');
  check(!has(r, 'risk', /products in the product filter/) && !has(r, 'block', /products in the product filter/) && /^4 in this campaign · 2 eligible once enabled · 1 limited · 1 not eligible$/.test(fact(r, 'Products')), 'the whole-feed product fact is unchanged');
  const missingFix = r.findings.find(f => /1 of 4 products cannot show/.test(f.text));
  check(missingFix && missingFix.fix === 'Fix the product issues in Merchant Center.', 'a real product issue keeps the Merchant Center fix');

  // No products, and the campaign's Merchant Center is not the linked one.
  const unlinked = JSON.parse(JSON.stringify(pmax)); unlinked.shopping_product = []; unlinked.product_link = [{ productLink: { merchantCenter: { merchantCenterId: '999' } } }];
  r = await S.auditCampaign({ gaql: fakeGoogle(unlinked).gaql, customerId: '123', campaignId: '77', channel: 'PERFORMANCE_MAX', today: '2026-09-29' });
  check(r.verdict === 'blocked' && has(r, 'block', /Google finds no products for this campaign \(Merchant Center 555, feed US\)/) && has(r, 'block', /Merchant Center 555 is not linked.*linked: 999/), 'no products and an unlinked Merchant Center block serving');
  check(has(r, 'block', /2 of 2 products in the product filter are not in Merchant Center/), 'when Google has none of the filtered products, that blocks too');

  // Every product not eligible for a real reason, campaign enabled.
  const allBad = JSON.parse(JSON.stringify(pmax)); allBad.campaign[0].campaign.status = 'ENABLED';
  allBad.shopping_product = [{ shoppingProduct: { itemId: 'shopify_US_1_2', status: 'NOT_ELIGIBLE', issues: [{ description: 'Image too small', adsSeverity: 'ERROR' }] } }];
  r = await S.auditCampaign({ gaql: fakeGoogle(allBad).gaql, customerId: '123', campaignId: '77', channel: 'PERFORMANCE_MAX', today: '2026-09-29' });
  check(has(r, 'block', /None of the 1 products can show.*Image too small \(1\)/), 'no eligible product blocks serving, with Google\'s issue');

  // ── 2b. A one-product campaign in a very large feed ──────────────────────────────────
  // Google answers shopping_product with the whole feed's scope, each product with its status FOR this campaign. A campaign that includes
  // only one product's offers gets every other product as "Excluded product or listing group"; the plain read is cut at 1000 of them, so it
  // can hold none of the campaign's own products. The check asks for the campaign's own offers by id, and never counts the rest.
  const ownIds = ['shopify_US_111_222', 'shopify_US_111_333'], EXCLUDED = 'Excluded product or listing group';
  const leftOut = n => Array.from({ length: n }, (_, i) => ({ shoppingProduct: { itemId: 'shopify_US_9' + String(i).padStart(5, '0'), status: 'NOT_ELIGIBLE', feedLabel: 'US', issues: [{ description: EXCLUDED, adsSeverity: 'ERROR' }] } }));
  const ownRow = (itemId, status = 'ELIGIBLE', issues) => ({ shoppingProduct: { itemId, status, feedLabel: 'US', ...(issues ? { issues } : {}) } });
  const filterOf = ids => [{ assetGroupListingGroupFilter: { assetGroup: ag1, type: 'SUBDIVISION' } },
    ...ids.map(id => ({ assetGroupListingGroupFilter: { assetGroup: ag1, type: 'UNIT_INCLUDED', caseValue: { productItemId: { value: id } } } })),
    { assetGroupListingGroupFilter: { assetGroup: ag1, type: 'UNIT_EXCLUDED', caseValue: { productItemId: {} } } }];
  // One asset group, one filter, nothing else wrong with the campaign: only its products can decide the verdict.
  const oneProduct = (rows, { ids = ownIds, enabled = false } = {}) => {
    const d = JSON.parse(JSON.stringify(pmax));
    d.campaign[0].campaign = { ...d.campaign[0].campaign, status: enabled ? 'ENABLED' : 'PAUSED', primaryStatus: enabled ? 'ELIGIBLE' : 'PAUSED', primaryStatusReasons: enabled ? [] : ['CAMPAIGN_PAUSED'], endDateTime: '2026-12-31 23:59:59' };
    d.campaign_criterion = d.campaign_criterion.filter(x => !/2124/.test(JSON.stringify(x)));
    d.asset_group = [{ assetGroup: { ...d.asset_group[0].assetGroup, primaryStatus: 'ELIGIBLE', primaryStatusReasons: [], adStrength: 'GOOD' } }];
    d.asset_group_asset = d.asset_group_asset.filter(x => !x.assetGroupAsset.policySummary).concat([link(ag1, 'LOGO')]);
    d.asset_group_signal = []; d.asset_group_listing_group_filter = filterOf(ids); d.shopping_product = rows;
    return d;
  };
  const audit = (data, extra = {}) => { const gg = fakeGoogle(data, extra.fake); return S.auditCampaign({ gaql: gg.gaql, customerId: '123', campaignId: '77', channel: 'PERFORMANCE_MAX', today: '2026-09-29', shippingCountries: ['2124', '2840'], ...extra.run }).then(res => ({ r: res, g: gg })); };
  const productsOf = res => res.findings.filter(f => f.area === 'Products');

  // (a) The real case: the first 1000 products are all left out by the filter; the two offers it includes are eligible.
  ({ r, g } = await audit(oneProduct([...leftOut(1000), ownRow(ownIds[0]), ownRow(ownIds[1])])));
  const targetedReads = g.queries.filter(q => /FROM shopping_product/.test(q) && /item_id IN \(/.test(q));
  check(targetedReads.length === 1 && JSON.stringify(idsOf(targetedReads[0])) === JSON.stringify(ownIds) && /shopping_product\.campaign = 'customers\/123\/campaigns\/77' AND shopping_product\.item_id IN \('shopify_US_111_222','shopify_US_111_333'\)\s*$/.test(targetedReads[0]),
    'the campaign\'s own offers are asked for by id, in the campaign scope, with the ids exactly as Google returned them');
  check(g.queries.filter(q => /FROM shopping_product/.test(q) && !/item_id IN/.test(q) && /LIMIT 1000\s*$/.test(q)).length === 1 && g.queries.filter(q => /FROM shopping_product/.test(q)).length === 2, 'next to it the plain 1000-product read is still made, as the fallback');
  check(productsOf(r).length === 0 && !JSON.stringify(r.findings).match(/None of the|Excluded product|1000/) && r.counts.block === 0 && r.verdict === 'ready', 'products the filter leaves out are not a finding: nothing blocks, and the verdict is ready');
  check(fact(r, 'Products') === '2 included · 2 can show · 1000+ others left out by the product filter', 'the Products fact names what the campaign includes and that the others are left out by the product filter');
  check(r.ok && !r.partial && r.warnings.length === 0, 'and the check is complete, with no warning');

  // (b) The included offers themselves are not eligible for a real reason.
  const noShipping = [{ description: 'Missing shipping settings', adsSeverity: 'ERROR' }];
  ({ r, g } = await audit(oneProduct([...leftOut(1000), ownRow(ownIds[0], 'NOT_ELIGIBLE', noShipping), ownRow(ownIds[1], 'NOT_ELIGIBLE', noShipping)], { enabled: true })));
  const noneShow = r.findings.find(f => f.level === 'block' && f.area === 'Products');
  check(r.verdict === 'blocked' && noneShow && noneShow.text === 'None of the 2 products can show.' && noneShow.reason === 'Missing shipping settings (2)' && noneShow.fix === 'Fix the product issues in Merchant Center.',
    'included offers that are not eligible block, with Google\'s reason and the Merchant Center fix');
  check(!JSON.stringify(r.findings).includes('Excluded product') && fact(r, 'Products') === '2 included · 0 can show · 2 not eligible · 1000+ others left out by the product filter', 'and the 1000 left out are still not counted');
  ({ r } = await audit(oneProduct([...leftOut(1000), ownRow(ownIds[0]), ownRow(ownIds[1], 'NOT_ELIGIBLE', noShipping)], { enabled: true })));
  check(has(r, 'risk', /1 of 2 products cannot show.*Missing shipping settings \(1\).*Fix the product issues in Merchant Center/) && !r.findings.some(f => f.level === 'block'), 'one included offer not eligible is a risk, not a block');

  // (c) An included offer whose own row says it is excluded: the product filter contradicts itself.
  ({ r } = await audit(oneProduct([...leftOut(1000), ownRow(ownIds[0]), ownRow(ownIds[1], 'NOT_ELIGIBLE', [{ description: EXCLUDED, adsSeverity: 'ERROR' }])])));
  const excl = r.findings.find(f => f.level === 'block' && f.area === 'Products');
  check(r.verdict === 'blocked' && excl && /The campaign's product filter excludes 1 of the 2 products it includes: shopify_US_111_333\./.test(excl.text) && excl.reason === 'Excluded product or listing group (1)' && excl.fix === 'Re-publish the campaign or fix its product filter.', 'an included offer Google reports as excluded blocks, saying the product filter excludes it');
  check(productsOf(r).length === 1 && !JSON.stringify(r.findings).includes('Fix the product issues in Merchant Center') && fact(r, 'Products') === '2 included · 1 can show · 1 excluded by the product filter · 1000+ others left out by the product filter', 'it is reported once, not also as a product issue');
  ({ r } = await audit(oneProduct([...leftOut(5), ownRow(ownIds[0], 'NOT_ELIGIBLE', [{ description: EXCLUDED }, { description: 'Missing shipping settings', adsSeverity: 'ERROR' }]), ownRow(ownIds[1], 'NOT_ELIGIBLE', [{ description: EXCLUDED }])])));
  check(productsOf(r).length === 1 && /excludes 2 of the 2 products it includes/.test(productsOf(r)[0].text), 'offers excluded by the filter are named even when Google lists another issue next to it; the filter is what to fix first');

  // (d) The targeted read fails (for example item_id cannot be filtered): the sample stands, but it says nothing about the campaign's own products.
  const refuse = { fake: { fail: { shopping_product: q => /item_id IN/.test(q) ? '[gads] search failed: queryError=UNRECOGNIZED_FIELD · item_id is not filterable' : null } } };
  ({ r, g } = await audit(oneProduct([...leftOut(1000), ownRow(ownIds[0]), ownRow(ownIds[1])]), refuse));
  check(r.ok && r.verdict !== 'blocked' && r.counts.block === 0 && !JSON.stringify(r.findings).match(/None of the|Excluded product|not in Merchant Center|not listed/), 'when the targeted read fails there is no false "None of the 1000" and nothing is claimed about the included offers');
  const cutNote = r.findings.find(f => f.level === 'note' && f.area === 'Products');
  check(productsOf(r).length === 1 && cutNote && /^Google's list was cut at 1000 products, so the check could not look at this campaign's own products\.$/.test(cutNote.text) && /Check again with Google/.test(cutNote.fix) && /Products view in Google Ads/.test(cutNote.fix), 'a note says the list was cut and tells the owner to check again or open the Products view in Google Ads');
  check(r.partial && r.warnings.some(w => /The products this campaign includes could not be read \(Google Ads: queryError=UNRECOGNIZED_FIELD\); the check could only sample 1000 products\./.test(w)), 'a warning says the check could only sample 1000 products');
  check(fact(r, 'Products') === "2 included · 2 not checked (Google's list was cut at 1000 products) · 1000+ others left out by the product filter" && g.queries.filter(q => /item_id IN/.test(q)).length === 1, 'the fact says they were not checked; the failing read is made once');
  // A sample that is not cut is the whole list: it can be judged, and a missing offer is missing.
  ({ r } = await audit(oneProduct([...leftOut(3), ownRow(ownIds[0])]), refuse));
  check(has(r, 'risk', /1 of 2 products in the product filter are not in Merchant Center under this campaign's feed: shopify_us_111_333/) && !r.findings.some(f => /cut at/.test(f.text)) && r.warnings.length === 1, 'when the sample is complete (fewer than 1000 products) it is judged as the targeted read would have been');
  // Too little time left for a second read: the same fallback, with its own warning.
  ({ r, g } = await audit(oneProduct([...leftOut(1000), ownRow(ownIds[0]), ownRow(ownIds[1])]), { run: { deadlineMs: 1000 } }));
  check(!g.queries.some(q => /item_id IN/.test(q)) && r.warnings.some(w => /The products this campaign includes were not read because too little time was left; the check could only sample 1000 products\./.test(w)) && has(r, 'note', /cut at 1000 products/) && r.counts.block === 0, 'with little of the deadline left the targeted read is skipped and the sample stands');
  const quotaStop = Object.assign(new Error('Google Ads request quota is temporarily exhausted.'), { code: 'GADS_QUOTA_EXHAUSTED' });
  ({ r } = await audit(oneProduct([...leftOut(1000), ownRow(ownIds[0])]), { fake: { fail: { shopping_product: q => /item_id IN/.test(q) ? quotaStop : null } }, run: { isQuotaError: e => e.code === 'GADS_QUOTA_EXHAUSTED' } }));
  check(r.quotaExhausted === true && r.warnings.some(w => /The products this campaign includes could not be read \(Google Ads request quota is exhausted\)/.test(w)) && r.counts.block === 0, 'a quota error on the targeted read is reported and falls back too');

  // The other way round: the plain sample cannot be read, the targeted read can. The included offers are judged; the others cannot be counted.
  ({ r } = await audit(oneProduct([ownRow(ownIds[0]), ownRow(ownIds[1], 'NOT_ELIGIBLE', noShipping)], { enabled: true }), { fake: { fail: { shopping_product: q => /item_id IN/.test(q) ? null : '[gads] search failed: queryError=INTERNAL' } } }));
  check(has(r, 'risk', /1 of 2 products cannot show.*Missing shipping settings/) && fact(r, 'Products') === '2 included · 1 can show · 1 not eligible' && r.warnings.some(w => /Products in this campaign could not be read/.test(w)) && !has(r, 'block', /Google finds no products/), 'the included offers are judged even when the plain sample could not be read');

  // (e) No offers named: the whole list is judged as before (also covered by the all-products campaign above).
  const everythingElse = oneProduct([...leftOut(3)]); everythingElse.asset_group_listing_group_filter = filterOf(ownIds).concat([{ assetGroupListingGroupFilter: { assetGroup: ag1, type: 'UNIT_INCLUDED' } }]);
  ({ r, g } = await audit(everythingElse));
  const wholeList = r.findings.find(f => f.level === 'block' && f.area === 'Products');
  check(!g.queries.some(q => /item_id IN/.test(q)) && wholeList && wholeList.text === 'None of the 3 products can show.', 'a filter that includes more than named offers is judged on everything Google lists, as before');
  check(wholeList.fix === "Check the campaign's product filter in Google Ads." && wholeList.reason === 'Excluded product or listing group (3)', 'and when every issue is the filter\'s exclusion, the fix is the filter, not Merchant Center');

  // (f) Included offers Google does not list at all: a note right after a publication, otherwise a block (the targeted read answers with nothing).
  ({ r, g } = await audit(oneProduct([...leftOut(1000)]), { run: { settling: true } }));
  check(r.counts.block === 0 && has(r, 'note', /2 of 2 products in the product filter are not listed for this campaign yet: shopify_us_111_222, shopify_us_111_333/) && !has(r, 'block', /Google finds no products/) && !has(r, 'note', /cut at/), 'included offers Google does not list yet are a note while settling');
  check(fact(r, 'Products') === '2 included · 2 not listed by Google · 1000+ others left out by the product filter', 'and the fact says so');
  ({ r } = await audit(oneProduct([...leftOut(1000)])));
  check(r.verdict === 'blocked' && has(r, 'block', /2 of 2 products in the product filter are not in Merchant Center under this campaign's feed.*Remove them from the filter, or fix the feed label/) && !has(r, 'block', /Google finds no products|None of the/), 'otherwise they block, without the false "None of the 1000"');
  ({ r } = await audit(oneProduct([...leftOut(1000), ownRow(ownIds[0])])));
  check(has(r, 'risk', /1 of 2 products in the product filter are not in Merchant Center under this campaign's feed: shopify_us_111_333/) && r.counts.block === 0, 'one offer missing is a risk');

  // Offers are compared in lower case; more than 100 are asked for 100 at a time and the rest is said to be unchecked; quotes in an id are escaped.
  const ownCase = S.analyze({ campaign: pmax.campaign, listingGroups: filterOf(['Shopify_US_1_2']), productsIncluded: [ownRow('shopify_us_1_2')], products: leftOut(2) }, { today: '2026-09-29' });
  check(!ownCase.findings.some(f => f.area === 'Products') && /^1 included · 1 can show · 2 others left out/.test(ownCase.facts.find(f => f.label === 'Products').value), 'an offer Google lists in another case is still the included offer');
  const many = Array.from({ length: 120 }, (_, i) => 'shopify_US_5_' + i);
  ({ r, g } = await audit(oneProduct([...leftOut(2), ...many.map(id => ownRow(id))], { ids: many })));
  const manyQuery = g.queries.find(q => /item_id IN/.test(q));
  check(JSON.stringify(idsOf(manyQuery)) === JSON.stringify(many.slice(0, 100)) && fact(r, 'Products') === '120 included · 100 can show · 20 more not checked · 2 others left out by the product filter' && productsOf(r).length === 0, 'at most 100 ids are asked for; the others are said to be unchecked, never missing');
  check(S.ownProductsQuery({ customerId: '123', campaignId: '77', itemIds: ["o'brien_1", 'a\\b', 'x\ny'] }).endsWith("item_id IN ('o\\'brien_1','a\\\\b','x y')") && JSON.stringify(idsOf(S.ownProductsQuery({ customerId: '1', campaignId: '2', itemIds: ["o'brien_1"] }))) === JSON.stringify(["o'brien_1"]), 'ids in the targeted read are quote-escaped like other GAQL literals');
  const scope = S.includedScope([{ assetGroupListingGroupFilter: { type: 'SUBDIVISION' } }, { assetGroupListingGroupFilter: { type: 'UNIT_INCLUDED', caseValue: { productItemId: { value: 'A_1' } } } }, { assetGroupListingGroupFilter: { type: 'UNIT_INCLUDED', caseValue: { productItemId: { value: 'A_1' } } } },
    { assetGroupListingGroupFilter: { type: 'UNIT_INCLUDED', caseValue: { productItemId: { value: 'a_1' } } } }, { assetGroupListingGroupFilter: { type: 'UNIT_EXCLUDED', caseValue: { productItemId: {} } } }]);
  check(JSON.stringify(scope) === '{"ids":["A_1","a_1"],"catchAll":false}' && S.includedScope([{ assetGroupListingGroupFilter: { type: 'UNIT_INCLUDED' } }]).catchAll === true && S.includedScope(undefined).ids.length === 0, 'the offers of a filter are its included nodes, de-duplicated, in the case Google returns; an unnamed included node means everything');

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

  // ── 4. The console's servingCheck reads only what it shows, and never fails because of it ──
  const enginePath = path.join(FN, 'googleAdsAutopilot.js'), fetched = [];
  const cx = { module: { exports: {} }, exports: {}, require: n => n === 'node-fetch' ? (async url => { fetched.push(String(url)); throw Error('network forbidden'); }) : require('module').createRequire(enginePath)(n),
    process: { env: { GADS_CUSTOMER_ID: '123' } }, console, Buffer, Date, Intl, Map, Set, URL, setTimeout, clearTimeout };
  vm.createContext(cx);
  vm.runInContext(fs.readFileSync(enginePath, 'utf8') + '\nmodule.exports.__t={set:v=>{if(v.gaql)gaql=v.gaql;if(v.fb!==undefined)_fb=v.fb;}};', cx, { filename: enginePath });
  const E = cx.module.exports;
  const engineData = JSON.parse(JSON.stringify(search));
  engineData.campaign[0].campaign.startDateTime = '2026-08-20 00:00:00';
  // Every Google read of one run, sorted; the account currency is cached per warm instance, so each run starts cold.
  const readsOf = async (run, data) => { const qs = [], g = fakeGoogle(data).gaql; vm.runInContext('_acctCurrencyCache = null', cx);
    E.__t.set({ fb: false, gaql: async q => { qs.push(q.replace(/\s+/g, ' ').trim()); return g(q); } }); return { out: await run(), qs: qs.sort() }; };
  let own = await readsOf(() => cx._servingCheck('101', { source: 'console' }), engineData), asked = await readsOf(() => E.servingCheck({ id: '101' }), engineData);
  check(JSON.stringify(Object.keys(asked.out)) === '["serving"]' && asked.out.serving.ok && asked.out.serving.verdict === 'blocked' && asked.out.serving.campaignId === '101', 'servingCheck returns the serving check and nothing else');
  check(asked.out.serving.apiVersion === 'v24' && asked.out.serving.readOnly === true && asked.qs.every(q => /^SELECT\s/.test(q)), 'the engine check reports its API version and only reads');
  check(asked.qs.length >= 10 && JSON.stringify(asked.qs) === JSON.stringify(own.qs), 'servingCheck runs the serving check\'s own ' + own.qs.length + ' Google reads and no other query');
  check(!asked.qs.some(q => /segments\.date|metrics\.|by_conversion_date/.test(q)) && fetched.length === 0, 'no day series, second date basis or exchange rate is read');
  own = await readsOf(() => cx._servingCheck('77', { source: 'console' }), pmax); asked = await readsOf(() => E.servingCheck({ id: '77' }), pmax);
  check(asked.out.serving.channel === 'PERFORMANCE_MAX' && JSON.stringify(asked.qs) === JSON.stringify(own.qs) && !asked.qs.some(q => /field_type IN|segments\.date|metrics\./.test(q)) && fetched.length === 0, 'for Performance Max too: no ad strength count of its own, only the serving check\'s reads');
  E.__t.set({ gaql: async () => { throw new Error('[gads] search failed: queryError=INTERNAL'); } });
  const failed = await E.servingCheck({ id: '101' });
  check(!failed.error && failed.serving && failed.serving.ok === false && failed.serving.verdict === 'unknown', 'a serving check Google cannot answer is an answer, not an error');
  let reads = 0; E.__t.set({ gaql: async () => { reads++; return []; } });
  for (const id of [undefined, '', ' ', 'abc', '101 OR 1=1']) { const r = await E.servingCheck({ id }); assert.ok(r.error && !r.serving, 'refused: ' + id); }
  check(reads === 0 && (await E.servingCheck()).error, 'a missing or malformed campaign ID is refused before any Google read');

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
  check(/^function renderServing\(s\)\{/m.test(html) && /^async function loadServing\(box\)\{[^\n]*\n(?:[^\n]*\n){0,6}[^\n]*renderServing\(s\)/m.test(html), 'the campaign details render the serving check');
  const reasonLabels = html.match(/var REASON_LABEL=\{[^\n]*\};/)[0];
  check(/BUDGET_CONSTRAINED:/.test(reasonLabels) && /HAS_ASSET_GROUPS_DISAPPROVED:/.test(reasonLabels) && /MISSING_LOCATION_TARGETING:/.test(reasonLabels) && !/CAMPAIGN_BUDGET_LIMITED|BIDDING_STRATEGY_SUGGESTED/.test(reasonLabels), 'status reason labels use the reason names Google actually returns');
  const escSrc = html.match(/function esc\(s\)\{[^\n]*\}/)[0], block = html.slice(html.indexOf('var SERV_LV='), html.indexOf('\n}\n', html.indexOf('function renderServing(')) + 3);
  const ui = vm.createContext({}); vm.runInContext(escSrc + '\n' + block, ui);
  const shown = ui.renderServing({ ok: true, verdict: 'blocked', headline: 'Will not serve as intended: <b>x</b>', apiVersion: 'v24', checkedAt: '2026-09-29T12:00:00Z',
    findings: [{ level: 'block', area: 'Dates', text: 'End <img src=x> passed', fix: 'Extend it' }, { level: 'note', area: 'Ads', text: 'Under review', reason: 'r' }], facts: [{ label: 'Budget', value: 'CAD 8.00 a day' }], warnings: ['Keywords could not be read'] });
  check(/Stops it/.test(shown) && /&lt;img src=x&gt;/.test(shown) && !/<img/.test(shown) && /&lt;b&gt;x&lt;\/b&gt;/.test(shown), 'findings render with their level and are HTML-escaped');
  check(/<summary>Notes <span class="xd-n">1<\/span><\/summary>/.test(shown) && /What Google has now/.test(shown) && /CAD 8\.00 a day/.test(shown) && /nothing was changed/.test(shown) && /Not checked: Keywords could not be read/.test(shown), 'notes and facts fold away; unread parts are named');
  check(/Google serving check unavailable: boom/.test(ui.renderServing({ ok: false, error: 'boom' })) && ui.renderServing(null) === '', 'an unavailable check says so and a missing one renders nothing');
  let scripts = 0; for (const m of html.matchAll(/<script\b([^>]*)>([\s\S]*?)<\/script>/gi)) { if (/\bsrc\s*=/.test(m[1])) continue; new vm.Script(m[2]); scripts++; }
  check(scripts >= 1, 'every inline console script still compiles');

  // ── 8. What the Overview badge keeps, and a new campaign's products while Google lists them ──
  const kept = S.summaryOf({ ok: true, verdict: 'blocked', headline: 'h'.repeat(300), counts: { block: 1, risk: 3, note: 5 }, partial: true, settling: true, channel: 'SEARCH', checkedAt: '2026-09-29T12:00:00Z',
    findings: [{ level: 'block', area: 'Dates', text: 'Ended' }, { level: 'note', area: 'Ads', text: 'Under review' }, { level: 'risk', area: 'Locations', text: 'y'.repeat(300) }, { level: 'risk', area: 'Networks', text: 'Display on' }, { level: 'risk', area: 'Goals', text: 'Two goals' }] }, 'daily');
  check(kept.verdict === 'blocked' && kept.source === 'daily' && kept.counts.block === 1 && kept.counts.risk === 3 && kept.counts.note === 5 && kept.partial && kept.settling && kept.checkedAt === '2026-09-29T12:00:00Z', 'the badge keeps the verdict, its counts, its source and when');
  check(kept.top.length === 3 && kept.top.every(f => f.level !== 'note') && kept.top[0].area === 'Dates' && kept.top[1].text.length === 200 && kept.headline.length === 240, 'it keeps the first three findings that decide it, clipped, and no notes');
  // Google's reason and the fix ride along with the first findings, so the stored card says why.
  const stuck = await S.auditCampaign({ gaql: fakeGoogle(allBad).gaql, customerId: '123', campaignId: '77', channel: 'PERFORMANCE_MAX', today: '2026-09-29' }), stuckTop = (S.summaryOf(stuck, 'publication').top || []).find(f => f.area === 'Products');
  check(stuckTop && stuckTop.level === 'block' && /^None of the 1 products can show\.$/.test(stuckTop.text) && /Image too small \(1\)/.test(stuckTop.reason) && stuckTop.fix === 'Fix the product issues in Merchant Center.', 'a blocked products finding keeps Google\'s reason and the fix in the stored summary');
  const longer = S.summaryOf({ ok: true, verdict: 'blocked', headline: 'h', counts: { block: 2 }, findings: [{ level: 'block', area: 'Products', text: 't', reason: 'r'.repeat(300), fix: 'f'.repeat(300) }, { level: 'risk', area: 'Dates', text: 'No reason here' }, { level: 'risk', area: 'Goals', text: 'Blank', reason: '', fix: '  ' }] }, 'daily').top;
  check(longer[0].reason.length === 160 && longer[0].reason.endsWith('…') && longer[0].fix.length === 160 && longer[0].fix.endsWith('…'), 'a long reason and fix are clipped to 160 characters');
  check(!('reason' in longer[1]) && !('fix' in longer[1]) && !('reason' in longer[2]) && !('fix' in longer[2]) && Object.keys(longer[1]).join() === 'level,area,text', 'a finding without a reason or fix keeps the stored shape it always had');
  check(S.summaryOf({ ok: false, verdict: 'unknown' }, 'daily') === null && S.summaryOf({ ok: false, error: 'x' }) === null && S.summaryOf(null) === null, 'a check that could not read the campaign keeps nothing');
  const unlisted = JSON.parse(JSON.stringify(pmax)); unlisted.shopping_product = [];
  r = await S.auditCampaign({ gaql: fakeGoogle(unlisted).gaql, customerId: '123', campaignId: '77', channel: 'PERFORMANCE_MAX', today: '2026-09-29' });
  check(has(r, 'block', /Google finds no products/) && has(r, 'block', /2 of 2 products in the product filter/) && r.settling === false, 'later, a campaign with no products listed is blocked');
  r = await S.auditCampaign({ gaql: fakeGoogle(unlisted).gaql, customerId: '123', campaignId: '77', channel: 'PERFORMANCE_MAX', today: '2026-09-29', settling: true });
  check(r.settling === true && !r.findings.some(f => f.level === 'block' && f.area === 'Products') && has(r, 'note', /Google has not listed products for this campaign yet \(Merchant Center 555, feed US\)/) && has(r, 'note', /2 of 2 products in the product filter are not listed for this campaign yet/), 'right after a publication, products Google has not listed yet are a note');
  // The filter now also names an offer Google reports as not eligible (a real issue), next to the eligible one and the one Google does not list.
  const named3 = JSON.parse(JSON.stringify(pmax));
  named3.asset_group_listing_group_filter.push({ assetGroupListingGroupFilter: { assetGroup: ag1, type: 'UNIT_INCLUDED', caseValue: { productItemId: { value: 'shopify_US_7_8' } } } });
  r = await S.auditCampaign({ gaql: fakeGoogle(named3).gaql, customerId: '123', campaignId: '77', channel: 'PERFORMANCE_MAX', today: '2026-09-29', settling: true });
  check(has(r, 'note', /1 of 3 products in the product filter are not listed for this campaign yet: shopify_us_3_4/) && !has(r, 'risk', /products in the product filter/) && has(r, 'risk', /1 of 2 products cannot show.*Missing shipping information/), 'a changed product filter Google has not applied yet is a note; product issues Google reports for the included products still count');
  r = await S.auditCampaign({ gaql: q429.gaql, customerId: '123', campaignId: '5', isQuotaError: e => e.code === 'GADS_QUOTA_EXHAUSTED' });
  check(r.quotaExhausted === true && (await S.auditCampaign({ gaql: fakeGoogle(search).gaql, customerId: '123', campaignId: '101', channel: 'SEARCH', today: '2026-09-29' })).quotaExhausted === false, 'the result says when Google\'s request quota stopped it');

  // ── 9. The engine keeps each verdict: after a publication, once per five minutes ─────────
  function fakeStore() {
    const docs = new Map(), writes = [];
    const docRef = (col, id) => { const key = col + '/' + id; return { id,
      get: async () => ({ exists: docs.has(key), id, data: () => docs.has(key) ? JSON.parse(JSON.stringify(docs.get(key))) : undefined }),
      set: async (v, o) => { writes.push('set ' + key); docs.set(key, o && o.merge ? { ...(docs.get(key) || {}), ...v } : v); },
      update: async v => { writes.push('update ' + key); if (!docs.has(key)) throw Object.assign(new Error('5 NOT_FOUND: no document to update'), { code: 5 }); docs.set(key, { ...docs.get(key), ...v }); },
      create: async v => { writes.push('create ' + key); docs.set(key, v); }, collection: sub => colRef(key + '/' + sub) }; };
    const colRef = col => { const q = { doc: id => docRef(col, id), where: () => q, orderBy: () => q, limit: () => q, select: () => q, startAfter: () => q,
      get: async () => { const rows = [...docs.entries()].filter(([k]) => k.startsWith(col + '/') && !k.slice(col.length + 1).includes('/')).map(([k, v]) => ({ id: k.slice(col.length + 1), exists: true, data: () => JSON.parse(JSON.stringify(v)) }));
        return { docs: rows, size: rows.length, empty: !rows.length, forEach: fn => rows.forEach(fn) }; } }; return q; };
    const db = { collection: colRef, runTransaction: async fn => fn({ get: ref => ref.get(), set: (ref, v, o) => ref.set(v, o), update: (ref, v) => ref.update(v), create: (ref, v) => ref.create(v) }),
      batch: () => { const ops = []; return { set: (ref, v, o) => ops.push(() => ref.set(v, o)), update: (ref, v) => ops.push(() => ref.update(v)), delete: () => {}, commit: async () => { for (const op of ops) await op(); } }; } };
    return { f: { db, FV: { serverTimestamp: () => new Date().toISOString(), increment: n => n, arrayUnion: (...a) => a, delete: () => null } }, docs, writes, serving: id => docs.get('Brites_GAds_Serving/' + id) };
  }
  const servingReads = qs => qs.filter(q => /FROM campaign_conversion_goal\b/.test(q)).length;   // read by the serving check, never by the version observation
  let store = fakeStore(), pubQueries = [];
  const published = JSON.parse(JSON.stringify(unlisted)); published.campaign[0].campaign.endDateTime = '2037-12-30 23:59:59';   // the engine judges against today's date
  const pubGoogle = fakeGoogle(published).gaql;
  E.__t.set({ fb: store.f, gaql: async q => { pubQueries.push(q); return pubGoogle(q); } });
  vm.runInContext('_servingRecent.clear()', cx);
  await cx._recordMutationVersions(null, [{ campaignOperation: { create: { resourceName: 'customers/123/campaigns/-1', name: 'Fixture' } } }],
    { mutateOperationResponses: [{ campaignResult: { resourceName: 'customers/123/campaigns/77' } }] }, 'fixture create', 'ledger-1');
  let doc = store.serving('77');
  check(doc && doc.source === 'publication' && doc.settling === true && doc.campaignId === '77' && doc.verdict === 'attention' && doc.counts.block === 0 && doc.lastAttempt === null, 'a publication Google accepted stores its serving verdict; a new campaign\'s unlisted products do not block it');
  check(servingReads(pubQueries) === 1 && pubQueries.every(q => /^\s*SELECT\s/.test(q)), 'the publication check reads once and only reads');
  let before = pubQueries.length;
  await cx._recordMutationVersions('campaigns', [{ update: { resourceName: 'customers/123/campaigns/77', name: 'Fixture 2' }, updateMask: 'name' }],
    { results: [{ resourceName: 'customers/123/campaigns/77' }] }, 'fixture rename', 'ledger-2');
  doc = store.serving('77');
  check(servingReads(pubQueries.slice(before)) === 0 && doc.checkedAt && Date.parse(doc.changedAt) >= Date.parse(doc.checkedAt), 'a second publication within five minutes reads nothing more and marks the verdict as older than the campaign');
  const summaries = await cx._servingSummaries(store.f);
  check(summaries['77'] && summaries['77'].verdict === 'attention' && summaries['77'].changedAt === doc.changedAt && summaries['77'].source === 'publication', 'the dashboard reads the kept verdicts from Firestore, with no Google read');
  check(/out\.servingChecks = await _servingSummaries\(f\)/.test(fs.readFileSync(enginePath, 'utf8')), 'the dashboard returns them as servingChecks');
  vm.runInContext('_servingRecent.clear()', cx);
  await cx._recordMutationVersions('campaigns', [{ update: { resourceName: 'customers/123/campaigns/77', name: 'Fixture 3' }, updateMask: 'name' }],
    { results: [{ resourceName: 'customers/123/campaigns/77' }] }, 'fixture rename later', 'ledger-3');
  doc = store.serving('77');
  check(doc.settling === false && !doc.changedAt && doc.verdict === 'blocked' && doc.top[0].area === 'Products', 'the next publication after five minutes reads again: an edit is not settling, so products Google still does not list now block it');
  // A check that cannot read the campaign keeps the last verdict and says so; an unknown campaign gets no record.
  E.__t.set({ gaql: async () => { throw new Error('[gads] search failed: queryError=INTERNAL'); } });
  const unread = await cx._servingCheck('77', { source: 'console' });
  doc = store.serving('77');
  check(unread.verdict === 'unknown' && doc.verdict === 'blocked' && doc.lastAttempt && doc.lastAttempt.source === 'console' && /could not be read/.test(doc.lastAttempt.error), 'an unreadable check keeps the last verdict and records the attempt');
  await cx._servingCheck('88', { source: 'daily' }); await cx._storeServing('abc', { verdict: 'ready', counts: {} }, 'daily'); await cx._servingChanged('99');
  check(!store.serving('88') && !store.serving('abc') && !store.serving('99'), 'nothing is created for a campaign never checked or an invalid ID');

  // ── 10. The daily pass over ENABLED campaigns ─────────────────────────────────────────
  const hours = h => new Date(Date.now() - h * 3600000).toISOString();
  store = fakeStore();
  store.docs.set('Brites_GAds_Serving/201', { verdict: 'ready', counts: {}, top: [], source: 'daily', checkedAt: hours(1) });                                 // checked an hour ago
  store.docs.set('Brites_GAds_Serving/202', { verdict: 'attention', counts: {}, top: [], source: 'publication', settling: true, checkedAt: hours(1) });      // Google was still listing products
  store.docs.set('Brites_GAds_Serving/203', { verdict: 'ready', counts: {}, top: [], source: 'daily', checkedAt: hours(2), changedAt: hours(1) });           // published since
  store.docs.set('Brites_GAds_Serving/204', { verdict: 'ready', counts: {}, top: [], source: 'daily', checkedAt: hours(7) });                                // older than six hours
  let sweepQueries = [];
  const listed = ['201', '202', '203', '204', '205'], sweepGoogle = fakeGoogle(engineData).gaql;
  const sweepGaql = (fail = null) => async q => { sweepQueries.push(q);
    if (/^SELECT campaign\.id FROM campaign WHERE campaign\.status = 'ENABLED'$/.test(q.trim())) return listed.map(id => ({ campaign: { id } }));
    if (fail) throw fail; return sweepGoogle(q); };
  E.__t.set({ fb: store.f, gaql: sweepGaql() });
  let sweep = await cx.servingSweep();
  check(sweep.enabled === 5 && sweep.fresh === 1 && sweep.checked === 4 && sweep.blocked === 4 && sweep.deferred === 0 && sweep.quotaExhausted === false, 'the daily pass checks every ENABLED campaign except one checked in the last six hours (' + JSON.stringify(sweep) + ')');
  check(['202', '203', '204', '205'].every(id => store.serving(id).source === 'daily' && store.serving(id).verdict === 'blocked' && !store.serving(id).changedAt) && store.serving('201').checkedAt === store.docs.get('Brites_GAds_Serving/201').checkedAt && store.serving('201').verdict === 'ready', 'a settling, changed or old verdict is replaced; a fresh one is left alone');
  check(sweepQueries.every(q => /^\s*SELECT\s/.test(q)) && (sweepQueries.length - 1) / 4 <= 16, 'the pass only reads, about 14 searches per campaign (' + ((sweepQueries.length - 1) / 4) + ')');
  store = fakeStore(); sweepQueries = []; E.__t.set({ fb: store.f, gaql: sweepGaql() });
  sweep = await cx.servingSweep({ limit: 2 });
  check(sweep.checked === 2 && sweep.deferred === 3 && store.docs.size === 2, 'at most the campaign limit is checked; the rest wait for the next day');
  store = fakeStore(); store.docs.set('Brites_GAds_Serving/201', { verdict: 'blocked', counts: { block: 1 }, top: [], source: 'daily', checkedAt: hours(8) });
  sweepQueries = []; E.__t.set({ fb: store.f, gaql: sweepGaql(quota) });
  sweep = await cx.servingSweep();
  check(sweep.quotaExhausted === true && sweep.checked === 1 && sweep.unknown === 1 && sweep.deferred === 4 && store.serving('201').verdict === 'blocked' && /quota/.test(store.serving('201').lastAttempt.error + ' ' + JSON.stringify(sweep)), 'Google\'s request quota stops the pass at once, and the kept verdict stays');
  sweepQueries = [];
  sweep = await cx.servingSweep({ budgetMs: -1 });
  check(sweep.skipped && sweepQueries.length === 0, 'with no time left in the run it reads nothing');

  // ── 11. Scheduled once a day, after the other daily and weekly work, within the worker's time ──
  const fixedDate = iso => { const at = typeof iso === 'function' ? iso : () => Date.parse(iso); return class extends Date { constructor(...a) { super(...(a.length ? a : [at()])); } static now() { return at(); } }; };
  const stub = n => n === './_editPasscode' ? { sameSecret: (a, b) => typeof a === 'string' && a === b, envPasscode: () => null, resolve: async () => ({ value: null }) } : null;
  const kicked = async iso => {
    const sent = [], mod = { exports: {} }, admin = { firestore: () => ({ collection: () => ({ doc: () => ({ get: async () => ({ exists: false }), set: async () => {} }) }) }) }; admin.firestore.FieldValue = { serverTimestamp: () => 0 };
    const fakeE = { COL: { state: 'state' }, control: async () => ({ enabled: true }) };
    const kx = { process: { env: { URL: 'https://example.invalid', GADS_DAILY_HOUR: '8', GADS_WEEKLY_DOW: '1', EDIT_PASSCODE: 'fixture-pass' } }, console, Date: fixedDate(iso), Set, JSON, module: mod, exports: mod.exports,
      require: n => n === 'node-fetch' ? async (url, o) => { sent.push(JSON.parse(o.body)); return { ok: true, status: 202 }; } : n === './googleAdsAutopilot' ? fakeE : n === './firebaseAdmin' ? admin : stub(n) || require(n) };
    vm.createContext(kx); vm.runInContext(fs.readFileSync(path.join(FN, 'googleAdsAutopilotKick.js'), 'utf8'), kx);
    await mod.exports.kick(); return sent[0].tasks;
  };
  let tasks = await kicked('2026-09-28T08:45:00Z');   // a Monday at the daily hour
  check(tasks.includes('serving') && tasks.indexOf('serving') > Math.max(tasks.indexOf('measure'), tasks.indexOf('designStudioLearn'), tasks.indexOf('budgets')) && tasks.indexOf('serving') < tasks.indexOf('bestSellers'), 'the daily kick asks for the serving pass after the daily and weekly tasks (' + tasks.join(',') + ')');
  check(!(await kicked('2026-09-28T09:45:00Z')).includes('serving'), 'an ordinary hourly kick does not');
  let clock = Date.parse('2026-09-28T08:45:00Z'), spent = 0;
  const worked = async () => {
    const calls = [], mod = { exports: {} }, env = { GADS_REFRESH_TOKEN: 'r', GADS_CLIENT_SECRET: 's', GADS_DEVELOPER_TOKEN: 'd' };
    const fakeE = { control: async () => { clock += spent; return { enabled: true }; }, servingSweep: async o => { calls.push(o); return { checked: 0 }; } };
    const wx = { process: { env }, console, Date: fixedDate(() => clock), Set, JSON, Math, module: mod, exports: mod.exports, setTimeout, clearTimeout,
      require: n => n === 'node-fetch' ? async () => ({ ok: true, status: 202 }) : n === './googleAdsAutopilot' ? fakeE : stub(n) || require(n) };
    vm.createContext(wx); vm.runInContext(fs.readFileSync(path.join(FN, 'googleAdsAutopilot-background.js'), 'utf8'), wx);
    const token = 'internal-' + require('crypto').createHmac('sha256', 'r|s|d').update('brites-gads-background-worker/v1').digest('hex');
    const res = await mod.exports.handler({ httpMethod: 'POST', headers: {}, body: JSON.stringify({ tasks: ['serving'], token }) });
    return { calls, out: JSON.parse(res.body) };
  };
  let w = await worked();
  check(w.out.status === 'ran' && w.calls.length === 1 && w.calls[0].budgetMs === 180000, 'the worker runs the serving pass with three minutes');
  spent = 11 * 60000; w = await worked();
  check(w.calls.length === 1 && w.calls[0].budgetMs <= 0, 'late in a run it leaves the worker\'s last three minutes to the tasks after it');

  console.log(`google-serving-audit: ${passed} checks passed`);
  require('./suite-guard.cjs').done();
})().catch(e => { console.error(e); process.exit(1); });
