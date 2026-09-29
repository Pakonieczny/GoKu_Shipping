'use strict';
// netlify/functions/_googleAdsServing.js
//
// "What Google has now" for one campaign. READ-ONLY.
//
// The console publishes campaigns paused. Before Paul enables one, this check reads
// what Google actually holds for it and says, in plain words, what would stop it
// serving the way he intends:
//   • Google's policy review, with Google's own reasons (ads, keywords, asset links,
//     asset groups, search themes) and every primary status reason;
//   • end dates, budget, bid strategy and its status;
//   • locations and the location option (people IN the countries, not people
//     interested in them), languages, networks (search partners, display expansion);
//   • the conversion goals Smart Bidding optimizes for (exactly one purchase action);
//   • for Performance Max: the Merchant Center link, feed label, product eligibility,
//     product filters, ad strength with Google's action items and asset coverage.
//
// Only GAQL searches are issued (the caller's gaql function); nothing here can change
// the account. Every read is isolated: a failing query becomes a warning in the result,
// never an error, so the campaign timeline that shows this can never break because of it.
//
// Levels:  block — Google will not serve it (or its core part) until this is fixed.
//          risk  — it serves, but not the way Paul intends, or it can waste money.
//          note  — worth knowing (under review, learning, weak ad strength).

const SMART_BIDDING = new Set(['MAXIMIZE_CONVERSIONS', 'MAXIMIZE_CONVERSION_VALUE', 'TARGET_CPA', 'TARGET_ROAS']);
const PRODUCT_LIMIT = 1000;
const COUNTRY = { '2840': 'United States', '2124': 'Canada', '2826': 'United Kingdom', '2036': 'Australia', '2554': 'New Zealand', '2372': 'Ireland' };
const FEED_COUNTRY = { US: '2840', CA: '2124', GB: '2826', UK: '2826', AU: '2036', NZ: '2554', IE: '2372' };
const LANGUAGE = { '1000': 'English', '1001': 'German', '1002': 'French', '1003': 'Spanish', '1004': 'Italian' };
const FIELD = { SITELINK: 'sitelink', CALLOUT: 'callout', STRUCTURED_SNIPPET: 'structured snippet', AD_IMAGE: 'image', HEADLINE: 'headline',
  LONG_HEADLINE: 'long headline', DESCRIPTION: 'description', BUSINESS_NAME: 'business name', LOGO: 'logo', LANDSCAPE_LOGO: 'landscape logo',
  MARKETING_IMAGE: 'landscape image', SQUARE_MARKETING_IMAGE: 'square image', PORTRAIT_MARKETING_IMAGE: 'portrait image', YOUTUBE_VIDEO: 'video',
  CALL_TO_ACTION_SELECTION: 'call to action' };
// Google's minimum per asset group when brand guidelines are off (developers.google.com/google-ads/api/performance-max/asset-requirements).
const PMAX_MINIMUM = { HEADLINE: 3, LONG_HEADLINE: 1, DESCRIPTION: 2, MARKETING_IMAGE: 1, SQUARE_MARKETING_IMAGE: 1, BUSINESS_NAME: 1, LOGO: 1 };
// Campaign primary status reasons -> [level, plain words]. CAMPAIGN_PAUSED/ENDED/REMOVED/PENDING are
// reported from the campaign's own status and dates instead, so they are not repeated here.
const CAMPAIGN_REASON = {
  CAMPAIGN_DRAFT: ['block', 'It is still a draft.'],
  NO_AD_GROUPS: ['block', 'It has no ad groups.'],
  AD_GROUPS_PAUSED: ['block', 'All of its ad groups are paused.'],
  NO_AD_GROUP_ADS: ['block', 'It has no ads.'],
  AD_GROUP_ADS_PAUSED: ['block', 'All of its ads are paused.'],
  NO_KEYWORDS: ['block', 'It has no keywords.'],
  KEYWORDS_PAUSED: ['block', 'All of its keywords are paused.'],
  NO_ASSET_GROUPS: ['block', 'It has no asset groups.'],
  ASSET_GROUPS_PAUSED: ['block', 'All of its asset groups are paused.'],
  BUDGET_MISCONFIGURED: ['block', 'Its budget is set up wrongly.'],
  BIDDING_STRATEGY_MISCONFIGURED: ['block', 'Its bid strategy is set up wrongly, often because no conversion action counts for bidding.'],
  MISSING_LOCATION_TARGETING: ['risk', 'It has no location targeting.'],
  HAS_ADS_DISAPPROVED: ['risk', 'Some ads are disapproved.'],
  HAS_ADS_LIMITED_BY_POLICY: ['risk', 'Some ads are limited by policy.'],
  HAS_ASSET_GROUPS_DISAPPROVED: ['risk', 'Some asset groups are disapproved.'],
  HAS_ASSET_GROUPS_LIMITED_BY_POLICY: ['risk', 'Some asset groups are limited by policy.'],
  BUDGET_CONSTRAINED: ['risk', 'The budget limits how often the ads show.'],
  BIDDING_STRATEGY_LIMITED: ['risk', 'The bid strategy is limited.'],
  BIDDING_STRATEGY_CONSTRAINED: ['risk', 'The bid strategy is held back by its targets.'],
  SEARCH_VOLUME_LIMITED: ['risk', 'Few people search for its keywords.'],
  BIDDING_STRATEGY_LEARNING: ['note', 'The bid strategy is learning.'],
  MOST_ADS_UNDER_REVIEW: ['note', 'Most ads are still under review.'],
  MOST_ASSET_GROUPS_UNDER_REVIEW: ['note', 'Most asset groups are still under review.']
};
const SYSTEM_STATUS = {
  MISCONFIGURED_ZERO_ELIGIBILITY: ['block', 'The bid strategy has no conversion action it is allowed to optimize for.'],
  MISCONFIGURED_CONVERSION_TYPES: ['risk', 'The bid strategy is set up with the wrong conversion types.'],
  MISCONFIGURED_CONVERSION_SETTINGS: ['risk', 'The bid strategy\'s conversion settings are set up wrongly.'],
  MISCONFIGURED_SHARED_BUDGET: ['risk', 'The bid strategy does not fit its shared budget.'],
  MISCONFIGURED_STRATEGY_TYPE: ['risk', 'The bid strategy type does not suit this campaign.'],
  LIMITED_BY_BUDGET: ['risk', 'The budget holds the bid strategy back.'],
  LIMITED_BY_DATA: ['note', 'The bid strategy does not have enough conversion data yet.'],
  LIMITED_BY_INVENTORY: ['note', 'The bid strategy is limited by available ad space.'],
  LIMITED_BY_LOW_QUALITY: ['note', 'The bid strategy is limited by low ad quality.'],
  LIMITED_BY_LOW_PRIORITY_SPEND: ['note', 'The bid strategy is limited by low-priority spend.'],
  LIMITED_BY_CPC_BID_CEILING: ['note', 'The bid strategy is limited by its maximum bid.'],
  LIMITED_BY_CPC_BID_FLOOR: ['note', 'The bid strategy is limited by its minimum bid.']
};
const LABEL = { campaign: 'Campaign settings', criteria: 'Locations, languages and schedule', goals: 'Campaign conversion goals', goalConfig: 'Campaign goal setup',
  actions: 'Conversion actions', customer: 'Account settings', lifecycle: 'New-customer goal', campaignAssets: 'Campaign assets and their review',
  adGroups: 'Ad groups', ads: 'Ads and their policy review', keywords: 'Keywords and their review', adGroupAssets: 'Ad group assets and their review',
  assetGroups: 'Asset groups and ad strength', assetGroupAssets: 'Asset group assets and their review', listingGroups: 'Product filters',
  signals: 'Search themes and audience signals', products: 'Products in this campaign', productLinks: 'Merchant Center link' };

const words = v => String(v == null ? '' : v).toLowerCase().replace(/_/g, ' ').trim();
const idOf = res => String(res || '').split('/').pop();
const dateOnly = s => { const m = String(s || '').match(/^(\d{4})-?(\d{2})-?(\d{2})/); return m ? `${m[1]}-${m[2]}-${m[3]}` : null; };
const dayDiff = (a, b) => Math.round((Date.parse(b + 'T00:00:00Z') - Date.parse(a + 'T00:00:00Z')) / 86400000);
const clip = (s, n) => { s = String(s == null ? '' : s).replace(/\s+/g, ' ').trim(); return s.length > n ? s.slice(0, n - 1) + '…' : s; };
const plural = (n, one, many) => n + ' ' + (n === 1 ? one : (many || one + 's'));
const listOf = (items, n = 4) => { const a = [...new Set(items.filter(Boolean))]; return a.slice(0, n).join(', ') + (a.length > n ? ` and ${a.length - n} more` : ''); };
const countryName = res => { const id = idOf(res); return COUNTRY[id] || (id ? 'location ' + id : ''); };
const languageName = res => { const id = idOf(res); return LANGUAGE[id] || (id ? 'language ' + id : ''); };
const fieldName = f => FIELD[f] || words(f);
const moneyText = (micros, currency) => micros == null || micros === '' || !isFinite(Number(micros)) ? null : `${currency ? currency + ' ' : ''}${(Number(micros) / 1e6).toFixed(2)}`;
const isPaused = s => s === 'PAUSED';
const OWNED_HOSTS = ['britesjewelry.com', 'www.britesjewelry.com'];
// The account's calendar date; end dates are stored in the account's time zone.
function localDate(timeZone, now = Date.now()) {
  try {
    const p = {};
    new Intl.DateTimeFormat('en-CA', { timeZone: timeZone || 'UTC', year: 'numeric', month: '2-digit', day: '2-digit' }).formatToParts(new Date(now)).forEach(x => { p[x.type] = x.value; });
    return `${p.year}-${p.month}-${p.day}`;
  } catch (e) { return new Date(now).toISOString().slice(0, 10); }
}

// Google's policy findings in plain words: the topic, where it applies, and the text Google quoted.
function policyText(summary) {
  const out = [];
  for (const e of (summary && summary.policyTopicEntries) || []) {
    if (!e || !e.topic || e.type === 'DESCRIPTIVE' || e.type === 'BROADENING') continue;
    let t = words(e.topic);
    const where = [];
    for (const c of e.constraints || []) for (const list of [c.countryConstraintList, c.certificateMissingInCountryList, c.certificateDomainMismatchInCountryList])
      for (const x of (list && list.countries) || []) where.push(countryName(x.countryCriterion));
    if (where.length) t += ' (' + listOf(where, 3) + ')';
    const quoted = (e.evidences || []).flatMap(v => (v.textList && v.textList.texts) || []).filter(Boolean).slice(0, 2);
    if (quoted.length) t += ': "' + quoted.map(q => clip(q, 50)).join('", "') + '"';
    out.push(t);
  }
  return listOf(out, 3);
}
function reasonWords(reasons) { return listOf((reasons || []).filter(r => r && r !== 'UNSPECIFIED' && r !== 'UNKNOWN').map(words), 4); }
function approvalOf(summary) { return (summary && summary.approvalStatus) || ''; }
function underReview(summary, reasons) {
  const r = summary && summary.reviewStatus;
  return r === 'REVIEW_IN_PROGRESS' || (reasons || []).some(x => /UNDER_REVIEW|PENDING_REVIEW/.test(x));
}

// ── Queries ──────────────────────────────────────────────────────────────────
// A list means: try the first; if Google rejects it (for example an older API
// version without a field), fall back to the next, reduced one.
function buildQueries({ customerId, campaignId, channel } = {}) {
  const id = String(campaignId || '').replace(/\D/g, ''), cid = String(customerId || '').replace(/\D/g, '');
  const campaignRes = `customers/${cid}/campaigns/${id}`, byCampaign = `campaign.id = ${id}`;
  const coreCampaign = `campaign.id, campaign.name, campaign.status, campaign.serving_status, campaign.primary_status, campaign.primary_status_reasons,
      campaign.advertising_channel_type, campaign.bidding_strategy_type, campaign.start_date_time, campaign.end_date_time,
      campaign.network_settings.target_google_search, campaign.network_settings.target_search_network, campaign.network_settings.target_content_network,
      campaign.geo_target_type_setting.positive_geo_target_type, campaign.shopping_setting.merchant_id, campaign.shopping_setting.feed_label,
      campaign.final_url_suffix, campaign.tracking_url_template, campaign_budget.amount_micros, campaign_budget.delivery_method, campaign_budget.explicitly_shared`;
  const q = {
    campaign: [
      `SELECT ${coreCampaign}, campaign.advertising_channel_sub_type, campaign.bidding_strategy_system_status, campaign.bidding_strategy,
      campaign.maximize_conversion_value.target_roas, campaign.maximize_conversions.target_cpa_micros, campaign.target_roas.target_roas,
      campaign.target_cpa.target_cpa_micros, campaign.manual_cpc.enhanced_cpc_enabled, campaign.network_settings.target_partner_search_network,
      campaign.geo_target_type_setting.negative_geo_target_type, campaign.contains_eu_political_advertising, campaign.missing_eu_political_advertising_declaration,
      campaign.asset_automation_settings, campaign.brand_guidelines_enabled, campaign.optimization_score, campaign_budget.status, campaign_budget.period,
      campaign_budget.total_amount_micros, campaign_budget.has_recommended_budget, campaign_budget.recommended_budget_amount_micros
      FROM campaign WHERE ${byCampaign}`,
      `SELECT ${coreCampaign} FROM campaign WHERE ${byCampaign}`
    ],
    criteria: `SELECT campaign.id, campaign_criterion.criterion_id, campaign_criterion.type, campaign_criterion.negative, campaign_criterion.status,
      campaign_criterion.location.geo_target_constant, campaign_criterion.language.language_constant, campaign_criterion.ad_schedule.day_of_week,
      campaign_criterion.ad_schedule.start_hour, campaign_criterion.ad_schedule.end_hour, campaign_criterion.keyword.text, campaign_criterion.proximity.radius
      FROM campaign_criterion WHERE ${byCampaign} AND campaign_criterion.status != 'REMOVED'`,
    goals: `SELECT campaign.id, campaign_conversion_goal.category, campaign_conversion_goal.origin, campaign_conversion_goal.biddable
      FROM campaign_conversion_goal WHERE ${byCampaign}`,
    goalConfig: `SELECT campaign.id, conversion_goal_campaign_config.goal_config_level, conversion_goal_campaign_config.custom_conversion_goal
      FROM conversion_goal_campaign_config WHERE ${byCampaign}`,
    actions: `SELECT conversion_action.id, conversion_action.name, conversion_action.status, conversion_action.type, conversion_action.category,
      conversion_action.origin, conversion_action.primary_for_goal FROM conversion_action WHERE conversion_action.status = 'ENABLED'`,
    customer: `SELECT customer.id, customer.status, customer.auto_tagging_enabled, customer.currency_code, customer.time_zone,
      customer.conversion_tracking_setting.conversion_tracking_status FROM customer LIMIT 1`,
    lifecycle: `SELECT campaign_lifecycle_goal.campaign, campaign_lifecycle_goal.customer_acquisition_goal_settings.optimization_mode
      FROM campaign_lifecycle_goal WHERE campaign_lifecycle_goal.campaign = '${campaignRes}'`,
    // campaign and ad_group are SEGMENTING resources of campaign_asset/ad_group_asset:
    // a campaign.id filter must also be selected, or Google rejects the query
    // (EXPECTED_REFERENCED_FIELD_IN_SELECT_CLAUSE).
    campaignAssets: `SELECT campaign.id, campaign_asset.resource_name, campaign_asset.field_type, campaign_asset.status, campaign_asset.source,
      campaign_asset.primary_status, campaign_asset.primary_status_reasons, asset.id, asset.type, asset.name, asset.policy_summary.approval_status,
      asset.policy_summary.review_status, asset.policy_summary.policy_topic_entries, asset.text_asset.text, asset.sitelink_asset.link_text,
      asset.callout_asset.callout_text FROM campaign_asset WHERE ${byCampaign} AND campaign_asset.status != 'REMOVED'`
  };
  if (channel && channel !== 'PERFORMANCE_MAX') {
    q.adGroups = `SELECT campaign.id, ad_group.id, ad_group.name, ad_group.status, ad_group.type, ad_group.primary_status, ad_group.primary_status_reasons,
      ad_group.cpc_bid_micros FROM ad_group WHERE ${byCampaign} AND ad_group.status != 'REMOVED'`;
    q.ads = `SELECT campaign.id, ad_group.id, ad_group.name, ad_group.status, ad_group_ad.ad.id, ad_group_ad.ad.type, ad_group_ad.status,
      ad_group_ad.primary_status, ad_group_ad.primary_status_reasons, ad_group_ad.policy_summary.approval_status, ad_group_ad.policy_summary.review_status,
      ad_group_ad.policy_summary.policy_topic_entries, ad_group_ad.ad_strength, ad_group_ad.action_items, ad_group_ad.ad.final_urls
      FROM ad_group_ad WHERE ${byCampaign} AND ad_group_ad.status != 'REMOVED' AND ad_group.status != 'REMOVED'`;
    q.adGroupAssets = `SELECT campaign.id, ad_group.id, ad_group_asset.resource_name, ad_group_asset.field_type, ad_group_asset.status,
      ad_group_asset.primary_status, ad_group_asset.primary_status_reasons, asset.id, asset.type, asset.name, asset.policy_summary.approval_status,
      asset.policy_summary.review_status, asset.policy_summary.policy_topic_entries, asset.text_asset.text, asset.sitelink_asset.link_text,
      asset.callout_asset.callout_text FROM ad_group_asset WHERE ${byCampaign} AND ad_group_asset.status != 'REMOVED'`;
  }
  if (channel === 'SEARCH') {
    q.keywords = `SELECT campaign.id, ad_group.id, ad_group.status, ad_group_criterion.criterion_id, ad_group_criterion.keyword.text,
      ad_group_criterion.keyword.match_type, ad_group_criterion.status, ad_group_criterion.approval_status, ad_group_criterion.disapproval_reasons,
      ad_group_criterion.system_serving_status, ad_group_criterion.primary_status, ad_group_criterion.primary_status_reasons,
      ad_group_criterion.quality_info.quality_score FROM ad_group_criterion WHERE ${byCampaign} AND ad_group_criterion.type = 'KEYWORD'
      AND ad_group_criterion.negative = FALSE AND ad_group_criterion.status != 'REMOVED' AND ad_group.status != 'REMOVED'`;
  }
  if (channel === 'PERFORMANCE_MAX') {
    q.assetGroups = [
      `SELECT campaign.id, asset_group.id, asset_group.resource_name, asset_group.name, asset_group.status, asset_group.primary_status,
      asset_group.primary_status_reasons, asset_group.ad_strength, asset_group.asset_coverage.ad_strength_action_items, asset_group.final_urls
      FROM asset_group WHERE ${byCampaign} AND asset_group.status != 'REMOVED'`,
      `SELECT campaign.id, asset_group.id, asset_group.resource_name, asset_group.name, asset_group.status, asset_group.primary_status,
      asset_group.primary_status_reasons, asset_group.ad_strength, asset_group.final_urls FROM asset_group WHERE ${byCampaign} AND asset_group.status != 'REMOVED'`
    ];
    q.assetGroupAssets = `SELECT campaign.id, asset_group_asset.asset_group, asset_group_asset.field_type, asset_group_asset.status, asset_group_asset.source,
      asset_group_asset.primary_status, asset_group_asset.primary_status_reasons, asset_group_asset.policy_summary.approval_status,
      asset_group_asset.policy_summary.review_status, asset_group_asset.policy_summary.policy_topic_entries, asset.id, asset.type, asset.text_asset.text
      FROM asset_group_asset WHERE ${byCampaign} AND asset_group_asset.status != 'REMOVED' AND asset_group.status != 'REMOVED'`;
    q.listingGroups = `SELECT campaign.id, asset_group_listing_group_filter.asset_group, asset_group_listing_group_filter.type,
      asset_group_listing_group_filter.listing_source, asset_group_listing_group_filter.case_value.product_item_id.value,
      asset_group_listing_group_filter.parent_listing_group_filter FROM asset_group_listing_group_filter WHERE ${byCampaign} AND asset_group.status != 'REMOVED'`;
    q.signals = `SELECT campaign.id, asset_group_signal.asset_group, asset_group_signal.approval_status, asset_group_signal.disapproval_reasons,
      asset_group_signal.search_theme.text, asset_group_signal.audience.audience FROM asset_group_signal WHERE ${byCampaign} AND asset_group.status != 'REMOVED'`;
    // Campaign scope: only the products this campaign includes, with their status FOR this campaign.
    q.products = `SELECT shopping_product.item_id, shopping_product.status, shopping_product.feed_label, shopping_product.issues
      FROM shopping_product WHERE shopping_product.campaign = '${campaignRes}' LIMIT ${PRODUCT_LIMIT}`;
    q.productLinks = `SELECT product_link.type, product_link.merchant_center.merchant_center_id FROM product_link WHERE product_link.type = 'MERCHANT_CENTER'`;
  }
  return q;
}

// ── Analysis (pure) ──────────────────────────────────────────────────────────
// raw: { key: rows } for every read that succeeded; a missing key means the read failed.
function analyze(raw, { today = null, shippingCountries = null, apiVersion = '', reduced = [], ownedHosts = OWNED_HOSTS } = {}) {
  const findings = [], facts = [], warnings = [], fewer = new Set(reduced || []);
  const add = (level, area, text, extra = {}) => findings.push({ level, area, text, ...(extra.reason ? { reason: extra.reason } : {}), ...(extra.fix ? { fix: extra.fix } : {}) });
  const fact = (label, value) => { if (value != null && value !== '') facts.push({ label, value: String(value) }); };
  const row = (raw.campaign || [])[0] || {}, c = row.campaign || null, budget = row.campaignBudget || {};
  if (!c) {
    return { ok: false, verdict: 'unknown', headline: 'Google\'s settings for this campaign could not be read.', findings, facts, warnings: ['Campaign settings could not be read, so nothing below can be judged.'], counts: { block: 0, risk: 0, note: 0 } };
  }
  const channel = String(c.advertisingChannelType || ''), pmax = channel === 'PERFORMANCE_MAX', search = channel === 'SEARCH';
  const customer = ((raw.customer || [])[0] || {}).customer || {}, currency = customer.currencyCode || '';
  today = today || localDate(customer.timeZone);
  const merchantId = c.shoppingSetting && c.shoppingSetting.merchantId ? String(c.shoppingSetting.merchantId) : '';
  const feedLabel = c.shoppingSetting && c.shoppingSetting.feedLabel ? String(c.shoppingSetting.feedLabel) : '';
  const retail = pmax && !!merchantId;
  const smart = SMART_BIDDING.has(String(c.biddingStrategyType || ''));
  const reasons = (c.primaryStatusReasons || []).map(String);

  // Status and dates.
  const endDate = dateOnly(c.endDateTime), startDate = dateOnly(c.startDateTime);
  fact('Status', words(c.status) + (c.primaryStatus ? ' · Google: ' + words(c.primaryStatus) : '') + (reasonWords(reasons) ? ' (' + reasonWords(reasons) + ')' : ''));
  fact('Dates', (startDate ? 'starts ' + startDate : 'no start date') + ' · ' + (endDate ? 'ends ' + endDate : 'no end date'));
  if (customer.status && customer.status !== 'ENABLED') add('block', 'Account', `The Google Ads account is ${words(customer.status)}. No campaign in it can serve.`, { fix: 'Resolve the account status in Google Ads.' });
  if (c.status === 'REMOVED') add('block', 'Status', 'The campaign is removed in Google Ads. It cannot serve again.');
  else if (isPaused(c.status)) add('note', 'Status', 'Paused. It spends nothing until you enable it.');
  if (endDate && today && endDate < today) add('block', 'Dates', `The end date (${endDate}) has passed. Enabling it will not make it serve.`, { fix: 'Set a later end date, or clear it, before enabling.' });
  else if (endDate && today && dayDiff(today, endDate) <= 7) add('risk', 'Dates', `It ends on ${endDate}, in ${plural(dayDiff(today, endDate), 'day')}.`, { fix: 'Extend the end date if it should keep running.' });
  if (startDate && today && startDate > today) add('note', 'Dates', `It starts on ${startDate}.`);
  if (c.servingStatus === 'SUSPENDED') add('block', 'Status', 'Google has suspended serving for this campaign.', { reason: words(c.servingStatus), fix: 'Check billing and account notifications in Google Ads.' });

  // Google's primary status reasons.
  let reasonBlocks = 0;
  const locationsMissingReported = reasons.includes('MISSING_LOCATION_TARGETING');
  for (const r of [...new Set(reasons)]) {
    const known = CAMPAIGN_REASON[r];
    // The location read reports a missing location itself, with the countries it found.
    if (known) { if (r === 'MISSING_LOCATION_TARGETING' && raw.criteria) continue; if (known[0] === 'block') reasonBlocks++; add(known[0], 'Google status', known[1], { reason: words(r) }); }
    else if (r === 'UNKNOWN') add('note', 'Google status', `Google gave a reason that this API version (${apiVersion || 'in use'}) cannot name.`, { fix: 'Open the campaign in Google Ads to read it.' });
    else if (!/^(CAMPAIGN_PAUSED|CAMPAIGN_ENDED|CAMPAIGN_REMOVED|CAMPAIGN_PENDING|UNSPECIFIED)$/.test(r)) add('note', 'Google status', 'Google reports: ' + words(r) + '.', { reason: words(r) });
  }
  if (['NOT_ELIGIBLE', 'MISCONFIGURED'].includes(c.primaryStatus) && !reasonBlocks && c.status !== 'REMOVED' && !(endDate && today && endDate < today))
    add('block', 'Google status', `Google says the campaign is ${words(c.primaryStatus)}.`, { reason: reasonWords(reasons) || words(c.primaryStatus), fix: 'Open the campaign in Google Ads to see what it needs.' });

  // Budget.
  const amount = budget.amountMicros != null ? Number(budget.amountMicros) : null;
  if (budget.period === 'CUSTOM_PERIOD') fact('Budget', (moneyText(budget.totalAmountMicros, currency) || '?') + ' for the whole campaign');
  else fact('Budget', amount != null ? moneyText(amount, currency) + ' a day' + (budget.explicitlyShared ? ' (shared)' : '') : null);
  if (row.campaignBudget && budget.period !== 'CUSTOM_PERIOD' && !(amount > 0)) add('block', 'Budget', 'The daily budget is zero or missing.', { fix: 'Set a daily budget.' });
  if (budget.deliveryMethod === 'ACCELERATED') add('note', 'Budget', 'Accelerated delivery can spend the day\'s budget early.');
  if (budget.explicitlyShared) add('note', 'Budget', 'This budget is shared, so other campaigns draw from it too.');
  if (budget.hasRecommendedBudget && Number(budget.recommendedBudgetAmountMicros) > amount) add('note', 'Budget', `Google suggests ${moneyText(budget.recommendedBudgetAmountMicros, currency)} a day.`);

  // Bidding.
  const bidType = String(c.biddingStrategyType || '');
  const targetRoas = (c.maximizeConversionValue && c.maximizeConversionValue.targetRoas) || (c.targetRoas && c.targetRoas.targetRoas);
  const targetCpa = (c.maximizeConversions && c.maximizeConversions.targetCpaMicros) || (c.targetCpa && c.targetCpa.targetCpaMicros);
  fact('Bidding', words(bidType) + (targetRoas ? ` · target ROAS ${Math.round(Number(targetRoas) * 100)}%` : '') + (targetCpa ? ` · target CPA ${moneyText(targetCpa, currency)}` : '')
    + (c.biddingStrategySystemStatus && !['ENABLED', 'UNSPECIFIED', 'UNKNOWN'].includes(c.biddingStrategySystemStatus) ? ` · ${words(c.biddingStrategySystemStatus)}` : '') + (c.biddingStrategy ? ' (portfolio strategy)' : ''));
  const sys = SYSTEM_STATUS[c.biddingStrategySystemStatus];
  if (sys) add(sys[0], 'Bidding', sys[1], { reason: words(c.biddingStrategySystemStatus) });
  else if (/^LEARNING_/.test(String(c.biddingStrategySystemStatus || '')) && !reasons.includes('BIDDING_STRATEGY_LEARNING')) add('note', 'Bidding', 'The bid strategy is learning.', { reason: words(c.biddingStrategySystemStatus) });
  if (bidType === 'MANUAL_CPC') add('note', 'Bidding', 'Manual CPC: you set every bid and Google does not use your sales to adjust them.');
  if (c.manualCpc && c.manualCpc.enhancedCpcEnabled) add('note', 'Bidding', 'Enhanced CPC is on; Google has retired it for Search, where it now behaves like manual CPC.');

  // Locations, location option, languages, schedule.
  const criteria = (raw.criteria || []).map(r => r.campaignCriterion).filter(Boolean);
  const positiveLocations = criteria.filter(x => x.type === 'LOCATION' && !x.negative && x.location);
  const proximity = criteria.filter(x => x.type === 'PROXIMITY' && !x.negative);
  const excluded = criteria.filter(x => x.type === 'LOCATION' && x.negative && x.location);
  const languages = criteria.filter(x => x.type === 'LANGUAGE' && !x.negative && x.language);
  const schedule = criteria.filter(x => x.type === 'AD_SCHEDULE');
  const negativeKeywords = criteria.filter(x => x.type === 'KEYWORD' && x.negative);
  const geoType = c.geoTargetTypeSetting && c.geoTargetTypeSetting.positiveGeoTargetType;
  if (raw.criteria) {
    const where = positiveLocations.map(x => countryName(x.location.geoTargetConstant));
    fact('Locations', (where.length ? listOf(where, 6) : proximity.length ? plural(proximity.length, 'radius target') : 'all countries')
      + (geoType ? (geoType === 'PRESENCE' ? ' · people in these places only' : ' · also people interested in them') : '')
      + (excluded.length ? ` · excludes ${listOf(excluded.map(x => countryName(x.location.geoTargetConstant)), 3)}` : ''));
    if (!positiveLocations.length && !proximity.length) add('risk', 'Locations', 'No locations are set, so Google can show the ads in every country, including ones you do not ship to.', { reason: locationsMissingReported ? 'missing location targeting' : undefined, fix: 'Add the countries you ship to.' });
    const shipping = new Set((shippingCountries || []).map(String));
    if (shipping.size) {
      const outside = positiveLocations.map(x => idOf(x.location.geoTargetConstant)).filter(id => /^2\d{3}$/.test(id) && !shipping.has(id));
      if (outside.length) add('risk', 'Locations', `It targets ${listOf(outside.map(id => COUNTRY[id] || 'location ' + id))}, outside your shipping countries.`, { fix: 'Remove locations you do not ship to.' });
    }
    if (retail && feedLabel && FEED_COUNTRY[feedLabel.toUpperCase()]) {
      const feedCountry = FEED_COUNTRY[feedLabel.toUpperCase()], others = positiveLocations.map(x => idOf(x.location.geoTargetConstant)).filter(id => /^2\d{3}$/.test(id) && id !== feedCountry);
      if (others.length) add('risk', 'Products', `The products come from the ${feedLabel.toUpperCase()} feed, but the campaign also targets ${listOf(others.map(id => COUNTRY[id] || 'location ' + id))}. Products listed for one country do not show as product ads in another.`, { fix: 'Target only the feed\'s country, or use a campaign per country feed.' });
    }
    fact('Languages', languages.length ? listOf(languages.map(x => languageName(x.language.languageConstant)), 5) : 'all languages');
    if (!languages.length) add('note', 'Languages', 'No language is set, so Google can show these English ads to people who use Google in any language.', { fix: 'Target English (and French for Canada only if you have French ads).' });
    fact('Schedule', schedule.length ? plural(schedule.length, 'time slot') : 'all day, every day');
    if (negativeKeywords.length) fact('Negative keywords', String(negativeKeywords.length) + ' at campaign level');
  }
  if (geoType && geoType !== 'PRESENCE') add('risk', 'Locations', 'Location option is "presence or interest": people outside your countries who show interest in them can see and click the ads.', { reason: words(geoType), fix: 'Set the location option to Presence (people in or regularly in your locations).' });
  if (!geoType && (search || pmax)) add('note', 'Locations', 'Google did not report the location option for this campaign.');

  // Landing pages: every final URL must be a valid https page on the store.
  const urls = [].concat(...(raw.ads || []).map(r => (((r.adGroupAd || {}).ad || {}).finalUrls) || []), ...(raw.assetGroups || []).map(r => (r.assetGroup || {}).finalUrls || []));
  if (urls.length) {
    const hosts = new Set(), owned = new Set((ownedHosts || []).map(h => String(h).toLowerCase()));
    let invalid = 0;
    for (const u of urls) { try { const x = new URL(u); hosts.add(x.hostname.toLowerCase()); if (x.protocol !== 'https:') invalid++; } catch (e) { invalid++; } }
    fact('Landing pages', `${plural(new Set(urls).size, 'page')} on ${listOf([...hosts], 3)}`);
    const foreign = owned.size ? [...hosts].filter(h => !owned.has(h)) : [];
    if (foreign.length) add('risk', 'Landing pages', `Some ads send people to ${listOf(foreign, 3)}, which is not your store.`, { fix: 'Point the final URLs at your store.' });
    if (invalid) add('risk', 'Landing pages', `${plural(invalid, 'final URL')} ${invalid === 1 ? 'is not a valid https address' : 'are not valid https addresses'}.`, { fix: 'Use full https links to your store.' });
  }

  // Networks (Search).
  const net = c.networkSettings || {};
  if (search) {
    const on = ['Google Search'].concat(net.targetSearchNetwork ? ['search partners'] : [], net.targetContentNetwork ? ['display expansion'] : []);
    fact('Networks', net.targetGoogleSearch === false ? 'not on Google Search' : on.join(' + '));
    if (net.targetGoogleSearch === false) add('block', 'Networks', 'The campaign is not set to show on Google Search.', { fix: 'Turn on Google Search in the campaign\'s networks.' });
    if (net.targetSearchNetwork) add('risk', 'Networks', 'Search partners is on: ads also show on partner sites, which usually sell less for the money.', { fix: 'Turn off search partners.' });
    if (net.targetContentNetwork) add('risk', 'Networks', 'Display expansion is on: part of the budget goes to display sites instead of searches.', { fix: 'Turn off the Display Network for this Search campaign.' });
  }

  // Conversion goals: what Smart Bidding optimizes for in THIS campaign.
  const config = ((raw.goalConfig || [])[0] || {}).conversionGoalCampaignConfig || {};
  if (raw.goals && raw.actions) {
    const biddable = new Set(raw.goals.map(r => r.campaignConversionGoal).filter(g => g && g.biddable === true).map(g => g.category + '|' + g.origin));
    const actions = raw.actions.map(r => r.conversionAction).filter(a => a && a.status === 'ENABLED' && a.primaryForGoal !== false);
    const counted = actions.filter(a => biddable.has(a.category + '|' + a.origin));
    const purchases = counted.filter(a => a.category === 'PURCHASE'), others = counted.filter(a => a.category !== 'PURCHASE');
    const name = a => `${clip(a.name, 40)} (${words(a.type)})`;
    fact('Conversion goals', config.customConversionGoal ? 'a custom goal' : counted.length ? listOf([...new Set(counted.map(a => words(a.category)))], 6) + ' · ' + plural(counted.length, 'action') : 'none counted');
    if (config.customConversionGoal) add('note', 'Goals', 'This campaign uses a custom conversion goal; check in Google Ads that it contains only your purchase action.');
    else if (!raw.goals.length) add('note', 'Goals', 'Google returned no conversion goals for this campaign, so what it optimizes for could not be checked.');
    else {
      const lvl = smart ? 'risk' : 'note';
      if (!counted.length) add(smart ? 'block' : 'note', 'Goals', smart ? 'No conversion action counts for bidding in this campaign, so Smart Bidding has nothing to optimize for.' : 'No conversion action counts in this campaign\'s Conversions column.', { fix: 'Make the Purchase goal biddable and keep one purchase action as Primary.' });
      else if (!purchases.length) add(lvl, 'Goals', `It optimizes for ${listOf(others.map(a => words(a.category)))} but not for purchases.`, { fix: 'Make the Purchase goal biddable for this campaign.' });
      if (purchases.length > 1) add(lvl, 'Goals', `${purchases.length} purchase actions count: ${listOf(purchases.map(name), 3)}. If they record the same sale, Google counts it twice${smart ? ' and bids too high' : ''}.`, { fix: 'In Google Ads Goals, keep one purchase action as Primary and set the others to Secondary.' });
      if (purchases.length && others.length) add(lvl, 'Goals', `It also optimizes for ${listOf([...new Set(others.map(a => words(a.category)))])}${smart ? ', so Google can chase these cheaper actions instead of sales' : ''}.`, { reason: listOf(others.map(name), 4), fix: 'Make only the Purchase goal biddable for this campaign, or set these actions to Secondary.' });
    }
  }
  const mode = (((raw.lifecycle || [])[0] || {}).campaignLifecycleGoal || {}).customerAcquisitionGoalSettings;
  if (raw.lifecycle) fact('New customers', mode && mode.optimizationMode === 'TARGET_NEW_CUSTOMER' ? 'bids for new customers only' : mode && mode.optimizationMode === 'BID_HIGHER_FOR_NEW_CUSTOMER' ? 'bids higher for new customers' : 'all customers equally');
  if (customer.autoTaggingEnabled === false) add('risk', 'Tracking', 'Auto-tagging is off, so clicks carry no Google click ID and uploaded sales cannot be matched to ads.', { fix: 'Turn on auto-tagging in the account settings.' });
  const tracking = customer.conversionTrackingSetting && customer.conversionTrackingSetting.conversionTrackingStatus;
  if (tracking === 'NOT_CONVERSION_TRACKED') add('risk', 'Tracking', 'Google says this account does not track conversions.', { fix: 'Check the purchase conversion action in Google Ads.' });
  fact('Tracking', [customer.autoTaggingEnabled === true ? 'auto-tagging on' : customer.autoTaggingEnabled === false ? 'auto-tagging OFF' : null,
    c.finalUrlSuffix ? 'final URL suffix set' : 'no final URL suffix', c.trackingUrlTemplate ? 'tracking template set' : null].filter(Boolean).join(' · '));
  const eu = c.containsEuPoliticalAdvertising;
  if (c.missingEuPoliticalAdvertisingDeclaration === true || (!fewer.has('campaign') && eu !== undefined && !/^(DOES_NOT_CONTAIN|CONTAINS)_EU_POLITICAL_ADVERTISING$/.test(String(eu))))
    add('risk', 'Settings', 'Google marks the EU political ads declaration as missing.', { fix: 'Declare that the campaign does not contain EU political ads.' });
  else if (eu === 'CONTAINS_EU_POLITICAL_ADVERTISING') add('risk', 'Settings', 'The campaign is declared as containing EU political ads, which Google restricts.', { fix: 'Declare that the campaign does not contain EU political ads.' });
  if (c.optimizationScore != null) fact('Optimization score', Math.round(Number(c.optimizationScore) * 100) + '%');

  // Assets linked to the campaign (sitelinks, callouts, snippets, images, logo, business name).
  const linkCheck = (rows, linkKey, where) => {
    const links = (rows || []).map(r => ({ link: r[linkKey] || {}, asset: r.asset || {}, group: r.adGroup || null })).filter(x => x.link.fieldType);
    const label = x => { const a = x.asset; const text = (a.sitelinkAsset && a.sitelinkAsset.linkText) || (a.calloutAsset && a.calloutAsset.calloutText) || (a.textAsset && a.textAsset.text) || a.name; return fieldName(x.link.fieldType) + (text ? ` "${clip(text, 30)}"` : ''); };
    const summaryOf = x => (x.link.policySummary && x.link.policySummary.approvalStatus ? x.link.policySummary : x.asset.policySummary) || {};
    const bad = links.filter(x => approvalOf(summaryOf(x)) === 'DISAPPROVED' || (x.link.primaryStatusReasons || []).includes('ASSET_DISAPPROVED'));
    const limited = links.filter(x => ['APPROVED_LIMITED', 'AREA_OF_INTEREST_ONLY'].includes(approvalOf(summaryOf(x))));
    const reviewing = links.filter(x => !bad.includes(x) && underReview(summaryOf(x), x.link.primaryStatusReasons));
    if (bad.length) add('risk', 'Assets', `${plural(bad.length, where + ' asset')} disapproved: ${listOf(bad.map(label), 3)}.`, { reason: listOf(bad.map(x => policyText(summaryOf(x))).filter(Boolean), 3) || 'disapproved', fix: 'Edit or replace the disapproved assets; they will not show.' });
    if (limited.length) add('note', 'Assets', `${plural(limited.length, where + ' asset')} limited by policy: ${listOf(limited.map(label), 3)}.`, { reason: listOf(limited.map(x => policyText(summaryOf(x))).filter(Boolean), 3) || undefined });
    if (reviewing.length) add('note', 'Assets', `${plural(reviewing.length, where + ' asset')} still under review.`);
    return links;
  };
  const campaignLinks = raw.campaignAssets ? linkCheck(raw.campaignAssets, 'campaignAsset', 'campaign') : [];
  const groupLinks = raw.adGroupAssets ? linkCheck(raw.adGroupAssets, 'adGroupAsset', 'ad group') : [];
  if (raw.campaignAssets) {
    const n = f => campaignLinks.filter(x => x.link.fieldType === f && x.link.status !== 'PAUSED').length + groupLinks.filter(x => x.link.fieldType === f && x.link.status !== 'PAUSED').length;
    fact('Extensions', `${plural(n('SITELINK'), 'sitelink')} · ${plural(n('CALLOUT'), 'callout')} · ${plural(n('STRUCTURED_SNIPPET'), 'snippet')}` + (search ? ` · ${plural(n('AD_IMAGE'), 'image')}` : '')
      + ` · business name ${n('BUSINESS_NAME') ? 'yes' : 'none'} · logo ${n('LOGO') ? 'yes' : 'none'} (${search ? 'campaign and ad group level' : 'campaign level'})`);
    if (search && !n('SITELINK')) add('note', 'Assets', 'No sitelinks on this campaign or its ad groups (unless the account adds them). Google recommends at least four.');
  }

  // Search and Display: ad groups, ads (policy review) and keywords.
  if (!pmax && raw.adGroups) {
    const groups = raw.adGroups.map(r => r.adGroup).filter(Boolean), enabledGroups = groups.filter(g => g.status === 'ENABLED');
    const ads = (raw.ads || []).map(r => ({ ad: r.adGroupAd || {}, group: r.adGroup || {} }));
    const keywords = (raw.keywords || []).map(r => ({ kw: r.adGroupCriterion || {}, group: r.adGroup || {} }));
    if (!enabledGroups.length && !reasons.some(r => /^(NO_AD_GROUPS|AD_GROUPS_PAUSED)$/.test(r))) add('block', 'Ad groups', groups.length ? 'Every ad group is paused.' : 'It has no ad groups.', { fix: 'Enable at least one ad group with ads and keywords.' });
    const enabledAds = ads.filter(x => x.ad.status === 'ENABLED' && x.group.status === 'ENABLED');
    fact('Ads', raw.ads ? `${enabledAds.length} enabled in ${plural(enabledGroups.length, 'enabled ad group')}` : null);
    if (raw.ads) {
      for (const g of enabledGroups) if (!enabledAds.some(x => String(x.group.id) === String(g.id))) add('risk', 'Ads', `Ad group "${clip(g.name, 40)}" has no enabled ad, so it cannot show.`, { fix: 'Enable or add an ad in this ad group.' });
      const dis = enabledAds.filter(x => approvalOf(x.ad.policySummary) === 'DISAPPROVED');
      const lim = enabledAds.filter(x => ['APPROVED_LIMITED', 'AREA_OF_INTEREST_ONLY'].includes(approvalOf(x.ad.policySummary)));
      const rev = enabledAds.filter(x => !dis.includes(x) && underReview(x.ad.policySummary, x.ad.primaryStatusReasons));
      const topics = list => listOf(list.map(x => policyText(x.ad.policySummary)).filter(Boolean), 3);
      if (enabledAds.length && dis.length === enabledAds.length) add('block', 'Ads', 'Every enabled ad is disapproved, so Google cannot show this campaign.', { reason: topics(dis) || 'disapproved', fix: 'Fix the ad text or landing page Google flagged, then resubmit.' });
      else if (dis.length) add('risk', 'Ads', `${plural(dis.length, 'ad')} disapproved in ${listOf(dis.map(x => '"' + clip(x.group.name, 30) + '"'), 3)}.`, { reason: topics(dis) || 'disapproved', fix: 'Fix the ad text or landing page Google flagged, then resubmit.' });
      if (lim.length) add('risk', 'Ads', `${plural(lim.length, 'ad')} limited by policy, so they show in fewer places.`, { reason: topics(lim) || undefined });
      if (rev.length) add('note', 'Ads', `${plural(rev.length, 'ad')} still under Google review.`);
      const poor = enabledAds.filter(x => x.ad.adStrength === 'POOR');
      if (poor.length) add('note', 'Ads', `${plural(poor.length, 'ad')} with Poor ad strength.`, { fix: listOf(poor.flatMap(x => x.ad.actionItems || []).map(s => clip(s, 90)), 2) || 'Add more varied headlines and descriptions.' });
    }
    if (search && raw.keywords) {
      const live = keywords.filter(x => x.kw.status === 'ENABLED' && x.group.status === 'ENABLED');
      const text = x => `"${clip((x.kw.keyword || {}).text, 30)}"`;
      fact('Keywords', `${live.length} enabled`);
      for (const g of enabledGroups) if (!live.some(x => String(x.group.id) === String(g.id))) add('risk', 'Keywords', `Ad group "${clip(g.name, 40)}" has no enabled keyword, so it cannot show.`, { fix: 'Enable or add keywords in this ad group.' });
      const dis = live.filter(x => x.kw.approvalStatus === 'DISAPPROVED' || (x.kw.primaryStatusReasons || []).includes('AD_GROUP_CRITERION_DISAPPROVED'));
      const rare = live.filter(x => x.kw.systemServingStatus === 'RARELY_SERVED' || (x.kw.primaryStatusReasons || []).includes('AD_GROUP_CRITERION_RARELY_SERVED'));
      const low = live.filter(x => (x.kw.primaryStatusReasons || []).includes('AD_GROUP_CRITERION_BELOW_FIRST_PAGE_BID'));
      if (dis.length) add('risk', 'Keywords', `${plural(dis.length, 'keyword')} disapproved: ${listOf(dis.map(text), 4)}.`, { reason: listOf(dis.flatMap(x => x.kw.disapprovalReasons || []).map(words), 3) || 'disapproved' });
      if (live.length && rare.length === live.length) add('risk', 'Keywords', 'Every keyword is rarely shown because few people search for them.', { fix: 'Add broader or more common keywords.' });
      else if (rare.length) add('note', 'Keywords', `${plural(rare.length, 'keyword')} rarely shown (low search volume): ${listOf(rare.map(text), 4)}.`);
      if (low.length) add('risk', 'Keywords', `${plural(low.length, 'keyword')} bid below Google's first-page estimate: ${listOf(low.map(text), 4)}.`, { fix: 'Raise those bids, or they will seldom show.' });
    }
  }

  // Performance Max: asset groups, assets, product filters, signals, products, Merchant link.
  if (pmax) {
    fact('Merchant Center', merchantId ? merchantId + (feedLabel ? ' · feed ' + feedLabel : ' · every feed label') : 'not linked to this campaign');
    const automation = {}; for (const s of c.assetAutomationSettings || []) automation[s.assetAutomationType] = s.assetAutomationStatus;
    const onOff = t => automation[t] ? (automation[t] === 'OPTED_IN' ? 'on' : 'off') : 'on (Google default)';
    // An empty list from the full read means every automation is at Google's default.
    if (!fewer.has('campaign')) {
      fact('Google automation', `final URL expansion ${onOff('FINAL_URL_EXPANSION_TEXT_ASSET_AUTOMATION')} · Google-written text ${onOff('TEXT_ASSET_AUTOMATION')} · image enhancement ${onOff('GENERATE_IMAGE_ENHANCEMENT')} · video enhancement ${onOff('GENERATE_ENHANCED_YOUTUBE_VIDEOS')}`);
      if (onOff('FINAL_URL_EXPANSION_TEXT_ASSET_AUTOMATION') !== 'off') add('note', 'Settings', 'Final URL expansion is on: Google may send people to other pages of your site than the ones you chose.');
    }
    if (c.brandGuidelinesEnabled) fact('Brand guidelines', 'on (business name and logo live on the campaign)');
    const groups = (raw.assetGroups || []).map(r => r.assetGroup).filter(Boolean);
    if (raw.assetGroups && !groups.length && !reasons.includes('NO_ASSET_GROUPS')) add('block', 'Asset groups', 'It has no asset groups.', { fix: 'Add an asset group.' });
    const links = (raw.assetGroupAssets || []).map(r => ({ link: r.assetGroupAsset || {}, asset: r.asset || {} })).filter(x => x.link.fieldType);
    const filters = (raw.listingGroups || []).map(r => r.assetGroupListingGroupFilter).filter(Boolean);
    const signals = (raw.signals || []).map(r => r.assetGroupSignal).filter(Boolean);
    const brandLinks = new Set(campaignLinks.filter(x => x.link.status !== 'PAUSED').map(x => x.link.fieldType));
    const strengths = [], includedIds = new Set();
    let groupsWithoutFilter = 0;
    for (const g of groups) {
      const name = `"${clip(g.name, 40)}"`, own = links.filter(x => x.link.assetGroup === g.resourceName && x.link.status !== 'PAUSED');
      strengths.push(clip(g.name, 30) + ': ' + words(g.adStrength || 'unknown'));
      const gr = (g.primaryStatusReasons || []).map(String);
      if (gr.includes('ASSET_GROUP_DISAPPROVED')) add(groups.length === 1 ? 'block' : 'risk', 'Asset groups', `Asset group ${name} is disapproved.`, { reason: listOf(own.map(x => policyText(x.link.policySummary)).filter(Boolean), 3) || 'asset group disapproved', fix: 'Replace the disapproved assets in this asset group.' });
      if (gr.includes('ASSET_GROUP_LIMITED')) add('risk', 'Asset groups', `Asset group ${name} is limited by policy.`, { reason: listOf(own.map(x => policyText(x.link.policySummary)).filter(Boolean), 3) || undefined });
      if (gr.includes('ASSET_GROUP_UNDER_REVIEW')) add('note', 'Asset groups', `Asset group ${name} is still under review.`);
      if (g.status === 'PAUSED') add('note', 'Asset groups', `Asset group ${name} is paused.`);
      const items = ((g.assetCoverage || {}).adStrengthActionItems || []).map(i => i.addAssetDetails).filter(Boolean)
        .map(d => `add ${d.assetCount ? d.assetCount + ' ' : ''}${fieldName(d.assetFieldType)}${Number(d.assetCount) > 1 ? 's' : ''}${d.videoAspectRatioRequirement && !/UNSPECIFIED|UNKNOWN/.test(d.videoAspectRatioRequirement) ? ' (' + words(d.videoAspectRatioRequirement) + ')' : ''}`);
      if (g.adStrength === 'POOR') add('risk', 'Ad strength', `Asset group ${name} has Poor ad strength, so Google builds fewer and weaker ads from it.`, { fix: items.length ? 'Google asks: ' + listOf(items, 4) + '.' : 'Add more headlines, descriptions, images and a video.' });
      else if (g.adStrength === 'AVERAGE') add('note', 'Ad strength', `Asset group ${name} has Average ad strength.`, { fix: items.length ? 'Google asks: ' + listOf(items, 4) + '.' : undefined });
      if (own.length) {
        const short = Object.entries(PMAX_MINIMUM).filter(([f]) => !(c.brandGuidelinesEnabled && /^(BUSINESS_NAME|LOGO)$/.test(f)))
          .map(([f, min]) => [f, min, own.filter(x => x.link.fieldType === f).length]).filter(([, min, has]) => has < min);
        // Retail groups may run below the minimum (Google fills gaps from the feed), but Google
        // applies the full minimum to any later change of this group's assets.
        const need = listOf(short.map(([f, min, has]) => `${min} ${fieldName(f)}${min > 1 ? 's' : ''} (has ${has})`), 4);
        if (short.length) add(retail ? 'note' : 'block', 'Assets', retail ? `Asset group ${name} has fewer assets than Google's minimum: ${need}. Google fills the gaps from the product feed, but requires the full minimum whenever this group's assets are changed.` : `Asset group ${name} is below Google's minimum: ${need}.`, { fix: 'Add the missing assets.' });
      } else if (retail) add('note', 'Assets', `Asset group ${name} has no text or images of its own, so Google builds its ads from the product feed only.`);
      const dis = own.filter(x => approvalOf(x.link.policySummary) === 'DISAPPROVED' || (x.link.primaryStatusReasons || []).includes('ASSET_DISAPPROVED'));
      const lim = own.filter(x => ['APPROVED_LIMITED', 'AREA_OF_INTEREST_ONLY'].includes(approvalOf(x.link.policySummary)));
      const rev = own.filter(x => !dis.includes(x) && underReview(x.link.policySummary, x.link.primaryStatusReasons));
      const label = x => fieldName(x.link.fieldType) + (x.asset.textAsset && x.asset.textAsset.text ? ` "${clip(x.asset.textAsset.text, 30)}"` : '');
      if (dis.length) add('risk', 'Assets', `${plural(dis.length, 'asset')} disapproved in ${name}: ${listOf(dis.map(label), 3)}.`, { reason: listOf(dis.map(x => policyText(x.link.policySummary)).filter(Boolean), 3) || 'disapproved', fix: 'Replace the disapproved assets; they will not show.' });
      if (lim.length) add('note', 'Assets', `${plural(lim.length, 'asset')} limited by policy in ${name}: ${listOf(lim.map(label), 3)}.`, { reason: listOf(lim.map(x => policyText(x.link.policySummary)).filter(Boolean), 3) || undefined });
      if (rev.length) add('note', 'Assets', `${plural(rev.length, 'asset')} in ${name} still under review.`);
      if (raw.listingGroups && retail) {
        const mine = filters.filter(f => f.assetGroup === g.resourceName);
        const included = mine.filter(f => f.type === 'UNIT_INCLUDED'), excludedAll = mine.length && !included.length;
        included.forEach(f => { const v = f.caseValue && f.caseValue.productItemId && f.caseValue.productItemId.value; if (v) includedIds.add(String(v).toLowerCase()); });
        if (!mine.length) groupsWithoutFilter++;
        else if (excludedAll) add('risk', 'Products', `Asset group ${name} excludes every product.`, { fix: 'Include the products this asset group should sell.' });
      }
      const themes = signals.filter(s => s.assetGroup === g.resourceName && s.searchTheme), audiences = signals.filter(s => s.assetGroup === g.resourceName && s.audience);
      const badThemes = themes.filter(s => s.approvalStatus === 'DISAPPROVED');
      if (badThemes.length) add('note', 'Signals', `${plural(badThemes.length, 'search theme')} disapproved in ${name}: ${listOf(badThemes.map(s => '"' + clip(s.searchTheme.text, 30) + '"'), 3)}.`, { reason: listOf(badThemes.flatMap(s => s.disapprovalReasons || []).map(words), 3) || undefined });
      if (raw.signals) fact('Signals ' + clip(g.name, 24), `${plural(themes.length, 'search theme')} · ${audiences.length ? 'audience signal' : 'no audience signal'}`);
    }
    if (strengths.length) fact('Ad strength', strengths.join(' · '));
    if (c.brandGuidelinesEnabled && raw.campaignAssets) for (const f of ['BUSINESS_NAME', 'LOGO']) if (!brandLinks.has(f)) add('risk', 'Assets', `Brand guidelines are on but the campaign has no ${fieldName(f)}.`, { fix: `Add a ${fieldName(f)} to the campaign.` });
    if (retail && raw.listingGroups && groupsWithoutFilter) add(groupsWithoutFilter === groups.length ? 'block' : 'risk', 'Products', `${plural(groupsWithoutFilter, 'asset group')} ${groupsWithoutFilter === 1 ? 'has' : 'have'} no product filter; Google requires one in every asset group of a retail campaign.`, { fix: 'Add a product filter (for example "all products").' });

    // Products, for this campaign.
    if (retail && raw.products) {
      const products = raw.products.map(r => r.shoppingProduct).filter(Boolean), truncated = products.length >= PRODUCT_LIMIT;
      const pauseIssue = i => /paus/i.test(`${i.errorCode || ''} ${i.description || ''} ${i.detail || ''}`);
      const blocking = p => (p.issues || []).filter(i => !(isPaused(c.status) && pauseIssue(i)) && i.adsSeverity !== 'WARNING');
      const notEligible = products.filter(p => p.status === 'NOT_ELIGIBLE' && (!isPaused(c.status) || blocking(p).length));
      const limited = products.filter(p => p.status === 'ELIGIBLE_LIMITED');
      const topIssues = list => { const n = new Map(); list.forEach(p => new Set((p.issues || []).filter(i => !(isPaused(c.status) && pauseIssue(i))).map(i => clip(i.description || words(i.errorCode), 60))).forEach(d => n.set(d, (n.get(d) || 0) + 1)));
        return [...n].sort((a, b) => b[1] - a[1]).slice(0, 3).map(([d, k]) => `${d} (${k})`).join('; '); };
      const pauseOnly = isPaused(c.status) && products.some(p => p.status === 'NOT_ELIGIBLE' && !blocking(p).length);
      fact('Products', products.length ? `${truncated ? PRODUCT_LIMIT + '+' : products.length} in this campaign · ${products.length - notEligible.length - limited.length} eligible${pauseOnly ? ' once enabled' : ''}` + (limited.length ? ` · ${limited.length} limited` : '') + (notEligible.length ? ` · ${notEligible.length} not eligible` : '') : 'none found');
      if (!products.length) add('block', 'Products', `Google finds no products for this campaign (Merchant Center ${merchantId}${feedLabel ? ', feed ' + feedLabel : ''}), so it cannot show product ads.`, { fix: 'Check the feed label and that the products are approved in Merchant Center.' });
      else if (notEligible.length === products.length) add('block', 'Products', `None of the ${products.length} products can show.`, { reason: topIssues(notEligible) || 'not eligible', fix: 'Fix the product issues in Merchant Center.' });
      else if (notEligible.length) add('risk', 'Products', `${notEligible.length} of ${products.length} products cannot show.`, { reason: topIssues(notEligible) || 'not eligible', fix: 'Fix the product issues in Merchant Center.' });
      if (limited.length) add('note', 'Products', `${plural(limited.length, 'product')} can show only in some places.`, { reason: topIssues(limited) || undefined });
      if (pauseOnly && !notEligible.length) add('note', 'Products', 'Google lists the products as not eligible only because the campaign is paused.');
      if (!truncated && includedIds.size) {
        const found = new Set(products.map(p => String(p.itemId || '').toLowerCase())), missing = [...includedIds].filter(id => !found.has(id));
        if (missing.length) add(missing.length === includedIds.size ? 'block' : 'risk', 'Products', `${missing.length} of ${includedIds.size} products in the product filter are not in Merchant Center under this campaign's feed: ${listOf(missing, 3)}.`, { fix: 'Remove them from the filter, or fix the feed label.' });
      }
      if (!products.length && raw.productLinks) {
        const linked = raw.productLinks.map(r => r.productLink && r.productLink.merchantCenter && String(r.productLink.merchantCenter.merchantCenterId)).filter(Boolean);
        if (!linked.includes(merchantId)) add('block', 'Merchant Center', `Merchant Center ${merchantId} is not linked to this Google Ads account${linked.length ? ' (linked: ' + listOf(linked, 3) + ')' : ''}.`, { fix: 'Link Merchant Center to Google Ads, or pick the linked account.' });
      }
    }
    if (pmax && !merchantId) add('note', 'Merchant Center', 'No Merchant Center account is set, so this campaign cannot show your products.');
  }

  // Result.
  const order = { block: 0, risk: 1, note: 2 };
  findings.sort((a, b) => order[a.level] - order[b.level]);
  const counts = { block: 0, risk: 0, note: 0 }; findings.forEach(f => { counts[f.level]++; });
  const verdict = counts.block ? 'blocked' : counts.risk ? 'attention' : 'ready';
  const headline = counts.block ? 'Will not serve as intended: ' + findings[0].text
    : counts.risk ? `${plural(counts.risk, 'setting')} to fix before enabling.`
      : 'Google reports nothing that would stop it serving as intended.';
  return { ok: true, verdict, headline, counts, findings, facts, warnings };
}

// ── Reads (the only I/O) ─────────────────────────────────────────────────────
function shortError(e) {
  const m = String((e && e.message) || e || 'unknown error');
  const codes = [...new Set(m.match(/\b[a-z][A-Za-z]*Error=[A-Z][A-Z0-9_]*\b/g) || [])].slice(0, 3);
  return codes.length ? 'Google Ads: ' + codes.join('; ') : clip(m.replace(/^\[gads\]\s*/, ''), 160);
}
function withDeadline(promise, at) {
  let timer;
  const late = new Promise((_, reject) => { timer = setTimeout(() => reject(Object.assign(new Error('Google did not answer in time.'), { code: 'SERVING_DEADLINE' })), Math.max(0, at - Date.now())); });
  return Promise.race([promise, late]).finally(() => clearTimeout(timer));
}
// today and shippingCountries may be values or promises; they are awaited only after the reads.
async function auditCampaign({ gaql, customerId, campaignId, channel = null, today = null, shippingCountries = null, apiVersion = '', deadlineMs = 8000, isQuotaError = null, ownedHosts = OWNED_HOSTS } = {}) {
  const id = String(campaignId || '').replace(/\D/g, '');
  if (!id || typeof gaql !== 'function') return { ok: false, error: 'A campaign ID and a Google Ads reader are required.' };
  const raw = {}, readWarnings = [], reduced = [], at = Date.now() + deadlineMs;
  let quotaHit = false;
  const read = async (key, variants) => {
    variants = Array.isArray(variants) ? variants : [variants];
    let first = null;
    for (let i = 0; i < variants.length && !quotaHit; i++) {
      try {
        raw[key] = await withDeadline(Promise.resolve().then(() => gaql(variants[i])), at);
        if (i) { reduced.push(key); readWarnings.push(`${LABEL[key] || key}: read with fewer details (${shortError(first)}).`); }
        return;
      } catch (e) { first = first || e; if (e && e.code === 'SERVING_DEADLINE') break; if (isQuotaError && isQuotaError(e)) { quotaHit = true; break; } }
    }
    readWarnings.push(`${LABEL[key] || key} could not be read (${quotaHit ? 'Google Ads request quota is exhausted' : shortError(first)}).`);
  };
  // Every channel-independent read starts at once; the channel's own reads start the moment the
  // campaign row names the channel (or at once when the caller already knows it).
  const common = buildQueries({ customerId, campaignId: id }), jobs = Object.keys(common).map(key => read(key, common[key]));
  const channelReads = ch => { const all = buildQueries({ customerId, campaignId: id, channel: ch }); return Promise.all(Object.keys(all).filter(k => !(k in common)).map(k => read(k, all[k]))); };
  const channelOf = () => (((raw.campaign || [])[0] || {}).campaign || {}).advertisingChannelType || null;
  jobs.push(channel ? channelReads(channel) : jobs[0].then(() => channelOf() && channelReads(channelOf())));
  await Promise.all(jobs);
  const [todayValue, shipping] = await Promise.all([Promise.resolve(today).catch(() => null), Promise.resolve(shippingCountries).catch(() => null)]);
  let result;
  try { result = analyze(raw, { today: todayValue, shippingCountries: shipping, apiVersion, reduced, ownedHosts }); }
  catch (e) { result = { ok: false, verdict: 'unknown', headline: 'The serving check could not be completed.', findings: [], facts: [], warnings: ['Analysis failed: ' + clip(e && e.message, 160)], counts: { block: 0, risk: 0, note: 0 } }; }
  return { ...result, warnings: readWarnings.concat(result.warnings || []), partial: readWarnings.length > 0, campaignId: id, channel: channelOf() || channel || null,
    apiVersion: apiVersion || null, checkedAt: new Date().toISOString(), readOnly: true };
}

module.exports = { buildQueries, analyze, auditCampaign, policyText, localDate, PRODUCT_LIMIT, PMAX_MINIMUM, OWNED_HOSTS };
