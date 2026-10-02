'use strict';

// Read-only Google Ads v24 conversion-goal evidence. This module accepts a
// paged GAQL reader, never a mutation/upload client, and stores nothing.
// Field references checked 2026-10-02:
// https://developers.google.com/google-ads/api/fields/v24/conversion_action
// https://developers.google.com/google-ads/api/fields/v24/customer_conversion_goal
// https://developers.google.com/google-ads/api/fields/v24/campaign_conversion_goal
// https://developers.google.com/google-ads/api/fields/v24/conversion_goal_campaign_config
// https://developers.google.com/google-ads/api/fields/v24/custom_conversion_goal
// https://developers.google.com/google-ads/api/docs/conversions/goals/campaign-goals
// Standard goals match BOTH category and origin and require primary_for_goal.
// Custom goals name exact action resources and can include secondary actions.
// These settings do not prove an upload succeeded, an order was attributed, or
// two actions recorded the same order. No role-switch advice is generated.

const QUERIES = Object.freeze({
  campaigns: "SELECT campaign.resource_name, campaign.id, campaign.name, campaign.status, campaign.bidding_strategy_type FROM campaign WHERE campaign.status = 'ENABLED'",
  actions: 'SELECT conversion_action.resource_name, conversion_action.id, conversion_action.name, conversion_action.status, conversion_action.category, conversion_action.origin, conversion_action.primary_for_goal FROM conversion_action',
  customerGoals: 'SELECT customer_conversion_goal.resource_name, customer_conversion_goal.category, customer_conversion_goal.origin, customer_conversion_goal.biddable FROM customer_conversion_goal',
  campaignGoals: "SELECT campaign_conversion_goal.campaign, campaign_conversion_goal.category, campaign_conversion_goal.origin, campaign_conversion_goal.biddable FROM campaign_conversion_goal WHERE campaign.status = 'ENABLED'",
  campaignConfigs: "SELECT conversion_goal_campaign_config.campaign, conversion_goal_campaign_config.goal_config_level, conversion_goal_campaign_config.custom_conversion_goal FROM conversion_goal_campaign_config WHERE campaign.status = 'ENABLED'",
  customGoals: 'SELECT custom_conversion_goal.resource_name, custom_conversion_goal.id, custom_conversion_goal.name, custom_conversion_goal.status, custom_conversion_goal.conversion_actions FROM custom_conversion_goal'
});
const ROW_KEYS = Object.freeze({ campaigns: 'campaign', actions: 'conversionAction', customerGoals: 'customerConversionGoal',
  campaignGoals: 'campaignConversionGoal', campaignConfigs: 'conversionGoalCampaignConfig', customGoals: 'customConversionGoal' });
const CONVERSION_BIDDING = new Set(['MAXIMIZE_CONVERSIONS', 'MAXIMIZE_CONVERSION_VALUE', 'TARGET_CPA', 'TARGET_ROAS']);
const OTHER_BIDDING = new Set(['MANUAL_CPC', 'MANUAL_CPM', 'MANUAL_CPV', 'FIXED_CPM', 'FIXED_SHARE_OF_VOICE', 'TARGET_SPEND', 'TARGET_IMPRESSION_SHARE', 'TARGET_CPM', 'TARGET_CPV']);
const bool = value => typeof value === 'boolean' ? value : null;
const enumValue = value => typeof value === 'string' && value && !['UNKNOWN', 'UNSPECIFIED', 'INVALID'].includes(value) ? value : null;
const union = (one, two) => one === true || two === true ? true : one === false && two === false ? false : null;
const conjunction = (one, two) => one === false || two === false ? false : one === true && two === true ? true : null;
function resource(value, collection) {
  const match = typeof value === 'string' && value.match(new RegExp('^customers/(\\d+)/' + collection + '/(\\d+)$'));
  return match ? { resourceName: value, customerId: match[1], id: match[2] } : null;
}
const decision = (value, reason, unavailable = false) => ({ eligible: value,
  state: value === true ? 'included' : value === false ? 'excluded' : unavailable ? 'unavailable' : 'unknown', reason });

function configFor(campaign, source) {
  if (source.state !== 'available') return { state: 'unavailable', reason: 'campaign_config_unavailable' };
  const matches = source.rows.filter(row => row.campaign === campaign.resourceName);
  if (matches.length !== 1) return { state: 'unknown', reason: matches.length ? 'duplicate_campaign_config' : 'campaign_config_missing' };
  const raw = matches[0], level = enumValue(raw.goalConfigLevel);
  if (!['CUSTOMER', 'CAMPAIGN'].includes(level)) return { state: 'unknown', reason: 'goal_config_level_unknown' };
  // Omitted empty strings are normal proto JSON for an unset custom goal.
  const custom = raw.customConversionGoal == null || raw.customConversionGoal === '' ? null : resource(raw.customConversionGoal, 'customConversionGoals');
  if (raw.customConversionGoal != null && raw.customConversionGoal !== '' && !custom) return { state: 'unknown', reason: 'custom_goal_resource_invalid' };
  if (level === 'CUSTOMER' && custom) return { state: 'unknown', reason: 'inconsistent_customer_custom_config' };
  return { state: 'available', level, customResource: custom?.resourceName || null };
}

function standardGoal(action, campaign, config, sources) {
  if (action.primaryForGoal === false) return decision(false, 'secondary_excluded_from_standard_goals');
  if (config.state !== 'available') return decision(null, config.reason, config.state === 'unavailable');
  if (!action.category || !action.origin) return decision(null, 'action_category_or_origin_unknown');
  const source = config.level === 'CUSTOMER' ? sources.customerGoals : sources.campaignGoals;
  if (source.state !== 'available') return decision(null, config.level === 'CUSTOMER' ? 'customer_goals_unavailable' : 'campaign_goals_unavailable', true);
  const matches = source.rows.filter(row => row.category === action.category && row.origin === action.origin &&
    (config.level === 'CUSTOMER' || row.campaign === campaign.resourceName));
  if (matches.length !== 1) return decision(null, matches.length ? 'duplicate_matching_standard_goal' : 'matching_standard_goal_missing');
  const biddable = bool(matches[0].biddable);
  if (biddable === false) return { ...decision(false, 'standard_goal_not_biddable'), biddable };
  if (biddable == null) return { ...decision(null, 'standard_goal_biddability_unknown'), biddable };
  if (action.primaryForGoal == null) return { ...decision(null, 'action_primary_role_unknown'), biddable };
  return { ...decision(true, config.level === 'CUSTOMER' ? 'primary_in_customer_goal' : 'primary_in_campaign_goal'), biddable };
}

function customGoal(action, config, source) {
  if (config.state !== 'available') return decision(null, config.reason, config.state === 'unavailable');
  if (!config.customResource) return { ...decision(false, 'no_custom_goal_configured'), member: false, resourceName: null };
  const base = { resourceName: config.customResource };
  if (source.state !== 'available') return { ...decision(null, 'custom_goals_unavailable', true), ...base, member: null };
  const matches = source.rows.filter(row => row.resourceName === config.customResource);
  if (matches.length !== 1) return { ...decision(null, matches.length ? 'duplicate_custom_goal' : 'configured_custom_goal_missing'), ...base, member: null };
  const goal = matches[0];
  if (goal.status === 'REMOVED') return { ...decision(false, 'custom_goal_removed'), ...base, status: goal.status, member: null };
  if (goal.status !== 'ENABLED') return { ...decision(null, 'custom_goal_status_unknown'), ...base, status: goal.status || null, member: null };
  // Repeated empty fields may also be omitted from proto JSON.
  const members = goal.conversionActions == null ? [] : goal.conversionActions;
  if (!Array.isArray(members) || members.some(value => !resource(value, 'conversionActions')))
    return { ...decision(null, 'custom_goal_members_invalid'), ...base, member: null };
  const member = members.includes(action.resourceName);
  return { ...decision(member, member ? 'exact_action_in_custom_goal' : 'action_not_in_custom_goal'), ...base, status: goal.status, member };
}

function actionBinding(action, campaign, sources) {
  const config = configFor(campaign, sources.campaignConfigs);
  const standard = standardGoal(action, campaign, config, sources), custom = customGoal(action, config, sources.customGoals);
  let eligible = union(standard.eligible, custom.eligible), reason = eligible === true ? 'included_by_goal_configuration' : eligible === false ? 'excluded_by_goal_configuration' : 'goal_eligibility_unconfirmed';
  let unavailable = standard.state === 'unavailable' || custom.state === 'unavailable' || action.evidence === 'unavailable';
  if (!action.identityVerified) { eligible = null; reason = 'action_identity_unconfirmed'; }
  else if (action.status === 'REMOVED') { eligible = false; reason = 'action_removed'; unavailable = false; }
  else if (action.status !== 'ENABLED') { eligible = null; reason = 'action_status_unconfirmed'; }
  const conversionBasedBidding = CONVERSION_BIDDING.has(campaign.biddingStrategyType) ? true : OTHER_BIDDING.has(campaign.biddingStrategyType) ? false : null;
  return { campaignResourceName: campaign.resourceName, campaignId: campaign.id, campaignName: campaign.name,
    goalConfigLevel: config.level || null, customGoalResourceName: config.customResource || null,
    standard, custom, ...decision(eligible, reason, unavailable), conversionBasedBidding,
    usedByConversionBasedBidding: conjunction(eligible, conversionBasedBidding) };
}

function aggregate(bindings, complete) {
  if (bindings.some(binding => binding.eligible === true)) return { status: 'included', eligible: true };
  if (complete && !bindings.length) return { status: 'no_active_campaigns', eligible: null };
  if (complete && bindings.every(binding => binding.eligible === false)) return { status: 'excluded', eligible: false };
  return { status: bindings.some(binding => binding.state === 'unavailable') || !complete ? 'unavailable' : 'unknown', eligible: null };
}

function buildGoalEvidence(sources, { at, requiredActionResources = [] }) {
  const sourceMeta = Object.fromEntries(Object.entries(sources).map(([key, value]) => [key,
    { state: value.state, rowCount: value.state === 'available' ? value.rows.length : null, ...(value.error ? { error: value.error } : {}) }]));
  const campaigns = [], byAction = new Map(); let badCampaigns = 0, badActions = 0;
  for (const raw of sources.campaigns.rows) {
    const parsed = resource(raw.resourceName, 'campaigns');
    if (!parsed || (raw.id != null && String(raw.id) !== parsed.id) || !['ENABLED', 'PAUSED', 'REMOVED'].includes(raw.status)) { badCampaigns++; continue; }
    if (raw.status !== 'ENABLED') continue;
    campaigns.push({ ...parsed, name: typeof raw.name === 'string' ? raw.name : null, status: raw.status,
      biddingStrategyType: enumValue(raw.biddingStrategyType) });
  }
  // Duplicate campaign identity is ambiguous, even if one row looks correct.
  const counts = new Map(); campaigns.forEach(c => counts.set(c.resourceName, (counts.get(c.resourceName) || 0) + 1));
  const uniqueCampaigns = campaigns.filter(c => counts.get(c.resourceName) === 1);
  badCampaigns += campaigns.length - uniqueCampaigns.length;
  for (const raw of sources.actions.rows) {
    const parsed = resource(raw.resourceName, 'conversionActions');
    if (!parsed) { badActions++; continue; }
    const item = { ...parsed, name: typeof raw.name === 'string' ? raw.name : null, status: enumValue(raw.status),
      category: enumValue(raw.category), origin: enumValue(raw.origin), primaryForGoal: bool(raw.primaryForGoal),
      identityVerified: raw.id == null || String(raw.id) === parsed.id, evidence: 'verified' };
    if (!item.identityVerified) item.evidence = 'unknown';
    const items = byAction.get(item.resourceName) || []; items.push(item); byAction.set(item.resourceName, items);
  }
  const requested = Array.isArray(requiredActionResources) ? requiredActionResources : [requiredActionResources];
  const allResources = new Set([...byAction.keys()].filter(name => byAction.get(name).some(action => action.status !== 'REMOVED')).concat(requested));
  const complete = sources.campaigns.state === 'available' && !badCampaigns;
  const actions = [];
  for (const name of allResources) {
    const items = byAction.get(name) || [], parsed = resource(name, 'conversionActions');
    let action = items.length === 1 ? items[0] : { resourceName: typeof name === 'string' ? name : null,
      customerId: parsed?.customerId || null, id: parsed?.id || null, name: null, status: null, category: null, origin: null,
      primaryForGoal: null, identityVerified: false, evidence: sources.actions.state !== 'available' ? 'unavailable' : 'unknown' };
    if (items.length > 1) action = { ...action, evidence: 'unknown', identityVerified: false };
    const bindings = uniqueCampaigns.map(campaign => actionBinding(action, campaign, sources));
    const summary = aggregate(bindings, complete);
    actions.push({ ...action, optimizationStatus: action.evidence === 'unavailable' ? 'unavailable' : !action.identityVerified ? 'unknown' : summary.status,
      eligibleForOptimization: action.identityVerified ? summary.eligible : null, campaigns: bindings });
  }
  sourceMeta.campaigns.invalidRows = badCampaigns; sourceMeta.actions.invalidRows = badActions;
  const coIncludedPurchaseActions = uniqueCampaigns.flatMap(campaign => {
    const included = actions.filter(action => action.category === 'PURCHASE' && action.campaigns.some(binding =>
      binding.campaignResourceName === campaign.resourceName && binding.eligible === true));
    return included.length > 1 ? [{ campaignResourceName: campaign.resourceName, campaignId: campaign.id,
      actionResources: included.map(action => action.resourceName), orderOverlapVerified: false, duplicateCountingVerified: false }] : [];
  });
  const unavailable = Object.values(sourceMeta).filter(source => source.state !== 'available').length;
  const uncertain = badCampaigns || badActions || actions.some(action => ['unknown', 'unavailable'].includes(action.optimizationStatus) ||
    action.campaigns.some(binding => binding.eligible == null));
  return { schemaVersion: 1, readOnly: true, evidenceKind: 'conversion_goal_configuration', at,
    status: unavailable === Object.keys(QUERIES).length ? 'unavailable' : unavailable || uncertain ? 'partial' : 'verified',
    receiptConfirmationChecked: false, orderOverlapChecked: false, duplicateCountingVerified: false,
    sources: sourceMeta, activeCampaignCount: complete ? uniqueCampaigns.length : null,
    observedActiveCampaignCount: uniqueCampaigns.length, campaignsComplete: complete, campaigns: uniqueCampaigns,
    actions, coIncludedPurchaseActions,
    notes: ['Goal configuration is separate from receipt processing and order attribution.',
      'Co-included purchase actions require an order-overlap check before changing action roles.',
      'Enabled campaign status alone does not prove that ads are currently serving.'] };
}

function createGoalEvidence({ gaql, now = Date.now } = {}) {
  if (typeof gaql !== 'function') throw TypeError('A read-only paged GAQL function is required.');
  async function read({ requiredActionResources = [], maxMs = 8000 } = {}) {
    const budget = Number.isFinite(Number(maxMs)) ? Math.min(60000, Math.max(0, Number(maxMs))) : 8000;
    const keys = Object.keys(QUERIES);
    const results = await Promise.allSettled(keys.map(async key => {
      if (!budget) throw Error('Goal evidence read deadline reached.');
      let timer;
      try {
        // The injected reader must enforce its own HTTP timeout; this limits
        // how long the audit waits and does not cancel an already-started read.
        const rows = await Promise.race([Promise.resolve().then(() => gaql(QUERIES[key])), new Promise((_, reject) => {
          timer = setTimeout(() => reject(Error('Goal evidence read deadline reached.')), budget);
        })]);
        if (!Array.isArray(rows) || rows.some(row => !row || typeof row[ROW_KEYS[key]] !== 'object' || !row[ROW_KEYS[key]] || Array.isArray(row[ROW_KEYS[key]])))
          throw Error('Goal evidence response was not a complete row array.');
        return rows.map(row => row[ROW_KEYS[key]]);
      } finally { clearTimeout(timer); }
    }));
    const sources = Object.fromEntries(keys.map((key, i) => [key, results[i].status === 'fulfilled'
      ? { state: 'available', rows: results[i].value }
      : { state: 'unavailable', rows: [], error: String(results[i].reason?.message || results[i].reason).slice(0, 300) }]));
    return buildGoalEvidence(sources, { at: now(), requiredActionResources });
  }
  return { read };
}

module.exports = { createGoalEvidence, buildGoalEvidence, QUERIES };
