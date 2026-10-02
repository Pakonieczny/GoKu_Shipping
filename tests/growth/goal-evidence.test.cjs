'use strict';

// Synthetic conversion owners, campaign accounts and goal settings only.
// These tests never contact Google or change live bidding/tracking settings.
const test = require('node:test');
const assert = require('node:assert/strict');
const { createGoalEvidence, QUERIES } = require('../../netlify/functions/googleAdsGoalEvidence');
const clone = value => JSON.parse(JSON.stringify(value));
const ACTION = 'customers/123/conversionActions/456';
const OTHER = 'customers/123/conversionActions/457';
const CAMPAIGN = 'customers/999/campaigns/900';
const CUSTOM = 'customers/123/customConversionGoals/700';
const row = (key, value) => ({ [key]: value });
const action = over => ({ resourceName: ACTION, id: '456', name: 'Synthetic purchase', status: 'ENABLED',
  category: 'PURCHASE', origin: 'WEBSITE', primaryForGoal: true, ...over });
const campaign = over => ({ resourceName: CAMPAIGN, id: '900', name: 'Synthetic active campaign', status: 'ENABLED',
  biddingStrategyType: 'MAXIMIZE_CONVERSION_VALUE', ...over });
const customerGoal = over => ({ resourceName: 'customers/123/customerConversionGoals/PURCHASE~WEBSITE', category: 'PURCHASE', origin: 'WEBSITE', biddable: true, ...over });
const campaignGoal = over => ({ campaign: CAMPAIGN, category: 'PURCHASE', origin: 'WEBSITE', biddable: true, ...over });
const config = over => ({ campaign: CAMPAIGN, goalConfigLevel: 'CUSTOMER', ...over });
const customGoal = over => ({ resourceName: CUSTOM, id: '700', name: 'Synthetic custom goal', status: 'ENABLED', conversionActions: [ACTION], ...over });

function fixture(options = {}) {
  const tables = {
    campaigns: [row('campaign', campaign())],
    actions: [row('conversionAction', action())],
    customerGoals: [row('customerConversionGoal', customerGoal())],
    campaignGoals: [row('campaignConversionGoal', campaignGoal())],
    campaignConfigs: [row('conversionGoalCampaignConfig', config())],
    customGoals: []
  };
  const calls = [];
  const api = createGoalEvidence({ now: () => 123456789, gaql: async query => {
    calls.push(query); assert.match(query, /^SELECT /); assert.doesNotMatch(query, /\b(?:MUTATE|INSERT|UPDATE|DELETE)\b/i);
    const key = Object.keys(QUERIES).find(name => QUERIES[name] === query);
    assert.ok(key, 'audit sends only its documented SELECT queries');
    if (options.reject?.includes(key)) throw Error('Synthetic unavailable ' + key);
    if (options.read) return options.read(key, tables);
    return clone(tables[key]);
  } });
  return { tables, calls, api, read: opts => api.read({ requiredActionResources: [ACTION], ...opts }) };
}
const binding = evidence => evidence.actions.find(a => a.resourceName === ACTION).campaigns[0];
const targetAction = evidence => evidence.actions.find(a => a.resourceName === ACTION);

test('six independent reads verify account-default goal eligibility without proving receipts or overlap', async () => {
  const f = fixture(); const evidence = await f.read();
  assert.equal(f.calls.length, 6); assert.equal(new Set(f.calls).size, 6);
  assert.equal(evidence.readOnly, true); assert.equal(evidence.evidenceKind, 'conversion_goal_configuration');
  assert.equal(evidence.status, 'verified'); assert.equal(evidence.at, 123456789);
  assert.equal(evidence.receiptConfirmationChecked, false); assert.equal(evidence.orderOverlapChecked, false);
  assert.equal(evidence.duplicateCountingVerified, false); assert.equal(evidence.activeCampaignCount, 1);
  assert.equal(targetAction(evidence).customerId, '123', 'conversion owner is distinct from the campaign account');
  assert.equal(targetAction(evidence).id, '456'); assert.equal(binding(evidence).campaignId, '900');
  assert.equal(binding(evidence).goalConfigLevel, 'CUSTOMER'); assert.equal(binding(evidence).standard.eligible, true);
  assert.equal(binding(evidence).custom.eligible, false); assert.equal(binding(evidence).eligible, true);
  assert.equal(binding(evidence).usedByConversionBasedBidding, true); assert.equal(targetAction(evidence).optimizationStatus, 'included');
});

test('campaign-specific goals override customer defaults without falling back', async () => {
  const f = fixture(); f.tables.campaignConfigs = [row('conversionGoalCampaignConfig', config({ goalConfigLevel: 'CAMPAIGN' }))];
  f.tables.campaignGoals = [row('campaignConversionGoal', campaignGoal({ biddable: false }))];
  const evidence = await f.read(); assert.equal(binding(evidence).eligible, false);
  assert.equal(binding(evidence).standard.reason, 'standard_goal_not_biddable'); assert.equal(targetAction(evidence).optimizationStatus, 'excluded');
  f.tables.campaignGoals = []; const missing = await f.read(); assert.equal(binding(missing).eligible, null);
  assert.equal(binding(missing).standard.reason, 'matching_standard_goal_missing'); assert.equal(binding(missing).state, 'unknown');
});

test('standard goal membership matches origin as well as category', async () => {
  const f = fixture(); f.tables.actions = [row('conversionAction', action({ origin: 'APP' }))];
  f.tables.customerGoals.push(row('customerConversionGoal', customerGoal({ resourceName: 'customers/123/customerConversionGoals/PURCHASE~APP', origin: 'APP', biddable: false })));
  const evidence = await f.read(); assert.equal(binding(evidence).eligible, false, 'the WEBSITE purchase goal does not make APP purchases biddable');
  f.tables.customerGoals = f.tables.customerGoals.filter(r => r.customerConversionGoal.origin !== 'APP');
  const missing = await f.read(); assert.equal(binding(missing).eligible, null, 'a missing exact category/origin goal stays unknown');
});

test('secondary actions are excluded from standard goals, even if the goal is biddable', async () => {
  const f = fixture(); f.tables.actions = [row('conversionAction', action({ primaryForGoal: false }))];
  const evidence = await f.read(); assert.equal(binding(evidence).standard.eligible, false);
  assert.equal(binding(evidence).eligible, false); assert.equal(binding(evidence).usedByConversionBasedBidding, false);
});

test('an exact custom-goal member is eligible even when the action is secondary', async () => {
  const f = fixture(); f.tables.actions = [row('conversionAction', action({ primaryForGoal: false }))];
  f.tables.campaignConfigs = [row('conversionGoalCampaignConfig', config({ goalConfigLevel: 'CAMPAIGN', customConversionGoal: CUSTOM }))];
  f.tables.customGoals = [row('customConversionGoal', customGoal())];
  const evidence = await f.read(); assert.equal(binding(evidence).standard.eligible, false);
  assert.equal(binding(evidence).custom.member, true); assert.equal(binding(evidence).custom.eligible, true);
  assert.equal(binding(evidence).eligible, true); assert.equal(binding(evidence).usedByConversionBasedBidding, true);
});

test('matching only an action ID from another conversion account cannot match a custom goal', async () => {
  const f = fixture(); f.tables.actions = [row('conversionAction', action({ primaryForGoal: false }))];
  f.tables.campaignConfigs = [row('conversionGoalCampaignConfig', config({ goalConfigLevel: 'CAMPAIGN', customConversionGoal: CUSTOM }))];
  f.tables.customGoals = [row('customConversionGoal', customGoal({ conversionActions: ['customers/444/conversionActions/456'] }))];
  const evidence = await f.read(); assert.equal(binding(evidence).custom.member, false); assert.equal(binding(evidence).eligible, false);
});

test('custom membership does not depend on primary role, category or origin being reported', async () => {
  const f = fixture(); f.tables.actions = [row('conversionAction', action({ primaryForGoal: undefined, category: undefined, origin: undefined }))];
  f.tables.campaignConfigs = [row('conversionGoalCampaignConfig', config({ goalConfigLevel: 'CAMPAIGN', customConversionGoal: CUSTOM }))];
  f.tables.customGoals = [row('customConversionGoal', customGoal())];
  const evidence = await f.read(); assert.equal(binding(evidence).standard.eligible, null); assert.equal(binding(evidence).custom.eligible, true);
  assert.equal(binding(evidence).eligible, true);
});

test('manual bidding is distinct from eligibility in a conversion goal', async () => {
  const f = fixture(); f.tables.campaigns = [row('campaign', campaign({ biddingStrategyType: 'MANUAL_CPC' }))];
  const evidence = await f.read(); assert.equal(binding(evidence).eligible, true);
  assert.equal(binding(evidence).conversionBasedBidding, false); assert.equal(binding(evidence).usedByConversionBasedBidding, false);
  assert.equal(targetAction(evidence).optimizationStatus, 'included', 'goal membership remains visible');
});

test('unreported or unfamiliar bidding strategy does not become a claim about actual bidding', async () => {
  for (const biddingStrategyType of [undefined, 'UNKNOWN', 'FUTURE_STRATEGY']) {
    const f = fixture(); f.tables.campaigns = [row('campaign', campaign({ biddingStrategyType }))];
    const evidence = await f.read(); assert.equal(binding(evidence).eligible, true);
    assert.equal(binding(evidence).conversionBasedBidding, null); assert.equal(binding(evidence).usedByConversionBasedBidding, null);
  }
});

for (const [name, change, reason] of [
  ['missing configuration', f => { f.tables.campaignConfigs = []; }, 'campaign_config_missing'],
  ['unknown configuration level', f => { f.tables.campaignConfigs = [row('conversionGoalCampaignConfig', config({ goalConfigLevel: 'UNKNOWN' }))]; }, 'goal_config_level_unknown'],
  ['duplicate configuration', f => { f.tables.campaignConfigs.push(row('conversionGoalCampaignConfig', config({ goalConfigLevel: 'CAMPAIGN' }))); }, 'duplicate_campaign_config'],
  ['customer-level configuration with a custom override', f => { f.tables.campaignConfigs = [row('conversionGoalCampaignConfig', config({ customConversionGoal: CUSTOM }))]; }, 'inconsistent_customer_custom_config'],
  ['invalid custom resource', f => { f.tables.campaignConfigs = [row('conversionGoalCampaignConfig', config({ goalConfigLevel: 'CAMPAIGN', customConversionGoal: '700' }))]; }, 'custom_goal_resource_invalid']
]) test(name + ' remains unknown rather than excluded or implicitly inherited', async () => {
  const f = fixture(); change(f); const evidence = await f.read();
  assert.equal(binding(evidence).eligible, null); assert.equal(binding(evidence).state, 'unknown');
  assert.equal(binding(evidence).standard.reason, reason); assert.equal(evidence.status, 'partial');
});

test('a missing primary flag and missing biddability flag remain unknown', async () => {
  const f = fixture(); f.tables.actions = [row('conversionAction', action({ primaryForGoal: undefined }))];
  const role = await f.read(); assert.equal(binding(role).eligible, null); assert.equal(binding(role).standard.reason, 'action_primary_role_unknown');
  f.tables.actions = [row('conversionAction', action())]; f.tables.customerGoals = [row('customerConversionGoal', customerGoal({ biddable: undefined }))];
  const goal = await f.read(); assert.equal(binding(goal).eligible, null); assert.equal(binding(goal).standard.reason, 'standard_goal_biddability_unknown');
});

test('a known non-biddable standard goal excludes an action even if its primary flag is missing', async () => {
  const f = fixture(); f.tables.actions = [row('conversionAction', action({ primaryForGoal: undefined }))];
  f.tables.customerGoals = [row('customerConversionGoal', customerGoal({ biddable: false }))];
  const evidence = await f.read(); assert.equal(binding(evidence).eligible, false); assert.equal(binding(evidence).standard.eligible, false);
});

test('duplicate exact category/origin goals do not select whichever looks favorable', async () => {
  const f = fixture(); f.tables.customerGoals.push(row('customerConversionGoal', customerGoal({ biddable: false })));
  const evidence = await f.read(); assert.equal(binding(evidence).eligible, null); assert.equal(binding(evidence).standard.reason, 'duplicate_matching_standard_goal');
});

test('a configured custom goal must exist, be enabled and have a valid membership list', async () => {
  for (const goal of [null, customGoal({ status: 'UNKNOWN' }), customGoal({ conversionActions: '456' }), customGoal({ conversionActions: ['456'] })]) {
    const f = fixture(); f.tables.actions = [row('conversionAction', action({ primaryForGoal: false }))];
    f.tables.campaignConfigs = [row('conversionGoalCampaignConfig', config({ goalConfigLevel: 'CAMPAIGN', customConversionGoal: CUSTOM }))];
    f.tables.customGoals = goal ? [row('customConversionGoal', goal)] : [];
    const evidence = await f.read(); assert.equal(binding(evidence).eligible, null); assert.equal(binding(evidence).custom.state, 'unknown');
  }
});

test('a removed custom goal cannot make a secondary action eligible', async () => {
  const f = fixture(); f.tables.actions = [row('conversionAction', action({ primaryForGoal: false }))];
  f.tables.campaignConfigs = [row('conversionGoalCampaignConfig', config({ goalConfigLevel: 'CAMPAIGN', customConversionGoal: CUSTOM }))];
  f.tables.customGoals = [row('customConversionGoal', customGoal({ status: 'REMOVED' }))];
  const evidence = await f.read(); assert.equal(binding(evidence).custom.eligible, false); assert.equal(binding(evidence).eligible, false);
});

test('an unavailable customer-goal read is different from a missing goal', async () => {
  const f = fixture({ reject: ['customerGoals'] }); const evidence = await f.read();
  assert.equal(f.calls.length, 6); assert.equal(evidence.sources.customerGoals.state, 'unavailable');
  assert.equal(binding(evidence).state, 'unavailable'); assert.equal(binding(evidence).eligible, null);
  assert.equal(binding(evidence).standard.reason, 'customer_goals_unavailable'); assert.equal(evidence.status, 'partial');
});

test('unavailable custom-goal access does not override a known standard inclusion when no custom goal is configured', async () => {
  const f = fixture({ reject: ['customGoals'] }); const evidence = await f.read();
  assert.equal(binding(evidence).eligible, true); assert.equal(binding(evidence).custom.eligible, false);
  assert.equal(evidence.status, 'partial', 'source availability remains visible separately');
});

test('an unavailable configured custom goal cannot silently exclude secondary conversions', async () => {
  const f = fixture({ reject: ['customGoals'] }); f.tables.actions = [row('conversionAction', action({ primaryForGoal: false }))];
  f.tables.campaignConfigs = [row('conversionGoalCampaignConfig', config({ goalConfigLevel: 'CAMPAIGN', customConversionGoal: CUSTOM }))];
  const evidence = await f.read(); assert.equal(binding(evidence).standard.eligible, false);
  assert.equal(binding(evidence).custom.eligible, null); assert.equal(binding(evidence).state, 'unavailable');
  assert.equal(binding(evidence).eligible, null);
});

test('a missing or unavailable action record cannot be enabled just because a custom goal names it', async () => {
  for (const reject of [[], ['actions']]) {
    const f = fixture({ reject }); f.tables.actions = [];
    f.tables.campaignConfigs = [row('conversionGoalCampaignConfig', config({ goalConfigLevel: 'CAMPAIGN', customConversionGoal: CUSTOM }))];
    f.tables.customGoals = [row('customConversionGoal', customGoal())];
    const evidence = await f.read(); assert.equal(binding(evidence).custom.member, true);
    assert.equal(binding(evidence).eligible, null); assert.equal(targetAction(evidence).eligibleForOptimization, null);
    assert.equal(targetAction(evidence).id, '456'); assert.equal(targetAction(evidence).identityVerified, false);
    assert.equal(targetAction(evidence).evidence, reject.length ? 'unavailable' : 'unknown');
  }
});

test('contradictory action identity or duplicate action records remain unknown', async () => {
  for (const rows of [[action({ id: '789' })], [action(), action({ primaryForGoal: false })]]) {
    const f = fixture(); f.tables.actions = rows.map(value => row('conversionAction', value));
    const evidence = await f.read(); assert.equal(targetAction(evidence).identityVerified, false);
    assert.equal(binding(evidence).eligible, null); assert.equal(targetAction(evidence).optimizationStatus, 'unknown');
  }
});

test('removed actions are excluded and unconfirmed hidden status stays unknown', async () => {
  const removed = fixture(); removed.tables.actions = [row('conversionAction', action({ status: 'REMOVED' }))];
  const no = await removed.read(); assert.equal(binding(no).eligible, false); assert.equal(binding(no).reason, 'action_removed');
  const hidden = fixture(); hidden.tables.actions = [row('conversionAction', action({ status: 'HIDDEN' }))];
  const uncertain = await hidden.read(); assert.equal(binding(uncertain).eligible, null); assert.equal(binding(uncertain).reason, 'action_status_unconfirmed');
});

test('co-included purchases are reported as a potential overlap without claiming double counting', async () => {
  const f = fixture(); f.tables.actions.push(row('conversionAction', action({ resourceName: OTHER, id: '457', name: 'Another synthetic purchase' })));
  const evidence = await f.read(); assert.equal(evidence.coIncludedPurchaseActions.length, 1);
  const potential = evidence.coIncludedPurchaseActions[0]; assert.equal(potential.campaignResourceName, CAMPAIGN);
  assert.deepEqual(potential.actionResources, [ACTION, OTHER]); assert.equal(potential.orderOverlapVerified, false);
  assert.equal(potential.duplicateCountingVerified, false); assert.equal(evidence.receiptConfirmationChecked, false);
  assert.equal(evidence.recommendations, undefined, 'an audit does not issue role-switch recommendations');
});

test('separate campaign goals do not become a claim that both actions feed one campaign', async () => {
  const SECOND_CAMPAIGN = 'customers/999/campaigns/901', SECOND_CUSTOM = 'customers/123/customConversionGoals/701';
  const f = fixture(); f.tables.campaigns.push(row('campaign', campaign({ resourceName: SECOND_CAMPAIGN, id: '901' })));
  f.tables.actions = [row('conversionAction', action({ primaryForGoal: false })), row('conversionAction', action({ resourceName: OTHER, id: '457', primaryForGoal: false }))];
  f.tables.campaignConfigs = [row('conversionGoalCampaignConfig', config({ goalConfigLevel: 'CAMPAIGN', customConversionGoal: CUSTOM })),
    row('conversionGoalCampaignConfig', config({ campaign: SECOND_CAMPAIGN, goalConfigLevel: 'CAMPAIGN', customConversionGoal: SECOND_CUSTOM }))];
  f.tables.customGoals = [row('customConversionGoal', customGoal()), row('customConversionGoal', customGoal({ resourceName: SECOND_CUSTOM, id: '701', conversionActions: [OTHER] }))];
  const evidence = await f.read(); assert.equal(evidence.coIncludedPurchaseActions.length, 0);
  assert.equal(targetAction(evidence).campaigns[0].eligible, true); assert.equal(targetAction(evidence).campaigns[1].eligible, false);
  assert.equal(evidence.actions.find(a => a.resourceName === OTHER).campaigns[0].eligible, false);
  assert.equal(evidence.actions.find(a => a.resourceName === OTHER).campaigns[1].eligible, true);
});

test('no enabled campaigns is a verified observation and not a claim about purchase tracking', async () => {
  const f = fixture(); f.tables.campaigns = []; const evidence = await f.read();
  assert.equal(evidence.activeCampaignCount, 0); assert.equal(evidence.campaignsComplete, true);
  assert.equal(targetAction(evidence).optimizationStatus, 'no_active_campaigns');
  assert.equal(targetAction(evidence).eligibleForOptimization, null); assert.equal(evidence.receiptConfirmationChecked, false);
});

test('campaign read failures, invalid identities or duplicate campaigns cannot prove there are no active campaigns', async () => {
  const rejected = fixture({ reject: ['campaigns'] }); const unavailable = await rejected.read();
  assert.equal(unavailable.activeCampaignCount, null); assert.equal(unavailable.campaignsComplete, false);
  for (const campaigns of [[campaign({ resourceName: '900' })], [campaign({ id: '789' })], [campaign({ status: 'UNKNOWN' })], [campaign(), campaign()]]) {
    const f = fixture(); f.tables.campaigns = campaigns.map(value => row('campaign', value)); const evidence = await f.read();
    assert.equal(evidence.activeCampaignCount, null); assert.equal(evidence.campaignsComplete, false); assert.equal(evidence.status, 'partial');
  }
});

test('paused and removed campaigns do not claim current conversion optimization', async () => {
  const f = fixture(); f.tables.campaigns = [row('campaign', campaign({ status: 'PAUSED' })), row('campaign', campaign({ status: 'REMOVED', resourceName: 'customers/999/campaigns/901', id: '901' }))];
  const evidence = await f.read(); assert.equal(evidence.activeCampaignCount, 0); assert.equal(targetAction(evidence).campaigns.length, 0);
});

test('all source failures report unavailable and preserve every independent access result', async () => {
  const f = fixture({ reject: Object.keys(QUERIES) }); const evidence = await f.read();
  assert.equal(evidence.status, 'unavailable'); assert.equal(f.calls.length, 6); assert.equal(evidence.activeCampaignCount, null);
  assert.equal(targetAction(evidence).optimizationStatus, 'unavailable'); assert.equal(targetAction(evidence).eligibleForOptimization, null);
  assert.equal(Object.values(evidence.sources).filter(source => source.state === 'unavailable' && source.error).length, 6);
});

test('malformed or partial row arrays are unavailable instead of silently accepted as no data', async () => {
  for (const malformed of [{ results: [] }, [null], [{ conversionAction: action() }, {}]]) {
    const f = fixture({ read: (key, tables) => key === 'actions' ? malformed : clone(tables[key]) });
    const evidence = await f.read(); assert.equal(evidence.sources.actions.state, 'unavailable');
    assert.equal(targetAction(evidence).eligibleForOptimization, null); assert.equal(targetAction(evidence).optimizationStatus, 'unavailable');
  }
});

test('observer deadlines return unavailable without waiting indefinitely', { timeout: 1000 }, async () => {
  const f = fixture({ read: () => new Promise(() => {}) }); const evidence = await f.read({ maxMs: 15 });
  assert.equal(evidence.status, 'unavailable'); assert.equal(f.calls.length, 6);
  assert.equal(Object.values(evidence.sources).filter(source => /deadline reached/.test(source.error || '')).length, 6);
});

test('zero read budget makes no API calls', async () => {
  const f = fixture(); const evidence = await f.read({ maxMs: 0 }); assert.equal(f.calls.length, 0); assert.equal(evidence.status, 'unavailable');
});

test('required action resources never interpolate into GAQL and ID-only requests stay unknown', async () => {
  const f = fixture(); const requested = ['456', "customers/123/conversionActions/456'; DELETE account", ACTION];
  const evidence = await f.read({ requiredActionResources: requested });
  assert.equal(f.calls.length, 6); assert.deepEqual(new Set(f.calls), new Set(Object.values(QUERIES)));
  for (const name of requested.slice(0, 2)) {
    const unknown = evidence.actions.find(a => a.resourceName === name); assert.equal(unknown.identityVerified, false);
    assert.equal(unknown.id, null); assert.equal(unknown.eligibleForOptimization, null); assert.equal(unknown.optimizationStatus, 'unknown');
  }
});

test('missing GAQL dependency fails before any work is attempted', () => {
  assert.throws(() => createGoalEvidence(), /read-only paged GAQL/);
});
