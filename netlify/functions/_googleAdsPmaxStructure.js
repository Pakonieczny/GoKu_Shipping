// Performance Max structure: products join a campaign that is already learning instead of each
// splitting a small budget, campaign negative keywords, and brand exclusions. Pure functions
// used by googleAdsAutopilot.js and its offline tests: no network, no storage, no clock.
'use strict';

// Google Ads API v24: Performance Max accepts campaign-level negative keywords (CampaignCriterion
// with keyword and negative = true), applied to Search and Shopping inventory. This short list
// removes makers, resellers, job seekers and digital-file searches, which a made-to-order jewellery
// store cannot sell to. Negatives never match close variants, so plurals are listed where needed.
// "free" is deliberately absent: "nickel free" and "tarnish free" are buying searches for jewellery.
const PMAX_DEFAULT_NEGATIVES = Object.freeze([
  ['diy', 'BROAD'], ['how to make', 'PHRASE'], ['tutorial', 'BROAD'], ['wholesale', 'BROAD'], ['bulk', 'BROAD'],
  ['supplier', 'BROAD'], ['suppliers', 'BROAD'], ['manufacturer', 'BROAD'], ['repair', 'BROAD'], ['jobs', 'BROAD'],
  ['hiring', 'BROAD'], ['salary', 'BROAD'], ['replica', 'BROAD'], ['knockoff', 'BROAD'], ['svg', 'BROAD'],
  ['clipart', 'BROAD'], ['printable', 'BROAD']
].map(([text, matchType]) => Object.freeze({ text, matchType })));

// The brand these exclusions protect, and the words that mark a search for it.
const BRAND = Object.freeze({ name: 'Brites', domain: 'britesjewelry.com', listName: 'Brites · brand exclusions', words: ['brites'] });
const MAX_ASSET_GROUPS = 100; // Google's limit per Performance Max campaign

const lower = s => String(s == null ? '' : s).trim().toLowerCase();
const label = s => String(s == null ? '' : s).trim().toUpperCase();
const round2 = n => Math.round(Number(n) * 100) / 100;
const isBrandSearch = term => BRAND.words.some(w => new RegExp('(^|[^a-z])' + w + '($|[^a-z])', 'i').test(String(term || '')));
// Google's keyword limits: at most 80 characters and 10 words.
const validKeyword = t => { const s = String(t || '').trim(); return !!s && s.length <= 80 && s.split(/\s+/).length <= 10; };

function negativeOps(campaign, terms) {
  return (terms || []).map(t => ({ campaignCriterionOperation: { create: { campaign, negative: true, keyword: { text: t.text, matchType: t.matchType } } } }));
}

// Turns a new-campaign build into one more asset group inside an existing campaign: only asset-group
// resources survive, each group belongs to the existing campaign, and nothing at campaign level
// (budget, settings, countries, sitelinks, negatives) is created. With brand guidelines Google keeps
// the business name and logos on the campaign, so they are neither linked to the group nor created.
function intoExistingCampaign(ops, { customerId, campaignId, brandGuidelinesEnabled, takenNames = [] }) {
  if (!/^\d+$/.test(String(campaignId || ''))) throw new Error('Choose an existing Performance Max campaign.');
  if (typeof brandGuidelinesEnabled !== 'boolean') throw new Error('Google did not confirm whether this campaign uses brand guidelines, so nothing was prepared. Try again shortly.');
  const campaign = `customers/${customerId}/campaigns/${campaignId}`;
  const keep = new Set(['assetOperation', 'assetGroupOperation', 'assetGroupAssetOperation', 'assetGroupListingGroupFilterOperation', 'assetGroupSignalOperation']);
  const brandLink = o => brandGuidelinesEnabled && ['BUSINESS_NAME', 'LOGO', 'LANDSCAPE_LOGO'].includes(((o.assetGroupAssetOperation || {}).create || {}).fieldType);
  const kept = (ops || []).filter(o => keep.has(Object.keys(o)[0]) && !brandLink(o));
  // Assets created only for campaign-level links (sitelinks, callouts) or a dropped brand link are
  // not created unlinked.
  const linked = JSON.stringify(kept.filter(o => !o.assetOperation));
  const out = JSON.parse(JSON.stringify(kept.filter(o => !o.assetOperation || !o.assetOperation.create || linked.includes(JSON.stringify(o.assetOperation.create.resourceName)))));
  const taken = new Set((takenNames || []).map(lower));
  out.forEach(o => {
    const g = o.assetGroupOperation && o.assetGroupOperation.create; if (!g) return;
    g.campaign = campaign;
    // Asset group names must be unique within a campaign (DUPLICATE_NAME rejects the whole request).
    const base = String(g.name || 'Product group').slice(0, 118); let name = base, n = 2;
    while (taken.has(lower(name))) name = `${base} · ${n++}`;
    taken.add(lower(name)); g.name = name;
  });
  if (!out.some(o => o.assetGroupOperation)) throw new Error('No product group could be prepared for this campaign.');
  return out;
}

// Destinations offered for a product: retail Performance Max campaigns on the same Merchant Center
// account that can take one more asset group and still serve. Countries are the campaign's own.
function eligibleTarget(t) {
  return !!t && !!t.merchantId && t.status !== 'REMOVED' && t.servingStatus !== 'ENDED' && t.primaryStatus !== 'ENDED' && Number(t.assetGroups || 0) < MAX_ASSET_GROUPS;
}

// What changed in Google since the draft was reviewed, as the sentence Paul reads; null when the
// draft still describes exactly what will happen. Never re-bases a reviewed budget.
function existingTargetProblem({ live, guard, ops, customerId, money = v => Number(v).toFixed(2) }) {
  const name = '“' + (guard.name || ('campaign ' + guard.campaignId)) + '”', res = `customers/${customerId}/campaigns/${guard.campaignId}`;
  if (!live || live.status === 'REMOVED' || live.channel !== 'PERFORMANCE_MAX') return `The Performance Max campaign ${name} is no longer in Google Ads. Nothing was changed. Delete this draft and choose another destination.`;
  if (live.servingStatus === 'ENDED' || live.primaryStatus === 'ENDED') return `The campaign ${name} has ended, so the product would never show. Nothing was changed. Delete this draft and choose another destination.`;
  if (String(live.merchantId || '') !== String(guard.merchantId || '') || label(live.feedLabel) !== label(guard.feedLabel)) return `The campaign ${name} now uses a different product feed. Nothing was changed. Delete this draft and prepare it again.`;
  if (typeof live.brandGuidelinesEnabled !== 'boolean') return `Google did not confirm the brand guidelines setting of ${name}. Nothing was changed. Try again shortly.`;
  if (live.brandGuidelinesEnabled !== !!guard.brandGuidelinesEnabled) return `The brand guidelines setting of ${name} changed after this draft was prepared. Nothing was changed. Delete this draft and prepare it again.`;
  const creates = (ops || []).filter(o => o.assetGroupOperation).map(o => o.assetGroupOperation);
  const budgetOps = (ops || []).filter(o => o.campaignBudgetOperation).map(o => o.campaignBudgetOperation);
  const foreign = (ops || []).some(o => o.campaignOperation || o.campaignCriterionOperation || o.campaignAssetOperation || o.campaignSharedSetOperation || o.sharedSetOperation || o.sharedCriterionOperation || o.adGroupOperation || o.adGroupAdOperation || o.adGroupCriterionOperation);
  if (!creates.length || foreign || creates.some(o => !o.create || o.create.campaign !== res) || budgetOps.length > 1 || budgetOps.some(o => !o.update || o.update.resourceName !== guard.budgetRes || Math.round(Number(o.update.amountMicros)) !== Math.round(Number(guard.budgetAfter) * 1e6) || !(Number(guard.budgetAfter) > Number(guard.budgetBefore))))
    return `This draft may only add product groups${guard.budgetAfter > guard.budgetBefore ? ' and the reviewed budget' : ''} to ${name}. Nothing was changed.`;
  if (Number(live.assetGroupCount || 0) + creates.length > MAX_ASSET_GROUPS) return `${name} already has ${live.assetGroupCount} asset groups; Google allows ${MAX_ASSET_GROUPS}. Nothing was changed. Choose another destination.`;
  const names = new Set((live.assetGroupNames || []).map(lower));
  if (creates.some(o => names.has(lower(o.create.name)))) return `${name} now has an asset group with the same name. Nothing was changed. Delete this draft and prepare it again.`;
  if (budgetOps.length) {
    if (live.budgetRes !== guard.budgetRes) return `${name} now uses a different budget. Nothing was changed. Delete this draft and prepare it again.`;
    if (!(Math.abs(Number(live.budget) - Number(guard.budgetBefore)) < 0.005)) return `The budget of ${name} changed after this draft was prepared (now ${money(live.budget)}, reviewed from ${money(guard.budgetBefore)}). Nothing was changed. Delete this draft and prepare it again; reviewed budgets are never re-based.`;
  }
  return null;
}

// Per campaign: default negatives it lacks, and searches that cost money without a sale.
// terms: [{campaignId, term, clicks, cost, conversions}] summed over the reviewed window.
// existing: Map campaignId -> Set of "text|MATCHTYPE" already excluded. waiting: Set of
// "campaignResource|text" already in a draft. Brand searches are left to the brand exclusion.
function negativePlan({ customerId, campaigns, terms, existing, waiting, wasteCost = 8, wasteClicks = 30 }) {
  const out = [], sums = new Map();
  (terms || []).forEach(r => {
    const term = String(r.term || '').trim().toLowerCase(); if (!term) return;
    const k = String(r.campaignId) + '|' + term, x = sums.get(k) || { campaignId: String(r.campaignId), term, clicks: 0, cost: 0, conversions: 0 };
    x.clicks += Number(r.clicks) || 0; x.cost += Number(r.cost) || 0; x.conversions += Number(r.conversions) || 0; sums.set(k, x);
  });
  for (const c of campaigns || []) {
    const id = String(c.id), res = `customers/${customerId}/campaigns/${id}`, have = (existing && existing.get(id)) || new Set();
    const excluded = text => [...have].some(k => k.split('|')[0] === lower(text));
    const pending = text => !!waiting && waiting.has(res + '|' + lower(text));
    const adds = [];
    PMAX_DEFAULT_NEGATIVES.forEach(n => { if (!excluded(n.text) && !pending(n.text)) adds.push({ text: n.text, matchType: n.matchType, reason: 'default' }); });
    [...sums.values()].filter(x => x.campaignId === id && x.conversions <= 0 && x.cost >= wasteCost && x.clicks >= wasteClicks && validKeyword(x.term) && !isBrandSearch(x.term) && !excluded(x.term) && !pending(x.term) && !adds.some(a => a.text === x.term))
      .sort((a, b) => b.cost - a.cost).forEach(x => adds.push({ text: x.term, matchType: 'EXACT', reason: 'waste', clicks: x.clicks, cost: round2(x.cost) }));
    if (adds.length) out.push({ campaignId: id, name: c.name, adds });
  }
  return out;
}

// Spend Performance Max put on searches for the brand, per campaign, from search-term rows.
function brandSpend(terms) {
  const by = new Map();
  (terms || []).filter(r => isBrandSearch(r.term)).forEach(r => {
    const id = String(r.campaignId), x = by.get(id) || { campaignId: id, clicks: 0, cost: 0, conversions: 0, terms: new Set() };
    x.clicks += Number(r.clicks) || 0; x.cost += Number(r.cost) || 0; x.conversions += Number(r.conversions) || 0; x.terms.add(String(r.term).toLowerCase()); by.set(id, x);
  });
  return [...by.values()].map(x => ({ ...x, cost: round2(x.cost), conversions: round2(x.conversions), terms: [...x.terms].slice(0, 5) }));
}

// Google's verified brand for Brites among SuggestBrands results: the entry that names Brites and,
// when Google lists web addresses, points at the store's domain. Retired or rejected brands are skipped.
function pickBrand(suggestions) {
  const usable = (suggestions || []).filter(b => b && b.id && !['DEPRECATED', 'CANCELLED', 'REJECTED'].includes(String(b.state || '').toUpperCase()) && /^brites\b/i.test(String(b.name || '').trim()));
  const onDomain = usable.filter(b => (b.urls || []).some(u => lower(u).includes(BRAND.domain)));
  const chosen = onDomain[0] || (usable.length === 1 && !(usable[0].urls || []).length ? usable[0] : null);
  return chosen ? { entityId: String(chosen.id), name: String(chosen.name), urls: chosen.urls || [], state: chosen.state || null } : null;
}

// The operations for a reviewed brand exclusion: reuse (or create) one BRANDS list holding the
// brand, then exclude that list in each Performance Max campaign (CampaignCriterion brand_list,
// negative = true). Temporary ID -1 is the only one used, so it cannot collide.
function brandExclusionOps({ customerId, list, brand, campaignIds }) {
  const ops = []; let listRes = list && list.resourceName;
  if (!listRes) { listRes = `customers/${customerId}/sharedSets/-1`; ops.push({ sharedSetOperation: { create: { resourceName: listRes, name: BRAND.listName, type: 'BRANDS' } } }); }
  if (!(list && list.hasBrand)) ops.push({ sharedCriterionOperation: { create: { sharedSet: listRes, brand: { entityId: brand.entityId } } } });
  (campaignIds || []).forEach(id => ops.push({ campaignCriterionOperation: { create: { campaign: `customers/${customerId}/campaigns/${id}`, negative: true, brandList: { sharedSet: listRes } } } }));
  return ops;
}

module.exports = { PMAX_DEFAULT_NEGATIVES, BRAND, MAX_ASSET_GROUPS, negativeOps, intoExistingCampaign, eligibleTarget, existingTargetProblem, negativePlan, brandSpend, pickBrand, brandExclusionOps, isBrandSearch, validKeyword };
