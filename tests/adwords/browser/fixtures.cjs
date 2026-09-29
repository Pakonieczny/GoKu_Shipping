'use strict';
// Synthetic answers for every console API action the click-through harness can
// reach. Shapes follow netlify/functions/googleAdsAutopilotKick.js handleAction
// and the engine/report builders it calls. Every name, id, order and amount is
// invented. Nothing here reads a store, Google Ads, Shopify or an AI provider.
//
// Money tracers: amounts in the Google Ads account currency (CAD) all use cents
// ending in 7 (13.37, 27.17, ...). Report amounts (USD) never end in 7. The
// browser audit uses CAD_TRACERS to find a CAD amount printed with a bare "$".

const DAY = 86400000;
const ACCOUNT_TZ = 'America/Toronto';
const CID = '5550001111';
const SHOP = 'https://britesjewelry.com';
const IMG = 'https://cdn.shopify.com/s/files/harness';

function ymdIn(tz, ms) {
  const p = new Intl.DateTimeFormat('en-CA', { timeZone: tz, year: 'numeric', month: '2-digit', day: '2-digit' }).formatToParts(new Date(ms));
  const v = {}; p.forEach(x => { v[x.type] = x.value; });
  return v.year + '-' + v.month + '-' + v.day;
}
function addDays(ymd, n) { const d = new Date(ymd + 'T12:00:00Z'); d.setUTCDate(d.getUTCDate() + n); return d.toISOString().slice(0, 10); }
function daysBetween(start, end) { const out = []; for (let d = start; d <= end && out.length < 400; d = addDays(d, 1)) out.push(d); return out; }
// Deterministic pseudo-random in [0,1) from a string.
function rnd(key) { let h = 2166136261; for (const ch of String(key)) { h ^= ch.charCodeAt(0); h = Math.imul(h, 16777619); } return ((h >>> 0) % 100000) / 100000; }
const r2 = n => Math.round(n * 100) / 100;
// USD report figures must never end in a 7-cent digit (reserved for CAD tracers).
const usd = n => { let v = r2(n); if (Math.round(v * 100) % 10 === 7) v = r2(v + 0.01); return v; };

// ---------------------------------------------------------------- the world
const CAMPAIGNS = [
  { id: '9100000001', name: 'Harness Search · Charm Necklaces', channel: 'SEARCH', status: 'ENABLED', primaryStatus: 'ELIGIBLE', primaryStatusReasons: [], budget: 13.37, lane: 'search', weight: 1.0, start: -40, end: 50 },
  { id: '9100000002', name: 'Harness PMax · Best Sellers Feed', channel: 'PERFORMANCE_MAX', status: 'ENABLED', primaryStatus: 'LIMITED', primaryStatusReasons: ['CAMPAIGN_BUDGET_LIMITED', 'UNKNOWN', 'UNKNOWN'], budget: 27.17, lane: 'product', weight: 1.6, start: -90, end: null },
  { id: '9100000003', name: 'Harness Studio · Design Your Own Charm', channel: 'PERFORMANCE_MAX', status: 'PAUSED', primaryStatus: 'PAUSED', primaryStatusReasons: ['CAMPAIGN_PAUSED'], budget: 12.37, lane: 'studio', weight: 0.4, start: -20, end: 70 },
  { id: '9100000004', name: 'Harness Search · Personalized Initial Charm Necklaces for Holiday Gift Buyers in Canada and the United States (Exact + Phrase)', channel: 'SEARCH', status: 'ENABLED', primaryStatus: 'PENDING', primaryStatusReasons: ['CAMPAIGN_PENDING'], budget: 9.17, lane: 'search', weight: 0, start: 6, end: 96 },
  { id: '9100000005', name: 'Harness Display · Retired Test', channel: 'DISPLAY', status: 'REMOVED', primaryStatus: 'REMOVED', primaryStatusReasons: ['CAMPAIGN_REMOVED'], budget: 6.47, lane: 'other', weight: 0.3, start: -120, end: -10, deleted: true },
  { id: '9100000006', name: 'Harness PMax · Not Eligible Feed', channel: 'PERFORMANCE_MAX', status: 'ENABLED', primaryStatus: 'NOT_ELIGIBLE', primaryStatusReasons: ['HAS_ADS_DISAPPROVED', 'SOME_NEW_REASON_CODE'], budget: 8.77, lane: 'product', weight: 0.2, start: -15, end: null }
];
const CAD_BUDGET_TOTAL = r2(CAMPAIGNS.filter(c => c.status !== 'REMOVED').reduce((s, c) => s + c.budget, 0)); // 70.85? recomputed below
const PRODUCTS = [
  { pid: '71000001', handle: 'harness-duck-charm-necklace', title: 'Harness Duck Charm Necklace', price: 48 },
  { pid: '71000002', handle: 'harness-fox-initial-necklace', title: 'Harness Fox Initial Necklace', price: 56 },
  { pid: '71000003', handle: 'harness-moon-disc-bracelet', title: 'Harness Moon Disc Bracelet', price: 39 },
  { pid: '71000004', handle: 'harness-custom-photo-charm', title: 'Harness Custom Photo Charm — Engraved Sterling Silver Keepsake With A Very Long Listing Title', price: 89 },
  { pid: '71000005', handle: 'harness-birthstone-stack-ring', title: 'Harness Birthstone Stack Ring', price: 42 }
];
const offerId = p => 'shopify_US_' + p.pid + '_' + p.pid.slice(-3) + '01';
const productUrl = p => SHOP + '/products/' + p.handle;
const productImage = (p, i = 0) => IMG + '/' + p.handle + '-' + i + '.jpg?v=1';

const GROUPS = [
  { campaignId: '9100000001', ref: 'customers/' + CID + '/adGroups/8100000011', name: 'Duck charm necklace', channel: 'search', products: [0] },
  { campaignId: '9100000001', ref: 'customers/' + CID + '/adGroups/8100000012', name: 'Fox initial necklace · gift intent', channel: 'search', products: [1, 3] },
  { campaignId: '9100000002', ref: 'customers/' + CID + '/assetGroups/8200000021', name: 'Best sellers · necklaces', channel: 'pmax', products: [0, 1] },
  { campaignId: '9100000002', ref: 'customers/' + CID + '/assetGroups/8200000022', name: 'Moon disc bracelet', channel: 'pmax', products: [2] },
  { campaignId: '9100000002', ref: 'customers/' + CID + '/assetGroups/8200000023', name: 'Stack rings (broad filter)', channel: 'pmax', products: [], broad: true },
  { campaignId: '9100000003', ref: 'customers/' + CID + '/assetGroups/8200000031', name: 'Studio · photo to charm', channel: 'pmax', products: [3] },
  { campaignId: '9100000004', ref: 'customers/' + CID + '/adGroups/8100000041', name: 'Initial necklaces · holiday', channel: 'search', products: [1] },
  { campaignId: '9100000006', ref: 'customers/' + CID + '/assetGroups/8200000061', name: 'Birthstone rings', channel: 'pmax', products: [4] }
];

const COUNTRIES = [
  ['2124', 'Canada', 'CA'], ['2840', 'United States', 'US'], ['2826', 'United Kingdom', 'GB'], ['2036', 'Australia', 'AU'],
  ['2554', 'New Zealand', 'NZ'], ['2372', 'Ireland', 'IE'], ['2276', 'Germany', 'DE'], ['2250', 'France', 'FR']
].map(([id, name, code]) => ({ id, name, code }));

// Every CAD amount the fixtures emit (budgets, ceilings and their sums).
function cadTracers() {
  const live = CAMPAIGNS.filter(c => c.status !== 'REMOVED');
  const vals = new Set([...CAMPAIGNS.map(c => c.budget), r2(live.reduce((s, c) => s + c.budget, 0)), 11.27, 14.57, 16.47, 12.07, 7.47, 21.17, 170, 23.47, 410.47, 370.47, 330.47, 8.47, 12.77, 21.24, 99.17, 30.17, 41.27,
    17.47 /* the walk's answer to budget prompts */]);
  return [...vals];
}

function createFixtures(opts = {}) {
  const now = opts.now || Date.now();
  const today = ymdIn(ACCOUNT_TZ, now);
  const state = {
    control: { enabled: true, dryRun: false, smartBidding: false, maxDailyBudgetTotal: 170, maxBudgetStepPct: 20, budgetMoveApprovalPct: 25, targetRoas: 3, minConvForTargetTune: 15, anomalySpendMultiple: 2.5, learningCooldownDays: 14, defaultCountries: ['2124', '2840'], maxMonthlySpend: 0, creativeBudgetUsd: 8, budgetCurrency: 'CAD', autoApproveVettedTemplates: false },
    deleted: new Set(['9100000005']),
    pending: null,
    ledger: null,
    genPolls: new Map(),
    creative: new Map(),
    diagRun: 0
  };

  const campaignDates = c => ({ startDate: addDays(today, c.start), endDate: c.end == null ? null : addDays(today, c.end) });
  const campaignBase = c => Object.assign({ id: c.id, name: c.name, status: c.status, channel: c.channel, primaryStatus: c.primaryStatus, primaryStatusReasons: c.primaryStatusReasons.slice(), budget: c.budget, budgetRes: 'customers/' + CID + '/campaignBudgets/' + c.id.slice(-4), opportunityLane: c.lane, version: { version: 3 + Number(c.id.slice(-1)), updatedAt: now - (2 + Number(c.id.slice(-1))) * DAY, summary: 'Budget and headline update' } }, campaignDates(c));

  // Per-day, per-campaign synthetic metrics. Removed campaigns only have
  // activity before their end date; pending campaigns have none.
  function dayMetrics(c, date) {
    const { startDate, endDate } = campaignDates(c);
    if (!c.weight || date < startDate || (endDate && date > endDate) || c.primaryStatus === 'PENDING') return null;
    const k = c.id + date, impr = Math.round(c.weight * (380 + 900 * rnd(k + 'i'))), clicks = Math.round(impr * (0.02 + 0.04 * rnd(k + 'c')));
    const cost = usd(clicks * (0.35 + 0.5 * rnd(k + 'p')) * c.weight), conv = Math.round(clicks * 0.08 * rnd(k + 'v') * 10) / 10, convCd = Math.round(clicks * 0.075 * rnd(k + 'w') * 10) / 10;
    return { impr, clicks, cost, conv, value: usd(conv * 52.3), convCd, valueCd: usd(convCd * 51.9) };
  }
  function rangeRows(start, end) {
    const days = daysBetween(start, end);
    return CAMPAIGNS.map(c => {
      const row = Object.assign(campaignBase(c), { cost: 0, conv: 0, value: 0, clicks: 0, impr: 0, convCd: 0, valueCd: 0, costNative: 0, valueNative: 0, valueCdNative: 0, cdUnavailable: false, fxIncomplete: false, currency: 'USD', metricsUnavailable: false, historicalOnly: c.status === 'REMOVED', metricsRange: { start, end } });
      let active = false;
      days.forEach(d => { const m = dayMetrics(c, d); if (!m) return; active = active || m.impr > 0; ['cost', 'conv', 'value', 'clicks', 'impr', 'convCd', 'valueCd'].forEach(f => { row[f] += m[f]; }); row.costNative += m.cost * 1.37; row.valueNative += m.value * 1.37; row.valueCdNative += m.valueCd * 1.37; });
      ['cost', 'value', 'valueCd'].forEach(f => { row[f] = usd(row[f]); });
      ['costNative', 'valueNative', 'valueCdNative'].forEach(f => { row[f] = usd(row[f]); });
      row.conv = Math.round(row.conv * 10) / 10; row.convCd = Math.round(row.convCd * 10) / 10;
      if (state.deleted.has(c.id)) { row.deleted = true; row.historicalOnly = true; }
      row._active = active;
      return row;
    }).filter(r => r._active || (!state.deleted.has(r.id) && r.status !== 'REMOVED')).map(r => { delete r._active; return r; });
  }
  function validRange(body, fallbackDays = 14) {
    let start = /^\d{4}-\d{2}-\d{2}$/.test(body && body.start || '') ? body.start : addDays(today, -(fallbackDays - 1));
    let end = /^\d{4}-\d{2}-\d{2}$/.test(body && body.end || '') ? body.end : today;
    return { start, end };
  }
  const context = () => ({ accountTimezone: ACCOUNT_TZ, accountToday: today, budgetCurrency: 'CAD', checkedAt: now });

  function orders() {
    const out = [], sources = [
      { source: 'google', medium: 'cpc', campaign: '9100000001', hasClickId: true, campaignId: '9100000001', adGroupId: '8100000011', adId: '7100000111', pipeline: null },
      { source: 'google', medium: 'cpc', campaign: '9100000002', hasClickId: true, campaignId: '9100000002', pipeline: 'pmax' },
      { source: 'google', medium: 'organic', campaign: 'sag_organic' },
      { source: 'direct', medium: '', campaign: '' },
      { source: 'facebook', medium: 'paid_social', campaign: 'harness-meta-test' },
      { source: 'newsletter', medium: 'email', campaign: 'harness-autumn' }
    ];
    for (let i = 0; i < 46; i++) {
      const ts = now - Math.floor((i * 1.37 + rnd('o' + i) * 0.9) * DAY), p = PRODUCTS[i % PRODUCTS.length], qty = 1 + (i % 3 === 0 ? 1 : 0), src = sources[i % sources.length];
      const value = usd(p.price * qty + (i % 4) * 3.5);
      out.push(Object.assign({ id: 'harness-order-' + i, orderId: 'gid://shopify/Order/6' + String(100000 + i), orderNumericId: '6' + String(100000 + i), ts, value, netValue: i % 11 === 5 ? usd(value - p.price) : value, financialStatus: i === 3 ? 'PENDING' : i % 11 === 5 ? 'PARTIALLY_REFUNDED' : i === 8 ? null : 'PAID', cancelledAt: null, test: false, currency: i === 13 ? 'CAD' : 'USD', captured: true, reason: src.campaign === 'sag_organic' ? 'free Google listing' : '',
        items: [{ title: p.title, productId: 'gid://shopify/Product/' + p.pid, variantId: 'gid://shopify/ProductVariant/' + p.pid + '9', sku: 'HRN-' + p.pid.slice(-3), qty, lineRevenue: usd(p.price * qty), refundedQty: i % 11 === 5 ? 1 : 0, refundedRevenue: i % 11 === 5 ? p.price : 0 }],
        itemCount: qty }, src));
    }
    return out;
  }
  function ledger() {
    const items = [
      { kind: 'mutate', label: 'Budget raised 11.27 → 13.37 CAD · Harness Search · Charm Necklaces', ok: true, verified: true },
      { kind: 'uploadConversions', label: 'Conversion upload · 4 orders', ok: true, partialFailure: { message: 'Harness: 1 conversion rejected (click too old).', details: [{ code: 'expired click' }] } },
      { kind: 'mutateAll', label: 'Negative keywords added · 6 terms', ok: true, verified: false },
      { kind: 'mutate', label: 'Headline rewrite · Harness PMax · Best Sellers Feed', ok: false, error: 'Harness: headline rejected by policy review' },
      { kind: 'measure', label: 'Measure', ok: true, validateOnly: true },
      { kind: 'mutate', label: 'Campaign paused · Harness Studio · Design Your Own Charm', ok: true }
    ];
    return items.map((x, i) => Object.assign({ at: now - (i * 7 + 1) * 3600000 }, x));
  }
  function searchPayload(extra) {
    const ops = [
      { campaignBudgetOperation: { create: { amountMicros: String(11.27 * 1e6) } } },
      { campaignOperation: { create: { name: 'Harness Search draft', startDateTime: addDays(today, 2).replace(/-/g, '') + ' 00:00:00', endDateTime: addDays(today, 46).replace(/-/g, '') + ' 23:59:59' } } },
      { adGroupAdOperation: { create: { ad: { finalUrls: [productUrl(PRODUCTS[1])], responsiveSearchAd: { headlines: ['Fox Initial Necklace', 'Personalized Gift Idea', 'Handmade in Canada — Ships Fast', 'Engraved Initial Charm Jewelry For Her'].map(text => ({ text })), descriptions: ['Hand-finished initial necklaces in sterling silver and gold fill. Free gift wrap on every order.', 'Choose a letter, a chain length and a finish. Shipped from our studio within two business days.'].map(text => ({ text })) } } } } },
      ...['fox initial necklace', 'initial necklace gift', 'personalized letter necklace'].map((text, i) => ({ adGroupCriterionOperation: { create: { keyword: { text, matchType: i ? 'PHRASE' : 'EXACT' } } } })),
      { campaignCriterionOperation: { create: { negative: true, keyword: { text: 'free' } } } },
      { assetOperation: { create: { sitelinkAsset: { linkText: 'Shop initials' } } } },
      { assetOperation: { create: { calloutAsset: { calloutText: 'Free gift wrap' } } } }
    ];
    return Object.assign({ mutateOperations: ops, countries: ['2124', '2840'], adGroupSummary: [{ name: 'Initial necklaces', finalUrl: productUrl(PRODUCTS[1]), keywords: ['fox initial necklace', 'initial necklace gift'] }], keywordValidation: { confidence: 82, evidence: { accepted: 12, rejected: 3 } } }, extra || {});
  }
  function pendingApprovals() {
    return [
      { id: 'harness-approval-search', type: 'search', status: 'PENDING', summary: 'Harness Search · Fox initial necklaces for holiday gift buyers (draft)', payload: searchPayload(), creative: null, vetted: false },
      { id: 'harness-approval-pmax', type: 'pmax', status: 'PENDING', summary: 'Harness Product ads · Moon disc bracelet feed', creative: { phase: 'not_started' },
        payload: { meta: { dailyBudget: 14.57, targetRoas: 3.5, itemIds: [offerId(PRODUCTS[2])], productTitles: [PRODUCTS[2].title], assetGroups: [{ name: 'Moon disc bracelet', itemIds: [offerId(PRODUCTS[2])] }], searchThemes: ['moon bracelet', 'disc bracelet gift', 'celestial jewelry'], merchantId: '123450000', feedLabel: 'ca', countries: ['2124'], assetMode: 'reviewed-custom', images: 3 }, mutateOperations: [{ campaignBudgetOperation: { create: { amountMicros: String(14.57 * 1e6) } } }] } },
      { id: 'harness-approval-studio', type: 'studio', status: 'PENDING', summary: 'Harness Design Studio · Search · high-intent charm design', creative: null,
        payload: { countries: ['2124', '2840'], meta: { kind: 'designStudioSearch', dailyBudget: 16.47, countries: ['2124', '2840'], startDate: addDays(today, 1), endDate: addDays(today, 91) }, mutateOperations: searchPayload().mutateOperations } },
      { id: 'harness-approval-split', type: 'groupSplit', status: 'PENDING', summary: 'Harness · Split "Best sellers · necklaces" into one group per product',
        payload: { groupSplitGuard: { campaignId: '9100000002', sourceGroupRef: GROUPS[2].ref, channel: 'pmax' }, meta: { sourceGroupRef: GROUPS[2].ref, assetGroups: [0, 1].map(i => ({ name: PRODUCTS[i].title, itemIds: [offerId(PRODUCTS[i])], url: productUrl(PRODUCTS[i]), ref: 'customers/' + CID + '/assetGroups/-' + (i + 1) })) }, mutateOperations: [] } }
    ];
  }
  function stuckApprovals() {
    return [{ id: 'harness-stuck-1', type: 'search', summary: 'Harness Search · Birthstone rings (publish failed)', status: 'APPROVED', lastError: 'Harness: the budget is below the minimum.', creative: null, vetted: false, payload: searchPayload() }];
  }
  function conversionHealth() {
    return { validated: false, healthy: true, failedCount: 2, queueDepth: 3, adjQueueDepth: 1, recentConversions: 14, actionConfigured: true, actionsChecked: true, configuredAction: { id: '4400001', name: 'Harness purchase (upload)', status: 'ENABLED' },
      lastUpload: { at: now - 5 * 3600000 }, reasons: ['Two recent uploads were rejected by Google Ads (see details).'],
      failedSamples: [{ orderId: '6100004', error: 'Harness: the click identifier is older than the conversion window.' }, { orderId: '6100009', error: '' }],
      actions: [{ id: '4400001', name: 'Harness purchase (upload)', status: 'ENABLED' }, { id: '4400002', name: 'Harness legacy tag', status: 'REMOVED' }],
      dataManager: { configured: true, confirmed: 9, processing: 2, unknown: 1, retryable: true, missingScopes: ['https://www.googleapis.com/auth/cloud-platform'] } };
  }

  function dashboard() {
    const lastRange = { start: addDays(today, -13), end: today };
    const lm = rangeRows(lastRange.start, lastRange.end).filter(c => !state.deleted.has(c.id)).map(c => Object.assign(c, { metricsRange: lastRange }));
    const series = [0, 1, 2].map(i => ({ at: now - (2 - i) * DAY, kind: 'measure', snapshot: lm, range: lastRange, currency: 'USD', budgetCurrency: 'CAD' }));
    if (!state.pending) state.pending = pendingApprovals();
    if (!state.ledger) state.ledger = ledger();
    return Object.assign({ control: Object.assign({}, state.control), currency: 'USD', budgetCurrency: 'CAD', collections: [], occasions: [], terms: ['free', 'cheap', 'replica'],
      pending: state.pending.slice(), stuck: stuckApprovals(), recentLedger: state.ledger.slice(), lastMetrics: lm, lastMetricsRange: lastRange, lastMetricsAt: now - DAY, lastMetricsCurrency: 'USD', metricsSeries: series,
      campaignInventory: { ok: true, checkedAt: now }, deletedCampaignIds: [...state.deleted], conversionHealth: conversionHealth(), recentOrders: orders(), editPasscodeSet: true }, context());
  }

  function dailyStats(body) {
    const range = validRange(body, 7), days = daysBetween(range.start, range.end), zero = () => ({ impr: 0, clicks: 0, cost: 0, conv: 0, value: 0, convCd: 0, valueCd: 0 });
    const totalsByDay = {}, byCamp = {};
    days.forEach(d => { totalsByDay[d] = zero(); });
    CAMPAIGNS.forEach(c => {
      if (body && body.campaignId && String(body.campaignId) !== c.id) return;
      const series = {}, totals = zero(); let any = false;
      days.forEach(d => { const m = dayMetrics(c, d); if (!m) return; any = true; series[d] = m; Object.keys(totals).forEach(k => { totals[k] += m[k]; totalsByDay[d][k] += m[k]; }); });
      if (!any && c.status === 'REMOVED') return;
      totals.costNative = usd(totals.cost * 1.37);
      byCamp[c.id] = { id: c.id, name: c.name, channel: c.channel, status: c.status, totals, series };
    });
    days.forEach(d => { const t = totalsByDay[d]; ['cost', 'value', 'valueCd'].forEach(k => { t[k] = usd(t[k]); }); });
    const products = PRODUCTS.slice(0, 4).map((p, i) => ({ campaignId: i < 2 ? '9100000002' : '9100000006', itemId: offerId(p), title: p.title, productUrl: productUrl(p), productId: 'gid://shopify/Product/' + p.pid, storeProductId: p.pid, identityComplete: true, impr: 900 - i * 100, clicks: 40 - i * 5, cost: usd(22.4 - i * 3), conv: 2 - i * 0.5, value: usd(104.6 - i * 20), convCd: 1.5, valueCd: usd(88.2), purchaseConversions: 1, purchaseValue: usd(52.3) }));
    const ads = [{ campaignId: '9100000001', adGroup: 'Duck charm necklace', adGroupId: '8100000011', adId: '7100000111', finalUrls: [productUrl(PRODUCTS[0])], headlines: ['Duck Charm Necklace', 'Handmade Sterling Charm'], strength: 'GOOD', status: 'ENABLED', impr: 812, clicks: 31, cost: usd(18.2), conv: 1.5, value: usd(78.4) }];
    const keywords = [{ campaignId: '9100000001', text: 'duck charm necklace', match: 'EXACT', status: 'ENABLED', qs: 7, impr: 400, clicks: 21, cost: usd(9.6), conv: 1, value: usd(48.3) }];
    const assetGroups = [{ campaignId: '9100000002', agId: '8200000021', name: 'Best sellers · necklaces', strength: 'AVERAGE', impr: 1400, clicks: 55, cost: usd(30.1), conv: 2, value: usd(110.2) }];
    return Object.assign({ ok: true, range, days, fxIncomplete: false }, context(), { currency: 'USD', breakdownCurrency: 'CAD', breakdownBasis: 'click', cdAvailable: true, includesRemovedWithActivity: true, warnings: [], coverage: { products: { ok: true } },
      totalsByDay: days.map(d => Object.assign({ date: d }, totalsByDay[d])),
      campaigns: Object.values(byCamp).map(c => Object.assign({}, c, { series: days.map(d => Object.assign({ date: d }, c.series[d] || zero())) })),
      ads, keywords, assetGroups, products, productTotals: {}, productReport: { source: 'shopping_performance_view', range, currency: 'CAD', complete: true, identityComplete: true, rows: products.length }, channelTotals: {} });
  }

  function groupsIndex(body) {
    const range = validRange(body, 30), campaignId = body && body.campaignId ? String(body.campaignId) : null;
    const groups = GROUPS.filter(g => !campaignId || g.campaignId === campaignId).filter(g => !state.deleted.has(g.campaignId)).map((g, i) => {
      const c = CAMPAIGNS.find(x => x.id === g.campaignId), ps = g.products.map(k => PRODUCTS[k]);
      const urls = g.channel === 'pmax' ? (ps.length === 1 ? [productUrl(ps[0])] : [SHOP + '/collections/necklaces']) : ps.map(productUrl);
      const mapping = { itemIds: g.channel === 'pmax' ? ps.map(offerId) : [], productIds: g.channel === 'pmax' ? ps.map(p => p.pid) : [], handles: g.channel === 'search' ? ps.map(p => p.handle) : ps.length === 1 ? [ps[0].handle] : [], urls,
        exactOfferScope: g.channel === 'pmax' && !g.broad && ps.length > 0, status: g.broad ? 'unverified' : ps.length > 1 ? 'mixed' : ps.length === 1 ? 'focused' : 'unverified',
        reason: g.broad ? 'The live product filter is broader than exact offer IDs.' : ps.length > 1 ? 'Multiple product listings share this group.' : null };
      const w = c.weight || 0, metrics = w ? { impressions: Math.round(900 * w + i * 37), clicks: Math.round(40 * w + i), spend: usd(21.3 * w + i), conversions: Math.round(2 * w * 10) / 10, value: usd(98.4 * w + i * 2) } : { impressions: 0, clicks: 0, spend: 0, conversions: 0, value: 0 };
      return { ref: g.ref, id: g.ref.split('/').pop(), name: g.name, channel: g.channel, campaignId: g.campaignId, campaignName: c.name, campaignStatus: c.status, status: c.status === 'PAUSED' ? 'PAUSED' : 'ENABLED', primaryStatus: c.primaryStatus === 'LIMITED' ? 'LIMITED' : 'ELIGIBLE', urls, mapping, metrics,
        listingMetrics: body && body.reportingTree && g.channel === 'pmax' ? ps.map(p => ({ itemId: offerId(p), metrics: { impressions: 300, clicks: 12, spend: usd(6.1), conversions: 1, value: usd(48.2) }, report: null })) : null,
        serving: c.status !== 'ENABLED' ? 'Campaign ' + c.status.toLowerCase() : c.primaryStatus === 'LIMITED' ? 'LIMITED' : 'ELIGIBLE',
        report: { currency: 'USD', click: { impressions: metrics.impressions, clicks: metrics.clicks, spend: usd(metrics.spend * 0.73), conversions: metrics.conversions, value: usd(metrics.value * 0.73) }, conversion: { impressions: metrics.impressions, clicks: metrics.clicks, spend: usd(metrics.spend * 0.73), conversions: metrics.conversions, value: usd(metrics.value * 0.7) } } };
    }).sort((a, b) => a.campaignName.localeCompare(b.campaignName) || a.name.localeCompare(b.name))
      // Alternate PMax and Search rows so the first rows the walk opens cover both kinds.
      .map((g, i, all) => ({ g, k: all.filter((x, j) => j < i && x.channel === g.channel).length * 2 + (g.channel === 'pmax' ? 0 : 1) })).sort((a, b) => a.k - b.k).map(x => x.g);
    return { ok: true, groups, range, currency: 'CAD', timeZone: ACCOUNT_TZ, basis: 'Ad-click date', checkedAt: now, warnings: body && body.reportingTree ? ['Listing metrics: harness sample warning shown in report coverage.'] : [], complete: true };
  }
  function groupDetail(body) {
    const index = groupsIndex(Object.assign({}, body, { campaignId: body && body.campaignId }));
    const group = index.groups.find(g => g.ref === (body && body.groupRef));
    if (!group) return { ok: false, error: 'The group is outside the selected campaign.' };
    const g = GROUPS.find(x => x.ref === group.ref), ps = g.products.map(k => PRODUCTS[k]);
    const products = ps.map(p => ({ id: 'gid://shopify/Product/' + p.pid, title: p.title, url: productUrl(p), handle: p.handle, offerIds: [offerId(p)], image: productImage(p), description: 'Synthetic listing used by the browser harness.' }));
    const copy = g.channel === 'pmax' ? [{ ref: g.ref, name: g.name, headlines: ['Handmade charm jewelry', 'Gifts she will wear daily', 'Sterling silver & gold fill'], longHeadlines: ['Hand-finished charm necklaces, engraved to order and shipped fast from our studio'], descriptions: ['Free gift wrap on every order.', 'Designed and finished in our studio.'] }]
      : ps.map((p, i) => ({ ref: 'customers/' + CID + '/adGroupAds/' + g.ref.split('/').pop() + '~71000' + i, name: 'Ad 71000' + i, status: 'ENABLED', url: productUrl(p), headlines: [p.title.slice(0, 30), 'Free gift wrap'], descriptions: ['Hand-finished in our studio. Ships in two business days.'] }));
    const split = ps.length > 1 && g.channel === 'pmax';
    return { ok: true, group, products, copy, splitProposals: [], images: g.channel === 'pmax' && ps[0] ? [{ asset: 'customers/' + CID + '/assets/5100001', url: productImage(ps[0], 1), fieldType: 'MARKETING_IMAGE' }] : [],
      keywords: g.channel === 'pmax' ? [{ text: 'charm necklace gift' }, { text: 'personalized necklace' }] : [{ text: (ps[0] || PRODUCTS[0]).handle.replace(/-/g, ' '), matchType: 'EXACT', status: 'ENABLED', negative: false }, { text: 'cheap', matchType: 'BROAD', status: 'ENABLED', negative: true }],
      keywordStatus: 'available', creativeRef: g.channel === 'pmax' ? g.ref : copy[0] && copy[0].ref, range: index.range, currency: 'CAD', timeZone: ACCOUNT_TZ, basis: 'Ad-click date', warnings: [], sourceVersion: 4, snapshotHash: 'a'.repeat(64),
      splitAvailable: split, splitReason: split ? null : g.channel === 'search' ? 'Search groups can be split after each destination has a verified product and keyword plan.' : 'Review exact Merchant offers before splitting a broad product filter.' };
  }

  function diagnostics() {
    const live = CAMPAIGNS.filter(c => c.status !== 'REMOVED' && c.primaryStatus !== 'PENDING');
    return { generatedAt: now - 2 * 3600000, aiError: null,
      campaigns: live.map((c, i) => Object.assign(campaignBase(c), { lostISBudget: 12 + i, lostISRank: 30 - i * 5, impressionShare: 41 + i, avgQualityScore: i === 0 ? 4.5 : 7, adStrength: i === 1 ? { POOR: 1, GOOD: 2 } : { GOOD: 1 }, disapprovedAds: c.primaryStatus === 'NOT_ELIGIBLE' ? 2 : 0,
        // As _diagReasonsText: known codes get a label, unknown ones are lower-cased words.
        reasonsText: [...new Set(c.primaryStatusReasons.filter(r => r !== 'UNKNOWN' && r !== 'UNSPECIFIED').map(r => ({ CAMPAIGN_BUDGET_LIMITED: 'Limited by budget', CAMPAIGN_PAUSED: 'Campaign paused', HAS_ADS_DISAPPROVED: 'Some ads disapproved' })[r] || r.toLowerCase().replace(/_/g, ' ')))],
        recommendations: i === 1 ? [{ type: 'CAMPAIGN_BUDGET', resourceName: 'customers/' + CID + '/recommendations/r' + i, currentBudget: c.budget, recommendedBudget: 21.17, options: [{ budget: 21.17, weeklyClicksDelta: 34, weeklyCostDelta: 30.17 }, { budget: 23.47, weeklyClicksDelta: 45, weeklyCostDelta: 41.27 }] }] : [],
        keywords: [{ text: 'duck necklace', matchType: 'EXACT' }] })),
      ai: { accountSummary: 'Harness summary: budget-limited PMax is the main constraint; Search is healthy.', campaigns: live.map((c, i) => ({ id: c.id, severity: i === 1 ? 'attention' : i === 3 ? 'critical' : 'healthy', verdict: ['agree', 'partial', 'disagree', 'agree'][i % 4], headline: 'Harness headline for ' + c.name, googleSays: i === 1 ? 'Raise the budget' : 'none', aiSays: 'Harness specialist read: keep the current structure and watch the impression share for a week.', findings: ['Lost impression share to budget on weekends.', 'Two headlines rated LOW by Google.'], action: 'Watch', fixReview: i === 0 ? [{ applied: 'Added negative keywords', working: 'working', daysAgo: 6, note: 'CPC fell 11%.' }] : [],
        remedies: [{ issue: 'Weak headline coverage', fix: 'Append two specific headlines', why: 'Ad strength is AVERAGE', executable: { kind: 'rewriteAds', headlines: ['Engraved Duck Charm', 'Gift-Ready Packaging'], descriptions: [] } }, { issue: 'Irrelevant searches', fix: 'Add negatives', executable: { kind: 'addNegatives', keywords: ['free', 'diy'] } }] })) } };
  }

  function opportunities(body) {
    const mk = (i, over) => Object.assign({ tag: 'harness-opp-' + i, occasion: ['Harness Holiday Gifts', 'Harness Valentine Initials', 'Harness Graduation Charms'][i], collectionTitle: ['Charm Necklaces', 'Initial Necklaces', 'Custom Charms'][i], priority: ['high', 'medium', 'low'][i], score: 81 - i * 9,
      rationale: 'Harness rationale: measured demand rises four weeks before the occasion and past campaigns converted at 3x.', startDate: addDays(today, 3 + i * 10), endDate: addDays(today, 40 + i * 10), durationDays: 37, daysOut: 3 + i * 10,
      recommendedDailyBudget: [11.27, 12.07, 7.47][i], estTotalSpend: [410.47, 370.47, 330.47][i], currency: 'CAD', expectedRoasBand: [2.1, 3.4], confidence: { score: 72 - i * 5 }, pastStats: i === 0 ? { roas: 3.2 } : null, proven: i === 0,
      eligibility: { ready: i !== 2, measuredKeywords: 12 - i * 4 }, research: { searchVolume: 5400 - i * 1000, competitionIndex: 38 + i * 10, realCount: 12 - i * 4, source: 'keyword_planner' }, plan: { cpc: 0.92 + i * 0.1 },
      keyPhrases: ['initial necklace', 'charm necklace gift'], keywordData: [{ text: 'initial necklace', volume: 2900, cpcLow: 0.61, cpcHigh: 1.42, competition: 'MEDIUM' }],
      audience: { buyer: 'Gift buyers', recipient: 'Partner', motivation: 'Personal meaning', searchStyle: 'initial necklace for her' }, acted: null }, over || {});
    const list = [mk(0), mk(1), mk(2, { acted: { status: 'PENDING', approvalId: 'harness-approval-search', at: now - DAY } })];
    return { opportunities: list, scannedAt: now - 3 * 3600000, scanning: false, progress: null, lastError: null, researchStatus: { search: { ok: true, at: now - 3 * 3600000 }, pmax: { ok: true, at: now - 3 * 3600000 } },
      pmaxList: [], pmaxError: null, pmaxAt: now - 3 * 3600000, searchLearning: null, pmaxLearning: null, reconciliation: null,
      scanAudit: { schema: 2, runId: 'opp-harness', status: 'completed', startedAt: now - 3 * 3600000, completedAt: now - 3 * 3600000 + 42000, updatedAt: now - 3 * 3600000 + 42000, summary: { total: 3, ok: 2, warning: 1, failed: 0, skipped: 0, running: 0, queued: 0 },
        checks: [{ id: 'console_request', category: 'orchestration', label: 'Console scan request', status: 'ok', tookMs: 12, detail: 'Accepted.' }, { id: 'keyword_planner', category: 'research', label: 'Keyword Planner', status: 'warning', tookMs: 5400, detail: 'Harness: two keywords returned no volume.' }, { id: 'catalog', category: 'store', label: 'Catalog read', status: 'ok', tookMs: 800, detail: '5 products.' }] },
      started: !!(body && body.force), runId: body && body.force ? 'opp-harness-run' : undefined };
  }

  function playbook() {
    const lesson = (i, over) => Object.assign({ id: 'lesson-' + i, rule: ['Lead Search headlines with the product noun, then the gift angle.', 'Use square lifestyle images for Product ads in Q4.', 'Exclude “free” and “DIY” searches for charm campaigns.'][i], category: ['copy', 'images', 'negatives'][i], scope: ['global', 'jewelryType:necklace', 'collection:charms'][i], channels: [['search'], ['pmax'], ['search']][i], eligible: i !== 1, evidenceVerified: i !== 1, support: 3 - i,
      evidence: 'Harness evidence: 3 campaigns, 60 days.', usage: { draftCount: 2 - (i % 2), publishedCount: i === 0 ? 1 : 0, researchCount: 1, stages: ['search_generation'], records: [{ campaignId: '9100000001', at: now - 5 * DAY, stage: 'search_generation' }] },
      outcome: i === 0 ? { status: 'improved', summary: 'CPA fell after adoption.', comparisons: [{ metric: 'cpa', before: { cpa: 18.2, roas: 2.1 }, after: { cpa: 14.1, roas: 2.9 }, campaignId: '9100000001' }] } : { status: 'pending' } }, over || {});
    return { version: 4, updatedAt: now - 6 * DAY, lessons: [lesson(0), lesson(1), lesson(2)], retired: [{ rule: 'Harness retired rule', why: 'Contradicted by later results.' }], changeLog: 'Harness: added negatives lesson.', measurement: { measured: 1, pending: 2 } };
  }

  function studioStatus() {
    return { ok: true, scannedAt: now - 2 * DAY, scanning: false,
      blueprint: { positioning: { promise: 'Turn what matters into a charm you design' }, page: { source: 'Harness scan' }, budget: { recommendedDaily: 21.17, ceiling: 170, headroom: 99.17, pmaxDaily: 8.47, searchDaily: 12.77, countries: ['2124', '2840'] },
        pmax: { groups: [{ name: 'Photo to charm', angle: 'Upload a photo', searchThemes: ['photo charm', 'custom charm'] }, { name: 'Draw your own', angle: 'Sketch it', searchThemes: ['drawing charm'] }] }, measurement: { readiness: { purchaseReady: true, apiOk: true, assistCoverage: 3, start: ['a'], design: ['b'], approve: [], cart: ['c'], purchase: ['d'] } } },
      lanes: { pmax: { where: 'campaign', status: 'PAUSED', campaignId: '9100000003' }, search: { where: 'none' } },
      performance: { ok: true, start: addDays(today, -29), end: today, overall: { cost: 212.4, clicks: 380 }, purchase: { cpa: 42.4, roas: 1.8 }, funnel: { start: 120, design: 64, approve: 0, cart: 12, purchase: 5, available: { start: true, design: true, approve: false, cart: true, purchase: true } }, rates: { visitToStart: 0.32 } },
      learning: { phase: 'learning', recommendations: [{ priority: 'high', area: 'Creative', observation: 'Photo angle leads', action: 'Test a second photo-led asset group.' }] } };
  }

  function campaignVersions(body) {
    const id = String(body && (body.id || body.campaignId) || '');
    const before = body && body.beforeVersion;
    const top = before ? Number(before) - 1 : 6;
    const versions = [];
    for (let v = top; v >= Math.max(1, top - 2); v--) versions.push({ version: v, updatedAt: now - (7 - v) * 3 * DAY, source: v === 6 ? 'console' : 'observed', summary: v === 1 ? '' : 'Harness change v' + v, baseline: v === 1, categories: v === 1 ? [] : ['budget', 'copy'], changes: v === 1 ? [] : [{ category: 'Budget', summary: 'Daily budget ' + (v === 6 ? '11.27 → 13.37 CAD' : 'unchanged') }], restorable: v === 5, restorationReason: v === 5 ? null : 'Historical metadata only; a restorable content snapshot is not available.' });
    return { ok: true, id, currentVersion: 6, coverage: 'History starts with the first observed version (harness).', warnings: [], versions, hasMore: top - 2 > 1, nextBeforeVersion: top - 2 };
  }
  function campaignImprovement(body) {
    return { ok: true, campaignId: String(body && body.campaignId || ''), status: 'ready', report: { id: 'harness-improvement', createdAt: now - DAY, summary: 'Harness: raise the budget on the best asset group and add two headlines.', actions: [{ id: 'act-1', title: 'Add two headlines', detail: 'Two LOW-rated headlines replaced.', kind: 'copy', draftable: true }, { id: 'act-2', title: 'Raise budget to 16.47 CAD', detail: 'Budget-limited on weekends.', kind: 'budget', draftable: false }] }, facts: [] };
  }
  // servingCheck in googleAdsAutopilot.js: { serving } is the read-only check of _googleAdsServing.js, nothing else.
  function servingCheck(body) {
    const id = String(body && body.id || ''), c = CAMPAIGNS.find(x => x.id === id) || CAMPAIGNS[0];
    const verdict = c.primaryStatus === 'NOT_ELIGIBLE' ? 'blocked' : c.primaryStatus === 'LIMITED' ? 'attention' : 'ready';
    const findings = (verdict === 'blocked' ? [{ level: 'block', area: 'Ads', text: '1 ad disapproved in "Best sellers · necklaces".', reason: 'Trademarks in ad text', fix: 'Edit the ad text and send it for review again.' }]
      : verdict === 'attention' ? [{ level: 'risk', area: 'Budget', text: 'The budget limits how often the ads show.', fix: 'Raise the daily budget or narrow the targeting.' }] : [])
      .concat([{ level: 'note', area: 'Ads', text: '1 ad still under Google review.' }]);
    const counts = { block: 0, risk: 0, note: 0 }; findings.forEach(f => { counts[f.level]++; });
    return { serving: { ok: true, readOnly: true, campaignId: c.id, channel: c.channel, apiVersion: 'v24', checkedAt: new Date(now).toISOString(), partial: false, settling: false, quotaExhausted: false, warnings: [], verdict, counts, findings,
      headline: verdict === 'blocked' ? 'Will not serve as intended: ' + findings[0].text : verdict === 'attention' ? '1 setting to fix before enabling.' : 'Google reports nothing that would stop it serving as intended.',
      facts: [{ label: 'Status', value: c.status === 'ENABLED' ? 'enabled' : c.status === 'PAUSED' ? 'paused' : 'removed' }, { label: 'Networks', value: c.channel === 'SEARCH' ? 'Google Search' : 'all Google channels' }] } };
  }
  function adDesignWorkspace(body) {
    const g = GROUPS.find(x => x.ref === (body && body.groupRef)) || GROUPS[2], ps = (g.products.length ? g.products : [0]).map(k => PRODUCTS[k]);
    const products = ps.map(p => ({ id: 'gid://shopify/Product/' + p.pid, title: p.title, description: 'Synthetic product facts for the harness.', url: productUrl(p), images: [0, 1, 2].map(i => ({ id: p.pid + '-im' + i, url: productImage(p, i), alt: p.title + ' photo ' + (i + 1) })) }));
    const c = CAMPAIGNS.find(x => x.id === g.campaignId);
    return { ok: true, workspaceId: 'harness-workspace-' + g.ref.split('/').pop(), sourceVersion: 4, snapshotHash: 'b'.repeat(64),
      context: { gallerySchema: 2, gallerySources: [], campaignId: g.campaignId, campaignName: c.name, scopeProductId: products[0].id, groups: GROUPS.filter(x => x.campaignId === g.campaignId).map(x => ({ ref: x.ref, name: x.name, url: x.products.length ? productUrl(PRODUCTS[x.products[0]]) : SHOP })) },
      products, references: [], settings: { productId: products[0].id, sourceImageId: products[0].images[0].id, groupRef: g.ref, referenceIds: [], direction: 'Keep the charm recognisable', style: { background: 'warm ivory', font: 'Montserrat', border: 'none', scale: 'close detail' }, formats: ['square', 'landscape', 'portrait'] },
      phase: 'draft', status: 'draft', imageLibrary: [], savedDesigns: [],
      provider: { available: true, label: 'Harness image model', quality: 'high', formats: [{ key: 'square', label: 'Square', required: true }, { key: 'landscape', label: 'Landscape', required: true }, { key: 'portrait', label: 'Portrait', required: true }] } };
  }

  // A job id answers "done" on its second status read so polling loops end fast.
  // The PMax image backfill (genId "...-pmxbf") answers with the fields the
  // background task spreads into its status: draftPmaxRefresh's
  // {ok, queued, results, campaigns} and, while running, {campaign, assetGroup, done, total}.
  function genStatus(body) {
    const id = String(body && body.genId || ''), n = (state.genPolls.get(id) || 0) + 1;
    state.genPolls.set(id, n);
    if (/-pmx(bf|up)$/.test(id)) {
      if (n < 2) return { phase: 'running', campaign: CAMPAIGNS[1].name, assetGroup: GROUPS[2].name, done: 0, total: 3, heartbeatAt: Date.now() };
      return { ok: true, queued: 2, campaigns: 2, results: [{ campaign: CAMPAIGNS[1].name, assetGroup: GROUPS[2].name, approvalId: 'harness-approval-refresh-1' }, { campaign: CAMPAIGNS[1].name, assetGroup: GROUPS[3].name, approvalId: 'harness-approval-refresh-2' }, { campaign: CAMPAIGNS[5].name, assetGroup: GROUPS[7].name, skipped: 'A creative refresh is already awaiting review.' }] };
    }
    if (n < 2) return { pending: false, phase: 'running', ok: undefined, label: 'Harness job running', progress: { pct: 40, label: 'Harness step', detail: 'Synthetic progress' }, pct: 40, updatedAt: Date.now() };
    return { pending: false, phase: 'done', ok: true, result: { ok: true, reason: 'Harness job finished.', changeLog: 'No change (harness).', unchanged: true }, approvalId: 'harness-approval-search', done: 1, total: 1 };
  }

  // creativeApprovalStatus shape: {ok,id,status,summary,creative,imageAccess,leaseUntil,current}.
  // A package is "not_started" until creativePrepare runs, then "ready".
  function readyCreative(approval) {
    const p = PRODUCTS[2];
    return { phase: 'ready', revision: 1, payloadHash: 'd'.repeat(64), progress: { pct: 100, label: 'Creative package ready' }, imageSpendUsd: 2.4, allowanceUsd: 8, playbookVersion: 4,
      groups: [{ key: 'g0', channel: 'pmax', name: p.title, url: productUrl(p), sourceUrl: productImage(p, 3), sourceTitle: p.title,
        brief: { buyer: 'Gift buyers shopping for a partner', hypothesis: 'Close-up product shots beat lifestyle shots for this item.', successMetric: 'Purchase CPA / ROAS', rationale: 'Harness rationale: the product photo is the strongest signal in past tests.' },
        copy: { headlines: ['Moon Disc Bracelet', 'Handmade Celestial Gift', 'Sterling Silver Moon'], longHeadlines: ['A hand-finished moon disc bracelet, engraved to order and shipped from our studio'], descriptions: ['Free gift wrap on every order.', 'Designed and finished in our studio in two business days.'] },
        assets: { square: { url: productImage(p, 0), width: 1200, height: 1200 }, landscape: { url: productImage(p, 1), width: 1200, height: 628 }, portrait: { url: productImage(p, 2), width: 960, height: 1200 } },
        keywords: ['moon bracelet', 'disc bracelet gift'], review: { pass: true, score: 84 } }],
      summary: approval && approval.summary };
  }
  function creativeStatus(body) {
    const id = String(body && body.id || ''), a = (state.pending || pendingApprovals()).concat(stuckApprovals()).find(x => x.id === id);
    if (!a) return { error: 'Draft not found.' };
    const creative = state.creative.get(id) || Object.assign({ phase: 'not_started' }, a.creative || {});
    return { ok: true, id, status: a.status, summary: a.summary, creative, imageAccess: { ok: true }, leaseUntil: 0, current: true };
  }

  const read = {
    dashboard,
    metricsRange: body => { const range = validRange(body, 7); return Object.assign({ ok: true, snapshot: rangeRows(range.start, range.end), range }, context(), { currency: 'USD', fxIncomplete: false, cdAvailable: true, scheduleAvailable: true, includesRemovedWithActivity: true, warnings: [] }); },
    dailyStats,
    adGroups: groupsIndex,
    adGroupDetail: groupDetail,
    adDesignSavedWorkspaces: () => ({ ok: true, workspaces: [{ workspaceId: 'harness-workspace-8200000021', name: 'Best sellers · necklaces', productId: 'gid://shopify/Product/71000001', campaignId: '9100000002', groupRef: GROUPS[2].ref, sourceVersion: 4, updatedAt: now - 2 * DAY }], nextCursor: null }),
    diagnostics,
    diagRunStatus: () => ({ ok: true, phase: 'done', done: 4, total: 4 }),
    remedyHistory: () => ({ items: [{ campaignId: '9100000001', kind: 'addNegatives', issue: 'Irrelevant searches', fix: 'Add negatives', at: now - 6 * DAY, verified: true, executable: { kind: 'addNegatives', keywords: ['free', 'diy'] } }] }),
    adReviewStatus: body => ({ statuses: Object.fromEntries((body && body.adIds || []).map(id => [id, { approvalStatus: 'APPROVED', reviewStatus: 'REVIEWED' }])) }),
    countries: () => ({ ok: true, list: COUNTRIES }),
    collections: () => ({ collections: [{ handle: 'charm-necklaces', title: 'Charm Necklaces', count: 24 }, { handle: 'initial-necklaces', title: 'Initial Necklaces', count: 12 }] }),
    occasions: () => ({ occasions: [{ id: 'holiday', label: 'Holiday gifting', why: 'Harness: demand rises from early November.', peakDate: addDays(today, 60) }, { id: 'birthday', label: 'Birthdays', why: 'Evergreen.' }] }),
    opportunities,
    playbook,
    // playbookVersions shape: {items:[{id:"v<version>-<ms>",version,updatedAt,changeLog,lessons,archived}],currentVersion,archiveAvailable}
    playbookVersions: () => ({ items: [{ id: null, version: 4, updatedAt: now - 6 * DAY, changeLog: 'Harness: added negatives lesson.', lessons: 3, current: true, archived: false }, { id: 'v3-' + (now - 20 * DAY), version: 3, updatedAt: now - 20 * DAY, changeLog: 'Older harness change.', lessons: 2, archived: true }], currentVersion: 4, archiveAvailable: true }),
    designStudioStatus: studioStatus,
    conversionHealth: () => conversionHealth(),
    campaignVersions,
    campaignVersionDetail: body => ({ ok: true, id: String(body && body.id || ''), version: Number(body && body.version) || 6, snapshot: { complete: true, campaign: { name: 'Harness snapshot', status: 'ENABLED' }, components: { searchAds: [], assetGroups: [], assetLinks: [] } }, restorable: Number(body && body.version) === 5, summary: 'Harness version detail' }),
    campaignImprovement,
    servingCheck,
    keywordDiag: () => ({ ok: true, source: 'keyword_planner', keyword: 'charm necklace', rows: [{ text: 'charm necklace', volume: 12100, competition: 'HIGH', cpcLow: 0.55, cpcHigh: 1.62 }], checks: [{ label: 'OAuth', ok: true }, { label: 'Keyword Planner access', ok: true }] }),
    genStatus,
    approvalStatus: body => ({ ok: true, id: body && body.id, status: 'APPLIED', error: null, validatedAt: now, startedAt: now }),
    creativeStatus,
    analyzeAdStatus: body => ({ ok: true, analysisId: body && body.analysisId, status: 'complete', phase: 'done', analysis: { summary: 'Harness ad analysis.', score: 71, findings: [] } }),
    adVersionApprovalStatus: body => ({ ok: true, id: body && body.id, status: 'PENDING' }),
    pmaxRecommendationEvidence: () => ({ ok: true, products: [], evidence: [] }),
    adDesignStatus: adDesignWorkspace,
    adDesignSavedDesigns: () => ({ ok: true, designs: [], nextCursor: null }),
    adDesignProductImages: () => ({ ok: true, images: [] }),
    adDesignGooglePreview: () => ({ ok: true, previews: [] }),
    adDesignDelivery: () => ({ ok: true, delivery: null }),
    adDesignGalleryPage: () => ({ ok: true, items: [], nextCursor: null }),
    adDesignEditorSource: () => ({ ok: false, error: 'Harness: the editor source is not simulated.' }),
    // editorState / editorAIStatus in googleAdsAdDesign.js: no saved board and no AI attempt yet.
    adDesignEditorState: () => ({ ok: true, savedDesigns: [], design: null, sources: [], exports: [], designs: [] }),
    adDesignEditorAIStatus: () => ({ ok: true, jobId: null, phase: 'idle', result: null }),
    monthlyGuard: () => ({ ok: true, tripped: false }),
    measureNow: () => ({ ok: true, campaigns: CAMPAIGNS.length - 1, at: Date.now() })
  };

  // Mutations only ever touch this in-memory state.
  const write = {
    kill: () => { state.control.enabled = false; return { enabled: false }; },
    resume: () => { state.control.enabled = true; return { enabled: true }; },
    dryRun: body => { state.control.dryRun = !!(body && body.on); return { dryRun: state.control.dryRun }; },
    setControl: body => { const patch = Object.assign({}, body && body.patch); Object.assign(state.control, patch); return { patched: patch }; },
    clearLedger: () => { const n = (state.ledger || []).length; state.ledger = []; return { ok: true, deleted: n }; },
    reject: body => { state.pending = (state.pending || []).filter(a => a.id !== (body && body.id)); return { ok: true, deleted: true }; },
    deleteCampaign: body => { state.deleted.add(String(body && body.id)); return { ok: true, removed: true }; },
    enforceCeiling: () => ({ ok: true, withinCeiling: true, total: CAD_BUDGET_TOTAL, ceiling: state.control.maxDailyBudgetTotal }),
    runDiagnostics: body => ({ queued: true, runId: String(body && body.runId || Date.now()), upstream: 202 }),
    runNow: body => ({ status: 'kicked', tasks: body && body.tasks || [], upstream: 202 }),
    generate: () => ({ queued: true, genId: 'harness-gen-' + (++state.diagRun), upstream: 202 }),
    generatePmax: () => ({ queued: true, genId: 'harness-pmax-' + (++state.diagRun) }),
    designStudioScan: () => ({ ok: true, queued: true, genId: 'studio-scan-' + (++state.diagRun) }),
    designStudioGenerate: () => ({ ok: true, queued: true, genId: 'studio-gen-' + (++state.diagRun) }),
    designStudioAnalyze: () => ({ ok: true, queued: true, genId: 'studio-an-' + (++state.diagRun) }),
    distill: body => ({ queued: true, genId: String(body && body.genId || 'learning-harness') }),
    pmaxBackfillImages: () => ({ queued: true, genId: 'harness-backfill-' + (++state.diagRun) }),
    pmaxUpgradeAdStrength: () => ({ queued: true, genId: 'harness-upgrade-' + (++state.diagRun) }),
    approve: body => ({ queued: true, id: String(body && body.id) }),
    apply: body => ({ queued: true, id: String(body && body.id) }),
    creativePrepare: body => { const id = String(body && body.id), a = (state.pending || []).find(x => x.id === id); state.creative.set(id, readyCreative(a)); return { queued: true, id }; },
    syncConversions: () => ({ ok: true, uploaded: { uploaded: 2, submitted: 3, processing: 1, errors: [] }, adjustments: { uploaded: 1 }, health: conversionHealth() }),
    backfillOrders: () => ({ ok: true, added: 3, skipped: 12 }),
    setBudget: body => ({ ok: true, verified: true, budget: Number(body && body.budget), id: body && body.id }),
    setStatus: body => ({ ok: true, verified: true, status: body && body.status, id: body && body.id }),
    setEndDate: body => ({ ok: true, verified: true, endDate: body && body.endDate || null, id: body && body.id }),
    startNow: body => ({ ok: true, verified: true, id: body && body.id }),
    setCountries: body => ({ ok: true, verified: true, countries: body && body.countries || [] }),
    setApprovalDates: body => ({ ok: true, startDate: body && body.startDate, endDate: body && body.endDate }),
    setApprovalCountries: body => ({ ok: true, countries: body && body.countries || [] }),
    retryStuck: () => ({ ok: true, retried: 1 }),
    deleteOpportunity: () => ({ ok: true }),
    releaseOpportunity: () => ({ ok: true }),
    draftAdGroupSplit: () => ({ ok: true, approvalId: 'harness-approval-split', groups: [], summary: 'New groups start paused.' }),
    draftAdGroupActivation: () => ({ ok: true, approvalId: 'harness-approval-activation', changes: [] }),
    resetAdDesignFailures: () => ({ ok: true, reset: 0, workspaces: 0 }),
    restorePlaybook: () => ({ ok: true }),
    applyRec: () => ({ ok: true, verified: true }),
    applyRemedy: () => ({ ok: true, verified: true, verification: 'Harness read-back matched.' }),
    dismissRec: () => ({ ok: true }),
    reviewCreative: () => ({ ok: true }),
    creativeRevise: () => ({ queued: true }),
    reviewAdVersion: () => ({ ok: true }),
    createCampaignRestoreDraft: () => ({ ok: true, approvalId: 'harness-approval-restore' }),
    createImprovementDraft: () => ({ ok: true, approvalId: 'harness-approval-improvement' }),
    saveDataManagerConnection: () => ({ ok: true }),
    beginAnalyzeAd: body => ({ ok: true, analysisId: 'harness-analysis-' + (body && (body.campaignId || body.id)), status: 'queued' }),
    adDesignWorkspace,
    saveAdDesign: body => Object.assign(adDesignWorkspace(body), { savedAt: Date.now() }),
    uploadAdDesignReference: () => ({ ok: true, reference: null }),
    startAdDesign: () => ({ ok: true, queued: true, jobId: 'harness-job' }),
    cropAdDesignImage: () => ({ ok: true }),
    deleteAdDesignSavedDesign: () => ({ ok: true }),
    deleteAdDesignGeneratedImage: () => ({ ok: true }),
    openAdDesignSavedDesign: () => ({ ok: true }),
    saveAdDesignCopy: () => ({ ok: true }),
    prepareAdDesignPublication: () => ({ ok: true, status: 'PENDING', approvalId: 'harness-approval-design', reviewHash: 'c'.repeat(64) }),
    publishAdDesignSubmission: () => ({ ok: true, status: 'PENDING' }),
    publishAdDesignPublication: () => ({ ok: true, status: 'PENDING' })
  };

  function respond(action, body) {
    if (read[action]) return { kind: 'read', body: read[action](body || {}) };
    if (write[action]) return { kind: 'write', body: write[action](body || {}) };
    return null;
  }
  return { respond, today, state, cadTracers: cadTracers() };
}

module.exports = { createFixtures, cadTracers, CAMPAIGNS, PRODUCTS, GROUPS, ACCOUNT_TZ };
