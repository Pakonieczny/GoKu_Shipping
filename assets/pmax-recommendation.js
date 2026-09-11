/* Shared, deterministic recommendation explanations. No provider calls or invented benchmarks. */
(function (root, factory) {
  if (typeof module === 'object' && module.exports) module.exports = factory();
  else root.BritesPmaxRecommendation = factory();
})(typeof globalThis !== 'undefined' ? globalThis : this, function () {
  'use strict';
  var number = function (v) { return Number.isFinite(Number(v)) ? Math.max(0, Number(v)) : 0; };
  var round = function (v, digits) { var p = Math.pow(10, digits == null ? 2 : digits); return Math.round(v * p) / p; };
  var unique = function (v) { return Array.from(new Set(v)); };
  function productKey(offer) {
    var m = String(offer.itemId || '').match(/^shopify_[A-Z]{2}_(\d+)_(\d+)$/i);
    return String(offer.productId || (m && m[1]) || offer.itemId || '');
  }
  function paidTotal(rows, meta) {
    meta = meta || {};
    var r = { impressions: 0, clicks: 0, conversions: 0, cost: 0, value: 0, days: meta.days || 90,
      currency: meta.currency || 'USD', available: meta.available === true, monetaryComplete: meta.monetaryComplete !== false };
    rows.forEach(function (x) {
      ['impressions', 'clicks', 'conversions', 'cost', 'value'].forEach(function (key) { r[key] += number(x[key]); });
      if (x.monetaryComplete === false || (x.currency && x.currency !== r.currency)) r.monetaryComplete = false;
    });
    if (!r.monetaryComplete) { r.cost = null; r.value = null; }
    return r;
  }
  function freeTotal(rows) {
    var r = { available: rows.length > 0 && rows.every(function (x) { return x.available === true; }), impressions: 0, clicks: 0, conversions: 0, value: 0, valueComplete: true };
    rows.forEach(function (x) { ['impressions', 'clicks', 'conversions', 'value'].forEach(function (k) { r[k] += number(x[k]); }); if (x.valueComplete === false) r.valueComplete = false; });
    if (!r.valueComplete) r.value = null;
    return r;
  }
  function forecast(candidate, offers, scope, budget, days) {
    var currency = candidate.budgetCurrencyVerified === false ? 'Unverified' : candidate.budgetCurrency || 'USD', missing = [], exact = paidTotal(offers.map(function (x) { return x.paidPerformance || {}; }), candidate.paidPerformance);
    var choices = [];
    if (!scope.legacy) choices.push(Object.assign({}, exact, { scope: 'selected_products', label: 'Selected products · paid PMax history' }));
    (candidate.forecastBaselines || []).forEach(function (b) { choices.push(b); });
    // Old aggregate histories can still be inspected as a broader benchmark, never as selected-product proof.
    if (scope.legacy && candidate.paidPerformance) choices.push(Object.assign({}, candidate.paidPerformance, { scope: 'saved_candidate', label: 'Saved suggestion history · exact selected scope unverified' }));
    var fx = number(candidate.budgetToEvidenceFx);
    choices = choices.map(function (x) {
      if (x.currency === currency) return x;
      if (x.currency === 'USD' && currency !== 'USD' && fx > 0) return Object.assign({}, x, { currency: currency, sourceCurrency: 'USD', cost: x.cost == null ? null : x.cost / fx, value: x.value == null ? null : x.value / fx, budgetFxDate: candidate.budgetFxDate || null, budgetToEvidenceFx: fx });
      return x;
    });
    if (candidate.budgetCurrencyVerified === false) missing.push('The Google Ads budget currency could not be verified. Refresh evidence before comparing the planned spend with reported revenue or projecting performance.');
    var valid = choices.filter(function (x) { return candidate.budgetCurrencyVerified !== false && x.available === true && x.monetaryComplete !== false && x.currency === currency && number(x.clicks) >= 20 && number(x.cost) > 0; });
    var b = valid.find(function (x) { return number(x.clicks) >= 50 && number(x.conversions) >= 5 && number(x.conversions) <= number(x.clicks); }) || valid[0] || null;
    var rates = { ctr: null, cpc: null, cvr: null, aov: null };
    if (b) {
      rates.cpc = number(b.cost) / number(b.clicks);
      if (number(b.impressions) >= 100 && number(b.clicks) <= number(b.impressions)) rates.ctr = number(b.clicks) / number(b.impressions);
      if (number(b.clicks) >= 50 && number(b.conversions) >= 5 && number(b.conversions) <= number(b.clicks)) rates.cvr = number(b.conversions) / number(b.clicks);
      if (rates.cvr != null && number(b.value) > 0) rates.aov = number(b.value) / number(b.conversions);
    }
    if (currency !== 'USD' && !fx && !choices.some(function (x) { return x.currency === currency; })) missing.push('A current ' + currency + ' to USD exchange rate is unavailable; USD evidence is not compared with a different-currency budget.');
    if (rates.cpc == null) missing.push('A paid CPC needs at least 20 observed clicks, verified spend and matching currency. Organic clicks cannot supply an ad auction price.');
    if (rates.ctr == null) missing.push('A paid CTR needs at least 100 reported impressions with consistent click counts.');
    if (rates.cvr == null) missing.push('Conversion projections need at least 50 paid clicks and 5 reported conversions in the same history. Free-listing conversions are never divided by organic clicks to manufacture a paid conversion rate.');
    if (rates.aov == null) missing.push('Revenue projections need verified paid conversion value and a usable conversion rate.');
    var scenarios = [];
    if (b && budget > 0 && days > 0 && scope.selectedItemIds.length) {
      [{ label: 'Conservative', cpc: 1.25, cvr: 0.75 }, { label: 'At observed rates', cpc: 1, cvr: 1 }, { label: 'Upside', cpc: 0.8, cvr: 1.25 }].forEach(function (s) {
        var spend = budget * days, clicks = spend / (rates.cpc * s.cpc), conversions = rates.cvr == null ? null : clicks * Math.min(1, rates.cvr * s.cvr), revenue = conversions == null || rates.aov == null ? null : conversions * rates.aov;
        scenarios.push({ label: s.label, spend: round(spend), impressions: rates.ctr ? Math.round(clicks / rates.ctr) : null, clicks: Math.round(clicks), conversions: conversions == null ? null : round(conversions), revenue: revenue == null ? null : round(revenue), roas: revenue == null ? null : round(revenue / spend), cpc: round(rates.cpc * s.cpc, 4), cvr: rates.cvr == null ? null : round(Math.min(1, rates.cvr * s.cvr), 6) });
      });
    }
    return { status: !scope.selectedItemIds.length ? 'no_selection' : !b ? 'insufficient_evidence' : rates.cvr == null || rates.aov == null ? 'partial' : 'conditional',
      currency: currency, budget: budget, days: days, spendCeiling: round(budget * days), baseline: b, rates: rates, scenarios: scenarios, missing: missing, budgetToEvidenceFx: fx || null, budgetFxDate: candidate.budgetFxDate || null,
      methodology: 'Conditional planning scenarios, not a Google forecast, guaranteed sales, or a confidence interval. Assumes the planned budget is spent and historical traffic quality persists. Clicks = spend / paid CPC; conversions = clicks × paid conversion rate; revenue = conversions × recorded value per conversion; impressions = clicks / paid CTR. Sensitivity varies CPC +25%/−20% and conversion rate −25%/+25%, holding CTR and value per conversion fixed. PMax reported conversions may include non-purchase goals; these are not verified incremental orders. Actual delivery, conversion lag and competition can change every result.' + (b && b.sourceCurrency ? ' USD history is converted into the budget currency using the verified rate for ' + (b.budgetFxDate || 'the current reporting date') + '; this planning conversion is not a reconstruction of historical native-currency payments.' : '') };
  }
  function seasonality(rows, candidate, now, legacy) {
    var map = {}, coverage = candidate.seasonalityCoverage || {}, totals = { orders: 0, orders30d: 0, revenue: 0, revenue30d: 0 };
    rows.forEach(function (p) {
      Object.keys(totals).forEach(function (k) { totals[k] += number(p[k]); });
      (p.monthly || []).forEach(function (m) { if (!/^\d{4}-\d{2}$/.test(String(m.month))) return; var a = map[m.month] || (map[m.month] = { month: m.month, orders: 0, revenue: 0 }); a.orders += number(m.orders); a.revenue += number(m.revenue); });
    });
    var current = new Date(now).toISOString().slice(0, 7), months = Object.keys(map).sort().map(function (k) { return Object.assign(map[k], { partial: k === current || coverage.complete !== true || (coverage.startAt && Number(coverage.startAt) > Date.parse(k + '-01T00:00:00Z')), observed: true }); });
    var trend = null, recentComplete = candidate.demandCoverage && candidate.demandCoverage.days30 === true && candidate.demandCoverage.days90 === true;
    if (!legacy && recentComplete && totals.orders >= totals.orders30d && candidate.evidenceDays === 90) {
      var prior = totals.orders - totals.orders30d, rate = prior > 0 ? (totals.orders30d / 30) / (prior / 60) : null;
      trend = { recentOrders: totals.orders30d, previous60Orders: prior, dailyRateRatio: rate == null ? null : round(rate), direction: rate == null ? (totals.orders30d ? 'new_recent_demand' : 'no_observed_demand') : rate > 1.2 ? 'rising' : rate < 0.8 ? 'falling' : 'steady', basis: 'Non-overlapping comparison: latest 30 days versus the previous 60 days, using product-order matches per day.' };
    }
    var peak = months.filter(function (m) { return !m.partial; }).sort(function (a, b) { return b.orders - a.orders; })[0];
    var detail = legacy ? 'Refresh research to retrieve monthly history for these exact products. The saved suggestion does not establish their seasonal timing.' : months.length ? months.length + ' month(s) contain recorded sales for the selected products.' + (peak ? ' The largest observed complete-month count is ' + peak.month + ' (' + peak.orders + ' product-order matches).' : ' Available monthly counts include incomplete coverage, so a seasonal peak cannot be established.') : 'No monthly order history is available for these exact products.';
    if (trend) detail += trend.dailyRateRatio == null ? ' Latest 30 days: ' + trend.recentOrders + ' product-order matches; previous 60 days: ' + trend.previous60Orders + '. No percentage increase is inferred from a zero baseline.' : ' Recent daily purchase pace is ' + trend.dailyRateRatio + '× the preceding 60 days (' + trend.recentOrders + ' versus ' + trend.previous60Orders + ' product-order matches).';
    detail += ' These data do not include a matched multi-year comparison; no recurring seasonal uplift or holiday lift is assumed. Use a demand-led test, and reassess timing when product-level history or dated search-demand evidence supports it.';
    return { status: legacy || !months.length ? 'insufficient_history' : 'observed_demand', title: 'Why now · demand and seasonality', detail: detail, months: months, trend: trend, coverage: coverage, totals: totals };
  }
  function buildRecommendation(candidate, options) {
    candidate = candidate || {}; options = options || {};
    var now = Number(options.now) || Date.now(), allIds = unique((candidate.itemIds || []).map(String)), selectedIds = unique((options.selectedItemIds == null ? allIds : options.selectedItemIds).map(String)).filter(function (id) { return allIds.indexOf(id) >= 0; });
    var allOffers = (candidate.offerDetails || []).filter(function (p) { return allIds.indexOf(String(p.itemId)) >= 0; }), offers = allOffers.filter(function (p) { return selectedIds.indexOf(String(p.itemId)) >= 0; });
    var legacy = Number(candidate.recommendationSchema) !== 1 || !Array.isArray(candidate.demandEvidence), productMap = {};
    offers.forEach(function (p) { var key = productKey(p), row = productMap[key] || (productMap[key] = { productId: key, title: p.productTitle || p.title || 'Verified feed offer', itemIds: [] }); row.itemIds.push(String(p.itemId)); });
    selectedIds.filter(function (id) { return !offers.some(function (p) { return String(p.itemId) === id; }); }).forEach(function (id) { productMap['offer:' + id] = { productId: null, title: 'Feed offer ' + id, itemIds: [id] }; });
    var products = Object.keys(productMap).map(function (id) { return productMap[id]; }), evidenceIds = unique(offers.reduce(function (a, p) { return a.concat(p.evidenceIds || []); }, []));
    var rows = legacy ? [] : candidate.demandEvidence.filter(function (p) { return evidenceIds.indexOf(p.evidenceId) >= 0; });
    var selectedNames = products.map(function (p) { return p.title; }), excludedTitles = unique((candidate.productTitles || []).filter(function (t) { return selectedNames.indexOf(t) < 0; }));
    var themes = unique(products.map(function (p) { return p.title.toLowerCase().replace(/[^a-z0-9 ]+/g, ' ').replace(/\s+/g, ' ').trim().split(' ').filter(Boolean).reduce(function (a, word) { return (a ? a + ' ' : '') .length + word.length <= 80 ? (a ? a + ' ' : '') + word : a; }, ''); }).filter(Boolean));
    var scope = { selectedItemIds: selectedIds, products: products, excludedTitles: excludedTitles, selectionChanged: selectedIds.length !== allIds.length, legacy: legacy, searchThemes: themes.slice(0, 10) };
    var seasonal = seasonality(rows, candidate, now, legacy), totals = seasonal.totals, reasons = [], limits = [], paid = paidTotal(offers.map(function (p) { return p.paidPerformance || {}; }), candidate.paidPerformance), free = freeTotal(offers.map(function (p) { return p.freePerformance && p.freePerformance.days30 || {}; }));
    var budget = options.dailyBudget == null ? number(candidate.dailyBudget) : number(options.dailyBudget), days = options.days == null ? number(candidate.days) || 30 : number(options.days);
    if (!selectedIds.length) return { schema: 1, summary: 'Select products to see why they merit a test and what the available evidence can project.', reasons: [], seasonality: seasonal, forecast: forecast(candidate, [], scope, budget, days), scope: scope, limitations: [] };
    if (legacy) {
      reasons.push({ title: 'Selected products need a fresh evidence check', detail: 'These ' + products.length + ' product(s) are the offers selected for this ad. This saved suggestion lacks a verifiable link between each offer and its original sales rationale; other product names or collection totals are not evidence for your selection.' });
      limits.push('Refresh product research to rebuild an exact-product recommendation. Existing paid history is shown only as a labelled broader benchmark.');
    } else {
      if (totals.orders > 0) reasons.push({ title: 'Purchases support a reach test', detail: totals.orders + ' observed product-order matches over ' + (candidate.evidenceDays || 90) + ' days' + (candidate.demandCoverage && candidate.demandCoverage.monetaryComplete ? ' and ' + (candidate.salesCurrency || 'USD') + ' ' + round(totals.revenue).toFixed(2) + ' in recorded product revenue' : '') + ' show that buyers have already chosen these exact products. That makes testing additional qualified reach more defensible than starting from clicks alone. It does not prove that paid traffic will convert at the same rate.' });
      if (free.available && free.conversions > 0) reasons.push({ title: 'Free listings reveal demand to test with ads', detail: round(free.conversions) + ' separately reported Merchant free-listing conversions in 30 days provide an additional buying signal.' + (paid.available && paid.conversions > 0 ? ' Paid history also reports ' + round(paid.conversions) + ' conversions, so creative improvement can be tested against an existing paid baseline.' : ' Paid success is not established for this selection; begin with a measured acquisition test, not an assumption that organic demand guarantees profitable ads.') + ' Shopify and Merchant attribution can overlap; these conversions are not added to order counts.' });
      if (paid.available && paid.cost > 0 && !paid.conversions) reasons.push({ title: 'Paid performance is a reason for caution', detail: (paid.currency || 'USD') + ' ' + round(paid.cost).toFixed(2) + ' has been spent on the selected products without a reported paid conversion in this history. A new image or message needs to outperform that baseline before increasing spend.' });
      if (seasonal.trend) reasons.push({ title: 'Recent demand determines test priority', detail: seasonal.trend.direction === 'rising' ? 'Observed daily purchase pace has risen to ' + seasonal.trend.dailyRateRatio + '× the previous 60 days, giving a concrete reason to test reach now. This is recent momentum, not a proven seasonal effect.' : seasonal.trend.direction === 'falling' ? 'Observed daily purchase pace has fallen to ' + seasonal.trend.dailyRateRatio + '× the previous 60 days. Treat this as a cautious creative test; the data do not support scaling because of seasonality.' : 'Recent demand is ' + (seasonal.trend.direction === 'steady' ? 'broadly steady' : 'too sparse to establish a stable trend') + '. Test whether more qualified reach produces purchases before claiming an upcoming demand surge.' });
      if (candidate.rankingContext && !scope.selectionChanged) reasons.push({ title: 'Why it made the shortlist', detail: 'Ranked ' + candidate.rankingContext.rank + ' of ' + candidate.rankingContext.candidateCount + ' non-overlapping candidates using observed purchases, separate free-listing evidence, paid outcomes and wasted-spend penalties.' + (candidate.rankingContext.alternative ? ' The next comparison is ' + candidate.rankingContext.alternative.title + ' (priority score ' + candidate.rankingContext.alternative.score + ' versus ' + candidate.score + ').' : '') + ' These scores prioritize research; they are not probabilities of success or projected returns.' });
    }
    reasons.push({ title: 'Why this product set', detail: selectedIds.length + ' exact ' + (candidate.feedLabel || '') + ' feed offer(s) map to ' + products.length + ' selected product(s). Only their linked product evidence is included here. Related or unselected products can inspire creative but are not silently added to the ad or its sales case.' });
    var projections = forecast(candidate, offers, scope, budget, days);
    reasons.push({ title: 'What would change this recommendation', detail: 'Reconsider if the selected offers become unavailable, recent purchase pace weakens, or paid acquisition fails to cover contribution margin.' + (candidate.breakEvenRoas ? ' The current estimated break-even ROAS is ' + number(candidate.breakEvenRoas).toFixed(2) + '×; verify actual margins before using it as a spending threshold.' : '') + ' Review product availability, tracking, CTR, paid conversion rate and revenue after the planned ' + days + '-day test; organic revenue alone cannot justify raising the ad budget.' });
    if (!legacy && candidate.demandCoverage && (!candidate.demandCoverage.days30 || !candidate.demandCoverage.days90)) limits.push('Some order-history periods are incomplete. Observed counts are lower bounds and do not support a reliable period-over-period comparison.');
    if (projections.baseline && projections.baseline.scope !== 'selected_products') limits.push('Projection rates come from ' + projections.baseline.label + ', not demonstrated performance of this exact selection.');
    limits.push('A product-order match counts an order containing a product; a basket containing several selected products can appear more than once. Buyer motivations are hypotheses, not facts inferred from orders.');
    var summary = legacy ? 'Refresh the evidence for these ' + products.length + ' selected product(s) before relying on the saved recommendation.' : totals.orders > 0 || free.conversions > 0 ? 'Test these products because buyers have already chosen them' + (seasonal.trend && seasonal.trend.direction === 'rising' ? ' and recent demand is increasing' : '') + '. The next question is whether paid reach can convert profitably.' : 'This selection needs more purchase evidence before a strong advertising case can be made.';
    return { schema: 1, summary: summary, reasons: reasons, seasonality: seasonal, forecast: projections, scope: scope, limitations: limits };
  }
  return { buildRecommendation: buildRecommendation, paidTotal: paidTotal };
});
