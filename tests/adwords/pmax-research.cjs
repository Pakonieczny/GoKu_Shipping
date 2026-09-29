'use strict';
// Product ads (Performance Max) research: the dated reasoning, the store's own evidence, the listing fit, the
// prompt for the model and the strict validation of what the model returns. Offline: pure functions, no Google
// request, no paid AI call; the "model answers" below are hand-written fixtures.
const assert = require('node:assert/strict'), fs = require('node:fs'), path = require('node:path'), vm = require('node:vm');
const R = require('../../netlify/functions/_googleAdsPmaxResearch');
let checks = 0;
const test = (name, fn) => { try { fn(); } catch (e) { e.message = name + ': ' + e.message; throw e; } checks++; };
const clone = v => JSON.parse(JSON.stringify(v));
const TODAY = '2026-09-29', DAY = 86400000;

// ---------- fixtures ----------
const occasions = [
  { label: 'Halloween', date: '2026-10-31', markets: null },
  { label: 'Canadian Thanksgiving', date: '2026-10-12', markets: ['CA'] },
  { label: 'Black Friday', date: '2026-11-27', markets: null },
  { label: 'Christmas', date: '2026-12-25', markets: null }
];
const timing = R.timingWindows({ today: TODAY, occasions, markets: ['US', 'CA'] });
const ids = { bracelet: 'shopify_US_111111_1001', braceletB: 'shopify_US_111111_1002', pendant: 'shopify_US_222222_2001', initial: 'shopify_US_333333_3001' };
const paid = { available: true, monetaryComplete: true, impressions: 5000, clicks: 120, cost: 84.5, conversions: 3, value: 260, currency: 'USD', days: 90 };
const free = (clicks, conversions) => ({ available: true, impressions: 900, clicks, conversions, value: conversions * 45, valueComplete: true });
const offer = (itemId, title, productTitle, productId, evidenceIds, extra = {}) => ({ itemId, title, productTitle, productId, type1: 'Bracelets', type2: 'Charm Bracelets', feedLabel: 'US', customLabels: [], evidenceIds,
  paidPerformance: { impressions: 0, clicks: 0, conversions: 0, cost: 0, value: 0, currency: 'USD', available: true }, freePerformance: { days30: { ...free(0, 0) }, days90: { ...free(0, 0) } }, ...extra });
const candidate = {
  handle: 'charm-bracelets', collectionTitle: 'Charm Bracelets', feedLabel: 'US', recommendationSchema: 1, salesCurrency: 'USD',
  itemIds: [ids.bracelet, ids.braceletB, ids.pendant, ids.initial],
  productTitles: ['Personalized Birthstone Charm Bracelet - 14k Gold Filled', 'Mom Heart Pendant Necklace', 'Engraved Initial Necklace'],
  evidenceDays: 90, evidenceTotals: { orders: 18, revenue: 1000, orders30d: 9, revenue30d: 520 },
  demandCoverage: { days30: true, days90: true, monetaryComplete: true },
  seasonalityCoverage: { days: 365, complete: false, historyComplete: false, coverage: 'Monthly history is partial', startAt: Date.parse('2026-03-01T00:00:00Z'), endAt: Date.parse('2026-09-28T00:00:00Z') },
  demandEvidence: [
    { evidenceId: 'demand-0', title: 'Personalized Birthstone Charm Bracelet - 14k Gold Filled', orders: 12, orders30d: 7, revenue: 780, revenue30d: 420, monthly: [{ month: '2026-07', orders: 3, revenue: 200 }, { month: '2026-08', orders: 4, revenue: 250 }, { month: '2026-09', orders: 5, revenue: 330 }] },
    { evidenceId: 'demand-1', title: 'Mom Heart Pendant Necklace', orders: 4, orders30d: 2, revenue: 160, revenue30d: 80, monthly: [] },
    { evidenceId: 'demand-2', title: 'Engraved Initial Necklace', orders: 2, orders30d: 0, revenue: 60, revenue30d: 20, monthly: [] }
  ],
  estimatedProfit30d: 230, marginRate: 0.48, breakEvenRoas: 2.08,
  paidPerformance: { ...paid }, freePerformance: { days30: { ...free(60, 4) }, days90: { ...free(150, 9) }, source: 'Merchant API reports' },
  offerDetails: [
    offer(ids.bracelet, 'Birthstone Charm Bracelet 14k', 'Personalized Birthstone Charm Bracelet - 14k Gold Filled', '111111', ['demand-0'], { customLabels: ['bestseller'], paidPerformance: { ...paid, clicks: 90, conversions: 3, cost: 60, value: 260 }, freePerformance: { days30: free(40, 3), days90: free(100, 6) } }),
    offer(ids.braceletB, 'Birthstone Charm Bracelet 14k (long)', 'Personalized Birthstone Charm Bracelet - 14k Gold Filled', '111111', ['demand-0']),
    offer(ids.pendant, 'Mom Heart Pendant Necklace', 'Mom Heart Pendant Necklace', '222222', ['demand-1'], { type1: 'Necklaces', type2: 'Pendants' }),
    offer(ids.initial, 'Engraved Initial Necklace', 'Engraved Initial Necklace', '333333', ['demand-2'], { type1: 'Necklaces', type2: null })
  ],
  rankingContext: { rank: 1, candidateCount: 3 }
};
const rankedKeywords = [
  { text: 'personalized birthstone charm bracelet', kind: 'product', reason: 'What this best seller is called.', monthlySearches: 1900, competition: 'medium' },
  { text: 'christmas gifts for mom', kind: 'occasion', reason: 'Gift searches build toward December.', monthlySearches: 74000, competition: 'high' },
  { text: 'mom heart pendant necklace', kind: 'product', reason: '', monthlySearches: 720, competition: 'low' },
  { text: 'gold filled charm bracelet', kind: 'material', reason: 'Buyers search the metal.', monthlySearches: null, competition: null },
  { text: 'engraved initial necklace', kind: 'style', reason: 'Matches a listed necklace.', monthlySearches: 2400, competition: 'medium' }
];
const ctx = { today: TODAY, timing, keywords: rankedKeywords, orderCutoffDays: 0, markets: ['US', 'CA'], at: 1790000000000, modelLabel: 'Sonnet 5.5',
  sources: [{ title: 'Gift trends', url: 'https://example.com/gifts' }] };
const words = s => JSON.stringify(s);

// ---------- timing ----------
test('timing on 2026-09-29 for US and CA: near dates are too late, Black Friday leads, Christmas follows', () => {
  assert.equal(timing.length, 4);
  const by = Object.fromEntries(timing.map(t => [t.label, t]));
  assert.equal(timing[0].label, 'Black Friday'); assert.equal(timing[0].role, 'main');
  assert.equal(by['Black Friday'].daysAway, 59); assert.equal(by['Black Friday'].startBy, '2026-10-09');
  assert.equal(by['Christmas'].role, 'also'); assert.equal(by['Christmas'].daysAway, 87); assert.equal(by['Christmas'].startBy, '2026-11-06');
  assert.equal(by['Halloween'].role, 'too-late'); assert.equal(by['Halloween'].daysAway, 32); assert.equal(by['Halloween'].startBy, null);
  assert.equal(by['Canadian Thanksgiving'].role, 'too-late'); assert.equal(by['Canadian Thanksgiving'].daysAway, 13);
  assert.match(by['Halloween'].note, /still be learning when it passes/); assert.match(by['Christmas'].note, /Nov 6/); assert.match(by['Black Friday'].note, /Nov 27 is 59 days away/);
  assert.equal(by['Black Friday'].market, 'US/CA'); assert.equal(by['Canadian Thanksgiving'].market, 'CA');
  assert(timing.every(t => t.note.length <= 160));
});
test('startBy is the date minus learning, the order cutoff and a week; main is the first date with room', () => {
  const d = date => new Date(Date.parse(date + 'T00:00:00Z') - 49 * DAY).toISOString().slice(0, 10);
  assert.equal(timing.find(t => t.role === 'main').startBy, d('2026-11-27'));
  // exactly 49 days of room still counts; one day less does not
  const edge = R.timingWindows({ today: TODAY, occasions: [{ label: 'Edge', date: '2026-11-17' }, { label: 'Short', date: '2026-11-16' }], markets: ['US'] });
  assert.deepEqual(edge.map(t => [t.label, t.role]), [['Edge', 'main'], ['Short', 'too-late']]);
  const cut = R.timingWindows({ today: TODAY, occasions: [{ label: 'Gift day', date: '2026-11-27' }], markets: ['US'], orderCutoffDays: 10 });
  assert.equal(cut[0].role, 'main'); assert.equal(cut[0].startBy, '2026-09-29'); assert.match(cut[0].note, /last day to order/);
  const late = R.timingWindows({ today: TODAY, occasions: [{ label: 'Gift day', date: '2026-11-27' }], markets: ['US'], orderCutoffDays: 11 });
  assert.equal(late[0].role, 'too-late');
});
test('only occasions observed in the candidate markets appear; markets are merged; past dates and bad dates are dropped', () => {
  const t = R.timingWindows({ today: TODAY, markets: ['US'], occasions: [
    { label: "Mother's Day", date: '2027-05-09', markets: ['US', 'CA', 'AU'] }, { label: 'Canadian Thanksgiving', date: '2026-10-12', markets: ['CA'] },
    { label: 'Australian Father\'s Day', date: '2026-11-08', markets: ['AU'] }, { label: 'Old', date: '2026-09-01' }, { label: 'Broken', date: '2026-13-40' }, { label: 'Free', date: 'soon' }] });
  assert.deepEqual(t.map(x => x.label), ["Mother's Day"]);
  assert.equal(t[0].market, 'US');
  const both = R.timingWindows({ today: TODAY, markets: ['US', 'CA'], occasions: [{ label: 'Christmas', date: '2026-12-25', markets: ['US'] }, { label: 'Christmas', date: '2026-12-25', markets: ['CA'] }] });
  assert.equal(both.length, 1); assert.equal(both[0].market, 'US/CA');
  assert.deepEqual(R.timingWindows({ today: 'bad', occasions }), []);
});
test('timing is capped at four with the main date first and later dates kept ahead of a run of near misses', () => {
  const many = [1, 2, 3, 4, 5, 6].map(i => ({ label: 'Near ' + i, date: '2026-10-0' + i })).concat([{ label: 'Black Friday', date: '2026-11-27' }, { label: 'Cyber Monday', date: '2026-11-30' }, { label: 'Christmas', date: '2026-12-25' }, { label: "Valentine's Day", date: '2027-02-14' }]);
  const t = R.timingWindows({ today: TODAY, occasions: many, markets: ['US'] });
  assert.equal(t.length, 4); assert.equal(t[0].label, 'Black Friday'); assert.equal(t[0].role, 'main');
  assert(t.some(x => x.label === 'Christmas'), 'Christmas stays visible'); assert(!t.some(x => x.label === 'Cyber Monday'), 'a date 3 days after the main date adds nothing');
  assert.equal(t.filter(x => x.role === 'too-late').length, 2);
  const none = R.timingWindows({ today: TODAY, occasions: [{ label: 'Near', date: '2026-10-05' }], markets: ['US'] });
  assert.equal(none.length, 1); assert.equal(none[0].role, 'too-late');
  assert.match(R.timingWindows({ today: TODAY, occasions: [{ label: 'Graduation', date: '2027-06-10', approx: true }], markets: ['US'] })[0].note, /^Falls around Jun 10, 2027/);
});

// ---------- evidence ----------
test('evidence facts carry the real numbers and skip anything that is missing', () => {
  const f = R.evidenceFacts(candidate, { today: TODAY, timing });
  assert(f.length >= 4 && f.length <= 5); assert(f.every(s => s.length <= 160 && !/undefined|NaN|null/.test(s)));
  const all = f.join(' | ');
  assert.match(f[0], /ordered these products 18 times in the last 90 days, \$1,000 in sales/);
  assert.match(f[1], /9 of those orders came in the last 30 days, against 9 in the 60 days before, so buying is picking up/);
  assert.match(all, /free product listings reported 4 purchases and 60 clicks in the last 30 days/);
  assert.match(all, /Paid ads on these products in the last 90 days: 3 purchases from 120 clicks, \$84\.50 spent and \$260 in sales/);
  const more = R.evidenceFacts(candidate, { today: TODAY }).join(' | ');
  assert.match(more, /Estimated profit on these products in the last 30 days is \$230\. To break even, ads need about \$2\.08 in sales for every \$1 spent/);
  const bare = R.evidenceFacts({ evidenceDays: 90, evidenceTotals: { orders: 3, revenue: 0, orders30d: 0 }, demandCoverage: { days30: false, days90: true }, paidPerformance: { available: false }, freePerformance: { days30: { available: false }, days90: { available: false } } }, { today: TODAY });
  assert.deepEqual(bare, ['Buyers ordered these products 3 times in the last 90 days.']);
  assert.deepEqual(R.evidenceFacts({}, { today: TODAY }), []);
  const nopaid = R.evidenceFacts({ ...candidate, paidPerformance: { available: true, impressions: 0, clicks: 0, cost: 0, conversions: 0, value: 0 } }, { today: TODAY }).join(' ');
  assert.match(nopaid, /no paid ad history yet/);
  const spent = R.evidenceFacts({ ...candidate, paidPerformance: { ...paid, conversions: 0, value: 0 } }, { today: TODAY }).join(' ');
  assert.match(spent, /0 purchases from 120 clicks, \$84\.50 spent/);
});
test('the trend uses the latest 30 days against the previous 60 and states its counts', () => {
  const t = (orders, orders30d, extra = {}) => R.evidenceFacts({ ...candidate, evidenceTotals: { orders, revenue: 500, orders30d, revenue30d: 100 }, ...extra }, { today: TODAY })[1];
  assert.match(t(12, 4), /4 of those orders came in the last 30 days, against 8 in the 60 days before, so buying is steady/);
  assert.match(t(12, 5), /against 7 in the 60 days before, so buying is picking up/);
  assert.match(t(12, 1), /1 of those orders came in the last 30 days, against 11 in the 60 days before, so buying is slowing/);
  assert.match(t(12, 0), /None of those orders came in the last 30 days, against 12/);
  assert.match(t(6, 6), /All 6 of those orders came in the last 30 days and none in the 60 days before/);
  // incomplete periods: no trend, a lower-bound wording instead
  const partial = R.evidenceFacts({ ...candidate, demandCoverage: { days30: true, days90: false, monetaryComplete: true } }, { today: TODAY });
  assert.match(partial[0], /at least 18 times/); assert(!partial.some(s => /so buying is/.test(s)));
});
test('last year at the same season appears only when the store history really covers that month', () => {
  const monthly = [{ month: '2025-11', orders: 9, revenue: 500 }, { month: '2026-03', orders: 2, revenue: 90 }];
  const covered = { ...candidate, seasonalityCoverage: { complete: true, startAt: Date.parse('2025-06-01T00:00:00Z') }, demandEvidence: candidate.demandEvidence.map((r, i) => (i ? r : { ...r, monthly })) };
  assert.match(R.evidenceFacts(covered, { today: TODAY, timing }).join(' '), /Last November, the same time of year, these products were ordered 9 times, more than in any other full month on record/);
  const noOrders = { ...covered, demandEvidence: candidate.demandEvidence.map(r => ({ ...r, monthly: [] })) };
  assert.match(R.evidenceFacts(noOrders, { today: TODAY, timing }).join(' '), /Last November, the same time of year, these products had no recorded orders/);
  assert(!R.evidenceFacts(candidate, { today: TODAY, timing }).join(' ').includes('the same time of year'));
});

// ---------- computed research ----------
const computed = R.computedResearch(candidate, ctx);
test('computed research is complete, valid and specific', () => {
  assert.deepEqual(R.validateResearch(computed), []);
  assert.equal(computed.source, 'computed'); assert.equal(computed.model, null); assert.equal(computed.version, 1); assert.equal(computed.whyNow.marketRead, null);
  assert.equal(computed.today, TODAY); assert.equal(computed.at, ctx.at);
  assert.match(computed.headline, /Start by Oct 9 for Black Friday, 59 days away/); assert.match(computed.headline, /Personalized Birthstone Charm Bracelet/); assert.match(computed.headline, /18 times in 90 days/);
  const s = computed.whyNow.summary;
  assert.match(s, /Black Friday is 59 days away \(Nov 27\)/); assert.match(s, /six-week learning period if this starts by Oct 9/); assert.match(s, /Canadian Thanksgiving \(Oct 12\) and Halloween \(Oct 31\) are too close/); assert.match(s, /ordered Personalized Birthstone Charm Bracelet and 2 more 18 times/);
  assert.match(s, /picking up/);
  assert(s.length <= 420 && computed.headline.length <= 140);
  assert.deepEqual(computed.whyNow.timing, timing); assert.deepEqual(computed.whyNow.evidence, R.evidenceFacts(candidate, { today: TODAY, timing }));
  assert.equal(computed.keywords.length, 5); assert.equal(computed.keywords[0].monthlySearches, 1900); assert.equal(computed.keywords[2].reason, 'About 720 searches a month.');
  assert.deepEqual(computed.sources, ctx.sources);
  const text = words({ ...computed, listingFit: computed.listingFit.map(({ itemId, ...rest }) => rest) });
  assert(!/undefined|NaN|\[object|shopify_/.test(text), 'no technical text: ' + text.match(/undefined|NaN|\[object|shopify_[A-Za-z0-9_]*/));
  assert(!/[\u{1F300}-\u{1FAFF}☀-➿]/u.test(text));
});
test('listing fit: one entry per product, real reasons, the leaders are heroes', () => {
  const fit = computed.listingFit;
  assert.equal(fit.length, 3); assert.deepEqual(fit.map(f => f.itemId), [ids.bracelet, ids.pendant, ids.initial]);
  assert(fit.every(f => candidate.itemIds.includes(f.itemId)));
  assert.deepEqual(fit.map(f => f.role), ['hero', 'support', 'support']);
  const strong = clone(candidate); strong.demandEvidence[1].orders = 7; assert.deepEqual(R.computedResearch(strong, ctx).listingFit.map(f => f.role), ['hero', 'hero', 'support'], 'a second seller with over half the leader orders is a hero too');
  assert.deepEqual(R.computedResearch({ ...candidate, itemIds: [ids.bracelet], offerDetails: [candidate.offerDetails[0]] }, ctx).listingFit.map(f => f.role), ['hero']);
  assert.match(fit[0].reason, /12 orders in 90 days, 7 in the last 30/); assert.match(fit[0].reason, /Listed under Charm Bracelets/); assert.match(fit[0].reason, /Tagged bestseller/); assert.match(fit[0].reason, /3 free-listing purchases in 30 days/);
  assert.match(R.computedResearch({ ...candidate, offerDetails: [{ ...candidate.offerDetails[0], customLabels: [] }, ...candidate.offerDetails.slice(1)] }, ctx).listingFit[0].reason, /3 paid purchases/);
  assert.match(fit[1].reason, /4 orders in 90 days, 2 in the last 30/); assert.match(fit[1].reason, /Listed under Pendants/); assert.doesNotMatch(fit[1].reason, /Necklaces/);
  assert(fit.every(f => f.reason.length <= 140 && f.title.length <= 120));
  assert.equal(fit[0].title, 'Personalized Birthstone Charm Bracelet - 14k Gold Filled');
  assert.match(R.computedResearch({ ...candidate, offerDetails: [{ ...candidate.offerDetails[0], customLabels: ['bestseller', 'gold'] }] }, ctx).listingFit[0].reason, /Tagged bestseller, gold/);
});
test('creative angles come from the occasion and the products', () => {
  const a = computed.creativeAngles;
  assert(a.length >= 3 && a.length <= 4 && a.every(x => x.length <= 90));
  assert.equal(a[0], 'A charm bracelet to give for Black Friday'); assert(a.some(x => /personalized charm bracelet made for one person/.test(x))); assert(a.some(x => /mom/.test(x))); assert(a.some(x => /14k gold filled/i.test(x)));
  const memorial = R.computedResearch({ ...candidate, offerDetails: [{ ...candidate.offerDetails[0], productTitle: 'Memorial Remembrance Charm', title: 'Memorial Remembrance Charm' }] }, ctx).creativeAngles;
  assert.equal(memorial[0], 'A quiet keepsake to remember someone dear');
});
test('limits are honest about history, paid data, reports and keywords', () => {
  const l = computed.limits.join(' | ');
  assert.match(l, /Order history only reaches back to Mar 1, 2026, so last year's Black Friday sales cannot be compared/);
  assert.match(l, /no outside web research was done/);
  assert(!/Paid ad history/.test(l));
  const gaps = R.computedResearch({ ...candidate, paidPerformance: { available: false }, freePerformance: { days30: { available: false }, days90: { available: false } }, demandCoverage: { days30: false, days90: true } }, { ...ctx, keywords: [] });
  const g = gaps.limits.join(' | ');
  assert.match(g, /Paid ad history for these products could not be loaded/); assert.match(g, /free-listing reports were not available/); assert.match(g, /Some recent order history is incomplete/);
  assert.match(gaps.limits.concat(R.computedResearch(candidate, { ...ctx, keywords: [] }).limits).join(' '), /No search volumes were available/);
  assert(gaps.limits.length <= 6); assert.deepEqual(R.validateResearch(gaps), []);
  const unknown = R.computedResearch({ ...candidate, seasonalityCoverage: { complete: false, startAt: null } }, ctx).limits.join(' ');
  assert.match(unknown, /could not be confirmed, so last year's Black Friday sales were not compared/);
});
test('with no ranked keywords the product names stand in; with no calendar the reasoning says so', () => {
  const bare = R.computedResearch(candidate, { today: TODAY, timing: [], keywords: [] });
  assert.deepEqual(R.validateResearch(bare), []);
  assert.equal(bare.keywords[0].text, 'personalized birthstone charm bracelet 14k gold filled'); assert.equal(bare.keywords[0].monthlySearches, null);
  assert.match(bare.whyNow.summary, /No dated occasion is on the calendar/); assert.match(bare.headline, /no dated occasion/);
  const allLate = R.computedResearch(candidate, { ...ctx, timing: R.timingWindows({ today: '2026-12-10', occasions, markets: ['US'] }), today: '2026-12-10' });
  assert.match(allLate.whyNow.summary, /No date on the calendar leaves room for Google's six-week learning period: Christmas is only 15 days away/);
  assert.match(allLate.whyNow.caution, /too close for Google's learning period/); assert.deepEqual(R.validateResearch(allLate), []);
  assert.equal(R.computedResearch({}, {}).listingFit.length, 0);
});
test('headline and summary stay within their limits for long titles and many notes', () => {
  const long = clone(candidate); long.demandEvidence[0].title = long.offerDetails[0].productTitle = long.offerDetails[1].productTitle = 'Personalized Birthstone Charm Bracelet with Engraved Name Tag and Extra Long Adjustable Chain in Solid Gold for Grandmothers Everywhere';
  const r = R.computedResearch(long, { ...ctx, timing: R.timingWindows({ today: TODAY, occasions, markets: ['US', 'CA'] }) });
  assert(r.headline.length <= 140 && r.whyNow.summary.length <= 420); assert.deepEqual(R.validateResearch(r), []); assert.match(r.headline, /Black Friday/);
});

// ---------- prompt ----------
test('the prompt carries the date, markets, timing, evidence, keywords, every offer and the JSON-only instruction', () => {
  const p = R.researchPrompt(candidate, ctx);
  assert.match(p, /TODAY: 2026-09-29/); assert.match(p, /ACCOUNT MARKETS: US, CA/);
  timing.forEach(t => { assert(p.includes(t.label) && p.includes(t.date), t.label); });
  assert(p.includes('start ads by 2026-10-09')); assert(p.includes('too late for a campaign started today'));
  R.evidenceFacts(candidate, { today: TODAY, timing }).forEach(f => assert(p.includes(f), f));
  rankedKeywords.forEach(k => assert(p.includes('"' + k.text + '"'), k.text)); assert(p.includes('about 74,000 searches a month')); assert(p.includes('search volume not available'));
  Object.values(ids).forEach(id => assert(p.includes(id), id));
  candidate.offerDetails.forEach(o => assert(p.includes(JSON.stringify(o.title).slice(1, -1)), o.title));
  assert(p.includes('"ordersForThisProduct":12')); assert(p.includes('"tags":["bestseller"]'));
  assert.match(p, /web search is available/i); assert.match(p, /current gifting demand and trend evidence/); assert.match(p, /naming the page in marketRead/);
  assert.match(p, /Return ONLY this JSON/); assert.match(p, /Quote only numbers that appear above/); assert.match(p, /memorial, sympathy or loss/); assert.match(p, /No medical or health claims/); assert.match(p, /No discounts, sale wording, urgency/);
  assert.match(p, /Do not promise or predict/); assert.match(p, /Choose only from these exact texts/);
  assert(p.includes('"headline"') && p.includes('"listingFit"') && p.includes('"creativeAngles"') && p.includes('"marketRead"') && p.includes('"caution"'));
  assert(!R.researchPrompt(candidate, { ...ctx, timing: [], keywords: [] }).includes('undefined'));
});

// ---------- normalizing the model's answer ----------
const good = () => ({
  headline: 'Start by Oct 9 for Black Friday: the birthstone charm bracelet was ordered 18 times in 90 days.',
  whyNow: { summary: 'Black Friday is 59 days away (Nov 27), so ads started by Oct 9 finish learning in time. Buyers ordered these products 18 times in 90 days, 9 in the last 30, which shows current interest.',
    marketRead: 'Gift guides on example.com/gifts list personalized jewelry among the top picks this season, up 30% on last year.', caution: 'Paid ads have returned 3 purchases so far, so keep the first budget small.' },
  listingFit: [
    { itemId: ids.bracelet, reason: 'The best seller: 12 orders in 90 days, and a personalized gift.', role: 'hero' },
    { itemId: 'shopify_US_999999_9999', reason: 'Not part of this idea.', role: 'hero' },
    { itemId: ids.braceletB, reason: 'Same product, longer chain.', role: 'support' },
    { itemId: ids.pendant, reason: 'A gift for mom with 4 orders.', role: 'hero' }
  ],
  keywords: [{ text: 'Christmas Gifts For Mom', reason: 'Gift searches build toward December.' }, { text: 'invented keyword', reason: 'Not supplied.' }, { text: 'personalized birthstone charm bracelet', reason: 'The best seller by name, 1,900 searches a month.' }, { text: 'gold filled charm bracelet', reason: 'The metal buyers ask for.' }],
  creativeAngles: ['A birthstone charm bracelet to give this season', 'Made for one person, with their stone'],
  limits: ['Web pages were read for demand only, not for this shop.']
});
test('a good answer is kept, checked and completed from the deterministic parts', () => {
  const n = R.normalizeResearch(good(), candidate, ctx);
  assert.deepEqual(R.validateResearch(n), []);
  assert.equal(n.source, 'ai'); assert.equal(n.model, 'Sonnet 5.5'); assert.equal(n.at, ctx.at);
  assert.match(n.headline, /Black Friday/); assert.match(n.whyNow.summary, /59 days away/);
  assert.deepEqual(n.whyNow.timing, timing); assert.deepEqual(n.whyNow.evidence, computed.whyNow.evidence);
  assert.match(n.whyNow.marketRead, /example\.com\/gifts/); assert.match(n.whyNow.caution, /first budget small/);
  assert.deepEqual(n.listingFit.map(f => f.itemId), [ids.bracelet, ids.pendant]); assert.deepEqual(n.listingFit.map(f => f.role), ['hero', 'hero']);
  assert.equal(n.listingFit[0].title, 'Personalized Birthstone Charm Bracelet - 14k Gold Filled'); assert(!n.listingFit.some(f => f.itemId === ids.braceletB), 'one entry per product');
  assert.deepEqual(n.keywords.map(k => k.text), ['christmas gifts for mom', 'personalized birthstone charm bracelet', 'gold filled charm bracelet']);
  assert.equal(n.keywords[0].monthlySearches, 74000); assert.equal(n.keywords[0].competition, 'high'); assert.equal(n.keywords[0].kind, 'occasion'); assert.equal(n.keywords[2].monthlySearches, null);
  assert.deepEqual(n.creativeAngles, good().creativeAngles); assert.deepEqual(n.sources, ctx.sources);
  assert(n.limits.includes('Web pages were read for demand only, not for this shop.')); assert(!n.limits.includes(R.NO_WEB_LIMIT)); assert.match(n.limits[0], /Order history only reaches back/);
});
test('invented item ids and keyword texts never get through', () => {
  const raw = good(); raw.listingFit = [{ itemId: 'shopify_US_999999_9999', reason: 'Invented.', role: 'hero' }, { itemId: ids.bracelet, reason: 'Real.', role: 'support' }];
  raw.keywords = [{ text: 'cheap silver rings', reason: 'x' }, { text: 'christmas gifts for mom', reason: 'real' }];
  const n = R.normalizeResearch(raw, candidate, ctx);
  assert(n.listingFit.every(f => candidate.itemIds.includes(f.itemId))); assert(n.keywords.every(k => rankedKeywords.some(r => r.text === k.text)));
  assert(!words(n).includes('999999') && !words(n).includes('cheap silver rings'));
  assert.equal(n.listingFit[0].itemId, ids.bracelet); assert.equal(n.listingFit[0].role, 'hero', 'the first real listing becomes the hero when the model named none');
});
test('thin answers are filled from the computed research so the card is never thin', () => {
  const raw = good(); raw.listingFit = [{ itemId: ids.pendant, reason: 'A gift for mom.', role: 'support' }]; raw.keywords = [{ text: 'christmas gifts for mom', reason: 'real' }];
  const n = R.normalizeResearch(raw, candidate, ctx);
  assert.deepEqual(n.listingFit.map(f => f.itemId).sort(), [ids.bracelet, ids.pendant, ids.initial].sort()); assert.equal(n.listingFit.find(f => f.itemId === ids.pendant).reason, 'A gift for mom.');
  assert(n.listingFit.some(f => f.role === 'hero')); assert.equal(n.keywords.length, 5); assert.equal(n.keywords[0].text, 'christmas gifts for mom');
  const empty = R.normalizeResearch({ whyNow: { summary: 'Black Friday is 59 days away, and buyers ordered these products 18 times.' } }, candidate, ctx);
  assert.equal(empty.listingFit.length, 3); assert.equal(empty.keywords.length, 5); assert.equal(empty.headline, computed.headline); assert.deepEqual(empty.creativeAngles, computed.creativeAngles); assert.equal(empty.whyNow.caution, computed.whyNow.caution);
  assert.deepEqual(R.validateResearch(empty), []);
});
test('every string is clamped, cleaned of control characters and emoji, and limited in count', () => {
  const raw = good(), big = 'Black Friday is 59 days away. ' + 'Buyers ordered these products 18 times in 90 days. '.repeat(20);
  raw.whyNow.summary = big; raw.headline = 'Start by Oct 9. ' + 'x'.repeat(300); raw.whyNow.marketRead = 'Read on example.com. ' + 'y'.repeat(500); raw.whyNow.caution = 'Watch closely. ' + 'z'.repeat(400);
  raw.listingFit = [{ itemId: ids.bracelet, reason: 'r'.repeat(400), role: 'hero' }]; raw.creativeAngles = ['a'.repeat(200), 'b', 'c', 'd', 'e', 'f'];
  raw.keywords = [{ text: 'christmas gifts for mom', reason: 'k'.repeat(300) }]; raw.limits = ['l'.repeat(300)];
  const n = R.normalizeResearch(raw, candidate, ctx);
  assert.deepEqual(R.validateResearch(n), []);
  assert(n.whyNow.summary.length <= 420 && n.headline.length <= 140 && n.whyNow.marketRead.length <= 300 && n.whyNow.caution.length <= 200);
  assert(n.listingFit[0].reason.length <= 140 && n.keywords[0].reason.length <= 100); assert.equal(n.creativeAngles.length, 4); assert(n.creativeAngles.every(a => a.length <= 90)); assert(n.limits.every(l => l.length <= 160));
  const dirty = good(); dirty.whyNow.summary = 'Black Friday is 59 days away.\u0000\u0007 ​Buyers ordered these products 18 times \u{1F381}\u{2728} in 90 days.\n\n**Start by Oct 9.**';
  const c = R.normalizeResearch(dirty, candidate, ctx).whyNow.summary;
  assert.equal(c, 'Black Friday is 59 days away. Buyers ordered these products 18 times in 90 days. Start by Oct 9.');
  assert(!/[\u0000-\u001F]/.test(words(R.normalizeResearch(dirty, candidate, ctx)).replace(/\\n|\\u[0-9a-f]{4}/g, '')));
});
test('prose that quotes an unsupplied number, hypes, promises or leaks internal names is rejected or replaced', () => {
  const inv = good(); inv.whyNow.summary = 'Black Friday is 59 days away and 40% of shoppers buy jewelry then. Buyers ordered these products 18 times.';
  assert.equal(R.normalizeResearch(inv, candidate, ctx), null, 'an invented statistic makes the summary unusable');
  const wrongDate = good(); wrongDate.whyNow.summary = 'Black Friday is 59 days away, so start by Nov 13. Buyers ordered these products 18 times.';
  assert.equal(R.normalizeResearch(wrongDate, candidate, ctx), null, 'a start date the calendar did not give is rejected');
  const hype = good(); hype.whyNow.summary = 'Black Friday is 59 days away and this campaign is guaranteed to sell out, so act now.';
  assert.equal(R.normalizeResearch(hype, candidate, ctx), null);
  const leak = good(); leak.whyNow.summary = 'Black Friday is 59 days away; the itemId list shows 18 orders in 90 days.';
  assert.equal(R.normalizeResearch(leak, candidate, ctx), null);
  const parts = good(); parts.headline = 'Limited time: 25% off charm bracelets'; parts.listingFit[0].reason = 'Sold 500 last year.'; parts.creativeAngles = ['Act now before it sells out', 'Heals grief with a keepsake', 'A charm bracelet made for one person'];
  parts.keywords[0].reason = 'Searched 99999 times.'; parts.whyNow.caution = 'Expect 300% growth.';
  const n = R.normalizeResearch(parts, candidate, ctx);
  assert.equal(n.headline, computed.headline); assert.match(n.listingFit[0].reason, /12 orders in 90 days/); assert.deepEqual(n.creativeAngles, ['A charm bracelet made for one person']);
  assert.equal(n.keywords[0].reason, 'Gift searches build toward December.'); assert.equal(n.whyNow.caution, computed.whyNow.caution);
  assert.match(R.normalizeResearch(good(), candidate, ctx).whyNow.marketRead, /30%/, 'a web finding keeps its own numbers in the market read');
});
test('malformed input never throws and dates the model made up are rejected', () => {
  const junk = [null, 7, { date: 'x' }, { label: 'A', date: '2026-11-27', daysAway: 59, role: 'main' }, { label: 'B', date: '2026-11-27', daysAway: 59, role: 'main', startBy: 'never' }];
  const r = R.computedResearch(candidate, { ...ctx, timing: junk, keywords: [null, 'x', { text: 'a b c d e f g h i j k' }, { text: 'y'.repeat(90) }, { text: 'ok text', kind: 'bogus', competition: 'HIGH', monthlySearches: '150' }] });
  assert.deepEqual(R.validateResearch(r), []); assert.deepEqual(r.whyNow.timing, []); assert.deepEqual(r.keywords.map(k => [k.text, k.kind, k.competition, k.monthlySearches]), [['x', 'product', null, null], ['ok text', 'product', 'high', 150]]);
  assert.match(R.researchPrompt({}, {}), /TODAY:/); assert.deepEqual(R.validateResearch(R.computedResearch({}, {})), []);
  assert.equal(R.normalizeResearch({ whyNow: { summary: 'Black Friday is 59 days away (Nov 27), so ads started by Oct 9 are ready in time.' }, listingFit: [{ itemId: ids.bracelet, reason: 'x', role: 'hero' }] }, {}, ctx).listingFit.length, 0);
  const iso = good(); iso.whyNow.summary = 'Black Friday is 59 days away, so start on 2026-11-13. Buyers ordered these products 18 times.';
  assert.equal(R.normalizeResearch(iso, candidate, ctx), null);
  const okDates = good(); okDates.whyNow.summary = 'Black Friday falls on November 27, so ads started by October 9 (or 2026-10-09) are ready. Buyers ordered these products 18 times, and Christmas on 25th of December follows.';
  assert(R.normalizeResearch(okDates, candidate, ctx), 'dates from the calendar are fine');
  const nan = good(); nan.whyNow.summary = 'Black Friday is 59 days away, and Nan will love the Mom Heart Pendant Necklace. Buyers ordered these products 18 times.';
  assert(R.normalizeResearch(nan, candidate, ctx), 'a grandmother called Nan is not a technical leak');
});
test('unusable answers return null and JSON text is accepted', () => {
  [null, undefined, 5, [], {}, { whyNow: {} }, { whyNow: { summary: '' } }, { whyNow: { summary: 'Too short.' } }, { summary: 'Black Friday is 59 days away and it is a good idea.' }, '{"broken"', 'no json here'].forEach(raw => assert.equal(R.normalizeResearch(raw, candidate, ctx), null, words(raw)));
  const text = '```json\n' + JSON.stringify(good()) + '\n```';
  assert.equal(R.normalizeResearch(text, candidate, ctx).source, 'ai');
  assert.equal(R.normalizeResearch(good(), candidate, { ...ctx, modelLabel: undefined }).model, null);
});
test('sources come only from the pages the engine supplied', () => {
  const raw = good(); raw.sources = [{ title: 'Invented', url: 'https://invented.example' }];
  assert.deepEqual(R.normalizeResearch(raw, candidate, ctx).sources, ctx.sources);
  const many = Array.from({ length: 12 }, (_, i) => ({ title: 'Page ' + i, url: 'https://example.com/' + i }));
  assert.equal(R.normalizeResearch(good(), candidate, { ...ctx, sources: many.concat([{ title: 'bad', url: 'javascript:alert(1)' }, { title: 'dup', url: 'https://example.com/1' }]) }).sources.length, 8);
  assert.deepEqual(R.normalizeResearch(good(), candidate, { ...ctx, sources: undefined }).sources, []);
});

// ---------- merging ----------
test('merge keeps the model prose where valid and the deterministic timing and evidence always', () => {
  const ai = R.normalizeResearch(good(), candidate, ctx), tampered = clone(ai);
  tampered.whyNow.timing = []; tampered.whyNow.evidence = ['Invented fact.'];
  const m = R.mergeResearch(computed, tampered);
  assert.deepEqual(m.whyNow.timing, computed.whyNow.timing); assert.deepEqual(m.whyNow.evidence, computed.whyNow.evidence);
  assert.equal(m.source, 'ai'); assert.equal(m.model, 'Sonnet 5.5'); assert.equal(m.headline, ai.headline); assert.equal(m.whyNow.summary, ai.whyNow.summary); assert.equal(m.whyNow.marketRead, ai.whyNow.marketRead);
  assert.deepEqual(R.validateResearch(m), []);
  const partial = R.mergeResearch(computed, { source: 'ai', model: 'Sonnet 5.5', headline: '', whyNow: { summary: 'Black Friday is 59 days away and buyers ordered these products 18 times.', marketRead: null, caution: null }, listingFit: [], keywords: [{ text: 'christmas gifts for mom', reason: 'x', kind: 'occasion', monthlySearches: 74000, competition: 'high' }], creativeAngles: [], sources: [], limits: [] });
  assert.equal(partial.headline, computed.headline); assert.equal(partial.listingFit.length, 3); assert.equal(partial.keywords.length, 5); assert.deepEqual(partial.creativeAngles, computed.creativeAngles); assert.equal(partial.whyNow.caution, computed.whyNow.caution); assert.deepEqual(partial.sources, computed.sources);
  assert.equal(partial.source, 'ai'); assert(!partial.limits.includes(R.NO_WEB_LIMIT)); assert.deepEqual(R.validateResearch(partial), []);
  assert.deepEqual(R.mergeResearch(computed, null), computed); assert.deepEqual(R.mergeResearch(computed, {}), { ...computed, limits: computed.limits, source: 'computed' });
  assert.equal(R.mergeResearch(computed, { source: 'ai', whyNow: {} }).source, 'computed', 'no usable model prose leaves the computed card labelled as computed');
  assert.equal(R.mergeResearch(null, ai), ai);
});

// ---------- fingerprint ----------
test('the fingerprint is stable, order-free and changes with the day, feed or listings', () => {
  const f = R.researchFingerprint(candidate, TODAY);
  assert.match(f, /^2026-09-29-[0-9a-f]{12}$/); assert.equal(f, R.researchFingerprint({ ...candidate, itemIds: [...candidate.itemIds].reverse() }, TODAY));
  assert.notEqual(f, R.researchFingerprint(candidate, '2026-09-30')); assert.notEqual(f, R.researchFingerprint({ ...candidate, feedLabel: 'CA' }, TODAY));
  assert.notEqual(f, R.researchFingerprint({ ...candidate, itemIds: candidate.itemIds.slice(1) }, TODAY)); assert.notEqual(f, R.researchFingerprint({ ...candidate, handle: 'other' }, TODAY));
});

// ---------- the engine's own candidate shape ----------
const enginePath = path.resolve(__dirname, '../../netlify/functions/googleAdsAutopilot.js');
const sandbox = { module: { exports: {} }, process: { env: { GADS_CURRENCY: 'USD' } }, require: require('module').createRequire(enginePath), URL, Intl, Date, console, Buffer, setTimeout, clearTimeout, AbortController };
vm.createContext(sandbox); vm.runInContext(fs.readFileSync(enginePath, 'utf8'), sandbox);
(async () => {
  const eids = ['shopify_US_123456_111111', 'shopify_US_234567_222222'];
  const mk = (name, productId, variantId, orders, revenue, monthly) => ({ name, productId, variantId, orders, revenue, estimatedProfit: revenue / 2, monthly });
  const sig = { complete: true, monetaryComplete: true, productRows: [mk('Corgi charm necklace', '123456', '111111', 6, 300), mk('Fox charm necklace', '234567', '222222', 3, 150)] };
  const sig30 = { ...sig, productRows: [mk('Corgi charm necklace', '123456', '111111', 4, 200), mk('Fox charm necklace', '234567', '222222', 1, 50)] };
  const sig365 = { complete: true, monetaryComplete: true, historicalImport: { complete: true }, historyCoverage: 'Complete', startAt: Date.parse('2025-09-01T00:00:00Z'), endAt: Date.parse('2026-09-28T00:00:00Z'),
    productRows: [mk('Corgi charm necklace', '123456', '111111', 20, 900, [{ month: '2025-11', orders: 8, revenue: 400 }, { month: '2026-08', orders: 3, revenue: 150 }]), mk('Fox charm necklace', '234567', '222222', 3, 150, [{ month: '2025-11', orders: 1, revenue: 50 }])] };
  const merchant = eids.map((itemId, i) => ({ itemId, title: i ? 'Fox charm necklace' : 'Corgi charm necklace', feedLabel: 'US', status: 'ELIGIBLE', availability: 'IN_STOCK', type1: 'Necklaces', type2: 'Charm Necklaces', customLabels: ['pets'] }));
  const perf = { impressions: 900, clicks: 30, conversions: 1, cost: 25, value: 60, currency: 'USD', monetaryComplete: true };
  const built = sandbox.module.exports.pmaxCandidatesFromSignals({ collections: [{ handle: 'pets', title: 'Pets' }], profiles: [{ handle: 'pets', topProducts: [{ title: 'Corgi charm necklace', productId: '123456' }, { title: 'Fox charm necklace', productId: '234567' }] }],
    sig30, sig90: sig, sig365, merchant, paid: { complete: true, days: 90, rows: [perf], byId: { [eids[0].toLowerCase()]: perf } }, merchantFree30: { complete: true, byId: { [eids[0].toLowerCase()]: { impressions: 200, clicks: 20, conversions: 2, value: 90, valueComplete: true } } }, merchantFree90: { complete: true, byId: {} } });
  assert.equal(built.length, 1); const real = JSON.parse(JSON.stringify(built[0]));
  const realTiming = R.timingWindows({ today: TODAY, occasions, markets: ['US'] });
  const rc = { today: TODAY, timing: realTiming, keywords: [], orderCutoffDays: 0, markets: ['US'], at: 1 };
  const out = R.computedResearch(real, rc);
  assert.deepEqual(R.validateResearch(out), []);
  assert.deepEqual(out.listingFit.map(f => f.itemId), eids); assert.match(out.listingFit[0].reason, /6 orders in 90 days, 4 in the last 30/); assert.match(out.headline, /Corgi charm necklace/);
  assert.match(out.whyNow.evidence.join(' '), /Last November, the same time of year, these products were ordered 9 times/); assert.match(out.whyNow.evidence.join(' '), /free product listings reported 2 purchases and 20 clicks/);
  assert(!out.limits.join(' ').includes('Order history only reaches back'), 'a year of history reaches last December');
  assert.match(R.researchPrompt(real, rc), /Corgi charm necklace/);
  const ai = R.normalizeResearch({ headline: 'Corgi necklaces are the pet-lover pick for Black Friday.', whyNow: { summary: 'Black Friday is 59 days away, so ads started by Oct 9 have their learning done. Buyers ordered these products 9 times in 90 days.' }, listingFit: [{ itemId: eids[0], reason: 'The leader with 6 orders.', role: 'hero' }] }, real, rc);
  assert.equal(ai.source, 'ai'); assert.equal(ai.listingFit.length, 2); assert.deepEqual(R.validateResearch(ai), []);
  checks++;
  console.log('PMax research: ' + checks + ' focused checks passed');
  require('./suite-guard.cjs').done();
})().catch(e => { console.error(e); process.exit(1); });
