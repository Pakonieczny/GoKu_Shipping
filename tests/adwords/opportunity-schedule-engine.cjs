// Timing and learning periods in the opportunity engine: every recommendation says which kind of campaign it is, why now and for how
// long (schedule.js: buildSchedule), a dated occasion is proposed while there is time to plan its launch and never when Google's learning
// period cannot finish, and Product ads get the same treatment. Also: listing photo, price and currency on each offer for the previews.
// Everything is fake: the model, Keyword Planner, Shopify, Merchant Center, Firestore and Google Ads. No network, no paid AI.
const assert = require('node:assert/strict'), fs = require('fs'), vm = require('vm'), path = require('path');
const FN = path.resolve(__dirname, '../../netlify/functions'), source = fs.readFileSync(FN + '/googleAdsAutopilot.js', 'utf8');
const util = require(FN + '/googleAdsSalesEvidence.js'), Thumbs = require(path.resolve(__dirname, '../../assets/ad-preview-thumbs.js')), Sched = require(FN + '/_googleAdsSchedule.js');
const plain = x => JSON.parse(JSON.stringify(x)), J = JSON.stringify;
const at = ymd => Date.parse(ymd + 'T15:00:00Z'); // 11:00 in Toronto: the account's date is that day
const NOW = at('2026-09-29');
const fixedDate = now => class extends Date { constructor(...a) { if (a.length) super(...a); else super(now); } static now() { return now; } };

/* ---- the engine in a vm with the clock fixed and every outside dependency replaced by a fake ---- */
function engine(fns = {}, now = NOW, hide = []) { // hide: helper modules that "cannot be found", to test the failure paths
  const real = require('module').createRequire(path.join(FN, 'googleAdsAutopilot.js'));
  const sandbox = { module: { exports: {} }, process: { env: { GADS_CURRENCY: 'USD', KP_BACKOFF_MS: '0' } }, URL, URLSearchParams, Intl, Date: fixedDate(now), console, Buffer, setTimeout, clearTimeout, AbortController, fns,
    require: n => n === 'node-fetch' ? (() => { throw Error('Unexpected network'); }) : hide.includes(n) ? (() => { throw new Error("Cannot find module '" + n + "'"); })() : real(n) };
  vm.createContext(sandbox);
  vm.runInContext(source + '\nmodule.exports.t={plan:planCampaign,fields:_campaignScheduleFields,status:_opportunityResearchStatus,row:_merchantProductRow,image:_previewImageUrl,preview:_offerPreviewFields,hosts:_PREVIEW_IMAGE_HOSTS};', sandbox);
  vm.runInContext('for(const k of Object.keys(fns))globalThis[k]=fns[k];', sandbox);
  return sandbox.module.exports;
}
// A small in-memory Firestore (documents by "collection/id", shallow merge).
function store(init = {}) {
  const m = new Map(Object.entries(init)), c = v => JSON.parse(JSON.stringify(v));
  const ref = p => ({ id: p.split('/').pop(), get: async () => ({ exists: m.has(p), data: () => m.has(p) ? c(m.get(p)) : undefined }), set: async (v, o) => { m.set(p, o && o.merge ? Object.assign({}, m.get(p) || {}, c(v)) : c(v)); } });
  const coll = n => ({ doc: id => ref(n + '/' + id), get: async () => ({ empty: true, size: 0, docs: [], forEach() {} }), where: () => coll(n), limit: () => coll(n), orderBy: () => coll(n) });
  return { m, db: { collection: coll, runTransaction: async fn => { const ops = [], tx = { get: r => r.get(), set: (r, v, o) => { ops.push(() => r.set(v, o)); return tx; } }; const out = await fn(tx); for (const op of ops) await op(); return out; } } };
}

/* ---- the Search scan: two collections, a strategist that proposes the occasions given ---- */
const months = ['SEPTEMBER', 'OCTOBER', 'NOVEMBER', 'DECEMBER', 'JANUARY', 'FEBRUARY', 'MARCH', 'APRIL', 'MAY', 'JUNE', 'JULY', 'AUGUST'];
const profile = (h, a, b) => ({ handle: h, title: h, sampled: 30, count: 30, typesDetail: [{ type: 'Necklace', n: 30 }], motifs: [{ t: a, n: 20 }, { t: b, n: 10 }], mats: [{ t: 'gold', n: 10 }], personalization: ['engraved'], topProducts: [{ title: a + ' necklace' }] });
const idea = s => ({ text: s, searches: 600, competition: 'LOW', competitionIndex: 30, low: 0.6, high: 1.4, monthly: months.map((_, i) => i === 1 ? 1200 : 300), monthlyEnd: '2026-08' });
const kwFor = w => ['gold moon necklace ' + w, 'gold moon necklaces ' + w, 'engraved star necklace ' + w, 'gold star necklace ' + w, 'engraved moon necklace ' + w, 'moon necklace ' + w + ' gift'];
const PROPOSAL = {
  christmas: { collectionTitle: 'celestial', occasion: 'Christmas', peakDate: '2026-12-25', dateSource: 'calendar', markets: ['US', 'CA'], priority: 'high', market: { fitWhy: 'Moon and star motifs.', demand: 'rising', angle: 'A gift under the tree' }, rationale: 'The biggest gifting season', keywords: kwFor('christmas') },
  halloween: { collectionTitle: 'celestial', occasion: 'Halloween', peakDate: '2026-10-31', dateSource: 'calendar', markets: ['US', 'CA'], priority: 'high', market: { fitWhy: 'Moon and star motifs.', demand: 'rising', angle: 'Glow all night' }, rationale: 'Costume season', keywords: kwFor('halloween') },
  evergreen: { collectionTitle: 'maple', occasion: 'Evergreen gifting', peakDate: null, dateSource: '', priority: 'medium', market: {}, rationale: 'Always on', keywords: ['gold maple necklace', 'gold maple necklaces', 'engraved leaf necklace', 'gold leaf necklace', 'engraved maple necklace', 'maple leaf necklace gift'] }
};
function fakes({ cut = 7, smart = true, props = ['christmas', 'halloween'], onPrompt = () => {}, extra = {} } = {}) {
  return Object.assign({ fb: () => null,
    control: async () => ({ maxDailyBudgetTotal: 100, budgetCurrency: 'CAD', budgetCurrencyVerified: true, defaultCountries: ['2124', '2840'], smartBidding: smart, orderCutoffDays: cut }),
    getCollections: async () => [{ handle: 'celestial', title: 'celestial' }, { handle: 'maple', title: 'maple' }], fetchTopProducts: async () => [], conversionHealth: async () => ({ validated: true }),
    collectionProfiles: async () => ({ list: [profile('celestial', 'moon', 'star'), profile('maple', 'maple', 'leaf')], at: NOW, salesBasis: 'fixture' }), proposePmaxOpportunities: async () => ({ list: [], at: NOW }),
    playbookSlice: async () => null, playbookText: () => '', _learningTrace: () => null,
    storeSalesEvidence: async () => ({ available: false, periods: { days30: null, days90: null }, seasonality: {}, merchant: {}, warnings: [] }),
    _enabledBudgetTotal: async () => 20, storeSignals: async () => ({ orders: 10, totalRevenue: 900, excludedCurrencyOrders: 1, productRows: [] }), collectionAdsPerformance: async () => ({}),
    accountCvr: async () => ({ cvr: 0.02, source: 'benchmark' }), _fxRateToUsd: async () => 0.72, _accountTz: async () => 'America/Toronto', listCountries: async () => [],
    keywordResearch: async (s) => ({ ok: true, status: 200, ideas: s.map(idea) }),
    openaiJSON: async (prompt, opts) => { onPrompt(prompt); if (opts && opts.info) opts.info.sources = []; return { opportunities: JSON.parse(JSON.stringify(props.map(k => PROPOSAL[k]))) }; } }, extra);
}
async function scan(o = {}, now = NOW) {
  const st = store(); // the scan saves its result and its audit, as it does in the app
  let prompt = ''; const e = engine(fakes({ ...o, onPrompt: p => { prompt = p; }, extra: { fb: () => ({ db: st.db, FV: { serverTimestamp: () => null } }), ...(o.extra || {}) } }), now), r = await e.scanOpportunities({ force: true });
  return { e, r, prompt, status: e.t.status(r), check: id => (r.scanAudit.checks || []).find(c => c.id === id), by: label => r.opportunities.find(x => x.occasion === label) };
}

/* ---- Product ads: a few collections with sales that match live products, real research and keyword modules, fake model ---- */
const day = 86400000, wall = Date.now(), E0 = engine();
const item = (vid, title, pid) => ({ title, variantId: 'gid://shopify/ProductVariant/' + vid, productId: 'gid://shopify/Product/' + pid, qty: 1, lineRevenue: 40 });
const order = (id, it, ago) => ({ orderId: String(id), ts: wall - ago * day, value: 40, currency: 'USD', financialStatus: 'PAID', items: [it] });
const aggregate = (rows, days) => util.aggregateOrderEvidence({ rows: rows.filter(r => r.ts >= wall - days * day), days, startAt: wall - days * day, endAt: wall, complete: true, currency: 'USD', normalizeItem: x => x, marginForText: () => ({ rate: .6, tier: 'estimated' }), googlePaid: E0._util.paidAttribution, merchantOrganic: E0._util.merchantOrganic, paidChannel: E0._util.paidChannel });
const SHOP = [
  { handle: 'birds', coll: 'Bird jewelry', pid: '999001', vid: '111001', title: 'Silver bird necklace', market: 'US', orders: 5, img: 'https://cdn.shopify.com/s/files/1/0001/birds.jpg?v=1', price: 48.5, currency: 'USD' },
  { handle: 'pets', coll: 'Pet jewelry', pid: '999002', vid: '111002', title: 'Gold dog charm', market: 'CA', orders: 4, img: 'http://cdn.shopify.com/s/files/1/0001/dog.jpg', price: null, currency: null },
  { handle: 'moon', coll: 'Celestial jewelry', pid: '999003', vid: '111003', title: 'Moon phase necklace', market: 'US', orders: 3, img: 'https://images.example.com/moon.jpg', price: 39, currency: 'cad' }
];
const shopRows = []; let oid = 0; SHOP.forEach(p => { for (let i = 0; i < p.orders; i++) shopRows.push(order(++oid, item(p.vid, p.title, p.pid), 1 + i)); });
const sigs = { 30: aggregate(shopRows, 30), 90: aggregate(shopRows, 90), 365: aggregate(shopRows, 365) };
const offers = SHOP.map(p => ({ itemId: `shopify_${p.market}_${p.pid}_${p.vid}`, title: p.title, feedLabel: p.market, availability: 'IN_STOCK', status: 'ELIGIBLE', merchantId: '123', imageUrl: p.img, price: p.price, currency: p.currency }));
const free = { complete: true, configured: true, rows: [], byId: {}, totals: {}, pages: 1, httpStatuses: [200], valueComplete: true };
async function ads(o = {}, args = {}, now = NOW, hide = []) {
  const audits = [], openaiJSON = async (prompt, opts) => {
    if (opts && opts.info) { opts.info.sources = []; return { nothing: true }; }                     // research: unusable, so the computed research completes the card
    return { pmax: SHOP.map(p => ({ handle: p.handle, feedLabel: p.market, rationale: 'r', dailyBudget: 10, days: 25, angle: 'a' })) }; // the selector still says 25 days: the code decides
  };
  const e = engine({ fb: () => null, _accountTz: async () => 'America/Toronto', backfillOrders: async () => ({ fetched: 0 }), storeSignals: async ({ days }) => sigs[days] || null, merchantProducts: async a => a.itemIds ? [] : offers,
    pmaxProductPerformance: async () => ({ complete: true, monetaryComplete: true, rows: [], byId: {} }), merchantFreeProductPerformance: async () => free, playbookSlice: async () => ({ lessons: [], antiPatterns: [] }),
    takenTags: async () => ({}), openaiJSON, keywordResearchPool: async seeds => ({ ok: true, cached: false, status: 200, ideasByText: Object.fromEntries(seeds.map(s => [s, { text: s, searches: 320, competition: 'LOW' }])), seedCount: seeds.length }), ...o }, now, hide);
  const result = await e.proposePmaxOpportunities({ collections: SHOP.map(p => ({ handle: p.handle, title: p.coll })), profiles: SHOP.map(p => ({ handle: p.handle, topProducts: [{ title: p.title, productId: p.pid }], listingTags: [{ t: p.handle + ' lover gift', n: 6 }] })),
    ceiling: 100, currency: 'USD', onAudit: async x => { audits.push(plain(x)); }, today: '2026-09-29', ...args });
  return { result, audits, e };
}

let pass = 0; async function test(name, fn) { try { await fn(); pass++; } catch (e) { console.error('FAIL ' + name); throw e; } }

(async () => {
  // ================= Search: Christmas, planned early enough to learn =================
  const S1 = await scan();
  const xmas = S1.by('Christmas'), hall1 = S1.by('Halloween');
  await test('Christmas Search from 2026-09-29 on Smart Bidding: learning first, launching mid-November, ending before the last order day', () => {
    assert.ok(xmas, 'Christmas is proposed 87 days ahead: ' + J(S1.r.opportunities.map(o => o.occasion)));
    const s = xmas.schedule;
    assert.equal(s.kind, 'search'); assert.equal(s.timeSensitive, true); assert.equal(J([s.event.label, s.event.date, s.event.daysAway]), J(['Christmas', '2026-12-25', 87]));
    assert.equal(J([s.start, s.end, s.days]), J(['2026-11-14', '2026-12-18', 35]), 'Smart Bidding learns for 2 weeks, then 3 weeks of selling, ending at Dec 25 less the 7-day cutoff');
    assert.equal(J([s.learning.days, s.learning.endsOn, s.verdict]), J([14, '2026-11-27', 'good']));
    assert.ok(s.days - s.learning.days >= 10, 'at least 10 selling days after learning');
    assert.ok(s.end < '2026-12-25' && s.end === '2026-12-18', 'ends before the gift day, on the last order that can arrive');
    // The card's window is the schedule's: one set of dates everywhere.
    assert.equal(J([xmas.startDate, xmas.endDate, xmas.durationDays, xmas.daysOut]), J(['2026-11-14', '2026-12-18', 35, 46]), 'a future start is kept, not pulled forward to today');
    assert.equal(J([xmas.plan.duration.startDate, xmas.plan.duration.endDate, xmas.plan.duration.days, xmas.plan.duration.lastOrderDate]), J(['2026-11-14', '2026-12-18', 35, '2026-12-18']));
    assert.equal(xmas.plan.duration.basis, s.basis, 'one sentence for how long');
    assert.match(s.basis, /^Ends Dec 18 \(Christmas Dec 25 less a 7-day order cutoff\)\./);
    assert.match(S1.status.search.message, /1 occasion was left out because there is too little time to finish Google's learning period before the last gift orders \(Canadian Thanksgiving\)\.$/, 'Canadian Thanksgiving is the one occasion too late on this date, and the message says so');
  });
  await test('the strategist is told the learning period and asked to propose an occasion up to about 75 days before its launch', () => {
    assert.match(S1.prompt, /between 2026-10-24 and 2027-01-24/);
    assert.match(S1.prompt, /Google's learning period \(about 2 weeks on Smart Bidding\)/); assert.match(S1.prompt, /up to about 75 days before the campaign would start/);
    assert.match(S1.prompt, /ends 7 days before the occasion's date, the last day an order can still arrive in time/);
    assert.ok(!/runs for up to 18 days/.test(S1.prompt), 'the old 18-day run is gone');
  });
  await test('Halloween from the same day is a good fit and starts now; an undated idea is an ongoing test whose schedule matches the plan', async () => {
    assert.equal(J([hall1.schedule.start, hall1.schedule.end, hall1.schedule.days, hall1.schedule.verdict]), J(['2026-09-29', '2026-10-24', 26, 'good']));
    const ev = (await scan({ props: ['evergreen'] })).by('Evergreen gifting');
    assert.ok(ev, 'undated idea proposed'); assert.equal(ev.schedule.verdict, 'evergreen'); assert.equal(ev.schedule.timeSensitive, false); assert.equal(ev.schedule.event, null);
    assert.equal(J([ev.schedule.start, ev.schedule.days]), J([ev.startDate, ev.durationDays]), 'the schedule is drawn for the length the plan chose (competition adjusted)');
    assert.equal(ev.schedule.days, ev.plan.duration.days);
  });

  // ================= Search: an occasion that cannot be made in time is skipped, with a reason =================
  const S2 = await scan({ cut: 16 });
  await test('early November launch with a 16-day cutoff; Halloween skipped: too late to finish learning, with a plain reason and the count in the research message', () => {
    const x = S2.by('Christmas'); assert.equal(J([x.schedule.start, x.schedule.end, x.schedule.days]), J(['2026-11-05', '2026-12-09', 35]));
    assert.equal(S2.by('Halloween'), undefined, 'Halloween would leave 3 selling days after learning: not proposed');
    const chk = S2.check('opportunity_schedule'); assert.ok(chk, 'the timing check is in the audit'); assert.equal(chk.status, 'ok');
    const hallo = chk.meta.skipped.find(s => s.occasion === 'Halloween'); assert.ok(hallo, J(chk.meta));
    assert.equal(hallo.reason, "Halloween: too late to finish Google's learning period before the last gift orders"); assert.equal(hallo.date, '2026-10-31');
    assert.equal(chk.meta.occasions, 2, 'Halloween and Canadian Thanksgiving (its last order day was Sep 26)'); assert.equal(chk.meta.tooShort, 1, 'one proposal dropped by the strategist path; the other came from the calendar');
    assert.match(chk.detail, /2 occasions were left out because there is too little time: Canadian Thanksgiving, Halloween\./);
    const dates = S2.check('opportunity_dates'); assert.match(dates.detail, /1 too late, 0 too early \(timing check, 16-day order cutoff\)/);
    const st = S2.status.search; assert.equal(st.skippedCount, 2);
    assert.match(st.message, /2 occasions were left out because there is too little time to finish Google's learning period before the last gift orders \(Canadian Thanksgiving, Halloween\)\./);
    assert.ok(st.message.length < 400 && !/[a-z]+_[a-z]+|\bid\b|https?:/i.test(st.message), 'plain words, no internal ids: ' + st.message);
    assert.equal(J(st.skippedOccasions.map(s => s.occasion)), J(['Canadian Thanksgiving', 'Halloween']));
  });
  await test('the calendar names a familiar occasion that is too late even when the strategist does not propose it (scan on 2026-10-10)', async () => {
    const S3 = await scan({ props: ['christmas'] }, at('2026-10-10'));
    assert.equal(S3.by('Halloween'), undefined); const chk = S3.check('opportunity_schedule');
    assert.equal(J(chk.meta.skipped.map(s => s.occasion)), J(['Canadian Thanksgiving', 'Halloween']));
    assert.equal(chk.meta.skipped[0].reason, 'Canadian Thanksgiving: the last gift orders that can arrive were due Oct 5', 'closed windows say when the last orders were due');
    assert.equal(chk.meta.skipped[1].reason, "Halloween: too late to finish Google's learning period before the last gift orders");
    const x = S3.by('Christmas'); assert.equal(J([x.startDate, x.endDate, x.daysOut]), J(['2026-11-14', '2026-12-18', 35]));
    assert.match(S3.status.search.message, /2 occasions were left out/);
  });
  await test('a proposal whose launch is more than 75 days away is not proposed yet', async () => {
    const far = { ...PROPOSAL.christmas, occasion: "Valentine's Day", peakDate: '2027-02-14', keywords: kwFor('valentine') };
    let sent = false; const e = engine(fakes({ props: [], extra: { openaiJSON: async (p, o) => { if (o && o.info) o.info.sources = []; sent = true; return { opportunities: [far, PROPOSAL.halloween] }; } } }));
    const r = await e.scanOpportunities({ force: true }); assert.ok(sent);
    assert.equal(J(r.opportunities.map(o => o.occasion)), J(['Halloween'])); assert.match(r.scanAudit.checks.find(c => c.id === 'opportunity_dates').detail, /0 too late, 1 too early/);
  });

  // ================= Serving late: the start moves to today and the schedule follows =================
  const serve = (list, now) => engine(fakes({ extra: { scanOpportunities: async () => ({ opportunities: plain(list), scannedAt: now - 3600000, searchResearchVersion: 3, pmaxList: [] }), _deletedOpportunityTags: async () => new Set(), takenTags: async () => ({}) } }), now).opportunitiesWithStatus({});
  await test('acting late: on Nov 20 the Christmas run starts today, keeps the last order day, and the schedule, days and spend agree', async () => {
    const x = (await serve([xmas], at('2026-11-20'))).opportunities[0], s = x.schedule;
    assert.equal(J([x.startDate, x.daysOut, x.durationDays]), J(['2026-11-20', 0, 29]));
    assert.equal(J([s.today, s.start, s.end, s.days, s.verdict, s.event.daysAway]), J(['2026-11-20', '2026-11-20', '2026-12-18', 29, 'good', 35]));
    assert.equal(x.plan.duration.days, 29); assert.match(x.plan.duration.basis, /^Now runs 29 days \(researched as 35\)/);
    assert.equal(x.estTotalSpend, Math.round(xmas.estTotalSpend * 29 / 35), 'the total scales with the days actually run');
    assert.equal(s.days, x.durationDays); assert.match(s.basis, /^Ends Dec 18 \(Christmas Dec 25 less a 7-day order cutoff\)\. Counted back from there: about 2 weeks of learning, then 15 days of selling\.$/);
    assert.equal(x.eligibility.ready, xmas.eligibility.ready, 'still a good fit, so still ready');
  });
  await test('acting too late: on Dec 5 learning cannot finish, so the card is no longer ready and says why', async () => {
    const x = (await serve([xmas], at('2026-12-05'))).opportunities[0];
    assert.equal(x.schedule.verdict, 'too_short'); assert.equal(x.eligibility.ready, false); assert.equal(x.eligibility.label, 'Too late for this occasion');
    assert.match(x.eligibility.reason, /^Too late to test this for Christmas: learning would end only 0 days before the last gift orders can arrive|^Too late to test this for Christmas/);
    assert.equal(J([x.startDate, x.durationDays]), J(['2026-12-05', 14]));
  });
  await test('read before the start: the start stays in the future and only the days-to-go follow the clock', async () => {
    const x = (await serve([xmas], at('2026-10-15'))).opportunities[0];
    assert.equal(J([x.startDate, x.daysOut, x.schedule.start, x.schedule.end, x.schedule.today, x.schedule.event.daysAway]), J(['2026-11-14', 30, '2026-11-14', '2026-12-18', '2026-10-15', 71]));
  });

  // ================= Search draft: a future start is honoured =================
  const drafting = (list, over = {}) => {
    const out = { approvals: [], built: [] }, st = store({ 'Brites_GAds_State/opportunities': { list } });
    const fns = Object.assign({}, fakes(), { fb: () => ({ db: st.db, FV: { serverTimestamp: () => null } }), loadCalendar: async () => ({}), accountWasteNegatives: async () => [], recordOccasionUse: async () => {}, gaql: async () => [],
      generateRSAAssets: async () => ({ headlines: Array.from({ length: 15 }, (_, i) => 'Headline ' + i), descriptions: Array.from({ length: 4 }, (_, i) => 'Description ' + i) }),
      enqueueApproval: async item => { out.approvals.push(item); return 'ap' + out.approvals.length; },
      buildSearchCampaignOps: (coll, event, assets, o) => { out.built.push(o); return { ops: [{ campaignOperation: { create: Object.assign({}, o.startDate ? { startDateTime: o.startDate + ' 00:00:00' } : {}, o.endDate ? { endDateTime: o.endDate + ' 23:59:59' } : {}) } }], tag: 'celestial-christmas', negatives: [], assetSummary: null, keywordSummary: { count: (o.keywordPlan || []).length, measured: (o.keywordPlan || []).length, researched: true, exact: 0 }, adGroupSummary: [] }; } }, over);
    return { e: engine(fns), out };
  };
  const ctrlSmart = { maxDailyBudgetTotal: 100, budgetCurrency: 'CAD', budgetCurrencyVerified: true, defaultCountries: ['2124', '2840'], smartBidding: true, orderCutoffDays: 7 };
  await test('the draft for a scanned Christmas card, and one built with no dates, both schedule the campaign for the later start and end at the last order day', async () => {
    const card = drafting([xmas]), g = await card.e.generateForCollection('celestial', 'Christmas', 0, { ctrl: ctrlSmart, startDate: xmas.startDate, endDate: xmas.endDate, countries: ['2124'], peakDate: xmas.peakDate });
    assert.equal(g.ok, true, g.reason); assert.equal(J([g.startDate, g.endDate]), J(['2026-11-14', '2026-12-18']));
    assert.equal(card.out.built[0].startDate, '2026-11-14'); assert.match(card.out.approvals[0].summary, /\(2026-11-14 → 2026-12-18, 35d\)/); assert.equal(card.out.approvals[0].payload.startDate, '2026-11-14');
    const none = drafting([xmas]), g2 = await none.e.generateForCollection('celestial', 'Christmas', 0, { ctrl: ctrlSmart, countries: ['2124'] });
    assert.equal(g2.ok, true, g2.reason); assert.equal(J([g2.startDate, g2.endDate, g2.plan.duration.days]), J(['2026-11-14', '2026-12-18', 35]), 'no chosen dates: the same learning-aware window as the card, not the old 17 days');
    const f = E0.t.fields('2026-11-14', '2026-12-18'); // the fields Google receives for that window
    assert.equal(J([f.startDateTime, f.endDateTime]), J(['2026-11-14 00:00:00', '2026-12-18 23:59:59']), 'a future start is sent as a scheduled start');
  });

  // ================= Product ads: the schedule, days, and the listing photo, price and currency =================
  const A1 = await ads();
  await test('every Product ads idea carries a schedule for its market\'s main occasion, and days follow it', () => {
    const list = A1.result.list; assert.equal(list.length, 3);
    for (const o of list) {
      const s = o.schedule; assert.ok(s, o.handle + ' has a schedule'); assert.equal(s.kind, 'pmax'); assert.equal(s.timeSensitive, true); assert.equal(o.days, s.days, 'days is the schedule\'s'); assert.equal(o.days, 42, 'not the selector\'s 25');
      assert.equal(s.verdict, 'good'); assert.equal(s.learning.days, 21); assert.equal(s.end, Sched.addDays(s.event.date, -7), 'ends at the last order day'); assert.equal(Sched.daysBetween(s.start, s.end) + 1, 42);
      assert.equal(s.judgeAfter, Sched.addDays(s.start, 42)); noUndefined(o);
    }
    const by = h => list.find(o => o.handle === h).schedule;
    assert.equal(J([by('birds').event.label, by('birds').start, by('birds').end]), J(['Thanksgiving', '2026-10-09', '2026-11-19']), 'US: Thanksgiving is the main occasion');
    assert.equal(J([by('pets').event.label, by('pets').start, by('pets').end]), J(['Black Friday', '2026-10-10', '2026-11-20']), 'CA: Canadian Thanksgiving is too close, Black Friday is the main occasion');
    assert.equal(A1.result.list[0].dailyBudget * A1.result.list[0].days, A1.result.list[0].dailyBudget * 42, 'planned spend = daily budget x days of the schedule');
  });
  await test('with no season calendar an idea is an ongoing test of the usual length; without the schedule module nothing is guessed', async () => {
    const down = await ads({}, {}, NOW, ['./_googleAdsPmaxResearch', './_googleAdsPmaxKeywords']); assert.equal(down.result.list.length, 3, 'the ideas still appear');
    for (const o of down.result.list) { assert.equal(J([o.schedule.verdict, o.schedule.timeSensitive, o.schedule.event, o.schedule.start, o.schedule.end, o.schedule.days, o.days]), J(['evergreen', false, null, '2026-09-29', '2026-11-09', 42, 42])); }
    const none = await ads({}, {}, NOW, ['./_googleAdsSchedule']); assert.equal(none.result.list.length, 3);
    for (const o of none.result.list) { assert.ok(!('schedule' in o), 'no schedule field'); assert.ok(o.days >= 21 && o.days <= 45, 'days keeps its usual clamp'); }
    const search = engine(fakes({ props: ['halloween'], extra: { fb: () => ({ db: store().db, FV: {} }) } }), NOW, ['./_googleAdsSchedule']), r = await search.scanOpportunities({ force: true }), h = r.opportunities.find(o => o.occasion === 'Halloween');
    assert.ok(h && !('schedule' in h), 'Search still proposes what it always did'); assert.equal(J([h.startDate, h.endDate]), J(['2026-10-07', '2026-10-24']), 'the planner\'s own 18-day window (last order day less 17) when the schedule module is missing');
  });
  await test('served the next day, an event-tied Product ads schedule keeps its dates and its days-to-go follow the clock', async () => {
    const e = engine(fakes({ extra: { scanOpportunities: async () => ({ opportunities: [], pmaxList: plain(A1.result.list), scannedAt: NOW, pmaxAt: NOW, searchResearchVersion: 3, pmaxResearchVersion: 3 }), _deletedOpportunityTags: async () => new Set(), takenTags: async () => ({}) } }), at('2026-10-02'));
    const served = (await e.opportunitiesWithStatus({})).pmaxList, b = served.find(o => o.handle === 'birds');
    assert.equal(J([b.schedule.today, b.schedule.start, b.schedule.end, b.schedule.event.daysAway, b.days]), J(['2026-10-02', '2026-10-09', '2026-11-19', 55, 42]));
  });
  await test('offerDetails carry a listing photo only from an allowed https host, and price and currency when present', () => {
    const d = plain(A1.result.list.find(o => o.handle === 'birds').offerDetails[0]), pets = plain(A1.result.list.find(o => o.handle === 'pets').offerDetails[0]), moon = plain(A1.result.list.find(o => o.handle === 'moon').offerDetails[0]);
    assert.equal(J([d.imageUrl, d.price, d.currency]), J(['https://cdn.shopify.com/s/files/1/0001/birds.jpg?v=1', 48.5, 'USD']));
    assert.ok(!('imageUrl' in pets) && !('price' in pets) && !('currency' in pets), 'an http photo and a missing price are left out, not nulled: ' + J(pets));
    assert.ok(!('imageUrl' in moon), 'a photo from another host is left out'); assert.equal(J([moon.price, moon.currency]), J([39, 'CAD']), 'currency is upper-cased');
    for (const o of A1.result.list.flatMap(x => x.offerDetails)) { if (o.imageUrl) assert.ok(Thumbs.safeImageUrl(o.imageUrl), 'the preview accepts every photo the engine sends'); assert.ok(o.price === undefined || typeof o.price === 'number'); }
  });
  await test('the engine\'s photo rule is the preview module\'s rule', () => {
    assert.equal(J(E0.t.hosts), J(Thumbs.IMAGE_HOSTS));
    const urls = ['https://cdn.shopify.com/a.jpg', 'https://www.britesjewelry.com/a.jpg', 'https://britesjewelry.com/a.jpg', 'http://cdn.shopify.com/a.jpg', 'https://evil.example.com/a.jpg', 'https://cdn.shopify.com.evil.com/a.jpg', 'https://user:pw@cdn.shopify.com/a.jpg',
      'https://cdn.shopify.com:8443/a.jpg', 'https://CDN.SHOPIFY.COM/A.jpg', 'data:image/png;base64,AAAA', 'javascript:alert(1)', '', '   ', null, undefined, 42, { u: 1 }, 'https://cdn.shopify.com/' + 'a'.repeat(2100), ' https://cdn.shopify.com/a.jpg '];
    for (const u of urls) assert.equal(E0.t.image(u) || '', Thumbs.safeImageUrl(u), 'same verdict for ' + String(J(u)).slice(0, 60));
    assert.equal(E0.t.preview({ imageUrl: 'https://cdn.shopify.com/' + 'b'.repeat(590), price: 10, currency: 'CAD' }).imageUrl, undefined, 'a very long URL is left out to keep the saved document small');
    assert.equal(J(E0.t.preview({ price: 0, currency: 'CAD' })), '{}'); assert.equal(J(E0.t.preview({ price: 12.5 })), '{"price":12.5}'); assert.equal(J(E0.t.preview({ price: 12.5, currency: 'dollars' })), '{"price":12.5}');
  });
  await test('the Merchant row reads price and currency, and a Google rejection of the new fields falls back without breaking the catalogue read', async () => {
    const shop = { merchantCenterId: '123', itemId: 'shopify_CA_1_2', title: 'Moon Ring', status: 'ELIGIBLE', availability: 'IN_STOCK', feedLabel: 'CA', productImageUri: 'https://cdn.shopify.com/a.jpg', priceMicros: '48500000', currencyCode: 'cad' };
    const row = plain(E0.t.row(shop, '123')); assert.equal(J([row.price, row.currency, row.imageUrl]), J([48.5, 'CAD', 'https://cdn.shopify.com/a.jpg']));
    assert.equal(J([E0.t.row({ ...shop, priceMicros: undefined }, '123').price, E0.t.row({ ...shop, priceMicros: '0' }, '123').price, E0.t.row({ ...shop, priceMicros: 'abc' }, '123').price]), J([null, null, null]));
    const run = async reject => { const seen = []; const e = engine({ merchantCenterId: async () => '123', gaql: async q => { seen.push(q); if (reject(q)) throw new Error('INVALID_ARGUMENT: unrecognized field'); return [{ shoppingProduct: shop }]; } });
      const list = await e.merchantProducts({ force: true, itemIds: ['shopify_CA_1_2'] }); return { list, seen, diag: list._diag }; };
    const all = await run(() => false); assert.equal(all.seen.length, 1); assert.match(all.seen[0], /shopping_product\.price_micros, shopping_product\.currency_code/); assert.equal(all.list[0].price, 48.5);
    const noPrice = await run(q => /price_micros/.test(q)); assert.equal(noPrice.seen.length, 2); assert.ok(!/price_micros/.test(noPrice.seen[1]) && /product_image_uri/.test(noPrice.seen[1]), 'the second read keeps the photo and the types');
    assert.equal(J([noPrice.list.length, noPrice.list[0].imageUrl, noPrice.diag.queryModes.enriched, noPrice.diag.queryModes.coreFallback]), J([1, 'https://cdn.shopify.com/a.jpg', 1, 0]));
    const core = await run(q => /price_micros|product_image_uri/.test(q)); assert.equal(core.seen.length, 3); assert.equal(J([core.list.length, core.diag.queryModes.coreFallback]), J([1, 1])); assert.ok(!/price_micros|product_image_uri/.test(core.seen[2]));
  });

  console.log(`opportunity-schedule-engine: ${pass} checks passed`);
})().catch(e => { console.error(e); process.exit(1); });

function noUndefined(v, at = 'value') { if (v === undefined) throw new Error(at + ' is undefined'); if (v && typeof v === 'object') Object.keys(v).forEach(k => noUndefined(v[k], at + '.' + k)); }
