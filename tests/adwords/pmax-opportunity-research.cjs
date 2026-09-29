// Product ads (PMax) opportunity research in the engine: season calendar, keywords, the model's reasoning with a computed fallback,
// same-day reuse, plain "unavailable" naming in the research status, and the "why so few ideas" funnel.
// Everything is fake: the two research helper modules are stubs that follow the contract shapes, openaiJSON records its prompts and
// answers with contract JSON, and the Keyword Planner pool is a stub. No model, no network, no Google Ads.
const assert = require('node:assert/strict'), fs = require('fs'), vm = require('vm'), path = require('path');
const root = path.resolve(__dirname, '../../netlify/functions'), source = fs.readFileSync(root + '/googleAdsAutopilot.js', 'utf8');
const util = require(root + '/googleAdsSalesEvidence.js');
const plain = x => JSON.parse(JSON.stringify(x));

/* ---- stubs of the two helper modules (exact contract shapes; deterministic, no network, no model) ---- */
function makeModules(spy) {
  const R = {
    RESEARCH_VERSION: 1,
    timingWindows({ today, occasions, markets, orderCutoffDays = 0, learningDays = 42 }) {
      const base = Date.parse(today + 'T00:00:00Z'), rows = [];
      (occasions || []).forEach(o => (markets || []).forEach(m => {
        if (o.markets && o.markets.length && !o.markets.includes(m)) return;
        rows.push({ label: o.label, date: o.date, daysAway: Math.round((Date.parse(o.date + 'T00:00:00Z') - base) / 864e5), market: m, role: 'also', startBy: null, note: o.approx ? 'approximate date' : 'calendar date' });
      }));
      rows.sort((a, b) => a.daysAway - b.daysAway);
      const main = rows.find(r => r.daysAway >= learningDays + orderCutoffDays);
      rows.forEach(r => { r.role = r === main ? 'main' : r.daysAway < learningDays + orderCutoffDays ? 'too-late' : 'also'; if (r === main) r.startBy = today; });
      return rows.slice(0, 4);
    },
    evidenceFacts(c) { return [`${(c.evidenceTotals || {}).orders || 0} orders in the last 90 days`]; },
    computedResearch(c, ctx) {
      spy.computed.push({ handle: c.handle, ctx });
      const main = ctx.timing.find(t => t.role === 'main');
      return { version: 1, at: ctx.at, today: ctx.today, source: 'computed', model: null,
        headline: `${c.productTitles[0]} ahead of ${main ? main.label + ' on ' + main.date : 'the next season'}`.slice(0, 140),
        whyNow: { summary: 'Computed timing summary.', timing: ctx.timing, evidence: this.evidenceFacts(c), marketRead: null, caution: null },
        listingFit: (c.itemIds || []).slice(0, 8).map(id => ({ itemId: id, title: 'Listing ' + id, reason: 'Sold in your store recently', role: 'hero' })),
        keywords: ctx.keywords, creativeAngles: ['Computed gift angle'], sources: [], limits: [] };
    },
    researchPrompt(c, ctx) {
      return `RESEARCH_PROMPT\nToday: ${ctx.today}\nCollection: ${c.collectionTitle}\nOffers: ${(c.itemIds || []).join(', ')}\nMarkets: ${ctx.markets.join(',')}\nTiming: ${JSON.stringify(ctx.timing)}`;
    },
    normalizeResearch(raw, c, ctx) {
      spy.normalize.push({ handle: c.handle, ctx });
      if (!raw || !raw.headline) return null;
      return { version: 1, at: ctx.at, today: ctx.today, source: 'ai', model: ctx.modelLabel,
        headline: String(raw.headline).slice(0, 140),
        whyNow: { summary: raw.summary || '', timing: [], evidence: [], marketRead: raw.marketRead || null, caution: null },
        listingFit: raw.listingFit || [], keywords: [], creativeAngles: raw.creativeAngles || [], sources: (ctx.sources || []).map(s => ({ title: s.title, url: s.url })), limits: [] };
    },
    mergeResearch(computed, ai) {
      return { ...computed, ...ai, source: 'ai', keywords: computed.keywords, listingFit: ai.listingFit.length ? ai.listingFit : computed.listingFit, whyNow: { ...ai.whyNow, timing: computed.whyNow.timing, evidence: computed.whyNow.evidence } };
    },
    researchFingerprint(c, today) { return [c.handle, c.feedLabel || '', (c.itemIds || []).join(','), today].join('|'); }
  };
  const K = {
    keywordCandidates(c, opts) {
      spy.keywordOpts.push({ handle: c.handle, opts });
      const t = c.productTitles[0].toLowerCase();
      return [{ text: t, kind: 'product', reason: 'Named on the listing' }, { text: t + ' gift', kind: 'occasion', reason: 'Gift buyers' }].concat((opts.tags || []).slice(0, 1).map(x => ({ text: String(x).toLowerCase(), kind: 'style', reason: 'A tag the store uses' })));
    },
    plannerSeeds(cands, limit) { return cands.map(x => x.text).slice(0, limit); },
    rankKeywords(cands, ideas, { limit = 12 } = {}) {
      return cands.slice(0, limit).map(x => { const i = ideas[x.text]; return { text: x.text, reason: x.reason, kind: x.kind, monthlySearches: i ? i.searches : null, competition: i ? String(i.competition).toLowerCase() : null }; });
    },
    themesFromKeywords(keywords) { return keywords.map(k => k.text).slice(0, 10); }
  };
  return { R, K };
}

/* ---- the engine in a vm, with the helper modules and every outside dependency injected ---- */
function engine(mocks = {}) {
  const real = require('module').createRequire(root + '/googleAdsAutopilot.js');
  const ctx = { module: { exports: {} }, process: { env: { GADS_CURRENCY: 'USD', GMC_REFRESH_TOKEN: 'mock' } }, URL, URLSearchParams, Intl, Date, console, Buffer, setTimeout, clearTimeout, AbortController, mocks,
    require: n => n === 'node-fetch' ? (mocks.fetch || (() => { throw Error('Unexpected network'); }))
      : n === './_googleAdsPmaxResearch' ? (() => { if (mocks.real) return real(n); if (!mocks.R) throw new Error("Cannot find module './_googleAdsPmaxResearch'"); return mocks.R; })()
      : n === './_googleAdsPmaxKeywords' ? (() => { if (mocks.real) return real(n); if (!mocks.K) throw new Error("Cannot find module './_googleAdsPmaxKeywords'"); return mocks.K; })()
      : real(n) };
  vm.createContext(ctx);
  vm.runInContext(source + '\nmodule.exports.testResearchStatus=_opportunityResearchStatus;module.exports.testOccasions=_pmaxUpcomingOccasions;module.exports.testSrc={scan:scanOpportunities.toString(),propose:proposePmaxOpportunities.toString()};', ctx);
  vm.runInContext('fb=mocks.fb||(()=>null);if(mocks.tz)_accountTz=mocks.tz;if(mocks.backfillOrders)backfillOrders=mocks.backfillOrders;if(mocks.storeSignals)storeSignals=mocks.storeSignals;if(mocks.merchantProducts)merchantProducts=mocks.merchantProducts;if(mocks.pmaxProductPerformance)pmaxProductPerformance=mocks.pmaxProductPerformance;if(mocks.merchantFreeProductPerformance)merchantFreeProductPerformance=mocks.merchantFreeProductPerformance;if(mocks.playbookSlice)playbookSlice=mocks.playbookSlice;if(mocks.openaiJSON)openaiJSON=mocks.openaiJSON;if(mocks.takenTags)takenTags=mocks.takenTags;if(mocks.keywordResearchPool)keywordResearchPool=mocks.keywordResearchPool;if(mocks.deleted)_deletedOpportunityTags=mocks.deleted;', ctx);
  return ctx.module.exports;
}

/* ---- the store: five collections, three with sales that match a live product, one that sold but is out of stock, one with no sales ---- */
const day = 86400000, now = Date.now(), E0 = engine();
const item = (vid, title, pid) => ({ title, variantId: 'gid://shopify/ProductVariant/' + vid, productId: 'gid://shopify/Product/' + pid, qty: 1, lineRevenue: 40 });
const order = (id, it, ago = 2) => ({ orderId: String(id), ts: now - ago * day, value: 40, currency: 'USD', financialStatus: 'PAID', items: [it] });
const aggregate = (rows, days) => util.aggregateOrderEvidence({ rows: rows.filter(r => r.ts >= now - days * day), days, startAt: now - days * day, endAt: now, complete: true, currency: 'USD', normalizeItem: x => x, marginForText: () => ({ rate: .6, tier: 'estimated' }), googlePaid: E0._util.paidAttribution, merchantOrganic: E0._util.merchantOrganic, paidChannel: E0._util.paidChannel });
const P = [
  { handle: 'birds', coll: 'Bird jewelry', pid: '999001', vid: '111001', title: 'Silver bird necklace', market: 'US', orders: 5 },
  { handle: 'pets', coll: 'Pet jewelry', pid: '999002', vid: '111002', title: 'Gold dog charm', market: 'CA', orders: 4 },
  { handle: 'moon', coll: 'Celestial jewelry', pid: '999003', vid: '111003', title: 'Moon phase necklace', market: 'US', orders: 3 },
  { handle: 'ocean', coll: 'Ocean jewelry', pid: '999004', vid: '111004', title: 'Silver wave bracelet', market: 'US', orders: 2, outOfStock: true }
];
const collections = P.map(p => ({ handle: p.handle, title: p.coll })).concat([{ handle: 'quiet', title: 'Quiet jewelry' }]);
const profiles = P.map(p => ({ handle: p.handle, topProducts: [{ title: p.title, productId: p.pid }], listingTags: [{ t: p.handle + ' lover gift', n: 6 }] })).concat([{ handle: 'quiet', topProducts: [{ title: 'Unsold lantern pendant', productId: '999099' }] }]);
const rows = []; let oid = 0; P.forEach(p => { for (let i = 0; i < p.orders; i++) rows.push(order(++oid, item(p.vid, p.title, p.pid), 1 + i)); });
const sigs = { 30: aggregate(rows, 30), 90: aggregate(rows, 90), 365: aggregate(rows, 365) };
const offers = P.map(p => ({ itemId: `shopify_${p.market}_${p.pid}_${p.vid}`, title: p.title, feedLabel: p.market, availability: p.outOfStock ? 'OUT_OF_STOCK' : 'IN_STOCK', status: p.outOfStock ? 'NOT_ELIGIBLE' : 'ELIGIBLE', merchantId: '123' }));
const free = { complete: true, configured: true, rows: [], byId: {}, totals: {}, pages: 1, httpStatuses: [200], valueComplete: true };

// One scan of the product-ads research with everything faked. `over` replaces any dependency.
function run(over = {}, args = {}) {
  const spy = { computed: [], normalize: [], keywordOpts: [], prompts: [], selector: [], research: [], pools: [] }, audits = [];
  const { R, K } = makeModules(spy); if (over.themes) K.themesFromKeywords = over.themes;
  const picks = over.picks || [{ handle: 'birds', feedLabel: 'US' }, { handle: 'pets', feedLabel: 'CA' }, { handle: 'moon', feedLabel: 'US' }];
  const openaiJSON = over.openaiJSON || (async (prompt, opts) => {
    spy.prompts.push({ prompt, opts });
    if (opts && opts.info) {
      spy.research.push({ prompt, opts });
      if (over.researchThrows) throw over.researchThrows;
      opts.info.sources = [{ title: 'Gift guide', url: 'https://example.com/gifts', pageAge: null }]; opts.info.costUsd = 0.05; opts.info.searches = 2; opts.info.model = 'fake';
      const coll = (prompt.match(/Collection: (.+)/) || prompt.match(/covers the "([^"]+)" collection/) || [])[1];
      return over.researchAnswer ? over.researchAnswer(coll, prompt) : { headline: `AI headline for ${coll}`, summary: 'Written by the fake model.', marketRead: 'Fake market read.', listingFit: [], creativeAngles: [`AI angle for ${coll}`] };
    }
    spy.selector.push({ prompt, opts });
    return { pmax: picks.map(p => ({ ...p, rationale: 'SELECTOR RATIONALE', dailyBudget: 10, days: 30, angle: 'SELECTOR ANGLE' })) };
  });
  const x = engine({ real: !!over.real, R: over.noModules ? null : R, K: over.noModules ? null : K, tz: over.tz || (async () => 'America/Toronto'), fb: over.fb,
    backfillOrders: async () => ({ fetched: 0 }), storeSignals: over.storeSignals || (async ({ days }) => sigs[days] || null), merchantProducts: over.merchantProducts || (async a => a.itemIds ? [] : offers),
    pmaxProductPerformance: async () => ({ complete: true, monetaryComplete: true, rows: [], byId: {} }), merchantFreeProductPerformance: async () => free, playbookSlice: async () => ({ lessons: [], antiPatterns: [] }),
    takenTags: over.takenTags || (async () => ({})), openaiJSON,
    keywordResearchPool: over.keywordResearchPool || (async (seeds, geo) => { spy.pools.push({ seeds, geo }); return { ok: true, cached: false, status: 200, ideasByText: Object.fromEntries(seeds.map(s => [s, { text: s, searches: 320, competition: 'LOW' }])), seedCount: seeds.length, chunkCount: 1 }; }) });
  const promise = x.proposePmaxOpportunities({ collections: over.collections || collections, profiles: over.profiles || profiles, ceiling: 100, currency: 'USD', onAudit: async e => { audits.push(plain(e)); }, ...args });
  return promise.then(result => ({ result, spy, audits, x }));
}
const ymdIn = tz => new Intl.DateTimeFormat('en-CA', { timeZone: tz, year: 'numeric', month: '2-digit', day: '2-digit' }).format(new Date());
const audit = (audits, id) => audits.filter(e => e.id === id).pop();
function noUndefined(v, at = 'value') { if (v === undefined) throw new Error(at + ' is undefined'); if (v && typeof v === 'object') Object.keys(v).forEach(k => noUndefined(v[k], at + '.' + k)); }

let pass = 0; async function test(name, fn) { try { await fn(); pass++; } catch (e) { console.error('FAIL ' + name); throw e; } }

(async () => {
  await test('season calendar: dated occasions for the pool markets, within about 240 days, only where observed', () => {
    const both = plain(E0.testOccasions('2026-09-29', ['US', 'CA'])), byLabel = Object.fromEntries(both.map(o => [o.label, o]));
    assert.equal(byLabel['Halloween'].date, '2026-10-31');
    assert.equal(byLabel['Canadian Thanksgiving'].date, '2026-10-12'); assert.deepEqual(byLabel['Canadian Thanksgiving'].markets, ['CA']);
    assert.equal(byLabel['Thanksgiving'].date, '2026-11-26'); assert.deepEqual(byLabel['Thanksgiving'].markets, ['US']);
    assert.equal(byLabel['Black Friday'].date, '2026-11-27'); assert.equal(byLabel['Cyber Monday'].date, '2026-11-30'); assert.equal(byLabel['Christmas'].date, '2026-12-25');
    assert.equal(byLabel["Galentine's Day"].date, '2027-02-13'); assert.equal(byLabel["Valentine's Day"].date, '2027-02-14'); assert.equal(byLabel["International Women's Day"].date, '2027-03-08');
    assert.equal(byLabel["Mother's Day"].date, '2027-05-09');
    assert.ok(!byLabel['Graduation'] && !byLabel["Father's Day"] && !byLabel['Back to school'], 'later than 240 days');
    assert.deepEqual(both.map(o => o.date), both.map(o => o.date).slice().sort(), 'soonest first');
    both.forEach(o => assert.ok((Date.parse(o.date) - Date.parse('2026-09-29')) / day <= 240 && o.date >= '2026-09-29'));
    const us = plain(E0.testOccasions('2026-09-29', ['US'])).map(o => o.label), ca = plain(E0.testOccasions('2026-09-29', ['CA'])).map(o => o.label);
    assert.ok(!us.includes('Canadian Thanksgiving') && us.includes('Thanksgiving')); assert.ok(!ca.includes('Thanksgiving') && ca.includes('Canadian Thanksgiving'));
    assert.equal(plain(E0.testOccasions('2026-09-29', ['US'])).find(o => o.label === 'Halloween').markets.join(), 'US');
    assert.ok(plain(E0.testOccasions('2027-05-20', ['US'])).some(o => o.label === 'Graduation' && o.approx === true));
    assert.deepEqual(plain(E0.testOccasions('not a date', ['US'])), []);
  });

  await test('happy path: three ideas, each with dated timing, listing fit and keywords; the model wrote the headline', async () => {
    const { result, spy, audits } = await run(), today = ymdIn('America/Toronto');
    assert.equal(result.list.length, 3);
    for (const o of result.list) {
      const r = o.research; assert.ok(r, o.handle + ' has research');
      assert.equal(r.source, 'ai'); assert.equal(r.model, 'Sonnet 5.5'); assert.equal(r.today, today); assert.equal(r.version, 1);
      assert.match(r.headline, /^AI headline for /); assert.ok(r.whyNow.timing.length > 0, 'timing'); assert.ok(r.whyNow.timing.every(t => t.market === o.feedLabel), 'timing only for this idea\'s market');
      assert.ok(r.listingFit.length > 0 && r.listingFit.every(f => o.itemIds.includes(f.itemId))); assert.ok(r.keywords.length >= 2);
      assert.deepEqual(plain(o.searchThemes), plain(r.keywords.map(k => k.text))); assert.equal(o.rationale, r.headline); assert.equal(o.angle, r.creativeAngles[0]);
      assert.notEqual(o.rationale, 'SELECTOR RATIONALE'); assert.ok(!/observed product-order matches/.test(o.rationale));
      assert.ok(o.researchFingerprint.includes(today)); assert.equal(r.sources.length, 1); assert.equal(r.sources[0].url, 'https://example.com/gifts');
      o.searchThemes.forEach(t => assert.ok(t.split(' ').length <= 10 && t.length <= 80 && /^[a-z0-9 ]+$/.test(t)));
      noUndefined(o);
    }
    const birds = result.list.find(o => o.handle === 'birds'); assert.ok(birds.research.keywords.every(k => k.monthlySearches === 320)); assert.ok(birds.research.keywords.some(k => k.text === 'birds lover gift'), 'the listing tags reach the keyword builder');
    // The model was asked about every idea with today's date and every one of its offers, with a small web-search budget and no more than 6000 tokens.
    assert.equal(spy.research.length, 3);
    for (const call of spy.research) {
      const o = result.list.find(x => call.prompt.includes('Collection: ' + x.collectionTitle)); assert.ok(o);
      assert.ok(call.prompt.includes('Today: ' + today)); o.itemIds.forEach(id => assert.ok(call.prompt.includes(id), 'prompt names offer ' + id));
      assert.equal(call.opts.maxTokens, 6000); assert.equal(call.opts.effort, 'medium'); assert.equal(call.opts.webSearch.maxUses, 3); assert.equal(call.opts.webSearch.userLocation.country, o.feedLabel === 'CA' ? 'CA' : 'US'); assert.equal(typeof call.opts.info, 'object');
    }
    assert.ok(spy.normalize.every(n => n.ctx.sources.length === 1 && /Sonnet/.test(n.ctx.modelLabel) && n.ctx.today === today), 'the pages the model read are handed to normalizeResearch');
    // One Keyword Planner call for all three ideas together, in both markets.
    assert.equal(spy.pools.length, 1); assert.deepEqual(plain(spy.pools[0].geo).sort(), ['2124', '2840']); assert.ok(spy.pools[0].seeds.length >= 6 && spy.pools[0].seeds.length <= 60);
    // The selector saw today's date, the dated windows for both markets, and was told to return three.
    assert.equal(spy.selector.length, 1); const sp = spy.selector[0].prompt;
    assert.ok(sp.includes(`Today is ${today} in the ad account's time zone`)); assert.match(sp, /- US: Halloween on \d{4}-10-31/); assert.match(sp, /- CA: Canadian Thanksgiving on \d{4}-10-1\d/); assert.match(sp, /Choose 3 market-specific/);
    assert.match(sp, /\$\{?USD\}? ?6-15\/day|USD 6-15\/day, respecting total ceiling USD 100\/day/);
    // Audit events.
    assert.equal(audit(audits, 'pmax_calendar').status, 'ok'); assert.match(audit(audits, 'pmax_calendar').detail, new RegExp('Today is ' + today + '.*Main occasion'));
    assert.equal(audit(audits, 'pmax_keyword_research').status, 'ok'); assert.match(audit(audits, 'pmax_keyword_research').detail, /Search volumes found for \d+ of \d+ keywords/);
    const ai = audit(audits, 'pmax_ai_research'); assert.equal(ai.status, 'ok'); assert.equal(ai.meta.written, 3); assert.equal(ai.meta.computed, 0); assert.equal(ai.meta.reused, 0); assert.equal(ai.meta.searches, 6); assert.equal(ai.meta.costUsd, 0.15);
    assert.ok(ai.sources.length >= 1 && ai.sources[0].url === 'https://example.com/gifts');
    assert.ok(!JSON.stringify(audits).includes('RESEARCH_PROMPT'), 'prompts are never recorded in the audit');
    assert.ok(audit(audits, 'pmax_ai_selector') && audit(audits, 'pmax_selector_fallback').status === 'skipped', 'existing audit ids keep working');
  });

  await test('the model fails: every idea still has complete computed research, and the audit names the plain reason', async () => {
    const { result, audits } = await run({ researchThrows: new Error('Claude request failed at https://api.example.com/v1/messages?key=sk-SECRET-123 after timeout 200000ms') });
    assert.equal(result.list.length, 3);
    for (const o of result.list) {
      assert.equal(o.research.source, 'computed'); assert.equal(o.research.model, null); assert.ok(o.research.headline && o.research.whyNow.timing.length && o.research.listingFit.length && o.research.keywords.length);
      assert.equal(o.rationale, o.research.headline); assert.ok(!/observed product-order matches/.test(o.rationale)); assert.equal(o.angle, 'Computed gift angle'); assert.ok(o.searchThemes.length >= 2);
    }
    const ai = audit(audits, 'pmax_ai_research'); assert.equal(ai.status, 'warning'); assert.match(ai.detail, /^Written from your own data only: the request timed out/); assert.equal(ai.meta.computed, 3); assert.equal(ai.meta.written, 0);
    assert.ok(!/sk-SECRET|https?:/.test(JSON.stringify(audits)), 'no secrets or links in the audit');
    const junk = await run({ researchAnswer: () => ({ nothing: true }) });
    assert.ok(junk.result.list.every(o => o.research.source === 'computed')); assert.match(audit(junk.audits, 'pmax_ai_research').detail, /^Written from your own data only: the model's answer could not be used/);
    const mixed = await run({ researchAnswer: coll => coll === 'Pet jewelry' ? { nothing: true } : { headline: 'AI ' + coll, creativeAngles: ['a'] } });
    assert.deepEqual(mixed.result.list.map(o => o.research.source).sort(), ['ai', 'ai', 'computed']); assert.match(audit(mixed.audits, 'pmax_ai_research').detail, /\(1 of 3 ideas\)/);
  });

  await test('Keyword Planner failure is soft: ideas continue without volumes and the audit says why', async () => {
    const limited = await run({ keywordResearchPool: async () => ({ ok: false, error: '429 Too Many Requests: resource exhausted', status: 429, ideasByText: {} }) });
    assert.equal(limited.result.list.length, 3);
    limited.result.list.forEach(o => { assert.ok(o.research.keywords.length >= 2 && o.research.keywords.every(k => k.monthlySearches === null)); assert.ok(o.searchThemes.length >= 2); assert.equal(o.research.source, 'ai'); });
    const k = audit(limited.audits, 'pmax_keyword_research'); assert.equal(k.status, 'warning'); assert.equal(k.detail, 'Keyword volumes unavailable: Google is limiting requests right now');
    const thrown = await run({ keywordResearchPool: async () => { throw new Error('socket hang up'); } });
    assert.equal(thrown.result.list.length, 3); assert.equal(audit(thrown.audits, 'pmax_keyword_research').detail, 'Keyword volumes unavailable: the connection failed');
    const none = await run({ keywordResearchPool: async seeds => ({ ok: true, ideasByText: {}, cached: false }) });
    assert.equal(audit(none.audits, 'pmax_keyword_research').status, 'warning'); assert.match(audit(none.audits, 'pmax_keyword_research').detail, /^Keyword volumes unavailable: Google returned no volumes/);
    const slow = await run({ keywordResearchPool: async seeds => ({ ok: true, partial: true, ideasByText: { [seeds[0]]: { text: seeds[0], searches: 90, competition: 'HIGH' } } }) });
    assert.equal(slow.result.list.length, 3); assert.match(audit(slow.audits, 'pmax_keyword_research').detail, /planner stopped early/);
  });

  await test('the same evidence on the same day reuses the model\'s saved answer without a new model call or planner lookup', async () => {
    const first = await run(), saved = plain(first.result.list);
    assert.equal(first.spy.research.length, 3);
    const docs = { 'Brites_GAds_State/opportunities': { pmaxList: saved } }, fb = () => ({ db: { collection: c => ({ doc: id => ({ get: async () => ({ exists: !!docs[c + '/' + id], data: () => docs[c + '/' + id] }) }) }) } });
    const again = await run({ fb });
    assert.equal(again.spy.research.length, 0, 'no research call'); assert.equal(again.spy.pools.length, 0, 'no keyword lookup'); assert.equal(again.spy.computed.length, 0);
    assert.equal(again.result.list.length, 3); again.result.list.forEach((o, i) => { const before = saved.find(s => s.handle === o.handle); assert.deepEqual(plain(o.research), before.research); assert.equal(o.rationale, before.research.headline); assert.deepEqual(plain(o.searchThemes), before.research.keywords.map(k => k.text)); });
    const ai = audit(again.audits, 'pmax_ai_research'); assert.equal(ai.status, 'ok'); assert.equal(ai.meta.reused, 3); assert.equal(ai.meta.costUsd, 0); assert.match(ai.detail, /3 reused from earlier today/);
    assert.equal(audit(again.audits, 'pmax_keyword_research').status, 'skipped');
    // Not reused: another day, changed evidence, or research the model did not write.
    docs['Brites_GAds_State/opportunities'] = { pmaxList: saved.map(s => ({ ...s, research: { ...s.research, today: '2020-01-01' } })) };
    assert.equal((await run({ fb })).spy.research.length, 3);
    docs['Brites_GAds_State/opportunities'] = { pmaxList: saved.map(s => ({ ...s, researchFingerprint: s.researchFingerprint + 'x' })) };
    assert.equal((await run({ fb })).spy.research.length, 3);
    docs['Brites_GAds_State/opportunities'] = { pmaxList: saved.map(s => ({ ...s, research: { ...s.research, source: 'computed' } })) };
    assert.equal((await run({ fb })).spy.research.length, 3, 'computed research is retried, since the model never wrote it');
    docs['Brites_GAds_State/opportunities'] = { pmaxList: saved.slice(0, 1) };
    const partial = await run({ fb }); assert.equal(partial.spy.research.length, 2); assert.equal(partial.spy.pools.length, 1); assert.equal(audit(partial.audits, 'pmax_ai_research').meta.reused, 1);
  });

  await test('timezone: the date is the ad account\'s date, never the server\'s UTC date', async () => {
    const east = 'Pacific/Kiritimati', west = 'Pacific/Pago_Pago';
    const a = await run({ tz: async () => east }), b = await run({ tz: async () => west });
    const dateOf = r => (r.spy.research[0].prompt.match(/Today: (\d{4}-\d{2}-\d{2})/) || [])[1];
    assert.equal(dateOf(a), ymdIn(east)); assert.equal(dateOf(b), ymdIn(west)); assert.notEqual(dateOf(a), dateOf(b));
    assert.ok(a.result.list.every(o => o.research.today === ymdIn(east))); assert.match(audit(a.audits, 'pmax_calendar').detail, new RegExp('Today is ' + ymdIn(east)));
    assert.match(a.spy.selector[0].prompt, new RegExp(`Today is ${ymdIn(east)} in the ad account`));
    const days = a.result.list[0].research.whyNow.timing.map(t => t.daysAway); assert.ok(days.every(d => d === Math.round((Date.parse(a.result.list[0].research.whyNow.timing[0].date) - Date.parse(ymdIn(east))) / day) || d >= 0));
    // The scan hands the account-date it already computed, plus its markets, to the PMax pipeline.
    const src = engine().testSrc;
    assert.match(src.scan, /proposePmaxOpportunities\(\{[^)]*currency:ctrl\.budgetCurrency, today:dateStr, markets:marketCodes, isoByGeo, geoIds:geoIds0, orderCutoffDays:cut/);
    const given = await run({ tz: async () => { throw new Error('the scan passes its own date'); } }, { today: '2026-09-29', markets: ['US'] });
    assert.ok(given.result.list.every(o => o.research.today === '2026-09-29'));
  });

  await test('search themes keep Google\'s limits (10 words, 80 characters, plain letters and digits) and fall back to the product titles when there are none', async () => {
    const long = 'Gold Filled: personalised "bird" necklace for mum, gift; birthday & anniversary present ideas 2026 extra words here';
    const { result } = await run({ themes: () => [long, 'silver bird necklace', 'silver bird necklace', '', '!!!'] });
    result.list.forEach(o => { assert.deepEqual(plain(o.searchThemes), ['gold filled personalised bird necklace for mum gift birthday anniversary', 'silver bird necklace']); o.searchThemes.forEach(t => assert.ok(t.split(' ').length <= 10 && t.length <= 80 && /^[a-z0-9 ]+$/.test(t))); });
    const none = await run({ themes: () => [] });
    none.result.list.forEach(o => { assert.ok(o.searchThemes.length > 0 && o.searchThemes.every(t => /^[a-z0-9 ]+$/.test(t))); assert.ok(o.searchThemes.some(t => t.includes(o.productTitles[0].toLowerCase().split(' ')[1]))); });
    assert.match(source, /searchThemes\.length\?searchThemes:savedIdea&&Array\.isArray\(savedIdea\.searchThemes\)&&savedIdea\.searchThemes\.length\?savedIdea\.searchThemes:_derivePmaxSearchThemes/, 'a draft without themes in its request uses the saved idea\'s keyword themes');
  });

  await test('with the REAL research and keyword modules: valid research on every idea, the model\'s answer is normalized, and a rejected answer falls back to computed research', async () => {
    const RR = require(root + '/_googleAdsPmaxResearch.js'), today = ymdIn('America/Toronto');
    const answer = (coll, prompt) => {
      const ids = [...prompt.matchAll(/^- \{"itemId":"([^"]+)"/gm)].map(m => m[1]), kws = [...prompt.matchAll(/^- "([^"]+)" \|/gm)].map(m => m[1]);
      return { headline: 'Start now for the next gift date with these bestsellers.', whyNow: { summary: 'The next gift date leaves room for the learning period, and these listings already sold in your store recently.', marketRead: null, caution: null },
        listingFit: ids.slice(0, 2).map((id, i) => ({ itemId: id, reason: 'Sold in your store recently.', role: i ? 'support' : 'hero' })), keywords: kws.slice(0, 5).map(t => ({ text: t, reason: 'Buyers search this when they shop for gifts.' })), creativeAngles: ['Gift-ready close-up of the charm'], limits: [] };
    };
    const good = await run({ real: true, researchAnswer: answer }), sp = good.spy;
    assert.equal(good.result.list.length, 3); assert.equal(sp.research.length, 3);
    for (const o of good.result.list) {
      assert.deepEqual(plain(RR.validateResearch(o.research)), [], o.handle + ' research is valid'); noUndefined(o);
      assert.equal(o.research.source, 'ai'); assert.equal(o.research.model, 'Sonnet 5.5'); assert.equal(o.research.today, today); assert.equal(o.rationale, o.research.headline); assert.equal(o.angle, o.research.creativeAngles[0]);
      assert.ok(o.research.whyNow.timing.length >= 1 && o.research.whyNow.timing.every(t => /^[A-Z]{2}(\/[A-Z]{2})*$/.test(t.market))); assert.ok(o.research.keywords.length >= 3 && o.research.keywords.every(k => k.monthlySearches === 320 && k.competition === 'low'));
      assert.ok(o.research.listingFit.every(f => o.itemIds.includes(f.itemId))); assert.equal(o.research.sources.length, 1); assert.deepEqual(plain(o.searchThemes), plain(o.research.keywords.map(k => k.text)).slice(0, 10));
      o.searchThemes.forEach(t => assert.ok(t.split(' ').length <= 10 && t.length <= 80 && /^[a-z0-9 ]+$/.test(t)));
      assert.ok(o.researchFingerprint.startsWith(today));
    }
    assert.ok(sp.research.every(c => c.prompt.includes('TODAY: ' + today)), 'the real prompt carries today\'s date'); assert.ok(good.result.list.every(o => sp.research.some(c => o.itemIds.every(id => c.prompt.includes(id)))), 'every offer is in a prompt');
    assert.equal(audit(good.audits, 'pmax_ai_research').status, 'ok');
    // A summary the module refuses (too short) means no model prose: the computed research stands alone, complete and valid.
    const bad = await run({ real: true, researchAnswer: (c, p) => ({ ...answer(c, p), whyNow: { summary: 'Too short.' } }) });
    for (const o of bad.result.list) { assert.deepEqual(plain(RR.validateResearch(o.research)), []); assert.equal(o.research.source, 'computed'); assert.equal(o.research.model, null); assert.equal(o.rationale, o.research.headline); assert.ok(o.research.keywords.length >= 2 && o.research.listingFit.length >= 1 && o.searchThemes.length >= 2); }
    assert.match(audit(bad.audits, 'pmax_ai_research').detail, /^Written from your own data only: the model's answer could not be used/);
    // The model throws, the planner fails: still complete and valid.
    const down = await run({ real: true, researchThrows: new Error('overloaded'), keywordResearchPool: async () => ({ ok: false, error: 'HTTP 503 internal error', ideasByText: {} }) });
    for (const o of down.result.list) { assert.deepEqual(plain(RR.validateResearch(o.research)), []); assert.equal(o.research.source, 'computed'); assert.ok(o.research.keywords.every(k => k.monthlySearches === null)); assert.ok(o.searchThemes.length >= 2); }
    assert.equal(audit(down.audits, 'pmax_keyword_research').detail, 'Keyword volumes unavailable: the service had a temporary problem');
    // The same-day reuse works with the real fingerprint too.
    const docs = { 'Brites_GAds_State/opportunities': { pmaxList: plain(good.result.list) } }, fb = () => ({ db: { collection: c => ({ doc: id => ({ get: async () => ({ exists: !!docs[c + '/' + id], data: () => docs[c + '/' + id] }) }) }) } });
    const again = await run({ real: true, fb }); assert.equal(again.spy.research.length, 0); assert.equal(again.spy.pools.length, 0); assert.equal(audit(again.audits, 'pmax_ai_research').meta.reused, 3);
  });

  await test('markets come from the feed label; other feed labels use the account\'s default countries', async () => {
    const two = offers.map(o => o.title === 'Gold dog charm' ? { ...o, feedLabel: 'PET_FEED' } : o);
    const { result } = await run({ merchantProducts: async a => a.itemIds ? [] : two }, { markets: ['CA', 'US'] });
    const pet = result.list.find(o => o.handle === 'pets'), bird = result.list.find(o => o.handle === 'birds');
    assert.deepEqual([...new Set(pet.research.whyNow.timing.map(t => t.market))].sort(), ['CA', 'US']); assert.deepEqual([...new Set(bird.research.whyNow.timing.map(t => t.market))], ['US']);
  });

  await test('the selector is asked for three, and when it names fewer the best-ranked qualifying candidates fill in', async () => {
    const two = await run({ picks: [{ handle: 'birds', feedLabel: 'US' }, { handle: 'pets', feedLabel: 'CA' }] });
    assert.equal(two.result.list.length, 3); assert.deepEqual(two.result.list.map(o => o.handle).sort(), ['birds', 'moon', 'pets']);
    const fill = audit(two.audits, 'pmax_selector_fallback'); assert.equal(fill.status, 'ok'); assert.match(fill.detail, /added the 1 best-ranked/);
    assert.ok(two.result.list.every(o => o.research && o.research.headline && o.searchThemes.length));
    const none = await run({ picks: [] }); assert.equal(none.result.list.length, 3); assert.equal(audit(none.audits, 'pmax_selector_fallback').status, 'warning');
    assert.ok(none.result.list.every(o => !/observed product-order matches/.test(o.rationale) && o.research.headline === o.rationale));
    const failed = await run({ openaiJSON: async prompt => { if (/^RESEARCH_PROMPT/.test(prompt)) return { headline: 'AI ' + prompt.match(/Collection: (.+)/)[1], creativeAngles: ['a'] }; throw new Error('selector down'); } });
    assert.equal(failed.result.list.length, 3);
  });

  await test('the research helper modules are missing: ideas still come back, plainly labelled, with no canned wording', async () => {
    const { result, audits } = await run({ noModules: true });
    assert.equal(result.list.length, 3); assert.ok(result.list.every(o => !o.research && o.searchThemes.length && !/observed product-order matches/.test(o.rationale)));
    assert.equal(audit(audits, 'pmax_ai_research').status, 'warning'); assert.match(audit(audits, 'pmax_ai_research').detail, /^Written from your own data only: /); assert.equal(audit(audits, 'pmax_keyword_research').status, 'warning'); assert.equal(audit(audits, 'pmax_calendar').status, 'warning');
  });

  await test('research status names the unavailable sources, in the message and in unavailable[]', () => {
    const S = engine().testResearchStatus, t0 = Date.now();
    const ok = id => ({ id, status: 'ok' });
    const audit1 = { startedAt: t0 - 60000, checks: [ok('pmax_store_signals'), ok('pmax_merchant_catalogue'), ok('pmax_candidate_scoring'), { id: 'pmax_paid_product_reporting', status: 'warning', meta: { rows: 0, complete: true, monetaryComplete: true } },
      { id: 'pmax_keyword_research', status: 'warning', error: '429 Too Many Requests from https://googleads.googleapis.com/v17/customers/1234567890/googleAds:generateKeywordIdeas' }, { id: 'scan_result', status: 'warning' }] };
    const p = plain(S({ pmaxAt: t0, pmaxResearchVersion: 3, scanAudit: audit1 }, t0).pmax);
    assert.equal(p.status, 'partial');
    assert.equal(p.message, 'Research refreshed. Partial data from: Paid product history (Google returned no spend for these products); Keyword volumes (Google is limiting requests right now). The ideas use what was available.');
    assert.deepEqual(p.unavailable, [{ id: 'pmax_paid_product_reporting', label: 'Paid product history', reason: 'Google returned no spend for these products' }, { id: 'pmax_keyword_research', label: 'Keyword volumes', reason: 'Google is limiting requests right now' }]);
    assert.doesNotMatch(p.message, /1234567890|https?:|customers/);
    // A required check that never ran is named too; more than three collapse to "and N more"; failures come first; shared checks count for both channels.
    const audit2 = { startedAt: t0 - 60000, checks: [ok('pmax_store_signals'), ok('pmax_merchant_catalogue'),
      { id: 'pmax_merchant_organic_30d', status: 'warning', error: 'HTTP 403 forbidden' }, { id: 'pmax_merchant_organic_90d', status: 'warning', error: 'HTTP 403 forbidden' }, { id: 'conversion_health', status: 'warning' }, { id: 'pmax_ai_research', status: 'warning', error: 'the request timed out' },
      { id: 'pmax_paid_product_reporting', status: 'failed', error: 'deadline exceeded: timeout' }, { id: 'store_sales_history', channel: 'shared', status: 'warning', fallback: 'Historical sales could not be retrieved; no purchase history is invented.' }] };
    const q = plain(S({ pmaxAt: t0, pmaxResearchVersion: 3, scanAudit: audit2 }, t0).pmax);
    assert.equal(q.unavailable[0].label, 'Paid product history'); assert.equal(q.unavailable[0].reason, 'the request timed out');
    assert.deepEqual(q.unavailable.map(u => u.label), ['Paid product history', 'Merchant free-listing reports', 'Conversion tracking check', 'AI research', 'Store sales signals', 'Product matching']);
    assert.equal(q.unavailable.find(u => u.label === 'Merchant free-listing reports').reason, 'access was refused, so the connection needs attention');
    assert.equal(q.unavailable.find(u => u.label === 'Product matching').reason, 'the check did not run in the latest refresh');
    assert.match(q.message, /^Research refreshed\. Partial data from: Paid product history \(the request timed out\); Merchant free-listing reports \(access was refused, so the connection needs attention\); Conversion tracking check \(purchase conversion tracking could not be confirmed\) and 3 more\. The ideas use what was available\.$/);
    // Statuses keep their codes; a clean refresh has nothing unavailable; an old or missing audit is explicit.
    const clean = plain(S({ pmaxAt: t0, pmaxResearchVersion: 3, scanAudit: { startedAt: t0 - 60000, checks: [ok('pmax_store_signals'), ok('pmax_merchant_catalogue'), ok('pmax_paid_product_reporting'), ok('pmax_candidate_scoring')] } }, t0).pmax);
    assert.equal(clean.status, 'ready'); assert.deepEqual(clean.unavailable, []);
    const noAudit = plain(S({ pmaxAt: t0, pmaxResearchVersion: 3 }, t0).pmax); assert.equal(noAudit.status, 'partial'); assert.equal(noAudit.unavailable[0].label, 'Source report'); assert.match(noAudit.message, /^Research refreshed\. Partial data from: Source report/);
    const err = plain(S({ pmaxAt: t0, pmaxResearchVersion: 3, pmaxError: 'Merchant Center catalogue read failed: 403 permission denied', scanAudit: audit1 }, t0).pmax);
    assert.equal(err.status, 'error'); assert.equal(err.unavailable[0].label, 'Product ads research'); assert.match(err.message, /^Research needs a refresh\. Unavailable: Product ads research \(access was refused/);
    assert.equal(plain(S({ pmaxAt: t0 - 13 * 3600000, pmaxResearchVersion: 3, scanAudit: audit1 }, t0).pmax).status, 'stale'); assert.equal(plain(S({}, t0).search).status, 'missing'); assert.deepEqual(plain(S({}, t0).search.unavailable), []);
    const searchSide = plain(S({ scannedAt: t0, searchResearchVersion: 3, scanAudit: { startedAt: t0 - 60000, checks: [ok('shopify_collections'), { id: 'collection_profiles', status: 'warning' }, { id: 'keyword_planner_batch_1', status: 'warning', error: 'HTTP 429' }] } }, t0).search);
    assert.deepEqual(searchSide.unavailable.map(u => u.label), ['Collection product profiles', 'Keyword volumes']); assert.equal(searchSide.unavailable[1].reason, 'Google is limiting requests right now');
  });

  await test('the research schema is not bumped: saved cards without research stay usable until the 12-hour refresh', () => {
    const S = engine().testResearchStatus, t0 = Date.now();
    assert.equal(plain(S({ pmaxAt: t0, pmaxResearchVersion: 3, scanAudit: null }, t0).pmax).legacy, false);
    assert.match(source, /const _OPPORTUNITY_RESEARCH_SCHEMA = 3;/);
  });

  /* ---- "why so few (or no) Product ads ideas" ---- */
  await test('funnel, happy path: real counts at each rule, the reason for every collection left out, and a one-sentence verdict', async () => {
    const { result } = await run(), f = plain(result.pmaxFunnel);
    assert.deepEqual(f.stages.map(s => s.key), ['collections', 'sales', 'products', 'overlap', 'free', 'shown']);
    assert.deepEqual(f.stages.map(s => s.count), [5, 4, 3, 3, 3, 3]);
    f.stages.forEach(s => { assert.ok(s.label && typeof s.note === 'string' && !/[\u{1F300}-\u{1FAFF}]/u.test(s.note + s.label)); });
    assert.equal(f.verdict, '3 ideas shown from 5 collections.'); assert.ok(f.at > 0);
    assert.deepEqual(f.skipped.map(s => [s.title, s.feedLabel, s.reason]), [['Ocean jewelry', null, 'its feed products are out of stock or not approved'], ['Quiet jewelry', null, 'no sales in the last 90 days matched one of its products']]);
    assert.match(f.stages[1].note, /1 collection had no sale/); assert.match(f.stages[2].note, /1 collection sold but had no product that is in stock and approved/);
    noUndefined(f);
  });

  await test('funnel, empty catalogue: the verdict names the Merchant Center read and its plain reason', async () => {
    const empty = await run({ merchantProducts: async () => [] }); assert.deepEqual(plain(empty.result.list), []); const f = plain(empty.result.pmaxFunnel);
    assert.match(f.verdict, /^None qualified: the Merchant Center product read returned no products/); assert.equal(f.stages[0].count, 5); assert.equal(f.stages[5].count, 0); assert.ok(f.verdict.length <= 240);
    assert.equal(empty.result.error, 'The linked Merchant Center catalogue returned no products');
    const failed = await run({ merchantProducts: async () => { throw new Error('PERMISSION_DENIED 403 for https://googleads.googleapis.com/v17/customers/5550001234'); } });
    assert.equal(plain(failed.result.pmaxFunnel).verdict, 'None qualified: the Merchant Center product read failed (access was refused, so the connection needs attention).'); assert.match(failed.result.error, /Merchant Center catalogue read failed/);
    const noStock = await run({ merchantProducts: async () => offers.map(o => ({ ...o, availability: 'OUT_OF_STOCK' })) });
    assert.equal(noStock.result.list.length, 0); assert.match(plain(noStock.result.pmaxFunnel).verdict, /^None qualified: the products that sold are out of stock, not approved or missing from the Merchant Center feed\.$/);
    const noSales = await run({ storeSignals: async () => ({ ...sigs[90], productRows: [], topProducts: [], topMerchantProducts: [], topOrganicProducts: [] }) });
    assert.equal(noSales.result.list.length, 0); assert.equal(plain(noSales.result.pmaxFunnel).verdict, 'None qualified: no sales in the last 90 days matched a product in your collections.'); assert.equal(plain(noSales.result.pmaxFunnel).stages[1].count, 0);
    const unread = await run({ storeSignals: async () => null }); assert.equal(plain(unread.result.pmaxFunnel).verdict, "None qualified: the store's recent order history could not be read.");
    const nothing = await run({ profiles: [] }); assert.equal(plain(nothing.result.pmaxFunnel).verdict, 'None qualified: no collections were available to check.');
  });

  await test('funnel, already taken: ideas with a campaign or draft are left out with that reason; when all are taken they are shown as in use', async () => {
    const tag = (h, m) => 'pmax-' + h + '-' + m.toLowerCase();
    const one = await run({ takenTags: async () => ({ [tag('birds', 'US')]: { where: 'campaign', status: 'ENABLED' } }), picks: [{ handle: 'pets', feedLabel: 'CA' }, { handle: 'moon', feedLabel: 'US' }] });
    assert.deepEqual(one.result.list.map(o => o.handle).sort(), ['moon', 'pets']); const f = plain(one.result.pmaxFunnel);
    assert.deepEqual(f.stages.map(s => s.count), [5, 4, 3, 3, 2, 2]); assert.ok(f.skipped.some(s => s.title === 'Bird jewelry' && s.feedLabel === 'US' && s.reason === 'already has a campaign or review draft'));
    assert.equal(f.skipped[0].reason, 'already has a campaign or review draft', 'ideas that could have been shown come first'); assert.match(f.stages[4].note, /1 already has a campaign or review draft/); assert.match(f.verdict, /^2 ideas shown from 5 collections\. The rest were left out for the reasons listed\.$/);
    const all = await run({ takenTags: async () => ({ [tag('birds', 'US')]: { where: 'campaign' }, [tag('pets', 'CA')]: { where: 'approval' }, [tag('moon', 'US')]: { where: 'approval' } }) });
    assert.equal(all.result.list.length, 3); const g = plain(all.result.pmaxFunnel);
    assert.equal(g.stages[4].count, 0); assert.match(g.stages[4].note, /All of them already have a campaign or review draft/); assert.match(g.verdict, /^3 ideas shown from 5 collections\. All of them already have a campaign or review draft\.$/);
    assert.ok(!g.skipped.some(s => /already has a campaign/.test(s.reason)));
  });

  await test('funnel, overlap and the top-3 limit: a weaker idea over the same products is dropped naming the stronger one; extra candidates say why they were held back', async () => {
    const dup = profiles.concat([{ handle: 'allj', topProducts: [{ title: 'Silver bird necklace', productId: '999001' }] }]), colls = collections.concat([{ handle: 'allj', title: 'All jewelry' }]);
    const o = await run({ profiles: dup, collections: colls, picks: [{ handle: 'birds', feedLabel: 'US' }, { handle: 'pets', feedLabel: 'CA' }, { handle: 'moon', feedLabel: 'US' }] });
    const f = plain(o.result.pmaxFunnel);
    assert.deepEqual(f.stages.map(s => s.count), [6, 5, 4, 3, 3, 3]); assert.match(f.stages[3].note, /1 dropped for sharing products with a stronger idea/);
    const row = f.skipped.find(s => /overlaps the stronger idea/.test(s.reason)); assert.ok(row); assert.equal(row.title, 'All jewelry'); assert.equal(row.feedLabel, 'US'); assert.equal(row.reason, "overlaps the stronger idea 'Bird jewelry' (US) (same products)");
    // Four qualifying ideas, three shown.
    const four = await run({ profiles: profiles.concat([{ handle: 'wave', topProducts: [{ title: 'Star wave ring', productId: '999005' }] }]), collections: collections.concat([{ handle: 'wave', title: 'Ring jewelry' }]),
      merchantProducts: async a => a.itemIds ? [] : offers.concat([{ itemId: 'shopify_US_999005_111005', title: 'Star wave ring', feedLabel: 'US', availability: 'IN_STOCK', status: 'ELIGIBLE' }]),
      storeSignals: async ({ days }) => aggregate(rows.concat([order(900, item('111005', 'Star wave ring', '999005'), 3)]), days), picks: [{ handle: 'birds', feedLabel: 'US' }, { handle: 'pets', feedLabel: 'CA' }, { handle: 'moon', feedLabel: 'US' }] });
    assert.equal(four.result.list.length, 3); const h = plain(four.result.pmaxFunnel);
    assert.deepEqual(h.stages.map(s => s.count), [6, 5, 4, 4, 4, 3]); assert.ok(h.skipped.some(s => s.title === 'Ring jewelry' && s.reason === 'held back: only the best 3 are shown')); assert.match(h.stages[5].note, /1 held back/);
  });

  await test('funnel: pmaxCandidatesFromSignals keeps returning a plain array; the funnel rides along without being enumerable', () => {
    const x = engine(), c = x.pmaxCandidatesFromSignals({ collections, profiles, sig30: sigs[30], sig90: sigs[90], merchant: offers });
    assert.ok(Array.isArray(c) && c.length === 3); assert.equal(Object.keys(c).join(), '0,1,2'); assert.ok(!('funnel' in JSON.parse(JSON.stringify({ c })).c)); assert.deepEqual(plain(c.funnel).skipped.map(s => s.kind).sort(), ['no_offers', 'no_sales']);
    assert.equal(c.funnel.collections, 5); assert.equal(c.funnel.withSales, 4); assert.equal(c.funnel.noOffers, 1); assert.equal(c.funnel.matched, 3); assert.equal(c.funnel.afterOverlap, 3);
  });

  await test('funnel is saved next to the ideas, returned by the cache-only read, and passed on to the console', async () => {
    const src = engine().testSrc.scan;
    assert.match(src, /set\(\{ pmaxList, pmaxError, pmaxAt, pmaxResearchVersion, pmaxLearning, pmaxFunnel \}, \{ merge: true \}\)/); // interim save
    assert.match(src, /set\(\{ list, pmaxList, pmaxError, pmaxAt, pmaxFunnel, /); assert.match(src, /set\(\{ list: slim, pmaxList, pmaxError, pmaxAt, pmaxFunnel, /); assert.match(src, /set\(\{ list: \[\], pmaxList: \[\], pmaxError, pmaxAt, pmaxFunnel, /);
    assert.match(src, /\.\.\.\(pmaxAt \? \{ pmaxList, pmaxError, pmaxAt, pmaxResearchVersion, pmaxLearning, pmaxFunnel \}/);
    const funnel = { at: now, stages: [{ key: 'collections', label: 'Collections looked at', count: 5, note: '' }], skipped: [], verdict: '3 ideas shown from 5 collections.' };
    const docs = { 'Brites_GAds_State/opportunities': { pmaxList: [{ handle: 'birds', feedLabel: 'US', itemIds: ['a'] }], pmaxFunnel: funnel, pmaxAt: now, pmaxResearchVersion: 3, at: now, list: [] } };
    const fb = () => ({ db: { collection: c => ({ doc: id => ({ get: async () => ({ exists: !!docs[c + '/' + id], data: () => docs[c + '/' + id] }) }), where() { return this; }, limit() { return this; }, get: async () => ({ forEach() {}, docs: [] }) }) } });
    const x = engine({ fb, tz: async () => 'America/Toronto', takenTags: async () => ({}), deleted: async () => new Set() });
    const cached = await x.scanOpportunities({ cacheOnly: true }); assert.deepEqual(plain(cached.pmaxFunnel), funnel);
    const served = await x.opportunitiesWithStatus({ cacheOnly: true }); assert.deepEqual(plain(served.pmaxFunnel), funnel); assert.equal(served.pmaxList.length, 1);
    docs['Brites_GAds_State/opportunities'] = { pmaxList: [], pmaxAt: now }; assert.equal((await x.opportunitiesWithStatus({ cacheOnly: true })).pmaxFunnel, null);
  });

  await test('evidence refresh keeps the researched headline and keyword themes, and only the listings that are still live', async () => {
    const first = await run(), saved = plain(first.result.list.find(o => o.handle === 'birds')), keep = saved.itemIds[0];
    const docs = { 'Brites_GAds_State/opportunities': { pmaxList: [saved] } };
    const fb = () => ({ db: { collection: c => ({ doc: id => ({ get: async () => ({ exists: !!docs[c + '/' + id], data: () => docs[c + '/' + id] }) }) }) } });
    const live = { itemId: keep, title: 'Silver bird necklace', feedLabel: 'US', availability: 'IN_STOCK', status: 'ELIGIBLE', merchantId: '123' };
    const x = engine({ fb, tz: async () => 'America/Toronto', merchantProducts: async () => [live], storeSignals: async ({ days }) => sigs[days] || null, pmaxProductPerformance: async () => ({ complete: true, monetaryComplete: true, rows: [], byId: {} }), merchantFreeProductPerformance: async () => free });
    const r = await x.pmaxRecommendationEvidence({ handle: 'birds', feedLabel: 'US' });
    assert.equal(r.candidate.rationale, saved.research.headline); assert.deepEqual(plain(r.candidate.searchThemes), saved.searchThemes); assert.ok(r.candidate.research.listingFit.every(f => f.itemId === keep));
  });

  console.log('PMax opportunity research: ' + pass + ' focused checks passed');
  require('./suite-guard.cjs').done();
})().catch(e => { console.error(e); process.exitCode = 1; });
