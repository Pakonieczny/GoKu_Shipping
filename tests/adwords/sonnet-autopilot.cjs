// Offline checks: every autopilot text and vision AI call runs on Claude Sonnet 5.5.
// A fake node-fetch replays Anthropic SSE streams and a fake image endpoint; no
// request leaves this process and no Google Ads call is made.
const assert = require('node:assert/strict'), fs = require('fs'), vm = require('vm'), path = require('path'), { Readable } = require('node:stream');
const file = path.resolve(__dirname, '../../netlify/functions/googleAdsAutopilot.js'), source = fs.readFileSync(file, 'utf8'), realRequire = require('module').createRequire(file);
let passed = 0;
const plain = x => x === undefined ? x : JSON.parse(JSON.stringify(x)); // values built inside the vm have another realm's prototypes
const ok = (v, m) => { assert.ok(v, m); passed++; }, eq = (a, b, m) => { assert.deepEqual(plain(a), plain(b), m); passed++; };
async function rejects(fn, test, m) { await assert.rejects(fn, test, m); passed++; }
const say = console.log; console.log = () => {}; // the shared client logs one usage line per request

// One Anthropic reply: optional web searches with their results, then the answer text (optionally cited).
function reply({ text = '', stop = 'end_turn', searches = [], citations = [], stopDetails, model = 'claude-sonnet-5-5' } = {}) {
  const events = [{ type: 'message_start', message: { id: 'msg_fixture', type: 'message', role: 'assistant', model, content: [], stop_reason: null, usage: { input_tokens: 1200, output_tokens: 1 } } }];
  let index = 0;
  for (const s of searches) {
    events.push({ type: 'content_block_start', index, content_block: { type: 'server_tool_use', id: 'srv_' + index, name: 'web_search', input: {} } },
      { type: 'content_block_delta', index, delta: { type: 'input_json_delta', partial_json: JSON.stringify({ query: s.query }) } }, { type: 'content_block_stop', index });
    index++;
    events.push({ type: 'content_block_start', index, content_block: { type: 'web_search_tool_result', tool_use_id: 'srv_' + (index - 1), content: s.results.map(r => ({ type: 'web_search_result', url: r.url, title: r.title, page_age: r.pageAge || null, encrypted_content: 'fixture' })) } },
      { type: 'content_block_stop', index });
    index++;
  }
  events.push({ type: 'content_block_start', index, content_block: { type: 'text', text: '' } }, { type: 'content_block_delta', index, delta: { type: 'text_delta', text } },
    ...citations.map(c => ({ type: 'content_block_delta', index, delta: { type: 'citations_delta', citation: { type: 'web_search_result_location', cited_text: 'fixture', ...c } } })), { type: 'content_block_stop', index });
  events.push({ type: 'message_delta', delta: { stop_reason: stop, stop_sequence: null, ...(stopDetails ? { stop_details: stopDetails } : {}) }, usage: { output_tokens: 600, ...(searches.length ? { server_tool_use: { web_search_requests: searches.length } } : {}) } }, { type: 'message_stop' });
  return { ok: true, status: 200, headers: { get: () => null }, body: Readable.from([Buffer.from(events.map(e => 'event: ' + e.type + '\ndata: ' + JSON.stringify(e) + '\n\n').join(''), 'utf8')]) };
}
function network() {
  const calls = [], queue = [];
  const fetch = async (url, init = {}) => {
    url = String(url); calls.push({ url, init, body: init.body ? JSON.parse(init.body) : null });
    if (url === 'https://api.openai.com/v1/images/edits') return { ok: true, status: 200, json: async () => ({ data: [{ b64_json: Buffer.from('generated-photo').toString('base64') }] }) };
    if (url !== 'https://api.anthropic.com/v1/messages') throw new Error('Unexpected network request: ' + url);
    const next = queue.shift(); if (!next) throw new Error('Unexpected extra AI request');
    if (next.status) return { ok: false, status: next.status, statusText: 'error', headers: { get: () => null }, text: async () => JSON.stringify({ type: 'error', error: { type: next.errorType || 'invalid_request_error', message: next.message || 'bad request' } }) };
    return reply(next);
  };
  return { fetch, calls, queue, ai: () => calls.filter(c => c.url.includes('anthropic')), images: () => calls.filter(c => c.url.includes('images/edits')) };
}
// Pass-through stand-in for sharp so the creative flow needs no native module.
const fakeSharp = input => { const api = { rotate: () => api, resize: () => api, jpeg: () => api, toBuffer: async () => (Buffer.isBuffer(input) ? input : Buffer.from(String(input))), metadata: async () => ({ width: 1200, height: 1200 }) }; return api; };
// A clock fixed at `now` for date-dependent scan checks.
const fixedClock = now => class FixedDate extends Date { constructor(...a) { if (a.length) super(...a); else super(now); } static now() { return now; } };
function engine({ env = { ANTHROPIC_API_KEY: 'test-do-not-send' }, mocks = {}, now = null } = {}) {
  const net = network();
  const ctx = { module: { exports: {} }, exports: {}, process: { env: { GADS_CUSTOMER_ID: '123', GADS_CURRENCY: 'USD', ...env } }, URL, URLSearchParams, Intl, Date: now ? fixedClock(now) : Date, Buffer, setTimeout, clearTimeout, AbortController, console: { log() {}, warn() {}, error() {}, info() {} }, mocks,
    require: n => n === 'node-fetch' ? net.fetch : n === 'sharp' ? fakeSharp : realRequire(n) };
  vm.createContext(ctx); vm.runInContext(source, ctx, { filename: file });
  vm.runInContext('fb=mocks.fb||(()=>null);' + Object.keys(mocks).filter(n => n !== 'fb').map(n => n + '=mocks.' + n + ';').join(''), ctx);
  return { E: ctx.module.exports, get: n => vm.runInContext(n, ctx), net };
}
function memoryDb(seed = {}) {
  const docs = new Map(Object.entries(seed).map(([k, v]) => [k, JSON.parse(JSON.stringify(v))])), copy = v => JSON.parse(JSON.stringify(v));
  const ref = p => ({ path: p, get: async () => ({ exists: docs.has(p), data: () => copy(docs.get(p)) }), set: async v => { docs.set(p, copy(v)); },
    update: async v => { const cur = copy(docs.get(p) || {}); for (const [k, x] of Object.entries(v)) { const parts = k.split('.'); let at = cur; while (parts.length > 1) { const key = parts.shift(); at = at[key] || (at[key] = {}); } at[parts[0]] = x === undefined ? undefined : copy(x); } docs.set(p, cur); } });
  const db = { collection: c => ({ doc: id => ref(c + '/' + id) }), runTransaction: async fn => { const pending = []; const out = await fn({ get: r => r.get(), set: (r, v) => pending.push(() => r.set(v)), update: (r, v) => pending.push(() => r.update(v)) }); for (const w of pending) await w(); return out; } };
  return { docs, fb: { db, FV: { serverTimestamp: () => Date.now() } } };
}

(async () => {
  // 1. The generator wrapper: Sonnet 5.5 only, JSON contract, no OpenAI fallback.
  let e = engine({ env: { ANTHROPIC_API_KEY: 'test-do-not-send', GADS_GEN_MODEL: 'gpt-5.5', OPENAI_API_KEY: 'images-only' } });
  e.net.queue.push({ text: '{"ok":true,"n":2}' });
  let info = {};
  eq(await e.get('openaiJSON')('Return {"ok":true}', { maxTokens: 4000, info }), { ok: true, n: 2 }, 'parsed JSON returned');
  let [call] = e.net.ai();
  eq(call.body.model, 'claude-sonnet-5-5', 'a stale GADS_GEN_MODEL cannot switch the model'); eq(e.get('GEN_MODEL'), 'claude-sonnet-5-5');
  eq(call.body.thinking, { type: 'adaptive' }); eq(call.body.output_config.effort, 'high', 'research default effort stays high'); ok(call.body.max_tokens >= 16000, 'room for thinking');
  ok(!('temperature' in call.body) && !call.body.tools, 'no sampling parameters and no web search unless asked');
  ok(/opportunity engine/.test(call.body.system) && /untrusted business data/.test(call.body.system), 'untrusted-data system rules kept');
  eq(call.init.headers['x-api-key'], 'test-do-not-send'); ok(!call.init.headers.Authorization, 'the OpenAI key is never sent for text');
  eq(info.model, 'claude-sonnet-5-5'); eq(info.searches, 0); ok(info.costUsd > 0, 'estimated Sonnet 5.5 cost reported');

  const cut = '{"opportunities":[{"a":1},{"b":2},{"c":';
  e.net.queue.push({ text: cut, stop: 'max_tokens' }, { text: cut, stop: 'max_tokens' });
  info = {};
  eq(await e.get('openaiJSON')('x', { maxTokens: 24000, effort: 'high', info }), { opportunities: [{ a: 1 }, { b: 2 }] }, 'truncated output salvaged to its last complete item');
  ok(info.salvaged);
  const [first, second] = e.net.ai().slice(-2);
  eq(second.body.max_tokens, first.body.max_tokens * 2, 'one larger retry before salvage'); eq(second.body.output_config.effort, 'medium', 'retry at lower effort');

  e.net.queue.push({ text: '', stop: 'refusal', stopDetails: { category: 'cyber' } });
  await rejects(() => e.get('openaiJSON')('x'), err => err.code === 'CLAUDE_REFUSAL' && /^\[gads\] Claude declined/.test(err.message), 'refusal throws a descriptive error');
  e.net.queue.push({ text: 'Sorry, I cannot produce that.' });
  const before = e.net.ai().length;
  await rejects(() => e.get('openaiJSON')('x'), /\[gads\] Claude returned unparseable JSON/, 'prose is never a silent null');
  eq(e.net.ai().length, before + 1, 'a complete non-JSON answer is not re-bought');

  e.net.queue.push({ text: '{"ok":true}' });
  await e.get('openaiJSON')('x', { webSearch: true });
  call = e.net.ai().pop();
  eq(call.body.tools, [{ type: 'web_search_20260209', name: 'web_search', max_uses: 5, user_location: { type: 'approximate', country: 'US' } }], 'web search is bounded and US by default');
  ok(/Use web search/.test(call.body.system) && !call.body.output_config.format, 'search answers are cited text, not structured output');
  const where = ids => e.get('_aiWebSearch')(ids).userLocation.country;
  eq([where(['2124']), where(['2124', '2840']), where([]), where(undefined), where(['2826']), where(['99999'])], ['CA', 'US', 'US', 'US', 'GB', 'US'], 'search follows the target market');
  eq([2, 9, 0, 'x'].map(n => e.get('_aiWebSearch')(['2124'], n).maxUses), [2, 6, 5, 5], 'search count honored, 1-6');
  e.net.queue.push({ text: '{"ok":true}' }, { text: '{"ok":true}' });
  await e.get('openaiJSON')('x', { webSearch: 3 }); await e.get('openaiJSON')('x', { webSearch: 0 });
  const [three, none] = e.net.ai().slice(-2);
  eq(three.body.tools, [{ type: 'web_search_20260209', name: 'web_search', max_uses: 3, user_location: { type: 'approximate', country: 'US' } }], 'a number is the search count');
  ok(!none.body.tools, 'zero searches means no search tool');
  const cite = (ref, urls) => e.get('_citedSource')(ref, urls.map(url => ({ url, title: '' })));
  const pages = ['https://www.timeanddate.com/holidays/world/world-kindness-day', 'https://en.wikipedia.org/wiki/Sweetest_Day', 'javascript:alert(1)'];
  eq(cite('timeanddate.com', pages), pages[0], 'domain'); eq(cite('https://www.timeanddate.com/holidays/world/world-kindness-day', pages), pages[0], 'full URL');
  eq(cite('Time and Date', pages), pages[0], 'site name'); eq(cite('Wikipedia: Sweetest Day', pages), pages[1]);
  eq([cite('nationaltoday.com', pages), cite('', pages), cite(null, pages), cite('timeanddate.com', []), cite('alert', pages)], [null, null, null, null, null], 'a page no search returned is not a citation');
  ok(e.net.calls.every(c => c.url.includes('anthropic')), 'no OpenAI text request');

  e = engine({ env: { OPENAI_API_KEY: 'images-only' } });
  await rejects(() => e.get('openaiJSON')('x'), err => err.code === 'CLAUDE_NOT_CONFIGURED' && err.notDispatched === true, 'missing Anthropic key');
  eq(e.net.calls.length, 0, 'missing key sends nothing');

  // 2. Opportunity scan: web search is on, localized, its pages are kept, and a researched
  //    date survives only when its dateSource names a page the search returned.
  const NOW = Date.parse('2026-09-29T15:00:00Z'), months = ['SEPTEMBER', 'OCTOBER', 'NOVEMBER', 'DECEMBER', 'JANUARY', 'FEBRUARY', 'MARCH', 'APRIL', 'MAY', 'JUNE', 'JULY', 'AUGUST'];
  const motifs = { celestial: ['moon', 'star'], maple: ['maple', 'leaf'], garden: ['fern', 'rose'] }, seeds = []; // one collection per idea, so no overlap suppression
  const kw4 = (tag, coll = 'celestial') => motifs[coll].flatMap(m => ['gold ' + m + ' necklace ' + tag, 'engraved ' + m + ' necklace ' + tag]);
  const profile = h => ({ handle: h, title: h, sampled: 30, count: 30, typesDetail: [{ type: 'Necklace', n: 30 }], motifs: [{ t: motifs[h][0], n: 20 }, { t: motifs[h][1], n: 10 }], mats: [{ t: 'gold', n: 10 }], personalization: ['engraved'], topProducts: [{ title: motifs[h][0] + ' necklace' }] });
  const scanMocks = {
    control: async () => ({ maxDailyBudgetTotal: 100, defaultCountries: ['2124'], budgetCurrency: 'CAD', budgetCurrencyVerified: true, smartBidding: false }),
    getCollections: async () => Object.keys(motifs).map(h => ({ title: h, handle: h })), fetchTopProducts: async () => [], listCountries: async () => [],
    _accountTz: async () => 'America/Toronto', _fxRateToUsd: async () => 0.73, conversionHealth: async () => ({ validated: false, healthy: false }),
    collectionProfiles: async () => ({ list: Object.keys(motifs).map(profile), at: NOW, salesBasis: 'fixture' }),
    proposePmaxOpportunities: async () => ({ list: [], error: null, at: NOW }), playbookSlice: async () => ({ lessons: [], antiPatterns: [] }),
    storeSalesEvidence: async () => ({ available: false, periods: { days30: null, days90: null }, seasonality: { months: [] }, merchant: {}, warnings: [] }),
    _enabledBudgetTotal: async () => 0, storeSignals: async () => ({ orders: 10, totalRevenue: 900 }), collectionAdsPerformance: async () => ({}),
    accountCvr: async () => ({ cvr: 0.02, source: 'fixture prior' }),
    keywordResearch: async s => { seeds.push(...s); return { ok: true, status: 200, ideas: s.map(text => ({ text, searches: 600, competition: 'LOW', competitionIndex: 30, low: 0.6, high: 1.4, monthly: months.map(() => 600), monthlyEnd: '2026-08' })) }; }
  };
  const proposal = (occasion, peakDate, dateSource, tag, coll = 'celestial') => ({ collectionTitle: coll, occasion, peakDate, dateSource, markets: ['CA'], priority: 'medium', market: { fitWhy: 'Motifs fit the occasion.', demand: 'rising', angle: 'Made for them' }, rationale: 'Fixture', keywords: kw4(tag, coll), keywordStrategy: 'Motif by type', negatives: ['star chart'], keyPhrases: ['Made for them'], audience: { buyer: 'friends', recipient: 'a friend', motivation: 'thanks', searchStyle: 'gift phrases' } });
  const answer = { opportunities: [
    proposal('World Kindness Day', '2026-11-13', 'kindnessday.example.org/2026', 'kindness'), // cited by URL
    proposal('Sweetest Day', '2026-10-17', 'Holiday Calendar', 'sweetest day', 'maple'), // cited by site name
    proposal('National Cat Day', '2026-10-29', 'catday.example.net', 'cat lover'), // no search returned this page
    proposal('Diwali', '2026-11-8', 'diwali.example.org', 'diwali'), // malformed date
    proposal('Evergreen gifting', null, '', 'gift', 'garden')
  ] };
  e = engine({ mocks: scanMocks, now: NOW });
  e.net.queue.push({ searches: [
    { query: 'World Kindness Day 2026 date', results: [{ url: 'https://kindnessday.example.org/2026', title: 'World Kindness Day 2026', pageAge: '2 days ago' }, { url: 'javascript:alert(1)', title: 'Not a page' }] },
    { query: 'Sweetest Day 2026', results: [{ url: 'https://www.holidaycalendar.example.com/sweetest-day', title: 'Sweetest Day 2026' }, { url: 'https://kindnessday.example.org/2026', title: 'World Kindness Day 2026' }] }
  ], citations: [{ url: 'https://www.holidaycalendar.example.com/sweetest-day', title: 'Sweetest Day 2026' }], text: JSON.stringify(answer) });
  const scan = await e.E.scanOpportunities({ force: true, runId: 'fixture-run' });
  const ai = e.net.ai();
  eq(ai.length, 1, 'one strategist request');
  eq(ai[0].body.tools, [{ type: 'web_search_20260209', name: 'web_search', max_uses: 5, user_location: { type: 'approximate', country: 'CA' } }], "the research ladder's 5 searches, in the account market (Canada)");
  eq(ai[0].body.output_config.effort, 'high'); ok(/Verify every date/.test(ai[0].body.messages[0].content[0].text) && /domain or URL of the web page/.test(ai[0].body.messages[0].content[0].text), 'research prompt asks for the page it verified');
  eq(scan.scanAudit.sources.map(s => s.url), ['https://www.holidaycalendar.example.com/sweetest-day', 'https://kindnessday.example.org/2026'], 'pages kept once each, cited first; non-web links dropped');
  eq(scan.scanAudit.sources[1], { url: 'https://kindnessday.example.org/2026', title: 'World Kindness Day 2026', pageAge: '2 days ago' });
  const strategy = scan.scanAudit.checks.find(c => c.id === 'search_ai_strategy_1');
  ok(strategy && strategy.status === 'ok' && strategy.category === 'AI' && /Claude Sonnet 5\.5/.test(strategy.source), 'audit names the model');
  eq(strategy.meta.searches, 2); ok(strategy.meta.costUsd > 0.02, 'search fees included in the recorded cost');
  const dated = scan.scanAudit.checks.find(c => c.id === 'opportunity_dates');
  eq([dated.meta.research, dated.meta.unverified, dated.meta.uncited, dated.meta.undated], [2, 2, 1, 1], 'two cited dates kept; the uncited and the malformed date dropped');
  ok(/2 without a verified date \(1 cited an unsearched page\)/.test(dated.detail), dated.detail);
  ok(!seeds.some(s => /cat lover|diwali/.test(s)), 'dropped occasions spend no Keyword Planner quota');
  const kindness = scan.opportunities.find(o => o.occasion === 'World Kindness Day'), sweetest = scan.opportunities.find(o => o.occasion === 'Sweetest Day');
  ok(kindness && sweetest, 'cited occasions reach Paul: ' + JSON.stringify(scan.opportunities.map(o => o.occasion)));
  eq(kindness.dateCheck, { source: 'web-verified research', proposedDate: '2026-11-13', reference: 'kindnessday.example.org/2026', url: 'https://kindnessday.example.org/2026' }, 'the cited page is kept with the date');
  eq([kindness.peakDate, kindness.endDate, sweetest.dateCheck.url], ['2026-11-13', '2026-11-13', 'https://www.holidaycalendar.example.com/sweetest-day']);
  ok(!scan.opportunities.some(o => /Cat Day|Diwali/.test(o.occasion)), 'unverified dates never reach Paul');
  ok(!scan.scanAudit.checks.some(c => c.category === 'OpenAI'), 'no OpenAI audit rows');
  ok(e.net.calls.every(c => c.url.includes('anthropic')), 'scan made no other network request');

  // 3. Vision shot selection: labelled Shopify photos by URL, effort low.
  e = engine();
  e.net.queue.push({ text: '{"closeup_product":2,"closeup_model":null,"full_model":3}' });
  const product = { title: 'Duck Necklace', handle: 'duck', shots: [{ url: 'https://cdn.shopify.com/s/files/duck-1.jpg?v=1', width: 1000, height: 1000 }, { url: 'https://cdn.shopify.com/s/files/duck-2.jpg', width: 800, height: 1000 }, { url: 'https://cdn.shopify.com/s/files/duck-3.jpg', width: 1000, height: 800 }] };
  eq(await e.get('selectListingShots')(product), { title: 'Duck Necklace', url: product.shots[2].url, detailUrl: product.shots[1].url, modelUrl: null, ai: true }, 'roles mapped from photo numbers');
  call = e.net.ai()[0];
  eq(call.body.output_config.effort, 'low'); ok(!call.body.tools);
  const blocks = call.body.messages[0].content;
  eq(blocks.filter(b => b.type === 'image').length, 3); eq(blocks[1], { type: 'text', text: 'Photo 1' });
  eq(blocks[2], { type: 'image', source: { type: 'url', url: 'https://cdn.shopify.com/s/files/duck-1.jpg?v=1&width=512' } }, 'resized Shopify URL sent directly');
  e = engine({ env: {} });
  eq((await e.get('selectListingShots')(product)).ai, false, 'without a key the heuristic stands'); eq(e.net.calls.length, 0);

  // 4. Creative image review: structured verdict; only a failed verdict is a rejection.
  e = engine();
  const review = e.get('_reviewCreativeImages');
  e.net.queue.push({ text: '{"pass":true,"productFaithful":true,"mobileReadable":true,"issues":[],"score":92}' });
  eq((await review(Buffer.from('source-photo'), [Buffer.from('final-1'), Buffer.from('final-2')], { visualDirection: 'soft light' })).score, 92);
  call = e.net.ai()[0];
  eq(call.body.output_config.effort, 'high'); eq(call.body.output_config.format.type, 'json_schema');
  ok(!('minimum' in call.body.output_config.format.schema.properties.score) && /score: minimum 0, maximum 100/.test(call.body.system), 'score bounds moved to rules');
  eq(call.body.messages[0].content.map(b => b.type === 'text' ? (b.text.length > 40 ? 'prompt' : b.text) : b.source.media_type), ['prompt', 'SOURCE', 'image/jpeg', 'FINAL', 'image/jpeg', 'FINAL', 'image/jpeg']);
  eq(call.body.messages[0].content[2].source.data, Buffer.from('source-photo').toString('base64'));
  e.net.queue.push({ text: '{"pass":false,"productFaithful":false,"mobileReadable":true,"issues":["Engraving changed"],"score":60}' });
  await rejects(() => review(Buffer.from('s'), [Buffer.from('f')], {}), err => err.verdict === true && /needs changes: Engraving changed/.test(err.message), 'failed verdict');
  e.net.queue.push({ status: 400 });
  await rejects(() => review(Buffer.from('s'), [Buffer.from('f')], {}), err => !err.verdict && /Visual review failed/.test(err.message), 'reviewer outage is not a verdict');

  // 5. Creative production: keys are split, and paid images survive a reviewer outage.
  const group = { key: 'g0', ref: 'customers/123/assetGroups/-10', name: 'Duck', channel: 'pmax', url: 'https://britesjewelry.com/products/duck', keywords: ['duck necklace'], original: {} };
  const draft = channelGroup => ({ type: 'creative', status: 'PENDING', payload: { reviewGroups: [channelGroup], mutateOperations: [], meta: { handle: 'ducks', assetGroups: [{ itemIds: ['shopify_US_10_100'] }], sourceProducts: [{ id: 'gid://shopify/Product/10', title: 'Duck Necklace', shots: [{ url: 'https://cdn.shopify.com/s/files/duck.jpg' }] }] } } });
  const concept = JSON.stringify({ brief: { buyer: 'Duck lovers', promise: 'A tiny duck charm', visualDirection: 'Soft studio light', rationale: 'Exact product', hypothesis: 'Clear product wins', successMetric: 'purchase ROAS', demographics: 'broad' }, copy: { headlines: ['Duck Necklace'], longHeadlines: ['A duck necklace'], descriptions: ['Shop the duck necklace.'] } });
  const creativeMocks = db => ({ fb: () => db.fb, control: async () => ({ creativeBudgetUsd: 8 }), _accountCurrency: async () => 'CAD', _copyValid: () => true,
    playbookSlice: async () => ({ lessons: [], antiPatterns: [] }),
    _creativeFetch: async (url, image) => image ? Buffer.from('source-photo') : '<html><body>' + 'Handcrafted duck necklace in sterling silver with a tiny duck charm. '.repeat(4) + '</body></html>',
    _saveCreativeAsset: async (id, bytes, kind, extra) => ({ path: 'Brites_GAds_Creative/' + id + '/' + kind + '-fixture.jpg', hash: 'hash-' + kind, bytes: bytes.length, ...(extra || {}) }),
    _loadCreativeAsset: async a => Buffer.from('final-' + a.path) });
  const approval = 'Brites_GAds_Approvals/draft1';
  let db = memoryDb({ [approval]: draft(group) });
  e = engine({ env: { OPENAI_API_KEY: 'images-only' }, mocks: creativeMocks(db) });
  await rejects(() => e.E.prepareCreativeApproval('draft1'), /ANTHROPIC_API_KEY is missing/); eq(e.net.calls.length, 0, 'no paid request without the Anthropic key');
  db = memoryDb({ [approval]: draft(group) });
  e = engine({ mocks: creativeMocks(db) });
  await rejects(() => e.E.prepareCreativeApproval('draft1'), /OPENAI_API_KEY is missing\. Product photography/); eq(e.net.calls.length, 0, 'product photography needs the OpenAI key before any spend');

  db = memoryDb({ [approval]: draft({ ...group, ref: 'customers/123/adGroups/-20', channel: 'search' }) });
  e = engine({ mocks: creativeMocks(db) });
  e.net.queue.push({ text: concept }, { text: '{"pass":true,"issues":[]}' });
  ok((await e.E.prepareCreativeApproval('draft1')).ok, 'Search copy is written and reviewed without an OpenAI key');
  eq(e.net.ai().map(c => [c.body.model, c.body.output_config.effort]), [['claude-sonnet-5-5', 'medium'], ['claude-sonnet-5-5', 'medium']], 'concept and copy check run on Sonnet 5.5');
  eq(db.docs.get(approval).creative.phase, 'ready');

  db = memoryDb({ [approval]: draft(group) });
  e = engine({ env: { ANTHROPIC_API_KEY: 'test-do-not-send', OPENAI_API_KEY: 'images-only' }, mocks: creativeMocks(db) });
  e.net.queue.push({ text: concept }, { text: '{"pass":true,"issues":[]}' }, { status: 400, message: 'Could not process image' });
  await rejects(() => e.E.prepareCreativeApproval('draft1'), /Visual review failed/);
  let saved = db.docs.get(approval).creative;
  eq(Object.keys(saved.groups[0].assets).sort(), ['landscape', 'portrait', 'square'], 'paid images kept when the reviewer is unreachable');
  ok(!saved.groups[0].rejectedAssets && saved.phase === 'needs_changes' && saved.imageRequests === 3);
  eq(e.net.images().length, 3); ok(e.net.images().every(c => c.init.headers.Authorization === 'Bearer images-only'), 'only image generation uses the OpenAI key');
  e.net.queue.push({ text: '{"pass":true,"productFaithful":true,"mobileReadable":true,"issues":[],"score":93}' });
  const resumed = await e.E.prepareCreativeApproval('draft1', { retry: true });
  ok(resumed.ok && resumed.imageSpendUsd === 3, 'resume reviews the saved images');
  eq(e.net.images().length, 3, 'resume never re-buys images'); eq(e.net.ai().length, 4, 'resume only repeats the review');
  eq(db.docs.get(approval).creative.phase, 'ready');

  db = memoryDb({ [approval]: draft(group) });
  e = engine({ env: { ANTHROPIC_API_KEY: 'test-do-not-send', OPENAI_API_KEY: 'images-only' }, mocks: creativeMocks(db) });
  e.net.queue.push({ text: concept }, { text: '{"pass":true,"issues":[]}' }, { text: '{"pass":false,"productFaithful":false,"mobileReadable":true,"issues":["Chain redesigned"],"score":55}' });
  await rejects(() => e.E.prepareCreativeApproval('draft1'), /Visual review needs changes: Chain redesigned/);
  saved = db.docs.get(approval).creative;
  eq(saved.groups[0].assets, {}, 'a failed verdict clears the images for regeneration'); eq(Object.keys(saved.groups[0].rejectedAssets).length, 3);

  // 6. No OpenAI text model, vision model or chat endpoint is left in the autopilot.
  ok(!/gpt-6-astra|OPENAI_VISION_MODEL|GADS_GEN_MODEL \|\||api\.openai\.com\/v1\/chat\/completions/.test(source), 'only image generation still calls OpenAI');
  say('PASS ' + passed + ' Sonnet 5.5 autopilot wrapper, scan web search, vision, creative review and key-split checks');
})().catch(err => { process.stderr.write(String(err && err.stack || err) + '\n'); process.exit(1); });
