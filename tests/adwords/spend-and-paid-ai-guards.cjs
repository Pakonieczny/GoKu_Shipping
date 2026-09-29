// Budget guards and paid-AI triggers. Every budget refusal gives its figures in the account currency;
// the Controls ceiling cannot pass the site limit; a planned run length starts when the campaign is
// enabled (unless it already served); new PMax and Search campaigns launch without a target ROAS, which
// Ad Doctor offers later as a reviewed draft; a proposed negative is checked against every converting
// search term (PMax: search categories); and no paid AI request starts without an explicit request
// (opening a view, a reload or a scheduled run reads saved results; copy that failed brand safety is
// not paid for again by the daily run). Synthetic data only; every live call fails.
const fs = require('fs'), vm = require('vm'), assert = require('assert/strict'), path = require('path'), Module = require('module');
const repo = path.resolve(__dirname, '../..'), dir = path.join(repo, 'netlify/functions') + '/', file = dir + 'googleAdsAutopilot.js', realRequire = Module.createRequire(file);
const clone = x => x == null ? x : JSON.parse(JSON.stringify(x)), plain = x => JSON.parse(JSON.stringify(x));
let passed = 0; const check = (v, msg) => { assert(v, msg); passed++; console.log('PASS', msg); };
const DAY = 86400000, ymd = t => new Date(t).toISOString().slice(0, 10), plus = (d, n) => ymd(Date.parse(d + 'T12:00:00Z') + n * DAY), today = ymd(Date.now());

function engine(env = {}) {
  const cx = vm.createContext({ module: { exports: {} }, exports: {}, require: n => n === 'node-fetch' ? (...a) => cx.__fetch(...a) : realRequire(n),
    process: { env: { GADS_CUSTOMER_ID: '123', ...env } }, console, Buffer, Date, Intl, Map, Set, URL, setTimeout, clearTimeout });
  cx.__fetch = async () => { throw Error('Live network forbidden'); };
  vm.runInContext(fs.readFileSync(file, 'utf8'), cx);
  const e = { cx, get: n => vm.runInContext(n, cx), bind(v) { cx.__m = v; vm.runInContext(Object.keys(v).map(k => k + '=__m.' + k).join('\n'), cx); } };
  e.bind({ gaql: async () => { throw Error('Live Google Ads read forbidden'); }, mintToken: async () => { throw Error('Live token forbidden'); },
    openaiJSON: async () => { throw Error('Paid AI call forbidden'); }, mutate: async () => { throw Error('Live change forbidden'); }, mutateAll: async () => { throw Error('Live change forbidden'); },
    _accountTz: async () => 'UTC', _accountCurrency: async () => 'CAD', ledger: async () => {} });
  return e;
}
function memory() {
  const docs = new Map(); let n = 0;
  const doc = p => ({ path: p, id: p.split('/').pop(), get: async () => ({ exists: docs.has(p), id: p.split('/').pop(), data: () => clone(docs.get(p)), ref: doc(p) }),
    set: async (v, o) => { docs.set(p, o && o.merge ? { ...(docs.get(p) || {}), ...clone(v) } : clone(v)); },
    update: async v => { docs.set(p, { ...(docs.get(p) || {}), ...clone(v) }); }, collection: c => col(p + '/' + c) });
  const col = (p, fs = []) => ({ doc: id => doc(p + '/' + id), add: async v => { const r = doc(p + '/auto' + (++n)); await r.set(v); return r; },
    where: (k, op, v) => col(p, fs.concat([[k, v]])), orderBy: () => col(p, fs), limit: () => col(p, fs),
    get: async () => { const list = [...docs].filter(([k, v]) => k.startsWith(p + '/') && !k.slice(p.length + 1).includes('/') && fs.every(([fk, fv]) => fk.split('.').reduce((o, k) => o == null ? o : o[k], v) === fv)).map(([k, v]) => ({ id: k.split('/').pop(), exists: true, data: () => clone(v), ref: doc(k) }));
      return { docs: list, empty: !list.length, size: list.length, forEach: fn => list.forEach(fn) }; } });
  return { docs, db: { collection: c => col(c), runTransaction: async fn => fn({ get: r => r.get(), set: (r, v, o) => r.set(v, o), update: (r, v) => r.update(v), delete: r => docs.delete(r.path) }),
    batch: () => { const ops = []; return { set: (r, v, o) => ops.push(() => r.set(v, o)), commit: async () => { for (const op of ops) await op(); } }; } }, FV: { serverTimestamp: () => Date.now() } };
}
// Router and worker with a fake engine; dispatches to the worker are recorded, never sent. The passcode
// helper runs with the same test env (EDIT_PASSCODE), so no Firestore passcode is read.
function load(name, env, E, calls) {
  const mod = { exports: {} }, saved = [], admin = { firestore: () => ({ collection: c => ({ doc: d => ({ get: async () => ({ exists: false }), set: async v => { saved.push([c + '/' + d, v]); } }) }) }) };
  admin.firestore.FieldValue = { serverTimestamp: () => Date.now() };
  const penv = { URL: 'https://example.invalid', ...env }, ep = { exports: {} }, epCtx = { process: { env: penv }, console, Date, module: ep, exports: ep.exports, require: n => n === './firebaseAdmin' ? admin : realRequire(n) };
  vm.createContext(epCtx); vm.runInContext(fs.readFileSync(dir + '_editPasscode.js', 'utf8'), epCtx);
  const ctx = { process: { env: penv }, console, Date, Set, Map, JSON, module: mod, exports: mod.exports,
    require: n => n === 'node-fetch' ? async (url, opts) => { calls.push(['dispatch', JSON.parse(opts.body)]); return { ok: true, status: 202 }; } : n === './googleAdsAutopilot' ? E : n === './firebaseAdmin' ? admin : n === './_editPasscode' ? ep.exports : realRequire(n) };
  vm.createContext(ctx); vm.runInContext(fs.readFileSync(dir + name, 'utf8'), ctx); return { api: mod.exports, ctx, saved };
}
const post = (body, headers = {}) => ({ httpMethod: 'POST', headers, body: JSON.stringify(body) });
const html = fs.readFileSync(path.join(repo, 'brites-adwords.html'), 'utf8');
const pick = name => { const m = new RegExp('^(?:async )?function ' + name + '\\(', 'm').exec(html); assert(m, name); const rest = html.slice(m.index), next = /\n(?:async )?function \w+\(|\nvar \w+=/.exec(rest.slice(1)); return next ? rest.slice(0, next.index + 1) : rest; };

(async () => {
  const COL = engine().get('COL');

  // ── 1-2. Budget refusals: one total check, figures in the account currency ──────────
  {
    const e = engine(), f = memory(); let sent = 0;
    await f.db.collection(COL.approvals).doc('n1').set({ type: 'pmax', status: 'APPROVED', payload: { mutateOperations: [
      { campaignBudgetOperation: { create: { resourceName: 'customers/123/campaignBudgets/-1', amountMicros: 50e6 } } },
      { campaignOperation: { create: { resourceName: 'customers/123/campaigns/-2', name: 'BA · test', status: 'PAUSED', campaignBudget: 'customers/123/campaignBudgets/-1' } } }] } });
    e.bind({ fb: () => f, assertCreativeReviewed: () => {}, materializeReviewedCreative: async it => clone(it.payload.mutateOperations), _deletedCampaignIds: async () => new Set(),
      gaql: async () => [], _enabledBudgetTotal: async () => 90, mutateAll: async () => { sent++; return {}; } });
    await assert.rejects(() => e.get('applyApproval')('n1', { maxDailyBudgetTotal: 100, budgetCurrency: 'CAD' }),
      e => /these budgets total CAD 50\.00, but only CAD 10\.00 of your CAD 100\.00 ceiling is free \(enabled campaigns use CAD 90\.00\)/.test(e.message) && /Nothing was published/.test(e.message));
    const left = (await f.db.collection(COL.approvals).doc('n1').get()).data();
    check(sent === 0 && left.status === 'APPROVED' && /CAD 10\.00/.test(left.lastError), 'a new campaign over the ceiling is refused with what it adds, what is free and what enabled campaigns use, before anything is sent');
    const src = fs.readFileSync(file, 'utf8'), branch = src.slice(src.indexOf('} else if (ex.kind === "setBudget") {'), src.indexOf('remedy kind \'"'));
    check(/setCampaignBudget\(campaignId, budget, \{ ctrl \}\)/.test(branch) && !/_enabledBudgetTotal/.test(branch), 'an Ad Doctor budget fix uses the one setCampaignBudget total check instead of its own');
  }

  // ── 5. The Controls ceiling cannot pass the site limit ────────────────────────────────
  {
    const f = memory(); await f.db.collection(COL.control).doc('control').set({ maxDailyBudgetTotal: 300 });
    let e = engine({ GADS_MAX_DAILY_BUDGET_TOTAL: '150' }); e.bind({ fb: () => f });
    let c = plain(await e.get('control')());
    check(c.maxDailyBudgetTotal === 150 && c.maxDailyBudgetLimit === 150, 'the dashboard control carries the effective ceiling and the site limit');
    e = engine(); e.bind({ fb: () => f }); c = plain(await e.get('control')());
    check(c.maxDailyBudgetTotal === 300 && c.maxDailyBudgetLimit === null, 'without a site limit the saved ceiling applies and no limit is reported');
    const calls = [], E = { COL: { state: 'state', control: 'control', approvals: 'approvals' }, control: async () => ({ enabled: true, maxDailyBudgetLimit: 150 }) };
    const K = load('googleAdsAutopilotKick.js', { EDIT_PASSCODE: 'pw' }, E, calls), H = { 'x-edit-passcode': 'pw' };
    let r = await K.api.httpHandler(post({ action: 'setControl', patch: { maxDailyBudgetTotal: 200 } }, H));
    check(r.statusCode === 400 && /from 1 to 150\. Nothing was saved/.test(JSON.parse(r.body).error) && !K.saved.length, 'a ceiling above the site limit is refused, not saved and silently capped');
    r = await K.api.httpHandler(post({ action: 'setControl', patch: { maxDailyBudgetTotal: 150 } }, H));
    check(r.statusCode === 200 && K.saved[0][1].maxDailyBudgetTotal === 150, 'the site limit itself is accepted');
    const { JSDOM } = require('jsdom'), dom = new JSDOM('<input type="range" id="cCeil" min="40" max="500" step="10">'), stub = () => ({ textContent: '', innerHTML: '', value: '', getAttribute: () => null, classList: { contains: () => false } });
    const els = { '#cCeil': dom.window.document.getElementById('cCeil') }, ui = { DASH: { control: { maxDailyBudgetTotal: 150, maxDailyBudgetLimit: 150 } }, $: s => els[s] || (els[s] = stub()), $$: () => [],
      cur: () => 'CAD', ctrlField: () => {}, setSw: () => {}, tsMs: () => 0, ccyMoney: v => 'CAD ' + v, roasTxt: () => '', esc: String, ctyChips: () => '', defCountries: () => [] };
    vm.createContext(ui); vm.runInContext(pick('renderControls'), ui); ui.renderControls();
    check(els['#cCeil'].max === '150' && els['#cCeil'].value === '150', 'the Controls slider stops at the most the server accepts');
    ui.DASH.control = { maxDailyBudgetTotal: 100 }; ui.renderControls();
    check(els['#cCeil'].max === '500', 'with no site limit the slider keeps its range');
  }

  // ── 3. Lessons: automatic runs pay only for a new measured fix result ────────────────
  {
    const e = engine(), f = memory(); let paid = 0, outcomes = { '42': [{ historyId: 'r1', working: 'working', note: 'n', daysAgo: 40 }] };
    e.bind({ fb: () => f, getPlaybook: async () => null, getDiagnostics: async () => ({ campaigns: [{ id: '42', channel: 'SEARCH', startDate: '2020-01-01', d90: { clicks: 500 } }] }),
      remedyHistory: async () => ({ items: [{ id: 'r1', campaignId: '42', kind: 'addNegatives', verified: true, at: Date.now() - 40 * DAY, baseline: { clicks: 400 }, executable: { keywords: ['free'] } }] }),
      _remedyOutcomes: async () => clone(outcomes), openaiJSON: async () => { paid++; throw Error('no model in tests'); } });
    const distill = o => e.get('distillLessons')(o);
    await assert.rejects(() => distill({ auto: true }), /no model in tests/);
    check(paid === 1, 'after a diagnosis, a newly measured fix result is sent for learning once');
    let out = plain(await distill({ auto: true }));
    check(paid === 1 && out.unchanged && /No new measured fix result/.test(out.reason), 'the next diagnosis with the same measured results makes no AI request');
    outcomes = { '42': [{ historyId: 'r1', working: 'not working', note: 'n', daysAgo: 55 }] };
    await assert.rejects(() => distill({ auto: true }), /no model in tests/);
    check(paid === 2, 'a changed measured result is new evidence');
    outcomes = { '42': [{ historyId: 'r2', working: 'not enough data', note: 'n', daysAgo: 20 }] };
    out = plain(await distill({ auto: true }));
    check(paid === 2 && out.unchanged, '"not enough data" is not a result worth paying for');
    outcomes = { '42': [{ historyId: 'r1', working: 'not working', note: 'n', daysAgo: 55 }] };
    await assert.rejects(() => distill({}), /no model in tests/);
    check(paid === 3, 'Update from results still runs on request');
  }

  // ── 4. Negatives are checked against every converting term and enabled keyword ───────
  {
    const e = engine(), queries = [];
    let fail = false;
    e.bind({ gaql: async q => {
      if (/campaign\.primary_status_reasons/.test(q)) return [{ campaign: { id: '42', name: 'Search', status: 'ENABLED', primaryStatus: 'ELIGIBLE', primaryStatusReasons: [], advertisingChannelType: 'SEARCH' }, campaignBudget: { amountMicros: '10000000' }, metrics: {} }];
      if (/metrics\.conversions > 0/.test(q)) { queries.push(q); if (fail) throw Error('read failed'); return [{ campaign: { id: '42' }, searchTermView: { searchTerm: 'engraved locket for mom' }, metrics: { conversions: 1 } }]; }
      if (/ad_group_criterion\.negative = FALSE/.test(q)) return [{ campaign: { id: '42' }, adGroupCriterion: { keyword: { text: 'moon charm' } } }];
      return [];
    } });
    let d = plain(await e.get('fetchDiagnostics')(null));
    const range = /BETWEEN '(\d{4}-\d{2}-\d{2})' AND '(\d{4}-\d{2}-\d{2})'/.exec(queries[0]);
    check(d.negativeGuard['42'].converting.includes('engraved locket for mom') && d.negativeGuard['42'].keywords.includes('moon charm'), 'the diagnosis reads every converting term and enabled keyword, not only the top rows');
    check(range && (Date.parse(range[2]) - Date.parse(range[1])) / DAY === 364 && !/\bLIMIT\b/.test(queries[0]), 'converting terms cover a full year with no row limit');
    fail = true; d = plain(await e.get('fetchDiagnostics')(null));
    check(!('negativeGuard' in d), 'a failed guard read leaves the diagnosis without it (so no negative is offered)');
  }

  // ── 6. New PMax campaigns launch without a target ROAS, and the draft says so ─────────
  {
    const e = engine({ GADS_TARGET_ROAS: '4' }), f = memory(); let item = null;
    e.bind({ fb: () => f, _assertOpportunityNotDeleted: async () => {}, _pmaxResearchCandidate: () => {}, control: async () => ({ targetRoas: 4, defaultCountries: ['2124'], maxDailyBudgetTotal: 100 }),
      getCollections: async () => [{ handle: 'rings', title: 'Rings' }], merchantCenterId: async () => '777', collectionProfiles: async () => ({ list: [] }),
      merchantProducts: async () => [{ itemId: 'shopify_CA_111_222', title: 'Moon Ring', feedLabel: 'CA', link: 'https://britesjewelry.com/products/moon-ring' }], _pmaxIsEligible: () => true,
      discoverPmaxAudienceResource: async () => ({ resource: null }), listCountries: async () => [{ code: 'CA', id: '2124' }], _pmaxAdCopy: async () => null,
      enqueueApproval: async x => { item = x; return 'a1'; } });
    await e.get('generatePmaxApproval')({ handle: 'rings', dailyBudget: 10, targetRoas: 2.5, days: 30, itemIds: ['shopify_CA_111_222'], productTitles: ['Moon Ring'], feedLabel: 'CA' });
    const m = item.payload.meta, camp = item.payload.mutateOperations.find(o => o.campaignOperation).campaignOperation.create;
    check(m.targetRoas === 0 && m.targetRoasLater === 2.5 && m.biddingMode === 'MAXIMIZE_CONVERSION_VALUE_LEARNING' && !camp.maximizeConversionValue.targetRoas, 'a new PMax campaign is built on Maximize conversion value without the requested or account target ROAS');
    check(/no target ROAS until it has about 6 weeks and 30 conversions in 30 days/.test(item.summary) && /30 days from enabling/.test(item.summary), 'the draft summary says which bidding applies and the planned run length');
    check(m.runDays === 30 && m.plannedDays[camp.resourceName] === 30 && camp.endDateTime === plus(today, 29).replace(/-/g, '') + ' 23:59:59', 'the planned days are kept with the draft; its first end date covers exactly 30 days');
    const ops = e.get('buildPmaxCampaignOps')({ handle: 'rings', title: 'Rings' }, { dailyBudget: 5, merchantId: 1, itemIds: ['shopify_CA_111_222'] }).ops;
    check(JSON.stringify(ops.find(o => o.campaignOperation).campaignOperation.create.maximizeConversionValue) === '{}', 'GADS_TARGET_ROAS is not applied to a new campaign by default');
  }

  // ── 7. A planned run length counts from publishing, then from enabling ────────────────
  {
    const e = engine(), f = memory(), sent = [], muts = []; let start = null;
    const prepared = plus(today, -10);
    await f.db.collection(COL.approvals).doc('p1').set({ type: 'pmax', status: 'APPROVED', payload: { mutateOperations: [
      { campaignBudgetOperation: { create: { resourceName: 'customers/123/campaignBudgets/-1', amountMicros: 10e6 } } },
      { campaignOperation: { create: { resourceName: 'customers/123/campaigns/-2', name: 'BA · flight', status: 'PAUSED', campaignBudget: 'customers/123/campaignBudgets/-1', endDateTime: plus(prepared, 29).replace(/-/g, '') + ' 23:59:59' } } }],
      meta: { plannedDays: { 'customers/123/campaigns/-2': 30 } } } });
    e.bind({ fb: () => f, assertCreativeReviewed: () => {}, materializeReviewedCreative: async it => clone(it.payload.mutateOperations), _deletedCampaignIds: async () => new Set(),
      _enabledBudgetTotal: async () => 0, _assertCampaignNotDeleted: async () => {}, _assertSpendLimits: async () => 'customers/123/campaignBudgets/9',
      gaql: async q => /start_date_time FROM campaign WHERE campaign\.id = 777/.test(q) && start ? [{ campaign: { id: '777', startDateTime: start + ' 00:00:00' } }] : [],
      mutateAll: async (ops, o) => { sent.push({ ops: clone(ops), validateOnly: !!o.validateOnly }); return o.validateOnly ? {} : { mutateOperationResponses: ops.map(x => x.campaignOperation ? { campaignResult: { resourceName: 'customers/123/campaigns/777' } } : {}) }; },
      mutate: async (service, ops) => { muts.push(clone(ops)); return {}; } });
    const ctrl = { maxDailyBudgetTotal: 100, budgetCurrency: 'CAD' }, end = s => s.ops.find(o => o.campaignOperation).campaignOperation.create.endDateTime;
    await e.get('applyApproval')('p1', ctrl);
    check(sent.length === 2 && sent.every(s => end(s) === plus(today, 29).replace(/-/g, '') + ' 23:59:59'), 'publishing sets the end date from the publish day, not the day the draft was prepared');
    const flight = f.docs.get(COL.state + '/plannedFlight_777');
    check(flight && flight.days === 30 && flight.approvalId === 'p1' && !flight.startedAt, 'the published campaign keeps its planned days until it is first enabled');
    const later = plus(today, 12); e.bind({ _acctDateYmd: () => later });
    let r = plain(await e.get('setCampaignStatus')('777', 'ENABLED', { ctrl }));
    check(muts[0][0].updateMask === 'status,end_date_time' && muts[0][0].update.endDateTime === plus(later, 29).replace(/-/g, '') + ' 23:59:59' && r.endDate === plus(later, 29), 'the first enable moves the end date in the same update, so the full 30 days start that day');
    check(f.docs.get(COL.state + '/plannedFlight_777').startedAt > 0, 'the run is marked started');
    r = plain(await e.get('setCampaignStatus')('777', 'ENABLED', { ctrl }));
    check(muts[1][0].updateMask === 'status' && !r.endDate, 'a later enable (after a pause) leaves the end date alone');
    await f.db.collection(COL.state).doc('plannedFlight_778').set({ days: 14, approvalId: 'p2', publishedAt: Date.now() }); start = plus(later, 5);
    e.bind({ gaql: async q => /start_date_time FROM campaign WHERE campaign\.id = 778/.test(q) ? [{ campaign: { id: '778', startDateTime: start + ' 00:00:00' } }] : [] });
    r = plain(await e.get('setCampaignStatus')('778', 'ENABLED', { ctrl: { ...ctrl, dryRun: true } }));
    check(r.endDate === plus(start, 13) && !f.docs.get(COL.state + '/plannedFlight_778').startedAt, 'a scheduled later start counts from that start; a dry run marks nothing started');
    let endRead = '2026-10-01';
    e.bind({ gaql: async q => /FROM campaign WHERE campaign\.id = 778/.test(q) ? [{ campaign: { id: '778', name: 'C', status: 'PAUSED', startDateTime: '2026-09-01 00:00:00', endDateTime: endRead + ' 23:59:59', endDate: endRead } }] : [],
      mutate: async (service, ops) => { muts.push(clone(ops)); endRead = plus(later, 60); return {}; } });
    await e.get('setCampaignEndDate')('778', { endDate: plus(later, 60), ctrl });
    check(f.docs.get(COL.state + '/plannedFlight_778').settledAt > 0, 'an end date chosen in Campaigns replaces the planned run length');
    r = plain(await e.get('setCampaignStatus')('778', 'ENABLED', { ctrl }));
    check(muts[muts.length - 1][0].updateMask === 'status' && !r.endDate, 'enabling after that keeps the chosen end date');
  }

  // ── 10. Occasions, Studio and scheduled events: only an explicit request pays ─────────
  {
    const e = engine(), f = memory(); let paid = 0, reply = null;
    e.bind({ fb: () => f, collectionMeta: async () => ({ title: 'Rings' }), openaiJSON: async () => { paid++; if (!reply) throw Error('model unavailable'); return reply; } });
    // Occasions: opening the builder, switching collection and the Sales link never reach the model.
    let list = plain(await e.get('suggestOccasions')('rings'));
    check(paid === 0 && list.length > 1 && list.some(o => /evergreen/i.test(o.label)), 'without a saved list the standard occasions are shown, with no AI request');
    await assert.rejects(() => e.get('suggestOccasions')('rings', { force: true }), /saved list is unchanged/);
    check(paid === 1 && !f.docs.has(COL.state + '/occasions_rings'), 'a failed refresh throws and saves nothing');
    reply = { occasions: [{ label: 'Anniversary gifting', daysOut: 40, recommendation: 'push', proven: true, why: 'w' }] };
    list = plain(await e.get('suggestOccasions')('rings', { force: true }));
    check(paid === 2 && list.some(o => o.label === 'Anniversary gifting') && f.docs.has(COL.state + '/occasions_rings'), 'Suggest asks the AI and saves the list');
    check(list.find(o => o.label === 'Anniversary gifting').proven === false, 'proven comes from records, not the model');
    const doc = f.docs.get(COL.state + '/occasions_rings'); doc.at = Date.now() - 10 * DAY; f.docs.set(COL.state + '/occasions_rings', doc);
    list = plain(await e.get('suggestOccasions')('rings'));
    check(paid === 2 && list.find(o => o.label === 'Anniversary gifting').daysOut === 30, 'a saved list is reused at no cost, however old (no 12-hour expiry), its day counts moved on');

    // Studio: the scheduled refresh keeps the last AI synthesis instead of paying for a new one.
    const e2 = engine({ ANTHROPIC_API_KEY: 'test' }), f2 = memory(); let studioPaid = 0;
    await f2.db.collection(COL.state).doc('designStudioAcquisition').set({ learning: { generatedAt: 1000, synthesis: { summary: 'kept' } } });
    e2.bind({ fb: () => f2, control: async () => ({ targetRoas: 0 }), openaiJSON: async () => { studioPaid++; return { summary: 'fresh' }; },
      designStudioPerformance: async () => ({ readiness: { apiOk: true, purchaseReady: true, assistCoverage: 4 }, overall: { clicks: 80, cost: 100, impressions: 2000, ctr: 0.04 },
        purchase: { conversions: 4, value: 400, cpa: 25, roas: 4 }, funnel: { start: 0, design: 0, approve: 0 }, rates: {}, campaigns: [{ id: '1' }], assetGroups: [], searchInsights: [], start: '2026-09-01', end: '2026-09-28', days: 28 }) });
    let out = plain(await e2.get('refreshDesignStudioLearning')({ withAI: false }));
    check(studioPaid === 0 && out.synthesis.summary === 'kept' && out.synthesis.at === 1000 && out.generatedAt > 1000, 'the scheduled Studio refresh makes no AI request and keeps the last synthesis, with the time it was made');
    out = plain(await e2.get('refreshDesignStudioLearning')({}));
    check(studioPaid === 1 && out.synthesis.summary === 'fresh' && out.synthesis.at >= out.generatedAt - 5000, 'the explicit Studio Analyze still asks the AI');

    // Scheduled events: a build stopped by the keyword checks pays for no copy.
    let copies = 0;
    e.bind({ collectionMeta: async h => ({ handle: h, title: 'Rings' }), collectionProfiles: async () => ({ list: [] }), getCollections: async () => [], _enabledBudgetTotal: async () => 0,
      researchOpportunity: async () => ({ ok: false }), storeSignals: async () => ({ orders: 0 }), accountCvr: async () => null, _fxRateToUsd: async () => null,
      generateRSAAssets: async () => { copies++; return null; } });
    const g = plain(await e.get('generateForCollection')('rings', 'Anniversary gifting', 8, { ctrl: { maxDailyBudgetTotal: 100, budgetCurrency: 'CAD' } }));
    check(g.ok === false && /keyword/i.test(g.reason) && copies === 0, 'a scheduled or custom build stopped by the keyword checks makes no copy request');
  }

  // ── Router and worker: reads are cache-only, Suggest runs in the background ──────────
  {
    const calls = [], E = { COL: { state: 'state', control: 'control', approvals: 'approvals' }, control: async () => ({ enabled: true }),
      suggestOccasions: async (coll, o) => (calls.push(['occasions', coll, clone(o)]), [{ label: 'Evergreen gifting' }]) };
    const K = load('googleAdsAutopilotKick.js', { EDIT_PASSCODE: 'pw' }, E, calls), H = { 'x-edit-passcode': 'pw' }, j = async b => JSON.parse((await K.api.httpHandler(post(b, H))).body);
    for (const body of [{ coll: 'rings', cacheOnly: true }, { coll: 'rings' }, { coll: 'rings', cacheOnly: true, force: true }]) {
      calls.length = 0; const r = await j({ action: 'occasions', ...body });
      assert(r.occasions.length === 1 && calls.length === 1 && calls[0][2].force === false, JSON.stringify(body));
    }
    check(true, 'the occasions action with cacheOnly (or without force) reads without reaching the model or the worker');
    calls.length = 0; const r = await j({ action: 'occasions', coll: 'rings', force: true }), d = calls.find(c => c[0] === 'dispatch');
    check(r.queued && r.genId && d && d[1].tasks[0] === 'suggestOccasions' && d[1].coll === 'rings' && d[1].genId === r.genId && !calls.some(c => c[0] === 'occasions'), 'Suggest is dispatched to the background worker, past the 26s gateway, with a status id to poll');
    check(!K.ctx.isReadAction('occasions', {}), 'occasions stays out of the read-only actions');

    const statuses = [], args = [], W = { COL: E.COL, control: async () => ({ enabled: false }), setGenStatus: async (id, s) => { statuses.push([id, clone(s)]); },
      suggestOccasions: async (coll, o) => { args.push(['occasions', coll, o]); if (coll === 'bad') throw Error('The AI did not return occasions. The saved list is unchanged.'); return [{ label: 'Evergreen gifting' }]; },
      refreshDesignStudioLearning: async o => { args.push(['studio', o]); return { phase: 'learning', recommendations: [] }; },
      runDiagnostics: async () => ({ generatedAt: 1 }), distillLessons: async o => { args.push(['distill', o]); return { unchanged: true }; } };
    const B = load('googleAdsAutopilot-background.js', { EDIT_PASSCODE: 'pw' }, W, []), run = async b => JSON.parse((await B.api.handler(post({ ...b, token: 'pw' }))).body);
    await run({ tasks: ['suggestOccasions'], coll: 'rings', genId: 'g1' });
    const done = statuses.filter(s => s[0] === 'g1');
    check(args.some(a => a[0] === 'occasions' && a[1] === 'rings' && a[2].force === true) && done[0][1].phase === 'running' && done[1][1].ok === true && done[1][1].occasions.length === 1, 'the worker runs Suggest with automation off and posts the list for the poll');
    await run({ tasks: ['suggestOccasions'], coll: 'bad', genId: 'g2' });
    const failed = statuses.filter(s => s[0] === 'g2').pop();
    check(failed[1].ok === false && /saved list is unchanged/.test(failed[1].error), 'a failed refresh reports its error to the poll');
    await run({ tasks: ['designStudioLearn'] }); await run({ tasks: ['designStudioAnalyze'], genId: 'g3' });
    const studio = args.filter(a => a[0] === 'studio');
    check(studio[0][1].withAI === false && studio[1][1].withAI === true, 'the scheduled Studio refresh passes withAI false; the explicit Analyze passes true');
    await run({ tasks: ['diagnostics'], runId: 'r1' });
    check(args.some(a => a[0] === 'distill' && a[1].auto === true), 'a diagnosis asks for lessons only as an automatic run');
  }

  // ── Console: every builder path reads cache-only; only Suggest polls the worker ──────
  {
    const { JSDOM } = require('jsdom'), dom = new JSDOM('<details id="customWrap"></details><select id="bColl"></select><select id="bEvent"></select><div id="bOccWhy"></div><button id="bOccRefresh"></button><span id="curLab"></span>'), d = dom.window.document;
    const requests = [], toasts = []; let gen = null;
    const ui = { document: d, Option: dom.window.Option, $: s => d.querySelector(s), toast: (m, ok) => toasts.push([m, ok]), actStart: () => 1, actEnd: () => {}, setTimeout: fn => fn(), Promise, Date,
      occReq: 0, OCC: [], benchLoaded: false, cur: () => 'CAD', go: () => {},
      api: async (a, x) => { requests.push([a, clone(x)]); if (a === 'collections') return { collections: [{ title: 'Rings', handle: 'rings' }, { title: 'Moon', handle: 'moon' }] };
        if (a === 'occasions' && !x.force) return { occasions: [{ label: 'Evergreen gifting', why: 'always' }] }; if (a === 'occasions') return { queued: true, genId: 'o1' }; if (a === 'genStatus') return gen; } };
    vm.createContext(ui); for (const n of ['ensureBench', 'loadCollections', 'loadOccasions', 'fillOccasions', 'showOccWhy', 'salesAdvertise']) vm.runInContext(pick(n), ui);
    const paidPaths = () => requests.filter(r => r[0] === 'occasions' && (r[1].force || !r[1].cacheOnly));
    ui.salesAdvertise('A listing'); await new Promise(r => setImmediate(r)); await new Promise(r => setImmediate(r));
    check(requests.some(r => r[0] === 'occasions') && !paidPaths().length && d.querySelector('#bEvent').options.length === 1, 'the Sales tab advertise link opens the builder with a cache-only read');
    ui.benchLoaded = false; requests.length = 0; await ui.ensureBench();
    check(requests.some(r => r[0] === 'occasions') && !paidPaths().length, 'opening the manual builder reads occasions cache-only');
    requests.length = 0; d.querySelector('#bColl').value = 'moon'; await ui.loadOccasions(false);
    check(requests.length === 1 && requests[0][1].coll === 'moon' && !paidPaths().length, 'changing the collection reads occasions cache-only');
    check(/\$\("#bColl"\)\.onchange=\(\)=>loadOccasions\(false\)/.test(html) && /cw\.addEventListener\("toggle",function\(\)\{if\(cw\.open\)\{ensureBench\(\)/.test(html) && /\$\("#bOccRefresh"\)\.onclick=\(\)=>loadOccasions\(true\)/.test(html) && (html.match(/loadOccasions\(true\)/g) || []).length === 1,
      'the collection change and the builder toggle use those reads; only the Suggest button asks for new suggestions');
    gen = { ok: true, occasions: [{ label: 'Evergreen gifting' }, { label: 'Anniversary gifting', daysOut: 30, recommendation: 'push' }] };
    d.querySelector('#bColl').value = 'rings'; requests.length = 0; const p = ui.loadOccasions(true);
    check(/Suggesting occasions with AI/.test(d.querySelector('#bOccWhy').textContent) && d.querySelector('#bOccWhy .spin') && !d.querySelector('#bEvent').disabled, 'Suggest shows a spinner saying what is happening and keeps the current options usable');
    await p;
    check(requests.some(r => r[0] === 'genStatus' && r[1].genId === 'o1') && d.querySelector('#bEvent').options.length === 2 && !d.querySelector('#bOccWhy .spin'), 'the refreshed list replaces the options once the background run finishes');
    gen = { ok: true, occasions: [{ label: 'Stale' }] }; const stale = ui.loadOccasions(true); d.querySelector('#bColl').value = 'moon'; await ui.loadOccasions(false); await stale;
    check(![...d.querySelector('#bEvent').options].some(o => o.value === 'Stale'), 'a refresh for a collection no longer selected does not replace the list');
    check(/runs to "\+r2\.endDate/.test(html) && /days from the day you enable it/.test(html), 'the enable notice and the plan state the planned run');
    const ap = { money: v => 'CAD ' + v, DASH: {} }; vm.createContext(ap); for (const n of ['apCurrency', 'apMoney', 'apBidding']) vm.runInContext(pick(n), ap);
    check(ap.apBidding({ maximizeConversionValue: {} }, { meta: { biddingMode: 'MAXIMIZE_CONVERSION_VALUE_LEARNING', targetRoas: 0, targetRoasLater: 2.5 } }, {}) === 'Maximize conversion value · no target ROAS until it has about 6 weeks and 30 conversions in 30 days'
      && /\["Schedule",esc\(m\.runDays\?m\.runDays\+" days from the day you enable it":apSchedule\(psd,ped\)\)\]/.test(html), 'the PMax approval card says no target ROAS applies at launch and counts the run from enabling');
  }

  // ── 11. New Search campaigns launch without a target ROAS; the account target is kept for later ──
  const K4 = ['bunny necklace', 'bunny charm necklace', 'easter bunny necklace', 'personalized bunny necklace'].map(t => ({ text: t, real: true, measured: true, searches: 90 }));
  const COPY = { headlines: ['Bunny charm necklace', 'Handmade bunny gifts', 'Personalized charms'], descriptions: ['Handmade bunny charm necklaces.', 'Made to order.'] };
  const sCtrl = { maxDailyBudgetTotal: 100, budgetCurrency: 'CAD', defaultCountries: ['2124'], targetRoas: 3, smartBidding: true };
  const searchFakes = (e, over) => e.bind({ _enabledBudgetTotal: async () => 0, collectionMeta: async h => ({ handle: h, title: 'Bunny Charms' }), collectionProfiles: async () => ({ list: [] }), getCollections: async () => [],
    researchOpportunity: async () => ({ ok: false }), storeSignals: async () => { throw Error('offline'); }, accountCvr: async () => ({ cvr: 0.02, source: 'benchmark' }), searchClickShare: async () => null, _fxRateToUsd: async () => null,
    groundKeywordPlan: () => ({ ok: true, keywords: clone(K4), groups: [{ label: 'Bunny', keywords: clone(K4) }], confidence: 90, evidence: { accepted: 4, rejected: 0 }, rejected: [] }),
    _bestSearchLandingUrl: () => 'https://britesjewelry.com/collections/bunny', accountWasteNegatives: async () => [], recordOccasionUse: async () => {}, ...over });
  {
    const e = engine({ GADS_TARGET_ROAS: '3' }), approvals = [], built = [], realBuild = e.get('buildSearchCampaignOps');
    searchFakes(e, { fb: () => null, generateRSAAssets: async () => clone(COPY), enqueueApproval: async a => { approvals.push(clone(a)); return 'd' + approvals.length; },
      buildSearchCampaignOps: (...a) => { built.push(clone(a[3])); return realBuild(...a); } });
    const r = plain(await e.get('generateForCollection')('bunny', 'Evergreen gifting', 12, { ctrl: sCtrl, maxCpc: 1.25, countries: ['2124'] }));
    const camp = approvals[0].payload.mutateOperations.find(o => o.campaignOperation).campaignOperation.create, m = approvals[0].payload.meta;
    check(r.ok && built[0].targetRoas === 0 && JSON.stringify(camp.maximizeConversionValue) === '{}', 'a new Search campaign launches on Maximize conversion value without the account target ROAS');
    check(m.targetRoasLater === 3 && m.biddingMode === 'MAXIMIZE_CONVERSION_VALUE_LEARNING' && /no target ROAS until it has about 6 weeks and 30 conversions in 30 days/.test(approvals[0].summary), 'the account target is kept for later (meta.targetRoasLater) and the draft says which bidding applies');
    const ops = realBuild({ handle: 'bunny', title: 'Bunny' }, null, COPY, { dailyBudget: 5, maxCpc: 1, withAssets: false, smartBidding: true,
      adGroups: [{ name: 'Bunny', finalUrl: 'https://britesjewelry.com/collections/bunny', keywords: K4.map(k => k.text), assets: COPY }] }).ops;
    check(JSON.stringify(ops.find(o => o.campaignOperation).campaignOperation.create.maximizeConversionValue) === '{}', 'GADS_TARGET_ROAS is no longer applied to a new Search campaign');
  }

  // ── 12. The daily events run does not pay again for copy that failed brand safety ─────────────
  {
    const e = engine(), f = memory(), approvals = []; let copies = 0, reply = null;
    searchFakes(e, { fb: () => f, generateRSAAssets: async () => { copies++; return clone(reply); }, enqueueApproval: async a => { approvals.push(clone(a)); return 'd' + approvals.length; } });
    const run = auto => e.get('generateForCollection')('bunny', 'Easter', 12, { ctrl: sCtrl, peakDate: plus(today, 30), countries: ['2124'], ...(auto ? { auto: true } : {}) });
    const mark = () => (f.docs.get(COL.state + '/eventsCopyRejected') || {})['bunny-easter'];
    let r = plain(await run(true));
    check(!r.ok && copies === 1 && mark() && mark().date === today && mark().occasion === 'Easter', 'copy that fails brand safety marks the occasion');
    r = plain(await run(true));
    check(!r.ok && r.skipped && copies === 1 && /build it by hand/.test(r.reason), 'the next daily run skips that occasion without paying for copy again');
    r = plain(await run(false));
    check(!r.ok && copies === 2 && mark(), 'a build by hand retries it; copy that fails again keeps the mark');
    reply = COPY; r = plain(await run(false));
    check(r.ok && copies > 2 && !mark() && approvals.length === 1, 'a build by hand whose copy passes clears the mark');
    const before = copies; r = plain(await run(true));
    check(r.ok && copies > before, 'after that the daily run builds that occasion again');
    reply = null; await run(true); const held = copies; r = plain(await run(true));
    check(!r.ok && r.skipped && copies === held && mark().peak === plus(today, 30), 'the mark holds for that occasion date');
    const season = copies; r = plain(await e.get('generateForCollection')('bunny', 'Easter', 12, { ctrl: sCtrl, peakDate: plus(today, 60), countries: ['2124'], auto: true }));
    check(!r.skipped && copies > season, 'the next date of the same occasion is drafted again by the daily run');
  }

  // ── 13. The launch target ROAS is offered later by Ad Doctor, as a reviewed draft only ─────────
  {
    // Publishing keeps it for each new campaign that launched without one.
    const e = engine(), f = memory(), camp = { resourceName: 'customers/123/campaigns/-2', name: 'BA · later', status: 'PAUSED', campaignBudget: 'customers/123/campaignBudgets/-1', maximizeConversionValue: {} };
    const draft = (id, c) => f.db.collection(COL.approvals).doc(id).set({ type: 'pmax', status: 'APPROVED', payload: { mutateOperations: [
      { campaignBudgetOperation: { create: { resourceName: 'customers/123/campaignBudgets/-1', amountMicros: 10e6 } } }, { campaignOperation: { create: c } }], meta: { targetRoasLater: 2.5 } } });
    let cid = '901';
    e.bind({ fb: () => f, assertCreativeReviewed: () => {}, materializeReviewedCreative: async it => clone(it.payload.mutateOperations), _deletedCampaignIds: async () => new Set(), gaql: async () => [], _enabledBudgetTotal: async () => 0,
      mutateAll: async (ops, o) => o.validateOnly ? {} : { mutateOperationResponses: ops.map(x => x.campaignOperation ? { campaignResult: { resourceName: 'customers/123/campaigns/' + cid } } : {}) } });
    await draft('t1', { ...camp }); await e.get('applyApproval')('t1', { maxDailyBudgetTotal: 100, budgetCurrency: 'CAD' });
    const kept = () => f.docs.get(COL.state + '/targetRoasLater') || {};
    check(kept()['901'] && kept()['901'].targetRoas === 2.5 && kept()['901'].approvalId === 't1' && kept()['901'].publishedAt > 0, 'publishing keeps the launch target ROAS for later, per campaign');
    cid = '902'; await draft('t2', { ...camp, name: 'BA · dry' }); await e.get('applyApproval')('t2', { maxDailyBudgetTotal: 100, budgetCurrency: 'CAD', dryRun: true });
    cid = '903'; await draft('t3', { ...camp, name: 'BA · set', maximizeConversionValue: { targetRoas: 3 } }); await e.get('applyApproval')('t3', { maxDailyBudgetTotal: 100, budgetCurrency: 'CAD' });
    check(!kept()['902'] && !kept()['903'], 'a dry run, or a campaign that launched with a target, keeps nothing');
  }
  {
    // The diagnosis reads readiness: about 42 days of delivery, 30 conversions in 30 days, still no target.
    const e = engine(), f = memory(), queries = []; let bidding = { biddingStrategyType: 'MAXIMIZE_CONVERSION_VALUE', maximizeConversionValue: {} }, firstDay = plus(today, -50), conv = 34;
    await f.db.collection(COL.state).doc('targetRoasLater').set({ '42': { targetRoas: 2.5, approvalId: 't1', publishedAt: Date.parse(plus(today, -60) + 'T12:00:00Z') } });
    e.bind({ fb: () => f, gaql: async q => { queries.push(q);
      if (/campaign\.primary_status_reasons/.test(q)) return [{ campaign: { id: '42', name: 'Rings', status: 'ENABLED', primaryStatus: 'ELIGIBLE', primaryStatusReasons: [], advertisingChannelType: 'PERFORMANCE_MAX' }, campaignBudget: { amountMicros: '10000000' }, metrics: { conversions: conv, conversionsValue: 1054, costMicros: 340e6 } }];
      if (/campaign\.bidding_strategy_type/.test(q)) return [{ campaign: { id: '42', ...bidding } }];
      if (/metrics\.impressions > 0/.test(q)) return [{ campaign: { id: '42' }, segments: { date: firstDay }, metrics: { impressions: '40' } }];
      if (/category_label, metrics\.conversions FROM campaign_search_term_insight/.test(q)) return [{ campaignSearchTermInsight: { categoryLabel: 'free charm patterns' }, metrics: { conversions: 0 } }, { campaignSearchTermInsight: { categoryLabel: 'charm bracelets' }, metrics: { conversions: 3 } }];
      return []; } });
    const ready = async () => (plain(await e.get('fetchDiagnostics')(null)).campaigns[0] || {}).targetRoasReady;
    const t = await ready();
    check(t && t.targetRoas === 2.5 && t.conversions30 === 34 && t.roas30 === 3.1 && t.servingDays === 50 && t.servingSince === firstDay, 'a campaign serving about 42 days with 30 conversions in 30 days is ready for the target kept at launch');
    check(queries.some(q => /metrics\.impressions > 0/.test(q) && q.includes(`BETWEEN '${plus(today, -60)}' AND '${today}'`)), 'its serving days count from its first impression since publication');
    conv = 29; const few = await ready(); conv = 34; firstDay = plus(today, -30); const young = await ready(); firstDay = plus(today, -50);
    bidding = { biddingStrategyType: 'MAXIMIZE_CONVERSION_VALUE', maximizeConversionValue: { targetRoas: 3 } }; const set = await ready();
    bidding = { biddingStrategyType: 'MANUAL_CPC' }; const manual = await ready();
    check(!few && !young && !set && !manual, 'not yet with fewer conversions or fewer days, and never over an existing target or another bidding strategy');
    bidding = { biddingStrategyType: 'MAXIMIZE_CONVERSION_VALUE', maximizeConversionValue: {} };
    const d = plain(await e.get('fetchDiagnostics')(null)), cq = queries.filter(q => /category_label, metrics\.conversions FROM campaign_search_term_insight/.test(q)).pop();
    check(JSON.stringify(d.negativeGuard['42'].categories) === JSON.stringify([{ label: 'free charm patterns', conv: 0 }, { label: 'charm bracelets', conv: 3 }]) && cq.includes(`BETWEEN '${plus(today, -364)}' AND '${today}'`) && /campaign_id = 42/.test(cq),
      'a PMax campaign\'s search categories and their conversions over a year are read for the negative check');
  }
  {
    // Ad Doctor's own remedy, even when the AI review fails; the model cannot set a target itself.
    const e = engine(), f = memory(); let verdict = null;
    const c = { id: '42', name: 'Rings', status: 'ENABLED', channel: 'PERFORMANCE_MAX', targetRoasReady: { targetRoas: 2.5, conversions30: 34, roas30: 3.1, servingDays: 50, servingSince: plus(today, -50) } };
    e.bind({ fb: () => f, control: async () => ({ maxDailyBudgetTotal: 100, budgetCurrency: 'CAD' }), fetchDiagnostics: async () => ({ campaigns: [clone(c)], account: { recommendations: [] } }),
      remedyHistory: async () => ({ items: [] }), _remedyOutcomes: async () => ({}), _enabledBudgetTotal: async () => 10,
      analyzeDiagnostics: async () => { if (!verdict) throw Error('AI offline'); return clone(verdict); } });
    let doc = plain(await e.get('runDiagnostics')({}));
    const rem = (((doc.ai.campaigns.find(v => v.id === '42') || {}).remedies) || []).find(r => r.executable.kind === 'setTargetRoas');
    check(rem && rem.executable.targetRoas === 2.5 && rem.issue === 'Ready for its target ROAS of 250%' && /Serving 50 days; last 30 days: 34 conversions, ROAS 310%/.test(rem.fix) && doc.aiError, 'Ad Doctor offers "Set target ROAS 250%" by its own rule, even when the AI review fails');
    verdict = { campaigns: [{ id: '42', severity: 'healthy', remedies: [{ issue: 'x', fix: 'y', executable: { kind: 'setTargetRoas', targetRoas: 9 } }] }] };
    doc = plain(await e.get('runDiagnostics')({}));
    const rs = doc.ai.campaigns.find(v => v.id === '42').remedies.filter(r => r.executable.kind === 'setTargetRoas');
    check(rs.length === 1 && rs[0].executable.targetRoas === 2.5, 'the model cannot set a target: only the one kept at launch is offered');
  }
  {
    // Sending it: a draft in Approvals, never a change in Google Ads.
    const e = engine(), f = memory(); let bidding = { biddingStrategyType: 'MAXIMIZE_CONVERSION_VALUE', maximizeConversionValue: {} };
    const ready = { targetRoas: 2.5, conversions30: 34, roas30: 3.1, servingDays: 50, servingSince: plus(today, -50) }, remedy = plain(e.get('_targetRoasRemedy')(ready));
    e.bind({ fb: () => f, _currentDiagnosis: async () => ({ id: '42', name: 'Rings', targetRoasReady: ready }), _campaignBaseline: async () => ({ name: 'Rings', conv: 60 }),
      gaql: async q => /bidding_strategy_type/.test(q) ? [{ campaign: { id: '42', ...bidding } }] : [] });
    const r = plain(await e.get('applyRemedy')('42', remedy, { ctrl: { maxDailyBudgetTotal: 100 } }));
    const drafts = () => [...f.docs].filter(([k]) => k.startsWith(COL.approvals + '/')).map(([, v]) => v), a = drafts()[0];
    check(r.ok && r.queued && drafts().length === 1 && a.status === 'PENDING' && a.type === 'bidding' && a.payload.service === 'campaigns' && a.payload.meta.targetRoasGuard === true, 'Set target ROAS goes to Approvals as a draft; nothing is sent to Google Ads');
    check(JSON.stringify(a.payload.operations) === JSON.stringify([{ update: { resourceName: 'customers/123/campaigns/42', maximizeConversionValue: { targetRoas: 2.5 } }, updateMask: 'maximize_conversion_value.target_roas' }]) && a.payload.meta.evidence.conversions30 === 34 && a.summary === 'Target ROAS 250% · Rings',
      'the draft holds exactly one bidding change, with its evidence');
    const log = [...f.docs].filter(([k]) => k.startsWith(COL.remedies + '/')).map(([, v]) => v);
    check(log.length === 1 && log[0].queued && log[0].approvalId === r.approvalId && log[0].kind === 'setTargetRoas', 'it is logged as waiting in Approvals, not as a live change');
    const again = plain(await e.get('applyRemedy')('42', remedy, { ctrl: {} }));
    check(again.reused && again.approvalId === r.approvalId && drafts().length === 1, 'a second click returns the draft already waiting');
    bidding = { biddingStrategyType: 'MAXIMIZE_CONVERSION_VALUE', maximizeConversionValue: { targetRoas: 3 } };
    await assert.rejects(() => e.get('applyRemedy')('43', remedy, { ctrl: {} }), /bidding changed since it was diagnosed/);
    check(drafts().length === 1, 'a campaign whose bidding changed gets no draft');
  }
  {
    // Approving it: only that change, only while the campaign still has no target.
    const e = engine(), f = memory(), sent = []; let bidding = { biddingStrategyType: 'MAXIMIZE_CONVERSION_VALUE', maximizeConversionValue: {} };
    const op = { update: { resourceName: 'customers/123/campaigns/42', maximizeConversionValue: { targetRoas: 2.5 } }, updateMask: 'maximize_conversion_value.target_roas' };
    const draft = (id, ops) => f.db.collection(COL.approvals).doc(id).set({ type: 'bidding', status: 'APPROVED', payload: { service: 'campaigns', operations: ops, meta: { existingCampaignId: '42', targetRoas: 2.5, targetRoasGuard: true } } });
    e.bind({ fb: () => f, _deletedCampaignIds: async () => new Set(), gaql: async q => /bidding_strategy_type/.test(q) ? [{ campaign: { id: '42', ...bidding } }] : [],
      mutate: async (service, ops, o) => { sent.push([service, clone(ops), !!(o && o.validateOnly)]); return {}; } });
    const ctrl = { maxDailyBudgetTotal: 100, budgetCurrency: 'CAD' };
    await draft('b1', [op]); await e.get('applyApproval')('b1', ctrl);
    check(sent.length === 1 && sent[0][0] === 'campaigns' && JSON.stringify(sent[0][1]) === JSON.stringify([op]) && f.docs.get(COL.approvals + '/b1').status === 'APPLIED', 'approving the draft sets the target ROAS, and only that');
    await draft('b2', [{ update: { ...op.update, name: 'Renamed' }, updateMask: op.updateMask }]);
    await assert.rejects(() => e.get('applyApproval')('b2', ctrl), /unexpected change/);
    bidding = { biddingStrategyType: 'MAXIMIZE_CONVERSION_VALUE', maximizeConversionValue: { targetRoas: 3 } }; await draft('b3', [op]);
    await assert.rejects(() => e.get('applyApproval')('b3', ctrl), /bidding changed after this draft was made/);
    check(sent.length === 1 && f.docs.get(COL.approvals + '/b3').status === 'APPROVED', 'an unexpected operation or a bidding changed since publishes nothing');
  }

  // ── 14. A campaign that already served keeps its end date at the console's first enable ─────────
  {
    const e = engine(), f = memory(), muts = [], queries = []; let fail = true;
    await f.db.collection(COL.state).doc('plannedFlight_779').set({ days: 30, approvalId: 'p9', publishedAt: Date.parse(plus(today, -20) + 'T12:00:00Z') });
    e.bind({ fb: () => f, _assertCampaignNotDeleted: async () => {}, _assertSpendLimits: async () => 'x',
      gaql: async q => { queries.push(q); if (/metrics\.impressions FROM campaign WHERE campaign\.id = 779/.test(q)) { if (fail) throw Error('read failed'); return [{ campaign: { id: '779' }, metrics: { impressions: '120' } }]; } return []; },
      mutate: async (service, ops) => { muts.push(clone(ops)); return {}; } });
    let r = plain(await e.get('setCampaignStatus')('779', 'ENABLED', { ctrl: { maxDailyBudgetTotal: 100 } }));
    check(muts[0][0].updateMask === 'status' && !r.endDate && !f.docs.get(COL.state + '/plannedFlight_779').startedAt, 'when its delivery cannot be read, the end date stays and the run is not marked started');
    fail = false; r = plain(await e.get('setCampaignStatus')('779', 'ENABLED', { ctrl: { maxDailyBudgetTotal: 100 } }));
    const fl = f.docs.get(COL.state + '/plannedFlight_779');
    check(muts[1][0].updateMask === 'status' && !r.endDate && r.alreadyServed && fl.startedAt > 0 && fl.servedBeforeEnable, 'a campaign that already served since publication keeps its end date and is marked started');
    check(queries.some(q => q.includes(`BETWEEN '${plus(today, -20)}' AND '${today}'`)), 'its delivery is read from the publish day');
  }

  // ── 15. PMax negatives only inside a search category that never converted ──────────────────────
  {
    const san = engine().get('_diagSanitize'), neg = keywords => ({ campaigns: [{ id: '50', remedies: [{ issue: 'Waste', fix: 'Exclude', executable: { kind: 'addNegatives', keywords } }] }] });
    const diag = cats => ({ campaigns: [{ id: '50', channel: 'PERFORMANCE_MAX', status: 'ENABLED' }], negativeGuard: { '50': { converting: [], keywords: [], ...(cats ? { categories: cats } : {}) } } });
    const cats = [{ label: 'free charm patterns', conv: 0 }, { label: 'charm bracelets', conv: 3 }];
    let ex = plain(san(neg(['free charm patterns', 'patterns', 'silver charm bracelets', 'wholesale']), diag(cats), { maxDailyBudgetTotal: 100 }, 10)).campaigns[0].remedies[0].executable;
    check(ex.kind === 'addNegatives' && JSON.stringify(ex.keywords) === '["free charm patterns","patterns"]', 'a PMax negative is offered only inside a search category that never converted');
    check(JSON.stringify(ex.skipped) === '["silver charm bracelets","wholesale"]' && /never converted/.test(ex.skippedWhy), 'one near a converting category, or in no reported category, is left out with the reason');
    ex = plain(san(neg(['free charm patterns']), diag(null), {}, 10)).campaigns[0].remedies[0].executable;
    check(ex.kind === 'none', 'without the category read no PMax negative is offered');
  }

  // ── Console: the Studio AI read, the target ROAS remedy and its Approvals card ─────────────────
  {
    const { JSDOM } = require('jsdom'), dom = new JSDOM('<div id="studioGrowth"></div>'), d = dom.window.document;
    const ui = { document: d, $: s => d.querySelector(s), DASH: { control: { maxDailyBudgetTotal: 100 } }, cur: () => 'CAD', defCountries: () => ['2124'], ctyName: x => x, moneyIn: v => 'CAD ' + v,
      studioGrowthBusy: null, studioGrowthCountries: null, studioGrowthDraft: null, studioGrowthRun: () => {}, go: () => {}, openCountryEditor: () => {},
      STUDIO_GROWTH: { blueprint: { budget: { recommendedDaily: 10, countries: ['2124'] }, pmax: { groups: [] }, positioning: {} }, lanes: {}, learning: { phase: 'learning', generatedAt: Date.now() - 2 * 3600e3, recommendations: [],
        synthesis: { summary: 'Designers stall before approval.', nextTest: { lane: 'landing', hypothesis: 'h', change: 'Show the metal preview first', successMetric: 'approval rate at least 25%', holdDays: 14 }, warnings: [] } } } };
    vm.createContext(ui); for (const n of ['esc', 'timeago', 'sgMetric', 'sgLaneState', 'sgCountryText', 'renderDesignStudioGrowth']) vm.runInContext(pick(n), ui);
    ui.renderDesignStudioGrowth();
    const read = [...d.querySelectorAll('#studioGrowth .sgSection')].find(x => /AI read/.test(x.textContent));
    check(read && !read.closest('details') && /Designers stall before approval\./.test(read.textContent) && /Next test · landing: Show the metal preview first · success: approval rate at least 25% · hold 14 days/.test(read.textContent) && /2h ago/.test(read.textContent),
      'the Studio panel shows the AI read and next test that Analyze paid for, without opening anything');
    ui.STUDIO_GROWTH.learning.synthesis.at = Date.now() - 3 * DAY; ui.renderDesignStudioGrowth();
    check(/AI read · 3d ago/.test(d.querySelector('#studioGrowth').textContent), 'its age is the time of the AI read, not of a later scheduled refresh');
    ui.STUDIO_GROWTH.learning.synthesis = { error: 'model offline' }; ui.renderDesignStudioGrowth();
    check(![...d.querySelectorAll('#studioGrowth .sgLabel')].some(x => /AI read/.test(x.textContent)), 'no AI read is shown when there is none');

    const ad = { REMHIST: [{ id: 'h1', campaignId: '42', kind: 'setTargetRoas', issue: 'Ready for its target ROAS of 250%', executable: { kind: 'setTargetRoas', targetRoas: 2.5 }, queued: true, approvalState: 'PENDING' }], diagMoney: v => 'CAD ' + v, apCampaignName: () => 'Rings' };
    vm.createContext(ad); for (const n of ['remKey', 'histFor', 'queuedTag', 'remedyParams', 'approvalAction']) vm.runInContext(pick(n), ad);
    const rm = { issue: 'Ready for its target ROAS of 300%', executable: { kind: 'setTargetRoas', targetRoas: 3 } };
    check(ad.histFor('42', rm) && !ad.histFor('43', rm), 'a target ROAS already waiting in Approvals is not offered again');
    check(/live bidding changes only after you approve/.test(ad.queuedTag(ad.REMHIST[0])[2]) && ad.remedyParams(ad.REMHIST[0]) === 'Target ROAS 250%', 'the fix history names the bidding change');
    const act = ad.approvalAction({ type: 'bidding', payload: { service: 'campaigns', operations: [{}], meta: { existingCampaignId: '42', targetRoas: 2.5 } } }, false);
    check(act.publish === 'Set target ROAS' && act.from === 'Ad Doctor' && /else if\(pl\.service==="campaigns"&&\(pl\.meta\|\|\{\}\)\.targetRoas\)\{var tm=pl\.meta/.test(html) && /ex\.kind==="setTargetRoas"\?\("Send a target ROAS of "/.test(html),
      'the Approvals card names the change it publishes, and Ad Doctor asks before sending it');
  }

  console.log(passed + ' spend and paid-AI guard checks passed.');
})().catch(e => { console.error(e); process.exit(1); });
