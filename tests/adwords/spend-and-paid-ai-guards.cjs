// Budget guards and paid-AI triggers. Every budget refusal gives its figures in the account currency;
// the Controls ceiling cannot pass the site limit; a planned run length starts when the campaign is
// enabled; new PMax campaigns launch without a target ROAS; a proposed negative is checked against
// every converting search term; and no paid AI request starts without an explicit request (opening a
// view, a reload or a scheduled run reads saved results). Synthetic data only; every live call fails.
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
    get: async () => { const list = [...docs].filter(([k, v]) => k.startsWith(p + '/') && !k.slice(p.length + 1).includes('/') && fs.every(([fk, fv]) => v[fk] === fv)).map(([k, v]) => ({ id: k.split('/').pop(), exists: true, data: () => clone(v), ref: doc(k) }));
      return { docs: list, empty: !list.length, size: list.length, forEach: fn => list.forEach(fn) }; } });
  return { docs, db: { collection: c => col(c), runTransaction: async fn => fn({ get: r => r.get(), set: (r, v, o) => r.set(v, o), update: (r, v) => r.update(v), delete: r => docs.delete(r.path) }),
    batch: () => { const ops = []; return { set: (r, v, o) => ops.push(() => r.set(v, o)), commit: async () => { for (const op of ops) await op(); } }; } }, FV: { serverTimestamp: () => Date.now() } };
}
// Router and worker with a fake engine; dispatches to the worker are recorded, never sent.
function load(name, env, E, calls) {
  const mod = { exports: {} }, saved = [], admin = { firestore: () => ({ collection: c => ({ doc: d => ({ get: async () => ({ exists: false }), set: async v => { saved.push([c + '/' + d, v]); } }) }) }) };
  admin.firestore.FieldValue = { serverTimestamp: () => Date.now() };
  const ctx = { process: { env: { URL: 'https://example.invalid', ...env } }, console, Date, Set, Map, JSON, module: mod, exports: mod.exports,
    require: n => n === 'node-fetch' ? async (url, opts) => { calls.push(['dispatch', JSON.parse(opts.body)]); return { ok: true, status: 202 }; } : n === './googleAdsAutopilot' ? E : n === './firebaseAdmin' ? admin : realRequire(n) };
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
    const e2 = engine({ OPENAI_API_KEY: 'test' }), f2 = memory(); let studioPaid = 0;
    await f2.db.collection(COL.state).doc('designStudioAcquisition').set({ learning: { synthesis: { summary: 'kept' } } });
    e2.bind({ fb: () => f2, control: async () => ({ targetRoas: 0 }), openaiJSON: async () => { studioPaid++; return { summary: 'fresh' }; },
      designStudioPerformance: async () => ({ readiness: { apiOk: true, purchaseReady: true, assistCoverage: 4 }, overall: { clicks: 80, cost: 100, impressions: 2000, ctr: 0.04 },
        purchase: { conversions: 4, value: 400, cpa: 25, roas: 4 }, funnel: { start: 0, design: 0, approve: 0 }, rates: {}, campaigns: [{ id: '1' }], assetGroups: [], searchInsights: [], start: '2026-09-01', end: '2026-09-28', days: 28 }) });
    let out = plain(await e2.get('refreshDesignStudioLearning')({ withAI: false }));
    check(studioPaid === 0 && out.synthesis.summary === 'kept', 'the scheduled Studio refresh makes no AI request and keeps the last synthesis');
    out = plain(await e2.get('refreshDesignStudioLearning')({}));
    check(studioPaid === 1 && out.synthesis.summary === 'fresh', 'the explicit Studio Analyze still asks the AI');

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
  }

  console.log(passed + ' spend and paid-AI guard checks passed.');
})().catch(e => { console.error(e); process.exit(1); });
