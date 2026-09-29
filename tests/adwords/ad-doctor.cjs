// Ad Doctor: what a diagnosis may offer, what each fix really does, how applied fixes
// are measured, and what the card shows. Synthetic data only; any live call fails.
const fs = require('fs'), vm = require('vm'), assert = require('assert/strict'), path = require('path'), Module = require('module');
const repo = path.resolve(__dirname, '../..'), file = path.join(repo, 'netlify/functions/googleAdsAutopilot.js'), realRequire = Module.createRequire(file);
const clone = x => x == null ? x : JSON.parse(JSON.stringify(x)), plain = x => JSON.parse(JSON.stringify(x));
let passed = 0; const check = (v, msg) => { assert(v, msg); passed++; console.log('PASS', msg); };
const DAY = 86400000;

function engine() {
  const cx = vm.createContext({ module: { exports: {} }, exports: {}, require: n => n === 'node-fetch' ? (...a) => cx.__fetch(...a) : realRequire(n),
    process: { env: { GADS_CUSTOMER_ID: '123' } }, console, Buffer, Date, Intl, Map, Set, URL, setTimeout, clearTimeout });
  cx.__fetch = async () => { throw Error('Live network forbidden'); };
  vm.runInContext(fs.readFileSync(file, 'utf8'), cx);
  const e = { cx, get: n => vm.runInContext(n, cx), bind(v) { cx.__m = v; vm.runInContext(Object.keys(v).map(k => k + '=__m.' + k).join('\n'), cx); } };
  e.bind({ gaql: async () => { throw Error('Live Google Ads read forbidden'); }, mintToken: async () => { throw Error('Live token forbidden'); },
    openaiJSON: async () => { throw Error('Paid AI call forbidden'); }, _accountTz: async () => 'UTC', _accountCurrency: async () => 'CAD' });
  return e;
}
function memory() {
  const cols = new Map(), col = n => { if (!cols.has(n)) cols.set(n, new Map()); return cols.get(n); }; let seq = 0;
  const ref = (n, id) => ({ id, get: async () => ({ id, exists: col(n).has(id), data: () => clone(col(n).get(id)) }),
    set: async (v, o) => { col(n).set(id, o && o.merge ? { ...(col(n).get(id) || {}), ...clone(v) } : clone(v)); },
    update: async v => { col(n).set(id, { ...(col(n).get(id) || {}), ...clone(v) }); } });
  const query = (n, f = [], lim = 1e9, ord = null) => ({ where: (k, op, v) => query(n, f.concat([[k, op, v]]), lim, ord), orderBy: (k, d) => query(n, f, lim, [k, d]), limit: x => query(n, f, x, ord),
    get: async () => {
      let rows = [...col(n)].filter(([, v]) => f.every(([k, op, x]) => { const got = k.split('.').reduce((o, p) => o == null ? o : o[p], v); return op === 'in' ? x.includes(got) : got === x; }));
      if (ord) rows.sort((a, b) => (ord[1] === 'desc' ? -1 : 1) * ((a[1][ord[0]] || 0) - (b[1][ord[0]] || 0)));
      const docs = rows.slice(0, lim).map(([id, v]) => ({ id, exists: true, data: () => clone(v), ref: ref(n, id) }));
      return { docs, size: docs.length, empty: !docs.length, forEach: fn => docs.forEach(fn) };
    } });
  const db = { collection: n => Object.assign(query(n), { doc: id => ref(n, String(id)), add: async v => { const id = 'r' + (++seq); col(n).set(id, clone(v)); return { id }; } }),
    runTransaction: async fn => fn({ get: r => r.get(), set: (r, v, o) => r.set(v, o), update: (r, v) => r.update(v) }),
    batch: () => { const ops = []; return { set: (r, v, o) => ops.push(() => r.set(v, o)), commit: async () => { for (const op of ops) await op(); } }; } };
  return { db, FV: { serverTimestamp: () => Date.now() }, col };
}
const COLS = { state: 'Brites_GAds_State', remedies: 'Brites_GAds_Remedies', approvals: 'Brites_GAds_Approvals' };

(async () => {
  // ── Evidence the diagnosis is allowed to act on ──────────────────────────
  {
    const e = engine(), txt = e.get('_diagReasonsText');
    check(JSON.stringify(txt(['BUDGET_CONSTRAINED', 'UNKNOWN', 'CAMPAIGN_PAUSED', 'UNSPECIFIED'])) === '["Limited by budget","Paused"]', 'status reasons use the v24 names and never show UNKNOWN');
    e.bind({ gaql: async q => {
      if (/campaign\.primary_status_reasons/.test(q)) return [{ campaign: { id: '42', name: 'PMax', status: 'ENABLED', primaryStatus: 'LIMITED', primaryStatusReasons: ['BUDGET_CONSTRAINED', 'UNKNOWN'], advertisingChannelType: 'PERFORMANCE_MAX' }, campaignBudget: { amountMicros: '10000000' }, metrics: {} }];
      if (/policy_summary\.approval_status/.test(q)) return [['APPROVED_LIMITED', 'REVIEWED'], ['DISAPPROVED', 'REVIEWED'], ['APPROVED', 'REVIEW_IN_PROGRESS']].map(([a, r], i) => ({ campaign: { id: '42' }, adGroupAd: { ad: { id: String(i) }, adStrength: 'GOOD', policySummary: { approvalStatus: a, reviewStatus: r } } }));
      if (/segments\.ad_network_type/.test(q)) { assert.match(q, /metrics\.conversions_value/); return [{ campaign: { id: '42' }, segments: { adNetworkType: 'YOUTUBE' }, metrics: { clicks: '5', costMicros: '2500000', conversions: 1, conversionsValue: 40 } }]; }
      return [];
    } });
    const pulled = plain(await e.get('fetchDiagnostics')(null)).campaigns[0];
    check(pulled.reasonsText.join() === 'Limited by budget' && pulled.limitedAds === 1 && pulled.disapprovedAds === 1 && pulled.underReviewAds === 1, 'policy-limited, disapproved and in-review ads are counted separately');
    check(pulled.channelBreakdown[0].label === 'YouTube' && pulled.channelBreakdown[0].value === 40, 'PMax channel rows name YouTube and carry conversion value');
    const diag = { campaigns: [{ id: '42', status: 'ENABLED', budget: 10,
      keywordDetail: [{ adGroupId: '7', criterionId: '99', text: 'charm necklace', match: 'PHRASE' }],
      searchTerms: [{ term: 'personalized charm necklace', conv: 2 }, { term: 'free jewelry', conv: 0 }], adsContent: [{ adId: '555' }] }] };
    const ai = { campaigns: [{ id: '999', severity: 'bogus', verdict: 'maybe', googleSays: 'none', headline: 'h', aiSays: 'a', findings: ['1', '2', '3', '4', '5', '6'],
      fixReview: [{ working: 'working' }], action: { kind: 'raiseBudget', budget: 25, urgency: 'now' }, remedies: [
        { issue: 'same budget', fix: 'x', impact: 'high', executable: { kind: 'setBudget', budget: 10 } },
        { issue: 'over ceiling', fix: 'x', impact: 'high', executable: { kind: 'setBudget', budget: 40 } },
        { issue: 'valid budget', fix: 'x', impact: 'high', executable: { kind: 'setBudget', budget: 12 } },
        { issue: 'unknown keyword', fix: 'x', executable: { kind: 'pauseKeywords', keywords: [{ adGroupId: '7', criterionId: '1' }] } },
        { issue: 'known keyword', fix: 'x', executable: { kind: 'pauseKeywords', keywords: [{ adGroupId: '7', criterionId: '99', text: 'model text' }] } },
        { issue: 'negatives', fix: 'x', executable: { kind: 'addNegatives', keywords: ['free', 'charm necklace', 'Personalized'] } },
        { issue: 'existing keyword', fix: 'x', executable: { kind: 'addKeywords', adGroupId: '7', keywords: [{ text: 'charm necklace', matchType: 'PHRASE' }, { text: 'mom gift', matchType: 'EXACT' }] } },
        { issue: 'invented kind', fix: 'x', executable: { kind: 'deleteCampaign' } }] }] };
    const sanitize = (x, total) => plain(e.get('_diagSanitize')(x, diag, { maxDailyBudgetTotal: 30 }, total)).campaigns[0];
    const v = sanitize(ai, 20), ex = v.remedies.map(r => r.executable);
    const other = sanitize({ campaigns: [{ id: '42', remedies: [{ issue: 'foreign ad group', executable: { kind: 'addKeywords', adGroupId: '8', keywords: ['gift'] } },
      { issue: 'foreign ad', executable: { kind: 'rewriteAds', adId: '1', headlines: ['New'] } }] }] }, 20).remedies.map(r => r.executable);
    check(v.id === '42', 'a one-campaign verdict is filed under that campaign');
    check(v.severity === 'attention' && v.verdict === null && v.googleSays === '', 'unknown severity, verdict and a "none" Google note are normalized');
    check(v.findings.length === 5 && !('fixReview' in v), 'findings are capped and the model cannot grade applied fixes');
    check(v.action.budget === null, 'an AI budget that would pass the account ceiling gets no button');
    check(ex[0].kind === 'none' && ex[1].kind === 'none', 'budgets equal to the current one or over the ceiling are advice only');
    check(ex[2].kind === 'setBudget' && ex[2].budget === 12, 'a valid budget change keeps its button');
    check(ex[3].kind === 'none' && ex[4].kind === 'pauseKeywords' && ex[4].keywords[0].text === 'charm necklace', 'only evidence keywords can be paused, named as the evidence names them');
    check(ex[5].kind === 'addNegatives' && ex[5].keywords.join() === 'free' && ex[5].skipped.includes('charm necklace') && ex[5].skipped.includes('personalized'), 'negatives that would block converting searches or active keywords are left out');
    check(other[0].kind === 'none' && ex[6].keywords.length === 1 && ex[6].keywords[0].text === 'mom gift', 'keywords go only to evidence ad groups and are not re-added');
    check(other[1].kind === 'none' && ex[7].kind === 'none', 'unknown ads and invented actions are advice only');
  }

  // ── Applied fixes are measured by the console, 14 days either side ──────
  {
    const e = engine(), win = e.get('_learningWindow'), now = Date.now();
    const h = [
      { id: 'a', campaignId: '42', at: now - 40 * DAY, fix: 'improved fix' },
      { id: 'b', campaignId: '43', at: now - 5 * DAY, fix: 'recent fix' },
      { id: 'c', campaignId: '44', at: now - 30 * DAY, fix: 'small fix' },
      { id: 'd', campaignId: '45', at: now - 40 * DAY, fix: 'queued rewrite', queued: true, approvalState: 'PENDING' },
      { id: 'e', campaignId: '46', at: now - 60 * DAY, fix: 'published rewrite', queued: true, approvalState: 'APPLIED', appliedAt: now - 40 * DAY }];
    const row = (cid, date, clicks, conv, cost, value) => ({ campaign: { id: cid }, segments: { date }, metrics: { impressions: String(clicks * 20), clicks: String(clicks), costMicros: String(cost * 1e6), conversions: conv, conversionsValue: value } });
    const wa = win(now - 40 * DAY, 'UTC'), wc = win(now - 30 * DAY, 'UTC');
    const rows = [row('42', wa.beforeStart, 60, 6, 100, 300), row('42', wa.afterStart, 70, 8, 100, 500), row('44', wc.beforeStart, 5, 0, 4, 0), row('44', wc.afterStart, 6, 1, 5, 20),
      row('46', wa.beforeStart, 80, 10, 100, 400), row('46', wa.afterEnd, 80, 10, 100, 400)];
    let reads = 0; e.bind({ gaql: async q => { reads++; assert.match(q, /segments\.date BETWEEN/); return rows; } });
    const out = plain(await e.get('_remedyOutcomes')(h, { currency: 'CAD' }));
    check(reads === 1, 'every due fix is measured from one daily read');
    check(out['42'][0].working === 'working' && /14 days before: 60 clicks/.test(out['42'][0].note) && /CAD/.test(out['42'][0].note), 'a fix with enough data either side is judged, with its numbers and currency');
    check(out['43'][0].working === 'too early' && /Measurable after/.test(out['43'][0].note), 'a recent fix waits for 14 days plus 3 reporting days');
    check(out['44'][0].working === 'not enough data', 'a fix without 50 clicks and 5 conversions per window is not judged');
    check(!out['45'], 'a rewrite still waiting in Approvals is not measured as a live change');
    check(out['46'][0].working === 'no clear change' && out['46'][0].daysAgo === 40, 'a published rewrite is measured from its publish date');
  }

  // ── A one-campaign diagnosis updates only that campaign ─────────────────
  {
    const e = engine(), f = memory();
    await f.db.collection(COLS.state).doc('diagnostics').set({ generatedAt: 1000, campaigns: [{ id: '42', name: 'Camp A' }, { id: '43', name: 'Camp B' }],
      ai: { accountSummary: 'acct', campaigns: [{ id: '42', headline: 'old' }, { id: '43', headline: 'keep' }] },
      aiError: 'partial — Camp A: timeout | Camp B: bad json', fixOutcomes: { '43': [{ historyId: 'x' }] } });
    let analyses = 0;
    e.bind({ fb: () => f, control: async () => ({ maxDailyBudgetTotal: 30, budgetCurrency: 'CAD' }), remedyHistory: async () => ({ items: [] }), _enabledBudgetTotal: async () => 20,
      fetchDiagnostics: async () => ({ campaigns: [{ id: '42', name: 'Camp A', status: 'ENABLED', budget: 10 }], account: { recommendations: [] } }),
      analyzeDiagnostics: async () => { analyses++; if (analyses > 1) throw Error('boom'); return { campaigns: [{ id: '42', severity: 'healthy', headline: 'new', remedies: [] }] }; } });
    let doc = plain(await e.get('runDiagnostics')({ campaignId: '42' }));
    check(doc.generatedAt === 1000 && doc.updatedAt > 1000, 'the full-check time is kept; the campaign carries its own time');
    check(doc.ai.campaigns.find(c => c.id === '42').headline === 'new' && doc.ai.campaigns.find(c => c.id === '43').headline === 'keep', 'only the diagnosed campaign verdict is replaced');
    check(doc.aiError === 'partial — Camp B: bad json', 'a campaign that now succeeds loses its old AI error; others keep theirs');
    check(doc.fixOutcomes['43'].length === 1 && Array.isArray(doc.fixOutcomes['42']), 'fix results merge per campaign');
    doc = plain(await e.get('runDiagnostics')({ campaignId: '42' }));
    check(doc.aiError === 'partial — Camp B: bad json | Camp A: boom', 'a failed one-campaign review is recorded against that campaign only');
  }

  // ── Fixes do exactly what their button says ─────────────────────────────
  {
    const e = engine(), f = memory(); let mutations = 0, approvals = 0, budgets = 0;
    const rsa = [{ adGroup: { id: '7' }, adGroupAd: { ad: { id: '555', finalUrls: ['https://shop.example/p'], responsiveSearchAd: { headlines: [{ text: 'One' }, { text: 'Two' }, { text: 'Three' }], descriptions: [{ text: 'D1' }, { text: 'D2' }] } } } }];
    let gaqlReply = () => [];
    // Only a current check can offer a fix: age, status and the offered fix are checked server-side.
    const gate = e.get('_currentDiagnosis'), stored = (over = {}) => f.db.collection(COLS.state).doc('diagnostics').set({ generatedAt: Date.now(),
      campaigns: [{ id: '42', status: 'ENABLED', recommendations: [{ recommendedBudget: 15 }] }], ai: { campaigns: [{ id: '42', action: { budget: 14 }, remedies: [{ executable: { kind: 'addNegatives', keywords: ['free'] } }] }] }, ...over });
    let liveStatus = 'ENABLED'; e.bind({ fb: () => f, gaql: async q => (assert.match(q, /campaign\.status FROM campaign/), [{ campaign: { status: liveStatus } }]) });
    await stored({ generatedAt: Date.now() - 74 * DAY });
    await assert.rejects(() => gate('42', { kind: 'addNegatives', keywords: ['free'] }), /out of date/); passed++;
    await stored(); liveStatus = 'PAUSED';
    await assert.rejects(() => gate('42', { kind: 'addNegatives', keywords: ['free'] }), /changed since it was diagnosed/); passed++;
    liveStatus = 'ENABLED';
    await assert.rejects(() => gate('42', { kind: 'addNegatives', keywords: ['cheap'] }), /not part of the campaign's current diagnosis/); passed++;
    await assert.rejects(() => gate('42', { kind: 'setBudget', budget: 39 }), /not part/); passed++;
    await gate('42', { kind: 'addNegatives', keywords: ['free'] }); await gate('42', { kind: 'setBudget', budget: 15 }); await gate('42', { kind: 'setBudget', budget: 14 });
    check(true, 'a stale, changed or unoffered fix is refused server-side; an offered fix from a current check passes');
    e.bind({ fb: () => f, _currentDiagnosis: async () => {}, _campaignBaseline: async () => ({ name: 'Camp A' }), mutate: async () => { mutations++; return {}; }, _verifyLedger: async () => {},
      enqueueApproval: async item => { approvals++; const r = await f.db.collection(COLS.approvals).add({ ...item, status: 'PENDING' }); return r.id; },
      setCampaignBudget: async () => { budgets++; return { verified: true }; }, _enabledBudgetTotal: async () => 25, gaql: async q => gaqlReply(q) });
    const apply = (cid, remedy, ctrl = { maxDailyBudgetTotal: 30 }) => e.get('applyRemedy')(cid, remedy, { ctrl });
    await assert.rejects(() => apply('abc', { executable: { kind: 'addNegatives', keywords: ['x'] } }), /valid campaign/); passed++;
    gaqlReply = () => [{ campaignCriterion: { keyword: { text: 'free', matchType: 'PHRASE' } } }];
    let r = plain(await apply('42', { executable: { kind: 'addNegatives', keywords: ['Free'] } }));
    check(r.ok && r.added.length === 0 && r.alreadyLive[0] === 'free' && mutations === 0, 'negatives already live are not sent again');
    gaqlReply = () => [];
    await assert.rejects(() => apply('42', { executable: { kind: 'pauseKeywords', keywords: [{ adGroupId: '7', criterionId: '99', text: 'k' }] } }), /no longer in this campaign/); passed++;
    gaqlReply = q => /FROM ad_group_ad WHERE/.test(q) ? (assert.match(q, /campaign\.id = 42/), rsa) : [];
    const rewrite = { issue: 'Weak copy', fix: 'Add lines', executable: { kind: 'rewriteAds', adId: '555', headlines: ['Handmade charms'], descriptions: [] } };
    r = plain(await apply('42', rewrite));
    const logged = [...f.col(COLS.remedies).values()].find(x => x.kind === 'rewriteAds');
    check(r.queued && approvals === 1 && mutations === 0 && logged && logged.queued && logged.approvalId === r.approvalId, 'an ad rewrite is sent to Approvals and logged as queued, never applied directly');
    r = plain(await apply('42', rewrite));
    check(r.reused && approvals === 1, 'a second click returns the draft already waiting in Approvals');
    gaqlReply = () => [{ campaign: { status: 'ENABLED' }, campaignBudget: { amountMicros: String(5e6) } }];
    await assert.rejects(() => apply('42', { executable: { kind: 'setBudget', budget: 12 } }), /ceiling/); passed++;
    check(budgets === 0, 'a budget that would take enabled budgets over the ceiling is refused before any write');
    r = plain(await apply('42', { executable: { kind: 'setBudget', budget: '9.999' } }));
    check(budgets === 1 && r.budget === 10, 'a budget within the ceiling is applied at a clean amount');
  }

  // ── Dismissing a Google recommendation ──────────────────────────────────
  {
    const e = engine(), f = memory(), rn = 'customers/123/recommendations/abc'; let posts = 0, ledgers = [];
    await f.db.collection(COLS.state).doc('diagnostics').set({ campaigns: [{ id: '42', recommendations: [{ resourceName: rn }, { resourceName: 'customers/123/recommendations/keep' }] }], accountRecommendations: [{ resourceName: rn }] });
    let reply = { ok: true, status: 200, json: async () => ({ partialFailureError: { message: 'expired' } }) };
    e.cx.__fetch = async (url, opts) => { posts++; assert.match(url, /recommendations:dismiss/); assert.equal(JSON.parse(opts.body).partialFailure, false); return reply; };
    e.bind({ fb: () => f, mintToken: async () => 't', ledger: async x => { ledgers.push(x); } });
    const dismiss = (name, dryRun = false) => e.get('dismissGoogleRecommendation')(name, { ctrl: { dryRun } });
    await assert.rejects(() => dismiss('customers/999/recommendations/abc'), /not a recommendation in this Google Ads account/); passed++;
    const dry = plain(await dismiss(rn, true));
    check(dry.dryRun && posts === 0, 'a dry run dismisses nothing in Google Ads');
    await assert.rejects(() => dismiss(rn), /dismiss failed/); passed++;
    check(ledgers[ledgers.length - 1].ok === false, 'a rejected dismissal is recorded as a failure, not a success');
    reply = { ok: true, status: 200, json: async () => ({ results: [{ resourceName: rn }] }) };
    await dismiss(rn);
    const saved = (await f.db.collection(COLS.state).doc('diagnostics').get()).data();
    check(saved.campaigns[0].recommendations.length === 1 && saved.accountRecommendations.length === 0, 'a dismissed recommendation leaves the saved check without another diagnosis');
  }

  // ── Lessons only learn from measured, applied fixes ─────────────────────
  {
    const e = engine(), f = memory(), now = Date.now();
    const rows = e.get('_improvementTimeline')([], [{ id: 'q', campaignId: '42', queued: true, at: now, fix: 'queued' }, { id: 'p', campaignId: '42', at: now, fix: 'applied' }], [], 'search', '42', 'UTC');
    check(rows.length === 1 && rows[0].id === 'remedy:p', 'a queued rewrite is not shown as a published change');
    e.bind({ fb: () => f, getPlaybook: async () => null, gaql: async () => [],
      getDiagnostics: async () => ({ campaigns: [{ id: '42', channel: 'SEARCH', startDate: '2020-01-01', d90: { clicks: 10 } }] }),
      remedyHistory: async () => ({ items: [{ id: 'r1', campaignId: '42', kind: 'addNegatives', verified: true, at: now - 40 * DAY, baseline: { clicks: 400 }, executable: { keywords: ['free'] } }] }) });
    const out = plain(await e.get('distillLessons')({}));
    check(out.unchanged && /Insufficient/.test(out.reason), 'a verified fix without a measured result is not lesson evidence, and no AI request is made');
  }

  // ── A campaign analysis keeps budget and spend currencies apart ─────────
  {
    const e = engine(), f = memory(); let prompt = '', reply = null;
    await f.db.collection('Brites_GAds_Metrics').add({ at: 1, snapshot: [{ id: '42', name: 'Camp A', status: 'ENABLED', channel: 'PERFORMANCE_MAX', budget: 20, cost: 45.5, value: 120, conv: 3, clicks: 40, impr: 900, currency: 'USD' }] });
    e.bind({ fb: () => f, control: async () => ({ targetRoas: 0, maxDailyBudgetTotal: 60, budgetCurrency: 'CAD' }), conversionHealth: async () => ({ validated: true }),
      openaiJSON: async p => { prompt = p; if (!reply) throw Error('model unavailable'); return reply; } });
    reply = { score: 60, status: 'healthy', summary: 's', actions: [{ type: 'budget', title: 'Raise', detail: 'd', suggestedBudget: 25 }, { type: 'budget', title: 'Same', detail: 'd', suggestedBudget: 20 }] };
    let an = plain(await e.get('analyzeCampaign')('42', { force: true }));
    check(/dailyBudget=20 CAD/.test(prompt) && /spend14d=USD45\.5/.test(prompt) && /daily budget in CAD/.test(prompt), 'the analysis labels the budget in the account currency and spend in the reporting currency');
    check(/Performance Max campaign/.test(prompt) && /not set by the owner/.test(prompt), 'the analysis names the campaign type and never invents a target ROAS');
    check(an.actions[0].suggestedBudget === 25 && an.actions[1].suggestedBudget === null && an.budgetCurrency === 'CAD', 'a suggested budget equal to the current one gets no button');
    reply = null; an = plain(await e.get('analyzeCampaign')('42', { force: true }));
    check(an.status === 'insufficient data', 'without the AI and without a target ROAS, no health grade is claimed');
    const html = fs.readFileSync(path.join(repo, 'brites-adwords.html'), 'utf8');
    const pick = name => { const m = new RegExp('^(?:async )?function ' + name + '\\(', 'm').exec(html); const rest = html.slice(m.index), next = /\n(?:async )?function \w+\(/.exec(rest); return next ? rest.slice(0, next.index) : rest; };
    const ui = vm.createContext({ DASH: { budgetCurrency: 'CAD' } });
    for (const name of ['money', 'esc', 'optDot', 'actIcon', 'analysisHtml']) vm.runInContext(pick(name), ui);
    check(/Set \$25 CAD\/day/.test(ui.analysisHtml({ id: '42', name: 'Camp A', status: 'ENABLED' }, { score: 60, status: 'healthy', summary: 's', budgetCurrency: 'CAD', actions: [{ type: 'budget', title: 'Raise', detail: 'd', suggestedBudget: 25 }] })), 'the Set budget button says the currency it will set');
  }

  // ── The diagnostic page is locked with the console passcode ────────────
  {
    const reply = (status, body) => ({ ok: status < 400, status, json: async () => body, text: async () => JSON.stringify(body) });
    const stubFetch = async url => /oauth2/.test(url) ? reply(200, { access_token: 't' }) : reply(200, { results: [{ campaign: { id: '1', name: 'A', status: 'PAUSED' }, campaignBudget: { amountMicros: '5000000' }, customer: { currencyCode: 'CAD' } }] });
    const empty = { where() { return this; }, limit() { return this; }, orderBy() { return this; }, doc() { return this; }, get: async () => ({ exists: false, forEach() {}, data: () => ({}) }) };
    const admin = { firestore: () => ({ collection: () => empty }) };
    const realResolve = Module._resolveFilename;
    Module._resolveFilename = function (request, parent, ...rest) {
      if (request === 'node-fetch' && /googleAdsDiag/.test((parent || {}).filename || '')) return 'STUB:diag-fetch';
      if (/firebaseAdmin$/.test(request) && /googleAdsDiag/.test((parent || {}).filename || '')) return 'STUB:diag-admin';
      return realResolve.call(this, request, parent, ...rest);
    };
    require.cache['STUB:diag-fetch'] = { id: 'STUB:diag-fetch', filename: 'STUB:diag-fetch', loaded: true, exports: stubFetch };
    require.cache['STUB:diag-admin'] = { id: 'STUB:diag-admin', filename: 'STUB:diag-admin', loaded: true, exports: admin };
    // Node caches "node-fetch"/"./firebaseAdmin" per directory once any sibling function has loaded
    // them, which would bypass the resolver hook above; swap the cached modules too while the page runs.
    const fnDir = path.join(repo, 'netlify/functions'), swapped = [];
    for (const [request, exports] of [['node-fetch', stubFetch], ['./firebaseAdmin', admin]]) {
      let file; try { file = require.resolve(request, { paths: [fnDir] }); } catch (e) { continue; }
      swapped.push([file, require.cache[file]]); require.cache[file] = { id: file, filename: file, loaded: true, exports };
    }
    delete require.cache[require.resolve(path.join(fnDir, 'googleAdsDiag.js'))];
    Object.assign(process.env, { GADS_CUSTOMER_ID: '123', EDIT_PASSCODE: 'secret' });
    const diag = require(path.join(repo, 'netlify/functions/googleAdsDiag.js'));
    check((await diag.handler({ queryStringParameters: {}, headers: {} })).statusCode === 401, 'the diagnostic page refuses a request without the passcode');
    check((await diag.handler({ queryStringParameters: { key: 'wrong' }, headers: {} })).statusCode === 401, 'the diagnostic page refuses a wrong passcode');
    const ok = await diag.handler({ queryStringParameters: { key: 'secret' }, headers: { accept: 'text/html' } });
    check(ok.statusCode === 200 && /5 CAD/.test(ok.body) && !/\$5/.test(ok.body), 'with the passcode it opens, showing budgets in the account currency');
    Module._resolveFilename = realResolve; delete process.env.EDIT_PASSCODE;
    for (const [file, prev] of swapped) { if (prev) require.cache[file] = prev; else delete require.cache[file]; }
  }

  // ── The Ad Doctor card ───────────────────────────────────────────────────
  {
    const { JSDOM } = require('jsdom');
    const html = fs.readFileSync(path.join(repo, 'brites-adwords.html'), 'utf8');
    const pick = name => { const m = new RegExp('^(?:async )?function ' + name + '\\(', 'm').exec(html); assert(m, name); const rest = html.slice(m.index), next = /\n(?:async )?function \w+\(/.exec(rest); return next ? rest.slice(0, next.index) : rest; };
    const dom = new JSDOM('<span id="diagSub"></span><button id="diagToggleAll"></button><button id="diagRun"></button><div id="diagBody"></div>', { url: 'https://console.example/', runScripts: 'outside-only' });
    const w = dom.window;
    w.eval('var DASH=null,__calls=[],__replies={},__toasts=[];function api(a,b){__calls.push([a,b]);var r=__replies[a];return Promise.resolve(typeof r==="function"?r(b):r||{});}' +
      'function toast(m){__toasts.push(m);}function uiSnapshot(){return null;}function uiRestore(){}function confirm(){return true;}');
    for (const name of ['money', 'esc', 'btnBusy', 'timeago']) w.eval(pick(name));
    w.eval(html.slice(html.indexOf('/* ---- Fix History'), html.indexOf('// full schedule line + Start-now button')));
    const now = Date.now(), body = () => w.document.getElementById('diagBody');
    const camp = (over = {}) => ({ id: '42', name: 'Camp A', status: 'ENABLED', budget: 10, primaryStatus: 'LIMITED', reasonsText: ['Limited by budget', 'unknown'],
      lostISBudget: 20, recommendations: [{ resourceName: 'customers/123/recommendations/b1', type: 'CAMPAIGN_BUDGET', currentBudget: 10, recommendedBudget: 15, options: [{ budget: 15, weeklyClicksDelta: 12, weeklyCostDelta: 30 }] }, { type: 'SITELINK_ASSET' }],
      observationWindows: { capturedAt: now - 3600000 }, ...over });
    const verdict = { id: '42', severity: 'attention', verdict: 'partial', headline: 'Budget-limited but efficient', aiSays: 'Raise budget.', findings: ['f1', 'f2'],
      action: { kind: 'raiseBudget', budget: 14, urgency: 'this week' }, remedies: [
        { issue: 'Weak copy', fix: 'Add lines', impact: 'medium', executable: { kind: 'rewriteAds', adId: '555', headlines: ['New line'], descriptions: [] } },
        { issue: 'Wasted terms', fix: 'Add negatives', impact: 'high', executable: { kind: 'addNegatives', keywords: ['free'] } }] };
    const set = (d, dash, hist) => { w.DIAG = d; w.DASH = dash; w.REMHIST = hist || []; w.renderDiag(); };
    const dash = (status, over = {}) => ({ budgetCurrency: 'CAD', campaignInventory: { ok: true }, lastMetrics: [{ id: '42', name: 'Camp A', status, budget: 10, ...over }] });
    w.eval('diagOpen={"42":true}');

    set({ generatedAt: now - 3600000, campaigns: [camp()], ai: { campaigns: [verdict] } }, dash('ENABLED'));
    let t = body().innerHTML, text = body().textContent;
    check(/Google status:.*Limited.*Limited by budget/.test(text) && !/unknown/i.test(text), 'the Google status line shows the real reasons and never "unknown"');
    check(/Apply AI budget \$14 CAD\/day/.test(text) && /Apply Google \$15 CAD\/day/.test(text) && /\$10 CAD/.test(text), 'budgets are labelled in the account currency');
    check(body().querySelector('details summary') && /Evidence/.test(t) && /Send to review/.test(text) && /Apply fix/.test(text), 'the verdict leads; evidence sits behind an expander; a rewrite is sent to review, not applied');
    check(/Other Google recommendations: Add sitelinks/.test(text), 'Google recommendation types read as plain actions');

    set({ generatedAt: now - 75 * DAY, campaigns: [camp({ observationWindows: null })], ai: { campaigns: [verdict] } }, dash('PAUSED'));
    text = body().textContent;
    check(/OUTDATED/.test(text) && /It is now paused/.test(text) && /Previous check/.test(text), 'an old check of a campaign that has since changed is marked outdated');
    check(!body().querySelector('.dg-remedy,.dg-aibudget,.dg-gbudget,.dg-dismiss'), 'an outdated check offers no Apply, budget or dismiss buttons');

    set({ generatedAt: now, campaigns: [camp(), camp({ id: '77', name: 'Removed camp' })], ai: { campaigns: [verdict] } }, dash('ENABLED'));
    check(!/Removed camp/.test(body().textContent), 'a campaign no longer in Google Ads is not shown');
    set({ generatedAt: now, campaigns: [camp()], ai: { campaigns: [verdict] } }, { ...dash('ENABLED'), lastMetrics: [{ id: '42', status: 'ENABLED', budget: 10 }, { id: '88', name: 'New camp', status: 'PAUSED', cost: 0, clicks: 0 }] });
    const row = body().querySelector('[data-camp="88"]').textContent;
    check(/NOT DIAGNOSED/.test(row) && /paused/.test(row) && !/\$|spend|clicks/.test(row), 'an undiagnosed campaign shows its status, not spend from an unlabelled window');

    set({ generatedAt: now, campaigns: [camp()], ai: { campaigns: [verdict] }, fixOutcomes: { '42': [{ historyId: 'h1', working: 'not enough data', note: 'Too little data to judge.' }] } }, dash('ENABLED'),
      [{ id: 'h1', campaignId: '42', issue: 'Old negatives', kind: 'addNegatives', executable: { kind: 'addNegatives', keywords: ['cheap'] }, verified: true, at: now - 30 * DAY },
       { id: 'h2', campaignId: '42', issue: 'Earlier copy', kind: 'rewriteAds', adId: '555', queued: true, approvalState: 'PENDING', executable: { kind: 'rewriteAds', adId: '555', headlines: ['x'] }, at: now - DAY },
       { id: 'h3', campaignId: '42', issue: 'Dry test', kind: 'addNegatives', dryRun: true, executable: { kind: 'addNegatives', keywords: ['y'] }, at: now }]);
    text = body().textContent;
    check(/NOT ENOUGH DATA/.test(text) && /Too little data to judge/.test(text) && /IN APPROVALS/.test(text) && !/Dry test/.test(text), 'past fixes show the measured result, drafts waiting in Approvals, and no dry runs');
    check(!/Send to review/.test(text), 'a rewrite already waiting in Approvals for the same ad is not offered again');

    w.__replies.dismissRec = { ok: true };
    set({ generatedAt: now, campaigns: [camp()], ai: { campaigns: [verdict] } }, dash('ENABLED'));
    body().querySelector('.dg-dismiss').click(); await new Promise(r => setTimeout(r, 20));
    check(!w.__calls.some(c => c[0] === 'runDiagnostics') && !w.DIAG.campaigns[0].recommendations.some(r => r.resourceName === 'customers/123/recommendations/b1'), 'dismissing updates the card without a paid re-diagnosis');

    w.__replies.applyRemedy = { ok: true, queued: true, approvalId: 'ap1', note: 'Sent to Approvals for review. No live ad was changed.' };
    set({ generatedAt: now, campaigns: [camp()], ai: { campaigns: [verdict] } }, dash('ENABLED'));
    [...body().querySelectorAll('.dg-remedy')].find(b => /Send to review/.test(b.textContent)).click(); await new Promise(r => setTimeout(r, 20));
    check(/Sent to Approvals/.test(w.__toasts.join(' ')) && !/Send to review/.test(body().textContent) && /IN APPROVALS/.test(body().textContent), 'a sent rewrite moves to past fixes as waiting in Approvals');

    w.__replies.runDiagnostics = { queued: true };
    w.__replies.diagRunStatus = { ok: false, phase: 'running', done: 0, total: 1 };
    w.runDiagOne('42'); w.renderDiag();
    const btn = body().querySelector('.dg-one[data-id="42"]');
    check(btn.disabled && /Diagnosing/.test(btn.textContent) && /"42"/.test(w.sessionStorage.getItem('baDiagRuns')), 'a running one-campaign diagnosis keeps its spinner through re-renders and page reloads');
    w.close();
  }
  console.log(passed + ' Ad Doctor checks passed.');
})().catch(e => { console.error(e); process.exit(1); });
