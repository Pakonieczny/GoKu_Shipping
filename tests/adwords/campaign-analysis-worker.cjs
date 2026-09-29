// Campaign analysis: the paid AI call runs in the background worker, never inside the ~26 s console
// gateway. A saved analysis returns at once, a running one is joined instead of paid for twice, and the
// console polls genStatus for the answer. Offline: fetch, Firestore and the model are fakes.
const fs = require('fs'), vm = require('vm'), assert = require('assert/strict'), path = require('path'), Module = require('module');
const repo = path.resolve(__dirname, '../..'), dir = path.join(repo, 'netlify/functions') + '/', engineFile = dir + 'googleAdsAutopilot.js';
const clone = x => x == null ? x : JSON.parse(JSON.stringify(x)), J = JSON.stringify;
let passed = 0; const check = (v, msg) => { assert(v, msg); passed++; console.log('PASS', msg); };

// ── Kick and worker, loaded with a fake engine, fetch and clock ────────────
const calls = [], status = new Map(); let upstream = 202, now = Date.parse('2026-09-29T12:00:00Z'), saved = null, analyze = null;
class Clock extends Date { constructor(...a) { if (a.length) super(...a); else super(now); } static now() { return now; } }
const E = { COL: { state: 'state', control: 'control', approvals: 'approvals' }, control: async () => ({ enabled: false, dryRun: false }),
  analyzeCampaign: async (id, o) => { calls.push(['analyze', id, clone(o)]); return o && o.cacheOnly ? clone(saved) : analyze(id, o); },
  getGenStatus: async id => clone(status.get(id) || null),
  setGenStatus: async (id, v) => { calls.push(['status', id, v.phase + (v.ok === undefined ? '' : ':' + v.ok)]); status.set(id, { ...v, at: now }); } };
const db = { collection: () => ({ doc: () => ({ get: async () => ({ exists: false }) }) }) }, admin = { firestore: () => db }; admin.firestore.FieldValue = {};
function editPasscode(env) { const m = { exports: {} }, c = { process: { env }, console, Date: Clock, module: m, exports: m.exports, require: n => n === './firebaseAdmin' ? admin : require(n) }; vm.createContext(c); vm.runInContext(fs.readFileSync(dir + '_editPasscode.js', 'utf8'), c); return m.exports; }
function load(file) {
  const mod = { exports: {} }, env = { EDIT_PASSCODE: 'test-pass', URL: 'https://example.invalid' }, EP = editPasscode(env);
  const ctx = { process: { env }, console, Date: Clock, Set, JSON, module: mod, exports: mod.exports,
    require: n => n === 'node-fetch' ? async (url, opts) => { calls.push(['dispatch', JSON.parse(opts.body)]); return { ok: upstream < 400, status: upstream }; } : n === './googleAdsAutopilot' ? E : n === './firebaseAdmin' ? admin : n === './_editPasscode' ? EP : require(n) };
  vm.createContext(ctx); vm.runInContext(fs.readFileSync(dir + file, 'utf8'), ctx); return mod.exports;
}
// ── The engine itself, offline, for the console's quick cache check ────────
function engine() {
  const cx = vm.createContext({ module: { exports: {} }, exports: {}, require: n => n === 'node-fetch' ? async () => { throw Error('Live network forbidden'); } : Module.createRequire(engineFile)(n),
    process: { env: { GADS_CUSTOMER_ID: '123' } }, console, Buffer, Date, Intl, Map, Set, URL, setTimeout, clearTimeout });
  vm.runInContext(fs.readFileSync(engineFile, 'utf8'), cx);
  return { get: n => vm.runInContext(n, cx), bind(v) { cx.__m = v; vm.runInContext(Object.keys(v).map(k => k + '=__m.' + k).join('\n'), cx); } };
}
function memory() {
  const cols = new Map(), col = n => { if (!cols.has(n)) cols.set(n, new Map()); return cols.get(n); };
  const ref = (n, id) => ({ id, get: async () => ({ id, exists: col(n).has(id), data: () => clone(col(n).get(id)) }), set: async v => { col(n).set(id, clone(v)); } });
  return { db: { collection: n => ({ doc: id => ref(n, String(id)) }) } };
}

(async () => {
  {
    const e = engine(), f = memory(); let ai = 0, snaps = 0;
    e.bind({ fb: () => f, control: async () => ({ targetRoas: 0, maxDailyBudgetTotal: 60 }), openaiJSON: async () => { ai++; throw Error('Paid AI call forbidden'); },
      latestSnapshotCampaign: async () => { snaps++; return null; } });
    const an = e.get('analyzeCampaign'), doc = f.db.collection('Brites_GAds_State').doc('analysis_42');
    check(await an('42', { cacheOnly: true }) === null && ai === 0 && snaps === 0, 'engine: with no saved analysis the quick check returns nothing and spends nothing');
    await doc.set({ analysis: { summary: 'saved read', score: 70 }, at: Date.now() - 3600000 });
    check(clone(await an('42', { cacheOnly: true })).summary === 'saved read' && ai === 0, 'engine: a saved analysis under 6 h old is returned by the quick check');
    await doc.set({ analysis: { summary: 'old read' }, at: Date.now() - 7 * 3600000 });
    check(await an('42', { cacheOnly: true }) === null && ai === 0 && snaps === 0, 'engine: an analysis over 6 h old is not reused, and the quick check still spends nothing');
  }

  const api = load('googleAdsAutopilotKick.js'), act = async b => clone(await api.handleAction(b)), sent = () => calls.filter(c => c[0] === 'dispatch');
  saved = { summary: 'saved read', score: 70, status: 'healthy', actions: [], campaignId: '42' };
  let r = await act({ action: 'analyzeCampaign', id: '42' });
  check(r.summary === 'saved read' && !r.queued, 'console: a saved analysis (under 6 h) returns at once');
  check(J(calls) === J([['analyze', '42', { cacheOnly: true }]]), 'console: answering from the saved analysis reads only the cache; nothing is dispatched or paid for');
  saved = null; calls.length = 0; r = await act({ action: 'analyzeCampaign', id: 'cmp-42' });
  check(r.queued === true && r.genId === 'analysis-42', 'console: without a saved analysis the request is queued under a per-campaign status id');
  const d = sent();
  check(d.length === 1 && J(d[0][1].tasks) === '["analyzeCampaign"]' && d[0][1].campaignId === '42' && d[0][1].genId === 'analysis-42' && d[0][1].force === false && d[0][1].token === 'test-pass',
    'console: the background worker gets the task, campaign, status id and server token');
  check(calls.findIndex(c => c[0] === 'status' && c[2] === 'running') < calls.findIndex(c => c[0] === 'dispatch'), 'console: the status reads running before dispatch, so a poll never sees an older result');
  check(!calls.some(c => c[0] === 'analyze' && !(c[2] && c[2].cacheOnly)), 'console: the paid analysis itself never runs inside the gateway request');
  now += 4 * 60000; calls.length = 0; r = await act({ action: 'analyzeCampaign', id: '42' });
  check(r.queued && r.joined && r.genId === 'analysis-42' && !sent().length, 'console: a second request while the analysis runs joins it instead of paying again');
  calls.length = 0; r = await act({ action: 'analyzeCampaign', id: '42', force: true });
  check(r.joined && !calls.length, 'console: Re-analyze during a run joins that run');
  now += 11 * 60000; calls.length = 0; r = await act({ action: 'analyzeCampaign', id: '42', force: true });
  check(r.queued && !r.joined && sent().length === 1 && sent()[0][1].force === true && !calls.some(c => c[0] === 'analyze'),
    'console: a run silent for over 10 minutes is presumed stopped; Re-analyze dispatches a forced run without reading the cache');
  upstream = 500; status.delete('analysis-42'); calls.length = 0; r = await act({ action: 'analyzeCampaign', id: '42' });
  check(/Background dispatch failed: HTTP 500/.test(r.error) && status.get('analysis-42').phase === 'done' && status.get('analysis-42').ok === false,
    'console: a failed dispatch is reported and closes its status');
  upstream = 202; calls.length = 0; r = await act({ action: 'analyzeCampaign', id: '42' });
  check(r.queued && !r.joined && sent().length === 1, 'console: after a failed dispatch the next request dispatches again');
  calls.length = 0; r = await act({ action: 'analyzeCampaign', id: 'none' });
  check(/Campaign id missing/.test(r.error) && !calls.length, 'console: a request without a campaign id reads and dispatches nothing');
  status.set('analysis-7', { phase: 'done', ok: true, kind: 'campaign-analysis', analysis: { summary: 'finished read' }, at: now });
  check((await act({ action: 'genStatus', genId: 'analysis-7' })).analysis.summary === 'finished read', 'console: genStatus returns the finished analysis');
  calls.length = 0; let h = await api.httpHandler({ httpMethod: 'POST', headers: { 'x-edit-passcode': 'wrong' }, body: J({ action: 'analyzeCampaign', id: '43' }) });
  check(h.statusCode === 401 && !calls.length, 'console: without the passcode nothing is read, dispatched or paid for');
  h = await api.httpHandler({ httpMethod: 'POST', headers: { 'x-edit-passcode': 'test-pass' }, body: J({ action: 'analyzeCampaign', id: '43' }) });
  check(h.statusCode === 200 && JSON.parse(h.body).genId === 'analysis-43' && sent().length === 1, 'console: with the passcode the analysis is queued');

  const bg = load('googleAdsAutopilot-background.js'), run = async b => JSON.parse((await bg.handler({ httpMethod: 'POST', body: J({ tasks: ['analyzeCampaign'], token: 'test-pass', ...b }) })).body);
  analyze = async (id, o) => ({ summary: 'fresh read', score: 64, status: 'learning', actions: [], campaignId: String(id), note: undefined });
  status.clear(); calls.length = 0; let w = await run({ genId: 'analysis-42', campaignId: '42', force: true });
  check(w.status === 'ran' && w.result.analyzeCampaign.ok === true, 'worker: the analysis runs with automation switched off (a read, not a Google Ads change)');
  const a = calls.filter(c => c[0] === 'analyze');
  check(a.length === 1 && a[0][1] === '42' && a[0][2].force === true && !a[0][2].cacheOnly, 'worker: one paid analysis, forced when the owner asked to re-analyze');
  const st = status.get('analysis-42');
  check(J(calls.filter(c => c[0] === 'status').map(c => c[2])) === '["running","done:true"]' && st.kind === 'campaign-analysis' && st.analysis.summary === 'fresh read' && !('note' in st.analysis),
    'worker: the finished analysis lands on the status doc the console polls, with undefined fields dropped for Firestore');
  analyze = async () => { throw Error('model unavailable'); }; calls.length = 0; w = await run({ genId: 'analysis-42', campaignId: '42' });
  check(status.get('analysis-42').ok === false && /model unavailable/.test(status.get('analysis-42').error) && /model unavailable/.test(w.result.analyzeCampaign.error),
    'worker: a failed analysis reports its error to the console instead of leaving it waiting');
  calls.length = 0; const denied = await bg.handler({ httpMethod: 'POST', body: J({ tasks: ['analyzeCampaign'], genId: 'analysis-42', campaignId: '42', token: 'wrong' }) });
  check(denied.statusCode === 401 && !calls.length, 'worker: an unauthenticated analysis request is refused');

  // ── The console: queue, poll genStatus, show the answer ──────────────────
  const html = fs.readFileSync(path.join(repo, 'brites-adwords.html'), 'utf8');
  const pick = name => { const m = new RegExp('^(?:async )?function ' + name + '\\(', 'm').exec(html); assert(m, name); const rest = html.slice(m.index), next = /\n(?:async )?function \w+\(/.exec(rest); return next ? rest.slice(0, next.index) : rest; };
  let clock = 0, replies = [], sawLoading = true; const asked = [], toasts = [];
  class UIDate extends Date { static now() { return clock; } }
  const ui = vm.createContext({ P: { analysis: {}, loading: {} }, renderPerf: () => {}, actStart: () => 1, actEnd: () => {}, toast: m => toasts.push(String(m)), Promise, Date: UIDate,
    setTimeout: (fn, ms) => { clock += ms; fn(); },
    api: async (action, extra) => { asked.push([action, clone(extra)]); if (action === 'genStatus' && !ui.P.loading['42']) sawLoading = false;
      const next = replies.length ? replies.shift() : { phase: 'running' }; if (next instanceof Error) throw next; return clone(next); } });
  for (const n of ['doAnalyze', 'analysisWait']) vm.runInContext(pick(n), ui);
  replies = [{ summary: 'saved read', score: 70, actions: [] }]; await ui.doAnalyze('42', false);
  check(ui.P.analysis['42'].summary === 'saved read' && J(asked) === J([['analyzeCampaign', { id: '42', force: false }]]) && ui.P.loading['42'] === false, 'page: a saved analysis shows at once, with no polling');
  delete ui.P.analysis['42']; asked.length = 0;
  replies = [{ queued: true, genId: 'analysis-42' }, { phase: 'running' }, Error('network blip'), { phase: 'done', ok: true, analysis: { summary: 'fresh read', score: 64, actions: [] } }];
  await ui.doAnalyze('42', true);
  check(ui.P.analysis['42'].summary === 'fresh read' && ui.P.loading['42'] === false && sawLoading, 'page: a queued analysis is polled until done and then shown; the spinner stays up meanwhile');
  check(J(asked.map(x => x[0])) === J(['analyzeCampaign', 'genStatus', 'genStatus', 'genStatus']) && asked.slice(1).every(x => x[1].genId === 'analysis-42') && asked[0][1].force === true,
    'page: polls ask for that campaign\'s status id, and a failed poll just waits for the next one');
  delete ui.P.analysis['42']; replies = [{ queued: true, genId: 'analysis-42' }, { phase: 'done', ok: false, error: 'model unavailable' }]; await ui.doAnalyze('42', false);
  check(!ui.P.analysis['42'] && toasts.pop() === 'model unavailable' && ui.P.loading['42'] === false, 'page: a failed analysis shows its error and clears the spinner');
  asked.length = 0; replies = [{ queued: true, genId: 'analysis-42' }]; await ui.doAnalyze('42', false);
  check(!ui.P.analysis['42'] && /still running/.test(toasts.pop()) && asked.filter(x => x[0] === 'genStatus').length === 200 && ui.P.loading['42'] === false,
    'page: after 10 minutes of polling the page stops and says the analysis is still running');
  console.log(`${passed} campaign analysis checks passed.`);
})().catch(e => { console.error(e); process.exitCode = 1; });
