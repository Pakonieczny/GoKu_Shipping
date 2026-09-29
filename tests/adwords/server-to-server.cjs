// Server-to-server paths keep working whether or not EDIT_PASSCODE is set: the hourly scheduled
// kick reaches the background worker, console actions queue work the worker accepts, the worker's
// own continuations are accepted, and the Shopify order webhook (Shopify HMAC, never the passcode)
// still records sales and refunds. Everything is local: the kick's fetch is delivered straight to
// the worker's handler, and Google Ads, Firestore and Shopify are fakes. No network, no paid calls.
const fs = require('fs'), vm = require('vm'), crypto = require('crypto'), assert = require('assert/strict'), path = require('path');
const dir = path.resolve(__dirname, '../../netlify/functions') + '/', realRequire = require('module').createRequire(dir + 'googleAdsAutopilot.js');
let passed = 0; const check = (v, msg) => { assert(v, msg); passed++; };
const SECRETS = { GADS_REFRESH_TOKEN: 'refresh', GADS_CLIENT_SECRET: 'secret', GADS_DEVELOPER_TOKEN: 'dev' };
const WORKER_URL = /^https:\/\/example\.invalid\/\.netlify\/functions\/googleAdsAutopilot-background$/;

// An engine that records every call and answers { ok: true }, except where a path needs more.
function fakeEngine(calls, ctrl) {
  const count = n => calls.filter(c => c[0] === n).length;
  const fixed = {
    COL: { state: 'Brites_GAds_State', control: 'Brites_GAds_Control', approvals: 'Brites_GAds_Approvals' },
    control: async () => ctrl(),
    gAdsTime: d => d.toISOString(),
    startAdDesign: async () => ({ ok: true, queued: true, workspaceId: 'w1', jobId: 'j1' }),
    runAdDesign: async () => (count('runAdDesign') === 1 ? { ok: true, dispatch: true } : { ok: true }),
    startAdDesignMotion: async () => ({ ok: true, queued: true, workspaceId: 'w1', jobId: 'm1' }),
    runAdDesignMotion: async () => (count('runAdDesignMotion') === 1 ? { ok: true, continue: true, workspaceId: 'w1', jobId: 'm1' } : { ok: true })
  };
  return new Proxy(fixed, { get(t, k) {
    if (typeof k === 'symbol' || k === 'then') return undefined;
    if (k === 'COL') return t.COL;
    const impl = t[k];
    return (...args) => { calls.push([k, ...args]); return impl ? impl(...args) : Promise.resolve({ ok: true }); };
  } });
}
function memoryAdmin() {
  const docs = new Map();
  const doc = p => ({ get: async () => ({ exists: docs.has(p), data: () => docs.get(p) }), set: async (v, o) => { docs.set(p, o && o.merge ? { ...docs.get(p), ...v } : v); } });
  const admin = { firestore: () => ({ collection: c => ({ doc: d => doc(c + '/' + d) }) }) };
  admin.firestore.FieldValue = { serverTimestamp: () => Date.now() };
  return admin;
}
function load(file, env, mods) {
  const mod = { exports: {} };
  const ctx = { process: { env }, console: { ...console, log() {} }, Buffer, URL, URLSearchParams, setTimeout, clearTimeout, module: mod, exports: mod.exports,
    require: n => (n in mods ? mods[n] : realRequire(n)) };
  vm.createContext(ctx); vm.runInContext(fs.readFileSync(dir + file, 'utf8'), ctx, { filename: file });
  return { api: mod.exports, ctx };
}
// The kick, the console API and the worker, wired together: every fetch the kick or the worker
// makes is delivered to the worker's own handler, and its answer is what the caller sees.
function site(env, ctrl) {
  const calls = [], trail = [], E = fakeEngine(calls, ctrl), admin = memoryAdmin();
  let worker = null;
  const deliver = async (url, opts) => {
    assert(WORKER_URL.test(url), 'server-to-server calls go only to the worker: ' + url);
    const res = await worker.handler({ httpMethod: 'POST', headers: { 'content-type': 'application/json' }, body: opts.body });
    trail.push({ sent: JSON.parse(opts.body), status: res.statusCode, out: JSON.parse(res.body) });
    return { ok: res.statusCode < 400, status: res.statusCode };
  };
  const mods = { 'node-fetch': deliver, './googleAdsAutopilot': E, './firebaseAdmin': admin };
  worker = load('googleAdsAutopilot-background.js', env, mods).api;
  const kick = load('googleAdsAutopilotKick.js', env, mods);
  return { calls, trail, worker, kick: kick.api, ctx: kick.ctx };
}
const post = (body, headers = {}) => ({ httpMethod: 'POST', headers, body: JSON.stringify(body) });

// Every fetch to the worker, in the kick and in the worker itself, carries the server credential.
function workerCallsCarryToken(file) {
  const src = fs.readFileSync(dir + file, 'utf8'), found = [];
  let at = 0;
  while ((at = src.indexOf('fetch(', at)) !== -1) {
    let i = at + 6, depth = 1, quote = null;
    for (; i < src.length && depth; i++) {
      const ch = src[i];
      if (quote) { if (ch === '\\') i++; else if (ch === quote) quote = null; continue; }
      if (ch === '"' || ch === "'" || ch === '`') quote = ch; else if (ch === '(') depth++; else if (ch === ')') depth--;
    }
    const call = src.slice(at, i);
    if (/googleAdsAutopilot-background/.test(call)) found.push(call);
    at = i;
  }
  return found;
}

(async () => {
  for (const file of ['googleAdsAutopilotKick.js', 'googleAdsAutopilot-background.js']) {
    const calls = workerCallsCarryToken(file);
    check(calls.length > 0 && calls.every(c => /token:\s*workerToken\(\)/.test(c)), file + ': all ' + calls.length + ' calls to the worker send workerToken()');
  }

  for (const [label, passcode] of [['EDIT_PASSCODE unset', undefined], ['EDIT_PASSCODE set', '"s3cret-pass" ']]) {
    const env = { URL: 'https://example.invalid', GADS_DAILY_HOUR: '99', ...SECRETS };
    if (passcode !== undefined) env.EDIT_PASSCODE = passcode;
    let ctrl = { enabled: true, dryRun: false };
    const s = site(env, () => ctrl), token = s.ctx.workerToken();
    check(passcode === undefined ? /^internal-[a-f0-9]{64}$/.test(token) : token === 's3cret-pass', label + ': the kick holds a worker credential');

    // 1. The hourly schedule, exactly as Netlify invokes it.
    let r = await s.kick.handler({ headers: { 'x-nf-event': 'schedule' }, body: '' });
    let out = JSON.parse(r.body);
    check(r.statusCode === 200 && out.status === 'kicked' && out.upstream === 200, label + ': scheduled kick accepted by the worker');
    check(s.trail.length === 1 && s.trail[0].out.status === 'ran' && s.trail[0].sent.token === token, label + ': the worker ran the hourly tasks');
    check(['anomalyCheck', 'monthlySpendGuard', 'enforceBudgetCeiling', 'uploadConversions'].every(n => s.calls.some(c => c[0] === n)), label + ': anomaly, monthly stop, ceiling and conversions ran');
    // Automation off with a monthly stop set: the stop keeps being checked every hour.
    ctrl = { enabled: false, maxMonthlySpend: 500 }; s.calls.length = 0;
    r = await s.kick.handler({ isScheduled: true, headers: {} }); out = JSON.parse(r.body);
    check(out.upstream === 200 && s.trail[1].out.status === 'ran' && s.calls.some(c => c[0] === 'monthlySpendGuard'), label + ': with automation off the monthly stop is still checked');
    ctrl = { enabled: true, dryRun: false };

    // 2. Console actions that queue work for the worker, and the worker's own continuations.
    const auth = passcode === undefined ? {} : { 'x-edit-passcode': 's3cret-pass' };
    const before = s.trail.length; s.calls.length = 0;
    const approve = await s.kick.httpHandler(post({ action: 'approve', id: 'd1' }, auth));
    const design = await s.kick.httpHandler(post({ action: 'startAdDesign', workspaceId: 'w1' }, auth));
    const motion = await s.kick.httpHandler(post({ action: 'startAdDesignMotion', workspaceId: 'w1' }, auth));
    const run = await s.kick.httpHandler(post({ action: 'runNow', tasks: ['conversions'] }, auth));
    const dash = await s.kick.httpHandler(post({ action: 'dashboard' }, auth));
    check(dash.statusCode === 200, label + ': the dashboard read answers');
    if (passcode === undefined) {
      check([approve, design, motion, run].every(x => x.statusCode === 403 && JSON.parse(x.body).code === 'EDIT_PASSCODE_NOT_SET') && s.trail.length === before && !s.calls.some(c => c[0] !== 'control' && c[0] !== 'dashboard'),
        label + ': console changes are refused before anything is queued (the schedule above is unaffected)');
    } else {
      const sent = s.trail.slice(before);
      check([approve, design, motion, run].every(x => x.statusCode === 200), label + ': console actions with the passcode are accepted');
      check(sent.length === 6 && sent.every(t => t.status === 200 && t.out.status === 'ran' && t.sent.token === token), label + ': all 6 worker calls accepted (4 queued by the console, 2 worker continuations)');
      check(s.calls.some(c => c[0] === 'applyApproval' && c[1] === 'd1') && s.calls.filter(c => c[0] === 'runAdDesign').length === 2 && s.calls.filter(c => c[0] === 'runAdDesignMotion').length === 2 && s.calls.some(c => c[0] === 'uploadConversions'),
        label + ': publication, design and film continuations, and the manual run all reached the engine');
      const wrong = await s.kick.httpHandler(post({ action: 'runNow' }, { 'x-edit-passcode': 'guess' }));
      check(wrong.statusCode === 401 && s.trail.length === before + 6, label + ': a wrong passcode queues nothing');
    }
    // Nobody but the server can drive the worker directly.
    const n = s.trail.length; s.calls.length = 0;
    for (const t of [undefined, 'guess', 'internal-' + '0'.repeat(64)]) {
      const w = await s.worker.handler(post({ tasks: ['publishApproval'], id: 'd1', token: t }));
      assert(w.statusCode === (passcode === undefined ? 403 : 401), label + ' worker refuses token ' + t);
    }
    check(!s.calls.some(c => c[0] !== 'control') && s.trail.length === n, label + ': the worker refuses anonymous and guessed tokens without doing anything');

    // 3. The Shopify order webhook: authenticated by Shopify's HMAC, independent of EDIT_PASSCODE.
    const hookCalls = [], studio = { grantStudioCredits: async () => ({ ok: true }), markSessionOrdered: async () => ({ ok: true }) };
    const hookEnv = { ...env, SHOPIFY_WEBHOOK_SECRET: 'whsec-test-only' };
    const hook = load('shopifyOrderWebhook.js', hookEnv, { './googleAdsAutopilot': fakeEngine(hookCalls, () => ctrl), './britesAuth': studio }).api;
    const signed = (topic, payload, secret = 'whsec-test-only', extra = {}) => {
      const raw = JSON.stringify(payload);
      return { httpMethod: 'POST', isBase64Encoded: false, body: raw,
        headers: { 'x-shopify-topic': topic, 'x-shopify-hmac-sha256': crypto.createHmac('sha256', secret).update(Buffer.from(raw)).digest('base64'), ...extra } };
    };
    const order = { id: 1001, name: '#1001', total_price: '42.00', currency: 'CAD', financial_status: 'paid', created_at: '2026-09-01T12:00:00Z',
      note_attributes: [{ name: 'gclid', value: 'test-click' }], line_items: [{ title: 'Test charm', sku: 'T-1', quantity: 1, price: '42.00' }] };
    r = await hook.handler(signed('orders/paid', order));
    check(r.statusCode === 200 && JSON.parse(r.body).ok && hookCalls.some(c => c[0] === 'enqueueConversion' && c[1].orderId === '1001' && c[1].gclid === 'test-click') && hookCalls.some(c => c[0] === 'recordOrderEvent'),
      label + ': a signed paid order is queued for Google Ads and logged');
    hookCalls.length = 0;
    r = await hook.handler(signed('orders/paid', { ...order, id: 1002, note_attributes: [] }));
    check(r.statusCode === 200 && !hookCalls.some(c => c[0] === 'enqueueConversion') && hookCalls.some(c => c[0] === 'recordOrderEvent'), label + ': a signed organic order is logged, not uploaded');
    hookCalls.length = 0;
    r = await hook.handler(signed('refunds/create', { id: 5001, order_id: 1001, created_at: '2026-09-02T12:00:00Z', transactions: [{ kind: 'refund', status: 'success', amount: '10.00' }] }));
    check(r.statusCode === 200 && hookCalls.some(c => c[0] === 'recordRefund' && c[1].orderId === '1001' && c[1].refundAmount === 10), label + ': a signed refund is recorded');
    hookCalls.length = 0;
    const unsigned = signed('orders/paid', order); delete unsigned.headers['x-shopify-hmac-sha256'];
    const forged = [unsigned, signed('orders/paid', order, 'guess'), signed('orders/paid', order, 's3cret-pass'),
      { ...unsigned, headers: { ...unsigned.headers, 'x-edit-passcode': 's3cret-pass' } },
      { ...signed('orders/paid', order), body: JSON.stringify({ ...order, total_price: '4200.00' }) }];
    for (const e of forged) assert.equal((await hook.handler(e)).statusCode, 401);
    check(hookCalls.length === 0, label + ': unsigned, wrongly signed, passcode-only and tampered webhooks are refused before any work');
  }
  check(!/EDIT_PASSCODE/.test(fs.readFileSync(dir + 'shopifyOrderWebhook.js', 'utf8')), 'the webhook never depends on EDIT_PASSCODE');
  console.log('PASS ' + passed + ' server-to-server checks (scheduled kick, console-queued work, worker continuations, Shopify webhook; EDIT_PASSCODE unset and set)');
})().catch(e => { console.error(e); process.exit(1); });
