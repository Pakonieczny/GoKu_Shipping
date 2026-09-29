// The Adwords console passcode kept in Firebase (Firestore config/editPasscode, _editPasscode.js):
// EDIT_PASSCODE in Netlify still wins; a missing document is created once, atomically, with a
// generated passcode that is then required; an empty passcode or a failing Firestore locks changes
// (reads only) and generates nothing; a passcode changed in Firestore is used once the one-minute
// cache expires; the passcode never appears in a response or a log line; the hourly kick still
// reaches the worker with the server-only token; and the check pages accept the Firebase passcode
// as ?key=, header or body. Offline: Firestore, Google, Shopify and the worker are local fakes, and
// every passcode here is synthetic.
'use strict';
const fs = require('fs'), vm = require('vm'), path = require('path'), assert = require('assert/strict'), Module = require('module');
const dir = path.resolve(__dirname, '../../netlify/functions') + '/';
let passed = 0; const check = (v, msg) => { assert(v, msg); passed++; };
const SECRETS = { GADS_REFRESH_TOKEN: 'refresh', GADS_CLIENT_SECRET: 'secret', GADS_DEVELOPER_TOKEN: 'dev' };
const env = () => ({ URL: 'https://example.invalid', GADS_DAILY_HOUR: '99', ...SECRETS });
const UNSET = 'Changes are locked until a passcode is saved in Firebase (Firestore config/editPasscode)';
const NOTE = 'Passcode for the Brites Adwords console and its check pages. Change it here any time; the site picks up a change within a minute.';
const PATTERN = /^[abcdefghjkmnpqrstuvwxyz23456789]{4}-[abcdefghjkmnpqrstuvwxyz23456789]{4}-[abcdefghjkmnpqrstuvwxyz23456789]{4}$/;
const clone = x => x == null ? x : JSON.parse(JSON.stringify(x));
const tick = () => new Promise(r => setImmediate(r));

// Every response and every log line, to prove at the end that no passcode is in any of them.
const responses = [], said = [], secrets = new Set();
const seen = res => { responses.push(res); return res; };
const record = (...a) => { said.push(a.map(x => typeof x === 'string' ? x : JSON.stringify(x)).join(' ')); };
const quietConsole = { log: record, info: record, warn: record, error: record, debug: record };

// One clock for every helper copy, so the one-minute cache can be walked past.
let now = Date.parse('2026-09-29T12:00:00Z');
class FakeDate extends Date { constructor(...a) { super(...(a.length ? a : [now])); } static now() { return now; } }

// Firestore: an atomic create() (ALREADY_EXISTS when the document exists), a log of every access,
// and switchable read or create failures. A failing create echoes what it was given, so the test
// can prove the helper keeps the passcode out of its error text.
function store(seed = {}) {
  const docs = new Map(Object.entries(seed).map(([k, v]) => [k, clone(v)])), log = [], attempted = [];
  let failing = null;
  const ref = p => ({
    get: async () => { log.push(['get', p]); await tick(); if (failing === 'read') throw Object.assign(Error('14 UNAVAILABLE: synthetic Firestore outage'), { code: 14 });
      const v = docs.get(p); return { exists: docs.has(p), data: () => clone(v) }; },
    create: async v => { log.push(['create', p]); attempted.push(clone(v)); await tick();
      if (failing === 'create') throw Object.assign(Error('7 PERMISSION_DENIED: refused to write ' + JSON.stringify(v)), { code: 7 });
      if (docs.has(p)) throw Object.assign(Error('6 ALREADY_EXISTS: Document already exists: ' + p), { code: 6 });
      docs.set(p, clone(v)); },
    set: async (v, o) => { log.push(['set', p]); docs.set(p, o && o.merge ? { ...docs.get(p), ...clone(v) } : clone(v)); }
  });
  const db = { collection: c => ({ doc: d => ref(c + '/' + d) }) };
  const admin = { firestore: () => db }; admin.firestore.FieldValue = { serverTimestamp: () => 'server-time' };
  return { docs, log, attempted, db, admin, fail: m => { failing = m; },
    count: (op, p = 'config/editPasscode') => log.filter(x => x[0] === op && x[1] === p).length };
}

// An engine that records every call and answers { ok: true }.
function engine(calls) {
  const COL = { state: 'Brites_GAds_State', control: 'Brites_GAds_Control', approvals: 'Brites_GAds_Approvals' };
  return new Proxy({}, { get(t, k) {
    if (typeof k === 'symbol' || k === 'then') return undefined;
    if (k === 'COL') return COL;
    return (...args) => { calls.push([k, ...args]); return Promise.resolve(k === 'control' ? { enabled: true, dryRun: false } : { ok: true }); };
  } });
}

// One warm lambda: a function file with its own copy of the passcode helper (its own cache), over
// the given env and fake Firestore. With no file, just the helper.
function lambda(file, penv, st, extra = {}) {
  const hm = { exports: {} };
  const hctx = { process: { env: penv }, console: quietConsole, Date: FakeDate, module: hm, exports: hm.exports, require: n => n === './firebaseAdmin' ? st.admin : require(n) };
  vm.createContext(hctx); vm.runInContext(fs.readFileSync(dir + '_editPasscode.js', 'utf8'), hctx, { filename: '_editPasscode.js' });
  if (!file) return { EP: hm.exports };
  const mod = { exports: {} };
  const ctx = { process: { env: penv }, console: quietConsole, Buffer, URL, URLSearchParams, setTimeout, clearTimeout, module: mod, exports: mod.exports,
    require: n => n === 'node-fetch' ? extra.fetch : n === './googleAdsAutopilot' ? extra.E : n === './firebaseAdmin' ? st.admin : n === './_editPasscode' ? hm.exports : require(n) };
  vm.createContext(ctx); vm.runInContext(fs.readFileSync(dir + file, 'utf8'), ctx, { filename: file });
  return { api: mod.exports, ctx, EP: hm.exports };
}

// The console API (kick) and the worker as two lambdas: every fetch either makes is delivered to
// the worker's handler.
function site(penv, st) {
  const calls = [], trail = [], E = engine(calls);
  let worker = null;
  const fetch = async (url, opts) => {
    assert(/^https:\/\/example\.invalid\/\.netlify\/functions\/googleAdsAutopilot-background$/.test(url), 'only the worker is called: ' + url);
    const res = seen(await worker.api.handler({ httpMethod: 'POST', headers: {}, body: opts.body }));
    trail.push({ sent: JSON.parse(opts.body), status: res.statusCode, out: JSON.parse(res.body) });
    return { ok: res.statusCode < 400, status: res.statusCode };
  };
  worker = lambda('googleAdsAutopilot-background.js', penv, st, { E, fetch });
  const kick = lambda('googleAdsAutopilotKick.js', penv, st, { E, fetch });
  const post = async (body, headers = {}) => seen(await kick.api.httpHandler({ httpMethod: 'POST', headers, body: JSON.stringify(body) }));
  return { calls, trail, worker, kick, post };
}

(async () => {
  // 1. The helper on its own.
  {
    const st = store({ 'config/editPasscode': { passcode: '  "from-fire-base" ' } }), { EP } = lambda(null, {}, st);
    let r = await EP.resolve({ env: { EDIT_PASSCODE: '  "envv-pass-1111" ' }, db: st.db });
    check(r.value === 'envv-pass-1111' && r.source === 'env' && st.log.length === 0, 'EDIT_PASSCODE (trimmed, unquoted) wins and Firestore is not read');
    r = await EP.resolve({ env: {}, db: st.db });
    check(r.value === 'from-fire-base' && r.source === 'firebase' && !r.error, 'without it config/editPasscode.passcode is used, trimmed and unquoted');
    const numeric = lambda(null, {}, store({ 'config/editPasscode': { passcode: 482913 } })).EP;
    check((await numeric.resolve()).value === '482913', 'a passcode typed into Firestore as a number still works');
    const many = Array.from({ length: 400 }, () => EP.generate());
    check(many.every(p => PATTERN.test(p)) && new Set(many).size === many.length, 'generated passcodes are 3 groups of 4 from the unambiguous alphabet, never repeated');
    check(EP.sameSecret(' abc ', 'abc') && !EP.sameSecret('abc', 'abd') && !EP.sameSecret('', '') && !EP.sameSecret(undefined, 'abc') && !EP.sameSecret('abc', ''),
      'sameSecret trims the offer and never matches an empty value');
  }

  // 2. First use: no document. It is created once, atomically, and its passcode is then required.
  {
    const st = store(), s = site(env(), st);
    const first = await Promise.all(Array.from({ length: 5 }, () => s.post({ action: 'dashboard' })));
    const doc = st.docs.get('config/editPasscode'), pass = doc && doc.passcode; secrets.add(pass);
    check(st.count('get') === 1 && st.count('create') === 1 && PATTERN.test(pass), 'five concurrent first requests read once and create config/editPasscode once, with a generated passcode');
    check(doc.note === NOTE && Date.parse(doc.createdAt) === now && Object.keys(doc).sort().join() === 'createdAt,note,passcode', 'the document holds the passcode, createdAt and a note saying what it is for');
    check(first.every(r => r.statusCode === 401), 'from then on the passcode is required: the requests that carried none were refused');
    let r = await s.post({ action: 'kill' });
    check(r.statusCode === 401 && !st.docs.has('Brites_GAds_Control/control'), 'a change without the passcode gets 401 and changes nothing');
    r = await s.post({ action: 'kill' }, { 'x-edit-passcode': 'wrng-pass-0000' });
    check(r.statusCode === 401 && !st.docs.has('Brites_GAds_Control/control'), 'a change with a wrong passcode gets 401 and changes nothing');
    r = await s.post({ action: 'kill' }, { 'x-edit-passcode': pass });
    check(r.statusCode === 200 && st.docs.get('Brites_GAds_Control/control').enabled === false, 'with the Firebase passcode in the header the change is made');
    r = await s.post({ action: 'resume', passcode: '  ' + pass + ' ' });
    check(r.statusCode === 200 && st.docs.get('Brites_GAds_Control/control').enabled === true, 'the passcode is accepted in the body too (trimmed)');
    r = await s.post({ action: 'dashboard' }, { 'x-edit-passcode': pass });
    check(r.statusCode === 200 && JSON.parse(r.body).editPasscodeSet === true, 'the dashboard reports that a passcode is set');
    check(st.count('get') === 1 && st.count('create') === 1, 'all of that took one read and one create: the passcode is cached');

    // Two cold lambdas racing on first use: one create wins, the other reads that passcode back.
    const st2 = store(), a = site(env(), st2), b = site(env(), st2);
    await Promise.all([a.post({ action: 'dashboard' }), b.post({ action: 'dashboard' })]);
    const kept = st2.docs.get('config/editPasscode').passcode; st2.attempted.forEach(v => secrets.add(v.passcode));
    const [ra, rb] = await Promise.all([a.post({ action: 'kill' }, { 'x-edit-passcode': kept }), b.post({ action: 'resume' }, { 'x-edit-passcode': kept })]);
    check(st2.count('create') === 2 && st2.attempted[0].passcode === kept && st2.attempted[1].passcode !== kept && ra.statusCode === 200 && rb.statusCode === 200,
      'two lambdas creating at once: one document is written, and both accept its passcode');
    const loser = await b.post({ action: 'kill' }, { 'x-edit-passcode': st2.attempted[1].passcode });
    check(loser.statusCode === 401, 'the passcode that lost the race is never accepted');
  }

  // 3. The document exists but holds no passcode: reads only, and nothing is generated over it.
  for (const [label, seed] of [['an empty', { passcode: '', note: 'kept' }], ['a missing', { note: 'kept' }], ['a blank', { passcode: '   ' }]]) {
    const st = store({ 'config/editPasscode': seed }), s = site(env(), st);
    let r = await s.post({ action: 'dashboard' });
    check(r.statusCode === 200 && JSON.parse(r.body).editPasscodeSet === false, label + ' passcode: the dashboard still reads and reports that no passcode is set');
    r = await s.post({ action: 'kill' }, { 'x-edit-passcode': 'anything' });
    const j = JSON.parse(r.body);
    check(r.statusCode === 403 && j.code === 'EDIT_PASSCODE_NOT_SET' && j.error === UNSET && !st.docs.has('Brites_GAds_Control/control'), label + ' passcode: a change gets 403 EDIT_PASSCODE_NOT_SET and changes nothing');
    check(st.count('create') === 0 && JSON.stringify(st.docs.get('config/editPasscode')) === JSON.stringify(seed), label + ' passcode: the document is left exactly as it was');
  }

  // 4. Firestore failing: reads only, nothing generated, and the failure is not remembered.
  {
    const st = store(); st.fail('read'); const s = site(env(), st);
    let r = await s.post({ action: 'dashboard' });
    check(r.statusCode === 200 && JSON.parse(r.body).editPasscodeSet === false, 'Firestore unreachable: the dashboard still reads');
    r = await s.post({ action: 'kill' });
    check(r.statusCode === 403 && JSON.parse(r.body).code === 'EDIT_PASSCODE_NOT_SET' && st.count('create') === 0 && st.docs.size === 0, 'Firestore unreachable: changes are locked (403) and no passcode is generated');
    const direct = await s.kick.EP.resolve();
    check(direct.source === 'none' && direct.value === '' && /UNAVAILABLE/.test(direct.error), 'resolve() reports source "none" with the error');
    st.fail(null); st.docs.set('config/editPasscode', { passcode: 'rcvr-7777-back' }); secrets.add('rcvr-7777-back');
    r = await s.post({ action: 'kill' }, { 'x-edit-passcode': 'rcvr-7777-back' });
    check(r.statusCode === 200, 'once Firestore answers again the saved passcode works at once (a failure is not cached)');
  }
  {
    const st = store(); st.fail('create'); const s = site(env(), st);
    let r = await s.post({ action: 'kill' });
    st.attempted.forEach(v => secrets.add(v.passcode));
    check(r.statusCode === 403 && st.count('create') === 1 && !st.docs.has('config/editPasscode'), 'the document cannot be created: changes are locked (403) and nothing is stored');
    r = await s.post({ action: 'kill' }, { 'x-edit-passcode': st.attempted[0].passcode });
    st.attempted.forEach(v => secrets.add(v.passcode));
    check(r.statusCode === 403, 'a passcode that could not be saved is never accepted');
    const direct = await s.kick.EP.resolve();
    st.attempted.forEach(v => secrets.add(v.passcode));
    check(direct.source === 'none' && /PERMISSION_DENIED/.test(direct.error) && /\[passcode\]/.test(direct.error) && !st.attempted.some(v => direct.error.includes(v.passcode)),
      'the error names the failure but never the passcode, even when Firestore echoes the write');
  }

  // 5. A passcode changed in Firestore is used once the one-minute cache expires.
  {
    const st = store({ 'config/editPasscode': { passcode: 'old2-pass-2222' } }), s = site(env(), st);
    ['old2-pass-2222', 'new3-pass-4444', 'hook-5555-6666'].forEach(p => secrets.add(p));
    const kill = p => s.post({ action: 'kill' }, { 'x-edit-passcode': p });
    check((await kill('old2-pass-2222')).statusCode === 200, 'the saved passcode is accepted');
    st.docs.set('config/editPasscode', { passcode: 'new3-pass-4444' });
    now += 30000;
    let oldR = await kill('old2-pass-2222'), newR = await kill('new3-pass-4444');
    check(oldR.statusCode === 200 && newR.statusCode === 401 && st.count('get') === 1, 'within the minute the cached passcode still applies (no extra read)');
    now += 31000;
    newR = await kill('new3-pass-4444'); oldR = await kill('old2-pass-2222');
    check(newR.statusCode === 200 && oldR.statusCode === 401 && st.count('get') === 2, 'after the minute the changed passcode is used and the old one is refused');
    st.docs.set('config/editPasscode', { passcode: 'hook-5555-6666' }); s.kick.EP.resetCache();
    check((await kill('hook-5555-6666')).statusCode === 200, 'resetCache() drops the cached passcode at once');
  }

  // 6. EDIT_PASSCODE in Netlify still wins over Firebase, which is then never read.
  {
    const st = store({ 'config/editPasscode': { passcode: 'fbfb-fbfb-fbfb' } }), s = site({ ...env(), EDIT_PASSCODE: ' "envv-pass-1111" ' }, st);
    ['fbfb-fbfb-fbfb', 'envv-pass-1111'].forEach(p => secrets.add(p));
    const a = await s.post({ action: 'kill' }, { 'x-edit-passcode': 'fbfb-fbfb-fbfb' }), b = await s.post({ action: 'kill' }, { 'x-edit-passcode': 'envv-pass-1111' });
    check(a.statusCode === 401 && b.statusCode === 200 && st.log.filter(x => x[1] === 'config/editPasscode').length === 0, 'EDIT_PASSCODE wins: the Firebase passcode is ignored and never read');
  }

  // 7. The hourly kick reaches the worker with the server-only token, without touching the passcode.
  {
    const st = store({ 'config/editPasscode': { passcode: 'fire-base-9999' } }), s = site(env(), st); secrets.add('fire-base-9999');
    const token = s.kick.ctx.workerToken();
    const r = seen(await s.kick.api.handler({ headers: { 'x-nf-event': 'schedule' }, body: '' })), out = JSON.parse(r.body);
    check(/^internal-[a-f0-9]{64}$/.test(token) && token === s.worker.ctx.workerToken(), 'the kick and the worker derive the same server-only HMAC token');
    check(out.status === 'kicked' && out.upstream === 200 && s.trail.length === 1 && s.trail[0].out.status === 'ran' && s.trail[0].sent.token === token,
      'the scheduled kick reaches the worker with the HMAC token and the hourly tasks run');
    check(['anomalyCheck', 'monthlySpendGuard', 'enforceBudgetCeiling', 'uploadConversions'].every(n => s.calls.some(c => c[0] === n)), 'anomaly, monthly stop, ceiling and conversions ran');
    check(st.log.filter(x => x[1] === 'config/editPasscode').length === 0, 'the scheduled path never reads or creates the passcode');
    let w = seen(await s.worker.api.handler({ httpMethod: 'POST', headers: {}, body: JSON.stringify({ tasks: ['publishApproval'], id: 'd1', token: 'fire-base-9999' }) }));
    check(w.statusCode === 200 && s.calls.some(c => c[0] === 'applyApproval' && c[1] === 'd1'), 'the worker also accepts the Firebase passcode as its token');
    s.calls.length = 0;
    for (const t of [undefined, 'guess', 'internal-' + '0'.repeat(64)]) {
      w = seen(await s.worker.api.handler({ httpMethod: 'POST', headers: {}, body: JSON.stringify({ tasks: ['publishApproval'], id: 'd1', token: t }) }));
      assert.equal(w.statusCode, 401, 'worker refuses token ' + t);
    }
    check(!s.calls.some(c => c[0] !== 'control'), 'the worker refuses anonymous and guessed tokens (401) without doing anything');
    const q = await s.post({ action: 'runNow', tasks: ['conversions'] }, { 'x-edit-passcode': 'fire-base-9999' }), last = s.trail[s.trail.length - 1];
    check(q.statusCode === 200 && last.sent.token === token && last.status === 200 && last.out.status === 'ran', 'console-queued work carries the server token and the worker runs it');
  }

  // 8. The check pages (the shared _adsCheckGate) with the passcode in Firebase.
  await checkPages();

  // 9. The passcode is never in a response or a log line.
  secrets.delete(undefined);
  const leaked = [...secrets].filter(p => responses.some(r => JSON.stringify(r).includes(p)) || said.some(l => l.includes(p)));
  check(secrets.size >= 12 && !leaked.length, 'none of the ' + secrets.size + ' passcodes appears in any of the ' + responses.length + ' responses or ' + said.length + ' log lines');
  check(said.some(l => /created config\/editPasscode/.test(l)) && said.some(l => /failed/.test(l)), 'creation and failures are logged (without the passcode)');

  console.log('PASS ' + passed + ' edit-passcode checks (env wins, created once in Firestore, empty or failing Firestore locks changes, one-minute cache, never revealed, server token, check pages)');
})().catch(e => { console.error(e); process.exit(1); });

// The check pages run from their real files, with node-fetch and firebaseAdmin intercepted: every
// Google, Merchant and Shopify call answers an empty 200, and Firestore holds only the passcode.
async function checkPages() {
  const pass = 'chek-page-7890'; secrets.add(pass);
  let fetches = 0, reads = 0, passcode = pass;
  const work = () => fetches + reads; // anything a page does past the gate: a fetch or a Firestore read
  const reply = body => ({ ok: true, status: 200, json: async () => body, text: async () => JSON.stringify(body), headers: { get: () => null } });
  const stubFetch = async () => { fetches++; return reply({}); };
  const emptyQuery = { where() { return this; }, limit() { return this; }, orderBy() { return this; }, doc() { return this; }, collection() { return this; },
    get: async () => (reads++, { docs: [], forEach() {}, size: 0, empty: true, exists: false, data: () => ({}) }) };
  const passcodeRef = { get: async () => ({ exists: true, data: () => ({ passcode }) }), create: async () => { throw Error('the document exists'); } };
  const admin = { firestore: () => ({ collection: n => n === 'config' ? { doc: id => id === 'editPasscode' ? passcodeRef : emptyQuery } : emptyQuery, doc: () => emptyQuery,
    runTransaction: async fn => fn({ get: async () => ({ exists: false, data: () => ({}) }), update() {}, set() {} }) }) };
  const realResolve = Module._resolveFilename, realConsole = { ...console };
  Module._resolveFilename = function (request, parent, ...rest) {
    if (request === 'node-fetch') return 'STUB:node-fetch';
    if (/firebaseAdmin$/.test(request)) return 'STUB:firebaseAdmin';
    return realResolve.call(this, request, parent, ...rest);
  };
  require.cache['STUB:node-fetch'] = { id: 'STUB:node-fetch', filename: 'STUB:node-fetch', loaded: true, exports: stubFetch };
  require.cache['STUB:firebaseAdmin'] = { id: 'STUB:firebaseAdmin', filename: 'STUB:firebaseAdmin', loaded: true, exports: admin };
  const savedEnv = { ...process.env };
  delete process.env.EDIT_PASSCODE;
  Object.assign(process.env, { GADS_CLIENT_ID: 'x.apps.googleusercontent.com', GADS_CLIENT_SECRET: 'GOCSPX-x', GADS_REFRESH_TOKEN: '1//x', GADS_DEVELOPER_TOKEN: 'dev',
    GADS_CUSTOMER_ID: '1234567890', GADS_LOGIN_CUSTOMER_ID: '1234567890', FIREBASE_PRIVATE_KEY: 'k', FIREBASE_PROJECT_ID: 'p',
    SHOPIFY_STORE: 'x.myshopify.com', SHOPIFY_CLIENT_ID: 'c', SHOPIFY_CLIENT_SECRET: 's' });
  Object.assign(console, quietConsole);
  try {
    const EP = require(dir + '_editPasscode.js'); EP.resetCache();
    for (const file of ['googleAdsAuthCheck.js', 'googleAdsDiag.js', 'googleConnectionsCheck.js', 'googleMerchantHealth.js', 'shopifyAttributionCheck.js']) {
      const mod = require(dir + file), name = file.replace('.js', '');
      const f0 = work();
      const none = seen(await mod.handler({ queryStringParameters: {}, headers: {} }));
      const wrong = seen(await mod.handler({ httpMethod: 'POST', queryStringParameters: { key: 'nope' }, headers: { 'x-edit-passcode': 'nope' }, body: JSON.stringify({ passcode: 'nope' }) }));
      check(none.statusCode === 401 && wrong.statusCode === 401 && work() === f0, name + ': no or a wrong passcode gets 401 before anything is fetched or read');
      for (const [how, event] of [['?key=', { queryStringParameters: { key: pass, format: 'json' }, headers: {} }],
        ['the X-Edit-Passcode header', { queryStringParameters: { format: 'json' }, headers: { 'X-Edit-Passcode': pass } }],
        ['a body passcode', { httpMethod: 'POST', queryStringParameters: { format: 'json' }, headers: {}, body: JSON.stringify({ passcode: ' ' + pass + ' ' }) }]]) {
        const f1 = work(), res = seen(await mod.handler(event));
        check(res.statusCode === 200 && !/EDIT_PASSCODE_NOT_SET/.test(res.body) && work() > f1, name + ' accepts the Firebase passcode as ' + how + ' and runs');
      }
    }
    // An empty passcode in Firebase locks the check pages outright.
    passcode = ''; EP.resetCache();
    const f2 = work(), locked = seen(await require(dir + 'googleAdsDiag.js').handler({ queryStringParameters: { key: pass }, headers: {} }));
    check(locked.statusCode === 403 && JSON.parse(locked.body).code === 'EDIT_PASSCODE_NOT_SET' && JSON.parse(locked.body).error === 'Locked until a passcode is saved in Firebase (Firestore config/editPasscode)' && work() === f2,
      'an empty Firebase passcode locks the check pages (403) before anything is fetched');
  } finally {
    Module._resolveFilename = realResolve;
    Object.assign(console, realConsole);
    for (const k of Object.keys(process.env)) if (!(k in savedEnv)) delete process.env[k];
    Object.assign(process.env, savedEnv);
  }
}
