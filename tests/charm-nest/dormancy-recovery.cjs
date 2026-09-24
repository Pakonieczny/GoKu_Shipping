// Dormancy and wake-up: after a sleep, a network drop, a function outage or a reload the sorter takes up its work again
// by itself, and nothing a person decided is undone (audit of 24 Sep, findings 1-11). The pieces that can be read on
// their own run in node's vm; the rest opens the sorter in headless Chromium, where every request that is not to
// 127.0.0.1 is aborted, the Netlify functions are answered by stubs and the Design Station is a loopback fake. Nothing
// live is contacted and nothing is paid for.
//   node tests/charm-nest/dormancy-recovery.cjs [playwright-core dir]
const http = require('http'), fs = require('fs'), path = require('path'), vm = require('vm'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');
const bridge = fs.readFileSync(path.join(root, 'charm-nest-bridge.js'), 'utf8'), page = fs.readFileSync(path.join(root, 'charm-nest-1.html'), 'utf8');
const part = (src, a, b) => { const i = src.indexOf(a); if (i < 0) throw new Error(`not found: ${a.trim()}`); const j = src.indexOf(b, i); if (j < 0) throw new Error(`not found: ${b.trim()}`); return src.slice(i, j); };
const results = [];
async function check(name, fn) { try { await fn(); results.push([name, null]); } catch (e) { results.push([name, e]); } }

/* ── node vm ── */
async function vmChecks() {
  await check('4 · the Etsy hour rolls over inside hourCalls', () => {
    const c = { E_: { window: [{ t: Date.now() - 10 * 3600000, n: 500 }, { t: Date.now() - 2 * 3600000, n: 100 }, { t: Date.now() - 1000, n: 3 }] } };
    vm.createContext(c); vm.runInContext(part(bridge, '  const hourCalls = ', '  const etsyCap = ') + ';this.hourCalls=hourCalls;', c);
    assert.equal(c.hourCalls(), 3, 'calls from before a sleep no longer count as this hour'); assert.equal(c.E_.window.length, 1, 'and are dropped');
  });
  await check('5 · a dead station frame is loaded again, a live one never', () => {
    const now = Date.now(), frameUrl = () => 'http://station.test/design-1.html?bridge=1';
    const c = { S_: null, navigator: { onLine: true }, agent() {}, frameUrl, Date };
    vm.createContext(c); vm.runInContext(part(bridge, '  function reloadFrame(why) {', '  async function open() {') + ';this.reloadFrame=reloadFrame;', c);
    const S = o => (c.S_ = Object.assign({ frame: { src: 'old' }, up: false, reloadedAt: 0, loadedAt: now - 60000, heardAt: 0 }, o));
    S({}); assert.equal(c.reloadFrame('test'), true, 'silent since it loaded (an error page): loaded again'); assert.equal(c.S_.frame.src, frameUrl());
    S({ heardAt: now - 10000 }); assert.equal(c.reloadFrame('test'), false, 'spoke since it loaded: alive, maybe busy with a person, never reloaded'); assert.equal(c.S_.frame.src, 'old');
    S({ loadedAt: now - 20 * 60000, heardAt: now - 6 * 60000 }); assert.equal(c.reloadFrame('test'), true, 'silent for over 5 minutes: loaded again');
    S({ reloadedAt: now - 10000 }); assert.equal(c.reloadFrame('test'), false, 'at most once in 45 s');
    S({ up: true }); assert.equal(c.reloadFrame('test'), false, 'a station that answers is left alone');
    c.navigator.onLine = false; S({}); assert.equal(c.reloadFrame('test'), false, 'not while the network is down');
    // every message from the station's origin says it is alive; others do not
    const m = { S_: { nonce: 'n', pending: new Map(), dropped: 0 }, origin: () => 'http://station.test', onEtsyConnected() {}, onEvent() {}, renderConsole() {}, logLine() {}, agentLive() {}, performance, clearTimeout };
    vm.createContext(m); vm.runInContext(part(bridge, '  function onMessage(ev) {', '  /* ── Connect Etsy') + ';this.onMessage=onMessage;', m);
    m.onMessage({ origin: 'http://elsewhere.test', data: { source: 'brites-design', nonce: 'n', id: 1 } }); assert.equal(m.S_.heardAt, undefined);
    m.onMessage({ origin: 'http://station.test', data: { source: 'brites-design', nonce: 'n', id: 7, type: 'ack' } }); assert(m.S_.heardAt > now - 1000, 'heard');
  });
  await check('7 · a back file the cloud could not take keeps its approval', async () => {
    const timers = [], reviews = []; let settled = 0;
    const c = { Promise, Map, Set, Object, Error, console, B: { run: null }, render() {}, refreshBacks() {}, agent() {}, Orders: { render() {} }, Session: { schedule() {} }, Review: { add: x => reviews.push(x) },
      RunCtl: { backgroundSettled: () => { settled++; } }, setTimeout: (f, ms) => timers.push(ms), resumeBacks() {}, writeBacks: async () => { throw new Error('Failed to fetch'); } };
    vm.createContext(c); vm.runInContext(part(bridge, '  async function saveBacks(job) {', '  /** A reload while an approval') + ';this.saveBacks=saveBacks;', c);
    const job = { key: 'k', state: 'approved', approvedAt: 1, approvedBy: 'Paul', backs: [], copies: ['p'], row: { order: { receiptId: 'o' }, engrave: { state: 'approved', approved: true } } };
    await c.saveBacks(job);
    assert.equal(job.state, 'approved', 'the approval stands'); assert.equal(job.row.engrave.approved, true); assert.match(job.backPending || '', /Failed to fetch/, 'the back file waits for the cloud');
    assert.equal(reviews.length, 0, 'nothing goes back to Review'); assert.equal(timers.length, 1, 'and it is tried again later'); assert.equal(settled, 0, 'the run is not woken for nothing');
    c.writeBacks = async j => { j.backs.push({ poolId: 'p' }); j.state = 'written'; };
    await c.saveBacks(job); assert.equal(job.state, 'written'); assert.equal(job.backPending, undefined, 'written once the cloud is back');
    job.state = 'approved'; job.backs = []; c.writeBacks = async () => { throw new Error('the written back file did not re-verify (mirror)'); };
    await c.saveBacks(job); assert.equal(job.state, 'review', 'a file that fails its own checks still goes to review'); assert.equal(reviews.length, 1);
  });
  await check('8 · a failed new-orders check is made again soon, keeps its snapshot, waits out a wake', async () => {
    const c = { console, __listeners: {}, __intervals: [] }; vm.createContext(c);
    vm.runInContext(`
      const __realNow = Date.now; let __clock = null; Date.now = () => __clock ?? __realNow.call(Date);
      const window = { addEventListener: (t, f) => (__listeners[t] ||= []).push(f) };
      const document = { hidden: false, addEventListener() {}, getElementById: () => null, querySelector: () => null, querySelectorAll: () => [], body: { appendChild() {} } };
      const el = () => ({ style: {}, classList: { toggle() {} }, setAttribute() {}, removeAttribute() {} });
      const storage = new Map(), localStorage = { getItem: k => storage.get(k), setItem: (k, v) => storage.set(k, v) }, navigator = {};
      const S = { settings: { pollMinutes: 10, runMode: 'manual' }, cloud: { ok: true }, mode: 'nest' }, WORKSPACE_SANDBOX = false;
      const B = { run: null, orders: { rows: [], byKey: new Map() } }, allSheets = () => [];
      const Orders = { view: () => ({}), rows: () => B.orders.rows, render() {}, interpretAll() {}, claim: async () => {}, applyPullRule: x => x, loadMaps: async () => {} };
      const Recall = { state: () => ({}), on: () => false }, Engrave = { render() {}, background() {} }, Review = { render() {} }, Session = { schedule() {} }, ListMedia = { prepare() {} }, Master = { load: async () => {} };
      const RunCtl = { renderBanner() {}, save: async () => {}, poke() {}, stop() {}, start: async () => {}, clearRunState() {} };
      const O = { lineKey: (o, l) => o.receiptId + '/' + l.transactionId }, Sandbox = { on: () => false, streaming: () => false };
      const toast = () => {}, notifyPerson = () => {}, refreshAllCards = () => {}, agent = () => {}, setMode = () => {};
      let snapshots = 0, failCloud = false, failStation = null;
      const DesignLink = { ensure: async () => { if (failStation) throw new Error(failStation); }, etsyBudgetOk: () => true, meter() {},
        call: async () => { snapshots++; return { total: 1, hydrated: 1, openIds: ['1'], orders: [{ receiptId: '1', hydrated: true, updateTs: 1, lines: [{ transactionId: 't' }] }] }; } };
      const api = async () => { if (failCloud) throw new Error('Failed to fetch'); return { firstSeen: { 1: 1 }, count24: 1, count1: 1 }; };
      const setInterval = f => { __intervals.push(f); return __intervals.length; }, clearInterval = () => {};
    `, c);
    vm.runInContext(part(bridge, 'const Arrivals = window.Arrivals =', '/* A new batch'), c);
    const run = code => vm.runInContext(code, c), A = run('Arrivals'), left = () => run('Arrivals.state().nextCheck - Date.now()');
    run('failCloud = true'); await A.check();
    assert.equal(A.state().error, 'Failed to fetch'); assert(left() <= 16000, `tried again in 15 s, not ${Math.round(left() / 60000)} min`);
    run('Arrivals.state().nextCheck = Date.now() + 600000'); for (const f of c.__listeners.online || []) f();
    assert(left() <= 3100, 'the network back brings the check forward');
    run('failCloud = false'); await A.check();
    assert.equal(A.state().error, null); assert.equal(run('snapshots'), 1, 'the snapshot taken for the failed check is used again: no second Etsy sweep');
    assert.equal(run('B.orders.rows.length'), 1, 'its orders come in'); assert(left() > 590000, 'then the usual interval');
    run("failStation = 'the Design Station did not answer in time'"); await A.check(); const first = left(); await A.check(); const second = left();
    assert(first <= 16000 && second > first + 10000 && second <= 31000, `backs off 15 s, 30 s (${first}, ${second})`);
    run("failStation = 'Etsy hourly cap reached'"); await A.check(); assert(left() > 590000, 'a spent Etsy budget waits the whole interval');
    run('failStation = null'); A.start(); const tick = c.__intervals.at(-1), before = run('snapshots');
    run('__clock = Date.now()'); tick();
    run('__clock += 5 * 60000; Arrivals.state().nextCheck = __clock - 1'); tick();
    assert.equal(left(), 10000, 'a tick five minutes late (a wake) gives the network 10 s'); assert.equal(run('snapshots'), before, 'no check at the moment of waking');
  });
  await check('10 · a Claude job is read through failed polls', async () => {
    const remote = part(page, 'async function agentCallRemote(', 'function toBase64(');
    let polls = 0; const ops = [];
    const c = { performance: { now: () => 0 }, setTimeout: fn => fn(), fmt: { s: () => '' },
      api: async (_, b) => { ops.push(b.op); if (b.op === 'startAgent') return { id: 'job-1' }; if (++polls <= 3) throw new Error('Failed to fetch'); return { job: { status: 'done', result: { text: 'Emma' } } }; } };
    vm.createContext(c); vm.runInContext(remote, c);
    const r = await c.agentCallRemote('engraveIntent', {}, {}); assert.equal(r.text, 'Emma', 'the answer paid for is read'); assert.equal(ops.filter(x => x === 'startAgent').length, 1, 'one job');
    c.api = async (_, b) => { if (b.op === 'startAgent') return { id: 'job-2' }; throw Object.assign(new Error('job not found'), { status: 404 }); };
    await assert.rejects(c.agentCallRemote('engraveIntent', {}, {}), /job not found/, 'a 4xx is an answer');
  });
  await check('11 · a set selection that failed to save is tried again, and is not carried across a reload', async () => {
    let fail = false, assembled = 0, saves = 0;
    const c = { window: {}, Promise, Error, console, allSheets: () => [], refreshMembership() {}, RunCtl: { save: async () => { saves++; } }, assemble: async () => { if (fail) throw new Error('Failed to fetch'); assembled++; } };
    vm.createContext(c); vm.runInContext(part(bridge, '  const R = { lastReleased', '  const O_ = window.CharmNestOrders;') + part(bridge, '  async function flush(run) {', '  function changed() {') + ';this.R=R;this.flush=flush;', c);
    const run = { runId: 'r' };
    c.R.membershipError = 'Failed to fetch'; c.R.membershipRun = 'r'; c.R.membershipPending = true;
    assert.deepEqual(Object.keys(c.R).filter(k => /^membership/.test(k)), [], 'kept out of the saved workspace'); assert(!/membership/.test(JSON.stringify(c.R)));
    await c.flush(run); assert.equal(c.R.membershipError, null, 'the retry saved it'); assert.equal(assembled, 1); assert.equal(saves, 1);
    c.R.membershipError = 'HTTP 503'; fail = true;
    await assert.rejects(c.flush(run), /Set selection not saved: Failed to fetch/, 'a retry that fails says the fresh reason'); assert.equal(run.membershipDirty, true, 'and the save stays owed');
  });
}

/* ── headless Chromium ── */
const STATION = `<!doctype html><meta charset="utf-8"><title>fake station</title><body>fake station<script>
let nonce = null, origin = null, master = null; window.__signedIn = true;
const CMDS = {
  hello(a, ev) { nonce = ev.data.nonce; origin = ev.origin; master = ev.source; return { sandbox: false, bench: 'design-1', version: 'fake', employee: '', counts: { open: 0, hydrated: 0, selected: 0, completed: 0, locked: 0, claimed: 0 }, selection: [], filters: {}, etsy: { signedIn: window.__signedIn, meter: null }, commands: Object.keys(CMDS) }; },
  ping() { return { pong: true }; }, 'feed.post'() { return {}; }, 'etsy.connect'() { return { signedIn: window.__signedIn, url: location.origin + '/design-1.html?connect=1' }; },
  claim(a) { return { claimed: a.receiptIds || [] }; }, unclaim() { return {}; }, release() { return {}; },
  'orders.snapshot'() { return { total: 0, hydrated: 0, orders: [], openIds: [], etsyCalls: 0 }; }, 'orders.check'() { return { orders: {}, swept: true, etsyCalls: 0 }; } };
addEventListener('message', async ev => { const d = ev.data; if (!d || d.source !== 'brites-sorter') return;
  const reply = m => ev.source.postMessage(Object.assign({ source: 'brites-design', nonce: d.nonce }, m), ev.origin); reply({ id: d.id, type: 'ack' });
  const fn = CMDS[d.type]; if (!fn) return reply({ id: d.id, type: 'error', error: 'unknown ' + d.type });
  try { reply({ id: d.id, type: 'done', result: await fn(d.args || {}, ev) }); } catch (e) { reply({ id: d.id, type: 'error', error: e.message }); } });
window.__emit = (type, p) => { if (master) master.postMessage(Object.assign({ source: 'brites-design', nonce, id: 0, type }, p || {}), origin); return !!master; };
</script>`;
const MIME = { '.html': 'text/html', '.js': 'text/javascript', '.css': 'text/css', '.json': 'application/json', '.otf': 'font/otf', '.ttf': 'font/ttf', '.png': 'image/png' };
const listen = handler => new Promise(ok => { const s = http.createServer(handler).listen(0, '127.0.0.1', () => ok(s)); });
const SHEETS = n => Array.from({ length: n }, (_, i) => ({ id: 's' + i, sheetId: 's' + i, metal: 'gold', metalLabel: 'GF 14/20', day: '2026-09-23', placedCount: 10, charmCount: 10, density: .7, folder: `GF_Sep.23.26_Set-${1 + (i >> 2)}_Sheet-${1 + (i & 3)}`, fileBase: `GF_Sep.23.26_Set-${1 + (i >> 2)}_Sheet-${1 + (i & 3)}`, updatedAt: Date.now() - i, poolIds: [], page: 1 + (i & 3) }));
function reply(fn, b, sheets) {
  if (fn === 'authGate') return { locked: false };
  if (fn !== 'charmNestLibrary') return null;
  if (b.op === 'runGet') return { run: b.runId ? { runId: b.runId, status: 'running' } : null };   // the run is on record, open
  return ({ ping: { sheets: 2, charms: 5, calibration: [] }, optionMapGet: { maps: {} }, aliasGet: { aliases: {} }, noDesignGet: { list: { patterns: [], skus: [], rows: [] } }, masterList: { entries: [] }, masterListFiles: { files: [] },
    runList: { runs: [] }, listSheets: { sheets }, listCharms: { charms: [{ hash: 'h1', name: 'Heart' }, { hash: 'h2', name: 'Star' }] }, arrivalRecord: { firstSeen: {}, count24: 0, count1: 0 }, laserStatus: { sheets: [], sets: [] }, history: { sets: [] } })[b.op] || { ok: true };
}
async function openSorter(browser, servers, opts = {}) {
  const [sorter, station] = servers, A = `http://127.0.0.1:${sorter.address().port}`, St = `http://127.0.0.1:${station.address().port}`;
  const context = await browser.newContext({ viewport: { width: 1400, height: 900 } }), calls = [], errors = [];
  const ctl = { fail: () => false, sheets: opts.sheets || SHEETS(2) };
  await context.route(u => !u.href.startsWith('http://127.0.0.1'), r => r.abort());
  await context.route(u => u.href.includes('/.netlify/functions/'), async r => {
    const fn = new URL(r.request().url()).pathname.split('/').pop(); let b = {}; try { b = JSON.parse(r.request().postData() || '{}'); } catch (_) {}
    calls.push({ fn, op: b.op, t: Date.now() });
    if (ctl.fail(fn, b)) return r.abort('internetdisconnected');
    const out = reply(fn, b, ctl.sheets); if (out == null) return r.fulfill({ status: 404, contentType: 'application/json', body: '{"error":"stub"}' });
    return r.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify(out) });
  });
  const settings = { v: 26, dsOrigin: St, runMode: 'manual', sound: 'off', notify: 'off', review: 'on' };
  await context.addInitScript(s => { try { if (!sessionStorage.getItem('__seeded')) { localStorage.setItem('cn.settings', JSON.stringify(s)); sessionStorage.setItem('__seeded', '1'); } } catch (_) {} window.__cloudBack = 0; addEventListener('cn-cloud-back', () => window.__cloudBack++); }, settings);
  if (opts.before) opts.before(ctl);
  const p = await context.newPage(); p.on('pageerror', e => errors.push(e.message));
  await p.goto(A + '/charm-nest-1.html');
  // (the bridge is booted, and its workspace listening, once the orders counter is up)
  await p.waitForFunction(() => window.CN && CN.S.cloud.ok !== null && window.RunCtl && window.DesignLink && window.Session && window.Arrivals && window.Dock, null, { timeout: 60000 });
  await p.waitForTimeout(300);
  return { page: p, context, ctl, calls, errors, station: () => p.frames().find(f => /design-1\.html/.test(f.url())) };
}
async function chromiumChecks() {
  const pwDir = process.argv[2] || process.env.PW_DIR || path.join(root, 'node_modules');
  const { chromium } = require(path.join(pwDir, 'playwright-core'));
  const sorter = await listen((req, res) => {
    const u = decodeURIComponent(req.url.split('?')[0]), f = path.join(root, u === '/' ? 'charm-nest-1.html' : u);
    if (!f.startsWith(root) || !fs.existsSync(f) || fs.statSync(f).isDirectory()) { res.writeHead(404); return res.end(); }
    res.writeHead(200, { 'Content-Type': MIME[path.extname(f)] || 'application/octet-stream', 'Cache-Control': 'no-store' }); fs.createReadStream(f).pipe(res);
  });
  const station = await listen((req, res) => { res.writeHead(200, { 'Content-Type': 'text/html', 'Cache-Control': 'no-store' }); res.end(STATION); });
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  const servers = [sorter, station];
  try {
    await check('2 · a failed first probe does not latch "Cloud offline"', async () => {
      let down = true; const h = await openSorter(browser, servers, { before: ctl => { ctl.fail = (fn, b) => down && fn === 'charmNestLibrary' && b.op === 'ping'; } });
      try {
        assert.equal(await h.page.evaluate(() => CN.S.cloud.ok), false, 'offline at load');
        down = false;
        await h.page.waitForFunction(() => CN.S.cloud.ok === true && window.__cloudBack > 0, null, { timeout: 12000 }).catch(() => { throw new Error('still "Cloud offline" 12 s after the network came back'); });
        assert.match(await h.page.evaluate(() => document.querySelector('#cloudText').textContent), /Library/);
      } finally { await h.context.close(); }
    });
    await check('1, 3 · sign-in and passing stops: Auto carries on, the operator\'s Stop is kept', async () => {
      const h = await openSorter(browser, servers);
      try {
        await h.page.evaluate(async () => {
          await DesignLink.ensure(); CN.S.settings.runMode = 'auto';
          B.run = { runId: 'run-2026-09-24-signin', day: '2026-09-24', status: 'processed', step: 'complete', mode: 'auto', processingComplete: true, errors: [], lines: {}, sheets: {}, holds: {}, orders: [], startedAt: Date.now(), updatedAt: Date.now() };
        });
        // 1 · a sign-in notice while the run rests between updates leaves it resting
        assert.equal(await h.station().evaluate(() => window.__emit('etsy.signin', { reason: 'refresh-failed' })), true);
        await h.page.waitForTimeout(500);
        assert.equal(await h.page.evaluate(() => B.run.status), 'processed', 'a resting run is not turned into a stopped one');
        // 1 · stopped for the sign-in, the station signed in again by itself: Connect Etsy carries the run on
        const connected = await h.page.evaluate(async () => {
          B.run.status = 'running'; RunCtl.stopIfRunning('the Design Station is not signed in to Etsy', 'Press Connect Etsy', 'auth');
          const kind = RunCtl.stopKind(B.run); await DesignLink.connectEtsy();
          for (let i = 0; i < 40 && B.run.status === 'stopped'; i++) await new Promise(z => setTimeout(z, 100));
          return { kind, status: B.run.status };
        });
        assert.equal(connected.kind, 'auth'); assert.notEqual(connected.status, 'stopped', 'Connect Etsy on a signed-in station resumes the run');
        // 3 · a passing failure: its kind, the banner, and Auto's own resume
        const transient = await h.page.evaluate(async () => {
          B.run = Object.assign({}, B.run, { runId: B.run.runId + '-t', status: 'running', step: 'complete', errors: [] });
          RunCtl.stop('New orders could not be added: Failed to fetch', 'Resume after fixing the cause. Your earlier work is kept.');
          const r = { kind: RunCtl.stopKind(B.run), banner: document.getElementById('runBanner').textContent };
          r.resumed = await RunCtl.autoResume(true); r.status = B.run.status; return r;
        });
        assert.equal(transient.kind, 'transient'); assert.match(transient.banner, /Auto carries on by itself/, 'the banner says Auto will carry on'); assert.equal(transient.resumed, true); assert.notEqual(transient.status, 'stopped');
        const operator = await h.page.evaluate(async () => {
          B.run = Object.assign({}, B.run, { runId: B.run.runId + '-o', status: 'running', step: 'complete', errors: [] }); RunCtl.operatorStop();
          return { kind: RunCtl.stopKind(B.run), resumed: await RunCtl.autoResume(true), status: B.run.status, banner: document.getElementById('runBanner').textContent };
        });
        assert.equal(operator.kind, 'operator'); assert.equal(operator.resumed, false); assert.equal(operator.status, 'stopped', 'the operator\'s Stop is never lifted by Auto'); assert.doesNotMatch(operator.banner, /Auto carries on/);
        const watchdog = await h.page.evaluate(async () => {
          B.run = Object.assign({}, B.run, { runId: B.run.runId + '-w', status: 'running', step: 'complete', errors: [] });
          DesignLink._E.station = { braked: true, brakeUntil: Date.now() + 60000 }; RunCtl.stop('Etsy watchdog: burst', 'wait', null, 'watchdog');
          const during = await RunCtl.autoResume(true); DesignLink._E.station = { braked: false, brakeUntil: Date.now() - 1 };
          return { during, after: await RunCtl.autoResume(true), status: B.run.status };
        });
        assert.equal(watchdog.during, false, 'not while the station\'s brake holds'); assert.equal(watchdog.after, true, 'and once it has lifted'); assert.notEqual(watchdog.status, 'stopped');
        assert.deepEqual(h.errors, []);
      } finally { await h.context.close(); }
    });
    await check('6 · a reload in Auto takes the station again and carries on', async () => {
      const h = await openSorter(browser, servers);
      try {
        await h.page.evaluate(async () => {
          B.run = { runId: 'run-2026-09-24-reload', day: '2026-09-24', status: 'running', step: 'engrave', mode: 'auto', errors: [], lines: {}, sheets: {}, holds: {}, orders: [], startedAt: Date.now(), updatedAt: Date.now(), cloudSavedAt: Date.now() };
          CN.S.settings.runMode = 'auto'; saveSettings(); await Session.flush();
        });
        await h.page.reload();
        await h.page.waitForFunction(() => window.CN && CN.S.cloud.ok !== null && window.RunCtl && window.B && B.run, null, { timeout: 60000 });
        const ok = await h.page.waitForFunction(() => B.run && B.run.stoppedBy !== 'Workspace restored after refresh' && document.getElementById('dsFrame') && DesignLink.up(), null, { timeout: 15000 }).then(() => true, () => false);
        const state = await h.page.evaluate(() => ({ runId: B.run && B.run.runId, restored: !!(B.run && B.run.workspaceRestored), status: B.run && B.run.status, stoppedBy: B.run && B.run.stoppedBy, station: !!document.getElementById('dsFrame') }));
        assert.equal(state.runId, 'run-2026-09-24-reload', 'the run came back from the workspace'); assert.equal(state.restored, true);
        assert(ok, `still ${state.status}: ${state.stoppedBy} · station mounted ${state.station}`);
      } finally { await h.context.close(); }
    });
    await check('9 · Library and Charms: shown where they were, kept when a refresh fails', async () => {
      const h = await openSorter(browser, servers, { sheets: SHEETS(300) });
      try {
        const listed = () => h.calls.filter(c => c.op === 'listSheets').length;
        await h.page.evaluate(() => CN.setMode('library'));
        await h.page.waitForFunction(() => document.querySelectorAll('#libBody .libCard').length >= 300, null, { timeout: 20000 });
        const place = () => h.page.evaluate(() => { const st = document.getElementById('stage'), top = st.getBoundingClientRect().top; const c = [...document.querySelectorAll('#libBody .libCard')].find(n => n.getBoundingClientRect().bottom > top); return { scroll: st.scrollTop, id: c && c.dataset.id }; });
        await h.page.evaluate(() => { const st = document.getElementById('stage'); st.scrollTop = Math.round(st.scrollHeight / 2); });
        const before = await place(), n0 = listed();
        await h.page.evaluate(() => { window.__marker = document.querySelector('#libBody .libCard'); CN.setMode('nest'); CN.setMode('library'); });
        await h.page.waitForTimeout(500);
        assert.equal(listed(), n0, 'a list read under a minute ago is not read again'); assert.equal(await h.page.evaluate(() => window.__marker.isConnected), true, 'nor rebuilt');
        assert.deepEqual((await place()).id, before.id, 'the same card is at the top');
        await h.page.evaluate(() => { CN.S.library.loadedAt = Date.now() - 120000; CN.setMode('nest'); CN.setMode('library'); });
        await h.page.waitForFunction(() => !window.__marker.isConnected && document.querySelectorAll('#libBody .libCard').length >= 300, null, { timeout: 10000 });
        assert.equal(listed(), n0 + 1, 'an older list is refreshed behind it'); assert.equal((await place()).id, before.id, 'and the place is kept across the rebuild');
        h.ctl.fail = (fn, b) => fn === 'charmNestLibrary' && ['listSheets', 'listCharms'].includes(b.op);
        await h.page.evaluate(() => { CN.S.library.loadedAt = 0; CN.setMode('nest'); CN.setMode('library'); });
        await h.page.waitForFunction(() => document.querySelector('#libBody .libStale'), null, { timeout: 10000 }).catch(() => {});
        const failed = await h.page.evaluate(() => ({ cards: document.querySelectorAll('#libBody .libCard').length, stale: document.querySelector('#libBody .libStale')?.textContent || '', text: document.querySelector('#libBody').textContent.slice(0, 80) }));
        assert.equal(failed.cards, 300, `the list stays on screen when its refresh fails (${failed.text})`); assert.match(failed.stale, /not refreshed.*Retry/);
        h.ctl.fail = () => false;
        await h.page.evaluate(() => CN.setMode('charms')); await h.page.waitForFunction(() => document.querySelectorAll('#charmsBody .charmTile').length === 2, null, { timeout: 10000 });
        h.ctl.fail = (fn, b) => fn === 'charmNestLibrary' && b.op === 'listCharms';
        const seen = await h.page.evaluate(async () => { const body = document.getElementById('charmsBody'), seen = []; const mo = new MutationObserver(() => seen.push(body.querySelectorAll('.charmTile').length)); mo.observe(body, { childList: true, subtree: true }); CN.setMode('nest'); CN.setMode('charms'); await new Promise(z => setTimeout(z, 1000)); mo.disconnect(); return { seen, tiles: body.querySelectorAll('.charmTile').length }; });
        assert.equal(seen.tiles, 2, 'the Charms grid stays when its refresh fails'); assert(!seen.seen.includes(0), 'and is never blanked');
      } finally { await h.context.close(); }
    });
  } finally { await browser.close(); sorter.close(); station.close(); }
}

(async () => {
  await vmChecks();
  await chromiumChecks().catch(e => results.push(['chromium', e]));
  for (const [name, e] of results) console.log(`${e ? 'FAIL' : 'ok  '} ${name}${e ? ' — ' + String(e.message || e).split('\n')[0] : ''}`);
  const failed = results.filter(r => r[1]);
  if (failed.length) { process.exitCode = 1; console.error(`${failed.length} of ${results.length} dormancy checks failed`); }
  else console.log(`Dormancy recovery OK: ${results.length} checks`);
})();
