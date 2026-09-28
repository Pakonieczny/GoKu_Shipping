// Adversarial: a cancelled order must not get past a production station without the full-screen alert, and the open
// door (firebaseOrders {timeline}) must refuse what a station never sends. No network except loopback stubs.
//   1 · firebaseOrders: a sandbox event is never written to production, non-station types are refused, a request of
//       more than 100 events is refused, a flood from one sender is slowed (429), oversized details are cut
//   2 · weld-1.html with the real station module:
//       · a cancel check slower than 2.5 s: the scan goes on, and the late "cancelled" still raises the alert
//       · Complete Order on an order never scanned here: the guard asks the server, raises the alert, Stop sends nothing
//       · an order cancelled after its scan said "clear": Complete Order a minute later asks again and stops
//       · a 200 answer that is not an answer does not count as "clear" and does not wipe a known cancel
//   NODE_PATH=$(npm root -g) PW_DIR=$(npm root -g)/playwright/node_modules CHROMIUM=… node tests/stations/adv-stations.cjs
'use strict';
const fs = require('fs'), path = require('path'), assert = require('assert'), Module = require('module');
const root = path.join(__dirname, '../..');

/* ── 1 · the open door ─────────────────────────────────────────────────────────────────────────────────────────── */
async function door() {
  const docs = new Map();
  const ref = p => ({ path: p, id: p.split('/').pop(), get: async () => ({ exists: docs.has(p), id: p.split('/').pop(), data: () => docs.get(p) }) });
  const fakeDb = { collection: c => ({ doc: id => ref(c + '/' + id) }), batch() { const w = []; return { set(r, d) { w.push([r.path, d]); }, delete() {}, commit: async () => { for (const [p, d] of w) docs.set(p, Object.assign({}, docs.get(p) || {}, d)); } }; },
    getAll: async (...refs) => Promise.all(refs.map(r => r.get())) };
  const fakeAdmin = { firestore: Object.assign(() => fakeDb, { FieldValue: { serverTimestamp: () => 'ts', delete: () => null } }) };
  const realLoad = Module._load;
  Module._load = function (req, ...rest) { if (/[\/]firebaseAdmin(\.js)?$/.test(req)) return fakeAdmin; return realLoad.call(this, req, ...rest); };
  const fn = require(path.join(root, 'netlify/functions/firebaseOrders.js'));
  Module._load = realLoad;
  const post = (body, { sandbox, ip = '203.0.113.9' } = {}) => fn.handler({ httpMethod: 'POST', headers: { 'x-nf-client-connection-ip': ip }, body: JSON.stringify(body), queryStringParameters: sandbox ? { sandbox: '1' } : {} })
    .then(r => ({ status: r.statusCode, body: JSON.parse(r.body || '{}') }));
  const ev = (o = {}) => Object.assign({ orderId: '3521000100', type: 'scan', at: Date.now(), by: 'Tess', station: 'welding', device: 'weld-1', sandbox: false, id: 'x' + Math.random() }, o);
  const prod = () => [...docs.keys()].filter(k => k.startsWith('Order_Timeline/'));
  const sand = () => [...docs.keys()].filter(k => k.startsWith('Sandbox_Order_Timeline/'));

  let r = await post({ timeline: [ev({ id: 'sb1', sandbox: true })] });
  assert.strictEqual(r.status, 200);
  assert.deepStrictEqual(prod(), [], 'a sandbox event sent without ?sandbox=1 is not written to production');
  r = await post({ timeline: [ev({ id: 'p1', sandbox: false })] }, { sandbox: true });
  assert.deepStrictEqual(sand(), [], 'nor a production event into the sandbox');
  r = await post({ timeline: [ev({ id: 'sb2', sandbox: true })] }, { sandbox: true });
  assert.strictEqual(sand().length, 1, 'a sandbox event goes to the sandbox');
  r = await post({ timeline: [ev({ id: 'ok1' }), ev({ id: 'c1', type: 'cancelled' }), ev({ id: 'c2', type: 'cancelRestored' }), ev({ id: 'c3', type: 'removed' })] });
  assert.deepStrictEqual(prod().map(k => k.split('~')[1]), ['scan'], 'only station event types pass: ' + prod().join(', '));
  r = await post({ timeline: Array.from({ length: 101 }, () => ev()) });
  assert.strictEqual(r.status, 413, 'more than 100 events in one request is refused');
  r = await post({ timeline: [ev({ id: 'big', data: { blob: 'x'.repeat(50000) }, text: 'y'.repeat(5000), by: 'z'.repeat(500) })] });
  const big = docs.get(prod().find(k => /big$/.test(k)));
  assert(big && JSON.stringify(big).length < 3000 && big.data.note, 'oversized details are cut: ' + (big && JSON.stringify(big).length));
  let refusedAt = 0;
  for (let i = 1; i <= 10 && !refusedAt; i++) { r = await post({ timeline: Array.from({ length: 100 }, (_, j) => ev({ id: `f${i}-${j}` })) }, { ip: '198.51.100.7' }); if (r.status === 429) refusedAt = i; }
  assert(refusedAt && refusedAt <= 7, 'a flood from one sender is refused within a minute (request ' + refusedAt + ')');
  r = await post({ timeline: [ev({ id: 'other-ip' })] }, { ip: '198.51.100.8' });
  assert.strictEqual(r.status, 200, 'another sender is not held up');
  console.log('door: sandbox/production kept apart, station types only, ≤100 a request, flood slowed, details cut');
}

/* ── 2 · the Welding station ───────────────────────────────────────────────────────────────────────────────────── */
async function station() {
  const pwDir = process.argv[2] || process.env.PW_DIR || path.join(root, 'node_modules');
  const { chromium } = require(path.join(pwDir, 'playwright-core'));
  const ORIGIN = 'http://weld.test';
  const LATE = '3521000201', NOSCAN = '3521000202', AFTER = '3521000203', MALFORMED = '3521000204';
  const rec = { at: Date.now() - 3600e3, by: 'Paul', why: 'Buyer asked to cancel', source: 'sorter' };
  const st = { events: [], completes: 0, cancelled: new Set([LATE, NOSCAN, MALFORMED]), slow: new Set([LATE]), junk: new Set() };
  const fbStub = `window.firebase = (() => {
    window.__snaps = [];
    const snap = (exists, data) => ({ exists, data: () => data || {}, get: f => (data || {})[f] });
    const doc = (c, id) => ({ id, onSnapshot(cb) { window.__snaps.push({ c, id, cb }); try { cb(snap(false)); } catch (_) {} return () => {}; },
      set: async () => {}, update: async () => {}, get: async () => snap(false), collection: n => col(c + '/' + id + '/' + n) });
    const col = c => { const q = { doc: id => doc(c, id), where: () => q, orderBy: () => q, limit: () => q, add: async () => ({ id: 'x' }),
      onSnapshot(cb) { try { cb({ docs: [], empty: true, size: 0, forEach() {}, docChanges: () => [] }); } catch (_) {} return () => {}; },
      get: async () => ({ docs: [], empty: true, size: 0, forEach() {} }) }; return q; };
    const firestore = () => ({ collection: col });
    firestore.FieldValue = { delete: () => null, serverTimestamp: () => null, arrayUnion: () => null };
    return { initializeApp() {}, firestore, auth: () => ({ signInAnonymously: async () => ({}), onAuthStateChanged() {} }) };
  })();`;
  const mStub = `window.M = { AutoInit() {}, toast() {}, updateTextFields() {},
    Modal: { init() { return { open() {}, close() {} }; }, getInstance() { return { open() {}, close() {} }; } },
    FormSelect: { init() { return {}; }, getInstance() { return { getSelectedValues: () => [] }; } } };`;
  const json = (r, body, status = 200) => r.fulfill({ status, contentType: 'application/json', body: JSON.stringify(body) });
  const wait = ms => new Promise(r => setTimeout(r, ms));
  async function until(fn, what, ms = 9000) { const t0 = Date.now(); for (;;) { const v = await fn(); if (v) return v; if (Date.now() - t0 > ms) throw new Error('timed out waiting for ' + what); await wait(60); } }
  const ev = (type, id) => st.events.find(e => e.type === type && e.orderId === id);

  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  try {
    const ctx = await browser.newContext({ viewport: { width: 1400, height: 950 } });
    await ctx.route(/.*/, async r => {
      const u = new URL(r.request().url()), m = r.request().method();
      if (/code\.jquery\.com/.test(u.host)) return r.fulfill({ status: 200, contentType: 'text/javascript', body: fs.existsSync('/usr/share/javascript/jquery/jquery.min.js') ? fs.readFileSync('/usr/share/javascript/jquery/jquery.min.js') : 'window.$=window.jQuery=()=>({on(){},ready(){}});' });
      if (/materialize/.test(u.pathname)) return r.fulfill({ status: 200, contentType: /\.css$/.test(u.pathname) ? 'text/css' : 'text/javascript', body: /\.css$/.test(u.pathname) ? '' : mStub });
      if (/gstatic\.com/.test(u.host)) return r.fulfill({ status: 200, contentType: 'text/javascript', body: /firebase-app-compat/.test(u.pathname) ? fbStub : '' });
      if (u.origin !== ORIGIN) return r.abort();
      if (u.pathname.startsWith('/.netlify/functions/')) {
        const fn = u.pathname.split('/').pop();
        if (fn === 'firebaseOrders') {
          const id = u.searchParams.get('cancelCheck');
          if (id) {
            if (st.junk.has(id)) return r.fulfill({ status: 200, contentType: 'text/html', body: '<html>captive portal</html>' });
            if (st.slow.has(id)) await wait(3500);
            return json(r, { cancelled: st.cancelled.has(id) ? { [id]: rec } : {}, now: Date.now() });
          }
          if (m === 'POST') { const b = JSON.parse(r.request().postData() || '{}'); if (Array.isArray(b.timeline)) { st.events.push(...b.timeline); return json(r, { ok: true }); } return json(r, { success: true }); }
          return json(r, { success: true, data: {} });
        }
        if (fn === 'etsyOrderProxy') return json(r, { receipt_id: Number(u.searchParams.get('orderId')), status: 'Paid', transactions: [] });
        if (fn === 'trackOrderProxy') { st.completes++; return r.fulfill({ status: 200, contentType: 'text/plain', body: 'ok' }); }
        return json(r, {});
      }
      const file = path.join(root, decodeURIComponent(u.pathname));
      if (file.startsWith(root) && fs.existsSync(file) && fs.statSync(file).isFile()) return r.fulfill({ status: 200, path: file });
      return r.fulfill({ status: 404, body: 'not here' });
    });
    await ctx.addInitScript(() => {
      localStorage.setItem('access_token', 'tok'); localStorage.setItem('refresh_token', 'ref');
      localStorage.setItem('token_expires_at', String(Math.floor(Date.now() / 1000) + 7200));
      localStorage.setItem('employee_id', '123456'); localStorage.setItem('employee_name', 'Tess Welder');
    });
    const page = await ctx.newPage();
    await page.goto(ORIGIN + '/weld-1.html');
    await page.waitForFunction(() => window.StationTimeline && window.OrderTimeline, null, { timeout: 15000 });
    const enter = async id => { await page.fill('#etsyOrderNumber', id); await page.focus('#etsyOrderNumber'); await page.keyboard.press('Enter'); };
    const alertFor = id => page.evaluate(id => StationTimeline.alertOpen() && document.querySelector('.sttl-alert').innerText.includes('Order ' + id), id);
    const understood = async () => { await wait(500); await page.focus('.sttl-ok'); await page.keyboard.press('Enter'); await page.waitForFunction(() => !document.querySelector('.sttl-alert'), null, { timeout: 3000 }); };
    const complete = () => page.evaluate(() => { document.getElementById('trackingNumberInput').value = '9400100000000000000000'; document.getElementById('carrierSelect').value = 'usps'; document.getElementById('completeOrderBtn').click(); });

    // a slow check (3.5 s): the scan goes on unchecked, and the late "cancelled" still raises the alert
    await enter(LATE);
    await until(() => ev('scan', LATE), 'the slow scan');
    assert.strictEqual(ev('scan', LATE).data.check, 'unchecked');
    await until(() => alertFor(LATE), 'the late alert for a slow check');
    assert(await page.evaluate(id => !!StationTimeline.isCancelled(id), LATE), 'the late answer is kept for the guard');
    await understood();
    await until(() => ev('cancelAlert', LATE), 'Understood recorded');
    assert.strictEqual(ev('cancelAlert', LATE).by, 'Tess Welder'); assert.strictEqual(ev('cancelAlert', LATE).data.how, 'enter');

    // Complete Order on an order never scanned here (typed, no Enter): the guard asks, the alert shows, Stop sends nothing
    await page.fill('#etsyOrderNumber', NOSCAN);
    await complete(); await complete();                      // a double click
    await until(() => alertFor(NOSCAN), 'the alert from the guard');
    await page.waitForSelector('.sttl-alert .sttl-guard', { timeout: 3000 });
    assert.strictEqual(await page.evaluate(() => document.querySelectorAll('.sttl-bar, .sttl-guard').length), 1, 'one question, inside the alert');
    await page.click('.sttl-guard .sttl-btn.stop'); await wait(300);
    assert.strictEqual(st.completes, 0, 'a cancelled order is not completed on Etsy');
    await understood();

    // cancelled after its scan said clear: a minute later Complete Order asks again
    st.cancelled.delete(AFTER);
    await enter(AFTER);
    await until(() => ev('scan', AFTER) && ev('welded', AFTER), 'the clear scan');
    assert.strictEqual(ev('scan', AFTER).data.check, 'clear');
    st.cancelled.add(AFTER);
    await page.evaluate(() => { const real = Date.now.bind(Date); Date.now = () => real() + 90000; });
    await complete();
    await until(() => alertFor(AFTER), 'the alert for an order cancelled after its scan');
    await page.waitForSelector('.sttl-alert .sttl-guard', { timeout: 3000 });
    await page.click('.sttl-guard .sttl-btn.stop'); await wait(300);
    assert.strictEqual(st.completes, 0, 'not completed');
    await understood();

    // a known cancel, then a 200 answer that is no answer: still cancelled, still the alert
    await enter(MALFORMED);
    await until(() => alertFor(MALFORMED), 'the first alert');
    await understood();
    st.junk.add(MALFORMED);
    await enter(MALFORMED);
    await until(() => alertFor(MALFORMED), 'the alert after a junk answer');
    assert(await page.evaluate(id => !!StationTimeline.isCancelled(id), MALFORMED), 'the known cancel is kept');
    await understood();

    // a clear order is still never asked, and the page is not held up
    st.cancelled.delete('3521000299');
    const t0 = Date.now();
    assert.strictEqual(await page.evaluate(() => StationTimeline.guard('3521000299')), true);
    assert(Date.now() - t0 < 2000, 'a clear answer comes back quickly');
    console.log('station: late answer alerts, the guard asks the server (never scanned, cancelled since), junk answers keep the cancel');
  } finally { await browser.close(); }
}

(async () => { await door(); await station(); console.log('adv-stations: all passed'); })().catch(e => { console.error(e); process.exit(1); });
