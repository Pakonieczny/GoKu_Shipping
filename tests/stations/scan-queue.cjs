// Phone scans while nobody is signed in (weld-1, assembly-1, shipping-1 and design-message, which carry the same relay; the other
// copies, assembly-2..4, shipping-2..3 and design-message-1, are checked below only for the same wiring). The real pages, the real
// station-session.js, station-activity.js, station-timeline.js (where the page has it) and station-scan-queue.js run in headless Chromium; Firebase, Materialize and every Netlify function are
// fakes, and every request that is not to 127.0.0.1 is aborted. Fake PINs, made up when the test runs: they are only ever typed
// on the page and sent to the fake login door, and no message of this test can print one.
//   1 · the relay listener is attached ONCE at page load, with nobody signed in
//   2 · scans that arrive while nobody is signed in are KEPT (memory + sessionStorage: order numbers only), shown as one small
//       note ("N phone scans waiting for a sign-in", no toast, never blocks a click), not loaded, not recorded; a repeat of
//       the newest scan is ignored
//   3 · they survive a reload of the page
//   4 · after the next sign-in they load ONCE each, in the order they arrived, recorded under the person who signed in
//   5 · sign out, a scan, a sign-in by somebody else, and more re-logins: still one listener, nothing processed twice, and
//       the late scan is recorded under the person who processed it
//   6 · where the page has the red CANCELLED alert (weld, assembly, shipping), it still holds the next scan until Understood,
//       and none is lost
//   NODE_PATH=$(npm root -g) PW_DIR=$(npm root -g)/playwright/node_modules CHROMIUM=... node tests/stations/scan-queue.cjs
'use strict';
const http = require('http'), fs = require('fs'), path = require('path'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');
const wait = ms => new Promise(r => setTimeout(r, ms));
async function until(fn, what, ms = 12000) {
  const t0 = Date.now();
  for (;;) { const v = await fn(); if (v) return v; if (Date.now() - t0 > ms) throw new Error('timed out waiting for ' + what); await wait(60); }
}
const rnd = () => String(100000 + Math.floor(Math.random() * 900000)).replace(/^(\d)\1{5}$/, '482913');
const PIN_A = rnd(), PIN_B = (() => { let p; do p = rnd(); while (p === PIN_A); return p; })();
const NAME_A = 'Marco R.', NAME_B = 'Tess W.';
const CANC = '3822000099';

const FIREBASE = `(function () {
  const snaps = window.__fbSnaps = {}; window.__sets = [];
  const empty = () => ({ exists: false, data: () => undefined, docs: [], empty: true, size: 0, forEach() {}, docChanges: () => [] });
  function ref(p) {
    const r = { path: p, id: p.split('/').pop(), collection: n => ref(p + '/' + n), doc: n => ref(p + '/' + n),
      where: () => r, orderBy: () => r, limit: () => r, limitToLast: () => r, startAfter: () => r,
      onSnapshot(cb) { (snaps[p] = snaps[p] || []).push(cb); setTimeout(() => { try { cb(empty()); } catch (_) {} }, 0); return () => {}; },
      get: async () => empty(), set: async d => { window.__sets.push({ path: p, data: d }); }, update: async () => {}, add: async () => ref(p + '/new'), delete: async () => {} };
    return r;
  }
  const firestore = () => ({ collection: n => ref(n), doc: p => ref(p), batch: () => ({ set() {}, update() {}, delete() {}, commit: async () => {} }) });
  firestore.FieldValue = { delete: () => ({ __delete: true }), serverTimestamp: () => ({}), arrayUnion: (...a) => a, increment: n => n };
  firestore.Timestamp = { now: () => ({ toDate: () => new Date(), toMillis: () => Date.now() }) };
  const auth = () => ({ signInAnonymously: async () => ({}), onAuthStateChanged(cb) { setTimeout(() => cb({ uid: 'anon' }), 0); return () => {}; }, currentUser: { uid: 'anon' } });
  window.firebase = { apps: [], initializeApp() { return {}; }, firestore, auth, storage: () => ({ ref: () => ({}) }) };
})();`;
const MATERIALIZE = `window.__toasts = []; window.M = { AutoInit() {}, updateTextFields() {}, toast(o) { window.__toasts.push(String((o && o.html) || '')); },
  Modal: { init(el) { const i = { open() {}, close() {}, isOpen: false }; if (el) el.__m = i; return i; }, getInstance(el) { return (el && el.__m) || { open() {}, close() {} }; } },
  FormSelect: { init(el) { const i = { destroy() {}, getSelectedValues: () => [el && el.value] }; if (el) el.__fs = i; return i; }, getInstance(el) { return el && el.__fs; } },
  Dropdown: { init() {} } };`;
const MIME = { '.html': 'text/html', '.js': 'text/javascript', '.css': 'text/css' };

const results = [];
async function check(name, fn) { try { await fn(); results.push([name, null]); } catch (e) { results.push([name, e]); } }

async function scenario(browser, base, P) {
  const events = [], timeline = [], errors = [];
  const context = await browser.newContext({ viewport: { width: 1400, height: 900 } });
  await context.route(() => true, r => r.abort());                                     // nothing leaves the machine
  await context.route(u => u.href.startsWith('http://127.0.0.1'), r => r.continue());
  await context.route(u => /gstatic\.com\/firebasejs\//.test(u.href), r => r.fulfill({ contentType: 'text/javascript', body: /firebase-app-compat/.test(r.request().url()) ? FIREBASE : '' }));
  await context.route(u => /materialize/.test(u.href), r => r.fulfill({ contentType: /\.css/.test(r.request().url()) ? 'text/css' : 'text/javascript', body: /\.css/.test(r.request().url()) ? '' : MATERIALIZE }));
  await context.route(u => /code\.jquery\.com|qz-tray/.test(u.href), r => r.fulfill({ contentType: 'text/javascript', body: '' }));
  await context.route(u => u.href.startsWith('http://127.0.0.1') && u.pathname.includes('/.netlify/functions/'), async r => {
    const req = r.request(), u = new URL(req.url()), fn = u.pathname.split('/').pop();
    const json = (o, status) => r.fulfill({ status: status || 200, contentType: 'application/json', body: JSON.stringify(o) });
    if (req.method() === 'POST') {
      let b = {}; try { b = JSON.parse(req.postData() || '{}'); } catch (_) {}
      if (fn === 'firebaseOrders' && b.pinLogin !== undefined) return json(b.pinLogin === PIN_A ? { ok: true, name: NAME_A } : b.pinLogin === PIN_B ? { ok: true, name: NAME_B } : { ok: false, error: 'not on the list' });
      if (fn === 'firebaseOrders' && Array.isArray(b.activity)) { events.push(...b.activity); return json({ success: true, written: b.activity.length, duplicate: 0, refused: 0, scrubbed: 0 }); }
      if (fn === 'firebaseOrders' && Array.isArray(b.timeline)) { timeline.push(...b.timeline); return json({ ok: true, ids: b.timeline.map(e => e.id) }); }
      return json({ success: true });
    }
    if (fn === 'etsyOrderProxy') {
      const id = u.searchParams.get('orderId');
      return json({ receipt_id: Number(id) || 0, status: 'Paid', transactions: [{ transaction_id: 91000 + (Number(id) % 1000), title: 'Custom Stud Earrings', quantity: 2, sku: 'ST-1', variations: [] }] });
    }
    if (fn === 'firebaseOrders' && u.searchParams.get('cancelCheck')) {
      const out = {};
      for (const id of u.searchParams.get('cancelCheck').split(',')) if (id === CANC) out[id] = { at: Date.now() - 3600e3, by: 'Sam', why: 'Buyer cancelled', source: 'sheet' };
      return json({ success: true, cancelled: out, now: Date.now() });
    }
    if (fn === 'firebaseOrders' && /employee/i.test(u.searchParams.get('orderId') || '')) return json({ success: false, error: 'closed' }, 401);   // the roster is never read
    if (fn === 'firebaseOrders') return json({ success: true, data: {} });
    return json({});
  });
  await context.addInitScript(() => {          // an Etsy token so the page does not start OAuth; nobody is signed in
    try { localStorage.setItem('access_token', 'test-token'); localStorage.setItem('refresh_token', 'ref'); localStorage.setItem('token_expires_at', String(Math.floor(Date.now() / 1000) + 86400)); } catch (_) {}
  });
  const page = await context.newPage();
  page.on('pageerror', e => errors.push(e.message));
  const ready = async () => {
    await page.goto(base + '/' + P.file);
    await page.waitForFunction(t => window.StationActivity && window.StationSession && (!t || window.StationTimeline) && window.StationScanQueue, P.timeline, { timeout: 20000 });
    await page.waitForTimeout(400);
  };
  const doc = 'Brites_Orders/' + P.relay;
  const phone = n => page.evaluate(([d, n]) => { const cbs = window.__fbSnaps[d]; cbs[cbs.length - 1]({ exists: true, data: () => ({ 'Order Number': n }) }); }, [doc, n]);
  const listeners = () => page.evaluate(d => (window.__fbSnaps[d] || []).length, doc);
  const signIn = async pin => {
    await page.evaluate(p => { const i = document.getElementById('employeeNumberInput'); i.dataset.raw = p; i.value = '******'; }, pin);
    await page.evaluate(() => document.getElementById('employeeLoginBtn').click());
    await page.waitForFunction(() => window.isEmployeeLoggedIn === true && window.StationActivity.who(), null, { timeout: 10000 });
  };
  const signOut = async () => {
    await page.evaluate(() => document.getElementById('signOutBtn').click());
    await page.waitForFunction(() => window.isEmployeeLoggedIn === false, null, { timeout: 5000 });
  };
  const flush = () => page.evaluate(() => window.StationActivity.flush());
  const scans = id => events.filter(e => e.device === P.device && e.action === 'scan' && e.orderId === id);
  const note = () => page.evaluate(() => { const n = document.getElementById('stationScanQueueNote'); return n && n.style.display !== 'none' ? { text: n.textContent, role: n.getAttribute('role'), pe: getComputedStyle(n).pointerEvents } : null; });
  const stored = () => page.evaluate(k => sessionStorage.getItem(k), 'stationScanQueue.v1.' + P.device);
  const loaded = async id => { await until(async () => { await flush(); return scans(id).length >= 1; }, 'the scan of ' + id + ' on ' + P.device); };
  const A = P.id(1), B = P.id(2), C = P.id(3), D = P.id(4), E = P.id(5);
  const tag = s => `${P.file}: ${s}`;

  await ready();

  await check(tag('the relay listener is attached once at page load, with nobody signed in'), async () => {
    assert.equal(await page.evaluate(() => window.isEmployeeLoggedIn), false);
    assert.equal(await listeners(), 1);
  });

  await check(tag('scans with nobody signed in are kept, shown as one small note, not loaded, not recorded'), async () => {
    await phone(A); await phone(B); await phone(B);             // a repeat of the newest scan is ignored
    await page.waitForTimeout(700);
    const n = await note();
    assert.ok(n, 'the note is shown');
    assert.equal(n.text, '2 phone scans waiting for a sign-in');
    assert.equal(n.role, 'status'); assert.equal(n.pe, 'none', 'the note never blocks a click');
    const raw = await stored(), d = JSON.parse(raw);
    assert.deepEqual(d.items.map(i => i.n), [A, B], 'kept in arrival order');
    assert.deepEqual(Object.keys(d).sort(), ['items', 'v']); assert.ok(d.items.every(i => Object.keys(i).sort().join() === 'at,n'), 'order numbers and times only');
    assert.ok(!raw.includes(PIN_A) && !raw.includes(PIN_B) && !raw.includes(NAME_A) && !raw.includes(NAME_B), 'no PIN and no person in the stored queue');
    assert.equal(await page.inputValue('#etsyOrderNumber'), '', 'nothing was loaded');
    await flush(); await page.waitForTimeout(300);
    assert.equal(events.length, 0, 'nothing recorded: nobody to credit yet');
    assert.equal(await page.evaluate(() => window.__toasts.filter(t => /scan/i.test(t)).length), 0, 'no pop-up for it');
    assert.equal(await page.evaluate(d => window.__sets.filter(s => s.path === d && s.data && s.data['Order Number'] && s.data['Order Number'].__delete).length, doc), 2, 'each kept scan is cleared from the relay once');
    assert.equal(await listeners(), 1);
  });

  await check(tag('the waiting scans survive a reload of the page'), async () => {
    await ready();
    assert.equal(await listeners(), 1);
    const n = await note();
    assert.ok(n && n.text === '2 phone scans waiting for a sign-in', 'the note is back: ' + JSON.stringify(n));
    assert.deepEqual(JSON.parse(await stored()).items.map(i => i.n), [A, B]);
  });

  await check(tag('after the next sign-in each waiting scan loads once, in arrival order, under that person'), async () => {
    await signIn(PIN_A);
    await loaded(A); await loaded(B);
    await until(async () => (await stored()) === null && !(await note()), 'the queue to empty');
    await page.waitForTimeout(1500); await flush();
    for (const id of [A, B]) {
      assert.equal(scans(id).length, 1, 'one scan record for ' + id);
      assert.equal(scans(id)[0].person, NAME_A); assert.equal(scans(id)[0].detail, 'phone scan');
    }
    assert.ok(scans(A)[0].seq < scans(B)[0].seq && scans(A)[0].at <= scans(B)[0].at, 'A before B');
    assert.equal(await page.inputValue('#etsyOrderNumber'), B, 'the newest is the one on screen');
    assert.equal(events.filter(e => e.action === 'scan').length, 2, 'nothing else recorded');
    assert.equal(await listeners(), 1);
  });

  await check(tag('re-login: still one listener, nothing twice, a late scan is credited to whoever processed it'), async () => {
    await signOut();
    assert.equal(await listeners(), 1);
    await phone(C); await page.waitForTimeout(500);
    const n = await note();
    assert.ok(n && n.text === '1 phone scan waiting for a sign-in', JSON.stringify(n));
    assert.equal(scans(C).length, 0);
    await signIn(PIN_B);                                           // somebody else signs in
    await loaded(C);
    assert.equal(await listeners(), 1, 'a second sign-in did not start a second listener');
    await phone(D);                                                // live scan while signed in
    await loaded(D);
    await signOut(); await signIn(PIN_B);                          // and again
    await signOut(); await signIn(PIN_A);
    await page.waitForTimeout(1800); await flush();
    assert.equal(await listeners(), 1, 'still one listener after three sign-ins');
    assert.equal(scans(C).length, 1); assert.equal(scans(C)[0].person, NAME_B, 'credited to the person who processed it');
    assert.equal(scans(D).length, 1); assert.equal(scans(D)[0].person, NAME_B);
    assert.equal(scans(A).length, 1); assert.equal(scans(B).length, 1);
    assert.equal(await note(), null); assert.equal(await stored(), null);
  });

  if (P.timeline) await check(tag('the red CANCELLED alert holds the next scan until Understood; none is lost'), async () => {
    await phone(CANC);
    await until(() => page.evaluate(() => !!document.querySelector('.sttl-ok')), 'the cancelled alert');
    await phone(E);
    await page.waitForTimeout(900);
    assert.equal(scans(E).length, 0, 'held behind the alert');
    const n = await note();
    assert.ok(n && n.text === '1 more phone scan waiting to load', JSON.stringify(n));
    await page.click('.sttl-ok');
    await loaded(E);
    await until(async () => !(await note()), 'the note to go');
    await page.waitForTimeout(1200); await flush();
    assert.equal(scans(E).length, 1); assert.equal(scans(CANC).length, 1);
    assert.equal(await stored(), null);
  });

  await check(tag('no page errors from the queue or the relay'), async () => {
    assert.deepEqual(errors.filter(m => /scan|queue|relay|Enter|onOrderEnter|processScanned|StationScanQueue/i.test(m)), []);
  });
  await context.close();
}

async function main() {
  const pwDir = process.env.PW_DIR || path.join(root, 'node_modules');
  const { chromium } = require(path.join(pwDir, 'playwright-core'));
  const server = await new Promise(ok => { const s = http.createServer((req, res) => {
    const f = path.join(root, decodeURIComponent(req.url.split('?')[0]));
    if (!f.startsWith(root) || !fs.existsSync(f) || fs.statSync(f).isDirectory()) { res.writeHead(404); return res.end(); }
    res.writeHead(200, { 'Content-Type': MIME[path.extname(f)] || 'application/octet-stream' }); fs.createReadStream(f).pipe(res);
  }).listen(0, '127.0.0.1', () => ok(s)); });
  const base = `http://127.0.0.1:${server.address().port}`;
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  try {
    await scenario(browser, base, { file: 'weld-1.html', device: 'weld-1', relay: 'weld-scan-1', timeline: true, id: n => '35223000' + String(n).padStart(2, '0') });
    await scenario(browser, base, { file: 'assembly-1.html', device: 'assembly-1', relay: 'assembly-scan-1', timeline: true, id: n => '38223000' + String(n).padStart(2, '0') });
    await scenario(browser, base, { file: 'shipping-1.html', device: 'shipping-1', relay: 'shipping-scan-1', timeline: true, id: n => '39223000' + String(n).padStart(2, '0') });
    await scenario(browser, base, { file: 'design-message.html', device: 'design-message', relay: 'design-scan-11', timeline: false, id: n => '40223000' + String(n).padStart(2, '0') });
    // the other copies are the same pages apart from their names: the same wiring, each on its own relay document and device
    await check('the other copies carry the same queue wiring, each on its own relay and device', async () => {
      const copies = [['assembly-2', 'assembly-scan-2'], ['assembly-3', 'assembly-scan-3'], ['assembly-4', 'assembly-scan-4'],
        ['shipping-2', 'shipping-scan-2'], ['shipping-3', 'shipping-scan-3'], ['design-message-1', 'design-scan-111'],
        ['weld-1', 'weld-scan-1'], ['assembly-1', 'assembly-scan-1'], ['shipping-1', 'shipping-scan-1'], ['design-message', 'design-scan-11']];
      for (const [dev, relay] of copies) {
        const s = fs.readFileSync(path.join(root, dev + '.html'), 'utf8');
        assert.equal((s.match(/<script src="station-scan-queue\.js\?v=20261003-sq1"><\/script>/g) || []).length, 1, dev + ': script tag');
        assert.ok(s.includes(`device: "${dev}",\n          signedIn:`), dev + ': queue device');
        assert.equal((s.match(new RegExp(`\\.doc\\("${relay}"\\)`, 'g')) || []).length, 2, dev + ': relay doc (listen + clear)');
        assert.equal((s.match(/\.doc\("(?:weld|assembly|shipping|design)-scan-\d+"\)/g) || []).length, 2, dev + ': no other relay');
        assert.ok(s.includes('startScannedOrderListener();\n        /* the phone-scan relay') || s.includes('        startScannedOrderListener();\n'), dev + ': attached at load');
        assert.ok(s.includes('if (scanRelayOn)') && s.includes('window.__stationEnterRun = run;') && !s.includes('startScannedOrderListener called but user not logged in'), dev + ': attach-once and the Enter hand-off');
      }
    });
  } finally { await browser.close(); server.close(); }
  let bad = 0;
  for (const [name, err] of results) { console.log((err ? 'FAIL ' : 'ok   ') + name); if (err) { bad++; console.log('     ' + String(err.message).split('\n')[0]); } }
  console.log(bad ? `\n${bad} of ${results.length} failed` : `\nscan-queue: all ${results.length} checks passed`);
  process.exit(bad ? 1 : 0);
}
main().catch(e => { console.error(e); process.exit(1); });
