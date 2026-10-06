// The live stations board, from the pages (Paul, 5 Oct 2026: "show me the current order each station is processing ... as the
// employee is working"): what weld-1.html, assembly-1.html and the Charm Sorter (its order window and its laser sheet) tell
// the live layer (StationActivity.working() / idle(), plans/employee-hr/api.md) as the person works, in a real browser with the
// real station-session.js and station-activity.js. Every /.netlify/functions call is a fake in this file (the sorter uses the
// test server of tests/charm-nest/bridge-server.cjs) and every other host is aborted: nothing real is touched or written.
//   WELD      · nobody signed in: nothing · a scan shows the order at once with a piece for each stud line (not the necklace) and
//               the buyer, then "Welded" · a no-stud and a cancelled order never stay on the board · a phone scan is credited to
//               the desktop's person · the next scan replaces the last (one order at a time)
//   ASSEMBLY  · nobody signed in: nothing · a scan shows the order with all its lines · a message or picture to the Team keeps
//               it, a flag too · the first Team stamp (QA1, Done, QA 2) ends it · a phone scan is credited to the desktop's
//               person · a cancelled order never stays · a stamp that failed to send does not end it
//   SORTER    · no name: nothing, and no name is asked for · an open order window is the sorter's current order (once, however
//               often it is drawn); Complete Order ends it and a repaint does not bring it back; closing it ends it; a laser
//               sheet shows at the Laser station and ends when completed · the sandbox page writes only the Sandbox_ store, and
//               a sandbox workspace not signed in as the sandbox writes nothing
//   · no PIN in any live request
//   NODE_PATH=$(npm root -g) PW_DIR=$(npm root -g)/playwright/node_modules CHROMIUM=… node tests/stations/station-live-pages.cjs
'use strict';
const http = require('http'), fs = require('fs'), path = require('path'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');
const pwDir = process.argv[2] || process.env.PW_DIR || path.join(root, 'node_modules');
const { chromium } = require(path.join(pwDir, 'playwright-core'));
const CHROME = process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome';
const { start } = require('../charm-nest/bridge-server.cjs');

const results = [];
async function check(name, fn) { try { await fn(); results.push([name, null]); } catch (e) { results.push([name, e]); } }
const wait = ms => new Promise(r => setTimeout(r, ms));
async function until(fn, what, ms = 10000) {
  const t0 = Date.now();
  for (;;) { const v = await fn(); if (v) return v; if (Date.now() - t0 > ms) throw new Error('timed out waiting for ' + what); await wait(60); }
}
const PIN = String(100000 + Math.floor(Math.random() * 900000)).replace(/^(\d)\1{5}$/, '482913');   // made up per run: only the login door may carry it
const WHO = 'Marco R.';

/* ── the fake world ── */
const FIREBASE = `(function () {
  const snaps = window.__fbSnaps = {};
  const empty = () => ({ exists: false, data: () => undefined, docs: [], empty: true, size: 0, forEach() {}, docChanges: () => [] });
  function ref(p) {
    const r = { path: p, id: p.split('/').pop(), collection: n => ref(p + '/' + n), doc: n => ref(p + '/' + n),
      where: () => r, orderBy: () => r, limit: () => r, limitToLast: () => r, startAfter: () => r,
      onSnapshot(cb) { (snaps[p] = snaps[p] || []).push(cb); setTimeout(() => { try { cb(empty()); } catch (_) {} }, 0); return () => {}; },
      get: async () => empty(), set: async () => {}, update: async () => {}, add: async () => ref(p + '/new'), delete: async () => {} };
    return r;
  }
  const firestore = () => ({ collection: n => ref(n), doc: p => ref(p), batch: () => ({ set() {}, update() {}, delete() {}, commit: async () => {} }) });
  firestore.FieldValue = { delete: () => ({}), serverTimestamp: () => ({}), arrayUnion: (...a) => a, increment: n => n };
  firestore.Timestamp = { now: () => ({ toDate: () => new Date(), toMillis: () => Date.now() }) };
  const auth = () => ({ signInAnonymously: async () => ({}), onAuthStateChanged(cb) { setTimeout(() => cb({ uid: 'anon' }), 0); return () => {}; }, currentUser: { uid: 'anon' } });
  window.firebase = { apps: [], initializeApp() { return {}; }, firestore, auth, storage: () => ({ ref: () => ({}) }) };
})();`;
const MATERIALIZE = `window.M = { AutoInit() {}, updateTextFields() {}, toast() {},
  Modal: { init(el) { const i = { open() {}, close() {}, isOpen: false }; if (el) el.__m = i; return i; }, getInstance(el) { return (el && el.__m) || { open() {}, close() {} }; } },
  FormSelect: { init(el) { const i = { destroy() {}, getSelectedValues: () => [el && el.value] }; if (el) el.__fs = i; return i; }, getInstance(el) { return el && el.__fs; } },
  Dropdown: { init() {} } };`;
const O = { OUT: '3812300001', MIXED: '3812300002', NOSTUD: '3812300003', CANC: '3812300004', PHONE: '3812300005', STUD: '3812300006',
  TWO: '3812300011', ONE: '3812300012', FAIL: '3812300013', QA2: '3812300014', ACANC: '3812300015' };
const TX = {
  [O.MIXED]: [
    { transaction_id: 94201, listing_id: 155510010, title: 'Pet Portrait Earrings', quantity: 1, sku: 'PET-ST', variations: [{ formatted_name: 'Style', formatted_value: 'Studs' }] },
    { transaction_id: 94202, listing_id: 155510020, title: 'Pet Portrait Necklace', quantity: 1, sku: 'PET-NK', variations: [] },
    { transaction_id: 94203, listing_id: 155510030, title: 'Tiny Initial Stud Earrings', quantity: 2, sku: 'INI-ST', variations: [] }],
  [O.NOSTUD]: [{ transaction_id: 94501, title: 'Pet Portrait Necklace', quantity: 1, variations: [] }],
  [O.CANC]: [{ transaction_id: 94601, title: 'Stud earrings', quantity: 1, variations: [] }],
  [O.PHONE]: [{ transaction_id: 94701, listing_id: 155510070, title: 'Custom Photo Stud Earrings', quantity: 1, variations: [] }],
  [O.STUD]: [{ transaction_id: 94801, listing_id: 155510080, title: 'Birthstone Stud Earrings', quantity: 1, variations: [] }],
  [O.TWO]: [{ transaction_id: 95001, listing_id: 166610010, title: 'Name Charm', quantity: 2, sku: 'AAA-1' }, { transaction_id: 95002, listing_id: 166610020, title: 'Heart Charm', quantity: 1, sku: 'BBB-2' }],
  [O.ONE]: [{ transaction_id: 95101, listing_id: 166610030, title: 'Solo Charm', quantity: 3, sku: 'SOLO-9' }],
  [O.QA2]: [{ transaction_id: 95201, listing_id: 166610040, title: 'Queue Charm', quantity: 2, sku: 'Q-1' }],
  [O.FAIL]: [{ transaction_id: 95301, listing_id: 166610050, title: 'Fail Charm', quantity: 1, sku: 'F-1' }],
  [O.ACANC]: [{ transaction_id: 95401, listing_id: 166610060, title: 'Cancel Charm', quantity: 1, sku: 'C-1' }]
};
const CANCELLED = new Set([O.CANC, O.ACANC]);
const MIME = { '.html': 'text/html', '.js': 'text/javascript', '.css': 'text/css' };
const json = (r, body, status = 200) => r.fulfill({ status, contentType: 'application/json', body: JSON.stringify(body) });

/** what the server would hold: the last thing each station/device/person said (a beat changes nothing) */
function boardOf(lives, q) {
  const m = new Map();
  for (const l of lives) {
    if (q != null && l._q !== q) continue;
    const k = [l.station, l.device, l.person].join('__');
    if (l.event === 'work') m.set(k, { state: 'working', order: l.order, sandbox: !!l.sandbox });
    else if (l.event === 'idle') m.set(k, { state: 'idle' });
  }
  return m;
}
const workingNow = (lives, q) => [...boardOf(lives, q)].filter(([, v]) => v.state === 'working').map(([k, v]) => [k, v.order]);

/** a station page (weld or assembly) on the local server with the fake functions */
async function stationPage(browser, base, file, device) {
  const reqs = [], lives = [], errors = [], toasts = [];
  const context = await browser.newContext({ viewport: { width: 1400, height: 950 } });
  await context.route(() => true, r => r.abort());
  await context.route(u => u.href.startsWith('http://127.0.0.1'), r => r.continue());
  await context.route(u => /gstatic\.com\/firebasejs\//.test(u.href), r => r.fulfill({ contentType: 'text/javascript', body: /firebase-app-compat/.test(r.request().url()) ? FIREBASE : '' }));
  await context.route(u => /materialize/.test(u.href), r => r.fulfill({ contentType: /\.css/.test(r.request().url()) ? 'text/css' : 'text/javascript', body: /\.css/.test(r.request().url()) ? '' : MATERIALIZE }));
  await context.route(u => /code\.jquery\.com|qz-tray/.test(u.href), r => r.fulfill({ contentType: 'text/javascript', body: '' }));
  await context.route(u => u.href.startsWith('http://127.0.0.1') && u.pathname.includes('/.netlify/functions/'), async r => {
    const req = r.request(), u = new URL(req.url()), fn = u.pathname.split('/').pop();
    reqs.push({ method: req.method(), url: req.url(), raw: req.postData() || '' });
    if (req.method() === 'POST') {
      let b = {}; try { b = JSON.parse(req.postData() || '{}'); } catch (_) {}
      if (fn === 'firebaseOrders' && b.pinLogin !== undefined) return json(r, b.pinLogin === PIN ? { ok: true, name: WHO } : { ok: false, error: 'not on the list' });
      if (fn === 'firebaseOrders' && b.live) { lives.push(Object.assign({ _q: u.search }, b.live)); return json(r, { success: true, written: 1 }); }
      if (fn === 'firebaseOrders' && Array.isArray(b.activity)) return json(r, { success: true, written: b.activity.length, duplicate: 0, refused: 0, scrubbed: 0 });
      if (fn === 'firebaseOrders' && b.session) return json(r, { success: true });
      if (fn === 'firebaseOrders' && Array.isArray(b.timeline)) return json(r, { ok: true, ids: b.timeline.map(e => e.id) });
      if (fn === 'firebaseOrders' && b.newMessage && b.orderNumber === O.FAIL) return json(r, { success: false, error: 'down' }, 500);
      return json(r, { success: true });
    }
    if (fn === 'etsyOrderProxy') {
      const id = u.searchParams.get('orderId');
      return json(r, { receipt_id: Number(id) || 0, name: 'Test Buyer', status: CANCELLED.has(id) ? 'Paid' : 'Paid', transactions: TX[id] || [] });
    }
    if (fn === 'firebaseOrders' && u.searchParams.get('cancelCheck')) {
      const out = {}; for (const id of u.searchParams.get('cancelCheck').split(',')) if (CANCELLED.has(id)) out[id] = { at: Date.now() - 60000, by: 'Office', why: 'Buyer cancelled', source: 'sheet' };
      return json(r, { success: true, cancelled: out, now: Date.now() });
    }
    if (fn === 'firebaseOrders' && /employee/i.test(u.searchParams.get('orderId') || '')) return json(r, { success: false, error: 'closed' }, 401);
    if (fn === 'firebaseOrders') return json(r, { success: true, data: {} });
    return json(r, {});
  });
  await context.addInitScript(() => {
    try { localStorage.setItem('access_token', 'test-token'); localStorage.setItem('refresh_token', 'ref'); localStorage.setItem('token_expires_at', String(Math.floor(Date.now() / 1000) + 86400)); } catch (_) {}
  });
  const page = await context.newPage();
  page.on('pageerror', e => errors.push(e.message));
  await page.goto(base + '/' + file);
  await page.waitForFunction(() => window.StationActivity && window.StationSession && window.__fbSnaps, null, { timeout: 20000 });
  await wait(500);
  const open = async (id, how, relay) => {
    if (how === 'phone') {
      await page.evaluate(([n, doc]) => { const cbs = window.__fbSnaps[doc]; cbs[cbs.length - 1]({ exists: true, data: () => ({ 'Order Number': n }) }); }, [id, relay]);
      await page.waitForFunction(n => document.getElementById('etsyOrderNumber').value === n, id, { timeout: 5000 });
    } else { await page.fill('#etsyOrderNumber', id); await page.focus('#etsyOrderNumber'); await page.keyboard.press('Enter'); }
    await wait(900);
  };
  return { context, page, reqs, lives, errors, open, device };
}

/** the sorter page (real bridge, Library, Rose Gold) on the test server; `sandbox` is the Settings switch */
async function sorterPage(browser, srv, errors, sandbox) {
  const lives = [], reqs = [];
  const context = await browser.newContext({ viewport: { width: 1440, height: 950 } });
  const js = body => ({ status: 200, contentType: 'text/javascript', headers: { 'Cross-Origin-Resource-Policy': 'cross-origin', 'Access-Control-Allow-Origin': '*' }, body });
  await context.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => {
    const u = r.request().url();
    if (/qrcodejs/.test(u)) return r.fulfill(js(fs.readFileSync(path.join(root, 'lib/qrcode.min.js'))));
    if (/fonts\.googleapis|fonts\.gstatic/.test(u)) return r.fulfill({ status: 200, contentType: 'text/css', body: '' });
    return r.abort();
  });
  const ok = body => ({ status: 200, contentType: 'application/json', headers: { 'Access-Control-Allow-Origin': '*' }, body: JSON.stringify(body) });
  await context.route(u => /\/\.netlify\/functions\/firebaseOrders/.test(u.pathname), r => {
    const body = r.request().postData() || '';
    let j = null; try { j = JSON.parse(body || 'null'); } catch (_) {}
    if (r.request().method() === 'POST' && j && (j.live || Array.isArray(j.activity) || j.session)) {
      const q = new URL(r.request().url()).search;
      reqs.push({ q, body });
      if (j.live) lives.push(Object.assign({ _q: q }, j.live));
      return r.fulfill(ok({ success: true, written: 1 }));
    }
    return r.fallback();
  });
  await context.addInitScript(sb => {
    try {
      localStorage.setItem('cn.settings', JSON.stringify({ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on', sandbox: sb ? 'on' : 'off', sandboxStream: 'off' }));
      window.__prompts = [];
      window.prompt = (...a) => { window.__prompts.push(a); return null; };
    } catch (_) {}
  }, !!sandbox);
  const page = await context.newPage();
  page.setDefaultTimeout(30000);
  page.on('pageerror', e => errors.push(e.message));
  await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
  await page.waitForFunction(() => window.CN && window.Orders && window.OrderWin && window.CNAct && window.CNLive && window.StationActivity && window.StationSession && CN.S.cloud.ok === true, null, { timeout: 60000 });
  return { context, page, lives, reqs };
}

async function main() {
  const server = await new Promise(ok => { const s = http.createServer((req, res) => {
    const f = path.join(root, decodeURIComponent(req.url.split('?')[0]));
    if (!f.startsWith(root) || !fs.existsSync(f) || fs.statSync(f).isDirectory()) { res.writeHead(404); return res.end(); }
    res.writeHead(200, { 'Content-Type': MIME[path.extname(f)] || 'application/octet-stream' }); fs.createReadStream(f).pipe(res);
  }).listen(0, '127.0.0.1', () => ok(s)); });
  const base = `http://127.0.0.1:${server.address().port}`;
  const browser = await chromium.launch({ executablePath: CHROME, args: ['--no-sandbox'] });
  const allLives = [], allReqs = [];
  try {
    /* ═══════════ WELD ═══════════ */
    {
      const w = await stationPage(browser, base, 'weld-1.html', 'weld-1');
      const { page, lives, open } = w;
      const here = () => lives.filter(l => l.device === 'weld-1');
      const works = id => here().filter(l => l.event === 'work' && l.order && l.order.rid === id);
      await check('weld: signed out, a scan is read as before and tells the live board nothing', async () => {
        await open(O.OUT);
        await wait(700);
        assert.equal(lives.length, 0, JSON.stringify(lives));
        assert.equal(await page.evaluate(() => StationActivity.current().length), 0);
      });
      await page.focus('#employeeNumberInput'); await page.keyboard.type(PIN);
      await page.click('#employeeLoginBtn');
      await page.waitForFunction(() => window.StationActivity.who(), null, { timeout: 10000 });
      await wait(300);

      await check('weld: a typed scan shows the order at once, one piece for each stud line (not the necklace), the buyer, then "Welded"', async () => {
        await open(O.MIXED);
        await until(() => works(O.MIXED).some(l => l.order.note === 'Welded'), 'the welded note');
        const first = works(O.MIXED)[0], last = works(O.MIXED).pop();
        for (const l of works(O.MIXED)) { assert.equal(l.person, WHO); assert.equal(l.station, 'welding'); assert.equal(l.device, 'weld-1'); assert(!l.sandbox); assert.equal(l._q, ''); }
        assert.equal(first.order.note, '', 'the scan is shown before the outcome is known');
        assert.deepEqual(last.order.pieces.map(p => p.id), [O.MIXED + '_94201', O.MIXED + '_94203']);
        assert.deepEqual(last.order.pieces.map(p => p.label), ['Pet Portrait Earrings', '2 x Tiny Initial Stud Earrings']);
        assert.deepEqual(last.order.pieces.map(p => p.sku), ['PET-ST', 'INI-ST']);
        assert.deepEqual(last.order.pieces.map(p => p.listingId), ['155510010', '155510030']);
        assert.equal(last.order.customer, 'Test Buyer'); assert.equal(last.order.orderNumber, O.MIXED); assert.equal(last.order.pieceCount, 2);
        assert.equal(first.order.scannedAt, last.order.scannedAt, 'one scan time, however often it is told');
        assert(Math.abs(last.order.scannedAt - Date.now()) < 30000, 'the scan time is now');
        assert.deepEqual(workingNow(here()).map(([k, o]) => [k, o.rid]), [['welding__weld-1__' + WHO, O.MIXED]]);
      });
      await check('weld: an order with no stud earrings never stays on the board', async () => {
        await open(O.NOSTUD);
        await wait(1200);
        assert.deepEqual(workingNow(here()), [], JSON.stringify(workingNow(here())));
        assert.equal(await page.evaluate(() => StationActivity.current().length), 0);
        assert(here().some(l => l.event === 'idle' && l.ended && l.ended.rid === O.NOSTUD), 'the server is told it ended');
      });
      await check('weld: the next scan replaces the last (one order at a time); a phone scan is credited to the desktop\'s person', async () => {
        await open(O.STUD);
        await until(() => works(O.STUD).length, 'the typed stud order');
        await open(O.PHONE, 'phone', 'Brites_Orders/weld-scan-1');
        await until(() => works(O.PHONE).some(l => l.order.note === 'Welded'), 'the phone-scanned order');
        assert.equal(works(O.PHONE)[0].person, WHO, 'the desktop\'s signed-in person');
        const now = workingNow(here());
        assert.deepEqual(now.map(([k, o]) => [k, o.rid]), [['welding__weld-1__' + WHO, O.PHONE]], JSON.stringify(now));
        assert.equal(await page.evaluate(() => StationActivity.current().length), 1);
      });
      await check('weld: a cancelled order never stays on the board', async () => {
        await open(O.CANC);
        await until(() => here().some(l => l.event === 'idle' && l.ended && l.ended.rid === O.CANC) || !works(O.CANC).length, 'cancel');
        await wait(900);
        assert.deepEqual(workingNow(here()).filter(([, o]) => o.rid === O.CANC), []);
        assert.equal(await page.evaluate(id => StationActivity.current().filter(c => c.rid === id).length, O.CANC), 0);
      });
      await check('weld: nothing but a live request is added (the page works as before); no page error from the wiring', async () => {
        assert.deepEqual(w.errors.filter(m => /StationActivity|weldLive|weldAct|weldActivity/.test(m)), []);
        assert(w.reqs.some(q => /"activity"/.test(q.raw)), 'the activity log still goes out');
      });
      allLives.push(...lives); allReqs.push(...w.reqs);
      await w.context.close();
    }

    /* ═══════════ ASSEMBLY ═══════════ */
    {
      const a = await stationPage(browser, base, 'assembly-1.html', 'assembly-1');
      const { page, lives, open } = a;
      const here = () => lives.filter(l => l.device === 'assembly-1');
      const works = id => here().filter(l => l.event === 'work' && l.order && l.order.rid === id);
      const idled = id => here().filter(l => l.event === 'idle' && l.ended && l.ended.rid === id);
      const send = async text => {
        await page.fill('#britesMsgInput', text);
        await page.evaluate(() => document.getElementById('goScreenTwoBtn').click());
        await page.waitForFunction(() => document.getElementById('britesMsgInput').value === '', null, { timeout: 5000 }).catch(() => {});
        await wait(250);
      };
      await check('assembly: signed out, a scan and a Team stamp tell the live board nothing', async () => {
        await open(O.OUT);
        await send('QA1');
        await wait(600);
        assert.equal(lives.length, 0, JSON.stringify(lives));
      });
      await page.evaluate(p => { const i = document.getElementById('employeeNumberInput'); i.dataset.raw = p; i.value = '******'; }, PIN);
      await page.evaluate(() => document.getElementById('employeeLoginBtn').click());
      await page.waitForFunction(() => window.isEmployeeLoggedIn === true && window.StationActivity.who(), null, { timeout: 8000 });
      await page.waitForFunction(() => (window.__fbSnaps['Brites_Orders/assembly-scan-1'] || []).length > 0);
      await wait(300);

      await check('assembly: a scan shows the order at once with every line; a message, a flag and a picture keep it; the first stamp ends it', async () => {
        await open(O.TWO);
        await until(() => works(O.TWO).length, 'the scan');
        const w1 = works(O.TWO)[0];
        assert.equal(w1.person, WHO); assert.equal(w1.station, 'assembly'); assert.equal(w1.device, 'assembly-1'); assert.equal(w1._q, '');
        /* one piece per UNIT of every line (2 + 1 pieces are 3 cards), through the shared station-live-order.js: the board counts pieces, never lines */
        assert.deepEqual(w1.order.pieces.map(p => p.id), ['95001-1', '95001-2', '95002-1']);
        assert.deepEqual(w1.order.pieces.map(p => p.label), ['Name Charm', 'Name Charm', 'Heart Charm']);
        assert.deepEqual(w1.order.pieces.map(p => p.sku), ['AAA-1', 'AAA-1', 'BBB-2']);
        assert.deepEqual(w1.order.pieces.map(p => p.listingId), ['166610010', '166610010', '166610020']);
        assert.equal(w1.order.customer, 'Test Buyer'); assert.equal(w1.order.pieceCount, 3); assert.equal(w1.order.orderNumber, O.TWO);
        await send('please check the chain length'); await send('wrong chain, needs rework');
        await page.evaluate(async () => {
          window.uploadViaResumable = async () => 'http://127.0.0.1/x.png';
          const dt = new DataTransfer(); dt.items.add(new File(['x'], 'a.png', { type: 'image/png' }));
          document.getElementById('customerMessageHistory').dispatchEvent(new DragEvent('drop', { dataTransfer: dt, bubbles: true, cancelable: true }));
        });
        await wait(400);
        assert.equal(idled(O.TWO).length, 0, 'a message, a flag and a picture do not end it');
        assert.deepEqual(workingNow(here()).map(([, o]) => o.rid), [O.TWO]);
        await send('QA1');
        await until(() => idled(O.TWO).length, 'the stamp ends it');
        assert.deepEqual(workingNow(here()), []);
        assert.equal(await page.evaluate(() => StationActivity.current().length), 0);
      });
      await check('assembly: a phone scan is credited to the desktop\'s person; "QA 2" (a second person\'s stamp) ends it too', async () => {
        await open(O.ONE, 'phone', 'Brites_Orders/assembly-scan-1');
        await until(() => works(O.ONE).length, 'the phone scan');
        assert.equal(works(O.ONE)[0].person, WHO);
        assert.deepEqual(works(O.ONE)[0].order.pieces.map(p => [p.label, p.sku]), [['Solo Charm', 'SOLO-9'], ['Solo Charm', 'SOLO-9'], ['Solo Charm', 'SOLO-9']]);
        assert.equal(works(O.ONE)[0].order.pieceCount, 3); assert.equal(works(O.ONE)[0].order.note, 'phone scan');
        await send('QA 2');
        await until(() => idled(O.ONE).length, 'the QA 2 stamp ends it');
        assert.deepEqual(workingNow(here()), []);
      });
      await check('assembly: a stamp that failed to send does not end it; the next order replaces it', async () => {
        await open(O.FAIL);
        await until(() => works(O.FAIL).length, 'the scan');
        await send('Done');
        await wait(500);
        assert.equal(idled(O.FAIL).length, 0, 'the stamp was not sent, so the order is still in hand');
        assert.deepEqual(workingNow(here()).map(([, o]) => o.rid), [O.FAIL]);
        await open(O.QA2);
        await until(() => works(O.QA2).length, 'the next scan');
        assert.deepEqual(workingNow(here()).map(([, o]) => o.rid), [O.QA2], 'one order at a time');
      });
      await check('assembly: a cancelled order never stays on the board', async () => {
        await open(O.ACANC);
        await wait(1500);
        await page.evaluate(() => { const b = document.querySelector('.sttl-ok'); if (b) b.click(); });
        assert.deepEqual(workingNow(here()).filter(([, o]) => o.rid === O.ACANC), []);
        assert.equal(await page.evaluate(id => StationActivity.current().filter(c => c.rid === id).length, O.ACANC), 0);
      });
      await check('assembly: Sign Out ends it and a later scan shows nothing new', async () => {
        await open(O.TWO);
        await until(() => workingNow(here()).some(([, o]) => o.rid === O.TWO), 'a scan before sign-out');
        await page.evaluate(() => document.getElementById('signOutBtn').click());
        await until(() => workingNow(here()).length === 0, 'signed out ends it', 15000);
        const n = here().length;
        await open(O.OUT);
        await wait(600);
        assert.equal(here().length, n, 'nothing after sign-out');
      });
      await check('assembly: no page error from the wiring', async () => assert.deepEqual(a.errors.filter(m => /StationActivity|actLive|actScan|actStamp|actMessage|actImage/.test(m)), []));
      allLives.push(...lives); allReqs.push(...a.reqs);
      await a.context.close();
    }

    /* ═══════════ SORTER ═══════════ */
    const srv = await start({ receipts: [] });
    const errors = [];
    try {
      const RID = '4176576272', RID2 = '4176576273';
      {
        const s = await sorterPage(browser, srv, errors, false);
        const { page, lives } = s;
        const here = () => lives.filter(l => l.device === 'charm-nest-1');
        const sorter = () => here().filter(l => l.station === 'sorter');
        const laser = () => here().filter(l => l.station === 'laser');
        await check('sorter: no name is set: an order window and a sheet show nothing, and no name is asked for', async () => {
          assert.equal(await page.evaluate(() => StationActivity.who()), null);
          await page.evaluate(rid => OrderWin.openOrder(rid), RID);
          await page.waitForFunction(() => OrderWin.isOpen(), null, { timeout: 15000 });
          assert.equal(await page.evaluate(() => CNLive.sheet('GF Sheet 1')), false);
          await wait(800);
          assert.equal(lives.length, 0);
          assert.equal((await page.$$('.cnNameBar')).length, 0, 'no name field offered by this');
          assert.deepEqual(await page.evaluate(() => window.__prompts), []);
          await page.evaluate(() => OrderWin.close());
          await page.waitForFunction(() => !OrderWin.isOpen(), null, { timeout: 15000 });
        });
        await page.evaluate(() => { B.employee = 'tess welder'; });
        await page.waitForFunction(() => StationActivity.who() && StationActivity.who().person === 'Tess Welder');
        const PERSON = 'Tess Welder';

        await check('sorter: the open order window is the sorter\'s current order, once however often it is drawn', async () => {
          await page.evaluate(rid => OrderWin.openOrder(rid), RID);
          await until(() => sorter().some(l => l.event === 'work' && l.order.rid === RID), 'the open order window', 20000);
          await wait(1200);
          await page.evaluate(() => { OrderWin.paint(); OrderWin.paint(); });
          await wait(600);
          const w = sorter().filter(l => l.event === 'work' && l.order.rid === RID);
          for (const l of w) { assert.equal(l.person, PERSON); assert.equal(l.station, 'sorter'); assert.equal(l.device, 'charm-nest-1'); assert.equal(l.order.orderNumber, RID); assert.equal(l._q, ''); assert(!l.sandbox); }
          assert(w.length <= 3, 'a few writes at most (the lines read, then the same again): ' + w.length);
          assert.equal(new Set(w.map(l => l.order.scannedAt)).size, 1, 'one scan time for the whole opening');
          assert.deepEqual(workingNow(here()).map(([k, o]) => [k, o.rid]), [['sorter__charm-nest-1__' + PERSON, RID]]);
        });
        await check('sorter: Complete Order ends it and a repaint does not bring it back; closing ends the next', async () => {
          const before = sorter().filter(l => l.event === 'work').length;
          await page.evaluate(rid => CNAct('complete', { orderId: rid, parts: 1, orders: 1, detail: 'Complete Order' }), RID);
          await until(() => sorter().some(l => l.event === 'idle' && l.ended && l.ended.rid === RID), 'the completion ends it');
          await page.evaluate(() => { OrderWin.paint(); });
          await wait(700);
          assert.equal(sorter().filter(l => l.event === 'work').length, before, 'a repaint does not start it again');
          assert.deepEqual(workingNow(here()), []);
          await page.evaluate(() => OrderWin.close());
          await page.waitForFunction(() => !OrderWin.isOpen(), null, { timeout: 15000 });
          await page.evaluate(rid => OrderWin.openOrder(rid), RID2);
          await until(() => sorter().some(l => l.event === 'work' && l.order.rid === RID2), 'the next order');
          assert.deepEqual(workingNow(here()).map(([, o]) => o.rid), [RID2]);
          await page.evaluate(() => OrderWin.close());
          await page.waitForFunction(() => !OrderWin.isOpen(), null, { timeout: 15000 });
          await until(() => workingNow(here()).length === 0, 'closing the window ends it');
          assert.equal(await page.evaluate(() => StationActivity.current().length), 0);
        });
        await check('sorter: a press keeps it alive; a laser sheet shows at the Laser station and a laser completion ends it', async () => {
          assert.equal(await page.evaluate(() => CNLive.sheet('GF Sheet 2 · Set 4')), true);
          await until(() => laser().some(l => l.event === 'work'), 'the laser sheet');
          const w = laser().find(l => l.event === 'work');
          assert.equal(w.order.kind, 'sheet'); assert.equal(w.order.title, 'GF Sheet 2 · Set 4'); assert.equal(w.person, PERSON); assert.equal(w.station, 'laser'); assert.equal(w.device, 'charm-nest-1');
          assert.deepEqual(w.order.pieces, []);
          await page.evaluate(() => CNAct('note', { detail: 'a press' }));
          assert.equal(await page.evaluate(() => StationActivity.current().length), 1, 'a press does not end it');
          await page.evaluate(() => CNAct('complete', { station: 'laser', parts: 3, detail: 'Library laser check' }));
          await until(() => laser().some(l => l.event === 'idle'), 'the laser completion ends it');
          assert.deepEqual(workingNow(here()), []);
          await page.evaluate(() => CNLive.close('laser'));
          assert.equal(await page.evaluate(() => CNLive.sheet('GF Sheet 2 · Set 4')), true, 'the next opening of a sheet shows again');
          await until(() => laser().filter(l => l.event === 'work').length === 2, 'the sheet shown again');
          await page.evaluate(() => CNLive.close('laser'));
          await until(() => workingNow(here()).length === 0, 'closing the sheet window ends it');
        });
        await check('sorter: no PIN or page error from the wiring', async () => {
          assert(!JSON.stringify(lives).includes('000000'));
          assert.deepEqual(errors.filter(m => /CNLive|StationActivity|working|idle/.test(m)), []);
        });
        allLives.push(...lives); allReqs.push(...s.reqs);
        await s.context.close();
      }
      {
        const s = await sorterPage(browser, srv, errors, true);
        const { page, lives } = s;
        await check('sorter sandbox: the live board write goes only to the Sandbox_ store and says so', async () => {
          await page.evaluate(() => { B.employee = 'tess welder'; });
          await page.waitForFunction(() => StationActivity.who() && StationActivity.who().person === 'Tess Welder' && StationSession.who().sandbox === true);
          await page.evaluate(rid => OrderWin.openOrder(rid), RID);
          await until(() => lives.some(l => l.event === 'work' && l.order.rid === RID), 'the sandbox order', 20000);
          assert(lives.every(l => /sandbox=1/.test(l._q) && l.sandbox === true), JSON.stringify(lives.map(l => [l._q, l.sandbox])));
          assert.deepEqual(workingNow(lives, '').length, 0, 'nothing reached the real store');
          assert.equal(workingNow(lives, '?sandbox=1').length, 1);
          await page.evaluate(() => OrderWin.close());
          await until(() => workingNow(lives, '?sandbox=1').length === 0, 'closing ends it');
        });
        await check('sorter sandbox: a sandbox workspace whose sign-in is not the sandbox writes nothing (the rule of CNAct)', async () => {
          const n = lives.length;
          const out = await page.evaluate(() => {
            const real = StationSession.who;
            try { StationSession.who = function () { const w = real.apply(StationSession, arguments); return w ? Object.assign({}, w, { sandbox: false }) : w; }; return [CNLive.sheet('GF Sheet 9'), CNLive.order('4176576299', [])]; }
            finally { StationSession.who = real; }
          });
          assert.deepEqual(out, [false, false]);
          await wait(500);
          assert.equal(lives.length, n, 'nothing was sent');
        });
        allLives.push(...lives); allReqs.push(...s.reqs);
        await s.context.close();
      }
    } finally { try { srv.close(); } catch (_) {} }

    await check('no PIN in any live request, and no live request carries more than the order, pieces, person and station', async () => {
      assert(allLives.length > 10, 'live requests: ' + allLives.length);
      const text = JSON.stringify(allLives);
      assert(!text.includes(PIN), 'the PIN is in a live request');
      for (const l of allLives) {
        assert(/^(work|beat|idle)$/.test(l.event)); assert(l.person && /\p{L}/u.test(l.person));
        assert(!('pin' in l) && !('employeeId' in l) && !('passcode' in l));
      }
      for (const q of allReqs) assert(!q.url ? true : !q.url.includes(PIN), 'PIN in a URL');
    });
  } finally { await browser.close(); server.close(); }
}

main().catch(e => results.push(['setup', e])).finally(() => {
  let bad = 0;
  for (const [name, e] of results) { if (e) { bad++; console.log('FAIL', name, '\n   ', String(e && e.stack || e).split('\n').slice(0, 16).join('\n    ')); } else console.log('ok  ', name); }
  console.log(`${results.length - bad}/${results.length} passed`);
  process.exit(bad ? 1 : 0);
});
