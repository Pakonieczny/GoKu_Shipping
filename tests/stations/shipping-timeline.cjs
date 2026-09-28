// Browser test of the Shipping stations' order timeline wiring (shipping-1/2/3.html → StationTimeline, Paul A4 C5 C6 C7).
// A stub StationTimeline (served as station-timeline.js) records every call. Firebase, Materialize, Chit Chats,
// ShipStation and every Netlify function are faked here; any request off the loopback server is aborted.
//   • init: station "shipping", device "shipping-N", the operator from localStorage employee_name
//   • typed, pasted and phone-scanned orders each call scanned(orderId, { how }) after the Etsy order pull
//   • Complete Order: guard() false → no ShipStation mark-as-shipped; true → shipped + etsyCompleted with the tracking
//   • Buy & Print: guard() false → no Chit Chats buy; true → labelPrinted with the shipment and tracking
//   • without station-timeline.js the page works exactly as before (orders load, Complete and Buy go straight through)
//   node tests/stations/shipping-timeline.cjs [playwright-core dir]
const path = require('path'), assert = require('assert'), fs = require('fs'), http = require('http');
const root = path.join(__dirname, '../..');
const pwDir = process.argv[2] || process.env.PW_DIR || path.join(root, 'node_modules');
const { chromium } = require(path.join(pwDir, 'playwright-core'));

const TYPES = { '.html': 'text/html', '.js': 'text/javascript', '.css': 'text/css', '.png': 'image/png', '.jpg': 'image/jpeg', '.mp3': 'audio/mpeg' };
const PNG_1PX = Buffer.from('iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNk+M9QDwADhgGAWjR9awAAAABJRU5ErkJggg==', 'base64');

// The StationTimeline stand-in: records calls; the test sets the guard's answer.
const STATION_STUB = `
window.__tl = { calls: [], guardAnswer: true, getEmployee: null };
window.StationTimeline = {
  init(o) { __tl.getEmployee = o.getEmployee; __tl.calls.push({ fn: 'init', station: o.station, device: o.device }); },
  scanned(orderId, opts) { __tl.calls.push({ fn: 'scanned', orderId, how: opts && opts.how }); return Promise.resolve({ cancelled: false }); },
  did(type, orderId, text, data) { __tl.calls.push({ fn: 'did', type, orderId, text, data }); },
  guard(orderId) { __tl.calls.push({ fn: 'guard', orderId }); return new Promise(r => setTimeout(() => r(__tl.guardAnswer), 30)); },
  isCancelled() { return false; }
};`;

// Firebase compat stand-in: chainable no-ops; onSnapshot callbacks are kept so the test can play the phone relay.
const FIREBASE_STUB = `(function () {
  const snaps = window.__fsSnaps = {};
  const listen = (p, cb) => { (snaps[p] = snaps[p] || []).push(cb); return () => {}; };
  const emptyDoc = id => ({ exists: false, id, data: () => undefined, get: () => undefined });
  function docRef(p) { return { path: p, id: p.split('/').pop(), collection: n => colRef(p + '/' + n),
    get: () => Promise.resolve(emptyDoc(p.split('/').pop())), set: () => Promise.resolve(), update: () => Promise.resolve(),
    delete: () => Promise.resolve(), onSnapshot: cb => listen(p, cb) }; }
  function colRef(p) { const q = { path: p, doc: id => docRef(p + '/' + (id || 'auto')), add: () => Promise.resolve(docRef(p + '/auto')),
    where: () => q, orderBy: () => q, limit: () => q, limitToLast: () => q, startAfter: () => q,
    get: () => Promise.resolve({ empty: true, size: 0, docs: [], forEach() {} }), onSnapshot: cb => listen(p, cb) }; return q; }
  const db = { collection: colRef, doc: docRef, batch: () => ({ set() {}, update() {}, delete() {}, commit: () => Promise.resolve() }) };
  const firestore = () => db;
  firestore.FieldValue = { delete: () => ({ op: 'delete' }), serverTimestamp: () => ({ op: 'ts' }), arrayUnion: (...a) => a, increment: n => n };
  firestore.Timestamp = { now: () => ({ toDate: () => new Date(), toMillis: () => Date.now() }), fromDate: d => ({ toDate: () => d }) };
  const auth = { currentUser: { uid: 'anon' }, signInAnonymously: () => Promise.resolve({}), onAuthStateChanged(cb) { setTimeout(() => cb({ uid: 'anon' }), 0); return () => {}; } };
  window.firebase = { apps: [], initializeApp: () => ({}), firestore, auth: () => auth, storage: () => ({}) };
})();`;

// The page's modular imports (storage uploads): present and inert.
const FIREBASE_MODULE_STUB = "const nope = () => { throw new Error('firebase stub'); }; export const initializeApp = () => ({}), getApp = () => ({}), getStorage = () => ({}), ref = nope, uploadBytesResumable = nope, getDownloadURL = nope, getAuth = () => ({}), signInAnonymously = () => Promise.resolve({});";

// Materialize stand-in: toasts are kept for the assertions.
const MATERIALIZE_STUB = `(function () {
  window.__toasts = [];
  const inst = { open() {}, close() {}, destroy() {}, isOpen: false, getSelectedValues: () => [] };
  window.M = { AutoInit() {}, updateTextFields() {}, toast(o) { __toasts.push(String((o && o.html) || '')); return inst; },
    Modal: { init: () => inst, getInstance: () => inst }, FormSelect: { init: () => inst, getInstance: () => null } };
})();`;

function staticServer() {
  const srv = http.createServer((req, res) => {
    const p = decodeURIComponent(new URL(req.url, 'http://x').pathname);
    const file = path.join(root, p);
    if (!file.startsWith(root) || !fs.existsSync(file) || !fs.statSync(file).isFile()) { res.writeHead(404); return res.end('not found'); }
    res.writeHead(200, { 'Content-Type': TYPES[path.extname(file)] || 'application/octet-stream' });
    fs.createReadStream(file).pipe(res);
  });
  return new Promise(r => srv.listen(0, '127.0.0.1', () => r(srv)));
}

async function run(browser, origin, n, { withTimeline }) {
  const fnCalls = [];      // every Netlify function request: { fn, method, qs, body }
  const ctx = await browser.newContext({ viewport: { width: 1500, height: 950 } });
  const js = body => ({ status: 200, contentType: 'text/javascript', headers: { 'Cross-Origin-Resource-Policy': 'cross-origin' }, body });
  const json = (status, obj) => ({ status, contentType: 'application/json', body: JSON.stringify(obj) });
  await ctx.route('**/*', async route => {
    const req = route.request(), url = new URL(req.url());
    if (url.hostname === '127.0.0.1') {
      if (url.pathname === '/station-timeline.js') return withTimeline ? route.fulfill(js(STATION_STUB)) : route.fulfill({ status: 404, body: 'not found' });
      if (url.pathname.startsWith('/.netlify/functions/')) {
        const fn = url.pathname.split('/').pop();
        let body = null; try { body = JSON.parse(req.postData() || 'null'); } catch (_) { body = req.postData(); }
        fnCalls.push({ fn, method: req.method(), qs: url.search, body });
        if (fn === 'etsyOrderProxy') {
          const id = url.searchParams.get('orderId');
          if (url.searchParams.get('include') === 'transactions') return route.fulfill(json(200, { transactions: [] }));
          return route.fulfill(json(200, { receipt_id: Number(id), name: 'Test Buyer', status: 'Paid', is_shipped: false, transactions: [] }));
        }
        if (fn === 'firebaseOrders') return route.fulfill(json(404, { error: 'Order not found' }));
        if (fn === 'trackOrderProxy') return route.fulfill(json(200, { orderId: 987654, orderNumber: body && body.receiptId }));
        if (fn === 'testChitChats') {
          const r = url.searchParams.get('resource');
          if (r === 'shipment') return route.fulfill(json(200, { shipment: { id: url.searchParams.get('id'), carrier: 'usps', carrier_tracking_code: 'TRK123', postage_label_png_url: 'https://chitchats.test/label.png' } }));
          if (r === 'label') return url.searchParams.get('format') === 'png'
            ? route.fulfill({ status: 200, contentType: 'image/png', body: PNG_1PX }) : route.fulfill(json(404, { error: 'no pdf' }));
          return route.fulfill(json(200, {}));
        }
        return route.fulfill(json(200, {}));
      }
      return route.continue();
    }
    const u = req.url();
    if (/gstatic\.com\/firebasejs\/.*firebase-app-compat/.test(u)) return route.fulfill(js(FIREBASE_STUB));
    if (/gstatic\.com\/firebasejs/.test(u)) return route.fulfill(js(/-compat\.js/.test(u) ? '' : FIREBASE_MODULE_STUB));
    if (/materialize.*\.js/.test(u)) return route.fulfill(js(MATERIALIZE_STUB));
    if (/code\.jquery\.com|qz-tray/.test(u)) return route.fulfill(js(''));
    if (/\.css(\?|$)|fonts\.(googleapis|gstatic)/.test(u)) return route.fulfill({ status: 200, contentType: 'text/css', body: '' });
    return route.abort();
  });
  await ctx.addInitScript(() => {
    localStorage.setItem('access_token', 'tok'); localStorage.setItem('refresh_token', 'ref');
    localStorage.setItem('token_expires_at', String(Math.floor(Date.now() / 1000) + 7200));
    localStorage.setItem('employee_id', '123456'); localStorage.setItem('employee_name', 'Tester');
    window.alert = () => {}; window.print = () => {};
  });
  const page = await ctx.newPage();
  const errors = [];
  page.on('pageerror', e => errors.push(e.stack || e.message));
  await page.goto(`${origin}/shipping-${n}.html`);
  await page.waitForFunction(() => window.shipTimeline && window.__fsSnaps && Object.keys(window.__fsSnaps).some(k => /shipping-scan-/.test(k)));
  const calls = () => page.evaluate(() => (window.__tl ? __tl.calls : []));
  const waitCall = pred => page.waitForFunction(p => window.__tl && __tl.calls.some(c => new Function('c', 'return ' + p)(c)), pred, { timeout: 8000 });
  const toasts = () => page.evaluate(() => window.__toasts.slice());
  const buys = () => fnCalls.filter(c => c.fn === 'testChitChats' && c.method === 'PATCH' && c.body && c.body.action === 'buy');
  const marks = () => fnCalls.filter(c => c.fn === 'trackOrderProxy' && c.method === 'POST');
  const setOrderFields = () => page.evaluate(() => {
    document.getElementById('trackingNumberInput').value = 'TRK123';
    document.getElementById('carrierSelect').value = 'chitchats';
  });
  const clickBuy = () => page.evaluate(() => {
    document.getElementById('ccShipmentId').value = 'SHIP1';
    const b = document.getElementById('ccBuy'); b.disabled = false; b.click();
  });
  const settle = ms => page.waitForTimeout(ms);

  // 1 · a typed order (a real key press): pulled from Etsy, then recorded as a scan
  await page.fill('#etsyOrderNumber', '1111111111');
  await page.press('#etsyOrderNumber', 'Enter');
  await page.waitForFunction(() => document.getElementById('etsyOrderNumber').value === '1111111111');
  if (withTimeline) {
    await waitCall(`c.fn === 'scanned' && c.orderId === '1111111111'`);
    const init = (await calls()).find(c => c.fn === 'init');
    assert.deepStrictEqual(init, { fn: 'init', station: 'shipping', device: `shipping-${n}` }, 'init names the station and device');
    assert.strictEqual(await page.evaluate(() => __tl.getEmployee()), 'Tester', 'the operator comes from localStorage employee_name');
    assert.strictEqual((await calls()).find(c => c.fn === 'scanned').how, 'typed', 'a typed order is recorded as typed');
  } else {
    await settle(400);
  }
  assert(fnCalls.some(c => c.fn === 'etsyOrderProxy' && /orderId=1111111111/.test(c.qs)), 'the order was pulled from Etsy');

  // 2 · the phone scanner relay (Brites_Orders/shipping-scan-N): recorded as a scan
  await page.evaluate(n => {
    const cbs = window.__fsSnaps[`Brites_Orders/shipping-scan-${n}`];
    cbs.forEach(cb => cb({ exists: true, data: () => ({}) }));                              // the first delivery is skipped
    cbs.forEach(cb => cb({ exists: true, data: () => ({ 'Order Number': '2222222222' }) }));
  }, n);
  await page.waitForFunction(() => document.getElementById('etsyOrderNumber').value === '2222222222');
  if (withTimeline) {
    await waitCall(`c.fn === 'scanned' && c.orderId === '2222222222'`);
    assert.strictEqual((await calls()).find(c => c.fn === 'scanned' && c.orderId === '2222222222').how, 'scan', 'a phone scan is recorded as scan');
  }

  // 3 · the Paste button: recorded as paste
  await page.evaluate(() => { navigator.clipboard.readText = async () => '3333333333'; });
  await page.click('#pasteOrderBtn');
  await page.waitForFunction(() => document.getElementById('etsyOrderNumber').value === '3333333333');
  if (withTimeline) {
    await waitCall(`c.fn === 'scanned' && c.orderId === '3333333333'`);
    assert.strictEqual((await calls()).find(c => c.fn === 'scanned' && c.orderId === '3333333333').how, 'paste', 'a pasted order is recorded as paste');
  }
  await settle(300);
  assert(fnCalls.some(c => c.fn === 'etsyOrderProxy' && /orderId=3333333333/.test(c.qs)), 'the pasted order was pulled from Etsy');

  if (withTimeline) {
    // 4 · Complete Order on a cancelled order, second confirmation refused: ShipStation is never asked to mark it shipped
    await page.evaluate(() => { __tl.guardAnswer = false; });
    await setOrderFields();
    await page.click('#completeOrderBtn');
    await waitCall(`c.fn === 'guard' && c.orderId === '3333333333'`);
    await page.waitForFunction(() => __toasts.some(t => /not marked shipped/.test(t)));
    await settle(300);
    assert.strictEqual(marks().length, 0, 'guard false → no trackOrderProxy mark-as-shipped');
    assert(!(await calls()).some(c => c.fn === 'did' && /shipped|etsyCompleted/.test(c.type)), 'guard false → nothing recorded as shipped');

    // 5 · Buy & Print on a cancelled order, refused: Chit Chats never sells a label
    await clickBuy();
    await page.waitForFunction(() => __toasts.some(t => /no label bought/.test(t)));
    await settle(300);
    assert.strictEqual((await calls()).filter(c => c.fn === 'guard').length, 2, 'Buy & Print asked the guard');
    assert.strictEqual(buys().length, 0, 'guard false → no Chit Chats buy');
    assert(!(await calls()).some(c => c.fn === 'did' && c.type === 'labelPrinted'), 'guard false → no label recorded');

    // 6 · confirmed (or not cancelled): Complete Order goes through and is recorded as shipped + completed on Etsy
    await page.evaluate(() => { __tl.guardAnswer = true; });
    await setOrderFields();
    await page.click('#completeOrderBtn');
    await waitCall(`c.fn === 'did' && c.type === 'etsyCompleted'`);
    assert.strictEqual(marks().length, 1, 'guard true → one mark-as-shipped');
    assert.deepStrictEqual(marks()[0].body, { receiptId: '3333333333', tracking: 'TRK123', carrier: 'chitchats' });
    const shipped = (await calls()).filter(c => c.fn === 'did' && (c.type === 'shipped' || c.type === 'etsyCompleted'));
    assert.deepStrictEqual(shipped.map(c => [c.type, c.orderId, c.data.tracking, c.data.carrier, c.data.shipStationOrderId]),
      [['shipped', '3333333333', 'TRK123', 'chitchats', 987654], ['etsyCompleted', '3333333333', 'TRK123', 'chitchats', 987654]]);
    assert(shipped.every(c => /TRK123/.test(c.text) && c.text.length <= 200), 'the texts carry the tracking number');

    // 7 · confirmed: Buy & Print buys the label, prints it and records labelPrinted with the shipment and tracking
    await clickBuy();
    await waitCall(`c.fn === 'did' && c.type === 'labelPrinted'`);
    assert.strictEqual(buys().length, 1, 'guard true → one Chit Chats buy');
    const label = (await calls()).find(c => c.fn === 'did' && c.type === 'labelPrinted');
    assert.strictEqual(label.orderId, '3333333333');
    assert.deepStrictEqual({ shipmentId: label.data.shipmentId, tracking: label.data.tracking, carrier: label.data.carrier, printed: label.data.printed, service: label.data.service },
      { shipmentId: 'SHIP1', tracking: 'TRK123', carrier: 'usps', printed: true, service: 'chitchats' });
    const order = (await calls()).filter(c => c.fn === 'guard' || (c.fn === 'did' && c.type === 'labelPrinted')).map(c => c.fn);
    assert.deepStrictEqual(order.slice(-2), ['guard', 'did'], 'the guard came before the label');
    await settle(200);
    assert.strictEqual((await calls()).filter(c => c.fn === 'did' && c.type === 'labelPrinted').length, 1, 'the label is recorded once');
  } else {
    // 4′ · no StationTimeline: Complete Order and Buy & Print go straight through, as before
    assert.strictEqual(await page.evaluate(() => typeof window.StationTimeline), 'undefined');
    await setOrderFields();
    await page.click('#completeOrderBtn');
    await page.waitForFunction(() => __toasts.some(t => /marked complete/.test(t)));
    assert.strictEqual(marks().length, 1, 'Complete Order posted mark-as-shipped');
    await clickBuy();
    await page.waitForFunction(() => __toasts.some(t => /Label printed/.test(t)));
    assert.strictEqual(buys().length, 1, 'Buy & Print bought the label');
  }

  assert.deepStrictEqual(errors, [], 'no page errors');
  await ctx.close();
  return { scans: withTimeline ? 3 : 0, functionCalls: fnCalls.length };
}

(async () => {
  const srv = await staticServer();
  const origin = `http://127.0.0.1:${srv.address().port}`;
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  const t0 = Date.now(), out = [];
  try {
    for (const n of [1, 2, 3]) out.push(`shipping-${n} ${JSON.stringify(await run(browser, origin, n, { withTimeline: true }))}`);
    out.push(`shipping-1 without station-timeline.js ${JSON.stringify(await run(browser, origin, 1, { withTimeline: false }))}`);
    console.log(`shipping timeline OK in ${((Date.now() - t0) / 1000).toFixed(1)} s · ${out.join(' · ')}`);
  } finally { await browser.close(); srv.close(); }
})().catch(e => { console.error(e); process.exit(1); });
