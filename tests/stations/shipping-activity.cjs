// Browser test of the Shipping stations' efficiency events (shipping-1/2/3.html → StationActivity, Paul 2 Oct "every scan,
// every click that matters, per employee"). The real station-session.js and station-activity.js run; Firebase, Materialize,
// Chit Chats and every Netlify function are faked here and any request off the loopback server is aborted.
//   • signed out: a scan records nothing; the PIN login (a fake "Employee Numbers" doc) signs the person in
//   • one full order run: typed scan, phone scan, Buy & Print, Complete Order → exactly one event per real moment, each with
//     this person, station "shipping", this page's device, the order id and its pieces; Complete once per order (orders: 1)
//   • a second Buy & Print is a "reprint" print, a second Complete is a note (nothing counted twice)
//   • failures: an order Etsy cannot find, and an Etsy post that fails, are errors with a category only (never the raw message)
//   • a cancelled order: the scan is a reject, a refused "do it anyway" a note; no label, no complete
//   • signed out again: nothing more is recorded; no request anywhere carries the PIN
//   node tests/stations/shipping-activity.cjs [playwright-core dir]
const path = require('path'), assert = require('assert'), fs = require('fs'), http = require('http');
const root = path.join(__dirname, '../..');
const pwDir = process.argv[2] || process.env.PW_DIR || path.join(root, 'node_modules');
const { chromium } = require(path.join(pwDir, 'playwright-core'));

const PIN = '654321', WHO = 'Tester Name';
const TYPES = { '.html': 'text/html', '.js': 'text/javascript', '.css': 'text/css', '.png': 'image/png', '.jpg': 'image/jpeg', '.mp3': 'audio/mpeg' };
const PNG_1PX = Buffer.from('iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNk+M9QDwADhgGAWjR9awAAAABJRU5ErkJggg==', 'base64');
const ORDERS = {   // what the faked Etsy answers for each order
  '1111111111': { transactions: [{ quantity: 2, sku: 'CHARM-A' }, { quantity: 1, sku: 'CHARM-B' }] },   // 3 pieces, two skus
  '2222222222': { transactions: [{ quantity: 1, sku: 'SKU-ONE' }] }                                       // 1 piece
};

// Cancel-alert stand-in for the cancelled-order run: every scan is cancelled and the second confirmation is refused.
const CANCEL_TIMELINE = `
window.StationTimeline = { init() {}, scanned() { return Promise.resolve({ cancelled: true }); }, did() {},
  guard() { return Promise.resolve(false); }, isCancelled() { return {}; } };`;

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
const FIREBASE_MODULE_STUB = "const nope = () => { throw new Error('firebase stub'); }; export const initializeApp = () => ({}), getApp = () => ({}), getStorage = () => ({}), ref = nope, uploadBytesResumable = nope, getDownloadURL = nope, getAuth = () => ({}), signInAnonymously = () => Promise.resolve({});";
const MATERIALIZE_STUB = `(function () {
  window.__toasts = [];
  const inst = { open() {}, close() {}, destroy() {}, isOpen: false, getSelectedValues: () => [] };
  window.M = { AutoInit() {}, updateTextFields() {}, toast(o) { __toasts.push(String((o && o.html) || '')); return inst; },
    Modal: { init: () => inst, getInstance: () => inst }, FormSelect: { init: () => inst, getInstance: () => null } };
})();`;

function staticServer() {
  const srv = http.createServer((req, res) => {
    const p = decodeURIComponent(new URL(req.url, 'http://x').pathname);
    const file = p === '/station-activity.js' && process.env.ACT_FILE ? process.env.ACT_FILE : path.join(root, p);
    if (!(file.startsWith(root) || file === process.env.ACT_FILE) || !fs.existsSync(file) || !fs.statSync(file).isFile()) { res.writeHead(404); return res.end('not found'); }
    res.writeHead(200, { 'Content-Type': TYPES[path.extname(file)] || 'application/octet-stream' });
    fs.createReadStream(file).pipe(res);
  });
  return new Promise(r => srv.listen(0, '127.0.0.1', () => r(srv)));
}

async function run(browser, origin, n, mode) {
  const requests = [];      // every request to the loopback server: { url, method, text }
  const activity = [];      // every event sent to the station door ({ activity: [...] })
  const state = { etsyPost: 200 };
  const ctx = await browser.newContext({ viewport: { width: 1500, height: 950 } });
  const js = body => ({ status: 200, contentType: 'text/javascript', headers: { 'Cross-Origin-Resource-Policy': 'cross-origin' }, body });
  const json = (status, obj) => ({ status, contentType: 'application/json', body: JSON.stringify(obj) });
  await ctx.route('**/*', async route => {
    const req = route.request(), url = new URL(req.url());
    if (url.hostname === '127.0.0.1') {
      if (url.pathname === '/station-timeline.js') return mode === 'cancel' ? route.fulfill(js(CANCEL_TIMELINE)) : route.fulfill({ status: 404, body: 'not found' });
      if (url.pathname.startsWith('/.netlify/functions/')) {
        const fn = url.pathname.split('/').pop(), text = req.postData() || '';
        requests.push({ url: req.url(), method: req.method(), text });
        let body = null; try { body = JSON.parse(text || 'null'); } catch (_) {}
        if (fn === 'etsyOrderProxy') {
          const id = url.searchParams.get('orderId');
          if (url.searchParams.get('include') === 'transactions') return route.fulfill(json(200, { transactions: [] }));
          if (!ORDERS[id]) return route.fulfill(json(404, { error: 'Resource not found' }));
          return route.fulfill(json(200, Object.assign({ receipt_id: Number(id), name: 'Test Buyer', status: 'Paid', is_shipped: false }, ORDERS[id])));
        }
        if (fn === 'firebaseOrders') {
          if (req.method() === 'GET' && url.searchParams.get('orderId') === 'Employee Numbers') return route.fulfill(json(200, { success: true, data: { [PIN]: WHO } }));
          if (body && Array.isArray(body.activity)) { activity.push(...body.activity); return route.fulfill(json(200, { success: true, written: body.activity.length, duplicate: 0, refused: 0, scrubbed: 0 })); }
          if (body && (body.session || body.timeline || body.newMessage)) return route.fulfill(json(200, { success: true }));
          return route.fulfill(json(404, { error: 'Order not found' }));
        }
        if (fn === 'trackOrderProxy') return state.etsyPost === 200
          ? route.fulfill(json(200, { orderId: 987654, orderNumber: body && body.receiptId }))
          : route.fulfill(json(state.etsyPost, { error: 'boom, customer at 123 Main St' }));
        if (fn === 'testChitChats') {
          const r = url.searchParams.get('resource');
          if (r === 'shipment') return route.fulfill(json(200, { shipment: { id: url.searchParams.get('id'), carrier: 'usps', carrier_tracking_code: 'TRK123', postage_label_png_url: 'https://chitchats.test/label.png' } }));
          if (r === 'label') return url.searchParams.get('format') === 'png'
            ? route.fulfill({ status: 200, contentType: 'image/png', body: PNG_1PX }) : route.fulfill(json(404, { error: 'no pdf' }));
          return route.fulfill(json(200, {}));
        }
        return route.fulfill(json(200, {}));
      }
      requests.push({ url: req.url(), method: req.method(), text: '' });
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
  await ctx.addInitScript(() => {          // nobody is signed in: no employee_id / employee_name
    localStorage.setItem('access_token', 'tok'); localStorage.setItem('refresh_token', 'ref');
    localStorage.setItem('token_expires_at', String(Math.floor(Date.now() / 1000) + 7200));
    window.alert = () => {}; window.print = () => {};
  });
  const page = await ctx.newPage();
  const errors = [];
  page.on('pageerror', e => errors.push(e.stack || e.message));
  await page.goto(`${origin}/shipping-${n}.html`);
  await page.waitForFunction(() => window.shipTimeline && window.StationActivity && window.__fsSnaps);

  const flush = async () => { await page.evaluate(() => window.StationActivity.flush()); await page.waitForTimeout(150); };
  const settle = ms => page.waitForTimeout(ms);
  const typeOrder = async id => {
    await page.fill('#etsyOrderNumber', id);
    await page.press('#etsyOrderNumber', 'Enter');
    await page.waitForFunction(v => document.getElementById('etsyOrderNumber').value === v, id);
    await settle(500);
  };
  const setFields = () => page.evaluate(() => { document.getElementById('trackingNumberInput').value = 'TRK123'; document.getElementById('carrierSelect').value = 'chitchats'; });
  const clickComplete = async () => { await setFields(); await page.click('#completeOrderBtn'); await settle(500); };
  const clickBuy = async () => {
    await page.evaluate(() => { document.getElementById('ccShipmentId').value = 'SHIP1'; const b = document.getElementById('ccBuy'); b.disabled = false; b.click(); });
    await settle(700);
  };

  // 1 · signed out: a scan records nothing
  await typeOrder('1111111111');
  await flush();
  assert.strictEqual(activity.length, 0, 'signed out: no event is sent');
  assert.deepStrictEqual(await page.evaluate(() => window.StationActivity.pending().length), 0, 'signed out: nothing is waiting to be sent');

  // 2 · the PIN login (the page's own form, a fake employee document)
  await page.type('#employeeNumberInput', PIN);
  await page.click('#employeeLoginBtn');
  await page.waitForFunction(() => window.isEmployeeLoggedIn === true);
  await settle(300);

  if (mode === 'run') {
    // 3 · a typed order, then the phone scanner's relay
    await typeOrder('1111111111');
    await page.evaluate(n => {
      const cbs = window.__fsSnaps[`Brites_Orders/shipping-scan-${n}`];
      cbs.forEach(cb => cb({ exists: true, data: () => ({}) }));
      cbs.forEach(cb => cb({ exists: true, data: () => ({ 'Order Number': '2222222222' }) }));
    }, n);
    await page.waitForFunction(() => document.getElementById('etsyOrderNumber').value === '2222222222');
    await settle(600);
    // 4 · label, completed on Etsy; then a second label (reprint) and a second Complete (nothing counted twice)
    await clickBuy();
    await clickComplete();
    await clickBuy();
    await clickComplete();
    // 5 · an order Etsy cannot find; an Etsy post that fails, then succeeds
    await typeOrder('9999999999');
    await typeOrder('1111111111');
    state.etsyPost = 500;
    await clickComplete();
    state.etsyPost = 200;
    await clickComplete();
  } else {
    // cancelled order: typed scan → alert (reject); Complete and Buy & Print refused ("do it anyway" declined)
    await typeOrder('2222222222');
    await clickComplete();
    await clickBuy();
  }

  // 6 · signed out again: nothing more is recorded
  await flush();
  const before = activity.length;
  await page.click('#signOutBtn');
  await settle(300);
  await typeOrder('2222222222');
  await flush();
  assert.strictEqual(activity.length, before, 'signed out again: no further event');

  // every event: this person, station and device; one event per moment; the pieces and the order
  for (const e of activity) {
    assert.strictEqual(e.person, WHO, 'person is the employee name');
    assert.strictEqual(e.station, 'shipping');
    assert.strictEqual(e.device, `shipping-${n}`);
  }
  const ids = activity.map(e => e.id).filter(Boolean);
  assert.strictEqual(new Set(ids).size, ids.length, 'no event is sent twice');
  const brief = activity.map(e => `${e.action}:${e.orderId}:${e.parts || 0}:${e.orders || 0}`);
  if (mode === 'run') {
    assert.deepStrictEqual(brief, [
      'scan:1111111111:3:0',   // typed
      'scan:2222222222:1:0',   // phone relay
      'print:2222222222:0:0',  // label bought and printed
      'complete:2222222222:1:1',
      'print:2222222222:0:0',  // reprint
      'note:2222222222:0:0',   // Complete again: not counted
      'scan:9999999999:0:0', 'error:9999999999:0:0',   // Etsy could not find it
      'scan:1111111111:3:0',
      'error:1111111111:0:0',  // the Etsy post failed
      'complete:1111111111:3:1'
    ], 'one event per real moment, with the order and its pieces');
    const by = a => activity.filter(e => e.action === a);
    assert.strictEqual(by('scan')[0].detail, 'typed');
    assert.strictEqual(by('scan')[1].detail, 'phone scan');
    assert.strictEqual(by('scan')[1].sku, 'SKU-ONE');
    assert.strictEqual(by('scan')[0].sku || '', '', 'two skus: none is claimed');
    assert.deepStrictEqual(by('print').map(e => e.detail), ['Chit Chats label', 'reprint']);
    assert(/^Etsy order completed/.test(by('complete')[0].detail));
    assert(/Complete on Etsy: failed \(HTTP 500\)/.test(by('error')[1].detail), 'a category and the status only: ' + by('error')[1].detail);
    assert(!JSON.stringify(activity).includes('Main St'), 'a raw error message (it may carry an address) is never sent');
    assert.strictEqual(by('complete').reduce((s, e) => s + (e.orders || 0), 0), 2, 'two orders completed');
  } else {
    assert.deepStrictEqual(brief, ['scan:2222222222:1:0', 'reject:2222222222:0:0', 'note:2222222222:0:0', 'note:2222222222:0:0'],
      'cancelled: a scan and a reject, then the refused Complete and Buy & Print are notes; no print, no complete');
  }

  // no request carries the PIN (URL or body), and no event does
  for (const r of requests) assert(!r.url.includes(PIN) && !r.text.includes(PIN), 'the PIN is in no request: ' + r.url);
  assert(!JSON.stringify(activity).includes(PIN), 'the PIN is in no event');
  assert.deepStrictEqual(errors, [], 'no page errors');
  await ctx.close();
  return `${mode}: ${activity.length} events`;
}

(async () => {
  const srv = await staticServer();
  const origin = `http://127.0.0.1:${srv.address().port}`;
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  const t0 = Date.now(), out = [];
  try {
    for (const n of [1, 2, 3]) {
      out.push(`shipping-${n} ${await run(browser, origin, n, 'run')}`);
      out.push(`shipping-${n} ${await run(browser, origin, n, 'cancel')}`);
    }
    console.log(`shipping activity OK in ${((Date.now() - t0) / 1000).toFixed(1)} s · ${out.join(' · ')}`);
  } finally { await browser.close(); srv.close(); }
})().catch(e => { console.error(e); process.exit(1); });
