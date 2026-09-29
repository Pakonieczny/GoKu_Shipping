// Part H (Shipping) of the station tracking work: shipping-1.html with the REAL station-timeline.js and order-timeline.js;
// every Netlify function is faked here (no Etsy, ShipStation, Chit Chats or printer); any request off loopback is aborted.
// Reads the events the outbox POSTs to firebaseOrders {timeline:[…]} and checks:
//   • Buy & Print, printed → labelPrinted (label "shipping", printed) + shipped (via label), by the signed-in person
//   • Buy & Print, bought but not printed → a note (kind labelBought), no labelPrinted, no shipped
//   • Complete Order → shipped (via completeOrder) + etsyCompleted once: a second press, or a reload, never adds another
//   • an order Etsy already showed as completed → shipped only
//   • nobody signed in → by "" and data.signedIn false; the 6-digit PIN is never written
//   node tests/stations/st-h.cjs [playwright-core dir]
const path = require('path'), assert = require('assert'), fs = require('fs'), http = require('http');
const root = path.join(__dirname, '../..');
const pwDir = process.argv[2] || process.env.PW_DIR || path.join(root, 'node_modules');
const { chromium } = require(path.join(pwDir, 'playwright-core'));

const TYPES = { '.html': 'text/html', '.js': 'text/javascript', '.css': 'text/css', '.png': 'image/png', '.jpg': 'image/jpeg', '.mp3': 'audio/mpeg' };
const PNG_1PX = Buffer.from('iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNk+M9QDwADhgGAWjR9awAAAABJRU5ErkJggg==', 'base64');

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

(async () => {
  const srv = await staticServer();
  const origin = `http://127.0.0.1:${srv.address().port}`;
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  const t0 = Date.now();
  const events = [], fnCalls = [];
  const fake = { labelFails: false, etsyStatus: 'Paid' };
  try {
    const ctx = await browser.newContext({ viewport: { width: 1500, height: 950 } });
    const js = body => ({ status: 200, contentType: 'text/javascript', headers: { 'Cross-Origin-Resource-Policy': 'cross-origin' }, body });
    const json = (status, obj) => ({ status, contentType: 'application/json', body: JSON.stringify(obj) });
    await ctx.route('**/*', async route => {
      const req = route.request(), url = new URL(req.url());
      if (url.hostname === '127.0.0.1') {
        if (url.pathname.startsWith('/.netlify/functions/')) {
          const fn = url.pathname.split('/').pop();
          let body = null; try { body = JSON.parse(req.postData() || 'null'); } catch (_) { body = req.postData(); }
          fnCalls.push({ fn, method: req.method(), qs: url.search, body, raw: req.postData() || '' });
          if (fn === 'etsyOrderProxy') {
            const id = url.searchParams.get('orderId');
            if (url.searchParams.get('include') === 'transactions') return route.fulfill(json(200, { transactions: [] }));
            return route.fulfill(json(200, { receipt_id: Number(id), name: 'Test Buyer', status: fake.etsyStatus, is_shipped: false, transactions: [] }));
          }
          if (fn === 'firebaseOrders') {
            if (url.searchParams.get('cancelCheck')) return route.fulfill(json(200, { cancelled: {} }));
            if (req.method() === 'POST' && body && Array.isArray(body.timeline)) { events.push(...body.timeline); return route.fulfill(json(200, { ok: true })); }
            return route.fulfill(json(404, { error: 'Order not found' }));
          }
          if (fn === 'trackOrderProxy') return route.fulfill(json(200, { orderId: 987654, orderNumber: body && body.receiptId }));
          if (fn === 'testChitChats') {
            const r = url.searchParams.get('resource');
            if (r === 'shipment') return route.fulfill(json(200, { shipment: { id: url.searchParams.get('id'), carrier: 'usps', carrier_tracking_code: 'TRK123', postage_label_png_url: 'https://chitchats.test/label.png' } }));
            if (r === 'label') return (!fake.labelFails && url.searchParams.get('format') === 'png')
              ? route.fulfill({ status: 200, contentType: 'image/png', body: PNG_1PX }) : route.fulfill(json(404, { error: 'no label' }));
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
      if (sessionStorage.getItem('st-h-booted')) return;   // a reload keeps what the test left in localStorage
      sessionStorage.setItem('st-h-booted', '1');
      localStorage.setItem('access_token', 'tok'); localStorage.setItem('refresh_token', 'ref');
      localStorage.setItem('token_expires_at', String(Math.floor(Date.now() / 1000) + 7200));
      localStorage.setItem('employee_id', '123456'); localStorage.setItem('employee_name', 'Marco R.');
    });
    await ctx.addInitScript(() => { window.alert = () => {}; window.print = () => {}; });
    const page = await ctx.newPage();
    const errors = [];
    page.on('pageerror', e => errors.push(e.stack || e.message));
    const boot = () => page.waitForFunction(() => window.shipTimeline && window.StationTimeline && window.OrderTimeline && window.__fsSnaps
      && Object.keys(window.__fsSnaps).some(k => /shipping-scan-/.test(k)));
    await page.goto(`${origin}/shipping-1.html`); await boot();
    const settle = ms => page.waitForTimeout(ms);
    const ofType = (t, id) => events.filter(e => e.type === t && (!id || e.orderId === id));
    const waitEvent = async (pred, what) => { const end = Date.now() + 8000; while (Date.now() < end) { if (events.some(pred)) return; await settle(100); } throw new Error('never sent: ' + what); };
    const openOrder = async (id, status) => {
      fake.etsyStatus = status || 'Paid';
      await page.fill('#etsyOrderNumber', id); await page.press('#etsyOrderNumber', 'Enter');
      await waitEvent(e => e.type === 'scan' && e.orderId === id, 'scan ' + id);
    };
    const doneToasts = () => page.evaluate(() => window.__toasts.filter(t => /marked complete/.test(t)).length);
    const complete = async () => {
      const before = fnCalls.filter(c => c.fn === 'trackOrderProxy').length, n = await doneToasts();
      await page.evaluate(() => { document.getElementById('trackingNumberInput').value = 'TRK123'; document.getElementById('carrierSelect').value = 'chitchats'; });
      await page.click('#completeOrderBtn');
      await page.waitForFunction(n => window.__toasts.filter(t => /marked complete/.test(t)).length > n, n);
      assert.strictEqual(fnCalls.filter(c => c.fn === 'trackOrderProxy').length, before + 1, 'Complete Order posted once');
    };
    const buy = () => page.evaluate(() => { document.getElementById('ccShipmentId').value = 'SHIP1'; const b = document.getElementById('ccBuy'); b.disabled = false; b.click(); });
    const mine = e => { assert.strictEqual(e.station, 'shipping', e.type + ' station'); assert.strictEqual(e.device, 'shipping-1', e.type + ' device'); assert(e.id, e.type + ' has a stable id'); };

    // 1 · Buy & Print, printed: the shipping label and the shipment, by the person signed in on this station
    await openOrder('4444444444');
    await buy();
    await waitEvent(e => e.type === 'shipped' && e.orderId === '4444444444', 'shipped (label)');
    const lp = ofType('labelPrinted', '4444444444');
    assert.strictEqual(lp.length, 1, 'one labelPrinted');
    assert.strictEqual(lp[0].by, 'Marco R.'); mine(lp[0]);
    assert.deepStrictEqual([lp[0].data.label, lp[0].data.printed, lp[0].data.service, lp[0].data.tracking, lp[0].data.signedIn], ['shipping', true, 'chitchats', 'TRK123', undefined]);
    const sl = ofType('shipped', '4444444444');
    assert.strictEqual(sl.length, 1); assert.strictEqual(sl[0].by, 'Marco R.'); assert.strictEqual(sl[0].data.via, 'label'); mine(sl[0]);
    assert.strictEqual(ofType('etsyCompleted', '4444444444').length, 0, 'a label is not an Etsy completion');

    // 2 · Complete Order: shipped + etsyCompleted; pressed again (and after a reload): still one etsyCompleted
    await complete();
    await waitEvent(e => e.type === 'etsyCompleted' && e.orderId === '4444444444', 'etsyCompleted');
    const ec = ofType('etsyCompleted', '4444444444')[0];
    assert.strictEqual(ec.by, 'Marco R.'); mine(ec); assert.strictEqual(ec.data.tracking, 'TRK123');
    assert(ofType('shipped', '4444444444').some(e => e.data.via === 'completeOrder'), 'Complete Order recorded shipped');
    await complete(); await settle(1500);
    assert.strictEqual(ofType('etsyCompleted', '4444444444').length, 1, 'a second Complete Order adds no etsyCompleted');
    await page.reload(); await boot();
    await openOrder('4444444444');
    await complete(); await settle(1500);
    assert.strictEqual(ofType('etsyCompleted', '4444444444').length, 1, 'nor one after a reload');

    // 3 · an order Etsy already shows as completed: shipped, no second Etsy completion
    await openOrder('5555555555', 'Completed');
    await complete();
    await waitEvent(e => e.type === 'shipped' && e.orderId === '5555555555', 'shipped 5555555555');
    await settle(1200);
    assert.strictEqual(ofType('etsyCompleted', '5555555555').length, 0, 'already completed on Etsy → no etsyCompleted');

    // 4 · bought but not printed: a note, never a printed label or a shipment
    fake.labelFails = true;
    await openOrder('6666666666');
    await buy();
    await waitEvent(e => e.type === 'note' && e.orderId === '6666666666', 'labelBought note');
    const nb = ofType('note', '6666666666')[0];
    assert.deepStrictEqual([nb.data.kind, nb.data.printed], ['labelBought', false]);
    await settle(1200);
    assert.strictEqual(ofType('labelPrinted', '6666666666').length + ofType('shipped', '6666666666').length, 0, 'not printed → no labelPrinted, no shipped');
    fake.labelFails = false;

    // 5 · nobody signed in: by "" and signedIn false, not the name the page loaded with
    await page.evaluate(() => { localStorage.removeItem('employee_id'); localStorage.removeItem('employee_name'); });
    await openOrder('7777777777');
    await complete();
    await waitEvent(e => e.type === 'etsyCompleted' && e.orderId === '7777777777', 'etsyCompleted signed out');
    for (const t of ['shipped', 'etsyCompleted']) {
      const e = ofType(t, '7777777777')[0];
      assert.strictEqual(e.by, '', t + ': nobody signed in → by ""');
      assert.strictEqual(e.data.signedIn, false, t + ': signedIn false');
    }
    assert.strictEqual(ofType('scan', '7777777777')[0].by, '', 'the scan is not given the earlier name either');

    // 6 · never a PIN in a record
    const posted = fnCalls.filter(c => c.fn === 'firebaseOrders' && c.method === 'POST').map(c => c.raw).join('\n');
    assert(!posted.includes('123456'), 'the 6-digit PIN is never written');
    assert.deepStrictEqual(errors, [], 'no page errors');
    await ctx.close();
    console.log(`st-h OK in ${((Date.now() - t0) / 1000).toFixed(1)} s · ${events.length} events: ${[...new Set(events.map(e => e.type))].join(', ')}`);
  } finally { await browser.close(); srv.close(); }
})().catch(e => { console.error(e); process.exit(1); });
