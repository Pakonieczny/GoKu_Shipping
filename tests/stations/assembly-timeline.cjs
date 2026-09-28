// Assembly stations and the order timeline (Paul, 28 Sep: A4, C5, C6). Opens assembly-1.html in headless Chromium with
// Firebase, Materialize and the Netlify functions stubbed and a stub StationTimeline standing in for station-timeline.js,
// then checks what the page tells the timeline: init once with its station and device, scanned() for a phone scan, a
// typed number and a paste (with how each came in), did("assembled") for a Team stamp such as "QA1", and guard() plus
// did("etsyCompleted") on Complete Order. Last, the page without station-timeline.js still loads a scanned order.
// Every request that is not to 127.0.0.1 is answered by a stub or aborted; nothing live is contacted.
//   NODE_PATH=$(npm root -g) PW_DIR=$(npm root -g)/playwright/node_modules CHROMIUM=... node tests/stations/assembly-timeline.cjs
const http = require('http'), fs = require('fs'), path = require('path'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');
const results = [];
async function check(name, fn) { try { await fn(); results.push([name, null]); } catch (e) { results.push([name, e]); } }

const FIREBASE = `(function () {
  const snaps = window.__fbSnaps = {}, writes = window.__fbWrites = [];
  const empty = () => ({ exists: false, data: () => undefined, docs: [], empty: true, size: 0, forEach() {}, docChanges: () => [] });
  function ref(p) {
    const r = {
      path: p, id: p.split('/').pop(),
      collection: n => ref(p + '/' + n), doc: n => ref(p + '/' + n),
      where: () => r, orderBy: () => r, limit: () => r, limitToLast: () => r, startAfter: () => r,
      onSnapshot(cb) { (snaps[p] = snaps[p] || []).push(cb); setTimeout(() => { try { cb(empty()); } catch (_) {} }, 0); return () => {}; },
      get: async () => empty(),
      set: async (d, o) => { writes.push({ path: p, op: 'set', d, o }); },
      update: async d => { writes.push({ path: p, op: 'update', d }); },
      add: async d => { writes.push({ path: p, op: 'add', d }); return ref(p + '/new'); },
      delete: async () => { writes.push({ path: p, op: 'delete' }); }
    };
    return r;
  }
  const firestore = () => ({ collection: n => ref(n), doc: p => ref(p), batch: () => ({ set() {}, update() {}, delete() {}, commit: async () => {} }) });
  firestore.FieldValue = { delete: () => ({ __op: 'delete' }), serverTimestamp: () => ({ __op: 'ts' }), arrayUnion: (...a) => a, increment: n => n };
  firestore.Timestamp = { now: () => ({ toDate: () => new Date(), toMillis: () => Date.now() }) };
  const auth = () => ({ signInAnonymously: async () => ({}), onAuthStateChanged(cb) { setTimeout(() => cb({ uid: 'anon' }), 0); return () => {}; }, currentUser: { uid: 'anon' } });
  window.firebase = { apps: [], initializeApp() { return {}; }, firestore, auth, storage: () => ({ ref: () => ({}) }) };
})();`;
const MATERIALIZE = `window.M = {
  AutoInit() {}, updateTextFields() {}, toast(o) { (window.__toasts = window.__toasts || []).push(o && o.html); },
  Modal: { init(el) { const i = { open() {}, close() {}, isOpen: false }; if (el) el.__m = i; return i; }, getInstance(el) { return (el && el.__m) || { open() {}, close() {} }; } },
  FormSelect: { init(el) { const i = { destroy() {}, getSelectedValues: () => [el && el.value] }; if (el) el.__fs = i; return i; }, getInstance(el) { return el && el.__fs; } },
  Dropdown: { init() {} }
};`;
// the stand-in for station-timeline.js (written by another agent): it only notes each call
const STATION = `window.__st = [];
window.StationTimeline = {
  init(o) { __st.push(['init', { station: o.station, device: o.device, employee: typeof o.getEmployee === 'function' ? o.getEmployee() : null }]); },
  scanned(id, o) { __st.push(['scanned', id, o]); return Promise.resolve({ cancelled: false }); },
  did(type, id, text, data) { __st.push(['did', type, id, text, data]); },
  guard(id, o) { __st.push(['guard', id, o]); return Promise.resolve(window.__guardAnswer !== false); },
  isCancelled() { return false; }
};`;
const RECEIPT = { receipt_id: 3812345678, name: 'Test Buyer', status: 'Paid', is_shipped: false, is_gift: false, message_from_buyer: '', shipments: [],
  transactions: [{ transaction_id: 1, title: 'Heart charm', quantity: 1, created_timestamp: 1790000000, expected_ship_date: 1790500000, variations: [] }] };

const MIME = { '.html': 'text/html', '.js': 'text/javascript', '.css': 'text/css', '.png': 'image/png', '.jpg': 'image/jpeg', '.mp3': 'audio/mpeg' };
const listen = handler => new Promise(ok => { const s = http.createServer(handler).listen(0, '127.0.0.1', () => ok(s)); });

async function openStation(browser, base, opts = {}) {
  const context = await browser.newContext({ viewport: { width: 1400, height: 900 } }), calls = [], errors = [];
  await context.route(() => true, r => r.abort());                                     // nothing leaves the machine…
  await context.route(u => u.href.startsWith('http://127.0.0.1'), r => r.continue());   // …but the page itself
  await context.route(u => /gstatic\.com\/firebasejs\//.test(u.href), r => r.fulfill({ contentType: 'text/javascript', body: /firebase-app-compat/.test(r.request().url()) ? FIREBASE : '' }));
  await context.route(u => /materialize/.test(u.href), r => r.fulfill({ contentType: 'text/javascript', body: MATERIALIZE }));
  await context.route(u => /code\.jquery\.com|qz-tray/.test(u.href), r => r.fulfill({ contentType: 'text/javascript', body: '' }));
  await context.route(u => u.href.startsWith('http://127.0.0.1') && u.pathname.endsWith('/station-timeline.js'),
    r => opts.noStation ? r.fulfill({ status: 404, body: 'not deployed' }) : r.fulfill({ contentType: 'text/javascript', body: STATION }));
  await context.route(u => u.href.startsWith('http://127.0.0.1') && u.pathname.includes('/.netlify/functions/'), async r => {
    const req = r.request(), u = new URL(req.url()), fn = u.pathname.split('/').pop();
    let body = {}; try { body = JSON.parse(req.postData() || '{}'); } catch (_) {}
    calls.push({ fn, method: req.method(), q: u.search, body, t: Date.now() });
    const json = o => r.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify(o) });
    if (fn === 'etsyOrderProxy') return json(Object.assign({}, RECEIPT, { receipt_id: Number(u.searchParams.get('orderId')) || 0 }));
    if (fn === 'firebaseOrders') return json({ success: true, data: {} });
    if (fn === 'trackOrderProxy') return r.fulfill({ status: 200, contentType: 'text/plain', body: 'ok' });
    if (fn === 'testChitChats' || fn === 'chitChatSearch') return json({ batches: [], shipments: [] });
    return json({});
  });
  await context.addInitScript(() => {
    try {
      if (!sessionStorage.getItem('__seeded')) {
        localStorage.setItem('employee_id', '123456'); localStorage.setItem('employee_name', 'Test Operator');
        localStorage.setItem('access_token', 'test-token'); localStorage.setItem('token_expires_at', String(Math.floor(Date.now() / 1000) + 86400));
        sessionStorage.setItem('__seeded', '1');
      }
    } catch (_) {}
  });
  const page = await context.newPage(); page.on('pageerror', e => errors.push(e.message));
  await page.goto(base + '/assembly-1.html');
  // the relay listener is up once the operator is restored from localStorage
  await page.waitForFunction(() => window.__fbSnaps && (window.__fbSnaps['Brites_Orders/assembly-scan-1'] || []).length > 0, null, { timeout: 20000 });
  await page.waitForTimeout(100);                          // the stub's first delivery (skipped by the page) has gone by
  return { page, context, calls, errors };
}
const stCalls = (page, kind) => page.evaluate(k => (window.__st || []).filter(c => c[0] === k), kind);
async function waitCall(page, kind, n, what) {
  await page.waitForFunction(([k, m]) => (window.__st || []).filter(c => c[0] === k).length >= m, [kind, n], { timeout: 10000 })
    .catch(() => { throw new Error(`no ${kind}() call ${what}`); });
}
async function scanFromPhone(page, id) {
  await page.evaluate(v => (window.__fbSnaps['Brites_Orders/assembly-scan-1'] || []).forEach(cb => cb({ exists: true, data: () => ({ 'Order Number': v }) })), id);
}

async function main() {
  const pwDir = process.argv[2] || process.env.PW_DIR || path.join(root, 'node_modules');
  const { chromium } = require(path.join(pwDir, 'playwright-core'));
  const server = await listen((req, res) => {
    const u = decodeURIComponent(req.url.split('?')[0]), f = path.join(root, u);
    if (!f.startsWith(root) || !fs.existsSync(f) || fs.statSync(f).isDirectory()) { res.writeHead(404); return res.end(); }
    res.writeHead(200, { 'Content-Type': MIME[path.extname(f)] || 'application/octet-stream', 'Cache-Control': 'no-store' }); fs.createReadStream(f).pipe(res);
  });
  const base = `http://127.0.0.1:${server.address().port}`;
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  try {
    const h = await openStation(browser, base);
    try {
      await check('init: once, station "assembly", device "assembly-1", the signed-in operator', async () => {
        const inits = await stCalls(h.page, 'init');
        assert.equal(inits.length, 1, 'init exactly once');
        assert.deepEqual(inits[0][1], { station: 'assembly', device: 'assembly-1', employee: 'Test Operator' });
      });
      await check('scan from the phone scanner: scanned(id, { how: "scan", extra: { etsyStatus } }) after the order is pulled', async () => {
        await scanFromPhone(h.page, '3812345678');
        await waitCall(h.page, 'scanned', 1, 'after a phone scan');
        const [c] = await stCalls(h.page, 'scanned');
        assert.deepEqual(c.slice(1), ['3812345678', { how: 'scan', extra: { etsyStatus: 'Paid' } }], 'with the Etsy status the page already read');
        const pulled = h.calls.find(x => x.fn === 'etsyOrderProxy' && x.q.includes('3812345678'));
        assert(pulled, 'the order was pulled from Etsy as before');
      });
      await check('typed number + Enter: scanned(id, { how: "typed" })', async () => {
        await h.page.fill('#etsyOrderNumber', '3812345679');
        await h.page.focus('#etsyOrderNumber');
        await h.page.keyboard.press('Enter');
        await waitCall(h.page, 'scanned', 2, 'after typing');
        const c = (await stCalls(h.page, 'scanned'))[1];
        assert.deepEqual(c.slice(1), ['3812345679', { how: 'typed', extra: { etsyStatus: 'Paid' } }]);
      });
      await check('Paste button: scanned(id, { how: "paste" })', async () => {
        await h.page.evaluate(() => { navigator.clipboard.readText = async () => '3812345680'; });
        await h.page.evaluate(() => document.getElementById('pasteOrderBtn').click());
        await waitCall(h.page, 'scanned', 3, 'after a paste');
        const c = (await stCalls(h.page, 'scanned'))[2];
        assert.deepEqual(c.slice(1), ['3812345680', { how: 'paste', extra: { etsyStatus: 'Paid' } }]);
      });
      await check('a Team stamp "QA1" sent from Assembly records assembled; an ordinary message does not', async () => {
        await h.page.fill('#britesMsgInput', 'please check the chain length');
        await h.page.evaluate(() => document.getElementById('goScreenTwoBtn').click());
        await h.page.waitForFunction(() => document.getElementById('britesMsgInput').value === '', null, { timeout: 5000 });
        assert.equal((await stCalls(h.page, 'did')).length, 0, 'no assembled for a normal message');
        await h.page.fill('#britesMsgInput', 'QA1');
        await h.page.evaluate(() => document.getElementById('goScreenTwoBtn').click());
        await waitCall(h.page, 'did', 1, 'after sending QA1');
        const [c] = await stCalls(h.page, 'did');
        assert.deepEqual(c.slice(1), ['assembled', '3812345680', 'Assembled · QA1', { message: 'QA1' }]);
        assert.equal(h.calls.filter(x => x.fn === 'firebaseOrders' && x.method === 'POST' && x.body.newMessage).length, 2, 'both messages were still sent');
      });
      await check('Complete Order: guard(id) first, then did("etsyCompleted", …) after Etsy accepts', async () => {
        await h.page.evaluate(() => {
          document.getElementById('trackingNumberInput').value = '9400111899223344556677';
          document.getElementById('carrierSelect').value = 'usps';
          document.getElementById('completeOrderBtn').click();
        });
        await waitCall(h.page, 'did', 2, 'after Complete Order');
        const guards = await stCalls(h.page, 'guard'), dids = await stCalls(h.page, 'did');
        assert.deepEqual(guards.map(g => g.slice(1)), [['3812345680', { action: 'Complete Order' }]]);
        assert.deepEqual(dids[1].slice(1), ['etsyCompleted', '3812345680', 'Completed on Etsy · USPS 9400111899223344556677', { tracking: '9400111899223344556677', carrier: 'usps' }]);
        const order = await h.page.evaluate(() => window.__st.map(c => c[0]).slice(-2));
        assert.deepEqual(order, ['guard', 'did'], 'guard before the stamp');
        assert.equal(h.calls.filter(x => x.fn === 'trackOrderProxy').length, 1, 'tracking was posted once');
      });
      await check('Complete Order on a cancelled order the operator backs out of: nothing is posted, nothing is stamped', async () => {
        await h.page.evaluate(() => {
          window.__guardAnswer = false;
          document.getElementById('trackingNumberInput').value = '9400111899223344556688';
          document.getElementById('carrierSelect').value = 'usps';
          document.getElementById('completeOrderBtn').click();
        });
        await waitCall(h.page, 'guard', 2, 'on the second Complete Order');
        await h.page.waitForTimeout(400);
        assert.equal(h.calls.filter(x => x.fn === 'trackOrderProxy').length, 1, 'no second tracking post');
        assert.equal((await stCalls(h.page, 'did')).length, 2, 'no second etsyCompleted');
      });
      await check('no page error from the timeline wiring', async () => {
        const mine = h.errors.filter(m => /StationTimeline|stationHow|isAssemblyStamp|stName/.test(m));
        assert.deepEqual(mine, []);
      });
    } finally { await h.context.close(); }

    await check('without station-timeline.js the page works as before: a scan still loads the order', async () => {
      const g = await openStation(browser, base, { noStation: true });
      try {
        assert.equal(await g.page.evaluate(() => typeof window.StationTimeline), 'undefined');
        await scanFromPhone(g.page, '3812345690');
        await g.page.waitForFunction(() => true);
        const t0 = Date.now();
        while (Date.now() - t0 < 10000 && !g.calls.some(x => x.fn === 'firebaseOrders' && x.q.includes('orderId=3812345690'))) await new Promise(r => setTimeout(r, 100));
        assert(g.calls.some(x => x.fn === 'etsyOrderProxy' && x.q.includes('3812345690')), 'pulled from Etsy');
        assert(g.calls.some(x => x.fn === 'firebaseOrders' && x.q.includes('orderId=3812345690')), 'and ran to the end of the load');
        assert.deepEqual(g.errors.filter(m => /StationTimeline|stationHow|isAssemblyStamp|stName/.test(m)), []);
      } finally { await g.context.close(); }
    });
  } finally { await browser.close(); server.close(); }
}

main().catch(e => results.push(['setup', e])).finally(() => {
  let bad = 0;
  for (const [name, e] of results) { if (e) { bad++; console.log('FAIL', name, '\n   ', (e && e.stack || e).toString().split('\n').slice(0, 4).join('\n    ')); } else console.log('ok  ', name); }
  console.log(`${results.length - bad}/${results.length} passed`);
  process.exit(bad ? 1 : 0);
});
