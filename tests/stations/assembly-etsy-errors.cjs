// ASMERR (Paul, 15 Oct 2026): the console of all four Assembly desks showed etsyOrderProxy 403 and 400 on every scan, "Uncaught (in promise) FirebaseError: Missing or insufficient permissions", a 404 and
// "The AudioContext was not allowed to start". Opens assembly-1.html in headless Chromium with Firebase, Materialize and the Netlify functions stubbed (nothing live is contacted) and checks:
// one Etsy receipt read per order, nothing sent when the desk has no Etsy token, an Etsy error is never read as the order, a refused listener or write is said once and never uncaught, no page-icon 404.
//   PW_DIR=... node tests/stations/assembly-etsy-errors.cjs
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
      onSnapshot(cb, errCb) { (snaps[p] = snaps[p] || []).push(cb); if (window.__fbFail === 'listen') { setTimeout(() => { try { errCb ? errCb({ code: 'permission-denied', message: 'Missing or insufficient permissions.' }) : Promise.reject(Object.assign(new Error('Missing or insufficient permissions.'), { code: 'permission-denied' })); } catch (_) {} }, 0); return () => {}; } setTimeout(() => { try { cb(empty()); } catch (_) {} }, 0); return () => {}; },
      get: async () => empty(),
      set: async (d, o) => { if (window.__fbFail === 'write') throw Object.assign(new Error('Missing or insufficient permissions.'), { code: 'permission-denied' }); writes.push({ path: p, op: 'set', d, o }); },
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
    if (fn === 'etsyOrderProxy') {
      if (opts.proxy === '403') return r.fulfill({ status: 403, contentType: 'application/json', body: JSON.stringify({ error: 'Forbidden', error_description: 'invalid token' }) });
      if (opts.proxy === '400') return r.fulfill({ status: 400, contentType: 'application/json', body: JSON.stringify({ error: 'Missing access token' }) });
      return json(Object.assign({}, RECEIPT, { receipt_id: Number(u.searchParams.get('orderId')) || 0, ...(opts.shipped ? { is_shipped: true } : {}) }));
    }
    if (fn === 'firebaseOrders') return json({ success: true, data: {} });
    if (fn === 'trackOrderProxy') return r.fulfill({ status: 200, contentType: 'text/plain', body: 'ok' });
    if (fn === 'testChitChats' || fn === 'chitChatSearch') return json({ batches: [], shipments: [] });
    return json({});
  });
  await context.addInitScript(o => { window.__noEtsyToken = !!o.noToken; window.__fbFail = o.fail || ''; }, { noToken: !!opts.noToken, fail: opts.fail || '' });
  await context.addInitScript(() => {
    try {
      if (!sessionStorage.getItem('__seeded')) {
        localStorage.setItem('employee_id', '123456'); localStorage.setItem('employee_name', 'Test Operator');
        if (!window.__noEtsyToken) { localStorage.setItem('access_token', 'test-token'); localStorage.setItem('token_expires_at', String(Math.floor(Date.now() / 1000) + 86400)); }
        sessionStorage.setItem('__seeded', '1');
      }
    } catch (_) {}
  });
  const page = await context.newPage(); page.on('pageerror', e => errors.push(e.message));
  const warns = [], notFound = []; page.on('console', m => { if (m.type() === 'warning') warns.push(m.text()); }); context.on('response', r => { if (r.status() === 404) notFound.push(new URL(r.url()).pathname); });
  await page.goto(base + '/assembly-1.html');
  // the relay listener is up once the operator is restored from localStorage
  await page.waitForFunction(() => window.__fbSnaps && (window.__fbSnaps['Brites_Orders/assembly-scan-1'] || []).length > 0, null, { timeout: 20000 });
  if (opts.fail) await page.waitForTimeout(300);
  await page.waitForTimeout(100);                          // the stub's first delivery (skipped by the page) has gone by
  return { page, context, calls, errors, warns, notFound };
}
const stCalls = (page, kind) => page.evaluate(k => (window.__st || []).filter(c => c[0] === k), kind);
async function waitCall(page, kind, n, what) {
  await page.waitForFunction(([k, m]) => (window.__st || []).filter(c => c[0] === k).length >= m, [kind, n], { timeout: 10000 })
    .catch(() => { throw new Error(`no ${kind}() call ${what}`); });
}
async function scanFromPhone(page, id) {
  await page.evaluate(v => (window.__fbSnaps['Brites_Orders/assembly-scan-1'] || []).forEach(cb => cb({ exists: true, data: () => ({ 'Order Number': v }) })), id);
}


const realErrors = h => h.errors.filter(e => !/does not provide an export named/.test(e));   // (the stand-in firebase-app.js has no modular exports: the page's second, modular import fails in this harness only)
const orderProxyCalls = h => h.calls.filter(c => c.fn === 'etsyOrderProxy');
const typeOrder = async (page, id) => { await page.fill('#etsyOrderNumber', id); await page.focus('#etsyOrderNumber'); await page.keyboard.press('Enter'); };
const toasts = page => page.evaluate(() => (window.__toasts || []).slice());
const settle = async (page, ms = 900) => { await page.waitForTimeout(ms); };

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
    // 1 · connected, Etsy answers: ONE receipt read per order (the details and the transactions used to be two identical calls)
    { const h = await openStation(browser, base);
      await check('a connected desk reads the order\'s Etsy receipt ONCE, and fills the order from it', async () => {
        await typeOrder(h.page, '3812345678'); await waitCall(h.page, 'scanned', 1, 'after typing'); await settle(h.page);
        assert.equal(orderProxyCalls(h).length, 1, 'one etsyOrderProxy call for one order, got ' + orderProxyCalls(h).map(c => c.q).join(' | '));
        assert.equal(await h.page.evaluate(() => window.orderStatus), 'Paid', 'the order was read');
        assert.notEqual(await h.page.evaluate(() => document.getElementById('dateOrdered').value), '', 'its date is filled');
      });
      await check('the same order scanned again within a moment does not ask Etsy twice; another order does', async () => {
        await typeOrder(h.page, '3812345678'); await settle(h.page, 500);
        assert.equal(orderProxyCalls(h).length, 1, 'the receipt was kept for a moment');
        await typeOrder(h.page, '3812345679'); await settle(h.page, 700);
        assert.equal(orderProxyCalls(h).length, 2, 'a different order is asked');
      });
      await check('no 404 for the page icon, no page error, no AudioContext warning', async () => {
        assert(!h.notFound.some(p => /favicon/.test(p)), 'favicon asked: ' + h.notFound.join(','));
        assert.equal(realErrors(h).length, 0, 'page errors: ' + realErrors(h).join(' | '));
        assert(!h.warns.some(w => /AudioContext/.test(w)), 'warnings: ' + h.warns.join(' | '));
      });
      await h.context.close(); }

    // 2 · NO Etsy token on this desk (cleared storage): nothing is sent to the proxy, one calm notice, the scan is still recorded, the fields are cleared
    { const h = await openStation(browser, base, { noToken: true });
      await check('a desk with no Etsy token sends NOTHING to etsyOrderProxy (no 403, no 400), says so once, and still records the scan', async () => {
        await h.page.evaluate(() => { window.orderStatus = 'STALE'; document.getElementById('dateOrdered').value = ' 01 Jan 2020'; });
        await typeOrder(h.page, '3812345678'); await waitCall(h.page, 'scanned', 1, 'after typing'); await settle(h.page);
        await typeOrder(h.page, '3812345679'); await waitCall(h.page, 'scanned', 2, 'after typing again'); await settle(h.page);
        assert.equal(orderProxyCalls(h).length, 0, 'no call to the proxy: ' + orderProxyCalls(h).map(c => c.q).join(' | '));
        const t = (await toasts(h.page)).filter(x => /Etsy is not connected/.test(x));
        assert.equal(t.length, 1, 'the notice shows once, not at every scan: ' + JSON.stringify(await toasts(h.page)));
        assert.equal(await h.page.evaluate(() => window.orderStatus), '', 'the previous order\'s fields are cleared, not left on screen');
        assert.equal(await h.page.evaluate(() => document.getElementById('dateOrdered').value), '', 'its date is cleared');
        assert(h.warns.some(w => /not connected/.test(w)), 'said in the console too');
        assert.equal(realErrors(h).length, 0, 'page errors: ' + realErrors(h).join(' | '));
      });
      await h.context.close(); }

    // 3 · a token that Etsy refuses (403): one call, an error is never read as the order, the notice names the status
    { const h = await openStation(browser, base, { proxy: '403' });
      await check('Etsy answers 403: one call, not read as the order, a notice that says what to press, the scan still recorded', async () => {
        await h.page.evaluate(() => { window.orderStatus = 'STALE'; });
        await typeOrder(h.page, '3812345678'); await waitCall(h.page, 'scanned', 1, 'after typing'); await settle(h.page);
        assert.equal(orderProxyCalls(h).length, 1, 'one call');
        assert.equal(await h.page.evaluate(() => window.orderStatus), '', 'the error is not the order');
        const t = (await toasts(h.page)).filter(x => /403/.test(x) && /Connect Etsy/.test(x));
        assert.equal(t.length, 1, 'notice: ' + JSON.stringify(await toasts(h.page)));
        assert(h.warns.some(w => /Etsy did not give order 3812345678 \(403: invalid token\)/.test(w)), 'the console says what Etsy answered: ' + h.warns.join(' | '));
      });
      await h.context.close(); }

    // 4 · the security rules refuse the chat listener and the scan relay: said once, quietly; never an uncaught promise
    { const h = await openStation(browser, base, { fail: 'listen' });
      await check('Firestore refuses the listeners: no uncaught error, one console line each, a notice for the phone relay', async () => {
        await typeOrder(h.page, '3812345678'); await waitCall(h.page, 'scanned', 1, 'after typing'); await settle(h.page);
        assert.equal(realErrors(h).length, 0, 'page errors (an uncaught FirebaseError would be here): ' + realErrors(h).join(' | '));
        assert(h.warns.some(w => /Live chat for order 3812345678 is not available \(permission-denied\)/.test(w)), 'chat said once: ' + h.warns.join(' | '));
        assert(h.warns.some(w => /phone-scan relay is not available \(permission-denied\)/.test(w)), 'relay said');
        assert((await toasts(h.page)).some(x => /Phone scanning is not connected/.test(x)), 'the desk is told about the phone scanner');
        await typeOrder(h.page, '3812345679'); await settle(h.page, 600);
        assert.equal(h.warns.filter(w => /Live chat for order/.test(w)).length, 1, 'said once, not at every order');
      });
      await h.context.close(); }
    { const h = await openStation(browser, base, { fail: 'write' });
      await check('Firestore refuses the relay\'s one-shot wipe: no uncaught promise', async () => {
        await scanFromPhone(h.page, '3812345678'); await waitCall(h.page, 'scanned', 1, 'after a phone scan'); await settle(h.page);
        assert.equal(realErrors(h).length, 0, 'page errors: ' + realErrors(h).join(' | '));
        assert(h.warns.some(w => /Clearing the phone-scan relay is not available \(permission-denied\)/.test(w)), 'said: ' + h.warns.join(' | '));
      });
      await h.context.close(); }
  } finally { await browser.close(); server.close(); }
  let bad = 0; for (const [name, err] of results) { if (err) { bad++; console.log('FAIL ' + name + '\n     ' + String(err.message || err).split('\n')[0]); } else console.log('ok   ' + name); }
  console.log(`${results.length - bad}/${results.length} passed`); process.exit(bad ? 1 : 0);
}
main().catch(e => { console.error(e); process.exit(1); });
