// The Shipping station (shipping-1/2/3.html and their phone scanners shipping-scan-1/2/3.html) wired into the Employee efficiency portal,
// end to end over the fake backend (stations-round2 plan, item 1: "every station app, each with its own scanner, fully wired").
// The real pages, the real station-session.js / station-activity.js / station-live-order.js / station-scan-queue.js, the real
// firebaseOrders door (PIN login, sessions, activity, live, the scanner's relay write) and the real employeeEfficiency reader run over an
// in-memory Firestore. Firebase (browser), Materialize, Chit Chats, Etsy and every other host are faked or aborted. Fake people only:
// the PINs are made up when this test runs and are never printed (every check below asserts booleans).
//   1 · the sign-in: PIN through the server's login door -> ONE Station_Sessions document under the person's NAME (no id, no PIN), a beat
//       (pagehide), an end on Sign Out ("signOut"); a phone scan that arrives before the sign-in waits and is credited to the person
//       who signs in
//   2 · the scanner page: its code goes to the door as the relay write for ITS desktop (shipping-scan-N -> shipping-N), no person,
//       no PIN; a failed send is tried again instead of being skipped as "already scanned"
//   3 · each real action is a Station_Activity event with the order id, the person, the page and the session id: scan (phone, typed),
//       Buy & Print (print), Complete Order (complete, orders 1, the pieces)
//   4 · the live order (Station_Live): the phone-scanned order with its pieces, thumbnails, QR text, person, scan time; the real
//       order card of the board draws it (QR, one picture per piece, person, ticking "since")
//   5 · numbers: the board, the Overview (station and person) and the person's page say the same pieces, scans, orders
//   6 · a phone scan is input at the station (StationSession.touch) so it keeps the person from the idle sign-out
//   7 · the page's own signOut handler for "idle", "closing" and "midnight": its login is cleared, its PIN box comes back, a calm one-line
//       reason, the order and the fields stay on screen, a scan that arrives meanwhile waits for the next sign-in, and the next
//       sign-in is a new session that picks it up; the board stops showing the order of the person who was signed out
//   node tests/stations/shipping-wired.cjs [playwright-core dir]        (SHOTS_DIR=<dir> also writes a picture of the board card)
'use strict';
const path = require('path'), assert = require('assert'), fs = require('fs'), http = require('http'), Module = require('module');
const root = path.join(__dirname, '../..');
const pwDir = process.argv[2] || process.env.PW_DIR || path.join(root, 'node_modules');
const { chromium } = require(path.join(pwDir, 'playwright-core'));

/* ── an in-memory Firestore: queries, transactions, getAll, nested merges, increments ── */
const INC = n => ({ __inc: n }), TS = { __ts: true }, DEL = { __del: true };
const isPlain = v => v && typeof v === 'object' && !Array.isArray(v) && !v.__inc && !v.__ts && !v.__del;
const clone = v => v == null ? v : JSON.parse(JSON.stringify(v));
function apply(prev, data, merge) {
  const out = merge && prev ? clone(prev) : {};
  for (const [k, v] of Object.entries(data)) {
    if (v && v.__inc != null) out[k] = (Number(out[k]) || 0) + v.__inc;
    else if (v && v.__ts) out[k] = Date.now();
    else if (v && v.__del) delete out[k];
    else if (isPlain(v)) out[k] = apply(merge && isPlain(out[k]) ? out[k] : null, v, merge);
    else out[k] = v;
  }
  return out;
}
const colls = new Map();
const data = n => { if (!colls.has(n)) colls.set(n, new Map()); return colls.get(n); };
function query(name, filters, order, lim, sel) {
  return {
    where: (f, op, v) => query(name, filters.concat([[f, op, v]]), order, lim, sel),
    orderBy: (f, d) => query(name, filters, [f, d || 'asc'], lim, sel),
    limit: n => query(name, filters, order, n, sel),
    select: (...f) => query(name, filters, order, lim, f),
    get: async () => {
      let docs = [...data(name)].map(([id, d]) => ({ id, d }));
      for (const [f, op, v] of filters) docs = docs.filter(({ d }) => {
        const x = d[f]; if (x === undefined || typeof x !== typeof v) return false;
        return op === '==' ? x === v : op === '>=' ? x >= v : op === '>' ? x > v : op === '<' ? x < v : op === '<=' ? x <= v : false;
      });
      if (order) { const [f, dir] = order; docs = docs.filter(({ d }) => d[f] !== undefined).sort((p, q) => (p.d[f] < q.d[f] ? -1 : p.d[f] > q.d[f] ? 1 : 0) * (dir === 'desc' ? -1 : 1)); }
      if (lim != null) docs = docs.slice(0, lim);
      return { docs: docs.map(({ id, d }) => ({ id, data: () => clone(sel ? Object.fromEntries(Object.entries(d).filter(([k]) => sel.includes(k))) : d) })), size: docs.length, empty: !docs.length };
    }
  };
}
const dref = (name, id) => ({ id, name, path: name + '/' + id,
  get: async () => { const d = data(name).get(id); return { exists: !!d, id, data: () => clone(d) }; },
  set: async (v, o) => { data(name).set(id, apply(data(name).get(id), v, !!(o && o.merge))); },
  update: async v => { if (!data(name).has(id)) throw Object.assign(new Error('5 NOT_FOUND'), { code: 5 }); data(name).set(id, apply(data(name).get(id), v, true)); } });
const fakeDb = {
  collection: name => Object.assign(query(name, [], null, null, null), { doc: id => dref(name, id) }),
  getAll: async (...a) => Promise.all(a.filter(x => x && x.get).map(r => r.get())),
  runTransaction: async fn => {
    const writes = [];
    const out = await fn({ get: r => r.get(), getAll: (...rs) => Promise.all(rs.filter(x => x && x.get).map(r => r.get())), set: (r, v, o) => writes.push([r, v, o]) });
    for (const [r, v, o] of writes) await r.set(v, o);
    return out;
  }
};
const fakeAdmin = { firestore: Object.assign(() => fakeDb, { FieldValue: { serverTimestamp: () => TS, increment: INC, delete: () => DEL }, Timestamp: { fromMillis: m => m } }) };
const realLoad = Module._load;
Module._load = function (req, ...rest) { if (/[\/]firebaseAdmin(\.js)?$/.test(req)) return fakeAdmin; return realLoad.call(this, req, ...rest); };
const door = require(path.join(root, 'netlify/functions/firebaseOrders.js'));
const eff = require(path.join(root, 'netlify/functions/employeeEfficiency.js'));
const liveMod = require(path.join(root, 'netlify/functions/_stationLive.js'));
const pinDoor = require(path.join(root, 'netlify/functions/_stationPinLogin.js'));
Module._load = realLoad;
pinDoor.deps.sleep = async () => {};                       // a wrong try's pause is not waited for here
const PASS = 'synthetic-pass-9f3k'; process.env.EDIT_PASSCODE = PASS;
const say = (...a) => process.stdout.write(a.join(' ') + '\n');
console.warn = () => {}; console.error = () => {};         // the server modules' own warnings

const ok = (cond, msg) => { if (!cond) throw new Error(msg); };       // booleans only: nothing secret can end up in a failure
const eq = (a, b, msg) => assert.deepStrictEqual(a, b, msg);

/* ── the fake shop ── */
const NAMES = ['Tess Shipper', 'Una Shipper', 'Vic Shipper'];                      // one per page (each run starts from an empty store)
const fakePin = used => { for (;;) { const p = String(100000 + Math.floor(Math.random() * 900000)); if (!/^(\d)\1{5}$/.test(p) && !used.includes(p)) return p; } };
const PINS = []; NAMES.forEach(() => PINS.push(fakePin(PINS)));
const URL_A = 'https://firebasestorage.googleapis.com/v0/b/shop/o/thumbs%2Fcharm-a-L.png?alt=media&token=ta';
const URL_PHOTO = 'https://i.etsystatic.com/111/r/il/abc/1/il_570xN.1_xyz.jpg';
const ORDERS = {   // what the faked Etsy answers for each order
  '1111111111': { transactions: [{ quantity: 2, sku: 'CHARM-A', listing_id: 4455667788, title: 'Stud earrings, gold' }, { quantity: 1, sku: 'CHARM-B', listing_id: 5566778899, title: 'Charm, silver' }] },   // 3 pieces, two skus
  '2222222222': { transactions: [{ quantity: 1, sku: 'SKU-ONE', listing_id: 5566778899, title: 'Charm, silver' }] },
  '3333333333': { transactions: [{ quantity: 2, sku: 'CHARM-A', listing_id: 4455667788, title: 'Stud earrings, gold', variations: [{ formatted_name: 'Size', formatted_value: 'Large' }] }] },   // 2 pieces, a size
  '4444444444': { transactions: [{ quantity: 1, sku: 'SKU-ONE', listing_id: 5566778899, title: 'Charm, silver' }] }
};
function resetBackend(n) {
  colls.clear();
  liveMod._t.seen.clear(); pinDoor.reset(); require(path.join(root, 'netlify/functions/_editPasscode.js')).resetCache();
  data('Brites_Orders').set('Employee Numbers', { [PINS[n - 1]]: NAMES[n - 1] });
  data('Charm_Master_Index').set('CHARM-A', { sku: 'CHARM-A', thumbUrl: '', sizes: { L: { thumbUrl: URL_A } } });
  data('Etsy_Listing_Image_Cache').set('4455667788', { images: [{ rank: 1, url_570xN: URL_PHOTO }] });
}
const stored = name => [...data(name)].map(([id, d]) => Object.assign({ _id: id }, clone(d)));
const storedNoRoster = () => JSON.stringify([...colls].filter(([c]) => c !== 'Brites_Orders').map(([c, m]) => [c, [...m]]).concat([[ 'scanDocs', [...data('Brites_Orders')].filter(([id]) => /scan/.test(id)) ]]));

let ipN = 0;
const ask = async (body) => {      // the portal's reader, a fresh handle each time (no answer cache from an earlier read)
  const r = await eff._t.handle({ httpMethod: 'POST', headers: { 'x-nf-client-connection-ip': '203.0.113.' + (++ipN % 250) }, body: JSON.stringify(Object.assign({ key: PASS }, body)) }, Object.assign({}, fakeDb));
  return JSON.parse(r.body || '{}');
};

/* ── the browser side ── */
const TYPES = { '.html': 'text/html', '.js': 'text/javascript', '.css': 'text/css', '.png': 'image/png', '.jpg': 'image/jpeg', '.mp3': 'audio/mpeg' };
const PNG_1PX = Buffer.from('iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNk+M9QDwADhgGAWjR9awAAAABJRU5ErkJggg==', 'base64');
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
  window.__toasts = []; window.__opens = [];
  const mk = el => { const i = { isOpen: false, open() { i.isOpen = true; window.__opens.push(el && el.id || ''); }, close() { i.isOpen = false; }, destroy() {}, getSelectedValues: () => [] }; if (el) el.__m = i; return i; };
  window.M = { AutoInit() {}, updateTextFields() {}, toast(o) { __toasts.push(String((o && o.html) || '')); return { dismiss() {} }; },
    Modal: { init: el => mk(el), getInstance: el => (el && el.__m) || mk(null) }, FormSelect: { init: () => ({}), getInstance: () => null } };
})();`;
/* appended to the real station-session.js: remembers the page's own StationSession.init options (so a test can call the page's signOut the
   way the library does) and counts what the page calls StationSession.touch for (the phone scan is input at the station) */
const SESSION_WRAP = `
;(function () { try { var S = window.StationSession; if (!S) return; window.__touches = 0;
  var t = S.touch; S.touch = function () { window.__touches++; return typeof t === 'function' ? t.apply(this, arguments) : undefined; };
  var i = S.init; S.init = function (o) { window.__ssCfg = o; return i.apply(this, arguments); }; } catch (e) {} })();`;

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

/** what a page sent to the functions: { url, method, text }; `kinds` keeps the parsed station bodies */
function wireContext(ctx, log, st) {
  const js = body => ({ status: 200, contentType: 'text/javascript', headers: { 'Cross-Origin-Resource-Policy': 'cross-origin' }, body });
  const json = (status, obj) => ({ status, contentType: 'application/json', body: JSON.stringify(obj) });
  return ctx.route('**/*', async route => {
    const req = route.request(), url = new URL(req.url());
    if (url.hostname === '127.0.0.1') {
      if (url.pathname === '/station-session.js') return route.fulfill(js(fs.readFileSync(path.join(root, 'station-session.js'), 'utf8') + SESSION_WRAP));
      if (url.pathname === '/station-timeline.js') return route.fulfill({ status: 404, body: 'not found' });      // the order timeline is not what is tested here
      if (url.pathname.startsWith('/.netlify/functions/')) {
        const fn = url.pathname.split('/').pop(), text = req.postData() || '';
        let body = null; try { body = JSON.parse(text || 'null'); } catch (_) {}
        if (!(body && body.pinLogin !== undefined)) log.requests.push({ url: req.url(), method: req.method(), text });     // (the login door is the one request that carries the number)
        if (fn === 'etsyOrderProxy') {
          const id = url.searchParams.get('orderId');
          if (url.searchParams.get('include') === 'transactions') return route.fulfill(json(200, { transactions: [] }));
          if (!ORDERS[id]) return route.fulfill(json(404, { error: 'Resource not found' }));
          return route.fulfill(json(200, Object.assign({ receipt_id: Number(id), name: 'Test Buyer', status: 'Paid', is_shipped: false }, ORDERS[id])));
        }
        if (fn === 'firebaseOrders') {
          if (req.method() === 'GET') {
            if (/employee/i.test(url.searchParams.get('orderId') || '')) { log.rosterGets++; return route.fulfill(json(401, { success: false })); }
            if (url.searchParams.get('cancelCheck')) return route.fulfill(json(200, { success: true, cancelled: {}, now: Date.now() }));
            return route.fulfill(json(404, { error: 'Order not found' }));
          }
          if (body && body.orderNumber && /^shipping-scan-\d$/.test(body.orderNumber) && st.scannerDown > 0) { st.scannerDown--; log.scannerPosts.push(body); return route.fulfill(json(500, { error: 'down' })); }
          if (body && body.orderNumber && /^shipping-scan-\d$/.test(body.orderNumber)) log.scannerPosts.push(body);
          if (body && body.session) log.sessions.push(body.session);
          if (body && body.live) log.lives.push(body.live);
          if (body && Array.isArray(body.activity)) log.activity.push(...body.activity);
          const out = await door.handler({ httpMethod: 'POST', headers: { 'x-nf-client-connection-ip': '198.51.100.7' }, queryStringParameters: Object.fromEntries(url.searchParams), body: text });
          return route.fulfill({ status: out.statusCode, contentType: 'application/json', body: out.body });
        }
        if (fn === 'trackOrderProxy') return route.fulfill(json(200, { orderId: 987654, orderNumber: body && body.receiptId }));
        if (fn === 'testChitChats') {
          const r = url.searchParams.get('resource');
          if (r === 'shipment') return route.fulfill(json(200, { shipment: { id: url.searchParams.get('id'), carrier: 'usps', carrier_tracking_code: 'TRK123', postage_label_png_url: 'https://chitchats.test/label.png' } }));
          if (r === 'label') return url.searchParams.get('format') === 'png' ? route.fulfill({ status: 200, contentType: 'image/png', body: PNG_1PX }) : route.fulfill(json(404, { error: 'no pdf' }));
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
}

async function until(fn, what, ms = 9000) {
  const t0 = Date.now();
  for (;;) {
    let v; try { v = await fn(); } catch (e) { v = null; }
    if (v) return v;
    if (Date.now() - t0 > ms) throw new Error('timed out: ' + what);
    await new Promise(r => setTimeout(r, 80));
  }
}

async function run(browser, origin, n, full) {
  const WHO = NAMES[n - 1], PIN = PINS[n - 1], DEV = `shipping-${n}`, DOC = `shipping-scan-${n}`;
  resetBackend(n);
  const log = { requests: [], sessions: [], lives: [], activity: [], scannerPosts: [], rosterGets: 0 }, st = { scannerDown: 0 };
  const errors = [];

  // the phone scanner (its own browser profile, like the phone)
  const sctx = await browser.newContext({ viewport: { width: 420, height: 800 } });
  await wireContext(sctx, log, st);
  await sctx.addInitScript(() => { window.alert = () => {}; try { navigator.mediaDevices.getUserMedia = () => Promise.reject(new Error('no camera')); } catch (_) {} });
  const sp = await sctx.newPage(); sp.on('pageerror', e => errors.push('scanner: ' + (e.message || e)));
  await sp.goto(`${origin}/shipping-scan-${n}.html`);
  await sp.waitForFunction(() => typeof handleScannedCode === 'function' && window.M);

  // the desktop page
  const ctx = await browser.newContext({ viewport: { width: 1500, height: 950 } });
  await wireContext(ctx, log, st);
  await ctx.addInitScript(() => {
    localStorage.setItem('access_token', 'tok'); localStorage.setItem('refresh_token', 'ref');
    localStorage.setItem('token_expires_at', String(Math.floor(Date.now() / 1000) + 7200));
    window.alert = () => {}; window.print = () => {};
  });
  const page = await ctx.newPage(); page.on('pageerror', e => errors.push('desktop: ' + (e.message || e)));
  await page.goto(`${origin}/shipping-${n}.html`);
  await page.waitForFunction(() => window.shipTimeline && window.StationActivity && window.__ssCfg && window.__fsSnaps && window.__fsSnaps['Brites_Orders/shipping-scan-' + location.pathname.match(/shipping-(\d)/)[1]]);

  const flush = async () => { await page.evaluate(() => window.StationActivity.flush()); };
  const sessions = () => stored('Station_Sessions');
  const events = () => stored('Station_Activity').sort((a, b) => a.at - b.at || a.seq - b.seq);
  const brief = () => events().map(e => `${e.action}:${e.orderId}:${e.parts || 0}:${e.orders || 0}`);
  const eventsHave = async (action, orderId, why) => { await flush(); return until(async () => { await flush(); return events().find(e => e.action === action && e.orderId === orderId); }, why || (action + ' ' + orderId)); };
  const orderOnScreen = () => page.evaluate(() => document.getElementById('etsyOrderNumber').value);
  const toastsNow = () => page.evaluate(() => window.__toasts.slice());
  const login = async () => {
    await page.evaluate(() => { const i = document.getElementById('employeeNumberInput'); i.dataset.raw = ''; i.value = ''; });
    await page.focus('#employeeNumberInput'); await page.keyboard.type(PIN);
    await page.click('#employeeLoginBtn', { force: true });
    await until(() => page.evaluate(() => window.isEmployeeLoggedIn === true), 'the PIN login works (toasts: ' + 'see below)', 12000).catch(async e => { throw new Error(e.message.replace('see below', JSON.stringify((await toastsNow()).slice(-3))) + ' pin box length ' + (await page.evaluate(() => document.getElementById('employeeNumberInput').value.length))); });
  };
  const typeOrder = async id => {              // a typed scan: one more `typed` scan event for this order
    const typed = () => events().filter(e => e.action === 'scan' && e.orderId === id && e.detail === 'typed').length, n0 = typed();
    await page.fill('#etsyOrderNumber', id);
    await page.press('#etsyOrderNumber', 'Enter');
    await until(async () => { await flush(); return typed() > n0; }, 'typed scan ' + id);
  };
  /** the scanner page reads a code; its write goes to the real door; what the door stored is what Firestore would hand the desktop's listener */
  const phoneScan = async (code, deliverTo) => {
    const before = log.scannerPosts.length;
    await sp.evaluate(c => handleScannedCode(c), code);
    await until(() => log.scannerPosts.length > before, 'the scanner sends ' + code);
    await until(() => { const d = (data('Brites_Orders').get(DOC) || {}); return d['Order Number'] === code; }, 'the door stores the relay write for ' + code);
    const d = clone(data('Brites_Orders').get(DOC));
    await (deliverTo || page).evaluate(([doc, dd]) => { const cbs = window.__fsSnaps['Brites_Orders/' + doc]; cbs.forEach(cb => cb({ exists: true, data: () => dd })); }, [DOC, d]);
    return d;
  };
  const loginState = () => page.evaluate(() => ({ id: localStorage.getItem('employee_id'), name: localStorage.getItem('employee_name'), on: window.isEmployeeLoggedIn, opens: window.__opens.slice(), pin: document.getElementById('employeeNumberInput').value, note: (document.getElementById('stationScanQueueNote') || {}).textContent || '', noteShown: !!(document.getElementById('stationScanQueueNote') && document.getElementById('stationScanQueueNote').style.display !== 'none') }));

  // the first (empty) delivery of the relay listener is skipped by the page on purpose: deliver it, as Firestore does
  await page.evaluate(doc => window.__fsSnaps['Brites_Orders/' + doc].forEach(cb => cb({ exists: true, data: () => ({}) })), DOC);

  /* 1 · nobody signed in: a phone scan from THIS station's scanner waits; nothing is recorded, nobody is on the board */
  const touches00 = await page.evaluate(() => window.__touches);
  const first = await phoneScan('3333333333');
  ok(first['Order Number'] === '3333333333' && first['Employee Name'] === 'ScannerBot', DEV + ': the scanner relays its code under the bot name, to its own doc');
  ok(log.scannerPosts[0].orderNumber === DOC && log.scannerPosts[0].orderNumField === '3333333333' && !('person' in log.scannerPosts[0]), DEV + ': the scanner write is for ' + DOC + ' and names no person');
  const w1 = await until(async () => { const s = await loginState(); return s.noteShown && s; }, DEV + ': the waiting scan is announced');
  ok(/^1 phone scan waiting for a sign-in/.test(w1.note), DEV + ': one small note says the scan waits: ' + w1.note);
  ok((await orderOnScreen()) === '' && stored('Station_Activity').length === 0 && stored('Station_Sessions').length === 0 && stored('Station_Live').length === 0, DEV + ': signed out: nothing loaded, nothing recorded, nobody on the board');
  ok(await page.evaluate(() => window.__touches) > touches00, DEV + ': the relayed scan was told to StationSession.touch (input at the station), even with nobody signed in');

  /* 2 · the PIN sign-in: ONE session under the NAME; the waiting scan loads and is the signed-in person's */
  const t0 = Date.now();
  await login();
  const s1 = await until(() => sessions().find(s => s.person === WHO), DEV + ': a session starts');
  eq([s1.station, s1.device, s1.employeeId, s1.endAt], ['shipping', DEV, '', null], DEV + ': the session is for this page, has no id and is open');
  ok(!JSON.stringify(s1).includes(PIN), DEV + ': no PIN in the session');
  ok(sessions().length === 1, DEV + ': one session');
  const ev1 = await eventsHave('scan', '3333333333', DEV + ': the waiting phone scan loads after the sign-in');
  eq([ev1.person, ev1.station, ev1.device, ev1.session, ev1.parts, ev1.sku, ev1.detail], [WHO, 'shipping', DEV, s1._id, 2, 'CHARM-A', 'phone scan'], DEV + ': it is credited to the person who signed in, with the pieces');
  ok((await orderOnScreen()) === '3333333333', DEV + ': the order is on screen');

  /* 3 · the live order: the board shows it with its pieces, thumbnails, QR text, person and scan time */
  const liveDoc = await until(() => { const d = data('Station_Live').get(`shipping__${DEV}__${WHO}`); return d && d.state === 'working' && d.rid === '3333333333' && d; }, DEV + ': Station_Live holds the order in hand');
  eq([liveDoc.person, liveDoc.device, liveDoc.note, liveDoc.pieces.length], [WHO, DEV, 'phone scan', 2], DEV + ': the live document names the person, the page, how it came in, both pieces');
  const board = await ask({ op: 'live' });
  const sh = board.stations.find(s => s.key === 'shipping');
  const whoNames = x => (Array.isArray(x.names) ? x.names : (x.people || []).map(p => (typeof p === 'string' ? p : p.name)));          // (C4: people are objects now, `names` the plain list)
  eq([sh.state, whoNames(sh), sh.current.length], ['working', [WHO], 1], DEV + ': the board: Shipping is working, with this person');
  const cur = sh.current[0];
  ok(cur.person === WHO && cur.rid === '3333333333' && cur.device === DEV && cur.deviceLabel === `Shipping ${n}` && cur.qr && cur.qr.text === '3333333333' && cur.note === 'phone scan', DEV + ': the card names the order, the person, the page, the QR text, the note');
  ok(cur.pieces.length === 2 && cur.pieces.every(p => p.thumbUrl === URL_A && p.photoUrl === URL_PHOTO), DEV + ': one thumbnail per piece (the design of that size, and the listing photo)');
  ok(cur.thumbUrl === URL_A && cur.scannedAt >= t0 - 1000 && cur.scannedAt <= Date.now() && board.at - cur.scannedAt < 60000, DEV + ': the order picture and the scan time (time since scan is measured from it)');
  ok(sh.devices.find(d => d.device === DEV).state === 'working' && sh.devices.find(d => d.device === DEV).person === WHO, DEV + ': the page shows as working on the Shipping row');
  // the real order card of the board draws it
  const bp = await ctx.newPage(); bp.on('pageerror', e => errors.push('board: ' + (e.message || e)));
  await bp.route('**/__sa3/board.html', r => r.fulfill({ status: 200, contentType: 'text/html', body: '<!doctype html><meta charset=utf-8><body style="margin:20px;font:14px system-ui;background:#f6f7f9"><div id="host" style="max-width:520px"></div><script src="/lib/qrcode.min.js"></script><script src="/charm-nest-efficiency-stations.js"></script></body>' }));
  await bp.route('**/__sa3/pic.png*', r => r.fulfill({ status: 200, contentType: 'image/png', body: PNG_1PX }));          // the pictures of the fixture shop (the card is given addresses on this server)
  await bp.goto(`${origin}/__sa3/board.html`);
  await bp.waitForFunction(() => window.EfficiencyStations && window.QRCode);
  const card = await bp.evaluate(c => {
    const el = EfficiencyStations.orderCard(c); document.getElementById('host').appendChild(el);
    return new Promise(res => setTimeout(() => res({ html: el.outerHTML.slice(0, 2500), text: el.textContent, imgs: [...el.querySelectorAll('img')].map(i => ({ src: (i.getAttribute('src') || '').slice(0, 30), alt: i.alt || '', cls: i.className })), timer: [...el.querySelectorAll('time.esT')].map(x => x.textContent), label: (el.querySelector('.esTl') || {}).textContent || '' }), 600));
  }, (() => { const pic = (u, i) => u ? `${origin}/__sa3/pic.png?${i}` : ''; return Object.assign({}, cur, { station: 'shipping', stationLabel: 'Shipping', thumbUrl: pic(cur.thumbUrl, 'o'), photoUrl: pic(cur.photoUrl, 'o'), vectorUrl: pic(cur.vectorUrl, 'o'),
    pieces: cur.pieces.map((p, i) => Object.assign({}, p, { thumbUrl: pic(p.thumbUrl, 'p' + i), vectorUrl: pic(p.vectorUrl, 'p' + i), photoUrl: pic(p.photoUrl, 'p' + i) })) }); })());
  ok(card.text.includes('3333333333') && card.text.includes(WHO), DEV + ': the board card shows the order number and the person');
  ok(card.timer.length === 1 && /^00:\d\d$/.test(card.timer[0]) && card.label === 'since scanned', DEV + ': the card shows a ticking time since scanned, counted from the scan time: ' + JSON.stringify([card.timer, card.label]));
  ok(card.imgs.length === 4 && card.imgs.filter(i => /^data:image/.test(i.src)).length === 1 && card.imgs.filter(i => /^http:\/\/127/.test(i.src)).length === 3, DEV + ': the card draws the QR and a picture per piece: ' + JSON.stringify(card.imgs.map(i => i.src.slice(0, 12))) + ' ' + card.html);
  if (process.env.SHOTS_DIR && n === 1) { fs.mkdirSync(process.env.SHOTS_DIR, { recursive: true }); await bp.setViewportSize({ width: 600, height: 420 }); await bp.screenshot({ path: path.join(process.env.SHOTS_DIR, 'shipping-board-card.png') }); }
  await bp.close();

  /* 4 · beats: the page leaving sends one (the library beats every five minutes while the page is open) */
  const beatsBefore = log.sessions.filter(b => b.event === 'beat').length;
  await page.evaluate(() => window.dispatchEvent(new Event('pagehide')));
  await until(() => log.sessions.filter(b => b.event === 'beat').length > beatsBefore, DEV + ': a beat is sent');
  const beat = log.sessions.filter(b => b.event === 'beat').pop();
  ok(beat.id === s1._id && beat.person === WHO && beat.station === 'shipping' && beat.device === DEV && !beat.employeeId, DEV + ': the beat is the same session, by name');

  /* 5 · a second phone scan while signed in: input at the station, credited to the person, the order in hand changes */
  const touches0 = await page.evaluate(() => window.__touches);
  await phoneScan('2222222222');
  const ev2 = await eventsHave('scan', '2222222222', DEV + ': the relayed scan loads');
  eq([ev2.person, ev2.session, ev2.parts, ev2.detail], [WHO, s1._id, 1, 'phone scan'], DEV + ': credited to the signed-in person');
  ok(await page.evaluate(() => window.__touches) > touches0, DEV + ': the phone scan is input at the station (StationSession.touch)');
  await until(() => { const d = data('Station_Live').get(`shipping__${DEV}__${WHO}`); return d && d.rid === '2222222222'; }, DEV + ': the order in hand is now the new scan');

  /* 6 · a typed scan, Buy & Print and Complete Order: print and complete events, the order and the person */
  await page.fill('#etsyOrderNumber', '1111111111'); await page.press('#etsyOrderNumber', 'Enter');
  await eventsHave('scan', '1111111111', DEV + ': a typed scan');
  await page.evaluate(() => { document.getElementById('ccShipmentId').value = 'SHIP1'; const b = document.getElementById('ccBuy'); b.disabled = false; b.click(); });
  const pr = await eventsHave('print', '1111111111', DEV + ': Buy & Print');
  await page.evaluate(() => { document.getElementById('trackingNumberInput').value = 'TRK123'; document.getElementById('carrierSelect').value = 'chitchats'; });
  await page.click('#completeOrderBtn');
  const co = await eventsHave('complete', '1111111111', DEV + ': Complete Order');
  eq([pr.detail, pr.person, pr.session], ['Chit Chats label', WHO, s1._id], DEV + ': the label is the person\'s');
  eq([co.parts, co.orders, co.person, co.session, co.device], [3, 1, WHO, s1._id, DEV], DEV + ': Complete Order is one order and its three pieces');
  await flush();
  eq(brief(), ['scan:3333333333:2:0', 'scan:2222222222:1:0', 'scan:1111111111:3:0', 'print:1111111111:0:0', 'complete:1111111111:3:1'], DEV + ': one event per real moment');
  ok(new Set(events().map(e => e.session)).size === 1 && events().every(e => e.person === WHO && e.station === 'shipping' && e.device === DEV), DEV + ': every event joins to the one session');
  await until(() => { const d = data('Station_Live').get(`shipping__${DEV}__${WHO}`); return d && d.state === 'idle'; }, DEV + ': Complete Order puts nothing in hand');

  /* 7 · the numbers agree: board, Overview (station and person), the person's own page */
  const b2 = await ask({ op: 'live' }), ov = await ask({ op: 'overview' }), pv = await ask({ op: 'person', name: WHO, range: 'day' });
  const exp = { parts: 3, scans: 3, scanParts: 6, completes: 1, prints: 1, orders: 3 };
  const shB = b2.stations.find(s => s.key === 'shipping');
  eq(shB.counts, { partsToday: exp.parts, ordersToday: exp.orders, scansToday: exp.scans }, DEV + ': the board\'s numbers');
  const shO = ov.business.stations.find(s => s.station === 'shipping');
  eq([shO.parts, shO.scans, shO.orders], [exp.parts, exp.scans, exp.orders], DEV + ': the Overview station row says the same: ' + JSON.stringify(shO));
  const pO = ov.people.find(p => p.name === WHO), pOs = pO.stations.find(s => s.station === 'shipping');
  eq([pOs.parts, pOs.scans, pOs.scanParts, pOs.completes, pOs.prints, pOs.orders], [exp.parts, exp.scans, exp.scanParts, exp.completes, exp.prints, exp.orders], DEV + ': the Overview person row says the same');
  eq([pO.totals.parts, pO.totals.orders, pO.totals.scans], [exp.parts, exp.orders, exp.scans], DEV + ': the person\'s totals say the same');
  eq([pO.status, pO.nowAt], ['on', ['shipping']], DEV + ': the Overview has the person on now, at Shipping');
  ok(!ov.partial && !b2.partial && !pv.partial, DEV + ': nothing partial: ' + JSON.stringify(ov.errors || b2.errors || pv.errors || ''));
  const pvs = (pv.stations || []).find(s => s.station === 'shipping');
  eq([pvs.parts, pvs.scans, pvs.orders, pvs.completes, pvs.prints], [exp.parts, exp.scans, exp.orders, exp.completes, exp.prints], DEV + ': the person\'s page says the same');
  eq([pv.kpis.parts.value, pv.kpis.orders.value, pv.kpis.ordersCompleted.value, pv.kpis.scans.value, pv.kpis.prints.value], [exp.parts, exp.orders, exp.completes, exp.scans, exp.prints], DEV + ': the person\'s figures say the same');
  ok(ov.business.totals.parts === exp.parts && ov.business.totals.orders === exp.orders, DEV + ': the business total counts these pieces and orders once');

  /* 8 · the page's own signOut for each new end reason: the login goes, the PIN box returns, the work stays, a scan waits */
  const reasons = full ? [['idle', /Signed out after 10 minutes without input/], ['closing', /Signed out at 5:00 pm/], ['midnight', /Signed out at midnight/]] : [['idle', /Signed out after 10 minutes without input/], ['closing', /Signed out at 5:00 pm/]];
  let prevSession = s1, lastScan = '2222222222';
  for (const [reason, words] of reasons) {
    await typeOrder('2222222222');                                             // an order in hand again (typed)
    await page.evaluate(() => { document.getElementById('trackingNumberInput').value = 'KEEP123'; });
    const opens0 = (await loginState()).opens.length, nT = (await toastsNow()).length;
    await page.evaluate(r => window.__ssCfg.signOut(r), reason);              // what the library does when it signs the page out
    const ls = await loginState(), tt = (await toastsNow()).slice(nT);
    ok(ls.id === null && ls.name === null && ls.on === false && ls.pin === '', DEV + ' ' + reason + ': the page\'s own login is cleared');
    ok(ls.opens.length > opens0 && ls.opens[ls.opens.length - 1] === 'userLoginModal', DEV + ' ' + reason + ': its PIN box comes back');
    ok(tt.length === 1 && words.test(tt[0]), DEV + ' ' + reason + ': one calm line says why: ' + tt.join(' | '));
    ok((await orderOnScreen()) === '2222222222' && await page.evaluate(() => document.getElementById('trackingNumberInput').value) === 'KEEP123', DEV + ' ' + reason + ': the order and the fields stay on screen');
    await page.evaluate(() => window.dispatchEvent(new Event('focus')));          // the library's own tick: nobody is signed in any more
    const ended = await until(() => { const s = sessions().find(x => x._id === prevSession._id); return s && s.endAt != null && s; }, DEV + ' ' + reason + ': the session ends');
    ok(ended.person === WHO && typeof ended.endReason === 'string', DEV + ' ' + reason + ': the session is ended (' + ended.endReason + ')');
    if (full && reason === 'idle') {
      // the board stops showing the order of the person who was signed out
      const gone = await until(async () => { const b = await ask({ op: 'live' }); const s = b.stations.find(x => x.key === 'shipping'); return s.current.length === 0 && b; }, DEV + ': the board lets go of the signed-out person\'s order', 14000);
      ok(gone.stations.find(x => x.key === 'shipping').current.length === 0, DEV + ': nothing in hand on the board');
    }
    // a phone scan meanwhile waits (it is never dropped); it is not loaded over the order on screen
    const nEv = events().length;
    lastScan = lastScan === '4444444444' ? '1111111111' : '4444444444';
    await phoneScan(lastScan);
    const w = await until(async () => { const s = await loginState(); return s.noteShown && s; }, DEV + ' ' + reason + ': the waiting scan is announced');
    ok(/^1 phone scan waiting for a sign-in/.test(w.note) && (await orderOnScreen()) === '2222222222' && events().length === nEv, DEV + ' ' + reason + ': the scan waits, nothing is loaded or recorded');
    // the next sign-in is a new session and picks it up
    await login();
    const s2 = await until(() => sessions().find(s => s._id !== prevSession._id && s.endAt == null && s.person === WHO), DEV + ' ' + reason + ': the next sign-in is a new session');
    const evx = await until(async () => { await flush(); return events().filter(e => e.action === 'scan' && e.orderId === lastScan && e.session === s2._id).pop(); }, DEV + ' ' + reason + ': the waiting scan loads under the new session');
    eq([evx.person, evx.detail], [WHO, 'phone scan'], DEV + ' ' + reason + ': credited to the person who signed in');
    prevSession = s2;
  }

  /* 9 · Sign Out ends the session; no PIN anywhere */
  await page.click('#signOutBtn');
  const last = await until(() => { const s = sessions().find(x => x._id === prevSession._id); return s && s.endAt != null && s; }, DEV + ': Sign Out ends the session');
  ok(last.endReason === 'signOut', DEV + ': Sign Out ends it as signOut');
  await flush();
  ok(!storedNoRoster().includes(PIN), DEV + ': a PIN is in a stored session, event, live document or relay doc');
  for (const r of log.requests) ok(!r.url.includes(PIN) && !r.text.includes(PIN), DEV + ': a PIN is in a request to ' + r.url.split('/').pop());
  ok(log.rosterGets === 0, DEV + ': the roster was never asked for');
  ok(!(await toastsNow()).some(t => t.includes(PIN)), DEV + ': a PIN is in a toast');
  ok(await page.evaluate(PIN => Object.entries(localStorage).filter(([k]) => k !== 'employee_id').every(([, v]) => !String(v).includes(PIN)), PIN), DEV + ': a PIN is in the browser storage (the page\'s own employee_id apart, and that is gone after Sign Out)');
  ok(await page.evaluate(() => localStorage.getItem('employee_id') === null), DEV + ': Sign Out clears the login');

  /* 10 · the scanner: a failed send is tried again, not skipped as "already scanned" (first page only: the code is shared) */
  let retry = '';
  if (full) {
    st.scannerDown = 1;
    const posts0 = log.scannerPosts.length;
    await sp.evaluate(() => handleScannedCode('7777777777'));
    await until(() => log.scannerPosts.length > posts0, DEV + ': the scanner tries');
    await sp.evaluate(() => handleScannedCode('7777777777'));                    // the camera sees the same code again straight away: still the same attempt
    await new Promise(r => setTimeout(r, 400));
    ok(log.scannerPosts.length === posts0 + 1, DEV + ': the same code is not sent twice at once');
    await sp.waitForTimeout(3300);
    await sp.evaluate(() => handleScannedCode('7777777777'));                    // a few seconds later the camera reads it again: it goes through
    await until(() => (data('Brites_Orders').get(DOC) || {})['Order Number'] === '7777777777', DEV + ': the retried code reaches the door');
    retry = ', a failed scanner send is retried';
  }
  ok(errors.length === 0, DEV + ': no page errors: ' + errors.join(' | ').slice(0, 300));
  await sctx.close(); await ctx.close();
  return `${DEV}: sign-in, beat, ${events().length} events, board = Overview = person page, ${reasons.map(r => r[0]).join('/')} sign-outs${retry}`;
}

(async () => {
  const srv = await staticServer();
  const origin = `http://127.0.0.1:${srv.address().port}`;
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  const t0 = Date.now(), out = [];
  try {
    // the three desktop pages and the three scanners are near copies: the wiring is the same text (only the number differs)
    for (const f of ['shipping', 'shipping-scan']) {
      const src = [1, 2, 3].map(n => fs.readFileSync(path.join(root, `${f}-${n}.html`), 'utf8'));
      const marks = f === 'shipping'
        ? ['signOut: reason => {', 'Signed out after 10 minutes without input', 'Signed out at 5:00 pm', 'StationSession.touch()', 'StationScanQueue.create']
        : ['not sent', 'lastScanned = ""'];
      for (const m of marks) ok(src.every(s => s.includes(m)), `${f}-1..3: the same wiring (${m})`);
      const norm = s => s.replace(/shipping-scan-\d/g, 'shipping-scan-N').replace(/shipping-\d/g, 'shipping-N').replace(/Shipping_\d/g, 'Shipping_N');
      if (f === 'shipping-scan') ok(norm(src[0]) === norm(src[1]) && norm(src[1]) === norm(src[2]), 'the three scanner pages differ only by their number');
    }
    out.push(await run(browser, origin, 1, true));
    out.push(await run(browser, origin, 2, false));
    out.push(await run(browser, origin, 3, false));
    say(`shipping wired OK in ${((Date.now() - t0) / 1000).toFixed(1)} s`);
    for (const o of out) say('  ' + o);
  } finally { await browser.close(); srv.close(); }
})().catch(e => { console.error = console.log; console.log(e && e.stack || e); process.exit(1); });
