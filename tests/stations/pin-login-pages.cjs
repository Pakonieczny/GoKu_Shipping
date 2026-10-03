// How the ten PIN pages (weld-1, assembly-1..4, shipping-1..3, design-message, design-message-1) ask who a typed Employee
// Number is: the real pages and station-session.js, the real firebaseOrders handler (its {pinLogin} door) over a fake
// Firestore, plus faked failures. Every PIN is made up when this test runs and never printed (booleans only in the checks).
//   1 · the door answers yes: the person is welcomed by name, a session starts with the name, the page NEVER asks for the
//       roster document, and the only request that carries the number is { pinLogin } (never a URL)
//   2 · the door answers no (ok:false): the refusal text stays, the box is cleared, nobody is signed in, and the page does
//       NOT fall back to the roster: a "no" is final, even when the roster would have said yes
//   3 · the door answers "too many tries": that text is shown, no fall-back, nobody is signed in
//   4 · the door cannot be reached (no network, 500, 503, an older server that answers 400 without `ok`, an HTML 502, a hang
//       past the page's timeout): the page falls back to the old roster read, once, and the person still signs in
//   5 · unreachable and the roster read refused: a clear error toast, nobody signed in
//   6 · end to end (weld-1): ten wrong tries over the real door start its lockout; a right number is then told to wait
//   NODE_PATH=… PW_DIR=… CHROMIUM=… node tests/stations/pin-login-pages.cjs
'use strict';
const fs = require('fs'), path = require('path'), Module = require('module');
const root = path.join(__dirname, '../..');
const pwDir = process.argv[2] || process.env.PW_DIR || path.join(root, 'node_modules');
const { chromium } = require(path.join(pwDir, 'playwright-core'));
const ORIGIN = 'http://pinlogin.test';
const PAGES = [['weld-1', 'welding'], ['assembly-1', 'assembly'], ['assembly-2', 'assembly'], ['assembly-3', 'assembly'], ['assembly-4', 'assembly'],
  ['shipping-1', 'shipping'], ['shipping-2', 'shipping'], ['shipping-3', 'shipping'], ['design-message', 'design'], ['design-message-1', 'design']];
const ok = (cond, msg) => { if (!cond) throw new Error(msg); };       // booleans only: nothing secret can end up in a failure
const wait = ms => new Promise(r => setTimeout(r, ms));

const used = new Set();
const fakePin = () => { for (;;) { const p = String(100000 + Math.floor(Math.random() * 900000)); if (!/^(\d)\1{5}$/.test(p) && !used.has(p)) { used.add(p); return p; } } };
const seesPin = text => [...used].some(p => String(text).includes(p));
const PIN = { Giovanna: fakePin(), Anna: fakePin(), Michael: fakePin(), Ivy: fakePin(), Spacey: fakePin() };
const ROSTER = { [PIN.Giovanna]: 'Giovanna', [PIN.Anna]: 'Anna', [PIN.Michael]: 'Michael', [PIN.Ivy]: 'Ivy', [PIN.Spacey]: '  Anna   B.  ' };

/* ── the real firebaseOrders handler over a fake Firestore (a Map) ── */
const docs = new Map([['Brites_Orders/Employee Numbers', ROSTER]]);
const ref = p => ({ path: p, id: p.split('/').pop(), get: async () => ({ exists: docs.has(p), data: () => docs.get(p) }) });
const fakeDb = {
  collection: c => ({ doc: id => ref(c + '/' + id) }),
  runTransaction: async fn => { const w = []; const r = await fn({ get: x => x.get(), set: (x, d, o) => w.push([x.path, d, o]) }); for (const [p, d, o] of w) docs.set(p, o && o.merge ? Object.assign({}, docs.get(p) || {}, d) : d); return r; }
};
const fakeAdmin = { firestore: Object.assign(() => fakeDb, { FieldValue: { serverTimestamp: () => 'ts', delete: () => null } }) };
const realLoad = Module._load;
Module._load = function (req, ...rest) { if (/[\/]firebaseAdmin(\.js)?$/.test(req)) return fakeAdmin; return realLoad.call(this, req, ...rest); };
const door = require(path.join(root, 'netlify/functions/firebaseOrders.js'));
const pinDoor = require(path.join(root, 'netlify/functions/_stationPinLogin.js'));
Module._load = realLoad;
pinDoor.deps.sleep = async () => {};                                 // a wrong try's pause is not waited for here

const fbStub = `window.firebase = (() => {
  const snap = (exists, data) => ({ exists, data: () => data || {}, get: f => (data || {})[f] });
  const doc = (c, id) => ({ id, onSnapshot(cb) { try { cb(snap(false)); } catch (_) {} return () => {}; },
    set: async () => {}, update: async () => {}, get: async () => snap(false), collection: n => col(c + '/' + id + '/' + n) });
  const col = c => { const q = { doc: id => doc(c, id), where: () => q, orderBy: () => q, limit: () => q, limitToLast: () => q, startAfter: () => q, add: async () => ({ id: 'x' }),
    onSnapshot(cb) { try { cb({ docs: [], empty: true, size: 0, forEach() {}, docChanges: () => [] }); } catch (_) {} return () => {}; },
    get: async () => ({ docs: [], empty: true, size: 0, forEach() {} }) }; return q; };
  const firestore = () => ({ collection: col, batch: () => ({ set() {}, update() {}, delete() {}, commit: async () => {} }), runTransaction: async () => {} });
  firestore.FieldValue = { delete: () => null, serverTimestamp: () => null, arrayUnion: () => null, increment: () => null };
  firestore.Timestamp = { now: () => ({ toMillis: () => Date.now() }), fromMillis: ms => ({ toMillis: () => ms }) };
  return { initializeApp() {}, firestore, auth: () => ({ signInAnonymously: async () => ({}), onAuthStateChanged(cb) { try { cb({ uid: 'u' }); } catch (_) {} return () => {}; }, currentUser: { uid: 'u' } }) };
})();`;
const mStub = `window.__toasts = [];
  window.M = { AutoInit() {}, toast(o) { __toasts.push(o && o.html); }, updateTextFields() {}, textareaAutoResize() {},
    Modal: { init(el) { const i = { open() { i.isOpen = true; }, close() { i.isOpen = false; }, isOpen: false }; if (el) el.__m = i; return i; },
             getInstance(el) { return (el && el.__m) || { open() {}, close() {} }; } },
    FormSelect: { init() { return {}; }, getInstance() { return { getSelectedValues: () => [] }; } } };`;

const allReqs = [];                                                  // every request any page sent: { kind, url, text }
async function run(browser, dev, station, ip) {
  const mode = { op: 'live', map: 'doc' }, reqs = [], hung = [];
  const ctx = await browser.newContext({ viewport: { width: 1400, height: 950 } });
  await ctx.route(/.*/, async r => {
    const u = new URL(r.request().url()), m = r.request().method();
    const json = (body, status = 200) => r.fulfill({ status, contentType: 'application/json', body: JSON.stringify(body) });
    if (/code\.jquery\.com/.test(u.host)) return r.fulfill({ status: 200, contentType: 'text/javascript', body: 'window.$=window.jQuery=()=>({on(){},ready(){}});' });
    if (/materialize/.test(u.pathname)) return r.fulfill({ status: 200, contentType: /\.css$/.test(u.pathname) ? 'text/css' : 'text/javascript', body: /\.css$/.test(u.pathname) ? '' : mStub });
    if (/gstatic\.com/.test(u.host)) return r.fulfill({ status: 200, contentType: 'text/javascript', body: /firebase-app-compat/.test(u.pathname) ? fbStub : '' });
    if (u.origin !== ORIGIN) return r.abort();                                   // nothing leaves the machine
    if (u.pathname.startsWith('/.netlify/functions/')) {
      const text = r.request().postData() || '', qs = Object.fromEntries(u.searchParams);
      let b = {}; try { b = JSON.parse(text || '{}'); } catch (_) {}
      const isDoor = u.pathname.endsWith('/firebaseOrders');
      if (isDoor && m === 'GET' && /employee/i.test(qs.orderId || '')) {          // the old roster read
        const q = { kind: 'map', url: u.pathname + u.search, text }; reqs.push(q); allReqs.push(q);
        return mode.map === 'doc' ? json({ success: true, data: ROSTER }) : json({ success: false, error: 'closed' }, 401);
      }
      if (isDoor && m === 'POST' && b.pinLogin !== undefined) {                   // the login door
        const q = { kind: 'op', url: u.pathname + u.search, text }; reqs.push(q); allReqs.push(q);
        switch (mode.op) {
          case 'live': { const out = await door.handler({ httpMethod: m, headers: { 'x-nf-client-connection-ip': ip }, queryStringParameters: qs, body: text }); return r.fulfill({ status: out.statusCode, contentType: 'application/json', body: out.body }); }
          case 'no': return json({ ok: false, error: 'that Employee Number is not on the list' });
          case 'many': return json({ ok: false, tooMany: true, error: 'Too many tries, wait a minute' }, 429);
          case 'abort': return r.abort('failed');
          case '500': return json({ error: 'boom' }, 500);
          case '503': return json({ error: 'the sign-in list is not available' }, 503);
          case 'stale400': return json({ error: 'No actionable fields provided' }, 400);       // what a server that does not know { pinLogin } answers
          case 'html502': return r.fulfill({ status: 502, contentType: 'text/html', body: '<html><body>Bad gateway</body></html>' });
          case 'hang': await new Promise(res => hung.push(res)); return r.abort('failed').catch(() => {});
        }
      }
      if (isDoor && m === 'POST' && b.session) {
        reqs.push({ kind: 'session', url: u.pathname, text }); allReqs.push({ kind: 'session', url: u.pathname + u.search, text });
        const out = await door.handler({ httpMethod: m, headers: { 'x-nf-client-connection-ip': ip }, queryStringParameters: qs, body: text });
        return r.fulfill({ status: out.statusCode, contentType: 'application/json', body: out.body });
      }
      allReqs.push({ kind: 'other', url: u.pathname + u.search, text });
      if (qs.cancelCheck) return json({ success: true, cancelled: {}, now: Date.now() });
      return json({ success: true, data: {} });
    }
    const file = path.join(root, decodeURIComponent(u.pathname));
    if (file.startsWith(root) && fs.existsSync(file) && fs.statSync(file).isFile()) return r.fulfill({ status: 200, path: file });
    return r.fulfill({ status: 404, body: 'not here' });
  });
  const page = await ctx.newPage();
  const errors = []; page.on('pageerror', e => { if (!/gstatic\.com\/firebasejs/.test(String(e))) errors.push(String(e && e.message || e)); });
  await page.clock.install({ time: new Date('2026-10-05T14:00:00Z') });            // mid-morning in New York: no midnight in sight
  await page.goto(ORIGIN + '/' + dev + '.html');
  await page.waitForFunction(() => window.StationSession && (document.querySelector('#userLoginModal .station-session-pc') || /design-message/.test(location.pathname))
    && document.getElementById('userLoginModal').__m && document.getElementById('userLoginModal').__m.isOpen, null, { timeout: 15000 });
  await wait(300);

  const count = k => reqs.filter(q => q.kind === k).length;
  const state = () => page.evaluate(() => ({ name: localStorage.getItem('employee_name'), toasts: window.__toasts.join(' | '), raw: document.getElementById('employeeNumberInput').dataset.raw || '',
    boxOpen: !!document.getElementById('userLoginModal').__m.isOpen }));
  const starts = () => reqs.filter(q => q.kind === 'session').map(q => JSON.parse(q.text).session).filter(s => s.event === 'start');
  /* type a number, press Log In, and wait for the page to say something (a toast) */
  const attempt = async (pin, { hang = false } = {}) => {
    await page.evaluate(() => { const i = document.getElementById('employeeNumberInput'); i.dataset.raw = ''; i.value = ''; window.__toasts.length = 0; });
    await page.focus('#employeeNumberInput'); await page.keyboard.type(pin);
    await page.click('#employeeLoginBtn', { force: true });
    if (hang) { for (let i = 0; i < 100 && !reqs.some(q => q.kind === 'op'); i++) await wait(30); await page.clock.runFor(16000); }
    for (let i = 0; i < 300; i++) { const s = await state(); if (s.toasts) return s; await wait(30); }
    throw new Error(dev + ': the page said nothing');
  };
  const signOut = async () => {
    await page.click('#signOutBtn', { force: true });
    for (let i = 0; i < 100; i++) { if (!(await state()).name) return; await wait(30); }
    throw new Error(dev + ': sign out did not clear the login');
  };
  const reset = () => { reqs.length = 0; pinDoor.reset(); mode.op = 'live'; mode.map = 'doc'; hung.length = 0; };

  // 1 · the door says yes: welcomed, a session with the name, no roster read, the number only in { pinLogin }
  reset();
  for (const name of ['Giovanna', 'Anna', 'Michael', 'Ivy']) {
    const n0 = starts().length;
    const s = await attempt(PIN[name]);
    ok(s.toasts.includes('Welcome, ' + name + '!') && s.name === name, dev + ': ' + name + ' is welcomed by name');
    for (let i = 0; i < 100 && starts().length === n0; i++) await wait(30);
    ok(starts().length === n0 + 1 && starts().pop().person === name && starts().pop().station === station, dev + ': a session starts for ' + name);
    await signOut();
  }
  ok(count('map') === 0, dev + ': the page asked for the roster document although the door answered');
  ok(count('op') === 4 && reqs.filter(q => q.kind === 'op').every(q => /^\{"pinLogin":"\d{6}"\}$/.test(q.text) && q.url === '/.netlify/functions/firebaseOrders'), dev + ': the number goes only in a { pinLogin } body to the door');
  // the name is tidied as stored
  const sp = await attempt(PIN.Spacey);
  ok(sp.name === 'Anna B.' && sp.toasts.includes('Welcome, Anna B.!'), dev + ': a name with stray spaces is tidied');
  await signOut();

  // 2 · the door says no: final, the refusal text stays, no roster read even though the roster has the number
  reset(); mode.op = 'no';
  let s = await attempt(PIN.Anna);
  ok(/That Employee Number is not on the list\. Check the 6 digits and try again\./.test(s.toasts) && !s.name && s.boxOpen && s.raw === '', dev + ': a "no" shows the refusal, signs nobody in and clears the box');
  ok(count('map') === 0, dev + ': a "no" was not checked against the roster');
  reset(); mode.op = 'live';
  s = await attempt(fakePin());
  ok(/not on the list/.test(s.toasts) && !seesPin(s.toasts) && !s.name && count('map') === 0, dev + ': a number that is not on the list is refused by the real door, without a roster read');

  // 3 · too many tries: shown, final
  reset(); mode.op = 'many';
  s = await attempt(PIN.Anna);
  ok(s.toasts.includes('Too many tries, wait a minute') && !s.name && s.raw === '' && count('map') === 0, dev + ': "too many tries" is shown, nobody is signed in, the roster is not read');

  // 4 · the door cannot be reached: the old roster read, once, and the person still signs in
  for (const how of ['abort', '500', '503', 'stale400', 'html502', 'hang']) {
    reset(); mode.op = how;
    s = await attempt(PIN.Michael, { hang: how === 'hang' });
    ok(s.name === 'Michael' && s.toasts.includes('Welcome, Michael!'), dev + ': with the door ' + how + ' the person still signs in');
    ok(count('map') === 1 && count('op') === 1, dev + ': with the door ' + how + ' the roster was read once and the door asked once');
    await signOut();
    hung.forEach(res => res());
  }
  // a number that is not on the old roster is refused the old way too
  reset(); mode.op = '500';
  s = await attempt(fakePin());
  ok(/not on the list/.test(s.toasts) && !s.name && count('map') === 1, dev + ': the fall-back refuses an unknown number as before');

  // 5 · unreachable and the roster read refused
  reset(); mode.op = 'abort'; mode.map = 'closed';
  s = await attempt(PIN.Michael);
  ok(/Error fetching Employee Numbers doc!/.test(s.toasts) && !s.name, dev + ': nothing reachable: a clear error and nobody signed in');

  // 6 · end to end: ten wrong tries over the real door start its lockout
  if (dev === 'weld-1') {
    reset(); mode.op = 'live';
    for (let i = 0; i < 10; i++) { s = await attempt(fakePin()); ok(/not on the list/.test(s.toasts), dev + ': wrong try ' + (i + 1) + ' is refused'); }
    s = await attempt(PIN.Giovanna);
    ok(s.toasts.includes('Too many tries, wait a minute') && !s.name && count('map') === 0, dev + ': after ten wrong tries even a right number is told to wait, and the roster is not read');
  }

  ok(errors.length === 0, dev + ': page errors: ' + errors.join('; ').replace(/\d{6}/g, '#'));
  await ctx.close();
}

(async () => {
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  try {
    let i = 0;
    for (const [dev, station] of PAGES) { await run(browser, dev, station, '203.0.113.' + (50 + i++)); console.log(dev + ': door yes/no/too many, fall-back on an unreachable door, nothing asked for the roster when the door answers'); }
    ok(!allReqs.some(q => seesPin(q.url)), 'a PIN was sent in a URL');
    ok(!allReqs.some(q => seesPin(q.text) && q.kind !== 'op'), 'a PIN was sent in a request that is not the login door\'s');
    console.log('pin-login-pages: all passed (' + allReqs.filter(q => q.kind === 'op').length + ' door requests, ' + allReqs.filter(q => q.kind === 'map').length + ' fall-back roster reads, no PIN in any URL)');
  } finally { await browser.close(); }
})().catch(e => { console.error('FAILED:', String(e && e.message || e).replace(/\d{6}/g, '#')); process.exit(1); });
