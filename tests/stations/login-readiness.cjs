// Login readiness for the seven names on Paul's PIN list (Giovanna C., Empress D., Michael_V, Michelle_R, Ivy_Y, Ana_M, Paul_K: the older style with a period next to the underscore style) at the PIN stations: the real station pages
// (weld-1, assembly-1..4, shipping-1..3, design-message, design-message-1), the real station-session.js and the real
// firebaseOrders handler (its {pinLogin} door) over a fake Firestore. The sign-in goes through the server's login door: no page
// may ask for the roster document, and a PIN may be in no URL and in no request body except the login door's own.
// Fake employees only: the PINs are made up when this test runs, never written anywhere and never put in a message
// (the checks below assert booleans only, so a failure cannot print one). Every request that is not the page itself or
// its stubs is aborted. The clock is Playwright's, set to 23:35 New York and run past midnight.
//   1 · each of the seven signs in: a session starts with that name, and no PIN is in any URL, in any request but the login
//       door's, in the sessions store, in the toasts or (the login's own employee_id apart) in the browser's storage; Sign Out
//       ends it ("signOut")
//   2 · a PIN that is not on the list, or a short one, is refused with a clear message that does not echo the digits;
//       no session starts and the PIN box stays
//   3 · the midnight sign-out ends the session ("midnight"), brings the PIN box back and clears the login; the next
//       day the same person signs in again and gets a new session
//   NODE_PATH=$(npm root -g) PW_DIR=$(npm root -g)/playwright/node_modules CHROMIUM=… node tests/stations/login-readiness.cjs
'use strict';
const fs = require('fs'), path = require('path'), Module = require('module');
const root = path.join(__dirname, '../..');
const pwDir = process.argv[2] || process.env.PW_DIR || path.join(root, 'node_modules');
const { chromium } = require(path.join(pwDir, 'playwright-core'));
const ORIGIN = 'http://station.test';
const MIDNIGHT = Date.parse('2026-09-29T04:00:00Z');               // 00:00 on 29 Sep in New York (EDT)
const NAMES = ['Giovanna C.', 'Empress D.', 'Michael_V', 'Michelle_R', 'Ivy_Y', 'Ana_M', 'Paul_K'];   // as stored in the list: the underscore and the period must survive the login, the session and the welcome
const PAGES = [['weld-1', 'welding'], ['assembly-1', 'assembly'], ['assembly-2', 'assembly'], ['assembly-3', 'assembly'], ['assembly-4', 'assembly'],
  ['shipping-1', 'shipping'], ['shipping-2', 'shipping'], ['shipping-3', 'shipping'], ['design-message', 'design'], ['design-message-1', 'design']];

const ok = (cond, msg) => { if (!cond) throw new Error(msg); };       // booleans only: nothing secret can end up in a failure
const pins = new Map();                                              // name -> a made-up 6-digit PIN (this run only)
const fakePin = () => { for (;;) { const p = String(100000 + Math.floor(Math.random() * 900000)); if (!/^(\d)\1{5}$/.test(p) && ![...pins.values()].includes(p)) return p; } };
NAMES.forEach(n => pins.set(n, fakePin()));
const unknownPin = fakePin();                                        // on nobody's record
const anyPin = text => [...pins.values(), unknownPin].some(p => String(text).includes(p));

/* ── the real firebaseOrders handler over a fake Firestore (a Map) ── */
const docs = new Map();
docs.set('Brites_Orders/Employee Numbers', Object.fromEntries([...pins].map(([n, p]) => [p, n])));
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
const stored = () => [...docs].filter(([k]) => k.startsWith('Station_Sessions/')).map(([, v]) => v);

const fbStub = `window.firebase = (() => {
  const snap = (exists, data) => ({ exists, data: () => data || {}, get: f => (data || {})[f] });
  const doc = (c, id) => ({ id, onSnapshot(cb) { try { cb(snap(false)); } catch (_) {} return () => {}; },
    set: async () => {}, update: async () => {}, get: async () => snap(false), collection: n => col(c + '/' + id + '/' + n) });
  const col = c => { const q = { doc: id => doc(c, id), where: () => q, orderBy: () => q, limit: () => q, add: async () => ({ id: 'x' }),
    onSnapshot(cb) { try { cb({ docs: [], empty: true, size: 0, forEach() {}, docChanges: () => [] }); } catch (_) {} return () => {}; },
    get: async () => ({ docs: [], empty: true, size: 0, forEach() {} }) }; return q; };
  const firestore = () => ({ collection: col });
  firestore.FieldValue = { delete: () => null, serverTimestamp: () => null, arrayUnion: () => null };
  return { initializeApp() {}, firestore, auth: () => ({ signInAnonymously: async () => ({}), onAuthStateChanged() {} }) };
})();`;
const mStub = `window.__toasts = [];
  window.M = { AutoInit() {}, toast(o) { __toasts.push(o && o.html); }, updateTextFields() {},
    Modal: { init(el) { const i = { open() { i.isOpen = true; }, close() { i.isOpen = false; }, isOpen: false }; if (el) el.__m = i; return i; },
             getInstance(el) { return (el && el.__m) || { open() {}, close() {} }; } },
    FormSelect: { init() { return {}; }, getInstance() { return { getSelectedValues: () => [] }; } } };`;

let mapGets = 0;                                                     // the roster document must never be asked for
async function run(browser, dev, station, all, allReqs, ip) {
  const seen = [];                                                               // what this page sent
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
      seen.push(m + ' ' + u.pathname + u.search + ' ' + text);
      allReqs.push({ m, url: u.pathname + u.search, text });
      let b = {}; try { b = JSON.parse(text || '{}'); } catch (_) {}
      if (u.pathname.endsWith('/firebaseOrders') && m === 'GET' && /employee/i.test(qs.orderId || '')) { mapGets++; return json({ success: false, error: 'closed' }, 401); }
      const real = u.pathname.endsWith('/firebaseOrders') && m === 'POST' && (b.session || b.pinLogin !== undefined);
      if (real) {
        const out = await door.handler({ httpMethod: m, headers: { 'x-nf-client-connection-ip': ip }, queryStringParameters: qs, body: text });
        return r.fulfill({ status: out.statusCode, contentType: 'application/json', body: out.body });
      }
      if (qs.cancelCheck) return json({ success: true, cancelled: {}, now: Date.now() });
      return json({ success: true, data: {} });
    }
    const file = path.join(root, decodeURIComponent(u.pathname));
    if (file.startsWith(root) && fs.existsSync(file) && fs.statSync(file).isFile()) return r.fulfill({ status: 200, path: file });
    return r.fulfill({ status: 404, body: 'not here' });
  });
  const page = await ctx.newPage();
  const errors = []; page.on('pageerror', e => { if (!/gstatic\.com\/firebasejs/.test(String(e))) errors.push(String(e && e.message || e)); });
  await page.clock.install({ time: new Date(MIDNIGHT - 25 * 60000) });          // 23:35 in New York
  await page.goto(ORIGIN + '/' + dev + '.html');
  await page.waitForFunction(() => window.StationSession && (document.querySelector('#userLoginModal .station-session-pc') || /design-message/.test(location.pathname)) && document.getElementById('userLoginModal').__m && document.getElementById('userLoginModal').__m.isOpen, null, { timeout: 15000 });

  const until = async (fn, what) => { for (let i = 0; i < 100; i++) { const v = await fn(); if (v) return v; await page.clock.runFor(100); } throw new Error('timed out: ' + what); };
  const login = async pin => {
    await page.evaluate(() => { const i = document.getElementById('employeeNumberInput'); i.dataset.raw = ''; i.value = ''; window.__toasts.length = 0; });
    await page.focus('#employeeNumberInput'); await page.keyboard.type(pin);
    await page.click('#employeeLoginBtn', { force: true });
    if (dev === 'weld-1' && [...pins.values()].includes(pin)) {                    // the Welding station asks "Welding or Matching?" after a number it knows
      await until(() => page.evaluate(() => document.getElementById('userLoginModal').classList.contains('weld-task')), dev + ': the Welding or Matching step');
      await page.evaluate(() => document.getElementById('weldTaskMatching').click());
    }
  };
  /* (weld-1 keeps who is signed in as names under tasks in weld_people, and no longer writes employee_id / employee_name) */
  const state = () => page.evaluate(() => ({ name: (() => { const r = localStorage.getItem('weld_people'); if (r !== null) { try { const l = JSON.parse(r); return l.length ? l[0].name : null; } catch (_) {} } return localStorage.getItem('employee_name'); })(),
    idSet: (() => { const r = localStorage.getItem('weld_people'); if (r !== null) { try { return JSON.parse(r).length > 0; } catch (_) {} } return !!localStorage.getItem('employee_id'); })(), toasts: window.__toasts.join(' | '),
    boxOpen: !!document.getElementById('userLoginModal').__m.isOpen, store: Object.entries(localStorage).filter(([k]) => k !== 'employee_id').map(([k, v]) => k + '=' + v).join('\n'),
    all: Object.entries(localStorage).map(([k, v]) => k + '=' + v).join('\n') }));
  const sessionsOf = name => stored().filter(s => s.person === name && s.device === dev);
  const events = () => seen.filter(t => t.startsWith('POST') && t.includes('"session"')).map(t => JSON.parse(t.slice(t.indexOf('{')))).map(b => b.session);

  // 1 · each of the seven signs in and out
  for (const name of NAMES) {
    await login(pins.get(name));
    const start = await until(async () => events().find(e => e.event === 'start' && e.person === name), dev + ': a session starts for ' + name);
    const st = await state();
    ok(start.station === station && start.device === dev, dev + ': the session names the station and the page');
    ok(start.employeeId === '', dev + ': no id sent');
    ok(st.name === name && st.toasts.includes('Welcome, ' + name + '!'), dev + ': ' + name + ' is welcomed by name');
    ok(!anyPin(st.store) && !anyPin(st.toasts), dev + ': a PIN is in the browser storage or a toast (besides the login\'s own employee_id)');
    ok(sessionsOf(name).length >= 1 && sessionsOf(name).every(s => s.employeeId === ''), dev + ': the session is in Station_Sessions without an id');
    await page.click('#signOutBtn', { force: true });
    const end = await until(async () => events().find(e => e.event === 'end' && e.id === start.id), dev + ': ' + name + ' signs out');
    const after = await state();
    ok(end.reason === 'signOut' && !after.name && !after.idSet && !anyPin(after.all), dev + ': Sign Out ends the session and clears the login (no PIN left in storage)');
    await until(async () => sessionsOf(name).some(s => s.endReason === 'signOut'), dev + ': the end is stored');
  }

  // 2 · a wrong or short PIN is refused clearly, never echoed
  const before = events().length;
  await login(unknownPin);
  await until(async () => /not on the list/i.test((await state()).toasts), dev + ': a wrong PIN says so');
  let st = await state();
  ok(!anyPin(st.toasts) && /Check the 6 digits/.test(st.toasts), dev + ': the refusal echoes the digits or is unclear');
  ok(!st.name && !st.idSet && st.boxOpen, dev + ': a wrong PIN signs nobody in and the box stays');
  await login('123');
  await until(async () => /6-digit/.test((await state()).toasts), dev + ': a short PIN is refused');
  await page.clock.runFor(1500);
  ok(events().length === before, dev + ': a refused PIN starts no session');

  // 3 · midnight ends it; the next day the same person signs in again
  const name = 'Michael_V', n0 = events().length;
  await login(pins.get(name));
  const s1 = await until(async () => events().slice(n0).find(e => e.event === 'start' && e.person === name), dev + ': midnight session starts');
  await page.clock.runFor(30 * 60000);                                           // past midnight
  const end = await until(async () => events().find(e => e.event === 'end' && e.id === s1.id), dev + ': the midnight end');
  st = await state();
  ok(end.reason === 'midnight' && end.at === MIDNIGHT, dev + ': the session ends at midnight');
  ok(!st.name && !st.idSet && st.boxOpen && !anyPin(st.all), dev + ': at midnight the login clears and the PIN box comes back');
  await until(async () => sessionsOf(name).some(s => s.endReason === 'midnight'), dev + ': the midnight end is stored');
  await login(pins.get(name));
  const s2 = await until(async () => events().find(e => e.event === 'start' && e.person === name && e.id !== s1.id && e.at > MIDNIGHT), dev + ': next day, a new session');
  st = await state();
  ok(st.name === name && s2.station === station, dev + ': the next day the same person signs in again');
  ok(await page.evaluate(() => localStorage.getItem('station_signin_day')) === '2026-09-29', dev + ': the new day is recorded');
  ok(errors.length === 0, dev + ': page errors: ' + errors.join('; ').replace(/\d{6}/g, '#'));
  all.push(...seen);
  await ctx.close();
}

(async () => {
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  const seen = [], reqs = [];
  try {
    let i = 0;
    for (const [dev, station] of PAGES) { await run(browser, dev, station, seen, reqs, '203.0.113.' + (10 + i++)); console.log(dev + ': ' + NAMES.join(', ') + ' sign in and out, a wrong PIN is refused, midnight and the next day work'); }
    ok(mapGets === 0, 'a page asked for the whole roster document');
    ok(!reqs.some(q => anyPin(q.url)), 'a PIN was sent in a URL');
    ok(!reqs.some(q => anyPin(q.text) && !(q.m === 'POST' && /^\{"pinLogin":"\d{6}"\}$/.test(q.text))), 'a PIN was sent in a request that is not the login door\'s');
    ok(reqs.filter(q => /pinLogin/.test(q.text)).length >= PAGES.length * (NAMES.length + 2), 'the pages did not use the login door');
    ok(!anyPin(JSON.stringify(stored())), 'a PIN is in the sessions store');
    ok(stored().every(s => NAMES.includes(s.person)), 'a session has an unexpected name');
    console.log('login-readiness: all passed (' + stored().length + ' sessions; no PIN in any URL or the store, only the login door\'s requests carry one, no page asked for the roster)');
  } finally { await browser.close(); }
})().catch(e => { console.error('FAILED:', String(e && e.message || e).replace(/\d{6}/g, '#')); process.exit(1); });
