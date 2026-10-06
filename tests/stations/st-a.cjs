// Station tracking, part A (Welding): weld-1.html records `welded` with the person signed in on its own PIN login, in
// headless Chromium with Firebase, Materialize and every function stubbed here: no request leaves the machine.
//   · a stud order typed in: one `welded` for the order, by the signed-in person, stable id device-order-type-minute
//   · the phone scanner (weld-scan-1 relay) with an order mixing studs and a necklace: one `welded` per stud line
//   · signed out: `by: ""` and data.signedIn = false; never the earlier person, never the typed name field
//   NODE_PATH=$(npm root -g) PW_DIR=$(npm root -g)/playwright/node_modules CHROMIUM=… node tests/stations/st-a.cjs
'use strict';
const fs = require('fs'), path = require('path'), assert = require('assert');
const root = path.join(__dirname, '../..');
const pwDir = process.argv[2] || process.env.PW_DIR || path.join(root, 'node_modules');
const { chromium } = require(path.join(pwDir, 'playwright-core'));

const ORIGIN = 'http://weld.test';
const STUD = '3521100001', MIXED = '3521100002', NOBODY = '3521100003';
const TX = {
  [STUD]: [{ transaction_id: 91001, title: 'Custom Photo Stud Earrings', quantity: 1, variations: [] }],
  [MIXED]: [
    { transaction_id: 92001, title: 'Pet Portrait Earrings', quantity: 1, variations: [{ formatted_name: 'Style', formatted_value: 'Studs' }] },
    { transaction_id: 92002, title: 'Pet Portrait Necklace', quantity: 1, variations: [] },
    { transaction_id: 92003, title: 'Tiny Initial Stud Earrings', quantity: 1, variations: [] }
  ],
  [NOBODY]: [{ transaction_id: 93001, title: 'Stud earrings', quantity: 1, variations: [] }]
};
const st = { events: [] };

const fbStub = `window.firebase = (() => {
  window.__snaps = [];
  const snap = (exists, data) => ({ exists, data: () => data || {}, get: f => (data || {})[f] });
  const doc = (c, id) => ({ id, onSnapshot(cb) { window.__snaps.push({ c, id, cb }); try { cb(snap(false)); } catch (_) {} return () => {}; },
    set: async () => {}, update: async () => {}, get: async () => snap(false), collection: n => col(c + '/' + id + '/' + n) });
  const col = c => { const q = { doc: id => doc(c, id), where: () => q, orderBy: () => q, limit: () => q, add: async () => ({ id: 'x' }),
    onSnapshot(cb) { try { cb({ docs: [], empty: true, size: 0, forEach() {}, docChanges: () => [] }); } catch (_) {} return () => {}; },
    get: async () => ({ docs: [], empty: true, size: 0, forEach() {} }) }; return q; };
  const firestore = () => ({ collection: col });
  firestore.FieldValue = { delete: () => null, serverTimestamp: () => null, arrayUnion: () => null };
  return { initializeApp() {}, firestore, auth: () => ({ signInAnonymously: async () => ({}), onAuthStateChanged() {} }) };
})();`;
const mStub = `window.M = { AutoInit() {}, toast() {}, updateTextFields() {},
  Modal: { init() { return { open() {}, close() {} }; }, getInstance() { return { open() {}, close() {} }; } },
  FormSelect: { init() { return {}; }, getInstance() { return { getSelectedValues: () => [] }; } } };`;
const json = (r, body, status = 200) => r.fulfill({ status, contentType: 'application/json', body: JSON.stringify(body) });
const wait = ms => new Promise(r => setTimeout(r, ms));
async function until(fn, what, ms = 8000) {
  const t0 = Date.now();
  for (;;) { const v = await fn(); if (v) return v; if (Date.now() - t0 > ms) throw new Error('timed out waiting for ' + what); await wait(60); }
}
const all = (type, id) => st.events.filter(e => e.type === type && e.orderId === id);

(async () => {
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  const ctx = await browser.newContext({ viewport: { width: 1400, height: 950 } });
  await ctx.route(/.*/, async r => {
    const u = new URL(r.request().url()), m = r.request().method();
    if (/code\.jquery\.com/.test(u.host)) return r.fulfill({ status: 200, contentType: 'text/javascript', body: fs.existsSync('/usr/share/javascript/jquery/jquery.min.js') ? fs.readFileSync('/usr/share/javascript/jquery/jquery.min.js') : 'window.$=window.jQuery=()=>({on(){},ready(){}});' });
    if (/materialize/.test(u.pathname)) return r.fulfill({ status: 200, contentType: /\.css$/.test(u.pathname) ? 'text/css' : 'text/javascript', body: /\.css$/.test(u.pathname) ? '' : mStub });
    if (/gstatic\.com/.test(u.host)) return r.fulfill({ status: 200, contentType: 'text/javascript', body: /firebase-app-compat/.test(u.pathname) ? fbStub : '' });
    if (u.origin !== ORIGIN) return r.abort();                    // nothing leaves the machine
    if (u.pathname.startsWith('/.netlify/functions/')) {
      const fn = u.pathname.split('/').pop();
      if (fn === 'firebaseOrders') {
        if (u.searchParams.get('cancelCheck')) return json(r, { cancelled: {}, now: Date.now() });
        if (m === 'POST') {
          const b = JSON.parse(r.request().postData() || '{}');
          if (Array.isArray(b.timeline)) { st.events.push(...b.timeline); return json(r, { ok: true, ids: b.timeline.map(e => e.id) }); }
          return json(r, { success: true });
        }
        return json(r, { success: true, data: {} });
      }
      if (fn === 'etsyOrderProxy') { const id = u.searchParams.get('orderId'); return json(r, { receipt_id: Number(id), status: 'Paid', transactions: TX[id] || [] }); }
      return json(r, {});
    }
    const file = path.join(root, decodeURIComponent(u.pathname));
    if (file.startsWith(root) && fs.existsSync(file) && fs.statSync(file).isFile()) return r.fulfill({ status: 200, path: file });
    return r.fulfill({ status: 404, body: 'not here' });
  });
  await ctx.addInitScript(() => {
    if (sessionStorage.getItem('seeded')) return;
    sessionStorage.setItem('seeded', '1');
    localStorage.setItem('access_token', 'tok'); localStorage.setItem('refresh_token', 'ref');
    localStorage.setItem('token_expires_at', String(Math.floor(Date.now() / 1000) + 7200));
    localStorage.setItem('employee_id', '123456'); localStorage.setItem('employee_name', 'Marco R.');
  });
  const page = await ctx.newPage(), ours = [];
  page.on('pageerror', e => { if (/weld|station-timeline|StationTimeline|OrderTimeline|order-timeline/.test(String(e.stack || e.message))) ours.push(e.message); });

  await page.goto(ORIGIN + '/weld-1.html');
  await page.waitForFunction(() => window.StationTimeline && window.OrderTimeline && window.__snaps && window.__snaps.some(s => s.id === 'weld-scan-1'), null, { timeout: 15000 });
  const enter = async id => { await page.fill('#etsyOrderNumber', id); await page.focus('#etsyOrderNumber'); await page.keyboard.press('Enter'); };

  // 1 · a stud order typed in: one welded for the order, by the PIN login's name, stable id
  await enter(STUD);
  await until(() => all('scan', STUD).length && all('welded', STUD).length, 'the stud order\'s scan and weld');
  await wait(1200);
  const w1 = all('welded', STUD);
  assert.strictEqual(w1.length, 1, 'one welded for a stud order');
  assert.strictEqual(w1[0].by, 'Marco R.', 'by the person signed in with the PIN login');
  assert.strictEqual(w1[0].station, 'welding'); assert.strictEqual(w1[0].device, 'weld-1');
  assert.strictEqual(w1[0].id, `weld-1-${STUD}-welded-${Math.floor(w1[0].at / 60000)}`, 'stable id device-order-type-minute');
  assert.strictEqual(w1[0].transactionId, undefined, 'the whole order');
  assert.deepStrictEqual(w1[0].data, { lines: 1, studs: 1 });
  assert(!JSON.stringify(st.events).includes('123456'), 'the PIN is never recorded');

  // 2 · the phone scanner relays an order of two stud lines and a necklace: one welded per stud line, none for the necklace
  await page.evaluate(id => { const s = window.__snaps.find(x => x.id === 'weld-scan-1'); s.cb({ exists: true, data: () => ({ 'Order Number': id }) }); }, MIXED);
  await until(() => all('welded', MIXED).length >= 2, 'the stud lines\' welds');
  await wait(1200);
  const w2 = all('welded', MIXED);
  assert.deepStrictEqual(w2.map(e => e.transactionId).sort(), ['92001', '92003'], 'only the stud lines: ' + JSON.stringify(w2));
  assert(w2.every(e => e.by === 'Marco R.' && e.station === 'welding' && e.data.lines === 3 && e.data.studs === 2), 'who and where on each');
  assert.strictEqual(new Set(w2.map(e => e.id)).size, 2, 'a distinct stable id per line');
  assert.strictEqual(all('scan', MIXED)[0].data.how, 'scan', 'the phone relay is a scan');

  // 3 · signed out, a name typed in the Employee Name field: the weld says not signed in, never Marco nor the typed name
  await page.click('#signOutBtn');
  await page.evaluate(() => { document.getElementById('employeeName').value = 'Typed Guess'; });   // (the name field is hidden on the Welding page now: the roster chips replace it)
  await enter(NOBODY);
  await until(() => all('welded', NOBODY).length, 'the weld with nobody signed in');
  const w3 = all('welded', NOBODY)[0], s3 = all('scan', NOBODY)[0];
  assert.strictEqual(w3.by, '', 'nobody signed in: by is empty: ' + JSON.stringify(w3));
  assert.strictEqual(w3.data.signedIn, false, 'and says so');
  assert.strictEqual(s3.by, '', 'the scan is not the earlier person either');

  assert.deepStrictEqual(ours, [], 'no page errors from the weld wiring');
  await browser.close();
  console.log('st-a: welding records welded with who — all passed');
})().catch(e => { console.error(e); process.exit(1); });
