// Employee efficiency, Welding: weld-1.html feeds StationActivity (station-activity.js) with what the welder does, in
// headless Chromium with Firebase, Materialize and every function stubbed here: no request leaves the machine.
//   · nobody signed in: a scan records no activity at all
//   · PIN login (a fake PIN), then a typed stud order: `scan` (stud pieces) and `complete` (orders 1, parts) once each
//   · the phone scanner relay (weld-scan-1) with a mixed order: `scan` "phone scan" and ONE `complete` for the order
//   · the same order again: a `scan` ("again"), no second `complete`
//   · rejects: an unknown order, an order with no studs, a cancelled order: `scan` + `reject`, never `complete`
//   · Etsy down: `scan`, `error`, and the order still counted (the page still seals it welded)
//   · text with no digits is one error (no scan, no completed order); an order welded before a page reload is not counted again
//   · the order chat: a message and a picture are notes, a failure of either an error
//   · every event says who (the name), welding, weld-1, this computer and session; signed out again: nothing more
//   · no request carries the PIN but the login door's { pinLogin }
//   NODE_PATH=$(npm root -g) PW_DIR=$(npm root -g)/playwright/node_modules CHROMIUM=… node tests/stations/ea-weld.cjs
'use strict';
const fs = require('fs'), path = require('path'), assert = require('assert');
const root = path.join(__dirname, '../..');
const pwDir = process.argv[2] || process.env.PW_DIR || path.join(root, 'node_modules');
const { chromium } = require(path.join(pwDir, 'playwright-core'));

const ORIGIN = 'http://weld.test', WHO = 'Marco R.';
const PIN = String(100000 + Math.floor(Math.random() * 900000)).replace(/^(\d)\1{5}$/, '135792');   // made up per run: only the login door's request may carry it
const NOBODY = '3521200001', STUD = '3521200002', MIXED = '3521200003', UNKNOWN = '3521200004', NOSTUD = '3521200005', CANC = '3521200006', DOWN = '3521200007';
const TX = {
  [NOBODY]: [{ transaction_id: 94001, title: 'Stud earrings', quantity: 1, variations: [] }],
  [STUD]: [{ transaction_id: 94101, title: 'Custom Photo Stud Earrings', quantity: 2, variations: [] }],
  [MIXED]: [
    { transaction_id: 94201, title: 'Pet Portrait Earrings', quantity: 1, variations: [{ formatted_name: 'Style', formatted_value: 'Studs' }] },
    { transaction_id: 94202, title: 'Pet Portrait Necklace', quantity: 1, variations: [] },
    { transaction_id: 94203, title: 'Tiny Initial Stud Earrings', quantity: 2, variations: [] }
  ],
  [NOSTUD]: [{ transaction_id: 94501, title: 'Pet Portrait Necklace', quantity: 1, variations: [] }],
  [CANC]: [{ transaction_id: 94601, title: 'Stud earrings', quantity: 1, variations: [] }]
};
const st = { acts: [], sessions: [], events: [], reqs: [] };

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
const acts = (order, action) => st.acts.filter(e => e.orderId === order && (!action || e.action === action));

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
        if (u.searchParams.get('cancelCheck')) {
          const out = {}; for (const id of u.searchParams.get('cancelCheck').split(',')) if (id === CANC) out[id] = { at: Date.now() - 60000, by: 'Office', why: 'Buyer cancelled', source: 'sheet' };
          return json(r, { cancelled: out, now: Date.now() });
        }
        if (m === 'POST') {
          const b = JSON.parse(r.request().postData() || '{}');
          if (b.pinLogin !== undefined) return json(r, b.pinLogin === PIN ? { ok: true, name: WHO } : { ok: false, error: 'not on the list' });   // the server's login door
          if (Array.isArray(b.activity)) { st.acts.push(...b.activity); return json(r, { success: true, written: b.activity.length, duplicate: 0, refused: 0, scrubbed: 0 }); }
          if (b.session) { st.sessions.push(b.session); return json(r, { success: true }); }
          if (b.newMessage && st.msgFail) return json(r, { success: false, error: 'down' }, 500);
          if (Array.isArray(b.timeline)) { st.events.push(...b.timeline); return json(r, { ok: true, ids: b.timeline.map(e => e.id) }); }
          return json(r, { success: true });
        }
        if (/employee/i.test(u.searchParams.get('orderId') || '')) return json(r, { success: false, error: 'closed' }, 401);          // the roster is never read
        return json(r, { success: true, data: {} });
      }
      if (fn === 'etsyOrderProxy') {
        const id = u.searchParams.get('orderId');
        if (id === UNKNOWN) return json(r, { error: 'Resource not found.' }, 404);
        if (id === DOWN) return json(r, { error: 'upstream timeout' }, 502);
        return json(r, { receipt_id: Number(id), status: id === CANC ? 'Canceled' : 'Paid', transactions: TX[id] || [] });
      }
      return json(r, {});
    }
    // the helper under test: the repo's station-activity.js (ACTIVITY_JS only stands in for it while it is not merged yet)
    const file = u.pathname === '/station-activity.js' && process.env.ACTIVITY_JS ? process.env.ACTIVITY_JS : path.join(root, decodeURIComponent(u.pathname));
    if ((file.startsWith(root) || file === process.env.ACTIVITY_JS) && fs.existsSync(file) && fs.statSync(file).isFile()) return r.fulfill({ status: 200, path: file });
    return r.fulfill({ status: 404, body: 'not here' });
  });
  await ctx.addInitScript(() => {
    if (sessionStorage.getItem('seeded')) return;
    sessionStorage.setItem('seeded', '1');
    localStorage.setItem('access_token', 'tok'); localStorage.setItem('refresh_token', 'ref');
    localStorage.setItem('token_expires_at', String(Math.floor(Date.now() / 1000) + 7200));
  });
  const page = await ctx.newPage(), ours = [];
  page.on('request', q => { st.reqs.push(q.method() + ' ' + q.url() + ' ' + (q.postData() || '')); });
  page.on('pageerror', e => { if (/weld|station-timeline|station-activity|StationActivity|StationTimeline|OrderTimeline|order-timeline/.test(String(e.stack || e.message))) ours.push(e.message); });

  await page.goto(ORIGIN + '/weld-1.html');
  await page.waitForFunction(() => window.StationTimeline && window.StationSession && window.StationActivity && window.__snaps, null, { timeout: 15000 });
  const enter = async id => { await page.fill('#etsyOrderNumber', id); await page.focus('#etsyOrderNumber'); await page.keyboard.press('Enter'); };
  const phone = id => page.evaluate(o => { const s = window.__snaps.filter(x => x.id === 'weld-scan-1').pop(); s.cb({ exists: true, data: () => ({ 'Order Number': o }) }); }, id);
  const flush = async () => { await page.evaluate(() => window.StationActivity.flush && window.StationActivity.flush()); await wait(150); };
  const outcome = async (id, action) => { await until(async () => { await flush(); return acts(id, action).length; }, `${action} for ${id}`); await wait(500); await flush(); };

  // 1 · nobody signed in: the scan is read and sealed as before (not signed in), but no activity is recorded
  await enter(NOBODY);
  await until(() => st.events.some(e => e.type === 'scan' && e.orderId === NOBODY), 'the signed-out scan on the timeline');
  await wait(800); await flush();
  assert.strictEqual(st.acts.length, 0, 'nobody signed in: no activity events: ' + JSON.stringify(st.acts));

  // 2 · PIN login (the fake PIN is only ever typed on the page and sent to the login door), then a typed stud order of 2 pieces
  await page.focus('#employeeNumberInput'); await page.keyboard.type(PIN);
  await page.click('#employeeLoginBtn');
  const start = await until(() => st.sessions.find(s => s.event === 'start' && s.person === WHO), 'the sign-in session');
  assert.strictEqual(start.employeeId, '', 'the session carries the name, never the PIN');
  await enter(STUD);
  await outcome(STUD, 'complete');
  assert.deepStrictEqual(acts(STUD).map(e => e.action), ['scan', 'complete'], 'a typed stud order: one scan, one complete: ' + JSON.stringify(acts(STUD)));
  const [s1, c1] = acts(STUD);
  assert.strictEqual(s1.parts, 2, 'the scan carries the stud pieces'); assert(/typed/.test(s1.detail), 'how: ' + s1.detail);
  assert.strictEqual(c1.parts, 2, 'the weld produced 2 pieces'); assert.strictEqual(c1.orders, 1, 'and finished the order once');
  for (const e of st.acts) {
    assert.strictEqual(e.person, WHO, 'who'); assert.strictEqual(e.station, 'welding'); assert.strictEqual(e.device, 'weld-1');
    assert.strictEqual(e.session, start.id, 'this sign-in session'); assert.strictEqual(e.computer, start.computerId, 'this computer');
  }

  // 3 · the phone relay with studs (1 + 2 pieces) and a necklace: one scan, one complete for the order
  await phone(MIXED);
  await outcome(MIXED, 'complete');
  assert.deepStrictEqual(acts(MIXED).map(e => e.action), ['scan', 'complete'], 'one scan and ONE complete for a two-stud-line order: ' + JSON.stringify(acts(MIXED)));
  assert.strictEqual(acts(MIXED, 'scan')[0].detail, 'phone scan'); assert.strictEqual(acts(MIXED, 'scan')[0].parts, 3);
  assert.strictEqual(acts(MIXED, 'complete')[0].parts, 3); assert.strictEqual(acts(MIXED, 'complete')[0].orders, 1);

  // 4 · the same order again: a scan, no second complete
  await enter(STUD);
  await until(async () => { await flush(); return acts(STUD, 'scan').length === 2; }, 'the second scan'); await wait(600); await flush();
  assert.strictEqual(acts(STUD, 'complete').length, 1, 'an order counts once'); assert(/again/.test(acts(STUD, 'scan')[1].detail));

  // 5 · rejects: unknown order, no studs on the order, cancelled order
  await enter(UNKNOWN); await outcome(UNKNOWN, 'reject');
  assert.deepStrictEqual(acts(UNKNOWN).map(e => e.action), ['scan', 'reject'], 'unknown: scan + reject, never complete');
  assert.strictEqual(acts(UNKNOWN, 'reject')[0].detail, 'order not found');
  await enter(NOSTUD); await outcome(NOSTUD, 'reject');
  assert.deepStrictEqual(acts(NOSTUD).map(e => e.action), ['scan', 'reject'], 'no studs: scan + reject');
  await enter(CANC); await outcome(CANC, 'reject');
  assert.deepStrictEqual(acts(CANC).map(e => e.action), ['scan', 'reject'], 'cancelled: scan + reject');
  assert.strictEqual(acts(CANC, 'reject')[0].detail, 'cancelled order');
  await page.click('.sttl-ok');                                      // Understood

  // 6 · Etsy down: the scan, an error, and the order still counted (the weld is sealed as before)
  await enter(DOWN); await outcome(DOWN, 'complete');
  assert.deepStrictEqual(acts(DOWN).map(e => e.action), ['scan', 'error', 'complete'], 'Etsy down: scan, error, complete: ' + JSON.stringify(acts(DOWN)));
  assert.strictEqual(acts(DOWN, 'complete')[0].parts, 0, 'pieces unknown'); assert(/not checked/.test(acts(DOWN, 'complete')[0].detail));

  // 7 · no duplicates anywhere, and the order the page logged them in
  const keys = st.acts.map(e => e.action + '|' + e.orderId + '|' + e.detail);
  assert.strictEqual(new Set(st.acts.map(e => e.id)).size, st.acts.length, 'every event once');
  assert.strictEqual(st.acts.filter(e => e.action === 'complete').length, 3, 'three orders completed: ' + keys.join(' / '));
  assert.strictEqual(st.acts.filter(e => e.action === 'reject').length, 3);

  // 7b · text with no digits is not an order: one error, no scan, nothing completed (the timeline has no order to seal either)
  const completesBefore = st.acts.filter(e => e.action === 'complete').length;
  await page.fill('#etsyOrderNumber', ''); await enter('not an order');
  await until(async () => { await flush(); return st.acts.some(e => /not an order number/.test(e.detail)); }, 'the no-digits error');
  await wait(700); await flush();
  const junk = st.acts.filter(e => e.orderId === '' && e.action !== 'note');
  assert.deepStrictEqual(junk.map(e => e.action), ['error'], 'junk text: one error, no scan, no complete: ' + JSON.stringify(junk));
  assert.strictEqual(st.acts.filter(e => e.action === 'complete').length, completesBefore, 'junk text completes nothing');

  // 7c · a page reload does not forget a welded order: the same order scanned again is a scan, never a second complete
  const stScans = acts(STUD, 'scan').length;
  await page.reload();
  await page.waitForFunction(() => window.StationTimeline && window.StationSession && window.StationActivity && window.__snaps && window.StationActivity.who && window.StationActivity.who(), null, { timeout: 15000 });
  await enter(STUD);
  await until(async () => { await flush(); return acts(STUD, 'scan').length === stScans + 1; }, 'the scan after the reload'); await wait(700); await flush();
  assert.strictEqual(acts(STUD, 'complete').length, 1, 'an order welded before the reload is not counted again');
  assert(/again/.test(acts(STUD, 'scan').pop().detail), 'the scan says it is a repeat');

  // 7d · the order chat: a message and a picture are notes, a failure of either an error (fixed words, never the text)
  const chatFrom = st.acts.length;
  const say = async text => { await page.fill('#britesMsgInput', text); await page.evaluate(() => document.getElementById('goScreenTwoBtn').click()); await wait(500); };
  const drop = async ok => { await page.evaluate(async ok => {
    window.uploadViaResumable = async () => { if (!ok) throw new Error('storage down'); return 'http://weld.test/x.png'; };
    const dt = new DataTransfer(); dt.items.add(new File(['x'], 'a.png', { type: 'image/png' }));
    document.getElementById('customerMessageHistory').dispatchEvent(new DragEvent('drop', { dataTransfer: dt, bubbles: true, cancelable: true }));
  }, ok); await wait(500); };
  await say('secret words about the stud');
  st.msgFail = true; await say('words that fail'); st.msgFail = false;
  await drop(true); await drop(false);
  await flush();
  assert.deepStrictEqual(st.acts.slice(chatFrom).map(e => e.action + ':' + e.orderId + ':' + e.detail), [
    'note:' + STUD + ':order chat message sent', 'error:' + STUD + ':order chat message failed',
    'note:' + STUD + ':order chat image sent', 'error:' + STUD + ':order chat image failed'], 'chat: one event per message and picture');

  // 8 · signed out again: nothing more is recorded, and the session ends
  const before = st.acts.length;
  await page.click('#signOutBtn');
  await until(() => st.sessions.some(s => s.event === 'end' && s.reason === 'signOut'), 'the session end');
  await page.fill('#etsyOrderNumber', ''); await enter(STUD);
  await wait(1200); await flush();
  assert.strictEqual(st.acts.length, before, 'signed out: no new activity');

  // 9 · the PIN is in no request but the login door's { pinLogin } (never a URL), and the roster is never read
  const doorReq = q => q.endsWith(' ' + JSON.stringify({ pinLogin: PIN })) && /^POST \S+\/\.netlify\/functions\/firebaseOrders /.test(q);
  assert(!st.reqs.some(q => q.includes(PIN) && !doorReq(q)), 'the PIN was sent in a request that is not the login door\'s');
  assert(st.reqs.some(doorReq) && !st.reqs.some(q => /employee/i.test(q.split(' ')[1] || '')), 'the sign-in must use the login door and never read the roster');
  assert(!JSON.stringify(st.acts).includes(PIN) && !JSON.stringify(st.sessions).includes(PIN) && !JSON.stringify(st.events).includes(PIN), 'the PIN is never recorded');

  assert.deepStrictEqual(ours, [], 'no page errors from the activity wiring');
  await browser.close();
  console.log(`ea-weld: welding records scan / complete / reject / error with who, where and parts — all passed (${st.acts.length} events)`);
})().catch(e => { console.error(e); process.exit(1); });
