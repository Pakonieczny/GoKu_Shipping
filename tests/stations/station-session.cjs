// Sign-in sessions and the midnight sign-out (station-session.js + firebaseOrders {session}). Fakes only: Firestore is a
// Map, every request that is not the page itself is stubbed or aborted, and the clock is faked to cross New York midnight.
//   1 · the door: the shape, the server's own times, a PIN never kept, bad events refused, a beat after the day turned or
//       after 15 quiet minutes ends the session, another computer cannot touch it, a late end stays within bounds
//   2 · weld-1.html: PIN login → a session (name only, never the PIN), beats, midnight → the session ends "midnight",
//       the PIN box comes back and the order typed on screen stays; sign in again → a new session; Sign Out ends it;
//       a page loaded after the day turned signs out before anything else; the computer line sits in the PIN box
//   NODE_PATH=$(npm root -g) PW_DIR=$(npm root -g)/playwright/node_modules CHROMIUM=… node tests/stations/station-session.cjs
'use strict';
const fs = require('fs'), path = require('path'), assert = require('assert'), Module = require('module');
const root = path.join(__dirname, '../..');
const MIDNIGHT = Date.parse('2026-09-29T04:00:00Z');          // 00:00 on 29 Sep in New York (EDT)

/* two made-up numbers for this run: never written down, never printed */
const fakePin = used => { for (;;) { const p = String(100000 + Math.floor(Math.random() * 900000)); if (!/^(\d)\1{5}$/.test(p) && p !== used) return p; } };
const P1 = fakePin(), P2 = fakePin(P1);
const ROSTER = { [P1]: 'Tess Welder', [P2]: 'Ray Welder' };

/* ── 1 · the door ── */
async function door() {
  const docs = new Map();
  const ref = p => ({ path: p, id: p.split('/').pop(), get: async () => ({ exists: docs.has(p), data: () => docs.get(p) }) });
  const fakeDb = {
    collection: c => ({ doc: id => ref(c + '/' + id) }),
    runTransaction: async fn => { const w = []; const r = await fn({ get: x => x.get(), set: (x, d, o) => w.push([x.path, d, o]) }); for (const [p, d, o] of w) docs.set(p, o && o.merge ? Object.assign({}, docs.get(p) || {}, d) : d); return r; }
  };
  const fakeAdmin = { firestore: Object.assign(() => fakeDb, { FieldValue: { serverTimestamp: () => 'ts', delete: () => null } }) };
  const realLoad = Module._load;
  Module._load = function (req, ...rest) { if (/[\/]firebaseAdmin(\.js)?$/.test(req)) return fakeAdmin; return realLoad.call(this, req, ...rest); };
  const fn = require(path.join(root, 'netlify/functions/firebaseOrders.js'));
  Module._load = realLoad;
  const realNow = Date.now; let now = MIDNIGHT - 20 * 60000; Date.now = () => now;
  try {
    const post = (session, ip = '203.0.113.' + Math.floor(Math.random() * 200), sandbox) => fn.handler({ httpMethod: 'POST', headers: { 'x-nf-client-connection-ip': ip }, body: JSON.stringify({ session }), queryStringParameters: sandbox ? { sandbox: '1' } : {} })
      .then(r => ({ status: r.statusCode, body: JSON.parse(r.body || '{}') }));
    const S = (o = {}) => Object.assign({ id: 'weld-1-ABCD-k1', event: 'start', person: 'Tess Welder', employeeId: P1, station: 'welding', device: 'weld-1', computerId: 'pc-ABCDEFGHJKMN', computerLabel: 'Welding weld-1 · ABCD', at: now }, o);
    const doc = id => docs.get('Station_Sessions/' + id);

    let r = await post(S());
    assert.strictEqual(r.status, 200, JSON.stringify(r.body));
    assert.deepStrictEqual(Object.keys(doc('weld-1-ABCD-k1')).sort(), ['admin', 'computerId', 'computerLabel', 'device', 'employeeId', 'endAt', 'endReason', 'id', 'lastSeenAt', 'minutes', 'person', 'startAt', 'station'].sort(), 'the Station_Sessions shape (admin: whether the person is on the Admin list, kept for the auto sign-out)');
    assert.strictEqual(doc('weld-1-ABCD-k1').employeeId, '', 'a PIN (digits only) is never kept');
    assert(!JSON.stringify([...docs.values()]).includes(P1), 'the PIN is nowhere in the store');
    assert.strictEqual(doc('weld-1-ABCD-k1').startAt, now);
    now += 5 * 60000; r = await post(S({ event: 'beat', at: now + 9e6 }));
    assert.strictEqual(doc('weld-1-ABCD-k1').lastSeenAt, now, 'the server stamps its own time, not the client\'s');
    assert.strictEqual(doc('weld-1-ABCD-k1').minutes, 5);
    r = await post(S({ event: 'beat', computerId: 'pc-OTHERCOMPUTR' }));
    assert.strictEqual(r.status, 409, 'another computer cannot touch the session');
    for (const bad of [{ station: 'kitchen' }, { id: 'x' }, { event: 'poke' }, { person: '' }, { computerId: '../x' }]) {
      r = await post(S(Object.assign({ id: 'bad-session-1' }, bad)));
      assert.strictEqual(r.status, 400, 'refused: ' + JSON.stringify(bad));
    }
    r = await fn.handler({ httpMethod: 'POST', headers: {}, body: JSON.stringify({ session: S({ id: 'big-session-1', computerLabel: 'x'.repeat(5000) }) }), queryStringParameters: {} });
    assert.strictEqual(r.statusCode, 413, 'an oversized session event is refused');
    // the day turns with no end from the page: the next beat ends it at midnight
    now = MIDNIGHT - 2 * 60000; await post(S({ event: 'beat' }));
    now = MIDNIGHT + 3 * 60000; r = await post(S({ event: 'beat' }));
    assert.strictEqual(doc('weld-1-ABCD-k1').endAt, MIDNIGHT); assert.strictEqual(doc('weld-1-ABCD-k1').endReason, 'midnight');
    assert.strictEqual(doc('weld-1-ABCD-k1').minutes, 20);
    r = await post(S({ event: 'beat' })); assert.strictEqual(r.body.ended, true, 'an ended session stays ended');
    // 15 quiet minutes: a beat does not bring it back, it closed at its last beat (a station with the default rule; Welding and Laser keep a quiet page open: auto-signout-server.cjs)
    await post(S({ id: 'weld-1-ABCD-k2', station: 'assembly', device: 'assembly-1' }));
    const t2 = now; now += 20 * 60000; r = await post(S({ id: 'weld-1-ABCD-k2', station: 'assembly', device: 'assembly-1', event: 'beat' }));
    assert.strictEqual(doc('weld-1-ABCD-k2').endReason, 'closed'); assert.strictEqual(doc('weld-1-ABCD-k2').endAt, t2);
    // ... but not a Welding page (it is signed out at 17:00, never by quiet): the same 20 minutes is the same session carrying on
    await post(S({ id: 'weld-1-ABCD-k2w' }));
    now += 20 * 60000; r = await post(S({ id: 'weld-1-ABCD-k2w', event: 'beat' }));
    assert.strictEqual(r.body.ended, false); assert.strictEqual(doc('weld-1-ABCD-k2w').endAt, null); assert.strictEqual(doc('weld-1-ABCD-k2w').lastSeenAt, now);
    // a late end (sent from an outbox) keeps its own moment, never before the last beat, never after now
    await post(S({ id: 'weld-1-ABCD-k3' })); const t3 = now; now += 4 * 60000; await post(S({ id: 'weld-1-ABCD-k3', event: 'beat' }));
    now += 60000; r = await post(S({ id: 'weld-1-ABCD-k3', event: 'end', reason: 'signOut', at: t3 }));
    assert.strictEqual(doc('weld-1-ABCD-k3').endAt, t3 + 4 * 60000, 'not before the last beat'); assert.strictEqual(doc('weld-1-ABCD-k3').endReason, 'signOut');
    await post(S({ id: 'weld-1-ABCD-k4' })); now += 60000; r = await post(S({ id: 'weld-1-ABCD-k4', event: 'end', reason: 'switched', at: now + 3600e3 }));
    assert.strictEqual(doc('weld-1-ABCD-k4').endAt, now, 'not after now'); assert.strictEqual(doc('weld-1-ABCD-k4').endReason, 'switched');
    await post(S({ id: 'weld-1-ABCD-k5' }), undefined, true);
    assert(docs.has('Sandbox_Station_Sessions/weld-1-ABCD-k5') && !doc('weld-1-ABCD-k5'), 'a sandbox session stays in the sandbox');
  } finally { Date.now = realNow; }
  console.log('door: shape, server times, no PIN, refusals, midnight and closed ends, one computer, late ends bounded, sandbox');
}

/* ── 2 · the Welding station ── */
async function station() {
  const pwDir = process.argv[2] || process.env.PW_DIR || path.join(root, 'node_modules');
  const { chromium } = require(path.join(pwDir, 'playwright-core'));
  const ORIGIN = 'http://weld.test';
  const sessions = [], bodies = [], doorBodies = []; let mapGets = 0;
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
  const mStub = `window.__opens = 0; window.__toasts = [];
    window.M = { AutoInit() {}, toast(o) { __toasts.push(o && o.html); }, updateTextFields() {},
      Modal: { init(el) { const i = { open() { __opens++; i.isOpen = true; }, close() { i.isOpen = false; }, isOpen: false }; if (el) el.__m = i; return i; },
               getInstance(el) { return (el && el.__m) || { open() {}, close() {} }; } },
      FormSelect: { init() { return {}; }, getInstance() { return { getSelectedValues: () => [] }; } } };`;
  const json = (r, body, status = 200) => r.fulfill({ status, contentType: 'application/json', body: JSON.stringify(body) });
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  try {
    const ctx = await browser.newContext({ viewport: { width: 1400, height: 950 } });
    await ctx.route(/.*/, async r => {
      const u = new URL(r.request().url()), m = r.request().method();
      if (/code\.jquery\.com/.test(u.host)) return r.fulfill({ status: 200, contentType: 'text/javascript', body: 'window.$=window.jQuery=()=>({on(){},ready(){}});' });
      if (/materialize/.test(u.pathname)) return r.fulfill({ status: 200, contentType: /\.css$/.test(u.pathname) ? 'text/css' : 'text/javascript', body: /\.css$/.test(u.pathname) ? '' : mStub });
      if (/gstatic\.com/.test(u.host)) return r.fulfill({ status: 200, contentType: 'text/javascript', body: /firebase-app-compat/.test(u.pathname) ? fbStub : '' });
      if (u.origin !== ORIGIN) return r.abort();                                   // nothing leaves the machine
      if (u.pathname.startsWith('/.netlify/functions/')) {
        if (m === 'POST') {
          const text = r.request().postData() || '{}'; bodies.push(text);
          const b = JSON.parse(text);
          if (b.pinLogin !== undefined) {                                         // the server's login door: the only request that carries a number
            bodies.pop(); doorBodies.push(text);
            const name = typeof b.pinLogin === 'string' && Object.prototype.hasOwnProperty.call(ROSTER, b.pinLogin) ? ROSTER[b.pinLogin] : '';
            return json(r, name ? { ok: true, name } : { ok: false, error: 'not on the list' });
          }
          if (b.session) { sessions.push(b.session); return json(r, { success: true }); }
          return json(r, { success: true });
        }
        if (/employee/i.test(u.searchParams.get('orderId') || '')) { mapGets++; return json(r, { success: false }, 401); }   // the roster is never asked for
        if (u.searchParams.get('cancelCheck')) return json(r, { cancelled: {}, now: Date.now() });
        return json(r, { success: true, data: {} });
      }
      const file = path.join(root, decodeURIComponent(u.pathname));
      if (file.startsWith(root) && fs.existsSync(file) && fs.statSync(file).isFile()) return r.fulfill({ status: 200, path: file });
      return r.fulfill({ status: 404, body: 'not here' });
    });
    const page = await ctx.newPage();
    const errors = []; page.on('pageerror', e => { if (!/gstatic\.com\/firebasejs/.test(String(e))) errors.push(String(e)); });   // (the stubbed Firebase modules are not the page's)
    await page.clock.install({ time: new Date(MIDNIGHT - 20 * 60000) });   // 23:40 in New York
    await page.goto(ORIGIN + '/weld-1.html');
    await page.waitForFunction(() => window.StationSession && document.querySelector('#userLoginModal .station-session-pc'), null, { timeout: 15000 });
    const pcLine = await page.textContent('#userLoginModal .station-session-pc');
    assert(/^Computer: Welding weld-1 · [2-9A-Z]{4}Name it$/.test(pcLine), 'the computer line in the PIN box: ' + pcLine);
    const login = async pin => {
      await page.evaluate(() => { const i = document.getElementById('employeeNumberInput'); i.dataset.raw = ''; i.value = ''; });
      await page.focus('#employeeNumberInput'); await page.keyboard.type(pin);
      await page.click('#employeeLoginBtn', { force: true });
      await until(() => page.evaluate(() => document.getElementById('userLoginModal').classList.contains('weld-task')), 'the Welding or Matching step');    // (after the number: one extra step)
      await page.evaluate(() => document.getElementById('weldTaskMatching').click());
    };
    const until = async (fn, what) => { for (let i = 0; i < 100; i++) { const v = await fn(); if (v) return v; await page.clock.runFor(100); } throw new Error('timed out: ' + what); };

    await login(P1);
    const start = await until(() => sessions.find(s => s.event === 'start'), 'a session start');
    assert.strictEqual(start.person, 'Tess Welder'); assert.strictEqual(start.employeeId, ''); assert.strictEqual(start.station, 'welding'); assert.strictEqual(start.device, 'weld-1');
    assert(/^pc-[2-9A-Z]{12}$/.test(start.computerId), start.computerId);
    assert.strictEqual(await page.evaluate(() => localStorage.getItem('station_signin_day')), '2026-09-28');
    await page.fill('#etsyOrderNumber', '3521000777');                                     // work on screen

    const active = async ms => { for (let left = ms, n = 0; left > 0; n++) { await page.mouse.move(120 + (n % 40), 220 + (n % 7)); const step = Math.min(left, 4 * 60000); await page.clock.runFor(step); left -= step; } };   // a hand at the screen: input every 4 minutes
    await active(6 * 60000);
    assert(sessions.some(s => s.event === 'beat' && s.id === start.id), 'a beat every 5 minutes');
    await active(15 * 60000);                                                               // past midnight
    const end = await until(() => sessions.find(s => s.event === 'end' && s.id === start.id), 'the midnight end');
    assert.strictEqual(end.reason, 'midnight'); assert.strictEqual(end.at, MIDNIGHT);
    const after = await page.evaluate(() => ({ id: localStorage.getItem('employee_id'), name: localStorage.getItem('employee_name'), people: localStorage.getItem('weld_people'), in: window.isEmployeeLoggedIn,
      opens: window.__opens, order: document.getElementById('etsyOrderNumber').value, toast: (window.__toasts || []).join(' | ') }));
    assert.strictEqual(after.id, null); assert.strictEqual(after.name, null); assert.strictEqual(after.in, false);
    assert.strictEqual(after.people, '[]', 'nobody is signed in at the Welding station any more');
    assert(after.opens >= 1, 'the PIN box is back'); assert.strictEqual(after.order, '3521000777', 'the order typed on screen stays');
    assert(/midnight/i.test(after.toast), after.toast);

    await login(P2);
    const start2 = await until(() => sessions.find(s => s.event === 'start' && s.id !== start.id), 'a new session');
    assert.strictEqual(start2.person, 'Ray Welder');
    assert.strictEqual(await page.evaluate(() => localStorage.getItem('station_signin_day')), '2026-09-29');
    await page.click('#signOutBtn', { force: true });
    const end2 = await until(() => sessions.find(s => s.event === 'end' && s.id === start2.id), 'the sign-out end');
    assert.strictEqual(end2.reason, 'signOut');

    // a page loaded after the day turned: signed out before anything else, no session for yesterday's person
    const pc = start.computerId, n0 = sessions.length;
    await page.evaluate(() => { localStorage.setItem('weld_people', JSON.stringify([{ name: 'Tess Welder', task: 'welding', at: 1 }])); localStorage.setItem('station_signin_day', '2026-09-28'); });
    await page.reload();
    await page.waitForFunction(() => window.StationSession && window.__opens >= 1, null, { timeout: 15000 });
    const reloaded = await page.evaluate(() => ({ id: localStorage.getItem('weld_people'), in: window.isEmployeeLoggedIn, pc: localStorage.getItem('station_computer_id') }));
    assert.strictEqual(reloaded.id, '[]', 'yesterday\'s login is cleared on load'); assert.notStrictEqual(reloaded.in, true);
    assert.strictEqual(reloaded.pc, pc, 'the computer id stays');
    await page.clock.runFor(2000);
    assert(!sessions.slice(n0).some(s => s.event === 'start'), 'no session starts for yesterday\'s login');
    assert(!bodies.some(b => b.includes(P1) || b.includes(P2)), 'no PIN is sent except to the login door');
    assert(doorBodies.length === 2 && doorBodies.every(b => /^\{"pinLogin":"\d{6}"\}$/.test(b)) && mapGets === 0, 'the two sign-ins went to the login door only, and the roster was never read');
    assert.deepStrictEqual(errors, [], 'no page errors: ' + errors.join('; '));
    console.log('weld-1: PIN login → session (name only), beats, midnight ends it and brings the PIN box back with the work kept, sign-in again, Sign Out, stale day on load');
  } finally { await browser.close(); }
}

(async () => {
  await door();
  await station();
  console.log('station-session: all passed');
})().catch(e => { console.error(e); process.exit(1); });
