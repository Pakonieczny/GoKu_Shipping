// Assembly: the phone scanner tells the assembler when nobody is signed in at the desk, and a scan sent to a desk nobody was signed in at is
// not lost when a desk page opens and signs in within 10 minutes (Paul, 6 Oct 2026: "I'm not seeing the Assembly stations record any
// information or a user sign or any progress or scanning of orders"; live evidence: the Assembly 2 phone wrote a scan at 17:32 UTC and the
// board still showed 0 orders, because a phone scan only counts when the desk page is open AND somebody is signed in there).
// The REAL pages (assembly-N.html, assembly-scan-N.html), the REAL station libraries and the REAL server code (firebaseOrders.js: the PIN login
// door, {session}, {activity}, the scanner's relay write, the new read-only ?deskFor= door) over ONE in-memory Firestore in its own process
// (tests/charm-nest/efficiency-portal-backend.cjs). Fake: the Etsy order answers, Firebase's browser SDK (its first delivery can carry the
// relay document the way a real listener's first delivery does), Materialize, the phone's camera. Every request that is not loopback is
// aborted. No Etsy call, no AI call, no real name, no PIN: the PINs here are made up per run.
//   door   ?deskFor=assembly-N is a yes or no from the sessions the desk keeps: open and beating in the last 8 minutes and not due to be signed
//          out by the auto sign-out rules = yes; none, ended, another desk's, a page silent for 9 minutes, idle for 12 = no; it writes nothing
//          (no session is ended by the question), answers no name, refuses a device that is not an Assembly desk
//   phone  the plain notice when the shop says nobody is signed in (on opening the page, and after each scan), none when somebody is, none when
//          the question cannot be answered (the scan still goes); the scan says "no desk signed in" in the same write only then; the same code
//          can be scanned again once it has left the camera (only after a notice)
//   desk   A  desk closed, scan, desk opens and signs in: the scan loads once, credited to the person who signed in; a reload does not load it again
//          B  the scan is older than 10 minutes: not guessed
//          C  the desk opens in time but the sign-in comes after the 10 minutes: not loaded for the person who signed in
//          D  desk open, signed out (PIN box): the scan waits and loads at the sign-in
//          E  desk signed in: no notice, the scan loads, credited
//          F  desk signed out by idle, then a scan: notice, waits, loads at the next sign-in
//          G  no scan mark (an older phone page), the browser cannot keep the seen mark: nothing is loaded
//          H  two desks of one number: signed in at one is "in"; both out is "out"
//   PW_DIR=<playwright node_modules> CHROMIUM=<chrome> node tests/stations/assembly-desk-presence.cjs
'use strict';
const http = require('http'), fs = require('fs'), path = require('path'), assert = require('assert/strict'), { spawn } = require('child_process');
const root = path.join(__dirname, '../..');
const results = [];
async function check(name, fn) { try { await fn(); results.push([name, null]); } catch (e) { results.push([name, e]); } }
const wait = ms => new Promise(r => setTimeout(r, ms));
async function until(fn, what, ms = 12000) {
  const t0 = Date.now();
  for (;;) { let v; try { v = await fn(); } catch (_) { v = null; } if (v) return v; if (Date.now() - t0 > ms) throw new Error('timed out waiting for ' + what); await wait(80); }
}
const PASS = 'assembly-desk-fake-pass-7731';        // invented here: the real manager passcode is never used
const rnd6 = () => { let p; do p = String(100000 + Math.floor(Math.random() * 900000)); while (/^(\d)\1{5}$/.test(p) || /012345|123456|234567|345678|456789/.test(p)); return p; };
const NAMES = ['Mia R.', 'Noah T.', 'Ola K.', 'Pia S.', 'Quin V.', 'Rae D.', 'Sol B.', 'Tia F.', 'Uma G.'];
const PINS = NAMES.map(() => rnd6());
const ROSTER = {}; NAMES.forEach((n, i) => { ROSTER[PINS[i]] = n; });
const pinOf = name => PINS[NAMES.indexOf(name)];
const allPins = () => PINS.slice();
const oid = k => '39' + String(100000000 + k * 7919).slice(-8);             // ten digits
const LINES = {};
for (let k = 1; k <= 30; k++) LINES[oid(k)] = [{ transaction_id: 91000 + k, listing_id: 1666000000 + k, title: 'Desk Charm ' + k, quantity: 1 + (k % 3), sku: 'DK-' + k, variations: [] }];
const MIME = { '.html': 'text/html', '.js': 'text/javascript', '.css': 'text/css', '.mp3': 'audio/mpeg', '.png': 'image/png', '.svg': 'image/svg+xml' };

const FIREBASE = `(function () {
  const snaps = window.__fbSnaps = {};
  const empty = () => ({ exists: false, data: () => undefined, docs: [], empty: true, size: 0, forEach() {}, docChanges: () => [] });
  function ref(p) {
    const r = { path: p, id: p.split('/').pop(), collection: n => ref(p + '/' + n), doc: n => ref(p + '/' + n),
      where: () => r, orderBy: () => r, limit: () => r, limitToLast: () => r, startAfter: () => r,
      onSnapshot(cb) { (snaps[p] = snaps[p] || []).push(cb); setTimeout(() => { try { const f = window.__firstDoc && window.__firstDoc[p]; cb(f ? { exists: true, data: () => f } : empty()); } catch (_) {} }, 0); return () => {}; },
      get: async () => empty(), set: async () => {}, update: async () => {}, add: async () => ref(p + '/new'), delete: async () => {} };
    return r;
  }
  const firestore = () => ({ collection: n => ref(n), doc: p => ref(p), batch: () => ({ set() {}, update() {}, delete() {}, commit: async () => {} }) });
  firestore.FieldValue = { delete: () => ({}), serverTimestamp: () => ({}), arrayUnion: (...a) => a, increment: n => n };
  firestore.Timestamp = { now: () => ({ toDate: () => new Date(), toMillis: () => Date.now() }) };
  const auth = () => ({ signInAnonymously: async () => ({}), onAuthStateChanged(cb) { setTimeout(() => cb({ uid: 'anon' }), 0); return () => {}; }, currentUser: { uid: 'anon' } });
  window.firebase = { apps: [], initializeApp() { return {}; }, firestore, auth, storage: () => ({ ref: () => ({}) }) };
})();`;
const MATERIALIZE = `window.__toasts = []; window.__modalOpens = [];
window.M = { AutoInit() {}, updateTextFields() {}, toast(o) { try { window.__toasts.push(String((o && o.html) || '')); } catch (_) {} },
  Modal: { init(el) { const i = { open() { try { window.__modalOpens.push(el && el.id); } catch (_) {} }, close() {}, isOpen: false }; if (el) el.__m = i; return i; }, getInstance(el) { return (el && el.__m) || { open() {}, close() {} }; } },
  FormSelect: { init(el) { const i = { destroy() {}, getSelectedValues: () => [el && el.value] }; if (el) el.__fs = i; return i; }, getInstance(el) { return el && el.__fs; } },
  Dropdown: { init() {} } };`;
const CAPTURE = `(function () { let real; try { Object.defineProperty(window, 'StationSession', { configurable: true, get() { return real; },
  set(v) { if (v && typeof v.init === 'function' && !v.__w) { const init = v.init; v.__w = true; v.init = function (o) { window.__ssInit = o; return init.apply(this, arguments); }; } real = v; } }); } catch (_) {} })();`;
const CAMERA = `(function () {
  window.__qrText = '';
  const gum = async () => {
    const c = document.createElement('canvas'); c.width = 640; c.height = 480; const x = c.getContext('2d');
    const draw = () => {
      x.fillStyle = '#fff'; x.fillRect(0, 0, 640, 480);
      if (!window.__qrText || !window.QRCode) return;
      const d = document.createElement('div'); new QRCode(d, { text: window.__qrText, width: 300, height: 300, correctLevel: QRCode.CorrectLevel.M });
      const src = d.querySelector('canvas'); if (src) x.drawImage(src, 170, 90, 300, 300);
    };
    draw(); setInterval(draw, 150);
    return c.captureStream(10);
  };
  try { if (!navigator.mediaDevices) Object.defineProperty(navigator, 'mediaDevices', { value: {}, configurable: true }); navigator.mediaDevices.getUserMedia = gum; } catch (_) {}
})();`;

function backend() {
  return new Promise((resolve, reject) => {
    const ch = spawn(process.execPath, [path.join(root, 'tests/charm-nest/efficiency-portal-backend.cjs')], { env: Object.assign({}, process.env, { PORTAL_PASS: PASS, PORTAL_EMPTY: '1' }) });
    let buf = '', err = ''; ch.stderr.on('data', d => { err += d; });
    const t = setTimeout(() => reject(new Error('the fake shop did not start: ' + err)), 90000);
    ch.stdout.on('data', d => {
      buf += d; const m = /PORT (\d+)/.exec(buf); if (!m) return;
      clearTimeout(t); const port = +m[1], base = `http://127.0.0.1:${port}`, get = async p => (await fetch(base + p)).json();
      resolve({ port, base, child: ch, kill: () => { try { ch.kill(); } catch (_) {} },
        now: async () => (await get('/ctl/now')).now, skew: ms => get('/ctl/skew?ms=' + ms), stats: () => get('/ctl/reads'),
        list: c => get('/ctl/list?c=' + encodeURIComponent(c)), doc: (c, id) => get(`/ctl/doc?c=${encodeURIComponent(c)}&id=${encodeURIComponent(id)}`),
        put: async (c, id, doc) => (await fetch(`${base}/ctl/put`, { method: 'POST', body: JSON.stringify({ c, id, doc }) })).json(),
        desk: async (dev, extra) => { const r = await fetch(`${base}/fn/firebaseOrders?deskFor=${encodeURIComponent(dev)}${extra || ''}`); return { status: r.status, json: await r.json().catch(() => null) }; } });
    });
    ch.on('exit', () => { if (!buf) reject(new Error('the fake shop stopped: ' + err)); });
  });
}

async function main() {
  const pwDir = process.env.PW_DIR || (fs.existsSync(path.join(root, 'node_modules/playwright-core')) ? path.join(root, 'node_modules') : '/opt/node22/lib/node_modules/playwright/node_modules');
  let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { console.log('  – no playwright-core: nothing was run'); return; }
  const B = await backend();
  const server = await new Promise(ok => { const s = http.createServer((req, res) => {
    const f = path.join(root, decodeURIComponent(req.url.split('?')[0]));
    if (!f.startsWith(root) || !fs.existsSync(f) || fs.statSync(f).isDirectory()) { res.writeHead(404); return res.end(); }
    res.writeHead(200, { 'Content-Type': MIME[path.extname(f)] || 'application/octet-stream', 'Cache-Control': 'no-store' }); fs.createReadStream(f).pipe(res);
  }).listen(0, '127.0.0.1', () => ok(s)); });
  const base = `http://127.0.0.1:${server.address().port}`;
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox', '--autoplay-policy=no-user-gesture-required'] });
  const reqLog = [];
  const ctx = { hook: null, relay: [] };                                    // relay: desks that hear the scanner's write (a real Firestore listener does)
  try {
    await B.skew(Date.now() - await B.now());
    await B.put('Brites_Orders', 'Employee Numbers', ROSTER);

    async function newContext(viewport, init, initArg) {
      const context = await browser.newContext({ viewport });
      await context.route(() => true, r => r.abort());
      await context.route(u => u.href.startsWith(base), r => r.continue());
      await context.route(u => /gstatic\.com\/firebasejs\//.test(u.href), r => r.fulfill({ contentType: 'text/javascript', body: /firebase-app-compat/.test(r.request().url()) ? FIREBASE : '' }));
      await context.route(u => /materialize/.test(u.href), r => r.fulfill({ contentType: /\.css/.test(r.request().url()) ? 'text/css' : 'text/javascript', body: /\.css/.test(r.request().url()) ? '' : MATERIALIZE }));
      await context.route(u => /code\.jquery\.com|qz-tray/.test(u.href), r => r.fulfill({ contentType: 'text/javascript', body: '' }));
      await context.route(u => u.href.startsWith(base) && u.pathname.includes('/.netlify/functions/'), async r => {
        const req = r.request(), u = new URL(req.url()), fn = u.pathname.split('/').pop();
        const rec = { method: req.method(), url: req.url(), raw: req.postData() || '', fn }; reqLog.push(rec);
        const json = (o, status) => r.fulfill({ status: status || 200, contentType: 'application/json', body: JSON.stringify(o) });
        if (ctx.hook) { const h = await ctx.hook(rec); if (h) { rec.status = h === 'abort' ? -1 : h.status; return h === 'abort' ? r.abort() : json(h.json, h.status); } }
        if (fn === 'firebaseOrders' && req.method() === 'POST' && /"newMessage"/.test(rec.raw)) return json({ success: true, message: 'Chat doc added.' });
        if (fn === 'etsyOrderProxy') {
          const id = u.searchParams.get('orderId');
          if (!LINES[id]) return json({ error: 'not found' }, 404);
          return json({ receipt_id: Number(id), name: 'Test Buyer', status: 'Paid', transactions: LINES[id] });
        }
        if (fn === 'firebaseOrders') {
          const out = await fetch(B.base + '/fn/firebaseOrders' + u.search, { method: req.method(), body: req.method() === 'POST' ? req.postData() : undefined, headers: { 'x-test-ip': '198.51.100.' + (1 + Math.floor(Math.random() * 200)) } });
          const text = await out.text();
          rec.status = out.status;
          if (rec.raw && /orderNumField/.test(rec.raw) && out.ok) for (const h of ctx.relay.slice()) h(JSON.parse(rec.raw));      // the scanner's write reached Firestore: the desktops listening hear it
          return r.fulfill({ status: out.status, contentType: 'application/json', body: text });
        }
        return json({});
      });
      if (init) await context.addInitScript(init, initArg);
      return context;
    }
    /** a desk page: n = the Assembly number; firstDoc = what the relay listener's first delivery carries (the document as stored); clock = fake timers */
    async function desk(n, o) {
      o = o || {};
      const context = await newContext({ width: 1400, height: 900 }, CAPTURE);
      await context.addInitScript(() => { try { localStorage.setItem('access_token', 'test-token'); localStorage.setItem('token_expires_at', String(Math.floor(Date.now() / 1000) + 86400)); } catch (_) {} });
      if (o.firstDoc) await context.addInitScript(a => { window.__firstDoc = { ['Brites_Orders/assembly-scan-' + a.n]: a.doc }; }, { n, doc: o.firstDoc });
      if (o.noSeenMark) await context.addInitScript(() => { const set = Storage.prototype.setItem; Storage.prototype.setItem = function (k, v) { if (/^assemblyRelaySeen/.test(String(k))) throw new Error('storage refused'); return set.apply(this, arguments); }; });
      if (o.stored) await context.addInitScript(a => { try { for (const k of Object.keys(a)) localStorage.setItem(k, a[k]); } catch (_) {} }, o.stored);
      const page = await context.newPage(); const errors = []; page.on('pageerror', e => errors.push(e.message));
      if (o.clock) await page.clock.install({ time: Date.now() });
      await page.goto(`${base}/assembly-${n}.html`);
      await page.waitForFunction(() => window.StationActivity && window.StationSession && window.OrderTimeline && window.StationTimeline && window.__ssInit, null, { timeout: 25000 });
      await page.waitForFunction(k => (window.__fbSnaps[k] || []).length > 0, `Brites_Orders/assembly-scan-${n}`, { timeout: 15000 });
      await wait(500);
      const d = { n, context, page, errors };
      ctx.relay.push(body => { if (body.orderNumber === 'assembly-scan-' + n && !d.closed) page.evaluate(a => { const cbs = window.__fbSnaps['Brites_Orders/assembly-scan-' + a.n]; cbs[cbs.length - 1]({ exists: true, data: () => a.data }); },
        { n, data: { 'Order Number': body.orderNumField, 'Brites Messages': body.britesMessages, 'Shipping Label Timestamps': body.shippingLabelTimestamps, 'Client Name': body.clientName, 'Employee Name': body.employeeName } }).catch(() => {}); });
      return d;
    }
    async function phone(n) {
      const context = await newContext({ width: 390, height: 800 }, CAMERA);
      const page = await context.newPage(); const errors = []; page.on('pageerror', e => errors.push(e.message));
      await page.goto(`${base}/assembly-scan-${n}.html`);
      await page.waitForFunction(() => typeof window.handleScannedCode === 'function', null, { timeout: 15000 });
      return { n, context, page, errors };
    }
    const relayWrites = () => reqLog.filter(q => /orderNumField/.test(q.raw));
    async function showQR(ph, code) {
      const before = relayWrites().length;
      await ph.page.evaluate(c => { window.__qrText = c; }, code);
      await until(() => relayWrites().length > before && relayWrites().pop().status, 'the scanner page to decode ' + code + ' and relay it', 15000);
      return JSON.parse(relayWrites().pop().raw);
    }
    const toasts = p => p.page.evaluate(() => window.__toasts.slice());
    const NOBODY = n => new RegExp('^Nobody is signed in at the Assembly ' + n + ' desk\\. Sign in there so (this scan|your scans) counts?\\.$');
    const sessionsOf = async (name, dev) => (await B.list('Station_Sessions')).filter(s => s.person === name && s.device === dev);
    const eventsOf = async (dev, order) => (await B.list('Station_Activity')).filter(e => e.device === dev && e.action === 'scan' && (!order || e.orderId === order));
    const qnote = d => d.page.evaluate(() => { const b = document.getElementById('stationScanQueueNote'); return b && b.style.display !== 'none' ? b.textContent : ''; });
    const signIn = async (d, name) => {
      await d.page.evaluate(() => { const i = document.getElementById('employeeNumberInput'); i.dataset.raw = ''; i.value = ''; });
      await d.page.focus('#employeeNumberInput');
      await d.page.keyboard.type(pinOf(name));
      await d.page.evaluate(() => document.getElementById('employeeLoginBtn').click());
      await d.page.waitForFunction(() => window.isEmployeeLoggedIn === true && window.StationActivity.who(), null, { timeout: 10000 });
    };
    const signOutBtn = async d => { await d.page.evaluate(() => document.getElementById('signOutBtn').click()); await wait(700); };
    const relayDoc = n => B.doc('Brites_Orders', 'assembly-scan-' + n);
    const putSession = (id, o) => B.put('Station_Sessions', id, Object.assign({ id, station: 'assembly', computerId: 'pc-' + id.replace(/\W/g, '').slice(0, 20).padEnd(8, 'x'), computerLabel: 'x', employeeId: '', endAt: null, endReason: null, minutes: 0 }, o));
    const MIN = 60000;

    /* ── the door ── */
    await check('door ?deskFor=: nobody, an open beating session, an ended one, another desk\'s, a page silent for 9 minutes, one idle for 12: no, yes, no, no, no, no', async () => {
      const now = await B.now();
      assert.deepEqual((await B.desk('assembly-3')).json.signedIn, false);
      await putSession('assembly__assembly-3__Mia R.', { person: 'Mia R.', device: 'assembly-3', startAt: now - 30 * MIN, lastSeenAt: now - 2 * MIN, lastInputAt: now - 3 * MIN, admin: false });
      const yes = await B.desk('assembly-3'); assert.equal(yes.status, 200); assert.equal(yes.json.success, true); assert.equal(yes.json.signedIn, true); assert.equal(yes.json.device, 'assembly-3');
      assert(!JSON.stringify(yes.json).includes('Mia'), 'the answer names the person');
      assert.equal((await B.desk('assembly-2')).json.signedIn, false, 'another desk\'s session counted');
      await putSession('assembly__assembly-2__Noah T.', { person: 'Noah T.', device: 'assembly-2', startAt: now - 40 * MIN, lastSeenAt: now - 20 * MIN, endAt: now - 20 * MIN, endReason: 'signOut', minutes: 20 });
      assert.equal((await B.desk('assembly-2')).json.signedIn, false, 'an ended session counted');
      await putSession('assembly__assembly-4__Ola K.', { person: 'Ola K.', device: 'assembly-4', startAt: now - 60 * MIN, lastSeenAt: now - 9 * MIN, lastInputAt: now - 9 * MIN, admin: false });
      assert.equal((await B.desk('assembly-4')).json.signedIn, false, 'a page that stopped beating 9 minutes ago counted');
      await putSession('assembly__assembly-1__Pia S.', { person: 'Pia S.', device: 'assembly-1', startAt: now - 60 * MIN, lastSeenAt: now - 1 * MIN, lastInputAt: now - 12 * MIN, admin: false });
      assert.equal((await B.desk('assembly-1')).json.signedIn, false, 'a person idle for 12 minutes counted');
      const row = await B.doc('Station_Sessions', 'assembly__assembly-1__Pia S.'); assert.equal(row.endAt, null, 'the question ended the session');
      for (const id of ['assembly__assembly-3__Mia R.', 'assembly__assembly-4__Ola K.', 'assembly__assembly-1__Pia S.', 'assembly__assembly-2__Noah T.']) await B.put('Station_Sessions', id, Object.assign({}, await B.doc('Station_Sessions', id), { endAt: now - MIN, endReason: 'signOut' }));
    });
    await check('door ?deskFor=: it writes nothing, refuses anything that is not an Assembly desk, and cannot be asked too fast', async () => {
      const w0 = (await B.stats()).writes;
      for (let i = 0; i < 5; i++) await B.desk('assembly-1');
      assert.equal((await B.stats()).writes, w0, 'the question wrote');
      for (const bad of ['shipping-1', 'assembly-', 'assembly-0', 'assembly-100', 'Assembly_1', '', 'assembly-1%27']) assert.equal((await B.desk(bad)).status === 200 && (await B.desk(bad)).json.success === true, false, 'accepted ' + bad);
      assert.equal((await B.desk('shipping-1')).status, 400);
      let limited = 0; for (let i = 0; i < 140; i++) { const r = await fetch(`${B.base}/fn/firebaseOrders?deskFor=assembly-2`, { headers: { 'x-test-ip': '192.0.2.77' } }); if (r.status === 429) limited++; }
      assert(limited > 0, 'no limit on the question');
    });

    /* ── the phone, and the desk that opens later ── */
    const [A, B2, C, D, E, F, G, H, I] = NAMES;

    // A · desk closed, scan, desk opens and signs in
    const phA = await phone(1);
    await check('assembly-scan-1 opened while nobody is signed in at the desk says so plainly (once, a moment after opening)', async () => {
      await until(async () => (await toasts(phA)).some(t => NOBODY(1).test(t)), 'the notice on opening');
      assert.equal((await toasts(phA)).filter(t => NOBODY(1).test(t)).length, 1);
      assert(/your scans count/.test((await toasts(phA)).find(t => NOBODY(1).test(t))));
    });
    const X1 = oid(1);
    let bodyA;
    await check('a scan to a desk nobody is signed in at: sent, the notice names this scan, the same write says "no desk signed in", no new key, no person, no PIN', async () => {
      bodyA = await showQR(phA, X1);
      await until(async () => (await toasts(phA)).some(t => /this scan counts/.test(t)), 'the notice after the scan');
      const t = await toasts(phA); assert(t.some(x => NOBODY(1).test(x) && /this scan counts/.test(x)), JSON.stringify(t));
      assert(t.some(x => /^Firestore updated: doc 'assembly-scan-1' => /.test(x)));
      assert.match(bodyA.britesMessages, /no desk signed in/);
      assert.equal(Object.keys(bodyA).sort().join(), 'britesMessages,clientName,employeeName,orderNumField,orderNumber,shippingLabelTimestamps');
      for (const p of allPins()) assert(!JSON.stringify(bodyA).includes(p));
      assert.equal((await relayDoc(1))['Order Number'], X1);
      assert.equal((await eventsOf('assembly-1')).length, 0, 'recorded with nobody signed in');
    });
    await check('the same code can be scanned again once it has left the camera (only after the notice); while it stays in view it is not sent again', async () => {
      const n0 = relayWrites().length;
      await wait(1800); assert.equal(relayWrites().length, n0, 'the code in view was sent again');
      await phA.page.evaluate(() => { window.__qrText = ''; });
      await wait(2600);                                                         // out of view for more than 1.5 s
      await showQR(phA, X1);
      assert.equal(relayWrites().length, n0 + 1, 'a second showing was not sent');
    });
    const docA = await relayDoc(1);
    let dA;
    await check('A: a desk that opens within 10 minutes of that scan, signs in: the scan loads once, credited to the person who signed in, with its note while waiting', async () => {
      dA = await desk(1, { firstDoc: docA });
      await until(async () => /1 phone scan waiting for a sign-in/.test(await qnote(dA)), 'the waiting note');
      assert.equal((await eventsOf('assembly-1')).length, 0, 'credited to nobody before a sign-in');
      await signIn(dA, A);
      await until(async () => (await eventsOf('assembly-1', X1)).length === 1, 'the recovered scan');
      const e = (await eventsOf('assembly-1', X1))[0]; assert.equal(e.person, A); assert.equal(e.detail, 'phone scan');
      assert.equal(await dA.page.inputValue('#etsyOrderNumber'), X1);
      assert.equal(await qnote(dA), '');
      assert.equal(await dA.page.evaluate(() => localStorage.getItem('assemblyRelaySeen.assembly-scan-1')), docA['Shipping Label Timestamps']);
    });
    await check('A: the desk page, still open, hears the same code from the phone again (shown again after it left the camera): ignored, still one scan event', async () => {
      await phA.page.evaluate(() => { window.__qrText = ''; deskAt = 0; });
      await wait(2600);
      const b = await showQR(phA, X1);
      assert.equal(b.britesMessages, '(Auto push from assembly-scan-1.html)', 'the desk is signed in: the scan carries no mark');
      await wait(1000);
      assert.equal((await eventsOf('assembly-1', X1)).length, 1, 'the same scan was counted twice');
    });
    await check('A: reloading that desk does not load the scan again (this browser has handled it): still one scan event', async () => {
      await dA.page.reload();
      await dA.page.waitForFunction(() => window.StationActivity && window.StationSession && window.__ssInit && window.isEmployeeLoggedIn === true, null, { timeout: 25000 });
      await wait(1500);
      assert.equal((await eventsOf('assembly-1', X1)).length, 1, 'the scan was counted twice');
      await signOutBtn(dA);
      await until(async () => (await B.desk('assembly-1')).json.signedIn === false, 'the desk to be signed out');
      dA.closed = true;
    });

    // B · older than 10 minutes
    await check('B: a scan sent more than 10 minutes before the desk opened is not guessed: nothing waits, nothing loads', async () => {
      const doc = { 'Order Number': oid(2), 'Brites Messages': '(Auto push from assembly-scan-2.html; no desk signed in)', 'Shipping Label Timestamps': new Date(Date.now() - 11 * MIN).toISOString(), 'Client Name': 'Scanner Page', 'Employee Name': 'ScannerBot' };
      const d = await desk(2, { firstDoc: doc });
      await signIn(d, B2); await wait(1500);
      assert.equal(await qnote(d), ''); assert.equal((await eventsOf('assembly-2')).length, 0);
      assert.equal(await d.page.inputValue('#etsyOrderNumber'), '');
      await signOutBtn(d);
    });

    // C · opens in time, signs in too late
    await check('C: the desk opens in time but the sign-in comes after the 10 minutes: the scan is dropped, not loaded for whoever signed in', async () => {
      const doc = { 'Order Number': oid(3), 'Brites Messages': '(Auto push from assembly-scan-3.html; no desk signed in)', 'Shipping Label Timestamps': new Date(Date.now() - 9 * MIN - 30000).toISOString(), 'Client Name': 'Scanner Page', 'Employee Name': 'ScannerBot' };
      const d = await desk(3, { firstDoc: doc, clock: true });
      await until(async () => /1 phone scan waiting for a sign-in/.test(await qnote(d)), 'the waiting note');
      await d.page.clock.runFor(2 * MIN);
      await signIn(d, C); await wait(1500);
      assert.equal((await eventsOf('assembly-3')).length, 0, 'a late sign-in was credited with an old scan');
      assert.equal(await qnote(d), '');
      await signOutBtn(d);
    });

    // D · desk open, signed out, scan waits
    const phD = await phone(4);
    let dD;
    await check('D: desk open at the PIN box: the phone says nobody is signed in, the scan waits ("waiting for a sign-in") and loads after the sign-in, credited to the person who signed in', async () => {
      dD = await desk(4);
      await until(async () => (await toasts(phD)).some(t => NOBODY(4).test(t)), 'the notice on opening');
      const X = oid(4); const b = await showQR(phD, X);
      assert.match(b.britesMessages, /no desk signed in/);
      await until(async () => /1 phone scan waiting for a sign-in/.test(await qnote(dD)), 'the waiting note');
      assert.equal((await eventsOf('assembly-4')).length, 0);
      await signIn(dD, D);
      await until(async () => (await eventsOf('assembly-4', X)).length === 1, 'the scan after the sign-in');
      assert.equal((await eventsOf('assembly-4', X))[0].person, D);
      assert.equal(await dD.page.evaluate(() => localStorage.getItem('assemblyRelaySeen.assembly-scan-4')) != null, true);
    });

    // E · desk signed in
    await check('E: desk signed in: no notice, the scan carries no "no desk signed in", it loads at once and is credited', async () => {
      await until(async () => (await B.desk('assembly-4')).json.signedIn === true, 'the desk to count as signed in');
      await phD.page.evaluate(() => { window.__toasts.length = 0; window.__qrText = ''; deskAt = 0; }); await wait(2600);
      const X = oid(5); const b = await showQR(phD, X);
      assert.doesNotMatch(b.britesMessages, /no desk/); assert.equal(b.britesMessages, '(Auto push from assembly-scan-4.html)');
      await until(async () => (await eventsOf('assembly-4', X)).length === 1, 'the scan');
      assert.equal((await eventsOf('assembly-4', X))[0].person, D);
      await wait(500); assert(!(await toasts(phD)).some(t => /Nobody is signed in/.test(t)), JSON.stringify(await toasts(phD)));
    });

    // F · the desk signed the person out (idle), then a scan
    await check('F: the desk signed the person out by idle: the door says nobody; a scan gets the notice, waits at the PIN box and loads at the next sign-in, for the next person', async () => {
      await dD.page.evaluate(() => { StationSession.signedOut('idle'); window.__ssInit.signOut('idle', null); });
      await until(async () => (await B.desk('assembly-4')).json.signedIn === false, 'the door to say nobody after the idle sign-out');
      assert.equal((await sessionsOf(D, 'assembly-4'))[0].endReason, 'idle');
      await phD.page.evaluate(() => { window.__toasts.length = 0; window.__qrText = ''; deskAt = 0; }); await wait(2600);
      const X = oid(6); const b = await showQR(phD, X);
      assert.match(b.britesMessages, /no desk signed in/);
      await until(async () => (await toasts(phD)).some(t => NOBODY(4).test(t) && /this scan counts/.test(t)), 'the notice');
      await until(async () => /1 phone scan waiting for a sign-in/.test(await qnote(dD)), 'the waiting note at the PIN box');
      assert.equal((await eventsOf('assembly-4', X)).length, 0);
      await signIn(dD, E);
      await until(async () => (await eventsOf('assembly-4', X)).length === 1, 'the scan after the new sign-in');
      assert.equal((await eventsOf('assembly-4', X))[0].person, E, 'credited to the wrong person');
      await signOutBtn(dD);
    });

    // G · no scan mark, or no storage for the seen mark
    await check('G: a fresh scan WITHOUT the mark (an older phone page), or a browser that cannot keep the seen mark: nothing is loaded at the desk that opens', async () => {
      const stamp = () => new Date(Date.now() - 2 * MIN).toISOString();
      const plain = { 'Order Number': oid(7), 'Brites Messages': '(Auto push from assembly-scan-2.html)', 'Shipping Label Timestamps': stamp(), 'Client Name': 'Scanner Page', 'Employee Name': 'ScannerBot' };
      const d1 = await desk(2, { firstDoc: plain });
      await signIn(d1, F); await wait(1500);
      assert.equal((await eventsOf('assembly-2', oid(7))).length, 0, 'an unmarked scan was loaded'); assert.equal(await qnote(d1), ''); await signOutBtn(d1); await d1.context.close();
      const marked = Object.assign({}, plain, { 'Order Number': oid(8), 'Brites Messages': '(Auto push from assembly-scan-2.html; no desk signed in)' });
      const d2 = await desk(2, { firstDoc: marked, noSeenMark: true });
      await signIn(d2, G); await wait(1500);
      assert.equal((await eventsOf('assembly-2', oid(8))).length, 0, 'loaded although this browser cannot remember it');
      await signOutBtn(d2); await d2.context.close();
      const d3 = await desk(2, { firstDoc: marked });                                    // the same, with a browser that can: it IS loaded (so the two checks above are about the cause)
      await signIn(d3, G);
      await until(async () => (await eventsOf('assembly-2', oid(8))).length === 1, 'the marked scan in a browser that can keep the seen mark');
      await signOutBtn(d3); await d3.context.close();
    });

    // the phone cannot ask
    await check('the phone cannot reach the question: no notice, the scan still goes (no mark)', async () => {
      ctx.hook = rec => (/deskFor=/.test(rec.url) ? 'abort' : null);
      const ph = await phone(3);
      try {
        await wait(2200); assert(!(await toasts(ph)).some(t => /Nobody is signed in/.test(t)), 'a notice without an answer');
        const b = await showQR(ph, oid(9)); assert.equal(b.britesMessages, '(Auto push from assembly-scan-3.html)');
        await wait(600); assert(!(await toasts(ph)).some(t => /Nobody is signed in/.test(t)));
        assert.equal((await relayDoc(3))['Order Number'], oid(9));
      } finally { ctx.hook = null; await ph.context.close(); }
    });
    await check('an older shop that does not know the question (400) is the same: no notice, the scan goes', async () => {
      ctx.hook = rec => (/deskFor=/.test(rec.url) ? { status: 400, json: { success: false, msg: 'orderId required' } } : null);
      const ph = await phone(2);
      try { await wait(2200); const b = await showQR(ph, oid(10)); assert.equal(b.britesMessages, '(Auto push from assembly-scan-2.html)'); assert(!(await toasts(ph)).some(t => /Nobody is signed in/.test(t))); }
      finally { ctx.hook = null; await ph.context.close(); }
    });

    // H · two desks of one number
    await check('H: two desk pages of one number: signed in at one = somebody is signed in (no notice); both signed out = nobody (notice)', async () => {
      const d1 = await desk(1), d2 = await desk(1);
      await signIn(d1, H);
      await until(async () => (await B.desk('assembly-1')).json.signedIn === true, 'the desk to count as signed in');
      await phA.page.evaluate(() => { window.__toasts.length = 0; window.__qrText = ''; deskAt = 0; }); await wait(2600);
      const b = await showQR(phA, oid(11)); assert.equal(b.britesMessages, '(Auto push from assembly-scan-1.html)');
      await until(async () => (await eventsOf('assembly-1', oid(11))).length >= 1, 'the scan at the desk that is signed in');
      assert.equal((await eventsOf('assembly-1', oid(11))).every(e => e.person === H), true, 'a scan credited to somebody who did not sign in');
      await wait(500); assert(!(await toasts(phA)).some(t => /Nobody is signed in/.test(t)));
      await signOutBtn(d1);
      await until(async () => (await B.desk('assembly-1')).json.signedIn === false, 'both desks signed out');
      await phA.page.evaluate(() => { window.__toasts.length = 0; window.__qrText = ''; deskAt = 0; }); await wait(2600);
      await showQR(phA, oid(12));
      await until(async () => (await toasts(phA)).some(t => NOBODY(1).test(t) && /this scan counts/.test(t)), 'the notice with both signed out');
      d1.closed = d2.closed = true; await d1.context.close(); await d2.context.close();
    });

    await check('no PIN in any request but the login door\'s, in a stored document or in any toast; no page error in the Assembly pages or phones', async () => {
      for (const q of reqLog) for (const p of allPins()) { assert(!q.url.includes(p), 'a PIN in a URL'); assert(!q.raw.includes(p) || q.raw === JSON.stringify({ pinLogin: p }), 'a PIN in a body to ' + q.fn); }
      for (const c of ['Station_Sessions', 'Station_Activity', 'Station_Live']) { const text = JSON.stringify(await B.list(c)); for (const p of allPins()) assert(!text.includes(p), 'a PIN in ' + c); }
      assert.deepEqual([phA, phD].flatMap(p => p.errors), [], 'page errors on the phones');
      assert.deepEqual([dA, dD].flatMap(p => p.errors).filter(m => !/does not provide an export named 'getApp'/.test(m)), [], 'page errors on the desks');   // (the page's own module import of Firebase's storage is not served by the fake)
    });
  } finally { await browser.close(); server.close(); B.kill(); }
}

main().catch(e => results.push(['setup', e])).finally(() => {
  let bad = 0;
  for (const [name, e] of results) { if (e) { bad++; console.log('FAIL', name, '\n   ', String(e && e.stack || e).split('\n').slice(0, 6).join('\n    ')); } else console.log('ok  ', name); }
  console.log(`${results.length - bad}/${results.length} passed`);
  process.exit(bad ? 1 : 0);
});
