// Assembly (assembly-1..4.html and their phone scanners assembly-scan-1..4.html) wired into the Employee efficiency portal
// (Paul, 6 Oct 2026, stations round 2: "every station app, each with its own scanner, fully wired into the Employee efficiency portal").
// The REAL pages, the REAL station libraries (station-session.js, station-activity.js, station-live-order.js, station-scan-queue.js,
// order-timeline.js, station-timeline.js) and the REAL server code (firebaseOrders.js: the PIN login door, {session}, {activity}, {live},
// {timeline}, the scanner's relay write; employeeEfficiency.js: the live board, the Overview and the person view) over ONE in-memory
// Firestore, in its own process (tests/charm-nest/efficiency-portal-backend.cjs, clock aligned to this machine's). Fake: the Etsy order
// answers, Firebase's browser SDK, Materialize, and the phone's camera (a canvas stream that shows the order's QR; the page's own
// jsQR decodes it). Every request that is not loopback is aborted. No Etsy call, no AI call, no real name, no PIN: the PINs here are
// made up per run, written only into the fake roster and checked as booleans.
//   for each of assembly-1..4 (own person, own phone scanner):
//   1  nobody signed in: a phone scan is kept ("waiting for a sign-in"), nothing is recorded, nothing is live
//   2  the PIN box signs in through the server door: ONE Station_Sessions row under the person's NAME (station assembly, device assembly-N,
//      no PIN, no employee id), open (no end); the waiting phone scan then loads and is credited to that person
//   3  a typed order: one scan event (order id, person, pieces) and the live order (Station_Live): the portal's `live` op shows it at
//      Assembly N for that person with the QR text, one picture per piece, the true piece count and the scan time
//   4  a phone scan from this station's own scanner (camera, jsQR, relay write, desktop) is credited to the person signed in; the
//      live order says "phone scan"
//   5  QA1, then Done, an Assembled stamp, QA 2: one completion; the live order ends at the first stamp
//   6  the Overview, the person view (Day) and the live board agree on pieces, orders and scans of the assembly station
//   7  the page's own signOut copes with the new end reasons "idle" and "closing" (the library calls it): its own login is cleared,
//      its own sign-in is shown, a calm one-line notice names the reason, the order and the chat stay on screen, and the next person
//      signs in and is credited
//   8  Sign Out ends the session (reason signOut), the live order goes idle, more scans wait
//   once: the scanner pages relay to their OWN desktop (scan-N to assembly-N), a failed relay is retried, no person or PIN in a scan
//   once: assembly-1..4 and assembly-scan-1..4 are identical apart from their name
//   PW_DIR=<playwright node_modules> CHROMIUM=<chrome> node tests/stations/assembly-wired.cjs      (ONLY=1,2 to run some stations)
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
const ONLY = (process.env.ONLY || '').split(',').filter(Boolean).map(Number);
const PASS = 'assembly-wired-fake-pass-5521';        // invented here: the real manager passcode is never used
const rnd6 = () => { let p; do p = String(100000 + Math.floor(Math.random() * 900000)); while (/^(\d)\1{5}$/.test(p) || /012345|123456|234567|345678|456789/.test(p)); return p; };

/* ── the people (made up) and their numbers (made up per run) ── */
const PEOPLE = { 1: ['Marco R.', 'Lena K.'], 2: ['Tess W.', 'Omar B.'], 3: ['Ivy Y.', 'Dana P.'], 4: ['Ana M.', 'Kofi A.'] };
const PINS = {}; for (const n of [1, 2, 3, 4]) PINS[n] = [rnd6(), rnd6()];
const BEAT = { name: 'Beat T.', pin: rnd6() };                                  // for the beat check on its own page
const allPins = () => [].concat(...Object.values(PINS), BEAT.pin);
const ROSTER = {}; for (const n of [1, 2, 3, 4]) { ROSTER[PINS[n][0]] = PEOPLE[n][0]; ROSTER[PINS[n][1]] = PEOPLE[n][1]; }
ROSTER[BEAT.pin] = BEAT.name;

/* ── orders: Etsy's answers (fake) ── order ids are 10 digits, one block per station so the four runs never share an order ── */
const oid = (n, k) => '38' + String(n) + String(1000 + k).padStart(7, '0').slice(-7) + '0';     // e.g. 3811000010
const ORD = n => ({ WAIT: oid(n, 1), TWO: oid(n, 2), PHONE: oid(n, 3), LATE: oid(n, 4), QA2: oid(n, 5), AFTER: oid(n, 6) });
const LINES = {};
for (const n of [1, 2, 3, 4]) {
  const o = ORD(n);
  LINES[o.WAIT] = [{ transaction_id: 70000 + n * 10 + 1, listing_id: 1555000000 + n * 10 + 1, title: 'Waiting Charm', quantity: 1, sku: 'W-' + n, variations: [] }];
  LINES[o.TWO] = [{ transaction_id: 70000 + n * 10 + 2, listing_id: 1555000000 + n * 10 + 2, title: 'Name Charm', quantity: 2, sku: 'AAA-' + n, variations: [{ formatted_name: 'Size', formatted_value: 'Large' }] },
                  { transaction_id: 70000 + n * 10 + 3, listing_id: 1555000000 + n * 10 + 3, title: 'Heart Charm', quantity: 1, sku: 'BBB-' + n, variations: [] }];
  LINES[o.PHONE] = [{ transaction_id: 70000 + n * 10 + 4, listing_id: 1555000000 + n * 10 + 4, title: 'Solo Charm', quantity: 3, sku: 'SOLO-' + n, variations: [] }];
  LINES[o.LATE] = [{ transaction_id: 70000 + n * 10 + 5, listing_id: 1555000000 + n * 10 + 5, title: 'Late Charm', quantity: 1, sku: 'L-' + n, variations: [] }];
  LINES[o.QA2] = [{ transaction_id: 70000 + n * 10 + 6, listing_id: 1555000000 + n * 10 + 6, title: 'Queue Charm', quantity: 2, sku: 'Q-' + n, variations: [] }];
  LINES[o.AFTER] = [{ transaction_id: 70000 + n * 10 + 7, listing_id: 1555000000 + n * 10 + 7, title: 'After Charm', quantity: 1, sku: 'A-' + n, variations: [] }];
}
const UNITS = id => (LINES[id] || []).reduce((s, l) => s + l.quantity, 0);
const MIME = { '.html': 'text/html', '.js': 'text/javascript', '.css': 'text/css', '.mp3': 'audio/mpeg', '.png': 'image/png', '.svg': 'image/svg+xml' };

const FIREBASE = `(function () {
  const snaps = window.__fbSnaps = {};
  const empty = () => ({ exists: false, data: () => undefined, docs: [], empty: true, size: 0, forEach() {}, docChanges: () => [] });
  function ref(p) {
    const r = { path: p, id: p.split('/').pop(), collection: n => ref(p + '/' + n), doc: n => ref(p + '/' + n),
      where: () => r, orderBy: () => r, limit: () => r, limitToLast: () => r, startAfter: () => r,
      onSnapshot(cb) { (snaps[p] = snaps[p] || []).push(cb); setTimeout(() => { try { cb(empty()); } catch (_) {} }, 0); return () => {}; },
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
/** the page's own StationSession.init options are kept, so the test can call the page's signOut exactly as the library does */
const CAPTURE = `(function () { let real; try { Object.defineProperty(window, 'StationSession', { configurable: true, get() { return real; },
  set(v) { if (v && typeof v.init === 'function' && !v.__w) { const init = v.init; v.__w = true; v.init = function (o) { window.__ssInit = o; return init.apply(this, arguments); }; } real = v; } }); } catch (_) {} })();`;
/** the phone's camera: a canvas stream that shows the QR of window.__qrText (white margin around it), read by the page's own jsQR */
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

/* ── the whole shop in its own process: the real server code over an in-memory Firestore ── */
function backend() {
  return new Promise((resolve, reject) => {
    const ch = spawn(process.execPath, [path.join(root, 'tests/charm-nest/efficiency-portal-backend.cjs')], { env: Object.assign({}, process.env, { PORTAL_PASS: PASS, PORTAL_EMPTY: '1' }) });
    let buf = '', err = ''; ch.stderr.on('data', d => { err += d; });
    const t = setTimeout(() => reject(new Error('the fake shop did not start: ' + err)), 90000);
    ch.stdout.on('data', d => {
      buf += d; const m = /PORT (\d+)/.exec(buf); if (!m) return;
      clearTimeout(t); const port = +m[1], base = `http://127.0.0.1:${port}`, get = async p => (await fetch(base + p)).json();
      resolve({ port, base, child: ch, kill: () => { try { ch.kill(); } catch (_) {} },
        door: async (body, q, ip) => { const r = await fetch(`${base}/fn/firebaseOrders${q || ''}`, { method: 'POST', body: JSON.stringify(body), headers: ip ? { 'x-test-ip': ip } : {} }); return { status: r.status, json: await r.json().catch(() => null) }; },
        ask: async body => (await fetch(`${base}/fn/employeeEfficiency`, { method: 'POST', body: JSON.stringify(Object.assign({ key: PASS }, body)) })).json(),
        now: async () => (await get('/ctl/now')).now, skew: ms => get('/ctl/skew?ms=' + ms),
        list: c => get('/ctl/list?c=' + encodeURIComponent(c)), doc: (c, id) => get(`/ctl/doc?c=${encodeURIComponent(c)}&id=${encodeURIComponent(id)}`),
        put: async (c, id, doc) => (await fetch(`${base}/ctl/put`, { method: 'POST', body: JSON.stringify({ c, id, doc }) })).json() });
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
  try {
    // the fake shop's clock is the seed's day: move it to this machine's now, so the pages' clocks and the server's agree
    await B.skew(Date.now() - await B.now());
    await B.put('Brites_Orders', 'Employee Numbers', ROSTER);                                       // the fake roster: number -> name (read only by the login door)
    for (const n of [1, 2, 3, 4]) for (const l of [].concat(...Object.values(ORD(n)).map(id => LINES[id]))) {   // what the app already stores for the pictures
      await B.put('Charm_Master_Index', l.sku, Object.assign({ sku: l.sku, thumbUrl: `https://thumbs.test/vec/${l.sku}.svg` }, /Large/.test(JSON.stringify(l.variations)) ? { sizes: { L: { thumbUrl: `https://thumbs.test/vec/${l.sku}-L.svg` } } } : {}));
      await B.put('Etsy_Listing_Image_Cache', String(l.listing_id), { images: [{ rank: 1, url_570xN: `https://i.etsystatic.com/fake/il_570xN.${l.listing_id}_abc.jpg` }] });
    }
    const reqLog = [];                                                                               // every function request any page made (for the PIN check)

    /** one browser context that talks to the fake shop: the functions go to the real server code, Etsy answers from LINES */
    async function newContext(viewport, init) {
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
        if (fn === 'firebaseOrders' && req.method() === 'POST' && /"newMessage"/.test(rec.raw)) return json({ success: true, message: 'Chat doc added.' });   // (the Team chat is a sub-collection the in-memory Firestore does not model; it is no part of the portal)
        if (fn === 'etsyOrderProxy') {
          const id = u.searchParams.get('orderId');
          if (!LINES[id]) return json({ error: 'not found' }, 404);
          return json({ receipt_id: Number(id), name: 'Test Buyer', status: 'Paid', transactions: LINES[id] });
        }
        if (fn === 'firebaseOrders') {
          const out = await fetch(B.base + '/fn/firebaseOrders' + u.search, { method: req.method(), body: req.method() === 'POST' ? req.postData() : undefined, headers: { 'x-test-ip': '198.51.100.' + (1 + Math.floor(Math.random() * 200)) } });
          const text = await out.text();
          rec.status = out.status;
          if (rec.raw && /orderNumField/.test(rec.raw) && out.ok && ctx.relay) ctx.relay(JSON.parse(rec.raw));      // the scanner's write reached Firestore: the desktop's listener hears it
          return r.fulfill({ status: out.status, contentType: 'application/json', body: text });
        }
        return json({});
      });
      if (init) await context.addInitScript(init);
      return context;
    }
    const ctx = { hook: null, relay: null };

    async function desktop(n) {
      const context = await newContext({ width: 1400, height: 900 }, CAPTURE);
      await context.addInitScript(() => { try { localStorage.setItem('access_token', 'test-token'); localStorage.setItem('token_expires_at', String(Math.floor(Date.now() / 1000) + 86400)); } catch (_) {} });
      const page = await context.newPage(); const errors = []; page.on('pageerror', e => errors.push(e.message));
      await page.goto(`${base}/assembly-${n}.html`);
      await page.waitForFunction(() => window.StationActivity && window.StationSession && window.OrderTimeline && window.StationTimeline && window.__ssInit, null, { timeout: 25000 });
      await page.waitForFunction(k => (window.__fbSnaps[k] || []).length > 0, `Brites_Orders/assembly-scan-${n}`, { timeout: 15000 });
      await wait(400);
      return { n, context, page, errors };
    }
    async function phone(n) {
      const context = await newContext({ width: 390, height: 800 }, CAMERA);
      const page = await context.newPage(); const errors = []; page.on('pageerror', e => errors.push(e.message));
      await page.goto(`${base}/assembly-scan-${n}.html`);
      await page.waitForFunction(() => typeof window.handleScannedCode === 'function', null, { timeout: 15000 });
      return { n, context, page, errors };
    }
    /** the phone shows an order's QR to its camera; resolves when the page has decoded it and answered the relay write */
    async function showQR(ph, code) {
      const before = reqLog.filter(q => /orderNumField/.test(q.raw)).length;
      await ph.page.evaluate(c => { window.__qrText = c; }, code);
      await until(() => reqLog.filter(q => /orderNumField/.test(q.raw)).length > before && reqLog.filter(q => /orderNumField/.test(q.raw)).pop().status, 'the scanner page to decode ' + code + ' and relay it', 15000);
    }
    const deliver = async (desk, body) => desk.page.evaluate(n => { const cbs = window.__fbSnaps['Brites_Orders/assembly-scan-' + n.n]; cbs[cbs.length - 1]({ exists: true, data: () => ({ 'Order Number': n.code }) }); }, { n: desk.n, code: body.orderNumField });

    const namesOf = st => (st.people || []).map(p => (typeof p === 'string' ? p : p && p.name));     // the board's people: names, or (C4) objects with the name in them
    const sessionsOf = async (name, dev) => (await B.list('Station_Sessions')).filter(s => s.person === name && s.device === dev);
    const eventsOf = async dev => (await B.list('Station_Activity')).filter(e => e.device === dev).sort((a, b) => a.at - b.at || a.seq - b.seq);
    const liveDoc = async (dev, name) => B.doc('Station_Live', `assembly__${dev}__${name}`);
    const flush = d => d.page.evaluate(() => window.StationActivity.flush());
    const typed = async (d, id) => { await d.page.fill('#etsyOrderNumber', id); await d.page.focus('#etsyOrderNumber'); await d.page.keyboard.press('Enter'); await wait(1000); };
    const send = async (d, text) => {
      await d.page.fill('#britesMsgInput', text);
      await d.page.evaluate(() => document.getElementById('goScreenTwoBtn').click());
      await d.page.waitForFunction(() => document.getElementById('britesMsgInput').value === '', null, { timeout: 6000 }).catch(() => {});
      await wait(200);
    };
    const signIn = async (d, pin) => {
      await d.page.evaluate(() => { const i = document.getElementById('employeeNumberInput'); i.dataset.raw = ''; i.value = ''; });
      await d.page.focus('#employeeNumberInput');
      await d.page.keyboard.type(pin);                                                              // the masked box reads each key, as a person types
      await d.page.evaluate(() => document.getElementById('employeeLoginBtn').click());
      await d.page.waitForFunction(() => window.isEmployeeLoggedIn === true && window.StationActivity.who(), null, { timeout: 10000 });
    };
    const qnote = d => d.page.evaluate(() => { const b = document.getElementById('stationScanQueueNote'); return b && b.style.display !== 'none' ? b.textContent : ''; });

    let lastLive = 0;
    const stations = [1, 2, 3, 4].filter(n => !ONLY.length || ONLY.includes(n));
    const D = {}, P = {}, errs = {};
    for (const n of stations) {
      D[n] = await desktop(n); P[n] = await phone(n);
      ctx.relay = null;
    }
    // the scanner's write reaches Firestore, the desktop of the SAME number hears it (the page's own onSnapshot on Brites_Orders/assembly-scan-N)
    ctx.relay = body => { const m = /^assembly-scan-(\d)$/.exec(String(body.orderNumber || '')); if (m && D[+m[1]]) deliver(D[+m[1]], body).catch(() => {}); };

    for (const n of stations) {
      const d = D[n], ph = P[n], dev = 'assembly-' + n, [A, A2] = PEOPLE[n], O = ORD(n);
      const tag = `assembly-${n}`;

      /* 1 · nobody signed in */
      await check(`${tag}: nobody signed in: a phone scan waits for a sign-in and nothing is recorded or live`, async () => {
        await showQR(ph, O.WAIT);
        await until(async () => /1 phone scan waiting for a sign-in/.test(await qnote(d)), 'the waiting note');
        await wait(600);
        assert.equal((await eventsOf(dev)).length, 0);
        assert.equal((await B.list('Station_Live')).filter(l => l.device === dev).length, 0);
        assert.equal((await sessionsOf(A, dev)).length, 0);
      });

      /* 2 · the PIN box, the server door, the session under the NAME */
      await signIn(d, PINS[n][0]);
      await check(`${tag}: the PIN box signs in through the server door: one open session under the person's NAME, never a PIN`, async () => {
        const ss = await until(async () => { const s = await sessionsOf(A, dev); return s.length === 1 && s; }, 'the session row');
        const s = ss[0];
        assert.equal(s.station, 'assembly'); assert.equal(s.device, dev); assert.equal(s.person, A);
        assert.equal(s.endAt, null); assert(s.startAt > 0 && s.lastSeenAt >= s.startAt);
        assert(/^pc-/.test(s.computerId));
        assert.equal(s.employeeId, '', 'no employee id (it would be the PIN)');
        const door = reqLog.filter(q => /pinLogin/.test(q.raw)); assert(door.length >= 1, 'the sign-in did not use the login door');
        assert(!reqLog.some(q => /employee/i.test(q.url)), 'the whole roster was read');
        const stored = await d.page.evaluate(() => ({ id: localStorage.getItem('employee_id'), name: localStorage.getItem('employee_name') }));
        assert.equal(stored.name, A);
        assert(!allPins().includes(stored.id), 'the PIN itself is kept in the browser as employee_id');
      });
      await check(`${tag}: the phone scan that was waiting loads right after the sign-in and is credited to the person who signed in`, async () => {
        await until(async () => (await eventsOf(dev)).some(e => e.action === 'scan' && e.orderId === O.WAIT), 'the credited scan');
        assert.equal(await d.page.inputValue('#etsyOrderNumber'), O.WAIT);
        const e = (await eventsOf(dev)).filter(x => x.action === 'scan' && x.orderId === O.WAIT);
        assert.equal(e.length, 1); assert.equal(e[0].person, A); assert.equal(e[0].detail, 'phone scan'); assert.equal(e[0].parts, 1);
        assert.equal(await qnote(d), '');
      });

      /* 3 · a typed order: the scan event and the live order */
      await typed(d, O.TWO); await flush(d);
      await check(`${tag}: a typed order is one scan (order id, person, 3 pieces) and the live order is on the board with QR, one picture per piece and the true count`, async () => {
        const sc = await until(async () => { const s = (await eventsOf(dev)).filter(e => e.action === 'scan' && e.orderId === O.TWO); return s.length && s; }, 'the scan event');
        assert.equal(sc.length, 1); assert.equal(sc[0].person, A); assert.equal(sc[0].parts, UNITS(O.TWO)); assert.equal(sc[0].detail, 'typed'); assert.equal(sc[0].station, 'assembly');
        const lv = await until(async () => { const l = await liveDoc(dev, A); return l && l.state === 'working' && l.rid === O.TWO && l; }, 'the live document for the typed order');
        assert.equal(lv.pieceCount, UNITS(O.TWO), 'the card must count pieces, not lines');
        assert.equal(lv.pieces.length, UNITS(O.TWO));
        assert(lv.pieces.every(p => p.sku && p.listingId), 'each piece names its sku and listing so the picture can be found');
        assert(lv.pieces.some(p => p.size === 'L'), 'the size of the line is told so the right vector design is found');
        assert.equal(lv.person, A); assert(lv.scannedAt > Date.now() - 30000);
      });
      await check(`${tag}: the portal's live board shows it at Assembly ${n} for that person: QR text, a picture per piece, the true piece count, the scan time`, async () => {
        const lv = await B.ask({ op: 'live' }); lastLive = Date.now();
        assert.equal(lv.ok, true, JSON.stringify(lv).slice(0, 200));
        const st = lv.stations.find(s => s.key === 'assembly'); assert(st, 'no assembly station');
        assert(namesOf(st).includes(A), 'people: ' + JSON.stringify(st.people));
        const cur = st.current.find(c => c.device === dev && c.person === A); assert(cur, 'no live order for ' + dev + ': ' + JSON.stringify(st.current.map(c => [c.device, c.person])));
        assert.equal(cur.rid, O.TWO); assert.equal(cur.deviceLabel, 'Assembly ' + n); assert.deepEqual(cur.qr, { text: O.TWO });
        assert.equal(cur.pieceCount, UNITS(O.TWO)); assert.equal(cur.pieces.length, UNITS(O.TWO));
        assert(cur.pieces.every(p => /^https:\/\//.test(p.thumbUrl)), 'a piece has no picture: ' + JSON.stringify(cur.pieces.map(p => p.thumbUrl)));
        assert(cur.pieces.some(p => /-L\.svg$/.test(p.thumbUrl)), 'the large size must show its own design');
        assert(/^https:\/\//.test(cur.thumbUrl), 'the order has no picture'); assert(cur.scannedAt > 0 && lv.at - cur.scannedAt < 60000 && lv.at - cur.scannedAt >= -1000);
        assert.equal(cur.customer, 'Test Buyer');
        assert(st.devices.some(x => x.device === dev && x.state === 'working' && x.person === A), 'device row: ' + JSON.stringify(st.devices));
      });

      /* the first Team stamp ("Done") completes the order the person has in hand: one completion, the live order ends */
      await send(d, 'Done'); await flush(d);
      await check(`${tag}: Done is the completion of the typed order (order id, person, 3 pieces, 1 order) and the live order ends at the stamp`, async () => {
        const c = await until(async () => { const l = (await eventsOf(dev)).filter(e => e.action === 'complete' && e.orderId === O.TWO); return l.length && l; }, 'the completion');
        assert.equal(c.length, 1); assert.equal(c[0].person, A); assert.equal(c[0].orders, 1); assert.equal(c[0].parts, UNITS(O.TWO)); assert.match(c[0].detail, /^Team stamp Done/);
        const lv = await until(async () => { const l = await liveDoc(dev, A); return l && l.state === 'idle' && l; }, 'the live order to end at the stamp');
        assert.equal(lv.last.rid, O.TWO);
      });

      /* 4 · this station's own phone */
      await showQR(ph, O.PHONE);
      await check(`${tag}: a scan from this station's own phone (camera, relay write, desktop) is credited to the person signed in, and the live order says phone scan`, async () => {
        await until(async () => (await eventsOf(dev)).some(e => e.action === 'scan' && e.orderId === O.PHONE), 'the phone scan event');
        assert.equal(await d.page.inputValue('#etsyOrderNumber'), O.PHONE);
        const e = (await eventsOf(dev)).filter(x => x.action === 'scan' && x.orderId === O.PHONE);
        assert.equal(e.length, 1); assert.equal(e[0].person, A); assert.equal(e[0].detail, 'phone scan'); assert.equal(e[0].parts, 3); assert.equal(e[0].sku, 'SOLO-' + n);
        const lv = await until(async () => { const l = await liveDoc(dev, A); return l && l.state === 'working' && l.rid === O.PHONE && l; }, 'the live order of the phone scan');
        assert.equal(lv.note, 'phone scan'); assert.equal(lv.pieceCount, 3);
        const relay = await B.doc('Brites_Orders', 'assembly-scan-' + n); assert(relay && relay['Order Number'] === O.PHONE, 'the scanner wrote its own relay document');
        const other = [1, 2, 3, 4].filter(x => x !== n).map(x => reqLog.filter(q => /orderNumField/.test(q.raw) && JSON.parse(q.raw).orderNumber === 'assembly-scan-' + x && JSON.parse(q.raw).orderNumField === O.PHONE)).flat();
        assert.equal(other.length, 0, 'the code went to another station\'s relay');
      });

      /* 5 · the other stamps: "Assembled" completes the phone-scanned order, "QA1" a third order; later stamps for an order are notes, QA 2 is a check */
      await send(d, 'Assembled');
      await typed(d, O.QA2);
      await send(d, 'QA1'); await send(d, 'Done'); await send(d, 'QA 2');
      await flush(d);
      await check(`${tag}: Assembled and QA1 each complete their own order once; Done and QA 2 after the first stamp are notes (QA 2: a check, never a second completion)`, async () => {
        const ev = await until(async () => { const e = await eventsOf(dev); return e.filter(x => x.action === 'complete').length >= 3 && e.filter(x => x.action === 'note' && x.orderId === O.QA2).length >= 2 && e; }, 'three completions');
        const cP = ev.filter(e => e.action === 'complete' && e.orderId === O.PHONE); assert.equal(cP.length, 1);
        assert.equal(cP[0].person, A); assert.equal(cP[0].orders, 1); assert.equal(cP[0].parts, 3); assert.equal(cP[0].sku, 'SOLO-' + n); assert.match(cP[0].detail, /^Team stamp Assembled/);
        const c2 = ev.filter(e => e.action === 'complete' && e.orderId === O.QA2); assert.equal(c2.length, 1);
        assert.equal(c2[0].person, A); assert.equal(c2[0].orders, 1); assert.equal(c2[0].parts, 2); assert.match(c2[0].detail, /^Team stamp QA1/);
        const notes = ev.filter(e => e.action === 'note' && e.orderId === O.QA2).map(e => e.detail).sort();
        assert.deepEqual(notes, ['QA 2 check', 'stamp again: Done']);
        assert.equal(ev.filter(e => e.action === 'complete').length, 3, 'exactly three orders were completed');
        const lv = await until(async () => { const l = await liveDoc(dev, A); return l && l.state === 'idle' && l; }, 'the live order to end at the stamp');
        assert.equal(lv.last.rid, O.QA2);
        // the Assembled seal on the order timeline names the person (the in-memory Firestore has no batched writes, so what is checked is what the page SENT to the timeline door)
        await d.page.evaluate(() => window.OrderTimeline.flush());
        await until(() => reqLog.some(q => /"timeline"/.test(q.raw) && q.raw.includes(O.QA2) && /assembled/.test(q.raw) && q.raw.includes(A)), 'the Assembled seal for the order, naming the person');
      });

      /* 6 · the portal's numbers agree (read once, after the reader's own cache window): 8 pieces and 3 orders completed, 4 orders scanned */
      D[n].expect = { parts: 3 + 3 + 2, completed: 3, scans: 4, touched: 4 };

      /* a reload while signed in goes on with the same session (the login is restored from the browser, the PIN is not needed again) */
      if (n === stations[0]) await check(`${tag}: a reload while signed in restores the login and goes on with the same session (no second session, no PIN asked)`, async () => {
        const before = await sessionsOf(A, dev); assert.equal(before.length, 1);
        await d.page.reload();
        await d.page.waitForFunction(() => window.StationActivity && window.StationSession && window.__ssInit && window.isEmployeeLoggedIn === true, null, { timeout: 25000 });
        await d.page.waitForFunction(k => (window.__fbSnaps[k] || []).length > 0, `Brites_Orders/assembly-scan-${n}`, { timeout: 15000 });
        assert.equal((await d.page.evaluate(() => window.StationActivity.who()) || {}).person, A);
        await wait(800);
        const after = await sessionsOf(A, dev); assert.equal(after.length, 1, 'a second session was started'); assert.equal(after[0].id, before[0].id); assert.equal(after[0].endAt, null);
        assert.equal(await d.page.inputValue('#employeeName'), A);
      });
    }
    if (stations.length) await wait(Math.max(5500, 21000 - (Date.now() - lastLive)));      // the reader keeps today's rollups 20 s for the board and 5 s for the Overview
    for (const n of stations) {
      const [A] = PEOPLE[n], tag = `assembly-${n}`, ex = D[n].expect;
      await check(`${tag}: the Overview, the person view and the live board agree on pieces, orders and scans of the assembly station`, async () => {
        const ov = await B.ask({ op: 'overview' }); assert.equal(ov.ok, true);
        const p = ov.people.find(x => x.name === A); assert(p, 'the person is not in the Overview: ' + ov.people.map(x => x.name));
        const row = p.stations.find(s => s.station === 'assembly'); assert(row, 'no assembly row');
        assert.equal(row.parts, ex.parts, 'Overview pieces'); assert.equal(row.scans, ex.scans, 'Overview scans'); assert.equal(row.completes, ex.completed, 'Overview completions');
        assert.equal(row.orders, ex.touched, 'Overview orders (distinct orders worked)');
        assert.equal(p.status, 'on'); assert(p.nowAt.includes('assembly'));
        const pv = await B.ask({ op: 'person', name: A, range: 'day', compare: false }); assert.equal(pv.ok, true, JSON.stringify(pv).slice(0, 200));
        const pst = pv.stations.find(s => s.station === 'assembly'); assert(pst, 'person view has no assembly');
        assert.equal(pst.parts, ex.parts, 'person view pieces'); assert.equal(pst.orders, row.orders, 'person view orders'); assert.equal(pst.completes, ex.completed);
        assert.equal(pv.kpis.parts.value, ex.parts); assert.equal(pv.kpis.scans.value, ex.scans); assert.equal(pv.kpis.ordersCompleted.value, ex.completed);
        assert.equal(pv.kpis.orders.value, ex.touched, 'person view: distinct orders worked');
      });
    }
    if (stations.length) await check('the live board, the Overview and the sum of the people agree on the assembly station (pieces, scans, orders)', async () => {
      const ov = await B.ask({ op: 'overview' }), lv = await B.ask({ op: 'live' });
      const st = lv.stations.find(s => s.key === 'assembly'), biz = ov.business.stations.find(s => s.station === 'assembly');
      const sum = k => stations.reduce((t, n) => t + D[n].expect[k], 0);
      assert.equal(st.counts.partsToday, sum('parts'), 'board pieces'); assert.equal(biz.parts, sum('parts'), 'Overview pieces');
      assert.equal(st.counts.scansToday, sum('scans'), 'board scans'); assert.equal(biz.scans, sum('scans'), 'Overview scans');
      assert.equal(st.counts.ordersToday, biz.orders, 'board orders = Overview orders'); assert.equal(biz.orders, sum('touched'), 'orders worked');
      for (const n of stations) assert(namesOf(st).includes(PEOPLE[n][0]), PEOPLE[n][0] + ' is not on the board');
    });

    /* 7 · the page's own signOut copes with the new end reasons, then the next person signs in */
    for (const n of stations) {
      const d = D[n], dev = 'assembly-' + n, [A, A2] = PEOPLE[n], O = ORD(n), tag = `assembly-${n}`;
      for (const [reason, re] of [['idle', /10 minutes/i], ['closing', /5:00 pm/i]]) {
        await check(`${tag}: signOut("${reason}") clears the page's own login, shows its own sign-in with a one-line notice, keeps the order on screen`, async () => {
          if (reason === 'closing') { await signIn(d, PINS[n][1]); await typed(d, O.LATE); }
          await d.page.evaluate(() => { window.__toasts.length = 0; window.__modalOpens.length = 0; });
          const orderOnScreen = await d.page.inputValue('#etsyOrderNumber');
          await d.page.evaluate(r => { window.__ssInit.signOut(r, null); }, reason);
          const st = await d.page.evaluate(() => ({ id: localStorage.getItem('employee_id'), nm: localStorage.getItem('employee_name'), li: window.isEmployeeLoggedIn,
            modal: window.__modalOpens.slice(), toasts: window.__toasts.slice(), order: document.getElementById('etsyOrderNumber').value, name: document.getElementById('employeeName').value,
            note: (document.getElementById('signedOutNote') || {}).textContent || '', pin: document.getElementById('employeeNumberInput').value }));
          assert.equal(st.id, null); assert.equal(st.nm, null); assert.equal(st.li, false);
          assert(st.modal.includes('userLoginModal'), 'the sign-in was not shown: ' + JSON.stringify(st.modal));
          const said = st.toasts.concat([st.note]).join(' | ');
          assert(re.test(said), `no notice names the reason: ${said}`);
          assert(!/midnight/i.test(said), 'it says midnight: ' + said);
          assert.equal(st.order, orderOnScreen, 'the order left the screen'); assert.equal(st.name, ''); assert.equal(st.pin, '');
          assert.equal(await d.page.evaluate(() => window.StationSession.who()), null, 'the library still has this person');
        });
      }
      await check(`${tag}: after a signOut the library has nobody; a scan waits; the next person signs in and is credited, the one before is not`, async () => {
        await showQR(P[n], O.AFTER);
        await until(async () => /1 phone scan waiting for a sign-in/.test(await qnote(d)), 'the waiting note after the sign-out');
        assert.equal((await eventsOf(dev)).filter(e => e.orderId === O.AFTER).length, 0);
        await signIn(d, PINS[n][0]);                                                                 // A again
        await until(async () => (await eventsOf(dev)).some(e => e.action === 'scan' && e.orderId === O.AFTER), 'the scan credited after the sign-in');
        const e = (await eventsOf(dev)).filter(x => x.action === 'scan' && x.orderId === O.AFTER); assert.equal(e.length, 1); assert.equal(e[0].person, A);
        const ss = (await sessionsOf(A, dev)); assert(ss.length >= 1);
      });

      /* 8 · Sign Out */
      await d.page.evaluate(() => document.getElementById('signOutBtn').click());
      await check(`${tag}: Sign Out ends the session (signOut), the live order goes idle, a later scan waits`, async () => {
        const s = await until(async () => { const l = (await sessionsOf(A, dev)).filter(x => x.endReason === 'signOut'); return l.length && l; }, 'the session to end with signOut');
        assert(s[0].endAt >= s[0].startAt && s[0].minutes >= 0);
        assert.equal((await sessionsOf(A, dev)).filter(x => x.endAt == null).length, 0, 'a session is still open');
        assert.equal(await d.page.evaluate(() => window.StationSession.who()), null);
        const lv = await until(async () => { const l = await liveDoc(dev, A); return l && l.state === 'idle' && l; }, 'the live order to go idle at sign-out');
        assert(lv);
        const n0 = (await eventsOf(dev)).length;
        await showQR(P[n], O.LATE);
        await wait(600); await flush(d);
        assert.equal((await eventsOf(dev)).length, n0);
      });
    }

    /* once: scanners */
    if (stations.length) {
      const n = stations[0], ph = P[n], O = ORD(n);
      await check('assembly-scan: a relay write that fails is retried and the scan still reaches its desktop; the scan carries no person and no PIN', async () => {
        const d = D[n]; await signIn(d, PINS[n][1]);
        let fails = 2; ctx.hook = rec => (rec.fn === 'firebaseOrders' && /orderNumField/.test(rec.raw) && fails-- > 0 ? { status: 503, json: { error: 'down' } } : null);
        try { await showQR(ph, oid(n, 9)).catch(() => {}); } finally { /* the scanner page decides what a failure means */ }
        const got = await until(async () => (await B.doc('Brites_Orders', 'assembly-scan-' + n) || {})['Order Number'] === oid(n, 9), 'the retried relay write', 20000).catch(() => null);
        ctx.hook = null;
        assert(got, 'a failed relay write was lost');
        for (const q of reqLog.filter(q => /orderNumField/.test(q.raw))) {
          const b = JSON.parse(q.raw); assert.equal(Object.keys(b).sort().join(), 'britesMessages,clientName,employeeName,orderNumField,orderNumber,shippingLabelTimestamps');
          for (const p of allPins()) assert(!q.raw.includes(p), 'a PIN in a scan');
        }
      });
      await check('assembly-1: while signed in the page beats every 5 minutes (the page clock and the shop clock moved on 5 minutes): the session stays open and its last-seen time moves on', async () => {
        const context = await newContext({ width: 1400, height: 900 }, CAPTURE);
        await context.addInitScript(() => { try { localStorage.setItem('access_token', 'test-token'); localStorage.setItem('token_expires_at', String(Math.floor(Date.now() / 1000) + 86400)); } catch (_) {} });
        const page = await context.newPage();
        await page.clock.install({ time: Date.now() });
        await page.goto(`${base}/assembly-1.html`);
        await page.waitForFunction(() => window.StationActivity && window.StationSession && window.__ssInit && window.__fbSnaps, null, { timeout: 25000 });
        const d = { page };
        await signIn(d, BEAT.pin);
        const s0 = await until(async () => { const s = await sessionsOf(BEAT.name, 'assembly-1'); return s.length === 1 && s[0]; }, 'the session row');
        const beats0 = reqLog.filter(q => /"event":"beat"/.test(q.raw) && q.raw.includes(s0.id)).length;
        await B.skew(5 * 60000 + 35000);                                                             // the shop's clock goes on with the page's
        await page.clock.runFor(5 * 60000 + 35000);
        await until(() => reqLog.filter(q => /"event":"beat"/.test(q.raw) && q.raw.includes(s0.id)).length > beats0, 'a beat from the page');
        const s1 = await until(async () => { const s = (await sessionsOf(BEAT.name, 'assembly-1'))[0]; return s.lastSeenAt - s0.startAt >= 5 * 60000 && s; }, 'the last-seen time to move on');
        assert.equal(s1.endAt, null); assert.equal(s1.id, s0.id);
        await context.close();
      });
      await check('no PIN in any request but the login door\'s, in any stored document, or in any toast; the roster is never read whole', async () => {
        for (const q of reqLog) { for (const p of allPins()) { assert(!q.url.includes(p), 'a PIN in a URL'); assert(!q.raw.includes(p) || q.raw === JSON.stringify({ pinLogin: p }), 'a PIN in a body to ' + q.fn); } }
        for (const c of ['Station_Sessions', 'Station_Activity', 'Station_Live', 'Efficiency_Daily', 'Order_Timeline']) {
          const text = JSON.stringify(await B.list(c)); for (const p of allPins()) assert(!text.includes(p), 'a PIN in ' + c);
        }
        for (const n of stations) assert.deepEqual(D[n].errors.filter(m => /Station(Session|Activity|Live)|actLog|actScan|actStamp|actLive/.test(m)), [], 'page errors in assembly-' + n);
      });
    }
    await check('assembly-1..4 and assembly-scan-1..4 are identical apart from their number', async () => {
      const norm = s => s.replace(/assembly-(scan-)?[1-4]/g, 'assembly-$1N').replace(/Assembly_[1-4]/g, 'Assembly_N');
      for (const kind of ['assembly-', 'assembly-scan-']) {
        const one = norm(fs.readFileSync(path.join(root, kind + '1.html'), 'utf8'));
        for (const n of [2, 3, 4]) assert.equal(norm(fs.readFileSync(path.join(root, kind + n + '.html'), 'utf8')) === one, true, `${kind}${n}.html differs`);
      }
    });
  } finally { await browser.close(); server.close(); B.kill(); }
}

main().catch(e => results.push(['setup', e])).finally(() => {
  let bad = 0;
  for (const [name, e] of results) { if (e) { bad++; console.log('FAIL', name, '\n   ', String(e && e.stack || e).split('\n').slice(0, 5).join('\n    ')); } else console.log('ok  ', name); }
  console.log(`${results.length - bad}/${results.length} passed`);
  process.exit(bad ? 1 : 0);
});
