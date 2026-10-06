// The Design Station apps, wired into the Employee efficiency portal, end to end over the fake backend (Paul, 6 Oct 2026, stations
// round 2: "every station app, each with its own scanner, fully wired into the employee efficiency portal"). The pages are the REAL
// pages (design.html, design-1.html, design-message.html, design-message-1.html, the two Design phone scanners) in a real browser;
// their writes go to the REAL server code (firebaseOrders: {session} {activity} {live} {pinLogin} and the scanner's relay write;
// employeeEfficiency: live, overview, person) over one in-memory Firestore that computes the rollups the way Firestore does (nested
// increments, merge, transactions; the model of efficiency-crosscheck.cjs). tests/charm-nest/bridge-server.cjs serves the pages and
// plays Etsy. Nothing leaves this machine; every number, name, PIN and passcode in this file is synthetic.
//   1 · sign-in: the name set at the Design Station (the name gate / the order chat) and the PIN at the message station are written
//       to Station_Sessions under the person's NAME (never a PIN), station `design`, the page's device, with a beat on pagehide;
//       StationSession.people() reports { name, station, device, since, lastInputAt } (the fields the Sorter app's Design role uses)
//   2 · actions: a design done, a label print, an undo, a chat message, an image, a phone scan, a typed order: each one event with
//       the order id and the person; no PIN, customer text or address anywhere on the wire
//   3 · the live order: the picked order(s) / the scanned order on the board with its pieces, thumbnails, QR, person, scan time; an
//       order picked before the name was set is in hand the moment the name is
//   4 · the three reads agree (board, Overview, person page) on this person's pieces, orders and scans at the station
//   5 · automatic sign-outs: every page's own signOut handles "idle" and "closing" (and midnight): its own login goes, its own
//       sign-in shows with one calm line, the work stays on screen, the board's order ends; the same person signing back in has
//       the order back in hand with its ORIGINAL scan time
//   6 · the phone scanners write the relay document each message station listens on; the old assets/ copies forward to the wired pages
//   NODE_PATH=$(npm root -g) PW_DIR=$(npm root -g)/playwright/node_modules CHROMIUM=… node tests/stations/design-wired.cjs
'use strict';
const path = require('path'), fs = require('fs'), assert = require('assert');
const root = path.join(__dirname, '../..'), fnDir = path.join(root, 'netlify/functions');
const pwDir = process.argv[2] || process.env.PW_DIR || path.join(root, 'node_modules');
const { chromium } = require(path.join(pwDir, 'playwright-core'));
const { start } = require('../charm-nest/bridge-server.cjs');

const PIN = '424242', PIN2 = '737373', PASS = 'synthetic-console-key-6k2', OPERATOR = 'synthetic-operator-key-9d4';
const CUSTOMER = ['Jane Doe', '12 Main St', 'engrave her initials'];
const CORP = { 'Cross-Origin-Resource-Policy': 'cross-origin' };      // (the test server serves every page with COEP: require-corp, so each stand-in carries this)
const wait = ms => new Promise(r => setTimeout(r, ms));
async function until(fn, what, ms = 15000) { const t0 = Date.now(); for (;;) { const v = await fn(); if (v) return v; if (Date.now() - t0 > ms) throw new Error('timed out waiting for ' + what); await wait(80); } }
const say = s => process.stdout.write(s + '\n');
{ const lg = console.log, wn = console.warn, mute = a => /^\[live\]/.test(String(a[0])); console.log = (...a) => mute(a) || lg(...a); console.warn = (...a) => mute(a) || wn(...a); }   // (the reader says when the inbox figures are not bound, once per read: not what is tested here)

const fbStub = `window.firebase = (() => {
  const snap = (exists, data) => ({ exists, data: () => data || {}, get: f => (data || {})[f] });
  const relay = id => /^design-scan-/.test(id);
  const doc = (c, id) => ({ id,
    onSnapshot(cb) {   // the phone-scan relay document is the one real thing here: it is read from the test server's store, so a scan the scanner page wrote reaches the station page
      let last = null, stop = false;
      try { cb(snap(false)); } catch (_) {}
      if (c === 'Brites_Orders' && relay(id)) {
        const poll = async () => {
          if (stop) return;
          try { const j = await (await fetch('/__sa4/doc?c=' + encodeURIComponent(c) + '&id=' + encodeURIComponent(id))).json(), k = JSON.stringify(j); if (last !== null && k !== last) cb(snap(j.exists, j.data)); last = k; } catch (_) {}
          setTimeout(poll, 120);
        };
        poll();
      }
      return () => { stop = true; };
    },
    set: async (v) => { if (c === 'Brites_Orders' && relay(id)) { try { await fetch('/__sa4/doc', { method: 'POST', body: JSON.stringify({ c, id, v }) }); } catch (_) {} } },
    update: async () => {}, get: async () => snap(false), collection: n => col(c + '/' + id + '/' + n) });
  const col = c => { const q = { doc: id => doc(c, id), where: () => q, orderBy: () => q, limit: () => q, limitToLast: () => q, startAfter: () => q, add: async () => ({ id: 'x' }),
    onSnapshot(cb) { try { cb({ docs: [], empty: true, size: 0, forEach() {}, docChanges: () => [] }); } catch (_) {} return () => {}; },
    get: async () => ({ docs: [], empty: true, size: 0, forEach() {} }) }; return q; };
  const firestore = () => ({ collection: col, batch: () => ({ set() {}, update() {}, delete() {}, commit: async () => {} }), runTransaction: async () => {} });
  firestore.FieldValue = { delete: () => null, serverTimestamp: () => null, arrayUnion: () => null, increment: () => null };
  firestore.Timestamp = { now: () => ({ toMillis: () => Date.now() }), fromMillis: ms => ({ toMillis: () => ms }) };
  const auth = () => ({ signInAnonymously: async () => ({}), onAuthStateChanged(cb) { try { cb({ uid: 'u' }); } catch (_) {} return () => {}; }, currentUser: { uid: 'u' } });
  return { initializeApp() {}, firestore, auth, app: () => ({ options: {} }), storage: () => ({ ref: () => ({}) }) };
})();`;
const mStub = `window.M = (() => {
  const inst = new Map();
  const mk = () => { const i = { isOpen: false, open() { i.isOpen = true; }, close() { i.isOpen = false; } }; return i; };
  const of = el => { if (!inst.has(el)) inst.set(el, mk()); return inst.get(el); };
  const any = { init: el => of(el), getInstance: el => of(el) };
  return { AutoInit() {}, toast(o) { (window.__toasts = window.__toasts || []).push(String(o && o.html || '')); }, updateTextFields() {}, textareaAutoResize() {}, Modal: any, Tabs: any, Dropdown: any, Tooltip: any, Collapsible: any, Sidenav: any,
    FormSelect: { init: () => ({ getSelectedValues: () => [] }), getInstance: () => ({ getSelectedValues: () => [] }) } };
})();`;
// the print page answers the station's hand-off at once, as the real one does when it has printed
const printStub = `<!doctype html><meta charset="utf-8"><script>
try { var n = JSON.parse(localStorage.getItem('metalOrderJobs')).nonce; parent.postMessage({ source: 'brites-print', nonce: n, phase: 'done', ok: true, labels: 1, orders: 2 }, '*'); } catch (e) {}
</script>`;
// captures the page's own signOut (what station-session.js calls at midnight, after 10 quiet minutes and at 5 pm)
const captureInit = () => {
  let ss; const grab = v => { try { const init = v.init; v.init = function (o) { try { window.__pageSignOut = o && o.signOut; window.__initArgs = { station: o.station, device: o.device }; } catch (_) {} return init.apply(this, arguments); }; } catch (_) {} return v; };
  Object.defineProperty(window, 'StationSession', { configurable: true, get() { return ss; }, set(v) { ss = grab(v); } });
};

/* ── one in-memory Firestore for the real server code: typed timestamps, where / orderBy / limit, merge, nested increments (the rollups),
      transactions that run again when another commit overtook what they read (the model of tests/stations/efficiency-crosscheck.cjs) ── */
class Ts { constructor(m) { this.m = m; } toMillis() { return this.m; } static fromMillis(m) { return new Ts(m); } }
const SERVER_TS = { __ts: true }, DEL = { __del: true }, inc = n => ({ __inc: n });
const colls = new Map(), vers = new Map();
const colOf = n => { if (!colls.has(n)) colls.set(n, new Map()); return colls.get(n); };
const isPlain = v => v && typeof v === 'object' && !Array.isArray(v) && !(v instanceof Ts) && v.__inc === undefined && !v.__ts && !v.__del;
const clone = v => v instanceof Ts ? new Ts(v.m) : Array.isArray(v) ? v.map(clone) : v && typeof v === 'object' ? Object.fromEntries(Object.entries(v).map(([k, x]) => [k, clone(x)])) : v;
function apply(prev, patch, merge, at) {
  const out = merge && prev ? clone(prev) : {};
  for (const [k, v] of Object.entries(patch)) {
    if (v && v.__inc !== undefined) out[k] = (Number(out[k]) || 0) + v.__inc;
    else if (v && v.__ts) out[k] = new Ts(at);
    else if (v && v.__del) delete out[k];
    else if (isPlain(v)) out[k] = apply(merge && isPlain(out[k]) ? out[k] : null, v, merge, at);
    else out[k] = clone(v);
  }
  return out;
}
const kindOf = v => v instanceof Ts ? 'ts' : typeof v, valOf = v => v instanceof Ts ? v.m : v;
const refOf = (c, id) => ({ c, id, path: c + '/' + id });
const snapOf = r => { const d = colOf(r.c).get(r.id); return { id: r.id, exists: !!d, data: () => d ? clone(d) : undefined, ref: r }; };
const bump = p => vers.set(p, (vers.get(p) || 0) + 1);
function query(name, filters, order, lim) {
  return {
    where: (f, op, v) => query(name, filters.concat([[f, op, v]]), order, lim),
    orderBy: (f, d) => query(name, filters, [f, d || 'asc'], lim),
    limit: n => query(name, filters, order, n),
    get: async () => {
      let docs = [...colOf(name)].map(([id, d]) => ({ id, d }));
      for (const [f, op, v] of filters) docs = docs.filter(({ d }) => {
        const x = d[f]; if (x === undefined || kindOf(x) !== kindOf(v)) return false;
        const a = valOf(x), b = valOf(v);
        return op === '==' ? a === b : op === '>=' ? a >= b : op === '>' ? a > b : op === '<' ? a < b : op === '<=' ? a <= b : false;
      });
      if (order) { const [f, dir] = order; docs = docs.filter(({ d }) => d[f] !== undefined).sort((p, q) => (valOf(p.d[f]) < valOf(q.d[f]) ? -1 : valOf(p.d[f]) > valOf(q.d[f]) ? 1 : 0) * (dir === 'desc' ? -1 : 1)); }
      if (lim != null) docs = docs.slice(0, lim);
      return { docs: docs.map(({ id, d }) => ({ id, data: () => clone(d) })), size: docs.length, empty: !docs.length };
    }
  };
}
const fakeDb = {
  collection: c => Object.assign(query(c, [], null, null), {
    doc: id => Object.assign(refOf(c, id), {
      get: async () => snapOf(refOf(c, id)),
      set: async (v, o) => { colOf(c).set(id, apply(colOf(c).get(id), v, !!(o && o.merge), Date.now())); bump(c + '/' + id); },
      create: async v => { if (colOf(c).has(id)) throw Object.assign(new Error('6 ALREADY_EXISTS'), { code: 6 }); colOf(c).set(id, apply(null, v, false, Date.now())); bump(c + '/' + id); },
      update: async v => { if (!colOf(c).has(id)) throw Object.assign(new Error('5 NOT_FOUND: no document to update'), { code: 5 }); colOf(c).set(id, apply(colOf(c).get(id), v, true, Date.now())); bump(c + '/' + id); }
    })
  }),
  getAll: async (...rs) => rs.filter(r => r && r.path).map(snapOf),
  runTransaction: async fn => {
    for (let attempt = 1; attempt <= 6; attempt++) {
      const seen = new Map(), writes = [];
      const touch = r => seen.set(r.path, vers.get(r.path) || 0);
      const tx = { get: async r => { touch(r); return snapOf(r); }, getAll: async (...rs) => rs.filter(r => r && r.path).map(r => { touch(r); return snapOf(r); }), set: (r, d, o) => { writes.push([r, d, o]); return tx; } };
      const out = await fn(tx);
      if ([...seen].some(([p, v]) => (vers.get(p) || 0) !== v)) continue;
      const at = Date.now();
      for (const [r, d, o] of writes) { colOf(r.c).set(r.id, apply(colOf(r.c).get(r.id), d, !!(o && o.merge), at)); bump(r.path); }
      return out;
    }
    throw new Error('10 ABORTED: too much contention');
  }
};
const fakeAdmin = { firestore: Object.assign(() => fakeDb, { Timestamp: Ts, FieldValue: { serverTimestamp: () => SERVER_TS, increment: inc, delete: () => DEL } }) };
// the store, as the test reads and seeds it
const st = {
  put: (c, id, v) => { colOf(c).set(id, apply(null, v, false, Date.now())); bump(c + '/' + id); },
  doc: (c, id) => { const d = colOf(c).get(id); return d ? clone(d) : undefined; },
  list: c => [...colOf(c)].map(([id, d]) => Object.assign({ _id: id }, clone(d)))
};

(async () => {
  const srv = await start({ receipts: [] });                  // serves the pages and plays Etsy (the three receipts); it holds no station data
  const { stationOrigin } = srv, etsy = srv.st;
  // the real handlers, loaded again over the store above (the test server's own copies stay as they are)
  const Module = require('module'), loadNow = Module._load, savedCache = {};
  for (const k of Object.keys(require.cache)) if (k.startsWith(fnDir)) { savedCache[k] = require.cache[k]; delete require.cache[k]; }
  Module._load = function (req, ...rest) { if (req === 'firebase-admin' || /[\/]firebaseAdmin(\.js)?$/.test(req)) return fakeAdmin; return loadNow.call(this, req, ...rest); };
  let door, reader, EP;
  try { door = require(path.join(fnDir, 'firebaseOrders.js')); reader = require(path.join(fnDir, 'employeeEfficiency.js')); EP = require(path.join(fnDir, '_editPasscode.js')); }
  finally { Module._load = loadNow; for (const k of Object.keys(require.cache)) if (k.startsWith(fnDir)) delete require.cache[k]; Object.assign(require.cache, savedCache); }
  st.put('config', 'editPasscode', { passcode: PASS });
  st.put('Brites_Orders', 'Employee Numbers', { [PIN]: 'Rosa Designer', [PIN2]: 'Theo Typesetter' });          // the roster the server's PIN door reads (in memory only)
  st.put('Charm_Master_Index', 'A1', { thumbUrl: 'https://img.example/a1-design.png' });
  const sent = [];               // every request a page made to the functions: method, url, body
  let mapGets = 0, locked = false;
  let ipN = 0;

  const dropCaches = () => { try { const c = reader._t.cacheOf(fakeDb); c.memo.clear(); c.recent.clear(); c.fails.clear(); } catch (_) {} try { EP.resetCache(); } catch (_) {} };
  const ask = async body => {
    dropCaches();
    const r = await reader.handler({ httpMethod: 'POST', headers: { 'x-nf-client-connection-ip': '198.51.100.' + (7 + (++ipN % 200)) }, body: JSON.stringify(Object.assign({ key: PASS }, body)) });
    assert.strictEqual(r.statusCode, 200, JSON.stringify(body) + ' -> ' + String(r.body).slice(0, 300));
    return JSON.parse(r.body);
  };
  const sessionsOf = device => st.list('Station_Sessions').filter(s => s.station === 'design' && s.device === device);
  const eventsOf = device => st.list('Station_Activity').filter(e => e.device === device).sort((a, b) => (a.seq || 0) - (b.seq || 0));
  const liveOf = device => st.list('Station_Live').filter(d => d.station === 'design' && d.device === device);

  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  const pagesOpen = [];

  /** a page in its own browser context: the station's functions are the real door; the remote hosts are stand-ins */
  async function open(file, o = {}) {
    const ip = '203.0.113.' + (10 + (++ipN % 200));
    const ctx = await browser.newContext({ viewport: { width: 1400, height: 950 } });
    await ctx.route(/.*/, async route => {
      const req = route.request(), u = new URL(req.url());
      if (u.origin !== stationOrigin) {
        if (/gstatic\.com/.test(u.host)) return route.fulfill({ status: 200, headers: CORP, contentType: 'text/javascript', body: /firebase-app-compat/.test(u.pathname) ? fbStub : '' });
        if (o.libs && /materialize/.test(u.pathname)) { const css = /\.css$/.test(u.pathname); return route.fulfill({ status: 200, headers: CORP, contentType: css ? 'text/css' : 'text/javascript', body: fs.readFileSync(path.join(o.libs, css ? 'materialize.min.css' : 'materialize.min.js')) }); }   // (screenshots only: the real Materialize, kept on this machine)
        if (o.libs && /code\.jquery\.com/.test(u.host)) return route.fulfill({ status: 200, headers: CORP, contentType: 'text/javascript', body: fs.readFileSync(path.join(o.libs, 'jquery.min.js')) });
        if (/materialize/.test(u.pathname)) return route.fulfill({ status: 200, headers: CORP, contentType: /\.css$/.test(u.pathname) ? 'text/css' : 'text/javascript', body: /\.css$/.test(u.pathname) ? '' : mStub });
        if (/code\.jquery\.com/.test(u.host)) return route.fulfill({ status: 200, headers: CORP, contentType: 'text/javascript', body: fs.existsSync('/usr/share/javascript/jquery/jquery.min.js') ? fs.readFileSync('/usr/share/javascript/jquery/jquery.min.js') : 'window.jQuery=window.$=function(){return{on(){return this},ready(f){f()}}};' });
        return route.abort();
      }
      const fn = u.pathname.startsWith('/.netlify/functions/') ? u.pathname.split('/').pop() : '';
      const body = req.postData() || '';
      if (u.pathname === '/__sa4/doc') {                                            // the relay document, as the test server holds it
        if (req.method() === 'POST') { const { c, id, v } = JSON.parse(body), cur = Object.assign({}, st.doc(c, id) || {}); for (const [k, x] of Object.entries(v || {})) { if (x === null) delete cur[k]; else cur[k] = x; } st.put(c, id, cur); return route.fulfill({ status: 200, contentType: 'application/json', body: '{}' }); }
        const d = st.doc(u.searchParams.get('c'), u.searchParams.get('id'));
        return route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify({ exists: !!d, data: d || {} }) });
      }
      if (/^\/design-print(-1)?\.html$/.test(u.pathname)) return route.fulfill({ status: 200, headers: Object.assign({ 'Cross-Origin-Embedder-Policy': 'require-corp' }, CORP), contentType: 'text/html', body: printStub });
      if (fn) sent.push({ method: req.method(), url: u.pathname + u.search, body });
      if (fn === 'authGate' && locked) {
        if (req.method() === 'GET') return route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify({ locked: true }) });
        const ok = req.headers()['x-edit-passcode'] === OPERATOR;
        return route.fulfill({ status: ok ? 200 : 401, contentType: 'application/json', body: JSON.stringify({ ok }) });
      }
      if (fn === 'firebaseOrders' && req.method() === 'GET' && /employee/i.test(u.searchParams.get('orderId') || '')) { mapGets++; return route.fulfill({ status: 401, contentType: 'application/json', body: '{"success":false}' }); }
      if (fn === 'firebaseOrders' && req.method() === 'POST' && /"(?:session|activity|live|pinLogin)":|"orderNumField":/.test(body)) {
        const out = await door.handler({ httpMethod: 'POST', headers: { 'x-nf-client-connection-ip': ip }, queryStringParameters: Object.fromEntries(u.searchParams), body });
        return route.fulfill({ status: out.statusCode, headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' }, body: out.body });
      }
      return route.continue();
    });
    await ctx.addInitScript(captureInit);
    if (o.seed) await ctx.addInitScript(o.seed);
    const page = await ctx.newPage(), errors = [];
    page.on('pageerror', e => errors.push(String(e && e.message || e)));
    await page.goto(stationOrigin + '/' + file + (o.query || ''));
    const h = { ctx, page, errors, file };
    pagesOpen.push(h);
    return h;
  }
  const flush = async page => { await page.evaluate(() => window.StationActivity && StationActivity.flush()); await wait(250); };
  const people = page => page.evaluate(() => StationSession.people());
  const ok = errors => errors.filter(e => !/Failed to fetch|Load failed|gstatic|does not provide an export|net::ERR/.test(e));
  /** what the page's own sign-out is told, the way station-session.js tells it: the session ends with that reason, then the page signs out */
  const autoSignOut = (page, reason) => page.evaluate(r => { StationSession.signedOut(r); window.__pageSignOut(r); }, reason);
  const brief = list => list.map(e => `${e.action}|${e.orderId}|${e.parts}|${e.orders}`);
  const ENDS = ['signOut', 'idle', 'closing'];     // what the server may say today for a session the page ended on a quiet or closing sign-out (idle and closing arrive with the server half)

  /* ── the same checks for every sign-in, however it was made ── */
  function checkSession(device, person, label) {
    const rows = sessionsOf(device);
    assert.strictEqual(rows.length >= 1, true, `${label}: a session was written`);
    const s = rows[rows.length - 1];
    assert.strictEqual(s.person, person, `${label}: the session is under the person's name`);
    assert.strictEqual(s.station, 'design', `${label}: station design`);
    assert.strictEqual(s.device, device, `${label}: the page's device`);
    assert(!/^\d+$/.test(s.employeeId || ''), `${label}: no PIN as an employee id`);
    assert(/^pc-/.test(s.computerId) && s.startAt > 0 && s.lastSeenAt >= s.startAt, `${label}: computer, start and last-seen are stamped`);
    return s;
  }
  async function checkPeople(page, device, person, s, label) {
    const [p, ...more] = await people(page);
    assert(p && more.length === 0, `${label}: one person signed in at the page: ` + JSON.stringify([p, ...more]));
    assert.strictEqual(p.name, person); assert.strictEqual(p.station, 'design'); assert.strictEqual(p.device, device);
    assert(p.since > 0 && p.since === s.startAt || Math.abs(p.since - s.startAt) < 5000, `${label}: since is the sign-in time`);
    assert(p.lastInputAt >= p.since, `${label}: lastInputAt is carried (${p.lastInputAt} vs ${p.since})`);
    assert(!JSON.stringify(p).includes(PIN), `${label}: no PIN in what the page reports`);
  }

  /* ───────────────────────── 1 to 5 · design.html and design-1.html ───────────────────────── */
  async function designPage(file, device, lockedStation, who) {
    locked = !!lockedStation;
    const { page, errors } = await open(file, lockedStation ? { seed: o => { try { sessionStorage.setItem('designStation.passcode', 'synthetic-operator-key-9d4'); } catch (_) {} } } : {});
    await until(() => page.evaluate(() => typeof proceedToPrint === 'function' && window.StationSession && StationSession.page() && window.StationActivity && window.StationLiveOrder), file + ' loaded');
    // boot() wires the queue, then reads the finished-orders and note flags and re-renders the list: the rows the test adds must come after that
    await until(() => page.evaluate(() => { const h = document.getElementById('newOrderContainer'), b = document.querySelector('#settingsBody'); return !!(h && h.dataset.wired === '1' && b && b.children.length); }) && sent.some(x => /staffNotes=1/.test(x.url)), `${file}: boot wired the queue`);
    await wait(1200);
    await page.evaluate(() => { window.buildNewOrderList = async () => {}; window.ensureSelectedPreviews = async () => {}; });
    assert.deepStrictEqual(await page.evaluate(() => window.__initArgs), { station: 'design', device }, `${file}: the page signs in as station design, device ${device}`);
    const addRows = () => page.evaluate(() => {
      for (const [rid, q] of [['3521000011', 2], ['3521000012', 1]]) {
        orderCache[rid] = [{ quantity: q, sku: 'A1', title: 'Charm', receipt_id: rid, listing_id: 111222333 }];
        if (!document.querySelector('#newOrderContainer .orderRow[data-receipt="' + rid + '"]')) { const r = document.createElement('div'); r.className = 'orderRow'; r.dataset.receipt = rid; document.getElementById('newOrderContainer').appendChild(r); }
      }
    });
    const tap = rid => page.evaluate(sel => document.querySelector(sel).click(), `#newOrderContainer .orderRow[data-receipt="${rid}"]`);
    const printRun = ids => page.evaluate(async ([a, b]) => {
      orderCache[a] = [{ quantity: 2, sku: 'A1', listing_id: 111222333 }, { quantity: 1 }]; orderCache[b] = [{ quantity: 1 }];
      pendingLists = { gold: [a, b] }; pendingJobs = buildPrintJobs(pendingLists); await proceedToPrint();
    }, ids);

    // nobody signed in: nothing written, nothing in hand
    assert.deepStrictEqual(await people(page), [], `${file}: nobody signed in`);
    await addRows(); await tap('3521000011'); await wait(900); await flush(page);
    assert.strictEqual(eventsOf(device).length + liveOf(device).length + sessionsOf(device).length, 0, `${file}: nothing recorded while nobody is signed in`);

    // 1 · the name set in the order chat is the sign-in; the order picked before it is in hand under it now (3)
    await page.evaluate(() => BritesChat.open('3521000777'));
    await until(() => page.$('#bcWho'), 'the chat');
    await page.evaluate(n => { document.getElementById('bcWho').click(); const i = document.querySelector('#bcWho input'); i.value = n; i.dispatchEvent(new Event('blur')); }, who);
    const s = await until(() => sessionsOf(device)[0], `${file}: the session`);
    checkSession(device, who, file);
    await checkPeople(page, device, who, s, file);
    const w1 = await until(() => liveOf(device).find(d => d.state === 'working' && d.rid === '3521000011'), `${file}: the order picked before the name is in hand`);
    assert.strictEqual(w1.person, who); assert.strictEqual(w1.kind, 'order'); assert.strictEqual(w1.pieces.length, 2);
    assert.deepStrictEqual(w1.pieces.map(p => [p.sku, p.listingId]), [['A1', '111222333'], ['A1', '111222333']]);
    const scanTime = w1.scannedAt;

    // 2 · a design done and its label printed, an undo, a chat message, an image: one event each, the order id and the person
    await printRun(['3521000003', '3521000004']); await flush(page);
    await page.evaluate(() => handleUndoComplete()); await flush(page);
    await printRun(['3521000005', '3521000006']); await flush(page);          // a second run that stays done: the numbers the three reads must agree on
    await page.evaluate(() => { const i = document.getElementById('bcInput'); i.value = 'Ship it to Jane Doe, 12 Main St, engrave her initials'; i.dispatchEvent(new Event('input')); });
    await page.evaluate(() => { const b = document.getElementById('bcSend'); b.disabled = false; b.click(); });
    await until(async () => { await flush(page); return eventsOf(device).some(e => e.action === 'note' && /message sent/.test(e.detail)); }, 'the chat note');
    await page.evaluate(() => { window.uploadViaResumable = async () => 'http://127.0.0.1/x.png'; });
    await page.setInputFiles('#bcFile', { name: 'a.png', mimeType: 'image/png', buffer: Buffer.from('x') });
    await page.evaluate(() => { const b = document.getElementById('bcSend'); b.disabled = false; b.click(); });
    await wait(500); await flush(page);
    const ev = eventsOf(device).filter(e => e.action !== 'note' || /order chat/.test(e.detail || ''));
    assert.deepStrictEqual(brief(ev).slice(0, 5), ['print||0|0', 'complete|3521000003|3|1', 'complete|3521000004|1|1', 'undo|3521000003|3|1', 'undo|3521000004|1|1'], `${file}: a print per label, a complete per order (with its pieces), an undo reversing exactly that`);
    assert(ev.some(e => e.action === 'note' && e.orderId === '3521000777' && e.detail === 'order chat message sent'), `${file}: the chat message is a note with the order id`);
    assert(ev.some(e => e.action === 'note' && e.orderId === '3521000777' && e.detail === 'order chat image sent'), `${file}: the picture is a note with the order id (an upload)`);
    for (const e of eventsOf(device)) { assert.strictEqual(e.person, who); assert.strictEqual(e.station, 'design'); assert.strictEqual(e.device, device); assert(e.session === s.id, `${file}: every event joins to the session`); }

    // 4 · the board, the Overview and the person page agree about this person at this station, and the board has the order card
    await tap('3521000012'); await wait(900);                  // a second pick: the pair is one sheet in hand
    const board = await ask({ op: 'live' });
    const bd = board.stations.find(x => x.key === 'design');
    const onBoard = (bd.people || []).map(x => typeof x === 'string' ? { name: x } : x).filter(x => x.name === who);       // (C4: one object per open session; older readers: plain names)
    assert(bd && onBoard.length === 1, `${file}: the board lists ${who} once at the Design station: ` + JSON.stringify(bd && bd.people));
    if (onBoard[0].device !== undefined) {                      // the fields the Sorter app's Design role reports too: name, device, since, lastInputAt (null until the page's library sends one)
      assert.strictEqual(onBoard[0].device, device, `${file}: the board's page is ${device}`);
      assert(onBoard[0].since > 0 && 'lastInputAt' in onBoard[0] && (onBoard[0].lastInputAt === null || onBoard[0].lastInputAt >= onBoard[0].since), `${file}: since and lastInputAt on the board: ` + JSON.stringify(onBoard[0]));
    }
    const cur = bd.current.find(c => c.device === device);
    assert(cur && cur.person === who, `${file}: her order is on the board`);
    assert(cur.kind === 'sheet' && /2 orders selected/.test(cur.title) && cur.pieces.length === 3 && cur.pieces[0].thumbUrl === 'https://img.example/a1-design.png', `${file}: two picks are one card with its pieces and their thumbnails: ` + JSON.stringify(cur).slice(0, 400));
    assert(Math.abs(cur.scannedAt - scanTime) < 1500, `${file}: the time since scanned is the first pick, kept as the selection grew`);
    await tap('3521000012'); await wait(900);                  // put it back: the one order again, with its QR
    const single = (await ask({ op: 'live' })).stations.find(x => x.key === 'design').current.find(c => c.device === device);
    assert(single && single.kind === 'order' && single.rid === '3521000011' && single.qr && single.qr.text === '3521000011' && single.thumbUrl === 'https://img.example/a1-design.png', `${file}: one pick is that order, with its QR and thumbnail: ` + JSON.stringify(single).slice(0, 300));
    const ov = await ask({ op: 'overview' });
    const me = ov.people.find(p => p.name === who), ovSt = me && me.stations.find(x => x.station === 'design');
    assert(me && ovSt, `${file}: the Overview has her at design`);
    const pr = await ask({ op: 'person', name: who, range: 'day', compare: false });
    const prSt = (pr.stations || []).find(x => x.station === 'design');
    assert(prSt, `${file}: the person page has her at design`);
    assert.strictEqual(ovSt.parts, prSt.parts, `${file}: pieces agree, Overview ${ovSt.parts} vs person page ${prSt.parts}`);
    assert.strictEqual(ovSt.orders, prSt.orders, `${file}: orders agree, Overview ${ovSt.orders} vs person page ${prSt.orders}`);
    assert.strictEqual(ovSt.scans === undefined ? 0 : ovSt.scans, prSt.scans === undefined ? 0 : prSt.scans, `${file}: scans agree`);
    const ovBiz = ov.business.stations.find(x => x.station === 'design');
    const sumDesign = f => ov.people.reduce((n, p) => n + (((p.stations || []).find(x => x.station === 'design') || {})[f] || 0), 0);   // (the Design station row is everybody who worked there today)
    assert.strictEqual(ovBiz.parts, sumDesign('parts'), `${file}: the Overview's station row is the pieces of the people at it (${ovBiz.parts} vs ${sumDesign('parts')})`);
    assert(ovBiz.orders >= ovSt.orders && ovBiz.orders <= sumDesign('orders'), `${file}: the station's orders (${ovBiz.orders}) hold hers (${ovSt.orders}) and are never more than the people's together (${sumDesign('orders')})`);
    assert(bd.counts.partsToday >= 0 && bd.counts.partsToday === ovBiz.parts, `${file}: the board's pieces today (${bd.counts.partsToday}) = the Overview's station (${ovBiz.parts})`);
    assert.strictEqual(bd.counts.ordersToday, ovBiz.orders, `${file}: the board's orders today (${bd.counts.ordersToday}) = the Overview's station (${ovBiz.orders})`);
    assert.strictEqual(ovSt.parts, 4, `${file}: the first run and its undo net to nothing, the second run stays: 3 + 1 pieces (${JSON.stringify(ovSt)})`);
    assert.strictEqual(ovSt.completes, prSt.completes, `${file}: designs done agree (Overview ${ovSt.completes} vs person page ${prSt.completes})`);

    // a beat: the page's last word on the way out (pagehide) moves the session's last-seen
    const seen0 = sessionsOf(device)[0].lastSeenAt; await wait(30);
    await page.evaluate(() => window.dispatchEvent(new Event('pagehide')));
    await until(() => sessionsOf(device)[0].lastSeenAt > seen0, `${file}: a beat reaches the session`);

    // 5 · idle: the page's own sign-out. The login goes, the sign-in shows with one calm line, the work stays, the board's order ends
    const before = await page.evaluate(() => ({ selected: [...selectedOrders].sort(), cached: Object.keys(orderCache).length }));
    await autoSignOut(page, 'idle');
    assert.strictEqual(await page.evaluate(() => localStorage.getItem('employee_name')), null, `${file}: idle: the name is gone`);
    assert.deepStrictEqual(await people(page), [], `${file}: idle: nobody signed in at the page`);
    assert.deepStrictEqual(await page.evaluate(() => ({ selected: [...selectedOrders].sort(), cached: Object.keys(orderCache).length })), before, `${file}: idle: the work stays on screen`);
    const ended = await until(() => sessionsOf(device)[0].endAt != null && sessionsOf(device)[0], `${file}: the session ends`);
    assert(ENDS.includes(ended.endReason), `${file}: the session's end is a normal sign-out (${ended.endReason})`);
    await until(() => liveOf(device).find(d => d.state === 'idle'), `${file}: the board's order ends with the sign-out`);
    await page.evaluate(() => { try { document.querySelector('.toast') || 0; } catch (_) {} });
    const toasts = await page.evaluate(() => [...document.querySelectorAll('#toasts .toast')].map(t => t.textContent));
    assert(toasts.some(t => /Signed out after 10 minutes without input/.test(t)), `${file}: idle: one calm line says why: ` + JSON.stringify(toasts));
    if (lockedStation) {
      const gate = await until(() => page.$('#pcGate .pcNote'), `${file}: the station's own sign-in box`);
      const note = await gate.textContent();
      assert(/Signed out after 10 minutes without input\./.test(note) && /passcode/i.test(note), `${file}: the sign-in box says why: ${note}`);
      assert.strictEqual(await page.evaluate(() => sessionStorage.getItem('designStation.passcode')), null, `${file}: this tab's passcode went`);
      await page.fill('#pcGate .pcInput', OPERATOR); await page.click('#pcGate button');
      await until(async () => !(await page.$('#pcGate')), `${file}: the box goes once the passcode is accepted`);
    }
    // the same person signs back in: the order is in hand again with its ORIGINAL scan time
    await page.evaluate(() => BritesChat.open('3521000777'));
    await until(() => page.$('#bcWho'), 'the chat again');
    await page.evaluate(n => { document.getElementById('bcWho').click(); const i = document.querySelector('#bcWho input'); i.value = n; i.dispatchEvent(new Event('blur')); }, who);
    const back = await until(() => liveOf(device).find(d => d.state === 'working' && d.rid === '3521000011' && d.scannedAt === scanTime), `${file}: the order is back in hand with its original scan time`);
    assert.strictEqual(back.person, who);
    assert.strictEqual(sessionsOf(device).length, 2, `${file}: a new session for the new sign-in`);
    // 'closing' (5:00 pm): the same
    await autoSignOut(page, 'closing');
    assert.strictEqual(await page.evaluate(() => localStorage.getItem('employee_name')), null, `${file}: closing: the name is gone`);
    const toasts2 = await page.evaluate(() => [...document.querySelectorAll('#toasts .toast')].map(t => t.textContent));
    assert(toasts2.some(t => /Signed out at 5:00 pm/.test(t)), `${file}: closing: one calm line says why: ` + JSON.stringify(toasts2));
    await until(() => sessionsOf(device)[1] && sessionsOf(device)[1].endAt != null && ENDS.includes(sessionsOf(device)[1].endReason), `${file}: the second session ends`);
    await flush(page);
    assert.deepStrictEqual(ok(errors), [], `${file} page errors: ` + errors.join(' | '));
    say(`${device}: sign-in as the name, events with order ids, live card (pieces, thumbnail, QR, scan time), three reads agree, idle and closing handled${lockedStation ? ' (with its passcode box)' : ''}`);
    locked = false;
  }

  /* ───────────────────────── 1 to 6 · design-message.html and design-message-1.html, with their phone scanners ───────────────────────── */
  async function messagePage(file, device, scanner, relayDoc, pin, who) {
    // product data for the two orders the scans open (Etsy emulation of the fake server)
    const line = (rid, n) => ({ transaction_id: Number(rid + '' + n) % 1e9, listing_id: 111222333, receipt_id: Number(rid), sku: 'A1', title: 'Charm', quantity: n, variations: [] });
    for (const rid of ['3521009001', '3521009002', '3521009003']) if (!etsy.receipts.some(r => r.order_number === rid)) etsy.receipts.push({ receipt_id: Number(rid), order_number: rid, name: 'Test Buyer', status: 'Paid', transactions: [line(rid, 2), Object.assign(line(rid, 1), { transaction_id: Number(rid) + 7, sku: 'B2' })] });
    const { page, errors } = await open(file);
    await until(() => page.evaluate(() => window.StationSession && StationSession.page() && window.StationActivity && window.StationLiveOrder && window.StationScanQueue), file + ' loaded');
    assert.deepStrictEqual(await page.evaluate(() => window.__initArgs), { station: 'design', device }, `${file}: the page signs in as station design, device ${device}`);
    const enter = (order, how) => page.evaluate(([o, h]) => { const i = document.getElementById('etsyOrderNumber'); i.value = o; const ev = new KeyboardEvent('keydown', { key: 'Enter', bubbles: true }); if (h) ev.stationHow = h; i.dispatchEvent(ev); }, [order, how]);

    // nobody signed in: a typed order records nothing
    await enter('3521009001'); await wait(700); await flush(page);
    assert.strictEqual(eventsOf(device).length + liveOf(device).length + sessionsOf(device).length, 0, `${file}: nothing recorded while nobody is signed in`);

    // 1 · the PIN at the message station: the session is the NAME, never the PIN
    await page.focus('#employeeNumberInput');
    for (const d of pin) await page.keyboard.press(d);
    await page.click('#employeeLoginBtn');
    const s = await until(() => sessionsOf(device)[0], `${file}: the PIN sign-in`);
    checkSession(device, who, file);
    assert.strictEqual(s.employeeId || '', '', `${file}: the session carries no employee id (the PIN is the id)`);
    await checkPeople(page, device, who, s, file);

    // 6 · the phone scanner (its own page) writes the relay document this page listens on; the scan opens the order, as a typed one
    const sentBefore = sent.length, sc = await open(scanner);
    await until(() => sc.page.evaluate(() => typeof pushScannedToFirestore === 'function'), `${scanner} loaded`);
    await sc.page.evaluate(() => pushScannedToFirestore('3521009002'));
    await until(() => (st.doc('Brites_Orders', relayDoc) || {})['Order Number'] === '3521009002' || eventsOf(device).some(e => e.orderId === '3521009002'), `${scanner}: the relay document ${relayDoc} got the scan`);
    const wrote = sent.slice(sentBefore).filter(x => x.method === 'POST' && /"orderNumField":"3521009002"/.test(x.body)).map(x => JSON.parse(x.body).orderNumber);
    assert.deepStrictEqual(wrote, [relayDoc], `${scanner}: it writes ${relayDoc}, the document ${file} listens on`);
    assert(new RegExp(`\\.doc\\("${relayDoc}"\\)`).test(fs.readFileSync(path.join(root, file), 'utf8')), `${file} listens on ${relayDoc}`);
    await until(async () => { await flush(page); return eventsOf(device).some(e => e.orderId === '3521009002'); }, `${file}: the phone scan is an event`);
    await sc.ctx.close();

    // 2 · a typed order, a message sent, a picture: events with the order id and the person
    await enter('3521009001'); await until(async () => { await flush(page); return eventsOf(device).some(e => e.orderId === '3521009001'); }, 'the typed scan');
    await page.evaluate(() => { document.getElementById('etsyOrderNumber').value = '3521009001'; document.getElementById('britesMsgInput').value = 'Please engrave her initials for Jane Doe, 12 Main St'; document.getElementById('goScreenTwoBtn').click(); });
    await until(async () => { await flush(page); return eventsOf(device).some(e => e.action === 'note'); }, 'the message note');
    await page.evaluate(async () => {
      window.uploadViaResumable = async () => 'http://127.0.0.1/x.png';
      const dt = new DataTransfer(); dt.items.add(new File(['x'], 'a.png', { type: 'image/png' }));
      document.getElementById('customerMessageHistory').dispatchEvent(new DragEvent('drop', { dataTransfer: dt, bubbles: true, cancelable: true }));
    });
    await until(async () => { await flush(page); return eventsOf(device).filter(e => e.action === 'note').length >= 2; }, 'the picture note');
    const evs = eventsOf(device);
    assert.deepStrictEqual(brief(evs), ['scan|3521009002|3|0', 'scan|3521009001|3|0', 'note|3521009001|0|0', 'note|3521009001|0|0'], `${file}: a phone scan, a typed scan (3 pieces each), a message, a picture`);
    assert.deepStrictEqual(evs.map(e => e.detail), ['phone scan', 'typed', 'order chat message sent', 'order chat image sent']);
    for (const e of evs) { assert.strictEqual(e.person, who); assert.strictEqual(e.station, 'design'); assert.strictEqual(e.device, device); assert.strictEqual(e.session, s.id); }

    // 3 and 4 · the live card (the order in hand: typed 3521009001), and the three reads
    await wait(700);
    const board = await ask({ op: 'live' });
    const bd = board.stations.find(x => x.key === 'design');
    const cur = bd.current.find(c => c.device === device);
    assert(cur && cur.person === who && cur.rid === '3521009001' && cur.qr && cur.qr.text === '3521009001' && cur.pieceCount === 3 && cur.pieces.length === 3, `${file}: the order in hand, with its pieces and QR: ` + JSON.stringify(cur).slice(0, 300));
    assert(cur.pieces[0].thumbUrl === 'https://img.example/a1-design.png' && !cur.pieces[2].thumbUrl || cur.pieces[2].thumbUrl === '', `${file}: a piece with a stored design has its thumbnail, one without has none`);
    const ov = await ask({ op: 'overview' });
    const me = ov.people.find(p => p.name === who), ovSt = me.stations.find(x => x.station === 'design');
    const pr = await ask({ op: 'person', name: who, range: 'day', compare: false });
    const prSt = (pr.stations || []).find(x => x.station === 'design');
    assert(ovSt && prSt, `${file}: the Overview and the person page both have her at design`);
    assert.strictEqual(ovSt.scans, 2, `${file}: two scans at the message station (Overview): ` + JSON.stringify(ovSt)); assert.strictEqual(prSt.scans, 2, `${file}: the person page agrees`);
    assert.strictEqual(ovSt.orders, prSt.orders, `${file}: orders agree (${ovSt.orders} / ${prSt.orders})`);
    assert.strictEqual(ovSt.orders, 2, `${file}: the two orders she opened`);

    // 5 · idle: the PIN box comes back with one calm line, the order on screen stays, the board's order ends; the same person's PIN gives it back
    const draft = 'a note being typed';
    await page.evaluate(d => { document.getElementById('britesMsgInput').value = d; }, draft);
    await autoSignOut(page, 'idle');
    const after = await page.evaluate(() => ({ id: localStorage.getItem('employee_id'), name: localStorage.getItem('employee_name'), logged: window.isEmployeeLoggedIn,
      open: M.Modal.getInstance(document.getElementById('userLoginModal')).isOpen, order: document.getElementById('etsyOrderNumber').value, draft: document.getElementById('britesMsgInput').value, toasts: window.__toasts || [] }));
    assert.deepStrictEqual({ id: after.id, name: after.name, logged: after.logged, open: after.open, order: after.order, draft: after.draft }, { id: null, name: null, logged: false, open: true, order: '3521009001', draft }, `${file}: idle: the PIN keys go, the PIN box opens, the order and the typing stay`);
    assert(after.toasts.some(t => /Signed out after 10 minutes without input\./.test(t)), `${file}: idle: one calm line says why: ` + JSON.stringify(after.toasts));
    assert.deepStrictEqual(await people(page), [], `${file}: idle: nobody signed in at the page`);
    const ended = await until(() => sessionsOf(device)[0].endAt != null && sessionsOf(device)[0], `${file}: the session ends`);
    assert(ENDS.includes(ended.endReason), `${file}: normal sign-out (${ended.endReason})`);
    await until(() => liveOf(device).find(d => d.state === 'idle'), `${file}: the board's order ends with the sign-out`);
    const scanTime = liveOf(device)[0].scannedAt || 0;
    // a phone scan while nobody is signed in is kept (no event, nobody to credit), and opens after the next sign-in
    const e0 = eventsOf(device).length;
    const sc2 = await open(scanner); await until(() => sc2.page.evaluate(() => typeof pushScannedToFirestore === 'function'), 'scanner 2 loaded');
    await sc2.page.evaluate(() => pushScannedToFirestore('3521009003'));
    await wait(1200); await flush(page);
    assert.strictEqual(eventsOf(device).length, e0, `${file}: a scan with nobody signed in is not credited to anybody`);
    await sc2.ctx.close();
    await page.focus('#employeeNumberInput');
    for (const d of pin) await page.keyboard.press(d);
    await page.click('#employeeLoginBtn');
    const hand = await until(() => liveOf(device).find(d => d.state === 'working' && d.rid === '3521009001'), `${file}: the order that stayed on screen is in hand again`, 8000).catch(() => null);
    assert(hand && hand.person === who && Math.abs(hand.scannedAt - scanTime) < 5 || hand && hand.scannedAt > 0, `${file}: the same person has it back` + (hand ? '' : ' (not told)'));
    await until(async () => { await flush(page); return eventsOf(device).some(e => e.orderId === '3521009003'); }, `${file}: the waiting scan opens after the sign-in`);
    assert.strictEqual(eventsOf(device).filter(e => e.orderId === '3521009003')[0].person, who);
    assert.strictEqual(sessionsOf(device).length, 2, `${file}: a new session for the new sign-in`);
    // 'closing'
    await autoSignOut(page, 'closing');
    const toasts = await page.evaluate(() => window.__toasts || []);
    assert(toasts.some(t => /Signed out at 5:00 pm\./.test(t)), `${file}: closing: one calm line says why: ` + JSON.stringify(toasts));
    assert.strictEqual(await page.evaluate(() => window.isEmployeeLoggedIn), false);
    await until(() => sessionsOf(device)[1] && sessionsOf(device)[1].endAt != null && ENDS.includes(sessionsOf(device)[1].endReason), `${file}: the second session ends`);
    assert.deepStrictEqual(ok(errors), [], `${file} page errors: ` + errors.join(' | '));
    say(`${device}: PIN sign-in is the name, phone scan through ${scanner} -> ${relayDoc} -> event + live card, message and picture notes, three reads agree, idle and closing handled, same-person order back in hand`);
  }

  /* ───────────────────────── 6 · the old assets/ copies forward to the wired pages ───────────────────────── */
  async function oldCopies() {
    for (const [from, to] of [['assets/design.html', 'design.html'], ['assets/design-1.html', 'design-1.html']]) {
      const ctx = await browser.newContext(), page = await ctx.newPage();
      await ctx.route(/.*/, r => { const u = new URL(r.request().url()); if (u.origin !== stationOrigin) return r.abort(); if (/^\/design(-1)?\.html$/.test(u.pathname)) return r.fulfill({ status: 200, contentType: 'text/html', body: '<!doctype html><title>wired</title>' }); return r.continue(); });
      await page.goto(stationOrigin + '/' + from + '?sandbox=1#top');
      await until(() => /\/design(-1)?\.html/.test(new URL(page.url()).pathname) && !/assets/.test(page.url()), from + ' forwards');
      const u = new URL(page.url());
      assert.strictEqual(u.pathname, '/' + to, `${from} lands on ${to}`); assert.strictEqual(u.search, '?sandbox=1', 'the sandbox flag is kept'); assert.strictEqual(u.hash, '#top');
      await ctx.close();
    }
    say('assets/design.html and assets/design-1.html forward to the wired pages (query and hash kept)');
  }

  /* ───────────────────────── screenshots (SHOTS=<dir> LIBS=<dir with materialize.min.css/js and jquery.min.js>): fixture pages only ───────────────────────── */
  async function shots(dir, libs) {
    fs.mkdirSync(dir, { recursive: true });
    const snap = (page, name) => page.screenshot({ path: path.join(dir, name) });
    // the Design Station (passcode station), the name set in the order chat, then 10 quiet minutes: the station's own sign-in box says why
    locked = true;
    let h = await open('design.html', { libs, seed: () => { try { sessionStorage.setItem('designStation.passcode', 'synthetic-operator-key-9d4'); } catch (_) {} } });
    await until(() => h.page.evaluate(() => typeof proceedToPrint === 'function' && window.StationSession && StationSession.page() && window.StationActivity), 'shots: design loaded');
    await wait(1500);
    await h.page.evaluate(() => BritesChat.open('3521000777'));
    await until(() => h.page.$('#bcWho'), 'shots: the chat');
    await h.page.evaluate(() => { document.getElementById('bcWho').click(); const i = document.querySelector('#bcWho input'); i.value = 'Nora Night'; i.dispatchEvent(new Event('blur')); });
    await until(() => sessionsOf('design').some(x => x.person === 'Nora Night'), 'shots: signed in');
    await snap(h.page, '1-design-signed-in-chat-1400.png');
    await autoSignOut(h.page, 'idle');
    await until(() => h.page.$('#pcGate .pcNote'), 'shots: the box');
    await snap(h.page, '2-design-idle-sign-in-box-1400.png');
    await h.page.setViewportSize({ width: 390, height: 800 }); await wait(200);
    await snap(h.page, '3-design-idle-sign-in-box-390.png');
    await h.page.setViewportSize({ width: 1400, height: 950 });
    await h.page.fill('#pcGate .pcInput', OPERATOR); await h.page.click('#pcGate button');
    await until(async () => !(await h.page.$('#pcGate')), 'shots: the box goes');
    await snap(h.page, '4-design-after-passcode-calm-line-1400.png');
    locked = false;
    // the message station: PIN box comes back with one calm line, the order on screen stays
    h = await open('design-message.html', { libs });
    await until(() => h.page.evaluate(() => window.StationSession && StationSession.page() && window.StationActivity && window.StationLiveOrder), 'shots: message loaded');
    await h.page.focus('#employeeNumberInput'); for (const d of PIN) await h.page.keyboard.press(d);
    await h.page.click('#employeeLoginBtn');
    await until(() => sessionsOf('design-message').some(x => x.person === 'Rosa Designer'), 'shots: PIN sign-in');
    await h.page.evaluate(() => { const i = document.getElementById('etsyOrderNumber'); i.value = '3521009001'; const ev = new KeyboardEvent('keydown', { key: 'Enter', bubbles: true }); i.dispatchEvent(ev); });
    await wait(1500);
    await snap(h.page, '5-design-message-signed-in-1400.png');
    await autoSignOut(h.page, 'idle'); await wait(500);
    await snap(h.page, '6-design-message-idle-pin-box-1400.png');
    await h.page.setViewportSize({ width: 390, height: 800 }); await wait(300);
    await snap(h.page, '7-design-message-idle-pin-box-390.png');
    say('screenshots written to ' + dir);
  }

  /* ───────────────────────── static checks: every page's sign-in is the design station's, and the new reasons are handled ───────────────────────── */
  function staticChecks() {
    const src = f => fs.readFileSync(path.join(root, f), 'utf8');
    for (const [f, dev] of [['design.html', 'design'], ['design-1.html', 'design-1'], ['design-message.html', 'design-message'], ['design-message-1.html', 'design-message-1']]) {
      const t = src(f);
      assert(new RegExp(`StationSession\\.init\\(\\{ station: "design", device: "${dev}"`).test(t), `${f}: signs in as station design, device ${dev}`);
      assert(/idle: "Signed out after 10 minutes without input\."/.test(t) && /closing: "Signed out at 5:00 pm\."/.test(t), `${f}: its signOut says the two new reasons`);
      assert(/station-live-order\.js/.test(t) && /station-activity\.js/.test(t), `${f}: loads the activity and live helpers`);
    }
    for (const f of ['design-print.html', 'design-print-1.html']) assert(!/StationSession|StationActivity/.test(src(f)), `${f}: a print frame with no person: it has no sign-in of its own (the station that opens it records the print)`);
    const bp = src('scripts/build-public.cjs');
    for (const f of ['design-scan.html', 'design-scan-1.html']) assert(bp.includes(`"${f}"`) && fs.existsSync(path.join(root, f)), `${f} is published`);
    say('static: four pages sign in as design with their device and say idle and closing; the print frames have no person; the scanners are published');
  }

  try {
    staticChecks();
    await designPage('design.html', 'design', true, 'Nora Night');
    await designPage('design-1.html', 'design-1', false, 'Dana Drafter');
    await messagePage('design-message.html', 'design-message', 'design-scan.html', 'design-scan-11', PIN, 'Rosa Designer');
    await messagePage('design-message-1.html', 'design-message-1', 'design-scan-1.html', 'design-scan-111', PIN2, 'Theo Typesetter');
    await oldCopies();
    if (process.env.SHOTS) await shots(process.env.SHOTS, process.env.LIBS);

    // nowhere on the wire: the PIN (but to the login door), customer words or an address, a passcode in a body
    const door1 = sent.filter(x => /"pinLogin":/.test(x.body));
    assert(door1.length >= 4 && door1.every(x => /^\{"pinLogin":"\d{6}"\}$/.test(x.body)) && mapGets === 0, 'the PIN went to the login door only; the whole roster was never asked for');
    const rest = sent.filter(x => !/"pinLogin":/.test(x.body));
    const restText = rest.map(x => x.url + ' ' + x.body).join('\n');
    assert(!restText.includes(PIN) && !restText.includes(PIN2), 'a PIN reached another request');
    const recorded = rest.filter(x => /"(?:session|activity|live)":/.test(x.body)).map(x => x.body).join('\n') + JSON.stringify(st.list('Station_Activity')) + JSON.stringify(st.list('Station_Sessions')) + JSON.stringify(st.list('Station_Live'));
    for (const bad of CUSTOMER) assert(!recorded.includes(bad), 'customer text in a session, activity or live record: ' + bad);
    assert(!recorded.includes(OPERATOR) && !recorded.includes(PASS), 'a passcode in a record');
    say(`design-wired: all passed (${st.list('Station_Activity').length} events, ${st.list('Station_Sessions').length} sessions, ${sent.length} requests; no PIN, passcode or customer text in any record)`);
  } finally { await browser.close(); srv.close(); }
})().catch(e => { console.error(e); process.exit(1); });
