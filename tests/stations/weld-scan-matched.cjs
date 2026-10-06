// The Welding station's scanner app: every order QR scanned while a person matches welded stud earrings to their orders is ONE `matched`
// activity event (order, person, task matching, the real scan time), credited by rule R3 (stations round 2, plans/stations-round2/api.md).
// The real phone page (weld-scan-1.html), the real desktop libraries (station-session.js in multi mode, station-activity.js,
// station-scan-queue.js) and the real activity / session door (netlify/functions/firebaseOrders.js with the real _stationActivity.js) run
// in headless Chromium and node. The fakes: Firestore (a Map with transactions, nested merges and increments), the relay document
// (the phone's POST is handed to the desktop page the way Firestore's snapshot would), Materialize and Firebase in the browser. Every
// request that is not to 127.0.0.1 is aborted. No PIN is typed or sent anywhere: people are signed in through the StationSession API.
//   1  · one person in Matching: that person, task matching, the scan's real time; scanned work (scans, matched), never an order completion
//   2  · two in Matching: the one with the latest input; a welder signed in last is never credited; a sign-out hands it to the other
//   3  · nobody in Matching (only a welder signed in): `unattributed`, filed under "Unattributed", never credited to the welder
//   4  · offline replay: scans that arrive while nobody is signed in wait (with their real times) and are credited at the replay by the
//        people signed in THEN (a Matching person, or unattributed when only a welder is in); a scan of an earlier day is never credited
//        to whoever signs in later
//   5  · duplicates: the same order within seconds is one event (two scan ids, or the same scan id told twice); a repeat later is its own
//        event ("again"); the event id is made from the scan, so the same scan written twice is stored once and counted once
//   6  · the phone page: offline scans are kept on the phone with their real times, survive a reload mid-queue, go out once each in
//        order when the connection is back, and reach the station with their real times; a repeat within seconds after a reload is one
//   7  · relayInfo: a phone clock that is minutes off is corrected, a scan that waited keeps its time, nothing is in the future
//   8  · a scan counts as input at the station (StationSession.touch) and other stations' queues are not affected
//   9  · the real weld-1.html passes the relay's data to the queue (a restored login is credited, one `matched` per scan)
//   NODE_PATH=$(npm root -g) PW_DIR=$(npm root -g)/playwright/node_modules CHROMIUM=… node tests/stations/weld-scan-matched.cjs
'use strict';
const http = require('http'), fs = require('fs'), path = require('path'), assert = require('assert/strict'), Module = require('module');
const root = path.join(__dirname, '../..');
const wait = ms => new Promise(r => setTimeout(r, ms));
async function until(fn, what, ms = 12000) {
  const t0 = Date.now();
  for (;;) { const v = await fn(); if (v) return v; if (Date.now() - t0 > ms) throw new Error('timed out waiting for ' + what); await wait(50); }
}

/* ── the server side: the real door over a Map-backed Firestore (as tests/stations/station-activity-door.cjs) ── */
const docs = new Map();
const INC = n => ({ __inc: n }), TS = { __ts: true };
const isPlain = v => v && typeof v === 'object' && !Array.isArray(v) && !v.__inc && !v.__ts;
function apply(prev, data, merge) {
  const out = merge && prev ? JSON.parse(JSON.stringify(prev)) : {};
  for (const [k, v] of Object.entries(data)) {
    if (v && v.__inc != null) out[k] = (Number(out[k]) || 0) + v.__inc;
    else if (v && v.__ts) out[k] = 'TS';
    else if (v && v.__del) delete out[k];
    else if (isPlain(v)) out[k] = apply(merge && isPlain(out[k]) ? out[k] : null, v, merge);
    else out[k] = v;
  }
  return out;
}
const ref = p => ({ path: p, id: p.split('/').pop(), set: async (d, o) => { docs.set(p, apply(docs.get(p), d, !!(o && o.merge))); } });
const snap = r => ({ exists: docs.has(r.path), data: () => docs.get(r.path), ref: r });
const fakeDb = {
  collection: c => ({ doc: id => ref(c + '/' + id) }),
  runTransaction: async fn => {
    const writes = [];
    const out = await fn({ getAll: (...rs) => Promise.resolve(rs.map(snap)), get: r => Promise.resolve(snap(r)), set: (r, d, o) => writes.push([r.path, d, o]) });
    for (const [p, d, o] of writes) docs.set(p, apply(docs.get(p), d, !!(o && o.merge)));
    return out;
  }
};
const fakeAdmin = { firestore: Object.assign(() => fakeDb, { FieldValue: { serverTimestamp: () => TS, increment: INC, delete: () => ({ __del: true }) } }) };
const realLoad = Module._load;
Module._load = function (req, ...rest) { if (/[\/]firebaseAdmin(\.js)?$/.test(req)) return fakeAdmin; return realLoad.call(this, req, ...rest); };
const door = require(path.join(root, 'netlify/functions/firebaseOrders.js'));
Module._load = realLoad;
let ipN = 0;
const send = body => door.handler({ httpMethod: 'POST', headers: { 'x-nf-client-connection-ip': '203.0.113.' + (++ipN % 250) }, queryStringParameters: {}, body: JSON.stringify(body) })
  .then(r => ({ status: r.statusCode, body: JSON.parse(r.body || '{}') }));
const matchedDocs = () => [...docs.entries()].filter(([k, v]) => k.startsWith('Station_Activity/') && v.action === 'matched').map(([, v]) => v).sort((a, b) => a.at - b.at || (a.id < b.id ? -1 : 1));
const matchedOf = order => matchedDocs().filter(e => e.orderId === order);
const rollups = () => [...docs.entries()].filter(([k]) => k.startsWith('Efficiency_Daily/')).map(([k, v]) => [k.split('__').slice(1).join('__'), v]);
const rollupOf = person => { const f = rollups().find(([p]) => p === person); return f ? f[1] : null; };

/* ── the browser side ── */
const FIREBASE = `(function () {
  const snaps = window.__fbSnaps = {}; window.__sets = [];
  const empty = () => ({ exists: false, data: () => undefined, docs: [], empty: true, size: 0, forEach() {}, docChanges: () => [] });
  function ref(p) {
    const r = { path: p, id: p.split('/').pop(), collection: n => ref(p + '/' + n), doc: n => ref(p + '/' + n),
      where: () => r, orderBy: () => r, limit: () => r, limitToLast: () => r, startAfter: () => r,
      onSnapshot(cb) { (snaps[p] = snaps[p] || []).push(cb); setTimeout(() => { try { cb(empty()); } catch (_) {} }, 0); return () => {}; },
      get: async () => empty(), set: async d => { window.__sets.push({ path: p, data: d }); }, update: async () => {}, add: async () => ref(p + '/new'), delete: async () => {} };
    return r;
  }
  const firestore = () => ({ collection: n => ref(n), doc: p => ref(p), batch: () => ({ set() {}, update() {}, delete() {}, commit: async () => {} }) });
  firestore.FieldValue = { delete: () => ({ __delete: true }), serverTimestamp: () => ({}), arrayUnion: (...a) => a, increment: n => n };
  firestore.Timestamp = { now: () => ({ toDate: () => new Date(), toMillis: () => Date.now() }) };
  const auth = () => ({ signInAnonymously: async () => ({}), onAuthStateChanged(cb) { setTimeout(() => cb({ uid: 'anon' }), 0); return () => {}; }, currentUser: { uid: 'anon' } });
  window.firebase = { apps: [], initializeApp() { return {}; }, firestore, auth, storage: () => ({ ref: () => ({}) }) };
})();`;
const MATERIALIZE = `window.__toasts = []; window.M = { AutoInit() {}, updateTextFields() {}, toast(o) { window.__toasts.push(String((o && o.html) || '')); },
  Modal: { init(el) { const i = { open() {}, close() {}, isOpen: false }; if (el) el.__m = i; return i; }, getInstance(el) { return (el && el.__m) || { open() {}, close() {} }; } },
  FormSelect: { init(el) { const i = { destroy() {}, getSelectedValues: () => [el && el.value] }; if (el) el.__fs = i; return i; }, getInstance(el) { return el && el.__fs; } },
  Dropdown: { init() {} } };`;
/* the desktop page's part that this test needs, kept exactly as weld-1.html has it: the libraries, the multi-person session, the queue and
   the relay listener that hands the relay document to the queue (the real weld-1.html is checked in 9) */
const HARNESS = `<!doctype html><html><head><meta charset="utf-8"><title>weld harness</title></head><body>
<script>${FIREBASE}</script><script>${MATERIALIZE}</script>
<script src="/station-session.js"></script><script src="/station-activity.js"></script><script src="/station-scan-queue.js"></script>
<script>
  window.__people = []; window.__ran = [];
  StationSession.init({ station: 'welding', device: 'weld-1', multi: true, people: () => window.__people.map(p => ({ name: p.name, task: p.task })), signOut() {} });
  window.__q = StationScanQueue.create({ device: 'weld-1', signedIn: () => window.__people.length > 0,
    run: (n, it) => { window.__ran.push({ n, at: it.at, s: it.s || '', m: it.m || 0 }); return Promise.resolve(); } });
  let first = true;
  firebase.firestore().collection('Brites_Orders').doc('weld-scan-1').onSnapshot(function (snap) {
    if (first) { first = false; return; }
    const data = snap.exists ? snap.data() : null;
    if (data && data['Order Number']) window.__q.offer(data['Order Number'], data);
  });
</script></body></html>`;
const MIME = { '.html': 'text/html', '.js': 'text/javascript', '.css': 'text/css', '.png': 'image/png' };

const results = [];
async function check(name, fn) { try { await fn(); results.push([name, null]); } catch (e) { results.push([name, e]); } }

/* ten-digit order ids, one block per scenario */
const oid = (block, n) => '35' + String(block).padStart(2, '0') + '0000' + String(n).padStart(2, '0');

async function main() {
  const pwDir = process.env.PW_DIR || path.join(root, 'node_modules');
  const { chromium } = require(path.join(pwDir, 'playwright-core'));
  const server = await new Promise(ok => { const s = http.createServer((req, res) => {
    const url = decodeURIComponent(req.url.split('?')[0]);
    if (url === '/__weld-harness.html') { res.writeHead(200, { 'Content-Type': 'text/html' }); return res.end(HARNESS); }
    const f = path.join(root, url);
    if (!f.startsWith(root) || !fs.existsSync(f) || fs.statSync(f).isDirectory()) { res.writeHead(404); return res.end(); }
    res.writeHead(200, { 'Content-Type': MIME[path.extname(f)] || 'application/octet-stream' }); fs.createReadStream(f).pipe(res);
  }).listen(0, '127.0.0.1', () => ok(s)); });
  const base = `http://127.0.0.1:${server.address().port}`;
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  const errors = [];
  const net = { down: false, bodies: [], desk: null, relay: {} };            // the phone's connection, what it posted, where the relay goes

  /* a context whose network is the fake: firebaseOrders is the real door (activity, session) or the relay; nothing leaves the machine */
  async function newContext() {
    const context = await browser.newContext({ viewport: { width: 1200, height: 900 } });
    await context.route(() => true, r => r.abort());
    await context.route(u => u.href.startsWith('http://127.0.0.1'), r => r.continue());
    await context.route(u => /gstatic\.com\/firebasejs\//.test(u.href), r => r.fulfill({ contentType: 'text/javascript', body: /firebase-app-compat/.test(r.request().url()) ? FIREBASE : '' }));
    await context.route(u => /materialize/.test(u.href), r => r.fulfill({ contentType: /\.css/.test(r.request().url()) ? 'text/css' : 'text/javascript', body: /\.css/.test(r.request().url()) ? '' : MATERIALIZE }));
    await context.route(u => /code\.jquery\.com|qz-tray/.test(u.href), r => r.fulfill({ contentType: 'text/javascript', body: '' }));
    return context;
  }
  const json = (r, o, status) => r.fulfill({ status: status || 200, contentType: 'application/json', body: JSON.stringify(o) });
  /* the phone's context: its POSTs are the relay (handed to the desktop page as a Firestore snapshot) */
  async function phoneRoutes(context) {
    await context.route(u => u.href.startsWith('http://127.0.0.1') && u.pathname.includes('/.netlify/functions/'), async r => {
      if (net.down) return r.abort('internetdisconnected');
      const req = r.request();
      let b = {}; try { b = JSON.parse(req.postData() || '{}'); } catch (_) {}
      if (b.orderNumber === 'weld-scan-1' && b.orderNumField !== undefined) {
        net.bodies.push(b);
        const stored = await send(b);                                         // the real door writes the relay document (Brites_Orders/weld-scan-1)
        if (stored.status !== 200) return json(r, stored.body, stored.status);
        const data = Object.assign(net.relay, { 'Order Number': b.orderNumField, 'Client Name': b.clientName, 'Brites Messages': b.britesMessages, 'Shipping Label Timestamps': b.shippingLabelTimestamps, 'Employee Name': b.employeeName, 'Staff Note': b.staffNote });
        const copy = JSON.parse(JSON.stringify(data));
        if (net.desk) await net.desk.evaluate(d => { const cbs = window.__fbSnaps['Brites_Orders/weld-scan-1']; cbs[cbs.length - 1]({ exists: true, data: () => d }); }, copy);
        return json(r, { success: true });
      }
      return json(r, { success: true });
    });
  }
  /* the desktop's context: activity and session go to the real door */
  async function deskRoutes(context, extra) {
    await context.route(u => u.href.startsWith('http://127.0.0.1') && u.pathname.includes('/.netlify/functions/'), async r => {
      const req = r.request(), u = new URL(req.url());
      if (req.method() === 'POST') {
        let b = {}; try { b = JSON.parse(req.postData() || '{}'); } catch (_) {}
        if (Array.isArray(b.activity) || (b.session && typeof b.session === 'object')) { const out = await send(b); return json(r, out.body, out.status); }
        if (extra) { const x = await extra(u, b); if (x) return json(r, x); }
        return json(r, { success: true });
      }
      if (extra) { const x = await extra(u, {}); if (x) return json(r, x); }
      return json(r, { success: true, data: {} });
    });
  }
  const watch = (page, tag) => { page.on('pageerror', e => errors.push(tag + ': ' + e.message)); };

  try {
    /* ═══ the desktop harness ═══ */
    const deskCtx = await newContext();
    await deskRoutes(deskCtx);
    const desk = await deskCtx.newPage(); watch(desk, 'desk');
    net.desk = desk;
    await desk.goto(base + '/__weld-harness.html');
    await desk.waitForFunction(() => window.StationSession && window.StationActivity && window.StationScanQueue && StationSession.page(), null, { timeout: 15000 });
    await wait(200);
    const signIn = async (name, task) => { await desk.evaluate(([n, t]) => { window.__people.push({ name: n, task: t }); StationSession.signedIn({ name: n, id: null, task: t }); window.__q.drain(); }, [name, task]); await wait(25); };   // (the page runs the waiting scans right after a login: startScannedOrderListener)
    const signOut = async (name, task) => { await desk.evaluate(([n, t]) => { window.__people = window.__people.filter(p => !(p.name === n && p.task === t)); StationSession.signedOut('signOut', { name: n, task: t }); }, [name, task]); await wait(25); };
    const signAllOut = async () => { await desk.evaluate(() => { const l = window.__people.slice(); window.__people = []; for (const p of l) StationSession.signedOut('signOut', { name: p.name, task: p.task }); }); await wait(25); };
    const touchAs = (name, task) => desk.evaluate(([n, t]) => StationSession.touch(Date.now(), { name: n, task: t }), [name, task]);
    const payload = (code, o = {}) => {
      const now = Date.now(), at = o.at != null ? o.at : now, sent = o.sent != null ? o.sent : now;
      const d = { 'Order Number': code, 'Client Name': 'Scanner Page', 'Brites Messages': '(Auto push from weld-scan-1.html)', 'Shipping Label Timestamps': new Date(at).toISOString(), 'Employee Name': 'ScannerBot' };
      if (o.id !== false) d['Staff Note'] = JSON.stringify({ v: 1, id: o.id || 's' + Math.random().toString(36).slice(2, 10), at, sent, q: 0 });
      return d;
    };
    const relay = d => desk.evaluate(x => { const cbs = window.__fbSnaps['Brites_Orders/weld-scan-1']; cbs[cbs.length - 1]({ exists: true, data: () => x }); }, d);
    const flush = () => desk.evaluate(() => window.StationActivity.flush());
    const settle = async () => { await wait(150); await flush(); await wait(100); };
    const note = () => desk.evaluate(() => { const n = document.getElementById('stationScanQueueNote'); return n && n.style.display !== 'none' ? n.textContent : null; });
    const ran = () => desk.evaluate(() => window.__ran.slice());
    const A = 'Ana M.', B = 'Bea K.', W = 'Wes T.';

    /* ── 1 ── */
    await check('1 one person in Matching: credited, task matching, the real scan time; scanned work, never an order completion', async () => {
      await signIn(A, 'matching');
      await wait(1150);                                             // (input is stamped one a second at most)
      const o = oid(1, 1), t0 = Date.now(), at = t0 - 40000;
      const li0 = await desk.evaluate(() => StationSession.lastInput());
      await relay(payload(o, { at, sent: t0 }));
      await until(async () => { await flush(); return matchedOf(o).length >= 1; }, 'the matched event');
      await settle();
      const ev = matchedOf(o);
      assert.equal(ev.length, 1, 'one event');
      assert.equal(ev[0].person, A); assert.equal(ev[0].task, 'matching'); assert.equal(ev[0].station, 'welding'); assert.equal(ev[0].device, 'weld-1');
      assert.equal(ev[0].action, 'matched'); assert.equal(ev[0].orderId, o); assert.equal(ev[0].parts, 0); assert.equal(ev[0].orders, 0);
      assert.ok(!('unattributed' in ev[0]), 'credited: not unattributed');
      assert.ok(Math.abs(ev[0].at - at) < 3000, `the real scan time, not the arrival time (${ev[0].at - at} ms off)`);
      assert.match(ev[0].session, /^welding__weld-1__Ana_M\.__matching__/, 'the credited person\'s own session');
      assert.match(ev[0].id, /^mt_weld-1_3501000001_/, 'the event id is made from the scan');
      const r = rollupOf(A);
      assert.ok(r, 'the person-day rollup exists');
      const w = r.stations.welding;
      assert.equal(w.matched, 1); assert.equal(w.scans, 1, 'scanned work');
      assert.ok(!w.completes && !w.parts && !w.orders, 'never a completion, a piece or an order');
      assert.ok((await desk.evaluate(() => StationSession.lastInput())) > li0, 'a scan is input at the station');
      assert.deepEqual((await ran()).map(x => x.n), [o], 'the order still loads through the page as before');
      assert.equal((await ran())[0].m, 1, 'and says it was recorded');
    });

    /* ── 2 ── */
    await check('2 two in Matching: the latest input wins; a welder signed in last is never credited; a sign-out hands it over', async () => {
      await signIn(B, 'matching');                                  // Bea signed in after Ana
      const o1 = oid(2, 1), o2 = oid(2, 2), o3 = oid(2, 3), o4 = oid(2, 4);
      await relay(payload(o1));
      await until(async () => { await flush(); return matchedOf(o1).length >= 1; }, 'scan 1');
      assert.equal(matchedOf(o1)[0].person, B, 'the one who signed in last has the latest input');
      await wait(30); await touchAs(A, 'matching');                 // Ana does something at the station
      await relay(payload(o2));
      await until(async () => { await flush(); return matchedOf(o2).length >= 1; }, 'scan 2');
      assert.equal(matchedOf(o2)[0].person, A, 'latest input: Ana');
      await signIn(W, 'welding');                                   // a welder signs in last, and has the latest input of all
      await wait(30); await touchAs(W, 'welding');
      await relay(payload(o3));
      await until(async () => { await flush(); return matchedOf(o3).length >= 1; }, 'scan 3');
      assert.equal(matchedOf(o3)[0].person, A, 'never the welder');
      assert.ok(!('unattributed' in matchedOf(o3)[0]));
      await signOut(A, 'matching');
      await relay(payload(o4));
      await until(async () => { await flush(); return matchedOf(o4).length >= 1; }, 'scan 4');
      assert.equal(matchedOf(o4)[0].person, B, 'Ana signed out: Bea carries on');
      assert.match(matchedOf(o4)[0].session, /^welding__weld-1__Bea_K\.__matching__/);
      assert.equal(rollupOf(W), null, 'the welder has no rollup from scans');
      assert.equal(rollupOf(B).stations.welding.matched, 2); assert.equal(rollupOf(A).stations.welding.matched, 3, 'Ana: the first scan and two more');
    });

    /* ── 3 ── */
    await check('3 nobody in Matching: unattributed, filed under "Unattributed", never the welder', async () => {
      await signOut(B, 'matching');                                 // only the welder is left
      const people = await desk.evaluate(() => StationSession.people().map(p => p.name + ':' + p.task));
      assert.deepEqual(people, [W + ':welding']);
      const o = oid(3, 1);
      await relay(payload(o));
      await until(async () => { await flush(); return matchedOf(o).length >= 1; }, 'the unattributed event');
      const ev = matchedOf(o)[0];
      assert.equal(ev.unattributed, true); assert.equal(ev.person, 'Unattributed'); assert.equal(ev.task, 'matching'); assert.equal(ev.session, '');
      assert.equal(ev.orderId, o); assert.equal(ev.parts, 0); assert.equal(ev.orders, 0);
      assert.equal(rollupOf(W), null, 'nothing credited to the welder');
      assert.deepEqual((await ran()).filter(x => x.n === o).map(x => x.m), [1], 'the scan was not dropped: it still loads');
    });

    /* ── 4 ── */
    await check('4 offline replay: scans that arrive with nobody signed in wait with their real times and are credited at the replay', async () => {
      await signAllOut();
      const o1 = oid(4, 1), o2 = oid(4, 2), t0 = Date.now();
      await relay(payload(o1, { at: t0 - 150000, sent: t0 }));
      await wait(30);
      await relay(payload(o2, { at: t0 - 90000, sent: t0 }));
      await wait(300); await flush();
      assert.equal(await note(), '2 phone scans waiting for a sign-in');
      assert.equal(matchedOf(o1).length + matchedOf(o2).length, 0, 'nothing is recorded or credited while nobody is there');
      await signIn(B, 'matching');
      await until(async () => { await flush(); return matchedOf(o1).length + matchedOf(o2).length >= 2; }, 'the replay');
      await settle();
      for (const [o, at] of [[o1, t0 - 150000], [o2, t0 - 90000]]) {
        const ev = matchedOf(o);
        assert.equal(ev.length, 1, 'one event for ' + o);
        assert.equal(ev[0].person, B, 'credited at the replay to the person signed in then');
        assert.ok(Math.abs(ev[0].at - at) < 3000, `with its real scan time (${ev[0].at - at} ms off)`);
      }
      assert.equal(await note(), null);
      // only a welder signs in after a scan made with nobody there: unattributed at the replay
      await signAllOut();
      const o3 = oid(4, 3);
      await relay(payload(o3, { at: Date.now() - 20000 }));
      await wait(200);
      assert.equal(matchedOf(o3).length, 0);
      await signIn(W, 'welding');
      await until(async () => { await flush(); return matchedOf(o3).length >= 1; }, 'the replay under a welder');
      assert.equal(matchedOf(o3)[0].unattributed, true, 'a welder is never credited, not even at a replay');
      await signAllOut(); await signIn(A, 'matching');
      // a scan from an earlier day is not credited to whoever signs in later
      const o4 = oid(4, 4), old = Date.now() - 30 * 3600e3;
      await relay(payload(o4, { at: old, sent: Date.now() }));
      await until(async () => { await flush(); return matchedOf(o4).length >= 1; }, 'the old scan');
      assert.equal(matchedOf(o4)[0].unattributed, true, 'unattributed');
      assert.ok(Math.abs(matchedOf(o4)[0].at - old) < 3000, 'with its real time');
    });

    /* ── 5 ── */
    await check('5 duplicates: the same order within seconds is one event; a later repeat is its own ("again"); a scan id is stored once', async () => {
      const o = oid(5, 1), t0 = Date.now();
      await relay(payload(o, { at: t0, id: 'dupA1' }));
      await wait(30);
      await relay(payload(o, { at: t0 + 2500, sent: t0 + 2500, id: 'dupA2' }));      // the camera swung back 2.5 s later
      await wait(30);
      await relay(payload(o, { at: t0, id: 'dupA1' }));                              // the same scan told again
      await settle();
      assert.equal(matchedOf(o).length, 1, 'one event');
      assert.equal((await ran()).filter(x => x.n === o).length, 1, 'and the order loaded once');
      // a different order in between does not make a repeat of the first one new inside the window
      const o2 = oid(5, 2);
      await relay(payload(o2, { at: t0 + 3000, id: 'dupB1' }));
      await relay(payload(o, { at: t0 + 4000, sent: t0 + 4000, id: 'dupA3' }));
      await settle();
      assert.equal(matchedOf(o).length, 1, 'A, B, A inside 10 s is still one scan of A');
      // a real repeat long after the first: its own event
      const o3 = oid(5, 3), base = Date.now() - 120000;
      await relay(payload(o3, { at: base, sent: Date.now(), id: 'dupC1' }));
      await relay(payload(o3, { at: Date.now() - 1000, sent: Date.now(), id: 'dupC2' }));
      await settle();
      const evs = matchedOf(o3);
      assert.equal(evs.length, 2, 'a repeat long after the first is its own event');
      assert.ok(!/again/.test(evs[0].detail) && /phone scan · again$/.test(evs[1].detail), 'and says "again": ' + evs.map(e => e.detail).join(' | '));
      // the same scan written twice to the door is stored once and counted once
      const before = rollupOf(A).stations.welding.matched, ev = evs[1];
      const r1 = await send({ activity: [ev] }), r2 = await send({ activity: [ev, ev] });
      assert.deepEqual([r1.body.written, r1.body.duplicate, r2.body.written, r2.body.duplicate], [0, 1, 0, 2]);
      assert.equal(rollupOf(A).stations.welding.matched, before, 'counted once');
    });

    /* ── 7 (pure) ── */
    await check('7 relayInfo: a clock that is minutes off is corrected, a waiting scan keeps its time, never the future', async () => {
      const r = await desk.evaluate(() => {
        const now = Date.now(), f = (m, n) => StationScanQueue.relayInfo(m, n);
        const note = o => JSON.stringify(Object.assign({ v: 1, id: 'sx' }, o));
        const iso = t => new Date(t).toISOString();
        return {
          fast: f({ 'Shipping Label Timestamps': iso(now + 300000), 'Staff Note': note({ at: now + 300000, sent: now + 300400 }) }, now).at - now,          // phone 5 min fast, sent 0.4 s before arrival... (sent is phone time)
          waited: f({ 'Staff Note': note({ at: now - 600000, sent: now }) }, now).at - now,                                                                     // scanned 10 min ago, sent just now
          slow: f({ 'Staff Note': note({ at: now - 300000 - 100, sent: now - 300000 }) }, now).at - now,                                                        // phone 5 min slow: sent 5 min "ago", scanned 0.1 s before sending
          future: f({ 'Staff Note': note({ at: now + 99999999, sent: now }) }, now).at - now,
          plain: f({ 'Order Number': '1' }, now).at - now,                                                                                                       // no descriptor: arrival time
          isoNear: f({ 'Shipping Label Timestamps': iso(now - 30000) }, now).at - now,                                                                          // an older phone page, clock right
          isoFar: f({ 'Shipping Label Timestamps': iso(now - 3600000) }, now).at - now,                                                                         // an older phone page, clock off: arrival time
          ancient: f({ 'Staff Note': note({ at: now - 20 * 86400000, sent: now - 20 * 86400000 + 100 }) }, now).at - now,
          id: f({ 'Staff Note': note({ id: 'ab-c.d:e f!?', at: now, sent: now }) }, now).id,
          none: f(null, now).at - now
        };
      });
      assert.ok(Math.abs(r.fast + 400) < 5 || r.fast <= 0, 'a fast clock is pulled back, never ahead: ' + r.fast);
      assert.ok(r.fast > -1000 && r.fast <= 0, 'the fast phone\'s scan time lands at the arrival time: ' + r.fast);
      assert.ok(Math.abs(r.waited + 600000) < 5, 'a scan that waited on the phone keeps its real time: ' + r.waited);
      assert.ok(Math.abs(r.slow + 100) < 5, 'a slow clock is moved forward by the measured difference: ' + r.slow);
      assert.equal(r.future, 0, 'never in the future');
      assert.equal(r.plain, 0); assert.ok(r.isoNear < 0 && r.isoNear > -31000, 'an older phone page with a right clock: its time'); assert.equal(r.isoFar, 0);
      assert.ok(r.ancient > -7 * 86400000, 'never older than a week: ' + r.ancient); assert.equal(r.none, 0);
      assert.equal(r.id, 'ab-c.d:ef', 'the id is tidy');
    });

    /* ── 6: the real phone page ── */
    await check('6 the phone page: offline scans are kept with their real times, survive a reload mid-queue, go out once each in order, and a repeat after the reload is one', async () => {
      const phoneCtx = await newContext();
      await phoneCtx.addInitScript(() => { try { Object.defineProperty(navigator, 'mediaDevices', { value: { getUserMedia: () => new Promise(() => {}) }, configurable: true }); } catch (_) {} });
      await phoneRoutes(phoneCtx);
      const phone = await phoneCtx.newPage(); watch(phone, 'phone');
      phone.on('dialog', d => d.dismiss().catch(() => {}));
      await signAllOut(); await signIn(A, 'matching');
      const open = async () => { await phone.goto(base + '/weld-scan-1.html'); await phone.waitForFunction(() => typeof handleScannedCode === 'function' && window.M, null, { timeout: 15000 }); await wait(150); };
      const scan = c => phone.evaluate(x => handleScannedCode(x), c);
      const outbox = () => phone.evaluate(() => { const d = JSON.parse(localStorage.getItem('weldScanOutbox.v1') || 'null'); return d ? d.items : []; });
      const o1 = oid(6, 1), o2 = oid(6, 2), o3 = oid(6, 3);
      await open();
      // live: one scan goes straight out, with its time and id
      const live = oid(6, 9), tl = Date.now();
      await scan(live);
      await until(async () => matchedOf(live).length >= 1 || (await flush(), matchedOf(live).length >= 1), 'the live scan');
      assert.equal(net.bodies.at(-1).orderNumField, live); assert.equal(net.bodies.at(-1).orderNumber, 'weld-scan-1');
      const nb = JSON.parse(net.bodies.at(-1).staffNote);
      assert.equal(nb.v, 1); assert.match(nb.id, /^s[\w]{6,}$/); assert.ok(nb.at >= tl && nb.at <= nb.sent && nb.sent - nb.at < 3000);
      assert.equal(net.bodies.at(-1).shippingLabelTimestamps, new Date(nb.at).toISOString(), 'the scan time stays where it always was');
      assert.equal(matchedOf(live)[0].person, A); assert.ok(Math.abs(matchedOf(live)[0].at - nb.at) < 2000);
      assert.deepEqual(await outbox(), [], 'sent: nothing left on the phone');
      const relayDoc = docs.get('Brites_Orders/weld-scan-1');
      assert.ok(relayDoc && relayDoc['Order Number'] === live && relayDoc['Shipping Label Timestamps'] === new Date(nb.at).toISOString() && JSON.parse(relayDoc['Staff Note']).id === nb.id, 'the real door stores the relay document with the scan\'s time and id: ' + JSON.stringify(relayDoc));
      assert.equal(relayDoc['Employee Name'], 'ScannerBot', 'no person: the phone never says who');
      // offline: three scans are kept on the phone
      net.down = true; await phoneCtx.setOffline(true);
      const t1 = Date.now(); await scan(o1); await wait(1100); await scan(o2); await wait(1100); await scan(o3);
      const kept = await outbox();
      assert.deepEqual(kept.map(i => i.n), [o1, o2, o3], 'kept in the order scanned');
      assert.ok(kept[0].at >= t1 && kept[1].at - kept[0].at >= 1000 && kept[2].at - kept[1].at >= 1000, 'with their real scan times');
      assert.equal(new Set(kept.map(i => i.id)).size, 3, 'each with its own id');
      const sentBefore = net.bodies.length;
      assert.ok((await phone.evaluate(() => window.__toasts.join('|'))).includes('Saved on this phone'), 'the person is told it is kept');
      // the page reloads mid-queue (still offline): the scans are still there
      await phoneCtx.setOffline(false); await open();               // (the server stays unreachable: net.down)
      assert.deepEqual((await outbox()).map(i => i.n), [o1, o2, o3], 'a reload does not lose them');
      await phoneCtx.setOffline(true);
      // a repeat of the last order right after the reload is one scan
      await scan(o3);
      assert.equal((await outbox()).length, 3, 'the same order within seconds after a reload: not a second scan');
      assert.ok((await phone.evaluate(() => window.__toasts.join('|'))).includes('Already scanned'));
      await wait(2500);
      assert.equal(net.bodies.length, sentBefore, 'nothing went out while offline');
      assert.equal(matchedOf(o1).length + matchedOf(o2).length + matchedOf(o3).length, 0);
      // the connection is back: everything goes out once, in order
      const tBack = Date.now();
      net.down = false; await phoneCtx.setOffline(false);
      await until(async () => { await flush(); return matchedOf(o1).length && matchedOf(o2).length && matchedOf(o3).length; }, 'the replay from the phone', 25000);
      await settle();
      assert.deepEqual((await outbox()), [], 'the outbox is empty');
      const posts = net.bodies.slice(sentBefore).map(b => b.orderNumField);
      assert.deepEqual(posts, [o1, o2, o3], 'each went out once, in the order scanned: ' + posts);
      for (const [o, i] of [[o1, 0], [o2, 1], [o3, 2]]) {
        const ev = matchedOf(o);
        assert.equal(ev.length, 1, 'one event for ' + o); assert.equal(ev[0].person, A);
        assert.ok(Math.abs(ev[0].at - kept[i].at) < 2500, `the real scan time, not the replay time (${ev[0].at - kept[i].at} ms off)`);
        assert.ok(tBack - ev[0].at > 1500, 'older than the moment the connection came back');
      }
      const sentAt = net.bodies.slice(sentBefore).map(b => JSON.parse(b.staffNote).sent);
      assert.ok(sentAt[1] - sentAt[0] >= 1500 && sentAt[2] - sentAt[1] >= 1500, 'a couple of seconds apart: the relay holds one scan at a time');
      await phoneCtx.close();
    });

    /* ── 8 ── */
    await check('8 other stations\' queues are not touched: no matched event, the queue behaves as before', async () => {
      const r = await desk.evaluate(async () => {
        const got = [];
        const q = StationScanQueue.create({ device: 'assembly-9', matched: false, signedIn: () => true, run: (n, it) => { got.push({ n, keys: Object.keys(it).sort().join() }); } });
        const a = q.offer('4400000001'), b = q.offer('4400000001');          // as before: no new duplicate rule here
        await q.drain(); await new Promise(r => setTimeout(r, 100));
        return { a, b, got, left: q.count() };
      });
      assert.deepEqual([r.a, r.b], [true, true]);
      assert.deepEqual(r.got.map(x => x.keys), ['at,n', 'at,n'], 'items as before: the order and the time only');
      assert.equal(matchedOf('4400000001').length, 0);
    });

    /* ── 9: the real weld-1.html ── */
    await check('9 the real weld-1.html hands the relay data to the queue: a restored login is credited, one matched per scan', async () => {
      const ctx = await newContext();
      await deskRoutes(ctx, async (u, b) => {
        const fn = u.pathname.split('/').pop();
        if (fn === 'etsyOrderProxy') { const id = u.searchParams.get('orderId'); return { receipt_id: Number(id) || 0, status: 'Paid', transactions: [{ transaction_id: 91000 + (Number(id) % 1000), title: 'Custom Stud Earrings', quantity: 2, sku: 'ST-1', variations: [] }] }; }
        if (fn === 'firebaseOrders' && u.searchParams.get('cancelCheck')) return { success: true, cancelled: {}, now: Date.now() };
        if (fn === 'firebaseOrders' && /employee/i.test(u.searchParams.get('orderId') || '')) return { success: false, error: 'closed' };
        return null;
      });
      await ctx.addInitScript(() => {
        try {
          localStorage.setItem('access_token', 'test-token'); localStorage.setItem('refresh_token', 'ref'); localStorage.setItem('token_expires_at', String(Math.floor(Date.now() / 1000) + 86400));
          // a login left from before: the name and a made-up id (never a PIN); the page restores it as one person under Matching
          localStorage.setItem('employee_id', 'fixture-login'); localStorage.setItem('employee_name', 'Ana M.');
        } catch (_) {}
      });
      const page = await ctx.newPage(); watch(page, 'weld-1');
      await page.goto(base + '/weld-1.html');
      await page.waitForFunction(() => window.StationActivity && window.StationSession && window.StationScanQueue && StationSession.who(), null, { timeout: 20000 });
      await wait(400);
      const html = fs.readFileSync(path.join(root, 'weld-1.html'), 'utf8');
      assert.ok(/scanQueue\.offer\(orderNumber, data\)/.test(html), 'the relay listener passes the relay data');
      assert.ok(/station-scan-queue\.js\?v=\d{8}-\w+/.test(html) && !/station-scan-queue\.js\?v=20261003-sq1/.test(html), 'the queue script tag is bumped');
      const o1 = oid(9, 1), o2 = oid(9, 2), at = Date.now() - 30000;
      const deliver = d => page.evaluate(x => { const cbs = window.__fbSnaps['Brites_Orders/weld-scan-1']; cbs[cbs.length - 1]({ exists: true, data: () => x }); }, d);
      await deliver(payload(o1, { at, sent: Date.now() }));
      await page.waitForTimeout(300);
      await deliver(payload(o2));
      await until(async () => { await page.evaluate(() => window.StationActivity.flush()); return matchedOf(o1).length && matchedOf(o2).length; }, 'both matched events on the real page', 20000);
      await page.waitForTimeout(1500); await page.evaluate(() => window.StationActivity.flush()); await wait(200);
      for (const o of [o1, o2]) {
        assert.equal(matchedOf(o).length, 1, 'one matched per scan: ' + o);
        assert.equal(matchedOf(o)[0].person, 'Ana M.'); assert.equal(matchedOf(o)[0].task, 'matching'); assert.equal(matchedOf(o)[0].device, 'weld-1');
      }
      assert.ok(Math.abs(matchedOf(o1)[0].at - at) < 3000, 'the real scan time reaches the event');
      assert.equal(await page.evaluate(() => window.__sets.filter(s => s.path === 'Brites_Orders/weld-scan-1' && s.data && s.data['Order Number'] && s.data['Order Number'].__delete).length), 2, 'each scan is still cleared from the relay once');
      assert.equal(await page.evaluate(() => (window.__fbSnaps['Brites_Orders/weld-scan-1'] || []).length), 1, 'still one relay listener');
      await ctx.close();
    });

    await check('no page errors from the queue, the recorder or the phone page', async () => {
      assert.deepEqual(errors.filter(m => /scan|queue|matched|relay|outbox|StationActivity|StationScanQueue|handleScannedCode/i.test(m)), []);
    });
    await check('no PIN-looking value in anything stored', async () => {
      for (const e of matchedDocs()) {
        assert.ok(/\p{L}/u.test(e.person), 'a name, never digits: ' + e.person);
        assert.ok(!/(?<!\d)\d{6}(?!\d)/.test(e.detail + ' ' + e.person), 'no 6-digit number in the text');
      }
      assert.ok(matchedDocs().length >= 15, 'the events are there: ' + matchedDocs().length);
    });
  } finally { await browser.close(); server.close(); }

  let bad = 0;
  for (const [name, err] of results) { console.log((err ? 'FAIL ' : 'ok   ') + name); if (err) { bad++; console.log('     ' + String(err.stack || err.message).split('\n').slice(0, 4).join('\n     ')); } }
  console.log(bad ? `\n${bad} of ${results.length} failed` : `\nweld-scan-matched: all ${results.length} checks passed`);
  process.exit(bad ? 1 : 0);
}
main().catch(e => { console.error(e); process.exit(1); });
