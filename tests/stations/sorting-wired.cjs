// The Sorting station, wired into the Employee efficiency portal, end to end over the fake backend (Paul, 6 Oct 2026: every
// station app, each with its own scanner, fully wired). The real pages run in a real browser (sorting.html, sorting-2.html,
// sort-scan.html, the Charm Sorter charm-nest-1.html) with the real station-session.js, station-activity.js and
// station-live-order.js; every /.netlify/functions/firebaseOrders call goes to the REAL door (firebaseOrders.js: pinLogin, session,
// activity, live) over ONE in-memory Firestore, and the portal numbers are read back through the REAL reader (employeeEfficiency.js:
// live, overview, person) from that same store. Etsy, Firebase's browser SDK, Materialize and the printer are stubs; every other host
// is aborted; every Employee Number here is made up when the test runs and is never printed.
//   1 · sorting.html: the PIN door signs the person in by NAME (no PIN in any request, document or storage), a session with beats
//       (pagehide) in Station_Sessions; a typed batch is one scan per order with its pieces, a sticker is a print and the order sorted
//       once; the live order is in hand with a piece per unit (sheet "Batch of N orders" or the order with its QR text) and goes
//       when the last sticker prints; the signed-in person is credited, never anybody else
//   2 · the phone scanner: sort-scan.html (its real Send button and the real door) relays a sheet-QR batch; sorting.html loads it, the
//       scan is credited to the signed-in person of that desk, the relay counts as input (StationSession.touch), a reload does not
//       load the same batch again; a phone batch that loads while nobody is signed in is kept and credited to the person who signs in
//       next (never to nobody, never lost, never put under yesterday's name)
//   3 · the sign-in ends by itself (midnight, idle, closing): the page's own signOut clears ITS login, shows its own sign-in with
//       the right plain words and keeps the work on screen; the session ends and the live order goes
//   4 · sorting-2.html: the same wiring (it is not an old copy: it signs in, beats, logs, shows live), its reload no longer loads
//       the old phone batch again, its phone batch is kept for the next sign-in, its signOut copes with every reason
//   5 · the Charm Sorter (the nesting part of Sorting): the typed name signs in at station sorter, Print QR label is a print and the
//       order completed once, the open order window is the live order, its signOut copes with every reason
//   6 · the portal numbers agree: the board (live), the Overview and the person view give the same pieces, scans and orders for
//       the Sorting station (the Sorter's rows folded into it by displayStation), the live card has its thumbnail, QR and person
//   NODE_PATH=$(npm root -g) PW_DIR=$(npm root -g)/playwright/node_modules CHROMIUM=... node tests/stations/sorting-wired.cjs
'use strict';
const fs = require('fs'), path = require('path'), assert = require('assert'), Module = require('module');
const root = path.join(__dirname, '../..');
const pwDir = process.argv[2] || process.env.PW_DIR || path.join(root, 'node_modules');
const { chromium } = require(path.join(pwDir, 'playwright-core'));
const CHROME = process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome';
const { start } = require('../charm-nest/bridge-server.cjs');

/* ═══════════════ one in-memory Firestore (typed fields, queries, increments, merge, transactions) ═══════════════ */
class Ts { constructor(m) { this.m = m; } toMillis() { return this.m; } toDate() { return new Date(this.m); } static fromMillis(m) { return new Ts(m); } }
const SERVER_TS = { __ts: true }, DEL = { __del: true }, inc = n => ({ __inc: n });
const colls = new Map();
const col = n => { if (!colls.has(n)) colls.set(n, new Map()); return colls.get(n); };
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
const kind = v => v instanceof Ts ? 'ts' : typeof v, val = v => v instanceof Ts ? v.m : v;
const refOf = (c, id) => ({ c, id, path: c + '/' + id });
const snapOf = r => { const d = col(r.c).get(r.id); return { id: r.id, exists: !!d, data: () => d ? clone(d) : undefined, ref: r, get: f => d ? clone(d[f]) : undefined }; };
function query(name, filters, order, lim, sel) {
  return {
    where: (f, op, v) => query(name, filters.concat([[f, op, v]]), order, lim, sel),
    orderBy: (f, d) => query(name, filters, [f, d || 'asc'], lim, sel),
    limit: n => query(name, filters, order, n, sel),
    select: (...f) => query(name, filters, order, lim, f),
    get: async () => {
      let docs = [...col(name)].map(([id, d]) => ({ id, d }));
      for (const [f, op, v] of filters) docs = docs.filter(({ d }) => {
        const x = d[f]; if (x === undefined || kind(x) !== kind(v)) return false;
        const a = val(x), b = val(v);
        return op === '==' ? a === b : op === '>=' ? a >= b : op === '>' ? a > b : op === '<' ? a < b : op === '<=' ? a <= b : false;
      });
      if (order) { const [f, dir] = order; docs = docs.filter(({ d }) => d[f] !== undefined).sort((p, q) => (val(p.d[f]) < val(q.d[f]) ? -1 : val(p.d[f]) > val(q.d[f]) ? 1 : 0) * (dir === 'desc' ? -1 : 1)); }
      if (lim != null) docs = docs.slice(0, lim);
      const pick = d => sel ? Object.fromEntries(Object.entries(d).filter(([k]) => sel.includes(k))) : d;
      return { docs: docs.map(({ id, d }) => ({ id, data: () => clone(pick(d)), ref: refOf(name, id) })), size: docs.length, empty: !docs.length, forEach(f) { this.docs.forEach(f); } };
    }
  };
}
const notFound = () => Object.assign(new Error('5 NOT_FOUND: No document to update'), { code: 5 });
const docRef = (c, id) => Object.assign(refOf(c, id), {
  get: async () => snapOf(refOf(c, id)),
  set: async (v, o) => { col(c).set(id, apply(col(c).get(id), v, !!(o && o.merge), Date.now())); },
  update: async v => { if (!col(c).has(id)) throw notFound(); col(c).set(id, apply(col(c).get(id), v, true, Date.now())); },
  delete: async () => { col(c).delete(id); }
});
const fakeDb = {
  collection: c => Object.assign(query(c, [], null, null, null), { doc: id => docRef(c, id), add: async v => { const id = 'auto' + Math.random().toString(36).slice(2, 10); await docRef(c, id).set(v); return docRef(c, id); } }),
  getAll: async (...a) => Promise.all(a.filter(x => x && x.get).map(r => r.get())),
  runTransaction: async fn => {
    const writes = [];
    const tx = { get: async r => snapOf(r), getAll: async (...rs) => rs.map(snapOf), set: (r, d, o) => { writes.push([r, d, o]); return tx; } };
    const out = await fn(tx); const at = Date.now();
    for (const [r, d, o] of writes) col(r.c).set(r.id, apply(col(r.c).get(r.id), d, !!(o && o.merge), at));
    return out;
  }
};
const fakeAdmin = { firestore: Object.assign(() => fakeDb, { Timestamp: Ts, FieldValue: { serverTimestamp: () => SERVER_TS, increment: inc, delete: () => DEL } }) };

/* the real server modules over it */
const realLoad = Module._load;
Module._load = function (req, ...rest) { if (/[\/]firebaseAdmin(\.js)?$/.test(req)) return fakeAdmin; return realLoad.call(this, req, ...rest); };
const door = require(path.join(root, 'netlify/functions/firebaseOrders.js'));
const reader = require(path.join(root, 'netlify/functions/employeeEfficiency.js'));
const pinDoor = require(path.join(root, 'netlify/functions/_stationPinLogin.js'));
const EP = require(path.join(root, 'netlify/functions/_editPasscode.js'));
const KINDS = require(path.join(root, 'netlify/functions/_activityKinds.js'));
Module._load = realLoad;
pinDoor.deps.sleep = async () => {};
const PASS = 'synthetic-sorting-wired-pass-3c8k';
process.env.EDIT_PASSCODE = PASS;
const displayStation = typeof KINDS.displayStation === 'function' ? KINDS.displayStation : (k => k);   // (before the fold existed: every key as it is)

/* ═══════════════ the shop: made-up people, made-up Employee Numbers, orders ═══════════════ */
const fakePin = () => { for (;;) { const p = String(100000 + Math.floor(Math.random() * 900000)); if (!/^(\d)\1{5}$/.test(p) && !pins.has(p)) return p; } };
const pins = new Map();                                               // PIN -> name (this run only; never printed)
const PEOPLE = { maya: 'Maya Sorter', ivy: 'Ivy Two', noor: 'Noor Late' };
const PIN = { maya: fakePin(), ivy: fakePin(), noor: fakePin() };
for (const k of Object.keys(PEOPLE)) pins.set(PIN[k], PEOPLE[k]);
col('Brites_Orders').set('Employee Numbers', Object.fromEntries([...pins]));
const allPins = () => [...pins.keys()];
const hasPin = text => allPins().some(p => String(text).includes(p));

const A = '3700000001', B = '3700000002', C = '3700000003', D = '3700000004', E = '3700000005', F = '3700000006';
const QTY = { [A + '1']: 1, [A + '2']: 2 };                           // A has two lines, three pieces; the others one piece each
const day = Math.floor(Date.now() / 1000);
const LISTING = n => 1718000000 + n;       // (real listing ids have 9 or 10 digits; a 4 to 8 digit number would be taken for a PIN)
const receipt = rid => ({
  receipt_id: Number(rid), status: 'Paid', message_from_buyer: '', name: 'Test Buyer',
  transactions: (rid === A ? [1, 2] : [1]).map(n => ({ transaction_id: Number(rid) * 10 + n, receipt_id: Number(rid), listing_id: LISTING(n), sku: 'SKU-' + n, title: 'Silver Dog Charm Necklace', quantity: QTY[rid + n] || 1,
    variations: [{ formatted_name: 'Metal', formatted_value: 'Silver' }], expected_ship_date: day + 3 * 86400 }))
});
const NOT_FOUND = new Set([D]);
// the pictures the app already stores (the server finds them; no Etsy)
col('Etsy_Listing_Image_Cache').set(String(LISTING(1)), { images: [{ rank: 1, url_570xN: 'https://i.etsystatic.com/111/r/il/abc/1/il_570xN.1_xyz.jpg' }] });
col('Etsy_Listing_Image_Cache').set(String(LISTING(2)), { images: [{ rank: 1, url_570xN: 'https://i.etsystatic.com/222/r/il/def/2/il_570xN.2_xyz.jpg' }] });

/* ═══════════════ the browser side ═══════════════ */
const ORIGIN = 'http://sorting.test';
const wait = ms => new Promise(r => setTimeout(r, ms));
const say = s => process.stdout.write(s + '\n');
let ipN = 0;
const requests = [];                                                   // every request body that reached the door, for the no-PIN scan
const doorCall = (method, u, text, ip) => door.handler({ httpMethod: method, headers: { 'x-nf-client-connection-ip': ip }, queryStringParameters: Object.fromEntries(u.searchParams), body: text });
const firebaseStub = `
  window.__snap = { cb: null };
  const mk = d => ({ exists: !!d, data: () => d });
  window.firebase = { initializeApp() { return {}; }, firestore() { return { collection() { return { doc() { return {
    onSnapshot(cb) { window.__snap.cb = cb; setTimeout(() => cb(mk(window.__relayDoc)), 0); return () => {}; } }; } }; } }; } };
  window.__fireSnap = d => { window.__relayDoc = d; window.__snap.cb && window.__snap.cb(mk(d)); };`;
const materializeStub = `
  window.__toasts = [];
  const inst = { open() {}, close() {} };
  window.M = { AutoInit() {}, toast(o) { window.__toasts.push(o && o.html); }, Modal: { init() { return inst; }, getInstance() { return inst; } } };`;
const timelineStub = `window.StationTimeline = { init() {}, did() {}, isCancelled() { return false; }, guard() { return Promise.resolve(true); }, scanned() { return Promise.resolve({ cancelled: false }); } };`;
// station-session.js as the page gets it, plus two read-only probes for this test: the page's own signOut handler, and every call of touch()
const sessionProbe = `
;(function () {
  const S = window.StationSession; if (!S) return;
  window.__touches = 0; window.__touchTimes = [];
  const realTouch = S.touch; S.touch = function () { window.__touches++; window.__touchTimes.push(Date.now()); return typeof realTouch === 'function' ? realTouch.apply(this, arguments) : undefined; };
  const realInit = S.init; S.init = function (o) { window.__signOut = o && o.signOut; return realInit.apply(this, arguments); };
})();`;
const js = body => ({ status: 200, contentType: 'text/javascript', body });
const jsonOut = (r, body, status = 200) => r.fulfill({ status, contentType: 'application/json', body: JSON.stringify(body) });

async function deskPage(browser, file, errors, { origin = ORIGIN, relay = null } = {}) {
  const ip = '192.0.2.' + (++ipN);
  const ctx = await browser.newContext({ viewport: { width: 1500, height: 900 } });
  await ctx.route('**/*', async r => {
    const u = new URL(r.request().url());
    if (/gstatic\.com$/.test(u.hostname)) return r.fulfill(js(/firebase-app-compat/.test(u.pathname) ? firebaseStub : ''));
    if (/materialize/.test(u.pathname)) return r.fulfill(js(materializeStub));
    if (/jquery/.test(u.pathname)) return r.fulfill(js(fs.existsSync('/usr/share/javascript/jquery/jquery.min.js') ? fs.readFileSync('/usr/share/javascript/jquery/jquery.min.js') : ''));
    if (u.origin !== origin) return r.abort();
    const p = decodeURIComponent(u.pathname);
    if (p === '/station-session.js') return r.fulfill(js(fs.readFileSync(path.join(root, p.slice(1)), 'utf8') + sessionProbe));
    if (p === '/station-timeline.js') return r.fulfill(js(timelineStub));
    if (p === '/QR Printer.html') return r.fulfill({ status: 200, contentType: 'text/html', body: '<!doctype html><title>printer stub</title>' });
    if (p === '/.netlify/functions/etsyOrderProxy') {
      const id = u.searchParams.get('orderId');
      return NOT_FOUND.has(id) ? jsonOut(r, {}, 404) : jsonOut(r, receipt(id));
    }
    if (p === '/.netlify/functions/firebaseOrders') {
      const text = r.request().postData() || '', m = r.request().method();
      requests.push(m + ' ' + u.pathname + u.search + ' ' + text);
      let b = {}; try { b = JSON.parse(text || '{}'); } catch (_) {}
      if (/employee/i.test(u.searchParams.get('orderId') || '')) return jsonOut(r, { success: false, error: 'closed' }, 401);      // the roster is never read by a page
      if (m === 'POST' && (b.pinLogin !== undefined || b.session || Array.isArray(b.activity) || b.live || (b.orderNumber === 'ScannedSortingOrder'))) {
        const out = await doorCall(m, u, text, ip);
        return r.fulfill({ status: out.statusCode, contentType: 'application/json', body: out.body });
      }
      if (u.searchParams.get('cancelCheck')) return jsonOut(r, { success: true, cancelled: {}, now: Date.now() });
      return jsonOut(r, { success: true, data: {} });
    }
    if (p === '/.netlify/functions/etsyImages') return jsonOut(r, []);
    if (p.startsWith('/.netlify/functions/')) return jsonOut(r, {});
    const f = path.join(root, p);
    if (f.startsWith(root) && fs.existsSync(f) && fs.statSync(f).isFile()) return r.fulfill({ status: 200, path: f });
    return r.fulfill({ status: 404, body: '' });
  });
  await ctx.addInitScript(r => {
    localStorage.setItem('access_token', 'tok'); localStorage.setItem('refresh_token', 'ref');
    localStorage.setItem('token_expires_at', String(Math.floor(Date.now() / 1000) + 7200));
    window.__relayDoc = r;
  }, relay);
  const page = await ctx.newPage();
  page.on('pageerror', e => errors.push(file + ': ' + e.message));
  await page.goto(`${origin}/${file}`);
  return { ctx, page, ip, flush: () => page.evaluate(() => StationActivity.flush()) };
}

/* what the store holds */
const docsOf = c => [...col(c).entries()].map(([id, d]) => Object.assign({ _id: id }, clone(d)));
const eventsOf = (person, device) => docsOf('Station_Activity').filter(e => e.person === person && (!device || e.device === device));
const brief = e => [e.action, e.orderId, e.parts, e.orders, e.person, e.station, e.device];
const sessionsOf = (person, device) => docsOf('Station_Sessions').filter(s => s.person === person && (!device || s.device === device));
const liveDoc = (station, device, person) => col('Station_Live').get(`${station}__${device}__${person}`);
const T = reader._t;
const dropCaches = () => { try { const c = T.cacheOf(fakeDb); c.memo.clear(); c.recent.clear(); c.fails.clear(); } catch (_) {} try { EP.resetCache(); } catch (_) {} };
async function ask(body) {
  dropCaches();
  const r = await T.handle({ httpMethod: 'POST', headers: { 'x-nf-client-connection-ip': '203.0.113.' + (++ipN % 250) }, body: JSON.stringify(Object.assign({ op: 'overview', key: PASS }, body)) }, fakeDb);
  const out = JSON.parse(r.body || '{}');
  assert.strictEqual(r.statusCode, 200, 'reader ' + (body.op || 'overview') + ': ' + String(r.body).slice(0, 300));
  return out;
}
const nyToday = () => new Intl.DateTimeFormat('en-CA', { timeZone: 'America/New_York' }).format(new Date());

(async () => {
  const browser = await chromium.launch({ executablePath: CHROME, args: ['--no-sandbox'] });
  const errors = [];
  const typeBatch = async (page, ids) => { await page.fill('#etsyOrderNumber', ids.join(',')); await page.press('#etsyOrderNumber', 'Enter'); };
  const tilesFor = (page, n) => page.waitForFunction(n => (window.cachedOrderItems || []).length === n && document.getElementById('previewCell' + (n - 1)), n, { timeout: 15000 });
  const cellOf = (page, rid) => page.evaluate(rid => window.cachedOrderItems.findIndex(t => String(t.receipt_id) === rid), rid);
  const signInByNumber = async (page, pin, name) => {
    await page.click('#sortingAsChip .st-as-name');
    await page.waitForSelector('#sortingAsChip .st-as-input:not([hidden])');
    await page.keyboard.type(pin); await page.keyboard.press('Enter');
    await page.waitForFunction(n => StationActivity.who() && StationActivity.who().person === n, name, { timeout: 8000 });
  };
  const until = async (fn, what, ms = 8000) => { const t0 = Date.now(); for (;;) { const v = await fn(); if (v) return v; if (Date.now() - t0 > ms) throw new Error('timed out waiting for ' + what); await wait(80); } };
  const chipText = page => page.evaluate(() => document.querySelector('#sortingAsChip .st-as-name').textContent);
  const toasts = page => page.evaluate(() => (window.__toasts || []).slice());
  let desk1, desk2;

  /* ───────────── 1 · sorting.html ───────────── */
  {
    desk1 = await deskPage(browser, 'sorting.html', errors);
    const { page, flush } = desk1;
    await page.waitForFunction(() => window.SortTL && window.StationActivity && window.StationSession && document.querySelector('#sortingAsChip .st-as-name') && window.__snap && window.__snap.cb && window.__signOut);
    // the sign-in goes through the server's PIN door and keeps the NAME only
    await signInByNumber(page, PIN.maya, PEOPLE.maya);
    const who = await page.evaluate(() => StationActivity.who());
    assert.deepStrictEqual([who.person, who.station, who.device], [PEOPLE.maya, 'sorting', 'sorting-1']);
    const sess = (await until(async () => sessionsOf(PEOPLE.maya, 'sorting-1')[0], 'the session in Station_Sessions'));
    assert.strictEqual(sess.station, 'sorting'); assert.strictEqual(sess.device, 'sorting-1'); assert(!sess.employeeId, 'no id (a PIN) is stored with the session');
    assert(sess.startAt && sess.endAt == null, 'the session is open');
    // a beat (the page says so on pagehide and every 5 minutes): the server moves lastSeenAt
    const seen0 = sess.lastSeenAt;
    await wait(30);
    await page.evaluate(() => window.dispatchEvent(new Event('pagehide')));
    await until(async () => { const s = sessionsOf(PEOPLE.maya, 'sorting-1')[0]; return s && val(s.lastSeenAt) >= val(seen0); }, 'a beat'); // (never earlier; a beat in the same ms is fine)
    say('sorting.html: the PIN door signs Maya in by name; a session with a beat is in Station_Sessions');

    // a typed batch: one scan per order with its pieces; the live order is the batch; the first sticker of an order is a print + the order sorted once
    await typeBatch(page, [A, C]); await tilesFor(page, 3);
    await page.waitForFunction(() => StationActivity.pending() >= 2);
    await flush();
    assert.deepStrictEqual(eventsOf(PEOPLE.maya).filter(e => e.action === 'scan').map(brief).sort(), [['scan', A, 3, 0, PEOPLE.maya, 'sorting', 'sorting-1'], ['scan', C, 1, 0, PEOPLE.maya, 'sorting', 'sorting-1']].sort());
    await until(async () => liveDoc('sorting', 'sorting-1', PEOPLE.maya), 'the live order');
    let live = liveDoc('sorting', 'sorting-1', PEOPLE.maya);
    assert.strictEqual(live.state, 'working'); assert.strictEqual(live.kind, 'sheet'); assert.strictEqual(live.title, 'Batch of 2 orders');
    assert.strictEqual(live.pieces.length, 4, "a piece for every unit of both orders"); assert(live.pieces.every(p => p.listingId), "with its listing, so the board finds the picture");
    const iA = await cellOf(page, A);
    await page.evaluate(i => openIframePrinterForListing(i), iA);
    await page.waitForFunction(() => [...document.querySelectorAll('iframe')].some(f => /QR/.test(f.src)));
    await flush();
    assert.deepStrictEqual(eventsOf(PEOPLE.maya).filter(e => e.action === 'print' || e.action === 'complete').map(brief), [['print', A, 3, 0, PEOPLE.maya, 'sorting', 'sorting-1'], ['complete', A, 3, 1, PEOPLE.maya, 'sorting', 'sorting-1']]);
    say('sorting.html: scan per order, sticker = print + the order sorted once, the batch is the live order');
    desk1.cellA = iA;
  }

  /* ───────────── 2 · the phone scanner and the relay ───────────── */
  {
    const { page, flush } = desk1;
    // sort-scan.html: the real page, its real Send button, the real door
    const sc = await deskPage(browser, 'sort-scan.html', errors);
    await sc.page.addInitScript(() => {});
    await sc.page.waitForFunction(() => typeof handleScannedCode === 'function' && document.getElementById('sendBtn'));
    const b36 = n => BigInt(n).toString(36);
    const code = `B36|silver|${b36(E)}.${b36(F)}`;
    await sc.page.evaluate(c => handleScannedCode(c), code);
    assert.strictEqual(await sc.page.evaluate(() => document.getElementById('orderNumInput').value), [E, F].join(','), 'the scanner decodes the sheet QR');
    await sc.page.click('#sendBtn');
    const relay = await until(async () => col('Brites_Orders').get('ScannedSortingOrder'), 'the relay document written by the door');
    assert.strictEqual(relay['Order Number'], [E, F].join(','));
    assert(relay['Shipping Label Timestamps'], 'with the time it was sent');
    assert(!hasPin(JSON.stringify(relay)), 'no PIN in the relay');
    // the desk hears it (Firestore would push the document): loaded, credited to Maya, counted as input
    const touches0 = await page.evaluate(() => window.__touches);
    await page.evaluate(d => __fireSnap(d), { 'Order Number': relay['Order Number'], 'Shipping Label Timestamps': relay['Shipping Label Timestamps'] });
    await tilesFor(page, 2);
    await page.waitForFunction(() => StationActivity.pending() >= 2);
    await flush();
    const sheet = eventsOf(PEOPLE.maya).filter(e => e.action === 'scan' && e.detail === 'sheet QR');
    assert.deepStrictEqual(sheet.map(e => e.orderId).sort(), [E, F], 'the phone scan is the signed-in person\'s scan, two orders');
    assert(sheet.every(e => e.person === PEOPLE.maya && e.device === 'sorting-1' && e.station === 'sorting' && e.parts === 1));
    assert.strictEqual(await page.evaluate(() => window.__touches), touches0 + 1, 'the phone scan is input at the station (StationSession.touch once)');
    await until(async () => { const l = liveDoc('sorting', 'sorting-1', PEOPLE.maya); return l && l.title === 'Batch of 2 orders' && l.note === 'sheet QR'; }, 'the live order says sheet QR');
    // the same batch again (another field of the relay document changed, or a reload): not loaded again, no second scan, no touch
    const n0 = eventsOf(PEOPLE.maya).filter(e => e.action === 'scan').length;
    await page.evaluate(d => __fireSnap(d), { 'Order Number': relay['Order Number'], 'Shipping Label Timestamps': relay['Shipping Label Timestamps'], 'Client Name': 'x' });
    await wait(400); await flush();
    assert.strictEqual(eventsOf(PEOPLE.maya).filter(e => e.action === 'scan').length, n0, 'a repeat of the batch is no new scan');
    assert.strictEqual(await page.evaluate(() => window.__touches), touches0 + 1, 'and no new input');
    say('sort-scan.html -> sorting.html: the relayed batch is credited to the signed-in person of the desk and counts as input; a repeat is ignored');
    await sc.ctx.close();
  }


  /* ───────────── 3 · the sign-in ends by itself: the page's own signOut for every reason ───────────── */
  {
    const { page, flush } = desk1;
    const onScreen = () => page.evaluate(() => ({ items: (window.cachedOrderItems || []).length, tile: !!document.getElementById('previewCell0'), field: document.getElementById('etsyOrderNumber').value }));
    // the page's own handler, called the way station-session.js calls it: login cleared, its own sign-in again, one plain line, the work stays
    const said = async (reason, text) => {
      const was = await onScreen();
      await page.evaluate(() => { window.__toasts.length = 0; });
      await page.evaluate(r => window.__signOut(r), reason);
      assert.strictEqual(await chipText(page), 'Set your name', reason + ': the page shows its own sign-in again');
      assert.strictEqual(await page.evaluate(() => localStorage.getItem('sorting.employee')), null, reason + ': its login is cleared');
      assert.deepStrictEqual(await toasts(page), [text], reason + ': one calm line, in plain words');
      assert.deepStrictEqual(await onScreen(), was, reason + ': the batch on screen and the order field stay');
    };
    const sessId = (await page.evaluate(() => StationSession.current())).id;
    const was0 = await onScreen(); assert.strictEqual(was0.items, 2); assert(was0.tile);
    await said('idle', 'Signed out after 10 minutes without input. Click “Set your name” to sign in again.');
    // (the page told StationSession by clearing its login: the next look at it, a focus or its 30 s tick, closes the session; the live order goes)
    await page.evaluate(() => window.dispatchEvent(new Event('focus')));
    await until(async () => { const s = col('Station_Sessions').get(sessId); return s && s.endAt != null; }, 'the session ends');
    assert.strictEqual(await page.evaluate(() => StationActivity.who()), null, 'nobody is signed in any more');
    await until(async () => { const l = liveDoc('sorting', 'sorting-1', PEOPLE.maya); return l && l.state === 'idle'; }, 'the live order goes (an idle for Maya)', 12000);
    say('sorting.html: signOut("idle"): its own login cleared, "Set your name" again, one plain line, the batch stays; the session ends and the live order goes');

    // nobody is signed in: a TYPED batch and a PHONE batch record nothing yet; both are kept (20 minutes) and load on screen at once
    // (7 Oct 2026: a sticker's Print now asks for the name, so the person who signs in there is credited with the batch they were working on)
    const n0 = docsOf('Station_Activity').length;
    await typeBatch(page, [A, C]); await tilesFor(page, 3); await wait(300); await flush();
    assert.strictEqual(docsOf('Station_Activity').length, n0, 'a typed batch with nobody signed in records nothing yet');
    assert.strictEqual(await page.evaluate(() => SortTL.kept()), 1, 'but it is kept for the next sign-in');
    await page.evaluate(d => __fireSnap(d), { 'Order Number': [B, F].join(','), 'Shipping Label Timestamps': new Date().toISOString() });
    await tilesFor(page, 2); await wait(300); await flush();
    assert.strictEqual(docsOf('Station_Activity').length, n0, 'a phone batch with nobody signed in records nothing yet');
    assert.strictEqual(await page.evaluate(() => SortTL.kept()), 2, 'it is kept for the next sign-in too');
    assert.strictEqual((await onScreen()).items, 2, 'and it loaded on screen at once: nothing is blocked');
    // the person signs in: the kept scans are theirs, and the batch is in their hand
    await signInByNumber(page, PIN.noor, PEOPLE.noor);
    await flush();
    assert.deepStrictEqual(eventsOf(PEOPLE.noor).filter(e => e.action === 'scan').map(e => [e.orderId, e.detail, e.parts]).sort(), [[A, 'typed', 3], [B, 'sheet QR', 1], [C, 'typed', 1], [F, 'sheet QR', 1]].sort(), 'the kept scans (typed and phone) are recorded under the person who signed in');
    assert.strictEqual(await page.evaluate(() => SortTL.kept()), 0, 'and are not kept any more');
    await until(async () => { const l = liveDoc('sorting', 'sorting-1', PEOPLE.noor); return l && l.state === 'working' && l.title === 'Batch of 2 orders'; }, 'the kept batch is in hand for the new person');
    assert(!eventsOf(PEOPLE.maya).some(e => (e.orderId === B) && e.action === 'scan'), 'nothing of it went to the person before');
    say('sorting.html: a phone or typed batch that loads with nobody signed in is kept and credited to the person who signs in next');

    // kept, then the sign-in day ends: what was kept is never put under the next name
    await said('closing', 'Signed out at 5:00 pm. Click “Set your name” to sign in again.');
    await page.evaluate(() => window.dispatchEvent(new Event('focus')));
    await until(async () => (sessionsOf(PEOPLE.noor, 'sorting-1')[0] || {}).endAt != null, 'Noor\'s session ends');
    await page.evaluate(d => __fireSnap(d), { 'Order Number': C, 'Shipping Label Timestamps': new Date(Date.now() + 1000).toISOString() });
    await tilesFor(page, 1); await wait(300);
    assert.strictEqual(await page.evaluate(() => SortTL.kept()), 1, 'a phone batch after the sign-out is kept');
    await page.evaluate(() => window.__signOut('midnight'));
    assert.strictEqual(await page.evaluate(() => SortTL.kept()), 0, 'a sign-out drops what was kept (never put under the next name)');
    await signInByNumber(page, PIN.maya, PEOPLE.maya); await flush(); await wait(200); await flush();
    assert(!eventsOf(PEOPLE.maya).some(e => e.orderId === C && e.detail === 'sheet QR'), 'so the next person is not credited with it');
    // the other words
    await said('midnight', 'Signed out at midnight. Click “Set your name” to sign in for the new day.');
    await signInByNumber(page, PIN.maya, PEOPLE.maya);
    await said(undefined, 'Signed out at midnight. Click “Set your name” to sign in for the new day.');     // (no reason: the old call, unchanged)
    await signInByNumber(page, PIN.maya, PEOPLE.maya);
    await said('something-new', 'Signed out. Click “Set your name” to sign in again.');
    say('sorting.html: signOut("closing" | "midnight" | none | a reason it does not know): the right plain words each time');
    // Maya is back in for the rest of the test
    await signInByNumber(page, PIN.maya, PEOPLE.maya);
  }

  /* ───────────── 4 · sorting-2.html ───────────── */
  {
    const old = { 'Order Number': A, 'Shipping Label Timestamps': new Date(Date.now() - 3600e3).toISOString() };   // a batch the phone sent an hour ago
    desk2 = await deskPage(browser, 'sorting-2.html', errors, { relay: old });
    const { page, flush } = desk2;
    await page.waitForFunction(() => window.SortAct && window.StationActivity && document.querySelector('#sortingAsChip .st-as-name') && window.__snap && window.__snap.cb && window.__signOut);
    await wait(500);
    // opening the page does not load the old phone batch again (it waits in the field, as on sorting.html)
    assert.strictEqual(await page.evaluate(() => (window.cachedOrderItems || []).length), 0, 'sorting-2: an old phone batch is not loaded again on open');
    assert.strictEqual(await page.evaluate(() => document.getElementById('etsyOrderNumber').value), A, 'sorting-2: its numbers wait in the field');
    await signInByNumber(page, PIN.ivy, PEOPLE.ivy);
    const who = await page.evaluate(() => StationActivity.who());
    assert.deepStrictEqual([who.person, who.station, who.device], [PEOPLE.ivy, 'sorting', 'sorting-2']);
    await until(async () => sessionsOf(PEOPLE.ivy, 'sorting-2')[0], 'sorting-2 has its session');
    assert.strictEqual(eventsOf(PEOPLE.ivy).length, 0, 'sorting-2: the sign-in alone logs no action');
    // typed batch, sticker
    await typeBatch(page, [A, C]); await tilesFor(page, 3);
    const iA = await cellOf(page, A);
    await page.evaluate(i => openIframePrinterForListing(i), iA);
    await page.waitForFunction(() => [...document.querySelectorAll('iframe')].some(f => /QR/.test(f.src)));
    await flush();
    assert.deepStrictEqual(eventsOf(PEOPLE.ivy, 'sorting-2').map(brief).sort(), [['complete', A, 3, 1, PEOPLE.ivy, 'sorting', 'sorting-2'], ['print', A, 3, 0, PEOPLE.ivy, 'sorting', 'sorting-2'],
      ['scan', A, 3, 0, PEOPLE.ivy, 'sorting', 'sorting-2'], ['scan', C, 1, 0, PEOPLE.ivy, 'sorting', 'sorting-2']].sort(), 'sorting-2: scans, a print and the order sorted once, with the person, station and device');
    await until(async () => liveDoc('sorting', 'sorting-2', PEOPLE.ivy), 'sorting-2 live order');
    assert.strictEqual(liveDoc('sorting', 'sorting-2', PEOPLE.ivy).title, 'Batch of 2 orders');
    say('sorting-2.html: PIN sign-in by name (device sorting-2), scans, print, order sorted once, live order');
    // a fresh phone batch: loaded, Ivy's, input
    const fresh = { 'Order Number': [E, F].join(','), 'Shipping Label Timestamps': new Date().toISOString() };
    const t0 = await page.evaluate(() => window.__touches);
    await page.evaluate(d => __fireSnap(d), fresh);
    await tilesFor(page, 2); await page.waitForFunction(() => StationActivity.pending() >= 2); await flush();
    assert.deepStrictEqual(eventsOf(PEOPLE.ivy, 'sorting-2').filter(e => e.detail === 'sheet QR').map(e => e.orderId).sort(), [E, F], 'sorting-2: the phone scan is the signed-in person\'s');
    assert.strictEqual(await page.evaluate(() => window.__touches), t0 + 1, 'sorting-2: the phone scan is input at the station');
    // a reload does not load it again (before: every reload loaded the last batch once more, a second scan for each order, under whoever was signed in)
    await desk2.ctx.addInitScript(d => { window.__relayDoc = d; }, fresh);
    const scans0 = eventsOf(PEOPLE.ivy).filter(e => e.action === 'scan').length;
    await page.reload();
    await page.waitForFunction(() => window.SortAct && window.__snap && window.__snap.cb && StationActivity.who());
    await wait(700); await flush();
    assert.strictEqual(await page.evaluate(() => (window.cachedOrderItems || []).length), 0, 'sorting-2: a reload does not load the last phone batch again');
    assert.strictEqual(eventsOf(PEOPLE.ivy).filter(e => e.action === 'scan').length, scans0, 'so no phantom scans are credited');
    assert.strictEqual(await page.evaluate(() => StationSession.current() && StationSession.current().person), PEOPLE.ivy, 'and the session went on (a reload within 15 minutes keeps it)');
    say('sorting-2.html: a reload no longer loads the old phone batch again (no phantom scans)');
    // the sign-in ends by itself
    await typeBatch(page, [A, C]); await tilesFor(page, 3);
    const same = async (reason, text) => {
      await page.evaluate(() => { window.__toasts.length = 0; });
      const items = await page.evaluate(() => (window.cachedOrderItems || []).length);
      await page.evaluate(r => window.__signOut(r), reason);
      assert.strictEqual(await chipText(page), 'Set your name', 'sorting-2 ' + reason + ': its own sign-in again');
      assert.strictEqual(await page.evaluate(() => localStorage.getItem('sorting.employee')), null);
      assert.deepStrictEqual(await toasts(page), [text], 'sorting-2 ' + reason + ': one plain line');
      assert.strictEqual(await page.evaluate(() => (window.cachedOrderItems || []).length), items, 'sorting-2 ' + reason + ': the batch stays on screen');
    };
    await same('idle', 'Signed out after 10 minutes without input. Click “Set your name” to sign in again.');
    await page.evaluate(() => window.dispatchEvent(new Event('focus')));
    await until(async () => sessionsOf(PEOPLE.ivy, 'sorting-2').every(s => s.endAt != null), 'sorting-2: the session ends');
    // a phone batch with nobody signed in is kept for the next sign-in; a typed one is not
    await flush(); await wait(300);                                    // the two typed scans from just above go out first
    const n1 = docsOf('Station_Activity').length;
    await page.evaluate(d => __fireSnap(d), { 'Order Number': [F].join(','), 'Shipping Label Timestamps': new Date(Date.now() + 2000).toISOString() });
    await tilesFor(page, 1); await wait(300); await flush();
    assert.strictEqual(docsOf('Station_Activity').length, n1, 'sorting-2: nothing recorded for nobody');
    assert.strictEqual(await page.evaluate(() => SortAct.kept()), 1, 'sorting-2: the phone batch is kept');
    await signInByNumber(page, PIN.ivy, PEOPLE.ivy); await flush();
    assert.deepStrictEqual(eventsOf(PEOPLE.ivy, 'sorting-2').filter(e => e.detail === 'sheet QR' && e.orderId === F).length >= 2, true, 'sorting-2: the kept phone scan is credited to the person who signed in (F again, after E and F)');
    await same('closing', 'Signed out at 5:00 pm. Click “Set your name” to sign in again.');
    await signInByNumber(page, PIN.ivy, PEOPLE.ivy);
    await same('midnight', 'Signed out at midnight. Click “Set your name” to sign in for the new day.');
    await signInByNumber(page, PIN.ivy, PEOPLE.ivy);
    say('sorting-2.html: signOut idle / closing / midnight: its own sign-in again, one plain line, the batch stays; a kept phone batch goes to the next person');
  }

  /* ───────────── 5 · the Charm Sorter, the nesting part of Sorting ───────────── */
  let SORTER_RID;
  {
    const srv = await start({ receipts: [] });
    const ip = '192.0.2.' + (++ipN);
    try {
      const context = await browser.newContext({ viewport: { width: 1440, height: 950 } });
      const jsH = body => ({ status: 200, contentType: 'text/javascript', headers: { 'Cross-Origin-Resource-Policy': 'cross-origin', 'Access-Control-Allow-Origin': '*' }, body });
      const PDFMAKE = `window.pdfMake = { createPdf(dd) { return { getBlob(cb) { cb(new Blob(['<!doctype html><title>label</title><script>window.print = () => {};<\\/script>'], { type: 'text/html' })); } }; } };`;
      await context.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => {
        const u = r.request().url();
        if (/cdn\.jsdelivr\.net\/npm\/pdfmake@[^/]+\/build\/pdfmake/.test(u)) return r.fulfill(jsH(PDFMAKE));
        if (/cdn\.jsdelivr\.net\/npm\/pdfmake@[^/]+\/build\/vfs_fonts/.test(u)) return r.fulfill(jsH(''));
        if (/qrcodejs/.test(u)) return r.fulfill(jsH(fs.readFileSync(path.join(root, 'lib/qrcode.min.js'))));
        if (/fonts\.googleapis|fonts\.gstatic/.test(u)) return r.fulfill({ status: 200, contentType: 'text/css', body: '' });
        return r.abort();
      });
      // the stations' door is the REAL one (sessions, activity, live over the one store); the rest is the sorter's own test server
      await context.route(u => /\/\.netlify\/functions\/firebaseOrders/.test(u.pathname), async r => {
        const text = r.request().postData() || ''; let b = null; try { b = JSON.parse(text || 'null'); } catch (_) {}
        if (r.request().method() === 'POST' && b && (b.session || Array.isArray(b.activity) || b.live)) {
          requests.push('POST sorter ' + text);
          const u = new URL(r.request().url()); const out = await doorCall('POST', u, text, ip);
          return r.fulfill({ status: out.statusCode, contentType: 'application/json', headers: { 'Access-Control-Allow-Origin': '*' }, body: out.body });
        }
        return r.fallback();
      });
      await context.route(u => /\/station-session\.js/.test(u.pathname), r => r.fulfill(jsH(fs.readFileSync(path.join(root, 'station-session.js'), 'utf8') + sessionProbe)));
      // (this file sets a synthetic EDIT_PASSCODE for the reader, so the sorter's own functions are gated too: the page is given the same synthetic one)
      await context.addInitScript(p => { try { localStorage.setItem('cn.settings', JSON.stringify({ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on' })); sessionStorage.setItem('cn.passcode', p); } catch (_) {} }, PASS);
      const page = await context.newPage();
      page.setDefaultTimeout(30000);
      page.on('pageerror', e => errors.push('charm-nest-1.html: ' + e.message));
      await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
      const NEED = ['CN', 'Orders', 'Review', 'CustomPrint', 'Seal', 'OrderWin', 'CNAct', 'StationActivity', 'StationSession', '__signOut'];
      try { await page.waitForFunction(n => n.every(k => window[k]) && CN.S.cloud.ok === true, NEED, { timeout: 60000 }); }
      catch (e) { throw new Error('the sorter did not come up; missing: ' + JSON.stringify(await page.evaluate(n => n.filter(k => !window[k]).concat(window.CN && CN.S && CN.S.cloud ? ['cloud.ok=' + CN.S.cloud.ok + ' ' + CN.S.cloud.msg] : []), NEED)) + ' errors: ' + errors.join(' | ')); }
      const RID = '4176576272', KEY = '4176576272_41765762721', SHIP = Math.floor(Date.UTC(2026, 9, 2, 17) / 1000), DAY = 86400;
      SORTER_RID = RID;
      const ORDERS = [{ receiptId: RID, orderNumber: RID, createTs: SHIP - 5 * DAY, updateTs: SHIP - 5 * DAY + 60, shipBy: SHIP, buyer: { name: 'Jessica Strom' }, buyerMessage: '', isGift: false, giftMessage: '', staffNote: '', messages: [],
        lines: [{ transactionId: '41765762721', listingId: String(LISTING(1)), sku: 'RE_5460', title: 'MODIFICATION REWORK FREE SHIPPING', quantity: 2, expectedShipDate: SHIP, variations: [{ name: 'Price', value: '144' }], metalKey: '', metalLabel: '', personalization: '' }] }];
      await page.evaluate(async orders => {
        await Orders.loadMaps(true);
        for (const order of orders) for (const line of order.lines) { const key = CharmNestOrders.lineKey(order, line); const row = { key, order, line, arrivedAt: Date.now(), spec: null, problems: [], state: 'pulled', reason: null, claimedBy: null, poolIds: [], engrave: null, material: null }; B.orders.rows.push(row); B.orders.byKey.set(key, row); }
        Orders.interpretAll(); Review.syncOrderItems(); CN.setMode('review'); Review.render();
      }, ORDERS);
      await page.click('#reviewView .egTab[data-k="customOrder"]');
      const card = `#rvList .reviewListRow[data-rid="${RID}"]`;
      const settle = () => page.waitForFunction(() => !document.querySelector('.cuStat, .btn.working, .cuSealHost, #motionLayer .mGhost, .sealTool, .seal.pending'), null, { timeout: 15000 });
      const flush = () => page.evaluate(() => StationActivity.flush());
      const dismiss = () => page.evaluate(() => document.querySelectorAll('.mNote').forEach(n => n.close()));
      // the typed name is the person: Maya types her name here too (the same person works at sorting-1 and in the sorter)
      assert.strictEqual(await page.evaluate(() => StationActivity.who()), null, 'the sorter: nobody until a name is set');
      // (LD1's "Laser or Design?" question, once it is on the page, is asked of everybody but the Admin: this person is the Admin, so the station stays the Sorter's)
      await page.evaluate(() => { if (window.CNRole && typeof CNRole.setAdminLookup === 'function') CNRole.setAdminLookup(async () => true); });
      await page.evaluate(n => { B.employee = n; localStorage.setItem('cn.employee', n); }, PEOPLE.maya);
      const w = await page.evaluate(() => StationActivity.who());
      assert.deepStrictEqual([w.person, w.station, w.device], [PEOPLE.maya, 'sorter', 'charm-nest-1'], 'the typed name signs in at the sorter');
      const sSess = await until(async () => sessionsOf(PEOPLE.maya, 'charm-nest-1')[0], 'the sorter session');
      assert(sSess.station === 'sorter' && !sSess.employeeId, 'a session for the name, no PIN');
      // Print QR label: a print, and the order completed once
      await page.click(card + ' [data-cu-print]');
      await page.waitForFunction(k => B.maps.customDone[k], KEY); await settle(); await dismiss(); await flush();
      assert.deepStrictEqual(eventsOf(PEOPLE.maya, 'charm-nest-1').filter(e => e.action === 'print' || e.action === 'complete').map(brief).sort(),
        [['complete', RID, 2, 1, PEOPLE.maya, 'sorter', 'charm-nest-1'], ['print', RID, 2, 0, PEOPLE.maya, 'sorter', 'charm-nest-1']].sort(), 'the sorter: Print QR label is a print and the order completed once, with the person');
      // the open order window is the live order (pieces, QR text for the board)
      await page.evaluate(k => OrderWin.open(k), KEY);
      await until(async () => { const l = liveDoc('sorter', 'charm-nest-1', PEOPLE.maya); return l && l.state === 'working' && l.rid === RID; }, 'the sorter live order', 12000);
      const sl = liveDoc('sorter', 'charm-nest-1', PEOPLE.maya);
      assert(sl.pieces.length >= 1 && sl.pieces.every(p => p.listingId), 'the sorter: the live order has its pieces and their listing');
      say('charm-nest-1.html (the Sorter): the typed name signs in at station sorter, Print QR label = print + order completed once, the open order is the live order');
      // the sign-in ends by itself: the right plain words, the sheets and windows stay
      await page.evaluate(() => { const d = document.getElementById('orderWin'); if (d && d.open) d.close(); });
      const sayIt = async (reason, text) => {
        await page.evaluate(() => { window.__lastToast = ''; const t = CN.toast; if (!window.__toastWrapped) { window.__toastWrapped = true; CN.toast = function (m) { window.__lastToast = String(m); return t.apply(this, arguments); }; } });
        await page.evaluate(r => window.__signOut(r), reason);
        assert.strictEqual(await page.evaluate(() => localStorage.getItem('cn.employee')), null, 'the sorter ' + reason + ': its name is cleared');
        assert.strictEqual(await page.evaluate(() => (window.B && B.employee) || ''), '', 'the sorter ' + reason + ': the name is not kept in memory');
        assert.strictEqual(await page.evaluate(() => window.__lastToast), text + ' Your name is asked again at your next approval, label or decision.', 'the sorter ' + reason + ': one plain line');
        assert.strictEqual(await page.evaluate(() => !!document.getElementById('reviewView') && CN.S.cloud.ok === true), true, 'the sorter ' + reason + ': the page carries on');
      };
      await sayIt('idle', 'Signed out after 10 minutes without input.');
      await page.evaluate(() => window.dispatchEvent(new Event('focus')));
      await until(async () => sessionsOf(PEOPLE.maya, 'charm-nest-1').every(s => s.endAt != null), 'the sorter session ends');
      await page.evaluate(n => { B.employee = n; }, PEOPLE.maya);
      await sayIt('closing', 'Signed out at 5:00 pm.');
      await page.evaluate(n => { B.employee = n; }, PEOPLE.maya);
      await sayIt('midnight', 'Signed out at midnight.');
      say('charm-nest-1.html: signOut idle / closing / midnight: the right plain words, the name cleared, the page carries on');
      // Maya is back in the sorter for the portal numbers
      await page.evaluate(n => { B.employee = n; }, PEOPLE.maya);
      await flush();
      await context.close();
    } finally { srv.close(); }
  }

  /* ───────────── 6 · the portal numbers agree ───────────── */
  {
    await wait(300);
    const today = nyToday();
    const live = await ask({ op: 'live' });
    const ov = await ask({ op: 'overview', day: today, days: 1, trend: false });
    // what the store holds, per person, folded by displayStation (the Sorter's rows are Sorting's)
    const SORTING = new Set(['sorting', 'sorter', 'qr']);
    const expect = person => {
      const evs = docsOf('Station_Activity').filter(e => e.person === person && SORTING.has(e.station) && e.day === today);
      const parts = evs.filter(e => e.action === 'complete').reduce((n, e) => n + e.parts, 0) - evs.filter(e => e.action === 'undo').reduce((n, e) => n + e.parts, 0);
      return { parts: Math.max(0, parts), scans: evs.filter(e => e.action === 'scan').length, orders: new Set(evs.filter(e => e.orderId).map(e => e.orderId)).size };
    };
    const folded = rows => { const o = { parts: 0, scans: 0, orders: 0 }; for (const r of rows || []) if (displayStation(r.station || r.key) === 'sorting') { o.parts += r.parts || 0; o.scans += r.scans || 0; o.orders += r.orders || 0; } return o; };
    const mayaIn = ov.people.find(p => p.name === PEOPLE.maya);
    assert(mayaIn, 'the Overview lists Maya');
    const ovMaya = folded(mayaIn.stations);
    const pr = await ask({ op: 'person', name: PEOPLE.maya, range: 'day', day: today });
    assert.strictEqual(pr.ok, true);
    const prMaya = folded(pr.stations);
    const exp = expect(PEOPLE.maya);
    assert.deepStrictEqual(ovMaya, exp, 'Overview: Maya\'s Sorting pieces, scans and orders are what her events say');
    assert.deepStrictEqual(prMaya, exp, 'person view: the same');
    // the board's Sorting card: everyone's pieces, scans, orders today
    const sortingPeople = [...new Set(docsOf('Station_Activity').filter(e => SORTING.has(e.station) && e.day === today).map(e => e.person))];
    const all = sortingPeople.map(expect);
    const expAll = { parts: all.reduce((n, o) => n + o.parts, 0), scans: all.reduce((n, o) => n + o.scans, 0) };
    const cards = (live.stations || []).filter(s => displayStation(s.key) === 'sorting');
    assert(cards.length >= 1, 'the board has a Sorting card');
    const boardAll = { parts: cards.reduce((n, s) => n + ((s.counts && s.counts.partsToday) || 0), 0), scans: cards.reduce((n, s) => n + ((s.counts && s.counts.scansToday) || 0), 0) };
    assert.deepStrictEqual(boardAll, expAll, 'the board\'s Sorting pieces and scans today are what the events say');
    const ovAll = (ov.business.stations || []).filter(s => displayStation(s.station) === 'sorting').reduce((o, s) => ({ parts: o.parts + (s.parts || 0), scans: o.scans + (s.scans || 0) }), { parts: 0, scans: 0 });
    assert.deepStrictEqual(ovAll, expAll, 'the Overview\'s Sorting card says the same');
    say('portal: the board, the Overview and the person view agree with the events (Sorting pieces ' + expAll.parts + ', scans ' + expAll.scans + ')');
    // the live card: person, order, thumbnails, QR
    const cur = cards.flatMap(s => s.current || []).filter(c => c.person === PEOPLE.ivy || c.person === PEOPLE.maya);
    assert(cur.length >= 1, 'the board shows a current order at Sorting');
    const ivyCur = cards.flatMap(s => s.current || []).find(c => c.person === PEOPLE.ivy && c.device === 'sorting-2');
    if (ivyCur) {
      assert(ivyCur.pieces.length >= 1 && ivyCur.pieces.some(p => p.thumbUrl), 'a batch card has its pieces with their thumbnails: ' + JSON.stringify(ivyCur).slice(0, 600));
    }
    const mayaSorter = cards.flatMap(s => s.current || []).find(c => c.person === PEOPLE.maya && c.device === 'charm-nest-1');
    if (mayaSorter) assert(mayaSorter.qr && mayaSorter.qr.text === SORTER_RID, 'an order card has its QR text');
    // no PIN anywhere: not in any request but the login door's, not in any stored document, not in what the reader says
    const stored = JSON.stringify([...colls].filter(([n]) => n !== 'Brites_Orders').map(([n, m]) => [n, [...m]]));
    assert(!hasPin(stored), 'no PIN in any stored document');
    assert(!hasPin(JSON.stringify(live)) && !hasPin(JSON.stringify(ov)) && !hasPin(JSON.stringify(pr)), 'no PIN in anything the portal reads');
    assert(requests.filter(t => hasPin(t)).every(t => /^POST \/\.netlify\/functions\/firebaseOrders\S* \{"pinLogin":"\d{6}"\}$/.test(t)), 'a PIN is only ever in the login door\'s own request');
    say('portal: the live card has its pieces and thumbnails and QR text; no PIN anywhere');
  }

  assert.deepStrictEqual(errors, [], 'no page errors: ' + errors.join(' | '));
  await browser.close();
  say('sorting-wired: all passed');
})().catch(e => { console.error(e); process.exit(1); });
