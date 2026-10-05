// A completed order's seal in the Design Station's archived-order pop-up (Paul, 5 Oct 2026: "The 'Complete' orders/pieces are
// missing their associated stamp/seal. Please review all modals and fix."), on both Design pages, with the real
// order-timeline.js and station-seals.js and the Charm Sorter's own Seal component (charm-nest-motion.js), against a fake
// backend that answers the order's timeline (timelineGet) and the archive record.
//   · an order the sorter completed by hand and printed (three seals on two lines) shows ONE small seal, the latest
//     Complete Order press (ORDER COMPLETE, its person, date and time), and "+2"; it zooms in place; no title or tooltip
//   · an order whose timeline holds no seal shows none, and the seal script is not even fetched; a timeline that cannot be
//     read shows none; the last order's seal never stays under the next order's number
//   · a print-only order shows the blue QR LABEL PRINTED seal and no "+N"
//   · one timeline read per opening, no Etsy call, no polling; the dialogs stay the browser's own (Motion is not put on the page)
//   · a seal script that does not load leaves the pop-up exactly as it was (and the next opening tries again)
//   · at phone width the header takes the seal without a sideways scroll
//   · mutants: the page not asking for the seal, and the module drawing nothing, are both caught by the same check
// Every /.netlify/functions call goes to a fake in this file; every other host is aborted (stand-ins for the CDN scripts).
//   NODE_PATH=$(npm root -g) PW_DIR=$(npm root -g)/playwright/node_modules CHROMIUM=… node tests/stations/completed-seals-station-modals.cjs
//   SHOTS=/some/dir  also writes the screenshots there (fixture only)
'use strict';
const http = require('http'), fs = require('fs'), path = require('path'), assert = require('assert');
const root = path.join(__dirname, '../..');
const pwDir = process.argv[2] || process.env.PW_DIR || path.join(root, 'node_modules');
const { chromium } = require(path.join(pwDir, 'playwright-core'));
const SHOTS = process.env.SHOTS || '';

const NOON = Date.UTC(2026, 9, 5, 16, 41);          // Mon 5 Oct 2026, 12:41 PM in the shop's time
const A = '4171711853', B = '4171711999', C = '4171711777', D = '4171711555';
const L = n => `${A}_50010${n}`;
const ev = (orderId, type, at, o = {}) => Object.assign({ id: `${orderId}~${type}~${at}`, orderId, type, at, by: 'Paul', station: 'sorter', device: 'charm-nest-1' }, o);
const TIMELINES = {
  // completed by hand in the sorter: line 1 printed (its QR label) then completed, line 2 completed, line 3 still on a sheet
  [A]: [ev(A, 'arrived', NOON - 9e6, { by: 'Etsy', station: '' }), ev(A, 'placed', NOON - 8e6, { lineKey: L(3), by: 'Rosa' }),
        ev(A, 'sealPrinted', NOON - 3e6, { lineKey: L(1), data: { n: 1, prints: 1 } }), ev(A, 'sealCompleted', NOON - 2.9e6, { lineKey: L(1), data: { how: 'button' } }),
        ev(A, 'sealCompleted', NOON, { lineKey: L(2), data: { how: 'button' } }),
        ev(A, 'labelPrinted', NOON - 1e6, { station: 'design', device: 'design-1', by: 'Nora', data: { label: 'sheetQR' } })],
  // finished at the Design Station only: nothing the sorter sealed
  [B]: [ev(B, 'arrived', NOON - 9e6, { by: 'Etsy', station: '' }), ev(B, 'labelPrinted', NOON - 1e6, { station: 'design', device: 'design-1', by: 'Nora', data: { label: 'sheetQR' } }),
        ev(B, 'note', NOON - 1e6, { station: 'design', device: 'design-1', by: 'Nora', data: { kind: 'designed', stamp: 'DESIGNED :)' } })],
  // one QR label printed by hand
  [D]: [ev(D, 'sealPrinted', NOON - 6e5, { lineKey: `${D}_700101`, by: 'Rosa', data: { n: 1, prints: 1 } })]
};
const archiveRow = rid => ({ receiptId: rid, orderNumber: rid, completedDay: '2026-10-05', completedAtMs: NOON, completedBy: 'Paul', money: { currency: 'USD', grand: 48 },
  ship: { name: 'Carolyn Schmidt', city: 'Toronto', state: 'ON', country: 'Canada' }, totals: { items: 1, quantity: 1 }, status: {}, shipments: [],
  items: [{ title: 'SPORTS 10 - FIGURE SKATE', quantity: 1, sku: 'SK10', metal: 'gold' }], messages: {} });

const sent = [], files = [];                          // every function call (url + body); every static file asked for
let timelineStatus = {};                              // orderId -> status to answer with instead of its timeline
let mutate = null;                                    // (file, text) -> text, for the mutants

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
  const auth = () => ({ signInAnonymously: async () => ({}), onAuthStateChanged(cb) { try { cb({ uid: 'u' }); } catch (_) {} return () => {}; }, currentUser: { uid: 'u' } });
  return { initializeApp() {}, firestore, auth, app: () => ({ options: {} }), storage: () => ({ ref: () => ({}) }) };
})();`;
const mStub = `window.M = (() => {
  const inst = new Map();
  const mk = () => { const i = { isOpen: false, open() { i.isOpen = true; }, close() { i.isOpen = false; } }; return i; };
  const of = el => { if (!inst.has(el)) inst.set(el, mk()); return inst.get(el); };
  const any = { init: el => of(el), getInstance: el => of(el) };
  return { AutoInit() {}, toast() {}, updateTextFields() {}, textareaAutoResize() {}, Modal: any, Tabs: any, Dropdown: any, Tooltip: any, Collapsible: any, Sidenav: any,
    FormSelect: { init: () => ({ getSelectedValues: () => [] }), getInstance: () => ({ getSelectedValues: () => [] }) } };
})();`;

function fake(method, url, body) {
  const u = new URL(url, 'http://x'), name = u.pathname.split('/').pop(), q = Object.fromEntries(u.searchParams);
  let b = {}; try { b = body ? JSON.parse(body) : {}; } catch (_) {}
  if (name === 'charmNestLibrary' && b.op === 'timelineGet') {
    if (timelineStatus[b.orderId]) return { __status: timelineStatus[b.orderId], error: 'the server is down' };
    return { orderId: b.orderId, events: TIMELINES[b.orderId] || [], cancelled: null, where: {} };
  }
  if (name === 'designArchive' && q.op === 'get') return { success: true, row: archiveRow(q.id) };
  if (name === 'designArchive') return { success: true, rows: [], have: [], total: 0, orders: [] };
  if (name === 'authGate') return { locked: false, ok: true };
  if (name === 'firebaseOrders') { if (method === 'GET') return { __status: 404, error: 'not found' }; return { success: true }; }
  return { ok: true };
}
const server = http.createServer((req, res) => {
  const u = decodeURIComponent(req.url.split('?')[0]);
  if (u.startsWith('/.netlify/functions/')) {
    let body = ''; req.on('data', d => { body += d; });
    req.on('end', () => {
      sent.push(req.url + ' ' + body);
      const out = fake(req.method, req.url, body), status = out.__status || 200; delete out.__status;
      res.writeHead(status, { 'Content-Type': 'application/json' }); res.end(JSON.stringify(out));
    });
    return;
  }
  const f = path.join(root, u);
  if (!f.startsWith(root) || !fs.existsSync(f) || fs.statSync(f).isDirectory()) { res.writeHead(404); return res.end(); }
  files.push(u);
  const js = /\.js$/.test(f), html = /\.html$/.test(f);
  res.writeHead(200, { 'Content-Type': js ? 'text/javascript' : html ? 'text/html' : 'application/octet-stream' });
  if ((js || html) && mutate) { const text = mutate(path.basename(f), fs.readFileSync(f, 'utf8')); return res.end(text); }
  fs.createReadStream(f).pipe(res);
}).listen(0);

const wait = ms => new Promise(r => setTimeout(r, ms));
async function until(fn, what, ms = 12000) { const t0 = Date.now(); for (;;) { const v = await fn(); if (v) return v; if (Date.now() - t0 > ms) throw new Error('timed out waiting for ' + what); await wait(60); } }
const origin = () => `http://127.0.0.1:${server.address().port}`;
const reads = id => sent.filter(s => /charmNestLibrary/.test(s) && s.includes('"timelineGet"') && s.includes(`"orderId":"${id}"`)).length;
const etsy = () => sent.filter(s => /etsyOrderProxy|etsy.*Proxy/i.test(s.split(' ')[0])).length;
const motionHits = () => files.filter(f => f === '/charm-nest-motion.js').length;

async function open(browser, file, { width = 1400, height = 950, motion } = {}) {
  const ctx = await browser.newContext({ viewport: { width, height } });
  await ctx.route(x => !/^http:\/\/127\.0\.0\.1[:/]/.test(x.href), r => {
    const u = new URL(r.request().url());
    if (/gstatic\.com/.test(u.host)) return r.fulfill({ status: 200, contentType: 'text/javascript', body: /firebase-app-compat/.test(u.pathname) ? fbStub : '' });
    if (/materialize/.test(u.pathname)) return r.fulfill({ status: 200, contentType: /\.css$/.test(u.pathname) ? 'text/css' : 'text/javascript', body: /\.css$/.test(u.pathname) ? '' : mStub });
    return r.abort();
  });
  if (motion) await ctx.route(/\/charm-nest-motion\.js/, motion);
  const page = await ctx.newPage(), errors = [];
  // (the stand-in for the Firebase CDN module does not export what the page's module script imports: not under test)
  page.on('pageerror', e => { const m = String(e && e.message || e); if (!/firebasejs|does not provide an export/.test(m)) errors.push(m); });
  await page.goto(origin() + '/' + file);
  await until(() => page.evaluate(() => typeof openArchiveOrder === 'function' && !!window.OrderTimeline && !!window.StationSeals && !!document.getElementById('archiveModal')), file + ' loaded');
  return { ctx, page, errors };
}
const openOrder = async (page, rid) => {
  await page.evaluate(r => { openArchiveOrder(r); }, rid);
  await until(() => page.evaluate(r => document.getElementById('amTitle').textContent === 'Order ' + r && !!document.querySelector('#amBody .amGrid'), rid), 'the pop-up of ' + rid);
};
const closeOrder = async page => { await page.click('#amClose'); await until(() => page.evaluate(() => !document.getElementById('archiveModal').open), 'the pop-up closed'); };
const seal = page => page.evaluate(() => {
  const host = document.getElementById('amSeal'), s = host.querySelector('.seal'), svg = s && s.querySelector('svg');
  let model = null; try { model = svg && JSON.parse(svg.dataset.sealModel); } catch (_) {}
  const r = s && s.getBoundingClientRect(), plus = host.querySelector('.sealPlus');
  return { shown: host.style.display !== 'none' && !!s, seals: host.querySelectorAll('.seal').length, plus: plus ? plus.textContent : '', model, width: s ? s.offsetWidth : 0, family: svg && svg.dataset.sealFamily };
});

// THE check: the order of A shows its one seal, as the sorter draws it. Throws when it does not (the mutants must fail it).
async function checkSealShown(page, what) {
  await until(async () => (await seal(page)).shown, what + ': the seal');
  const s = await seal(page);
  assert.strictEqual(s.seals, 1, what + ': one small seal');
  assert.strictEqual(s.plus, '+2', what + ': "+2" for the other two seals');
  assert.strictEqual(s.family, 'fulfilment', what + ': the green ORDER COMPLETE family');
  assert.strictEqual(s.model.action, 'ORDER COMPLETE', what + ': ORDER COMPLETE');
  assert.strictEqual(s.model.by, 'Paul', what + ': the person who pressed');
  assert.strictEqual(s.model.time, '12:41 PM', what + ': its own time (the latest press)');
  assert.strictEqual(s.model.date, '05 OCT 2026', what + ': its own date');
  assert(s.width >= 20 && s.width <= 24, what + ': small (22 px), was ' + s.width);
  return s;
}

async function designPage(browser, file) {
  sent.length = 0; files.length = 0; timelineStatus = {};
  const base = motionHits();
  const { ctx, page, errors } = await open(browser, file);
  assert.strictEqual(await page.evaluate(() => !!window.Seal), false, file + ': the Seal is not on the page until a seal is to be drawn');

  // an order with nothing the sorter sealed: no seal, and the seal script is never fetched
  await openOrder(page, B);
  await wait(500);
  assert.strictEqual((await seal(page)).shown, false, file + ': no seal for an order with none recorded');
  assert.strictEqual(motionHits(), base, file + ': the seal script is not fetched for it');
  assert.strictEqual(reads(B), 1, file + ': one timeline read');
  await closeOrder(page);

  // a timeline that cannot be read: no seal, the pop-up as it was
  timelineStatus[C] = 500;
  await openOrder(page, C);
  await wait(500);
  assert.strictEqual((await seal(page)).shown, false, file + ': no seal when the timeline cannot be read');
  assert(await page.evaluate(() => /^Completed /.test(document.getElementById('amSub').textContent) && !!document.querySelector('#amBody .amItem')), file + ': the pop-up is complete');
  await closeOrder(page);

  // the completed order: its seal
  await openOrder(page, A);
  const s = await checkSealShown(page, file);
  assert.strictEqual(motionHits(), base + 1, file + ': the seal script was fetched once, when a seal was to be drawn');
  assert.strictEqual(reads(A), 1, file + ': one timeline read when the pop-up opened');
  assert(await page.evaluate(() => /^Completed .* by Paul$/.test(document.getElementById('amSub').textContent)), file + ': the words are as they were');
  assert.strictEqual(await page.evaluate(() => document.querySelectorAll('#amSeal [title], #amSeal .sealMore, #archiveModal .seal [title]').length), 0, file + ': no tooltip or caption on a seal');
  assert.strictEqual(await page.evaluate(() => document.getElementById('amSeal').getAttribute('title')), null);
  assert(await page.evaluate(() => /\[native code\]/.test(HTMLDialogElement.prototype.showModal.toString()) && /\[native code\]/.test(HTMLDialogElement.prototype.close.toString())), file + ': the dialogs are the browser\'s own (Motion is not on the page)');
  assert.strictEqual(await page.evaluate(() => document.querySelectorAll('dialog.mdIn, .mGhost, #motionLayer').length), 0, file + ': nothing of the sorter\'s window motion');
  assert(!s.model.scope, 'a plain seal');
  if (SHOTS) await page.screenshot({ path: path.join(SHOTS, `${file.replace('.html', '')}-archive-completed-seal.png`) });

  // it grows in place, as every seal does (a click at once), and a second click puts it back
  const box0 = await page.evaluate(() => { const r = document.querySelector('#amSeal .seal').getBoundingClientRect(); return { x: r.left + r.width / 2, y: r.top + r.height / 2, w: r.width }; });
  await page.mouse.click(box0.x, box0.y);
  await until(() => page.evaluate(() => !!document.querySelector('#amSeal .seal').dataset.sealZoom), file + ': the seal zoomed');
  await wait(450);
  const zoomed = await page.evaluate(() => { const e = document.querySelector('#amSeal .seal'), r = e.getBoundingClientRect(); return { x: r.left + r.width / 2, y: r.top + r.height / 2, w: r.width, k: +e.dataset.sealZoom, open: document.getElementById('archiveModal').open }; });
  assert(zoomed.k > 1.3 && zoomed.w > box0.w * 1.3, file + ': it grew, ×' + zoomed.k);
  assert(Math.abs(zoomed.x - box0.x) < 40 && Math.abs(zoomed.y - box0.y) < 40, file + ': in place, not a copy somewhere else');
  assert.strictEqual(await page.evaluate(() => document.querySelectorAll('#amSeal .seal').length), 1, file + ': no second seal');
  if (SHOTS) await page.screenshot({ path: path.join(SHOTS, `${file.replace('.html', '')}-archive-seal-zoomed.png`) });
  await page.keyboard.press('Escape');
  await until(() => page.evaluate(() => !document.querySelector('#amSeal .seal').classList.contains('sealZoomed')), file + ': the zoom went back');
  assert(await page.evaluate(() => document.getElementById('archiveModal').open), file + ': Esc put the seal back and left the pop-up open');

  // no polling, no Etsy
  const r0 = reads(A); await wait(1500);
  assert.strictEqual(reads(A), r0, file + ': no polling');
  assert.strictEqual(etsy(), 0, file + ': no Etsy call');
  await closeOrder(page);

  // the next order never carries the last one's seal
  await openOrder(page, B);
  assert.strictEqual((await seal(page)).shown, false, file + ': the last order\'s seal is gone');
  await closeOrder(page);

  // a print-only order: the blue QR LABEL PRINTED seal, nothing else
  await openOrder(page, D);
  await until(async () => (await seal(page)).shown, file + ': the print seal');
  const p = await seal(page);
  assert.strictEqual(p.model.action, 'QR LABEL PRINTED'); assert.strictEqual(p.family, 'prepared'); assert.strictEqual(p.plus, ''); assert.strictEqual(p.model.by, 'Rosa');
  assert.strictEqual(motionHits(), base + 1, file + ': the script is kept for the next order');
  await closeOrder(page);

  assert.deepStrictEqual(errors, [], file + ': no page error');
  await ctx.close();
  console.log(file + ': seal on the completed order, none where nothing is recorded or readable, a print seal, zoom in place, one read, no Etsy');
}

async function failedScript(browser, file) {
  sent.length = 0; files.length = 0;
  const base = motionHits();
  const { ctx, page, errors } = await open(browser, file, { motion: r => r.fulfill({ status: 500, contentType: 'text/javascript', body: '' }) });
  await openOrder(page, A);
  await wait(900);
  assert.strictEqual((await seal(page)).shown, false, file + ': no seal when the seal script cannot be had');
  assert(await page.evaluate(() => /^Completed .* by Paul$/.test(document.getElementById('amSub').textContent) && !!document.querySelector('#amBody .amItem') && !document.getElementById('amBody').textContent.includes('not in the archive')), file + ': the pop-up is complete');
  assert.strictEqual(await page.evaluate(() => !!window.Seal), false);
  assert.strictEqual(await page.evaluate(() => { const b = document.getElementById('amShipBtn'); return !b.disabled; }), true, file + ': its buttons work');
  await closeOrder(page);
  await openOrder(page, A); await wait(600);   // the next opening asks again (one request each), and the page still works
  assert.strictEqual(await page.evaluate(() => document.getElementById('archiveModal').open), true);
  await closeOrder(page);
  assert.deepStrictEqual(errors, [], file + ': no page error');
  await ctx.close();
  console.log(file + ': a seal script that fails to load leaves the pop-up working');
}

async function phone(browser, file) {
  const { ctx, page } = await open(browser, file, { width: 390, height: 780 });
  await openOrder(page, A);
  await until(async () => (await seal(page)).shown, file + ' (phone): the seal');
  await wait(300);
  const g = await page.evaluate(() => {
    const d = document.querySelector('#archiveModal').getBoundingClientRect(), s = document.querySelector('#amSeal').getBoundingClientRect(), close = document.getElementById('amClose').getBoundingClientRect();
    return { dl: d.left, dr: d.right, sl: s.left, sr: s.right, st: s.top, sb: s.bottom, closeR: close.right, closeL: close.left, sw: document.documentElement.scrollWidth, iw: innerWidth };
  });
  assert(g.sl >= g.dl - 1 && g.sr <= g.dr + 1, file + ' (phone): the seal is inside the pop-up');
  assert(g.sw <= g.iw + 1, file + ' (phone): no sideways scroll, ' + g.sw + ' in ' + g.iw);
  assert(g.closeR <= g.dr + 1 && g.closeL >= g.dl - 1, file + ' (phone): Close is still in the window');
  if (SHOTS) await page.screenshot({ path: path.join(SHOTS, `${file.replace('.html', '')}-archive-completed-seal-phone.png`) });
  await ctx.close();
  console.log(file + ': at phone width the header takes the seal without a sideways scroll');
}

async function mutants(browser) {
  // the page does not ask for the seal
  mutate = (f, t) => f === 'design-1.html' ? t.replace('StationSeals.show($("#amSeal"), rec.receiptId)', 'void 0') : t;
  let caught = false;
  { const { ctx, page } = await open(browser, 'design-1.html'); await openOrder(page, A);
    try { await wait(600); await checkSealShown(page, 'mutant: page drops the seal'); } catch (e) { caught = /seal/.test(String(e.message)); } await ctx.close(); }
  assert(caught, 'the mutant that does not ask for the seal was not caught');
  // the module draws nothing
  mutate = (f, t) => f === 'station-seals.js' ? t.replace('host.innerHTML = html;', 'host.innerHTML = "";') : t;
  caught = false;
  { const { ctx, page } = await open(browser, 'design.html'); await openOrder(page, A);
    try { await checkSealShown(page, 'mutant: module draws nothing'); } catch (e) { caught = /seal/.test(String(e.message)); } await ctx.close(); }
  assert(caught, 'the mutant that draws nothing was not caught');
  mutate = null;
  console.log('mutants: the page not asking for the seal and the module drawing nothing are caught');
}

(async () => {
  if (SHOTS) fs.mkdirSync(SHOTS, { recursive: true });
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  try {
    // the files exist where the build serves them
    const manifest = fs.readFileSync(path.join(root, 'scripts/build-public.cjs'), 'utf8');
    assert(/"station-seals\.js"/.test(manifest) && /"charm-nest-motion\.js"/.test(manifest), 'station-seals.js and charm-nest-motion.js are in the public build');
    // the seal's rules in station-seals.js are the sorter's own lines (a drift in either is caught here)
    const mod = fs.readFileSync(path.join(root, 'station-seals.js'), 'utf8'), sorter = fs.readFileSync(path.join(root, 'charm-nest-1.html'), 'utf8');
    const rules = [...mod.matchAll(/^\s*"(\.(?:sealRow|seal)[^"]*\{[^"]*\})",?$/gm)].map(m => m[1]);
    assert(rules.length >= 5, 'the seal rules were read from the module: ' + rules.length);
    for (const r of rules) assert(sorter.includes(r), 'the sorter\'s page has this rule too: ' + r.slice(0, 60));
    await designPage(browser, 'design.html');
    await designPage(browser, 'design-1.html');
    await failedScript(browser, 'design-1.html');
    await failedScript(browser, 'design.html');
    await phone(browser, 'design-1.html');
    await mutants(browser);
    console.log('completed-seals-station-modals: all passed');
  } finally { await browser.close(); server.close(); }
})().catch(e => { console.error(e); process.exit(1); });
