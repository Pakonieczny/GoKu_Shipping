// The Design stations (design.html, design-1.html) record every print and completion under a real person, and give design
// work minute-level time resolution (Employee efficiency console, station-activity.js). One focused offline test, real pages,
// real station-session.js and station-activity.js, the real server rollup (netlify/functions/_stationActivity.js) for the
// numbers, a fake door for everything else:
//   · the gate: with no name set, Generate QR and Print labels do NOT run; the small "Set your name" field is shown beside the
//     action and focused, the selection, the open QR dialog and its labels stay exactly as they were, nothing is recorded or
//     completed; a number is refused as a name; once a name is set (Enter, or just clicking the action next) the action works
//     and is recorded under that name; the Charm Sorter's remote commands never ask for a name
//   · the name: tidied before it is stored and sent ("  dana   d " = "Dana D."), the same from the gate and from the order
//     chat's own control, so the session and every event carry one spelling
//   · the open-order event: a person's own click on a tile or a row records a low-weight `note` (fixed text, no order id, no
//     parts), at most one per 30 s, never for the sorter; against the real rollup it adds no scan, no piece, no order and no
//     "worked" order, only a note, and it turns minutes between prints from idle into working time
//   NODE_PATH=$(npm root -g) PW_DIR=$(npm root -g)/playwright/node_modules CHROMIUM=… node tests/stations/design-name-gate.cjs
'use strict';
const http = require('http'), fs = require('fs'), path = require('path'), assert = require('assert');
const root = path.join(__dirname, '../..');
const pwDir = process.argv[2] || process.env.PW_DIR || path.join(root, 'node_modules');
const { chromium } = require(path.join(pwDir, 'playwright-core'));
const SA = require(path.join(root, 'netlify/functions/_stationActivity.js'));

const FAKE_NUMBER = '424242';                       // a stand-in for a PIN typed into a name field: never a name
const acts = [], sent = [];                          // every activity event the door got; every request (url + body) the pages made

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
// the print page: answers the station's hand-off at once, as the real one does when it has printed
const printStub = `<!doctype html><meta charset="utf-8"><script>
try { var n = JSON.parse(localStorage.getItem('metalOrderJobs')).nonce; parent.postMessage({ source: 'brites-print', nonce: n, phase: 'done', ok: true, labels: 1, orders: 2 }, '*'); } catch (e) {}
</script>`;

function fake(method, url, body) {
  const u = new URL(url, 'http://x'), name = u.pathname.split('/').pop(), q = Object.fromEntries(u.searchParams);
  let b = {}; try { b = body ? JSON.parse(body) : {}; } catch (_) {}
  if (name === 'firebaseOrders') {
    if (method === 'GET') return { __status: 404, error: 'not found' };
    if (Array.isArray(b.activity)) { acts.push(...b.activity); return { success: true, written: b.activity.length, duplicate: 0, refused: 0, scrubbed: 0 }; }
    return { success: true };
  }
  if (name === 'authGate') return { locked: false, ok: true };
  if (name === 'etsyOrderProxy') {
    const txs = [{ transaction_id: 1, quantity: 2, title: 'Charm', sku: 'A1' }, { transaction_id: 2, quantity: 1, title: 'Chain', sku: 'B2' }];
    return q.include ? { transactions: txs } : { status: 'open', name: 'Test Buyer', transactions: txs, receipt: { receipt_id: q.orderId, name: 'Test Buyer' } };
  }
  if (name === 'firestoreProxy') return q.op === 'get' ? { exists: false } : q.op === 'list' || q.op === 'listSub' ? { docs: [] } : { ok: true };
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
  if (/^\/design-print(-1)?\.html$/.test(u)) { res.writeHead(200, { 'Content-Type': 'text/html' }); return res.end(req.method === 'HEAD' ? undefined : printStub); }
  const f = path.join(root, u);
  if (!f.startsWith(root) || !fs.existsSync(f) || fs.statSync(f).isDirectory()) { res.writeHead(404); return res.end(); }
  res.writeHead(200, { 'Content-Type': /\.js$/.test(f) ? 'text/javascript' : /\.html$/.test(f) ? 'text/html' : 'application/octet-stream' });
  fs.createReadStream(f).pipe(res);
}).listen(0);

const wait = ms => new Promise(r => setTimeout(r, ms));
async function until(fn, what, ms = 12000) { const t0 = Date.now(); for (;;) { const v = await fn(); if (v) return v; if (Date.now() - t0 > ms) throw new Error('timed out waiting for ' + what); await wait(80); } }
const origin = () => `http://127.0.0.1:${server.address().port}`;

async function open(browser, file) {
  const ctx = await browser.newContext({ viewport: { width: 1400, height: 950 } });
  await ctx.route(x => !/^http:\/\/127\.0\.0\.1[:/]/.test(x.href), r => {
    const u = new URL(r.request().url());
    if (/gstatic\.com/.test(u.host)) return r.fulfill({ status: 200, contentType: 'text/javascript', body: /firebase-app-compat/.test(u.pathname) ? fbStub : '' });
    if (/materialize/.test(u.pathname)) return r.fulfill({ status: 200, contentType: /\.css$/.test(u.pathname) ? 'text/css' : 'text/javascript', body: /\.css$/.test(u.pathname) ? '' : mStub });
    if (/code\.jquery\.com/.test(u.host)) return r.fulfill({ status: 200, contentType: 'text/javascript', body: fs.existsSync('/usr/share/javascript/jquery/jquery.min.js') ? fs.readFileSync('/usr/share/javascript/jquery/jquery.min.js') : 'window.jQuery=window.$=function(){return{on(){return this},ready(f){f()}}};' });
    return r.abort();
  });
  const page = await ctx.newPage(), errors = [];
  page.on('pageerror', e => errors.push(String(e && e.message || e)));
  await page.goto(origin() + '/' + file);
  return { ctx, page, errors };
}
const flush = async page => { await page.evaluate(() => window.StationActivity && StationActivity.flush()); await wait(250); };
const mine = device => acts.filter(e => e.device === device);
const brief = list => list.map(e => `${e.action}|${e.orderId}|${e.parts}|${e.orders}`);
const gateState = page => page.evaluate(() => {
  const b = document.getElementById('dsNameGate');
  return { shown: !!b && !b.hidden, focused: (document.activeElement || {}).id || '', text: b ? b.textContent : '', inFooter: !!(b && b.closest('#qrPreviewModal .dlgFoot')) };
});
const personsOfSessions = device => [...new Set(sent.filter(s => /"session"/.test(s)).map(s => { try { return JSON.parse(s.slice(s.indexOf(' {') + 1)).session; } catch (_) { return null; } })
  .filter(x => x && x.device === device).map(x => x.person))];
/** the real server rollup for one person's events at one device (what Efficiency_Daily would hold) */
function rollup(events) {
  const now = Math.max(...events.map(e => e.at)) + 1000;
  const docs = events.map(e => SA.clean(e, now, '').doc);
  assert(docs.every(Boolean), 'every event passes the server');
  const patch = SA.rollupPatch({ increment: v => v }, null, docs[0].day, docs[0].person, docs, '');
  return { st: patch.stations.design || {}, touched: Object.keys(patch.touched || {}).sort(), hours: JSON.stringify(patch.hours || {}) };
}

const NORMAL = [
  ['  dana   d ', 'Dana D.'], ['Dana D', 'Dana D.'], ['Dana D.', 'Dana D.'], ['DANA', 'Dana'], ['dana', 'Dana'], ['Dana', 'Dana'],
  ['mary-jane  O\'BRIEN', 'Mary-Jane O\'Brien'], ['o\'brien-smith', 'O\'Brien-Smith'], ['McDonald  k', 'McDonald K.'], ['zoë  müller', 'Zoë Müller'],
  ['\tAna  B\n', 'Ana B.'], ['Dana 482915', 'Dana'], [FAKE_NUMBER, ''], ['123 456', ''], ['   ', ''], ['...', ''], ['', ''], ['Ev<e>', 'Ev E.']
];

async function designPage(browser, file, device) {
  const { ctx, page, errors } = await open(browser, file);
  await until(() => page.evaluate(() => typeof proceedToPrint === 'function' && typeof DsName === 'object' && window.StationSession && StationSession.page() && window.StationActivity), file + ' loaded');
  await page.evaluate(() => {   // what the page loads from Etsy and the shared locks is not under test
    window.buildNewOrderList = async () => {}; window.ensureSelectedPreviews = async () => {}; window.ensureTilesFor = async () => {};
    window.rtQueueLock = () => {}; window.openListingModal = async () => {};
    try { SETTINGS.labelsFromSorter = false; } catch (_) {}                    // design-1: the print route (Settings), whose button the preview would hide
    document.getElementById('qrPreviewPrintBtn').classList.remove('hidden');
    window.__skew = 0; const real = Date.now.bind(Date); Date.now = () => real() + window.__skew;
  });

  // ── the name is tidied the same way everywhere ──
  const normal = await page.evaluate(list => list.map(([raw]) => DsName.normalize(raw)), NORMAL);
  NORMAL.forEach(([raw, want], i) => assert.strictEqual(normal[i], want, `${device}: normalize(${JSON.stringify(raw)})`));
  assert.deepStrictEqual(await page.evaluate(list => list.map(s => DsName.normalize(DsName.normalize(s))), NORMAL.map(x => x[0])), normal, device + ': tidying twice changes nothing');
  assert(normal.every(n => !/[<>]/.test(n)), 'markup characters never reach a name');
  assert.strictEqual(await page.evaluate(() => DsName.normalize('x'.repeat(200) + ' y').length), 80, 'a name is cut at 80 characters');

  // ── nobody named: Generate QR asks for a name beside the button and does nothing else ──
  const RID = '3521000005';
  await page.evaluate(rid => {
    selectedOrders.clear(); orderCache[rid] = [{ quantity: 1, title: 'plain thing' }]; allOpenReceipts.push({ receipt_id: rid }); selectedOrders.add(rid);
    const b = document.getElementById('completeBtn'); b.disabled = false; b.click();
  }, RID);
  await wait(300);
  let g = await gateState(page);
  assert(g.shown && g.focused === 'dsNameInput' && !g.inFooter, device + ': Generate QR shows the name field and focuses it: ' + JSON.stringify(g));
  assert(/Set your name/.test(g.text) && /Generate QR/.test(g.text) && /counted/.test(g.text), 'a labelled hint says what to do: ' + g.text);
  assert.deepStrictEqual(await page.evaluate(rid => ({ kept: selectedOrders.has(rid), qr: document.getElementById('qrPreviewModal').open, who: StationActivity.who(), name: localStorage.getItem('employee_name') }), RID),
    { kept: true, qr: false, who: null, name: null }, device + ': the selection is kept, no dialog opened, nobody signed in');
  await page.fill('#dsNameInput', FAKE_NUMBER); await page.press('#dsNameInput', 'Enter');
  g = await gateState(page);
  assert(g.shown && /not a number/.test(g.text), 'a number is refused with a plain sentence: ' + g.text);
  assert.strictEqual(await page.evaluate(() => localStorage.getItem('employee_name')), null, 'a number is never stored as a name');
  assert.strictEqual(await page.inputValue('#dsNameInput'), '', 'a refused number is not left on screen');
  // the Charm Sorter's own command is not a person's click: it never asks for a name
  await page.evaluate(() => document.getElementById('dsNameGate').hidden = true);
  await page.evaluate(() => handleCompleteClick({ remote: true }));
  assert.strictEqual((await gateState(page)).shown, false, device + ': a remote (sorter) command never asks for a name');

  // ── nobody named: Print labels asks beside its own button, the open dialog and its labels stay ──
  const A = '3521000001', B = '3521000002';
  await page.evaluate(([a, b]) => {
    orderCache[a] = [{ quantity: 2 }, { quantity: 1 }]; orderCache[b] = [{ quantity: 1 }];
    pendingLists = { gold: [a, b] }; pendingJobs = buildPrintJobs(pendingLists);
    try { SETTINGS.labelsFromSorter = false; } catch (_) {}                   // (boot() loads the settings again once the passcode gate has answered: on a slow start that replaced the object set above)
    document.getElementById('qrPreviewPrintBtn').classList.remove('hidden'); // design-1: the print route, whose button the preview would hide
    document.getElementById('qrPreviewPrintBtn').disabled = false;           // as the preview does once it has labels
    openDlg(document.getElementById('qrPreviewModal'));
  }, [A, B]);
  await page.click('#qrPreviewPrintBtn');
  await wait(300); await flush(page);
  g = await gateState(page);
  assert(g.shown && g.inFooter && g.focused === 'dsNameInput' && /Print labels/.test(g.text), device + ': Print labels shows the name field in the dialog footer: ' + JSON.stringify(g));
  assert.deepStrictEqual(await page.evaluate(([a, b]) => ({ dlg: document.getElementById('qrPreviewModal').open, jobs: !!(pendingJobs && pendingJobs.length), lists: JSON.stringify(pendingLists), done: completedOrders.has(a) || completedOrders.has(b), cached: !!(orderCache[a] && orderCache[b]) }), [A, B]),
    { dlg: true, jobs: true, lists: JSON.stringify({ gold: [A, B] }), done: false, cached: true }, device + ': the dialog, the labels and the orders are untouched');
  assert.strictEqual(mine(device).length, 0, device + ': nothing recorded, nothing completed without a name');

  // ── typing the name and simply pressing Print labels works in one go (no click lost), under the tidied name ──
  await page.fill('#dsNameInput', ' ana   b ');
  await page.click('#qrPreviewPrintBtn');
  await until(async () => { await flush(page); return mine(device).some(e => e.action === 'complete'); }, 'the print after naming');
  let got = mine(device);
  assert.deepStrictEqual(brief(got), ['print||0|0', `complete|${A}|3|1`, `complete|${B}|1|1`], device + ': the click went through: one print, one complete per order');
  assert(got.every(e => e.person === 'Ana B.' && e.station === 'design' && e.device === device), device + ': recorded under the tidied name');
  assert.strictEqual(await page.evaluate(() => localStorage.getItem('employee_name')), 'Ana B.');
  assert.strictEqual((await gateState(page)).shown, false, 'the field is gone once the action runs');

  // ── Generate QR now runs, recorded under the name ──
  await page.evaluate(() => document.getElementById('completeBtn').click());
  await until(async () => { await flush(page); return mine(device).some(e => e.action === 'reject'); }, 'Generate QR after naming');
  assert.deepStrictEqual(brief(mine(device).filter(e => e.action === 'reject')), [`reject|${RID}|0|0`], device + ': Generate QR ran and recorded under the name');

  // ── the order chat's own control tidies the same way, and the session carries one spelling ──
  await page.evaluate(() => BritesChat.open('3521000777'));
  await until(() => page.$('#bcWho'), 'the chat');
  await page.evaluate(() => { document.getElementById('bcWho').click(); const i = document.querySelector('#bcWho input'); i.value = '  dana   d '; i.dispatchEvent(new Event('blur')); });
  await until(() => page.evaluate(() => { const w = StationActivity.who(); return w && w.person === 'Dana D.'; }), 'the chat sign-in');
  assert.strictEqual(await page.evaluate(() => localStorage.getItem('employee_name')), 'Dana D.', device + ': the chat stores the tidied name');
  await page.evaluate(() => { document.querySelector('.bcClose, #lmClose')?.click(); });
  await flush(page);
  assert.deepStrictEqual(personsOfSessions(device).sort(), ['Ana B.', 'Dana D.'], device + ': sessions carry one spelling per person');
  assert(!sent.some(s => /dana {2,}d|ana {2,}b/i.test(s)), 'no untidied spelling in any request');

  // ── opening an order: a low-weight note, only for the person's own click, 30 s apart at most ──
  const base = mine(device).length;
  await page.evaluate(() => {
    window.__skew += 200000;                                                    // three minutes of looking at orders, between sign-in and print
    const host = document.getElementById('newOrderContainer'); const row = document.createElement('div');
    row.className = 'orderRow'; row.dataset.receipt = '3521000555'; host.appendChild(row); row.click();
  });
  await until(async () => { await flush(page); return mine(device).length > base; }, 'the row note');
  await page.evaluate(() => { const t = makeTile({ transaction_id: 91, receipt_id: '3521000556', quantity: 1, sku: 'A1', title: 'Charm' }); t.querySelector('.tileBody').click(); });
  await flush(page);
  assert.strictEqual(mine(device).length, base + 1, device + ': a second click within 30 s is not another event');
  await page.evaluate(() => {
    window.__skew += 200000;
    const t = makeTile({ transaction_id: 92, receipt_id: '3521000557', quantity: 1, sku: 'A1', title: 'Charm' }); t.querySelector('.tileBody').click();
  });
  await until(async () => { await flush(page); return mine(device).length > base + 1; }, 'the tile note');
  await page.evaluate(() => { window.__skew += 200000; });
  await page.evaluate(async ([a, b]) => {   // the person prints what they looked at
    orderCache[a] = [{ quantity: 2 }, { quantity: 1 }]; orderCache[b] = [{ quantity: 1 }];
    pendingLists = { gold: [a, b] }; pendingJobs = buildPrintJobs(pendingLists); await proceedToPrint();
  }, ['3521000003', '3521000004']);
  await flush(page);
  const mineNow = mine(device).filter(e => e.person === 'Dana D.');
  const opens = mineNow.filter(e => e.action === 'note');
  assert.deepStrictEqual(opens.map(e => `${e.detail}|${e.orderId}|${e.parts}|${e.orders}`), ['selected order||0|0', 'opened order||0|0'], device + ': a fixed text, no order id, no parts');
  assert(opens.every(e => e.sincePrevMs >= 200000 && e.sincePrevMs < 260000), 'each one carries the minutes since the action before it');

  // ── what the server rollup makes of it ──
  // (without the open events the print would have carried the whole gap since the action before them: carry the dropped gaps forward)
  let carry = 0; const kept = [];
  for (const e of mineNow) { if (e.action === 'note') { carry += e.sincePrevMs; continue; } kept.push(Object.assign({}, e, { sincePrevMs: Math.min(3600000, e.sincePrevMs + carry) })); carry = 0; }
  const withOpens = rollup(mineNow), without = rollup(kept);
  for (const k of ['scans', 'scanParts', 'parts', 'orders', 'completes', 'prints', 'rejects', 'errors', 'undos']) assert.strictEqual(withOpens.st[k] || 0, without.st[k] || 0, `${device}: the open events do not change ${k}`);
  assert.deepStrictEqual([withOpens.st.completes, withOpens.st.parts, withOpens.st.orders, withOpens.st.prints, withOpens.st.scans || 0], [2, 4, 2, 1, 0], 'pieces and orders come only from the completions');
  assert.deepStrictEqual(withOpens.touched, without.touched, device + ': the open events make no order "worked"');
  assert.strictEqual(withOpens.hours, without.hours, device + ': nor any hourly throughput');
  assert.strictEqual(withOpens.st.notes, 2); assert.strictEqual(without.st.notes || 0, 0);
  assert(withOpens.st.activeMs >= 600000 && !withOpens.st.idleMs, `${device}: ten minutes of looking and printing is working time (${withOpens.st.activeMs} ms active, ${withOpens.st.idleMs || 0} idle)`);
  assert(without.st.idleMs >= 600000, device + ': without the open events the same ten minutes was idle (' + without.st.idleMs + ' ms)');

  // ── the page is quiet about it ──
  assert.strictEqual(errors.filter(e => !/Failed to fetch|Load failed|gstatic|does not provide an export/.test(e)).length, 0, device + ' page errors: ' + errors.join(' | '));
  await ctx.close();
  console.log(`${device}: gate (Generate QR, Print labels, number refused, sorter exempt), tidy name, open-order note (idle 10 min -> active), rollup unchanged`);
}

(async () => {
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  try {
    await designPage(browser, 'design.html', 'design');
    await designPage(browser, 'design-1.html', 'design-1');
    const all = sent.join('\n');
    assert(!all.includes(FAKE_NUMBER), 'a number typed into the name field reached a request');
    for (const e of acts) assert(e.parts <= 100000 && e.detail.length <= 120 && Buffer.byteLength(JSON.stringify(e)) <= 600, 'event within the door limits');
    console.log('design-name-gate: all passed (' + acts.length + ' events, no number from the name field in ' + sent.length + ' requests)');
  } finally { await browser.close(); server.close(); }
})().catch(e => { console.error(e); process.exit(1); });
