// What the Design pages and the Inbox record for the Employee efficiency console (station-activity.js), end to end with
// the real station-session.js and station-activity.js: a fake run per page (sign in, design finished, label printed, a
// message, an undo, a refusal, a failure), each event once with the right person, station, device and order, nothing while
// nobody is signed in, nothing for the Charm Sorter's own commands, and no PIN, customer text or address in any request.
//   · design.html, design-1.html: name set in the order chat (the station has no PIN); a label print = one `print` per
//     label; a finished design = one `complete` per order with its pieces; Undo reverses exactly that; orders left off
//     the labels = `reject` once; an order-chat message = `note` (never its words); a failed print = `error`
//   · design-message.html, design-message-1.html: 6-digit PIN sign-in; an order opened (typed or the phone's scan) = `scan`
//     with its pieces, and an `error` too when Etsy has no answer for it; a message sent = `note`; a picture dropped into the
//     chat = `note` (an `error` when it fails); nothing after Sign Out
//   · etsy-mail-1.html: operator account; a reply queued = `note`, a conversation done = `complete`, a reply that did
//     not go = `error`, a Done that the server refused = `error`
// Every /.netlify/functions call goes to a fake in this file; every other host is aborted (stand-ins for the CDN scripts).
//   NODE_PATH=$(npm root -g) PW_DIR=$(npm root -g)/playwright/node_modules CHROMIUM=… node tests/stations/design-activity.cjs
'use strict';
const http = require('http'), fs = require('fs'), path = require('path'), assert = require('assert');
const root = path.join(__dirname, '../..');
const pwDir = process.argv[2] || process.env.PW_DIR || path.join(root, 'node_modules');
const { chromium } = require(path.join(pwDir, 'playwright-core'));

const PIN = '424242';
const CUSTOMER = ['Jane Doe', '12 Main St', 'Test Buyer', 'engrave her initials'];
let mapGets = 0, failStatus = false;
const acts = [], sent = [], lives = [];            // every activity event the door got; every request (url + body) the pages made; every live write ({ live }: the order in hand)
const ts = ms => ({ _ts: true, ms });
const T0 = Date.now();
const IDS = { A: 'etsy_conv_1001', B: 'etsy_conv_1002' };
const THREADS = Object.entries(IDS).map(([k, id], i) => ({ id, customerName: 'Cust ' + k, status: 'etsy_scraped', unread: false, etsyOrderId: k === 'A' ? '3521000444' : undefined,
  lastInboundAt: ts(T0 - 3600e3 - i * 60e3), awaitingReplySince: ts(T0 - 3600e3), updatedAt: ts(T0 - 3600e3), etsyConversationUrl: 'https://example.invalid/c/' + id }));
const msgs = id => [{ id: id + '_m1', direction: 'inbound', senderName: 'Cust', senderRole: 'customer', text: 'Hi, thank you for the update!', timestamp: ts(T0 - 7200e3), createdAt: ts(T0 - 7200e3) }];

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
    if (b.pinLogin !== undefined) return { ok: b.pinLogin === PIN, ...(b.pinLogin === PIN ? { name: 'Rosa Designer' } : { error: 'not on the list' }) };   // the server's login door
    if (method === 'GET' && /employee/i.test(q.orderId || '')) { mapGets++; return { __status: 401, success: false }; }   // the roster is never asked for
    if (method === 'GET') return { __status: 404, error: 'not found' };
    if (b.live) { lives.push(b.live); return { success: true, written: 1 }; }
    if (Array.isArray(b.activity)) { acts.push(...b.activity); return { success: true, written: b.activity.length, duplicate: 0, refused: 0, scrubbed: 0 }; }
    if (b.newMessage) return { success: true, messageId: b.designSetId ? 'designed-set-' + encodeURIComponent(b.designSetId) : 'm1' };
    return { success: true };
  }
  if (name === 'authGate') return { locked: false, ok: true };
  if (name === 'etsyOrderProxy' && q.orderId === '3521009995') return { __status: 404, error: 'Resource not found' };   // Etsy has no answer for this one
  if (name === 'etsyMailThreads' && b.action === 'setStatus' && failStatus) return { __status: 500, error: 'the server is down' };
  if (name === 'etsyOrderProxy') {
    const txs = [{ transaction_id: 1, quantity: 2, title: 'Charm', sku: 'A1' }, { transaction_id: 2, quantity: 1, title: 'Chain', sku: 'B2' }];
    return q.include ? { transactions: txs } : { status: 'open', name: 'Test Buyer', transactions: txs, receipt: { receipt_id: q.orderId, name: 'Test Buyer' } };
  }
  if (name === 'firestoreProxy') {
    if (q.op === 'list' && q.coll === 'EtsyMail_Threads') return { docs: THREADS };
    if (q.op === 'listSub') return { docs: msgs(q.id) };
    if (q.op === 'get') return { exists: false };
    return { ok: true };
  }
  if (name === 'etsyMailAuth') return { ok: true, username: 'paul', displayName: 'Paul Inbox', role: 'owner' };
  if (name === 'etsyMailDraftSend' && method === 'POST' && b.op === 'enqueue') {
    if (b.threadId === IDS.B) return { __status: 400, error: 'The Etsy helper refused it' };
    return { ok: true, draftId: 'draft_' + b.threadId, attachments: b.attachments || [], text: b.text };
  }
  if (name === 'etsyMailDraftSend') return { __status: 404, error: 'Draft not found' };
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

async function open(browser, file, seed, query = '') {
  const ctx = await browser.newContext({ viewport: { width: 1400, height: 950 } });
  await ctx.route(x => !/^http:\/\/127\.0\.0\.1[:/]/.test(x.href), r => {
    const u = new URL(r.request().url());
    if (/gstatic\.com/.test(u.host)) return r.fulfill({ status: 200, contentType: 'text/javascript', body: /firebase-app-compat/.test(u.pathname) ? fbStub : '' });
    if (/materialize/.test(u.pathname)) return r.fulfill({ status: 200, contentType: /\.css$/.test(u.pathname) ? 'text/css' : 'text/javascript', body: /\.css$/.test(u.pathname) ? '' : mStub });
    if (/code\.jquery\.com/.test(u.host)) return r.fulfill({ status: 200, contentType: 'text/javascript', body: fs.existsSync('/usr/share/javascript/jquery/jquery.min.js') ? fs.readFileSync('/usr/share/javascript/jquery/jquery.min.js') : 'window.jQuery=window.$=function(){return{on(){return this},ready(f){f()}}};' });
    return r.abort();
  });
  await ctx.addInitScript(seed || (() => {}));
  const page = await ctx.newPage(), errors = [];
  page.on('pageerror', e => errors.push(String(e && e.message || e)));
  await page.goto(origin() + '/' + file + query);
  return { ctx, page, errors };
}
const flush = async page => { await page.evaluate(() => window.StationActivity && StationActivity.flush()); await wait(250); };
const mine = device => acts.filter(e => e.device === device);
const brief = list => list.map(e => `${e.action}|${e.orderId}|${e.parts}|${e.orders}`);
// the live board: what a page said is in hand right now, one word per write (beats are keep-alives)
const liveOf = device => lives.filter(l => l.event !== 'beat' && l.device === device);
const liveBrief = device => liveOf(device).map(l => l.event + '|' + (l.order ? l.order.kind + ':' + (l.order.rid || l.order.title) + ':' + l.order.pieces.length : (l.ended.rid || l.ended.title)));
const look = (list, who) => list.forEach(e => {
  assert.strictEqual(e.person, who.person, 'person'); assert.strictEqual(e.station, who.station, 'station'); assert.strictEqual(e.device, who.device, 'device');
  assert(/^pc-/.test(e.computer) && e.session && e.sincePrevMs >= 0 && e.at > 0, 'computer, session and time are stamped: ' + JSON.stringify(e));
});

/* ── design.html and design-1.html ── */
async function designPage(browser, file, device) {
  const { ctx, page, errors } = await open(browser, file);
  await until(() => page.evaluate(() => typeof proceedToPrint === 'function' && window.StationSession && StationSession.page() && window.StationActivity), file + ' loaded');
  await page.evaluate(() => {   // what the page loads from Etsy is not under test: its list refreshes are no-ops
    window.buildNewOrderList = async () => {}; window.ensureSelectedPreviews = async () => {};
  });
  const printRun = ids => page.evaluate(async ([a, b]) => {
    orderCache[a] = [{ quantity: 2 }, { quantity: 1 }]; orderCache[b] = [{ quantity: 1 }];
    pendingLists = { gold: [a, b] }; pendingJobs = buildPrintJobs(pendingLists);
    await proceedToPrint();
  }, ids);

  // the queue's rows the person picks (a click on a row, as in the page): the order's lines are what the page already holds
  const rowSel = rid => `#newOrderContainer .orderRow[data-receipt="${rid}"]`;
  const addRows = () => page.evaluate(() => {
    for (const [rid, q] of [['3521000011', 2], ['3521000012', 1]]) {
      orderCache[rid] = [{ quantity: q, sku: 'A1', title: 'Charm', receipt_id: rid, listing_id: 111222333 }];
      if (!document.querySelector('#newOrderContainer .orderRow[data-receipt="' + rid + '"]')) { const r = document.createElement('div'); r.className = 'orderRow'; r.dataset.receipt = rid; document.getElementById('newOrderContainer').appendChild(r); }
    }
  });
  const tap = rid => page.evaluate(sel => document.querySelector(sel).click(), rowSel(rid));

  // nobody signed in: nothing is recorded
  const before = acts.length;
  await printRun(['3521000001', '3521000002']); await flush(page);
  assert.strictEqual(mine(device).length, 0, device + ': nothing recorded while nobody is signed in');
  assert.strictEqual(await page.evaluate(() => StationActivity.who()), null);
  await addRows(); await tap('3521000011'); await wait(700); await tap('3521000011'); await wait(400);
  assert.strictEqual(liveOf(device).length, 0, device + ': nobody signed in: nothing is in hand');

  // the name set in the order chat is the sign-in
  await page.evaluate(() => BritesChat.open('3521000777'));
  await until(() => page.$('#bcWho'), 'the chat');
  await page.evaluate(() => { document.getElementById('bcWho').click(); const i = document.querySelector('#bcWho input'); i.value = 'Nora Night'; i.dispatchEvent(new Event('blur')); });
  await until(() => page.evaluate(() => { const w = StationActivity.who(); return w && w.person === 'Nora Night'; }), 'the sign-in');

  // a design finished and its label printed: one print per label, one complete per order, with the pieces
  await printRun(['3521000003', '3521000004']); await flush(page);
  let got = mine(device);
  assert.deepStrictEqual(brief(got), ['print||0|0', 'complete|3521000003|3|1', 'complete|3521000004|1|1'], device + ': one print per label, one complete per order');
  assert(/ order label · 2 orders$/.test(got[0].detail), 'the label detail: ' + got[0].detail);
  look(got, { person: 'Nora Night', station: 'design', device });

  // Undo reverses exactly what was credited
  await page.evaluate(() => handleUndoComplete()); await flush(page);
  got = mine(device).slice(3);
  assert.deepStrictEqual(brief(got), ['undo|3521000003|3|1', 'undo|3521000004|1|1'], device + ': undo reverses the pieces and orders credited');

  // an order-chat message: counted, never its words
  await page.evaluate(() => { const i = document.getElementById('bcInput'); i.value = 'Ship it to Jane Doe, 12 Main St, engrave her initials'; i.dispatchEvent(new Event('input')); });
  await page.evaluate(() => { const b = document.getElementById('bcSend'); b.disabled = false; b.click(); });
  await until(async () => { await flush(page); return mine(device).some(e => e.action === 'note'); }, 'the chat note');
  const note = mine(device).filter(e => e.action === 'note');
  assert.deepStrictEqual(brief(note), ['note|3521000777|0|0']); assert.strictEqual(note[0].detail, 'order chat message sent');

  // orders the labels leave out: refused once, however often it is tried
  await page.evaluate(async () => {
    selectedOrders.clear(); const rid = '3521000005'; orderCache[rid] = [{ quantity: 1, title: 'plain thing' }]; allOpenReceipts.push({ receipt_id: rid }); selectedOrders.add(rid);
    await handleCompleteClick(); await handleCompleteClick();
  });
  await flush(page);
  const rej = mine(device).filter(e => e.action === 'reject');
  assert.deepStrictEqual(brief(rej), ['reject|3521000005|0|0'], device + ': an order left off the labels is refused once'); assert(/no recognised metal/.test(rej[0].detail));

  // a failed print is an error, and nothing is completed
  const n0 = mine(device).length;
  await page.evaluate(async () => {
    window.runPrintFrame = async () => ({ ok: false, error: 'The QR library did not load for 3521000099' });
    const a = '3521000008'; orderCache[a] = [{ quantity: 1 }]; pendingLists = { gold: [a] }; pendingJobs = buildPrintJobs(pendingLists); await proceedToPrint();
  });
  await flush(page);
  const tail = mine(device).slice(n0);
  assert.deepStrictEqual(brief(tail), ['error||0|0']); assert(/^print failed · The QR library did not load for #$/.test(tail[0].detail), tail[0].detail);

  // the Charm Sorter's own commands are not this page's person's work
  if (device === 'design-1') {
    const n1 = mine(device).length;
    await page.evaluate(async () => {
      const rid = '3521000006'; orderCache[rid] = [{ quantity: 1 }]; allOpenReceipts.push({ receipt_id: rid });
      await commitCompletion([rid], { setId: 'S1', completedBy: 'Charm Sorter', labels: { setId: 'S1', files: [1] } });
      await handleUndoComplete({ remote: true });
      const held = '3521000007'; orderCache[held] = [{ quantity: 1, title: 'plain thing' }]; allOpenReceipts.push({ receipt_id: held }); selectedOrders.add(held);
      await handleCompleteClick({ remote: true });
    });
    await flush(page);
    assert.deepStrictEqual(mine(device).slice(n1).map(e => e.action + '|' + e.orderId), [], 'the sorter commands a completion, an undo and a preview: none is this page\'s person');
  }
  // the live board: the orders picked here are in hand (one order = that order, several = "N orders selected", the scan time is the
  // first pick); putting one back, printing the labels or clearing the selection drops it; the Charm Sorter's own selection is not told
  await addRows();
  await page.evaluate(() => clearSelection()); await wait(700);
  const l0 = liveOf(device).length;
  await tap('3521000011'); await wait(700);
  await tap('3521000012'); await wait(700);
  await tap('3521000012'); await wait(700);
  await page.evaluate(() => clearSelection()); await wait(700);
  assert.deepStrictEqual(liveBrief(device).slice(l0), ['work|order:3521000011:2', 'work|sheet:2 orders selected:3', 'work|order:3521000011:2', 'idle|3521000011'],
    device + ': a pick puts the order in hand with its pieces, a second makes it a batch, putting one back returns to the order, clearing drops it: ' + liveBrief(device).join(' , '));
  const mineLive = liveOf(device).slice(l0);
  assert.strictEqual(mineLive[0].order.scannedAt, mineLive[1].order.scannedAt, device + ': the scan time is the first pick');
  assert.strictEqual(mineLive[1].order.scannedAt, mineLive[2].order.scannedAt);
  assert.deepStrictEqual(mineLive[0].order.pieces.map(p => [p.sku, p.listingId]), [['A1', '111222333'], ['A1', '111222333']], device + ': one piece per unit');
  for (const l of lives.filter(x => x.device === device)) { assert.strictEqual(l.person, 'Nora Night'); assert.strictEqual(l.station, 'design'); assert(!('sandbox' in l)); }
  if (device === 'design-1') {
    const l1 = liveOf(device).length;
    await page.evaluate(async () => { await selectRow(document.querySelector('#newOrderContainer .orderRow[data-receipt="3521000011"]'), { silent: true }); });
    await wait(900);
    assert.strictEqual(liveOf(device).length, l1, "design-1: the Charm Sorter's own selection is not told as this person's order in hand");
  }
  assert.strictEqual(errors.filter(e => !/Failed to fetch|Load failed|gstatic|does not provide an export/.test(e)).length, 0, device + ' page errors: ' + errors.join(' | '));
  await ctx.close();
  console.log(device + ': print, complete, undo, chat note, reject, error once each; nothing signed out; sorter commands not recorded');
}

/* ── design-message.html and design-message-1.html ── */
async function messagePage(browser, file, device) {
  const { ctx, page } = await open(browser, file);
  await until(() => page.evaluate(() => window.StationSession && StationSession.page() && window.StationActivity), file + ' loaded');
  const enter = (order, how) => page.evaluate(([o, h]) => { const i = document.getElementById('etsyOrderNumber'); i.value = o; const ev = new KeyboardEvent('keydown', { key: 'Enter', bubbles: true }); if (h) ev.stationHow = h; i.dispatchEvent(ev); }, [order, how]);

  await enter('3521009998'); await wait(600); await flush(page);
  assert.strictEqual(mine(device).length, 0, device + ': nothing while nobody is signed in');

  await page.focus('#employeeNumberInput');
  for (const d of PIN) await page.keyboard.press(d);
  await page.click('#employeeLoginBtn');
  await until(() => page.evaluate(() => { const w = StationActivity.who(); return w && w.person === 'Rosa Designer'; }), 'the PIN sign-in');

  await enter('3521009999'); await until(async () => { await flush(page); return mine(device).length >= 1; }, 'the typed scan');
  await enter('3521009997', 'scan'); await until(async () => { await flush(page); return mine(device).length >= 2; }, 'the phone scan');
  await page.evaluate(() => { document.getElementById('etsyOrderNumber').value = '3521009999'; document.getElementById('britesMsgInput').value = 'Please engrave her initials for Jane Doe, 12 Main St'; document.getElementById('goScreenTwoBtn').click(); });
  await until(async () => { await flush(page); return mine(device).length >= 3; }, 'the message note');
  const got = mine(device);
  assert.deepStrictEqual(brief(got), ['scan|3521009999|3|0', 'scan|3521009997|3|0', 'note|3521009999|0|0'], device + ': two orders opened (3 pieces each), one message');
  assert.deepStrictEqual(got.map(e => e.detail), ['typed', 'phone scan', 'order chat message sent']);
  look(got, { person: 'Rosa Designer', station: 'design', device });

  // an order Etsy has no answer for: the scan stands (0 pieces) and the failure is one error; then a picture dropped into the chat
  // that uploads (a note) and one that does not (an error): fixed words, never the picture
  await enter('3521009995'); await until(async () => { await flush(page); return mine(device).length >= 5; }, 'the failed lookup');
  for (const ok of [true, false]) {
    await page.evaluate(async ok => {
      window.uploadViaResumable = async () => { if (!ok) throw new Error('storage down'); return 'http://127.0.0.1/x.png'; };
      const dt = new DataTransfer(); dt.items.add(new File(['x'], 'a.png', { type: 'image/png' }));
      document.getElementById('customerMessageHistory').dispatchEvent(new DragEvent('drop', { dataTransfer: dt, bubbles: true, cancelable: true }));
    }, ok);
    await wait(400);
  }
  await until(async () => { await flush(page); return mine(device).length >= 7; }, 'the picture events');
  assert.deepStrictEqual(brief(mine(device).slice(3)), ['scan|3521009995|0|0', 'error|3521009995|0|0', 'note|3521009995|0|0', 'error|3521009995|0|0'], device + ': a failed lookup is a scan and an error; a picture a note, a failed one an error');
  assert.deepStrictEqual(mine(device).slice(3).map(e => e.detail), ['typed', 'order lookup failed', 'order chat image sent', 'order chat image failed']);

  // the live board: an order opened here is in hand until the next scan; an order Etsy has no answer for is not
  await wait(700);
  assert.deepStrictEqual(liveBrief(device), ['work|order:3521009999:3', 'work|order:3521009997:3', 'idle|3521009997'], device + ': ' + liveBrief(device).join(' , '));
  assert.strictEqual(liveOf(device)[1].order.note, 'phone scan', device + ': the phone scan says so');
  for (const l of lives.filter(x => x.device === device)) { assert.strictEqual(l.person, 'Rosa Designer'); assert.strictEqual(l.station, 'design'); }

  await page.evaluate(() => document.getElementById('signOutBtn').click());
  await enter('3521009996'); await wait(600); await flush(page);
  assert.strictEqual(mine(device).length, 7, device + ': nothing after Sign Out');
  assert.strictEqual(liveOf(device).length, 3, device + ': nothing in hand after Sign Out');
  await ctx.close();
  console.log(device + ': PIN sign-in, two scans with pieces, a message note; nothing signed out');
}

/* ── etsy-mail-1.html ── */
async function inbox(browser) {
  const seed = () => { try { localStorage.setItem('etsymail_session', 'tok'); localStorage.setItem('etsymail_session_profile', JSON.stringify({ username: 'paul', displayName: 'Paul Inbox', role: 'owner' })); } catch (_) {} };
  const { ctx, page } = await open(browser, 'etsy-mail-1.html', seed);
  const device = 'etsy-mail-1';
  await until(() => page.evaluate(() => window.StationActivity && StationActivity.who()), 'the inbox session');
  const reply = async (id, text) => {
    await page.click(`[data-id="${id}"]`); await page.waitForSelector('#emDraftText', { timeout: 10000 });
    await page.fill('#emDraftText', text); await page.click('#emSendEtsyBtn');
  };
  await reply(IDS.A, 'Thanks! Jane Doe, your order ships tomorrow.');
  await until(async () => { await flush(page); return mine(device).some(e => e.action === 'note'); }, 'the reply note');
  await reply(IDS.B, 'A second reply that will not go.');
  await until(async () => { await flush(page); return mine(device).some(e => e.action === 'error'); }, 'the failed reply');
  await page.waitForSelector("#emArchiveBtn", { timeout: 10000 });   // thread B, whose reply did not go, is still open
  failStatus = true; await page.click('#emArchiveBtn');                 // the server refuses the Done: an error, nothing completed
  await until(async () => { await flush(page); return mine(device).filter(e => e.action === 'error').length >= 2; }, 'the refused Done');
  failStatus = false; await wait(500); await page.waitForSelector("#emArchiveBtn", { timeout: 10000 });
  await page.click('#emArchiveBtn');
  await until(async () => { await flush(page); return mine(device).some(e => e.action === 'complete'); }, 'the conversation done');
  const got = mine(device);
  assert.deepStrictEqual(brief(got), ['note|3521000444|0|0', 'error||0|0', 'error||0|0', 'complete||0|0'], 'inbox: a reply, a failed reply, a refused Done, a conversation done, once each');
  // (the reply's note also says how long the customer waited: the fake thread's first message is an hour old)
  assert(/^reply sent · first reply (59|60|61)m$/.test(got[0].detail), 'the reply note: ' + got[0].detail);
  assert.deepStrictEqual(got.slice(1).map(e => e.detail), ['reply not sent', 'status change not saved', 'conversation done']);
  look(got, { person: 'Paul Inbox', station: 'inbox', device });
  // the live board: an opened conversation is in hand (its order when it names one, else "Customer conversation"); the reply sent
  // or the conversation done drops it; a refused reply or a refused Done does not
  await wait(900);
  assert.deepStrictEqual(liveBrief(device), ['work|order:3521000444:0', 'idle|3521000444', 'work|sheet:Customer conversation:0', 'idle|Customer conversation'], 'inbox live: ' + liveBrief(device).join(' , '));
  assert.strictEqual(liveOf(device)[0].order.customer, 'Cust A');
  for (const l of lives.filter(x => x.device === device)) { assert.strictEqual(l.person, 'Paul Inbox'); assert.strictEqual(l.station, 'inbox'); }
  await ctx.close();
  console.log('inbox: reply note, failed-reply error, conversation-done complete, once each');
}

(async () => {
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  try {
    await designPage(browser, 'design.html', 'design');
    await designPage(browser, 'design-1.html', 'design-1');
    await messagePage(browser, 'design-message.html', 'design-message');
    await messagePage(browser, 'design-message-1.html', 'design-message-1');
    await inbox(browser);
    // no PIN anywhere (it is only ever typed into the keypad); no customer text or address in anything recorded about the
    // people (the activity and session requests; the order chat's own post carries its message, as it always did)
    // (the login door's own request, { pinLogin }, is the one place the number goes: it is checked apart)
    const door = sent.filter(s => /\{"pinLogin":/.test(s));
    assert(door.length >= 2 && door.every(s => / \{"pinLogin":"\d{6}"\}$/.test(s)) && mapGets === 0, 'the sign-ins went to the login door only; the roster was never read');
    const all = sent.filter(s => !/\{"pinLogin":/.test(s)).join('\n');
    assert(!all.includes(PIN), 'the PIN reached a request');
    const recorded = sent.filter(s => /"activity"|"session"/.test(s)).join('\n') + '\n' + JSON.stringify(acts);
    for (const bad of CUSTOMER) assert(!recorded.includes(bad), 'customer text in an activity or session request: ' + bad);
    assert(/"activity"/.test(recorded), 'the activity requests were captured');
    assert(acts.length >= 20, 'events arrived at the door: ' + acts.length);
    console.log('design-activity: all passed (' + acts.length + ' events, no PIN or customer text in ' + sent.length + ' requests)');
  } finally { await browser.close(); server.close(); }
})().catch(e => { console.error(e); process.exit(1); });
