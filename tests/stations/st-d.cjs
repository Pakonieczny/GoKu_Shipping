// Part D of the station tracking (Paul, 28 Sep): labels printed at the Design Station on each order's timeline.
//  · design.html and design-1.html: once the print page confirms the labels, every order on them gets `labelPrinted`
//    (station design, label sheetQR, who = the name the station knows) and its "DESIGNED :)" completion as a note, sent
//    through the real order-timeline.js outbox to firebaseOrders (the sandbox door for design-1 in the sandbox), stored
//    by the real _orderTimeline.add; nobody named → by "" and signedIn false; the 6-digit PIN never goes anywhere; a
//    failed print records nothing; without order-timeline.js the print completes as before; the recorded note replaces
//    the derived "DESIGNED :)" stamp (dedupe), so the seal names the real person.
//  · the sorter's Review tab "Print QR label" (CustomPrint, charm-nest-bridge.js): a printed sticker records
//    `labelPrinted` (station design, label custom, who); a failed print records nothing.
// No browser, no network: the page code runs in a vm with fakes.   node tests/stations/st-d.cjs
const assert = require('node:assert/strict'), fs = require('node:fs'), path = require('node:path'), vm = require('node:vm');
const root = path.join(__dirname, '../..');
const TL = require(path.join(root, 'netlify/functions/_orderTimeline.js'));
const read = f => fs.readFileSync(path.join(root, f), 'utf8');
const slice = (code, start, end) => { const i = code.indexOf(start); assert(i >= 0, 'missing: ' + start); const j = code.indexOf(end, i + start.length); assert(j > i, 'missing end: ' + end); return code.slice(i, j + end.length); };
const wait = ms => new Promise(r => setTimeout(r, ms));
const PIN = '482915';

/* a page: order-timeline.js (real) + the page's constants, DsTimeline and proceedToPrint (real), everything else faked */
function designPage(file, { name, sandbox = false, timeline = true, printOk = true } = {}) {
  const html = read(file), store = new Map(), posts = [], calls = [];
  if (name) store.set('employee_name', name);
  store.set('employee_id', PIN);   // what a PIN login on the same origin keeps: never recorded
  const listeners = {};
  const ctx = vm.createContext({
    console, setTimeout, clearTimeout, Blob, URLSearchParams,
    localStorage: { getItem: k => (store.has(k) ? store.get(k) : null), setItem: (k, v) => store.set(k, String(v)), removeItem: k => store.delete(k) },
    sessionStorage: { getItem: () => (sandbox ? '1' : null), setItem() {}, removeItem() {} },
    location: { protocol: 'https:', origin: 'https://design.test', search: sandbox ? '?sandbox=1' : '' },
    navigator: { onLine: true },
    document: { addEventListener() {}, visibilityState: 'visible' },
    addEventListener: (t, fn) => { (listeners[t] = listeners[t] || []).push(fn); },
    fetch: async (url, opts) => { posts.push({ url, body: JSON.parse(opts.body) }); return { ok: true, status: 200, json: async () => ({ success: true }) }; }
  });
  ctx.window = ctx;
  if (timeline) vm.runInContext(read('order-timeline.js'), ctx);
  const btn = { disabled: false, innerHTML: '', textContent: '' };
  Object.assign(ctx, {
    FN: '/fn', NS: sandbox ? ':sandbox' : '', SANDBOX: sandbox, METAL_BY_KEY: { '14k': { label: '14K Gold' }, ss: { label: 'Silver' } },
    $: () => btn, $$: () => [], cssEsc: x => x, toast: (m, k) => calls.push(['toast', m, k || '']), closeDlg() {}, randomString: () => 'nonce',
    resolvePrintPage: async () => (file === 'design.html' ? 'design-print.html' : 'design-print-1.html'),
    setItemEssential: (k, v) => store.set(k, v), Activity: { show() {}, hide() {} },
    runPrintFrame: async () => (printOk ? { ok: true, labels: 2 } : { ok: false, error: 'The print page did not respond in time' }),
    removePreviewBoxesForOrder() {}, orderCache: {}, selectedOrders: new Set(), completedOrders: new Set(), allOpenReceipts: [], currentReceipts: [],
    persistCompleted: async ids => calls.push(['completed', [...ids]]), RT: { clientId: 't' }, rtSelected: new Set(),
    persistSelection() {}, updateCounters() {}, applyMetalFilter() {}, refreshChrome() {},
    markBritesDesigned: async ids => calls.push(['designed', [...ids]]), archiveOrders: async () => {},
    commitCompletion: async ids => { calls.push(['designed', [...ids]]); return { completed: ids }; }
  });
  const ls = slice(html, '\nconst LS = {', '\n};\n');
  const ds = slice(html, 'const DsTimeline = (() => {', '\n})();\n');
  const da = slice(html, 'const DsActivity = (() => {', '\n})();\n');   // the efficiency log (no station-activity.js here: it records nothing)
  const print = slice(html, 'async function proceedToPrint() {', '\n}\n');
  vm.runInContext(`${ls}\n${ds}\n${da}\nvar pendingJobs = null, pendingLists = null, lastClearedReceipts = [];\n${print}\nthis.__go = (jobs, lists) => { pendingJobs = jobs; pendingLists = lists; return proceedToPrint(); };`, ctx);
  return { ctx, posts, calls, store };
}
const A = '4100000001', B = '4100000002';
const LISTS = { '14k': [A, B], ss: [B] };
const JOBS = [{ metal: '14k', ids: [A, B], payload: 'B36|14k|x.y' }, { metal: 'ss', ids: [B], payload: 'B36|ss|y' }];
const timelinePosts = posts => posts.filter(p => /\/firebaseOrders/.test(p.url) && Array.isArray(p.body.timeline)).map(p => JSON.parse(JSON.stringify(p)));
function fakeDb() {
  const docs = new Map();
  const col = name => ({ doc: id => ({ path: name + '/' + id }) });
  return { docs, collection: col, batch: () => { const w = []; return { set: (ref, d) => w.push([ref.path, d]), commit: async () => { for (const [p, d] of w) docs.set(p, d); } }; } };
}

(async () => {
  // ── design.html, signed in: every order on the printed labels, once each, with who, through the real outbox ──
  let p = designPage('design.html', { name: 'Maya R.' });
  await p.ctx.__go(JOBS, LISTS);
  await wait(1300);
  let sent = timelinePosts(p.posts);
  assert.equal(sent.length, 1, 'one batch to the stations\' door');
  assert.equal(sent[0].url, '/.netlify/functions/firebaseOrders', 'the production door (no sandbox)');
  let evs = sent[0].body.timeline;
  const lp = evs.filter(e => e.type === 'labelPrinted'), notes = evs.filter(e => e.type === 'note');
  assert.deepEqual(lp.map(e => e.orderId).sort(), [A, B], 'labelPrinted for each order on the labels, once each');
  const minute = Math.floor(lp[0].at / 60000), b = lp.find(e => e.orderId === B);
  assert.deepEqual({ by: b.by, station: b.station, device: b.device, id: b.id, label: b.data.label, metals: b.data.metals, labels: b.data.labels, orders: b.data.orders, page: b.data.printPage, signedIn: b.data.signedIn, mode: b.mode },
    { by: 'Maya R.', station: 'design', device: 'design', id: `design-${B}-labelPrinted-${minute}`, label: 'sheetQR', metals: ['14k', 'ss'], labels: 2, orders: 2, page: 'design-print.html', signedIn: undefined, mode: 'station' });
  assert.match(b.text, /^QR label printed at the Design Station · 14K Gold, Silver$/);
  assert.deepEqual(notes.map(e => [e.orderId, e.text, e.data.stamp, e.data.kind, e.by, e.station, e.milestone]).sort(),
    [[A, 'DESIGNED :)', 'DESIGNED :)', 'designed', 'Maya R.', 'design', true], [B, 'DESIGNED :)', 'DESIGNED :)', 'designed', 'Maya R.', 'design', true]]);
  assert(!JSON.stringify(p.posts).includes(PIN), 'the PIN is never sent');
  assert.deepEqual(p.calls.filter(c => c[0] === 'designed').map(c => c[1]), [[A, B]], 'the station still completes and stamps DESIGNED :) as before');
  // stored by the real server helper through the open door (station types only), and the derived stamp is replaced
  const db = fakeDb();
  const out = await TL.add(db, { serverTimestamp: () => 0 }, evs, { source: 'station', stationOnly: true });
  assert.equal(out.ids.length, 4, 'all four events pass the stations\' door');
  const stored = [...db.docs.values()];
  assert(stored.every(d => d.station === 'design' && d.by === 'Maya R.' && d.device === 'design' && d.source === 'station'));
  const recorded = stored.map(d => Object.assign({}, d));
  const derived = [{ orderId: B, type: 'note', at: lp[0].at + 4000, by: 'Design', station: 'design', text: 'DESIGNED :)', data: { stamp: 'DESIGNED :)' }, milestone: true },
                   { orderId: B, type: 'note', at: lp[0].at + 9000, by: '', station: 'design', text: 'Design complete', data: { stamp: 'designComplete' }, milestone: true }];
  assert.deepEqual(TL.dedupe(recorded.filter(e => e.orderId === B), derived), [], 'the derived "DESIGNED :)" (sender "Design") and plain completion give way to the recorded one');
  const where = TL.whereOf(recorded.filter(e => e.orderId === B), null, { record: true });
  assert(where.designed && where.by === 'Maya R.' && where.station === 'design', 'the order reads as designed at the Design Station by Maya R.: ' + where.text);
  console.log('design.html: labelPrinted (sheetQR, metals, labels) and the DESIGNED :) note for each order, by the signed-in name, stored as station design');

  // ── the same print again within the minute: the same ids (idempotent) ──
  const ids1 = evs.map(e => e.id).sort();
  await p.ctx.__go(JOBS, LISTS); await wait(1300);
  const again = timelinePosts(p.posts).slice(1).flatMap(x => x.body.timeline);
  if (Math.floor(Date.now() / 60000) === minute) assert.deepEqual(again.map(e => e.id).sort(), ids1, 'a retry in the same minute writes the same documents');
  console.log('a retry keeps the same ids (orderId~type~device-order-type-minute)');

  // ── nobody named: by "" and signedIn false (never "Design") ──
  p = designPage('design.html', {});
  await p.ctx.__go(JOBS, LISTS); await wait(1300);
  evs = timelinePosts(p.posts)[0].body.timeline;
  assert(evs.length === 4 && evs.every(e => e.by === '' && e.data.signedIn === false), 'not signed in: ' + JSON.stringify(evs.map(e => [e.by, e.data.signedIn])));
  console.log('nobody named at the station: by "" and data.signedIn false');

  // ── a print that failed records nothing and completes nothing ──
  p = designPage('design.html', { name: 'Maya R.', printOk: false });
  await p.ctx.__go(JOBS, LISTS); await wait(1300);
  assert.equal(timelinePosts(p.posts).length, 0, 'no event for a label that was not printed');
  assert(!p.calls.some(c => c[0] === 'designed') && p.calls.some(c => c[0] === 'toast' && c[2] === 'bad'), 'the page says it failed, as before');
  console.log('failed print: nothing recorded, nothing completed');

  // ── without order-timeline.js: the print completes exactly as before ──
  p = designPage('design.html', { name: 'Maya R.', timeline: false });
  await p.ctx.__go(JOBS, LISTS); await wait(200);
  assert.deepEqual(p.calls.filter(c => c[0] === 'designed').map(c => c[1]), [[A, B]], 'completed as before with no timeline client');
  assert.equal(timelinePosts(p.posts).length, 0, 'nothing sent to the timeline');
  console.log('no timeline client: the station works as before');

  // ── design-1.html in the sandbox: its own device, the sandbox door ──
  p = designPage('design-1.html', { name: 'Leo', sandbox: true });
  await p.ctx.__go(JOBS, LISTS); await wait(1300);
  sent = timelinePosts(p.posts);
  assert.equal(sent.length, 1); assert.match(sent[0].url, /firebaseOrders\?sandbox=1$/, 'sandbox door');
  evs = sent[0].body.timeline;
  assert(evs.length === 4 && evs.every(e => e.device === 'design-1' && e.station === 'design' && e.by === 'Leo' && e.sandbox === true));
  assert.equal(evs.find(e => e.type === 'labelPrinted').data.printPage, 'design-print-1.html');
  assert.deepEqual(p.calls.filter(c => c[0] === 'designed').map(c => c[1]), [[A, B]], 'design-1 still commits through commitCompletion');
  console.log('design-1.html: device design-1, sandbox events through the sandbox door');

  // ── the sorter's Review tab "Print QR label" (CustomPrint) ──
  const bridge = read('charm-nest-bridge.js');
  const cpCode = slice(bridge, 'const CustomPrint = window.CustomPrint = (() => {', '\n})();\n');
  async function customPrint({ ok, done = false }) {
    const events = [], msgs = [], api = [];
    const ctx = vm.createContext({ console, setTimeout, clearTimeout, requestAnimationFrame: fn => setTimeout(fn, 0), location: { origin: 'https://sorter.test' } });
    ctx.window = ctx;
    const listeners = new Set();
    Object.assign(ctx, {
      addEventListener: (t, fn) => { if (t === 'message') listeners.add(fn); }, removeEventListener: (t, fn) => listeners.delete(fn),
      document: { addEventListener() {}, removeEventListener() {}, querySelector: () => null, querySelectorAll: () => [], createElement: () => ({ style: {}, setAttribute() {}, remove() {} }), body: { appendChild(f) {
        const nonce = String(f.src).split('#notify=')[1].split('&')[0];
        setTimeout(() => { for (const fn of [...listeners]) fn({ origin: 'https://sorter.test', data: { source: 'qr-printer', nonce, phase: 'done', ok, error: ok ? '' : 'the print dialog was closed' } }); }, 5);
      } } },
      localStorage: { setItem() {}, removeItem() {}, getItem: () => null },
      B: { maps: { customWrites: new Map(), customDone: {} }, employee: 'Paul K.' }, employeeName: () => 'Paul K.',
      Review: { render() {}, cardKey: r => r.key }, OrderWin: { isOpen: () => false }, Orders: { interpretAll() {}, render() {} }, renderRail() {}, updateTopSub() {}, RunCtl: { poke() {} },
      O: { sortingLabel: () => ({ userTypedOrderNum: '4100000009', items: [] }) }, esc: x => String(x),
      api: async (fn, body) => { api.push(body); return { record: { key: body.key, stamps: [] } }; },
      toast: (m, k) => msgs.push([m, k || '']), agent() {}, humanAct: () => true, piecesOfRows: rows => (rows || []).length,
      SheetEvents: { order: e => events.push(e) }
    });
    vm.runInContext(cpCode, ctx);
    const row = { key: '4100000009_91', state: 'pulled', poolIds: [], order: { receiptId: '4100000009' }, line: { transactionId: '91', sku: 'CUS-1', title: 'Custom charm' }, spec: { special: { label: 'Custom order', kind: 'photo' } } };
    ctx.CustomPrint.print({ key: 'cu:4100000009_91', kind: 'customOrder', rows: [row], row, done, record: done ? { key: row.key } : null });
    await wait(120);
    return JSON.parse(JSON.stringify({ events, msgs, api }));
  }
  let cp = await customPrint({ ok: true });
  assert.equal(cp.events.length, 1, 'one event for the printed sticker');
  const e = cp.events[0];
  assert.deepEqual({ type: e.type, orderId: e.orderId, by: e.by, station: e.station, device: e.device, id: e.id, data: e.data },
    { type: 'labelPrinted', orderId: '4100000009', by: 'Paul K.', station: 'design', device: 'charm-nest-1', id: `charm-nest-1-4100000009-labelPrinted-${e.at}`, data: { label: 'custom', printPage: 'QR Printer.html', lines: 1, print: 1 } });
  assert.match(e.text, /^Custom QR label printed at the Design Station \(Review\)$/);
  assert(cp.api.some(b => b.op === 'customPut' && b.by === 'Paul K.'), 'the completion (server sealPrinted stamp) is saved as before');
  const kept = TL.clean(e, { source: 'sorter' });
  assert.equal(kept.doc.station, 'design', 'the server keeps station design');
  cp = await customPrint({ ok: true, done: true });
  assert.equal(cp.events[0].data.again, true, 'Print again says so');
  cp = await customPrint({ ok: false });
  assert.equal(cp.events.length, 0, 'a sticker not printed records nothing');
  assert(!cp.api.some(b => b.op === 'customPut') && cp.msgs.some(x => /not printed|didn't open/.test(x[0])), 'and nothing is completed, as before');
  console.log('sorter Review "Print QR label": labelPrinted (custom, station design, who) once printed; nothing when the print fails');

  console.log('st-d: all passed');
})().catch(e => { console.error(e); process.exit(1); });
