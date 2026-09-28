// The Sorting station (sorting.html) on the order timeline: "Sorting as: <name>" set inline and kept; the relay doc's old
// batch is not reloaded on open; a scanned batch records every order through StationTimeline.scanned(); a batch with a
// cancelled order shows ONE full-screen alert, marks that order's tiles, and its sticker is never printed (guarded,
// single and Print Row); printed stickers record "sorted"; a batch with two cancelled orders (one known only from
// Etsy's answer) still shows one alert listing both. Without station-timeline.js the page works as before.
// Network, Firebase, Materialize and StationTimeline are stubbed; every request off the fake origin is aborted.
//   NODE_PATH=$(npm root -g) PW_DIR=$(npm root -g)/playwright/node_modules CHROMIUM=... node tests/stations/sorting-timeline.cjs
const path = require('path'), assert = require('assert'), fs = require('fs');
const root = path.join(__dirname, '../..');
const pwDir = process.argv[2] || process.env.PW_DIR || path.join(root, 'node_modules');
const { chromium } = require(path.join(pwDir, 'playwright-core'));

const ORIGIN = 'http://sorting.test';
const A = '3700000001', B = '3700000002', C = '3700000003', E = '3700000005', F = '3700000006', G = '3700000007';
const TL_CANCELLED = new Set([B, G]);          // the timeline knows these are cancelled
const ETSY_CANCELLED = new Set([F]);           // Etsy's own answer says this one is
const day = Math.floor(Date.now() / 1000);
const receipt = rid => ({
  receipt_id: Number(rid), status: ETSY_CANCELLED.has(rid) ? 'Canceled' : 'Paid', message_from_buyer: '',
  transactions: (rid === A ? [1, 2] : [1]).map(n => ({ transaction_id: Number(rid) * 10 + n, receipt_id: Number(rid), listing_id: 1718000 + n,
    title: 'Silver Dog Charm Necklace', quantity: 1, variations: [{ formatted_name: 'Metal', formatted_value: 'Silver' }], expected_ship_date: day + 3 * 86400 }))
});

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
const stationStub = `
  window.__st = { init: null, scanned: [], did: [], guard: [] };
  const TL = new Set(${JSON.stringify([...TL_CANCELLED])});
  window.StationTimeline = {
    init(o) { __st.init = o; },
    scanned(id, o) { __st.scanned.push({ id, o }); return new Promise(r => setTimeout(() => r(TL.has(id)
      ? { cancelled: true, record: { by: 'Paul', why: 'Buyer asked to cancel', at: Date.now() } } : { cancelled: false }), 40)); },
    did(type, id, text, data) { __st.did.push({ type, id, text, data }); },
    guard(id) { __st.guard.push(id); return Promise.resolve(!TL.has(id)); },
    isCancelled(id) { return TL.has(id); }
  };`;

(async () => {
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  const errors = [];
  async function open({ station, relayDoc }) {
    const ctx = await browser.newContext({ viewport: { width: 1500, height: 900 } });
    const proxied = [];
    const js = body => ({ status: 200, contentType: 'text/javascript', body });
    await ctx.route('**/*', r => {
      const u = new URL(r.request().url());
      if (/gstatic\.com$/.test(u.hostname)) return r.fulfill(js(/firebase-app-compat/.test(u.pathname) ? firebaseStub : ''));
      if (/materialize/.test(u.pathname)) return r.fulfill(js(materializeStub));
      if (/jquery/.test(u.pathname)) return r.fulfill(js(fs.existsSync('/usr/share/javascript/jquery/jquery.min.js') ? fs.readFileSync('/usr/share/javascript/jquery/jquery.min.js') : ''));
      if (u.origin !== ORIGIN) return r.abort();
      const p = decodeURIComponent(u.pathname);
      if (p === '/sorting.html') return r.fulfill({ status: 200, contentType: 'text/html', body: fs.readFileSync(path.join(root, 'sorting.html')) });
      if (p === '/order-timeline.js') return r.fulfill(js(fs.readFileSync(path.join(root, 'order-timeline.js'))));
      if (p === '/station-timeline.js') return station ? r.fulfill(js(stationStub)) : r.fulfill({ status: 404, body: '' });
      if (p === '/QR Printer.html') return r.fulfill({ status: 200, contentType: 'text/html', body: '<!doctype html><title>printer stub</title>' });
      if (p === '/.netlify/functions/etsyOrderProxy') { const id = u.searchParams.get('orderId'); proxied.push(id); return r.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify(receipt(id)) }); }
      if (p === '/.netlify/functions/etsyImages') return r.fulfill({ status: 200, contentType: 'application/json', body: '[]' });
      if (p.startsWith('/.netlify/functions/')) return r.fulfill({ status: 200, contentType: 'application/json', body: '{}' });
      return r.fulfill({ status: 404, body: '' });
    });
    await ctx.addInitScript(doc => {
      localStorage.setItem('access_token', 'tok'); localStorage.setItem('refresh_token', 'ref');
      localStorage.setItem('token_expires_at', String(Math.floor(Date.now() / 1000) + 7200));
      window.__relayDoc = doc;
    }, relayDoc);
    const page = await ctx.newPage();
    page.on('pageerror', e => errors.push(e.message));
    await page.goto(`${ORIGIN}/sorting.html`);
    await page.waitForFunction(() => window.SortTL && document.querySelector('#sortingAsChip .st-as-name') && window.__snap && window.__snap.cb);
    return { ctx, page, proxied };
  }
  const iso = ms => new Date(Date.now() - ms).toISOString();
  const tilesFor = (page, n) => page.waitForFunction(n => (window.cachedOrderItems || []).length === n && document.getElementById('previewCell' + (n - 1)), n, { timeout: 15000 });
  const cellOf = (page, rid) => page.evaluate(rid => window.cachedOrderItems.findIndex(t => String(t.receipt_id) === rid), rid);
  const printerFrames = page => page.evaluate(() => [...document.querySelectorAll('iframe')].filter(f => /QR%20Printer|QR Printer/.test(f.src)).length);

  // ── with StationTimeline: the operator chip, and the old relay batch is not reloaded on open ──
  let { ctx, page, proxied } = await open({ station: true, relayDoc: { 'Order Number': '111,222', 'Shipping Label Timestamps': iso(2 * 3600e3) } });
  await page.waitForTimeout(400);
  assert.deepStrictEqual(proxied, [], 'the batch sent two hours ago is not loaded again on open');
  assert.strictEqual(await page.inputValue('#etsyOrderNumber'), '111,222', 'its numbers wait in the field, loaded only on Enter');
  let st = await page.evaluate(() => ({ station: __st.init && __st.init.station, device: __st.init && __st.init.device, scanned: __st.scanned.length, chip: document.getElementById('sortingAsChip').textContent.replace(/\s+/g, ' ').trim() }));
  assert.strictEqual(st.station, 'sorting'); assert.strictEqual(st.device, 'sorting-1');
  assert.strictEqual(st.scanned, 0, 'no scan recorded on open');
  assert(/^Sorting as: Set your name/.test(st.chip), 'the chip asks for a name: ' + st.chip);
  await page.click('#sortingAsChip .st-as-name');
  await page.waitForSelector('#sortingAsChip .st-as-input:not([hidden])');
  await page.keyboard.type('Maya'); await page.keyboard.press('Enter');
  st = await page.evaluate(() => ({ saved: localStorage.getItem('sorting.employee'), shown: document.querySelector('#sortingAsChip .st-as-name').textContent, input: document.querySelector('#sortingAsChip .st-as-input').hidden, emp: __st.init.getEmployee() }));
  assert.deepStrictEqual(st, { saved: 'Maya', shown: 'Maya', input: true, emp: 'Maya' }, 'the name is set inline, kept, and handed to StationTimeline');
  console.log('chip: set inline once, kept in localStorage, read through getEmployee; the old relay batch is not reloaded on open');

  // ── a sheet QR batch with one cancelled order ──
  const sent = iso(0);
  await page.evaluate(d => __fireSnap(d), { 'Order Number': [A, B, C].join(','), 'Shipping Label Timestamps': sent });
  await tilesFor(page, 4);
  await page.waitForSelector('#stCancelAlert.on', { timeout: 10000 });
  st = await page.evaluate(() => ({ scanned: __st.scanned.map(s => ({ id: s.id, how: s.o.how, quiet: s.o.quiet, batch: s.o.extra && s.o.extra.batch, size: s.o.extra && s.o.extra.batchSize })) }));
  assert.deepStrictEqual(st.scanned.map(s => s.id), [A, B, C], 'every order of the batch is recorded as scanned');
  assert(st.scanned.every(s => s.how === 'sheet QR' && s.quiet === true && s.batch === 'sort-' + sent && s.size === 3), 'each scan says sheet QR, carries the batch id, and asks the module to stay quiet: ' + JSON.stringify(st.scanned));
  await page.waitForTimeout(6300);   // past the 6 s fallback that could raise the alert a second time
  const alert = await page.evaluate(() => ({ shown: SortTL.alertsShown, inDom: document.querySelectorAll('#stCancelAlert').length,
    head: document.querySelector('#stCancelAlert .st-h1').textContent + ' — ' + document.querySelector('#stCancelAlert .st-h2').textContent, orders: [...document.querySelectorAll('#stCancelAlert .st-list .st-who')].map(o => o.dataset.order),
    text: document.querySelector('#stCancelAlert .st-list .st-who').textContent, scan: document.querySelector('#stCancelAlert .st-scan').textContent, button: document.querySelector('#stCancelAlert .st-ok').textContent, logged: __st.did.filter(d => d.type === 'cancelAlert').map(d => d.id) }));
  assert.strictEqual(alert.shown, 1, 'the alert shows once'); assert.strictEqual(alert.inDom, 1);
  assert.strictEqual(alert.head, 'CANCELLED ORDER — DO NOT PROCEED');
  assert.deepStrictEqual(alert.orders, [B], 'it lists the cancelled order only');
  assert(/Buyer asked to cancel/.test(alert.text) && /by Paul/.test(alert.text), 'with the reason and who cancelled: ' + alert.text);
  assert(/^Scanned at Sorting by Maya · this scan is on the order's timeline$/.test(alert.scan), 'and who scanned it: ' + alert.scan);
  assert.strictEqual(alert.button, 'Understood');
  assert.deepStrictEqual(alert.logged, [B], 'the alert is recorded once (cancelAlert)');
  const iB = await cellOf(page, B), iA = await cellOf(page, A), iC = await cellOf(page, C);
  const tiles = await page.evaluate(([iB, iA]) => {
    const pb = document.getElementById('previewCell' + iB), oc = document.getElementById('orderCell' + iB), cs = getComputedStyle(oc);
    return { marked: pb.classList.contains('st-cancelled'), label: getComputedStyle(pb, '::after').content, strike: cs.textDecorationLine, color: cs.color,
      others: [...document.querySelectorAll('.preview-box.st-cancelled')].map(e => e.id), aMarked: document.getElementById('orderCell' + iA).classList.contains('st-cancelled') };
  }, [iB, iA]);
  assert(tiles.marked && /CANCELLED — DO NOT SORT/.test(tiles.label), 'the cancelled order\'s tile says CANCELLED — DO NOT SORT: ' + tiles.label);
  assert(tiles.strike === 'line-through' && tiles.color === 'rgb(211, 47, 47)', 'its order number is red and struck through: ' + JSON.stringify(tiles));
  assert.deepStrictEqual(tiles.others, ['previewCell' + iB], 'no other tile is marked'); assert(!tiles.aMarked);
  await page.click('#stCancelAlert .st-ok');
  await page.waitForFunction(() => !document.querySelector('#stCancelAlert'), null, { timeout: 3000 });
  console.log('batch: 3 scans recorded (sheet QR, batch id, quiet), one alert for the cancelled order, its tile marked red and struck through');

  // ── printing: the cancelled order's sticker is guarded; a printed sticker records sorted ──
  await page.evaluate(i => openIframePrinterForListing(i), iB);
  await page.waitForFunction(b => __st.guard.includes(b), B);
  await page.waitForTimeout(200);
  st = await page.evaluate(() => ({ frames: [...document.querySelectorAll('iframe')].length, sorted: __st.did.filter(d => d.type === 'sorted').map(d => d.id) }));
  assert.strictEqual(await printerFrames(page), 0, 'the cancelled order\'s sticker is not printed');
  assert.deepStrictEqual(st.sorted, [], 'and it is not recorded as sorted');
  await page.evaluate(i => openIframePrinterForListing(i), iA);
  await page.waitForFunction(() => [...document.querySelectorAll('iframe')].some(f => /QR/.test(f.src)), null, { timeout: 5000 });
  st = await page.evaluate(() => ({ printed: JSON.parse(localStorage.getItem('qrPrintAll')).userTypedOrderNum, sorted: __st.did.filter(d => d.type === 'sorted').map(d => [d.id, d.data && d.data.how]) }));
  assert.strictEqual(st.printed, A);
  assert.deepStrictEqual(st.sorted, [[A, 'print']], 'a printed sticker records its order as sorted');
  await page.evaluate(() => { __st.guard.length = 0; return handlePrintRowClick(); });
  await page.waitForFunction(() => localStorage.getItem('qrPrintBatch'), null, { timeout: 5000 });
  st = await page.evaluate(() => ({ batch: JSON.parse(localStorage.getItem('qrPrintBatch')).map(o => o.userTypedOrderNum), guard: __st.guard.slice().sort(),
    sorted: __st.did.filter(d => d.type === 'sorted' && d.data.how === 'row').map(d => d.id), toast: __toasts.join(' | ') }));
  assert.deepStrictEqual(st.batch, [A, C], 'Print Row leaves the cancelled order out');
  assert.deepStrictEqual(st.guard, [A, C], 'and guards the others');
  assert.deepStrictEqual(st.sorted, [A, C], 'each printed sticker records sorted');
  assert(st.toast.includes('Not printed') && st.toast.includes(B), 'the skipped order is named: ' + st.toast);
  console.log('print: the cancelled sticker is guarded (single) and left out (Print Row); printed stickers record sorted');

  // ── the same relay doc delivered again is not reloaded ──
  const before = proxied.length, scans = await page.evaluate(() => __st.scanned.length);
  await page.evaluate(d => __fireSnap(d), { 'Order Number': [A, B, C].join(','), 'Shipping Label Timestamps': sent, 'Employee Name': 'SortScannerBot' });
  await page.waitForTimeout(300);
  assert.strictEqual(proxied.length, before, 'a repeat of the loaded batch is not fetched again');
  assert.strictEqual(await page.evaluate(() => __st.scanned.length), scans, 'nor scanned again');

  // ── two cancelled orders in one batch (one only Etsy knows about): still one alert, listing both ──
  await page.evaluate(d => __fireSnap(d), { 'Order Number': [E, F, G].join(','), 'Shipping Label Timestamps': iso(-1000) });
  await tilesFor(page, 3);
  await page.waitForFunction(() => document.querySelectorAll('#stCancelAlert .st-list .st-who').length === 2, null, { timeout: 10000 });
  st = await page.evaluate(() => ({ shown: SortTL.alertsShown, orders: [...document.querySelectorAll('#stCancelAlert .st-list .st-who')].map(o => o.dataset.order).sort(),
    marked: [...document.querySelectorAll('.preview-box.st-cancelled')].length, sub: document.querySelector('#stCancelAlert .st-scan').textContent }));
  assert.strictEqual(st.shown, 2, 'one alert for this batch (the first batch had the other)');
  assert.deepStrictEqual(st.orders, [F, G].sort(), 'listing both cancelled orders');
  assert.strictEqual(st.marked, 2, 'both tiles are marked'); assert(/Scanned at Sorting by Maya · 2 cancelled orders/.test(st.sub), st.sub);
  console.log('two cancelled orders in a batch (timeline + Etsy status): one alert listing both, both tiles marked');
  await ctx.close();

  // ── without station-timeline.js: nothing changes (a fresh relay batch still loads on open, prints right away) ──
  ({ ctx, page, proxied } = await open({ station: false, relayDoc: { 'Order Number': [A, B, C].join(','), 'Shipping Label Timestamps': iso(30e3) } }));
  await tilesFor(page, 4);
  assert.deepStrictEqual(proxied, [A, B, C], 'a batch sent while the page was opening still loads');
  const iB2 = await cellOf(page, B);
  st = await page.evaluate(i => { openIframePrinterForListing(i); return { frames: [...document.querySelectorAll('iframe')].filter(f => /QR/.test(f.src)).length,
    alert: !!document.querySelector('#stCancelAlert'), marked: document.querySelectorAll('.st-cancelled').length, st: typeof window.StationTimeline }; }, iB2);
  assert.deepStrictEqual(st, { frames: 1, alert: false, marked: 0, st: 'undefined' }, 'with no StationTimeline the sticker prints at once, no alert, no marks');
  await page.evaluate(() => handlePrintRowClick());
  assert.strictEqual(JSON.parse(await page.evaluate(() => localStorage.getItem('qrPrintBatch'))).length, 3, 'Print Row prints every order as before');
  console.log('without StationTimeline: the page behaves as before');
  await ctx.close();

  await browser.close();
  assert.deepStrictEqual(errors, [], 'no page errors: ' + errors.join(' | '));
  console.log('sorting-timeline: all passed');
})().catch(e => { console.error(e); process.exit(1); });
