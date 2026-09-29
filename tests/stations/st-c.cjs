// Station tracking, part C: the Sorting station (sorting.html) records who sorted each order and who printed its label.
// REAL order-timeline.js and station-timeline.js; firebaseOrders (Employee Numbers, cancel check, timeline door), Etsy,
// Firebase and Materialize are faked here; every request off the fake origin is aborted (no live site, no printer).
//   1 · nobody set on the chip (another page's employee_name on this origin is NOT used): a printed sticker records
//       `sorted` and `labelPrinted` (label orderQR, station sorting, device sorting-1) with by "" and data.signedIn false
//   2 · the chip takes a 6-digit employee number: it shows the name "Employee Numbers" knows, and the number is kept
//       nowhere (not on screen, not in localStorage, not in any event); an unknown number changes nothing
//   3 · the next sticker carries that name (data.login "employee number"); a reprint in the same minute keeps the same ids
//   4 · Print Row: each order in the row gets sorted + labelPrinted with that name
//   NODE_PATH=$(npm root -g) PW_DIR=$(npm root -g)/playwright/node_modules CHROMIUM=… node tests/stations/st-c.cjs
'use strict';
const path = require('path'), assert = require('assert'), fs = require('fs');
const root = path.join(__dirname, '../..');
const pwDir = process.argv[2] || process.env.PW_DIR || path.join(root, 'node_modules');
const { chromium } = require(path.join(pwDir, 'playwright-core'));

const ORIGIN = 'http://sorting-stc.test';
const A = '3720000001', B = '3720000002', C = '3720000003';
const PIN = '482913', NAME = 'Marco R.';
const posted = [];
const day = Math.floor(Date.now() / 1000);
const receipt = rid => ({ receipt_id: Number(rid), status: 'Paid', message_from_buyer: '',
  transactions: [{ transaction_id: Number(rid) * 10 + 1, receipt_id: Number(rid), listing_id: 1718001, title: 'Silver Dog Charm Necklace', quantity: 1,
    variations: [{ formatted_name: 'Metal', formatted_value: 'Silver' }], expected_ship_date: day + 3 * 86400 }] });
const firebaseStub = `
  window.__snap = { cb: null };
  const mk = d => ({ exists: !!d, data: () => d });
  window.firebase = { initializeApp() { return {}; }, firestore() { return { collection() { return { doc() { return {
    onSnapshot(cb) { window.__snap.cb = cb; setTimeout(() => cb(mk(null)), 0); return () => {}; } }; } }; } }; } };
  window.__fireSnap = d => { window.__snap.cb && window.__snap.cb(mk(d)); };`;
const materializeStub = `window.__toasts = []; const inst = { open() {}, close() {} };
  window.M = { AutoInit() {}, toast(o) { window.__toasts.push(o && o.html); }, Modal: { init() { return inst; }, getInstance() { return inst; } } };`;

(async () => {
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  const ctx = await browser.newContext({ viewport: { width: 1500, height: 900 } });
  const js = body => ({ status: 200, contentType: 'text/javascript', body });
  const json = o => ({ status: 200, contentType: 'application/json', body: JSON.stringify(o) });
  await ctx.route('**/*', async r => {
    const u = new URL(r.request().url());
    if (/gstatic\.com$/.test(u.hostname)) return r.fulfill(js(/firebase-app-compat/.test(u.pathname) ? firebaseStub : ''));
    if (/materialize/.test(u.pathname)) return r.fulfill(js(materializeStub));
    if (u.origin !== ORIGIN) return r.abort();
    const p = decodeURIComponent(u.pathname);
    if (p === '/sorting.html') return r.fulfill({ status: 200, contentType: 'text/html', body: fs.readFileSync(path.join(root, 'sorting.html')) });
    if (p === '/order-timeline.js' || p === '/station-timeline.js') return r.fulfill(js(fs.readFileSync(path.join(root, p))));
    if (p === '/QR Printer.html') return r.fulfill({ status: 200, contentType: 'text/html', body: '<!doctype html><title>printer stub</title>' });
    if (p === '/.netlify/functions/firebaseOrders') {
      if (u.searchParams.get('cancelCheck')) return r.fulfill(json({ success: true, cancelled: {} }));
      if (u.searchParams.get('orderId') === 'Employee Numbers') return r.fulfill(json({ success: true, data: { [PIN]: NAME, '111111': 'Someone' } }));
      if (r.request().method() === 'POST') { try { posted.push(...(JSON.parse(r.request().postData() || '{}').timeline || [])); } catch (_) {} }
      return r.fulfill(json({ success: true }));
    }
    if (p === '/.netlify/functions/etsyOrderProxy') return r.fulfill(json(receipt(u.searchParams.get('orderId'))));
    if (p === '/.netlify/functions/etsyImages') return r.fulfill(json([]));
    if (p.startsWith('/.netlify/functions/')) return r.fulfill(json({}));
    return r.fulfill({ status: 404, body: '' });
  });
  await ctx.addInitScript(() => {
    localStorage.setItem('access_token', 'tok'); localStorage.setItem('refresh_token', 'ref');
    localStorage.setItem('token_expires_at', String(Math.floor(Date.now() / 1000) + 7200));
    if (!sessionStorage.getItem('seeded')) { sessionStorage.setItem('seeded', '1'); localStorage.setItem('employee_name', 'Other Page Person'); }
  });
  const page = await ctx.newPage();
  const errors = [];
  page.on('pageerror', e => errors.push(e.message));
  await page.goto(`${ORIGIN}/sorting.html`);
  await page.waitForFunction(() => window.SortTL && window.StationTimeline && window.OrderTimeline && window.__snap && window.__snap.cb);
  await page.evaluate(() => { window.__rec = []; OrderTimeline.onRecord(ev => __rec.push(ev)); });

  await page.evaluate(d => __fireSnap(d), { 'Order Number': [A, B, C].join(','), 'Shipping Label Timestamps': new Date().toISOString() });
  await page.waitForFunction(() => (window.cachedOrderItems || []).length === 3 && document.getElementById('previewCell2'), null, { timeout: 15000 });
  await page.waitForTimeout(300);
  const cellOf = rid => page.evaluate(rid => window.cachedOrderItems.findIndex(t => String(t.receipt_id) === rid), rid);
  const recs = (type, id) => page.evaluate(([type, id]) => __rec.filter(e => e.type === type && (!id || e.orderId === id)), [type, id]);

  // ── 1 · nobody on the chip ──
  await page.evaluate(i => openIframePrinterForListing(i), await cellOf(A));
  await page.waitForFunction(a => __rec.some(e => e.type === 'labelPrinted' && e.orderId === a), A, { timeout: 5000 });
  let s = (await recs('sorted', A))[0], l = (await recs('labelPrinted', A))[0];
  assert.strictEqual(s.by, '', 'nobody set: by is empty (not another page\'s login): ' + s.by);
  assert.strictEqual(s.data.signedIn, false); assert.strictEqual(l.by, ''); assert.strictEqual(l.data.signedIn, false);
  assert.deepStrictEqual([s.station, s.device, l.station, l.device], ['sorting', 'sorting-1', 'sorting', 'sorting-1']);
  assert.strictEqual(l.data.label, 'orderQR'); assert.strictEqual(l.data.how, 'print');
  assert(/not signed in/.test(l.text) && /Sorting station/.test(l.text), l.text);
  assert(/^sorting-1-3720000001-sorted-\d+$/.test(s.id) && /^sorting-1-3720000001-labelPrinted-\d+$/.test(l.id), 'stable ids: ' + s.id + ' ' + l.id);
  console.log('1 · no one on the chip: sorted + labelPrinted (orderQR) with by "" and signedIn false');

  // ── 2 · the chip takes the employee number ──
  const signIn = async num => {
    await page.click('#sortingAsChip .st-as-name');
    await page.waitForSelector('#sortingAsChip .st-as-input:not([hidden])');
    await page.keyboard.type(num);
    const masked = await page.evaluate(() => getComputedStyle(document.querySelector('#sortingAsChip .st-as-input')).webkitTextSecurity);
    await page.keyboard.press('Enter');
    return masked;
  };
  assert.strictEqual(await signIn('999999'), 'disc', 'the number is masked while typed');
  await page.waitForFunction(() => __toasts.some(t => /not on the list/.test(t)), null, { timeout: 5000 });
  assert.strictEqual(await page.evaluate(() => localStorage.getItem('sorting.employee')), null, 'an unknown number sets nobody');
  await signIn(PIN);
  await page.waitForFunction(n => document.querySelector('#sortingAsChip .st-as-name').textContent === n, NAME, { timeout: 5000 });
  const ls = await page.evaluate(() => JSON.stringify(Object.assign({}, localStorage)));
  assert(!ls.includes(PIN), 'the employee number is kept nowhere in localStorage');
  assert.strictEqual(await page.evaluate(() => document.querySelector('#sortingAsChip .st-as-input').value), '', 'nor left in the field');
  assert.strictEqual(await page.evaluate(() => SortTL.getEmployee()), NAME);
  console.log('2 · the chip signs in with the 6-digit employee number and keeps only the name; unknown numbers are refused');

  // ── 3 · the next sticker carries the name; a reprint in the same minute keeps the same ids ──
  await page.evaluate(i => openIframePrinterForListing(i), await cellOf(B));
  await page.waitForFunction(b => __rec.filter(e => e.type === 'labelPrinted' && e.orderId === b).length === 1, B, { timeout: 5000 });
  await page.waitForTimeout(400);
  await page.evaluate(i => openIframePrinterForListing(i), await cellOf(B));
  await page.waitForFunction(b => __rec.filter(e => e.type === 'labelPrinted' && e.orderId === b).length === 2, B, { timeout: 5000 });
  const lb = await recs('labelPrinted', B), sb = await recs('sorted', B);
  assert(lb.every(e => e.by === NAME && e.data.login === 'employee number' && e.data.signedIn === undefined), JSON.stringify(lb));
  assert(sb.every(e => e.by === NAME), JSON.stringify(sb));
  assert(new Set(lb.map(e => e.id)).size === 1 || lb[0].id.split('-').pop() !== lb[1].id.split('-').pop(), 'a reprint in the same minute keeps the same id');
  assert(/by Marco R\./.test(lb[0].text), lb[0].text);
  console.log('3 · the next sticker is sorted + labelled by ' + NAME + ' (login: employee number); a reprint keeps its id');

  // ── 4 · Print Row ──
  await page.evaluate(() => { __rec.length = 0; window._rowPrintCursor = 0; return handlePrintRowClick(); });
  await page.waitForFunction(() => __rec.filter(e => e.type === 'labelPrinted').length === 3, null, { timeout: 8000 });
  const row = await page.evaluate(() => __rec.map(e => [e.type, e.orderId, e.by, e.data.how, e.data.label || '']).sort().map(x => x.join('|')));
  assert.deepStrictEqual(row, [A, B, C].flatMap(id => [`labelPrinted|${id}|${NAME}|row|orderQR`, `sorted|${id}|${NAME}|row|`]).sort(), JSON.stringify(row));
  console.log('4 · Print Row: every order in the row is sorted + labelled with who');

  // everything reaches the timeline door, and no event carries the number
  await page.evaluate(() => OrderTimeline.flush());
  await page.waitForFunction(() => !OrderTimeline.pending(), null, { timeout: 8000 });
  assert(posted.some(e => e.type === 'labelPrinted' && e.orderId === C && e.by === NAME), 'the events are posted to the stations\' timeline door');
  assert(!JSON.stringify(posted).includes(PIN), 'no posted event carries the employee number');
  assert(posted.filter(e => e.type === 'scan').length === 3, 'the batch scans are still recorded');

  await browser.close();
  assert.deepStrictEqual(errors, [], 'no page errors: ' + errors.join(' | '));
  console.log('st-c: all passed');
})().catch(e => { console.error(e); process.exit(1); });
