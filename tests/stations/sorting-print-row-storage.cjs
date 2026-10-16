// Print Row on a browser whose storage is full (Paul, 16 Oct 2026: "the row will not print all together, each label prints on its own").
// A single sticker is written to localStorage "qrPrintAll", a key that is only ever REPLACED (same size, no new room needed), so it keeps
// working when localStorage is full. Print Row wrote a new, larger key, "qrPrintBatch", holding the whole order objects of the row, which
// is refused (QuotaExceededError) in that state: "Print Row error (see console)", nothing printed. This page keeps the Matrix photos in
// localStorage (userImagesArray), so a full store is its normal state after a long analysis.
//   1 · localStorage full: a single sticker still prints; Print Row prints the whole row (the real QR Printer.html, one page per order)
//       and leaves nothing behind in either store
//   2 · localStorage AND sessionStorage full: Print Row says so (storage is full, with the browser's own words), prints nothing
//   3 · the batch handed over carries only what the printer reads (not the whole Etsy transactions)
// Network, Firebase, Materialize and StationTimeline are stubbed; every request off the fake origin is aborted.
//   PW_DIR=<dir with playwright-core> CHROMIUM=... node tests/stations/sorting-print-row-storage.cjs
'use strict';
const path = require('path'), assert = require('assert'), fs = require('fs');
const root = path.join(__dirname, '../..');
const pwDir = process.argv[2] || process.env.PW_DIR || path.join(root, 'node_modules');
const { chromium } = require(path.join(pwDir, 'playwright-core'));

const ORIGIN = 'http://sorting.test';
const IDS = Array.from({ length: 9 }, (_, i) => String(3800000001 + i));   // one full row of the grid
const day = Math.floor(Date.now() / 1000);
const BLURB = 'Handmade in our studio, 14k gold filled and sterling silver. '.repeat(40);   // an Etsy transaction carries its listing description
const receipt = rid => ({
  receipt_id: Number(rid), status: 'Paid', message_from_buyer: '',
  transactions: [{ transaction_id: Number(rid) * 10 + 1, receipt_id: Number(rid), listing_id: 1718001, title: 'Silver Dog Charm Necklace', description: BLURB, quantity: 1,
    tags: ['dog', 'charm', 'necklace', 'gift'], variations: [{ formatted_name: 'Metal', formatted_value: 'Silver' }], expected_ship_date: day + 3 * 86400 }]
});

const firebaseStub = `
  window.__snap = { cb: null };
  const mk = d => ({ exists: !!d, data: () => d });
  window.firebase = { initializeApp() { return {}; }, firestore() { return { collection() { return { doc() { return {
    onSnapshot(cb) { window.__snap.cb = cb; setTimeout(() => cb(mk(window.__relayDoc)), 0); return () => {}; } }; } }; } }; } };`;
const materializeStub = `
  window.__toasts = [];
  const inst = { open() {}, close() {} };
  window.M = { AutoInit() {}, toast(o) { window.__toasts.push(o && o.html); }, Modal: { init() { return inst; }, getInstance() { return inst; } } };`;
const stationStub = `
  window.StationTimeline = { init() {}, scanned() { return Promise.resolve({ cancelled: false }); }, did() {}, guard() { return Promise.resolve(true); }, isCancelled() { return false; } };`;
// the label's PDF library records the document it is given and its "PDF" is a page whose print() only counts; the QR library is the real one
const PDFMAKE = `window.pdfMake = { createPdf(dd) { const top = window.top; (top.__qrDocs = top.__qrDocs || []).push(JSON.parse(JSON.stringify(dd)));
  return { getBlob(cb) { cb(new Blob(['<!doctype html><title>label</title><script>window.print = () => { window.top.__printed = (window.top.__printed || 0) + 1; };<\\/script>'], { type: 'text/html' })); } }; } };`;
const QRLIB = () => fs.readFileSync(path.join(root, 'lib/qrcode.min.js'), 'utf8') + `
;(function () { const Orig = window.QRCode; window.QRCode = function (el, o) { (window.top.__qrTexts = window.top.__qrTexts || []).push(String(o && o.text)); return new Orig(el, o); }; window.QRCode.CorrectLevel = Orig.CorrectLevel; })();`;

(async () => {
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  const errors = [];
  async function open() {
    const ctx = await browser.newContext({ viewport: { width: 1500, height: 900 } });
    const js = body => ({ status: 200, contentType: 'text/javascript', body });
    await ctx.route('**/*', r => {
      const url = r.request().url(), u = new URL(url);
      if (/gstatic\.com$/.test(u.hostname)) return r.fulfill(js(/firebase-app-compat/.test(u.pathname) ? firebaseStub : ''));
      if (/materialize/.test(u.pathname)) return r.fulfill(js(materializeStub));
      if (/jquery/.test(u.pathname)) return r.fulfill(js(fs.existsSync('/usr/share/javascript/jquery/jquery.min.js') ? fs.readFileSync('/usr/share/javascript/jquery/jquery.min.js') : ''));
      if (/cdn\.jsdelivr\.net\/npm\/pdfmake@[^/]+\/build\/pdfmake/.test(url)) return r.fulfill(js(PDFMAKE));
      if (/cdn\.jsdelivr\.net\/npm\/pdfmake@[^/]+\/build\/vfs_fonts/.test(url)) return r.fulfill(js(''));
      if (/qrcodejs/.test(url)) return r.fulfill(js(QRLIB()));
      if (u.origin !== ORIGIN) return r.abort();
      const p = decodeURIComponent(u.pathname), file = f => r.fulfill({ status: 200, contentType: f.endsWith('.html') ? 'text/html' : 'text/javascript', body: fs.readFileSync(path.join(root, f)) });
      if (p === '/sorting.html') return file('sorting.html');
      if (p === '/QR Printer.html') return file('QR Printer.html');
      if (p === '/qr-piece-label.js') return file('qr-piece-label.js');
      if (p === '/order-timeline.js') return file('order-timeline.js');
      if (p === '/station-timeline.js') return r.fulfill(js(stationStub));
      if (p === '/.netlify/functions/etsyOrderProxy') return r.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify(receipt(u.searchParams.get('orderId'))) });
      if (p === '/.netlify/functions/etsyImages') return r.fulfill({ status: 200, contentType: 'application/json', body: '[]' });
      if (p.startsWith('/.netlify/functions/')) return r.fulfill({ status: 200, contentType: 'application/json', body: '{}' });
      return r.fulfill({ status: 404, body: '' });
    });
    await ctx.addInitScript(doc => {
      localStorage.setItem('access_token', 'tok'); localStorage.setItem('refresh_token', 'ref');
      localStorage.setItem('token_expires_at', String(Math.floor(Date.now() / 1000) + 7200));
      localStorage.setItem('sorting.employee', 'Maya');
      window.__relayDoc = doc;
    }, { 'Order Number': IDS.join(','), 'Shipping Label Timestamps': new Date(Date.now() - 5000).toISOString() });
    const page = await ctx.newPage();
    page.on('pageerror', e => errors.push(e.message));
    page.on('console', m => { if (m.type() === 'error') errors.push('console: ' + m.text()); });
    await page.goto(`${ORIGIN}/sorting.html`);
    await page.waitForFunction(() => window.SortTL && window.__snap && window.__snap.cb);
    await page.waitForFunction(n => (window.cachedOrderItems || []).length === n && document.getElementById('previewCell' + (n - 1)), IDS.length, { timeout: 15000 });
    return { ctx, page };
  }
  // fill a store to the brim: big pieces first, then smaller ones, until not even 20 characters more fit
  const fill = (page, store) => page.evaluate(name => {
    const s = window[name]; let n = 0;
    for (const unit of [200000, 20000, 2000, 200, 20]) for (;;) { try { s.setItem('zz_fill_' + n, 'x'.repeat(unit)); n++; } catch (_) { break; } }
    let room = true; try { s.setItem('zz_probe', 'x'.repeat(1000)); s.removeItem('zz_probe'); } catch (_) { room = false; }
    return { pieces: n, roomFor1000: room };
  }, store);
  const printerFrames = page => page.evaluate(() => [...document.querySelectorAll('iframe')].filter(f => /QR%20Printer|QR Printer/.test(f.src)).length);
  const printed = page => page.evaluate(() => window.__printed || 0);

  /* ── 1 · localStorage full ── */
  let { ctx, page } = await open();
  await page.evaluate(() => openIframePrinterForListing(0));
  await page.waitForFunction(() => window.__printed >= 1, null, { timeout: 15000 });
  const f1 = await fill(page, 'localStorage');
  assert.strictEqual(f1.roomFor1000, false, 'the store really is full: ' + JSON.stringify(f1));
  await page.evaluate(() => { window.__toasts.length = 0; window.__qrDocs.length = 0; window.__qrTexts.length = 0; });

  const before = await printed(page), framesBefore = await printerFrames(page);
  await page.evaluate(() => openIframePrinterForListing(0));
  await page.waitForFunction(b => window.__printed > b, before, { timeout: 15000 });
  assert.deepStrictEqual(await page.evaluate(() => window.__toasts), [], 'a single sticker prints on a full store');
  console.log('full localStorage: a single sticker still prints (its key is replaced in place)');

  const b2 = await printed(page), frames2 = await printerFrames(page);
  await page.evaluate(() => { window.__qrDocs.length = 0; window.__qrTexts.length = 0; });
  await page.evaluate(() => handlePrintRowClick());
  let toasts = await page.evaluate(() => window.__toasts);
  assert(!toasts.some(t => /Print Row error/.test(t)), 'Print Row must not fail on a full store: ' + JSON.stringify(toasts));
  assert.strictEqual(await printerFrames(page), frames2 + 1, 'the printer page is opened for the row');
  await page.waitForFunction(b => window.__printed > b, b2, { timeout: 20000 });
  const row = await page.evaluate(() => ({ docs: window.__qrDocs.length, breaks: window.__qrDocs[window.__qrDocs.length - 1].content.filter(c => c.pageBreak).length,
    texts: window.__qrTexts.slice(), left: [localStorage.getItem('qrPrintBatch'), sessionStorage.getItem('qrPrintBatch')] }));
  assert.strictEqual(row.docs, 1, 'one print job for the row');
  assert.strictEqual(row.breaks, IDS.length - 1, 'one page for each of the 9 orders');
  assert.deepStrictEqual(row.texts, IDS, 'the QR on each page is its order number, in row order');
  assert.deepStrictEqual(row.left, [null, null], 'the batch is not left in either store after printing');
  console.log('full localStorage: Print Row prints all ' + IDS.length + ' orders in one job (' + f1.pieces + ' filler pieces), nothing left behind');
  await ctx.close();

  /* ── 2 · both stores full: said plainly, nothing printed ── */
  ({ ctx, page } = await open());
  await page.evaluate(() => openIframePrinterForListing(0));
  await page.waitForFunction(() => window.__printed >= 1, null, { timeout: 15000 });
  assert.strictEqual((await fill(page, 'localStorage')).roomFor1000, false);
  assert.strictEqual((await fill(page, 'sessionStorage')).roomFor1000, false);
  await page.evaluate(() => { window.__toasts.length = 0; });
  const frames3 = await printerFrames(page), b3 = await printed(page);
  await page.evaluate(() => handlePrintRowClick());
  toasts = await page.evaluate(() => window.__toasts);
  assert(toasts.length === 1 && /Print Row error/.test(toasts[0]) && /storage is full/i.test(toasts[0]), 'the error says the browser storage is full: ' + JSON.stringify(toasts));
  await page.waitForTimeout(500);
  assert.strictEqual(await printerFrames(page), frames3, 'no printer page opened');
  assert.strictEqual(await printed(page), b3, 'nothing printed');
  console.log('both stores full: "' + toasts[0].replace(/<[^>]+>/g, '') + '", nothing printed');
  await ctx.close();

  /* ── 3 · the batch carries what the printer reads, not the whole transactions ── */
  ({ ctx, page } = await open());
  await page.evaluate(() => {
    window.__batch = null; const set = Storage.prototype.setItem;
    Storage.prototype.setItem = function (k, v) { if (k === 'qrPrintBatch') window.__batch = v; return set.call(this, k, v); };
    return handlePrintRowClick();
  });
  const batch = JSON.parse(await page.evaluate(() => window.__batch));
  assert.strictEqual(batch.length, IDS.length);
  const bytes = (await page.evaluate(() => window.__batch)).length;
  assert(bytes < 12000, '9 orders hand the printer under 12 KB, not the ~' + (IDS.length * BLURB.length >> 10) + ' KB of listing descriptions: ' + bytes);
  for (const [i, o] of batch.entries()) {
    assert.strictEqual(o.userTypedOrderNum, IDS[i]); assert.strictEqual(o.items.length, 1);
    assert.strictEqual(String(o.items[0].receipt_id), IDS[i]); assert.strictEqual(o.items[0].title, 'Silver Dog Charm Necklace');
    assert(Array.isArray(o.items[0].keywords) && o.items[0].keywords.length, 'the keywords the dot count is read from are kept');
    assert(!('description' in o.items[0]) && !('variations' in o.items[0]), 'no listing description or variations handed over');
    assert(o.notesBlock && /Silver/.test(o.notesBlock.metal) && o.notesBlock.title4, 'the label text is the notes block');
  }
  console.log('the batch is ' + bytes + ' bytes for 9 orders (items trimmed to receipt, title, keywords)');
  await ctx.close();

  await browser.close();
  assert.deepStrictEqual(errors.filter(e => !/QuotaExceeded|quota|Print Row failed|Failed to load resource/i.test(e)), [], 'no unexpected page errors: ' + errors.join(' | '));
  console.log('sorting-print-row-storage: all passed');
})().catch(e => { console.error(e); process.exit(1); });
