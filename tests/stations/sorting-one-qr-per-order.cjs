// ONE QR sticker per ORDER at the Sorting station (Paul, 18 Nov 2026: "it forces the user to print an individual QR code per each individual piece in a multi-piece order that has individual coloured charm.
// All charms in an order no matter their colour or material all need to go into one QR code ... one QR code per order only").
// A multi-piece order (three pieces: Gold, Silver, Rose Gold) sits in the grid next to single-piece orders, its cells straddling the end of a row of 9.
//   1 · Print Row on row 1 then row 2: the multi-piece order's sticker comes out ONCE (its QR text is its order number once across both rows), every other order once
//   2 · the single "Print" of the Match & Print flow on that order prints ONE page for all three pieces, and a second press of Print Row never prints it again
//   3 · the label's notes carry all three metals (they are in the one sticker), and the QR printer itself never prints the same order twice in one job
// Network, Firebase, Materialize and StationTimeline are stubbed; every request off the fake origin is aborted.
//   PW_DIR=<dir with playwright-core> CHROMIUM=... node tests/stations/sorting-one-qr-per-order.cjs
'use strict';
const path = require('path'), assert = require('assert'), fs = require('fs');
const root = path.join(__dirname, '../..');
const pwDir = process.argv[2] || process.env.PW_DIR || path.join(root, 'node_modules');
const { chromium } = require(path.join(pwDir, 'playwright-core'));

const ORIGIN = 'http://sorting.test';
const SINGLES = Array.from({ length: 10 }, (_, i) => String(3800000001 + i)), MULTI = '3800000099';
// orders in the grid, in order: 7 singles, the 3-piece order (cells 7, 8, 9: the third falls in row 2), then 3 more singles
const IDS = [...SINGLES.slice(0, 7), MULTI, ...SINGLES.slice(7)];
const day = Math.floor(Date.now() / 1000);
const BLURB = 'Handmade in our studio, 14k gold filled and sterling silver. '.repeat(40);   // an Etsy transaction carries its listing description
const tx = (rid, n, title, metal) => ({ transaction_id: Number(rid) * 10 + n, receipt_id: Number(rid), listing_id: 1718000 + n, title, description: BLURB, quantity: 1,
  tags: ['charm', 'necklace', 'gift'], variations: [{ formatted_name: 'Metal', formatted_value: metal }], expected_ship_date: day + 3 * 86400 });
const receipt = rid => ({
  receipt_id: Number(rid), status: 'Paid', message_from_buyer: '',
  transactions: rid === MULTI ? [tx(rid, 1, 'Gold Heart Charm Necklace', 'Gold'), tx(rid, 2, 'Silver Dog Charm Necklace', 'Silver'), tx(rid, 3, 'Rose Gold Star Charm Necklace', 'Rose Gold')] : [tx(rid, 1, 'Silver Dog Charm Necklace', 'Silver')]
});
const CELLS = IDS.flatMap(id => id === MULTI ? [id, id, id] : [id]);   // the grid: one cell per piece

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
    await page.waitForFunction(n => (window.cachedOrderItems || []).length === n && document.getElementById('previewCell' + (n - 1)), CELLS.length, { timeout: 15000 });
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

  const count = (arr, v) => arr.filter(x => x === v).length;
  const fails = []; const check = (ok, msg) => { if (!ok) fails.push(msg); console.log((ok ? '  ok   ' : '  FAIL ') + msg); };

  /* ── 1 · Print Row, row 1 then row 2: every order's sticker once ── */
  let { ctx, page } = await open();
  await page.evaluate(() => { window.__qrDocs = []; window.__qrTexts = []; });
  for (let row = 0; row < 2; row++) {
    const frames = await printerFrames(page), before = await printed(page);
    await page.evaluate(() => handlePrintRowClick());
    if (await printerFrames(page) > frames) await page.waitForFunction(b => window.__printed > b, before, { timeout: 20000 }).catch(() => {});
  }
  const texts = await page.evaluate(() => window.__qrTexts.slice());
  console.log('   QR texts printed by row 1 then row 2:', JSON.stringify(texts));
  check(count(texts, MULTI) === 1, 'the 3-piece order (Gold, Silver, Rose Gold) has ONE sticker across both rows: ' + count(texts, MULTI));
  check(IDS.every(id => count(texts, id) === 1), 'and every order has exactly one: ' + JSON.stringify(IDS.map(id => count(texts, id))));
  // a third press goes round to row 1 again: nothing printed twice that was printed
  const f3 = await printerFrames(page), b3 = await printed(page);
  await page.evaluate(() => handlePrintRowClick());
  await page.waitForTimeout(1500);
  const texts3 = await page.evaluate(() => window.__qrTexts.slice());
  check(IDS.every(id => count(texts3, id) === 1), 'pressing Print Row round again prints no order a second time: ' + JSON.stringify(IDS.map(id => count(texts3, id))));
  await ctx.close();

  /* ── 2 · the Print button of Match & Print on the 3-piece order: ONE page, all three metals ── */
  ({ ctx, page } = await open());
  const one = await page.evaluate(async MULTI => {
    window.__qrDocs = []; window.__qrTexts = [];
    const cell = window.cachedOrderItems.findIndex(i => String(i.receipt_id) === MULTI);
    window.__batch1 = null; const set = Storage.prototype.setItem;
    Storage.prototype.setItem = function (k, v) { if (k === 'qrPrintAll') window.__batch1 = v; return set.call(this, k, v); };
    openIframePrinterForListing(cell);
    for (let i = 0; i < 80 && !window.__printed; i++) await new Promise(r => setTimeout(r, 150));
    const b = JSON.parse(window.__batch1 || 'null');
    return { texts: window.__qrTexts.slice(), docs: window.__qrDocs.length, pages: window.__qrDocs.length ? window.__qrDocs[0].content.filter(c => c.pageBreak).length + 1 : 0, items: b && b.items.length, hasPieces: !!(b && b.pieces), metal: b && b.notesBlock && b.notesBlock.metal };
  }, MULTI);
  console.log('   single Print:', JSON.stringify(one));
  check(one.texts.length === 1 && one.texts[0] === MULTI && one.pages === 1, 'Print on the 3-piece order prints ONE page with its one QR: ' + JSON.stringify({ texts: one.texts, pages: one.pages }));
  check(one.items === 3 && !one.hasPieces, 'all three pieces ride in that one sticker, no per-piece page list: ' + JSON.stringify({ items: one.items, pieces: one.hasPieces }));
  check(/Gold/.test(one.metal) && /Silver/.test(one.metal) && /Rose/.test(one.metal), 'its notes name all three metals: ' + one.metal);
  await ctx.close();

  /* ── 3 · the QR printer itself never prints one order twice in a job ── */
  ({ ctx, page } = await open());
  const dup = await page.evaluate(async MULTI => {
    window.__qrDocs = []; window.__qrTexts = [];
    const mk = id => ({ dispatchDate: '1 Jan 2027', userTypedOrderNum: id, items: [{ receipt_id: id, title: 'Gold Heart Charm Necklace', keywords: ['necklace'] }], notesBlock: { metal: 'Gold', title4: 'Gold Heart', matrixNums: '' } });
    const batch = [mk(MULTI), mk('3800000001'), mk(MULTI)];
    localStorage.setItem('qrPrintBatch', JSON.stringify(batch));
    const ifr = document.createElement('iframe'); ifr.style.cssText = 'position:absolute;width:1px;height:1px;opacity:0;border:0'; ifr.src = 'QR Printer.html#batch'; document.body.appendChild(ifr);
    for (let i = 0; i < 80 && !window.__printed; i++) await new Promise(r => setTimeout(r, 150));
    return { texts: window.__qrTexts.slice() };
  }, MULTI);
  check(count(dup.texts, MULTI) === 1 && dup.texts.length === 2, 'a batch that names one order twice prints it once: ' + JSON.stringify(dup.texts));
  await ctx.close();

  await browser.close();
  assert.deepStrictEqual(errors.filter(e => !/QuotaExceeded|quota|Print Row failed|Failed to load resource/i.test(e)), [], 'no unexpected page errors: ' + errors.join(' | '));
  if (fails.length) { console.log('sorting-one-qr-per-order: ' + fails.length + ' failed'); process.exit(1); }
  console.log('sorting-one-qr-per-order: all passed');
})().catch(e => { console.error(e); process.exit(1); });
