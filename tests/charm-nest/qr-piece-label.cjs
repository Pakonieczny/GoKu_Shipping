// QRLABEL (Paul, 14 Oct 2026): ONE QR sticker per order at the Sorting Station, whatever the order holds; the only exception is a sticker a
// person prints BY HAND at the Design Station (the Charm Sorter's Print QR label) for one piece of an order that has more pieces: the same
// 1 x 1 in sticker, its QR still the MASTER ORDER's number, with a note (what the piece is for) and ORDER <number> in big type, fitted to the
// note area so a longer message gets smaller type and never overflows.
//   1 · the note fitter (node): 1 word, 20 characters, 60 characters, a 30-character single word: every line inside the box, the type never
//       larger for a longer text, the order line never touching the note
//   2 · the Sorting Station (sorting.html in Chromium): a mismatched pair, a matching pair and a 3-line order hand QR Printer.html NO `pieces`
//       (StationLiveOrder.ears still lists ears for the pairs: they are simply not handed over), and the printer makes ONE page for each
//   3 · QR Printer.html itself (Chromium, stubbed PDF library): the QR text is the order number; a stale list of ears prints one page (single and
//       batch); a piece sticker has the fitted note and ORDER <number> in place of Metal / Title / Match, with the date and order number kept
//   4 · the Charm Sorter (Chromium, fake site): Print QR label on a card that is the whole order prints the ordinary sticker with no question; on
//       one piece of a two-line order it asks for the note (prefilled, 60 characters, live size), Cancel prints nothing, Print label prints the
//       piece sticker, and the order's timeline says it was a piece label
//   node tests/charm-nest/qr-piece-label.cjs        (PW_DIR=<dir with playwright-core>; the browser parts say SKIP without it)
'use strict';
const fs = require('fs'), path = require('path'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');
const Q = require(path.join(root, 'qr-piece-label.js'));
const read = f => fs.readFileSync(path.join(root, f), 'utf8');
let checks = 0; const ok = m => { checks++; console.log('  ok ' + m); };
const J = o => JSON.parse(JSON.stringify(o));

/* ═══ 1 · the note fitter ═══ */
{
  const boxW = 37, boxH = 30, inside = (r, o) => { const g = Object.assign({ boxW, boxH }, o); assert.ok(r.fits, 'fits'); assert.ok(r.h <= g.boxH + 1e-9, `height ${r.h} inside ${g.boxH}`); for (const l of r.lines) assert.ok(Q.width(l, r.pt) <= g.boxW + 1e-9, `line "${l}" at ${r.pt} pt is ${Q.width(l, r.pt).toFixed(1)} wide, box ${g.boxW}`); assert.ok(r.pt >= 4.5 && r.pt <= 10); };
  const cases = { 'one word': 'CHAIN', '20 characters': 'EXTRA GOLD CHAIN 18"', '60 characters': 'EXTRA 18 INCH GOLD FILLED CHAIN FOR THE NECKLACE IN THIS ORDER'.slice(0, 60), 'a 30-character single word': 'ABCDEFGHIJKLMNOPQRSTUVWXYZABCD' };
  assert.equal(cases['60 characters'].length, 60); assert.equal(cases['20 characters'].length, 20); assert.equal(cases['a 30-character single word'].length, 30);
  const out = {};
  for (const [k, t] of Object.entries(cases)) { out[k] = Q.fitNote(t, { boxW, boxH }); inside(out[k]); assert.equal(out[k].truncated, false, k + ' is shown whole'); }
  assert.equal(out['one word'].pt, 10, 'a short word keeps the LEFT-sized type'); assert.equal(out['one word'].lines.length, 1);
  assert.ok(out['60 characters'].pt < out['20 characters'].pt && out['20 characters'].pt < out['one word'].pt, 'longer text, smaller type: ' + Object.values(out).map(r => r.pt).join(' > '));
  assert.ok(out['a 30-character single word'].lines.length > 1 && out['a 30-character single word'].lines.join('') === cases['a 30-character single word'], 'a long single word is broken by characters, none lost');
  // monotone: every longer prefix of a message is set at the same size or smaller, and always inside the box
  const msg = 'EXTRA 18 INCH GOLD FILLED CHAIN FOR THE NECKLACE IN THIS ORDER AB'; let last = 99;
  for (let n = 1; n <= 60; n++) { const r = Q.fitNote(msg.slice(0, n), { boxW, boxH }); inside(r); assert.ok(r.pt <= last + 1e-9, `size never grows with length (${n}: ${r.pt} after ${last})`); last = r.pt; }
  last = 99; for (let n = 1; n <= 30; n++) { const r = Q.fitNote('W'.repeat(n), { boxW, boxH }); inside(r); assert.ok(r.pt <= last + 1e-9); last = r.pt; }
  // text that cannot fit even at the smallest type is shortened with an ellipsis, never allowed out of the box
  const big = Q.fitNote('M'.repeat(200), { boxW, boxH: 12 }); assert.ok(big.truncated && big.fits && big.h <= 12 && big.lines.join('').endsWith('…'));
  ok('1 fitNote: 1 word / 20 / 60 characters / a 30-character word all inside the box, type shrinks as the text grows (' + Object.entries(out).map(([k, r]) => r.pt + ' pt').join(', ') + '), overlong text is shortened, never overflows');

  // the whole sticker's note area (72 x 72 pt page): the note and ORDER <number> stay inside x 33..70, y 0.5..52 and apart
  for (const n of ['', 'Chain', 'Extra chain', cases['20 characters'], cases['60 characters'], cases['a 30-character single word'], 'i'.repeat(60)]) for (const rid of ['4194598655', '41945986551234', 'UnknownOrder']) {
    const L = Q.layout(n, rid), g = L.geo;
    for (const part of [L.note, L.order]) { assert.ok(part.x >= 33 && part.x + part.w <= 70 + 1e-9, 'inside the page width: ' + (part.x + part.w)); assert.ok(part.y >= 0.5 && part.y + part.h <= g.bottom + 1e-9, 'inside the page height: ' + (part.y + part.h)); }
    assert.ok(L.note.y + L.note.h <= L.order.y + 1e-9, 'the note ends above the order line'); assert.ok(L.order.lines.join(' ').includes(rid.slice(0, 4)) || /…/.test(L.order.lines.join('')), 'the order number is said');
  }
  assert.deepEqual(J(Q.layout('Extra chain', '4194598655').order.lines), ['ORDER', '4194598655'], 'ORDER and its number');
  assert.deepEqual(J(Q.layout('Extra chain', '4194598655').note.lines), ['EXTRA', 'CHAIN'], 'the note is set in capitals');
  ok('1b layout: the note and ORDER <number> stay inside the note area of the 72 x 72 pt page, apart, for every length');

  // the card is one piece of a bigger order only when the order has a line that is not on it
  const a = { key: 'a' }, b = { key: 'b' };
  assert.equal(Q.isPiece([a], [a, b]), true); assert.equal(Q.isPiece([a, b], [a, b]), false); assert.equal(Q.isPiece([a], [a]), false); assert.equal(Q.isPiece([], [a]), false); assert.equal(Q.isPiece([a], []), false);
  assert.equal(Q.nameOf([{ spec: { special: { label: 'Chain only' } }, line: { title: 'x' } }]), 'Chain only'); assert.equal(Q.nameOf([{ spec: { designSku: 'BAR_1548' }, line: {} }]), 'BAR 1548');
  assert.equal(Q.nameOf([{ line: { title: 'Extra gold filled chain 18 inch for me' } }]), 'Extra gold filled chain'); assert.equal(Q.cleanNote('  a\n\tb   ' + 'x'.repeat(80)).length, 60);
  ok('1c a card is a piece when the order has another line; its name is offered first; a note is one line of at most 60 characters');
}

/* ═══ the browser parts ═══ */
const pwDir = process.argv[2] || process.env.PW_DIR || path.join(root, 'node_modules');
let chromium = null; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { /* skipped below */ }
const MIME = { '.html': 'text/html', '.js': 'text/javascript', '.json': 'application/json', '.css': 'text/css', '.png': 'image/png' };
const jsRes = body => ({ status: 200, contentType: 'text/javascript', headers: { 'Access-Control-Allow-Origin': '*', 'Cross-Origin-Resource-Policy': 'cross-origin' }, body });
// the label's PDF library: records the page it is given; the "PDF" is a page whose print() only counts. The QR library is the real one, wrapped to say what text it is given.
const PDFMAKE = `window.pdfMake = { createPdf(dd) { const top = window.top; (top.__qrDocs = top.__qrDocs || []).push(JSON.parse(JSON.stringify(dd)));
  return { getBlob(cb) { cb(new Blob(['<!doctype html><title>label</title><script>window.print = () => { window.top.__printed = (window.top.__printed || 0) + 1; };<\\/script>'], { type: 'text/html' })); } }; } };`;
const QRLIB = () => fs.readFileSync(path.join(root, 'lib/qrcode.min.js'), 'utf8') + `
;(function () { const Orig = window.QRCode; window.QRCode = function (el, o) { (window.top.__qrTexts = window.top.__qrTexts || []).push(String(o && o.text)); return new Orig(el, o); }; window.QRCode.CorrectLevel = Orig.CorrectLevel; })();`;
const externals = r => {
  const u = r.request().url();
  if (/cdn\.jsdelivr\.net\/npm\/pdfmake@[^/]+\/build\/pdfmake/.test(u)) return r.fulfill(jsRes(PDFMAKE));
  if (/cdn\.jsdelivr\.net\/npm\/pdfmake@[^/]+\/build\/vfs_fonts/.test(u)) return r.fulfill(jsRes(''));
  if (/qrcodejs/.test(u)) return r.fulfill(jsRes(QRLIB()));
  if (/gstatic\.com\/firebasejs/.test(u)) return r.fulfill(jsRes(/app-compat/.test(u) ? `window.firebase = { initializeApp() { return {}; }, firestore() { return { collection() { return { doc() { return { onSnapshot(cb) { setTimeout(() => cb({ exists: false, data: () => null }), 0); return () => {}; } }; } }; } }; } };` : ''));
  if (/materialize/.test(u)) return r.fulfill(jsRes('window.M = { AutoInit() {}, toast() {}, Modal: { init() { return { open() {}, close() {} }; }, getInstance() { return { open() {}, close() {} }; } } };'));
  if (/fonts\.googleapis|fonts\.gstatic/.test(u)) return r.fulfill({ status: 200, contentType: 'text/css', body: '' });
  return r.abort();
};
// a static site of the repo for the pages that need no fake backend (every function answers {}); nothing leaves the machine
function staticSite(ORIGIN) {
  return async r => {
    const u = new URL(r.request().url());
    if (u.origin !== ORIGIN) return externals(r);
    const p = decodeURIComponent(u.pathname);
    if (p.startsWith('/.netlify/functions/')) return r.fulfill({ status: 200, contentType: 'application/json', body: '{}' });
    const f = path.join(root, p);
    if (!f.startsWith(root) || !fs.existsSync(f) || fs.statSync(f).isDirectory()) return r.fulfill({ status: 404, body: '' });
    return r.fulfill({ status: 200, contentType: MIME[path.extname(f)] || 'application/octet-stream', body: fs.readFileSync(f) });
  };
}

async function main() {
  if (!chromium) { console.log('  SKIP the browser parts (2, 3, 4): no playwright-core in ' + pwDir + ' (set PW_DIR)'); return; }
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  try {
    /* ── 2 · the Sorting Station hands QR Printer.html no pieces ── */
    {
      const ORIGIN = 'http://sorting-qr.test', context = await browser.newContext({ viewport: { width: 1300, height: 900 } });
      await context.route('**/*', staticSite(ORIGIN));
      await context.addInitScript(() => { localStorage.setItem('access_token', 'tok'); localStorage.setItem('refresh_token', 'ref'); localStorage.setItem('token_expires_at', String(Math.floor(Date.now() / 1000) + 7200)); localStorage.setItem('sorting.employee', 'Maya'); });
      const page = await context.newPage(), errors = []; page.on('pageerror', e => errors.push(e.message));
      await page.goto(ORIGIN + '/sorting.html');
      await page.waitForFunction(() => typeof buildQrDataForCell === 'function' && window.StationLiveOrder && window.CharmNestOrders, null, { timeout: 30000 });
      const tx = (rid, tid, title, sku) => ({ receipt_id: rid, transaction_id: tid, listing_id: 1800000000 + tid, title, sku, quantity: 1, variations: [{ formatted_name: 'Metal', formatted_value: 'Gold Filled' }], keywords: ['earrings'], typedOrderNumber: String(rid), dispatch_date: '14 Oct 2026' });
      const orders = {
        'a mismatched earring pair': [tx(4194000001, 1, 'Mismatched stud earrings', 'MISMATCHED_7134')],
        'a matching earring pair': [tx(4194000002, 2, 'Stud earrings', 'CRAB_53423')],
        'a 3-line order (pair + necklace + chain)': [tx(4194000003, 3, 'Stud earrings', 'SURFER_4264'), tx(4194000003, 4, 'Dainty charm necklace', 'LOVE_1'), tx(4194000003, 5, 'Chain extender 2 inch', 'CHAIN_8941')]
      };
      const got = await page.evaluate(orders => {
        const all = Object.values(orders).flat(); window.cachedOrderItems = all;
        return Object.entries(orders).map(([name, lines]) => { const data = buildQrDataForCell(all.indexOf(lines[0])); return { name, data, ears: StationLiveOrder.ears(lines).length }; });
      }, orders);
      // QR Printer.html's own functions make the page from exactly that data
      const vm = require('vm'), html = read('QR Printer.html'), script = html.slice(html.indexOf('<script>\n') + 9, html.lastIndexOf('</script>'));
      const pr = vm.createContext({ window: { addEventListener() {}, parent: null }, document: { createElement: () => ({ style: {} }), body: { appendChild() {} } }, location: { hash: '' }, localStorage: { getItem: () => null }, console: { log() {}, warn() {}, error() {} }, setTimeout, pdfMake: {}, QRCode: {} });
      pr.window.parent = pr.window; vm.runInContext(script, pr); pr.data = null;
      for (const g of got) {
        assert.ok(g.ears > 0, g.name + ': StationLiveOrder.ears still lists ears (' + g.ears + '): the pair is real, it is just not handed over');
        assert.ok(!('pieces' in g.data) && !('pieceLabel' in g.data), g.name + ': no pieces handed to the printer: ' + Object.keys(g.data).join(','));
        assert.equal(g.data.items.length, orders[g.name].length); assert.equal(g.data.userTypedOrderNum, String(orders[g.name][0].receipt_id));
        pr.data = g.data;
        const doc = J(vm.runInContext('buildPDFDocWithDots("data:x", getCumulativeDotCount(data.items), data.dispatchDate, data.userTypedOrderNum, buildAggregatedNotes(data, data.items), piecesOf(data), pieceLabelOf(data))', pr));
        assert.equal(doc.content.filter(c => c.pageBreak).length, 0, g.name + ': ONE page'); assert.equal(doc.content.length, 5, 'the sticker it always was');
        const text = JSON.stringify(doc); assert.ok(/Metal: /.test(text) && !/LEFT|RIGHT/.test(text), g.name + ': Metal / Title / Match, no LEFT or RIGHT');
      }
      assert.ok(!/\.pieces = ears/.test(read('sorting.html')) && !/\.pieces = ears/.test(read('sorting-2.html')) && !/stickerPieces|withPieces/.test(read('charm-nest-bridge.js')), 'no page builds a list of ears for the printer');
      assert.deepEqual(errors, []);
      ok('2 Sorting Station: ' + got.map(g => g.name + ' → ' + g.ears + ' ears listed by the helper, 0 handed over, 1 page').join('; '));
      await context.close();
    }

    /* ── 3 · QR Printer.html: the QR is the order number; one page per order; the piece sticker ── */
    {
      const ORIGIN = 'http://printer-qr.test', run = async (key, value) => {
        const context = await browser.newContext(); await context.route('**/*', staticSite(ORIGIN));
        const page = await context.newPage(), errors = []; page.on('pageerror', e => errors.push(e.message));
        await page.addInitScript(([k, v]) => localStorage.setItem(k, v), [key, JSON.stringify(value)]);
        await page.goto(ORIGIN + '/QR Printer.html');
        await page.waitForFunction(() => window.__printed >= 1, null, { timeout: 30000 });
        const out = await page.evaluate(() => ({ docs: window.__qrDocs, texts: window.__qrTexts, printed: window.__printed }));
        await context.close(); assert.deepEqual(errors, []); return out;
      };
      const item = (rid, title) => ({ receipt_id: rid, title, keywords: ['earrings'] });
      const order = (rid, extra) => Object.assign({ dispatchDate: '14 Oct 2026', items: [item(rid, 'Lobster Earrings')], userTypedOrderNum: String(rid), notesBlock: { metal: 'Gold Filled', title4: 'Lobster Earrings', matrixNums: '87' } }, extra || {});
      const at = (dd, x, y) => dd.content.find(c => c.absolutePosition && c.absolutePosition.x === x && c.absolutePosition.y === y);
      const ears = [{ side: 'L', n: 1, of: 1 }, { side: 'R', n: 1, of: 1 }];
      // an order sticker, even with a stale list of ears: one page, the QR is the order number
      const plain = await run('qrPrintAll', order(4194598655, { pieces: ears }));
      assert.equal(plain.docs.length, 1); assert.equal(plain.docs[0].content.filter(c => c.pageBreak).length, 0, 'ONE page'); assert.deepEqual(plain.texts, ['4194598655'], 'the QR is the order number');
      assert.match(JSON.stringify(at(plain.docs[0], 33, 0.5)), /Metal: Gold Filled[\s\S]*Match: 87/); assert.ok(!/LEFT|RIGHT/.test(JSON.stringify(plain.docs[0])));
      assert.equal(at(plain.docs[0], 2, 46).text, '4194598655'); assert.equal(at(plain.docs[0], 2, 39).text, '14 Oct 2026');
      // a Print Row batch of three orders carrying stale ears: one page each
      const batch = await run('qrPrintBatch', [order(4194000001, { pieces: ears }), order(4194000002, { pieces: ears }), order(4194000003)]);
      assert.equal(batch.docs[0].content.filter(c => c.pageBreak).length, 2, 'three orders, three pages'); assert.ok(!/LEFT|RIGHT/.test(JSON.stringify(batch.docs[0])));
      assert.deepEqual(batch.texts, ['4194000001', '4194000002', '4194000003']);
      // the piece sticker: the same page, the QR the MASTER ORDER's number, the note and ORDER <number> where Metal / Title / Match were
      const piece = await run('qrPrintAll', order(4194598655, { pieceLabel: { note: 'Extra chain 18 in' } }));
      const dd = piece.docs[0], text = JSON.stringify(dd), ex = Q.layout('Extra chain 18 in', '4194598655');
      assert.equal(dd.content.filter(c => c.pageBreak).length, 0, 'one page'); assert.deepEqual(piece.texts, ['4194598655'], 'the QR text is the master order number');
      assert.ok(!/Metal:|Title:|Match:/.test(text), 'no Metal / Title / Match on a piece sticker');
      assert.equal(at(dd, 2, 6).width, 28.9); assert.equal(at(dd, 2, 39).text, '14 Oct 2026', 'the date stays'); assert.equal(at(dd, 2, 46).text, '4194598655', 'the order number stays bottom left');
      const noteNode = at(dd, 33, 0.5), orderNode = dd.content.find(c => c.text === 'ORDER\n4194598655');
      assert.equal(noteNode.text, ex.note.lines.join('\n')); assert.equal(noteNode.text.replace(/\n/g, ' '), 'EXTRA CHAIN 18 IN'); assert.equal(noteNode.fontSize, ex.note.pt); assert.equal(noteNode.bold, true);
      assert.ok(orderNode && orderNode.absolutePosition.y === ex.order.y && orderNode.fontSize === ex.order.pt, 'ORDER <number> in big type, bottom of the note area');
      assert.ok(noteNode.fontSize >= 4.5 && noteNode.fontSize <= 10 && noteNode.absolutePosition.y + ex.note.h <= orderNode.absolutePosition.y, 'the note fits above ORDER <number>');
      // a long note is smaller than a short one, in the real printed document
      const longer = await run('qrPrintAll', order(4194598655, { pieceLabel: { note: 'EXTRA 18 INCH GOLD FILLED CHAIN FOR THE NECKLACE IN THIS ORDER'.slice(0, 60) } }));
      assert.ok(at(longer.docs[0], 33, 0.5).fontSize < noteNode.fontSize, 'a 60-character note is set smaller');
      // a piece sticker listing ears (the engine kept for it) puts the ear in the note, one page each, all with the master order's QR
      const earPiece = await run('qrPrintAll', order(4194598655, { pieceLabel: { note: 'Studs' }, pieces: ears }));
      assert.equal(earPiece.docs[0].content.filter(c => c.pageBreak).length, 1); assert.match(JSON.stringify(earPiece.docs[0]), /STUDS\\nLEFT/); assert.match(JSON.stringify(earPiece.docs[0]), /STUDS\\nRIGHT/);
      ok('3 QR Printer.html: QR text = order number; stale ears print one page (single and batch of 3); a piece sticker = fitted note (' + noteNode.fontSize + ' pt) + ORDER <number>, no Metal/Title/Match, date and order number kept');
    }

    /* ── 4 · the Charm Sorter: Print QR label by hand ── */
    {
      const { start } = require('./bridge-server.cjs');
      const DAY = 86400, SHIP = Math.floor(Date.UTC(2026, 9, 21, 17) / 1000);
      const order = (rid, lines) => ({ receiptId: String(rid), orderNumber: String(rid), createTs: SHIP - 5 * DAY, updateTs: SHIP - 5 * DAY + 60, shipBy: SHIP, buyer: { name: 'Buyer ' + String(rid).slice(-4) }, buyerMessage: '', isGift: false, giftMessage: '', staffNote: '', messages: [], lines });
      const line = (tid, sku, title, variations, extra) => Object.assign({ transactionId: String(tid), listingId: String(1800000000 + (tid % 100000)), sku, title, quantity: 1, expectedShipDate: SHIP, variations: variations.map(([name, value]) => ({ name, value })), metalKey: '', metalLabel: '', personalization: [], buyerMessage: '' }, extra || {});
      const ORDERS = [
        order(4194000010, [line(41940000101, 'CHAIN_8941', 'CHAIN REPLACEMENT', [['Metal', 'Gold'], ['Length', '17 Inches']])]),                                   // the whole order is the chain
        order(4194000020, [line(41940000201, 'BLOOMING_20239', 'Blooming Flower Charm Necklace', [['Metal', '14k Gold Filled']], { metalKey: 'gold', metalLabel: 'GF 14/20' }),
                           line(41940000202, 'CHAIN_8941', 'Extra chain 18 inch', [['Metal', 'Gold'], ['Length', '18 Inches']])]),                              // a charm order with an extra chain
        order(4194000030, [line(41940000301, 'BLOOMING_20239', 'Blooming Flower Charm Necklace', [['Metal', '14k Gold Filled']], { metalKey: 'gold', metalLabel: 'GF 14/20' }),
                           line(41940000302, 'CHAIN_8941', 'Extra chain 16 inch', [['Metal', 'Gold'], ['Length', '16 Inches']])])
      ];
      const srv = await start({ receipts: [] });
      try {
        const context = await browser.newContext({ viewport: { width: 1440, height: 950 } });
        await context.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), externals);
        await context.addInitScript(() => { try { if (!sessionStorage.getItem('__seeded')) { localStorage.setItem('cn.settings', JSON.stringify({ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on' })); localStorage.setItem('cn.employee', 'Test Operator'); sessionStorage.setItem('__seeded', '1'); } } catch (_) {} window.prompt = () => 'Test Operator'; });
        const page = await context.newPage(), errors = []; page.setDefaultTimeout(30000);
        page.on('pageerror', e => { errors.push(e.message); console.error('page error:', String(e.stack || e.message).split('\n').slice(0, 4).join(' | ')); });
        await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
        await page.waitForFunction(() => window.CN && window.Orders && window.Review && window.CustomPrint && window.OrderWin && window.QrPieceLabel && CN.S.cloud.ok === true, null, { timeout: 60000 });
        await page.evaluate(async orders => {
          await Orders.loadMaps(true);
          for (const order of orders) for (const line of order.lines) { const key = CharmNestOrders.lineKey(order, line); const row = { key, order, line, arrivedAt: Date.now(), spec: null, problems: [], state: 'pulled', reason: null, claimedBy: null, poolIds: [], engrave: null, material: null }; B.orders.rows.push(row); B.orders.byKey.set(key, row); }
          Orders.interpretAll(); Review.syncOrderItems(); CN.setMode('review'); Review.render();
          window.__ev = []; const o = SheetEvents.order; SheetEvents.order = function (e) { window.__ev.push(JSON.parse(JSON.stringify(e))); return o.apply(this, arguments); };
        }, ORDERS);
        await page.click('#reviewView .egTab[data-k="customOrder"]');
        const btn = rid => `#rvList .reviewListRow[data-rid="${rid}"] [data-cu-print]`;
        const printed = () => page.evaluate(() => window.__printed || 0);
        const dlg = () => page.evaluate(() => { const d = document.querySelector('dialog.pieceDlg'); return d ? { open: d.open, value: d.querySelector('#pieceNote').value, max: d.querySelector('#pieceNote').maxLength, size: d.querySelector('.pieceSize').textContent, preview: [...d.querySelectorAll('.piecePrev div')].map(n => n.textContent), go: !d.querySelector('[data-k=go]').disabled, text: d.querySelector('.dlgBody p').textContent } : null; });

        // (a) the card IS the whole order: the ordinary sticker, no question
        await page.click(btn('4194000010'));
        await page.waitForFunction(() => B.maps.customDone['4194000010_41940000101'] && window.__printed === 1, null, { timeout: 30000 });
        assert.equal(await dlg(), null, 'no question for a card that is the whole order');
        let st = await page.evaluate(() => ({ docs: window.__qrDocs, handed: JSON.parse(localStorage.getItem('qrPrintAll')), texts: window.__qrTexts }));
        assert.ok(!('pieceLabel' in st.handed) && !('pieces' in st.handed), 'the label handed over is the order sticker'); assert.match(JSON.stringify(st.docs[0]), /Metal: Gold Filled/); assert.deepEqual(st.texts, ['4194000010']);
        await page.evaluate(() => document.querySelectorAll('.mNote').forEach(n => n.close()));

        // (b) ONE PIECE of a two-line order: it asks what the sticker is for; Cancel prints nothing
        await page.click(btn('4194000020'));
        await page.waitForSelector('dialog.pieceDlg[open]');
        let d = await dlg();
        assert.equal(d.value, 'Chain only', 'prefilled with the piece\'s own name'); assert.equal(d.max, 60); assert.match(d.text, /Order 4194000020 has more pieces/); assert.match(d.size, /^Prints at \d+(\.\d+)? pt$/); assert.deepEqual(d.preview, ['CHAIN', 'ONLY', 'ORDER', '4194000020']);
        const sizeShort = +/(\d+(\.\d+)?)/.exec(d.size)[1];
        await page.fill('#pieceNote', ''); await page.type('#pieceNote', 'Extra chain 18 inch gold filled for the necklace in this order, please');   // (typed: the field stops at 60)
        d = await dlg(); assert.equal(d.value.length, 60, 'at most 60 characters'); assert.ok(+/(\d+(\.\d+)?)/.exec(d.size)[1] < sizeShort, 'the live size shrinks as the note grows: ' + d.size);
        await page.fill('#pieceNote', ''); assert.equal((await dlg()).go, false, 'an empty note cannot be printed');
        await page.click('dialog.pieceDlg [data-k=cancel]');
        await page.waitForFunction(() => !document.querySelector('dialog.pieceDlg'));
        await page.waitForTimeout(400);
        assert.equal(await printed(), 1, 'Cancel printed nothing'); assert.equal(await page.evaluate(() => !!B.maps.customDone['4194000020_41940000202']), false, 'and completed nothing');

        // (c) print it: the piece sticker for the master order, recorded on the order's timeline
        await page.click(btn('4194000020'));
        await page.waitForSelector('dialog.pieceDlg[open]');
        assert.equal((await dlg()).value, 'Chain only');
        await page.fill('#pieceNote', 'Extra chain 18 in');
        await page.click('dialog.pieceDlg [data-k=go]');
        await page.waitForFunction(() => B.maps.customDone['4194000020_41940000202'] && window.__printed === 2, null, { timeout: 30000 });
        st = await page.evaluate(() => ({ docs: window.__qrDocs, handed: JSON.parse(localStorage.getItem('qrPrintAll')), texts: window.__qrTexts, ev: window.__ev.filter(e => e.type === 'labelPrinted') }));
        assert.deepEqual(st.handed.pieceLabel, { note: 'Extra chain 18 in' }); assert.equal(st.handed.userTypedOrderNum, '4194000020', 'the master order'); assert.ok(!('pieces' in st.handed));
        assert.equal(st.handed.items.length, 2, 'the label still belongs to the whole order (both lines)');
        assert.equal(st.texts[1], '4194000020', 'the QR text is the master order number');
        const dd = st.docs[1], note = dd.content.find(c => c.absolutePosition && c.absolutePosition.x === 33 && c.absolutePosition.y === 0.5);
        assert.equal(note.text.replace(/\n/g, ' '), 'EXTRA CHAIN 18 IN'); assert.ok(dd.content.some(c => c.text === 'ORDER\n4194000020')); assert.ok(!/Metal:|Match:/.test(JSON.stringify(dd)));
        const ev = st.ev[st.ev.length - 1]; assert.equal(ev.orderId, '4194000020'); assert.equal(ev.data.piece, 'Extra chain 18 in'); assert.match(ev.text, /^Piece QR label printed at the Design Station \(Review\): Extra chain 18 in \(scans to this order\)$/);
        const rec = srv.st.doc('Charm_Custom_Orders', '4194000020_41940000202'); assert.ok(rec && rec.state === 'completed' && rec.prints === 1, 'marked completed in the cloud as before: ' + JSON.stringify(rec && [rec.state, rec.prints]));
        assert.deepEqual(errors, []);
        ok('4 Charm Sorter: a whole-order card prints the ordinary sticker with no question; one piece of a two-line order asks (prefilled "Chain only", 60 characters, live size), Cancel prints nothing, Print label prints the piece sticker for the master order and the timeline records it');
        await context.close();
      } finally { srv.close(); }
    }
  } finally { await browser.close(); }
}
main().then(() => console.log('QRLABEL OK: ' + checks + ' groups of checks passed')).catch(e => { console.error(e); process.exit(1); });
