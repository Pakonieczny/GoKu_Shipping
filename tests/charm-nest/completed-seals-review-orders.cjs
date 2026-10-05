// Every Completed piece or order on the Review tab and the Orders tab shows its own seals (Paul, 5 Oct 2026: "The 'Complete'
// orders/pieces are missing their associated stamp/seal. Please review all modals and fix."). The sorter runs in headless Chromium
// against the local fake site (bridge-server.cjs): every request off the loopback is aborted, nothing is printed or written live.
// A fixture of orders whose records are seeded the way the server keeps them, and each surface read as drawn:
//   Review → Completed: a piece completed with Complete Order and printed (ORDER COMPLETE and QR LABEL PRINTED), one with only
//   Complete Order, older records without `stamps` (Complete Order, a print, a print that kept only its completion), a card of two
//   lines of one SKU done by different presses (every line's seals) and by one press (one seal), a card sent to the sheets
//   (its kept print and its send), a held order's and a cancelled order's completed card;
//   Orders → Open Orders (cards and list), On hold and Cancelled: each piece's own seals beside its state, an order's on its
//   cancelled card; none on a piece that is not completed.
// Seals are for good: a piece completed here is completed and reopened with the real buttons and its seal stays on every surface.
// Each seal once per card; the cards fit at 390 and 900 px; a build that drops the seals is caught by the same checks.
//   node tests/charm-nest/completed-seals-review-orders.cjs [playwright-core dir]   (CN_SHOTS=dir saves the screenshots)
const fs = require('fs'), path = require('path'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');
const { start } = require('./bridge-server.cjs');

const DAY = 86400, SHIP = Math.floor(Date.UTC(2026, 9, 9, 17) / 1000);
const order = (rid, lines) => ({ receiptId: String(rid), orderNumber: String(rid), createTs: SHIP - 5 * DAY, updateTs: SHIP - 5 * DAY + 60, shipBy: SHIP, buyer: { name: 'Buyer ' + String(rid).slice(-4) }, buyerMessage: '', isGift: false, giftMessage: '', staffNote: '', messages: [], lines });
const line = (tid, sku, title) => ({ transactionId: String(tid), listingId: String(1800000000 + (tid % 100000)), sku, title, quantity: 1, expectedShipDate: SHIP, variations: [{ name: 'Metal', value: 'Gold' }], metalKey: '', metalLabel: '', personalization: '' });
const T = (h, m, s = 0) => Date.UTC(2026, 9, 5, h, m, s);
const A = '4171711853', LB = '4170000002', LP = '4170000003', LC = '4170000004', TW = '4170000005', SE = '4170000006', HD = '4170000007', CX = '4170000008', RO = '4170000009', ON = '4170000010', U = '4175892473';
const ORDERS = [
  order(A, [line(41717118531, 'SPORTS 10 - FIGURE SKATE', 'Figure skate charm'), line(41717118532, 'SNAKE 5', 'Snake charm'), line(41717118533, 'FOOTBALL', 'Football charm')]),
  order(LB, [line(41700000021, 'LEGACY BUTTON', 'Legacy button charm')]),
  order(LP, [line(41700000031, 'LEGACY PRINT', 'Legacy print charm')]),
  order(LC, [line(41700000041, 'LEGACY COMPLETED ONLY', 'Legacy completed-only charm')]),
  order(TW, [line(41700000051, 'TWIN CHARM', 'Twin charm'), line(41700000052, 'TWIN CHARM', 'Twin charm')]),
  order(SE, [line(41700000061, 'SENT CHARM', 'Sent charm')]),
  order(HD, [line(41700000071, 'HELD CHARM', 'Held charm')]),
  order(CX, [line(41700000081, 'CANCELLED CHARM', 'Cancelled charm')]),
  order(RO, [line(41700000091, 'REOPENED CHARM', 'Reopened charm')]),
  order(ON, [line(41700000101, 'ONE PRESS', 'One press charm'), line(41700000102, 'ONE PRESS', 'One press charm')]),
  order(U, [line(41758924731, 'DAVID STAR', 'David Star Necklace')])
];
const lineOf = (rid, tid) => ORDERS.find(o => o.receiptId === rid).lines.find(l => l.transactionId === String(tid));
const REC = (rid, tid, extra) => ({ key: `${rid}_${tid}`, receiptId: rid, transactionId: String(tid), sku: lineOf(rid, tid).sku, title: lineOf(rid, tid).title, category: 'Unknown SKU', kind: '', state: 'completed', updatedAtMs: Date.now() - 5000, hasLabel: true, ...extra });
const BTN = (at, by) => ({ how: 'button', at, by }), PRN = (at, by) => ({ how: 'print', at, by });
const printed = (at, by, prints = 1) => ({ printedAt: at, printedBy: by, prints, lastPrintedAt: at, lastPrintedBy: by });
const SEEDS = [
  // the order of Paul's picture: a piece completed with Complete Order and then printed, one completed with Complete Order only
  REC(A, 41717118531, { how: 'button', completedAt: T(14, 57), completedBy: 'Paul', ...printed(T(16, 41), 'Paul'), stamps: [BTN(T(14, 57), 'Paul'), PRN(T(16, 41), 'Paul')] }),
  REC(A, 41717118533, { how: 'button', completedAt: T(16, 41), completedBy: 'Paul', stamps: [BTN(T(16, 41), 'Paul')] }),
  // older records, written before the stamps were kept
  REC(LB, 41700000021, { how: 'button', completedAt: T(13, 0), completedBy: 'Seth' }),
  REC(LP, 41700000031, { how: 'print', completedAt: T(13, 5), completedBy: 'Seth', ...printed(T(13, 5), 'Seth') }),
  REC(LC, 41700000041, { how: 'print', completedAt: T(13, 9), completedBy: 'Seth' }),
  // two lines of one SKU, completed by two different presses: the card has both seals
  REC(TW, 41700000051, { how: 'button', completedAt: T(12, 0), completedBy: 'Seth', stamps: [BTN(T(12, 0), 'Seth')] }),
  REC(TW, 41700000052, { how: 'print', completedAt: T(12, 30), completedBy: 'Ann', ...printed(T(12, 30), 'Ann'), stamps: [PRN(T(12, 30), 'Ann')] }),
  // a piece printed, reopened and then sent to the sheets with its own designs (the send is its decision, set in the page below)
  REC(SE, 41700000061, { state: 'open', how: 'print', completedAt: T(11, 0), completedBy: 'Seth', ...printed(T(11, 0), 'Seth'), reopenedAt: T(11, 30), reopenedBy: 'Seth', stamps: [PRN(T(11, 0), 'Seth')] }),
  // an order on hold, and an order that was cancelled (Etsy or a person), each with seals earned before
  REC(HD, 41700000071, { how: 'button', completedAt: T(10, 0), completedBy: 'Ann', stamps: [BTN(T(10, 0), 'Ann')] }),
  REC(CX, 41700000081, { how: 'button', completedAt: T(9, 0), completedBy: 'Seth', ...printed(T(9, 30), 'Seth'), stamps: [BTN(T(9, 0), 'Seth'), PRN(T(9, 30), 'Seth')] }),
  // a piece completed and reopened: its seal stays (the Open card keeps it on the button that made it)
  REC(RO, 41700000091, { state: 'open', how: 'button', completedAt: T(8, 0), completedBy: 'Ann', reopenedAt: T(8, 20), reopenedBy: 'Ann', stamps: [BTN(T(8, 0), 'Ann')] }),
  // two lines pressed together: their records differ by a few hundred milliseconds, and that is one seal
  REC(ON, 41700000101, { how: 'button', completedAt: T(7, 0, 0), completedBy: 'Seth', stamps: [BTN(T(7, 0, 0), 'Seth')] }),
  REC(ON, 41700000102, { how: 'button', completedAt: T(7, 0, 0) + 400, completedBy: 'Seth', stamps: [BTN(T(7, 0, 0) + 400, 'Seth')] })
];
// what each Completed card must show: [ORDER COMPLETE | QR LABEL PRINTED | SENT TO SHEET, by, print number]
const COMPLETED = {
  [`${A}|SPORTS 10 - FIGURE SKATE`]: [['ORDER COMPLETE', 'Paul', 0], ['QR LABEL PRINTED', 'Paul', 1]],
  [`${A}|FOOTBALL`]: [['ORDER COMPLETE', 'Paul', 0]],
  [`${LB}|LEGACY BUTTON`]: [['ORDER COMPLETE', 'Seth', 0]],
  [`${LP}|LEGACY PRINT`]: [['QR LABEL PRINTED', 'Seth', 1]],
  [`${LC}|LEGACY COMPLETED ONLY`]: [['QR LABEL PRINTED', 'Seth', 1]],
  [`${TW}|TWIN CHARM`]: [['ORDER COMPLETE', 'Seth', 0], ['QR LABEL PRINTED', 'Ann', 1]],
  [`${SE}|SENT CHARM`]: [['QR LABEL PRINTED', 'Seth', 1], ['SENT TO SHEET', 'Paul', 0]],
  [`${HD}|HELD CHARM`]: [['ORDER COMPLETE', 'Ann', 0]],
  [`${CX}|CANCELLED CHARM`]: [['ORDER COMPLETE', 'Seth', 0], ['QR LABEL PRINTED', 'Seth', 1]],
  [`${ON}|ONE PRESS`]: [['ORDER COMPLETE', 'Seth', 0]]
};
// what each piece's row in the Orders list must show (by line key)
const ROWS = {
  [`${A}_41717118531`]: [['ORDER COMPLETE', 'Paul', 0], ['QR LABEL PRINTED', 'Paul', 1]],
  [`${A}_41717118532`]: [],
  [`${A}_41717118533`]: [['ORDER COMPLETE', 'Paul', 0]],
  [`${LB}_41700000021`]: [['ORDER COMPLETE', 'Seth', 0]],
  [`${LP}_41700000031`]: [['QR LABEL PRINTED', 'Seth', 1]],
  [`${LC}_41700000041`]: [['QR LABEL PRINTED', 'Seth', 1]],
  [`${TW}_41700000051`]: [['ORDER COMPLETE', 'Seth', 0]],
  [`${TW}_41700000052`]: [['QR LABEL PRINTED', 'Ann', 1]],
  [`${SE}_41700000061`]: [['QR LABEL PRINTED', 'Seth', 1]],
  [`${HD}_41700000071`]: [['ORDER COMPLETE', 'Ann', 0]],
  [`${RO}_41700000091`]: [['ORDER COMPLETE', 'Ann', 0]],
  [`${ON}_41700000101`]: [['ORDER COMPLETE', 'Seth', 0]],
  [`${ON}_41700000102`]: [['ORDER COMPLETE', 'Seth', 0]]
};

(async () => {
  const pwDir = process.argv[2] || process.env.PW_DIR || path.join(root, 'node_modules');
  let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { console.log('  - no playwright-core: not run'); return; }
  const shots = process.env.CN_SHOTS || '';
  if (shots) fs.mkdirSync(shots, { recursive: true });
  const srv = await start({ receipts: [] });
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  try {
    for (const r of SEEDS) srv.st.put('Charm_Custom_Orders', r.key, r);
    const context = await browser.newContext({ viewport: { width: 1440, height: 950 } });
    const js = body => ({ status: 200, contentType: 'text/javascript', headers: { 'Cross-Origin-Resource-Policy': 'cross-origin', 'Access-Control-Allow-Origin': '*' }, body });
    const outside = [];
    await context.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => {
      const u = r.request().url();
      if (/cdn\.jsdelivr\.net\/npm\/pdfmake@[^/]+\/build\/pdfmake/.test(u)) return r.fulfill(js('window.pdfMake={createPdf(){return{getBlob(cb){cb(new Blob([""]));}};}};'));
      if (/vfs_fonts/.test(u)) return r.fulfill(js(''));
      if (/qrcodejs/.test(u)) return r.fulfill(js(fs.readFileSync(path.join(root, 'lib/qrcode.min.js'))));
      if (/fonts\.googleapis|fonts\.gstatic/.test(u)) return r.fulfill({ status: 200, contentType: 'text/css', body: '' });
      if (/etsy/i.test(u)) outside.push(u);
      return r.abort();
    });
    await context.addInitScript(() => { try { if (!sessionStorage.getItem('__seeded')) { localStorage.setItem('cn.settings', JSON.stringify({ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on' })); localStorage.setItem('cn.employee', 'Test Operator'); sessionStorage.setItem('__seeded', '1'); } } catch (_) {} window.prompt = () => 'Test Operator'; });
    const page = await context.newPage(), errors = [];
    page.setDefaultTimeout(30000);
    page.on('pageerror', e => { errors.push(e.message); console.error('page error:', String(e.stack || e.message).split('\n').slice(0, 4).join(' | ')); });
    await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
    await page.waitForFunction(() => window.CN && window.Orders && window.Review && window.CustomPrint && window.Seal && window.Motion && window.Cancelled && CN.S.cloud.ok === true, null, { timeout: 60000 });
    await page.evaluate(async ({ orders, sent, hold, cancelled }) => {
      for (const order of orders) for (const line of order.lines) { const key = CharmNestOrders.lineKey(order, line); const row = { key, order, line, arrivedAt: Date.now(), spec: null, problems: [], state: 'pulled', reason: null, claimedBy: null, poolIds: [], engrave: null, material: null }; B.orders.rows.push(row); B.orders.byKey.set(key, row); }
      await Orders.loadMaps(true);
      B.maps.customSent = { [sent.key]: sent.decision };   // (the send to the sheets of the reopened piece: its decision)
      for (const r of Orders.rows()) if (r.order.receiptId === hold) r.hold = 'Held by Paul';
      Orders.interpretAll();
      await Cancelled.put({ orderId: cancelled, by: 'Paul', why: 'asked by the buyer', buyer: 'Buyer ' + cancelled.slice(-4), lines: [{ sku: 'CANCELLED CHARM', title: 'Cancelled charm', quantity: 1 }] });
      for (const r of Orders.rows()) if (r.order.receiptId === cancelled) { r.state = 'gone'; r.reason = 'cancelled by Paul'; }
      Orders.interpretAll(); Review.syncOrderItems(); CN.setMode('review'); Review.render();
    }, { orders: ORDERS, sent: { key: `${SE}_41700000061`, decision: { at: T(15, 0), by: 'Paul', id: 'designSent-test' } }, hold: HD, cancelled: CX });
    const settle = () => page.waitForFunction(() => !document.querySelector('.cuStat, .btn.working, .cuSealHost, #motionLayer .mGhost, .sealTool, .seal.pending'), null, { timeout: 15000 });
    // the seals inside a node: [action, by, print number], and the times (one seal each)
    const sealsIn = sel => page.evaluate(sel => [...document.querySelectorAll(sel)].map(n => ({ rid: n.dataset.rid, key: n.dataset.key || '', sku: (n.querySelector('.sku') || {}).textContent || '', seals: [...n.querySelectorAll('.seal')].map(s => { const m = JSON.parse(s.querySelector('svg').dataset.sealModel); return [m.action, m.by, m.n || 0, +s.dataset.at]; }) })), sel);
    const faces = list => list.map(([a, b, n]) => [a, b, n]);
    const nodup = (what, seals) => assert.equal(new Set(seals.map(s => s[0] + '|' + s[3])).size, seals.length, what + ': a seal is drawn once: ' + JSON.stringify(seals));
    const bounds = what => page.evaluate(() => { const w = innerWidth; return { over: document.documentElement.scrollWidth > w + 1, out: [...document.querySelectorAll('#rvList .seal, #ordItems .seal, #ordBody .cxSeals .seal')].filter(s => { const r = s.getBoundingClientRect(); return r.width && (r.left < -1 || r.right > w + 1); }).length }; }).then(b => assert.deepEqual(b, { over: false, out: 0 }, what + ' fits: ' + JSON.stringify(b)));

    // one run of every check: returns the failures instead of throwing, so the same checks can be run against a build that drops the seals
    const checkAll = async () => {
      const fails = [];
      const check = async (what, fn) => { try { await fn(); } catch (e) { fails.push(what + ': ' + String(e.message).split('\n')[0].slice(0, 220)); } };
      await check('review completed', async () => {
        await page.evaluate(() => { document.querySelectorAll('.mNote').forEach(n => n.close && n.close()); CN.setMode('review'); });
        await page.click('#reviewView .rvSeg [data-cseg="done"]'); await settle();
        for (const [k, want] of Object.entries(COMPLETED)) {
          const [rid, sku] = k.split('|');
          const cards = await sealsIn(`#rvList .reviewListRow[data-rid="${rid}"]`);
          const card = cards.find(c => c.sku.trim() === sku);
          assert.ok(card, 'no Completed card for ' + k);
          assert.deepEqual(faces(card.seals), want, k + ' shows its seals: ' + JSON.stringify(card.seals));
          nodup(k, card.seals);
        }
        // a card is its seals once: the same seal never stands twice in the list
        const all = (await sealsIn('#rvList .reviewListRow')).flatMap(c => c.seals.map(s => c.rid + '|' + s[0] + '|' + s[3]));
        assert.equal(new Set(all).size, all.length, 'no seal doubled in the Completed list');
      });
      await check('orders list rows', async () => {
        for (const mode of ['list', 'cards']) {
          await page.evaluate(m => { CN.setMode('orders'); const b = document.querySelector(`#ordViewSeg [data-view="${m}"]`); if (b) b.click(); const p = document.querySelector('#ordChips [data-pile=""]'); if (p) p.click(); }, mode);
          await page.waitForSelector('#ordItems [data-key]'); await settle();
          const rows = await sealsIn('#ordItems [data-key]');
          for (const [key, want] of Object.entries(ROWS)) {
            const row = rows.find(r => r.key === key);
            assert.ok(row, mode + ': no row for ' + key);
            assert.deepEqual(faces(row.seals), want, mode + ' ' + key + ' shows its seals: ' + JSON.stringify(row.seals));
            nodup(mode + ' ' + key, row.seals);
          }
        }
      });
      await check('orders on hold', async () => {
        await page.evaluate(() => { CN.setMode('orders'); document.querySelector('#ordChips [data-pile="hold"]').click(); });
        await page.waitForSelector('#ordItems [data-key]'); await settle();
        const rows = await sealsIn('#ordItems [data-key]');
        assert.deepEqual(rows.map(r => r.key), [`${HD}_41700000071`], 'the held order only');
        assert.deepEqual(faces(rows[0].seals), [['ORDER COMPLETE', 'Ann', 0]], 'a held order keeps its seal');
        await page.evaluate(() => document.querySelector('#ordChips [data-pile=""]').click());
      });
      await check('orders cancelled', async () => {
        await page.evaluate(() => { CN.setMode('orders'); document.querySelector('#ordChips [data-pile="cancelled"]').click(); });
        await page.waitForSelector(`#ordBody .cxRow[data-rid="${CX}"]`); await settle();
        const cards = await sealsIn(`#ordBody .cxRow[data-rid="${CX}"]`);
        assert.deepEqual(faces(cards[0].seals), [['ORDER COMPLETE', 'Seth', 0], ['QR LABEL PRINTED', 'Seth', 1]], 'a cancelled order keeps its seals: ' + JSON.stringify(cards[0].seals));
        nodup('cancelled', cards[0].seals);
        await page.evaluate(() => document.querySelector('#ordChips [data-pile=""]').click());
      });
      return fails;
    };
    await page.evaluate(() => { window.OV_VIEW = m => { const b = document.querySelector(`#ordViewSeg [data-view="${m}"]`); if (b) b.click(); }; });

    // 1 · every surface, as drawn
    const first = await checkAll();
    assert.deepEqual(first, [], 'every Completed piece and order shows its seals:\n' + first.join('\n'));
    console.log('  ✓ Review → Completed (print + complete, complete only, older records, two lines of one SKU, one press, sent to the sheets, held, cancelled) and Orders (cards, list, On hold, Cancelled): each shows its own seals, once');

    // 2 · Complete Order and Reopen with the real buttons: the seal is on every surface, and stays after the reopen
    await page.evaluate(() => CN.setMode('review'));
    await page.click('#reviewView .rvSeg [data-cseg="open"]');
    await page.click('#reviewView .egTab[data-k="unmatchedSku"]');
    const ucard = `#rvList .reviewListRow[data-rid="${U}"]`, ukey = `${U}_41758924731`;
    await page.click(ucard + ' [data-cu-complete]');
    await page.waitForFunction(k => B.maps.customDone[k] && !document.querySelector('.cuStat, .btn.working'), ukey, { timeout: 30000 });
    await settle();
    const rec = srv.st.doc('Charm_Custom_Orders', ukey);
    assert.ok(rec && rec.how === 'button' && rec.stamps.length === 1, 'completed on the server record: ' + JSON.stringify(rec));
    await page.evaluate(() => document.querySelectorAll('.mNote').forEach(n => n.close && n.close()));
    await page.click('#reviewView .rvSeg [data-cseg="done"]'); await settle();
    const doneCards = (await sealsIn(`#rvList .reviewListRow[data-rid="${U}"]`)).filter(c => c.seals.length);
    assert.equal(doneCards.length, 1, 'its Completed card, with a seal, once: ' + JSON.stringify(doneCards));
    assert.deepEqual(faces(doneCards[0].seals), [['ORDER COMPLETE', 'Test Operator', 0]]);
    const rowSeals = async () => { await page.evaluate(() => { CN.setMode('orders'); document.querySelector('#ordChips [data-pile=""]').click(); }); await page.waitForSelector(`#ordItems [data-key="${ukey}"]`); await settle(); return faces((await sealsIn(`#ordItems [data-key="${ukey}"]`))[0].seals); };
    assert.deepEqual(await rowSeals(), [['ORDER COMPLETE', 'Test Operator', 0]], 'the Orders row shows it at once');
    await page.evaluate(() => CN.setMode('review'));
    await page.click('#reviewView .rvSeg [data-cseg="done"]');
    await page.click(ucard + ' [data-cu-reopen]');
    await page.waitForFunction(k => !B.maps.customDone[k] && B.maps.customKept[k], ukey, { timeout: 30000 }); await settle();
    assert.equal(srv.st.doc('Charm_Custom_Orders', ukey).state, 'open'); assert.equal(srv.st.doc('Charm_Custom_Orders', ukey).stamps.length, 1, 'the record keeps its seal');
    assert.deepEqual(await rowSeals(), [['ORDER COMPLETE', 'Test Operator', 0]], 'a reopened piece keeps its seal on the Orders row');
    await page.evaluate(() => CN.setMode('review')); await page.click('#reviewView .rvSeg [data-cseg="open"]'); await settle();
    const kept = await sealsIn(`#rvList .reviewListRow[data-rid="${U}"]`);
    assert.deepEqual(faces(kept[0].seals), [['ORDER COMPLETE', 'Test Operator', 0]], 'and on the Open card, on the button that made it');
    console.log('  ✓ Complete Order, then Reopen: the seal is on the Completed card and the Orders row, and stays on both after the reopen');

    // 3 · 900 and 390 px: nothing leaves the page, every seal is inside it
    // (the dark rail folds below 900 px, as the page's own toggle does)
    const fold = w => page.evaluate(w => { const off = document.getElementById('app').classList.contains('railOff'); if ((w < 900) !== off) document.getElementById('btnRail').click(); }, w);
    for (const [w, h] of [[900, 900], [390, 844]]) {
      await page.setViewportSize({ width: w, height: h }); await fold(w); await page.waitForTimeout(400);
      await page.evaluate(() => CN.setMode('review')); await page.click('#reviewView .rvSeg [data-cseg="done"]'); await settle();
      await bounds(w + ' px Completed');
      await page.evaluate(() => { CN.setMode('orders'); document.querySelector('#ordChips [data-pile=""]').click(); });
      for (const m of ['cards', 'list']) { await page.evaluate(v => OV_VIEW(v), m); await settle(); await bounds(`${w} px Orders ${m}`); }
      await page.evaluate(() => document.querySelector('#ordChips [data-pile="cancelled"]').click()); await page.waitForSelector('#ordBody .cxSeals .seal'); await bounds(`${w} px Cancelled`);
      await page.evaluate(() => document.querySelector('#ordChips [data-pile=""]').click());
    }
    await page.setViewportSize({ width: 1440, height: 950 }); await fold(1440); await page.waitForTimeout(300);
    console.log('  ✓ at 900 and 390 px the cards fit and every seal is inside the page');

    // 4 · screenshots of the fixture (the sorter's own page)
    if (shots) {
      const shot = async (name, prep, sel) => { await prep(); await settle(); await page.waitForTimeout(250); const n = sel ? page.locator(sel).first() : null; if (n) await n.screenshot({ path: path.join(shots, name) }); else await page.screenshot({ path: path.join(shots, name) }); };
      await shot('review-completed.png', async () => { await page.evaluate(() => CN.setMode('review')); await page.click('#reviewView .rvSeg [data-cseg="done"]'); }, '#reviewView');
      await shot('orders-cards.png', async () => { await page.evaluate(() => { CN.setMode('orders'); document.querySelector('#ordChips [data-pile=""]').click(); OV_VIEW('cards'); }); }, '#ordersView');
      await shot('orders-list.png', async () => { await page.evaluate(() => OV_VIEW('list')); }, '#ordersView');
      await shot('orders-on-hold.png', async () => { await page.evaluate(() => document.querySelector('#ordChips [data-pile="hold"]').click()); }, '#ordersView');
      await shot('orders-cancelled.png', async () => { await page.evaluate(() => document.querySelector('#ordChips [data-pile="cancelled"]').click()); await page.waitForSelector('#ordBody .cxSeals .seal'); }, '#ordersView');
      await page.setViewportSize({ width: 390, height: 844 }); await fold(390); await page.waitForTimeout(300);
      await shot('review-completed-390.png', async () => { await page.evaluate(() => CN.setMode('review')); await page.click('#reviewView .rvSeg [data-cseg="done"]'); }, null);
      await shot('orders-390.png', async () => { await page.evaluate(() => { CN.setMode('orders'); document.querySelector('#ordChips [data-pile=""]').click(); OV_VIEW('cards'); }); }, null);
      await page.setViewportSize({ width: 1440, height: 950 }); await fold(1440); await page.waitForTimeout(300);
      console.log('  . screenshots in ' + shots);
    }

    // 5 · a build that drops the seals is caught: the same checks, with the helper that finds them returning nothing and every list redrawn
    await page.evaluate(() => {
      Seal.ofPiece = () => []; Seal.pieceRow = () => '';
      for (const m of [B.maps.customDone, B.maps.customKept]) for (const k of Object.keys(m)) m[k] = Object.assign({}, m[k], { updatedAtMs: (m[k].updatedAtMs || 0) + 1, completedAt: (+m[k].completedAt || 0) ? +m[k].completedAt + 1 : m[k].completedAt, stamps: [] });
      Orders.interpretAll(); Review.syncOrderItems(); Review.render(); Orders.render();
    });
    const mutant = await checkAll();
    assert.ok(mutant.length >= 3, 'a build that drops the seals is caught by the checks: ' + JSON.stringify(mutant));
    console.log(`  ✓ a build whose helper returns no seals fails ${mutant.length} of the 4 checks`);

    assert.deepEqual(outside, [], 'no Etsy call');
    assert.deepEqual(errors, [], 'no page errors');
    console.log('Completed seals on Review and Orders OK');
  } finally { await browser.close(); srv.close(); }
})().catch(e => { console.error(e); process.exit(1); });
