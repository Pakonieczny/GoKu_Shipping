// Adversarial checks of the order view's Sheet tab (OrderWin's sheet view, SheetWin.drawOrder):
//  · an order split over a 14K sheet and a Rose Gold sheet (with its green lines saved): each sheet shows its own pieces,
//    and the panel's back engraving and metal are those of the charm shown, not of the line the view was opened on;
//  · Front/Back: the back words drawn are this charm's;
//  · a sheet switched while the last one is still being drawn: the old drawing never repaints or resizes the plate;
//  · "Open full sheet" hands over and comes back on the same sheet and charm, drawn from the record as it is now;
//  · a large sheet (90 pieces) draws, and hover/click still find the charm.
//   node tests/charm-nest/adv-sheettab.cjs   (PW_DIR=<playwright node_modules>, CHROMIUM=<chrome>)
const path = require('path'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');
const { start } = require('./bridge-server.cjs');

const DAY = 86400, SHIP = Math.floor(Date.UTC(2026, 9, 2, 17) / 1000);
const S = { rid: '4177000001', t1: '41770000011', t2: '41770000012' }, O = { rid: '4177000002', tid: '41770000021' };
const P1 = `${S.rid}_${S.t1}_1`, P2 = `${S.rid}_${S.t2}_1`, PO = `${O.rid}_${O.tid}_1`;
const SH14 = 'sheet-adv-14k', SHRG = 'sheet-adv-rg', BIG = 'sheet-adv-big';
const line = (tid, sku, metalKey, metalLabel) => ({ transactionId: tid, listingId: '18000' + tid.slice(-5), sku, title: sku + ' necklace', quantity: 1, expectedShipDate: SHIP, variations: [{ name: 'Metal', value: metalLabel }], metalKey, metalLabel, personalization: [], buyerMessage: '' });
const order = (rid, buyer, lines) => ({ receiptId: rid, orderNumber: rid, createTs: SHIP - 5 * DAY, updateTs: SHIP - 5 * DAY + 60, shipBy: SHIP, buyer: { name: buyer }, buyerMessage: '', isGift: false, giftMessage: '', staffNote: '', messages: [], lines });

async function main() {
  const pwDir = process.env.PW_DIR || path.join(root, 'node_modules');
  let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { console.log('  – no playwright-core: the browser checks were not run'); return; }
  const srv = await start({ receipts: [] });
  const box = (id, cx, cy, w = 34) => ({ id, cxPt: cx, cyPt: cy, angle: 0, wPt: w, hPt: w });
  // a 14K sheet (wide) with line 1's piece, its back engraving approved; a Rose Gold sheet (square) with line 2's piece,
  // behind a saved green line; a big GF sheet with 90 pieces, one of them O's
  srv.st.put('Charm_Nest_Sheets', SH14, { id: SH14, metal: 'gold14k', sheetIndex: 1, day: '2026-09-27', status: 'written', stock: { wPt: 300, hPt: 120 }, orders: [S.rid, O.rid],
    placements: [box('a1', 60, 60), box('ao', 200, 60)], charms: [{ id: 'a1', name: `${S.rid} · TINY_TAG`, poolId: P1, order: S.rid, sku: 'TINY_TAG' }, { id: 'ao', name: `${O.rid} · OTHER`, poolId: PO, order: O.rid, sku: 'OTHER' }],
    backPool: [{ poolId: P1, order: S.rid, sku: 'TINY_TAG', copy: 1, text: 'For Mia', approvedAt: Date.now() - 1000, approvedBy: 'Test Operator' }] });
  srv.st.put('Charm_Nest_Sheets', SHRG, { id: SHRG, metal: 'rose', sheetIndex: 3, day: '2026-09-27', status: 'written', stock: { wPt: 180, hPt: 180 }, orders: [S.rid],
    rosePlan: { stages: [{ n: 1, ids: ['r2'], at: Date.now() - 5000 }] }, roseStock: { id: 'rs-1' },
    placements: [box('r2', 90, 90)], charms: [{ id: 'r2', name: `${S.rid} · LEAF_CHARM`, poolId: P2, order: S.rid, sku: 'LEAF_CHARM' }] });
  const big = [], bigC = [];
  for (let i = 0; i < 90; i++) { const id = 'b' + i, rid = i === 45 ? O.rid : String(4178000000 + i); big.push(box(id, 12 + (i % 15) * 22, 12 + Math.floor(i / 15) * 22, 18)); bigC.push({ id, name: `${rid} · SKU${i}`, poolId: i === 45 ? PO : `${rid}_${rid}1_1`, order: rid, sku: 'SKU' + i }); }
  srv.st.put('Charm_Nest_Sheets', BIG, { id: BIG, metal: 'gold', sheetIndex: 7, day: '2026-09-27', status: 'written', stock: { wPt: 340, hPt: 140 }, orders: [...new Set(bigC.map(c => c.order))], placements: big, charms: bigC });
  for (const [p, sheet, mat, name] of [[P1, SH14, 'gold14k', '2026-09-27_14K_Set-1_Sheet-1'], [P2, SHRG, 'rose', '2026-09-27_RG_Set-1_Sheet-3']]) srv.st.put('Charm_Pool', p, { poolId: p, orderId: S.rid, transactionId: p.split('_')[1], lineKey: p.replace(/_1$/, ''), sku: p === P1 ? 'TINY_TAG' : 'LEAF_CHARM', material: mat, copy: 1, quantity: 1, state: 'written', sheetId: sheet, sheetName: name, updatedAt: Date.now() });
  srv.st.put('Charm_Pool', '4178000046_41780000461_1', { poolId: '4178000046_41780000461_1', orderId: '4178000046', transactionId: '41780000461', lineKey: '4178000046_41780000461', sku: 'SKU46', material: 'gold', copy: 1, quantity: 1, state: 'written', sheetId: BIG, sheetName: 'GF_Sheet-7', updatedAt: Date.now() });
  srv.st.put('Charm_Pool', PO, { poolId: PO, orderId: O.rid, transactionId: O.tid, lineKey: `${O.rid}_${O.tid}`, sku: 'OTHER', material: 'gold', copy: 1, quantity: 1, state: 'written', sheetId: BIG, sheetName: 'GF_Sheet-7', updatedAt: Date.now() });
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  try {
    const context = await browser.newContext({ viewport: { width: 1440, height: 900 } });
    await context.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => /fonts\.googleapis|fonts\.gstatic/.test(r.request().url()) ? r.fulfill({ status: 200, contentType: 'text/css', body: '' }) : r.abort());
    await context.addInitScript(() => { try { if (!sessionStorage.getItem('__seeded')) { localStorage.setItem('cn.settings', JSON.stringify({ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on' })); localStorage.setItem('cn.employee', 'Test Operator'); sessionStorage.setItem('__seeded', '1'); } } catch (_) {} window.prompt = () => 'Test Operator'; });
    const page = await context.newPage(), errors = [];
    page.setDefaultTimeout(20000);
    page.on('pageerror', e => { errors.push(e.message); console.error('page error:', String(e.stack || e.message).split('\n').slice(0, 4).join(' | ')); });
    await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
    await page.waitForFunction(() => window.CN && window.Orders && window.OrderWin && window.SheetWin && SheetWin.drawOrder && CN.S.cloud.ok === true, null, { timeout: 60000 });
    await page.evaluate(async ({ orders }) => {
      await Orders.loadMaps(true);
      for (const [order, pools] of orders) order.lines.forEach((line, i) => { const key = CharmNestOrders.lineKey(order, line); const row = { key, order, line, arrivedAt: Date.now(), spec: null, problems: [], state: 'pooled', reason: null, claimedBy: null, poolIds: [pools[i]], engrave: null, material: null }; B.orders.rows.push(row); B.orders.byKey.set(key, row); });
      Orders.interpretAll();
      // line 1 carries a back engraving; line 2 none
      const r1 = B.orders.rows.find(r => r.poolIds[0] === orders[0][1][0]); r1.engrave = { needed: true, text: 'For Mia', approved: true, state: 'approved' };
      CN.setMode('orders'); Orders.render();
    }, { orders: [[order(S.rid, 'Mia Lund', [line(S.t1, 'TINY_TAG', 'gold14k', '14k Solid Gold'), line(S.t2, 'LEAF_CHARM', 'rose', 'Rose Gold Filled')]), [P1, P2]], [order(O.rid, 'Ola Berg', [line(O.tid, 'OTHER', 'gold', '14k Gold Filled')]), [PO]]] });
    const drawn = (rid, sheet) => page.waitForFunction(([rid, sheet]) => { const i = OrderWin._sheet(); return i && i.sheet.id === sheet && i.mine.every(x => x.rid === rid) && document.getElementById('owPlateWait').hidden && !document.getElementById('owSheetCv').getAnimations().some(a => a.playState === 'running'); }, [rid, sheet]);
    const panel = () => page.evaluate(() => ({ chips: [...document.querySelectorAll('#owSheetPanel .owShTabs button')].map(b => b.textContent.trim()), on: document.querySelector('#owSheetPanel .owShTabs button.on')?.textContent.trim(),
      eng: document.querySelector('#owSheetPanel .owBackEng')?.textContent || '', charm: document.querySelector('#owSheetPanel .owCharm')?.textContent || '', mine: OrderWin._sheet()?.mine.map(x => x.poolId) }));

    // 1 · opened on line 1 (14K): both sheets, the 14K one first, its back engraving
    await page.evaluate(k => OrderWin.open(k, { view: 'sheet' }), `${S.rid}_${S.t1}`);
    await page.evaluate(() => OrderWin.view() === 'sheet' || document.querySelector('.owTabsV [data-ow-view="sheet"]').click());
    await drawn(S.rid, SH14);
    let p = await panel();
    assert.deepEqual(p.chips.sort(), ['14K Sheet 1', 'RG Sheet 3'], 'both sheets of the split order'); assert.deepEqual(p.mine, [P1]); assert.match(p.eng, /For Mia/);
    // 2 · the Rose Gold sheet: its own piece, and the panel speaks of that charm (no engraving, Rose Gold), not line 1's
    await page.evaluate(() => [...document.querySelectorAll('#owSheetPanel .owShTabs button')].find(b => /RG/.test(b.textContent)).click());
    await drawn(S.rid, SHRG);
    p = await panel();
    assert.deepEqual(p.mine, [P2], 'the RG sheet shows line 2\'s piece');
    assert.doesNotMatch(p.eng, /For Mia/, 'line 1\'s back engraving is not shown for the RG charm: ' + p.eng);
    assert.doesNotMatch(p.charm, /14K|14k/, 'the RG charm is not labelled 14K: ' + p.charm);
    // Back: no words on the RG charm (line 1's words belong to the 14K sheet)
    assert.equal(await page.evaluate(() => OrderWin._sheet().mine.filter(x => x.eng && x.eng.text).length), 0);
    await page.evaluate(() => [...document.querySelectorAll('#owSheetPanel .owShTabs button')].find(b => /14K/.test(b.textContent)).click());
    await drawn(S.rid, SH14);
    assert.deepEqual(await page.evaluate(() => OrderWin._sheet().mine.map(x => x.eng && x.eng.text)), ['For Mia'], 'the 14K charm\'s back words');

    // 3 · a drawing left behind (the RG sheet's, still reading when the 14K sheet was chosen) never touches the plate again
    const size14 = await page.evaluate(() => { const cv = document.getElementById('owSheetCv'); return [cv.width, cv.height]; });
    await page.evaluate(() => [...document.querySelectorAll('#owSheetPanel .owShTabs button')].find(b => /RG/.test(b.textContent)).click());
    await drawn(S.rid, SHRG);
    await page.evaluate(() => { window.__oldInfo = OrderWin._sheet(); [...document.querySelectorAll('#owSheetPanel .owShTabs button')].find(b => /14K/.test(b.textContent)).click(); });
    await drawn(S.rid, SH14);
    const after = await page.evaluate(async () => { window.__oldInfo.redraw(); window.__oldInfo.back(true); await new Promise(r => requestAnimationFrame(() => requestAnimationFrame(r))); const cv = document.getElementById('owSheetCv'); return [cv.width, cv.height]; });
    assert.deepEqual(after, size14, 'the old sheet\'s drawing does not resize the plate to its own shape');
    // (and no second loop keeps painting the old sheet: the plate is painted once a frame)
    const perFrame = await page.evaluate(async () => {
      const proto = CanvasRenderingContext2D.prototype, orig = proto.drawImage; let n = 0;
      proto.drawImage = function (...a) { if (this.canvas && this.canvas.id === 'owSheetCv') n++; return orig.apply(this, a); };
      let frames = 0; const t0 = performance.now(); await new Promise(res => { const f = () => { frames++; if (performance.now() - t0 < 600) requestAnimationFrame(f); else res(); }; requestAnimationFrame(f); });
      proto.drawImage = orig; return n / Math.max(1, frames);
    });
    assert(perFrame <= 1.3, 'one drawing loop on the plate, not one per sheet shown: ' + perFrame.toFixed(2) + ' draws a frame');

    // 4 · Open full sheet (RG, its charm chosen) and back: the same sheet and charm, drawn from the record as it is now
    await page.evaluate(() => [...document.querySelectorAll('#owSheetPanel .owShTabs button')].find(b => /RG/.test(b.textContent)).click());
    await drawn(S.rid, SHRG);
    await page.click('#owSheetPanel [data-full]');
    await page.waitForFunction(sh => SheetWin.isOpen() && SheetWin.current() === sh && !document.getElementById('orderWin').open, SHRG);
    assert.equal(await page.evaluate(() => document.querySelectorAll('dialog[open]').length), 1, 'the sheet window takes the view\'s place');
    // (meanwhile the sheet changes: its piece moves)
    const rg = srv.st.doc('Charm_Nest_Sheets', SHRG); srv.st.put('Charm_Nest_Sheets', SHRG, Object.assign({}, rg, { placements: [box('r2', 40, 130)] }));
    await page.waitForTimeout(700);
    await page.evaluate(() => { const d = SheetWin.drawOrder; window.__drew = []; SheetWin.drawOrder = (cv, t, ...a) => { window.__drew.push(t && t.sheetId || t); return d(cv, t, ...a); }; SheetWin.close(); });
    await page.waitForFunction(() => document.getElementById('orderWin').open && OrderWin.view() === 'sheet', null, { timeout: 5000 });
    const seen = [];
    await page.waitForFunction(sh => { const i = OrderWin._sheet(); return i && i.sheet.id === sh && document.getElementById('owPlateWait').hidden; }, SHRG).catch(async () => { seen.push(await page.evaluate(() => OrderWin._sheet()?.sheet.id)); });
    assert.deepEqual(seen, [], 'comes back on the RG sheet it left from');
    assert.deepEqual(await page.evaluate(() => window.__drew), [SHRG], 'straight to it, not the first sheet drawn and then this one');
    const back = await page.evaluate(() => { const x = OrderWin._sheet().mine[0]; return { at: [x.p.cxPt, x.p.cyPt], on: document.querySelector('#owSheetPanel .owPieces li.on .sku')?.textContent }; });
    assert.deepEqual(back.at, [40, 130], 'the sheet is drawn as it is now, not from a copy read before the hand-over');

    // 5 · a large sheet (90 pieces): O's piece in gold, hover finds it, a click on another piece swaps the order
    await page.evaluate(() => OrderWin.close && OrderWin.close());
    await page.waitForFunction(() => !document.getElementById('orderWin').open, null, { timeout: 3000 }).catch(() => page.keyboard.press('Escape'));
    await page.waitForFunction(() => !document.getElementById('orderWin').open, null, { timeout: 3000 });
    await page.evaluate(k => OrderWin.open(k, { view: 'sheet' }), `${O.rid}_${O.tid}`);
    await page.evaluate(() => OrderWin.view() === 'sheet' || document.querySelector('.owTabsV [data-ow-view="sheet"]').click());
    const t0 = Date.now();
    await drawn(O.rid, BIG);
    const took = Date.now() - t0;
    const bigInfo = await page.evaluate(() => { const i = OrderWin._sheet(); return { n: i.pieces.length, mine: i.mine.map(x => x.poolId), foot: document.getElementById('owPlateFoot').textContent }; });
    assert.equal(bigInfo.n, 90); assert.deepEqual(bigInfo.mine, [PO]); assert.match(bigInfo.foot, /90 charms/); assert(took < 8000, 'drawn in ' + took + ' ms');
    const pt = await page.evaluate(k => OrderWin._sheet().pointOf(k), PO);
    await page.mouse.move(pt.x, pt.y);
    await page.waitForFunction(rid => !document.getElementById('owPlateTip').hidden && document.getElementById('owPlateTip').textContent.startsWith(rid), O.rid);
    const pn = await page.evaluate(() => OrderWin._sheet().pointOf('b46'));
    await page.mouse.click(pn.x, pn.y);
    await page.waitForFunction(() => /Order 4178000046/.test(document.getElementById('owTitle').textContent) && OrderWin._sheet() && OrderWin._sheet().sheet.id === 'sheet-adv-big', null, { timeout: 15000 });
    assert.equal(await page.evaluate(() => document.querySelectorAll('dialog[open]').length), 1, 'one window');
    assert.deepEqual(errors, [], 'no page errors');
    console.log('  ✓ split 14K/RG order, back words per charm, a stale drawing kept off the plate, full sheet and back, 90-piece sheet');
  } finally { await browser.close(); srv.close(); }
}
main().then(() => console.log('Sheet tab (adversarial) OK')).catch(e => { console.error(e); process.exit(1); });
