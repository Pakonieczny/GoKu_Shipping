// A piece's status on its row under "Its pieces" in the order window (Paul, 5 Oct 2026, point 4): "Remove the 'Next: Laser...' text and functionality
// from this specific UI in all detailed order modals. Also the 'On RG Sheet 1' looks very beta and afterthought in design, style, functionality.
// Please update the UI so it's of the same flow state and calibre as the other surrounding UI ... Keep it minimalist in design and core
// functionality."
// Proved here, in headless Chromium over the real charmNestLibrary ops and an in-memory store (nothing live, no Etsy, no paid call):
//  - every state the row can be in says ONE short plain thing: "GF Sheet 1" (on a sheet), the same with a small check (cut, or its set sent to the
//    laser), "Waiting for a sheet", "On hold", "Cancelled", "Sorted", "Shipped", "Completed"; never "On ...", never "next: ..." anywhere in the window
//  - the chip is the Sheet card's own (owShChip), in the app's type (not monospace, not brown), never cut short with an ellipsis, a dot in the
//    sheet's metal; hollow for waiting and cancelled; the Hold button's orange for on hold
//  - a sheet's chip is a button (aria-label "Open GF Sheet 1", in the tab order): a press or Enter opens that sheet in the Sheet view (no pop-up
//    over the pop-up), and does not press the row; the others only say where the piece is
//  - hover and focus: a tint fading in (opacity) and a 1px lift (transform), nothing else animates
//  - no "next: ..." anywhere in the order window but the header rail's own Now strip (another element, which this change leaves alone)
//  - the source has no next-step code any more, and a mutant that brings "next: ..." back is caught
//  - 1440, 900 and 390 px: the chip fits inside its row beside the name and the Hold button (or under the name where the room is short), never
//    clipped, no sideways scroll of the list
//   node tests/charm-nest/piece-status-chip.cjs   (PW_DIR=<playwright-core's node_modules>, CHROMIUM=<chrome>, SHOTS=<dir>)
const fs = require('fs'), path = require('path'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');
const { start } = require('./bridge-server.cjs');

const DAY = 86400, SHIP = Math.floor(Date.UTC(2026, 9, 10, 17) / 1000);
const A = { rid: '4180000001', a1: '41800000011', a2: '41800000012', a3: '41800000013' };                       // on GF Sheet 1, on RG Sheet 1 and its set sent, waiting
const B = { rid: '4180000002', b1: '41800000021', b2: '41800000022' };                                           // an order on hold (no Hold button: plain rows)
const E = { rid: '4180000005', e1: '41800000051', e2: '41800000052' };                                           // one piece cancelled, the other on GF Sheet 1
const C = { rid: '4180000003', c1: '41800000031', c2: '41800000032', c3: '41800000033', c4: '41800000034' };    // cut, sorted, shipped, completed by hand
const D = { rid: '4180000004', d1: '41800000041' };                                                            // a single piece
const GF1 = 'sheet-psc-gf1', RG1 = 'sheet-psc-rg1';
const kOf = (o, t) => `${o.rid}_${o[t]}`, pidOf = (o, t) => `${o.rid}_${o[t]}_1`;
const line = (tid, sku, metalKey, metalLabel) => ({ transactionId: tid, listingId: '19008' + tid.slice(-5), sku, title: sku + ' necklace', quantity: 1, expectedShipDate: SHIP, variations: [{ name: 'Metal', value: metalLabel }], metalKey, metalLabel, personalization: [], buyerMessage: '' });
const order = (rid, buyer, lines) => ({ receiptId: rid, orderNumber: rid, createTs: SHIP - 7 * DAY, updateTs: SHIP - 7 * DAY + 60, shipBy: SHIP, buyer: { name: buyer }, buyerMessage: '', isGift: false, giftMessage: '', staffNote: '', messages: [], lines });
const GOLD = ['gold', '14k Gold Filled'], ROSE = ['rose', 'Rose Gold Filled'], SILVER = ['silver', 'Sterling Silver'];
const ORDERS = [
  [order(A.rid, 'Ada Alpha', [line(A.a1, 'MIDDLE_9935', ...GOLD), line(A.a2, 'MIDDLE_9935', ...ROSE), line(A.a3, 'STAR_C', ...GOLD)]), [pidOf(A, 'a1'), pidOf(A, 'a2'), null]],
  [order(B.rid, 'Bea Beta', [line(B.b1, 'MIDDLE_9935', ...GOLD), line(B.b2, 'MIDDLE_9935', ...ROSE)]), [pidOf(B, 'b1'), pidOf(B, 'b2')]],
  [order(E.rid, 'Eve Epsilon', [line(E.e1, 'MIDDLE_9935', ...SILVER), line(E.e2, 'MIDDLE_9935', ...GOLD)]), [pidOf(E, 'e1'), pidOf(E, 'e2')]],
  [order(C.rid, 'Cy Gamma', [line(C.c1, 'MIDDLE_9935', ...GOLD), line(C.c2, 'MIDDLE_9935', ...ROSE), line(C.c3, 'STAR_C', ...GOLD), line(C.c4, 'STAR_C', ...GOLD)]), [pidOf(C, 'c1'), pidOf(C, 'c2'), pidOf(C, 'c3'), pidOf(C, 'c4')]],
  [order(D.rid, 'Dee Delta', [line(D.d1, 'MIDDLE_9935', ...GOLD)]), [pidOf(D, 'd1')]]];
// "next: ..." as it was: the checker every state goes through, in the page
const NO_NEXT = () => {
  const bad = [];
  // (the header rail's own "Now" strip, #owRail .tlNowS, is another element of the timeline: it is not the pieces' status and is left as it is)
  for (const n of document.querySelectorAll('#orderWin, #orderWin *')) { if (n.closest('#owRail')) continue; for (const t of n.childNodes) if (t.nodeType === 3 && /\bnext\s*:/i.test(t.nodeValue)) bad.push(((n.closest('[id]') || {}).id || '') + ' > ' + (n.parentElement ? n.parentElement.className : '') + ' | ' + t.nodeValue.trim()); }
  for (const n of document.querySelectorAll('#owPcSum [aria-label],#owPcSum [title],#owPcSum [data-why]')) for (const a of ['aria-label', 'title', 'data-why']) if (/\bnext\s*:/i.test(n.getAttribute(a) || '')) bad.push(a + ' | ' + n.getAttribute(a));
  return bad;
};

async function main() {
  // the source itself: no next-step computation, no "next: " text, in the row's builder
  const src = fs.readFileSync(path.join(root, 'charm-nest-bridge.js'), 'utf8'), a = src.indexOf('function paintPieceSum('), b = src.indexOf('function wirePcAct(');
  assert(a > 0 && b > a, 'the row builders are found');
  const rowCode = src.slice(a, b);
  assert(!/next\s*:/i.test(rowCode.replace(/\/\*[\s\S]*?\*\/|\/\/[^\n]*/g, '')), 'the row builders hold no "next: " text');
  assert(!/\bnx\b/.test(rowCode.replace(/\/\*[\s\S]*?\*\/|\/\/[^\n]*/g, '')), 'and no next-step computation (nx)');

  const pwDir = process.env.PW_DIR || path.join(root, 'node_modules');
  let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { console.log('  – no playwright-core: the browser checks were not run'); return; }
  const shots = process.env.SHOTS || null; if (shots) fs.mkdirSync(shots, { recursive: true });
  const srv = await start({ receipts: [] });
  const box = (id, cx, cy) => ({ id, cxPt: cx, cyPt: cy, angle: 0, wPt: 34, hPt: 34 });
  const ch = (id, o, t, sku) => ({ id, name: `${o.rid} · ${sku}`, poolId: pidOf(o, t), order: o.rid, sku });
  srv.st.put('Charm_Nest_Sheets', GF1, { id: GF1, metal: 'gold', sheetIndex: 1, day: '2026-10-04', status: 'written', stock: { wPt: 300, hPt: 140 }, orders: [A.rid, B.rid, C.rid, D.rid, E.rid],
    placements: [box('g1', 40, 40), box('g2', 90, 40), box('g3', 140, 40), box('g4', 190, 40), box('g5', 40, 90), box('g6', 90, 90), box('g7', 140, 90)],
    charms: [ch('g1', A, 'a1', 'MIDDLE_9935'), ch('g2', B, 'b1', 'MIDDLE_9935'), ch('g3', C, 'c1', 'MIDDLE_9935'), ch('g4', C, 'c3', 'STAR_C'), ch('g5', C, 'c4', 'STAR_C'), ch('g6', D, 'd1', 'MIDDLE_9935'), ch('g7', E, 'e2', 'MIDDLE_9935')] });
  srv.st.put('Charm_Nest_Sheets', RG1, { id: RG1, metal: 'rose', sheetIndex: 1, day: '2026-10-04', status: 'written', stock: { wPt: 200, hPt: 140 }, orders: [A.rid, B.rid, C.rid],
    placements: [box('r1', 40, 40), box('r2', 90, 40), box('r3', 140, 40)], charms: [ch('r1', A, 'a2', 'MIDDLE_9935'), ch('r2', B, 'b2', 'MIDDLE_9935'), ch('r3', C, 'c2', 'MIDDLE_9935')] });
  const poolRow = (o, t, sku, material, sheetId, metal) => srv.st.put('Charm_Pool', pidOf(o, t), { poolId: pidOf(o, t), orderId: o.rid, transactionId: o[t], lineKey: kOf(o, t), sku, material, copy: 1, quantity: 1, state: 'written', sheetId, sheetName: `2026-10-04_${metal}_Set-1_Sheet-1`, updatedAt: Date.now() });
  poolRow(A, 'a1', 'MIDDLE_9935', 'gold', GF1, 'GF'); poolRow(A, 'a2', 'MIDDLE_9935', 'rose', RG1, 'RG');
  poolRow(B, 'b1', 'MIDDLE_9935', 'gold', GF1, 'GF'); poolRow(B, 'b2', 'MIDDLE_9935', 'rose', RG1, 'RG');
  poolRow(C, 'c1', 'MIDDLE_9935', 'gold', GF1, 'GF'); poolRow(C, 'c2', 'MIDDLE_9935', 'rose', RG1, 'RG'); poolRow(C, 'c3', 'STAR_C', 'gold', GF1, 'GF'); poolRow(C, 'c4', 'STAR_C', 'gold', GF1, 'GF');
  poolRow(D, 'd1', 'MIDDLE_9935', 'gold', GF1, 'GF'); poolRow(E, 'e1', 'MIDDLE_9935', 'silver', GF1, 'SS'); poolRow(E, 'e2', 'MIDDLE_9935', 'gold', GF1, 'GF');
  // the orders' timelines
  const evAt = Date.now(); let evN = 0;
  const ev = (o, type, ago, extra) => srv.st.put('Order_Timeline', `${o.rid}~${type}~e${++evN}`, Object.assign({ orderId: o.rid, type, at: evAt - ago * 60000, by: 'Test Operator', source: 'sorter', station: '', text: '', data: {} }, extra || {}));
  const placed = (o, t, sheet, sheetId, ago) => ev(o, 'placed', ago, { sheet, sheetId, lineKey: kOf(o, t), data: { poolId: pidOf(o, t) } });
  for (const o of [A, B, C, D, E]) ev(o, 'arrived', 4000, { source: 'etsy', by: 'Etsy' });
  placed(A, 'a1', 'GF Sheet 1', GF1, 3000); placed(A, 'a2', 'RG Sheet 1', RG1, 2900);
  ev(A, 'setCommitted', 2000, { sheetId: RG1, sheet: 'RG Sheet 1', setId: 'set-psc-1' });                   // (the set of RG Sheet 1 is sent to the laser: its chip has the check)
  placed(B, 'b1', 'GF Sheet 1', GF1, 3000); placed(B, 'b2', 'RG Sheet 1', RG1, 2900);
  placed(C, 'c1', 'GF Sheet 1', GF1, 3000); placed(C, 'c2', 'RG Sheet 1', RG1, 2900); placed(C, 'c3', 'GF Sheet 1', GF1, 2800); placed(C, 'c4', 'GF Sheet 1', GF1, 2700);
  ev(C, 'laserDone', 2000, { sheet: 'GF Sheet 1', sheetId: GF1, lineKey: kOf(C, 'c1') });                      // c1: cut on the laser
  ev(C, 'laserDone', 1900, { sheet: 'RG Sheet 1', sheetId: RG1, lineKey: kOf(C, 'c2') }); ev(C, 'sorted', 1500, { lineKey: kOf(C, 'c2'), station: 'sorting' });   // c2: sorted
  ev(C, 'laserDone', 1800, { sheet: 'GF Sheet 1', sheetId: GF1, lineKey: kOf(C, 'c3') }); ev(C, 'sorted', 1400, { lineKey: kOf(C, 'c3'), station: 'sorting' });
  ev(C, 'assembled', 1000, { lineKey: kOf(C, 'c3'), station: 'assembly' }); ev(C, 'shipped', 500, { lineKey: kOf(C, 'c3'), station: 'shipping' });                   // c3: shipped
  ev(C, 'sealCompleted', 300, { lineKey: kOf(C, 'c4'), data: { how: 'button', pressedIn: 'Order window' } });                                                       // c4: completed by hand
  placed(D, 'd1', 'GF Sheet 1', GF1, 3000);
  placed(E, 'e1', 'SS Sheet 1', GF1, 3000); placed(E, 'e2', 'GF Sheet 1', GF1, 2900);
  ev(E, 'cancelled', 1000, { lineKey: kOf(E, 'e1'), text: 'Cancelled by the buyer', data: { reason: 'buyer asked' } });                                    // e1: cancelled
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  try {
    const context = await browser.newContext({ viewport: { width: 1440, height: 900 } });
    const js = body => ({ status: 200, contentType: 'text/javascript', headers: { 'Cross-Origin-Resource-Policy': 'cross-origin', 'Access-Control-Allow-Origin': '*' }, body });
    const outside = [];
    await context.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => {
      const u = r.request().url();
      if (/cdn\.jsdelivr\.net\/npm\/pdfmake@[^/]+\/build\/pdfmake/.test(u)) return r.fulfill(js('window.pdfMake = { createPdf() { return { getBlob(cb) { cb(new Blob([""])); } }; } };'));
      if (/cdn\.jsdelivr\.net\/npm\/pdfmake@[^/]+\/build\/vfs_fonts/.test(u)) return r.fulfill(js(''));
      if (/qrcodejs/.test(u)) return r.fulfill(js(fs.readFileSync(path.join(root, 'lib/qrcode.min.js'))));
      if (/fonts\.googleapis|fonts\.gstatic/.test(u)) return r.fulfill({ status: 200, contentType: 'text/css', body: '' });
      if (/etsy/i.test(u)) outside.push(u);
      return r.abort();
    });
    await context.addInitScript(() => { try { if (!sessionStorage.getItem('__seeded')) { localStorage.setItem('cn.settings', JSON.stringify({ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on' })); localStorage.setItem('cn.employee', 'Test Operator'); sessionStorage.setItem('__seeded', '1'); } } catch (_) {} window.prompt = () => 'Test Operator'; });
    const seedPage = async (page, errors) => {
      page.setDefaultTimeout(30000);
      page.on('pageerror', e => { errors.push(e.message); console.error('page error:', String(e.stack || e.message).split('\n').slice(0, 4).join(' | ')); });
      await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
      await page.waitForFunction(() => window.CN && window.Orders && window.OrderWin && window.OrderTimeline && CN.S.cloud.ok === true, null, { timeout: 60000 });
      await page.evaluate(async ({ orders }) => {
        await Orders.loadMaps(true);
        for (const s of ['MIDDLE_9935', 'STAR_C']) B.master.entries.set(s, { sku: s, updatedAt: 1 });   // (designs in a master file: the pieces are in no Review card)
        for (const [order, pools] of orders) order.lines.forEach((line, i) => { const key = CharmNestOrders.lineKey(order, line); if (B.orders.byKey.has(key)) return; const row = { key, order, line, arrivedAt: Date.now(), spec: null, problems: [], state: pools[i] ? 'pooled' : 'pulled', reason: null, claimedBy: null, poolIds: pools[i] ? [pools[i]] : [], engrave: null, material: null }; B.orders.rows.push(row); B.orders.byKey.set(key, row); });
        Orders.interpretAll();
        // the order on hold: every piece held but the cancelled one (so the order shows no Hold button of its own, and its rows are plain)
        for (const r of B.orders.rows) if (String(r.order.receiptId) === '4180000002') { r.hold = { by: 'Test Operator', at: Date.now() }; r.reason = 'on hold'; }
        CN.setMode('orders'); Orders.render();
      }, { orders: ORDERS });
    };
    const page = await context.newPage(), errors = [];
    await seedPage(page, errors);

    const shot = async name => { if (shots) { await page.waitForTimeout(900); await page.screenshot({ path: path.join(shots, name + '.png') }); } };
    const closeWin = async () => { await page.evaluate(() => OrderWin.isOpen() && OrderWin.close()); await page.waitForFunction(() => !document.getElementById('orderWin').open, null, { timeout: 5000 }); };
    // each row of "Its pieces", as drawn
    const rows = () => page.evaluate(() => [...document.querySelectorAll('#owPcSum .owPcRow')].map(r => {
      const st = r.querySelector('.st'), c = r.querySelector('.pcSt'), cs = c && getComputedStyle(c), sp = c && c.querySelector('span'), sps = sp && getComputedStyle(sp);
      return { key: r.dataset.piece || (r.querySelector('[data-pc-act]') || { dataset: {} }).dataset.pcAct, tag: r.tagName, hold: r.querySelectorAll('.holdBtn').length, hidden: st && st.classList.contains('owPcSr') ? st.textContent.trim() : null,
        chip: c && { tag: c.tagName, text: c.textContent.replace(/\s+/g, ' ').trim(), cls: (c.dataset.k || '') + ([...c.classList].includes('off') ? ' off' : ''), aria: c.getAttribute('aria-label'), check: !!c.querySelector('.pcCk'), tab: c.tabIndex,
          font: sps.fontFamily, size: sps.fontSize, weight: sps.fontWeight, color: sps.color, ellipsis: sps.textOverflow, over: getComputedStyle(c).textOverflow, clipped: c.scrollWidth > c.clientWidth + 1 || sp.scrollWidth > sp.clientWidth + 1,
          dot: getComputedStyle(c.querySelector('i')).backgroundColor, rowDot: getComputedStyle(r.querySelector('.dot')).backgroundColor, border: cs.borderTopStyle, radius: cs.borderTopLeftRadius, bg: cs.backgroundColor } };
    }));
    const byKey = (R, o, t) => R.find(r => r.key === kOf(o, t));
    const openWin = async (o, n) => { await page.evaluate(k => OrderWin.open(k), kOf(o, Object.keys(o).find(k => k !== 'rid'))); await page.waitForFunction(([r, n]) => OrderWin.rid() === r && document.querySelectorAll('#owPcSum .owPcRow').length === n && [...document.querySelectorAll('#owPcSum .owPcRow')].every(x => x.querySelector('.st')), [o.rid, n], { timeout: 20000 }).catch(async e => { throw new Error('the window did not draw ' + n + ' rows: ' + JSON.stringify(await page.evaluate(() => ({ rid: OrderWin.rid(), pieces: document.getElementById('owPcSum').hidden ? 'hidden' : [...document.querySelectorAll('#owPcSum .owPcRow')].map(r => r.className + ' | ' + r.textContent.replace(/\s+/g, ' ').trim()) })))); }); await page.waitForTimeout(600); };
    // every state, drawn: the words, the look, the chip a button only when it opens a sheet
    const nothing = async what => { const bad = await page.evaluate(NO_NEXT); assert.deepEqual(bad, [], 'no "next: ..." anywhere in the window, ' + what + ': ' + JSON.stringify(bad)); };
    const plain = (c, what) => {
      assert(!/mono|Menlo|Consolas|Courier/i.test(c.font), what + ': the app\'s type, not monospace: ' + c.font);
      assert.equal(c.ellipsis, 'clip', what + ': no ellipsis'); assert(!c.clipped, what + ': never cut short');
      assert.notEqual(c.color, 'rgb(122, 90, 29)', what + ': not the old brown words');
      assert(!/^On (?!hold$)/.test(c.text), what + ': says where, not "On ..." ' + c.text);
    };

    // ── 1 · Ada Alpha's order (three pieces: on GF Sheet 1, on RG Sheet 1 with its set sent, waiting): hold rows, chips that open sheets ──
    await openWin(A, 3);
    let R = await rows();
    const a1 = byKey(R, A, 'a1'), a2 = byKey(R, A, 'a2'), a3 = byKey(R, A, 'a3');
    assert.deepEqual([a1, a2, a3].map(r => r.chip.text), ['GF Sheet 1', 'RG Sheet 1', 'Waiting for a sheet'], JSON.stringify(R.map(r => r.chip)));
    assert([a1, a2, a3].every(r => r.tag === 'DIV' && r.hold === 1), 'each row keeps its one orange Hold: ' + JSON.stringify(R.map(r => [r.tag, r.hold])));
    assert(a1.chip.tag === 'BUTTON' && a1.chip.aria === 'Open GF Sheet 1' && a1.chip.tab === 0 && !a1.chip.check, 'GF Sheet 1: a button, aria-label "Open GF Sheet 1", in the tab order, no check: ' + JSON.stringify(a1.chip));
    assert(a2.chip.tag === 'BUTTON' && a2.chip.aria === 'Open RG Sheet 1' && a2.chip.check, 'RG Sheet 1: a button with its small check (its set is sent to the laser): ' + JSON.stringify(a2.chip));
    assert(a3.chip.tag === 'SPAN' && a3.chip.cls.includes('wait') && a3.chip.cls.includes('off') && a3.chip.border === 'dashed' && a3.chip.aria === null, 'Waiting for a sheet: said, not pressed, hollow and dashed: ' + JSON.stringify(a3.chip));
    for (const r of [a1, a2, a3]) { plain(r.chip, r.chip.text); assert(r.chip.radius.endsWith('px') && parseFloat(r.chip.radius) >= 12, 'a pill'); }
    assert.equal(a1.chip.dot, a1.chip.rowDot, 'GF Sheet 1: its dot is the gold of the piece: ' + a1.chip.dot + ' vs ' + a1.chip.rowDot);
    assert.equal(a2.chip.dot, a2.chip.rowDot, 'RG Sheet 1: its dot is the rose of the piece');
    assert.notEqual(a1.chip.dot, a2.chip.dot, 'two metals, two dots');
    assert.equal(a3.chip.dot, 'rgba(0, 0, 0, 0)', 'waiting: a hollow dot');
    await nothing('open, all pieces'); assert.equal(await page.evaluate(() => document.querySelectorAll('#owPcSum button button').length), 0, 'no button inside a button');
    // the dots, the Hold and the name are still there beside it
    assert.equal(await page.evaluate(() => [...document.querySelectorAll('#owPcSum .owPcRow')].every(r => r.querySelectorAll('.steps i').length >= 4 && r.querySelector('.owPcName'))), true, 'name and dots stay');

    // the keyboard: Tab reaches the chip (a focus ring), Enter opens its sheet
    await page.evaluate(() => document.querySelector('#owPcSum .owPcRow .owPcName').focus());
    let seen = []; for (let i = 0; i < 6 && !seen.includes('chip'); i++) { await page.keyboard.press('Tab'); seen.push(await page.evaluate(() => document.activeElement.classList.contains('pcSt') ? 'chip' : document.activeElement.className.split(' ')[0])); }
    assert(seen.includes('chip'), 'Tab reaches the chip: ' + seen);
    const ring = await page.evaluate(() => { const c = getComputedStyle(document.activeElement); return { w: c.outlineWidth, s: c.outlineStyle, k: document.activeElement.getAttribute('aria-label') }; });
    assert(ring.s === 'solid' && ring.w === '2px', 'a visible focus ring: ' + JSON.stringify(ring)); assert.equal(ring.k, 'Open GF Sheet 1');
    await page.keyboard.press('Enter');
    await page.waitForFunction(g => OrderWin.view() === 'sheet' && OrderWin._sheet() && OrderWin._sheet().sheet.id === g && document.getElementById('owPlateWait').hidden, GF1, { timeout: 20000 });
    assert.equal(await page.evaluate(() => document.querySelectorAll('dialog[open]').length), 1, 'no pop-up over the pop-up');
    assert.equal(await page.evaluate(() => OrderWin._scope().piece), null, 'the row was not pressed with the chip: all pieces still shown');
    await page.evaluate(() => OrderWin.setView('info'));
    await page.waitForFunction(() => document.querySelectorAll('#owPcSum .pcSt').length === 3);
    // a press with the pointer opens the other sheet
    await page.click(`#owPcSum [data-piece="${kOf(A, 'a2')}"] .pcSt`);
    await page.waitForFunction(g => OrderWin.view() === 'sheet' && OrderWin._sheet() && OrderWin._sheet().sheet.id === g && document.getElementById('owPlateWait').hidden, RG1, { timeout: 20000 });
    assert.equal(await page.evaluate(() => OrderWin._scope().piece), null, 'a press on the chip is not a press on the row');
    await page.evaluate(() => OrderWin.setView('info'));
    await page.waitForFunction(() => document.querySelectorAll('#owPcSum .pcSt').length === 3);

    // hover and focus: a tint fading in and a lift, nothing else moves
    await page.hover(`#owPcSum [data-piece="${kOf(A, 'a1')}"] .pcSt`); await page.waitForTimeout(400);
    const hov = await page.evaluate(k => { const c = document.querySelector(`#owPcSum [data-piece="${k}"] .pcSt`), s = getComputedStyle(c), b = getComputedStyle(c, '::before'); return { tr: s.transitionProperty, trB: b.transitionProperty, op: b.opacity, ty: new DOMMatrix(s.transform).m42, shadow: s.boxShadow }; }, kOf(A, 'a1'));
    assert.equal(hov.tr, 'transform', 'only the lift animates: ' + hov.tr); assert.equal(hov.trB, 'opacity', 'and the tint fades: ' + hov.trB);
    assert.equal(hov.op, '1', 'the tint is in on hover'); assert(hov.ty < 0 && hov.ty >= -1.5, 'a 1px lift: ' + hov.ty); assert.equal(hov.shadow, 'none');
    if (shots) { await page.evaluate(() => document.getElementById('owPcSum').scrollIntoView({ block: 'center' })); await page.hover(`#owPcSum [data-piece="${kOf(A, 'a1')}"] .pcSt`); await shot('p3-5-chip-hover-1440'); }
    await page.mouse.move(5, 5);

    // ── 2 · widths: the chip fits beside the name and the Hold, or under the name; nothing clipped, no sideways scroll ──
    const fit = async (what, sizes, name) => {
      for (const [w, h] of sizes) {
        await page.setViewportSize({ width: w, height: h }); await page.waitForTimeout(500);
        const g = await page.evaluate(() => {
          const sum = document.getElementById('owPcSum'), sr = sum.getBoundingClientRect(), out = { over: sum.scrollWidth - sum.clientWidth, w: sr.width, rows: [] };
          for (const r of sum.querySelectorAll('.owPcRow')) {
            const c = r.querySelector('.pcSt'), rr = r.getBoundingClientRect(), cr = c.getBoundingClientRect(), nm = r.querySelector('.owPcName, .nm'), h = r.querySelector('.holdBtn'), d = r.querySelector('.steps');
            const box = n => { if (!n) return null; const b = n.getBoundingClientRect(); return { l: b.left, r: b.right, t: b.top, b: b.bottom }; };
            out.rows.push({ text: c.textContent.trim(), row: { l: rr.left, r: rr.right }, chip: { l: cr.left, r: cr.right, t: cr.top, b: cr.bottom }, nm: box(nm), hold: box(h), dots: box(d), clipped: c.scrollWidth > c.clientWidth + 1, h: cr.height });
          }
          return out;
        });
        const hit = (p, q) => p && q && p.l < q.r - 0.5 && p.r > q.l + 0.5 && p.t < q.b - 0.5 && p.b > q.t + 0.5;
        assert(g.over <= 1, `${what} ${w}px: the list does not scroll sideways (${g.over})`);
        for (const r of g.rows) {
          assert(r.chip.l >= r.row.l - 1 && r.chip.r <= r.row.r + 1, `${what} ${w}px: "${r.text}" is inside its row: ${JSON.stringify(r)}`); assert(!r.clipped, `${what} ${w}px: "${r.text}" is not clipped`);
          assert(!hit(r.chip, r.nm) && !hit(r.chip, r.hold) && !hit(r.chip, r.dots), `${what} ${w}px: "${r.text}" touches nothing beside it: ${JSON.stringify(r)}`);
          assert(r.h >= 20 && r.h <= 30, `${what} ${w}px: a calm height (${r.h})`);
        }
        console.log(`    ${what} ${w}px: list ${Math.round(g.w)}px, chips ` + g.rows.map(r => `"${r.text}" ${Math.round(r.chip.r - r.chip.l)}px ${r.nm && r.chip.t >= r.nm.b - 4 ? 'under the name' : 'beside it'}`).join('; '));
        if (shots && name) { await page.evaluate(() => document.getElementById('owPcSum').scrollIntoView({ block: 'center' })); await shot(`${name}-${w}`); }
      }
    };
    await fit('states A', [[1440, 900], [900, 800], [390, 844]], 'p3-1-on-sheets-waiting');
    await page.setViewportSize({ width: 1440, height: 900 }); await page.waitForTimeout(300);
    await closeWin();

    // ── 3 · Bea Beta's order is on hold: no Hold button, plain rows, the chip inside the row's own button (no button in a button) ──
    await openWin(B, 2);
    R = await rows();
    const b1 = byKey(R, B, 'b1'), b2 = byKey(R, B, 'b2');
    assert.deepEqual([b1, b2].map(r => r.chip && r.chip.text), ['On hold', 'On hold'], JSON.stringify(R.map(r => r.chip)));
    assert([b1, b2].every(r => r.tag === 'BUTTON' && r.hold === 0 && r.chip.tag === 'SPAN'), 'plain rows: the row is the button, its chip only says: ' + JSON.stringify(R.map(r => [r.tag, r.chip.tag])));
    assert(b1.chip.cls.includes('hold') && /^rgb\(162, 89, 28\)$/.test(b1.chip.color), 'On hold takes the Hold button\'s orange: ' + b1.chip.color + ' ' + b1.chip.cls);
    assert(b1.chip.dot === 'rgb(162, 89, 28)', 'and its dot: ' + b1.chip.dot);
    for (const r of [b1, b2]) plain(r.chip, r.chip.text);
    assert.equal(await page.evaluate(() => document.querySelectorAll('#owPcSum button button').length), 0, 'no button inside a button');
    await nothing('on hold');
    await fit('states B', [[1440, 900], [390, 844]], 'p3-2-on-hold');
    await page.setViewportSize({ width: 1440, height: 900 }); await page.waitForTimeout(300);
    await closeWin();

    // ── 3b · Eve Epsilon's order: one piece cancelled (hollow, dashed), the other on its sheet ──
    await openWin(E, 2);
    R = await rows();
    const e1 = byKey(R, E, 'e1'), e2 = byKey(R, E, 'e2');
    assert.deepEqual([e1, e2].map(r => r.chip && r.chip.text), ['Cancelled', 'GF Sheet 1'], JSON.stringify(R.map(r => r.chip)));
    assert(e1.chip.tag === 'SPAN' && e1.chip.cls.includes('cancel') && e1.chip.border === 'dashed' && e1.chip.dot === 'rgba(0, 0, 0, 0)' && e1.chip.aria === null, 'Cancelled: said, hollow and dashed: ' + JSON.stringify(e1.chip));
    for (const r of [e1, e2]) plain(r.chip, r.chip.text);
    await nothing('cancelled');
    await fit('states E', [[1440, 900], [390, 844]], 'p3-3-cancelled');
    await page.setViewportSize({ width: 1440, height: 900 }); await page.waitForTimeout(300);
    await closeWin();

    // ── 4 · Cy Gamma's order: cut (with its check), sorted, shipped, completed by hand ──
    await openWin(C, 4);
    R = await rows();
    const [c1, c2, c3, c4] = ['c1', 'c2', 'c3', 'c4'].map(t => byKey(R, C, t));
    assert.deepEqual([c1, c2, c3, c4].map(r => r.chip.text), ['GF Sheet 1', 'Sorted', 'Shipped', 'Completed'], JSON.stringify(R.map(r => r.chip)));
    assert(c1.chip.tag === 'BUTTON' && c1.chip.check && c1.chip.aria === 'Open GF Sheet 1', 'a sheet that is cut reads the same, with its small check: ' + JSON.stringify(c1.chip));
    assert(c2.chip.tag === 'SPAN' && !c2.chip.check && c2.chip.cls.includes('stage') && c3.chip.cls.includes('stage'), 'Sorted and Shipped only say where the piece is');
    assert(c4.chip.cls.includes('done') && c4.chip.color === 'rgb(25, 102, 63)', 'Completed takes the Completed button\'s green: ' + JSON.stringify(c4.chip));
    for (const r of [c1, c2, c3, c4]) plain(r.chip, r.chip.text);
    await nothing('cut, sorted, shipped, completed');
    await fit('states C', [[1440, 900], [390, 844]], 'p3-4-cut-sorted-shipped-completed');
    await page.setViewportSize({ width: 1440, height: 900 }); await page.waitForTimeout(300);
    // the same chip, one piece's row alone (a piece picked, or a single piece): the words stay for a screen reader only, no chip to press twice
    await closeWin();
    await openWin(D, 1);
    R = await rows();
    assert(R.length === 1 && !R[0].chip && /^On GF Sheet 1$/.test(R[0].hidden || ''), 'a single piece: the Now card says where it is, its row keeps the words for a screen reader only: ' + JSON.stringify(R[0]));
    await nothing('single piece');
    // (the Timeline of the same order: the Now line is its own element)
    await closeWin();

    // ── 5 · the mutant: the old words are brought back, and the check catches them ──
    const mutated = src.replace('<span>${esc(s.text)}</span>', '<span>${esc(s.text)} · next: Laser cut</span>');
    assert.notEqual(mutated, src, 'the mutant changed the builder');
    assert.deepEqual(errors, [], 'no page errors'); await page.close();
    const page2 = await context.newPage(), errors2 = [];
    await page2.route(u => /\/charm-nest-bridge\.js(\?|$)/.test(u.pathname + u.search), r => r.fulfill(js(mutated)));
    await seedPage(page2, errors2);
    await page2.evaluate(k => OrderWin.open(k), kOf(A, 'a1'));
    await page2.waitForFunction(() => document.querySelectorAll('#owPcSum .pcSt').length === 3, null, { timeout: 20000 }).catch(async e => { throw new Error('the mutant page drew no chips: ' + JSON.stringify(await page2.evaluate(() => ({ open: OrderWin.isOpen(), rid: OrderWin.rid(), chips: [...document.querySelectorAll('#owPcSum .pcSt')].map(c => c.textContent), rows: document.querySelectorAll('#owPcSum .owPcRow').length })))); });
    const caught = await page2.evaluate(NO_NEXT);
    assert(caught.length >= 3 && caught.every(t => /next\s*:\s*Laser cut/.test(t)), 'the mutant that brings "next: ..." back is caught: ' + JSON.stringify(caught));
    await page2.close();

    assert.deepEqual(outside, [], 'no Etsy call');
    assert.deepEqual(errors2, [], 'no page errors, mutant page');
    console.log('  ✓ a piece\'s row says where it is in one calm chip: GF Sheet 1 (a button that opens that sheet, with a check once cut or sent), Waiting for a sheet, On hold, Cancelled, Sorted, Shipped, Completed; no "next: ..." anywhere; fits 1440, 900 and 390; a mutant is caught');
  } finally { await browser.close(); srv.close(); }
}
main().then(() => console.log('Piece status chip OK')).catch(e => { console.error(e); process.exit(1); });
