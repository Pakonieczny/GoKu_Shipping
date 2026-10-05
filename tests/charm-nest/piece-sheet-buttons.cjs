// The order window's Sheet affordances answer for ONE piece (Paul, 5 Oct 2026, point 5):
//  "When I'm on the charm that has no SKU and I click on any of the 'Sheet' buttons/options then it takes me to the sheet that
//   has the other charm ... the option to go to a sheet for a charm that is not yet nested should be greyed out and unavailable."
// Fixtures: order 4170408845 (Emily Chambers: CUTE TRICERATOPS W/ HEARTS has an unknown SKU and no sheet, HEALTH1 is on GF
// Sheet 1), and a 3-piece order (one on GF Sheet 1, one with no SKU, one on SS Sheet 1, another metal).
// Proves, from each piece's side and from "All pieces": the Sheet button, the sheet chips, the Sheet tab (and its count) and the
// panel's tabs are greyed / inert / scoped for a piece on no sheet, with one plain reason on a press and on focus; the Sheet
// view of such a piece is "Not on a sheet yet" and never another piece's sheet; counts are real sheets only; a click on a
// piece of another sheet in the panel moves the scope to that piece; every entry point that opens an order on a piece
// (the Orders list, a charm on a sheet, an order outside the pull) shows the same.
//   node tests/charm-nest/piece-sheet-buttons.cjs   (PW_DIR=<playwright node_modules>, CHROMIUM=<chrome>, SHOTS=<dir>)
const path = require('path'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');
const { start } = require('./bridge-server.cjs');

const DAY = 86400, SHIP = Math.floor(Date.UTC(2026, 9, 10, 17) / 1000);
const I5 = { rid: '4170408845', ta: '41704088451', tb: '41704088452' };              // CUTE (no sheet), HEALTH1 (GF Sheet 1)
const T3 = { rid: '4170500003', ta: '41705000031', tb: '41705000032', tc: '41705000033' };   // GF Sheet 1, no SKU, SS Sheet 1
const OT = { rid: '4170500004', ta: '41705000041' };                                      // a neighbour on GF Sheet 1
const OUT = { rid: '4170500005', ta: '41705000051', tb: '41705000052' };                  // outside the pull: one piece on SS Sheet 1, one pooled
const GF1 = 'sheet-psb-gf1', SS1 = 'sheet-psb-ss1';
const pid = (o, t) => `${o.rid}_${o[t]}_1`;
const line = (tid, sku, metalKey, metalLabel) => ({ transactionId: tid, listingId: '19000' + tid.slice(-5), sku, title: (sku || 'Charm') + ' earrings', quantity: 1, expectedShipDate: SHIP, variations: [{ name: 'Metal', value: metalLabel }], metalKey, metalLabel, personalization: [], buyerMessage: '' });
const order = (rid, buyer, lines) => ({ receiptId: rid, orderNumber: rid, createTs: SHIP - 7 * DAY, updateTs: SHIP - 7 * DAY + 60, shipBy: SHIP, buyer: { name: buyer }, buyerMessage: '', isGift: false, giftMessage: '', staffNote: '', messages: [], lines });

async function main() {
  const pwDir = process.env.PW_DIR || path.join(root, 'node_modules');
  let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { console.log('  – no playwright-core: the browser checks were not run'); return; }
  const shots = process.env.SHOTS || null; if (shots) require('fs').mkdirSync(shots, { recursive: true });
  const srv = await start({ receipts: [] });
  const box = (id, cx, cy, w = 34) => ({ id, cxPt: cx, cyPt: cy, angle: 0, wPt: w, hPt: w });
  const ch = (id, o, t, sku) => ({ id, name: `${o.rid} · ${sku}`, poolId: pid(o, t), order: o.rid, sku });
  srv.st.put('Charm_Nest_Sheets', GF1, { id: GF1, metal: 'gold', sheetIndex: 1, day: '2026-10-04', status: 'written', stock: { wPt: 300, hPt: 140 }, orders: [I5.rid, T3.rid, OT.rid],
    placements: [box('g1', 60, 60), box('g2', 130, 60), box('g3', 200, 60)], charms: [ch('g1', I5, 'tb', 'HEALTH1'), ch('g2', T3, 'ta', 'HEART_A'), ch('g3', OT, 'ta', 'OTHER_GF')] });
  srv.st.put('Charm_Nest_Sheets', SS1, { id: SS1, metal: 'silver', sheetIndex: 1, day: '2026-10-04', status: 'written', stock: { wPt: 200, hPt: 140 }, orders: [T3.rid, OUT.rid],
    placements: [box('s1', 60, 60), box('s2', 130, 60)], charms: [ch('s1', T3, 'tc', 'STAR_C'), ch('s2', OUT, 'ta', 'MOON_O')] });
  const poolRow = (o, t, sku, material, sheetId, n, metal) => srv.st.put('Charm_Pool', pid(o, t), { poolId: pid(o, t), orderId: o.rid, transactionId: o[t], lineKey: `${o.rid}_${o[t]}`, sku, material, copy: 1, quantity: 1, state: sheetId ? 'written' : 'pooled', sheetId: sheetId || null, sheetName: sheetId ? `2026-10-04_${metal}_Set-1_Sheet-${n}` : null, updatedAt: Date.now() });
  poolRow(I5, 'tb', 'HEALTH1', 'gold', GF1, 1, 'GF');
  poolRow(T3, 'ta', 'HEART_A', 'gold', GF1, 1, 'GF'); poolRow(T3, 'tc', 'STAR_C', 'silver', SS1, 1, 'SS');
  poolRow(OT, 'ta', 'OTHER_GF', 'gold', GF1, 1, 'GF');
  poolRow(OUT, 'ta', 'MOON_O', 'silver', SS1, 1, 'SS'); poolRow(OUT, 'tb', 'RING_O', 'silver', null, 0, 'SS');   // (outside the pull)
  // Emily Chambers' timeline: arrived, HEALTH1 placed on GF Sheet 1 (the CUTE piece has nothing: its SKU is in no master file)
  const evAt = Date.now(); let evN = 0;
  const ev = (type, ago, extra) => { const id = `e${++evN}`; srv.st.put('Order_Timeline', `${I5.rid}~${type}~${id}`, Object.assign({ orderId: I5.rid, type, at: evAt - ago * 60000, by: 'Test Operator', source: 'sorter', station: '', text: '', data: {} }, extra || {})); return id; };
  ev('arrived', 4000, { source: 'etsy', by: 'Etsy' });
  const healthPlaced = ev('placed', 2000, { sheet: 'GF Sheet 1', sheetId: GF1, lineKey: `${I5.rid}_${I5.tb}`, data: { poolId: pid(I5, 'tb') } });
  // the 3-piece order's timeline: arrived, HEART_A placed on GF Sheet 1, STAR_C placed on SS Sheet 1; the piece with no SKU has no step of its own
  const ev3 = (type, ago, extra) => srv.st.put('Order_Timeline', `${T3.rid}~${type}~e${++evN}`, Object.assign({ orderId: T3.rid, type, at: evAt - ago * 60000, by: 'Test Operator', source: 'sorter', station: '', text: '', data: {} }, extra || {}));
  ev3('arrived', 4000, { source: 'etsy', by: 'Etsy' });
  ev3('placed', 3000, { sheet: 'GF Sheet 1', sheetId: GF1, lineKey: `${T3.rid}_${T3.ta}`, data: { poolId: pid(T3, 'ta') } });
  ev3('placed', 2000, { sheet: 'SS Sheet 1', sheetId: SS1, lineKey: `${T3.rid}_${T3.tc}`, data: { poolId: pid(T3, 'tc') } });
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
      for (const [order, pools] of orders) order.lines.forEach((line, i) => { const key = CharmNestOrders.lineKey(order, line); const row = { key, order, line, arrivedAt: Date.now(), spec: null, problems: [], state: pools[i] ? 'pooled' : 'pulled', reason: null, claimedBy: null, poolIds: pools[i] ? [pools[i]] : [], engrave: null, material: null }; B.orders.rows.push(row); B.orders.byKey.set(key, row); });
      Orders.interpretAll();
      // the CUTE piece: held, its SKU in no master file (as Emily Chambers' order shows it)
      const cute = B.orders.rows.find(r => r.line.sku === 'CUTE TRICERATOPS W/ HEARTS'); cute.state = 'held'; cute.hold = { by: 'Test Operator', at: Date.now() }; cute.reason = 'not in any master file';
      CN.setMode('orders'); Orders.render();
    }, { orders: [
      [order(I5.rid, 'Emily Chambers', [line(I5.ta, 'CUTE TRICERATOPS W/ HEARTS', 'gold', '14k Gold Filled'), line(I5.tb, 'HEALTH1', 'gold', '14k Gold Filled')]), [null, pid(I5, 'tb')]],
      [order(T3.rid, 'Tara Three', [line(T3.ta, 'HEART_A', 'gold', '14k Gold Filled'), line(T3.tb, '', 'gold', '14k Gold Filled'), line(T3.tc, 'STAR_C', 'silver', 'Sterling Silver')]), [pid(T3, 'ta'), null, pid(T3, 'tc')]],
      [order(OT.rid, 'Olive Other', [line(OT.ta, 'OTHER_GF', 'gold', '14k Gold Filled')]), [pid(OT, 'ta')]]] });
    const keyOf = (o, t) => `${o.rid}_${o[t]}`, hasOP = await page.evaluate(() => !!window.OrderPieces);
    // (open, with the order's records read: no piece is still 'loading', and the card has been drawn for them)
    const settle = () => page.waitForFunction(() => { if (!OrderWin.isOpen()) return false; const sc = OrderWin._scope(); return sc && sc.all.every(p => !p.loading) && document.querySelectorAll('#owNowCard .owShChip').length >= sc.all.length; }, null, { timeout: 8000 });
    const closeWin = async () => { await page.evaluate(() => OrderWin.isOpen() && OrderWin.close()); await page.waitForFunction(() => !document.getElementById('orderWin').open, null, { timeout: 4000 }); };
    // what the window shows about its Sheet controls right now
    const ui = () => page.evaluate(() => {
      const q = s => document.querySelector(s), t = q('.owTabsV [data-ow-view="sheet"]'), btn = q('#owNowCard [data-go="sheet"]');
      const chip = c => ({ text: c.querySelector('b').textContent.trim(), sub: c.querySelector('b').nextElementSibling.textContent.trim(), off: c.getAttribute('aria-disabled') === 'true', why: c.dataset.why || '' });
      return { view: OrderWin.view(), piece: OrderWin._scope() && OrderWin._scope().piece,
        pills: [...document.querySelectorAll('#owPcSum .owPcRow[data-piece]')].map(b => ({ k: b.dataset.piece, on: b.classList.contains('sel'), t: b.querySelector('.owPcName').textContent.trim().slice(0, 30) })),
        tab: { off: t.getAttribute('aria-disabled') === 'true', cls: t.classList.contains('off'), why: t.dataset.why || '', count: document.getElementById('owShCount').textContent, sel: t.getAttribute('aria-selected') },
        btn: btn && { off: btn.getAttribute('aria-disabled') === 'true', why: btn.dataset.why || '', tabbable: btn.tabIndex >= 0 },
        chips: [...document.querySelectorAll('#owNowCard .owShChip')].map(chip),
        note: [...document.querySelectorAll('.mNote .mNoteT')].map(n => n.textContent) };
    });
    const pick = key => page.evaluate(k => { OrderWin.selectPiece(k); }, key);   // (the Its pieces list's own switch: the rows are on the Overview only)
    const pickAll = () => page.evaluate(() => { OrderWin.selectPiece(null); });
    const sheetPanel = () => page.evaluate(() => ({ tabs: [...document.querySelectorAll('#owSheetPanel .owShTabs button')].map(b => b.textContent.trim()), on: document.querySelector('#owSheetPanel .owShTabs button.on')?.textContent.trim() || null,
      sheet: OrderWin._sheet() && OrderWin._sheet().sheet.id, none: document.querySelector('#owPlateWrap .owPlateNone[data-none]')?.innerText.replace(/\s+/g, ' ').trim() || null, count: document.getElementById('owShCount').textContent,
      pieces: [...document.querySelectorAll('#owSheetPanel .owPieces li')].map(li => li.innerText.replace(/\s+/g, ' ').trim()), canvasHidden: document.getElementById('owSheetCv').style.visibility === 'hidden' }));
    const drawn = sheet => page.waitForFunction(s => { const i = OrderWin._sheet(); return i && i.sheet.id === s && document.getElementById('owPlateWait').hidden; }, sheet, { timeout: 20000 });
    const shot = async name => { if (shots) { await page.waitForTimeout(1200); await page.screenshot({ path: path.join(shots, name + '.png') }); } };   // (taken once the window has settled)
    const noteNow = () => page.evaluate(() => [...document.querySelectorAll('.mNote .mNoteT')].map(n => n.textContent));
    const clearNotes = () => page.evaluate(() => document.querySelectorAll('.mNote').forEach(n => n.remove()));
    // (the note before leaves with a 0.3 s fade as the next one arrives: one note at rest)
    const oneNote = () => page.waitForFunction(() => document.querySelectorAll('.mNote').length === 1, null, { timeout: 3000 });

    // ── 1 · Emily Chambers' order, opened on the CUTE piece (no SKU, on no sheet), all pieces in front ──
    await page.evaluate(k => OrderWin.open(k), keyOf(I5, 'ta'));
    await settle();
    let u = await ui();
    assert.equal(u.view, 'info'); assert.equal(u.piece, null, 'all pieces');
    assert.deepEqual(u.pills.map(p => p.t.split(' ')[0]), ['CUTE', 'HEALTH1'], JSON.stringify(u.pills)); assert(u.pills.every(p => !p.on), 'all pieces: no row marked');
    // the Sheet button answers for the piece on show (CUTE): greyed, inert, its reason in plain words
    assert(u.btn.off && /^Not on a sheet yet: its SKU is not in any master file$/.test(u.btn.why), 'Sheet button greyed with its reason: ' + JSON.stringify(u.btn));
    assert(u.tab.off && u.tab.cls && u.tab.count === '', 'the Sheet tab is greyed too and counts nothing: ' + JSON.stringify(u.tab));
    // each piece's own sheet, separately: HEALTH1's chip is open, CUTE's is muted and inert
    assert.deepEqual(u.chips.map(c => [c.text, c.sub, c.off]), [['Not on a sheet yet', 'CUTE TRICERATOPS W/ HEARTS', true], ['GF Sheet 1', 'HEALTH1', false]], JSON.stringify(u.chips));
    assert(/^Not on a sheet yet: its SKU is not in any master file$/.test(u.chips[0].why), u.chips[0].why);
    await shot('1-overview-all-opened-on-cute');
    // pressing the greyed Sheet button, its chip and the Sheet tab: nothing opens; the reason is said, once
    await page.click('#owNowCard [data-go="sheet"]', { force: true });
    u = await ui(); assert.equal(u.view, 'info', 'the Sheet button went nowhere'); assert.equal(u.note.length, 1); assert.match(u.note[0], /^Not on a sheet yet: its SKU is not in any master file$/);
    await shot('2-reason-on-press');
    await page.click('#owNowCard .owShChip.off', { force: true });
    await oneNote(); u = await ui(); assert.equal(u.view, 'info'); assert.equal(u.note.length, 1, 'one note at a time: ' + JSON.stringify(u.note));
    await page.click('.owTabsV [data-ow-view="sheet"]', { force: true });
    await oneNote(); u = await ui(); assert.equal(u.view, 'info', 'the Sheet tab of a piece on no sheet is not opened'); assert.equal(u.tab.sel, 'false'); assert.equal(u.note.length, 1);
    await clearNotes();
    // the keyboard reaches the greyed tab (arrow keys), says why on focus, and does not open it
    await page.focus('.owTabsV [data-ow-view="info"]'); await page.keyboard.press('ArrowRight'); await page.keyboard.press('ArrowRight');
    u = await ui(); assert.equal(u.view, 'timeline', 'only the Timeline opened'); assert.equal(await page.evaluate(() => document.activeElement.dataset.owView), 'sheet', 'focus is on the greyed tab'); assert.match(u.note.join('|'), /not on a sheet yet/i);
    await page.keyboard.press('Enter'); u = await ui(); assert.equal(u.view, 'timeline', 'Enter on the greyed tab opens nothing');
    await page.evaluate(() => OrderWin.setView('info')); await clearNotes();
    // HEALTH1's chip is open: it takes to GF Sheet 1 with HEALTH1's charm chosen
    await page.click('#owNowCard .owShChip:not(.off)');
    await drawn(GF1);
    let sp = await sheetPanel();
    assert.equal(sp.sheet, GF1); assert.deepEqual(sp.tabs, ['GF Sheet 1']);
    assert(sp.pieces.some(t => /HEALTH1.*this sheet/.test(t)), JSON.stringify(sp.pieces));
    // (the panel's own list of pieces is OrderPieces' to draw: with it, the piece on no sheet is listed as such)
    if (hasOP) assert(sp.pieces.some(t => /CUTE.*not on a sheet yet/.test(t)), 'the other piece is told to be on no sheet: ' + JSON.stringify(sp.pieces));
    assert.equal(await page.evaluate(() => document.querySelector('#owSheetPanel .owCharm b').textContent), 'HEALTH1', 'the charm shown is HEALTH1\'s');
    await closeWin();

    // ── 2 · the CUTE piece picked: everything is its own ──
    await page.evaluate(k => OrderWin.open(k), keyOf(I5, 'ta')); await settle();
    await pick(keyOf(I5, 'ta'));
    u = await ui();
    assert.equal(u.piece, keyOf(I5, 'ta'));
    assert(u.btn.off && /^This piece is not on a sheet yet: its SKU is not in any master file$/.test(u.btn.why), JSON.stringify(u.btn));
    assert.deepEqual(u.chips.map(c => [c.text, c.sub, c.off]), [['Not on a sheet yet', 'Its SKU is not in any master file', true]], 'one muted chip, no sheet of the other piece: ' + JSON.stringify(u.chips));
    assert(u.tab.off && u.tab.count === '', JSON.stringify(u.tab));
    await shot('3-overview-cute-picked');
    await page.click('#owNowCard [data-go="sheet"]', { force: true }); u = await ui(); assert.equal(u.view, 'info'); assert.match(u.note.join('|'), /^This piece is not on a sheet yet: its SKU is not in any master file$/); await clearNotes();
    // reached by another way (the view asked for directly): "Not on a sheet yet", the reason, and NOT the other piece's sheet
    await page.evaluate(() => OrderWin.setView('sheet'));
    await page.waitForFunction(() => document.querySelector('#owPlateWrap .owPlateNone[data-none]'), null, { timeout: 8000 });
    sp = await sheetPanel();
    assert.match(sp.none, /^Not on a sheet yet Its SKU is not in any master file\. CUTE TRICERATOPS W\/ HEARTS/, sp.none); assert.equal(sp.sheet, null, 'no sheet drawn'); assert.deepEqual(sp.tabs, [], 'no sheet tab'); assert(sp.canvasHidden, 'the plate is not drawn');
    assert.equal(sp.count, '', 'the count counts real sheets only'); assert.equal(await page.evaluate(() => OrderWin._sheet() === null), true);
    await shot('4-sheet-tab-empty-state');
    // (still on the Sheet view: its tab is the selected one, never greyed)
    u = await ui(); assert(!u.tab.off && u.tab.sel === 'true');
    await closeWin();
    // opened on the Sheet view directly, on the CUTE row (the Orders list, Review and the sheet window all open like this)
    await page.evaluate(k => OrderWin.open(k, { view: 'sheet' }), keyOf(I5, 'ta'));
    await drawn(GF1); sp = await sheetPanel();
    assert.deepEqual(sp.tabs, ['GF Sheet 1'], 'all pieces: the sheets of the pieces that have one, each its own'); assert.equal(sp.none, null);
    if (hasOP) assert(sp.pieces.some(t => /CUTE.*not on a sheet yet/.test(t)));
    await closeWin();

    // ── 3 · the HEALTH1 piece picked: its sheet, counted once ──
    await page.evaluate(k => OrderWin.open(k), keyOf(I5, 'ta')); await settle();
    await pick(keyOf(I5, 'tb'));
    u = await ui();
    assert(!u.btn.off && !u.tab.off && u.tab.count === '1 sheet', JSON.stringify([u.btn, u.tab]));
    assert.deepEqual(u.chips.map(c => [c.text, c.sub, c.off]), [['GF Sheet 1', 'open it', false]]);
    await page.click('#owNowCard [data-go="sheet"]'); await drawn(GF1);
    sp = await sheetPanel(); assert.deepEqual(sp.tabs, ['GF Sheet 1']); assert.equal(sp.count, '1 sheet');
    await closeWin();

    // ── 4 · a 3-piece order: GF Sheet 1, no SKU, SS Sheet 1 — from each piece's side ──
    const kA = keyOf(T3, 'ta'), kB = keyOf(T3, 'tb'), kC = keyOf(T3, 'tc');
    await page.evaluate(k => OrderWin.open(k), kA); await settle();
    u = await ui();   // all pieces, opened on A (on GF Sheet 1)
    assert(!u.btn.off && !u.tab.off && u.tab.count === '2 sheets', 'all pieces: two real sheets: ' + JSON.stringify([u.btn, u.tab]));
    assert.deepEqual(u.chips.map(c => [c.text, c.sub, c.off]), [['GF Sheet 1', 'HEART A', false], ['Not on a sheet yet', 'Charm earrings', true], ['SS Sheet 1', 'STAR C', false]], JSON.stringify(u.chips));
    // three pieces: the chip row that sat beside the live line is gone; nothing in the tab row is cut
    await page.waitForTimeout(1300);   // (the window has settled: its live line and the Skip label at their full width)
    const room = await page.evaluate(() => { const t = document.querySelector('.owTabsV .owTools'); return { chipRow: !!document.getElementById('owPieceSw'), cut: [...t.querySelectorAll('*')].filter(x => x.scrollWidth > x.clientWidth + 1 && getComputedStyle(x).overflowX !== 'visible').map(x => x.textContent.trim().slice(0, 30)) }; });
    assert(!room.chipRow && room.cut.length === 0, 'no chip row, and the live line is not cut: ' + JSON.stringify(room));
    await shot('5-three-pieces-all');
    await pick(kA); u = await ui(); assert.equal(u.tab.count, '1 sheet'); assert.deepEqual(u.chips.map(c => c.text), ['GF Sheet 1']);
    await page.click('.owTabsV [data-ow-view="sheet"]'); await drawn(GF1); sp = await sheetPanel(); assert.deepEqual(sp.tabs, ['GF Sheet 1'], 'A: only its own sheet'); assert.equal(await page.evaluate(() => document.querySelector('#owSheetPanel .owCharm b').textContent), 'HEART_A');
    // from A's Sheet view, the panel's list says where the others are; a piece with no sheet is greyed and inert there
    const rows = await page.evaluate(() => [...document.querySelectorAll('#owSheetPanel .owPieces li')].map(li => ({ t: li.innerText.replace(/\s+/g, ' ').trim(), cls: li.className, dis: li.getAttribute('aria-disabled') })));
    if (hasOP) assert(rows.some(x => /not on a sheet yet/.test(x.t) && /off/.test(x.cls)), JSON.stringify(rows));
    // a click on C's row (on SS Sheet 1): the scope follows C — its own sheet only, never A's with C beside it
    await page.evaluate(() => [...document.querySelectorAll('#owSheetPanel .owPieces li')].find(li => /SS Sheet 1/.test(li.textContent)).click());
    await drawn(SS1); sp = await sheetPanel(); assert.deepEqual(sp.tabs, ['SS Sheet 1'], 'the scope followed the piece: ' + JSON.stringify(sp)); assert.equal((await ui()).piece, kC);
    assert.equal(await page.evaluate(() => document.querySelector('#owSheetPanel .owCharm b').textContent), 'STAR_C');
    // B's side (no SKU): greyed; a click does nothing; the view, asked for anyway, is the empty state
    await page.evaluate(() => OrderWin.setView('info')); await pick(kB);
    u = await ui(); assert(u.btn.off && /^This piece is not on a sheet yet: it has no SKU$/.test(u.btn.why), JSON.stringify(u.btn)); assert(u.tab.off && u.tab.count === '');
    assert.deepEqual(u.chips.map(c => [c.text, c.off]), [['Not on a sheet yet', true]]);
    await page.evaluate(() => OrderWin.setView('sheet')); await page.waitForFunction(() => document.querySelector('#owPlateWrap .owPlateNone[data-none]'), null, { timeout: 8000 });
    sp = await sheetPanel(); assert.match(sp.none, /^Not on a sheet yet It has no SKU\./); assert.equal(sp.sheet, null);
    // C's side (the other metal): its own sheet, counted once
    await page.evaluate(() => OrderWin.setView('info')); await pick(kC);
    u = await ui(); assert(!u.btn.off && u.tab.count === '1 sheet' && u.chips.length === 1 && u.chips[0].text === 'SS Sheet 1', JSON.stringify(u));
    await page.click('#owNowCard [data-go="sheet"]'); await drawn(SS1); sp = await sheetPanel(); assert.deepEqual(sp.tabs, ['SS Sheet 1']);
    await closeWin();
    // opened on B (no SKU) with all pieces in front: the Sheet button and tab answer for B (greyed); A's and C's chips stay open
    await page.evaluate(k => OrderWin.open(k), kB); await settle();
    u = await ui(); assert(u.btn.off && u.tab.off, JSON.stringify([u.btn, u.tab]));
    assert.deepEqual(u.chips.map(c => [c.text, c.off]), [['GF Sheet 1', false], ['Not on a sheet yet', true], ['SS Sheet 1', false]]);
    await page.click('#owNowCard .owShChip[data-sheet="' + SS1 + '"]'); await drawn(SS1); sp = await sheetPanel();
    assert(sp.tabs.includes('SS Sheet 1')); await closeWin();

    // ── 5 · an order outside the pull (a search), one piece on a sheet and one pooled and not placed yet ──
    await page.evaluate(rid => OrderWin.openOrder(rid, { highlight: true }), OUT.rid);
    await page.waitForFunction(() => OrderWin.isOpen() && document.getElementById('owLoading').hidden && document.querySelectorAll('#owPcSum .owPcRow[data-piece]').length === 3, null, { timeout: 20000 });
    u = await ui();
    assert.equal(u.chips.filter(c => !c.off).map(c => c.text).join(), 'SS Sheet 1'); assert.equal(u.chips.filter(c => c.off).length, 1, 'the pooled piece has its muted chip: ' + JSON.stringify(u.chips));
    const outKeys = await page.evaluate(() => OrderWin._scope().all.map(p => [p.key, p.nested, p.why]));
    const cold = outKeys.find(x => !x[1]); assert(cold && /^it is waiting to be placed$/.test(cold[2]), JSON.stringify(outKeys));
    await pick(cold[0]);
    u = await ui(); assert(u.btn.off && u.tab.off && /^This piece is not on a sheet yet: it is waiting to be placed$/.test(u.btn.why), JSON.stringify([u.btn, u.tab]));
    await closeWin();

    // ── 5b · the other doors into an order on a given piece: a Review card (its piece's row), the Library's "Open order" (no piece named) ──
    for (const [what, k] of [['Review card on the CUTE piece', 'ta'], ['Review card on the HEALTH1 piece', 'tb']]) {
      await page.evaluate(({ rid, key, pool }) => { const b = document.createElement('button'); b.id = 'psbDoor'; document.body.appendChild(b); openOrderFrom(b, rid, { row: { key }, poolId: pool }); }, { rid: I5.rid, key: keyOf(I5, k), pool: pid(I5, k) });
      await settle(); u = await ui();
      assert.equal(u.view, 'info'); assert.equal(u.piece, null, 'all pieces in front, the piece of the card shown');
      if (k === 'ta') { assert(u.btn.off && /^Not on a sheet yet: /.test(u.btn.why), what + ': ' + JSON.stringify(u.btn)); assert(u.tab.off, what); }
      else { assert(!u.btn.off && !u.tab.off && u.tab.count === '1 sheet', what + ': ' + JSON.stringify([u.btn, u.tab])); }
      assert.deepEqual(u.chips.map(c => [c.text, c.off]), [['Not on a sheet yet', true], ['GF Sheet 1', false]], what + ': ' + JSON.stringify(u.chips));
      await closeWin(); await page.evaluate(() => document.getElementById('psbDoor').remove());
    }
    await page.evaluate(rid => { const b = document.createElement('button'); b.id = 'psbDoor'; document.body.appendChild(b); openOrderFrom(b, rid); }, I5.rid);   // (the Library: the order, no piece named: its first)
    await settle(); u = await ui(); assert(u.btn.off && u.tab.off, "the Library's Open order, first piece CUTE: " + JSON.stringify([u.btn, u.tab]));
    assert.deepEqual(u.chips.map(c => [c.text, c.off]), [['Not on a sheet yet', true], ['GF Sheet 1', false]]);
    await closeWin(); await page.evaluate(() => document.getElementById('psbDoor').remove());

    // ── 6 · the Timeline's sheet links answer for one piece too ──
    // (the step's own "Open sheet", the strip's under the rail; the Timeline of the CUTE piece lists its steps alone)
    const stalePlaced = ev('placed', 3000, { sheet: 'GF Sheet 1', sheetId: GF1, lineKey: `${I5.rid}_${I5.ta}`, data: { poolId: pid(I5, 'ta') } });   // (a step of the CUTE piece, older, on a sheet it has since left)
    await page.evaluate(k => OrderWin.open(k), keyOf(I5, 'ta')); await settle();
    await page.click('.owTabsV [data-ow-view="timeline"]');
    const steps = () => page.evaluate(() => [...document.querySelectorAll('#owTimeline .tlSt[data-key]')].map(x => x.dataset.key));
    const detail = () => page.evaluate(() => { const b = document.querySelector('#owTimeline .tlDetail .tlOpenSheet'); return b ? { off: b.getAttribute('aria-disabled') === 'true', why: b.dataset.why || '', pool: b.dataset.pool } : null; });
    const strip = () => page.evaluate(() => { const s = document.querySelector('#owTimeline .tlNowS'), b = s && s.querySelector('.tlOpenSheet'); return s ? { text: s.textContent.replace(/\s+/g, ' ').trim(), btn: b ? { off: b.getAttribute('aria-disabled') === 'true', why: b.dataset.why || '' } : null } : null; });
    const open = key => page.evaluate(k => { document.querySelector(`#owTimeline .tlSt[data-key="${k}"]`).click(); }, key);
    const waitDetail = () => page.waitForFunction(() => document.querySelector('#owTimeline .tlDetail .tlOpenSheet'), null, { timeout: 8000 });
    await page.waitForFunction(() => document.querySelectorAll('#owTimeline .tlSt[data-key]').length >= 2, null, { timeout: 15000 });
    // all pieces: HEALTH1's own "placed" step opens its sheet; the CUTE piece's older one, naming a sheet it has left, is greyed and inert
    const all = await steps(); assert.deepEqual(all, ['arrived~e1', `placed~${healthPlaced}`, `placed~${stalePlaced}`].sort((x, y) => all.indexOf(x) - all.indexOf(y)), JSON.stringify(all)); assert(all.includes(`placed~${healthPlaced}`) && all.includes(`placed~${stalePlaced}`) && all.length === 3);
    await open(`placed~${healthPlaced}`); await waitDetail(); let d = await detail(); assert(d && !d.off, 'HEALTH1\'s step opens its sheet: ' + JSON.stringify(d));
    await open(`placed~${stalePlaced}`); await page.waitForFunction(() => document.querySelector('#owTimeline .tlDetail .tlOpenSheet[aria-disabled="true"]'), null, { timeout: 8000 });
    d = await detail(); assert(d.off && /^CUTE TRICERATOPS W\/ HEARTS is no longer on that sheet$/.test(d.why), JSON.stringify(d));
    await clearNotes(); await page.click('#owTimeline .tlDetail .tlOpenSheet', { force: true }); await oneNote(); u = await ui();
    assert.equal(u.view, 'timeline', 'the greyed link went nowhere'); assert.match(u.note[0], /no longer on that sheet/);
    // (the control is at the foot of the window: its note stands above it, whole on screen)
    const nr = await page.evaluate(() => { const r = document.querySelector('.mNote').getBoundingClientRect(); return { top: r.top, bottom: r.bottom, h: innerHeight }; }); assert(nr.top >= 0 && nr.bottom <= nr.h, 'the note is whole on screen: ' + JSON.stringify(nr));
    await shot('6-timeline-cute-step-link-greyed');
    // the strip (all pieces): the sheet is named for the piece that sits on it, not read as the whole order's
    const st = await strip(); assert(st && /HEALTH1 on GF Sheet 1/.test(st.text) && st.btn && !st.btn.off, JSON.stringify(st));
    // CUTE picked: its own steps alone, none of HEALTH1's; the same grey; HEALTH1's step is not there to open
    await pick(keyOf(I5, 'ta'));
    await page.waitForFunction(() => document.querySelectorAll('#owTimeline .tlSt[data-key]').length === 2, null, { timeout: 8000 });
    assert.deepEqual(await steps(), ['arrived~e1', `placed~${stalePlaced}`]);
    await open(`placed~${stalePlaced}`); await page.waitForFunction(() => document.querySelector('#owTimeline .tlDetail .tlOpenSheet[aria-disabled="true"]'), null, { timeout: 8000 });
    d = await detail(); assert(d.off && /no longer on that sheet/.test(d.why), JSON.stringify(d));
    const st2 = await strip(); assert(!st2 || !st2.btn || st2.btn.off, 'the strip of the CUTE piece does not open HEALTH1\'s sheet: ' + JSON.stringify(st2));
    // HEALTH1 picked: its own sheet, open, and it opens
    await pick(keyOf(I5, 'tb'));
    await page.waitForFunction(({ h, st }) => { const k = [...document.querySelectorAll('#owTimeline .tlSt[data-key]')].map(x => x.dataset.key); return k.includes('placed~' + h) && !k.includes('placed~' + st); }, { h: healthPlaced, st: stalePlaced }, { timeout: 8000 });
    await open(`placed~${healthPlaced}`); await waitDetail(); d = await detail(); assert(d && !d.off, JSON.stringify(d));
    await page.click('#owTimeline .tlDetail .tlOpenSheet'); await drawn(GF1); assert.equal((await ui()).view, 'sheet');
    await closeWin();

    // ── 7 · the header's rail of the 3-piece order, from each piece's side: "Nested" is marked for the pieces on a sheet only ──
    await page.evaluate(k => OrderWin.open(k), keyOf(T3, 'tb')); await settle();
    const rail = () => page.evaluate(() => [...document.querySelectorAll('#owRail .tlStop')].map(x => [x.dataset.stage || x.getAttribute('data-tl-step') || '', x.className.replace(/\btlStop\b/, '').trim(), x.innerText.replace(/\s+/g, ' ').trim()]));
    // (a step's class: d done, c the one being worked towards, f still to come)
    const stepOf = async k => (await rail()).find(x => x[0] === k)[1];
    await page.waitForFunction(() => document.querySelector('#owRail .tlStop[data-stage="arrived"].d'), null, { timeout: 15000 });
    // all pieces: the order is where its slowest piece is, so "Nested" is not done while one piece has not been placed
    assert.equal(await stepOf('sheet'), 'c', 'all pieces: Nested is not done for the order');
    // the piece with no SKU: its own steps, and none of the other pieces' sheets
    await pick(keyOf(T3, 'tb')); await page.waitForFunction(() => { const x = document.querySelector('#owRail .tlStop[data-stage="sheet"]'); return x && x.classList.contains('c'); }, null, { timeout: 8000 });
    assert.equal(await stepOf('sheet'), 'c', 'the piece with no SKU: Nested is not marked, with no sheet of another piece behind it');
    assert.doesNotMatch((await rail()).find(x => x[0] === 'sheet')[2], /OCT 2026/, 'no sheet step of another piece');
    // the pieces on a sheet: Nested is done, with their own step
    await pick(keyOf(T3, 'ta')); await page.waitForFunction(() => { const x = document.querySelector('#owRail .tlStop[data-stage="sheet"]'); return x && x.classList.contains('d'); }, null, { timeout: 8000 });
    assert.equal(await stepOf('laser'), 'c');
    await pick(keyOf(T3, 'tc')); await page.waitForFunction(() => { const x = document.querySelector('#owRail .tlStop[data-stage="sheet"]'); return x && x.classList.contains('d'); }, null, { timeout: 8000 });
    await closeWin();
    assert.deepEqual(errors, [], 'no page errors');
    console.log('  ✓ piece-scoped Sheet affordances: greyed and inert for a piece on no sheet (button, chip, tab, panel), reason on press and focus, empty state not another piece\'s sheet, counts of real sheets, scope follows a piece chosen in the panel, same from the Orders list and an order outside the pull');
  } finally { await browser.close(); srv.close(); }
}
main().then(() => console.log('Piece Sheet buttons OK')).catch(e => { console.error(e); process.exit(1); });
