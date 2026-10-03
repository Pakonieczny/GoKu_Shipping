// The order view (Paul, 28 Sep, E1-F3; plans/design/order-view-spec.md): the order window as one full-screen view that
// grows out of what was clicked, with Overview · Timeline · Sheet, the order's sheet drawn with its pieces in gold, and
// orders outside the current pull opened from the records (OrderWin.openOrder, as search does).
// It opens the sorter in headless Chromium against the local fake site (bridge-server.cjs): every request that is not to
// the loopback is aborted, and the records the view reads (a saved sheet, the pool's pieces, the run that pulled an old
// order) are written into the fake store first.
//   node tests/charm-nest/order-view.cjs   (PW_DIR=<playwright node_modules>, CHROMIUM=<chrome>, CN_SHOTS=dir keeps screenshots)
const path = require('path'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');
const { start } = require('./bridge-server.cjs');

const DAY = 86400, SHIP = Math.floor(Date.UTC(2026, 9, 2, 17) / 1000);
const A = { rid: '4176208841', tid: '41762088411', sku: 'TINY_TAG' }, B2 = { rid: '4176200172', tid: '41762001721', sku: 'LEAF_CHARM' };
const C = { rid: '4175000123', tid: '41750001231', sku: 'ASTER_FLOWER' };   // an order outside the pull
const poolOf = o => `${o.rid}_${o.tid}_1`, SHEET = 'sheet-ov-1', RUN = 'run-ov-1';
const order = (o, buyer) => ({ receiptId: o.rid, orderNumber: o.rid, createTs: SHIP - 5 * DAY, updateTs: SHIP - 5 * DAY + 60, shipBy: SHIP, buyer: { name: buyer }, buyerMessage: '', isGift: false, giftMessage: '', staffNote: '', messages: [],
  lines: [{ transactionId: o.tid, listingId: '1800000' + o.tid.slice(-3), sku: o.sku, title: o.sku.replace(/_/g, ' ') + ' necklace', quantity: 1, expectedShipDate: SHIP, variations: [{ name: 'Metal', value: '14k Gold Filled' }], metalKey: 'gold', metalLabel: 'GF 14/20', personalization: ['Initial: H'], buyerMessage: '' }] });

async function main() {
  const pwDir = process.env.PW_DIR || path.join(root, 'node_modules');
  let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { console.log('  – no playwright-core: the browser checks were not run'); return; }
  const shots = process.env.CN_SHOTS || null;
  const srv = await start({ receipts: [] });
  // the records: a saved GF sheet (100 × 50 mm) holding a piece of A, a piece of C and a piece of another order; the
  // pool's pieces of A and C; the run that pulled C, with C's line kept
  const box = (id, cx, cy) => ({ id, cxPt: cx, cyPt: cy, angle: 0, wPt: 34, hPt: 34 });
  srv.st.put('Charm_Nest_Sheets', SHEET, { id: SHEET, metal: 'gold', sheetIndex: 2, day: '2026-09-27', status: 'written', density: 0.42, stock: { wPt: 283.46, hPt: 141.73 }, orders: [A.rid, C.rid, '4179999999'],
    placements: [box('pa', 60, 50), box('pc', 150, 80), box('px', 225, 45)],
    charms: [{ id: 'pa', name: `${A.rid} · ${A.sku}`, poolId: poolOf(A), order: A.rid, sku: A.sku }, { id: 'pc', name: `${C.rid} · ${C.sku}`, poolId: poolOf(C), order: C.rid, sku: C.sku }, { id: 'px', name: '4179999999 · OTHER', poolId: '4179999999_41799999991_1', order: '4179999999', sku: 'OTHER' }],
    backPool: [{ poolId: poolOf(C), order: C.rid, sku: C.sku, copy: 1, text: 'Love, Mom', approvedAt: Date.now() - DAY * 1000, approvedBy: 'Test Operator' }] });
  for (const [o, runId] of [[A, null], [C, RUN]]) srv.st.put('Charm_Pool', poolOf(o), { poolId: poolOf(o), orderId: o.rid, transactionId: o.tid, lineKey: `${o.rid}_${o.tid}`, sku: o.sku, material: 'gold', copy: 1, quantity: 1, state: 'placed', sheetId: SHEET, sheetName: 'GF_Sheet-2', runId, updatedAt: Date.now() });
  srv.st.put('Charm_Nest_Runs', RUN, { runId: RUN, day: '2026-09-20', status: 'complete', step: 'complete', updatedAt: Date.now() - 7 * DAY * 1000,
    lines: { [`${C.rid}_${C.tid}`]: { state: 'committed', poolIds: [poolOf(C)], sku: C.sku, material: 'gold', quantity: 1, orderId: C.rid, transactionId: C.tid, createTs: SHIP - 12 * DAY, arrivedAt: 0,
      snap: { title: 'Aster birth flower necklace', listingId: '1800004321', metalKey: 'gold', metalLabel: 'GF 14/20', orderNumber: C.rid, buyer: 'Janet Steptoe', shipBy: SHIP - 4 * DAY, isGift: false, vars: ['Metal␟14k Gold Filled'], pers: ['September'] } } } });
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  try {
    const context = await browser.newContext({ viewport: { width: 1440, height: 900 } });
    await context.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => {
      if (/fonts\.googleapis|fonts\.gstatic/.test(r.request().url())) return r.fulfill({ status: 200, contentType: 'text/css', body: '' });
      return r.abort();
    });
    await context.addInitScript(() => { try { if (!sessionStorage.getItem('__seeded')) { localStorage.setItem('cn.settings', JSON.stringify({ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on' })); localStorage.setItem('cn.employee', 'Test Operator'); sessionStorage.setItem('__seeded', '1'); } } catch (_) {} window.prompt = () => 'Test Operator'; });
    const page = await context.newPage(), errors = [];
    page.setDefaultTimeout(30000);
    page.on('pageerror', e => { errors.push(e.message); console.error('page error:', String(e.stack || e.message).split('\n').slice(0, 4).join(' | ')); });
    await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
    await page.waitForFunction(() => window.CN && window.Orders && window.OrderWin && window.SheetWin && SheetWin.drawOrder && CN.S.cloud.ok === true, null, { timeout: 60000 });
    await page.evaluate(async ({ orders, pools }) => {
      await Orders.loadMaps(true);
      for (const [order, poolId] of orders.map((o, i) => [o, pools[i]])) for (const line of order.lines) { const key = CharmNestOrders.lineKey(order, line); const row = { key, order, line, arrivedAt: Date.now(), spec: null, problems: [], state: poolId ? 'pooled' : 'pulled', reason: null, claimedBy: null, poolIds: poolId ? [poolId] : [], engrave: null, material: null }; B.orders.rows.push(row); B.orders.byKey.set(key, row); }
      Orders.interpretAll(); CN.setMode('orders'); Orders.render();
    }, { orders: [order(A, 'Hannah Whitford'), order(B2, 'Ava Patel')], pools: [poolOf(A), null] });
    const rowSel = `#ordItems [data-key="${A.rid}_${A.tid}"]`;
    await page.waitForSelector(rowSel);
    const settled = () => page.waitForFunction(() => { const d = document.getElementById('orderWin'); return d.open && !d.getAnimations({ subtree: true }).some(a => a.playState === 'running' && a.effect && a.effect.getTiming().iterations !== Infinity); }, null, { timeout: 5000 });

    // 1 · a row of the Orders tab opens the view, grown out of the row
    await page.click(rowSel);
    const grew = await page.evaluate(() => document.getElementById('orderWin').getAnimations().some(a => a.effect.getKeyframes().some(k => k.clipPath && /inset\(\d/.test(k.clipPath))));
    assert(grew, 'the view grows out of the row (its clip starts at the row)');
    await settled();
    const v1 = await page.evaluate(() => { const d = document.getElementById('orderWin'), r = d.getBoundingClientRect();
      return { full: [Math.round(r.width), Math.round(r.height)], cls: d.className, title: document.getElementById('owTitle').textContent, tab: document.querySelector('.owTabsV [aria-selected=true]').dataset.owView,
        info: !document.querySelector('.owVInfo').hidden, prev: !document.getElementById('owPrev').hidden, pos: document.getElementById('owPos').textContent, sub: document.getElementById('owSub').textContent,
        meta: [...document.querySelectorAll('#owMeta .m i')].map(i => i.textContent), note: document.querySelector('label[for=owNote]').textContent, tabs: [...document.querySelectorAll('#orderWin [data-ow-tab]')].map(b => [...b.children].map(c => c.textContent.trim()).filter(Boolean).join(' ')),
        skip: !document.getElementById('owSkipBox').hidden, dialogs: document.querySelectorAll('dialog[open]').length }; });
    assert.deepEqual(v1.full, [1440, 900], 'it fills the screen'); assert.match(v1.cls, /owFull/);
    assert.equal(v1.title, `Order ${A.rid}`); assert.equal(v1.tab, 'info'); assert(v1.info, 'Overview in front');
    assert(v1.prev && /^\d+ of \d+$/.test(v1.pos), 'Previous and Next walk the Orders list: ' + v1.pos); assert(v1.skip, 'Skip this Order, in the tab row');
    assert.match(v1.sub, /Hannah Whitford · 1 piece · ship by Oct/);
    assert(v1.meta.includes('Order') && v1.meta.includes('Buyer')); assert.match(v1.note, /^Order notes/);
    assert.deepEqual(v1.tabs, ['Team internal', 'Customer on Etsy']); assert.equal(v1.dialogs, 1, 'one window');
    if (shots) await page.screenshot({ path: path.join(shots, 'order-view-overview.png') });

    // 2 · the views: Timeline (the mount point, or its quiet placeholder), then Sheet
    await page.click('.owTabsV [data-ow-view="timeline"]');
    await page.waitForFunction(() => !document.querySelector('.owVTime').hidden && document.querySelector('.owVInfo').hidden && document.getElementById('owTimeline').children.length > 0);
    await page.click('.owTabsV [data-ow-view="sheet"]');
    await page.waitForFunction(() => OrderWin._sheet() && document.querySelector('#owSheetPanel .owPieces') && document.getElementById('owPlateWait').hidden && document.querySelector('.owVTime').hidden, null, { timeout: 15000 });
    // 3 · the sheet view draws the order's pieces in gold and the rest whole, with no haze
    const px = async key => page.evaluate(key => { const inf = OrderWin._sheet(), p = inf.pointOf(key), cv = document.getElementById('owSheetCv'), r = cv.getBoundingClientRect();
      if (!p) throw new Error('no point for ' + key + ': ' + JSON.stringify({ cv: [cv.width, cv.height, r.width, r.height], wrap: [cv.parentElement.clientWidth, cv.parentElement.clientHeight], pieces: inf.pieces.map(x => x.poolId) }));
      const x = Math.round((p.x - r.left) * cv.width / r.width), y = Math.round((p.y - r.top) * cv.height / r.height), d = cv.getContext('2d').getImageData(x, y, 1, 1).data; return [d[0], d[1], d[2]]; }, key);
    const sv = await page.evaluate(() => { const inf = OrderWin._sheet(); return { mine: inf.mine.map(x => x.poolId), sheet: inf.sheet.id, chips: [...document.querySelectorAll('#owSheetPanel .owShTabs button')].map(b => b.textContent.trim()), head: [...document.querySelectorAll('#owSheetPanel .fLabel')].map(l => l.textContent),
      count: document.getElementById('owShCount').textContent, full: !!document.querySelector('#owSheetPanel [data-full]') }; });
    assert.deepEqual(sv.mine, [poolOf(A)], 'the order\'s piece on the sheet'); assert.equal(sv.sheet, SHEET);
    assert.deepEqual(sv.chips, ['GF Sheet 2']); assert(sv.head.includes('This order · 1 piece'), sv.head.join(' | ')); assert.equal(sv.count, '1 sheet'); assert(sv.full, 'Open full sheet');
    const gold = await px(poolOf(A)), rest = await px('4179999999_41799999991_1');
    assert(gold[0] - gold[2] > 30, 'its piece is gold: ' + gold); assert(rest[0] - rest[2] < 24 && (gold[0] - gold[2]) - (rest[0] - rest[2]) > 14, 'the other orders\' pieces are drawn whole, not in gold: ' + rest + ' vs ' + gold);
    // hovering a charm names its order
    const pc = await page.evaluate(k => OrderWin._sheet().pointOf(k), poolOf(C));
    await page.mouse.move(pc.x, pc.y);
    await page.waitForFunction(rid => !document.getElementById('owPlateTip').hidden && document.getElementById('owPlateTip').textContent.startsWith(rid), C.rid);
    if (shots) await page.screenshot({ path: path.join(shots, 'order-view-sheet.png') });

    // 4 · close: back into the row it came from
    await page.click('#owClose');
    const shrank = await page.evaluate(() => document.getElementById('orderWin').getAnimations().some(a => a.effect.getKeyframes().some(k => k.clipPath && /inset\(\d/.test(k.clipPath))));
    assert(shrank, 'it shrinks back into the row');
    await page.waitForFunction(() => !OrderWin.isOpen() && !document.getElementById('orderWin').open, null, { timeout: 2000 });

    // 5 · an order outside the pull, as the search opens it (charm-nest-search.js: the number lit, grown out of a lifted copy
    //     of its card): a spinner line while its records are read, then its line
    const early = await page.evaluate(rid => {
      const card = document.createElement('div'); card.id = 'liftedCard'; Object.assign(card.style, { position: 'fixed', left: '520px', top: '160px', width: '400px', height: '84px', background: '#fff' }); document.body.appendChild(card);
      OrderWin.openOrder(rid, { highlight: true, from: card, q: rid, row: null });
      const l = document.getElementById('owLoading'), d = document.getElementById('orderWin');
      return { open: OrderWin.isOpen(), loading: !l.hidden && l.textContent, title: document.getElementById('owTitle').textContent, grew: d.getAnimations().some(a => a.effect.getKeyframes().some(k => k.clipPath && /inset\(160px/.test(k.clipPath))) }; }, C.rid);
    assert(early.grew, 'it grows out of the card it was opened from');
    await page.evaluate(() => document.getElementById('liftedCard').remove());
    assert(early.open, 'it opens at once'); assert.match(early.loading || '', /Looking up order 4175000123/); assert.equal(early.title, `Order ${C.rid}`);
    await page.waitForFunction(() => document.getElementById('owLoading').hidden && /Janet Steptoe/.test(document.getElementById('owSub').textContent), null, { timeout: 15000 });
    const v5 = await page.evaluate(() => ({ title: document.getElementById('owTitle').textContent, lit: !!document.querySelector('#owTitle .num.found'), prev: document.getElementById('owPrev').hidden, skip: document.getElementById('owSkipBox').hidden,
      meta: [...document.querySelectorAll('#owMeta .m')].map(m => m.querySelector('i').textContent + ': ' + m.querySelector('span').textContent), said: document.getElementById('owNotes').textContent }));
    assert.equal(v5.title, `Order ${C.rid}`); assert(v5.lit, 'the searched number is highlighted'); assert(v5.prev, 'no Previous/Next: not walking the Orders list'); assert(v5.skip, 'no Skip: not a line of the pull');
    assert(v5.meta.includes('Buyer: Janet Steptoe') && v5.meta.includes('Title: Aster birth flower necklace'), JSON.stringify(v5.meta)); assert.match(v5.said, /September/);
    // a repaint of the lists never closes a view of an order the pull does not hold
    await page.evaluate(() => { Orders.render(); OrderWin.paint(); });
    assert(await page.evaluate(() => OrderWin.isOpen()), 'still open after a repaint');
    await page.click('.owTabsV [data-ow-view="sheet"]');
    await page.waitForFunction(rid => { const i = OrderWin._sheet(); return i && i.mine.length === 1 && i.mine[0].rid === rid && document.getElementById('owPlateWait').hidden; }, C.rid, { timeout: 15000 });
    assert.match(await page.textContent('#owSheetPanel .owBackEng'), /Love, Mom/, 'its back engraving');
    // a click on another order's charm opens that order here, on the same sheet
    const pa = await page.evaluate(k => OrderWin._sheet().pointOf(k), poolOf(A));
    await page.mouse.click(pa.x, pa.y);
    await page.waitForFunction(rid => document.getElementById('owTitle').textContent === 'Order ' + rid && OrderWin.view() === 'sheet' && OrderWin._sheet() && OrderWin._sheet().mine[0].rid === rid, A.rid, { timeout: 15000 });
    assert.equal(await page.evaluate(() => !!document.querySelector('#owTitle .num.found')), false, 'another order is not lit as the searched one');
    await settled();
    if (shots) await page.screenshot({ path: path.join(shots, 'order-view-swap.png') });

    // 5b · the header number is the view's own search ("/" is handed to it while the view is open): an order found opens here
    await page.keyboard.press('/');
    await page.waitForFunction(rid => { const q = document.getElementById('owFindQ'); return q && document.activeElement === q && q.value === rid && document.getElementById('owTitle').offsetParent === null; }, A.rid);
    await page.keyboard.type(B2.rid.slice(0, 8));
    await page.waitForFunction(rid => [...document.querySelectorAll('#owFindList [role=option]')].some(o => o.textContent.includes(rid) && /Ava Patel/.test(o.textContent)), B2.rid);
    assert.equal(await page.evaluate(() => document.querySelectorAll('dialog[open]').length), 1, 'no search window over the view');
    if (shots) await page.screenshot({ path: path.join(shots, 'order-view-find.png') });
    await page.keyboard.press('Enter');
    await page.waitForFunction(rid => document.getElementById('owTitle').textContent === 'Order ' + rid && !!document.querySelector('#owTitle .num.found') && document.getElementById('owTitle').offsetParent !== null && document.getElementById('owFindList').hidden, B2.rid, { timeout: 15000 });
    // Esc in the field gives the number back and leaves the view open
    await page.evaluate(() => OrderWin.focusSearch());
    await page.keyboard.press('Escape');
    await page.waitForFunction(() => document.getElementById('owFindQ').closest('.owFind').hidden && document.getElementById('owTitle').offsetParent !== null);
    assert(await page.evaluate(() => OrderWin.isOpen()), 'Esc in the search field leaves the view open');

    // 6 · Esc closes it too
    await page.keyboard.press('Escape');
    await page.waitForFunction(() => !document.getElementById('orderWin').open, null, { timeout: 2000 });

    // 7 · an order of two pieces (Paul, 28 Sep: "convoluted and confusing especially on multipiece orders"): a switch of
    //     "All 2 pieces" and each piece; all pieces put the order where its slowest piece is, a step some pieces reached
    //     says how many; one piece shows only its own steps. The header no longer says "line 1 of 2".
    const D2 = { rid: '4174322410', t1: '41743224101', t2: '41743224102' }, k1 = `${D2.rid}_${D2.t1}`, k2 = `${D2.rid}_${D2.t2}`;
    await page.evaluate(({ D2, k1, k2, SHIP }) => {
      const T0 = Date.now() - 3 * 3600e3, ln = (tid, sku) => ({ transactionId: tid, listingId: '18000' + tid.slice(-4), sku, title: sku.replace(/_/g, ' ') + ' necklace', quantity: 1, expectedShipDate: SHIP, variations: [{ name: 'Metal', value: '14k Gold Filled' }], metalKey: 'gold', metalLabel: 'GF 14/20', personalization: [], buyerMessage: '' });
      const order = { receiptId: D2.rid, orderNumber: D2.rid, createTs: Math.floor(T0 / 1000) - 60, updateTs: Math.floor(T0 / 1000), shipBy: SHIP, buyer: { name: 'Stephanie Lopez' }, buyerMessage: '', isGift: false, giftMessage: '', staffNote: '', messages: [], lines: [ln(D2.t1, 'SHEEP_3'), ln(D2.t2, 'COW_1')] };
      for (const line of order.lines) { const key = CharmNestOrders.lineKey(order, line); const row = { key, order, line, arrivedAt: T0, spec: null, problems: [], state: 'pooled', reason: null, claimedBy: null, poolIds: [key + '_1'], engrave: null, material: null }; B.orders.rows.push(row); B.orders.byKey.set(key, row); }
      Orders.interpretAll(); Orders.render();
      const ev = (type, min, o) => Object.assign({ id: `${D2.rid}~${type}~${min}`, type, at: T0 + min * 60e3, by: 'paul', source: 'sorter' }, o || {});
      const events = [ev('arrived', 0, { source: 'etsy', by: 'Etsy' }), ev('placed', 10, { lineKey: k1, transactionId: D2.t1, sheetId: 'sh-a', sheet: 'GF Sheet 1' }), ev('placed', 12, { lineKey: k2, transactionId: D2.t2, sheetId: 'sh-b', sheet: 'GF Sheet 2' }), ev('laserDone', 40, { lineKey: k1, transactionId: D2.t1, sheetId: 'sh-a', sheet: 'GF Sheet 1' })];
      const get = OrderTimeline.get; OrderTimeline.get = (id, ...r) => String(id) === D2.rid ? Promise.resolve({ events, cancelled: null, where: null }) : get.call(OrderTimeline, id, ...r);
      OrderWin.open(k1);
    }, { D2, k1, k2, SHIP });
    await page.waitForFunction(() => { const sw = document.getElementById('owPieceSw'); return sw && !sw.hidden && sw.querySelectorAll('button').length === 3 && document.querySelector('#owRail [data-stage="laser"] .tlCnt:not([hidden])'); }, null, { timeout: 15000 });
    await settled();
    const v7 = await page.evaluate(() => ({ title: document.getElementById('owTitle').textContent, sw: [...document.querySelectorAll('#owPieceSw button')].map(b => b.textContent.trim()), on: document.querySelector('#owPieceSw button.on').textContent.trim(),
      sheet: document.querySelector('#owRail [data-stage="sheet"]').className, laser: [document.querySelector('#owRail [data-stage="laser"]').className, document.querySelector('#owRail [data-stage="laser"] .tlCnt').textContent],
      rows: [...document.querySelectorAll('#owPcSum .owPcRow')].map(r => r.className + ' | ' + r.textContent) }));
    assert.equal(v7.title, `Order ${D2.rid}`, 'no "line 1 of 2" in the header');
    assert.equal(v7.sw[0], 'All 2 pieces'); assert.match(v7.sw[1], /^SHEEP 3 · GF/); assert.match(v7.sw[2], /^COW 1 · GF/); assert.equal(v7.on, 'All 2 pieces', 'all pieces first');
    assert.match(v7.sheet, /\bd\b/, 'both pieces are on a sheet: done'); assert.doesNotMatch(v7.laser[0], /\bd\b/, 'one piece is not cut yet: the order is where its slowest piece is'); assert.equal(v7.laser[1], '1 of 2');
    assert.equal(v7.rows.length, 2, 'each piece on a row under where it is now'); assert.match(v7.rows[1], /slow/, 'the slowest piece in gold');
    if (shots) await page.screenshot({ path: path.join(shots, 'order-view-pieces-all.png') });
    // one piece: its line on the Overview, only its own steps on the rail and the Timeline
    await page.click('#owPieceSw button:nth-of-type(2)');
    await page.waitForFunction(k1 => OrderWin.key() === k1 && /\bd\b/.test(document.querySelector('#owRail [data-stage="laser"]').className) && document.querySelector('#owRail [data-stage="laser"] .tlCnt').hidden && document.getElementById('owPcSum').hidden, k1);
    await page.click('#owPieceSw button:nth-of-type(3)');
    await page.waitForFunction(k2 => OrderWin.key() === k2 && !/\bd\b/.test(document.querySelector('#owRail [data-stage="laser"]').className) && /COW/.test(document.getElementById('owSku').textContent), k2);
    await page.click('.owTabsV [data-ow-view="timeline"]');
    await page.waitForFunction(() => !document.getElementById('owPieceSw').hidden && document.querySelectorAll('#owTimeline .tlSt[data-key]').length === 2, null, { timeout: 15000 });
    if (shots) await page.screenshot({ path: path.join(shots, 'order-view-pieces-one.png') });
    await page.click('#owPieceSw button:nth-of-type(1)');
    await page.waitForFunction(() => document.querySelectorAll('#owTimeline .tlSt[data-key]').length === 4 && !!document.querySelector('#owRail [data-stage="laser"] .tlCnt:not([hidden])'));
    // a one-piece order has no switch
    await page.evaluate(k => OrderWin.open(k), `${A.rid}_${A.tid}`);
    await page.waitForFunction(rid => document.getElementById('owTitle').textContent === 'Order ' + rid && document.getElementById('owPieceSw').hidden, A.rid);
    await page.keyboard.press('Escape');
    await page.waitForFunction(() => !document.getElementById('orderWin').open, null, { timeout: 2000 });

    assert.deepEqual(errors, [], 'no page errors');
    console.log('  ✓ opened from a row (grown out of it, full screen), views, the sheet in gold, an order outside the pull, highlight, the header search, close, an order of two pieces');
  } finally { await browser.close(); srv.close(); }
}
main().then(() => console.log('Order view OK')).catch(e => { console.error(e); process.exit(1); });
