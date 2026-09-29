// The Timeline tab scrolls as one page (Paul, 29 Sep: "the scrolling only scrolls the tiny bit of the UI at the bottom ... please
// ensure that the progress chart also scrolls so that the entire page is scrolling"). On a short window the chart used to stay
// put and only the details panel under it scrolled, a sliver too small to read. Now the order window's Timeline view is the one
// scroller: the chart and the details keep their own height and move together (the wheel over the chart scrolls the page too),
// the last line of the details can be brought fully into view, the chart's own sideways scroll stays, the top of the chart shows
// at scroll 0, and nothing scrolls sideways. Checked at a short laptop window, a small window and a phone. The Overview tab and the
// header's rail (the compact timeline) are left as they were. Runs in headless Chromium against the local fake site.
//   SHOTS=<dir> node tests/charm-nest/timeline-page-scroll.cjs [playwright-core dir]
const fs = require('fs'), path = require('path'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');
const { start } = require('./bridge-server.cjs');

const DAY = 86400, SHIP = Math.floor(Date.UTC(2026, 9, 2, 17) / 1000);
const RID = '4176744752', KEY = '4176744752_41767447521';
const ORDERS = [{ receiptId: RID, orderNumber: RID, createTs: SHIP - 5 * DAY, updateTs: SHIP - 5 * DAY + 60, shipBy: SHIP, buyer: { name: 'Buyer 4752' }, buyerMessage: '', isGift: false, giftMessage: '', staffNote: '', messages: [],
  lines: [{ transactionId: '41767447521', listingId: '1800047521', sku: 'CHAIN_8941', title: 'CHAIN REPLACEMENT', quantity: 1, expectedShipDate: SHIP, variations: [{ name: 'Metal', value: 'Gold' }, { name: 'Length', value: '17 Inches' }], metalKey: '', metalLabel: '', personalization: '' }] }];

(async () => {
  const pwDir = process.argv[2] || process.env.PW_DIR || path.join(root, 'node_modules');
  let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { console.log('  – no playwright-core: not run'); return; }
  const shots = process.env.SHOTS || ''; if (shots) fs.mkdirSync(shots, { recursive: true });
  const srv = await start({ receipts: [] });
  // the order's record, completed: a QR label printed, then Complete Order, so the details have a stamp, a line and the neighbours
  const t0 = Date.now() - 3 * 3600e3;
  srv.st.put('Charm_Custom_Orders', KEY, { key: KEY, receiptId: RID, transactionId: '41767447521', sku: 'CHAIN_8941', title: 'CHAIN REPLACEMENT', category: 'Rework', kind: '', state: 'completed', how: 'print',
    completedAt: t0, completedBy: 'Paul', printedAt: t0, printedBy: 'Paul', lastPrintedAt: t0, lastPrintedBy: 'Paul', prints: 1, hasLabel: true, updatedAtMs: t0 + 60e3,
    stamps: [{ how: 'print', at: t0, by: 'Paul' }, { how: 'button', at: t0 + 60e3, by: 'Paul' }] }, false);
  // the order came in earlier (its first step, "Order placed on Etsy"), and the two presses left their events
  srv.st.put('Charm_Nest_Arrivals', RID, { id: RID, firstSeenAt: t0 - 26 * 3600e3, createTs: Math.floor((t0 - 26 * 3600e3) / 1000) });
  const rec = (type, at, extra) => srv.st.put('Order_Timeline', `${RID}~${type}~${KEY}-${at}`, Object.assign({ orderId: RID, type, at, by: 'Paul', source: 'sorter', station: 'sorter', device: '', transactionId: '41767447521', sheetId: '', sheet: '', setId: '', milestone: false, lineKey: KEY }, extra));
  rec('sealPrinted', t0, { text: 'CHAIN_8941 · print 1', data: { how: 'print', prints: 1, completed: false, sku: 'CHAIN_8941' } });
  rec('sealCompleted', t0 + 60e3, { text: 'CHAIN_8941 · Complete Order', data: { how: 'button', prints: 1, completed: true, sku: 'CHAIN_8941' } });
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  try {
    const context = await browser.newContext({ viewport: { width: 1280, height: 520 }, deviceScaleFactor: 1 });
    await context.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => /fonts\.googleapis|fonts\.gstatic/.test(r.request().url()) ? r.fulfill({ status: 200, contentType: 'text/css', body: '' }) : r.abort());
    await context.addInitScript(() => { try { localStorage.setItem('cn.settings', JSON.stringify({ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on' })); localStorage.setItem('cn.employee', 'Test Operator'); } catch (_) {} });
    const page = await context.newPage(), errors = [];
    page.setDefaultTimeout(30000);
    page.on('pageerror', e => { errors.push(e.message); console.error('page error:', e.message); });
    await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
    await page.waitForFunction(() => window.CN && window.Orders && window.Review && window.OrderWin && window.OrderTimelineUI && CN.S.cloud.ok === true, null, { timeout: 60000 });
    await page.evaluate(async orders => {
      for (const order of orders) for (const line of order.lines) { const key = CharmNestOrders.lineKey(order, line); const row = { key, order, line, arrivedAt: Date.now(), spec: null, problems: [], state: 'pulled', reason: null, claimedBy: null, poolIds: [], engrave: null, material: null }; B.orders.rows.push(row); B.orders.byKey.set(key, row); }
      Orders.interpretAll(); await Orders.loadMaps(true); Orders.interpretAll(); Review.syncOrderItems(); CN.setMode('review'); Review.render();
    }, ORDERS);

    await page.evaluate(k => OrderWin.open(k), KEY);
    await page.evaluate(() => OrderWin.setView('timeline'));
    await page.waitForFunction(() => document.querySelectorAll("#owTimeline .tlSt[data-key]").length >= 2 && document.querySelector("#owTimeline .tlDetail h3"), null, { timeout: 20000 });
    await page.waitForTimeout(1200);   // (the entrance animations)

    // where things are now: the page (the Timeline view), the chart, and the last thing the details say
    const look = () => page.evaluate(() => {
      const v = document.querySelector('#orderWin .owVTime'), g = document.querySelector('#owTimeline .tlGrid'), d = document.querySelector('#owTimeline .tlDetail'), sc = document.querySelector('#owTimeline .tlScroll');
      const vr = v.getBoundingClientRect(), gr = g.getBoundingClientRect(), dr = d.getBoundingClientRect();
      let last = 0; for (const n of d.querySelectorAll('*')) { const r = n.getBoundingClientRect(); if (r.width && r.height && getComputedStyle(n).visibility !== 'hidden') last = Math.max(last, r.bottom); }
      const rail = document.querySelector('#owRail .tlUI.compact');
      return { vTop: vr.top, vBottom: vr.bottom, vh: v.clientHeight, sh: v.scrollHeight, st: v.scrollTop, sw: v.scrollWidth, cw: v.clientWidth, gTop: gr.top, gBottom: gr.bottom, gh: gr.height, dTop: dr.top, dBottom: dr.bottom, dh: dr.height, last,
        detailOverflow: getComputedStyle(d).overflowY, chartOverflowX: getComputedStyle(sc).overflowX, scrollerOwn: [...document.querySelectorAll('#owTimeline *')].filter(n => n !== sc && /(auto|scroll)/.test(getComputedStyle(n).overflowY) && n.scrollHeight > n.clientHeight + 1).length,
        rail: rail ? Math.round(rail.getBoundingClientRect().height) : 0, inner: innerHeight };
    });
    const sizes = [[1280, 520], [800, 600], [390, 700]];
    for (const [w, h] of sizes) {
      await page.setViewportSize({ width: w, height: h }); await page.waitForTimeout(500);
      const tag = `${w}x${h}`;
      await page.evaluate(() => { document.querySelector('#orderWin .owVTime').scrollTop = 0; });
      const a = await look();
      // scroll 0: the top of the chart is on screen, and the page is longer than the window (it scrolls as a whole)
      assert(a.gTop >= a.vTop - 0.5 && a.gTop < a.vBottom, `${tag}: the top of the chart shows at scroll 0: ` + JSON.stringify(a));
      assert(a.sh > a.vh + 40, `${tag}: the Timeline page is longer than the window and scrolls: ` + JSON.stringify(a));
      assert.equal(a.st, 0, `${tag}: it starts at the top`);
      assert(a.dh > 150, `${tag}: the details keep their own height, not a sliver: ${a.dh}`);
      assert.equal(a.detailOverflow, 'visible', `${tag}: the details do not scroll on their own`);
      assert.equal(a.scrollerOwn, 0, `${tag}: no inner panel scrolls up and down on its own`);
      assert.equal(a.chartOverflowX, 'auto', `${tag}: the chart keeps its own sideways scroll`);
      assert(a.sw <= a.cw + 1, `${tag}: nothing scrolls sideways: ${a.sw} > ${a.cw}`);
      assert(a.cw <= w + 1, `${tag}: the page is no wider than the screen (a phone shows all of it): ${a.cw}`);
      if (w > 900) assert(a.rail > 10, `${tag}: the header's compact rail is still drawn: ${a.rail}`);   // (under 900 px the page hides it, as before)
      if (shots) await page.screenshot({ path: path.join(shots, `${tag}-top.png`) });
      // the wheel over the chart scrolls the whole page, not the chart alone
      const box = await page.evaluate(() => { const r = document.querySelector('#owTimeline .tlScroll').getBoundingClientRect(); return { x: r.left + r.width / 2, y: Math.min(r.top + 60, innerHeight - 30) }; });
      await page.mouse.move(box.x, box.y); await page.mouse.wheel(0, 240); await page.waitForTimeout(400);
      const b = await look();
      assert(b.st > 100, `${tag}: the wheel over the chart scrolled the page: ${b.st}`);
      assert(b.gTop < a.gTop - 100 && b.dTop < a.dTop - 100, `${tag}: the chart and the details moved together: chart ${a.gTop} -> ${b.gTop}, details ${a.dTop} -> ${b.dTop}`);
      // the end: the chart's top is off the top of the page, the last line of the details is fully in view
      await page.evaluate(() => { const v = document.querySelector('#orderWin .owVTime'); v.scrollTop = v.scrollHeight; }); await page.waitForTimeout(300);
      const c = await look();
      assert(c.gTop < c.vTop, `${tag}: at the end the top of the chart has scrolled away: ${c.gTop} vs ${c.vTop}`);
      assert(c.st + c.vh >= c.sh - 1, `${tag}: reached the end`);
      assert(c.last <= c.vBottom + 0.5 && c.last <= c.inner && c.last > c.vTop, `${tag}: the last of the details ends inside the window: ${c.last} (window ${c.vBottom}/${c.inner})`);
      assert(c.dBottom <= c.vBottom + 0.5, `${tag}: the details end inside the window: ${c.dBottom} vs ${c.vBottom}`);
      if (shots) await page.screenshot({ path: path.join(shots, `${tag}-end.png`) });
      console.log(`  ✓ ${tag}: page ${a.sh}px in a ${a.vh}px window, wheel and end both move chart and details together, the last line ends ${Math.round(c.vBottom - c.last)}px above the window's foot`);
    }

    // the other tab is as it was
    await page.setViewportSize({ width: 1280, height: 520 });
    await page.evaluate(() => OrderWin.setView('info')); await page.waitForTimeout(400);
    const info = await page.evaluate(() => { const v = document.querySelector('#orderWin .owVInfo'), t = document.querySelector('#orderWin .owVTime'); return { info: getComputedStyle(v).display, h: v.getBoundingClientRect().height, time: t.hidden ? 'none' : getComputedStyle(t).display }; });
    assert.equal(info.info, 'grid'); assert(info.h > 200, 'the Overview fills the window: ' + info.h); assert.equal(info.time, 'none');
    await page.setViewportSize({ width: 390, height: 700 }); await page.waitForTimeout(400);
    const phone = await page.evaluate(() => { const v = document.querySelector('#orderWin .owVInfo'), r = v.getBoundingClientRect(); return { w: Math.round(r.width), h: Math.round(r.height), sw: v.scrollWidth, cw: v.clientWidth }; });
    assert(phone.w <= 391 && phone.h > 200, 'the Overview is as wide as a phone screen, no wider: ' + JSON.stringify(phone));
    if (shots) await page.screenshot({ path: path.join(shots, '390x700-overview.png') });

    assert.deepEqual(errors, [], 'no page errors');
    console.log('Timeline page scroll OK');
  } finally { await browser.close(); srv.close(); }
})().catch(e => { console.error(e); process.exit(1); });
