// The Timeline tab does not scroll (Paul, 5 Oct 2026: "the full chart should always be visible on one screen without scrolling"). On 29 Sep it was the
// other way round, the tab was one long page (chart and details) the order window scrolled; with the chart fitted to the room (charm-nest-timeline-ui.js,
// "the chart fits its room") a short window, a small window and a phone all show the whole chart at once: the tab does not scroll up and down, the chart does
// not scroll sideways, a wheel over the chart moves nothing, the chart is the tab's box, and the seals are drawn at the size the room allows. The Overview
// tab and the header's rail (the compact timeline) are left as they were. (timeline-fit-screen.cjs is the full proof of the fit over the six screens and the
// live re-fits; this one keeps the short-window, small-window and phone contract of the old page scroll, turned round.) Runs in headless Chromium against the
// local fake site.
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
    await page.waitForFunction(() => document.querySelectorAll("#owTimeline .tlSt[data-key]").length >= 2, null, { timeout: 20000 });
    await page.waitForTimeout(1200);   // (the entrance animations)

    // where things are now: the tab, the chart, the drawing's seals
    const look = () => page.evaluate(() => {
      const v = document.querySelector('#orderWin .owVTime'), g = document.querySelector('#owTimeline .tlGrid'), sc = document.querySelector('#owTimeline .tlScroll');
      const vr = v.getBoundingClientRect(), gr = g.getBoundingClientRect();
      const rail = document.querySelector('#owRail .tlUI.compact');
      return { vTop: vr.top, vBottom: vr.bottom, vh: v.clientHeight, sh: v.scrollHeight, st: v.scrollTop, sw: v.scrollWidth, cw: v.clientWidth, gTop: gr.top, gBottom: gr.bottom, gh: gr.height, gs: g.scrollHeight - g.clientHeight, ss: [sc.scrollWidth - sc.clientWidth, sc.scrollHeight - sc.clientHeight],
        chartOverflowX: getComputedStyle(sc).overflowX, scrollerOwn: [...document.querySelectorAll('#owTimeline *')].filter(n => /(auto|scroll)/.test(getComputedStyle(n).overflowY) && n.scrollHeight > n.clientHeight + 1).length,
        pane: document.querySelectorAll('#owTimeline .tlDetail, #owTimeline .tlBig, #owTimeline .tlArw').length, rail: rail ? Math.round(rail.getBoundingClientRect().height) : 0, inner: innerHeight, doc: document.scrollingElement.scrollHeight - innerHeight,
        seal: Math.max(...[...document.querySelectorAll('#owTimeline .tlCanvas .tlSt[data-key]')].map(b => b.offsetWidth)), lane: parseFloat(getComputedStyle(document.querySelector('#owTimeline .tlLane')).height) };
    });
    const sizes = [[1280, 520], [800, 600], [390, 700]];
    for (const [w, h] of sizes) {
      await page.setViewportSize({ width: w, height: h }); await page.waitForTimeout(700);
      const tag = `${w}x${h}`;
      const a = await look();
      // nothing is drawn under the chart (the detail pane went on 5 Oct), the tab does not scroll, and the chart is the tab's box
      assert.equal(a.pane, 0, `${tag}: no detail pane under the chart`);
      assert(a.sh <= a.vh + 1, `${tag}: the Timeline tab does not scroll up and down: ${a.sh} > ${a.vh}`);
      assert.equal(a.st, 0, `${tag}: it is at the top`);
      assert(a.sw <= a.cw + 1, `${tag}: nothing scrolls sideways: ${a.sw} > ${a.cw}`);
      assert(a.cw <= w + 1 && a.doc <= 1, `${tag}: the page is no wider or taller than the screen: ${a.cw}, ${a.doc}`);
      assert(Math.abs(a.gTop - a.vTop) <= 3 && Math.abs(a.gBottom - a.vBottom) <= 3, `${tag}: the chart is the tab's box: ${a.gTop}-${a.gBottom} in ${a.vTop}-${a.vBottom}`);
      assert(!/(auto|scroll)/.test(a.chartOverflowX), `${tag}: the chart has no sideways scroll: ${a.chartOverflowX}`);
      assert.equal(a.scrollerOwn, 0, `${tag}: no panel scrolls up and down on its own`);
      assert(a.gs <= 0 && a.ss[0] <= 0 && a.ss[1] <= 0, `${tag}: the chart shows all of itself: ${a.gs} ${a.ss}`);
      if (w > 900) assert(a.rail > 10, `${tag}: the header's compact rail is still drawn: ${a.rail}`);   // (under 900 px the page hides it, as before)
      // the seals take the room the lanes give them (the shared 84 px at most), the lanes are cut to the screen's height
      assert(a.seal >= (w < 500 ? 18 : 24) && a.seal <= 84, `${tag}: the chart's seals are drawn to the room: ${a.seal}`);
      assert(a.lane * 7 <= a.vh && a.lane * 7 >= a.vh - 140, `${tag}: seven lanes share the tab's height: ${a.lane} x 7 in ${a.vh}`);
      if (shots) await page.screenshot({ path: path.join(shots, `${tag}-tab.png`) });
      // a wheel over the chart moves nothing: the whole chart is on screen
      const box = await page.evaluate(() => { const r = document.querySelector('#owTimeline .tlScroll').getBoundingClientRect(); return { x: r.left + r.width / 2, y: Math.min(r.top + 60, innerHeight - 30) }; });
      await page.mouse.move(box.x, box.y); await page.mouse.wheel(0, 240); await page.mouse.wheel(120, 0); await page.waitForTimeout(400);
      const b = await look();
      assert.equal(b.st, 0, `${tag}: the wheel over the chart scrolled the tab: ${b.st}`);
      assert(Math.abs(b.gTop - a.gTop) < 1 && Math.abs(b.seal - a.seal) < .5, `${tag}: the chart did not move: ${a.gTop} -> ${b.gTop}`);
      console.log(`  ✓ ${tag}: the tab is ${a.vh}px and does not scroll, the chart is its box, seals ${a.seal}px on ${a.lane}px lanes, the wheel moves nothing`);
    }

    // the other tab is as it was
    await page.setViewportSize({ width: 1280, height: 520 });
    await page.mouse.move(3, 3); await page.waitForTimeout(500);   // (a seal under the pointer is zoomed, and a view switch waits for it to let go)
    await page.evaluate(() => OrderWin.setView('info')); await page.waitForTimeout(600);
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
