// The gold line round the order's charm follows the charm (Paul, 29 Sep: "a strange outline that doesn't look very nice on
// any of the charms … before it used to pulsate, but now it just sits there … fix this so the outline matches the actual
// charm"). A real hand charm with its welded jump ring, turned on the sheet:
//  · the order view's Sheet tab: every strong gold pixel outside the charm lies the same distance out from its cut edge
//    (one thin line, shape-true; not the outline scaled about its middle, and no second line on the edge itself);
//  · it breathes (the plate changes frame to frame), and with reduced motion it stands still, still shape-true;
//  · the sheet window's charm in hand is ringed the same way.
//   node tests/charm-nest/adv-charm-outline.cjs   (PW_DIR=<playwright node_modules>, CHROMIUM=<chrome>)
const path = require('path'), fs = require('fs'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');
const { start } = require('./bridge-server.cjs');

const DAY = 86400, SHIP = Math.floor(Date.UTC(2026, 9, 2, 17) / 1000);
const O = { rid: '4179100001', tid: '41791000011' }, P1 = `${O.rid}_${O.tid}_1`, SH = 'sheet-outline-1';
const hand = JSON.parse(fs.readFileSync(path.join(__dirname, 'fixtures/middle_5903-paths.json'), 'utf8'));
const order = { receiptId: O.rid, orderNumber: O.rid, createTs: SHIP - 5 * DAY, updateTs: SHIP - 5 * DAY + 60, shipBy: SHIP, buyer: { name: 'Ola Berg' }, buyerMessage: '', isGift: false, giftMessage: '', staffNote: '', messages: [],
  lines: [{ transactionId: O.tid, listingId: '1800011', sku: 'HAND', title: 'HAND necklace', quantity: 1, expectedShipDate: SHIP, variations: [{ name: 'Metal', value: '14k Gold Filled' }], metalKey: 'gold', metalLabel: '14k Gold Filled', personalization: [], buyerMessage: '' }] };

async function main() {
  const pwDir = process.env.PW_DIR || path.join(root, 'node_modules');
  let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { console.log('  – no playwright-core: the browser checks were not run'); return; }
  const srv = await start({ receipts: [] });
  srv.st.put('Charm_Pool', P1, { poolId: P1, orderId: O.rid, transactionId: O.tid, lineKey: `${O.rid}_${O.tid}`, sku: 'HAND', material: 'gold', copy: 1, quantity: 1, state: 'written', sheetId: SH, sheetName: '2026-09-29_GF_Set-1_Sheet-1', updatedAt: Date.now() });
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  try {
    const context = await browser.newContext({ viewport: { width: 1440, height: 900 }, reducedMotion: 'reduce' });
    await context.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => /fonts\.googleapis|fonts\.gstatic/.test(r.request().url()) ? r.fulfill({ status: 200, contentType: 'text/css', body: '' }) : r.abort());
    await context.addInitScript(() => { try { if (!sessionStorage.getItem('__seeded')) { localStorage.setItem('cn.settings', JSON.stringify({ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on' })); localStorage.setItem('cn.employee', 'Test Operator'); sessionStorage.setItem('__seeded', '1'); } } catch (_) {} window.prompt = () => 'Test Operator'; });
    const page = await context.newPage(), errors = [];
    page.setDefaultTimeout(25000);
    page.on('pageerror', e => errors.push(e.message));
    await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
    await page.waitForFunction(() => window.CN && window.Orders && window.OrderWin && window.SheetWin && SheetWin.drawOrder && CN.S.cloud.ok === true, null, { timeout: 60000 });
    // the sheet as this sorter holds it: the order's hand (its jump ring welded on, as the masters are read), turned, and
    // another order's charm beside it
    const rec = await page.evaluate(async ({ order, P1, SH, hand }) => {
      await Orders.loadMaps(true);
      const line = order.lines[0], key = CharmNestOrders.lineKey(order, line), row = { key, order, line, arrivedAt: Date.now(), spec: null, problems: [], state: 'pooled', reason: null, claimedBy: null, poolIds: [P1], engrave: null, material: null };
      B.orders.rows.push(row); B.orders.byKey.set(key, row); Orders.interpretAll();
      const mk = (id, poolId, rid) => {
        const segs = JSON.parse(JSON.stringify(hand)).map((m, index) => Object.assign(m, { index })), area = b => (b[2] - b[0]) * (b[3] - b[1]);
        const outline = segs.filter(m => m.layer === 'CUT').sort((a, b) => area(b.bbox) - area(a.bbox))[0];
        const c = { id, name: `${rid} · HAND`, poolId, order: rid, outline, members: segs, bbox: [5, 5, 26.4, 44.6], strokePt: .25 };
        CharmNestPDF.integrateRings(c);
        const b = c.outline.bbox; return Object.assign(c, { centerPt: [(b[0] + b[2]) / 2, (b[1] + b[3]) / 2], widthPt: b[2] - b[0], heightPt: b[3] - b[1] });
      };
      const charms = [mk('h1', P1, order.receiptId), mk('h2', '4179100002_41791000021_1', '4179100002')];
      const placements = charms.map((c, i) => ({ id: c.id, cxPt: 70 + i * 90, cyPt: 70, angle: i ? 0 : 23, wPt: c.widthPt, hPt: c.heightPt }));
      S.sheets.gold.pages.push({ sheetId: SH, metal: 'gold', sheetIndex: 1, page: 1, status: 'complete', placements, charms, rejects: [] });
      CN.setMode('orders'); Orders.render();
      const st = stockFor('gold', S.sheets.gold.pages[S.sheets.gold.pages.length - 1]);
      return { stock: { wPt: st.wPt, hPt: st.hPt }, placements, charms: charms.map(c => ({ id: c.id, name: c.name, poolId: c.poolId, order: c.order, sku: 'HAND' })) };
    }, { order, P1, SH, hand });
    srv.st.put('Charm_Nest_Sheets', SH, Object.assign({ id: SH, metal: 'gold', sheetIndex: 1, day: '2026-09-29', status: 'written', orders: [O.rid, '4179100002'] }, rec));

    /* Where the strong gold pixels round a piece lie, measured from its own cut edge (the outline, in the piece's frame, as
       the plate draws it): the nearest and the farthest, and how many. A line at an even distance keeps them in a narrow
       band; the outline scaled about its middle, or a second gold line on the edge itself, does not. */
    const band = (sel, pieceOf) => page.evaluate(([sel, pieceOf]) => {
      const cv = document.querySelector(sel), ctx = cv.getContext('2d');
      const g = pieceOf === 'order' ? (() => { const G = cv._order, x = G.mine[0]; return { x, k: G.k, X: G.R + x.p.cxPt * G.k, Y: G.R + x.p.cyPt * G.k }; })()
        : (() => { const W = SheetWin._W, x = W.sel; return { x, k: W.k, X: W.R + x.p.cxPt * W.k, Y: W.R + x.p.cyPt * W.k }; })();
      const { x, k, X, Y } = g, c = x.c, a = x.p.angle * Math.PI / 180, cos = Math.cos(a), sin = Math.sin(a);
      const edge = new Path2D(); CharmNestPDF.pathToCanvas(edge, c.outline, (px, py) => [(px - c.centerPt[0]) * k, (c.centerPt[1] - py) * k]);
      const probe = document.createElement('canvas').getContext('2d'); probe.lineJoin = 'round';
      const within = (lx, ly, d) => { probe.lineWidth = 2 * d; return probe.isPointInStroke(edge, lx, ly); };
      const half = Math.ceil(Math.hypot(x.p.wPt, x.p.hPt) / 2 * k + 14), x0 = Math.round(X - half), y0 = Math.round(Y - half);
      const d = ctx.getImageData(x0, y0, 2 * half, 2 * half).data, ds = [];
      for (let j = 0; j < 2 * half; j++) for (let i = 0; i < 2 * half; i++) {
        const o = (j * 2 * half + i) * 4, r = d[o], b = d[o + 2], al = d[o + 3];
        if (al < 128 || b > 170 || r < 150 || r - b < 55) continue;   // strong gold only (the pale fill and the charm's own ink are not)
        const dx = x0 + i + .5 - X, dy = y0 + j + .5 - Y, lx = dx * cos + dy * sin, ly = -dx * sin + dy * cos;
        if (probe.isPointInPath(edge, lx, ly)) continue;               // on the charm itself
        let lo = 0, hi = 12; if (within(lx, ly, hi)) { for (let n = 0; n < 12; n++) { const m = (lo + hi) / 2; if (within(lx, ly, m)) hi = m; else lo = m; } ds.push(hi); } else ds.push(99);
      }
      ds.sort((p, q) => p - q);
      return { n: ds.length, near: ds[Math.floor(ds.length * .01)], far: ds[Math.floor(ds.length * .99)], max: ds[ds.length - 1] };
    }, [sel, pieceOf]);
    const plateFrames = () => page.evaluate(async () => {
      const cv = document.getElementById('owSheetCv'), ctx = cv.getContext('2d'), seen = new Set();
      for (let i = 0; i < 5; i++) { await new Promise(r => setTimeout(r, 180)); const d = ctx.getImageData(0, 0, cv.width, cv.height).data; let h = 0; for (let j = 0; j < d.length; j += 61) h = (h * 31 + d[j]) >>> 0; seen.add(h); }
      return { frames: seen.size, running: !!cv._order.raf };
    });

    // 1 · the order view's Sheet tab, reduced motion: one line, the same distance out all the way round, standing still
    await page.evaluate(k => OrderWin.open(k, { view: 'sheet' }), `${O.rid}_${O.tid}`);
    await page.evaluate(() => OrderWin.view() === 'sheet' || document.querySelector('.owTabsV [data-ow-view="sheet"]').click());
    await page.waitForFunction(sh => { const i = OrderWin._sheet(); return i && i.sheet.id === sh && i.mine.length === 1 && i.mine[0].c && document.getElementById('owPlateWait').hidden; }, SH);
    await page.evaluate(p => OrderWin._sheet().focus(p), P1);
    const still = await band('#owSheetCv', 'order');
    assert(still.n > 150, 'the order\'s charm is ringed in gold: ' + JSON.stringify(still));
    assert(still.near > .6 && still.far < 4.6 && still.max < 5.5, 'the gold line keeps one distance from the charm\'s own edge (no second line on the edge, not the outline scaled about its middle): ' + JSON.stringify(still));
    assert.deepEqual(await plateFrames(), { frames: 1, running: false }, 'with reduced motion the line stands still');

    // 2 · motion allowed: the same line breathes (the plate changes from frame to frame), and stays in its band
    await page.emulateMedia({ reducedMotion: 'no-preference' });
    await page.evaluate(() => OrderWin._sheet().redraw());
    const live = await plateFrames();
    assert(live.frames > 2 && live.running, 'the line round the order\'s charm pulses: ' + JSON.stringify(live));
    const moving = await band('#owSheetCv', 'order');
    assert(moving.near > .6 && moving.far < 6.5, 'breathing, it keeps to the charm\'s shape: ' + JSON.stringify(moving));

    // 3 · the sheet window: the charm in hand, ringed the same way
    await page.emulateMedia({ reducedMotion: 'reduce' });
    await page.keyboard.press('Escape');
    await page.waitForFunction(() => !document.getElementById('orderWin').open, null, { timeout: 5000 });
    await page.evaluate(sh => SheetWin.open(sh), SH);
    await page.waitForFunction(() => SheetWin.isOpen() && SheetWin._W.geom);
    const at = await page.evaluate(p => { const W = SheetWin._W, r = W.el.fx.getBoundingClientRect(), s = r.width / W.el.fx.width, x = W.pieces.find(q => q.poolId === p); return { x: r.left + (W.R + x.p.cxPt * W.k) * s, y: r.top + (W.R + x.p.cyPt * W.k) * s }; }, P1);
    await page.mouse.click(at.x, at.y);
    await page.waitForFunction(p => SheetWin._W.sel && SheetWin._W.sel.poolId === p, P1);
    await page.evaluate(() => { SheetWin._W.el.fx.dataset.outlineTest = '1'; });   // (its marks layer: the charms in hand)
    const sw = await band('canvas[data-outline-test]', 'sheet');
    assert(sw.n > 150 && sw.near > .6 && sw.far < 4.6, 'the sheet window rings the charm in hand along its own outline: ' + JSON.stringify(sw));
    assert.deepEqual(errors, [], 'no page errors');
    console.log('  ✓ the gold line round the order\'s charm follows its own outline at one distance (order view and sheet window), pulses, and stands still with reduced motion');
    console.log('charm outline (adversarial) OK');
  } finally { await browser.close(); srv.close(); }
}
main().then(() => process.exit(0), e => { console.error(e); process.exit(1); });
