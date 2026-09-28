// No haze over a sheet, anywhere one is drawn (Paul, 28 Sep: "there seems to be a haze over all the parts, all the
// charms … make sure that everything is always perfectly visible and there's no weird semi-transparent haze over the
// sheet"). Checked on the two plates that used to lay a wash over the charms:
//  · the order view's Sheet tab (SheetWin.drawOrder): every other order's charm is drawn at its full strength, and the
//    open order's pieces are told apart by gold, a firmer line and a ring — not by dimming the rest;
//  · the sheet window's marks layer (hover, selection, a search): the same, the charm in hand marked, the rest sharp;
//  · a sheet switched while the last is still being read leaves no half-faded plate behind.
//   node tests/charm-nest/adv-haze.cjs   (PW_DIR=<playwright node_modules>, CHROMIUM=<chrome>)
const path = require('path'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');
const { start } = require('./bridge-server.cjs');

const DAY = 86400, SHIP = Math.floor(Date.UTC(2026, 9, 2, 17) / 1000);
const O = { rid: '4179000001', t1: '41790000011', t2: '41790000012' };
const P1 = `${O.rid}_${O.t1}_1`, P2 = `${O.rid}_${O.t2}_1`, SH1 = 'sheet-haze-1', SH2 = 'sheet-haze-2';
const line = (tid, sku, metalKey, metalLabel) => ({ transactionId: tid, listingId: '18000' + tid.slice(-5), sku, title: sku + ' necklace', quantity: 1, expectedShipDate: SHIP, variations: [{ name: 'Metal', value: metalLabel }], metalKey, metalLabel, personalization: [], buyerMessage: '' });
const order = (rid, buyer, lines) => ({ receiptId: rid, orderNumber: rid, createTs: SHIP - 5 * DAY, updateTs: SHIP - 5 * DAY + 60, shipBy: SHIP, buyer: { name: buyer }, buyerMessage: '', isGift: false, giftMessage: '', staffNote: '', messages: [], lines });

async function main() {
  const pwDir = process.env.PW_DIR || path.join(root, 'node_modules');
  let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { console.log('  – no playwright-core: the browser checks were not run'); return; }
  const srv = await start({ receipts: [] });
  const box = (id, cx, cy, w = 18) => ({ id, cxPt: cx, cyPt: cy, angle: 0, wPt: w, hPt: w });
  // two sheets of 60 pieces, one piece of each belonging to the order opened
  const sheet = (id, n, mine, poolId) => {
    const pl = [], ch = [];
    for (let i = 0; i < 60; i++) { const cid = id + '-' + i, rid = i === mine ? O.rid : String(4180000000 + i); pl.push(box(cid, 12 + (i % 15) * 22, 12 + Math.floor(i / 15) * 22)); ch.push({ id: cid, name: `${rid} · SKU${i}`, poolId: i === mine ? poolId : `${rid}_${rid}1_1`, order: rid, sku: 'SKU' + i }); }
    srv.st.put('Charm_Nest_Sheets', id, { id, metal: 'gold', sheetIndex: n, day: '2026-09-27', status: 'written', stock: { wPt: 340, hPt: 140 }, orders: [...new Set(ch.map(c => c.order))], placements: pl, charms: ch });
  };
  sheet(SH1, 1, 45, P1); sheet(SH2, 2, 20, P2);
  for (const [p, tid, sh, n] of [[P1, O.t1, SH1, 1], [P2, O.t2, SH2, 2]])
    srv.st.put('Charm_Pool', p, { poolId: p, orderId: O.rid, transactionId: tid, lineKey: `${O.rid}_${tid}`, sku: 'SKU', material: 'gold', copy: 1, quantity: 1, state: 'written', sheetId: sh, sheetName: `2026-09-27_GF_Set-1_Sheet-${n}`, updatedAt: Date.now() });

  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  try {
    const context = await browser.newContext({ viewport: { width: 1440, height: 900 } });
    await context.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => /fonts\.googleapis|fonts\.gstatic/.test(r.request().url()) ? r.fulfill({ status: 200, contentType: 'text/css', body: '' }) : r.abort());
    await context.addInitScript(() => { try { if (!sessionStorage.getItem('__seeded')) { localStorage.setItem('cn.settings', JSON.stringify({ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on' })); localStorage.setItem('cn.employee', 'Test Operator'); sessionStorage.setItem('__seeded', '1'); } } catch (_) {} window.prompt = () => 'Test Operator'; });
    const page = await context.newPage(), errors = [];
    page.setDefaultTimeout(25000);
    page.on('pageerror', e => errors.push(e.message));
    await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
    await page.waitForFunction(() => window.CN && window.Orders && window.OrderWin && window.SheetWin && SheetWin.drawOrder && CN.S.cloud.ok === true, null, { timeout: 60000 });
    await page.evaluate(async ({ orders }) => {
      await Orders.loadMaps(true);
      for (const [order, pools] of orders) order.lines.forEach((line, i) => { const key = CharmNestOrders.lineKey(order, line); const row = { key, order, line, arrivedAt: Date.now(), spec: null, problems: [], state: 'pooled', reason: null, claimedBy: null, poolIds: [pools[i]], engrave: null, material: null }; B.orders.rows.push(row); B.orders.byKey.set(key, row); });
      Orders.interpretAll(); CN.setMode('orders'); Orders.render();
    }, { orders: [[order(O.rid, 'Ola Berg', [line(O.t1, 'SKU45', 'gold', '14k Gold Filled'), line(O.t2, 'SKU20', 'gold', '14k Gold Filled')]), [P1, P2]]] });

    /* the darkest pixel across a piece's middle: how strongly that charm is drawn on the plate. A wash over the sheet
       lifts every one of them towards paper white; without one they keep their own ink. */
    const inkOf = (cvId, which) => page.evaluate(([cvId, which]) => {
      const cv = document.getElementById(cvId) || document.querySelector(cvId), ctx = cv.getContext('2d');
      const G = cv._order, W = window.SheetWin._W;
      const pieces = G ? G.pieces : W.pieces, mineSet = new Set(G ? G.mine : []);
      const R = G ? G.R : W.R, k = G ? G.k : W.k;
      const x = pieces.find(p => (which === 'mine') === mineSet.has(p) && !p.gone);
      if (!x) return null;
      const cx = Math.round(R + x.p.cxPt * k), cy = Math.round(R + x.p.cyPt * k), half = Math.round(x.p.wPt * k / 2) + 3;
      const d = ctx.getImageData(Math.max(0, cx - half), cy, half * 2, 1).data;
      let dark = 255, gold = 0;
      for (let i = 0; i < d.length; i += 4) { const l = (d[i] * 299 + d[i + 1] * 587 + d[i + 2] * 114) / 1000; if (l < dark) dark = l; if (d[i] - d[i + 2] > 24) gold++; }
      return { dark: Math.round(dark), gold };
    }, [cvId, which]);

    // 1 · the order view's Sheet tab
    await page.evaluate(k => OrderWin.open(k, { view: 'sheet' }), `${O.rid}_${O.t1}`);
    await page.evaluate(() => OrderWin.view() === 'sheet' || document.querySelector('.owTabsV [data-ow-view="sheet"]').click());
    const drawn = sh => page.waitForFunction(sh => { const i = OrderWin._sheet(); return i && i.sheet.id === sh && document.getElementById('owPlateWait').hidden && !document.getElementById('owSheetCv').getAnimations().some(a => a.playState === 'running'); }, sh);
    await drawn(SH1);
    const other = await inkOf('owSheetCv', 'other'), mine = await inkOf('owSheetCv', 'mine');
    assert(other && other.dark < 200, 'another order\'s charm is drawn at its own strength, with no wash over it: ' + JSON.stringify(other));
    assert(mine && mine.gold > 0 && mine.dark < 200, 'the open order\'s piece is marked in gold: ' + JSON.stringify(mine));
    // it is marked by its own colour, not by paling the others: both are drawn about as strongly
    assert(Math.abs(other.dark - mine.dark) < 70, 'the order\'s piece is not marked by dimming the rest: ' + JSON.stringify({ other, mine }));
    // the plate itself is never left half-faded, even when a sheet is chosen while the last is still being read
    await page.evaluate(() => { const b = [...document.querySelectorAll('#owSheetPanel .owShTabs button')]; b[1].click(); b[0].click(); });
    await drawn(SH1);
    assert.equal(await page.evaluate(() => getComputedStyle(document.getElementById('owSheetCv')).opacity), '1', 'the plate comes back to full after a sheet switched mid-read');

    // 2 · the sheet window's marks layer: a charm hovered, then a search
    await page.evaluate(() => OrderWin.close());
    await page.evaluate(sh => SheetWin.open(sh), SH1);
    await page.waitForFunction(() => SheetWin.isOpen() && SheetWin._W.geom);
    const at = await page.evaluate(() => { const W = SheetWin._W, r = W.el.fx.getBoundingClientRect(), s = r.width / W.el.fx.width, x = W.pieces[10]; return { x: r.left + (W.R + x.p.cxPt * W.k) * s, y: r.top + (W.R + x.p.cyPt * W.k) * s, poolId: x.poolId }; });
    await page.mouse.move(at.x, at.y);
    await page.waitForFunction(p => SheetWin._W.hover && SheetWin._W.hover.poolId === p, at.poolId);
    await page.evaluate(() => new Promise(r => requestAnimationFrame(() => requestAnimationFrame(r))));
    // the marks layer over the sheet holds nothing but the marks: every pixel of it outside a marked charm is clear
    const wash = await page.evaluate(() => {
      const W = SheetWin._W, cv = W.el.fx, ctx = cv.getContext('2d'), d = ctx.getImageData(W.R, W.R, cv.width - W.R, cv.height - W.R).data;
      let on = 0; for (let i = 3; i < d.length; i += 4) if (d[i]) on++;
      return on / (d.length / 4);
    });
    assert(wash < 0.25, 'the sheet window lays no wash over its sheet: ' + (wash * 100).toFixed(1) + '% of the plate covered');
    await page.evaluate(() => { const i = document.querySelector('.swFind input'); i.value = 'SKU1'; i.dispatchEvent(new Event('input', { bubbles: true })); });
    await page.evaluate(() => new Promise(r => setTimeout(() => requestAnimationFrame(() => requestAnimationFrame(r)), 350)));
    const wash2 = await page.evaluate(() => {
      const W = SheetWin._W, cv = W.el.fx, ctx = cv.getContext('2d'), d = ctx.getImageData(W.R, W.R, cv.width - W.R, cv.height - W.R).data;
      let on = 0; for (let i = 3; i < d.length; i += 4) if (d[i]) on++;
      return on / (d.length / 4);
    });
    assert(wash2 < 0.4, 'a search marks what it finds without washing the sheet: ' + (wash2 * 100).toFixed(1) + '% covered');
    assert.deepEqual(errors, [], 'no page errors');
    console.log('  ✓ no haze over a sheet: the order view\'s plate, the sheet window on hover and on a search, and no half-faded plate left behind');
    console.log('haze (adversarial) OK');
  } finally { await browser.close(); }
}
main().then(() => process.exit(0), e => { console.error(e); process.exit(1); });
