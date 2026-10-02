// The order view's Sheet tab (Paul, 28 Sep: "a button that allows me to open that order's detailed view … a beautiful and
// seamless animation"; 2 Oct: "I should be able to click all of the orders on the sheet and just have the page adjust to
// that order"): ONE click on another order's charm moves the view to that order with the Previous / Next slide — one view,
// never a view over it, no "Open order" label in between — on the same sheet and the same side, that order's charm
// chosen; "Back to order N" brings N back as it was (its sheet, its charm, its side); a charm of this order is only
// chosen; a click beside the charms does nothing; Esc closes the view. The slide is measured:
// only transform and opacity animate, the plate is not drawn again until it has landed, and frames stay under 34 ms.
// Headless Chromium against the fake site (bridge-server.cjs); every request that is not to the loopback is aborted.
//   node tests/charm-nest/adv-orderview-sheet-open.cjs   (PW_DIR=<playwright node_modules>, CHROMIUM=<chrome>, SHOTS=<dir>)
const path = require('path'), os = require('os'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');
const { start } = require('./bridge-server.cjs');

const DAY = 86400, SHIP = Math.floor(Date.UTC(2026, 9, 2, 17) / 1000);
const S = { rid: '4177000001', tid: '41770000011', sku: 'TINY_TAG' }, T = { rid: '4177000002', tid: '41770000021', sku: 'LEAF_CHARM' };
const SH = 'sheet-adv-open', PS = `${S.rid}_${S.tid}_1`, PT = `${T.rid}_${T.tid}_1`;
const line = (tid, sku) => ({ transactionId: tid, listingId: '18000' + tid.slice(-5), sku, title: sku.replace(/_/g, ' ') + ' necklace', quantity: 1, expectedShipDate: SHIP, variations: [{ name: 'Metal', value: '14k Gold Filled' }], metalKey: 'gold', metalLabel: 'GF 14/20', personalization: ['Initial: H'], buyerMessage: '' });
const order = (o, buyer) => ({ receiptId: o.rid, orderNumber: o.rid, createTs: SHIP - 5 * DAY, updateTs: SHIP - 5 * DAY + 60, shipBy: SHIP, buyer: { name: buyer }, buyerMessage: '', isGift: false, giftMessage: '', staffNote: '', messages: [], lines: [line(o.tid, o.sku)] });

function seed(st) {
  const box = (id, cx, cy, w) => ({ id, cxPt: cx, cyPt: cy, angle: 0, wPt: w, hPt: w }), pl = [], ch = [];
  for (let i = 0; i < 60; i++) {
    const o = i === 20 ? S : i === 36 ? T : null, rid = o ? o.rid : String(4178000000 + i), pid = o ? `${o.rid}_${o.tid}_1` : `${rid}_${rid}1_1`;
    pl.push(box('b' + i, 12 + (i % 15) * 22, 12 + Math.floor(i / 15) * 22, 18)); ch.push({ id: 'b' + i, name: `${rid} · SKU${i}`, poolId: pid, order: rid, sku: o ? o.sku : 'SKU' + i });
  }
  st.put('Charm_Nest_Sheets', SH, { id: SH, metal: 'gold', sheetIndex: 2, day: '2026-09-27', status: 'written', stock: { wPt: 340, hPt: 110 }, orders: [...new Set(ch.map(c => c.order))], placements: pl, charms: ch });
  for (const [o, pid] of [[S, PS], [T, PT]]) st.put('Charm_Pool', pid, { poolId: pid, orderId: o.rid, transactionId: o.tid, lineKey: `${o.rid}_${o.tid}`, sku: o.sku, material: 'gold', copy: 1, quantity: 1, state: 'written', sheetId: SH, sheetName: '2026-09-27_GF_Set-1_Sheet-2', updatedAt: Date.now() });
}

async function main() {
  const pwDir = process.env.PW_DIR || path.join(root, 'node_modules');
  let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { console.log('  – no playwright-core: the browser checks were not run'); return; }
  const srv = await start({ receipts: [] }); seed(srv.st);
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  const shots = process.env.SHOTS || '';
  try {
    const context = await browser.newContext({ viewport: { width: 1440, height: 900 } });
    await context.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => /fonts\.googleapis|fonts\.gstatic/.test(r.request().url()) ? r.fulfill({ status: 200, contentType: 'text/css', body: '' }) : r.abort());
    await context.route(/\/\.netlify\/functions\/etsyMailOrderLink/, r => r.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify({ engagements: [], active: null, conversation: null, ok: true, n: 0 }) }));
    await context.addInitScript(() => {
      try { if (!sessionStorage.getItem('__seeded')) { localStorage.setItem('cn.settings', JSON.stringify({ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on' })); localStorage.setItem('cn.employee', 'Test Operator'); localStorage.setItem('cn.mail.station', JSON.stringify('k-adv-test')); sessionStorage.setItem('__seeded', '1'); } } catch (_) {}
      window.prompt = () => 'Test Operator';
      window.__lt = []; try { new PerformanceObserver(l => { for (const e of l.getEntries()) window.__lt.push({ s: e.startTime, d: e.duration }); }).observe({ type: 'longtask', buffered: true }); } catch (_) {}
    });
    const page = await context.newPage(), errors = [];
    page.setDefaultTimeout(20000);
    page.on('pageerror', e => { errors.push(e.message); console.error('page error:', String(e.stack || e.message).split('\n').slice(0, 4).join(' | ')); });
    await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
    await page.waitForFunction(() => window.CN && window.Orders && window.OrderWin && window.SheetWin && SheetWin.drawOrder && CN.S.cloud.ok === true, null, { timeout: 60000 });
    await page.evaluate(async ({ orders }) => {
      await Orders.loadMaps(true);
      for (const [order, pool] of orders) for (const line of order.lines) { const key = CharmNestOrders.lineKey(order, line); const row = { key, order, line, arrivedAt: Date.now(), spec: null, problems: [], state: 'pooled', reason: null, claimedBy: null, poolIds: [pool], engrave: null, material: null }; B.orders.rows.push(row); B.orders.byKey.set(key, row); }
      Orders.interpretAll(); CN.setMode('orders'); Orders.render();
    }, { orders: [[order(S, 'Mia Lund'), PS], [order(T, 'Ada Byrne'), PT]] });
    await page.waitForTimeout(500);

    const title = () => page.evaluate(() => document.getElementById('owTitle').textContent);
    const drawn = (rid, pid) => page.waitForFunction(({ sh, pid }) => { const i = OrderWin._sheet(); return !!(i && i.sheet && i.sheet.id === sh && i.pointOf(pid)) && document.getElementById('owSheetCv').style.visibility !== 'hidden'; }, { sh: SH, pid }, { timeout: 15000 });
    const clickPiece = async pid => { const pt = await page.evaluate(p => OrderWin._sheet().pointOf(p), pid); await page.mouse.move(pt.x - 3, pt.y); await page.mouse.click(pt.x, pt.y); };
    const face = () => page.evaluate(() => document.querySelector('#owFace [data-face].on').dataset.face);
    const chosen = () => page.evaluate(() => (document.querySelector('#owSheetPanel .owCharm b') || {}).textContent || '');

    // S's Sheet view
    await page.evaluate(({ rid, pool }) => OrderWin.openOrder(rid, { view: 'sheet', poolId: pool }), { rid: S.rid, pool: PS });
    await drawn(S.rid, PS); await page.waitForTimeout(700);
    assert(/4177000001/.test(await title()), 'S is open');

    // its Back side (the side must come along and come back)
    await page.click('#owFace [data-face=back]'); await page.waitForTimeout(900);
    assert.equal(await face(), 'back', 'turned to its Back');

    // a click on the plate's bare edge does nothing; a click on S's own charm only chooses it
    const edge = await page.evaluate(() => { const r = document.getElementById('owSheetCv').getBoundingClientRect(); return { x: r.left + r.width * .5, y: r.bottom - 6 }; });
    await page.mouse.click(edge.x, edge.y); await page.waitForTimeout(150);
    assert(/4177000001/.test(await title()), 'a click beside the charms: still S');
    assert(!(await page.evaluate(() => !!document.getElementById('owPlatePick'))), 'and no "Open order" label exists on the plate');
    await clickPiece(PS); await page.waitForTimeout(120);
    { const c = await chosen(); assert(/4177000001/.test(await title()) && c === S.sku, 'a charm of this order is chosen on S: ' + c + ' / ' + (await title())); }

    // ONE click on T's charm: the Previous / Next slide to T, measured
    await page.mouse.move(edge.x, edge.y);
    await page.evaluate(() => {
      // (measured from the click itself: the frames, the plate's visibility and the moment the plate is drawn again)
      const cv = document.getElementById('owSheetCv'), M = window.__s = { fr: [], vis: [], t0: performance.now(), on: true, go: false, infoAt: null };
      cv.addEventListener('click', () => { M.go = true; M.t0 = performance.now(); M.fr = []; M.vis = []; }, true);
      const loop = t => { if (!M.on) return; if (M.go) { M.fr.push(t); M.vis.push(cv.style.visibility); if (M.infoAt == null && OrderWin._sheet()) M.infoAt = t - M.t0; } requestAnimationFrame(loop); }; requestAnimationFrame(loop);
    });
    await clickPiece(PT);
    const kf = await page.evaluate(() => { const a = document.querySelector('#orderWin .owBody').getAnimations().slice(-1)[0]; return a ? { keys: a.effect.getKeyframes().flatMap(k => Object.keys(k).filter(p => !['offset', 'easing', 'composite', 'computedOffset'].includes(p))), dur: a.effect.getTiming().duration } : { keys: [], dur: 0 }; });
    await page.waitForTimeout(1000);
    const m = await page.evaluate(({ keys, dur }) => {
      const M = window.__s; M.on = false; const t0 = M.t0;
      const inFlight = M.fr.filter(t => t - t0 <= dur + 17), dt = inFlight.slice(1).map((t, i) => t - inFlight[i]);
      const lt = window.__lt.filter(x => x.s + x.d > t0 && x.s < t0 + dur);
      return { dur, keys, maxDt: Math.round(Math.max(0, ...dt)), over34: dt.filter(x => x > 34).length, lt: lt.length, ltMax: Math.round(Math.max(0, ...lt.map(x => x.d))), infoAt: M.infoAt == null ? null : Math.round(M.infoAt), hidden: M.vis.slice(0, 12).filter(v => v === 'hidden').length, dialogs: document.querySelectorAll('dialog[open]').length };
    }, kf);
    console.log(`  slide: ${m.dur} ms of ${[...new Set(m.keys || [])].join('+')} · max frame ${m.maxDt} ms · ${m.over34} over 34 ms · ${m.lt} long task(s) max ${m.ltMax} ms · plate drawn at ${m.infoAt} ms`);
    assert(/4177000002/.test(await title()), 'one click moved the view to T');
    assert.equal(m.dialogs, 1, 'one view: never a view over the view');
    assert(m.dur >= 280, 'the slide lasts at least 280 ms');
    assert.deepEqual([...new Set(m.keys)].filter(p => !['opacity', 'transform'].includes(p)), [], 'only transform and opacity animate');
    assert.equal(m.hidden, 0, 'the plate stays in view through the slide (the same sheet)');
    assert(m.infoAt == null || m.infoAt >= m.dur - 20, `the plate is not drawn again mid-slide (${m.infoAt} ms)`);
    await drawn(T.rid, PT); await page.waitForTimeout(500);
    assert.equal(await face(), 'back', 'T is shown on the same side');
    assert.equal(await chosen(), T.sku, "T's charm is chosen");
    const back = await page.evaluate(() => { const b = document.querySelector('#owSheetPanel [data-ow-back]'); return b ? b.textContent : ''; });
    assert(back.includes('Back to order ' + S.rid), 'Back to order S is offered: ' + back);
    if (os.loadavg()[0] / os.cpus().length < 1.2) { assert(m.over34 <= 2, 'at most two frames over 34 ms'); assert(m.ltMax <= 50, 'no long task over 50 ms in the slide'); }

    // a mid-slide picture, going back
    if (shots) { await page.evaluate(() => document.querySelector('#owSheetPanel [data-ow-back]').click()); await page.waitForTimeout(130); await page.screenshot({ path: path.join(shots, 'b-mid-slide.png') }); }
    else await page.click('#owSheetPanel [data-ow-back]');
    await page.waitForFunction(() => /4177000001/.test(document.getElementById('owTitle').textContent));
    await drawn(S.rid, PS); await page.waitForTimeout(600);
    assert.equal(await face(), 'back', 'S comes back on its side');
    assert.equal(await chosen(), S.sku, "S comes back with its charm chosen");
    assert(await page.evaluate(sh => OrderWin._sheet().sheet.id === sh, SH), 'on the same sheet');
    assert(!(await page.evaluate(() => !!document.querySelector('#owSheetPanel [data-ow-back]'))), 'no way back past where it started');

    // Esc closes the view
    await page.keyboard.press('Escape'); await page.waitForTimeout(700);
    assert(!(await page.evaluate(() => OrderWin.isOpen())), 'Esc closes the view');
    assert.deepEqual(errors, [], 'no page errors');
    console.log('  ✓ one click on the charm, one view, slide, same sheet / side / charm, back, Esc');
    await context.close();
  } finally { await browser.close(); srv.close(); }
}
main().then(() => console.log('adv-orderview-sheet-open: ok'), e => { console.error(e); process.exit(1); });
