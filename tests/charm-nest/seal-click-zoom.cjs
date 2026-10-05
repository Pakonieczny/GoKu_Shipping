// A click on a seal zooms it at once (Paul, 2 Oct 21:03: "add click to zoom so the user can skip that [the rest before the zoom] and just
// click a given seal and have the zoom come immediately"). The seal grows by the same adaptive animation as the one a resting pointer
// brings, with no waiting; a second click puts it back; every action a seal has still happens. Runs in headless Chromium against the local
// fake site (bridge-server.cjs), the page's own seals (no copy, no tooltip, no explainer card for a click):
//  · a large, a medium and a tiny seal, the pointer having rested < 100 ms: the scale is > 1.05 within 120 ms of the click and has reached
//    the adaptive target within 400 ms; a second click puts it back and the pointer resting on it does not bring it round again;
//    a click elsewhere or Esc puts it back; a zoom that only just came by resting is not undone by the click that ends the same press;
//  · Enter and Space on a focused seal grow it and put it back;
//  · the Review card: a click on the printed-label seal prints nothing; a click on a seal where it lies over its button presses that button
//    exactly once (the action, not the zoom, wins there);
//  · the order window: the header strip seal (opens its step on the Timeline AND grows), the piece row's seals, the timeline's lane
//    stamp (selects its step AND grows, no explainer card), its detail seal (a click grows it, a second puts it back), and the "Where it is
//    now" seal (its click opens the step on the Timeline: no zoom, the view is gone).
//   SHOTS=<dir> node tests/charm-nest/seal-click-zoom.cjs [playwright-core dir]
const fs = require('fs'), path = require('path'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');
const { start } = require('./bridge-server.cjs');
const Timeline = require(path.join(root, 'netlify/functions/_orderTimeline.js'));

const DAY = 86400, SHIP = Math.floor(Date.UTC(2026, 9, 2, 17) / 1000);
const RID = '4176744752', KEY = '4176744752_41767447521';
const ORDERS = [{ receiptId: RID, orderNumber: RID, createTs: SHIP - 5 * DAY, updateTs: SHIP - 5 * DAY + 60, shipBy: SHIP, buyer: { name: 'Buyer 4752' }, buyerMessage: '', isGift: false, giftMessage: '', staffNote: '', messages: [],
  lines: [{ transactionId: '41767447521', listingId: '1800047521', sku: 'CHAIN_8941', title: 'CHAIN REPLACEMENT', quantity: 1, expectedShipDate: SHIP, variations: [{ name: 'Metal', value: 'Gold' }, { name: 'Length', value: '17 Inches' }], metalKey: '', metalLabel: '', personalization: '' }] }];

(async () => {
  const pwDir = process.argv[2] || process.env.PW_DIR || path.join(root, 'node_modules');
  let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { console.log('  – no playwright-core: not run'); return; }
  const shots = process.env.SHOTS || ''; if (shots) fs.mkdirSync(shots, { recursive: true });
  const srv = await start({ receipts: [] });
  const t0 = Date.now() - 3 * 3600e3;
  srv.st.put('Charm_Custom_Orders', KEY, { key: KEY, receiptId: RID, transactionId: '41767447521', sku: 'CHAIN_8941', title: 'CHAIN REPLACEMENT', category: 'Rework', kind: '', state: 'completed', how: 'print',
    completedAt: t0, completedBy: 'Paul', printedAt: t0, printedBy: 'Paul', lastPrintedAt: t0, lastPrintedBy: 'Paul', prints: 1, hasLabel: true, updatedAtMs: t0 + 60e3,
    stamps: [{ how: 'print', at: t0, by: 'Paul' }, { how: 'button', at: t0 + 60e3, by: 'Paul' }] }, false);
  const H = 36e5, T0 = Date.now() - 4 * 24 * H, ev = (type, h, x) => Object.assign({ id: `${RID}~${type}~${h}`, orderId: RID, type, at: T0 + h * H, by: 'Paul', source: 'sorter' }, x || {});
  const evs = Timeline.chronology([ev('arrived', 0, { source: 'etsy', by: 'Etsy' }), ev('placed', 5, { sheetId: 'shA', sheet: 'GF Sheet 1' }), ev('engraveApproved', 20, { sheetId: 'shA', sheet: 'GF Sheet 1' }), ev('laserDone', 30, { sheetId: 'shA', sheet: 'GF Sheet 1' })]).sort(Timeline.byTime);
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  const fails = [];
  const check = (ok, msg) => { if (!ok) fails.push(msg); console.log((ok ? '  ✓ ' : '  ✗ ') + msg); };
  const context = await browser.newContext({ viewport: { width: 1440, height: 900 }, deviceScaleFactor: shots ? 2 : 1 });
  await context.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => /fonts\.googleapis|fonts\.gstatic/.test(r.request().url()) ? r.fulfill({ status: 200, contentType: 'text/css', body: '' }) : r.abort());
  await context.addInitScript(() => {
    try { localStorage.setItem('cn.settings', JSON.stringify({ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on' })); localStorage.setItem('cn.employee', 'Test Operator'); } catch (_) {}
    // when the pointer came onto the seal under test, when the click came, and the seal's scale frame by frame from then on
    document.addEventListener('pointerover', e => { const z = window.__z; if (z && !z.over && z.s.contains(e.target)) z.over = performance.now(); }, true);
    document.addEventListener('click', e => { const z = window.__z; if (z && !z.click) z.click = performance.now(); }, true);
    window.__tick = s => {
      const z = window.__z = { s, over: 0, click: 0, samples: [] }, t0 = performance.now();
      (function tick() { const t = performance.now(), m = new DOMMatrix(getComputedStyle(s).transform); z.samples.push([t, Math.hypot(m.a, m.b)]); if (t - t0 < 1200) requestAnimationFrame(tick); })();
    };
  });
  const page = await context.newPage(), errors = [];
  page.setDefaultTimeout(30000);
  page.on('pageerror', e => { errors.push(e.message); console.error('page error:', e.message); });
  try {
    await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
    await page.waitForFunction(() => window.CN && window.Orders && window.Review && window.CustomPrint && window.Seal && window.OrderWin && window.OrderTimeline && CN.S.cloud.ok === true, null, { timeout: 60000 });
    await page.evaluate(async ({ orders, evs, where }) => {
      for (const order of orders) for (const line of order.lines) { const key = CharmNestOrders.lineKey(order, line); const row = { key, order, line, arrivedAt: Date.now(), spec: null, problems: [], state: 'pulled', reason: null, claimedBy: null, poolIds: [], engrave: null, material: null }; B.orders.rows.push(row); B.orders.byKey.set(key, row); }
      Orders.interpretAll(); await Orders.loadMaps(true); Orders.interpretAll(); Review.syncOrderItems(); CN.setMode('review'); Review.render();
      window.__prints = 0; CustomPrint.print = function () { __prints++; };   // (no label is made here)
      const wait = window.setTimeout.bind(window);
      OrderTimeline.get = async () => { await new Promise(r => wait(r, 20)); return JSON.parse(JSON.stringify({ id: orders[0].receiptId, events: evs, cancelled: null, where })); };
    }, { orders: ORDERS, evs, where: Timeline.whereOf(evs, null, { record: true }) });

    // a point on the nth seal that the seal itself answers for (clear: and none of the page's buttons does, over: the part that lies over a sealed button)
    const point = (sel, n = 0, mode = '') => page.evaluate(([sel, n, mode]) => {
      const s = document.querySelectorAll(sel)[n]; if (!s) return null; const r = s.getBoundingClientRect(), own = e => e && (e === s || s.contains(e)), bs = [...document.querySelectorAll('[data-seal-btn]')].map(b => b.getBoundingClientRect());
      // (m: a click's own position is a whole pixel and the button's edge moves a hair as it settles, so a "clear" point keeps 3 px off every button and an "over" point 3 px in)
      const onBtn = (x, y, m = 0) => bs.some(b => x >= b.left - m && x <= b.right + m && y >= b.top - m && y <= b.bottom + m), cx = r.left + r.width / 2, cy = r.top + r.height / 2;
      const f = mode === 'over' ? 0 : .25;   // (a seal hangs over its button by its edge only); the point nearest the seal's heart is the steadiest
      let best = null;
      for (let y = r.top + r.height * f + 2; y < r.bottom - 2; y += 2) for (let x = r.left + r.width * f + 2; x < r.right - 2; x += 2) if (own(document.elementFromPoint(x, y)) && (mode === 'over' ? onBtn(x, y, -3) : mode === 'clear' ? !onBtn(x, y, 3) : true)) { const d = Math.hypot(x - cx, y - cy); if (!best || d < best.d) best = { x, y, d }; }
      return best && { x: best.x, y: best.y };
    }, [sel, n, mode]);
    const sealState = (sel, n = 0) => page.evaluate(([sel, n]) => { const s = document.querySelectorAll(sel)[n]; if (!s) return null; const m = new DOMMatrix(getComputedStyle(s).transform), r = s.getBoundingClientRect(); return { k: s.dataset.sealZoom ? +s.dataset.sealZoom : 0, scale: Math.hypot(m.a, m.b), inView: r.left >= 0 && r.top >= 0 && r.right <= innerWidth && r.bottom <= innerHeight, size: Math.min(s.offsetWidth, s.offsetHeight) }; }, [sel, n]);
    const zoomed = () => page.evaluate(() => document.querySelectorAll('[data-seal-zoom]').length);
    const gone = () => page.waitForFunction(() => !document.querySelector('[data-seal-zoom]') && !document.querySelector('.sealZoomed') && !document.querySelector('[style*="z-index: 900"]'), null, { timeout: 4000 });
    const away = async () => { await page.mouse.move(5, 880, { steps: 2 }); await gone(); };
    /** Rests the pointer on the nth seal for an instant and clicks it, with the scale sampled every frame; → how quickly it grew. */
    const click = async (sel, n = 0, p) => {
      p = p || await point(sel, n); assert(p, 'a point on ' + sel + ' ' + n);
      await page.mouse.move(5, 880); await page.evaluate(([sel, n]) => window.__tick(document.querySelectorAll(sel)[n]), [sel, n]);
      await page.mouse.move(p.x, p.y); await page.mouse.click(p.x, p.y); await page.waitForTimeout(450);
      return page.evaluate(() => {
        const z = window.__z, k = +z.s.dataset.sealZoom || 0, at = f => { const q = z.samples.find(([t, v]) => t >= z.click && f(v)); return q ? q[0] - z.click : null; };
        return { k, rest: z.click - z.over, before: z.samples.filter(([t]) => t < z.click).every(([, v]) => Math.abs(v - 1) < .01), t105: at(v => v > 1.05), tTarget: k ? at(v => Math.abs(v - k) <= .05 * k) : null };
      });
    };
    const DELAY = await page.evaluate(() => Seal.zoom.DELAY);

    // ═══ 1 · a large, a medium and a tiny seal on a bare strip ═══
    await page.evaluate(() => {
      const d = document.createElement('div'); d.id = 'zlab'; d.style.cssText = 'position:fixed;left:470px;top:420px;display:flex;gap:110px;align-items:center;z-index:5';
      d.innerHTML = '<button type="button" id="zpre" style="position:absolute;left:-60px;width:20px;height:20px">·</button>' + [84, 56, 24].map((s, i) => Seal.html({ how: i % 2 ? 'button' : 'print', at: Date.now() - 3600e3 * (i + 1), by: 'Paul' }, 84).replace('class="seal ', `style="--seal-fit:${s}px" class="seal `)).join('');
      document.body.appendChild(d);
    });
    const lab = '#zlab .seal', sizes = [84, 56, 24];   // (the gentle curve, 3 Oct: ×1.08, ×1.15, ×1.55; none wider than 96 px)
    { const p = await point(lab, 0); await click(lab, 0, p); await page.mouse.click(40, 700); await gone(); }   // (a first run warms the page and the protocol up, so the rests below are real)
    for (let i = 0; i < 3; i++) {
      const p = await point(lab, i), r = await click(lab, i, p), st = await sealState(lab, i);   // (p: a point on the seal at rest, which the grown seal covers too)
      check(r.rest < 100 && r.rest < DELAY && r.before, `${sizes[i]}px seal: the pointer had rested ${r.rest.toFixed(0)} ms (the zoom waits ${DELAY} ms) and the seal was still at rest at the click`);
      check(r.t105 != null && r.t105 <= 120, `…a click grew it past ×1.05 within ${r.t105 == null ? '–' : r.t105.toFixed(0)} ms`);
      check(r.tTarget != null && r.tTarget <= 400 && r.k === (await page.evaluate(n => Motion.sealZoomScale(n, 96), st.size)) && r.k > 1, `…and reached its adaptive target ×${r.k} within ${r.tTarget == null ? '–' : r.tTarget.toFixed(0)} ms`);
      check(st.inView && await page.evaluate(() => !document.querySelector('.sealLens,.tlLoupe,.tlExp.on,.seal[title]')), '…in the view, no copy, no card, no tooltip');
      if (shots && i === 1) await page.screenshot({ path: path.join(shots, '1-clicked-seal.png'), clip: { x: 300, y: 250, width: 800, height: 400 } });
      // a second click puts it back, and the pointer resting on it does not bring it round again
      await page.mouse.click(p.x, p.y); await gone();
      await page.mouse.move(p.x + 3, p.y + 2); await page.waitForTimeout(DELAY + 250);
      check(await zoomed() === 0, '…a second click puts it back, and resting on it afterwards does not zoom it again');
      // …leaving and coming back works as ever
      await away(); await page.mouse.move(p.x, p.y, { steps: 2 }); await page.waitForFunction(i => document.querySelectorAll('#zlab .seal')[i].dataset.sealZoom, i, { timeout: DELAY + 1500 }); await away();
      // a click elsewhere puts it back; so does Esc
      await click(lab, i, p); await page.mouse.click(40, 700); await gone();
      check(true, '…a click elsewhere puts it back');
      await click(lab, i, p); await page.keyboard.press('Escape'); await gone();
      check(await zoomed() === 0, '…and so does Esc'); await page.evaluate(() => document.activeElement && document.activeElement.blur()); await away();
    }
    // a seal that has only just grown by resting is not put back by the click that ends the very press that was under way
    {
      const p = await point(lab, 1); await page.mouse.move(5, 880); await page.mouse.move(p.x, p.y); await page.waitForTimeout(DELAY - 60);
      await page.mouse.down(); await page.waitForTimeout(140); await page.mouse.up(); await page.waitForTimeout(500);
      check(await zoomed() === 1, 'a click that ends a press which began just before the rest did leaves the seal grown (it is not a "second" click)');
      await page.mouse.click(p.x, p.y); await gone();
      check(await zoomed() === 0, '…and the next click, on the grown seal, puts it back'); await away();
    }
    // the keyboard: Tab zooms at once (as ever); Enter and Space put it back and grow it again
    await page.focus('#zpre'); await page.keyboard.press('Tab');
    await page.waitForFunction(() => document.querySelector('#zlab .seal[data-seal-zoom]'), null, { timeout: 1500 });
    await page.keyboard.press('Enter'); await gone(); check(await zoomed() === 0, 'Enter on the focused, grown seal puts it back');
    await page.keyboard.press('Space'); await page.waitForFunction(() => document.querySelector('#zlab .seal[data-seal-zoom]'), null, { timeout: 1500 });
    await page.keyboard.press('Space'); await gone(); check(await zoomed() === 0 && await page.evaluate(() => window.scrollY === 0), 'Space grows it and puts it back (the page does not scroll)');
    await page.evaluate(() => { document.activeElement && document.activeElement.blur(); document.getElementById('zlab').remove(); }); await away();

    // ═══ 2 · the Review card: the printed-label seal prints nothing; a seal over its button still presses it ═══
    await page.click('#reviewView .rvSeg [data-cseg="done"]');
    const card = `#rvList .reviewListRow[data-rid="${RID}"]`, prints = () => page.evaluate(() => __prints);
    await page.waitForSelector(card + ' .sealRow .seal'); await page.waitForTimeout(700);
    {
      const sel = card + ' .sealRow .seal-print', clear = await point(sel, 0, 'clear'); assert(clear, 'a point on the printed-label seal clear of its button');
      const r = await click(sel, 0, clear);
      check(r.t105 != null && r.t105 <= 120 && r.tTarget != null && r.tTarget <= 400 && await prints() === 0, `Review card, printed-label seal: a click grew it (×1.05 in ${r.t105 == null ? '–' : r.t105.toFixed(0)} ms, ×${r.k} in ${r.tTarget == null ? '–' : r.tTarget.toFixed(0)} ms) and printed nothing`);
      if (shots) await page.screenshot({ path: path.join(shots, '2-review-card-click.png'), clip: { x: 0, y: 40, width: 1440, height: 520 } });
      await page.mouse.click(clear.x, clear.y); await gone();
      check(await prints() === 0 && await zoomed() === 0, '…a second click puts it back, still printing nothing'); await away();
    }
    // a seal that lies over a button: where it covers the button a click presses the button (exactly once), where it does not it grows
    {
      await page.evaluate(() => {
        window.__zb = 0; const d = document.createElement('div'); d.id = 'zbl'; d.style.cssText = 'position:fixed;left:700px;top:520px;z-index:5;display:flex;align-items:center';
        d.innerHTML = '<button type="button" class="btn sealedPrint sm" id="zbtn" data-seal-btn style="width:160px;height:44px">Print QR label</button>' + Seal.row({ stamps: [{ how: 'print', at: Date.now() - 3600e3, by: 'Paul' }], prints: 1 });
        d.lastElementChild.style.marginLeft = '-30px'; document.body.appendChild(d); document.getElementById('zbtn').addEventListener('click', () => window.__zb++);
      });
      const sel = '#zbl .seal', over = await point(sel, 0, 'over'), clear = await point(sel, 0, 'clear'), pressed = () => page.evaluate(() => __zb);
      assert(over && clear, 'a seal that lies partly over its button');
      await page.mouse.move(5, 880); await page.mouse.move(over.x, over.y); await page.mouse.click(over.x, over.y); await page.waitForTimeout(500);
      check(await pressed() === 1 && await zoomed() === 0, 'a click on a seal where it lies over its button presses the button exactly once (the button, not the zoom, answers there)');
      await away(); const r = await click(sel, 0, clear);
      check(await pressed() === 1 && r.t105 != null && r.t105 <= 120 && r.tTarget != null && r.tTarget <= 400, `…a click on the same seal where it does not presses nothing and grows it (×1.05 in ${r.t105 == null ? '–' : r.t105.toFixed(0)} ms)`);
      const o2 = await point(sel, 0, 'over'); await page.mouse.click(o2.x, o2.y); await page.waitForTimeout(500);
      check(await pressed() === 2 && await zoomed() === 1, '…and a click on the grown seal over the button presses it once more and leaves it grown');
      await away(); await page.evaluate(() => document.getElementById('zbl').remove());
    }

    // ═══ 3 · the order window ═══
    await page.evaluate(k => OrderWin.open(k), KEY);
    await page.waitForFunction(() => document.querySelectorAll('#owRail .tlStop .tlSeal').length > 3 && document.querySelectorAll('#owPcSum .sealRow .seal').length >= 1, null, { timeout: 15000 });
    await page.waitForTimeout(1200); await page.mouse.move(700, 600);
    const view = () => page.evaluate(() => (document.querySelector('#orderWin [data-ow-view][aria-selected="true"]') || {}).dataset.owView);
    // the piece row's seals
    {
      const sel = '#owPcSum .sealRow .seal', r = await click(sel, 0, await point(sel, 0, 'clear'));
      check(r.t105 != null && r.t105 <= 120 && r.tTarget != null && r.tTarget <= 400, `order window piece row seal (its one small seal): a click grew it (×1.05 in ${r.t105 == null ? '–' : r.t105.toFixed(0)} ms, target ×${r.k} in ${r.tTarget == null ? '–' : r.tTarget.toFixed(0)} ms)`);
      await away();
    }
    // the header strip: the click opens its step on the Timeline, and the seal grows all the same
    {
      const sel = '#owRail .tlStop .tlSeal:not(.pending)', i = await page.evaluate(sel => [...document.querySelectorAll(sel)].findIndex(s => /\bd\b/.test(s.closest('.tlStop').className)), sel);
      assert(i >= 0 && await view() === 'info', 'a done step in the strip, on the Overview');
      const r = await click(sel, i);
      // (this click also opens the Timeline, which is some 100 ms of the page's own work before the next frame)
      check(r.t105 != null && r.t105 <= 250 && r.tTarget != null && r.tTarget <= 550, `header strip seal: a click grew it (×1.05 in ${r.t105 == null ? '–' : r.t105.toFixed(0)} ms, target ×${r.k} in ${r.tTarget == null ? '–' : r.tTarget.toFixed(0)} ms)`);
      check(await view() === 'timeline', '…and still opened the step on the Timeline');
      await page.waitForTimeout(600); const still = await zoomed();
      check(still <= 1, '…and at most that one seal is grown afterwards (' + still + ')'); await away();
    }
    // the Timeline: a lane stamp selects its step and grows (no explainer card for a click); the detail seal grows, and a second click puts it back
    await page.waitForSelector('#orderWin .tlSt[data-key]', { timeout: 8000 }); await page.waitForTimeout(900);
    {
      const sel = '#orderWin .tlSt[data-key]:not(.pending)', key = await page.evaluate(sel => document.querySelectorAll(sel)[1].dataset.key, sel);
      const r = await click(sel, 1);
      // (selecting a step lays the lanes out again, which puts the grown stamp back once; it grows again at once, a frame or two later)
      check(r.t105 != null && r.t105 <= 250 && r.tTarget != null && r.tTarget <= 600, `timeline lane stamp: a click grew it (×1.05 in ${r.t105 == null ? '–' : r.t105.toFixed(0)} ms, target ×${r.k} in ${r.tTarget == null ? '–' : r.tTarget.toFixed(0)} ms)`);
      check(await page.evaluate(k => (document.querySelector('#orderWin .tlBig') || {}).dataset.key === k, key) && await page.evaluate(() => !document.querySelector('.tlExp.on')), '…its step is selected in the detail, and no explainer card opened');
      if (shots) await page.screenshot({ path: path.join(shots, '3-timeline-stamp-click.png') });
      const p = await point(sel, 1); await page.mouse.click(p.x, p.y); await page.waitForTimeout(350);
      check(await zoomed() === 1, '…a second click on a stamp (a button) leaves it grown, selecting again');
      await away();
      // an "Around this step" seal: its click selects that step (and the detail is drawn again), and no other seal is left grown by it
      {
        const asel = '#orderWin .tlArw:not(.cur) .sv', key2 = await page.evaluate(sel => document.querySelector(sel).dataset.key, asel);
        await click(asel, 0); await page.waitForTimeout(300);
        check(await page.evaluate(k => (document.querySelector('#orderWin .tlBig') || {}).dataset.key === k, key2) && await page.evaluate(k => [...document.querySelectorAll('[data-seal-zoom]')].every(e => (e.closest('[data-key]') || e).dataset.key === k), key2), 'an "Around this step" seal: its click selects that step, and no neighbour is left grown');
        await away();
      }
      const big = '#orderWin .tlBig', rb = await click(big, 0);
      // (a detail seal drawn wider than the order window's 72px cap is not grown at all, only lifted: its zoom then has no ×1.05 to reach)
      check((rb.k === 1 || (rb.t105 != null && rb.t105 <= 120)) && rb.tTarget != null && rb.tTarget <= 400, `timeline detail seal: a click grew it (×1.05 in ${rb.t105 == null ? '–' : rb.t105.toFixed(0)} ms, target ×${rb.k} in ${rb.tTarget == null ? '–' : rb.tTarget.toFixed(0)} ms)`);
      const pb = await point(big, 0); await page.mouse.click(pb.x, pb.y); await gone();
      check(await zoomed() === 0, '…and a second click puts it back'); await away();
    }
    // the Overview's "Where it is now" seal: its click opens the step on the Timeline (the Overview goes away), so there is nothing to zoom
    await page.click('#orderWin [data-ow-view="info"]'); await page.waitForSelector('#orderWin .tlNowSeal', { timeout: 8000 }); await page.waitForTimeout(700);
    {
      const p = await point('#orderWin .tlNowSeal', 0); await page.mouse.move(5, 880); await page.mouse.move(p.x, p.y); await page.mouse.click(p.x, p.y); await page.waitForTimeout(600);
      check(await view() === 'timeline' && await zoomed() === 0, 'the "Where it is now" seal: its click opens the step on the Timeline, and leaves no zoom behind'); await away();
    }
    assert.deepEqual(errors, [], 'no page errors');
  } finally { await browser.close(); srv.close(); }
  if (fails.length) { console.error('\nFAILED:\n - ' + fails.join('\n - ')); process.exit(1); }
  console.log('Seal click zoom OK');
})().catch(e => { console.error(e); process.exit(1); });
