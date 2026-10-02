// The seal zoom (Paul, 2 Oct 18:56: "instead of hover states, just zoom the seal where it stands, like a magnifying glass:
// a little if it is large, more if it is small, even more if it is very small; an adaptive system", and "a 750 ms delay
// so the zoom does not happen immediately if the person runs the cursor quickly across the screen").
// There is no second seal, no bubble, no tooltip: the seal itself grows from its own centre by one scale curve
// (Motion.sealZoomScale). Runs in headless Chromium against the local fake site (bridge-server.cjs):
//  · the curve itself (a table), every size within its band, never rising as a seal gets larger;
//  · a large, a medium and a tiny seal rested on: the ratios grow as the seal shrinks, each grown seal stays in the view
//    and below the top bar, and unclipped; leaving puts every style back (transform, filter, overflow, z-index);
//  · a pointer sweeping across a seal for 300 ms zooms nothing, one resting 560 ms does (the delay is 500 ms); Tab zooms at once; Esc returns it;
//  · the real surfaces: the Review card's seals, the order window's header strip (24 px, in a 44 px box that clips), its
//    custom-order bar (once 1 px wide), the timeline's lane stamps and detail seal: each grows, in view, unclipped;
//  · only transform, filter and opacity move; a finger's tap zooms and a second tap returns it; reduced motion is short.
//   SHOTS=<dir> node tests/charm-nest/seal-zoom.cjs [playwright-core dir]
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
  const open = async (opts = {}) => {
    const context = await browser.newContext(Object.assign({ viewport: { width: 1440, height: 900 }, deviceScaleFactor: shots ? 2 : 1 }, opts));
    await context.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => /fonts\.googleapis|fonts\.gstatic/.test(r.request().url()) ? r.fulfill({ status: 200, contentType: 'text/css', body: '' }) : r.abort());
    await context.addInitScript(() => {
      try { localStorage.setItem('cn.settings', JSON.stringify({ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on' })); localStorage.setItem('cn.employee', 'Test Operator'); } catch (_) {}
      // no second seal, no bubble, ever: anything the old lens or loupe would have made is noticed the moment it exists
      window.__copies = [];
      new MutationObserver(ms => { for (const m of ms) for (const n of m.addedNodes) if (n.nodeType === 1 && (n.matches('.sealLens,.tlLoupe,.tlNowZoom') || n.querySelector && n.querySelector('.sealLens,.tlLoupe,.tlNowZoom'))) window.__copies.push(n.className); }).observe(document, { subtree: true, childList: true });
    });
    const page = await context.newPage(), errors = [];
    page.setDefaultTimeout(30000);
    page.on('pageerror', e => { errors.push(e.message); console.error('page error:', e.message); });
    await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
    await page.waitForFunction(() => window.CN && window.Orders && window.Review && window.CustomPrint && window.Seal && window.OrderWin && window.OrderTimeline && CN.S.cloud.ok === true, null, { timeout: 60000 });
    await page.evaluate(async ({ orders, evs, where }) => {
      for (const order of orders) for (const line of order.lines) { const key = CharmNestOrders.lineKey(order, line); const row = { key, order, line, arrivedAt: Date.now(), spec: null, problems: [], state: 'pulled', reason: null, claimedBy: null, poolIds: [], engrave: null, material: null }; B.orders.rows.push(row); B.orders.byKey.set(key, row); }
      Orders.interpretAll(); await Orders.loadMaps(true); Orders.interpretAll(); Review.syncOrderItems(); CN.setMode('review'); Review.render();
      window.__prints = 0; CustomPrint.print = function () { __prints++; };   // (no label is made here)
      const wait = window.setTimeout.bind(window);   // (the order's timeline: the steps it has taken, answered here)
      OrderTimeline.get = async () => { await new Promise(r => wait(r, 20)); return JSON.parse(JSON.stringify({ id: orders[0].receiptId, events: evs, cancelled: null, where })); };
    }, { orders: ORDERS, evs, where: Timeline.whereOf(evs, null, { record: true }) });
    return { context, page, errors };
  };
  const api = page => ({
    // a point on the seal found by `sel` (the nth one) that the seal itself answers for
    point: (sel, n = 0) => page.evaluate(([sel, n]) => {
      const s = document.querySelectorAll(sel)[n]; if (!s) return null; const r = s.getBoundingClientRect(), own = e => e && (e === s || s.contains(e));
      for (let y = r.top + r.height * .3; y < r.bottom - 2; y += 2) for (let x = r.left + r.width * .3; x < r.right - 2; x += 2) if (own(document.elementFromPoint(x, y))) return { x, y };
      return null;
    }, [sel, n]),
    // what the nth seal looks like now: its size at rest, the zoom it has, where it is, what clips it, what was done to its boxes
    probe: (sel, n = 0) => page.evaluate(([sel, n]) => {
      const s = document.querySelectorAll(sel)[n]; if (!s) return null; const r = s.getBoundingClientRect(), cs = getComputedStyle(s), tb = document.querySelector('.topbar');
      const clipped = []; for (let a = s.parentElement; a && a !== document.body && a !== document.documentElement; a = a.parentElement) {
        const c = getComputedStyle(a); if (c.overflowX === 'visible' && c.overflowY === 'visible') continue; const b = a.getBoundingClientRect(); if (!(a.clientWidth > 0)) continue;
        const L = b.left + a.clientLeft, T = b.top + a.clientTop, R = L + a.clientWidth, B = T + a.clientHeight;
        if (r.left < L - 1 || r.top < T - 1 || r.right > R + 1 || r.bottom > B + 1) clipped.push((a.id ? '#' + a.id : '') + '.' + String(a.className).split(' ')[0]);
      }
      const animated = [...new Set(document.getAnimations().filter(an => an.effect && an.effect.target && (an.effect.target === s || s.contains(an.effect.target))).flatMap(an => an.effect.getKeyframes().flatMap(k => Object.keys(k))))].filter(k => !['offset', 'computedOffset', 'easing', 'composite'].includes(k)).sort();
      return { size: Math.min(s.offsetWidth, s.offsetHeight), k: s.dataset.sealZoom ? +s.dataset.sealZoom : 0, w: r.width, rect: { l: r.left, t: r.top, r: r.right, b: r.bottom }, tf: cs.transform, filter: cs.filter, sealZoomed: s.classList.contains('sealZoomed'),
        inView: r.left >= 0 && r.top >= 0 && r.right <= innerWidth && r.bottom <= innerHeight, top: tb && !s.closest('dialog') ? tb.getBoundingClientRect().bottom : 0, clipped, animated, running: s.getAnimations().length,
        titled: s.hasAttribute('title'), openedBoxes: [...document.querySelectorAll('[style*="z-index: 900"]')].length, inDialog: !!s.closest('dialog') };
    }, [sel, n]),
    settle: () => page.waitForFunction(() => !document.getAnimations().some(a => a.playState === 'running' && Number.isFinite(a.effect.getComputedTiming().endTime) && a.effect.target && a.effect.target.closest && a.effect.target.closest('[data-seal-zoom],.seal,.tlSeal,.tlSt,.tlBig,.tlNowSeal')), null, { timeout: 5000 }),
    gone: () => page.waitForFunction(() => !document.querySelector('[data-seal-zoom]') && !document.querySelector('.sealZoomed') && !document.querySelector('[style*="z-index: 900"]'), null, { timeout: 4000 }),
    zoomed: () => page.evaluate(() => [...document.querySelectorAll('[data-seal-zoom]')].length),
  });
  try {
    // ═══ 1 · the curve ═══
    const A = await open(), { page } = A, X = api(page);
    const table = await page.evaluate(() => [12, 20, 24, 26, 30, 36, 40, 48, 56, 60, 72, 84, 100, 120, 140, 168, 240].map(n => [n, Motion.sealZoomScale(n)]));
    const bands = { 120: [1.1, 1.3], 140: [1.1, 1.25], 168: [1.1, 1.2], 240: [1.1, 1.2], 100: [1.5, 1.7], 84: [1.6, 2.2], 72: [1.6, 2.2], 60: [1.6, 2.5], 56: [2.4, 3.2], 48: [2.6, 3.2], 40: [2.6, 3.2], 36: [2.6, 3.3], 30: [3.3, 3.6], 26: [3.5, 3.6], 24: [3.5, 3.6], 20: [3.5, 3.6], 12: [3.5, 3.6] };
    check(table.every(([n, k]) => k >= bands[n][0] && k <= bands[n][1]), 'the curve: ' + table.map(([n, k]) => n + 'px→×' + k).join(' '));
    check(table.every(([, k], i) => i === 0 || k <= table[i - 1][1] + 1e-9) && table.every(([, k]) => k >= 1.12 && k <= 3.6), 'it never rises as a seal gets larger, between 1.12 and 3.6');

    // ═══ 2 · a large, a medium and a tiny seal, on a bare strip (one engine, whatever the page) ═══
    const lab = await page.evaluate(() => {
      const d = document.createElement('div'); d.id = 'zlab'; d.style.cssText = 'position:fixed;left:470px;top:420px;display:flex;gap:110px;align-items:center;z-index:5';
      d.innerHTML = '<button type="button" id="zpre" style="position:absolute;left:-60px;width:20px;height:20px">·</button>' + [140, 84, 56, 24].map((s, i) => Seal.html({ how: i % 2 ? 'button' : 'print', at: Date.now() - 3600e3 * (i + 1), by: 'Paul' }, 84).replace('class="seal ', `data-lab="${s}" style="--seal-fit:${s}px" class="seal `)).join('');
      document.body.appendChild(d);
      // a seal tucked against the very top left corner, under the bar, and one against the right edge
      for (const [id, css] of [['zcorner', 'left:3px;top:50px'], ['zedge', 'right:2px;top:300px']]) { const e = document.createElement('div'); e.id = id; e.style.cssText = 'position:fixed;z-index:5;' + css; e.innerHTML = Seal.html({ how: 'print', at: Date.now() - 7200e3, by: 'Paul' }, 84).replace('class="seal ', 'style="--seal-fit:30px" class="seal '); document.body.appendChild(e); }
      return [...d.querySelectorAll('.seal')].length;
    });
    assert.equal(lab, 4);
    const labSel = '#zlab .seal', want = [140, 84, 56, 24], ks = [];
    for (let i = 0; i < 4; i++) {
      const base = await X.probe(labSel, i), p = await X.point(labSel, i); assert(p, 'a point on lab seal ' + i);
      await page.mouse.move(5, 880); await page.mouse.move(p.x, p.y, { steps: 4 });
      await page.waitForFunction(i => document.querySelectorAll('#zlab .seal')[i].dataset.sealZoom, i, { timeout: 4000 }); await X.settle();
      const z = await X.probe(labSel, i); ks.push(z.k);
      check(z.size === want[i] && z.k === (await page.evaluate(n => Motion.sealZoomScale(n), z.size)) && Math.abs(z.w / z.size - z.k) < .25 * z.k, `${z.size}px seal rested on: grown ×${z.k} (drawn ${z.w.toFixed(0)}px wide)`);
      check(z.inView && z.rect.t >= z.top && !z.clipped.length && z.tf.startsWith('matrix(' + z.k + ', 0, 0, ' + z.k), `…upright, in the view, below the top bar (${z.top.toFixed(0)}), nothing clipping it ${JSON.stringify(z.clipped)}`);
      check(z.sealZoomed && z.filter !== 'none' && !z.titled, '…lifted on a shadow, no tooltip');
      check(z.animated.length > 0 && z.animated.every(k => ['filter', 'opacity', 'transform'].includes(k)), 'only transform, filter and opacity move: ' + z.animated);
      await page.mouse.move(5, 880, { steps: 3 }); await X.gone();
      const back = await X.probe(labSel, i);
      check(back.tf === base.tf && back.filter === base.filter && !back.running && !back.sealZoomed, '…and leaving puts every style back (transform ' + back.tf.slice(0, 30) + ')');
    }
    check(ks[0] < ks[1] && ks[1] < ks[2] && ks[2] < ks[3], `the smaller the seal the more it grows: ×${ks.join(' < ×')} for ${want.join(', ')} px`);
    // against the top corner and the right edge: nudged in, never out of the view
    for (const [sel, name] of [['#zcorner .seal', 'the top left corner'], ['#zedge .seal', 'the right edge']]) {
      const p = await X.point(sel); await page.mouse.move(5, 880); await page.mouse.move(p.x, p.y, { steps: 3 });
      await page.waitForFunction(sel => document.querySelector(sel).dataset.sealZoom, sel); await X.settle();
      const z = await X.probe(sel);
      check(z.inView && z.rect.l >= 0 && z.rect.t >= z.top && z.rect.r <= 1440, `a 30px seal at ${name}: grown ×${z.k}, nudged to stay in the view and below the bar`);
      await page.mouse.move(5, 880); await X.gone();
    }
    await page.evaluate(() => document.querySelectorAll('#zcorner,#zedge').forEach(e => e.remove()));

    // ═══ 3 · 500 ms: a pointer running across a seal zooms nothing; one that rests does ═══
    {
      const p = await X.point(labSel, 1), still = () => page.evaluate(() => !document.querySelector('[data-seal-zoom]'));
      await page.mouse.move(5, 880); await page.mouse.move(p.x, p.y, { steps: 2 }); await page.waitForTimeout(300); await page.mouse.move(5, 880, { steps: 2 }); await page.waitForTimeout(1000);
      check(await still(), 'a pointer across a seal for 300 ms zooms nothing (not even later)');
      await page.mouse.move(p.x, p.y, { steps: 2 }); await page.waitForTimeout(300);
      check(await still(), 'resting 300 ms: not yet');
      await page.waitForTimeout(260); await page.waitForFunction(() => document.querySelector('[data-seal-zoom]'), null, { timeout: 1200 });
      const timing = await page.evaluate(() => window.Seal.zoom.DELAY);
      check(timing === 500, 'resting past 500 ms zooms it (one named delay: ' + timing + ' ms)'); await X.settle();
      await page.mouse.move(5, 880); await X.gone();
    }

    // ═══ 4 · the keyboard: Tab zooms at once, Esc returns it ═══
    await page.focus('#zpre'); await page.keyboard.press('Tab');   // (the next thing that takes a key is the first seal)
    await page.waitForFunction(() => document.querySelector('#zlab .seal[data-seal-zoom]'), null, { timeout: 1500 }); await X.settle();
    const kz = await X.probe(labSel, 0); check(kz.k > 1 && kz.inView, 'Tab onto a seal zooms it at once (×' + kz.k + ')');
    await page.keyboard.press('Escape'); await X.gone();
    check((await X.zoomed()) === 0 && await page.evaluate(() => document.activeElement && document.activeElement.classList.contains('seal')), 'Esc puts it back (focus stays)');
    await page.evaluate(() => document.activeElement.blur());
    await page.evaluate(() => document.getElementById('zlab').remove());

    // ═══ 5 · the Review tab's Custom Orders card ═══
    await page.click('#reviewView .rvSeg [data-cseg="done"]');
    const card = `#rvList .reviewListRow[data-rid="${RID}"]`, rs = card + ' .sealRow .seal';
    await page.waitForSelector(rs); await page.waitForTimeout(700);
    const rp = await page.evaluate(sel => { const s = document.querySelector(sel + ' .sealRow .seal.seal-button'), r = s.getBoundingClientRect(), b = document.querySelector(sel + ' [data-seal-btn]').getBoundingClientRect(); for (let y = r.top + 6; y < r.bottom; y += 3) for (let x = r.right - 6; x > r.left; x -= 3) { if (x >= b.left && x <= b.right && y >= b.top && y <= b.bottom) continue; const e = document.elementFromPoint(x, y); if (e && e.closest('.seal') === s) return { x, y }; } return null; }, card);
    assert(rp, 'a point on the Complete Order seal clear of its button');
    await page.mouse.move(rp.x, rp.y, { steps: 3 }); await page.waitForFunction(sel => document.querySelector(sel + '[data-seal-zoom]'), rs + '.seal-button', { timeout: 3000 }); await X.settle();
    const rz = await X.probe(rs + '.seal-button');
    check(rz.size === 84 && rz.k === 1.85 && rz.inView && rz.rect.t >= rz.top && !rz.clipped.length, `Review card seal: 84px grown ×${rz.k}, in the view below the top bar (${rz.top.toFixed(0)}), not clipped ${JSON.stringify(rz.clipped)}`);
    check(await page.evaluate(() => !document.querySelector('.sealLens') && !document.querySelector('.seal[title]')), 'no lens, no caption, no tooltip on any seal of the page');
    if (shots) await page.screenshot({ path: path.join(shots, '1-review-card-seal.png'), clip: { x: 0, y: 40, width: 1440, height: 520 } });
    await page.mouse.click(rp.x, rp.y); await page.waitForTimeout(200);
    check(await page.evaluate(() => __prints) === 0, 'a click on a seal away from its button prints nothing (and the seal stays grown)');
    await page.mouse.move(5, 880); await X.gone();
    // …but the press over its button is still the button's
    const bp = await page.evaluate(sel => { const s = document.querySelector(sel + ' .sealRow .seal.seal-button'), r = s.getBoundingClientRect(), b = document.querySelector(sel + ' [data-seal-btn]').getBoundingClientRect(); for (let y = Math.max(r.top, b.top) + 3; y < Math.min(r.bottom, b.bottom); y += 3) for (let x = r.left + 3; x < r.right; x += 3) { if (x < b.left || x > b.right) continue; const e = document.elementFromPoint(x, y); if (e && e.closest('.seal') === s) return { x, y }; } return null; }, card);
    if (bp) { await page.mouse.click(bp.x, bp.y); await page.waitForTimeout(250); check(await page.evaluate(() => __prints) >= 1, 'a click on the seal where it lies over its button still presses the button'); await page.mouse.move(5, 880); await X.gone(); }

    // ═══ 6 · the order window ═══
    await page.evaluate(k => OrderWin.open(k), KEY);
    await page.waitForFunction(() => document.querySelectorAll('#owRail .tlStop .tlSeal').length > 3 && document.querySelectorAll('#owCustom .sealRow .seal').length >= 2, null, { timeout: 15000 });
    await page.waitForTimeout(1200); await page.mouse.move(700, 600);
    // the order window's custom-order bar: its seals were 1px wide
    const bar = await page.evaluate(() => [...document.querySelectorAll('#owCustom .sealRow .seal')].map(s => s.offsetWidth));
    check(bar.length >= 2 && bar.every(w => w >= 24), 'the custom-order bar\'s seals have a real size again: ' + bar.join(', ') + ' px');
    // 6a · the header strip: 24px seals in a 44px box that clips
    const stops = '#owRail .tlStop .tlSeal:not(.pending)', nStops = await page.evaluate(sel => document.querySelectorAll(sel).length, stops);
    const done = await page.evaluate(sel => [...document.querySelectorAll(sel)].map((s, i) => [i, s.closest('.tlStop').className]).filter(([, c]) => /\bd\b/.test(c)).map(([i]) => i), stops);
    assert(done.length >= 2, 'milestones are done in the strip');
    for (const i of [done[0], done[Math.floor(done.length / 2)], done[done.length - 1]]) {
      const base = await X.probe(stops, i), p = await X.point(stops, i); await page.mouse.move(5, 880); await page.mouse.move(p.x, p.y, { steps: 4 });
      await page.waitForFunction(([s, i]) => document.querySelectorAll(s)[i].dataset.sealZoom, [stops, i], { timeout: 4000 }); await X.settle();
      const z = await X.probe(stops, i);
      const pop = await page.evaluate(() => { const c = document.querySelector('.tlExp.on'); if (!c) return null; const r = c.getBoundingClientRect(); return { l: r.left, t: r.top, r: r.right, b: r.bottom }; });
      check(z.k >= 3 && z.size <= 30 && z.inView && !z.clipped.length, `header strip seal ${i} (${z.size}px, in a box 44px tall): grown ×${z.k} to ${z.w.toFixed(0)}px, in the view, nothing clips it ${JSON.stringify(z.clipped)}`);
      check(!pop || pop.t >= z.rect.b - 1 || pop.l >= z.rect.r - 1 || pop.r <= z.rect.l + 1, 'the step card (Engraved · done …) stays where it was and clear of the grown seal ' + JSON.stringify(pop));
      if (shots && i === done[Math.floor(done.length / 2)]) await page.screenshot({ path: path.join(shots, '2-header-strip-seal.png'), clip: { x: 0, y: 0, width: 1440, height: 330 } });
      await page.mouse.move(5, 880, { steps: 3 }); await X.gone();
      const back = await X.probe(stops, i);
      check(back.tf === base.tf && !back.running && await page.evaluate(() => ['owRail', 'orderWin'].every(id => !document.getElementById(id).style.overflow)), 'the strip is closed again, the seal as it was');
    }
    // 6b · the custom-order bar's seals (the Completed card of the order window)
    {
      const sel = '#owCustom .sealRow .seal', p = await X.point(sel, 0); await page.mouse.move(p.x, p.y, { steps: 3 });
      await page.waitForFunction(sel => document.querySelector(sel).dataset.sealZoom, sel, { timeout: 4000 }); await X.settle();
      const z = await X.probe(sel, 0);
      check(z.inView && !z.clipped.length && z.k >= 1.8, `the bar's ${z.size}px seal: grown ×${z.k}, in view, not clipped ${JSON.stringify(z.clipped)}`);
      if (shots) await page.screenshot({ path: path.join(shots, '3-completed-card-seal.png'), clip: { x: 0, y: 40, width: 1440, height: 440 } });
      await page.mouse.move(5, 880); await X.gone();
    }
    // 6c · the Timeline tab: lane stamps, the detail seal, the one at the left edge
    await page.click('#orderWin [data-ow-view="timeline"]'); await page.waitForSelector('#orderWin .tlSt[data-key]', { timeout: 8000 }); await page.waitForTimeout(900);
    const lanes = await page.evaluate(() => document.querySelectorAll('#orderWin .tlSt[data-key]').length);
    for (const [sel, i0] of [['#orderWin .tlSt[data-key]:not(.pending)', 0], ['#orderWin .tlSt[data-key]:not(.pending)', 1], ['#orderWin .tlBig', 0]]) {
      const n = (await page.evaluate(s => document.querySelectorAll(s).length, sel)); if (!n) { check(false, 'there are ' + sel); continue; }
      const i = Math.min(i0, n - 1), p = await X.point(sel, i); if (!p) { check(false, 'a point on ' + sel); continue; }
      await page.mouse.move(5, 880); await page.mouse.move(p.x, p.y, { steps: 4 });
      await page.waitForFunction(([s, i]) => document.querySelectorAll(s)[i].dataset.sealZoom, [sel, i], { timeout: 4000 }); await X.settle();
      const z = await X.probe(sel, i);
      check(z.inView && !z.clipped.length && z.k > 1.5, `Timeline ${sel.includes('tlSt') ? 'lane stamp ' + i + (i === 0 ? ' (at the left edge, the first)' : '') : 'detail seal'} (${z.size}px): grown ×${z.k}, in view, not clipped ${JSON.stringify(z.clipped)}`);
      if (shots && sel.includes('tlSt') && i === 1) await page.screenshot({ path: path.join(shots, '4-timeline-page-seal.png') });
      await page.mouse.move(5, 880, { steps: 3 }); await X.gone();
    }
    // 6d · the Overview's Now card seal, and the engraving approval seals in a window that clips (the sheet window's engraving inspector)
    await page.click('#orderWin [data-ow-view="info"]'); await page.waitForSelector('#orderWin .tlNowSeal', { timeout: 8000 }); await page.waitForTimeout(700);
    {
      const sel = '#orderWin .tlNowSeal', p = await X.point(sel, 0); await page.mouse.move(5, 880); await page.mouse.move(p.x, p.y, { steps: 4 });
      await page.waitForFunction(sel => document.querySelector(sel).dataset.sealZoom, sel, { timeout: 4000 }); await X.settle();
      const z = await X.probe(sel, 0);
      check(z.inView && !z.clipped.length && z.k >= 1.1 && z.size >= 60, `Now card seal (${z.size}px): grown ×${z.k}, in view, not clipped ${JSON.stringify(z.clipped)}`);
      await page.mouse.move(5, 880, { steps: 3 }); await X.gone();
    }
    await page.evaluate(() => {
      const at = Date.now() - 3 * 3600e3, d = document.createElement('dialog'); d.id = 'zsw'; d.className = 'sheetWin'; d.style.cssText = 'width:700px;height:420px;padding:0;border:0;overflow:hidden';
      const html = CNEngravingSeals.panel({ kind: 'approved', at, by: 'Paul', job: { key: 'k1', state: 'approved', approvedAt: at, approvedBy: 'Paul', seals: [{ how: 'engraveApproved', at: at - 3600e3, by: 'Anna' }, { how: 'engraveApproved', at, by: 'Paul' }] } });
      d.innerHTML = '<button type="button" id="zswBtn">·</button><div class="swEng" style="overflow:hidden;padding:30px;position:relative;height:340px">' + html + '</div>';
      document.body.appendChild(d); d.showModal(); Seal.fitGroups(d); document.getElementById('zswBtn').focus();
    });
    await page.waitForTimeout(500); await page.mouse.move(5, 880);
    for (const i of [1, 0]) {
      const sel = '#zsw .seal', p = await X.point(sel, i); await page.mouse.move(5, 880); await page.mouse.move(p.x, p.y, { steps: 4 });
      await page.waitForFunction(([sel, i]) => document.querySelectorAll(sel)[i].dataset.sealZoom, [sel, i], { timeout: 4000 }); await X.settle();
      const z = await X.probe(sel, i);
      check(z.inView && !z.clipped.length && z.k >= 1.6, `engraving approval seal ${i} in the clipped window (${z.size}px): grown ×${z.k}, in view, nothing clips it ${JSON.stringify(z.clipped)}`);
      if (shots && i === 1) await page.screenshot({ path: path.join(shots, '5-engrave-window-seal.png'), clip: { x: 340, y: 200, width: 760, height: 500 } });
      await page.mouse.move(5, 880, { steps: 3 }); await X.gone();
    }
    check(await page.evaluate(() => { const e = document.querySelector('#zsw .swEng'); return !e.style.zIndex && e.style.overflow === 'hidden' && document.getElementById('zsw').style.overflow === 'hidden'; }), 'the window\'s clipping is closed again after each zoom');
    await page.evaluate(() => { const d = document.getElementById('zsw'); d.close(); d.remove(); });
    check(lanes >= 3, 'the timeline drew its stamps (' + lanes + ')');
    check(await page.evaluate(() => window.__copies.length === 0 && !document.querySelector('.sealLens,.tlLoupe,.tlNowZoom')), 'no lens, loupe or copy of any seal was ever made on this page');
    assert.deepEqual(A.errors, [], 'no page errors');
    await A.context.close();

    // ═══ 7 · a finger and reduced motion ═══
    const B = await open({ hasTouch: true, reducedMotion: 'reduce', viewport: { width: 1100, height: 800 } }), Y = api(B.page);
    await B.page.evaluate(() => { const d = document.createElement('div'); d.id = 'zlab'; d.style.cssText = 'position:fixed;left:400px;top:360px;z-index:5'; d.innerHTML = Seal.html({ how: 'print', at: Date.now() - 3600e3, by: 'Paul' }, 84); document.body.appendChild(d); });
    const tp = await Y.point('#zlab .seal');
    await B.page.touchscreen.tap(tp.x, tp.y); await B.page.waitForFunction(() => document.querySelector('#zlab .seal[data-seal-zoom]'), null, { timeout: 1500 }); await B.page.waitForTimeout(250);
    const tz = await Y.probe('#zlab .seal'), dur = await B.page.evaluate(() => Math.max(0, ...document.getAnimations().map(a => a.effect && a.effect.getTiming().duration || 0)));
    check(tz.k === 1.85 && tz.inView, 'a tap zooms at once (×' + tz.k + ')');
    check(await B.page.evaluate(() => Math.max(0, ...[...document.querySelectorAll('#zlab .seal')[0].getAnimations()].map(a => +a.effect.getTiming().duration)) <= 130), 'with reduced motion it is a short fade, no bounce');
    await B.page.touchscreen.tap(tp.x, tp.y); await Y.gone();
    check((await Y.zoomed()) === 0, 'a second tap puts it back');
    await B.page.touchscreen.tap(tp.x, tp.y); await B.page.waitForFunction(() => document.querySelector('#zlab .seal[data-seal-zoom]'));
    await B.page.touchscreen.tap(40, 700); await Y.gone();
    check((await Y.zoomed()) === 0, 'a tap elsewhere puts it back');
    assert.deepEqual(B.errors, [], 'no page errors');
    await B.context.close();
  } finally { await browser.close(); srv.close(); }
  if (fails.length) { console.error('\nFAILED:\n - ' + fails.join('\n - ')); process.exit(1); }
  console.log('Seal zoom OK');
})().catch(e => { console.error(e); process.exit(1); });
