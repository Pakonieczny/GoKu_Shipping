// The Timeline tab's chart fills the screen, on its own, and keeps filling it (Paul, 5 Oct 2026, with the detail pane gone):
//  "Please expand the chart and enlarge the seals and all other components of the chart to fit the empty space below. Make sure that the user doesn't
//   have to scroll to see the full chart, the full chart should always be visible on one screen without scrolling, this needs to be dynamic in nature."
// Fixture: a multi-day order (two pieces, a Friday arrival, the weekend, then Monday's milestones on six lanes) and a one-seal order, in the real order
// window over the local fake backend (nothing live, no Etsy, no paid call). Proves, in headless Chromium:
//  - at 1440x900, 1280x720, 1920x1080, 2560x1440, 900x700 and 390x844 the chart's box is the tab's box (within 3 px), nothing in the tab scrolls (the order
//    window body, the tab, the chart), no seal is clipped, no two seals touch, the lane labels and the day heads and the NOW pill stay inside their places,
//    and the text stays between its floor and its ceiling
//  - the seals are bigger at 1920x1080 than at 1280x720, never wider than the cap (84 px, the shared seal size), and a taller screen spreads the lanes
//  - the chart re-fits live, within a frame or two: the window resized, the browser's zoom (the same thing to a page: its CSS pixels change), the order window's
//    own height changed (a taller header), a seal arriving, another order opened
//  - the first drawing does not glide and animates nothing but transform and opacity; a re-fit glides the seals by transform alone; under reduced motion nothing moves
//  - a seal still grows in place on a click (Seal.zoom), the way it did
//  - no page errors; and a mutant that keeps the chart at its old fixed sizes is caught by the very same check
//   node tests/charm-nest/timeline-fit-screen.cjs   (PW_DIR=<playwright-core's node_modules>, CHROMIUM=<chrome>, SHOTS=<dir>)
const fs = require('fs'), path = require('path'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');
const { start } = require('./bridge-server.cjs');

const DAY = 86400, SHIP = Math.floor(Date.now() / 1000) + 4 * DAY, HOUR = 3600e3;
const mkOrder = (rid, buyer, lines) => ({ receiptId: rid, orderNumber: rid, createTs: SHIP - 7 * DAY, updateTs: SHIP - 7 * DAY + 60, shipBy: SHIP, buyer: { name: buyer }, buyerMessage: '', isGift: false, giftMessage: '', staffNote: '', messages: [],
  lines: lines.map(([tid, sku, title, type]) => ({ transactionId: tid, listingId: '18' + tid.slice(-8), sku, title, quantity: 1, expectedShipDate: SHIP, variations: [{ name: 'Metal', value: 'Gold' }, { name: 'Type', value: type }], metalKey: '', metalLabel: '', personalization: '' })) });
const A = mkOrder('4176744752', 'Buyer 4752', [['41767447521', 'CHAIN_8941', 'CHAIN REPLACEMENT', 'Necklace'], ['41767447522', 'STUD_SNAKE', 'SNAKE STUD EARRINGS', 'Stud earrings']]);
const B = mkOrder('4176744800', 'Buyer 4800', [['41767448001', 'RING_12', 'RING 12', 'Ring']]);
const keyOf = (o, i) => `${o.receiptId}_${o.lines[i].transactionId}`;
const SIZES = [[1440, 900], [1280, 720], [1920, 1080], [2560, 1440], [900, 700], [390, 844]];

(async () => {
  const pwDir = process.argv[2] || process.env.PW_DIR || path.join(root, 'node_modules');
  let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { console.log('  - no playwright-core: not run'); return; }
  const shots = process.env.SHOTS || ''; if (shots) fs.mkdirSync(shots, { recursive: true });
  const srv = await start({ receipts: [] });
  // ── the permanent timelines: A over a weekend, on six lanes; B only the arrival ──
  const now = Date.now(), fri = (() => { const d = new Date(now - 4 * 864e5); d.setHours(21, 41, 0, 0); return +d; })();
  let n = 0;
  const rec = (o, type, at, extra) => srv.st.put('Order_Timeline', `${o.receiptId}~${type}~t${++n}`, Object.assign({ orderId: o.receiptId, type, at, by: 'Paul', source: 'sorter', station: '', device: '', transactionId: '', sheetId: '', sheet: '', setId: '', milestone: false, lineKey: '', text: type, data: {} }, extra || {}));
  rec(A, 'arrived', fri, { by: 'Etsy', source: 'etsy' });
  const L0 = keyOf(A, 0), L1 = keyOf(A, 1), T0 = A.lines[0].transactionId, T1 = A.lines[1].transactionId;
  rec(A, 'placed', now - 8 * HOUR, { sheet: 'RG Sheet 1', sheetId: 'sheet-rg1', lineKey: L0, transactionId: T0 });
  rec(A, 'placed', now - 7.8 * HOUR, { sheet: 'RG Sheet 1', sheetId: 'sheet-rg1', lineKey: L1, transactionId: T1 });
  rec(A, 'laserDone', now - 6 * HOUR, { sheet: 'RG Sheet 1', sheetId: 'sheet-rg1', lineKey: L0, transactionId: T0 });
  rec(A, 'laserDone', now - 5.9 * HOUR, { sheet: 'RG Sheet 1', sheetId: 'sheet-rg1', lineKey: L1, transactionId: T1 });
  rec(A, 'sealPrinted', now - 5 * HOUR, { lineKey: L0, transactionId: T0, station: 'sorter', data: { how: 'print', prints: 1 } });
  rec(A, 'sorted', now - 4 * HOUR, { station: 'sorting', lineKey: L0, transactionId: T0 });
  rec(A, 'sorted', now - 3.9 * HOUR, { station: 'sorting', lineKey: L1, transactionId: T1 });
  rec(A, 'welded', now - 3 * HOUR, { station: 'welding', lineKey: L1, transactionId: T1 });
  rec(A, 'assembled', now - 2 * HOUR, { station: 'assembly', lineKey: L0, transactionId: T0 });
  rec(B, 'arrived', now - 2 * HOUR, { by: 'Etsy', source: 'etsy' });
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  const errors = [];
  const boot = async (opts, mutate) => {
    const context = await browser.newContext(Object.assign({ viewport: { width: 1440, height: 900 }, deviceScaleFactor: 1 }, opts || {}));
    await context.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => /fonts\.googleapis|fonts\.gstatic/.test(r.request().url()) ? r.fulfill({ status: 200, contentType: 'text/css', body: '' }) : r.abort());
    if (mutate) await context.route(/charm-nest-timeline-ui\.js/, r => r.fulfill({ status: 200, contentType: 'text/javascript', body: mutate(fs.readFileSync(path.join(root, 'charm-nest-timeline-ui.js'), 'utf8')) }));
    await context.addInitScript(() => { try { localStorage.setItem('cn.settings', JSON.stringify({ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on' })); localStorage.setItem('cn.employee', 'Test Operator'); } catch (_) {} });
    const page = await context.newPage();
    page.setDefaultTimeout(30000);
    page.on('pageerror', e => { errors.push(e.message); console.error('page error:', e.message); });
    // (the fake backend answers a few calls it has no record of with a 400: that is a failed request of the page, not an error of the chart; a script error is one)
    page.on('console', m => { if (m.type() === 'error' && !/Failed to load resource/.test(m.text())) { errors.push(m.text()); console.error('console error:', m.text()); } });
    await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
    await page.waitForFunction(() => window.CN && window.Orders && window.Review && window.OrderWin && window.OrderTimelineUI && CN.S.cloud.ok === true, null, { timeout: 60000 });
    await page.evaluate(async orders => {
      OrderTimelineUI.pollOpenMs = 500;   // (the open order window reads again every half second here, so a seal that arrives shows quickly)
      for (const order of orders) for (const line of order.lines) { const key = CharmNestOrders.lineKey(order, line); const row = { key, order, line, arrivedAt: Date.now(), spec: null, problems: [], state: 'pulled', reason: null, claimedBy: null, poolIds: [], engrave: null, material: null }; B.orders.rows.push(row); B.orders.byKey.set(key, row); }
      Orders.interpretAll(); await Orders.loadMaps(true); Orders.interpretAll(); Review.syncOrderItems(); CN.setMode('review'); Review.render();
    }, [A, B]);
    return { context, page };
  };
  const frames = (page, k = 2) => page.evaluate(k => new Promise(res => { let i = 0; const f = () => ++i >= k ? res() : requestAnimationFrame(f); requestAnimationFrame(f); }), k);
  const openTimeline = async (page, key, want) => {
    await page.evaluate(k => OrderWin.open(k), key);
    await page.waitForFunction(k => OrderWin.key() === k && OrderWin._feed() && OrderWin._feed().answer, key, { timeout: 25000 });
    await page.waitForTimeout(1500);   // (the window has landed)
    await page.evaluate(() => OrderWin.setView('timeline'));
    await page.waitForFunction(w => document.querySelectorAll('#owTimeline .tlCanvas .tlSt[data-key]').length >= w, want, { timeout: 20000 });
    await page.mouse.move(3, 3); await page.waitForTimeout(700);
  };
  const closeWin = async page => { await page.evaluate(() => OrderWin.isOpen() && OrderWin.close()); await page.waitForFunction(() => !document.getElementById('orderWin').open, null, { timeout: 8000 }); await page.waitForTimeout(300); };
  /** Everything the chart is, as the page has it now (sizes from the layout, never from a transformed box). */
  const snap = page => page.evaluate(() => {
    const q = s => document.querySelector(s), R = e => { const r = e.getBoundingClientRect(); return { l: r.left, t: r.top, r: r.right, b: r.bottom, w: r.width, h: r.height }; };
    const tab = q('#orderWin .owVTime'), grid = q('#owTimeline .tlGrid'), sc = q('#owTimeline .tlScroll'), cv = q('#owTimeline .tlCanvas'), body = q('#orderWin .owBody'), box = q('#owTimeline .tlUI');
    const seals = [...cv.querySelectorAll('.tlSt')].map(b => ({ x: parseFloat(b.style.left), y: parseFloat(b.style.top), s: b.offsetWidth, ghost: b.classList.contains('ghost'), id: b.dataset.key || b.dataset.stage }));
    const cs = (e, p) => parseFloat(getComputedStyle(e)[p]);
    const lanes = [...document.querySelectorAll('#owTimeline .tlLane')].map(l => { const r = R(l), parts = [...l.children].filter(c => c.getClientRects().length).map(R); return { r, parts, name: l.querySelector('b').textContent.trim(), sub: !!(l.querySelector('span') && l.querySelector('span').getClientRects().length), font: cs(l.querySelector('b'), 'fontSize') }; });
    const heads = [...document.querySelectorAll('#owTimeline .tlDay .dh')].filter(h => h.getClientRects().length).map(h => ({ r: R(h), col: R(h.closest('.tlDay')), clip: h.scrollWidth - h.clientWidth, font: cs(h, 'fontSize'), small: !!(h.querySelector('small') && h.querySelector('small').getClientRects().length), text: h.textContent.trim() }));
    const idle = [...document.querySelectorAll('#owTimeline .tlDay.idle')].map(d => ({ col: R(d), label: R(d.querySelector('.dh')), text: d.textContent.trim() }));
    const pill = q('#owTimeline .tlNowLine span'), lanesBox = q('#owTimeline .tlLanes');
    return { vw: innerWidth, vh: innerHeight, tab: R(tab), grid: R(grid), sc: R(sc), cv: { w: cv.offsetWidth, h: cv.offsetHeight }, lanesBox: R(lanesBox), seals, lanes, heads, idle, pill: pill ? { r: R(pill), font: cs(pill, 'fontSize') } : null,
      scroll: { tab: [tab.scrollWidth - tab.clientWidth, tab.scrollHeight - tab.clientHeight], sc: [sc.scrollWidth - sc.clientWidth, sc.scrollHeight - sc.clientHeight], grid: [grid.scrollWidth - grid.clientWidth, grid.scrollHeight - grid.clientHeight], body: [body.scrollWidth - body.clientWidth, body.scrollHeight - body.clientHeight],
        doc: [document.scrollingElement.scrollWidth - innerWidth, document.scrollingElement.scrollHeight - innerHeight], overflowX: getComputedStyle(sc).overflowX },
      vars: { lane: cs(box, '--tl-lane-height'), t: box.style.getPropertyValue('--tl-t') }, laneH: lanes.length ? lanes[0].r.h : 0, route: cs(cv.querySelector('.tlPath path'), 'strokeWidth'), detail: !!q('#owTimeline .tlDetail, #owTimeline .tlBig, #owTimeline .tlArw'),
      over: (() => { const sr = sc.getBoundingClientRect(); return [...cv.querySelectorAll('*')].filter(e => { const r = e.getBoundingClientRect(); return r.width && (r.bottom > sr.bottom + .5 || r.right > sr.right + .5 || r.top < sr.top - .5); }).slice(0, 6).map(e => (e.className && e.className.baseVal !== undefined ? e.className.baseVal : e.className) + ':' + Math.round(e.getBoundingClientRect().bottom - sr.bottom)); })() };
  });
  /** The chart once everything in the order window that moves has ended (a seal in flight reaches past the box for a moment, and a machine under load plays a
   *  260 ms glide slower than the clock: the settled looks wait for the glides, never for a fixed time). */
  const calm = async page => {
    await page.evaluate(() => Promise.race([Promise.all(document.getAnimations().filter(a => { const t = a.effect && a.effect.target; return t && t.closest && t.closest('#orderWin') && a.effect.getComputedTiming().iterations !== Infinity; }).map(a => a.finished.catch(() => 0))), new Promise(r => setTimeout(r, 6000))]));
    await frames(page, 2); return snap(page);
  };
  /** What is wrong with the chart as it is now, for `what` (an empty list when nothing is). One function, so a mutant can be run through it. */
  const problems = (s, what, mid) => {   // mid: a re-fit that is still gliding (a seal in flight reaches past the box for a moment: only the layout is checked)
    const out = [], bad = m => out.push(`${what}: ${m}`);
    for (const e of ['l', 't', 'r', 'b']) if (Math.abs(s.grid[e] - s.tab[e]) > 3) bad(`the chart's ${{ l: 'left', t: 'top', r: 'right', b: 'bottom' }[e]} edge is ${s.grid[e].toFixed(1)}, the tab's is ${s.tab[e].toFixed(1)}`);
    if (Math.abs(s.cv.h - s.grid.h) > 3 || s.cv.w + s.lanesBox.w < s.grid.w - 3) bad(`the drawing (${s.cv.w}x${s.cv.h}) does not fill the chart (${s.grid.w - s.lanesBox.w}x${s.grid.h})`);
    if (s.scroll.tab[1] > 0 || s.scroll.tab[0] > 0) bad(`the tab scrolls: ${s.scroll.tab}`);
    if (!mid && (s.scroll.sc[0] > 0 || s.scroll.sc[1] > 0 || s.scroll.grid[0] > 0 || s.scroll.grid[1] > 0)) bad(`the chart scrolls: ${s.scroll.sc} ${s.scroll.grid} (over: ${s.over})`);
    if (!mid && (s.scroll.body[0] > 0 || s.scroll.body[1] > 0 || s.scroll.doc[0] > 0 || s.scroll.doc[1] > 0)) bad(`the window body or the page scrolls: ${s.scroll.body} ${s.scroll.doc}`);
    if (s.detail) bad('a detail pane is drawn under the chart');
    if (/auto|scroll/.test(s.scroll.overflowX)) bad(`the chart has a sideways scroll: ${s.scroll.overflowX}`);
    const c = s.seals.map(x => Object.assign({}, x)), cap = 84;
    if (c.length < 4) bad(`only ${c.length} seals drawn`);
    for (const x of c) {
      if (x.s > cap + .5) bad(`a seal is ${x.s}px, the cap is ${cap}`);
      if (x.x - x.s / 2 < -.5 || x.x + x.s / 2 > s.cv.w + .5 || x.y - x.s / 2 < -.5 || x.y + x.s / 2 > s.cv.h + .5) bad(`a seal is clipped: ${JSON.stringify(x)}`);
    }
    for (let i = 0; i < c.length; i++) for (let j = i + 1; j < c.length; j++) { const d = Math.hypot(c[i].x - c[j].x, c[i].y - c[j].y); if (d < (c[i].s + c[j].s) / 2 - .5) bad(`seals ${c[i].id} and ${c[j].id} overlap: ${d.toFixed(1)}px apart, ${(c[i].s + c[j].s) / 2}px wanted`); }
    s.lanes.forEach(l => {
      if (l.r.l < s.lanesBox.l - .5 || l.r.r > s.lanesBox.r + .5) bad(`lane ${l.name} sticks out of its column`);
      for (const p of l.parts) if (p.b > l.r.b + 1.5 || p.t < l.r.t - 1.5 || p.r > l.r.r + 1.5) bad(`lane ${l.name}'s label is clipped: ${JSON.stringify(p)} in ${JSON.stringify(l.r)}`);
      if (l.font < 8.4 || l.font > 16.1) bad(`lane ${l.name}'s text is ${l.font}px, outside 8.5 to 16`);
    });
    const first = s.lanes[0];
    for (const h of s.heads) { if (h.r.b > first.r.t + 1.5) bad(`a day head (${h.text}) runs into the first lane: ${h.r.b.toFixed(1)} > ${first.r.t.toFixed(1)}`); if (h.font < 8.9 || h.font > 17.1) bad(`the day head text is ${h.font}px, outside 9 to 17`); }
    for (const d of s.heads) if (d.clip > 1) bad(`a day head is cut off: ${d.text} (${d.clip}px)`);
    for (const h of s.heads) if (h.r.r > h.col.r - 2) bad(`a day head (${h.text}) runs out of its column: its words end at ${h.r.r.toFixed(1)}, the column at ${h.col.r.toFixed(1)}`);
    for (const d of s.idle) if (d.label.l < d.col.l - 1 || d.label.r > d.col.r + 1) bad(`an idle day's name (${d.text}) runs out of its column: ${d.label.l.toFixed(0)}-${d.label.r.toFixed(0)} in ${d.col.l.toFixed(0)}-${d.col.r.toFixed(0)}`);
    if (s.pill) { const p = s.pill.r; if (p.l < s.sc.l - 1 || p.r > s.sc.r + 1 || p.t < s.sc.t - 1 || p.b > s.sc.b + 1) bad(`the NOW pill is clipped: ${JSON.stringify(p)} in ${JSON.stringify(s.sc)}`); if (s.pill.font < 8.4 || s.pill.font > 15.1) bad(`the NOW pill text is ${s.pill.font}px`); }
    else bad('no NOW pill');
    return out;
  };
  const sealSize = s => Math.max(...s.seals.filter(x => !x.ghost).map(x => x.s));

  const { context: ctx, page } = await boot();
  try {
    // ── 1 · the first drawing: nothing glides, and only transform and opacity animate ──
    await page.evaluate(k => OrderWin.open(k), keyOf(A, 0));
    await page.waitForFunction(k => OrderWin.key() === k && OrderWin._feed() && OrderWin._feed().answer, keyOf(A, 0), { timeout: 25000 });
    await page.waitForTimeout(1500);
    await page.evaluate(() => {
      window.__anims = []; const seen = new Set();
      const sample = () => { for (const a of document.getAnimations()) { const t = a.effect && a.effect.target; if (!t || !t.closest || !t.closest('#owTimeline')) continue; const props = a.transitionProperty ? [a.transitionProperty] : (a.effect.getKeyframes ? a.effect.getKeyframes().flatMap(k => Object.keys(k)).filter(p => !/^(offset|easing|composite|computedOffset)$/.test(p)) : []); const glide = !!(t.classList && t.classList.contains('tlSt')) && !a.transitionProperty && !a.animationName; for (const p of props) seen.add(p + (glide ? ' [seal glide]' : '')); } window.__anims = [...seen]; window.__raf = requestAnimationFrame(sample); };
      sample();
    });
    await page.evaluate(() => OrderWin.setView('timeline'));
    await page.waitForFunction(() => document.querySelectorAll('#owTimeline .tlCanvas .tlSt[data-key]').length >= 6, null, { timeout: 20000 });
    await page.waitForTimeout(1200);
    const first = await page.evaluate(() => { cancelAnimationFrame(window.__raf); return window.__anims; });
    assert(first.every(p => /^(opacity|transform)$/.test(p)), `only transform and opacity animate while the chart first appears: ${first}`);
    assert(!first.some(p => /glide/.test(p)), `the seals do not glide on the first drawing: ${first}`);
    console.log(`  ✓ 1 · the first drawing animates only ${first.join(', ') || 'nothing'} and no seal glides`);
    await page.mouse.move(3, 3);

    // ── 2 · every size: the chart is the tab ──
    const byTag = {};
    for (const [w, h] of SIZES) {
      await page.setViewportSize({ width: w, height: h }); await page.waitForTimeout(900);
      const tag = `${w}x${h}`, s = byTag[tag] = await calm(page);
      const bad = problems(s, tag); assert.deepEqual(bad, [], bad.join('\n'));
      if (shots) await page.screenshot({ path: path.join(shots, `fit-${tag}.png`) });
      console.log(`  ✓ 2 · ${tag}: chart ${Math.round(s.grid.w)}x${Math.round(s.grid.h)} = the tab, lanes ${s.laneH}px, seals ${sealSize(s)}px, text scale ${s.vars.t}, route line ${s.route}px, no scroll anywhere, nothing overlaps or is clipped`);
    }
    const sz = t => sealSize(byTag[t]);
    assert(sz('1920x1080') > sz('1280x720'), `bigger seals on the bigger screen: ${sz('1920x1080')} vs ${sz('1280x720')}`);
    assert(sz('1440x900') > sz('1280x720') || sz('1440x900') >= 40, `1440x900 seals: ${sz('1440x900')}`);
    assert(sz('2560x1440') <= 84 && sz('2560x1440') >= sz('1920x1080'), `the cap holds on the tallest screen: ${sz('2560x1440')}`);
    assert(byTag['2560x1440'].laneH > byTag['1920x1080'].laneH && byTag['1920x1080'].laneH > byTag['1280x720'].laneH, 'a taller screen spreads the lanes');
    assert(byTag['390x844'].lanes.every(l => l.r.w < 140) && sz('390x844') >= 20, `a phone: narrow label column, seals ${sz('390x844')}px`);
    assert(byTag['1920x1080'].route > byTag['1280x720'].route, 'the route line grows with the chart');
    console.log(`  ✓ 2 · seals ${sz('1280x720')}px at 1280x720, ${sz('1920x1080')}px at 1920x1080, ${sz('2560x1440')}px at 2560x1440 (cap 84); lanes ${byTag['1280x720'].laneH} < ${byTag['1920x1080'].laneH} < ${byTag['2560x1440'].laneH}px`);

    // short screens: the labels give way before the seals (fewer axis labels, never a clipped one)
    for (const [w, h] of [[1280, 520], [844, 390], [667, 375], [390, 520], [800, 300], [600, 260]]) {
      await page.setViewportSize({ width: w, height: h }); await page.waitForTimeout(700);
      const tag = `${w}x${h}`, s = await calm(page), bad = problems(s, tag); assert.deepEqual(bad, [], bad.join('\n'));
      assert(sealSize(s) >= (h < 280 ? 12 : 17), `${tag}: the seals stay readable: ${sealSize(s)}px`);
      if (shots) await page.screenshot({ path: path.join(shots, `short-${tag}.png`) });
      console.log(`  ✓ 2 · short ${tag}: chart ${Math.round(s.grid.w)}x${Math.round(s.grid.h)} = the tab, lanes ${s.laneH}px, seals ${sealSize(s)}px, ${s.heads.some(h => h.small) ? 'both lines of the day heads' : 'day heads of one line'}, ${s.lanes.some(l => l.sub) ? 'lane names with who' : 'lane names alone'}`);
    }

    // ── 3 · it re-fits live, in a frame or two ──
    const refit = async (what, change, tag) => {
      const before = await snap(page);
      await change(); await frames(page, 2);
      const after = await snap(page), bad = problems(after, what + ' (two frames after)', true);
      assert.deepEqual(bad, [], bad.join('\n'));
      await page.waitForTimeout(600);
      const settled = problems(await calm(page), what + ' (settled)'); assert.deepEqual(settled, [], settled.join('\n'));
      return { before, after };
    };
    await page.setViewportSize({ width: 1280, height: 720 }); await page.waitForTimeout(900);
    let r = await refit('resized to 1920x1080', () => page.setViewportSize({ width: 1920, height: 1080 }));
    assert(sealSize(r.after) > sealSize(r.before), `the seals grew within two frames: ${sealSize(r.before)} -> ${sealSize(r.after)}`);
    r = await refit('resized back to 1280x720', () => page.setViewportSize({ width: 1280, height: 720 }));
    assert(sealSize(r.after) < sealSize(r.before), `the seals shrank within two frames: ${sealSize(r.before)} -> ${sealSize(r.after)}`);
    console.log(`  ✓ 3 · a window resize re-fits within two frames: seals ${sealSize(r.before)} -> ${sealSize(r.after)}px, still the tab, still no scroll`);
    // the browser's zoom is the page's CSS pixels changing: 150% leaves 853 CSS px, 67% leaves 1910 and more
    r = await refit('zoomed in (a quarter of the pixels)', () => page.setViewportSize({ width: 853, height: 480 }));
    r = await refit('zoomed out', () => page.setViewportSize({ width: 1910, height: 1070 }));
    console.log('  ✓ 3 · browser zoom (the page\'s CSS pixels change) re-fits too, at 853x480 and at 1910x1070');
    await page.setViewportSize({ width: 1440, height: 900 }); await page.waitForTimeout(900);
    // the order window's own height: a taller header takes room from the tab
    await page.evaluate(() => { const s = document.createElement('style'); s.id = 'tallHead'; s.textContent = '#orderWin .owHead{padding-bottom:200px!important}'; document.head.appendChild(s); });
    await frames(page, 2);
    const tall = await snap(page), tb = problems(tall, 'a taller header', true); assert.deepEqual(tb, [], tb.join('\n'));
    await page.waitForTimeout(600); { const t2 = problems(await calm(page), 'a taller header (settled)'); assert.deepEqual(t2, [], t2.join('\n')); }
    const base1440 = byTag['1440x900'];
    assert(tall.grid.h < base1440.grid.h - 100, `the taller header took room from the tab: ${base1440.grid.h} -> ${tall.grid.h}`);
    await page.evaluate(() => document.getElementById('tallHead').remove()); await frames(page, 2);
    const back = await snap(page), bb = problems(back, 'the header back', true); assert.deepEqual(bb, [], bb.join('\n'));
    await page.waitForTimeout(600); { const b2 = problems(await calm(page), 'the header back (settled)'); assert.deepEqual(b2, [], b2.join('\n')); }
    assert(Math.abs(back.grid.h - base1440.grid.h) <= 2, 'the chart took the room back');
    console.log(`  ✓ 3 · the order window's own height: tab ${Math.round(base1440.grid.h)} -> ${Math.round(tall.grid.h)} -> ${Math.round(back.grid.h)}px, the chart follows each`);
    // a seal arrives: the chart takes it in and re-fits
    const had = (await snap(page)).seals.filter(x => !x.ghost).length;
    srv.st.put('Order_Timeline', `${A.receiptId}~shipped~t99`, { orderId: A.receiptId, type: 'shipped', at: Date.now() - 60e3, by: 'Paul', source: 'station', station: 'shipping', device: '', transactionId: T0, sheetId: '', sheet: '', setId: '', milestone: true, lineKey: L0, text: 'shipped', data: {} });
    await page.waitForFunction(h => document.querySelectorAll('#owTimeline .tlCanvas .tlSt[data-key]').length > h, had, { timeout: 15000 });
    await page.waitForTimeout(1600);
    const arrived = await calm(page), ab = problems(arrived, 'a seal arrived'); assert.deepEqual(ab, [], ab.join('\n'));
    assert(arrived.seals.filter(x => !x.ghost).length === had + 1, 'the new seal is drawn');
    console.log(`  ✓ 3 · a seal arrives (${had} -> ${had + 1}): the chart takes it in and still fills the tab`);
    await closeWin(page);
    await openTimeline(page, keyOf(B, 0), 1);
    const other = await calm(page), ob = problems(other, 'another order'); assert.deepEqual(ob.filter(m => !/only \d+ seals drawn/.test(m)), [], ob.join('\n'));
    assert(other.seals.filter(x => !x.ghost).length === 1, 'the one-seal order draws one seal');
    console.log(`  ✓ 3 · another order (one seal, no days): ${Math.round(other.grid.w)}x${Math.round(other.grid.h)} chart, seals ${sealSize(other)}px`);
    await closeWin(page);
    await openTimeline(page, keyOf(A, 0), 6);

    // ── 4 · a re-fit glides by transform alone, and a seal still grows where it stands ──
    await page.setViewportSize({ width: 1440, height: 900 }); await page.waitForTimeout(900);
    await page.evaluate(() => { window.__g = new Set(); const f = () => { for (const a of document.getAnimations()) { const t = a.effect && a.effect.target; if (!t || !t.closest || !t.closest('#owTimeline')) continue; const props = a.transitionProperty ? [a.transitionProperty] : (a.effect.getKeyframes ? a.effect.getKeyframes().flatMap(k => Object.keys(k)).filter(p => !/^(offset|easing|composite|computedOffset)$/.test(p)) : []); for (const p of props) window.__g.add(p + (t.classList.contains('tlSt') && !a.transitionProperty && !a.animationName ? ' [glide]' : '')); } window.__gr = requestAnimationFrame(f); }; f(); });
    await page.setViewportSize({ width: 1180, height: 760 }); await page.waitForTimeout(500);
    const glid = await page.evaluate(() => { cancelAnimationFrame(window.__gr); return [...window.__g]; });
    assert(glid.every(p => /^(opacity|transform)/.test(p)), `a re-fit animates only transform and opacity: ${glid}`);
    assert(glid.includes('transform [glide]'), `the seals glide to their new places on a re-fit: ${glid}`);
    console.log(`  ✓ 4 · a re-fit animates ${glid.join(', ')}: transform and opacity only, the seals glide`);
    await page.setViewportSize({ width: 1440, height: 900 }); await page.waitForTimeout(900);
    const mid = await page.evaluate(() => { const b = [...document.querySelectorAll('#owTimeline .tlCanvas .tlSt[data-key]')][3], r = b.getBoundingClientRect(); return { x: r.left + r.width / 2, y: r.top + r.height / 2, w: b.offsetWidth }; });
    await page.mouse.move(mid.x, mid.y); await page.mouse.down(); await page.mouse.up(); await page.waitForTimeout(900);
    const zoomed = await page.evaluate(() => { const z = document.querySelector('#owTimeline .sealZoomed'); if (!z) return null; const r = z.getBoundingClientRect(); return { w: z.offsetWidth, shown: r.width }; });
    assert(zoomed, 'a click on a chart seal grows it in place (Seal.zoom)');
    assert(zoomed.shown >= mid.w - 1 && zoomed.shown <= 96, `the grown seal is gentle: ${mid.w}px -> ${zoomed.shown.toFixed(1)}px`);
    console.log(`  ✓ 4 · a click still grows a seal in place: ${mid.w}px -> ${zoomed.shown.toFixed(1)}px (the zoom's own gentle curve and its 72 px cap in the timeline)`);
    await page.mouse.move(3, 3); await page.waitForTimeout(500);

    assert.deepEqual(errors, [], 'no page or console errors: ' + errors.join(' | '));
  } finally { await ctx.close(); }

  // ── 5 · reduced motion: a re-fit moves nothing ──
  {
    const { context, page: p2 } = await boot({ reducedMotion: 'reduce' });
    try {
      await openTimeline(p2, keyOf(A, 0), 6);
      await p2.setViewportSize({ width: 1180, height: 760 }); await frames(p2, 2);
      const moving = await p2.evaluate(() => document.getAnimations().filter(a => { const t = a.effect && a.effect.target; return t && t.closest && t.closest('#owTimeline') && !a.transitionProperty && !a.animationName && a.playState === 'running' && a.effect.getComputedTiming().activeDuration > 5; }).length);
      assert.equal(moving, 0, 'nothing glides under reduced motion');
      const s = await calm(p2), bad = problems(s, 'reduced motion 1180x760'); assert.deepEqual(bad, [], bad.join('\n'));
      console.log('  ✓ 5 · under reduced motion a re-fit is instant: no seal glides, the chart still fits');
    } finally { await context.close(); }
  }

  // ── 6 · a mutant that keeps the old fixed sizes (a 360 px chart of 26 px seals) is caught by the very same check ──
  {
    const { context, page: p3 } = await boot({}, src => { const m = src.replace('h = Math.max(120, +h || 360); w = Math.max(200, +w || 800);', 'h = 360; w = 800; cap = 26;'); assert.notEqual(m, src, 'the mutant applies'); return m; });
    try {
      await openTimeline(p3, keyOf(A, 0), 6);
      await p3.setViewportSize({ width: 1920, height: 1080 }); await p3.waitForTimeout(900);
      const s = await calm(p3), bad = problems(s, 'mutant 1920x1080');
      assert(bad.length > 0, 'the fixed-size mutant is caught');
      console.log(`  ✓ 6 · a mutant with fixed sizes is caught (${bad.length} problems, e.g. ${bad[0].slice(0, 90)})`);
    } finally { await context.close(); }
  }
  assert.deepEqual(errors, [], 'no errors: ' + errors.join(' | '));
  await browser.close(); srv.close();
  console.log('Timeline fit-to-screen OK');
})().catch(e => { console.error(e); process.exit(1); });
