// The order window's Timeline tab has no detail pane (Paul, 5 Oct 2026, on the lower pane of the Timeline tab: "Please remove this entire UI
// and functionality from the 'Timeline' tab of the detail order view modal"): no big seal with its MILESTONE n OF m line, title, date and
// station chip, no field cards, no "Next for this order" with its requirement lines and its "All that … needs" link, no "Around this step"
// list, no selection, no Earlier / Later and no arrow-key walk, and no "next: ..." in the Now strip.  What stays is the chart: its lanes, days, route, NOW marker and seals.
// A click on a seal only grows it where it stands (Seal.zoom, at once); anything that used to open the pane (a rail step, the Overview
// card's seal, OrderWin.openOrder(rid, { highlight })) lands on the chart and only puts a thin ring in the accent round that seal.
//   fixture page + the fake backend (bridge-server.cjs; every other request is aborted), 390 / 900 / 1440 px wide, no console errors,
//   and a mutant that brings the pane back (a script that draws it under the chart again) is caught by the same checks.
//   node tests/charm-nest/timeline-no-detail-pane.cjs [playwright-core dir]      (SHOTS=<dir> keeps the screenshots)
const fs = require('fs'), path = require('path'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');
const pwDir = process.argv[2] || process.env.PW_DIR || path.join(root, 'node_modules');
const { chromium } = require(path.join(pwDir, 'playwright-core'));
const { start } = require('./bridge-server.cjs');
const SHOTS = process.env.SHOTS || '';
const sleep = ms => new Promise(r => setTimeout(r, ms));

const RID = '4176576272', LINE = '41765762721', K = `${RID}_${LINE}`;
const H = 3600e3, NOW = Date.now();
// what the pane drew, found by what it was made of (the elements, then its words): [] when the tab is the chart alone
const paneProblems = () => {
  const tab = document.querySelector('#owTimeline'), tools = document.querySelector('#owTlTools'), bad = [];
  for (const sel of ['.tlDetail', '.tlDetIn', '.tlDetMain', '.tlBig', '.tlAround', '.tlArw', '.tlStepReq', '.tlPin', '.tlPinH', '.tlPath2', '[data-pin]', '[data-unpin]', '[data-open-ev]', '[data-step]',
    '.tlActs', '.tlWhy', '.tlMeta', '.tlBA', '.tlBadgeRow', '.tlLegend', '.tlChip', '.tlStop.pinned']) {
    if (tab.querySelector(sel) || (tools && tools.querySelector(sel))) bad.push('element ' + sel);
  }
  const text = ((tab.textContent || '') + ' ' + (tools ? tools.textContent : '')).replace(/\s+/g, ' ');
  // (and no "next: ..." anywhere in the header's rail strip or the tab either, Paul 5 Oct point 4: the steps ahead are the rail's dots to show)
  const rail = document.querySelector('#owRail');
  if (/\bnext:/i.test(((rail ? rail.textContent : '') + ' ' + text).replace(/\s+/g, ' '))) bad.push('text next: …');
  for (const [name, re] of [['Around this step', /around this step/i], ['Next for this order', /next for this order/i], ['All that … needs', /all that .{1,40} needs/i],
    ['MILESTONE n OF m', /milestone \d+ of \d+/i], ['Earlier / Later', /‹ Earlier|Later ›/], ['The path', /\bthe path\b/i], ['Show the step', /show the step/i], ['Stamps chip', /^\s*Stamps\b/]]) {
    if (re.test(text)) bad.push('text ' + name);
  }
  // no field cards (PIECE, RECORDED BY, PRINTS, HOW, PRESSED IN, COMPLETED, SKU, TITLE) and no divider or empty bordered box under the chart
  const vis = el => getComputedStyle(el).display !== 'none';
  const ui = tab.querySelector('.tlUI:not(.compact)');
  if (!ui) bad.push('no timeline drawn');
  else {
    const kids = [...ui.children].filter(vis).map(c => c.className.split(' ')[0]);
    if (kids.join() !== 'tlGrid') bad.push('visible parts of the tab: ' + kids.join(',') + ' (the chart alone is expected)');
    const g = ui.querySelector('.tlGrid');
    if (g && parseFloat(getComputedStyle(g).borderBottomWidth) > 0) bad.push('a divider under the chart');
  }
  return bad;
};

(async () => {
  const srv = await start({ receipts: [] });
  const { sorterOrigin } = srv;
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  const ok = [];
  try {
    // ── the fixture: an order of one piece with four steps on the permanent timeline (its own events in the fake store) ──
    const rec = (type, at, extra) => srv.st.put('Order_Timeline', `${RID}~${type}~${extra.id || type}`, Object.assign({ orderId: RID, type, at, by: 'Paul', source: 'sorter', station: '', device: '', transactionId: '', sheetId: '', sheet: '', setId: '', milestone: false }, extra));
    rec('arrived', NOW - 50 * H, { by: 'Etsy', source: 'etsy', text: 'Order arrived from Etsy', id: 'e1' });
    rec('placed', NOW - 49 * H, { by: '', source: 'system', lineKey: K, sheet: 'RG Sheet 1', sheetId: 'sh-rg1', text: 'Football charm placed on RG Sheet 1', id: 'e2', data: { poolId: `${K}_1` } });
    rec('laserDone', NOW - 5 * H, { by: 'Marco R.', station: 'laser', lineKey: K, sheet: 'RG Sheet 1', sheetId: 'sh-rg1', text: 'RG Sheet 1 laser cut', id: 'e3' });
    rec('sorted', NOW - 2 * H, { by: 'Ana P.', source: 'station', station: 'sorting', device: 'sorting-1', lineKey: K, text: 'Sorted to the order', id: 'e4' });
    const SHIP = Math.floor(NOW / 1000) + 5 * 86400;
    const order = { receiptId: RID, orderNumber: RID, createTs: SHIP - 8 * 86400, updateTs: SHIP - 8 * 86400 + 60, shipBy: SHIP, buyer: { name: 'Jessica Strom' }, buyerMessage: '', isGift: false, giftMessage: '', staffNote: '', messages: [],
      lines: [{ transactionId: LINE, listingId: '1800000' + LINE.slice(-3), sku: 'FOOTBALL', title: 'Football Charm Sports Jewelry Add On Charm', quantity: 1, expectedShipDate: SHIP, variations: [{ name: 'Metal', value: 'Rose Gold' }], metalKey: '', metalLabel: '', personalization: [], buyerMessage: '' }] };

  // one page of the sorter with the order loaded; `mutant` is a script appended to the Timeline's own, to prove the checks catch a pane that comes back
    const open = async (width, height, mutant) => {
      const ctx = await browser.newContext({ viewport: { width, height } });
      await ctx.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => /fonts\.googleapis|fonts\.gstatic/.test(r.request().url()) ? r.fulfill({ status: 200, contentType: 'text/css', body: '' }) : r.abort());
      if (mutant) await ctx.route(/charm-nest-timeline-ui\.js/, async r => { const res = await r.fetch(); r.fulfill({ response: res, body: (await res.text()) + '\n' + mutant }); });
      await ctx.addInitScript(() => { try { localStorage.setItem('cn.settings', JSON.stringify({ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on' })); localStorage.setItem('cn.employee', 'Test Operator'); } catch (_) {} window.prompt = () => 'Test Operator'; });
      const page = await ctx.newPage(), errors = [];
      page.setDefaultTimeout(30000);
      page.on('pageerror', e => errors.push('page: ' + String(e.stack || e.message).split('\n').slice(0, 3).join(' | ')));
      page.on('console', m => { if (m.type() === 'error' && !/Failed to load resource|net::ERR/i.test(m.text())) errors.push('console: ' + m.text().slice(0, 300)); });
      await page.goto(`${sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
      await page.waitForFunction(() => window.CN && window.Orders && window.Review && window.OrderWin && window.OrderTimelineUI && window.Seal && CN.S.cloud.ok === true, null, { timeout: 60000 });
      await page.evaluate(async o => {
        await Orders.loadMaps(true);
        for (const line of o.lines) { const key = CharmNestOrders.lineKey(o, line); const row = { key, order: o, line, arrivedAt: Date.now(), spec: null, problems: [], state: 'pulled', reason: null, claimedBy: null, poolIds: [], engrave: null, material: null }; B.orders.rows.push(row); B.orders.byKey.set(key, row); }
        Orders.interpretAll(); Review.syncOrderItems(); CN.setMode('review'); Review.render();
      }, order);
      return { page, errors, ctx, shot: async name => { if (SHOTS) { fs.mkdirSync(SHOTS, { recursive: true }); await page.waitForTimeout(300); await page.screenshot({ path: path.join(SHOTS, name + '.png') }); } } };
    };
    const seals = page => page.evaluate(() => [...document.querySelectorAll('#owTimeline .tlSt[data-key]')].map(b => b.dataset.key));
    const settle = async page => { await page.waitForFunction(() => document.querySelectorAll('#owTimeline .tlSt[data-key]').length >= 4 && !document.querySelector('#owTimeline .tlSt.pending'), null, { timeout: 20000 }); await page.waitForTimeout(1200); };
    const ringed = page => page.evaluate(() => [...document.querySelectorAll('#owTimeline .tlSt.sel')].map(b => b.dataset.key || 'ghost:' + b.dataset.stage));
    const close = async page => { await page.evaluate(() => OrderWin.close()); await page.waitForFunction(() => !OrderWin.isOpen()); };

    /* ── 1 · 1440 px: the Timeline tab is the chart alone ── */
    const A = await open(1440, 900);
    const { page } = A;
    await page.evaluate(k => OrderWin.open(k, { view: 'timeline' }), K);
    await settle(page);
    assert.deepEqual(await seals(page), ['arrived~e1', 'placed~e2', 'laserDone~e3', 'sorted~e4'], 'the chart has the four seals');
    assert.deepEqual(await page.evaluate(paneProblems), [], 'no detail pane');
    const chart = await page.evaluate(() => ({ lanes: document.querySelectorAll('#owTimeline .tlLane').length, days: document.querySelectorAll('#owTimeline .tlDay').length, now: !!document.querySelector('#owTimeline .tlNowLine span'), route: !!document.querySelector('#owTimeline .tlPath path'),
      ghosts: document.querySelectorAll('#owTimeline .tlSt.ghost').length, idle: document.querySelectorAll('#owTimeline .tlDay.idle').length, rings: document.querySelectorAll('#owTimeline .tlSt.sel').length, lit: document.querySelectorAll('#owTimeline .tlLane.on').length, hl: document.querySelectorAll('#owTimeline .tlSt.hl').length }));
    assert.equal(chart.lanes, 7, 'the seven lanes'); assert(chart.days >= 2 && chart.now && chart.route, 'days, the NOW marker and the route stay: ' + JSON.stringify(chart)); assert(chart.ghosts >= 1, 'the steps to come stay as dashed seals');
    assert.equal(chart.rings + chart.lit + chart.hl, 0, 'nothing is selected on open (no ring, no lit lane): ' + JSON.stringify(chart));
    await A.shot('1-timeline-1440');
    ok.push('1440 px: the Timeline tab is the chart (7 lanes, days, route, NOW marker, 4 seals, dashed steps to come) and nothing under it: no big seal, no MILESTONE n OF m, no field cards, no Next for this order, no All that … needs, no Around this step, no Earlier / Later, no divider');

    /* ── 2 · a click on a seal only grows it where it stands, at once; nothing opens, nothing is selected ── */
    const snap = () => page.evaluate(() => ({ text: document.querySelector('#owTimeline').textContent.replace(/\s+/g, ' '), nodes: document.querySelectorAll('#owTimeline *').length, dialogs: document.querySelectorAll('dialog[open]').length, view: OrderWin.view(), ring: document.querySelectorAll('#owTimeline .tlSt.sel').length, lit: document.querySelectorAll('#owTimeline .tlLane.on').length }));
    const before = await snap();
    await page.mouse.move(5, 5);
    await page.click('#owTimeline .tlSt[data-key="placed~e2"]');
    const clicked = await page.evaluate(() => { const s = document.querySelector('#owTimeline .tlSt[data-key="placed~e2"]'); return { grown: !!s.dataset.sealZoom, others: document.querySelectorAll('[data-seal-zoom]').length, card: !!document.querySelector('.tlExp.on'), copy: !!document.querySelector('.tlLoupe,.tlNowZoom,.sealLens'), title: s.hasAttribute('title') }; });
    assert(clicked.grown && clicked.others === 1, 'a click grows that seal at once, the only one: ' + JSON.stringify(clicked));
    assert(!clicked.card && !clicked.copy && !clicked.title, 'no card, no second seal, no tooltip: ' + JSON.stringify(clicked));
    const after = await snap();
    assert.equal(after.text, before.text, 'the click changed no word of the tab'); assert.equal(after.dialogs, 1, 'no pop-up (the order window alone)'); assert.equal(after.view, 'timeline');
    assert.equal(after.ring + after.lit, 0, 'a click selects nothing: no ring, no lit lane'); assert.deepEqual(await page.evaluate(paneProblems), [], 'and draws no pane');
    await page.keyboard.press('Escape'); await page.waitForTimeout(400);
    assert.equal(await page.evaluate(() => document.querySelectorAll('[data-seal-zoom]').length), 0, 'Escape puts it back');
    // a dashed seal of a step to come: the same, nothing pinned
    await page.click('#owTimeline .tlSt.ghost[data-stage]');
    const g = await page.evaluate(() => ({ grown: document.querySelectorAll('[data-seal-zoom]').length, pane: !!document.querySelector('#owTimeline .tlPin, #owTimeline .tlDetail'), ring: document.querySelectorAll('#owTimeline .tlSt.sel').length }));
    assert.deepEqual(g, { grown: 1, pane: false, ring: 0 }, 'a dashed seal only grows too: ' + JSON.stringify(g));
    await page.mouse.move(700, 880); await page.waitForTimeout(500);
    // the arrow keys walk nothing (they used to step Earlier / Later); Tab reaches a seal and the keys leave it where it is
    await page.focus('#owTimeline .tlSt[data-key="laserDone~e3"]');
    await page.keyboard.press('ArrowRight'); await page.keyboard.press('ArrowLeft'); await page.waitForTimeout(150);
    const keys = await page.evaluate(() => ({ focus: document.activeElement.dataset.key, ring: document.querySelectorAll('#owTimeline .tlSt.sel').length, view: OrderWin.view() }));
    assert.deepEqual(keys, { focus: 'laserDone~e3', ring: 0, view: 'timeline' }, 'the arrow keys no longer walk the seals: ' + JSON.stringify(keys));
    await page.evaluate(() => document.activeElement.blur()); await page.mouse.move(700, 880); await page.waitForTimeout(400);
    ok.push('a click grows the seal in place at once (one grown seal, no card, no copy, no tooltip), changes no word of the tab, opens no pop-up and selects nothing; a dashed seal the same; Escape puts it back; ← → walk nothing');

    /* ── 3 · what used to open the pane lands on the chart: a ring on that seal ── */
    // the header rail's step (its seal on the chart), the next step (its dashed seal), the Overview card's seal and "Open timeline"
    await page.evaluate(() => OrderWin.setView('info')); await page.waitForTimeout(700);
    await page.click('#owRail .tlStop.d[data-stage="laser"]');
    await page.waitForFunction(() => OrderWin.view() === 'timeline' && document.querySelector('#owTimeline .tlSt.sel'), null, { timeout: 10000 }); await page.waitForTimeout(700);
    assert.deepEqual(await ringed(page), ['laserDone~e3'], 'a rail step opens the Timeline with that step\'s seal ringed');
    const ring = await page.evaluate(() => { const b = document.querySelector('#owTimeline .tlSt.sel'), c = getComputedStyle(b, '::before'); return { w: c.borderTopWidth, style: c.borderTopStyle, content: c.content, radius: c.borderTopLeftRadius, lane: document.querySelector('#owTimeline .tlLane.on').dataset.lane, pane: document.querySelectorAll('#owTimeline .tlDetail').length }; });
    assert.equal(ring.style, 'solid'); assert(parseFloat(ring.w) > 0 && parseFloat(ring.w) <= 2, 'a thin ring: ' + ring.w); assert.equal(ring.lane, 'sheet', 'the seal\'s lane is lit'); assert.equal(ring.pane, 0);
    assert.deepEqual(await page.evaluate(paneProblems), [], 'no pane after a rail click');
    await A.shot('2-rail-step-rings-its-seal');
    await page.evaluate(() => OrderWin.setView('info')); await page.waitForTimeout(700);
    const nextStep = await page.evaluate(() => document.querySelector('#owRail .tlStop.c').dataset.stage);
    await page.click('#owRail .tlStop.c');
    await page.waitForFunction(() => OrderWin.view() === 'timeline', null, { timeout: 10000 }); await page.waitForTimeout(700);
    assert.deepEqual(await ringed(page), ['ghost:' + nextStep], 'the next step opens the Timeline with its dashed seal ringed');
    assert.deepEqual(await page.evaluate(paneProblems), [], 'no pane for a step to come (it used to pin "All that … needs")');
    await page.evaluate(() => OrderWin.setView('info')); await page.waitForTimeout(700);
    await page.click('#owNowCard .tlNowSeal');
    await page.waitForFunction(() => OrderWin.view() === 'timeline', null, { timeout: 10000 }); await page.waitForTimeout(700);
    assert.deepEqual(await ringed(page), ['ghost:' + nextStep], 'the Overview card\'s seal opens the Timeline on its next step');
    assert.deepEqual(await page.evaluate(paneProblems), []);
    await page.evaluate(() => OrderWin.setView('info')); await page.waitForTimeout(700);
    await page.click('#owNowCard [data-go="timeline"]');
    await page.waitForFunction(() => OrderWin.view() === 'timeline', null, { timeout: 10000 }); await page.waitForTimeout(500);
    assert.deepEqual(await page.evaluate(paneProblems), [], '"Open timeline" opens the chart, no pane');
    ok.push(`a rail step, the next step (${nextStep}, its dashed seal), the Overview card's seal and "Open timeline" all land on the chart; the first two ring that seal (thin solid ring, its lane lit), none draws a pane`);

    /* ── 4 · OrderWin.openOrder(rid, { highlight }): the seal is ringed, the chart is all there is ── */
    await close(page);
    await page.evaluate(rid => OrderWin.openOrder(rid, { view: 'timeline', highlight: `${rid}~sorted~e4` }), RID);
    await settle(page);
    assert.deepEqual(await ringed(page), ['sorted~e4'], 'highlight: an event id rings that seal'); assert.deepEqual(await page.evaluate(paneProblems), []);
    await A.shot('3-highlight-rings-its-seal');
    await close(page);
    await page.evaluate(rid => OrderWin.openOrder(rid, { view: 'timeline', highlight: true }), RID);   // (a search: the number is lit, no event is asked for)
    await settle(page);
    assert.deepEqual(await ringed(page), [], 'highlight: true (a search) rings nothing'); assert.equal(await page.evaluate(() => document.querySelectorAll('#owTimeline .tlSt.hl').length), 0, 'and lights no seal');
    await close(page);
    await page.evaluate(rid => OrderWin.openOrder(rid, { view: 'timeline', highlight: 'RG Sheet 1' }), RID);   // (words: the seals that say them glow, the last is ringed)
    await settle(page);
    const hl = await page.evaluate(() => ({ glow: [...document.querySelectorAll('#owTimeline .tlSt.hl')].map(b => b.dataset.key), ring: [...document.querySelectorAll('#owTimeline .tlSt.sel')].map(b => b.dataset.key) }));
    assert.deepEqual(hl.glow, ['placed~e2', 'laserDone~e3'], 'highlight: words glow the seals that carry them'); assert.deepEqual(hl.ring, ['laserDone~e3'], 'and the last is ringed');
    assert.deepEqual(await page.evaluate(paneProblems), []);
    await close(page);
    ok.push('OrderWin.openOrder(rid, { highlight }): an event id rings that seal; true (a search) rings nothing; words glow the seals that carry them and ring the last; never a pane');

    /* ── 5 · 900 px and 390 px ── */
    for (const [w, h] of [[900, 800], [390, 844]]) {
      await page.setViewportSize({ width: w, height: h });
      await page.evaluate(k => OrderWin.open(k, { view: 'timeline' }), K);
      await settle(page);
      assert.deepEqual(await page.evaluate(paneProblems), [], `${w} px: no detail pane`);
      const m = await page.evaluate(() => ({ sw: document.documentElement.scrollWidth, iw: innerWidth, seals: document.querySelectorAll('#owTimeline .tlSt[data-key]').length, grid: document.querySelector('#owTimeline .tlGrid').getBoundingClientRect().width }));
      assert.equal(m.seals, 4, `${w} px: the seals`); assert(m.sw <= m.iw + 1, `${w} px: the page does not scroll sideways: ${JSON.stringify(m)}`);
      await page.click('#owTimeline .tlSt[data-key="sorted~e4"]');
      assert.equal(await page.evaluate(() => document.querySelectorAll('[data-seal-zoom]').length), 1, `${w} px: a click grows the seal`);
      assert.deepEqual(await page.evaluate(paneProblems), []);
      await page.mouse.move(2, h - 2); await page.keyboard.press('Escape'); await page.waitForTimeout(400);
      await A.shot(`4-timeline-${w}`);
      await close(page);
    }
    ok.push('900 px and 390 px: the same, the chart alone, no sideways page scroll, a click grows a seal');

    assert.deepEqual(A.errors, [], 'no page or console errors: ' + A.errors.join('\n'));
    await A.ctx.close();

    /* ── 6 · a mutant that brings the pane back is caught ── */
    // (a script after the Timeline's own: every mount draws a "Around this step" pane under the chart again, with a Next for this order block)
    const mutant = `(function(){const U=window.OrderTimelineUI,m=U.mount;U.mount=function(el,o){const h=m.call(this,el,o);if(!(o&&o.compact)){const box=el.querySelector('.tlUI');if(box){const d=document.createElement('div');d.className='tlDetail';d.innerHTML='<div class="tlDetIn"><span class="tlBig"></span><div class="tlDetMain"><span class="tlLbl">Order completed · milestone 3 of 4</span><div class="tlStepReq"><span class="tlLbl">Next for this order</span><button type="button" class="tlLink" data-pin="laser">All that Laser cut needs</button></div></div><div class="tlAround"><span class="tlLbl">Around this step</span></div></div>';box.appendChild(d);}}return h;};})();`;
    const Mu = await open(1440, 900, mutant);
    await Mu.page.evaluate(k => OrderWin.open(k, { view: 'timeline' }), K);
    await Mu.page.waitForFunction(() => document.querySelector('#owTimeline .tlDetail'), null, { timeout: 20000 });
    const caught = await Mu.page.evaluate(paneProblems);
    for (const want of ['element .tlDetail', 'element .tlAround', 'element .tlStepReq', 'element [data-pin]', 'text Around this step', 'text Next for this order', 'text All that … needs', 'text MILESTONE n OF m'])
      assert(caught.includes(want), `the mutant is caught by "${want}": ${JSON.stringify(caught)}`);
    await Mu.ctx.close();
    ok.push(`a mutant that draws the pane again under the chart is caught (${caught.length} findings: elements and words)`);
  } finally { await browser.close(); srv.close(); }
  console.log('Timeline without a detail pane OK:\n - ' + ok.join('\n - '));
  process.exit(0);
})().catch(e => { console.error(e); process.exit(1); });
