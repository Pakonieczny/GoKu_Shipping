// The dots of each piece's row in the order window are circles with a card (Paul, 5 Oct 2026, point 5): "Enable each of these solid and hollow green dots
// into an active hover state like the other milestone timeline dots that showcase a bit of info regarding each milestone. Incorporate a 350 ms delay for the
// hover state so that it's not annoying when the user is quickly moving their mouse across the screen."
// Proved here in headless Chromium on the real page over the fake backend (bridge-server.cjs: nothing live, no Etsy, no paid call): order 4170837249 (Leslie
// Suhr, three pieces: CABLE CHAIN ONLY in Review, a GF piece that was nested, laser cut and sorted by a person, an RG piece that was only nested).
//  1  what is drawn: six dots to a row, each a labelled zoom dot of the shared engine ("Nested, done", "Laser cut, not yet"), one Tab stop per row, no
//     native title, the same solid/hollow as before (done = solid), and a hit area a little wider than the dot (the gap between two dots is not dead)
//  2  the 350 ms rest: a quick pass over the whole row (200 ms) opens nothing, a rest of 400 ms opens the card (measured: 330 to 480 ms after the pointer came),
//     the dot grows gently (transform only: ~x1.7), nothing shifts; leaving puts both back
//  3  once a card was open the next dot opens after a short beat (not the whole 350), a pointer that left the row and came back waits the whole 350 again
//  4  a click opens it at once and holds it (a second click, Esc, a scroll or moving away puts it back); a tap too; the click never selects the row or
//     reaches the row's own press
//  5  the card: a done step says its name, "Done", when and who ("Oct 3 · 4:12 PM · Ana P. at Sorting", the shop's time), where; a step still to do says
//     "Not yet" and what it waits on; a step further on says what it comes after; never the word "lines"
//  6  the keyboard: Tab opens the card at once, the arrows move along the row, Esc closes it, Enter or Space on the dot too
//  7  the card never takes a press: the row's buttons (Hold, Print QR label, Complete Order) are what is under every point of it, and it never sits over the
//     buttons of the row it belongs to (above the dot, below it only when there is no room)
//  8  the seal slot: window.PieceSeals.render(stepKey, pieceCtx, {done}) is asked when the card opens and what it returns is drawn in the card; without it
//     the card has no slot
//  9  a redraw under the pointer (the step is done while the card is open): the card follows to the new dot and says Done
// 10  1440, 900 and 390: the card fits the screen and the page is not made wider; reduced motion: no growth, the card still shows
// 11  a mutant with no delay is caught by check 2
// 12  (C1, PiecePlacement) the dots follow where the pieces are NOW: a held order of two pieces that were nested and then taken off their sheets has one solid dot (Order
//     in), Nested hollow, its card says "Not on a sheet now", "Was on GF Sheet 2 until Paul took it off, <when>" and "On hold", the real ON SHEET seal stays in it (a seal
//     is history), every later card says "After Nested"; a held piece the history shows laser cut and sorted is not clamped; a mutant that ignores the placement is caught
//   node tests/charm-nest/piece-row-dot-hover.cjs   (PW_DIR=<playwright-core's node_modules>, CHROMIUM=<chrome>, SHOTS=<dir> saves screenshots)
const fs = require('fs'), path = require('path'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');
const { start } = require('./bridge-server.cjs');

const DAY = 86400, SHIP = Math.floor(Date.UTC(2026, 9, 10, 17) / 1000);
const P = { rid: '4170837249', cable: '41708372491', gf: '41708372492', rg: '41708372493' };
const GF1 = 'sheet-pd-gf1', RG1 = 'sheet-pd-rg1';
const kOf = (o, t) => `${o.rid}_${o[t]}`, pidOf = (o, t) => `${o.rid}_${o[t]}_1`;
const line = (tid, sku, metalKey, metalLabel) => ({ transactionId: tid, listingId: '19008' + tid.slice(-5), sku, title: sku + ' necklace', quantity: 1, expectedShipDate: SHIP, variations: [{ name: 'Metal', value: metalLabel }], metalKey, metalLabel, personalization: [], buyerMessage: '' });
const order = (rid, buyer, lines) => ({ receiptId: rid, orderNumber: rid, createTs: SHIP - 7 * DAY, updateTs: SHIP - 7 * DAY + 60, shipBy: SHIP, buyer: { name: buyer }, buyerMessage: '', isGift: false, giftMessage: '', staffNote: '', messages: [], lines });
const ORDERS = [[order(P.rid, 'Leslie Suhr', [line(P.cable, 'CABLE CHAIN ONLY', 'rose', 'Rose Gold Filled'), line(P.gf, 'MIDDLE_9935', 'gold', '14k Gold Filled'), line(P.rg, 'MIDDLE_9935', 'rose', 'Rose Gold Filled')]), [null, pidOf(P, 'gf'), pidOf(P, 'rg')]]];
// two more orders for check 14 (C1: the dots follow where the pieces are NOW): HELD, two pieces nested on sheets and then taken off by the Hold (the sheets' records are
// gone, the line carries the hold marker); and PAST, two held pieces, one the history shows laser cut and sorted (a piece cannot un-cut: its dots are not clamped)
const HELD = { rid: '4170999001', a: '41709990011', b: '41709990012' }, PAST = { rid: '4170999002', a: '41709990021', b: '41709990022' };
ORDERS.push([order(HELD.rid, 'Hadley Holden', [line(HELD.a, 'MIDDLE_9935', 'gold', '14k Gold Filled'), line(HELD.b, 'MIDDLE_9935', 'rose', 'Rose Gold Filled')]), [null, null], 'held by Test Operator']);
ORDERS.push([order(PAST.rid, 'Percy Past', [line(PAST.a, 'MIDDLE_9935', 'gold', '14k Gold Filled'), line(PAST.b, 'MIDDLE_9935', 'rose', 'Rose Gold Filled')]), [null, null], 'Add to next sheet']);
const sleep = ms => new Promise(r => setTimeout(r, ms));
// the shop's time, as the card says it: "Oct 3 · 4:12 PM"
const when = at => { const d = new Date(at), f = o => new Intl.DateTimeFormat('en-US', Object.assign({ timeZone: 'America/Toronto' }, o)).format(d); return `${f({ month: 'short', day: 'numeric' })} · ${f({ hour: 'numeric', minute: '2-digit' }).replace(/ /g, ' ')}`; };

async function main() {
  const pwDir = process.env.PW_DIR || path.join(root, 'node_modules');
  let chromium;
  for (const d of [process.env.PW_DIR, '/opt/node22/lib/node_modules/playwright', path.join(root, 'node_modules/playwright'), path.join(root, 'node_modules/playwright-core')].filter(Boolean)) { for (const n of [d, path.join(d, 'playwright-core'), path.join(d, 'playwright')]) { try { ({ chromium } = require(n)); if (chromium) break; } catch (_) {} } if (chromium) break; }
  if (!chromium) { try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) {} }
  if (!chromium) { console.log('  - no playwright: the browser checks were not run'); return; }
  const shots = process.env.SHOTS || null; if (shots) fs.mkdirSync(shots, { recursive: true });
  const srv = await start({ receipts: [] });
  const box = (id, cx, cy) => ({ id, cxPt: cx, cyPt: cy, angle: 0, wPt: 34, hPt: 34 });
  const ch = (id, o, t, sku) => ({ id, name: `${o.rid} · ${sku}`, poolId: pidOf(o, t), order: o.rid, sku });
  srv.st.put('Charm_Nest_Sheets', GF1, { id: GF1, metal: 'gold', sheetIndex: 1, day: '2026-10-04', status: 'written', stock: { wPt: 300, hPt: 140 }, orders: [P.rid], placements: [box('g1', 60, 60)], charms: [ch('g1', P, 'gf', 'MIDDLE_9935')] });
  srv.st.put('Charm_Nest_Sheets', RG1, { id: RG1, metal: 'rose', sheetIndex: 1, day: '2026-10-04', status: 'written', stock: { wPt: 200, hPt: 140 }, orders: [P.rid], placements: [box('r1', 60, 60)], charms: [ch('r1', P, 'rg', 'MIDDLE_9935')] });
  const poolRow = (o, t, sku, material, sheetId, metal) => srv.st.put('Charm_Pool', pidOf(o, t), { poolId: pidOf(o, t), orderId: o.rid, transactionId: o[t], lineKey: kOf(o, t), sku, material, copy: 1, quantity: 1, state: 'written', sheetId, sheetName: `2026-10-04_${metal}_Set-1_Sheet-1`, updatedAt: Date.now() });
  poolRow(P, 'gf', 'MIDDLE_9935', 'gold', GF1, 'GF'); poolRow(P, 'rg', 'MIDDLE_9935', 'rose', RG1, 'RG');
  // the order's timeline: arrived; each piece on its sheet; the GF sheet cut by Paul at the laser; the GF piece sorted by Ana P. at the Sorting station
  const evAt = Date.now(); let evN = 0; const AT = {};
  const ev = (type, ago, extra) => { const at = evAt - ago * 60000; srv.st.put('Order_Timeline', `${P.rid}~${type}~e${++evN}`, Object.assign({ orderId: P.rid, type, at, by: 'Test Operator', source: 'sorter', station: '', text: '', data: {} }, extra || {})); AT[type + (extra && extra.lineKey ? ':' + extra.lineKey : '')] = at; return at; };
  ev('arrived', 4000, { source: 'etsy', by: 'Etsy' });
  ev('placed', 3000, { sheet: 'GF Sheet 1', sheetId: GF1, lineKey: kOf(P, 'gf'), data: { poolId: pidOf(P, 'gf') } });
  ev('placed', 2900, { sheet: 'RG Sheet 1', sheetId: RG1, lineKey: kOf(P, 'rg'), data: { poolId: pidOf(P, 'rg') } });
  ev('laserDone', 2000, { sheet: 'GF Sheet 1', sheetId: GF1, by: 'Paul', source: 'station', station: 'laser' });
  ev('sorted', 1000, { by: 'Ana P.', source: 'station', station: 'sorting', lineKey: kOf(P, 'gf') });
  // HELD: arrived; each piece on a sheet (GF Sheet 2 / RG Sheet 2, no record of either now); taken off by Paul's Hold, 5 and 4 minutes apart; the order held
  const evO = (o, type, ago, extra) => { const at = evAt - ago * 60000; srv.st.put('Order_Timeline', `${o.rid}~${type}~e${++evN}`, Object.assign({ orderId: o.rid, type, at, by: 'Test Operator', source: 'sorter', station: '', text: '', data: {} }, extra || {})); return at; };
  const GH = 'sheet-pd-held-gf', RH = 'sheet-pd-held-rg', HOLD_AT = {};
  evO(HELD, 'arrived', 4000, { source: 'etsy', by: 'Etsy' });
  evO(HELD, 'placed', 3000, { sheet: 'GF Sheet 2', sheetId: GH, lineKey: kOf(HELD, 'a'), data: { poolId: pidOf(HELD, 'a') } });
  evO(HELD, 'placed', 2990, { sheet: 'RG Sheet 2', sheetId: RH, lineKey: kOf(HELD, 'b'), data: { poolId: pidOf(HELD, 'b') } });
  HOLD_AT.a = evO(HELD, 'removed', 300, { by: 'Paul', sheet: 'GF Sheet 2', sheetId: GH, lineKey: kOf(HELD, 'a'), text: 'Taken off GF Sheet 2: order on hold', data: { reason: 'Order on hold', poolId: pidOf(HELD, 'a') } });
  HOLD_AT.b = evO(HELD, 'removed', 299, { by: 'Paul', sheet: 'RG Sheet 2', sheetId: RH, lineKey: kOf(HELD, 'b'), text: 'Taken off RG Sheet 2: order on hold', data: { reason: 'Order on hold', poolId: pidOf(HELD, 'b') } });
  evO(HELD, 'held', 298, { by: 'Paul', text: 'Order on hold', data: { reason: 'Order on hold' } });
  evO(PAST, 'arrived', 4000, { source: 'etsy', by: 'Etsy' });
  evO(PAST, 'placed', 3000, { sheet: 'GF Sheet 3', sheetId: 'sheet-pd-past-gf', lineKey: kOf(PAST, 'a'), data: { poolId: pidOf(PAST, 'a') } });
  evO(PAST, 'laserDone', 2000, { sheet: 'GF Sheet 3', sheetId: 'sheet-pd-past-gf', lineKey: kOf(PAST, 'a'), by: 'Paul', source: 'station', station: 'laser' });
  evO(PAST, 'sorted', 1000, { by: 'Ana P.', source: 'station', station: 'sorting', lineKey: kOf(PAST, 'a') });
  evO(PAST, 'placed', 2990, { sheet: 'RG Sheet 3', sheetId: 'sheet-pd-past-rg', lineKey: kOf(PAST, 'b'), data: { poolId: pidOf(PAST, 'b') } });
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  const errors = [];
  const open = async (opts = {}) => {
    const context = await browser.newContext(Object.assign({ viewport: { width: 1440, height: 900 } }, opts));
    await context.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => {
      const u = r.request().url();
      if (/qrcodejs/.test(u)) return r.fulfill({ status: 200, contentType: 'text/javascript', headers: { 'Access-Control-Allow-Origin': '*' }, body: fs.readFileSync(path.join(root, 'lib/qrcode.min.js')) });
      if (/fonts\.googleapis|fonts\.gstatic/.test(u)) return r.fulfill({ status: 200, contentType: 'text/css', body: '' });
      return r.abort();
    });
    await context.addInitScript(() => { try { if (!sessionStorage.getItem('__seeded')) { localStorage.setItem('cn.settings', JSON.stringify({ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on' })); localStorage.setItem('cn.employee', 'Test Operator'); sessionStorage.setItem('__seeded', '1'); } } catch (_) {} window.prompt = () => 'Test Operator'; });
    const page = await context.newPage();
    page.setDefaultTimeout(30000);
    page.on('pageerror', e => { errors.push(e.message); console.error('page error:', String(e.stack || e.message).split('\n').slice(0, 4).join(' | ')); });
    page.on('console', m => { if (m.type() === 'error' && !/Failed to load resource|net::ERR/.test(m.text())) { errors.push(m.text()); console.error('console error:', m.text().slice(0, 200)); } });
    await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
    await page.waitForFunction(() => window.CN && window.Orders && window.Review && window.Seal && window.OrderWin && window.OrderTimeline && window.RailTip && CN.S.cloud.ok === true, null, { timeout: 60000 });
    await page.evaluate(async ({ orders }) => {
      await Orders.loadMaps(true);
      B.master.entries.set('MIDDLE_9935', { sku: 'MIDDLE_9935', updatedAt: 1 });
      for (const [order, pools, held] of orders) order.lines.forEach((line, i) => { const key = CharmNestOrders.lineKey(order, line); const row = { key, order, line, arrivedAt: Date.now(), spec: null, problems: [], state: held ? 'held' : pools[i] ? 'pooled' : 'pulled', reason: held || null, hold: held || null, claimedBy: null, poolIds: pools[i] ? [pools[i]] : [], engrave: null, material: null }; B.orders.rows.push(row); B.orders.byKey.set(key, row); });
      Orders.interpretAll(); Review.syncOrderItems(); CN.setMode('review'); Review.render();
      window.OrderTimelineUI.pollOpenMs = 700;   // (the open window's feed reads often: the redraw check below does not wait long)
    }, { orders: ORDERS });
    await page.waitForFunction(() => document.querySelectorAll('#rvList .reviewListRow').length >= 1, null, { timeout: 20000 });
    await page.evaluate(k => OrderWin.open(k), kOf(P, 'cable'));
    await page.waitForFunction(() => document.querySelectorAll('#owPcSum .owPcRow').length === 3 && document.querySelectorAll('#owPcSum .steps i[data-pdot]').length === 18 && !!OrderWin.isOpen(), null, { timeout: 20000 });
    // the events have to be in (the dots follow them): the GF piece has four solid dots
    await page.waitForFunction(k => document.querySelectorAll(`#owPcSum .steps i.on[data-pdot-piece="${k}"]`).length >= 4, kOf(P, 'gf'), { timeout: 20000 });
    await page.evaluate(() => {   // (the window's own opening motion is over; the page's timers of this test)
      window.__t = {}; document.addEventListener('pointerdown', e => { if (e.target.closest && e.target.closest('[data-pdot]')) window.__t.down = performance.now(); }, true); document.addEventListener('focusin', e => { if (e.target.closest && e.target.closest('[data-pdot]')) window.__t.focus = performance.now(); }, true); document.addEventListener('pointerover', e => { const d = e.target.closest && e.target.closest('[data-pdot]'); if (d) window.__t.over = performance.now(); }, true);
      document.addEventListener('dotzoom', e => { if (e.detail && e.detail.on && e.detail.el.hasAttribute('data-pdot')) { window.__t.open = performance.now(); window.__t.opened = (window.__t.opened || 0) + 1; } }, true);
    });
    await sleep(600);
    return { context, page };
  };
  // the dot of a piece's row, its centre
  const dot = (key, step) => `#owPcSum i[data-pdot="${step}"][data-pdot-piece="${key}"]`;
  const centre = async (page, sel) => { await page.evaluate(sel => document.querySelector(sel).scrollIntoView({ block: 'center', inline: 'nearest' }), sel); await sleep(80); const b = await (await page.$(sel)).boundingBox(); return { x: b.x + b.width / 2, y: b.y + b.height / 2, b }; };
  // what the card and the dot are now
  const read = (page, sel) => page.evaluate(sel => {
    const d = document.querySelector(sel), r = d.getBoundingClientRect(), t = document.querySelector('.railTip'), tr = t && t.getBoundingClientRect(), cs = getComputedStyle(d), on = !!(t && t.hasAttribute('data-on') && getComputedStyle(t).visibility === 'visible' && +getComputedStyle(t).opacity > 0.5);
    return { k: +(r.width / d.offsetWidth).toFixed(3), cls: d.className, shown: on, step: d.getAttribute('data-pdot'),
      tip: on ? { name: t.querySelector('b')?.textContent, state: t.querySelector('.rtState')?.textContent, line: t.querySelector('.rtLine')?.textContent || '', by: t.querySelector('.rtBy')?.textContent || '', seal: t.querySelector('.rtSeal')?.innerHTML || '', text: t.textContent.replace(/\s+/g, ' ').trim(), below: t.classList.contains('below'), l: tr.left, t: tr.top, r: tr.right, b: tr.bottom, w: tr.width, h: tr.height, ax: parseFloat(t.style.getPropertyValue('--ax')), pe: getComputedStyle(t).pointerEvents, inDialog: !!t.closest('dialog') } : null,
      dot: { l: r.left, t: r.top, r: r.right, b: r.bottom }, vw: innerWidth, vh: innerHeight, scrollW: document.documentElement.scrollWidth, ring: cs.boxShadow };
  }, sel);
  // (polls: the card fades in over 150 ms)
  const shownWithin = async (page, sel, ms) => { const t0 = Date.now(); while (Date.now() - t0 < ms) { if ((await read(page, sel)).shown) return true; await sleep(40); } return false; };
  const goneWithin = async (page, sel, ms = 4000) => { const t0 = Date.now(); while (Date.now() - t0 < ms) { const m = await read(page, sel); if (!m.shown && m.k < 1.05) return true; await sleep(40); } return false; };
  const away = async (page, ms = 450) => { await page.mouse.move(8, 8, { steps: 3 }); await sleep(ms); };
  const rest = async (page, sel) => { const c = await centre(page, sel); await page.mouse.move(c.x - 60, c.y - 30); await page.mouse.move(c.x, c.y, { steps: 4 }); await shownWithin(page, sel, 6000); await sleep(60); return c; };
  // 2 · a quick pass over the whole row opens nothing (the check a mutant with no delay must fail); quick is under 150 ms to a dot, far under the 350 asked
  const quickPass = async (page, key) => {
    const first = await centre(page, dot(key, 'arrived')), last = await (async () => { const b = await (await page.$(dot(key, 'shipped'))).boundingBox(); return { x: b.x + b.width / 2, y: b.y + b.height / 2 }; })();
    await page.mouse.move(first.x - 30, first.y); await page.evaluate(() => { window.__t.opened = 0; });
    const t0 = Date.now(); await page.mouse.move(last.x + 4, last.y, { steps: 12 }); const used = Date.now() - t0;
    await page.mouse.move(8, 8); await sleep(120);   // (gone before a rest of 350 could pass)
    const m = await page.evaluate(() => ({ opened: window.__t.opened || 0, shown: !!document.querySelector('.railTip[data-on]'), grown: document.querySelectorAll('#owPcSum .sealZoomed').length }));
    assert(used < 6 * 150, `the pass over the row was quick: under 150 ms to a dot (${used} ms for six)`);
    assert.deepEqual(m, { opened: 0, shown: false, grown: 0 }, 'a quick pass over the dots opens nothing: ' + JSON.stringify(m));
  };
  const rowsInfo = page => page.evaluate(() => [...document.querySelectorAll('#owPcSum .owPcRow')].map(r => ({ key: (r.querySelector('[data-pdot]') || { getAttribute: () => '' }).getAttribute('data-pdot-piece'), dots: [...r.querySelectorAll('i[data-pdot]')].map(d => ({ step: d.getAttribute('data-pdot'), on: d.classList.contains('on'), label: d.getAttribute('aria-label'), tab: d.getAttribute('tabindex'), role: d.getAttribute('role'), title: d.getAttribute('title'), zoom: d.getAttribute('data-zoom-dot'), delay: d.getAttribute('data-zoom-delay'), group: d.getAttribute('data-zoom-group'), rid: d.getAttribute('data-pdot-rid') })), btns: [...r.querySelectorAll('button')].map(b => b.textContent.trim()).filter(Boolean) })));
  // (how many rows are the selected piece: a press on a dot must select none)
  const nRows = page => page.evaluate(() => document.querySelectorAll('#owPcSum .owPcRow.sel').length);

  try {
    const { context, page } = await open();
    const gf = kOf(P, 'gf'), rg = kOf(P, 'rg'), cable = kOf(P, 'cable');

    // ── 1 · what is drawn ──
    { const rows = await rowsInfo(page);
      assert.equal(rows.length, 3, 'three rows');
      const by = Object.fromEntries(rows.map(r => [r.key, r]));
      assert.deepEqual(by[gf].dots.map(d => d.step), ['arrived', 'sheet', 'laser', 'sorted', 'assembled', 'shipped'], 'six dots, in the order of the steps');
      assert.deepEqual(by[gf].dots.map(d => d.on), [true, true, true, true, false, false], 'the GF piece: four solid, two hollow (what the dots always meant)');
      assert.deepEqual(by[rg].dots.map(d => d.on), [true, true, false, false, false, false]); assert.deepEqual(by[cable].dots.map(d => d.on), [true, false, false, false, false, false]);
      assert.deepEqual(by[gf].dots.map(d => d.label), ['Order in, done', 'Nested, done', 'Laser cut, done', 'Sorted, done', 'Assembled, not yet', 'Shipped, not yet'], 'each dot says its step and its state');
      for (const r of rows) {
        assert.equal(r.dots.filter(d => d.tab === '0').length, 1, `one Tab stop in the row of ${r.key}`);
        for (const d of r.dots) { assert.equal(d.role, 'img'); assert.equal(d.title, '', 'no native tooltip (the card says it, an empty title overrides the row\'s own)'); assert.equal(d.delay, '350'); assert(+d.zoom > 1 && +d.zoom <= 2, 'grows gently'); assert.equal(d.rid, P.rid); assert(d.group.endsWith('|' + r.key)); }
      }
      assert.equal(by[gf].dots.find(d => d.tab === '0').step, 'assembled', 'the Tab stop is the step the piece works towards');
      // a hit area wider than the dot: no dead gap between two dots, and clear of the row's buttons
      const hit = await page.evaluate(gf => { const ds = [...document.querySelectorAll(`#owPcSum i[data-pdot-piece="${gf}"]`)].map(d => d.getBoundingClientRect()), out = []; for (let i = 0; i < ds.length - 1; i++) { const mid = (ds[i].right + ds[i + 1].left) / 2, y = (ds[i].top + ds[i].bottom) / 2, t = document.elementFromPoint(mid - 0.2, y), u = document.elementFromPoint(mid + 0.2, y); out.push([t && t.getAttribute('data-pdot'), u && u.getAttribute('data-pdot')]); } return out; }, gf);
      assert(hit.every(([a, b]) => a && b), 'a point in the gap between two dots is on a dot, not dead: ' + JSON.stringify(hit)); }

    // ── 2 · the 350 ms rest ──
    await quickPass(page, gf);
    { const sel = dot(gf, 'sorted'), c = await centre(page, sel);
      await page.evaluate(() => { window.__t.opened = 0; window.__t.open = 0; });
      await page.mouse.move(c.x - 60, c.y - 30); await page.mouse.move(c.x, c.y, { steps: 3 });
      let m = await read(page, sel); assert.equal(m.shown, false, 'no card as the pointer arrives'); assert(m.k < 1.02, `no growth yet (${m.k})`);
      assert.equal(await shownWithin(page, sel, 3000), true, 'the card opens');
      const ms = await page.evaluate(() => window.__t.open - window.__t.over);
      assert(ms >= 330 && ms <= 900, `opened ${Math.round(ms)} ms after the pointer came (350 asked, so nothing at 200)`);
      await sleep(250); m = await read(page, sel);
      assert(m.k >= 1.5 && m.k <= 1.9, `grown gently in place (${m.k})`); assert.match(m.ring, /rgb/, 'a thin ring'); assert.equal(m.tip.inDialog, true, 'the card is in the window\'s layer, so it shows over it');
      assert.equal(await page.evaluate(sel => getComputedStyle(document.querySelector(sel)).transform !== 'none', sel), true, 'the growth is a transform');
      await away(page, 100); assert.equal(await goneWithin(page, sel), true, 'leaving puts both back (the card of a done step waits a moment for its seal)'); }

    // ── 3 · the next dot opens after a short beat once a card was open; the whole 350 again after the row was left (measured in the page, so load does not matter) ──
    { const a = dot(gf, 'laser'), b = dot(gf, 'sorted'), ca = await centre(page, a), cb = await (async () => { const bb = await (await page.$(b)).boundingBox(); return { x: bb.x + bb.width / 2, y: bb.y + bb.height / 2 }; })();
      const opened = async () => { const t0 = Date.now(); while (Date.now() - t0 < 2500) { const r = await page.evaluate(() => window.__t.open > window.__t.over ? window.__t.open - window.__t.over : -1); if (r >= 0) return r; await sleep(30); } return -1; };
      await page.mouse.move(ca.x - 40, ca.y - 20); await page.mouse.move(ca.x, ca.y, { steps: 3 }); await sleep(520); assert.equal((await read(page, a)).shown, true);
      await page.mouse.move(cb.x, cb.y, { steps: 3 }); let ms = await opened();
      assert(ms >= 0 && ms < 300, `the next dot opened after a short beat, not the whole delay (${Math.round(ms)} ms after the pointer came)`); assert.equal(await shownWithin(page, b, 1000), true); assert.equal((await read(page, b)).tip.name, 'Sorted');
      await away(page, 700); await page.mouse.move(cb.x, cb.y, { steps: 3 }); ms = await opened();
      assert(ms >= 330, `a pointer that left the row and came back waits the whole delay (${Math.round(ms)} ms)`); await away(page); }

    // ── 4 · a click opens at once and holds; a second click, Esc or a scroll puts it back; it never selects the row ──
    { const sel = dot(gf, 'laser'), c = await centre(page, sel), n0 = await nRows(page), picked = () => nRows(page);
      await page.mouse.move(c.x - 60, c.y - 30); await page.mouse.move(c.x, c.y, { steps: 2 }); await page.evaluate(() => { window.__t.open = 0; }); await page.mouse.down(); await page.mouse.up(); assert.equal(await shownWithin(page, sel, 1500), true, 'a click opens the card');
      const atOnce = await page.evaluate(() => window.__t.open - window.__t.down); assert(atOnce >= 0 && atOnce < 200, `at once, not after the rest (${Math.round(atOnce)} ms after the press)`);
      await sleep(300); let m = await read(page, sel); assert(m.k >= 1.5);
      assert.equal(await picked(), 0, 'a click on a dot does not select the row'); assert.equal(n0, 0);
      await sleep(900); m = await read(page, sel); assert.equal(m.shown, true, 'held while the pointer stays');
      await page.mouse.down(); await page.mouse.up(); assert.equal(await goneWithin(page, sel), true, 'a second click puts it back');
      await page.mouse.down(); await page.mouse.up(); assert.equal(await shownWithin(page, sel, 1500), true);
      await page.keyboard.press('Escape'); assert.equal(await goneWithin(page, sel), true, 'Esc puts it back'); assert(await page.evaluate(() => OrderWin.isOpen()), 'and Esc did not close the window');
      await page.mouse.move(c.x + 1, c.y + 1); await page.mouse.down(); await page.mouse.up(); assert.equal(await shownWithin(page, sel, 1500), true);
      await page.evaluate(() => document.querySelector('.owBody').dispatchEvent(new Event('scroll'))); assert.equal(await goneWithin(page, sel), true, 'a scroll puts it back');
      await page.mouse.move(c.x, c.y); await page.mouse.down(); await page.mouse.up(); await shownWithin(page, sel, 1500); await page.mouse.move(c.x + 220, c.y + 90, { steps: 4 });
      assert.equal(await goneWithin(page, sel), true, 'moving away puts it back'); assert.equal(await picked(), 0);
      // (the same detector sees a real selection: a press on the row's name picks the piece, and again puts it back)
      await away(page); await page.click(`#owPcSum .owPcRow[data-piece="${gf}"] .owPcName`); assert.equal(await picked(), 1, 'the detector: a press on the name selects the row'); await page.click(`#owPcSum .owPcRow[data-piece="${gf}"] .owPcName`); assert.equal(await picked(), 0); await away(page); }

    // ── 5 · the card's words ──
    { // a done step: who, where and when
      let sel = dot(gf, 'sorted'); await rest(page, sel, 650); let m = await read(page, sel);
      assert.equal(m.tip.name, 'Sorted'); assert.equal(m.tip.state, 'Done'); assert.equal(m.tip.by, `${when(AT['sorted:' + gf])} · Ana P. at Sorting`, 'when and who and where: ' + m.tip.by);
      await away(page);
      sel = dot(gf, 'laser'); await rest(page, sel, 650); m = await read(page, sel);
      assert.equal(m.tip.state, 'Done'); assert.equal(m.tip.by, `${when(AT.laserDone)} · Paul at Laser`); assert.equal(m.tip.line, 'On GF Sheet 1'); await away(page);
      sel = dot(gf, 'arrived'); await rest(page, sel, 650); m = await read(page, sel); assert.equal(m.tip.name, 'Order in'); assert.equal(m.tip.state, 'Done'); assert.match(m.tip.by, new RegExp('^' + when(AT.arrived).replace(/[.*+?^${}()|[\]\\]/g, '\\$&') + ' · Etsy')); await away(page);
      // a step still to do: what it waits on
      sel = dot(gf, 'assembled'); await rest(page, sel, 650); m = await read(page, sel);
      assert.equal(m.tip.name, 'Assembled'); assert.equal(m.tip.state, 'Not yet'); assert(m.tip.line.length > 8, 'it says what it waits on: ' + m.tip.line); assert.equal(m.tip.by, '', 'no who and when for a step nobody did'); assert.doesNotMatch(m.tip.text, /\blines?\b/i, 'pieces, never lines');
      assert.match(m.tip.line, /^[A-Z]/); await away(page);
      // a step further on: the one it comes after
      sel = dot(gf, 'shipped'); await rest(page, sel, 650); m = await read(page, sel); assert.equal(m.tip.state, 'Not yet'); assert.equal(m.tip.line, 'After Assembled'); await away(page);
      sel = dot(rg, 'laser'); await rest(page, sel, 650); m = await read(page, sel); assert.equal(m.tip.state, 'Not yet'); assert(m.tip.line.length > 8, 'the RG piece\'s laser step says what it waits on: ' + m.tip.line); await away(page);
      sel = dot(cable, 'sheet'); await rest(page, sel, 650); m = await read(page, sel); assert.equal(m.tip.name, 'Nested'); assert.equal(m.tip.state, 'Not yet'); assert(m.tip.line.length > 8, 'the piece in Review says what it waits on: ' + m.tip.line); await away(page);
      assert.equal(await page.evaluate(() => document.querySelectorAll('[title]:not([title=""])').length && [...document.querySelectorAll('#owPcSum [data-pdot]')].filter(d => d.title).length), 0, 'no native tooltip on a dot'); }

    // ── 6 · the keyboard ──
    { await page.evaluate(() => { document.activeElement && document.activeElement.blur(); document.getElementById('owPcSum').scrollIntoView({ block: 'center' }); }); await away(page, 100);
      const first = dot(gf, 'assembled');   // (the Tab stop of the GF row)
      let found = false; await page.evaluate(() => { const rows = [...document.querySelectorAll('#owPcSum .owPcRow')]; const b = rows[1].querySelector('button, [tabindex="0"]'); b && b.focus && b.focus(); });
      for (let i = 0; i < 12 && !found; i++) { await page.keyboard.press('Tab'); found = await page.evaluate(() => !!(document.activeElement && document.activeElement.matches && document.activeElement.matches('#owPcSum [data-pdot]'))); }
      assert(found, 'Tab reaches a dot'); { const tt = await page.evaluate(() => ({ open: window.__t.open, focus: window.__t.focus, el: document.activeElement && document.activeElement.getAttribute('data-pdot'), shown: !!document.querySelector('.railTip[data-on]') })); assert(Math.abs(tt.open - tt.focus) < 80, 'the card opened with the focus, at once (not after a rest): ' + JSON.stringify(tt)); } await sleep(250);
      const name = () => page.evaluate(() => { const t = document.querySelector('.railTip[data-on]'); return t ? t.querySelector('b').textContent : ''; });
      // (polls: the card fades, and a busy machine is slow)
      const nameIs = async want => { const t0 = Date.now(); let n; while (Date.now() - t0 < 4000) { n = await name(); if (n === want) return n; await sleep(40); } return n; };
      const step = () => page.evaluate(() => document.activeElement && document.activeElement.getAttribute('data-pdot'));
      assert(await name(), 'the card is open as soon as the dot has the keyboard (200 ms)');
      await page.keyboard.press('Home'); assert.equal(await nameIs('Order in'), 'Order in'); assert.equal(await step(), 'arrived');
      await page.keyboard.press('ArrowRight'); assert.equal(await nameIs('Nested'), 'Nested'); assert.equal(await step(), 'sheet');
      await page.keyboard.press('End'); assert.equal(await nameIs('Shipped'), 'Shipped'); assert.equal(await step(), 'shipped');
      await page.keyboard.press('ArrowLeft'); assert.equal(await nameIs('Assembled'), 'Assembled'); assert.equal(await step(), 'assembled');
      const stops = await page.evaluate(gf => [...document.querySelectorAll(`#owPcSum i[data-pdot-piece="${gf}"]`)].filter(d => d.tabIndex === 0).map(d => d.getAttribute('data-pdot')), gf); assert.deepEqual(stops, ['assembled'], 'one Tab stop, where the keyboard is');
      await page.keyboard.press('Escape'); assert.equal(await nameIs(''), '', 'Esc closes the card'); assert(await page.evaluate(() => OrderWin.isOpen()), 'the window stays');
      await page.keyboard.press('Enter'); assert.equal(await nameIs('Assembled'), 'Assembled', 'Enter on the dot opens the card'); await sleep(400); await page.keyboard.press('Enter'); assert.equal(await nameIs(''), '', 'and Enter again puts it away');
      await page.keyboard.press('Space'); assert.equal(await nameIs('Assembled'), 'Assembled', 'Space too'); await page.keyboard.press('Escape'); await nameIs('');
      await page.evaluate(() => document.activeElement && document.activeElement.blur()); }

    // ── 7 · the card never takes a press and never sits over its own row's buttons ──
    { const sel = dot(gf, 'sorted'); await rest(page, sel, 650); const m = await read(page, sel); assert.equal(m.shown, true); assert.equal(m.tip.pe, 'none');
      const hits = await page.evaluate(() => { const t = document.querySelector('.railTip'), r = t.getBoundingClientRect(), out = []; for (const b of document.querySelectorAll('#owPcSum button')) { const q = b.getBoundingClientRect(); if (!(q.width > 0)) continue; const x = q.left + q.width / 2, y = q.top + q.height / 2, over = x >= r.left && x <= r.right && y >= r.top && y <= r.bottom, el = document.elementFromPoint(x, y); out.push({ text: b.textContent.trim(), over, press: !!(el && (el === b || b.contains(el))) }); } return out; });
      assert(hits.length >= 3, 'the rows\' buttons are there: ' + JSON.stringify(hits)); assert(hits.every(h => h.press), 'every button of every row is what is under its own centre, even where the card is: ' + JSON.stringify(hits.filter(h => !h.press)));
      const own = await page.evaluate(gf => { const t = document.querySelector('.railTip').getBoundingClientRect(), row = document.querySelector(`#owPcSum i[data-pdot-piece="${gf}"]`).closest('.owPcRow'); return [...row.querySelectorAll('button')].map(b => b.getBoundingClientRect()).filter(q => q.width > 0).filter(q => !(q.right < t.left || q.left > t.right || q.bottom < t.top || q.top > t.bottom)).length; }, gf);
      assert.equal(own, 0, 'the card is clear of the buttons of its own row'); assert(!m.tip.below || m.tip.t >= m.dot.b, 'above the dot, or below it when there is no room');
      // (a click on a button under the card's place still reaches the button: the Hold of the row above opens the hold flow, which this test does not press; the click is delivered to it)
      const t = await page.evaluate(() => { const b = [...document.querySelectorAll('#owPcSum .holdBtn')][0]; window.__held = 0; b.addEventListener('click', e => { window.__held++; e.stopImmediatePropagation(); e.preventDefault(); }, true); const q = b.getBoundingClientRect(); return { x: q.left + q.width / 2, y: q.top + q.height / 2 }; });
      await page.mouse.click(t.x, t.y); assert.equal(await page.evaluate(() => window.__held), 1, 'a click on the row\'s Hold arrives at the Hold'); await away(page); }

    // ── 8 · the seal slot: the hook (a stand-in module), then the real PieceSeals (the real seal of a done step, the unfinished one of a step to come) ──
    { await page.evaluate(() => { window.__real = window.PieceSeals; window.__asked = []; window.PieceSeals = { render: (step, ctx, o) => { window.__asked.push({ step, key: ctx && ctx.p && ctx.p.key, events: Array.isArray(ctx && ctx.events), pieces: Array.isArray(ctx && ctx.pieces), done: o && o.done, caption: o && o.caption }); return step === 'sorted' ? '<svg data-test-seal viewBox="0 0 10 10" width="40" height="40"><circle cx="5" cy="5" r="4"/></svg>' : null; } }; });
      let sel = dot(gf, 'sorted'); await rest(page, sel); let m = await read(page, sel);
      assert(/data-test-seal/.test(m.tip.seal), 'what PieceSeals returned is in the card: ' + m.tip.seal);
      const ask = await page.evaluate(() => window.__asked.filter(a => a.step === 'sorted').pop()); assert.deepEqual(ask, { step: 'sorted', key: gf, events: true, pieces: true, done: true, caption: false }, 'asked with the step, the piece, the events and whether it is done: ' + JSON.stringify(ask));
      await away(page); sel = dot(gf, 'shipped'); await rest(page, sel); m = await read(page, sel); assert.equal(m.tip.seal, '', 'null from PieceSeals: no slot');
      await away(page); await page.evaluate(() => { delete window.PieceSeals; });
      sel = dot(gf, 'sorted'); await rest(page, sel); m = await read(page, sel); assert.equal(m.tip.seal, '', 'without PieceSeals: no slot, nothing broken'); assert.equal(m.tip.name, 'Sorted'); await away(page);
      await page.evaluate(() => { window.PieceSeals = window.__real; }); }
    { const seal = page2 => page2.evaluate(() => { const t = document.querySelector('.railTip[data-on]'), q = t && t.querySelector('.rtSeal'); if (!q) return null; const s = q.querySelector('.seal'), g = q.querySelector('.pgGhost'), r = (s || g || q).getBoundingClientRect(), tr = t.getBoundingClientRect(); return { real: !!s, ghost: !!g, state: q.querySelector('[data-pg-state]') && q.querySelector('[data-pg-state]').getAttribute('data-pg-state'), live: q.classList.contains('live'), tab: s ? s.getAttribute('tabindex') : null, pe: s ? getComputedStyle(s).pointerEvents : g ? getComputedStyle(g).pointerEvents : '', cardPe: getComputedStyle(t).pointerEvents, caption: !!q.querySelector('.pgBy'), x: (r.left + r.right) / 2, y: (r.top + r.bottom) / 2, w: r.width, h: r.height, below: t.classList.contains('below'), inside: r.left >= tr.left && r.right <= tr.right && r.top >= tr.top && r.bottom <= tr.bottom }; });
      // a done step with a recorded seal: the real seal; a step to come: the unfinished one; the words are the card's own (no second caption)
      let sel = dot(gf, 'sorted'); await rest(page, sel); let q = await seal(page);
      assert(q && q.real && !q.ghost && q.state === 'done' && q.live, 'a done step: the real seal in the card ' + JSON.stringify(q)); assert.equal(q.caption, false, 'the card writes who and when; the seal has no caption'); assert.equal(q.tab, '-1', 'no Tab stop inside the card'); assert.equal(q.pe, 'auto', 'the real seal takes the pointer, so it zooms'); assert.equal(q.cardPe, 'none', 'the rest of the card does not'); assert(q.inside && q.w >= 30, 'the seal is inside the card, a small one'); await away(page);
      sel = dot(gf, 'assembled'); await rest(page, sel); q = await seal(page); assert(q && q.ghost && !q.real && q.state === 'missing' && !q.live, 'a step still to come: the unfinished seal ' + JSON.stringify(q)); assert.equal(q.pe, 'none'); await away(page);
      sel = dot(gf, 'laser'); await rest(page, sel); q = await seal(page); assert(q && q.real, 'Laser cut is done: its real seal'); await away(page);
      sel = dot(cable, 'sheet'); await rest(page, sel); q = await seal(page); assert(q && q.ghost, 'a piece not on a sheet yet: the unfinished Nested seal'); await away(page);
      // the card gives way everywhere but the seal: the point under its text is what is under it, the point on its seal is the seal
      sel = dot(gf, 'sorted'); await rest(page, sel); q = await seal(page);
      const under = await page.evaluate(({ x, y }) => { const t = document.querySelector('.railTip'), r = t.getBoundingClientRect(), e1 = document.elementFromPoint(x, y), e2 = document.elementFromPoint(r.left + 20, r.top + 10); return { onSeal: !!(e1 && e1.closest('.railTip .seal')), text: !!(e2 && e2.closest('.railTip')) }; }, q);
      assert.deepEqual(under, { onSeal: true, text: false }, 'only the seal takes the pointer: ' + JSON.stringify(under));
      // the pointer travels from the dot to the seal: the card waits for it, and the seal zooms in place after its own rest (500 ms)
      const c = await centre(page, sel); await page.mouse.move(c.x, c.y);
      const steps = 6; for (let i = 1; i <= steps; i++) { await page.mouse.move(c.x + (q.x - c.x) * i / steps, c.y + (q.y - c.y) * i / steps); await sleep(20); }
      await sleep(250); assert((await seal(page)), 'the card is still there with the pointer on its seal');
      await sleep(900); const zoomed = await page.evaluate(() => { const s = document.querySelector('.railTip .seal'), r = s && s.getBoundingClientRect(); return { cls: !!(s && s.classList.contains('sealZoomed')), k: s ? +(r.width / s.offsetWidth).toFixed(2) : 0, tip: !!document.querySelector('.railTip[data-on]') }; });
      assert(zoomed.cls && zoomed.k >= 1.15 && zoomed.tip, 'the real seal in the card grew in place on rest, the card stayed: ' + JSON.stringify(zoomed));
      await away(page); await sleep(700); assert.deepEqual(await page.evaluate(() => ({ tip: !!document.querySelector('.railTip[data-on]'), grown: document.querySelectorAll('.railTip .sealZoomed').length })), { tip: false, grown: 0 }, 'leaving the seal puts both away');
      // the pointer leaves the dot for nowhere: the card is gone soon, and a pointer resting on the dot opens it again
      sel = dot(gf, 'sorted'); await rest(page, sel); await away(page, 100); assert.equal(await goneWithin(page, sel, 4000), true, 'a pointer that leaves the dot for elsewhere: the card goes (a moment late for a seal, never held)');
      // Esc puts away a held card too
      await rest(page, sel); const c2 = await centre(page, sel); await page.mouse.move(c2.x, c2.y - 14, { steps: 2 }); await page.keyboard.press('Escape'); assert.equal(await goneWithin(page, sel, 4000), true, 'Esc puts the card away'); await away(page); }

    // ── 9 · 1440, 900, 390: the card fits, the page is not made wider ──
    for (const [w, h] of [[1440, 900], [900, 800], [390, 844]]) {
      await page.setViewportSize({ width: w, height: h }); await sleep(500);
      for (const [key, step, state] of [[gf, 'sorted', 'Done'], [gf, 'assembled', 'Not yet'], [gf, 'arrived', 'Done'], [gf, 'shipped', 'Not yet']]) {
        const sel = dot(key, step), base = await page.evaluate(() => document.documentElement.scrollWidth); await rest(page, sel, 700); const m = await read(page, sel);
        assert.equal(m.shown, true, `${w}: the ${step} card opens`); assert.equal(m.tip.state, state);
        assert(m.tip.l >= 7.5 && m.tip.r <= m.vw - 7.5 && m.tip.t >= 7.5 && m.tip.b <= m.vh - 7.5, `${w}: ${step} card inside the screen (${m.tip.l}..${m.tip.r} of ${m.vw}, ${m.tip.t}..${m.tip.b} of ${m.vh})`);
        assert(m.scrollW <= Math.max(base, m.vw) + 1, `${w}: the page is not wider with a card open (${m.scrollW} vs ${base})`); assert(m.tip.ax >= 13.5 && m.tip.ax <= m.tip.w - 13.5, `${w}: the arrow stays on the card`);
        const cx = (m.dot.l + m.dot.r) / 2; assert(Math.abs(m.tip.l + m.tip.ax - cx) < 2 || m.tip.ax <= 14.5 || m.tip.ax >= m.tip.w - 14.5, `${w}: the arrow points at the dot`);
        if (shots && step === 'sorted' || shots && step === 'assembled') {
          if (w === 1440 || w === 390) { const x = Math.max(0, Math.min(m.vw - Math.min(m.vw, 560), cx - 280)), top = Math.max(0, Math.min(m.tip.t, m.dot.t) - 90), bot = Math.min(m.vh, Math.max(m.tip.b, m.dot.b) + 130);
            await page.screenshot({ path: path.join(shots, `${step === 'sorted' ? 'done' : 'not-done'}-dot-card-${w}.png`), clip: { x, y: top, width: Math.min(m.vw, 560), height: bot - top } }); }
        }
        await away(page, 300); }
    }
    await page.setViewportSize({ width: 1440, height: 900 }); await sleep(500);

    // ── 10 · a redraw under the pointer: the step is done while the card is open ──
    { const sel = dot(gf, 'assembled'); await rest(page, sel, 650); let m = await read(page, sel); assert.equal(m.tip.state, 'Not yet');
      const before = await page.evaluate(sel => { const d = document.querySelector(sel); d.__mine = 1; return 1; }, sel);
      ev('assembled', 5, { by: 'Mia K.', source: 'station', station: 'assembly', lineKey: gf });
      await page.waitForFunction(sel => document.querySelector(sel).classList.contains('on') && !document.querySelector(sel).__mine, sel, { timeout: 20000 });
      await sleep(900); m = await read(page, sel); assert.equal(m.shown, true, 'the card is still there after the row was drawn again'); assert.equal(m.tip.state, 'Done', 'and says Done now'); assert.match(m.tip.by, /Mia K\. at Assembly$/); assert(m.k >= 1.5, 'the new dot is grown'); await away(page);
      const stops = await page.evaluate(gf => [...document.querySelectorAll(`#owPcSum i[data-pdot-piece="${gf}"]`)].filter(d => d.tabIndex === 0).length, gf); assert.equal(stops, 1); }

    assert.deepEqual(errors, [], 'no page errors'); await context.close();

    // ── 11 · touch: a tap opens at once and holds; a tap elsewhere puts it back ──
    { const { context: tctx, page: tp } = await open({ viewport: { width: 390, height: 844 }, hasTouch: true, isMobile: true });
      const sel = dot(gf, 'sorted'); await tp.evaluate(sel => document.querySelector(sel).scrollIntoView({ block: 'center' }), sel); await sleep(250);
      const n0 = await nRows(tp); await tp.tap(sel); await sleep(600); let m = await read(tp, sel);   // (taps are apart by more than a double-tap) assert.equal(m.shown, true, 'touch: a tap opens the card at once'); assert.equal(m.tip.state, 'Done'); assert.equal(await nRows(tp), n0, 'and does not select the row');
      await tp.tap(sel); assert.equal(await goneWithin(tp, sel), true, 'a second tap puts it back'); await sleep(600); await tp.tap(sel); assert.equal(await shownWithin(tp, sel, 3000), true); await sleep(600);
      await tp.tap('.owHead', { position: { x: 6, y: 6 } }).catch(() => {}); assert.equal(await goneWithin(tp, sel), true, 'a tap elsewhere puts it back');
      await tctx.close(); }

    // ── 12 · reduced motion: the dot does not grow, the card still shows ──
    { const { context: rctx, page: rp } = await open(); await rp.emulateMedia({ reducedMotion: 'reduce' }); await sleep(200);
      const sel = dot(gf, 'sorted'); await rest(rp, sel, 700); const m = await read(rp, sel); assert(m.k < 1.02, `reduced motion: no growth (${m.k})`); assert.equal(m.shown, true, 'reduced motion: the card still shows'); assert.equal(m.tip.name, 'Sorted');
      await rctx.close(); }

    // ── 13 · a mutant with no delay is caught by the quick pass (check 2) ──
    { const { context: mctx, page: mp } = await open();
      await quickPass(mp, gf);   // (the real page passes)
      await mp.evaluate(() => { for (const d of document.querySelectorAll('#owPcSum [data-pdot]')) d.setAttribute('data-zoom-delay', '0'); });
      let caught = false; try { await quickPass(mp, gf); } catch (e) { caught = /opens nothing|quick/.test(String(e.message)); }
      assert(caught, 'a dot with no delay opens its card during a quick pass: the check catches it');
      await mctx.close(); }

    // ── 14 · C1: the dots follow where the pieces are NOW. A held order whose two pieces were nested and then taken off their sheets by the Hold: at most one solid dot (Order
    //        in), Nested hollow, its card says "Not on a sheet now" and where it was; every later card says it comes after Nested; the history's real ON SHEET seal stays in the
    //        Nested card. A piece the history shows laser cut and sorted is not clamped. A mutant that ignores the placement (the dots read the history alone) is caught. ──
    { const { context: hctx, page: hp } = await open();
      const ha = kOf(HELD, 'a'), hb = kOf(HELD, 'b'), pa = kOf(PAST, 'a'), pb = kOf(PAST, 'b');
      const openOrder = async (o, dots, events) => { await hp.evaluate(k => OrderWin.open(k), kOf(o, 'a')); await hp.waitForFunction(({ rid, dots, events }) => !!OrderWin.isOpen() && document.querySelectorAll(`#owPcSum .steps i[data-pdot][data-pdot-rid="${rid}"]`).length === dots && +((document.querySelector('#owTlCount') || {}).textContent || 0) >= events, { rid: o.rid, dots, events }, { timeout: 25000 }); await sleep(500); };
      const dotsOf = async () => Object.fromEntries((await rowsInfo(hp)).map(r => [r.key, r]));
      const heldIsFenced = async () => {
        const rows = await dotsOf();
        for (const k of [ha, hb]) {
          assert.deepEqual(rows[k].dots.filter(d => d.on).map(d => d.step), ['arrived'], `the held piece ${k}: one solid dot, Order in (Nested was done once, it is on no sheet now)`);
          assert.deepEqual(rows[k].dots.map(d => d.label), ['Order in, done', 'Nested, not yet', 'Laser cut, not yet', 'Sorted, not yet', 'Assembled, not yet', 'Shipped, not yet'], 'each label says it too');
        }
      };
      await openOrder(HELD, 12, 6); await heldIsFenced();
      // the Nested card of the first piece: where it is now, where it was, the hold, and the history's real seal (a seal is permanent)
      let sel = dot(ha, 'sheet'); await rest(hp, sel); let m = await read(hp, sel);
      assert.equal(m.shown, true); assert.equal(m.tip.name, 'Nested'); assert.equal(m.tip.state, 'Not yet'); assert.equal(m.tip.line, 'Not on a sheet now', 'the Nested card says where the piece is now: ' + m.tip.text);
      assert.equal(m.tip.by, `Was on GF Sheet 2 until Paul took it off, ${when(HOLD_AT.a)} · On hold`, 'and where it was, who took it off and when, and that it is on hold: ' + m.tip.by);
      assert(!/\blines?\b/i.test(m.tip.text), 'pieces, never lines');
      const sealOf = () => hp.evaluate(() => { const q = document.querySelector('.railTip[data-on] .rtSeal'); return q ? { real: !!q.querySelector('.seal'), ghost: !!q.querySelector('.pgGhost') } : null; });
      assert.deepEqual(await sealOf(), { real: true, ghost: false }, 'the ON SHEET seal is history and stays: the real one, never a ghost, in the hollow Nested dot\'s card');
      if (shots) { const cx = (m.dot.l + m.dot.r) / 2, x = Math.max(0, Math.min(m.vw - 560, cx - 280)), top = Math.max(0, Math.min(m.tip.t, m.dot.t) - 90), bot = Math.min(m.vh, Math.max(m.tip.b, m.dot.b) + 130);
        await hp.screenshot({ path: path.join(shots, 'held-nested-dot-card-1440.png'), clip: { x, y: top, width: Math.min(m.vw, 560), height: bot - top } }); }
      await away(hp);
      // the second piece, off its own sheet (RG Sheet 2), says the same of its own sheet
      sel = dot(hb, 'sheet'); await rest(hp, sel); m = await read(hp, sel); assert.equal(m.tip.line, 'Not on a sheet now'); assert.match(m.tip.by, /^Was on RG Sheet 2 until Paul took it off, /); await away(hp);
      // every later step waits for Nested
      for (const k of ['laser', 'sorted', 'assembled', 'shipped']) { sel = dot(ha, k); await rest(hp, sel); m = await read(hp, sel); assert.equal(m.tip.state, 'Not yet'); assert.equal(m.tip.line, 'After Nested', `${k}: the step ahead names the one it waits on`); await away(hp); }
      // Order in stays Done, with its seal
      sel = dot(ha, 'arrived'); await rest(hp, sel); m = await read(hp, sel); assert.equal(m.tip.state, 'Done'); assert.deepEqual(await sealOf(), { real: true, ghost: false }); await away(hp);
      // a piece the history shows laser cut and sorted is not clamped (it cannot un-cut); its sibling, nested only, is
      await openOrder(PAST, 12, 5);
      { const rows = await dotsOf();
        assert.deepEqual(rows[pa].dots.filter(d => d.on).map(d => d.step), ['arrived', 'sheet', 'laser', 'sorted'], 'a held piece the history shows sorted keeps its dots: ' + JSON.stringify(rows[pa].dots.map(d => d.on)));
        assert.deepEqual(rows[pb].dots.filter(d => d.on).map(d => d.step), ['arrived'], 'its sibling, nested only and held, has one'); }
      sel = dot(pb, 'sheet'); await rest(hp, sel); m = await read(hp, sel); assert.equal(m.tip.line, 'Not on a sheet now'); assert.equal(m.tip.by, 'On hold: Add to next sheet', 'no history of a sheet taken off: only the hold: ' + m.tip.by); await away(hp);
      sel = dot(pa, 'sheet'); await rest(hp, sel); m = await read(hp, sel); assert.equal(m.tip.state, 'Done', 'the sorted piece\'s Nested is Done (history)'); await away(hp);
      // a mutant that ignores the placement (the dots read the history alone) is caught by the held-order check
      await openOrder(HELD, 12, 6); await heldIsFenced();   // (the real page passes)
      await hp.evaluate(() => { window.__PP = window.PiecePlacement; window.PiecePlacement = Object.assign({}, window.__PP, { of: () => null }); });
      await openOrder(PAST, 12, 5); await openOrder(HELD, 12, 6);
      let caught = false; try { await heldIsFenced(); } catch (e) { caught = /one solid dot/.test(String(e.message)); }
      assert(caught, 'with the placement ignored, the held pieces read Nested solid from the history: the check catches it');
      await hp.evaluate(() => { window.PiecePlacement = window.__PP; });
      await hctx.close(); }
    assert.deepEqual(errors, [], 'no page errors, no console errors');
  } finally { await browser.close(); srv.close(); }
  console.log('Piece row dot hover OK: six labelled zoom dots to a row with one Tab stop and a hit area wider than the dot; 350 ms rest (a 200 ms pass opens nothing, measured 330-520 ms), click, tap, Tab, Enter and Space open it at once, Esc, scroll, a second click and moving away put it back; the card says step, state, when and who and where, or what it waits on, with a seal slot for PieceSeals; never over the row\'s buttons, never clickable; the next dot opens after a short beat once a card was open; a redraw under the pointer keeps the card; 1440/900/390 fit; reduced motion; touch; a no-delay mutant is caught; a held order\'s dots follow where its pieces are now (one solid dot, Nested hollow, "Not on a sheet now", the real seal kept), a piece the history shows sorted is not clamped, a placement-blind mutant is caught');
}
main().then(() => process.exit(0), e => { console.error(e); process.exit(1); });
