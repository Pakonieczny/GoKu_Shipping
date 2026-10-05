// The "Its pieces" list in the order window is the piece selector (Paul, 5 Oct 2026, 17:23 UTC, points 2 and 3):
//  "2. Show the user which one of these is actually being displayed in the detailed order view. Allow the user to click each one of these
//   without leaving this "All" view. Depending on which one is selected, then the detail view should display all the information pertaining
//   to that specific piece. This is how the user is to toggle between individual pieces in a multi-piece order on all detailed order modals
//   moving forward. 3. ... remove this UI [the chip row 'All 6 pieces | SPORTS 10 - FIGURE SKATE | SNAKE 5'] and specific functionality from
//   all detailed order modals."
// Fixture: an order of three pieces (SPORTS 10 - FIGURE SKATE, SNAKE 5, FOOTBALL, two of each: six pieces), one piece cut already, and a
// single-piece order; headless Chromium over the local fake site (bridge-server.cjs), offline, nothing live, no Etsy, no paid call.
// Proves: no chip row anywhere (no element, no CSS, no text) and the checks that say so fail on a build that keeps one; every row is a button for
// its piece (click on the row body, Enter, Space; aria-pressed on its name, aria-current on the row shown); the row shown is marked (tint, a bar in the
// piece's metal colour, bolder name) and the quiet line at the head says what is shown ("Showing all 6 pieces" / "Showing SNAKE 5 · Show all");
// a press changes the detail (the line the Overview holds, its photo/SKU, the header rail, the Timeline and Sheet focus) WITHOUT leaving the Overview,
// without a scroll jump, with only opacity and transform animating and never the list itself; the row again (or Show all) returns to all pieces;
// Up/Down/Home/End move the pick with the focus; a button, the dots, anything marked data-no-select does not pick; one piece: one neutral row, no
// "Show all"; OrderWin.openOrder/open with `pick` lands on the right row; OrderWin.selectPiece / selectedPiece; fits 1440, 900 and 390.
//   SHOTS=<dir> node tests/charm-nest/order-piece-selector.cjs   (PW_DIR=<playwright-core's node_modules>, CHROMIUM=<chrome>)
const fs = require('fs'), path = require('path'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');
const { start } = require('./bridge-server.cjs');

const DAY = 86400, SHIP = Math.floor(Date.UTC(2026, 9, 10, 17) / 1000);
const R3 = '4177000003', R1 = '4177000001';
const ln = (rid, n, sku, qty) => ({ transactionId: rid + n, listingId: '19037' + n + '9935', sku, title: sku.replace(/_/g, ' ') + ' necklace', quantity: qty, expectedShipDate: SHIP, variations: [{ name: 'Metal', value: 'RG 14/20' }], metalKey: 'rose', metalLabel: 'RG 14/20', personalization: '' });
const order = (rid, buyer, lines) => ({ receiptId: rid, orderNumber: rid, createTs: SHIP - 5 * DAY, updateTs: SHIP - 5 * DAY + 60, shipBy: SHIP, buyer: { name: buyer }, buyerMessage: '', isGift: false, giftMessage: '', staffNote: '', messages: [], lines });
const ORDERS = [
  order(R3, 'Sam Skater', [ln(R3, '1', 'SPORTS_10_-_FIGURE_SKATE', 2), ln(R3, '2', 'SNAKE_5', 2), ln(R3, '3', 'FOOTBALL', 2)]),
  order(R1, 'Una Solo', [ln(R1, '1', 'PLAIN_TAG', 1)])];
const key = (rid, n) => `${rid}_${rid}${n}`;
const K = [key(R3, 1), key(R3, 2), key(R3, 3)], KS = key(R1, 1);

(async () => {
  const pwDir = process.env.PW_DIR || path.join(root, 'node_modules');
  let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { try { ({ chromium } = require('/opt/node22/lib/node_modules/playwright/node_modules/playwright-core')); } catch (__) { console.log('  – no playwright-core: the browser checks were not run'); return; } }
  const shots = process.env.SHOTS || ''; if (shots) fs.mkdirSync(shots, { recursive: true });
  const srv = await start({ receipts: [] });
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  const fails = [];
  const check = (ok, msg) => { if (!ok) fails.push(msg); console.log((ok ? '  ✓ ' : '  ✗ ') + msg); };
  try {
    const context = await browser.newContext({ viewport: { width: 1440, height: 900 } });
    const outside = [];
    await context.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => { const u = r.request().url(); if (/etsy/i.test(u)) outside.push(u); return /fonts\.googleapis|fonts\.gstatic/.test(u) ? r.fulfill({ status: 200, contentType: 'text/css', body: '' }) : r.abort(); });
    await context.addInitScript(() => { try { localStorage.setItem('cn.settings', JSON.stringify({ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on' })); localStorage.setItem('cn.employee', 'Test Operator'); } catch (_) {} });
    const page = await context.newPage(), errors = [];
    page.setDefaultTimeout(30000);
    page.on('pageerror', e => { errors.push(e.message); console.error('page error:', String(e.stack || e.message).split('\n').slice(0, 4).join(' | ')); });
    await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
    await page.waitForFunction(() => window.CN && window.Orders && window.OrderWin && window.OrderTimeline && window.HoldUI && CN.S.cloud.ok === true, null, { timeout: 60000 });

    // ── the orders and their timelines: the first piece is cut, the second is on a sheet, the third waits ──
    await page.evaluate(async ({ orders, K, R3 }) => {
      const T0 = Date.now() - 6 * 3600e3;
      for (const o of orders) for (const line of o.lines) { const k = CharmNestOrders.lineKey(o, line); const row = { key: k, order: o, line, arrivedAt: T0, spec: null, problems: [], state: 'pooled', reason: null, claimedBy: null, poolIds: [k + '_1'], engrave: null, material: null }; B.orders.rows.push(row); B.orders.byKey.set(k, row); }
      Orders.interpretAll(); await Orders.loadMaps(true); Orders.interpretAll();
      const ev = (type, min, o) => Object.assign({ id: `${R3}~${type}~${min}`, type, at: T0 + min * 60e3, by: 'paul', source: 'sorter' }, o || {});
      const t = n => R3 + n;
      const events = [ev('arrived', 0, { source: 'etsy', by: 'Etsy' }),
        ev('placed', 10, { lineKey: K[0], transactionId: t(1), sheetId: 'sh-a', sheet: 'RG Sheet 1' }), ev('placed', 12, { lineKey: K[1], transactionId: t(2), sheetId: 'sh-a', sheet: 'RG Sheet 1' }),
        ev('laserDone', 40, { lineKey: K[0], transactionId: t(1), sheetId: 'sh-a', sheet: 'RG Sheet 1' })];
      const get = OrderTimeline.get; OrderTimeline.get = (id, ...r) => String(id) === R3 ? Promise.resolve({ events: JSON.parse(JSON.stringify(events)), cancelled: null, where: null }) : get.call(OrderTimeline, id, ...r);
      CN.setMode('orders'); Orders.render();
    }, { orders: ORDERS, K, R3 });

    const shot = async name => { if (shots) { await page.waitForTimeout(450); await page.screenshot({ path: path.join(shots, name + '.png') }); } };
    const rowsOf = () => page.evaluate(() => [...document.querySelectorAll('#owPcSum .owPcRow')].map(r => ({ key: r.dataset.piece || null, sel: r.classList.contains('sel'), pics: r.classList.contains('pics'), cur: r.getAttribute('aria-current'), pressed: (r.querySelector('.owPcName') || document.body).getAttribute('aria-pressed'), solo: r.classList.contains('solo'), tag: r.tagName })));
    const stateText = () => page.evaluate(() => { const s = document.querySelector('#owPcSum .owPcState'); return s ? s.textContent.replace(/\s+/g, ' ').trim() : null; });
    const settled = () => page.waitForFunction(() => !document.getElementById('orderWin').getAnimations({ subtree: true }).some(a => a.playState === 'running' && a.effect && a.effect.getTiming().iterations !== Infinity), null, { timeout: 15000 });
    const openWin = async (k, extra) => {
      await page.evaluate(([k, o]) => OrderWin.open(k, o || {}), [k, extra]);
      await page.waitForFunction(k => OrderWin.isOpen() && OrderWin.key() === k && document.querySelectorAll('#owPcSum .owPcRow').length >= 1, k, { timeout: 20000 });
      await settled();
    };
    const closeWin = async () => { await page.evaluate(() => OrderWin.isOpen() && OrderWin.close()); await page.waitForFunction(() => !document.getElementById('orderWin').open, null, { timeout: 8000 }); };
    const waitRows = n => page.waitForFunction(n => document.querySelectorAll('#owPcSum .owPcRow[data-piece]').length === n, n, { timeout: 20000 });
    const rowSel = k => `#owPcSum .owPcRow[data-piece="${k}"]`;
    // every Element.animate the page makes while a pick is made (what moved, how long, which properties), and the pressed row's place on every frame
    await page.evaluate(() => { window.__anims = []; const orig = Element.prototype.animate; Element.prototype.animate = function (kf, o) { const props = new Set(); for (const f of (Array.isArray(kf) ? kf : [])) for (const p of Object.keys(f)) if (p !== 'offset' && p !== 'easing') props.add(p); window.__anims.push({ id: this.id || (this.className && String(this.className).split(' ')[0]) || this.tagName, d: o && o.duration, props: [...props].sort().join() }); return orig.apply(this, arguments); }; });
    const trace = k => page.evaluate(k => { window.__tr = []; const t = performance.now(), el = () => document.querySelector(`#owPcSum .owPcRow[data-piece="${k}"]`); const sc = (() => { for (let e = document.getElementById('owPcSum').parentElement; e; e = e.parentElement) { const o = getComputedStyle(e).overflowY; if ((o === 'auto' || o === 'scroll') && e.scrollHeight > e.clientHeight + 1) return e; } return null; })(); const f = () => { const e = el(); if (e) __tr.push([Math.round(performance.now() - t), e.getBoundingClientRect().top, sc ? sc.scrollTop : 0]); if (performance.now() - t < 700) requestAnimationFrame(f); }; f(); }, k);
    // what the Overview shows of the piece (the line it holds, its SKU, the header rail's cut step) and what is selected
    const detail = () => page.evaluate(() => { const r = document.querySelector('#owRail [data-stage="laser"]'); return { key: OrderWin.key(), sel: OrderWin.selectedPiece(), view: OrderWin.view(), sku: (document.getElementById('owSku') || {}).textContent, laserDone: !!r && /\bd\b/.test(r.className), tab: document.querySelector('.owTabsV [role=tab][aria-selected=true]')?.dataset.owView }; });

    // ── the checks that a build with a chip row must fail, kept as a function ──
    const chipRow = () => page.evaluate(() => {
      const bad = [];
      for (const s of ['#owPieceSw', '.owPieceSw', '.owPcThumb', '[aria-label="Which piece of the order"]']) if (document.querySelector(s)) bad.push('element ' + s);
      for (const e of document.querySelectorAll('button, [role=button], [role=tab]')) if (/^All \d+ pieces$/.test(e.textContent.trim())) bad.push('"' + e.textContent.trim() + '" button');
      for (const sh of document.styleSheets) { let rules = []; try { rules = [...sh.cssRules]; } catch (_) {} for (const r of rules) if (/owPieceSw|owPcThumb/.test(r.cssText || '')) bad.push('css ' + (r.selectorText || '').slice(0, 40)); }
      return bad;
    });
    const markVisible = k => page.evaluate(k => {
      const row = document.querySelector(`#owPcSum .owPcRow[data-piece="${k}"]`), others = [...document.querySelectorAll('#owPcSum .owPcRow[data-piece]')].filter(r => r !== row);
      if (!row) return { ok: false, why: 'no row' };
      const be = getComputedStyle(row, '::before'), af = getComputedStyle(row, '::after'), b = row.querySelector('.nm b'), dot = row.querySelector('.dot');
      const oBe = others.map(r => +getComputedStyle(r, '::before').opacity), oAf = others.map(r => +getComputedStyle(r, '::after').opacity);
      const bar = af.backgroundColor, dotC = getComputedStyle(dot).backgroundColor;
      return { ok: +be.opacity > .3 && +af.opacity > .9 && +(b ? getComputedStyle(b).fontWeight : 0) >= 700 && oBe.every(o => o === 0) && oAf.every(o => o === 0) && bar === dotC && row.getAttribute('aria-current') === 'true',
        tint: +be.opacity, bar: +af.opacity, barColour: bar, dotColour: dotC, weight: b ? getComputedStyle(b).fontWeight : null, others: [oBe, oAf] };
    }, k);
    const press = async (k, how) => {
      const sel = rowSel(k);
      if (how === 'click') await page.click(`${sel} .dot`);   // (the row's own body, not its status words or buttons: those belong to other parts)
      else if (how === 'name') await page.click(`${sel} .owPcName`);
      else if (how === 'api') await page.evaluate(k => OrderWin.selectPiece(k), k);
    };

    // ═══ 1 · the order of three pieces, all shown ═══
    await openWin(K[0]); await waitRows(3);
    await page.waitForFunction(() => document.querySelectorAll('#owRail [data-stage]').length >= 6 && document.querySelector('#owRail [data-stage="sheet"] .tlCnt:not([hidden])'), null, { timeout: 15000 });
    await settled();
    check((await chipRow()).length === 0, 'no chip row: no #owPieceSw, no "All 6 pieces" button, no .owPieceSw / .owPcThumb CSS anywhere: ' + JSON.stringify(await chipRow()));
    let R = await rowsOf();
    check(R.length === 3 && R.map(r => r.key).join() === K.join(), 'the three pieces are three rows of the "Its pieces" list: ' + R.map(r => r.key).join());
    check(R.every(r => !r.sel && r.cur === null && r.pressed === 'false' && r.tag === 'DIV'), 'with all pieces shown no row is marked: ' + JSON.stringify(R.map(r => [r.sel, r.cur, r.pressed])));
    check((await stateText()) === 'Showing all 6 pieces', 'the quiet line at the head says what is shown: ' + await stateText());
    check(await page.evaluate(() => !document.querySelector('#owPcSum [data-pc-all]') && /^Its pieces/.test(document.querySelector('#owPcSum .fLabel').textContent)), 'no "Show all" while all pieces are shown; the list is still headed "Its pieces"');
    check(R.filter(r => r.pics).length === 1 && R.find(r => r.pics).key === K[0], 'with all pieces shown, the row whose photo and details the Overview holds has a quiet ring on its dot: ' + JSON.stringify(R.map(r => r.pics)));
    let D = await detail();
    check(D.sel === null && D.view === 'info' && D.laserDone === false, 'the Overview, no piece selected, the order is where its slowest piece is (not cut): ' + JSON.stringify(D));
    await shot('all-1440');

    // ═══ 2 · the rows are the selector: a press on the row body picks the piece without leaving the Overview ═══
    const listTop = k => page.evaluate(k => document.querySelector(`#owPcSum .owPcRow[data-piece="${k}"]`).getBoundingClientRect().top, k);
    const scrollTop = () => page.evaluate(() => { for (let e = document.getElementById('owPcSum').parentElement; e; e = e.parentElement) { const o = getComputedStyle(e).overflowY; if ((o === 'auto' || o === 'scroll') && e.scrollHeight > e.clientHeight + 1) return e.scrollTop; } return 0; });
    await page.evaluate(() => { window.__tabs = []; const n = document.querySelector('.owTabsV'); new MutationObserver(() => __tabs.push(document.querySelector('.owTabsV [role=tab][aria-selected=true]')?.dataset.owView)).observe(n, { attributes: true, subtree: true, attributeFilter: ['aria-selected'] }); });
    const t0 = await listTop(K[1]), s0 = await scrollTop();
    await page.evaluate(() => { window.__anims.length = 0; });
    await trace(K[1]);
    await press(K[1], 'click');
    await page.waitForFunction(k => OrderWin.key() === k && OrderWin.selectedPiece() === k, K[1], { timeout: 10000 });
    await page.waitForTimeout(450);
    // (during the 180 ms: what changed crosses over with opacity and transform only; the list may only glide, by transform, never jump)
    const anims = await page.evaluate(() => window.__anims.slice());
    const changed = anims.filter(a => /^(owPics|owNowCard|owMeta)$/.test(a.id));
    check(changed.length >= 2 && changed.every(a => a.d === 180 && a.props === 'opacity,transform'), 'what changed (the pictures, the status card) crosses over in 180 ms with opacity and transform only: ' + JSON.stringify(changed));
    check(anims.filter(a => a.id === 'owPcSum').every(a => a.d === 180 && a.props === 'transform'), 'the list itself only ever glides (transform): ' + JSON.stringify(anims.filter(a => a.id === 'owPcSum')));
    await settled();
    D = await detail(); R = await rowsOf();
    check(D.view === 'info' && D.tab === 'info' && (await page.evaluate(() => __tabs.length)) === 0, 'still the Overview: the tab never changed (' + D.tab + ')');
    check(D.key === K[1] && D.sel === K[1] && /SNAKE/.test(D.sku), 'the detail is the second piece\'s: its line and SKU ' + JSON.stringify([D.key, D.sku]));
    check(R.length === 3 && R.filter(r => r.sel).map(r => r.key).join() === K[1] && R.find(r => r.key === K[1]).pressed === 'true' && R.find(r => r.key === K[1]).cur === 'true' && R.filter(r => r.pressed === 'true').length === 1, 'all three rows stay; only the second is marked (aria-current on the row, aria-pressed on its name): ' + JSON.stringify(R.map(r => [r.key.slice(-1), r.sel, r.cur, r.pressed])));
    check(!R.some(r => r.pics), 'the ring of "holds the photo" is only for the all-pieces state');
    let mv = await markVisible(K[1]);
    check(mv.ok, 'the row shown is clearly marked: a soft tint, a thin bar in the piece\'s metal colour, a bolder name, nothing on the others: ' + JSON.stringify(mv));
    check((await stateText()) === 'Showing SNAKE 5 · Show all', 'the head says "Showing SNAKE 5 · Show all": ' + await stateText());
    check(await page.evaluate(() => !!document.querySelector('#owPcSum button[data-pc-all]')), '"Show all" is a button');
    const t1 = await listTop(K[1]), s1 = await scrollTop(), tr = await page.evaluate(() => __tr);
    const lo = Math.min(t0, t1) - 1.5, hi = Math.max(t0, t1) + 1.5, first = tr[0];
    check(tr.length > 5 && tr.every(f => f[1] >= lo && f[1] <= hi) && (Math.abs(t1 - t0) <= 1.5 || Math.abs(first[1] - t0) <= Math.abs(t1 - t0) * .6 + 2), `no scroll jump: the pressed row never leaves the way between where it was (${t0}) and where it is (${t1}), and it starts from where it was (frame 1: ${first && first[1]}); scroller ${s0} -> ${s1}; ${tr.length} frames`);
    check(D.laserDone === false, 'the header rail shows this piece\'s own steps (not cut)');
    await shot('one-selected-1440');

    // each row in turn; the first piece is the cut one
    await press(K[2], 'name');
    await page.waitForFunction(k => OrderWin.selectedPiece() === k && OrderWin.key() === k, K[2], { timeout: 10000 }); await settled();
    D = await detail(); R = await rowsOf();
    check(D.view === 'info' && /FOOTBALL/.test(D.sku) && R.filter(r => r.sel).map(r => r.key).join() === K[2] && (await stateText()) === 'Showing FOOTBALL · Show all', 'the third piece: its own detail, only its row marked, the line says so: ' + JSON.stringify([D.sku, await stateText()]));
    await press(K[0], 'click');
    await page.waitForFunction(k => OrderWin.selectedPiece() === k && OrderWin.key() === k, K[0], { timeout: 10000 }); await settled();
    D = await detail(); R = await rowsOf();
    check(D.view === 'info' && /SPORTS/.test(D.sku) && D.laserDone === true && R.filter(r => r.sel).map(r => r.key).join() === K[0], 'the first piece (cut): its own detail, and the header rail shows its cut step done: ' + JSON.stringify(D));
    mv = await markVisible(K[0]); check(mv.ok, 'its row is the one marked: ' + JSON.stringify(mv));
    // the Timeline and the Sheet follow the piece (the tabs hold the same pick)
    const tlCount = async () => page.evaluate(() => document.querySelectorAll('#owTimeline .tlSt[data-key]').length);
    await page.evaluate(() => OrderWin.setView('timeline'));
    await page.waitForFunction(() => document.querySelectorAll('#owTimeline .tlSt[data-key]').length > 0, null, { timeout: 15000 }); await settled();
    const tOne = await tlCount();
    await page.evaluate(() => OrderWin.setView('sheet')); await page.waitForFunction(() => OrderWin.view() === 'sheet'); await settled();
    const sc = await page.evaluate(() => OrderWin._scope());
    check(sc && sc.piece === K[0], 'the Sheet tab holds the same piece: ' + JSON.stringify(sc && [sc.piece, sc.sel]));
    await page.evaluate(() => OrderWin.setView('info')); await page.waitForFunction(() => OrderWin.view() === 'info'); await settled();
    check((await rowsOf()).filter(r => r.sel).map(r => r.key).join() === K[0] && (await stateText()) === 'Showing SPORTS 10 - FIGURE SKATE · Show all', 'back on the Overview the same row is marked');

    // ═══ 3 · the selected row again, or Show all, returns to all pieces ═══
    await press(K[0], 'click');
    await page.waitForFunction(() => OrderWin.selectedPiece() === null, null, { timeout: 10000 }); await settled();
    R = await rowsOf(); D = await detail();
    check(D.view === 'info' && R.length === 3 && R.every(r => !r.sel && r.cur === null && r.pressed === 'false') && (await stateText()) === 'Showing all 6 pieces' && D.laserDone === false, 'the selected row pressed again: all pieces, the order is where its slowest piece is, no row marked: ' + JSON.stringify([D, await stateText()]));
    await page.evaluate(() => OrderWin.setView('timeline')); await page.waitForFunction(() => document.querySelectorAll('#owTimeline .tlSt[data-key]').length > 0, null, { timeout: 15000 }); await settled();
    const tAll = await tlCount();
    check(tOne > 0 && tAll > tOne, `the Timeline shows one piece's steps when it is selected (${tOne}) and all of them otherwise (${tAll})`);
    await page.evaluate(() => OrderWin.setView('info')); await page.waitForFunction(() => OrderWin.view() === 'info'); await settled();
    await press(K[1], 'name'); await page.waitForFunction(k => OrderWin.selectedPiece() === k, K[1]); await settled();
    await page.click('#owPcSum [data-pc-all]');
    await page.waitForFunction(() => OrderWin.selectedPiece() === null, null, { timeout: 10000 }); await settled();
    check((await rowsOf()).every(r => !r.sel) && (await stateText()) === 'Showing all 6 pieces', '"Show all" returns to all pieces');

    // ═══ 4 · the keyboard ═══
    await page.focus(`${rowSel(K[0])} .owPcName`);
    await page.keyboard.press('Enter');
    await page.waitForFunction(k => OrderWin.selectedPiece() === k, K[0], { timeout: 10000 }); await settled();
    check(await page.evaluate(k => document.activeElement === document.querySelector(`#owPcSum .owPcRow[data-piece="${k}"] .owPcName`), K[0]), 'Enter on a row\'s name picks it and the focus stays on it');
    await page.keyboard.press('ArrowDown');
    await page.waitForFunction(k => OrderWin.selectedPiece() === k, K[1], { timeout: 10000 }); await settled();
    check(await page.evaluate(k => document.activeElement === document.querySelector(`#owPcSum .owPcRow[data-piece="${k}"] .owPcName`), K[1]), 'Arrow Down moves the pick to the next row and the focus with it');
    await page.keyboard.press('ArrowDown'); await page.waitForFunction(k => OrderWin.selectedPiece() === k, K[2], { timeout: 10000 }); await settled();
    await page.keyboard.press('ArrowDown'); await page.waitForTimeout(150);
    check((await detail()).sel === K[2], 'Arrow Down on the last row stays on it');
    await page.keyboard.press('ArrowUp'); await page.waitForFunction(k => OrderWin.selectedPiece() === k, K[1], { timeout: 10000 }); await settled();
    await page.keyboard.press('Home'); await page.waitForFunction(k => OrderWin.selectedPiece() === k, K[0], { timeout: 10000 }); await settled();
    await page.keyboard.press('End'); await page.waitForFunction(k => OrderWin.selectedPiece() === k, K[2], { timeout: 10000 }); await settled();
    check(true, 'Arrow Up, Home and End move the pick too');
    await page.keyboard.press('Space');
    await page.waitForFunction(() => OrderWin.selectedPiece() === null, null, { timeout: 10000 }); await settled();
    check(await page.evaluate(k => document.activeElement === document.querySelector(`#owPcSum .owPcRow[data-piece="${k}"] .owPcName`), K[2]) && (await rowsOf()).every(r => r.pressed === 'false'), 'Space on the selected row\'s name shows all pieces again, the focus stays');
    check(await page.evaluate(() => [...document.querySelectorAll('#owPcSum .owPcName')].every(b => b.tagName === 'BUTTON' && b.matches('[aria-pressed]'))), 'every row has a real button for its name (aria-pressed): reachable by Tab');

    // ═══ 5 · what is inside a row is its own: it does not pick the piece ═══
    const sel0 = (await detail()).sel;
    await page.evaluate(k => {   // (stand-ins for what rows hold: a button of the card, a seal, a status chip, a quiet note)
      const row = document.querySelector(`#owPcSum .owPcRow[data-piece="${k}"]`), mk = (tag, att, text) => { const e = document.createElement(tag); for (const [a, v] of Object.entries(att)) e.setAttribute(a, v); e.textContent = text; e.style.cssText = 'display:inline-block;min-width:30px;min-height:14px;position:relative;z-index:3;pointer-events:auto'; return e; };
      window.__hits = { btn: 0, seal: 0, own: 0 };
      const b = mk('button', { type: 'button', id: 'tBtn' }, 'Test button'), s = mk('span', { class: 'seal', id: 'tSeal' }, 'seal'), o = mk('span', { 'data-no-select': '', id: 'tOwn' }, 'own');
      b.onclick = () => __hits.btn++; s.onclick = () => __hits.seal++; o.onclick = () => __hits.own++;
      row.appendChild(b); row.appendChild(s); row.appendChild(o);
    }, K[1]);
    for (const [id, name] of [['#tBtn', 'a button'], ['#tSeal', 'a seal'], ['#tOwn', 'a data-no-select part']]) { await page.click(`#owPcSum ${id}`); await page.waitForTimeout(120); check((await detail()).sel === sel0 && (await rowsOf()).every(r => !r.sel), `a press on ${name} in a row does not pick the piece`); }
    check((await page.evaluate(() => __hits)).btn === 1 && (await page.evaluate(() => __hits)).seal === 1 && (await page.evaluate(() => __hits)).own === 1, 'and each of them still got its press');
    const dots = await page.locator(`${rowSel(K[2])} .steps`).first().boundingBox();
    await page.mouse.click(dots.x + 4, dots.y + dots.height / 2); await page.waitForTimeout(150);
    check((await detail()).sel === sel0, 'a press on the dots does not pick the piece');
    // (a row's body does)
    await page.evaluate(() => { for (const id of ['tBtn', 'tSeal', 'tOwn']) document.getElementById(id)?.remove(); });
    await press(K[2], 'click'); await page.waitForFunction(k => OrderWin.selectedPiece() === k, K[2], { timeout: 10000 }); await settled();
    check(true, 'while a press on the row body still picks it');
    await page.evaluate(() => OrderWin.selectPiece(null)); await settled();

    // ═══ 6 · the API ═══
    check(await page.evaluate(() => OrderWin.selectPiece('nope') === false && OrderWin.selectedPiece() === null), 'selectPiece of a piece that is not in the order is refused');
    check(await page.evaluate(k => OrderWin.selectPiece(k) === true && OrderWin.selectedPiece() === k, K[1]), 'OrderWin.selectPiece(key) shows it, selectedPiece() says so');
    await settled(); R = await rowsOf(); check(R.filter(r => r.sel).map(r => r.key).join() === K[1] && (await stateText()) === 'Showing SNAKE 5 · Show all', 'and the list marks it');
    check(await page.evaluate(() => OrderWin.selectPiece(null) === true && OrderWin.selectedPiece() === null), 'selectPiece(null) shows all pieces'); await settled();

    // ═══ 7 · opened on a piece (the dots, the sheet window, the searches: `pick`) ═══
    await closeWin();
    await page.evaluate(([rid, k]) => OrderWin.openOrder(rid, { row: { key: k }, pick: k }), [R3, K[1]]);
    await page.waitForFunction(k => OrderWin.isOpen() && OrderWin.selectedPiece() === k && document.querySelector(`#owPcSum .owPcRow.sel[data-piece="${k}"]`), K[1], { timeout: 20000 }); await settled();
    check((await rowsOf()).filter(r => r.sel).map(r => r.key).join() === K[1] && (await detail()).key === K[1] && (await stateText()) === 'Showing SNAKE 5 · Show all', 'openOrder(rid, { row, pick }) opens with that piece\'s row selected, its detail shown');
    await closeWin();
    await page.evaluate(k => OrderWin.open(k, { pick: true }), K[2]);
    await page.waitForFunction(k => OrderWin.isOpen() && OrderWin.selectedPiece() === k && document.querySelector(`#owPcSum .owPcRow.sel[data-piece="${k}"]`), K[2], { timeout: 20000 }); await settled();
    check((await detail()).key === K[2], 'OrderWin.open(key, { pick }) the same');
    await closeWin();
    await page.evaluate(k => OrderWin.open(k), K[1]);
    await page.waitForFunction(k => OrderWin.isOpen() && document.querySelectorAll('#owPcSum .owPcRow[data-piece]').length === 3 && OrderWin.selectedPiece() === null, K[1], { timeout: 20000 }); await settled();
    check(true, 'opened without `pick` it shows all pieces');

    // ═══ 8 · the Hold buttons in the rows (the plain rows: pieces on sheets, in no Review card) are theirs too ═══
    await page.evaluate(() => { window.__plans = 0; window.OrderHold = { plan: () => { window.__plans++; return new Promise(() => {}); }, run: async () => ({ ok: false }), status: () => ({ running: false }) }; OrderWin.paint(); });
    await page.waitForFunction(() => document.querySelectorAll('#owPcSum .owPcRow [data-hold-btn]').length === 3, null, { timeout: 15000 });
    check((await rowsOf()).length === 3 && await page.evaluate(() => [...document.querySelectorAll('#owPcSum .owPcRow')].every(r => r.querySelector('.pcAct [data-hold-btn]') && r.querySelector('.owPcName'))), 'with the engine in, each row has its orange Hold at the right end and its name button: the same list');
    await press(K[2], 'click'); await page.waitForFunction(k => OrderWin.selectedPiece() === k, K[2], { timeout: 10000 }); await settled();
    check((await rowsOf()).filter(r => r.sel).map(r => r.key).join() === K[2], 'a press on the body of a row that has a Hold picks the piece');
    await page.click(`${rowSel(K[0])} [data-hold-btn]`);
    await page.waitForFunction(() => window.__plans === 1, null, { timeout: 8000 }); await page.waitForTimeout(250);
    check((await detail()).sel === K[2] && (await rowsOf()).filter(r => r.sel).map(r => r.key).join() === K[2], 'with one piece picked, a press on another row\'s Hold does Hold (the plan is read once) and does not switch the pick');
    await page.keyboard.press('Escape').catch(() => {});
    await shot('with-hold-one-selected-1440');
    check(await page.evaluate(() => document.querySelectorAll('#owPcSum button button').length === 0), 'no button inside a button');
    await page.evaluate(() => { delete window.OrderHold; OrderWin.selectPiece(null); }); await settled();

    // ═══ 9 · a single piece: one neutral row, nothing to pick ═══
    await closeWin();
    await page.evaluate(() => { window.OrderHold = { plan: async () => ({ canHold: false }), run: async () => ({ ok: false }), status: () => ({ running: false }) }; });
    await openWin(KS);
    await page.waitForFunction(() => document.querySelector('#owPcSum') && !document.getElementById('owPcSum').hidden, null, { timeout: 15000 });
    R = await rowsOf();
    check(R.length === 1 && R[0].solo && !R[0].sel && R[0].key === null && !(await page.evaluate(() => document.querySelector('#owPcSum .owPcState, #owPcSum [data-pc-all]'))), 'a single-piece order: one neutral row, no state line, no "Show all": ' + JSON.stringify(R));
    await page.click('#owPcSum .owPcRow'); await page.waitForTimeout(150);
    check(await page.evaluate(() => OrderWin.selectedPiece() === null && !document.querySelector('#owPcSum .owPcRow.sel')) && (await chipRow()).length === 0, 'pressing it changes nothing; no chip row');
    check(await page.evaluate(() => OrderWin.selectPiece('x') === false && OrderWin.selectedPiece() === null), 'selectPiece on a single-piece order is refused');
    await page.evaluate(() => { delete window.OrderHold; }); await closeWin();

    // ═══ 10 · fits 1440, 900 and 390 ═══
    const fit = async label => {
      const m = await page.evaluate(() => {
        const box = document.getElementById('owPcSum'), br = box.getBoundingClientRect(), win = document.getElementById('orderWin').getBoundingClientRect(), st = box.querySelector('.owPcState'), sr = st && st.getBoundingClientRect(), hd = box.querySelector('.owPcHd').getBoundingClientRect();
        const rows = [...box.querySelectorAll('.owPcRow')].map(r => ({ clip: r.scrollWidth > r.clientWidth + 1, right: Math.round(r.getBoundingClientRect().right), h: Math.round(r.getBoundingClientRect().height) }));
        return { vw: innerWidth, win: Math.round(win.width), boxL: Math.round(br.left), boxR: Math.round(br.right), stR: sr ? Math.round(sr.right) : 0, stIn: !sr || (sr.left >= br.left - 1 && sr.right <= br.right + 1), hdIn: hd.right <= br.right + 1, rows, clip: rows.some(r => r.clip), pageOver: document.documentElement.scrollWidth > innerWidth + 1 };
      });
      check(!m.clip && m.stIn && m.hdIn && m.rows.every(r => r.right <= m.boxR + 1) && !m.pageOver, `${label}: the list, its head and its state line fit; no row is clipped; no page scroll (window ${m.win}px of ${m.vw}px): ` + JSON.stringify({ boxL: m.boxL, boxR: m.boxR, stR: m.stR, h: m.rows.map(r => r.h) }));
      return m;
    };
    for (const [w, h] of [[1440, 900], [900, 800], [390, 844]]) {
      await page.setViewportSize({ width: w, height: h });
      await openWin(K[0]); await waitRows(3); await settled();
      await fit(`${w}px, all pieces`);
      await press(K[1], 'name'); await page.waitForFunction(k => OrderWin.selectedPiece() === k, K[1], { timeout: 10000 }); await settled();
      const m = await fit(`${w}px, one piece picked`);
      const mk = await markVisible(K[1]); check(mk.ok, `${w}px: the row shown is marked: ` + JSON.stringify(mk));
      if (w === 390) { await page.evaluate(() => { const b = document.getElementById('owPcSum'); b.scrollIntoView({ block: 'center' }); }); await shot('one-selected-390'); }
      else await shot(`one-selected-${w}`);
      await closeWin();
    }
    await page.setViewportSize({ width: 1440, height: 900 });

    // ═══ 11 · mutants: the same checks must fail on a build that keeps a chip row, or marks nothing ═══
    await openWin(K[0]); await waitRows(3); await settled();
    await page.evaluate(() => { const sw = document.createElement('div'); sw.className = 'owPieceSw'; sw.id = 'owPieceSw'; sw.innerHTML = '<button type="button" data-piece="">All 6 pieces</button><button type="button" data-piece="x">SNAKE 5</button>'; document.querySelector('.owTabsV').appendChild(sw); });
    let bad = await chipRow();
    check(bad.length >= 2 && bad.some(b => /#owPieceSw/.test(b)) && bad.some(b => /All 6 pieces/.test(b)), 'mutant: a build that keeps the chip row is caught by the chip row check: ' + JSON.stringify(bad));
    await page.evaluate(() => document.getElementById('owPieceSw').remove());
    check((await chipRow()).length === 0, 'the mutant removed, the check passes again');
    await press(K[1], 'name'); await page.waitForFunction(k => OrderWin.selectedPiece() === k, K[1]); await settled();
    await page.evaluate(() => { document.querySelectorAll('#owPcSum .owPcRow.sel').forEach(r => r.classList.remove('sel')); });
    mv = await markVisible(K[1]);
    check(!mv.ok, 'mutant: a build that does not mark the row shown is caught by the mark check: ' + JSON.stringify(mv));
    await closeWin();

    check(outside.length === 0, 'no Etsy call'); check(errors.length === 0, 'no page errors: ' + errors.join(' | '));
  } finally { await browser.close(); srv.close(); }
  if (fails.length) { console.error('\nFAILED: ' + fails.length); process.exit(1); }
  console.log('Order piece selector OK');
})().catch(e => { console.error(e); process.exit(1); });
