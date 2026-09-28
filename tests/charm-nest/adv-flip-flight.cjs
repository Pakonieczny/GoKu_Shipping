// Front | Back · engraving, one turn at a time on every sheet view (Paul, 28 Sep: "a beautiful animation on every sheet
// view"). Back then Front pressed quickly must run ONE turn (never a second one started over the first) and end on the
// side asked for last, in: the order view's Sheet tab (#owFace), the Sets window (#hFace), the Sets menu preview
// (SetPicker.preview), the Library's bar and card switches (LibraryBacks) and the Nest card (turnCard).
// And the turn never waits on a download: the engraving font is asked for before the first turn, and the sheet's backs
// are read as the pointer comes to the switch, before it is pressed.
//   node tests/charm-nest/adv-flip-flight.cjs   (PW_DIR=<playwright node_modules>, CHROMIUM=<chrome>)
const path = require('path'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');
const { start } = require('./bridge-server.cjs');

const DAY = 86400, SHIP = Math.floor(Date.UTC(2026, 9, 2, 17) / 1000);
const SH = 'sheet-flipflight-gf', RID = '4173711092', TID = '41737110921', PA = `${RID}_${TID}_1`, PB = `${RID}_${TID}_2`;
const BACK = { poolId: PA, sheetId: SH, order: RID, sku: 'TINY_TAG', copy: 1, text: 'For Mia', lines: ['For Mia'], sizePt: 8, capMm: 2, weight: 'Regular', lineGap: .216, centre: [17, 17], angle: 0, upAngle: 90, approvedAt: Date.now() - 4000, approvedBy: 'Test Operator' };
const lineOf = (tid, sku) => ({ transactionId: tid, listingId: '18000' + tid.slice(-5), sku, title: sku + ' necklace', quantity: 2, expectedShipDate: SHIP, variations: [{ name: 'Metal', value: '14k Gold Filled' }], metalKey: 'gold', metalLabel: '14k Gold Filled', personalization: [], buyerMessage: '' });
const orderOf = (rid, buyer, lines) => ({ receiptId: rid, orderNumber: rid, createTs: SHIP - 5 * DAY, updateTs: SHIP - 5 * DAY + 60, shipBy: SHIP, buyer: { name: buyer }, buyerMessage: '', isGift: false, giftMessage: '', staffNote: '', messages: [], lines });

(async () => {
  const pwDir = process.env.PW_DIR || path.join(root, 'node_modules');
  let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { console.log('  – no playwright-core: the browser check was not run'); return; }
  const srv = await start({ receipts: [] });
  const charms = [{ id: 'g1', name: `${RID} · TINY_TAG · 1/2`, poolId: PA, order: RID, sku: 'TINY_TAG' }, { id: 'g2', name: `${RID} · TINY_TAG · 2/2`, poolId: PB, order: RID, sku: 'TINY_TAG' }];
  const placements = [['g1', 60, 60], ['g2', 200, 80]].map(([id, cx, cy]) => ({ id, cxPt: cx, cyPt: cy, angle: 0, wPt: 34, hPt: 34 }));
  srv.st.put('Charm_Nest_Sheets', SH, { id: SH, metal: 'gold', metalLabel: '14k Gold Filled', sheetIndex: 1, day: new Date().toISOString().slice(0, 10), status: 'written', stock: { wPt: 300, hPt: 150 },
    charmCount: 2, placedCount: 2, density: .2, freePt2: 30000, orders: [RID], poolIds: [PA, PB], placements, charms, backPool: [BACK], preview: '/flipflight.png', outputs: { preview: { url: '/flipflight.png' } }, updatedAt: Date.now(), createdAt: Date.now() });
  srv.st.put('Charm_Pool_Back', PA, BACK);
  for (const p of [PA, PB]) srv.st.put('Charm_Pool', p, { poolId: p, orderId: RID, transactionId: TID, lineKey: `${RID}_${TID}`, sku: 'TINY_TAG', material: 'gold', copy: +p.split('_').pop(), quantity: 2, state: 'written', sheetId: SH, sheetName: '2026-09-28_GF_Set-1_Sheet-1', updatedAt: Date.now() });
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  try {
    const context = await browser.newContext({ viewport: { width: 1440, height: 900 } });
    let png = null;
    await context.route(u => /flipflight\.png/.test(u.href), r => png ? r.fulfill({ status: 200, contentType: 'image/png', headers: { 'Access-Control-Allow-Origin': '*' }, body: png }) : r.abort());
    await context.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => /fonts\.googleapis|fonts\.gstatic/.test(r.request().url()) ? r.fulfill({ status: 200, contentType: 'text/css', body: '' }) : r.abort());
    await context.addInitScript(() => { try { if (!sessionStorage.getItem('__seeded')) { localStorage.setItem('cn.settings', JSON.stringify({ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on' })); localStorage.setItem('cn.employee', 'Test Operator'); sessionStorage.setItem('__seeded', '1'); } } catch (_) {} window.prompt = () => 'Test Operator';
      // when the engraving font is first asked for, and when a Front | Back switch is first pressed
      const f0 = window.fetch; window.fetch = function (u) { if (/SourceSans3-Regular/.test(String(u && u.url || u)) && !window.__fontAt) window.__fontAt = performance.now(); return f0.apply(this, arguments); };
      addEventListener('click', e => { if (!window.__turnAt && e.target.closest && e.target.closest('[data-face],[data-sp-face],[data-r2=face]')) window.__turnAt = performance.now(); }, true); });
    const page = await context.newPage(), errors = [];
    page.setDefaultTimeout(20000);
    page.on('pageerror', e => { errors.push(e.message); console.error('page error:', String(e.stack || e.message).split('\n').slice(0, 4).join(' | ')); });
    await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
    await page.waitForFunction(() => window.CN && window.Orders && window.OrderWin && window.RunHistory && window.SetPicker && window.LibraryBacks && window.SheetWin && SheetWin.drawOrder && SheetWin.armBack && CN.S.cloud.ok === true, null, { timeout: 60000 });
    png = Buffer.from(await page.evaluate(() => { const c = document.createElement('canvas'); c.width = 600; c.height = 300; const x = c.getContext('2d'); x.fillStyle = '#fff'; x.fillRect(0, 0, 600, 300); return c.toDataURL('image/png').split(',')[1]; }), 'base64');
    // the sheet as this sorter holds it on its page (each charm has its drawing), and its order in the Orders tab
    await page.evaluate(async ({ SH, charms, placements, order, pools }) => {
      const sq = { kind: 'path', subpaths: [[['m', [0, 0]], ['l', [34, 0]], ['l', [34, 34]], ['l', [0, 34]], ['h']]], stroke: true, strokeRGB: [0, 0, 0], lwPt: .25, bbox: [0, 0, 34, 34] };
      S.sheets.gold.pages.push({ sheetId: SH, metal: 'gold', sheetIndex: 1, page: 1, status: 'complete', placements, rejects: [],
        charms: charms.map(c => Object.assign({}, c, { centerPt: [17, 17], widthPt: 34, heightPt: 34, outline: sq, members: [sq] })) });
      await Orders.loadMaps(true);
      for (const line of order.lines) { const key = CharmNestOrders.lineKey(order, line); const row = { key, order, line, arrivedAt: Date.now(), spec: null, problems: [], state: 'pooled', reason: null, claimedBy: null, poolIds: pools, engrave: null, material: null }; B.orders.rows.push(row); B.orders.byKey.set(key, row); }
      Orders.interpretAll(); CN.setMode('orders'); Orders.render();
    }, { SH, charms, placements, order: orderOf(RID, 'Mia Lund', [lineOf(TID, 'TINY_TAG')]), pools: [PA, PB] });

    /* In-page: Back then Front 120 ms apart, then every frame for 1.7 s: how many turns were started on each element (a
       turn is an animation from rotateY(0) to its edge) and the most running at once on any one of them. */
    const run = async (name, selOf, turning, done) => {
      const r = await page.evaluate(async ({ selOf, turning }) => {
        const sel = eval(selOf), els = () => [...document.querySelectorAll(turning)];
        const seen = new Set(); let most = 0, on = true, last = 0, slow = 0; const long = [];
        const po = new PerformanceObserver(l => { for (const e of l.getEntries()) long.push(Math.round(e.duration)); }); try { po.observe({ type: 'longtask' }); } catch (_) {}
        const tick = now => { if (last && seen.size) slow = Math.max(slow, now - last); last = now; for (const e of els()) { const run = e.getAnimations().filter(x => x.playState === 'running' && x.effect.getKeyframes().some(k => /rotateY/.test(k.transform || '')));
          most = Math.max(most, run.length); for (const x of run) { const k = x.effect.getKeyframes(); if (/rotateY\(0/.test(k[0].transform || '')) seen.add(x); } }
          if (on) requestAnimationFrame(tick); };
        requestAnimationFrame(tick);
        document.querySelector(sel('back')).click(); await new Promise(r => setTimeout(r, 120)); document.querySelector(sel('front')).click();
        await new Promise(r => setTimeout(r, 1700)); on = false; po.disconnect();
        return { turns: seen.size / Math.max(1, els().length), most, n: els().length, slow: Math.round(slow), long, still: els().some(e => e.getAnimations().some(x => x.playState === 'running')) };
      }, { selOf: selOf.toString(), turning });
      const end = await page.evaluate(done);
      console.log(`  ${name}: ${r.n} sheet(s), ${r.turns} turn(s) each, at most ${r.most} running at once · ends on ${end.face} · slowest frame ${r.slow} ms${r.long.length ? ' · long tasks ' + r.long.join(', ') + ' ms' : ''}`);
      assert(r.n >= 1, `${name}: a sheet turned`);
      assert.equal(r.turns, 1, `${name}: Back then Front pressed quickly is one turn (never a second started over the first)`);
      assert(r.most <= 1 && !r.still, `${name}: one animation at a time, and at rest after`);
      assert.equal(end.face, 'front', `${name}: it ends on the side asked for last`);
      assert(end.ok !== false, `${name}: the switch says so (${JSON.stringify(end)})`);
    };

    // ── 1 · the order view's Sheet tab: the backs are read as the pointer comes to Back, before the press
    await page.evaluate(k => OrderWin.open(k, { view: 'sheet' }), `${RID}_${TID}`);
    await page.evaluate(() => OrderWin.view() === 'sheet' || document.querySelector('.owTabsV [data-ow-view="sheet"]').click());
    await page.waitForFunction(sh => { const i = OrderWin._sheet(); return i && i.sheet.id === sh && i.pieces.every(x => x.c) && document.getElementById('owPlateWait').hidden; }, SH, { timeout: 30000 });
    await page.waitForTimeout(300);
    const lists = () => srv.st.calls.filter(c => c.name === 'charmNestLibrary' && c.op === 'backList' && JSON.stringify(c).includes(SH)).length;
    const l0 = lists();
    await page.hover('#owFace [data-face="back"]');
    await page.waitForTimeout(300);
    const l1 = lists();
    const t = await page.evaluate(() => ({ font: window.__fontAt || null, turn: window.__turnAt || null }));
    await run('order view Sheet tab', f => `#owFace [data-face="${f}"]`, '#owSheetCv',
      () => ({ face: /Hover a charm/.test(document.getElementById('owPlateFoot').textContent) ? 'front' : 'back', ok: document.querySelector('#owFace [data-face="front"]').getAttribute('aria-pressed') === 'true' }));
    const t2 = await page.evaluate(() => ({ font: window.__fontAt || null, turn: window.__turnAt || null }));
    console.log(`  engraving font asked for at ${t2.font && t2.font.toFixed(0)} ms, first turn at ${t2.turn && t2.turn.toFixed(0)} ms · backs read on hover: ${l1 - l0} (before the press)`);
    assert(t.turn == null, 'nothing pressed before');
    assert(t2.font != null && t2.turn != null && t2.font < t2.turn, 'the engraving font is asked for before the first turn');
    assert(l1 - l0 >= 1 || l0 >= 1, 'the sheet\'s backs are read as the pointer comes to Back, before it is pressed');
    await page.keyboard.press('Escape');
    await page.waitForFunction(() => !document.querySelector('dialog[open]'));

    // ── 2 · the Sets window
    await page.evaluate(() => RunHistory.show(''));
    await page.waitForSelector(`#histDlg .hTile[data-sheet="${SH}"] .hPlate`);
    await page.waitForTimeout(400);
    await run('Sets window', f => `#hFace [data-face="${f}"]`, '#histDlg .hPlate',
      () => { const d = document.getElementById('histDlg'); return { face: d.dataset.face === 'back' ? 'back' : 'front', ok: d.querySelector('#hFace [data-face="front"]').getAttribute('aria-pressed') === 'true' }; });
    await page.keyboard.press('Escape');
    await page.waitForFunction(() => !document.querySelector('#histDlg[open]'));

    // ── 3 · the Sets menu's preview
    await page.evaluate(sh => SetPicker.preview({ name: 'Set 1', day: '2026-09-28', status: 'written', sheets: [{ id: sh, stock: { wPt: 300, hPt: 150 }, metal: 'gold', preview: '/flipflight.png', placedCount: 2, fileBase: sh, page: S.sheets.gold.pages.find(p => p.sheetId === sh) }] }), SH);
    await page.waitForSelector('#setPreview .spPlate');
    await run('Sets menu preview', f => `#setPreview [data-sp-face="${f}"]`, '#setPreview .spPlate',
      () => ({ face: document.querySelector('#setPreview .spPlate.back') ? 'back' : 'front', ok: document.querySelector('#setPreview [data-sp-face="front"]').getAttribute('aria-pressed') === 'true' }));
    await page.keyboard.press('Escape');
    await page.waitForFunction(() => !document.querySelector('#setPreview[open]'));

    // ── 4 · the Library: the bar's switch, then the card's own
    await page.evaluate(() => CN.setMode('library'));
    const card = `#libBody .libCard[data-id="${SH}"]`;
    await page.waitForSelector(`${card} .pvTurn`);
    await page.waitForTimeout(400);
    const libEnd = () => { const c = '#libBody .libCard[data-id="sheet-flipflight-gf"]'; return { face: document.querySelector(c + ' .pvTurn.back') ? 'back' : 'front', ok: document.querySelector(c + ' .pvFace [data-face="front"]').getAttribute('aria-pressed') === 'true' }; };
    await run('Library bar', f => `#libFace [data-face="${f}"]`, `${card} .pvTurn`, libEnd);
    await page.hover(`${card} .pvTurn`);
    await run('Library card', f => `#libBody .libCard[data-id="sheet-flipflight-gf"] .pvFace [data-face="${f}"]`, `${card} .pvTurn`, libEnd);

    // ── 5 · the Nest card
    await page.evaluate(({ SH, PA, PB, BACK }) => {
      CN.setMode('nest');
      const sq = { kind: 'path', subpaths: [[['m', [0, 0]], ['l', [34, 0]], ['l', [34, 34]], ['l', [0, 34]], ['h']]], stroke: true, strokeRGB: [0, 0, 0], lwPt: .25, bbox: [0, 0, 34, 34] };
      const charm = (id, poolId) => ({ id, name: poolId.split('_')[0] + ' · TAG', poolId, order: poolId.split('_')[0], metal: 'silver', centerPt: [17, 17], widthPt: 34, heightPt: 34, areaPt2: 34 * 34, outline: sq, members: [sq], thumb: '' });
      const pg = S.sheets.silver;
      Object.assign(pg, { sheetId: SH + '-n', status: 'complete', charms: [charm('n1', PA), charm('n2', PB)], backPool: [Object.assign({}, BACK, { sheetId: SH + '-n' })],
        placements: [{ id: 'n1', cxPt: 60, cyPt: 60, angle: 0, wPt: 34, hPt: 34 }, { id: 'n2', cxPt: 150, cyPt: 60, angle: 0, wPt: 34, hPt: 34 }] });
      CN.showPage('silver', 0);
    }, { SH, PA, PB, BACK });
    const nc = '.sheetCard[data-m="silver"]';
    await page.waitForFunction(c => { const s = document.querySelector(c + ' [data-r="face"]'); return s && !s.hidden; }, nc);
    await page.waitForTimeout(300);
    await run('Nest card', f => `.sheetCard[data-m="silver"] [data-r="face"] [data-face="${f}"]`, `${nc} [data-r="canvas"]`,
      () => { const c = '.sheetCard[data-m="silver"]', cv = document.querySelector(c + ' [data-r="canvas"]'); return { face: cv._face === 'back' ? 'back' : 'front', ok: document.querySelector(c + ' [data-r="face"] [data-face="front"]').getAttribute('aria-pressed') === 'true' }; });

    // ── 6 · words that land while a sheet turns: it comes round on its plain back first, then they fade in (never a pop)
    const land = await page.evaluate(async () => {
      const host = document.createElement('div'); host.style.cssText = 'position:fixed;left:0;top:0;width:300px;height:200px;z-index:9999';
      const cv = document.createElement('canvas'); cv.width = 200; cv.height = 100; cv.style.cssText = 'position:absolute;left:10px;top:10px;width:200px;height:100px'; host.appendChild(cv); document.body.appendChild(host);
      const t0 = performance.now(); let at = 0;
      const a = cv.animate([{ transform: 'rotateY(0)' }, { transform: 'rotateY(90deg)' }], { duration: 300 });
      Motion.landIn(cv, () => { at = performance.now() - t0; });
      const early = at; await a.finished; await new Promise(r => requestAnimationFrame(r));
      const over = host.querySelector('canvas.mLand'), fading = !!over && over.getAnimations().some(x => x.effect.getKeyframes().some(k => k.opacity != null));
      await new Promise(r => setTimeout(r, 600)); const gone = !host.querySelector('canvas.mLand'); host.remove();
      return { early, at: Math.round(at), fading, gone };
    });
    console.log(`  words landing mid-turn: drawn at ${land.at} ms (after the turn), faded in over the plain back: ${land.fading}`);
    assert(land.early === 0 && land.at >= 280, 'words that land mid-turn wait for the sheet to come round');
    assert(land.fading && land.gone, 'then fade in over the plain back, and the copy is gone after');
    assert.deepEqual(errors, [], 'no page errors');
    console.log('  ✓ one turn per double press in every sheet view, ending on the side asked for last; the font asked for first');
  } finally { await browser.close(); srv.close(); }
})().then(() => console.log('adv flip flight OK')).catch(e => { console.error(e); process.exit(1); });
