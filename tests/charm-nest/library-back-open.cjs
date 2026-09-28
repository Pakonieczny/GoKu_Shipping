// A sheet looked at from its Back opens on its Back (Paul, 28 Sep 21:22: the Front / "Back · engraving" view everywhere a
// sheet is visible): a Library card turned over, clicked on a charm, opens the sheet window already on its Back, grown
// out of the card, with the charm clicked (found through the mirror) chosen.
//   node tests/charm-nest/library-back-open.cjs   (PW_DIR=<playwright node_modules>, CHROMIUM=<chrome>)
const path = require('path'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');
const { start } = require('./bridge-server.cjs');

const SH = 'sheet-libback-open', RID = '4173711093', TID = '41737110931', PA = `${RID}_${TID}_1`, PB = `${RID}_${TID}_2`;
const BACK = { poolId: PA, sheetId: SH, order: RID, sku: 'TINY_TAG', copy: 1, text: 'For Mia', lines: ['For Mia'], sizePt: 8, capMm: 2, weight: 'Regular', lineGap: .216, centre: [17, 17], angle: 0, upAngle: 90, approvedAt: Date.now() - 4000, approvedBy: 'Test Operator' };

(async () => {
  const pwDir = process.env.PW_DIR || path.join(root, 'node_modules');
  let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { console.log('  – no playwright-core: the browser check was not run'); return; }
  const srv = await start({ receipts: [] });
  const charms = [{ id: 'g1', name: `${RID} · TINY_TAG · 1/2`, poolId: PA, order: RID, sku: 'TINY_TAG' }, { id: 'g2', name: `${RID} · TINY_TAG · 2/2`, poolId: PB, order: RID, sku: 'TINY_TAG' }];
  const placements = [['g1', 60, 60], ['g2', 200, 80]].map(([id, cx, cy]) => ({ id, cxPt: cx, cyPt: cy, angle: 0, wPt: 34, hPt: 34 }));
  srv.st.put('Charm_Nest_Sheets', SH, { id: SH, metal: 'gold', metalLabel: '14k Gold Filled', sheetIndex: 1, day: '2026-09-28', status: 'written', stock: { wPt: 300, hPt: 150 },
    charmCount: 2, placedCount: 2, density: .2, freePt2: 30000, orders: [RID], poolIds: [PA, PB], placements, charms, backPool: [BACK], updatedAt: Date.now(), createdAt: Date.now() });
  srv.st.put('Charm_Pool_Back', PA, BACK);
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  try {
    const context = await browser.newContext({ viewport: { width: 1440, height: 900 }, deviceScaleFactor: 1 });
    await context.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => /fonts\.googleapis|fonts\.gstatic/.test(r.request().url()) ? r.fulfill({ status: 200, contentType: 'text/css', body: '' }) : r.abort());
    await context.addInitScript(() => { try { if (!sessionStorage.getItem('__seeded')) { localStorage.setItem('cn.settings', JSON.stringify({ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on' })); localStorage.setItem('cn.employee', 'Test Operator'); sessionStorage.setItem('__seeded', '1'); } } catch (_) {} window.prompt = () => 'Test Operator'; });
    const page = await context.newPage(), errors = [];
    page.setDefaultTimeout(20000);
    page.on('pageerror', e => { errors.push(e.message); console.error('page error:', String(e.stack || e.message).split('\n').slice(0, 4).join(' | ')); });
    await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
    await page.waitForFunction(() => window.CN && window.SheetWin && window.LibraryBacks && CN.S.cloud.ok === true, null, { timeout: 60000 });
    await page.evaluate(({ SH, charms, placements }) => {
      const sq = { kind: 'path', subpaths: [[['m', [0, 0]], ['l', [34, 0]], ['l', [34, 34]], ['l', [0, 34]], ['h']]], stroke: true, strokeRGB: [0, 0, 0], lwPt: .25, bbox: [0, 0, 34, 34] };
      S.sheets.gold.pages.push({ sheetId: SH, metal: 'gold', sheetIndex: 1, page: 1, status: 'complete', placements, rejects: [],
        charms: charms.map(c => Object.assign({}, c, { centerPt: [17, 17], widthPt: 34, heightPt: 34, outline: sq, members: [sq] })) });
      CN.setMode('library');
    }, { SH, charms, placements });
    await page.waitForSelector(`#libBody .libCard[data-id="${SH}"] .pvTurn`);
    await page.click('#libFace [data-face="back"]');
    await page.waitForFunction(id => { const k = LibraryBacks._L.drawn.get(id), card = document.querySelector(`#libBody .libCard[data-id="${id}"]`);
      return k && !k.pending && k.info && card.querySelector('.pvTurn.back') && !card.querySelector('.pvTurn').getAnimations().some(a => a.playState === 'running'); }, SH, { timeout: 30000 });
    // the first charm lies at 60 pt of the 300 pt sheet's width: from behind it is at 80 % across the card's picture
    const at = await page.evaluate(id => { const k = LibraryBacks._L.drawn.get(id), q = k.info.pointOf(k.info.pieces.find(x => x.id === 'g1').poolId); return q; }, SH);
    await page.mouse.click(at.x, at.y);
    await page.waitForFunction(() => { const W = SheetWin._W; return W.dlg && W.dlg.open && W.rec && W.sel && W.geom; }, null, { timeout: 30000 });
    const got = await page.evaluate(() => {
      const W = SheetWin._W, f = W.el.strip.querySelector('[data-r2=face]');
      return { face: W.face, grew: W.dlg.classList.contains('swGrow'), sel: W.sel && W.sel.poolId, btn: f && f.getAttribute('aria-pressed'), label: f && f.textContent, pv: getComputedStyle(W.el.pv).opacity };
    });
    assert.equal(got.face, 'back', 'the sheet window opens on its Back');
    assert(got.grew, 'it grew out of the card it was opened from');
    assert.equal(got.sel, PA, 'the charm clicked on the turned card is the one chosen');
    assert.equal(got.btn, 'true', 'its own switch says Back');
    assert.equal(got.label, 'Front', 'its switch offers the Front');
    assert.equal(got.pv, '0', 'the front picture is not shown on the Back');
    assert.deepEqual(errors, [], 'no page errors');
    console.log('  ✓ a turned Library card opens the sheet window on its Back, on the charm clicked');
  } finally { await browser.close(); srv.close(); }
})().then(() => console.log('Library back open OK')).catch(e => { console.error(e); process.exit(1); });
