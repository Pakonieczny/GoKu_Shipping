// A Nest sheet card turns to its Back like the sheet window and the order view (Paul, 28 Sep: "I should be able to see
// the backings everywhere ... The sheet is visible"): the card's Front | Back · engraving switch sits in its controls row
// without adding height, and turned over, the charm with a back text shows its words in ink where the front had none,
// while the plain charm stays plain.
//   node tests/charm-nest/nest-card-back.cjs   (PW_DIR=<playwright node_modules>, CHROMIUM=<chrome>)
const path = require('path'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');
const { start } = require('./bridge-server.cjs');

const SH = 'sheet-nest-card-back';
const PA = '4173711092_41737110921_1', PB = '4173711093_41737110931_1';
const BACK = { sheetId: SH, poolId: PA, order: '4173711092', sku: 'TINY_TAG', copy: 1, text: 'For Mia', lines: ['For Mia'], sizePt: 7, capMm: 1.7, weight: 'Regular', lineGap: .216, centre: [17, 17], angle: 0, upAngle: 90, approvedAt: Date.now() - 4000, approvedBy: 'Test Operator' };

(async () => {
  const pwDir = process.env.PW_DIR || path.join(root, 'node_modules');
  let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { console.log('  – no playwright-core: the browser check was not run'); return; }
  const srv = await start({ receipts: [] });
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  try {
    const context = await browser.newContext({ viewport: { width: 1440, height: 900 } });
    await context.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => /fonts\.googleapis|fonts\.gstatic/.test(r.request().url()) ? r.fulfill({ status: 200, contentType: 'text/css', body: '' }) : r.abort());
    await context.addInitScript(() => { try { if (!sessionStorage.getItem('__seeded')) { localStorage.setItem('cn.settings', JSON.stringify({ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on' })); localStorage.setItem('cn.employee', 'Test Operator'); sessionStorage.setItem('__seeded', '1'); } } catch (_) {} window.prompt = () => 'Test Operator'; });
    const page = await context.newPage(), errors = [];
    page.setDefaultTimeout(20000);
    page.on('pageerror', e => { errors.push(e.message); console.error('page error:', String(e.stack || e.message).split('\n').slice(0, 4).join(' | ')); });
    await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
    await page.waitForFunction(() => window.CN && window.SheetWin && SheetWin.cardBack && CN.S.cloud.ok === true, null, { timeout: 60000 });
    // two square charms on the gold card's sheet, as the sorter holds them: one with an approved back, one plain
    const before = await page.evaluate(({ SH, PA, PB, BACK }) => {
      CN.setMode('nest');
      const sq = { kind: 'path', subpaths: [[['m', [0, 0]], ['l', [34, 0]], ['l', [34, 34]], ['l', [0, 34]], ['h']]], stroke: true, strokeRGB: [0, 0, 0], lwPt: .25, bbox: [0, 0, 34, 34] };
      const charm = (id, poolId) => ({ id, name: poolId.split('_')[0] + ' · TAG', poolId, order: poolId.split('_')[0], metal: 'gold', centerPt: [17, 17], widthPt: 34, heightPt: 34, areaPt2: 34 * 34, outline: sq, members: [sq], thumb: '' });
      const pg = S.sheets.gold;
      Object.assign(pg, { sheetId: SH, status: 'complete', charms: [charm('n1', PA), charm('n2', PB)], backPool: [BACK],
        placements: [{ id: 'n1', cxPt: 60, cyPt: 60, angle: 0, wPt: 34, hPt: 34 }, { id: 'n2', cxPt: 150, cyPt: 60, angle: 0, wPt: 34, hPt: 34 }] });
      CN.showPage('gold', 0);
      return document.querySelector('.sheetCard[data-m="gold"] .shControls').getBoundingClientRect().height;
    }, { SH, PA, PB, BACK });
    const card = '.sheetCard[data-m="gold"]';
    await page.waitForFunction(c => { const s = document.querySelector(c + ' [data-r="face"]'); return s && !s.hidden; }, card);
    // ink (dark pixels) in a box round each charm's centre, through the card's own view (mirrored when turned over)
    const inkAt = () => page.evaluate(c => {
      const cv = document.querySelector(c + ' [data-r="canvas"]'), sh = S.sheets.gold, v = sh._view, ctx = cv.getContext('2d');
      const W = cv._face === 'back', sheetW = stockFor('gold', sh).wPt;
      const at = p => { const x = v.R + (W ? sheetW - p.cxPt : p.cxPt) * v.k, y = v.R + p.cyPt * v.k, h = Math.round(9 * v.k);
        const d = ctx.getImageData(Math.round(x) - h, Math.round(y) - h, 2 * h + 1, 2 * h + 1).data; let n = 0; for (let j = 0; j < d.length; j += 4) if (d[j] + d[j + 1] + d[j + 2] < 330) n++; return n; };
      return { face: cv._face || 'front', a: at(sh.placements[0]), b: at(sh.placements[1]) };
    }, card);
    const front = await inkAt();
    assert.equal(front.a, 0, 'no ink in the middle of the charm from the front: ' + front.a);
    await page.click(`${card} [data-r="face"] [data-face="back"]`);
    await page.waitForFunction(c => { const cv = document.querySelector(c + ' [data-r="canvas"]'), seg = document.querySelector(c + ' [data-r="face"]');
      return cv._face === 'back' && !cv._turning && seg.querySelector('.spin').hidden && window.Engrave && Engrave.fonts && Engrave.fonts.ok; }, card, { timeout: 30000 });
    await page.evaluate(() => new Promise(r => requestAnimationFrame(() => requestAnimationFrame(r))));
    const back = await inkAt();
    if (process.env.SHOT) await page.locator(card).screenshot({ path: process.env.SHOT });
    assert(back.a >= 4, `turned over, the charm with a back text shows its words in ink (${back.a} dark pixels)`);
    assert.equal(back.b, 0, 'the charm with no engraving stays plain from behind: ' + back.b);
    const after = await page.evaluate(c => ({ h: document.querySelector(c + ' .shControls').getBoundingClientRect().height, on: document.querySelector(c + ' [data-face="back"]').getAttribute('aria-pressed') }), card);
    assert.equal(after.on, 'true');
    assert(Math.abs(after.h - before) < 1, `the switch adds no height to the card's controls row (${before} → ${after.h})`);
    // and round again to the Front
    await page.click(`${card} [data-r="face"] [data-face="front"]`);
    await page.waitForFunction(c => { const cv = document.querySelector(c + ' [data-r="canvas"]'); return cv._face !== 'back' && !cv._turning; }, card);
    assert.equal((await inkAt()).a, 0, 'back to the front: the words are gone again');
    assert.deepEqual(errors, [], 'no page errors');
    console.log('  ✓ a Nest card turned over shows the engraved charm\'s words in ink, the plain charm plain, no added height');
  } finally { await browser.close(); srv.close(); }
})().then(() => console.log('Nest card back OK')).catch(e => { console.error(e); process.exit(1); });
