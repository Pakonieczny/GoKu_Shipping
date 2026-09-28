// The Sets window turned to its Back shows every charm's own engraving (Paul, 28 Sep 21:22: "add the back engraving view …
// to all places where a sheet is visible not only the pop-ups"): one Front | Back · engraving switch in the window's bar
// turns every sheet over with the flip, and each sheet's picture gives way to its back, drawn by the order view's own
// plate — each charm's words in ink on that charm, mirrored as the laser sees them, the plain charm plain.
//   node tests/charm-nest/sets-back-face.cjs   (PW_DIR=<playwright node_modules>, CHROMIUM=<chrome>)
const path = require('path'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');
const { start } = require('./bridge-server.cjs');

const SH = 'sheet-sets-backface';
const PA = '4173711092_41737110921_1', PB = '4173711093_41737110931_1', PC = '4173711094_41737110941_1', PD = '4173711094_41737110941_2';
const BACKS = [
  { poolId: PA, order: '4173711092', sku: 'TINY_TAG', copy: 1, text: 'For Mia', lines: ['For Mia'], sizePt: 9, capMm: 2.2, weight: 'Regular', lineGap: .216, centre: [17, 17], angle: 0, upAngle: 90, approvedAt: Date.now() - 4000, approvedBy: 'Test Operator' },
  { poolId: PB, order: '4173711093', sku: 'LEAF', copy: 1, text: 'Love, Dad', lines: ['Love, Dad'], sizePt: 8, capMm: 2, weight: 'Regular', lineGap: .216, centre: [17, 17], angle: 0, upAngle: 0, approvedAt: Date.now() - 3000, approvedBy: 'Test Operator' },
  { poolId: PC, order: '4173711094', sku: 'DISC', copy: 1, text: 'Ada 2026', lines: ['Ada', '2026'], sizePt: 8, capMm: 2, weight: 'Regular', lineGap: .3, centre: [17, 17], angle: 0, upAngle: 90, approvedAt: Date.now() - 2000, approvedBy: 'Test Operator' }
];
const SPOTS = [['g1', 50, 50, PA], ['g2', 130, 50, PB], ['g3', 210, 50, PC], ['g4', 210, 110, PD]];

(async () => {
  const pwDir = process.env.PW_DIR || path.join(root, 'node_modules');
  let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { console.log('  – no playwright-core: the browser check was not run'); return; }
  const srv = await start({ receipts: [] });
  const day = new Date().toISOString().slice(0, 10);
  const charms = SPOTS.map(([id, , , poolId]) => ({ id, name: `${poolId.split('_')[0]} · TAG`, poolId, order: poolId.split('_')[0], sku: 'TAG' }));
  srv.st.put('Charm_Nest_Sheets', SH, { id: SH, metal: 'gold', sheetIndex: 1, day, status: 'written', stock: { wPt: 300, hPt: 150 }, updatedAt: Date.now(),
    orders: ['4173711092', '4173711093', '4173711094'], placements: SPOTS.map(([id, cx, cy]) => ({ id, cxPt: cx, cyPt: cy, angle: 0, wPt: 34, hPt: 34 })), charms, backPool: BACKS });
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  try {
    const context = await browser.newContext({ viewport: { width: 1440, height: 900 }, deviceScaleFactor: 2 });
    await context.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => /fonts\.googleapis|fonts\.gstatic/.test(r.request().url()) ? r.fulfill({ status: 200, contentType: 'text/css', body: '' }) : r.abort());
    await context.addInitScript(() => { try { if (!sessionStorage.getItem('__seeded')) { localStorage.setItem('cn.settings', JSON.stringify({ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on' })); localStorage.setItem('cn.employee', 'Test Operator'); sessionStorage.setItem('__seeded', '1'); } } catch (_) {} window.prompt = () => 'Test Operator'; });
    const page = await context.newPage(), errors = [];
    page.setDefaultTimeout(20000);
    page.on('pageerror', e => { errors.push(e.message); console.error('page error:', String(e.stack || e.message).split('\n').slice(0, 4).join(' | ')); });
    await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
    await page.waitForFunction(() => window.CN && window.RunHistory && window.SheetWin && SheetWin.drawOrder && CN.S.cloud.ok === true, null, { timeout: 60000 });
    // the sheet as this sorter holds it on its page, so every charm has its drawing (a saved .ai is not read here)
    await page.evaluate(({ SH, charms, SPOTS }) => {
      const sq = { kind: 'path', subpaths: [[['m', [0, 0]], ['l', [34, 0]], ['l', [34, 34]], ['l', [0, 34]], ['h']]], stroke: true, strokeRGB: [0, 0, 0], lwPt: .25, bbox: [0, 0, 34, 34] };
      S.sheets.gold.pages.push({ sheetId: SH, metal: 'gold', sheetIndex: 1, page: 1, status: 'complete', rejects: [],
        placements: SPOTS.map(([id, cx, cy]) => ({ id, cxPt: cx, cyPt: cy, angle: 0, wPt: 34, hPt: 34 })),
        charms: charms.map(c => Object.assign({}, c, { centerPt: [17, 17], widthPt: 34, heightPt: 34, outline: sq, members: [sq] })) });
      RunHistory.show('');
    }, { SH, charms, SPOTS });
    const tile = `#histDlg .hTile[data-sheet="${SH}"]`;
    await page.waitForSelector(tile, { timeout: 20000 });
    const bar0 = await page.evaluate(() => ({ bar: document.querySelector('#histDlg .hBar').offsetHeight, face: document.getElementById('hFace').offsetHeight, when: document.getElementById('hWhen').offsetHeight }));
    assert.equal(bar0.face, bar0.when, 'the switch is the bar\'s own segmented control, no taller');
    // Back · engraving: the sheets turn
    await page.click('#hFace [data-face="back"]');
    const turning = await page.evaluate(t => document.querySelector(t + ' .hPlate').getAnimations().length, tile);
    assert(turning > 0, 'the sheet turns over');
    await page.waitForFunction(t => { const d = document.getElementById('histDlg'), cv = document.querySelector(t + ' .hBack canvas');
      return d.dataset.face === 'back' && cv && cv.width > 40 && !document.querySelector(t + ' .hPlate').getAnimations().some(a => a.playState === 'running'); }, tile, { timeout: 20000 });
    // every charm's words drawn in ink on its own charm (the back is mirrored: a charm at x lies at 300 − x)
    await page.waitForFunction(({ t, SPOTS }) => { const cv = document.querySelector(t + ' .hBack canvas'), ctx = cv.getContext('2d'), k = cv.width / 300;
      return SPOTS.slice(0, 3).every(([, cx, cy]) => { const d = ctx.getImageData(Math.round((300 - cx - 15) * k), Math.round((cy - 15) * k), Math.round(30 * k), Math.round(30 * k)).data;
        let n = 0; for (let j = 0; j < d.length; j += 4) if (d[j] + d[j + 1] + d[j + 2] < 330) n++; return n >= 4; }); }, { t: tile, SPOTS }, { timeout: 20000 });
    const got = await page.evaluate(({ t, SPOTS }) => {
      const cv = document.querySelector(t + ' .hBack canvas'), ctx = cv.getContext('2d'), k = cv.width / 300;
      const ink = (cx, cy) => { const d = ctx.getImageData(Math.round((cx - 15) * k), Math.round((cy - 15) * k), Math.round(30 * k), Math.round(30 * k)).data; let n = 0; for (let j = 0; j < d.length; j += 4) if (d[j] + d[j + 1] + d[j + 2] < 330) n++; return n; };
      const img = document.querySelector(t + ' .hPlate > img, ' + t + ' .hPlate > .hNoPv');
      return { back: SPOTS.map(([, cx, cy]) => ink(300 - cx, cy)), front: SPOTS.map(([, cx, cy]) => ink(cx, cy)),
        picture: img ? getComputedStyle(img).visibility : 'none', pressed: document.querySelector('#hFace [data-face="back"]').getAttribute('aria-pressed'),
        bar: document.querySelector('#histDlg .hBar').offsetHeight };
    }, { t: tile, SPOTS });
    for (let i = 0; i < 3; i++) assert(got.back[i] >= 4, `${SPOTS[i][3]}: its words in ink on its charm from behind (${got.back[i]} dark pixels)`);
    assert.equal(got.back[3], 0, 'the charm with no engraving stays plain');
    assert.equal(got.front[0] + got.front[1], 0, 'mirrored: the words lie where the laser sees them, not where the front has the charm');
    assert.notEqual(got.picture, 'visible', 'the front picture gives way to the back, nothing laid over it');
    assert.equal(got.pressed, 'true');
    assert.equal(got.bar, bar0.bar, 'the bar keeps its height');
    // and back to the Front
    await page.click('#hFace [data-face="front"]');
    await page.waitForFunction(() => document.getElementById('histDlg').dataset.face === 'front', null, { timeout: 5000 });
    assert.deepEqual(errors, [], 'no page errors');
    console.log('  ✓ the Sets window turned over: every charm\'s own words in ink on its charm, the plain charm plain');
  } finally { await browser.close(); srv.close(); }
})().then(() => console.log('Sets window back side OK')).catch(e => { console.error(e); process.exit(1); });
