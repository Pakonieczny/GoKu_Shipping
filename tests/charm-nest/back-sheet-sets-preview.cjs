// The Sets menu's preview (SetPicker.preview) turns to its Back like the order view's Sheet tab (Paul, 28 Sep 21:22:
// "add the back engraving view ... to all places where a sheet is visible ... I should be able to see the backings
// everywhere"): the Front | Back · engraving switch sits on the title line (no added height), every sheet turns, and
// each charm's own words are drawn in ink on its own charm, the plain charm left plain; Front brings the picture back.
//   node tests/charm-nest/back-sheet-sets-preview.cjs   (PW_DIR=<playwright node_modules>, CHROMIUM=<chrome>)
const path = require('path'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');
const { start } = require('./bridge-server.cjs');

const SH = 'sheet-backface-sp';
const A = '4173711192', B2 = '4173711193';
const PA = `${A}_${A}1_1`, PB = `${B2}_${B2}1_1`, PC = `${B2}_${B2}1_2`;
const BACKS = [
  { poolId: PA, order: A, sku: 'TINY_TAG', copy: 1, text: 'For Mia', lines: ['For Mia'], sizePt: 7, capMm: 1.7, weight: 'Regular', lineGap: .216, centre: [20, 17], angle: 0, upAngle: 90, approvedAt: Date.now() - 4000, approvedBy: 'Test Operator' },
  { poolId: PB, order: B2, sku: 'LEAF', copy: 1, text: 'Love, Dad', lines: ['Love, Dad'], sizePt: 6.5, capMm: 1.6, weight: 'Regular', lineGap: .216, centre: [17, 21], angle: 0, upAngle: 0, approvedAt: Date.now() - 3000, approvedBy: 'Test Operator' }
];

(async () => {
  const pwDir = process.env.PW_DIR || path.join(root, 'node_modules');
  let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { console.log('  – no playwright-core: the browser check was not run'); return; }
  const srv = await start({ receipts: [] });
  const charms = [{ id: 'g1', name: `${A} · TINY_TAG`, poolId: PA, order: A, sku: 'TINY_TAG' }, { id: 'g2', name: `${B2} · LEAF`, poolId: PB, order: B2, sku: 'LEAF' }, { id: 'g3', name: `${B2} · LEAF`, poolId: PC, order: B2, sku: 'LEAF' }];
  const at = (id, cx, cy) => ({ id, cxPt: cx, cyPt: cy, angle: 0, wPt: 34, hPt: 34 });
  srv.st.put('Charm_Nest_Sheets', SH, { id: SH, metal: 'gold', sheetIndex: 1, day: '2026-09-28', status: 'written', stock: { wPt: 300, hPt: 150 },
    orders: [A, B2], placements: [at('g1', 50, 50), at('g2', 150, 50), at('g3', 250, 100)], charms, backPool: BACKS });
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  try {
    const context = await browser.newContext({ viewport: { width: 1440, height: 900 } });
    await context.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => /fonts\.googleapis|fonts\.gstatic/.test(r.request().url()) ? r.fulfill({ status: 200, contentType: 'text/css', body: '' }) : r.abort());
    await context.addInitScript(() => { try { if (!sessionStorage.getItem('__seeded')) { localStorage.setItem('cn.settings', JSON.stringify({ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on' })); localStorage.setItem('cn.employee', 'Test Operator'); sessionStorage.setItem('__seeded', '1'); } } catch (_) {} window.prompt = () => 'Test Operator'; });
    const page = await context.newPage(), errors = [];
    page.setDefaultTimeout(20000);
    page.on('pageerror', e => { errors.push(e.message); console.error('page error:', String(e.stack || e.message).split('\n').slice(0, 4).join(' | ')); });
    await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
    await page.waitForFunction(() => window.CN && window.SetPicker && window.SheetWin && SheetWin.drawOrder && CN.S.cloud.ok === true, null, { timeout: 60000 });
    // the sheet as this sorter holds it on its page, every charm with its drawing
    await page.evaluate(({ SH, charms }) => {
      const sq = { kind: 'path', subpaths: [[['m', [0, 0]], ['l', [34, 0]], ['l', [34, 34]], ['l', [0, 34]], ['h']]], stroke: true, strokeRGB: [0, 0, 0], lwPt: .25, bbox: [0, 0, 34, 34] };
      S.sheets.gold.pages.push({ sheetId: SH, metal: 'gold', sheetIndex: 1, page: 1, status: 'complete',
        placements: [['g1', 50, 50], ['g2', 150, 50], ['g3', 250, 100]].map(([id, cx, cy]) => ({ id, cxPt: cx, cyPt: cy, angle: 0, wPt: 34, hPt: 34 })),
        charms: charms.map(c => Object.assign({}, c, { centerPt: [17, 17], widthPt: 34, heightPt: 34, outline: sq, members: [sq] })), rejects: [] });
      SetPicker.previewCurrent();
    }, { SH, charms });
    await page.waitForSelector('#setPreview[open] [data-sp-face="back"]');
    // the switch sits on the title line: the title is no taller than one line of it
    const hh = await page.evaluate(() => { const h = document.querySelector('#setPreview h2'), s = h.querySelector('.spFace'), a = h.getBoundingClientRect().height; s.style.display = 'none'; const b = h.getBoundingClientRect().height; s.style.display = ''; return { a, b }; });
    assert(hh.a <= hh.b + 1, `the switch adds no height to the title line (${hh.a} vs ${hh.b})`);
    await page.click('#setPreview [data-sp-face="back"]');
    // the plate turns (a flip on the sheet's box), and its back is drawn with every charm's words
    const turned = await page.evaluate(() => document.querySelector('#setPreview .spPlate').getAnimations().length > 0 || matchMedia('(prefers-reduced-motion: reduce)').matches);
    assert(turned, 'the sheet turns over');
    await page.waitForFunction(() => { const h = document.querySelector('#setPreview .spBack'); if (!h || !h._info || h.dataset.engraved == null) return false;
      const w = h._info.words(); return w.length === 2 && w.every(x => x.drawn) && h.querySelector('.spWait').hidden && !document.querySelector('#setPreview .spPlate').getAnimations().some(a => a.playState === 'running'); }, null, { timeout: 30000 });
    if (process.env.SHOT) await page.screenshot({ path: process.env.SHOT });
    const got = await page.evaluate(() => {
      const h = document.querySelector('#setPreview .spBack'), i = h._info, cv = h.querySelector('canvas'), ctx = cv.getContext('2d');
      const ink = p => { const d = ctx.getImageData(Math.round(p.x) - 9, Math.round(p.y) - 9, 19, 19).data; let n = 0; for (let j = 0; j < d.length; j += 4) if (d[j] + d[j + 1] + d[j + 2] < 330) n++; return n; };
      const r = cv.getBoundingClientRect(), onCv = p => ({ x: (p.x - r.left) * cv.width / r.width, y: (p.y - r.top) * cv.height / r.height });
      const words = i.words(), plain = i.pieces.find(x => !words.some(w => w.poolId === x.poolId));
      const pressed = [...document.querySelectorAll('#setPreview [data-sp-face]')].map(b => b.dataset.spFace + ':' + b.getAttribute('aria-pressed'));
      return { words: words.map(w => ({ poolId: w.poolId, text: w.text, ink: ink(w) })), plain: ink(onCv(i.pointOf(plain.poolId))), pressed, back: document.querySelector('#setPreview .spPlate').classList.contains('back') };
    });
    assert.deepEqual(got.pressed, ['front:false', 'back:true']);
    assert(got.back, 'the plate is on its back');
    assert.deepEqual(got.words.map(w => w.text).sort(), ['For Mia', 'Love, Dad']);
    for (const w of got.words) assert(w.ink >= 4, `${w.poolId}: its words in ink on its charm (${w.ink} dark pixels)`);
    assert.equal(got.plain, 0, 'the charm with no engraving stays plain');
    // and back to the Front: the back layer goes, the sheet's own picture shows again
    await page.click('#setPreview [data-sp-face="front"]');
    await page.waitForFunction(() => { const p = document.querySelector('#setPreview .spPlate'); return !p.classList.contains('back') && p.querySelector('.spBack').hidden && !p.getAnimations().some(a => a.playState === 'running'); }, null, { timeout: 15000 });
    assert.deepEqual(errors, [], 'no page errors');
    console.log('  ✓ the Sets preview turns to its Back: each charm\'s own words in ink, the plain charm plain, and back to the Front');
  } finally { await browser.close(); srv.close(); }
})().then(() => console.log('Sets preview back side OK')).catch(e => { console.error(e); process.exit(1); });
