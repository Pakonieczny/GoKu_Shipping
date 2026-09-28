// The Library turned over (Paul, 28 Sep 21:22: "add the back engraving view … to all places where a sheet is visible"):
// the Library bar's Front | Back · engraving switch turns the sheet cards, and a card whose charm has a back text shows
// that charm's words in ink on its picture (pixels read off the card), the plain charm left plain; Front brings the saved
// picture back, and each card has its own small switch.
//   node tests/charm-nest/library-back-flip.cjs   (PW_DIR=<playwright node_modules>, CHROMIUM=<chrome>)
const path = require('path'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');
const { start } = require('./bridge-server.cjs');

const SH = 'sheet-libback-gf', RID = '4173711092', TID = '41737110921', PA = `${RID}_${TID}_1`, PB = `${RID}_${TID}_2`;
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
    const context = await browser.newContext({ viewport: { width: 1440, height: 900 }, deviceScaleFactor: 2 });
    await context.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => /fonts\.googleapis|fonts\.gstatic/.test(r.request().url()) ? r.fulfill({ status: 200, contentType: 'text/css', body: '' }) : r.abort());
    await context.addInitScript(() => { try { if (!sessionStorage.getItem('__seeded')) { localStorage.setItem('cn.settings', JSON.stringify({ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on' })); localStorage.setItem('cn.employee', 'Test Operator'); sessionStorage.setItem('__seeded', '1'); } } catch (_) {} window.prompt = () => 'Test Operator'; });
    const page = await context.newPage(), errors = [];
    page.setDefaultTimeout(20000);
    page.on('pageerror', e => { errors.push(e.message); console.error('page error:', String(e.stack || e.message).split('\n').slice(0, 4).join(' | ')); });
    await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
    await page.waitForFunction(() => window.CN && window.SheetWin && window.LibraryBacks && CN.S.cloud.ok === true, null, { timeout: 60000 });
    // the sheet as this sorter holds it on its page, so each charm has its drawing (a saved .ai is not read here)
    await page.evaluate(({ SH, charms, placements }) => {
      const sq = { kind: 'path', subpaths: [[['m', [0, 0]], ['l', [34, 0]], ['l', [34, 34]], ['l', [0, 34]], ['h']]], stroke: true, strokeRGB: [0, 0, 0], lwPt: .25, bbox: [0, 0, 34, 34] };
      S.sheets.gold.pages.push({ sheetId: SH, metal: 'gold', sheetIndex: 1, page: 1, status: 'complete', placements, rejects: [],
        charms: charms.map(c => Object.assign({}, c, { centerPt: [17, 17], widthPt: 34, heightPt: 34, outline: sq, members: [sq] })) });
      CN.setMode('library');
    }, { SH, charms, placements });
    await page.waitForSelector(`#libBody .libCard[data-id="${SH}"] .pvTurn`);
    assert(await page.$('#libView > .libBar #libFace'), 'the switch sits in the Library bar');
    // (the page reads what it reads on its own: counted from here, the turned Library's reads alone)
    await page.waitForTimeout(800);
    const n0 = srv.st.calls.filter(c => c.name === 'charmNestLibrary' && ['getSheet', 'backList'].includes(c.op)).length;
    const reads = () => srv.st.calls.filter(c => c.name === 'charmNestLibrary' && ['getSheet', 'backList'].includes(c.op)).length - n0;
    await page.click('#libFace [data-face="back"]');
    await page.waitForFunction(id => { const k = LibraryBacks._L.drawn.get(id), card = document.querySelector(`#libBody .libCard[data-id="${id}"]`);
      return k && !k.pending && !k.waiting && k.info && k.info.words().every(w => w.drawn) && card.querySelector('.pvTurn.back') && !card.querySelector('.pvSpin')
        && !card.querySelector('.pvTurn').getAnimations().some(a => a.playState === 'running'); }, SH, { timeout: 30000 });
    const got = await page.evaluate(id => {
      const k = LibraryBacks._L.drawn.get(id), cv = k.cv, ctx = cv.getContext('2d'), inf = k.info;
      const ink = (x, y) => { const d = ctx.getImageData(Math.round(x) - 9, Math.round(y) - 9, 19, 19).data; let n = 0; for (let j = 0; j < d.length; j += 4) if (d[j] + d[j + 1] + d[j + 2] < 600) n++; return n; };
      const card = document.querySelector(`#libBody .libCard[data-id="${id}"]`), img = card.querySelector('img.pv'), r = cv.getBoundingClientRect(), ri = img.getBoundingClientRect();
      const words = inf.words(), plain = inf.pieces.find(x => x.poolId !== words[0].poolId);
      // the plain charm's centre on the canvas, mirrored as the laser sees it
      const q = inf.pointOf(plain.poolId), px = (q.x - r.left) * cv.width / r.width, py = (q.y - r.top) * cv.height / r.height;
      return { words: words.map(w => ({ poolId: w.poolId, text: w.text, ink: ink(w.x, w.y), x: w.x })), plain: ink(px, py),
        onCard: card.contains(cv), shown: getComputedStyle(cv.parentElement).visibility, box: [Math.round(r.width), Math.round(ri.width)], mirrored: words[0].x > cv.width / 2 };
    }, SH);
        assert.equal(got.words.length, 1, 'the one charm with a back text');
    assert.equal(got.words[0].text, 'For Mia');
    assert(got.words[0].ink >= 4, `ink of the words on the card (${got.words[0].ink} dark pixels)`);
    assert(got.mirrored, 'a charm on the left of the front is on the right from behind');
    assert.equal(got.plain, 0, 'the charm with no engraving stays plain');
    assert(got.onCard && got.shown === 'visible', 'the back is on the card, shown');
    assert(Math.abs(got.box[0] - got.box[1]) <= 3, 'the back fills the same true-scale frame as the picture: ' + got.box);
    assert.equal(reads(), 1, 'one read for the card (its record carries its backs)');
    // each card's own switch, and the Library's Front: the saved picture comes back, nothing read again
    await page.hover(`#libBody .libCard[data-id="${SH}"] .pvTurn`);
    assert(await page.$(`#libBody .libCard[data-id="${SH}"] .pvFace [data-face="front"]`), 'each card has its own switch');
    await page.click('#libFace [data-face="front"]');
    await page.waitForFunction(id => !document.querySelector(`#libBody .libCard[data-id="${id}"] .pvTurn.back`), SH);
    await page.click('#libFace [data-face="back"]');
    await page.waitForFunction(id => document.querySelector(`#libBody .libCard[data-id="${id}"] .pvTurn.back`), SH);
    assert.equal(reads(), 1, 'a card turned again is drawn from what was kept');
    assert.deepEqual(errors, [], 'no page errors');
    console.log('  ✓ the Library turned over: the card\'s charm shows its own words in ink, mirrored; the plain one stays plain');
  } finally { await browser.close(); srv.close(); }
})().then(() => console.log('Library back side OK')).catch(e => { console.error(e); process.exit(1); });
