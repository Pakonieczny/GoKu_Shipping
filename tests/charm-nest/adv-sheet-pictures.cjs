// Adversarial check (wave 3, area 15): sheet pictures at true scale, front and back.
// A 100 x 50 mm sheet and a 40 x 25 mm sheet side by side must show millimetres alike: the Sets window's and the Sets
// menu's pictures, and each one turned to its Back.
//   node tests/charm-nest/adv-sheet-pictures.cjs   (PW_DIR=<playwright node_modules>, CHROMIUM=<chrome>)
const path = require('path'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');
const { start } = require('./bridge-server.cjs');

const MM = 72 / 25.4;
const BIG = 'sheet-adv15-big', SMALL = 'sheet-adv15-small';
const st = (wMm, hMm) => ({ wPt: wMm * MM, hPt: hMm * MM, wIn: wMm / 25.4, hIn: hMm / 25.4 });
// two pieces per sheet 20 mm apart, so a drawing's scale is read from where it puts them
const SPOTS = { [BIG]: [['b1', 20, 12], ['b2', 40, 12]], [SMALL]: [['s1', 10, 12], ['s2', 30, 12]] };
const SIZE = { [BIG]: [100, 50], [SMALL]: [40, 25] };

(async () => {
  const pwDir = process.env.PW_DIR || path.join(root, 'node_modules');
  let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { console.log('  – no playwright-core: the browser check was not run'); return; }
  const srv = await start({ receipts: [] });
  const day = new Date().toISOString().slice(0, 10);
  const recs = {};
  for (const id of [BIG, SMALL]) {
    const [w, h] = SIZE[id], charms = SPOTS[id].map(([cid], i) => ({ id: cid, name: `41737${i}${id.length} · TAG`, poolId: `41737${i}${id.length}_1_1`, order: `41737${i}${id.length}`, sku: 'TAG' }));
    recs[id] = { id, metal: 'gold14k', sheetIndex: id === BIG ? 1 : 2, day, status: 'written', stock: st(w, h), updatedAt: Date.now(), preview: `/adv15-${id}.png`, outputs: { preview: { url: `/adv15-${id}.png` } },
      orders: charms.map(c => c.order), placements: SPOTS[id].map(([cid, x, y]) => ({ id: cid, cxPt: x * MM, cyPt: y * MM, angle: 0, wPt: 6 * MM, hPt: 6 * MM })), charms };
    srv.st.put('Charm_Nest_Sheets', id, recs[id]);
  }
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  try {
    const context = await browser.newContext({ viewport: { width: 1440, height: 900 } });
    let png = null;
    await context.route(u => /adv15-.*\.png/.test(u.href), async r => { r.fulfill({ status: 200, contentType: 'image/png', headers: { 'Access-Control-Allow-Origin': '*' }, body: png[/small/.test(r.request().url()) ? 'small' : 'big'] }); });
    await context.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => /fonts\.googleapis|fonts\.gstatic/.test(r.request().url()) ? r.fulfill({ status: 200, contentType: 'text/css', body: '' }) : r.abort());
    await context.addInitScript(() => { try { if (!sessionStorage.getItem('__seeded')) { localStorage.setItem('cn.settings', JSON.stringify({ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on' })); localStorage.setItem('cn.employee', 'Test Operator'); sessionStorage.setItem('__seeded', '1'); } } catch (_) {} window.prompt = () => 'Test Operator'; });
    const page = await context.newPage(), errors = [];
    page.setDefaultTimeout(20000);
    page.on('pageerror', e => { errors.push(e.message); console.error('page error:', String(e.stack || e.message).split('\n').slice(0, 4).join(' | ')); });
    await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
    await page.waitForFunction(() => window.CN && window.RunHistory && window.SetPicker && window.SheetWin && SheetWin.drawOrder && CN.S.cloud.ok === true, null, { timeout: 60000 });
    // each sheet's saved picture at its own shape (10 px a millimetre)
    png = await page.evaluate(() => { const one = (w, h) => { const c = document.createElement('canvas'); c.width = w * 10; c.height = h * 10; const x = c.getContext('2d'); x.fillStyle = '#fff'; x.fillRect(0, 0, c.width, c.height); return c.toDataURL('image/png').split(',')[1]; }; return { big: one(100, 50), small: one(40, 25) }; });
    png = { big: Buffer.from(png.big, 'base64'), small: Buffer.from(png.small, 'base64') };
    // the pages hold both sheets, every charm with its drawing
    await page.evaluate(({ recs, MM }) => {
      const sq = { kind: 'path', subpaths: [[['m', [0, 0]], ['l', [6 * MM, 0]], ['l', [6 * MM, 6 * MM]], ['l', [0, 6 * MM]], ['h']]], stroke: true, strokeRGB: [0, 0, 0], lwPt: .25, bbox: [0, 0, 6 * MM, 6 * MM] };
      for (const r of Object.values(recs)) S.sheets.gold14k.pages.push({ sheetId: r.id, metal: 'gold14k', sheetIndex: r.sheetIndex, page: r.sheetIndex, status: 'complete', rejects: [], roseStock: r.stock,
        placements: r.placements, charms: r.charms.map(c => Object.assign({}, c, { centerPt: [3 * MM, 3 * MM], widthPt: 6 * MM, heightPt: 6 * MM, outline: sq, members: [sq] })) });
    }, { recs, MM });

    // ── the Sets menu's preview: front picture and back, px per mm
    await page.evaluate(({ recs }) => SetPicker.preview({ name: 'Set 1', day: '2026-09-28', status: 'written', sheets: Object.values(recs).map(r => ({ id: r.id, stock: r.stock, metal: r.metal, preview: r.preview, placedCount: 2, fileBase: r.id, page: S.sheets.gold14k.pages.find(p => p.sheetId === r.id) })) }), { recs });
    await page.waitForFunction(() => [...document.querySelectorAll('#setPreview .spPlate img')].every(i => i.complete && i.naturalWidth));
    const spFront = await page.evaluate(({ SIZE, ids }) => [...document.querySelectorAll('#setPreview .spPlate')].map((p, i) => { const r = p.querySelector('img').getBoundingClientRect(); return { id: ids[i], pxmm: r.width / SIZE[ids[i]][0], left: r.left, centre: r.left + r.width / 2, plate: p.getBoundingClientRect() }; }), { SIZE, ids: [BIG, SMALL] });
    await page.click('#setPreview [data-sp-face="back"]');
    await page.waitForFunction(() => [...document.querySelectorAll('#setPreview .spBack')].length === 2 && [...document.querySelectorAll('#setPreview .spBack')].every(h => h._info && h._info.pointOf) && ![...document.querySelectorAll('#setPreview .spPlate')].some(p => p.getAnimations().some(a => a.playState === 'running')));
    const spBack = await page.evaluate(({ SPOTS, ids }) => [...document.querySelectorAll('#setPreview .spBack')].map((h, i) => { const s = SPOTS[ids[i]], a = h._info.pointOf(s[0][0]), b = h._info.pointOf(s[1][0]), cv = h.querySelector('canvas').getBoundingClientRect(); return { id: ids[i], pxmm: Math.abs(b.x - a.x) / 20, centre: cv.left + cv.width / 2 }; }), { SPOTS, ids: [BIG, SMALL] });
    console.log('  Sets menu front', spFront.map(x => `${x.id} ${x.pxmm.toFixed(2)} px/mm`).join(', '), '· back', spBack.map(x => `${x.id} ${x.pxmm.toFixed(2)} px/mm`).join(', '));
    await page.keyboard.press('Escape');
    await page.waitForFunction(() => !document.querySelector('#setPreview[open]'));

    // ── the Sets window: front picture and back, px per mm
    await page.evaluate(() => RunHistory.show(''));
    const tile = id => `#histDlg .hTile[data-sheet="${id}"]`;
    await page.waitForSelector(tile(SMALL)); await page.waitForSelector(tile(BIG));
    await page.mouse.move(1430, 890);   // (no tile hovered: a hovered picture grows a little)
    await page.waitForTimeout(300);
        await page.waitForFunction(ids => ids.every(id => { const i = document.querySelector(`#histDlg .hTile[data-sheet="${id}"] img.hThumb`); return i && i.complete && i.naturalWidth; }), [BIG, SMALL]);
    const swFront = await page.evaluate(({ SIZE, ids }) => ids.map(id => { const r = document.querySelector(`#histDlg .hTile[data-sheet="${id}"] img.hThumb`).getBoundingClientRect(); return { id, pxmm: r.width / SIZE[id][0], pxmmH: r.height / SIZE[id][1] }; }), { SIZE, ids: [BIG, SMALL] });
    await page.click('#hFace [data-face="back"]');
    await page.waitForFunction(ids => ids.every(id => { const c = document.querySelector(`#histDlg .hTile[data-sheet="${id}"] .hBack canvas`); return c && c.width > 20; }), [BIG, SMALL]);
    await page.waitForTimeout(900);
    const swBack = await page.evaluate(({ SIZE, ids }) => ids.map(id => { const r = document.querySelector(`#histDlg .hTile[data-sheet="${id}"] .hBack canvas`).getBoundingClientRect(); return { id, pxmm: r.width / SIZE[id][0], pxmmH: r.height / SIZE[id][1] }; }), { SIZE, ids: [BIG, SMALL] });
    console.log('  Sets window front', swFront.map(x => `${x.id} ${x.pxmm.toFixed(2)}/${x.pxmmH.toFixed(2)} px/mm`).join(', '), '· back', swBack.map(x => `${x.id} ${x.pxmm.toFixed(2)}/${x.pxmmH.toFixed(2)} px/mm`).join(', '));
    if (process.env.SHOT) await page.screenshot({ path: process.env.SHOT });

    const near = (a, b, tol, what) => assert(Math.abs(a - b) <= tol * Math.max(a, b), `${what}: ${a.toFixed(2)} vs ${b.toFixed(2)} px/mm`);
    near(spFront[0].pxmm, spFront[1].pxmm, .03, 'Sets menu: both pictures at one scale');
    near(spBack[0].pxmm, spBack[1].pxmm, .06, 'Sets menu turned to its Back: both sheets at one scale');
    for (let i = 0; i < 2; i++) {
      near(spBack[i].pxmm, spFront[i].pxmm, .03, `Sets menu: ${spFront[i].id}'s back at its front's scale`);
      assert(Math.abs(spBack[i].centre - spFront[i].centre) <= 4, `Sets menu: ${spFront[i].id} turns over in its own place (${spFront[i].centre.toFixed(0)} vs ${spBack[i].centre.toFixed(0)})`);
    }
    near(swFront[0].pxmm, swFront[1].pxmm, .03, 'Sets window: both pictures at one scale');
    near(swBack[0].pxmm, swBack[1].pxmm, .03, 'Sets window turned to its Back: both sheets at one scale');
    for (let i = 0; i < 2; i++) near(swBack[i].pxmm, swFront[i].pxmm, .05, `Sets window: ${swFront[i].id}'s back at its front's scale`);
    console.log('  ✓ true scale front and back in the Sets menu preview and the Sets window');

    // ── the sheet window's Front | Back pressed twice quickly (Back, then Front before it is round): one turn that never
    // snaps — the plate's angle, read every frame, moves by no more than a smooth turn's step
    await page.keyboard.press('Escape');
    await page.waitForFunction(() => !document.querySelector('#histDlg[open]'));
    await page.evaluate(id => SheetWin.open(id), BIG);
    await page.waitForFunction(id => SheetWin.isOpen() && SheetWin.current() === id && SheetWin._W.geom && !SheetWin._W.flying && !SheetWin._W.el.plate.getAnimations().some(a => a.playState === 'running'), BIG, { timeout: 30000 });
    const turn = await page.evaluate(async () => {
      const W = SheetWin._W, pl = W.el.plate, angles = [], deltas = [];
      const ang = () => { const t = getComputedStyle(pl).transform; if (!t || t === 'none') return 0; const m = new DOMMatrix(t); return Math.atan2(-m.m13, m.m11) * 180 / Math.PI; };
      let last = performance.now(), on = true, turns = 0;
      const tick = now => { deltas.push(now - last); last = now; angles.push(ang()); turns = Math.max(turns, pl.getAnimations().filter(a => a.playState === "running").length); if (on) requestAnimationFrame(tick); };
      requestAnimationFrame(tick);
      const btn = () => document.querySelector('.swStrip [data-r2=face]');
      btn().click();
      await new Promise(r => setTimeout(r, 120));
      btn().click();
      await new Promise(r => setTimeout(r, 1500));
      on = false;
      return { turns, series: angles.map((a, i) => Math.round(a) + "@" + Math.round(deltas[i])).join(" "), face: W.face, end: angles[angles.length - 1], running: pl.getAnimations().filter(a => a.playState === 'running').length, maxDelta: Math.max(...deltas.slice(2)), text: document.querySelector('.swStrip').textContent };
    });
    if (process.env.SERIES) console.log(turn.series);
    console.log(`  sheet window double turn: ${turn.turns} turn(s) at once, ends at ${turn.end.toFixed(1)}°, slowest frame ${turn.maxDelta.toFixed(0)} ms`);
    assert.equal(turn.face, 'front', 'the second press wins: the plate is on its front');
    assert.match(turn.text, /Hover a charm/, 'and says so');
    assert(Math.abs(turn.end) < .5 && !turn.running, 'the plate comes to rest square');
    assert(turn.turns <= 1, `one turn at a time: a second press never starts a turn over the one running (${turn.turns} at once)`);
    assert.deepEqual(errors, [], 'no page errors');
  } finally { await browser.close(); srv.close(); }
})().then(() => console.log('adv sheet pictures OK')).catch(e => { console.error(e); process.exit(1); });
