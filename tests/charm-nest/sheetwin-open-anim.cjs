// The sheet window grows out of the sheet that was clicked and lands on the charm clicked (Paul, 28 Sep): a charm
// clicked on a Nest card, and a charm clicked on a Library card's picture. Opens the page in headless Chromium with a
// sheet of 30 charms on the Gold card (a stand-in painter draws them; the sheet's record is answered in the page), and
// checks, with the window's animations held at their first frame, that the plate's sheet lies over the card's sheet and
// the window's surface over the card's frame, the panel still to come; then, played through, that the window lands on
// the charm clicked ("Charm N of M"), with nothing of the flight left; that closing goes back into the card; that a
// Library picture clicked in a gap opens on the orders list; that Esc in flight closes cleanly and it opens again as
// before; and that with reduced motion it opens without the flight, on the same charm.
//   node tests/charm-nest/sheetwin-open-anim.cjs [playwright-core dir]
const http = require('http'), fs = require('fs'), path = require('path'), assert = require('assert');
const root = path.join(__dirname, '../..');
const pwDir = process.argv[2] || process.env.PW_DIR || path.join(root, 'node_modules');
const { chromium } = require(path.join(pwDir, 'playwright-core'));
const MIME = { '.html': 'text/html', '.js': 'text/javascript', '.css': 'text/css', '.json': 'application/json' };
const server = http.createServer((req, res) => {
  const u = decodeURIComponent(req.url.split('?')[0]);
  if (u.startsWith('/.netlify/functions/')) { res.writeHead(404, { 'Content-Type': 'application/json' }); return res.end('{"error":"no functions in the test server"}'); }
  const f = path.join(root, u === '/' ? 'charm-nest-1.html' : u);
  if (!f.startsWith(root) || !fs.existsSync(f) || fs.statSync(f).isDirectory()) { res.writeHead(404); return res.end(); }
  res.writeHead(200, { 'Content-Type': MIME[path.extname(f)] || 'application/octet-stream' }); fs.createReadStream(f).pipe(res);
}).listen(0);

/** 30 ring charms of 8-11 mm on the Gold card's 100 × 50 mm sheet, two to an order; its record answered in the page. */
async function setup(page) {
  await page.evaluate(async () => {
    const { S } = CN; const MM = 72 / 25.4;
    CharmNestPDF.drawCharm = (ctx, c, tx, k) => { const [x, y] = tx(c.centerPt[0], c.centerPt[1]); ctx.beginPath(); ctx.arc(x, y, c.rMm * MM * k, 0, 7); ctx.lineWidth = Math.max(1, .35 * k); ctx.strokeStyle = '#d0312d'; ctx.stroke(); };
    const realPath = CharmNestPDF.pathToCanvas;
    CharmNestPDF.pathToCanvas = (ctx, p, tx) => { if (p && p.circle) { const [x, y] = tx(p.cx, p.cy), [x1] = tx(p.cx + p.r, p.cy), r = Math.abs(x1 - x); ctx.moveTo(x + r, y); ctx.arc(x, y, r, 0, Math.PI * 2); return; } return realPath(ctx, p, tx); };
    CharmNestPDF.cutLinesOf = () => [];
    const sh = S.sheets.gold.pages[0]; sh.charms = []; sh.placements = [];
    let i = 0; const orders = [];
    for (let row = 0; row < 4; row++) for (let col = 0; col < 8; col++) {
      const rMm = 4.2 + ((row * 3 + col * 5) % 4) * .45, x = 7 + col * 12 + (row % 2) * 5.5, y = 7 + row * 11.8;
      if (x + rMm > 99 || y + rMm > 49) continue;
      const rid = String(3812345600 + Math.floor(i / 2)), id = 'g' + i;
      if (!orders.includes(rid)) orders.push(rid);
      sh.charms.push({ id, name: `${rid} · CHARM-${i}`, poolId: `${rid}_${900 + i}_1`, order: rid, sourceId: 's', centerPt: [0, 0], bbox: [-rMm * MM, -rMm * MM, rMm * MM, rMm * MM], outline: { circle: 1, cx: 0, cy: 0, r: rMm * MM }, members: [], rMm, widthPt: 2 * rMm * MM, heightPt: 2 * rMm * MM, areaPt2: Math.PI * (rMm * MM) ** 2 });
      sh.placements.push({ id, cxPt: x * MM, cyPt: y * MM, angle: 0, wPt: 2 * rMm * MM, hPt: 2 * rMm * MM });
      i++;
    }
    sh.status = 'complete'; sh.sheetId = 'sheet-gold-2'; sh.sheetIndex = 2; sh.seq = 1;
    CN.renderCard(sh);
    const st = stockFor('gold', sh);
    const rec = { id: sh.sheetId, metal: 'gold', metalLabel: 'Gold Filled', folder: 'GF_Sep.28.26_Set-1_Sheet-2', fileBase: 'GF_Sep.28.26_Set-1_Sheet-2', day: '2026-09-28', sheetIndex: 2, setSeq: 1,
      stock: { wPt: st.wPt, hPt: st.hPt, wIn: st.wPt / 72, hIn: st.hPt / 72 }, placements: sh.placements.map(p => ({ ...p })), charms: sh.charms.map(c => ({ id: c.id, name: c.name, poolId: c.poolId, order: c.order, sku: c.name.split(' · ')[1] })),
      density: .62, freePt2: 1400, orders, placedCount: sh.placements.length, charmCount: sh.charms.length, outputs: {}, createdAt: Date.now(), updatedAt: Date.now() };
    window.__rec = rec; window.__reads = 0;
    const realApi = window.api;
    window.api = async (fn, body, opts) => {
      if (fn === 'charmNestLibrary' && body && body.op === 'getSheet') { window.__reads++; await new Promise(r => setTimeout(r, 40)); return { sheet: JSON.parse(JSON.stringify(window.__rec)) }; }
      return realApi(fn, body, opts);
    };
    const cv = document.createElement('canvas'); cv.width = 1026; cv.height = Math.round(1026 * st.hPt / st.wPt); paintPreview(cv, sh, true);
    window.__row = { id: rec.id, metal: 'gold', metalLabel: 'Gold Filled', day: rec.day, folder: rec.folder, fileBase: rec.fileBase, sheetIndex: 2, setSeq: 1, placedCount: rec.placedCount, charmCount: rec.charmCount, density: .62, freePt2: 1400, orders, stock: rec.stock, preview: cv.toDataURL('image/png'), outputs: {}, updatedAt: Date.now(), createdAt: Date.now() };
    window.scrollTo(0, 0); await new Promise(r => setTimeout(r, 300));
  });
}
// the window's animations held at their first frame: where the plate's sheet, the surface and the panel start
const firstFrame = page => page.evaluate(() => {
  const W = SheetWin._W, E = W.el, inDlg = a => a.effect && a.effect.target && W.dlg.contains(a.effect.target);
  const anims = document.getAnimations().filter(inDlg); anims.forEach(a => { a.pause(); a.currentTime = 0; });
  const fx = E.fx.getBoundingClientRect(), s = fx.width / E.fx.width, cur = document.querySelector('.swCurtain');
  const box = r => r && { x: r.left, y: r.top, w: r.width, h: r.height };
  const out = { flying: !!W.flip, n: anims.length, sheet: { x: fx.left + W.R * s, y: fx.top + W.R * s, w: W.st.wPt * W.k * s, h: W.st.hPt * W.k * s }, curtain: box(cur && cur.getBoundingClientRect()), side: +getComputedStyle(E.side).opacity, head: +getComputedStyle(E.headBar).opacity, snap: !!E.plate.querySelector('canvas.swSnap') };
  anims.forEach(a => a.play());
  return out;
});
const settled = page => page.evaluate(() => { const W = SheetWin._W, E = W.el; return { open: SheetWin.isOpen(), flip: !!W.flip, curtain: !!document.querySelector('.swCurtain'), snap: !!document.querySelector('canvas.swSnap'), cls: W.dlg.className, anims: [E.plate, E.headBar, E.side, E.strip].map(n => n.getAnimations().length), sel: W.sel && W.sel.poolId, view: W.view, pos: E.pos.textContent, side: getComputedStyle(E.side).opacity, plate: getComputedStyle(E.plate).transform, land: W.fx.some(f => f.kind === 'land') }; });
const near = (a, b, tol, what) => { for (const k of ['x', 'y', 'w', 'h']) assert(Math.abs(a[k] - b[k]) <= tol, `${what}: ${k} ${a[k].toFixed(1)} vs ${b[k].toFixed(1)} (${JSON.stringify(a)} ${JSON.stringify(b)})`); };

(async () => {
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  try {
    const page = await browser.newPage({ viewport: { width: 1500, height: 900 } });
    const errors = []; page.on('pageerror', e => errors.push(e.message));
    await page.goto(`http://127.0.0.1:${server.address().port}/charm-nest-1.html`);
    await page.waitForFunction(() => window.CN && window.SheetWin && window.Gate);
    await setup(page);

    /* 1 · a charm clicked on the Nest card */
    const nest = i => page.evaluate(i => {
      const sh = CN.S.sheets.gold.pages[0], cv = sh.el.querySelector('[data-r="canvas"]'); cv.scrollIntoView({ block: 'center' });
      const r = cv.getBoundingClientRect(), v = sh._view, s = r.width / cv.width, p = sh.placements[i], st = stockFor('gold', sh), f = cv.closest('.shPreviewWrap').getBoundingClientRect();
      return { x: r.left + (v.R + p.cxPt * v.k) * s, y: r.top + (v.R + p.cyPt * v.k) * s, poolId: sh.charms.find(c => c.id === p.id).poolId,
        sheet: { x: r.left + v.R * s, y: r.top + v.R * s, w: st.wPt * v.k * s, h: st.hPt * v.k * s }, frame: { x: f.left, y: f.top, w: f.width, h: f.height } };
    }, i);
    let t = await nest(13); await page.waitForTimeout(200);
    await page.mouse.click(t.x, t.y);
    let f0 = await firstFrame(page);
    assert(f0.flying && f0.n >= 5, 'a charm clicked on a Nest card opens the window flying out of the card: ' + JSON.stringify(f0));
    near(f0.sheet, t.sheet, 2, 'the plate\'s sheet starts over the card\'s sheet');
    near(f0.curtain, t.frame, 2, 'the window\'s surface starts over the card\'s frame');
    assert(f0.side === 0 && f0.head === 0 && f0.snap, 'the panel and the header are still to come, and the card\'s picture flies with the plate: ' + JSON.stringify(f0));
    await page.waitForTimeout(1700);
    let s = await settled(page);
    assert(s.open && !s.flip && !s.curtain && !s.snap && s.cls === 'sheetWin swGrow', 'it settles with nothing of the flight left: ' + JSON.stringify(s));
    assert.deepStrictEqual(s.anims, [0, 0, 0, 0], 'no animation is left on the plate, header, panel or strip');
    assert.strictEqual(s.sel, t.poolId, 'the window lands on the charm clicked');
    assert(s.view === 'piece' && /^Charm \d+ of 30$/.test(s.pos) && s.side === '1' && s.plate === 'none', 'on its piece view ("Charm N of M"), the panel in, the plate in place: ' + JSON.stringify(s));
    const pos = await page.evaluate(() => { const W = SheetWin._W, list = W.pieces.slice().sort((a, b) => (a.rid || '~').localeCompare(b.rid || '~') || (a.sku || '').localeCompare(b.sku || '') || a.copy - b.copy); return list.indexOf(W.sel) + 1; });
    assert.strictEqual(s.pos, `Charm ${pos} of 30`, 'the same state as stepping to it with ›');
    // closing goes back into the card: the plate's sheet ends over the card's sheet
    await page.evaluate(() => { SheetWin.close(); });
    await page.waitForTimeout(60);
    const back = await page.evaluate(() => { const W = SheetWin._W, a = W.el.plate.getAnimations().find(x => x.effect.getKeyframes().some(k => k.transform && k.transform !== 'none')); if (!a) return null; a.pause(); a.currentTime = a.effect.getComputedTiming().endTime; const fx = W.el.fx.getBoundingClientRect(), s = fx.width / W.el.fx.width, out = { x: fx.left + W.R * s, y: fx.top + W.R * s, w: W.st.wPt * W.k * s, h: W.st.hPt * W.k * s }; a.play(); return out; });
    assert(back, 'closing flies the plate back');
    const t2 = await page.evaluate(() => { const sh = CN.S.sheets.gold.pages[0], cv = sh.el.querySelector('[data-r="canvas"]'), r = cv.getBoundingClientRect(), v = sh._view, s = r.width / cv.width, st = stockFor('gold', sh); return { x: r.left + v.R * s, y: r.top + v.R * s, w: st.wPt * v.k * s, h: st.hPt * v.k * s }; });
    near(back, t2, 2, 'the plate goes back into the card\'s sheet');
    await page.waitForTimeout(900);
    s = await settled(page); assert(!s.open && !/swGrow|swFlying|swBack|closing/.test(s.cls) && !s.curtain, 'closed, clean: ' + JSON.stringify(s));

    /* 2 · Esc in flight: the flight turns round and the window closes; the next opening is as the first */
    t = await nest(7); await page.mouse.click(t.x, t.y); await page.waitForTimeout(200);
    await page.keyboard.press('Escape');
    await page.waitForTimeout(900);
    s = await settled(page); assert(!s.open && !s.curtain, 'Esc in flight closes it: ' + JSON.stringify(s));
    t = await nest(7); await page.mouse.click(t.x, t.y); await page.waitForTimeout(1700);
    s = await settled(page); assert(s.open && !s.flip && !s.curtain && s.sel === t.poolId && s.side === '1', 'and it opens again as before: ' + JSON.stringify(s));
    await page.evaluate(() => SheetWin.close()); await page.waitForTimeout(300);

    /* 3 · the Library: a charm clicked on a sheet card's picture, and a gap */
    await page.evaluate(async () => { setMode('library'); await new Promise(r => setTimeout(r, 500)); CN.S.library.rows = [window.__row]; CN.renderLibrary(); await new Promise(r => setTimeout(r, 500)); });
    await page.waitForFunction(() => { const im = document.querySelector('#libBody .libCard[data-id="sheet-gold-2"] img.pv'); return im && im.complete && im.naturalWidth > 0; });
    const lib = (i, gap) => page.evaluate(([i, gap]) => {
      const card = document.querySelector('#libBody .libCard[data-id="sheet-gold-2"]'), im = card.querySelector('img.pv'); im.scrollIntoView({ block: 'center' });
      const r = im.getBoundingClientRect(), cs = getComputedStyle(im), n = v => parseFloat(v) || 0;
      // the picture as drawn: in the content box (the padding of the true-scale frame left out), fitted by object-fit: contain
      const bx = r.left + n(cs.borderLeftWidth) + n(cs.paddingLeft), by = r.top + n(cs.borderTopWidth) + n(cs.paddingTop);
      const bw = r.width - n(cs.borderLeftWidth) - n(cs.borderRightWidth) - n(cs.paddingLeft) - n(cs.paddingRight), bh = r.height - n(cs.borderTopWidth) - n(cs.borderBottomWidth) - n(cs.paddingTop) - n(cs.paddingBottom);
      const a = im.naturalWidth / im.naturalHeight, w = Math.min(bw, bh * a), h = w / a, x0 = bx + (bw - w) / 2, y0 = by + (bh - h) / 2;
      const rec = window.__rec, p = rec.placements[i], k = w / rec.stock.wPt, c = card.getBoundingClientRect();
      // (a gap: the sheet's right edge, below its last charm of the second row)
      const at = gap ? { x: x0 + w - 2, y: y0 + h * .45 } : { x: x0 + p.cxPt * k, y: y0 + p.cyPt * k };
      return { ...at, poolId: rec.charms.find(c => c.id === p.id).poolId, sheet: { x: x0, y: y0, w, h }, frame: { x: c.left, y: c.top, w: c.width, h: c.height } };
    }, [i, !!gap]);
    t = await lib(20); await page.waitForTimeout(200);
    await page.mouse.click(t.x, t.y);
    f0 = await firstFrame(page);
    assert(f0.flying && f0.snap, 'a Library card opens the window flying out of it, its picture on the plate: ' + JSON.stringify(f0));
    near(f0.sheet, t.sheet, 2, 'the plate\'s sheet starts over the picture\'s content box (inside its true-scale frame)');
    near(f0.curtain, t.frame, 2, 'the window\'s surface starts over the card');
    await page.waitForTimeout(1700);
    s = await settled(page);
    assert(s.open && !s.flip && !s.curtain && s.sel === t.poolId && s.view === 'piece' && /^Charm \d+ of 30$/.test(s.pos), 'it lands on the charm clicked on the picture: ' + JSON.stringify(s));
    await page.evaluate(() => SheetWin.close()); await page.waitForTimeout(200);
    s = await settled(page); assert(!s.open && !s.curtain, 'closed');
    t = await lib(0, true); await page.mouse.click(t.x, t.y); await page.waitForTimeout(1700);
    s = await settled(page);
    assert(s.open && !s.flip && s.view === 'sheet' && !s.sel, 'a gap in the picture opens on the sheet\'s orders list: ' + JSON.stringify(s));
    await page.evaluate(() => SheetWin.close()); await page.waitForTimeout(200);
    assert.deepStrictEqual(errors, [], 'no page errors');
    await page.close();

    /* 4 · reduced motion: no flight, the same landing */
    const rm = await browser.newPage({ viewport: { width: 1500, height: 900 }, reducedMotion: 'reduce' });
    const errs2 = []; rm.on('pageerror', e => errs2.push(e.message));
    await rm.goto(`http://127.0.0.1:${server.address().port}/charm-nest-1.html`);
    await rm.waitForFunction(() => window.CN && window.SheetWin && window.Gate);
    await setup(rm);
    const t3 = await rm.evaluate(() => { const sh = CN.S.sheets.gold.pages[0], cv = sh.el.querySelector('[data-r="canvas"]'); cv.scrollIntoView({ block: 'center' }); const r = cv.getBoundingClientRect(), v = sh._view, s = r.width / cv.width, p = sh.placements[4]; return { x: r.left + (v.R + p.cxPt * v.k) * s, y: r.top + (v.R + p.cyPt * v.k) * s, poolId: sh.charms.find(c => c.id === p.id).poolId }; });
    await rm.waitForTimeout(200); await rm.mouse.click(t3.x, t3.y);
    const flew = await rm.evaluate(() => !!SheetWin._W.flip || !!document.querySelector('.swCurtain'));
    await rm.waitForTimeout(900);
    s = await settled(rm);
    assert(!flew && s.open && s.sel === t3.poolId && s.view === 'piece' && !/swGrow/.test(s.cls), 'with reduced motion it opens without the flight, on the charm clicked: ' + JSON.stringify(s));
    await rm.evaluate(() => SheetWin.close()); await rm.waitForTimeout(300);
    assert(!(await rm.evaluate(() => SheetWin.isOpen())), 'and closes');
    assert.deepStrictEqual(errs2, [], 'no page errors (reduced motion)');
    console.log('sheet window opening OK · grows out of the Nest card and the Library card (plate over the card\'s sheet, surface over its frame), lands on the charm clicked, goes back into the card, Esc in flight, a gap opens the list, reduced motion');
  } finally { await browser.close(); server.close(); }
})().catch(e => { console.error(e); process.exit(1); });
