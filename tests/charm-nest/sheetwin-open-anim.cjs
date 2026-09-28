// The sheet window grows out of the sheet that was clicked and lands on the charm clicked (Paul, 28 Sep): a charm
// clicked on a Nest card, and a charm clicked on a Library card's picture. Opens the page in headless Chromium with a
// sheet of 30 charms on the Gold card (a stand-in painter draws them; the sheet's record is answered in the page), and
// checks, with the window's animations held at their first frame, that the plate's sheet lies over the card's sheet and
// the window's surface over the card's frame, opaque and in the card's colour, the sheet drawn at the window's size (a
// Library card: its own picture) with the charm or point clicked marked, the panel holding its shape and the header
// still to come, on the review's timings (28 Sep); then, played through, that the window lands on the charm clicked
// ("Charm N of M"), with nothing of the flight left; that a landing ring (a charm put on by hand) never stops the plate's
// marks or their loop; that closing goes back into the card, and fades where it is when the card is out of view or shows
// another of its sheets; that Esc in flight closes cleanly and it opens again as before; that a slow read holds the
// panel's shape and then shows the charm, the plate not moving; that a Library picture clicked in a gap opens on the
// orders list; and that with reduced motion it opens without the flight, on the same charm.
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
      if (fn === 'charmNestLibrary' && body && body.op === 'getSheet') { window.__reads++; await new Promise(r => setTimeout(r, window.__delay || 40)); return { sheet: JSON.parse(JSON.stringify(window.__rec)) }; }
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
  const snap = E.plate.querySelector('canvas.swSnap'), tint = cur && cur.lastChild, op = el => +getComputedStyle(el).opacity;
  const timing = el => anims.filter(a => a.effect.target === el).map(a => { const t = a.effect.getTiming(), k = a.effect.getKeyframes(); return { ms: t.duration, delay: t.delay, easing: t.easing.replace(/\s/g, ''), props: [...new Set(k.flatMap(f => Object.keys(f).filter(p => !['offset', 'easing', 'composite', 'computedOffset'].includes(p))))].join() }; });
  const out = { flying: !!W.flip, n: anims.length, sheet: { x: fx.left + W.R * s, y: fx.top + W.R * s, w: W.st.wPt * W.k * s, h: W.st.hPt * W.k * s }, curtain: box(cur && cur.getBoundingClientRect()), side: op(E.side), head: op(E.headBar), snap: !!snap,
    snapW: snap ? snap.width : 0, curOp: cur ? op(cur) : 0, tint: tint ? op(tint) : 0, tintBg: tint ? getComputedStyle(tint).backgroundColor : '', rule: op(E.rule), base: op(E.base), pre: !!(W.pre && W.pre.pieces), preSel: W.pre && W.pre.sel ? W.pre.sel.poolId : null, at: !!(W.pre && W.pre.at),
    pv: E.pv.getAttribute('src') || '', pvShown: op(E.pv), skel: !E.skel.hidden,
    plateT: timing(E.plate), curtainT: timing(cur), headT: timing(E.headBar) };
  anims.forEach(a => a.play());
  return out;
});
const bez = s => (String(s).match(/-?[\d.]+/g) || []).map(Number).join();
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
    assert(f0.side === 0 && f0.head === 0 && f0.skel, 'the panel (holding its shape) and the header are still to come: ' + JSON.stringify(f0));
    // (the window draws the card's sheet itself, at its own size, from the first frame: no copy of the card's small drawing)
    assert(f0.pre && f0.base === 1 && !f0.snap && f0.preSel === t.poolId, 'the plate shows the sheet drawn at the window\'s size, the charm clicked marked on it: ' + JSON.stringify(f0));
    assert(f0.curOp === 1 && f0.tint === 1 && /^rgb\(/.test(f0.tintBg) && f0.rule === 0, 'the surface is opaque from the first frame, in the card\'s colour, the rulers still to come: ' + JSON.stringify(f0));
    // (the timings of the review: the sheet 650 ms, the surface 600 ms, both on (.3,0,.1,1); the header from 560 ms)
    const GROW = bez('cubic-bezier(.3,0,.1,1)'), mv = (T, prop) => T.find(x => x.props.includes(prop)) || {};
    assert(mv(f0.plateT, 'transform').ms === 650 && bez(mv(f0.plateT, 'transform').easing) === GROW, 'the sheet grows over 650 ms: ' + JSON.stringify(f0.plateT));
    assert(mv(f0.curtainT, 'transform').ms === 600 && bez(mv(f0.curtainT, 'transform').easing) === GROW, 'the surface over 600 ms: ' + JSON.stringify(f0.curtainT));
    assert(mv(f0.headT, 'opacity').delay === 560, 'the header from 560 ms: ' + JSON.stringify(f0.headT));
    await page.waitForTimeout(1700);
    let s = await settled(page);
    assert(s.open && !s.flip && !s.curtain && !s.snap && s.cls === 'sheetWin swGrow', 'it settles with nothing of the flight left: ' + JSON.stringify(s));
    assert.deepStrictEqual(s.anims, [0, 0, 0, 0], 'no animation is left on the plate, header, panel or strip');
    assert.strictEqual(s.sel, t.poolId, 'the window lands on the charm clicked');
    assert(s.view === 'piece' && /^Charm \d+ of 30$/.test(s.pos) && s.side === '1' && s.plate === 'none', 'on its piece view ("Charm N of M"), the panel in, the plate in place: ' + JSON.stringify(s));
    const pos = await page.evaluate(() => { const W = SheetWin._W, list = W.pieces.slice().sort((a, b) => (a.rid || '~').localeCompare(b.rid || '~') || (a.sku || '').localeCompare(b.sku || '') || a.copy - b.copy); return list.indexOf(W.sel) + 1; });
    assert.strictEqual(s.pos, `Charm ${pos} of 30`, 'the same state as stepping to it with ›');

    // a landing ring (a charm put on the sheet by hand rings where it went) has no piece of its own: with one ringing,
    // the plate's marks are still drawn at once (a hover) and in the loop (a click's pulse), and the loop lets go when
    // it is over; a charm clicked after it pulses as ever (it used to throw, stopping the drop and holding the loop)
    const warns = []; page.on('console', m => { if (/plate marks/.test(m.text())) warns.push(m.text()); });
    const plateAt = i => page.evaluate(i => { const W = SheetWin._W, r = W.el.fx.getBoundingClientRect(), s = r.width / W.el.fx.width, x = W.pieces[i]; return { x: r.left + (W.R + x.p.cxPt * W.k) * s, y: r.top + (W.R + x.p.cyPt * W.k) * s, poolId: x.poolId }; }, i);
    const loop = () => page.evaluate(() => { const W = SheetWin._W; return { raf: W.raf, kinds: W.fx.map(f => f.kind), sel: W.sel && W.sel.poolId }; });
    await page.evaluate(() => { SheetWin._W.fx.push({ kind: 'land', t0: performance.now(), ms: 1100 }); });
    let c = await plateAt(2); await page.mouse.move(c.x, c.y); await page.waitForTimeout(50);
    c = await plateAt(3); await page.mouse.click(c.x, c.y);
    let L = await loop();
    assert(L.raf && L.kinds.includes('land') && L.kinds.includes('pulse') && L.sel === c.poolId, 'a charm clicked while a landing rings is chosen and pulses: ' + JSON.stringify(L));
    await page.waitForTimeout(1300);
    L = await loop(); assert(!L.raf && !L.kinds.length, 'the loop lets go once the ring and the pulse are over: ' + JSON.stringify(L));
    c = await plateAt(5); await page.mouse.click(c.x, c.y);
    L = await loop(); assert(L.raf && L.kinds.includes('pulse') && L.sel === c.poolId, 'and a charm clicked after it pulses again: ' + JSON.stringify(L));
    await page.waitForTimeout(1100);
    L = await loop(); assert(!L.raf && !L.kinds.length, 'and lets go: ' + JSON.stringify(L));
    assert.deepStrictEqual(errors.concat(warns), [], 'no mark failed to draw');
    await page.mouse.move(5, 5);

    // closing goes back into the card: the plate's sheet ends over the card's sheet
    await page.evaluate(() => { SheetWin.close(); });
    await page.waitForTimeout(60);
    const back = await page.evaluate(() => { const W = SheetWin._W, a = W.el.plate.getAnimations().find(x => x.effect.getKeyframes().some(k => k.transform && k.transform !== 'none')); if (!a) return null; a.pause(); a.currentTime = a.effect.getComputedTiming().endTime; const fx = W.el.fx.getBoundingClientRect(), s = fx.width / W.el.fx.width, out = { x: fx.left + W.R * s, y: fx.top + W.R * s, w: W.st.wPt * W.k * s, h: W.st.hPt * W.k * s, ms: a.effect.getTiming().duration, easing: a.effect.getTiming().easing }; a.play(); return out; });
    assert(back, 'closing flies the plate back');
    assert(back.ms === 480 && bez(back.easing) === bez('cubic-bezier(.4,0,.2,1)'), 'over 480 ms on (.4,0,.2,1): ' + JSON.stringify(back));
    const t2 = await page.evaluate(() => { const sh = CN.S.sheets.gold.pages[0], cv = sh.el.querySelector('[data-r="canvas"]'), r = cv.getBoundingClientRect(), v = sh._view, s = r.width / cv.width, st = stockFor('gold', sh); return { x: r.left + v.R * s, y: r.top + v.R * s, w: st.wPt * v.k * s, h: st.hPt * v.k * s }; });
    near(back, t2, 2, 'the plate goes back into the card\'s sheet');
    await page.waitForTimeout(900);
    s = await settled(page); assert(!s.open && !/swGrow|swFlying|swBack|closing/.test(s.cls) && !s.curtain, 'closed, clean: ' + JSON.stringify(s));

    /* 1b · a grown window that cannot go back into its card fades where it is (it used to vanish at once): the card out
       of view, or the card showing another of its sheets (a tab, a merge), which it must not go back into */
    const openOn = async i => { const t = await nest(i); await page.waitForTimeout(150); await page.mouse.click(t.x, t.y); await page.waitForTimeout(1700); return t; };
    const fadeOut = async () => {
      const m0 = await page.evaluate(() => ({ cls: SheetWin._W.dlg.className, home: !!(SheetWin._W.origin && SheetWin._W.origin.rects()) }));
      // (the dialog's opacity each frame until it closes: the least of it)
      await page.evaluate(() => { const d = SheetWin._W.dlg; window.__minOp = 1; const tick = () => { if (!d.open) return; window.__minOp = Math.min(window.__minOp, +getComputedStyle(d).opacity); requestAnimationFrame(tick); }; SheetWin.close(); requestAnimationFrame(tick); });
      await page.waitForTimeout(60);
      const m = await page.evaluate(() => { const d = SheetWin._W.dlg, cs = getComputedStyle(d); return { cls: d.className, anim: cs.animationName, curtain: !!document.querySelector('.swCurtain'), flying: SheetWin._W.el.plate.getAnimations().length }; });
      await page.waitForTimeout(500);
      return { m0, ...m, op: await page.evaluate(() => window.__minOp), after: await settled(page) };
    };
    await openOn(9);
    await page.evaluate(() => { CN.S.sheets.gold.pages[0].el.style.display = 'none'; });
    let fo = await fadeOut();
    await page.evaluate(() => { CN.S.sheets.gold.pages[0].el.style.display = ''; });
    assert(/swGrow/.test(fo.m0.cls) && !fo.m0.home, 'the window grew out of the card, which is now out of view: ' + JSON.stringify(fo));
    assert(/closing/.test(fo.cls) && fo.anim === 'swOut' && fo.op < .8 && !fo.curtain && !fo.flying, 'it fades where it is: ' + JSON.stringify(fo));
    assert(!fo.after.open && !fo.after.curtain && !/swGrow|closing/.test(fo.after.cls), 'and closes, clean: ' + JSON.stringify(fo.after));
    // (the card shows another of its sheets meanwhile: the same canvas, on screen, but not this sheet)
    await openOn(9);
    await page.evaluate(() => { addPage('gold'); showPage('gold', 1); });
    fo = await fadeOut();
    await page.evaluate(() => { const pg = CN.S.sheets.gold.pages[1]; removePage(pg); });
    assert(!fo.m0.home && fo.anim === 'swOut' && fo.op < .8 && !fo.curtain && !fo.flying, 'the card showing another sheet, it fades where it is: ' + JSON.stringify(fo));
    assert(!fo.after.open && !fo.after.curtain, 'and closes: ' + JSON.stringify(fo.after));
    assert(await page.evaluate(() => { const g = CN.S.sheets.gold; return g.pages.length === 1 && g.pages[0].el === g.cardEl; }), 'the card is back on its sheet');

    /* 2 · Esc in flight: the flight turns round and the window closes; the next opening is as the first */
    t = await nest(7); await page.mouse.click(t.x, t.y); await page.waitForTimeout(200);
    await page.keyboard.press('Escape');
    await page.waitForTimeout(900);
    s = await settled(page); assert(!s.open && !s.curtain, 'Esc in flight closes it: ' + JSON.stringify(s));
    t = await nest(7); await page.mouse.click(t.x, t.y); await page.waitForTimeout(1700);
    s = await settled(page); assert(s.open && !s.flip && !s.curtain && s.sel === t.poolId && s.side === '1', 'and it opens again as before: ' + JSON.stringify(s));
    await page.evaluate(() => SheetWin.close()); await page.waitForTimeout(700);

    /* 2b · a slow read: the panel holds its shape (no list, no veil) until the charm is known, then shows it at once; the
       plate and its foot do not move when the record comes */
    await page.evaluate(() => { window.__delay = 1300; });
    t = await nest(11); await page.waitForTimeout(150); await page.mouse.click(t.x, t.y); await page.waitForTimeout(1000);
    const look = () => page.evaluate(() => { const W = SheetWin._W, E = W.el, r = E.plate.getBoundingClientRect(); return { skel: !E.skel.hidden, view: W.view, veil: !E.veil.hidden, flip: !!W.flip, plate: [r.left, r.top, r.width, r.height].map(Math.round).join(), strip: Math.round(E.strip.getBoundingClientRect().height), sel: W.sel && W.sel.poolId, pos: E.pos.textContent }; });
    const held = await look();
    await page.waitForTimeout(800);
    const got = await look();
    await page.evaluate(() => { window.__delay = 0; });
    assert(held.skel && !held.veil && held.view === 'sheet' && !held.sel, 'while the sheet is read, the panel holds its shape, with no veil over the sheet: ' + JSON.stringify(held));
    assert(got.view === 'piece' && !got.skel && !got.flip && got.sel === t.poolId && /^Charm \d+ of 30$/.test(got.pos), 'then shows the charm clicked: ' + JSON.stringify(got));
    assert(held.plate === got.plate && held.strip === got.strip && held.strip >= 30, 'the plate and its foot stay where they are: ' + JSON.stringify([held, got]));
    await page.evaluate(() => SheetWin.close()); await page.waitForTimeout(700);

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
      return { ...at, poolId: rec.charms.find(c => c.id === p.id).poolId, sheet: { x: x0, y: y0, w, h }, frame: { x: c.left, y: c.top, w: c.width, h: c.height }, src: im.currentSrc, nw: im.naturalWidth };
    }, [i, !!gap]);
    t = await lib(20); await page.waitForTimeout(200);
    await page.mouse.click(t.x, t.y);
    f0 = await firstFrame(page);
    // (the plate shows the card's own picture, sharp: the window's preview points at it; a copy only until it has decoded
    // it, and then at the picture's own size)
    assert(f0.flying && f0.pv === t.src && f0.pvShown === 1 && (!f0.snap || f0.snapW === t.nw), 'a Library card opens the window flying out of it, its own picture on the plate: ' + JSON.stringify(f0));
    assert(f0.at && f0.curOp === 1 && f0.tint === 1 && f0.skel, 'the point clicked marked from the first frame, the surface opaque, the panel holding its shape: ' + JSON.stringify(f0));
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
    console.log('sheet window opening OK · grows out of the Nest card and the Library card (plate over the card\'s sheet, surface over its frame, opaque, the sheet sharp, the charm marked, review timings), lands on the charm clicked, a landing ring keeps the marks\' loop, goes back into the card or fades (card out of view, card on another sheet), Esc in flight, slow read, a gap opens the list, reduced motion');
  } finally { await browser.close(); server.close(); }
})().catch(e => { console.error(e); process.exit(1); });
