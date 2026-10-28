// MARGINS (Paul, 28 Oct 2026): "the margins for where engravable text is allowed are too high and we're losing too much surface area of the charm, The engravable area should be allowed to back up
// against the inner edge of the cut line for every charm. That goes for holes and cut outs and engraving and outer edge profile cut out. Text should be allowed to overlap with all engravings. The system
// should be a little bit more loose and allowing the user to place the text a bit more freely."
//   A  (node, the real geometry: charm-nest-geom.js, charm-nest-engrave-fit.js, the bridge's own fitOpts and the page's settings) on a made-up plate with a jump-ring hole, an inner cut-out and front art:
//      A1 the engravable area reaches the inner edge of the cut line as the work area draws it (half of CUT_LINE_PT, 0.088 mm) off the outer profile, the hole and the cut-out, where it stopped 0.3 mm off
//         before; no letter ever reaches the cut path itself
//      A2 words over the design's front art, and over parts a person added to a back, verify and are fitted there (the rule before refused them; it is kept only for placements approved under it)
//      A3 a place by hand a little over the edge slides back onto the metal at the very size asked (it used to shrink or be refused); a place clearly off the charm still is not taken; fitOpts by hand slides
//      A4 the Settings: the default margin is the cut line's inner edge and a saved 0.3 mm (or 0.8 mm) comes down to it (v27); a 0 is a 0, never the old 0.8
//   B  the real page (_engraveRealWorld.cjs, offline, the real COMET drawing): the work area's area reaches the cut line's inner edge (outer and hole) and the line is drawn CUT_LINE_PT wide; a drag that
//      puts the words a little over the edge is kept against the edge at the same size (not sent back, not made smaller), and still verifies on the metal
//   PW_DIR=<playwright node_modules> CHROMIUM=<chrome> node tests/charm-nest/engrave-margins.cjs      (MG_ONLY=A or B)
'use strict';
const path = require('node:path'), fs = require('node:fs'), vm = require('node:vm');
const ROOT = path.join(__dirname, '../..');
const G = require(path.join(ROOT, 'charm-nest-geom.js')), Fit = require(path.join(ROOT, 'charm-nest-engrave-fit.js')), ot = require(path.join(ROOT, 'vendor/opentype-1.3.4.min.js'));
const MM = 25.4 / 72, PT = 72 / 25.4;
const RESULTS = [];
const check = (id, ok, evidence) => { RESULTS.push({ id, ok: !!ok }); console.log(`${ok ? 'PASS' : 'FAIL'}  ${id}  ${ok ? '' : evidence || ''}`.trimEnd()); };

/** How close the area comes to each cut path (mm): for points every 0.5 pt along the path, the distance to the nearest pixel of the area (its centre), the median over the points; and the least distance of
 *  any pixel of the area from the path (mm). Pure: runs in node and, as its source, in the page. */
function reachOf(mask, polys) {
  const dist = (x, y) => { let d = Infinity; for (const pg of polys) for (let i = 0, j = pg.length - 1; i < pg.length; j = i++) { const a = pg[j], b = pg[i], dx = b[0] - a[0], dy = b[1] - a[1], L = dx * dx + dy * dy; let t = L ? ((x - a[0]) * dx + (y - a[1]) * dy) / L : 0; t = Math.max(0, Math.min(1, t)); d = Math.min(d, Math.hypot(x - a[0] - t * dx, y - a[1] - t * dy)); } return d; };
  const on = (px, py) => px >= 0 && py >= 0 && px < mask.w && py < mask.h && mask.bits[py * mask.w + px];
  const R = Math.ceil(1.2 * 72 / 25.4 * mask.res), gaps = [], near = new Set(); let least = Infinity;
  for (const pg of polys) for (let i = 0, j = pg.length - 1; i < pg.length; j = i++) {
    const a = pg[j], b = pg[i], n = Math.max(1, Math.ceil(Math.hypot(b[0] - a[0], b[1] - a[1]) / 0.5));
    for (let k = 0; k < n; k++) {
      const x = a[0] + (b[0] - a[0]) * k / n, y = a[1] + (b[1] - a[1]) * k / n, cx = Math.floor((x - mask.ox) * mask.res), cy = Math.floor((y - mask.oy) * mask.res); let best = Infinity;
      for (let py = cy - R; py <= cy + R; py++) for (let px = cx - R; px <= cx + R; px++) if (on(px, py)) { const X = mask.ox + (px + 0.5) / mask.res, Y = mask.oy + (py + 0.5) / mask.res, d = Math.hypot(X - x, Y - y); if (d < best) best = d; if (d < 0.5) near.add(py * mask.w + px); }
      if (best < Infinity) gaps.push(best);
    }
  }
  for (const i of near) least = Math.min(least, dist(mask.ox + (i % mask.w + 0.5) / mask.res, mask.oy + (Math.floor(i / mask.w) + 0.5) / mask.res));   // (the pixels nearest the path, each once)
  gaps.sort((p, q) => p - q);
  return { medianMm: gaps.length ? gaps[gaps.length >> 1] * 25.4 / 72 : Infinity, leastMm: least * 25.4 / 72, n: gaps.length };
}

function partA() {
  const fonts = { Regular: ot.loadSync(path.join(ROOT, 'vendor/fonts/SourceSans3-Regular.otf')), Semibold: ot.loadSync(path.join(ROOT, 'vendor/fonts/SourceSans3-Semibold.otf')) };
  const html = fs.readFileSync(path.join(ROOT, 'charm-nest-1.html'), 'utf8'), bridge = fs.readFileSync(path.join(ROOT, 'charm-nest-bridge.js'), 'utf8');
  const setting = +(/engraveMarginMm: ([0-9.]+), engraveMinCapMm/.exec(html) || [])[1];
  // a made-up plate, 20 x 12 mm: a jump-ring hole at the top, a cut-out low on the left, and front art in the middle (a blue hatched square and a red engraved ring)
  const poly = pts => pts.map((p, i) => [i ? 'l' : 'm', p]).concat([['h']]);
  const bb = pts => [Math.min(...pts.map(p => p[0])), Math.min(...pts.map(p => p[1])), Math.max(...pts.map(p => p[0])), Math.max(...pts.map(p => p[1]))];
  const rect = (x, y, w, h) => [[x, y], [x + w, y], [x + w, y + h], [x, y + h]];
  const ring = (cx, cy, r) => [...Array(48).keys()].map(i => [cx + r * Math.cos(i / 48 * 2 * Math.PI), cy + r * Math.sin(i / 48 * 2 * Math.PI)]);
  const seg = (pts, o) => Object.assign({ kind: 'path', closed: true, subpaths: [poly(pts)], bbox: bb(pts), lwPt: 0.25, paintOp: o.fill ? 'f' : 'S' }, o);
  const cutP = pts => seg(pts, { layer: 'CUT', stroke: true, fill: false, strokeRGB: [0, 0, 0] });
  const W = 20 * PT, H = 12 * PT;
  const outline = cutP(rect(0, 0, W, H)), hole = cutP(ring(W / 2, H - 1.6 * PT, 0.8 * PT)), cutout = cutP(rect(1.5 * PT, 1.5 * PT, 3 * PT, 2 * PT));
  const art = seg(rect(W / 2 - 3 * PT, H / 2 - 3.4 * PT, 6 * PT, 6 * PT), { layer: 'HATCH', stroke: false, fill: true, fillRGB: [0, 0, 1] });
  const artRing = seg(ring(W / 2 + 6 * PT, H / 2 - 0.4 * PT, 1.5 * PT), { layer: 'ENGRAVE', stroke: true, fill: false, strokeRGB: [1, 0.114, 0.145], lwPt: 0.283 });
  const charm = { outline, members: [outline, hole, cutout, art, artRing], bbox: outline.bbox, widthPt: W, heightPt: H };
  const res = 6, px = 1 / res * MM;

  /* A1 · the area reaches the cut line's inner edge, off the outer profile, the hole and the cut-out */
  const back = G.backView(charm, { res, upAngle: 90 }), front = G.backView(charm, { res, upAngle: 90, face: 'front' });
  const now = G.engraveMask(back, { marginMm: setting }), before = G.engraveMask(back, { marginMm: 0.3, keepArtClear: true });
  const parts = { 'outer profile': back.members[0], 'jump-ring hole': back.members[1], 'cut-out': back.members[2] };
  const reach = {}, was = {};
  for (const [k, m] of Object.entries(parts)) { const pg = G.flatten(m, 24); reach[k] = reachOf(now, pg); was[k] = reachOf(before, pg); }
  check(`A1a the cut line is drawn ${G.CUT_LINE_PT} pt wide and the area's edge is its inner edge: ${G.EDGE_MM.toFixed(3)} mm in from the cut path (half the line); the Settings default ${setting} mm is that edge`,
    G.CUT_LINE_PT === 0.5 && Math.abs(G.EDGE_MM - G.CUT_LINE_PT / 2 * MM) < 1e-12 && Math.abs(setting - G.EDGE_MM) < 0.005 && /lineWidth = Math.max\(1, \(G\.CUT_LINE_PT \|\| 0\.5\) \* k\)/.test(bridge),
    JSON.stringify({ CUT_LINE_PT: G.CUT_LINE_PT, EDGE_MM: G.EDGE_MM, setting }));
  for (const k of Object.keys(parts)) {
    check(`A1b ${k}: the area comes to ${reach[k].medianMm.toFixed(3)} mm of the cut path (the line's inner edge ${G.EDGE_MM.toFixed(3)} mm, give or take a ${px.toFixed(3)} mm pixel; it was ${was[k].medianMm.toFixed(3)} mm), and no pixel of it is nearer than ${reach[k].leastMm.toFixed(3)} mm`,
      reach[k].n > 20 && reach[k].medianMm <= G.EDGE_MM + 1.5 * px && reach[k].medianMm < was[k].medianMm - 0.15 && reach[k].leastMm >= G.EDGE_MM - 1.01 * px && reach[k].leastMm > 0.025,
      JSON.stringify({ now: reach[k], was: was[k] }));
  }
  check(`A1c the area grew: ${(G.area(now) / res / res * MM * MM).toFixed(1)} mm² of the plate's back, was ${(G.area(before) / res / res * MM * MM).toFixed(1)} mm²; a mask made with no margin given is the same (the cut line's inner edge)`,
    G.area(now) > G.area(before) * 1.05 && ((d, u) => d.bits.every((v, i) => v === u.bits[i]) && Math.abs(d.marginMm - G.EDGE_MM) < 1e-12)(G.engraveMask(back, {}), G.engraveMask(back, { marginMm: undefined })) && G.engraveMask(back, { marginMm: 0 }).marginMm === 0,
    JSON.stringify({ now: G.area(now), before: G.area(before) }));

  /* A2 · the words may overlap every engraving */
  const fNow = G.engraveMask(front, { marginMm: setting }), fWas = G.engraveMask(front, { marginMm: setting, keepArtClear: true });
  const artView = G.transformSeg(G.transformSeg(art, front.M), front.R), ac = [(artView.bbox[0] + artView.bbox[2]) / 2, (artView.bbox[1] + artView.bbox[3]) / 2];
  const L = G.layoutLines(['Cym'], fonts.Regular, 9, 0.216, 0, ac);
  const vNow = G.verifyInk(L.cmds, fNow), vWas = G.verifyInk(L.cmds, fWas);
  check('A2a words laid right over the front art (a hatched square, a red ring) verify on the front\'s area; the rule before refused them; the mask says it lets the words cross the art',
    vNow.ok && !vWas.ok && fNow.artFree === true && !fWas.artFree && front.dropped.length === 2, JSON.stringify({ vNow, vWas, dropped: front.dropped.length }));
  const fit = Fit.calculate({ charm, lines: ['Cym'], lineMode: 'auto', viewOptions: { res, upAngle: 90, face: 'front' }, maskOptions: { marginMm: setting, keepOut: [] },
    opts: { minCapMm: 1.6, maxHeightFrac: 0.4, lineGap: 0.216, minStrokeMm: 0, minGapMm: 0, tryRotated: false } }, fonts, G);
  const fitOld = Fit.calculate({ charm, lines: ['Cym'], lineMode: 'auto', viewOptions: { res, upAngle: 90, face: 'front' }, maskOptions: { marginMm: 0.3, keepOut: [], keepArtClear: true },
    opts: { minCapMm: 1.6, maxHeightFrac: 0.4, lineGap: 0.216, minStrokeMm: 0, minGapMm: 0, tryRotated: false } }, fonts, G);
  const inkBox = fit.fit && G.glyphPolys(fit.fit.cmds).flat().reduce((a, p) => [Math.min(a[0], p[0]), Math.min(a[1], p[1]), Math.max(a[2], p[0]), Math.max(a[3], p[1])], [Infinity, Infinity, -Infinity, -Infinity]);
  const overArt = inkBox && !(inkBox[2] < artView.bbox[0] || inkBox[0] > artView.bbox[2] || inkBox[3] < artView.bbox[1] || inkBox[1] > artView.bbox[3]);
  check(`A2b the automatic fit of the front uses the area over the art: "Cym" at cap ${fit.fit && fit.fit.capMm.toFixed(2)} mm across the art (it fitted at ${fitOld.fit ? fitOld.fit.capMm.toFixed(2) + ' mm beside it' : 'nothing'} before), its ink checked on the metal`,
    fit.fit && fit.check.ok && overArt && (!fitOld.fit || fit.fit.fittedMax >= fitOld.fit.fittedMax), JSON.stringify({ inkBox, art: artView.bbox, size: fit.fit && fit.fit.size, old: fitOld.fit && fitOld.fit.size }));
  // a back with a part a person added to it (charm editor: artBack, handed to the mask as `avoid`): the words may cross it too
  const added = seg(rect(W / 2 - 2 * PT, H / 2 - 2 * PT, 4 * PT, 3 * PT), { layer: 'ENGRAVE', stroke: false, fill: true, fillRGB: [0, 0, 0] });
  const bAvoid = G.engraveMask(back, { marginMm: setting, avoid: [added] }), bWas = G.engraveMask(back, { marginMm: setting, avoid: [added], keepArtClear: true });
  const addView = G.transformSeg(G.transformSeg(added, back.M), back.R), adc = [(addView.bbox[0] + addView.bbox[2]) / 2, (addView.bbox[1] + addView.bbox[3]) / 2];
  const L2 = G.layoutLines(['Cym'], fonts.Regular, 8, 0.216, 0, adc);
  check('A2c on a back, words over a part a person added in the charm editor verify (the rule before kept them off it)', G.verifyInk(L2.cmds, bAvoid).ok && !G.verifyInk(L2.cmds, bWas).ok && bAvoid.bits.every((v, i) => v === now.bits[i]),
    JSON.stringify({ now: G.verifyInk(L2.cmds, bAvoid), was: G.verifyInk(L2.cmds, bWas) }));

  /* A3 · placing by hand is looser: a little over the edge slides back at the same size */
  const font = fonts.Regular, size = 9, c0 = [back.cx, back.cy - 1 * PT], fits = dx => G.verifyInk(G.layoutLines(['Cym'], font, size, 0.216, 0, [c0[0] + dx, c0[1]]).cmds, now).ok;
  let dx0 = 0; while (fits(dx0 + 0.02 * PT) && dx0 < W) dx0 += 0.02 * PT;
  const drop = [c0[0] + dx0 + 0.3 * PT, c0[1]], byHand = { lineGap: 0.216, maxHeightFrac: 40, slideMm: 1 };
  const slid = G.reflowAt(['Cym'], font, now, byHand, { centre: drop, angle: 0, size }, 'auto');
  const shrunk = G.reflowAt(['Cym'], font, now, { lineGap: 0.216, maxHeightFrac: 40 }, { centre: drop, angle: 0, size }, 'auto');
  const moved = slid.ok ? Math.hypot(slid.centre[0] - drop[0], slid.centre[1] - drop[1]) * MM : Infinity;
  check(`A3a words dropped 0.3 mm over the right edge keep their size (${size} pt) and slide ${moved.toFixed(3)} mm back onto the metal, against the edge; without the slide they were made smaller (${shrunk.ok ? shrunk.size.toFixed(2) + ' pt' : 'refused'})`,
    slid.ok && Math.abs(slid.size - size) < 1e-9 && moved <= 0.36 && slid.centre[0] >= c0[0] + dx0 - 0.36 * PT && G.verifyInk(slid.cmds, now).ok && (!shrunk.ok || shrunk.size < size - 0.01),
    JSON.stringify({ slid: slid.ok && { size: slid.size, centre: slid.centre, slidMm: slid.slidMm }, drop, dx0, shrunk: shrunk.ok && shrunk.size }));
  const far = [W + 6 * PT, c0[1]], farR = G.reflowAt(['Cym'], font, now, byHand, { centre: far, angle: 0, size }, 'auto');
  check('A3b words dropped clearly off the charm (6 mm past its edge) are not slid back across it: no room there, as before', !farR.ok && G.slideAt(['Cym'], font, now, byHand, { centre: far, angle: 0 }, size) === null, JSON.stringify({ ok: farR.ok, size: farR.size }));
  // the bridge's own fitOpts: by hand it slides, the automatic fit does not
  const grab = (a, b) => { const i = bridge.indexOf(a), j = bridge.indexOf(b, i); if (!(i > 0 && j > i)) throw new Error('bridge section not found: ' + a); return bridge.slice(i, j); };
  const ctx = vm.createContext({ S: { settings: { engraveMinCapMm: 1.6, engraveMaxHeightFrac: 0.4, engraveMinStrokeMm: 0, engraveMinGapMm: 0 } } });
  vm.runInContext(grab('  const fitOpts =', '  const items =') + '\nthis.fitOpts=fitOpts;', ctx);
  const job = { mask: now, lineGap: 0.216 };
  check('A3c the editor\'s placing by hand (fitOpts(job, true)) slides up to 1 mm; the automatic fit does not', ctx.fitOpts(job, true).slideMm === 1 && ctx.fitOpts(job).slideMm == null, JSON.stringify(ctx.fitOpts(job, true)));
  // what the laser engraves is still only on the metal: the written file's check (verifyBackFile) is made on the same rule
  check('A3d the ink check is not loosened: every placement still verifies on the area (verifyInk), and the written file\'s check reads the mask\'s own margin', /G\.verifyInk\(textCmds,G\.engraveMask\(\{mask:W\},\{marginMm:job\.mask\?\.marginMm \|\| 0\}\)\)/.test(bridge) && !G.verifyInk(G.layoutLines(['Cym'], font, size, 0.216, 0, drop).cmds, now).ok, '');

  /* A4 · the Settings */
  const line = (/if \(\(saved\.v \|\| 1\) < 27\) \{[^\n]*\}/.exec(html) || [''])[0];
  const migrate = saved => { const c = vm.createContext({ saved, DEFAULTS: { engraveMarginMm: setting } }); vm.runInContext(line, c); return saved; };
  const m1 = migrate({ v: 26, engraveMarginMm: 0.3 }), m2 = migrate({ v: 17, engraveMarginMm: 0.8 }), m3 = migrate({ v: 26, engraveMarginMm: 0.05 }), m4 = migrate({ v: 27, engraveMarginMm: 0.3 }), m5 = migrate({ v: 26 });
  check(`A4 Settings v27: a saved 0.3 mm and an old 0.8 mm come down to ${setting} mm, a smaller one a person chose is kept, a later choice is never touched again; the margin box steps by 0.01 mm`,
    !!line && /v: 27,/.test(html) && m1.engraveMarginMm === setting && m1.v === 27 && m2.engraveMarginMm === setting && m3.engraveMarginMm === 0.05 && m4.engraveMarginMm === 0.3 && m5.engraveMarginMm === setting && /id="stEngraveMarginMm" type="number" min="0" max="5" step="0.01"/.test(html),
    JSON.stringify({ line: !!line, m1, m2, m3, m4, m5 }));
  // the masks every maker of a face's area builds read the Settings as they are (a 0 is a 0: engraveMask's own default only for no number)
  check('A4b no maker of a face\'s area falls back to the old 0.8 mm', !/engraveMarginMm \|\| \.8|engraveMarginMm \|\| 0\.8/.test(bridge) && !/engraveMarginMm \|\| 0\.8/.test(fs.readFileSync(path.join(ROOT, 'charm-nest-engrave-preview.js'), 'utf8')), '');
}

async function partB() {
  const R = require('./_engraveRealWorld.cjs');
  const w = await R.world({ viewport: { width: 1440, height: 900 } });
  if (!w) { console.log('SKIP no playwright-core'); return; }
  const page = w.page;
  await page.waitForFunction(() => !window.CNRole || ['ask', 'role', 'admin'].includes(CNRole.state()), null, { timeout: 20000 }).catch(() => {});
  await page.evaluate(() => { try { if (window.CNRole && CNRole.state() === 'ask') CNRole.choose('design'); } catch (_) {} });
  try {
    await R.build(w, ['back']);
    const jkey = await R.openEngraveDetail(w, 'back', { face: 'back' }); await R.settle(page, 700);
    await page.waitForFunction(k => { const j = Engrave.items().get(k), c = [...document.querySelectorAll('.backHost canvas')].find(x => x._editable); return j && j.fit && j.state === 'review' && c && c._box; }, jkey, { timeout: 30000 }).catch(() => {});
    const b1 = await page.evaluate(([k, src]) => {
      const reachOf = eval('(' + src + ')'), G = CharmNestGeom, j = Engrave.items().get(k), bc = [...document.querySelectorAll('.backHost canvas')].find(x => x._editable);
      const members = j.view.members.map(m => ({ w: m.bbox[2] - m.bbox[0], r: reachOf(j.mask, G.flatten(m, 24)) }));
      return { margin: j.mask.marginMm, setting: S.settings.engraveMarginMm, artFree: !!j.mask.artFree, EDGE: G.EDGE_MM, members, k: bc && bc._map.k, state: j.state };
    }, [jkey, reachOf.toString()]);
    check(`B1 the real COMET back: the area is made by the new rule (${b1.margin} mm, the Settings' ${b1.setting} mm) and comes to ${b1.members.map(m => m.r.medianMm.toFixed(3)).join(' / ')} mm of each cut path (outer, hoop, hole): the cut line's inner edge`,
      Math.abs(b1.margin - b1.EDGE) < 0.005 && b1.artFree && b1.members.length >= 2 && b1.members.every(m => m.r.n > 10 && m.r.medianMm <= b1.EDGE + 1.5 / 6 * MM), JSON.stringify(b1));   // (no least distance here: the real drawing's hoop ring runs partly through the welded metal, inside the area)
    // B2 · a drag that puts the words a little over the edge
    const plan = await page.evaluate(k => {
      const G = CharmNestGeom, j = Engrave.items().get(k), f = j.fit, sh = (cmds, dx) => cmds.map(c => { if (c.type === 'Z') return c; const o = Object.assign({}, c, { x: c.x + dx }); if (c.x1 != null) o.x1 = c.x1 + dx; if (c.x2 != null) o.x2 = c.x2 + dx; return o; });
      let dx0 = 0; const step = 0.02 * 72 / 25.4; while (G.verifyInk(sh(f.cmds, dx0 + step), j.mask).ok && dx0 < 200) dx0 += step;
      const bc = [...document.querySelectorAll('.backHost canvas')].find(x => x._editable), r = bc.getBoundingClientRect(), s = r.width / bc.width, p = bc._box.centrePx;
      const over = 0.3 * 72 / 25.4, dxPt = dx0 + over;
      return { size: f.size, centre: f.centre.slice(), dx0, dxPt, from: [r.left + p[0] * s, r.top + p[1] * s], by: dxPt * bc._map.k * s, maskCx: j.mask.cx };
    }, jkey);
    await page.mouse.move(plan.from[0], plan.from[1]); await page.mouse.down();
    for (let i = 1; i <= 8; i++) { await page.mouse.move(plan.from[0] + plan.by * i / 8, plan.from[1]); await R.settle(page, 30); }
    await page.mouse.up(); await R.settle(page, 500);
    if (process.env.SHOTS) await R.shot(w, 'MARGINS-drag-to-edge', { clip: await page.evaluate(() => { const r = [...document.querySelectorAll('.backHost canvas')].find(x => x._editable).getBoundingClientRect(); return { x: r.left, y: r.top, width: r.width, height: r.height }; }) });
    const after = await page.evaluate(k => { const G = CharmNestGeom, j = Engrave.items().get(k); return { size: j.fit.size, centre: j.fit.centre.slice(), ok: G.verifyInk(j.fit.cmds, j.mask).ok, verify: !!(j.verify && j.verify.geometry && j.verify.geometry.ok), nudged: !!j.nudged, toasts: [...document.querySelectorAll('.toast, #toast, .toasts > *')].map(t => t.textContent).join(' | ') }; }, jkey);
    const dxMm = (after.centre[0] - plan.centre[0]) * MM, wantMm = plan.dxPt * MM, edgeMm = plan.dx0 * MM;
    check(`B2 a drag ${wantMm.toFixed(2)} mm to the right, 0.3 mm over the edge: the words stay against the edge (moved ${dxMm.toFixed(2)} mm; room ended at ${edgeMm.toFixed(2)} mm) at the same size (${plan.size.toFixed(2)} → ${after.size.toFixed(2)} pt), not sent back, and verify on the metal`,
      Math.abs(after.size - plan.size) < 1e-6 && dxMm >= edgeMm - 0.36 && dxMm <= wantMm + 0.05 && after.ok && after.verify && after.nudged && !/No room there/.test(after.toasts), JSON.stringify({ plan, after }));
    check('B3 no page error', w.errors.length === 0, JSON.stringify(w.errors).slice(0, 600));
  } finally { await w.close(); }
}

(async () => {
  const only = process.env.MG_ONLY || '';
  if (!only || only === 'A') partA();
  if (!only || only === 'B') await partB();
  const bad = RESULTS.filter(r => !r.ok);
  console.log(`engrave-margins: ${RESULTS.length - bad.length}/${RESULTS.length} checks pass`);
  process.exit(bad.length ? 1 : 0);
})().catch(e => { console.error(e); process.exit(1); });
