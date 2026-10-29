// ENGVIEW (Paul, 29 Oct 2026, the Design & Engrave detail card's work area):
//   "5. Remove the "Back" and "Front" toggle from this place. Instead the user can now click on the actual back and front thumbnail ... The switch between back and front needs to be animated in the same
//    way as we currently do for the Sheets when transitioning between back and front views."
//   "6. ... We also have a duplicate way to select pieces in a multi-charm order ... simply remove the visual bar images and just let the user choose using the existing toggle. This will also solve the
//    overlapping issue with the "Open in Charm Editor" button."
//   "7. There is no zoom/Pan functionality in the engraving preview. Look at how we handle that in the charm editor and add the exact same functionality. Also add the same dynamic X, Y Rulers that adapt
//    to the real size as the user zooms in/out."
//   "8. Remove the "SMALL cap. 1.22" text and function from this UI."
// The real sorter page over the repo's fake site (_engraveRealWorld.cjs; offline, made-up names), on two real masters (the bird SILHOUETTE_34560 engraved on its back, its front view only; the TINY BAR
// engraved on its front):
//   1  no Front | Back switch on the grid, no row of piece pictures above it, no "SMALL · cap" text in the head (even for a fit flagged small)
//   2  the wheel zooms about the pointer (the point under it stays put), the grid's box never moves or resizes, the zoom read-out follows
//   3  the rulers (the editor's paintRuler) rescale with the zoom: px a mm = the drawing's own scale, the numbered step from the editor's 1-2-5 ladder, in mm from the piece's top-left
//   4  Space-drag, the middle button and the Hand each pan by exactly the hand's travel; ⌘ + / − step the editor's ladder; ⌘ 0 is the steady fit again; the zoom box's preset
//   5  at 300 % the words dragged by the pointer move by exactly the pointer's travel in pt (the map carries the zoom), and the fit / placement are otherwise untouched
//   6  Engrave.showFace turns the work area (the egFlipping class, a rotateY turn) and it ends on the other face; called for the face shown: nothing happens
//   7  at 1440×900, 1280×800 and 1024×768 nothing at the top overlaps: Open in Charm Editor, Approve, the grid; the zoom box and the rulers are inside the grid, the grid's label clear of the rulers
//   PW_DIR=<playwright node_modules> CHROMIUM=<chrome> node tests/charm-nest/engrave-work-zoom.cjs      (SHOTS=<dir> for the screenshots)
'use strict';
const RESULTS = [];
const check = (id, ok, evidence) => { RESULTS.push({ id, ok: !!ok }); console.log(`${ok ? 'PASS' : 'FAIL'}  ${id}  ${ok ? '' : evidence || ''}`.trimEnd()); };
const Q = '#egQueue .rvItem[data-kind=placement]';
const VIEWS = [[1440, 900], [1280, 800], [1024, 768]];
const PT = 72 / 25.4, PX100 = 96 / 72;

function seedWorld(R) {
  R.DESIGNS.SILHOUETTE_34560 = { file: 'PERCHED_43564.ai', entry: { sku: 'SILHOUETTE_34560', widthPt: 36.515260000000126, heightPt: 31.177639999999883, areaPt2: 449.3611111111111, members: 4, holes: 2, engravable: true, engravableBy: 'index', upAngle: 267.50234443756403, upSource: 'hole', charmHash: '831b2d80', backKeepOut: [] } };
  R.DESIGNS['TINY BAR_7427'] = { file: 'TINY_BAR_7427.ai', entry: { sku: 'TINY BAR_7427', sym: 'symmetric', widthPt: 50.956999999999994, heightPt: 11.25, areaPt2: 420.6388888888889, members: 1, holes: 0, engravable: true, engravableBy: 'index', upAngle: 89.852, upSource: 'long', charmHash: '9ac1e42a', backKeepOut: [] } };
  R.ORDERS.sil = Object.assign({}, R.ORDERS.back, { rid: '4290000141', sku: 'SILHOUETTE_34560', listing: '1890000141', title: 'Perched Bird Silhouette Necklace in Sterling Silver', buyer: 'Name Twelve', personalization: ['T'] });
  R.ORDERS.bar = { rid: '4290000151', sku: 'TINY BAR_7427', listing: '1890000151', title: 'Tiny Bar Necklace, Personalized Coordinates Bar Necklace in Sterling Silver', buyer: 'Name Thirteen', placed: { day: 0, h: 10, m: 20 }, ship: 5,
    variations: [{ name: 'Metal choice', value: 'Silver' }, { name: 'Chain length', value: '16"' }, { name: 'Font', value: 'Angelina' }], personalization: ['33.47765N 86.80020W'] };
  R.EXPECT_JOBS.sil = 1; R.EXPECT_JOBS.bar = 1;
}

(async () => {
  const R = require('./_engraveRealWorld.cjs'); seedWorld(R);
  const shots = process.env.SHOTS || '/tmp/engrave-work-zoom-shots';
  const w = await R.world({ viewport: { width: VIEWS[0][0], height: VIEWS[0][1] }, shots });
  if (!w) { console.log('SKIP no playwright-core'); return; }
  await w.page.waitForFunction(() => !window.CNRole || ['ask', 'role', 'admin'].includes(CNRole.state()), null, { timeout: 20000 }).catch(() => {});
  await w.page.evaluate(() => { try { if (window.CNRole && CNRole.state() === 'ask') CNRole.choose('design'); } catch (_) { /* no role step on this page */ } });
  const page = w.page, st = w.srv.st;
  const settle = ms => R.settle(page, ms);
  /** The work area now (screen px): the grid, its view (zoom, centre), the rulers, the zoom box, the head, and what must stay clear of each other. */
  const look = () => page.evaluate(Q => {
    const c = document.querySelector(Q), cv = c && c.querySelector('.backHost > canvas'), j = c && Engrave.items().get(c.dataset.key); if (!cv || !j) return null;
    const rect = el => { if (!el) return null; const r = el.getBoundingClientRect(); return r.width && r.height ? [r.left, r.top, r.right, r.bottom].map(v => Math.round(v * 10) / 10) : null; };
    const s = cv.getBoundingClientRect(), sc = s.width / cv.width, f = cv._steadyFit, g = cv._gridLabel, rx = c.querySelector('.backHost .egRuleX'), ry = c.querySelector('.backHost .egRuleY');
    const head = c.querySelector('.rh');
    return { key: j.key, grid: rect(cv), k: cv._map.k * sc, k0: f && f.k0 * sc, z: f && f.z, c: f && f.c, fit: f && [f.cx, f.cy], face: cv._viewFace || (j.face === 'front' ? 'front' : 'back'), zero: cv._zero,
      label: g ? [s.left + g.x * sc, s.top + g.y * sc, s.left + (g.x + g.w) * sc, s.top + (g.y + g.h) * sc] : null, rx: rect(rx), ry: rect(ry), rc: rect(c.querySelector('.backHost .egRuleC')),
      pxmm: rx ? +rx.dataset.pxmm : 0, zb: rect(c.querySelector('.backHost .egZoom')), read: (c.querySelector('.egZoom [data-ezread]') || {}).textContent || '',
      ce: rect(c.querySelector('.egCharmEdit')), ap: rect(c.querySelector('.pvApproval')), apBtn: rect(c.querySelector('[data-a="approve"], [data-a="approved"]')),
      sw: !!c.querySelector('.egFaceView, [data-fv]'), strip: !!c.querySelector('.engSet, .engSetWrap, .engSetPc'), small: !!(head && (head.querySelector('.small') || /SMALL\s*·\s*cap/i.test(head.textContent))),
      chips: [...c.querySelectorAll('.reviewIdentity > *')].map(rect).filter(Boolean), headBox: rect(head), centre: j.fit && j.fit.centre.slice(), box: cv._box ? cv._box.centrePx.map(v => v) : null, sc, mask: j.mask && [j.mask.cx, j.mask.cy], size: j.fit && j.fit.size, angle: j.fit && j.fit.angle,
      flipping: !!c.querySelector('.backHost.egFlipping') };
  }, Q);
  /** The point (pt) of the drawing under the screen point (x, y). */
  const ptAt = (x, y) => page.evaluate(([Q, x, y]) => { const cv = document.querySelector(Q + ' .backHost > canvas'), r = cv.getBoundingClientRect(); return cv._map.inv((x - r.left) * cv.width / r.width, (y - r.top) * cv.height / r.height); }, [Q, x, y]);
  const over = (a, b) => !!(a && b && a[0] < b[2] - 0.5 && b[0] < a[2] - 0.5 && a[1] < b[3] - 0.5 && b[1] < a[3] - 0.5);
  const inside = (a, g) => !!(a && g && a[0] >= g[0] - 0.6 && a[1] >= g[1] - 0.6 && a[2] <= g[2] + 0.6 && a[3] <= g[3] + 0.6);
  const same = (a, b) => !!(a && b && a.every((v, i) => Math.abs(v - b[i]) <= 0.5));
  const near = (a, b, e) => Math.abs(a - b) <= e;
  const steps = pxmm => page.evaluate(p => CharmEditor.view.ruleSteps(p), pxmm);
  try {
    const made = await R.build(w, ['sil', 'bar']);
    const sheets = await page.evaluate(() => { const out = []; for (const s of allSheets()) if (s.charms && s.charms.length) { if (!s.folderPath) s.folderPath = 'charmnest/sheets/' + s.sheetId; out.push({ sheetId: s.sheetId, poolIds: s.charms.map(x => x.poolId) }); } return out; });
    for (const s of sheets) st.put('Charm_Nest_Sheets', s.sheetId, { sheetId: s.sheetId, poolIds: s.poolIds, backPool: [] });
    check('0 the world: the bird engraved on its back, the bar on its front, both fitted', made.sil.jobs.length === 1 && made.sil.jobs[0].fitted && made.bar.jobs.length === 1 && made.bar.jobs[0].fitted, JSON.stringify({ sil: made.sil.jobs, bar: made.bar.jobs }));

    // ═══ 1 · no switch on the grid, no picture row, no SMALL · cap ═══
    await R.openEngraveDetail(w, 'sil', { face: 'back' }); await settle(900);
    await page.evaluate(Q => { const c = document.querySelector(Q), j = Engrave.items().get(c.dataset.key); j.fit.small = true; Engrave.render(); }, Q); await settle(700);
    let a = await look();
    check('1 no Front | Back switch on the grid, no row of piece pictures above it, no "SMALL · cap" in the head (a fit flagged small)', a && !a.sw && !a.strip && !a.small, JSON.stringify(a && { sw: a.sw, strip: a.strip, small: a.small }));
    await page.evaluate(Q => { const c = document.querySelector(Q), j = Engrave.items().get(c.dataset.key); j.fit.small = false; Engrave.render(); }, Q); await settle(700);
    a = await look();
    const g0 = a.grid;
    check(`1b the work area opens at the steady fit (zoom 1), with rulers along its top and left and the zoom box inside it (${a.read})`, a.z === 1 && !!a.rx && !!a.ry && inside(a.zb, g0) && inside(a.rx, g0) && inside(a.ry, g0) && /%$/.test(a.read), JSON.stringify(a));

    // ═══ 2 · wheel zoom about the pointer ═══
    const P = [g0[0] + (g0[2] - g0[0]) * 0.62, g0[1] + (g0[3] - g0[1]) * 0.41];
    const p0 = await ptAt(P[0], P[1]);
    await page.mouse.move(P[0], P[1]);
    for (let i = 0; i < 3; i++) { await page.mouse.wheel(0, -100); await settle(120); }
    await settle(300);
    const b = await look(), p1 = await ptAt(P[0], P[1]);
    check(`2 the wheel zooms in about the pointer: ×${(b.z).toFixed(2)}, the point under it stays put (${p0.map(v => v.toFixed(2))} → ${p1.map(v => v.toFixed(2))} pt), the grid did not move`, b.z > 1.3 && near(p0[0], p1[0], 0.25) && near(p0[1], p1[1], 0.25) && same(b.grid, g0) && b.read !== a.read, JSON.stringify({ z: b.z, p0, p1, g: [g0, b.grid], read: [a.read, b.read] }));
    // a ctrl-wheel (a trackpad's pinch) zooms too, about the pointer
    await page.keyboard.down('Control'); await page.mouse.wheel(0, -40); await page.keyboard.up('Control'); await settle(300);
    const b2 = await look(), p2 = await ptAt(P[0], P[1]);
    check(`2b a pinch (ctrl + wheel) zooms about the pointer too (×${b2.z.toFixed(2)})`, b2.z > b.z * 1.2 && near(p0[0], p2[0], 0.3) && near(p0[1], p2[1], 0.3), JSON.stringify({ z: [b.z, b2.z], p0, p2 }));

    // ═══ 3 · the rulers rescale ═══
    const s0 = await steps(a.pxmm), s2 = await steps(b2.pxmm);
    check(`3 the rulers rescale with the zoom: ${a.pxmm.toFixed(2)} → ${b2.pxmm.toFixed(2)} px a mm (the drawing's own scale), numbered every ${s0.major} mm → every ${s2.major} mm`,
      near(a.pxmm, a.k * PT, 0.01) && near(b2.pxmm, b2.k * PT, 0.01) && s2.major < s0.major, JSON.stringify({ a: [a.pxmm, a.k * PT], b: [b2.pxmm, b2.k * PT], s0, s2 }));
    // the zero: the piece's top-left; the ruler's ink at the zero tick (the numbered tick is a full-depth line) and the label "0" (read off the canvas: the tick column is darker than its neighbours)
    const z0 = await page.evaluate(Q => {
      const c = document.querySelector(Q), cv = c.querySelector('.backHost > canvas'), rx = c.querySelector('.egRuleX'), s = cv.getBoundingClientRect().width / cv.width, o = cv._map.tx(cv._zero[0], cv._zero[1]);
      const x = o[0] * s - 16, dpr = rx.width / rx.getBoundingClientRect().width, ctx = rx.getContext('2d'), col = px => { const d = ctx.getImageData(Math.round(px * dpr), Math.round(rx.height * 0.85), 1, 1).data; return d[0] + d[1] + d[2]; };
      return x >= 2 && x <= rx.getBoundingClientRect().width - 2 ? { x, at: col(x + 0.5), off: col(x + 3.5) } : { x, at: null };
    }, Q);
    check('3b the X ruler counts from the piece\'s top-left: its zero tick is drawn where the piece starts', z0.at == null || z0.at < z0.off, JSON.stringify(z0));

    // ═══ 4 · pan: Space-drag, middle button, the Hand; ⌘ keys; ⌘0 ═══
    const pan = async (how, dx, dy) => {
      const before = await look(), q = [g0[0] + 200, g0[1] + 200], pa = await ptAt(q[0], q[1]);
      await page.mouse.move(q[0], q[1]);
      if (how === 'space') { await page.keyboard.down('Space'); await page.mouse.down(); await page.mouse.move(q[0] + dx, q[1] + dy, { steps: 6 }); await page.mouse.up(); await page.keyboard.up('Space'); }
      else if (how === 'middle') { await page.mouse.down({ button: 'middle' }); await page.mouse.move(q[0] + dx, q[1] + dy, { steps: 6 }); await page.mouse.up({ button: 'middle' }); }
      else { await page.click(Q + ' .egZoom [data-ez="hand"]'); await page.mouse.move(q[0], q[1]); await page.mouse.down(); await page.mouse.move(q[0] + dx, q[1] + dy, { steps: 6 }); await page.mouse.up(); await page.click(Q + ' .egZoom [data-ez="hand"]'); }
      await settle(300);
      const after = await look(), pb = await ptAt(q[0] + dx, q[1] + dy);
      return { ok: near(pa[0], pb[0], 0.2) && near(pa[1], pb[1], 0.2) && same(after.grid, g0) && near(after.z, before.z, 1e-9) && JSON.stringify(after.centre) === JSON.stringify(before.centre), pa, pb, words: [before.centre, after.centre] };
    };
    for (const how of ['space', 'middle', 'hand']) { const r = await pan(how, -40, -25); check(`4 ${how === 'space' ? 'Space-drag' : how === 'middle' ? 'the middle button' : 'the Hand'} pans by exactly the hand's travel (the point grabbed stays under it), the words are not moved`, r.ok, JSON.stringify(r)); }
    const cmd = async keys => { await page.mouse.move(g0[0] + 150, g0[1] + 150); await page.keyboard.press(keys); await settle(300); return look(); };
    const pctOf = x => x.k / PX100 * 100;
    const c0 = await look(), up = await cmd('Control+Equal'), down = await cmd('Control+Minus');
    const ladder = await page.evaluate(p => [CharmEditor.view.stepZoom(p, 1)], pctOf(c0));
    check(`4b ⌘ + goes up the editor's ladder (${pctOf(c0).toFixed(0)}% → ${pctOf(up).toFixed(0)}%) and ⌘ − down again (${pctOf(down).toFixed(0)}%)`, near(pctOf(up), ladder[0], 0.6) && pctOf(down) < pctOf(up), JSON.stringify([pctOf(c0), pctOf(up), pctOf(down), ladder]));
    const fit = await cmd('Control+Digit0'), zs = await page.evaluate(Q => { const c = document.querySelector(Q); return Engrave.workZoomOf(Engrave.items().get(c.dataset.key)); }, Q);
    check('4c ⌘ 0 is the steady fit again (zoom 1 about the piece\'s turning point, no zoom kept), the grid where it was', fit.z === 1 && JSON.stringify(fit.c) === JSON.stringify(fit.fit) && zs === null && same(fit.grid, g0) && near(fit.pxmm, a.pxmm, 1e-6), JSON.stringify({ fit, zs }));
    const act = await cmd('Control+Digit1');
    check(`4d ⌘ 1 is actual size (${pctOf(act).toFixed(1)}%)`, near(pctOf(act), 100, 0.6), String(pctOf(act)));
    await page.click(Q + ' .egZoom [data-ez="menu"]'); await settle(150);
    await page.click(Q + ' .egZoom [data-ezz="400"]'); await settle(350);
    const pre = await look();
    check(`4e the zoom box's preset 400% (the editor's presets) and its read-out (${pre.read})`, near(pctOf(pre), 400, 1) && pre.read === '400%', JSON.stringify([pctOf(pre), pre.read]));
    await page.click(Q + ' .egZoom [data-ez="fit"]'); await settle(350);
    const fb = await look();
    check('4f the zoom box\'s Fit is the steady fit', fb.z === 1 && same(fb.grid, g0), JSON.stringify(fb.z));

    // ═══ 5 · the words dragged at 300 % ═══
    await page.evaluate(() => Engrave.workZoomCmd('300')); await settle(400);
    const d0 = await look();
    const bx = await page.evaluate(Q => { const cv = document.querySelector(Q + ' .backHost > canvas'), r = cv.getBoundingClientRect(), s = r.width / cv.width; return { x: r.left + cv._box.centrePx[0] * s, y: r.top + cv._box.centrePx[1] * s, in: cv._box.centrePx[0] > 20 && cv._box.centrePx[1] > 20 && cv._box.centrePx[0] < cv.width - 20 && cv._box.centrePx[1] < cv.height - 20 }; }, Q);
    const DX = 9, DY = 6;
    await page.mouse.move(bx.x, bx.y); await page.mouse.down(); await page.mouse.move(bx.x + DX, bx.y + DY, { steps: 5 }); await page.mouse.up(); await settle(600);
    const d1 = await look();
    const want = [d0.centre[0] + DX / d0.k, d0.centre[1] - DY / d0.k], snap = 0.36 * PT;
    const fits = (v, wv, m) => near(v, wv, 0.05) || (near(wv, m, snap) && near(v, m, 1e-6));   // (within a third of a mm of the charm's centre line the words take it, at every zoom)
    const okx = fits(d1.centre[0], want[0], d0.mask[0]), oky = fits(d1.centre[1], want[1], d0.mask[1]);
    check(`5 at 300% (${pctOf(d0).toFixed(0)}%) the words follow the pointer exactly: moved ${(d1.centre[0] - d0.centre[0]).toFixed(3)}, ${(d1.centre[1] - d0.centre[1]).toFixed(3)} pt for ${DX}, ${DY} px (want ${(DX / d0.k).toFixed(3)}, ${(-DY / d0.k).toFixed(3)}); the size and turn kept, the zoom kept`,
      bx.in && near(pctOf(d0), 300, 1) && okx && oky && d1.size === d0.size && d1.angle === d0.angle && near(d1.z, d0.z, 1e-9), JSON.stringify({ bx, d0: [d0.centre, d0.k], d1: d1.centre, want }));
    await page.evaluate(() => Engrave.workZoomCmd('fit')); await settle(400);

    // ═══ 6 · showFace turns the work area ═══
    const flip = await page.evaluate(async Q => {
      const c = document.querySelector(Q), j = Engrave.items().get(c.dataset.key), host = () => document.querySelector(Q + ' .backHost');
      const seen = { cls: false, rot: false };
      const watch = () => { const h = host(); if (h && h.classList.contains('egFlipping')) seen.cls = true; for (const an of document.getAnimations()) { const t = an.effect && an.effect.target; if (t && t.classList && t.classList.contains('backHost')) { try { if (an.effect.getKeyframes().some(k => /rotateY/.test(k.transform || ''))) seen.rot = true; } catch (_) {} } } };
      const id = setInterval(watch, 16);
      const r = Engrave.showFace(j, 'front');
      watch();
      await new Promise(res => setTimeout(res, 1100));
      clearInterval(id);
      const cv = document.querySelector(Q + ' .backHost > canvas'), again = Engrave.showFace(j, 'front'), cls2 = !!(host() && host().classList.contains('egFlipping'));
      await new Promise(res => setTimeout(res, 400));
      const cv2 = document.querySelector(Q + ' .backHost > canvas');
      Engrave.showFace(j, 'back', { animate: false });
      await new Promise(res => requestAnimationFrame(() => requestAnimationFrame(res)));
      const cv3 = document.querySelector(Q + ' .backHost > canvas');
      return { r, seen, face: cv && cv._viewFace, again, cls2, sameCanvas: cv2 === cv, back: cv3 && !cv3._viewFace, rulers: !!document.querySelector(Q + ' .backHost .egRuleX'), flipping: !!(host() && host().classList.contains('egFlipping')) };
    }, Q);
    check('6 Engrave.showFace(job, "front") turns the work area as the sheets turn (egFlipping, a rotateY turn) and it ends on the front, with its rulers', flip.r.ok && flip.seen.cls && flip.seen.rot && flip.face === 'front' && flip.rulers && !flip.flipping, JSON.stringify(flip));
    check('6b called again for the face shown it does nothing (no turn, the same drawing); animate:false goes back at once', flip.again.same === true && !flip.cls2 && flip.sameCanvas && flip.back, JSON.stringify(flip));

    // ═══ 7 · nothing overlaps at the top, three sizes, both pieces ═══
    for (const [vw, vh] of VIEWS) {
      await page.setViewportSize({ width: vw, height: vh }); await settle(400);
      for (const [name, face] of [['sil', 'back'], ['bar', 'front']]) {
        await R.openEngraveDetail(w, name, { face }); await settle(900);
        const x = await look();
        const okTop = !!x.ce && !over(x.grid, x.ce) && !over(x.grid, x.ap) && !over(x.ce, x.ap) && !over(x.ce, x.apBtn) && !over(x.label, x.ce) && !over(x.label, x.ap);
        const okIn = inside(x.zb, x.grid) && inside(x.rx, x.grid) && inside(x.ry, x.grid) && !over(x.label, x.rx) && !over(x.label, x.ry) && !over(x.label, x.rc) && !over(x.zb, x.ce) && !over(x.zb, x.ap);
        const okHead = x.headBox && x.chips.every(r => r[2] <= x.headBox[2] + 0.5);
        check(`7 ${name} ${vw}×${vh}: Open in Charm Editor, Approve and the grid do not overlap; the rulers and the zoom box are inside the grid, its label clear of them; the head's chips inside the head`, okTop && okIn && okHead && !x.sw && !x.strip && !x.small,
          JSON.stringify({ grid: x.grid, ce: x.ce, ap: x.ap, label: x.label, rx: x.rx, ry: x.ry, zb: x.zb, head: x.headBox, chips: x.chips }));
        await R.shot(w, `ZOOM-${name}-${vw}x${vh}`);
      }
    }
    // a last look, zoomed, for the eye
    await page.setViewportSize({ width: 1440, height: 900 }); await R.openEngraveDetail(w, 'sil', { face: 'back' }); await settle(800);
    await page.evaluate(() => Engrave.workZoomCmd('1200')); await settle(500); await R.shot(w, 'ZOOM-sil-1200');
    const ms = await page.evaluate(Q => { const c = document.querySelector(Q), t = []; for (let i = 0; i < 6; i++) { const t0 = performance.now(); c._remountWork(); t.push(performance.now() - t0); } t.sort((a, b) => a - b); return t[3]; }, Q);
    check(`8 one zoom step draws the work area afresh in ${ms.toFixed(1)} ms (median, at 1200%)`, ms < 120, String(ms));
    await page.evaluate(() => Engrave.workZoomCmd('fit')); await settle(300);
    check('E1 no page error', w.errors.length === 0, JSON.stringify(w.errors).slice(0, 600));
    console.log('screenshots in ' + shots);
  } finally { await w.close(); }
  const bad = RESULTS.filter(r => !r.ok);
  console.log(`\n${RESULTS.length - bad.length}/${RESULTS.length} checks pass`);
  process.exit(bad.length ? 1 : 0);
})().catch(e => { console.error(e); process.exit(1); });
