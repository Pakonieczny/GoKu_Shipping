// THE CHARM EDITOR'S EXTRAS (charm-nest-ce-extras.js; Design & Engrave 8 and 10, Paul 28 Oct 2026: "A lot of the high-end features are missing and I would like you to put all
// the useful ones in ... I'm trying to eliminate the use of the Adobe platform"). No server, no network.
//   node tests/charm-nest/ce-extras.cjs --node-only                       the geometry (N)
//   $SP/int/slot.sh timeout 600 node tests/charm-nest/ce-extras.cjs       the geometry and one real Chromium pass on the editor's harness page (B)
// N1 the blanks fill their box exactly; a star's points are regular (5 outer at one radius, 5 inner at 45 % of it); a rounded corner; a ring's wall; a blank's numbers change it after
// N2 letter spacing adds (letters − 1) × tracking; words along a circle stand on it, centred at its top; flipped they read along its bottom in the same band
// N3 clip to the charm: a hatched bar half off the metal keeps exactly its part on it; a line keeps its length inside; a part wholly off is hidden; ⌥⌘7 gives back the drawing
// N4 the laser check finds a 0.2 mm bridge (and reads its width), leaves a 0.5 mm one alone, finds an open cut line, engraving off the metal, in the jump-ring hole, small letters
// N5 the jump ring on a cut outline is welded into it (one outline with the ring's bump) with its hole a cut of its own; the check's steps stay short on a 1500-point outline
// B  in the editor: the flyout, a star drawn with Shift (one step, the chip and W/H live), the corner on the bar, the jump ring, words round the charm (Flip, edit), letter
//    spacing, ⌘7 / ⌥⌘7, the check's chip and Save's badge and Show, ⌘Y, Alt-drag copies, L and \ — with the main-thread work of each press measured
'use strict';
const path = require('path'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');
const CE = require(path.join(root, 'charm-nest-charm-editor.js')), X = require(path.join(root, 'charm-nest-ce-extras.js'));
const M = CE.model, PT = 72 / 25.4, MM = 25.4 / 72, geo = M.createGeo();
const results = [], failures = [];
const pass = m => { results.push(m); console.log('  ok   ' + m); };
const fail = (m, e) => { failures.push(m + ' :: ' + String(e && e.message || e).split('\n')[0].slice(0, 400)); console.log('  FAIL ' + m + '\n         ' + String(e && e.stack || e).split('\n').slice(0, 10).join('\n         ')); };
function check(name, fn) { try { fn(); pass(name); } catch (e) { fail(name, e); } }
async function step(name, fn) { try { await fn(); pass(name); } catch (e) { fail(name, e); } }
const near = (a, b, eps) => Math.abs(a - b) <= eps;
const P = (x, y) => [x * PT, y * PT];
const rect = (x0, y0, x1, y1) => [[['m', P(x0, y0)], ['l', P(x1, y0)], ['l', P(x1, y1)], ['l', P(x0, y1)], ['h']]];
const circ = (cx, cy, r) => M.ellipseSP(cx * PT, cy * PT, r * PT, r * PT);
const box = sps => M.bboxOf(sps).map(v => v * MM);
const area = sps => M.inkArea(sps) * MM * MM;
const ptsOf = sps => { const o = []; for (const sp of sps) for (const op of sp) for (const q of op.slice(1)) if (Array.isArray(q)) o.push(q); return o; };

console.log('ce-extras · N (node)');
/* ── N1 the blanks ── */
check('N1 a star of 5 points at its own proportions is regular: 5 outer points at one radius, 5 notches at 45 % of it; its tight box is the box drawn', () => {
  const u = X.starUnit(5, 0.45), ub = M.bboxOf(u), asp = (ub[2] - ub[0]) / (ub[3] - ub[1]);
  const it = X.shapeItem('star', [0, 0, 10 * asp * PT, 10 * PT], { points: 5, inner: 45 });
  const k = 10 * PT / (ub[3] - ub[1]), o = [-ub[0] * k, -ub[1] * k], pts = ptsOf(it.subpaths).map(q => Math.hypot(q[0] - o[0], q[1] - o[1]));
  const outer = pts.filter((_, i) => i % 2 === 0), inner = pts.filter((_, i) => i % 2 === 1);
  assert.equal(pts.length, 10);
  assert.ok(outer.every(r => near(r, k, 1e-6)) && inner.every(r => near(r, 0.45 * k, 1e-6)), JSON.stringify(pts.map(v => +(v * MM).toFixed(4))));
  assert.ok(box(it.subpaths).every((v, i) => near(v, [0, 0, 10 * asp, 10][i], 1e-6)));
  console.log(`       star: outer radius ${(k * MM).toFixed(3)} mm × 5, inner ${(0.45 * k * MM).toFixed(3)} mm × 5, aspect ${asp.toFixed(4)}`);
});
check('N1b a rounded rectangle, a heart and a ring fill their box exactly; the corner is 1.5 mm, the ring\'s hole 1 mm in; a new corner on the bar redraws it in its own box', () => {
  const b = [2 * PT, 3 * PT, 12 * PT, 9 * PT];
  const rr = X.shapeItem('rrect', b, { corner: 1.5 }), he = X.shapeItem('heart', b, {}), ri = X.shapeItem('ring', b, { wall: 1 });
  for (const it of [rr, he, ri]) assert.ok(box(it.subpaths).every((v, i) => near(v, b[i] * MM, 1e-6)), it.name + ' ' + box(it.subpaths));
  assert.ok(near(rr.subpaths[0][0][1][0] * MM, 3.5, 1e-9), 'the top edge starts 1.5 mm in');
  assert.equal(ri.subpaths.length, 2); assert.equal(ri.role, 'cut'); assert.ok(ri.evenOdd);
  assert.ok(box([ri.subpaths[1]]).every((v, i) => near(v, [3, 4, 11, 8][i], 1e-6)), 'the hole 1 mm in');
  const moved = Object.assign({}, rr, { tx: 5, sx: 2, sy: 2 });
  const r2 = X.reshape(moved, { corner: 1 }, geo);
  assert.ok(r2 && near(r2.subpaths[0][0][1][0] * MM, 2 + 0.5, 1e-9), 'corner 1 mm on the charm = 0.5 mm in its own drawing at 200 %');
  assert.equal(X.shapeOf(r2).corner, 1); assert.equal(X.shapeOf(Object.assign({}, rr, { subpaths: rect(0, 0, 1, 1) })), null, 'a blank redrawn by other means keeps no numbers');
});
/* ── N2 text ── */
check('N2 letter spacing: 200/1000 em on "ABC" at 10 pt adds exactly 2 × 2 pt; centred words stay centred', () => {
  const t = { id: 't', kind: 'text', role: 'hatch', text: 'ABC', fontKey: 'sans', sizePt: 10, align: 'left' };
  const b0 = geo.raw(t).bbox, b1 = geo.raw(Object.assign({}, t, { tracking: 200 })).bbox;
  assert.ok(near((b1[2] - b1[0]) - (b0[2] - b0[0]), 4, 1e-9), `${b0} -> ${b1}`);
  const c0 = geo.raw(Object.assign({}, t, { align: 'center' })).bbox, c1 = geo.raw(Object.assign({}, t, { align: 'center', tracking: -100 })).bbox;
  assert.ok(near(c0[0] + c0[2], c1[0] + c1[2], 1e-9), 'the middle stays');
  console.log(`       ABC: ${((b0[2] - b0[0]) * MM).toFixed(3)} mm → ${((b1[2] - b1[0]) * MM).toFixed(3)} mm at +200`);
});
check('N2b words round a 20 mm circle stand on it (baseline on the circle, letters outward), centred at its top; Flip takes them to the bottom in the same band, reading on', () => {
  const t = { id: 'w', kind: 'text', role: 'hatch', text: 'LOVE YOU', fontKey: 'sans', sizePt: 8, align: 'left', tx: 30, ty: 30 };
  const c = [40 * PT, 40 * PT], R = 10 * PT, n = X.onPathItem(t, M.ellipseSP(c[0], c[1], R, R), M.I, {});
  assert.ok(n.pathD.length < 200, 'a circle\'s path is a short string (a design keeps it): ' + n.pathD.length);
  const cap = X.capOf(geo.engine, 'sans', 8), placed = it => M.transformSubpaths(geo.raw(it).subpaths, geo.matrix(it));
  const radii = it => ptsOf(placed(it)).map(q => Math.hypot(q[0] - c[0], q[1] - c[1]));
  const r1 = radii(n), b1 = M.bboxOf(placed(n)), hw = 0.46 * 8 * 0.7 / 6;   // (the stand-in font's bars are this thick either side of their line)
  assert.ok(Math.min(...r1) > R - 2 * hw && Math.max(...r1) < R + cap + 2 * hw, `${Math.min(...r1)} .. ${Math.max(...r1)} for R ${R}, cap ${cap}`);
  assert.ok(near((b1[0] + b1[2]) / 2, c[0], 0.05) && b1[3] < c[1] - R * 0.5, 'centred on the top: ' + b1);
  const top = X.guideOf(n.pathD).fwd.top(), bottom = X.guideOf(n.pathD).fwd.bottom();
  const fp = X.flipPatch(n, geo), f = M.retext(n, fp, geo), r2 = radii(f), b2 = M.bboxOf(placed(f));
  assert.ok(fp.pathFlip && near(fp.pathAt, bottom, 1e-4), 'Flip on a circle\'s top goes to its bottom');
  assert.ok(near(Math.min(...r2), Math.min(...r1), 0.1) && near(Math.max(...r2), Math.max(...r1), 0.1) && b2[1] > c[1] + R * 0.5 && near((b2[0] + b2[2]) / 2, c[0], 0.3), 'the same band, at the bottom: ' + b2 + ' ' + Math.min(...r2) + '..' + Math.max(...r2));
  // reading on: the first letter (L) is left of the last (U) at the bottom too
  const first = ptsOf(M.transformSubpaths(geo.raw(Object.assign({}, f, { text: 'L' })).subpaths, geo.matrix(f))), last = ptsOf(M.transformSubpaths(geo.raw(Object.assign({}, f, { text: 'LOVE YOU' })).subpaths, geo.matrix(f)));
  assert.ok(Math.min(...first.map(q => q[0])) < Math.max(...last.map(q => q[0])));
  const off = X.offPathItems(n, geo);
  assert.equal(off.text.pathD, undefined); assert.equal(off.path, null, 'a circle made for the words is not given back as a part');
  console.log(`       round: radii ${(Math.min(...r1) * MM).toFixed(2)}–${(Math.max(...r1) * MM).toFixed(2)} mm on a ${(R * MM).toFixed(1)} mm circle, top ${(top).toFixed(3)}, bottom ${bottom.toFixed(3)} of the way round`);
});
/* ── N3 clip ── */
const plate = () => [
  { id: 'out', kind: 'path', role: 'cut', source: 'master', name: 'Outline', stroke: true, subpaths: rect(0, 0, 10, 6) },
  { id: 'bar', kind: 'path', role: 'hatch', fill: true, name: 'Bar', subpaths: rect(-2, 2, 4, 4) },
  { id: 'ln', kind: 'path', role: 'line', stroke: true, name: 'Line', subpaths: [[['m', P(8, 3)], ['l', P(13, 3)]]] },
  { id: 'far', kind: 'path', role: 'hatch', fill: true, name: 'Far', subpaths: rect(12, 1, 13, 2) },
  { id: 'in', kind: 'path', role: 'hatch', fill: true, name: 'Inside', subpaths: rect(1, 1, 2, 2) }
];
check('N3 ⌘7: a 6 × 2 mm bar 2 mm off the metal keeps exactly its 4 × 2 mm on it; a 5 mm line keeps its 2 mm inside; a part wholly off is hidden; one inside is untouched', () => {
  const items = plate(), plan = X.clipPlan(items, geo, []);
  assert.deepEqual([...plan.map.keys()].sort(), ['bar', 'far', 'ln']);
  const bar = plan.map.get('bar'), ln = plan.map.get('ln'), far = plan.map.get('far');
  assert.ok(near(area(bar.subpaths), 8, 1e-3), 'area ' + area(bar.subpaths)); assert.ok(box(bar.subpaths).every((v, i) => near(v, [0, 2, 4, 4][i], 1e-4)));
  const L = ptsOf(ln.subpaths); assert.ok(near(Math.hypot(L[1][0] - L[0][0], L[1][1] - L[0][1]) * MM, 2, 1e-4) && ln.stroke && !ln.fill);
  assert.ok(far.hidden && far.clipOf, 'wholly off: hidden, not engraved');
  assert.ok(bar.clipOf && bar.clipOf.subpaths === items[1].subpaths);
  // given back, as it has been moved since
  const after = M.replaceIn(items, new Map([['bar', Object.assign({}, bar, { tx: 3 * PT })]]));
  const back = X.releasePlan(after, geo, ['bar']).get('bar');
  assert.ok(near(area(M.outlinesOf(back, geo)), 12, 1e-3) && box(M.outlinesOf(back, geo)).every((v, i) => near(v, [1, 2, 7, 4][i], 1e-6)), 'the whole bar, 3 mm along: ' + box(M.outlinesOf(back, geo)));
  console.log(`       bar ${area(items[1].subpaths).toFixed(3)} → ${area(bar.subpaths).toFixed(3)} mm²; line 5 → ${(Math.hypot(L[1][0] - L[0][0], L[1][1] - L[0][1]) * MM).toFixed(3)} mm`);
});
check('N3b with two things selected and the top one a closed path, the other is clipped to that path (Illustrator\'s mask)', () => {
  const items = plate().concat([{ id: 'mask', kind: 'path', role: 'line', stroke: true, subpaths: circ(1, 3, 1) }]);
  const plan = X.clipPlan(items, geo, ['bar', 'mask']);
  assert.equal(plan.to, 'path'); assert.deepEqual([...plan.map.keys()], ['bar']);
  assert.ok(near(area(plan.map.get('bar').subpaths), Math.PI, 0.01), 'the disc: ' + area(plan.map.get('bar').subpaths));
});
/* ── N4 the check ── */
const holes = gap => [
  { id: 'o', kind: 'path', role: 'cut', subpaths: rect(0, 0, 10, 6) },
  { id: 'h1', kind: 'path', role: 'cut', subpaths: rect(1, 1, 5 - gap / 2, 5) },
  { id: 'h2', kind: 'path', role: 'cut', subpaths: rect(5 + gap / 2, 1, 9, 5) }
];
check('N4 the laser check finds a 0.2 mm bridge between two cut-outs and reads it 0.2 mm wide; a 0.5 mm bridge passes', () => {
  const a = X.checkFace(holes(0.2), geo, {}), b = X.checkFace(holes(0.5), geo, {});
  const br = a.filter(p => p.kind === 'bridge');
  assert.equal(br.length, 1, JSON.stringify(a)); assert.ok(/0\.2 mm wide/.test(br[0].text), br[0].text);
  assert.ok(near((br[0].box[0] + br[0].box[2]) / 2 * MM, 5, 0.05), 'where it is');
  assert.deepEqual(b, [], JSON.stringify(b));
  console.log(`       "${br[0].text}" at x ${((br[0].box[0] + br[0].box[2]) / 2 * MM).toFixed(2)} mm; the 0.5 mm bridge: ${b.length} problems`);
});
check('N4b it finds a cut line not closed, engraving off the metal and in the jump-ring hole, letters under 1.6 mm; a clean charm has nothing', () => {
  const items = [
    { id: 'o', kind: 'path', role: 'cut', subpaths: circ(8, 8, 8) },
    { id: 'ring', kind: 'path', role: 'cut', subpaths: circ(8, 2, 0.65) },
    { id: 'gap', kind: 'path', role: 'cut', subpaths: [[['m', P(3, 8)], ['l', P(5, 8)], ['l', P(5, 10)]]] },
    { id: 'off', kind: 'path', role: 'hatch', fill: true, name: 'Leaf', subpaths: rect(14, 7, 17, 9) },
    { id: 'inring', kind: 'path', role: 'line', stroke: true, name: 'Dash', subpaths: [[['m', P(7, 2)], ['l', P(9, 2)]]] },
    { id: 'tiny', kind: 'text', role: 'hatch', text: 'TINY', fontKey: 'sans', sizePt: 3, tx: 6 * PT, ty: 12 * PT },
    { id: 'ok', kind: 'text', role: 'hatch', text: 'OK', fontKey: 'sans', sizePt: 9, tx: 6 * PT, ty: 9 * PT }
  ];
  const ps = X.checkFace(items, geo, {}), kinds = ps.map(p => p.kind + ':' + p.ids[0]);
  assert.deepEqual(kinds, ['open:gap', 'outside:off', 'hole:inring', 'small:tiny'], JSON.stringify(ps.map(p => p.text)));
  assert.ok(ps.every(p => p.box && p.box.every(Number.isFinite)), 'every problem has a spot to show');
  assert.deepEqual(X.checkFace(items.filter(i => ['o', 'ring', 'ok'].includes(i.id)), geo, {}), []);
  console.log('       ' + ps.map(p => p.text).join(' | '));
});
/* ── N5 jump ring, speed ── */
check('N5 the check of a 1500-point outline and 12 words runs in short steps (CPU time, after a first run warms it up): the middle step a few ms, none near the 50 ms limit', () => {
  const pts = []; for (let i = 0; i < 1500; i++) { const a = i / 1500 * 2 * Math.PI, r = 12 + 0.6 * Math.sin(a * 23); pts.push(P(12.5 + r * Math.cos(a), 12.5 + r * Math.sin(a))); }
  const items = [{ id: 'o', kind: 'path', role: 'cut', subpaths: [[['m', pts[0]]].concat(pts.slice(1).map(p => ['l', p]), [['h']])] }, { id: 'h', kind: 'path', role: 'cut', subpaths: circ(12.5, 1.6, 0.65) }];
  for (let i = 0; i < 12; i++) items.push({ id: 'w' + i, kind: 'text', role: 'hatch', text: 'WORDS ' + i, fontKey: 'sans', sizePt: 7, tx: (5 + (i % 3) * 6) * PT, ty: (7 + Math.floor(i / 3) * 4) * PT });
  const cpu = () => { const u = process.cpuUsage(); return (u.user + u.system) / 1000; };
  let ts = [];
  for (let run = 0; run < 3; run++) {
    const g = X.checkSteps('front', items, geo, {}, {}); let r, t = cpu(); ts = [];
    do { r = g.next(); const t2 = cpu(); ts.push(t2 - t); t = t2; } while (!r.done);
  }
  const sorted = ts.slice().sort((a, b) => a - b), med = sorted[sorted.length >> 1], worst = sorted[sorted.length - 1];
  console.log(`       check: ${ts.length} steps, middle ${med.toFixed(1)} ms, longest ${worst.toFixed(1)} ms, all ${ts.reduce((a, b) => a + b, 0).toFixed(1)} ms of CPU`);
  assert.ok(med < 8 && worst < 40, 'steps ' + ts.map(v => v.toFixed(1)).join(' '));
});

/* ════ B · the editor in Chromium ════ */
(async () => {
  if (process.argv.includes('--node-only')) return done();
  const pwDir = process.env.PW_DIR || path.join(root, 'node_modules');
  let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { console.log('  - no playwright-core: the browser checks were not run'); return done(); }
  console.log('ce-extras · B (Chromium)');
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1243/chrome-linux64/chrome', args: ['--no-sandbox'] });
  const ctx0 = await browser.newContext({ viewport: { width: 1280, height: 760 }, reducedMotion: 'reduce' });
  await ctx0.route(u => !u.href.startsWith('file://'), r => r.abort());
  const page = await ctx0.newPage(), errs = [];
  page.on('pageerror', e => errs.push('pageerror: ' + e.message));
  page.on('console', m => { if (m.type() === 'error') errs.push('console: ' + m.text()); });
  await page.goto('file://' + path.join(__dirname, 'fixtures', 'charm-editor-harness.html'));
  await page.evaluate(() => { window.__log = []; window.CNDesignLog = { event: (k, d) => window.__log.push([k, d]) }; });
  await page.addScriptTag({ path: path.join(root, 'charm-nest-vector.js') });
  await page.addScriptTag({ path: path.join(root, 'charm-nest-ce-extras.js') });
  const settle = () => page.evaluate(() => new Promise(r => requestAnimationFrame(() => requestAnimationFrame(r))));
  const H = (fn, arg) => page.evaluate(fn, arg);
  const C = 'window.CharmEditor.current';
  const cur = expr => page.evaluate(`(() => { const C = ${C}; return ${expr}; })()`);
  const ptClient = (x, y) => H(([x, y]) => window.CharmEditor.current.clientOf(x, y), [x, y]);
  const mm = (x, y) => H(([x, y]) => [window.fixture.X0 + x * window.fixture.PT, window.fixture.Y0 + y * window.fixture.PT], [x, y]);
  const mmClient = async (x, y) => { const p = await mm(x, y); return ptClient(p[0], p[1]); };
  const item = id => H(id => { let f = null; const w = l => (l || []).forEach(i => { if (i.id === id) f = i; w(i.children); }); w(window.CharmEditor.current.items()); return f; }, id);
  const boxOf = id => H(id => window.CharmEditor.current.box(id), id);
  const key = async k => { await page.keyboard.press(k); await settle(); };
  /** The main-thread work of one press: the event dispatched in the page and the layout it asks for, in ms. */
  const workOf = (type, init, on) => H(([type, init, on]) => {
    const st = document.querySelector('.ce-stage'), t0 = performance.now();
    const ev = type.startsWith('key') ? new KeyboardEvent(type, Object.assign({ bubbles: true, cancelable: true }, init)) : new PointerEvent(type, Object.assign({ bubbles: true, cancelable: true, pointerId: 1, pointerType: 'mouse', isPrimary: true }, init));
    (on ? document.querySelector(on) : type.startsWith('key') ? document.querySelector('.ce-win') : st).dispatchEvent(ev); document.body.getBoundingClientRect(); void st.offsetWidth;
    return performance.now() - t0;
  }, [type, init, on || null]);
  const work = [];
  await page.evaluate(() => { window.openEditor(); }); await page.waitForSelector('.ce-root .ce-items path'); await settle(); await settle();

  await step('B1 the Shapes flyout: the editor\'s rectangle, ellipse, line, polygon, then rounded rectangle, star, heart, ring, jump ring; L arms the ellipse, \\ the line', async () => {
    const t = await H(() => [...document.querySelectorAll('[data-drop="shapes"] .ce-menu [data-tool]')].map(b => b.dataset.tool));
    assert.deepEqual(t, ['rect', 'ellipse', 'line', 'polygon', 'shape-rrect', 'shape-star', 'shape-heart', 'shape-ring', 'shape-bail']);
    await key('l'); assert.equal(await cur('C.tool()'), 'ellipse'); await key('\\'); assert.equal(await cur('C.tool()'), 'line'); await key('v');
  });
  await step('B2 a star dragged with Shift: one item, one undo step, regular (its own proportions), the size chip and the bar\'s W / H follow the drag; a move is under a frame of work', async () => {
    await H(() => window.CharmEditor.current.ctx.setTool('shape-star')); await settle();
    assert.ok(await H(() => !document.querySelector('.cx-bar').hidden && !document.querySelector('[data-cx-f="points"]').hidden), 'the star\'s numbers on the bar');
    const n0 = (await cur('C.items()')).length, h0 = (await cur('C.history()')).undo;
    const a = await mmClient(3, 12), b = await mmClient(9, 15);
    await page.keyboard.down('Shift'); await page.mouse.move(a.x, a.y); await page.mouse.down(); await page.mouse.move(b.x, b.y, { steps: 8 }); await settle();
    const live = await H(() => ({ chip: [...document.querySelectorAll('.ce-ui .u-dim text')].map(t => t.textContent).join('|'), w: document.querySelector('[data-f="w"]').value, h: document.querySelector('[data-f="h"]').value }));
    for (let i = 0; i < 6; i++) work.push(['star move', await workOf('pointermove', { clientX: b.x + i, clientY: b.y + i, buttons: 1, shiftKey: true })]);
    await page.mouse.up(); await page.keyboard.up('Shift'); await settle();
    const its = await cur('C.items()'), st = its[its.length - 1], bb = await boxOf(st.id);
    assert.equal(its.length, n0 + 1); assert.equal((await cur('C.history()')).undo, h0 + 1); assert.equal(await cur('C.tool()'), 'select');
    assert.equal(st.shape.kind, 'star'); assert.equal(st.subpaths[0].length, 11);
    const u = await H(() => { const B = window.CharmEditor.model.bboxOf(window.CharmNestCeExtras.starUnit(5, 0.45)); return (B[2] - B[0]) / (B[3] - B[1]); });
    assert.ok(near((bb[2] - bb[0]) / (bb[3] - bb[1]), u, 1e-6), 'regular: ' + (bb[2] - bb[0]) / (bb[3] - bb[1]));
    assert.ok(/\d+\.\d\d × \d+\.\d\d mm/.test(live.chip) && live.w && live.h && live.chip.includes(live.w), 'chip and fields during the drag: ' + JSON.stringify(live));
    assert.ok(await H(l => window.__log.some(e => e[0] === 'shapePreset' && e[1].kind === 'star')));
    console.log('       chip ' + JSON.stringify(live));
  });
  await step('B3 a rounded rectangle drawn, then its corner typed on the bar (2 mm): redrawn in its box, one more step', async () => {
    await H(() => window.CharmEditor.current.ctx.setTool('shape-rrect')); await settle();
    const a = await mmClient(12, 12), b = await mmClient(20, 16);
    await page.mouse.move(a.x, a.y); await page.mouse.down(); await page.mouse.move(b.x, b.y, { steps: 5 }); await page.mouse.up(); await settle();
    const it0 = (await cur('C.items()')).slice(-1)[0], b0 = await boxOf(it0.id), h0 = (await cur('C.history()')).undo;
    assert.ok(await H(() => !document.querySelector('[data-cx-f="corner"]').hidden), 'the corner on the bar for the selected blank');
    await page.click('[data-cx="corner"]'); await page.keyboard.press('Control+a'); await page.keyboard.type('2'); await page.keyboard.press('Enter'); await settle();
    const it1 = await item(it0.id), b1 = await boxOf(it0.id);
    assert.ok(near(it1.subpaths[0][0][1][0] - b1[0], 2 * 72 / 25.4, 1e-4), 'corner 2 mm: ' + (it1.subpaths[0][0][1][0] - b1[0]));
    assert.ok(b1.every((v, i) => near(v, b0[i], 1e-6))); assert.equal((await cur('C.history()')).undo, h0 + 1);
  });
  await step('B4 the jump ring: on the filled heart it sits on the heart\'s top as a cut ring; on the unlocked outline it is welded in (the outline higher by the ring) with its hole a cut', async () => {
    await H(() => window.CharmEditor.current.ctx.select(['heart-fill'])); await settle();
    const hb = await boxOf('heart-fill');
    await H(() => window.CharmEditor.current.ctx.setTool('shape-bail')); await settle(); await settle();
    const r = (await cur('C.items()')).slice(-1)[0], rb = await boxOf(r.id);
    assert.equal(r.role, 'cut'); assert.ok(r.evenOdd); assert.equal(await cur('C.tool()'), 'select');
    assert.ok(near(rb[2] - rb[0], 6.5, 1e-6) && near((rb[0] + rb[2]) / 2, (hb[0] + hb[2]) / 2, 1e-6) && rb[3] > hb[1] - 0.01, 'the ring on the heart\'s middle: ' + rb + ' / ' + hb);
    await H(() => document.querySelector('[data-cmd="unlockAll"]').click()); await settle();
    await H(() => window.CharmEditor.current.ctx.select(['cut-outline'])); await settle();
    const ob = await boxOf('cut-outline'), n0 = (await cur('C.items()')).length;
    await H(() => document.querySelector('[data-drop="xcmd"] .ce-drop__b').click()); await settle();
    await H(() => document.querySelector('[data-drop="xcmd"] [data-xcmd="cx.jumpRing"]').click()); await settle();
    const ob2 = await boxOf('cut-outline'), its = await cur('C.items()'), hole = its.find(i => i.name === 'Jump-ring hole');
    assert.equal(its.length, n0 + 1); assert.ok(hole && hole.role === 'cut');
    assert.ok(near(ob[1] - ob2[1], 6.5 / 2 + (6.5 + 3.7) / 4, 0.02) && near(ob2[3], ob[3], 1e-3), 'the outline grew up by the ring (its centre a wall\'s middle above the edge): ' + (ob[1] - ob2[1]));
    await key('Control+z'); await key('Control+z');
    assert.ok((await boxOf('cut-outline')).every((v, i) => near(v, ob[i], 1e-6)), 'undone');
  });
  await step('B5 words round the charm: a text alone selected goes round it (inside its edge, centred on top); Along, Rise and Flip on the bar; Flip takes it to the bottom; double-click edits the words, still round', async () => {
    const fr = await cur('C.frame()');
    const ids = await H(fr => window.CharmEditor.current.ctx.addItems([{ kind: 'text', role: 'hatch', text: 'SUMMER', fontKey: 'sans', sizePt: 7, align: 'center', tx: fr.x0 + fr.wPt / 2, ty: fr.y0 + fr.hPt / 2 }]), fr);
    await H(() => window.CharmEditor.current.ctx.commit()); await settle();
    const h0 = (await cur('C.history()')).undo;
    await H(() => document.querySelector('[data-drop="xcmd"] .ce-drop__b').click()); await settle();
    await H(() => document.querySelector('[data-drop="xcmd"] [data-xcmd="cx.onPath"]').click()); await settle(); await settle();
    const t = await item(ids[0]), b = await boxOf(ids[0]);
    assert.ok(t.pathD && t.kind === 'text'); assert.equal((await cur('C.history()')).undo, h0 + 1);
    // (round the charm: a circle 0.8 mm in from the charm's frame on its shorter side, here 24 of 24 × 30 mm, so the words' top is 0.8 mm under the circle's top)
    const want = fr.y0 + fr.hPt / 2 - Math.min(fr.wPt, fr.hPt) / 2 + 0.8 * 72 / 25.4;
    assert.ok(near(b[1], want, 0.25 * 72 / 25.4) && near((b[0] + b[2]) / 2, fr.x0 + fr.wPt / 2, 0.3), 'on top, 0.8 mm in: ' + (b[1] - want) * 25.4 / 72);
    const bar = await H(() => ['along', 'rise', 'track'].map(f => !document.querySelector(`[data-cx-f="${f}"]`).hidden).concat(!document.querySelector('[data-cx-act="flip"]').hidden));
    assert.deepEqual(bar, [true, true, true, true]);
    assert.ok(await H(() => !!document.querySelector('.ce-ui .cx-guide')), 'its path drawn while selected');
    await page.click('[data-cx-act="flip"]'); await settle();
    const b2 = await boxOf(ids[0]);
    assert.ok(b2[3] > fr.y0 + fr.hPt * 0.8 && near((b2[0] + b2[2]) / 2, fr.x0 + fr.wPt / 2, 0.3), 'at the bottom: ' + b2);
    // a click on its letters takes it (not the empty middle of the circle)
    const mid = await ptClient(fr.x0 + fr.wPt / 2, fr.y0 + fr.hPt / 2 - 6 * 72 / 25.4);
    await page.mouse.click(mid.x, mid.y); await settle();
    assert.ok(!(await cur('C.selection()')).includes(ids[0]), 'the middle of the circle is not the words');
    const c = await ptClient((b2[0] + b2[2]) / 2, (b2[1] + b2[3]) / 2);
    await page.mouse.dblclick(c.x, c.y); await settle();
    assert.equal(await H(() => document.querySelector('.ce-editor') && document.querySelector('.ce-editor').value), 'SUMMER');
    await page.keyboard.press('Control+a'); await page.keyboard.type('WINTER 2026'); await page.keyboard.press('Enter'); await settle();
    const t3 = await item(ids[0]), b3 = await boxOf(ids[0]);
    assert.equal(t3.text, 'WINTER 2026'); assert.ok(t3.pathD && t3.pathFlip && b3[3] > fr.y0 + fr.hPt * 0.8, 'still round the bottom');
    const out = await cur('C.result()'), o = out.faces.front.items.find(i => i.id === ids[0]);
    assert.ok(o.subpaths.length > 10 && o.pathD, 'outlines in the result, its path with it');
  });
  await step('B6 letter spacing on the bar: +100 on the words makes them wider by 9 × 0.1 em (one step)', async () => {
    const id = (await cur('C.items()')).find(i => i.text === 'WINTER 2026').id;
    await H(id => window.CharmEditor.current.ctx.select([id]), id); await settle();
    const t0 = await item(id), w0 = await H(t => { const g = window.CharmEditor.current.ctx.geo, b = g.raw(Object.assign({}, t, { pathD: undefined })).bbox; return b[2] - b[0]; }, t0);
    await page.click('[data-cx="track"]'); await page.keyboard.press('Control+a'); await page.keyboard.type('100'); await page.keyboard.press('Enter'); await settle();
    const t1 = await item(id), w1 = await H(t => { const g = window.CharmEditor.current.ctx.geo, b = g.raw(Object.assign({}, t, { pathD: undefined })).bbox; return b[2] - b[0]; }, t1);
    assert.equal(t1.tracking, 100); assert.ok(near(w1 - w0, 10 * 0.1 * t1.sizePt, 1e-6), `${w0} -> ${w1}`);
    await page.focus('[data-cx="track"]');
    for (let i = 0; i < 3; i++) work.push(['letter spacing ↑ (the words laid out again)', await workOf('keydown', { key: 'ArrowUp' }, '[data-cx="track"]')]);
    await H(() => document.querySelector('[data-cx="track"]').blur()); await settle();
    assert.equal((await item(id)).tracking, 130, 'three steps of 10');
  });
  await step('B7 ⌘7 clips a bar that runs off the charm to the cut line (one step, its outside gone); ⌥⌘7 gives it back whole; a press of either is under a frame of work', async () => {
    const fr = await cur('C.frame()'), P2 = 72 / 25.4;
    const id = (await H(fr => window.CharmEditor.current.ctx.addItems([{ kind: 'path', role: 'hatch', fill: true, name: 'Band', subpaths: [[['m', [fr.x0 - 3 * 72 / 25.4, fr.y0 + 15 * 72 / 25.4]], ['l', [fr.x0 + 8 * 72 / 25.4, fr.y0 + 15 * 72 / 25.4]], ['l', [fr.x0 + 8 * 72 / 25.4, fr.y0 + 17 * 72 / 25.4]], ['l', [fr.x0 - 3 * 72 / 25.4, fr.y0 + 17 * 72 / 25.4]], ['h']]] }]), fr))[0];
    await H(() => window.CharmEditor.current.ctx.commit()); await H(() => window.CharmEditor.current.ctx.select([])); await settle();
    for (let k = 0; k < 3; k++) {
      const h0 = (await cur('C.history()')).undo;
      work.push(['⌘7 clip', await workOf('keydown', { key: '7', code: 'Digit7', ctrlKey: true })]);
      await page.waitForFunction(id => { let f = null; window.CharmEditor.current.items().forEach(i => { if (i.id === id) f = i; }); return f && f.clipOf; }, id);
      await settle();
      const b = await boxOf(id);
      assert.ok(near(b[0], fr.x0 + 1 * P2, 0.05) && near(b[2], fr.x0 + 8 * P2, 1e-3), 'clipped to the outline (1 mm in): ' + ((b[0] - fr.x0) / P2));
      assert.equal((await cur('C.history()')).undo, h0 + 1);
      work.push(['⌥⌘7 release', await workOf('keydown', { key: '7', code: 'Digit7', ctrlKey: true, altKey: true })]); await settle();
      const b2 = await boxOf(id), it = await item(id);
      assert.ok(near(b2[0], fr.x0 - 3 * P2, 1e-6) && !it.clipOf, 'whole again: ' + ((b2[0] - fr.x0) / P2));
    }
  });
  await step('B8 the laser check: the bar off the charm and a hole 0.2 mm from the edge are found by themselves; the chip and Save say how many; Show selects the part and zooms to it; Save still saves at once', async () => {
    const fr = await cur('C.frame()');
    await H(fr => { const c = window.CharmEditor.current.ctx, P = 72 / 25.4, cx = fr.x0 + 1.2 * P + 1 * P, cy = fr.y0 + 20 * P;
      c.addItems([{ kind: 'path', role: 'cut', name: 'Near hole', stroke: true, subpaths: window.CharmEditor.model.ellipseSP(cx, cy, 1 * P, 1 * P) }], { select: false }); c.commit(); }, fr);
    await page.waitForFunction(() => { const c = document.querySelector('.cx-chk'); return c && !c.hidden && /\d+ to check/.test(c.textContent) && +c.textContent.match(/\d+/)[0] >= 2; }, null, { timeout: 8000 });
    const s = await H(() => ({ chip: document.querySelector('.cx-chk').textContent, badge: document.querySelector('[data-act="save"] .cx-badge').textContent, hidden: document.querySelector('[data-act="save"] .cx-badge').hidden }));
    assert.ok(!s.hidden && s.badge === s.chip, JSON.stringify(s));
    await page.click('.cx-chk'); await settle();
    const rows = await H(() => [...document.querySelectorAll('.cx-list .cx-row')].map(r => r.textContent));
    assert.ok(rows.some(r => /Bridge 0\.\d+ mm wide/.test(r)) && rows.some(r => /Band runs past the cut line/.test(r)), rows.join(' | '));
    const z0 = await cur('C.zoom()'), i = await H(() => [...document.querySelectorAll('.cx-list .cx-row')].findIndex(r => /Band/.test(r.textContent)));
    await page.click(`.cx-list [data-cx-show="${i}"]`); await settle();
    const sel = await cur('C.selection()'), band = (await cur('C.items()')).find(x => x.name === 'Band');
    assert.deepEqual(sel, [band.id]); assert.ok((await cur('C.zoom()')) > z0 * 1.5, 'zoomed in'); assert.ok(await H(() => !!document.querySelector('.ce-ui .cx-mark')), 'the spot marked');
    console.log('       ' + s.chip + ' · ' + rows.length + ' rows: ' + rows.map(r => r.replace(/Show$/, '')).join(' | ').slice(0, 300));
  });
  await step('B9 ⌘Y Outline view: every part a hairline, no fill; ⌘Y again: as engraved; ⇧⌘Z still redoes', async () => {
    for (let k = 0; k < 2; k++) { work.push(['⌘Y outline', await workOf('keydown', { key: 'y', code: 'KeyY', ctrlKey: true })]); await settle(); work.push(['⌘Y outline', await workOf('keydown', { key: 'y', code: 'KeyY', ctrlKey: true })]); await settle(); }
    work.push(['⌘Y outline', await workOf('keydown', { key: 'y', code: 'KeyY', ctrlKey: true })]); await settle();
    const on = await H(() => { const p = document.querySelector('.ce-items path.ce-p--hatch'); return { cls: document.querySelector('.ce-root').classList.contains('cx-outline'), fill: getComputedStyle(p).fill, stroke: getComputedStyle(p).stroke }; });
    assert.ok(on.cls && on.fill === 'none' && on.stroke !== 'none', JSON.stringify(on));
    await key('Control+y');
    assert.equal(await H(() => getComputedStyle(document.querySelector('.ce-items path.ce-p--hatch')).fill !== 'none'), true);
    const u0 = (await cur('C.history()')).undo; await key('Control+z'); await key('Control+Shift+z'); assert.equal((await cur('C.history()')).undo, u0);
  });
  await step('B10 Alt-drag moves a copy and the original stays (one step); an Alt-click with no drag still reaches inside a group', async () => {
    await key('Escape'); await key('Control+0');
    const b0 = await boxOf('heart-line'), n0 = (await cur('C.items()')).length, h0 = (await cur('C.history()')).undo;
    const q = await H(() => window.CharmEditor.current.ctx.entry('heart-line').it.subpaths[0][0][1]), p = await ptClient(q[0], q[1]), hl = true;
    await page.mouse.click(p.x, p.y); await settle();
    const s0 = await cur('C.selection()');
    await page.keyboard.down('Alt'); await page.mouse.move(p.x, p.y); await page.mouse.down(); await page.mouse.move(p.x + 60, p.y + 10, { steps: 8 }); await page.mouse.up(); await page.keyboard.up('Alt'); await settle();
    const its = await cur('C.items()'), copy = its.find(i => i.name === 'Heart outline' && i.id !== 'heart-line');
    assert.ok(hl && s0.includes('heart-line'), 'heart selected: ' + s0);
    assert.equal(its.length, n0 + 1); assert.ok(copy && copy.source === 'added');
    assert.ok((await boxOf('heart-line')).every((v, i) => near(v, b0[i], 1e-6)), 'the original stays');
    assert.ok((await boxOf(copy.id))[0] > b0[0] + 10, 'the copy moved'); assert.deepEqual(await cur('C.selection()'), [copy.id]);
    assert.equal((await cur('C.history()')).undo, h0 + 1);
    await key('Control+z'); assert.equal((await cur('C.items()')).length, n0);
  });
  await step('B11 every kind of press measured is under a frame of main-thread work (16 ms; the fastest of its repeats, so a busy machine\'s pauses do not count)', async () => {
    const by = new Map(); for (const [k, ms] of work) by.set(k, (by.get(k) || []).concat(ms));
    for (const [k, v] of by) console.log(`       ${k}: fastest ${Math.min(...v).toFixed(2)} ms (${v.map(x => x.toFixed(1)).join(', ')})`);
    const bad = [...by].filter(([, v]) => Math.min(...v) > 16).map(([k]) => k);
    assert.deepEqual(bad, []);
  });
  await step('B12 Save saves at once with things to check (the log says how many); no page errors', async () => {
    await H(() => document.querySelector('.ce-root [data-act="save"]').click());
    await page.waitForFunction(() => window.__closed === true, null, { timeout: 3000 });
    const ok = await H(() => !!window.__result && window.__log.some(e => e[0] === 'designCheck' && e[1].problems >= 2));
    assert.ok(ok, 'saved, and designCheck logged');
    assert.deepEqual(errs, []);
  });
  await browser.close();
  done();
})().catch(e => { console.error(e); process.exit(1); });

function done() {
  console.log(`\nce-extras: ${results.length} passed, ${failures.length} failed`);
  if (failures.length) { failures.forEach(f => console.log('  - ' + f)); process.exit(1); }
}
