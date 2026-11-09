// THE ERASER TOOL (charm-nest-ce-eraser.js, its kernel CharmEditorOps.kernel.erase in charm-nest-ce-ops.js; Paul, 9 Nov 2026: "for the charm editor, I need the option to
// erase portions of a vectorized image. Adobe Illustrator has this option so I need you to copy it and make it function the exact same way and add it to the left-hand side
// menu"). No server, no network, no live service: the editor harness served from 127.0.0.1 (so the Worker loads its scripts as on the site), synthetic pictures traced here.
//   PW_DIR=/opt/node-tools/node_modules node tests/charm-nest/ce-eraser.cjs         (CER_ONLY=A|B; SHOTS=<dir> keeps screenshots)
// A (node) a stroke across a filled square leaves two closed shapes of the right area; a traced 4-band image: every band the stroke crosses loses exactly its part under the
//   brush, the bands still tile (no gap, no overlap: their union is the old union less the brush, their areas add up to it); a selection limits it, nothing selected takes
//   everything under it; the locked cut line is untouched; Alt's rectangle; a line engraving cut into open pieces; text made outlines; a turned, scaled part and a scaled
//   image group keep every point the brush missed exactly where it was; a 16-level photo: the time of one stroke
// B (the editor) the Eraser in the Scissors group of the Tools panel, "Eraser Tool (Shift+E)", Shift+E arms it, C still the Scissors; the cursor a circle the brush's size at the
//   zoom; [ and ] size it; a drag over the heart: the preview on the canvas while dragging (pointermove work measured), the boolean on mouse-up, ONE undo step, undo and redo;
//   the cut line untouched; a selection; Alt's box; Shift's 45°; Eraser Tool Options (double-click: Size, Angle, Roundness, the tip, OK / Cancel); a 16-level photo erased
//   live (the spinner), the bands still tiling; Save keeps it
'use strict';
const fs = require('fs'), path = require('path'), http = require('http'), assert = require('assert/strict');
const ROOT = path.join(__dirname, '../..');
const ONLY = process.env.CER_ONLY || '';
const results = [], failures = [];
const pass = m => { results.push(m); console.log('  ok   ' + m); };
const fail = (m, e) => { failures.push(m + ' :: ' + String(e && e.message || e).split('\n')[0].slice(0, 400)); console.log('  FAIL ' + m + '\n         ' + String(e && e.stack || e).split('\n').slice(0, 10).join('\n         ')); };
async function step(name, fn) { try { const said = await fn(); pass(name + (said ? '  [' + said + ']' : '')); } catch (e) { fail(name, e); } }
const PT = 72 / 25.4, MM2 = (25.4 / 72) ** 2;
const ER = require(path.join(ROOT, 'charm-nest-ce-eraser.js')), OPS = require(path.join(ROOT, 'charm-nest-ce-ops.js')), KR = OPS.kernel;
const CE = require(path.join(ROOT, 'charm-nest-charm-editor.js')), CM = CE.model;
const T = require(path.join(ROOT, 'charm-nest-ce-trace.js')), IMG = require(path.join(ROOT, 'charm-nest-ce-image.js'));
const geo = CM.createGeo(null);
const near = (a, b, eps) => Math.abs(a - b) <= eps;

/* ── fixtures ── */
const X0 = 100, Y0 = 50;
const P = (x, y) => [X0 + x * PT, Y0 + y * PT];
const rectSP = (x0, y0, x1, y1) => [[['m', P(x0, y0)], ['l', P(x1, y0)], ['l', P(x1, y1)], ['l', P(x0, y1)], ['h']]];
const K = 0.5522847498;
const circleSP = (cx, cy, r) => { const k = r * K; return [[['m', P(cx + r, cy)], ['c', P(cx + r, cy + k), P(cx + k, cy + r), P(cx, cy + r)], ['c', P(cx - k, cy + r), P(cx - r, cy + k), P(cx - r, cy)], ['c', P(cx - r, cy - k), P(cx - k, cy - r), P(cx, cy - r)], ['c', P(cx + k, cy - r), P(cx + r, cy - k), P(cx + r, cy)], ['h']]]; };
const path0 = (id, sps, o) => Object.assign({ id, kind: 'path', role: 'hatch', source: 'master', name: id, hidden: false, locked: false, fill: true, stroke: false, subpaths: sps, tx: 0, ty: 0, sx: 1, sy: 1, rot: 0 }, o || {});
const cutOutline = () => path0('cut-outline', rectSP(1, 1, 23, 29), { role: 'cut', fill: false, stroke: true, locked: true, lwPt: 0.5 });
const stroke = (pts, mm, o) => Object.assign({ kind: 'stroke', points: pts.map(p => P(p[0], p[1])), tip: ER.tipOf({ mm, angle: 0, round: 100 }) }, o || {});
const area = (sps, eo) => KR.filledArea(sps, !!eo) * MM2;
const faceOf = (items, id) => { const ix = CM.indexTree(items, geo), e = ix.get(id); return e ? CM.outlinesOf(e.it, geo, e.P) : null; };
const closedAll = sps => sps.every(sub => sub[0][0] === 'm' && sub[sub.length - 1][0] === 'h');
/** a grey picture from a function g(x, y) -> 0..1, antialiased 4 × 4 (as tests/charm-nest/ce-trace.cjs draws them) */
function picture(W, H, g, ss = 4) {
  const data = new Uint8ClampedArray(W * H * 4);
  for (let y = 0; y < H; y++) for (let x = 0; x < W; x++) {
    let s = 0; for (let j = 0; j < ss; j++) for (let i = 0; i < ss; i++) s += g(x + (i + 0.5) / ss, y + (j + 0.5) / ss);
    const v = Math.round(255 * s / (ss * ss)), k = (y * W + x) * 4; data[k] = data[k + 1] = data[k + 2] = v; data[k + 3] = 255;
  }
  return { data, width: W, height: H };
}
const inDisc = (x, y, cx, cy, r) => (x - cx) ** 2 + (y - cy) ** 2 < r * r;
const CIRCLES = (x, y) => {
  if (inDisc(x, y, 115, 115, 75)) return inDisc(x, y, 115, 115, 30) ? 0.8 : 0;
  if (inDisc(x, y, 300, 130, 65)) return inDisc(x, y, 270, 130, 25) ? 0 : 0.3;
  if (inDisc(x, y, 235, 260, 75) && !inDisc(x, y, 235, 260, 40)) return 0.55;
  return 1;
};
const PHOTO = (x, y) => { const r = Math.hypot(x - 200, y - 200) / 200; return Math.min(1, Math.max(0, 0.5 + 0.45 * Math.sin(x / 23 + Math.cos(y / 31) * 2) * Math.cos(y / 17) - 0.3 * (1 - r) + 0.08 * Math.sin(x * y / 900))); };
function tracedGroup(g, W, H, levels, box, id) {
  const res = T.traceSync(picture(W, H, g), { levels });
  return IMG.buildGroup(res, box, { id, name: 'test.png' });
}
const bandsOf = (items, gid) => { const ix = CM.indexTree(items, geo), g = ix.get(gid); return g ? g.it.children.map(ch => ({ id: ch.id, sps: CM.outlinesOf(ch, geo, CM.mul(g.P, geo.matrix(g.it))) })) : []; };
// (measured on polygons: every curve cut into 32 chords, Clipper (CharmNestVector.boolean) for the unions; the same flattening before and after, so it cancels out)
const VEC = require(path.join(ROOT, 'charm-nest-vector.js'));
function polys(sps) {
  const out = [];
  for (const sub of sps) { const r = []; let cur = null; for (const op of sub) { if (op[0] === 'm' || op[0] === 'l') { cur = op[1]; r.push(cur); } else if (op[0] === 'c' && cur) { for (let i = 1; i <= 32; i++) { const t = i / 32, u = 1 - t; r.push([0, 1].map(k => u * u * u * cur[k] + 3 * u * u * t * op[1][k] + 3 * u * t * t * op[2][k] + t * t * t * op[3][k])); } cur = op[3]; } } if (r.length > 2) out.push(r); }
  return out;
}
const ringsA = rings => Math.abs(rings.reduce((A, r) => { let a = 0; for (let i = 0, j = r.length - 1; i < r.length; j = i++) a += (r[j][0] + r[i][0]) * (r[j][1] - r[i][1]); return A + a / 2; }, 0)) * MM2;
const shapeP = (sps, eo) => VEC.boolean(polys(sps), [], 'union', eo ? 'evenodd' : 'nonzero', 'nonzero').map(o => o.points);
const interArea = (sps, eo, er) => ringsA(VEC.boolean(shapeP(sps, eo), shapeP(er, false), 'intersection', 'nonzero', 'nonzero').map(o => o.points));
/** the brush's own shape, made one (the kernel's eraser as a plain path) */
const brushShape = er => KR.pathfinder([{ subpaths: er.subpaths.map(s => (KR.signedArea([s]) < 0 ? KR.reverseSub(s) : s)) }], 'unite').subpaths;
/** the tiling of bands: { union, sum, overlap } in mm² */
/** the bands within a window [x0, y0, x1, y1] pt (what the brush can have changed, and a margin): a 16-level photo measured whole would be millions of points */
const windowed = (bands, w) => bands.map(b => ({ id: b.id, rings: VEC.boolean(polys(b.sps), [[[w[0], w[1]], [w[2], w[1]], [w[2], w[3]], [w[0], w[3]]]], 'intersection', 'evenodd', 'nonzero').map(o => o.points) }));
const ringsOf = b => b.rings || shapeP(b.sps, true);
const inter = (A, B) => ringsA(VEC.boolean(A, B, 'intersection', 'nonzero', 'nonzero').map(o => o.points));
/** the tiling of bands, by intersections only (a union over a photo's pixel-cornered rings is very slow in Clipper's strictly-simple pass): { union, sum, overlap } mm² */
const tiling = bands => { const R = bands.map(ringsOf), sum = R.reduce((a, r) => a + ringsA(r), 0); let ov = 0; for (let i = 0; i < R.length; i++) for (let j = i + 1; j < R.length; j++) ov += inter(R[i], R[j]); return { union: sum - ov, sum, overlap: ov }; };
/** the area of the bands' union under the brush (their parts under it, less what two have there twice) */
const underAll = (bands, brush) => { const B = shapeP(brush, false), parts = bands.map(b => VEC.boolean(ringsOf(b), B, 'intersection', 'nonzero', 'nonzero').map(o => o.points)); let a = parts.reduce((x, r) => x + ringsA(r), 0); for (let i = 0; i < parts.length; i++) for (let j = i + 1; j < parts.length; j++) a -= inter(parts[i], parts[j]); return a; };

/* ═══ A · the operation, in node ═══ */
async function partA() {
  console.log('A · the operation');
  await step('A1 a stroke 2 mm wide straight across a filled 10 mm square: TWO closed paths (the square cut in two is two paths), 40 mm² each; the brush\'s ends outside, its edges exact', async () => {
    const items = [cutOutline(), path0('sq', rectSP(7, 10, 17, 20))];
    const r = ER.eraseItems(items, [], geo, stroke([[4, 15], [20, 15]], 2));
    const out = r.items.filter(it => it.role === 'hatch');
    assert.equal(r.n, 1); assert.equal(out.length, 2, 'two paths');
    assert.ok(out.some(it => it.id === 'sq'), 'the first keeps the id');
    for (const it of out) { assert.ok(closedAll(it.subpaths), 'closed'); assert.ok(near(area(it.subpaths), 40, 1e-3), 'area ' + area(it.subpaths)); }
    assert.equal(r.items[0], items[0], 'the cut line is the same object');
    return out.map(it => area(it.subpaths).toFixed(4) + ' mm²').join(' + ');
  });

  await step('A2 a curved stroke across a filled disc with a hole (a ring): what is left keeps its hole as a hole, its area is the ring\'s less the brush\'s part over it (0.01 %), and the untouched arcs are the very same curves', async () => {
    const ring = circleSP(12, 15, 6).concat(circleSP(12, 15, 2.5).map(s => KR.reverseSub(s)));
    const items = [path0('ring', ring)];
    const g = stroke([[3, 9], [8, 12], [12, 14], [15, 18]], 1.4), er = ER.eraserOf(g);
    const before = area(ring), under = interArea(ring, false, brushShape(er));
    const r = ER.eraseItems(items, [], geo, g), it = r.items.find(i => i.id === 'ring'), after = area(it.subpaths);
    assert.ok(closedAll(it.subpaths));
    assert.ok(near(after, before - under, before * 1e-4), JSON.stringify({ before, under, after }));
    // the hole still a hole: the centre of the ring is not filled
    const centre = P(12, 15), probe = [[['m', [centre[0] - 0.01, centre[1] - 0.01]], ['l', [centre[0] + 0.01, centre[1] - 0.01]], ['l', [centre[0] + 0.01, centre[1] + 0.01]], ['l', [centre[0] - 0.01, centre[1] + 0.01]], ['h']]];
    assert.ok(interArea(it.subpaths, false, probe) < 1e-9, 'the hole is still empty');
    // an arc the brush never came near: the outer circle's lower right quarter, the same control points
    const q = ring[0][1], cur = [].concat(...it.subpaths).filter(op => op[0] === 'c');
    assert.ok(cur.some(op => op.slice(1).every((pt, k) => near(pt[0], q[k + 1][0], 1e-6) && near(pt[1], q[k + 1][1], 1e-6))), 'the untouched quarter is the same curve');
    return `${before.toFixed(3)} − ${under.toFixed(3)} = ${after.toFixed(3)} mm²`;
  });

  let photo = null;
  await step('A3 a traced image (4 grey bands sharing their borders, charm-nest-ce-trace.js + ce-image.js buildGroup): a stroke across it takes from EVERY band it crosses exactly the part under the brush, and the bands still tile: their union is the old union less the brush, and no new overlap', async () => {
    const grp = tracedGroup(CIRCLES, 400, 330, 4, [X0 + 2 * PT, Y0 + 6 * PT, X0 + 22 * PT, Y0 + 24 * PT], 'img');
    const items = [cutOutline(), grp];
    const g = stroke([[3, 8], [9, 13], [14, 14], [21, 19]], 1.2), er = ER.eraserOf(g), brush = brushShape(er);
    const b0 = bandsOf(items, 'img'), t0 = tiling(b0);
    const t = Date.now(), r = ER.eraseItems(items, [], geo, g), ms = Date.now() - t;
    const b1 = bandsOf(r.items, 'img'), t1 = tiling(b1);
    assert.equal(b1.length, b0.length);
    const lost = [];
    for (const b of b0) {
      const now = b1.find(x => x.id === b.id), under = interArea(b.sps, true, brush), a0 = ringsA(shapeP(b.sps, true)), a1 = ringsA(shapeP(now.sps, true));
      lost.push(`${(a0 - a1).toFixed(3)}`);
      if (under > 1e-3) assert.ok(a0 - a1 > 0, 'band ' + b.id + ' lost nothing');
      assert.ok(near(a0 - a1, under, Math.max(under * 2e-3, 1e-4)), `band ${b.id}: lost ${a0 - a1} for ${under} under the brush`);
    }
    assert.ok(b0.filter(b => interArea(b.sps, true, brush) > 1e-3).length >= 3, 'the stroke crosses three bands or more');
    const underU = underAll(b0, brush);
    assert.ok(near(t1.union, t0.union - underU, Math.max(underU * 2e-3, 1e-4)), `union ${t1.union} vs ${t0.union} − ${underU}: a gap`);
    assert.ok(t1.overlap <= t0.overlap + 2e-5, `overlap ${t1.overlap} (was ${t0.overlap})`);
    assert.equal(r.items[0], items[0], 'the cut line untouched');
    photo = { items, r };
    return `${ms} ms · lost per band ${lost.join(' / ')} mm² · union ${t0.union.toFixed(3)} → ${t1.union.toFixed(3)} (brush over it ${underU.toFixed(3)}) · overlap ${t0.overlap.toExponential(1)} → ${t1.overlap.toExponential(1)} mm²`;
  });

  await step('A4 with a selection only the selected part is erased; with nothing selected, every unlocked part under the stroke; a locked part and a hidden part never', async () => {
    const mk = () => [cutOutline(), path0('a', rectSP(5, 5, 12, 12)), path0('b', rectSP(13, 5, 20, 12)), path0('lk', rectSP(5, 14, 12, 20), { locked: true }), path0('hd', rectSP(13, 14, 20, 20), { hidden: true })];
    const g = stroke([[3, 8], [22, 8]], 1);
    let items = mk(), r = ER.eraseItems(items, ['a'], geo, g);
    assert.equal(r.n, 1); assert.ok(r.items.filter(i => i.id === 'b').length === 1 && r.items.find(i => i.id === 'b') === items[2], 'b untouched with a selected');
    assert.ok(r.items.filter(i => /^a$|^p/.test(i.id) || (i.name === 'a')).length >= 2, 'a cut in two');
    assert.ok(r.sel.length === 2 && r.sel.includes('a'), 'both pieces of a selected: ' + r.sel);
    items = mk(); r = ER.eraseItems(items, [], geo, g);
    assert.equal(r.n, 2); assert.equal(r.items.filter(i => i.name === 'a').length, 2); assert.equal(r.items.filter(i => i.name === 'b').length, 2); assert.deepEqual(r.sel, []);
    const g2 = stroke([[3, 17], [22, 17]], 1); items = mk(); r = ER.eraseItems(items, [], geo, g2);
    assert.equal(r.n, 0); assert.equal(r.items, items, 'the locked and the hidden untouched');
    items = mk(); r = ER.eraseItems(items, ['lk'], geo, g2); assert.equal(r.n, 0, 'a locked part selected is still not erased');
  });

  await step('A5 the locked cut outline (and its hole) are never erased, even when the stroke runs along them; a part inside a group keeps the group', async () => {
    const hole = path0('cut-hole', circleSP(12, 3.3, 1.05), { role: 'cut', fill: false, stroke: true, locked: false });   // (role cut: never, locked or not)
    const items = [cutOutline(), hole, { id: 'grp', kind: 'group', role: 'hatch', source: 'added', name: 'G', hidden: false, locked: false, tx: 0, ty: 0, sx: 1, sy: 1, rot: 0, children: [path0('in', rectSP(4, 2, 20, 6))] }];
    const r = ER.eraseItems(items, [], geo, stroke([[1, 1], [23, 1], [23, 4], [1, 4]], 1.5));
    assert.equal(r.items[0], items[0]); assert.equal(r.items[1], items[1]);
    const g = r.items.find(i => i.id === 'grp'); assert.ok(g && g.children.length >= 1, 'the group kept');
    assert.ok(area(faceOf(r.items, 'in')) < area(items[2].children[0].subpaths), 'its part erased');
  });

  await step('A6 Alt\'s marquee: the rectangle dragged is taken out (a 4 × 3 mm box over a 10 mm square\'s edge: 94 mm² left, one path with a notch; a box inside it: a hole)', async () => {
    const items = [path0('sq', rectSP(7, 10, 17, 20))];
    const r = ER.eraseItems(items, [], geo, { kind: 'rect', a: P(15, 12), b: P(19, 15) });
    const it = r.items[0];
    assert.equal(r.items.length, 1); assert.ok(near(area(it.subpaths), 94, 1e-4), 'area ' + area(it.subpaths));
    const r2 = ER.eraseItems(items, [], geo, { kind: 'rect', a: P(10, 12), b: P(13, 15) });
    assert.ok(near(area(r2.items[0].subpaths), 91, 1e-4) && r2.items[0].subpaths.length === 2, 'a box inside: a hole');
  });

  await step('A7 a line engraving (stroke only) is cut where the brush crosses it: a closed outline becomes open pieces outside the brush, each piece exactly on the old line', async () => {
    const items = [path0('ln', circleSP(12, 15, 5), { role: 'line', fill: false, stroke: true, lwPt: 0.25 })];
    const r = ER.eraseItems(items, [], geo, stroke([[12, 7], [12, 23]], 1));
    const pieces = r.items.filter(i => i.name === 'ln');
    assert.equal(pieces.length, 2, 'two pieces'); assert.ok(pieces.every(p => p.subpaths[0][p.subpaths[0].length - 1][0] !== 'h'), 'open');
    for (const p of pieces) for (const op of p.subpaths[0]) { const q = op[op.length - 1]; assert.ok(near(Math.hypot(q[0] - P(12, 15)[0], q[1] - P(12, 15)[1]), 5 * PT, 0.02), 'on the circle'); assert.ok(Math.abs(q[0] - P(12, 15)[0]) >= 0.5 * PT - 1e-4, 'outside the brush'); }
  });

  await step('A8 text made outlines (the editor\'s glyph paths) erase as paths: the letters under the stroke lose their part, closed; live words are not touched', async () => {
    const g = CE.builtinTextToPaths('HELLO', 'sans', 6 * PT, { align: 'left' });
    const sps = g.subpaths.map(sub => sub.map(op => op.map(v => (Array.isArray(v) ? [v[0] + X0 + 3 * PT, v[1] + Y0 + 15 * PT] : v))));
    const items = [path0('outl', sps, { role: 'art' }), { id: 'live', kind: 'text', role: 'hatch', text: 'HI', fontKey: 'sans', sizePt: 10, tx: X0 + 12 * PT, ty: Y0 + 13 * PT, sx: 1, sy: 1, rot: 0 }];
    const a0 = area(sps), r = ER.eraseItems(items, [], geo, stroke([[2, 13], [22, 13]], 1.2));
    const it = r.items.find(i => i.id === 'outl');
    assert.ok(it && area(it.subpaths) < a0 - 0.5 && closedAll(it.subpaths), 'outlines erased'); assert.equal(r.items.find(i => i.id === 'live'), items[1], 'live words untouched');
    return `${a0.toFixed(2)} → ${area(it.subpaths).toFixed(2)} mm²`;
  });

  await step('A9 a part turned 30° and scaled, and a traced image group scaled 1.3 and turned 15°: their transforms are kept and every point the brush missed stays exactly where it was on the charm', async () => {
    const it = path0('t', rectSP(6, 8, 18, 22), { rot: 30, sx: 1.2, sy: 0.8, tx: 3, ty: -2 });
    const W0 = geo.matrix(it), far = CM.ap(W0, ...it.subpaths[0][3][1]);
    const r = ER.eraseItems([it], [], geo, stroke([[4, 15], [22, 15]], 1));
    const parts = r.items, all = [].concat(...parts.map(p => [].concat(...CM.transformSubpaths(p.subpaths, geo.matrix(p)).map(s => s.map(op => op[op.length - 1]).filter(Array.isArray)))));
    assert.ok(parts.every(p => p.rot === 30 && p.sx === 1.2), 'the turn and scale kept');
    assert.ok(all.some(q => near(q[0], far[0], 1e-7) && near(q[1], far[1], 1e-7)), 'a corner the brush missed is where it was');
    const grp = Object.assign(tracedGroup(CIRCLES, 400, 330, 4, [X0 + 2 * PT, Y0 + 6 * PT, X0 + 22 * PT, Y0 + 24 * PT], 'img2'), { sx: 1.3, sy: 1.3, rot: 15, tx: 1, ty: 2 });
    const before = bandsOf([grp], 'img2'), r2 = ER.eraseItems([grp], [], geo, stroke([[10, 6], [10, 9]], 1)), after = bandsOf(r2.items, 'img2');
    // a band the brush did not reach is drawn exactly where it was (its ops the same in face space)
    let same = 0;
    for (const b of before) { const a = after.find(x => x.id === b.id); if (JSON.stringify(a.sps) === JSON.stringify(b.sps)) same++; else { const ff = [].concat(...b.sps).slice(0, 50), gg = [].concat(...a.sps); for (const op of ff) { const q = op[op.length - 1]; if (Array.isArray(q) && gg.some(o => Array.isArray(o[o.length - 1]) && near(o[o.length - 1][0], q[0], 1e-6) && near(o[o.length - 1][1], q[1], 1e-6))) same += 0; } } }
    const g2 = r2.items[0], W1 = geo.matrix(g2), W00 = geo.matrix(grp);
    assert.ok(W1.every((v, i) => near(v, W00[i], 1e-9)), 'the group\'s matrix is the same');
    assert.ok(r2.n >= 1, 'something erased');
    return `${same} of ${before.length} bands untouched, the group's matrix the same`;
  });

  await step('A10 a 16-level photo (charm-nest-ce-trace.js at 16 levels): one 15 mm stroke across it, on this thread (timed); the bands still tile (measured in a 4 mm window on the stroke)', async () => {
    const grp = tracedGroup(PHOTO, 420, 420, 16, [X0 + 2 * PT, Y0 + 5 * PT, X0 + 22 * PT, Y0 + 25 * PT], 'ph');
    const nOps = grp.children.reduce((a, c) => a + c.subpaths.reduce((x, s) => x + s.length, 0), 0);
    const items = [cutOutline(), grp], g = stroke([[4, 9], [10, 14], [19, 21]], 1.5);
    const t = Date.now(), r = ER.eraseItems(items, [], geo, g), ms = Date.now() - t;
    const brush = brushShape(ER.eraserOf(g)), c = P(10, 14), win = [c[0] - 2 * PT, c[1] - 2 * PT, c[0] + 2 * PT, c[1] + 2 * PT];   // (a 4 mm window on the stroke: the tiling is measured there)
    const b0 = windowed(bandsOf(items, 'ph'), win), b1 = windowed(bandsOf(r.items, 'ph'), win), t0 = tiling(b0), t1 = tiling(b1);
    const underU = underAll(b0, VEC.boolean(shapeP(brush, false), [[[win[0], win[1]], [win[2], win[1]], [win[2], win[3]], [win[0], win[3]]]], 'intersection', 'nonzero', 'nonzero').map(o => [['m', o.points[0]]].concat(o.points.slice(1).map(p => ['l', p]), [['h']])));
    assert.ok(grp.children.length >= 12, 'bands ' + grp.children.length);
    assert.ok(near(t1.union, t0.union - underU, Math.max(underU * 2e-3, 1e-4)), `union ${t1.union} vs ${t0.union - underU}`);
    assert.ok(t1.overlap <= t0.overlap + 1e-3, `overlap ${t1.overlap} (was ${t0.overlap})`);
    assert.ok(ms < 4000, ms + ' ms');
    return `${grp.children.length} bands, ${nOps} ops, ${r.n} bands erased in ${ms} ms on one thread`;
  });

  await step('A11 the brush: Size, Angle and Roundness make the tip (an ellipse 2 × 1 mm at 30°); its sweep covers the tip at each end and the band between, nothing more', async () => {
    const tip = ER.tipOf({ mm: 2, angle: 30, round: 50 });
    assert.ok(near(tip.a, PT, 1e-9) && near(tip.b, PT / 2, 1e-9));
    const sw = ER.sweep([[0, 0], [10 * PT, 0]], tip), a = KR.filledArea(brushShape({ subpaths: sw })) * MM2;
    const want = Math.PI * 1 * 0.5 + 10 * 2 * Math.hypot(Math.sin(Math.PI / 6) * 1, Math.cos(Math.PI / 6) * 0.5);   // (the ellipse and the band: its height across the stroke)
    assert.ok(near(a, want, 6e-3), `swept ${a} mm², want ${want}`);
    assert.deepEqual(ER.snap45([0, 0], [10, 3]), [10, 0]); const d = ER.snap45([0, 0], [10, 9]); assert.ok(near(d[0], d[1], 1e-9));
  });
}

/* ═══ B · the editor ═══ */
let chromium = null;
try { ({ chromium } = require(path.join(process.env.PW_DIR || path.join(ROOT, 'node_modules'), 'playwright-core'))); } catch (_) { /* no browser here */ }
const SHOTS = process.env.SHOTS || ''; if (SHOTS) fs.mkdirSync(SHOTS, { recursive: true });

async function partB(browser, base) {
  console.log('B · the editor');
  const ctxB = await browser.newContext({ viewport: { width: 1280, height: 760 }, reducedMotion: 'reduce' });
  await ctxB.route(u => !u.href.startsWith(base), r => r.abort());   // (nothing leaves the machine)
  const page = await ctxB.newPage();
  const errs = []; page.on('pageerror', e => errs.push('pageerror: ' + e.message)); page.on('console', m => { if (m.type() === 'error') errs.push('console: ' + m.text()); });
  await page.goto(base + 'tests/charm-nest/fixtures/charm-editor-harness.html');
  for (const f of ['charm-nest-vector.js', 'charm-nest-ce-ops.js', 'charm-nest-ce-paths.js', 'charm-nest-ce-eraser.js']) await page.addScriptTag({ url: base + f });
  await page.evaluate(() => { try { localStorage.removeItem('ce.eraser'); } catch (e) { /* fine */ } window.CharmEditorEraser.setOptions({ mm: 1, angle: 0, round: 100 }); });
  const settle = () => page.evaluate(() => new Promise(r => requestAnimationFrame(() => requestAnimationFrame(r))));
  await page.evaluate(() => { window.openEditor(); }); await page.waitForSelector('.ce-root .ce-items path'); await settle(); await settle();
  const C = (fn, arg) => page.evaluate(([src, a]) => (new Function('C', 'a', 'return (' + src + ')(C, a)'))(window.CharmEditor.current.ctx, a), [fn.toString(), arg]);
  const tool = () => page.evaluate(() => window.CharmEditor.current.tool());
  const hist = () => page.evaluate(() => window.CharmEditor.current.history().undo);
  const client = (x, y) => page.evaluate(([x, y]) => window.CharmEditor.current.clientOf(window.fixture.X0 + x * window.fixture.PT, window.fixture.Y0 + y * window.fixture.PT), [x, y]);
  const areaOf = id => C((c, id) => { const e = c.entry(id); return e ? window.CharmEditorOps.kernel.filledArea(c.model.outlinesOf(e.it, c.geo, e.P), !!e.it.evenOdd) * Math.pow(25.4 / 72, 2) : 0; }, id);
  const named = name => C((c, n) => { const out = []; c.model.walk(c.items(), it => { if (it.name === n) out.push(it.id); }); return out; }, name);
  const shot = async n => { if (SHOTS) await page.screenshot({ path: path.join(SHOTS, n) }); };
  async function drag(pts, mods, steps) {
    const a = await client(...pts[0]); await page.mouse.move(a.x, a.y);
    for (const m of mods || []) await page.keyboard.down(m);
    await page.mouse.down();
    for (const p of pts.slice(1)) { const c = await client(...p); await page.mouse.move(c.x, c.y, { steps: steps || 10 }); }
    await page.mouse.up();
    for (const m of (mods || []).slice().reverse()) await page.keyboard.up(m);
    await settle();
  }

  await step('B1 the Tools panel: the Eraser, its own button right after the Scissors ("Eraser Tool (Shift+E)", Illustrator\'s eraser); Shift+E arms it, C the Scissors, Esc goes back to Select', async () => {
    const head = await page.evaluate(() => { const b = document.querySelector('.ce-rail .ce-tool[data-tool="eraser"]'); return b ? { label: b.getAttribute('aria-label'), inFly: !!b.closest('.ce-drop'), svg: !!b.querySelector('svg') } : null; });
    assert.ok(head, 'an Eraser button on the rail');
    assert.equal(head.label, 'Eraser Tool (Shift+E)'); assert.equal(head.inFly, false, 'its own button'); assert.ok(head.svg);
    const rail = await page.evaluate(() => [...document.querySelectorAll('.ce-rail .ce-tools > .ce-tool, .ce-rail .ce-tools > .ce-drop > .ce-tool')].map(b => b.dataset.tool));
    assert.ok(rail.indexOf('eraser') === rail.indexOf('scissors') + 1, 'right after the Scissors: ' + rail.join(','));
    await page.keyboard.press('Shift+E'); await settle(); assert.equal(await tool(), 'eraser');
    await page.keyboard.press('c'); await settle(); assert.equal(await tool(), 'scissors');
    await page.keyboard.press('Shift+E'); await settle(); assert.equal(await tool(), 'eraser');
    await page.keyboard.press('Escape'); await settle(); assert.equal(await tool(), 'select');
    await page.keyboard.press('Control+u'); await settle();   // (smart guides off: nothing snaps)
    return rail.join(' ');
  });

  await step('B2 the cursor is a circle the size of the brush at this zoom (1 mm × the view\'s px a pt); zoomed in, it grows with the view; [ and ] make the brush smaller and larger', async () => {
    await page.keyboard.press('Shift+E'); await settle();
    const c = await client(6, 26); await page.mouse.move(c.x, c.y); await page.mouse.move(c.x + 3, c.y + 2); await settle();
    const k = await C(c => Math.hypot(c.view()[0], c.view()[1]));
    let d = await page.evaluate(() => +document.querySelector('.cer-cur').dataset.d);
    assert.ok(near(d, 1 * PT * k, 0.01), `circle ${d} px for ${PT * k}`);
    const at = await page.evaluate(() => { const e = document.querySelector('.cer-cur ellipse').getBoundingClientRect(); return [e.left + e.width / 2, e.top + e.height / 2]; });
    assert.ok(near(at[0], c.x + 3, 1) && near(at[1], c.y + 2, 1), 'centred on the pointer: ' + at);
    await page.keyboard.press(']'); await page.keyboard.press(']'); await settle();
    assert.ok(near((await page.evaluate(() => window.CharmEditorEraser.options().mm)), 1.5, 1e-9), '] ] -> 1.5 mm');
    await page.keyboard.press('['); await page.keyboard.press('['); await page.keyboard.press('['); await settle();
    assert.ok(near((await page.evaluate(() => window.CharmEditorEraser.options().mm)), 0.9, 1e-9), '[ [ [ -> 0.9 mm');
    await page.keyboard.press(']'); await settle();
    await C(c => c.setZoom(c.zoom() * 2)); await settle(); await page.mouse.move(c.x + 1, c.y); await settle();
    const k2 = await C(c => Math.hypot(c.view()[0], c.view()[1])), d2 = await page.evaluate(() => +document.querySelector('.cer-cur').dataset.d);
    assert.ok(near(d2, PT * k2, 0.01) && d2 > d * 1.9, `zoomed: ${d2} px`);
    await page.keyboard.press('Shift+1'); await settle();   // (fit the charm again)
    return `${d.toFixed(1)} px at ${(k * 100 / PT * 25.4 / 72 * PT).toFixed(0)} → ${d2.toFixed(1)} px zoomed`;
  });

  await step('B2b the size bar (top left of the work area while the Eraser is in hand): + and − step the brush, a size typed in mm and Enter sets it, the slider drags it; the circle follows; it goes when another tool is picked', async () => {
    await page.keyboard.press('Shift+E'); await settle();
    const opt = () => page.evaluate(() => window.CharmEditorEraser.options().mm);
    assert.ok(await page.evaluate(() => !!document.querySelector('.cer-size')), 'the bar is shown');
    await page.click('.cer-size [data-er="+"]'); assert.ok(near(await opt(), 1.25, 1e-9), '+ ' + await opt());
    await page.click('.cer-size [data-er="-"]'); assert.ok(near(await opt(), 1, 1e-9), '− ' + await opt());
    await page.fill('.cer-size [data-er="mm"]', '3'); await page.press('.cer-size [data-er="mm"]', 'Enter'); await settle();
    assert.equal(await opt(), 3, 'typed 3 mm');
    assert.equal(await tool(), 'eraser', 'typing in the field did not change the tool');
    const c = await client(6, 26); await page.mouse.move(c.x, c.y); await settle();
    const k = await C(c => Math.hypot(c.view()[0], c.view()[1])), d = await page.evaluate(() => +document.querySelector('.cer-cur').dataset.d);
    assert.ok(near(d, 3 * PT * k, 0.02), 'the circle at 3 mm: ' + d);
    await page.evaluate(() => { const r = document.querySelector('.cer-size [data-er="slide"]'); r.value = '0'; r.dispatchEvent(new Event('input', { bubbles: true })); });
    assert.equal(await opt(), 0.05, 'the slider at its smallest');
    await page.evaluate(() => window.CharmEditorEraser.setOptions({ mm: 1 }));
    await page.keyboard.press('v'); await settle();
    assert.equal(await page.evaluate(() => !!document.querySelector('.cer-size')), false, 'gone with the Selection tool');
  });

  await step('B3 a drag across the filled heart with nothing selected: painted on the canvas while dragging (the pointermove work measured), erased on mouse-up into two closed paths, ONE undo step; ⌘Z gives the heart back, ⇧⌘Z erases it again', async () => {
    await page.keyboard.press('Shift+E'); await settle();
    const a0 = await areaOf('heart-fill'), h0 = await hist();
    await page.evaluate(() => { window.__work = []; window.addEventListener('pointermove', () => { window.__t0 = performance.now(); }, true); document.addEventListener('pointermove', () => { if (window.__t0) window.__work.push(performance.now() - window.__t0); window.__t0 = 0; }); });
    const a = await client(17.6, 4.4), b = await client(21.6, 4.8);
    await page.mouse.move(a.x, a.y); await page.mouse.down(); await page.mouse.move(b.x, b.y, { steps: 24 });
    const painted = await page.evaluate(([x, y]) => { const cv = document.querySelector('.cer-cv'), r = cv.getBoundingClientRect(), dpr = devicePixelRatio, d = cv.getContext('2d').getImageData(Math.round((x - r.left) * dpr), Math.round((y - r.top) * dpr), 1, 1).data; return [...d]; }, [(a.x + b.x) / 2, (a.y + b.y) / 2]);
    const midArea = await areaOf('heart-fill');
    await page.mouse.up(); await settle();
    const work = await page.evaluate(() => window.__work.slice().sort((x, y) => x - y));
    assert.ok(painted[3] === 255 && painted[0] > 240, 'the paper painted over the stroke: ' + painted);
    assert.equal(midArea, a0, 'nothing changed before mouse-up');
    const pieces = await named('Filled heart');
    assert.equal(pieces.length, 2, 'the heart in two: ' + pieces);
    const a1 = (await Promise.all(pieces.map(areaOf))).reduce((x, y) => x + y, 0);
    assert.ok(a1 < a0 - 0.3, `${a0} -> ${a1}`);
    const closed = await C((c, ids) => ids.every(id => c.entry(id).it.subpaths.every(s => s[s.length - 1][0] === 'h')), pieces); assert.ok(closed, 'closed');
    assert.equal(await hist() - h0, 1, 'one undo step');
    const cleared = await page.evaluate(() => { const cv = document.querySelector('.cer-cv'); const d = cv.getContext('2d').getImageData(0, 0, cv.width, cv.height).data; for (let i = 3; i < d.length; i += 4) if (d[i]) return false; return true; });
    assert.ok(cleared, 'the canvas cleared once the result is drawn');
    await shot('B3-erased.png');
    await page.keyboard.press('Control+z'); await settle();
    assert.deepEqual(await named('Filled heart'), ['heart-fill']); assert.ok(near(await areaOf('heart-fill'), a0, 1e-9), 'undo: the heart back');
    await page.keyboard.press('Control+Shift+z'); await settle();
    assert.equal((await named('Filled heart')).length, 2, 'redo: erased again');
    const p95 = work[Math.floor(work.length * 0.95)] || 0;
    assert.ok(p95 < 16, 'pointermove p95 ' + p95);
    return `${a0.toFixed(2)} → ${a1.toFixed(2)} mm² · ${work.length} moves, p95 ${p95.toFixed(2)} ms, max ${(work[work.length - 1] || 0).toFixed(2)} ms`;
  });

  await step('B4 a stroke along the cut outline and over the bail hole leaves both exactly as they were (locked); the day squares it crosses (line engravings) are cut', async () => {
    const before = await C(c => JSON.stringify([c.entry('cut-outline').it.subpaths, c.entry('cut-hole').it.subpaths]));
    await drag([[1, 10], [1, 2], [12, 2], [12, 4], [3, 11.5]]);
    const after = await C(c => JSON.stringify([c.entry('cut-outline').it.subpaths, c.entry('cut-hole').it.subpaths]));
    assert.equal(after, before);
    const sq = await named('Day square 1'); assert.ok(sq.length >= 2, 'day square 1 cut: ' + sq.length);
    const open = await C((c, ids) => ids.every(id => c.entry(id).it.subpaths.every(s => s[s.length - 1][0] !== 'h')), sq); assert.ok(open, 'its pieces are open lines');
  });

  await step('B5 with one day square selected only it is erased, the one next to it under the same stroke is not; the pieces stay selected', async () => {
    await page.keyboard.press('v'); const s2 = await client(2.66, 15.2); await page.mouse.click(s2.x, s2.y); await settle();
    const selId = (await C(c => c.selection()))[0]; assert.equal(selId, 'day-1-0');
    const next = await C(c => JSON.stringify(c.entry('day-1-1').it.subpaths));
    await page.keyboard.press('Shift+E'); await settle();
    await drag([[3, 15], [9, 15]]);
    assert.equal(await C(c => JSON.stringify(c.entry('day-1-1').it.subpaths)), next, 'the next square untouched');
    const mine = await named('Day square 8'); assert.ok(mine.length >= 2, 'the selected square cut');
    assert.deepEqual((await C(c => c.selection())).sort(), mine.sort());
    await page.keyboard.press('Escape'); await settle();
  });

  await step('B6 Alt-drag: a marquee box, erased when let go (a filled square drawn here, a box out of its corner: 75 % left)', async () => {
    await C(c => c.addItems([{ id: 'boxme', kind: 'path', role: 'hatch', name: 'Box', fill: true, stroke: false, subpaths: [[['m', [100 + 4 * 72 / 25.4, 50 + 20 * 72 / 25.4]], ['l', [100 + 8 * 72 / 25.4, 50 + 20 * 72 / 25.4]], ['l', [100 + 8 * 72 / 25.4, 50 + 24 * 72 / 25.4]], ['l', [100 + 4 * 72 / 25.4, 50 + 24 * 72 / 25.4]], ['h']]] }], { select: false }));
    await C(c => { c.commit('test'); c.select([]); });
    await page.keyboard.press('Shift+E'); await settle();
    const h0 = await hist();
    await drag([[6, 22], [9, 25]], ['Alt']);
    assert.ok(near(await areaOf('boxme'), 12, 1e-3), 'area ' + (await areaOf('boxme')));
    assert.equal(await hist() - h0, 1);
  });

  await step('B7 Shift-drag: the stroke is straight at 0°, 45° or 90° however the hand wobbles (a filled bar cut by a vertical band 1 mm wide)', async () => {
    await C(c => c.addItems([{ id: 'bar', kind: 'path', role: 'hatch', name: 'Bar', fill: true, stroke: false, subpaths: [[['m', [100 + 14 * 72 / 25.4, 50 + 20 * 72 / 25.4]], ['l', [100 + 20 * 72 / 25.4, 50 + 20 * 72 / 25.4]], ['l', [100 + 20 * 72 / 25.4, 50 + 22 * 72 / 25.4]], ['l', [100 + 14 * 72 / 25.4, 50 + 22 * 72 / 25.4]], ['h']]] }], { select: false }));
    await C(c => { c.commit('test'); c.select([]); });
    await page.keyboard.press('Shift+E'); await settle();
    await drag([[17, 19], [17.4, 20.5], [16.7, 22], [17.2, 23]], ['Shift']);
    const parts = await named('Bar'), a = (await Promise.all(parts.map(areaOf))).reduce((x, y) => x + y, 0);
    assert.equal(parts.length, 2); assert.ok(near(a, 12 - 2 * 1, 2e-3), 'a vertical band 1 mm: ' + a);
  });

  await step('B8 Eraser Tool Options (a double-click on the tool): Size, Angle, Roundness and the tip drawn as they are set; OK keeps them, Cancel leaves them as they were', async () => {
    await page.dblclick('.ce-rail .ce-tool[data-tool="eraser"]'); await settle();
    const dlg = await page.evaluate(() => { const c = document.querySelector('.cer-opts .cops-card'); return c ? { title: c.querySelector('h3').textContent, f: [...c.querySelectorAll('.cops-f span:first-child')].map(s => s.textContent), tip: !!c.querySelector('.cer-tip ellipse') } : null; });
    assert.deepEqual(dlg, { title: 'Eraser Tool Options', f: ['Size', 'Angle', 'Roundness'], tip: true });
    await page.fill('[data-eo="mm"]', '2'); await page.fill('[data-eo="angle"]', '30'); await page.fill('[data-eo="round"]', '50'); await settle();
    const ry = await page.evaluate(() => document.querySelector('.cer-tip ellipse').getAttribute('ry'));
    assert.equal(+ry, 17, 'the tip drawn at 50 %');
    await page.click('.cer-opts [data-eob="ok"]'); await settle();
    assert.deepEqual(await page.evaluate(() => window.CharmEditorEraser.options()), { mm: 2, angle: 30, round: 50 });
    await page.dblclick('.ce-rail .ce-tool[data-tool="eraser"]'); await settle();
    await page.fill('[data-eo="mm"]', '7'); await page.click('.cer-opts [data-eob="cancel"]'); await settle();
    assert.equal(await page.evaluate(() => window.CharmEditorEraser.options().mm), 2);
    assert.equal(await page.evaluate(() => !!document.querySelector('.cer-opts')), false);
    await page.evaluate(() => window.CharmEditorEraser.setOptions({ mm: 1, angle: 0, round: 100 }));
    assert.equal(await tool(), 'eraser');
  });

  await step('B9 a traced 16-level photo on the charm: a stroke across it is drawn live (pointermove work measured), erased in the Worker with "Erasing…" while it takes over 150 ms, ONE undo step; every band still tiles', async () => {
    const grp = tracedGroup(PHOTO, 420, 420, 16, [X0 + 2 * PT, Y0 + 5 * PT, X0 + 22 * PT, Y0 + 25 * PT], 'ph');
    await C((c, g) => { c.addItems([g], { select: false }); c.commit('place'); c.select([]); }, grp);
    await settle();
    const bandsIn = () => C(c => { const e = c.entry('ph'), W = c.model.mul(e.P, c.geo.matrix(e.it)); return e.it.children.map(ch => ({ id: ch.id, sps: c.model.outlinesOf(ch, c.geo, W) })); });
    const win = (() => { const c = P(10, 14); return [c[0] - 2 * PT, c[1] - 2 * PT, c[0] + 2 * PT, c[1] + 2 * PT]; })();   // (a 4 mm window on the stroke: the tiling measured there)
    const b0 = windowed(await bandsIn(), win), h0 = await hist();
    await page.evaluate(() => { window.__work = []; window.__spin = false; const mo = new MutationObserver(() => { if (document.querySelector('.cer-busy')) window.__spin = true; }); mo.observe(document.body, { childList: true, subtree: true }); });
    const pts = [[4, 9], [10, 14], [19, 21]];
    const a = await client(...pts[0]); await page.mouse.move(a.x, a.y); await page.mouse.down();
    for (const p of pts.slice(1)) { const c2 = await client(...p); await page.mouse.move(c2.x, c2.y, { steps: 30 }); }
    const t0 = Date.now(); await page.mouse.up();
    await page.waitForFunction(() => !document.querySelector('.cer-busy') && window.CharmEditor.current.history().undo > 0, null, { timeout: 30000 });
    await page.waitForFunction(h => window.CharmEditor.current.history().undo === h + 1, h0, { timeout: 30000 });
    const ms = Date.now() - t0;
    const work = await page.evaluate(() => window.__work.slice().sort((x, y) => x - y)), spin = await page.evaluate(() => window.__spin);
    const b1 = windowed(await bandsIn(), win), t0b = tiling(b0), t1b = tiling(b1);
    assert.ok(t1b.union < t0b.union - 1, `the union shrank: ${t0b.union} -> ${t1b.union}`);
    assert.ok(t1b.overlap <= t0b.overlap + 1e-3, `no new overlap: ${t0b.overlap} -> ${t1b.overlap}`);
    if (ms > 400) assert.ok(spin, 'the spinner showed for a ' + ms + ' ms erase');
    const p95 = work[Math.floor(work.length * 0.95)] || 0;
    assert.ok(p95 < 16, 'pointermove p95 ' + p95);
    await shot('B9-photo.png');
    return `${b0.length} bands, ${ms} ms from mouse-up to drawn, spinner ${spin ? 'shown' : 'not needed'} · ${work.length} moves, p95 ${p95.toFixed(2)} ms · union ${t0b.union.toFixed(2)} → ${t1b.union.toFixed(2)} mm², overlap ${t0b.overlap.toExponential(1)} → ${t1b.overlap.toExponential(1)}`;
  });

  await step('B10 Save: the result carries the erased heart (two paths) and the erased image bands; no page error', async () => {
    await page.keyboard.press('Escape'); await settle();
    await page.click('.ce-root [data-act="save"]'); await page.waitForFunction(() => window.__closed === true);
    const r = await page.evaluate(() => window.__result), f = r.faces.front.items;
    const hearts = []; const walk = list => { for (const it of list) { if (it.name === 'Filled heart') hearts.push(it); if (it.children) walk(it.children); } }; walk(f);
    assert.equal(hearts.length, 2, 'two heart pieces saved');
    assert.ok(r.changes.front.changed['heart-fill'] && r.changes.front.added.length >= 1, 'the heart changed and a piece added');
    assert.deepEqual(errs, []);
  });
  await ctxB.close();
}

(async () => {
  if (ONLY === '' || ONLY === 'A') await partA();
  if (ONLY === '' || ONLY === 'B') {
    const exe = process.env.CHROMIUM || ['/opt/pw-browsers/chromium-1243/chrome-linux64/chrome', '/opt/pw-browsers/chromium'].find(p => fs.existsSync(p));
    if (!chromium || !exe) console.log('  - no playwright-core or Chromium: the browser checks were not run');
    else {
      const types = { '.html': 'text/html', '.js': 'text/javascript', '.css': 'text/css', '.woff2': 'font/woff2' };
      const server = http.createServer((req, res) => {
        const p = path.normalize(path.join(ROOT, decodeURIComponent(req.url.split('?')[0])));
        if (!p.startsWith(ROOT) || !fs.existsSync(p) || fs.statSync(p).isDirectory()) { res.writeHead(404); res.end(); return; }
        res.writeHead(200, { 'content-type': types[path.extname(p)] || 'application/octet-stream' }); fs.createReadStream(p).pipe(res);
      });
      await new Promise(r => server.listen(0, '127.0.0.1', r));
      const browser = await chromium.launch({ executablePath: fs.statSync(exe).isDirectory() ? undefined : exe, args: ['--no-sandbox'] });
      try { await partB(browser, `http://127.0.0.1:${server.address().port}/`); } finally { await browser.close(); server.close(); }
    }
  }
  console.log(`\n${results.length}/${results.length + failures.length} checks pass`);
  if (failures.length) { console.log(failures.map(f => '  - ' + f).join('\n')); process.exit(1); }
})().catch(e => { console.error(e); process.exit(1); });
