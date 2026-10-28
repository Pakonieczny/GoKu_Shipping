/* THE CHARM EDITOR'S OBJECT COMMANDS (charm-nest-ce-ops.js; Paul, 28 Oct 2026: "A lot of the high-end features are missing and I would like you to put all the useful
 * ones in ... follow the 70/30 Ruel"). No network, no live service.
 *
 *   node tests/charm-nest/ce-ops.cjs                       the geometry and the operations on items (node only)
 *   $SP/int/slot.sh node tests/charm-nest/ce-ops.cjs       ... and the browser pass (a real Chromium, the editor's harness page served from 127.0.0.1)
 *
 *  A  Pathfinder on two circles: Unite / Minus Front / Intersect / Exclude areas against the analytic lens within 0.1 %; the curves are curves, an untouched quarter is the
 *     very same Bézier; Divide makes the three faces, Trim the two; two squares exact to a millionth; an even-odd operand keeps its hole
 *  B  Offset Path of a 10 mm square: mitre 144, round 100 + 40 + π, bevel 142, inset 64 mm² (0.1 %), drawn as lines and quarter arcs; the cut line round two circles is one
 *     outline with the holes filled
 *  C  Outline Stroke: a 10 mm line 1 mm wide is 10 + π/4 mm²; Round Corners: a 10 mm square at 2 mm is 100 − (4 − π)·4 mm²
 *  D  on the editor's items: the answer where the frontmost was, painted as Illustrator paints it; a turned rectangle and words as operands; a group is one operand
 *  E  Align (selection, key, charm, centre of area), Distribute (centres, even gaps, a 2 mm gap from the key), Centre on the charm
 *  F  Move / Rotate / Scale / Reflect by values (with Copy), words scaled take the scale into their size, Step and Repeat round the charm's centre
 *  G  compound paths made and released; Select Same / Inverse; lock and hide everything else, unlock and show all
 *  H  how long Pathfinder takes on the page's thread, by size (where the Worker takes over)
 *  W  the browser: the Pathfinder and Align flyouts on the property bar, the Object menu, the Offset dialog, Rotate with Copy then ⌘D (Transform Again) and ⌘D after
 *     anything else (Duplicate, as before); each one undo step; a big Unite runs in the Worker while the page stays free
 */
'use strict';
const assert = require('node:assert/strict'), path = require('node:path'), fs = require('node:fs'), http = require('node:http');
const ROOT = path.join(__dirname, '..', '..');
const CE = require(path.join(ROOT, 'charm-nest-charm-editor.js'));
const OPS = require(path.join(ROOT, 'charm-nest-ce-ops.js'));
const K = OPS.kernel, O = OPS.ops, M = CE.model, PT = 72 / 25.4, MM = 25.4 / 72;
const geo = M.createGeo();
let n = 0;
const ok = (c, label) => { assert.ok(c, label); n++; console.log('  ok   ' + label); };
const pct = (a, b) => (a / b - 1) * 100;
const within = (a, b, p) => Math.abs(pct(a, b)) <= p;
const mm2 = sps => Math.abs(K.signedArea(sps)) * MM * MM;
const kinds = sps => sps.map(s => s.map(o => o[0]).join('')).join('|');
const rect = (id, x0, y0, x1, y1, extra) => Object.assign({ id, kind: 'path', role: 'art', source: 'master', name: id, stroke: false, fill: true, subpaths: M.rectSP(x0 * PT, y0 * PT, x1 * PT, y1 * PT) }, extra || {});
const disc = (id, cx, cy, r, extra) => Object.assign({ id, kind: 'path', role: 'art', source: 'master', name: id, stroke: false, fill: true, subpaths: M.ellipseSP(cx * PT, cy * PT, r * PT, r * PT) }, extra || {});
const drawn = (items, id) => { const e = M.indexTree(items, geo).get(id); return e ? geo.drawn(e.it, e.P) : null; };
const toMm = b => b.map(v => v * MM);
const lens = (r, d) => 2 * r * r * Math.acos(d / (2 * r)) - (d / 2) * Math.sqrt(4 * r * r - d * d);

/* ── A · Pathfinder ── */
{
  const r = 20, d = 24, A = M.ellipseSP(50, 50, r, r), B = M.ellipseSP(50 + d, 50, r, r), L = lens(r, d), disk = Math.PI * r * r;
  const want = { unite: 2 * disk - L, minus: disk - L, intersect: L, exclude: 2 * disk - 2 * L };
  for (const op of ['unite', 'minus', 'intersect', 'exclude']) {
    const res = K.pathfinder([{ subpaths: A }, { subpaths: B }], op), a = Math.abs(K.signedArea(res.subpaths));
    ok(within(a, want[op], 0.1) && /^mc+h$/.test(kinds(res.subpaths).split('|')[0]), `A1 ${op}: ${a.toFixed(3)} pt² against ${want[op].toFixed(3)} (${pct(a, want[op]).toFixed(4)} %), all curves: ${kinds(res.subpaths)}`);
  }
  const u = K.pathfinder([{ subpaths: A }, { subpaths: B }], 'unite').subpaths[0];
  const keptQuarter = u.some(op => op[0] === 'c' && JSON.stringify(op.slice(1)) === JSON.stringify(B[0][1].slice(1)));
  ok(keptQuarter, 'A2 a quarter of the front circle that nothing cut is the very same Bézier (control points to the last digit)');
  const dv = K.pathfinder([{ subpaths: A }, { subpaths: B }], 'divide'), sum = dv.pieces.reduce((a, p) => a + Math.abs(K.signedArea(p.subpaths)), 0);
  ok(dv.pieces.length === 3 && within(sum, want.unite, 0.1) && dv.pieces.filter(p => p.of === 1).length === 2, `A3 Divide: 3 faces (${dv.pieces.map(p => Math.abs(K.signedArea(p.subpaths)).toFixed(1) + '@' + p.of).join(' ')}) adding up to the union`);
  const tr = K.pathfinder([{ subpaths: A }, { subpaths: B }], 'trim');
  ok(tr.pieces.length === 2 && within(Math.abs(K.signedArea(tr.pieces[0].subpaths)), want.minus, 0.1) && within(Math.abs(K.signedArea(tr.pieces[1].subpaths)), disk * 1.0002800, 0.1), 'A4 Trim: the back circle less the front one, the front one whole');
  const S1 = M.rectSP(0, 0, 30, 20), S2 = M.rectSP(10, 10, 40, 30);
  const sq = op => Math.abs(K.signedArea(K.pathfinder([{ subpaths: S1 }, { subpaths: S2 }], op).subpaths));
  ok(Math.abs(sq('unite') - 1000) < 1e-6 && Math.abs(sq('minus') - 400) < 1e-6 && Math.abs(sq('intersect') - 200) < 1e-6 && Math.abs(sq('exclude') - 800) < 1e-6, 'A5 two 30 × 20 squares overlapping 20 × 10: 1000 / 400 / 200 / 800 to a millionth of a pt²');
  const ring = [M.ellipseSP(0, 0, 30, 30)[0], M.ellipseSP(0, 0, 15, 15)[0]];   // both wound the same way: a hole only under even-odd
  const withHole = Math.abs(K.signedArea(K.pathfinder([{ subpaths: ring, evenOdd: true }, { subpaths: M.rectSP(100, 100, 101, 101) }], 'unite').subpaths)) - 1;
  ok(within(withHole, Math.PI * (900 - 225), 0.1), `A6 an even-odd operand keeps its hole through Unite: ${withHole.toFixed(2)} pt²`);
}

/* ── B · Offset Path, the cut line round the art ── */
{
  const sq = M.rectSP(0, 0, 10 * PT, 10 * PT);
  for (const [join, d, want, shape] of [['miter', 1, 144, /^mlllh$/], ['round', 1, 100 + 40 + Math.PI, /^m(lc){3}lch$/], ['bevel', 1, 142, /^ml{7}h$/], ['miter', -1, 64, /^mlllh$/], ['round', -1, 64, /^mlllh$/]]) {
    const sp = K.offsetPath(sq, d * PT, join, 4, false), a = mm2(sp);
    ok(within(a, want, 0.1) && shape.test(kinds(sp)), `B1 a 10 mm square offset ${d > 0 ? '+' : ''}${d} mm, ${join}: ${a.toFixed(4)} mm² against ${want.toFixed(4)} (${pct(a, want).toFixed(4)} %), drawn ${kinds(sp)}`);
  }
  const two = [M.ellipseSP(0, 0, 3 * PT, 3 * PT)[0], M.ellipseSP(7 * PT, 0, 3 * PT, 3 * PT)[0], K.reverseSub(M.ellipseSP(0, 0, 1 * PT, 1 * PT)[0])];
  const cut = K.offsetPath({ subpaths: two }, 0.8 * PT, 'round', 4, false, true), want = 2 * Math.PI * 3.8 * 3.8 - lens(3.8, 7);
  ok(cut.length === 1 && within(mm2(cut), want, 0.1), `B2 the cut line round two circles 1 mm apart (one with a hole) at 0.8 mm: ONE outline, holes filled, ${mm2(cut).toFixed(3)} mm² against ${want.toFixed(3)}`);
}

/* ── C · Outline Stroke, Round Corners ── */
{
  const os = K.outlineStroke(M.lineSP(0, 0, 10 * PT, 0), 1 * PT);
  ok(within(mm2(os), 10 + Math.PI / 4, 0.1), `C1 Outline Stroke: a 10 mm line 1 mm wide covers ${mm2(os).toFixed(4)} mm² (10 + π/4 = ${(10 + Math.PI / 4).toFixed(4)}), drawn ${kinds(os)}`);
  const rc = K.roundCorners(M.rectSP(0, 0, 10 * PT, 10 * PT), 2 * PT), want = 100 - (4 - Math.PI) * 4;
  ok(within(mm2(rc), want, 0.1) && kinds(rc) === 'mlclclclch', `C2 Round Corners 2 mm on a 10 mm square: ${mm2(rc).toFixed(4)} mm² against ${want.toFixed(4)}, four lines and four arcs`);
  const big = K.roundCorners(M.rectSP(0, 0, 4 * PT, 10 * PT), 5 * PT), wantBig = 40 - (4 - Math.PI) * 4;
  ok(within(mm2(big), wantBig, 0.1), `C3 a radius too big for the short side takes what fits (half of each side): ${mm2(big).toFixed(3)} mm² against ${wantBig.toFixed(3)}`);
}

/* ── D · Pathfinder on the editor's items ── */
{
  const items = [rect('back', 0, 0, 10, 10, { role: 'cut', stroke: true, fill: false, lwPt: 0.5, layer: 'CUT' }), rect('mid', 20, 0, 30, 4), rect('front', 5, 5, 15, 15, { role: 'hatch', fill: true })];
  const u = O.pathfinderItems(items, ['front', 'back'], 'unite', geo);
  ok(u.items.length === 2 && u.items[0].id === 'mid' && u.items[1].role === 'hatch' && u.sel[0] === u.items[1].id && Math.abs(mm2(u.items[1].subpaths) - 175) < 1e-3,
    'D1 Unite: one hatched shape (the frontmost\'s paint) where the frontmost was, 175 mm²; the one between them untouched');
  const m = O.pathfinderItems(items, ['front', 'back'], 'minus', geo);
  ok(m.items[0].id === m.sel[0] && m.items[1].id === 'mid' && m.items[0].role === 'cut' && m.items[0].layer === 'CUT' && Math.abs(mm2(m.items[0].subpaths) - 75) < 1e-3,
    'D2 Minus Front keeps the backmost\'s place, its paint and whatever it carries (its laser layer): a cut line, 75 mm²');
  const turned = [rect('a', 0, 0, 10, 2), rect('b', 0, 0, 10, 2, { rot: 90, tx: 0, ty: 0 })];
  const plus = O.pathfinderItems(turned, ['a', 'b'], 'unite', geo);
  ok(Math.abs(mm2(plus.items[0].subpaths) - 36) < 1e-3, 'D3 a turned rectangle is cut where it is drawn: two 10 × 2 bars crossed make a plus of 36 mm²');
  const words = { id: 'w', kind: 'text', role: 'hatch', source: 'added', text: 'HI', fontKey: 'sans', sizePt: 20, align: 'left', tx: 0, ty: 30 };
  const wt = O.pathfinderItems([rect('slab', -1, 0, 15, 12), words], ['slab', 'w'], 'minus', geo);
  ok(!wt.error && wt.items.length === 1 && mm2(wt.items[0].subpaths) < 16 * 12 && mm2(wt.items[0].subpaths) > 16 * 12 * 0.6, `D4 words are an operand by their outlines: a slab less "HI" (${mm2(wt.items[0].subpaths).toFixed(2)} of 192 mm²)`);
  const grp = { id: 'g', kind: 'group', role: 'art', name: 'Two', children: [rect('g1', 0, 0, 4, 4), rect('g2', 6, 0, 10, 4)] };
  const gi = O.pathfinderItems([grp, rect('bar', 2, 1, 8, 3)], ['g', 'bar'], 'intersect', geo);
  ok(Math.abs(mm2(gi.items[0].subpaths) - 8) < 1e-3, 'D5 a group is ONE operand (its parts united): a bar meets the two squares in 2 × 4 mm²');
  const dv = O.pathfinderItems(items, ['back', 'front'], 'divide', geo), g = dv.items.find(i => i.id === dv.sel[0]);
  ok(g.kind === 'group' && g.children.length === 3 && g.children.map(c => c.role).sort().join() === 'cut,hatch,hatch', 'D6 Divide answers with a group of its pieces, each painted as the shape it came from');
  ok(O.pathfinderItems(items, ['back'], 'minus', geo).error, 'D7 one thing alone cannot be minused: a reason, not a change');
}

/* ── E · Align and Distribute ── */
const heartSP = (cx, cy, s) => { const q = (x, y) => [(cx + x * s) * PT, (cy + y * s) * PT]; return [[['m', q(0, 0.9)], ['c', q(-0.6, 0.45), q(-1, 0.05), q(-1, -0.35)], ['c', q(-1, -0.8), q(-0.55, -1), q(-0.3, -1)], ['c', q(-0.12, -1), q(0, -0.85), q(0, -0.65)], ['c', q(0, -0.85), q(0.12, -1), q(0.3, -1)], ['c', q(0.55, -1), q(1, -0.8), q(1, -0.35)], ['c', q(1, 0.05), q(0.6, 0.45), q(0, 0.9)], ['h']]]; };
{
  const frame = { x0: 0, y0: 0, wPt: 30 * PT, hPt: 30 * PT };
  const outline = { id: 'outline', kind: 'path', role: 'cut', name: 'Outline', stroke: true, fill: false, subpaths: heartSP(15, 15, 14), locked: true };
  const items = [outline, rect('a', 2, 2, 6, 4), rect('b', 8, 5, 10, 9), rect('c', 20, 10, 23, 11)];
  const L = O.alignItems(items, ['a', 'b', 'c'], geo, 'left', { to: 'selection' });
  ok(['a', 'b', 'c'].every(id => Math.abs(drawn(L.items, id)[0] - 2 * PT) < 1e-9), 'E1 align left to the selection: every left edge at the leftmost (2 mm)');
  const K2 = O.alignItems(items, ['a', 'b', 'c'], geo, 'bottom', { to: 'key', key: 'b' });
  ok(['a', 'c'].every(id => Math.abs(drawn(K2.items, id)[3] - 9 * PT) < 1e-9) && K2.items.find(i => i.id === 'b') === items[2], 'E2 align bottoms to the key object: the key stays, the rest come to its 9 mm');
  const ob = toMm(geo.drawn(outline));
  const C1 = O.alignItems(items, ['c'], geo, 'hcenter', { to: 'charm' });
  ok(Math.abs((drawn(C1.items, 'c')[0] + drawn(C1.items, 'c')[2]) / 2 * MM - (ob[0] + ob[2]) / 2) < 1e-9, 'E3 align to the charm: one thing alone centred across its outline\'s box');
  const cen = O.charmCentre(items, geo, frame).map(v => v * MM);
  const C2 = O.alignItems(items, ['c'], geo, 'vcenter', { to: 'centre' }), cy = (drawn(C2.items, 'c')[1] + drawn(C2.items, 'c')[3]) / 2 * MM;
  ok(Math.abs(cy - cen[1]) < 1e-9 && Math.abs(cen[1] - (ob[1] + ob[3]) / 2) > 0.5 && Math.abs(cen[0] - 15) < 0.01, `E4 align to the charm's centre of area: a heart balances at ${cen[1].toFixed(2)} mm down, not its box's ${((ob[1] + ob[3]) / 2).toFixed(2)}`);
  const D = O.distributeItems(items, ['a', 'b', 'c'], geo, 'hcenter', {}), cs = ['a', 'b', 'c'].map(id => { const b = drawn(D.items, id); return (b[0] + b[2]) / 2 * MM; }).sort((x, y) => x - y);
  ok(Math.abs(cs[1] - cs[0] - (cs[2] - cs[1])) < 1e-9, `E5 distribute centres: ${cs.map(v => v.toFixed(2)).join(' / ')} mm, equal steps`);
  const S = O.distributeItems(items, ['a', 'b', 'c'], geo, 'hspace', {}), bs = ['a', 'b', 'c'].map(id => toMm(drawn(S.items, id))).sort((x, y) => x[0] - y[0]);
  ok(Math.abs((bs[1][0] - bs[0][2]) - (bs[2][0] - bs[1][2])) < 1e-9, 'E6 distribute spacing: the two gaps equal');
  const G = O.distributeItems(items, ['a', 'b', 'c'], geo, 'hspace', { gapMm: 2, key: 'b' }), gs = ['a', 'b', 'c'].map(id => toMm(drawn(G.items, id))).sort((x, y) => x[0] - y[0]);
  ok(Math.abs(gs[1][0] - gs[0][2] - 2) < 1e-9 && Math.abs(gs[2][0] - gs[1][2] - 2) < 1e-9 && G.items.find(i => i.id === 'b') === items[2], 'E7 spacing by a value: 2 mm gaps either side of the key, which stays');
  const CC = O.centreOnCharm(items, ['a', 'b'], geo, frame), u = M.union(drawn(CC.items, 'a'), drawn(CC.items, 'b')).map(v => v * MM);
  ok(Math.abs((u[0] + u[2]) / 2 - cen[0]) < 1e-9 && Math.abs((u[1] + u[3]) / 2 - cen[1]) < 1e-9 && Math.abs((drawn(CC.items, 'b')[0] - drawn(CC.items, 'a')[0]) * MM - 6) < 1e-9, 'E8 Centre on the charm: the selection, kept together, on the heart\'s centre of area');
}

/* ── F · Transform by values, Step and Repeat ── */
{
  const items = [rect('bar', 10, 10, 20, 12)];
  const r = O.transformItems(items, ['bar'], geo, { deg: 90 }, { copy: true, pivot: 'selection' });
  const copy = r.items[1], cb = toMm(drawn(r.items, copy.id));
  ok(r.items.length === 2 && r.items[0] === items[0] && copy.source === 'added' && r.sel[0] === copy.id && Math.abs(cb[2] - cb[0] - 2) < 1e-9 && Math.abs(cb[3] - cb[1] - 10) < 1e-9 && Math.abs((cb[0] + cb[2]) / 2 - 15) < 1e-9,
    'F1 Rotate 90° with Copy: the original stays, the copy (new id, selected, just in front) stands upright about the same middle');
  const f = O.transformItems(items, ['bar'], geo, { reflect: 'v' }, { pivot: 'charm', frame: { x0: 0, y0: 0, wPt: 30 * PT, hPt: 30 * PT } });
  const fb = toMm(drawn(f.items, 'bar'));
  const fm = geo.matrix(f.items[0]);
  ok(Math.abs(fb[0] - 10) < 1e-9 && Math.abs(fb[2] - 20) < 1e-9 && fm[0] * fm[3] - fm[1] * fm[2] < 0, 'F2 Reflect across the charm\'s upright middle (no cut line: the frame\'s): 10..20 mm stays 10..20, mirrored');
  const mv = O.transformItems(items, ['bar'], geo, { dx: 1.5 * PT, dy: -2 * PT }, {});
  ok(Math.abs(toMm(drawn(mv.items, 'bar'))[0] - 11.5) < 1e-9 && Math.abs(toMm(drawn(mv.items, 'bar'))[1] - 8) < 1e-9, 'F3 Move by values: 1.5 mm across, 2 mm up');
  const words = [{ id: 'w', kind: 'text', role: 'hatch', text: 'AB', fontKey: 'sans', sizePt: 10, align: 'left', tx: 50, ty: 50 }];
  const sc = O.transformItems(words, ['w'], geo, { sx: 1.5, sy: 1.5 }, { pivot: 'each' });
  ok(Math.abs(sc.items[0].sizePt - 15) < 1e-9 && sc.items[0].sx === 1, 'F4 Scale 150 % of words takes the scale into their size: 10 pt becomes 15 pt');
  const frame = { x0: 0, y0: 0, wPt: 30 * PT, hPt: 30 * PT };
  const ringItems = [{ id: 'outline', kind: 'path', role: 'cut', stroke: true, subpaths: M.ellipseSP(15 * PT, 15 * PT, 14 * PT, 14 * PT), locked: true }, disc('dot', 15, 4, 1)];
  const sr = O.stepRepeatItems(ringItems, ['dot'], geo, { deg: 60 }, { count: 5, pivot: 'charm', frame });
  const radii = sr.sel.map(id => { const b = toMm(drawn(sr.items, id)); return Math.hypot((b[0] + b[2]) / 2 - 15, (b[1] + b[3]) / 2 - 15); });
  ok(sr.sel.length === 6 && radii.every(rr => Math.abs(rr - 11) < 1e-6), `F5 Step and Repeat: 5 copies turned 60° each about the charm's middle, a ring of 6 dots all 11 mm from it`);
  const again = O.transformItems(sr.items, [sr.sel[5]], geo, { deg: 60 }, { copy: true, pivot: 'charm', frame });
  const back = toMm(drawn(again.items, again.sel[0]));
  ok(Math.abs((back[0] + back[2]) / 2 - 15) < 1e-6 && Math.abs((back[1] + back[3]) / 2 - 4) < 1e-6, 'F6 the same transform again on the last copy closes the ring where it began (Transform Again\'s arithmetic)');
}

/* ── G · compound paths, Select Same, lock and hide ── */
{
  const items = [rect('outer', 0, 0, 10, 10), rect('inner', 3, 3, 7, 7, { role: 'hatch' })];
  const cp = O.makeCompound(items, ['outer', 'inner'], geo), path1 = cp.items[0];
  ok(cp.items.length === 1 && path1.subpaths.length === 2 && path1.even === true && path1.role === 'art' && Math.abs(mm2(path1.subpaths) - 84) < 1e-6,
    'G1 Make Compound Path: one path of two rings, the inner a hole (84 mm², wound so non-zero agrees with even-odd), the backmost\'s paint');
  const rel = O.releaseCompound(cp.items, [path1.id], geo);
  ok(rel.items.length === 2 && rel.items.every(i => i.subpaths.length === 1 && !i.even) && Math.abs(mm2(rel.items[1].subpaths) - 16) < 1e-6, 'G2 Release Compound Path: two paths again, the hole a shape of its own');
  const many = [rect('r1', 0, 0, 1, 1, { role: 'line', stroke: true, fill: false, lwPt: 0.3 }), rect('r2', 2, 0, 3, 1, { role: 'line', stroke: true, fill: false, lwPt: 0.3 }), rect('r3', 4, 0, 5, 1, { role: 'line', stroke: true, fill: false, lwPt: 0.6 }),
    { id: 'grp', kind: 'group', role: 'hatch', children: [rect('h1', 0, 5, 1, 6, { role: 'hatch' }), rect('h2', 2, 5, 3, 6, { role: 'line', stroke: true, fill: false, lwPt: 0.3 })] }, rect('hid', 6, 0, 7, 1, { role: 'line', stroke: true, fill: false, lwPt: 0.3, hidden: true })];
  ok(O.selectSame(many, ['r1'], geo, 'role').sel.join() === 'r1,r2,r3,h2', 'G3 Select Same role: every line engraving, inside groups too, never a hidden one');
  ok(O.selectSame(many, ['r1'], geo, 'stroke').sel.join() === 'r1,r2,h2', 'G4 Select Same line: the same role, colour and width (0.6 pt is not 0.3)');
  ok(O.selectInverse(many, ['r1', 'h1'], geo).sel.join() === 'r2,r3', 'G5 Select Inverse: what is not selected and holds nothing selected');
  const lk = O.flagOthers(many, ['r1'], geo, 'locked');
  ok(lk.items.filter(i => i.locked).map(i => i.id).join() === 'r2,r3,grp,hid', 'G6 Lock everything else');
  const un = O.clearFlagAll(lk.items, 'locked'), sh = O.clearFlagAll(many, 'hidden');
  ok(!un.items.some(i => i.locked) && !sh.items.some(i => i.hidden) && O.clearFlagAll(many, 'locked').error, 'G7 Unlock all, Show all (and a reason when there is nothing to do)');
}

/* ── H · Pathfinder's time on the page's own thread, by size ── */
const sizes = [];
{
  const ringOf = (count, phase) => { const out = []; for (let i = 0; i < count; i++) { const a = (i + phase) / count * 2 * Math.PI; out.push(...M.ellipseSP(100 + 60 * Math.cos(a), 100 + 60 * Math.sin(a), 6, 6)); } return out; };
  K.pathfinder([{ subpaths: ringOf(4, 0) }, { subpaths: ringOf(4, 0.5) }], 'unite');   // (warm)
  for (const count of [4, 10, 30]) {
    const A = ringOf(count, 0), B = ringOf(count, 0.5);
    let best = Infinity; for (let i = 0; i < 3; i++) { const t = process.hrtime.bigint(); K.pathfinder([{ subpaths: A }, { subpaths: B }], 'unite'); best = Math.min(best, Number(process.hrtime.bigint() - t) / 1e6); }
    sizes.push({ count, ms: best });
    console.log(`  ..   Unite of two rings of ${count} circles (${count * 2} circles): ${best.toFixed(1)} ms`);
  }
  ok(sizes[0].ms < 50, `H1 a charm-sized Unite (eight circles) takes ${sizes[0].ms.toFixed(1)} ms of the thread, under 50 ms (bigger than 3000 chord corners goes to the Worker)`);
}

console.log(`ce-ops node: ${n} checks passed`);

/* ── W · the browser ── */
(async () => {
  let chromium; try { ({ chromium } = require(path.join(process.env.PW_DIR || path.join(ROOT, 'node_modules'), 'playwright-core'))); } catch (_) { console.log('  - no playwright-core: the browser pass was not run'); return; }
  if (!process.env.CHROMIUM && !fs.existsSync('/opt/pw-browsers/chromium-1243/chrome-linux64/chrome')) { console.log('  - no Chromium: the browser pass was not run'); return; }
  // (the harness page served from 127.0.0.1, so the Worker can load its scripts as on the site; nothing leaves the machine)
  const types = { '.html': 'text/html', '.js': 'text/javascript', '.css': 'text/css' };
  const server = http.createServer((req, res) => {
    const p = path.normalize(path.join(ROOT, decodeURIComponent(req.url.split('?')[0])));
    if (!p.startsWith(ROOT) || !fs.existsSync(p) || fs.statSync(p).isDirectory()) { res.writeHead(404); res.end(); return; }
    res.writeHead(200, { 'content-type': types[path.extname(p)] || 'application/octet-stream' }); fs.createReadStream(p).pipe(res);
  });
  await new Promise(r => server.listen(0, '127.0.0.1', r));
  const base = `http://127.0.0.1:${server.address().port}/`;
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1243/chrome-linux64/chrome', args: ['--no-sandbox'] });
  const failures = [];
  const step = async (name, fn) => { try { await fn(); n++; console.log('  ok   ' + name); } catch (e) { failures.push(name); console.log('  FAIL ' + name + '\n         ' + String(e && e.stack || e).split('\n').slice(0, 8).join('\n         ')); } };
  try {
    const ctxB = await browser.newContext({ viewport: { width: 1280, height: 800 }, reducedMotion: 'reduce' });
    await ctxB.route(u => !u.href.startsWith(base), r => r.abort());
    const page = await ctxB.newPage();
    const errs = []; page.on('pageerror', e => errs.push(e.message)); page.on('console', m => { if (m.type() === 'error') errs.push(m.text()); });
    await page.goto(base + 'tests/charm-nest/fixtures/charm-editor-harness.html');
    await page.addScriptTag({ url: base + 'charm-nest-vector.js' }); await page.addScriptTag({ url: base + 'charm-nest-ce-ops.js' });
    const settle = () => page.evaluate(() => new Promise(r => requestAnimationFrame(() => requestAnimationFrame(r))));
    await page.evaluate(() => { window.openEditor(); }); await page.waitForSelector('.ce-root .ce-items path'); await settle();
    const C = (fn, arg) => page.evaluate(([src, a]) => (new Function('C', 'a', 'return (' + src + ')(C, a)'))(window.CharmEditor.current.ctx, a), [fn.toString(), arg]);
    const hist = () => page.evaluate(() => window.CharmEditor.current.history().undo);
    const client = (xmm, ymm) => page.evaluate(([x, y]) => window.CharmEditor.current.clientOf(window.fixture.X0 + x * window.fixture.PT, window.fixture.Y0 + y * window.fixture.PT), [xmm, ymm]);
    const areaMm = id => C((c, id) => { const e = c.entry(id); return Math.abs(window.CharmEditorOps.kernel.signedArea(c.model.outlinesOf(e.it, c.geo, e.P))) * Math.pow(25.4 / 72, 2); }, id);

    await step('W1 the Object menu lists the new commands by group (Pathfinder, Path, Transform, Align, Select, Lock and hide)', async () => {
      await page.click('.ce-root [data-drop="xcmd"] .ce-drop__b'); await settle();
      const heads = await page.$$eval('.ce-root [data-drop="xcmd"] .ce-menu .ce-menu__head', els => els.map(e => e.textContent));
      for (const h of ['Pathfinder', 'Path', 'Transform', 'Align', 'Select', 'Lock and hide']) assert.ok(heads.includes(h), 'menu heads: ' + heads.join(', '));
      await page.keyboard.press('Escape'); await settle();
    });
    let united = null;
    await step('W2 two overlapping rectangles drawn with the mouse, both selected, Unite from the Pathfinder flyout on the property bar: ONE shape, their union\'s area (0.1 %), one undo step, ⌘Z gives both back', async () => {
      await page.keyboard.press('r');
      const a = await client(5, 12), b = await client(11, 16), c2 = await client(8, 14), d2 = await client(14, 18);
      await page.mouse.move(a.x, a.y); await page.mouse.down(); await page.mouse.move(b.x, b.y, { steps: 6 }); await page.mouse.up(); await settle();
      const r1 = (await C(c => c.selection()))[0];
      await page.keyboard.press('r');
      await page.mouse.move(c2.x, c2.y); await page.mouse.down(); await page.mouse.move(d2.x, d2.y, { steps: 6 }); await page.mouse.up(); await settle();
      const r2 = (await C(c => c.selection()))[0];
      await page.keyboard.down('Shift'); const e1 = await client(5, 13); await page.mouse.click(e1.x, e1.y); await page.keyboard.up('Shift'); await settle();
      assert.deepEqual((await C(c => c.selection())).sort(), [r1, r2].sort());
      // (the two as drawn: the editor snaps a shape being drawn to the grid and the guides, so the hand's 6 × 4 mm may land a little off; the answer must be THEIR union)
      const bx = await C((c, ids) => ids.map(id => { const e = c.entry(id); return c.geo.drawn(e.it, e.P).map(v => v * 25.4 / 72); }), [r1, r2]);
      const ov = Math.max(0, Math.min(bx[0][2], bx[1][2]) - Math.max(bx[0][0], bx[1][0])) * Math.max(0, Math.min(bx[0][3], bx[1][3]) - Math.max(bx[0][1], bx[1][1]));
      const want = bx.reduce((a, b) => a + (b[2] - b[0]) * (b[3] - b[1]), 0) - ov;
      const n0 = (await C(c => c.items().length)), h0 = await hist();
      await page.click('.ce-root .ce-xbar [data-xcmd="ops.pfBar"]'); await settle();
      assert.ok(await page.isVisible('.ce-root .cops-fly [data-ops="pf:unite"]'), 'the flyout opened');
      // (the work on the page's thread: the click handler, timed inside the page)
      const ms = await page.evaluate(() => { const t = performance.now(); document.querySelector('.ce-root .cops-fly [data-ops="pf:unite"]').click(); return performance.now() - t; }); await settle();
      const sel = await C(c => c.selection()); united = sel[0];
      assert.equal(await C(c => c.items().length), n0 - 1); assert.equal(await hist(), h0 + 1);
      const ar = await areaMm(united); assert.ok(ov > 0 && Math.abs(ar - want) < want * 0.001, `area ${ar} against ${want}`);
      console.log(`  ..   Unite on the page: ${ar.toFixed(4)} mm² (the two drawn: ${want.toFixed(4)}), ${ms.toFixed(1)} ms of the page's thread`);
      assert.ok(ms < 50, 'Unite took ' + ms.toFixed(1) + ' ms of the page');
      await page.keyboard.press('Control+z'); await settle();
      assert.equal(await C(c => c.items().length), n0);
      await page.keyboard.press('Control+Shift+z'); await settle();
      assert.equal(await C(c => c.items().length), n0 - 1); united = (await C(c => c.items()[c.items().length - 1].id));
    });
    await step('W3 the Align flyout: to the charm\'s centre of area, centred across: one step', async () => {
      await C((c, id) => c.select([id]), united); await settle();
      await page.click('.ce-root .ce-xbar [data-xcmd="ops.alignBar"]'); await settle();
      await page.click('.ce-root .cops-fly [data-ops="to:centre"]'); await settle();
      const h0 = await hist();
      await page.click('.ce-root .cops-fly [data-ops="al:hcenter"]'); await settle();
      const b = await C((c, id) => { const e = c.entry(id); return c.geo.drawn(e.it, e.P); }, united), cx = await page.evaluate(() => window.fixture.X0 + 12 * window.fixture.PT);
      assert.ok(Math.abs((b[0] + b[2]) / 2 - cx) < 0.01, 'centred: ' + ((b[0] + b[2]) / 2 - cx)); assert.equal(await hist(), h0 + 1);
      await page.keyboard.press('Escape'); await settle();
      assert.equal(await page.$('.ce-root .cops-fly:not([hidden])'), null, 'Esc folds the flyout');
    });
    await step('W4 Offset Path… from the Object menu: 0.5 mm typed, Enter: a new path behind the shape, 0.5 mm bigger all round, one step', async () => {
      await C((c, id) => c.select([id]), united); await settle();
      const b0 = await C((c, id) => { const e = c.entry(id); return c.geo.drawn(e.it, e.P); }, united), n0 = await C(c => c.items().length), h0 = await hist();
      await page.click('.ce-root [data-drop="xcmd"] .ce-drop__b'); await settle();
      await page.click('.ce-root [data-drop="xcmd"] .ce-menu [data-xcmd="ops.offset"]'); await settle();
      await page.waitForSelector('.ce-root .cops-card [data-dnum="mm"]');
      await page.click('.ce-root .cops-card [data-dseg="join"][data-v="miter"]');
      await page.fill('.ce-root .cops-card [data-dnum="mm"]', '0.5'); await page.keyboard.press('Enter'); await settle();
      const its = await C(c => c.items().map(i => i.id)), sel = (await C(c => c.selection()))[0];
      assert.equal(its.length, n0 + 1); assert.equal(its.indexOf(sel), its.indexOf(united) - 1, 'just behind it'); assert.equal(await hist(), h0 + 1);
      const b1 = await C((c, id) => { const e = c.entry(id); return c.geo.drawn(e.it, e.P); }, sel), PTm = 72 / 25.4;
      assert.ok([b0[0] - b1[0], b0[1] - b1[1], b1[2] - b0[2], b1[3] - b0[3]].every(v => Math.abs(v - 0.5 * PTm) < 0.01), 'grown 0.5 mm: ' + [b0, b1].join(' / '));
    });
    await step('W5 Rotate… 30° with Copy, then ⌘D twice: Transform Again turns copy after copy (each one step); after a nudge ⌘D duplicates as before', async () => {
      await C(c => c.select(['heart-fill'])); await settle();
      const n0 = await C(c => c.items().length), h0 = await hist();
      await page.click('.ce-root [data-drop="xcmd"] .ce-drop__b'); await settle();
      await page.click('.ce-root [data-drop="xcmd"] .ce-menu [data-xcmd="ops.rotate"]'); await settle();
      await page.fill('.ce-root .cops-card [data-dnum="deg"]', '30');
      await page.click('.ce-root .cops-card [data-db="copy"]'); await settle();
      const rot = () => C(c => { const id = c.selection()[0]; return c.entry(id).it.rot; });
      assert.equal(await C(c => c.items().length), n0 + 1); assert.ok(Math.abs(await rot() - 30) < 1e-6, 'rot ' + await rot());
      await page.keyboard.press('Control+d'); await settle();
      await page.keyboard.press('Control+d'); await settle();
      assert.equal(await C(c => c.items().length), n0 + 3); assert.ok(Math.abs(await rot() - 90) < 1e-6, 'rot ' + await rot()); assert.equal(await hist(), h0 + 3);
      await page.keyboard.press('ArrowRight'); await settle();
      const r0 = await rot();
      await page.keyboard.press('Control+d'); await settle();
      assert.equal(await C(c => c.items().length), n0 + 4); assert.ok(Math.abs(await rot() - r0) < 1e-6, 'a duplicate keeps the turn: ' + await rot());
    });
    await step('W6 a big Unite (two rings of 240 circles, ~44 000 chord corners) runs in the Worker: the click returns at once, the answer lands as one step', async () => {
      const ids = await C(c => {
        const M = c.model, K = 72 / 25.4, mk = ph => { const out = []; for (let i = 0; i < 240; i++) { const a = (i + ph) / 240 * 2 * Math.PI; out.push(...M.ellipseSP(100 + 12 * K + 9 * K * Math.cos(a), 50 + 15 * K + 9 * K * Math.sin(a), 0.6 * K, 0.6 * K)); } return out; };
        const got = c.addItems([{ kind: 'path', role: 'hatch', name: 'Ring A', fill: true, subpaths: mk(0) }, { kind: 'path', role: 'hatch', name: 'Ring B', fill: true, subpaths: mk(0.5) }]); c.commit('rings'); return got;
      });
      await C((c, ids) => c.select(ids), ids); await settle();
      const n0 = await C(c => c.items().length), h0 = await hist();
      const { ms, cue } = await page.evaluate(() => { const b = document.querySelector('.ce-root .ce-xbar [data-xcmd="ops.pfBar"]'); b.click(); const t = performance.now(); document.querySelector('.ce-root .cops-fly [data-ops="pf:unite"]').click(); return { ms: performance.now() - t, cue: (document.querySelector('.ce-root .ce-say') || {}).textContent || '' }; });
      assert.match(cue, /working/, 'the work area says it is working: ' + cue);
      await page.waitForFunction(n => window.CharmEditor.current.ctx.items().length === n - 1, n0, { timeout: 20000 }); await settle();
      assert.ok(ms < 50, 'the click took ' + ms.toFixed(1) + ' ms of the page'); assert.equal(await hist(), h0 + 1);
      const sp = await C(c => c.entry(c.selection()[0]).it.subpaths.length);
      console.log(`  ..   the big Unite: the click took ${ms.toFixed(1)} ms of the page's thread (the work in the Worker), ${sp} rings came back`);
      assert.ok(sp >= 2);
    });
    await step('W7 no errors on the page', async () => { assert.deepEqual(errs, []); });
    await ctxB.close();
  } finally { await browser.close(); server.close(); }
  console.log(`ce-ops: ${n} checks passed${failures.length ? `, ${failures.length} FAILED: ${failures.join(' · ')}` : ''}`);
  if (failures.length) process.exitCode = 1;
})().catch(e => { console.error(e); process.exitCode = 1; });
