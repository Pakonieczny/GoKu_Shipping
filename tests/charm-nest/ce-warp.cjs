// BLENDS AND WARPS IN THE CHARM EDITOR (charm-nest-ce-warp.js; Paul, 28 Oct 2026: "designing custom charms, which requires blinds and deforming lines"; the lead's D3:
// blinds = BLENDS, deforming lines = warp / envelope presets, free distort, perspective). No server, no network, no live service.
//   $SP/int/slot.sh node tests/charm-nest/ce-warp.cjs        (the maths run in node first; the browser part needs PW_DIR / CHROMIUM, which slot.sh sets)
// A the maths, in node
//   A1 an Arc warp of a known 20 × 5 mm rectangle lands on the analytic annular sector: its top and bottom on the two circles within 0.01 mm, its corners exactly where
//      the sector's are, its sides straight radii; −50 % is the same bowed down; curves stay curves (a handful of cubics, no polyline)
//   A2 every one of the fifteen styles, with both distortions and turned vertical, and Free Distort (projective and bilinear): the paths made are within 0.01 mm of the
//      true image of a shape of lines and curves, measured both ways (no true point far from the path made, no point made far from the true image)
//   A3 Free Distort keeps straight lines straight (a rectangle stays four lines); Perspective moves the partner corner the other way
//   A4 a 5-step blend: square into a bigger, turned-round square: every step an exact square, sides growing evenly, centres evenly along the line; square into circle:
//      the middle step's box is halfway, its corners met at 45° (no twist)
//   A5 along a spine: the steps' centres on the spine, evenly spaced along it; Specified Distance makes the count from the length; Align to Path turns them
//   A6 compound shapes: a ring blended into a disc, the hole shrinking into a point
//   A7 the work: a dense drawing warped at 0.01 mm, ms printed
// B in a real Chromium page (fixtures/charm-editor-harness.html with this file added): Warp on the filled heart (the panel, the live preview with the heart ghosted, OK
//   is one undo step, the warp kept so Warp starts again from the original, Expand), words warped into outlines on the back, the Blend tool (W: two clicks, the
//   panel, 3 steps, one group, undo), Free Distort by a corner, Esc cancels, preview frames measured in main-thread ms, no page errors
'use strict';
const path = require('path'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');
const CW = require(path.join(root, 'charm-nest-ce-warp.js')), W = CW.model;
const PT = 72 / 25.4, MM = 25.4 / 72, TOL = 0.01 * PT;
const results = [], failures = [];
const pass = m => { results.push(m); console.log('  ok   ' + m); };
const fail = (m, e) => { failures.push(m + ' :: ' + String(e && e.message || e).split('\n')[0].slice(0, 400)); console.log('  FAIL ' + m + '\n         ' + String(e && e.stack || e).split('\n').slice(0, 10).join('\n         ')); };
const stepSync = (name, fn) => { try { const r = fn(); pass(name + (r ? ' · ' + r : '')); } catch (e) { fail(name, e); } };
const near = (a, b, eps) => Math.abs(a - b) <= eps;
const bez = (a0, a1, a2, a3, t) => { const u = 1 - t; return u * u * u * a0 + 3 * u * u * t * a1 + 3 * u * t * t * a2 + t * t * t * a3; };
const rect = (x0, y0, x1, y1) => [[['m', [x0, y0]], ['l', [x1, y0]], ['l', [x1, y1]], ['l', [x0, y1]], ['h']]];
const K = 0.5522847498;
const circle = (cx, cy, r, ccw) => {
  const k = r * K;
  const sp = [['m', [cx + r, cy]], ['c', [cx + r, cy + k], [cx + k, cy + r], [cx, cy + r]], ['c', [cx - k, cy + r], [cx - r, cy + k], [cx - r, cy]], ['c', [cx - r, cy - k], [cx - k, cy - r], [cx, cy - r]], ['c', [cx + k, cy - r], [cx + r, cy - k], [cx + r, cy]], ['h']];
  return [ccw ? reverse(sp) : sp];
};
function reverse(sp) {   // one closed subpath drawn the other way round
  const pts = []; let cur = null; const segs = [];
  for (const op of sp) { if (op[0] === 'm') cur = op[1]; else if (op[0] === 'l') { segs.push(['l', cur, op[1]]); cur = op[1]; } else if (op[0] === 'c') { segs.push(['c', cur, op[1], op[2], op[3]]); cur = op[3]; } }
  const out = [['m', segs[segs.length - 1][segs[segs.length - 1].length - 1]]];
  for (let i = segs.length - 1; i >= 0; i--) { const s = segs[i]; out.push(s[0] === 'l' ? ['l', s[1]] : ['c', s[3], s[2], s[1]]); }
  out.push(['h']); void pts; return out;
}
/** Points along subpaths: every segment cut into n pieces (lines into 2). */
function pointsOf(sps, n) {
  const out = [];
  for (const sp of sps) {
    let cur = null, start = null;
    for (const op of sp) {
      if (op[0] === 'm') { cur = op[1]; start = cur; out.push(cur); }
      else if (op[0] === 'l') { for (let i = 1; i <= 8; i++) out.push([cur[0] + (op[1][0] - cur[0]) * i / 8, cur[1] + (op[1][1] - cur[1]) * i / 8]); cur = op[1]; }
      else if (op[0] === 'c') { for (let i = 1; i <= n; i++) { const t = i / n; out.push([bez(cur[0], op[1][0], op[2][0], op[3][0], t), bez(cur[1], op[1][1], op[2][1], op[3][1], t)]); } cur = op[3]; }
      else if (op[0] === 'h' && cur && start) { for (let i = 1; i <= 8; i++) out.push([cur[0] + (start[0] - cur[0]) * i / 8, cur[1] + (start[1] - cur[1]) * i / 8]); cur = start; }
    }
  }
  return out;
}
/** The true image of subpaths under f, densely: f of points along the source. */
function trueImage(sps, f, n) { return pointsOf(sps, n).map(p => f(p[0], p[1])); }
/** Polylines of subpaths (fine), for distances. */
function polylines(sps, n) {
  const out = [];
  for (const sp of sps) {
    let pl = [], cur = null, start = null;
    for (const op of sp) {
      if (op[0] === 'm') { if (pl.length > 1) out.push(pl); pl = [op[1]]; cur = op[1]; start = cur; }
      else if (op[0] === 'l') { pl.push(op[1]); cur = op[1]; }
      else if (op[0] === 'c') { for (let i = 1; i <= n; i++) { const t = i / n; pl.push([bez(cur[0], op[1][0], op[2][0], op[3][0], t), bez(cur[1], op[1][1], op[2][1], op[3][1], t)]); } cur = op[3]; }
      else if (op[0] === 'h') { pl.push(start); cur = start; }
    }
    if (pl.length > 1) out.push(pl);
  }
  return out;
}
function distToPolys(p, polys) {
  let best = Infinity;
  for (const pl of polys) for (let i = 0; i + 1 < pl.length; i++) {
    const a = pl[i], b = pl[i + 1], dx = b[0] - a[0], dy = b[1] - a[1], L = dx * dx + dy * dy;
    let t = L ? ((p[0] - a[0]) * dx + (p[1] - a[1]) * dy) / L : 0; t = Math.max(0, Math.min(1, t));
    const d = Math.hypot(p[0] - a[0] - t * dx, p[1] - a[1] - t * dy); if (d < best) best = d;
  }
  return best;
}
const boxOf = pts => { let b = [Infinity, Infinity, -Infinity, -Infinity]; for (const p of pts) b = [Math.min(b[0], p[0]), Math.min(b[1], p[1]), Math.max(b[2], p[0]), Math.max(b[3], p[1])]; return b; };
const anchors = sps => { const out = []; for (const sp of sps) for (const op of sp) { if (op[0] === 'm' || op[0] === 'l') out.push(op[1]); else if (op[0] === 'c') out.push(op[3]); } return out; };
const ops = (sps, k) => sps.reduce((n, sp) => n + sp.filter(op => op[0] === k).length, 0);

/* ════ A · the maths ════ */
console.log('A · the maths (node)');
// a 20 × 5 mm rectangle, placed off the origin the way a charm's view sits in its master
const X0 = 100, Y0 = 50, Wd = 20 * PT, Hd = 5 * PT, BOX = [X0, Y0, X0 + Wd, Y0 + Hd], RECT = rect(...BOX);
stepSync('A1 an Arc warp (bend 50 %) of a 20 × 5 mm rectangle lands on the analytic annular sector within 0.01 mm; corners exact; sides straight radii; −50 % bows down', () => {
  const th = 0.5 * Math.PI, Rm = Wd / th, cx = X0 + Wd / 2, cy = Y0 + Hd / 2, C = [cx, cy + Rm], rTop = Rm + Hd / 2, rBot = Rm - Hd / 2;
  const out = W.warpSubpaths(RECT, { style: 'arc', bend: 0.5 }, BOX, TOL);
  assert.equal(out.length, 1);
  const sp = out[0];
  // walk the ops: the top edge (from the first corner to the second), the right side (a line), the bottom edge, the left side (a line), closed
  const cs = sp.filter(op => op[0] === 'c').length, ls = sp.filter(op => op[0] === 'l').length;
  assert.equal(ls, 1, 'the right side a line (the left one is the close, straight, as it came in)'); assert.equal(sp[sp.length - 1][0], 'h'); assert.ok(cs >= 2 && cs <= 16, 'a handful of cubics: ' + cs);
  let worstTop = 0, worstBot = 0, cur = null, edge = 0, n = 0;
  for (const op of sp) {
    if (op[0] === 'm') { cur = op[1]; continue; }
    if (op[0] === 'l') { edge++; cur = op[1]; continue; }
    if (op[0] !== 'c') continue;
    for (let i = 0; i <= 200; i++) {
      const t = i / 200, x = bez(cur[0], op[1][0], op[2][0], op[3][0], t), y = bez(cur[1], op[1][1], op[2][1], op[3][1], t), r = Math.hypot(x - C[0], y - C[1]);
      if (edge === 0) worstTop = Math.max(worstTop, Math.abs(r - rTop)); else worstBot = Math.max(worstBot, Math.abs(r - rBot)); n++;
    }
    cur = op[3];
  }
  assert.ok(worstTop * MM <= 0.01 && worstBot * MM <= 0.01, `on the circles: top ${(worstTop * MM).toFixed(5)} mm, bottom ${(worstBot * MM).toFixed(5)} mm`);
  // the corners: at ±45° on the two circles
  const corner = (r, a) => [C[0] + r * Math.sin(a), C[1] - r * Math.cos(a)];
  const an = anchors(out), want = [corner(rTop, -th / 2), corner(rTop, th / 2), corner(rBot, th / 2), corner(rBot, -th / 2)];
  for (const w of want) assert.ok(an.some(a => Math.hypot(a[0] - w[0], a[1] - w[1]) < 1e-5), 'a corner where the sector has it: ' + w);
  // the sides: straight lines along radii (their ends at the same angle from the centre)
  const ang = p => Math.atan2(p[0] - C[0], C[1] - p[1]);
  assert.ok(near(ang(want[1]), ang(want[2]), 1e-9) && near(ang(want[0]), ang(want[3]), 1e-9), 'the sides are radii');
  // −50 %: the same sector bowed the other way, the mirror image about the box's middle line (the middle column stays where it was, the ends lift)
  const down = W.warpSubpaths(RECT, { style: 'arc', bend: -0.5 }, BOX, TOL), bd = boxOf(pointsOf(down, 40)), bu = boxOf(pointsOf(out, 40));
  assert.ok(near(bu[1], Y0, 1e-6) && bu[3] > Y0 + Hd + 1, 'bowed up: the middle stays, the ends droop');
  assert.ok([bu[0], 2 * cy - bu[3], bu[2], 2 * cy - bu[1]].every((v, i) => near(v, bd[i], 1e-6)), '−50 % is the mirror image: ' + bd.map(v => v.toFixed(3)));
  return `top ${(worstTop * MM * 1000).toFixed(2)} µm, bottom ${(worstBot * MM * 1000).toFixed(2)} µm off the circles (${n} points), ${cs} cubics + 2 straight sides`;
});

// a shape of lines and curves: a rounded tab with a hole and a slanted bar, about 18 × 9 mm
const SHAPE = (() => {
  const x0 = 102, y0 = 52, x1 = x0 + 18 * PT, y1 = y0 + 9 * PT, r = 2 * PT, k = r * K;
  const tab = [['m', [x0 + r, y0]], ['l', [x1 - r, y0]], ['c', [x1 - r + k, y0], [x1, y0 + r - k], [x1, y0 + r]], ['l', [x1, y1]], ['l', [x0, y1]], ['l', [x0, y0 + r]], ['c', [x0, y0 + r - k], [x0 + r - k, y0], [x0 + r, y0]], ['h']];
  return [tab, circle(x0 + 5 * PT, y0 + 4.5 * PT, 2 * PT, true)[0], [['m', [x0 + 9 * PT, y1 - 1 * PT]], ['l', [x1 - 2 * PT, y0 + 1.5 * PT]]]];
})();
const SBOX = (() => { const p = pointsOf(SHAPE, 64); return boxOf(p); })();
function bothWays(sps, f, label) {
  const made = W.mapSubpaths(sps, f, TOL);
  const truth = trueImage(sps, f, 64), madePolys = polylines(made, 64), truthPolys = polylines(W.mapSubpaths(sps, f, TOL / 20), 32);
  let a = 0, b = 0;
  for (const p of truth) a = Math.max(a, distToPolys(p, madePolys));
  for (const pl of madePolys) for (const p of pl) b = Math.max(b, distToPolys(p, truthPolys));
  assert.ok(a * MM <= 0.01 && b * MM <= 0.0101, `${label}: true → made ${(a * MM).toFixed(5)} mm, made → true ${(b * MM).toFixed(5)} mm`);
  return { a: a * MM, b: b * MM, c: ops(made, 'c'), l: ops(made, 'l') };
}
stepSync('A2 all fifteen styles (bend 60 %, distortions 30 % / −20 %, horizontal and vertical) and Free Distort: within 0.01 mm of the true image, both ways', () => {
  let worst = 0; const rows = [];
  for (const [style] of W.STYLES) for (const vertical of [false, true]) {
    const o = { style, bend: 0.6, h: 0.3, v: -0.2, vertical };
    const r = bothWays(SHAPE, W.warpFn(o, SBOX), style + (vertical ? ' vertical' : ''));
    worst = Math.max(worst, r.a, r.b); if (!vertical) rows.push(`${style} ${r.c}c/${r.l}l`);
  }
  const quadP = [[SBOX[0] + 10, SBOX[1] - 6], [SBOX[2] - 4, SBOX[1] + 3], [SBOX[2] + 12, SBOX[3] + 5], [SBOX[0] - 3, SBOX[3] - 2]];
  assert.ok(W.convex(quadP)); const rp = bothWays(SHAPE, W.quadFn(SBOX, quadP), 'free distort (projective)'); worst = Math.max(worst, rp.a, rp.b);
  const bow = [[SBOX[0], SBOX[1]], [SBOX[2], SBOX[1]], [SBOX[0] + 20, SBOX[1] + 10], [SBOX[0], SBOX[3]]];   // a corner dragged inside: not convex
  assert.ok(!W.convex(bow)); const rb = bothWays(SHAPE, W.quadFn(SBOX, bow), 'free distort (bilinear)'); worst = Math.max(worst, rb.a, rb.b);
  for (const [style] of W.STYLES) assert.equal(W.warpFn({ style, bend: 0 }, SBOX), null, style + ' at 0 % changes nothing');
  return `worst ${(worst * 1000).toFixed(2)} µm · ${rows.join(', ')}`;
});
stepSync('A3 Free Distort keeps straight lines straight (a rectangle stays four lines on the four corners); Perspective moves the partner corner the other way', () => {
  const q = [[X0 - 10, Y0 + 4], [X0 + Wd + 6, Y0 - 3], [X0 + Wd + 2, Y0 + Hd + 9], [X0 + 3, Y0 + Hd + 1]];
  const out = W.mapSubpaths(RECT, W.quadFn(BOX, q), TOL);
  assert.equal(ops(out, 'c'), 0, 'no curves'); assert.equal(ops(out, 'l'), 3, 'three lines and the close, as it came in');
  const an = anchors(out);
  for (const c of q) assert.ok(an.some(a => Math.hypot(a[0] - c[0], a[1] - c[1]) < 1e-6), 'a corner on ' + c);
  const q0 = W.boxQuad(BOX), side = W.perspectiveDrag(q0, 1, 8, 1), updown = W.perspectiveDrag(q0, 2, 1, 5);
  assert.ok(near(side[1][0], q0[1][0] + 8, 1e-9) && near(side[0][0], q0[0][0] - 8, 1e-9) && near(side[2][0], q0[2][0], 1e-9), 'sideways: top-left moves out with top-right');
  assert.ok(near(updown[2][1], q0[2][1] + 5, 1e-9) && near(updown[1][1], q0[1][1] - 5, 1e-9), 'up or down: the partner on the same side');
  const tr = W.mapSubpaths(RECT, W.quadFn(BOX, side), TOL); assert.equal(ops(tr, 'c'), 0, 'a trapezium of lines');
});

// blends
const SQ = rect(0, 0, 10 * PT, 10 * PT);
stepSync('A4 a 5-step blend: square into a bigger square drawn from another corner the other way round: five exact squares, sides growing evenly, centres evenly spaced', () => {
  const big = [reverse(rect(60 * PT, 4 * PT, 80 * PT, 24 * PT)[0])];   // 20 mm, drawn anticlockwise
  const big2 = [[big[0][0]].concat(big[0].slice(1))];
  const r = W.blend(SQ, big2, { steps: 5 });
  assert.equal(r.n, 5); assert.equal(r.steps.length, 5);
  const out = [];
  r.steps.forEach((st, i) => {
    const t = (i + 1) / 6; assert.ok(near(st.t, t, 1e-12));
    const pts = anchors(st.subpaths), b = boxOf(pts), side = 10 + 10 * t;
    assert.ok(near((b[2] - b[0]) * MM, side, 1e-6) && near((b[3] - b[1]) * MM, side, 1e-6), `step ${i + 1}: ${((b[2] - b[0]) * MM).toFixed(4)} mm`);
    const cx = (b[0] + b[2]) / 2 * MM, cy = (b[1] + b[3]) / 2 * MM;
    assert.ok(near(cx, 5 + 65 * t, 1e-6) && near(cy, 5 + 9 * t, 1e-6), 'centre on the line');
    // every anchor on the square's outline (no twist: a corner stays a corner)
    for (const p of pts) assert.ok(near(p[0], b[0], 1e-6) || near(p[0], b[2], 1e-6) || near(p[1], b[1], 1e-6) || near(p[1], b[3], 1e-6), 'on the outline');
    const corners = [[b[0], b[1]], [b[2], b[1]], [b[2], b[3]], [b[0], b[3]]];
    for (const c of corners) assert.ok(pts.some(p => near(p[0], c[0], 1e-6) && near(p[1], c[1], 1e-6)), 'a corner at ' + c);
    assert.equal(ops(st.subpaths, 'c'), 0, 'lines stay lines');
    out.push(((b[2] - b[0]) * MM).toFixed(3));
  });
  // square into a circle: the middle step's box halfway, its corners at the circle's 45° points (no twist)
  const C = circle(50 * PT, 5 * PT, 8 * PT), rc = W.blend(SQ, C, { steps: 5 }), mid = rc.steps[2];
  const bm = boxOf(pointsOf(mid.subpaths, 64)), want = [(0 + 42 * PT) / 2, (0 - 3 * PT) / 2, (10 * PT + 58 * PT) / 2, (10 * PT + 13 * PT) / 2];
  assert.ok(bm.every((v, i) => near(v, want[i], 1e-3)), 'the middle box is halfway: ' + bm.map(v => (v * MM).toFixed(3)));
  const c45 = [50 * PT - 8 * PT * Math.SQRT1_2, 5 * PT - 8 * PT * Math.SQRT1_2], midCorner = [(0 + c45[0]) / 2, (0 + c45[1]) / 2];
  assert.ok(anchors(mid.subpaths).some(p => Math.hypot(p[0] - midCorner[0], p[1] - midCorner[1]) < 1e-6), 'the square\'s corner met the circle at 45°');
  return 'sides ' + out.join(', ') + ' mm';
});
stepSync('A5 along a spine: steps centred on it, evenly along it; the ends moved onto its ends; Specified Distance counts from the length; Align to Path turns the steps', () => {
  const spine = [[['m', [5 * PT, 5 * PT]], ['c', [20 * PT, -25 * PT], [45 * PT, -25 * PT], [55 * PT, 5 * PT]]]];
  const C = circle(55 * PT, 5 * PT, 6 * PT), wk = W.walker(spine);
  const r = W.blend(SQ, C, { steps: 4, spine });
  assert.equal(r.steps.length, 4);
  let prevF = 0;
  r.steps.forEach((st, i) => {
    const b = boxOf(pointsOf(st.subpaths, 64)), c = [(b[0] + b[2]) / 2, (b[1] + b[3]) / 2], on = wk.at(st.t).p;
    assert.ok(Math.hypot(c[0] - on[0], c[1] - on[1]) * MM < 0.01, `step ${i + 1} centred on the spine: ${(Math.hypot(c[0] - on[0], c[1] - on[1]) * MM).toFixed(4)} mm`);
    assert.ok(near(st.t - prevF, 0.2, 1e-12)); prevF = st.t;
  });
  assert.ok(Math.hypot(r.aM[4] - 0, r.aM[5] - 0) < 1e-9, 'the square already sits on the spine\'s start');
  const atEnd = [r.bM[0] * 55 * PT + r.bM[2] * 5 * PT + r.bM[4], r.bM[1] * 55 * PT + r.bM[3] * 5 * PT + r.bM[5]];
  assert.ok(Math.hypot(atEnd[0] - 55 * PT, atEnd[1] - 5 * PT) < 1e-6, 'the circle\'s centre on the spine\'s end');
  const d = W.blend(SQ, C, { distance: 10 * PT, spine });   // the spine is about 63 mm: about six intervals of 10 mm, five steps
  assert.equal(d.n, Math.round(wk.length / (10 * PT)) - 1);
  const al = W.blend(SQ, SQ.map(sp => sp.map(op => op.map(v => (Array.isArray(v) ? [v[0] + 50 * PT, v[1]] : v)))), { steps: 3, spine, orient: 'path' });
  const turned = al.steps.map(st => { const a = anchors(st.subpaths); return Math.atan2(a[1][1] - a[0][1], a[1][0] - a[0][0]) * 180 / Math.PI; });
  assert.ok(Math.abs(turned[0]) > 10 && Math.abs(turned[2] - turned[0]) > 10, 'aligned to the path: ' + turned.map(v => v.toFixed(1)));
  return `spine ${(wk.length * MM).toFixed(2)} mm · distance 10 mm → ${d.n} steps · turns ${turned.map(v => v.toFixed(0) + '°').join(' ')}`;
});
stepSync('A6 compound: a ring blended into a disc: two subpaths every step, the hole shrinking into a point', () => {
  const ring = circle(10 * PT, 10 * PT, 8 * PT).concat(circle(10 * PT, 10 * PT, 4 * PT, true)), disc = circle(50 * PT, 10 * PT, 8 * PT);
  const r = W.blend(ring, disc, { steps: 3 });
  const sizes = r.steps.map(st => { assert.equal(st.subpaths.length, 2); const b = boxOf(pointsOf([st.subpaths[1]], 32)); return (b[2] - b[0]) * MM; });
  assert.ok(sizes[0] > sizes[1] && sizes[1] > sizes[2] && near(sizes[1], 4, 1e-3), 'the hole: ' + sizes.map(v => v.toFixed(3)));
  return 'hole ' + sizes.map(v => v.toFixed(2)).join(' → ') + ' mm';
});
stepSync('A7 the work: 120 circles and 120 rectangles (1 680 ops) warped at 0.01 mm', () => {
  const many = []; for (let i = 0; i < 120; i++) { const x = 100 + (i % 12) * 6, y = 50 + Math.floor(i / 12) * 6; many.push(circle(x, y, 2)[0], rect(x + 2.5, y - 1, x + 4, y + 1)[0]); }
  const box = boxOf(pointsOf(many, 4)), st = { evals: 0, pieces: 0 };
  W.warpSubpaths(many, { style: 'flag', bend: 0.5 }, box, TOL);   // (warm)
  const t0 = process.hrtime.bigint(); const out = W.warpSubpaths(many, { style: 'twist', bend: 0.7, h: 0.2 }, box, TOL, st); const ms = Number(process.hrtime.bigint() - t0) / 1e6;
  const t1 = process.hrtime.bigint(); W.warpSubpaths(many, { style: 'twist', bend: 0.7, h: 0.2 }, box, 0.35 / 4); const msPrev = Number(process.hrtime.bigint() - t1) / 1e6;
  assert.equal(out.length, 240);
  return `${ms.toFixed(1)} ms at 0.01 mm (${st.pieces} cubics, ${st.evals} map evaluations) · ${msPrev.toFixed(1)} ms at a preview's third of a pixel (400 %)`;
});

/* ════ B · in the browser ════ */
const pwDir = process.env.PW_DIR || path.join(root, 'node_modules');
let chromium = null; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { chromium = null; }
const HARNESS = 'file://' + path.join(__dirname, 'fixtures', 'charm-editor-harness.html');
const SHOTS = process.env.SHOTS || ''; if (SHOTS) require('fs').mkdirSync(SHOTS, { recursive: true });   // (SHOTS=<dir> keeps screenshots of the panels)
async function browserPart() {
  if (!chromium) { console.log('  - no playwright-core: the browser checks were not run (run through slot.sh)'); return; }
  console.log('B · in the browser');
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1243/chrome-linux64/chrome', args: ['--no-sandbox'] });
  const step = async (name, fn) => { try { const r = await fn(); pass(name + (r ? ' · ' + r : '')); } catch (e) { fail(name, e); } };
  const settle = page => page.evaluate(() => new Promise(r => requestAnimationFrame(() => requestAnimationFrame(r))));
  const ptClient = (page, x, y) => page.evaluate(([x, y]) => window.CharmEditor.current.clientOf(x, y), [x, y]);
  const items = (page, f) => page.evaluate(f => window.CharmEditor.current.items(f), f || null);
  const find = async (page, id, f) => { let r = null; const walk = l => l.forEach(i => { if (i.id === id) r = i; if (i.children) walk(i.children); }); walk(await items(page, f)); return r; };
  const hist = page => page.evaluate(() => window.CharmEditor.current.history().undo);
  try {
    const ctx = await browser.newContext({ viewport: { width: 1280, height: 760 }, reducedMotion: 'reduce' });
    await ctx.route(u => !u.href.startsWith('file://'), r => r.abort());
    const page = await ctx.newPage(), errs = [];
    page.on('pageerror', e => errs.push('pageerror: ' + e.message));
    page.on('console', m => { if (m.type() === 'error') errs.push('console: ' + m.text()); });
    await page.goto(HARNESS);
    await page.addScriptTag({ path: path.join(root, 'charm-nest-ce-warp.js') });
    await page.evaluate(() => { window.__log = []; window.CNDesignLog = { event: (k, d) => window.__log.push([k, d]) }; window.openEditor(); });
    await page.waitForSelector('.ce-root .ce-items path'); await settle(page); await settle(page);
    const clickSel = async id => { await page.evaluate(id => window.CharmEditor.current.ctx.select([id]), id); await settle(page); };

    await step('B1 the rail has the Blend tool (W) and the Distort flyout; the Object menu lists Blend and Warp & distort', async () => {
      const r = await page.evaluate(() => ({ blend: !!document.querySelector('.ce-rail [data-tool="cw-blend"]'), free: !!document.querySelector('.ce-rail [data-tool="cw-free"]'), persp: !!document.querySelector('.ce-rail .ce-menu [data-tool="cw-persp"]') }));
      assert.ok(r.blend && r.free && r.persp, JSON.stringify(r));
      await page.locator('.ce-root [data-drop="xcmd"] .ce-drop__b').click(); await settle(page);
      const heads = await page.evaluate(() => [...document.querySelectorAll('.ce-root [data-drop="xcmd"] .ce-menu .ce-menu__head')].map(h => h.textContent));
      assert.ok(heads.includes('Blend') && heads.includes('Warp & distort'), heads.join(','));
      await page.keyboard.press('Escape'); await settle(page);
    });

    let heart0 = null, u0 = 0;
    await step('B2 Warp on the filled heart (⌘⌥⇧W): the panel, a live preview over the ghosted heart, each slider move worked out within the frame budget', async () => {
      heart0 = await find(page, 'heart-fill'); u0 = await hist(page);
      await clickSel('heart-fill');
      await page.keyboard.press('Control+Alt+Shift+W'); await page.waitForSelector('.ce-root .cw-panel'); await settle(page); await settle(page);
      const r = await page.evaluate(() => ({ prev: document.querySelectorAll('.ce-art .cw-prev path').length, ghost: document.querySelector('.ce-items [data-id="heart-fill"]').style.opacity, title: document.querySelector('.cw-panel h3').textContent }));
      assert.equal(r.title, 'Warp'); assert.equal(r.prev, 1, 'one preview path'); assert.equal(r.ghost, '0.16', 'the heart ghosted under it');
      await page.selectOption('.cw-panel [data-cw="style"]', 'flag'); await settle(page);
      const before = await page.evaluate(() => window.CharmNestWarp.stats.frames.length);
      const slider = page.locator('.cw-panel [data-cw="bend"]'), sb = await slider.boundingBox();
      await page.mouse.move(sb.x + sb.width * 0.5, sb.y + sb.height / 2); await page.mouse.down();
      for (let i = 0; i <= 20; i++) { await page.mouse.move(sb.x + sb.width * (0.5 + 0.45 * i / 20), sb.y + sb.height / 2); await page.waitForTimeout(8); }
      await page.mouse.up(); await settle(page);
      const frames = await page.evaluate(n => window.CharmNestWarp.stats.frames.slice(n), before);
      assert.ok(frames.length >= 3, 'previews made: ' + frames.length);
      const max = Math.max(...frames);
      assert.ok(max < 16, 'every preview frame under 16 ms of work: ' + max);
      if (SHOTS) await page.screenshot({ path: path.join(SHOTS, 'warp-panel.png') });
      const d1 = await page.evaluate(() => document.querySelector('.ce-art .cw-prev path').getAttribute('d'));
      assert.ok(/C/.test(d1), 'the preview is curves');
      assert.equal(await hist(page), u0, 'nothing changed yet');
      return `${frames.length} preview frames, max ${max.toFixed(2)} ms, median ${frames.sort((a, b) => a - b)[frames.length >> 1].toFixed(2)} ms`;
    });
    await step('B3 Enter is OK: the heart is warped paths (curves, warp kept), one undo step, logged; ⌘Z puts it back; Esc on a new panel changes nothing', async () => {
      await page.keyboard.press('Enter'); await settle(page);
      const h = await find(page, 'heart-fill');
      assert.ok(!(await page.$('.cw-panel')), 'the panel closed'); assert.equal(await page.evaluate(() => document.querySelectorAll('.cw-prev').length), 0, 'the preview gone');
      assert.ok(h.warp && h.warp.style === 'flag' && Array.isArray(h.warp.src), 'warp kept: ' + JSON.stringify(h.warp && h.warp.style));
      assert.notDeepEqual(h.subpaths, heart0.subpaths); assert.ok(h.subpaths[0].some(op => op[0] === 'c'), 'curves stay curves');
      assert.equal(h.role, 'hatch'); assert.equal(await hist(page), u0 + 1, 'one undo step');
      assert.equal(await page.evaluate(() => document.querySelector('.ce-items [data-id="heart-fill"]').style.opacity), '', 'no ghost left');
      const lg = await page.evaluate(() => window.__log.map(e => e[0])); assert.ok(lg.includes('designWarp'), lg.join(','));
      // Warp again starts from the original: the panel opens on Flag, and bend 0 gives the heart back exactly
      await page.keyboard.press('Control+Alt+Shift+W'); await page.waitForSelector('.cw-panel');
      assert.equal(await page.$eval('.cw-panel [data-cw="style"]', s => s.value), 'flag');
      await page.fill('.cw-panel [data-cw-n="bend"]', '0'); await page.keyboard.press('Enter'); await settle(page);
      const h0 = await find(page, 'heart-fill');
      const flat = sps => sps.flat().filter(op => op[0] !== 'h').flatMap(op => op.slice(1)).flat();
      const A = flat(h0.subpaths), B = flat(heart0.subpaths);
      assert.equal(A.length, B.length); assert.ok(A.every((v, i) => Math.abs(v - B[i]) < 1e-5), 'bend 0 again: the original heart');
      await page.keyboard.press('Control+z'); await page.keyboard.press('Control+z'); await settle(page);
      const back = await find(page, 'heart-fill'); assert.deepEqual(back.subpaths, heart0.subpaths, 'undone'); assert.ok(!back.warp);
      await page.keyboard.press('Control+Alt+Shift+W'); await page.waitForSelector('.cw-panel'); await page.keyboard.press('Escape'); await settle(page);
      assert.deepEqual((await find(page, 'heart-fill')).subpaths, heart0.subpaths, 'Esc: no change'); assert.equal(await hist(page), u0);
    });
    await step('B4 words warped on the back: FOREVER becomes its outlines (a hatch path, filled), Arc 40 %, and Expand drops the warp', async () => {
      await page.locator('.ce-root .ce-head [data-face="back"]').click(); await settle(page); await settle(page);
      await clickSel('b-words');
      await page.evaluate(() => { const c = window.CharmEditor.commands['cw-warp']; c.run(window.CharmEditor.current.ctx); }); await page.waitForSelector('.cw-panel');
      assert.equal(await page.$eval('.cw-panel [data-cw="style"]', s => s.value), 'flag', 'the panel remembers the last style, as Illustrator does');
      await page.selectOption('.cw-panel [data-cw="style"]', 'arc'); await page.fill('.cw-panel [data-cw-n="bend"]', '40'); await settle(page);
      await page.locator('.cw-panel [data-cw-act="ok"]').click(); await settle(page);
      const w = await find(page, 'b-words', 'back');
      assert.equal(w.kind, 'path'); assert.equal(w.role, 'hatch'); assert.ok(w.fill && !w.text, 'outlines, filled, no words');
      assert.equal(w.warp.style, 'arc'); assert.ok(near(w.warp.bend, 0.4, 1e-9));
      await page.evaluate(() => window.CharmEditor.commands['cw-warp-expand'].run(window.CharmEditor.current.ctx)); await settle(page);
      assert.ok(!(await find(page, 'b-words', 'back')).warp, 'expanded');
      await page.locator('.ce-root .ce-head [data-face="front"]').click(); await settle(page); await settle(page);
    });
    await step('B5 the Blend tool (W): click one day square, then another; the panel shows the steps live; 3 steps, OK: one Blend group (2 ends + 3 steps), one undo step', async () => {
      const u1 = await hist(page);
      await page.evaluate(() => window.CharmEditor.current.ctx.select([])); await page.keyboard.press('w'); await settle(page);
      assert.equal(await page.evaluate(() => window.CharmEditor.current.tool()), 'cw-blend');
      const edge = async id => { const b = await page.evaluate(id => window.CharmEditor.current.box(id), id); return ptClient(page, b[0], b[1] + (b[3] - b[1]) * 0.3); };
      const a = await edge('day-0-0'), b = await edge('day-0-6');
      await page.mouse.move(a.x, a.y); await settle(page);
      assert.equal(await page.evaluate(() => document.querySelectorAll('.ce-ui .cw-hov').length), 1, 'the anchor under the cursor marked');
      await page.mouse.click(a.x, a.y); await settle(page);
      assert.equal(await page.evaluate(() => document.querySelectorAll('.ce-ui .cw-pick').length), 1, 'the first pick marked');
      await page.mouse.click(b.x, b.y); await page.waitForSelector('.cw-panel'); await settle(page); await settle(page);
      assert.equal(await page.evaluate(() => document.querySelectorAll('.ce-art .cw-prev path').length), 5, 'five steps previewed');
      await page.fill('.cw-panel [data-cw-n="steps"]', '3'); await settle(page); await settle(page);
      assert.equal(await page.evaluate(() => document.querySelectorAll('.ce-art .cw-prev path').length), 3, 'three steps previewed');
      if (SHOTS) await page.screenshot({ path: path.join(SHOTS, 'blend-panel.png') });
      await page.keyboard.press('Enter'); await settle(page);
      const its = await items(page), G = its.find(i => i.blend);
      assert.ok(G && G.kind === 'group' && G.name === 'Blend', 'a blend group');
      assert.equal(G.children.length, 5); assert.deepEqual(G.blend.ends, ['day-0-0', 'day-0-6']);
      assert.equal(G.children.filter(c => c.blendStep).length, 3);
      const xs = G.children.map(c => { let x0 = Infinity, x1 = -Infinity; for (const sp of c.subpaths) for (const op of sp) for (const p of op.slice(1)) { x0 = Math.min(x0, p[0]); x1 = Math.max(x1, p[0]); } return (x0 + x1) / 2; });
      const gaps = xs.slice(1).map((x, i) => x - xs[i]); assert.ok(gaps.every(g => near(g, gaps[0], 1e-4)), 'evenly spaced: ' + gaps.map(g => (g * MM).toFixed(3)));
      assert.equal(await hist(page), u1 + 1, 'one undo step');
      assert.deepEqual(await page.evaluate(() => window.CharmEditor.current.selection()), [G.id]);
      // Blend options again: 1 step
      await page.evaluate(() => window.CharmEditor.commands['cw-blend-options'].run(window.CharmEditor.current.ctx)); await page.waitForSelector('.cw-panel');
      await page.fill('.cw-panel [data-cw-n="steps"]', '1'); await page.keyboard.press('Enter'); await settle(page);
      const G2 = (await items(page)).find(i => i.blend); assert.equal(G2.children.length, 3, 'options: one step now'); assert.equal(G2.id, G.id);
      await page.keyboard.press('Control+z'); await page.keyboard.press('Control+z'); await settle(page);
      const ids = (await items(page)).map(i => i.id); assert.ok(ids.includes('day-0-0') && ids.includes('day-0-6') && !(await items(page)).some(i => i.blend), 'undone: the squares back');
      return 'gaps ' + (gaps[0] * MM).toFixed(3) + ' mm';
    });
    await step('B6 Free Distort: the selection\'s corners on the charm; the top-right corner dragged 3 mm: the outline follows (projective, curves kept), one undo step', async () => {
      await page.keyboard.press('Escape'); await page.keyboard.press('v'); await clickSel('heart-line');
      const u2 = await hist(page), h0 = await find(page, 'heart-line');
      await page.evaluate(() => window.CharmEditor.commands['cw-free'].run(window.CharmEditor.current.ctx)); await settle(page);
      assert.equal(await page.evaluate(() => window.CharmEditor.current.tool()), 'cw-free');
      assert.equal(await page.evaluate(() => document.querySelectorAll('.ce-ui .cw-quad').length), 1, 'the quad drawn');
      const bx = await page.evaluate(() => window.CharmEditor.current.box('heart-line'));
      const ne = await ptClient(page, bx[2], bx[1]), to = await ptClient(page, bx[2] + 3 * PT, bx[1] - 1.5 * PT);
      await page.mouse.move(ne.x, ne.y); await page.mouse.down(); await page.mouse.move(to.x, to.y, { steps: 8 }); await settle(page);
      assert.equal(await page.evaluate(() => document.querySelectorAll('.ce-art .cw-prev path').length), 1, 'the live preview while dragging');
      if (SHOTS) await page.screenshot({ path: path.join(SHOTS, 'free-distort.png') });
      await page.mouse.up(); await settle(page);
      const h = await find(page, 'heart-line'), nb = await page.evaluate(() => window.CharmEditor.current.box('heart-line'));
      assert.notDeepEqual(h.subpaths, h0.subpaths); assert.ok(h.subpaths[0].some(op => op[0] === 'c'));
      assert.ok(nb[2] > bx[2] + 1 * PT && nb[1] < bx[1] - 0.3 * PT, 'it reaches toward the dragged corner: ' + nb.map(v => ((v - bx[0]) * MM).toFixed(2)));
      assert.equal(await hist(page), u2 + 1, 'one undo step');
      await page.keyboard.press('Control+z'); await settle(page);
      assert.deepEqual((await find(page, 'heart-line')).subpaths, h0.subpaths, 'undone');
      return `box grew to ${((nb[2] - nb[0]) * MM).toFixed(2)} × ${((nb[3] - nb[1]) * MM).toFixed(2)} mm`;
    });
    await step('B7 Save hands back plain data with the new parts; no page errors', async () => {
      await clickSel('heart-fill');
      await page.keyboard.press('Control+Alt+Shift+W'); await page.waitForSelector('.cw-panel'); await page.keyboard.press('Enter'); await settle(page);
      await page.keyboard.press('Escape'); await page.keyboard.press('Control+s'); await page.waitForFunction(() => window.__closed === true);
      const r = await page.evaluate(() => window.__result);
      assert.ok(r && r.changes.front.changed['heart-fill'], 'the heart changed in the result');
      assert.ok(r.faces.front.items.find(i => i.id === 'heart-fill').warp, 'the warp travels with the data');
      assert.deepEqual(errs, []);
    });
    await ctx.close();
  } finally { await browser.close(); }
}

(async () => {
  try { await browserPart(); } catch (e) { fail('browser', e); }
  console.log(`\n${results.length} passed, ${failures.length} failed`);
  if (failures.length) { for (const f of failures) console.log('  - ' + f); process.exit(1); }
})();
