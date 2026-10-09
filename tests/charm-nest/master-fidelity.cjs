// What the app shows must be what the master drawing shows (REVIEW71, 9 Oct 2026). Each case is a class of real designs where
// the app drew something other than the master:
//   1 · a white fill on a cut-line member was painted black ("POLICE" on its blue plate, a fish's eye)
//   2 · a red outline left on the CUT layer is engraving by its pen colour, not a cut and not a hole (cross bars, petal lines)
//   3 · lettering converted to outlines on the LABELS layer beside a piece ("Cut/&Solid", a size digit) is a marker, not ink
//   4 · a jump ring beside a solid black body is that body's ring, not a charm of its own (it was met before its host)
//   6 · a plate drawn on CUT in coloured open strokes (a green U and its top line) is a charm of its own, not ink of its neighbour
//   5 · hoops the finder missed: a circle drawn with eight Béziers (ten operators), and a washer drawn as one filled path
//   node tests/charm-nest/master-fidelity.cjs
const assert = require('node:assert/strict');
global.self = global; global.PDFLib = require('../../vendor/pdf-lib-1.17.1.min.js'); require('../../charm-nest-pdf.js');
const P = CharmNestPDF, G = require('../../charm-nest-geom.js');
const K = 0.5523;
const blob = (cx, cy, w, h) => { const a = w / 2, b = h / 2; return [['m', [cx + a, cy]], ['c', [cx + a, cy + K * b], [cx + K * a, cy + b], [cx, cy + b]], ['c', [cx - K * a, cy + b], [cx - a, cy + K * b], [cx - a, cy]], ['c', [cx - a, cy - K * b], [cx - K * a, cy - b], [cx, cy - b]], ['c', [cx + K * a, cy - b], [cx + a, cy - K * b], [cx + a, cy]], ['h']]; };
const bboxOf = subs => { const pts = subs.flat().flatMap(o => o.slice(1)); return [Math.min(...pts.map(p => p[0])), Math.min(...pts.map(p => p[1])), Math.max(...pts.map(p => p[0])), Math.max(...pts.map(p => p[1]))]; };
const path = (layer, subpaths, o = {}) => Object.assign({ kind: 'path', layer, subpaths, bbox: bboxOf(subpaths), closed: true, stroke: true, fill: false, strokeRGB: [0, 0, 0], fillRGB: [0, 0, 0], lwPt: 0.28, paintOp: 'S', depth: 0 }, o);
const fillOnly = (layer, subpaths, rgb) => path(layer, subpaths, { stroke: false, fill: true, fillRGB: rgb, paintOp: 'f' });
const parsedOf = segs => ({ segments: segs.map((s, i) => Object.assign(s, { index: i, start: i * 10, end: i * 10 + 9 })), nested: [], pageW: 80, pageH: 80 });
const group = (segs, opts) => { const parsed = parsedOf(segs); return { parsed, g: P.groupCharms(parsed, Object.assign({ minPt: 6 }, opts || {})) }; };
const recorder = () => { const rec = { fills: [], strokes: [] }; const s = { fillStyle: '#000', strokeStyle: '#000', lineWidth: 1 };
  const ctx = new Proxy(s, { get: (o, k) => k === 'fill' ? () => rec.fills.push(o.fillStyle) : k === 'stroke' ? () => rec.strokes.push({ color: o.strokeStyle, w: o.lineWidth }) : k in o ? o[k] : () => {}, set: (o, k, v) => { o[k] = v; return true; } });
  return { ctx, rec }; };
const draw = c => { const { ctx, rec } = recorder(); P.drawCharm(ctx, c, (x, y) => [x, y], 4); return rec; };
// a circle as eight Béziers (what Illustrator writes for some rings), and a clockwise four-Bézier circle (a washer's hole)
const circle8 = (cx, cy, r) => { const k = 4 / 3 * Math.tan(Math.PI / 16) * r, pt = a => [cx + r * Math.cos(a), cy + r * Math.sin(a)], tg = a => [-Math.sin(a) * k, Math.cos(a) * k]; const ops = [['m', pt(0)]];
  for (let i = 0; i < 8; i++) { const a0 = i * Math.PI / 4, a1 = (i + 1) * Math.PI / 4, p0 = pt(a0), p1 = pt(a1), t0 = tg(a0), t1 = tg(a1); ops.push(['c', [p0[0] + t0[0], p0[1] + t0[1]], [p1[0] - t1[0], p1[1] - t1[1]], p1]); } ops.push(['h']); return ops; };
const blobCW = (cx, cy, w, h) => { const a = w / 2, b = h / 2; return [['m', [cx + a, cy]], ['c', [cx + a, cy - K * b], [cx + K * a, cy - b], [cx, cy - b]], ['c', [cx - K * a, cy - b], [cx - a, cy - K * b], [cx - a, cy]], ['c', [cx - a, cy + K * b], [cx - K * a, cy + b], [cx, cy + b]], ['c', [cx + K * a, cy + b], [cx + a, cy + K * b], [cx + a, cy]], ['h']]; };
let checks = 0; const ok = m => { checks++; console.log('  ok ' + m); };

// 1 · a light fill on a cut-line member keeps its colour; black stays black; the cut line itself is still the black pen
{
  const body = path('CUT', [blob(40, 40, 30, 34)]);
  const plate = fillOnly('HATCH', [blob(40, 40, 20, 10)], [0, 0, 1]);
  const white = fillOnly('CUT', [blob(40, 40, 12, 4)], [1, 1, 1]);                      // white lettering knocked out of the blue plate
  const { g } = group([body, plate, white]);
  assert.equal(g.charms.length, 1); const c = g.charms[0];
  assert(c.members.includes(white), 'the white shape belongs to the charm');
  const rec = draw(c);
  assert(rec.fills.includes('rgb(255,255,255)'), 'the white fill is drawn white: ' + JSON.stringify(rec.fills));
  assert(rec.fills.includes('rgb(0,0,255)'), 'the blue plate is still blue');
  assert(!rec.fills.includes('rgb(0,0,0)'), 'nothing is painted black that the master does not paint black');
  assert(rec.strokes.every(s => s.color === '#000' || s.color === 'rgb(0,0,0)'), 'cut lines are drawn in the cut pen');
  ok('white fill on a cut-line member stays white');
  // a dark fill on a cut-line member (the twin of the silhouette, a black cut-out) is unchanged
  const dark = fillOnly('CUT', [blob(40, 40, 12, 4)], [0.05, 0.05, 0.05]);
  const r2 = group([body, dark]); const rec2 = draw(r2.g.charms[0]);
  assert(rec2.fills.every(f => f === 'rgb(0,0,0)'), 'a dark fill is still drawn black: ' + JSON.stringify(rec2.fills));
  ok('dark fill unchanged');
}

// 2 · a red outline on the CUT layer INSIDE a black cut outline is engraving; the layer still rules everywhere else
{
  const body = path('CUT', [blob(40, 40, 30, 34)]);
  const bar = path('CUT', [blob(40, 40, 6, 20)], { strokeRGB: [1, 0, 0] });
  assert.equal(G.pathRole(bar), 'cut', 'on its own the layer decides (the rule every other test relies on)');
  const { g } = group([body, bar]);
  assert.equal(g.charms.length, 1); const c = g.charms[0];
  assert.equal(c.outline, body, 'the black outline is the body');
  assert(c.members.includes(bar), 'the red bar is part of the charm');
  assert.equal(bar.manufacturingRole, 'engrave', 'enclosed by a black cut line, the red pen is engraving');
  assert(!P.isCutLine(bar) && !G.isCutLine(bar), 'so it is not a cut line, and not a hole');
  assert(!P.cutLinesOf(c).includes(bar), 'it is not among the cut-outs');
  const rec = draw(c);
  assert(rec.strokes.some(s => s.color === 'rgb(255,0,0)'), 'it is drawn red, as in the master: ' + JSON.stringify(rec.strokes));
  // a black bar inside stays a hole; a green one stays a cut; a role the shop set is kept; red with a fill is untouched
  const keep = [path('CUT', [blob(40, 40, 6, 20)]), path('CUT', [blob(40, 40, 6, 20)], { strokeRGB: [0.2, 0.7, 0.3] }),
    path('CUT', [blob(40, 40, 6, 20)], { strokeRGB: [1, 0, 0], manufacturingRole: 'cut' }),
    path('CUT', [blob(40, 40, 6, 20)], { strokeRGB: [1, 0, 0], fill: true, fillRGB: [0, 0, 0], paintOp: 'B' })];
  for (const k of keep) { const r = group([path('CUT', [blob(40, 40, 30, 34)]), k]); assert(P.isCutLine(k), 'stays a cut line: ' + JSON.stringify(k.strokeRGB)); assert(r.g.charms[0].members.includes(k)); }
  // a red outline that nothing black encloses is still a cut: a charm drawn in red alone, and a red ring beside a black body
  const alone = path('CUT', [blob(40, 40, 30, 34)], { strokeRGB: [1, 0, 0] });
  const r2 = group([alone]); assert.equal(r2.g.charms.length, 1); assert.equal(r2.g.charms[0].outline, alone, 'a red outline alone is the charm'); assert(P.isCutLine(alone));
  const outside = path('CUT', [blob(70, 70, 8, 8)], { strokeRGB: [1, 0, 0] });
  group([path('CUT', [blob(20, 20, 20, 20)]), outside]); assert(P.isCutLine(outside), 'a red outline outside the black one stays a cut');
  ok('a red outline inside a black cut line is engraving; everything else keeps its layer rule');
}

// 3 · outlined lettering on LABELS beside the piece is a marker (whether it touches the piece box or lies a few points away)
{
  const body = path('CUT', [blob(30, 40, 20, 24)]);                                    // box 20..40 x 28..52
  for (const [name, y] of [['just below', 25], ['a few points away', 18]]) {
    const letter = fillOnly('LABELS', [blob(30, y, 4, 5)], [0, 0, 0]);
    const { g } = group([body, letter]); const c = g.charms[0];
    assert.equal(g.charms.length, 1, name);
    assert(!c.members.includes(letter), name + ': the lettering is not a member');
    assert(g.markers.some(t => t.seg === letter && t.why === 'note lettering'), name + ': it is taken as note lettering');
    assert.deepEqual(c.bbox.map(v => Math.round(v)), [20, 28, 40, 52], name + ': it is not in the charm box');
    assert.equal(draw(c).fills.length, 0, name + ': nothing is painted for it');
  }
  // lettering ON the piece (inside its box) and a stroked ring on LABELS above it are not touched
  const inside = fillOnly('LABELS', [blob(30, 40, 4, 5)], [0, 0, 0]);
  const hoopOnLabels = path('LABELS', [blob(30, 55, 6, 6), blob(30, 55, 3.4, 3.4)], { strokeRGB: [0.31, 0.82, 0.85] });
  const r = group([body, inside, hoopOnLabels]); const c = r.g.charms[0];
  assert(c.members.includes(inside), 'lettering inside the piece stays');
  assert(c.members.includes(hoopOnLabels), 'a stroked ring on LABELS stays');
  ok('outlined note lettering is a marker; lettering on the piece and a labelled hoop are kept');
}

// 4 · a jump ring beside a SOLID black body: stroked outlines are accepted first, so the ring was met before its host
{
  const body = fillOnly('CUT', [blob(40, 30, 26, 30)], [0, 0, 0]);                       // box 27..53 x 15..45
  const ring = path('CUT', [blob(40, 47.5, 6.5, 6.5), blob(40, 47.5, 3.7, 3.7)]);        // sits on the top edge
  const { g } = group([body, ring]);
  assert.equal(g.charms.length, 1, 'the ring and the solid body are one charm: ' + g.charms.length);
  const c = g.charms[0];
  assert(c.members.includes(ring) && c.members.includes(body), 'both are members of it');
  assert.equal(g.mergedCount, 1, 'the ring is merged');
  // the same ring written BEFORE the body (stream order must not matter)
  const r2 = group([ring, body]); assert.equal(r2.g.charms.length, 1, 'ring first in the stream: one charm');
  // a stroked body keeps working as before
  const sBody = path('CUT', [blob(40, 30, 26, 30)]);
  const r3 = group([sBody, ring]); assert.equal(r3.g.charms.length, 1, 'stroked body and ring: one charm');
  // a ring far from every body stays its own piece
  const far = path('CUT', [blob(10, 70, 6.5, 6.5)]);
  const r4 = group([body, far]); assert.equal(r4.g.charms.length, 2, 'a distant ring is not attached');
  ok('a ring beside a solid black body joins it');
}

// 5 · hoops: a cyan ring whose outer circle is eight Béziers, and a black washer drawn as one filled path
{
  const body = () => path('CUT', [blob(40, 35, 30, 34)]);                                  // top edge at y = 52
  const cyan = [0.31, 0.82, 0.85];
  const weld = (ringSeg, label) => {
    const { g } = group([body(), ringSeg]);
    assert.equal(g.charms.length, 1, label + ': the ring belongs to the body'); const c = g.charms[0];
    const hoops = P.findHoops(c, require('../../charm-nest-vector.js'));
    assert.equal(hoops.length, 1, label + ': the hoop is found'); assert(hoops[0].aperture, label + ': with its hole');
    const res = P.integrateRings(c);
    assert.equal(res.welded, 1, label + ': welded ' + JSON.stringify(res)); assert.deepEqual(res.left, [], label);
    assert(!c.members.includes(ringSeg), label + ': the loose ring path is gone');
    assert(c.outline.bbox[3] >= 54.5 + 3.25 - 0.05, label + ': the outline now runs round the hoop');
    assert.equal(P.cutLinesOf(c).length, 1, label + ': the hoop hole is the only cut-out');
    return c;
  };
  weld(path('CUT', [circle8(40, 54.5, 3.25), blob(40, 54.5, 3.7, 3.7)], { strokeRGB: cyan }), 'eight-Bézier cyan ring');
  weld(path('CUT', [blob(40, 54.5, 6.5, 6.5), blobCW(40, 54.5, 3.7, 3.7)], { stroke: false, fill: true, fillRGB: [0, 0, 0], paintOp: 'f' }), 'black washer (nonzero, opposite winding)');
  weld(path('CUT', [blob(40, 54.5, 6.5, 6.5), blob(40, 54.5, 3.7, 3.7)], { stroke: false, fill: true, fillRGB: [0, 0, 0], paintOp: 'f*' }), 'black washer (even-odd)');
  // not hoops: a washer whose fill has no hole (same winding, nonzero), a washer on the LABELS layer (a letter O), a lone filled dot
  const V = require('../../charm-nest-vector.js');
  for (const [label, seg] of [
    ['solid disc (same winding, nonzero)', path('CUT', [blob(40, 54.5, 6.5, 6.5), blob(40, 54.5, 3.7, 3.7)], { stroke: false, fill: true, fillRGB: [0, 0, 0], paintOp: 'f' })],
    ['letter O on LABELS', path('LABELS', [blob(40, 54.5, 6.5, 6.5), blobCW(40, 54.5, 3.7, 3.7)], { stroke: false, fill: true, fillRGB: [0, 0, 0], paintOp: 'f' })],
    ['lone filled dot', path('CUT', [blob(40, 54.5, 6.5, 6.5)], { stroke: false, fill: true, fillRGB: [0, 0, 0], paintOp: 'f' })]]) {
    assert(!P.ringLike(seg) || label === 'solid disc (same winding, nonzero)', label + ': not ring-like');
    const c = { outline: body(), members: [], bbox: [25, 18, 55, 52] }; c.members.push(c.outline, seg);
    assert.equal(P.findHoops(c, V).length, 0, label + ': no hoop');
  }
  // a solid body (a fill with the 1 mm pen width the graphics state held) welded to its hoop gets a hairline cut line, not a 1 mm one
  const solid = fillOnly('CUT', [blob(40, 35, 30, 34)], [0, 0, 0]); solid.lwPt = 2.835;
  const gs = group([solid, path('CUT', [blob(40, 54.5, 6.5, 6.5), blob(40, 54.5, 3.7, 3.7)], { strokeRGB: cyan })]);
  assert.equal(gs.g.charms.length, 1); const sc = gs.g.charms[0];
  assert.equal(P.integrateRings(sc).welded, 1, 'the solid body takes its hoop');
  assert(sc.outline.lwPt <= 0.5, 'the welded cut line is a hairline: ' + sc.outline.lwPt);
  const stroked = path('CUT', [blob(40, 35, 30, 34)], { lwPt: 0.9 });
  const gt = group([stroked, path('CUT', [blob(40, 54.5, 6.5, 6.5), blob(40, 54.5, 3.7, 3.7)], { strokeRGB: cyan })]); P.integrateRings(gt.g.charms[0]);
  assert.equal(gt.g.charms[0].outline.lwPt, 0.9, 'a stroked body keeps its own pen width');
  ok('eight-Bézier rings and filled washers are welded; letters, discs and dots are not hoops; a solid body welds to a hairline');
}

// 6 · a green plate on CUT drawn as an open U and a straight top line: the chain is a cut line by its layer, so the plate is its own charm
{
  const green = [0.22, 0.7, 0.29];
  const u = path('CUT', [[['m', [10, 40]], ['l', [10, 30]], ['l', [50, 30]], ['l', [50, 40]]]], { closed: false, strokeRGB: green });
  const top = path('CUT', [[['m', [50, 40]], ['l', [10, 40]]]], { closed: false, strokeRGB: green });
  const hole = path('CUT', [blob(14, 35, 3.7, 3.7)], { strokeRGB: green });
  const neighbour = path('CUT', [[['m', [10, 15]], ['l', [50, 15]], ['l', [50, 23]], ['l', [10, 23]], ['h']]]);
  const { g } = group([neighbour, u, top, hole]);
  assert.equal(g.charms.length, 2, 'the plate and its neighbour are two charms: ' + g.charms.length);
  const plate = g.charms.find(c => c.members.includes(u)), other = g.charms.find(c => c.outline === neighbour);
  assert(plate && plate !== other, 'the plate is not part of the neighbour');
  assert(plate.members.includes(top) && plate.members.includes(hole), 'the plate has its top line and its hole');
  assert.equal(other.members.length, 1, 'the neighbour has none of the plate');
  assert.equal(plate.outline.layer, 'CUT', 'the chained outline is on its parts\' layer');
  // black chains behave as before
  const bu = Object.assign({}, u, { strokeRGB: [0, 0, 0] }), bt = Object.assign({}, top, { strokeRGB: [0, 0, 0] });
  assert.equal(group([neighbour, bu, bt]).g.charms.length, 2);
  ok('a plate in coloured open strokes on CUT is its own charm');
}

console.log(`${checks} checks passed`);
