// A charm's cut silhouette that a master draws as a black FILL (text converted to outlines: every LETTER_EARRING-n;
// an expanded shape: CELESTIAL15 - CRESCENT) is a cut line. It used to be painted as a solid black body (library card,
// order window, sheet) and written to the DXF as a SOLID hatch with no cut line, so a charm with no engraving looked and
// exported as one with solid black engraving. Real black engraving (a fill on an engraving layer) must stay solid.
//   node tests/charm-nest/cut-fill-outline.cjs
const assert = require('node:assert/strict');
global.self = global; global.PDFLib = require('../../vendor/pdf-lib-1.17.1.min.js'); require('../../charm-nest-pdf.js');
const P = CharmNestPDF, E = require('../../charm-nest-export.js');
const K = 0.5523;
const blob = (cx, cy, w, h) => { const a = w / 2, b = h / 2; return [['m', [cx + a, cy]], ['c', [cx + a, cy + K * b], [cx + K * a, cy + b], [cx, cy + b]], ['c', [cx - K * a, cy + b], [cx - a, cy + K * b], [cx - a, cy]], ['c', [cx - a, cy - K * b], [cx - K * a, cy - b], [cx, cy - b]], ['c', [cx + K * a, cy - b], [cx + a, cy - K * b], [cx + a, cy]], ['h']]; };
const bboxOf = subs => { const pts = subs.flat().flatMap(o => o.slice(1)); return [Math.min(...pts.map(p => p[0])), Math.min(...pts.map(p => p[1])), Math.max(...pts.map(p => p[0])), Math.max(...pts.map(p => p[1]))]; };
const path = (layer, subpaths, o = {}) => Object.assign({ kind: 'path', layer, subpaths, bbox: bboxOf(subpaths), closed: true, stroke: true, fill: false, strokeRGB: [0, 0, 0], fillRGB: [0, 0, 0], lwPt: 0.28, paintOp: 'S', depth: 0 }, o);
// what Illustrator leaves after "Create Outlines" on a letter: fill black, no stroke (paintOp f), a stray pink stroke width
const letter = (layer, cx, cy, o = {}) => path(layer, [blob(cx, cy, 14, 17), blob(cx, cy, 5, 7)], Object.assign({ stroke: false, fill: true, paintOp: 'f', strokeRGB: [0.97, 0.79, 0.87], lwPt: 2.835 }, o));
const parsedOf = segs => ({ segments: segs.map((s, i) => Object.assign(s, { index: i, start: i * 10, end: i * 10 + 9 })), nested: [], pageW: 80, pageH: 80 });
const group = segs => { const parsed = parsedOf(segs); return { parsed, g: P.groupCharms(parsed, { minPt: 6 }) }; };
// a canvas stand-in that records what is filled and what is stroked, and in which colour
const recorder = () => { const rec = { fills: [], strokes: [] }; const s = { fillStyle: '#000', strokeStyle: '#000', lineWidth: 1 };
  const ctx = new Proxy(s, { get: (o, k) => k === 'fill' ? () => rec.fills.push(o.fillStyle) : k === 'stroke' ? () => rec.strokes.push({ color: o.strokeStyle, w: o.lineWidth }) : k in o ? o[k] : () => {}, set: (o, k, v) => { o[k] = v; return true; } });
  return { ctx, rec }; };
const draw = c => { const { ctx, rec } = recorder(); P.drawCharm(ctx, c, (x, y) => [x, y], 4); return rec; };

// 1 · a letter drawn as a fill on CUT: it is the charm's outline, nothing else is on it, and it is drawn as a line
{
  const a = letter('CUT', 20, 20);
  const { g } = group([a]);
  assert.equal(g.charms.length, 1); const c = g.charms[0];
  assert.equal(c.outline, a, 'the filled letter is the outline (nesting keeps working)');
  assert.equal(c.members.length, 1, 'no engraving on it');
  assert(P.isCutSilhouetteFill(c, a));
  const rec = draw(c);
  assert.equal(rec.fills.length, 0, 'the cut silhouette is never filled: no solid black body');
  assert(rec.strokes.length >= 1 && rec.strokes.every(s => s.color === '#000' || s.color === 'rgb(0,0,0)'), 'it is drawn as a black cut line');
  assert(rec.strokes.every(s => s.w <= 4 * 0.25 + 1e-9), 'at the cut hairline, not the stray 1 mm stroke width the master left');
  assert(!a.stroke && a.fill, 'the parsed segment itself is left as the master drew it (nesting and the writer read it)');
}

// 2 · the same letter on the "Isolation Mode" layer and with no layer at all (a dark fill with no stroke cuts along its edge)
for (const layer of ['Isolation Mode', null]) {
  const a = letter(layer, 20, 20); const { g } = group([a]);
  assert.equal(g.charms.length, 1, 'layer ' + layer); assert.equal(draw(g.charms[0]).fills.length, 0, 'layer ' + layer + ' is a cut outline too');
}

// 3 · a stroked cut outline with its filled twin on CUT (WOLF_89694): the twin is the same silhouette and is drawn as a line
{
  const outline = path('CUT', [blob(20, 20, 14, 17)]), twin = letter('CUT', 20, 20, { subpaths: [blob(20, 20, 14, 17)], bbox: bboxOf([blob(20, 20, 14, 17)]) });
  const { g } = group([twin, outline]); const c = g.charms[0];
  assert(c, 'one charm'); const t = c.members.find(m => m !== c.outline && m.fill && !m.stroke);
  assert(t && P.isCutSilhouetteFill(c, t), 'the filled twin is the silhouette again');
  assert.equal(draw(c).fills.length, 0, 'no solid body');
}

// 4 · real engraving stays filled: a blue hatch fill keeps its paint; a black fill on an engraving layer inside a stroked outline is hatching (blue)
{
  const body = path('CUT', [blob(20, 20, 20, 24)]);
  const hatch = path('HATCH', [blob(20, 20, 10, 12)], { stroke: false, fill: true, fillRGB: [0, 0, 1], paintOp: 'f' });
  const ink = path('ENGRAVE', [blob(20, 24, 6, 5)], { stroke: false, fill: true, fillRGB: [0, 0, 0], paintOp: 'f' });
  const { g } = group([body, hatch, ink]); const c = g.charms[0];
  assert(!P.isCutSilhouetteFill(c, hatch) && !P.isCutSilhouetteFill(c, ink) && !P.isCutSilhouetteFill(c, body));
  const rec = draw(c);
  assert(rec.fills.includes('rgb(0,0,255)'), 'blue hatching stays filled');
  assert(!rec.fills.includes('rgb(0,0,0)') && rec.fills.filter(f => f === 'rgb(0,0,255)').length === 2, 'a black fill inside is blue hatching too (the black-fill rule, tests/charm-nest/black-fill.cjs): never a solid black body');
}

// 5 · a fill-only outline WITH engraving: the silhouette is a line, the engraving is still painted
{
  const body = letter('CUT', 20, 20, { subpaths: [blob(20, 20, 26, 30)], bbox: bboxOf([blob(20, 20, 26, 30)]) });
  const hatch = path('HATCH', [blob(20, 20, 8, 8)], { stroke: false, fill: true, fillRGB: [0, 0, 1], paintOp: 'f' });
  const { g } = group([body, hatch]); const c = g.charms[0];
  assert.equal(c.outline, body);
  const rec = draw(c);
  assert.deepEqual(rec.fills, ['rgb(0,0,255)'], 'only the engraving is filled');
}

// 6 · DXF: the filled silhouette is one closed cut polyline on its layer, with no SOLID hatch; real fills still hatch
{
  const a = letter('CUT', 20, 20), b = path('CUT', [blob(50, 20, 14, 17)]), twin = letter('CUT', 50, 20, { subpaths: [blob(50, 20, 14, 17)], bbox: bboxOf([blob(50, 20, 14, 17)]) });
  const hatch = path('HATCH', [blob(50, 20, 5, 5)], { stroke: false, fill: true, fillRGB: [0, 0, 1], paintOp: 'f' });
  const parsed = parsedOf([a, twin, b, hatch]);
  const out = E.dxf(E.productionPaths(parsed));
  const count = (re) => (out.text.match(re) || []).length;
  assert.equal(count(/AcDbHatch/g), 1, 'only the blue engraving fill is a hatch');
  // the letter 'a' contributes two closed polylines (outer and counter), the stroked body one, the twin none
  assert.equal(count(/AcDbPolyline/g), 3, 'the cut silhouette is a polyline; its twin adds no second cut line');
  assert(/AcDbPolyline\r\n90\r\n\d+\r\n70\r\n1\r\n/.test(out.text), 'the cut polylines are closed');
}
console.log('cut-fill-outline: ok');
