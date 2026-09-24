// Some library masters keep a charm's black body outline on the HATCH or ENGRAVE layer, with only the jump ring on CUT
// (SHEEP 2, CAPYBARA 1, FIREBIRD 4-DISC, FLORAL2, SITTING) or nothing on CUT at all (DRAGON11, S2). The grouping must
// take that body as the charm's outline, so the nest packs around the whole charm the sheet draws, while an engraving
// border or frame beside a real cut body stays artwork.
//   node tests/charm-nest/artwork-layer-body.cjs
const assert = require('node:assert/strict');
global.self = global; global.PDFLib = require('../../vendor/pdf-lib-1.17.1.min.js'); require('../../charm-nest-pdf.js');
const P = CharmNestPDF, G = require('../../charm-nest-geom.js');
const K = 0.5523;
const circle = (cx, cy, r) => [['m', [cx + r, cy]], ['c', [cx + r, cy + K * r], [cx + K * r, cy + r], [cx, cy + r]], ['c', [cx - K * r, cy + r], [cx - r, cy + K * r], [cx - r, cy]], ['c', [cx - r, cy - K * r], [cx - K * r, cy - r], [cx, cy - r]], ['c', [cx + K * r, cy - r], [cx + r, cy - K * r], [cx + r, cy]], ['h']];
// a rounded blob (curved, so never mistaken for a box), w × h about its centre
const blob = (cx, cy, w, h) => { const a = w / 2, b = h / 2; return [['m', [cx + a, cy]], ['c', [cx + a, cy + K * b], [cx + K * a, cy + b], [cx, cy + b]], ['c', [cx - K * a, cy + b], [cx - a, cy + K * b], [cx - a, cy]], ['c', [cx - a, cy - K * b], [cx - K * a, cy - b], [cx, cy - b]], ['c', [cx + K * a, cy - b], [cx + a, cy - K * b], [cx + a, cy]], ['h']]; };
const box = (x0, y0, x1, y1) => [['m', [x0, y0]], ['l', [x1, y0]], ['l', [x1, y1]], ['l', [x0, y1]], ['h']];
const bboxOf = subs => { const pts = subs.flat().flatMap(o => o.slice(1)); return [Math.min(...pts.map(p => p[0])), Math.min(...pts.map(p => p[1])), Math.max(...pts.map(p => p[0])), Math.max(...pts.map(p => p[1]))]; };
const path = (layer, subpaths, o = {}) => Object.assign({ kind: 'path', layer, subpaths, bbox: bboxOf(subpaths), closed: true, stroke: true, fill: false, strokeRGB: [0, 0, 0], fillRGB: [0, 0, 1], lwPt: 0.28, paintOp: 'S', depth: 0 }, o);
const CYAN = [0.31, 0.82, 0.85];
const parsedOf = segs => ({ segments: segs.map((s, i) => Object.assign(s, { index: i, start: i * 10, end: i * 10 + 9 })), nested: [], pageW: 60, pageH: 60 });
const group = segs => { const parsed = parsedOf(segs); return { parsed, g: P.groupCharms(parsed, { minPt: 6 }) }; };

// 1 · body on HATCH, jump ring on CUT touching its top, hatch fill inside: one charm, the body its outline, the ring welded on
{
  const body = path('HATCH', [blob(30, 26, 31, 23)]), ring = path('CUT', [circle(30, 38.4, 3.26), circle(30, 38.4, 1.84)], { strokeRGB: CYAN, lwPt: 0.25 });
  const fill = path('HATCH', [blob(30, 26, 26, 18)], { stroke: false, fill: true, paintOp: 'f' });
  const { g } = group([body, fill, ring]);
  assert.equal(g.charms.length, 1, 'one charm'); assert.equal(g.orphans.length, 0, 'nothing left loose');
  const c = g.charms[0];
  assert.equal(c.outline, body, 'the body on the engraving layer is the outline, not the 2 mm ring');
  assert.equal(G.pathRole(body), 'cut', 'and it carries the outline role from here on');
  assert(c.members.includes(ring) && c.members.includes(fill), 'the ring and the hatch belong to it');
  assert.equal(P.integrateRings(c).welded, 1, 'the ring welds onto the body as usual');
  assert(c.bbox[2] - c.bbox[0] > 30 && c.bbox[3] - c.bbox[1] > 26, 'the charm spans the body and ring: ' + c.bbox.map(v => v.toFixed(1)));
  assert.equal(G.pathRole(fill), 'artwork', 'the hatch itself stays artwork');
}
// 2 · no CUT layer at all (DRAGON11, S2): the body is still the charm, where the grouping used to find no outline
{
  const body = path('ENGRAVE', [blob(30, 30, 24, 26)]), detail = path('ENGRAVE', [blob(30, 30, 12, 14)], { strokeRGB: [1, 0.14, 0.18], lwPt: 0.25 });
  const { g } = group([body, detail]);
  assert.equal(g.charms.length, 1); assert.equal(g.charms[0].outline, body, 'the lone black outline is the charm'); assert(g.charms[0].members.includes(detail));
}
// 3 · a proper file: the body on CUT and a hatch border drawn a hair outside it; the border is not a second outline
{
  const cut = path('CUT', [blob(30, 30, 24, 20)], { strokeRGB: CYAN }), border = path('HATCH', [blob(30, 30, 24.8, 20.8)]);
  const { g } = group([cut, border]);
  assert.equal(g.charms.length, 1); assert.equal(g.charms[0].outline, cut, 'the cut line stays the outline');
  assert.equal(G.pathRole(border), 'artwork', 'a border the size of the cut body stays engraving');
}
// 4 · an engraving frame inside a real cut body stays a detail of it
{
  const cut = path('CUT', [blob(30, 30, 30, 26)]), frame = path('ENGRAVE', [blob(30, 30, 18, 14)]);
  const { g } = group([cut, frame]);
  assert.equal(g.charms.length, 1); assert.equal(g.charms[0].outline, cut); assert(g.charms[0].members.includes(frame));
  assert.equal(G.pathRole(frame), 'artwork', 'the frame inside the body is never promoted');
}
// 5 · a straight-sided box on an artwork layer around a small cut charm is a guide, not a body
{
  const cut = path('CUT', [blob(30, 30, 8, 8)]), guide = path('HATCH', [box(10, 10, 50, 50)]);
  const { g } = group([cut, guide]);
  assert.equal(G.pathRole(guide), 'artwork', 'a box is never promoted');
  assert(g.charms.every(c => c.outline !== guide));
}
// 6 · two charms on one master, one with its body on HATCH: each ring joins its own body, neither body is lost
{
  const bodyA = path('HATCH', [blob(15, 20, 16, 14)]), ringA = path('CUT', [circle(15, 28.2, 3.26), circle(15, 28.2, 1.84)], { strokeRGB: CYAN, lwPt: 0.25 });
  const bodyB = path('CUT', [blob(45, 20, 16, 14)]), ringB = path('CUT', [circle(45, 28.2, 3.26), circle(45, 28.2, 1.84)], { strokeRGB: CYAN, lwPt: 0.25 });
  const { g } = group([bodyA, ringA, bodyB, ringB]);
  assert.equal(g.charms.length, 2, 'two charms');
  const a = g.charms.find(c => c.outline === bodyA), b = g.charms.find(c => c.outline === bodyB);
  assert(a && b, 'both bodies are outlines'); assert(a.members.includes(ringA) && b.members.includes(ringB), 'each ring joins its own body');
}
console.log('Artwork-layer body OK: a black body on HATCH/ENGRAVE is the outline (ring welded, fill kept); a border, inner frame or box beside a real cut body stays artwork; two-charm master keeps each ring with its body');
