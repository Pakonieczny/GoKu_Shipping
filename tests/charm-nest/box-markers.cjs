// The grey boxes above and beside charms. A master carries notes for people next to each charm (a size badge on the
// LABELS layer, a grey "12 mm" with its two bracket lines, a remark such as "necklace + choker"). The grouping used to
// take them as the charm's own detail; the viewers cannot paint live text or a gradient, so each one was drawn as a grey
// placeholder box and also stretched the charm's size, silhouette and per-SKU file. groupCharms now leaves them out.
//   node tests/charm-nest/box-markers.cjs
// The fixture (fixtures/box-markers-masters.json) holds five real charms cut out of the three MASTER SKU_*_MV_2026-0914.ai
// files, every object the master's grouping held for each: the three order cards Paul sent (saxophone
// huggie, curb, rubber duck huggie) and two library cards (the Oklahoma seal with its callout, the Taekwondo belt whose
// sample text sits ON the piece and must stay).
const path = require('path'), assert = require('assert');
const { CharmNestPDF: P, Geom: G } = require('../../netlify/functions/_charmNestPdf.js');
const fx = require('./fixtures/box-markers-masters.json');
const MM = 25.4 / 72;

// keepSampleText isolates this rule from the on-piece sample-text rule (takeSampleText), which has its own test
const run = (ex, opts) => {
  const segs = JSON.parse(JSON.stringify(ex.segments));
  const p = { pageW: fx.pageW, pageH: fx.pageH, mediaBox: [0, 0, fx.pageW, fx.pageH], segments: segs.map((s, i) => Object.assign(s, { index: i })), nested: [] };
  const g = P.groupCharms(p, Object.assign({ minPt: 6, keepSampleText: true }, opts));
  const area = c => (c.outline.bbox[2] - c.outline.bbox[0]) * (c.outline.bbox[3] - c.outline.bbox[1]);
  const c = g.charms.slice().sort((a, b) => area(b) - area(a))[0];
  assert(c, ex.key + ': the charm is found');
  const sil = G.silhouetteBits(c, 6, {});
  return {
    g, c, w: (sil.bboxOuter[2] - sil.bboxOuter[0]) * MM, h: (sil.bboxOuter[3] - sil.bboxOuter[1]) * MM, area: sil.areaPt2 * MM * MM,
    holes: P.cutLinesOf(c).length, kinds: c.members.map(m => m.kind),
    markers: (g.markers || []).filter(m => m.charm === c.index),
  };
};
const near = (a, b, tol, what) => assert(Math.abs(a - b) <= (tol || 0.06), `${what}: ${a.toFixed(2)} should be ${b}`);
const ex = key => fx.examples.find(e => e.key === key);
const whys = r => r.markers.map(m => m.why).sort().join(',');

// 1 · the three order cards: the size on the card is the size with the box, and the box is the badge or note
{
  const old = run(ex('saxophone-huggie'), { keepMarkers: true }), now = run(ex('saxophone-huggie'));
  near(old.h, 20.6, 0.05, 'saxophone huggie card height before'); near(old.w, 5.6, 0.15, 'saxophone huggie card width before');
  assert(old.kinds.filter(k => k === 'shading').length === 2 && old.kinds.includes('text'), 'before: the gradient chip (two shapes) and the "12" are members of the charm');
  assert.strictEqual(whys(now), 'size badge (gradient),size badge (gradient),size badge text', 'the saxophone huggie box is a size badge: chip + number');
  assert(now.kinds.every(k => k === 'path'), 'after: only drawn paths are left on the charm');
  near(now.h, 11.06, 0.06, 'saxophone huggie height after'); assert(now.h < old.h - 9, 'the charm lost its 9.5 mm of box');
  assert.strictEqual(now.holes, old.holes, 'hole count unchanged');
}
{
  const old = run(ex('curb'), { keepMarkers: true }), now = run(ex('curb'));
  near(old.w, 20.5, 0.25, 'curb card width before'); near(old.h, 7.2, 0.05, 'curb card height before');
  assert(old.kinds.includes('text'), 'before: the remark is a member of the charm');
  assert.strictEqual(whys(now), 'note text', 'the curb box is a remark ("necklace + choker")');
  assert.strictEqual(now.markers[0].seg.str, 'necklace + choker', 'the remark text');
  near(now.w, 15.88, 0.06, 'curb width after'); near(now.h, 3.88, 0.06, 'curb height after'); assert.strictEqual(now.holes, 2, 'the curb keeps its two holes');
  const o = now.c.outline.bbox, b = now.c.bbox;
  assert(b[0] >= o[0] - 1e-6 && b[1] >= o[1] - 1e-6 && b[2] <= o[2] + 1e-6 && b[3] <= o[3] + 1e-6, 'the charm box is its own outline again');
}
{
  const old = run(ex('rubber-duck-huggie'), { keepMarkers: true }), now = run(ex('rubber-duck-huggie'));
  near(old.h, 15.04, 0.05, 'rubber duck card height before'); near(old.w, 6.83, 0.05, 'rubber duck card width before');
  assert.strictEqual(whys(now), 'size badge (gradient),size badge (gradient)', 'the duck box is a size badge: a gradient chip and its numeral');
  near(now.h, 9.39, 0.06, 'rubber duck height after'); near(now.w, old.w, 0.01, 'rubber duck width unchanged');
  assert.strictEqual(now.holes, 1, 'the duck keeps its ring hole'); near(now.area, old.area, 0.5, 'silhouette area unchanged');
}

// 2 · library cards: the callout beside the seal (text + two bracket lines) goes, the seal stays whole
{
  const old = run(ex('oklahoma-seal'), { keepMarkers: true }), now = run(ex('oklahoma-seal'));
  assert.strictEqual(whys(now), 'dimension line,dimension line,dimension text', 'a grey measurement and its two brackets');
  near(old.w, 18.69, 0.06, 'seal width before'); near(now.w, 13.15, 0.06, 'seal width after'); near(now.h, 14.91, 0.06, 'seal height after');
  assert(now.markers.some(m => m.why === 'dimension text' && /mm$/.test(m.seg.str)), 'the measurement text is "NN mm"');
}

// 3 · text ON the piece is not a marker: the Taekwondo belt keeps its sample text; only the callout beside it goes
{
  const old = run(ex('taekwondo-belt'), { keepMarkers: true }), now = run(ex('taekwondo-belt'));
  const texts = r => r.c.members.filter(m => m.kind === 'text').length;
  assert(texts(old) > 10 && texts(now) === texts(old) - 1, `design text stays (${texts(now)} runs of ${texts(old)}); only the callout text goes`);
  assert(now.markers.every(m => m.why === 'dimension text' || m.why === 'dimension line'), 'only the callout is taken: ' + whys(now));
  assert.strictEqual(now.holes, old.holes, 'hole count unchanged');
  near(now.w, 12.08, 0.06, 'belt width after'); near(now.h, 10.69, 0.06, 'belt height after');
}

// 4 · the rule itself, on single objects
{
  const piece = [0, 0, 20, 20];
  const text = (bbox, o) => Object.assign({ kind: 'text', bbox, str: 'x', fillRGB: [0, 0, 0], layer: 'CUT' }, o);
  assert.strictEqual(P.markerReason(text([2, 2, 10, 6]), piece), null, 'text on the piece stays');
  assert.strictEqual(P.markerReason(text([2, 22, 10, 26]), piece), 'note text', 'text wholly beside the piece is a note');
  assert.strictEqual(P.markerReason(text([2, 22, 10, 26], { str: '12.5 mm', fillRGB: [0.47, 0.47, 0.47] }), piece), 'dimension text');
  assert.strictEqual(P.markerReason(text([2, 16, 10, 28], { fillRGB: [0.47, 0.47, 0.47] }), piece), 'note text', 'grazing grey text is a note');
  assert.strictEqual(P.markerReason(text([2, 16, 10, 28], { fillRGB: [0, 0, 1], layer: 'HATCH' }), piece), null, 'grazing blue engraving text is left to the text rule');
  assert.strictEqual(P.markerReason({ kind: 'shading', bbox: [5, 22, 15, 32], layer: 'LABELS' }, piece), 'size badge (gradient)');
  assert.strictEqual(P.markerReason({ kind: 'shading', bbox: [5, 5, 15, 15], layer: 'CUT' }, piece), null, 'a gradient inside the piece is artwork');
  const bracket = { kind: 'path', stroke: true, fill: false, closed: false, strokeRGB: [0.8, 0.8, 0.8], lwPt: 0.14, bbox: [0, -3, 20, -1], subpaths: [[['m', [0, -1]], ['l', [0, -3]], ['l', [20, -3]], ['l', [20, -1]]]] };
  const square = [[[0, 0], [20, 0], [20, 20], [0, 20], [0, 0]]];
  assert.strictEqual(P.markerReason(bracket, piece, square), 'dimension line');
  assert.strictEqual(P.markerReason(Object.assign({}, bracket, { strokeRGB: [0, 0, 0] }), piece, square), null, 'a black line is a cut or an engraving, never a bracket');
  assert.strictEqual(P.markerReason(Object.assign({}, bracket, { closed: true }), piece, square), null, 'a closed shape is not a bracket');
}
console.log('box-markers OK · 5 real charms from the masters · the badge, callout and remark boxes are gone from size, silhouette and file');
