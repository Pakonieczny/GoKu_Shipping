// Paul, 9 Oct 2026: "always place longer items horizontally because typically that's how the text will be written."
// The engraving editor showed order 4174372408 (TINY BAR_7427, two hoops, words FCSIII) standing almost upright (83 degrees)
// with its words running up the piece: the back view turned every charm hoop-up, and a bar's hoop sits at a top corner.
// A long charm (long side >= 1.4 x short side) now starts with its long axis flat and its words at 0 degrees; every other
// charm keeps the hoop-up rule; an engraving that was saved, approved or moved by hand is never turned. Offline, no network.
const assert = require('node:assert/strict');
const G = require('../../charm-nest-geom.js'), Fit = require('../../charm-nest-engrave-fit.js'), ot = require('../../vendor/opentype-1.3.4.min.js');
const fonts = { Regular: ot.loadSync('vendor/fonts/SourceSans3-Regular.otf'), Semibold: ot.loadSync('vendor/fonts/SourceSans3-Semibold.otf') };

const rect = (x0, y0, x1, y1) => [['m', [x0, y0]], ['l', [x1, y0]], ['l', [x1, y1]], ['l', [x0, y1]], ['l', [x0, y0]]];
const circ = (cx, cy, r) => { const k = 0.5523 * r; return [['m', [cx + r, cy]], ['c', [cx + r, cy + k], [cx + k, cy + r], [cx, cy + r]], ['c', [cx - k, cy + r], [cx - r, cy + k], [cx - r, cy]], ['c', [cx - r, cy - k], [cx - k, cy - r], [cx, cy - r]], ['c', [cx + k, cy - r], [cx + r, cy - k], [cx + r, cy]]]; };
const turn = (sub, deg, cx, cy) => { const t = deg * Math.PI / 180, c = Math.cos(t), s = Math.sin(t); return sub.map(sg => [sg[0], ...sg.slice(1).map(p => [cx + (p[0] - cx) * c - (p[1] - cy) * s, cy + (p[0] - cx) * s + (p[1] - cy) * c])]); };
const bbOf = sub => { let x0 = 1e9, y0 = 1e9, x1 = -1e9, y1 = -1e9; for (const s of sub) for (let i = 1; i < s.length; i++) { const p = s[i]; x0 = Math.min(x0, p[0]); y0 = Math.min(y0, p[1]); x1 = Math.max(x1, p[0]); y1 = Math.max(y1, p[1]); } return [x0, y0, x1, y1]; };
const mem = sub => ({ kind: 'path', layer: 'CUT', stroke: true, fill: false, strokeRGB: [0, 0, 0], fillRGB: [0, 0, 0], lwPt: 0.25, paintOp: 'S', closed: true, subpaths: [sub], bbox: bbOf(sub) });
const charm = (body, holes) => { const o = mem(body), hs = holes.map(mem); return { outline: o, members: [o, ...hs], bbox: o.bbox, widthPt: o.bbox[2] - o.bbox[0], heightPt: o.bbox[3] - o.bbox[1] }; };
// TINY BAR as the front reference drew it: a long bar, a hoop at each top corner (350 x 60 px there: about 58 x 10 pt)
const tinyBar = () => charm(rect(0, 0, 58, 10), [circ(1.5, 9, 2), circ(56.5, 9, 2)]);
const viewShape = v => { const b = v.outline.bbox; return (b[2] - b[0]) / (b[3] - b[1]); };    // width / height of the piece as the editor shows it
const text = (c, lines, viewOptions) => Fit.calculate({ charm: c, lines, lineMode: 'auto', viewOptions, maskOptions: { marginMm: .8, keepOut: [] }, opts: { minStrokeMm: 0, minGapMm: 0, minCapMm: 1.6, maxHeightFrac: .4, lineGap: .18, tryRotated: true } }, fonts, G);

// 1 · the cause: the hoop-up rule stands the TINY BAR about 83 degrees up, and the words run up the piece (the screenshot)
{
  const bar = tinyBar();
  const old = G.backView(bar, { res: 6, holeOnly: true });
  assert(Math.abs(Math.abs(old.angleDeg) - 83) < 4, 'the original rule turns the bar about 83 degrees (' + old.angleDeg.toFixed(1) + ')');
  assert(viewShape(old) < 0.5, 'and so the bar is drawn upright');
  const stored = G.upAngleOf(bar, { holeOnly: true });                        // what the master index stored for it ("up 7")
  const was = text(bar, ['FCSIII'], { res: 6, upAngle: stored.angle });       // the old fit, from the stored library angle
  assert(Math.abs(was.fit.angle) >= 60, 'FCSIII was fitted along the vertical axis (' + was.fit.angle + ')');
}

// 2 · the fix: the same library entry (stored hole angle, not typed by an operator) now starts flat and the words read at 0
{
  const bar = tinyBar(), ax = G.longAxisOf(bar);
  assert(ax.isLong && ax.aspect > 5 && ax.angle === 0, 'the bar is long and its axis is horizontal');
  const up = G.upAngleOf(bar);
  assert.equal(up.source, 'long'); assert.equal(up.angle, 90, 'drawn flat already: nothing to turn');
  const entry = { sku: 'TINY BAR_7427', upAngle: G.upAngleOf(bar, { holeOnly: true }).angle, upSource: 'hole' };
  const vo = G.viewOptionsFor({ entry });
  assert.equal(vo.upAngle, undefined, 'the stored computed angle no longer decides'); assert(!vo.holeOnly);
  const r = text(bar, ['FCSIII'], vo);
  assert(r.fit && r.check.ok, 'FCSIII fits on the flat bar');
  assert(Math.abs(r.view.angleDeg) < 1e-6, 'view turn 0');
  assert(viewShape(r.view) > 5, 'the bar is shown with its long axis horizontal');
  assert.equal(r.fit.angle, 0, 'the words are placed along the long axis, left to right at 0 degrees');
  const b = r.fit.layout.bbox; assert((b[2] - b[0]) > 2 * (b[3] - b[1]), 'the lettering is wider than it is tall');
  assert(r.fit.capMm > 0.8, 'readable lettering (' + r.fit.capMm.toFixed(2) + ' mm)');
}

// 3 · long charms drawn any other way up (master drawn upright or slanted, one hoop, no hoop) lie flat too
{
  const flat = v => assert(viewShape(v) > 1.39, 'flat, width/height ' + viewShape(v).toFixed(2));
  for (const d of [90, -90, 30, 135, -45, 200]) {
    const body = turn(rect(0, 0, 58, 10), d, 29, 5), hoops = [turn(circ(1.5, 9, 2), d, 29, 5), turn(circ(56.5, 9, 2), d, 29, 5)];
    const c = charm(body, hoops), v = G.backView(c, { res: 6 }), r = text(c, ['FCSIII'], G.viewOptionsFor({ entry: {} }));
    flat(v); assert.equal(r.fit.angle, 0, 'turned ' + d + ': words at 0'); flat(r.view);
  }
  const oneHoop = charm(rect(0, 0, 10, 16), [circ(5, 14, 1.5)]); flat(G.backView(oneHoop, { res: 6 }));           // a tall tag with its hoop at the short end
  const noHole = charm(turn(rect(0, 0, 40, 12), 90, 20, 6), []); flat(G.backView(noHole, { res: 6 }));
  const asym = charm(rect(0, 0, 40, 14), [circ(3, 11, 1.5)]); const va = G.backView(asym, { res: 6 }); flat(va);   // one hoop off-centre
}

// 4 · charms that are not long keep their behaviour exactly
{
  const keep = c => { const a = G.upAngleOf(c), b = G.upAngleOf(c, { holeOnly: true }); assert.equal(a.angle, b.angle); assert.notEqual(a.source, 'long'); assert.deepEqual(G.backView(c, { res: 6 }).angleDeg, G.backView(c, { res: 6, holeOnly: true }).angleDeg); };
  keep(charm(rect(0, 0, 12, 10), [circ(6, 8.5, 1.5)]));                              // 1.2 : 1
  keep(charm(circ(10, 10, 10), [circ(10, 18, 1.5)]));                                // round
  keep(charm(rect(0, 0, 10, 10), [circ(2, 8, 1)]));                                  // square, hoop at a corner
  keep(charm(rect(0, 0, 13.9, 10), [circ(6, 8.5, 1.5)]));                            // 1.39 : 1, just under the threshold
  const justOver = charm(rect(0, 0, 14.1, 10), [circ(7, 8.5, 1.5)]); assert.equal(G.upAngleOf(justOver).source, 'long', '1.41 : 1 is long');
  assert.equal(G.LONG_ASPECT, 1.4);
}

// 5 · nothing saved, approved or typed by hand is ever turned
{
  const bar = tinyBar(), saved = G.upAngleOf(bar, { holeOnly: true }).angle;      // the angle a decision made before this rule recorded
  const vo = G.viewOptionsFor({ editingBack: true, savedUp: saved, charmUp: 90 });
  assert.equal(vo.upAngle, saved); assert.equal(vo.holeOnly, true);
  assert.equal(G.backView(bar, { res: 6, ...vo }).angleDeg, G.backView(bar, { res: 6, upAngle: saved }).angleDeg, 'a saved placement keeps its turn');
  assert(Math.abs(Math.abs(G.backView(bar, { res: 6, ...vo }).angleDeg) - 83) < 8, '(the saved turn is the old, upright one)');
  const none = G.viewOptionsFor({ editingBack: true, savedUp: null, charmUp: null });         // no angle recorded: the original rule, never the new default
  assert.equal(none.upAngle, undefined); assert.equal(none.holeOnly, true);
  assert.equal(G.backView(bar, { res: 6, ...none }).angleDeg, G.backView(bar, { res: 6, holeOnly: true }).angleDeg);
  assert.equal(G.viewOptionsFor({ entry: { upAngle: 200, upSource: 'operator' } }).upAngle, 200, 'an angle an operator typed on the library card stands');
  const moved = G.viewOptionsFor({ nudged: true, oriented: false, viewUp: 6.9, entry: {} });   // moved or turned by hand on the old view, not yet re-made
  assert.equal(moved.upAngle, 6.9); assert.equal(moved.holeOnly, true);
  assert.equal(G.viewOptionsFor({ nudged: true, oriented: true, viewUp: 90, entry: {} }).upAngle, undefined, 'once made with the rule, a nudge no longer pins the old angle');
}

// 6 · which undecided placements are made again when a saved workspace comes back
{
  const j = (o) => Object.assign({ state: 'review' }, o);
  assert(G.orientStale(j({})), 'an undecided placement fitted before the rule is made again');
  assert(G.orientStale(j({ state: 'blocked' })) && G.orientStale(j({ state: 'fitting' })));
  for (const state of ['approved', 'written', 'skipped', 'none', 'words', 'ready']) assert(!G.orientStale(j({ state })), state + ' is left alone');
  assert(!G.orientStale(j({ nudged: true })), 'one a person moved or turned is left alone');
  assert(!G.orientStale(j({ orientVersion: G.ORIENT })), 'already made with the rule');
  assert(!G.orientStale(j({ editingBack: true })), 'a saved back being reopened is never made again');
}

console.log('long-flat: ok');
