// Paul, 10 Oct 2026, the oblong bar BIRTH_3611 (a poppy and a morning glory drawn as black hairlines): "All of these engravings should be Blue hatching".
// The master draws them as black STROKES on the CUT layer (open lines, and closed leaf shapes the veins end on), not as fills, so the black-fill rule never saw
// them: black in every picture, written black in the per-SKU file (a laser would cut the lines), and the closed ones counted as holes ("5 holes" on a bar
// with 2 hoop holes, and candidates for the hanging hole that decides the up angle). charm-nest-pdf.js "black LINE ART is blue hatching" reads them as hatching:
// drawn blue, written as a blue area of the line, never a hole. The cut outline, hoop holes, a cut that divides the charm and a window stay black cut.
//   node tests/charm-nest/black-art.cjs
const assert = require('node:assert/strict');
global.self = global; global.PDFLib = require('../../vendor/pdf-lib-1.17.1.min.js'); require('../../charm-nest-pdf.js');
const P = CharmNestPDF, E = require('../../charm-nest-export.js'), G = require('../../charm-nest-geom.js');
const K = 0.5523;
const blob = (cx, cy, w, h) => { const a = w / 2, b = h / 2; return [['m', [cx + a, cy]], ['c', [cx + a, cy + K * b], [cx + K * a, cy + b], [cx, cy + b]], ['c', [cx - K * a, cy + b], [cx - a, cy + K * b], [cx - a, cy]], ['c', [cx - a, cy - K * b], [cx - K * a, cy - b], [cx, cy - b]], ['c', [cx + K * a, cy - b], [cx + a, cy - K * b], [cx + a, cy]], ['h']]; };
const rect = (x0, y0, x1, y1) => [['m', [x0, y0]], ['l', [x1, y0]], ['l', [x1, y1]], ['l', [x0, y1]], ['h']];
const line = (...pts) => pts.map((p, i) => [i ? 'l' : 'm', p]);
const bboxOf = subs => { const pts = subs.flat().flatMap(o => o.slice(1)); return [Math.min(...pts.map(p => p[0])), Math.min(...pts.map(p => p[1])), Math.max(...pts.map(p => p[0])), Math.max(...pts.map(p => p[1]))]; };
const path_ = (layer, subpaths, o = {}) => Object.assign({ kind: 'path', layer, subpaths, bbox: bboxOf(subpaths), closed: true, stroke: true, fill: false, strokeRGB: [0, 0, 0], fillRGB: [0, 0, 0], lwPt: 0.283, paintOp: 'S', depth: 0 }, o);
const open_ = (layer, ...pts) => path_(layer, [line(...pts)], { closed: false });
const parsedOf = segs => ({ segments: segs.map((s, i) => Object.assign(s, { index: i, start: i * 10, end: i * 10 + 9 })), nested: [], pageW: 260, pageH: 140 });
const group = (segs, opts) => { const parsed = parsedOf(segs); return { parsed, g: P.groupCharms(parsed, Object.assign({ minPt: 6 }, opts || {})) }; };
const recorder = () => { const rec = { fills: [], strokes: [] }; const s = { fillStyle: '#000', strokeStyle: '#000', lineWidth: 1 };
  const ctx = new Proxy(s, { get: (o, k) => k === 'fill' ? () => rec.fills.push(o.fillStyle) : k === 'stroke' ? () => rec.strokes.push(o.strokeStyle) : k in o ? o[k] : () => {}, set: (o, k, v) => { o[k] = v; return true; } });
  return { ctx, rec }; };
const draw = c => { const { ctx, rec } = recorder(); P.drawCharm(ctx, c, (x, y) => [x, y], 4); return rec; };
const holesOf = c => P.cutLinesOf(c).length;
const BLUE = 'rgb(0,0,255)';
const plain = p => { const { doc, page, ...rest } = p; return rest; };
const viaWorker = parsed => P.adoptGrouping(structuredClone(P.groupForTransfer(structuredClone(plain(parsed)), { minPt: 6 })), parsed);

// the bar: a rounded black cut outline 100 x 20 pt, a hoop hole at each end, and flower line art at the right: stems and veins (open lines), a leaf (closed) the veins end on
const bar = () => {
  const body = path_('CUT', [rect(10, 10, 110, 30)]), hoopL = path_('CUT', [blob(16, 20, 4, 4)]), hoopR = path_('CUT', [blob(104, 20, 4, 4)]);
  const stem = open_('CUT', [80, 10], [80, 18], [79, 24]), vein1 = open_('CUT', [79, 24], [74, 27]), vein2 = open_('CUT', [79, 24], [85, 27]), vein3 = open_('CUT', [80, 17], [88, 19]);
  const leaf = path_('CUT', [[['m', [70, 22]], ['l', [74, 27]], ['l', [73, 20]], ['h']]]);
  const stray = path_('CUT', [blob(50, 20, 6, 6)]);                 // a closed shape far from the art: a window (stays a cut-out)
  return { body, hoopL, hoopR, stem, vein1, vein2, vein3, leaf, stray };
};

(async () => {
  // 1 · the flowers are blue hatching: open lines and the closed leaf they end on; holes are the real ones; the cut line and hoop holes stay black cut
  {
    const b = bar(); const { g } = group([b.body, b.hoopL, b.hoopR, b.stem, b.vein1, b.vein2, b.vein3, b.leaf, b.stray]); assert.equal(g.charms.length, 1); const c = g.charms[0];
    assert.equal(c.outline, b.body, 'the black bar is the cut outline');
    for (const [name, m] of [['stem', b.stem], ['vein1', b.vein1], ['vein2', b.vein2], ['vein3', b.vein3], ['leaf', b.leaf]]) {
      assert(m.hatchBlue && m.hatchLine && m.manufacturingRole === 'hatch', name + ' is line art: hatching ' + JSON.stringify([m.hatchBlue, m.hatchLine, m.manufacturingRole]));
      assert.equal(G.pathRole(m), 'artwork', name + ': an artwork role'); assert(!G.isCutLine(m), name + ': not a cut line');
    }
    for (const [name, m] of [['hoop hole L', b.hoopL], ['hoop hole R', b.hoopR], ['window', b.stray]]) assert(!m.hatchBlue && !m.manufacturingRole, name + ' is left as the cut-out it is');
    assert.equal(holesOf(c), 3, 'the holes are the two hoop holes and the window, not the leaf');
    const rec = draw(c); assert.equal(rec.strokes.filter(s => s === BLUE).length, 5, 'the five art lines are drawn blue: ' + JSON.stringify(rec.strokes)); assert.deepEqual(rec.fills, [], 'no fill'); assert.equal(rec.strokes.filter(s => s !== BLUE && s !== '#000' && s !== 'rgb(0,0,0)').length, 0, 'everything else is the black cut pen');
    // the back view (engraving card) holds the cut members only: no flower on it
    const v = G.backView(c, { res: 6, holeOnly: true }); assert(!v.members.some(m => [b.stem, b.leaf, b.vein1].includes(m.original || m)), 'the flowers are not on the back view');
    // up angle: the hanging hole is one of the real holes, never a leaf
    assert(![b.leaf].includes(G.holeUpAngle ? (G.holeUpAngle(c).hole) : null), 'the leaf is not the hanging hole');
  }

  // 2 · what stays as it was: a cut that divides the charm, a hoop drawn as an open ring, a lone closed window, a window touching the art, a line outside, opts.blackArt === false
  {
    const body = path_('CUT', [rect(10, 10, 110, 50)]), crack = [open_('CUT', [60, 10], [55, 20]), open_('CUT', [55, 20], [65, 30]), open_('CUT', [65, 30], [60, 50])];
    const { g } = group([body, ...crack]); const c = g.charms[0];
    assert(crack.every(m => !m.hatchBlue && !m.manufacturingRole), 'pieces joined end to end whose two ends stop on the outline are a cut that divides the charm (BEST FRIENDS_2505)');
    const body2 = path_('CUT', [rect(10, 10, 110, 50)]), half = open_('CUT', [60, 10], [60, 30]); group([body2, half]); assert(half.hatchLine, 'a line that stops on the outline at one end only is a stem: line art');
    const body3 = path_('CUT', [rect(10, 10, 110, 50)]), arc = path_('CUT', [blob(70, 30, 10, 10).slice(0, -1)], { closed: false }); group([body3, arc]); assert(!arc.hatchLine, 'a hoop drawn as an open ring is not line art');
    const body4 = path_('CUT', [rect(10, 10, 110, 50)]), win = path_('CUT', [blob(60, 30, 10, 10)]); const r4 = group([body4, win]); assert(!win.hatchBlue && holesOf(r4.g.charms[0]) === 1, 'a closed shape with no line art is a cut-out');
    const body5 = path_('CUT', [rect(10, 10, 110, 50)]), veins = open_('CUT', [30, 30], [40, 33]), big = path_('CUT', [rect(40, 12, 100, 48)]); group([body5, veins, big]); assert(veins.hatchLine && !big.hatchBlue, 'a closed shape covering a third of the charm is a window beside the art');
    const body6 = path_('CUT', [rect(10, 10, 110, 50)]), off = open_('CUT', [30, 30], [40, 33]); const r6 = group([body6, off], { blackArt: false }); assert(!off.hatchBlue && draw(r6.g.charms[0]).strokes.every(s => s === '#000' || s === 'rgb(0,0,0)'), 'opts.blackArt === false is the reader before this rule: black');
    const body7 = path_('CUT', [rect(10, 10, 110, 50)]), red = open_('CUT', [30, 30], [40, 33], {}); red.strokeRGB = [1, 0, 0]; group([body7, red]); assert(!red.hatchBlue, 'a red line is not black ink (the red-line rules are the other worker\'s)');
    const body8 = path_('CUT', [rect(10, 10, 110, 50)]), eng = open_('ENGRAVE', [30, 30], [40, 33]); group([body8, eng]); assert(!eng.hatchBlue && !eng.manufacturingRole, 'black ink on an engraving layer is left to the layer rule');
  }

  // 3 · the browser groups in a worker and hands the page its own segments back: the stamp of the line art travels with them (the earlier fault of every grouping stamp)
  {
    const b = bar(); const segs = [b.body, b.hoopL, b.hoopR, b.stem, b.vein1, b.vein2, b.vein3, b.leaf]; const parsed = parsedOf(segs);
    const g = viaWorker(parsed); const c = g.charms[0];
    assert(c.members.filter(m => m.hatchLine).length === 5 && c.members.every(m => !m.hatchLine || m.hatchBlue), 'the page\'s own segments carry hatchLine from the worker');
    assert.equal(holesOf(c), 2, 'the app reads two holes, as the server does'); assert.equal(draw(c).strokes.filter(s => s === BLUE).length, 5, 'and draws the art blue');
  }

  // 4 · the per-SKU file and the sheet carry the art as a BLUE AREA of the line (no black line is written), in the layer the master drew it in; read back, it is blue hatching with no black art left
  {
    const f3 = v => (Math.round(v * 1000) / 1000).toString();
    const ops = subs => subs.map(sp => sp.map(o => o[0] === 'h' ? 'h' : o[0] === 'c' ? [1, 2, 3].map(i => f3(o[i][0]) + ' ' + f3(o[i][1])).join(' ') + ' c' : f3(o[1][0]) + ' ' + f3(o[1][1]) + ' ' + o[0]).join(' ')).join(' ');
    const b = bar(), art = [b.stem, b.vein1, b.vein2, b.vein3, b.leaf];
    // a translated matrix around the leaf, as Illustrator leaves it; every other object is written in page space
    const content = `/OC /MC0 BDC 0 0 0 RG 0.283 w ${ops(b.body.subpaths)} S ${ops(b.hoopL.subpaths)} S ${ops(b.hoopR.subpaths)} S ${[b.stem, b.vein1, b.vein2, b.vein3].map(m => ops(m.subpaths) + ' S').join(' ')} q 1 0 0 1 5 7 cm ${ops([b.leaf].map(m => m.subpaths.map(sp => sp.map(o => o[0] === 'h' ? o : [o[0], [o[1][0] - 5, o[1][1] - 7]]))).flat())} S Q EMC\n`;
    const doc = await PDFLib.PDFDocument.create(), page = doc.addPage([140, 60]); page.node.normalize();
    page.node.Resources().set(PDFLib.PDFName.of('Properties'), doc.context.obj({ MC0: doc.context.register(doc.context.obj({ Type: 'OCG', Name: PDFLib.PDFString.of('CUT') })) }));
    page.node.addContentStream(doc.context.register(doc.context.flateStream(new TextEncoder().encode(content))));
    const parsed = await P.parseSource(await doc.save(), 'bar-master'), g = P.groupCharms(parsed, { minPt: 6 }); assert.equal(g.charms.length, 1, 'one charm in the master'); const c = g.charms[0];
    const stamped = c.members.filter(m => m.hatchLine); assert.equal(stamped.length, 5, 'the five art strokes are stamped in the master'); assert.equal(holesOf(c), 2, 'two holes');
    const file = await P.buildSingleCharm(Object.assign({}, c, { name: 'bar' }), parsed), back = await P.parseSource(file, 'bar file'), g2 = P.groupCharms(back, { minPt: 6 });
    const c2 = g2.charms.reduce((a, x) => x.members.length > a.members.length ? x : a);
    const blueFill = m => m.kind === 'path' && m.fill && !m.stroke && m.fillRGB[0] === 0 && m.fillRGB[1] === 0 && m.fillRGB[2] === 1;
    const areas = c2.members.filter(blueFill); assert(areas.length >= 4 && areas.length <= 5, 'the art is written as blue fills: ' + areas.length);
    assert(!c2.members.some(m => m.kind === 'path' && m.stroke && !m.fill && !m.closed), 'and no black open line is written at all');
    assert.equal(holesOf(c2), 2, 'the file reads back with the same two holes');
    const ring = sp => sp.filter(o => o[0] !== 'h').map(o => o[1]), area = sp => Math.abs(ring(sp).reduce((a, p, k, r) => a + p[0] * r[(k + 1) % r.length][1] - r[(k + 1) % r.length][0] * p[1], 0) / 2);
    const hl = P.lineAreaOf(path_('CUT', [line([0, 0], [10, 0])], { closed: false })); assert.equal(hl.length, 1, 'an open line is one polygon'); assert(Math.abs(area(hl[0]) - 10 * 0.283) < 0.01, 'as wide as its line (0.283 pt = 0.1 mm): ' + area(hl[0]));
    const cl = P.lineAreaOf(path_('CUT', [rect(0, 0, 10, 10)])); assert.equal(cl.length, 2, 'a closed line is two rings'); assert(Math.abs(Math.abs(area(cl[0]) - area(cl[1])) - 4 * 10 * 0.283) < 0.05, 'a ring as wide as its line: ' + (area(cl[0]) - area(cl[1])));
    assert(P.lineAreaOf(path_('CUT', [line([0, 0], [10, 0])], { closed: false, lwPt: 2 }))[0].length > 2 && Math.abs(area(P.lineAreaOf(path_('CUT', [line([0, 0], [10, 0])], { closed: false, lwPt: 2 }))[0]) - 20) < 0.01, 'a wider line keeps its own width');
    const rel = (m, o) => [m.bbox[0] - o.bbox[0], m.bbox[1] - o.bbox[1], m.bbox[2] - o.bbox[0], m.bbox[3] - o.bbox[1]].map(v => Math.round(v)).join(','), before = art.map(m => rel(m, b.body)).sort(), after = areas.map(m => rel(m, c2.outline)).sort();
    assert.deepEqual(after.filter(x => before.includes(x)).length >= 4, true, 'the areas sit where the master drew the lines (the translated leaf included): ' + JSON.stringify([before, after]));
    assert(draw(c2).fills.length >= 4 && draw(c2).fills.every(f => f === BLUE), 'painted blue from the stored file');
    // the stored file is not decided again: nothing in it is black line art; written again from the file gives the same
    assert.equal(c2.members.filter(m => m.hatchLine).length, 0, 'nothing left to stamp in the file');
    // the DXF: a blue HATCH of the line's area, never a polyline of the hairline
    const dxf = E.dxf(E.productionPaths(parsed)).text; assert(/AcDbHatch/.test(dxf), 'the DXF holds hatches'); assert(!/AcDbPolyline[\s\S]{0,400}\r\n0\r\nLWPOLYLINE/.test('') && (dxf.match(/\r\nLWPOLYLINE\r\n/g) || []).length <= 3, 'and polylines only for the cut outline and the two hoop holes: ' + (dxf.match(/\r\nLWPOLYLINE\r\n/g) || []).length);
  }
  console.log('black-art: ok');
})().catch(e => { console.error(e); process.exit(1); });
