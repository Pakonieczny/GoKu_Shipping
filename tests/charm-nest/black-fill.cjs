// Paul's rule for the master drawings: black means a cut LINE only; a black FILLED area is engraving drawn in the wrong colour
// and is blue hatching (charm-nest-pdf.js "a black FILL is blue hatching": classifyBlackFills). Candy_20812 (Huggie)'s black stripes,
// the BIRTHFLOWER line art, CHEVRON_7834 (Huggie)'s black bars and CHEVRON_9294 / CHEVRON_7834's black ring and bars were read as
// holes ("7 holes"), black bodies or black ink. A plain charm drawn as one black fill (WOLF + MOON, MAPLE_4007, MALE SYMBOL, every
// letter) is still only its cut outline: no charm may come out as a black picture.
//   node tests/charm-nest-pdf.js is not needed: node tests/charm-nest/black-fill.cjs
const assert = require('node:assert/strict');
const fs = require('fs'), path = require('path');
global.self = global; global.PDFLib = require('../../vendor/pdf-lib-1.17.1.min.js'); require('../../charm-nest-pdf.js');
const P = CharmNestPDF, E = require('../../charm-nest-export.js'), G = require('../../charm-nest-geom.js');
const K = 0.5523;
const blob = (cx, cy, w, h) => { const a = w / 2, b = h / 2; return [['m', [cx + a, cy]], ['c', [cx + a, cy + K * b], [cx + K * a, cy + b], [cx, cy + b]], ['c', [cx - K * a, cy + b], [cx - a, cy + K * b], [cx - a, cy]], ['c', [cx - a, cy - K * b], [cx - K * a, cy - b], [cx, cy - b]], ['c', [cx + K * a, cy - b], [cx + a, cy - K * b], [cx + a, cy]], ['h']]; };
const rect = (x0, y0, x1, y1) => [['m', [x0, y0]], ['l', [x1, y0]], ['l', [x1, y1]], ['l', [x0, y1]], ['h']];
const rectCW = (x0, y0, x1, y1) => [['m', [x0, y0]], ['l', [x0, y1]], ['l', [x1, y1]], ['l', [x1, y0]], ['h']];   // wound the other way: a hole of a non-zero fill
const blobCW = (cx, cy, w, h) => { const a = w / 2, b = h / 2; return [['m', [cx + a, cy]], ['c', [cx + a, cy - K * b], [cx + K * a, cy - b], [cx, cy - b]], ['c', [cx - K * a, cy - b], [cx - a, cy - K * b], [cx - a, cy]], ['c', [cx - a, cy + K * b], [cx - K * a, cy + b], [cx, cy + b]], ['c', [cx + K * a, cy + b], [cx + a, cy + K * b], [cx + a, cy]], ['h']]; };
const bboxOf = subs => { const pts = subs.flat().flatMap(o => o.slice(1)); return [Math.min(...pts.map(p => p[0])), Math.min(...pts.map(p => p[1])), Math.max(...pts.map(p => p[0])), Math.max(...pts.map(p => p[1]))]; };
const path_ = (layer, subpaths, o = {}) => Object.assign({ kind: 'path', layer, subpaths, bbox: bboxOf(subpaths), closed: true, stroke: true, fill: false, strokeRGB: [0, 0, 0], fillRGB: [0, 0, 0], lwPt: 0.28, paintOp: 'S', depth: 0 }, o);
const blackFill = (layer, subpaths, o = {}) => path_(layer, subpaths, Object.assign({ stroke: false, fill: true, paintOp: 'f' }, o));
const parsedOf = segs => ({ segments: segs.map((s, i) => Object.assign(s, { index: i, start: i * 10, end: i * 10 + 9 })), nested: [], pageW: 160, pageH: 120 });
const group = (segs, opts) => { const parsed = parsedOf(segs); return { parsed, g: P.groupCharms(parsed, Object.assign({ minPt: 6 }, opts || {})) }; };
const recorder = () => { const rec = { fills: [], strokes: [] }; const s = { fillStyle: '#000', strokeStyle: '#000', lineWidth: 1 };
  const ctx = new Proxy(s, { get: (o, k) => k === 'fill' ? () => rec.fills.push(o.fillStyle) : k === 'stroke' ? () => rec.strokes.push({ color: o.strokeStyle, w: o.lineWidth }) : k in o ? o[k] : () => {}, set: (o, k, v) => { o[k] = v; return true; } });
  return { ctx, rec }; };
const draw = c => { const { ctx, rec } = recorder(); P.drawCharm(ctx, c, (x, y) => [x, y], 4); return rec; };
const BLUE = 'rgb(0,0,255)', BLACK = 'rgb(0,0,0)';
const holesOf = c => P.cutLinesOf(c).length;
// the disc of CHEVRON_7834: one perfect circle holding black art (a thick ring and bars), with white strips between the bars
const discArt = (cx, cy, R) => [blob(cx, cy, 2 * R, 2 * R), rectCW(cx - 0.5 * R, cy + 0.45 * R, cx + 0.5 * R, cy + 0.75 * R), rectCW(cx - 0.5 * R, cy - 0.15 * R, cx + 0.5 * R, cy + 0.15 * R), rectCW(cx - 0.5 * R, cy - 0.75 * R, cx + 0.5 * R, cy - 0.45 * R)];

(async () => {
  // 1 · black fills inside a stroked charm are hatching, on any layer (Candy_20812 (Huggie): ENGRAVE; black line art: HATCH; a fill left on CUT)
  for (const layer of ['ENGRAVE', 'HATCH', 'CUT']) {
    const body = path_('CUT', [blob(40, 40, 30, 36)]), a = blackFill(layer, [blob(36, 44, 6, 8)]), b = blackFill(layer, [blob(44, 34, 6, 8)]);
    const { g } = group([body, a, b]); const c = g.charms[0];
    assert.equal(c.outline, body, layer + ': the stroked body is the outline');
    for (const m of [a, b]) { assert(m.hatchBlue && m.manufacturingRole === 'hatch', layer + ': a black fill inside is hatching'); assert.equal(G.pathRole(m), 'artwork', layer + ': an artwork role'); assert(!G.isCutLine(m), layer + ': not a cut line'); }
    assert.equal(holesOf(c), 0, layer + ': not counted as holes');
    const rec = draw(c); assert.deepEqual(rec.fills, [BLUE, BLUE], layer + ': painted blue hatching, never black');
    assert(!P.isCutSilhouetteFill(c, a), layer + ': not a silhouette');
  }

  // 2 · the outline's filled twin: the whole silhouette again stays a cut line (WOLF_89694); a twin that is only a ring and bars is hatching (CHEVRON_7834 (Huggie))
  {
    const outline = path_('CUT', [blob(40, 40, 30, 36)]), twin = blackFill('CUT', [blob(40, 40, 30, 36)]);
    const { g } = group([outline, twin]); const c = g.charms[0];
    assert(P.isCutSilhouetteFill(c, twin) && !twin.hatchBlue, 'a full-coverage twin is the silhouette, not hatching');
    assert.deepEqual(draw(c).fills, [], 'no fill is painted for it');
  }
  {
    const R = 15, outline = path_('CUT', [blob(40, 40, 2 * R, 2 * R)]), art = blackFill('CUT', discArt(40, 40, R));
    const { g } = group([outline, art]); const c = g.charms[0];
    assert.equal(c.outline, outline); assert(art.hatchBlue && !P.isCutSilhouetteFill(c, art), 'a ring and bars with the outline\'s own box (about 60 % of it) are hatching');
    assert.equal(holesOf(c), 0, 'the white strips are not holes'); assert.deepEqual(draw(c).fills, [BLUE]);
  }

  // 3 · a plain charm that is only a black fill stays a cut outline: WOLF + MOON, MAPLE_4007, a letter (one counter), MALE SYMBOL (a ring)
  for (const [what, subs] of [['a solid shape', [blob(40, 40, 24, 30)]], ['a letter with a counter', [blob(40, 40, 24, 30), blobCW(40, 40, 8, 12)]], ['a ring symbol', [blob(40, 40, 30, 30), blobCW(40, 40, 16, 16)]]]) {
    const a = blackFill('CUT', subs); const { g } = group([a]); const c = g.charms[0];
    assert.equal(c.outline, a, what); assert(!a.hatchBlue && a.manufacturingRole === undefined, what + ': not hatching');
    assert(P.isCutSilhouetteFill(c, a), what + ': the cut silhouette'); const rec = draw(c); assert.deepEqual(rec.fills, [], what + ': no black body'); assert(rec.strokes.every(s => s.color === '#000' || s.color === BLACK));
  }

  // 4 · a fill-only round body with art in it is a disc: the circle is the cut line, the fill is hatching, nothing is a hole (CHEVRON_9294, CHEVRON_7834, the mountain disc)
  {
    const disc = blackFill('CUT', discArt(40, 40, 15)); const { g } = group([disc]); const c = g.charms[0];
    assert.notEqual(c.outline, disc, 'the circle is now the outline'); assert(c.outline.synthetic && c.outline.stroke && !c.outline.fill && c.outline.subpaths.length === 1, 'a stroked circle'); assert.equal(c.outline.index, undefined, 'no index of its own: the writer keeps the master\'s fill by the fill\'s index');
    assert(disc.hatchBlue && c.members.includes(disc) && c.members.includes(c.outline), 'the fill stays a member, as hatching');
    assert.equal(holesOf(c), 0, 'no holes'); const rec = draw(c); assert.deepEqual(rec.fills, [BLUE], 'blue hatching'); assert(rec.strokes.length >= 1 && rec.strokes.every(s => s.color === '#000' || s.color === BLACK), 'black cut circle');
    assert(P.engravedDiscOf({ outline: disc }) >= 0, 'the finder names the circle of the fill');
    // with the hoop beside it: welded to the circle, the fill still there, and the weld cannot drop the fill from the file
    const { g: g2 } = group([blackFill('CUT', discArt(40, 40, 15)), path_('CUT', [blob(40, 57.5, 5, 5)], { lwPt: 0.28 })]);
    const c2 = g2.charms.find(k => k.members.length >= 2); const fill = c2.members.find(m => m.hatchBlue);
    const w = P.integrateRings(c2); assert.equal(w.left.length, 0); assert(w.welded >= 1, 'the hoop is welded to the circle');
    assert(c2.members.includes(fill) && fill.hatchBlue, 'the hatching survives the weld'); assert(!(c2.dropIndices && c2.dropIndices.has(fill.index)), 'the fill\'s index is not dropped');
    assert(holesOf(c2) <= 1, 'only the hoop\'s aperture is a hole, not the six strips of the old reading');
  }
  // a round OUTLINE without enough art is not a disc: a washer, a ring symbol and a monogram counter stay silhouettes (a ring with one hole)
  { const ring = blackFill('CUT', [blob(40, 40, 24, 24), blobCW(40, 40, 12, 12)]); group([ring]); assert(!ring.hatchBlue && P.engravedDiscOf({ outline: ring }) < 0, 'one hole is a ring, not art'); }
  // an oval is not a circle (a pumpkin with its face): it stays a silhouette with windows
  { const oval = blackFill('CUT', [blob(40, 40, 26, 20), ...discArt(40, 40, 6).slice(1)]); group([oval]); assert(P.engravedDiscOf({ outline: oval }) < 0, 'only a perfect circle'); }

  // a badge or note on the LABELS layer is not a charm: a round black disc with art on it is left as it is (the marker rules own it)
  { const note = blackFill('LABELS', discArt(40, 40, 15)); assert(P.engravedDiscOf({ outline: note }) < 0, 'a LABELS disc is not an engraved disc'); }

  // a fill painted with no closepath (an expanded stroke or a letter written `... c f`) is a filled area all the same
  { const body = path_('CUT', [blob(40, 40, 30, 36)]), open = blackFill('ENGRAVE', [blob(40, 40, 10, 14).slice(0, -1)], { closed: false }); group([body, open]); assert(P.isBlackFill(open) && open.hatchBlue && open.manufacturingRole === 'hatch', 'an unclosed black fill inside the charm is hatching'); }

  // 5 · a hoop drawn as a filled washer is still a hoop; a ring wholly inside the body is art (thick black ring)
  {
    const body = path_('CUT', [blob(40, 40, 30, 36)]), washer = blackFill('CUT', [blob(40, 62, 8, 8), blobCW(40, 62, 4, 4)]);
    group([body, washer]); assert(!washer.hatchBlue, 'a washer beside the body is the hoop: not hatching');
    const body2 = path_('CUT', [blob(40, 40, 40, 44)]), inner = blackFill('CUT', [blob(40, 40, 16, 16), blobCW(40, 40, 10, 10)]);
    group([body2, inner]); assert(inner.hatchBlue, 'a ring wholly inside the body is hatching');
  }

  // 6 · what is not touched: stroked paths, blue fills, text layers, shop roles, and the switch
  {
    const body = path_('CUT', [blob(40, 40, 30, 36)]), note = blackFill('LABELS', [blob(40, 40, 6, 6)]), roled = blackFill('CUT', [blob(36, 36, 4, 4)], { manufacturingRole: 'cut' }), blue = blackFill('HATCH', [blob(44, 44, 6, 6)], { fillRGB: [0, 0, 1] });
    const { g } = group([body, note, roled, blue]); const c = g.charms[0];
    assert(!note.hatchBlue && !roled.hatchBlue && !blue.hatchBlue, 'labels, a shop role and a blue fill are left as they are'); assert.equal(roled.manufacturingRole, 'cut');
    const off = blackFill('ENGRAVE', [blob(40, 40, 6, 6)]); const r2 = group([path_('CUT', [blob(40, 40, 30, 36)]), off], { blackFill: false }); assert(!off.hatchBlue, 'opts.blackFill === false is the reader before this rule');
    assert.deepEqual(draw(r2.g.charms[0]).fills, [BLACK], 'and it paints black as it did');
  }

  // 7 · the DXF writes the hatching in blue (the pen the laser maps), the cut circle as a polyline
  {
    const outline = path_('CUT', [blob(40, 40, 30, 30)]), art = blackFill('CUT', discArt(40, 40, 15).slice(1).map(s => s));
    const big = blackFill('ENGRAVE', [blob(40, 40, 8, 8)]);
    const parsed = parsedOf([outline, big]); const out = E.dxf(E.productionPaths(parsed));
    assert(/\r\n420\r\n255\r\n/.test(out.text) && !/AcDbHatch[\s\S]*420\r\n0\r\n/.test(out.text.split('AcDbHatch')[1] ? 'AcDbHatch' + out.text.split('AcDbHatch')[1].slice(0, 400) : ''), 'the hatch entity is blue');
  }

  // 8 · the per-SKU file round trip (the writer keeps the master's operators; the reader re-derives the same answer from the file)
  {
    const { PDFDocument, PDFName, PDFString } = PDFLib, f3 = n => (+n).toFixed(3);
    const circle = (cx, cy, r) => { const k = r * K; return `${f3(cx + r)} ${f3(cy)} m ${f3(cx + r)} ${f3(cy + k)} ${f3(cx + k)} ${f3(cy + r)} ${f3(cx)} ${f3(cy + r)} c ${f3(cx - k)} ${f3(cy + r)} ${f3(cx - r)} ${f3(cy + k)} ${f3(cx - r)} ${f3(cy)} c ${f3(cx - r)} ${f3(cy - k)} ${f3(cx - k)} ${f3(cy - r)} ${f3(cx)} ${f3(cy - r)} c ${f3(cx + k)} ${f3(cy - r)} ${f3(cx + r)} ${f3(cy - k)} ${f3(cx + r)} ${f3(cy)} c h`; };
    const strip = (cx, cy, R, dy) => `${f3(cx - 0.5 * R)} ${f3(cy + dy - 0.15 * R)} m ${f3(cx - 0.5 * R)} ${f3(cy + dy + 0.15 * R)} l ${f3(cx + 0.5 * R)} ${f3(cy + dy + 0.15 * R)} l ${f3(cx + 0.5 * R)} ${f3(cy + dy - 0.15 * R)} l h`;
    const doc = await PDFDocument.create(), page = doc.addPage([300, 140]);
    const ocg = n => doc.context.register(doc.context.obj({ Type: 'OCG', Name: PDFString.of(n) }));
    page.node.Resources().set(PDFName.of('Properties'), doc.context.obj({ MC1: ocg('CUT'), MC2: ocg('ENGRAVE') }));
    const ops = ['/OC /MC1 BDC', 'q', '0 0 0 rg'];
    // CHEVRON (fill only): one compound fill = circle + three strips, plus a stroked hoop ring above it
    ops.push(`${circle(50, 60, 20)} ${strip(50, 60, 20, 12)} ${strip(50, 60, 20, 0)} ${strip(50, 60, 20, -12)} f`, `q 0 0 0 RG 0.283 w ${circle(50, 84.5, 4)} S Q`);
    // CANDY (HUGGIE): stroked body, black stripes on ENGRAVE
    ops.push(`q 0 0 0 RG 0.283 w ${circle(150, 60, 20)} S Q`, 'Q', 'EMC', '/OC /MC2 BDC', 'q', '0 0 0 rg', `${strip(150, 60, 20, 8)} f`, `${strip(150, 60, 20, -8)} f`, 'Q', 'EMC');
    page.node.set(PDFName.of('Contents'), doc.context.obj([doc.context.register(doc.context.flateStream(ops.join('\n') + '\n'))]));
    const bytes = new Uint8Array(await doc.save({ useObjectStreams: false })), parsed = await P.parseSource(bytes, 'master.ai');
    const g = P.groupCharms(parsed, { minPt: 6 }); const byX = g.charms.slice().sort((a, b) => a.outline.bbox[0] - b.outline.bbox[0]);
    assert.equal(byX.length, 2, 'two charms'); const [chev, candy] = byX;
    for (const [name, c] of [['CHEVRON', chev], ['CANDY (HUGGIE)', candy]]) {
      const r = P.integrateRings(c); assert.equal(r.left.length, 0, name);
      const hatch = c.members.filter(m => m.hatchBlue); assert(hatch.length >= 1, name + ': hatching in the master'); assert(P.cutLinesOf(c).length <= 1, name + ': holes ' + P.cutLinesOf(c).length);
      c.name = name;
      const file = await P.buildSingleCharm(c, parsed), back = await P.parseSource(file, name + '.ai'), gb = P.groupCharms(back, { minPt: 6 });
      const cb = gb.charms.reduce((a, b) => (b.bbox[2] - b.bbox[0]) * (b.bbox[3] - b.bbox[1]) > (a.bbox[2] - a.bbox[0]) * (a.bbox[3] - a.bbox[1]) ? b : a);
      for (const o of gb.charms) if (o !== cb) for (const m of o.members) cb.members.push(m);
      P.integrateRings(cb);
      const blue = m => m.kind === 'path' && m.fill && !m.stroke && m.fillRGB[0] === 0 && m.fillRGB[1] === 0 && m.fillRGB[2] === 1;
      assert.equal(cb.members.filter(blue).length, hatch.length, name + ': the stored file reads back with the same hatching, written blue');
      assert(!cb.members.some(m => P.isBlackFill(m)), name + ': and holds no black fill at all');
      assert.equal(P.cutLinesOf(cb).length, P.cutLinesOf(c).length, name + ': and the same holes');
      assert(draw(cb).fills.length >= 1 && draw(cb).fills.every(f => f === BLUE), name + ': painted blue from the stored file');
    }
  }

  // 8b · the per-SKU file and the sheet carry the hatching in BLUE: no black fill operator is written for art inside the charm (the cut line stays as drawn)
  {
    const f3 = v => (Math.round(v * 1000) / 1000).toString();
    const ops = (subs, dx = 0, dy = 0) => subs.map(sp => sp.map(o => o[0] === 'h' ? 'h' : o[0] === 'c' ? [1, 2, 3].map(i => f3(o[i][0] - dx) + ' ' + f3(o[i][1] - dy)).join(' ') + ' c' : f3(o[1][0] - dx) + ' ' + f3(o[1][1] - dy) + ' ' + o[0]).join(' ')).join(' ');
    const bodySubs = [blob(40, 40, 36, 40)], aSubs = [blob(34, 44, 8, 10)], bSubs = [blob(46, 34, 8, 10)], cSubs = [blob(40, 52, 6, 6).slice(0, -1)], keepBlue = [blob(40, 28, 6, 6)];
    // a layer marker around everything, a fill under a translated matrix, a fill with no closepath, a blue fill that is already hatching
    const content = `/OC /MC0 BDC 0 0 0 RG 0.28 w ${ops(bodySubs)} S EMC\n/OC /MC1 BDC q 1 0 0 1 5 7 cm 0 0 0 rg ${ops(aSubs, 5, 7)} f Q 0 0 0 rg ${ops(bSubs)} f* 0 0 0 rg ${ops(cSubs)} f 0 0 1 rg ${ops(keepBlue)} f EMC\n`;
    const doc = await PDFLib.PDFDocument.create(), page = doc.addPage([80, 80]); page.node.normalize();
    const { PDFName, PDFString } = PDFLib, ocg = doc.context.register(doc.context.obj({ Type: 'OCG', Name: PDFString.of('ENGRAVE') })), ocg0 = doc.context.register(doc.context.obj({ Type: 'OCG', Name: PDFString.of('CUT') }));
    page.node.Resources().set(PDFName.of('Properties'), doc.context.obj({ MC0: ocg0, MC1: ocg }));
    page.node.addContentStream(doc.context.register(doc.context.flateStream(new TextEncoder().encode(content))));
    const parsed = await P.parseSource(await doc.save(), 'hatch-copy'), g = P.groupCharms(parsed, { minPt: 6 }); assert.equal(g.charms.length, 1);
    const c = g.charms[0], stamped = c.members.filter(m => m.hatchBlue); assert.equal(stamped.length, 3, 'the three black fills are stamped, the blue one is not');
    const bytes = await P.buildSingleCharm(Object.assign({}, c, { name: 'hatch-copy' }), parsed);
    const copy = await P.parseSource(bytes, 'hatch-copy file'), g2 = P.groupCharms(copy, { minPt: 6 }); assert.equal(g2.charms.length, 1, 'one charm in the per-SKU file');
    const c2 = g2.charms[0], paths2 = c2.members.filter(m => m.kind === 'path');
    const fillsOnly = paths2.filter(m => m.fill && !m.stroke);
    assert.equal(fillsOnly.length, 4, 'all four art fills are in the file');
    assert(fillsOnly.every(m => m.fillRGB[0] === 0 && m.fillRGB[1] === 0 && m.fillRGB[2] === 1), 'every art fill is written blue: ' + JSON.stringify(fillsOnly.map(m => m.fillRGB)));
    assert.equal(c2.members.filter(m => m.hatchBlue).length, 0, 'nothing left to stamp: the file has no black fill for the charm');
    assert.equal(c2.outline && c2.outline.stroke, true, 'the cut outline is still the stroked line'); assert.deepEqual(c2.outline.strokeRGB.map(v => Math.round(v)), [0, 0, 0]);
    // same shapes, same places (the translated one included), same paint rule (the f* one is still even-odd)
    const rel = (m, o) => [m.bbox[0] - o.bbox[0], m.bbox[1] - o.bbox[1], m.bbox[2] - o.bbox[0], m.bbox[3] - o.bbox[1]].map(v => Math.round(v * 10) / 10).join(',');   // (the per-SKU file has its own artboard: compare places relative to the outline)
    const box = (m, o) => rel(m, o || c.outline), before = c.members.filter(m => m.fill && !m.stroke).map(m => box(m, c.outline)).sort(), after = fillsOnly.map(m => box(m, c2.outline)).sort();
    assert.deepEqual(after, before, 'the copies sit exactly where the master drew the fills');
    assert(fillsOnly.some(m => String(m.paintOp).endsWith('*')), 'an even-odd fill stays even-odd');
    assert.equal(P.cutLinesOf(c2).length, P.cutLinesOf(c).length, 'the same holes'); assert.equal(draw(c2).fills.filter(f => f === BLACK).length, 0, 'drawn: no black');
    // written again from the read-back file: the same file contents (the copy is not rewritten twice)
    const again = await P.parseSource(await P.buildSingleCharm(Object.assign({}, c2, { name: 'hatch-copy' }), copy), 'again'), c3 = P.groupCharms(again, { minPt: 6 }).charms[0];
    assert.deepEqual(c3.members.filter(m => m.fill && !m.stroke).map(m => box(m, c3.outline)).sort(), before, 'idempotent');
    // the layer marker survives: the copies are still inside the layer the master drew them in
    assert.deepEqual(fillsOnly.map(m => m.layer).sort(), c.members.filter(m => m.fill && !m.stroke).map(m => m.layer).sort(), 'each copy keeps its layer'); assert(fillsOnly.some(m => m.layer === 'ENGRAVE'), 'and that layer is the ENGRAVE layer the master named');
  }

  // 8c · a filled twin of the cut line (WOLF_89694: windows cut in a silhouette, the same shape filled black) stays the silhouette through a hoop weld and the per-SKU file:
  //      the master and the file it wrote must give the same answer, or the wolf's linework would turn blue in every file that was welded
  {
    const f3 = v => (Math.round(v * 1000) / 1000).toString();
    const ops = subs => subs.map(sp => sp.map(o => o[0] === 'h' ? 'h' : o[0] === 'c' ? [1, 2, 3].map(i => f3(o[i][0]) + ' ' + f3(o[i][1])).join(' ') + ' c' : f3(o[1][0]) + ' ' + f3(o[1][1]) + ' ' + o[0]).join(' ')).join(' ');
    const sil = [blob(40, 40, 30, 36), blobCW(34, 40, 6, 14), blobCW(46, 40, 6, 14), blobCW(40, 28, 10, 5)], hoop = [blob(40, 62.5, 8, 8)];
    const content = `/OC /MC0 BDC 0 0 0 rg ${ops(sil)} f* 0 0 0 RG 0.28 w ${ops(sil)} S ${ops(hoop)} S EMC\n`;
    const doc = await PDFLib.PDFDocument.create(), page = doc.addPage([80, 80]); page.node.normalize();
    page.node.Resources().set(PDFLib.PDFName.of('Properties'), doc.context.obj({ MC0: doc.context.register(doc.context.obj({ Type: 'OCG', Name: PDFLib.PDFString.of('CUT') })) }));
    page.node.addContentStream(doc.context.register(doc.context.flateStream(new TextEncoder().encode(content))));
    const parsed = await P.parseSource(await doc.save(), 'twin-weld'), c = P.groupCharms(parsed, { minPt: 6 }).charms[0];
    assert(!c.members.some(m => m.hatchBlue), 'in the master the filled twin is the silhouette again');
    const w = P.integrateRings(c); assert.equal(w.welded, 1, 'the hoop is welded: ' + JSON.stringify(w));
    const copy = await P.parseSource(await P.buildSingleCharm(Object.assign({}, c, { name: 'twin-weld' }), parsed), 'twin-weld file'), c2 = P.groupCharms(copy, { minPt: 6 }).charms[0];
    assert(!c2.members.some(m => m.hatchBlue), 'and in the welded per-SKU file it is still the silhouette (not hatching)');
    assert.equal(draw(c2).fills.filter(f => f === BLUE).length, 0, 'nothing is painted blue');
  }

  // 9 · the stored-picture route draws it blue too (scripts/index-master.cjs; resvg is not a dependency of the test run, so it is only checked when it can be loaded)
  let Resvg = null; for (const base of [path.join(__dirname, '..', '..'), '/tmp/catapply-deps']) { try { Resvg = require(require.resolve('@resvg/resvg-js', { paths: [base] })); break; } catch (_) {} }
  if (Resvg) {
    const IM = require('../../scripts/index-master.cjs'), { Geom, CharmNestPDF } = require('../../netlify/functions/_charmNestPdf.js');
    const body = path_('CUT', [blob(40, 40, 30, 36)]), a = blackFill('ENGRAVE', [blob(40, 40, 14, 18)]), { g } = group([body, a]); const c = g.charms[0];
    const png = IM.thumbnailPng(Geom, c, 168, CharmNestPDF); assert(png && png.length > 100, 'a picture');
    const src = fs.readFileSync(path.join(__dirname, '..', '..', 'netlify/functions/charmMaster-background.js'), 'utf8');
    assert(/m\.hatchBlue && !m\.stroke \? "rgb\(0,0,255\)"/.test(src) && /lightFill/.test(src), 'the server route draws hatching blue and a white cut fill white');
  } else console.log('  - no @resvg/resvg-js here: the stored-PNG check was not run');
  console.log('black-fill: ok');
})().catch(e => { console.error(e); process.exit(1); });
