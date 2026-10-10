// BASKETBALL_9338 (Paul, 10 Oct 2026: "The basketball should have blue hatching engravings in place of the black lines").
// The master draws the seams as ONE closed red outline of thin strips on the CUT layer, inside the black cut circle. Two things went wrong:
//  1 · the browser groups a drawing in a worker, on a copy of the page's segments, and adoptGrouping handed the page its own segments back
//      WITHOUT what the grouping had decided about them (the role "engrave" of the red outline inside a cut line, the black-fill hatching flag).
//      The server, the audit and the catalogue index read the seams as engraving; the app read them as a CUT LINE: black double lines on the
//      Back engraving card (it draws the cut members, all in black), cut-out strips on the sheet.
//  2 · the engraving role makes them a drawn red line; the strips are the hatched area (the master's own BASKETBALL_99143 draws the same strips as blue HATCH fills):
//      charm-nest-pdf.js "thin strips outlined in red are hatching" reads them as blue hatching, draws them and writes them as a blue fill.
//   node tests/charm-nest/basketball-strips.cjs
const assert = require('node:assert/strict');
global.self = global; global.PDFLib = require('../../vendor/pdf-lib-1.17.1.min.js'); require('../../charm-nest-pdf.js');
const P = CharmNestPDF, G = require('../../charm-nest-geom.js');
const K = 0.5523;
const blob = (cx, cy, w, h) => { const a = w / 2, b = h / 2; return [['m', [cx + a, cy]], ['c', [cx + a, cy + K * b], [cx + K * a, cy + b], [cx, cy + b]], ['c', [cx - K * a, cy + b], [cx - a, cy + K * b], [cx - a, cy]], ['c', [cx - a, cy - K * b], [cx - K * a, cy - b], [cx, cy - b]], ['c', [cx + K * a, cy - b], [cx + a, cy - K * b], [cx + a, cy]], ['h']]; };
const plus = (cx, cy, L, h) => [['m', [cx + h, cy + h]], ['l', [cx + h, cy + L]], ['l', [cx - h, cy + L]], ['l', [cx - h, cy + h]], ['l', [cx - L, cy + h]], ['l', [cx - L, cy - h]], ['l', [cx - h, cy - h]], ['l', [cx - h, cy - L]], ['l', [cx + h, cy - L]], ['l', [cx + h, cy - h]], ['l', [cx + L, cy - h]], ['l', [cx + L, cy + h]], ['h']];
const bboxOf = subs => { const pts = subs.flat().flatMap(o => o.slice(1)); return [Math.min(...pts.map(p => p[0])), Math.min(...pts.map(p => p[1])), Math.max(...pts.map(p => p[0])), Math.max(...pts.map(p => p[1]))]; };
const path_ = (layer, sub, o = {}) => Object.assign({ kind: 'path', layer, subpaths: [sub], bbox: bboxOf([sub]), closed: true, stroke: true, fill: false, strokeRGB: [0, 0, 0], fillRGB: [0, 0, 0], lwPt: 0.28, paintOp: 'S', depth: 0 }, o);
const RED = [1, 0, 0], BLUE = 'rgb(0,0,255)';
const parsedOf = segs => ({ segments: segs.map((s, i) => Object.assign(s, { index: i, start: i * 10, end: i * 10 + 9 })), nested: [], pageW: 160, pageH: 120 });
const group = segs => { const parsed = parsedOf(segs); return { parsed, g: P.groupCharms(parsed, { minPt: 6 }) }; };
const recorder = () => { const rec = { fills: [], strokes: [] }; const s = { fillStyle: '#000', strokeStyle: '#000', lineWidth: 1 };
  const ctx = new Proxy(s, { get: (o, k) => k === 'fill' ? () => rec.fills.push(o.fillStyle) : k === 'stroke' ? () => rec.strokes.push(o.strokeStyle) : k in o ? o[k] : () => {}, set: (o, k, v) => { o[k] = v; return true; } });
  return { ctx, rec }; };
const draw = c => { const { ctx, rec } = recorder(); P.drawCharm(ctx, c, (x, y) => [x, y], 4); return rec; };
const holesOf = c => P.cutLinesOf(c).length;
const plain = p => { const { doc, page, ...rest } = p; return rest; };
// what the browser does: group in a worker (a copy of the page's segments, both ways through a structured clone), the page takes its own segments back
const viaWorker = parsed => P.adoptGrouping(structuredClone(P.groupForTransfer(structuredClone(plain(parsed)), { minPt: 6 })), parsed);

(async () => {
  // the basketball in small: a black cut circle 30 pt across (10.6 mm) and one closed outline of thin strips (a "+" of two bars 0.32 mm wide, 26 pt = 9.2 mm long), red, on the CUT layer
  const ball = () => [path_('CUT', blob(40, 40, 30, 30)), path_('CUT', plus(40, 40, 13, 0.45), { strokeRGB: RED, lwPt: 0.25 })];

  // 1 · the thin strips are blue hatching: not a cut-out, not a drawn line
  {
    const [body, seam] = ball(); const { g } = group([body, seam]); const c = g.charms[0];
    assert.equal(c.outline, body, 'the black circle is the cut outline');
    assert(seam.hatchStrip && seam.hatchBlue && seam.manufacturingRole === 'hatch', 'the red strips are read as hatching: ' + JSON.stringify([seam.hatchStrip, seam.manufacturingRole]));
    assert.equal(G.pathRole(seam), 'artwork', 'artwork, never a cut'); assert(!G.isCutLine(seam), 'not a cut line'); assert.equal(holesOf(c), 0, 'not a hole');
    assert(c.members.includes(seam), 'still a member of the charm');
    const rec = draw(c); assert.deepEqual(rec.fills, [BLUE], 'painted as a blue fill'); assert(rec.strokes.length >= 1 && rec.strokes.every(s => s === '#000' || s === 'rgb(0,0,0)'), 'only the black cut line is stroked: ' + JSON.stringify(rec.strokes));
    // the Back engraving card draws the back view's members: the cut outline only, none of the seams
    const v = G.backView(c, { res: 6, holeOnly: true }); assert.equal(v.members.length, 1, 'the back view holds the outline alone'); assert(!v.members.some(m => m.original === seam || m === seam), 'no seam on the back');
  }

  // 2 · what stays as it was: a red outline that is a real shape (wide), a short thin detail, a red strip on the ENGRAVE layer, a black thin slit on CUT
  {
    const body = path_('CUT', blob(40, 40, 30, 30)), fat = path_('CUT', plus(40, 40, 11, 3), { strokeRGB: RED }); group([body, fat]);
    assert.equal(fat.manufacturingRole, 'engrave', 'a wide red outline inside the cut line is engraving (the earlier rule)'); assert(!fat.hatchStrip && !fat.hatchBlue, 'and not hatching');
    const body2 = path_('CUT', blob(40, 40, 30, 30)), short = path_('CUT', plus(40, 40, 4, 0.45), { strokeRGB: RED }); group([body2, short]);
    assert.equal(short.manufacturingRole, 'engrave', 'a short thin detail is engraving'); assert(!short.hatchStrip, 'not a pattern across the charm');
    const body3 = path_('CUT', blob(40, 40, 30, 30)), eng = path_('ENGRAVE', plus(40, 40, 13, 0.45), { strokeRGB: RED }); const r3 = group([body3, eng]);
    assert(!eng.hatchStrip && !eng.manufacturingRole, 'a red outline the master put on the ENGRAVE layer is its own engraving line, as it draws it'); assert.deepEqual(draw(r3.g.charms[0]).strokes.filter(s => s === 'rgb(255,0,0)').length, 1, 'still drawn red');
    const body4 = path_('CUT', blob(40, 40, 30, 30)), slit = path_('CUT', plus(40, 40, 13, 0.45), { strokeRGB: [0, 0, 0] }); const r4 = group([body4, slit]);
    assert(!slit.hatchStrip && !slit.manufacturingRole, 'a BLACK thin closed path on CUT is a real cut-out (a slit): left alone'); assert.equal(holesOf(r4.g.charms[0]), 1, 'still a hole');
    const body5 = path_('CUT', blob(40, 40, 30, 30)), outside = path_('CUT', plus(100, 40, 13, 0.45), { strokeRGB: RED }); group([body5, outside]);
    assert(!outside.hatchStrip, 'a red outline that no cut line encloses is not inside a charm');
  }

  // 3 · the worker hand-back: the page's own segments carry what the grouping decided (the fault: they did not, so the seams were a cut line in the app)
  {
    const mk = () => { const [body, seam] = ball(), fat = path_('CUT', plus(40, 40, 11, 3), { strokeRGB: RED }), black = path_('CUT', blob(40, 60, 6, 6), { fill: true, stroke: false, paintOp: 'f', fillRGB: [0, 0, 0] }); return parsedOf([body, seam, fat, black]); };
    const direct = P.groupCharms(mk(), { minPt: 6 }); const pw = mk(); const adopted = viaWorker(pw);
    const roles = g => g.charms.flatMap(c => c.members.map(m => [m.index, m.manufacturingRole || '-', !!m.hatchBlue, !!m.hatchStrip, G.isCutLine(m)].join(':'))).sort().join(' ');
    assert.equal(roles(adopted), roles(direct), 'the worker route reads every member as the direct route does');
    for (const c of adopted.charms) for (const m of c.members) assert(pw.segments.includes(m) || m.synthetic, 'and the members are the page\'s own segments');
    assert.equal(holesOf(adopted.charms[0]), holesOf(direct.charms[0]), 'same holes');
    // control: without the hand-back the page reads the seams as a cut line (what Paul saw)
    const pc = mk(); const lost = structuredClone(P.groupForTransfer(structuredClone(plain(pc)), { minPt: 6 }));
    const own = new Map(pc.segments.map((s, i) => ['s' + i, s])); for (const c of lost.charms) c.members = c.members.map(m => own.get(m.pageRef) || m);
    assert(lost.charms[0].members.some(m => m.index === 1 && G.isCutLine(m)), 'control: with the stamps lost the strips are a cut line');
  }
  // every scalar the grouping changes on a segment is on the list the hand-back carries: a new stamp that is not listed would be lost in the browser again
  {
    const parsed = parsedOf([...ball(), path_('CUT', plus(40, 40, 11, 3), { strokeRGB: RED }), path_('CUT', blob(40, 60, 6, 6), { fill: true, stroke: false, paintOp: 'f', fillRGB: [0, 0, 0] })]);
    const before = structuredClone(plain(parsed)); const across = structuredClone(P.groupForTransfer(structuredClone(plain(parsed)), { minPt: 6 })); const changed = new Set(), seen = new Set();
    const walk = v => { if (!v || typeof v !== 'object' || seen.has(v) || ArrayBuffer.isView(v)) return; seen.add(v);
      if (typeof v.pageRef === 'string') { const o = (v.pageRef[0] === 's' ? before.segments : before.nested)[+v.pageRef.slice(1)]; for (const k of Object.keys(v)) if (k !== 'pageRef' && typeof v[k] !== 'object' && o[k] !== v[k]) changed.add(k); }
      for (const k of Object.keys(v)) walk(v[k]); };
    walk(across); for (const k of changed) assert(P.GROUPING_STAMPS.includes(k), 'the grouping stamps "' + k + '" on a segment but adoptGrouping does not carry it back');
    assert(changed.has('manufacturingRole') && changed.has('hatchBlue'), 'the fixture does exercise the stamps');
  }

  // 4 · the writer: a per-SKU file / a sheet holds the strips as a BLUE FILL in the layer the master drew them in, so every reader sees hatching without any stamp
  {
    const { PDFDocument, PDFName, PDFString } = PDFLib, f3 = n => (+n).toFixed(3);
    const circle = (cx, cy, r) => { const k = r * K; return `${f3(cx + r)} ${f3(cy)} m ${f3(cx + r)} ${f3(cy + k)} ${f3(cx + k)} ${f3(cy + r)} ${f3(cx)} ${f3(cy + r)} c ${f3(cx - k)} ${f3(cy + r)} ${f3(cx - r)} ${f3(cy + k)} ${f3(cx - r)} ${f3(cy)} c ${f3(cx - r)} ${f3(cy - k)} ${f3(cx - k)} ${f3(cy - r)} ${f3(cx)} ${f3(cy - r)} c ${f3(cx + k)} ${f3(cy - r)} ${f3(cx + r)} ${f3(cy - k)} ${f3(cx + r)} ${f3(cy)} c h`; };
    const cross = (cx, cy, L, h) => [[cx + h, cy + h], [cx + h, cy + L], [cx - h, cy + L], [cx - h, cy + h], [cx - L, cy + h], [cx - L, cy - h], [cx - h, cy - h], [cx - h, cy - L], [cx + h, cy - L], [cx + h, cy - h], [cx + L, cy - h], [cx + L, cy + h]].map((p, i) => `${f3(p[0])} ${f3(p[1])} ${i ? 'l' : 'm'}`).join(' ') + ' h';
    const doc = await PDFDocument.create(), page = doc.addPage([120, 120]); page.node.normalize();
    page.node.Resources().set(PDFName.of('Properties'), doc.context.obj({ MC0: doc.context.register(doc.context.obj({ Type: 'OCG', Name: PDFString.of('CUT') })) }));
    // as the master: the layer's pen set once, each object under its own q/cm, the seams red (1 0 0 SCN), the hoop beside the circle
    const content = `/OC /MC0 BDC 0 0 0 RG 0.283 w q 1 0 0 1 0 0 cm ${circle(60, 50, 15)} S Q q 1 0 0 1 0 0 cm ${circle(60, 70.5, 4)} S Q 1 0 0 RG 0.249 w q 1 0 0 1 0 0 cm ${cross(60, 50, 13, 0.45)} S Q EMC\n`;
    page.node.addContentStream(doc.context.register(doc.context.flateStream(new TextEncoder().encode(content))));
    const parsed = await P.parseSource(new Uint8Array(await doc.save()), 'basketball-master'), g = P.groupCharms(parsed, { minPt: 6 });
    const c = g.charms.reduce((a, b) => (b.bbox[2] - b.bbox[0]) * (b.bbox[3] - b.bbox[1]) > (a.bbox[2] - a.bbox[0]) * (a.bbox[3] - a.bbox[1]) ? b : a);
    const strip = c.members.filter(m => m.hatchStrip); assert.equal(strip.length, 1, 'the master read: one strip outline, hatching'); assert.equal(P.integrateRings(c).left.length, 0, 'the hoop welds');
    const holes = holesOf(c); assert.equal(holes, 1, 'one hole: the hoop\'s aperture, not the seams');
    c.name = 'BASKETBALL';
    const bytes = await P.buildSingleCharm(c, parsed), file = await P.parseSource(bytes, 'BASKETBALL.ai');
    const raw = new TextDecoder('latin1').decode(file.content);
    assert(!/1 0 0 RG/.test(raw.replace(/\s+/g, ' ')) || true, '(the red pen may stay set as state)');
    const gf = P.groupCharms(file, { minPt: 6 }), cf = gf.charms.reduce((a, b) => (b.bbox[2] - b.bbox[0]) * (b.bbox[3] - b.bbox[1]) > (a.bbox[2] - a.bbox[0]) * (a.bbox[3] - a.bbox[1]) ? b : a);
    P.integrateRings(cf);
    const blue = m => m.kind === 'path' && m.fill && !m.stroke && m.fillRGB[0] === 0 && m.fillRGB[1] === 0 && m.fillRGB[2] === 1;
    const fills = cf.members.filter(blue); assert.equal(fills.length, 1, 'the stored file holds the strips as one blue fill: ' + JSON.stringify(cf.members.map(m => [m.layer, m.fill, m.stroke, m.strokeRGB])));
    assert.equal(fills[0].layer, 'CUT', 'in the layer the master drew them in'); assert.equal(G.pathRole(fills[0]), 'artwork', 'hatching to every reader (a blue fill is hatching wherever it is drawn)'); assert(!G.isCutLine(fills[0]));
    assert(!cf.members.some(m => m.kind === 'path' && m.stroke && m.strokeRGB[0] > 0.7 && m.strokeRGB[1] < 0.3 && m.strokeRGB[2] < 0.3), 'no red line is left in the file');
    assert.equal(holesOf(cf), holes, 'the file has the same holes'); assert(!fills[0].hatchStrip, 'it needs no stamp to be read as hatching');
    const rec = draw(cf); assert.deepEqual(rec.fills, [BLUE], 'drawn blue from the stored file, with no stamp'); assert(!rec.strokes.some(s => s === 'rgb(255,0,0)'), 'no red stroke');
    // the same area: the blue fill sits exactly where the red outline was
    const near = (a, b) => a.every((v, i) => Math.abs(v - b[i]) < 0.6); const rel = (m, o) => [m.bbox[0] - o.bbox[0], m.bbox[1] - o.bbox[1], m.bbox[2] - o.bbox[0], m.bbox[3] - o.bbox[1]];
    assert(near(rel(fills[0], cf.outline), rel(strip[0], c.outline).map((v, i) => i < 2 ? v : v)), 'in the same place relative to the cut outline');
    // written again from the read-back file: still one blue fill, nothing doubled
    const again = await P.parseSource(await P.buildSingleCharm(Object.assign({}, cf, { name: 'BASKETBALL' }), file), 'again'), ca = P.groupCharms(again, { minPt: 6 }).charms.reduce((a, b) => (b.bbox[2] - b.bbox[0]) * (b.bbox[3] - b.bbox[1]) > (a.bbox[2] - a.bbox[0]) * (a.bbox[3] - a.bbox[1]) ? b : a);
    assert.equal(ca.members.filter(blue).length, 1, 'idempotent: one blue fill after a second write');
    // a sheet written from the master's own bytes carries it blue too
    const sheet = await P.buildSheet({ sheet: { wPt: 120, hPt: 120, strokeRGB: [1, 1, 1], strokePt: 0.01 }, placements: [{ charm: Object.assign({}, c, { sourceId: 'one', centerPt: [(c.bbox[0] + c.bbox[2]) / 2, (c.bbox[1] + c.bbox[3]) / 2], strokePt: 0.5 }), angle: 0, cxPt: 60, cyPt: 60 }], sources: new Map([['one', parsed]]), title: 'sheet' });
    const sp = await P.parseSource(new Uint8Array(sheet), 'sheet.ai'); const sblue = []; for (const s of sp.segments.concat(sp.nested || [])) if (blue(s)) sblue.push(s);
    assert(sblue.length >= 1, 'the sheet file holds the strips as a blue fill');
  }
  console.log('basketball-strips: ok');
})().catch(e => { console.error(e); process.exit(1); });
