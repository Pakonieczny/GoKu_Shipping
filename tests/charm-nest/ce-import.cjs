/* VECTOR IMPORT (Design & Engrave item 23; Paul, 28 Oct 2026: "the designer would either bring in an existing vector design either an illustrator file or the XF file into
   the charm editor and then work on that file there ... You need to ensure the sanctity of the designed charm. It's exact dimensions"). charm-nest-ce-import.js:
     A  SVG in millimetres: 30.00 × 20.00 mm exactly, the CUT layer cut, the red line a line with every curve kept (an arc written with its flags run together, a quadratic,
        a smooth quadratic, a smooth cubic), the blue dot hatched where it was drawn, live text left out and said
     B  Illustrator's SVG (its px are points): a 20 mm disc reads 20.00 mm, not the 15 mm a browser draws it; paints from <style> classes; a <use> drawn twice, once turned
     C  SVG with no units (CSS px): the size at 96 to the inch, a white background left out, even-odd kept, a line never filled, a hidden part left out
     D  path data: implicit commands, a subpath after Z, numbers run together
     E  DXF in millimetres: 25.00 × 18.00 mm; an outline drawn as 8 LINEs and ARCs joined into ONE closed cut; a LWPOLYLINE's bulges as arcs; a cubic SPLINE as Béziers that
        ARE the spline (de Boor against each span, to a millionth of a mm); a rational SPLINE (a quarter circle) within 0.0001 mm of its radius; a block INSERTed at twice its
        size and turned; a HATCH hatched; text left out and said
     F  DXF in inches (12.70 mm) and with no units (millimetres, said)
     G  real .ai masters through the app's own reader (CharmNestPDF.parseSource + groupCharms): sizes to 0.01 mm of the drawing's own tight box, the roles the art edit gives
        each part, every curve kept, an outline drawn in two open pieces joined into one closed cut with its curves; a file with two charms: one picked, at its own size
     H  faceDoc / designOf (what "Open as a new design" hands CharmNestCeMasters): the frame is the size, the outline the cut; the role guesses
     I  (browser, through slot.sh) the editor's own import entry (CharmEditor.importers): a file dropped on the work area opens the picker in the same frame, a group's role
        changed with one press, "Place into this charm" puts one group at the drop point at its exact size, one undo step takes it back; Esc puts the picker away and the
        editor stays; the Import button and "Open as a new design"; an .ai in the page; the picker at phone width; the employee log's vectorImport event
   node tests/charm-nest/ce-import.cjs            (node checks only: --node)
   $SP/int/slot.sh node tests/charm-nest/ce-import.cjs   (with the browser check) */
'use strict';
const assert = require('node:assert/strict'), path = require('node:path'), fs = require('node:fs');
const ROOT = path.join(__dirname, '..', '..'), FIX = path.join(__dirname, 'fixtures'), CEF = path.join(FIX, 'ce-import');
global.window = global; global.self = global;
global.PDFLib = require(path.join(ROOT, 'vendor/pdf-lib-1.17.1.min.js'));
require(path.join(ROOT, 'charm-nest-pdf.js'));
global.CharmNestVector = require(path.join(ROOT, 'charm-nest-vector.js'));
global.CharmNestGeom = require(path.join(ROOT, 'charm-nest-geom.js'));
const CI = require(path.join(ROOT, 'charm-nest-ce-import.js')), CE = require(path.join(ROOT, 'charm-nest-charm-editor.js')), AE = require(path.join(ROOT, 'charm-nest-art-edit.js'));
const P = global.CharmNestPDF, M = CE.model, MM = 25.4 / 72, PT = 72 / 25.4;
let n = 0; const ok = (c, m) => { assert.ok(c, m); n++; }, eq = (a, b, m) => { assert.deepEqual(a, b, m); n++; };
const near = (a, b, e, m) => { assert.ok(Math.abs(a - b) <= e, `${m}: ${a} vs ${b}`); n++; };
const say = s => console.log('  ' + s);
const fileOf = f => ({ name: path.basename(f), bytes: new Uint8Array(fs.readFileSync(f)) });
const boxMm = it => M.bboxOf(it.subpaths).map(v => v * MM);
const ops = (subs, k) => subs.reduce((a, sp) => a + sp.filter(o => o[0] === k).length, 0);
const role = (r, re) => r.items.filter(it => re.test(it.name));

(async () => {
  const timing = [];
  const read = async f => { const r = await CI.read(typeof f === 'string' ? fileOf(f) : f); timing.push([r.name, r.ms]); return r; };
  await read(path.join(CEF, 'px96.svg'));   // (warm: the editor model, the regexes)
  timing.length = 0;

  /* ── A · SVG in millimetres ── */
  {
    const r = await read(path.join(CEF, 'tag-mm.svg'));
    near(r.widthMm, 30, 0.005, 'A width'); near(r.heightMm, 20, 0.005, 'A height');
    say(`A tag-mm.svg: ${r.widthMm.toFixed(4)} × ${r.heightMm.toFixed(4)} mm, ${r.items.length} parts, ${r.ms} ms`);
    const outline = r.items.find(it => it.id === r.outlineId);
    ok(outline && outline.role === 'cut' && outline.layer === 'CUT' && outline.closed, 'A the outline is the rounded rectangle on CUT, a cut line');
    eq([ops(outline.subpaths, 'c'), ops(outline.subpaths, 'l')], [4, 4], 'A its four corners are curves');
    const b = boxMm(outline); near(b[2] - b[0], 30, 1e-6, 'A outline width'); near(b[3] - b[1], 20, 1e-6, 'A outline height');
    const hole = r.items.find(it => it.layer === 'CUT' && it !== outline); const hb = boxMm(hole);
    ok(hole.role === 'cut', 'A the hole is cut'); near(hb[2] - hb[0], 2.5, 1e-6, 'A hole Ø 2.5 mm'); near((hb[0] + hb[2]) / 2, 4, 1e-6, 'A hole centre x'); near((hb[1] + hb[3]) / 2, 4, 1e-6, 'A hole centre y');
    const wave = r.items.find(it => it.name === 'wave');
    ok(wave && wave.role === 'line' && !wave.closed, 'A the red stroke on ENGRAVE is a line engraving (named by its id)');
    eq(ops(wave.subpaths, 'c'), 5, 'A curves kept: the half circle (2), the quadratic, the smooth quadratic, the smooth cubic');
    const wb = boxMm(wave), end = wave.subpaths[0][wave.subpaths[0].length - 1][3];
    near(wb[1], 9, 1e-4, 'A the arc (flags written "116") rises to y 9 mm, its radius 3'); near(wb[3], 13.5, 1e-4, 'A the quadratic dips to 13.5 mm');
    near(end[0] * MM, 25, 1e-6, 'A the smooth cubic ends at x 25 mm'); near(end[1] * MM, 12, 1e-6, 'A and y 12 mm');
    near(wave.lwPt, 0.2 * PT, 1e-6, 'A its line width from the class: 0.2 mm');
    const dot = r.items.find(it => it.fill && it.fill[2] === 1); const db = boxMm(dot);
    ok(dot.role === 'hatch', 'A the blue fill is hatched'); eq(db.map(v => +v.toFixed(6)), [20, 4, 24, 8], 'A the blue dot where it was drawn (mm)');
    ok(r.items.some(it => it.role === 'hatch' && it.fill && it.fill[0] === 0 && it.fill[2] === 0), 'A the black triangle is hatched');
    ok(r.warnings.some(w => /1 live text object left out/.test(w)), 'A the live text is left out and said: ' + r.warnings[0]);
  }
  /* ── B · Illustrator's SVG ── */
  {
    const r = await read(path.join(CEF, 'heart-ai.svg'));
    near(r.widthMm, 20, 0.01, 'B Illustrator px are points: 56.693 px = 20 mm'); near(r.heightMm, 20, 0.01, 'B height');
    say(`B heart-ai.svg: ${r.widthMm.toFixed(4)} × ${r.heightMm.toFixed(4)} mm (a browser would draw ${(r.widthMm * 0.75).toFixed(2)} mm), ${r.ms} ms`);
    const o = r.items.find(it => it.id === r.outlineId); ok(o.role === 'cut' && o.layer === 'CUT_LINE', 'B the disc on CUT_LINE is the outline, cut');
    near(o.lwPt, 0.25, 1e-9, 'B the class gives its stroke 0.25 pt');
    const petals = r.items.filter(it => it.name === 'petal');
    eq(petals.length, 2, 'B the petal drawn once and used twice');
    ok(petals.every(p => p.role === 'line' && p.stroke && p.stroke[0] > 0.8 && !p.fill), 'B both red lines: the paint is the <use>\'s class');
    const [p1, p2] = petals.map(boxMm);
    near(p1[2] - p1[0], 8 * MM, 1e-6, 'B petal 1 is 8 pt across'); near(p2[3] - p2[1], 8 * MM, 1e-6, 'B petal 2 is turned: 8 pt tall');
    ok(r.items.some(it => it.role === 'hatch' && it.fill && it.fill[0] === 1), 'B the red fill is hatched');
  }
  /* ── C · SVG with no units ── */
  {
    const r = await read(path.join(CEF, 'px96.svg'));
    near(r.widthMm, 80 * 0.75 * MM, 0.005, 'C 80 px at 96 to the inch'); near(r.heightMm, 64 * 0.75 * MM, 0.005, 'C 64 px');
    say(`C px96.svg: ${r.widthMm.toFixed(4)} × ${r.heightMm.toFixed(4)} mm, warnings: ${r.warnings.join(' | ')}`);
    eq(r.items.length, 4, 'C the white background and the hidden path are left out');
    ok(r.warnings.some(w => /white shape/.test(w)) && r.warnings.some(w => /CSS pixels/.test(w)), 'C both said');
    ok(r.items.find(it => it.evenOdd && it.subpaths.length === 2), 'C the frame with its window keeps even-odd');
    const line = r.items.find(it => it.name.startsWith('Line')); ok(line && !line.fill && line.role === 'line', 'C a <line> is never filled: a blue line engraving');
    ok(r.items.find(it => it.id === r.outlineId).name.startsWith('Ellipse'), 'C the black ellipse is the outline');
  }
  /* ── D · path data ── */
  {
    const s = CI.pathSubs('M0,0 10,0 10-5.5.5 1e1zm20 0l5 5');
    eq(s.length, 2, 'D two subpaths'); eq(s[0].map(o => o[0]).join(''), 'mlllh', 'D implicit lines after M; numbers run together ("10-5.5.5" is 10, -5.5, .5)');
    eq(s[0][3][1], [0.5, 10], 'D 1e1 is 10'); eq(s[1][0][1], [20, 0], 'D a relative move after Z starts from the closed subpath\'s start');
    const a = CI.pathSubs('M0 0A5 5 0 0 1 10 0'); eq(a[0].length, 3, 'D a half circle is two Béziers'); eq(a[0][2][3], [10, 0], 'D ending exactly on its end point');
  }
  /* ── E · DXF in millimetres ── */
  {
    const r = await read(path.join(CEF, 'tag-mm.dxf'));
    near(r.widthMm, 25, 0.005, 'E width'); near(r.heightMm, 18, 0.005, 'E height');
    say(`E tag-mm.dxf: ${r.widthMm.toFixed(4)} × ${r.heightMm.toFixed(4)} mm, ${r.items.length} parts, ${r.ms} ms`);
    // DXF is y up: a drawing point (x, y) mm is item point (x, 18 − y) mm
    const toDxf = q => [q[0] * MM, 18 - q[1] * MM];
    const o = r.items.find(it => it.id === r.outlineId);
    ok(o.role === 'cut' && o.closed && o.subpaths.length === 1, 'E the 8 LINEs and ARCs of the outline are joined into ONE closed cut line');
    eq([ops(o.subpaths, 'l'), ops(o.subpaths, 'c')], [4, 4], 'E its four corners are arcs');
    const ob = boxMm(o); near(ob[2] - ob[0], 25, 1e-6, 'E outline 25 mm'); near(ob[3] - ob[1], 18, 1e-6, 'E outline 18 mm');
    const cuts = r.items.filter(it => it.role === 'cut'); eq(cuts.length, 2, 'E the hole is cut too');
    const slot = r.items.find(it => it.closed && ops(it.subpaths, 'l') === 2 && ops(it.subpaths, 'c') === 4);
    ok(slot && slot.role === 'line', 'E the LWPOLYLINE slot: two straight runs and two bulges as half circles, red, a line');
    const sb = boxMm(slot); near(sb[0], 6.5, 1e-6, 'E the left bulge reaches x 6.5'); near(sb[2], 17.5, 1e-6, 'E the right one 17.5');
    // the cubic spline: its Béziers ARE the spline: de Boor at every quarter of each span against the Bézier at the same place
    const ctrl = [[6, 15], [8, 17.5], [10, 15], [12, 17.5], [14, 15], [16, 17.5], [18, 15]], U = [0, 0, 0, 0, 1, 2, 3, 4, 4, 4, 4];
    const deBoor = u => { let k = 3; while (k < ctrl.length - 1 && u >= U[k + 1]) k++; const d = []; for (let j = 0; j <= 3; j++) d.push(ctrl[k - 3 + j].slice()); for (let rr = 1; rr <= 3; rr++) for (let j = 3; j >= rr; j--) { const i = k - 3 + j, a = (u - U[i]) / (U[i + 4 - rr] - U[i]); d[j] = [(1 - a) * d[j - 1][0] + a * d[j][0], (1 - a) * d[j - 1][1] + a * d[j][1]]; } return d[3]; };
    const spline = r.items.find(it => !it.closed && ops(it.subpaths, 'c') === 4);
    ok(spline, 'E the cubic SPLINE is four Bézier spans (one per knot span), no sampled points'); eq(ops(spline.subpaths, 'l'), 0, 'E and no straight pieces');
    let worst = 0, cur = toDxf(spline.subpaths[0][0][1]);
    spline.subpaths[0].slice(1).forEach((op, s) => {
      const c = [cur, toDxf(op[1]), toDxf(op[2]), toDxf(op[3])];
      for (const t of [0, 0.25, 0.5, 0.75, 1]) { const u = 1 - t, q = [0, 1].map(k => u * u * u * c[0][k] + 3 * u * u * t * c[1][k] + 3 * u * t * t * c[2][k] + t * t * t * c[3][k]), e = deBoor(Math.min(s + t, 4 - 1e-12)); worst = Math.max(worst, Math.hypot(q[0] - e[0], q[1] - e[1])); }
      cur = c[3];
    });
    ok(worst < 1e-6, `E the Béziers are the spline itself: worst ${worst.toExponential(2)} mm`);
    say(`E cubic spline: 4 Béziers, worst distance from the B-spline ${worst.toExponential(2)} mm`);
    const quarter = r.items.find(it => !it.closed && it !== spline && ops(it.subpaths, 'c') >= 2 && (() => { const b = boxMm(it); return Math.abs(b[2] - b[0] - 2) < 1e-3 && Math.abs(b[3] - b[1] - 2) < 1e-3 && b[0] > 19; })());
    ok(quarter, 'E the rational SPLINE (a quarter circle, weights 1, √½, 1) is Béziers');
    let rad = 0; cur = toDxf(quarter.subpaths[0][0][1]);
    for (const op of quarter.subpaths[0].slice(1)) { const c = [cur, toDxf(op[1]), toDxf(op[2]), toDxf(op[3])]; for (let i = 0; i <= 20; i++) { const t = i / 20, u = 1 - t, q = [0, 1].map(k => u * u * u * c[0][k] + 3 * u * u * t * c[1][k] + 3 * u * t * t * c[2][k] + t * t * t * c[3][k]); rad = Math.max(rad, Math.abs(Math.hypot(q[0] - 22, q[1] - 9) - 2)); } cur = c[3]; }
    ok(rad < 1e-4, `E every point within 0.0001 mm of the 2 mm radius: ${rad.toExponential(2)} mm over ${ops(quarter.subpaths, 'c')} Béziers`);
    say(`E rational spline: ${ops(quarter.subpaths, 'c')} Béziers, off the circle by ${rad.toExponential(2)} mm at most`);
    const dot = r.items.find(it => it.closed && it.role === 'line' && (() => { const b = boxMm(it); return Math.abs(b[2] - b[0] - 2) < 1e-6 && Math.abs(b[3] - b[1] - 2) < 1e-6; })());
    ok(dot, 'E the block DOT (r 0.5) inserted at twice its size is a 2 mm circle');
    const dbx = boxMm(dot), dd = [toDxf([dbx[0] * PT, dbx[3] * PT]), toDxf([dbx[2] * PT, dbx[1] * PT])];
    eq([dd[0][0], dd[0][1], dd[1][0], dd[1][1]].map(v => +v.toFixed(6)), [20, 13, 22, 15], 'E at the insertion point (21, 14)');
    const tick = r.items.find(it => !it.closed && it.subpaths[0].length === 2); const tb = tick && boxMm(tick);
    ok(tick && Math.abs(tb[2] - tb[0]) < 1e-6 && Math.abs(tb[3] - tb[1] - 2) < 1e-6, 'E and its line, turned 90°, stands 2 mm tall');
    const hatch = r.items.find(it => it.layer === 'HATCH'); ok(hatch && hatch.role === 'hatch' && hatch.evenOdd, 'E the HATCH is hatched (even-odd)');
    ok(r.warnings.some(w => /1 text/.test(w)), 'E the TEXT is left out and said');
  }
  /* ── F · DXF units ── */
  {
    const r = await read(path.join(CEF, 'small-in.dxf'));
    near(r.widthMm, 12.7, 0.005, 'F half an inch is 12.70 mm'); eq(r.units, 'in', 'F read in inches ($INSUNITS 1)');
    const t = fs.readFileSync(path.join(CEF, 'tag-mm.dxf'), 'utf8').replace(/\n9\n\$INSUNITS\n70\n4/, '');
    const u = await read({ name: 'nounits.dxf', text: t });
    near(u.widthMm, 25, 0.005, 'F no units: millimetres'); ok(u.warnings.some(w => /no units in the file: read as millimetres/.test(w)), 'F and said');
    say(`F small-in.dxf ${r.widthMm.toFixed(4)} mm; no units: ${u.widthMm.toFixed(4)} mm`);
  }
  /* ── G · real .ai masters through the app's own reader ── */
  for (const f of ['MASTER_5X8_5.ai', 'COMET_9427.ai', 'TINY_INITIAL_TAG_1.ai', 'CELESTIAL3-COMET.ai', 'SCHRODINGER_CAT.ai']) {
    const file = fileOf(path.join(FIX, f)), r = await read(file);
    const g = P.groupCharms(await P.parseSource(file.bytes, f), { minPt: 6 }), c = g.charms[0];
    // the same charm, measured here on its own: the tight box of its parts (Bézier extremes), in pt
    const chain = new Set((c.outline.parts) || []), paths = c.members.filter(m => m.kind === 'path' && (m.stroke || m.fill) && !chain.has(m));
    let tb = null; for (const m of paths.concat(chain.size ? [c.outline] : [])) tb = M.union(tb, M.bboxOf(m.subpaths));
    near(r.widthMm, (tb[2] - tb[0]) * MM, 0.01, `G ${f} width`); near(r.heightMm, (tb[3] - tb[1]) * MM, 0.01, `G ${f} height`);
    const want = paths.map(m => (m === c.outline ? 'cut' : AE.roleOf(m, c))).concat(chain.size ? ['cut'] : []).sort(), got = r.items.map(it => it.role).sort();
    eq(got, want, `G ${f}: each part takes the role the art edit gives it`);
    const cIn = paths.reduce((a, m) => a + ops(m.subpaths, 'c'), 0) + [...chain].reduce((a, m) => a + ops(m.subpaths, 'c'), 0), cOut = r.items.reduce((a, it) => a + ops(it.subpaths, 'c'), 0);
    eq(cOut, cIn, `G ${f}: every curve kept (${cIn})`);
    const o = r.items.find(it => it.id === r.outlineId), ob = boxMm(o), cb = (chain.size ? M.bboxOf(c.outline.subpaths) : M.bboxOf(c.outline.subpaths)).map(v => v * MM);
    ok(o.role === 'cut' && o.closed, `G ${f}: the outline is a closed cut line`); near(ob[2] - ob[0], cb[2] - cb[0], 0.01, `G ${f} outline width`); near(ob[3] - ob[1], cb[3] - cb[1], 0.01, `G ${f} outline height`);
    if (chain.size) {
      eq(o.subpaths.length, 1, `G ${f}: the outline drawn in ${chain.size} open pieces is ONE closed subpath`);
      eq(ops(o.subpaths, 'c'), [...chain].reduce((a, m) => a + ops(m.subpaths, 'c'), 0), `G ${f}: with the pieces' own curves (the grouping's copy is a polyline)`);
    }
    say(`G ${f}: ${r.widthMm.toFixed(3)} × ${r.heightMm.toFixed(3)} mm (outline ${(ob[2] - ob[0]).toFixed(3)} × ${(ob[3] - ob[1]).toFixed(3)}), roles ${got.join(' ')}, ${cOut} curves, ${r.ms} ms${chain.size ? ', outline joined from ' + chain.size + ' pieces' : ''}`);
  }
  {
    const r = await read(path.join(FIX, 'SKUs_CORRECTED_Broken_Heart.ai'));
    eq(r.charms && r.charms.length, 2, 'G a file with two charms lists both');
    const one = CI.subset(r, r.charms[1].ids);
    near(one.widthMm, r.charms[1].widthMm, 1e-9, 'G one charm picked: its own width'); near(one.heightMm, r.charms[1].heightMm, 1e-9, 'G and height');
    ok(one.outlineId === r.charms[1].outlineId && one.items.find(it => it.id === one.outlineId).role === 'cut', 'G and its own outline, cut');
    const ob = boxMm(one.items.find(it => it.id === one.outlineId)); near(ob[0], 0, 1e-9, 'G placed at 0,0 again'); near(ob[1], 0, 1e-6, 'G (y)');
    say(`G two charms: ${r.charms.map(c => c.widthMm.toFixed(2) + ' × ' + c.heightMm.toFixed(2)).join(', ')} mm; the second alone ${one.widthMm.toFixed(2)} × ${one.heightMm.toFixed(2)} mm`);
  }
  await assert.rejects(CI.read({ name: 'note.txt', text: 'hello' }), /not a drawing this editor reads/); n++;
  await assert.rejects(CI.read({ name: 'old.ai', text: '%!PS-Adobe-3.0\n%%Creator: Adobe Illustrator 8' }), /without PDF compatibility/); n++;

  /* ── H · new design, roles ── */
  {
    const r = await read(path.join(CEF, 'tag-mm.dxf'));
    const fd = CI.faceDoc(r), d = CI.designOf(r);
    eq([fd.unitPt, fd.frame.x0, fd.frame.y0], [true, 0, 0], 'H a FaceDoc in points from 0,0');
    near(fd.frame.wPt * MM, 25, 1e-6, 'H framed at the exact width'); near(fd.frame.hPt * MM, 18, 1e-6, 'H and height');
    ok(fd.items.find(it => it.id === r.outlineId).role === 'cut' && /^Outline/.test(fd.items.find(it => it.id === r.outlineId).name), 'H the outline is the cut, named so');
    eq([d.name, d.source.kind, d.source.file, d.source.type, +d.widthMm.toFixed(2)], ['tag-mm', 'import', 'tag-mm.dxf', 'dxf', 25], 'H designOf: the design library\'s shape, source import');
    const G = CI.guessRole, red = [1, 0, 0], k = [0, 0, 0], blue = [0, 0, 1];
    eq([G({ stroke: red, closed: true }), G({ fill: k }), G({ fill: blue }), G({ stroke: k, closed: true, lwPt: 0.25 }), G({ stroke: k, closed: true, lwPt: 2 }), G({ stroke: k, closed: false, lwPt: 0.25 }),
      G({ stroke: red, closed: true, layer: 'Cut' }), G({ fill: blue, layer: 'CUT' }), G({ stroke: k, layer: 'Engrave', closed: true }), G({ fill: red, layer: 'etch' })],
      ['line', 'hatch', 'hatch', 'cut', 'line', 'line', 'cut', 'hatch', 'line', 'hatch'], 'H red stroke a line; black or blue fill hatched; a closed black hairline or a CUT layer cut; an engrave layer engraved');
  }
  const svgDxf = timing.filter(([nm]) => /\.(svg|dxf)$/.test(nm));
  const worstMs = Math.max(...svgDxf.map(t => t[1]));
  ok(worstMs < 250, 'timing: an SVG or DXF fixture reads in well under a quarter second: ' + worstMs + ' ms');
  say('read times (ms): ' + timing.map(([nm, ms]) => `${nm} ${ms}`).join(', '));
  console.log(`ce-import (node): ${n} checks passed`);

  if (process.argv.includes('--node')) return;
  await browser();
})().catch(e => { console.error(e); process.exit(1); });

/* ── I · the browser: the editor's own import entry ── */
async function browser() {
  const pwDir = process.env.PW_DIR || path.join(ROOT, 'node_modules');
  let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { console.log('  - no playwright-core: the browser check was not run'); return; }
  const HARNESS = 'file://' + path.join(FIX, 'charm-editor-harness.html');
  const b = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1243/chrome-linux64/chrome', args: ['--no-sandbox'] });
  let m = 0; const check = (c, msg) => { assert.ok(c, msg); m++; console.log('  ok   ' + msg); };
  const settle = page => page.evaluate(() => new Promise(r => requestAnimationFrame(() => requestAnimationFrame(r))));
  const newPage = async (w, h) => {
    const ctx = await b.newContext({ viewport: { width: w, height: h }, reducedMotion: 'reduce' });
    await ctx.route(u => !u.href.startsWith('file://'), r => r.abort());
    const page = await ctx.newPage(); page.__errs = [];
    page.on('pageerror', e => page.__errs.push('pageerror: ' + e.message));
    page.on('console', x => { if (x.type() === 'error') page.__errs.push('console: ' + x.text()); });
    await page.goto(HARNESS);
    for (const f of ['vendor/pdf-lib-1.17.1.min.js', 'charm-nest-vector.js', 'charm-nest-geom.js', 'charm-nest-pdf.js', 'charm-nest-art-edit.js', 'charm-nest-dxf.js', 'charm-nest-ce-import.js']) await page.addScriptTag({ path: path.join(ROOT, f) });
    await page.evaluate(() => { window.__log = []; window.CNDesignLog = { event: (k, d) => window.__log.push([k, d]) }; });
    await page.evaluate(() => { window.openEditor(); }); await page.waitForSelector('.ce-root .ce-items path'); await settle(page); await settle(page);
    return page;
  };
  /** A file dropped on the work area at client (x, y), as a hand lets go of it: the editor's own drop handler; → main-thread ms of the drop (the picker is up by then). */
  const drop = (page, f, x, y) => page.evaluate(([name, b64, type, x, y]) => {
    const bin = atob(b64), u = new Uint8Array(bin.length); for (let i = 0; i < bin.length; i++) u[i] = bin.charCodeAt(i);
    const dt = new DataTransfer(); dt.items.add(new File([u], name, { type }));
    const st = document.querySelector('.ce-root .ce-stage'), t0 = performance.now();
    st.dispatchEvent(new DragEvent('dragover', { bubbles: true, cancelable: true, clientX: x, clientY: y, dataTransfer: dt }));
    st.dispatchEvent(new DragEvent('drop', { bubbles: true, cancelable: true, clientX: x, clientY: y, dataTransfer: dt }));
    return { ms: performance.now() - t0, up: !!document.querySelector('.cei-root') };
  }, [path.basename(f), fs.readFileSync(f).toString('base64'), /\.svg$/.test(f) ? 'image/svg+xml' : '', x, y]);
  const ready = page => page.waitForSelector('.cei-root [data-size]');
  const shot = async (page, name) => { if (process.env.SHOTS) { fs.mkdirSync(process.env.SHOTS, { recursive: true }); await page.screenshot({ path: path.join(process.env.SHOTS, name) }); } };
  try {
    const page = await newPage(1280, 720);
    check(await page.evaluate(() => CharmEditor.importers[0] && CharmEditor.importers[0].id === 'vector' && CharmEditor.importers[0].accept.includes('.dxf')), 'I the vector importer is first in CharmEditor.importers (an .svg is image/svg+xml)');
    await page.evaluate(() => { CharmEditor.current.ctx.say && 0; });
    // reopen so the workspace reads the importer list (it is read when a workspace opens)
    await page.evaluate(() => document.querySelector('.ce-root [data-act="cancel"]').click()); await settle(page);
    await page.evaluate(() => { window.openEditor(); }); await page.waitForSelector('.ce-root .ce-items path'); await settle(page);
    check(await page.evaluate(() => { const bt = document.querySelector('.ce-root [data-act="import"]'); return !!bt && !bt.hidden && /\.ai/.test(bt.title); }), 'I the head\'s Import button is there and names .ai');
    const fr = await page.evaluate(() => CharmEditor.current.frame());
    const at = await page.evaluate(([x, y]) => CharmEditor.current.clientOf(x, y), [fr.x0 + fr.wPt * 0.4, fr.y0 + fr.hPt * 0.55]);
    const before = await page.evaluate(() => ({ n: CharmEditor.current.items().length, h: CharmEditor.current.history().undo }));
    const d1 = await drop(page, path.join(CEF, 'tag-mm.svg'), at.x, at.y);
    check(d1.up, `I a file dropped on the charm: the picker is up in the same frame (${d1.ms.toFixed(1)} ms of main thread for the drop)`);
    check(d1.ms < 50, 'I the drop never holds the page: ' + d1.ms.toFixed(1) + ' ms');
    await ready(page);
    check(await page.locator('.cei-root [data-size]').innerText() === '30.00 × 20.00 mm', 'I it reads its exact size: 30.00 × 20.00 mm'); await shot(page, 'ce-import-picker.png');
    const rows = await page.evaluate(() => [...document.querySelectorAll('.cei-row')].map(r => [r.querySelector('.cei-lab').firstChild.textContent, r.querySelector('[aria-pressed="true"]').dataset.role]));
    console.log('         groups: ' + rows.map(r => r.join(' → ')).join(' | '));
    check(rows.some(r => /CUT/.test(r[0]) && r[1] === 'cut') && rows.some(r => /red/.test(r[0]) && r[1] === 'line') && rows.some(r => /blue fill/.test(r[0]) && r[1] === 'hatch'), 'I the groups show the guessed roles');
    // the blue dot: Hatch -> Line with one press
    const blueRow = page.locator('.cei-row', { hasText: 'blue fill' });
    await blueRow.locator('[data-role="line"]').click(); await settle(page);
    check(await blueRow.locator('[data-role="line"]').getAttribute('aria-pressed') === 'true', 'I one press changes the group\'s role');
    check(await page.evaluate(() => [...document.querySelectorAll('.cei-pre .cei-p--line')].length) === 2, 'I the preview draws it as a line at once (the red line and the dot)');
    await page.locator('.cei-root [data-a="place"]').click(); await settle(page);
    check(!(await page.$('.cei-root')), 'I Place into this charm: the picker is gone');
    const after = await page.evaluate(() => { const C = CharmEditor.current, its = C.items(), g = its[its.length - 1]; return { n: its.length, h: C.history().undo, g: { kind: g.kind, name: g.name, roles: (g.children || []).map(c => c.role), id: g.id }, box: C.box(g.id), sel: C.selection() }; });
    check(after.n === before.n + 1 && after.g.kind === 'group' && after.g.name === 'tag-mm.svg', 'I one group named for the file on the face');
    check(JSON.stringify(after.g.roles) === JSON.stringify(['cut', 'cut', 'line', 'line', 'hatch']), 'I the roles as picked: ' + after.g.roles.join(' '));
    const w = (after.box[2] - after.box[0]) * MM, h = (after.box[3] - after.box[1]) * MM, cx = (after.box[0] + after.box[2]) / 2, cy = (after.box[1] + after.box[3]) / 2;
    check(Math.abs(w - 30) < 0.01 && Math.abs(h - 20) < 0.01, `I at its exact size in the charm: ${w.toFixed(4)} × ${h.toFixed(4)} mm`);
    const want = [fr.x0 + fr.wPt * 0.4, fr.y0 + fr.hPt * 0.55], tol = 0.6 / (await page.evaluate(() => CharmEditor.current.zoom()) / 100 * 96 / 72);
    check(Math.abs(cx - want[0]) < tol && Math.abs(cy - want[1]) < tol, `I centred where it was dropped (${((cx - want[0]) * MM).toFixed(3)}, ${((cy - want[1]) * MM).toFixed(3)} mm off)`);
    check(after.sel.length === 1 && after.sel[0] === after.g.id && after.h === before.h + 1, 'I selected, and one step of history'); await shot(page, 'ce-import-placed.png');
    await page.keyboard.press('Control+z'); await settle(page);
    check(await page.evaluate(() => CharmEditor.current.items().length) === before.n, 'I one undo takes it all back');
    const log1 = await page.evaluate(() => window.__log.filter(x => x[0] === 'vectorImport').map(x => x[1]));
    check(log1.length === 1 && log1[0].mode === 'place' && log1[0].via === 'drop' && log1[0].parts === 5 && Math.abs(log1[0].widthMm - 30) < 0.001, 'I the employee log has one vectorImport (place, drop, 5 parts, 30 mm)');

    // the Import button, then Esc: the picker goes, the editor stays, nothing changes
    const chooser = page.waitForEvent('filechooser');
    await page.locator('.ce-root [data-act="import"]').click();
    await (await chooser).setFiles(path.join(CEF, 'tag-mm.dxf')); await ready(page);
    check(await page.locator('.cei-root [data-size]').innerText() === '25.00 × 18.00 mm', 'I the Import button: a .dxf, 25.00 × 18.00 mm');
    check(!(await page.$('.cei-root [data-a="new"]')), 'I with no new-design module on the page, only Place is offered');
    await page.keyboard.press('Escape'); await settle(page);
    check(!(await page.$('.cei-root')) && await page.evaluate(() => !!CharmEditor.current && CharmEditor.current.items().length) === before.n, 'I Esc puts the picker away; the editor stays open and unchanged');

    // Open as a new design (CharmNestCeMasters owns new designs: a stand-in records what it is handed)
    await page.evaluate(() => { window.__new = null; window.CharmNestCeMasters = { openDesign: (d, o) => { window.__new = { d, o, open: !!CharmEditor.current }; return Promise.resolve(null); } }; });
    await drop(page, path.join(CEF, 'heart-ai.svg'), at.x, at.y); await ready(page);
    check(await page.locator('.cei-root [data-size]').innerText() === '20.00 × 20.00 mm', 'I Illustrator\'s SVG: 20.00 × 20.00 mm');
    await page.locator('.cei-root [data-a="new"]').click(); await page.waitForFunction(() => !!window.__new);
    const nd = await page.evaluate(() => window.__new);
    const outl = nd && nd.d.faces.front.items.find(it => it.id === nd.d.outlineId);
    check(nd && nd.d.source.kind === 'import' && nd.d.source.file === 'heart-ai.svg' && !nd.d.id && Math.abs(nd.d.faces.front.frame.wPt * MM - 20) < 0.01 && outl && outl.role === 'cut',
      'I Open as a new design: CharmNestCeMasters.openDesign gets an unsaved design (source import), its frame 20 mm, the outline its cut');
    check(await page.evaluate(() => window.__log.filter(x => x[0] === 'vectorImport').map(x => x[1].mode).join(',')) === 'place,new', 'I and the log says so');

    // an .ai in the page (the app's PDF reader)
    await drop(page, path.join(FIX, 'COMET_9427.ai'), at.x, at.y); await ready(page);
    check(await page.locator('.cei-root [data-size]').innerText() === '7.20 × 13.38 mm', 'I an .ai master through the app\'s reader: 7.20 × 13.38 mm');
    await page.locator('.cei-root [data-a="cancel"]').click(); await settle(page);
    check(page.__errs.length === 0, 'I no page errors: ' + page.__errs.join(' | '));
    await page.context().close();

    // phone width: the picker fits
    const ph = await newPage(390, 844);
    await ph.evaluate(() => document.querySelector('.ce-root [data-act="cancel"]').click()); await settle(ph);
    await ph.evaluate(() => { window.openEditor(); }); await ph.waitForSelector('.ce-root .ce-items path'); await settle(ph);
    const c2 = await ph.evaluate(() => { const r = document.querySelector('.ce-root .ce-stage').getBoundingClientRect(); return { x: r.left + r.width / 2, y: r.top + r.height / 2 }; });
    await drop(ph, path.join(CEF, 'tag-mm.svg'), c2.x, c2.y); await ready(ph);
    await shot(ph, 'ce-import-phone.png');
    const fit = await ph.evaluate(() => { const card = document.querySelector('.cei-card').getBoundingClientRect(), out = []; for (const el of document.querySelectorAll('.cei-card button, .cei-card .cei-pre, .cei-row')) { const r = el.getBoundingClientRect(); if (r.left < card.left - 0.5 || r.right > card.right + 0.5) out.push(el.className || el.tagName); } return { card: [card.left, card.right], out, scroll: document.documentElement.scrollWidth <= innerWidth }; });
    check(fit.card[0] >= 15.5 && fit.card[1] <= 390 - 15.5 && !fit.out.length && fit.scroll, `I at 390 px wide the picker keeps a 16 px gutter and nothing spills (${fit.card.map(v => v.toFixed(0)).join('..')})`);
    await ph.keyboard.press('Escape');
    check(ph.__errs.length === 0, 'I no page errors at phone width');
    await ph.context().close();
  } finally { await b.close(); }
  console.log(`ce-import (browser): ${m} checks passed`);
}
