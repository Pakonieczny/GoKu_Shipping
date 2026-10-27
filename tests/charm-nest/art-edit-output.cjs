/* ART EDIT (Paul, 27 Oct 2026: "... e.g. add a date inside a given calendar day square and update the month name located on top of the calendar charm"): the laser files of ONE piece whose
   own artwork was edited. Offline: pdf-lib, JSZip and the repo's own modules, a frozen clock and a seeded Math.random (the two things pdf-lib varies a file by); no network.
     A  buildSheet with a real master (fixtures/MASTER_5X8_5.ai, a per-SKU file: the whole charm in ONE form): the edited piece is drawn again from its own members, without the part removed
        and with the parts added, each in the master's own layer and the laser's colours; every other piece of the sheet is byte for byte what it was (its form's stream and resources);
        a piece whose edit was taken off writes exactly the bytes of a piece never edited
     B  the calendar through the sheet: the month name gone, the new month and the date in their square, blue hatching, the cut outline kept; the parsed sheet groups it as one charm again
     C  export and DXF: the composed production .ai carries the additions on the piece (where the sheet put it), and the DXF carries them as HATCH entities on the master's layer
     D  the back: a part added to the BACK is engraved in the back file's ENGRAVE layer, turned over with the cut; a back file without one is byte for byte as before
     E  weight and line width (27 Oct 2026): words the charm editor made at three weights and a day square's line thinned with its Stroke: the record keeps each weight with its outlines
        and the line's width, the piece opened again says them (itemsOf, decode), a weight changed there grows only by the difference, and the sheet's file engraves exactly those
        outlines (more ink at +0.05 mm, less at -0.03 mm) and writes that line at 0.142 pt while the other squares keep their own
   node tests/charm-nest/art-edit-output.cjs */
'use strict';
const assert = require('node:assert/strict'), fs = require('node:fs'), path = require('node:path');
const ROOT = path.join(__dirname, '..', '..');
const FIXED = Date.UTC(2026, 9, 27, 12, 0, 0), RealDate = Date;
global.Date = class extends RealDate { constructor(...a) { if (a.length) super(...a); else super(FIXED); } static now() { return FIXED; } };
let seed = 1; const reseed = () => { seed = 20261027; };
Math.random = () => { seed = (Math.imul(seed, 1664525) + 1013904223) >>> 0; return seed / 4294967296; };
global.window = global; global.self = global;
global.PDFLib = require(path.join(ROOT, 'vendor/pdf-lib-1.17.1.min.js')); global.JSZip = require(path.join(ROOT, 'vendor/jszip-3.10.1.min.js'));
require(path.join(ROOT, 'charm-nest-pdf.js'));
global.CharmNestVector = require(path.join(ROOT, 'charm-nest-vector.js'));
global.CharmNestGeom = require(path.join(ROOT, 'charm-nest-geom.js'));
const P = global.CharmNestPDF, G = global.CharmNestGeom, L = global.PDFLib, E = global.CharmNestExport = require(path.join(ROOT, 'charm-nest-export.js'));
const AE = require(path.join(ROOT, 'charm-nest-art-edit.js'));
let n = 0; const ok = (c, m) => { assert.ok(c, m); n++; }, eq = (a, b, m) => { assert.deepEqual(a, b, m); n++; };
const near = (a, b, e, m) => { assert.ok(Math.abs(a - b) <= e, `${m}: ${a} vs ${b}`); n++; };
const sq = (x, y, w, h) => [[['m', [x, y]], ['l', [x + w, y]], ['l', [x + w, y + h]], ['l', [x, y + h]], ['h']]];
const ident = { M: [1, 0, 0, 1, 0, 0], R: [1, 0, 0, 1, 0, 0] };
const toItem = (fr, subs) => subs.map(sp => sp.map(o => (o[0] === 'h' ? ['h'] : [o[0]].concat(o.slice(1).map(fr.toItem)))));
const SHEET = { wPt: 220, hPt: 120 };

/** One charm form of a written sheet: its decoded stream and its Resources resolved all the way down (object numbers left out), to compare two files' copies of one piece. */
async function formsOf(bytes) {
  const doc = await L.PDFDocument.load(bytes), ctx = doc.context, page = doc.getPage(0), xo = ctx.lookup(page.node.Resources().get(L.PDFName.of('XObject')));
  const deep = (v, seen) => { v = v instanceof L.PDFRef ? ctx.lookup(v) : v; if (v instanceof L.PDFDict) { if (seen.has(v)) return '<loop>'; seen.add(v); return '<<' + v.entries().map(([k, x]) => k.toString() + ' ' + deep(x, seen)).sort().join(' ') + '>>'; } if (v instanceof L.PDFArray) return '[' + v.asArray().map(x => deep(x, seen)).join(' ') + ']'; if (v && v.dict) return 'stream ' + deep(v.dict, seen) + ' ' + Buffer.from(L.decodePDFRawStream ? L.decodePDFRawStream(v).decode() : v.getContents()).toString('latin1').length; return String(v); };
  const out = {};
  for (const [k, ref] of xo.entries()) { const st = ctx.lookup(ref); out[k.toString().slice(1).replace(/-\d+$/, '')] = { content: Buffer.from(L.decodePDFRawStream(st).decode()).toString('latin1'), res: deep(st.dict.get(L.PDFName.of('Resources')), new Set()), bbox: deep(st.dict.get(L.PDFName.of('BBox')), new Set()) }; }
  return out;
}
const dxfCount = (txt, type) => (txt.match(new RegExp('\\r\\n0\\r\\n' + type + '\\r\\n', 'g')) || []).length;

(async () => {
  /* ── A · a real per-SKU master ── */
  const real = await P.parseSource(new Uint8Array(fs.readFileSync(path.join(__dirname, 'fixtures', 'MASTER_5X8_5.ai'))), 'MASTER_5X8_5.ai');
  const c0 = P.groupCharms(real).charms[0]; eq(P.integrateRings(c0).left, [], 'A the real master reads as the page reads it (rings integrated)');
  const base = Object.assign(c0, { id: 'm', sourceId: 's', name: 'MASTER_5X8_5', centerPt: [(c0.bbox[0] + c0.bbox[2]) / 2, (c0.bbox[1] + c0.bbox[3]) / 2], strokePt: 0.5, hash: 'm5x8' });
  ok(base.topIndices.length === 1 && real.segments[base.topIndices[0]].kind === 'xobj', 'A the whole charm is ONE form of the master (the reason it is drawn again, not copied)');
  const ids = AE.memberIds(base), roles = base.members.map(m => AE.roleOf(m, base));
  eq(roles.slice().sort(), ['cut', 'cut', 'line', 'line'], 'A two cut lines and two red line engravings');
  const lineAt = roles.indexOf('line'), cx = base.centerPt[0], cy = base.centerPt[1], fr = AE.frame(ident);
  const rec = Object.assign({ at: FIXED, by: 'Ana_M' }, AE.encode({ removed: [ids[lineAt]], added: [{ id: 'dot', kind: 'text', role: 'hatch', text: '1', subpaths: toItem(fr, sq(cx - 1, cy - 1, 2, 2)) }, { id: 'tick', role: 'line', stroke: [1, 0, 0], lwPt: 0.3, subpaths: [[['m', fr.toItem([cx - 2, cy + 3])], ['l', fr.toItem([cx + 2, cy + 3])]]] }] }, { charm: base, view: ident, face: 'front', placed: true }));
  const piece = id => Object.assign({}, base, { id, poolId: '4170000001_1001_' + id.slice(1) });
  const edited = AE.apply(piece('p2'), rec, 'front');
  const sheet = async list => { const placements = list.map((c, i) => ({ charm: c, angle: i === 1 ? 30 : 0, cxPt: 40 + 70 * i, cyPt: 60, scale: 0.971 })); reseed(); const bytes = await P.buildSheet({ sheet: SHEET, placements, sources: new Map([['s', real]]), title: 'art edit' }); return { bytes, placements }; };
  const plain = await sheet([piece('p1'), piece('p2'), piece('p3')]), mixed = await sheet([piece('p1'), edited, piece('p3')]);
  const f0 = await formsOf(plain.bytes), f1 = await formsOf(mixed.bytes);
  for (const k of ['Charm0', 'Charm2']) { eq(f1[k].content, f0[k].content, `A ${k}: the other piece's drawing is byte for byte what it was`); eq(f1[k].res, f0[k].res, `A ${k}: and so are its resources (no layer key was added to the shared ones)`); eq(f1[k].bbox, f0[k].bbox, `A ${k}: and its box`); }
  ok(mixed.placements[0].layerSig === plain.placements[0].layerSig && mixed.placements[2].layerSig === plain.placements[2].layerSig, 'A their layer signatures (bytes, box, place) are the same');
  ok(f1.Charm1.content !== f0.Charm1.content, 'A the edited piece is written anew');
  ok(/\bDo\b/.test(f0.Charm1.content) && !/\bDo\b/.test(f1.Charm1.content), 'A it no longer copies the master\'s form (the form that held the removed part is out)');
  ok(/\/OC \/aeL\d+ BDC [^\n]*0 0 1 rg/.test(f1.Charm1.content) && /aeL\d+/.test(f1.Charm1.res), 'A its parts are written in the master\'s layers, named from its own Resources');
  const off = AE.apply(edited, null, 'front'), back0 = await sheet([piece('p1'), off, piece('p3')]);
  ok(Buffer.compare(Buffer.from(back0.bytes), Buffer.from(plain.bytes)) === 0, 'A an edit taken off writes exactly the file of a piece never edited (' + plain.bytes.length + ' bytes)');
  // what the edited piece draws: parsed back from the written sheet
  const q = await P.parseSource(mixed.bytes, 'sheet.ai'), forms = q.segments.filter(s => s.kind === 'xobj'), kids = f => f.children.filter(k => k.kind === 'path');
  eq(forms.length, 3, 'A three pieces on the sheet');
  const layersOf = f => kids(f).map(k => k.layer + (k.fill ? ':F' + k.fillRGB.join(',') : '') + (k.stroke ? ':S' + k.strokeRGB.map(v => +v.toFixed(3)).join(',') : '')).sort();
  eq(layersOf(forms[0]), ['CUT:S0,0,0', 'CUT:S0,0,0', 'ENGRAVE:S1,0,0', 'ENGRAVE:S1,0,0'], 'A a piece never edited: the master\'s four parts');
  eq(layersOf(forms[1]), ['CUT:S0,0,0', 'CUT:S0,0,0', 'ENGRAVE:F0,0,1', 'ENGRAVE:S1,0,0', 'ENGRAVE:S1,0,0'], 'A the edited piece: its cut lines, ONE of its two line engravings, the new red tick and the new blue dot, all on the master\'s layers');
  { // the parts land where the piece is on the sheet: its placement's matrix over the charm's own points
    const pl = mixed.placements[1], th = -pl.angle * Math.PI / 180, s = pl.scale, c = Math.cos(th) * s, sn = Math.sin(th) * s, e = pl.cxPt - (c * cx - sn * cy), f = SHEET.hPt - pl.cyPt - (sn * cx + c * cy);
    const at = (x, y) => [c * x - sn * y + e, sn * x + c * y + f], dot = kids(forms[1]).find(k => k.fill), want = [at(cx - 1, cy - 1), at(cx + 1, cy - 1), at(cx + 1, cy + 1), at(cx - 1, cy + 1)];
    const wb = [Math.min(...want.map(p => p[0])), Math.min(...want.map(p => p[1])), Math.max(...want.map(p => p[0])), Math.max(...want.map(p => p[1]))];
    dot.bbox.forEach((v, i) => near(v, wb[i], 1e-3, 'A the dot is where the piece put it, turned 30° with it (' + i + ')'));
    const gone = base.members[lineAt].bbox, removedThere = kids(forms[1]).some(k => k.stroke && k.strokeRGB[0] === 1 && Math.abs(k.bbox[0] - at(gone[0], gone[1])[0]) < 1e-3 && Math.abs((k.bbox[2] - k.bbox[0]) - (forms[0].children.find(z => z.kind === 'path' && z.stroke && z.strokeRGB[0] === 1 && Math.abs((z.bbox[2] - z.bbox[0]) - ((gone[2] - gone[0]) * s)) < 1e-3) ? (gone[2] - gone[0]) * s : -1)) < 1e-3);
    ok(!removedThere, 'A the removed line engraving is not drawn');
  }

  /* ── B · the calendar ── */
  const cal = await require('./_artCalendar.cjs')(ROOT), cb = cal.charm, cids = AE.memberIds(cb), view = G.backView(cb, { face: 'front', upAngle: 75, res: 6 }), cfr = AE.frame(view);
  const dayBox = cb.members[cal.day(2, 3)].bbox, dx = (dayBox[0] + dayBox[2]) / 2, dy = (dayBox[1] + dayBox[3]) / 2, [bx, by] = cal.at(20, 68);
  const crec = Object.assign({ at: FIXED, by: 'Ana_M' }, AE.encode({ removed: cal.month.map(i => cids[i]), added: [0, 1, 2].map(k => ({ id: 'jun' + k, kind: 'text', role: 'art', fill: '#000', subpaths: toItem(cfr, sq(bx + 2 + 9 * k, by + 0.5, 5, 7)) })).concat([{ id: 'date', kind: 'group', children: [{ id: 'd2', kind: 'text', fill: [0, 0, 0], subpaths: toItem(cfr, sq(dx - 3, dy - 2.5, 2.5, 5)) }, { id: 'd4', kind: 'text', fill: [0, 0, 0], subpaths: toItem(cfr, sq(dx + 0.5, dy - 2.5, 2.5, 5)) }] }]) }, { charm: cb, view, face: 'front', placed: true }));
  const cEd = AE.apply(Object.assign({}, cb, { id: 'cal-2', poolId: '4170000002_1002_1' }), crec, 'front');
  reseed(); const calPl = [{ charm: Object.assign({}, cb, { id: 'cal-1' }), angle: 0, cxPt: 50, cyPt: 60, scale: 1 }, { charm: cEd, angle: 0, cxPt: 150, cyPt: 60, scale: 1 }];
  const calBytes = await P.buildSheet({ sheet: SHEET, placements: calPl, sources: new Map([['s', cal.parsed]]), title: 'calendar' });
  const cq = await P.parseSource(calBytes, 'cal-sheet.ai'), cforms = cq.segments.filter(s => s.kind === 'xobj');
  const fillsOf = f => kids(f).filter(k => k.fill);
  eq(kids(cforms[0]).length, 24, 'B the calendar never edited: 24 parts (outline, three letters, twenty squares)');
  eq(kids(cforms[1]).length, 26, 'B the edited calendar: 24 − 3 letters + 3 new letters + 2 digits');
  ok(fillsOf(cforms[1]).every(k => k.fillRGB.join() === '0,0,1' && !k.stroke && k.layer === 'ENGRAVE'), 'B every fill of it is blue hatching on ENGRAVE (the new month, the date)');
  ok(fillsOf(cforms[0]).every(k => k.fillRGB.join() === '0,0,1'), 'B (the master\'s own letters are hatched blue too: hatchCopies)');
  const shift = (cforms[1].bbox[0] + cforms[1].bbox[2]) / 2 - (cforms[0].bbox[0] + cforms[0].bbox[2]) / 2;
  const oldLetters = fillsOf(cforms[0]).map(k => k.bbox.map((v, i) => +(v + (i % 2 ? 0 : shift)).toFixed(2)).join()), newFills = fillsOf(cforms[1]).map(k => k.bbox.map(v => +v.toFixed(2)).join());
  ok(oldLetters.every(b => !newFills.includes(b)), 'B none of "MAY" is left');
  { // the second calendar is not turned and not scaled: a charm point (x, y) is at (x + e, y + f) on the sheet
    const e = calPl[1].cxPt - cb.centerPt[0], f = SHEET.hPt - calPl[1].cyPt - cb.centerPt[1], on = b => [b[0] + e, b[1] + f, b[2] + e, b[3] + f].map(v => +v.toFixed(3)).join();
    eq(fillsOf(cforms[1]).map(k => k.bbox.map(v => +v.toFixed(3)).join()).filter(b => [on([dx - 3, dy - 2.5, dx - 0.5, dy + 2.5]), on([dx + 0.5, dy - 2.5, dx + 3, dy + 2.5])].includes(b)).length, 2, 'B the two digits of the date, each where the editor put it');
    ok(kids(cforms[1]).some(k => k.stroke && !k.fill && k.bbox.map(v => +v.toFixed(3)).join() === on(dayBox)), 'B in the day square they were put in (row 3, column 4), which is kept');
  }
  ok(kids(cforms[1]).some(k => k.layer === 'CUT' && k.stroke && k.closed && !k.fill && G.isCutLine(k)), 'B the cut outline is kept');
  const regroup = P.groupCharms(cq).charms;
  eq(regroup.length, 2, 'B the written sheet groups as two charms again'); ok(regroup.every(c => c.outline && G.isCutLine(c.outline)), 'B each with its cut outline');

  /* ── C · export and DXF ── */
  const recOf = (s, list) => ({ id: 'sheet-art', fileBase: 'GF_art', metal: 'gold', charms: list.map(c => ({ id: c.id, poolId: c.poolId })), poolIds: list.map(c => c.poolId), placements: s.placements.map((pl, i) => ({ id: list[i].id, n: i + 1, layer: pl.layerName, scale: pl.scale })), backPool: [], outputs: {} });
  reseed(); const made0 = await E.compose(plain.bytes, recOf(plain, [piece('p1'), piece('p2'), piece('p3')]), []);
  reseed(); const made1 = await E.compose(mixed.bytes, recOf(mixed, [piece('p1'), edited, piece('p3')]), []);
  const lay0 = await P.parseSource(made0.ai, 'x0'), lay1 = await P.parseSource(made1.ai, 'x1');
  const leaves1 = E.leaves(lay1), leaves0 = E.leaves(lay0);
  eq(leaves1.length, leaves0.length + 1, 'C the production .ai: one part removed, two added (' + leaves0.length + ' → ' + leaves1.length + ')');
  ok(leaves1.some(p => p.fill && p.fillRGB.join() === '0,0,1' && p.layer === 'ENGRAVE') && !leaves0.some(p => p.fill), 'C the blue dot is in it, on ENGRAVE');
  const dxf0 = E.dxf(E.productionPaths(lay0), E.layerNames(lay0)).text, dxf1 = E.dxf(E.productionPaths(lay1), E.layerNames(lay1)).text;
  eq(dxfCount(dxf0, 'HATCH'), 0, 'C the master draws no hatching: its DXF has no HATCH'); eq(dxfCount(dxf1, 'HATCH'), 1, 'C the edited piece\'s dot is one HATCH in the DXF');
  ok(dxfCount(dxf1, 'LWPOLYLINE') === dxfCount(dxf0, 'LWPOLYLINE'), 'C its polylines: one line engraving fewer, the new tick one more');
  { const i = dxf1.indexOf('\r\n0\r\nHATCH\r\n'), ent = dxf1.slice(i, i + 400); ok(/\r\n8\r\nENGRAVE\r\n/.test(ent), 'C the HATCH is on the master\'s ENGRAVE layer'); }
  reseed(); const madeCal = await E.compose(calBytes, recOf({ placements: calPl }, [calPl[0].charm, cEd]), []);
  const layCal = await P.parseSource(madeCal.ai, 'xc'), dxfCal = E.dxf(E.productionPaths(layCal), E.layerNames(layCal)).text;
  eq(dxfCount(dxfCal, 'HATCH'), 3 + 5, 'C the calendar sheet\'s DXF: the master\'s three letters hatched on the piece never edited, the new month and the date on the edited one');

  /* ── D · the back ── */
  const bview = G.backView(cb, { upAngle: 90, res: 6 }), bfr = AE.frame(bview);
  const brec = Object.assign({ at: FIXED, by: 'Ana_M' }, AE.encode({ added: [{ id: 'heart', kind: 'path', role: 'hatch', subpaths: toItem(bfr, sq(dx - 2, dy - 2, 4, 4)) }] }, { charm: cEd, view: bview, face: 'back' }));
  const both = AE.apply(cEd, brec, 'back');
  ok(both.members === cEd.members && both.artBack.length === 1, 'D the back\'s part is kept apart from the front\'s members');
  const spec = { charm: both, mirrored: false, parsed: cal.parsed, cutMembers: bview.cutMembers, cx: bview.cx, cy: bview.cy, angleDeg: bview.angleDeg, padPt: 5 / (25.4 / 72), glyphs: [], view: 'asSeenFromBack', title: 'back' };
  reseed(); const plainBack = await P.buildBackFile(spec);
  reseed(); const emptyBack = await P.buildBackFile(Object.assign({}, spec, { artBack: [] }));
  ok(Buffer.compare(Buffer.from(plainBack.bytes), Buffer.from(emptyBack.bytes)) === 0, 'D a back file with no added part is byte for byte as before');
  reseed(); const artBackFile = await P.buildBackFile(Object.assign({}, spec, { artBack: both.artBack }));
  const pb = await P.parseSource(artBackFile.bytes, 'back.ai'), all = pb.segments.concat(pb.nested).filter(s => s.kind === 'path');
  const eng = all.filter(s => s.layer === 'ENGRAVE'), cutRef = all.filter(s => /CUT OUTLINE/.test(s.layer || ''));
  eq(eng.length, 1, 'D the back file\'s ENGRAVE layer carries the added part'); ok(eng[0].fill && !eng[0].stroke && eng[0].fillRGB.join() === '0,0,0', 'D engraved as the words are: a filled path');
  { // turned over with the cut: its place against the mirrored outline is the mirror of its place against the outline
    const ref = cutRef.reduce((a, s) => [Math.min(a[0], s.bbox[0]), Math.min(a[1], s.bbox[1]), Math.max(a[2], s.bbox[2]), Math.max(a[3], s.bbox[3])], [Infinity, Infinity, -Infinity, -Infinity]);
    const ob = cb.outline.bbox, rel = x => (x - ob[0]) / (ob[2] - ob[0]), relY = y => (y - ob[1]) / (ob[3] - ob[1]);
    near((eng[0].bbox[0] + eng[0].bbox[2]) / 2, ref[0] + (1 - rel(dx)) * (ref[2] - ref[0]), 0.3, 'D across: mirrored'); near((eng[0].bbox[1] + eng[0].bbox[3]) / 2, ref[1] + relY(dy) * (ref[3] - ref[1]), 0.3, 'D up and down: as drawn');
  }
  /* ── E · weight and line width (Paul, 27 Oct 2026: "the thickness of the Red engrave line is too thick, I need the ability to control the thickness of fonts ... I need fine font
     thickness control over each font"): what the charm editor hands back at a weight and a width is what the record keeps and what the sheet's file engraves ── */
  {
    const CE = require(path.join(ROOT, 'charm-nest-charm-editor.js')), CM = CE.model, geo = CM.createGeo(null);
    ok(CM.canWeigh(), 'E the editor finds Clipper in node as the page has it (vendor/clipper-6.4.2.js)');
    const base = CM.prepIn(AE.itemsOf(cb, view, 'front'), ['cut']), out0 = CM.exportItems(base, geo);
    const sqIds = [cal.day(1, 0), cal.day(1, 2), cal.day(1, 4)].map(i => cids[i]), thinId = cids[cal.day(3, 1)];
    const boxOf = id => geo.box(base.find(i => i.id === id));
    // the same "8" at three weights, each in a day square; one square's red line thinned to 0.05 mm with the editor's own Stroke
    const weights = [0, 0.05, -0.03];
    const words = weights.map((w, k) => { const b = boxOf(sqIds[k]); return CM.weighItem({ id: 'eight' + k, kind: 'text', role: 'hatch', source: 'added', text: '8', fontKey: 'sans', sizePt: 6, align: 'left', tx: b[0] + 2.4, ty: b[3] - 2.6, sx: 1, sy: 1, rot: 0 }, w, geo); });
    const thin = Object.assign({}, base.find(i => i.id === thinId), { lwPt: +(0.05 * 72 / 25.4).toFixed(9) });
    const after = base.map(i => (i.id === thinId ? thin : i)).concat(words), out1 = CM.exportItems(after, geo), diff = CM.diffFace(out0, out1);
    eq([diff.added.map(a => a.id), Object.keys(diff.changed)], [['eight0', 'eight1', 'eight2'], [thinId]], 'E the editor hands back the three words added and the thinned line changed, nothing else');
    const drawn = diff.added.map(a => a.subpaths), inks = drawn.map(sp => CM.inkArea(sp));
    ok(inks[2] < inks[0] && inks[0] < inks[1], 'E the words\' outlines are more ink at +0.05 mm and less at -0.03 mm than the font\'s own (' + inks.map(v => v.toFixed(2)).join(' / ') + ' pt²)');
    ok(diff.added[1].weightMm === 0.05 && diff.added[2].weightMm === -0.03 && diff.added[0].weightMm === undefined, 'E each says its weight (none at the font\'s own)');
    const wrec = Object.assign({ at: FIXED, by: 'Ana_M' }, AE.encode(diff, { charm: cb, view, face: 'front', placed: true }));
    ok(wrec.added.map(e => e.weightMm).join() === ',0.05,-0.03' && wrec.added.every(e => e.role === 'hatch' && e.fill.join() === '0,0,1'), 'E the record keeps each weight with its outlines, hatched blue: ' + JSON.stringify(wrec.added.map(e => e.weightMm)));
    near(wrec.changed[thinId].lwPt, 0.142, 1e-9, 'E the record keeps the line\'s width as the editor set it (0.05 mm = 0.142 pt)');
    const toSrcBox = sp => { const pts = []; for (const s of sp) for (const o of s) for (const p of o.slice(1)) pts.push(cfr.toSource(p)); return [Math.min(...pts.map(p => p[0])), Math.min(...pts.map(p => p[1])), Math.max(...pts.map(p => p[0])), Math.max(...pts.map(p => p[1]))]; };
    wrec.added.forEach((e, k) => { const got = toSrcBox(toItem(cfr, AE.fromD(e.d))), want = toSrcBox(drawn[k]); got.forEach((v, i) => near(v, want[i], 0.002, `E the record's outlines of the ${weights[k]} mm "8" are the editor's (${i})`)); });
    // the round trip: the piece made with the record, its parts as the editor opens them again, decode
    const wed = AE.apply(Object.assign({}, cb, { id: 'cal-3', poolId: '4170000003_1003_1' }), wrec, 'front');
    const reopened = AE.itemsOf(wed, view, 'front'), r1 = reopened.find(i => i.id === 'eight1'), r2 = reopened.find(i => i.id === 'eight2');
    ok(wed.members.find(m => m.artMemberId === 'eight1').artWeightMm === 0.05 && r1.weightMm === 0.05 && r2.weightMm === -0.03 && !('weightMm' in reopened.find(i => i.id === 'eight0')), 'E opened again, the words say their weight (the piece\'s parts carry it)');
    ok(AE.decode(wrec, wed, view).added.map(a => a.weightMm).join() === ',0.05,-0.03', 'E decode gives it back too');
    near(reopened.find(i => i.id === thinId).lwPt, 0.142, 1e-9, 'E and the thinned line its width');
    { // a weight changed on words opened again grows their saved outlines by the difference only: 0.05 → 0.08 mm is 0.015 mm more on every edge
      const it = CM.prepIn([r1])[0], b0 = geo.box(it), more = CM.weighItem(it, 0.08, geo), b1 = geo.box(more), d = 0.015 * 72 / 25.4;
      ok(more.weightMm === 0.08 && b1[0] < b0[0] - d * 0.9 && b1[0] > b0[0] - d * 2.1 && b1[3] > b0[3] + d * 0.9 && b1[3] < b0[3] + d * 2.1, 'E words opened again at 0.05 mm and set to 0.08 mm grow by the 0.03 mm between: ' + JSON.stringify([b0, b1].map(b => b.map(v => +v.toFixed(3)))));
      const same = CM.weighItem(it, 0.05, geo); ok(same === it, 'E and set to the weight they have, nothing changes');
    }
    reseed(); const wpl = [{ charm: wed, angle: 0, cxPt: 60, cyPt: 60, scale: 1 }];
    const wBytes = await P.buildSheet({ sheet: SHEET, placements: wpl, sources: new Map([['s', cal.parsed]]), title: 'weight' });
    const wq = await P.parseSource(wBytes, 'weight-sheet.ai'), wform = wq.segments.filter(s => s.kind === 'xobj')[0], wk = kids(wform);
    const e = wpl[0].cxPt - cb.centerPt[0], f = SHEET.hPt - wpl[0].cyPt - cb.centerPt[1], onSheet = b => [b[0] + e, b[1] + f, b[2] + e, b[3] + f];
    const fills = wk.filter(k => k.fill && k.fillRGB.join() === '0,0,1');
    drawn.forEach((sp, k) => { const want = onSheet(toSrcBox(sp)), hit = fills.find(k2 => k2.bbox.every((v, i) => Math.abs(v - want[i]) < 0.003)); ok(!!hit, `E the sheet's file engraves the ${weights[k]} mm "8" with the editor's outlines (its box to 0.003 pt): ${JSON.stringify(want.map(v => +v.toFixed(3)))}`); });
    const wide = drawn.map(sp => { const b = toSrcBox(sp); return Math.hypot(b[2] - b[0], b[3] - b[1]); });
    ok(wide[2] < wide[0] && wide[0] < wide[1], 'E in the file too, -0.03 mm < the font\'s own < +0.05 mm');
    const reds = wk.filter(k => k.stroke && !k.fill && k.strokeRGB[0] > 0.5 && k.strokeRGB[2] < 0.5), thinned = reds.filter(k => Math.abs(k.lwPt - 0.142) < 1e-6), rest = reds.filter(k => Math.abs(k.lwPt - 0.3) < 1e-6);
    ok(reds.length === 20 && thinned.length === 1 && rest.length === 19, 'E the file writes the thinned day square at 0.142 pt (0.05 mm) and the other 19 at their own 0.3 pt: ' + JSON.stringify(reds.map(k => +k.lwPt.toFixed(3))));
    { const tb = toSrcBox(thin.subpaths.map(sp => sp)), want = onSheet(tb); ok(thinned[0].bbox.every((v, i) => Math.abs(v - want[i]) < 0.003), 'E …at the square that was thinned'); }
  }

  console.log(`PASS ${n} checks: ART EDIT output — the edited piece drawn again from its own members (removed, added, in the master's layers), every other piece byte for byte; the calendar's month and date; export and DXF carry the additions; the back's addition in the back file; the weight's outlines and the line's width as the editor set them`);
})().catch(e => { console.error(e); process.exit(1); });
