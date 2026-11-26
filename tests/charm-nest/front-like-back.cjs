/* FRONTLIKEBACK (Paul, 25 Nov 2026: "there is no difference between back or front engraving it should be exactly the same throughout the entire system for everything. The only difference is
   one is on the front one is on the back"). Offline: pdf-lib, JSZip and the repo's own modules, a frozen clock and a seeded Math.random; no network.
     A  the Charm Editor's ADDED parts on a front are the charm's artFront (as a back's are its artBack), not its drawing; a cut line added stays the drawing
     B  the sheet's drawing carries them (FRONTINSHEET, Paul 26 Nov 2026: "The front engravings are not appearing on the downloaded .ai illustrator and .DXF files"): a front is engraved on its
        charm in the cut run, so buildSheet draws them exactly as it did before 25 Nov; a piece whose front has no engraving file of its own has them in its .ai and .dxf
     C  the front's engraving file carries them in its ENGRAVE layer, exactly as a back file carries its artBack
     D  the download engraves them once: from the sheet's drawing, the file's copy left out (with no front file at all they are still in the .ai and the .dxf)
     E  a sheet drawn BEFORE that day (the parts in the charm's own drawing) with a front file written after it: the parts are engraved once, not twice
     F  a front approved BEFORE that day, made only of the editor's parts (artOnly, a file with nothing on ENGRAVE): the download goes on (its parts are in that sheet's drawing), it no longer
        stops ("Front engraving contains no engraving paths": three sheets of 22 Nov could not be downloaded)
     G  a front with words and no ENGRAVE paths is still refused
     H  a front that cannot be put on its charm is NEVER laid above the sheet like a back (FRONTONCHARM): it is left out and the manifest says why
   node tests/charm-nest/front-like-back.cjs */
'use strict';
const assert = require('node:assert/strict'), fs = require('node:fs'), path = require('node:path');
const ROOT = path.join(__dirname, '..', '..');
const FIXED = Date.UTC(2026, 10, 25, 12, 0, 0), RealDate = Date;
global.Date = class extends RealDate { constructor(...a) { if (a.length) super(...a); else super(FIXED); } static now() { return FIXED; } };
let seed = 1; const reseed = () => { seed = 20261125; };
Math.random = () => { seed = (Math.imul(seed, 1664525) + 1013904223) >>> 0; return seed / 4294967296; };
global.window = global; global.self = global;
global.PDFLib = require(path.join(ROOT, 'vendor/pdf-lib-1.17.1.min.js')); global.JSZip = require(path.join(ROOT, 'vendor/jszip-3.10.1.min.js'));
require(path.join(ROOT, 'charm-nest-pdf.js'));
global.CharmNestVector = require(path.join(ROOT, 'charm-nest-vector.js'));
global.CharmNestGeom = require(path.join(ROOT, 'charm-nest-geom.js'));
const P = global.CharmNestPDF, G = global.CharmNestGeom, E = global.CharmNestExport = require(path.join(ROOT, 'charm-nest-export.js'));
const AE = require(path.join(ROOT, 'charm-nest-art-edit.js')), BF = require(path.join(ROOT, 'charm-nest-back-file.js'));
let n = 0; const ok = (c, m) => { assert.ok(c, m); n++; console.log('PASS  ' + m); }, eq = (a, b, m) => { assert.deepEqual(a, b, m); n++; console.log('PASS  ' + m); };
const sq = (x, y, w, h) => [[['m', [x, y]], ['l', [x + w, y]], ['l', [x + w, y + h]], ['l', [x, y + h]], ['h']]];
const ident = { M: [1, 0, 0, 1, 0, 0], R: [1, 0, 0, 1, 0, 0] };
const toItem = (fr, subs) => subs.map(sp => sp.map(o => (o[0] === 'h' ? ['h'] : [o[0]].concat(o.slice(1).map(fr.toItem)))));
const SHEET = { wPt: 220, hPt: 120 };
const inside = (b, box, tol) => b[0] >= box[0] - tol && b[1] >= box[1] - tol && b[2] <= box[2] + tol && b[3] <= box[3] + tol;

(async () => {
  const real = await P.parseSource(new Uint8Array(fs.readFileSync(path.join(__dirname, 'fixtures', 'MASTER_5X8_5.ai'))), 'MASTER_5X8_5.ai');
  const c0 = P.groupCharms(real).charms[0]; P.integrateRings(c0);
  const base = Object.assign(c0, { id: 'm', sourceId: 's', name: 'MASTER_5X8_5', centerPt: [(c0.bbox[0] + c0.bbox[2]) / 2, (c0.bbox[1] + c0.bbox[3]) / 2], strokePt: 0.5, hash: 'm5x8' });
  const cx = base.centerPt[0], cy = base.centerPt[1], fr = AE.frame(ident);
  const piece = Object.assign({}, base, { id: 'p1', poolId: '4170000001_1001_1' });
  // a person adds a blue dot to the FRONT (no words): the front is made only of the editor's parts
  const rec = Object.assign({ at: FIXED, by: 'Ana_M' }, AE.encode({ removed: [], added: [{ id: 'dot', kind: 'text', role: 'hatch', text: '1', subpaths: toItem(fr, sq(cx - 1, cy - 1, 2, 2)) }] }, { charm: piece, view: ident, face: 'front' }));
  const ed = AE.apply(piece, rec, 'front');

  /* ── A ── */
  eq((ed.artFront || []).map(m => m.artMemberId), ['dot'], 'A the dot added to the front is the charm\'s artFront');
  eq(ed.members.length, base.members.length, 'A and not one of its members (its drawing is the master\'s)');
  eq(BF.faceArt(ed, 'front').length, 1, 'A faceArt reads a front\'s added parts as it reads a back\'s');
  const edB = AE.apply(piece, Object.assign({}, rec, { face: 'back' }), 'back');
  eq([(edB.artBack || []).length, (edB.artFront || []).length], [1, 0], 'A the same part added to the BACK is its artBack: one rule, two faces');

  /* ── B ── */
  const sheetOf = async (charm, id) => { reseed(); const placements = [{ charm, angle: 0, cxPt: 80, cyPt: 60, scale: 1 }]; const bytes = await P.buildSheet({ sheet: SHEET, placements, sources: new Map([['s', real]]), title: id });
    return { bytes, rec: { id, fileBase: id, metal: 'gold', charms: [{ id: charm.id, poolId: charm.poolId }], poolIds: [charm.poolId], placements: placements.map((pl, i) => ({ id: charm.id, n: i + 1, layer: pl.layerName, scale: pl.scale })), backPool: [], outputs: {} } }; };
  const fresh = await sheetOf(ed, 'new-sheet');
  const fillsIn = async bytes => (await P.parseSource(bytes, 'x')).segments.filter(s => s.kind === 'xobj').flatMap(f => (f.children || []).filter(k => k.kind === 'path' && k.fill)).length;
  eq(await fillsIn(fresh.bytes), 1, 'B FRONTINSHEET: a sheet drawn now has the dot in the charm\'s drawing (the front is engraved on the charm in the cut run)');
  // a sheet drawn before 25 Nov 2026: the dot was one of the charm's members, drawn again from its geometry (artRedraw)
  const oldCharm = Object.assign({}, ed, { members: ed.members.concat(ed.artFront) }); delete oldCharm.artFront;
  const old = await sheetOf(oldCharm, 'old-sheet');
  eq(await fillsIn(old.bytes), 1, 'B (a sheet drawn before that day has it in the charm\'s drawing)');
  { const sig = async bytes => JSON.stringify((await P.parseSource(bytes, 'x')).segments.filter(s => s.kind === 'xobj').map(f => (f.children || []).filter(k => k.kind === 'path').map(k => [k.layer, k.fill, k.stroke, (k.fillRGB || []).join(), (k.bbox || []).map(v => +v.toFixed(4))])));
    eq(await sig(fresh.bytes), await sig(old.bytes), 'B FRONTINSHEET: the charm is drawn exactly as a sheet drawn before 25 Nov drew it (same parts, layers, colours and places)'); }

  /* ── C ── */
  const frontFile = async (charm, art) => { const view = G.backView(charm, { upAngle: 90, face: 'front' }); reseed();
    return (await P.buildBackFile({ charm, mirrored: false, parsed: real, cutMembers: view.cutMembers, cx: view.cx, cy: view.cy, angleDeg: view.angleDeg, padPt: 5 / (25.4 / 72), glyphs: [], face: 'front', view: 'asSeenFromFront', title: 'front', meta: { poolId: charm.poolId }, ...(art ? { artBack: art } : {}) })).bytes; };
  const spec = BF.backSpec({ charm: ed, parsed: real, view: G.backView(ed, { upAngle: 90, face: 'front' }), fit: { glyphs: [], weight: 'Regular', size: 0, capMm: 0, angle: 0 }, face: 'front', title: 't', meta: {} });
  eq((spec.artBack || []).length, 1, 'C the front\'s file spec hands its added parts to the builder (backSpec), as a back\'s');
  const newFile = await frontFile(ed, BF.faceArt(ed, 'front'));
  const engraveOf = async bytes => E.leaves(await P.parseSource(bytes, 'f')).filter(p => /^ENGRAVE$/i.test(p.layer || ''));
  eq((await engraveOf(newFile)).length, 1, 'C the front\'s engraving file carries the dot on its ENGRAVE layer');
  const oldFile = await frontFile(ed, null);
  eq((await engraveOf(oldFile)).length, 0, 'C (a front file written before that day carried nothing on ENGRAVE)');
  const frontRec = (bytes, extra) => Object.assign({ poolId: piece.poolId, face: 'front', artOnly: true, approvedAt: FIXED - 1000, approvedBy: 'Ana_M', text: '', lines: [], bytes }, extra || {});

  /* ── D ── */
  // the case found live on 26 Nov (SS_Nov.22.26_Set-2_Sheet-1: "dnc" drawn on a puzzle piece's front, no words, no front engraving job, so no front file): still in the .ai and the .dxf
  reseed(); const bare = await E.compose(fresh.bytes, fresh.rec, [], []);
  const bp = await P.parseSource(bare.ai, 'bare'), bareFills = E.leaves(bp).filter(p => p.fill && p.fillRGB.join() === '0,0,1');
  eq(bareFills.length, 1, 'D FRONTINSHEET: no front file at all: the dot is in the download\'s .ai');
  const form = (await P.parseSource(fresh.bytes, 's')).segments.find(s => s.kind === 'xobj');
  ok(inside(bareFills[0].bbox, form.bbox, 0.5), 'D on its charm, where the sheet puts it');
  { const t = E.dxf(E.productionPaths(bp), E.layerNames(bp)).text, hatches = (t.match(/\r\n *0\r\nHATCH\r\n/g) || []).length; eq(hatches, 1, 'D FRONTINSHEET: and in the .dxf (one HATCH)'); }
  // with its front file (the file carries the same dot): engraved ONCE, from the sheet's drawing
  reseed(); const made = await E.compose(fresh.bytes, fresh.rec, [], [frontRec(newFile)]);
  const mk = await P.parseSource(made.ai, 'made'), frontPaths = E.leaves(mk).filter(p => /^FRONT \S+ \/ ENGRAVE$/.test(p.layer || ''));
  eq(frontPaths.length, 0, 'D with its front file: the file\'s copy of the dot is left out (the sheet draws it)');
  eq(E.leaves(mk).filter(p => p.fill && p.fillRGB.join() === '0,0,1').length, 1, 'D the dot is engraved once');
  ok(made.frontLayout && made.frontLayout[0].placed === true && made.frontLayout[0].inDrawing === 1, 'D the manifest says it is on its charm, its one part in the drawing');

  /* ── E ── */
  reseed(); const twice = await E.compose(old.bytes, old.rec, [], [frontRec(newFile)]);
  const tw = await P.parseSource(twice.ai, 'twice');
  eq(E.leaves(tw).filter(p => /^FRONT \S+ \/ ENGRAVE$/.test(p.layer || '')).length, 0, 'E an old sheet already draws the dot: the file\'s copy is left out (engraved once)');
  eq(twice.frontLayout[0].inDrawing, 1, 'E the manifest says one part was already in the drawing');

  /* ── F ── */
  reseed(); let oldMade = null, err = null; try { oldMade = await E.compose(old.bytes, old.rec, [], [frontRec(oldFile)]); } catch (e) { err = e; }
  ok(!err && oldMade && oldMade.ai && oldMade.ai.length > 0, 'F an old front made only of the editor\'s parts no longer stops the download' + (err ? ' (' + err.message + ')' : ''));
  ok(oldMade.frontLayout[0].inDrawing === true && /sheet drawing/.test(oldMade.frontLayout[0].reason), 'F the manifest says its parts are in the sheet drawing');
  const parsedMade = await P.parseSource(oldMade.ai, 'old-made'), dxf = E.dxf(E.productionPaths(parsedMade), E.layerNames(parsedMade));   // (the download's own fallback when charm-nest-dxf.js is not loaded)
  ok(dxf && dxf.entityCount > 0 && /ENTITIES/.test(dxf.text), 'F and the .dxf is written (' + dxf.entityCount + ' entities)');

  /* ── G ── */
  await assert.rejects(() => E.compose(old.bytes, old.rec, [], [frontRec(oldFile, { artOnly: false, text: 'Mia', lines: ['Mia'] })]), /Front engraving contains no engraving paths/); n++;
  console.log('PASS  G a front with words and no ENGRAVE paths is still refused');

  /* ── H · FRONTONCHARM (Paul, 25 Nov 2026: "front engravings do not end up as a charm on top of the sheet like back engravings do") ── */
  const L = global.PDFLib, noFrame = await L.PDFDocument.load(newFile); noFrame.getInfoDict().delete(L.PDFName.of('BritesFrontFrame'));
  const unplaced = await noFrame.save({ useObjectStreams: false });
  reseed(); const left = await E.compose(fresh.bytes, fresh.rec, [], [frontRec(unplaced)]);
  const lp = await P.parseSource(left.ai, 'left'), sheetP = await P.parseSource(fresh.bytes, 'sheet');
  eq(E.leaves(lp).filter(p => /^FRONT /.test(p.layer || '')).length, 0, 'H a front that cannot be put on its charm is not laid above the sheet (no FRONT layer anywhere)');
  ok(lp.pageH === sheetP.pageH && lp.pageW === sheetP.pageW, 'H the download is the cut sheet alone: nothing was added above it (' + lp.pageW.toFixed(1) + ' × ' + lp.pageH.toFixed(1) + ' pt)');
  ok(left.frontLayout[0].placed === false && left.frontLayout[0].leftOut === true && !!left.frontLayout[0].reason, 'H the manifest says it was left out, and why: ' + left.frontLayout[0].reason);

  console.log(`\n${n}/${n} checks pass`);
})().catch(e => { console.error(e); process.exit(1); });
