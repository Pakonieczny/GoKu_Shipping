// CUSTOMBACK (Paul, 26 Nov 2026: "the new process for adding multiple charms to a custom order in a single .ai illustrator file upload to the custom order does not recognize back engraving ... The back
// engraving portion of a given charm will always be above the main charm and will have the words back engraving underneath the back engraving silhouette which will always be above the primary charm.
// The system will have to be intelligent enough to recognize this and effectively add the back engraving on that particular charm ... and then will need back engraving approval").
//   A  the reading (charm-nest-pdf.js customBacks): each back leaves the charms, its drawing becomes the charm-under-it's back engraving (fileBack / artBack), turned over into the charm's own frame;
//      words typed in several runs; a file of one charm; a back of another size is said and left out; a file with no words reads exactly as before
//   B  the Charm Editor (charm-nest-art-edit.js): an edit keeps the file's back; the editor's back additions are put with it; the editor shows the file's back locked
//   C  the back's engraving file (charm-nest-back-file.js backSpec, charm-nest-pdf.js buildBackFile): it carries the drawing, where the person drew it on the back
//   D  the shared classifier (charm-nest-engrave-classify.js): a custom line with backs is read by no one; each piece with a back waits for Approve as drawn; a piece without one, and a front, are not engraved
//   E  the cloud: the world reads which pieces of the line have a back (afterRead: customBack, engraveCandidate); the piece maker marks them (fileBack) and leaves the line to the engraving;
//      the nest's rebuild cuts each piece from its charm with its back (several charms, and one charm and its back)
// Offline only: pdf-lib, the repo's modules, the shared cloud fakes. Made-up names.   node tests/charm-nest/custom-back.cjs
'use strict';
process.env.EDIT_PASSCODE = 'test-pass';
const path = require('path');
const ROOT = path.join(__dirname, '../..');
const T0 = Date.UTC(2026, 10, 26, 15, 0, 0);
const F = require('./_cloudFakes.cjs')({ now: T0 });
const log0 = console.log.bind(console); if (!process.env.CB_LOUD) { console.log = () => {}; console.warn = () => {}; }
let failed = 0, passed = 0;
const ok = (name, cond, detail) => { log0((cond ? 'OK   ' : 'FAIL ') + name + (cond || detail === undefined ? '' : ' :: ' + JSON.stringify(detail).slice(0, 1200))); if (cond) passed++; else failed++; };
const near = (b, want, tol = 0.6) => Array.isArray(b) && b.length === 4 && b.every((v, i) => Math.abs(v - want[i]) <= tol);
global.window = global; global.self = global;
global.PDFLib = require(path.join(ROOT, 'vendor/pdf-lib-1.17.1.min.js'));
const Pdf = require(path.join(ROOT, 'netlify/functions/_charmNestPdf.js')).withCanvas().CharmNestPDF;
global.CharmNestVector = global.CharmNestVector || require(path.join(ROOT, 'charm-nest-vector.js'));
global.CharmNestGeom = global.CharmNestGeom || require(path.join(ROOT, 'charm-nest-geom.js'));
const G = global.CharmNestGeom, AE = require(path.join(ROOT, 'charm-nest-art-edit.js')), BF = require(path.join(ROOT, 'charm-nest-back-file.js'));
const RID = '4260000101', TX = '42600001011', KEY = `${RID}_${TX}`, PDF2 = `charmnest/custom/${RID}/aa11bb22cc33-TWO.pdf`, PDF1 = `charmnest/custom/${RID}/dd44ee55ff66-ONE.pdf`;

/* ── the files: a 40 pt square charm (a hole at its left); above it its BACK (the same square turned over: its hole at the right) with a blue mark drawn inside; the words between ── */
async function fileOf(o) {
  const { PDFDocument, rgb, StandardFonts } = global.PDFLib;
  const doc = await PDFDocument.create(), page = doc.addPage([320, 230]), font = await doc.embedFont(StandardFonts.Helvetica);
  const sq = (x, y, s) => page.drawSvgPath(`M 0 0 L ${s} 0 L ${s} ${s} L 0 ${s} Z`, { x, y: y + s, borderColor: rgb(0, 0, 0), borderWidth: 0.25 });
  const hole = (x, y) => page.drawCircle({ x, y, size: 3, borderColor: rgb(0, 0, 0), borderWidth: 0.25 });
  for (const c of o.charms) {
    sq(c.x, 20, 40); hole(c.x + 5, 55);
    if (c.back) {
      const s = c.back.size || 40, bx = c.x + (40 - s) / 2;
      sq(bx, 110, s); hole(bx + s - 5, 110 + s - 5);
      page.drawRectangle({ x: c.x + 5, y: 120, width: 12, height: 6, color: rgb(0, 0, 1) });
      if (c.back.words !== false) { if (c.back.split) { page.drawText('Back', { x: c.x + 2, y: 88, size: 6, font }); page.drawText('Engraving', { x: c.x + 17, y: 88, size: 6, font }); } else page.drawText(c.back.label || 'Back Engraving', { x: c.x, y: 88, size: 6, font }); }
    }
  }
  return new Uint8Array(await doc.save());
}
const readOf = async bytes => { const parsed = await Pdf.parseSource(bytes, 'f.pdf'), g = Pdf.groupCharms(parsed, { minPt: 6 }), r = Pdf.customBacks(parsed, g); return { parsed, g, r, charms: g.charms.filter(c => !c.open) }; };
const xOf = c => Math.round((c.outline ? c.outline.bbox : c.bbox)[0]);

(async () => {
  try {
    /* ── A · the reading ── */
    const two = await fileOf({ charms: [{ x: 20, back: {} }, { x: 120, back: { split: true } }, { x: 230, back: null }] });
    const A = await readOf(two);
    ok('A1 three charms, two of them with a back above: three charms are read (the backs are not charms)', A.charms.length === 3 && A.r.backs.length === 2 && !A.r.notes.length, { n: A.charms.length, backs: A.r.backs.length, notes: A.r.notes });
    const byX = x => A.charms.find(c => xOf(c) === x);
    ok('A2 the first two charms carry their back (fileBack, and artBack, the frame every back engraving is kept in); the third none', [20, 120].every(x => (byX(x).fileBack || []).length === 1 && (byX(x).artBack || []).length === 1) && !byX(230).fileBack, A.charms.map(c => [xOf(c), (c.fileBack || []).length]));
    // the mark drawn at x+5..x+17, y 120..126 on the back (centre x+20, 130), turned over onto the charm (centre x+20, 40): x+23..x+35, 30..36
    ok('A3 the drawing is turned over onto its charm: mirrored about the charm\'s vertical middle, at the same height in it', near(byX(20).fileBack[0].bbox, [43, 30, 55, 36]) && near(byX(120).fileBack[0].bbox, [143, 30, 155, 36]), [byX(20).fileBack[0].bbox, byX(120).fileBack[0].bbox]);
    ok('A4 the back\'s own hole is never engraved (a cut line of the back), its blue mark is a hatched fill', byX(20).fileBack.every(p => p.fill && p.artRole === 'hatch' && p.fileBack === true), byX(20).fileBack.map(p => [p.fill, p.stroke, p.artRole]));
    ok('A5 the words typed in two runs ("Back", "Engraving") are read as one label', (byX(120).fileBack || []).length === 1);
    const charmBacks = A.charms.map((c, i) => (c.fileBack ? i : -1)).filter(i => i >= 0);
    ok('A6 which charms have a back, by their index in the file (what the card keeps: charmBacks)', charmBacks.length === 2 && charmBacks.every(i => A.charms[i].fileBack), charmBacks);
    const one = await fileOf({ charms: [{ x: 20, back: { label: 'BACK ENGRAVING' } }] }), A1 = await readOf(one);
    ok('A7 a file of ONE charm and its back: one charm, with its back ("BACK ENGRAVING" in capitals too)', A1.charms.length === 1 && (A1.charms[0].fileBack || []).length === 1, { n: A1.charms.length });
    const odd = await fileOf({ charms: [{ x: 20, back: { size: 28 } }, { x: 120, back: null }] }), A2 = await readOf(odd);
    ok('A8 a back far from its charm\'s size is left out and said; it is never a charm, and the charm has no back', A2.charms.length === 2 && !A2.charms.some(c => c.fileBack) && A2.r.notes.length === 1 && /not the size/.test(A2.r.notes[0]), { n: A2.charms.length, notes: A2.r.notes });
    const plain = await fileOf({ charms: [{ x: 20, back: { words: false } }] }), A3 = await readOf(plain);
    ok('A9 no words: the file reads exactly as before (two outlines, two charms, nothing attached)', A3.charms.length === 2 && !A3.charms.some(c => c.fileBack) && !A3.r.backs.length, { n: A3.charms.length });

    /* ── B · the Charm Editor ── */
    const c0 = Object.assign({}, byX(20), { id: 'cust:x:1', poolId: KEY + '_1', hash: 'h1' });
    const kept = AE.apply(c0, null, 'back');
    ok('B1 the back taken back to "no edit" keeps the file\'s back', (kept.artBack || []).length === 1 && kept.artBack[0].fileBack === true, kept.artBack);
    const bview = G.backView(c0, { upAngle: 90, res: 6 }), fr = AE.frame(bview), sqs = (x, y, w, h) => [[['m', [x, y]], ['l', [x + w, y]], ['l', [x + w, y + h]], ['l', [x, y + h]], ['h']]];
    const cx = (c0.outline.bbox[0] + c0.outline.bbox[2]) / 2, cy = (c0.outline.bbox[1] + c0.outline.bbox[3]) / 2;
    const toItem = subs => subs.map(sp => sp.map(o => o[0] === 'h' ? ['h'] : [o[0]].concat(o.slice(1).map(p => fr.toItem(p)))));
    const rec = Object.assign({ at: T0, by: 'Ana_M' }, AE.encode({ added: [{ id: 'dot', kind: 'path', role: 'hatch', subpaths: toItem(sqs(cx - 1, cy + 8, 2, 2)) }] }, { charm: c0, view: bview, face: 'back' }));
    const both = AE.apply(c0, rec, 'back');
    ok('B2 the editor\'s back addition is put WITH the file\'s back, never in its place', (both.artBack || []).length === 2 && both.artBack.some(p => p.fileBack) && both.artBack.some(p => p.artAdded), (both.artBack || []).map(p => [p.fileBack, p.artAdded]));
    const items = AE.itemsOf(c0, bview, 'back'), fileItems = items.filter(i => i.fileBack);
    ok('B3 the editor shows the file\'s back as the file\'s, locked (changed in the file)', fileItems.length === 1 && fileItems[0].editable === false && fileItems[0].origin === 'master', items.map(i => [i.id, i.origin, i.editable]));

    /* ── C · the back's engraving file ── */
    const spec = BF.backSpec({ charm: c0, parsed: A.parsed, view: bview, fit: { glyphs: [], weight: 'Regular', size: 0, capMm: 0, angle: 0 }, title: 'back', meta: { poolId: c0.poolId } });
    ok('C1 the back file is handed the file\'s back drawing (backSpec: artBack)', (spec.artBack || []).length === 1, (spec.artBack || []).length);
    const built = await Pdf.buildBackFile(spec), bp = await Pdf.parseSource(built.bytes, 'back.ai'), all = bp.segments.concat(bp.nested).filter(s => s.kind === 'path');
    const eng = all.filter(s => s.layer === 'ENGRAVE'), ref = all.filter(s => /CUT OUTLINE/.test(s.layer || ''));
    ok('C2 its ENGRAVE layer carries the drawing, one filled part', eng.length === 1 && eng[0].fill, eng.map(s => [s.layer, s.fill]));
    if (eng.length && ref.length) {
      const R = ref.reduce((a, s) => [Math.min(a[0], s.bbox[0]), Math.min(a[1], s.bbox[1]), Math.max(a[2], s.bbox[2]), Math.max(a[3], s.bbox[3])], [Infinity, Infinity, -Infinity, -Infinity]);
      const relX = ((eng[0].bbox[0] + eng[0].bbox[2]) / 2 - R[0]) / (R[2] - R[0]), relY = ((eng[0].bbox[1] + eng[0].bbox[3]) / 2 - R[1]) / (R[3] - R[1]);
      // drawn at x+5..x+17 of 40 (centre 0.275 across) and 120..126 of 110..150 (0.325 up) on the back: the back file, as seen from the back, shows it there
      ok('C3 in the back file (as seen from the back) the drawing is where the person drew it on the back', Math.abs(relX - 0.275) < 0.06 && Math.abs(relY - 0.325) < 0.06, { relX: +relX.toFixed(3), relY: +relY.toFixed(3) });
    } else ok('C3 the back file has its cut reference', false, { eng: eng.length, ref: ref.length });

    /* ── D · the shared classifier ── */
    const Classify = require(path.join(ROOT, 'charm-nest-engrave-classify.js'));
    const calls = [], jobs = [{ key: KEY + ':C1', slot: 'C1', face: 'back', state: 'classify', copies: [KEY + '_1'], fb: true }, { key: KEY + ':C2', slot: 'C2', face: 'back', state: 'classify', copies: [KEY + '_2'], fb: false }, { key: KEY + ':F', face: 'front', state: 'classify', copies: [KEY + '_1'], fb: true }];
    const C = Classify.make({ items: () => new Map(), ensureJobs: () => jobs, setNone: (j, why) => { calls.push(['none', j.key, why]); j.state = 'none'; return j; }, toWords: (j, why) => { calls.push(['words', j.key, why]); j.state = 'review'; return j; },
      setReady: () => null, seals: () => ({ keep() {}, merge: x => x }), neverEngraves: () => false, neverWhy: () => '', NEVER_OPEN: () => true, faceOf: j => j.face || 'back', fileBack: j => !!j.fb, agentCall: async () => { throw new Error('no paid reading may be asked'); } });
    const row = { key: KEY, state: 'pooled', order: { receiptId: RID }, line: { transactionId: TX }, spec: { customBack: 1, engraveCandidate: true, personalization: ['make it shiny'], designSku: 'CUSTOM' }, poolIds: [KEY + '_1', KEY + '_2'] };
    await C.classifyOnce(row, {});
    ok('D1 the piece with a back drawn in its file waits for Approve as drawn (EDITOR WORDS: no words, nothing paid for)', calls.some(c => c[0] === 'words' && c[1] === KEY + ':C1' && /custom file/.test(c[2])), calls);
    ok('D2 the piece without one is not engraved; nor is a front', calls.some(c => c[0] === 'none' && c[1] === KEY + ':C2') && calls.some(c => c[0] === 'none' && c[1] === KEY + ':F'), calls);

    /* ── E · the cloud ── */
    const W = require(path.join(ROOT, 'netlify/functions/_charmNestWorld.js'));
    const wrow = { key: KEY, state: 'pulled', spec: { problems: [], engraveCandidate: true }, poolIds: [] };
    W.afterRead(wrow, null, { metal: 'gold', pieces: 2, sent: true, backs: 1 });
    ok('E1 the world: a sent line with a piece whose charm has a back engraves (customBack, engraveCandidate)', wrow.spec.customBack === 1 && wrow.spec.engraveCandidate === true, wrow.spec);
    const wrow2 = { key: KEY, state: 'pulled', spec: { problems: [], engraveCandidate: true }, poolIds: [] };
    W.afterRead(wrow2, null, { metal: 'gold', pieces: 2, sent: true, backs: 0 });
    ok('E2 ... a sent line with no back engraves nothing, as before', !wrow2.spec.customBack && wrow2.spec.engraveCandidate === false, wrow2.spec);
    const Make = require(path.join(ROOT, 'netlify/functions/_charmNestCustomMake.js'));
    const send = { phase: 'sent', ck: 'custom:' + KEY, rid: RID, files: [{ id: 'f2', name: 'TWO.ai', cloud: { path: PDF2 }, metal: 'gold', qty: 1, pieces: 3, charmBacks }], sent: { id: 's1', at: T0, by: 'Pat', lines: { [KEY]: [{ f: 'f2', i: charmBacks[0] }, { f: 'f2', i: A.charms.findIndex(c => !c.fileBack) }] } } };
    const M = Make.create({ sends: new Map([[KEY, send]]), pdf: () => Pdf, bytes: async p => { if (p !== PDF2) throw new Error('not the file ' + p); return two; }, settings: { minPt: 6, silhouetteRes: 6 } });
    const mrow = { key: KEY, order: { receiptId: RID, createTs: 1795000000, updateTs: 1795000001 }, line: { transactionId: TX, sku: 'CUSTOM_ORDER' }, spec: { quantity: 1, form: 'charm' }, arrivedAt: T0 - 86400e3 };
    const prep = await M.prepare(mrow, { runId: 'run-cb', setId: null });
    const pools = (prep && prep.pools) || [];
    ok('E3 the piece maker: the piece cut from a charm with a back says so on its row (fileBack); the other does not', pools.length === 2 && pools[0].fileBack === 1 && !pools[1].fileBack, pools.map(p => [p.poolId, p.customIndex, p.fileBack]));
    ok('E4 ... and leaves the line to the engraving (needed, to be read), never "no engraving"', mrow.engrave && mrow.engrave.needed === true && mrow.engrave.state === 'classify', mrow.engrave);
    // the nest's rebuild (charm-nest-pool-make.js through _charmNestNestPieces.js): each piece made again from its charm, with its back
    await F.bucket.file(PDF2).save(Buffer.from(two), { contentType: 'application/pdf' });
    await F.bucket.file(PDF1).save(Buffer.from(one), { contentType: 'application/pdf' });
    const Pieces = require(path.join(ROOT, 'netlify/functions/_charmNestNestPieces.js'));
    const bucket = { file: p => ({ download: async () => { const b = p === PDF2 ? two : p === PDF1 ? one : null; if (!b) throw new Error('no such file ' + p); return [Buffer.from(b)]; } }) };
    const S = { settings: { minPt: 6, silhouetteRes: 6, insetPt: 0, maxFill: 0.8 } };
    const rcOf = (n, src) => ({ id: `${src}:${KEY}_${n}`, poolId: `${KEY}_${n}`, sourceId: src, sourceName: 'custom · ' + RID, custom: true, name: `${RID} · Custom · ${n}`, sku: 'CUSTOM', widthPt: 40, heightPt: 40 });
    const poolOf = (n, ci, pdf) => Object.assign({ poolId: `${KEY}_${n}`, orderId: RID, transactionId: TX, lineKey: KEY, sku: 'CUSTOM', material: 'gold', copy: n, quantity: 2, custom: true, customFile: 'f.ai', aiPath: pdf, state: 'nested', widthPt: 40, heightPt: 40, areaPt2: 1600 }, ci == null ? {} : { customIndex: ci });
    const recTwo = { id: 'gold-cb', metal: 'gold', sources: [{ id: 'cust:aa11bb22cc33', name: 'TWO.ai', path: PDF2, custom: true, pool: true }], charms: [rcOf(1, 'cust:aa11bb22cc33'), rcOf(2, 'cust:aa11bb22cc33')] };
    const r2 = await Pieces.create({ db: F.db, bucket, S, log: () => {} }).rebuild(recTwo, new Map([[`${KEY}_1`, poolOf(1, charmBacks[0], PDF2)], [`${KEY}_2`, poolOf(2, A.charms.findIndex(c => !c.fileBack), PDF2)]]));
    const [p1, p2] = r2.charms || [];
    ok('E5 several charms: each piece is made again from its own charm, the one with a back carries it, the other none', r2.missing.length === 0 && p1 && (p1.artBack || []).length === 1 && p2 && !(p2.artBack || []).length && Math.abs(p1.widthPt - 40) < 4, { missing: r2.missing, p1: p1 && [(p1.artBack || []).length, p1.widthPt], p2: p2 && [(p2.artBack || []).length] });
    const recOne = { id: 'gold-cb1', metal: 'gold', sources: [{ id: 'cust:dd44ee55ff66', name: 'ONE.ai', path: PDF1, custom: true, pool: true }], charms: [rcOf(1, 'cust:dd44ee55ff66')] };
    const r1 = await Pieces.create({ db: F.db, bucket, S, log: () => {} }).rebuild(recOne, new Map([[`${KEY}_1`, poolOf(1, null, PDF1)]]));
    const q1 = (r1.charms || [])[0];
    ok('E6 one charm and its back: the piece is the charm alone (its back never folded into it, never cut), its back carried', r1.missing.length === 0 && q1 && (q1.artBack || []).length === 1 && Math.abs(q1.heightPt - 40) < 4, { missing: r1.missing, q1: q1 && [(q1.artBack || []).length, q1.widthPt, q1.heightPt] });
  } catch (e) { ok('run', false, String(e && e.stack || e).split('\n').slice(0, 6).join(' | ')); }
  log0(`\ncustom-back: ${passed} passed, ${failed} failed`);
  process.exit(failed ? 1 : 0);
})();
