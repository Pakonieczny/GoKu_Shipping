// PAIRMIRROR (pairs-1009): left and right earrings are mirror images, and the direction is never lost (Paul, 9 Oct 18:47, contract AMENDMENT 2).
//   node tests/charm-nest/pairs-mirror.cjs
// Offline, deterministic, nothing paid, no network, no Firestore. Proves, with the real modules (charm-nest-pair.js, charm-nest-solver.js, charm-nest-pdf.js, charm-nest-export.js):
//  A  the Right of a matching pair is the EXACT mirror of the Left (cut line, hole, hoop, engraving, bits, hole direction), the mirror of the mirror is the original, the original is untouched
//  B  text is not reversed (a text member of a mirrored charm is the very same object, as drawn; a facing a person set turns with the mirror)
//  C  which piece is mirrored: unknown facing = the Left as drawn and the Right turned; a person's facing; letters, initials, numbers and an old facing "X" are mirrored like any other earring
//     (Paul, 10 Oct 2026: the "reads one way" exemption is gone; see pairs-lettermirror.cjs); singles, necklaces and discs (no side) are never mirrored; old records without side, mirror or facing still work
//  D  a mismatched pair turns each body to its own ear; a symmetric design gives identical-looking pieces
//  E  the facing method: symmetric -> nothing to decide; a directional shape with no word is UNKNOWN (no guess); a person wins; the second body is read from the first
//  F  the nester never reflects: no mirror code in the solver, lookahead, GPU, workers; a pair placed under every rotation is still a mirror pair; a Right is never a rotation of its Left
//     (chiral); a real solve seats both with rotations only
//  G  the laser file: the mirrored ear written by the real sheet builder and read back is the exact mirror of the Left (colours kept, holes kept), at any angle
//  H  the Master tab's "faces" box (the real card code cut out of the bridge and run over fakes): shown for directional designs only, saves { facing } for every SKU, keeps a held copy in step
const assert = require('node:assert/strict'), fs = require('node:fs'), path = require('node:path');
const Pair = require('../../charm-nest-pair.js');
const L = require('./pocket-fill-lib.cjs'), S = L.solverOf(L.REPO);
const root = path.join(__dirname, '../..');
let n = 0; const ok = (c, m) => { assert.ok(c, m); n++; };
const close = (a, b, e) => Math.abs(a - b) <= (e == null ? 1e-9 : e);

/* ── fixtures: paths the way the reader gives them ── */
const poly = (pts, extra) => { const xs = pts.map(p => p[0]), ys = pts.map(p => p[1]); return Object.assign({ kind: 'path', closed: true, stroke: true, fill: false, layer: 'CUT', strokeRGB: [0, 0, 0], lwPt: .25, bbox: [Math.min(...xs), Math.min(...ys), Math.max(...xs), Math.max(...ys)], subpaths: [[['m', pts[0]]].concat(pts.slice(1).map(p => ['l', p])).concat([['l', pts[0]]])] }, extra || {}); };
const rect = (x0, y0, x1, y1, extra) => poly([[x0, y0], [x1, y0], [x1, y1], [x0, y1]], extra);
const ink = (pts, extra) => Object.assign(poly(pts, { closed: false, layer: 'ENGRAVE', strokeRGB: [1, 0, 0], lwPt: .25 }), extra || {});
const THUMB_LEFT = [[6, 0], [24, 0], [24, 30], [6, 30], [6, 28], [0, 28], [0, 18], [6, 18]];   // a mitten-like body: the thumb sticks out to the LEFT
const CHIRAL = [[0, 0], [10, 0], [10, 20], [24, 20], [24, 30], [0, 30]];   // a bar with a cap to one side: no turn of it is its own mirror image
const flipPts = (pts, w) => pts.map(p => [w - p[0], p[1]]);
const shift = (pts, dx) => pts.map(p => [p[0] + dx, p[1]]);

// the design: a mitten with a hole, a hoop on top, hatching and a text member
function design(extra) {
  const outline = poly(THUMB_LEFT, { index: 1 }), hole = rect(14, 20, 18, 24, { index: 2 }), hoop = rect(13, 30, 17, 34, { index: 3 });
  const hatch = ink([[8, 3], [22, 3], [22, 6], [8, 8]], { index: 4 }), hatch2 = ink([[9, 12], [21, 14]], { index: 5 });
  const text = { kind: 'text', index: 6, layer: 'ENGRAVE', text: 'AB', bbox: [9, 24, 19, 28], reversed: false };
  const members = [outline, hole, hoop, hatch, hatch2, text];
  return Object.assign({ id: 'D', sourceId: 's', name: 'MITTEN', outline, members, bbox: [0, 0, 24, 34], topIndices: [1, 2, 3, 4, 5, 6], extras: [], centerPt: [12, 17], strokePt: .5, upAngle: 30 }, extra || {});
}
const ptsOf = seg => seg.subpaths.flatMap(sp => sp.filter(o => o[0] !== 'h').map(o => o[o.length - 1]));
const cxOf = c => (c.bbox[0] + c.bbox[2]) / 2;
const sideWord = ps => ps.map(p => (p.side || '-') + (p.mirror ? 'm' : '')).join();
const line = (over) => Object.assign({ receiptId: '3912345678', transactionId: '4455', quantity: 1, form: 'earrings' }, over || {});

/* ── A · the Right is the exact mirror of the Left ── */
{
  const c = design(), before = JSON.stringify(c);
  const R = Pair.mirrorOf(c), cx = cxOf(c);
  ok(R !== c && R.mirrored === true && c.mirrored === undefined && JSON.stringify(c) === before, 'A1 mirrorOf makes a new charm and never touches the original');
  ok(R.members.length === c.members.length, 'A2 nothing dropped, nothing added');
  const paths = c.members.filter(m => m.kind === 'path');
  for (const m of paths) {
    const k = R.members.find(x => x.original === m);
    ok(!!k, 'A3 every path has its mirror image (index ' + m.index + ')');
    const a = ptsOf(m), b = ptsOf(k);
    ok(a.length === b.length && a.every((p, i) => close(b[i][0], 2 * cx - p[0]) && close(b[i][1], p[1])), 'A4 index ' + m.index + ': every point is x -> 2cx - x, y kept');
    ok(JSON.stringify(k.strokeRGB) === JSON.stringify(m.strokeRGB) && k.layer === m.layer && k.lwPt === m.lwPt && k.closed === m.closed && k.fill === m.fill, 'A5 colour, layer, line width and fill kept (index ' + m.index + ')');
    ok(k.synthetic === true && k.bbox.length === 4 && close(k.bbox[0], 2 * cx - m.bbox[2]) && close(k.bbox[2], 2 * cx - m.bbox[0]) && k.bbox[1] === m.bbox[1] && k.bbox[3] === m.bbox[3], 'A6 the path is written from its geometry and its box is the mirrored box (index ' + m.index + ')');
  }
  ok(R.outline === R.members.find(x => x.original === c.outline), 'A7 the outline is the mirrored outline');
  ok(R.bbox.join() === c.bbox.join(), 'A8 the charm box is the same box (mirrored about its own centre)');
  ok(R.centerPt[0] === 2 * cx - c.centerPt[0] && R.centerPt[1] === c.centerPt[1], 'A9 the centre mark mirrors');
  ok(R.upAngle === 150 && Pair.mirrorOf(Object.assign(design(), { upAngle: 90 })).upAngle === 90 && Pair.mirrorOf(Object.assign(design(), { upAngle: 0 })).upAngle === 180, 'A10 the hole direction turns about the vertical axis (30 -> 150, 90 stays up, 0 -> 180)');
  const back = Pair.mirrorOf(R);
  ok(JSON.stringify(back.outline.subpaths) === JSON.stringify(c.outline.subpaths) && back.members.filter(m => m.kind === 'path').every((m, i) => JSON.stringify(m.subpaths) === JSON.stringify(paths[i].subpaths)), 'A11 the mirror of the mirror is the original (every path)');
  // a silhouette mask flips its columns; the cut outline and its mask agree
  const bits = new Uint8Array([1, 0, 0, 0, 1, 1, 1, 1, 0]), m = Pair.mirrorOf({ bits, w: 3, h: 3 });
  ok(m.bits.join('') === '001110011' && Pair.mirrorOf(m).bits.join('') === bits.join('') && m.bits !== bits, 'A12 a mask is flipped left to right, twice is the original, the original is untouched');
  const c2 = design({ bits: new Uint8Array(12).map((_, i) => i % 5 === 0 ? 1 : 0), w: 4, h: 3 });
  ok(Pair.mirrorOf(c2).bits.join('') === Pair.mirrorOf({ bits: c2.bits, w: 4, h: 3 }).bits.join(''), 'A13 a charm that carries its mask mirrors it too');
  // pieceGeometry: the Right piece (mirror) is the same thing, cached, and the as-drawn piece is the charm itself
  const mp = Pair.pieceGeometry(c, { side: 'R', bodyIndex: 0, mirror: true }), lp = Pair.pieceGeometry(c, { side: 'L', bodyIndex: 0, mirror: false });
  ok(lp === c && mp.mirrored === true && mp !== c && Pair.pieceGeometry(c, { side: 'R', bodyIndex: 0, mirror: true }) === mp, 'A14 pieceGeometry: the Left as drawn, the Right the mirror, kept by (charm, mirror)');
  ok(ptsOf(mp.outline).every((p, i) => close(p[0], 2 * cx - ptsOf(c.outline)[i][0])), 'A15 pieceGeometry(Right) is the exact mirror of the outline');
  ok(Pair.pieceGeometry(c, { side: 'R', bodyIndex: 0, mirror: false }) === c && Pair.pieceGeometry(c, { side: 'L', bodyIndex: 0, mirror: true }) !== c, 'A16 the Left of a right-facing design is the mirrored one (the cache keeps the two apart)');
}

/* ── B · text is not reversed ── */
{
  const c = design(), R = Pair.mirrorOf(c), t = c.members.find(m => m.kind === 'text');
  ok(R.members.includes(t) && t.reversed === false && t.bbox.join() === '9,24,19,28', 'B1 a text member is the same object, as drawn, not reversed (the engraving code places text per piece; glyphs are never flipped)');
  ok(R.unmirrored && R.unmirrored.includes(t), 'B2 the mirrored charm says which members were not mirrored');
  ok(Pair.mirrorOf(Pair.mirrorOf(c)).members.includes(t), 'B3 and a mirror of the mirror still holds it');
  ok(Pair.facingOf(Pair.mirrorOf(Object.assign(design(), { facing: 'L' }))) === 'R' && Pair.facingOf(Pair.mirrorOf(Object.assign(design(), { facing: 'R' }))) === 'L', 'B4 a facing a person set turns with the mirror image');
}

/* ── C · which piece is mirrored ── */
{
  const c = design(), ear = line();
  ok(sideWord(Pair.piecesFor(ear, c)) === 'L,Rm', 'C1 unknown facing (a mitten, thumb drawn left): the Left as drawn, the Right the mirror');
  ok(sideWord(Pair.piecesFor(ear, Object.assign(design(), { facing: 'L' }))) === 'L,Rm' && sideWord(Pair.piecesFor(ear, Object.assign(design(), { facing: 'R' }))) === 'Lm,R', 'C2 a person says it faces left: Left as drawn; says right: the Left is the mirror and the Right as drawn');
  ok(sideWord(Pair.piecesFor(line({ quantity: 2 }), c)) === 'L,Rm,L,Rm', 'C3 quantity 2 = two Lefts and two Rights, every Right the mirror');
  for (const form of ['stud', 'studs', 'hoop', 'hoops', 'huggie', 'huggies', 'earrings'])
    ok(sideWord(Pair.piecesFor(line({ form }), c)) === 'L,Rm', 'C4 a pair of ' + form + ' is a Left and a mirrored Right');
  const one = Pair.piecesFor(line({ form: 'earring-single' }), c);
  ok(one.length === 1 && one[0].side === null && one[0].mirror === false, 'C5 a single earring is never mirrored');
  const neck = Pair.piecesFor({ receiptId: 1, transactionId: 2, quantity: 1, spec: { quantity: 1, pieceCount: 3, form: 'necklace' } }, c);
  ok(neck.length === 3 && neck.every(p => p.side === null && p.mirror === false), 'C6 three discs or charms on a necklace: three pieces, no side, never mirrored');
  ok(Pair.piecesFor({ receiptId: 1, transactionId: 2, quantity: 1, form: 'necklace' }, c).every(p => !p.mirror && p.side === null), 'C7 a pendant is never mirrored');
  // a design that "reads one way" is mirrored too (Paul, 10 Oct 2026, 15:42: every earring pair is a Left and an exact-mirror Right, letters, initials and numbers included)
  const X = Object.assign(design(), { facing: 'X' });
  ok(Pair.facingOf(X) === null && sideWord(Pair.piecesFor(ear, X)) === 'L,Rm', 'C8 an old facing "X" no longer stops mirroring: a Left and a mirrored Right');
  ok(sideWord(Pair.piecesFor(ear, Object.assign(design(), { sku: 'ALPHABET LETTER A' }))) === 'L,Rm' && sideWord(Pair.piecesFor(ear, Object.assign(design(), { sku: 'BIBLE SCRIPTURE (HUGGIE)' }))) === 'L,Rm', 'C9 a design named a letter or scripture is mirrored on an earring pair');
  ok(sideWord(Pair.piecesFor(ear, Object.assign(design(), { sku: 'HEART LOVE LETTER' }))) === 'L,Rm' && sideWord(Pair.piecesFor(ear, Object.assign(design(), { sku: 'PIRATE SWORD' }))) === 'L,Rm', 'C10 LOVE LETTER and SWORD are pictures, not lettering: still mirrored');
  ok(sideWord(Pair.piecesFor(ear, Object.assign(design(), { sku: 'INITIAL M', facing: 'L' }))) === 'L,Rm', 'C11 a person who says it faces left stands over the name');
  ok(Pair.readsOneWay({ sku: 'NUMBER 7' }) && !Pair.readsOneWay({ sku: 'MITTENS 1' }) && !Pair.readsOneWay(null), 'C12 readsOneWay on an entry, a plain name, nothing');
  // old records: no side, mirror, facing, form, pair field; and entries with no geometry
  const old = Pair.piecesFor({ receiptId: 11, transactionId: 22, quantity: 1 }, c);
  ok(old.length === 1 && old[0].side === null && old[0].mirror === false && old[0].groupKey === '11:22', 'C13 an old line with no form or side is one piece, no side, not mirrored');
  ok(Pair.pieceGeometry(c, {}) === c && Pair.pieceGeometry(c, undefined) === c && Pair.pieceGeometry(c, { side: null, mirror: false }) === c && Pair.pieceGeometry(null, { mirror: true }) === null, 'C14 a piece with no mirror (an old pool row) is the charm itself');
  ok(Pair.facingOf({ sku: 'OLD_ENTRY' }) === null && Pair.facingOf(null) === null && Pair.facingInfo(null, null).facing === null && Pair.needsFacing(null) === false, 'C15 an index entry with no geometry, facing or pair field answers unknown without throwing');
  ok(sideWord(Pair.piecesFor(ear, { sku: 'OLD_ENTRY' })) === 'L,Rm', 'C16 an old index entry (no outline) for a pair: the Left as drawn, the Right the mirror');
  ok(Pair.piecesFor(ear, null).length === 2 && sideWord(Pair.piecesFor(ear, null)) === 'L,Rm', 'C17 a pair line with no charm read yet is still a Left and a Right');
}

/* ── D · a mismatched pair turns each body to its own ear; a symmetric design gives identical pieces ── */
{
  const body = (pts, idx, rgb, dx) => [poly(shift(pts, dx), { index: idx }), ink([[dx + 8, 4], [dx + 22, 5]], { index: idx + 1, strokeRGB: rgb })];
  const mk = (rightPts, over) => { const a = body(THUMB_LEFT, 10, [1, 0, 0], 0), b = body(rightPts, 20, [0, 0, 1], 40); return Object.assign({ outline: a[0], members: a.concat(b), bbox: [0, 0, 64, 30], extras: [] }, over || {}); };
  const same = mk(THUMB_LEFT), mirr = mk(flipPts(THUMB_LEFT, 24));
  ok(Pair.isMismatched(same) && Pair.isMismatched(mirr), 'D1 two bodies with different engraving are a mismatched pair');
  ok(sideWord(Pair.piecesFor(line(), same)) === 'L,Rm', 'D2 both bodies drawn thumb-left (Paul\'s picture): the left body is the Left as drawn, the right body is the Right turned');
  ok(sideWord(Pair.piecesFor(line(), mirr)) === 'L,R', 'D3 the right body already drawn as the mirror image of the left: nothing is turned');
  ok(sideWord(Pair.piecesFor(line(), Object.assign(mk(THUMB_LEFT), { facing: 'R' }))) === 'Lm,R', 'D4 a person says the first body faces right and the second is drawn the same way: both face right, so the left body is turned and the right body is as drawn');
  // an index entry with no geometry (the order window asks piecesFor with the entry): the words it holds for each body decide, as the drawing would
  const entryMis = { sku: 'M', pair: { v: 1, bodies: 2, mismatched: true } };
  ok(sideWord(Pair.piecesFor(line(), entryMis)) === 'L,Rm' && sideWord(Pair.piecesFor(line(), Object.assign({ facings: [null, 'R'] }, entryMis))) === 'L,R' && sideWord(Pair.piecesFor(line(), Object.assign({ facings: ['R', 'R'] }, entryMis))) === 'Lm,R' && sideWord(Pair.piecesFor(line(), Object.assign({ facing: 'R' }, entryMis))) === 'Lm,Rm', 'D3b a mismatched entry with no drawing: unknown = Left as drawn, Right turned; the words for each body (facings, or facing for body 0) decide');
  const gL = Pair.pieceGeometry(same, { side: 'L', bodyIndex: 0, mirror: false }), gR = Pair.pieceGeometry(same, { side: 'R', bodyIndex: 1, mirror: true });
  ok(gL.bbox.join() === '0,0,24,30' && gR.bbox.join() === '40,0,64,30', 'D5 each piece is its own body (not the pair)');
  const rx = ptsOf(gR.outline), lx = ptsOf(gL.outline);
  ok(rx.length === lx.length && rx.every((p, i) => close(p[0] - 40, 24 - lx[i][0]) && close(p[1], lx[i][1])), 'D6 the Right body is the mirror of the Left body about its own middle');
  const gR2 = Pair.pieceGeometry(mirr, { side: 'R', bodyIndex: 1, mirror: false });
  ok(ptsOf(gR2.outline).every((p, i) => close(p[0] - 40, 24 - lx[i][0])), 'D7 a Right body that is drawn as the mirror image is used as drawn (and equals the mirror of the Left)');
  // a symmetric design: the two pieces look the same
  const sym = { id: 'S', outline: null, members: [], bbox: [0, 0, 24, 30], extras: [] };
  const so = poly([[2, 0], [22, 0], [22, 30], [2, 30]], { index: 1 }), sh = rect(10, 20, 14, 24, { index: 2 });
  Object.assign(sym, { outline: so, members: [so, sh, ink([[6, 4], [18, 4]], { index: 3 }), ink([[6, 8], [18, 8]], { index: 4 })], bbox: [2, 0, 22, 30] });
  const si = Pair.facingInfo(Pair.bodiesOf(sym)[0], sym);
  ok(si.directional === false && si.level === 'symmetric' && si.facing === null && !Pair.needsFacing(sym), 'D8 a symmetric design is not directional and needs nobody to say anything');
  const IM = require('../../scripts/index-master.cjs');
  ok(IM.symLevel(Pair, sym) === 'symmetric' && IM.symLevel(Pair, design()) === 'directional' && IM.symLevel(null, sym) === undefined && IM.symLevel(Pair, {}) === undefined, 'D8b the indexer stores sym: symmetric / directional from the first body, nothing when it cannot say');
  const sl = Pair.pieceGeometry(sym, { side: 'L', mirror: false }), sr = Pair.pieceGeometry(sym, { side: 'R', mirror: true });
  const key = g => g.members.filter(m => m.kind === 'path').map(m => [...new Set(ptsOf(m).map(p => p.map(v => v.toFixed(6)).join(',')))].sort().join(';')).sort().join('|');
  ok(key(sl) === key(sr), 'D9 the Right of a symmetric design looks the same as its Left (the same points)');
}

/* ── E · the facing method ── */
{
  const c = design(), b0 = Pair.bodiesOf(c)[0], info = Pair.facingInfo(b0, c);
  ok(info.directional === true && info.facing === null && info.source === 'unknown' && info.confidence === 0 && Pair.needsFacing(c) === true, 'E1 a mitten is directional, and nothing in a shape says which way it faces: unknown, confidence 0, a person is asked');
  ok(Pair.facingInfo(b0, Object.assign(design(), { facing: 'R' })).facing === 'R' && Pair.facingInfo(b0, Object.assign(design(), { facing: 'R' })).source === 'person', 'E2 a person\'s word wins and says so');
  ok(Pair.needsFacing(Object.assign(design(), { facing: 'L' })) === false, 'E3 once a person has said, nothing is left to ask');
  const sy = Pair.symmetryOf(b0);
  ok(sy.level === 'directional' && sy.cut >= 0.065 && Pair.symmetryOf(b0) === sy, 'E4 the symmetry measure is read from the shape and cached by body: ' + JSON.stringify(sy));
  const bump = poly([[0, 0], [24, 0], [24, 30], [8, 30], [8, 34], [2, 34], [2, 30], [0, 30]], { index: 1 });   // a hoop welded on off to one side
  const wc = { outline: bump, members: [bump], bbox: [0, 0, 24, 34], extras: [] };
  ok(['slight', 'directional'].includes(Pair.symmetryOf(Pair.bodiesOf(wc)[0]).level), 'E5 a body with its hoop off to one side is not called symmetric: ' + JSON.stringify(Pair.symmetryOf(Pair.bodiesOf(wc)[0])));
  // the second body of a mismatched pair is read from the first
  const body = (pts, idx, rgb, dx) => [poly(shift(pts, dx), { index: idx }), ink([[dx + 8, 4], [dx + 22, 5]], { index: idx + 1, strokeRGB: rgb })];
  const mk = (rightPts, over) => { const a = body(THUMB_LEFT, 10, [1, 0, 0], 0), b = body(rightPts, 20, [0, 0, 1], 40); return Object.assign({ outline: a[0], members: a.concat(b), bbox: [0, 0, 64, 30], extras: [] }, over || {}); };
  const mirr = mk(flipPts(THUMB_LEFT, 24)), alike = mk(THUMB_LEFT);
  ok(Pair.facingInfo(Pair.bodiesOf(mirr)[1], mirr).facing === 'R' && Pair.facingInfo(Pair.bodiesOf(mirr)[1], mirr).source === 'mirror-of-first', 'E6 a body drawn as the mirror image of the first faces the other way');
  // a mirror pair drawn a little differently (the live HEART_2206 pair overlaps its mirror image by 0.88 and its own drawing by 0.65) is still read as a mirror pair
  const nearMirror = mk(flipPts([[6, 0], [24, 0], [24, 30], [6, 30], [6, 24], [1, 24], [1, 21], [6, 21]], 24)), farMirror = mk(flipPts([[6, 0], [24, 0], [24, 30], [6, 30], [6, 25], [3, 25], [3, 20], [6, 20]], 24));
  const simNear = Pair.shapeSimilarity(Pair.bodiesOf(nearMirror)[0], Pair.bodiesOf(nearMirror)[1]);
  ok(simNear.mirrored < Pair.PAIR_DEFAULTS.sameShapeIoU && simNear.mirrored >= .85 && simNear.same < .7 && Pair.facingOfBody(Pair.bodiesOf(nearMirror)[1], nearMirror) === 'R', 'E6b a pair drawn as near mirror images (overlap 0.88 against 0.65) is read as a mirror pair: ' + JSON.stringify(simNear));
  ok(Pair.facingOfBody(Pair.bodiesOf(farMirror)[1], farMirror) === null, 'E6c bodies that are only loosely alike are not guessed at');
  ok(Pair.facingOfBody(Pair.bodiesOf(alike)[1], alike) === null && Pair.facingOfBody(Pair.bodiesOf(Object.assign(mk(THUMB_LEFT), { facing: 'R' }))[1], Object.assign(mk(THUMB_LEFT), { facing: 'R' })) === 'R', 'E7 drawn the same way as the first: unknown, or the first\'s way once a person said it');
  ok(Pair.facingOfBody(Pair.bodiesOf(alike)[0], Object.assign({}, alike, { facings: ['R', 'L'] })) === 'R' && Pair.facingOfBody(Pair.bodiesOf(alike)[1], Object.assign({}, alike, { facings: ['R', 'L'] })) === 'L', 'E8 facings[i] says each body\'s way');
}

/* ── F · the nester never reflects ── */
{
  // F1 no mirror/reflect code anywhere in what places pieces
  const code = f => fs.readFileSync(path.join(root, f), 'utf8').split('\n').filter(l => !/^\s*(\/\/|\*|\/\*)/.test(l)).join('\n').replace(/\/\*[\s\S]*?\*\//g, '');
  for (const f of ['charm-nest-solver.js', 'charm-nest-lookahead.js', 'charm-nest-gpu.js', 'charm-nest-worker.js', 'charm-nest-partial-nest.js'])
    if (fs.existsSync(path.join(root, f))) {
      const src = code(f);
      ok(!/\bflipMask\b|\bmirrorOf\b|\breflect\w*\b|\bmirror(?:ed)?\b\s*[:=(]|\bflip\w*\s*[:=(]|scale\(\s*-1|\[\s*-1\s*,\s*0\s*,\s*0\s*,\s*1\s*\]/i.test(src), 'F1 ' + f + ' has no code that mirrors or reflects a piece');
    }
  ok(/never mirrored/i.test(fs.readFileSync(path.join(root, 'charm-nest-solver.js'), 'utf8')), 'F2 the solver\'s own header says mirroring is never applied');

  // a chiral shape on the solver's grid, 4 px per pt
  const K = 4, rast = (pts, scale) => { const xs = pts.map(p => p[0]), ys = pts.map(p => p[1]), W = Math.ceil(Math.max(...xs) * scale), H = Math.ceil(Math.max(...ys) * scale), bits = new Uint8Array(W * H);
    for (let y = 0; y < H; y++) for (let x = 0; x < W; x++) { const px = (x + .5) / scale, py = (y + .5) / scale; let ins = false; for (let i = 0, j = pts.length - 1; i < pts.length; j = i++) { const [xi, yi] = pts[i], [xj, yj] = pts[j]; if ((yi > py) !== (yj > py) && px < (xj - xi) * (py - yi) / (yj - yi) + xi) ins = !ins; } bits[y * W + x] = ins ? 1 : 0; }
    return { bits, w: W, h: H }; };
  const mit = rast(CHIRAL, K), mitR = Pair.mirrorOf({ bits: mit.bits, w: mit.w, h: mit.h });
  const piece = (id, r) => { let a = 0; for (const v of r.bits) a += v; return { id, w: r.w, h: r.h, scale: K, bits: r.bits, areaPt2: a / (K * K), widthPt: r.w / K, heightPt: r.h / K, order: 'O', orderDate: 0 }; };
  const Lp = piece('L', mit), Rp = piece('R', mitR);
  // image of a bitmap centred on its middle in a common frame
  const FR = 260, framed = (bits, w, h, cx, cy) => { const o = new Uint8Array(FR * FR), ox = Math.round(FR / 2 - cx), oy = Math.round(FR / 2 - cy); for (let y = 0; y < h; y++) for (let x = 0; x < w; x++) if (bits[y * w + x]) { const X = x + ox, Y = y + oy; if (X >= 0 && Y >= 0 && X < FR && Y < FR) o[Y * FR + X] = 1; } return o; };
  const iou = (a, b) => { let i = 0, u = 0; for (let k = 0; k < a.length; k++) { if (a[k] & b[k]) i++; if (a[k] | b[k]) u++; } return u ? i / u : 0; };
  const placed = (p, deg) => { const r = S.rotateBitmap(p.bits, p.w, p.h, deg); return framed(r.bits, r.w, r.h, r.cx, r.cy); };
  const flipFrame = f => { const o = new Uint8Array(FR * FR); for (let y = 0; y < FR; y++) for (let x = 0; x < FR; x++) o[y * FR + x] = f[y * FR + FR - 1 - x]; return o; };
  // F3 the Right is the Left's mirror image, not a rotation of it: no angle makes them alike (the shape is chiral)
  let bestRot = 0; for (let a = 0; a < 360; a += 5) bestRot = Math.max(bestRot, iou(placed(Lp, a), placed(Rp, 0)));
  ok(iou(placed(Lp, 0), flipFrame(placed(Rp, 0))) > .95, 'F3 the Right is the mirror image of the Left');
  ok(bestRot < .75, 'F4 and no rotation of the Left makes it the Right (chiral: if a piece was ever reflected it would, wrongly, be found congruent): best ' + bestRot.toFixed(3));
  // F5 a pair placed under every rotation stays a mirror pair: Right turned by a is the mirror of Left turned by -a
  let worst = 1; for (let a = 0; a < 360; a += 15) worst = Math.min(worst, iou(flipFrame(placed(Lp, a)), placed(Rp, (360 - a) % 360)));
  ok(worst > .93, 'F5 placed at every angle (15 degree steps) the Right is still the mirror of the Left turned the other way: worst ' + worst.toFixed(3));
  // F6 a placed piece is a ROTATION of its own shape: the solver's turned bitmap equals the same outline turned by the same angle (clockwise on screen) and drawn again, for the Left and for the Right
  const inPoly = (pts, px, py) => { let ins = false; for (let i = 0, j = pts.length - 1; i < pts.length; j = i++) { const [xi, yi] = pts[i], [xj, yj] = pts[j]; if ((yi > py) !== (yj > py) && px < (xj - xi) * (py - yi) / (yj - yi) + xi) ins = !ins; } return ins; };
  const turnedOutline = (pts, w, h, deg) => { const t = deg * Math.PI / 180, c = Math.cos(t), s = Math.sin(t), o = new Uint8Array(FR * FR); for (let Y = 0; Y < FR; Y++) for (let X = 0; X < FR; X++) { const u = X + .5 - FR / 2, v = Y + .5 - FR / 2, sx = c * u + s * v + w / 2, sy = -s * u + c * v + h / 2; if (inPoly(pts, sx / K, sy / K)) o[Y * FR + X] = 1; } return o; };
  let agree = 1; for (const [p, pts] of [[Lp, CHIRAL], [Rp, flipPts(CHIRAL, 24)]]) for (let a = 0; a < 360; a += 30) agree = Math.min(agree, iou(placed(p, a), turnedOutline(pts, p.w, p.h, a)));
  ok(agree > .93, 'F6 the solver\'s turned bitmap is the outline turned by the same angle, for the Left and the Right (a rotation, never a reflection): ' + agree.toFixed(3));
}
(async () => {
  // F7 a real solve with both ears and other charms: only rotations, nothing overlaps, the Right comes out as a rotated mirror image
  const K = 4, sq = (id, w, h) => ({ id, w: w * K, h: h * K, scale: K, bits: new Uint8Array(w * K * h * K).fill(1), areaPt2: w * h, widthPt: w, heightPt: h, order: id, orderDate: 1 });
  const rast = (pts) => { const xs = pts.map(p => p[0]), ys = pts.map(p => p[1]), W = Math.ceil(Math.max(...xs) * K), H = Math.ceil(Math.max(...ys) * K), bits = new Uint8Array(W * H);
    for (let y = 0; y < H; y++) for (let x = 0; x < W; x++) { const px = (x + .5) / K, py = (y + .5) / K; let ins = false; for (let i = 0, j = pts.length - 1; i < pts.length; j = i++) { const [xi, yi] = pts[i], [xj, yj] = pts[j]; if ((yi > py) !== (yj > py) && px < (xj - xi) * (py - yi) / (yj - yi) + xi) ins = !ins; } bits[y * W + x] = ins ? 1 : 0; }
    return { bits, w: W, h: H }; };
  const m = rast(CHIRAL), mr = Pair.mirrorOf({ bits: m.bits, w: m.w, h: m.h });
  const mp = (id, r) => { let a = 0; for (const v of r.bits) a += v; return { id, w: r.w, h: r.h, scale: K, bits: r.bits, areaPt2: a / (K * K), widthPt: r.w / K, heightPt: r.h / K, order: 'O1', orderDate: 1 }; };
  const angles = [0, 30, 60, 90, 120, 150, 180, 210, 240, 270, 300, 330];
  const job = { sheet: { wPt: 90, hPt: 70, insetPt: 1 }, angles, fineRes: 2, coarseRes: .5, maxFill: 1, timeBudgetMs: 8000, maxTrials: 30, clearancePt: 0, seed: 7, careful: true, pieces: [mp('EAR.L', m), mp('EAR.R', mr), sq('S1', 14, 10), sq('S2', 10, 10), mp('EAR2.L', m), mp('EAR2.R', mr)] };
  const r = await S.solve(job, {});
  const ids = r.placements.map(p => p.id);
  ok(['EAR.L', 'EAR.R', 'EAR2.L', 'EAR2.R'].every(i => ids.includes(i)), 'F7 the solver seats both ears of both pairs: ' + ids.join());
  ok(S.verify(job, r.placements, 4).ok, 'F8 no overlap');
  ok(r.placements.every(p => angles.includes(((p.angle % 360) + 360) % 360)), 'F9 every angle used is one of the rotations offered (no other transform exists in a placement): ' + r.placements.map(p => p.angle).join());
  ok(r.placements.every(p => !('mirror' in p) && !('flip' in p) && !('reflect' in p) && !('scaleX' in p)), 'F10 a placement carries only an angle and a position: nothing that could reflect');
  // the piece a placement names is exactly the piece that was sent: a placed Right is the Right (its bits are not swapped for the Left's)
  const byId = new Map(job.pieces.map(p => [p.id, p]));
  ok(r.placements.every(p => byId.has(p.id)) && byId.get('EAR.R').bits.join('') !== byId.get('EAR.L').bits.join(''), 'F11 the Left and the Right are different bitmaps all the way through the solve (Left and Right never share a shape)');
  ok(S.shapeKey(byId.get('EAR.L')) !== S.shapeKey(byId.get('EAR.R')), 'F12 the solver caches shapes by piece: the Left\'s rotations are never served for the Right');

  /* ── G · the laser file ── */
  const g = globalThis; if (!g.window) g.window = g; g.self = g; g.PDFLib = require('../../vendor/pdf-lib-1.17.1.min.js');
  require('../../charm-nest-pdf.js'); const PDF = g.CharmNestPDF; const Ex = require('../../charm-nest-export.js');
  async function vector(name, w, h, ops) { const Lb = g.PDFLib, d = await Lb.PDFDocument.create(), p = d.addPage([w, h]); p.node.normalize(); const ref = d.context.register(d.context.obj({ Type: 'OCG', Name: Lb.PDFString.of(name) })); p.node.Resources().set(Lb.PDFName.of('Properties'), d.context.obj({ art: ref })); p.node.addContentStream(d.context.register(d.context.flateStream('/OC /art BDC ' + ops + ' EMC'))); d.catalog.set(Lb.PDFName.of('OCProperties'), d.context.obj({ OCGs: [ref], D: { Order: [ref], ON: [ref] } })); return d.save({ useObjectStreams: false }); }
  const pathOps = (pts, close) => pts.map((p, i) => p[0] + ' ' + p[1] + (i ? ' l' : ' m')).join(' ') + (close ? ' h' : '');
  // a mitten: black cut line, a black hole, red engraving lines, a blue one (colours must survive)
  const ops = ['0 0 0 RG 0.25 w', pathOps(THUMB_LEFT, true) + ' S', pathOps([[14, 20], [18, 20], [18, 24], [14, 24]], true) + ' S', '1 0 0 RG', pathOps([[8, 3], [22, 3], [22, 6]], false) + ' S', pathOps([[9, 12], [21, 14]], false) + ' S', '0 0 1 RG', pathOps([[10, 8], [20, 9]], false) + ' S'].join('\n');
  const src = await PDF.parseSource(await vector('Artwork', 40, 40, ops), 'mitten');
  const grp = PDF.groupCharms(src); ok(grp.charms.length === 1, 'G1 the reader sees one charm in the master: ' + grp.charms.length);
  const left = Object.assign(grp.charms[0], { id: 'e1', sourceId: 's', name: '4190000001 · MITTEN · 1/2', side: 'L', centerPt: [12, 15], strokePt: .25 });
  const right = Object.assign(Pair.pieceGeometry(left, { side: 'R', bodyIndex: 0, mirror: true }) , {});
  const Rch = Object.assign({}, right, { id: 'e2', name: '4190000001 · MITTEN · 2/2', side: 'R' });
  ok(Rch.mirrored === true && Rch.members.every(x => x.kind !== 'path' || x.synthetic === true), 'G2 the Right is the charm made of mirrored paths written from geometry');
  const place = (c, x, y, angle) => ({ charm: c, angle: angle || 0, cxPt: x, cyPt: y, scale: 1 });
  const cuts = async (placements) => {
    const bytes = await PDF.buildSheet({ sheet: { wPt: 300, hPt: 200 }, sources: new Map([['s', src]]), placements, title: 't' });
    const sheet = await PDF.parseSource(bytes, 'sheet'), leaves = Ex.productionPaths(sheet).filter(p => p.layer !== 'SHEET (do not cut)');
    return leaves;
  };
  const rot = (p, deg, cx, cy) => { const a = deg * Math.PI / 180, c = Math.cos(a), s = Math.sin(a), dx = p[0] - cx, dy = p[1] - cy; return [cx + dx * c - dy * s, cy + dx * s + dy * c]; };
  const keyOf = (leaf) => (leaf.strokeRGB || []).map(v => Math.round(v)).join('') + '|' + (leaf.closed ? 'c' : 'o') + '|' + leaf.subpaths.length;
  const pointsOf = leaf => ptsOf(leaf);
  // Left at angle 0 centred 80,100; Right (mirror) at angle 0 centred 200,100
  const leaves0 = await cuts([place(left, 80, 100, 0), place(Rch, 200, 100, 0)]);
  const half = (leaves, cx) => leaves.filter(l => Math.abs((l.bbox[0] + l.bbox[2]) / 2 - cx) < 40);
  const lefts = half(leaves0, 80), rights = half(leaves0, 200);
  ok(lefts.length === rights.length && lefts.length >= 4, 'G3 the Left and the Right ear have the same parts (outline, hole, engraving lines): ' + lefts.length + ' and ' + rights.length);
  // the Left's centre in the sheet is (80,100); its drawing centre is the middle of the box [0..24] x [0..30] = (12,15): a point p of the drawing lands at (80 + p.x - 12, 100 + p.y - 15) (y may flip with the sheet's axis: compare through one fixed frame)
  const frameOf = (leaves) => { const xs = leaves.flatMap(l => [l.bbox[0], l.bbox[2]]), ys = leaves.flatMap(l => [l.bbox[1], l.bbox[3]]); return [(Math.min(...xs) + Math.max(...xs)) / 2, (Math.min(...ys) + Math.max(...ys)) / 2]; };
  const fl = frameOf(lefts), fr = frameOf(rights);
  const norm = (leaves, f, mirror) => leaves.map(l => ({ key: keyOf(l), pts: pointsOf(l).map(p => [(mirror ? -1 : 1) * (p[0] - f[0]), p[1] - f[1]]) }));
  const A = norm(lefts, fl, true), B = norm(rights, fr, false);   // the Left mirrored in its own frame must be the Right
  const near = (pa, pb) => { let worstD = 0; for (const p of pa) { let best = Infinity; for (const q of pb) best = Math.min(best, Math.hypot(p[0] - q[0], p[1] - q[1])); worstD = Math.max(worstD, best); } return worstD; };
  let hd = 0, matched = 0;
  for (const a of A) { const b = B.find(x => x.key === a.key && x.pts.length === a.pts.length && near(a.pts, x.pts) < 1e-3); if (b) matched++; hd = Math.max(hd, Math.min(...B.filter(x => x.key === a.key).map(x => Math.max(near(a.pts, x.pts), near(x.pts, a.pts))))); }
  ok(matched === A.length, 'G4 each part of the written Right is the exact mirror image of the Left\'s part, with the same colour (matched ' + matched + ' of ' + A.length + ', Hausdorff ' + hd.toExponential(1) + ')');
  ok(new Set(lefts.map(keyOf)).size >= 3 && lefts.map(keyOf).sort().join() === rights.map(keyOf).sort().join(), 'G5 black cut line and hole, red and blue engraving survive in the Right');
  const outlineOf = ls => ls.slice().sort((a, b) => (b.bbox[2] - b.bbox[0]) * (b.bbox[3] - b.bbox[1]) - (a.bbox[2] - a.bbox[0]) * (a.bbox[3] - a.bbox[1]))[0];
  const thumbSide = leaf => {   // the mass of a mitten lies away from its thumb: the shoelace centroid against the middle of the box
    const q = ptsOf(leaf); let A = 0, Cx = 0; for (let i = 0; i < q.length; i++) { const [x0, y0] = q[i], [x1, y1] = q[(i + 1) % q.length], k = x0 * y1 - x1 * y0; A += k; Cx += (x0 + x1) * k; }
    return Cx / (3 * A) > (leaf.bbox[0] + leaf.bbox[2]) / 2 ? 'L' : 'R'; };
  ok(thumbSide(outlineOf(lefts)) === 'L' && thumbSide(outlineOf(rights)) === 'R', 'G6 the thumb is on the left of the Left ear and on the right of the Right ear in the file');
  // turned by an angle: the Right turned by 37 is the Left turned by -37, mirrored
  const t1 = await cuts([place(left, 80, 100, 37), place(Rch, 200, 100, -37)]);
  const l37 = half(t1, 80), r37 = half(t1, 200), f1 = frameOf(l37), f2 = frameOf(r37);
  const A2 = norm(l37, f1, true), B2 = norm(r37, f2, false);
  const hd2 = Math.max(...A2.map(a => { const b = B2.filter(x => x.key === a.key); return b.length ? Math.min(...b.map(x => Math.max(near(a.pts, x.pts), near(x.pts, a.pts)))) : Infinity; }));
  ok(hd2 < 0.05, 'G7 turned by 37 degrees one way and -37 the other, the two ears are still mirror images: ' + hd2.toExponential(1));
  // a Right turned by the SAME angle as the Left is not a rotated Left: the ears are never congruent by a turn
  const t2 = await cuts([place(left, 80, 100, 37), place(Rch, 200, 100, 37)]);
  const l2 = half(t2, 80), r2 = half(t2, 200);
  const outL = outlineOf(l2), outR = outlineOf(r2), cL = frameOf([outL]), cR = frameOf([outR]);
  let bestTurn = Infinity; for (let a = 0; a < 360; a += 1) { const pa = ptsOf(outL).map(p => rot([p[0] - cL[0], p[1] - cL[1]], a, 0, 0)); bestTurn = Math.min(bestTurn, near(pa, ptsOf(outR).map(p => [p[0] - cR[0], p[1] - cR[1]]))); }
  ok(bestTurn > 0.5, 'G8 no turn of the Left\'s cut line lands on the Right\'s: the ears are mirror images, not copies (closest ' + bestTurn.toFixed(2) + ' pt)');
  // an as-drawn charm is written as before
  const plain = await cuts([place(Object.assign({}, left, { side: undefined }), 80, 100, 0)]);
  ok(plain.length === lefts.length && plain.every(l => !l.subpaths.some(sp => sp.length === 0)), 'G9 the as-drawn piece is written with the same parts as ever');

  // G10 a REAL per-SKU file keeps the master's layers (CUT, ENGRAVE, HATCH) in the form that holds the charm: the mirrored ear goes back into the same layers, part for part
  {
    async function layered(w, h, layers) { const Lb = g.PDFLib, d = await Lb.PDFDocument.create(), p = d.addPage([w, h]); p.node.normalize(); const props = {}, refs = []; let o = ''; layers.forEach(([name, ops2], i) => { const ref = d.context.register(d.context.obj({ Type: 'OCG', Name: Lb.PDFString.of(name) })); refs.push(ref); props['L' + i] = ref; o += '/OC /L' + i + ' BDC ' + ops2 + ' EMC\n'; }); p.node.Resources().set(Lb.PDFName.of('Properties'), d.context.obj(props)); p.node.addContentStream(d.context.register(d.context.flateStream(o))); d.catalog.set(Lb.PDFName.of('OCProperties'), d.context.obj({ OCGs: refs, D: { Order: refs, ON: refs } })); return d.save({ useObjectStreams: false }); }
    const master = await PDF.parseSource(await layered(40, 40, [['CUT', '0 0 0 RG 0.25 w ' + pathOps(THUMB_LEFT, true) + ' S ' + pathOps([[14, 20], [18, 20], [18, 24], [14, 24]], true) + ' S'], ['ENGRAVE', '1 0 0 RG 0.25 w ' + pathOps([[8, 3], [22, 3], [22, 6]], false) + ' S'], ['HATCH', '0 0 1 rg ' + pathOps([[9, 10], [20, 10], [20, 14], [9, 14]], true) + ' f']]), 'master');
    const c0 = PDF.groupCharms(master).charms[0]; c0.name = 'MITTEN';
    const per = await PDF.parseSource(await PDF.buildSingleCharm(c0, master), 'perSku'), c = PDF.groupCharms(per).charms[0];
    const bb = c.bbox, w = bb[2] - bb[0], h = bb[3] - bb[1];
    Object.assign(c, { id: 'a', sourceId: 's', name: 'DESIGN', side: 'L', centerPt: [(bb[0] + bb[2]) / 2, (bb[1] + bb[3]) / 2], strokePt: .5 });
    const R2 = Object.assign({}, Pair.pieceGeometry(c, { side: 'R', bodyIndex: 0, mirror: true }), { id: 'b', sourceId: 's', name: 'DESIGN', side: 'R' });
    const W = 2 * w + 80, H = h + 60, bytes = await PDF.buildSheet({ sheet: { wPt: W, hPt: H }, sources: new Map([['s', per]]), placements: [place(c, w / 2 + 20, H / 2, 0), place(R2, w * 1.5 + 60, H / 2, 0)], title: 't' });
    const sheet = await PDF.parseSource(bytes, 'sheet'), leaves = Ex.leaves(sheet).filter(l => !/^SHEET/.test(l.layer || '')), split = (w / 2 + 20 + w * 1.5 + 60) / 2, mid = l => (l.bbox[0] + l.bbox[2]) / 2;
    const lay = ls => ls.map(l => l.layer).sort().join();
    ok(lay(leaves.filter(l => mid(l) < split)) === 'CUT,CUT,ENGRAVE,HATCH' && lay(leaves.filter(l => mid(l) >= split)) === 'CUT,CUT,ENGRAVE,HATCH', 'G10 the Right ear is in the master\'s own layers (cut line and hole on CUT, engraving on ENGRAVE, hatching on HATCH), like the Left: ' + lay(leaves.filter(l => mid(l) >= split)));
    const dx = Ex.dxf(Ex.productionPaths(sheet), Ex.layerNames(sheet)).text;
    ok(!/DESIGN Right/.test(dx.split('ENTITIES')[1] || ''), 'G11 and the .dxf puts no part of it on a layer of its own');
  }
  // G12 a cut line chained from open strokes is a synthetic outline that is only geometry: the mirrored ear has the same parts, and the laser does not cut it twice
  {
    const a = ink([[0, 0], [10, 0], [10, 20]], { index: 1, layer: 'CUT', strokeRGB: [0, 0, 0] }), b2 = ink([[10, 20], [0, 20], [0, 0]], { index: 2, layer: 'CUT', strokeRGB: [0, 0, 0] });
    const chained = Object.assign(poly([[0, 0], [10, 0], [10, 20], [0, 20]], { index: 3, synthetic: true }), { parts: [a, b2] });
    const ch = { id: 'c', outline: chained, members: [a, b2], bbox: [0, 0, 10, 20], topIndices: [1, 2], extras: [] }, m2 = Pair.mirrorOf(ch);
    ok(m2.members.length === 2 && !m2.members.includes(m2.outline) && m2.outline.parts.length === 2 && m2.outline.parts.every(q => m2.members.includes(q)), 'G12 a chained outline stays geometry only in the mirrored charm (its parts are the mirrored members, the outline is not written a second time)');
    const loose = poly([[0, 0], [10, 0], [10, 20], [0, 20]], { index: 5 }), lc = { id: 'd', outline: loose, members: [], bbox: [0, 0, 10, 20], topIndices: [5], extras: [] };
    ok(Pair.mirrorOf(lc).members.length === 1, 'G13 an outline that is a path of its own and is not among the members is still written (as before)');
  }

  /* ── H · the Master tab's "faces" box (the real code of charm-nest-bridge.js, cut out and run over fakes) ── */
  {
    const ctl = Pair.facingControl;
    ok(ctl({ sku: 'A', sym: 'directional' }).show && ctl({ sku: 'A', sym: 'directional' }).value === '' && !ctl({ sku: 'A', sym: 'symmetric' }).show && !ctl({ sku: 'A', sym: 'slight' }).show && !ctl({ sku: 'A' }).show, 'H1 the box is shown for a directional design only (a symmetric one or one not measured yet shows nothing)');
    ok(ctl({ sku: 'A', sym: 'symmetric', facing: 'R' }).show && ctl({ sku: 'A', facing: 'X' }).value === 'X' && ctl({ sku: 'A' }, 'directional').show && ctl(null).show === false, 'H2 a word a person set stays visible and can be changed; the page may bring its own measure; nothing at all shows nothing');
    ok(ctl({ sku: 'A', facing: 'sideways', sym: 'directional' }).value === '' && ctl({}).options.map(o => o[0]).join() === ',L,R' && ctl({ sku: 'A', facing: 'X' }).options.map(o => o[0]).join() === ',L,R,X', 'H3 three choices (not set, left, right: "reads one way" is not offered any more, it would stop nothing); a record that still holds X shows it so it can be cleared; junk is "not set"');
    ok(ctl({ sku: 'INITIAL LETTER A', sym: 'directional' }).options[0][1] === 'faces: not set' && ctl({ sku: 'INITIAL LETTER A', sym: 'directional', facing: 'L' }).options[0][1] === 'faces: not set' && ctl({ sku: 'MITTEN', sym: 'directional' }).options[0][1] === 'faces: not set' && /no longer used/.test(ctl({ sku: 'A', facing: 'X' }).options[3][1]) && !/reads one way|cut as drawn/.test(ctl({ sku: 'A', sym: 'directional' }).hint), 'H3b a lettering SKU is a design like any other (not set); the leftover X says it is no longer used; the explanation no longer promises a design is cut as drawn');
    const bridge = fs.readFileSync(path.join(root, 'charm-nest-bridge.js'), 'utf8');
    const a = bridge.indexOf('    const heldSym = e =>'), b = bridge.indexOf('    grid.innerHTML = shown.map(d => {', a);
    ok(a > 0 && b > a, 'H4 the Master card code is where the test expects it');
    const esc = x => String(x).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/"/g, '&quot;');
    const entries = { A: { sku: 'A', sizes: null, sym: 'directional', facing: 'R' }, B: { sku: 'B', sym: 'symmetric' }, C: { sku: 'C', sizes: { S: {}, L: {} } }, D: { sku: 'D', sym: 'directional' } };
    const holder = { charms: [Object.assign(design(), { facing: 'R' })] }; holder.bodies = [Object.assign(design(), { facing: 'R' })];
    const mk = new Function('window', 'Pool', 'B', 'esc', 'entryFor', bridge.slice(a, b) + '\nreturn { heldSym, facingLive, facesBox };');
    const fakePool = { sizeEntry: (e, size) => ({ aiPath: 'path' + e.sku + (size || '') }) };
    const Bf = { pool: { sources: new Map([['pathA', holder], ['pathCS', holder], ['pathCL', holder], ['pathE', { charms: [design()] }]]) } };
    const M = mk({ CharmNestPair: Pair }, fakePool, Bf, esc, sku => entries[sku]);
    const html = M.facesBox(entries.D, 'D'), none = M.facesBox(entries.B, 'B'), set = M.facesBox(entries.A, 'A|A2');
    ok(/^<select data-faces="D"/.test(html) && /<option value="" selected>faces: not set<\/option>/.test(html) && /value="L">faces left/.test(html) && /value="R">faces right/.test(html) && !/value="X"/.test(html) && /title="Which way the master file draws/.test(html), 'H5 a directional design gets the box with its three words (no "reads one way") and its explanation: ' + html.slice(0, 120));
    ok(none === '' && /<option value="R" selected>/.test(set) && /data-faces="A\|A2"/.test(set), 'H6 a symmetric design gets nothing, one a person set shows the word (the box saves for every SKU of the charm)');
    ok(M.facesBox({ sku: 'E' }, 'E') !== '' && M.facesBox({ sku: 'Q' }, 'Q') === '', 'H7 a design with no measure yet is judged from the copy the page already holds (a mitten is directional), and shown nothing when there is none');
    // the handler: saves through patchMany for every SKU, tells the held copy, says "from now on"; a failure puts the old word back and says so
    const hs = bridge.indexOf('    each("faces"'), he = bridge.indexOf('    each("unblock"', hs), line = bridge.slice(hs, he);
    ok(/patchMany\(skus, patch\)/.test(line) && /\{ facing: v2 === "" \? null : v2 \}/.test(line) && /applies to orders made up from now on/.test(line), 'H8 the box saves { facing } (null gives the word back) for every SKU and says it applies from now on');
    const run = async (patchMany) => {
      const toasts = [], sel = { value: 'L', disabled: false, dataset: {} }, fake = { each: (attr, fn) => fn(sel, ['A', 'A2']), patchMany, toast: (m, k) => toasts.push([m, k]), facingLive: (...a) => { toasts.live = a; } };
      new Function('each', 'patchMany', 'toast', 'facingLive', line)(fake.each, fake.patchMany, fake.toast, fake.facingLive);
      sel.value = 'R'; sel.onchange(); await new Promise(r => setTimeout(r, 5)); return { sel, toasts };
    };
    const calls = []; const good = await run(async (skus, p) => { calls.push([skus.slice(), p]); });
    ok(calls.length === 1 && calls[0][0].join() === 'A,A2' && calls[0][1].facing === 'R' && good.toasts[0][1] === 'ok' && /faces right/.test(good.toasts[0][0]) && good.toasts.live[1].facing === 'R' && good.sel.disabled === false, 'H9 choosing "faces right" saves { facing: "R" } for both SKUs and says so');
    const bad = await run(async () => { throw new Error('offline'); });
    ok(bad.toasts[0][1] === 'bad' && /not saved — offline/.test(bad.toasts[0][0]) && bad.sel.value === 'L' && bad.sel.disabled === false, 'H10 a save that fails says so and puts the old word back');
    // a copy the page already holds hears the change; one it does not hold is left alone
    M.facingLive(['A', 'D'], { facing: 'L' }); ok(holder.charms[0].facing === 'L' && holder.bodies[0].facing === 'L', 'H11 the page\'s held copy of the design (and its bodies) takes the new word');
    M.facingLive(['A'], { facing: null }); ok(!('facing' in holder.charms[0]) && !('facing' in holder.bodies[0]), 'H12 and gives it back when the word is cleared');
    M.facingLive(['C'], { facing: 'X' }); ok(holder.charms[0].facing === 'X', 'H13 every size of the design is told');
    // a MISMATCHED pair (a left body and a right body): one box per body, saved together, body 0 also as `facing`
    const mis = { sku: 'M', pair: { v: 1, bodies: 2, mismatched: true }, facings: [null, 'R'] }, mc = Pair.facingControl(mis);
    ok(mc.show && mc.mismatched && mc.bodies.length === 2 && mc.bodies[0].value === '' && mc.bodies[1].value === 'R' && mc.bodies[1].options.map(o => o[0]).join() === ',L,R' && Pair.facingControl({ sku: 'M', pair: { v: 1, bodies: 2, mismatched: true }, facing: 'L' }).bodies[0].value === 'L', 'H14 a mismatched pair always gets one box per body, each with its own word');
    const mhtml = M.facesBox(mis, 'M');
    ok((mhtml.match(/<select data-faces="M" data-body="/g) || []).length === 2 && /data-body="1"[\s\S]*<option value="R" selected>Right faces right/.test(mhtml) && /Left: not set/.test(mhtml), 'H15 and draws both boxes: ' + mhtml.slice(0, 160));
    {
      const mk2 = v => ({ value: v, dataset: { body: '' } }); const s0 = Object.assign(mk2(''), { dataset: { body: '0' } }), s1 = Object.assign(mk2('R'), { dataset: { body: '1' } });
      s0.parentNode = s1.parentNode = { querySelectorAll: () => [s1, s0] };
      const calls2 = []; const toasts2 = [];
      new Function('each', 'patchMany', 'toast', 'facingLive', line)((attr, fn) => fn(s1, ['M', 'M2']), async (skus, p) => { calls2.push([skus, p]); }, (m, k) => toasts2.push([m, k]), () => {});
      s0.value = 'L'; s1.onchange(); await new Promise(r => setTimeout(r, 5));
      ok(calls2.length === 1 && JSON.stringify(calls2[0][1]) === JSON.stringify({ facing: 'L', facings: ['L', 'R'] }) && /left body faces left, right body faces right/.test(toasts2[0][0]), 'H16 changing a body saves both words together, body 0 also as facing: ' + JSON.stringify(calls2[0] && calls2[0][1]));
      s0.value = ''; s1.value = ''; s1.onchange(); await new Promise(r => setTimeout(r, 5));
      ok(JSON.stringify(calls2[1][1]) === JSON.stringify({ facing: null, facings: null }), 'H17 both boxes cleared gives the words back');
    }
  }

  /* ── I · the list for Paul (scripts/facing-report.cjs) ── */
  {
    const FR = require('../../scripts/facing-report.cjs');
    const rep = FR.report([{ skus: ['MITTEN (HUGGIE)'], level: 'directional', cut: .3, art: null, mm: [9, 9] }, { skus: ['HEART'], level: 'symmetric', cut: 0, art: 0 }, { skus: ['INITIAL LETTER STUD'], level: 'directional', cut: .4, art: null },
      { skus: ['DOG'], level: 'directional', cut: .2, art: null, facing: 'R' }, { skus: ['WOBBLE'], level: 'slight', cut: .05, art: null }, { skus: ['HUGGIE HOOPS- GIRAFFE', 'GIRAFFE (HUGGIE)'], level: 'directional', cut: .1, art: .2 }, { skus: ['GIRAFFE_2069'], level: 'directional', cut: .09, art: null }]);
    ok(rep.counts.needAWord === 3 && rep.counts.wordSet === 1 && rep.counts.readsOneWayByName === 1 && rep.counts.symmetric === 1 && rep.counts.slight === 1, 'I1 the list asks for a word only for directional designs with none: ' + JSON.stringify(rep.counts));
    ok(rep.designs.every(d => d.guess === null) && rep.readsOneWayByName.length === 1 && rep.slight.length === 1, 'I2 no side is guessed; letters and nearly-symmetric designs are listed apart');
    ok(rep.designs[0].skus[0] === 'MITTEN (HUGGIE)' && rep.designs[0].use === 'earring' && rep.bySku['MITTEN (HUGGIE)'] === 1 && FR.familyOf(['HUGGIE HOOPS- GIRAFFE', 'GIRAFFE (HUGGIE)']) === 'GIRAFFE' && FR.familyOf(['GIRAFFE_2069']) === 'GIRAFFE', 'I3 earring SKUs come first, strongest first, by SKU; huggie and plain versions of one animal are one family');
    ok(rep.families.some(f => f.family === 'GIRAFFE' && f.designs.length === 2) && FR.levelOf(.07, null) === 'directional' && FR.levelOf(.01, null) === 'symmetric' && FR.levelOf(.05, null) === 'slight', 'I4 the same bars as the module (0.065 / 0.04 for the cut line)');
  }

  console.log('pairs-mirror: ' + n + ' checks passed');
})().catch(e => { console.error(e); process.exit(1); });
