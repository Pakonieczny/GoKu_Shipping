// Pairs on the master side (PAIRMASTER): the shared module charm-nest-pair.js (bodiesOf, sides, groups, pieces) on synthetic charms.
//   node tests/charm-nest/pairs-master.cjs
const assert = require('assert');
const Pair = require('../../charm-nest-pair.js');
const rect = (x0, y0, x1, y1, extra) => Object.assign({ kind: 'path', closed: true, stroke: true, fill: false, layer: 'CUT', strokeRGB: [0, 0, 0], lwPt: .25, bbox: [x0, y0, x1, y1], subpaths: [[['m', [x0, y0]], ['l', [x1, y0]], ['l', [x1, y1]], ['l', [x0, y1]], ['l', [x0, y0]]]] }, extra || {});
const eng = (x0, y0, x1, y1, rgb) => Object.assign(rect(x0, y0, x1, y1), { layer: 'ENGRAVE', strokeRGB: rgb || [1, 0, 0] });
let n = 0; const ok = (c, m) => { assert(c, m); n++; };

// one body, a body with a hoop beside it, a body with a hole: ONE body
const o1 = rect(0, 0, 20, 26), e1 = eng(4, 2, 16, 6), hoop = rect(8, 26, 12, 30), hole = rect(8, 18, 12, 22);
ok(Pair.bodiesOf({ outline: o1, members: [o1, e1], bbox: [0, 0, 20, 26] }).length === 1, 'a normal charm is one body');
ok(Pair.bodiesOf({ outline: o1, members: [o1, e1, hoop], bbox: [0, 0, 20, 30] }).length === 1, 'a hoop beside the body is part of it');
ok(Pair.bodiesOf({ outline: o1, members: [o1, e1, hole], bbox: [0, 0, 20, 26] }).length === 1, 'a cut-out is not a body');
ok(!Pair.isMismatched({ outline: o1, members: [o1, e1], bbox: [0, 0, 20, 26] }), 'one body is not mismatched');

// two bodies of one cut shape and different engraving (MITTENS 1 + MITTENS 2): a mismatched pair, left to right
const o2 = rect(21, 0, 41, 26), e2 = eng(25, 10, 30, 15, [0, 0, 1]);
const pair = { outline: o2, members: [o2, e2, o1, e1], bbox: [0, 0, 41, 26] };   // (the outline is the RIGHT one: order comes from x, not from the outline)
const b = Pair.bodiesOf(pair);
ok(b.length === 2 && b[0].bbox[0] === 0 && b[1].bbox[0] === 21, 'two bodies, left to right by x');
ok(b[0].side === 'L' && b[1].side === 'R', 'sides L then R');
ok(b[0].members.includes(e1) && b[1].members.includes(e2), 'each body keeps its own engraving');
ok(Pair.isMismatched(pair), 'different engraving on one cut shape is mismatched');
// two identical bodies are one charm drawn twice, not a mismatched pair
const twin = { outline: o1, members: [o1, e1, rect(21, 0, 41, 26), eng(25, 2, 37, 6)], bbox: [0, 0, 41, 26] };
ok(Pair.bodiesOf(twin).length === 2 && !Pair.isMismatched(twin), 'identical twin bodies are not mismatched');
// a small sample beside a charm is not a second body
const sample = { outline: o1, members: [o1, e1, rect(25, 0, 33, 8)], bbox: [0, 0, 33, 26] };
ok(Pair.bodiesOf(sample).length === 1, 'a small sample is not a body');
// an entry that carries the pair field answers without geometry
ok(Pair.isMismatched({ sku: 'MISMATCHED_7134', pair: { v: 1, bodies: 2, mismatched: true } }), 'a master entry with pair.mismatched');
ok(!Pair.isMismatched({ sku: 'X', pair: { v: 1, bodies: 2, mismatched: false } }), 'a master entry with a twin pair');

// sides and labels
ok(Pair.sideOf(0, 2) === 'L' && Pair.sideOf(1, 2) === 'R' && Pair.sideOf(0, 1) === null && Pair.sideOf(2, 3) === null, 'sideOf');
ok(Pair.sideLabel('L') === 'Left' && Pair.sideLabel('R') === 'Right' && Pair.sideLabel(null) === '', 'sideLabel');

// groups
const line = { receiptId: '3912345678', transactionId: '4455', quantity: 1, form: 'earrings' };
ok(Pair.groupKey(line) === '3912345678:4455', 'groupKey of a line');
ok(Pair.groupKey({ orderId: 3912345678, transactionId: 4455, poolId: '3912345678_4455_2' }) === '3912345678:4455', 'groupKey of a pool row');
ok(Pair.groupKey('3912345678_4455_2') === '3912345678:4455', 'groupKey of a pool id');
ok(Pair.groupKey({ poolId: '3912345678_4455_1' }) === '3912345678:4455', 'groupKey from the pool id alone');
ok(Pair.mustShareSheet({ poolId: '3912345678_4455_1' }, { poolId: '3912345678_4455_2' }), 'two copies of one line share a sheet');
ok(!Pair.mustShareSheet({ poolId: '3912345678_4455_1' }, { poolId: '3912345678_9999_1' }), 'two lines of one order do not');
ok(!Pair.mustShareSheet({ poolId: '3912345678_4455_1' }, { poolId: '3912345678_4455_1' }), 'a piece is not its own sibling');

// pieces (Paul, 9 Oct 18:46-18:47: an earring pair is always a Left and a Right, quantity q = q of each; a mismatched design's bodies are its Left and Right)
const one = { outline: o1, members: [o1, e1], bbox: [0, 0, 20, 26] };
const earrings = Object.assign({}, line, { form: 'earrings' }), two = Object.assign({}, earrings, { quantity: 2 });
const mp1 = Pair.piecesFor(earrings, pair);
ok(mp1.length === 2 && mp1[0].side === 'L' && mp1[0].bodyIndex === 0 && mp1[1].side === 'R' && mp1[1].bodyIndex === 1 && mp1.every(p => p.groupKey === '3912345678:4455' && p.of === 2), 'a mismatched earring line makes its left body then its right body');
ok(Pair.piecesFor(two, pair).map(p => p.side).join('') === 'LRLR' && Pair.piecesFor(two, pair).map(p => p.bodyIndex).join('') === '0101', 'quantity 2 mismatched: two of each side');
const mm = Pair.piecesFor(earrings, one);
ok(mm.length === 2 && mm[0].side === 'L' && mm[1].side === 'R' && mm.every(p => p.bodyIndex === 0), 'a matching pair: two pieces of one body, still Left and Right');
ok(Pair.piecesFor(two, one).length === 4 && Pair.piecesFor(two, one).map(p => p.side).join('') === 'LRLR', 'pair quantity 2 gives 4 pieces');
ok(Pair.piecesFor({ receiptId: 1, transactionId: 2, quantity: 1, form: 'earring-single' }, one).length === 1 && Pair.piecesFor({ receiptId: 1, transactionId: 2, quantity: 1, form: 'earring-single' }, one)[0].side === null, 'a single earring is one piece, side null');
ok(Pair.piecesFor({ receiptId: 1, transactionId: 2, quantity: 1, form: 'necklace' }, one).length === 1, 'a necklace is one piece');
ok(Pair.piecesFor({ receiptId: 1, transactionId: 2, quantity: 1, form: 'huggie' }, one).length === 2 && Pair.piecesFor({ receiptId: 1, transactionId: 2, quantity: 1, form: 'hoop' }, one).length === 2, 'huggie and hoop earrings are pairs too');
ok(Pair.piecesFor({ receiptId: 1, transactionId: 2, spec: { quantity: 1, pieceCount: 3, form: 'necklace' } }, one).every(p => p.side === null && p.mirror === false), '3 discs: three pieces, side null, never mirrored');
ok(Pair.piecesFor({ receiptId: 1, transactionId: 2, spec: { quantity: 1, pieceCount: 2, pair: { kind: 'pair' } } }, one).length === 2, 'spec.pieceCount and spec.pair.kind win');
ok(Pair.piecesFor({ receiptId: 1, transactionId: 2, quantity: 1, poolIds: ['a', 'b', 'c'] }, one).length === 3, 'the pool ids are the fact');
ok(Pair.kindOf(earrings, pair) === 'mismatched' && Pair.kindOf(earrings, one) === 'pair' && Pair.kindOf(two, one) === 'multi' && Pair.kindOf({ receiptId: 1, transactionId: 2, quantity: 1, form: 'earring-single' }, one) === 'single', 'kindOf');
ok(Pair.discsOf({ title: 'Disc necklace, 3 discs' }) === 3, 'discs are read from the listing text as information');

// direction: Left and Right earrings are mirror images (Paul, 9 Oct 18:47)
const poly = (pts, extra) => { const xs = pts.map(p => p[0]), ys = pts.map(p => p[1]); return Object.assign({ kind: 'path', closed: true, stroke: true, fill: false, layer: 'CUT', strokeRGB: [0, 0, 0], lwPt: .25, bbox: [Math.min(...xs), Math.min(...ys), Math.max(...xs), Math.max(...ys)], subpaths: [[['m', pts[0]]].concat(pts.slice(1).map(p => ['l', p])).concat([['l', pts[0]]])] }, extra || {}); };
const thumbLeft = [[5, 0], [20, 0], [20, 26], [5, 26], [5, 14], [0, 14], [0, 8], [5, 8]];             // a mitten-like body: the thumb sticks out to the LEFT, the mass lies right
const mitten = poly(thumbLeft, { index: 1 }), mink = eng(8, 2, 18, 6); mink.index = 2;
const mCharm = { outline: mitten, members: [mitten, mink], bbox: [0, 0, 20, 26], upAngle: 30 };
ok(Pair.facingOf(mCharm) === 'L', 'a body with its thumb out left faces left');
ok(Pair.facingOf({ outline: o1, members: [o1], bbox: [0, 0, 20, 26] }) === null, 'a symmetric body has no facing');
ok(Pair.facingOf(Object.assign({ facing: 'R' }, mCharm)) === 'R', 'a facing a person set wins over the heuristic');
const mirrored = Pair.mirrorOf(mCharm);
ok(mirrored.bbox.join() === '0,0,20,26' && mirrored.mirrored === true && mirrored !== mCharm && mCharm.mirrored === undefined, 'mirrorOf: a new charm, the same box, the original untouched');
ok(mirrored.outline.subpaths[0][0][1][0] === 15 && mirrored.outline.subpaths[0][5][1][0] === 20 && mirrored.outline.synthetic === true, 'mirrorOf reflects x about the box centre and marks the path as written from geometry');
ok(Pair.facingOf(mirrored) === 'R', 'the mirror image of a left-facing body faces right');
ok(mirrored.upAngle === 150, 'the hole direction turns about the vertical axis (30 -> 150)');
const back = Pair.mirrorOf(mirrored);
ok(JSON.stringify(back.outline.subpaths) === JSON.stringify(mitten.subpaths) && back.bbox.join() === '0,0,20,26', 'mirroring twice gives the drawing back');
ok(JSON.stringify(Pair.mirrorOf([[0, 0], [10, 0], [10, 5]])) === JSON.stringify([[10, 0], [0, 0], [0, 5]]), 'mirrorOf a polygon about its own box');
ok(Pair.mirrorOf({ bits: new Uint8Array([1, 0, 0, 0, 1, 1]), w: 3, h: 2 }).bits.join('') === '001110', 'mirrorOf a mask flips its columns');
// pieces carry mirror: as drawn faces left, so the Right piece is the mirror
const mline = Object.assign({}, line, { form: 'earrings' });
ok(Pair.piecesFor(mline, mCharm).map(p => p.side + (p.mirror ? 'm' : '')).join() === 'L,Rm', 'a left-facing master: Left as drawn, Right mirrored');
ok(Pair.piecesFor(mline, Object.assign({}, mCharm, { facing: 'R' })).map(p => p.side + (p.mirror ? 'm' : '')).join() === 'Lm,R', 'a right-facing master: Left mirrored, Right as drawn');
ok(Pair.piecesFor(mline, { outline: o1, members: [o1], bbox: [0, 0, 20, 26] }).map(p => p.side + (p.mirror ? 'm' : '')).join() === 'L,Rm', 'an unknown facing: as drawn is the Left, the Right is the mirror');
ok(Pair.piecesFor({ receiptId: 1, transactionId: 2, quantity: 1, form: 'earring-single' }, mCharm).every(p => !p.mirror), 'a single earring is never mirrored');
// a mismatched pair: each body faces its own ear (both bodies drawn thumb-left: the Right body is mirrored)
const mA = poly(thumbLeft, { index: 11 }), mB = poly(thumbLeft.map(p => [p[0] + 25, p[1]]), { index: 12 });
const mBink = eng(30, 3, 40, 7, [0, 0, 1]); mBink.index = 13;
const mismatchedCharm = { outline: mA, members: [mA, mB, mBink], bbox: [0, 0, 45, 26] };
const mmp = Pair.piecesFor(mline, mismatchedCharm);
ok(mmp.length === 2 && mmp[0].side === 'L' && mmp[0].bodyIndex === 0 && mmp[0].mirror === false && mmp[1].side === 'R' && mmp[1].bodyIndex === 1 && mmp[1].mirror === true, 'a mismatched pair: the right body faces the wrong way and is mirrored');
// piece geometry
const gL = Pair.pieceGeometry(mismatchedCharm, mmp[0]), gR = Pair.pieceGeometry(mismatchedCharm, mmp[1]);
ok(gL.outline.bbox.join() === '0,0,20,26' && gL.members.length === 1 && gL.mirrored !== true, 'pieceGeometry: the left body alone, as drawn');
ok(gR.outline.bbox.join() === '25,0,45,26' && gR.mirrored === true && gR.outline.subpaths[0][0][1][0] === 40, 'pieceGeometry: the right body alone, mirrored about its own centre');
ok(Pair.pieceGeometry(mismatchedCharm, mmp[1]) === gR && Pair.pieceGeometry(mismatchedCharm, mmp[0]) === gL, 'pieceGeometry is cached by body and mirror');
const whole = Pair.pieceGeometry(mCharm, { bodyIndex: 0, mirror: false }), flipped = Pair.pieceGeometry(mCharm, { bodyIndex: 0, mirror: true });
ok(whole === mCharm && flipped !== mCharm && flipped.mirrored === true, 'a one-body design: as drawn is the charm itself, the mirror is a separate variant');
ok(Pair.splitAcross([{ poolId: '3912345678_4455_1', s: 'A' }, { poolId: '3912345678_4455_2', s: 'B' }, { poolId: '3912345678_9999_1', s: 'A' }], p => p.s).length === 1, 'a group on two sheets is found');
// the intake's own answer (charm-nest-orders.js spec.pair): earring / glued / legacy / sides win over the form
const ln = pr => ({ receiptId: 5, transactionId: 6, spec: { quantity: 2, pieceCount: pr.count, pair: pr } });
const sd = l => Pair.piecesFor(l, null).map(x => x.side || '-').join('');
ok(sd(ln({ count: 4, earring: true, kind: 'multi', sides: ['L', 'R', 'L', 'R'] })) === 'LRLR', 'a quantity-2 earring pair (kind multi) is four pieces, L R L R');
ok(sd(ln({ count: 2, earring: false, kind: 'multi', sides: [null, null] })) === '--', 'a 2-disc necklace (kind multi, not an earring) has no sides');
ok(sd(ln({ count: 1, earring: true, single: true, sides: ['R'] })) === 'R', 'a single earring that names its ear keeps it');
ok(sd(ln({ count: 2, earring: true, glued: true, kind: 'mismatched' })) === '--', 'a mismatched design counted as one glued copy has no sides');
ok(sd(ln({ count: 2, earring: true, legacy: true, kind: 'pair' })) === '--', 'an old line pinned to the pieces it had has no sides');
ok(sd(ln({ count: 2, earring: true, kind: 'pair' })) === 'LR', 'an earring pair without sides alternates L R');
ok(Pair.isEarringPair({ spec: { pair: { earring: false, kind: 'pair' } } }) === false && Pair.isEarringPair({ spec: { pair: { earring: true, kind: 'multi' } } }) === true, 'spec.pair.earring wins over spec.pair.kind');
const mm2 = Pair.piecesFor(ln({ count: 1, earring: true, single: true, sides: ['R'] }), mismatchedCharm);
ok(mm2.length === 1 && mm2[0].side === 'R' && mm2[0].bodyIndex === 1, 'a single Right earring of a mismatched design is its right body');

// master side: a row of two touching bodies with one label centred under it
const rowCharm = (idx, x0, ink, label) => { const o = rect(x0, 0, x0 + 20, 26); o.index = idx * 10; const i = eng(x0 + 4, 8, x0 + 12, 14, ink); i.index = idx * 10 + 1; return { index: idx, outline: o, members: [o, i], bbox: [x0, 0, x0 + 20, 26], topIndices: [idx * 10], extras: [], strokePt: 0.5 }; };
const cA = rowCharm(1, 0, [1, 0, 0]), cB = rowCharm(2, 22, [0, 0, 1]);
const lab1 = c => c.index === 1 ? [{ sku: 'MISMATCHED_9', bbox: [10, -12, 32, -4] }] : [];
const rows = Pair.masterPairs([cA, cB], lab1);
ok(rows.length === 1 && rows[0].kind === 'mismatched' && rows[0].sure && rows[0].owner === 1 && rows[0].charms.join() === '1,2', 'masterPairs: two touching bodies, one centred label, different engraving: a sure mismatched pair');
ok(Pair.pairField(rows[0]).mismatched === true && Pair.pairField(rows[0]).bodies === 2 && Pair.pairField(rows[0]).v === 1, 'pairField is { v: 1, bodies: 2, mismatched: true }');
const beforeA = JSON.stringify(cA), beforeB = JSON.stringify(cB);
const fc = Pair.foldRow(cA, [cB]);
ok(fc !== cA && fc.members.length === 4 && fc.bbox.join() === '0,0,42,26' && fc.outline === cA.outline && fc.topIndices.length === 2, 'foldRow: a new charm with both bodies, the box of both, the owner outline kept');
ok(JSON.stringify(cA) === beforeA && JSON.stringify(cB) === beforeB, 'foldRow leaves the grouping alone');
ok(Pair.bodiesOf(fc).length === 2 && Pair.isMismatched(fc), 'the folded charm reads back as two bodies');
ok(Pair.masterPairs([cA, cB], c => c.index === 1 ? [{ sku: 'X_1', bbox: [28, -12, 50, -4] }] : []).every(r => !r.sure), 'a label that is not under the middle of the row makes no pair');
ok(Pair.masterPairs([cA, cB], c => c.index === 1 ? [{ sku: 'V1 V2', bbox: [10, -12, 32, -4] }] : [])[0].sure === false, 'a label of short words ("V1 V2") is not sure');
ok(Pair.masterPairs([cA, cB], c => [{ sku: 'ONE_' + c.index, bbox: [c.index === 1 ? 0 : 22, -12, c.index === 1 ? 20 : 42, -4] }]).every(r => !r.sure), 'two bodies with a label each are neighbours, not a pair');
ok(Pair.masterPairs([cA, Object.assign({}, rowCharm(3, 22, [1, 0, 0]))], lab1)[0].kind === 'twins', 'two identical bodies are twins, not a mismatched pair');
console.log(`pairs-master: ${n} checks passed`);
