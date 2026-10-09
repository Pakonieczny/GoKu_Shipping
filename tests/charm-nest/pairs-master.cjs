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
ok(Pair.facingOf(mCharm) === null && Pair.facingInfo(Pair.bodiesOf(mCharm)[0], mCharm).directional === true, 'a body with its thumb out left is directional, and nothing in its shape says which way it faces: unknown (as drawn is the Left)');
ok(Pair.facingOf({ outline: o1, members: [o1], bbox: [0, 0, 20, 26] }) === null, 'a symmetric body has no facing');
ok(Pair.facingOf(Object.assign({ facing: 'R' }, mCharm)) === 'R', 'a facing a person set wins over the heuristic');
const mirrored = Pair.mirrorOf(mCharm);
ok(mirrored.bbox.join() === '0,0,20,26' && mirrored.mirrored === true && mirrored !== mCharm && mCharm.mirrored === undefined, 'mirrorOf: a new charm, the same box, the original untouched');
ok(mirrored.outline.subpaths[0][0][1][0] === 15 && mirrored.outline.subpaths[0][5][1][0] === 20 && mirrored.outline.synthetic === true, 'mirrorOf reflects x about the box centre and marks the path as written from geometry');
ok(Pair.facingOf(Pair.mirrorOf(Object.assign({ facing: 'L' }, mCharm))) === 'R', 'the mirror image of a body a person set to face left faces right');
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

// the indexer's pair layer (scripts/index-master.cjs pairLayer) on synthetic charms, with the geometry readers stubbed
{
  const IM = require('../../scripts/index-master.cjs');
  const Pst = { integrateRings: () => ({ left: [] }), cutLinesOf: c => c.members.filter(m => m !== c.outline && m.layer === 'CUT') };
  const Gst = { flatten: o => [[[o.bbox[0], o.bbox[1]], [o.bbox[2], o.bbox[1]], [o.bbox[2], o.bbox[3]], [o.bbox[0], o.bbox[3]], [o.bbox[0], o.bbox[1]]]], upAngleOf: () => ({ angle: 90, source: 'drawn' }), backView: () => ({}), engraveMask: () => ({}), largestRectangles: () => [{ wPt: 40, hPt: 40 }] };
  const mk = () => {
    const a = rowCharm(1, 0, [1, 0, 0]), b = rowCharm(2, 22, [0, 0, 1]), lone = rowCharm(7, 200, [1, 0, 0]);
    const g = { charms: [a, b, lone] }, lab = { labels: new Map([[1, { sku: 'MISMATCHED_9', bbox: [10, -12, 32, -4], extra: [{ sku: 'EXTRA_9', size: null, bbox: [10, -20, 32, -14] }] }], [7, { sku: 'LONE_1', bbox: [200, -12, 220, -4], extra: [] }]]) };
    return { g, lab, items: [...lab.labels].map(([index, l]) => ({ index, l, c: g.charms.find(x => x.index === index) })) };
  };
  let w = mk(); const before = JSON.stringify(w.g.charms);
  const lg = [], r1 = IM.pairLayer(Pst, Gst, Pair, w.g, w.lab, w.items, { engraveMarginMm: .8 }, m => lg.push(m));
  ok(r1.fold.size === 1 && r1.fold.has(1) && !r1.fold.has(7), 'pairLayer folds the sure row and nothing else');
  const pm = r1.fold.get(1);
  ok(pm.field.v === 1 && pm.field.bodies === 2 && pm.field.mismatched === true && pm.charm.members.length === 4 && pm.charm.bbox.join() === '0,0,42,26', 'the folded design: pair field, both bodies, the box of both');
  ok(pm.view.outline.bbox.join() === '0,0,42,26' && pm.view.outline.subpaths.length === 2 && pm.charm.outline === w.g.charms[0].outline, 'the silhouette view has both outlines, the charm keeps a real body outline');
  ok(pm.holes === 0 && pm.open === false && pm.engrave.engravable === true, 'holes, open and engravable are read from the bodies');
  ok(JSON.stringify(w.g.charms) === before, 'the grouping\'s charms are left as they were');
  ok(r1.rows.length === 1 && r1.rows[0].folded === true && r1.rows[0].sure === true && r1.rows[0].skus.join() === 'MISMATCHED_9,EXTRA_9', 'the report row says folded and names every SKU line');
  w = mk(); ok(IM.pairLayer(Pst, Gst, Pair, w.g, w.lab, w.items, { noPairs: true }, () => {}).rows.length === 0, '--no-pairs: no rows, nothing folded');
  w = mk(); ok(IM.pairLayer(Pst, Gst, Pair, w.g, w.lab, w.items.filter(i => i.index === 7), {}, () => {}).fold.size === 0, 'a row whose owner is not built in this run is not folded');
  w = mk(); ok(IM.pairLayer(null, null, null, w.g, w.lab, w.items, {}, () => {}).fold.size === 0, 'without the module the layer does nothing');
  // a doubtful row (identical bodies) is reported, not folded, unless a person names it
  const twinCharms = () => { const a = rowCharm(1, 0, [1, 0, 0]), b = rowCharm(2, 22, [1, 0, 0]); return { g: { charms: [a, b] }, lab: { labels: new Map([[1, { sku: 'TWINS_1', bbox: [10, -12, 32, -4], extra: [] }]]) } }; };
  let t = twinCharms(); let ti = [{ index: 1, l: t.lab.labels.get(1), c: t.g.charms[0] }];
  const rt = IM.pairLayer(Pst, Gst, Pair, t.g, t.lab, ti, {}, () => {});
  ok(rt.rows.length === 1 && rt.rows[0].kind === 'twins' && !rt.rows[0].folded && rt.fold.size === 0, 'identical twins are listed, not folded');
  t = twinCharms(); ti = [{ index: 1, l: t.lab.labels.get(1), c: t.g.charms[0] }];
  const rf = IM.pairLayer(Pst, Gst, Pair, t.g, t.lab, ti, { pairAlso: 'TWINS_1' }, () => {});
  ok(rf.fold.size === 1 && rf.fold.get(1).field.mismatched === false && rf.fold.get(1).field.bodies === 2 && rf.rows[0].forced === true, '--pair-also folds a doubtful row a person named, as a matching pair drawn twice');
}

// agreement with the intake's own rule (charm-nest-orders.js piecesOf / pieceCountOf / kindFor) on lines carrying spec.pair
{
  const Orders = require('../../charm-nest-orders.js');
  const L = (q, pr, extra) => Object.assign({ receiptId: 5, transactionId: 6, quantity: q, spec: Object.assign({ quantity: q, pair: pr }, extra || {}) });
  const cases = [
    L(2, { earring: true, single: false, mismatched: false, perUnit: 2 }),
    L(1, { earring: true, mismatched: true, perUnit: 2 }),
    L(3, { earring: true, mismatched: true, perUnit: 2 }),
    L(1, { earring: true, mismatched: true, glued: true, perUnit: 1 }),
    L(1, { earring: false, single: true, sideSaid: 'R', perUnit: 1 }),
    L(1, { earring: false, perUnit: 3 }, { pieceCount: 3 }),
    L(1, { earring: true, perUnit: 2 }),
    L(1, { earring: true, legacy: true, perUnit: 2 }, { pieceCount: 1 })
  ];
  for (const c of cases) {
    const a = Orders.piecesOf(c).map(p => (p.side || '-') + p.bodyIndex).join(' '), b = Pair.piecesFor(c, null).map(p => (p.side || '-') + p.bodyIndex).join(' ');
    ok(a === b && Orders.pieceCountOf(c) === Pair.pieceCountOf(c, null), 'piecesFor agrees with CharmNestOrders.piecesOf: ' + a);
  }
  ok(Pair.kindOf(cases[1], null) === 'mismatched' && Pair.kindOf(cases[0], null) === 'multi' && Pair.kindOf(cases[3], null) === 'mismatched' && Pair.kindOf(cases[6], null) === 'pair', 'kindOf follows the intake facts (quantity-2 pair is multi, one glued copy is mismatched)');
}

// ── item 8: the pair layer is ONE shared function (CharmNestPair.pairLayer), and no writer turns a pair design back into one body ──
{
  const Pst = { integrateRings: () => ({ left: [] }), cutLinesOf: c => c.members.filter(m => m !== c.outline && m.layer === 'CUT') };
  const Gst = { flatten: o => [[[o.bbox[0], o.bbox[1]], [o.bbox[2], o.bbox[1]], [o.bbox[2], o.bbox[3]], [o.bbox[0], o.bbox[3]], [o.bbox[0], o.bbox[1]]]], upAngleOf: () => ({ angle: 90, source: 'drawn' }), backView: () => ({}), engraveMask: () => ({}), largestRectangles: () => [{ wPt: 40, hPt: 40 }] };
  const mk = () => {
    const a = rowCharm(1, 0, [1, 0, 0]), b = rowCharm(2, 22, [0, 0, 1]), lone = rowCharm(7, 200, [1, 0, 0]);
    const g = { charms: [a, b, lone] }, lab = { labels: new Map([[1, { sku: 'MISMATCHED_9', bbox: [10, -12, 32, -4], extra: [] }], [7, { sku: 'LONE_1', bbox: [200, -12, 220, -4], extra: [] }]]) };
    return { g, lab, items: [...lab.labels].map(([index, l]) => ({ index, l, c: g.charms.find(x => x.index === index) })) };
  };
  ok(typeof Pair.pairLayer === 'function' && typeof Pair.entryFields === 'function' && typeof Pair.keepPairs === 'function' && typeof Pair.heldPair === 'function', 'the module exports pairLayer, entryFields, keepPairs, heldPair');
  let w = mk(); const IM = require('../../scripts/index-master.cjs');
  const direct = Pair.pairLayer(Pst, Gst, w.g, w.lab, w.items, {}, () => {}), wrapped = IM.pairLayer(Pst, Gst, Pair, mk().g, w.lab, w.items, {}, () => {});
  ok(direct.fold.size === 1 && direct.fold.has(1) && JSON.stringify(direct.rows) === JSON.stringify(wrapped.rows) && JSON.stringify(direct.fold.get(1).field) === JSON.stringify(wrapped.fold.get(1).field) && direct.refused.size === 0 && wrapped.refused instanceof Map, 'the script\'s pairLayer is the module\'s: same rows, same field');
  // --pair-also / pairAlso: a Set, a list or a comma list
  const tw = () => { const a = rowCharm(1, 0, [1, 0, 0]), b = rowCharm(2, 22, [1, 0, 0]); const l = { sku: 'TWINS_1', bbox: [10, -12, 32, -4], extra: [] }; return { g: { charms: [a, b] }, lab: { labels: new Map([[1, l]]) }, items: [{ index: 1, l, c: a }] }; };
  for (const also of [new Set(['TWINS_1']), ['twins_1'], 'X_1, twins_1']) { const t = tw(); ok(Pair.pairLayer(Pst, Gst, t.g, t.lab, t.items, { pairAlso: also }, () => {}).fold.size === 1, 'pairAlso as ' + (typeof also === 'string' ? 'a comma list' : also instanceof Set ? 'a Set' : 'a list')); }
  { const t = tw(); ok(Pair.pairLayer(Pst, Gst, t.g, t.lab, t.items, {}, () => {}).fold.size === 0, 'without pairAlso a doubtful row stays as it was'); }
  // a row that is surely a pair but cannot be folded is refused, never written as its lone body
  { w = mk(); const lg = [], r = Pair.pairLayer({ integrateRings: c => { if (c.index === 2) throw new Error('boom'); return { left: [] }; }, cutLinesOf: Pst.cutLinesOf }, Gst, w.g, w.lab, w.items, {}, m => lg.push(m));
    ok(r.fold.size === 0 && r.refused.has(1) && /boom/.test(r.refused.get(1)) && r.rows[0].folded === false && lg.some(m => /could not be folded/.test(m)), 'a pair that cannot be folded is refused and named'); }
  { w = mk(); w.g.charms.splice(1, 1); w.lab.labels.set(1, w.lab.labels.get(1)); const r = Pair.pairLayer(Pst, Gst, w.g, w.lab, w.items, {}, () => {}); ok(r.refused.size === 0 && r.fold.size === 0, 'a row that is no longer a row (one body left) is nothing to refuse'); }
  // entryFields: sym for every design, facings only for a folded pair drawn as mirror images, never throws
  ok(['symmetric', 'slight', 'directional'].includes(Pair.entryFields(w.g.charms[0], false).sym) && !('facings' in Pair.entryFields(w.g.charms[0], false)) && Object.keys(Pair.entryFields(null, true)).length === 0 && Object.keys(Pair.entryFields({}, false)).length === 0, 'entryFields: sym; nothing for a missing charm; no throw');
  // heldPair / keepPairs: what the browser indexer must leave alone
  const P2 = { sku: 'X', pair: { v: 1, bodies: 2, mismatched: true } };
  ok(Pair.heldPair(P2) && !Pair.heldPair({ sku: 'X' }) && !Pair.heldPair(null) && !Pair.heldPair({ pair: { bodies: 1 } }) && Pair.heldPair({ sku: 'X', sizes: { S: { pair: { v: 1, bodies: 2 } } } }, 's') && !Pair.heldPair({ sku: 'X', sizes: { S: {} }, pair: null }, 'S') , 'heldPair reads the pair field of the entry or of its size');
  { w = mk(); const lab = c => { const l = w.lab.labels.get(c.index); return l ? [l].concat(l.extra || []).map(x => ({ sku: x.sku, size: x.size, bbox: x.bbox })) : []; };
    const none = Pair.keepPairs(w.g.charms, lab, new Map());
    ok(none.size === 1 && none.get(1).kind === 'row' && none.get(1).skus[0] === 'MISMATCHED_9' && /mismatched pair/.test(none.get(1).why) && !none.has(7) && !none.has(2), 'a sure row is held back (its owner), nothing else');
    const known = new Map([['LONE_1', P2]]); const held = Pair.keepPairs(w.g.charms, lab, known);
    ok(held.get(7).kind === 'held' && /holds LONE_1 as a pair of 2 bodies/.test(held.get(7).why) && held.get(1).kind === 'row', 'a SKU the library holds as a pair is held back when the drawing shows one body');
    const both = rowCharm(7, 200, [1, 0, 0]), second = rowCharm(8, 222, [0, 0, 1]); const folded = Pair.foldRow(both, [second]);
    ok(!Pair.keepPairs([folded], c => [{ sku: 'LONE_1', bbox: [210, -12, 232, -4] }], known).has(7), 'a charm that already shows both bodies is not held back');
    ok(Pair.keepPairs(w.g.charms, lab, null).size === 1, 'without the library\'s entries only the rows are held back');
    const sized = new Map([['LONE_1', { sku: 'LONE_1', sizes: { S: { pair: { v: 1, bodies: 2, mismatched: true } } } }]]);
    ok(!Pair.keepPairs(w.g.charms, c => c.index === 7 ? [{ sku: 'LONE_1', size: 'M', bbox: [200, -12, 220, -4] }] : [], sized).has(7) && Pair.keepPairs(w.g.charms, c => c.index === 7 ? [{ sku: 'LONE_1', size: 'S', bbox: [200, -12, 220, -4] }] : [], sized).has(7), 'a size is held back only when that size holds the pair'); }
  // the browser indexer asks before it uploads anything
  const bridge = require('fs').readFileSync(require('path').join(__dirname, '../../charm-nest-bridge.js'), 'utf8'), wi = bridge.slice(bridge.indexOf('async function writeIndex(job)'));
  ok(wi.indexOf('keepPairs(') > 0 && wi.indexOf('keepPairs(') < wi.indexOf('P.buildSingleCharm(c, parsed)') && /!keep\.has\(c\.index\)/.test(wi) && /were NOT written/.test(wi), 'writeIndex asks keepPairs before it builds or uploads a file, and says what it left');
}

// ── the indexers end to end on a synthetic master (one mismatched pair, one lone charm): the script and the server route write the same records ──
(async () => {
  const fs = require('fs'), os = require('os'), pth = require('path');
  const { start } = require('./bridge-server.cjs');
  const { PDFDocument, PDFName, rgb, StandardFonts } = require(pth.join(__dirname, '../../vendor/pdf-lib-1.17.1.min.js'));
  const IM = require('../../scripts/index-master.cjs');
  const { CharmNestPDF: PDF } = require('../../netlify/functions/_charmNestPdf.js');
  async function pairMaster() {
    const doc = await PDFDocument.create(), page = doc.addPage([420, 300]), font = await doc.embedFont(StandardFonts.Helvetica);
    const f = v => (+v).toFixed(3), poly = pts => pts.map((p, i) => `${f(p[0])} ${f(p[1])} ${i ? 'l' : 'm'}`).join(' ') + ' h';
    const circle = (cx, cy, r) => { const k = 0.5523 * r; return `${f(cx + r)} ${f(cy)} m ${f(cx + r)} ${f(cy + k)} ${f(cx + k)} ${f(cy + r)} ${f(cx)} ${f(cy + r)} c ${f(cx - k)} ${f(cy + r)} ${f(cx - r)} ${f(cy + k)} ${f(cx - r)} ${f(cy)} c ${f(cx - r)} ${f(cy - k)} ${f(cx - k)} ${f(cy - r)} ${f(cx)} ${f(cy - r)} c ${f(cx + k)} ${f(cy - r)} ${f(cx + r)} ${f(cy - k)} ${f(cx + r)} ${f(cy)} c h`; };
    const house = (x, y, w, h) => [[x, y], [x + w, y], [x + w, y + .7 * h], [x + w / 2, y + h], [x, y + .7 * h]], notch = (x, y, w, h) => [[x, y], [x + w, y], [x + w, y + h], [x + .3 * w, y + h], [x, y + .6 * h]];
    const ops = [], body = (pts, x, y, w, h, ink) => { ops.push(`q 0.05 0.05 0.05 RG 0.5 w ${poly(pts)} S Q`, `q ${ink} rg ${f(x + w * .3)} ${f(y + h * .2)} ${f(w * .3)} ${f(h * .2)} re f Q`, `q 0 0 0 RG 0.4 w ${circle(x + w * .7, y + h * .75, 2.4)} S Q`); };
    body(house(100, 150, 45, 45), 100, 150, 45, 45, '0.2 0.35 0.85'); body(notch(147, 150, 45, 45), 147, 150, 45, 45, '0.85 0.2 0.2'); body(house(300, 150, 45, 45), 300, 150, 45, 45, '0.2 0.6 0.3');
    page.node.set(PDFName.of('Contents'), doc.context.obj([doc.context.register(doc.context.flateStream(ops.join('\n') + '\n'))]));
    const label = (t, cx) => { const w = font.widthOfTextAtSize(t, 6); page.drawText(t, { x: cx - w / 2, y: 150 - 4 - 6 * .75, size: 6, font, color: rgb(.1, .1, .1) }); };
    label('MISMATCHED_9001', 146); label('LONE_9002', 322.5);
    return Buffer.from(await doc.save({ useObjectStreams: false }));
  }
  const noDates = buf => Buffer.from(buf).toString('latin1').replace(/\(D:\d{14}Z\)/g, '(D:0)');
  const tmp = fs.mkdtempSync(pth.join(os.tmpdir(), 'cn-pairix-')), file = pth.join(tmp, 'PAIR-master.ai'), bytes = await pairMaster(); fs.writeFileSync(file, bytes);
  const srv = await start({ receipts: [] }), { st, sorterOrigin } = srv, lines = [], log = l => lines.push(String(l)), call = (b) => IM.api(sorterOrigin, '', 'charmNestLibrary', b);
  const SKU = 'MISMATCHED_9001', LONE = 'LONE_9002', pairAi = `charmnest/master/${SKU}.ai`, loneAi = `charmnest/master/${LONE}.ai`;
  try {
    // the script, staged: the reference
    await IM.main(['node', 'x', file, '--out-dir', pth.join(tmp, 'stage')], log);
    const rec = JSON.parse(fs.readFileSync(pth.join(tmp, 'stage', 'records.json'), 'utf8')).entries, sPair = rec.find(e => e.sku === SKU), sLone = rec.find(e => e.sku === LONE);
    ok(sPair && sPair.pair && sPair.pair.bodies === 2 && sPair.pair.mismatched === true && !sLone.pair && sPair.widthPt === 95 && sPair.members === 6, 'the script folds the pair into one design of two bodies and leaves the lone charm alone');
    // the server route (the background function), over the same bytes
    st.blobs.set('charmnest/uploads/pair.ai', { buf: bytes, generation: 1, meta: { contentType: 'application/pdf', metadata: {} } });
    const runJob = async id => { st.put('Charm_Nest_Jobs', id, { id, kind: 'master', status: 'pending', path: 'charmnest/uploads/pair.ai', name: 'PAIR-master.ai', opts: {} }); const out = await st.handlers['charmMaster-background'].handler({ body: JSON.stringify({ id }) }); return { out, job: st.doc('Charm_Nest_Jobs', id) }; };
    const j1 = await runJob('job-1');
    ok(j1.out.statusCode === 200 && j1.job.status === 'done' && j1.job.result.pairs.length === 1 && j1.job.result.pairs[0].folded === true && j1.job.result.pairs[0].sku === SKU && j1.job.result.pairsKept.length === 0, 'the server route builds the pair and says so in its result');
    const vPair = st.doc('Charm_Master_Index', SKU), vLone = st.doc('Charm_Master_Index', LONE);
    for (const k of ['charmHash', 'widthPt', 'heightPt', 'areaPt2', 'members', 'holes', 'engravable', 'upAngle', 'upSource', 'open', 'sym']) { ok(JSON.stringify(vPair[k]) === JSON.stringify(sPair[k]), `server and script agree on the pair's ${k}`); ok(JSON.stringify(vLone[k]) === JSON.stringify(sLone[k]), `server and script agree on the lone charm's ${k}`); }
    ok(JSON.stringify(vPair.pair) === JSON.stringify(sPair.pair) && vLone.pair === undefined, 'server and script agree on the pair field');
    ok(noDates(st.blobs.get(pairAi).buf) === noDates(fs.readFileSync(pth.join(tmp, 'stage', 'files', pairAi))) && noDates(st.blobs.get(loneAi).buf) === noDates(fs.readFileSync(pth.join(tmp, 'stage', 'files', loneAi))), 'and write the same per-SKU files (PDF dates apart)');
    const charmsIn = async key => { const pr = await PDF.parseSource(new Uint8Array(st.blobs.get(key).buf), 'x.ai'); return PDF.groupCharms(pr, { minPt: 6 }).charms.length; };
    ok(await charmsIn(pairAi) === 2 && await charmsIn(loneAi) === 1, 'the pair\'s file holds two bodies as separate forms, the lone charm\'s one');
    // browser indexer's guard on the real grouping of this master
    { const pr = await PDF.parseSource(new Uint8Array(bytes), 'x.ai'), g = PDF.groupCharms(pr, { minPt: 6 }), lab = PDF.labelCharms(pr, g.charms, { pattern: PDF.SKU_PATTERN_DEFAULT, gapPt: 6.4 / (25.4 / 72), widen: .25 });
      const labelsOf = c => { const l = lab.labels.get(c.index); return l ? [l].concat(l.extra || []).map(x => ({ sku: x.sku, size: x.size, bbox: x.bbox })) : []; };
      const known = new Map([[SKU, vPair], [LONE, vLone]]), kept = Pair.keepPairs(g.charms, labelsOf, known);
      ok(kept.size === 1 && kept.get(0).skus[0] === SKU && kept.get(0).kind === 'row' && !kept.has(2), 'on the real grouping the browser indexer holds back the pair and nothing else'); }
    // re-running the server route over the stored pair leaves the record as it is
    const before = JSON.stringify(st.doc('Charm_Master_Index', SKU)), j2 = await runJob('job-2');
    ok(j2.job.status === 'done' && st.doc('Charm_Master_Index', SKU).pair.bodies === 2 && st.doc('Charm_Master_Index', SKU).widthPt === 95 && before.length > 0, 'a second server run over the pair keeps it a pair of two bodies');
    // a pair the server cannot fold is left as it was, and says so (the lone body must not take its place)
    const realRings = PDF.integrateRings, snapBlob = Buffer.from(st.blobs.get(pairAi).buf), snapRec = JSON.stringify(st.doc('Charm_Master_Index', SKU).pair) + st.doc('Charm_Master_Index', SKU).widthPt;
    PDF.integrateRings = c => { if (c.index === 1) throw new Error('forced failure'); return realRings(c); };
    let j3; try { j3 = await runJob('job-3'); } finally { PDF.integrateRings = realRings; }
    ok(j3.job.status === 'done' && j3.job.result.pairsKept.length === 1 && j3.job.result.pairsKept[0].sku === SKU && /forced failure/.test(j3.job.result.pairsKept[0].why), 'a pair the server could not fold is named in the job result');
    ok(Buffer.compare(st.blobs.get(pairAi).buf, snapBlob) === 0 && JSON.stringify(st.doc('Charm_Master_Index', SKU).pair) + st.doc('Charm_Master_Index', SKU).widthPt === snapRec, 'the pair\'s file and record are exactly what they were');
    // the stand-in library refuses to put one body over a pair (the server backstop), and null still takes the pair away
    const one = { sku: SKU, masterHash: 'ffeedd11', charmHash: 'x1', widthPt: 48, heightPt: 48, areaPt2: 1700, members: 3, holes: 1 };
    const r1 = await call({ op: 'masterPutIndex', entries: [one], masterHash: 'ffeedd11', masterName: 'other.ai' });
    ok(r1.pairKept && r1.pairKept.length === 1 && r1.pairKept[0].sku === SKU && /holds MISMATCHED_9001 as a pair/.test(r1.pairKept[0].reason) && r1.written === 0, 'the library names a SKU it left as a pair instead of writing one body over it');
    ok(st.doc('Charm_Master_Index', SKU).widthPt === 95 && st.doc('Charm_Master_Index', SKU).pair.bodies === 2, 'and the record is unchanged');
    const rOk = await call({ op: 'masterPutIndex', entries: [Object.assign({}, one, { sku: LONE })], masterHash: 'ffeedd11', masterName: 'other.ai' });
    ok(rOk.pairKept.length === 0 && rOk.written === 1, 'a design that is not a pair is written as always');
    // the script over the stored pair: --no-pairs would draw one body, so the pair is left as it was
    const blobBefore = Buffer.from(st.blobs.get(pairAi).buf), recBefore = JSON.stringify(st.doc('Charm_Master_Index', SKU).pair);
    const np = await IM.main(['node', 'x', file, '--origin', sorterOrigin, '--all', '--no-pairs'], log);
    ok(np.pairsKept.length === 1 && np.pairsKept[0].sku === SKU && Buffer.compare(st.blobs.get(pairAi).buf, blobBefore) === 0 && JSON.stringify(st.doc('Charm_Master_Index', SKU).pair) === recBefore && st.doc('Charm_Master_Index', SKU).widthPt === 95, '--no-pairs over a stored pair leaves its file and record, and the report names it');
    ok(lines.some(l => /NOT written, because this run drew them as one body/.test(l)), 'and the run says so in plain words');
    const yes = await IM.main(['node', 'x', file, '--origin', sorterOrigin, '--all'], log);
    ok(yes.pairsKept.length === 0 && st.doc('Charm_Master_Index', SKU).pair.bodies === 2 && st.doc('Charm_Master_Index', SKU).widthPt === 95, 'the same run with the pair layer writes the pair again');
  } finally { srv.close(); }
  console.log(`pairs-master: ${n} checks passed`);
})().catch(e => { console.error(e); process.exit(1); });
