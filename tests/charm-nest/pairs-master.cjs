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

// pieces
const pcs = Pair.piecesFor(line, pair);
ok(pcs.length === 2 && pcs[0].side === 'L' && pcs[0].bodyIndex === 0 && pcs[1].side === 'R' && pcs[1].bodyIndex === 1 && pcs.every(p => p.groupKey === '3912345678:4455' && p.of === 2), 'a mismatched line makes L then R');
ok(Pair.piecesFor(Object.assign({}, line, { quantity: 2 }), pair).length === 4, 'quantity 2 mismatched is two of each side');
const one = { outline: o1, members: [o1, e1], bbox: [0, 0, 20, 26] };
ok(Pair.piecesFor(line, one).length === 1, 'today a line makes quantity pieces (no doubling of earrings)');
const two = Object.assign({}, line, { quantity: 2 });
ok(Pair.piecesFor(two, one).length === 2 && Pair.piecesFor(two, one).every(p => p.side === null && p.bodyIndex === 0), 'a matching pair (quantity 2) has no sides');
ok(Pair.piecesFor({ receiptId: 1, transactionId: 2, spec: { quantity: 1, pieceCount: 2 } }, one).length === 2, 'spec.pieceCount wins');
ok(Pair.piecesFor({ receiptId: 1, transactionId: 2, quantity: 1, poolIds: ['a', 'b', 'c'] }, one).length === 3, 'the pool ids are the fact');
ok(Pair.piecesFor({ receiptId: 1, transactionId: 2, quantity: 1, form: 'earring-single' }, one).length === 1, 'a single earring is one piece');
ok(Pair.piecesFor({ receiptId: 1, transactionId: 2, quantity: 1, pieceCount: 3 }, one).length === 3, 'an explicit piece count wins');
ok(Pair.kindOf(line, pair) === 'mismatched' && Pair.kindOf(two, one) === 'pair' && Pair.kindOf(line, one) === 'single' && Pair.kindOf({ receiptId: 1, transactionId: 2, quantity: 1, form: 'earring-single' }, one) === 'single', 'kindOf');
ok(Pair.kindOf({ receiptId: 1, transactionId: 2, quantity: 3 }, one) === 'multi' && Pair.piecesFor({ receiptId: 1, transactionId: 2, quantity: 1, pieceCount: 3, discs: 3 }, one).length === 3, 'n discs (3 pieces) is multi');
ok(Pair.discsOf({ title: 'Disc necklace, 3 discs' }) === 3 && Pair.piecesFor({ receiptId: 1, transactionId: 2, quantity: 1, title: 'Disc necklace, 3 discs' }, one).length === 1, 'discs are information only: no extra pieces unless the intake says so');
ok(Pair.splitAcross([{ poolId: '3912345678_4455_1', s: 'A' }, { poolId: '3912345678_4455_2', s: 'B' }, { poolId: '3912345678_9999_1', s: 'A' }], p => p.s).length === 1, 'a group on two sheets is found');
console.log(`pairs-master: ${n} checks passed`);
