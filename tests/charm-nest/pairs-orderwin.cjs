// Pairs, mismatched pairs and multi-piece groups in the order window and every order list (Paul, 9 Oct 2026, rules R1 to R5; area 9, PAIRORDERWIN).
//   node tests/charm-nest/pairs-orderwin.cjs      (offline; nothing is written anywhere: this area only reads)
// A  OrderPieces.resolve / spread: side, groupKey, groupSize, kind on every piece; a mismatched pair makes 2 pieces per unit; split groups (R3); a line that is
//    not a mismatched pair resolves exactly as before
// B  the order window's side rows (piecesOf / scope) over the real bridge file, where it can run without the page
// C  search, shared-orders modal, piece dots, timeline per side
const path = require('path'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');
const Core = require(path.join(root, 'charm-nest-order-pieces.js'));
let ran = 0; const ok = name => { ran++; console.log('  ✓ ' + name); };

/* the contract's CharmNestPair.piecesFor for a mismatched pair design, as the fixtures need it (the real module is PAIRMASTER's) */
const planFor = (rid, tx, qty) => { const out = []; for (let u = 0; u < qty; u++) for (const [side, bodyIndex] of [['L', 0], ['R', 1]]) out.push({ side, bodyIndex, groupKey: `${rid}:${tx}`, n: out.length + 1, of: qty * 2 }); return out; };
const SHEET = { a: 'sheet-pow-a', b: 'sheet-pow-b' };
const sheet = (id, metal, n, poolIds) => ({ id, metal, sheetIndex: n, setId: 'set-pow', folder: `2026-10-09_${metal === 'gold' ? 'GF' : 'SS'}_Set-1_Sheet-${n}`, status: 'written', poolIds, updatedAt: 1000 + n });
const row = (rid, tx, copy, extra) => Object.assign({ poolId: `${rid}_${tx}_${copy}`, orderId: rid, transactionId: tx, lineKey: `${rid}_${tx}`, sku: 'MISMATCHED_7134', material: 'gold', copy, state: 'ready', sheetId: null, updatedAt: 1 }, extra || {});
const line = (rid, tx, sku, qty, extra) => Object.assign({ key: `${rid}_${tx}`, transactionId: tx, sku, title: sku, material: 'gold', quantity: qty, state: 'pooled', poolIds: [], spec: {}, problems: [] }, extra || {});
const pairOf = l => (/^MISMATCHED/.test(l.sku) ? planFor(String(l.key).split('_')[0], l.transactionId, Math.max(1, +l.quantity || 1)) : null);

/* ── A · the core ── */
{
  const rid = '4200000001', tx = '9001';
  // 1 · a mismatched pair line, nothing pooled yet: two pieces, Left then Right
  let ps = Core.resolve({ orderId: rid, lines: [line(rid, tx, 'MISMATCHED_7134', 1)], pools: [], sheets: [], sheetsKnown: true, pairOf });
  assert.equal(ps.length, 2); assert.deepEqual(ps.map(p => p.side), ['L', 'R']); assert.deepEqual(ps.map(p => p.sideLabel), ['Left', 'Right']);
  assert.deepEqual(ps.map(p => p.poolId), [`${rid}_${tx}_1`, `${rid}_${tx}_2`]); assert.deepEqual(ps.map(p => p.index), [1, 2]);
  assert.ok(ps.every(p => p.groupKey === `${rid}:${tx}` && p.groupSize === 2 && p.kind === 'mismatched' && p.lineKey === `${rid}_${tx}`));
  assert.deepEqual(ps.map(p => p.label), ['MISMATCHED 7134 · Left', 'MISMATCHED 7134 · Right']); ok('A1 a mismatched pair line makes a Left and a Right piece');

  // 2 · quantity 2: two pairs, four pieces, L R L R, one group of 4
  ps = Core.resolve({ orderId: rid, lines: [line(rid, tx, 'MISMATCHED_7134', 2)], pools: [], sheets: [], sheetsKnown: true, pairOf });
  assert.equal(ps.length, 4); assert.deepEqual(ps.map(p => p.side), ['L', 'R', 'L', 'R']); assert.ok(ps.every(p => p.groupSize === 4)); ok('A2 quantity 2 makes two pairs');

  // 3 · the pool rows' own fields win over the derived ones; an old row (no fields) is derived
  const pools = [row(rid, tx, 1, { side: 'R', bodyIndex: 1, groupKey: `${rid}:${tx}`, groupSize: 2 }), row(rid, tx, 2, { side: 'L', bodyIndex: 0, groupKey: `${rid}:${tx}`, groupSize: 2 })];
  ps = Core.resolve({ orderId: rid, lines: [line(rid, tx, 'MISMATCHED_7134', 1, { poolIds: pools.map(p => p.poolId) })], pools, sheets: [], sheetsKnown: true, pairOf });
  assert.deepEqual(ps.map(p => p.side), ['R', 'L']); ok('A3 the pool row says which side it is');
  ps = Core.resolve({ orderId: rid, lines: [line(rid, tx, 'MISMATCHED_7134', 1, { poolIds: pools.map(p => p.poolId) })], pools: pools.map(p => Object.assign({}, p, { side: undefined, bodyIndex: undefined, groupKey: undefined, groupSize: undefined })), sheets: [], sheetsKnown: true, pairOf });
  assert.deepEqual(ps.map(p => p.side), ['L', 'R']); ok('A3b an old row without fields is derived');
  // without the pair hook (the shared module is not on the page), a row that carries a side still says it
  ps = Core.resolve({ orderId: rid, lines: [line(rid, tx, 'MISMATCHED_7134', 1, { poolIds: pools.map(p => p.poolId) })], pools, sheets: [], sheetsKnown: true });
  assert.deepEqual(ps.map(p => p.side), ['R', 'L']); assert.ok(ps.every(p => p.kind === 'mismatched')); ok('A3c no hook: the row\'s own side is read');

  // 4 · Left on one sheet, Right on another: split
  const sheets = [sheet(SHEET.a, 'gold', 1, [`${rid}_${tx}_1`]), sheet(SHEET.b, 'silver', 1, [`${rid}_${tx}_2`])];
  const split = Core.resolve({ orderId: rid, lines: [line(rid, tx, 'MISMATCHED_7134', 1, { poolIds: [`${rid}_${tx}_1`, `${rid}_${tx}_2`] })], pools: [row(rid, tx, 1), row(rid, tx, 2)], sheets, sheetsKnown: true, pairOf });
  assert.deepEqual(split.map(p => [p.side, p.sheetLabel]), [['L', 'GF Sheet 1'], ['R', 'SS Sheet 1']]);
  const sp = Core.spread(split); assert.equal(sp.spreadAcross, true); assert.equal(sp.groups.length, 1); assert.equal(sp.splitGroups.length, 1);
  assert.deepEqual(sp.groups[0].sides, ['L', 'R']); assert.equal(sp.groups[0].size, 2); assert.equal(sp.groups[0].sheets.length, 2); assert.equal(sp.groups[0].kind, 'mismatched'); ok('A4 a pair on two sheets is a split group');

  // 5 · Left placed, Right not placed yet: split too (R3: "for any reason")
  const half = Core.resolve({ orderId: rid, lines: [line(rid, tx, 'MISMATCHED_7134', 1, { poolIds: [`${rid}_${tx}_1`, `${rid}_${tx}_2`] })], pools: [row(rid, tx, 1), row(rid, tx, 2)], sheets: [sheets[0]], sheetsKnown: true, pairOf });
  assert.deepEqual(half.map(p => p.nested), [true, false]); assert.equal(Core.spread(half).splitGroups.length, 1); ok('A5 one side placed and one not is a split group');
  // records not read yet: nothing is said
  const unread = Core.resolve({ orderId: rid, lines: [line(rid, tx, 'MISMATCHED_7134', 1)], pools: [], sheets: [], sheetsKnown: false, pairOf });
  assert.equal(Core.spread(unread).splitGroups.length, 0); ok('A5b nothing is said while the sheets are unread');

  // 6 · both on one sheet: not split
  const same = Core.resolve({ orderId: rid, lines: [line(rid, tx, 'MISMATCHED_7134', 1, { poolIds: [`${rid}_${tx}_1`, `${rid}_${tx}_2`] })], pools: [row(rid, tx, 1), row(rid, tx, 2)], sheets: [sheet(SHEET.a, 'gold', 1, [`${rid}_${tx}_1`, `${rid}_${tx}_2`])], sheetsKnown: true, pairOf });
  assert.equal(Core.spread(same).splitGroups.length, 0); assert.equal(Core.spread(same).spreadAcross, false); ok('A6 both sides on one sheet: together');
}

/* ── an old record: a mismatched pair pooled before pairs were tracked is ONE glued piece on its sheet, never a falsely split pair ── */
{
  const rid = '4200000003', tx = '9004';
  const old = [row(rid, tx, 1, { sheetId: SHEET.a })];
  const ps = Core.resolve({ orderId: rid, lines: [line(rid, tx, 'MISMATCHED_7134', 1, { poolIds: [`${rid}_${tx}_1`] })], pools: old, sheets: [sheet(SHEET.a, 'gold', 1, [`${rid}_${tx}_1`])], sheetsKnown: true, pairOf });
  assert.equal(ps.length, 1); assert.equal(ps[0].glued, true); assert.equal(ps[0].side, null); assert.equal(ps[0].kind, 'mismatched'); assert.equal(ps[0].nested, true); assert.equal(ps[0].label, 'MISMATCHED 7134');
  assert.equal(Core.spread(ps).splitGroups.length, 0); ok('A9 an old glued record is one piece on its sheet, not a split pair');
  // a new pool with only its Left row so far (it carries a side): the Right is missing, not glued
  const half = Core.resolve({ orderId: rid, lines: [line(rid, tx, 'MISMATCHED_7134', 1, { poolIds: [`${rid}_${tx}_1`] })], pools: [row(rid, tx, 1, { side: 'L', sheetId: SHEET.a })], sheets: [sheet(SHEET.a, 'gold', 1, [`${rid}_${tx}_1`])], sheetsKnown: true, pairOf });
  assert.equal(half.length, 2); assert.equal(half[0].glued, false); assert.deepEqual(half.map(p => p.nested), [true, false]); assert.equal(Core.spread(half).splitGroups.length, 1); ok('A9b a Left row with no Right yet: the pair is split, not glued');
}

/* ── a line that is not a mismatched pair resolves exactly as before ── */
{
  const rid = '4200000002', tx = '9002', tx2 = '9003';
  const lines = [line(rid, tx, 'STUD_HEART', 1), line(rid, tx2, 'DISC_NECKLACE', 3)];
  const pools = [row(rid, tx, 1, { sku: 'STUD_HEART' }), row(rid, tx2, 1, { sku: 'DISC_NECKLACE' }), row(rid, tx2, 2, { sku: 'DISC_NECKLACE' }), row(rid, tx2, 3, { sku: 'DISC_NECKLACE' })];
  const sheets = [sheet(SHEET.a, 'gold', 1, [`${rid}_${tx}_1`, `${rid}_${tx2}_1`, `${rid}_${tx2}_2`]), sheet(SHEET.b, 'gold', 2, [`${rid}_${tx2}_3`])];
  const base = Core.resolve({ orderId: rid, lines, pools, sheets, sheetsKnown: true });
  const hooked = Core.resolve({ orderId: rid, lines, pools, sheets, sheetsKnown: true, pairOf, kindOf: () => null });
  const strip = ps => ps.map(p => { const q = Object.assign({}, p); for (const k of ['side', 'sideLabel', 'glued', 'bodyIndex', 'groupKey', 'groupSize', 'kind']) delete q[k]; delete q.line; delete q.pool; return q; });
  assert.deepEqual(strip(hooked), strip(base)); assert.equal(base.length, 4); ok('A7 singles, matching pairs and discs: the same pieces with or without the pair hook');
  assert.ok(base.every(p => p.side === null && p.sideLabel === '' && p.kind === null)); ok('A7b no side, no kind on them');
  // discs: one group of 3, split over two sheets
  const sp = Core.spread(base); const disc = sp.groups.find(g => g.lineKey === `${rid}_${tx2}`);
  assert.equal(disc.size, 3); assert.equal(disc.split, true); assert.equal(sp.groups.find(g => g.lineKey === `${rid}_${tx}`).split, false); assert.equal(sp.splitGroups.length, 1); ok('A8 a disc necklace on two sheets is a split group, a single is not');
  assert.ok(base.filter(p => p.lineKey === `${rid}_${tx2}`).every(p => p.groupSize === 3)); ok('A8b every disc knows its group has 3');
}

console.log(`\n${ran} checks passed`);
