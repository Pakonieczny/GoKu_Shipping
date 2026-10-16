// PIECES GATE, part 1 (model): EVERY PIECE of a multi-piece order line is its own piece, offline and pure (no page, no network, nothing live).
//
// Paul, 16 Oct 2026, order 4195059483 (two pieces, ONE line "CORGI 34728 · SS · ×2", personalisation "N/A"): "Customer ordered two of the same Charm, but the engraving only shows one,
// sometimes customers will request different engraving for each piece, even though the two pieces are the same. ALL multipiece orders must always show each individual piece with respect to
// all Engravings and Monograms."  Before: a line of quantity 2 of one charm had ONE job under the line key (one card, one set of words, one Approve, one set of seals) because only ears
// (L/R) and discs (D1..Dn) were split into slots.  Rules proved here (R1-R6 of /tmp/claude-.../scratchpad/pc-brief.md):
//   A  the slot model (charm-nest-engrave-sides.js): a line of quantity N (single charm 2/3/6, a bar set x2, ears x2, discs x2) is N pieces, one slot each, one pool id each; quantity 1 is byte for byte
//      what it was (golden against the commit named in _piecesNames.cjs); a quantity that changes after jobs exist keeps every slot and every approval
//   B  per-piece words: a note with N plain parts gives them in order, any other note gives the whole text to every piece (and says so)
//   C  the cards and the order's list (charm-nest-engrave-card.js, charm-nest-engrave-order.js): one card per piece titled "Piece i of n", its own Approve; the order is worst-first over its pieces;
//      approving one never approves another; Back/Next reach every piece, nothing is skipped; a mixed order (monogram x2 + bar x3 + plain x2); quantity-1 lines are byte for byte as before
//   D  seals/summary/records per piece (Sides.summary, pieceRecords); a migrated whole-line approval is never lost and never asked for twice
//   E  every module that reads a job key agrees about the piece's slot
//   node tests/charm-nest/pieces-model.cjs        (the slot spelling and function names the gate expects live in _piecesNames.cjs)
// What a pass does NOT prove: the page (job creation in charm-nest-bridge.js, the windows): tests/charm-nest/pieces-flow.cjs, in Chromium on the offline fake. Nothing here touches a live service.
'use strict';
const path = require('path'), fs = require('fs'), os = require('os'), vm = require('vm'), assert = require('assert/strict'), { execSync } = require('child_process');
const root = path.join(__dirname, '../..');
const N = require('./_piecesNames.cjs');
const F = require('./pairs-fixtures.cjs');
const Pair = require(path.join(root, 'charm-nest-pair.js'));

// ── the pristine modules, for "quantity 1 is byte for byte what it was" ─────────────────────────────────────────────────────────────────────────────
function baseDir() {
  const dir = path.join(os.tmpdir(), 'cn-pieces-base-' + N.BASE_COMMIT);
  if (!fs.existsSync(path.join(dir, 'charm-nest-engrave-sides.js'))) { fs.mkdirSync(dir, { recursive: true }); execSync(`git archive ${N.BASE_COMMIT} -- ':(glob)charm-nest-*.js' | tar -x -C ${dir}`, { cwd: root, shell: '/bin/bash' }); }
  return dir;
}
const BASE = baseDir();
const sidesAt = dir => require(path.join(dir, 'charm-nest-engrave-sides.js'));
const Sides = sidesAt(root), SidesBase = sidesAt(BASE);
const Seals = require(path.join(root, 'charm-nest-engraving-seals.js'));

let passed = 0, failed = 0; const fails = [];
const t = (id, name, fn) => { try { fn(); passed++; console.log('  PASS ' + id + ' ' + name); } catch (e) { failed++; const msg = String(e && e.message || e).split('\n').slice(0, 3).join(' | '); fails.push(id + ' ' + name + ' :: ' + msg); console.log('  FAIL ' + id + ' ' + name + '\n       ' + msg); } };

// ── the worlds: orders on the repo's pair fixtures (real pool ids rid_tx_n, real sides) ──────────────────────────────────────────────────────────────
const lines = {
  s2: { n: 10, kind: 'single', qty: 2, on: 'sh-gf1' }, s3: { n: 11, kind: 'single', qty: 3, on: 'sh-gf1' }, s6: { n: 12, kind: 'single', qty: 6, on: 'sh-gf1' }, s1: { n: 13, kind: 'single', qty: 1, on: 'sh-gf1' },
  e1: { n: 20, kind: 'pair', qty: 1, on: 'sh-gf1' }, e2: { n: 21, kind: 'pair', qty: 2, on: 'sh-gf1' }, m1: { n: 22, kind: 'mismatched', qty: 1, on: 'sh-gf1' }, m2: { n: 23, kind: 'mismatched', qty: 2, on: 'sh-gf1' },
  d1: { n: 30, kind: 'discs', discs: 3, qty: 1, on: 'sh-gf1' }, d2: { n: 31, kind: 'discs', discs: 3, qty: 2, on: 'sh-gf1' },
  b1: { n: 40, kind: 'discs', discs: 2, qty: 1, on: 'sh-gf1' }, b2: { n: 41, kind: 'discs', discs: 2, qty: 2, on: 'sh-gf1' }
};
const W = F.world({ orders: Object.keys(lines).map((k, i) => ({ rid: F.rid(i + 1), lines: [lines[k]] })) });
const rowOf = (k, o) => {
  const ord = W.orders.find(x => x.lines.some(l => l.n === lines[k].n)), l = ord.lines.find(x => x.n === lines[k].n);
  const row = { key: F.lineKey(ord.rid, F.tx(l.n)), poolIds: l.pieces.map(p => p.poolId), order: { receiptId: ord.rid }, line: { transactionId: F.tx(l.n), sku: l.sku, title: l.line.title, quantity: l.qty, variations: [{ name: 'Metal choice', value: 'Silver' }] },
    spec: { designSku: l.sku, form: l.form, quantity: l.qty, personalization: ['Mom'], engraveCandidate: true }, state: 'pooled', material: 'silver', problems: [] };
  if (k[0] === 'b') { // a SET of two bars (listing 532401548): two different designs, two pieces per unit
    row.line.sku = 'BAR_1548'; row.spec.designSku = 'BAR_1548'; row.spec.form = 'necklace';
    row.spec.pieceCount = l.qty * 2; row.spec.pair = { kind: 'multi', set: { designs: ['BAR_1548', 'SKINNY_0191'], source: 'rule' } };
  }
  return Object.assign(row, o || {});
};
const poolMap = new Map(W.pool.map(p => [p.poolId, p]));
const ctx = { poolRow: id => poolMap.get(id), charmOf: id => { const r = poolMap.get(id); return r ? F.charmOf(r.sku) : null; }, entryFor: sku => { try { return F.entryOf(sku); } catch (_) { return null; } }, pair: Pair, mergeSeals: (...r) => Seals.merge(...r) };
const ROW = Object.fromEntries(Object.keys(lines).map(k => [k, rowOf(k)]));
const copiesOf = (S, row, slot) => (slot ? S.idsOfSlot(ctx, row, slot) : row.poolIds.slice());

// ═══ A · the slot model ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
// every piece of a counted line: its own slot, its own pool id, a key that reads back, a name that tells it from its sisters
function onePiecePerSlot(k, pieces, o) {
  const row = ROW[k], pl = Sides.plan(ctx, row, []);
  assert.equal(row.poolIds.length, pieces, `fixture: ${pieces} pool ids`);
  assert.equal(pl.split, true, `a line of ${pieces} pieces is cut into pieces (plan.split)`);
  assert.equal(pl.slots.length, pieces, `plan gives ${pieces} slots, got ${JSON.stringify(pl.slots)}`);
  assert.equal(new Set(pl.slots).size, pieces, 'the slots are distinct');
  for (const s of pl.slots) assert.equal(Sides.idsOfSlot(ctx, row, s).length, 1, `slot ${s} holds exactly ONE pool id`);
  assert.equal(new Set(pl.slots.flatMap(s => Sides.idsOfSlot(ctx, row, s))).size, pieces, 'every pool id is in exactly one slot');
  const keys = pl.slots.map(s => Sides.jobKey(row.key, s));
  assert.equal(new Set(keys).size, pieces, 'one job key per piece');
  for (const [i, s] of pl.slots.entries()) { const p = Sides.parseKey(keys[i]); assert.deepEqual({ rowKey: p.rowKey, slot: p.slot }, { rowKey: row.key, slot: s }, `job key of ${s} reads back (parseKey)`); }
  const shape = typeof Sides.shape === 'function' ? Sides.shape(pl.slots) : undefined, per = shape && shape.per ? shape.per : pieces;   // (contract section 2: tagOf(slot, of, shape): `of` = pieces of the line, or the discs of a unit)
  const tags = pl.slots.map(s => Sides.tagOf(s, pieces, shape)), labels = pl.slots.map(s => Sides.labelOf(s, shape)); void per;
  assert.equal(new Set(tags).size, pieces, 'each piece has its own tag: ' + JSON.stringify(tags)); assert.ok(tags.every(Boolean), 'no piece has an empty tag: ' + JSON.stringify(tags));
  assert.equal(new Set(labels).size, pieces, 'each piece has its own label: ' + JSON.stringify(labels)); assert.ok(labels.every(Boolean), 'no piece has an empty label: ' + JSON.stringify(labels));
  if (o && o.words) pl.slots.forEach((s, i) => assert.ok(N.pieceWord(i + 1, pieces).test(tags[i] + ' ' + labels[i]), `piece ${i + 1} of ${pieces} reads "Piece ${i + 1} of ${pieces}": ${tags[i]} / ${labels[i]}`));
  return pl;
}
t('A1', 'a single charm x2 is TWO pieces: two slots, one pool id each, "Piece 1 of 2" / "Piece 2 of 2" (Paul\'s order 4195059483)', () => onePiecePerSlot('s2', 2, { words: true }));
t('A2', 'a single charm x3 is three pieces', () => onePiecePerSlot('s3', 3, { words: true }));
t('A3', 'a single charm x6 is six pieces', () => onePiecePerSlot('s6', 6, { words: true }));
t('A4', 'a set of two bars (listing 532401548) x2 is FOUR pieces, each bar of each set its own, and each keeps its own design', () => {
  const pl = onePiecePerSlot('b2', 4), row = ROW.b2;
  // the design of a piece is the design of its place in the set: copies 1,3 are the large bar, 2,4 the small bar
  const designOf = s => { const base = typeof Sides.baseSlot === 'function' ? Sides.baseSlot(s) : s, sp = Sides.setPieceOf(row, base); return sp && sp.sku; };   // (a unit-qualified slot keeps its place in the set: D1.2 is the large bar again)
  const want = ['BAR_1548', 'SKINNY_0191', 'BAR_1548', 'SKINNY_0191'];
  assert.deepEqual(pl.slots.map(s => ({ s, id: Sides.idsOfSlot(ctx, row, s)[0] })).sort((a, b) => F.ids.poolId ? Number(a.id.split('_').pop()) - Number(b.id.split('_').pop()) : 0).map(x => designOf(x.s)), want, 'the piece of copy i is the large bar when i is odd, the small bar when even');
});
t('A5', 'ears: a pair x1 is still exactly L and R; a pair x2 is FOUR pieces (two Left, two Right), each with its own slot, one pool id, and the ear its pool id says', () => {
  assert.deepEqual(Sides.plan(ctx, ROW.e1, []).slots, ['L', 'R'], 'quantity 1 pair: L, R (unchanged)');
  const pl = onePiecePerSlot('e2', 4);
  for (const s of pl.slots) { const id = Sides.idsOfSlot(ctx, ROW.e2, s)[0], side = poolMap.get(id).side; assert.equal(String(s)[0], side, `slot ${s} holds ${id}, an ${side} piece: the slot keeps its ear`); }
  assert.equal(pl.slots.filter(s => String(s)[0] === 'L').length, 2); assert.equal(pl.slots.filter(s => String(s)[0] === 'R').length, 2);
});
t('A5b', 'a mismatched pair x1 is L and R, x2 is four pieces', () => { assert.deepEqual(Sides.plan(ctx, ROW.m1, []).slots, ['L', 'R']); onePiecePerSlot('m2', 4); });
t('A6', 'discs: 3 discs x1 is D1..D3 (unchanged); 3 discs x2 is SIX pieces, one pool id each', () => {
  assert.deepEqual(Sides.plan(ctx, ROW.d1, []).slots, ['D1', 'D2', 'D3'], 'quantity 1: D1..D3 as before');
  onePiecePerSlot('d2', 6);
});
t('A7', 'quantity 1 is byte for byte what it was: single charm, pair, mismatched pair, discs, set of bars (plan, slots, keys, labels, tags, slotOfId of every piece)', () => {
  for (const k of ['s1', 'e1', 'm1', 'd1', 'b1']) {
    const row = ROW[k], a = Sides.plan(ctx, row, []), b = SidesBase.plan(ctx, row, []);
    assert.deepEqual({ split: a.split, slots: a.slots, of: [...a.of] }, { split: b.split, slots: b.slots, of: [...b.of] }, `${k}: plan unchanged`);
    for (const s of b.slots) { assert.equal(Sides.jobKey(row.key, s), SidesBase.jobKey(row.key, s)); assert.equal(Sides.tagOf(s, b.slots.length), SidesBase.tagOf(s, b.slots.length)); assert.equal(Sides.labelOf(s), SidesBase.labelOf(s)); assert.equal(Sides.earOf(s), SidesBase.earOf(s)); assert.deepEqual(Sides.idsOfSlot(ctx, row, s), SidesBase.idsOfSlot(ctx, row, s)); }
    for (const id of row.poolIds) assert.equal(Sides.slotOfId(ctx, row, id), SidesBase.slotOfId(ctx, row, id), `${k}: slotOfId(${id})`);
  }
  assert.equal(Sides.jobKey('4190000001_5000000013', null), '4190000001_5000000013', 'a line with no slot: the job is keyed by the line, byte for byte');
  assert.equal(Sides.jobKey('k', null, 'front'), 'k~front'); assert.deepEqual(Sides.parseKey('k'), { rowKey: 'k', slot: null }); assert.deepEqual(Sides.parseKey('k#D2'), { rowKey: 'k', slot: 'D2' }); assert.deepEqual(Sides.parseKey('k#L~front'), { rowKey: 'k', slot: 'L', face: 'front' });
});
t('A8', 'quantity 1 of a plain line has NO slot: slotOfId null, plan {split false, slots [null]}', () => { const p = Sides.plan(ctx, ROW.s1, []); assert.deepEqual({ split: p.split, slots: p.slots }, { split: false, slots: [null] }); assert.equal(Sides.slotOfId(ctx, ROW.s1, ROW.s1.poolIds[0]), null); });
t('A9', 'the slot of a copy never depends on the quantity: copy 1 and 2 keep their slots when the line grows from 2 to 3 (so a job that exists keeps its piece)', () => {
  const two = ROW.s2, three = rowOf('s2', { poolIds: [...ROW.s2.poolIds, ROW.s2.poolIds[0].replace(/_1$/, '_3')], spec: Object.assign({}, ROW.s2.spec, { quantity: 3 }), line: Object.assign({}, ROW.s2.line, { quantity: 3 }) });
  const a = Sides.plan(ctx, two, []), b = Sides.plan(ctx, three, []);
  assert.equal(a.split && b.split, true, 'both are cut into pieces'); assert.equal(b.slots.length, 3);
  for (const id of two.poolIds) assert.equal(b.of.get(id), a.of.get(id), `${id} keeps its slot when the quantity grows`);
  assert.equal(a.of.get(two.poolIds[0]), N.copySlot(1), 'copy 1 is the first piece: ' + a.of.get(two.poolIds[0]));
});
t('A10', 'quantity changed AFTER jobs exist: jobs of the pieces that exist keep their slots (approved ones never re-cut); a line that shrinks keeps the approved piece\'s slot; a line that grows gets the new piece', () => {
  const [c1, c2] = [N.copySlot(1), N.copySlot(2)], row = ROW.s2;
  const j1 = { key: Sides.jobKey(row.key, c1), slot: c1, rowKey: row.key, state: 'approved', copies: [row.poolIds[0]] }, j2 = { key: Sides.jobKey(row.key, c2), slot: c2, rowKey: row.key, state: 'review', copies: [row.poolIds[1]] };
  const shrunk = rowOf('s2', { poolIds: [row.poolIds[0]], spec: Object.assign({}, row.spec, { quantity: 1 }), line: Object.assign({}, row.line, { quantity: 1 }) });
  const pk = Sides.plan(ctx, shrunk, [j1, j2]);
  assert.equal(pk.split, true, 'jobs with slots exist: the line stays cut'); assert.ok(pk.slots.includes(c1), 'the approved piece\'s slot is kept: ' + JSON.stringify(pk.slots)); assert.ok(pk.slots.includes(c2), 'the other existing slot is kept for the bridge to archive, never silently re-cut');
  const grown = rowOf('s2', { poolIds: [...row.poolIds, row.poolIds[0].replace(/_1$/, '_3')], spec: Object.assign({}, row.spec, { quantity: 3 }), line: Object.assign({}, row.line, { quantity: 3 }) });
  const pg = Sides.plan(ctx, grown, [j1, j2]); assert.deepEqual(pg.slots, [c1, c2, N.copySlot(3)], 'the new piece appears after the two that exist');
});

t('A11', 'pieceList: ONE entry per piece even where nothing is engraved (the Order window\'s ITS PIECE rows): x2 plain line = 2 entries "Piece 1/2"; x1 = 1 entry, no slot', () => {
  assert.equal(typeof Sides.pieceList, 'function', 'Sides.pieceList is missing');
  const pl = Sides.pieceList(ctx, ROW.s2); assert.equal(pl.length, 2); assert.deepEqual(pl.map(x => [x.index, x.of, x.slot, x.poolId]), [[1, 2, N.copySlot(1), ROW.s2.poolIds[0]], [2, 2, N.copySlot(2), ROW.s2.poolIds[1]]]); assert.ok(pl.every((x, i) => N.pieceWord(i + 1, 2).test(x.tag + ' ' + x.label)), pl.map(x => x.tag + '/' + x.label).join(' | '));
  const one = Sides.pieceList(ctx, ROW.s1); assert.equal(one.length, 1); assert.deepEqual([one[0].slot, one[0].of, one[0].index], [null, 1, 1]);
  assert.equal(Sides.pieceList(ctx, ROW.s6).length, 6); assert.equal(Sides.pieceList(ctx, ROW.e2).length, 4, 'ears x2: four pieces'); assert.deepEqual(Sides.pieceList(ctx, ROW.e2).map(x => x.slot), ['L', 'R', 'L.2', 'R.2']);
  assert.deepEqual(Sides.pieceList(ctx, ROW.d2).map(x => x.slot), ['D1', 'D2', 'D3', 'D1.2', 'D2.2', 'D3.2']); assert.deepEqual(Sides.pieceList(ctx, ROW.d1).map(x => x.slot), ['D1', 'D2', 'D3']);
  assert.equal(new Set(Sides.pieceList(ctx, ROW.b2).map(x => x.label)).size, 4, 'a set of bars x2: four distinct names');
});
t('A12', 'slot grammar: slotInfo / slotRank / isSlot (C1, L, R, D2, L.2, D2.3)', () => {
  assert.equal(typeof Sides.slotInfo, 'function', 'Sides.slotInfo is missing');
  const i = Sides.slotInfo(N.copySlot(2)); assert.deepEqual([i.kind, i.n, i.unit], ['copy', 2, 2]); const d = Sides.slotInfo('D2.3'); assert.deepEqual([d.kind, d.base, d.n, d.unit], ['disc', 'D2', 2, 3]); const l = Sides.slotInfo('L.2'); assert.deepEqual([l.kind, l.side, l.unit], ['ear', 'L', 2]); assert.equal(Sides.slotInfo('L').unit, 1); assert.equal(Sides.slotInfo('nope'), null);
  const order = ['R.2', 'D1.2', 'L.2', 'D2', 'R', 'L', 'D1'].sort((a, b) => Sides.slotRank(a) - Sides.slotRank(b)); assert.deepEqual(order, ['L', 'R', 'D1', 'D2', 'L.2', 'D1.2', 'R.2'].filter(x => order.includes(x)).sort((a, b) => Sides.slotRank(a) - Sides.slotRank(b)), 'unit-major, then L R D1 D2');
  assert.ok(Sides.slotRank('L.2') > Sides.slotRank('R') && Sides.slotRank('R') > Sides.slotRank('L') && Sides.slotRank('D1.2') > Sides.slotRank('D3'), 'unit 2 after all of unit 1'); assert.ok(Sides.slotRank(null) >= 9999);
  for (const ok of ['C1', 'C12', 'L', 'R', 'D3', 'L.2', 'D1.12']) assert.equal(Sides.isSlot(ok), true, ok); for (const no of ['X1', 'C', '', 'LR', 'D.2']) assert.equal(Sides.isSlot(no), false, JSON.stringify(no)); assert.ok(Sides.SLOT.test('C3') && Sides.SLOT.test('R.2'));
});
t('A13', 'unit-qualified names: ears x2 "Left" / "Left · pair 2", discs x2 "Disc 1 · set 2"; every piece of every multi line has its own label AND tag', () => {
  const shape = Sides.shape(['L', 'R', 'L.2', 'R.2']); assert.deepEqual([shape.units, shape.per, shape.total, shape.kind, shape.multi], [2, 2, 4, 'ear', true]);
  assert.match(Sides.labelOf('L.2', shape), /pair 2/i); assert.match(Sides.tagOf('R.2', 2, shape), /RIGHT EAR.*PAIR 2/); assert.match(Sides.tagOf('D1.2', 3, Sides.shape(['D1', 'D2', 'D3', 'D1.2', 'D2.2', 'D3.2'])), /DISC 1 of 3.*SET 2/);
  assert.equal(new Set(['L', 'R', 'L.2', 'R.2'].map(s => Sides.tagOf(s, 2, shape))).size, 4, 'four distinct tags'); assert.equal(Sides.shape(['L', 'R']).multi, false); assert.equal(Sides.labelOf('L'), 'Left'); assert.equal(Sides.tagOf('D2', 3), 'DISC 2 of 3');
});

const lineRow = (key, rid, extra) => Object.assign({ key, order: { receiptId: rid || '1001' }, line: { sku: 'CORGI_34728', transactionId: '1' }, spec: { designSku: 'CORGI_34728', quantity: 1 }, state: 'pooled', poolIds: [key + '_1'], material: 'silver' }, extra || {});
t('A14', 'a pinned count (quantity 2 but the line was pooled with 3 pieces, spec.pieceCount 3) is three pieces C1..C3, never a line that is not split', () => {
  const row = rowOf('s2', { poolIds: [...ROW.s2.poolIds, ROW.s2.poolIds[0].replace(/_1$/, '_3')], spec: Object.assign({}, ROW.s2.spec, { quantity: 2, pieceCount: 3 }) });
  const pl = Sides.plan(ctx, row, []); assert.equal(pl.split, true); assert.deepEqual(pl.slots, [N.copySlot(1), N.copySlot(2), N.copySlot(3)], 'one slot per pool id'); for (const s of pl.slots) assert.equal(Sides.idsOfSlot(ctx, row, s).length, 1, s);
});
t('A15', 'a pool id with no copy number is never lost: the line still lists it (the null slot), and quantity-1 lines never grow a slot', () => {
  const row = rowOf('s2', { poolIds: [...ROW.s2.poolIds, 'odd-piece-without-copy-number'] }), pl = Sides.plan(ctx, row, []);
  assert.ok(pl.split && pl.slots.includes(null), 'the piece with no copy number keeps a null-slot job: ' + JSON.stringify(pl.slots)); assert.equal(pl.of.get('odd-piece-without-copy-number'), null);
});
// ═══ B · per-piece words (contract section 5: wordsForPieces(spec, slots, o) -> { ok, words: { [slot]: text }, how, sure } | null) ═══════════════════════════════════════════════
const slotsN = n => Array.from({ length: n }, (_, i) => N.copySlot(i + 1));
const wf = () => { assert.equal(typeof Sides.wordsForPieces, 'function', 'CharmNestEngraveSides.wordsForPieces is missing: the buyer\'s words cannot be assigned piece by piece'); return Sides.wordsForPieces; };
const wordsOf = (note, n, extra) => { const r = wf()(Object.assign({ personalization: Array.isArray(note) ? note : [note] }, extra || {}), slotsN(n), {}); return r; };
const asList = (r, n) => slotsN(n).map(sl => r && r.words ? r.words[sl] : undefined);
t('B1', 'a note with N clear parts gives them in order (lines, "/", "&", ",", "and", numbered): "Anna / Ben" x2 is Anna on piece 1, Ben on piece 2', () => {
  for (const [note, want] of [['Anna\nBen', ['Anna', 'Ben']], ['Anna / Ben', ['Anna', 'Ben']], ['Anna & Ben', ['Anna', 'Ben']], ['Anna, Ben', ['Anna', 'Ben']], ['Anna and Ben', ['Anna', 'Ben']], ['1: Anna 2: Ben', ['Anna', 'Ben']], ['Piece 1: Anna, Piece 2: Ben', ['Anna', 'Ben']], ['2: Ben, 1: Anna', ['Anna', 'Ben']], ['Ben / Anna', ['Ben', 'Anna']]]) {
    const r = wordsOf(note, 2); assert.ok(r && r.ok !== false, 'a clear split of ' + JSON.stringify(note) + ': ' + JSON.stringify(r)); assert.deepEqual(asList(r, 2), want, JSON.stringify(note));
  }
  assert.deepEqual(asList(wordsOf('A / B / C', 3), 3), ['A', 'B', 'C']); assert.deepEqual(asList(wordsOf(['Anna', 'Ben'], 2), 2), ['Anna', 'Ben'], 'two entries of the personalisation field');
});
t('B2', 'numbered, lines and "same on all" are SURE (the pieces go ready); a list or letters ("Mom & Dad") are proposals the person confirms', () => {
  assert.equal(wordsOf('1: Anna 2: Ben', 2).sure, true); assert.equal(wordsOf('Anna\nBen', 2).sure, true); assert.equal(wordsOf('Both: Mom', 2).sure, true);
  assert.equal(wordsOf('Mom & Dad', 2).sure, false, 'ambiguous: a proposal, not a decision'); { const r = wordsOf('J Q', 2); assert.ok(!r || r.sure === false, 'two bare letters are a proposal at most (or no split: the whole text on each piece): ' + JSON.stringify(r)); }
});
t('B3', 'a note that says it is the same on every piece ("Both: Mom", "Mom on each") is the same words on each piece (how "same")', () => { for (const [note, n] of [['Both: Mom', 2], ['Mom on each', 3], ['Same on all: Mom', 2]]) { const r = wordsOf(note, n); assert.equal(r && r.how, 'same', JSON.stringify(note)); assert.deepEqual(asList(r, n), Array.from({ length: n }, () => 'Mom')); } });
t('B4', 'a note that is NOT clearly N parts (one bare word, a different count, other voices on the order) gives no per-piece split: null, so every piece gets the whole reading', () => {
  for (const [note, n, extra] of [['Mom', 2], ['Name One', 6], ['A, B, C', 2], ['Anna, Ben', 3], ['Mom and Dad forever', 2], ['Anna / Ben', 2, { buyerMessage: 'please make the first one smaller' }], ['Anna / Ben', 2, { staffNote: 'both the same' }]]) {
    const r = wordsOf(note, n, extra); assert.ok(!r || r.ok === false || r.how === 'same' || r.how === 'whole', JSON.stringify([note, n, extra]) + ' must not be split piece by piece: ' + JSON.stringify(r));
    if (r && r.ok !== false && r.how !== 'same' && r.how !== 'whole') assert.fail('split a note that is not N parts');
  }
});
t('B5', 'no words at all ("N/A", empty, "none") gives no words to any piece (Paul\'s order: personalisation N/A: a person types them)', () => { for (const note of ['N/A', '', 'n/a', 'None']) { const r = wordsOf(note, 2); assert.ok(!r || r.ok === false || asList(r, 2).every(x => !String(x || '').trim()), JSON.stringify(note) + ' -> ' + JSON.stringify(r)); } });
t('B6', 'words are per PIECE: line-level words are the fallback of every piece with none of its own, an own entry wins (Setup/Faces: engravingOverride.pieces[slot])', () => {
  const Faces = require(path.join(root, 'charm-nest-engrave-faces.js')), row = lineRow('KW', '5001', { poolIds: ['KW_1', 'KW_2'], spec: { designSku: 'CORGI_34728', quantity: 2, engraveCandidate: true, engraving: { v: 1, mode: 'back', back: { lines: ['Mom'], text: 'Mom' } } }, engravingOverride: { mode: 'back', back: { text: 'Mom' }, pieces: { [N.copySlot(2)]: { back: { text: 'Ben' } } } } });
  const a = Faces.setupOf(row, undefined, N.copySlot(1)), b = Faces.setupOf(row, undefined, N.copySlot(2));
  assert.equal(a && a.back && a.back.text, 'Mom', 'piece 1 has no words of its own: the line\'s'); assert.equal(b && b.back && b.back.text, 'Ben', 'piece 2: its own entry');
});

// ═══ C · cards and the order's pieces ═════════════════════════════════════════════════════════════════════════════════════════════════════════════
function world(dir, jobsByLine, rowsArr) {
  const sb = { console, setTimeout, clearTimeout, JSON, Math, Date, Promise, Map, Set, WeakMap, Array, Object, String, Number, RegExp };
  sb.self = sb; sb.window = sb; vm.createContext(sb);
  for (const f of ['charm-nest-engrave-faces.js', 'charm-nest-engraving-seals.js', 'charm-nest-engrave-sides.js', 'charm-nest-engrave-card.js', 'charm-nest-engrave-order.js']) vm.runInContext(fs.readFileSync(path.join(dir, f), 'utf8'), sb, { filename: f });
  sb.Seal = { BASE_SIZE: 50, html: s => `<span class="seal" data-by="${s.by}"></span>` };
  sb.Engrave = { jobsOf: row => jobsByLine[(row.parentRow || row).key] || [], isWorking: () => false };
  sb.Orders = { rows: () => rowsArr || [] };
  return sb;
}
const jobFor = (row, slot, state, text, extra) => Object.assign({ key: Sides.jobKey(row.key, slot), slot: slot || undefined, rowKey: slot ? row.key : undefined, row, state, text, lines: text ? text.split('\n') : [], copies: slot ? Sides.idsOfSlot(ctx, row, slot) : row.poolIds.slice(), fit: state === 'review' ? { glyphs: [] } : null, view: state === 'review' ? { members: [] } : null, verify: { geometry: { ok: true } }, backs: [] }, extra || {});
const plain = o => JSON.parse(JSON.stringify(o, (k, v) => (k === 'row' || k === 'job' || k === 'jobs' || k === 'piece' || k === 'parentRow' ? undefined : v)));
const lean = o => JSON.parse(JSON.stringify(o, (k, v) => (v === '' || v === null ? undefined : v)));   // (an ADDED property that is empty on a quantity-1 piece is no change: the pristine model has none)
const firstDiff = (a, b, p = '') => { if (JSON.stringify(a) === JSON.stringify(b)) return ''; if (a && b && typeof a === 'object' && typeof b === 'object') { for (const k of new Set([...Object.keys(a), ...Object.keys(b)])) { const d = firstDiff(a[k], b[k], p + '/' + k); if (d) return d; } } return p + ': ' + String(JSON.stringify(a)).slice(0, 90) + '  vs (pristine)  ' + String(JSON.stringify(b)).slice(0, 90); };
const eqJ = (a, b, m) => { const d = firstDiff(JSON.parse(JSON.stringify(a === undefined ? null : a)), JSON.parse(JSON.stringify(b === undefined ? null : b))); if (d) assert.fail((m || 'differs') + ' ' + d); };   // (values made in the vm sandbox have another realm's prototypes: compare as data)
const strip = h => h.replace(/<[^>]*>/g, ' ').replace(/\s+/g, ' ').trim();

// jobs the page would hold for a line cut into pieces: from the plan when it cuts the line (so the plan and the views are tested together), else the contract's copy slots
function jobsOfPlan(row, states, texts) { const pl = Sides.plan(ctx, row, []), slots = pl.split ? pl.slots : row.poolIds.map((_, i) => N.copySlot(i + 1)); return slots.map((s, i) => jobFor(row, s, (states || [])[i] || 'review', (texts || [])[i] || 'Mom')); }
function cardsFor(row, jobs, dir) { const W2 = world(dir || root, { [row.key]: jobs }, [row]); return { W2, ps: W2.CharmNestEngraveCard.piecesOf([row], { nameOf: () => 'CORGI 34728' }) }; }

function everyPieceItsCard(k, pieces, per) {
  const row = ROW[k], jobs = jobsOfPlan(row), { W2, ps } = cardsFor(row, jobs), C = W2.CharmNestEngraveCard;
  assert.equal(ps.length, pieces, `${pieces} pieces on the line: ${pieces} cards (one per piece), got ${ps.length}`);
  assert.equal(new Set(ps.map(p => p.pieceKey)).size, pieces, 'every card has its own piece key');
  const html = ps.map(p => C.html(p));
  if (!per) ps.forEach((p, i) => assert.ok(N.pieceWord(i + 1, pieces).test(strip(html[i]) + ' ' + p.title), `card ${i + 1} is titled "Piece ${i + 1} of ${pieces}": "${p.title}"`));
  else {   // a set / a disc necklace x N: the pieces keep the names they have (Disc 2 of 3, Small bar) and the units say which set / which pair
    assert.equal(new Set(ps.map(p => p.title)).size, pieces, 'every card has its own title: ' + ps.map(p => p.title).join(' | '));
    ps.forEach((p, i) => { if (i >= per) assert.ok(/\b(set|pair)\s*2\b/i.test(strip(html[i]) + ' ' + p.title), `card ${i + 1} (unit 2) says which set: "${p.title}"`); });
  }
  html.forEach((h, i) => assert.equal((h.match(/data-e="approve"/g) || []).length, 1, `card ${i + 1} has its OWN Approve`));
  return { ps, C };
}
t('C1', 'a line of one charm x2: TWO cards, "Piece 1 of 2" and "Piece 2 of 2", each with its own Approve (Paul\'s screenshot: one card)', () => everyPieceItsCard('s2', 2));
t('C2', 'a line of one charm x3 / x6: a card per piece', () => { everyPieceItsCard('s3', 3); everyPieceItsCard('s6', 6); });
t('C3', 'a set of two bars x2: four cards, each bar of each set its own ("... · set 2"); discs x2: six cards ("Disc 1 of 3 · set 2")', () => { everyPieceItsCard('b2', 4, 2); everyPieceItsCard('d2', 6, 3); });
t('C4', 'with jobs under the contract\'s copy slots a card is drawn per slot whatever the plan says (the views do not depend on the plan): titles "Piece i of n", own seals, own Approve', () => {
  const row = lineRow('K4', '1004', { poolIds: ['K4_1', 'K4_2'], spec: { designSku: 'CORGI_34728', quantity: 2 } }), s = [N.copySlot(1), N.copySlot(2)];
  const j1 = jobFor(row, s[0], 'approved', 'Anna', { approvedAt: 5, approvedBy: 'Test One', copies: ['K4_1'], engraveRec: { approved: true, state: 'approved', approvedAt: 5, approvedBy: 'Test One', seals: [{ by: 'Test One', at: 5, kind: 'engraved' }] } }), j2 = jobFor(row, s[1], 'review', 'Ben', { copies: ['K4_2'] });
  const { W2, ps } = cardsFor(row, [j1, j2]), C = W2.CharmNestEngraveCard;
  assert.equal(ps.length, 2, 'two pieces'); eqJ(ps.map(p => p.state), ['approved', 'approve'], 'piece 1 approved, piece 2 to approve (their own states)');
  const h = ps.map(p => C.html(p)); assert.ok(!/data-e="approve"/.test(h[0]) && /data-e="approve"/.test(h[1]), 'only the unapproved piece has an Approve'); assert.ok(/Anna/.test(h[0]) && !/Ben/.test(h[0]) && /Ben/.test(h[1]) && !/Anna/.test(h[1]), 'each card shows its own words');
  assert.ok(N.pieceWord(1, 2).test(ps[0].title) && N.pieceWord(2, 2).test(ps[1].title), 'titles: ' + ps.map(p => p.title).join(' | '));
});
t('C5', 'the order lists every piece: countOf, labels "Piece i of n", states per piece; approving one by one: worst-first, nothing skipped, one approval never approves another', () => {
  const row = lineRow('K5', '1005', { poolIds: ['K5_1', 'K5_2'], spec: { designSku: 'CORGI_34728', quantity: 2 } }), s = [N.copySlot(1), N.copySlot(2)];
  const mk = (a, b) => [jobFor(row, s[0], a, 'Anna', { copies: ['K5_1'] }), jobFor(row, s[1], b, 'Ben', { copies: ['K5_2'] })];
  const E = jobs => world(root, { [row.key]: jobs }, [row]).CharmNestEngraveOrder, o = { rows: [row] };
  let O = E(mk('review', 'review')), ps = O.piecesOf('1005', o);
  assert.equal(ps.length, 2); eqJ(ps.map(p => p.state), ['to-approve', 'to-approve']); assert.ok(ps.every((p, i) => N.pieceWord(i + 1, 2).test(p.label + ' ' + p.title)), 'labels say "Piece i of 2": ' + ps.map(p => p.label).join(' | '));
  eqJ(O.countOf('1005', o), { n: 2, approved: 0, left: 2, toApprove: 2, held: 0 });
  assert.equal(O.nextUnapproved('1005', ps[0].pieceKey, o).pieceKey, ps[1].pieceKey); assert.equal(O.step('1005', ps[0].pieceKey, 1, o).pieceKey, ps[1].pieceKey); assert.equal(O.step('1005', ps[1].pieceKey, 1, o), null, 'no skipping past the end'); assert.equal(O.step('1005', ps[1].pieceKey, -1, o).pieceKey, ps[0].pieceKey);
  // piece 1 approved on its own: the order is NOT done, piece 2 is still to approve
  O = E(mk('approved', 'review')); eqJ(O.countOf('1005', o), { n: 2, approved: 1, left: 1, toApprove: 1, held: 0 }); assert.equal(O.nextUnapproved('1005', ps[0].pieceKey, o).pieceKey, ps[1].pieceKey, 'next unapproved from piece 1 is piece 2');
  // worst first: an approved piece and a piece whose words are not settled: the order has one held piece
  O = E(mk('approved', 'words')); eqJ(O.countOf('1005', o), { n: 2, approved: 1, left: 1, toApprove: 0, held: 1 });
  O = E(mk('approved', 'approved')); eqJ(O.countOf('1005', o), { n: 2, approved: 2, left: 0, toApprove: 0, held: 0 }); assert.equal(O.nextUnapproved('1005', ps[0].pieceKey, o), null, 'all approved: nothing left to go to');
  assert.match(O.stripHtml(O.piecesOf('1005', o), ps[0].pieceKey), /Piece 1 of 2/, 'the strip says "Piece 1 of 2"');
});
t('C6', 'a MIXED order: a monogram line x2, an engraved bar x3 and a plain charm x2: the order\'s pieces are 2 monograms + 3 bars = 5, in the lines\' order, each with its own key; the plain line adds none', () => {
  const mono = lineRow('KM', '1006', { poolIds: ['KM_1', 'KM_2'], spec: { designSku: 'MONOGRAM_5351', quantity: 2, special: { monogram: true } } }), bar = lineRow('KB', '1006', { poolIds: ['KB_1', 'KB_2', 'KB_3'], spec: { designSku: 'COMET_9427', quantity: 3 } }), pl = lineRow('KP', '1006', { poolIds: ['KP_1', 'KP_2'], spec: { designSku: 'LEAF_CHARM', quantity: 2 } });
  const slots = [1, 2, 3].map(i => N.copySlot(i)), jobs = slots.map((s, i) => jobFor(bar, s, 'review', ['A', 'B', 'C'][i], { copies: ['KB_' + (i + 1)] }));
  const W2 = world(root, { [bar.key]: jobs }, [mono, bar, pl]), O = W2.CharmNestEngraveOrder;
  O.useMonogram(() => 'to-approve');   // (the page's answer for a monogram line; this stand-in answers for every line it is asked about, so it is asked about the monogram line only)
  O.useMonogram(r => ((r.parentRow || r).spec.special && (r.parentRow || r).spec.special.monogram ? 'to-approve' : null));
  const ps = O.piecesOf('1006', { rows: [mono, bar, pl] }), monos = ps.filter(p => p.kind === 'monogram'), bars = ps.filter(p => p.lineKey === bar.key);
  assert.equal(ps.length, 5, '2 monogram pieces + 3 bar pieces, got ' + ps.length + ' (' + ps.map(p => p.label).join(' | ') + ')'); assert.equal(monos.length, 2, 'the monogram line x2 is two monogram pieces'); assert.equal(bars.length, 3);
  assert.equal(new Set(ps.map(p => p.pieceKey)).size, 5, 'five distinct piece keys'); assert.ok(ps.slice(0, 2).every(p => p.kind === 'monogram') && ps.slice(2).every(p => p.lineKey === bar.key), 'the lines\' order');
  assert.ok(ps.every(p => p.lineKey !== pl.key), 'the plain charm x2 engraves nothing: no card, no engraved piece');
  assert.ok(monos.every((p, i) => N.pieceWord(i + 1, 2).test(p.label + ' ' + p.title)), 'monogram pieces are "Piece 1 of 2", "Piece 2 of 2": ' + monos.map(p => p.label).join(' | '));
  assert.equal(O.countOf('1006', { rows: [mono, bar, pl] }).n, 5);
});
t('C7', 'a line with no engraving (a plain charm x2) has no card at all, quantity 1 or 2', () => { const row = lineRow('KN', '1007', { poolIds: ['KN_1', 'KN_2'], spec: { designSku: 'LEAF_CHARM', quantity: 2 } }); assert.equal(cardsFor(row, []).ps.length, 0); });
t('C8', 'GOLDEN: quantity-1 lines: the card model and its markup, the order\'s pieces, the strip and the counts are byte for byte what the pristine modules give (single, front+back, discs, ears\' legacy front, set bars)', () => {
  const cases = [];
  const one = lineRow('G1', '2001'), two = lineRow('G2', '2001', { poolIds: ['G2_1'] }), disc = lineRow('G3', '2002', { poolIds: ['G3_1', 'G3_2', 'G3_3'], spec: { designSku: 'DISC', quantity: 1 } });
  const j = (row, slot, st, txt, ex) => Object.assign({ key: slot ? row.key + '#' + slot : row.key, slot: slot || undefined, rowKey: slot ? row.key : undefined, row, state: st, text: txt, lines: txt.split('\n'), copies: slot ? [row.key + '_' + slot.slice(1)] : row.poolIds.slice(), fit: st === 'review' ? { glyphs: [] } : null, view: st === 'review' ? { members: [] } : null, verify: { geometry: { ok: true } }, backs: [] }, ex || {});
  cases.push({ rows: [one], jobs: { G1: [j(one, null, 'review', 'Name One')] } });
  cases.push({ rows: [two], jobs: { G2: [j(two, null, 'review', 'Name Two', { key: 'G2~front', face: 'front' }), j(two, null, 'approved', 'Name Three', { approvedAt: 3, approvedBy: 'Test' })] } });
  cases.push({ rows: [disc], jobs: { G3: [j(disc, 'D1', 'review', 'A'), j(disc, 'D2', 'approved', 'B', { approvedAt: 9, approvedBy: 'Test' }), j(disc, 'D3', 'words', '')] } });
  cases.push({ rows: [one, disc], jobs: { G1: [j(one, null, 'approved', 'Name One', { approvedAt: 2, approvedBy: 'Test' })], G3: [j(disc, 'D1', 'review', 'A'), j(disc, 'D2', 'review', 'B'), j(disc, 'D3', 'review', 'C')] } });
  for (const c of cases) {
    const A = world(root, c.jobs, c.rows), B = world(BASE, c.jobs, c.rows);
    const pa = A.CharmNestEngraveCard.piecesOf(c.rows, {}), pb = B.CharmNestEngraveCard.piecesOf(c.rows, {});
    eqJ(lean(plain(pa)), lean(plain(pb)), 'card model'); { const ha = pa.map(p => A.CharmNestEngraveCard.html(p)), hb = pb.map(p => B.CharmNestEngraveCard.html(p)); ha.forEach((h, i) => { if (h !== hb[i]) { let k = 0; while (k < h.length && h[k] === (hb[i] || '')[k]) k++; assert.fail('card markup of piece ' + (i + 1) + ' differs at ' + k + ': now "' + h.slice(Math.max(0, k - 30), k + 70) + '" / pristine "' + String(hb[i]).slice(Math.max(0, k - 30), k + 70) + '"'); } }); assert.equal(ha.length, hb.length, 'card count'); }
    const rid = c.rows[0].order.receiptId, o = { rows: c.rows.filter(r => r.order.receiptId === rid) };
    const oa = A.CharmNestEngraveOrder.piecesOf(rid, o), ob = B.CharmNestEngraveOrder.piecesOf(rid, o);
    eqJ(lean(plain(oa)), lean(plain(ob)), 'order pieces'); assert.equal(A.CharmNestEngraveOrder.stripHtml(oa, oa[0] && oa[0].pieceKey), B.CharmNestEngraveOrder.stripHtml(ob, ob[0] && ob[0].pieceKey), 'strip'); eqJ(A.CharmNestEngraveOrder.countOf(rid, o), B.CharmNestEngraveOrder.countOf(rid, o), 'counts');
  }
});

// ═══ D · summary, records, seals, migration ═══════════════════════════════════════════════════════════════════════════════════════════════════════
t('D1', 'the line\'s summary is worst-first over its pieces, per piece decisions in pieces[poolId]; approved only when EVERY piece is; seals are the approved pieces\' own', () => {
  const row = lineRow('KD', '3001', { poolIds: ['KD_1', 'KD_2'] }), [c1, c2] = [N.copySlot(1), N.copySlot(2)];
  const s1 = { by: 'Test One', at: 5 }, rec1 = { needed: true, state: 'approved', approved: true, approvedAt: 5, approvedBy: 'Test One', text: 'Anna', seals: [s1] }, rec2 = { needed: true, state: 'review', approved: false, text: 'Ben' };
  const a = Object.assign(jobFor(row, c1, 'approved', 'Anna', { copies: ['KD_1'] }), { engraveRec: rec1 }), b = Object.assign(jobFor(row, c2, 'review', 'Ben', { copies: ['KD_2'] }), { engraveRec: rec2 });
  let s = Sides.summary(ctx, undefined, [a, b]);
  assert.equal(s.approved, false, 'one piece approved: the line is not'); assert.equal(s.state, 'review', 'worst first'); eqJ(s.pieces, { KD_1: { needed: true, state: 'approved', approved: true }, KD_2: { needed: true, state: 'review', approved: false } }, 'each piece its own decision');
  assert.ok(/Anna/.test(s.text) && /Ben/.test(s.text) && !/^All:.*All:/.test(s.text), 'the summary text tells the pieces apart: ' + s.text); assert.equal((s.seals || []).length, 1, 'only piece 1\'s seal so far');
  b.engraveRec = Object.assign({}, rec2, { state: 'approved', approved: true, approvedAt: 6, approvedBy: 'Test Two', seals: [{ by: 'Test Two', at: 6 }] });
  s = Sides.summary(ctx, undefined, [a, b]); assert.equal(s.approved, true); assert.equal((s.seals || []).length, 2, 'both pieces\' seals');
  const words = Sides.summary(ctx, undefined, [Object.assign(jobFor(row, c1, 'approved', 'Anna', { copies: ['KD_1'] }), { engraveRec: rec1 }), Object.assign(jobFor(row, c2, 'words', '', { copies: ['KD_2'] }), { engraveRec: { needed: true, state: 'words', approved: false } })]); assert.equal(words.state, 'words', 'worst-first: words to confirm outranks an approved piece');
});
t('D2', 'pieceRecords: piece i of n reads index i, of n, its own tag ("Piece i of n") and words, its own pool ids and seals', () => {
  const row = lineRow('KR', '3002', { poolIds: ['KR_1', 'KR_2', 'KR_3'] }), jobs = [1, 2, 3].map(i => jobFor(row, N.copySlot(i), 'review', 'W' + i, { copies: ['KR_' + i], engravingSeals: i === 2 ? [{ by: 'T', at: 1 }] : [] }));
  const rs = Sides.pieceRecords(jobs, {});
  assert.deepEqual(rs.map(r => [r.index, r.of]), [[1, 3], [2, 3], [3, 3]], 'index and count per piece'); assert.deepEqual(rs.map(r => r.words), ['W1', 'W2', 'W3']); assert.deepEqual(rs.map(r => r.poolIds), [['KR_1'], ['KR_2'], ['KR_3']]);
  assert.ok(rs.every((r, i) => N.pieceWord(i + 1, 3).test(r.tag + ' ' + r.label)), 'tags: ' + rs.map(r => r.tag).join(' | ')); assert.deepEqual(rs.map(r => r.sealed), [false, true, false]);
});
t('D3', 'MIGRATION (plan): an old whole-line record of a line of quantity 2, decided or not, no longer keeps the line whole: the plan cuts it into its pieces (the bridge migrates it: pieces-flow M1/M2 prove approvals and seals survive)', () => {
  const row = rowOf('s2'), old = state => ({ key: row.key, state, text: 'Test', lines: ['Test'], copies: row.poolIds.slice(), approvedAt: 6, approvedBy: 'Test Two', backs: [] });
  for (const st of ['approved', 'written', 'review', 'words']) { const pl = Sides.plan(ctx, row, [old(st)]); assert.equal(pl.split, true, `a whole-line job in state ${st} on a x2 line: cut into pieces`); assert.deepEqual(pl.slots, [N.copySlot(1), N.copySlot(2)], 'one slot per piece'); }
  // quantity 1 keeps the whole line whole (a decided job is never re-cut)
  const one = rowOf('s1'), plO = Sides.plan(ctx, one, [{ key: one.key, state: 'approved', copies: one.poolIds.slice() }]); assert.deepEqual({ split: plO.split, slots: plO.slots }, { split: false, slots: [null] }, 'quantity 1: the decided job under the line key stays the line\'s');
});
t('D4', 'an ear/disc job that covers several pieces of a x2 line (L holding copies 1 and 3) is a unit-1 job: the plan gives unit 2 its own slots (L.2 / D1.2) and keeps L / D1', () => {
  const e = ROW.e2, jl = { key: Sides.jobKey(e.key, 'L'), slot: 'L', rowKey: e.key, state: 'approved', copies: [e.poolIds[0], e.poolIds[2]] }, jr = { key: Sides.jobKey(e.key, 'R'), slot: 'R', rowKey: e.key, state: 'approved', copies: [e.poolIds[1], e.poolIds[3]] };
  const pl = Sides.plan(ctx, e, [jl, jr]); assert.deepEqual(pl.slots.slice().sort(), ['L', 'L.2', 'R', 'R.2'], 'unit 1 keeps its names, unit 2 is new: ' + JSON.stringify(pl.slots));
  assert.equal(Sides.idsOfSlot(ctx, e, 'L').length, 1, 'L holds only the unit-1 left ear now'); assert.equal(Sides.idsOfSlot(ctx, e, 'L.2').length, 1);
});
t('D5', 'summary: pieces archived (the quantity went down) are out of the line\'s state; n / done / open say how many pieces are decided and which are still open (worst first)', () => {
  const row = lineRow('KS', '3005', { poolIds: ['KS_1', 'KS_2', 'KS_3'] }), [c1, c2, c3] = slotsN(3), rec = (st, ap) => ({ needed: true, state: st, approved: ap, text: 'W', approvedAt: ap ? 1 : undefined, seals: ap ? [{ by: 'T', at: 1 }] : [] });
  const a = Object.assign(jobFor(row, c1, 'approved', 'A', { copies: ['KS_1'] }), { engraveRec: rec('approved', true) }), b = Object.assign(jobFor(row, c2, 'approved', 'B', { copies: ['KS_2'] }), { engraveRec: rec('approved', true) }), c = Object.assign(jobFor(row, c3, 'review', 'C', { copies: ['KS_3'], archived: 123 }), { engraveRec: rec('review', false) });
  let s = Sides.summary(ctx, undefined, [a, b, c]); assert.equal(s.approved, true, 'the archived piece 3 does not hold the line back'); assert.equal(s.n, 2, 'n counts live pieces'); assert.equal(s.done, 2);
  c.archived = undefined; s = Sides.summary(ctx, undefined, [a, b, c]); assert.equal(s.approved, false); assert.equal(s.n, 3); assert.equal(s.done, 2); assert.deepEqual(JSON.parse(JSON.stringify(s.open)), [{ slot: c3, poolId: 'KS_3', state: 'review' }], 'open: piece 3 only');
});

// ═══ E · every module that reads a job key agrees about the slot ══════════════════════════════════════════════════════════════════════════════
t('E1', 'the contract\'s slot is parsed the same by sides, faces, rows, the piece switch and the order window\'s key readers (a slot one module does not know turns a piece into the whole line)', () => {
  const Faces = require(path.join(root, 'charm-nest-engrave-faces.js')), Rows = require(path.join(root, 'charm-nest-engrave-rows.js')), Switch = require(path.join(root, 'charm-nest-piece-switch.js'));
  for (const slot of [N.copySlot(1), N.copySlot(2), N.copySlot(12), 'L.2', 'R.3', 'D1.2']) for (const face of ['', '~front']) {
    const key = 'R_T#' + slot + face, job = { key, rowKey: 'R_T', slot };
    assert.equal(Sides.parseKey(key).slot, slot, 'Sides.parseKey ' + key); assert.equal(Faces.parseKey(key).slot, slot, 'Faces.parseKey ' + key);
    assert.equal(Faces.pieceKeyOf(key), 'R_T#' + slot, 'Faces.pieceKeyOf ' + key); assert.ok(Faces.slotLabel(slot), 'Faces.slotLabel(' + slot + ') names the piece');
    assert.equal(Rows.pieceOf(job), slot, 'Rows.pieceOf: ' + slot + ' is a piece of its line'); assert.equal(Rows.lineKeyOf(job), 'R_T', 'Rows.lineKeyOf ' + key);
    const pcs = Switch.list([job]); assert.equal(pcs.length, 1, 'Switch.list gives the piece'); assert.ok(pcs[0].tag && pcs[0].label, 'the piece switch names ' + slot + ': ' + JSON.stringify([pcs[0].tag, pcs[0].label])); assert.ok(['discs', 'copy', 'ears', 'pieces'].includes(Switch.kindOf(pcs)) || Switch.kindOf(pcs), 'Switch.kindOf(list([' + slot + ']))');
  }
  assert.equal(Faces.pieceCount([{ key: 'R_T#' + N.copySlot(1) }, { key: 'R_T#' + N.copySlot(2) }, { key: 'R_T#' + N.copySlot(2) + '~front', face: 'front' }]), 2, 'Faces.pieceCount: two slots (one with a front) are two pieces');
  assert.equal(Faces.groupPieces([{ key: 'R_T#' + N.copySlot(2) }, { key: 'R_T#' + N.copySlot(1) }]).map(g => g.slot).join(), N.copySlot(1) + ',' + N.copySlot(2), 'Faces.groupPieces orders the pieces 1, 2');
});
t('E2', 'the Engrave tab\'s rows and Back / Next reach EVERY piece of a x3 line, in order, each as its own stop (one row listing the pieces, or one row each: both keep every piece reachable)', () => {
  const Rows = require(path.join(root, 'charm-nest-engrave-rows.js')), row = lineRow('KE', '4001', { poolIds: ['KE_1', 'KE_2', 'KE_3'] }), next = lineRow('KF', '4002', { poolIds: ['KF_1'] });
  const jobs = [1, 2, 3].map(i => jobFor(row, N.copySlot(i), 'review', 'W' + i, { copies: ['KE_' + i] })), other = jobFor(next, null, 'review', 'Z', { copies: ['KF_1'] });
  const rows = Rows.group(jobs.concat([other]), jobs.concat([other]), { discs: true });
  const owned = rows.flatMap(r => r.jobs.map(j => j.key)); assert.deepEqual(owned.slice().sort(), jobs.concat([other]).map(j => j.key).sort(), 'every piece is in exactly one row: ' + owned.length);
  assert.ok(rows.length === 2 || rows.length === 4, 'one row listing the three pieces (plus the other line), or one row per piece; got ' + rows.length + ' rows');
  const leadOf = s => (s.job || s.row.jobs[0]).key; let at = jobs[0].key; const seen = [at];
  for (let i = 0; i < 3; i++) { const s = Rows.step(rows, at, 1); at = leadOf(s); seen.push(at); }
  assert.deepEqual(seen, [jobs[0].key, jobs[1].key, jobs[2].key, other.key], 'Next visits piece 1, 2, 3 and then the next line: nothing is skipped');
  const back = Rows.step(rows, other.key, -1); assert.equal(leadOf(back), jobs[2].key, 'Back from the next line lands on the LAST piece');
});

console.log(`\npieces-model: ${passed} passed, ${failed} FAILED`);
if (failed) console.log(' - ' + fails.join('\n - '));
process.exit(failed ? 1 : 0);
