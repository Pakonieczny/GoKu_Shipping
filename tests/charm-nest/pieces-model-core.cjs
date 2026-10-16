// PIECES model (Paul, 16 Oct 2026, order 4195059483 "CORGI 34728 · SS · ×2": "ALL multipiece orders must always show each individual piece with respect to all Engravings and Monograms").
// A line of quantity N >= 2 is cut into one engraving job per PIECE (slots "C1".."CN"; "L.2" / "R.2" / "D1.2" for the later units of an ear pair or a disc necklace). Offline: the real
// charm-nest-engrave-sides.js / -faces.js run as they are; the bridge's job planning (ensureJobs, migrateToPieces, settlePieces, classifyOnce) runs over stand-ins in a vm. No browser, no network, no paid call.
//   1  the slot grammar: slotInfo / shape / slotRank / labelOf / tagOf / jobKey / parseKey (sides and faces read a slot the same way)
//   2  the plan: single charm x2/x3/x6, ears x1/x2, discs x1/x2, mismatched x2: one slot and one pool id per piece; the slot of a copy never depends on the quantity
//   3  GOLDEN: lines of quantity 1 (ears, mismatched pair, discs, single, letters; legacy and current) read byte for byte what the pristine module gave (tests/charm-nest/pieces-model-core.golden.json)
//   4  pieceList: every piece of a line, engraved or not
//   5  the line's summary: worst-first over the pieces, n / done / open, approved only when EVERY piece is; archived pieces are out of it
//   6  MIGRATION through the bridge: a whole-line job (approved / written / undecided) of a x2 line becomes one job per piece with the same words, approval and seals; an "L" job that held both pairs keeps the first
//      pair and hands the second its decision; a line of quantity 1 keeps its whole-line job; a quantity that goes down archives the pieces it loses (approvals kept), up again revives them
//   7  the buyer's words: a note that numbers / lists the pieces gives each its own; anything else is read ONCE (one paid call) and every piece gets the whole reading; ears are asked about ONE unit's slots
//   8  one piece's approval never reaches another: the pieces' own states, seals and records
//   node tests/charm-nest/pieces-model-core.cjs
'use strict';
const path = require('path'), fs = require('fs'), vm = require('vm'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');
const transport = require.resolve(path.join(root, 'netlify/functions/_etsyMailAnthropic.js'));
require.cache[transport] = { id: transport, filename: transport, loaded: true, exports: {} };
const Pair = require(path.join(root, 'charm-nest-pair.js'));
const Sides = require(path.join(root, 'charm-nest-engrave-sides.js'));
const Seals = require(path.join(root, 'charm-nest-engraving-seals.js'));
const Faces = require(path.join(root, 'charm-nest-engrave-faces.js'));
const F = require('./pairs-fixtures.cjs');

const J = x => (x === undefined ? undefined : JSON.parse(JSON.stringify(x))), deq = (a, b, m) => assert.deepEqual(J(a), J(b), m);
let pass = 0, fail = 0; const fails = [];
const t = async (name, fn) => { try { await fn(); pass++; console.log('  ok  ' + name); } catch (e) { fail++; fails.push(name); console.log('  FAIL ' + name + '\n      ' + String(e && e.stack || e).split('\n').slice(0, 7).join('\n      ')); } };

/* ── a world of lines ── */
const W = F.world({ orders: [
  { rid: F.rid(1), lines: [{ n: 10, kind: 'pair', on: 'sh-gf1' }] },
  { rid: F.rid(2), lines: [{ n: 20, kind: 'mismatched', on: 'sh-gf1' }] },
  { rid: F.rid(3), lines: [{ n: 30, kind: 'discs', discs: 3, on: 'sh-gf1' }] },
  { rid: F.rid(4), lines: [{ n: 40, kind: 'single', on: 'sh-gf1' }] },
  { rid: F.rid(5), lines: [{ n: 50, kind: 'single', qty: 2, on: 'sh-gf1' }, { n: 51, kind: 'single', qty: 3, on: 'sh-gf1' }, { n: 52, kind: 'single', qty: 6, on: 'sh-gf1' }] },
  { rid: F.rid(6), lines: [{ n: 60, kind: 'pair', qty: 2, on: 'sh-gf1' }, { n: 61, kind: 'discs', discs: 3, qty: 2, on: 'sh-gf1' }, { n: 62, kind: 'mismatched', qty: 2, on: 'sh-gf1' }] },
  { rid: F.rid(8), lines: [{ n: 80, kind: 'earring-single', qty: 2, on: 'sh-gf1' }] },
] });
const LEG = F.legacy(W);
const lineRow = (w, ord, n, extra) => {
  const o = w.orders.find(x => x.rid === F.rid(ord)), l = o.lines.find(x => x.n === n);
  return Object.assign({ key: F.lineKey(o.rid, F.tx(n)), poolIds: l.pieces.map(p => p.poolId), order: { receiptId: o.rid }, line: { transactionId: F.tx(n), sku: l.sku, title: 'CORGI 34728', quantity: l.qty }, spec: { designSku: l.sku, form: l.form, quantity: l.qty, pieceCount: l.line.pieceCount, pair: l.discs ? { kind: 'multi', discs: l.discs } : Object.assign({}, l.line.pair), personalization: ['Anna'], engraveCandidate: true }, state: 'pooled', material: 'silver' }, extra || {});
};
const ctxOf = w => { const rows = new Map(w.pool.map(p => [p.poolId, p])); return { poolRow: id => rows.get(id), charmOf: id => { const r = rows.get(id); return r ? F.charmOf(r.sku) : null; }, entryFor: sku => F.entryOf(sku), pair: Pair, mergeSeals: (...r) => Seals.merge(...r) }; };
const CTX = ctxOf(W);
const R = { s1: lineRow(W, 4, 40), s2: lineRow(W, 5, 50), s3: lineRow(W, 5, 51), s6: lineRow(W, 5, 52), p1: lineRow(W, 1, 10), p2: lineRow(W, 6, 60), m1: lineRow(W, 2, 20), m2: lineRow(W, 6, 62), d1: lineRow(W, 3, 30), d2: lineRow(W, 6, 61), e2: lineRow(W, 8, 80) };
const slotsOfPlan = (row, ctx = CTX) => Sides.plan(ctx, row, []).slots;

(async () => {
  console.log('PIECES model');

  /* 1 ── the slot grammar */
  await t('1 slot grammar: slotInfo / isSlot / baseSlot / unitOf / slotRank; sides and faces read the same slots', () => {
    const want = { L: ['ear', 'L', 1], R: ['ear', 'R', 1], D2: ['disc', 'D2', 1], 'L.2': ['ear', 'L', 2], 'D3.4': ['disc', 'D3', 4], C1: ['copy', 'C', 1], C12: ['copy', 'C', 12] };
    for (const [slot, [kind, base, unit]] of Object.entries(want)) { const i = Sides.slotInfo(slot); deq([i.kind, i.base, i.unit], [kind, base, unit], slot); assert.ok(Sides.isSlot(slot)); assert.equal(Sides.baseSlot(slot), base); assert.equal(Sides.unitOf(slot), unit); }
    for (const bad of ['', null, 'X', 'C', 'C0x', 'L.', 'D', 'LR', 'L.2.3', '#C1']) { assert.equal(Sides.slotInfo(bad), null, String(bad)); assert.ok(!Sides.isSlot(bad)); }
    assert.ok(Sides.slotRank('L') < Sides.slotRank('R') && Sides.slotRank('R') < Sides.slotRank('D1') && Sides.slotRank('D3') < Sides.slotRank('L.2') && Sides.slotRank('R.2') < Sides.slotRank('D1.2'), 'unit by unit');
    assert.ok(Sides.slotRank('C1') < Sides.slotRank('C2') && Sides.slotRank('C2') < Sides.slotRank(null));
    for (const slot of Object.keys(want)) {
      const key = 'r1_t1#' + slot; assert.equal(Sides.jobKey('r1_t1', slot), key); assert.equal(Sides.jobKey('r1_t1', slot, 'front'), key + '~front');
      deq(Sides.parseKey(key), { rowKey: 'r1_t1', slot }); deq(Sides.parseKey(key + '~front'), { rowKey: 'r1_t1', slot, face: 'front' });
      const f = Faces.parseKey(key + '~front'); deq([f.lineKey, f.slot, f.face, f.pieceKey], ['r1_t1', slot, 'front', key]); assert.equal(Faces.keyOf('r1_t1', 'back', slot), key); assert.equal(Faces.pieceKeyOf(key + '~front'), key);
    }
    deq(Sides.parseKey('r1_t1'), { rowKey: 'r1_t1', slot: null }); assert.equal(Faces.parseKey('r1_t1').slot, null);
    deq(['C1', 'L', 'D1.2', 'C2'].map(s => Faces.slotLabel(s)), ['Piece 1', 'Left', 'Disc 1 · set 2', 'Piece 2']);
    deq(['C2', 'L', 'D1', 'R.2', 'D2.2'].sort((a, b) => Faces.groupPieces([{ key: 'k#' + a, slot: a }, { key: 'k#' + b, slot: b }]).findIndex(g => g.slot === a) - Faces.groupPieces([{ key: 'k#' + a, slot: a }, { key: 'k#' + b, slot: b }]).findIndex(g => g.slot === b)), ['C2', 'L', 'D1', 'R.2', 'D2.2'].sort((a, b) => Sides.slotRank(a) - Sides.slotRank(b)), 'faces and sides order the pieces alike');
  });
  await t('1b names: Left / Disc 2 / Piece 2 and their chips; a unit says its unit ("pair 2", "set 2"); a line of several units names the first unit too', () => {
    deq([Sides.labelOf('L'), Sides.earOf('R'), Sides.tagOf('L'), Sides.tagOf('D2', 3), Sides.labelOf('D2')], ['Left', 'Right ear', 'LEFT EAR', 'DISC 2 of 3', 'Disc 2']);
    deq([Sides.labelOf('C2'), Sides.earOf('C2'), Sides.tagOf('C2', 5), Sides.tagOf('C2')], ['Piece 2', 'Piece 2', 'PIECE 2 of 5', 'PIECE 2']);
    deq([Sides.labelOf('L.2'), Sides.earOf('R.2'), Sides.tagOf('L.2'), Sides.tagOf('D2.2', 3), Sides.labelOf('D2.2')], ['Left · pair 2', 'Right ear · pair 2', 'LEFT EAR · PAIR 2', 'DISC 2 of 3 · SET 2', 'Disc 2 · set 2']);
    const shp = Sides.shape(['L', 'R', 'L.2', 'R.2']); deq(shp, { units: 2, per: 2, total: 4, kind: 'ear', multi: true });
    deq([Sides.labelOf('L', shp), Sides.tagOf('R', 2, shp)], ['Left · pair 1', 'RIGHT EAR · PAIR 1']);
    deq(Sides.shape(['D1', 'D2', 'D3']), { units: 1, per: 3, total: 3, kind: 'disc', multi: false }); deq(Sides.shape(['C1', 'C2', 'C3']), { units: 3, per: 1, total: 3, kind: 'copy', multi: true }); deq(Sides.shape([]), { units: 1, per: 0, total: 0, kind: 'none', multi: false });
    deq([Sides.labelOf('D1', Sides.shape(['D1', 'D2'])), Sides.tagOf('D2', 2, Sides.shape(['D1', 'D2']))], ['Disc 1', 'DISC 2 of 2'], 'one unit: the old names');
  });

  /* 2 ── the plan */
  await t('2 a single charm x1 has no slot; x2 / x3 / x6 are cut into C1..CN, one pool id each (Paul\'s order: two of one charm)', () => {
    const one = Sides.plan(CTX, R.s1, []); assert.equal(one.split, false); deq(one.slots, [null]); assert.ok(!one.byUnit);
    for (const [k, n] of [['s2', 2], ['s3', 3], ['s6', 6]]) {
      const p = Sides.plan(CTX, R[k], []); assert.equal(p.split, true, k); assert.equal(p.byUnit, true); deq(p.slots, Array.from({ length: n }, (_, i) => 'C' + (i + 1)), k);
      deq(R[k].poolIds.map(id => p.of.get(id)), p.slots, 'copy n is piece n'); for (const s of p.slots) assert.equal(Sides.idsOfSlot(CTX, R[k], s).length, 1, s + ' holds one piece');
    }
  });
  await t('2b the slot of a copy never depends on the quantity: copy 1 and 2 keep their slots when the line grows 2 -> 3, and an approved piece keeps its slot when the line shrinks', () => {
    const grown = Object.assign({}, R.s3, { poolIds: R.s3.poolIds.slice(0, 3) }), two = Object.assign({}, R.s3, { poolIds: R.s3.poolIds.slice(0, 2), spec: Object.assign({}, R.s3.spec, { quantity: 2, pieceCount: 2 }), line: Object.assign({}, R.s3.line, { quantity: 2 }) });
    deq(slotsOfPlan(two), ['C1', 'C2']); deq(slotsOfPlan(grown), ['C1', 'C2', 'C3']);
    // the line shrank from 3 to 2: the job of piece 3 still exists, so the slot stays in the plan (the bridge archives it)
    const have = [{ key: 'k#C1', slot: 'C1', state: 'approved' }, { key: 'k#C2', slot: 'C2', state: 'approved' }, { key: 'k#C3', slot: 'C3', state: 'approved' }];
    deq(Sides.plan(CTX, two, have).slots, ['C1', 'C2', 'C3']);
  });
  await t('2c ears: x1 is L and R (as always); x2 is four pieces L R L.2 R.2, one pool id each, the ear its pool id says; a mismatched pair x2 too; a single earring x2 is L and L.2', () => {
    deq(slotsOfPlan(R.p1), ['L', 'R']); const p = Sides.plan(CTX, R.p2, []); deq(p.slots, ['L', 'R', 'L.2', 'R.2']); deq(R.p2.poolIds.map(id => p.of.get(id)), ['L', 'R', 'L.2', 'R.2']);
    for (const s of p.slots) assert.equal(Sides.idsOfSlot(CTX, R.p2, s).length, 1);
    deq(slotsOfPlan(R.m1), ['L', 'R']); deq(slotsOfPlan(R.m2), ['L', 'R', 'L.2', 'R.2']);
    const side = Sides.sideRow(CTX, R.p2, 'R.2', {}); assert.equal(side.side, 'R'); assert.equal(side.slot, 'R.2'); deq(side.poolIds, [R.p2.poolIds[3]]); assert.equal(side.key, R.p2.key + '#R.2');
    const e = Sides.plan(CTX, R.e2, []); assert.ok(e.slots.every(Boolean), 'a single earring x2 has a slot for each piece'); assert.equal(e.slots.length, 2);
  });
  await t('2d discs: 3 discs x1 is D1..D3; x2 is SIX pieces D1..D3 then D1.2..D3.2, one pool id each', () => {
    deq(slotsOfPlan(R.d1), ['D1', 'D2', 'D3']); const p = Sides.plan(CTX, R.d2, []); deq(p.slots, ['D1', 'D2', 'D3', 'D1.2', 'D2.2', 'D3.2']); deq(R.d2.poolIds.map(id => p.of.get(id)), p.slots);
    for (const s of p.slots) deq(Sides.idsOfSlot(CTX, R.d2, s), [R.d2.poolIds[p.slots.indexOf(s)]]);
  });
  await t('2e one piece pooled of two is one job so far (nothing to split)', () => {
    const q1 = Object.assign({}, R.s2, { spec: Object.assign({}, R.s2.spec, { quantity: 2, pieceCount: 1 }), poolIds: R.s2.poolIds.slice(0, 1) }); assert.equal(Sides.plan(CTX, q1, []).split, false, 'one piece pooled of two: one job so far');
  });
  await t('2e2 a line of quantity 2 pinned to 3 pieces it already has (a count the quantity does not divide): every piece it has is its own, C1..C3; one pooled piece of two is one job so far', () => {
    const pinned = Object.assign({}, R.s3, { spec: Object.assign({}, R.s3.spec, { quantity: 2, pieceCount: 3 }) }); deq(slotsOfPlan(pinned), ['C1', 'C2', 'C3']);
    const q1 = Object.assign({}, R.p1, { spec: Object.assign({}, R.p1.spec, { quantity: 1 }) }); deq(slotsOfPlan(q1), ['L', 'R']);
  });
  await t('2f a whole-line job already decided keeps a line of quantity 1 whole (as always) but NOT a line of several pieces: the plan cuts it, byUnit says so', () => {
    const plain = [{ key: R.p1.key, state: 'approved' }];
    assert.equal(Sides.plan(CTX, R.s1, plain).split, false, 'a single charm: whole'); assert.equal(Sides.plan(CTX, Object.assign({}, R.p1, { poolIds: R.p1.poolIds.map(() => 'x_y_9') }), plain).split, false);
    const q = Sides.plan(CTX, R.s2, [{ key: R.s2.key, state: 'approved' }]); assert.equal(q.split, true); assert.equal(q.byUnit, true); deq(q.slots, ['C1', 'C2']);
    const d = Sides.plan(CTX, R.p2, [{ key: R.p2.key, state: 'written' }]); deq(d.slots, ['L', 'R', 'L.2', 'R.2']);
    // slotted jobs exist: the line is split and keeps their slots even when the pieces now say more
    deq(Sides.plan(CTX, R.p2, [{ key: R.p2.key + '#L', slot: 'L', state: 'approved' }, { key: R.p2.key + '#R', slot: 'R', state: 'approved' }]).slots, ['L', 'R', 'L.2', 'R.2']);
  });

  /* 3 ── golden: quantity 1 */
  await t('3 GOLDEN: lines of quantity 1 (pair, mismatched, discs, single, single earring, letters; current and legacy worlds) give the pristine plan, slots, names, side rows, summary and piece records, byte for byte', () => {
    const gold = JSON.parse(fs.readFileSync(path.join(__dirname, 'pieces-model-core.golden.json'), 'utf8'));
    const W2 = F.world({ orders: [
      { rid: F.rid(1), lines: [{ n: 10, kind: 'pair', on: 'sh-gf1' }] }, { rid: F.rid(2), lines: [{ n: 20, kind: 'mismatched', on: 'sh-gf1' }] }, { rid: F.rid(3), lines: [{ n: 30, kind: 'discs', discs: 3, on: 'sh-gf1' }] },
      { rid: F.rid(4), lines: [{ n: 40, kind: 'single', on: 'sh-gf1' }] }, { rid: F.rid(6), lines: [{ n: 60, kind: 'earring-single', on: 'sh-gf1' }] }, { rid: F.rid(7), lines: [{ n: 70, kind: 'letters', letters: 4, on: 'sh-gf1' }] }] });
    const out = {};
    for (const [label, w] of [['world', W2], ['legacy', F.legacy(W2)]]) {
      const ctx = ctxOf(w);
      for (const [ord, n] of [[1, 10], [2, 20], [3, 30], [4, 40], [6, 60], [7, 70]]) {
        const o = w.orders.find(x => x.rid === F.rid(ord)), l = o.lines.find(x => x.n === n), row = { key: F.lineKey(o.rid, F.tx(n)), poolIds: l.pieces.map(p => p.poolId), order: { receiptId: o.rid }, line: { transactionId: F.tx(n), sku: l.sku }, spec: { designSku: l.sku, form: l.form, quantity: l.qty, personalization: ['Anna'], engraveCandidate: true } };
        const pl = Sides.plan(ctx, row, []), slots = row.poolIds.map(id => Sides.slotOfId(ctx, row, id));
        const jobs = pl.slots.map(s => { const j = { key: Sides.jobKey(row.key, s), slot: s || undefined, rowKey: s ? row.key : undefined, state: s === 'L' ? 'approved' : 'review', text: 'T' + (s || ''), lines: ['T' + (s || '')], approvedBy: 'P', approvedAt: 5, copies: s ? Sides.idsOfSlot(ctx, row, s) : row.poolIds.slice() }; j.row = s ? Sides.sideRow(ctx, row, s, j) : row; j.engraveRec = { needed: true, state: j.state, approved: j.state === 'approved', text: j.text, seals: [] }; return j; });
        const parent = Object.assign({}, row); Sides.linkParent(ctx, parent, jobs);
        out[label + ':' + n] = { split: pl.split, slots: pl.slots, byPoolId: slots, keys: pl.slots.map(s => Sides.jobKey(row.key, s)), parsed: pl.slots.map(s => Sides.parseKey(Sides.jobKey(row.key, s))),
          labels: pl.slots.map(s => [Sides.labelOf(s), Sides.earOf(s), Sides.tagOf(s, pl.slots.length)]), side: jobs.map(j => [j.row.key, j.row.side, j.row.slot, j.row.poolIds]), summary: J(parent.engrave), records: J(Sides.pieceRecords(jobs)) };
      }
    }
    deq(Object.keys(out), Object.keys(gold)); for (const k of Object.keys(gold)) deq(out[k], gold[k], k);
  });

  /* 4 ── pieceList */
  await t('4 pieceList: one entry per piece even where nothing is engraved; x1 is one entry with no slot; x2 is "Piece 1 of 2", "Piece 2 of 2"; ears x2 four entries "pair 1/2"; a line not pooled yet lists the pieces it WILL have', () => {
    const one = Sides.pieceList(CTX, R.s1); assert.equal(one.length, 1); deq([one[0].slot, one[0].index, one[0].of, one[0].label], [null, 1, 1, '']);
    const two = Sides.pieceList(CTX, R.s2); deq(two.map(p => [p.poolId, p.slot, p.index, p.of, p.label, p.tag]), R.s2.poolIds.map((id, i) => [id, 'C' + (i + 1), i + 1, 2, 'Piece ' + (i + 1), `PIECE ${i + 1} of 2`]));
    const ears = Sides.pieceList(CTX, R.p2); deq(ears.map(p => [p.slot, p.unit, p.within, p.tag]), [['L', 1, 1, 'LEFT EAR · PAIR 1'], ['R', 1, 2, 'RIGHT EAR · PAIR 1'], ['L.2', 2, 1, 'LEFT EAR · PAIR 2'], ['R.2', 2, 2, 'RIGHT EAR · PAIR 2']]);
    const discs = Sides.pieceList(CTX, R.d2); deq(discs.map(p => p.tag), ['DISC 1 of 3 · SET 1', 'DISC 2 of 3 · SET 1', 'DISC 3 of 3 · SET 1', 'DISC 1 of 3 · SET 2', 'DISC 2 of 3 · SET 2', 'DISC 3 of 3 · SET 2']); deq(discs.map(p => p.index), [1, 2, 3, 4, 5, 6]);
    const pre = Object.assign({}, R.s3, { poolIds: [] }); pre.spec = Object.assign({}, pre.spec, { pieceCount: 3 }); const pl = Sides.pieceList(CTX, pre); assert.equal(pl.length, 3); deq(pl.map(p => p.poolId), [1, 2, 3].map(n => `${pre.order.receiptId}_${pre.line.transactionId}_${n}`)); deq(pl.map(p => p.slot), ['C1', 'C2', 'C3']);
    assert.equal(Sides.pieceList(CTX, null).length, 0);
  });

  /* 5 ── the summary */
  const jobFor = (row, slot, state, text, extra) => { const j = Object.assign({ key: Sides.jobKey(row.key, slot), slot: slot || undefined, rowKey: slot ? row.key : undefined, state, text, lines: text ? text.split('\n') : [], copies: Sides.idsOfSlot(CTX, row, slot) }, extra || {}); j.row = Sides.sideRow(CTX, row, slot, j); j.engraveRec = Object.assign({ needed: true, state, approved: state === 'approved' || state === 'written', text, seals: (extra && extra.seals) || [] }, extra && extra.rec || {}); return j; };
  await t('5 the line\'s summary is worst-first over its pieces: one approved piece of two is NOT the line approved; per piece decisions in pieces[poolId]; n / done / open; the text tells the pieces apart', () => {
    const row = Object.assign({}, R.s2), a = jobFor(row, 'C1', 'approved', 'Anna', { approvedBy: 'Paul', approvedAt: 5, rec: { approvedBy: 'Paul', approvedAt: 5, seals: [{ id: 's1', by: 'Paul', at: 5, kind: 'engraved' }] } }), b = jobFor(row, 'C2', 'words', 'Ben');
    Sides.linkParent(CTX, row, [a, b]); const s = row.engrave;
    assert.equal(s.approved, false); assert.equal(s.state, 'words', 'worst first'); deq([s.n, s.done], [2, 1]); deq(s.open.map(o => [o.slot, o.poolId, o.state]), [['C2', R.s2.poolIds[1], 'words']]);
    deq(s.pieces[R.s2.poolIds[0]], { needed: true, state: 'approved', approved: true }); deq(s.pieces[R.s2.poolIds[1]], { needed: true, state: 'words', approved: false });
    assert.match(s.text, /Piece 1: Anna/); assert.match(s.text, /Piece 2: Ben/);
    b.state = 'approved'; b.engraveRec = Object.assign({}, b.engraveRec, { state: 'approved', approved: true }); const s2 = row.engrave; assert.equal(s2.approved, true); assert.equal(s2.state, 'approved'); deq([s2.n, s2.done, s2.open], [2, 2, []]);
    const same = jobFor(row, 'C2', 'approved', 'Anna', { rec: { approved: true } }); Sides.linkParent(CTX, row, [a, same]); assert.equal(row.engrave.text, 'Anna', 'the same words on both: said once');
  });
  await t('5b a line of quantity 1 has no n / done / open (the old summary, field for field); archived pieces are out of the line\'s summary and its approval', () => {
    const one = Object.assign({}, R.s1), j = jobFor(one, null, 'approved', 'Anna'); j.row = one; j.engraveRec = { needed: true, state: 'approved', approved: true, text: 'Anna' }; Sides.linkParent(CTX, one, [j]);
    for (const k of ['n', 'done', 'open']) assert.ok(!(k in one.engrave), k);
    const row = Object.assign({}, R.s3), a = jobFor(row, 'C1', 'approved', 'A'), b = jobFor(row, 'C2', 'approved', 'A'), c = jobFor(row, 'C3', 'review', 'A', { archived: 7 }); Sides.linkParent(CTX, row, [a, b, c]);
    assert.equal(row.engrave.approved, true, 'the archived piece 3 does not hold the line back'); deq([row.engrave.n, row.engrave.done], [2, 2]);
  });

  /* 6 ── the bridge: job planning and migration over stand-ins */
  const bridgeSrc = fs.readFileSync(path.join(root, 'charm-nest-bridge.js'), 'utf8');
  const cut = (from, to) => { const a = bridgeSrc.indexOf(from), b = bridgeSrc.indexOf(to, a); assert.ok(a > 0 && b > a, 'slice ' + from.trim().slice(0, 30)); return bridgeSrc.slice(a, b); };
  const SLICE = cut('  const SIDES = ', '  function setNone(job');
  const deep = x => (x === undefined ? undefined : JSON.parse(JSON.stringify(x)));
  function harness(rows, o = {}) {
    const jobs = new Map(), asked = [], events = [], reviews = [], pool = new Map(W.pool.map(p => [p.poolId, p])), byKey = new Map(rows.map(r => [r.key, r]));
    const win = { CharmNestEngraveSides: Sides, CharmNestPair: Pair, CharmNestEngraveFaces: Faces };
    const ctx = {
      window: win, items: () => jobs, B: { pool: { rows: pool }, orders: { byKey }, run: null },
      Pool: { charmOf: id => { const r = pool.get(id); return r ? F.charmOf(r.sku) : null; }, previewCharmOf: () => null, previewIds: () => [], update: async () => {} }, Master: { entryFor: sku => F.entryOf(sku) }, CNEngravingSeals: Seals, DECIDED: ['approved', 'written', 'skipped'],
      O: Object.assign(Object.create(require(path.join(root, 'charm-nest-orders.js'))), { FINISHED_LINE: new Set(['committed', 'gone', 'cancelled']), fontOfRecord: () => 'f1', fontById: () => ({ name: 'F' }), DEFAULT_FONT: 'f1' }), loadFontSet: async () => null, S: { settings: { engraveConfidence: .65 } },
      render() {}, agent() {}, Date, Promise, Map, Set, WeakMap, Array, Object, String, Number, JSON, Math, RegExp, Error,
      Review: { add: it => reviews.push(it.key), remove() {}, render() {} }, RunCtl: { poke() {}, save: async () => {} }, Session: { schedule() {}, copy: deep }, Orders: { render() {}, rows: () => rows },
      TL: { line: (row, type, d) => { events.push({ row, type, id: d && d.id, text: d && d.text }); }, hash: s => String(s).length },
      CNListActivity: { touch() {} }, faceWord: j => (j && j.face === 'front' ? 'front' : 'back'), employeeName: () => 'Paul', needEmployee: async () => 'Paul', charmFor: () => null,
      setReady(job) { job.state = 'ready'; job.row.engrave = Object.assign({}, job.row.engrave, { needed: true, state: 'ready', text: job.text, approved: false }); return job; },
      toWords(job, why) { job.state = 'words'; job.reason = why; job.row.engrave = Object.assign({}, job.row.engrave, { needed: true, state: 'words', text: job.text, approved: false, reason: why }); return job; },
      setNone(job, why) { job.state = 'none'; job.reason = why; job.row.engrave = Object.assign({}, job.row.engrave, { needed: false, state: 'none', reason: why, approved: true }); return job; },
      agentCall: async (mode, ask) => { asked.push(ask); if (o.reading === undefined) throw new Error('a paid reading was asked for'); return typeof o.reading === 'function' ? o.reading(ask) : o.reading; },
    };
    vm.createContext(ctx); vm.runInContext(SLICE, ctx);
    vm.runInContext('this.jobsOf = jobsOf; this.ensureJobs = ensureJobs; this.classify = classify;', ctx);
    return { ctx, jobs, asked, events, reviews, byKey, keys: () => [...jobs.keys()] };
  }
  const mk = (base, extra) => { const r = Object.assign({}, base, extra || {}); delete r.engrave; r.poolIds = base.poolIds.slice(); r.spec = Object.assign({}, base.spec, (extra && extra.spec) || {}); return r; };
  const decided = (j, state, text, by, at, seals) => { j.state = state; j.text = text; j.lines = text.split('\n'); j.approvedBy = by; j.approvedAt = at; j.engravingSeals = seals || []; const rec = { needed: true, state, approved: true, text, approvedBy: by, approvedAt: at, seals: seals || [] }; if (j.slot || j.linked) j.engraveRec = rec; else j.row.engrave = rec; return j; };
  const SEAL = at => ({ id: 'seal' + at, by: 'Paul', at, kind: 'engraved' });

  await t('6 Paul\'s order: a line of one charm x2 gets TWO jobs C1 and C2, one pool id each, each with its own record; the line summary is theirs', () => {
    const row = mk(R.s2), h = harness([row]), js = h.ctx.ensureJobs(row);
    deq(js.map(j => j.key), [row.key + '#C1', row.key + '#C2']); deq(js.map(j => j.copies), row.poolIds.map(id => [id])); deq(js.map(j => j.slot), ['C1', 'C2']); deq(js.map(j => j.row.poolIds), row.poolIds.map(id => [id]));
    assert.notEqual(js[0].row, js[1].row); js[0].row.engrave = { needed: true, state: 'review', text: 'Anna', approved: false }; assert.equal(js[1].row.engrave, undefined, 'writing piece 1\'s record never reaches piece 2\'s');
    deq(h.ctx.jobsOf(row).map(j => j.key), js.map(j => j.key)); assert.ok(row._engraveLinked, 'the line reads the summary of its pieces');
    const one = mk(R.s1), h1 = harness([one]), j1 = h1.ctx.ensureJobs(one); deq(j1.map(j => [j.key, j.slot]), [[one.key, undefined]], 'quantity 1: one job under the line key, no slot');
  });
  await t('6b MIGRATION: an APPROVED whole-line job of a x2 line becomes two approved jobs with the same words, approver, time and seals; the file of each piece\'s own pool id goes with it; nothing asks again', () => {
    const row = mk(R.s2), h = harness([row]), [p1, p2] = row.poolIds;
    const old = { key: row.key, row, state: 'approved', text: 'Test', lines: ['Test'], copies: row.poolIds.slice(), backs: [{ poolId: p1, png: 'a.png' }, { poolId: p2, png: 'b.png' }], t: 1, fit: { size: 5, centre: [1, 2], cmds: [[1, 2, 3]] }, claudeJob: { id: 'cj1', ask: 'k' }, approvalIntent: 1 };
    decided(old, 'approved', 'Test', 'Paul', 77, [SEAL(77)]); old.engraveRec = undefined; h.jobs.set(old.key, old); row.engrave = { needed: true, state: 'approved', approved: true, text: 'Test', approvedBy: 'Paul', approvedAt: 77, seals: [SEAL(77)] };
    const js = h.ctx.ensureJobs(row);
    deq(js.map(j => j.key), [row.key + '#C1', row.key + '#C2']); assert.equal(h.jobs.get(row.key), undefined, 'the whole-line job is gone');
    for (const [i, j] of js.entries()) {
      deq([j.state, j.text, j.approvedBy, j.approvedAt, j.migratedFrom], ['approved', 'Test', 'Paul', 77, row.key]); deq(j.fit, old.fit); assert.notEqual(j.fit, old.fit, 'a copy, not the same object'); deq(j.claudeJob, { id: 'cj1', ask: 'k' });
      deq(j.backs.map(b => b.poolId), [row.poolIds[i]]); deq(j.engraveRec.seals, [SEAL(77)]); assert.equal(j.engraveRec.approved, true); assert.equal(j.copies.length, 1);
    }
    const s = row.engrave; assert.equal(s.approved, true, 'the line is still approved: no second approval is asked'); assert.equal(s.state, 'approved'); assert.equal(s.seals.length, 1, 'the seal is not doubled in the line\'s list'); assert.equal(s.seals[0].by, 'Paul'); assert.equal(s.seals[0].at, 77); deq([s.n, s.done], [2, 2]);
    assert.equal(h.reviews.length, 0, 'nothing is put to a person'); const again = h.ctx.ensureJobs(row); deq(again.map(j => j.key), js.map(j => j.key)); assert.equal(h.jobs.size, 2, 'a second pass changes nothing');
  });
  await t('6c MIGRATION: a WRITTEN whole-line job stays written on each piece; an undecided one (in review, with its fit) becomes two jobs in review: the paid reading it carries is not asked again', () => {
    const row = mk(R.s2), h = harness([row]), old = { key: row.key, row, state: 'written', text: 'Test', lines: ['Test'], copies: row.poolIds.slice(), backs: [{ poolId: row.poolIds[0] }, { poolId: row.poolIds[1] }], t: 1 }; decided(old, 'written', 'Test', 'Paul', 9, [SEAL(9)]); h.jobs.set(old.key, old); row.engrave = { needed: true, state: 'written', approved: true, text: 'Test', seals: [SEAL(9)] };
    deq(h.ctx.ensureJobs(row).map(j => j.state), ['written', 'written']);
    const r2 = mk(R.s3), h2 = harness([r2]), rv = { key: r2.key, row: r2, state: 'review', text: 'Kim', lines: ['Kim'], copies: r2.poolIds.slice(), backs: [], t: 1, claudeJob: { id: 'c', ask: 'k' } }; h2.jobs.set(rv.key, rv); r2.engrave = { needed: true, state: 'review', text: 'Kim', approved: false };
    const js = h2.ctx.ensureJobs(r2); deq(js.map(j => [j.slot, j.state, j.text]), [['C1', 'review', 'Kim'], ['C2', 'review', 'Kim'], ['C3', 'review', 'Kim']]); assert.equal(h2.asked.length, 0); assert.equal(r2.engrave.approved, false);
    // the words one piece is given later are its own
    js[1].text = 'Lee'; assert.equal(h2.jobs.get(r2.key + '#C1').text, 'Kim'); assert.equal(h2.jobs.get(r2.key + '#C3').text, 'Kim');
  });
  await t('6d MIGRATION: a whole-line job on a line of quantity 1 stays whole; a single skipped / none job is cloned too (a decision to cut plain is a decision)', () => {
    const row = mk(R.s1), h = harness([row]), old = { key: row.key, row, state: 'approved', text: 'A', lines: ['A'], copies: row.poolIds.slice(), backs: [], t: 1 }; decided(old, 'approved', 'A', 'Paul', 1); h.jobs.set(old.key, old); row.engrave = { needed: true, state: 'approved', approved: true, text: 'A' };
    deq(h.ctx.ensureJobs(row).map(j => j.key), [row.key]); assert.equal(h.jobs.get(row.key), old);
    const r2 = mk(R.s2), h2 = harness([r2]), pl = { key: r2.key, row: r2, state: 'skipped', text: '', lines: [], copies: r2.poolIds.slice(), backs: [], t: 1, decidedBy: 'Paul', decidedAt: 3 }; h2.jobs.set(pl.key, pl); r2.engrave = { needed: false, state: 'skipped', approved: true };
    deq(h2.ctx.ensureJobs(r2).map(j => [j.slot, j.state, j.decidedBy]), [['C1', 'skipped', 'Paul'], ['C2', 'skipped', 'Paul']]);
  });
  await t('6e MIGRATION of ears: an "L" job that held both pairs\' left ears (copies 1 and 3) keeps the first pair and its key; the second pair\'s L.2 is a copy of its decision with the file of copy 3; same for R and for discs', () => {
    const row = mk(R.p2), h = harness([row]), [i1, i2, i3, i4] = row.poolIds;
    const mkJob = (slot, copies) => { const j = { key: row.key + '#' + slot, slot, rowKey: row.key, groupKey: 'g', state: 'approved', text: 'Anna', lines: ['Anna'], copies: copies.slice(), backs: copies.map(poolId => ({ poolId })), t: 1 }; j.row = Sides.sideRow(CTX, row, slot, j); j.engraveRec = { needed: true, state: 'approved', approved: true, text: 'Anna', approvedBy: 'Paul', approvedAt: 5, seals: [SEAL(5)] }; j.approvedBy = 'Paul'; j.approvedAt = 5; return j; };
    h.jobs.set(row.key + '#L', mkJob('L', [i1, i3])); h.jobs.set(row.key + '#R', mkJob('R', [i2, i4]));
    const js = h.ctx.ensureJobs(row); deq(js.map(j => j.key.slice(row.key.length)), ['#L', '#R', '#L.2', '#R.2']); deq(js.map(j => j.copies), [[i1], [i2], [i3], [i4]]);
    deq(js.map(j => j.state), ['approved', 'approved', 'approved', 'approved']); deq(js.map(j => j.backs.map(b => b.poolId)), [[i1], [i2], [i3], [i4]], 'each piece keeps the file of its own pool id'); deq(js.map(j => j.migratedFrom), [undefined, undefined, row.key + '#L', row.key + '#R']);
    for (const j of js) deq(j.engraveRec.seals, [SEAL(5)]); assert.equal(row.engrave.approved, true); deq([row.engrave.n, row.engrave.done], [4, 4]); assert.equal(js[2].row.side, 'L');
    const dr = mk(R.d2), hd = harness([dr]); const mkD = (slot, copies) => { const j = { key: dr.key + '#' + slot, slot, rowKey: dr.key, groupKey: 'g', state: 'review', text: slot, lines: [slot], copies: copies.slice(), backs: [], t: 1 }; j.row = Sides.sideRow(CTX, dr, slot, j); j.engraveRec = { needed: true, state: 'review', approved: false, text: slot }; return j; };
    ['D1', 'D2', 'D3'].forEach((s, i) => hd.jobs.set(dr.key + '#' + s, mkD(s, [dr.poolIds[i], dr.poolIds[i + 3]])));
    const dj = hd.ctx.ensureJobs(dr); deq(dj.map(j => j.slot), ['D1', 'D2', 'D3', 'D1.2', 'D2.2', 'D3.2']); deq(dj.map(j => j.text), ['D1', 'D2', 'D3', 'D1', 'D2', 'D3']); deq(dj.map(j => j.copies.length), [1, 1, 1, 1, 1, 1]);
  });
  await t('6f QUANTITY CHANGES: a piece the line no longer has is archived when decided (approval and seals kept, out of the line\'s state and lists) or removed when nobody decided it; if it comes back it is the same job again', () => {
    const row = mk(R.s3), h = harness([row]); let js = h.ctx.ensureJobs(row); deq(js.map(j => j.slot), ['C1', 'C2', 'C3']);
    decided(h.jobs.get(row.key + '#C3'), 'approved', 'Kim', 'Paul', 4, [SEAL(4)]); h.jobs.get(row.key + '#C3').state = 'approved';
    decided(h.jobs.get(row.key + '#C1'), 'approved', 'Kim', 'Paul', 4, [SEAL(4)]); h.jobs.get(row.key + '#C2').state = 'review';
    // the line shrinks to 2 pieces
    row.poolIds = row.poolIds.slice(0, 2); row.spec = Object.assign({}, row.spec, { quantity: 2, pieceCount: 2 }); row.line = Object.assign({}, row.line, { quantity: 2 });
    js = h.ctx.ensureJobs(row); deq(js.map(j => j.slot), ['C1', 'C2']); deq(h.ctx.jobsOf(row).map(j => j.slot), ['C1', 'C2']);
    const gone = h.jobs.get(row.key + '#C3'); assert.ok(gone && gone.archived > 0, 'the approved piece is kept, archived'); assert.equal(gone.state, 'approved'); deq(gone.engraveRec.seals, [SEAL(4)]); assert.equal(row.engrave.approved, false, 'piece 2 is still to approve'); deq([row.engrave.n, row.engrave.done], [2, 1]);
    // the line shrinks again: piece 2, undecided, is removed outright
    row.poolIds = row.poolIds.slice(0, 1); row.spec = Object.assign({}, row.spec, { quantity: 1, pieceCount: 1 }); row.line = Object.assign({}, row.line, { quantity: 1 });
    js = h.ctx.ensureJobs(row); assert.ok(!h.jobs.has(row.key + '#C2'), 'an undecided piece the line lost is removed'); assert.ok(h.jobs.has(row.key + '#C3'), 'the approved one is still kept');
    // back to three pieces: the archived piece is itself again, with its approval
    row.poolIds = R.s3.poolIds.slice(); row.spec = Object.assign({}, row.spec, { quantity: 3, pieceCount: 3 }); row.line = Object.assign({}, row.line, { quantity: 3 });
    js = h.ctx.ensureJobs(row); const back = h.jobs.get(row.key + '#C3'); assert.ok(!back.archived, 'revived'); assert.equal(back.state, 'approved'); assert.equal(back.approvedAt, 4); deq(js.map(j => j.slot), ['C1', 'C2', 'C3']); assert.equal(h.ctx.jobsOf(row).length, 3);
  });
  await t('6g a line that GREW (a piece the first piece\'s job never held) starts with that piece\'s words, to confirm; it needs its own approval', () => {
    const row = mk(R.s2, { poolIds: R.s2.poolIds.slice(0, 2) }), h = harness([row]); h.ctx.ensureJobs(row);
    // the ears of one pair exist and are approved; a second pair is added to the line
    const ear = mk(R.p1), he = harness([ear]), js0 = he.ctx.ensureJobs(ear); js0.forEach(j => decided(j, 'approved', 'Anna', 'Paul', 3, [SEAL(3)]));
    const grown = ear; grown.spec = Object.assign({}, grown.spec, { quantity: 2, pieceCount: 4 }); grown.line = Object.assign({}, grown.line, { quantity: 2 }); grown.poolIds = R.p2.poolIds.slice();
    const js = he.ctx.ensureJobs(grown); deq(js.map(j => j.slot), ['L', 'R', 'L.2', 'R.2']); deq(js.slice(0, 2).map(j => j.state), ['approved', 'approved']); deq(js.slice(2).map(j => [j.state, j.text, j.copiedFrom]), [['classify', 'Anna', grown.key + '#L'], ['classify', 'Anna', grown.key + '#R']]);
    assert.equal(grown.engrave.approved, false, 'the new pair is not approved by the old one');
  });

  await t('6h front AND back on a x2 line: a Front and a Back job for EACH piece (C1, C1~front, C2, C2~front), each face its own record; a whole-line approved back and front migrate to both pieces with both faces kept', async () => {
    const BOTH = { v: 1, mode: 'both', source: 'labelled', front: { text: 'Name One' }, back: { text: 'Name Two' }, textSource: 'personalization', needsPerson: false, confidence: 0.95, why: 'labelled' };
    const row = mk(R.s2, { spec: { engraving: BOTH } }), h = harness([row]), js = h.ctx.ensureJobs(row);
    deq(js.map(j => j.key.slice(row.key.length)), ['#C1~front', '#C1', '#C2~front', '#C2']); deq(js.map(j => j.copies.length), [1, 1, 1, 1]);
    const r2 = mk(R.s2, { spec: { engraving: BOTH } }), h2 = harness([r2]);
    const back = { key: r2.key, rowKey: r2.key, linked: true, state: 'approved', text: 'Name Two', lines: ['Name Two'], copies: r2.poolIds.slice(), backs: r2.poolIds.map(poolId => ({ poolId })), t: 1, approvedBy: 'Paul', approvedAt: 3 }, front = { key: r2.key + '~front', face: 'front', rowKey: r2.key, state: 'approved', text: 'Name One', lines: ['Name One'], copies: r2.poolIds.slice(), backs: r2.poolIds.map(poolId => ({ poolId, face: 'front' })), t: 1, approvedBy: 'Paul', approvedAt: 4 };
    for (const j of [back, front]) { j.row = Sides.sideRow(CTX, r2, null, j); j.engraveRec = { needed: true, state: 'approved', approved: true, text: j.text, approvedBy: 'Paul', approvedAt: j.approvedAt, seals: [SEAL(j.approvedAt)] }; h2.jobs.set(j.key, j); }
    const mj = h2.ctx.ensureJobs(r2); deq(mj.map(j => j.key.slice(r2.key.length)), ['#C1~front', '#C1', '#C2~front', '#C2']); deq(mj.map(j => [j.state, j.text]), [['approved', 'Name One'], ['approved', 'Name Two'], ['approved', 'Name One'], ['approved', 'Name Two']]);
    deq(mj.map(j => j.backs.map(b => b.poolId)), [[r2.poolIds[0]], [r2.poolIds[0]], [r2.poolIds[1]], [r2.poolIds[1]]]); assert.ok(!h2.jobs.has(r2.key) && !h2.jobs.has(r2.key + '~front'), 'the whole-line jobs are gone'); assert.equal(r2.engrave.approved, true, 'no second approval is asked');
  });

  /* 7 ── the buyer's words */
  const READING = text => ({ text, engrave: true, source: 'personalization', confidence: 1, sourceQuote: text, requests: { side: 'back' }, questions: [], reasoning: 'x' });
  const withNote = (base, personalization, spec) => mk(base, { spec: Object.assign({ personalization }, spec || {}) });
  await t('7 a note that numbers the pieces gives each its own words, ready for its fit, no paid call; "the same on all" gives every piece the same', async () => {
    const row = withNote(R.s2, ['Piece 1: Anna, Piece 2: Ben']), h = harness([row]); await h.ctx.classify(row);
    deq(h.ctx.jobsOf(row).map(j => [j.slot, j.state, j.text]), [['C1', 'ready', 'Anna'], ['C2', 'ready', 'Ben']]); assert.equal(h.asked.length, 0);
    const r2 = withNote(R.s2, ['Both: Mom']), h2 = harness([r2]); await h2.ctx.classify(r2); deq(h2.ctx.jobsOf(r2).map(j => [j.state, j.text]), [['ready', 'Mom'], ['ready', 'Mom']]);
  });
  await t('7b two names in a list are a PROPOSAL (Words to confirm), each piece with its own name; one bare word is read once and goes on BOTH pieces', async () => {
    const row = withNote(R.s2, ['Anna & Ben']), h = harness([row]); await h.ctx.classify(row);
    deq(h.ctx.jobsOf(row).map(j => [j.state, j.text]), [['words', 'Anna'], ['words', 'Ben']]); assert.equal(h.asked.length, 0);
    const r2 = withNote(R.s2, ['Mom']), h2 = harness([r2], { reading: READING('Mom') }); await h2.ctx.classify(r2);
    assert.equal(h2.asked.length, 1, 'the paid reader is asked ONCE for the line, not once per piece'); assert.ok(!('slots' in h2.asked[0]), 'a copy of a charm is no slot to it'); deq(h2.ctx.jobsOf(r2).map(j => [j.state, j.text]), [['ready', 'Mom'], ['ready', 'Mom']]);
    const r3 = withNote(R.s6, ['Mom']), h3 = harness([r3], { reading: READING('Mom') }); await h3.ctx.classify(r3); assert.equal(h3.asked.length, 1); assert.equal(h3.ctx.jobsOf(r3).filter(j => j.text === 'Mom' && j.state === 'ready').length, 6);
  });
  await t('7c nothing to engrave ("N/A") or words that are not N parts: every piece gets the one reading; a buyer message beside the note defers to the reader (no split by the rules)', async () => {
    const row = withNote(R.s2, ['Mom and Dad forever']), h = harness([row], { reading: READING('Mom and Dad forever') }); await h.ctx.classify(row); assert.equal(h.asked.length, 1); deq(h.ctx.jobsOf(row).map(j => j.text), ['Mom and Dad forever', 'Mom and Dad forever']);
    const r2 = withNote(R.s2, ['Anna / Ben'], { buyerMessage: 'please make the first one smaller' }), h2 = harness([r2], { reading: READING('Anna / Ben') }); await h2.ctx.classify(r2); assert.equal(h2.asked.length, 1, 'another voice on the order: the reader decides'); deq(h2.ctx.jobsOf(r2).map(j => j.text), ['Anna / Ben', 'Anna / Ben']);
  });
  await t('7d ears x2: the reader is asked about the left and right ear of ONE unit (never "L.2"); the second pair gets the words of the same ear', async () => {
    const row = mk(R.p2), h = harness([row], { reading: { text: 'Anna', engrave: true, source: 'personalization', confidence: 1, requests: { side: 'back' }, questions: [], pieces: [{ slot: 'L', text: 'Anna' }, { slot: 'R', text: 'Ben' }] } }); await h.ctx.classify(row);
    assert.equal(h.asked.length, 1); deq(h.asked[0].slots, ['L', 'R']); deq(h.ctx.jobsOf(row).map(j => [j.slot, j.text]), [['L', 'Anna'], ['R', 'Ben'], ['L.2', 'Anna'], ['R.2', 'Ben']]);
  });
  await t('7e a person\'s words for ONE piece (engravingOverride.pieces) never reach another; the line-level words are the fallback of every piece with none of its own', () => {
    const row = mk(R.s3, { engravingOverride: { mode: 'back', back: { text: 'Mom' }, pieces: { C2: { back: { text: 'Ben' } } }, by: 'Paul', at: 1 } });
    const w = slot => Faces.setupOf(row, undefined, slot).back.text; deq(['C1', 'C2', 'C3'].map(w), ['Mom', 'Ben', 'Mom']); deq(Faces.setupOf(row).back.text, 'Mom');
    assert.equal(Faces.seedFor(Faces.setupOf(row, undefined, 'C2'), 'back', 'C2').text, 'Ben'); assert.equal(Faces.seedFor(Faces.setupOf(row, undefined, 'C3'), 'back', 'C3').text, 'Mom');
  });

  await t('7f "same on every piece" (patch.all): the line\'s words replace every piece\'s own words for that face; without it a line-level save leaves the pieces\' own words alone', () => {
    const Setup = require(path.join(root, 'charm-nest-engrave-setup.js')), row = mk(R.s3, { engravingOverride: { mode: 'back', back: { text: 'Mom' }, pieces: { C2: { back: { text: 'Ben' } }, C3: { back: { text: 'Kim' } } }, by: 'Paul', at: 1 } });
    const keep = Setup.buildOverride(row, { back: { text: 'Dad' } }, 'Paul', 2); deq(['C1', 'C2', 'C3'].map(sl => Faces.setupOf({ spec: row.spec }, keep, sl).back.text), ['Dad', 'Ben', 'Kim']);
    const all = Setup.buildOverride(row, { back: { text: 'Dad' }, all: true }, 'Paul', 2); deq(['C1', 'C2', 'C3'].map(sl => Faces.setupOf({ spec: row.spec }, all, sl).back.text), ['Dad', 'Dad', 'Dad']); assert.ok(!all.pieces, 'no piece keeps words of its own');
  });

  await t('7g the setup editor over a x2 line: one piece\'s words reach that piece only; "same on every piece" reaches both; an approved piece whose words are changed is cut plain only as the editor says (its seals stay)', async () => {
    const Setup = require(path.join(root, 'charm-nest-engrave-setup.js'));
    const row = mk(R.s2, { engravingOverride: { mode: 'back', back: { text: 'Mom' }, by: 'Paul', at: 1 } }), h = harness([row]); await h.ctx.classify(row);
    deq(h.ctx.jobsOf(row).map(j => [j.slot, j.state, j.text]), [['C1', 'ready', 'Mom'], ['C2', 'ready', 'Mom']]); assert.equal(h.asked.length, 0, 'a person\'s words: nothing is read');
    const log = []; const env = { who: () => 'Paul', now: () => 7, store: (l, ov) => { if (ov) l.engravingOverride = ov; else delete l.engravingOverride; }, jobsOf: l => h.ctx.jobsOf(l), ensureJobs: l => h.ctx.ensureJobs(l), slots: l => [...new Set(h.ctx.jobsOf(l).map(j => j.slot || null))],
      remove: j => { h.jobs.delete(j.key); }, skip: async j => { j.state = 'skipped'; }, setNone: j => { j.state = 'none'; }, reopen: j => j, seed: async (j, seed) => { log.push(j.key); j.text = seed.text; j.lines = seed.lines.slice(); }, changed() {} };
    await Setup.apply(row, { back: { text: 'Ben' }, slot: 'C2' }, env); deq(log, [row.key + '#C2'], 'only piece 2 is seeded'); deq(h.ctx.jobsOf(row).map(j => j.text), ['Mom', 'Ben']);
    log.length = 0; await Setup.apply(row, { back: { text: 'Dad' }, all: true }, env); deq(log.sort(), [row.key + '#C1', row.key + '#C2'], 'both pieces take the line\'s words'); deq(h.ctx.jobsOf(row).map(j => j.text), ['Dad', 'Dad']); assert.ok(!(row.engravingOverride && row.engravingOverride.pieces), 'no piece keeps words of its own');
  });

  /* 8 ── approvals */
  await t('8 one piece\'s approval never approves another: states, seals and records are per job; approved only when every piece is', () => {
    const row = mk(R.s3), h = harness([row]), js = h.ctx.ensureJobs(row);
    js.forEach((j, i) => { j.state = 'review'; j.text = 'W' + i; j.row.engrave = { needed: true, state: 'review', text: j.text, approved: false }; });
    decided(js[1], 'approved', 'W1', 'Paul', 8, [SEAL(8)]); const s = row.engrave;
    deq([s.approved, s.state, s.n, s.done], [false, 'review', 3, 1]); deq(s.open.map(o => o.slot), ['C1', 'C3']); deq(Object.fromEntries(Object.entries(s.pieces).map(([k, v]) => [k.slice(-1), v.approved])), { 1: false, 2: true, 3: false });
    assert.equal(js[0].approvedAt, null); deq(js[0].row.engrave, { needed: true, state: 'review', text: 'W0', approved: false }); deq(s.seals.map(x => [x.by, x.at]), [['Paul', 8]], 'the seal is the approved piece\'s own');
    decided(js[0], 'approved', 'W0', 'Paul', 9, [SEAL(9)]); decided(js[2], 'approved', 'W2', 'Paul', 10, [SEAL(10)]); const f = row.engrave; deq([f.approved, f.state, f.n, f.done], [true, 'approved', 3, 3]); deq(f.seals.map(x => x.at).sort((a, b) => a - b), [8, 9, 10]);
  });

  console.log(`\npieces-model-core: ${pass} passed, ${fail} FAILED`); if (fail) { for (const f of fails) console.log(' - ' + f); process.exit(1); }
})();
