// Removing an order that has several pieces (PAIRREMOVE, pairs-1009 area 8, rule R4). Offline: no browser, no network, nothing live, no paid call.
//   node tests/charm-nest/pairs-remove.cjs
// Words: PIECE = one physical charm; GROUP = every piece of one order line (receiptId:transactionId); a PAIR of earrings is a LEFT and a RIGHT
// piece per unit (matching or mismatched, Paul 9 Oct 18:47); DISCS = n pieces of one group, no sides.
// Proves:
//   1. charm-nest-pair-remove.js (window.PairRemove): the group of a piece is found from its pool id, row, charm or line; a removal that names one
//      piece is widened to the whole group across rows, pool rows and sheets; who would be left behind and where is told; the plain words for
//      a pair, several pairs, discs and a single piece (a single piece and a pendant with two copies say nothing new); gone pieces are never members.
//   2. the Hold plan (OrderHold.planFrom): a pair on two sheets says "Its pair comes off together: the left earring on A and the right earring on B";
//      a single-piece order's plan is exactly what it was (same sentences, same fields).
//   3. the cancel record (_orderCancel over the fake Firestore with the no-nested-arrays check): a pair line keeps kind and pieces, a sheet's step keeps
//      pieces and sides, a single line keeps none of them; a second cancel and a second step never remove anything (cancelled orders are permanent).
//   4. the server takes a pair off whole when the page names both pieces on two sheets, and says `extended` when it had to add a mate.
const path = require('path'), fs = require('fs'), vm = require('vm'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');
const F = require('./pairs-fixtures.cjs');
const PR = require(path.join(root, 'charm-nest-pair-remove.js'));

const results = [];
const t = async (name, fn) => { try { await fn(); results.push([name, null]); console.log('  ok  ' + name); } catch (e) { results.push([name, e]); console.log('  FAIL ' + name + '\n      ' + String(e && e.stack || e).split('\n').slice(0, 4).join('\n      ')); } };
const J = x => JSON.parse(JSON.stringify(x));

// a pair line (rid 4190000009 tx 5000000010): left on GF Sheet 1, right on GF Sheet 3; a 3-disc line; a single pendant; two copies of a pendant
const pid = (rid, tx, copy) => `${rid}_${tx}_${copy}`;
const MIS = { rid: '4190000009', tx: '5000000010' }, DISC = { rid: '4190000011', tx: '5000000012' }, ONE = { rid: '4190000013', tx: '5000000014' }, TWO = { rid: '4190000015', tx: '5000000016' };
const row = (o, ids, spec) => ({ key: `${o.rid}_${o.tx}`, order: { receiptId: o.rid }, line: { transactionId: o.tx, quantity: 1 }, poolIds: ids, spec: spec || {} });
const pools = [
  { poolId: pid(MIS.rid, MIS.tx, 1), orderId: MIS.rid, transactionId: MIS.tx, side: 'L', form: 'earrings', groupSize: 2, state: 'written', sheetId: 'sh-1' },
  { poolId: pid(MIS.rid, MIS.tx, 2), orderId: MIS.rid, transactionId: MIS.tx, side: 'R', form: 'earrings', groupSize: 2, state: 'written', sheetId: 'sh-3' },
  // (a counted necklace, 3 discs of ONE necklace: its pieces say they are a group of 3, no ear; Paul 9 Oct, ruling after ADVCOMPAT 1 and 2)
  { poolId: pid(DISC.rid, DISC.tx, 1), orderId: DISC.rid, transactionId: DISC.tx, groupSize: 3, state: 'written' }, { poolId: pid(DISC.rid, DISC.tx, 2), orderId: DISC.rid, transactionId: DISC.tx, groupSize: 3, state: 'written' },
  { poolId: pid(DISC.rid, DISC.tx, 3), orderId: DISC.rid, transactionId: DISC.tx, groupSize: 3, state: 'written' },
  { poolId: pid(ONE.rid, ONE.tx, 1), orderId: ONE.rid, transactionId: ONE.tx, state: 'written' },
  { poolId: pid(TWO.rid, TWO.tx, 1), orderId: TWO.rid, transactionId: TWO.tx, form: 'pendant', state: 'written' }, { poolId: pid(TWO.rid, TWO.tx, 2), orderId: TWO.rid, transactionId: TWO.tx, form: 'pendant', state: 'written' },
];
const rows = [row(MIS, [pid(MIS.rid, MIS.tx, 1), pid(MIS.rid, MIS.tx, 2)], { form: 'earrings', pieceCount: 2 }), row(DISC, [1, 2, 3].map(c => pid(DISC.rid, DISC.tx, c)), { pieceCount: 3 }), row(ONE, [pid(ONE.rid, ONE.tx, 1)]), Object.assign(row(TWO, [1, 2].map(c => pid(TWO.rid, TWO.tx, c)), { form: 'pendant' }), { line: { transactionId: TWO.tx, quantity: 2 } })];
const where = { [pid(MIS.rid, MIS.tx, 1)]: 'GF Sheet 1', [pid(MIS.rid, MIS.tx, 2)]: 'GF Sheet 3', [pid(DISC.rid, DISC.tx, 1)]: 'GF Sheet 1', [pid(DISC.rid, DISC.tx, 2)]: 'GF Sheet 1', [pid(DISC.rid, DISC.tx, 3)]: 'GF Sheet 2',
  [pid(ONE.rid, ONE.tx, 1)]: 'GF Sheet 1', [pid(TWO.rid, TWO.tx, 1)]: 'GF Sheet 2', [pid(TWO.rid, TWO.tx, 2)]: 'GF Sheet 2' };
const whereOf = id => where[id] || '';
const items = ids => PR.itemsFor(ids, { pools }, whereOf, false);

(async () => {
  console.log('PairRemove (the shared removal helper)');
  await t('a group is found from a pool id, a pool row, a charm and an Orders row', () => {
    assert.equal(PR.keyOf(pid(MIS.rid, MIS.tx, 2)), `${MIS.rid}:${MIS.tx}`);
    assert.equal(PR.keyOf(pools[0]), `${MIS.rid}:${MIS.tx}`);
    assert.equal(PR.keyOfRow(rows[1]), `${DISC.rid}:${DISC.tx}`);
    assert.equal(PR.keyOfCharm({ poolId: pid(ONE.rid, ONE.tx, 1) }), `${ONE.rid}:${ONE.tx}`);
    assert.equal(PR.keyOf('not-a-piece'), '', 'nothing usable is no group');
    assert.equal(PR.keyOf({ receiptId: '4190000099', transactionId: '' }), '', 'a line with no transaction id has no group (it would pull in the whole order)');
  });
  await t('naming one piece takes the whole group: a pair over two sheets, 3 discs, and not another order', () => {
    const src = { rows, pools };
    const e = PR.expand([pid(MIS.rid, MIS.tx, 1)], src);
    assert.deepEqual([...e.ids].sort(), [pid(MIS.rid, MIS.tx, 1), pid(MIS.rid, MIS.tx, 2)]);
    assert.deepEqual([...e.added], [pid(MIS.rid, MIS.tx, 2)]);
    assert.deepEqual([...PR.expand([pid(DISC.rid, DISC.tx, 3)], src).ids].sort(), [1, 2, 3].map(c => pid(DISC.rid, DISC.tx, c)));
    assert.deepEqual([...PR.expand([pid(ONE.rid, ONE.tx, 1)], src).added], [], 'a single piece adds nothing');
    assert.deepEqual([...PR.expand([pid(MIS.rid, MIS.tx, 1), pid(MIS.rid, MIS.tx, 2)], src).added], [], 'asking for the whole group adds nothing');
    // two copies of a PLAIN quantity-2 line (a pendant bought twice) each stand alone: naming one takes one (Paul 9 Oct, ADVCOMPAT 1 and 2)
    assert.deepEqual([...PR.expand([pid(TWO.rid, TWO.tx, 1)], src).added], [], 'a plain copy adds nothing');
    assert.deepEqual([...PR.expand([pid(TWO.rid, TWO.tx, 1)], { pools }).added], [], 'a plain copy adds nothing, from the pool rows alone');
    assert.deepEqual([...PR.expand([pid(DISC.rid, DISC.tx, 3)], { rows }).ids].sort(), [1, 2, 3].map(c => pid(DISC.rid, DISC.tx, c)), 'counted discs: the Orders row says the line makes 3 pieces for 1 unit, a group');
  });
  await t('the group is read from the charms on the sheets too (no Orders row, no pool row)', () => {
    const charms = [{ poolId: pid(MIS.rid, MIS.tx, 1), groupSize: 2, side: 'L' }, { poolId: pid(MIS.rid, MIS.tx, 2), groupSize: 2, side: 'R' }];
    assert.deepEqual([...PR.expand([pid(MIS.rid, MIS.tx, 2)], { charms }).ids].sort(), charms.map(c => c.poolId));
    const plain = [{ poolId: pid(TWO.rid, TWO.tx, 1) }, { poolId: pid(TWO.rid, TWO.tx, 2) }];
    assert.deepEqual([...PR.expand([pid(TWO.rid, TWO.tx, 2)], { charms: plain }).ids], [pid(TWO.rid, TWO.tx, 2)], 'charms of a plain line: one stands alone');
  });
  await t('gone pieces (abandoned, superseded) are never members', () => {
    const gone = pools.map(p => p.poolId === pid(MIS.rid, MIS.tx, 2) ? Object.assign({}, p, { state: 'abandoned' }) : p);
    assert.deepEqual([...PR.expand([pid(MIS.rid, MIS.tx, 1)], { pools: gone }).added], []);
  });
  await t('who is left behind and where (partnersOutside, splitBy)', () => {
    const left = PR.partnersOutside([pid(MIS.rid, MIS.tx, 1)], { pools }, whereOf);
    assert.deepEqual(left, [{ id: pid(MIS.rid, MIS.tx, 2), groupKey: `${MIS.rid}:${MIS.tx}`, where: 'GF Sheet 3' }]);
    assert.deepEqual(PR.partnersOutside([pid(MIS.rid, MIS.tx, 1), pid(MIS.rid, MIS.tx, 2)], { pools }, whereOf), []);
    const cut = PR.splitBy([pid(DISC.rid, DISC.tx, 1), pid(DISC.rid, DISC.tx, 2)], { pools }, whereOf);
    assert.equal(cut.length, 1); assert.deepEqual(cut[0].outside, [{ id: pid(DISC.rid, DISC.tx, 3), where: 'GF Sheet 2' }]);
    assert.deepEqual(PR.splitBy([pid(ONE.rid, ONE.tx, 1)], { pools }, whereOf), [], 'a single piece splits nothing');
    assert.deepEqual(PR.partnersOutside([pid(TWO.rid, TWO.tx, 1)], { pools, rows }, whereOf), [], 'one copy of a plain line leaves nobody behind');
    assert.deepEqual(PR.splitBy([pid(TWO.rid, TWO.tx, 1)], { pools, rows }, whereOf), [], 'one copy of a plain line splits nothing');
  });
  await t('plain words: a pair names its sides and sheets', () => {
    const ids = [pid(MIS.rid, MIS.tx, 1), pid(MIS.rid, MIS.tx, 2)];
    assert.deepEqual(PR.describe(items(ids)), ['Its pair: the left earring on GF Sheet 1 and the right earring on GF Sheet 3.']);
    const same = items(ids).map(i => Object.assign({}, i, { where: 'GF Sheet 1' }));
    assert.deepEqual(PR.describe(same), ['Its pair: the left and right earrings on GF Sheet 1.']);
    const g = PR.groupsOf(items(ids))[0]; assert.equal(g.kind, 'pair'); assert.equal(g.size, 2); assert.deepEqual(g.sheets, ['GF Sheet 1', 'GF Sheet 3']);
    assert.equal(PR.groupsOf(items(ids).map(i => Object.assign({ mismatched: true }, i)))[0].kind, 'mismatched', 'a mismatched design says so when the caller knows');
  });
  await t('plain words: two pairs in one line (quantity 2) say counts per sheet', () => {
    const four = ['L', 'R', 'L', 'R'].map((side, k) => ({ id: pid(MIS.rid, MIS.tx, k + 1), groupKey: `${MIS.rid}:${MIS.tx}`, side, where: k < 2 ? 'GF Sheet 1' : 'GF Sheet 2' }));
    assert.deepEqual(PR.describe(four), ['Its 4 earrings: 1 left and 1 right earrings on GF Sheet 1 and 1 left and 1 right earrings on GF Sheet 2.']);
  });
  await t('plain words: discs are "pieces" and speak only when spread over sheets; a pendant with two copies on one sheet says nothing; a single piece nothing', () => {
    assert.deepEqual(PR.describe(items([1, 2, 3].map(c => pid(DISC.rid, DISC.tx, c)))), ['Its 3 pieces: 2 on GF Sheet 1 and 1 on GF Sheet 2.']);
    const oneSheet = items([1, 2, 3].map(c => pid(DISC.rid, DISC.tx, c))).map(i => Object.assign({}, i, { where: 'GF Sheet 1' }));
    assert.deepEqual(PR.describe(oneSheet), []);
    assert.deepEqual(PR.describe(items([1, 2].map(c => pid(TWO.rid, TWO.tx, c)))), [], 'two copies of a pendant are not "a pair"');
    assert.deepEqual(PR.describe(items([pid(ONE.rid, ONE.tx, 1)])), []);
    assert.deepEqual(PR.groupsOf(items([1, 2].map(c => pid(TWO.rid, TWO.tx, c)))), [], 'the copies of a plain line are no group, so no words at all');
    const spread = items([1, 2].map(c => pid(TWO.rid, TWO.tx, c))).map((i, k) => Object.assign({}, i, { where: 'GF Sheet ' + (k + 1) }));
    assert.deepEqual(PR.describe(spread), [], 'two plain copies on two sheets: nothing is said (they each stand alone)');
  });
  await t('sides: a stored side wins; a mismatched design or earrings that are one of several fall back to the copy number; anything else has none', () => {
    assert.equal(PR.sideOfPiece({ poolId: pid(ONE.rid, ONE.tx, 1), side: 'R' }, false), 'R');
    assert.equal(PR.sideOfPiece({ poolId: pid(MIS.rid, MIS.tx, 1) }, true), 'L');
    assert.equal(PR.sideOfPiece({ poolId: pid(MIS.rid, MIS.tx, 2) }, true), 'R');
    assert.equal(PR.sideOfPiece({ poolId: pid(MIS.rid, MIS.tx, 4), form: 'studs', groupSize: 4 }, false), 'R');
    assert.equal(PR.sideOfPiece({ poolId: pid(ONE.rid, ONE.tx, 1) }, false), null);
    assert.equal(PR.sideOfPiece({ poolId: pid(ONE.rid, ONE.tx, 1), form: 'earrings' }, false), null, 'one piece alone (no group size) is no side');
    assert.equal(PR.pieceLabel('MITTENS', 'R'), 'MITTENS · right earring'); assert.equal(PR.pieceLabel('PENDANT', null, 1, 2), 'PENDANT · 1 of 2'); assert.equal(PR.pieceLabel('PENDANT'), 'PENDANT');
  });
  await t('lineInfo: what a cancel record keeps of a line; a single piece keeps nothing', () => {
    assert.deepEqual(PR.lineInfo({ poolIds: [pid(MIS.rid, MIS.tx, 1), pid(MIS.rid, MIS.tx, 2)], spec: { form: 'earrings' } }, { entryFor: () => null }), { pieces: 2, kind: 'pair' });
    assert.deepEqual(PR.lineInfo({ poolIds: [1, 2, 3].map(c => pid(DISC.rid, DISC.tx, c)), spec: {} }, { entryFor: () => null }), { pieces: 3, kind: 'multi' });
    assert.deepEqual(PR.lineInfo({ poolIds: [pid(ONE.rid, ONE.tx, 1)], spec: {} }, { entryFor: () => null }), { pieces: 1, kind: 'single' });
    assert.deepEqual(PR.lineInfo({ poolIds: [1, 2].map(c => pid(TWO.rid, TWO.tx, c)), spec: { form: 'pendant' } }, { entryFor: () => null }), { pieces: 2, kind: 'multi' }, 'two pendants are not a pair of earrings');
  });

  await t('a deleted sheet that split a pair says so (the server\'s splits); a whole group says nothing', () => {
    assert.deepEqual(PR.deletedSplits([{ groupKey: `${MIS.rid}:${MIS.tx}`, here: 1, of: 2 }]), [`Order ${MIS.rid}: 1 of its 2 pieces was on that sheet and is on no sheet now; the other one stays on its sheet.`]);
    assert.deepEqual(PR.deletedSplits([{ groupKey: 'x:y', here: 2, of: 2 }, null]), []); assert.deepEqual(PR.deletedSplits(undefined), []);
  });

  console.log('Hold plan (OrderHold.planFrom)');
  const src = fs.readFileSync(path.join(root, 'charm-nest-order-hold.js'), 'utf8');
  const loadHold = withPair => { const win = withPair ? { PairRemove: PR } : {}; vm.runInNewContext(src, { window: win, console, setTimeout, Date, localStorage: undefined }); return snap => J(win.OrderHold.planFrom(snap)); };
  const piece = (id, sheet, extra) => Object.assign({ poolId: id, lineKey: 'k:' + id, label: 'CHARM · ' + id, metal: 'gold', status: 'off', why: '', sheetId: sheet.replace(/ /g, '-').toLowerCase(), sheetLabel: sheet, setId: null, setLabel: '' }, extra);
  const base = pieces => ({ rid: MIS.rid, label: MIS.rid, customer: 'Jo Buyer', shipBy: 1790500000, cancelled: false, held: false, rowsLeft: 1, pieces,
    sheets: { 'gf-sheet-1': { label: 'GF Sheet 1', setLabel: '', metal: 'gold', removes: 1, spots: 1, fillable: true }, 'gf-sheet-3': { label: 'GF Sheet 3', setLabel: '', metal: 'gold', removes: 1, spots: 1, fillable: true } } });
  await t('a pair on two sheets: the plan says it comes off together, with sides and sheets', () => {
    const plan = loadHold(true)(base([piece('a1', 'GF Sheet 1', { side: 'L', groupKey: `${MIS.rid}:${MIS.tx}`, form: 'earrings' }), piece('a2', 'GF Sheet 3', { side: 'R', groupKey: `${MIS.rid}:${MIS.tx}`, form: 'earrings' })]));
    assert(plan.canHold);
    assert(plan.effects.includes('Its pair comes off together: the left earring on GF Sheet 1 and the right earring on GF Sheet 3.'), plan.effects.join(' | '));
    assert.equal(plan.pieces[0].side, 'L'); assert.equal(plan.pieces[1].groupKey, `${MIS.rid}:${MIS.tx}`);
  });
  await t('a single-piece order (and the same plan without the helper) reads exactly as before', () => {
    const one = base([piece('b1', 'GF Sheet 1')]);
    const withHelper = loadHold(true)(one), without = loadHold(false)(one);
    assert.deepEqual(withHelper, without, 'same plan, field for field');
    assert(!withHelper.effects.some(e => /pair|earring/i.test(e)), withHelper.effects.join(' | '));
    assert(!('side' in withHelper.pieces[0]) && !('groupKey' in withHelper.pieces[0]));
    const noSides = base([piece('c1', 'GF Sheet 1'), piece('c2', 'GF Sheet 3')]);
    assert.deepEqual(loadHold(true)(noSides), loadHold(false)(noSides), 'two plain pieces with no side or group: unchanged');
  });

  console.log('Cancel record (permanent) and the server');
  const OrderCancel = require(path.join(root, 'netlify/functions/_orderCancel.js'));
  await t('a pair line keeps kind and pieces; a single line keeps neither (old records unchanged)', () => {
    const rec = OrderCancel.record({ orderId: MIS.rid, by: 'Paul', why: 'asked', lines: [{ transactionId: MIS.tx, sku: 'MITTENS', title: 'Mittens', quantity: 1, kind: 'pair', pieces: 2 }, { transactionId: ONE.tx, sku: 'DUCK', title: 'Duck', quantity: 1 }, { transactionId: TWO.tx, quantity: 1, kind: 'bogus', pieces: 9 }] });
    assert.equal(rec.lines[0].kind, 'pair'); assert.equal(rec.lines[0].pieces, 2);
    assert(!('kind' in rec.lines[1]) && !('pieces' in rec.lines[1]), 'a single line has neither');
    assert(!('kind' in rec.lines[2]), 'an unknown kind is dropped');
  });
  await t('a removal keeps pieces and sides; merging never removes or shrinks anything', () => {
    const a = OrderCancel.removalOf({ id: 'sheet~GF Sheet 1', where: 'GF Sheet 1', kind: 'sheet', outcome: 'waiting', pieces: 2, sides: 'L,R<script>' }, 1000);
    assert.equal(a.pieces, 2); assert.equal(a.sides, 'L,R', 'only L, R, comma and dash survive');
    const plain = OrderCancel.removalOf({ id: 'sheet~GF Sheet 2', where: 'GF Sheet 2', kind: 'sheet', outcome: 'removed' }, 1000);
    assert(!('pieces' in plain) && !('sides' in plain), 'a single piece adds nothing');
    const m1 = OrderCancel.mergeRemovals([], [a, plain]); assert.equal(m1.list.length, 2);
    const m2 = OrderCancel.mergeRemovals(m1.list, [OrderCancel.removalOf({ id: 'sheet~GF Sheet 1', where: 'GF Sheet 1', kind: 'sheet', outcome: 'removed' }, 2000)]);
    assert.equal(m2.list.length, 2); const e = m2.list.find(x => x.id === 'sheet~GF Sheet 1');
    assert.equal(e.outcome, 'removed'); assert.deepEqual(e.was, { outcome: 'waiting', at: 1000 }); assert.equal(e.pieces, 2, 'pieces stay');
    const m3 = OrderCancel.mergeRemovals(m2.list, [OrderCancel.removalOf({ id: 'sheet~GF Sheet 1', where: 'GF Sheet 1', kind: 'sheet', outcome: 'waiting' }, 3000)]);
    assert.equal(m3.list.find(x => x.id === 'sheet~GF Sheet 1').outcome, 'removed', 'a final outcome never goes back to waiting');
  });

  const world = F.cases.mismatchedSplit(), pairKey = '4190000009:5000000010', L = '4190000009_5000000010_1', R = '4190000009_5000000010_2';
  const mount = () => { const st = F.fakeFirestore(); st.seed(world); const fns = F.functions(st, ['charmNestLibrary']); return { st, fns }; };
  await t('server: the cancel record of a pair keeps its steps; a second cancel and a second step remove nothing (no array in an array anywhere)', async () => {
    const { st, fns } = mount();
    try {
      const put = await fns.lib('cancelPut', { orderId: '4190000009', by: 'Paul', why: 'buyer asked', record: { buyer: 'Jo', lines: [{ transactionId: '5000000010', sku: 'MITTENS-MIS', title: 'Mittens', quantity: 1, kind: 'mismatched', pieces: 2 }] } });
      assert(put.ok, JSON.stringify(put));
      const fates = await fns.lib('cancelFates', { orderId: '4190000009', by: 'Paul', fates: [{ sheet: 'GF Sheet 1', fate: 'removed', text: 'taken off', pieces: 1, sides: 'L' }, { sheet: 'GF Sheet 3', fate: 'cut', text: 'on a cut sheet', pieces: 1, sides: 'R' }] });
      assert(fates.ok, JSON.stringify(fates));
      let rec = st.get('Charm_Nest_Cancelled', '4190000009');
      assert.equal(rec.lines[0].kind, 'mismatched'); assert.equal(rec.lines[0].pieces, 2);
      assert.equal(rec.removals.length, 2);
      assert.deepEqual(rec.removals.map(r => [r.where, r.outcome, r.pieces, r.sides]).sort(), [['GF Sheet 1', 'removed', 1, 'L'], ['GF Sheet 3', 'setAside', 1, 'R']]);
      // again: the same cancel, the same steps, and a step with no pair words: nothing goes
      await fns.lib('cancelPut', { orderId: '4190000009', by: 'Tess', why: 'again', record: {} });
      await fns.lib('cancelFates', { orderId: '4190000009', by: 'Tess', fates: [{ sheet: 'GF Sheet 1', fate: 'removed', text: 'taken off' }] });
      rec = st.get('Charm_Nest_Cancelled', '4190000009');
      assert.equal(rec.removals.length, 2, 'no removal is ever dropped'); assert.equal(rec.removals.find(r => r.where === 'GF Sheet 1').pieces, 1, 'the pair words stay');
    } finally { fns.restore(); }
  });
  await t('server: a hold that names both pieces (two sheets) takes both off and both sheets let them go; naming one on a sheet with its mate says extended', async () => {
    const { st, fns } = mount();
    try {
      const out = await fns.lib('poolUpdate', { poolIds: [L, R], patch: { state: 'abandoned', sheetId: null, setId: null, heldBy: 'Paul', heldReason: 'on hold', heldAt: Date.now() } });
      assert(out.ok, JSON.stringify(out)); assert(!out.extended, 'the page named both: the server had nothing to add');
      for (const id of [L, R]) assert.equal(st.get('Charm_Pool', id).state, 'abandoned');
      for (const s of world.sheets) { const rec = st.get('Charm_Nest_Sheets', s.id); assert(!(rec.poolIds || []).includes(L) && !(rec.poolIds || []).includes(R), `${s.id} still lists a piece of the pair`); }
      // the other pairs and pieces on those sheets stay
      assert(world.sheets.some(s => (st.get('Charm_Nest_Sheets', s.id).poolIds || []).length > 0), 'the rest of the sheets stay as they were');
    } finally { fns.restore(); }
  });

  await t('server: a stale page names one piece of a pair on one sheet: its mate comes off in the same commit and is told (extended); a single charm is never extended', async () => {
    const st = F.fakeFirestore(); st.seed(F.cases.pairOneSheet()); const fns = F.functions(st, ['charmNestLibrary']);
    try {
      const a = '4190000001_5000000010_1', b = '4190000001_5000000010_2', other = '4190000090_5000000090_1';
      const out = await fns.lib('poolUpdate', { poolIds: [a], patch: { state: 'abandoned', sheetId: null, setId: null, heldBy: 'Paul', heldReason: 'on hold', heldAt: Date.now() } });
      assert.deepEqual(out.extended, [b], JSON.stringify(out));
      assert.equal(st.get('Charm_Pool', b).state, 'abandoned'); assert.deepEqual(st.get('Charm_Nest_Sheets', 'sh-gf1').poolIds, [other], 'only the pair left the sheet');
      const one = await fns.lib('poolUpdate', { poolIds: [other], patch: { state: 'abandoned', sheetId: null, setId: null, heldBy: 'Paul', heldReason: 'on hold', heldAt: Date.now() } });
      assert(!('extended' in one), 'a single charm adds nothing: ' + JSON.stringify(one));
    } finally { fns.restore(); }
  });

  const bad = results.filter(r => r[1]);
  console.log(`\npairs-remove: ${results.length - bad.length} passed, ${bad.length} failed`);
  process.exit(bad.length ? 1 : 0);
})();
