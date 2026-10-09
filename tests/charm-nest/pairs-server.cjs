// Pairs, mismatched pairs and multi-piece orders on the SERVER (Paul, 9 Oct 2026; area 16, PAIRSERVER).
//
// What it proves, over the real charmNestLibrary handler on the in-memory Firestore (bridge-server.cjs; nothing live, no Etsy, no paid call):
//   · the server decides by POOL ID and ORDER, never by SKU or design, so a mismatched pair (a left and a right charm of two DIFFERENT designs
//     under one line) is held to the same rules as any multi-piece order: a sheet holding one ear cannot leave its set while the other ear is on
//     another sheet of it, with the page's one sentence; a sheet that shares nothing still can; a piece taken off on purpose cannot be put back;
//   · a line comes off whole: a hold or cancel that names one piece takes the rest of the line off the sheets it already reads, in the same commit,
//     and says so (`extended`); a sibling on a cut sheet stays; a single charm and a line sent whole behave exactly as before;
//   · getOrderPieces says plainly that a pair is split (`splits`: which piece on which sheet, the side of each) and says nothing for a whole pair
//     or a single charm; deleteSheet says which pairs it left with a piece elsewhere;
//   · poolPut keeps the pair fields it can vouch for (side, bodyIndex, groupSize, groupKey from the pool id) and writes a row without them as before;
//   · the master index keeps `pair` (write, read, list), a re-index that says nothing leaves it, null takes it away, a person's decision stands;
//   · a back keeps the ear it is for on the sheet record; the timeline says "left and right" and how many of the sheets that hold an order are cut;
//   · no document the fake stores has an array inside an array (_noNestedArrays.cjs).
//   node tests/charm-nest/pairs-server.cjs
const path = require('path'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');
const refuseNestedArrays = require('./_noNestedArrays.cjs');
const { start } = require('./bridge-server.cjs');
const Placement = require(path.join(root, 'netlify/functions/_charmNestPlacement.js'));
const Timeline = require(path.join(root, 'netlify/functions/_orderTimeline.js'));
const Master = require(path.join(root, 'netlify/functions/_charmNestMaster.js'));
const Readiness = require(path.join(root, 'charm-nest-readiness.js'));
const SetEdit = require(path.join(root, 'charm-nest-set-edit.js'));
const Pair = require(path.join(root, 'charm-nest-pair.js'));

const results = [];
const t = async (name, fn) => { try { await fn(); results.push([name, null]); console.log('  ok  ' + name); } catch (e) { results.push([name, e]); console.log('  FAIL ' + name + '\n      ' + String(e && e.message || e).split('\n').slice(0, 4).join('\n      ')); } };

// ── the fixtures: one order line of a mismatched design (a left charm and a right charm), sheets of every kind ──
const DAY = '2026-10-09', SET = 'set-2026-10-09-1', NOW = Date.now();
const line = (rid, tid) => ({ rid, tid, key: `${rid}_${tid}`, L: `${rid}_${tid}_1`, R: `${rid}_${tid}_2`, group: `${rid}:${tid}` });
const MIS = line('4200000001', '9100000001');       // the mismatched pair: pieces on two sheets of one committed set
const SOLO = line('4200000002', '9100000002');      // a single charm on its own sheet of that set
const WHOLE = line('4200000003', '9100000003');     // a pair of two pieces, both on one editable sheet
const SINGLE = line('4200000004', '9100000004');    // a single charm on an editable sheet
const CUTP = line('4200000005', '9100000005');      // a pair: one piece on an editable sheet, the other on a CUT sheet
const SPLIT = line('4200000006', '9100000006');     // a pair on two editable sheets
const pool = (l, copy, o = {}) => Object.assign({ poolId: copy === 1 ? l.L : l.R, orderId: l.rid, transactionId: l.tid, lineKey: l.key, sku: copy === 1 ? 'MITTENS 1' : 'MITTENS 2', material: 'gold', copy, quantity: 2, state: 'written', runId: 'run-pr', createdAt: NOW - 120000, side: copy === 1 ? 'L' : 'R', bodyIndex: copy - 1, groupKey: l.group, groupSize: 2 }, o);
const sheet = (id, idx, poolIds, o = {}) => Object.assign({ id, metal: 'gold', sheetIndex: idx, page: idx, day: DAY, status: 'written', folder: id, fileBase: id, poolIds, orders: [...new Set(poolIds.map(p => p.split('_')[0]))], runId: 'run-pr', draft: false }, o);

function seed(st) {
  const put = (c, id, d) => st.put(c, id, d);
  // a committed set of three sheets: GF Sheet 1 (the left ear of the mismatched pair), GF Sheet 2 (the right ear), GF Sheet 3 (a single charm of another order)
  put('Charm_Nest_Sets', SET, { setId: SET, seq: 1, day: DAY, name: 'Set-1', runId: 'run-pr', committedAt: NOW - 60000, status: 'committed', sheetIds: ['sheet-pr-a', 'sheet-pr-b', 'sheet-pr-c'], materials: ['gold'], orders: {}, committed: [MIS.rid, SOLO.rid] });
  put('Charm_Nest_Sheets', 'sheet-pr-a', sheet('sheet-pr-a', 1, [MIS.L], { setId: SET, setSeq: 1 }));
  put('Charm_Nest_Sheets', 'sheet-pr-b', sheet('sheet-pr-b', 2, [MIS.R], { setId: SET, setSeq: 1 }));
  put('Charm_Nest_Sheets', 'sheet-pr-c', sheet('sheet-pr-c', 3, [SOLO.L], { setId: SET, setSeq: 1 }));
  for (const [l, copy] of [[MIS, 1], [MIS, 2]]) put('Charm_Pool', l[copy === 1 ? 'L' : 'R'], pool(l, copy, { sheetId: copy === 1 ? 'sheet-pr-a' : 'sheet-pr-b', setId: SET }));
  put('Charm_Pool', SOLO.L, pool(SOLO, 1, { side: null, bodyIndex: 0, groupSize: 1, quantity: 1, sku: 'DUCK 38090', sheetId: 'sheet-pr-c', setId: SET }));
  // editable sheets for the take-off cases
  put('Charm_Nest_Sheets', 'sheet-pr-t1', sheet('sheet-pr-t1', 1, [WHOLE.L, WHOLE.R, SINGLE.L], { charmCount: 3, placedCount: 3 }));
  put('Charm_Pool', WHOLE.L, pool(WHOLE, 1, { sheetId: 'sheet-pr-t1' })); put('Charm_Pool', WHOLE.R, pool(WHOLE, 2, { sheetId: 'sheet-pr-t1' }));
  put('Charm_Pool', SINGLE.L, pool(SINGLE, 1, { side: null, groupSize: 1, quantity: 1, sku: 'DUCK 38090', sheetId: 'sheet-pr-t1' }));
  put('Charm_Nest_Sheets', 'sheet-pr-t2', sheet('sheet-pr-t2', 2, [CUTP.L]));
  put('Charm_Nest_Sheets', 'sheet-pr-t3', sheet('sheet-pr-t3', 3, [CUTP.R], { laserDoneAt: NOW - 3600000, laserDoneBy: 'Laser Lee' }));
  put('Charm_Pool', CUTP.L, pool(CUTP, 1, { sheetId: 'sheet-pr-t2' })); put('Charm_Pool', CUTP.R, pool(CUTP, 2, { sheetId: 'sheet-pr-t3' }));
  put('Charm_Nest_Sheets', 'sheet-pr-t4a', sheet('sheet-pr-t4a', 4, [SPLIT.L])); put('Charm_Nest_Sheets', 'sheet-pr-t4b', sheet('sheet-pr-t4b', 5, [SPLIT.R]));
  put('Charm_Pool', SPLIT.L, pool(SPLIT, 1, { sheetId: 'sheet-pr-t4a' })); put('Charm_Pool', SPLIT.R, pool(SPLIT, 2, { sheetId: 'sheet-pr-t4b' }));
}

(async () => {
  const srv = await start(); const st = srv.st; st.atomic = true; seed(st);
  const call = async (body, sandbox) => { const r = await fetch(`${srv.sorterOrigin}/.netlify/functions/charmNestLibrary`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(sandbox ? Object.assign({ sandbox: true }, body) : body) }); return { status: r.status, body: await r.json() }; };
  const row = id => st.doc('Charm_Pool', id), rec = id => st.doc('Charm_Nest_Sheets', id);
  const HOLD = (extra = {}) => Object.assign({ state: 'abandoned', sheetId: null, setId: null, heldBy: 'Paul', heldReason: 'on hold', heldAt: NOW }, extra);
  try {
    // ═══ 1 · a sheet that holds one ear of a mismatched pair cannot leave its set (same sentence as the page) ═══
    await t('1a the left ear and the right ear are different designs on different sheets, and the server still sees ONE order shared between them (sharedOrders)', async () => {
      assert.notEqual(row(MIS.L).sku, row(MIS.R).sku, 'two different designs');
      const out = (await call({ op: 'sharedOrders', kind: 'sheet', id: 'sheet-pr-a', to: { newSet: true } })).body;
      assert.equal(out.shared.length, 1, JSON.stringify(out.shared));
      assert.equal(out.shared[0].orderId, MIS.rid); assert.deepEqual(out.shared[0].thereIds, ['sheet-pr-b']); assert.equal(out.shared[0].total, 2);
    });
    await t('1b flowApply setMember: taking the left ear\'s sheet out of its set is refused, nothing written, with the order and both sheets named', async () => {
      const before = JSON.stringify([rec('sheet-pr-a'), rec('sheet-pr-b'), st.doc('Charm_Nest_Sets', SET)]);
      const r = await call({ op: 'flowApply', by: 'Paul', device: 'charm-nest-1', via: 'Library move', steps: [{ type: 'setMember', moves: [{ sheetId: 'sheet-pr-a', to: null }] }] });
      assert.equal(r.status, 409, JSON.stringify(r.body));
      assert(new RegExp(`Order ${MIS.rid} has (pieces on GF Sheet 1 and GF Sheet 2|its left earring on GF Sheet 1 and its right earring on GF Sheet 2)`).test(r.body.error), r.body.error);   // (PAIRSETS: a refusal says what the pieces are when the pool rows tell the side)
      assert(/stay in one set/.test(r.body.error));
      assert.equal(JSON.stringify([rec('sheet-pr-a'), rec('sheet-pr-b'), st.doc('Charm_Nest_Sets', SET)]), before, 'nothing was written');
    });
    await t('1c the right ear\'s sheet is refused the same way; and both together are refused too (each is checked on its own)', async () => {
      for (const moves of [[{ sheetId: 'sheet-pr-b', to: null }], [{ sheetId: 'sheet-pr-a', to: null }, { sheetId: 'sheet-pr-b', to: null }]]) {
        const r = await call({ op: 'flowApply', by: 'Paul', steps: [{ type: 'setMember', moves }] });
        assert.equal(r.status, 409, JSON.stringify(r.body)); assert(new RegExp(MIS.rid).test(r.body.error), r.body.error);
      }
    });
    await t('1d a sheet that shares no order with another still leaves the set (single charm behaviour is unchanged)', async () => {
      const r = await call({ op: 'flowApply', by: 'Paul', steps: [{ type: 'setMember', moves: [{ sheetId: 'sheet-pr-c', to: null }] }] });
      assert.equal(r.status, 200, JSON.stringify(r.body)); assert.equal(rec('sheet-pr-c').setId, null); assert.equal(rec('sheet-pr-c').draft, true);
      assert.deepEqual(st.doc('Charm_Nest_Sets', SET).sheetIds.slice().sort(), ['sheet-pr-a', 'sheet-pr-b']);
    });

    // ═══ 2 · a piece taken off on purpose is never put back, piece by piece ═══
    await t('2 putSheet: a held piece is refused, its sibling (not held) is accepted, a piece listed twice is refused; the back keeps its ear', async () => {
      // hold only the right ear of SPLIT's sheet row directly (the take-off marks), then try to put it back on a record
      st.put('Charm_Pool', SPLIT.R, { state: 'abandoned', sheetId: null, heldBy: 'Paul', heldAt: NOW, heldReason: 'on hold' });
      const back = await call({ op: 'putSheet', sheet: { id: 'sheet-pr-t4a', poolIds: [SPLIT.L, SPLIT.R] } });
      assert.equal(back.status, 409, JSON.stringify(back.body)); assert(new RegExp(SPLIT.R).test(back.body.error), back.body.error);
      const twice = await call({ op: 'putSheet', sheet: { id: 'sheet-pr-t4a', poolIds: [SPLIT.L, SPLIT.L] } });
      assert.equal(twice.status, 409, JSON.stringify(twice.body)); assert(/twice/.test(twice.body.error));
      const ok = await call({ op: 'putSheet', sheet: { id: 'sheet-pr-t4a', poolIds: [SPLIT.L], backPool: [{ poolId: SPLIT.L, sheetId: 'sheet-pr-t4a', order: SPLIT.rid, sku: 'MITTENS 1', copy: 1, side: 'L', text: 'Anna', approvedAt: NOW, approvedBy: 'Paul' }] } });
      assert.equal(ok.status, 200, JSON.stringify(ok.body));
      const b0 = rec('sheet-pr-t4a').backPool.find(b => b.poolId === SPLIT.L); assert.equal(b0.side, 'L', 'the sheet record keeps which ear the back is for');
      const listed = (await call({ op: 'listSheets', limit: 50 })).body.sheets.find(s => s.id === 'sheet-pr-t4a'); assert.equal(listed.backs[0].side, 'L');
      // put it back for the next cases
      st.put('Charm_Pool', SPLIT.R, { state: 'written', sheetId: 'sheet-pr-t4b', heldBy: null, heldAt: null, heldReason: null });
    });
    await t('2b a back for a piece the sheet does not list is refused (the right ear\'s back cannot land on the left ear\'s sheet)', async () => {
      const r = await call({ op: 'backPut', backs: [{ poolId: SPLIT.R, sheetId: 'sheet-pr-t4a', order: SPLIT.rid, copy: 2, side: 'R', text: 'Ben', approvedAt: NOW, approvedBy: 'Paul' }] });
      assert.notEqual(r.status, 200, JSON.stringify(r.body)); assert(/does not contain this exact charm copy/.test(JSON.stringify(r.body)));
    });

    // ═══ 3 · a line comes off whole ═══
    await t('3a a hold that names only the left piece takes the right piece of the line off the same sheet, in the same commit, and says so', async () => {
      const r = await call({ op: 'poolUpdate', poolIds: [WHOLE.L], patch: HOLD() });
      assert.equal(r.status, 200, JSON.stringify(r.body)); assert.deepEqual(r.body.extended, [WHOLE.R]);
      for (const id of [WHOLE.L, WHOLE.R]) { assert.equal(row(id).state, 'abandoned'); assert.equal(row(id).sheetId, null); assert.equal(row(id).heldBy, 'Paul'); }
      const s1 = rec('sheet-pr-t1'); assert.deepEqual(s1.poolIds, [SINGLE.L], 'the other order stays where it is'); assert.deepEqual(s1.orders, [SINGLE.rid]);
      assert.equal(s1.placedCount, 1); assert.equal(s1.charmCount, 1); assert.equal(s1.dirty, true);
      assert.equal(row(SINGLE.L).state, 'written', 'the single charm of the other order is untouched');
    });
    await t('3b a single charm (and a line sent whole) is taken off exactly as before: no `extended` in the answer', async () => {
      const r = await call({ op: 'poolUpdate', poolIds: [SINGLE.L], patch: HOLD() });
      assert.equal(r.status, 200); assert.equal('extended' in r.body, false, JSON.stringify(r.body)); assert.equal(row(SINGLE.L).state, 'abandoned'); assert.deepEqual(rec('sheet-pr-t1').poolIds, []);
      const w = await call({ op: 'poolUpdate', poolIds: [SPLIT.L, SPLIT.R], patch: HOLD({ heldReason: 'whole' }) });
      assert.equal(w.status, 200); assert.equal('extended' in w.body, false); assert.deepEqual([rec('sheet-pr-t4a').poolIds, rec('sheet-pr-t4b').poolIds], [[], []]);
      for (const id of [SPLIT.L, SPLIT.R]) assert.equal(row(id).heldReason, 'whole');
      // (put them back on their sheets for the cases below)
      st.put('Charm_Nest_Sheets', 'sheet-pr-t4a', { poolIds: [SPLIT.L], orders: [SPLIT.rid] }); st.put('Charm_Nest_Sheets', 'sheet-pr-t4b', { poolIds: [SPLIT.R], orders: [SPLIT.rid] });
      st.put('Charm_Pool', SPLIT.L, { state: 'written', sheetId: 'sheet-pr-t4a', heldAt: null, heldBy: null, heldReason: null, repooledAt: NOW }); st.put('Charm_Pool', SPLIT.R, { state: 'written', sheetId: 'sheet-pr-t4b', heldAt: null, heldBy: null, heldReason: null, repooledAt: NOW });
    });
    await t('3c a sibling on a CUT sheet stays (a cut sheet is a record of what was made); only what the server may edit comes off', async () => {
      const r = await call({ op: 'poolUpdate', poolIds: [CUTP.L], patch: HOLD() });
      assert.equal(r.status, 200); assert.equal('extended' in r.body, false);
      assert.equal(row(CUTP.L).state, 'abandoned'); assert.equal(row(CUTP.R).state, 'written'); assert.deepEqual(rec('sheet-pr-t3').poolIds, [CUTP.R]);
    });
    await t('3d a cancel names only one piece: the other comes off with it and BOTH are told to the order\'s timeline and cancel record (extended in the answer)', async () => {
      st.put('Charm_Nest_Sheets', 'sheet-pr-t5', sheet('sheet-pr-t5', 6, [MIS.L.replace(MIS.rid, '4200000007'), MIS.R.replace(MIS.rid, '4200000007')]));
      const A = '4200000007_9100000001_1', B = '4200000007_9100000001_2';
      st.put('Charm_Pool', A, pool(Object.assign({}, MIS, { rid: '4200000007', L: A, R: B, key: '4200000007_9100000001', group: '4200000007:9100000001' }), 1, { sheetId: 'sheet-pr-t5' }));
      st.put('Charm_Pool', B, pool(Object.assign({}, MIS, { rid: '4200000007', L: A, R: B, key: '4200000007_9100000001', group: '4200000007:9100000001' }), 2, { sheetId: 'sheet-pr-t5' }));
      const r = await call({ op: 'poolUpdate', poolIds: [A], patch: { state: 'abandoned', sheetId: null, setId: null, removedBy: 'Paul', removedReason: 'cancelled: changed mind', removedAt: NOW }, by: 'Paul' });
      assert.equal(r.status, 200, JSON.stringify(r.body)); assert.deepEqual(r.body.extended, [B]);
      assert.equal(row(B).removedReason, 'cancelled: changed mind');
      const events = st.list('Order_Timeline').filter(e => e.orderId === '4200000007' && e.type === 'removed');
      assert(events.length >= 1 && events.every(e => (e.data.poolIds || []).includes(A) && (e.data.poolIds || []).includes(B)), JSON.stringify(events.map(e => e.data)));
    });

    // ═══ 4 · the server says plainly that a pair is split ═══
    await t('4a getOrderPieces: a pair on two sheets answers `splits` with each piece, its side, its sheet; the pieces carry their side', async () => {
      const o = (await call({ op: 'getOrderPieces', orderIds: [SPLIT.rid] })).body.orders[SPLIT.rid];
      assert.equal(o.splits.length, 1, JSON.stringify(o.splits)); const sp = o.splits[0];
      assert.equal(sp.groupKey, SPLIT.group); assert.deepEqual(sp.sheets.slice().sort(), ['sheet-pr-t4a', 'sheet-pr-t4b']); assert.equal(sp.offSheet, 0);
      assert.deepEqual(sp.pieces.map(p => [p.poolId, p.side, p.sheetId]), [[SPLIT.L, 'L', 'sheet-pr-t4a'], [SPLIT.R, 'R', 'sheet-pr-t4b']]);
      assert.equal(o.placement[SPLIT.L].side, 'L'); assert.equal(o.placement[SPLIT.R].side, 'R');
      assert.equal(JSON.stringify(o.sheets.map(s => s.poolIds.length)), '[1,1]');
    });
    await t('4b a pair whose left ear is held while the right ear is still on a sheet is split too (piece waiting versus piece on a sheet)', async () => {
      await call({ op: 'poolUpdate', poolIds: [SPLIT.L], patch: HOLD() });   // (the right ear is on another sheet the take-off does not read: it stays, and the server says so below)
      const o = (await call({ op: 'getOrderPieces', orderIds: [SPLIT.rid] })).body.orders[SPLIT.rid];
      assert.equal(o.placement[SPLIT.L].state, 'held'); assert.equal(o.placement[SPLIT.R].state, 'sheet');
      assert.equal(o.splits.length, 1); assert.equal(o.splits[0].offSheet, 1); assert.deepEqual(o.splits[0].sheets, ['sheet-pr-t4b']);
    });
    await t('4c a whole pair on one sheet, a single charm, and a pair all on no sheet answer exactly as before: no `splits` key', async () => {
      st.put('Charm_Nest_Sheets', 'sheet-pr-w', sheet('sheet-pr-w', 7, [pool(line('4200000008', '9100000008'), 1).poolId, pool(line('4200000008', '9100000008'), 2).poolId]));
      const W = line('4200000008', '9100000008');
      st.put('Charm_Pool', W.L, pool(W, 1, { sheetId: 'sheet-pr-w' })); st.put('Charm_Pool', W.R, pool(W, 2, { sheetId: 'sheet-pr-w' }));
      const r = (await call({ op: 'getOrderPieces', orderIds: [W.rid, SOLO.rid, SINGLE.rid] })).body.orders;
      for (const id of [W.rid, SOLO.rid, SINGLE.rid]) assert.equal('splits' in r[id], false, id);
      assert.equal(r[W.rid].summary.onSheet, 2);
    });
    await t('4d deleteSheet names the pairs it leaves with a piece elsewhere (from the rows it already reads) and a plain sheet says nothing', async () => {
      const r = await call({ op: 'deleteSheet', id: 'sheet-pr-t4b', code: '975311' });
      assert.equal(r.status, 200, JSON.stringify(r.body)); assert.equal(r.body.unlinked, 1); assert.deepEqual(r.body.splits, [{ groupKey: SPLIT.group, here: 1, of: 2 }]);
      assert.equal(row(SPLIT.R).sheetId, null, 'the piece is on no sheet');
      const p = await call({ op: 'deleteSheet', id: 'sheet-pr-w', code: '975311' });
      assert.equal(p.body.unlinked, 2); assert.equal('splits' in p.body, false, 'a whole pair deleted with its sheet is not split');
    });

    // ═══ 5 · pool rows ═══
    await t('5a poolPut keeps the pair fields it can vouch for, rewrites groupKey from the pool id, drops what is not sound', async () => {
      const P = line('4200000010', '9100000010');
      const r = await call({ op: 'poolPut', pools: [Object.assign(pool(P, 1), { sheetId: null, side: 'X', bodyIndex: 9, groupSize: 99, groupKey: 'zzz' }), Object.assign(pool(P, 2), { sheetId: null, groupKey: 'zzz' })] });
      assert.equal(r.status, 200, JSON.stringify(r.body)); const a = row(P.L), b = row(P.R);
      assert.equal('side' in a, false); assert.equal('bodyIndex' in a, false); assert.equal('groupSize' in a, false); assert.equal(a.groupKey, P.group);
      assert.equal(b.side, 'R'); assert.equal(b.bodyIndex, 1); assert.equal(b.groupSize, 2); assert.equal(b.groupKey, P.group);
    });
    await t('5b a row with none of the pair fields is written exactly as before (no pair key appears)', async () => {
      const r = await call({ op: 'poolPut', pools: [{ poolId: '4200000011_9100000011_1', orderId: '4200000011', transactionId: '9100000011', sku: 'DUCK 38090', material: 'gold', copy: 1, quantity: 1, state: 'ready', runId: 'run-pr' }] });
      assert.equal(r.status, 200); const x = row('4200000011_9100000011_1');
      for (const k of ['side', 'bodyIndex', 'groupKey', 'groupSize']) assert.equal(k in x, false, k);
    });
    await t('5c cleanPiece / groupOfPool / groupMates / splitsOf (the pure helpers) agree with charm-nest-pair.js groupKey', () => {
      assert.equal(Placement.groupOfPool(MIS.L), Pair.groupKey(MIS.L)); assert.equal(Placement.groupOfPool('not a pool id'), '');
      assert.deepEqual(Placement.groupMates({ poolIds: [MIS.L, MIS.R, SOLO.L] }, new Set([MIS.L])), [MIS.R]);
      assert.deepEqual(Placement.groupMates({ poolIds: [SOLO.L] }, new Set([MIS.L])), []);
      assert.deepEqual(Placement.splitsOf({ [MIS.L]: { state: 'sheet', sheetId: 'a' }, [MIS.R]: { state: 'sheet', sheetId: 'a' } }, []), []);
      assert.equal(Placement.splitsOf({ [MIS.L]: { state: 'superseded' }, [MIS.R]: { state: 'sheet', sheetId: 'a' } }, []).length, 0, 'a piece made up again is history, not a split');
    });

    // ═══ 6 · the master index keeps `pair` ═══
    await t('6 masterPutIndex / masterGet / masterGetMany / masterList carry pair; silence leaves it; null removes it; a person\'s decision stands; junk is ignored', async () => {
      const e = (o = {}) => Object.assign({ sku: 'MISMATCHED_PAIRTEST', masterHash: 'abcdef12', charmHash: 'ch1', widthPt: 90, heightPt: 40, areaPt2: 2000, members: 9, holes: 0 }, o);
      const put = async entries => (await call({ op: 'masterPutIndex', entries, masterHash: 'abcdef12', masterName: 'test.ai' })).body;
      assert.equal((await put([e({ pair: { v: 1, bodies: 2, mismatched: true } })])).written, 1);
      const get = async () => (await call({ op: 'masterGet', sku: 'MISMATCHED_PAIRTEST' })).body.entry;
      assert.deepEqual((await get()).pair, { v: 1, bodies: 2, mismatched: true });
      assert.deepEqual((await call({ op: 'masterGetMany', skus: ['MISMATCHED_PAIRTEST'] })).body.entries.MISMATCHED_PAIRTEST.pair, { v: 1, bodies: 2, mismatched: true });
      assert.deepEqual((await call({ op: 'masterList', q: 'MISMATCHED_PAIRTEST' })).body.entries[0].pair, { v: 1, bodies: 2, mismatched: true });
      await put([e()]); assert.deepEqual((await get()).pair, { v: 1, bodies: 2, mismatched: true }, 'a re-index that says nothing of pair leaves it');
      await put([e({ pair: { bodies: 1, mismatched: true } })]); assert.deepEqual((await get()).pair, { v: 1, bodies: 2, mismatched: true }, 'a shape that is not a pair is ignored');
      await put([e({ pair: null })]); assert.equal('pair' in (await get()), false, 'null: it is one charm');
      await put([e({ pair: { v: 1, bodies: 3, mismatched: false } })]); assert.deepEqual((await get()).pair, { v: 1, bodies: 3, mismatched: false });
      assert.equal((await call({ op: 'masterPatch', sku: 'MISMATCHED_PAIRTEST', patch: { pair: { bodies: 2, mismatched: true } } })).status, 200);
      await put([e({ pair: { v: 1, bodies: 2, mismatched: false } })]); assert.deepEqual((await get()).pair, { v: 1, bodies: 2, mismatched: true }, 'a person\'s decision stands over a re-index');
      assert.equal((await call({ op: 'masterPatch', sku: 'MISMATCHED_PAIRTEST', patch: { pair: null } })).status, 200); assert.equal('pair' in (await get()), false);
      // a design that is not a pair is returned exactly as before: no pair key at all
      await put([e({ sku: 'PLAIN_PAIRTEST' })]); const plain = (await call({ op: 'masterGet', sku: 'PLAIN_PAIRTEST' })).body.entry; assert.equal('pair' in plain, false);
      assert.equal(Master.slimEntry({ sku: 'X1', pair: { bodies: 9 } }).pair, undefined);
    });

    // ═══ 7 · what the shared rules say of a mismatched pair (the pieces differ by design, the rules look at the pool id) ═══
    await t('7a Readiness: a sheet holding the left ear waits while the right ear is on a sheet that is not ready, is on none, or is held; a whole pair on one sheet waits for nothing', () => {
      const L = MIS.L, R = MIS.R;
      const lines = [{ key: MIS.key, orderId: MIS.rid, poolIds: [L, R], quantity: 1, pieceCount: 2, state: 'written' }];
      const sh = (id, ids, cut) => ({ id, setId: null, poolIds: ids, orders: [MIS.rid], laserDoneAt: cut ? 1 : 0, metal: 'gold' });
      const view = (sheets, asking, ls) => Readiness.forSheet(Readiness.orderReports(ls || lines, sheets)[MIS.rid], asking, null);
      assert.equal(view([sh('a', [L, R], false)], 'a').ready, true, 'both ears on one sheet: nothing else to wait for');
      assert.equal(view([sh('a', [L], false), sh('b', [R], true)], 'a').ready, true, 'the other ear is on a sheet that was cut');
      const waits = view([sh('a', [L], true), sh('b', [R], false)], 'a'); assert.equal(waits.ready, false); assert.equal(waits.blocks.length, 1); assert.equal(waits.blocks[0].poolId, R); assert.equal(waits.blocks[0].key, 'otherSheetNotReady'); assert.equal(waits.blocks[0].sheetId, 'b');
      const off = view([sh('a', [L], true)], 'a'); assert.equal(off.ready, false); assert.equal(off.blocks[0].key, 'pooled'); assert.equal(off.blocks[0].poolId, R, 'the sheet that holds the left ear is held back by the right ear, which is on no sheet');
      const held = view([sh('a', [L, R], true)], 'a', [Object.assign({}, lines[0], { hold: 'on hold' })]); assert.equal(held.ready, false); assert.equal(held.blocks[0].key, 'held');
      // (the two ears are different designs: nothing above looks at what a piece shows)
      assert.notEqual(row(MIS.L).sku, row(MIS.R).sku);
    });
    await t('7b Readiness: a line that lost its pool ids is still read as TWO pieces when the intake says so (pieceCount), and as one when it says nothing (as before)', () => {
      const twoPieces = Readiness.pieces([{ key: MIS.key, orderId: MIS.rid, quantity: 1, pieceCount: 2, state: 'pulled' }], [])[MIS.rid];
      assert.deepEqual(twoPieces.map(p => p.poolId), [MIS.L, MIS.R]);
      const one = Readiness.pieces([{ key: MIS.key, orderId: MIS.rid, quantity: 1, state: 'pulled' }], [])[MIS.rid];
      assert.deepEqual(one.map(p => p.poolId), [MIS.L]);
    });
    await t('7c SetEdit.setAfter: the order line keeps one entry per piece (by pool id) and the ear of each, whatever each piece\'s design', () => {
      const out = SetEdit.setAfter({ sheetIds: [], orders: {} }, { join: [{ id: 'a', poolIds: [MIS.L, MIS.R], fileBase: 'GF_Sheet-1' }], skus: { [MIS.L]: 'MISMATCHED_7134', [MIS.R]: 'MISMATCHED_7134' }, sides: { [MIS.L]: 'L', [MIS.R]: 'R' }, members: [] });
      const copies = out.orders[MIS.rid].lines[0].copies; assert.deepEqual(copies.map(c => [c.poolId, c.side]), [[MIS.L, 'L'], [MIS.R, 'R']]);
      const plain = SetEdit.setAfter({ sheetIds: [], orders: {} }, { join: [{ id: 'a', poolIds: [SOLO.L], fileBase: 'x' }], members: [] }).orders[SOLO.rid].lines[0].copies[0];
      assert.equal('side' in plain, false);
    });

    // ═══ 8 · the timeline ═══
    await t('8a whereOf: pieces on several sheets, some cut and some not, say how many are cut (stage and step are the furthest piece\'s, as before); nothing added otherwise', () => {
      const ev = [{ type: 'placed', at: 1000, sheetId: 'a', sheet: 'GF Sheet 1' }, { type: 'placed', at: 1100, sheetId: 'b', sheet: 'GF Sheet 2' }, { type: 'laserDone', at: 2000, sheetId: 'a', sheet: 'GF Sheet 1' }];
      const w = Timeline.whereOf(ev, null, { sheets: [{ sheetId: 'a', sheet: 'GF Sheet 1', cut: true }, { sheetId: 'b', sheet: 'GF Sheet 2', cut: false }] });
      assert.deepEqual(w.partial, { sheets: 2, cut: 1 }); assert.equal(w.stage, 'cut');
      const whole = Timeline.whereOf(ev, null, { sheets: [{ sheetId: 'a', sheet: 'GF Sheet 1', cut: true }, { sheetId: 'b', sheet: 'GF Sheet 2', cut: true }] }); assert.equal('partial' in whole, false);
      const one = Timeline.whereOf(ev.slice(0, 1), null, { sheets: [{ sheetId: 'a', sheet: 'GF Sheet 1', cut: false }] }); assert.equal('partial' in one, false);
      assert.equal('partial' in Timeline.whereOf(ev, null, {}), false);
    });
    await t('8b the derived timeline of a mismatched line says "left and right" with the sides; a line of single charms reads as it did', async () => {
      st.deriveTimeline = true;
      const m = (await call({ op: 'timelineGet', orderId: MIS.rid })).body;
      const pooled = (m.events || []).find(e => e.type === 'pooled'); assert(pooled, JSON.stringify((m.events || []).map(e => e.type)));
      assert(/2 pieces \(left and right\)/.test(pooled.text), pooled.text); assert.deepEqual(pooled.data.sides, ['L', 'R']);
      const s = (await call({ op: 'timelineGet', orderId: SOLO.rid })).body, one = (s.events || []).find(e => e.type === 'pooled');
      assert(one && !/left and right/.test(one.text) && !('sides' in one.data), JSON.stringify(one));
      st.deriveTimeline = false;
    });

    // ═══ 9 · nothing the fake stored has an array inside an array ═══
    await t('9 every document stored by every case above passes the Firestore nested-array check', () => {
      let n = 0; for (const [k, v] of st.docs) { refuseNestedArrays(v, k); n++; } assert(n > 20, 'documents checked: ' + n);
    });
  } finally { srv.close(); }
  const failed = results.filter(([, e]) => e);
  console.log(`\n${results.length - failed.length}/${results.length} passed`);
  if (failed.length) process.exit(1);
})().catch(e => { console.error(e); process.exit(1); });
