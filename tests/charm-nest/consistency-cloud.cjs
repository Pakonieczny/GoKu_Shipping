// One cloud truth for "is this piece on a sheet, which sheet, on hold, waiting, taken off" (Paul, 5 Oct 2026: "no mismatch in
// orders being on sheets, not being on sheets ... as information is added, changed, removed, altered ... seamlessly updated in the
// cloud"). The answer lives in four documents (the piece's pool row, the sheet's record, the set's record, the order's timeline),
// and the ops that change it wrote them in separate requests, so a reader in between saw a piece taken off its sheet by the pool
// row and still on it by the sheet's record (order 4174601819: the hold card "Not on a sheet yet", the piece row "Waiting · next:
// Laser cut"). Over the real charmNestLibrary handler and the real order timeline, on an in-memory Firestore that commits a batch
// or a transaction ALL AT ONCE and can look in at every read and every commit (bridge-server.cjs: st.atomic, st.tick):
//   A  the placement rules (netlify/functions/_charmNestPlacement.js) on their own
//   B  take-off (hold, cancel) is one commit across the pieces' rows and the sheet records; a reader looking in at EVERY step of it
//      never sees a contradictory pair, in the raw documents or in what getOrderPieces, poolList and the timeline answer
//   C  a stale page cannot put a held piece back on a sheet (putSheet), and Release makes a row live again
//   D  deleting a sheet clears the pointers to it (pool rows, open sets) in the same commit; a cut or committed record is never edited
//   E  repair on read: drift an older write left behind is answered as it is true, nothing is written back
//   F  placementRev / ifRev: a revision that moves with any write to the documents an answer is made from, and only then
//   G  two takes-off at once on one sheet both land (optimistic retry); the sandbox's copies are separate
//   node tests/charm-nest/consistency-cloud.cjs
//   CC_BASELINE=1 prints the failures instead of exiting 1 (run against a checkout without the fix, to see what drifts there)
const path = require('path'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');
const { start } = require('./bridge-server.cjs');
const P = require(path.join(root, 'netlify/functions/_charmNestPlacement.js'));
const Timeline = require(path.join(root, 'netlify/functions/_orderTimeline.js'));

const results = [];
const t = async (name, fn) => { try { await fn(); results.push([name, null]); console.log('  ✓ ' + name); } catch (e) { results.push([name, e]); console.log('  ✗ ' + name + '\n      ' + String(e && e.message || e).split('\n').slice(0, 6).join('\n      ')); } };

const NOW = 1790000000000, RID = { A: '4180000001', B: '4180000002', C: '4180000003', D: '4180000004' };
const pid = (rid, tx, copy = 1) => `${rid}_${tx}_${copy}`;
const PA1 = pid(RID.A, '41800000011'), PA2 = pid(RID.A, '41800000012'), PB1 = pid(RID.B, '41800000021'), PC1 = pid(RID.C, '41800000031');
const S1 = 'sheet-cc-gf1', S2 = 'sheet-cc-ss1', S3 = 'sheet-cc-cut', SET1 = 'set-cc-1', SET2 = 'set-cc-2';
const TAKE = new Set(['abandoned', 'superseded']);

function seed(st, pre = '') {
  const put = (c, id, d) => st.put(pre + c, id, d);
  const row = (poolId, material, sheetId, setId, sheetName) => ({ poolId, orderId: poolId.split('_')[0], transactionId: poolId.split('_')[1], lineKey: poolId.split('_').slice(0, 2).join('_'), sku: 'DUCK', material, copy: 1, quantity: 1, state: 'written', sheetId, sheetName, setId, runId: 'run-cc' });
  put('Charm_Nest_Sheets', S1, { id: S1, metal: 'gold', sheetIndex: 1, setId: SET1, setSeq: 1, folder: '2026-10-05_GF_Set-1_Sheet-1', fileBase: 'GF_Oct.5.26_Set-1_Sheet-1', day: '2026-10-05', status: 'written', orders: [RID.A, RID.B], poolIds: [PA1, PB1], backPool: [{ poolId: PA1, approvedAt: 1 }, { poolId: PB1, approvedAt: 1 }], placedCount: 2, charmCount: 2, dirty: false, runId: 'run-cc' });
  put('Charm_Nest_Sheets', S2, { id: S2, metal: 'silver', sheetIndex: 1, setId: SET1, setSeq: 1, folder: '2026-10-05_SS_Set-1_Sheet-1', fileBase: 'SS_Oct.5.26_Set-1_Sheet-1', day: '2026-10-05', status: 'written', orders: [RID.A], poolIds: [PA2], backPool: [], placedCount: 1, charmCount: 1, dirty: false, runId: 'run-cc' });
  put('Charm_Nest_Sheets', S3, { id: S3, metal: 'gold', sheetIndex: 2, setId: SET2, setSeq: 2, folder: '2026-10-05_GF_Set-2_Sheet-2', fileBase: 'GF_Oct.5.26_Set-2_Sheet-2', day: '2026-10-05', status: 'written', orders: [RID.C], poolIds: [PC1], placedCount: 1, charmCount: 1, laserDoneAt: NOW - 1000, runId: 'run-cc' });
  put('Charm_Nest_Sets', SET1, { setId: SET1, seq: 1, status: 'open', sheetIds: [S1, S2], orders: {} });
  put('Charm_Nest_Sets', SET2, { setId: SET2, seq: 2, status: 'complete', committedAt: NOW - 2000, laserDoneAt: NOW - 1000, sheetIds: [S3], orders: {} });
  put('Charm_Pool', PA1, row(PA1, 'gold', S1, SET1, 'GF_Oct.5.26_Set-1_Sheet-1'));
  put('Charm_Pool', PA2, row(PA2, 'silver', S2, SET1, 'SS_Oct.5.26_Set-1_Sheet-1'));
  put('Charm_Pool', PB1, row(PB1, 'gold', S1, SET1, 'GF_Oct.5.26_Set-1_Sheet-1'));
  put('Charm_Pool', PC1, row(PC1, 'gold', S3, SET2, 'GF_Oct.5.26_Set-2_Sheet-2'));
}

const hold = who => ({ state: 'abandoned', sheetId: null, setId: null, heldBy: who || 'Paul', heldReason: 'on hold', heldAt: NOW + 10 });
const cancel = who => ({ state: 'abandoned', sheetId: null, setId: null, removedBy: who || 'Paul', removedReason: 'cancelled: test', removedAt: NOW + 20 });

/* ── what a reader looks at, and what must never be true of it ─────────────────────────────────────────────────────────── */
function world(srv, sandbox) {
  const pre = sandbox ? 'Sandbox_' : '', st = srv.st;
  const call = async body => { const r = await fetch(`${srv.sorterOrigin}/.netlify/functions/charmNestLibrary`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(sandbox ? Object.assign({ sandbox: true }, body) : body) }); return { status: r.status, body: await r.json() }; };
  const sheetOk = id => { const d = st.docs.get(pre + 'Charm_Nest_Sheets/' + id); return !!d && !d.archived; };
  /** What the documents themselves must never say together (a commit is atomic, so no instant between two requests may show it). */
  const raw = () => {
    const bad = [], sheets = st.list(pre + 'Charm_Nest_Sheets').filter(s => !s.archived);
    for (const p of st.list(pre + 'Charm_Pool')) {
      if (P.takenOff(p)) for (const s of sheets) if (P.editable(s) && (s.poolIds || []).includes(p._id)) bad.push(`raw: ${p._id} is taken off (${p.state}) and sheet ${s._id} still lists it`);
      if (p.sheetId && !TAKE.has(p.state) && !sheetOk(p.sheetId)) bad.push(`raw: ${p._id} names sheet ${p.sheetId}, which is gone`);
    }
    for (const s of st.list(pre + 'Charm_Nest_Sets')) if (!s.committedAt) for (const id of s.sheetIds || []) if (!sheetOk(id)) bad.push(`raw: set ${s._id} lists sheet ${id}, which is gone`);
    return bad;
  };
  /** One reader's look at the order: getOrderPieces, poolList and the order timeline (derived), and the contradictions in them. */
  const look = async rid => {
    const [pieces, list] = await Promise.all([call({ op: 'getOrderPieces', orderId: rid }), call({ op: 'poolList', orderId: rid })]);
    const tl = await Timeline.get(st.db, rid, { prefix: pre });
    const o = pieces.body.orders[rid], bad = [], listed = new Map();
    for (const s of o.sheets) for (const id of s.poolIds) listed.set(id, s.id);
    for (const p of o.pools) {
      if (P.takenOff(p) && listed.has(p.poolId)) bad.push(`pieces: ${p.poolId} is taken off by its row and listed by ${listed.get(p.poolId)}`);
      if (p.sheetId && !TAKE.has(p.state) && !sheetOk(p.sheetId)) bad.push(`pieces: ${p.poolId} row names ${p.sheetId}, which is gone`);
    }
    for (const p of list.body.pools) if (p.sheetId && !TAKE.has(p.state) && !sheetOk(p.sheetId)) bad.push(`poolList: ${p.poolId} row names ${p.sheetId}, which is gone`);
    const w = tl.where;
    if (w && ['sheet', 'cut'].includes(w.stage)) {
      const rec = st.docs.get(pre + 'Charm_Nest_Sheets/' + w.sheetId), live = rec && (rec.poolIds || []).filter(x => x.startsWith(rid + '_')).some(x => !P.takenOff(st.docs.get(pre + 'Charm_Pool/' + x)));
      if (!live) bad.push(`timeline: where says "${w.label}" (${w.sheetId}) and no piece of the order is on it that its row says is on a sheet`);
    }
    return { pieces: pieces.body, list: list.body, tl, bad, o };
  };
  /** Looks in at every read and every commit of whatever runs next; `seen` holds what was found, `ticks` how often it looked. */
  const watching = async (rids, fn) => {
    const found = [], at = { ticks: 0 };
    st.tick = async what => { st.quiet = true; try { at.ticks++; for (const b of raw()) found.push(`[${what}] ${b}`); for (const rid of rids) for (const b of (await look(rid)).bad) found.push(`[${what}] ${b}`); } finally { st.quiet = false; } };
    try { await fn(); } finally { st.tick = null; }
    for (const b of raw()) found.push(`[after] ${b}`);
    for (const rid of rids) for (const b of (await look(rid)).bad) found.push(`[after] ${b}`);
    return { found: [...new Set(found)], ticks: at.ticks };
  };
  return { st, pre, call, raw, look, watching, sheetOk, doc: (c, id) => st.docs.get(pre + c + '/' + id), docs: c => st.list(pre + c) };
}
const fresh = async (sandbox, seeded = true) => { const srv = await start(); srv.st.atomic = true; const w = world(srv, sandbox); if (seeded) seed(srv.st, sandbox ? 'Sandbox_' : ''); w.srv = srv; return w; };
const noWrites = async (w, fn) => { const before = JSON.stringify([...w.st.docs.entries()]); const out = await fn(); assert.equal(JSON.stringify([...w.st.docs.entries()]), before, 'a read: no document changed'); return out; };

(async () => {
  /* ── A · the rules on their own ── */
  await t('A1 isTakeOff: a hold, a cancel and a superseded row are take-offs; a run given up ({state:"abandoned"} alone) and a placement are not', () => {
    assert(P.isTakeOff(hold())); assert(P.isTakeOff(cancel())); assert(P.isTakeOff({ state: 'superseded', sheetId: null }));
    assert(!P.isTakeOff({ state: 'abandoned' }), 'releaseRun: no sheetId in the patch, no mark');
    assert(!P.isTakeOff({ state: 'abandoned', sheetId: null }), 'no take-off mark');
    assert(!P.isTakeOff({ state: 'written', sheetId: 'x', heldAt: 1 })); assert(!P.isTakeOff(null));
  });
  await t('A2 takenOff reads the row as it stands; a re-pooled row (repooledAt) is live again though its marks stay for history', () => {
    assert(P.takenOff({ state: 'abandoned', sheetId: null, heldAt: 5 })); assert(P.takenOff({ state: 'abandoned', removedBy: 'x' })); assert(P.takenOff({ state: 'superseded' }));
    assert(!P.takenOff({ state: 'abandoned', sheetId: null })); assert(!P.takenOff({ state: 'abandoned', sheetId: 'S', heldAt: 5 })); assert(!P.takenOff({ state: 'ready', heldAt: 5 }));
    assert(!P.takenOff({ state: 'abandoned', sheetId: null, heldAt: 5, repooledAt: 9 }), 'released and made up again, then its run was given up: waiting, not held');
  });
  await t('A3 takeOffUpdate edits only what lists the pieces; a cut or archived record is not edited; counts and orders follow', () => {
    const s = { id: 's', poolIds: ['1_a_1', '1_b_1', '2_c_1'], orders: ['1', '2'], backPool: [{ poolId: '1_a_1' }, { poolId: '2_c_1' }], placedCount: 3, charmCount: 3 };
    const u = P.takeOffUpdate(s, new Set(['1_a_1', '1_b_1']), 'TS');
    assert.deepEqual(u.update.poolIds, ['2_c_1']); assert.deepEqual(u.update.orders, ['2']); assert.deepEqual(u.update.backPool, [{ poolId: '2_c_1' }]);
    assert.equal(u.update.placedCount, 1); assert.equal(u.update.dirty, true); assert.equal(u.update.updatedAt, 'TS');
    assert.deepEqual(P.takeOffUpdate(s, new Set(['1_a_1']), 'TS').update.orders, ['1', '2'], 'the order stays while one of its pieces does');
    assert.equal(P.takeOffUpdate(Object.assign({}, s, { laserDoneAt: 5 }), new Set(['1_a_1']), 'TS'), null); assert.equal(P.takeOffUpdate(Object.assign({}, s, { archived: true }), new Set(['1_a_1']), 'TS'), null);
    assert.equal(P.takeOffUpdate(s, new Set(['9_z_1']), 'TS'), null);
  });
  await t('A4 reconcile: the record\'s listing is the truth; a take-off beats a stale listing; a gone or non-listing sheet makes the row\'s hint stale; a cut record is physical', () => {
    const pool = (id, o) => Object.assign({ poolId: id, orderId: '1', state: 'written' }, o), sh = (id, ids, o) => Object.assign({ id, poolIds: ids, orders: ['1'], metal: 'gold', sheetIndex: 1 }, o);
    let r = P.reconcile({ orderId: '1', pools: [pool('1_a_1', { sheetId: 's1' }), pool('1_b_1', { state: 'abandoned', sheetId: null, heldAt: 5, heldBy: 'Paul' })], sheets: [sh('s1', ['1_a_1', '1_b_1'])] });
    assert.equal(r.placement['1_a_1'].state, 'sheet'); assert.equal(r.placement['1_b_1'].state, 'held'); assert.equal(r.placement['1_b_1'].by, 'Paul');
    assert.deepEqual(r.repaired.map(x => x.kind), ['staleListing']); assert.deepEqual(r.sheets[0].poolIds, ['1_a_1']);
    r = P.reconcile({ orderId: '1', pools: [pool('1_a_1', { sheetId: 'gone' })], sheets: [], hinted: new Map([['gone', { exists: false }]]) });
    assert.equal(r.placement['1_a_1'].state, 'waiting'); assert.equal(r.pools[0].sheetId, null); assert.equal(r.pools[0].sheetIdWas, 'gone'); assert.equal(r.repaired[0].kind, 'sheetGone');
    r = P.reconcile({ orderId: '1', pools: [pool('1_a_1', { sheetId: 's9' })], sheets: [sh('s9', ['1_x_1']), sh('s2', ['1_a_1'], { sheetIndex: 2 })] });
    assert.equal(r.placement['1_a_1'].sheetId, 's2', 'moved: the record that lists it says where'); assert.equal(r.pools[0].sheetId, 's2'); assert.equal(r.repaired[0].kind, 'hintStale');
    r = P.reconcile({ orderId: '1', pools: [pool('1_a_1', { state: 'abandoned', sheetId: null, heldAt: 5 })], sheets: [sh('s1', ['1_a_1'], { laserDoneAt: 7 })] });
    assert.equal(r.placement['1_a_1'].state, 'sheet'); assert.equal(r.placement['1_a_1'].cut, true); assert.equal(r.repaired.length, 0);
    r = P.reconcile({ orderId: '1', pools: [pool('1_a_1', { state: 'abandoned', sheetId: null, heldAt: 5, repooledAt: 9 })], sheets: [] });
    assert.equal(r.placement['1_a_1'].state, 'abandoned');
    r = P.reconcile({ orderId: '1', pools: [pool('1_a_1', { sheetId: null, state: 'ready' })], sheets: [sh('d1', ['1_a_1'], { draft: true })] });
    assert.equal(r.placement['1_a_1'].state, 'sheet'); assert.equal(r.placement['1_a_1'].draft, true); assert.equal(r.placement['1_a_1'].setId, null);
  });

  /* ── B · take-off is one commit ── */
  await t('B1 hold: the rows and every sheet record that listed the pieces change in ONE commit; a reader at every step sees no contradiction', async () => {
    const w = await fresh(false); try {
      const { found, ticks } = await w.watching([RID.A, RID.B], async () => { const r = await w.call({ op: 'poolUpdate', poolIds: [PA1, PA2], patch: hold(), by: 'Paul' }); assert.equal(r.status, 200, JSON.stringify(r.body)); });
      assert.deepEqual(found, [], found.join('\n'));
      assert(ticks >= 2, 'the reader looked in at the steps of the op (' + ticks + ')');
      const s1 = w.doc('Charm_Nest_Sheets', S1), s2 = w.doc('Charm_Nest_Sheets', S2);
      assert.deepEqual(s1.poolIds, [PB1], 'the other order\'s piece stays'); assert.deepEqual(s1.orders, [RID.B]); assert.deepEqual(s1.backPool.map(b => b.poolId), [PB1]); assert.equal(s1.placedCount, 1); assert.equal(s1.dirty, true);
      assert.deepEqual(s2.poolIds, []); assert.deepEqual(s2.orders, []); assert.equal(s2.dirty, true);
      assert.equal(w.doc('Charm_Pool', PA1).state, 'abandoned'); assert.equal(w.doc('Charm_Pool', PA1).heldBy, 'Paul'); assert.equal(w.doc('Charm_Pool', PB1).state, 'written', 'another order\'s row untouched');
      assert.deepEqual(w.doc('Charm_Nest_Sets', SET1).sheetIds, [S1, S2], 'the set record (what was assembled) is not edited by a hold');
      const look = await w.look(RID.A);
      assert.equal(look.o.placement[PA1].state, 'held'); assert.equal(look.o.placement[PA2].state, 'held'); assert.equal(look.o.summary.held, 2); assert.equal(look.o.summary.onSheet, 0);
      assert.equal(look.tl.where.stage, 'waiting', 'the timeline\'s where: on no sheet (the held event, stamped by the page next, makes it "held")');
      assert(look.o.sheets.every(s => !s.poolIds.includes(PA1) && !s.poolIds.includes(PA2)), 'no sheet lists them');
    } finally { w.srv.close(); }
  });
  await t('B2 the held event the page stamps next reads as held, and a piece taken off ONE sheet leaves the other sheets alone', async () => {
    const w = await fresh(false); try {
      assert.equal((await w.call({ op: 'poolUpdate', poolIds: [PA2], patch: hold(), by: 'Paul' })).status, 200);
      assert.deepEqual(w.doc('Charm_Nest_Sheets', S1).poolIds, [PA1, PB1], 'S1 untouched'); assert.deepEqual(w.doc('Charm_Nest_Sheets', S2).poolIds, []);
      let look = await w.look(RID.A);
      assert.equal(look.o.placement[PA1].state, 'sheet'); assert.equal(look.o.placement[PA2].state, 'held'); assert.equal(look.tl.where.stage, 'sheet');
      assert.equal((await w.call({ op: 'poolUpdate', poolIds: [PA1], patch: hold(), by: 'Paul' })).status, 200);
      const ev = await w.call({ op: 'timelineAdd', events: [{ orderId: RID.A, type: 'held', at: NOW + 11, by: 'Paul', id: 'sw-held-1', text: 'Taken off GF Sheet 1, SS Sheet 1 by Paul: on hold' }] }); assert.equal(ev.status, 200, JSON.stringify(ev.body));
      look = await w.look(RID.A); assert.equal(look.tl.where.stage, 'held'); assert.equal(look.o.summary.held, 2); assert.deepEqual(look.bad, []);
    } finally { w.srv.close(); }
  });
  await t('B3 cancel: pieces taken off with the cancel mark; the sheet records stop listing them in the same commit; "Removed from GF Sheet 1" names the sheet even when the row never did', async () => {
    const w = await fresh(false); try {
      // (a piece on a sheet still filling: its row has no sheetId, the record lists it)
      w.st.put('Charm_Pool', PB1, { sheetId: null, sheetName: null, setId: null, state: 'ready' });
      const { found } = await w.watching([RID.B], async () => { assert.equal((await w.call({ op: 'poolUpdate', poolIds: [PB1], patch: cancel(), by: 'Paul' })).status, 200); });
      assert.deepEqual(found, [], found.join('\n'));
      assert.deepEqual(w.doc('Charm_Nest_Sheets', S1).poolIds, [PA1]); assert.deepEqual(w.doc('Charm_Nest_Sheets', S1).orders, [RID.A]);
      const ev = w.docs('Order_Timeline').filter(e => e.orderId === RID.B && e.type === 'removed');
      assert.equal(ev.length, 1, 'one removed event'); assert.equal(ev[0].sheet, 'GF Sheet 1', 'it names the sheet the record listed the piece on'); assert.equal(ev[0].sheetId, S1);
      const look = await w.look(RID.B); assert.equal(look.o.placement[PB1].state, 'removed'); assert.equal(look.o.placement[PB1].cancel, true);
    } finally { w.srv.close(); }
  });
  await t('B4 the same take-off sent again (a retry) changes nothing more: the sheets as they are, one event', async () => {
    const w = await fresh(false); try {
      const p = cancel(); assert.equal((await w.call({ op: 'poolUpdate', poolIds: [PB1], patch: p, by: 'Paul' })).status, 200);
      const sheet = JSON.stringify(w.doc('Charm_Nest_Sheets', S1)), events = w.docs('Order_Timeline').length;
      assert.equal((await w.call({ op: 'poolUpdate', poolIds: [PB1], patch: p, by: 'Paul' })).status, 200);
      assert.equal(JSON.stringify(w.doc('Charm_Nest_Sheets', S1)), sheet, 'the sheet record not written again'); assert.equal(w.docs('Order_Timeline').length, events, 'no second event');
    } finally { w.srv.close(); }
  });
  await t('B5 a sheet that was cut is what was made: a take-off does not edit its record and the answer says it is on it (physical truth)', async () => {
    const w = await fresh(false); try {
      const before = JSON.stringify(w.doc('Charm_Nest_Sheets', S3));
      assert.equal((await w.call({ op: 'poolUpdate', poolIds: [PC1], patch: hold(), by: 'Paul' })).status, 200);
      assert.equal(JSON.stringify(w.doc('Charm_Nest_Sheets', S3)), before);
      const look = await w.look(RID.C); assert.equal(look.o.placement[PC1].state, 'sheet'); assert.equal(look.o.placement[PC1].cut, true); assert.equal(look.o.repaired.length, 0);
    } finally { w.srv.close(); }
  });
  await t('B6 a run given up ({state:"abandoned"} alone, releaseRun) is not a take-off: the pieces stay on their saved sheets', async () => {
    const w = await fresh(false); try {
      assert.equal((await w.call({ op: 'poolUpdate', poolIds: [PA1], patch: { state: 'abandoned' } })).status, 200);
      assert.deepEqual(w.doc('Charm_Nest_Sheets', S1).poolIds, [PA1, PB1]);
      const look = await w.look(RID.A); assert.equal(look.o.placement[PA1].state, 'sheet'); assert.deepEqual(look.bad, []);
    } finally { w.srv.close(); }
  });

  /* ── C · a held piece does not go back by a stale page; Release makes it live ── */
  await t('C1 putSheet refuses to put a held piece back on a sheet (409, the record as it was); saving the sheet as it stands is accepted', async () => {
    const w = await fresh(false); try {
      assert.equal((await w.call({ op: 'poolUpdate', poolIds: [PA1], patch: hold(), by: 'Paul' })).status, 200);
      const rec = w.doc('Charm_Nest_Sheets', S1), snapshot = JSON.stringify(rec.poolIds);
      const stale = await w.call({ op: 'putSheet', sheet: { id: S1, poolIds: [PA1, PB1], orders: [RID.A, RID.B] } });
      assert.equal(stale.status, 409, JSON.stringify(stale.body)); assert(/taken off on purpose/.test(stale.body.error), stale.body.error);
      assert.equal(JSON.stringify(w.doc('Charm_Nest_Sheets', S1).poolIds), snapshot, 'the record was not written');
      const same = await w.call({ op: 'putSheet', sheet: { id: S1, poolIds: [PB1], orders: [RID.B], dirty: false } }); assert.equal(same.status, 200, JSON.stringify(same.body));
      assert.equal(w.doc('Charm_Nest_Sheets', S1).dirty, false, 'the page\'s own save replaces the flag a take-off set');
      const other = await w.call({ op: 'putSheet', sheet: { id: S1, poolIds: [PB1, PC1], orders: [RID.B, RID.C] } }); assert.equal(other.status, 200, 'a piece that was not taken off may be added');
    } finally { w.srv.close(); }
  });
  await t('C2 Release: re-pooling a held row makes it live (repooledAt), the piece can be placed again, and a later "run given up" does not read it as held', async () => {
    const w = await fresh(false); try {
      assert.equal((await w.call({ op: 'poolUpdate', poolIds: [PA1], patch: hold(), by: 'Paul' })).status, 200);
      const put = await w.call({ op: 'poolPut', pools: [{ poolId: PA1, orderId: RID.A, transactionId: '41800000011', lineKey: pid(RID.A, '41800000011').replace(/_1$/, ''), sku: 'DUCK', material: 'gold', copy: 1, quantity: 1, state: 'ready', sheetId: null, runId: 'run-cc' }] });
      assert.equal(put.status, 200, JSON.stringify(put.body)); assert(w.doc('Charm_Pool', PA1).repooledAt, 'stamped');
      assert.equal(w.doc('Charm_Pool', PA1).heldAt, NOW + 10, 'the hold\'s own marks stay (history)');
      let look = await w.look(RID.A); assert.equal(look.o.placement[PA1].state, 'waiting');
      const back = await w.call({ op: 'putSheet', sheet: { id: S1, poolIds: [PA1, PB1], orders: [RID.A, RID.B] } }); assert.equal(back.status, 200, JSON.stringify(back.body));
      assert.equal((await w.call({ op: 'poolUpdate', poolIds: [PA1], patch: { state: 'written', sheetId: S1, setId: SET1, sheetName: 'x' } })).status, 200);
      look = await w.look(RID.A); assert.equal(look.o.placement[PA1].state, 'sheet'); assert.deepEqual(look.bad, []);
      // a second hold clears the stamp, so the new hold is a hold
      assert.equal((await w.call({ op: 'poolUpdate', poolIds: [PA1], patch: Object.assign(hold('Sam'), { heldAt: NOW + 99 }), by: 'Sam' })).status, 200);
      assert.equal(w.doc('Charm_Pool', PA1).repooledAt, null); look = await w.look(RID.A); assert.equal(look.o.placement[PA1].state, 'held'); assert.equal(look.o.placement[PA1].by, 'Sam');
      // the run is given up while it is held again, and while released
      assert.equal((await w.call({ op: 'poolPut', pools: [{ poolId: PA1, orderId: RID.A, transactionId: '41800000011', sku: 'DUCK', material: 'gold', copy: 1, quantity: 1, state: 'ready', sheetId: null, runId: 'run-cc' }] })).status, 200);
      assert.equal((await w.call({ op: 'poolUpdate', poolIds: [PA1], patch: { state: 'abandoned' } })).status, 200);
      look = await w.look(RID.A); assert.equal(look.o.placement[PA1].state, 'abandoned', 'released, then its run given up: not held');
    } finally { w.srv.close(); }
  });

  /* ── D · deleting a sheet ── */
  await t('D1 deleteSheet: the record, the pieces\' pointers and the open set\'s listing go in ONE commit; a committed set is not edited; a reader at every step sees no dangling pointer', async () => {
    const w = await fresh(false); try {
      const { found, ticks } = await w.watching([RID.A], async () => { const r = await w.call({ op: 'deleteSheet', id: S2, code: '975311' }); assert.equal(r.status, 200, JSON.stringify(r.body)); assert.equal(r.body.deleted, true); });
      assert.deepEqual(found, [], found.join('\n')); assert(ticks >= 3, 'ticks ' + ticks);
      assert.equal(w.doc('Charm_Nest_Sheets', S2), undefined);
      const row = w.doc('Charm_Pool', PA2); assert.equal(row.sheetId, null); assert.equal(row.sheetIdWas, S2); assert.equal(row.state, 'written', 'its state is the piece\'s own');
      assert.deepEqual(w.doc('Charm_Nest_Sets', SET1).sheetIds, [S1]);
      const look = await w.look(RID.A); assert.equal(look.o.placement[PA2].state, 'waiting'); assert.equal(look.o.placement[PA1].state, 'sheet');
      const cut = await w.call({ op: 'deleteSheet', id: S3, code: '975311' }); assert.equal(cut.status, 200, JSON.stringify(cut.body));
      assert.deepEqual(w.doc('Charm_Nest_Sets', SET2).sheetIds, [S3], 'the committed set keeps its record of what went to the laser');
      assert.equal((await w.call({ op: 'deleteSheet', id: S1, code: 'wrong' })).status, 403);
    } finally { w.srv.close(); }
  });

  /* ── E · repair on read ── */
  await t('E1 drift an older write left (a stale listing, a row naming a deleted sheet, one naming a sheet that does not list it, a listing on two sheets) is answered as it is true; nothing is written', async () => {
    const w = await fresh(false); try {
      const D = RID.D, pd = n => pid(D, '4180000004' + n), row = (id, o) => w.st.put('Charm_Pool', id, Object.assign({ poolId: id, orderId: D, transactionId: id.split('_')[1], lineKey: id.split('_').slice(0, 2).join('_'), sku: 'DUCK', material: 'gold', copy: 1, quantity: 1, state: 'written', sheetId: null, runId: 'run-cc' }, o));
      row(pd(1), { state: 'abandoned', heldBy: 'Paul', heldAt: NOW + 1 });            // taken off, a sheet record still lists it
      row(pd(2), { sheetId: 'sheet-gone-x' });                                           // names a sheet that no longer exists
      row(pd(3), { sheetId: S2 });                                                       // names a sheet that lists other pieces, not this one
      row(pd(4), { sheetId: S1 });                                                       // listed on two sheets
      row(pd(5), { state: 'abandoned', heldBy: 'Paul', heldAt: NOW + 1, repooledAt: NOW + 5 });   // held once, released, its run given up
      w.st.put('Charm_Nest_Sheets', S1, { orders: [RID.A, RID.B, D], poolIds: [PA1, PB1, pd(1), pd(4)] }); w.st.put('Charm_Nest_Sheets', S2, { orders: [RID.A, D], poolIds: [PA2, pd(4)], updatedAt: new Date(NOW - 5000) });
      const look = await noWrites(w, () => w.look(D)), kinds = look.o.repaired.map(x => `${x.poolId.slice(-3)}:${x.kind}`).sort();
      assert.equal(look.o.placement[pd(1)].state, 'held'); assert.equal(look.o.placement[pd(2)].state, 'waiting'); assert.equal(look.o.placement[pd(3)].state, 'waiting');
      assert(['sheet'].includes(look.o.placement[pd(4)].state)); assert.equal(look.o.placement[pd(5)].state, 'abandoned');
      assert(kinds.includes('1_1:staleListing') && kinds.includes('2_1:sheetGone') && kinds.includes('3_1:hintStale') && kinds.includes('4_1:duplicateListing'), kinds.join());
      assert.deepEqual(look.bad, [], look.bad.join('\n'));
      const poolRow = id => look.list.pools.find(p => p.poolId === id); assert.equal(poolRow(pd(2)).sheetId, null); assert.equal(poolRow(pd(2)).sheetIdWas, 'sheet-gone-x');
      assert.deepEqual(look.o.sheets.flatMap(s => s.poolIds).filter(id => id === pd(1)), [], 'the stale listing is not in the answer');
    } finally { w.srv.close(); }
  });
  await t('E2 the timeline\'s where follows the repaired answer: a record that still lists a held piece does not make the order "on a sheet"', async () => {
    const w = await fresh(false); try {
      w.st.put('Charm_Pool', PA2, { state: 'abandoned', sheetId: null, heldBy: 'Paul', heldAt: NOW + 1 }); w.st.put('Charm_Pool', PA1, { state: 'abandoned', sheetId: null, heldBy: 'Paul', heldAt: NOW + 1 });
      assert.equal((await w.call({ op: 'timelineAdd', events: [{ orderId: RID.A, type: 'held', at: NOW + 2, by: 'Paul', id: 'sw-held-2', text: 'on hold' }] })).status, 200);
      const look = await noWrites(w, () => w.look(RID.A)); assert.equal(look.tl.where.stage, 'held', JSON.stringify(look.tl.where)); assert.deepEqual(look.bad, []);
      assert.equal(look.tl.placement.summary.held, 2);
    } finally { w.srv.close(); }
  });

  /* ── F · placementRev ── */
  await t('F1 getOrderPieces rev: the same while nothing is written, a new one with any write to the order\'s rows or sheets, never for another order\'s; ifRev answers "unchanged" without the payload', async () => {
    const w = await fresh(false); try {
      const a1 = (await w.call({ op: 'getOrderPieces', orderId: RID.A })).body, a2 = (await w.call({ op: 'getOrderPieces', orderId: RID.A })).body, c1 = (await w.call({ op: 'getOrderPieces', orderId: RID.C })).body;
      assert(/^[0-9a-f]{12}$/.test(a1.rev), a1.rev); assert.equal(a1.rev, a2.rev); assert.equal(a1.orders[RID.A].rev, a2.orders[RID.A].rev);
      const same = (await w.call({ op: 'getOrderPieces', orderId: RID.A, ifRev: a1.rev })).body; assert.deepEqual(same, { ok: true, unchanged: true, rev: a1.rev, gen: a1.gen });   // (FC3: the answer also names the placement counter it was read at)
      w.st.put('Charm_Pool', PC1, { note: 'unrelated write to another order' });
      const a3 = (await w.call({ op: 'getOrderPieces', orderId: RID.A })).body, c2 = (await w.call({ op: 'getOrderPieces', orderId: RID.C })).body;
      assert.equal(a3.rev, a1.rev, 'another order\'s write is not this order\'s change'); assert.notEqual(c2.orders[RID.C].rev, c1.orders[RID.C].rev);
      assert.equal((await w.call({ op: 'poolUpdate', poolIds: [PA1], patch: hold(), by: 'Paul' })).status, 200);
      const a4 = (await w.call({ op: 'getOrderPieces', orderId: RID.A, ifRev: a1.rev })).body; assert.notEqual(a4.rev, a1.rev); assert(a4.orders, 'a moved rev answers in full');
      w.st.put('Charm_Nest_Sheets', S2, { label: 'edited by hand' });
      assert.notEqual((await w.call({ op: 'getOrderPieces', orderId: RID.A })).body.rev, a4.rev, 'a write to a sheet that lists its piece');
    } finally { w.srv.close(); }
  });
  await t('F2 the timeline answer carries placementRev (no extra read of its own) that moves with a take-off and not with another order\'s write', async () => {
    const w = await fresh(false); try {
      const r1 = (await Timeline.get(w.st.db, RID.A, { prefix: '' })), q1 = w.st.queries;
      assert(/^[0-9a-f]{12}$/.test(r1.placementRev), String(r1.placementRev));
      assert.equal((await Timeline.get(w.st.db, RID.A, { prefix: '' })).placementRev, r1.placementRev);
      w.st.put('Charm_Pool', PC1, { note: 'other order' }); assert.equal((await Timeline.get(w.st.db, RID.A, { prefix: '' })).placementRev, r1.placementRev);
      assert.equal((await w.call({ op: 'poolUpdate', poolIds: [PA1], patch: hold(), by: 'Paul' })).status, 200);
      const r2 = await Timeline.get(w.st.db, RID.A, { prefix: '' }); assert.notEqual(r2.placementRev, r1.placementRev);
      const used = w.st.queries; await Timeline.get(w.st.db, RID.A, { prefix: '' }); assert(w.st.queries - used <= q1 - 0 + 40, 'the derivation is the same reads as before: ' + (w.st.queries - used));
    } finally { w.srv.close(); }
  });
  await t('F3 a consistent order costs getOrderPieces its pool rows and its sheets and no more (two queries, no sheet read by id)', async () => {
    const w = await fresh(false); try {
      w.st.queries = w.st.queries || 0; w.st.reads = w.st.reads || 0; const q = w.st.queries, r = w.st.reads; const a = (await w.call({ op: 'getOrderPieces', orderId: RID.A })).body;
      assert.equal(w.st.queries - q, 2, 'queries'); assert.equal(w.st.reads - r, a.orders[RID.A].pools.length + a.orders[RID.A].sheets.length + 1, "documents read (and the one placement counter, FC3)");
    } finally { w.srv.close(); }
  });

  /* ── H · a writer lands in the middle of a read ── */
  await t('H1 a hold commits in the middle of getOrderPieces\' reads (after its pool rows, before its sheets): the answer is wholly before it or wholly after it, never a mix', async () => {
    const w = await fresh(false); try {
      let fired = false; const direct = body => w.st.handlers.charmNestLibrary.handler({ httpMethod: 'POST', headers: {}, body: JSON.stringify(body) });
      w.st.tick = async what => { if (what === 'query Charm_Nest_Sheets' && !fired) { fired = true; w.st.quiet = true; try { const r = await direct({ op: 'poolUpdate', poolIds: [PA1, PA2], patch: hold(), by: 'Paul' }); assert.equal(r.statusCode, 200, r.body); } finally { w.st.quiet = false; } } };
      let a; try { a = (await w.call({ op: 'getOrderPieces', orderId: RID.A })).body; } finally { w.st.tick = null; }
      assert(fired, 'the writer ran inside the read');
      const states = [PA1, PA2].map(id => a.orders[RID.A].placement[id].state);
      assert(states.every(s => s === 'sheet') || states.every(s => s === 'held'), 'before or after, not a mix: ' + states.join());
      assert.equal(states[0], 'sheet', 'a snapshot read: it is the moment the read began');
      const now = (await w.call({ op: 'getOrderPieces', orderId: RID.A })).body; assert.notEqual(now.rev, a.rev); assert.deepEqual([PA1, PA2].map(id => now.orders[RID.A].placement[id].state), ['held', 'held']);
    } finally { w.srv.close(); }
  });

  /* ── G · two at once; the sandbox ── */
  await t('G1 two take-offs at once on one sheet both land: the second finds the sheet changed and reads it again (optimistic retry), no listing is lost', async () => {
    const w = await fresh(false); try {
      w.st.txRetries = 0;
      // (straight to the handler, both started in the same breath: over HTTP the second request can start after the first is done)
      // and the first of them to reach its commit waits there until the other has finished: it read the sheet before the other changed it
      let armed = false, open; const gate = new Promise(r => { open = r; });
      w.st.tick = async what => { if (what === 'tx commit' && !armed) { armed = true; await gate; } };
      const direct = body => w.st.handlers.charmNestLibrary.handler({ httpMethod: 'POST', headers: {}, body: JSON.stringify(body) }).then(x => { open(); return x; });
      let x, y; try { [x, y] = await Promise.all([direct({ op: 'poolUpdate', poolIds: [PA1], patch: hold(), by: 'Paul' }), direct({ op: 'poolUpdate', poolIds: [PB1], patch: cancel(), by: 'Sam' })]); } finally { w.st.tick = null; }
      assert.equal(x.statusCode, 200, x.body); assert.equal(y.statusCode, 200, y.body);
      assert.deepEqual(w.doc('Charm_Nest_Sheets', S1).poolIds, [], 'neither piece is still listed'); assert.deepEqual(w.doc('Charm_Nest_Sheets', S1).orders, []);
      assert(w.st.txRetries >= 1, 'a transaction ran again (' + w.st.txRetries + ')'); assert.deepEqual(w.raw(), []);
    } finally { w.srv.close(); }
  });
  await t('G2 the sandbox has its own copies: a hold there edits Sandbox_ records only, and production\'s same-named records are untouched', async () => {
    const w = await fresh(true); try {
      seed(w.st, '');   // production holds the same ids
      const prod = JSON.stringify([...w.st.docs.entries()].filter(([k]) => !k.startsWith('Sandbox_')));
      const { found } = await w.watching([RID.A], async () => { assert.equal((await w.call({ op: 'poolUpdate', poolIds: [PA1, PA2], patch: hold(), by: 'Paul' })).status, 200); });
      assert.deepEqual(found, [], found.join('\n'));
      assert.deepEqual(w.doc('Charm_Nest_Sheets', S1).poolIds, [PB1]); assert.equal(w.doc('Charm_Pool', PA1).state, 'abandoned');
      assert.equal(JSON.stringify([...w.st.docs.entries()].filter(([k]) => !k.startsWith('Sandbox_') && !k.startsWith('Order_Timeline'))), JSON.stringify(JSON.parse(prod).filter(([k]) => !k.startsWith('Order_Timeline'))), 'production untouched');
      assert.equal((await w.call({ op: 'deleteSheet', id: S2, code: '975311' })).status, 200); assert.equal(w.st.docs.has('Charm_Nest_Sheets/' + S2), true, 'production\'s sheet is still there');
      assert.equal((await w.call({ op: 'putSheet', sheet: { id: S1, poolIds: [PA1, PB1] } })).status, 409, 'the guard reads the sandbox\'s rows');
    } finally { w.srv.close(); }
  });

  const failed = results.filter(([, e]) => e);
  console.log(`\n${results.length - failed.length}/${results.length} passed`);
  if (failed.length && !process.env.CC_BASELINE) process.exit(1);
})().catch(e => { console.error(e); process.exit(1); });
