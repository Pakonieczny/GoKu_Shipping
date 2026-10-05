// "Which sheet holds which piece of which order": one truth (Paul, 5 Oct 2026, round 2 point 1).
// An order with a Gold Filled charm on GF Sheet 1 and a Silver charm on SS Sheet 1 (both sheets in Set-1) read, from the Gold
// Filled sheet's Sheet tab, "2 FEMALE SYMBOL · not on a sheet yet" although the piece IS on SS Sheet 1 (and the other way round
// it was right), and the stale "pooled" held the Gold Filled sheet back. The pool row of the Silver piece had lost its sheet
// (the re-nest / restart paths clear it), the sheet's own record still listed the piece: this sheet came from the record, the
// others from the pool rows. Now both come from the sheets' records (OrderPieces).
//   node tests/charm-nest/order-pieces.cjs      (PW_DIR=<playwright node_modules>, CHROMIUM=<chrome>)
// A  the pure core (CharmNestOrderPieces.resolve/spread) on the fixture and its variants: the old rule is shown to fail first
// B  the server's read (getOrderPieces) over the real handler: production and sandbox collections, numbers and strings, read only
// C  the real order window's Sheet tab from BOTH sheets' sides, every variant, the Orders pill, and the order's other places
const path = require('path'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');
// OA_UI_ONLY=1: only Part C (the order window), so that it can be run against a clean checkout of main (which has no OrderPieces module) to show it red there
const UI_ONLY = !!process.env.OA_UI_ONLY;
const Core = UI_ONLY ? null : require(path.join(root, 'charm-nest-order-pieces.js'));
const { start } = require('./bridge-server.cjs');

const DAY = 86400, SHIP = Math.floor(Date.UTC(2026, 9, 2, 17) / 1000);
let ran = 0; const ok = (name) => { ran++; console.log('  ✓ ' + name); };

/* ── the fixture: sheets (saved records, listing their pieces), pool rows (some of them stale), order lines ── */
const SHEET = { gf1: 'sheet-oa-gf1', gf2: 'sheet-oa-gf2', ss1: 'sheet-oa-ss1', rg1: 'sheet-oa-rg1' };
const SET = 'set-oa-1';
const idsOf = (rid, tx, qty = 1) => Array.from({ length: qty }, (_, i) => `${rid}_${tx}_${i + 1}`);
const A = { rid: '4177100001', gf: '41771000011', ss: '41771000012' };                       // image 1 / 2: two pieces, two sheets
const B = { rid: '4177100002', gf: '41771000021', ss: '41771000022', rg: '41771000023' };    // three pieces over three sheets
const C = { rid: '4177100003', gf: '41771000031', ss: '41771000032' };                       // one piece on a sheet, one not nested yet
const D = { rid: '4177100004', gf: '41771000041', ss: '41771000042' };                       // quantity 2 (copies on two sheets) + one silver
const E = { rid: '4177100005', a: '41771000051', b: '41771000052' };                         // both pieces on the same sheet
const F = { rid: '4177100006', gf: '41771000061' };                                          // on a sheet whose record does not name the order
const sheets = (prefix = '') => ({
  [SHEET.gf1]: { id: SHEET.gf1, metal: 'gold', sheetIndex: 1, setId: SET, setSeq: 1, folder: '2026-10-02_GF_Set-1_Sheet-1', fileBase: '2026-10-02_GF_Set-1_Sheet-1', day: '2026-10-02', status: 'written', stock: { wPt: 300, hPt: 150 },
    orders: [A.rid, B.rid, C.rid, D.rid, E.rid], poolIds: [...idsOf(A.rid, A.gf), ...idsOf(B.rid, B.gf), ...idsOf(C.rid, C.gf), idsOf(D.rid, D.gf, 2)[0], ...idsOf(E.rid, E.a), ...idsOf(E.rid, E.b)] },
  [SHEET.gf2]: { id: SHEET.gf2, metal: 'gold', sheetIndex: 2, setId: SET, setSeq: 1, folder: '2026-10-02_GF_Set-1_Sheet-2', fileBase: '2026-10-02_GF_Set-1_Sheet-2', day: '2026-10-02', status: 'written', stock: { wPt: 300, hPt: 150 },
    orders: [D.rid], poolIds: [idsOf(D.rid, D.gf, 2)[1]] },
  [SHEET.ss1]: { id: SHEET.ss1, metal: 'silver', sheetIndex: 1, setId: SET, setSeq: 1, folder: '2026-10-02_SS_Set-1_Sheet-1', fileBase: '2026-10-02_SS_Set-1_Sheet-1', day: '2026-10-02', status: 'written', stock: { wPt: 300, hPt: 150 },
    orders: [A.rid, B.rid, D.rid], poolIds: [...idsOf(A.rid, A.ss), ...idsOf(B.rid, B.ss), ...idsOf(D.rid, D.ss)] },
  [SHEET.rg1]: { id: SHEET.rg1, metal: 'rose', sheetIndex: 1, setId: SET, setSeq: 1, folder: '2026-10-02_RG_Set-1_Sheet-1', fileBase: '2026-10-02_RG_Set-1_Sheet-1', day: '2026-10-02', status: 'written', stock: { wPt: 200, hPt: 200 },
    orders: [B.rid], poolIds: [...idsOf(B.rid, B.rg)] }
});
/** The pool rows as production left them: the GF pieces kept their sheet, the others lost it (a re-nest or an older run cleared it). */
const pools = () => {
  const row = (rid, tx, copy, qty, metal, sheetId, state, name) => ({ poolId: `${rid}_${tx}_${copy}`, orderId: rid, transactionId: tx, lineKey: `${rid}_${tx}`, sku: metal === 'gold' ? 'FEMALE_SYMBOL' : metal === 'silver' ? 'FEMALE_SYMBOL' : 'FEMALE_SYMBOL', material: metal, copy, quantity: qty, state, sheetId, sheetName: name || null, setId: sheetId ? SET : null, updatedAt: Date.now() });
  return [
    row(A.rid, A.gf, 1, 1, 'gold', SHEET.gf1, 'written', '2026-10-02_GF_Set-1_Sheet-1'), row(A.rid, A.ss, 1, 1, 'silver', null, 'ready'),
    row(B.rid, B.gf, 1, 1, 'gold', SHEET.gf1, 'written', '2026-10-02_GF_Set-1_Sheet-1'), row(B.rid, B.ss, 1, 1, 'silver', null, 'ready'), row(B.rid, B.rg, 1, 1, 'rose', null, 'ready'),
    row(C.rid, C.gf, 1, 1, 'gold', SHEET.gf1, 'written', '2026-10-02_GF_Set-1_Sheet-1'), row(C.rid, C.ss, 1, 1, 'silver', null, 'ready'),
    row(D.rid, D.gf, 1, 2, 'gold', SHEET.gf1, 'written', '2026-10-02_GF_Set-1_Sheet-1'), row(D.rid, D.gf, 2, 2, 'gold', SHEET.gf1, 'written', '2026-10-02_GF_Set-1_Sheet-1'),   // (copy 2 moved to GF Sheet 2: its row still says Sheet 1)
    row(D.rid, D.ss, 1, 1, 'silver', null, 'ready'),
    row(E.rid, E.a, 1, 1, 'gold', SHEET.gf1, 'written', '2026-10-02_GF_Set-1_Sheet-1'), row(E.rid, E.b, 1, 1, 'gold', null, 'ready')
  ];
};
const line = (tid, sku, metal, qty = 1) => ({ key: null, transactionId: tid, sku, title: sku + ' charm', material: metal, quantity: qty, state: 'pooled', poolIds: [], spec: {}, problems: [] });
const linesOf = (o, spec) => spec.map(([tid, metal, qty]) => Object.assign(line(tid, 'FEMALE_SYMBOL', metal, qty || 1), { key: `${o.rid}_${tid}` }));
const recs = () => Object.values(sheets());
const poolRows = () => pools();
const labelsFrom = (pieces, curId) => pieces.map(p => (p.sheetId === curId ? 'this sheet' : p.nested ? p.sheetLabel : 'not on a sheet yet'));

/** The rule the page used before (Sheet tab): this sheet from the sheet's record, the other pieces from their POOL ROWS only. */
function legacyLabels(rid, curSheet, lines) {
  const mine = new Set(curSheet.poolIds.filter(id => id.startsWith(rid + '_')));
  const out = [];
  for (const l of lines) for (const id of idsOf(rid, l.transactionId, l.quantity)) {
    const p = poolRows().find(x => x.poolId === id);
    out.push(mine.has(id) ? 'this sheet' : p && p.sheetId ? 'GF Sheet ' + (/_Sheet-(\d+)/.exec(p.sheetName || '') || [])[1] : 'not on a sheet yet');
  }
  return out;
}

function partA() {
  const S = sheets();
  const resolveFor = (o, lines, extra) => Core.resolve(Object.assign({ orderId: o.rid, lines, pools: poolRows(), sheets: recs(), sheetsKnown: true }, extra || {}));

  // 1 · image 1 / 2: the two-piece order; from BOTH sheets' sides
  const aLines = linesOf(A, [[A.gf, 'gold'], [A.ss, 'silver']]);
  assert.deepEqual(legacyLabels(A.rid, S[SHEET.gf1], aLines), ['this sheet', 'not on a sheet yet'], 'the old rule reproduces image 1 (GF side: "not on a sheet yet")');
  assert.deepEqual(legacyLabels(A.rid, S[SHEET.ss1], aLines).map(x => x), ['GF Sheet 1', 'this sheet'], 'and image 2 (SS side is right)');
  const a = resolveFor(A, aLines);
  assert.deepEqual(a.map(p => [p.key, p.state, p.sheetId, p.sheetLabel, p.setId]), [[`${A.rid}_${A.gf}_1`, 'sheeted', SHEET.gf1, 'GF Sheet 1', SET], [`${A.rid}_${A.ss}_1`, 'sheeted', SHEET.ss1, 'SS Sheet 1', SET]]);
  assert.deepEqual(labelsFrom(a, SHEET.gf1), ['this sheet', 'SS Sheet 1'], 'GF side: the Silver piece is on SS Sheet 1');
  assert.deepEqual(labelsFrom(a, SHEET.ss1), ['GF Sheet 1', 'this sheet'], 'SS side');
  assert(a.every(p => p.nested && !p.loading && p.index > 0 && p.lineKey && p.copy === 1), 'shape');
  let sp = Core.spread(a);
  assert.equal(sp.multi, true); assert.equal(sp.spreadAcross, true); assert.deepEqual(sp.unnested, []); assert.deepEqual(sp.sheets.map(s => [s.sheetId, s.sheetLabel, s.setId]), [[SHEET.gf1, 'GF Sheet 1', SET], [SHEET.ss1, 'SS Sheet 1', SET]]);
  ok('A1 two-piece order over GF Sheet 1 and SS Sheet 1: every piece on its own sheet, from both sides; the old rule is red on the same data');

  // 2 · three pieces over three sheets
  const b = resolveFor(B, linesOf(B, [[B.gf, 'gold'], [B.ss, 'silver'], [B.rg, 'rose']]));
  assert.deepEqual(b.map(p => p.sheetLabel), ['GF Sheet 1', 'SS Sheet 1', 'RG Sheet 1']);
  for (const [cur, want] of [[SHEET.gf1, ['this sheet', 'SS Sheet 1', 'RG Sheet 1']], [SHEET.ss1, ['GF Sheet 1', 'this sheet', 'RG Sheet 1']], [SHEET.rg1, ['GF Sheet 1', 'SS Sheet 1', 'this sheet']]]) assert.deepEqual(labelsFrom(b, cur), want, cur);
  sp = Core.spread(b); assert.equal(sp.sheets.length, 3); assert.equal(sp.spreadAcross, true);
  ok('A2 three pieces over three sheets (GF, SS, RG): the same list from each of the three sheets');

  // 3 · one piece not nested yet: it is the only issue, and it says so; the nested one is not
  const cLines = linesOf(C, [[C.gf, 'gold'], [C.ss, 'silver']]);
  const c = resolveFor(C, cLines);
  assert.deepEqual(labelsFrom(c, SHEET.gf1), ['this sheet', 'not on a sheet yet']);
  assert.deepEqual(c.map(p => [p.state, p.nested]), [['sheeted', true], ['unnested', false]]);
  assert.match(c[1].reason, /waiting to be placed/); assert.equal(c[1].loading, false);
  sp = Core.spread(c); assert.equal(sp.unnested.length, 1); assert.equal(sp.unnested[0].key, `${C.rid}_${C.ss}_1`); assert.equal(sp.spreadAcross, false); assert.equal(sp.sheets.length, 1);
  ok('A3 one piece nested and one not: unnested lists exactly that piece, with its reason');

  // 4 · quantity 2: copies on two sheets (the pool row of copy 2 still names Sheet 1: the record of Sheet 2 is the truth)
  const dLines = linesOf(D, [[D.gf, 'gold', 2], [D.ss, 'silver']]);
  const d = resolveFor(D, dLines);
  assert.deepEqual(d.map(p => [p.copy, p.qty, p.sheetLabel]), [[1, 2, 'GF Sheet 1'], [2, 2, 'GF Sheet 2'], [1, 1, 'SS Sheet 1']]);
  assert.deepEqual(legacyLabels(D.rid, S[SHEET.gf1], dLines), ['this sheet', 'GF Sheet 1', 'not on a sheet yet'], 'the old rule gets copy 2 and the silver piece wrong');
  assert.deepEqual(labelsFrom(d, SHEET.gf2), ['GF Sheet 1', 'this sheet', 'SS Sheet 1']);
  sp = Core.spread(d); assert.equal(sp.sheets.length, 3);
  ok('A4 quantity 2: copy 1 on GF Sheet 1, copy 2 on GF Sheet 2 although its pool row says Sheet 1, plus the silver piece');

  // 5 · both pieces on the same sheet: not spread, nothing outside
  const e = resolveFor(E, linesOf(E, [[E.a, 'gold'], [E.b, 'gold']]));
  assert.deepEqual(labelsFrom(e, SHEET.gf1), ['this sheet', 'this sheet']);
  sp = Core.spread(e); assert.equal(sp.spreadAcross, false); assert.equal(sp.sheets.length, 1); assert.deepEqual(sp.unnested, []); assert.equal(sp.multi, true);
  ok('A5 both pieces on one sheet: one sheet, spreadAcross false (the pool row of piece 2 lost its sheet and it does not matter)');

  // 6 · records not read yet: a hint, never a claim; read and silent: the pool row's sheet is out of date
  const early = Core.resolve({ orderId: A.rid, lines: aLines, pools: poolRows(), sheets: [], sheetsKnown: false });
  assert.deepEqual(early.map(p => [p.state, p.via || '', p.loading]), [['sheeted', 'hint', false], ['unnested', '', true]], 'while the records are unread: the pool row is a hint, the lost one is "loading", never "not on a sheet"');
  const failed = Core.resolve({ orderId: A.rid, lines: aLines, pools: poolRows(), sheets: [], sheetsKnown: false, failed: true });
  assert.equal(failed[1].unsure, true); assert.equal(failed[1].loading, false); assert.match(failed[1].reason, /could not be read/);
  const stale = Core.resolve({ orderId: A.rid, lines: aLines, pools: poolRows(), sheets: [Object.assign({}, S[SHEET.gf1], { poolIds: ['x_y_1'] })], sheetsKnown: true });
  assert.deepEqual(stale.map(p => p.state), ['unnested', 'unnested'], 'a sheet record that is read and does not list the piece contradicts the pool row that names it');
  ok('A6 unread records give a hint and "loading"; an unreadable answer is "unsure"; a record that lists other pieces contradicts a stale pool row');

  // 7 · a stale twin (an archived record of the same sheet) and a draft never win; a live page is the truth for a sheet this sorter holds
  const twin = Object.assign({}, S[SHEET.ss1], { id: 'sheet-old-ss1', archived: true, sheetIndex: 9 });
  const draft = Object.assign({}, S[SHEET.ss1], { id: 'sheet-draft-ss', draft: true, sheetIndex: 7 });
  const t = Core.resolve({ orderId: A.rid, lines: aLines, pools: poolRows(), sheets: [twin, draft, ...recs()], sheetsKnown: true });
  assert.equal(t[1].sheetId, SHEET.ss1, 'the archived twin and the draft do not win');
  const pg = { sheetId: SHEET.ss1, metal: 'silver', sheetIndex: 1, setId: SET, placedCount: 3, placements: [1, 2, 3] };
  const off = Core.resolve({ orderId: A.rid, lines: aLines, pools: poolRows(), sheets: recs(), sheetsKnown: true, livePlaced: id => (id === `${A.rid}_${A.gf}_1` ? { sheetId: SHEET.gf1, metal: 'gold', sheetIndex: 1, placedCount: 4 } : null), liveSheet: sid => (sid === SHEET.ss1 ? pg : sid === SHEET.gf1 ? { sheetId: SHEET.gf1, placedCount: 4 } : null) });
  assert.equal(off[1].nested, false, 'the live SS page no longer places the piece (taken off): the saved record\'s claim is dropped');
  assert.equal(off[0].nested, true);
  const fresh = Core.resolve({ orderId: A.rid, lines: aLines, pools: poolRows(), sheets: [], sheetsKnown: false, livePlaced: id => (id.endsWith(`_${A.ss}_1`) ? { sheetId: null, metal: 'silver', page: 2 } : null) });
  assert.deepEqual([fresh[1].state, fresh[1].nested, fresh[1].sheetId, fresh[1].sheetLabel], ['nested', true, null, 'SS Sheet 2'], 'on a page of this sorter that is not saved yet: nested, no sheet id');
  ok('A7 archived twin and draft never win; a live page is the truth for its sheet; an unsaved page counts as nested');

  // 8 · problems
  const lp = [Object.assign(line(A.gf, '', 'gold'), { key: `${A.rid}_${A.gf}` }), Object.assign(line(A.ss, 'WEIRD', 'silver'), { key: `${A.rid}_${A.ss}`, state: 'unmatched' }), Object.assign(line('41771000019', 'HELD', 'gold'), { key: `${A.rid}_41771000019`, hold: 'wait' }), Object.assign(line('41771000018', 'ND', 'gold'), { key: `${A.rid}_41771000018`, state: 'noDesign' })];
  const pr = Core.resolve({ orderId: A.rid, lines: lp, pools: [], sheets: [], sheetsKnown: true });
  assert.deepEqual(pr.map(p => p.problem), ['noSku', 'unmatched', 'held', 'noDesign']);
  assert.deepEqual(pr.map(p => p.reason), ['it has no SKU', 'its SKU is not in any master file', 'it is on hold', 'it has no design yet']);
  ok('A8 problems: noSku / unmatched / held / noDesign, each with its plain reason');

  // 9 · readiness (Issues-truth owns orderReports): a stale "pooled" on the Silver line must not hold either sheet
  const R = require(path.join(root, 'charm-nest-readiness.js'));
  const rows = [Object.assign({ order: { receiptId: A.rid }, line: { transactionId: A.gf }, key: `${A.rid}_${A.gf}`, state: 'pooled', poolIds: idsOf(A.rid, A.gf), spec: { quantity: 1 } }),
    Object.assign({ order: { receiptId: A.rid }, line: { transactionId: A.ss }, key: `${A.rid}_${A.ss}`, state: 'pooled', poolIds: idsOf(A.rid, A.ss), spec: { quantity: 1 } })];
  const done = Object.values(sheets()).filter(s => s.id !== SHEET.rg1).map(s => Object.assign({}, s, { laserDoneAt: Date.now() }));
  const rep = R.orderReports(rows, done)[A.rid];
  assert.equal(rep.ready, true, 'orderReports: a stale "pooled" line whose piece a sheet lists is not a block: ' + JSON.stringify(rep));
  ok('A9 readiness: the stale "pooled" Silver line no longer holds the order back (Issues-truth\'s rule; it did on clean main before d529858f)');

  // 10 · the two agree on WHICH piece is "piece 2": CharmNestReadiness.pieces (the Library's issues panel) and OrderPieces number the same pieces the same way
  const X = { rid: '4177100007', a: '41771000073', b: '41771000071', c: '41771000072', d: '41771000074' };
  const xRows = [[X.a, 'gold', 1, []], [X.b, 'silver', 2, [`${X.rid}_${X.b}_2`, `${X.rid}_${X.b}_1`]], [X.c, 'gold', 1, []], [X.d, 'gold', 1, []]].map(([tx, metal, qty, poolIds]) => ({ order: { receiptId: X.rid }, line: { transactionId: tx }, key: `${X.rid}_${tx}`, transactionId: tx, quantity: qty, material: metal, sku: 'FEMALE_SYMBOL', state: tx === X.c ? 'noDesign' : 'pooled', poolIds, spec: { quantity: qty } }));
  xRows.push({ order: { receiptId: X.rid }, line: { transactionId: '41771000079' }, key: `${X.rid}_41771000079`, transactionId: '41771000079', quantity: 1, state: 'gone', poolIds: [`${X.rid}_41771000079_1`], spec: { quantity: 1 } });
  const xSheets = [{ id: 'sheet-oa-x1', metal: 'gold', sheetIndex: 1, setId: SET, status: 'written', folder: '2026-10-02_GF_Set-1_Sheet-1', orders: [X.rid], poolIds: [`${X.rid}_${X.a}_1`, `${X.rid}_${X.d}_1`] }, { id: 'sheet-oa-x2', metal: 'silver', sheetIndex: 2, setId: SET, status: 'written', folder: '2026-10-02_SS_Set-1_Sheet-2', orders: [X.rid], poolIds: [`${X.rid}_${X.b}_2`] }];
  const theirs = R.pieces(xRows, xSheets)[X.rid], mine = Core.resolve({ orderId: X.rid, lines: xRows, pools: [], sheets: xSheets, sheetsKnown: true }).filter(p => !p.gone && !p.noDesign);
  const shape = ps => ps.map(p => [p.index, p.key, p.sheetId || null]);
  assert.deepEqual(shape(mine), shape(theirs), 'the same numbering and the same sheet');
  assert.deepEqual(theirs.map(p => p.key), [`${X.rid}_${X.b}_2`, `${X.rid}_${X.b}_1`, `${X.rid}_${X.a}_1`, `${X.rid}_${X.d}_1`], 'lines by the number at the end of their key, copies in their poolIds order; no-design and cancelled lines are no pieces');
  ok('A10 "piece 2" is the same piece in the issues panel (CharmNestReadiness.pieces) and in OrderPieces: line order, poolIds order, and the same sheet');
}

async function partB(srv) {
  const call = async (body) => { const r = await fetch(`${srv.sorterOrigin}/.netlify/functions/charmNestLibrary`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) }); return { status: r.status, body: await r.json() }; };
  const seed = (prefix) => {
    for (const s of Object.values(sheets())) srv.st.put(prefix + 'Charm_Nest_Sheets', s.id, s);
    srv.st.put(prefix + 'Charm_Nest_Sheets', 'sheet-gone', Object.assign({}, sheets()[SHEET.ss1], { id: 'sheet-gone', archived: true }));
    srv.st.put(prefix + 'Charm_Nest_Sheets', 'sheet-nolist', { id: 'sheet-nolist', metal: 'silver', sheetIndex: 4, orders: [], poolIds: [`${F.rid}_${F.gf}_1`], setId: SET });   // lists the piece, does not name the order
    srv.st.put(prefix + 'Charm_Pool', `${F.rid}_${F.gf}_1`, { poolId: `${F.rid}_${F.gf}_1`, orderId: F.rid, transactionId: F.gf, lineKey: `${F.rid}_${F.gf}`, sku: 'F', material: 'silver', copy: 1, quantity: 1, state: 'ready', sheetId: null });
    for (const p of pools()) srv.st.put(prefix + 'Charm_Pool', p.poolId, p);
  };
  seed('');
  const before = JSON.stringify([...srv.st.docs.entries()]);
  let r = await call({ op: 'getOrderPieces', orderId: A.rid });
  assert.equal(r.status, 200, JSON.stringify(r.body));
  const o = r.body.orders[A.rid];
  assert.deepEqual(o.pools.map(p => p.poolId).sort(), [`${A.rid}_${A.gf}_1`, `${A.rid}_${A.ss}_1`].sort());
  assert.deepEqual(o.sheets.map(s => s.id).sort(), [SHEET.gf1, SHEET.ss1].sort(), 'the sheets that list the order\'s pieces, the archived twin left out');
  const f = (await call({ op: 'getOrderPieces', orderId: F.rid })).body.orders[F.rid];
  assert.deepEqual(f.sheets.map(s => s.id), ['sheet-nolist'], 'a sheet that lists the piece but does not name the order is found by its poolIds');
  assert(o.sheets.every(s => s.poolIds.every(p => p.startsWith(A.rid + '_'))), 'only the order\'s own poolIds go out');
  assert.equal(JSON.stringify([...srv.st.docs.entries()]), before, 'a read: no document changed');
  ok('B1 getOrderPieces: the order\'s pool rows and the saved sheets that list its pieces (archived left out, found by poolIds too); nothing written');

  r = await call({ op: 'getOrderPieces', orderIds: [A.rid, B.rid, D.rid, 'x', '12'] });
  assert.deepEqual(Object.keys(r.body.orders).sort(), [A.rid, B.rid, D.rid].sort());
  assert.deepEqual(r.body.orders[D.rid].sheets.map(s => s.id).sort(), [SHEET.gf1, SHEET.gf2, SHEET.ss1].sort());
  assert.equal(r.body.orders[B.rid].sheets.length, 3);
  assert.equal((await call({ op: 'getOrderPieces', orderId: 'abc' })).status, 400);
  ok('B2 several orders in one read; bad ids are dropped; none valid is an error');

  // numbers: an order id stored as a number is found too
  srv.st.put('Charm_Pool', '4177199999_41771999991_1', { poolId: '4177199999_41771999991_1', orderId: 4177199999, lineKey: '4177199999_41771999991', sku: 'N', material: 'gold', copy: 1, quantity: 1, state: 'written', sheetId: SHEET.gf1 });
  srv.st.put('Charm_Nest_Sheets', 'sheet-num', { id: 'sheet-num', metal: 'gold', sheetIndex: 5, orders: [4177199999], poolIds: ['4177199999_41771999991_1'] });
  r = await call({ op: 'getOrderPieces', orderId: '4177199999' });
  assert.equal(r.body.orders['4177199999'].pools.length, 1); assert.deepEqual(r.body.orders['4177199999'].sheets.map(s => s.id), ['sheet-num']);
  ok('B3 an order number stored as a number is read as well');

  // the sandbox's own records: a sandbox read sees the Sandbox_ collections only, a production read never sees them
  srv.st.docs.clear(); seed('Sandbox_');
  r = await call({ op: 'getOrderPieces', orderId: A.rid, sandbox: true });
  assert.deepEqual(r.body.orders[A.rid].sheets.map(s => s.id).sort(), [SHEET.gf1, SHEET.ss1].sort());
  r = await call({ op: 'getOrderPieces', orderId: A.rid });
  assert.deepEqual(r.body.orders[A.rid], { pools: [], sheets: [] }, 'production does not see the sandbox');
  ok('B4 sandbox prefix: the Sandbox_ records are read for a sandbox call and never for production');
  srv.st.docs.clear();
}

async function partC(srv) {
  const pwDir = process.env.PW_DIR || path.join(root, 'node_modules');
  let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { console.log('  – no playwright-core: the browser checks were not run'); return; }
  const box = (id, cx, cy, w = 30) => ({ id, cxPt: cx, cyPt: cy, angle: 0, wPt: w, hPt: w });
  const seedAll = (prefix) => {
    let n = 0;
    for (const s of Object.values(sheets())) {
      const placements = [], charms = [];
      s.poolIds.forEach((pid, i) => { const id = 'c' + (++n); placements.push(box(id, 30 + (i % 8) * 34, 30 + Math.floor(i / 8) * 40)); charms.push({ id, name: `${pid.split('_')[0]} · FEMALE_SYMBOL`, poolId: pid, order: pid.split('_')[0], sku: 'FEMALE_SYMBOL' }); });
      srv.st.put(prefix + 'Charm_Nest_Sheets', s.id, Object.assign({}, s, { placements, charms, updatedAt: Date.now() }));
    }
    for (const p of pools()) srv.st.put(prefix + 'Charm_Pool', p.poolId, p);
  };
  const orderOf = (o, buyer, spec) => ({ receiptId: o.rid, orderNumber: o.rid, createTs: SHIP - 5 * DAY, updateTs: SHIP - 5 * DAY + 60, shipBy: SHIP, buyer: { name: buyer }, buyerMessage: '', isGift: false, giftMessage: '', staffNote: '', messages: [],
    lines: spec.map(([tid, key, label, qty]) => ({ transactionId: tid, listingId: '18000' + tid.slice(-5), sku: 'FEMALE_SYMBOL', title: 'Female symbol charm', quantity: qty || 1, expectedShipDate: SHIP, variations: [{ name: 'Metal', value: label }], metalKey: key, metalLabel: label, personalization: [], buyerMessage: '' })) });
  const specs = {
    A: [A, 'Nathaly Test', [[A.gf, 'gold', '14k Gold Filled'], [A.ss, 'silver', 'Sterling Silver']]],
    B: [B, 'Three Sheets', [[B.gf, 'gold', '14k Gold Filled'], [B.ss, 'silver', 'Sterling Silver'], [B.rg, 'rose', 'Rose Gold Filled']]],
    C: [C, 'One Waiting', [[C.gf, 'gold', '14k Gold Filled'], [C.ss, 'silver', 'Sterling Silver']]],
    D: [D, 'Two Copies', [[D.gf, 'gold', '14k Gold Filled', 2], [D.ss, 'silver', 'Sterling Silver']]],
    E: [E, 'Same Sheet', [[E.a, 'gold', '14k Gold Filled'], [E.b, 'gold', '14k Gold Filled']]]
  };
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  const errors = [];
  async function session(sandbox) {
    srv.st.docs.clear(); seedAll(sandbox ? 'Sandbox_' : '');
    const context = await browser.newContext({ viewport: { width: 1440, height: 900 } });
    await context.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => /fonts\.googleapis|fonts\.gstatic/.test(r.request().url()) ? r.fulfill({ status: 200, contentType: 'text/css', body: '' }) : r.abort());
    await context.addInitScript(sb => { try { if (!sessionStorage.getItem('__seeded')) { localStorage.setItem('cn.settings', JSON.stringify({ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on', sandbox: sb ? 'on' : 'off' })); localStorage.setItem('cn.employee', 'Test Operator'); sessionStorage.setItem('__seeded', '1'); } } catch (_) {} window.prompt = () => 'Test Operator'; }, sandbox);
    const page = await context.newPage();
    page.setDefaultTimeout(25000);
    page.on('pageerror', e => { errors.push(e.message); console.error('page error:', String(e.stack || e.message).split('\n').slice(0, 4).join(' | ')); });
    await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
    await page.waitForFunction(() => window.CN && window.Orders && window.OrderWin && window.SheetWin && SheetWin.drawOrder && CN.S.cloud.ok === true, null, { timeout: 60000 });
    await page.evaluate(async ({ orders }) => {
      await Orders.loadMaps(true);
      for (const [order, pools] of orders) order.lines.forEach((line, i) => { const key = CharmNestOrders.lineKey(order, line); const row = { key, order, line, arrivedAt: Date.now(), spec: null, problems: [], state: pools[i] && pools[i].length ? 'pooled' : 'pulled', reason: null, claimedBy: null, poolIds: pools[i] || [], engrave: null, material: null }; B.orders.rows.push(row); B.orders.byKey.set(key, row); });
      Orders.interpretAll();
      CN.setMode('orders'); Orders.render();
    }, { orders: Object.values(specs).map(([o, buyer, sp]) => { const order = orderOf(o, buyer, sp); return [order, sp.map(([tid, , , qty]) => idsOf(o.rid, tid, qty || 1))]; }) });
    return { page, context };
  }
  const drawn = (page, rid, sheet) => page.waitForFunction(([rid, sheet]) => { const i = window.OrderWin && OrderWin._sheet(); return i && i.sheet.id === sheet && i.mine.every(x => x.rid === rid) && document.getElementById('owPlateWait').hidden; }, [rid, sheet]);
  const read = page => page.evaluate(() => ({ tabs: [...document.querySelectorAll('#owSheetPanel .owShTabs button')].map(b => b.textContent.trim()), on: document.querySelector('#owSheetPanel .owShTabs button.on')?.textContent.trim(),
    pieces: [...document.querySelectorAll('#owSheetPanel .owPieces li')].map(li => ({ n: li.querySelector('.n')?.textContent.trim(), where: li.querySelector('em')?.textContent.trim() || '', off: li.classList.contains('off') })) }));
  const settle = async (page, rid) => { await page.waitForFunction(r => !window.OrderPieces || OrderPieces.known(r), rid); await page.waitForTimeout(900); };
  const open = async (page, o, key, sheetId, tabText) => {
    if (await page.evaluate(() => document.getElementById('orderWin').open)) { await page.evaluate(() => OrderWin.close()); await page.waitForFunction(() => !document.getElementById('orderWin').open); }
    await page.evaluate(k => OrderWin.open(k, { view: 'sheet' }), key);
    await page.evaluate(() => OrderWin.view() === 'sheet' || document.querySelector('.owTabsV [data-ow-view="sheet"]').click());
    await page.waitForFunction(() => document.querySelectorAll('#owSheetPanel .owShTabs button').length > 0);
    await settle(page, o.rid);
    await page.evaluate(t => { const b = [...document.querySelectorAll('#owSheetPanel .owShTabs button')].find(x => x.textContent.trim() === t); if (!b) throw new Error('no tab ' + t + ' among ' + [...document.querySelectorAll('#owSheetPanel .owShTabs button')].map(x => x.textContent.trim())); if (!b.classList.contains('on')) b.click(); }, tabText);
    await drawn(page, o.rid, sheetId);
    await page.waitForTimeout(300);
    return read(page);
  };
  try {
    for (const sandbox of [false, true]) {
      const tag = sandbox ? 'sandbox' : 'production';
      const { page, context } = await session(sandbox);
      const keyOf = (o, tx) => `${o.rid}_${tx}`;
      // image 1 and image 2: the same order, the GF sheet and the SS sheet
      let p = await open(page, A, keyOf(A, A.gf), SHEET.gf1, 'GF Sheet 1');
      assert.deepEqual(p.tabs.sort(), ['GF Sheet 1', 'SS Sheet 1'], `${tag} A: both sheets have a tab`);
      assert.deepEqual(p.pieces.map(x => x.where), ['this sheet', 'SS Sheet 1'], `${tag} A, GF Sheet 1 selected (image 1): the Silver piece is on SS Sheet 1: ${JSON.stringify(p)}`);
      p = await open(page, A, keyOf(A, A.gf), SHEET.ss1, 'SS Sheet 1');
      assert.deepEqual(p.pieces.map(x => x.where), ['GF Sheet 1', 'this sheet'], `${tag} A, SS Sheet 1 selected (image 2): ${JSON.stringify(p)}`);
      assert(p.pieces.every(x => !x.off), 'neither piece is dimmed as "not on a sheet"');
      ok(`C1 ${tag}: image 1 / image 2: GF side "this sheet · SS Sheet 1", SS side "GF Sheet 1 · this sheet"`);
      // opened from the SILVER line (the other line of the order): the same
      p = await open(page, A, keyOf(A, A.ss), SHEET.gf1, 'GF Sheet 1');
      assert.deepEqual(p.pieces.map(x => x.where), ['this sheet', 'SS Sheet 1'], `${tag} A opened from the silver line: ${JSON.stringify(p)}`);
      // a click on the piece on the other sheet goes to that sheet
      await page.evaluate(() => document.querySelectorAll('#owSheetPanel .owPieces li')[1].click());
      await drawn(page, A.rid, SHEET.ss1);
      assert.equal((await read(page)).on, 'SS Sheet 1', 'clicking the SS piece shows SS Sheet 1');
      ok(`C2 ${tag}: opened from either line the list is the same; a click on a piece of another sheet goes to that sheet`);

      // three pieces, three sheets: from each of the three
      for (const [tab, sid, want] of [['GF Sheet 1', SHEET.gf1, ['this sheet', 'SS Sheet 1', 'RG Sheet 1']], ['SS Sheet 1', SHEET.ss1, ['GF Sheet 1', 'this sheet', 'RG Sheet 1']], ['RG Sheet 1', SHEET.rg1, ['GF Sheet 1', 'SS Sheet 1', 'this sheet']]]) {
        p = await open(page, B, keyOf(B, B.gf), sid, tab);
        assert.deepEqual(p.tabs.sort(), ['GF Sheet 1', 'RG Sheet 1', 'SS Sheet 1'], `${tag} B tabs`);
        assert.deepEqual(p.pieces.map(x => x.where), want, `${tag} B from ${tab}: ${JSON.stringify(p)}`);
      }
      ok(`C3 ${tag}: three pieces over three sheets: the same list from each of the three sheets`);

      // one piece not nested yet: it says so, from the sheet that holds the other piece, and is dimmed
      p = await open(page, C, keyOf(C, C.gf), SHEET.gf1, 'GF Sheet 1');
      assert.deepEqual(p.pieces.map(x => x.where), ['this sheet', 'not on a sheet yet'], `${tag} C: ${JSON.stringify(p)}`);
      assert.deepEqual(p.pieces.map(x => x.off), [false, true]); assert.deepEqual(p.tabs, ['GF Sheet 1'], 'no tab for a sheet the piece is not on');
      ok(`C4 ${tag}: a piece that really is not nested yet still says "not on a sheet yet" (and has no sheet tab)`);

      // quantity 2: copy 2 on GF Sheet 2 although its pool row says Sheet 1
      p = await open(page, D, keyOf(D, D.gf), SHEET.gf1, 'GF Sheet 1');
      assert.deepEqual(p.tabs.sort(), ['GF Sheet 1', 'GF Sheet 2', 'SS Sheet 1']);
      assert.deepEqual(p.pieces.map(x => x.where), ['this sheet', 'GF Sheet 2', 'SS Sheet 1'], `${tag} D from GF 1: ${JSON.stringify(p)}`);
      p = await open(page, D, keyOf(D, D.gf), SHEET.gf2, 'GF Sheet 2');
      assert.deepEqual(p.pieces.map(x => x.where), ['GF Sheet 1', 'this sheet', 'SS Sheet 1'], `${tag} D from GF 2: ${JSON.stringify(p)}`);
      ok(`C5 ${tag}: quantity 2: the copy on the other GF sheet is named by that sheet, from both GF sheets`);

      // both pieces on one sheet
      p = await open(page, E, keyOf(E, E.a), SHEET.gf1, 'GF Sheet 1');
      assert.deepEqual(p.pieces.map(x => x.where), ['this sheet', 'this sheet'], `${tag} E: ${JSON.stringify(p)}`); assert.deepEqual(p.tabs, ['GF Sheet 1']);
      ok(`C6 ${tag}: both pieces on the same sheet: both "this sheet", one tab`);

      // the single truth, asked by the other screens
      if (await page.evaluate(() => !!window.OrderPieces)) {
        const s = await page.evaluate(([a, b, c]) => ({ a: OrderPieces.spread(a), b: OrderPieces.spread(b), c: OrderPieces.spread(c) }), [A.rid, B.rid, C.rid]);
        assert.equal(s.a.spreadAcross, true); assert.equal(s.a.sheets.length, 2); assert.equal(s.b.sheets.length, 3); assert.equal(s.c.unnested.length, 1); assert.equal(s.c.sheets.length, 1);
        // (the Library's rows hold the sheets: every order on a sheet and on a set is known without another read)
        await page.evaluate(() => { CN.S.library.kind = 'sheets'; return CN.loadLibrary(); });
        await page.waitForFunction(() => (CN.S.library.rows || []).length >= 4);
        const onGf = await page.evaluate(sid => OrderPieces.onSheet(sid).map(x => [x.orderId, x.pieces.length]).sort(), SHEET.gf1);
        assert.deepEqual(onGf, [[A.rid, 1], [B.rid, 1], [C.rid, 1], [D.rid, 1], [E.rid, 2]].sort(), 'every order with a piece on GF Sheet 1, and how many');
        const inSet = await page.evaluate(sid => OrderPieces.ordersOf(sid).map(x => [x.orderId, x.pieces.length]).sort(), SET);
        assert.deepEqual(inSet, [[A.rid, 2], [B.rid, 3], [C.rid, 1], [D.rid, 3], [E.rid, 2]].sort(), 'every order with pieces on the set\'s sheets');
        // (without them: loadSheet reads the sheet whole and its orders' records)
        await page.evaluate(() => { CN.S.library.rows = []; });
        await page.evaluate(sid => OrderPieces.loadSheet(sid), SHEET.gf1);
        const onGf2 = await page.evaluate(sid => OrderPieces.onSheet(sid).map(x => [x.orderId, x.pieces.length]).sort(), SHEET.gf1);
        assert.deepEqual(onGf2, onGf, 'loadSheet: the same without the Library rows');
        await page.evaluate(() => { CN.S.library.kind = 'sheets'; return CN.loadLibrary(); });
        await page.waitForFunction(() => (CN.S.library.rows || []).length >= 4);
        // the Orders list pill: the lines read "nested", not "pooled", wherever the page holds their sheets only as records
        const pills = await page.evaluate(([a]) => B.orders.rows.filter(r => String(r.order.receiptId) === a).map(r => Orders.statePill(r)[1]), [A.rid]);
        assert(pills.every(t => /nested/.test(t)), `${tag}: both lines of the order read nested: ${pills}`);
        ok(`C7 ${tag}: OrderPieces.spread / onSheet / ordersOf answer the other screens; both lines read "nested" in the Orders list`);
      }
      await context.close();
    }
  } finally { await browser.close(); }
  assert.deepEqual(errors, [], 'no page errors: ' + errors.join(' | '));
}

(async () => {
  console.log('order-pieces: one truth for which sheet holds which piece');
  if (!UI_ONLY) partA();
  const srv = await start({ receipts: [] });
  try { if (!UI_ONLY) await partB(srv); await partC(srv); } finally { srv.close(); }
  console.log(`order-pieces: ${ran} checks passed`);
})().catch(e => { console.error(e); process.exit(1); });
