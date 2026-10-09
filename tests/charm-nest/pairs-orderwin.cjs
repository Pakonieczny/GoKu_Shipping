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

/* ── Amendment 2 (Paul, 9 Oct 18:47): every EARRING pair, matching or mismatched, is a Left and a Right piece per unit, the Right the mirror image; the real shared module decides ── */
{
  const CP = require(path.join(root, 'charm-nest-pair.js'));
  const rid = '4200000020', tx = '9201';
  const real = l => { const mis = /^MISMATCHED/.test(l.sku); const x = CP.piecesFor({ receiptId: l.key.split('_')[0], transactionId: l.transactionId, quantity: l.quantity, form: l.form || '', spec: l.spec || {} }, mis ? { sku: l.sku, pair: { v: 1, bodies: 2, mismatched: true } } : null); return x.some(p => p.side) ? x : null; };
  const kindReal = l => { const mis = /^MISMATCHED/.test(l.sku); const arg = { receiptId: l.key.split('_')[0], transactionId: l.transactionId, quantity: l.quantity, form: l.form || '', spec: l.spec || {} }; return real(l) ? CP.kindOf(arg, mis ? { sku: l.sku, pair: { v: 1, bodies: 2, mismatched: true } } : null) : null; };
  // 10 · a matching stud pair, nothing pooled yet: a Left and a Right, the Right turned over
  let ps = Core.resolve({ orderId: rid, lines: [line(rid, tx, 'HEART_STUD', 1, { form: 'earrings' })], pools: [], sheets: [], sheetsKnown: true, pairOf: real, kindOf: kindReal });
  assert.deepEqual(ps.map(p => p.side), ['L', 'R']); assert.deepEqual(ps.map(p => p.mirror), [false, true]); assert.ok(ps.every(p => p.kind === 'pair' && p.groupSize === 2 && p.bodyIndex === 0));
  assert.deepEqual(ps.map(p => p.label), ['HEART STUD · Left', 'HEART STUD · Right']); ok('A10 a matching earring pair makes a Left and a Right, the Right a mirror image');
  ps = Core.resolve({ orderId: rid, lines: [line(rid, tx, 'HEART_STUD', 2, { form: 'huggie' })], pools: [], sheets: [], sheetsKnown: true, pairOf: real, kindOf: kindReal });
  assert.deepEqual(ps.map(p => p.side), ['L', 'R', 'L', 'R']); assert.equal(ps[0].kind, 'multi'); ok('A10b quantity 2 makes two Lefts and two Rights');
  // 10c · the pool rows' own side and mirror win; a Right with the mirror flag false (the drawing faces right) is kept
  const pr = [row(rid, tx, 1, { sku: 'HEART_STUD', side: 'R', mirror: false, bodyIndex: 0 }), row(rid, tx, 2, { sku: 'HEART_STUD', side: 'L', mirror: true, bodyIndex: 0 })];
  ps = Core.resolve({ orderId: rid, lines: [line(rid, tx, 'HEART_STUD', 1, { form: 'earrings', poolIds: pr.map(p => p.poolId) })], pools: pr, sheets: [], sheetsKnown: true, pairOf: real, kindOf: kindReal });
  assert.deepEqual(ps.map(p => [p.side, p.mirror]), [['R', false], ['L', true]]); ok('A10c the pool row says its side and its direction');
  // 10d · an OLD matching record (one pool piece, no side) is one piece, never a false split, and says no side
  const old = [row(rid, tx, 1, { sku: 'HEART_STUD', sheetId: SHEET.a })];
  ps = Core.resolve({ orderId: rid, lines: [line(rid, tx, 'HEART_STUD', 1, { form: 'earrings', poolIds: [`${rid}_${tx}_1`] })], pools: old, sheets: [sheet(SHEET.a, 'gold', 1, [`${rid}_${tx}_1`])], sheetsKnown: true, pairOf: real, kindOf: kindReal });
  assert.equal(ps.length, 1); assert.equal(ps[0].glued, true); assert.equal(ps[0].side, null); assert.equal(Core.spread(ps).splitGroups.length, 0); ok('A10d an old one-piece record of a pair is not split');
  // 10e · necklaces, letters, a single earring: no side, no mirror, as before
  for (const form of ['necklace', 'charm', 'earring-single', '']) {
    ps = Core.resolve({ orderId: rid, lines: [line(rid, tx, 'DISC_X', 3, { form })], pools: [], sheets: [], sheetsKnown: true, pairOf: real, kindOf: kindReal });
    assert.equal(ps.length, 3, form); assert.ok(ps.every(p => p.side === null && p.mirror === false && p.kind === null), form);
  }
  ok('A10e discs, letters, charms and a single earring have no side and no mirror');
}

/* ── B · the real order window (needs playwright; skipped without) ── */
async function partB() {
  const pwDir = process.env.PW_DIR || path.join(root, 'node_modules');
  let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { console.log('  – no playwright-core: the order window checks were not run'); return; }
  const { start } = require('./bridge-server.cjs');
  const srv = await start({ receipts: [] });
  const DAY = 86400, SHIP = Math.floor(Date.UTC(2026, 9, 12, 17) / 1000);
  const S = { gf1: 'sheet-pow-gf1', gf2: 'sheet-pow-gf2' }, SET = 'set-pow-1';
  const P = { rid: '4200000010', tx: '9101' }, Q = { rid: '4200000011', tx: '9102' }, R = { rid: '4200000012', tx: '9103', tx2: '9104' }, M = { rid: '4200000013', tx: '9105' }, E = { rid: '4200000014', tx: '9106' };
  const id = (o, tx, c) => `${o.rid}_${tx}_${c}`;
  const sheetDocs = () => ({
    [S.gf1]: { id: S.gf1, metal: 'gold', sheetIndex: 1, setId: SET, setSeq: 1, folder: '2026-10-09_GF_Set-1_Sheet-1', fileBase: '2026-10-09_GF_Set-1_Sheet-1', day: '2026-10-09', status: 'written', stock: { wPt: 300, hPt: 150 }, orders: [P.rid, Q.rid, R.rid, M.rid, E.rid],
      poolIds: [id(P, P.tx, 1), id(Q, Q.tx, 1), id(Q, Q.tx, 2), id(R, R.tx, 1), id(M, M.tx, 1), id(E, E.tx, 1)] },
    [S.gf2]: { id: S.gf2, metal: 'gold', sheetIndex: 2, setId: SET, setSeq: 1, folder: '2026-10-09_GF_Set-1_Sheet-2', fileBase: '2026-10-09_GF_Set-1_Sheet-2', day: '2026-10-09', status: 'written', stock: { wPt: 300, hPt: 150 }, orders: [P.rid, M.rid, E.rid],
      poolIds: [id(P, P.tx, 2), id(M, M.tx, 2), id(E, E.tx, 2)] }
  });
  const prow = (o, tx, copy, sku, side, sheetId, extra) => Object.assign({ poolId: id(o, tx, copy), orderId: o.rid, transactionId: tx, lineKey: `${o.rid}_${tx}`, sku, material: 'gold', copy, state: sheetId ? 'written' : 'ready', sheetId: sheetId || null, setId: sheetId ? SET : null, updatedAt: Date.now() },
    side ? { side, bodyIndex: side === 'L' ? 0 : 1, groupKey: `${o.rid}:${tx}`, groupSize: 2 } : {}, extra || {});
  const pools = () => [
    prow(P, P.tx, 1, 'MISMATCHED_7134', 'L', S.gf1), prow(P, P.tx, 2, 'MISMATCHED_7134', 'R', S.gf2),
    prow(Q, Q.tx, 1, 'MISMATCHED_7134', 'L', S.gf1), prow(Q, Q.tx, 2, 'MISMATCHED_7134', 'R', S.gf1),
    prow(R, R.tx, 1, 'MISMATCHED_7134', 'L', S.gf1), prow(R, R.tx, 2, 'MISMATCHED_7134', 'R', null), prow(R, R.tx2, 1, 'FEMALE_SYMBOL', null, null),
    prow(M, M.tx, 1, 'FEMALE_SYMBOL', null, S.gf1), prow(M, M.tx, 2, 'FEMALE_SYMBOL', null, S.gf2),
    prow(E, E.tx, 1, 'HEART_STUD', 'L', S.gf1, { bodyIndex: 0, mirror: false }), prow(E, E.tx, 2, 'HEART_STUD', 'R', S.gf2, { bodyIndex: 0, mirror: true })
  ];
  const orderOf = (o, buyer, lines) => ({ receiptId: o.rid, orderNumber: o.rid, createTs: SHIP - 5 * DAY, updateTs: SHIP - 5 * DAY + 60, shipBy: SHIP, buyer: { name: buyer }, buyerMessage: '', isGift: false, giftMessage: '', staffNote: '', messages: [],
    lines: lines.map(([tx, sku, qty]) => ({ transactionId: tx, listingId: '19000' + tx.slice(-5), sku, title: sku === 'MISMATCHED_7134' ? 'Mismatched mittens earrings' : sku === 'HEART_STUD' ? 'Heart stud earrings' : 'Female symbol charm', quantity: qty || 1, expectedShipDate: SHIP, variations: [{ name: 'Metal', value: '14k Gold Filled' }].concat(sku === 'HEART_STUD' || sku === 'MISMATCHED_7134' ? [{ name: 'Style', value: 'Earrings' }] : []), metalKey: 'gold', metalLabel: '14k Gold Filled' })) });
  const orders = [
    [orderOf(P, 'Split Pair', [[P.tx, 'MISMATCHED_7134']]), [[id(P, P.tx, 1), id(P, P.tx, 2)]]],
    [orderOf(Q, 'Together Pair', [[Q.tx, 'MISMATCHED_7134']]), [[id(Q, Q.tx, 1), id(Q, Q.tx, 2)]]],
    [orderOf(R, 'Pair Plus One', [[R.tx, 'MISMATCHED_7134'], [R.tx2, 'FEMALE_SYMBOL']]), [[id(R, R.tx, 1), id(R, R.tx, 2)], [id(R, R.tx2, 1)]]],
    [orderOf(M, 'Matching Two', [[M.tx, 'FEMALE_SYMBOL', 2]]), [[id(M, M.tx, 1), id(M, M.tx, 2)]]],
    [orderOf(E, 'Studs Pair', [[E.tx, 'HEART_STUD']]), [[id(E, E.tx, 1), id(E, E.tx, 2)]]]
  ];
  const seed = () => {
    srv.st.docs.clear(); let n = 0;
    for (const sh of Object.values(sheetDocs())) {
      const placements = [], charms = [];
      sh.poolIds.forEach((pid, i) => { const cid = 'c' + (++n); placements.push({ id: cid, cxPt: 30 + i * 34, cyPt: 30, angle: 0, wPt: 30, hPt: 30 }); charms.push({ id: cid, name: pid, poolId: pid, order: pid.split('_')[0], sku: 'X' }); });
      srv.st.put('Charm_Nest_Sheets', sh.id, Object.assign({}, sh, { placements, charms, updatedAt: Date.now() }));
    }
    for (const p of pools()) srv.st.put('Charm_Pool', p.poolId, p);
  };
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  const errors = [];
  try {
    seed();
    const context = await browser.newContext({ viewport: { width: 1440, height: 900 } });
    await context.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => /fonts\.googleapis|fonts\.gstatic/.test(r.request().url()) ? r.fulfill({ status: 200, contentType: 'text/css', body: '' }) : r.abort());
    await context.addInitScript(() => { try { if (!sessionStorage.getItem('__seeded')) { localStorage.setItem('cn.settings', JSON.stringify({ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on', sandbox: 'off' })); localStorage.setItem('cn.employee', 'Test Operator'); sessionStorage.setItem('__seeded', '1'); } } catch (_) {} window.prompt = () => 'Test Operator'; });
    const page = await context.newPage(); page.setDefaultTimeout(25000);
    page.on('pageerror', e => { errors.push(e.message); console.error('page error:', String(e.stack || e.message).split('\n').slice(0, 4).join(' | ')); });
    await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
    await page.waitForFunction(() => window.CN && window.Orders && window.OrderWin && window.OrderPieces && window.CharmNestPair && CN.S.cloud.ok === true, null, { timeout: 60000 });
    await page.evaluate(async ({ orders }) => {
      // (the master record of the mismatched design says it draws two different bodies: the page's own test of "a mismatched pair")
      const orig = Master.entryFor.bind(Master); Master.entryFor = sku => String(sku) === 'MISMATCHED_7134' ? { sku, pair: { v: 1, bodies: 2, mismatched: true }, updatedAt: 1 } : orig(sku);
      await Orders.loadMaps(true);
      for (const [order, pools] of orders) order.lines.forEach((line, i) => { const key = CharmNestOrders.lineKey(order, line); const row = { key, order, line, arrivedAt: Date.now(), spec: null, problems: [], state: pools[i] && pools[i].length ? 'pooled' : 'pulled', reason: null, claimedBy: null, poolIds: pools[i] || [], engrave: null, material: null }; B.orders.rows.push(row); B.orders.byKey.set(key, row); });
      Orders.interpretAll(); CN.setMode('orders'); Orders.render();
    }, { orders });
    const open = async (o, tx) => {
      if (await page.evaluate(() => document.getElementById('orderWin').open)) { await page.evaluate(() => OrderWin.close()); await page.waitForFunction(() => !document.getElementById('orderWin').open); }
      await page.evaluate(k => OrderWin.open(k, { view: 'info' }), `${o.rid}_${tx}`);
      await page.waitForFunction(r => OrderPieces.known(r), o.rid, { timeout: 20000 }).catch(() => {});
      await page.waitForTimeout(1200);
      return page.evaluate(() => {
        const txt = n => n ? n.textContent.replace(/\s+/g, ' ').trim() : '';
        return { rows: [...document.querySelectorAll('#owPcSum .owPcRow')].map(r => ({ side: r.dataset.side || '', piece: r.dataset.piece || '', tag: txt(r.querySelector('.owPcSide')), st: txt(r.querySelector('.pcSt span')) || txt(r.querySelector('.owPcSr')), dots: r.querySelectorAll('.steps i').length, thumb: !!r.querySelector('.owSidePic'), mirror: !!r.querySelector('.owSidePic[data-pc-mirror]'), name: txt(r.querySelector('.owPcName')) })),
          head: txt(document.getElementById('owPcSum')?.querySelector('.owPcHd')), sub: txt(document.getElementById('owSub')), sku: txt(document.getElementById('owSku')), meta: txt(document.getElementById('owMeta')), split: txt(document.querySelector('#owNowCard .owSplit')), chips: [...document.querySelectorAll('#owNowCard .owShChip')].map(txt),
          caption: [...document.querySelectorAll('#owPhoto, .owPics figcaption')].map(txt).join('|') };
      });
    };
    // 1 · a pair, Left on GF Sheet 1 and Right on GF Sheet 2
    let w = await open(P, P.tx);
    if (process.env.POW_SHOT) await page.locator('#orderWin').screenshot({ path: process.env.POW_SHOT });
    const sides = w.rows.filter(r => r.side);
    assert.deepEqual(sides.map(r => r.tag), ['Left', 'Right'], JSON.stringify(w)); assert.deepEqual(sides.map(r => r.st), ['GF Sheet 1', 'GF Sheet 2'], JSON.stringify(w));
    assert.ok(sides.every(r => r.dots >= 4 && r.thumb), 'each side has its own dots and its own picture box');
    assert.equal(w.head, 'Its pieces'); assert.match(w.sub, /2 pieces/); assert.match(w.sku, /Left \+ Right/); assert.match(w.meta, /Pair.*Mismatched/); assert.match(w.meta, /Left GF Sheet 1 · Right GF Sheet 2/);
    assert.match(w.split, /Its pair is split: Left on GF Sheet 1, Right on GF Sheet 2/); ok('B1 a split pair: a row for Left and one for Right, each with its own sheet, dots and picture; the card says the pair is split');
    assert.deepEqual(w.chips.filter(c => /Left|Right/.test(c)).length, 2, 'a box for each side'); ok('B1b the Where-it-is-now boxes name the sides');
    // 2 · both on one sheet
    w = await open(Q, Q.tx);
    assert.deepEqual(w.rows.filter(r => r.side).map(r => [r.tag, r.st]), [['Left', 'GF Sheet 1'], ['Right', 'GF Sheet 1']], JSON.stringify(w)); assert.equal(w.split, ''); assert.match(w.chips.join('|'), /Left \+ Right/); ok('B2 a pair on one sheet: both rows say GF Sheet 1, nothing says it is split, one box for the pair');
    // 3 · a pair plus another line, the Right not placed yet
    w = await open(R, R.tx);
    assert.deepEqual(w.rows.filter(r => r.side).map(r => [r.tag, r.st]), [['Left', 'GF Sheet 1'], ['Right', 'Waiting for a sheet']], JSON.stringify(w));
    assert.equal(w.rows.filter(r => !r.side).length, 1); assert.ok(w.rows.filter(r => r.side).every(r => r.piece === `${R.rid}_${R.tx}`), 'both side rows pick their line');
    assert.match(w.split, /Left on GF Sheet 1, Right not on a sheet yet/); ok('B3 a pair and another line: the Right is waiting, the Left is on its sheet, the other line has its own row');
    assert.equal(await page.evaluate(() => (document.querySelector('#owPcSum .owPcState') || {}).textContent), 'Showing all 3 pieces', 'the pair counts as two pieces beside the other line'); ok('B3a the quiet line counts pieces: a pair (2) and another line (1) are 3');
    // a press on the Right row picks the line, a second press shows all pieces again
    await page.evaluate(() => document.querySelector('#owPcSum .owPcRow[data-side="R"] .owPcName').click()); await page.waitForTimeout(300);
    assert.equal(await page.evaluate(() => OrderWin.selectedPiece()), `${R.rid}_${R.tx}`); ok('B3b a press on a side row picks its line');
    // 4 · a matching quantity-2 line stays as it was: one row, no sides, no split sentence
    w = await open(M, M.tx);
    assert.equal(w.rows.filter(r => r.side).length, 0); assert.ok(!/Left|Right/.test(w.sku + w.meta + w.sub), JSON.stringify(w)); assert.equal(w.split, ''); ok('B4 a matching line (quantity 2, two sheets) is drawn exactly as before: no side rows, no pair words');
    // 5 · a MATCHING pair of stud earrings (Paul 9 Oct 18:47: every earring pair is a Left and a Right, the Right the Left turned over): the same rows, each piece with its own direction
    w = await open(E, E.tx);
    const es = w.rows.filter(r => r.side);
    assert.deepEqual(es.map(r => [r.tag, r.st]), [['Left', 'GF Sheet 1'], ['Right', 'GF Sheet 2']], JSON.stringify(w));
    assert.deepEqual(es.map(r => r.mirror), [false, true], 'the Right picture is the one turned over');
    assert.match(w.sub, /2 pieces/); assert.match(w.sku, /Left \+ Right/); assert.match(w.meta, /Pair.*Matching/); assert.ok(!/Mismatched/.test(w.meta));
    assert.match(w.split, /Its pair is split: Left on GF Sheet 1, Right on GF Sheet 2/); ok('B5 a matching earring pair: a Left and a Right row, the Right drawn turned over, the pair said to be matching');
    // 6 · the run banner counts PIECES: a pair is two (PAIRFLOW's hand-over), a charm or a disc line as many as it always counted
    const ban = await page.evaluate(() => ({ pull: RunCtl.stepDetail({ step: 'pull' }), pool: RunCtl.stepDetail({ step: 'pool' }), lines: Orders.rows().filter(x => x.state !== 'gone').length, per: Orders.rows().map(x => [x.key, x.state, RunCtl.bannerPieces(x), x.spec && x.spec.form]) }));
    assert.equal(ban.lines, 6, JSON.stringify(ban)); assert.deepEqual(ban.per.map(x => x[3]), ['earrings', 'earrings', 'earrings', null, null, 'earrings'], JSON.stringify(ban)); assert.equal(ban.pull, ' \u00b7 10 pieces', JSON.stringify(ban)); assert.equal(ban.pool, ' \u00b7 10 of 10 pieces', JSON.stringify(ban)); ok('B6 the run banner counts pieces: four pair lines and two other lines make 10 pieces, not 6');
    // 7 · a seal written for the pair LINE (ONE welded or printed event: "Left + Right", PAIRLABELS item 43) shows on both rows; an event for one piece only on its own row
    const tl = await page.evaluate(({ rid, tx }) => {
      const line = `${rid}_${tx}`, mk = (side, pid) => ({ key: `${line}#${side}`, lineKey: line, side, tid: tx, qty: 1, pools: [pid], sheets: [], line: {}, name: 'X' });
      const L = mk('L', `${line}_1`), R = mk('R', `${line}_2`), all = [L, R], f = UI => ev => [UI.ofPiece(ev, L, all), UI.ofPiece(ev, R, all)];
      const U = OrderTimelineUI, one = f(U);
      return { lineEv: one({ type: 'welded', lineKey: line, transactionId: tx, at: 1 }), bothIds: one({ type: 'welded', data: { poolIds: [`${line}_1`, `${line}_2`] }, lineKey: line, at: 1 }), leftOnly: one({ type: 'qr-printed', data: { poolId: `${line}_1` }, lineKey: line, at: 1 }), rightOnly: one({ type: 'qr-printed', data: { poolId: `${line}_2` }, lineKey: line, at: 1 }) };
    }, { rid: E.rid, tx: E.tx });
    assert.deepEqual(tl.lineEv, [true, true], JSON.stringify(tl)); assert.deepEqual(tl.bothIds, [true, true]); assert.deepEqual(tl.leftOnly, [true, false]); assert.deepEqual(tl.rightOnly, [false, true]); ok('B7 one seal for the pair line shows on the Left and the Right row; a seal for one piece only on its own');
    await context.close();
  } finally { await browser.close(); srv.close(); }
  assert.deepEqual(errors, [], 'no page errors: ' + errors.join(' | '));
}

(async () => {
  await partB();
  console.log(`\n${ran} checks passed`);
})().catch(e => { console.error(e); process.exit(1); });
