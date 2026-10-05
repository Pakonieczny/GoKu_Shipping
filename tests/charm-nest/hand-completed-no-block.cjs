/* Paul's round 6, point 1 (5 Oct 2026), as a regression through the real code:
 *
 *   "Even though I completed this order by hand, which is part of a three-piece order, this chain only piece obviously does not go on any
 *    sheet so I approve that by pressing the complete button and the review panel, but it's still blocking this sheet from being approved
 *    to laser cutting process."
 *
 * Order 4170837249 has three pieces: CABLE CHAIN ONLY (Rose Gold, an unknown SKU: a custom order completed by hand in Review), MIDDLE 9935 on
 * GF Sheet 1 and MIDDLE 9935 on RG Sheet 1 (both sheets ready in every other way, in one set). The run's own copy of the chain's line is
 * stale, as it really is (a run saves its lines when it pools and nests, not when a card is completed: it still says 'unmatched'), and the
 * custom order's own record (Charm_Custom_Orders) says completed.
 *
 * A piece completed by hand (Review "Complete Order", or its QR label printed) is RESOLVED: it needs no sheet, blocks nothing, is nobody's
 * set mate and is never cut. REOPEN blocks again; a HELD piece still holds; a cancelled order is unchanged; a custom order sent to the sheets
 * (how 'sheet') is cut, not completed by hand. Every layer must say the same, for every variant:
 *   · the page: CharmNestReadiness (orderReports, issues = the Library's '!' panel, explain = the Order check step, laserSheet, set, laserGroup), OrderPieces (the order window's pieces), CharmNestOrders.evaluateOrder (the sorter's set release gate), SharedOrders
 *     (a hand piece is never a partner), interpretLine (never pooled);
 *   · the server's twin, the real charmNestLibrary handler over the in-memory Firestore: laserStatus (what the Library reads), setUpdate
 *     (completing the set), flowApply seal (the Approve press), and the cheap "unchanged?" probe, which must SEE the completion and the reopen.
 * The server reads its own saved custom records: the run's copy may carry a hint the page wrote (handDone) and is never trusted.
 * Read cost is bounded and printed: one small field-masked read per line on no sheet, 200 at most, batched by 100.
 *
 *   node tests/charm-nest/hand-completed-no-block.cjs
 * Never touches a live service: bridge-server.cjs is an in-memory fake. */
'use strict';
const assert = require('node:assert/strict');
const path = require('path');
const root = path.join(__dirname, '../..');
const R = require(path.join(root, 'charm-nest-readiness.js'));
const OP = require(path.join(root, 'charm-nest-order-pieces.js'));
const CO = require(path.join(root, 'charm-nest-orders.js'));
const SO = require(path.join(root, 'charm-nest-shared-orders.js')).core;
const S = require('./issues-shop.cjs');
const P = require('./issues-property.cjs');
const { seed } = require('./issues-server.cjs');
const { start } = require('./bridge-server.cjs');

const ORDER = '4170837249', SET = 'set-1', GF = 'gf-sheet-1', RG = 'rg-sheet-1';
const CHAIN = `${ORDER}_1001`;
let checks = 0;
const ok = (c, m) => { assert.ok(c, m); checks++; };
const eq = (a, b, m) => { assert.deepEqual(a, b, m); checks++; };

/** Paul's order as a shop spec. hand: the custom order's record (how, state, and how the run's copy of the line still reads) or null. */
const hand = (how = 'button', state = 'completed', rec = 'stale') => ({ how, state, rec });
function paul(h, chain = {}, o = {}) {
  const line = (tx, metal, more) => ({ tx, metal, q: 1, kind: 'paul', copies: [], state: 'written', problems: [], sku: 'MIDDLE-9935', hold: null, change: false, engrave: 'plain', noDesign: false, stale: false, ...more });
  const chainLine = line(1001, 'rose', { copies: [{ sheet: null, pooled: false }], state: 'unmatched', problems: ['unmatchedSku'], sku: '', ...(h ? { hand: h } : {}), ...chain });
  if (h && h.rec === 'hinted') { chainLine.state = 'noDesign'; chainLine.problems = []; }
  if (h && h.rec === 'legacy') { chainLine.state = 'noDesign'; chainLine.problems = []; chainLine.noDesign = true; }
  if (h && h.state === 'open' && h.rec === 'hinted') { chainLine.state = 'noDesign'; chainLine.problems = []; }
  return {
    seed: 1, sandbox: false, archivedSheets: [],
    sheets: [{ id: GF, metal: 'gold', index: 1, own: 'ok', setId: SET }, { id: RG, metal: 'rose', index: 1, own: 'ok', setId: SET }],
    orders: [{ id: ORDER, buyer: 'Buyer', lines: [chainLine, line(1002, 'gold', { copies: [{ sheet: GF, pooled: true }] }), line(1003, 'rose', { copies: [{ sheet: RG, pooled: true }] })] }],
    ...o
  };
}

/** The variants: what each layer must say. blocked: the order is an issue for both sheets, with the key that names it. */
const VARIANTS = [
  { name: 'completed with the Complete button (the run still says unmatched)', spec: paul(hand('button')), blocked: false },
  { name: 'completed by printing its QR label', spec: paul(hand('print')), blocked: false },
  { name: 'completed, the run copy carries the page hint (handDone)', spec: paul(hand('button', 'completed', 'hinted')), blocked: false },
  { name: 'completed, the run copy flagged noDesign (an older save)', spec: paul(hand('button', 'completed', 'legacy')), blocked: false },
  { name: 'never completed (the bug as Paul had it, without the completion)', spec: paul(null), blocked: 'noSku' },
  { name: 'completed, then REOPENED', spec: paul(hand('button', 'open', 'stale')), blocked: 'noSku' },
  { name: 'REOPENED while the run copy still carries the page hint', spec: paul(hand('button', 'open', 'hinted')), blocked: 'pooled' },
  { name: 'sent to the sheets with its own designs (how sheet), not completed by hand', spec: paul(hand('sheet', 'completed', 'stale')), blocked: 'noSku' },
  { name: 'completed by hand but HELD (a person or Etsy stops it)', spec: paul(hand('button'), { hold: 'Customer changed the order', state: 'held' }), blocked: 'held' },
  { name: 'completed by hand, held by an Etsy change waiting for review', spec: paul(hand('print'), { change: true, state: 'held' }), blocked: 'held' },
  { name: 'cancelled order (unchanged: never waited for)', spec: paul(hand('button'), { state: 'gone' }), blocked: false, cancelled: true }
];

/* ── the page ───────────────────────────────────────────────────────────────────────────────────────────────── */
function pageChecks(v) {
  const shop = S.materialize(v.spec), rows = S.uiRows(shop), sheets = P.pageSheets(shop), pre = P.serverLike(shop), chain = rows.find(r => r.key === CHAIN);
  const tag = v.name;
  // 1 · the readiness of the order, as the page reads it from its own rows (orderReports) and as the Library's '!' panel reads each sheet (issues)
  const rep = R.orderReports(rows, sheets)[ORDER];
  if (v.blocked) { ok(rep && rep.ready === false && rep.key === v.blocked, `${tag}: the order blocks with ${v.blocked}, got ${JSON.stringify(rep && [rep.ready, rep.key])}`); eq(rep.blocks.map(b => b.lineKey), [CHAIN], `${tag}: it is the chain that blocks`); }
  else eq(rep, { ready: true }, `${tag}: nothing blocks the order`);
  for (const id of [GF, RG]) {
    const sh = sheets.find(s => s.id === id), list = R.issues(sh, { rows, allSheets: sheets }).filter(i => i.step === 'orders'), recs = pre.find(s => s.id === id);
    eq(list.length, v.blocked ? 1 : 0, `${tag}: ${id}: the '!' panel lists ${v.blocked ? 'the order' : 'no order issue'}`);
    if (v.blocked) { eq(list[0].key, v.blocked, `${tag}: ${id}: its reason`); eq(list[0].pieces.map(p => p.poolId), [`${CHAIN}_1`], `${tag}: ${id}: the piece named is the chain`); }
    // the server's way (the record answered with its orderReadiness): the same '!' list, the Order check step and the laser gate
    eq(R.issues(recs, {}).filter(i => i.step === 'orders').length, v.blocked ? 1 : 0, `${tag}: ${id}: the answered record lists the same`);
    const step = R.explain(recs, { rows }).steps.find(s => s.key === 'orders');
    eq(step.state === 'done', !v.blocked, `${tag}: ${id}: the Order check step is ${v.blocked ? 'not ' : ''}done (${step.state}: ${step.detail})`);
    eq(R.laserSheet(recs).ready, !v.blocked, `${tag}: ${id}: laserSheet.ready`);
  }
  // 2 · the set and its Approve buttons (all green, or all grey)
  const set = { setId: SET, sheetIds: [GF, RG] };
  eq(R.set(set, pre).ready, !v.blocked, `${tag}: the set's readiness`);
  // (setGate, the Approve buttons' own hard test, is about back engravings and layout; an order wait is the plan's need, below, and the seal's)
  ok(R.setGate(set, pre).ready, `${tag}: the sheets are fine in every other way, so the Approve buttons' own test passes`);
  eq(R.laserGroup(set, pre).ready, !v.blocked, `${tag}: laserGroup`);
  // 3 · OrderPieces: the order window's pieces. The chain is resolved ('hand'), never "not on a sheet yet", unless it is held
  const lines = rows.map(r => ({ key: r.key, transactionId: r.line.transactionId, sku: r.line.sku || '', title: '', material: r.metal, quantity: r.spec.quantity, state: r.state, poolIds: (r.poolIds || []).slice(), hold: r.hold || null, changePending: !!r.changePending,
    problems: (r.problems || []).map(p => ({ kind: p.kind })), spec: { noDesign: r.spec.noDesign, customDone: r.spec.customDone || null }, reason: r.reason || '' }));
  const pcs = OP.resolve({ orderId: ORDER, lines, pools: shop.pool, sheets: shop.sheets, sheetsKnown: true }), piece = pcs.find(p => p.lineKey === CHAIN);
  if (!v.cancelled) {
    const resolved = !v.blocked;
    eq(!!piece.hand, resolved, `${tag}: OrderPieces says the chain is ${resolved ? '' : 'not '}completed by hand`);
    if (resolved) { ok(!piece.loading && /completed by hand/.test(piece.reason) && !/not on a sheet/.test(piece.reason) && piece.problem == null, `${tag}: it is not "not on a sheet yet": ${piece.reason}`); eq(piece.index > 2, true, `${tag}: it is numbered after the pieces that are cut`); }
    else ok(!piece.hand && !/completed by hand/.test(piece.reason), `${tag}: the blocking piece is not called completed: ${piece.reason}`);
  }
  // 4 · the sorter's set release gate
  const gate = CO.evaluateOrder(rows);
  if (!v.cancelled) eq(gate.committable, !v.blocked, `${tag}: evaluateOrder (the set release gate): ${JSON.stringify(gate.held)}`);
  // 5 · SharedOrders: the pieces are the sheets' own, so the chain is never a partner; the two sheets share the order through their middles
  const sh = shop.sheets.map(s => SO.sheetOf(s));
  eq(SO.groups(sh).map(g => g.ids.slice().sort()), [[GF, RG]], `${tag}: GF Sheet 1 and RG Sheet 1 are partners through their own pieces`);
  ok(!sh.some(s => s.pieces.some(p => p.key.startsWith(CHAIN))), `${tag}: the chain is on no sheet and no sheet's piece`);
  eq(SO.groups([SO.sheetOf(shop.sheets.find(s => s.id === GF))]).length, 0, `${tag}: with the chain, one sheet shares nothing`);
}

/** interpretLine: a line completed by hand is never pooled (makePool starts with `if (!sp || sp.noDesign) { row.state = "noDesign"; return null; }`). */
function interpretChecks() {
  const order = { receiptId: ORDER, orderNumber: ORDER, createTs: 1, updateTs: 1, shipBy: 2, buyer: { name: 'Buyer' }, lines: [] };
  const line = { transactionId: '1001', listingId: '1800000001', sku: '', title: 'CABLE CHAIN ONLY', quantity: 1, variations: [{ name: 'Metal', value: '14k Rose Gold Filled' }], metalKey: 'rose' };
  order.lines = [line];
  const ctx = extra => ({ optionMaps: {}, aliases: {}, noDesign: { patterns: [], skus: [] }, masterEntry: () => null, ...extra });
  const key = CO.lineKey(order, line);
  const done = CO.interpretLine(order, line, ctx({ customDone: { [key]: { state: 'completed', how: 'button' } } }));
  ok(done.noDesign === true && done.customDone && done.customDone.how === 'button' && !done.problems.length, 'a line completed by hand has nothing to cut, so it is never pooled or placed afterwards');
  // reopened: the page drops it from customDone (customKept), so the line is read afresh and is a piece again
  const again = CO.interpretLine(order, line, ctx({}));
  ok(!again.customDone && again.problems.length > 0, 'reopened, the line is read afresh: a piece with its question again');
}

/* ── the server ─────────────────────────────────────────────────────────────────────────────────────────────── */
async function serverChecks(srv, v) {
  const tag = v.name, shop = S.materialize(v.spec), post = body => fetch(srv.sorterOrigin + '/.netlify/functions/charmNestLibrary', { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(body) }).then(async r => ({ status: r.status, ...(await r.json()) }));
  const read = async (extra = {}) => { srv.st.reads = 0; srv.st.queries = 0; const a = await post({ op: 'laserStatus', sheetIds: [GF, RG], ...extra }); return { ...a, reads: srv.st.reads, queries: srv.st.queries }; };
  seed(srv.st, shop, false);
  const a = await read({ wantRevs: true });
  eq(a.status, 200, `${tag}: laserStatus answers`);
  for (const id of [GF, RG]) {
    const s = a.sheets.find(x => x.id === id), rd = s.orderReadiness[ORDER];
    if (v.blocked) { ok(rd.ready === false && rd.key === v.blocked && rd.blocks.length === 1 && rd.blocks[0].lineKey === CHAIN, `${tag}: ${id}: the server blocks the order with ${v.blocked}: ${JSON.stringify(rd).slice(0, 160)}`); eq(s.laser.ready, false, `${tag}: ${id}: not laser-ready`); }
    else { eq(rd, { ready: true }, `${tag}: ${id}: the server says the order is whole`); eq(s.laser.ready, true, `${tag}: ${id}: laser-ready`); }
  }
  // the server's answer is the page's answer (one truth): the Library's '!' list from the answered records
  for (const id of [GF, RG]) eq(R.issues(a.sheets.find(x => x.id === id), {}).filter(i => i.step === 'orders').length, v.blocked ? 1 : 0, `${tag}: ${id}: the Library's '!' list from the server's answer`);
  // setUpdate: recording the set complete is refused with the order not whole, allowed with it whole
  seed(srv.st, shop, false);
  const logged = console.error; console.error = () => {};   // (a refusal is logged by the handler: expected here)
  let done; try { done = await post({ op: 'setUpdate', setId: SET, patch: { status: 'complete', sheetIds: [GF, RG] } }); } finally { console.error = logged; }
  if (v.blocked) ok(done.error || done.status >= 400, `${tag}: the set cannot be recorded complete`); else ok(done.ok === true && srv.st.doc('Charm_Nest_Sets', SET).status === 'complete', `${tag}: the set is recorded complete: ${JSON.stringify(done)}`);
  // the Approve press (flowApply seal): the readiness seal is recorded only for a set that is ready
  seed(srv.st, shop, false);
  const seal = await post({ op: 'flowApply', by: 'Tester', steps: [{ type: 'seal', kind: 'set', id: SET }] });
  eq(seal.status, 200, `${tag}: flowApply answers`);
  eq((seal.added || []).length, v.blocked ? 0 : 3, `${tag}: the Approve press ${v.blocked ? 'records nothing' : 'records the readiness seals (GF Sheet 1, RG Sheet 1 and their set)'}`);
  return a;
}

/** The cheap "unchanged?" read sees a completion and a reopen at once (it watches the custom order's record), and no other write. */
async function probeChecks(srv) {
  const post = body => fetch(srv.sorterOrigin + '/.netlify/functions/charmNestLibrary', { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(body) }).then(r => r.json());
  const key = CHAIN, shop = S.materialize(paul(hand('button')));
  seed(srv.st, shop, false);
  // completed -> reopened
  const first = await post({ op: 'laserStatus', sheetIds: [GF, RG], wantRevs: true });
  ok(first.revs && Object.prototype.hasOwnProperty.call(first.revs, 'c:' + key), 'the answer is made from the chain\'s custom order record: it is watched');
  eq(Object.keys(first.revs).filter(k => k[0] === 'c'), ['c:' + key], 'and only that one record (the middles are on sheets: nothing to look up)');
  eq(first.sheets.every(s => s.orderReadiness[ORDER].ready === true), true, 'completed: whole');
  const same = await post({ op: 'laserStatus', sheetIds: [GF, RG], ifRevs: first.revs });
  eq(same.unchanged, true, 'nothing changed: answered unchanged, from the watched documents alone');
  srv.st.put('Charm_Custom_Orders', key, { state: 'open', reopenedBy: 'Seth', reopenedAt: Date.now() });
  const after = await post({ op: 'laserStatus', sheetIds: [GF, RG], ifRevs: first.revs, wantRevs: true });
  ok(!after.unchanged && after.sheets, 'a Reopen is a change: the full answer is made again');
  ok(after.sheets.every(s => s.orderReadiness[ORDER].ready === false && s.orderReadiness[ORDER].key === 'noSku'), 'and the chain blocks both sheets again');
  // reopened -> completed again (a record that only becomes completed later)
  const same2 = await post({ op: 'laserStatus', sheetIds: [GF, RG], ifRevs: after.revs });
  eq(same2.unchanged, true, 'the reopened state is unchanged until something is written');
  srv.st.put('Charm_Custom_Orders', key, { state: 'completed', how: 'print', completedAt: Date.now(), completedBy: 'Seth' });
  const back = await post({ op: 'laserStatus', sheetIds: [GF, RG], ifRevs: after.revs });
  ok(!back.unchanged && back.sheets.every(s => s.orderReadiness[ORDER].ready === true), 'completed again: seen at once, both sheets whole');
  // no record at all yet, then Complete: the record that did not exist is watched too
  seed(srv.st, S.materialize(paul(null)), false);
  const none = await post({ op: 'laserStatus', sheetIds: [GF, RG], wantRevs: true });
  ok(none.revs['c:' + key] === '0' && none.sheets.every(s => s.orderReadiness[ORDER].ready === false), 'no record yet: blocked, and the absent record is watched');
  srv.st.put('Charm_Custom_Orders', key, { state: 'completed', how: 'button', completedAt: Date.now(), completedBy: 'Seth' });
  const now = await post({ op: 'laserStatus', sheetIds: [GF, RG], ifRevs: none.revs });
  ok(!now.unchanged && now.sheets.every(s => s.orderReadiness[ORDER].ready === true), 'Complete Order pressed: the next read sees it');
}

/** The server never trusts the page: a hint in the run's copy of the line (handDone) without a completed record is no completion, and a record that is
 *  not read (a line already on a sheet) is not looked up. */
async function trustChecks(srv) {
  const post = body => fetch(srv.sorterOrigin + '/.netlify/functions/charmNestLibrary', { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(body) }).then(r => r.json());
  // a hint with no record at all
  const shop = S.materialize(paul(hand('button', 'completed', 'hinted')));
  seed(srv.st, shop, false); srv.st.docs.delete('Charm_Custom_Orders/' + CHAIN);
  let a = await post({ op: 'laserStatus', sheetIds: [GF, RG] });
  ok(a.sheets.every(s => s.orderReadiness[ORDER].ready === false), 'a handDone hint the custom order record does not back is no completion');
  // a hint with a record of another line only
  srv.st.put('Charm_Custom_Orders', `${ORDER}_9999`, { state: 'completed', how: 'button' });
  a = await post({ op: 'laserStatus', sheetIds: [GF, RG] });
  ok(a.sheets.every(s => s.orderReadiness[ORDER].ready === false), 'another line\'s completion does not complete this one');
  // the record unreadable: the order is "not verified", never read as whole
  seed(srv.st, S.materialize(paul(hand('button'))), false);
  const docs = srv.st.docs, warn = console.warn;
  docs.get = k => { if (String(k).startsWith('Charm_Custom_Orders/')) throw new Error('forced: the custom order records cannot be read'); return Map.prototype.get.call(docs, k); };
  console.warn = () => {};
  try { a = await post({ op: 'laserStatus', sheetIds: [GF, RG] }); } finally { delete docs.get; console.warn = warn; }
  ok(a.sheets.every(s => s.orderReadiness[ORDER].ready === false && /not been verified/i.test(s.orderReadiness[ORDER].why)), 'a custom order record that cannot be read leaves the order "not verified": never read as whole, never as a false block of a piece');
}

/** Read cost: one small field-masked read per line on no sheet, and 200 at most. */
async function costChecks(srv) {
  process.env.CHARM_NEST_HOLDERS_MS = '0';   // (no minute's memory of which runs hold an order: every read pays in full, so the counts are exact)
  const post = body => fetch(srv.sorterOrigin + '/.netlify/functions/charmNestLibrary', { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(body) }).then(async r => ({ status: r.status, ...(await r.json()) }));
  // the documents read, by collection (the fake counts every document a handler reads through st.docs.get)
  const counted = async fn => {
    const docs = srv.st.docs, seen = {}, from = srv.st.reads || 0; srv.st.reads = 0;
    docs.get = k => { const c = String(k).split('/')[0]; seen[c] = (seen[c] || 0) + 1; return Map.prototype.get.call(docs, k); };
    try { const out = await fn(); return { out, seen: { ...seen }, reads: srv.st.reads }; } finally { delete docs.get; srv.st.reads = from; }
  };
  const status = async (shop, ids) => { seed(srv.st, shop, false); return counted(() => post({ op: 'laserStatus', sheetIds: ids })); };
  // Paul's order: the chain is looked up (1 document), and nothing else is added
  const withLookup = await status(S.materialize(paul(hand('button'))), [GF, RG]), without = await status(S.materialize(paul(hand('button', 'completed', 'legacy'))), [GF, RG]);
  eq(withLookup.seen.Charm_Custom_Orders, 1, 'Paul\'s order: the chain\'s own custom order record is read, once');
  eq(without.seen.Charm_Custom_Orders || 0, 0, 'a line the run saved as nothing to cut (no hint) is not looked up');
  eq(withLookup.reads - without.reads, 1, 'Paul\'s order: one added document read in all, and no added query');
  // 230 pieces completed by hand on no sheet, one in each of 230 orders of one sheet: at most 200 are looked up (batches of 100), the rest are not
  const N = 230, sheet = { id: 'big-sheet', metal: 'gold', index: 1, own: 'ok', setId: 'set-big' }, orders = [];
  for (let i = 0; i < N; i++) {
    const oid = String(4170000000 + i * 10 + 1), line = (tx, extra) => ({ tx, metal: 'gold', q: 1, kind: 'x', copies: [], state: 'written', problems: [], sku: 'MIDDLE', hold: null, change: false, engrave: 'plain', noDesign: false, stale: false, ...extra });
    orders.push({ id: oid, buyer: 'B' + i, lines: [line(2000 + i * 2, { copies: [{ sheet: 'big-sheet', pooled: true }] }), line(2001 + i * 2, { copies: [{ sheet: null, pooled: false }], state: 'unmatched', problems: ['unmatchedSku'], sku: '', hand: hand('button') })] });
  }
  const big = await status(S.materialize({ seed: 1, sandbox: false, archivedSheets: [], sheets: [sheet], orders }), ['big-sheet']);
  eq(big.seen.Charm_Custom_Orders, 200, `${N} pieces completed by hand on no sheet: ${big.seen.Charm_Custom_Orders} custom order records read (the bound is 200, in two batches of 100)`);
  const rd = big.out.sheets[0].orderReadiness, blocked = Object.entries(rd).filter(([, v]) => v.ready !== true).length;
  eq(blocked, N - 200, 'the ones past the bound are not looked up: they read as the pieces their run copy says (conservative, never a false "whole")');
  return { paul: withLookup.reads - without.reads, big: big.seen.Charm_Custom_Orders };
}

(async () => {
  for (const v of VARIANTS) pageChecks(v);
  interpretChecks();
  const srv = await start({ receipts: [] });
  let cost;
  try {
    for (const v of VARIANTS) await serverChecks(srv, v);
    await probeChecks(srv);
    await trustChecks(srv);
    cost = await costChecks(srv);
  } finally { srv.close(); }
  console.log(`hand-completed-no-block OK: ${checks} checks. A piece completed by hand (Review Complete Order, or its QR label printed) is resolved on the page and on the server alike: it needs no sheet, blocks no sheet's Order check, '!' panel, Approve, set completion or seal, is nobody's set mate and is never pooled; Reopen blocks again, a hold still holds, how 'sheet' is cut, a cancelled order is unchanged; the server reads its own record (never the page's hint), and the "unchanged?" read sees a completion and a reopen. Added reads: ${cost.paul} document for Paul's order, at most ${cost.big} for ${230} such pieces.`);
})().catch(e => { console.error(e); process.exitCode = 1; });
