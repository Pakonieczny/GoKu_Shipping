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
 * Round 8 (Paul, 5 Oct 12:18 and 12:34), the same order and the same rule, harder:
 *   · "This listing is still holding up the GF sheet even though you can see it was already 'Completed' manually by me": RG Sheet 1 held / not ready in no set, in the
 *     same set and in another set, with the chain unpressed / print only / Complete Order only / both / reopened. The chain is never one of the pieces that hold anything;
 *     what waits is the MIDDLE RG piece alone, named with its real sheet, and a sheet with no set says so (another set: the split); a wait inside GF's own set is the
 *     set's, never the order's (rgPageChecks, rgServerChecks).
 *   · "If either or both of the buttons Print QR Label, Complete Order are pressed than that piece should be considered released and nothing should hold this order or
 *     it's parent sheets": print only, Complete Order only, both in either order, reprints and a Reopen in between, through the REAL customPut / customReopen ops, then
 *     laserStatus, the Approve seal and the order's timeline as the server wrote it, read by the timeline module (pressChecks); the screenshot's five-piece order
 *     (four Review pieces and one on SS Sheet 1) with some pieces pressed and some not (fiveServerChecks).
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
const hand = (how = 'button', state = 'completed', rec = 'stale', presses) => ({ how, state, rec, ...(presses ? { presses } : {}) });
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
  // round 8 (Paul): "If either or both of the buttons Print QR Label, Complete Order are pressed than that piece should be considered released"
  { name: 'Print QR label only (record as customPut keeps it: one print, no Complete Order press)', spec: paul(hand('print', 'completed', 'stale', ['print'])), blocked: false },
  { name: 'Complete Order only (no label printed)', spec: paul(hand('button', 'completed', 'stale', ['button'])), blocked: false },
  { name: 'both buttons: printed, then Complete Order', spec: paul(hand('print', 'completed', 'stale', ['print', 'button'])), blocked: false },
  { name: 'both buttons: Complete Order, then printed', spec: paul(hand('button', 'completed', 'stale', ['button', 'print'])), blocked: false },
  { name: 'printed again and again (reprints)', spec: paul(hand('print', 'completed', 'stale', ['print', 'print', 'print'])), blocked: false },
  { name: 'both buttons pressed, then REOPENED', spec: paul(hand('print', 'open', 'stale', ['print', 'button'])), blocked: 'noSku' },
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

/* ── round 8, Paul's order as it stands (12:18): "This listing is still holding up the GF sheet even though you can see it was already 'Completed' manually by me" ──
 * CABLE CHAIN ONLY is completed by hand and MIDDLE 9935 RG sits on RG Sheet 1, which is held / not ready, in NO set (or in the same set, or in another). The chain
 * contributes nothing anywhere; what still waits is the MIDDLE RG piece alone, on RG Sheet 1, and that is said with its real piece, its real sheet and, when the sheet
 * has no set, that it has none (another set: the split). The same shape is read through the page (issues, explain, laserSheet) and the server's answered records. */
const MID_RG = `${ORDER}_1003`, MID_GF = `${ORDER}_1002`;
// what RG Sheet 1 itself still lacks (the words that name it); null = it was RECORDED in a set but never joined it (a draft sheet, or a rose sheet left out of the set before Cut Sheet):
// the Library card does not show it in that set (O.libraryGroup) and readiness does not either (setOf), so it is in no set at all, whatever its record says
const RG_OWN = { noQr: /its QR labels are missing$/, held: /it is held back from Laser cutting$/, roseNoPlan: /its layout needs verification$/, draft: null, solidExcluded: null };
const RG_SETS = [['in no set', null], ['in the same set', SET], ['in another set', 'set-2']];
const RG_CHAIN = [['unpressed', null], ['print only', hand('print', 'completed', 'stale', ['print'])], ['Complete Order only', hand('button', 'completed', 'stale', ['button'])], ['both buttons', hand('print', 'completed', 'stale', ['print', 'button'])], ['reopened', hand('button', 'open', 'stale', ['button'])]];
const rgSpec = (h, own, setId) => paul(h, {}, { sheets: [{ id: GF, metal: 'gold', index: 1, own: 'ok', setId: SET }, { id: RG, metal: 'rose', index: 1, own, setId }] });
// `recorded` is the set id the sheet's record carries; `setId` the set it is really in (none, for a draft or an excluded sheet)
const rgCombos = function* () { for (const own of Object.keys(RG_OWN)) for (const [setName, recorded] of RG_SETS) { const setId = RG_OWN[own] === null ? null : recorded; for (const [pressName, h] of RG_CHAIN) yield { own, setName, recorded, setId, pressName, h, tag: `RG Sheet 1 ${own}, ${RG_OWN[own] === null ? `recorded ${setName}, joined none` : setName}, chain ${pressName}`, released: !!h && h.state !== 'open', same: setId === SET }; } };
/** What GF Sheet 1 must be held back by, in piece ids: the chain only while it is not released, the MIDDLE RG piece unless RG Sheet 1 is in GF's own set (the set's wait). */
const rgWant = c => (c.released ? [] : [`${CHAIN}_1`]).concat(c.same ? [] : [`${MID_RG}_1`]);
/** The words and flags of the one honest wait (a released chain, RG Sheet 1 not in GF's set): said of the real piece and the real sheet, never of the chain. */
function rgWords(c, i, tag) {
  eq(i.key, 'otherSheetNotReady', `${tag}: the one wait is for the other sheet`); eq(i.pieceCount, 2, `${tag}: two pieces count (the chain is not one of them)`);
  eq(i.pieces.map(p => [p.poolId, p.sheetLabel, p.kind]), [[`${MID_RG}_1`, 'RG Sheet 1', 'otherSheetNotReady']], `${tag}: it names the MIDDLE RG piece on RG Sheet 1`);
  if (c.setId === null) {
    ok(i.noSet === true && !i.split && i.pieces[0].noSet === true && !i.pieces[0].split, `${tag}: it says the sheet has no set (flags)`);
    ok(new RegExp('^Its other piece is on RG Sheet 1, which is in no set' + (RG_OWN[c.own] ? ', and ' + RG_OWN[c.own].source : '$')).test(i.why), `${tag}: it says so in words: ${i.why}`);
    ok(/in no set/.test(i.pieces[0].why), `${tag}: and in the piece's own line: ${i.pieces[0].why}`);
  } else {
    ok(i.split === true && !i.noSet && i.pieces[0].split === true && !i.pieces[0].noSet, `${tag}: another set is a split (flags)`);
    ok(/^Split between Set 1 and Set 2: its other piece is on RG Sheet 1, and /.test(i.why) && /in another set/.test(i.pieces[0].why), `${tag}: said as the split: ${i.why}`);
  }
  ok(!/completed|chain/i.test(i.why + ' ' + i.pieces[0].why) && !JSON.stringify(i).includes(CHAIN), `${tag}: the piece completed by hand is not named or blamed`);
}
function rgPageChecks() {
  let n = 0;
  for (const c of rgCombos()) {
    const { tag } = c, shop = S.materialize(rgSpec(c.h, c.own, c.recorded)), rows = S.uiRows(shop), sheets = P.pageSheets(shop), pre = P.serverLike(shop);
    const gf = sheets.find(s => s.id === GF), rg = sheets.find(s => s.id === RG), want = rgWant(c);
    // whichever sheet asks: the order waits for the MIDDLE RG piece (RG Sheet 1 is not ready) and for the chain only until it is released; it never counts a released chain as a piece
    const rep = R.orderReports(rows, sheets)[ORDER];
    eq(rep.blocks.map(b => b.lineKey).sort(), (c.released ? [] : [CHAIN]).concat([MID_RG]).sort(), `${tag}: the pieces that hold the order`);
    eq(rep.pieceCount, c.released ? 2 : 3, `${tag}: pieces counted`);
    const mid = rep.blocks.find(b => b.lineKey === MID_RG);
    eq([mid.key, mid.sheetLabel, mid.setId], ['otherSheetNotReady', 'RG Sheet 1', c.setId], `${tag}: the MIDDLE RG piece waits for RG Sheet 1, whose set is ${c.setId}`);
    // GF Sheet 1's '!' panel (rows + every sheet, the page's own reading) and the record the server answers
    const list = R.issues(gf, { rows, allSheets: sheets }).filter(i => i.step === 'orders'), answered = R.issues(pre.find(s => s.id === GF), {}).filter(i => i.step === 'orders');
    for (const l of [list, answered]) {
      eq(l.length, want.length ? 1 : 0, `${tag}: GF Sheet 1 lists ${want.length ? 'the order once' : 'no order'}`);
      if (want.length) eq(l[0].pieces.map(p => p.poolId).sort(), want.slice().sort(), `${tag}: exactly these pieces`);
    }
    if (want.length === 1 && !c.same) { rgWords(c, list[0], tag); rgWords(c, answered[0], tag + ' (server answer)'); }
    if (want.length === 2) {   // an unreleased chain AND the RG piece: each as what it is, the chain first (it is the order's head)
      eq(list[0].pieces.map(p => [p.poolId, p.kind]), [[`${CHAIN}_1`, 'noSku'], [`${MID_RG}_1`, 'otherSheetNotReady']], `${tag}: the chain holds as a piece with no SKU, the RG piece as a wait`);
      eq(!!list[0].pieces[1].noSet, c.setId === null, `${tag}: only the RG piece is told as no set`);
    }
    // the Order check step of the rail and the sheet's readiness
    const ex = R.explain(pre.find(s => s.id === GF), { rows }), step = ex.steps.find(s => s.key === 'orders');
    eq(step.state === 'done', !want.length, `${tag}: GF's Order check step ${want.length ? 'is not' : 'is'} done (${step.state}: ${step.detail})`);
    eq(R.laserSheet(pre.find(s => s.id === GF)).ready, !want.length, `${tag}: GF's laserSheet`);
    if (want.length) { eq(step.state, 'blocked', `${tag}: the step is blocked by a real wait`); eq(step.items.filter(i => i.kind === 'order').map(i => i.id), [ORDER], `${tag}: it lists the order`); eq(step.items.find(i => i.kind === 'order').why, list[0].why, `${tag}: with the issue's own words`); }
    if (want.length === 1 && !c.same && c.setId === null) ok(step.items.some(i => i.kind === 'sheet' && i.id === RG && (RG_OWN[c.own] ? /^Holds 1 order of this sheet back: it is in no set, and / : /^Holds 1 order of this sheet back: it is not in a set yet$/).test(i.why)), `${tag}: the checklist says the sheet has no set: ${JSON.stringify(step.items)}`);
    // RG Sheet 1's own panel: the chain (unreleased) holds it; GF Sheet 1 is ready, so nothing else does
    eq(R.issues(rg, { rows, allSheets: sheets }).filter(i => i.step === 'orders').map(i => i.pieces.map(p => p.poolId)), c.released ? [] : [[`${CHAIN}_1`]], `${tag}: RG Sheet 1's own panel`);
    n++;
  }
  return n;
}
async function rgServerChecks(srv) {
  const post = body => fetch(srv.sorterOrigin + '/.netlify/functions/charmNestLibrary', { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(body) }).then(async r => ({ status: r.status, ...(await r.json()) }));
  let n = 0;
  for (const c of rgCombos()) {
    const { tag } = c, want = rgWant(c);
    seed(srv.st, S.materialize(rgSpec(c.h, c.own, c.recorded)), false);
    const a = await post({ op: 'laserStatus', sheetIds: [GF, RG] }), gf = a.sheets.find(s => s.id === GF), rd = gf.orderReadiness[ORDER];
    eq(a.status, 200, `${tag}: laserStatus answers`);
    if (!want.length) eq(rd, { ready: true }, `${tag}: the server says GF Sheet 1's order is whole`);
    else {
      eq(rd.blocks.map(b => b.poolId).sort(), want.slice().sort(), `${tag}: the server's blocks are exactly the pieces that hold`);
      eq(rd.pieceCount, c.released ? 2 : 3, `${tag}: the server counts the pieces the same way`);
      const l = R.issues(gf, {}).filter(i => i.step === 'orders');
      eq(l.length, 1, `${tag}: the Library's '!' list from the server's answer`);
      if (want.length === 1 && !c.same) rgWords(c, l[0], tag + ' (from the server)');
    }
    eq(gf.laser.ready, !want.length, `${tag}: the server's laser.ready for GF Sheet 1`);
    // RG Sheet 1's own answer: held back by the chain only (never by GF Sheet 1, which is ready)
    const rr = a.sheets.find(s => s.id === RG).orderReadiness[ORDER];
    if (c.released) eq(rr, { ready: true }, `${tag}: RG Sheet 1 is held by nothing of this order`); else eq(rr.blocks.map(b => b.lineKey), [CHAIN], `${tag}: RG Sheet 1 is held by the chain alone`);
    n++;
  }
  return n;
}

/* ── "either or both buttons", through the real ops: customPut (Print QR label / Complete Order) and customReopen, then laserStatus, the Approve seal and the order's
 * timeline as the server wrote it (Order_Timeline) read by the timeline module: print only, Complete Order only, both in either order, reprints, and a Reopen in between. */
const fs = require('node:fs'), vm = require('node:vm');
const UI = (() => { const ctx = vm.createContext({ console }); vm.runInContext(fs.readFileSync(path.join(root, 'charm-nest-timeline-ui.js'), 'utf8'), ctx); return ctx.OrderTimelineUI; })();
const sleep = ms => new Promise(r => setTimeout(r, ms));
const timelineOf = (srv, orderId) => [...srv.st.docs].filter(([k]) => k.startsWith('Order_Timeline/' + orderId + '~')).map(([, v]) => ({ ...v })).sort((a, b) => a.at - b.at);
const FLOWS = [['print only', ['print']], ['Complete Order only', ['button']], ['print, then Complete Order', ['print', 'button']], ['Complete Order, then print', ['button', 'print']], ['printed again and again', ['print', 'print', 'print']],
  ['Complete Order, Reopen, print', ['button', 'reopen', 'print']], ['print, Reopen, Complete Order', ['print', 'reopen', 'button']], ['both buttons, then Reopen', ['print', 'button', 'reopen']]];
async function pressChecks(srv) {
  const post = body => fetch(srv.sorterOrigin + '/.netlify/functions/charmNestLibrary', { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(body) }).then(async r => ({ status: r.status, ...(await r.json()) }));
  const pieces = [{ key: CHAIN, tid: '1001', qty: 1, line: { title: 'CABLE CHAIN ONLY' }, pools: [], sheets: [] }, { key: `${ORDER}_1002`, tid: '1002', qty: 1, line: { title: 'MIDDLE 9935' }, pools: [`${MID_GF}_1`], sheets: [GF] }, { key: MID_RG, tid: '1003', qty: 1, line: { title: 'MIDDLE 9935' }, pools: [`${MID_RG}_1`], sheets: [RG] }];
  let n = 0;
  for (const [name, flow] of FLOWS) {
    seed(srv.st, S.materialize(paul(null)), false);
    const status = async () => (await post({ op: 'laserStatus', sheetIds: [GF, RG] })).sheets;
    const whole = async (released, tag) => {
      const ss = await status();
      for (const s of ss) {
        if (released) { eq(s.orderReadiness[ORDER], { ready: true }, `${name}: ${tag}: ${s.id}: released, nothing holds the order`); eq(s.laser.ready, true, `${name}: ${tag}: ${s.id}: laser-ready`); }
        else { eq([s.orderReadiness[ORDER].ready, s.orderReadiness[ORDER].key, s.orderReadiness[ORDER].blocks.map(b => b.lineKey)], [false, 'noSku', [CHAIN]], `${name}: ${tag}: ${s.id}: the unpressed chain holds it`); eq(s.laser.ready, false, `${name}: ${tag}: ${s.id}: not laser-ready`); }
      }
    };
    // the timeline's own reading of the events the server wrote, with the question the chain once asked (an Unknown SKU the person had to answer)
    const asked = { id: 'q-1', type: 'needsDecision', orderId: ORDER, lineKey: CHAIN, at: Date.now() - 60000, by: 'system', text: 'Which design is this?' };
    const timeline = (released, tag) => {
      const evs = [asked, ...timelineOf(srv, ORDER)], h = UI.handOf(evs), sum = UI.summary(evs, pieces), laser = sum.rail.find(r => r.s.k === 'laser'), bl = UI.blockerOf(evs);
      eq(!!h, released, `${name}: ${tag}: the order window's rail reads the chain as ${released ? '' : 'not '}completed by hand`);
      if (released) ok(h.lineKey === CHAIN && ['sealPrinted', 'sealCompleted'].includes(h.type), `${name}: ${tag}: by the last press: ${h && h.type}`);
      eq([laser.n, laser.of], [released ? 1 : 0, 3], `${name}: ${tag}: "LASER CUT n of m" counts the released piece as through, the unpressed one as waiting`);
      eq(sum.each[0].D.hand ? 'hand' : '', released ? 'hand' : '', `${name}: ${tag}: the piece row says completed by hand`);
      eq(bl && bl.label, released ? null : 'Needs a decision', `${name}: ${tag}: what holds the order up in words`);
    };
    await whole(false, 'before any press'); timeline(false, 'before any press');
    let released = false; const seen = [];
    for (const step of flow) {
      await sleep(4);
      if (step === 'reopen') { const r = await post({ op: 'customReopen', key: CHAIN, by: 'Seth', from: 'Review' }); ok(r.ok === true, `${name}: Reopen is recorded`); released = false; seen.push('reopen'); }
      else { const r = await post({ op: 'customPut', key: CHAIN, by: 'Seth', how: step, receiptId: ORDER, transactionId: '1001', sku: '', title: 'CABLE CHAIN ONLY', category: 'Chain only', kind: 'chain', from: 'Review' }); ok(r.ok === true && r.record.state === 'completed', `${name}: the ${step === 'print' ? 'Print QR label' : 'Complete Order'} press is recorded as completed`); released = true; seen.push(step); }
      await sleep(4);
      await whole(released, `after ${seen.join(', ')}`); timeline(released, `after ${seen.join(', ')}`);
    }
    // the Approve press: with the chain released the set is approved (three readiness seals), with a Reopen last it records nothing
    const seal = await post({ op: 'flowApply', by: 'Tester', steps: [{ type: 'seal', kind: 'set', id: SET }] });
    eq((seal.added || []).length, released ? 3 : 0, `${name}: the Approve press ${released ? 'records the readiness seals' : 'records nothing while the chain is open again'}`);
    n++;
  }
  return n;
}

/* ── the screenshot (5 Oct, 12:34): an order of four Review pieces (COMPASS 4682, BEACH 32125, WOLF 50442, KAYAK 77459) and one piece on SS Sheet 1 (AQUATIC 11- DOLPHIN),
 * each Review piece with Print QR label and Complete Order. Pressing either button, or both, on a piece releases it: nothing holds the order or SS Sheet 1 for it. The pieces
 * not pressed still hold; a Reopen holds again. Real ops over the server, and the page's own reading of the same records. */
const FIVE = '4170999001', SS = 'ss-sheet-1', NAMES = ['COMPASS 4682', 'BEACH 32125', 'WOLF 50442', 'KAYAK 77459'];
const fiveSpec = () => ({ seed: 1, sandbox: false, archivedSheets: [], sheets: [{ id: SS, metal: 'silver', index: 1, own: 'ok', setId: 'set-5' }],
  orders: [{ id: FIVE, buyer: 'Buyer', lines: [{ tx: 2000, metal: 'silver', q: 1, kind: 'p', copies: [{ sheet: SS, pooled: true }], state: 'written', problems: [], sku: 'AQUATIC-11', hold: null, change: false, engrave: 'plain', noDesign: false, stale: false },
    ...NAMES.map((nm, i) => ({ tx: 2001 + i, metal: 'silver', q: 1, kind: 'p', copies: [{ sheet: null, pooled: false }], state: 'unmatched', problems: ['unmatchedSku'], sku: '', hold: null, change: false, engrave: 'plain', noDesign: false, stale: false }))] }] });
async function fiveServerChecks(srv) {
  const post = body => fetch(srv.sorterOrigin + '/.netlify/functions/charmNestLibrary', { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(body) }).then(async r => ({ status: r.status, ...(await r.json()) }));
  const keys = NAMES.map((_, i) => `${FIVE}_${2001 + i}`);
  seed(srv.st, S.materialize(fiveSpec()), false);
  const state = async () => { const s = (await post({ op: 'laserStatus', sheetIds: [SS] })).sheets[0], rd = s.orderReadiness[FIVE]; return { rd, ready: s.laser.ready, holding: rd.ready === true ? [] : rd.blocks.map(b => b.lineKey).sort(), issues: R.issues(s, {}).filter(i => i.step === 'orders') }; };
  const put = (i, how) => post({ op: 'customPut', key: keys[i], by: 'Seth', how, receiptId: FIVE, transactionId: String(2001 + i), sku: '', title: NAMES[i], category: 'Custom order', kind: 'custom', from: 'Review' });
  let s = await state();
  eq(s.holding, keys.slice().sort(), 'five pieces, none pressed (as in the screenshot): each of the four Review pieces holds SS Sheet 1'); eq(s.ready, false, 'SS Sheet 1 is not laser-ready'); eq(s.issues.length, 1, 'one order issue'); eq(s.issues[0].pieces.length, 4, 'naming the four pieces');
  await put(0, 'print'); s = await state();
  eq(s.holding, keys.slice(1).sort(), 'COMPASS: Print QR label only: released, the other three still hold');
  await put(1, 'button'); s = await state();
  eq(s.holding, keys.slice(2).sort(), 'BEACH: Complete Order only: released');
  await put(2, 'button'); await sleep(3); await put(2, 'print'); s = await state();
  eq(s.holding, [keys[3]], 'WOLF: both buttons: released; only KAYAK, not pressed, still holds'); eq(s.ready, false, 'and SS Sheet 1 still waits for it'); eq(s.issues[0].pieces.map(p => p.poolId), [`${keys[3]}_1`], 'the list names KAYAK alone');
  await put(3, 'print'); await sleep(3); await put(3, 'print'); s = await state();
  eq(s.rd, { ready: true }, 'KAYAK: printed twice: all four released, nothing holds the order'); eq(s.ready, true, 'SS Sheet 1 is laser-ready'); eq(s.issues.length, 0, 'no order issue');
  const seal = await post({ op: 'flowApply', by: 'Tester', steps: [{ type: 'seal', kind: 'set', id: 'set-5' }] });
  eq((seal.added || []).length, 2, 'the Approve press records the readiness seals (SS Sheet 1 and its set)');
  // Reopen (Review tab): that piece holds again, only that one
  await sleep(3); await post({ op: 'customReopen', key: keys[2], by: 'Seth', from: 'Review' }); s = await state();
  eq(s.holding, [keys[2]], 'WOLF reopened: it holds again, the others stay released'); eq(s.ready, false, 'SS Sheet 1 waits again');
  await sleep(3); await put(2, 'button'); s = await state();
  eq(s.rd, { ready: true }, 'WOLF pressed again: released again');
  // the page's own reading of the same shapes (records as customPut keeps them), through every press state
  let pages = 0;
  for (const presses of [[null, null, null, null], [['print'], ['button'], ['print', 'button'], ['button', 'print', 'print']], [['print'], null, ['button'], null], [['print'], 'open', ['print', 'button'], ['button']]]) {
    const spec = fiveSpec();
    presses.forEach((p, i) => { if (p) spec.orders[0].lines[1 + i].hand = p === 'open' ? hand('button', 'open', 'stale', ['button']) : hand(p[0], 'completed', 'stale', p); });
    const shop = S.materialize(spec), rows = S.uiRows(shop), sheets = P.pageSheets(shop), ss = sheets.find(x => x.id === SS);
    const holding = presses.map((p, i) => (!p || p === 'open' ? `${FIVE}_${2001 + i}_1` : null)).filter(Boolean), list = R.issues(ss, { rows, allSheets: sheets }).filter(i => i.step === 'orders');
    eq(list.length ? list[0].pieces.map(p => p.poolId).sort() : [], holding.sort(), `page: ${JSON.stringify(presses)}: the pieces that hold SS Sheet 1`);
    eq(R.laserSheet(P.serverLike(shop).find(x => x.id === SS)).ready, !holding.length, `page: ${JSON.stringify(presses)}: SS Sheet 1 laser-ready`);
    eq(CO.evaluateOrder(rows).committable, !holding.length, `page: ${JSON.stringify(presses)}: the sorter's set release gate`);
    pages++;
  }
  return pages;
}

/* ── Paul's newest screenshot (12:4x): RG Sheet 1, GF Sheet 1 and SS Sheet 1 sit in ONE card, Set 1, with Leslie's order (chain completed by hand, MIDDLE on GF, MIDDLE on RG). How a Rose Gold
 * sheet is in a set: its OWN record says so (setId, with draft false and solidIncluded not false: the Cut Sheet press, or dragging it into a set, writes exactly that), and nothing else does.
 * The Library card (CharmNestOrders.libraryGroup), the order check (setOf, forSheet, ownSetWait) and the server read those same fields, so they cannot disagree about which set a sheet
 * is in. Until the press the sheet is a draft in no set; a set record that lists it (sheetIds) does not put it there. A wait for it is dropped only once it has joined. */
const imgSpec = (h, rgOwn = 'ok', rgSet = SET) => {
  const s = paul(h, {}, { sheets: [{ id: GF, metal: 'gold', index: 1, own: 'ok', setId: SET }, { id: SS, metal: 'silver', index: 1, own: 'ok', setId: SET }, { id: RG, metal: 'rose', index: 1, own: rgOwn, setId: rgSet }] });
  s.orders.push({ id: '4170888001', buyer: 'Other', lines: [{ tx: 3000, metal: 'silver', q: 1, kind: 'p', copies: [{ sheet: SS, pooled: true }], state: 'written', problems: [], sku: 'AQUATIC-11', hold: null, change: false, engrave: 'plain', noDesign: false, stale: false }] });
  return s;
};
async function rgMembershipChecks(srv) {
  const post = body => fetch(srv.sorterOrigin + '/.netlify/functions/charmNestLibrary', { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(body) }).then(async r => ({ status: r.status, ...(await r.json()) }));
  const card = s => CO.libraryGroup(s, { combineSolids: true }).setId || null;
  let n = 0;
  // 1. the screenshot's shape: three sheets in one card, the chain pressed (either button, or both): nothing is anyone's wait, on any sheet
  for (const [pressName, h] of RG_CHAIN.slice(1, 4)) {
    const shop = S.materialize(imgSpec(h)), rows = S.uiRows(shop), sheets = P.pageSheets(shop), pre = P.serverLike(shop);
    for (const s of sheets) { eq(card(s), SET, `${pressName}: the card puts ${s.id} in Set 1`); eq(R.setOf(s), SET, `${pressName}: and the order check reads ${s.id} in Set 1`); }
    for (const id of [GF, SS, RG]) {
      eq(R.issues(sheets.find(x => x.id === id), { rows, allSheets: sheets }).filter(i => i.step === 'orders'), [], `${pressName}: ${id}: no order is listed`);
      eq(R.explain(pre.find(x => x.id === id), { rows }).steps.find(x => x.key === 'orders').state, 'done', `${pressName}: ${id}: Order check is done`);
      eq(R.laserSheet(pre.find(x => x.id === id)).ready, true, `${pressName}: ${id}: laser-ready`);
    }
    n++;
  }
  // 2. the card and the order check read a Rose Gold sheet's set alike, whatever its own state and whichever set its record carries (draft, left out, held, not ready)
  for (const own of Object.keys(RG_OWN)) for (const [setName, recorded] of RG_SETS) {
    const shop = S.materialize(imgSpec(hand('button'), own, recorded));
    for (const s of P.pageSheets(shop)) eq(card(s), R.setOf(s), `RG Sheet 1 ${own}, recorded ${setName}: ${s.id}: the card's set and the order check's set are the same`);
    n++;
  }
  // 3. through the server: not joined yet (a draft in no set): the wait is listed, and said as a sheet in no set; the Cut Sheet press writes the membership on the sheet and the wait is gone at
  //    the next read, which the cheap "unchanged?" read sees at once. Not ready but joined: the wait is the set's, not the order's.
  const words = a => { const gf = a.sheets.find(s => s.id === GF), rd = gf.orderReadiness[ORDER], l = rd.ready === true ? [] : R.issues(gf, {}).filter(i => i.step === 'orders'); return { rd, l, gf }; };
  const join = (set, seq) => srv.st.put('Charm_Nest_Sheets', RG, { draft: false, setId: set, setSeq: seq, sheetIndex: 1, solidIncluded: null });   // (the fields the Cut Sheet press and a drag into a set save)
  for (const [name, own, ready] of [['a draft that is complete in every other way', 'draft', true], ['a sheet whose QR labels are missing', 'noQr', false]]) {
    const h = hand('button', 'completed', 'stale', ['button']);
    seed(srv.st, S.materialize(imgSpec(h, own, null)), false);
    let a = await post({ op: 'laserStatus', sheetIds: [GF, SS], wantRevs: true }), w = words(a);
    eq([w.rd.ready, w.rd.blocks.map(b => [b.lineKey, b.key, b.sheetLabel, b.setId])], [false, [[MID_RG, 'otherSheetNotReady', 'RG Sheet 1', null]]], `${name}, not in a set: GF Sheet 1 waits for the MIDDLE RG piece on RG Sheet 1, which has no set`);
    ok(w.l.length === 1 && w.l[0].noSet === true && /^Its other piece is on RG Sheet 1, which is in no set/.test(w.l[0].why) && w.l[0].pieces.length === 1, `${name}: it is told as a sheet in no set, with its one real piece: ${w.l[0] && w.l[0].why}`);
    eq(w.gf.laser.ready, false, `${name}: GF Sheet 1 is not laser-ready while that is so`);
    // the sheet's own set record lists it, its own record does not: still no set (the card shows it in none)
    srv.st.put('Charm_Nest_Sets', SET, { sheetIds: [GF, SS, RG] });
    a = await post({ op: 'laserStatus', sheetIds: [GF, SS] }); w = words(a);
    ok(w.rd.ready === false && w.rd.blocks[0].setId === null, `${name}: a set record that lists the sheet does not put it in the set: ${JSON.stringify(w.rd).slice(0, 140)}`);
    srv.st.put('Charm_Nest_Sets', SET, { sheetIds: [GF, SS] });
    // the press
    a = await post({ op: 'laserStatus', sheetIds: [GF, SS], wantRevs: true });
    join(SET, 1);
    const probe = await post({ op: 'laserStatus', sheetIds: [GF, SS], ifRevs: a.revs, wantRevs: true });
    ok(!probe.unchanged && probe.sheets, `${name}: the Cut Sheet press is a change the cheap read sees`);
    w = words(probe);
    eq(w.rd, { ready: true }, `${name}: joined the set (${ready ? 'ready' : 'not ready'}): GF Sheet 1 no longer waits for it: it is the set's wait at most, never the order's`);
    eq([w.l.length, w.gf.laser.ready], [0, true], `${name}: no '!' entry, GF Sheet 1 laser-ready`);
    // the sheet's answered record reads in Set 1 the way the card does
    const rgRec = (await post({ op: 'laserStatus', sheetIds: [GF, SS, RG] })).sheets.find(s => s.id === RG);
    eq([card(rgRec), R.setOf(rgRec)], [SET, SET], `${name}: RG Sheet 1's answered record is in Set 1 for the card and for readiness`);
    eq(R.laserSheet(rgRec).ready, ready, `${name}: and is ${ready ? '' : 'not '}laser-ready itself`);
    n++;
  }
  // 4. another set (the real split) still waits, and says so
  seed(srv.st, S.materialize(imgSpec(hand('button', 'completed', 'stale', ['button']), 'noQr', 'set-2')), false);
  const split = words(await post({ op: 'laserStatus', sheetIds: [GF, SS] }));
  ok(split.l.length === 1 && split.l[0].split === true && !split.l[0].noSet, `an order split between Set 1 and Set 2 stays an issue, told as the split: ${split.l[0] && split.l[0].why}`);
  return n + 1;
}

/* ── a chain line that has a pool id, or a copy on RG Sheet 1, and was completed by hand all the same. A completed custom order reads as "nothing to cut" (interpretLine: noDesign, as
 * interpretChecks shows), and the run's record carries that (bridge lineRecord: noDesign comes from the row's spec), so such a line is NOT one of the order's pieces, on the page
 * and on the server alike: it blocks nothing and is never named, whichever button was pressed. Checked against the oracle in every reading, and through the server. */
async function boundaryChecks(srv) {
  const { check } = require('./issues-server.cjs');
  let n = 0;
  for (const [name, copies] of [['a chain line with a pool id', [{ sheet: null, pooled: true }]], ['a chain copy that sits on RG Sheet 1', [{ sheet: RG, pooled: true }]]]) for (const presses of [['print'], ['button'], ['print', 'button']]) {
    const spec = paul(hand(presses[0], 'completed', 'stale', presses), { copies, state: 'pooled', problems: [], noDesign: true }, { sheets: [{ id: GF, metal: 'gold', index: 1, own: 'ok', setId: SET }, { id: RG, metal: 'rose', index: 1, own: 'noQr', setId: null }] });
    const shop = S.materialize(spec), tag = `${name}, ${presses.join('+')}`;
    eq(P.disagreementsWith(R, shop, ['rows', 'records', 'pre']), [], `${tag}: the page agrees with the oracle`);
    eq(await check(srv, shop, [GF, RG], false), [], `${tag}: the server agrees with the oracle`);
    const rows = S.uiRows(shop), sheets = P.pageSheets(shop), list = R.issues(sheets.find(s => s.id === GF), { rows, allSheets: sheets }).filter(i => i.step === 'orders');
    eq(list.length, 1, `${tag}: GF Sheet 1 lists the order once (RG Sheet 1 is not ready and in no set)`);
    eq(list[0].pieces.map(p => [p.poolId, p.noSet === true]), [[`${MID_RG}_1`, true]], `${tag}: only the MIDDLE RG piece, told as on a sheet in no set; the chain is not named`);
    n++;
  }
  return n;
}

(async () => {
  process.env.CHARM_NEST_HOLDERS_MS = '0';
  for (const v of VARIANTS) pageChecks(v);
  interpretChecks();
  const rgPages = rgPageChecks();
  const srv = await start({ receipts: [] });
  let cost, rgServers, presses, fives, members;
  try {
    for (const v of VARIANTS) await serverChecks(srv, v);
    rgServers = await rgServerChecks(srv);
    members = await rgMembershipChecks(srv);
    presses = await pressChecks(srv);
    fives = await fiveServerChecks(srv);
    await boundaryChecks(srv);
    await probeChecks(srv);
    await trustChecks(srv);
    cost = await costChecks(srv);
  } finally { srv.close(); }
  console.log(`hand-completed-no-block OK: ${checks} checks (${rgPages} RG Sheet 1 cases on the page, ${rgServers} through the server, ${presses} press sequences through the real ops, ${fives} five-piece shapes, ${members} Rose Gold set-membership cases). Either button, or both, releases a piece; a piece completed by hand (Review Complete Order, or its QR label printed) is resolved on the page and on the server alike: it needs no sheet, blocks no sheet's Order check, '!' panel, Approve, set completion or seal, is nobody's set mate and is never pooled; Reopen blocks again, a hold still holds, how 'sheet' is cut, a cancelled order is unchanged; the server reads its own record (never the page's hint), and the "unchanged?" read sees a completion and a reopen. Added reads: ${cost.paul} document for Paul's order, at most ${cost.big} for ${230} such pieces.`);
})().catch(e => { console.error(e); process.exitCode = 1; });
