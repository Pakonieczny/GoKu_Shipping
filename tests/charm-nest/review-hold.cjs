// An order sent to Review that sits on a sheet holds that sheet until it is resolved (charm-nest-review-hold.js + charm-nest-readiness.js + charm-nest-flow.js), on FAKE data only:
// no network, no Firestore, no buyer names. The hold is derived from the review record every time (a stub of CharmNestReviewMark), never remembered.
const assert = require('node:assert/strict');
const R = require('../../charm-nest-readiness.js');
const LF = require('../../charm-nest-flow.js');
const H = require('../../charm-nest-review-hold.js');
globalThis.CharmNestReviewHold = H;

// ── the review record (stub of charm-nest-review-mark.js) and the pieces (stub of OrderPieces) ──
const open = new Set(), markSubs = [], pieceSubs = [], pcs = {};
globalThis.CharmNestReviewMark = { isMarked: r => open.has(String(r)), markedRids: () => [...open], reasonOf: r => open.has(String(r)) ? 'Sent to Review by Name One' : '', onChange: f => { markSubs.push(f); return () => {}; } };
globalThis.OrderPieces = { of: rid => pcs[rid] || [], subscribe: f => { pieceSubs.push(f); }, load: () => Promise.resolve() };
const mark = rid => { open.add(rid); markSubs.forEach(f => f({ rid, kind: 'mark' })); };
const unmark = rid => { open.delete(rid); markSubs.forEach(f => f({ rid, kind: 'resolve' })); };
const piece = (rid, n, o) => ({ poolId: `${rid}_1_${n}`, lineKey: `${rid}_1`, key: `${rid}_1_${n}`, index: n, label: 'Round Charm', side: null, state: 'sheeted', sheetId: 'S1', sheetLabel: 'GF Sheet 1', setId: 'set1', hand: false, problem: null, ...o });

const back = id => ({ poolId: id, sheetId: 'S1', approvedAt: 10, approvedBy: 'Name One', verified: { geometry: { ok: true }, file: { ok: true } }, outputs: { ai: { path: id + '.ai', url: 'https://example.com/' + id + '.ai' } } });
const sheetOf = (id, rids, extra = {}) => {
  const ids = rids.map(r => `${r}_1_1`);
  return { id, setId: 'set1', runId: 'run1', status: 'complete', metal: 'gold', poolIds: ids, placedCount: ids.length, verification: { ok: true }, outputs: { ai: { url: 'https://example.com/f.ai' }, preview: { url: 'https://example.com/f.png' } }, orders: rids,
    orderReadiness: Object.fromEntries(rids.map(r => [r, { ready: true }])), label: { files: [{ path: 'qr.png', url: 'https://example.com/qr.png', payload: rids[0], orders: rids }] }, backPool: ids.map(back).map(b => ({ ...b, sheetId: id })), ...extra };
};
const A = '9100000001', B = '9100000002';

(async () => {
  // 0. nothing in Review: nothing changes
  const s1 = sheetOf('S1', [A, B]);
  assert.equal(R.sheet(s1).ready, true);
  assert.equal(H.overlay(s1), s1, 'no order in Review: the record comes back as it is');
  assert.deepEqual(H.heldBy('S1'), []); assert.equal(H.isHeld('S1'), false); assert.equal(H.reasonFor('S1'), '');
  assert.equal(R.approveBlock(s1), null);

  // 1. a one-piece order on the sheet is sent to Review: the sheet is held, the order stays on it
  pcs[A] = [piece(A, 1)];
  mark(A);
  let rows = H.heldBy('S1');
  assert.equal(rows.length, 1); assert.equal(rows[0].rid, A); assert.equal(rows[0].piece, '', 'a one-piece order names no piece'); assert.match(rows[0].reason, /^In Review/); assert.deepEqual(rows[0].poolIds, [`${A}_1_1`]); assert.equal(rows[0].sheetId, 'S1');
  assert.equal(H.isHeld('S1'), true); assert.match(H.reasonFor('S1'), new RegExp(`Order ${A} is in Review`));
  assert.deepEqual(H.sheetsHeldBy(A).map(x => x.sheetId), ['S1']);
  assert.deepEqual(H.heldBy('S2'), [], 'another sheet is not held');
  const o1 = H.overlay(s1);
  assert.notEqual(o1, s1, 'the cloud record is not mutated'); assert.equal(s1.orderReadiness[A].ready, true);
  assert.equal(R.sheet(o1).stages.orders, false, 'Order check is not done'); assert.equal(R.sheet(o1).ready, false); assert.equal(R.laserSheet(o1).ready, false);
  assert.equal(R.sheet(s1).ready, true, 'the original record still reads ready');
  const ab = R.approveBlock(o1); assert.ok(ab && ab.review && /held in Review: order 9100000001/.test(ab.why), 'Approve is grey with the order named: ' + JSON.stringify(ab));
  // the sheet-level and set-level readings
  const gate = R.setGate({ setId: 'set1', sheetIds: ['S1'] }, [o1]); assert.equal(gate.ready, false); assert.match(gate.reason, /GF Sheet 1|Sheet/); assert.match(gate.reason, /held in Review/);
  assert.equal(R.set({ sheetIds: ['S1'] }, [o1]).ready, false);
  // the issues list ('!'): key 'held', the review order's id, "In Review"
  const iss = R.issues(o1, { steps: ['orders'], perOrder: true }).filter(i => i.orderId === A);
  assert.equal(iss.length, 1); assert.equal(iss[0].key, 'held'); assert.match(iss[0].why, /^In Review/);
  const ex = R.explain(o1, { rows: [] }); assert.equal(ex.ready, false); assert.equal(ex.steps.find(t => t.key === 'orders').state !== 'done', true);
  const exo = ex.steps.find(t => t.key === 'orders'); assert.match(exo.detail, /in Review/); assert.doesNotMatch(exo.detail, /other pieces/); assert.match(ex.nextText, /in Review/i, ex.nextText);

  // 2. the flow: Approve / the move to Laser cutting refuses with the order named, and does not claim the order check passed
  const flowState = rec => ({ sheets: { S1: rec }, sets: { set1: { setId: 'set1', seq: 1, sheetIds: ['S1'], status: 'labelled' } }, runs: {}, live: {} });
  const recCloud = { ...s1, laser: R.laserSheet(s1) };
  const planWith = rec => LF.core.planMove(flowState(rec), { kind: 'sheet', id: 'S1', to: { area: 'laser' } }, { by: 'Name One', rows: [] });
  const okPlan = planWith(recCloud); assert.equal(okPlan.needs.filter(n => n.key === 'orders').length, 0, 'baseline: no order need');
  const heldPlan = planWith(H.overlayFlow(recCloud));
  assert.equal(heldPlan.ok, false, 'the plan refuses'); const need = heldPlan.needs.find(n => n.key === 'orders'); assert.ok(need, 'an Order check need: ' + JSON.stringify(heldPlan.needs.map(n => n.key)));
  assert.match(need.label, /in Review/); assert.ok(need.items.some(i => i.id === A && /In Review/.test(i.why)), 'it names the order: ' + JSON.stringify(need.items));
  assert.equal(heldPlan.auto.some(a => a.key === 'check:orders'), false, 'no false "other pieces ready" tick');

  // 3. resolved in the Review tab: the stop clears by itself, the very next reading
  const heard = []; const off = H.onChange(x => heard.push(x)); H._check();
  unmark(A); H._check();
  assert.deepEqual(H.heldBy('S1'), []); assert.equal(H.overlay(s1), s1); assert.equal(R.sheet(H.overlay(s1)).ready, true); assert.equal(planWith(H.overlayFlow(recCloud)).needs.filter(n => n.key === 'orders').length, 0);
  assert.ok(heard.some(x => x.rids.includes(A)), 'the change callback fired: ' + JSON.stringify(heard)); off();

  // 4. a pair (left and right earring) on one sheet is one order held together
  pcs[B] = [piece(B, 1, { side: 'L' }), piece(B, 2, { side: 'R', poolId: `${B}_1_2` })];
  const sPair = sheetOf('S1', [B], { poolIds: [`${B}_1_1`, `${B}_1_2`], placedCount: 2, backPool: [`${B}_1_1`, `${B}_1_2`].map(back) });
  mark(B);
  rows = H.heldBy('S1'); assert.equal(rows.length, 1, 'one order, one row'); assert.equal(rows[0].piece, 'left earring and right earring'); assert.equal(rows[0].poolIds.length, 2);
  const oPair = H.overlay(sPair); assert.equal(R.sheet(oPair).ready, false);
  const issPair = R.issues(oPair, { steps: ['orders'], perOrder: true }).find(i => i.orderId === B); assert.equal(issPair.pieces.length, 2); assert.ok(issPair.pieces.every(p => /In Review/.test(p.why)));
  assert.equal(R.blockText(R.orderBlockers(oPair)[0]).startsWith('the left earring') || /In Review/.test(R.blockText(R.orderBlockers(oPair)[0])), true);
  unmark(B);

  // 5. a multi-piece order across two sheets of one SET: both sheets are held, the set waits as one and says it once
  const C = '9100000003';
  pcs[C] = [piece(C, 1, { sheetId: 'S1', sheetLabel: 'GF Sheet 1' }), piece(C, 2, { sheetId: 'S2', sheetLabel: 'GF Sheet 2' })];
  const sA = sheetOf('S1', [C], { poolIds: [`${C}_1_1`], orderReadiness: { [C]: { ready: true } } }), sB = sheetOf('S2', [C], { poolIds: [`${C}_1_2`], orderReadiness: { [C]: { ready: true } }, backPool: [back(`${C}_1_2`)].map(b => ({ ...b, sheetId: 'S2' })) });
  mark(C);
  assert.deepEqual(H.sheetsHeldBy(C).map(x => x.sheetId).sort(), ['S1', 'S2']); assert.equal(H.heldBy('S1')[0].piece, 'piece 1 of 2'); assert.equal(H.heldBy('S2')[0].piece, 'piece 2 of 2');
  const set = { setId: 'set1', sheetIds: ['S1', 'S2'] };
  const g2 = R.setGate(set, [H.overlay(sA), H.overlay(sB)]); assert.equal(g2.ready, false); assert.equal(g2.blockers.length, 2); assert.match(g2.reason, /and 1 more$/);
  assert.equal(R.setGate(set, [sA, sB]).ready, true, 'baseline without the review: the set is ready');
  unmark(C);
  assert.equal(R.setGate(set, [H.overlay(sA), H.overlay(sB)]).ready, true, 'resolved: the set goes on by itself');

  // 6. a sheet already cut cannot be held; a piece already released cannot be held; the button's one line says so
  const D = '9100000004';
  pcs[D] = [piece(D, 1, { state: 'cut', sheetLabel: 'GF Sheet 4', sheetId: 'S4' })];
  mark(D);
  const cutRec = sheetOf('S4', [D], { laserDoneAt: Date.now() });
  assert.equal(H.overlay(cutRec), cutRec, 'a cut sheet is never touched'); assert.deepEqual(H.heldBy('S4'), []);
  const ch = H.canHold(D); assert.equal(ch.ok, false); assert.match(ch.why, /GF Sheet 4 is already cut/);
  unmark(D);
  const E = '9100000005';
  pcs[E] = [piece(E, 1, { hand: true, state: 'unnested', sheetId: null })];
  const ch2 = H.canHold(E); assert.equal(ch2.ok, false); assert.match(ch2.why, /already released/);
  pcs[E] = [piece(E, 1, { state: 'unnested', sheetId: null })];
  assert.deepEqual(H.canHold(E), { ok: true, sheets: [] }, 'an order on no sheet has nothing to hold');
  pcs[E] = [piece(E, 1, { sheetId: 'S1' })];
  assert.deepEqual(H.canHold(E).sheets.map(x => x.sheetId), ['S1']);
  // a cut sheet (done once, reopened) keeps its approval and is not held by a later review
  const reopened = sheetOf('S1', [A], { processSeals: [{ how: 'laserDone', at: 5 }] }); mark(A); assert.equal(H.overlay(reopened), reopened); unmark(A);

  // 7. the readiness reading from the page's rows (orderReports): the same held block, and a piece on a cut sheet is not held
  const row = (rid, n, state) => ({ key: `${rid}_1`, state, poolIds: [`${rid}_1_${n}`], order: { receiptId: rid }, line: { title: 'Round Charm', transactionId: '1' }, spec: { quantity: 1 } });
  mark(A);
  const live = sheetOf('S1', [A]);
  let reps = R.orderReports([row(A, 1, 'written')], [live]);
  assert.equal(reps[A].ready, false); assert.equal(reps[A].blocks[0].review, true); assert.match(reps[A].why, /^In Review/);
  const viaRows = R.sheet({ ...live, orderReadiness: Object.fromEntries(Object.entries(reps).map(([k, v]) => [k, R.forSheet(v, 'S1', 'set1')])) }); assert.equal(viaRows.stages.orders, false);
  reps = R.orderReports([row(A, 1, 'written')], [{ ...live, laserDoneAt: 99 }]); assert.equal(reps[A].ready, true, 'a piece on a sheet already cut is not held');
  unmark(A);
  reps = R.orderReports([row(A, 1, 'written')], [live]); assert.equal(reps[A].ready, true);

  // 8. a reader without the module (the server) reads what it always read
  delete globalThis.CharmNestReviewHold; mark(A);
  assert.equal(R.orderReports([row(A, 1, 'written')], [live])[A].ready, true); unmark(A);

  // 9. the server: where it WRITES (a transaction) it reads the review records that are open and holds the sheet; its list reads never ask (fake store, real productionReadiness)
  const fs = require('node:fs'), vm = require('node:vm');
  const source = fs.readFileSync('netlify/functions/charmNestLibrary.js', 'utf8');
  const clone = x => JSON.parse(JSON.stringify(x));
  const rid1 = '9200000001';
  const sheet1 = { id: 'sheet1', setId: 'set1', runId: 'run1', status: 'complete', metal: 'gold', poolIds: [`${rid1}_1_1`], placedCount: 1, verification: { ok: true }, outputs: { ai: { url: 'https://example.com/f.ai' }, preview: { url: 'https://example.com/f.png' } }, orders: [rid1],
    label: { files: [{ path: 'qr.png', url: 'https://example.com/qr.png', payload: rid1, orders: [rid1] }] }, backPool: [{ ...back(`${rid1}_1_1`), sheetId: 'sheet1' }] };
  const data = new Map([['sheets/sheet1', sheet1], ['runs/run1', { lines: { a: { orderId: rid1, state: 'written', poolIds: [`${rid1}_1_1`], engrave: { needed: false, state: 'none', approved: true } } } }]]);
  let reviewReads = 0; const RP = rid => 'Charm_Custom_Orders/' + rid + '_review';
  const reviewDocs = { set: (rid, d) => data.set(RP(rid), d), get: rid => data.get(RP(rid)) };
  const snap = ref => { if (/_review$/.test(ref.path)) reviewReads++; return { id: ref.path.split('/')[1], exists: data.has(ref.path), data: () => clone(data.get(ref.path)) }; };
  const noRuns = { isQuery: true, select: () => noRuns, get: async () => ({ docs: [] }) };
  const colFake = n => ({ where: () => noRuns, doc: id => ({ path: n + '/' + id, get: async () => snap({ path: n + '/' + id }) }) });
  const ctx = vm.createContext({ splitOfSet: async () => [], Readiness: R, process, console: { warn: m => { throw new Error('server warned: ' + m); }, error() {}, log() {} }, str: (v, n) => String(v == null ? '' : v).slice(0, n || 1e6), num: Number, isId: () => true, PREFIX: '', SETS: 'sets', SHEETS: 'sheets', RUNS: 'runs', RUN_LINES: 'run_lines', col: colFake, FV: { serverTimestamp: () => 100 },
    db: { runTransaction: async fn => fn({ get: async ref => ref.isQuery ? ref.get() : snap(ref) }), getAll: async (...refs) => refs.filter(x => x.path).map(snap) } });
  vm.runInContext(source.slice(source.indexOf('async function filingRecords'), source.indexOf('async function op_laserStatus')), ctx);
  const tx = { get: async ref => ref.isQuery ? ref.get() : snap(ref) };
  let rec = (await ctx.readinessRecords([clone(sheet1)]))[0]; assert.equal(rec.laser.ready, true, 'baseline on the server: ready');
  reviewDocs.set(rid1, { key: rid1 + '_review', kind: 'reviewMark', receiptId: rid1, state: 'open', review: { state: 'open', markedBy: 'Name One' } });
  reviewReads = 0; rec = (await ctx.readinessRecords([clone(sheet1)]))[0]; assert.equal(reviewReads, 0, 'a list read never asks for the reviews'); assert.equal(rec.laser.ready, true);
  const recs = [clone(sheet1)]; await ctx.productionReadiness(recs, { tx });
  assert.ok(reviewReads >= 1 && reviewReads <= 2, 'a write gate asks for the orders of the sheet only: ' + reviewReads); assert.equal(R.laserSheet(recs[0]).ready, false, 'a write gate refuses the sheet of an order in Review');
  assert.equal(recs[0].orderReadiness[rid1].ready, false); assert.match(recs[0].orderReadiness[rid1].blocks[0].why, /^In Review: Sent to Review by Name One/); assert.equal(recs[0].orderReadiness[rid1].blocks[0].review, true);
  reviewDocs.get(rid1).review.state = 'done';
  const recs2 = [clone(sheet1)]; await ctx.productionReadiness(recs2, { tx }); assert.equal(R.laserSheet(recs2[0]).ready, true, 'resolved: the write gate lets it through');
  // a read that fails does not stop the shop
  const failing = vm.createContext({ ...ctx, console: { warn() {}, error() {}, log() {} }, col: n => n === 'Charm_Custom_Orders' ? { doc: () => { throw new Error('boom'); } } : colFake(n) });
  vm.runInContext(source.slice(source.indexOf('async function filingRecords'), source.indexOf('async function op_laserStatus')), failing);
  const recs3 = [clone(sheet1)]; await failing.productionReadiness(recs3, { tx }); assert.equal(R.laserSheet(recs3[0]).ready, true, 'fail open: the page still holds it');

  // 10. the server's Approve gate (approveGate): an order in Review on the sheet, or on any sheet of its set, refuses the approving step; resolved lets it through
  const fnText = name => { const i = source.indexOf('async function ' + name); const j = source.indexOf('\n}\n', i); return source.slice(i, j + 3); };
  data.set('sets/setA', { setId: 'setA', sheetIds: ['sheet1', 'sheet2'] });
  data.set('sheets/sheet2', { ...clone(sheet1), id: 'sheet2', orders: ['9200000002'], poolIds: ['9200000002_1_1'] });
  data.set('sheets/sheet1', { ...clone(sheet1), setId: 'setA' });
  const gctx = vm.createContext({ Readiness: R, console: { warn() {}, error() {}, log() {} }, str: (v, n) => String(v == null ? '' : v).slice(0, n || 1e6), num: Number, isId: () => true, SETS: 'sets', SHEETS: 'sheets', col: colFake, db: { getAll: async (...refs) => refs.filter(x => x.path).map(snap) }, setGatesFor: async () => new Map() });
  vm.runInContext(fnText('openReviews') + fnText('approveGate'), gctx);
  reviewDocs.get(rid1).review.state = 'open';
  let refusal = await gctx.approveGate([{ type: 'seal', kind: 'sheet', id: 'sheet1' }]); assert.ok(refusal && refusal.status === 409 && refusal.orderId === rid1 && /in Review/.test(refusal.error), 'the sheet itself: ' + JSON.stringify(refusal));
  refusal = await gctx.approveGate([{ type: 'seal', kind: 'set', id: 'setA' }]); assert.ok(refusal && refusal.orderId === rid1, 'the set: its member sheet holds it');
  refusal = await gctx.approveGate([{ type: 'seal', kind: 'sheet', id: 'sheet2' }]); assert.equal(refusal, null, 'a sheet of another order is not held by it');
  reviewDocs.get(rid1).review.state = 'done';
  assert.equal(await gctx.approveGate([{ type: 'seal', kind: 'sheet', id: 'sheet1' }]), null, 'resolved: approved by itself');
  console.log('review-hold: ok');
})().catch(e => { console.error(e); process.exit(1); });
