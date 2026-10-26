// An order sent to Review that sits on a sheet holds that sheet until it is resolved (charm-nest-review-hold.js + charm-nest-readiness.js + charm-nest-flow.js), on FAKE data only:
// no network, no Firestore, no buyer names. The hold is derived from the review record every time (a stub of CharmNestReviewMark), never remembered.
const assert = require('node:assert/strict');
const R = require('../../charm-nest-readiness.js');
const LF = require('../../charm-nest-flow.js');
const H = require('../../charm-nest-review-hold.js');
globalThis.CharmNestReviewHold = H;

// ── the review record (stub of charm-nest-review-mark.js) and the pieces (stub of OrderPieces) ──
const open = new Set(), markSubs = [], pieceSubs = [], pcs = {};
const swapSet = new Set();   // orders whose picked drawing is not on their piece yet (the bridge's Pool.swapRids answers this on the page)
globalThis.Pool = { swapRids: () => new Set(swapSet), swapWaits: r => swapSet.has(String(r)) };
const doneHow = new Map();   // rid -> how a resolved review was answered ('pick' | 'ok')
globalThis.CharmNestReviewMark = { info: r => open.has(String(r)) ? { rid: String(r), state: 'open' } : doneHow.has(String(r)) ? { rid: String(r), state: 'done', how: doneHow.get(String(r)) } : null, isMarked: r => open.has(String(r)), markedRids: () => [...open], reasonOf: r => open.has(String(r)) ? 'Order held by Name One' : '', onChange: f => { markSubs.push(f); return () => {}; } };
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
  assert.equal(recs[0].orderReadiness[rid1].ready, false); assert.match(recs[0].orderReadiness[rid1].blocks[0].why, /^In Review: Order held by Name One/); assert.equal(recs[0].orderReadiness[rid1].blocks[0].review, true);
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

  // 11. the saved link / "Order changed" card never lifts a piece off a sheet for an order in Review, or one just answered by a pick (bridge Pool.keepsPlace + repoolLine)
  { const K = H.keepsPlace; const rid = '4197740562';
    assert.equal(K(rid), false, 'not in Review: nothing is kept');
    mark(rid); assert.equal(K(rid), true, 'in Review: keeps its place');
    unmark(rid); doneHow.set(rid, 'pick'); assert.equal(K(rid), false, 'answered by a pick: the person decides the swap (the card), the sheet waits for it (section 12)');
    doneHow.set(rid, 'ok'); assert.equal(K(rid), false, 'marked OK: the saved link has nothing to do with it');
    doneHow.delete(rid); assert.equal(K(rid), false); assert.equal(K(''), false); assert.equal(K(null), false);
    const OLD = 'CELESTIAL3-COMET', NEW = 'CELESTIAL 3 - COMET', LID = '1687890868', TITLE = 'Comet Pendant Charm Necklace';
    const O = require('../../charm-nest-orders.js'), keyWorld = require('./_keyWorld.cjs')(require('path').join(__dirname, '../..')), { DESIGNS, world, lineRow } = keyWorld, F = require('./pairs-fixtures.cjs');
    const real = JSON.parse(require('fs').readFileSync(require('path').join(__dirname, 'fixtures/comet-master-entries.json'), 'utf8')).entries;
    const lib = new Map(real.map(e => [e.sku, e])), loose = {}; for (const k of lib.keys()) { const l = O.looseKey(k); loose[l] = l in loose ? '' : k; };
    const ctx = aliases => ({ optionMaps: {}, aliases, noDesign: { patterns: [], skus: [] }, customDone: {}, customRead: {}, customDecided: {}, listingSkus: {}, masterEntry: s => lib.get(String(s || '').toUpperCase()) || null, masterLoose: s => loose[O.looseKey(s)] || '' });
    const line = { transactionId: '5246703786', listingId: LID, sku: 'Celestial3-Comet', title: TITLE, quantity: 1, metalKey: 'silver', metalLabel: 'Silver', personalization: [], buyerMessage: '', variations: [{ name: 'Metal Choice', value: 'Silver' }] };
    DESIGNS[OLD] = () => F.charmOf('ONE-PENDANT'); DESIGNS[NEW] = () => F.charmOf('PAIR-HOOP'); DESIGNS['COMET_9427'] = () => F.charmOf('PAIR-HOOP');
    const worldOf = () => {
      const w = world([OLD, NEW, 'COMET_9427']), reviews = [], timeline = [], toasts = [], calls = [];
      w.entries['COMET_9427'].aiPath = w.entries[NEW].aiPath;   // (one drawing, two names, as the library keeps it)
      w.c.Review = { add: it => reviews.push(it) }; w.c.B.review = { items: reviews }; w.c.Sets = { ofRun: () => [] }; w.c.Gate = { modern: () => false };
      w.c.TL = { line: (row, type, o) => { timeline.push({ type, o }); return true; }, hash: x => String(x).length.toString(36) };
      w.c.window.CharmNestReviewHold = H; return Object.assign(w, { reviews, timeline, toasts, calls });
    };
    const rowOf = () => { const spec = O.interpretLine({ receiptId: rid, updateTs: 1 }, line, ctx({ [LID]: { v: 2, bySku: { [OLD]: NEW }, by: 'X' } })); spec.material = 'gold'; return Object.assign(lineRow(rid, '5246703786', spec.designSku, 1, {}), { spec, line: { transactionId: line.transactionId, listingId: LID, sku: line.sku, title: TITLE } }); };
    const placed = (w, row, onSheet) => { const id = rid + '_5246703786_1'; row.poolIds = [id]; w.c.B.pool.rows.set(id, { poolId: id, state: 'ready', sku: OLD, copy: 1, quantity: 1, charmHash: 'x' }); if (onSheet) { w.pages.gold.charms.push({ poolId: id }); w.pages.gold.draft = true; w.pages.gold.fileBase = 'SS Draft 2'; } return id; };
    const logsOf = w => w.logs.filter(t => /stays on its sheet/.test(t)).length;
    // (a) not in Review: an open sheet is still offered the card, as before
    { const w = worldOf(), row = rowOf(); placed(w, row, true); w.Pool.pinPooled(row); assert.deepEqual([row.changePending, row.repoolChanged, w.reviews.length], [true, true, 1], 'baseline: offered'); }
    // (b) in Review, on an open sheet: no card, nothing flagged, said once in the log and once on the timeline; asked again it says nothing more
    mark(rid);
    { const w = worldOf(), row = rowOf(), id = placed(w, row, true); w.Pool.pinPooled(row); w.Pool.pinPooled(row);
      assert.deepEqual([row.changePending || false, row.repoolChanged || false, w.reviews.length, row.poolIds], [false, false, 0, [id]], 'in Review on a sheet: no card, the piece stays');
      assert.equal(logsOf(w), 1, 'said once in the log'); assert.equal(w.timeline.length, 1); assert.match(w.timeline[0].o.text, /^Saved link: a saved link makes this piece CELESTIAL 3 - COMET, but its piece stays on its sheet because the order was sent to Review/);
      assert.equal(w.pages.gold.charms.length, 1, 'the sheet is untouched'); assert.equal(w.calls.filter(c => /pool|sheet/i.test((c.body && c.body.op) || '')).length, 0, 'nothing written'); }
    // (c) in Review but its piece is not on a sheet yet: the card is offered as before (nothing to lift)
    { const w = worldOf(), row = rowOf(); placed(w, row, false); w.Pool.pinPooled(row); assert.equal(w.reviews.length, 1, 'no piece on a sheet: nothing to keep'); }
    // (d) answered by a pick, piece on an open sheet: the card is offered (a person accepts the swap), and the hold stays until the piece carries the picked charm
    unmark(rid); doneHow.set(rid, 'pick');
    { const w = worldOf(), row = rowOf(); placed(w, row, true); w.Pool.pinPooled(row); assert.deepEqual([row.repoolChanged || false, w.reviews.length], [true, 1], 'after a pick: the card is offered');
      w.c.window.CharmNestReviewMark = globalThis.CharmNestReviewMark; w.c.B.orders.rows = [row];
      assert.deepEqual([...w.Pool.swapRids()], [rid], 'a pick whose drawing is not on the piece: the swap is waited for'); assert.equal(w.Pool.swapWaits(rid), true); assert.equal(w.Pool.swapWaits('4100000000'), false);
      // the picked drawing is the one on the piece (or the same under another name): nothing to wait for
      for (const sku of [NEW, 'COMET_9427']) { const w2 = worldOf(), r2 = rowOf(); const id2 = placed(w2, r2, true); w2.c.B.pool.rows.get(id2).sku = sku; w2.c.window.CharmNestReviewMark = globalThis.CharmNestReviewMark; w2.c.B.orders.rows = [r2]; assert.equal(w2.Pool.swapRids().size, 0, `${sku} is already on the piece: the hold is released as before`); }
      // not on a sheet (nothing to cut), Review marked OK, an open Review, a piece superseded by the swap: no swap to wait for
      { const w3 = worldOf(), r3 = rowOf(); placed(w3, r3, false); w3.c.window.CharmNestReviewMark = globalThis.CharmNestReviewMark; w3.c.B.orders.rows = [r3]; assert.equal(w3.Pool.swapRids().size, 0, 'a piece on no sheet'); }
      doneHow.set(rid, 'ok'); assert.equal(w.Pool.swapRids().size, 0, 'marked OK is not a pick'); doneHow.set(rid, 'pick');
      mark(rid); assert.equal(w.Pool.swapRids().size, 0, 'an open Review is the Review hold, not a swap'); unmark(rid); doneHow.set(rid, 'pick');
      w.c.B.pool.rows.get(row.poolIds[0]).state = 'superseded'; w.c.B.pool.rows.set(rid + '_5246703786_9', { poolId: rid + '_5246703786_9', state: 'ready', sku: NEW, copy: 1, quantity: 1 }); row.poolIds = [rid + '_5246703786_9']; w.pages.gold.charms.push({ poolId: row.poolIds[0] });
      assert.equal(w.Pool.swapRids().size, 0, 'the piece now carries the picked charm: released'); }
    doneHow.set(rid, 'ok');
    { const w = worldOf(), row = rowOf(); placed(w, row, true); w.Pool.pinPooled(row); assert.equal(w.reviews.length, 1, 'marked OK: the card is offered as before'); }
    // (e) a card already offered, then the order goes to Review (or is picked): Accept the new order keeps the piece where it is (never lifted, never nested again)
    for (const state of ['review']) {
      const w = worldOf(), row = rowOf(), id = placed(w, row, true); doneHow.delete(rid); unmark(rid); w.Pool.pinPooled(row); assert.equal(row.repoolChanged, true, 'offered first');
      if (state === 'review') mark(rid); else doneHow.set(rid, 'pick');
      const src = keyWorld.src, a = src.indexOf('async function repoolLine(row) {'), b = src.indexOf('\n  const by = () => employeeName()', a); assert(a > 0 && b > a, 'repoolLine is where the test expects it');
      const updates = []; Object.assign(w.c, { CNListActivity: { touch() {} }, syncOrderItems() {}, renderRail() {}, updateTopSub() {}, refreshAllCards() {}, OrderWin: { isOpen: () => false }, toast: m => w.toasts.push(m), sheetName: sh => sh.fileBase, add() {}, RunCtl: { poke() {} }, Orders: { interpretAll() {}, render() {}, keepRest() {} } });
      w.c.Pool = Object.assign({}, w.Pool, { update: async (ids, patch) => { updates.push({ ids, patch }); } });
      w.c.window.Pool = w.c.Pool; w.c.B.run = null; w.c.Sets = { ofRun: () => [] }; w.c.Gate = { modern: () => false }; w.c.allSheets = () => Object.values(w.pages);
      require('vm').runInContext(src.slice(a, b) + '\nwindow.__repoolLine = repoolLine;', w.c);
      row.hold = null; await w.c.window.__repoolLine(row);
      assert.deepEqual([row.poolIds, row.repoolChanged, row.changePending, updates.length, w.pages.gold.charms.length], [[id], false, false, 0, 1], `${state}: the piece is not lifted, not superseded, not made up again`);
      assert.equal(w.toasts.length, 1); assert.match(w.toasts[0], /^Kept on its sheet with its old drawing: the order was sent to Review$/); assert.equal(row.state !== 'pulled', true, 'it was not made up again (no re-pool)');
    }
    // (f) a piece cut or released is never touched: the old held-by-hand path still answers it
    unmark(rid); doneHow.delete(rid);
  }

  // 12. SWAP: the Review was answered by picking another charm, the piece on the open sheet still has the old drawing: the sheet stays held, in plain words, until the piece carries the pick
  { const C = '9100000003'; pcs[C] = [piece(C, 1)]; const sC = sheetOf('S1', [C]);
    doneHow.set(C, 'pick'); assert.deepEqual(H.heldBy('S1'), [], 'a pick whose drawing is already on the piece holds nothing (released as before)'); assert.equal(H.overlay(sC), sC);
    const heardS = []; const offS = H.onChange(x => heardS.push(x)); H._check(); mark(C); H._check();
    unmark(C); swapSet.add(C); H._check();
    const rowsS = H.heldBy('S1'); assert.equal(rowsS.length, 1); assert.equal(rowsS[0].rid, C); assert.equal(rowsS[0].swap, true); assert.equal(rowsS[0].reason, 'New drawing chosen, swap it on the sheet'); assert.deepEqual(rowsS[0].poolIds, [`${C}_1_1`]);
    assert.ok(heardS.length >= 2 && heardS[heardS.length - 1].rids.includes(C), 'Review to swap is a change the Library hears: ' + JSON.stringify(heardS));
    assert.equal(H.isHeld('S1'), true); assert.equal(H.reasonFor('S1'), `Order ${C}: new drawing chosen, swap it on the sheet`); assert.deepEqual(H.sheetsHeldBy(C).map(x => x.sheetId), ['S1']); assert.equal(H.reason(C), 'New drawing chosen, swap it on the sheet'); assert.equal(H.inReview(C), false, 'it is not in Review');
    const oS = H.overlay(sC); assert.notEqual(oS, sC); assert.equal(R.sheet(oS).ready, false); assert.equal(R.laserSheet(oS).ready, false); assert.equal(R.sheet(oS).stages.orders, false);
    const abS = R.approveBlock(oS); assert.ok(abS && abS.review && abS.why === `new drawing chosen, swap it on the sheet: order ${C}`, JSON.stringify(abS));
    const issS = R.issues(oS, { steps: ['orders'], perOrder: true }).filter(i => i.orderId === C); assert.equal(issS.length, 1); assert.equal(issS[0].key, 'held'); assert.match(issS[0].why, /^New drawing chosen, swap it on the sheet/); assert.equal(issS[0].pieces[0].swap, true);
    const exS = R.explain(oS, { rows: [] }).steps.find(t => t.key === 'orders'); assert.match(exS.detail, /new drawing chosen/i); assert.doesNotMatch(exS.detail, /in Review|other pieces/);
    const gateS = R.setGate({ setId: 'set1', sheetIds: ['S1'] }, [oS]); assert.equal(gateS.ready, false); assert.match(gateS.reason, /new drawing chosen/);
    const planS = LF.core.planMove({ sheets: { S1: { ...sC, laser: R.laserSheet(sC) } }, sets: { set1: { setId: 'set1', seq: 1, sheetIds: ['S1'], status: 'labelled' } }, runs: {}, live: {} }, { kind: 'sheet', id: 'S1', to: { area: 'laser' } }, { by: 'Name One', rows: [] }, undefined);
    const heldFlow = LF.core.planMove({ sheets: { S1: H.overlayFlow({ ...sC, laser: R.laserSheet(sC) }) }, sets: { set1: { setId: 'set1', seq: 1, sheetIds: ['S1'], status: 'labelled' } }, runs: {}, live: {} }, { kind: 'sheet', id: 'S1', to: { area: 'laser' } }, { by: 'Name One', rows: [] });
    assert.equal(heldFlow.ok, false); const needS = heldFlow.needs.find(n => n.key === 'orders'); assert.ok(needS && /new drawing to swap in/.test(needS.label), JSON.stringify(heldFlow.needs.map(n => n.label))); assert.equal(planS.needs.filter(n => n.key === 'orders').length, 0, 'baseline without the hold');
    // the piece carries the picked charm (a person accepted the card, or swapped it by hand): the sheet goes on by itself
    swapSet.delete(C); H._check(); assert.deepEqual(H.heldBy('S1'), []); assert.equal(H.overlay(sC), sC); assert.equal(R.sheet(H.overlay(sC)).ready, true);
    assert.ok(heardS[heardS.length - 1].rids.includes(C), 'the release is heard'); offS(); doneHow.delete(C); delete pcs[C];
    // a piece already cut is not held by it, whatever it carries
    swapSet.add(C); pcs[C] = [piece(C, 1, { state: 'cut' })]; assert.deepEqual(H.heldBy('S1'), [], 'a sheet already cut cannot be held'); swapSet.delete(C); delete pcs[C];
    // both in one sheet: the wording names each in its own words
    mark(A); pcs[A] = [piece(A, 1)]; swapSet.add(C); pcs[C] = [piece(C, 1, { poolId: `${C}_1_1` })]; doneHow.set(C, 'pick');
    assert.equal(H.reasonFor('S1'), `Order ${A} is in Review; Order ${C}: new drawing chosen, swap it on the sheet`); unmark(A); swapSet.delete(C); delete pcs[A]; delete pcs[C]; doneHow.delete(C); }
  // 13. ADV-REVIEW (15 Oct 2026): an order cancelled while it sat in Review (every line gone) holds nothing: the Review tab draws no card for it, so nobody could resolve it; undone, it holds again
  { const D = '9100000004'; pcs[D] = [piece(D, 1)]; const sD = sheetOf('S1', [D]); mark(D);
    assert.equal(H.isHeld('S1'), true, 'in Review: held (no line of it is in the page yet: the pull is not read, as after a reload)');
    const line = (rid, state) => ({ key: rid + '_1', state, order: { receiptId: rid } });
    globalThis.Orders = { rows: () => [line(D, 'written'), line(A, 'gone')] };
    assert.equal(H.isHeld('S1'), true, 'a live line of it: held'); assert.equal(H.inReview(D), true);
    globalThis.Orders = { rows: () => [line(D, 'gone'), line(A, 'written')] };
    assert.deepEqual(H.heldBy('S1'), [], 'every line gone (cancelled): nothing is held'); assert.equal(H.isHeld('S1'), false); assert.equal(H.reasonFor('S1'), '');
    assert.deepEqual(H.sheetsHeldBy(D), []); assert.equal(H.reason(D), ''); assert.equal(H.inReview(D), false); assert.equal(H.keepsPlace(D), false); assert.equal(H.overlay(sD), sD, 'the record comes back as it is'); assert.equal(R.sheet(H.overlay(sD)).ready, true);
    globalThis.Orders = { rows: () => [line(D, 'gone'), line(D, 'written')] };
    assert.equal(H.isHeld('S1'), true, 'one line gone, one live: still held by the live one');
    globalThis.Orders = { rows: () => [line(D, 'gone')] }; assert.equal(H.isHeld('S1'), false);
    globalThis.Orders = { rows: () => [line(D, 'pulled')] }; assert.equal(H.isHeld('S1'), true, 'the cancel is undone (its lines are live again): held again, with its card');
    delete globalThis.Orders; unmark(D); delete pcs[D]; }
  console.log('review-hold: ok');
})().catch(e => { console.error(e); process.exit(1); });
