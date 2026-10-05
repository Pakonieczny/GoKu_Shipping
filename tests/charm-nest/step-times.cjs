// When each step of the Library's rail was completed (Paul, 5 Oct 2026, 13:16 UTC: "Please add the date and time each step was completed on.").
// The small card over a rail circle says it for every DONE step ("Oct 3, 11:40 AM · by Ana"), from the sheet's own record. Three parts, one file:
//  A · the shared reading (charm-nest-readiness.js: stepFacts, stepsRecord, stepStamps, stepTimes), pure: what "completed" means for each step, the latest
//      completion wins and the first stays in the record, an older sheet with nothing on record says "not recorded" (null) and never a time made up;
//  B · the server, the REAL charmNestLibrary handler over the in-memory shop: the pass that already records the process seals (laserStatus with
//      recordSeals) appends each step's completion, never removes or rewrites one (a step that goes back and completes again adds one), a stale save or a
//      plain read changes nothing, the live read never records, page and server read the same record to the same times;
//  C · the page, in Chromium at 1440 and 390 px around the REAL rail and card: each done step's card says its own time (and who), a step not done says
//      none, an older sheet says "Time not recorded", the card keeps its calm width, and the aria-label says the same words; screenshots (SHOTS=<dir>).
// Mutants (a pass that records nothing; a card or a reading that shows the first time instead of the latest; a pass that overwrites the earlier stamps;
// an older sheet given a time it never had; a live read that records) must each be caught by this very file.
//   node tests/charm-nest/step-times.cjs [playwright dir]     (PW_DIR=…, CHROMIUM=…; SHOTS=<dir> saves screenshots; NO_BROWSER=1 skips part C)
'use strict';
const fs = require('fs'), path = require('path'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');
const R = require('../../charm-nest-readiness.js');
const F = require('./library-issues-fixture.cjs');
const { start } = require('./bridge-server.cjs');
const SHOTS = process.env.SHOTS || '';
const sleep = ms => new Promise(r => setTimeout(r, ms));
const T = (h, m, day = 3) => Date.UTC(2026, 9, day, h, m);                  // Oct 3 2026, UTC (the shop's clock is four hours behind: 13:12 UTC is 9:12 AM there)
const SHOP = (ms, year) => new Date(ms).toLocaleString('en-US', { timeZone: 'America/Toronto', month: 'short', day: 'numeric', ...(year ? { year: 'numeric' } : {}), hour: 'numeric', minute: '2-digit' }).replace(/ /g, ' ');
const API = ['stepFacts', 'stepStamps', 'stepsRecord', 'stepTimes'];
const REAL = Object.fromEntries(API.map(k => [k, R[k]]));
const ev = (step, at, by = '', n = 0) => ({ id: `${step}-${at}-${n}`, step, at, by });
const clone = x => JSON.parse(JSON.stringify(x));

/* the shared reading, as a module of its own (a mutant is this file's text, changed, loaded the same way) */
function load(mutate) {
  let src = fs.readFileSync(path.join(root, 'charm-nest-readiness.js'), 'utf8');
  if (mutate) { const out = mutate(src); assert.notEqual(out, src, 'a mutant must change the source'); src = out; }
  const m = { exports: {} }; new Function('module', src)(m); return m.exports;
}
function install(api) { for (const k of API) R[k] = api[k]; }
function restore() { for (const k of API) R[k] = REAL[k]; }

// a sheet whose every step is done, with real approval times on its backs (the last one, at 15:40 UTC, by Ana)
function done(id, o = {}) {
  const s = F.sheet(id, { n: 3, base: o.base || 5200000, seq: 1, index: 1, setId: o.setId || 'set-' + id, ...(o.sheet || {}) });
  s.backPool = s.poolIds.map((p, i) => ({ ...F.back(p, id), approvedAt: T(14 + (i === 2 ? 1 : 0), i === 2 ? 40 : 10 * i), approvedBy: i === 2 ? 'Ana' : 'Seth' }));
  s.createdAt = o.created ?? R.STEPS_FROM + 60000; s.updatedAt = o.updated ?? T(13, 12);
  return s;
}

/* ───────────────────────────────────────── A · the shared reading ───────────────────────────────────────── */
function partA() {
  const NOW = R.STEPS_FROM + 6 * 3600 * 1000;
  // what "completed" is for each recorded step: the rail's own tests
  { const s = done('a1'); assert.deepEqual(R.stepFacts(s), { nesting: true, engraving: true, orders: true });
    assert.equal(R.stepFacts({ ...s, verification: { ok: false } }).nesting, false, 'nesting: the layout must be verified');
    assert.equal(R.stepFacts({ ...s, draft: true }).nesting, false, 'nesting: a draft has not left for its set');
    assert.equal(R.stepFacts({ ...s, laserHold: { at: 5, by: 'Paul' } }).nesting, true, 'nesting: a hold is no un-nesting (the sheet is as nested as it was)');
    assert.equal(R.stepFacts({ ...s, backPool: s.backPool.slice(0, 2) }).engraving, false, 'engraving: every back must be approved');
    assert.equal(R.stepFacts({ ...s, orderReadiness: { ...s.orderReadiness, [s.orders[0]]: { ready: false } } }).orders, false, 'order check: another piece holds an order'); }

  // a sheet seen from its start: each step is stamped when first seen done, at the time its own record gives (never later than the pass)
  { const s = done('a2'); delete s.stepState; const r = R.stepsRecord(s, NOW);
    assert.deepEqual(r.added.map(x => x.step), ['nesting', 'engraving', 'orders']);
    const by = k => r.added.find(x => x.step === k);
    assert.equal(by('nesting').at, T(13, 12), 'nesting: the record\'s own last update, which the save that completed it was'); assert.equal(by('nesting').by, '');
    assert.equal(by('engraving').at, T(15, 40), 'engraving: the last back\'s own approval time, not the pass'); assert.equal(by('engraving').by, 'Ana', 'engraving: the sorter\'s typed name on that approval');
    assert.equal(by('orders').at, NOW, 'order check: when the pass first saw the gate clear'); assert.equal(by('orders').by, '');
    assert.deepEqual(r.state, { nesting: true, engraving: true, orders: true });
    assert.deepEqual(r.stamps, r.added, 'the new sheet\'s whole record is its new stamps');
    for (const e of r.added) assert(e.at <= NOW && e.id.startsWith(e.step + '-' + e.at), 'a stamp carries its step, its time and a unique id');
    // a nesting time later than the pass is no time: the pass's own is used
    assert.equal(R.stepsRecord({ ...s, updatedAt: NOW + 5000 }, NOW).added.find(x => x.step === 'nesting').at, NOW); }

  // an older sheet seen for the first time: its steps are taken as they stand and NO stamp is written (no time is made up for what it did before)
  { const s = done('a3', { created: R.STEPS_FROM - 86400000 }); delete s.stepState; const r = R.stepsRecord(s, NOW);
    assert.equal(r.baseline, true); assert.deepEqual(r.added, [], 'an older sheet is not given a time it never had'); assert.deepEqual(r.state, { nesting: true, engraving: true, orders: true }); assert.deepEqual(r.stamps, []);
    assert.equal(R.stepTimes(s).nesting, null, 'its nesting says "not recorded", not a guess');
    // ... and a step of it that completes after that is stamped
    const later = R.stepsRecord({ ...s, stepState: { nesting: true, engraving: false, orders: true }, stepStamps: [] }, NOW);
    assert.deepEqual(later.added.map(x => x.step), ['engraving']); assert.equal(later.added[0].at, T(15, 40)); }

  // append-only: a step that goes back and completes again adds a stamp; none is ever removed or rewritten
  { let s = { ...done('a4'), stepState: { nesting: true, engraving: true, orders: true }, stepStamps: [ev('nesting', T(13, 12)), ev('engraving', T(15, 40), 'Ana', 1), ev('orders', T(16, 30), '', 2)] };
    const first = clone(s.stepStamps);
    assert.equal(R.stepsRecord(s, NOW), null, 'nothing changed, nothing is written');
    const back = { ...s, backPool: s.backPool.slice(0, 2) }, r1 = R.stepsRecord(back, NOW);
    assert.equal(r1.flipped, true); assert.deepEqual(r1.added, [], 'going back adds no stamp'); assert.deepEqual(r1.stamps, first, 'and removes none'); assert.equal(r1.state.engraving, false);
    const again = { ...back, stepState: r1.state, stepStamps: r1.stamps, backPool: s.backPool.map((b, i) => i === 2 ? { ...b, approvedAt: T(19, 5), approvedBy: 'Seth' } : b) };
    const r2 = R.stepsRecord(again, NOW);
    assert.deepEqual(r2.added.map(x => [x.step, x.at, x.by]), [['engraving', T(19, 5), 'Seth']], 'completing again adds a new stamp, at the new approval');
    assert.deepEqual(r2.stamps.slice(0, first.length), first, 'every earlier stamp is kept exactly as it was');
    assert.equal(r2.stamps.filter(x => x.step === 'engraving').length, 2);
    const t = R.stepTimes({ ...again, stepState: r2.state, stepStamps: r2.stamps });
    assert.equal(t.engraving.at, T(19, 5), 'the card shows the MOST RECENT completion'); assert.equal(t.engraving.by, 'Seth'); assert.equal(t.engraving.first, T(15, 40), 'the first completion is kept in the record');
    // a stamp is never reordered away by a later read: the reading is by time, the record by append
    assert.deepEqual(R.stepStamps({ stepStamps: [...r2.stamps].reverse() }).map(x => x.at), R.stepStamps({ stepStamps: r2.stamps }).map(x => x.at)); }

  // a sheet already cut has nothing more to record; a sheet that needs no back engraving has no engraving to time
  { const cut = { ...done('a5'), laserDoneAt: T(18, 5), stepState: { nesting: true, engraving: false, orders: true } }; assert.equal(R.stepsRecord(cut, NOW), null);
    const plain = done('a6'); plain.backPool = []; plain.engraving = Object.fromEntries(plain.poolIds.map(p => [p, { needed: false, state: 'none', approved: true }])); delete plain.stepState;
    const r = R.stepsRecord(plain, NOW); assert.deepEqual(r.added.map(x => x.step), ['nesting', 'orders'], 'no engraving was needed: no engraving stamp'); assert.equal(r.state.engraving, true);
    assert.deepEqual(R.stepTimes(plain).engraving, { plain: true }, 'and the card has no time line for it'); }

  // a record can only grow so far: past the cap the pass stops adding (nothing is removed), the state still follows
  { const many = Array.from({ length: 300 }, (_, i) => ev('orders', T(12, 0) + i, '', i)), s = { ...done('a7'), stepState: { nesting: true, engraving: true, orders: false }, stepStamps: many };
    const r = R.stepsRecord(s, NOW); assert.deepEqual(r.added, []); assert.equal(r.stamps.length, 300); assert.equal(r.state.orders, true); }

  // what each circle shows
  { const s = { ...done('a8'), laserDoneAt: T(18, 5), laserDoneBy: 'Paul', processSeals: [{ id: 'r1', how: 'laserReady', at: T(16, 31), by: 'System' }, { id: 'd1', how: 'laserDone', at: T(18, 5), by: 'Paul' }],
      stepStamps: [ev('nesting', T(13, 12)), ev('engraving', T(15, 40), 'Ana', 1), ev('orders', T(16, 30), '', 2)] }, t = R.stepTimes(s);
    assert.deepEqual([t.nesting.at, t.engraving.at, t.orders.at, t.laser.at, t.completed.at], [T(13, 12), T(15, 40), T(16, 30), T(18, 5), T(18, 5)]);
    assert.equal(t.engraving.by, 'Ana'); assert.equal(t.laser.by, 'Paul'); assert.equal(t.nesting.by, '');
    assert.notEqual(t.laser.at, t.orders.at, 'the cut is not the moment the order check cleared');
    assert.deepEqual(t.laser, t.completed, 'marking a sheet cut is what completes it: one press, so these two circles show the same moment');
    assert.equal(t.orders.recorded, true); assert.equal(R.stepTimes({ ...s, stepStamps: [] }).orders.at, T(16, 31), 'an older sheet: the moment nothing held it (its ready seal)');
    assert.equal(R.stepTimes({ ...s, stepStamps: [] }).orders.recorded, false); }
  // an older sheet: only what its own records hold, and "System" is no person
  { const s = done('a9'); s.processSeals = [{ id: 'x', how: 'laserReady', at: T(16, 0), by: 'Seth' }];
    const t = R.stepTimes(s); assert.equal(t.nesting, null); assert.deepEqual([t.engraving.at, t.engraving.by, t.engraving.recorded], [T(15, 40), 'Ana', false]); assert.equal(t.orders.at, T(16, 0)); assert.equal(t.orders.by, '', 'the ready seal\'s signer did not do the order check');
    assert.equal(t.laser, null, 'a sheet that was not cut has no cut time'); assert.equal(t.completed, null);
    const odd = done('a10'); odd.backPool = odd.backPool.map(b => ({ ...b, approvedAt: 10 })); assert.equal(R.stepTimes(odd).engraving, null, 'a time before 2020 is no time');
    const part = done('a11'); part.backPool[0] = { ...part.backPool[0], approvedAt: null, approvedBy: null }; part.engraving = { [part.poolIds[0]]: { needed: true, state: 'approved', approved: true } };
    assert.equal(R.stepTimes(part).engraving, null, 'one approval without its time: the last one cannot be known, so none is shown');
    assert.deepEqual(R.stepTimes({ stepStamps: [ev('engraving', T(10, 0), 'System')], ...s }).engraving.by, '', 'System is no person'); }
  // the reading never changes what it is given, and uses no network
  { const s = done('a12'); const before = JSON.stringify(s); R.stepTimes(s); R.stepsRecord(s, NOW); R.stepFacts(s); assert.equal(JSON.stringify(s), before);
    const src = fs.readFileSync(path.join(root, 'charm-nest-readiness.js'), 'utf8'), body = src.slice(src.indexOf('const STEP_LOG='), src.indexOf('function seal('));
    assert(!/\b(fetch|await|XMLHttpRequest|db\.|api\()/.test(body), 'the step reading makes no read and no call: it is pure'); }
}

/* ───────────────────────────────────────── B · the server ───────────────────────────────────────── */
const SHEETS = 'Charm_Nest_Sheets', SETS = 'Charm_Nest_Sets', RUNS = 'Charm_Nest_Runs';
const ts = ms => ({ toMillis: () => ms });
const PIC = 'data:image/svg+xml,' + encodeURIComponent('<svg xmlns="http://www.w3.org/2000/svg" width="30" height="20"><rect width="30" height="20" fill="#fff"/></svg>');
const A = '5100000001', B = '5100000002', C = '5100000003';
const backOf = (poolId, sheetId, at, by) => ({ poolId, sheetId, approvedAt: at, approvedBy: by, verified: { geometry: { ok: true }, file: { ok: true } }, outputs: { ai: { path: poolId + '.ai', url: 'https://example.com/' + poolId + '.ai' } } });
function seedSteps(srv, { created, updated, verified = false, id = 'steps-1', set = 'set-steps' }) {
  const { st } = srv, pool = [A + '_1_1', B + '_1_1'], line = o => ({ orderId: o, state: 'written', quantity: 1, poolIds: [o + '_1_1'], engrave: { needed: true, state: 'review', approved: false } });
  st.put(RUNS, 'run-steps', { runId: 'run-steps', lines: { [A + '_1']: line(A), [B + '_1']: line(B) } });
  st.put(SHEETS, id, { id, setId: set, setSeq: 1, sheetIndex: 1, runId: 'run-steps', metal: 'gold', day: '2026-10-05', status: 'complete', saving: false, placedCount: 2, charmCount: 2, density: .7, stock: { wIn: 6, hIn: 4.5 },
    poolIds: pool, orders: [A, B], listings: [], verification: { ok: verified }, outputs: { ai: { path: id + '.ai', url: 'https://example.com/' + id + '.ai' }, preview: { path: id + '.png', url: PIC } },
    label: { files: [{ path: id + '-qr.png', url: PIC, payload: A, orders: [A, B] }] }, backPool: [], createdAt: ts(created), updatedAt: ts(updated) });
  st.put(SETS, set, { setId: set, seq: 1, day: '2026-10-05', runId: 'run-steps', sheetIds: [id], materials: ['gold'], orders: {}, status: 'labelled', updatedAt: ts(updated), createdAt: ts(created) });
}
async function partB() {
  const srv = await start({ receipts: [] }), { st } = srv, realNow = Date.now; let clock = 0; Date.now = () => clock;
  const call = async b => { const r = await fetch(srv.sorterOrigin + '/.netlify/functions/charmNestLibrary', { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(b) }); return { status: r.status, ...await r.json() }; };
  const pass = (at, ids = ['steps-1'], by = 'Paul') => { clock = at; return call({ op: 'laserStatus', sheetIds: ids, recordSeals: true, by }); };
  const doc = (id = 'steps-1') => st.doc(SHEETS, id), stamps = (id, k) => (doc(id).stepStamps || []).filter(x => !k || x.step === k);
  try {
    // ── a sheet seen from its start (created after the recording began) ──
    const T0 = R.STEPS_FROM + 10 * 60000, T1 = T0 + 5 * 60000;
    seedSteps(srv, { created: T0, updated: T0, verified: false });
    // the live read (and a plain read) changes nothing, even where a pass would record
    const before = JSON.stringify([...st.docs.entries()]);
    clock = T0 + 30000; await call({ op: 'laserStatus', sheetIds: ['steps-1'], recordSeals: false, wantRevs: true }); await call({ op: 'laserStatus', sheetIds: ['steps-1'] }); await call({ op: 'listSheets' });
    assert.equal(JSON.stringify([...st.docs.entries()]), before, 'a read (the live read of the Library, a list) records nothing');
    let r = await pass(T0 + 60000); assert.equal(r.status, 200);
    assert.deepEqual(doc().stepState, { nesting: false, engraving: false, orders: true }, 'the pass notes which steps stand done');
    assert.deepEqual(stamps().map(x => [x.step, x.at, x.by]), [['orders', T0 + 60000, '']], 'order check: stamped when the pass first saw the gate clear, no person');
    // nesting completes (the save that completed it is the sheet's last write): stamped at that write, not at the pass
    st.put(SHEETS, 'steps-1', { verification: { ok: true }, updatedAt: ts(T1) });
    const prior = clone(stamps()); await pass(T1 + 30000);
    assert.deepEqual(stamps('steps-1', 'nesting').map(x => [x.at, x.by]), [[T1, '']], 'nesting: the sheet record\'s own update, not the later pass');
    assert.deepEqual(stamps().slice(0, prior.length), prior);
    // the backs are approved one by one: engraving completes with the LAST approval, by the person who gave it
    st.put(SHEETS, 'steps-1', { backPool: [backOf(A + '_1_1', 'steps-1', T1 + 60000, 'Seth')], updatedAt: ts(T1 + 60000) }); await pass(T1 + 70000);
    assert.equal(stamps('steps-1', 'engraving').length, 0, 'one back of two approved: engraving is not done');
    st.put(SHEETS, 'steps-1', { backPool: [backOf(A + '_1_1', 'steps-1', T1 + 60000, 'Seth'), backOf(B + '_1_1', 'steps-1', T1 + 120000, 'Ana')], updatedAt: ts(T1 + 120000) }); await pass(T1 + 3600000);
    assert.deepEqual(stamps('steps-1', 'engraving').map(x => [x.at, x.by]), [[T1 + 120000, 'Ana']], 'engraving: the last back\'s own approval time and signer, however late the pass came');
    const afterFirst = clone(stamps());
    // the engraving is reopened and approved again later: nothing is removed, the new completion is added, the card shows the latest, the first stays
    st.put(SHEETS, 'steps-1', { backPool: [backOf(A + '_1_1', 'steps-1', T1 + 60000, 'Seth')], updatedAt: ts(T1 + 4000000) }); await pass(T1 + 4000000);
    assert.deepEqual(clone(stamps()), afterFirst, 'going back adds nothing and removes nothing'); assert.equal(doc().stepState.engraving, false);
    st.put(SHEETS, 'steps-1', { backPool: [backOf(A + '_1_1', 'steps-1', T1 + 60000, 'Seth'), backOf(B + '_1_1', 'steps-1', T1 + 5000000, 'Seth')], updatedAt: ts(T1 + 5000000) }); await pass(T1 + 5000010);
    assert.deepEqual(stamps('steps-1', 'engraving').map(x => [x.at, x.by]), [[T1 + 120000, 'Ana'], [T1 + 5000000, 'Seth']], 'completed again: both stamps are in the record');
    assert.deepEqual(clone(stamps()).slice(0, afterFirst.length), afterFirst, 'the earlier stamps are exactly as they were');
    let rec = clone(doc()), t = R.stepTimes(rec);
    assert.equal(t.engraving.at, T1 + 5000000, 'the card shows the most recent completion'); assert.equal(t.engraving.first, T1 + 120000, 'the first is kept');
    // a new order lands (a piece of it is on no sheet yet) and goes: the order check completes again
    const lines = st.doc(RUNS, 'run-steps').lines, six = id => id + '_1';
    st.put(RUNS, 'run-steps', { lines: { ...lines, [six(C)]: { orderId: C, state: 'pooled', quantity: 2, poolIds: [C + '_1_1', C + '_1_2'], engrave: { needed: true, state: 'approved', approved: true } } } });
    st.put(SHEETS, 'steps-1', { poolIds: [...doc().poolIds, C + '_1_1'], orders: [A, B, C], placedCount: 3, charmCount: 3, backPool: [...doc().backPool, backOf(C + '_1_1', 'steps-1', T1 + 5000100, 'Seth')] });
    const ordersBefore = stamps('steps-1', 'orders').length; await pass(T1 + 5100000);
    assert.equal(doc().stepState.orders, false, 'order check: another piece of the new order holds the sheet'); assert.equal(stamps('steps-1', 'orders').length, ordersBefore);
    st.put(RUNS, 'run-steps', { lines: { ...lines, [six(C)]: { orderId: C, state: 'pooled', quantity: 2, poolIds: [C + '_1_1', C + '_1_2'], noDesign: true } } }); await pass(T1 + 5200000);
    assert.deepEqual(stamps('steps-1', 'orders').map(x => x.at), [T0 + 60000, T1 + 5200000], 'the order check completed again: the first stamp is kept, the new one added');

    // the live read still records nothing, a stale save cannot clear the record, and the steps are no seals (never drawn)
    const kept = clone([doc().stepStamps, doc().stepState, doc().processSeals]);
    clock = T1 + 5300000; await call({ op: 'laserStatus', sheetIds: ['steps-1'], recordSeals: false, wantRevs: true });
    await call({ op: 'putSheet', sheet: { id: 'steps-1', stepStamps: [], stepState: {}, processSeals: [], note: 'stale page' } });
    assert.deepEqual(clone([doc().stepStamps, doc().stepState, doc().processSeals]), kept, 'a stale page save cannot replace or clear the steps');
    for (const s of doc().processSeals || []) assert(['laserReady', 'laserDone'].includes(s.how), 'no step is written as a seal: ' + s.how);
    assert.deepEqual(R.processStamps(doc()).map(x => x.how).filter(h => !['laserReady', 'laserDone'].includes(h)), []);

    // page and server read the same record to the same times
    clock = T1 + 5400000; const ans = await call({ op: 'laserStatus', sheetIds: ['steps-1'] }), served = ans.sheets.find(x => x.id === 'steps-1');
    assert.deepEqual(R.stepTimes(served), R.stepTimes(clone(doc())), 'the page (the served record) and the server (the stored one) read the same times');
    assert.deepEqual(served.stepStamps, R.stepStamps(doc()), 'the record is served whole'); assert.deepEqual(served.stepState, doc().stepState);
    const lst = await call({ op: 'listSheets' }); assert.deepEqual(R.stepTimes(lst.sheets.find(x => x.id === 'steps-1')), R.stepTimes(served), 'the Library\'s list reads the same');

    // marking it cut (and taking that back) leaves every stamp as it is; the cut is the time of Laser cutting and Completed
    st.put(SHEETS, 'steps-1', { backPool: [backOf(A + '_1_1', 'steps-1', T1 + 60000, 'Seth'), backOf(B + '_1_1', 'steps-1', T1 + 5000000, 'Seth')], poolIds: [A + '_1_1', B + '_1_1'], orders: [A, B], placedCount: 2, charmCount: 2 });
    await pass(T1 + 5500000); const stampsBefore = clone(doc().stepStamps);
    clock = T1 + 6000000; r = await call({ op: 'laserDone', kind: 'sheet', id: 'steps-1', done: true, stage: 'laser', by: 'Paul' }); assert.equal(r.status, 200, JSON.stringify(r).slice(0, 200));
    assert.deepEqual(clone(doc().stepStamps), stampsBefore, 'marking it cut removes and rewrites no stamp');
    await pass(T1 + 6100000); assert.deepEqual(clone(doc().stepStamps), stampsBefore, 'a cut sheet has nothing more to record');
    t = R.stepTimes(clone(doc())); assert.deepEqual([t.laser.at, t.laser.by, t.completed.at], [T1 + 6000000, 'Paul', T1 + 6000000]); assert.notEqual(t.laser.at, t.orders.at);
    clock = T1 + 6200000; await call({ op: 'laserDone', kind: 'sheet', id: 'steps-1', done: false, stage: 'laser', by: 'Paul' });
    assert.deepEqual(clone(doc().stepStamps), stampsBefore, 'taking the cut back removes no stamp');

    // ── an older sheet (created before the recording): seen as it stands, no time made up; what completes after that is stamped ──
    st.docs.clear(); const OLD = R.STEPS_FROM - 3 * 86400000;
    seedSteps(srv, { created: OLD, updated: OLD + 1000, verified: true });
    st.put(SHEETS, 'steps-1', { processSeals: [], processReady: false, backPool: [backOf(A + '_1_1', 'steps-1', OLD + 5000, 'Seth'), backOf(B + '_1_1', 'steps-1', OLD + 9000, 'Ana')] });
    const upd = doc().updatedAt.toMillis();
    r = await pass(R.STEPS_FROM + 600000);
    assert.deepEqual(doc().stepStamps, [], 'an older sheet: no stamp for what it did before'); assert.deepEqual(doc().stepState, { nesting: true, engraving: true, orders: true });
    t = R.stepTimes(clone(doc()));
    assert.equal(t.nesting, null, 'nesting: nothing on record says when'); assert.deepEqual([t.engraving.at, t.engraving.by, t.engraving.recorded], [OLD + 9000, 'Ana', false], 'engraving: read from the approvals the backs carry');
    assert(t.orders && t.orders.recorded === false && t.orders.at === R.STEPS_FROM + 600000, 'order check: the moment nothing held it, which is the ready seal the pass wrote (the seal\'s own time, as the seals show it)');
    for (const k of ['nesting', 'engraving']) assert(!t[k] || t[k].at < R.STEPS_FROM, 'no time of the pass is shown as a time of the step: ' + k);
    // (a write that only notes the steps leaves updatedAt alone: the Library orders its lists by it, and seeing a sheet again is no activity of the sheet)
    seedSteps(srv, { created: OLD, updated: OLD + 1000, verified: false, id: 'steps-2', set: 'set-steps-2' });
    st.put(SHEETS, 'steps-2', { processSeals: [], processReady: false, saving: true });
    await pass(R.STEPS_FROM + 700000, ['steps-1', 'steps-2']);
    assert.deepEqual(doc('steps-2').stepState, { nesting: false, engraving: false, orders: true }); assert.deepEqual(doc('steps-2').stepStamps, []);
    assert.equal(doc('steps-2').updatedAt.toMillis(), OLD + 1000, 'noting the steps of a sheet is no activity of the sheet: its update time stays');
    // ... and the first step of it to complete after that is stamped, at the time its own record gives
    st.put(SHEETS, 'steps-2', { verification: { ok: true }, saving: false, updatedAt: ts(R.STEPS_FROM + 800000) }); await pass(R.STEPS_FROM + 900000, ['steps-1', 'steps-2']);
    assert.deepEqual(stamps('steps-2').map(x => [x.step, x.at]), [['nesting', R.STEPS_FROM + 800000]], 'the older sheet\'s nesting, completed after the recording began, is recorded');
  } finally { Date.now = realNow; srv.close(); }
}

/* ───────────────────────────────────────── C · the page ───────────────────────────────────────── */
let chromium;
function findChromium() {
  const dirs = [process.argv[2], process.env.PW_DIR, '/opt/node22/lib/node_modules/playwright', path.join(root, 'node_modules/playwright'), path.join(root, 'node_modules/playwright-core')].filter(Boolean);
  for (const d of dirs) { for (const n of [d, path.join(d, 'playwright-core'), path.join(d, 'playwright')]) { try { ({ chromium } = require(n)); if (chromium) return true; } catch (_) {} } }
  return false;
}
const CUR = +new Date().toLocaleString('en-US', { timeZone: 'America/Toronto', year: 'numeric' });
const LAST_YEAR_CUT = Date.UTC(CUR - 1, 9, 3, 18, 5);
// six sheets, each in a set of its own: every step recorded and cut · an older sheet with nothing on record · a step done again · no back engraving
// needed · a step not done that once was · cut a year ago
function records() {
  const mk = (id, base, o = {}) => { const s = done(id, { base, setId: 'set-' + id }); s.preview = F.sheetPicture(); Object.assign(s, o); return s; };
  const one = mk('full', 5300000, { laserDoneAt: T(18, 5), laserDoneBy: 'Paul', processReady: true, stepState: { nesting: true, engraving: true, orders: true },
    processSeals: [{ id: 'r', how: 'laserReady', at: T(16, 31), by: 'System' }, { id: 'd', how: 'laserDone', at: T(18, 5), by: 'Paul' }], stepStamps: [ev('nesting', T(13, 12)), ev('engraving', T(15, 40), 'Ana', 1), ev('orders', T(16, 30), '', 2)] });
  const old = mk('old', 5400000, { createdAt: R.STEPS_FROM - 86400000, processSeals: [{ id: 'r', how: 'laserReady', at: T(16, 0), by: 'Seth' }] });
  const again = mk('again', 5500000, { stepState: { nesting: true, engraving: true, orders: true }, processSeals: [{ id: 'r', how: 'laserReady', at: T(21, 0), by: 'Paul' }],
    stepStamps: [ev('nesting', T(13, 12)), ev('engraving', T(13, 0), 'Seth', 1), ev('orders', T(13, 30), '', 2), ev('engraving', T(17, 0), 'Ana', 3), ev('orders', T(19, 45), '', 4)] });
  const plain = mk('plain', 5600000, { stepState: { nesting: true, engraving: true, orders: true }, stepStamps: [ev('nesting', T(13, 12)), ev('orders', T(13, 30), '', 1)] });
  plain.backPool = []; plain.engraving = Object.fromEntries(plain.poolIds.map(p => [p, { needed: false, state: 'none', approved: true }]));
  const not = mk('not', 5700000, { stepState: { nesting: true, engraving: false, orders: true }, stepStamps: [ev('nesting', T(13, 12)), ev('engraving', T(12, 0), 'Seth', 1), ev('orders', T(13, 30), '', 2)] });
  not.backPool = not.backPool.slice(0, 1);
  const year = mk('year', 5800000, { laserDoneAt: LAST_YEAR_CUT, laserDoneBy: 'Paul', processReady: true });
  const sheets = [one, old, again, plain, not, year];
  const rows = sheets.flatMap(s => s.poolIds).map((p, i) => ({ key: p.replace(/_1$/, ''), order: { receiptId: p.split('_')[0], buyer: { name: F.NAMES[i % F.NAMES.length] } }, line: { title: 'Charm', listingId: 'L' + i }, state: 'written', poolIds: [p], engrave: p.startsWith('57') ? { needed: true, state: 'words', approved: false } : { needed: true, state: 'approved', approved: true } }));   // (the 'not' sheet's pieces still wait for a decision)
  return { sheets, rows };
}
const EXPECT = {   // the card's time line of each step of each sheet, in the shop's time (UTC less four hours)
  full: { nesting: 'Oct 3, 9:12 AM', engraving: 'Oct 3, 11:40 AM · by Ana', orders: 'Oct 3, 12:30 PM', laser: 'Oct 3, 2:05 PM · by Paul', completed: 'Oct 3, 2:05 PM · by Paul' },
  old: { nesting: 'Time not recorded', engraving: 'Oct 3, 11:40 AM · by Ana', orders: 'Oct 3, 12:00 PM', laser: '', completed: '' },
  again: { nesting: 'Oct 3, 9:12 AM', engraving: 'Oct 3, 1:00 PM · by Ana', orders: 'Oct 3, 3:45 PM', laser: '', completed: '' },
  plain: { nesting: 'Oct 3, 9:12 AM', engraving: '', orders: 'Oct 3, 9:30 AM', laser: '', completed: '' },
  not: { nesting: 'Oct 3, 9:12 AM', engraving: '', orders: 'Oct 3, 9:30 AM', laser: '', completed: '' },
  year: { nesting: 'Time not recorded', engraving: 'Oct 3, 11:40 AM · by Ana', orders: 'Time not recorded', laser: `Oct 3, ${CUR - 1}, 2:05 PM · by Paul`, completed: `Oct 3, ${CUR - 1}, 2:05 PM · by Paul` }
};
const CASE = { full: 'cut-sheet', old: 'older-sheet', again: 'ready-sheet', plain: 'no-engraving-sheet', not: 'engraving-not-done', year: 'cut-last-year' };   // (names of the screenshots)
async function setup(browser, width) {
  const errors = [], { page, context } = await F.openPage(browser, { width, height: width === 390 ? 844 : 900, fake: false, errors });
  if (width < 600) await page.evaluate(() => document.getElementById('app').classList.add('railOff'));
  page.setDefaultTimeout(8000);
  const { sheets, rows } = records();
  await page.evaluate(({ sheets, rows }) => {
    window.__sheets = sheets; window.__rows = rows; window.__asks = []; const api = window.api; window.api = async (fn, body, o) => { window.__asks.push(body); return api(fn, body, o); };
    const L = window.LaserReview, body = document.getElementById('libBody'); L.sections(body); for (const s of sheets) L.record(s);
    window.__sets = sheets.map((s, i) => ({ setId: s.setId, seq: i + 1, name: 'Set ' + (i + 1), day: '2026-10-03', sheetIds: [s.id], status: 'open', ...(s.laserDoneAt ? { laserDoneAt: s.laserDoneAt } : {}) }));
    window.__sets.forEach((st, i) => { const s = sheets[i], card = window.Sets.libraryCard(st, [s], [s]); L.place(card, L.group(st, [s]).ready, body); });
    L.changed();
  }, { sheets, rows });
  await page.waitForSelector('.flowBox'); await sleep(700);
  return { page, context, errors };
}
const circle = (id, step) => `.flowBox[data-flow-for="sheet:${id}"] .flowDot[data-step="${step}"]`;
const read = (page, sel) => page.evaluate(sel => {
  const d = document.querySelector(sel), t = document.querySelector('.railTip'), tr = t && t.getBoundingClientRect(), b = d.getBoundingClientRect();
  return { shown: !!(t && t.hasAttribute('data-on') && getComputedStyle(t).visibility === 'visible'), name: t?.querySelector('b')?.textContent, state: t?.querySelector('.rtState')?.textContent, line: t?.querySelector('.rtLine')?.textContent || '', by: t?.querySelector('.rtBy')?.textContent || '',
    label: d.getAttribute('aria-label'), tip: d.getAttribute('data-tip'), l: tr?.left, r: tr?.right, t: tr?.top, b: tr?.bottom, w: tr?.width, vw: innerWidth, scrollW: document.documentElement.scrollWidth, circle: { l: b.left, r: b.right, t: b.top, b: b.bottom }, vh: innerHeight };
}, sel);
async function hover(page, sel, ms = 700) {
  await page.evaluate(sel => document.querySelector(sel).scrollIntoView({ block: 'center' }), sel); await sleep(120);
  const b = await (await page.$(sel)).boundingBox(), x = b.x + b.width / 2, y = b.y + b.height / 2;
  await page.mouse.move(x - 40, y - 40); await page.mouse.move(x, y, { steps: 4 }); await sleep(ms);
}
const away = async (page, ms = 450) => { await page.mouse.move(5, 5, { steps: 3 }); await sleep(ms); };
async function shot(page, name, sel, w) {
  if (!SHOTS) return; fs.mkdirSync(SHOTS, { recursive: true });
  const m = await read(page, sel), cx = (m.circle.l + m.circle.r) / 2, x = Math.max(0, Math.min(m.vw - Math.min(m.vw, w), cx - w / 2)), top = Math.max(0, Math.min(m.t ?? m.circle.t, m.circle.t) - 14), bottom = Math.min(m.vh, m.circle.b + 40);
  await page.screenshot({ path: path.join(SHOTS, name + '.png'), clip: { x, y: top, width: Math.min(m.vw, w), height: bottom - top } });
}
async function partC(mutantName) {
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  try {
    for (const width of [1440, 390]) {
      const tag = `${width}px`, { page, errors } = await setup(browser, width), steps = ['nesting', 'engraving', 'orders', 'laser', 'completed'];
      const rails = await page.evaluate(() => Object.fromEntries([...document.querySelectorAll('.flowBox')].map(b => [b.dataset.flowFor.slice(6), [...b.querySelectorAll('.flowDot')].map(d => [d.dataset.step, d.getAttribute('data-tip').split('\n')[1], d.getAttribute('data-tip').split('\n')[3] || ''])])));
      assert.equal(Object.keys(rails).length, 6, `${tag}: six sheets, six rails`);
      for (const [id, want] of Object.entries(EXPECT)) {
        const rail = Object.fromEntries(rails[id].map(([k, state, when]) => [k, { state, when }]));
        for (const k of steps) {
          if (!rail[k]) continue;                                                   // (a step the rail no longer has has no card)
          const { state, when } = rail[k];
          // a step that is not done shows no time; a done one shows its own
          if (state !== 'Done') { assert.equal(when, '', `${tag}: ${id} ${k} is "${state}": no time`); continue; }
          assert.equal(when, want[k], `${tag}: ${id} ${k}: the time of the step`);
        }
      }
      // the five steps shown on the cut sheet (and every sheet's done steps), by hovering, in the real card, with the label saying the same words
      for (const [id, k] of [['full', 'nesting'], ['full', 'engraving'], ['full', 'orders'], ['full', 'laser'], ['full', 'completed'], ['old', 'nesting'], ['old', 'engraving'], ['old', 'orders'], ['again', 'nesting'], ['again', 'engraving'], ['again', 'orders'], ['year', 'laser'], ['plain', 'engraving'], ['not', 'engraving']]) {
        const sel = circle(id, k); if (!(await page.$(sel))) continue;
        await hover(page, sel); const m = await read(page, sel);
        assert.equal(m.shown, true, `${tag}: ${id} ${k}: the card shows`); assert.equal(m.by, EXPECT[id][k] === '' ? '' : m.state === 'Done' ? EXPECT[id][k] : '', `${tag}: ${id} ${k} says ${EXPECT[id][k] || 'no time'} (${m.by})`);
        { const [n0, n1, n2, n3] = m.tip.split('\n'); assert.equal(m.label, `${n0}. ${n1}. ${n2}${n3 ? ` ${n3}.` : ''}`, `${tag}: ${id} ${k}: the label is the card's own words`); }
        assert(m.w <= 236.5, `${tag}: the card keeps its calm width (${m.w})`); assert(m.l >= 7.5 && m.r <= m.vw - 7.5, `${tag}: inside the screen`); assert(m.scrollW <= m.vw, `${tag}: no sideways scroll`);
        if (m.by) assert(m.label.endsWith(' ' + m.by + '.'), `${tag}: the label carries the same words (${m.label})`);
        assert.doesNotMatch(m.tip, /\blines?\b/i, 'pieces, never lines');
        await shot(page, `${CASE[id]}-${k}-${width}`, sel, width === 390 ? 380 : 420);
        await away(page);
      }
      // page and server read the same stamps: the page's reading of each record is the node reading of the same record
      const reads = await page.evaluate(() => window.__sheets.map(s => [s.id, window.CharmNestReadiness.stepTimes(s)]));
      for (const [id, t] of reads) assert.deepEqual(clone(t), clone(REAL.stepTimes(window_sheet(id))), `${tag}: the page and the server read ${id} to the same times`);
      // the live read never records: every read that follows the cloud asks recordSeals:false; the seals' pass is the only one that does
      await sleep(1500); const asks = (await page.evaluate(() => window.__asks)).filter(a => a && a.op === 'laserStatus');
      for (const a of asks) if (a.wantRevs || a.ifRevs) assert.equal(a.recordSeals, false, `${tag}: the live read never carries recordSeals:true`);
      assert.deepEqual(errors, [], `${tag}: no page error`);
      await page.context().close();
    }
    // the source of the page: the pass is asked for in one place only (the Library's seal check), never in the live read
    const bridge = fs.readFileSync(path.join(root, 'charm-nest-bridge.js'), 'utf8');
    assert.equal((bridge.match(/recordSeals:true/g) || []).length, 1, 'one place records: the seals\' pass'); assert(/ask=\{op:'laserStatus',sheetIds:ids,setIds,recordSeals:false,wantRevs:true\}/.test(bridge), 'the live read asks recordSeals:false');
  } finally { await browser.close(); }
}
function window_sheet(id) { return records().sheets.find(s => s.id === id); }

/* ───────────────────────────────────────── the mutants ───────────────────────────────────────── */
const MUTANTS = [
  ['a pass that records no step', src => src.replace('if(is && !was && log.length+added.length<MAX_STEP_STAMPS)', 'if(false && is && !was && log.length+added.length<MAX_STEP_STAMPS)')],
  ['a reading that shows the first time instead of the latest', src => src.replace('const xs=log.filter(x=>x.step===k),l=xs[xs.length-1];', 'const xs=log.filter(x=>x.step===k),l=xs[0];')],
  ['a pass that overwrites the earlier stamps', src => src.replace('stamps:log.concat(added)', 'stamps:added')],
  ['an older sheet given a time it never had', src => src.replace('fresh=!had && msOf(s.createdAt)>=STEPS_FROM', 'fresh=!had')],
  ['a nesting time taken from the pass instead of the record', src => src.replace('if(u>=REAL_FROM && u<=now)at=u;', '')],
  ['an engraving time taken from the pass instead of the approval', src => src.replace('if(e && e.at<=now){at=e.at;by=e.by;}', '')],
  ['a laser time shown for order check', src => src.replace('out.laser=out.completed=at>0', 'out.orders=out.laser=out.completed=at>0')]
];
async function suite(label) {
  partA(); await partB();
}
async function main() {
  const t0 = Date.now(); let n = 0;
  await suite(); n++;
  console.log('  ok  A and B hold for the real code');
  let hasBrowser = !process.env.NO_BROWSER && findChromium();
  if (hasBrowser) { await partC(); console.log('  ok  C: the page, at 1440 and 390 px'); } else console.log('  - no playwright (or NO_BROWSER): the browser part was not run');
  // the mutants: each must be caught by A, B or (for the card) C
  for (const [name, mutate] of MUTANTS) {
    install(load(mutate)); let caught = false;
    let why = '';
    try { await suite(); } catch (e) { caught = true; why = String(e && e.message || e).split('\n')[0].slice(0, 110); } finally { restore(); }
    assert(caught, `MUTANT NOT CAUGHT: ${name}`); n++; console.log('  ok  caught: ' + name + '  [' + why + ']');
    // ... by the server's part on its own as well (the real handler over the in-memory shop), not only by the pure reading
    install(load(mutate)); let alone = false;
    try { await partB(); } catch (e) { alone = true; } finally { restore(); }
    assert(alone, `MUTANT NOT CAUGHT BY THE SERVER PART: ${name}`);
  }
  if (hasBrowser) {
    // the card showing the first time instead of the latest (the page's own code, changed)
    const realRead = fs.readFileSync; let caught = false;
    fs.readFileSync = function (p, ...a) { let t = realRead.call(this, p, ...a); if (typeof t === 'string' && String(p).endsWith('charm-nest-bridge.js')) { const o = t.replace('stepWhen(t.at):\'\'', 'stepWhen(t.first || t.at):\'\''); assert.notEqual(o, t, 'the card mutant must change the source'); t = o; } return t; };
    let why = '';
    try { await partC(); } catch (e) { caught = true; why = String(e && e.message || e).split('\n')[0].slice(0, 110); } finally { fs.readFileSync = realRead; }
    assert(caught, 'MUTANT NOT CAUGHT: a card that shows the first time instead of the latest'); n++; console.log('  ok  caught: a card that shows the first time instead of the latest  [' + why + ']');
    caught = false;
    fs.readFileSync = function (p, ...a) { let t = realRead.call(this, p, ...a); if (typeof t === 'string' && String(p).endsWith('charm-nest-bridge.js')) { const o = t.replace("ask={op:'laserStatus',sheetIds:ids,setIds,recordSeals:false,wantRevs:true}", "ask={op:'laserStatus',sheetIds:ids,setIds,recordSeals:true,wantRevs:true}"); assert.notEqual(o, t); t = o; } return t; };
    try { await partC(); } catch (e) { caught = true; why = String(e && e.message || e).split('\n')[0].slice(0, 110); } finally { fs.readFileSync = realRead; }
    assert(caught, 'MUTANT NOT CAUGHT: a live read that records'); n++; console.log('  ok  caught: a live read that records  [' + why + ']');
  }
  console.log(`PASS: step times: every done step's completion (nesting, engraving, order check, laser cutting, completed) read from the sheet's own record, recorded permanently by the seals' pass, latest shown and first kept, older sheets honest; ${n} checks incl. mutants, ${Date.now() - t0} ms`);
}
main().catch(e => { console.error(e); process.exitCode = 1; });
