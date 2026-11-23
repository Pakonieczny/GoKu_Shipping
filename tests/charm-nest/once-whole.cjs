// ONCEWHOLE (Paul, 22 Nov 2026: "Why did this happen? I want the root cause fixed not just moving the pieces over" · "I need to know why it marked this sheet as full you need to find out").
// RG Sheet 1 (rose-mwmr139j-cralro139j) held 29 pieces on its strip at 21:16. A Charm Editor update made order 4239845119's SPORTS 12 - BULLSEYE again at twice its old size (307 -> 650 pt2):
// it had no room on the sheet (true: no spot at any angle). The nest's own pick kept the orders whole and oldest first knowing the 29 were saved where they lay (lifted none); the cloud's sheet
// writer then ran that rule AGAIN on a sheet it made with appendOnly false, lifted the 11 saved pieces of the 9 orders younger than the BULLSEYE's, counted them as 9 more orders with no room
// ("12 different orders in a row had no room on it"), released the sheet full and moved the 12 to a new draft; it was cut at 21:18 with their room empty.
// The fixture is that nest as the cloud kept it (nestDebugGet / getSheet, read only): positions, orders and dates; no artwork.
//   1  the cause, as it was: the rule run again with the saved layout not held lifts exactly the 11 pieces the cloud moved, and the count reaches 12 and releases the sheet full with those words
//   2  now: the writer applies that nest: the 29 stay where they are, only the BULLSEYE is turned away; the run is 3 orders, the sheet is not full; only the BULLSEYE moves on
//   3  a nest without the pick's mark (an older job): the rule runs once, the saved layout held as the nest held it (the job's appendOnly, else the record's): none lifted
//   4  the rule still works where it should (a fresh nest, nothing saved): the younger orders seated in it are lifted, said in the result, and NOT counted as orders with no room
//   5  the cloud's pick says in its result whether it held the saved layout (appendOnly), and the writer reads it
//   NODE_PATH=<deps> node tests/charm-nest/once-whole.cjs
'use strict';
process.env.EDIT_PASSCODE = 'test-pass';
const fs = require('fs'), path = require('path');
const F = require('./_cloudFakes.cjs')({ now: Date.UTC(2026, 10, 22, 21, 16, 0) });
const root = path.join(__dirname, '../..');
let failed = 0;
const ok = (n, c, d) => { console.log((c ? 'OK   ' : 'FAIL ') + n + (c || d === undefined ? '' : ' :: ' + JSON.stringify(d).slice(0, 1200))); if (!c) failed++; };
const quiet = console.warn; console.warn = () => {};
const Finish = F.load('_charmNestFinish.js');
const FC = require(path.join(root, 'charm-nest-finish-core.js')), NP = require(path.join(root, 'charm-nest-nest-plan.js')), O = require(path.join(root, 'charm-nest-orders.js'));
const FX = JSON.parse(fs.readFileSync(path.join(__dirname, 'fixtures', 'rg-sheet1-oncewhole.json'), 'utf8'));
const clone = o => JSON.parse(JSON.stringify(o));
const BULL = FX.result.rejects[0], ordOf = id => String(id).split(':').pop().split('_')[0];
const byIdOf = () => new Map(FX.charms.map(c => [c.id, Object.assign({}, c)]));
const nameOf = id => { const c = FX.charms.find(x => x.id === id); return c && c.name; };
const S = { settings: { maxFill: 0.8, clearancePt: -0.51, nospread: 'on' } };
const env = { maxFill: 0.8, orderKey: NP.orderKey, orderRank: c => NP.orderRank(c, O) };
/** The writer's context for its layout step (S1), as Finish.run builds it. */
function writer(rec, result, appendOnly) {
  const byId = byIdOf(), X = { db: F.db, rec, task: { job: { nestJobId: 'nest-8462e548578a82c3789dfc62', appendOnly } }, byId, NP, O, notes: [], S, Host: { CHARM_SCALE: 0.971 }, CustomPool: { holdSize: () => {} },
    nest: { jobId: 'nest-8462e548578a82c3789dfc62', result, appendOnly } };
  X.sh = Finish.sheetOf(X, [...byId.values()]);
  return X;
}
const recOf = (o) => Object.assign({ id: FX.sheetId, metal: 'rose', runId: FX.runId, draft: true, appendOnly: true, charms: clone(FX.charms), placements: clone(FX.recPlacements), noRoomRun: FX.rec.noRoomRun.slice(), verification: { ok: true } }, o || {});

(async () => {
  /* ═══ 1 · the cause, as it was ═══ */
  {
    const sh = { metal: 'rose', charms: clone(FX.charms), placements: clone(FX.result.placements), rejects: FX.result.rejects.slice(), nestInitial: clone(FX.recPlacements), appendOnly: false, feedWait: [] };
    const w = NP.keepOrdersWhole(sh, byIdOf(), new Set(), env);
    const liftedNames = w.lifted.map(p => nameOf(p.id)).sort(), moved = FX.movedOn.filter(n => !/BULLSEYE/.test(n)).sort();
    ok('1 · the cause: the rule run again on the written nest with the saved layout NOT held lifts exactly the 11 saved pieces the cloud moved off Sheet 1 (the 9 orders younger than the BULLSEYE\'s)',
      w.lifted.length === 11 && JSON.stringify(liftedNames) === JSON.stringify(moved) && w.lifted.every(p => FX.charms.find(c => c.id === p.id).orderDate > FX.charms.find(c => c.id === BULL).orderDate), { liftedNames, moved });
    const run = FC.noRoomRunOf(sh, Object.assign({}, FX.result, { rejects: sh.rejects }), FX.recPlacements, FX.charms, FX.rec.noRoomRun, env);
    ok('1 · ... and counted them as orders with no room: the run reaches the very 12 orders Sheet 1 was released with', run.length === 12 && JSON.stringify(run.slice().sort()) === JSON.stringify(FX.noRoomRunAfter.slice().sort()), run);
  }
  /* ═══ 2 · now: the writer applies that nest ═══ */
  {
    const rec = recOf(), X = writer(rec, clone(FX.result), null);
    const W = await Finish.layoutOf(X);
    ok('2 · the writer keeps the 29 saved pieces where they are; only the BULLSEYE is turned away; nothing lifted', W.placements.length === 29 && JSON.stringify(W.rejects) === JSON.stringify([BULL]) && W.result.lifted.length === 0 && !X.notes.some(n => /lifted off/.test(n)),
      { placed: W.placements.length, rejects: W.rejects, lifted: W.result.lifted, notes: X.notes });
    const same = W.placements.every(p => { const q = FX.recPlacements.find(x => x.id === p.id); return q && Math.abs(q.cxPt - p.cxPt) < 1e-6 && Math.abs(q.cyPt - p.cyPt) < 1e-6 && q.angle === p.angle; });
    ok('2 · each of the 29 exactly where it lay', same);
    const sh = Object.assign(X.sh, { placements: W.placements, rejects: W.rejects.slice(), nestInitial: W.nestInitial, verification: { ok: true } });
    const m = await Finish.afterNestMarks(X, sh, Object.assign({}, W.result, { rejects: W.result.rejects }), FX.charms, { FC, NP, rec, S });
    ok('2 · the run of orders with no room is 3 (the two before and the BULLSEYE\'s): Sheet 1 is NOT released full', m.releaseFull === false && Array.isArray(m.noRoomRun) && m.noRoomRun.length === 3 && m.noRoomRun.includes(ordOf(BULL)) && !m.releaseWhy, m);
    const plan = FC.overflowPlan(Object.assign({}, sh, { rejects: W.rejects }), {});
    ok('2 · what moves on to the next sheet is the BULLSEYE alone', plan.kind === 'move' && plan.moving.length === 1 && plan.moving[0].id === BULL, { kind: plan.kind, moving: plan.moving.map(c => c.name) });
  }
  /* ═══ 3 · a nest without the pick's mark (an older job) ═══ */
  {
    const res = clone(FX.result); delete res.ordersWhole;
    for (const [why, rec, app] of [['the record\'s appendOnly', recOf(), null], ['the job\'s appendOnly', recOf({ appendOnly: false }), true]]) {
      const X = writer(rec, res, app), W = await Finish.layoutOf(X);
      ok(`3 · an older job (no ordersWhole): the rule runs once, the saved layout held (${why}): none of the 29 lifted, only the BULLSEYE turned away`, W.placements.length === 29 && W.rejects.length === 1 && W.result.lifted.length === 0, { placed: W.placements.length, rejects: W.rejects.length, notes: X.notes });
      ok(`3 · ... and the record's own flag is left as it was on the writer's sheet (${why})`, X.sh.appendOnly === false);
    }
  }
  /* ═══ 4 · the rule still works where it should ═══ */
  {
    const res = clone(FX.result); delete res.ordersWhole;
    const rec = recOf({ appendOnly: false, placements: [] }), X = writer(rec, res, false), W = await Finish.layoutOf(X);
    ok('4 · a fresh nest (nothing saved on the sheet): the younger orders it seated are lifted to keep the oldest first, and said in the result', W.placements.length === 18 && W.result.lifted.length === 11 && X.notes.some(n => /oldest first/.test(n)), { placed: W.placements.length, lifted: W.result.lifted.length });
    const sh = Object.assign(X.sh, { placements: W.placements, rejects: W.rejects.slice(), nestInitial: W.nestInitial, verification: { ok: true } });
    const m = await Finish.afterNestMarks(X, sh, Object.assign({}, W.result, { rejects: W.result.rejects }), FX.charms, { FC, NP, rec, S });
    ok('4 · ... the lifted ones fitted: they are not counted as orders with no room (the nest seated new pieces, so the run starts again with the BULLSEYE\'s order alone, not 10), the sheet is not released full', m.releaseFull === false && JSON.stringify(m.noRoomRun) === JSON.stringify([ordOf(BULL)]), { run: m.noRoomRun, why: m.releaseWhy });
    const sh2 = { placements: [{ id: 'a' }], rejects: ['x', 'y'] }, items = [{ id: 'a', order: '1' }, { id: 'x', order: '2' }, { id: 'y', order: '3' }];
    ok('4 · noRoomRunOf: a piece lifted by the pick (ordersWhole) or by the writer (lifted) is not counted; a piece the search turned away is',
      JSON.stringify(FC.noRoomRunOf(sh2, { endedBy: 'no-room', usablePt2: 0, ordersWhole: { done: true, lifted: ['y'] } }, [{ id: 'a' }], items, [], env)) === '["2"]'
      && JSON.stringify(FC.noRoomRunOf(sh2, { endedBy: 'no-room', usablePt2: 0, lifted: ['x'] }, [{ id: 'a' }], items, [], env)) === '["3"]'
      && JSON.stringify(FC.noRoomRunOf(sh2, { endedBy: 'no-room', usablePt2: 0 }, [{ id: 'a' }], items, [], env)) === '["2","3"]');
  }
  /* ═══ 5 · the pick says whether it held the saved layout ═══ */
  {
    const slot = fs.readFileSync(path.join(root, 'netlify/functions/_charmNestNestSlot.js'), 'utf8'), fin = fs.readFileSync(path.join(root, 'netlify/functions/_charmNestFinish.js'), 'utf8');
    ok('5 · the cloud\'s pick writes appendOnly in the nest\'s result beside ordersWhole; the writer reads the job\'s, else the result\'s',
      /appendOnly: !!sh\.appendOnly,[^\n]*\n\s*ordersWhole: \{ done: true/.test(slot) && /appendOnly: j\.appendOnly != null \? !!j\.appendOnly : result\.appendOnly != null \? !!result\.appendOnly :/.test(fin));
  }
  console.warn = quiet;
  console.log(failed ? `FAIL once-whole (${failed})` : 'PASS once-whole'); process.exit(failed ? 1 : 0);
})().catch(e => { console.log('FAIL once-whole: ' + (e && e.stack || e)); process.exit(1); });
