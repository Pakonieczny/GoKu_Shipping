// RGRESTORE (Paul, 22 Nov 2026): RG Sheet 1 (rose-mwmr139j-cralro139j) held 29 pieces after nest-8462e548578a82c3789dfc62. The writer's ONCEWHOLE bug lifted 11 of them; they and the
// remade BULLSEYE (4239845119_5302267849_1, which truly has no room on Sheet 1) went to a new draft, RG Draft 2 (d-0729e06624472beb), which took the leftover of Sheet 1's automatic cut;
// its nest placed 0 of 12 and its "The oldest order cannot fit on an empty sheet" stopped the run. The owner's request (runCmdPut "restorenest") puts the 11 back where nest-8462 left them,
// sets Sheet 1's automatic cut aside (its green line is drawn and cut again with them), nests the BULLSEYE on RG Draft 1, and empties and removes RG Draft 2 with its stock claim given back.
// Offline: the real driver pass, the real requests unit, the real library handler (roseRelease, roseReopen, cmdMovePieces, archiveEmptySheet, nestStart, the new cmdRestoreNest) on the
// in-memory cloud of _cloudFakes.cjs, seeded from the live records (fixtures/rg-restore.json, read only). No network, no Etsy.
//   A  refused in words, nothing written: laser cut, the set sent to the station, a piece of Sheet 1 not where the nest left it or a piece the nest did not place (overlap), the
//      stock used by a third sheet (an uncut sheet seated on it, the stock held by it, or the cut's leftover in use by it); a page's own call of cmdRestoreNest refused (CLOUD_ONLY)
//   B  the request on the real records: 29 pieces on Sheet 1 exactly at nest-8462's positions; its cut set aside, the stock Sheet 1's again at its pre-cut outline (cut 1's), the cut's
//      leftover discarded, the line owed again; Draft 2 empty, archived, no stock, no problemStop; the BULLSEYE on Draft 1 and nested there; the pieces' records and lines as the set's
//      others; the run still stopped (its Resume is the owner's) and nothing left that stops it again; answered done in words
//   C  idempotent: another pass and the same request asked again change nothing
//   D  the lane: done while only "cmd:renest" / "cmd:seat" are on (the owner's live cloudSteps); with neither, answered in words and nothing changed
//   NODE_PATH=<deps> node tests/charm-nest/rg-restore.cjs
'use strict';
process.env.EDIT_PASSCODE = 'test-pass';
const fs = require('fs'), path = require('path');
const T0 = Date.UTC(2026, 10, 23, 4, 30, 0);
const F = require('./_cloudFakes.cjs')({ now: T0 });
const log0 = console.log.bind(console); console.log = () => {}; console.warn = () => {}; console.error = () => {};
let failed = 0;
const ok = (name, cond, detail) => { log0((cond ? 'OK   ' : 'FAIL ') + name + (cond || detail === undefined ? '' : ' :: ' + JSON.stringify(detail).slice(0, 1500))); if (!cond) failed++; };
const lib = F.load('charmNestLibrary.js'), Drive = F.load('_charmNestDrive.js'), Lease = F.load('_charmNestLease.js'), Unit = F.load('_charmNestCmdUnit.js'), Nest = F.load('_charmNestNest.js');
const FX = JSON.parse(fs.readFileSync(path.join(__dirname, 'fixtures', 'rg-restore.json'), 'utf8'));
const R = FX.runId, S = FX.sheetId, D = FX.draftId, T = FX.targetId, JOB = FX.jobId, STOCK = FX.stockId, SET = FX.setId, DEV = 'd0paulk0001', CV = 'cloud14-20261122';
const BULL = '4239845119_5302267849_1';
const clone = o => JSON.parse(JSON.stringify(o));
const P = async body => { const r = await lib.handler({ httpMethod: 'POST', headers: { 'x-edit-passcode': 'test-pass' }, body: JSON.stringify(body) }); let o = {}; try { o = JSON.parse(r.body || '{}'); } catch (_) {} return Object.assign({}, o, { http: r.statusCode }); };
const sheet = id => F.doc('Charm_Nest_Sheets/' + id), stock = () => F.doc('Charm_Nest_Rose_Stock/' + STOCK), rem = rev => F.doc(`Charm_Nest_Remnants/${STOCK}-${rev}`), pool = id => F.doc('Charm_Pool/' + id);
const cutDoc = id => F.doc(`Charm_Nest_Rose_Stock/${STOCK}/cuts/${id}`);
const active = r => ((r && r.charms) || []).filter(c => c && !c.excluded);
const NESTP = new Map(FX.job.result.placements.map(p => [p.id, p]));
const CUT1 = FX.cuts.find(c => +c.revision === 1), CUT2 = FX.cuts.find(c => +c.revision === 2);
const D_PLACED = active(FX.sheets[D]).filter(c => NESTP.has(c.id)), D_REST = active(FX.sheets[D]).filter(c => !NESTP.has(c.id));

function seed(o = {}) {
  F.reset(); F.clock.set(T0);
  F.seed('Charm_Nest_Open/drive', { mode: 'on', runMode: FX.runMode || 'auto', cloudSteps: o.steps || FX.cloudSteps.filter(s => s === 'nest' || /^cmd:/.test(s)), flippedAt: T0 - 86400e3 }); lib._driveForget();
  F.seed('Charm_Nest_Runs/' + R, clone(FX.run));
  F.seed('Charm_Nest_Open/run', { runId: R, at: T0 - 86400e3 });
  F.seed('Charm_Nest_Run_Carrier/' + R, { runId: R, carrier: { dev: 'cloud', by: 'Cloud', at: T0, since: T0 - 86400e3 } });
  for (const [id, rec] of Object.entries(FX.sheets)) F.seed('Charm_Nest_Sheets/' + id, clone(rec));
  F.seed('Charm_Nest_Jobs/' + JOB, clone(FX.job));
  F.seed('Charm_Nest_Rose_Stock/' + STOCK, clone(FX.stock));
  for (const c of FX.cuts) F.seed(`Charm_Nest_Rose_Stock/${STOCK}/cuts/${c.sheetId}`, clone(c));
  F.seed('Charm_Nest_Rose_Stock/' + FX.stock2.id, clone(FX.stock2));
  for (const r of FX.remnants) F.seed('Charm_Nest_Remnants/' + r.id, clone(r));
  F.seed('Charm_Nest_Sets/' + SET, clone(FX.set));
  for (const [id, row] of Object.entries(FX.pool)) F.seed('Charm_Pool/' + id, clone(row));
  if (o.edit) o.edit();
}
async function pass() {
  F.net.kicks.length = 0;
  const L = F.doc('Charm_Nest_Drive/lease') || {};
  const nonce = L.state === 'dispatched' && L.nonce && Date.now() - (+L.at || 0) < 110000 ? L.nonce : (await Lease.dispatch(F.db, F.db.collection('Charm_Nest_Drive').doc('lease'), 'test', { heartbeat: true })).nonce;
  return Drive.pass({ db: F.db, Lib: lib, nonce, passItems: Unit.units.passItems, sleep: async () => {}, renewMs: 0, budgetMs: 600000 });
}
// the owner's request, as it is sent once this is live (runCmdPut, a person's press)
const ask = () => P({ op: 'runCmdPut', runId: R, dev: DEV, by: 'Paul_K', cv: CV, cmd: { type: 'restorenest', sheetId: S, sourceId: JOB, fromId: D, targetId: T, label: 'Put RG Sheet 1 back as nest-8462 left it' } });
const cmdOf = id => (((F.doc('Charm_Nest_Run_Cmds/' + R) || {}).cmds || {})[id]) || {};
/** Nothing of the three sheets, the stock or its leftovers was written. */
const untouched = () => {
  const s = sheet(S), d = sheet(D), t = sheet(T), st = stock();
  return s.layoutRev === FX.sheets[S].layoutRev && s.roseCutAt === FX.sheets[S].roseCutAt && active(s).length === 18 && d.layoutRev === FX.sheets[D].layoutRev && active(d).length === 12 && !d.archived && !!d.problemStop
    && t.layoutRev === FX.sheets[T].layoutRev && st.owner === D && st.profileJson === FX.stock.profileJson && rem(2).status === 'inUse' && !cutDoc(S).setAsideAt;
};
const state = () => JSON.stringify([sheet(S), sheet(D), sheet(T), stock(), rem(1), rem(2), cutDoc(S), Object.keys(FX.pool).map(pool), F.list('Charm_Nest_Jobs').length]);
async function refused(name, edit, re) {
  seed({ edit });
  const was = state(), r = await ask(); await pass();
  const c = cmdOf(r.id);
  ok(`A · ${name}: refused in words, nothing written`, r.ok && c.state === 'failed' && re.test(c.text || '') && state() === was, { c, s: { layoutRev: sheet(S).layoutRev, cut: sheet(S).roseCutAt }, d: { n: active(sheet(D)).length, layoutRev: sheet(D).layoutRev }, owner: stock().owner });
}

(async () => {
  /* ═══ A · refusals ═══ */
  ok('A · the fixture is the live state: Sheet 1 18 pieces, its cut at revision 2 on its own stock; Draft 2 12 pieces (11 of the nest + the BULLSEYE), owns the stock, problemStop; the nest 29 verified',
    active(FX.sheets[S]).length === 18 && FX.sheets[S].roseCutRevision === 2 && FX.sheets[S].roseStockId === STOCK && active(FX.sheets[D]).length === 12 && D_PLACED.length === 11 && D_REST.length === 1 && D_REST[0].poolId === BULL
      && FX.stock.owner === D && !!FX.sheets[D].problemStop && FX.job.result.placements.length === 29 && FX.job.result.verification.ok === true && FX.run.status === 'stopped');
  await refused('the laser cut Sheet 1', () => { const s = sheet(S); s.laserDoneAt = T0 - 60000; F.seed('Charm_Nest_Sheets/' + S, s); }, /cut on the laser/);
  await refused('Set 2 was sent to the station', () => { const s = F.doc('Charm_Nest_Sets/' + SET); s.committedAt = T0 - 60000; F.seed('Charm_Nest_Sets/' + SET, s); }, /sent to the station/);
  await refused('a piece of Sheet 1 is not where nest-8462 left it (the pieces put back could overlap)', () => { const s = sheet(S); s.placements[3].cxPt += 3; F.seed('Charm_Nest_Sheets/' + S, s); }, /no longer where that nest left it.*overlap/);
  await refused('the stock is used by a third sheet (an uncut sheet seated on it)', () => F.seed('Charm_Nest_Sheets/rose-third-1', { id: 'rose-third-1', runId: R, metal: 'rose', draft: true, roseStockId: STOCK, roseRevision: 2, charms: [], poolIds: [], placements: [], fileBase: 'RG_working_rose-third-1', page: 9 }), /third sheet/);
  await refused('Sheet 1 holds a piece that nest did not place (the pieces put back could overlap it)', () => { const s = sheet(S); s.charms.push({ id: 'pool:x:4299999999_5399999999_1', poolId: '4299999999_5399999999_1', order: '4299999999', name: 'NEW' }); s.poolIds.push('4299999999_5399999999_1'); F.seed('Charm_Nest_Sheets/' + S, s); }, /did not place.*overlap/);
  await refused('the stock is held by a third sheet', () => { const st = stock(); st.owner = 'rose-third-3'; F.seed('Charm_Nest_Rose_Stock/' + STOCK, st); }, /held by another sheet/);
  await refused('the cut\'s leftover is in use by a third sheet', () => { const r = rem(2); r.inUseBySheetId = 'rose-third-2'; r.inUseBySheetName = 'RG Draft 9'; F.seed(`Charm_Nest_Remnants/${STOCK}-2`, r); }, /in use by RG Draft 9/);
  seed();
  { const r = await P({ op: 'cmdRestoreNest', runId: R, sheetId: S, jobId: JOB, from: [D], dev: DEV, cv: CV, by: 'Paul_K' });
    ok('A · a page\'s own call of cmdRestoreNest is refused (CLOUD_ONLY), nothing written', r.http === 403 && r.cloudOnly === true && untouched(), r); }

  /* ═══ B · the owner's request on the real records ═══ */
  seed();
  const r1 = await ask();
  ok('B · the request is taken (runCmdPut restorenest: the sheet, the nest, the draft, the target)', r1.http === 200 && r1.ok && r1.cmd.type === 'restorenest' && r1.cmd.args.sheetId === S && r1.cmd.args.sourceId === JOB && r1.cmd.args.fromId === D && r1.cmd.args.targetId === T, r1);
  const p1 = await pass();
  const s = sheet(S), d = sheet(D), t = sheet(T), st = stock(), c1 = cmdOf(r1.id);
  ok('B · the pass ran (no error)', p1 && p1.ok !== false && !p1.error, p1);
  const sPl = new Map((s.placements || []).map(p => [p.id, p]));
  const exact = [...NESTP.values()].every(q => { const p = sPl.get(q.id); return p && p.cxPt === q.cxPt && p.cyPt === q.cyPt && (((p.angle % 360) + 360) % 360) === (((q.angle % 360) + 360) % 360) && (+p.scale || 1) === (+q.scale || 1); });
  ok('B1 · 29 pieces on Sheet 1, every one exactly where nest-8462 left it (the 18 untouched, the 11 back); no piece the nest did not place', active(s).length === 29 && s.placements.length === 29 && exact && active(s).every(c => NESTP.has(c.id)) && !active(s).some(c => c.poolId === BULL),
    { n: active(s).length, pl: (s.placements || []).length, exact });
  ok('B1 · the 18 that stayed kept their placement records byte for byte', FX.sheets[S].placements.every(p => JSON.stringify(sPl.get(p.id)) === JSON.stringify(p)));
  ok('B1 · Sheet 1 lists the 11 again (pool ids, orders), owes its files and its green line again (roseRedraw line + cut), owes no nest, is not dirty; its no-room run is the 3 orders before (not the 12)',
    D_PLACED.every(c => s.poolIds.includes(c.poolId) && s.orders.includes(String(c.order))) && s.filesOwed > 0 && s.roseRedraw && s.roseRedraw.line === true && s.roseRedraw.cut === true && !s.nestOwed && s.dirty === false
      && JSON.stringify(s.noRoomRun) === JSON.stringify(['4242618121', '4243182919', '4239845119']) && s.releaseFull === true && s.setId === SET && s.draft === false && s.movedOn === null,
    { filesOwed: s.filesOwed, roseRedraw: s.roseRedraw, nestOwed: s.nestOwed, dirty: s.dirty, noRoomRun: s.noRoomRun });
  ok('B2 · Sheet 1\'s automatic cut is set aside: no recorded cut, no plan; its roseReseated says which (stock, revision 2, reopen); roseReopened says a cut was set aside; the cut\'s own record stays, marked set aside',
    !s.roseCutAt && s.roseCutRevision == null && !s.rosePlanJson && !s.rosePlanHash && !s.roseProtectedJson && Array.isArray(s.roseReseated) && s.roseReseated.some(x => x.reopen && x.cut && x.cut.revision === 2 && x.cut.stockId === STOCK)
      && s.roseReopened && s.roseReopened.cut === true && cutDoc(S) && cutDoc(S).setAsideAt > 0 && cutDoc(S).planHash === CUT2.planHash && !cutDoc(FX.sheets['rose-mwiigu50-vk9lldgu50'].id).setAsideAt,
    { roseCutAt: s.roseCutAt, reseated: s.roseReseated, reopened: s.roseReopened, cut2: cutDoc(S) && cutDoc(S).setAsideAt });
  const pre = JSON.stringify(JSON.parse(CUT1.planJson).profile);
  ok('B3 · the stock is Sheet 1\'s again, at its pre-cut outline (the outline cut 1 left, which nest-8462 was nested on), revision 2; Sheet 1 holds it at that revision',
    st.owner === S && st.available === false && st.profileJson === pre && st.revision === 2 && s.roseStockId === STOCK && s.roseRevision === 2, { owner: st.owner, available: st.available, revision: st.revision, same: st.profileJson === pre });
  ok('B3 · the leftover Sheet 1\'s cut made (…-2) is no longer offered (discarded, with why); the leftover it was nested on (…-1) stays Sheet 1\'s', rem(2).status === 'discarded' && rem(2).supersededSheetId === S && /changed after its green line/.test(rem(2).reason || '') && rem(1).status === 'used' && rem(1).usedBySheetId === S, { r2: rem(2), r1: { status: rem(1).status, by: rem(1).usedBySheetId } });
  ok('B4 · RG Draft 2 is empty and gone: archived, no pieces, no stock claim, no problem, no problemStop, owes nothing', d.archived === true && !active(d).length && !(d.poolIds || []).length && !(d.placements || []).length && !d.roseStockId && !d.problem && !d.problemStop && !d.nestOwed && d.status === 'idle',
    { archived: d.archived, n: active(d).length, roseStockId: d.roseStockId, problemStop: d.problemStop, problem: d.problem });
  ok('B5 · the BULLSEYE waits on RG Draft 1 (listed, its record names Draft 1) and Draft 1\'s nest is asked (append: its 13 placed pieces held)',
    t.poolIds.includes(BULL) && active(t).some(c => c.poolId === BULL) && pool(BULL).sheetId === T && pool(BULL).movedFrom === D && (t.placements || []).length === 13 && FX.sheets[T].placements.every(p => JSON.stringify((t.placements || []).find(q => q.id === p.id)) === JSON.stringify(p))
      && F.list('Charm_Nest_Jobs').some(j => j.sheetId === T && ['queued', 'running', 'starting'].includes(j.status)), { tPool: t.poolIds.includes(BULL), jobs: F.list('Charm_Nest_Jobs').map(j => [j._id, j.sheetId, j.status]) });
  ok('B6 · the 11 pieces\' records name Sheet 1 again as Set 2\'s other pieces (setId, "written", its file name), moved back by the owner\'s request', D_PLACED.every(c => { const x = pool(c.poolId); return x.sheetId === S && x.setId === SET && x.state === 'written' && x.sheetName === s.fileBase && x.movedHow === 'restore' && x.movedFrom === D; }),
    D_PLACED.map(c => { const x = pool(c.poolId); return [c.poolId, x.sheetId, x.setId, x.state, x.movedHow]; }));
  const run = (await P({ op: 'runGet', runId: R })).run, keys = Object.keys(FX.run.lines).filter(k => D_PLACED.some(c => c.poolId.startsWith(k + '_')));   // (the run's lines as runGet reads them: a patch keeps them beside the record)
  ok('B6 · their 9 order lines (11 pieces: two pairs) are "written" (every piece on Sheet 1); the BULLSEYE\'s line is not', keys.length === 9 && keys.every(k => run.lines[k].state === 'written') && run.lines['4239845119_5302267849'].state === 'pooled', keys.map(k => [k, run.lines[k].state]));
  const left = await Nest.sheetsOfRun(F.db, R);
  ok('B7 · the run is still stopped with its own words (its Resume is the owner\'s press), and no sheet of the run asks it to stop again (no problemStop left)', run.status === 'stopped' && run.stoppedBy === FX.run.stoppedBy && !left.some(r => r.problemStop) && !left.some(r => r.id === D),
    { status: run.status, problem: left.filter(r => r.problemStop).map(r => r.id) });
  ok('B8 · the request now waits for Draft 1\'s nest, saying so', c1.state === 'running' && /Nesting SPORTS 12 - BULLSEYE on RG Draft/.test(c1.step || ''), c1);
  // Draft 1's nest ends (the writer applies it): the BULLSEYE seated, its 13 where they were
  { const t2 = sheet(T), bull = active(t2).find(c => c.poolId === BULL); t2.placements = t2.placements.concat([{ id: bull.id, cxPt: 260, cyPt: 110, angle: 0, scale: 0.971, wPt: 32, hPt: 35.3 }]); t2.layoutRev += 1; t2.nestOwed = null; t2.dirty = false; t2.status = 'complete'; t2.placedCount = 14; F.seed('Charm_Nest_Sheets/' + T, t2); }
  F.clock.advance(60000);
  await pass();
  const c2 = cmdOf(r1.id);
  ok('B9 · answered done, in words: the 11 back where the nest left them, the cut set aside, Draft 2 removed (the run no longer stopped by it), the BULLSEYE nested on Draft 1',
    c2.state === 'done' && /^RG Sheet 1: 11 pieces back where nest-8462e548578a82c3789dfc62 left them \(29 on it, nothing nested again\); its automatic cut set aside, its physical sheet its own at its pre-cut outline; files, green line and cut follow\. RG Draft \d+ emptied, its stock given back, removed: the run is no longer stopped by it \(press Resume\)\. SPORTS 12 - BULLSEYE nested on RG Draft \d+: 14 of 14 placed/.test(c2.text || '') && (c2.text || '').length < 400, c2);

  /* ═══ C · idempotent ═══ */
  const snap = () => JSON.stringify([sheet(S), sheet(D), sheet(T), stock(), rem(1), rem(2), cutDoc(S), D_PLACED.map(c => pool(c.poolId)), pool(BULL)]);
  const before = snap(), writes0 = F.count().writes;
  await pass();
  ok('C · another pass changes nothing of the sheets, the stock or the pieces', snap() === before);
  const r2 = await ask(); await pass();
  const c3 = cmdOf(r2.id);
  ok('C · the same request asked again: answered done ("holds every piece of nest … already: it was not changed"), nothing changed (no cut set aside again, no move, no nest)', c3.state === 'done' && /holds every piece of nest-8462e548578a82c3789dfc62 already: it was not changed/.test(c3.text || '') && snap() === before
    && F.list('Charm_Nest_Jobs').filter(j => j.sheetId === T).length === 1, { c3, writes: F.count().writes - writes0 });
  { const r = await lib.call({ op: 'cmdRestoreNest', runId: R, sheetId: S, jobId: JOB, from: [D], by: 'Paul_K' }, { unit: 'test|replay' });
    ok('C · the op itself, sent again (a new unit, the draft gone): nothing to bring back, answered noop, nothing written', r.status === 200 && r.ok && r.noop === true && snap() === before, r); }

  /* ═══ D · the lane ═══ */
  seed({ steps: ['nest', 'cmd:renest'] });
  { const r = await ask(); await pass(); const c = cmdOf(r.id);
    ok('D · with only "cmd:renest" on (a sheet lane of its family), the request is the cloud\'s and is done', c.state === 'running' || c.state === 'done', c); }
  seed({ steps: ['nest', 'cmd:hop', 'cmd:release'] });
  { const r = await ask(); await pass(); const c = cmdOf(r.id);
    ok('D · with neither "cmd:restorenest", "cmd:renest" nor "cmd:seat" on: answered in words, nothing changed', c.state === 'failed' && /restorenest/.test(c.text || '') && untouched(), c); }

  ok('rule · Etsy was never called; no network but our own functions', F.etsy.calls === 0 && !F.net.tripwire.length && !F.net.other.length, { etsy: F.etsy.calls, trip: F.net.tripwire, other: F.net.other });
  log0(failed ? `FAIL rg-restore (${failed})` : 'PASS rg-restore');
  process.exit(failed ? 1 : 0);
})().catch(e => { log0('FAIL crashed: ' + (e && e.stack || e)); process.exit(1); });
