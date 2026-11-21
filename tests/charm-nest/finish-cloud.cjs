// CLOUDDRIVE build step 7b (cloud plan v2 "Step 7b. The writer's after-nest decisions, and the files-lane page gate"; C4, P1 (c), P9 lane "files"). Offline: the in-memory cloud of
// _cloudFakes.cjs (no network, Etsy a counted tripwire) and the REAL page code in a vm (charm-nest-viewer.js, the sheet window's file writers, the page's writeAsPlaced) over a stand-in cloud.
//   Part 1  the cloud's sheet writer after a nest (_charmNestFinish.js S8, finish-core's ported decisions, op finishOverflow):
//           a nest with 3 rejects ends with them on the next sheet (the rows' sheetId moved, A without them, A.releaseFull true, exactly one nestStart for the next sheet); with no next
//           sheet a new draft takes them (a replay makes no second draft); a draft gets the working fileBase; a feedWait sheet starts its next turn once; the oldest order that cannot fit
//           on an empty sheet stops the run with those words once (then the sheet is held); a released sheet stays released; the lane "files" is built (readiness no longer lists it).
//   Part 2  the files-lane page gate: with the cloud carrying (standby, "files" in cloudSteps, carrier "cloud"; or mode "on") each page writer (mcFiles, mcFilesResume, writeAsPlaced,
//           mcWriteStandalone) sends finishSheet { id, reason, by } and writes no file, and the person is told asked / done / refused; a computer carrying the run writes as today; with
//           the drive off every writer runs exactly as today and the gate reads and sends nothing.
//   NODE_PATH=<deps> node tests/charm-nest/finish-cloud.cjs
'use strict';
process.env.EDIT_PASSCODE = 'test-pass';
const fs = require('fs'), path = require('path'), vm = require('vm');
const T0 = Date.UTC(2026, 10, 20, 18, 0, 0);
const F = require('./_cloudFakes.cjs')({ now: T0 });
const out = (...a) => console.log(...a);
let failed = 0;
const ok = (name, cond, detail) => { out((cond ? 'OK   ' : 'FAIL ') + name + (cond || detail === undefined ? '' : ' :: ' + (typeof detail === 'string' ? detail : JSON.stringify(detail)).slice(0, 1500))); if (!cond) failed++; };
process.on('unhandledRejection', e => { out('FAIL an unhandled rejection: ' + ((e && e.stack) || e)); failed++; });
const quiet = console.warn; console.warn = () => {};

const root = path.join(__dirname, '../..');
const lib = F.load('charmNestLibrary.js');
const Finish = F.load('_charmNestFinish.js'), Drive = F.load('_charmNestDrive.js'), Standby = F.load('_charmNestStandby.js'), NestStep = F.load('_charmNestNestStep.js');
const FC = require(path.join(root, 'charm-nest-finish-core.js')), NP = require(path.join(root, 'charm-nest-nest-plan.js'));
const RUN = 'run-fc7b';

/* ── the shop (made-up data) ── */
const charm = (id, order, o = {}) => Object.assign({ id, poolId: id, order, name: 'Piece ' + id, hash: 'h' + id, sourceId: 'src-' + id, areaPt2: 600, widthPt: 30, heightPt: 25, metal: 'gold', orderDate: T0 - 86400e3, excluded: false }, o);
const pl = (id, x) => ({ id, cxPt: x, cyPt: 40, angle: 0, scale: 0.971, wPt: 30, hPt: 25 });
function sheet(id, o) {
  return Object.assign({ id, metal: 'gold', runId: RUN, page: 1, draft: true, setId: null, cardStartedAt: T0 - 3 * 3600e3, layoutRev: 3, contentRev: 3, verification: { ok: true }, status: 'partial', archived: false, endedBy: 'cap', usablePt2: 20000, freePt2: 5000 }, o);
}
function seedSheet(rec) { F.seed('Charm_Nest_Sheets/' + rec.id, rec); for (const c of rec.charms || []) F.seed('Charm_Pool/' + c.poolId, { poolId: c.poolId, state: 'pooled', sheetId: rec.id, runId: RUN, material: 'gold', orderId: String(c.order).split('/')[0], areaPt2: c.areaPt2, widthPt: c.widthPt, heightPt: c.heightPt }); }
function seedWorld() {
  F.seed('Charm_Nest_Open/drive', { mode: 'standby', runMode: 'auto', cloudSteps: ['pull', 'claim', 'pool', 'plan', 'nest', 'files'], at: T0 - 3600e3 });
  F.seed('Charm_Nest_Open/run', { runId: RUN });
  F.seed('Charm_Nest_Runs/' + RUN, { runId: RUN, status: 'running', step: 'nest', mode: 'auto', day: '2026-11-20', releasePolicy: 2, lines: {}, errors: [], startedAt: T0 - 4 * 3600e3 });
  F.seed('Charm_Nest_Run_Carrier/' + RUN, { runId: RUN, carrier: { dev: 'cloud', by: 'Cloud', at: T0, since: T0 - 3600e3 } });
}
/** The writer's context for one sheet's S8, as Finish.run builds it: Lib.call fenced on the sheet's task nonce, under the job's unit. */
function writerX(sheetId, job) {
  const nonce = 'n' + sheetId.replace(/[^\w]/g, '');
  F.seed('Charm_Nest_Finish/' + sheetId, { sheetId, runId: RUN, nonce, state: 'running', heartbeat: true, startedAt: Date.now(), beatAt: Date.now() });
  const task = { runId: RUN, job: Object.assign({ jobId: 'fin-test-' + sheetId, step: 'S8', steps: [] }, job) }, saved = JSON.parse(JSON.stringify(task.job));
  const fence = { path: 'Charm_Nest_Finish/' + sheetId, nonce, runId: RUN }, calls = [];
  const X = { db: F.db, bucket: F.bucket, sheetId, task, notes: [], log: () => {}, live: () => {}, calls, saved,
    saveTask: async p => { Object.assign(task, p); },
    call: async (body, unit) => { calls.push(JSON.parse(JSON.stringify(body))); return lib.call(body, { unit: `finish|${sheetId}|${saved.jobId}|${unit}`, fence }); },
    read: body => lib.call(body, {}) };
  return X;
}
const jobsOf = id => F.list('Charm_Nest_Jobs').filter(j => j.sheetId === id);
const nestCalls = (X, id) => X.calls.filter(b => b.op === 'nestStart' && b.sheetId === id).length;

(async () => {
  seedWorld();
  /* ═══ Part 1 · the cloud's writer after a nest ═══ */
  // A: a Gold Filled sheet whose nest placed two pieces and turned three away (three orders), released full; B: the next sheet in line, still filling
  const A = sheet('sheetA', { page: 1, fileBase: 'GF_working_shA', releaseFull: true, charms: [charm('5001_1_1', '5001'), charm('5002_1_1', '5002'), charm('5003_1_1', '5003'), charm('5004_1_1', '5004'), charm('5005_1_1', '5005')],
    placements: [pl('5001_1_1', 40), pl('5002_1_1', 90)], rejects: ['5003_1_1', '5004_1_1', '5005_1_1'] });
  A.poolIds = A.charms.map(c => c.poolId);
  const B = sheet('sheetB', { page: 2, cardStartedAt: T0 - 2 * 3600e3, fileBase: 'GF_working_shB', charms: [charm('5010_1_1', '5010')], placements: [pl('5010_1_1', 40)], rejects: [], usablePt2: 20000, freePt2: 19000 });
  B.poolIds = B.charms.map(c => c.poolId);
  seedSheet(A); seedSheet(B);
  {
    // the after-nest marks a nest's write carries: this nest (ended at the ceiling's search, 78 % shown, three orders that each fit an empty sheet turned away) releases the sheet full
    const sh = { sheetId: 'sheetA', metal: 'gold', runId: RUN, charms: A.charms, placements: A.placements, rejects: A.rejects.slice(), verification: { ok: true }, topup: null, nestInitial: [] };
    const S = { settings: { maxFill: 0.8, clearancePt: 0, nospread: 'on' } }, X = writerX('sheetA', {});
    const m = await Finish.afterNestMarks(X, sh, { endedBy: 'cap', density: 0.78, usablePt2: 20000, placedPt2: 15600, freePt2: 4400, trials: 9, rejects: A.rejects }, A.charms, { FC, NP, rec: Object.assign({}, A, { releaseFull: false }), S });
    ok('1 · after-nest marks: a finished search that turned away whole orders that fit an empty sheet releases the sheet full (sheetFull, the gap fill not started at 78 %)', m.releaseFull === true && m.intakeOptimized === true && m.intakeFinalized === false, m);
    const sh2 = Object.assign({}, sh, { placements: A.placements.slice(0, 1), rejects: [] });
    const m2 = await Finish.afterNestMarks(X, sh2, { endedBy: 'stopped', density: 0.2, usablePt2: 20000, placedPt2: 4000, freePt2: 16000, trials: 1 }, A.charms, { FC, NP, rec: Object.assign({}, A, { releaseFull: true }), S });
    ok('1 · RELEASEMARK: a released sheet stays released whatever a later nest says (only a person reopens it)', m2.releaseFull === true, m2);
    const sh3 = Object.assign({}, sh, { rejects: ['5003_1_1'] });
    const m3 = await Finish.afterNestMarks(X, sh3, { endedBy: 'cap', density: 0.70, usablePt2: 20000, placedPt2: 14000, freePt2: 6000, trials: 9, rejects: ['5003_1_1'] }, A.charms, { FC, NP, rec: Object.assign({}, A, { releaseFull: false }), S });
    ok('1 · topupSettle: a sheet full below 75 % on the open Auto run starts its gap fill instead of releasing (the order that missed moves on)', m3.releaseFull === false && m3.topup && m3.topup.tried && m3.topup.tried.length === 0, m3);
  }
  {
    const X = writerX('sheetA', { nestJobId: 'nest-sheetA-1', written: { layoutRev: 3, contentRev: 3 }, doneWhy: 'written: GF_working_shA.ai' });
    const r = await Finish.STEP.S8(X);
    const a = F.doc('Charm_Nest_Sheets/sheetA'), b = F.doc('Charm_Nest_Sheets/sheetB'), moved = ['5003_1_1', '5004_1_1', '5005_1_1'];
    ok('1 · S8 ends the job done', r === 'end' && X.task.lastDone && X.task.lastDone.outcome === 'done', { r, lastDone: X.task.lastDone, notes: X.notes });
    ok('1 · overflow: the three rejected pieces\' rows now name the next sheet (sheetB)', moved.every(id => (F.doc('Charm_Pool/' + id) || {}).sheetId === 'sheetB'), moved.map(id => (F.doc('Charm_Pool/' + id) || {}).sheetId));
    ok('1 · overflow: sheet A no longer lists them (charms, pool ids, rejects), keeps its two placed pieces and its layout, and stays released full',
      !a.charms.some(c => moved.includes(c.id)) && !a.poolIds.some(id => moved.includes(id)) && (a.rejects || []).length === 0 && a.placements.length === 2 && a.releaseFull === true && a.movedOn && a.movedOn.to === 'sheetB', a);
    ok('1 · overflow: the next sheet holds them, owes a nest and appends round its own layout', moved.every(id => b.poolIds.includes(id) && b.charms.some(c => c.id === id)) && b.nestOwed > 0 && b.appendOnly === true && b.placements.length === 1, b);
    ok('1 · overflow: exactly one nestStart for the next sheet, and one nest job of it', nestCalls(X, 'sheetB') === 1 && jobsOf('sheetB').length === 1, { calls: X.calls.map(c => c.op + ':' + (c.sheetId || c.id)), jobs: jobsOf('sheetB').map(j => j._id) });
    ok('1 · overflow: one transaction (finishOverflow), fenced on the writer\'s task, and no nest asked for sheet A', X.calls.filter(b => b.op === 'finishOverflow').length === 1 && nestCalls(X, 'sheetA') === 0, X.calls.map(c => c.op));
    // a replay of the step (a crash before its end was saved): A changed since, so nothing moves twice and no second nest is asked
    X.task.job = JSON.parse(JSON.stringify(X.saved)); const before = X.calls.length;
    await Finish.STEP.S8(X);
    ok('1 · a replay of S8 moves nothing again and asks no second nest', jobsOf('sheetB').length === 1 && !X.calls.slice(before).some(b => b.op === 'finishOverflow' || b.op === 'nestStart'), X.calls.slice(before).map(c => c.op));
  }
  {
    // no next sheet: a new draft takes them, its id this move's own (a replay of the same call makes no second draft)
    F.seed('Charm_Nest_Sheets/sheetB', Object.assign(F.doc('Charm_Nest_Sheets/sheetB'), { releaseFull: true }));   // (B is now full: shut)
    const C = sheet('sheetC', { page: 3, cardStartedAt: T0 - 3600e3, fileBase: 'GF_working_shC', releaseFull: true, charms: [charm('5020_1_1', '5020'), charm('5021_1_1', '5021')], placements: [pl('5020_1_1', 40)], rejects: ['5021_1_1'] });
    C.poolIds = C.charms.map(c => c.poolId); seedSheet(C);
    const drafts0 = F.list('Charm_Nest_Sheets').filter(s => /^d-/.test(s._id)).length;
    const X = writerX('sheetC', { nestJobId: 'nest-sheetC-1', written: { layoutRev: 3, contentRev: 3 } });
    await Finish.STEP.S8(X);
    const row = F.doc('Charm_Pool/5021_1_1') || {}, d = row.sheetId ? F.doc('Charm_Nest_Sheets/' + row.sheetId) : null;
    ok('1 · overflow with no sheet open in line: a new draft takes the piece (draft, no set, owes a nest) and is asked to nest once',
      !!d && /^d-/.test(row.sheetId) && d.draft === true && !d.setId && d.poolIds.includes('5021_1_1') && d.nestOwed > 0 && nestCalls(X, row.sheetId) === 1 && jobsOf(row.sheetId).length === 1, { row, d, calls: X.calls.map(c => c.op) });
    const call = X.calls.find(b => b.op === 'finishOverflow');
    const again = await lib.call(call, { unit: `finish|sheetC|${X.saved.jobId}|overflow`, fence: { path: 'Charm_Nest_Finish/sheetC', nonce: 'nsheetC', runId: RUN } });
    ok('1 · the same move replayed makes no second draft', F.list('Charm_Nest_Sheets').filter(s => /^d-/.test(s._id)).length === drafts0 + 1 && (again.replayed === true || again.already === true || again.status === 409), again);
  }
  {
    // a draft gets the working fileBase (finish-core sheetFileBase), filed as a sheet of its day, never under a set it names but is not in (RELEASEMARK)
    F.seed('Charm_Nest_Sets/set-9', { setId: 'set-9', runId: RUN, sheetIds: ['other'], day: '2026-11-19', seq: 9 });
    const X = { db: F.db, rec: { id: 'd-0a1b2c3d4e5f6071', metal: 'gold', runId: RUN, draft: true, setId: 'set-9', setSeq: 9, day: '2026-11-20' }, O: require(path.join(root, 'charm-nest-orders.js')), Host: require(path.join(F.fnDir, '_charmNestFinishHost.js')), FC };
    const p = await Finish.pathsOf(X);
    ok('1 · a draft gets the working fileBase (GF_working_<id>) and is filed under its own day, not the set it names', p.name === 'GF_working_d-0a1b2c3d4e5f6071' && p.base === 'charmnest/sheets/2026-11-20/GF_working_d-0a1b2c3d4e5f6071', p);
    const X2 = Object.assign({}, X, { rec: Object.assign({}, X.rec, { fileBase: 'GF_Nov.20.26_Set-2_Sheet-1' }), paths: null });
    const p2 = await Finish.pathsOf(X2);
    ok('1 · an existing file name is never changed', p2.name === 'GF_Nov.20.26_Set-2_Sheet-1', p2);
    ok('1 · finish-core sheetFileBase: a draft or a sheet owed its number is working; a set sheet is named by its set', FC.sheetFileBase({ metal: 'silver', sheetId: 'x1', numberOwed: true }, { tag: () => 'SS', sheetName: () => 'no' }) === 'SS_working_x1'
      && FC.sheetFileBase({ metal: 'gold', sheetId: 'x2', seq: 3, sheetIndex: 2, setDay: '2026-11-20' }, { tag: () => 'GF', sheetName: (m, day, seq, i) => `${m}|${day}|${seq}|${i}` }) === 'gold|2026-11-20|3|2');
  }
  {
    // feedOn: a sheet whose nest left its later orders waiting their turn (feedWait) asks its next turn once
    const D = sheet('sheetD', { page: 4, cardStartedAt: T0 - 1800e3, fileBase: 'GF_working_shD', charms: [charm('5030_1_1', '5030'), charm('5031_1_1', '5031'), charm('5032_1_1', '5032')], placements: [pl('5030_1_1', 40)], rejects: [], feedWait: ['5031_1_1', '5032_1_1'], freePt2: 18000 });
    D.poolIds = D.charms.map(c => c.poolId); seedSheet(D);
    const X = writerX('sheetD', { nestJobId: 'nest-sheetD-1', written: { layoutRev: 3, contentRev: 3 } });
    await Finish.STEP.S8(X);
    ok('1 · feedOn: a feedWait sheet asks its next turn: exactly one nestStart and one nest job of it, nothing moved', nestCalls(X, 'sheetD') === 1 && jobsOf('sheetD').length === 1 && !X.calls.some(b => b.op === 'finishOverflow'), X.calls.map(c => c.op + ':' + (c.sheetId || c.id)));
    X.task.job = JSON.parse(JSON.stringify(X.saved));
    await Finish.STEP.S8(X);
    ok('1 · feedOn: replayed, the turn is still asked once (the ledger answers the same ask, no second job)', jobsOf('sheetD').length === 1, jobsOf('sheetD').map(j => j._id));
    const view = { metal: 'gold', placements: [pl('a', 1)], charms: [charm('a', '1'), charm('b', '2')], feedWait: ['b'], releaseFull: true, verification: { ok: true } };
    ok('1 · feedOn on a full sheet passes the waiting orders on (they go to the next sheet)', FC.feedNext(view).act === 'pass' && FC.feedNext(view).pass[0] === 'b');
    const unv = { metal: 'gold', placements: [pl('a', 1)], charms: [charm('a', '1'), charm('b', '2')], feedWait: ['b'], verification: { ok: false } };
    ok('1 · NOFLEE: a sheet that missed the gap check keeps its waiting orders and nests again (never sends them to the next draft)', FC.feedNext(unv).act === 'nest');
  }
  {
    // (d) the oldest order cannot fit on an empty sheet: the run stops with those words once; after a Resume the sheet is held
    const E = sheet('sheetE', { page: 5, cardStartedAt: T0 - 900e3, fileBase: 'GF_working_shE', charms: [charm('5040_1_1', '5040', { areaPt2: 90000 })], placements: [], rejects: ['5040_1_1'], status: 'partial' });
    E.poolIds = E.charms.map(c => c.poolId); seedSheet(E);
    const X = writerX('sheetE', { nestJobId: 'nest-sheetE-1', written: { layoutRev: 3, contentRev: 3 } });
    await Finish.STEP.S8(X);
    const e = F.doc('Charm_Nest_Sheets/sheetE');
    ok('1 · the oldest order that cannot fit on an empty sheet: the sheet says so (problemStop) and keeps the piece', e.problemStop && /The oldest order cannot fit on an empty sheet/.test(e.problemStop.why) && e.poolIds.includes('5040_1_1') && !X.calls.some(b => b.op === 'finishOverflow'), e);
    const run = F.doc('Charm_Nest_Runs/' + RUN);
    const ctx = { db: F.db, run, runId: RUN, cursor: {}, budgetEnd: Date.now() + 30000, call: (b, u) => lib.call(b, { unit: 'nest-test|' + u }) };
    const r1 = await NestStep.nestUnit(ctx);
    ok('1 · the nest step stops the run with those words', !!(r1 && r1.stop && /oldest order cannot fit on an empty sheet/.test(r1.stop.why) && r1.stop.kind === 'error'), r1);
    run.errors = (run.errors || []).concat([{ t: Date.now(), why: r1 && r1.stop && r1.stop.why }]);
    const r2 = await NestStep.nestUnit(Object.assign({}, ctx, { run }));
    ok('1 · after the stop was said (a person resumed), the sheet is held and the run goes on', !(r2 && r2.stop) && run.sheetHolds && /oldest order/.test(String(run.sheetHolds.sheetE || '')), { r2, holds: run.sheetHolds });
  }
  {
    // the lane "files" is built: readiness no longer lists it
    const lanes = Drive.defaultLanes(), ready = await Standby.readiness(F.db, { steps: Drive.defaultSteps(), lanes, now: Date.now() });
    ok('1 · the lane "files" is built (its pass item), and readiness no longer lists "writing a sheet\'s files"', lanes.files === true && Standby.laneBuilt(lanes, 'files') && !ready.missing.some(m => /writing a sheet's files/.test(m)), ready.missing);
    const Steps = F.load('_charmNestSteps.js'), item = Steps.passItems.find(i => i.name === 'files');
    ok('1 · its pass item is lane "files" (the driver skips it in a pass that already looked: one files look a pass)', !!item && item.lane === 'files' && /item\.name === "files" && filesLooked/.test(fs.readFileSync(path.join(F.fnDir, '_charmNestDrive.js'), 'utf8')));
  }
  ok('1 · Etsy never called, no other address fetched, the Etsy switch never written', F.etsy.calls === 0 && !F.net.tripwire.length && !F.net.other.length && !F.stopWrites.length, { etsy: F.etsy.calls, trip: F.net.tripwire, other: F.net.other });

  /* ═══ Part 2 · the files-lane page gate (the REAL page code in a vm, over a stand-in cloud) ═══ */
  const viewer = fs.readFileSync(path.join(root, 'charm-nest-viewer.js'), 'utf8'), sheetwin = fs.readFileSync(path.join(root, 'charm-nest-sheetwin.js'), 'utf8'), html = fs.readFileSync(path.join(root, 'charm-nest-1.html'), 'utf8');
  const cutFrom = (src, from, to) => { const a = src.indexOf(from), b = src.indexOf(to, a + from.length); if (a < 0 || b < 0) throw new Error('missing ' + from); return src.slice(a, b); };
  const mcBlock = cutFrom(sheetwin, '  const MCF = new Map(), MCF_TRIED = new Map();', '  setInterval(() => tryDo(mcFilesResume), 10000);');
  const standalone = cutFrom(sheetwin, '  async function mcWriteStandalone(sh, st, paint) {', '  /** ARTUNCUT');
  const wap = cutFrom(html, 'function writeAsPlaced(sh) {', '/* A big batch goes onto a sheet a few orders at a time');
  async function page(o) {
    const cloud = { calls: [], owedReads: 0 };
    const answer = body => {
      cloud.calls.push(body);
      switch (body.op) {
        case 'driveGet': return { ok: true, mode: o.mode, drive: o.mode === 'off' ? null : { mode: o.mode, cloudSteps: o.steps || [] } };
        case 'runCarrier': return { ok: true, carrier: { dev: o.carrier, by: o.carrier === 'cloud' ? 'Cloud' : 'Ann', at: Date.now() } };
        case 'finishSheet': return o.refuse ? { error: o.refuse, status: 409 } : { ok: true, id: body.id, owedAt: Date.now(), queued: true };
        case 'finishGet': cloud.owedReads++; return { ok: true, id: body.id, sheet: cloud.owedReads >= 2 ? { filesOwed: null, filesJob: null } : { filesOwed: Date.now(), filesJob: { jobId: 'fin-x' } }, task: null };
        default: return { ok: true };
      }
    };
    const local = { rewrite: 0, labels: 0, finishNest: 0, uploads: 0, putSheet: 0 }, toasts = [], settled = [], pending = o.pending || [];
    const c = vm.createContext({ console: { log() {}, warn() {}, error() {} }, setTimeout: (f, ms) => setTimeout(f, Math.min(+ms || 0, 5)), clearTimeout, setInterval: () => 0, clearInterval() {}, Promise, JSON, Date, Math, Map, Set, Object, Array, String, Number, Error, performance: require('perf_hooks').performance });
    c.cloudCall = async body => { const r = answer(JSON.parse(JSON.stringify(body))); if (r && r.error) throw Object.assign(new Error(r.error), { status: r.status || 400, data: r }); return r; };
    c.local = local; c.toasts = toasts; c.settled = settled; c.pendingJobs = pending;
    vm.runInContext(`
      var window = this, self = this;
      var CN = { api: (name, body) => cloudCall(body) };
      var toast = (t, tone) => { toasts.push({ t: String(t), tone: tone || '' }); };
      var employeeName = () => 'Ann';
      var CharmNestRunFollow = { dev: () => 'dev-ann', state: () => ({ runId: '${RUN}' }), now() {} };
      var B = { run: null, employee: 'Ann' };
      var CharmNestStore = { settle: (k, a) => settled.push(k), pending: f => pendingJobs.filter(f), mutate: () => ({}) };
    `, c);
    vm.runInContext(viewer, c);
    await vm.runInContext('CNViewer.ready()', c);
    vm.runInContext(`
      var sheets = [];
      var allSheets = () => sheets, busy = () => false, MC = { on: false }, HOP = { writing: new Set(), saving: new Set() }, W = { rec: null };
      var tryDo = f => { try { return f(); } catch (e) { return undefined; } }, within = p => Promise.resolve(p), pause = () => Promise.resolve(), spinScale = () => 0;
      var stepOf = () => ({ state: '', detail: '' }), sheetWord = (id, fb) => fb || id, whoAmI = () => 'Ann';
      var holdRelease = () => {}, letGo = () => {}, goldAfter = async () => {}, remakeLabels = async () => { local.labels++; };
      var rewritePage = async sh => { local.rewrite++; sh.persistedDone = true; return true; };
      var api = async (name, body) => { if (body && body.op === 'putSheet') local.putSheet++; return { ok: true }; };
      var uploadBytes = async () => { local.uploads++; return { path: 'p', url: 'u' }; };
      var activeCharms = sh => sh.charms, sheetDirty = () => {}, startNest = () => {}, stockFor = () => ({ wPt: 100, hPt: 100 }), manualSize = () => 1, uid = () => 'u1', log = () => {}, renderCard = () => {};
      var buildJob = () => ({ seed: 1, angles: [0], clearancePt: 0, sheet: { insetPt: 0 }, fineRes: 1, coarseRes: 1, maxFill: 0.8 });
      var finishNest = sh => { local.finishNest++; sh.persistedDone = true; sh.persisted = Promise.resolve(); return Promise.resolve(); };
      ${mcBlock}
      ${standalone}
      ${wap}
      var api0 = { mcFiles, mcFilesResume, mcWriteStandalone, writeAsPlaced };
    `, c);
    const mk = id => vm.runInContext(`(() => { const s = { sheetId: '${id}', runId: '${RUN}', metal: 'gold', fileBase: 'GF_working_${id}', charms: [{ id: 'c1' }], placements: [{ id: 'c1', cxPt: 1, cyPt: 1, angle: 0 }], status: 'complete', persistedDone: true }; sheets.push(s); return s; })()`, c);
    const settle = () => new Promise(r => setTimeout(r, 60));
    return { c, cloud, local, toasts, settled, mk, settle, ev: code => vm.runInContext(code, c) };
  }
  const writes = l => l.rewrite + l.labels + l.finishNest + l.uploads + l.putSheet;
  // A · the drive off: every writer runs here exactly as today; the gate reads and sends nothing
  {
    const P = await page({ mode: 'off', carrier: 'dev-ann', pending: [{ k: 'job1', job: 'mcFiles', mid: 'job-s4', after: 'patch-answered', args: { sheetId: 's4' }, at: Date.now() }] });
    const reads0 = P.cloud.calls.length, s1 = P.mk('s1'), s2 = P.mk('s2'), s3 = P.mk('s3'); P.mk('s4');
    const r1 = await P.ev('api0.mcFiles')(s1, {});
    const r2 = await P.ev('api0.writeAsPlaced')(s2);
    const st = { state: '', detail: '' }; let thrown = '';
    try { await P.ev('api0.mcWriteStandalone')(s3, st, () => {}); } catch (e) { thrown = e.message; }
    P.ev('api0.mcFilesResume')(); await P.settle();
    ok('2 · drive off: mcFiles writes here (the page\'s own rewrite, labels, the record told) and answers true', r1 === true && P.local.rewrite >= 1 && P.local.labels >= 1, P.local);
    ok('2 · drive off: writeAsPlaced writes here (finishNest, as it stands)', r2 === true && P.local.finishNest === 1, P.local);
    ok('2 · drive off: mcWriteStandalone goes to its own writer (here: "the sheet writer is not loaded", the stand-in page has none)', /not loaded in this page/.test(thrown) && /written from its saved record/.test(st.detail), { thrown, st });
    ok('2 · drive off: mcFilesResume runs the outbox file job here', P.local.rewrite >= 2, P.local);
    ok('2 · drive off: no finishSheet, and the gate read nothing (no driveGet, no runCarrier beyond the page\'s own boot read)', !P.cloud.calls.some(b => b.op === 'finishSheet') && P.cloud.calls.length === reads0, P.cloud.calls.map(b => b.op));
  }
  // B · standby, "files" in cloudSteps, the cloud carries the run: each writer sends finishSheet { id, reason, by } and writes no file; asked, then done
  {
    const P = await page({ mode: 'standby', steps: ['nest', 'files'], carrier: 'cloud', pending: [{ k: 'job9', job: 'mcFiles', mid: 'job-t4', after: 'patch-answered', args: { sheetId: 't4' }, at: Date.now() }] });
    const s1 = P.mk('t1'), s2 = P.mk('t2'), s3 = P.mk('t3'); P.mk('t4');
    const st0 = { state: '', detail: '' }, lb0 = { state: '', detail: '' };
    const r1 = await P.ev('api0.mcFiles')(s1, { files: st0, labels: lb0, paint() {} });
    const r2 = await P.ev('api0.writeAsPlaced')(s2);
    const st = { state: '', detail: '' };
    const r3 = await P.ev('api0.mcWriteStandalone')(s3, st, () => {});
    P.ev('api0.mcFilesResume')(); await P.settle(); await P.settle();
    const fin = P.cloud.calls.filter(b => b.op === 'finishSheet');
    ok('2 · cloud carrying: mcFiles, writeAsPlaced, mcWriteStandalone and mcFilesResume each send finishSheet once, with id, reason and by',
      r1 === true && r2 === true && r3 === true && ['t1', 't2', 't3', 't4'].every(id => fin.filter(b => b.id === id).length === 1) && fin.every(b => b.reason && b.by === 'Ann'), fin);
    ok('2 · cloud carrying: no file is written here (no rewrite, label, finishNest, upload or putSheet)', writes(P.local) === 0, P.local);
    ok('2 · cloud carrying: the outbox file job is the cloud\'s once asked (settled)', P.settled.includes('job9'), P.settled);
    ok('2 · cloud carrying: the sheet window\'s steps say the cloud writes the files', st0.state === 'ok' && /cloud writes its files/.test(st0.detail) && /cloud/.test(lb0.detail) && st.state === 'ok', { st0, lb0, st });
    ok('2 · cloud carrying: the person is told in words: asked, then done', P.toasts.some(t => /asked — the cloud writes its files/.test(t.t)) && P.toasts.some(t => /done — the cloud wrote its files/.test(t.t)), P.toasts);
  }
  // C · standby with the lane on, but a computer carries the run: that computer writes as today (one lane, one place)
  {
    const P = await page({ mode: 'standby', steps: ['files'], carrier: 'dev-ann' });
    const s1 = P.mk('u1'), r1 = await P.ev('api0.mcFiles')(s1, {}), r2 = await P.ev('api0.writeAsPlaced')(P.mk('u2'));
    ok('2 · standby, a computer carries the run: the files are written here, no finishSheet', r1 === true && r2 === true && P.local.rewrite === 1 && P.local.finishNest === 1 && !P.cloud.calls.some(b => b.op === 'finishSheet'), { local: P.local, calls: P.cloud.calls.map(b => b.op) });
  }
  // D · standby, the cloud carries the run but "files" is not one of its lanes: written here
  {
    const P = await page({ mode: 'standby', steps: ['nest'], carrier: 'cloud' });
    const r1 = await P.ev('api0.writeAsPlaced')(P.mk('v1'));
    ok('2 · standby, the files lane not on: written here, no finishSheet', r1 === true && P.local.finishNest === 1 && !P.cloud.calls.some(b => b.op === 'finishSheet'), P.cloud.calls.map(b => b.op));
  }
  // E · mode "on": the cloud writes every file; a refusal is said in words, and nothing is written here
  {
    const P = await page({ mode: 'on', steps: ['files'], carrier: 'cloud', refuse: 'The cloud does not write sheet files yet (its files lane is not on)' });
    const st = { state: '', detail: '' };
    const r1 = await P.ev('api0.mcFiles')(P.mk('w1'), { files: st, paint() {} }), r2 = await P.ev('api0.writeAsPlaced')(P.mk('w2')), r3 = await P.ev('api0.mcWriteStandalone')(P.mk('w3'), { state: '', detail: '' }, () => {});
    ok('2 · mode "on", refused: each writer answers false, writes nothing here, and the person is told why ("refused — …")',
      r1 === false && r2 === false && r3 === false && writes(P.local) === 0 && P.toasts.filter(t => /refused — the cloud did not take its files \(The cloud does not write sheet files yet/.test(t.t)).length === 3 && st.state === 'bad', { local: P.local, toasts: P.toasts });
  }
  console.warn = quiet;
  out(failed ? `\n${failed} FAILED` : '\nALL PASSED');
  process.exit(failed ? 1 : 0);
})().catch(e => { out('FAIL crashed: ' + ((e && e.stack) || e)); process.exit(1); });
