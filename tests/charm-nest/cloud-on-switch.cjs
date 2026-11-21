// THE ON SWITCH (ONSWITCH, Paul 20 Nov 2026: the cloud carries every run tonight, Charm_Nest_Open/drive mode "on"; pages are viewers), offline: the real library handler, the real
// Storage door (charmNestOutput), the real driver (_charmNestDrive.js pass) with the real steps of _charmNestSteps.js, against the in-memory cloud of _cloudFakes.cjs with a fake clock.
//   A  readiness: every run step is built (the "engrave" step is no stub any more) and every lane (the files lane, built on another branch, is a stub flag here); only a computer OPEN
//      NOW (seen in the last 10 minutes) must have the viewer build, one seen 5 to 13 hours ago is listed for later and does not hold the switch; "Cloud carries" lists every lane a
//      pass item runs on (rosecut and sweeps too); the viewer build says "viewer" in the computer's report whichever of charm-nest-drive.js / charm-nest-viewer.js loads first
//   B  an OLD page (no viewer, no cv) that opens while the mode is "on" does no production: its production writes and its presses get the reload words (409), its writes of a
//      sheet's or a set's files are refused at the Storage door, and it never takes the run (runCarrier); a current page never takes it either
//   C  the "engrave" step runs the engraving lane once more at its moment, counts what waits, and never blocks (a lane unit that has more to do or fails leaves the run going on)
//   D  the owner's hand-over (Standby, then Cloud carries) from his carrying computer (dc6v5bhnw2q): the press waits for its idle report, the mode flips with the carrier in one
//      transaction, and the cloud's FIRST pass on the resting run run-2026-11-12-wrtgl8fukc forms the set of GF Draft 1 and GF Draft 3 (both released, no shared orders) by the
//      shared core's own rules, Set 5 untouched; labels and commit only as their own units say (here the Etsy freshness gate holds them: nothing is committed)
// Asserted throughout: Etsy is never called, no other address is fetched, the Etsy switch is never written.
//   NODE_PATH=<deps> node tests/charm-nest/cloud-on-switch.cjs
'use strict';
process.env.EDIT_PASSCODE = 'test-pass';
const fs = require('fs'), path = require('path'), vm = require('vm');
const ROOT = path.join(__dirname, '../..');
const T0 = Date.UTC(2026, 10, 20, 23, 0, 0);
const F = require('./_cloudFakes.cjs')({ now: T0 });
const VERBOSE = !!process.env.CN_VERBOSE, said = [], log0 = console.log.bind(console);
console.log = (...a) => { said.push(a.join(' ')); if (VERBOSE) log0(...a); };
console.warn = (...a) => { said.push('WARN ' + a.join(' ')); if (VERBOSE) log0('WARN', ...a); };
let failed = 0;
const ok = (name, cond, detail) => { log0((cond ? 'OK   ' : 'FAIL ') + name + (cond || detail === undefined ? '' : ' :: ' + (typeof detail === 'string' ? detail : JSON.stringify(detail)).slice(0, 1500))); if (!cond) failed++; };
process.on('unhandledRejection', e => { log0('FAIL an unhandled rejection: ' + ((e && e.stack) || e)); failed++; });

const lib = F.load('charmNestLibrary.js'), Drive = F.load('_charmNestDrive.js'), Standby = F.load('_charmNestStandby.js'), Lease = F.load('_charmNestLease.js'), Steps = F.load('_charmNestSteps.js');
const Output = F.load('charmNestOutput.js'), EStep = F.load('_charmEngraveStep.js');
const repoStamp = require('./_repoToken.cjs');
const P = async body => { const r = await lib.handler({ httpMethod: 'POST', headers: { 'x-edit-passcode': 'test-pass' }, body: JSON.stringify(body) }); let o = {}; try { o = JSON.parse(r.body || '{}'); } catch (_) {} return Object.assign({}, o, { http: r.statusCode }); };
const PS = async body => P(await repoStamp('charmNestLibrary', body, F.db));   // (the owner's Settings press, unlocked as a signed-in owner would be: nothing real)
const OUT = async body => { const r = await Output.handler({ httpMethod: 'POST', headers: { 'x-edit-passcode': 'test-pass' }, body: JSON.stringify(body) }); let o = {}; try { o = JSON.parse(r.body || '{}'); } catch (_) {} return Object.assign({}, o, { http: r.statusCode }); };
/* the files lane ("writing a sheet's files") is built on another branch (fix-fileslane): here a stub flag says it is built, for the library's Settings ops and for every pass */
const realLanes = Drive.defaultLanes, lanesWithFiles = () => Object.assign(realLanes(), { files: true });
Drive.defaultLanes = lanesWithFiles;
const noSleep = async () => {};
const CV = 'cloud14-20261120', OWNER = 'dc6v5bhnw2q', OLD = 'd7oldpage01';
const ON = () => Standby.ON_STEPS.slice();
async function nextNonce() {
  F.net.kicks.length = 0;
  const L = F.doc('Charm_Nest_Drive/lease') || {};
  if (L.state === 'dispatched' && L.nonce && Date.now() - (+L.at || 0) < 110000) return L.nonce;
  return (await Lease.dispatch(F.db, F.db.collection('Charm_Nest_Drive').doc('lease'), 'test', { heartbeat: true })).nonce;
}
const pass = async (o = {}) => Drive.pass(Object.assign({ db: F.db, Lib: lib, nonce: await nextNonce(), steps: Drive.defaultSteps(), lanes: lanesWithFiles(), passItems: [], sleep: noSleep, renewMs: 0, budgetMs: 600000 }, o));
/** The owner's computer (open now, reloaded on the viewer build) and the shop's seven computers closed tonight (last seen 5 to 13 hours ago, on the build before the viewers). */
function seedComputers(now) {
  F.seed('Charm_Nest_Computers/' + OWNER, { dev: OWNER, by: 'Paul', station: 'pc-BKQMVHQFGA7U', role: 'carrier', at: now - 60000, drive: 'standby drain viewer' });
  for (let i = 0; i < 7; i++) F.seed('Charm_Nest_Computers/d0shop000' + i, { dev: 'd0shop000' + i, by: 'Staff ' + i, station: 'pc-SHOP' + i, role: 'follower', at: now - (5 + i) * 3600e3 - 60000 * i, drive: 'standby drain' });
}

(async () => {
  /* ═══ A · readiness ═══ */
  {
    F.reset(); F.clock.set(T0); seedComputers(Date.now());
    const steps = Drive.defaultSteps();
    ok('A · every run step is built in the cloud (the "engrave" step is no stub: _charmEngraveStep.js)', Drive.RUN_STEPS.every(s => Standby.isBuilt(steps, s)) && Steps.loaded.includes('./_charmEngraveStep'), Drive.RUN_STEPS.filter(s => !Standby.isBuilt(steps, s)));
    const noFiles = await Standby.readiness(F.db, { steps, lanes: realLanes(), now: Date.now() });
    // (merged with fix-fileslane, build step 7b: the files lane is built in the driver's own lanes now, so the driver's readiness misses nothing)
    ok('A · with the files lane merged (7b), the driver\'s own steps and lanes miss nothing', noFiles.ok && noFiles.missing.length === 0, noFiles.missing);
    const r = await Standby.readiness(F.db, { steps, lanes: lanesWithFiles(), now: Date.now() });
    ok('A · with the files lane built: ready, and the seven computers closed tonight do not hold the switch', r.ok && !r.missing.length, r.missing);
    ok('A · ... they are listed for later: each loads the viewer page when it is opened', r.later.length === 1 && /not open in the last 10 minutes/.test(r.later[0]) && [0, 1, 2, 3, 4, 5, 6].every(i => r.later[0].includes('pc-SHOP' + i)) && !r.later[0].includes('pc-BKQMVHQFGA7U'), r.later);
    F.seed('Charm_Nest_Computers/d0open0001', { dev: 'd0open0001', by: 'Ann', station: 'pc-OPEN', at: Date.now() - 3 * 60000, drive: 'standby drain' });
    const r2 = await Standby.readiness(F.db, { steps, lanes: lanesWithFiles(), now: Date.now() });
    ok('A · a computer open now (seen 3 minutes ago) without the viewer build holds the switch, named in words', !r2.ok && r2.missing.length === 1 && /open and has not loaded the viewer build/.test(r2.missing[0]) && r2.missing[0].includes('Ann on pc-OPEN'), r2.missing);
    F.seed('Charm_Nest_Computers/d0open0001', { dev: 'd0open0001', by: 'Ann', station: 'pc-OPEN', at: Date.now() - 11 * 60000, drive: 'standby drain' });
    const r3 = await Standby.readiness(F.db, { steps, lanes: lanesWithFiles(), now: Date.now() });
    ok('A · seen 11 minutes ago it is no longer open: listed for later, the switch is free', r3.ok && r3.later[0].includes('Ann on pc-OPEN'), r3);
    ok('A · "Cloud carries" lists every run step and the lanes files, takeoff, engrave, rosecut and sweeps', Drive.RUN_STEPS.concat(['files', 'takeoff', 'engrave', 'rosecut', 'sweeps']).every(s => ON().includes(s)), ON());
    ok('A · every pass item\'s lane is on in "Cloud carries" (none would sit idle in mode on)', Steps.passItems.every(it => Drive.laneOn(ON(), String(it.lane || it.name))), Steps.passItems.filter(it => !Drive.laneOn(ON(), String(it.lane || it.name))).map(it => [it.name, it.lane]));
    // the viewer build says so in the computer's report (charm-nest-drift.js sends CharmNestDrive.caps()), whichever file loads first
    const src = f => fs.readFileSync(path.join(ROOT, f), 'utf8');
    const page = () => { const timers = []; const w = { console: { log() {}, warn() {}, error() {} }, setTimeout: f => { timers.push(f); return timers.length; }, clearTimeout() {}, setInterval: () => 0, clearInterval() {} }; w.window = w; return { w, timers, c: vm.createContext(w) }; };
    { const { w, c } = page(); vm.runInContext(src('charm-nest-drive.js'), c); ok('A · an old page (no viewer on it) reports "standby drain" only', w.CharmNestDrive.caps() === 'standby drain', w.CharmNestDrive.caps()); }
    { const { w, c } = page(); vm.runInContext(src('charm-nest-drive.js'), c); vm.runInContext(src('charm-nest-viewer.js'), c); ok('A · drive then viewer (charm-nest-1.html\'s order): the report says "viewer"', w.CharmNestDrive.caps().split(' ').includes('viewer'), w.CharmNestDrive.caps()); }
    { const { w, c, timers } = page(); vm.runInContext(src('charm-nest-viewer.js'), c); vm.runInContext(src('charm-nest-drive.js'), c); const before = w.CharmNestDrive.caps(); for (const f of timers.splice(0)) f(); ok('A · viewer then drive: the report says "viewer" too', before.split(' ').includes('viewer') && w.CharmNestDrive.caps().split(' ').includes('viewer'), { before, after: w.CharmNestDrive.caps() }); }
    const html = src('charm-nest-1.html');
    ok('A · charm-nest-1.html loads the changed drive and viewer files under new tokens (-on1)', /charm-nest-drive\.js\?v=1120-drive1-on1"/.test(html) && /charm-nest-viewer\.js\?v=1120-v14-on1[-\w]*"/.test(html));
  }

  /* ═══ B · an old page in mode "on" does no production ═══ */
  {
    F.reset(); F.clock.set(T0);
    F.seed('Charm_Nest_Open/drive', { mode: 'on', runMode: 'manual', cloudSteps: ON(), flippedAt: Date.now() - 60000 }); lib._driveForget();
    const R = 'run-2026-11-20-onbbb', R2 = 'run-2026-11-20-onbb2';
    F.seed('Charm_Nest_Runs/' + R, { runId: R, releasePolicy: 2, status: 'processed', step: 'complete', mode: 'auto', day: '2026-11-20', sheets: {}, holds: {}, errors: [], orders: [] });
    F.seed('Charm_Nest_Open/run', { runId: R, at: Date.now() });
    F.seed('Charm_Nest_Run_Carrier/' + R, { runId: R, carrier: { dev: 'cloud', by: 'Cloud', at: Date.now(), since: Date.now() - 60000 } });
    const reload = r => r.http === 409 && r.reload === true && /This page is an old version: it reloads/.test(r.error || '');
    let r = await P({ op: 'runPut', run: { runId: R, status: 'running', step: 'nest' }, merge: true, dev: OLD });
    ok('B · an old page\'s production write (runPut) gets the reload words, and nothing is written', reload(r) && F.doc('Charm_Nest_Runs/' + R).status === 'processed', r);
    r = await P({ op: 'putSheet', sheet: { id: 'gold-s30-old', runId: R, metal: 'gold', page: 30, placements: [{ id: 'c0', cxPt: 1, cyPt: 1, angle: 0 }], charms: [{ id: 'c0', poolId: '9_1_1' }], poolIds: ['9_1_1'] }, dev: OLD });
    ok('B · its sheet save (putSheet with pieces) gets the reload words, and no sheet is made', reload(r) && !F.doc('Charm_Nest_Sheets/gold-s30-old'), r);
    r = await P({ op: 'putSheet', sheet: { id: 'gold-s30-old', runId: R, metal: 'gold', placements: [] }, dev: OLD, cv: CV });
    ok('B · ... and a current page\'s save of a sheet\'s pieces is refused in mode on too (the cloud\'s work)', r.http === 409 && r.cloudCarries === true && !r.reload, r);
    r = await P({ op: 'runLinesPatch', runId: R, lines: { '4300000001_1': { state: 'pooled' } }, dev: OLD });
    ok('B · its press (runLinesPatch) gets the reload words', reload(r), r);
    r = await P({ op: 'runCarrier', runId: R, dev: OLD, by: 'Old page', take: true });
    ok('B · it never takes the run: runCarrier (even with take) gets the reload words, the cloud still carries', reload(r) && F.doc('Charm_Nest_Run_Carrier/' + R).carrier.dev === 'cloud', { r, carrier: F.doc('Charm_Nest_Run_Carrier/' + R) });
    r = await P({ op: 'computerReport', dev: OLD, by: 'Old page', report: { drive: 'standby drain' } });
    ok('B · its computer report is refused too (an old page is never counted as seen in mode on)', reload(r) && !F.doc('Charm_Nest_Computers/' + OLD), r);
    r = await P({ op: 'runCarrier', runId: R, dev: OWNER, by: 'Paul', take: true, cv: CV });
    ok('B · a current page\'s take is answered "the cloud carries" and changes nothing', r.ok === true && r.mine === false && r.cloud === true && F.doc('Charm_Nest_Run_Carrier/' + R).carrier.dev === 'cloud', r);
    // (a run a computer carries in mode on should never be: the flip and runOpen give every run to the cloud; no page takes it all the same)
    F.seed('Charm_Nest_Runs/' + R2, { runId: R2, releasePolicy: 2, status: 'running', step: 'nest', mode: 'auto', day: '2026-11-20', sheets: {}, holds: {}, errors: [], orders: [] });
    F.seed('Charm_Nest_Run_Carrier/' + R2, { runId: R2, carrier: { dev: 'd9deadpc01', by: 'Gone', at: Date.now() - 10 * 60000, since: Date.now() - 3600e3 } });
    r = await P({ op: 'runCarrier', runId: R2, dev: OWNER, by: 'Paul', take: true, cv: CV });
    ok('B · (ONSWITCH guard) in mode on no page becomes a carrier, even of a run a dead computer held', r.ok === true && r.mine === false && r.cloud === true && F.doc('Charm_Nest_Run_Carrier/' + R2).carrier.dev === 'd9deadpc01', r);
    // the Storage door: a page's write under a sheet's or a set's folder
    const paths = ['charmnest/sheets/2026-11-20/GF_Nov.20.26_Set-6_Sheet-1/GF_Nov.20.26_Set-6_Sheet-1.ai', 'charmnest/sets/2026-11-20/Set-6/Set-6_labels.pdf'];
    const doors = [];
    for (const p of paths) for (const op of ['sign', 'put', 'finalize']) doors.push([op, p, await OUT({ op, path: p, contentType: 'application/pdf', base64: Buffer.from('%PDF-1.4 old page').toString('base64'), dev: OLD })]);
    ok('B · its sign, put and finalize of a sheet\'s or a set\'s files are refused at the Storage door (409, the cloud carries)', doors.every(([, , x]) => x.http === 409 && x.cloudCarries === true), doors.map(([op, p, x]) => [op, p.split('/')[1], x.http, x.error]));
    ok('B · ... and nothing reached Storage, no upload link was made', ![...F.blobs.keys()].some(k => /^charmnest\/(sheets|sets)\//.test(k)) && !F.signed.length, { blobs: [...F.blobs.keys()], signed: F.signed });
    // contrast: in standby the same old page's report goes through as today (the gate reads the switch only for mode on)
    F.seed('Charm_Nest_Open/drive', { mode: 'standby', runMode: 'manual', cloudSteps: Standby.STANDBY_STEPS.slice() }); lib._driveForget();
    r = await P({ op: 'computerReport', dev: OLD, by: 'Old page', report: { drive: 'standby drain' } });
    ok('B · contrast: in standby nothing of this is refused (its report is taken)', r.ok === true && !!F.doc('Charm_Nest_Computers/' + OLD), r);
  }

  /* ═══ C · the "engrave" step runs the engraving lane and never blocks ═══ */
  {
    const run = { runId: 'r-c', lines: { '4300000009_1': { state: 'pooled', engrave: { needed: true, approved: false } }, '4300000009_2': { state: 'pooled', engrave: { needed: true, approved: true } } } };
    let threw = null, r;
    try { r = await EStep.engraveStep({ run, cloudSteps: ['engrave'], lane: async () => { throw new Error('the reader did not answer in time (test)'); }, sheets: async () => [], say: () => {} }); } catch (e) { threw = e; }
    ok('C · a lane that fails: the step says it and goes on (undefined), and counts what waits (one line not approved)', !threw && r === undefined && run.pendingWork && run.pendingWork.engraving === 1, { threw: threw && threw.message, pending: run.pendingWork });
    threw = null; try { await EStep.engraveStep({ run, cloudSteps: ['engrave'], lane: async () => { throw Object.assign(new Error('another pass holds the lease'), { fenced: true }); } }); } catch (e) { threw = e; }
    ok('C · a fence still ends the pass (the step never writes past its lease)', !!(threw && threw.fenced));
    // in the driver: the run at "engrave", the engraving lane's units counted (one has more to do, one fails), a unit of another lane beside them
    F.reset(); F.clock.set(T0);
    F.seed('Charm_Nest_Open/drive', { mode: 'on', runMode: 'auto', cloudSteps: ON(), flippedAt: Date.now() - 3600e3 }); lib._driveForget();
    const R = 'run-2026-11-20-engrv';
    F.seed('Charm_Nest_Runs/' + R, { runId: R, status: 'running', step: 'engrave', mode: 'auto', day: '2026-11-20', sheets: {}, holds: {}, errors: [], orders: [] });
    F.seed('Charm_Nest_Open/run', { runId: R, at: Date.now() });
    F.seed('Charm_Nest_Run_Carrier/' + R, { runId: R, carrier: { dev: 'cloud', by: 'Cloud', at: Date.now(), since: Date.now() - 3600e3 } });
    const calls = [], item = (name, lane, order, f) => ({ name, lane, order, unit: async ctx => { calls.push(name); return f ? f(ctx) : undefined; } });
    const passItems = [item('engrave-classify', 'engrave', 80, () => ({ more: true, cursor: { runId: R, since: 1 } })), item('engrave-fit', 'engrave', 81, () => { throw new Error('a font could not be read (test)'); }), item('engrave-files', 'engrave', 82), item('labels', 'labels', 50)];
    const steps = {}; for (const s of Drive.RUN_STEPS) steps[s] = [async () => undefined]; steps.engrave = Steps.steps.engrave;
    const out = await pass({ steps, passItems });
    const run2 = F.doc('Charm_Nest_Runs/' + R);
    ok('C · the step ran the engraving lane once more at its moment (each engraving unit twice in the pass), the other lane once', calls.filter(n => n === 'engrave-classify').length === 2 && calls.filter(n => n === 'engrave-files').length === 2 && calls.filter(n => n === 'labels').length === 1, calls);
    ok('C · it never blocked: the run went on past "engrave" to its end in the same pass, not stopped', !['running', 'stopped'].includes(run2.status) && run2.step === 'complete' && !run2.stoppedBy, { status: run2.status, step: run2.step, stoppedBy: run2.stoppedBy, out });
    ok('C · the lane\'s own cursor was kept (state.items), and the pass asks again for the unit that had more', ((F.doc('Charm_Nest_Drive/state') || {}).items || {})['engrave-classify'] && out.again === true, { state: F.doc('Charm_Nest_Drive/state'), again: out.again });
  }

  /* ═══ D · the owner's hand-over, and the first pass on the resting run forms GF Draft 1 + GF Draft 3 ═══ */
  {
    F.reset(); F.clock.set(T0); seedComputers(Date.now());
    const RUN = 'run-2026-11-12-wrtgl8fukc', SET5 = 'set-2026-11-12-1', S15 = 'gold-s15-mwmg3fo9', S17 = 'gold-s17-mwoedhs2', S8 = 'gold-s8-mwdb8jkv', S9 = 'gold-s9-mweaiu9f';
    const charms = (n, suf) => Array.from({ length: n }, (_, i) => ({ id: 'c' + i, poolId: `42375${i}${suf}_42375${i}${suf}1_1`, order: `42375${i}${suf}`, areaPt2: 500, widthPt: 25, heightPt: 25 }));   // (each sheet its own orders)
    const layout = (n, suf) => ({ charms: charms(n, suf), placements: charms(n, suf).map((c, i) => ({ id: c.id, cxPt: 10 + i, cyPt: 10, angle: 0 })), poolIds: charms(n, suf).map(c => c.poolId) });
    const sheet = (id, page, suf, o) => Object.assign({ id, runId: RUN, metal: 'gold', page, day: '2026-11-18', status: 'complete', verification: { ok: true }, outputs: { ai: { path: `charmnest/sheets/2026-11-18/${id}/${id}.ai` } }, density: 0.75, fileBase: 'GF_working_' + id, layoutRev: 10, archived: false }, layout(3, suf), o || {});
    const recs = [sheet(S8, 7, '8', { draft: false, setId: SET5, setSeq: 5, sheetIndex: 1, releaseFull: true, fileBase: 'GF_Nov.12.26_Set-5_Sheet-1' }), sheet(S9, 9, '9', { draft: false, setId: SET5, setSeq: 5, sheetIndex: 2, releaseFull: true, fileBase: 'GF_Nov.12.26_Set-5_Sheet-2' }),
      sheet(S15, 15, '15', { draft: true, setId: SET5, setSeq: 5, sheetIndex: 6, releaseFull: true, density: 0.7768 }), sheet(S17, 17, '17', { draft: true, setId: null, setSeq: null, sheetIndex: null, releaseFull: true })];
    for (const s of recs) {
      F.seed('Charm_Nest_Sheets/' + s.id, s);
      for (const c of s.charms) F.seed('Charm_Pool/' + c.poolId, { poolId: c.poolId, runId: RUN, orderId: c.order, transactionId: c.order + '1', sku: 'COMET_9427', copy: 1, material: 'gold', state: s.setId && !s.draft ? 'written' : 'nested', sheetId: s.id, setId: s.draft ? null : s.setId });
    }
    F.seed('Charm_Nest_Sets/' + SET5, { setId: SET5, runId: RUN, day: '2026-11-12', seq: 5, name: 'Set-5', group: 'dispatch', key: RUN + '|dispatch', status: 'awaiting review', formedAt: T0 - 4 * 86400e3, sheetIds: [S8, S9], materials: ['gold'], orders: {}, sheetNos: { gold: 6 } });
    const membership = s => JSON.stringify(['sheetIds', 'status', 'seq', 'day', 'formedAt', 'committedAt', 'name'].map(k => (s || {})[k] === undefined ? null : s[k]));
    const set5 = membership(F.doc('Charm_Nest_Sets/' + SET5));
    F.seed('Charm_Nest_Runs/' + RUN, { runId: RUN, releasePolicy: 2, status: 'processed', step: 'complete', mode: 'auto', day: '2026-11-18', sheets: {}, holds: {}, errors: [], orders: [], solidIncluded: {}, processingComplete: true, setIds: [SET5], setId: SET5 });
    F.seed('Charm_Nest_Open/run', { runId: RUN, at: T0 - 86400e3 });
    F.seed('Charm_Nest_Run_Carrier/' + RUN, { runId: RUN, carrier: { dev: OWNER, by: 'Paul', at: Date.now(), since: T0 - 3 * 3600e3 } });
    F.seed('Charm_Nest_Shared/sorter-settings', { name: 'sorter-settings', f: { autoCommit: { j: JSON.stringify('off'), at: T0 - 3600e3, dev: OWNER, by: 'Paul' } }, at: T0 - 3600e3, n: 1 });
    F.seed(F.load('_etsyStop.js').DOC, { stopped: false });
    F.seed('Charm_Nest_Open/drive', { mode: 'off' }); lib._driveForget();
    const beat = o => P(Object.assign({ op: 'runCarrier', runId: RUN, dev: OWNER, by: 'Paul', cv: CV }, o || {}));

    // 1 · Settings → Standby
    let r = await PS({ op: 'driveSet', mode: 'standby', by: 'Paul' });
    ok('D1 · Standby: at once, the computer keeps carrying its run', r.ok === true && r.mode === 'standby' && F.doc('Charm_Nest_Open/drive').mode === 'standby', r);
    r = await beat();
    ok('D1 · the owner\'s computer still carries the run (its beat)', r.mine === true && r.driveMode === 'standby', r);
    let st = await P({ op: 'driveStatus' });
    ok('D1 · Settings says Cloud carries can be pressed (open computers on the viewer build; the seven closed ones listed for later)', st.ready && st.ready.ok === true && st.ready.later.length === 1, st.ready);
    // 2 · Settings → Cloud carries
    F.clock.advance(30000);
    r = await PS({ op: 'driveSet', mode: 'on', by: 'Paul' });
    const pressAt = (F.doc('Charm_Nest_Open/drive').pending || {}).at;
    ok('D2 · Cloud carries is taken (only drive.pending is written: the hand-over flips it)', r.ok === true && F.doc('Charm_Nest_Open/drive').pending && F.doc('Charm_Nest_Open/drive').pending.to === 'on' && F.doc('Charm_Nest_Open/drive').mode === 'standby', r);
    let p1 = await pass();
    ok('D2 · the cloud waits for the computer that carries the run to say it is idle (words name it), nothing flips', F.doc('Charm_Nest_Open/drive').mode === 'standby' && /Paul/.test(((F.doc('Charm_Nest_Open/drive').pendingWait) || {}).why || ''), { p1, d: F.doc('Charm_Nest_Open/drive') });
    r = await beat();
    ok('D2 · the owner\'s beat is told a hand-over waits (its drain starts)', r.mine === true && r.drivePending && r.drivePending.to === 'on', r);
    F.clock.advance(5000);
    r = await beat({ idle: true });
    ok('D2 · its page reports idle (nothing in hand, the outbox empty)', r.idle === true && F.doc('Charm_Nest_Run_Carrier/' + RUN).carrier.idleAt > pressAt, r);
    F.clock.advance(20000);
    const p2 = await pass();
    const d2 = F.doc('Charm_Nest_Open/drive'), car = F.doc('Charm_Nest_Run_Carrier/' + RUN).carrier;
    ok('D2 · the flip: mode "on" and the carrier taken by the cloud in one transaction, from the owner\'s computer', d2.mode === 'on' && !d2.pending && car.dev === 'cloud' && car.took && car.took.dev === OWNER && p2.modeChange && p2.modeChange.flipped === 'on', { d2, car, p2 });
    ok('D2 · "Cloud carries" put every run step and every lane in cloudSteps', ON().every(s => d2.cloudSteps.includes(s)) && d2.cloudSteps.includes('rosecut') && d2.cloudSteps.includes('sweeps') && d2.cloudSteps.includes('engrave'), d2.cloudSteps);
    F.clock.advance(31000);   // (each instance reads the switch again at most every 30 s)
    r = await beat();
    ok('D2 · the owner\'s page now follows: not its run, the cloud carries', r.mine === false && r.cloud === true && r.driveMode === 'on', r);
    // 3 · the cloud's first pass on the run
    const engraveItems = Steps.passItems.filter(it => it.lane === 'engrave');
    const p3 = await pass({ passItems: engraveItems });
    const sets = F.list('Charm_Nest_Sets'), fresh = sets.filter(s => s._id !== SET5 && (s.sheetIds || []).length);
    const s15 = F.doc('Charm_Nest_Sheets/' + S15), s17 = F.doc('Charm_Nest_Sheets/' + S17), run = F.doc('Charm_Nest_Runs/' + RUN);
    if (VERBOSE) log0(JSON.stringify({ p3, sets, run: { status: run.status, step: run.step, note: run.note, setIds: run.setIds, stoppedBy: run.stoppedBy } }, null, 1));
    ok('D3 · the first pass made a set look at once (no new order needed): the resting run went through its steps again from the checkpoint', p3.setLook === 'steps' && p3.units > 0, { setLook: p3.setLook, units: p3.units, why: p3.why });
    ok('D3 · GF Draft 1 and GF Draft 3 form ONE new set, by the shared core\'s rules (2 completed Gold Filled sheets, no shared orders)', fresh.length === 1 && JSON.stringify((fresh[0].sheetIds || []).slice().sort()) === JSON.stringify([S15, S17].sort()), sets.map(s => [s._id, s.sheetIds, s.status]));
    ok('D3 · both sheets are in it now (not drafts, its number on them)', !!fresh[0] && s15.draft === false && s17.draft === false && s15.setId === fresh[0]._id && s17.setId === fresh[0]._id, { s15: [s15.draft, s15.setId, s15.setSeq, s15.sheetIndex], s17: [s17.draft, s17.setId, s17.setSeq, s17.sheetIndex] });
    ok('D3 · Set 5 is not touched (its sheets, status, number and day as they were: no sheet joined or left it)', membership(F.doc('Charm_Nest_Sets/' + SET5)) === set5 && [S8, S9].every(id => F.doc('Charm_Nest_Sheets/' + id).setId === SET5), membership(F.doc('Charm_Nest_Sets/' + SET5)));
    ok('D3 · the new set waits for review, and the run lists it', !!fresh[0] && fresh[0].status === 'awaiting review' && (run.setIds || []).includes(fresh[0]._id), { status: fresh[0] && fresh[0].status, setIds: run.setIds });
    ok('D3 · labels and commit only as their own units say: the run waits at revalidate behind the Etsy freshness gate, nothing labelled or committed', run.status === 'running' && run.step === 'revalidate' && /Re-checking the orders waits/.test(run.note || '') && !F.list('Charm_Nest_Commits').length && !F.list('Design_RealTime_Selected_Orders').length && !(fresh[0] && fresh[0].labels) && fresh[0].status !== 'labelled', { status: run.status, step: run.step, note: run.note, commits: F.list('Charm_Nest_Commits').length });
    ok('D3 · the run was never stopped on the way', !run.stoppedBy && !(run.errors || []).length, { stoppedBy: run.stoppedBy, errors: run.errors });
    const p4 = await pass({ passItems: engraveItems });
    ok('D3 · the next pass makes no second look (once per take and flip)', !p4.setLook && F.list('Charm_Nest_Sets').filter(s => (s.sheetIds || []).length).length === 2, { setLook: p4.setLook, sets: F.list('Charm_Nest_Sets').map(s => [s._id, s.sheetIds]) });
  }

  ok('rule · Etsy was never called (its stub counted 0 calls)', F.etsy.calls === 0 && !F.etsy.inventory.length, F.etsy.log);
  ok('rule · no Etsy address and no other address was fetched', !F.net.tripwire.length && !F.net.other.length, { tripwire: F.net.tripwire, other: F.net.other });
  ok('rule · the Etsy switch was never written', !F.stopWrites.length, F.stopWrites);
  log0(failed ? `FAIL cloud-on-switch (${failed})` : 'PASS cloud-on-switch');
  process.exit(failed ? 1 : 0);
})().catch(e => { log0('FAIL the test crashed: ' + ((e && e.stack) || e)); log0('    ' + said.slice(-30).join('\n    ')); process.exit(1); });
