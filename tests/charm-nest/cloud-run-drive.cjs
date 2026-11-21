// CLOUDDRIVE build step 2: the cloud driver with stub steps (cloud plan v2 §1, cases B, C, D, D2, E, F, F2, G, H, I), offline. The real cron (charmNestDriveCron), the
// real background pass (charmNestDrive-background) and the real library handler run against the in-memory cloud of _cloudFakes.cjs, with a fake clock: no network, no Etsy
// (its stub counts every call), the Etsy switch never written, no page code loaded.
//   node tests/charm-nest/cloud-run-drive.cjs
'use strict';
process.env.EDIT_PASSCODE = 'test-pass';
const T0 = Date.UTC(2026, 10, 20, 15, 0, 0);
const F = require('./_cloudFakes.cjs')({ now: T0 });
let failed = 0;
const ok = (name, cond, detail) => { console.log((cond ? 'OK   ' : 'FAIL ') + name + (cond || detail === undefined ? '' : ' :: ' + (typeof detail === 'string' ? detail : JSON.stringify(detail)).slice(0, 900))); if (!cond) failed++; };
const lib = F.load('charmNestLibrary.js'), spy = F.spy(lib), Drive = F.load('_charmNestDrive.js'), cron = F.load('charmNestDriveCron.js'), WorldLease = F.load('_charmNestWorldLease.js'), worldBg = F.load('charmNestWorld-background.js');
const repoStamp = require('./_repoToken.cjs');   // (the Settings unlock for the owner's "Cloud carries" press, issued on the same in-memory cloud: no password, nothing real)
const P = async body => { const r = await lib.handler({ httpMethod: 'POST', headers: { 'x-edit-passcode': 'test-pass' }, body: JSON.stringify(body) }); let o = {}; try { o = JSON.parse(r.body || '{}'); } catch (_) {} return Object.assign({}, o, { http: r.statusCode }); };
const STEPS = Drive.RUN_STEPS, LEASE = 'Charm_Nest_Drive/lease';
const run = id => F.doc('Charm_Nest_Runs/' + id) || {};
function setDrive(mode, extra) { if (mode == null) F.store.delete('Charm_Nest_Open/drive'); else F.seed('Charm_Nest_Open/drive', Object.assign({ mode, runMode: 'auto', cloudSteps: STEPS.slice() }, extra || {})); lib._driveForget(); }
function seedRun(id, extra) {
  F.seed('Charm_Nest_Runs/' + id, Object.assign({ runId: id, status: 'running', step: 'pull', mode: 'auto', day: '2026-11-20', sheets: {}, holds: {}, errors: [], orders: [] }, extra || {}));
  F.seed('Charm_Nest_Open/run', { runId: id, at: Date.now() });
}
const cloudCarrier = (id, at) => F.seed('Charm_Nest_Run_Carrier/' + id, { runId: id, carrier: { dev: 'cloud', by: 'Cloud', at: at || Date.now(), since: at || Date.now() } });
const noSleep = async () => {};
/** One cron tick; when it dispatches, one pass of the driver with the given steps (as charmNestDrive-background runs it). */
async function tickAndPass(steps, o = {}) {
  const nonces = [], tick = await Drive.tick({ db: F.db, kick: n => nonces.push(n) });
  if (!nonces.length) return { tick };
  try { return { tick, pass: await Drive.pass(Object.assign({ db: F.db, Lib: lib, nonce: nonces[0], steps, sleep: noSleep, renewMs: 0, budgetMs: 3600e3 }, o)) }; }
  catch (e) { return { tick, killed: e }; }
}
/** Ticks and passes until a pass no longer asks for another (at most max). */
async function cycle(steps, o = {}) { const out = []; for (let k = 0; k < (o.max || 40); k++) { const r = await tickAndPass(steps, o); out.push(r); if (!r.pass || r.killed || !r.pass.again) break; F.clock.advance(1000); } return out; }
/** The nonce of the last pass driveSoon or a pass dispatched (the kick of charmNestDrive-background it sent), taken off the queue. */
function kickedNonce() { for (let i = F.net.kicks.length - 1; i >= 0; i--) if (F.net.kicks[i].name === 'charmNestDrive-background') { const k = F.net.kicks.splice(i, 1)[0]; return JSON.parse(k.body).nonce; } return null; }
const unitsOf = (log, n = {}) => { const out = {}; for (const s of STEPS) out[s] = Array.from({ length: n[s] || 1 }, (_, i) => async ctx => { log.push(s + '#' + i); if (ctx.__) await ctx.__; }); return out; };

(async () => {
  /* ═══ B · each stub step's units run once, in order, across passes; a pass killed in the nest step resumes from cursor.i, and only the killed unit runs twice ═══ */
  {
    F.reset(); F.clock.set(T0); setDrive('standby');
    const R = 'run-2026-11-20-bbbbb'; seedRun(R);
    const log = [], steps = {};
    let killed = false;
    for (const s of STEPS) { const n = s === 'nest' ? 3 : s === 'pool' ? 2 : 1; steps[s] = Array.from({ length: n }, (_, i) => async () => { log.push(s + '#' + i); F.clock.advance(20000); if (s === 'nest' && i === 1 && !killed) { killed = true; throw Object.assign(new Error('the instance was killed (test)'), { fatal: true }); } }); }
    const first = await cycle(steps, { budgetMs: 100000 });
    const k = first.find(x => x.killed);
    ok('B · the passes went step by step, a few units each (the budget), until the kill in the nest step', !!k && first.length >= 3 && log.join(',').startsWith('pull#0,claim#0,pool#0,pool#1,plan#0,nest#0,nest#1'), { passes: first.length, log });
    ok('B · the killed pass left the lease running and the cursor at the unit it was in', F.doc(LEASE).state === 'running' && F.doc('Charm_Nest_Drive/state').cursor.step === 'nest' && F.doc('Charm_Nest_Drive/state').cursor.i === 1, F.doc('Charm_Nest_Drive/state'));
    const quiet = await tickAndPass(steps, { budgetMs: 100000 });
    ok('B · while the dead pass\'s lease is fresh the cron dispatches no second pass', !quiet.pass && F.doc(LEASE).again === true, quiet.tick);
    F.clock.advance(6 * 60000);
    const rest = await cycle(steps, { budgetMs: 100000 });
    const want = ['pull#0', 'claim#0', 'pool#0', 'pool#1', 'plan#0', 'nest#0', 'nest#1', 'nest#1', 'nest#2', 'checkpoint#0', 'engrave#0', 'revalidate#0', 'labels#0', 'commit#0', 'complete#0'];
    ok('B · after the lease went stale a new pass took over at cursor.i: every unit ran once, in order, and only the killed one twice', JSON.stringify(log) === JSON.stringify(want), log);
    ok('B · the run ended (nothing pending: complete), carried by the cloud', run(R).status === 'complete' && F.doc('Charm_Nest_Run_Carrier/' + R).carrier.dev === 'cloud' && rest.every(x => !x.killed), { status: run(R).status, passes: rest.length });
    ok('B · each pass saved the run\'s scalar record only (no lines were sent by the driver)', spy.ops.filter(o => o.marked && o.op === 'runPut').length > 0 && !F.list('Charm_Nest_Run_Live').length);
  }

  /* ═══ C · a stop press is honoured within one unit, and resume finishes the run; a page's runPut gets 409 only while the mode is "on" ═══ */
  {
    F.reset(); F.clock.set(T0); setDrive('standby');
    const R = 'run-2026-11-20-ccccc'; seedRun(R);
    const log = [], steps = {};
    for (const s of STEPS) steps[s] = s === 'nest' ? [async () => { log.push('nest#0'); const a = await P({ op: 'runControl', runId: R, action: 'stop', by: 'Ann', dev: 'd1aaaa' }); log.push('stop pressed ' + a.seq); }, async () => { log.push('nest#1'); }, async () => { log.push('nest#2'); }] : [async () => { log.push(s + '#0'); }];
    await cycle(steps);
    ok('C · the stop pressed during nest#0 was honoured before nest#1 (within one unit)', log[log.length - 1] === 'stop pressed 1' && run(R).status === 'stopped' && run(R).stopKind === 'operator' && run(R).stoppedBy === 'stopped by the operator', { log, run: { status: run(R).status, kind: run(R).stopKind } });
    ok('C · the press was acknowledged (control.ack = its seq)', run(R).control && run(R).control.ack === 1, run(R).control);
    const idle = await tickAndPass(steps);
    ok('C · a stopped run (operator) is never taken up by the cron', !idle.pass && run(R).status === 'stopped', idle.tick);
    let r = await P({ op: 'runPut', run: { runId: R, status: 'stopped', step: 'nest' }, merge: true });
    ok('C · standby: a page\'s runPut is answered as today (200)', r.http === 200 && !r.cloudCarries, r);
    setDrive('on');
    r = await P({ op: 'runPut', cv: 'test', run: { runId: R, status: 'running' }, merge: true });
    ok('C · mode on: a page\'s runPut is refused (409 cloudCarries)', r.http === 409 && r.cloudCarries === true, r);
    setDrive('standby');
    r = await P({ op: 'runControl', runId: R, action: 'resume', by: 'Ann', dev: 'd1aaaa' });
    const nonce = kickedNonce();
    ok('C · the Resume press kicked the cloud driver (driveSoon)', r.ok === true && !!nonce, { r, kicks: F.net.kicks });
    const p = await Drive.pass({ db: F.db, Lib: lib, nonce, steps, sleep: noSleep, renewMs: 0 });
    ok('C · resume carries on from the cursor and finishes the run', p.ok && run(R).status === 'complete' && JSON.stringify(log.slice(-9)) === JSON.stringify(['nest#1', 'nest#2', 'checkpoint#0', 'engrave#0', 'revalidate#0', 'labels#0', 'commit#0', 'complete#0'].slice(-9)) || (run(R).status === 'complete' && log.includes('nest#1') && log.indexOf('nest#1') > log.indexOf('stop pressed 1')), { log, status: run(R).status, p });
  }

  /* ═══ D · "HTTP 503" is retried twice, then the run stops as transient; a tick before retry.next does nothing, one after it resumes ═══ */
  {
    F.reset(); F.clock.set(T0); setDrive('standby');
    const R = 'run-2026-11-20-ddddd'; seedRun(R);
    let fails = 3; const slept = [], log = [];
    const steps = {}; for (const s of STEPS) steps[s] = [async () => { if (s === 'pool' && fails > 0) { fails--; throw new Error('HTTP 503 from the cloud function'); } log.push(s); }];
    await cycle(steps, { sleep: async t => { slept.push(t); } });
    const st = F.doc('Charm_Nest_Drive/state');
    ok('D · the 503 was tried three times in the pass (after 4 s and 8 s), then the run stopped as transient', JSON.stringify(slept) === '[4000,8000]' && run(R).status === 'stopped' && run(R).stopKind === 'transient' && fails === 0, { slept, status: run(R).status, kind: run(R).stopKind });
    ok('D · the next try is set 60 s on (AUTO_WAIT[0])', st.retry && st.retry.next === Date.now() + 60000 && st.retry.n === 1, st.retry);
    F.clock.advance(30000);
    let t = await tickAndPass(steps);
    ok('D · a tick before retry.next does nothing', !t.pass && run(R).status === 'stopped', t.tick);
    F.clock.advance(31000);
    t = await cycle(steps);
    ok('D · a tick after it resumes the run, and the run finishes', t[0].pass && t[0].pass.resumed === true && run(R).status === 'complete' && log.includes('pool') && log[log.length - 1] === 'complete', { status: run(R).status, log });
  }

  /* ═══ D2 (E8) · Etsy stops: the watchdog and the budget are never taken up by the cloud; a 429 / etsy_stopped stop follows Resume Etsy only; nothing while the switch is on or unread ═══ */
  {
    const steps = {}; for (const s of STEPS) steps[s] = [async () => {}];
    const cases = [['watchdog', 'Etsy watchdog: no answer from Etsy for 10 minutes'], ['budget', 'Etsy call budget reached (500 calls today)'], ['etsy', 'New orders could not be added: listOpenOrders HTTP 429 – {"error":"etsy_stopped"}']];
    const result = {};
    for (const [kind, why] of cases) {
      F.reset(); F.clock.set(T0); setDrive('standby');
      const R = 'run-2026-11-20-e' + kind.slice(0, 4); seedRun(R, { status: 'stopped', step: 'nest', stoppedBy: why, stopKind: Drive.kindOf(why), errors: [{ t: Date.now() - 1000, why }] }); cloudCarrier(R);
      F.seed('EtsyApi_Config/stop', { stopped: true, since: Date.now() - 5000, rev: 1 });
      let resumed30 = false;
      for (let i = 0; i < 30; i++) { F.clock.advance(60000); const t = await tickAndPass(steps); if (t.pass && t.pass.resumed) resumed30 = true; if (run(R).status !== 'stopped') resumed30 = true; }
      F.seed('EtsyApi_Config/stop', { stopped: false, since: 0, resumedAt: Date.now(), resumedBy: 'Paul', rev: 2 });
      F.clock.advance(60000); await cycle(steps);
      result[kind] = { kind: Drive.kindOf(why), resumed30, after: run(R).status };
    }
    ok('D2 · each stop has its Etsy kind first (watchdog, budget, etsy)', result.watchdog.kind === 'watchdog' && result.budget.kind === 'budget' && result.etsy.kind === 'etsy', result);
    ok('D2 · over 30 cron ticks across 30 minutes none of the three was resumed', !result.watchdog.resumed30 && !result.budget.resumed30 && !result.etsy.resumed30, result);
    ok('D2 · after Resume Etsy (resumedAt later than the stop) only the "etsy" run resumes', result.watchdog.after === 'stopped' && result.budget.after === 'stopped' && result.etsy.after === 'complete', result);
    // a transient stop: not while the switch is on, not while its read throws
    F.reset(); F.clock.set(T0); setDrive('standby');
    const R = 'run-2026-11-20-etran'; seedRun(R, { status: 'stopped', step: 'nest', stoppedBy: 'HTTP 503 from the cloud function', stopKind: 'transient', errors: [{ t: Date.now(), why: 'HTTP 503 from the cloud function' }] }); cloudCarrier(R);
    F.seed('EtsyApi_Config/stop', { stopped: true, since: Date.now() });
    for (let i = 0; i < 5; i++) { F.clock.advance(120000); await tickAndPass(steps); }
    const onSwitch = run(R).status;
    F.seed('EtsyApi_Config/stop', { stopped: false, resumedAt: Date.now() });
    F.hooks.failRead = /^EtsyApi_Config\/stop$/;
    for (let i = 0; i < 3; i++) { F.clock.advance(120000); await tickAndPass(steps); }
    const onThrow = run(R).status;
    F.hooks.failRead = null; F.clock.advance(60000); await cycle(steps);
    ok('D2 · a transient stop is not resumed while the switch is on, nor while its read throws; it is once the switch reads "not stopped"', onSwitch === 'stopped' && onThrow === 'stopped' && run(R).status === 'complete', { onSwitch, onThrow, after: run(R).status });
  }

  /* ═══ E (C7) · a unit blocks past the lease's stale time; a second pass is dispatched; the first pass's next write is fenced; every document is written once ═══ */
  {
    F.reset(); F.clock.set(T0); setDrive('standby');
    const R = 'run-2026-11-20-eeeee', L1 = '3940000001_39400000011';
    seedRun(R, { step: 'nest', liveLines: null, lines: { [L1]: { orderId: '3940000001', state: 'pooled', poolIds: [] } } });
    const applied = [], realPatch = lib.ops.runLinesPatch;
    lib.ops.runLinesPatch = async b => { const out = await realPatch(b); if (out && out.ok && (out.applied || []).length) applied.push(JSON.stringify(b.lines)); return out; };
    let release, blocked = false; const gate = new Promise(r => { release = r; });
    const steps = {}; for (const s of STEPS) steps[s] = [async () => {}];
    steps.nest = [0, 1, 2].map(i => async ctx => { if (i === 0 && !blocked) { blocked = true; await gate; } await ctx.call({ op: 'runLinesPatch', runId: R, lines: { [L1]: { wait: 'unit-' + i } } }, ctx.unitKey); });
    const nonces = []; await Drive.tick({ db: F.db, kick: n => nonces.push(n) });
    const passA = Drive.pass({ db: F.db, Lib: lib, nonce: nonces[0], steps, sleep: noSleep, renewMs: 0 });
    for (let i = 0; i < 50 && !blocked; i++) await new Promise(r => setImmediate(r));
    F.clock.advance(6 * 60000);
    const b = await tickAndPass(steps);
    ok('E · the first pass\'s lease went stale (no beat for 6 min): the cron dispatched a second pass, which went through the rest of the steps (the line still pooled: processed)', b.pass && b.pass.ok && b.tick.why.includes('a pass went quiet') && run(R).status === 'processed' && run(R).processingComplete === true, { tick: b.tick, pass: b.pass, status: run(R).status });
    release();
    const a = await passA;
    ok('E · the first pass\'s next write was fenced, and it stopped there (it wrote nothing more, not even its lease\'s end)', a.fenced === true && F.doc(LEASE).nonce !== nonces[0], a);
    const counts = applied.reduce((m, x) => (m[x] = (m[x] || 0) + 1, m), {});
    ok('E · every document was written once (each unit\'s patch applied exactly once)', Object.keys(counts).length === 3 && Object.values(counts).every(n => n === 1), counts);
    lib.ops.runLinesPatch = realPatch;
  }

  /* ═══ C7 · a unit past 60 s stops before its next write, and runs again in the next pass ═══ */
  {
    F.reset(); F.clock.set(T0); setDrive('standby');
    const R = 'run-2026-11-20-slows', L1 = '3940000001_39400000011';
    seedRun(R, { step: 'nest', lines: { [L1]: { orderId: '3940000001', state: 'pooled', poolIds: [] } } });
    let runs = 0; const steps = {}; for (const s of STEPS) steps[s] = [async () => {}];
    steps.nest = [async ctx => { runs++; if (runs === 1) F.clock.advance(61000); return ctx.call({ op: 'runLinesPatch', runId: R, lines: { [L1]: { wait: 'slow' } } }, ctx.unitKey).then(() => undefined); }];
    const first = await tickAndPass(steps);
    const live = () => { const out = {}; for (const p of F.list('Charm_Nest_Run_Live')) if (p.runId === R) Object.assign(out, JSON.parse(p.json)); return out; };
    ok('C7 · the slow unit wrote nothing, and the pass asked for the next one at the same unit', first.pass && first.pass.again === true && !(live()[L1] && live()[L1].wait) && F.doc('Charm_Nest_Drive/state').cursor.step === 'nest' && F.doc('Charm_Nest_Drive/state').cursor.i === 0, { pass: first.pass, line: live()[L1] });
    await cycle(steps);
    ok('C7 · the next pass ran it again, quickly, and wrote it once', runs === 2 && live()[L1] && live()[L1].wait === 'slow' && run(R).processingComplete === true, { runs, line: live()[L1], status: run(R).status });
  }

  /* ═══ F (P7) · in mode off, sheetLayoutPatch, artPut and runCmdPut cause no drive dispatch and no carrier write; an idle tick makes 1 read and no write ═══ */
  {
    F.reset(); F.clock.set(T0); setDrive('off');
    const R = 'run-2026-11-20-fffff', PID = '3940000001_39400000011_1'; seedRun(R);
    F.seed('Charm_Pool/' + PID, { poolId: PID, state: 'written', sheetId: 'sheetF1', runId: R });
    F.seed('Charm_Nest_Sheets/sheetF1', { id: 'sheetF1', runId: R, metal: 'gf', layoutRev: 1, poolIds: [PID], placements: [{ id: 'c1', cxPt: 1, cyPt: 1, angle: 0 }], charms: [{ id: 'c1', poolId: PID }] });
    const m = F.count();
    const a1 = await P({ op: 'sheetLayoutPatch', id: 'sheetF1', tab: 't', by: 'Ann', poses: { c1: { cxPt: 5, cyPt: 1, angle: 0 } } });
    const a2 = await P({ op: 'artPut', poolId: PID, face: 'front', by: 'Ann', clear: true });
    const a3 = await P({ op: 'runCmdPut', runId: R, dev: 'd1aaaa', by: 'Ann', cmd: { type: 'reorder' } });
    const w = F.since(m);
    ok('F · mode off: the three presses were written as today', a1.http === 200 && a2.http === 200 && a3.http === 200, [a1.http, a2.http, a3.http]);
    ok('F · mode off: they caused no drive dispatch (no lease, no kick) and no carrier write', !F.doc(LEASE) && !F.net.ours.some(k => k.name === 'charmNestDrive-background') && !w.writePaths.some(p => /^Charm_Nest_Run_Carrier\//.test(p) || /^Charm_Nest_Drive\//.test(p)), w.writePaths);
    for (const mode of [null, 'off']) {
      setDrive(mode);
      const m2 = F.count(), res = await cron.handler(), d = F.since(m2);
      ok(`F · an idle tick with the drive ${mode == null ? 'absent' : '"off"'} makes 1 read and no write`, d.reads === 1 && d.writes === 0 && JSON.parse(res.body).off === true, { d, res });
    }
  }

  /* ═══ F2 (E1) · a resting run, 20 world builds through charmNestWorld-background: each moves feed-x, tells the driver, and one pass runs: 0 cloud writes, 0 world dispatches by a change or the drive ═══ */
  {
    F.reset(); F.clock.set(T0); setDrive('standby');
    const R = 'run-2026-11-20-f2f2f';
    seedRun(R, { status: 'processed', step: 'complete', processingComplete: true });
    const sig = Drive.workSignature({ lines: {} }, { sheets: [] });
    F.seed('Charm_Nest_Runs/' + R, Object.assign(run(R), { processingSignature: sig }));
    cloudCarrier(R);
    const opsBefore = spy.ops.length, bys = [], feeds = [], passes = [];
    for (let i = 0; i < 20; i++) {
      F.clock.advance(11 * 60000);
      const got = await WorldLease.dispatch(F.db, 'cron');
      await worldBg.handler({ httpMethod: 'POST', body: JSON.stringify({ nonce: got.nonce }) });
      bys.push((F.doc('Charm_World/lease') || {}).by);
      feeds.push((F.doc('Charm_Nest_Rev/feed-x') || {}).n || 0);
      const before = spy.ops.length; await F.drain(); passes.push(spy.ops.slice(before).filter(o => o.marked).map(o => o.op));
    }
    const marked = spy.ops.slice(opsBefore).filter(o => o.marked), OpClass = require(require('path').join(F.fnDir, '_charmNestOpClass.js'));
    const writes = marked.filter(o => !OpClass.READ.has(o.op) && !(o.op === 'runOpen')), perPass = passes.map(p => p.filter(op => !OpClass.READ.has(op) && op !== 'runOpen'));
    ok('F2 · the 20 builds ran and each moved the feed (feed-x)', feeds.every((n, i) => i === 0 || n > feeds[i - 1]) && feeds[19] > feeds[0], feeds);
    ok('F2 · after each build the driver was told and one pass ran (it read the run)', passes.length === 20 && passes.every(p => p.includes('runGet')), passes.slice(0, 3));
    // (build step 8, commit e1032d4: the cloud reconciles a run it takes ONCE per take, keyed on carrier.since, and says so on the run (its note). This run was handed to the cloud
    //  just before the builds, so the first pass makes the take's writes; no world build makes the driver write anything (E1).
    //  ONSWITCH (Paul, 20 Nov 2026): a take also makes ONE set look at once: with "checkpoint" the cloud's, the resting run goes through its steps again from the checkpoint, in that
    //  first pass, and waits at revalidate behind the Etsy freshness gate; the take's and the look's writes are the run's record only (runPut): no sheet or set here)
    const st = F.doc('Charm_Nest_Drive/state') || {};
    ok('F2 · the take\'s own writes, in the first pass only (step 8: reconciled once per take; ONSWITCH: one set look per take): the run\'s record only', perPass[0].length >= 1 && perPass[0].every(op => op === 'runPut') && !!(st.reconciled && st.reconciled.runId === R && st.reconciled.done) && String(st.setLook || '').startsWith(R + '|'), { first: perPass[0], note: run(R).note, reconciled: st.reconciled, setLook: st.setLook });
    ok('F2 · 0 Lib.call writes after it, across the other 19 builds (the run waits at revalidate behind the freshness gate, as it was left)', writes.length === perPass[0].length && perPass.slice(1).every(p => !p.length) && run(R).status === 'running' && run(R).step === 'revalidate' && /Re-checking the orders waits/.test(String(run(R).note || '')), { writes: writes.slice(0, 5), perPass, status: run(R).status, step: run(R).step, note: run(R).note });
    ok('F2 · 0 Charm_World/lease dispatches by a change or by the drive (the driver never builds the world)', bys.every(b => !/^change:|^drive/.test(String(b || ''))), bys);
    ok('F2 · and the pair cannot loop: no kick was left waiting', F.net.kicks.length === 0, F.net.kicks.map(k => k.name));
  }

  /* ═══ G (C6) · standby, a live computer carries the run: a cron tick writes no carrier and dispatches nothing ═══ */
  {
    F.reset(); F.clock.set(T0); setDrive('standby');
    const R = 'run-2026-11-20-ggggg'; seedRun(R);
    F.seed('Charm_Nest_Run_Carrier/' + R, { runId: R, carrier: { dev: 'd1aaaa', by: 'Ann', at: Date.now() - 20000, since: Date.now() - 600000 } });
    const m = F.count(), res = await cron.handler(), d = F.since(m);
    ok('G · zero carrier writes, zero writes at all, no dispatch', d.writes === 0 && !d.writePaths.some(p => /Carrier/.test(p)) && JSON.parse(res.body).dispatched === false, { d, body: res.body });
    // and a pass begun anyway yields, writing nothing for the run
    const got = await require(require('path').join(F.fnDir, '_charmNestLease.js')).dispatch(F.db, F.db.collection('Charm_Nest_Drive').doc('lease'), 'test', { heartbeat: true });
    const m2 = F.count(), p = await Drive.pass({ db: F.db, Lib: lib, nonce: got.nonce, renewMs: 0 }), d2 = F.since(m2);
    ok('G · a pass started while a live computer carries the run ends with { yield } and writes nothing for the run', p.yield === true && !d2.writePaths.some(x => /Carrier|Charm_Nest_Runs/.test(x)) && F.doc('Charm_Nest_Run_Carrier/' + R).carrier.dev === 'd1aaaa', { p, w: d2.writePaths });
  }

  /* ═══ H (C6) · the cloud holds the run with no pass for 150 s: a page's runCarrier without take gets mine:false ═══ */
  {
    F.reset(); F.clock.set(T0); setDrive('standby');
    const R = 'run-2026-11-20-hhhhh'; seedRun(R); cloudCarrier(R);
    F.clock.advance(150000);
    const r = await P({ op: 'runCarrier', runId: R, dev: 'd1aaaa', by: 'Ann' });
    ok('H · mine:false (the cloud is alive for 180 s, and a beat never takes the cloud\'s run)', r.mine === false && r.alive === true && F.doc('Charm_Nest_Run_Carrier/' + R).carrier.dev === 'cloud', r);
    const cs = F.since(F.count()); await cron.handler();
    ok('H · the cron keeps the cloud\'s carrier alive', F.doc('Charm_Nest_Run_Carrier/' + R).carrier.at === Date.now(), F.doc('Charm_Nest_Run_Carrier/' + R));
  }

  /* ═══ I (C6) · a page asks while a task runs: handingBack; it takes the run only after the task ends ═══ */
  {
    F.reset(); F.clock.set(T0); setDrive('standby');
    const R = 'run-2026-11-20-iiiii'; seedRun(R); cloudCarrier(R);
    F.seed('Charm_Nest_Jobs/nest-abc', { runId: R, sheetId: 'sheetI1', sheetName: 'SS Sheet 4', state: 'running', beatAt: Date.now() });
    let r = await P({ op: 'runCarrier', runId: R, dev: 'd1aaaa', by: 'Ann', restoreFromCloud: true });
    ok('I · while a task runs: handingBack, in words', r.mine === false && r.handingBack === true && /finishing SS Sheet 4/.test(r.why), r);
    const log = []; const steps = {}; for (const s of STEPS) steps[s] = [async () => { log.push(s); }];
    const t = await tickAndPass(steps);
    ok('I · the driver yields to the person who asked: it starts nothing new', t.pass && t.pass.yield === true && !log.length, t.pass);
    F.seed('Charm_Nest_Jobs/nest-abc', { runId: R, sheetId: 'sheetI1', state: 'done', beatAt: Date.now() });
    r = await P({ op: 'runCarrier', runId: R, dev: 'd1aaaa', by: 'Ann', restoreFromCloud: true });
    ok('I · once the task ended, the same press takes the run', r.mine === true && r.restored === true && F.doc('Charm_Nest_Run_Carrier/' + R).carrier.dev === 'd1aaaa', r);
    const after = await tickAndPass(steps);
    ok('I · from then on the cloud leaves the run to that computer', (!after.pass || after.pass.yield === true) && !log.length, after);
  }

  /* ═══ P12 · "Cloud carries" writes only drive.pending; "on" is refused in words while the cloud cannot carry every run (step 15's readiness); the way back to standby parks the run ═══ */
  {
    F.reset(); F.clock.set(T0); setDrive('standby');
    const R = 'run-2026-11-20-ppppp'; seedRun(R); cloudCarrier(R);
    // (build steps 8 and 15, commit e1032d4: runControl {action:"cloud"} needs the Settings unlock, as driveSet does (_charmNestRepoAuth.js); the owner's press carries it)
    const PS = async body => P(await repoStamp('charmNestLibrary', body, F.db));
    let r = await P({ op: 'runControl', action: 'cloud', to: 'on', by: 'Paul' });
    ok('P12 · "Cloud carries" without the Settings unlock is refused and writes nothing (e1032d4)', r.http === 401 && r.error === 'repo-locked' && !F.doc('Charm_Nest_Open/drive').pending && !kickedNonce(), { r, d: F.doc('Charm_Nest_Open/drive') });
    r = await PS({ op: 'runControl', action: 'cloud', to: 'on', by: 'Paul' });
    ok('P12 · "Cloud carries" only writes drive.pending (the mode stays standby)', r.ok === true && F.doc('Charm_Nest_Open/drive').pending.to === 'on' && F.doc('Charm_Nest_Open/drive').mode === 'standby', { r, d: F.doc('Charm_Nest_Open/drive') });
    const steps = {}; for (const s of STEPS) steps[s] = [async () => {}];
    let n = kickedNonce();
    ok('P12 · the press kicked the driver (driveSoon)', !!n);
    // (build step 7b built the files lane, the last lane readiness listed: a computer seen today that has not loaded the viewer build is what refuses "on" here)
    F.seed('Charm_Nest_Computers/dev-old', { by: 'Ann', station: 'Sorting', at: Date.now() - 3600e3, drive: '' });
    await Drive.pass({ db: F.db, Lib: lib, nonce: n, steps, sleep: noSleep, renewMs: 0 });
    const d = F.doc('Charm_Nest_Open/drive');
    // (step 15, e1032d4, plan §4 phase 3: "Cloud carries" needs every step and lane built and every computer seen in a day on the viewer build, else it is refused in words naming
    //  what is missing; since build step 7b the files lane is built, so the words no longer name "writing a sheet's files")
    ok('P12 · the driver refuses "on" in words naming what the cloud cannot do yet, and changes nothing (step 15\'s readiness)', d.mode === 'standby' && !d.pending && d.pendingRefused && /cannot carry every run yet: .+\. Nothing was changed/.test(d.pendingRefused.why) && /not loaded the viewer build/.test(d.pendingRefused.why) && !/writing a sheet's files/.test(d.pendingRefused.why) && F.doc('Charm_Nest_Run_Carrier/' + R).carrier.dev === 'cloud', d);
    setDrive('on'); seedRun(R, { status: 'running', step: 'nest' });
    r = await PS({ op: 'runControl', action: 'cloud', to: 'standby', by: 'Paul', cv: 'test' });
    n = kickedNonce(); await Drive.pass({ db: F.db, Lib: lib, nonce: n, steps, sleep: noSleep, renewMs: 0 });
    ok('P12 · the way back (on → standby) flips the mode and parks the running run with words (handback)', F.doc('Charm_Nest_Open/drive').mode === 'standby' && run(R).status === 'stopped' && run(R).stopKind === 'handback', { r, d: F.doc('Charm_Nest_Open/drive'), run: { status: run(R).status, kind: run(R).stopKind } });
  }

  /* ═══ auto with no run open: the cloud makes the run itself (runOpen, carried by the cloud from its first commit) ═══ */
  {
    F.reset(); F.clock.set(T0); setDrive('standby');
    F.seed('Charm_World/summary', { at: Date.now(), live: 3 });
    const steps = {}; for (const s of STEPS) steps[s] = [async () => {}];
    const t = await tickAndPass(steps);
    const id = t.pass && t.pass.opened, made = id ? run(id) : {};
    ok('auto, open orders, no run: the cloud opened a run, carried by the cloud, made by the cloud', !!id && made.madeBy === 'cloud' && F.doc('Charm_Nest_Run_Carrier/' + id).carrier.dev === 'cloud' && t.tick.why.includes('open orders and no run'), { tick: t.tick, pass: t.pass });
    setDrive('standby', { runMode: 'manual' }); F.seed('Charm_Nest_Runs/' + id, Object.assign(run(id), { status: 'complete' }));
    const t2 = await tickAndPass(steps);
    ok('manual: no run is made by the cloud', !t2.pass, t2.tick);
  }

  /* ═══ §1.8a · the Etsy freshness gate (used by revalidate, labels and commit from their build steps) ═══ */
  {
    F.reset(); F.clock.set(T0);
    const started = Date.now() - 60000;
    F.seed('EtsyMail_Config/receiptsMirrorState', { lastSyncOutcome: 'ok', lastSyncCutByClock: false, lastSyncCompletedAt: F.Timestamp.fromMillis(Date.now() - 30000) });
    F.seed('Charm_World/summary', { at: Date.now(), mirrorBefore: Date.now() - 30000, toRead: 0, toReadIds: [] });
    let g = await Drive.freshnessGate(F.db, { stepStartedAt: started, receipts: ['1'] });
    ok('gate · a good sync after the step started, a world built from it, nothing unread, no limit, switch off: it passes', g.ok === true, g);
    F.seed('EtsyApi_Config/stop', { stopped: true, since: Date.now() });
    g = await Drive.freshnessGate(F.db, { stepStartedAt: started });
    ok('gate · the switch on: it holds, in words', !g.ok && g.hold && g.why.some(w => /Etsy is stopped for the whole shop/.test(w)), g);
    F.store.delete('EtsyApi_Config/stop'); F.hooks.failRead = /^EtsyApi_Config\/stop$/;
    g = await Drive.freshnessGate(F.db, { stepStartedAt: started }); F.hooks.failRead = null;
    ok('gate · the switch cannot be read: it holds (never "not stopped")', !g.ok && g.hold && g.why.some(w => /could not be read/.test(w)), g);
    F.seed('EtsyMail_Config/receiptsMirrorState', { lastSyncOutcome: 'rate_limited', lastSyncCompletedAt: Date.now() - 1000 });
    g = await Drive.freshnessGate(F.db, { stepStartedAt: started });
    ok('gate · the mail system\'s last sync was not good: it waits, saying so', !g.ok && g.why.some(w => /last sync was rate_limited/.test(w)), g);
    F.seed('EtsyMail_Config/receiptsMirrorState', { lastSyncOutcome: 'ok', lastSyncCompletedAt: Date.now() - 1000 });
    F.seed('Charm_World/summary', { at: Date.now(), mirrorBefore: Date.now() - 30000, toRead: 1, toReadIds: ['7'] });
    g = await Drive.freshnessGate(F.db, { stepStartedAt: started, receipts: ['7'] });
    ok('gate · a world built before that sync, or a receipt of the set still to be read: it waits', !g.ok && g.why.some(w => /built from that copy/.test(w)) && g.why.some(w => /still to be read/.test(w)), g);
  }

  /* ═══ the record helpers ported from RunCtl (recordOf, sizeCheck, archiveFinished, updatePending) ═══ */
  {
    F.reset(); F.clock.set(T0);
    const R = 'run-2026-11-20-recor'; seedRun(R);
    const big = { runId: R, status: 'running', step: 'nest', errors: Array.from({ length: 70 }, (_, i) => ({ t: i, why: 'e' + i })), processingSignature: 'x'.repeat(200), lines: { a: 1 }, control: { seq: 1 }, liveLines: { ids: [] } };
    const rec = Drive.recordOf(big);
    ok('recordOf · scalar fields only (no lines, no control), the last 50 errors, the signature as a hash', rec.errors.length === 50 && rec.errors[0].why === 'e20' && rec.processingSignature.length < 64 && !('lines' in rec) && !('control' in rec) && !('liveLines' in rec), rec);
    ok('sizeCheck · a small record says nothing', Drive.sizeCheck(rec) === null);
    const lines = { '111_1': { orderId: '111', state: 'committed' }, '111_2': { orderId: '111', state: 'gone' }, '222_1': { orderId: '222', state: 'pooled' } };
    const puts = []; const put = (body, unit) => { puts.push(unit); return lib.call(body, { unit }); };
    const a1 = await Drive.archiveFinished({ runId: R }, lines, put);
    const parts = F.list('Charm_Nest_Run_Lines').filter(p => p.runId === R);
    ok('archiveFinished · the finished order\'s lines go to content-named parts through runArchive; the open one stays', a1.parts === 1 && parts.length === 1 && JSON.stringify(parts[0].keys.sort()) === JSON.stringify(['111_1', '111_2']) && a1.done['111_1'] && !a1.done['222_1'], { a1, parts: parts.map(p => p.keys) });
    const a2 = await Drive.archiveFinished({ runId: R, archivedLines: a1.done }, lines, put);
    ok('archiveFinished · a second time writes nothing', a2.parts === 0 && F.list('Charm_Nest_Run_Lines').filter(p => p.runId === R).length === 1, a2);
    const pend = Drive.updatePending({ lines, deferredSets: {} }, { sheets: [{ id: 's', charms: [1], outputs: null }], review: 2 });
    ok('updatePending · from the documents: orders waiting, sheets not written, Review', pend.orders === 1 && pend.sheets === 1 && pend.review === 2 && pend.commit === false, pend);
  }

  /* ═══ the owner's rules ═══ */
  ok('Etsy: the stub was never called, no Etsy address or proxy was asked, the Etsy switch was never written', F.etsy.calls === 0 && !F.net.tripwire.length && !F.stopWrites.length, { etsy: F.etsy.calls, trip: F.net.tripwire });
  ok('no address outside the named functions was asked', !F.net.other.length, F.net.other);
  ok('the driver never sent a denied op', !spy.ops.some(o => o.marked && require(require('path').join(F.fnDir, '_charmNestOpClass.js')).DENIED.has(o.op)));
  // (the cloud's own node canvas, _charmNestCanvas.js NodeCanvas, is put in place as OffscreenCanvas when the cloud draws a piece (build step 3, _charmNestPdf.js withCanvas(); here the
  //  engraving fit unit of step 11 asks for it): it is not a browser's. The same exception as cloud-owner-rule.cjs (7dd4e12); any other OffscreenCanvas still fails)
  const nodeCanvas = require.cache[require('path').join(F.fnDir, '_charmNestCanvas.js')] ? require(require('path').join(F.fnDir, '_charmNestCanvas.js')).NodeCanvas : null;
  const globals = F.browserGlobals().filter(k => !(k === 'OffscreenCanvas' && nodeCanvas && globalThis.OffscreenCanvas === nodeCanvas));
  ok('no browser global exists (the cloud\'s own node canvas is not one)', !globals.length, F.browserGlobals());
  console.log(failed ? `FAIL cloud-run-drive · ${failed} check(s)` : 'PASS cloud-run-drive');
  process.exit(failed ? 1 : 0);
})().catch(e => { console.error('ERROR', e && e.stack || e); process.exit(2); });
