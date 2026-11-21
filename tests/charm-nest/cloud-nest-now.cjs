// NESTNOW (Paul, 21 Nov 2026): a person's Nest press (nestStart from a page) queued a job that started only when the run reached its own "nest" step; with the run waiting at
// commit (the Etsy freshness gate) it never ran. Every pass of the driver now pumps the lanes (Nest.pumpLanes) of the run's queued and running nest jobs.
//   NODE_PATH=<deps> node tests/charm-nest/cloud-nest-now.cjs
'use strict';
process.env.EDIT_PASSCODE = 'test-pass';
const T0 = Date.UTC(2026, 10, 21, 20, 0, 0);
const F = require('./_cloudFakes.cjs')({ now: T0 });
const log0 = console.log.bind(console); console.log = () => {}; console.warn = () => {};
let failed = 0; const ok = (n, c, d) => { log0((c ? 'OK   ' : 'FAIL ') + n + (c || d === undefined ? '' : ' :: ' + JSON.stringify(d).slice(0, 600))); if (!c) failed++; };
const lib = F.load('charmNestLibrary.js'), Drive = F.load('_charmNestDrive.js'), Lease = F.load('_charmNestLease.js'), Nest = F.load('_charmNestNest.js'), Standby = F.load('_charmNestStandby.js');
(async () => {
  const R = 'run-2026-11-21-nestnow';
  F.seed('Charm_Nest_Open/drive', { mode: 'on', runMode: 'auto', cloudSteps: Standby.ON_STEPS.slice(), flippedAt: T0 - 3600e3 }); lib._driveForget();
  F.seed('Charm_Nest_Runs/' + R, { runId: R, status: 'running', step: 'commit', mode: 'auto', day: '2026-11-21', sheets: {}, holds: {}, errors: [], orders: [], lines: {} });
  F.seed('Charm_Nest_Open/run', { runId: R, at: T0 });
  F.seed('Charm_Nest_Run_Carrier/' + R, { runId: R, carrier: { dev: 'cloud', by: 'Cloud', at: T0, since: T0 - 3600e3 } });
  const calls = []; const real = Nest.pumpLanes; Nest.pumpLanes = async (db, jobs, o) => { calls.push({ ids: jobs.map(j => j.id), by: o && o.by }); };
  F.seed('Charm_Nest_Jobs/nest-test-a1', { id: 'nest-test-a1', kind: 'nest', cloud: true, status: 'queued', state: 'queued', sheetId: 'silver-s10-x', runId: R, lane: R + '~silver', createdMs: T0 });
  F.seed('Charm_Nest_Jobs/nest-test-done', { id: 'nest-test-done', kind: 'nest', cloud: true, status: 'done', sheetId: 'gold-s19-x', runId: R, lane: R + '~gold', createdMs: T0 - 1000 });
  const nonce = (await Lease.dispatch(F.db, F.db.collection('Charm_Nest_Drive').doc('lease'), 'test', { heartbeat: true })).nonce;
  const out = await Drive.pass({ db: F.db, Lib: lib, nonce, steps: Drive.defaultSteps(), lanes: Object.assign(Drive.defaultLanes(), { files: true }), passItems: [], sleep: async () => {}, renewMs: 0, budgetMs: 600000 }).catch(e => ({ error: String(e && e.stack || e) }));
  ok('1 · the run stands at commit, and a person\'s queued nest is started by this pass (pumpLanes with the queued job, by the driver)', calls.length >= 1 && calls[0].ids.includes('nest-test-a1') && !calls[0].ids.includes('nest-test-done') && calls[0].by === 'drive', { calls, out: out && (out.error || out.nestPumped) });
  Nest.pumpLanes = real;
  log0(failed ? `FAIL cloud-nest-now (${failed})` : 'PASS cloud-nest-now'); process.exit(failed ? 1 : 0);
})().catch(e => { log0('FAIL cloud-nest-now: ' + (e && e.stack || e)); process.exit(1); });
