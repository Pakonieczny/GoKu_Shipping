// CMDLANE (Paul, 22 Nov 2026: "Nesting new orders always takes last priority, user updated in the system always must take priority and happen immediately in real-time so users don't
// need to wait"). Offline: the real driver pass, the real requests unit and library handler on the in-memory cloud of _cloudFakes.cjs; no network, no Etsy.
//   1  a person's request put while a unit of the run is still working (a long nest step) is answered BEFORE that unit ends: the lane beside the unit took it up
//   2  a pass item that works long (not the requests item) gets the lane too
//   3  with nothing asked, the lane writes nothing and the unit's own answer is kept
//   NODE_PATH=<deps> node tests/charm-nest/cmd-lane.cjs
'use strict';
process.env.EDIT_PASSCODE = 'test-pass';
const T0 = Date.UTC(2026, 10, 23, 2, 0, 0);
const F = require('./_cloudFakes.cjs')({ now: T0 });
const log0 = console.log.bind(console); console.log = () => {}; console.warn = () => {};
let failed = 0; const ok = (n, c, d) => { log0((c ? 'OK   ' : 'FAIL ') + n + (c || d === undefined ? '' : ' :: ' + JSON.stringify(d).slice(0, 900))); if (!c) failed++; };
const lib = F.load('charmNestLibrary.js'), spy = F.spy(lib), Drive = F.load('_charmNestDrive.js'), Lease = F.load('_charmNestLease.js'), CmdUnit = F.load('_charmNestCmdUnit.js');
const R = 'run-2026-11-22-cmdlane';
const realWait = ms => new Promise(r => setTimeout(r, ms));
function world() {
  F.reset(); F.clock.set(T0); spy.ops.length = 0;
  F.seed('Charm_Nest_Open/drive', { mode: 'on', runMode: 'manual', cloudSteps: ['nest', 'cmd:backhome', 'cmd:keepsync'], flippedAt: T0 - 3600e3 }); lib._driveForget();
  F.seed('Charm_Nest_Runs/' + R, { runId: R, status: 'running', step: 'nest', mode: 'auto', day: '2026-11-22', sheets: {}, holds: {}, errors: [], orders: [], lines: {} });
  F.seed('Charm_Nest_Open/run', { runId: R, at: T0 });
  F.seed('Charm_Nest_Run_Carrier/' + R, { runId: R, carrier: { dev: 'cloud', by: 'Cloud', at: T0, since: T0 - 3600e3 } });
}
function putCmd(type, args) { const id = 'c' + type + Math.random().toString(36).slice(2, 6); const cur = F.doc('Charm_Nest_Run_Cmds/' + R) || { runId: R, cmds: {} }; cur.cmds[id] = { id, type, args, by: 'Paul_K', dev: 'dev-paul', at: Date.now(), state: 'waiting' }; F.seed('Charm_Nest_Run_Cmds/' + R, cur); return id; }
const cmdOf = id => ((F.doc('Charm_Nest_Run_Cmds/' + R) || {}).cmds || {})[id] || {};
async function pass(steps, items) {
  const d = await Lease.dispatch(F.db, F.db.collection('Charm_Nest_Drive').doc('lease'), 'test', { heartbeat: true });
  if (!d || !d.nonce) return { notDispatched: true };
  return Drive.pass({ db: F.db, Lib: lib, nonce: d.nonce, steps, lanes: Drive.defaultLanes(), passItems: items, sleep: async () => {}, renewMs: 0, budgetMs: 600000, log: () => {} }).catch(e => ({ error: String((e && e.stack) || e) }));
}
const holdAll = () => { const S = {}; for (const s of Drive.RUN_STEPS) S[s] = [async () => ({ wait: 'test', why: 'the test holds the run at its step' })]; return S; };

(async () => {
  /* ═══ 1 · a request put during a long unit of the run is answered before the unit ends ═══ */
  {
    world();
    const seen = {}, steps = holdAll();
    steps.nest = [async () => {
      seen.id = putCmd('backhome', { ids: ['4299999999_5399999999'] });   // (a line the run does not hold: the request is answered at once, in words)
      const t0 = Date.now();
      while (Date.now() - t0 < 9000) { await realWait(250); const c = cmdOf(seen.id); if (c.state === 'done' || c.state === 'failed') { seen.answeredInUnit = true; seen.c = c; break; } }
      seen.unitMs = Date.now() - t0;
      return { wait: 'test', why: 'a long nest unit' };
    }];
    const p = await pass(steps, [CmdUnit.units.passItems[0]]);
    ok('1 · the pass ran', p && !p.error && !p.notDispatched, p);
    ok('1 · CMDLANE: the person\'s request put during the nest unit was answered while that unit was still working (' + seen.unitMs + ' ms in)', !!seen.answeredInUnit && seen.unitMs < 8000, seen);
    ok('1 · and answered in words (that line is not in the run)', /no pieces|not|no longer/i.test(String((seen.c || {}).text || '')), seen.c);
  }
  /* ═══ 2 · a long pass item gets the lane too ═══ */
  {
    world();
    const seen = {};
    const longItem = { name: 'longwork', lane: 'nest', order: 60, unit: async () => {
      seen.id = putCmd('backhome', { ids: ['4299999998_5399999998'] });
      const t0 = Date.now();
      while (Date.now() - t0 < 9000) { await realWait(250); const c = cmdOf(seen.id); if (c.state === 'done' || c.state === 'failed') { seen.answeredInItem = true; break; } }
      seen.ms = Date.now() - t0; return {};
    } };
    const p = await pass(holdAll(), [CmdUnit.units.passItems[0], longItem]);
    ok('2 · CMDLANE: a request put while a long pass item works is answered before the item ends (' + seen.ms + ' ms in)', p && !p.error && !!seen.answeredInItem && seen.ms < 8000, { p, seen });
  }
  /* ═══ 3 · nothing asked: the lane is silent ═══ */
  {
    world();
    let ran = 0; const steps = holdAll();
    steps.nest = [async () => { ran++; await realWait(3600); return { wait: 'test', why: 'quiet' }; }];
    spy.ops.length = 0;
    const p = await pass(steps, [CmdUnit.units.passItems[0]]);
    const cmdOps = spy.ops.filter(o => /^(runCmd|cmdWork)/.test(o.op));
    ok('3 · with nothing asked, the lane writes no request op and the unit ran once, its answer kept', p && !p.error && ran === 1 && cmdOps.length === 0, { p, ran, cmdOps });
  }
  ok('no Etsy call, no network outside our own functions', F.etsy.calls === 0 && !F.net.tripwire.length && !F.net.other.length);
  log0(failed ? `FAIL cmd-lane (${failed})` : 'PASS cmd-lane'); process.exit(failed ? 1 : 0);
})().catch(e => { log0('FAIL cmd-lane: ' + (e && e.stack || e)); process.exit(1); });
