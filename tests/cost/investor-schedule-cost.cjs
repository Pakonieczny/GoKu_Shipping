// FC8 cost check: the simulation scheduler that investorKick runs EVERY MINUTE (Simulator.schedule) must read what it needs and no more.
// Run: node tests/cost/investor-schedule-cost.cjs        (no network, no secrets, in-memory Firestore from tests/cost/meter.cjs)
//
// Why it matters: investorKick fires 1,440 times a day. Each tick lists the simulation batches that are 'running', 'incomplete' or 'reset'
// (an old batch keeps one of those statuses for ever) and then read EVERY run document of EVERY one of them, whole, only to find that an
// archived batch needs nothing. The reads therefore grew with the history, not with the work. The tick now reads only the fields it uses
// for archived work and no run at all for an old incomplete batch (the loop skipped such a batch before it looked at a run).
//
// What this proves: (1) the numbers a tick reads over a typical archive, (2) that nothing the tick DOES changed (same dispatches, same writes).
// RUN_KB (default 15) is the assumed size of one run document; the real one was not measurable from here.
'use strict';
const assert = require('node:assert/strict');
const Module = require('module');
const meter = require('./meter.cjs');

// @google-cloud/firestore is not installed in every checkout; _investorAdmin only needs these names when it is required.
const realLoad = Module._load;
Module._load = function (req, ...rest) {
  if (req === '@google-cloud/firestore') {
    try { return realLoad.call(this, req, ...rest); } catch (e) {
      class X {}
      return { Firestore: X, FieldValue: meter.FieldValue, Timestamp: meter.Timestamp, DocumentReference: X, Transaction: X, WriteBatch: X };
    }
  }
  return realLoad.call(this, req, ...rest);
};
const A = require('../../netlify/functions/_investorAdmin');
const { Simulator } = require('../../netlify/functions/_investorEvals');

const RUN_KB = Number(process.env.RUN_KB || 15);
const NOW = Date.UTC(2026, 9, 7, 12, 0, 0);
const filler = kb => 'x'.repeat(Math.max(0, Math.round(kb * 1024)));

function fixture(shape) {
  const docs = {};
  const run = (batchId, owner, i, extra) => ({
    runId: `${batchId}_run${i}`, batchId, owner, index: i, status: 'complete', spentNano: 2000000, reservedNano: 0, pendingAiCount: 0,
    paused: true, initialized: true, leaseUntil: 0, dispatchedUntil: 0, nextAttemptAtMs: 0, dispatchSequence: 3,
    strategy: { versionId: 'baseline', rules: { note: filler(2) } }, aiPlan: { version: 'x', outputTokens: { a: 1 } },
    outcomeAnalysis: { text: filler(RUN_KB - 4) }, investments: [{ symbol: 'A', note: filler(1) }], ...extra,
  });
  const batch = (batchId, owner, status, n, createdAtMs, extra) => ({
    batchId, owner, status, count: n, createdAtMs, runIds: Array.from({ length: n }, (_, i) => `${batchId}_run${i}`), dates: ['2026-09-01'],
    repositoryMode: 'shared_first', cleanupVersion: 'shared-evidence-copies.v1', config: { from: '2026-09-01' }, limitations: [filler(2)],
    spentNano: 2000000 * n, reservedNano: 0, ...extra,
  });
  let t = NOW - 86400000 * 20;
  for (let b = 0; b < shape.reset; b++) {
    const id = `sim_reset${b}`, bt = batch(id, 'operator', 'reset', shape.runsPerBatch, t += 3600000, { resetAtMs: t + 1000, paused: true });
    docs['InvestorAI_SimulationBatches/' + id] = bt;
    for (let i = 0; i < shape.runsPerBatch; i++) docs[`InvestorAI_Simulations/${id}_run${i}`] = run(id, 'operator', i, { resetAtMs: bt.resetAtMs });
  }
  for (let b = 0; b < shape.incomplete; b++) {
    const id = `sim_inc${b}`, bt = batch(id, 'operator', 'incomplete', shape.runsPerBatch, t += 3600000);
    docs['InvestorAI_SimulationBatches/' + id] = bt;
    for (let i = 0; i < shape.runsPerBatch; i++) docs[`InvestorAI_Simulations/${id}_run${i}`] = run(id, 'operator', i, { status: 'incomplete', error: { code: 'OTHER' } });
  }
  return docs;
}

async function tick(shape, { pending = false } = {}) {
  const m = meter.create();
  const docs = fixture(shape);
  if (pending) {          // one run of a reset batch still owes an AI settlement: the tick must dispatch it exactly once
    docs['InvestorAI_Simulations/sim_reset0_run1'].pendingAiCount = 2;
    docs['InvestorAI_SimulationBatches/sim_reset0'].spentNano = 0;     // and the batch totals must be brought back in line
  }
  m.db.seed(docs);
  const admin = {
    COL: A.COL, FV: meter.FieldValue, TS: meter.Timestamp, envelope: () => ({}),
    col: n => m.db.collection(n), doc: p => m.db.doc(p), runTransaction: fn => m.db.runTransaction(fn), batch: () => m.db.batch(),
  };
  const sim = Simulator.create({ admin, wallNow: () => NOW, env: {} });
  const dispatched = [];
  const before = m.snapshot();
  await m.op('investorKick.simulator.schedule', () => sim.schedule({ dispatch: async job => { dispatched.push(job.jobId); return { upstream: 202 }; } }));
  const d = m.since(before);
  return { m, d, dispatched, after: m.db.dump() };
}

(async () => {
  // a typical archive: 6 reset batches and 6 old incomplete batches of 40 runs, nothing running
  const shape = { reset: 6, incomplete: 6, runsPerBatch: 40 };
  const idle = await tick(shape);
  const kb = n => (n / 1024).toFixed(0) + ' KB';
  const perDay = (x) => ({ reads: x.reads * 1440, bytes: x.bytes * 1440 });
  const day = perDay(idle.d);
  console.log(`archive: ${shape.reset} reset + ${shape.incomplete} incomplete batches x ${shape.runsPerBatch} runs, run doc about ${RUN_KB} KB`);
  console.log(`one idle tick reads ${idle.d.reads} docs, ${kb(idle.d.bytes)}, writes ${idle.d.writes}   ->  per day ${(day.reads / 1e3).toFixed(0)}k reads, ${(day.bytes / 1e9).toFixed(2)} GB`);
  idle.m.print();

  // behaviour: nothing to do means nothing dispatched and nothing written but the lock
  assert.deepEqual(idle.dispatched, [], 'an idle archive dispatches nothing');
  const lock = idle.after['InvestorAI_SimulationBatches/dispatch_lock'];
  assert(lock && lock.until === 0, 'the lock is released');
  const changed = Object.keys(idle.after).filter(p => p !== 'InvestorAI_SimulationBatches/dispatch_lock' && JSON.stringify(idle.after[p]) !== JSON.stringify(fixture(shape)[p]));
  assert.deepEqual(changed, [], 'no archived document was rewritten');

  // behaviour: a reset batch with an unsettled run still dispatches its settlement once and fixes the batch totals
  const owed = await tick(shape, { pending: true });
  assert.equal(owed.dispatched.length, 1, 'one settlement dispatched');
  assert(/sim_reset0_run1_settlement_4$/.test(owed.dispatched[0]) || owed.dispatched[0].includes('sim_reset0_run1'), 'the right run: ' + owed.dispatched[0]);
  assert.equal(owed.after['InvestorAI_SimulationBatches/sim_reset0'].spentNano, 2000000 * shape.runsPerBatch, 'batch totals brought in line');
  assert.equal(owed.after['InvestorAI_Simulations/sim_reset0_run1'].dispatchSequence, 4);
  assert.equal(owed.after['InvestorAI_Simulations/sim_reset0_run1'].pendingAiCount, 2, 'fields the tick does not touch are intact');

  // ceiling for the fix: the runs of archived batches no longer cost whole documents
  if (process.env.EXPECT_FIXED) {
    meter.assertMax(idle.d, { reads: 12 + 12 + 6 + 6 * 40 + 240 + 40 + 20, bytes: 12 * 40 * 1024 + 40 * RUN_KB * 1024 }, 'idle tick over an archive');
  }
  console.log('investor schedule cost: behaviour unchanged, reads measured');
})().catch(e => { console.error(e); process.exitCode = 1; });
