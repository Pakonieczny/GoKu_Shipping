'use strict';
// The real batch_sweep (geminiImageProxy-background.js) on an in-memory Firestore/Storage (tests/cost/meter.cjs),
// in the shape production had on 2026-10-24: thousands of uncollected records that only point at their retry
// (restart/queue pointers: locallyQueued + retryBatchName, still JOB_STATE_QUEUED and retryRequested:true; and
// replaced OpenAI jobs: final state + retryBatchName), every id of them "batch_6a…" (an OpenAI job id) sorting
// BEFORE "batch_local_…", plus five queued sets (batch_local_…, locallyQueued, retryRequested, JOB_STATE_QUEUED).
// The open scan reads at most 2,000 records in document-id order, so the queued sets were never reached
// ("waiting: 0 · openBatches: 2000") for 31 hours, and the pointers were never parked, so every run read the
// same 2,000 again.
// Checked: the first run counts the five queued sets as waiting (lastResult.waiting) and offers each to the
// refill; a second run reads few documents. Nothing here touches a network (fetch is stubbed and must not be used).
// Usage: node tests/listing-batch/queue-starvation.cjs   (from the repository root)
const path = require('node:path');
const assert = require('node:assert/strict');
const meter = require('../cost/meter.cjs');

const fetchCalls = [];
globalThis.fetch = async (url) => { fetchCalls.push(String(url)); throw new Error('offline test: no network'); };
for (const key of Object.keys(process.env)) if (/OPENAI|GEMINI|GOOGLE_API/i.test(key)) delete process.env[key];
// A key so submissions go through admission and preparation; any provider call would reach the stub above.
process.env.OPENAI_API_KEY = 'sk-offline-test';
const quiet = { log: console.log, warn: console.warn, error: console.error };
console.log = console.warn = console.error = () => {};

const m = meter.create();
m.install({ app: () => ({}) });
const impl = require(path.resolve('netlify/functions/geminiImageProxy-background.js'));
const T0 = Date.now() - 30 * 3600 * 1000;
const SESSION = 'sess_1792773258456_men4v0';
const POINTERS = 2300, QUEUED = 5;
const hex = (i) => (0x6a00000000 + i * 7919).toString(16).padStart(10, '0') + 'c5cac8190972348164ba767bc';
const sets = (n) => [{ category: 'Regular_Necklace', setN: n, outputBasePath: `listing-generator-1/Regular_Necklace/Ready_To_List/Set_${n}`,
  tasks: [0, 1, 2, 3, 4, 6].map((slotIndex) => ({ slotIndex, type: 'edits', prompt: 'Preserve the charm exactly. ' + 'x'.repeat(200),
    input_storage_path: `listing-generator-1/Regular_Necklace/Templates/Template_${slotIndex}.png`,
    input_charm_storage_path: `listing-generator-1/Charms/New_Charms/Charm_${n}.png` })) }];
const seed = {};
const ids = [];
for (let i = 0; i < POINTERS; i++) {
  const id = 'batch_' + hex(i);
  ids.push(id);
  const at = meter.Timestamp.fromMillis(T0 - i * 1000);
  // Most are queue/restart pointers (never final, so never parked before 2026-10-25), one in twelve a replaced OpenAI job;
  // all written unparked (sweepParked:false), as batch_restart_stalled and batch_submit write them.
  seed['ListingGenerator1Batches/' + id] = i % 12
    ? { batchName: id, docId: id, sessionId: SESSION, state: 'JOB_STATE_QUEUED', locallyQueued: true, retryRequested: true,
      retryBatchName: 'batch_next_' + i, retryStatus: 'submitted', collected: false, sweepParked: false, setComplete: false, stallRestarts: 1,
      createdAt: at, updatedAt: at, sets: sets(100000 + i) }
    : { batchName: id, docId: id, sessionId: SESSION, state: 'JOB_STATE_CANCELLED', locallyQueued: false, retryRequested: true,
      retryBatchName: 'batch_local_next_' + i, collected: false, sweepParked: false, setComplete: false, stallCancelRequestedAt: at,
      createdAt: at, updatedAt: at, sets: sets(100000 + i) };
}
const queued = [];
for (let i = 0; i < QUEUED; i++) {
  const id = 'batch_local_' + (0x91ce2041 + i).toString(16) + 'f6847cbd38cdaa9061b830222587dd0e';
  queued.push(id);
  const at = meter.Timestamp.fromMillis(T0 + 3600 * 1000 + i * 60000);
  seed['ListingGenerator1Batches/' + id] = { batchName: id, docId: id, sessionId: SESSION, state: 'JOB_STATE_QUEUED', locallyQueued: true,
    retryRequested: true, retryAttempt: 0, collected: false, setComplete: false, sweepParked: false,
    displayName: `lg1-Regular_Necklace-630sets-2026-10-231634-part${500 + i}of630`, model: 'gpt-image-2.5-sunburst', imageSize: '2K',
    createdAt: at, updatedAt: at, sets: sets(1800 + i) };
}
assert(ids.every((id) => id < 'batch_local'), 'the pointer ids sort before batch_local_…');
assert(ids.every((id) => queued.every((q) => id < q)));
// The session summary says nothing is running, so a scheduled run does not re-read the whole session first.
seed['ListingGenerator1Sessions/' + SESSION] = { sessionId: SESSION, status: 'in_progress', active: 0, queued: 0, saving: 0, checkedAt: Date.now() - 60000, sets: [] };
seed['LG1_Config/batchAdmission'] = { phase: 'idle' };
m.db.seed(seed);

const run = async (label) => {
  m.reset();
  const res = await m.op(label, () => impl.handler({ httpMethod: 'POST', headers: {}, body: JSON.stringify({ kind: 'batch_sweep', cronTriggered: true }) }));
  const r = m.report();
  const guard = m.db.dump()['LG1_Config/batchSweep'] || {};
  return { res, body: JSON.parse(res.body || '{}'), reads: r.total.reads, writes: r.total.writes, last: guard.lastResult || {} };
};

(async () => {
  const a = await run('run 1');
  const b = await run('run 2');
  const c = await run('run 3');
  Object.assign(console, quiet);
  const all = m.db.dump();
  const rec = (id) => all['ListingGenerator1Batches/' + id] || {};
  const unparkedPointers = ids.filter((id) => rec(id).sweepParked !== true).length;
  console.log(`${POINTERS} pointer/replaced records sorting before ${QUEUED} queued sets:`);
  for (const [n, x] of [['1', a], ['2', b], ['3', c]]) {
    console.log(`  run ${n}: status ${x.res.statusCode}, waiting ${x.last.waiting}, records listed ${x.last.openBatches}, sourceErrors ${x.last.sourceErrors}, reads ${x.reads}, writes ${x.writes}`);
  }
  console.log(`  pointers still unparked after the runs: ${unparkedPointers}; queued sets: ${queued.map((id) => rec(id).retryStatus || rec(id).state).join(', ')}`);

  for (const x of [a, b, c]) assert.equal(x.res.statusCode, 200, 'the sweep answers');
  assert.equal(a.last.waiting, QUEUED, `the first run counts the ${QUEUED} queued sets as waiting (got ${a.last.waiting}; before the fix 0: the 2,000-record scan never reached them)`);
  // Each queued set was offered to the refill: its preparation was tried (no reference image exists here, so it
  // fails before any paid call), recorded on its own record, and the sets behind it still had their turn.
  for (const id of queued) {
    const r = rec(id);
    assert.equal(r.preparationFailures, 1, `queued set ${id} was offered to the refill (record: ${JSON.stringify({ retryError: r.retryError, retryStatus: r.retryStatus })})`);
    assert.match(String(r.retryError), /not found/, 'and failed on its missing reference, before any provider call');
  }
  assert.equal(a.last.sourceErrors, QUEUED, 'one source error each: no set stopped the refill for the others');
  assert.equal(fetchCalls.length, 0, 'nothing was sent to a provider');
  assert.equal(unparkedPointers, 0, 'every pointer and replaced job is parked, so later runs stop listing them');
  assert(ids.every((id) => rec(id).retryRequested !== true), 'and their stale retryRequested:true is cleared, so the queue read stops returning them');
  assert(b.reads <= 60, `the second run reads few documents (got ${b.reads}; before the fix every run re-listed the same 2,000)`);
  assert(c.reads <= 60, `so does the third (got ${c.reads})`);
  console.log(`Listing batch queue starvation: ${QUEUED} queued sets behind ${POINTERS} pointers reach the refill; pointers parked; next run ${b.reads} reads passed (network calls attempted and refused: ${fetchCalls.length})`);
})().catch((e) => { Object.assign(console, quiet); console.error(e); process.exit(1); });
