'use strict';
// Firestore cost (S8): a scheduled Listing Generator batch sweep when MORE THAN 2,000 finished, replaced jobs sit uncollected (the sweep
// lists at most 2,000 records). After FC12 a scheduled run lists only the records not "parked", and a full list of every uncollected record
// is made once an hour. The hour's mark (openFullPassAt) used to be written only when the full list was shorter than the 2,000 cap, so with
// 2,000 or more uncollected records NO run was ever the cheap kind: every run, every ten minutes, listed 2,000 records and parked the same
// first 2,000, never reaching the rest.
//
//   node tests/cost/ru-S8-batch-sweep-large.cjs            (prints the numbers; exits 1 when a ceiling or a behaviour check fails)
//   run the same file from a copy of the tree before the change to see the "before" numbers (the assertions are for the new behaviour)
const path = require('path');
const assert = require('node:assert/strict');
const meter = require('./meter.cjs');

const m = meter.create();
m.install({ app: () => ({}) });
const impl = require(path.join(__dirname, '..', '..', 'netlify', 'functions', 'geminiImageProxy-background.js'));
const T0 = Date.now() - 6 * 3600 * 1000;
const N = 2300;
const seed = {};
for (let i = 0; i < N; i++) {
  const id = 'batch_dead_' + String(i).padStart(5, '0');
  seed['ListingGenerator1Batches/' + id] = { batchName: id, docId: id, sessionId: 'sess_aaaaaaaa', state: 'JOB_STATE_FAILED', collected: false, setComplete: false, retryBatchName: 'batch_next_' + i,
    retryRequested: true, createdAt: meter.Timestamp.fromMillis(T0 - i * 1000), updatedAt: meter.Timestamp.fromMillis(T0 - i * 1000), sets: [{ outputBasePath: 'x/Set_' + i, setKind: null, manifest: { m: 'x'.repeat(300) } }] };
}
seed['ListingGenerator1Sessions/sess_aaaaaaaa'] = { sessionId: 'sess_aaaaaaaa', status: 'completed', active: 0, queued: 0, saving: 0, checkedAt: Date.now() - 60000, sets: [] };
seed['LG1_Config/batchAdmission'] = { phase: 'idle' };
m.db.seed(seed);

const run = async (label) => {
  m.reset();
  const res = await m.op(label, () => impl.handler({ httpMethod: 'POST', headers: {}, body: JSON.stringify({ kind: 'batch_sweep', cronTriggered: true }) }));
  const r = m.report();
  return { res, body: JSON.parse(res.body || '{}'), reads: r.total.reads, writes: r.total.writes };
};
const parked = () => Object.values(m.db.dump()).filter((d) => d && d.sweepParked === true).length;

(async () => {
  const a = await run('run 1 (the hour\'s full list)');
  const b = await run('run 2');
  // a job sent after the first run (written unparked, as batch_submit writes it) is still listed by the cheap kind of run
  m.db.seed({ 'ListingGenerator1Batches/batch_zlive': { batchName: 'batch_zlive', docId: 'batch_zlive', sessionId: 'sess_aaaaaaaa', state: 'JOB_STATE_QUEUED', locallyQueued: true, collected: false, sweepParked: false,
    setComplete: false, retryRequested: false, createdAt: meter.Timestamp.fromMillis(Date.now()), updatedAt: meter.Timestamp.fromMillis(Date.now()), sets: [{ outputBasePath: 'x/Set_live', setKind: null }] } });
  const c = await run('run 3 (one new job)');
  const d = await run('run 4');
  // the next hour's full list, once everything listed is parked (the steady state)
  m.db.seed({ 'LG1_Config/batchSweep': Object.assign({}, m.db.dump()['LG1_Config/batchSweep'], { openFullPassAt: Date.now() - 2 * 3600 * 1000 }) });
  const e = await run('run 5 (the next hour\'s full list)');
  console.log(`${N} uncollected finished jobs, four scheduled runs in a row:`);
  for (const [name, x] of [['1', a], ['2', b], ['3', c], ['4', d]]) console.log(`  run ${name}: records listed ${x.body.openBatches}, reads ${x.reads}, writes ${x.writes}`);
  console.log(`  run 5 (the hourly full list, nothing left to park): records listed ${e.body.openBatches}, reads ${e.reads}, writes ${e.writes}`);
  console.log(`  parked after the runs: ${parked()} of ${N}   ->  per day (120 cheap runs + 24 hourly lists, steady state): reads ~${b.reads * 120 + e.reads * 24}   (before: every run listed 2,000, 2,106 reads x 144 = ${2106 * 144})`);
  for (const x of [a, b, c, d]) assert.equal(x.res.statusCode, 200);
  // 2026-10-25: records with work flagged (here retryRequested, kept on replaced jobs) are also read by
  // what they are, so the first run sees all 2,300, not only the 2,000 the scan holds, and parks them all
  // (the cap had hidden 124 queued sets for 31 hours in production).
  assert.equal(a.body.openBatches, N, 'the first run lists every record flagged for work, past the 2,000 the scan holds');
  assert.equal(parked(), N, 'and parks them');
  assert.equal(b.body.openBatches, 0, 'the second run lists nothing that is parked');
  assert(b.reads <= 20, 'the second run is a cheap one (got ' + b.reads + ' reads)');
  assert.equal(c.body.openBatches, 1, 'a job sent since is listed by the cheap run');
  assert(c.reads <= 40, 'and the third run is still cheap (got ' + c.reads + ' reads)');
  assert(d.reads <= 40, 'so is the fourth (got ' + d.reads + ' reads)');
  assert.equal(e.body.openBatches, 2000, 'the hourly full list is still made, as before');
  assert(e.reads <= 2200 && e.writes <= 10, 'and costs one read per record listed (got ' + e.reads + ' reads, ' + e.writes + ' writes)');
  m.print();
})().catch((e) => { console.error(e); process.exit(1); });
