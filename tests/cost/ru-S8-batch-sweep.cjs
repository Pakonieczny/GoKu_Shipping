'use strict';
// Firestore cost (S8): the two things an idle scheduled Listing Generator batch sweep (charmBatchSweepCron, every 10 minutes, 144 a day) still paid
// for after FC12, measured on the shared cost meter's in-memory Firestore with the real geminiImageProxy-background handler. No network (the provider
// is a stub).
//
//   1. 100 of the ~107 reads of an idle run were the 100 newest job records, read only to find the one to three session ids they belong to.
//      A scheduled run now keeps those ids on the guard record (LG1_Config/batchSweep.recentSessions) and re-checks them with one read.
//   2. With 30 jobs at the provider, the run wrote the guard record once for EVERY job ("checking provider job"), only to move a name.
//      It is written at most once every 15 seconds now.
//
//   node tests/cost/ru-S8-batch-sweep.cjs            (prints the numbers; exits 1 when a ceiling or a behaviour check fails)
//   run the same file from a copy of the tree before the change to see the "before" numbers (the assertions are for the new behaviour)
const path = require('path');
const assert = require('node:assert/strict');
const meter = require('./meter.cjs');

const m = meter.create();
m.install({ app: () => ({}) });
process.env.OPENAI_API_KEY = 'test-key';
globalThis.fetch = async () => ({ ok: true, status: 200, headers: { get: () => null }, text: async () => JSON.stringify({ status: 'in_progress', request_counts: { total: 5, completed: 0, failed: 0 } }) });
const impl = require(path.join(__dirname, '..', '..', 'netlify', 'functions', 'geminiImageProxy-background.js'));

const T0 = Date.now() - 6 * 3600 * 1000;
let seq = 0;
const prompt = (n) => ('Photograph this jewelry piece on a white seamless background. ' + 'Keep the exact design. '.repeat(160)).slice(0, n);
const task = (slot) => ({ type: 'edits', slotIndex: slot, input_storage_path: 'listing-generator-1/Rings/Primary_Models/m' + slot + '.png', prompt: prompt(2000) });
function record(id, sessionId, n, extra) {
  const tasks = [0, 1, 2, 3, 4].map(task);
  const set = { category: 'Rings', outputBasePath: 'listing-generator-1/Rings/Ready_To_List/Set_' + n, setN: n, setKind: null, manifest: { m: 'x'.repeat(500) }, tasks, allTasks: tasks };
  return Object.assign({ batchName: id, docId: id, displayName: 'listing-' + sessionId.slice(5) + '-part' + n, sessionId, model: 'gpt-image-2', provider: 'openai',
    state: 'JOB_STATE_SUCCEEDED', collected: true, setComplete: true, repairPending: false, collectionPending: false, retryRequested: false, recoveryStatus: 'complete',
    createdAt: meter.Timestamp.fromMillis(T0 + (++seq) * 1000), updatedAt: meter.Timestamp.fromMillis(T0 + seq * 1000 + 500), setKeys: [set.outputBasePath], setsCount: 1, requestCount: 5, charmPaths: [],
    batchStats: { requestCount: 5, successfulRequestCount: 5, failedRequestCount: 0, pendingRequestCount: 0 },
    results: { succeededCount: 5, failedCount: 0, missingSlotsCount: 0, failures: [] },
    sets: [set], routes: tasks.map((t, i) => ({ setIndex: 0, slotIndex: i, outputBasePath: set.outputBasePath })) }, extra || {});
}
const summary = (sessionId, count) => ({ sessionId, planned: count, registered: count, complete: count, approved: 0, active: 0, queued: 0, saving: 0, blocked: 0, cancelled: 0,
  missingImages: 0, checkedAt: Date.now() - 60000, unregistered: 0, issues: 0, processed: count, pending: 0, status: 'completed', finishedAt: Date.now() - 3600000,
  sets: Array.from({ length: count }, (_, i) => ({ category: 'Rings', setN: i + 1, outputBasePath: 'listing-generator-1/Rings/Ready_To_List/Set_' + (i + 1), status: 'complete', missingSlots: [], note: '' })) });

// three recent sessions inside the 100 newest records (30 + 30 + 40), the newest one last; finished, so no session is re-read
const SESSIONS = [['sess_pppppppp', 30], ['sess_oooooooo', 30], ['sess_nnnnnnnn', 40]];
const seed = {};
for (const [sid, count] of SESSIONS) {
  for (let n = 1; n <= count; n++) { const id = 'batch_' + sid.slice(5) + '_' + n; seed['ListingGenerator1Batches/' + id] = record(id, sid, n); }
  seed['ListingGenerator1Sessions/' + sid] = summary(sid, count);
}
for (let i = 0; i < 120; i++) { const id = 'batch_ancient_' + i; const r = record(id, 'sess_ancient0', 9000 + i); r.createdAt = meter.Timestamp.fromMillis(T0 - 86400000 - i * 1000); seed['ListingGenerator1Batches/' + id] = r; }
seed['ListingGenerator1Sessions/sess_ancient0'] = summary('sess_ancient0', 120);
seed['LG1_Config/batchAdmission'] = { phase: 'idle' };
m.db.seed(seed);
const files = m._storage.files;
for (const [sid, count] of [...SESSIONS, ['sess_ancient0', 120]]) for (let n = 1; n <= count; n++) { for (let s = 1; s <= 5; s++) files.set('listing-generator-1/Rings/Ready_To_List/Set_' + n + '/Slot_' + s + '.png', Buffer.alloc(8)); files.set('listing-generator-1/Rings/Ready_To_List/Set_' + n + '/manifest.json', Buffer.alloc(8)); }

const run = async (label, body) => {
  m.reset();
  const res = await m.op(label, () => impl.handler({ httpMethod: 'POST', headers: {}, body: JSON.stringify(body) }));
  const r = m.report();
  const col = (name) => r.byCollection[name] || { reads: 0, writes: 0 };
  return { res, body: JSON.parse(res.body || '{}'), reads: r.total.reads, writes: r.total.writes, batchReads: col('ListingGenerator1Batches').reads, guardWrites: col('LG1_Config').writes, total: r.total };
};
const guard = () => m.db.dump()['LG1_Config/batchSweep'] || {};
const CRON = { kind: 'batch_sweep', cronTriggered: true };

(async () => {
  // (1) idle runs. The first one has no kept list and reads the 100 newest records (as before); the next ones read ONE.
  const first = await run('batch_sweep (scheduled, first, no kept list)', CRON);
  assert.equal(first.res.statusCode, 200);
  assert.deepEqual(guard().recentSessions, ['sess_nnnnnnnn', 'sess_oooooooo', 'sess_pppppppp'], 'the first run keeps the three newest sessions, newest first');
  assert(Number(guard().recentSessionsAt) > Date.now() - 60000, 'with the time it kept them');
  const idle = await run('batch_sweep (scheduled, idle, kept list)', CRON);
  assert.equal(idle.res.statusCode, 200);
  console.log('idle scheduled sweep:  first run (lists the 100 newest) reads', first.reads, ' batch-record reads', first.batchReads,
    '\n                       next runs: reads', idle.reads, ' batch-record reads', idle.batchReads, ' ->  per day (144 runs): reads', idle.reads * 144, ' (before: ~', first.reads * 144, ')');
  assert(first.batchReads >= 100, 'the first run still lists the 100 newest records (got ' + first.batchReads + ')');
  assert(idle.batchReads <= 4, 'a later idle run reads the newest record (one) and the few admission records, not the 100 newest (got ' + idle.batchReads + ')');
  assert(idle.reads <= 15, 'an idle scheduled run reads under 15 documents (got ' + idle.reads + ')');
  assert.deepEqual(guard().recentSessions, ['sess_nnnnnnnn', 'sess_oooooooo', 'sess_pppppppp'], 'the kept list is not rewritten by a run that used it');

  // (2) the same three sessions are still checked: a session the kept list names whose summary is old IS reconciled by the run
  m.db.seed({ 'ListingGenerator1Sessions/sess_oooooooo': Object.assign(summary('sess_oooooooo', 30), { status: 'in_progress', checkedAt: Date.now() - 31 * 60000 }) });
  const stale = await run('batch_sweep (scheduled, idle session due)', CRON);
  assert(m.db.dump()['ListingGenerator1Sessions/sess_oooooooo'].checkedAt > Date.now() - 60000, 'a kept session that is not complete and was last checked half an hour ago is reconciled, as before');
  assert(stale.reads > idle.reads, 'and that costs its own reads');

  // (3) a job in a NEW session makes the newest record's session differ from the kept one: the 100 newest are listed again and the session is reconciled
  m.db.seed({ 'ListingGenerator1Batches/batch_qqqqqqqq_1': record('batch_qqqqqqqq_1', 'sess_qqqqqqqq', 1, { createdAt: meter.Timestamp.fromMillis(Date.now()) }) });
  const fresh = await run('batch_sweep (scheduled, new session)', CRON);
  assert(fresh.batchReads >= 100, 'a new session at the top of the list is found by listing the 100 newest (got ' + fresh.batchReads + ')');
  assert.equal(guard().recentSessions[0], 'sess_qqqqqqqq', 'and it is now first on the kept list');
  assert(m.db.dump()['ListingGenerator1Sessions/sess_qqqqqqqq'], 'its session summary was written by the run (the session was reconciled)');

  // (4) a list older than an hour is not trusted
  m.db.seed({ 'LG1_Config/batchSweep': Object.assign({}, guard(), { recentSessionsAt: Date.now() - 61 * 60000 }) });
  const old = await run('batch_sweep (scheduled, kept list over an hour old)', CRON);
  assert(old.batchReads >= 100, 'a kept list over an hour old is listed again (got ' + old.batchReads + ')');
  assert(Number(guard().recentSessionsAt) > Date.now() - 60000, 'and kept again');

  // (5) a person's own sweep lists the 100 newest every time, and puts the session it was asked about first (the old behaviour)
  const manual = await run('batch_sweep (manual)', { kind: 'batch_sweep', sessionId: 'sess_ancient0' });
  assert(manual.batchReads >= 100, 'a manual sweep lists the 100 newest records (got ' + manual.batchReads + ')');
  assert(guard().recentSessions.indexOf('sess_ancient0') < 0, 'and never keeps the session it was asked about as one of the newest');

  // (6) thirty jobs at the provider: the guard record is written once per 15 seconds, not once per job
  for (let n = 1; n <= 30; n++) { const id = 'batch_live_' + n; m.db.seed({ ['ListingGenerator1Batches/' + id]: record(id, 'sess_nnnnnnnn', 500 + n, { state: 'JOB_STATE_RUNNING', collected: false, setComplete: false, sweepParked: false,
    providerStatus: 'in_progress', recoveryStatus: null, createdAt: meter.Timestamp.fromMillis(Date.now() + n) }) }); }
  m.db.seed({ 'ListingGenerator1Sessions/sess_nnnnnnnn': Object.assign(summary('sess_nnnnnnnn', 70), { active: 0 }) });   // (its summary was checked a minute ago: no reconcile, so only the provider checks are measured)
  const live = await run('batch_sweep (scheduled, 30 jobs at the provider)', CRON);
  assert.equal(live.body.statusChecked, 30, 'all 30 jobs were checked with the provider');
  console.log('30 jobs at the provider: guard-record writes in one run', live.guardWrites, ' (run from a tree before the change: one per job, 34); all writes', live.writes, ' reads', live.reads);
  assert(live.guardWrites <= 8, 'the guard record is written a handful of times, not once per job (got ' + live.guardWrites + ')');
  assert.equal(guard().stage, 'idle', 'and the run still ends at idle with its result');
  assert.equal(guard().lastResult.statusChecked, 30);
  m.print();
})().catch((e) => { console.error(e); process.exit(1); });
