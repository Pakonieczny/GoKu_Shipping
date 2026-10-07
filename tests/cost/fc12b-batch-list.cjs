'use strict';
// Firebase cost (FC12b): what ONE Listing Generator Batch-panel poll (`batch_list`, limit 1000, includeCollected) reads from
// Firestore with a large listing history: the newest session of 1000 sets (one job each, 30 running), 300 + 100 older jobs,
// 250 restart pointers, ten session summaries (one of 1000 sets, one of 300, eight of 60), 600 old failed jobs.
// Runs the real geminiImageProxy-background handler on the shared cost meter's in-memory Firestore. No network.
//
//   node tests/cost/fc12b-batch-list.cjs                 (prints the table; asserts the ceilings and that a delta equals a full read)
//   BEFORE=path/to/old-copy-of-the-handler.js node ...   (measure an older copy placed in netlify/functions/ to compare)
const path = require('path');
const assert = require('node:assert/strict');
const meter = require('./meter.cjs');

const m = meter.create();
m.install({ app: () => ({}) });
process.env.OPENAI_API_KEY = 'test-key';                                       // the provider check below runs against this stub, never the network
globalThis.fetch = async () => ({ ok: true, status: 200, headers: { get: () => null }, text: async () => JSON.stringify({ status: 'in_progress', request_counts: { total: 5, completed: 0, failed: 0 } }) });
const target = process.env.BEFORE ? path.resolve(process.env.BEFORE) : path.join(__dirname, '..', '..', 'netlify', 'functions', 'geminiImageProxy-background.js');
const impl = require(target);
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

const prompt = (n) => ('Photograph this jewelry piece on a white seamless background. ' + 'Keep the exact design. '.repeat(160)).slice(0, n);
const task = (slot) => ({ type: 'edits', slotIndex: slot, input_storage_path: 'listing-generator-1/Rings/Primary_Models/m' + slot + '.png', prompt: prompt(4000) });
const T0 = Date.now() - 6 * 3600 * 1000;
let seq = 0;
function record(id, sessionId, n, extra) {
  const tasks = [0, 1, 2, 3, 4].map(task);
  const set = { category: 'Rings', outputBasePath: 'listing-generator-1/Rings/Ready_To_List/Set_' + n, setN: n, setKind: null, manifest: { m: 'x'.repeat(2000) }, tasks, allTasks: tasks };
  return Object.assign({ batchName: id, docId: id, displayName: 'listing-' + sessionId.slice(5) + '-1000sets-' + n + '-part' + n + 'of1000', sessionId, model: 'gpt-image-2', provider: 'openai',
    state: 'JOB_STATE_SUCCEEDED', collected: true, setComplete: true, repairPending: false, collectionPending: false, retryRequested: false, recoveryStatus: 'complete',
    createdAt: meter.Timestamp.fromMillis(T0 + (++seq) * 1000), updatedAt: meter.Timestamp.fromMillis(T0 + seq * 1000 + 500), setKeys: [set.outputBasePath], setsCount: 1, requestCount: 5, charmPaths: [],
    batchStats: { requestCount: 5, successfulRequestCount: 5, failedRequestCount: 0, pendingRequestCount: 0 },
    results: { succeededCount: 5, failedCount: 0, missingSlotsCount: 0, failures: [] },
    sets: [set], routes: tasks.map((t, i) => ({ setIndex: 0, slotIndex: i, outputBasePath: set.outputBasePath })) }, extra || {});
}
const summary = (sessionId, count, extra) => Object.assign({ sessionId, planned: count, registered: count, complete: count, approved: 0, active: 0, queued: 0, saving: 0, blocked: 0, cancelled: 0,
  missingImages: 0, checkedAt: Date.now() - 60000, unregistered: 0, issues: 0, processed: count, pending: 0, status: 'completed', finishedAt: Date.now() - 3600000,
  sets: Array.from({ length: count }, (_, i) => ({ category: 'Rings', setN: i + 1, outputBasePath: 'listing-generator-1/Rings/Ready_To_List/Set_' + (i + 1), status: 'complete', missingSlots: [], note: '' })) }, extra || {});

const SESSIONS = [['sess_nnnnnnnn', 1000], ['sess_oooooooo', 300], ['sess_pppppppp', 100]];
for (let i = 0; i < 7; i++) SESSIONS.push(['sess_old0000' + i, 60]);
const seed = {};
let running = 0;
for (const [sid, count] of SESSIONS) {
  const jobs = sid === 'sess_nnnnnnnn' ? 1000 : Math.min(count, 100);
  for (let n = 1; n <= jobs; n++) {
    const id = 'batch_' + sid.slice(5) + '_' + n;
    const live = sid === 'sess_nnnnnnnn' && n > 970;                       // 30 of the newest session's jobs are with OpenAI
    if (live) running++;
    seed['ListingGenerator1Batches/' + id] = record(id, sid, n, live ? { state: 'JOB_STATE_RUNNING', collected: false, setComplete: false, providerStatus: 'in_progress', recoveryStatus: null,
      batchStats: { requestCount: 5, successfulRequestCount: 0, failedRequestCount: 0, pendingRequestCount: 5 } } : {});
  }
  // finished sessions have a summary; the newest has one issue
  seed['ListingGenerator1Sessions/' + sid] = summary(sid, count, sid === 'sess_nnnnnnnn' ? { status: 'in_progress', pending: 30, active: 30, processed: 970, complete: 970,
    blocked: 1, issues: 1 } : {});
}
// the newest summary lists one set that needs a person
seed['ListingGenerator1Sessions/sess_nnnnnnnn'].sets[4] = { category: 'Rings', setN: 5, outputBasePath: 'listing-generator-1/Rings/Ready_To_List/Set_5', status: 'blocked', missingSlots: [2], note: 'Model photo rejected.' };
if (process.env.WITH_ISSUESETS !== '0') for (const [sid] of SESSIONS) seed['ListingGenerator1Sessions/' + sid].issueSets = seed['ListingGenerator1Sessions/' + sid].sets.filter((s) => ['blocked', 'saving', 'cancelled'].includes(s.status));
for (let i = 0; i < 250; i++) {                                              // restart pointers: queued records that only name the job sent for them
  const id = 'batch_local_' + String(i).padStart(40, '0');
  seed['ListingGenerator1Batches/' + id] = record(id, 'sess_nnnnnnnn', 5000 + i, { state: 'JOB_STATE_QUEUED', locallyQueued: true, collected: false, retryRequested: false, retryBatchName: 'batch_nnnnnnnn_' + (i + 1), setComplete: false });
}
for (let i = 0; i < 600; i++) {                                              // old failed jobs replaced by a retry and never collected
  const id = 'batch_dead_' + i;
  const r = record(id, 'sess_old00000', 9000 + i, { state: 'JOB_STATE_FAILED', collected: false, setComplete: false, retryBatchName: 'batch_next_' + i, retryRequested: true });
  r.createdAt = meter.Timestamp.fromMillis(T0 - 86400000 - i * 1000);
  seed['ListingGenerator1Batches/' + id] = r;
}
seed['LG1_Config/batchSweep'] = { stage: 'idle', lastSweepAt: meter.Timestamp.fromMillis(Date.now() - 300000), lastResult: { statusChecked: 30 } };
seed['LG1_Config/batchAdmission'] = { phase: 'idle' };
m.db.seed(seed);

const call = async (label, body) => {
  const before = m.snapshot();
  const res = await m.op(label, () => impl.handler({ httpMethod: 'POST', headers: {}, body: JSON.stringify(body) }));
  const d = m.since(before);
  return { res, d, body: JSON.parse(res.body || '{}') };
};
const FULL = { kind: 'batch_list', limit: 1000, includeCollected: true };

(async () => {
  const a = await call('batch_list (full)', FULL);
  assert.equal(a.res.statusCode, 200);
  console.log('batch_list full:', JSON.stringify({ batches: a.body.batches.length, sessions: a.body.sessions.length, truncated: a.body.truncated, token: !!a.body.token }),
    '\n  reads', a.d.reads, ' bytes', a.d.bytes, ' ->  per hour at 60 s: reads', a.d.reads * 60, ' MB', (a.d.bytes * 60 / 1e6).toFixed(0));
  // The page's own check of the 30 jobs that are with OpenAI (one batch_status each, every poll while a job is open). OpenAI is a stub.
  const pass = m.snapshot();
  for (let n = 971; n <= 1000; n++) await call('batch_status (one running job)', { kind: 'batch_status', batchName: 'batch_nnnnnnnn_' + n });
  const status = m.since(pass);
  console.log('30 batch_status calls (one poll\'s provider checks): reads', status.reads, ' bytes', status.bytes, ' writes', status.writes);
  // Then the small question (an older handler ignores `since` and answers the whole list again).
  await sleep(3);
  const small = await call('batch_list (changes since the last read)', { ...FULL, since: a.body.token, sessionIds: a.body.sessionIds });
  console.log('batch_list after those 30 checks:', JSON.stringify({ delta: !!small.body.delta, changed: small.body.changed && small.body.changed.length, batches: small.body.batches && small.body.batches.length }),
    '\n  reads', small.d.reads, ' bytes', small.d.bytes);
  await sleep(3);
  const idle = await call('batch_list (changes, nothing changed)', { ...FULL, since: small.body.token || a.body.token, sessionIds: a.body.sessionIds });
  console.log('batch_list, nothing changed:', JSON.stringify({ delta: !!idle.body.delta, changed: idle.body.changed && idle.body.changed.length }), '\n  reads', idle.d.reads, ' bytes', idle.d.bytes);
  // One visible tab with a job open: a poll every 3 minutes once nothing changes (20 an hour); each poll = the 30 checks + one list.
  // Before: every poll reads the whole list. After: the whole list every 10 minutes (5 an hour), small questions otherwise (15 an hour).
  const hour = (whole, smallList, nWhole) => ({ reads: nWhole * whole.reads + (20 - nWhole) * smallList.reads + 20 * status.reads, MB: ((nWhole * whole.bytes + (20 - nWhole) * smallList.bytes + 20 * status.bytes) / 1e6).toFixed(1) });
  console.log('one visible tab, one job open, per hour:  every poll whole:', JSON.stringify(hour(a.d, a.d, 20)), '  with small answers:', JSON.stringify(hour(a.d, small.d, 5)));
  if (process.env.BEFORE) { m.print(); return; }

  // (1) the small answers carry just what changed, and a delta read is the same list as a whole read once merged
  assert.equal(small.body.delta, true);
  assert.equal(small.body.changed.length, 30, 'exactly the 30 checked jobs');
  assert(small.d.reads <= 30 + 15, '30 records plus the fixed few (got ' + small.d.reads + ')');
  assert.equal(idle.body.delta, true);
  assert.equal(idle.body.changed.length, 0);
  assert(idle.d.reads <= 15 && idle.d.bytes < 30000, 'an idle small answer is the empty query, two small documents and the summaries (got ' + idle.d.reads + ' reads, ' + idle.d.bytes + ' bytes)');
  const whole2 = await call('batch_list (full, after)', FULL);
  const merged = new Map(a.body.batches.map((x) => [x.batchName, x]));
  for (const x of small.body.changed) merged.set(x.batchName, x);
  const byName = (list) => JSON.stringify([...list].sort((p, q) => String(p.batchName).localeCompare(q.batchName)));
  assert.equal(byName([...merged.values()]), byName(whole2.body.batches), 'first read + changes = a whole read');

  // (2) session summaries: every field but `sets`; the panel's issue list rides in `issueSets`
  assert.equal(a.body.sessions.length, 10);
  for (const s of a.body.sessions) { assert(Array.isArray(s.issueSets), 'summary carries issueSets'); assert.equal(s.sets, undefined, 'the long per-set list stays on the server'); }
  assert.deepEqual(a.body.sessions.find((s) => s.sessionId === 'sess_nnnnnnnn').issueSets.map((x) => x.setN), [5], 'the set that needs a person is in issueSets');
  assert.equal(a.body.sessions.find((s) => s.sessionId === 'sess_nnnnnnnn').blocked, 1, 'counts are untouched');
  const sessionBytes = m.report().byCollection['ListingGenerator1Sessions'].bytes;
  assert(sessionBytes <= 20000, 'ten summaries of a 1000-set history cost under 20 KB (got ' + sessionBytes + ')');
  // a summary written before issueSets existed is read whole, as before
  m.db.seed({ 'ListingGenerator1Sessions/sess_old00006': summary('sess_old00006', 60) });
  const b = await call('batch_list (old summary)', FULL);
  const old = b.body.sessions.find((s) => s.sessionId === 'sess_old00006');
  assert.equal(old.sets.length, 60, 'an old summary still carries its sets');
  assert.equal(b.body.sessions.filter((s) => s.sets === undefined).length, 9, 'the others stay compact');
  m.print();
})().catch((e) => { console.error(e); process.exit(1); });
