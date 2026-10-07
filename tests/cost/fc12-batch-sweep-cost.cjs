'use strict';
// Firebase cost (FC12): what ONE scheduled Listing Generator batch sweep (charmBatchSweepCron, every 10 minutes) reads
// from Firestore and lists in Storage when NOTHING is running, with a large listing history: three recent sessions
// (1000 + 300 + 60 sets), 600 old failed jobs that were replaced by a retry and never collected.
// Runs the real geminiImageProxy-background handler on the shared cost meter's in-memory Firestore and Storage. No network.
//
//   node tests/cost/fc12-batch-sweep-cost.cjs            (prints the table; asserts the idle ceilings)
//   BEFORE=path/to/old-copy-of-the-handler.js node ...    (measure an older copy placed in netlify/functions/ to compare)
const path = require('path');
const assert = require('node:assert/strict');
const meter = require('./meter.cjs');

const m = meter.create();
m.install({ app: () => ({}) });
const target = process.env.BEFORE ? path.resolve(process.env.BEFORE) : path.join(__dirname, '..', '..', 'netlify', 'functions', 'geminiImageProxy-background.js');
const impl = require(target);

const prompt = (n) => ('Photograph this jewelry piece on a white seamless background. ' + 'Keep the exact design. '.repeat(160)).slice(0, n);
const task = (slot) => ({ type: 'edits', slotIndex: slot, input_storage_path: 'listing-generator-1/Rings/Primary_Models/m' + slot + '.png', prompt: prompt(4000),
  image_roles: 'style_reference', background_policy: 'white', embed_metadata: { title: 'Ring', description: 'x'.repeat(500) } });
function record(sessionId, n, state, extra) {
  const tasks = [0, 1, 2, 3, 4].map(task);
  const set = { category: 'Rings', outputBasePath: 'listing-generator-1/Rings/Ready_To_List/Set_' + n, setN: n, setKind: null, manifest: { m: 'x'.repeat(2000) }, tasks, allTasks: tasks };
  return Object.assign({ batchName: 'batch_' + sessionId.slice(5) + '_' + n, displayName: 'listing-' + sessionId.slice(5) + '-1000sets-' + n, sessionId, model: 'gpt-image-2', provider: 'openai',
    state, collected: true, setComplete: true, repairPending: false, collectionPending: false, retryRequested: false, recoveryStatus: 'complete', createdAt: new Date(Date.now() - n * 1000), updatedAt: new Date(), setKeys: [set.outputBasePath], setsCount: 1, requestCount: 5, charmPaths: [],
    sets: [set], routes: tasks.map((t, i) => ({ setIndex: 0, slotIndex: i, outputBasePath: set.outputBasePath })), results: { failures: [] } }, extra || {});
}

const SESS = [['sess_aaaaaaaa', 1000], ['sess_bbbbbbbb', 300], ['sess_cccccccc', 60]];
const seed = {};
for (const [sid, count] of SESS) {
  for (let n = 1; n <= count; n++) seed['ListingGenerator1Batches/' + sid.slice(5) + '_' + n] = record(sid, n, 'JOB_STATE_SUCCEEDED');
  // finished sessions: nothing running, summaries from the last check
  seed['ListingGenerator1Sessions/' + sid] = { sessionId: sid, status: 'completed', active: 0, queued: 0, saving: 0, checkedAt: Date.now() - 5 * 60000, sets: Array.from({ length: count }, (_, i) => ({ setN: i + 1, status: 'complete' })) };
}
for (let i = 0; i < 600; i++) seed['ListingGenerator1Batches/old_' + i] = record('sess_aaaaaaaa', 2000 + i, 'JOB_STATE_FAILED', { collected: false, setComplete: false, retryBatchName: 'batch_next_' + i, retryRequested: true, createdAt: new Date(Date.now() - 86400000 - i) });
m.db.seed(seed);
const files = m._storage.files;
for (const [sid, count] of SESS) for (let n = 1; n <= count; n++) { for (let s = 1; s <= 5; s++) files.set('listing-generator-1/Rings/Ready_To_List/Set_' + n + '/Slot_' + s + '.png', Buffer.alloc(8)); files.set('listing-generator-1/Rings/Ready_To_List/Set_' + n + '/manifest.json', Buffer.alloc(8)); }

(async () => {
  const run = async (label, body) => {
    const before = m.snapshot();
    const res = await m.op(label, () => impl.handler({ httpMethod: 'POST', headers: {}, body: JSON.stringify(body) }));
    const d = m.since(before);
    return { res, d };
  };
  const cron = await run('batch_sweep (scheduled, idle)', { kind: 'batch_sweep', cronTriggered: true });
  const parsed = JSON.parse(cron.res.body || '{}');
  console.log('scheduled sweep, nothing running:', JSON.stringify({ status: cron.res.statusCode, openBatches: parsed.openBatches }),
    '\n  Firestore reads', cron.d.reads, ' document bytes', cron.d.bytes, ' writes', cron.d.writes, ' Storage lists', cron.d.storage.lists,
    ' ->  per hour (6 runs): reads', cron.d.reads * 6, ' MB', (cron.d.bytes * 6 / 1e6).toFixed(1));
  if (process.env.BEFORE) { m.print(); return; }
  assert.equal(cron.res.statusCode, 200);
  assert.equal(parsed.openBatches, 600, 'every uncollected record is still seen');
  // Ceilings for an idle scheduled run: the record scan and the 100 newest session ids, no session re-read, no file listing.
  assert(cron.d.reads <= 600 + 100 + 60, 'idle run reads only routing fields of the open records, the 100 newest session ids and three small summaries (got ' + cron.d.reads + ')');
  assert(cron.d.bytes <= 5e5, 'idle run moves under 0.5 MB (got ' + cron.d.bytes + ')');
  assert.equal(cron.d.storage.lists, 0, 'no Storage listing while every session is idle');
  // A person's own sweep still checks every session, at the start and at the end (the old behaviour).
  const manual = await run('batch_sweep (manual)', { kind: 'batch_sweep' });
  assert(manual.d.reads > 3500, 'a manual sweep still re-reads the sessions (' + manual.d.reads + ' reads)');
  console.log('manual sweep (unchanged behaviour): reads', manual.d.reads, ' MB', (manual.d.bytes / 1e6).toFixed(1), ' Storage lists', manual.d.storage.lists);
  m.print();
})().catch((e) => { console.error(e); process.exit(1); });
