'use strict';
// Firestore cost (S8): the session audit at the END of a scheduled Listing Generator batch sweep. When anything at all changed in a run
// (a job moved on, was saved, cancelled or submitted) the sweep audited every one of the up to three recent sessions again: every batch
// record of each, in full, prompts and all (about 49 KB each here), and a listing of their saved files. A job finishing in today's session
// therefore also re-read yesterday's finished sessions. A scheduled run now audits again only the sessions whose jobs it changed; a
// person's own sweep still audits all of them.
//
// Fixture: today's session (200 sets, 10 jobs at the provider) and two finished ones (400 and 100 sets).
//   node tests/cost/ru-S8-batch-sweep-sessions.cjs            (prints the numbers; exits 1 when a ceiling or a behaviour check fails)
//   run the same file from a copy of the tree before the change to see the "before" numbers (the assertions are for the new behaviour)
const path = require('path');
const assert = require('node:assert/strict');
const meter = require('./meter.cjs');

const m = meter.create();
m.install({ app: () => ({}) });
process.env.OPENAI_API_KEY = 'test-key';
let providerStatus = 'in_progress';
globalThis.fetch = async () => ({ ok: true, status: 200, headers: { get: () => null }, text: async () => JSON.stringify({ status: providerStatus, request_counts: { total: 5, completed: providerStatus === 'completed' ? 5 : 0, failed: 0 } }) });
const impl = require(path.join(__dirname, '..', '..', 'netlify', 'functions', 'geminiImageProxy-background.js'));

const T0 = Date.now() - 6 * 3600 * 1000;
let seq = 0;
const prompt = (n) => ('Photograph this jewelry piece on a white seamless background. ' + 'Keep the exact design. '.repeat(160)).slice(0, n);
const task = (slot) => ({ type: 'edits', slotIndex: slot, input_storage_path: 'listing-generator-1/Rings/Primary_Models/m' + slot + '.png', prompt: prompt(4000), image_roles: 'style_reference', background_policy: 'white', embed_metadata: { title: 'Ring', description: 'x'.repeat(500) } });
const BASE = { sess_aaaaaaaa: 0, sess_bbbbbbbb: 1000, sess_cccccccc: 2000 }, COUNT = { sess_aaaaaaaa: 200, sess_bbbbbbbb: 400, sess_cccccccc: 100 };       // set numbers do not repeat between sessions
function record(sid, n, extra) {
  const tasks = [0, 1, 2, 3, 4].map(task);
  const set = { category: 'Rings', outputBasePath: 'listing-generator-1/Rings/Ready_To_List/Set_' + (BASE[sid] + n), setN: BASE[sid] + n, setKind: null, manifest: { m: 'x'.repeat(2000) }, tasks, allTasks: tasks };
  const id = 'batch_' + sid.slice(5) + '_' + n;
  return [id, Object.assign({ batchName: id, docId: id, displayName: 'listing-' + sid.slice(5) + '-' + 'x' + 'sets-' + n, sessionId: sid, model: 'gpt-image-2', provider: 'openai', state: 'JOB_STATE_SUCCEEDED', collected: true, setComplete: true,
    repairPending: false, collectionPending: false, retryRequested: false, recoveryStatus: 'complete', createdAt: meter.Timestamp.fromMillis(T0 + Math.floor(n / COUNT[sid] * 20000) * 1000 + Object.keys(BASE).indexOf(sid) * 100), updatedAt: meter.Timestamp.fromMillis(T0 + 21000 * 1000),       // the three sessions are interleaved in time, so all three are among the 100 newest records
    setKeys: [set.outputBasePath], setsCount: 1, requestCount: 5, charmPaths: [], sets: [set], routes: tasks.map((t, i) => ({ setIndex: 0, slotIndex: i, outputBasePath: set.outputBasePath })), results: { failures: [] } }, extra || {})];
}
const summ = (sid, count, active) => ({ sessionId: sid, planned: count, registered: count, complete: count - active, approved: 0, active, queued: 0, saving: 0, blocked: 0, cancelled: 0, missingImages: 0,
  checkedAt: Date.now() - 5 * 60000, unregistered: 0, issues: 0, processed: count - active, pending: active, status: active ? 'in_progress' : 'completed', sets: [] });
const SESSIONS = [['sess_cccccccc', 100, 0], ['sess_bbbbbbbb', 400, 0], ['sess_aaaaaaaa', 200, 10]];
const seed = {};
for (const [sid, count, active] of SESSIONS) {
  for (let n = 1; n <= count; n++) {
    const live = n > count - active;
    const [id, r] = record(sid, n, live ? { state: 'JOB_STATE_RUNNING', collected: false, setComplete: false, sweepParked: false, providerStatus: 'in_progress', recoveryStatus: null, createdAt: meter.Timestamp.fromMillis(Date.now() - 60000 + n) } : {});
    seed['ListingGenerator1Batches/' + id] = r;
  }
  seed['ListingGenerator1Sessions/' + sid] = summ(sid, count, active);
}
seed['LG1_Config/batchAdmission'] = { phase: 'idle' };
m.db.seed(seed);
const files = m._storage.files;
for (const [sid, count, active] of SESSIONS) for (let n = 1; n <= count - active; n++) {
  for (let s = 1; s <= 5; s++) files.set('listing-generator-1/Rings/Ready_To_List/Set_' + (BASE[sid] + n) + '/Slot_' + s + '.png', Buffer.alloc(8));
  files.set('listing-generator-1/Rings/Ready_To_List/Set_' + (BASE[sid] + n) + '/manifest.json', Buffer.alloc(8));
}
const run = async (label, body) => {
  m.reset();
  const res = await m.op(label, () => impl.handler({ httpMethod: 'POST', headers: {}, body: JSON.stringify(body) }));
  const r = m.report();
  return { res, body: JSON.parse(res.body || '{}'), reads: r.total.reads, MB: r.total.bytes / 1e6, writes: r.total.writes, lists: r.storage.lists };
};
const checkedAt = (sid) => m.db.dump()['ListingGenerator1Sessions/' + sid].checkedAt;
const CRON = { kind: 'batch_sweep', cronTriggered: true };

(async () => {
  const warm = await run('warm-up (first run: hourly full pass, session list)', CRON);
  assert.equal(warm.res.statusCode, 200);
  // (1) a quiet run: the active session is audited at the start (as before), nothing changed, so no second audit
  const quiet = await run('scheduled, 10 jobs at the provider, nothing changes', CRON);
  console.log(`quiet run:   reads ${quiet.reads}, ${quiet.MB.toFixed(1)} MB, writes ${quiet.writes}, Storage lists ${quiet.lists}`);

  // (2) the jobs reach SUCCEEDED: a state change. Before: A, B and C audited again at the end. Now: only A.
  const b0 = checkedAt('sess_bbbbbbbb'), c0 = checkedAt('sess_cccccccc'), a0 = checkedAt('sess_aaaaaaaa');
  providerStatus = 'completed';
  const busy = await run('scheduled, the jobs finish', CRON);
  console.log(`busy run:    reads ${busy.reads}, ${busy.MB.toFixed(1)} MB, writes ${busy.writes}, Storage lists ${busy.lists}   (run from a tree before the change: the finished 400- and 100-set sessions are read again too)`);
  assert.equal(busy.res.statusCode, 200);
  assert(checkedAt('sess_aaaaaaaa') > a0, 'the session whose jobs changed is audited again at the end');
  assert.equal(checkedAt('sess_bbbbbbbb'), b0, 'a finished session no job of which changed is not audited again');
  assert.equal(checkedAt('sess_cccccccc'), c0, 'nor the other');
  assert(busy.reads < 200 * 2 + 400 + 100 - 100, 'the busy run reads far less than the three sessions twice (got ' + busy.reads + ')');

  // (3) a person's own sweep audits every session, at the start and at the end (the old behaviour)
  const manual = await run('manual sweep', { kind: 'batch_sweep' });
  console.log(`manual run:  reads ${manual.reads}, ${manual.MB.toFixed(1)} MB (unchanged behaviour: every session is audited)`);
  assert(checkedAt('sess_bbbbbbbb') > b0 && checkedAt('sess_cccccccc') > c0, 'a manual sweep audits all three sessions');
  assert(manual.reads > 700 * 2 - 100, 'and reads them (got ' + manual.reads + ')');
  m.print();
})().catch((e) => { console.error(e); process.exit(1); });
