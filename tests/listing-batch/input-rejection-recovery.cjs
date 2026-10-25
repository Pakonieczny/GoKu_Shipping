'use strict';
// What the saved-file check (reconcileSession, lib/listingBatchRecovery.cjs) does with a set OpenAI
// answered 6/6 "Invalid input image file. Please check the uploaded image." (2026-10-23/24: 505 jobs).
// That text contains "upload", so failureKind read it as a STORAGE failure and the set was marked
// 'saving' (collected:false, collectionPending:true): the sweep downloaded the same error file and saved
// nothing, on every run, for good (82 sets in that loop, 423 more stuck at 'incomplete').
// Checked: such a set is queued again for its missing images with checked images (repairPending +
// retryRequested, the reason says so), not 'saving'; one already sent again with checked images
// (inputRepairAttempt 2) stops with a reason; a real storage failure is still 'saving'; a failure that
// OpenAI itself reported (source 'provider') is never read as a storage failure.
// Usage: node tests/listing-batch/input-rejection-recovery.cjs   (from the repository root)
const assert = require('node:assert/strict');
const path = require('node:path');
const { reconcileSession, failureKind } = require(path.resolve('netlify/functions/lib/listingBatchRecovery.cjs'));

const sessionId = 'sess_1792773258456_men4v0';
const category = 'Regular_Necklace';
const base = n => `listing-generator-1/${category}/Ready_To_List/Set_${n}`;
const INVALID = 'Invalid input image file. Please check the uploaded image.';
// Six generated slots and the size guide (slot 6, a copy), as the production records carry them.
const tasks = n => [0, 1, 2, 3, 4, 5, 6].map(slotIndex => slotIndex === 5
  ? { slotIndex, type: 'copy', source_storage_path: `listing-generator-1/${category}/Size_Guide.png` }
  : { slotIndex, type: 'edits', prompt: 'Preserve the charm exactly.',
    input_storage_path: `listing-generator-1/${category}/Templates/Template_${slotIndex}.png`,
    input_charm_storage_path: `listing-generator-1/New_Charms/Charm_${n}.png` });
const generated = [0, 1, 2, 3, 4, 6];
// The collected OpenAI job (batch_<hex>) and the queued record that pointed at it (batch_local_…).
function setRecords(n, failure, extra = {}) {
  const job = `batch_6add${String(n).padStart(4, '0')}5cac8190972348164ba767bc`;
  const local = `batch_local_${String(n).padStart(4, '0')}ce2041f6847cbd38cdaa9061b830222587dd0e`;
  const sets = [{ category, setN: n, outputBasePath: base(n), tasks: tasks(n) }];
  return [
    { batchName: local, sessionId, displayName: `lg1-${category}-630sets-2026-10-231634-part${n}of630`, state: 'JOB_STATE_QUEUED',
      locallyQueued: true, retryRequested: true, retryBatchName: job, retryStatus: 'submitted', collected: false, createdAt: 1000 + n, sets },
    { batchName: job, sessionId, displayName: `lg1-${category}-630sets-2026-10-231634-part${n}of630`, state: 'JOB_STATE_SUCCEEDED',
      providerStatus: 'completed', locallyQueued: false, collected: true, collectionPending: false, repairPending: false,
      retryRequested: false, retryOf: local, retryAttempt: 0, setComplete: false, recoveryStatus: 'incomplete',
      responsesFile: `file-errors-${n}`, createdAt: 2000 + n,
      batchStats: { successfulRequestCount: 0, requestCount: 6, failedRequestCount: 6, pendingRequestCount: 0 },
      results: { failedCount: 6, succeededCount: 0, missingSlotsCount: 6, charmsMoved: 0,
        failures: generated.map(slot => ({ key: `s0_slot${slot}`, ...failure })) },
      sets, ...extra },
  ];
}

async function reconcile(records, files) {
  const writes = [];
  let summary = null;
  const db = {
    collection: name => name === 'batches'
      ? { where: () => ({ limit: () => ({ get: async () => ({ size: records.length, docs: records.map(r => ({ ref: r, data: () => r })) }) }) }) }
      : { doc: () => ({ set: async value => { summary = value; } }) },
    batch: () => ({ set: (ref, patch) => { writes.push({ ref, patch }); Object.assign(ref, patch); }, commit: async () => {} }),
  };
  const bucket = { getFiles: async ({ prefix }) => [[...files].filter(name => name.startsWith(prefix)).map(name => ({ name }))] };
  const result = await reconcileSession({ db, bucket, collection: 'batches', sessionId, timestamp: () => 5000, now: () => 5000 });
  assert.equal(summary.status, result.status, 'the summary is saved');
  return { result, writes };
}
const setOf = (result, n) => result.sets.find(s => s.setN === n);

(async () => {
  // the classification itself
  assert.equal(failureKind(INVALID), 'input', 'the OpenAI input-image rejection is not a storage failure (it says "uploaded")');
  assert.equal(failureKind({ error: INVALID, source: 'provider' }), 'input');
  assert.equal(failureKind({ error: 'Invalid image', code: 'invalid_image', source: 'provider' }), 'input');
  assert.notEqual(failureKind({ error: 'socket hang up', source: 'provider' }), 'storage', 'what OpenAI reported is never a storage failure');
  assert.notEqual(failureKind({ error: 'Upstream network error while the model was downloading its input', source: 'provider' }), 'storage');
  assert.equal(failureKind({ error: 'socket hang up', source: 'storage' }), 'storage', 'a failed save is still a storage failure');
  assert.equal(failureKind('socket hang up'), 'storage', 'old records (text only) read as before');

  const records = [
    ...setRecords(1930, { error: INVALID }),                                          // the production record, as saved before the fix (no source)
    ...setRecords(1931, { error: INVALID, source: 'provider', code: 'invalid_image_file' }),
    ...setRecords(1932, { error: INVALID, source: 'provider' }, { inputRepairAttempt: 2 }),   // already sent again with checked images
    ...setRecords(1933, { error: 'socket hang up', source: 'storage' }, { batchStats: { successfulRequestCount: 6, requestCount: 6, failedRequestCount: 0 } }),
    ...setRecords(1934, { error: 'Upstream network error while the model was downloading its input', source: 'provider' }),
  ];
  const job = n => records.find(r => r.batchName.startsWith('batch_6add' + n));
  // What Storage holds: the copied size guide (Slot_6) and the manifest of every set, no generated slot.
  const files = new Set([1930, 1931, 1932, 1933, 1934].flatMap(n => [`${base(n)}/Slot_6.png`, `${base(n)}/manifest.json`]));

  const first = await reconcile(records, files);
  for (const n of [1930, 1931]) {
    const set = setOf(first.result, n), rec = job(n);
    assert.equal(set.status, 'queued', `set ${n}: rejected input images are queued again (got ${set.status}), not 'saving'`);
    assert.deepEqual(set.missingSlots, [1, 2, 3, 4, 5, 7]);
    assert.equal(rec.repairPending, true, `set ${n}: repairPending`);
    assert.equal(rec.retryRequested, true, `set ${n}: retryRequested (the sweep's queue takes it)`);
    assert.equal(rec.recoveryStatus, 'queued');
    assert.match(String(rec.recoveryReason), /checked/i, `set ${n}: the reason says the images are checked (${rec.recoveryReason})`);
    assert.notEqual(rec.collectionPending, true, `set ${n}: NOT re-collected (that was the endless loop)`);
    assert.equal(rec.collected, true, `set ${n}: stays collected`);
    assert.equal(job(n).sets[0].tasks.length, 7, 'the record keeps its tasks');
  }
  const stopped = setOf(first.result, 1932), stoppedRec = job(1932);
  assert.equal(stopped.status, 'blocked', `a set OpenAI still cannot read after a checked resend stops (got ${stopped.status})`);
  assert.equal(stoppedRec.recoveryStatus, 'blocked');
  assert.equal(stoppedRec.retryRequested, false);
  assert.equal(stoppedRec.repairPending, false);
  assert.notEqual(stoppedRec.collectionPending, true);
  assert.match(String(stoppedRec.recoveryReason), /input image/i, `it says why (${stoppedRec.recoveryReason})`);
  assert.match(String(stoppedRec.recoveryReason), /Invalid input image file/, 'with OpenAI\'s words');
  assert.equal(stopped.note, stoppedRec.recoveryReason, 'and the page shows the same reason');

  const saving = setOf(first.result, 1933), savingRec = job(1933);
  assert.equal(saving.status, 'saving', 'a real storage failure is still saved again');
  assert.equal(savingRec.collectionPending, true);
  assert.equal(savingRec.collected, false);

  const provider = setOf(first.result, 1934), providerRec = job(1934);
  assert.notEqual(provider.status, 'saving', `a failure OpenAI reported is not re-collected (got ${provider.status})`);
  assert.notEqual(providerRec.collectionPending, true);
  assert.equal(providerRec.collected, true);

  assert.equal(first.result.saving, 1, 'only the storage failure is saving');
  // A network error OpenAI reported is transient: that set is queued again too (not stopped for good).
  assert.equal(provider.status, 'queued', `a network error OpenAI reported is tried again (got ${provider.status})`);
  assert.equal(first.result.queued, 3);
  assert.equal(first.result.status, 'in_progress');

  // The next check finds the same sets queued and writes nothing more to them (no loop either way).
  const before = new Map(records.map(r => [r.batchName, JSON.stringify(r)]));
  const second = await reconcile(records, files);
  for (const n of [1930, 1931]) assert.equal(setOf(second.result, n).status, 'queued', `set ${n} stays queued`);
  assert.equal(setOf(second.result, 1932).status, 'blocked');
  for (const n of [1930, 1931, 1932]) {
    assert(!second.writes.some(w => w.ref === job(n)), `set ${n}: the second check writes nothing to it`);
    assert.equal(JSON.stringify(job(n)), before.get(job(n).batchName));
  }
  console.log('Listing batch input rejection: re-queued with checked images (not re-collected), stopped after a checked resend, storage still saved, provider failures never storage passed');
})().catch(err => { console.error(err); process.exitCode = 1; });
