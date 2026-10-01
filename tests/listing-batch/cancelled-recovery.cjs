'use strict';
const assert = require('node:assert/strict');
const {reconcileSession, failureKind} = require('../../netlify/functions/lib/listingBatchRecovery.cjs');
const sessionId = 'sess_123456789_recovery';
const path = n => `listing-generator-1/Beady_Necklace/Ready_To_List/Set_${n}`;
const cancelledMessage = 'This request was not executed because the batch was cancelled.';
const records = Array.from({length: 331}, (_, i) => {
  const complete = i < 77;
  const tasks = Array.from({length: 8}, (_, slotIndex) => ({slotIndex, type: [5, 7].includes(slotIndex) ? 'copy' : 'edits'}));
  return {batchName: `batch_${i}`, sessionId, displayName: `lg1-Beady_Necklace-331sets-date-part${i + 1}of331`,
    state: complete ? 'JOB_STATE_SUCCEEDED' : 'JOB_STATE_CANCELLED', collected: true, setComplete: complete,
    stallCancelRequestedAt: complete ? null : 900, stallRestarts: complete ? 0 : 5, createdAt: i + 1,
    responsesFile: `file_${i}`, sets: [{category: 'Beady_Necklace', setN: i + 1, outputBasePath: path(i + 1), tasks}],
    results: {failedCount: complete ? 0 : 6, failures: complete ? [] : tasks.filter(t => t.type !== 'copy')
      .map(t => ({key: `s0_slot${t.slotIndex}`, error: cancelledMessage}))}};
});
const files = records.flatMap((r, i) => (i < 77 ? Array.from({length: 8}, (_, slot) => `${path(i + 1)}/Slot_${slot + 1}.png`)
  .concat(`${path(i + 1)}/manifest.json`) : [6, 8].map(slot => `${path(i + 1)}/Slot_${slot}.png`)));
let saved;
const db = {collection: name => name === 'batches' ? {where: () => ({limit: () => ({get: async () => ({size: records.length,
  docs: records.map(r => ({ref: r, data: () => r}))})})})} : {doc: () => ({set: async value => {saved = value;}})},
  batch: () => ({set: (ref, patch) => Object.assign(ref, patch), commit: async () => {}})};
const bucket = {getFiles: async ({prefix}) => [files.filter(n => n.startsWith(prefix)).map(name => ({name}))]};
const audit = () => reconcileSession({db, bucket, collection: 'batches', sessionId, timestamp: () => 1000, now: () => 1000});
(async () => {
  const before = JSON.stringify(files);
  const summary = await audit();
  assert.equal(summary.planned, 331);
  assert.equal(summary.complete, 77);
  assert.equal(summary.queued, 254, 'collected cancellation errors retain pending restart work');
  assert.equal(summary.pending, 254);
  assert.equal(summary.issues, 0, 'automatic cancellations are recoverable work');
  assert.equal(summary.processed, 77, 'unexecuted images cannot count as completed processing');
  assert.equal(summary.status, 'in_progress');
  assert.equal(summary.finishedAt, null);
  assert.equal(saved.status, 'in_progress');
  assert(records.slice(77).every(r => r.repairPending === true), 'every stranded listing is rediscovered by the worker');
  assert(records.slice(0, 77).every(r => !r.repairPending && r.setComplete), 'saved listings stay settled');
  assert.equal(JSON.stringify(files), before, 'audit does not modify existing images');
  const repeat = await audit();
  assert.equal(repeat.queued, 254, 'repeated audits retain the same work without extra listings');
  records[77].stallRestartBlocked = true;
  records[77].repairPending = false;
  const stopped = await audit();
  assert.equal(stopped.queued, 253);
  assert.equal(stopped.cancelled, 1, 'an explicit stop remains terminal');
  assert.equal(failureKind('This request could not be executed before the completion window expired.'), 'transient',
    'provider expiry can retry missing images after collection');
  console.log('331-listing regression: 77 saved preserved, 254 collected cancellations remain recoverable, no false completion, explicit stops retained');
})().catch(err => {console.error(err); process.exitCode = 1;});
