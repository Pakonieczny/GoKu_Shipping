'use strict';
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const { reconcileSession } = require('../../netlify/functions/lib/listingBatchRecovery.cjs');

const sessionId = 'sess_123456789_abcdef';
const path = n => `listing-generator-1/Beady_Necklace/Ready_To_List/Set_${n}`;
async function audit({ issue = true, pending = null, unregistered = false, preparationFailed = false } = {}) {
  const records = Array.from({length: 300}, (_, i) => ({batchName: `batch_${i}`, sessionId,
    displayName: `lg1-Beady_Necklace-${unregistered ? 301 : 300}sets-test-part${i + 1}of300`,
    state: 'JOB_STATE_SUCCEEDED', collected: true, setComplete: true, createdAt: i + 1,
    updatedAt: 1000 + i, collectedAt: 900 + i, responsesFile: `file_${i}`,
    sets: [{category: 'Beady_Necklace', setN: i + 1, outputBasePath: path(i + 1),
      tasks: [{slotIndex: 0, type: 'edits'}]}]}));
  const last = records[299];
  if(preparationFailed) Object.assign(last,{collected:false,state:'JOB_STATE_FAILED',locallyQueued:false,
    responsesFile:null,retryRequested:false,retryStatus:'preparation_failed',preparationFailures:3,
    recoveryReason:'Listing preparation failed 3 times. Reference download timed out.'});
  if (pending) Object.assign(last, {setComplete: false, collected: false,
    state: pending === 'active' ? 'JOB_STATE_RUNNING' : pending === 'queued' ? 'JOB_STATE_QUEUED' : 'JOB_STATE_SUCCEEDED',
    locallyQueued: pending === 'queued', retryRequested: pending === 'queued'});
  const files = records.flatMap((r, i) => issue && i === 299 ? [] :
    [`${path(i + 1)}/Slot_1.png`, `${path(i + 1)}/manifest.json`]);
  let saved;
  const db = {collection: name => name === 'batches' ? {where: () => ({limit: () => ({get: async () => ({
    size: records.length, docs: records.map(r => ({ref: r, data: () => r}))})})})}
    : {doc: () => ({set: async value => {saved = value;}})},
    batch: () => ({set: (ref, patch) => Object.assign(ref, patch), commit: async () => {}})};
  const bucket = {getFiles: async ({prefix}) => [files.filter(n => n.startsWith(prefix)).map(name => ({name}))]};
  const result = await reconcileSession({db, bucket, collection: 'batches', sessionId, timestamp: () => 2000, now: () => 2000});
  assert.equal(saved.status, result.status);
  assert.equal(records[0].setsCount, 1, 'existing records get compact list metadata');
  assert.deepEqual(records[0].setKeys, [path(1)]);
  assert.equal(records[0].requestCount, 1);
  return result;
}

const page = fs.readFileSync('Listing_Generator_1.html', 'utf8');
const start = page.indexOf('    function _renderSessionBlock(session) {');
const end = page.indexOf('    async function _cancelSession(sessionId)', start);
function render(summary) {
  return vm.runInNewContext(`${page.slice(start, end)}; _renderSessionBlock(session)`, {
    session: {sessionId, earliest: 1, latest: 1300, summary, batches: [{batchName: 'batch_issue',
      state: 'JOB_STATE_SUCCEEDED', collected: true, setComplete: false, results: {failedCount: 1}}]},
    _normBatchState: x => x, _awaitingStallRestart: () => false, _batchSafeText: String,
    _formatDuration: () => '1h', _batchSweepInfo: null, _batchNextSweepAt: null, _batchRetryLimit: 30,
    normalizeImageModelId: x => x, getImageModelConfig: () => ({label: 'Sunburst'}), DEFAULT_IMAGE_MODEL: 'sunburst',
  });
}

(async () => {
  const complete = await audit();
  assert.equal(complete.complete, 299);
  assert.equal(complete.processed, 300);
  assert.equal(complete.issues, 1);
  assert.equal(complete.pending, 0);
  assert.equal(complete.status, 'completed_with_issues');
  assert.equal(complete.finishedAt, 1299);
  const html = render(complete);
  const card = html.slice(0, html.indexOf('<details class="batch-details"'));
  assert.match(card, /Completed with 1 issue/);
  assert.match(card, /299 \/ 300/);
  assert.match(card, /listings saved/);
  assert.match(card, /299 saved · 1 needs attention/);
  assert.match(card, /Set_300/);
  assert.match(card, /Previously saved images are missing/);
  assert.match(card, /Saved listings are available in Review and Approved Sets/);
  assert(!card.includes('sets remaining'), 'terminal issue does not hold the batch open');
  for (const pending of ['active', 'queued', 'saving']) {
    const result = await audit({pending});
    assert.equal(result.status, 'in_progress', `${pending} work is still running`);
    assert.equal(result.pending, 1);
    assert.equal(result.finishedAt, null);
  }
  assert.equal((await audit({unregistered: true})).status, 'in_progress', 'unsubmitted work is not falsely completed');
  const success = await audit({issue: false});
  assert.equal(success.status, 'completed');
  assert.equal(success.processed, 300);
  assert.equal(success.issues, 0);
  const preparation = await audit({preparationFailed:true});
  assert.equal(preparation.status,'completed_with_issues','failed preparation cannot keep an otherwise finished batch open');
  assert.equal(preparation.pending,0);assert.equal(preparation.issues,1);
  assert.match(preparation.sets[299].note,/preparation failed 3 times/);
  console.log('Batch completion: all successes and terminal issues settle independently; active, queued, saving and unsubmitted work stay open');
})().catch(err => {console.error(err); process.exitCode = 1;});
