'use strict';
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const page = fs.readFileSync('Listing_Generator_1.html', 'utf8');
const cut = (from, to) => page.slice(page.indexOf(from), page.indexOf(to, page.indexOf(from)));
const dashboard = cut('    function _renderBatchDashboard(sessions, response) {', '    async function refreshBatchJobsPanel() {');
const summary = {status: 'completed_with_issues', pending: 0, processed: 300, complete: 299,
  sets: [{setN: 582, status: 'blocked'}]};
const history = {sessionId: 'done', summary, batches: [{state: 'JOB_STATE_SUCCEEDED', collected: true}]};
function render(sessions = [history], extras = {}) {
  return vm.runInNewContext(`${dashboard}; _renderBatchDashboard(sessions, response)`, {
    sessions, response: {sweep: {lastSweepAt: Date.now(), lastError: null}, admission: {busy: false}, ...extras},
    _renderSessionBlock: s => `<article>${s.sessionId}</article>`, _batchInFlight: false,
    _normBatchState: x => x, _awaitingStallRestart: b => b.stallRestart === 'pending' && !b.setComplete,
  });
}
const html = render();
assert.match(html, /✓ Ready for another batch/);
assert.match(html, /All batch processing is finished/);
assert.match(html, /300 processed · 299 saved · 1 issue/);
assert.match(html, /Set_582 needs attention/);
assert.match(html, /Previous issues do not block a new batch/);
assert.match(html, /Batch history \(1\) · ✓ Latest batch completed/);
assert(html.indexOf('<details class="batch-history"') < html.indexOf('<article>done'), 'completed submission is in history');
assert(!/<details class="batch-history"[^>]*\bopen\b/.test(html), 'history starts collapsed');
const queued = {sessionId: 'next', batches: [{state: 'JOB_STATE_QUEUED', retryRequested: true, collected: false}]};
const running = render([queued, history]);
assert(!running.includes('Ready for another batch'), 'newly queued work replaces the idle state');
assert(running.indexOf('<article>next') < running.indexOf('<details class="batch-history"'), 'new batch appears before retained history');
assert(!render([history], {admission: {busy: true}}).includes('Ready for another batch'), 'unconfirmed admission is not ready');
assert(!render([history], {admissionError: 'provider unavailable'}).includes('Ready for another batch'), 'admission errors are not masked');
assert(!render([history], {sweep: {lastSweepAt: Date.now() - 30 * 60000}}).includes('Ready for another batch'), 'stale worker is not advertised as ready');
const stranded = {sessionId: 'stranded', summary: {...summary, processed: 331, complete: 77},
  batches: [{state: 'JOB_STATE_CANCELLED', collected: true, stallRestart: 'pending', setComplete: false}]};
assert(!render([stranded, history]).includes('Ready for another batch'), 'a collected cancellation with pending recovery is not ready');
assert(render([stranded, history]).indexOf('<article>stranded') < render([stranded, history]).indexOf('<details class="batch-history"'),
  'unfinished recovered work stays outside completed history even with a stale completion summary');

const prepare = cut('    async function prepareBatchSources(category, slotPlan) {', '    async function runBatchModeSubmission() {');
async function sources(empty = false) {
  const reads = [];
  const result = await vm.runInNewContext(`${prepare}; prepareBatchSources('Beady_Necklace', plan)`, {
    ensureStorageSignedIn: async () => {}, ROOT: 'listing-generator-1', storage: {}, ref: (_, path) => path,
    plan: [{type: 'edits', folder: 'Models'}, {type: 'edits', folder: 'Models'}, {type: 'copy', folder: 'Guide'}],
    listAll: async path => {reads.push(path); return {items: empty ? [] : [{name: 'image.PNG', fullPath: path + '/image.PNG'}, {name: 'manifest.json'}]};},
    buildSlotPrompt: (category, folder, slot) => `${category}/${folder}/${slot}`,
  });
  return {result, reads};
}
(async () => {
  const {result, reads} = await sources();
  assert.equal(reads.length, 2, 'each distinct source folder is read once per submission');
  assert.equal(result.files.get('listing-generator-1/Beady_Necklace/Models').length, 1, 'non-image files are excluded');
  assert.equal(result.prompts[1], 'Beady_Necklace/Models/2');
  assert.equal(result.prompts[2], null);
  await assert.rejects(sources(true), /No source images/, 'missing input is detected during preflight');
  console.log('Batch readiness: completion checkmark, retained history, new active work, unhealthy admission, and source preflight passed');
})().catch(err => {console.error(err); process.exitCode = 1;});
