"use strict";

// Firebase cost (FC12b): the Batch panel's poll (batch_list) reads session summaries and job records.
//   - a session summary lists every set of the session (`sets`, up to 130 KB); the panel only needs the sets that need a
//     person, so the summary carries a short `issueSets` and batch_list reads every field but `sets`; a summary written before
//     `issueSets` existed is read whole and the page falls back to its `sets`
// No network. Usage: node tests/listing-batch/list-cost.cjs

const assert = require("node:assert/strict");
const fs = require("node:fs");
const vm = require("node:vm");

const page = fs.readFileSync("Listing_Generator_1.html", "utf8");
const cut = (src, from, to) => {
  const start = src.indexOf(from);
  const end = src.indexOf(to, start);
  assert(start > 0 && end > start, `found ${from.trim().split("\n")[0]}`);
  return src.slice(start, end);
};
const awaiting = cut(page, "    function _awaitingStallRestart(b) {", "    function _formatDuration(ms) {");
const renderCode = cut(page, "    function _renderSessionBlock(session) {", "    async function _cancelSession(sessionId) {");
const dashboardCode = cut(page, "    function _renderBatchDashboard(sessions, response) {", "    async function refreshBatchJobsPanel() {");

const now = Date.now();
const env = {
  Date, _batchRetryLimit: 30, _batchSweepInfo: null, _batchNextSweepAt: null, _batchInFlight: false,
  _normBatchState: (x) => x, _batchSafeText: (x) => String(x), _formatDuration: (ms) => `${Math.round(ms / 3600000)}h`,
  normalizeImageModelId: (x) => x, getImageModelConfig: () => ({ label: "Sunburst" }), DEFAULT_IMAGE_MODEL: "sunburst",
};
const api = vm.runInNewContext(`${awaiting}\n${renderCode}\n${dashboardCode}\n({ block: _renderSessionBlock, dashboard: _renderBatchDashboard })`, env);

const job = (n, extra = {}) => ({ batchName: `batch_${n}`, displayName: `lg1-Rings-3sets-test-part${n}of3`, state: "JOB_STATE_SUCCEEDED", collected: true,
  setComplete: false, createdAt: now - 3600000, updatedAt: now - 60000, setsCount: 1, recoveryStatus: "blocked", results: { succeededCount: 4, failedCount: 1 }, ...extra });
const issue = { category: "Rings", setN: 2, status: "blocked", missingSlots: [3], note: "Model photo rejected." };
const fine = { category: "Rings", setN: 1, status: "complete", missingSlots: [], note: "" };
const summary = (extra) => ({ sessionId: "sess_1", planned: 3, registered: 3, complete: 2, blocked: 1, cancelled: 0, saving: 0, issues: 1, processed: 3, pending: 0,
  status: "completed_with_issues", checkedAt: now, finishedAt: now, missingImages: 1, ...extra });
const session = (s) => ({ sessionId: "sess_1", batches: [job(1), job(2), job(3)], earliest: now - 3600000, latest: now, summary: s });

(async () => {
  // The card lists the set that needs attention from `issueSets` alone (no `sets` in the answer) ...
  const compact = api.block(session(summary({ issueSets: [issue] })));
  assert.match(compact, /Set_2<\/strong> · Rings · needs attention · missing slots 3 · Model photo rejected\./, "issue list comes from issueSets");
  // ... and exactly as before from an old summary that only has `sets`.
  const old = api.block(session(summary({ sets: [fine, issue] })));
  assert.match(old, /Set_2<\/strong> · Rings · needs attention · missing slots 3 · Model photo rejected\./, "old summaries still show their issues");
  assert(!/Set_1<\/strong>/.test(old), "finished sets are not listed");
  // Both give the same card.
  const strip = (html) => html.replace(/Submission · [^<]*/, "").replace(/\d{1,2}:\d{2}:\d{2}[^<]*/g, "");
  assert.equal(strip(compact), strip(api.block(session(summary({ sets: [fine, issue], issueSets: [issue] })))), "issueSets and sets draw the same card");
  // An empty issueSets means "nothing to show", not "fall back to the old field".
  const none = api.block(session(summary({ sets: [fine, issue], issueSets: [] })));
  assert(!/Set_2<\/strong>/.test(none), "an empty issueSets lists nothing");

  // The ready notice counts issues from issueSets, or from sets when that is all there is.
  const finished = (s) => api.dashboard([{ sessionId: "sess_1", batches: [job(1, { setComplete: true, recoveryStatus: "complete", results: { succeededCount: 5, failedCount: 0 } })],
    earliest: now - 3600000, latest: now, summary: s }], { sweep: { lastSweepAt: now }, admission: {} });
  assert.match(finished(summary({ issueSets: [issue, { ...issue, setN: 4, status: "cancelled" }] })), /· 2 issues/, "issues counted from issueSets");
  assert.match(finished(summary({ sets: [fine, issue] })), /· 1 issue/, "issues counted from the old sets field");
  console.log("list cost: summary issueSets and its fallback to sets passed");
})().catch((err) => { console.error(err); process.exitCode = 1; });
