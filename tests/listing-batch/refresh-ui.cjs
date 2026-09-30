"use strict";

const assert = require("node:assert/strict");
const fs = require("node:fs");
const vm = require("node:vm");

const source = fs.readFileSync("Listing_Generator_1.html", "utf8");
const start = source.indexOf("    function _showBatchCheck(message, error = false) {");
const end = source.indexOf("    function _startBatchAutoPoll() {", start);
assert(start > 0 && end > start, "provider refresh handler exists");
const helperStart = source.indexOf("    function _awaitingStallRestart(b) {");
const _awaitingStallRestart = vm.runInNewContext(`${source.slice(helperStart,
  source.indexOf("    function _formatDuration(ms) {", helperStart))}; _awaitingStallRestart`, {});

async function check({ queued = false, automatic = false, blocked = false } = {}) {
  const btn = { disabled: false, textContent: "↻ Refresh" };
  const status = { textContent: "", style: {}, changes: [] };
  Object.defineProperty(status, "textContent", {
    get() { return this.value; },
    set(value) { this.value = value; this.changes.push(value); },
  });
  const jobs = blocked ? [{ batchName: 'batch_issue', state: 'JOB_STATE_SUCCEEDED', collected: true,
      setComplete: false, recoveryStatus: 'blocked', results: { failedCount: 1 } },
      { batchName: 'batch_old_failure', state: 'JOB_STATE_FAILED', collected: false,
        retryBatchName: 'batch_issue', results: { failedCount: 1 } }] : queued
    ? [{ batchName: "batch_failed", state: "JOB_STATE_FAILED", collected: false, retryRequested: true }]
    : [{ batchName: "batch_running", state: "JOB_STATE_RUNNING", collected: false }];
  const sweep = { lastSweepAt: 0, lastResult: null };
  let serverState = jobs[0].state;
  let statusCalls = 0, sweepCalls = 0, collects = 0, listCalls = 0;
  const context = {
    _batchPollInFlight: false,
    _lastBatchList: jobs,
    _batchRetryLimit: 30,
    _batchSweepInfo: sweep,
    _autoCollectsInFlight: new Set(),
    document: { getElementById: (id) => id === "batchJobsRefreshBtn" ? btn :
      id === "batchRefreshStatus" ? status : null },
    _normBatchState: (value) => value,
    _awaitingStallRestart,
    refreshBatchJobsPanel: async () => {
      listCalls++;
      context._lastBatchList = jobs.map((job) => ({ ...job, state: blocked ? job.state : serverState }));
      return { ok: true, batches: context._lastBatchList };
    },
    postJson: async (_fn, payload) => {
      if (payload.kind === "batch_status") {
        statusCalls++;
        serverState = "JOB_STATE_SUCCEEDED";
        return { state: "JOB_STATE_SUCCEEDED" };
      }
      if (payload.kind === "batch_collect") { collects++; return { accepted: true }; }
      if (payload.kind === "batch_sweep") {
        sweepCalls++;
        sweep.lastSweepAt = 123;
        sweep.lastResult = { retriesSubmitted: 1 };
        return { accepted: true };
      }
      if (payload.kind === "batch_list") return { ok: true, sweep };
      throw new Error(`unexpected ${payload.kind}`);
    },
    sleep: async () => {},
    console: { warn: () => {} },
  };
  await vm.runInNewContext(`(async () => { ${source.slice(start, end)} await _checkBatchJobsNow(${!automatic}); })()`, context);
  return { btn, status, statusCalls, sweepCalls, collects, listCalls };
}

(async () => {
  const provider = await check();
  assert.equal(provider.statusCalls, 1, "Refresh queries live provider state");
  assert.equal(provider.collects, 1, "a new success is queued for collection");
  assert(provider.status.changes.some((x) => x.includes("Checking provider jobs: 0/1")));
  assert.match(provider.status.textContent, /Checked .*Saving images from 1 job/);
  assert.equal(provider.btn.disabled, false);
  assert.equal(provider.btn.textContent, "↻ Refresh");

  const retry = await check({ queued: true });
  assert.equal(retry.sweepCalls, 1, "Refresh asks collector to fill an available place");
  assert.match(retry.status.textContent, /1 retries admitted by collector/);
  const automatic = await check({ automatic: true });
  assert.match(automatic.status.textContent, /Checked .*Saving images from 1 job/);
  const terminal = await check({ blocked: true });
  assert.equal(terminal.listCalls, 1, 'completed refresh reads saved progress once');
  assert.equal(terminal.statusCalls + terminal.sweepCalls + terminal.collects, 0,
    'terminal issues and old failures never restart the worker or paid generation');
  assert.match(terminal.status.textContent, /Batch processing finished/);

  const renderStart = source.indexOf("    function _renderSessionBlock(session) {");
  const renderEnd = source.indexOf("    async function _cancelSession(sessionId) {", renderStart);
  assert(renderStart > 0 && renderEnd > renderStart);
  const now = Date.now();
  const batches = [
    ...Array.from({ length: 30 }, (_, i) => ({ batchName: `batch_active_${i}`,
      displayName: `lg1-Beady_Necklace-300sets-test-part${i + 1}of300`,
      state: "JOB_STATE_RUNNING", createdAt: now - 3 * 3600000, updatedAt: now - 60000,
      batchStats: { requestCount: 6, pendingRequestCount: 6, successfulRequestCount: 0, failedRequestCount: 0 },
      setsCount: 1 })),
    { batchName: "batch_failed", displayName: "lg1-Beady_Necklace-300sets-test-part31of300",
      state: "JOB_STATE_FAILED", retryRequested: true, setsCount: 1 },
  ];
  const render = (jobs, sweep = null) => vm.runInNewContext(`${source.slice(renderStart, renderEnd)}; _renderSessionBlock({
    sessionId: "sess_test", batches, earliest: Date.now() - 3 * 3600000, latest: Date.now()
  })`, {
    batches: jobs, Date, _batchRetryLimit: 30, _batchSweepInfo: sweep, _batchNextSweepAt: now + 600000,
    _normBatchState: (x) => x, _awaitingStallRestart, _batchSafeText: (x) => String(x),
    _formatDuration: (ms) => `${Math.round(ms / 3600000)}h`,
    normalizeImageModelId: (x) => x, getImageModelConfig: () => ({ label: "Sunburst" }),
    DEFAULT_IMAGE_MODEL: "sunburst",
  });
  const html = render(batches);
  assert.match(html, /Waiting for OpenAI/);
  assert.match(html, /0 completed · 180 pending · 0 failed/);
  assert.match(html, /OpenAI has not returned images for the active jobs/);
  assert.match(html, /Oldest active job/);
  const summary = html.slice(0, html.indexOf('<details class="batch-details"'));
  assert.match(summary, /0 \/ 300/);
  assert.match(summary, /30 active jobs · 1 queued/);
  assert(!summary.includes("Active image requests"), "technical stats are collapsed");
  assert(!summary.includes("data-session-cancel"), "destructive actions are collapsed");
  assert(!/<details[^>]*\sopen(?:[\s>])/.test(html), "details start closed");
  assert.match(html, /data-session-cancel/, "recovery and cancellation controls remain available");
  const stopping = render(batches.slice(0, 30).map((b) => ({ ...b, providerStatus: "cancelling" })));
  const stoppingSummary = stopping.slice(0, stopping.indexOf('<details class="batch-details"'));
  assert.match(stoppingSummary, /Stopping jobs/);
  assert.match(stoppingSummary, /30 stopping/);
  assert.match(stoppingSummary, /Waiting for OpenAI to confirm cancellation/);
  assert(!stoppingSummary.includes("Waiting for OpenAI</div>"), "cancellation takes precedence over the stale waiting headline");
  assert(!stopping.includes("data-session-cancel"), "already stopping jobs cannot be cancelled twice");

  const partial = render([
    { ...batches[0], state: "JOB_STATE_SUCCEEDED", collected: true, results: { succeededCount: 6, failedCount: 0 } },
    { ...batches[1], state: "JOB_STATE_SUCCEEDED", collected: true, results: { succeededCount: 5, failedCount: 1 } },
  ]);
  assert.match(partial, /1 \/ 300/, "a partly saved set is not counted as complete");
  assert.match(partial, /1 need review/, "missing images remain visible");
  const filling = render([...batches.slice(0, 7), batches[30]], { runningSince: now - 5000, stage: "validating new job" });
  const fillingSummary = filling.slice(0, filling.indexOf('<details class="batch-details"'));
  assert.match(fillingSummary, /Filling queue/);
  assert.match(fillingSummary, /OpenAI is validating the next set/);
  assert.match(fillingSummary, /7 active jobs · 1 queued/);
  const staleFilling = render([...batches.slice(0, 7), batches[30]], { runningSince: now - 15 * 60000, stage: "validating new job" });
  assert(!staleFilling.includes('>Filling queue</div>'), "stale worker state cannot pretend the queue is filling");
  console.log("Listing refresh UI: provider check, feedback, collection, retry sweep passed");
})().catch((err) => { console.error(err); process.exitCode = 1; });
