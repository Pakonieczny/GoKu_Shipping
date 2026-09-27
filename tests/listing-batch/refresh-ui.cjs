"use strict";

const assert = require("node:assert/strict");
const fs = require("node:fs");
const vm = require("node:vm");

const source = fs.readFileSync("Listing_Generator_1.html", "utf8");
const start = source.indexOf("    function _showBatchCheck(message, error = false) {");
const end = source.indexOf("    function _startBatchAutoPoll() {", start);
assert(start > 0 && end > start, "provider refresh handler exists");

async function check({ queued = false, automatic = false } = {}) {
  const btn = { disabled: false, textContent: "↻ Refresh" };
  const status = { textContent: "", style: {}, changes: [] };
  Object.defineProperty(status, "textContent", {
    get() { return this.value; },
    set(value) { this.value = value; this.changes.push(value); },
  });
  const jobs = queued
    ? [{ batchName: "batch_failed", state: "JOB_STATE_FAILED", collected: false, retryRequested: true }]
    : [{ batchName: "batch_running", state: "JOB_STATE_RUNNING", collected: false }];
  const sweep = { lastSweepAt: 0, lastResult: null };
  let serverState = jobs[0].state;
  let statusCalls = 0, sweepCalls = 0, collects = 0;
  const context = {
    _batchPollInFlight: false,
    _lastBatchList: jobs,
    _batchRetryLimit: 30,
    _batchSweepInfo: sweep,
    _autoCollectsInFlight: new Set(),
    document: { getElementById: (id) => id === "batchJobsRefreshBtn" ? btn :
      id === "batchRefreshStatus" ? status : null },
    _normBatchState: (value) => value,
    refreshBatchJobsPanel: async () => {
      context._lastBatchList = jobs.map((job) => ({ ...job, state: serverState }));
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
  return { btn, status, statusCalls, sweepCalls, collects };
}

(async () => {
  const provider = await check();
  assert.equal(provider.statusCalls, 1, "Refresh queries live provider state");
  assert.equal(provider.collects, 1, "a new success is queued for collection");
  assert(provider.status.changes.some((x) => x.includes("Checking provider jobs: 0/1")));
  assert.match(provider.status.textContent, /Manual check finished.*1 state changes/);
  assert.equal(provider.btn.disabled, false);
  assert.equal(provider.btn.textContent, "↻ Refresh");

  const retry = await check({ queued: true });
  assert.equal(retry.sweepCalls, 1, "Refresh asks collector to fill an available place");
  assert.match(retry.status.textContent, /1 retries admitted by collector/);
  const automatic = await check({ automatic: true });
  assert.match(automatic.status.textContent, /Automatic check finished.*1 state changes/);
  console.log("Listing refresh UI: provider check, feedback, collection, retry sweep passed");
})().catch((err) => { console.error(err); process.exitCode = 1; });
