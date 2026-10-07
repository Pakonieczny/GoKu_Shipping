"use strict";

// Firebase cost (FC12): the Listing Generator's batch poll reads up to 1,500 job records per pass.
//   - a hidden tab does not poll;
//   - passes that find nothing changed come further apart (1, 1, 2, 2, then 3 ticks of BATCH_POLL_MS), and any change,
//     a manual check or a return to the tab starts again at every tick;
//   - the Charm Maker panel, refreshed straight after the poll's own list, reuses that list instead of asking again.
// Usage: node tests/listing-batch/poll-cost.cjs

const assert = require("node:assert/strict");
const fs = require("node:fs");
const vm = require("node:vm");

const source = fs.readFileSync("Listing_Generator_1.html", "utf8");
const cut = (from, to) => {
  const start = source.indexOf(from);
  const end = source.indexOf(to, start);
  assert(start > 0 && end > start, `found ${from.trim()}`);
  return source.slice(start, end);
};
const pollCode = cut("    const _BATCH_QUIET_STEPS = ", '    document.getElementById("batchJobsRefreshBtn")');
const awaiting = cut("    function _awaitingStallRestart(b) {", "    function _formatDuration(ms) {");

function poller() {
  const state = { hidden: false, checks: [], tick: null, list: [{ batchName: "b1", state: "JOB_STATE_RUNNING", collected: false }] };
  const panel = { style: { display: "" } };
  const context = {
    _batchPollTimer: null, _batchPollInFlight: false, BATCH_POLL_MS: 60000, _lastBatchList: state.list,
    _normBatchState: (x) => x,
    document: { get hidden() { return state.hidden; }, getElementById: (id) => id === "batchJobsPanel" ? panel : null },
    setInterval: (fn) => { state.tick = fn; return 1; }, clearInterval: () => {},
    _checkBatchJobsNow: async (manual) => { state.checks.push(manual); },
    console,
  };
  const api = vm.runInNewContext(`${awaiting}\n let _batchPollTimer = null, _batchPollInFlight = false;\n ${pollCode}
    ({ start: _startBatchAutoPoll, wake: _batchPollWake, set list(v) { _lastBatchList = v; }, get quiet() { return _batchQuietPolls; } })`, context);
  return { state, api, context };
}
const flush = () => new Promise((resolve) => setImmediate(resolve));
async function tickN(p, n) { for (let i = 0; i < n; i++) { p.state.tick(); await flush(); } }

(async () => {
  // Hidden tab: no pass at all, however many ticks go by.
  {
    const p = poller(); p.api.start(); p.state.hidden = true;
    await tickN(p, 10);
    assert.equal(p.state.checks.length, 0, "a hidden tab does not poll");
    p.state.hidden = false; await tickN(p, 1);
    assert.equal(p.state.checks.length, 1, "visible again: the next tick polls");
  }
  // Nothing changes: passes at ticks 1, 2, 3 (then every 2 ticks twice, then every 3 ticks).
  {
    const p = poller(); p.api.start();
    const at = [];
    for (let t = 1; t <= 22; t++) { const before = p.state.checks.length; await tickN(p, 1); if (p.state.checks.length > before) at.push(t); }
    assert.deepEqual(at, [1, 2, 3, 5, 7, 10, 13, 16, 19, 22], "quiet passes drift to one every three ticks");
    // A change starts again at every tick.
    p.api.list = [{ batchName: "b1", state: "JOB_STATE_SUCCEEDED", collected: false }];
    const before = p.state.checks.length;
    await tickN(p, 2); assert.equal(p.state.checks.length, before, "still waiting out the three-tick gap");
    await tickN(p, 1); assert.equal(p.state.checks.length, before + 1, "the pass that sees the change");
    assert.equal(p.api.quiet, 0, "a change resets the back-off");
    const b2 = p.state.checks.length;
    await tickN(p, 2); assert.equal(p.state.checks.length, b2 + 2, "after a change every tick polls again");
    // Coming back to the tab or a manual check wakes it too.
    await tickN(p, 6); p.api.wake();
    const b3 = p.state.checks.length; await tickN(p, 1);
    assert.equal(p.state.checks.length, b3 + 1, "wake: the next tick polls");
  }
  // The Charm Maker panel reuses a list loaded moments ago; a click or an old list asks again.
  {
    const lines = source.split("\n");
    const charmCode = cut("    async function refreshCharmBatchPanel() {", '    document.getElementById("charmBatchJobsRefreshBtn")');
    const calls = [];
    const el = () => ({ innerHTML: "", textContent: "", style: {}, querySelectorAll: () => [] });
    let nowMs = 1000000;
    class Clock extends Date { static now() { return nowMs; } }
    const nodes = { charmBatchJobsList: el(), charmBatchJobsPanel: { style: { display: "" } } };
    const context = {
      document: { getElementById: (id) => nodes[id] || null },
      postJson: async (_fn, payload) => { calls.push(payload.kind); return { ok: true, batches: [], orchestrations: [] }; },
      _startBatchAutoPoll: () => {}, _isCharmMakerBatch: () => false, _normBatchState: (x) => x, _awaitingStallRestart: () => false,
      _renderSessionBlock: () => "", console, Date: Clock,
    };
    const api = vm.runInNewContext(`let _batchListSeq = 0, _batchListDataSeq = 0, _lastBatchList = [{ batchName: "kept" }], _batchListLoadedAt = ${nowMs - 5000}, _cmCollectedSeen = -1;
      ${charmCode}
      ({ refresh: refreshCharmBatchPanel, setLoaded(v) { _batchListLoadedAt = v; } })`, context);
    await api.refresh({ reuse: true });
    assert(!calls.includes("batch_list"), "reuse: no new batch_list within 20 s of the poll's list");
    calls.length = 0;
    await api.refresh({ type: "click" });
    assert(calls.includes("batch_list"), "a click asks the server");
    calls.length = 0;
    api.setLoaded(nowMs - 60000);
    await api.refresh({ reuse: true });
    assert(calls.includes("batch_list"), "an old list is not reused");
    void lines;
  }
  console.log("poll cost: ok");
})().catch((err) => { console.error(err); process.exit(1); });
