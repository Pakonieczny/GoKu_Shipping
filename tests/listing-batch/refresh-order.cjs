"use strict";

// A manual Refresh, the automatic poll and the Charm Maker panel can ask for
// batch_list at the same time. The dashboard must show only the newest answer:
// an older answer (or an older failure) that lands late must not put stale
// numbers back on screen or into _lastBatchList.
// Usage: node tests/listing-batch/refresh-order.cjs [Listing_Generator_1.html]

const assert = require("node:assert/strict");
const fs = require("node:fs");
const vm = require("node:vm");

const source = fs.readFileSync(process.argv[2] || "Listing_Generator_1.html", "utf8");
const cut = (from, to) => {
  const start = source.indexOf(from);
  const end = source.indexOf(to, start);
  assert(start > 0 && end > start, `found ${from.trim()}`);
  return source.slice(start, end);
};
const generate = cut("    let _batchPollTimer = null;", "    // Normalize state for both legacy");
const charm = cut("    async function refreshCharmBatchPanel() {",
  '    document.getElementById("charmBatchJobsRefreshBtn")');

function page() {
  const calls = [];
  const el = () => ({ innerHTML: "", textContent: "", style: {}, querySelectorAll: () => [] });
  const nodes = { batchJobsList: el(), charmBatchJobsList: el(), batchRefreshStatus: el() };
  const context = {
    document: { getElementById: (id) => nodes[id] || null },
    postJson: (_fn, payload) => new Promise((resolve, reject) => calls.push({ payload, resolve, reject })),
    _renderSessionBlock: (s) => `[${s.batches.filter((b) => b.collected).length} saved]`,
    _startBatchAutoPoll: () => {},
    _isCharmMakerBatch: () => false,
    console,
    Date,
  };
  const api = vm.runInNewContext(`${generate}
    let _cmCollectedSeen = -1;
    ${charm}
    ({ refresh: refreshBatchJobsPanel, charm: refreshCharmBatchPanel,
       list: () => _lastBatchList, sweep: () => _batchSweepInfo })`, context);
  const listCall = (n) => calls.filter((c) => c.payload.kind === "batch_list")[n];
  return { api, nodes, calls, listCall };
}
const answer = (saved, stage) => ({ ok: true, sweep: { stage },
  batches: Array.from({ length: 3 }, (_, i) => ({ batchName: `batch_${i}`, sessionId: "sess_x",
    state: i < saved ? "JOB_STATE_SUCCEEDED" : "JOB_STATE_RUNNING", collected: i < saved })) });
const tick = () => new Promise((resolve) => setImmediate(resolve));

(async () => {
  // Poll asks first, Refresh asks second; Refresh's newer answer lands first.
  {
    const { api, nodes, listCall } = page();
    const poll = api.refresh();
    const manual = api.refresh();
    listCall(1).resolve(answer(2, "newer"));
    assert.equal((await manual).ok, true);
    assert.equal(nodes.batchJobsList.innerHTML, "[2 saved]");
    listCall(0).resolve(answer(0, "older"));
    const late = await poll;
    assert.equal(nodes.batchJobsList.innerHTML, "[2 saved]", "older answer does not overwrite the dashboard");
    assert.equal(api.list().filter((b) => b.collected).length, 2, "_lastBatchList keeps the newer answer");
    assert.equal(api.sweep().stage, "newer", "collector line keeps the newer answer");
    assert.equal(late.ok, true, "a late answer is not an error");
    assert.equal(late.stale, true, "the late answer is reported as stale");
  }
  // An older request that fails late does not replace the newer dashboard with an error.
  {
    const { api, nodes, listCall } = page();
    const poll = api.refresh();
    const manual = api.refresh();
    listCall(1).resolve(answer(1, "newer"));
    await manual;
    listCall(0).reject(new Error("network hiccup"));
    const late = await poll;
    assert.equal(late.ok, true);
    assert.equal(nodes.batchJobsList.innerHTML, "[1 saved]");
    assert(!nodes.batchRefreshStatus.textContent.includes("Could not load"), "no stale error line");
  }
  // In order, both answers apply and the second one wins.
  {
    const { api, nodes, listCall } = page();
    const first = api.refresh();
    const second = api.refresh();
    listCall(0).resolve(answer(1, "first"));
    await first;
    assert.equal(nodes.batchJobsList.innerHTML, "[1 saved]");
    listCall(1).resolve(answer(3, "second"));
    await second;
    assert.equal(nodes.batchJobsList.innerHTML, "[3 saved]");
  }
  // The Charm Maker panel's late, older answer does not replace the shared list.
  {
    const { api, calls, listCall } = page();
    const charmRefresh = api.charm();
    await tick();
    calls.find((c) => c.payload.kind === "charm_batch_status").resolve({ orchestrations: [] });
    await tick();
    const manual = api.refresh();
    listCall(1).resolve(answer(3, "newer"));
    await manual;
    listCall(0).resolve(answer(0, "older"));
    await charmRefresh;
    assert.equal(api.list().filter((b) => b.collected).length, 3, "charm panel's older answer is ignored");
  }
  console.log("Listing batch dashboard: only the newest batch_list answer is applied");
})().catch((err) => { console.error(err); process.exitCode = 1; });
