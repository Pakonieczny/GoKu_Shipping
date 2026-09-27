"use strict";

const assert = require("node:assert/strict");
const fs = require("node:fs");
const vm = require("node:vm");

const source = fs.readFileSync("netlify/functions/geminiImageProxy-background.js", "utf8");
const start = source.indexOf('  if (kind === "batch_sweep") {');
const end = source.indexOf('  if (kind === "job_status") {', start);
assert(start > 0 && end > start, "scheduled sweep handler exists");

async function runSweep({ rejectRetry = false } = {}) {
  const jobs = [
    ...Array.from({ length: 30 }, (_, i) => ({ batchName: `batch_running_${i}`,
      state: "JOB_STATE_RUNNING", collected: false })),
    { batchName: "batch_failed_original", state: "JOB_STATE_FAILED",
      collected: false, retryRequested: true, retryAttempt: 0 },
    { batchName: "batch_failed_second", state: "JOB_STATE_FAILED",
      collected: false, retryRequested: true, retryAttempt: 0 },
  ];
  const guard = {};
  const submissions = [], collections = [];
  const db = { collection: (name) => {
    if (name === "LG1_Config") return { doc: () => ({
      get: async () => ({ exists: false }),
      set: async (value) => Object.assign(guard, value),
    }) };
    if (name === "orchestrations") return { where: () => ({ limit: () => ({
      get: async () => ({ forEach: () => {} }),
    }) }) };
    if (name === "batches") return {
      where: () => ({ orderBy: () => ({ limit: () => ({ get: async () => ({
        size: jobs.length,
        forEach: (fn) => jobs.forEach((job) => fn({ data: () => ({ ...job }) })),
      }) }) }) }),
      doc: () => ({ set: async () => {} }),
    };
    throw new Error(`unexpected collection ${name}`);
  } };
  const handler = async (event) => {
    const payload = JSON.parse(event.body);
    let response;
    if (payload.kind === "batch_status") {
      const finished = payload.batchName === "batch_running_0";
      const rejected = rejectRetry && payload.batchName === "batch_new_1";
      response = { ok: true, state: finished ? "JOB_STATE_SUCCEEDED" :
        rejected ? "JOB_STATE_FAILED" : "JOB_STATE_RUNNING" };
    } else if (payload.kind === "batch_collect") {
      collections.push(payload.batchName);
      response = { ok: true };
    } else if (payload.kind === "batch_retry_missing") {
      submissions.push(payload.batchName);
      response = { ok: true, batchName: `batch_new_${submissions.length}` };
    } else throw new Error(`unexpected call ${payload.kind}`);
    return { body: JSON.stringify(response) };
  };
  const result = await vm.runInNewContext(`(async () => { ${source.slice(start, end)} })()`, {
    kind: "batch_sweep", getDb: () => db,
    BATCHES_COLL: "batches", ORCH_COLL: "orchestrations",
    admin: { firestore: { FieldPath: { documentId: () => "__name__" },
      FieldValue: { serverTimestamp: () => Date.now() } } },
    module: { exports: { handler } },
    json: (statusCode, body) => ({ statusCode, ...body }),
    console: { warn: () => {}, error: () => {} },
    process: { env: {} },
    batchDocIdFromName: (x) => x,
    safeErr: (err) => ({ message: err.message }),
  });
  return { result, guard, submissions, collections };
}

(async () => {
  const healthy = await runSweep();
  assert.equal(healthy.result.statusCode, 200);
  assert.equal(healthy.result.statusChecked, 30);
  assert.deepEqual(healthy.collections, ["batch_running_0"]);
  assert.deepEqual(healthy.submissions, ["batch_failed_original"]);
  assert.equal(healthy.guard.lastResult.activeAtAdmission, 30);
  assert.equal(healthy.guard.lastResult.waiting, 2);
  assert.equal(healthy.guard.stage, "idle");
  assert.equal(healthy.guard.runningSince, null);

  const rejected = await runSweep({ rejectRetry: true });
  assert.equal(rejected.result.statusCode, 200);
  assert.deepEqual(rejected.submissions, ["batch_failed_original"],
    "provider rejection stops the sweep before another retry");
  console.log("Listing scheduled sweep: state refresh, capacity refill and refusal passed");
})().catch((err) => { console.error(err); process.exitCode = 1; });
