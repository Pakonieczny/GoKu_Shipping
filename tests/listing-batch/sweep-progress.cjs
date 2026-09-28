"use strict";

const assert = require("node:assert/strict");
const fs = require("node:fs");
const vm = require("node:vm");

const source = fs.readFileSync("netlify/functions/geminiImageProxy-background.js", "utf8");
const start = source.indexOf('  if (kind === "batch_sweep") {');
const end = source.indexOf('  if (kind === "job_status") {', start);
assert(start > 0 && end > start, "scheduled sweep handler exists");

async function runSweep({ rejectRetry = false, activeCount = 30, waitingCount = 2,
  finishOne = true, validationPolls = 0, existingPending = false,
  neverValidates = false, validationError = false } = {}) {
  let clock = 1000000;
  class Clock extends Date { static now() { return clock; } }
  const jobs = [
    ...Array.from({ length: activeCount }, (_, i) => ({ batchName: `batch_running_${i}`,
      state: i === 0 && existingPending ? "JOB_STATE_PENDING" : "JOB_STATE_RUNNING", collected: false })),
    ...Array.from({ length: waitingCount }, (_, i) => ({
      batchName: i === 0 ? "batch_failed_original" : `batch_failed_${i}`,
      state: "JOB_STATE_FAILED", collected: false, retryRequested: true, retryAttempt: 0 })),
  ];
  const guard = {};
  const submissions = [], collections = [], stages = [], continuations = [];
  const checks = new Map();
  let validating = existingPending ? "batch_running_0" : null;
  const db = { collection: (name) => {
    if (name === "LG1_Config") return { doc: () => ({
      get: async () => ({ exists: false }),
      set: async (value) => { if (value.stage) stages.push(value.stage); Object.assign(guard, value); },
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
      const finished = finishOne && payload.batchName === "batch_running_0";
      const rejected = rejectRetry && payload.batchName === "batch_new_1";
      const count = (checks.get(payload.batchName) || 0) + 1;
      checks.set(payload.batchName, count);
      const pending = payload.batchName === validating && (neverValidates || count <= validationPolls);
      if (!pending && payload.batchName === validating) validating = null;
      response = { ok: true, state: finished ? "JOB_STATE_SUCCEEDED" :
        pending ? "JOB_STATE_PENDING" : rejected ? "JOB_STATE_FAILED" : "JOB_STATE_RUNNING" };
      if (validationError && payload.batchName.startsWith("batch_new_") && !pending)
        response = { ok: false, error: { message: "Provider status unavailable" } };
    } else if (payload.kind === "batch_collect") {
      collections.push(payload.batchName);
      response = { ok: true };
    } else if (payload.kind === "batch_retry_missing") {
      if (validating) return { body: JSON.stringify({ ok: true, queued: true, reason: "Waiting for provider validation" }) };
      submissions.push(payload.batchName);
      validating = `batch_new_${submissions.length}`;
      response = { ok: true, batchName: validating };
    } else throw new Error(`unexpected call ${payload.kind}`);
    return { body: JSON.stringify(response) };
  };
  const result = await vm.runInNewContext(`(async () => { ${source.slice(start, end)} })()`, {
    kind: "batch_sweep", getDb: () => db,
    admissionControl: () => ({ reconcile: async () => {} }),
    quotaFailure: require("../../netlify/functions/lib/listingBatchAdmission.cjs").quotaFailure,
    BATCHES_COLL: "batches", ORCH_COLL: "orchestrations",
    admin: { firestore: { FieldPath: { documentId: () => "__name__" },
      FieldValue: { serverTimestamp: () => clock } } },
    module: { exports: { handler } },
    json: (statusCode, body) => ({ statusCode, ...body }),
    console: { warn: () => {}, error: () => {} },
    process: { env: { URL: "https://example.test" } },
    Date: Clock,
    setTimeout: (fn, ms) => { clock += ms; fn(); },
    fetch: async (url, options) => { continuations.push({ url, body: JSON.parse(options.body) }); return { ok: true, status: 202 }; },
    batchDocIdFromName: (x) => x,
    safeErr: (err) => ({ message: err.message }),
  });
  return { result, guard, submissions, collections, stages, continuations, checks };
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

  const refill = await runSweep({ activeCount: 7, waitingCount: 40, finishOne: false, validationPolls: 2 });
  assert.equal(refill.submissions.length, 23, "one worker fills seven active jobs to thirty despite delayed validation");
  assert.equal(refill.guard.lastResult.activeAtAdmission, 30, "refill stops at the active limit");
  assert.equal(refill.checks.get("batch_new_23"), 3, "each new job is confirmed before the next is submitted");
  assert(refill.stages.includes("validating new job"));
  assert.equal(refill.continuations.length, 0, "a full queue needs no continuation");

  const resumed = await runSweep({ activeCount: 7, waitingCount: 40, finishOne: false, validationPolls: 2, existingPending: true });
  assert.equal(resumed.submissions.length, 23, "a job already validating when the worker starts does not strand the queue");

  const delayedRefusal = await runSweep({ activeCount: 7, waitingCount: 40, finishOne: false, validationPolls: 2, rejectRetry: true });
  assert.equal(delayedRefusal.submissions.length, 1, "delayed refusal stops further paid submissions");
  assert.equal(delayedRefusal.continuations.length, 0, "quota refusal does not start a retry loop");

  const timeout = await runSweep({ activeCount: 7, waitingCount: 40, finishOne: false, neverValidates: true });
  assert.equal(timeout.submissions.length, 1, "an unvalidated request is never duplicated");
  assert.equal(timeout.guard.lastResult.activeAtAdmission, 8, "validation consumes an active place");
  assert.equal(timeout.continuations.length, 1, "budget exhaustion hands work to a fresh durable worker");
  assert.equal(timeout.continuations[0].body.kind, "batch_sweep");

  const unavailable = await runSweep({ activeCount: 7, waitingCount: 40, finishOne: false, validationPolls: 2, validationError: true });
  assert.equal(unavailable.result.statusCode, 502);
  assert.equal(unavailable.submissions.length, 1, "an unknown validation outcome stops admission");
  assert.match(unavailable.guard.lastError, /Provider status unavailable/);
  console.log("Listing sweep: refill to 30, delayed and existing validation, refusal, status errors and durable continuation passed");
})().catch((err) => { console.error(err); process.exitCode = 1; });
