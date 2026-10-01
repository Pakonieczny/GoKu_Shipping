"use strict";

const assert = require("node:assert/strict");
const fs = require("node:fs");
const vm = require("node:vm");

const source = fs.readFileSync("netlify/functions/geminiImageProxy-background.js", "utf8");
const start = source.indexOf('  if (kind === "batch_sweep") {');
const end = source.indexOf('  if (kind === "job_status") {', start);
assert(start > 0 && end > start, "scheduled sweep handler exists");

const { VALIDATION_WAIT_MS } = require("../../netlify/functions/lib/listingBatchAdmission.cjs");

async function runSweep({ rejectRetry = false, activeCount = 30, waitingCount = 2,
  finishOne = true, validationPolls = 0, existingPending = false, pendingSentAgo = 0,
  neverValidates = false, validationError = false, finishDuringRefill = false, failedAnswers = [], extraJobs = [],
  firstSourceError = false } = {}) {
  const T0 = Date.parse("2026-09-29T17:00:00Z");
  let clock = T0;
  let providerActive = activeCount;
  let originalFinished = false;
  class Clock extends Date { static now() { return clock; } }
  const jobs = [
    ...extraJobs,
    ...Array.from({ length: activeCount }, (_, i) => ({ batchName: `batch_running_${i}`,
      state: i === 0 && existingPending ? "JOB_STATE_PENDING" : "JOB_STATE_RUNNING", collected: false,
      ...(i === 0 && pendingSentAgo ? { createdAt: { toMillis: () => T0 - pendingSentAgo } } : {}) })),
    ...Array.from({ length: waitingCount }, (_, i) => ({
      batchName: i === 0 ? "batch_failed_original" : `batch_failed_${i}`,
      state: "JOB_STATE_FAILED", collected: false, retryRequested: true, retryAttempt: 0 })),
  ];
  const guard = {};
  const submissions = [], collections = [], stages = [], continuations = [], postponed = [];
  const checks = new Map();
  let validating = existingPending ? "batch_running_0" : null;
  let validatingSince = clock - pendingSentAgo;
  const openQuery = () => {
    let offset = 0, count = 50;
    const query = {
      orderBy: () => query,
      limit: n => { count = n; return query; },
      select: (...fields) => { assert(!fields.includes("preparedSubmission")); return query; },
      startAfter: id => { offset = Number(id) + 1; return query; },
      get: async () => {
        const docs = jobs.slice(offset, offset + count).map((job, i) => ({ id: offset + i, data: () => ({ ...job }) }));
        return {size: docs.length, docs, forEach: fn => docs.forEach(fn)};
      },
    };
    return query;
  };
  const db = { collection: (name) => {
    if (name === "LG1_Config") return { doc: () => ({
      get: async () => ({ exists: false }),
      set: async (value) => { if (value.stage) stages.push(value.stage); Object.assign(guard, value); },
    }) };
    if (name === "orchestrations") return { where: () => ({ limit: () => ({
      get: async () => ({ forEach: () => {} }),
    }) }) };
    if (name === "batches") return {
      orderBy: () => ({ limit: () => ({ select: () => ({ get: async () => ({ docs: [] }) }) }) }),
      where: (field) => field === "repairPending" ? { limit: () => ({ select: () => ({ get: async () => ({ forEach: () => {} }) }) }) } : field === "state" ? { get: async () => ({ docs:
        Array.from({ length: providerActive }, () => ({ data: () => ({ collected: false }) })) }) } : openQuery(),
      doc: () => ({ set: async () => {} }),
    };
    throw new Error(`unexpected collection ${name}`);
  } };
  const handler = async (event) => {
    const payload = JSON.parse(event.body);
    let response;
    if (payload.kind === "batch_status") {
      // OpenAI's edge failing to answer one check, as on 2026-09-29.
      const answer = `${payload.batchName}#${(checks.get(payload.batchName) || 0) + 1}`;
      if (failedAnswers.includes(answer)) {
        checks.set(payload.batchName, (checks.get(payload.batchName) || 0) + 1);
        return { body: JSON.stringify({ error: { message: "upstream connect error or disconnect/reset before headers. reset reason: connection timeout" } }) };
      }
      const finished = finishOne && payload.batchName === "batch_running_0";
      if (finished && !originalFinished) { providerActive--; originalFinished = true; }
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
      if (payload.batchName === 'batch_cancelled_paid') assert.equal(payload.force, true, 'recover a cancelled job\'s saved output');
      response = { ok: true };
    } else if (payload.kind === "batch_retry_missing") {
      if (firstSourceError && payload.batchName === "batch_failed_original") {
        postponed.push(payload.batchName);
        return { body: JSON.stringify({ ok: true, queued: true, sourceError: true,
          reason: "Listing reference download timed out" }) };
      }
      // Admission waits on the job being validated, until it is VALIDATION_WAIT_MS old.
      if (validating && clock - validatingSince < VALIDATION_WAIT_MS)
        return { body: JSON.stringify({ ok: true, queued: true, reason: "Waiting for provider validation" }) };
      submissions.push(payload.batchName);
      providerActive++;
      if (finishDuringRefill && submissions.length === 23) providerActive--;
      assert(providerActive <= 30, "actual provider concurrency never exceeds thirty");
      validating = `batch_new_${submissions.length}`;
      validatingSince = clock;
      response = { ok: true, batchName: validating };
    } else throw new Error(`unexpected call ${payload.kind}`);
    return { body: JSON.stringify(response) };
  };
  const result = await vm.runInNewContext(`(async () => { ${source.slice(start, end)} })()`, {
    kind: "batch_sweep", getDb: () => db,
    admissionControl: () => ({ reconcile: async () => {} }),
    quotaFailure: require("../../netlify/functions/lib/listingBatchAdmission.cjs").quotaFailure,
    neverStarted: (record, ...args) => {
      assert((record.sets || []).every(set => !set.tasks), "sweep does not retain historical prompts");
      return require("../../netlify/functions/lib/listingBatchAdmission.cjs").neverStarted(record, ...args);
    },
    stallRestartPending: require("../../netlify/functions/lib/listingBatchAdmission.cjs").stallRestartPending,
    VALIDATION_WAIT_MS, body: {}, stallCutoffMs: require("../../netlify/functions/lib/listingBatchAdmission.cjs").stallCutoffMs,
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
  return { result, guard, submissions, collections, stages, continuations, checks, postponed };
}

(async () => {
  const paidRecovery = await runSweep({activeCount:0,waitingCount:0,extraJobs:[{
    batchName:'batch_cancelled_paid',state:'JOB_STATE_CANCELLED',collected:false,
    collectionPending:true,responsesFile:'file-paid-results'}]});
  assert.deepEqual(paidRecovery.collections,['batch_cancelled_paid']);
  assert.equal(paidRecovery.submissions.length,0,'paid recovery never requests generation');
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
  const sourceFailure = await runSweep({ activeCount: 1, waitingCount: 40, finishOne: false, firstSourceError: true });
  assert.deepEqual(sourceFailure.postponed, ["batch_failed_original"]);
  assert.equal(sourceFailure.submissions.length, 29, "one bad reference does not prevent filling the other twenty-nine places");
  assert.equal(sourceFailure.guard.lastResult.sourceErrors, 1);
  assert.equal(sourceFailure.guard.currentBatchName, null, "finished worker clears its current-job checkpoint");
  assert.equal(sourceFailure.guard.lastProgressAt, sourceFailure.guard.lastSweepAt);
  const history = await runSweep({activeCount: 1, waitingCount: 40, finishOne: false,
    extraJobs: Array.from({length: 1200}, (_, i) => ({batchName: `batch_old_${i}`,
      state: "JOB_STATE_FAILED", collected: false, retryRequested: false,
      sets: [{outputBasePath: `old/${i}`, tasks: [{prompt: "historical prompt".repeat(1000)}]}],
      preparedSubmission: {ignored: true}}))});
  assert.equal(history.submissions.length, 29, "paged historical failures do not hide the live queue");
  assert.equal(history.guard.stage, "idle");
  const finishedDuring = await runSweep({ activeCount: 7, waitingCount: 40, finishOne: false, validationPolls: 2, finishDuringRefill: true });
  assert.equal(finishedDuring.submissions.length, 24, "a place freed during refill is filled before the worker stops");
  assert.equal(finishedDuring.guard.lastResult.activeAtAdmission, 30);

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
  assert.equal(unavailable.submissions.length, 1, "an unknown validation outcome stops admission");
  assert.equal(unavailable.checks.get("batch_new_1"), 5, "three failed answers in a row before giving up");
  assert.equal(unavailable.continuations.length, 0, "no immediate rerun into the same outage");
  assert.equal(unavailable.guard.lastResult.validationUnconfirmed, "batch_new_1");
  assert.equal(unavailable.result.statusCode, 200, "the run still finishes; the next one checks that job again");
  assert.equal(unavailable.guard.lastError, null, "no collector error for an outcome the next run reads");

  // 2026-09-29: one unanswered check while waiting on validation failed the
  // whole run and showed "Collector error: upstream connect error ...".
  const blip = await runSweep({ activeCount: 7, waitingCount: 40, finishOne: false, validationPolls: 2,
    failedAnswers: ["batch_new_1#2", "batch_new_5#3", "batch_new_5#4"] });
  assert.equal(blip.result.statusCode, 200);
  assert.equal(blip.guard.lastError, null);
  assert.equal(blip.submissions.length, 23, "failed answers are asked again and the refill goes on");
  assert.equal(blip.guard.lastResult.validationUnconfirmed, null);
  const blipResumed = await runSweep({ activeCount: 7, waitingCount: 40, finishOne: false, validationPolls: 2,
    existingPending: true, failedAnswers: ["batch_running_0#2", "batch_running_0#3", "batch_running_0#4"] });
  assert.equal(blipResumed.result.statusCode, 200, "a job the run cannot confirm does not fail the run");
  assert.equal(blipResumed.checks.get("batch_running_0"), 4, "given up after three failed answers in a row");
  assert.equal(blipResumed.submissions.length, 0, "admission keeps waiting on that job");

  // 2026-09-29: a job sat in validation from 17:35 UTC; every run spent its
  // budget waiting on it and every queued set waited behind it.
  const stuck = await runSweep({ activeCount: 7, waitingCount: 40, finishOne: false, existingPending: true,
    neverValidates: true, pendingSentAgo: 20 * 60000 });
  assert.equal(stuck.result.statusCode, 200);
  assert.equal(stuck.checks.get("batch_running_0"), 1, "a job validating for twenty minutes is read once, not waited on");
  assert(stuck.stages.indexOf("submitting queued sets") < stuck.stages.indexOf("validating new job"),
    "the run goes straight on to the queue");
  assert.equal(stuck.submissions[0], "batch_failed_original", "admission no longer waits on it");
  const young = await runSweep({ activeCount: 7, waitingCount: 40, finishOne: false, existingPending: true,
    neverValidates: true, pendingSentAgo: 10 * 60000 });
  assert.equal(young.checks.get("batch_running_0"), 61, "a job sent ten minutes ago is waited on until it is fifteen minutes old (a check every five seconds)");
  assert.equal(young.submissions[0], "batch_failed_original", "and the queue goes on then");
  console.log("Listing sweep: refill to 30, delayed and existing validation, refusal, status errors, stuck validation and durable continuation passed");
})().catch((err) => { console.error(err); process.exitCode = 1; });
