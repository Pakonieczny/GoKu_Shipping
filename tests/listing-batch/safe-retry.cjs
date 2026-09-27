"use strict";

const assert = require("node:assert/strict");
const fs = require("node:fs");
const vm = require("node:vm");

const source = fs.readFileSync("netlify/functions/geminiImageProxy-background.js", "utf8");
const start = source.indexOf('    if (kind === "batch_retry_missing") {');
const end = source.indexOf('    if (kind === "batch_status") {', start);
assert(start > 0 && end > start, "retry handler exists");
const retryBranch = source.slice(start, end);

const tasks = Array.from({ length: 6 }, (_, slotIndex) => ({
  type: slotIndex === 5 ? "copy" : "edits",
  slotIndex,
  prompt: `Original prompt ${slotIndex}`,
  input_storage_path: "reference.png",
  input_charm_storage_path: "original-charm.png",
  source_storage_path: "size-guide.png",
}));
const base = "listing-generator-1/Beady_Necklace/Ready_To_List/Set_123";

async function scenario({ approved = false, present = [1, 3, 6], responseFile = null,
                          retryBatchName = null } = {}) {
  const record = { batchName: "batch_original", state: "JOB_STATE_FAILED", model: "gpt-image-2.5-sunburst",
    imageSize: "2K", sessionId: "session-1", collected: false, retryBatchName,
    sets: [{ category: "Beady_Necklace", setN: 123, outputBasePath: base, tasks }] };
  const writes = [], submitted = [];
  const ref = {
    get: async () => ({ exists: true, data: () => record }),
    set: async (data) => { writes.push(data); Object.assign(record, data); },
  };
  const db = {
    collection: () => ({ doc: () => ref }),
    runTransaction: async (fn) => fn({ get: ref.get, set: (_ref, data) => {
      writes.push(data); Object.assign(record, data);
    } }),
  };
  const bucket = {
    getFiles: async ({ prefix }) => [prefix.includes("Completed_Listing_Sets")
      ? (approved ? [{ name: prefix + "Slot_1.png" }] : [])
      : present.map((n) => ({ name: `${base}/Slot_${n}.png` }))],
    file: (path) => ({ exists: async () => [present.includes(Number(/Slot_(\d+)/.exec(path)?.[1]))],
      copy: async () => { throw new Error("unexpected copy"); } }),
  };
  const sandbox = {
    kind: "batch_retry_missing", body: { batchName: "batch_original" },
    BATCHES_COLL: "batches", batchDocIdFromName: (n) => n,
    getDb: () => db, batchApiKey: () => "key",
    getGeminiBatchJob: async () => ({ state: "JOB_STATE_FAILED",
      response: { responsesFile: responseFile } }),
    batchFailureDetails: () => null,
    assertAllowedOutputBase: (path) => assert.equal(path, base),
    admin: { storage: () => ({ bucket: () => bucket }),
      firestore: { FieldValue: { serverTimestamp: () => ({ toMillis: () => Date.now() }) } } },
    module: { exports: { handler: async (event) => {
      submitted.push(JSON.parse(event.body));
      return { statusCode: 200, body: JSON.stringify({ ok: true, batchName: "batch_retry" }) };
    } } },
    json: (statusCode, value) => ({ statusCode, ...value }),
  };
  const result = await vm.runInNewContext(`(async () => { ${retryBranch} })()`, sandbox);
  return { result, writes, submitted, record };
}

(async () => {
  const retry = await scenario();
  assert.equal(retry.result.statusCode, 200);
  assert.equal(retry.result.batchName, "batch_retry");
  assert.deepEqual(Array.from(retry.submitted[0].sets[0].tasks, (t) => t.slotIndex), [1, 3, 4]);
  assert.equal(retry.submitted[0].sets[0].allTasks.length, 6);
  assert.equal(retry.submitted[0].retryOf, "batch_original");
  assert.equal(retry.record.retryBatchName, "batch_retry");

  const approval = await scenario({ approved: true });
  assert.equal(approval.result.statusCode, 409);
  assert.equal(approval.submitted.length, 0);

  const recoverFirst = await scenario({ responseFile: "file-errors" });
  assert.equal(recoverFirst.result.statusCode, 409);
  assert.equal(recoverFirst.submitted.length, 0);

  const repeated = await scenario({ retryBatchName: "batch_retry_existing" });
  assert.equal(repeated.result.batchName, "batch_retry_existing");
  assert.equal(repeated.submitted.length, 0);

  const complete = await scenario({ present: [1, 2, 3, 4, 5, 6] });
  assert.equal(complete.result.complete, true);
  assert.equal(complete.submitted.length, 0);

  const helperStart = source.indexOf("function batchFailureDetails(data) {");
  const helperEnd = source.indexOf("\n}", helperStart) + 2;
  const describe = vm.runInNewContext(`${source.slice(helperStart, helperEnd)}; batchFailureDetails`, {});
  assert.equal(describe({ errors: { data: [{ code: "invalid_request", message: "Invalid image edit" }] } }), "Invalid image edit");

  const queueStart = source.indexOf('    if (kind === "batch_retry_queue") {');
  const queueEnd = source.indexOf('    if (kind === "batch_retry_missing") {', queueStart);
  assert(queueStart > 0 && queueEnd > queueStart, "durable retry queue exists");
  const queueRecords = [
    { state: "JOB_STATE_FAILED", collected: false, sets: [{ setKind: null }] },
    { state: "JOB_STATE_RUNNING", collected: false, sets: [{ setKind: null }] },
    { state: "JOB_STATE_FAILED", collected: false, responsesFile: "file-123", sets: [{ setKind: null }] },
    { state: "JOB_STATE_FAILED", collected: false, retryRequested: true, sets: [{ setKind: null }] },
  ];
  const changes = [];
  const docs = queueRecords.map((value) => ({ data: () => value, ref: value }));
  const queueDb = { collection: () => ({ where: () => ({ limit: () => ({ get: async () => ({ docs }) }) }) }),
    batch: () => ({ set: (ref, value) => changes.push([ref, value]), commit: async () => {} }) };
  const queued = await vm.runInNewContext(`(async () => { ${source.slice(queueStart, queueEnd)} })()`, {
    kind: "batch_retry_queue", body: { sessionId: "sess_123456789_abcdef" },
    BATCHES_COLL: "batches", getDb: () => queueDb,
    admin: { firestore: { FieldValue: { serverTimestamp: () => 123 } } },
    json: (statusCode, value) => ({ statusCode, ...value }),
  });
  assert.equal(queued.queued, 1);
  assert.equal(queued.alreadyQueued, 1);
  assert.equal(queued.protected, 2);
  assert.equal(changes.length, 1);
  assert.equal(changes[0][0], queueRecords[0]);

  const admissionStart = source.indexOf('      const activeStates = ["JOB_STATE_PENDING", "JOB_STATE_RUNNING"];');
  const admissionEnd = source.indexOf('      // Resume orchestrations whose self-chain', admissionStart);
  assert(admissionStart > 0 && admissionEnd > admissionStart, "sweep admission exists");
  async function admission(active, failFirst = false) {
    let submitted = 0;
    const open = [
      ...Array.from({ length: active }, (_, i) => ({ batchName: `batch_running_${i}`,
        state: "JOB_STATE_RUNNING", collected: false })),
      ...Array.from({ length: 25 }, (_, i) => ({ batchName: `batch_failed_${i}`,
        state: "JOB_STATE_FAILED", collected: false, retryRequested: true, retryAttempt: 0 })),
    ];
    const context = {
      open, retriesSubmitted: 0, normState: (s) => s,
      isFinal: (s) => s === "JOB_STATE_FAILED", sweepStart: Date.now(),
      SWEEP_BUDGET_MS: 11 * 60 * 1000,
      inProcess: async (payload) => {
        if (payload.kind === "batch_retry_missing") {
          submitted++;
          return { batchName: `batch_new_${submitted}`, ok: true };
        }
        return failFirst && submitted === 1
          ? { state: "JOB_STATE_FAILED", providerError: "Enqueued token limit reached" }
          : { state: "JOB_STATE_PENDING" };
      },
      db: { collection: () => ({ doc: () => ({ set: async () => {} }) }) },
      guardRef: { set: async () => {} },
      BATCHES_COLL: "batches", batchDocIdFromName: (x) => x,
      console: { warn: () => {} },
    };
    const outcome = await vm.runInNewContext(
      `(async () => { ${source.slice(admissionStart, admissionEnd)} return { activeCount, retriesSubmitted }; })()`, context);
    return { submitted, outcome };
  }
  assert.equal((await admission(10)).submitted, 20, "fills twenty open places");
  assert.equal((await admission(28)).submitted, 2, "stops at thirty active jobs");
  assert.equal((await admission(10, true)).submitted, 1, "stops on provider refusal");

  const dedupeStart = source.indexOf('      const submitSessionId = String(body?.sessionId || "");');
  const dedupeEnd = source.indexOf('      // Step A: Run all "copy" tasks', dedupeStart);
  assert(dedupeStart > 0 && dedupeEnd > dedupeStart, "idempotent original submission exists");
  async function dedupe(path) {
    const old = { sessionId: "sess_live", batchName: "batch_existing",
      sets: [{ outputBasePath: base }], routes: Array(6).fill({}) };
    return vm.runInNewContext(`(async () => { ${source.slice(dedupeStart, dedupeEnd)} return { ok: false, newSubmission: true }; })()`, {
      body: { sessionId: "sess_live", displayName: "part254of300" },
      displayName: "part254of300", sets: [{ outputBasePath: path }],
      getDb: () => ({ collection: () => ({ where: () => ({ limit: () => ({
        get: async () => ({ docs: [{ id: "old-id", data: () => old }] }),
      }) }) }) }), BATCHES_COLL: "batches",
      json: (statusCode, data) => ({ statusCode, ...data }),
    });
  }
  const reused = await dedupe(base);
  assert.equal(reused.batchName, "batch_existing", "reuses a persisted identical provider job");
  assert.equal(reused.alreadySubmitted, true);
  const unrelated = await dedupe(base + "_different");
  assert.equal(unrelated.newSubmission, true, "does not reuse a job for a different set");
  console.log("Listing batch recovery: 12 scenarios passed");
})().catch((err) => { console.error(err); process.exitCode = 1; });
