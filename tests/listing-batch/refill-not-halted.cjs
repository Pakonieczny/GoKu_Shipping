"use strict";

// The refill of the batch sweep must go past a set whose submission failed for a reason of its own.
// batch_submit answers { ok, queued, sourceError } when preparation failed for THAT set (an unreadable
// reference, a refused job): admission has already recorded the wait or the stop on the set. For a set
// repaired from a collected job (the 2026-10-24 input-image repairs), batch_retry_missing turned that
// answer into a plain { queued: true }: it rewrote the record as retryRequested/"queued" (undoing a stop
// on every run), and the sweep reads a plain "queued" as "admission is busy" and stops the whole refill,
// so the sets behind the broken one were never sent.
// Checked: batch_retry_missing passes sourceError through and leaves the stop recorded, and the sweep's
// refill (the real code of both, run under node:vm) skips that set and submits the next ones.
// Usage: node tests/listing-batch/refill-not-halted.cjs   (from the repository root)

const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const vm = require("node:vm");

const source = fs.readFileSync(path.resolve("netlify/functions/geminiImageProxy-background.js"), "utf8");
const admissionLib = require(path.resolve("netlify/functions/lib/listingBatchAdmission.cjs"));
const recoveryLib = require(path.resolve("netlify/functions/lib/listingBatchRecovery.cjs"));
const slice = (from, to) => {
  const start = source.indexOf(from), end = source.indexOf(to, start);
  assert(start > 0 && end > start, `${from.trim()} exists`);
  return source.slice(start, end);
};
const retryBranch = slice('    if (kind === "batch_retry_missing") {', '    if (kind === "batch_stall_cancel") {');
const refill = slice('      const activeStates = ["JOB_STATE_PENDING", "JOB_STATE_RUNNING"];', "      // Resume orchestrations whose self-chain");

const INVALID = "Invalid input image file. Please check the uploaded image.";
const base = (n) => `listing-generator-1/Regular_Necklace/Ready_To_List/Set_${n}`;
const tasks = (n) => [0, 1, 2, 3, 4, 5, 6].map((slotIndex) => slotIndex === 5
  ? { slotIndex, type: "copy", source_storage_path: "listing-generator-1/Regular_Necklace/Size_Guide.png" }
  : { slotIndex, type: "edits", prompt: "Preserve the charm exactly.",
    input_storage_path: `listing-generator-1/Regular_Necklace/Templates/Template_${slotIndex}.png`,
    input_charm_storage_path: `listing-generator-1/Charms/New_Charms/Charm_${n}.png` });
// A collected OpenAI job whose six images were refused as unreadable input, queued for its missing images.
const repairRecord = (n) => ({ batchName: `batch_6add${n}5cac8190972348164ba767bc`, sessionId: "sess_1792773258456_men4v0",
  state: "JOB_STATE_SUCCEEDED", collected: true, repairPending: true, retryRequested: true, recoveryStatus: "queued",
  setComplete: false, retryAttempt: 0, model: "gpt-image-2.5-sunburst", imageSize: "2K", responsesFile: `file-errors-${n}`,
  results: { failedCount: 6, failures: [0, 1, 2, 3, 4, 6].map((slot) => ({ key: `s0_slot${slot}`, error: INVALID })) },
  sets: [{ category: "Regular_Necklace", setN: n, outputBasePath: base(n), tasks: tasks(n) }] });
// What admission writes on a set whose reference is not an image (preparationFailurePatch, permanent).
const STOP = "Charm image \"Charm_1930.png\" is a web page (HTML), not an image (1840 bytes, labelled image/png). Replace the file with a PNG or JPEG.";
const stopPatch = { retryStatus: "preparation_failed", retryRequested: false, repairPending: false, recoveryStatus: "blocked",
  recoveryReason: `Listing stopped before submission: ${STOP}`, retryError: STOP, preparationFailures: 1, retryNotBefore: null };

// batch_retry_missing, the real branch, over one record per document; `submit` stands for batch_submit.
function retryWorld(records, submit) {
  const submitted = [];
  const refOf = (name) => ({
    get: async () => ({ exists: !!records[name], data: () => records[name] }),
    set: async (data) => { Object.assign(records[name], data); },
  });
  const db = {
    collection: () => ({ doc: (name) => ({ ...refOf(name), _name: name }) }),
    runTransaction: async (fn) => fn({ get: (ref) => ref.get(), set: (ref, data) => { Object.assign(records[ref._name], data); } }),
  };
  const bucket = {
    getFiles: async ({ prefix }) => [/(Completed|Approved)_Listing_Sets/.test(prefix) ? []
      : [{ name: `${prefix}Slot_6.png` }]],          // only the copied size guide is there
    file: () => ({ exists: async () => [false], copy: async () => {} }),
  };
  const run = (batchName) => vm.runInNewContext(`(async () => { ${retryBranch} })()`, {
    ...admissionLib, ...recoveryLib,
    kind: "batch_retry_missing", body: { batchName },
    BATCHES_COLL: "batches", batchDocIdFromName: (n) => n,
    getDb: () => db, batchApiKey: () => "key",
    getGeminiBatchJob: async () => ({ state: "JOB_STATE_SUCCEEDED", response: { responsesFile: records[batchName].responsesFile } }),
    batchFailureDetails: () => null,
    assertAllowedOutputBase: () => {},
    admin: { storage: () => ({ bucket: () => bucket }),
      firestore: { FieldValue: { serverTimestamp: () => ({ toMillis: () => Date.now() }) } } },
    module: { exports: { handler: async (event) => {
      const payload = JSON.parse(event.body);
      submitted.push(payload);
      return submit(payload);
    } } },
    json: (statusCode, value) => ({ statusCode, body: JSON.stringify(value) }),
    console: { log: () => {}, warn: () => {} },
  });
  return { run, submitted };
}
// batch_submit as admission answers it: the first set's charm is not an image (its stop is recorded on the
// source record before the answer), every other set gets its job.
const submitFor = (records, broken) => async (payload) => {
  if (payload.retryOf === broken) {
    Object.assign(records[broken], stopPatch);
    return { statusCode: 200, body: JSON.stringify({ ok: true, queued: true, batchName: broken, state: "JOB_STATE_QUEUED",
      sourceError: true, reason: "Submission saved; waiting for the next server check" }) };
  }
  return { statusCode: 200, body: JSON.stringify({ ok: true, batchName: `batch_new_for_${payload.retryOf}` }) };
};

(async () => {
  // (1) batch_retry_missing passes the set's own failure through.
  {
    const records = { [repairRecord(1930).batchName]: repairRecord(1930) };
    const name = repairRecord(1930).batchName;
    const world = retryWorld(records, submitFor(records, name));
    const res = await world.run(name);
    const answer = JSON.parse(res.body);
    assert.equal(world.submitted.length, 1, "the missing images were submitted once");
    assert.deepEqual(world.submitted[0].sets[0].tasks.map((t) => t.slotIndex), [0, 1, 2, 3, 4, 6]);
    assert.equal(world.submitted[0].inputRepairAttempt, 1, "a resend after an input-image rejection is counted");
    assert.equal(res.statusCode, 200);
    assert.equal(answer.queued, true);
    assert.equal(answer.sourceError, true, `the answer says the failure is this set's own (got ${res.body})`);
    assert.equal(records[name].retryStatus, "preparation_failed", "the stop admission recorded is kept (it was rewritten as \"queued\")");
    assert.equal(records[name].retryRequested, false, "and the set is not queued again by it");
    assert.equal(records[name].recoveryStatus, "blocked");
    assert.equal(records[name].retryStartedAt, null, "the submit claim is given back");
  }

  // (2) the sweep's refill skips that set and submits the ones behind it.
  {
    const order = [1930, 1931, 1932, 1933];
    const records = Object.fromEntries(order.map((n) => [repairRecord(n).batchName, repairRecord(n)]));
    const broken = repairRecord(1930).batchName;
    const world = retryWorld(records, submitFor(records, broken));
    const calls = [];
    const open = order.map((n) => {
      const r = repairRecord(n);
      return { batchName: r.batchName, state: r.state, collected: true, repairPending: true, retryRequested: true,
        retryAttempt: 0, setComplete: false, sessionId: r.sessionId, sets: r.sets.map((s) => ({ outputBasePath: s.outputBasePath, setKind: null })) };
    });
    const context = {
      open, retriesSubmitted: 0, normState: (s) => String(s || "").replace(/^BATCH_STATE_/, "JOB_STATE_"),
      isFinal: (s) => ["JOB_STATE_FAILED", "JOB_STATE_CANCELLED", "JOB_STATE_EXPIRED"].includes(s), sweepStart: Date.now(),
      isSucceeded: (s) => s === "JOB_STATE_SUCCEEDED",
      checkValidation: async () => ({ ok: true, state: "JOB_STATE_RUNNING" }),
      SWEEP_BUDGET_MS: 11 * 60 * 1000,
      inProcess: async (payload) => {
        calls.push(payload);
        if (payload.kind !== "batch_retry_missing") throw new Error("unexpected " + payload.kind);
        return JSON.parse((await world.run(payload.batchName)).body);
      },
      db: { collection: () => ({ doc: () => ({ set: async () => {} }),
        where: () => ({ select: () => ({ get: async () => ({ docs: [] }) }), get: async () => ({ docs: [] }) }) }),
        runTransaction: async () => false },
      admin: { firestore: { FieldValue: { serverTimestamp: () => 1 } } },
      guardRef: { set: async () => {} }, guardWrite: async () => true, superseded: false,
      checkpoint: async () => {},
      touch: () => {}, openRefs: new Map(), lightFields: [], compact: (r) => r,
      BATCHES_COLL: "batches", batchDocIdFromName: (x) => x,
      stallRestartPending: admissionLib.stallRestartPending, quotaFailure: admissionLib.quotaFailure,
      console: { warn: () => {}, log: () => {} },
    };
    const outcome = await vm.runInNewContext(
      `(async () => { ${refill} return { activeCount, retriesSubmitted, sourceErrors, waiting: waiting.length }; })()`, context);
    assert.equal(outcome.waiting, 4, "all four sets wait for the refill");
    assert.deepEqual(calls.map((c) => c.batchName), order.map((n) => repairRecord(n).batchName),
      "every waiting set is offered to batch_retry_missing, in order");
    assert.equal(outcome.sourceErrors, 1, "the broken set counts as a source error");
    assert.equal(outcome.retriesSubmitted, 3, `the three sets behind it are submitted (got ${outcome.retriesSubmitted}; the refill used to stop at the first)`);
    for (const n of order.slice(1)) assert.equal(records[repairRecord(n).batchName].retryBatchName, `batch_new_for_${repairRecord(n).batchName}`);
    assert.equal(records[broken].retryStatus, "preparation_failed", "and the broken set keeps its stop");
    assert.equal(outcome.activeCount, 3);
  }
  console.log("Listing batch refill: a set's own submission failure is passed through (sourceError) and the refill submits the sets behind it passed");
})().catch((err) => { console.error(err); process.exitCode = 1; });
