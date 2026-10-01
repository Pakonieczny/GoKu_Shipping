"use strict";

// A set OpenAI keeps refusing for its enqueued-token limit must not loop
// forever. Token-limit refusals don't use one of the five retry attempts, so
// they have their own ceiling: after 12 refusals the set stops, is marked
// failed with a clear reason, the collector skips it without stalling the
// rest of the queue, and the dashboard says why.
// Usage: node tests/listing-batch/capacity-refusals.cjs [background.js] [page.html]

const assert = require("node:assert/strict");
const fs = require("node:fs");
const vm = require("node:vm");
const lib = require("../../netlify/functions/lib/listingBatchAdmission.cjs");

const server = fs.readFileSync(process.argv[2] || "netlify/functions/geminiImageProxy-background.js", "utf8");
const page = fs.readFileSync(process.argv[3] || "Listing_Generator_1.html", "utf8");
const cut = (src, from, to) => {
  const start = src.indexOf(from);
  const end = src.indexOf(to, start);
  assert(start > 0 && end > start, `found ${from.trim()}`);
  return src.slice(start, end);
};
const retryBranch = cut(server, '    if (kind === "batch_retry_missing") {', '    if (kind === "batch_status") {');
const sweepBranch = cut(server, '  if (kind === "batch_sweep") {', '  if (kind === "job_status") {');
const REFUSED = "Enqueued token limit reached for gpt-image in organization org-x. Limit: 1,000,000 enqueued tokens.";
const base = "listing-generator-1/Beady_Necklace/Ready_To_List/Set_7";
const tasks = [0, 1].map((slotIndex) => ({ type: "edits", slotIndex, prompt: `p${slotIndex}` }));

// One batch_retry_missing call against `record`, the provider always refusing.
async function retryOnce(record) {
  const submitted = [];
  const ref = {
    get: async () => ({ exists: true, data: () => record }),
    set: async (data) => Object.assign(record, data),
  };
  const db = { collection: () => ({ doc: () => ref }),
    runTransaction: async (fn) => fn({ get: ref.get, set: (_r, data) => Object.assign(record, data) }) };
  const bucket = { getFiles: async () => [[]], file: () => ({ exists: async () => [false] }) };
  const result = await vm.runInNewContext(`(async () => { ${retryBranch} })()`, {
    ...lib, ...require("../../netlify/functions/lib/listingBatchRecovery.cjs"), kind: "batch_retry_missing", body: { batchName: record.batchName },
    BATCHES_COLL: "batches", batchDocIdFromName: (n) => n, getDb: () => db, batchApiKey: () => "key",
    getGeminiBatchJob: async () => ({ state: "JOB_STATE_FAILED" }), batchFailureDetails: () => REFUSED,
    assertAllowedOutputBase: () => {},
    admin: { storage: () => ({ bucket: () => bucket }),
      firestore: { FieldValue: { serverTimestamp: () => ({ toMillis: () => 0 }) } } },
    module: { exports: { handler: async (event) => {
      submitted.push(JSON.parse(event.body));
      return { statusCode: 200, body: JSON.stringify({ ok: true, batchName: `batch_child_${submitted.length}` }) };
    } } },
    json: (statusCode, value) => ({ statusCode, ...value }),
  });
  return { result, submitted: submitted[0] };
}

async function sweep(jobs, retryAnswers) {
  const asked = [], writes = [];
  const db = { collection: (name) => {
    if (name === "LG1_Config") return { doc: () => ({ get: async () => ({ exists: false }), set: async () => {} }) };
    if (name === "orchestrations") return { where: () => ({ limit: () => ({ get: async () => ({ forEach: () => {} }) }) }) };
    return {
      orderBy: () => ({ limit: () => ({ select: () => ({ get: async () => ({ docs: [] }) }) }) }),
      where: (field) => field === "repairPending" ? { limit: () => ({ select: () => ({ get: async () => ({ forEach: () => {} }) }) }) } : ({ orderBy: () => ({ limit: () => ({ select: () => ({ get: async () => ({ size: jobs.length,
        forEach: (fn) => jobs.forEach((job) => fn({ data: () => ({ ...job }) })) }) }) }) }) }),
      doc: (id) => ({ set: async (value) => writes.push({ id, value }) }),
    };
  } };
  const handler = async (event) => {
    const payload = JSON.parse(event.body);
    if (payload.kind === "batch_retry_missing") {
      asked.push(payload.batchName);
      return { body: JSON.stringify(retryAnswers[payload.batchName]) };
    }
    if (payload.kind === "batch_status") return { body: JSON.stringify({ ok: true, state: "JOB_STATE_RUNNING" }) };
    throw new Error(`unexpected ${payload.kind}`);
  };
  const result = await vm.runInNewContext(`(async () => { ${sweepBranch} })()`, {
    ...lib, ...require("../../netlify/functions/lib/listingBatchRecovery.cjs"), kind: "batch_sweep", body: {}, getDb: () => db, admissionControl: () => ({ reconcile: async () => {} }),
    BATCHES_COLL: "batches", ORCH_COLL: "orchestrations",
    admin: { firestore: { FieldPath: { documentId: () => "__name__" }, FieldValue: { serverTimestamp: () => 1 } } },
    module: { exports: { handler } }, json: (statusCode, body) => ({ statusCode, ...body }),
    console: { warn: () => {}, error: () => {} }, process: { env: {} }, Date,
    setTimeout: (fn) => fn(), fetch: async () => ({ ok: true }), batchDocIdFromName: (x) => x,
    safeErr: (err) => ({ message: err.message }),
  });
  return { result, asked, writes };
}

(async () => {
  // Follow the chain: each retry is refused again, and its refusal count is
  // carried to the next job. It must end, at 12 refusals, not loop.
  let record = { batchName: "batch_original", state: "JOB_STATE_FAILED", providerError: REFUSED,
    retryRequested: true, retryAttempt: 0, collected: false, model: "m", imageSize: "2K", sessionId: "sess_1",
    sets: [{ category: "Beady_Necklace", setN: 7, outputBasePath: base, tasks }] };
  let retries = 0, stop = null;
  for (let hop = 0; hop < 40 && !stop; hop++) {
    const { result, submitted } = await retryOnce(record);
    if (result.statusCode !== 200) { stop = { result, record }; break; }
    retries++;
    assert.equal(submitted.retryAttempt, 0, "a token-limit refusal does not use one of the five attempts");
    record = { ...record, batchName: result.batchName, retryOf: record.batchName, retryBatchName: undefined,
      retryAttempt: submitted.retryAttempt, capacityRefusals: submitted.capacityRefusals, providerError: REFUSED };
  }
  assert(stop, "a set refused forever stops retrying");
  assert.equal(retries, 11, "11 retries follow the first refusal; the 12th refusal stops it");
  assert.equal(stop.result.statusCode, 409);
  assert.equal(stop.result.capacityExhausted, true);
  assert.equal(stop.record.retryRequested, false, "no longer queued");
  assert.equal(stop.record.retryStatus, "capacity_refused");
  assert.match(stop.record.retryError, /Stopped after 12 token-limit refusals/);

  // A refusal at submission (counted on the record) adds to the same ceiling.
  const early = await retryOnce({ ...record, batchName: "batch_other", capacityRefusals: 11, retryStatus: null,
    retryRequested: true });
  assert.equal(early.result.capacityExhausted, true);

  // The collector does not re-queue a stopped set, and a stop does not stall the queue.
  const stopped = { batchName: "batch_stopped", state: "JOB_STATE_FAILED", providerError: REFUSED, collected: false,
    retryRequested: false, retryStatus: "capacity_refused", sets: [{ setKind: null }], createdAt: null };
  const hitsLimit = { batchName: "batch_limit", state: "JOB_STATE_FAILED", providerError: REFUSED, collected: false,
    retryRequested: true, retryAttempt: 0, capacityRefusals: 11, sets: [{ setKind: null }] };
  const next = { batchName: "batch_next", state: "JOB_STATE_FAILED", providerError: REFUSED, collected: false,
    retryRequested: true, retryAttempt: 0, sets: [{ setKind: null }] };
  const run = await sweep([stopped, hitsLimit, next], {
    batch_limit: { capacityExhausted: true, error: { message: "Stopped after 12 token-limit refusals" } },
    batch_next: { ok: true, complete: true },
  });
  assert.equal(run.result.statusCode, 200);
  assert(!run.asked.includes("batch_stopped"), "a stopped set is not retried");
  assert(!run.writes.some((w) => w.id === "batch_stopped" && w.value.retryRequested), "a stopped set is not re-queued");
  assert.deepEqual(run.asked, ["batch_limit", "batch_next"], "the queue carries on past a stopped set");

  // The dashboard names the reason and does not offer a retry that cannot run.
  const render = cut(page, "    function _renderSessionBlock(session) {", "    async function _cancelSession(sessionId) {");
const _awaitingStallRestart = vm.runInNewContext(`${cut(page, "    function _awaitingStallRestart(b) {", "    function _formatDuration(ms) {")}; _awaitingStallRestart`, {});
  const html = vm.runInNewContext(`${render}; _renderSessionBlock({ sessionId: "sess_1", batches, earliest: Date.now(), latest: Date.now() })`, {
    batches: [{ batchName: "batch_limit", displayName: "lg1-Beady_Necklace-1sets-x-part1of1", state: "JOB_STATE_FAILED",
      setsCount: 1, collected: false, retryRequested: false, retryStatus: "capacity_refused",
      providerError: REFUSED, retryError: "Stopped after 12 token-limit refusals from OpenAI; this set will not retry automatically." }],
    Date, _batchRetryLimit: 30, _batchSweepInfo: null, _batchNextSweepAt: null,
    _normBatchState: (x) => x, _awaitingStallRestart, _batchSafeText: (x) => String(x), _formatDuration: () => "1m",
    normalizeImageModelId: (x) => x, getImageModelConfig: () => ({ label: "Sunburst" }), DEFAULT_IMAGE_MODEL: "m",
  });
  const summary = html.slice(0, html.indexOf('<details class="batch-details"'));
  assert.match(summary, /Completed with 1 issue/);
  assert.match(summary, /1 set\(s\) stopped after repeated token-limit refusals/);
  assert.match(html, /Stopped after 12 token-limit refusals from OpenAI/, "job detail shows the stop reason");
  assert(!html.includes("data-session-retry"), "no Queue missing images button for a stopped set");
  console.log("Listing batch: token-limit refusals stop at 12, collector skips stopped sets, dashboard says why");
})().catch((err) => { console.error(err); process.exitCode = 1; });
