"use strict";

// A set that has used its five retry attempts is never submitted again, but
// it used to stay flagged for retry: the dashboard showed it "queued" forever
// and the page kept polling for it. It must be marked stopped with its
// reason (as the 12-refusal stop is), never re-queued, shown as stopped, and
// the page's automatic polling must end for it.
// No network: every provider and Firestore call is a stub.
// Usage: node tests/listing-batch/attempt-limit.cjs [background.js] [page.html]

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
const render = cut(page, "    function _renderSessionBlock(session) {", "    async function _cancelSession(sessionId) {");
const _awaitingStallRestart = vm.runInNewContext(`${cut(page, "    function _awaitingStallRestart(b) {", "    function _formatDuration(ms) {")}; _awaitingStallRestart`, {});
const poll = cut(page, "    function _startBatchAutoPoll() {", '    document.getElementById("batchJobsRefreshBtn")');
const REFUSED = "Enqueued token limit reached for gpt-image in organization org-x. Limit: 1,000,000 enqueued tokens.";
const EXPIRED = "Batch expired before all requests completed.";

async function sweep(jobs, retryAnswers = {}) {
  const asked = [], writes = [];
  const db = { collection: (name) => {
    if (name === "LG1_Config") return { doc: () => ({ get: async () => ({ exists: false }), set: async () => {} }) };
    if (name === "orchestrations") return { where: () => ({ limit: () => ({ get: async () => ({ forEach: () => {} }) }) }) };
    return {
      // A full refill queue: the collector recounts 30 active jobs.
      orderBy: () => ({ limit: () => ({ get: async () => ({ docs: [] }) }) }),
      where: (field) => field === "repairPending" ? { limit: () => ({ get: async () => ({ forEach: () => {} }) }) } : field === "state"
        ? { get: async () => ({ docs: Array.from({ length: 30 }, () => ({ data: () => ({ collected: false }) })) }) }
        : { orderBy: () => ({ limit: () => ({ get: async () => ({ size: jobs.length,
          forEach: (fn) => jobs.forEach((job) => fn({ data: () => ({ ...job }) })) }) }) }) },
      doc: (id) => ({ set: async (value) => writes.push({ id, value }) }),
    };
  } };
  const handler = async (event) => {
    const payload = JSON.parse(event.body);
    if (payload.kind === "batch_retry_missing") {
      asked.push(payload.batchName);
      return { body: JSON.stringify(retryAnswers[payload.batchName] || { ok: true, complete: true }) };
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

async function retryOnce(record) {
  const submitted = [];
  const ref = { get: async () => ({ exists: true, data: () => record }), set: async (data) => Object.assign(record, data) };
  const db = { collection: () => ({ doc: () => ref }),
    runTransaction: async (fn) => fn({ get: ref.get, set: (_r, data) => Object.assign(record, data) }) };
  const result = await vm.runInNewContext(`(async () => { ${retryBranch} })()`, {
    ...lib, ...require("../../netlify/functions/lib/listingBatchRecovery.cjs"), kind: "batch_retry_missing", body: { batchName: record.batchName },
    BATCHES_COLL: "batches", batchDocIdFromName: (n) => n, getDb: () => db, batchApiKey: () => "key",
    getGeminiBatchJob: async () => { throw new Error("no provider call expected"); },
    batchFailureDetails: () => EXPIRED, assertAllowedOutputBase: () => {},
    admin: { storage: () => { throw new Error("no storage call expected"); },
      firestore: { FieldValue: { serverTimestamp: () => ({ toMillis: () => 0 }) } } },
    module: { exports: { handler: async (event) => { submitted.push(JSON.parse(event.body)); return { statusCode: 500, body: "{}" }; } } },
    json: (statusCode, value) => ({ statusCode, ...value }),
  });
  return { result, submitted };
}

(async () => {
  // The fifth retry failed. It inherited retryRequested, so it waits for a
  // sixth attempt the collector will never make.
  const stuck = { batchName: "batch_fifth", state: "JOB_STATE_FAILED", providerError: EXPIRED, collected: false,
    retryRequested: true, retryAttempt: 5, retryOf: "batch_fourth", sets: [{ setKind: null }], createdAt: null };
  const next = { batchName: "batch_next", state: "JOB_STATE_FAILED", providerError: REFUSED, collected: false,
    retryRequested: true, retryAttempt: 0, sets: [{ setKind: null }] };
  // Already stopped earlier, and refused at the token limit on its last try:
  // the collector must not flag it for retry again.
  const stopped = { batchName: "batch_stopped", state: "JOB_STATE_FAILED", providerError: REFUSED, collected: false,
    retryRequested: false, retryAttempt: 5, retryStatus: "attempts_exhausted", sets: [{ setKind: null }] };

  const running = Array.from({ length: 30 }, (_, i) => ({ batchName: `batch_run_${i}`, state: "JOB_STATE_RUNNING",
    collected: false, sets: [{ setKind: null }] }));
  const run = await sweep([stuck, next, stopped, ...running]);
  assert.equal(run.result.statusCode, 200);
  assert(!run.asked.includes("batch_fifth"), "no sixth attempt");
  const mark = run.writes.find((w) => w.id === "batch_fifth" && w.value.retryRequested === false);
  assert(mark, "the collector takes the set out of the retry queue even while the queue is full");
  assert.equal(mark.value.retryStatus, "attempts_exhausted");
  assert.match(mark.value.retryError, /five retry attempts/i, "the stop gives its reason");
  assert(!run.writes.some((w) => w.id === "batch_stopped" && w.value.retryRequested), "a stopped set is not re-queued");

  // Refill with room: the stopped sets are skipped and the queue carries on.
  const roomy = await sweep([{ ...stuck, retryRequested: false, retryStatus: "attempts_exhausted" }, next, stopped]);
  assert.deepEqual(roomy.asked.filter((n) => n !== "batch_next"), [], "stopped sets are not retried");

  // Asked directly, the retry is refused and the record is marked the same way.
  const direct = { ...stuck, batchName: "batch_direct", sets: [{ category: "Beady_Necklace", setN: 7, outputBasePath: "x", tasks: [] }] };
  const { result, submitted } = await retryOnce(direct);
  assert.equal(result.statusCode, 409);
  assert.equal(result.attemptsExhausted, true);
  assert.equal(submitted.length, 0, "nothing submitted");
  assert.equal(direct.retryRequested, false);
  assert.equal(direct.retryStatus, "attempts_exhausted");
  assert.match(direct.retryError, /five retry attempts/i);

  // What batch_list then returns for the set.
  const listed = { ...stuck, ...mark.value, displayName: "lg1-Beady_Necklace-1sets-x-part1of1", setsCount: 1 };

  // The dashboard names the reason and offers no retry that cannot run.
  const html = vm.runInNewContext(`${render}; _renderSessionBlock({ sessionId: "sess_1", batches, earliest: Date.now(), latest: Date.now() })`, {
    batches: [listed], Date, _batchRetryLimit: 30, _batchSweepInfo: null, _batchNextSweepAt: null,
    _normBatchState: (x) => x, _awaitingStallRestart, _batchSafeText: (x) => String(x), _formatDuration: () => "1m",
    normalizeImageModelId: (x) => x, getImageModelConfig: () => ({ label: "Sunburst" }), DEFAULT_IMAGE_MODEL: "m",
  });
  const summary = html.slice(0, html.indexOf('<details class="batch-details"'));
  assert(!/queued|In progress/i.test(summary), "not shown as queued");
  assert.match(summary, /Stopped: retry limit/);
  assert.match(summary, /1 set\(s\) stopped after five retry attempts/);
  assert.match(html, /five retry attempts/i, "job detail shows the stop reason");
  assert(!html.includes("data-session-retry"), "no Queue missing images button for a stopped set");

  // The page's automatic polling ends: nothing is left open.
  let cleared = false, tick = null;
  vm.runInNewContext(`let _batchPollTimer = null; ${poll}; _startBatchAutoPoll();`, {
    _lastBatchList: [listed], _batchPollInFlight: false, BATCH_POLL_MS: 60000, _normBatchState: (x) => x, _awaitingStallRestart,
    setInterval: (fn) => { tick = fn; return 7; }, clearInterval: (id) => { cleared = id === 7; },
    _checkBatchJobsNow: () => { throw new Error("polled a stopped set"); },
    document: { getElementById: () => ({ style: { display: "" } }) },
  });
  tick();
  assert(cleared, "polling stops");
  console.log("Listing batch: a set at the five-attempt limit is marked stopped with its reason, never re-queued, shown stopped, and polling ends");
})().catch((err) => { console.error(err); process.exitCode = 1; });
