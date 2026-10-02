"use strict";

// Adversarial (area 19): cancelling listing batches and the refill queue.
// - A cancelled job never holds up the queue refill, and is never retried.
// - A Charm Maker (or multi-set) job refused at the token limit does not
//   block the refill for the listing sets queued behind it.
// - batch_cancel takes a job out of the retry queue; a failed job waiting
//   for a retry is taken out without asking the provider.
// - "Cancel all running" also stops the submission's queued retries, and
//   shows its progress while it works.
// No network: every provider and Firestore call is a stub.

const assert = require("node:assert/strict");
const fs = require("node:fs");
const vm = require("node:vm");
const { quotaFailure, neverStarted, stallRestartPending, stallCutoffMs, SWEEP_CALL_LIMITS, withLimit } = require("../../netlify/functions/lib/listingBatchAdmission.cjs");

const server = fs.readFileSync("netlify/functions/geminiImageProxy-background.js", "utf8");
const page = fs.readFileSync("Listing_Generator_1.html", "utf8");
const slice = (src, from, to) => {
  const a = src.indexOf(from), b = src.indexOf(to, a + 1);
  assert(a > 0 && b > a, `found ${from.trim().slice(0, 40)}`);
  return src.slice(a, b);
};
const sweepSrc = slice(server, '  if (kind === "batch_sweep") {', '  if (kind === "job_status") {');
const cancelSrc = slice(server, '    if (kind === "batch_cancel") {\n      const apiKey', "    // Paginated scan and one collected batch");
const cancelSessionSrc = slice(page, "    async function _cancelSession(sessionId) {", "    // Recover (force-collect partial results)");
const renderSrc = slice(page, "    function _renderSessionBlock(session) {", "    // Cancel every still-running batch in a session.");
const _awaitingStallRestart = vm.runInNewContext(`${slice(page, "    function _awaitingStallRestart(b) {", "    function _formatDuration(ms) {")}; _awaitingStallRestart`, {});

const one = (extra = {}) => [{ category: "Beady_Necklace", setN: 1, outputBasePath: "x", ...extra }];

async function runSweep(jobs, statuses = {}) {
  let clock = 1000000;
  class Clock extends Date { static now() { return clock; } }
  const retries = [], submitted = [];
  const db = { collection: (name) => {
    if (name === "LG1_Config") return { doc: () => ({ get: async () => ({ exists: false }), set: async () => {} }) };
    if (name === "orchestrations") return { where: () => ({ limit: () => ({ get: async () => ({ forEach: () => {} }) }) }) };
    return {
      orderBy: () => ({ limit: () => ({ select: () => ({ get: async () => ({ docs: [] }) }) }) }),
      where: (field) => field === "repairPending" ? { limit: () => ({ select: () => ({ get: async () => ({ forEach: () => {} }) }) }) } : field === "state" ? { get: async () => ({ docs: [] }) } : ({ orderBy: () => ({ limit: () => ({ select: () => ({
        get: async () => ({ size: jobs.length, forEach: (fn) => jobs.forEach((j) => fn({ data: () => ({ ...j }) })) }),
      }) }) }) }),
      doc: () => ({ set: async () => {} }),
    };
  } };
  const handler = async (event) => {
    const p = JSON.parse(event.body);
    if (p.kind === "batch_status") return { body: JSON.stringify(statuses[p.batchName] || { ok: true, state: "JOB_STATE_RUNNING" }) };
    if (p.kind === "batch_retry_missing") {
      // The real handler's refusals (a 409 answer) for these records.
      retries.push(p.batchName);
      const job = jobs.find((j) => j.batchName === p.batchName);
      const st = statuses[p.batchName]?.state || job.state;
      if (!["JOB_STATE_FAILED", "JOB_STATE_EXPIRED", "JOB_STATE_QUEUED"].includes(st))
        return { body: JSON.stringify({ error: { message: `Original job is ${st}; wait for collection before retrying` } }) };
      if (job.sets?.length !== 1 || job.sets[0].setKind === "charm_maker")
        return { body: JSON.stringify({ error: { message: "Selective retry supports one listing set per batch" } }) };
      submitted.push(p.batchName);
      return { body: JSON.stringify({ ok: true, batchName: `batch_new_${submitted.length}` }) };
    }
    throw new Error(`unexpected ${p.kind}`);
  };
  const result = await vm.runInNewContext(`(async () => { ${sweepSrc} })()`, {
    kind: "batch_sweep", body: {}, getDb: () => db, admissionControl: () => ({ reconcile: async () => {} }), quotaFailure, neverStarted, stallRestartPending, stallCutoffMs, SWEEP_CALL_LIMITS, withLimit,
    BATCHES_COLL: "batches", ORCH_COLL: "orchestrations",
    admin: { firestore: { FieldPath: { documentId: () => "__name__" }, FieldValue: { serverTimestamp: () => clock } } },
    module: { exports: { handler } }, json: (statusCode, body) => ({ statusCode, ...body }),
    console: { warn: () => {}, error: () => {} }, process: { env: {} }, Date: Clock,
    setTimeout: (fn, ms) => { clock += ms; fn(); }, fetch: async () => ({ ok: true }),
    batchDocIdFromName: (x) => x, safeErr: (e) => ({ message: e.message }),
  });
  return { result, retries, submitted };
}

async function runCancel(doc, gate = {}) {
  const writes = [], providerCalls = [];
  const ref = { get: async () => ({ exists: !!doc, data: () => doc }), set: async (v) => { writes.push(v); Object.assign(doc, v); } };
  const gateRef = { get: async () => ({ exists: true, data: () => gate }) };
  const db = {
    collection: (name) => ({ doc: () => (name === "LG1_Config" ? gateRef : ref) }),
    runTransaction: async (fn) => fn({ get: (r) => r.get(), set: (r, v) => { writes.push(v); Object.assign(doc, v); } }),
  };
  const res = await vm.runInNewContext(`(async () => { ${cancelSrc} })()`, {
    kind: "batch_cancel", body: { batchName: doc.batchName }, batchApiKey: () => "k", getDb: () => db,
    cancelGeminiBatchJob: async (_k, name) => { providerCalls.push(name); return { state: "JOB_STATE_RUNNING", providerStatus: "cancelling" }; },
    firestoreRetry: (fn) => fn(), batchDocIdFromName: (x) => x, BATCHES_COLL: "batches",
    admin: { firestore: { FieldValue: { serverTimestamp: () => 1 } } },
    json: (statusCode, body) => ({ statusCode, ...body }),
  });
  return { res, writes, providerCalls, doc };
}

(async () => {
  // 1. A retry job the user cancelled keeps retryRequested from its parent.
  //    It sits first in the oldest-first queue; the real retry handler refuses
  //    it, and the sweep must still refill the waiting set behind it.
  const cancelled = await runSweep([
    { batchName: "batch_cancelled_child", state: "JOB_STATE_CANCELLED", collected: false, retryRequested: true,
      retryOf: "batch_a", sets: one(), createdAt: { toMillis: () => 1 } },
    { batchName: "batch_failed_b", state: "JOB_STATE_FAILED", collected: false, retryRequested: true,
      sets: one(), createdAt: { toMillis: () => 2 } },
  ]);
  assert.equal(cancelled.result.statusCode, 200);
  assert(!cancelled.retries.includes("batch_cancelled_child"), "a cancelled job is never sent to retry");
  assert.deepEqual(cancelled.submitted, ["batch_failed_b"], "a cancelled job does not block the queue refill");

  // 2. A Charm Maker job refused at the token limit during this sweep is not
  //    eligible for selective retry; it must not stop the listing refill.
  const charm = await runSweep([
    { batchName: "batch_charm", state: "JOB_STATE_RUNNING", collected: false,
      sets: one({ setKind: "charm_maker" }), createdAt: { toMillis: () => 1 } },
    { batchName: "batch_failed_c", state: "JOB_STATE_FAILED", collected: false, retryRequested: true,
      sets: one(), createdAt: { toMillis: () => 2 } },
  ], { batch_charm: { ok: true, state: "JOB_STATE_FAILED", providerError: "Enqueued token limit reached" } });
  assert(!charm.retries.includes("batch_charm"), "an ineligible refused job is not sent to retry");
  assert.deepEqual(charm.submitted, ["batch_failed_c"], "an ineligible refused job does not block the refill");

  // 3. Cancelling a running job takes it out of the retry queue.
  const running = await runCancel({ batchName: "batch_run", state: "JOB_STATE_RUNNING", retryRequested: true });
  assert.equal(running.res.statusCode, 200);
  assert.deepEqual(running.providerCalls, ["batch_run"]);
  assert.equal(running.doc.retryRequested, false, "a cancelled job is no longer queued for retry");

  // 4. A failed job waiting for its retry is taken out of the queue without a
  //    provider call (the provider cannot cancel a finished job).
  const waiting = await runCancel({ batchName: "batch_wait", state: "JOB_STATE_FAILED", retryRequested: true });
  assert.equal(waiting.res.statusCode, 200);
  assert.deepEqual(waiting.providerCalls, [], "no provider call for a finished job");
  assert.equal(waiting.doc.retryRequested, false);
  assert.equal(waiting.doc.state, "JOB_STATE_FAILED", "its state and saved images are kept");
  // ... unless its retry is being submitted right now.
  const busy = await runCancel({ batchName: "batch_busy", state: "JOB_STATE_FAILED", retryRequested: true },
    { owner: "t", sourceName: "batch_busy" });
  assert.equal(busy.res.statusCode, 409);
  assert.equal(busy.doc.retryRequested, true);

  // 5. "Cancel all running" also stops the submission's queued retries, and
  //    shows its progress.
  const list = [
    { sessionId: "s", batchName: "batch_r1", state: "JOB_STATE_RUNNING" },
    { sessionId: "s", batchName: "batch_local_q", state: "JOB_STATE_QUEUED", locallyQueued: true, retryRequested: true },
    { sessionId: "s", batchName: "batch_f1", state: "JOB_STATE_FAILED", retryRequested: true },
    { sessionId: "s", batchName: "batch_f2", state: "JOB_STATE_FAILED" },
    { sessionId: "s", batchName: "batch_done", state: "JOB_STATE_FAILED", retryRequested: true, retryBatchName: "batch_r1" },
    { sessionId: "other", batchName: "batch_x", state: "JOB_STATE_FAILED", retryRequested: true },
  ];
  const calls = [], texts = [];
  const status = { set textContent(v) { texts.push(v); }, get textContent() { return texts[texts.length - 1] || ""; } };
  let confirmText = "";
  await vm.runInNewContext(`(async () => { ${cancelSessionSrc} await _cancelSession("s"); })()`, {
    _lastBatchList: list, _normBatchState: (s) => s, _awaitingStallRestart, confirm: (t) => { confirmText = t; return true; },
    postJson: async (_fn, p) => { calls.push(p.batchName); if (p.batchName === "batch_f1") throw new Error("busy"); return { ok: true }; },
    refreshBatchJobsPanel: async () => ({ ok: true }), console: { warn: () => {} },
    document: { getElementById: (id) => id === "batchRecoveryStatus" ? status : null, querySelectorAll: () => [] },
  });
  assert.deepEqual(calls, ["batch_r1", "batch_local_q", "batch_f1"], "queued retries are cancelled with the running jobs");
  assert.match(confirmText, /3/);
  assert(texts.some((t) => /1\/3|2\/3/.test(t)), "cancellation shows its progress");
  assert.match(status.textContent, /1 could not be cancelled/, "a failed cancel is reported, not hidden");

  // The button counts the same jobs.
  const html = vm.runInNewContext(`${renderSrc}; _renderSessionBlock(session)`, {
    session: { sessionId: "s", batches: list.filter((b) => b.sessionId === "s"), earliest: 1, latest: 2 },
    _normBatchState: (s) => s, _awaitingStallRestart, _batchSafeText: (s) => String(s), _formatDuration: () => "1s",
    _batchSweepInfo: null, _batchRetryLimit: 30, _batchNextSweepAt: null,
    normalizeImageModelId: (x) => x, getImageModelConfig: () => ({ label: "m" }), DEFAULT_IMAGE_MODEL: "m",
  });
  assert.match(html, /Cancel all running \(3\)/);

  // 6. One failed poll (every minute while the panel is open) keeps the
  //    progress dashboard on screen and says so in the status line.
  const refreshSrc = slice(page, "    async function refreshBatchJobsPanel() {", "    // Normalize state for both legacy");
  const listEl = { innerHTML: "<section>dashboard</section>" };
  const line = { textContent: "" };
  const shown = await vm.runInNewContext(`(async () => { ${refreshSrc}; return refreshBatchJobsPanel(); })()`, {
    _lastBatchList: [{ batchName: "batch_r1" }], _batchSafeText: (s) => String(s),
    _batchListSeq: 0, _batchPanelSeq: 0, _batchListDataSeq: 0,
    postJson: async () => { throw new Error("Failed to fetch"); },
    document: { getElementById: (id) => id === "batchJobsList" ? listEl : id === "batchRefreshStatus" ? line : null },
  });
  assert.equal(shown.ok, false);
  assert.equal(listEl.innerHTML, "<section>dashboard</section>", "a failed poll keeps the dashboard");
  assert.match(line.textContent, /Could not load saved jobs: Failed to fetch/);

  console.log("Listing batch cancel and queue: cancelled and ineligible jobs never block the refill, cancel empties the retry queue with progress passed");
})().catch((err) => { console.error(err); process.exitCode = 1; });
