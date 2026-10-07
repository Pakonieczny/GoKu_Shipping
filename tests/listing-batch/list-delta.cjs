"use strict";

// Firebase cost (FC12b): the Batch panel's poll reads the newest 1,500 job records every pass (about 30 thousand reads an
// hour for one open tab). Every write to a job record now sets `rev`, batch_list answers "what was written since the read
// you hold" with just those records, and the page merges them into the list on screen.
//
// This runs the REAL server handler (cost meter's in-memory Firestore) and the REAL page code (vm), wired together, and checks
// after every kind of change that the list the page shows after a small answer is exactly the list a whole read gives.
// No network, no provider call: fetch is a stub. Usage: node tests/listing-batch/list-delta.cjs

const assert = require("node:assert/strict");
const fs = require("node:fs");
const vm = require("node:vm");
const meter = require("../cost/meter.cjs");

const m = meter.create();
m.install({ app: () => ({}) });
process.env.OPENAI_API_KEY = "test-key";
const impl = require("../../netlify/functions/geminiImageProxy-background.js");
const page = fs.readFileSync("Listing_Generator_1.html", "utf8");
const cut = (from, to) => {
  const start = page.indexOf(from);
  const end = page.indexOf(to, start);
  assert(start > 0 && end > start, `found ${from.trim()}`);
  return page.slice(start, end);
};
const loader = cut("    async function refreshBatchJobsPanel() {", "    // Normalize state for both legacy");
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const TS = meter.Timestamp;

// ---- fixture: one session of 300 jobs (30 running, 3 waiting in the queue), 1,300 older jobs so the window is truncated ----
const T0 = Date.now() - 6 * 3600 * 1000;
const hex = (n) => String(n).padStart(40, "0");
let tick = 0;
const task = (slot) => ({ type: "edits", slotIndex: slot, input_storage_path: "listing-generator-1/Rings/Primary_Models/m.png", prompt: "p".repeat(3000) });
function record(id, sessionId, n, extra) {
  const set = { category: "Rings", outputBasePath: "listing-generator-1/Rings/Ready_To_List/Set_" + n, setN: n, setKind: null, tasks: [0, 1, 2].map(task) };
  return Object.assign({ batchName: id, docId: id, displayName: "lg1-Rings-300sets-x-part" + n + "of300", sessionId, model: "gpt-image-2", state: "JOB_STATE_SUCCEEDED", collected: true,
    setComplete: true, repairPending: false, collectionPending: false, retryRequested: false, recoveryStatus: "complete", setsCount: 1, requestCount: 3, setKeys: [set.outputBasePath],
    createdAt: TS.fromMillis(T0 + (++tick) * 1000), updatedAt: TS.fromMillis(T0 + tick * 1000 + 400), sets: [set],
    batchStats: { requestCount: 3, successfulRequestCount: 3, failedRequestCount: 0, pendingRequestCount: 0 }, results: { succeededCount: 3, failedCount: 0, failures: [] } }, extra || {});
}
const seed = {};
for (let n = 1; n <= 1300; n++) { const id = "batch_old" + n; const r = record(id, "sess_oooooooo", n); r.createdAt = TS.fromMillis(T0 - 10 * 86400000 + n * 1000); seed["ListingGenerator1Batches/" + id] = r; }
for (let n = 1; n <= 300; n++) {
  const id = "batch_job" + n, running = n > 270;
  seed["ListingGenerator1Batches/" + id] = record(id, "sess_aaaaaaaa", n, running ? { state: "JOB_STATE_RUNNING", providerStatus: "in_progress", collected: false, setComplete: false, recoveryStatus: null,
    batchStats: { requestCount: 3, successfulRequestCount: 0, failedRequestCount: 0, pendingRequestCount: 3 } } : {});
}
for (let q = 1; q <= 3; q++) {
  const id = "batch_local_" + hex(q);
  seed["ListingGenerator1Batches/" + id] = record(id, "sess_aaaaaaaa", 300 + q, { state: "JOB_STATE_QUEUED", locallyQueued: true, retryRequested: true, collected: false, setComplete: false,
    recoveryStatus: null, batchStats: null, results: null });
}
seed["ListingGenerator1Sessions/sess_aaaaaaaa"] = { sessionId: "sess_aaaaaaaa", planned: 303, registered: 303, complete: 270, approved: 0, active: 30, queued: 3, saving: 0, blocked: 0, cancelled: 0,
  missingImages: 0, checkedAt: Date.now() - 60000, unregistered: 0, issues: 0, processed: 270, pending: 33, status: "in_progress", finishedAt: null, issueSets: [],
  sets: Array.from({ length: 303 }, (_, i) => ({ category: "Rings", setN: i + 1, status: i < 270 ? "complete" : "active", missingSlots: [], note: "" })) };
seed["ListingGenerator1Sessions/sess_oooooooo"] = { sessionId: "sess_oooooooo", planned: 1300, registered: 1300, complete: 1300, status: "completed", issueSets: [], sets: [] };
seed["LG1_Config/batchSweep"] = { stage: "idle", lastSweepAt: TS.fromMillis(Date.now() - 300000), lastResult: { statusChecked: 30 } };
seed["LG1_Config/batchAdmission"] = { phase: "idle" };
m.db.seed(seed);

// ---- provider stub: OpenAI's batch status, by job name ----
const provider = new Map();
globalThis.fetch = async (url) => {
  const name = /\/batches\/([^/]+)$/.exec(String(url))?.[1];
  const answer = provider.get(name) || { status: "in_progress", request_counts: { total: 3, completed: 0, failed: 0 } };
  return { ok: true, status: 200, headers: { get: () => null }, text: async () => JSON.stringify(answer) };
};
const call = async (body) => JSON.parse((await impl.handler({ httpMethod: "POST", headers: {}, body: JSON.stringify(body) })).body || "{}");

// ---- the page: its real loader and merge code, talking to the real handler ----
let clock = Date.now();
class Clock extends Date { static now() { return clock; } }
function panel({ dropToken = false } = {}) {
  const sent = [], rendered = [];
  const el = { innerHTML: "", querySelectorAll: () => [] };
  const nodes = { batchJobsList: el, batchRefreshStatus: { textContent: "", style: {} } };
  const ctx = {
    document: { getElementById: (id) => nodes[id] || null }, console, Date: Clock,
    postJson: async (_fn, body) => {
      const before = m.snapshot();
      const answer = await call(body);
      const d = m.since(before);
      sent.push({ since: !!body.since, delta: !!answer.delta, reads: d.reads, bytes: d.bytes, changed: answer.changed?.length });
      if (dropToken) { delete answer.token; delete answer.sessionIds; }
      return answer;
    },
    _renderBatchDashboard: (sessions, resp) => { rendered.push(JSON.stringify({ sessions, sweep: resp.sweep, admission: resp.admission, truncated: resp.truncated, summaries: resp.sessions })); return "dashboard"; },
    _startBatchAutoPoll: () => {}, _batchSafeText: String, _batchInFlight: false,
  };
  const api = vm.runInNewContext(`let _batchListSeq = 0, _batchListDataSeq = 0, _batchPanelSeq = 0, _lastBatchList = [], _batchSweepInfo = null,
    _batchNextSweepAt = null, _batchRetryLimit = 30, _batchListLoadedAt = 0;
    ${loader}
    ({ refresh: refreshBatchJobsPanel, list: () => _lastBatchList, setList(v) { _lastBatchList = v; }, token: () => _batchToken })`, ctx);
  return { api, sent, rendered };
}
// After a small answer, a whole read of the same moment must give exactly what the panel shows.
async function sameAsWholeRead(p, label) {
  const shownList = JSON.stringify(p.api.list()), shownRender = p.rendered[p.rendered.length - 1];
  await sleep(4);
  await p.api.refresh();
  assert.equal(shownList, JSON.stringify(p.api.list()), label + ": the list on screen equals a whole read");
  assert.equal(shownRender, p.rendered[p.rendered.length - 1], label + ": the panel's input equals a whole read's");
}
const statusAll = async (names) => { for (const n of names) await call({ kind: "batch_status", batchName: n }); };
const running = Array.from({ length: 30 }, (_, i) => "batch_job" + (271 + i));

(async () => {
  const p = panel();
  await p.api.refresh();
  assert.equal(p.sent.length, 1);
  assert(!p.sent[0].since, "the first load reads the whole list");
  assert(p.sent[0].reads >= 1500, "a whole read is about 1,500 reads");
  assert(p.api.token(), "the whole answer carries the token to ask for changes");

  // 1. Nothing changed: a small answer, no job record read except the empty query.
  await sleep(4);
  await p.api.refresh({ delta: true });
  assert.deepEqual(p.sent.map((s) => [s.since, s.delta]), [[false, false], [true, true]]);
  assert.equal(p.sent[1].changed, 0);
  assert(p.sent[1].reads <= 15, "an idle pass reads 1 + the collector and admission documents + the session summaries (got " + p.sent[1].reads + ")");
  assert(p.sent[1].bytes < 5000, "and moves a few KB (got " + p.sent[1].bytes + ")");
  await sameAsWholeRead(p, "idle pass");

  // 2. The page's own provider check of the 30 running jobs (each writes its record): 30 records come back, merged in place.
  await sleep(4);
  await statusAll(running);
  p.sent.length = 0;
  await p.api.refresh({ delta: true });
  assert.deepEqual(p.sent.map((s) => [s.since, s.delta]), [[true, true]], "one small answer, no whole read");
  assert.equal(p.sent[0].changed, 30, "exactly the 30 checked jobs");
  assert(p.sent[0].reads <= 45, "30 records + the fixed few (got " + p.sent[0].reads + ")");
  await sameAsWholeRead(p, "30 provider checks");

  // 3. A job finishes at the provider: its state, results file and counts change in place.
  provider.set("batch_job271", { status: "completed", output_file_id: "file-out", request_counts: { total: 3, completed: 3, failed: 0 } });
  await sleep(4);
  await statusAll(["batch_job271"]);
  p.sent.length = 0;
  await p.api.refresh({ delta: true });
  assert.deepEqual(p.sent.map((s) => [s.since, s.delta]), [[true, true]]);
  assert.equal(p.api.list().find((b) => b.batchName === "batch_job271").state, "JOB_STATE_SUCCEEDED");
  await sameAsWholeRead(p, "a job succeeds");

  // 4. A queued set is cancelled by a person (a transaction on the record).
  await sleep(4);
  const cancelled = await call({ kind: "batch_cancel", batchName: "batch_local_" + hex(1) });
  assert.equal(cancelled.state, "JOB_STATE_CANCELLED");
  p.sent.length = 0;
  await p.api.refresh({ delta: true });
  assert.deepEqual(p.sent.map((s) => [s.since, s.delta]), [[true, true]]);
  assert.equal(p.api.list().find((b) => b.batchName === "batch_local_" + hex(1)).state, "JOB_STATE_CANCELLED");
  await sameAsWholeRead(p, "a queued set is cancelled");

  // 5. A failed job is queued for retry by the page's "Queue missing images" (a batch write).
  m.db.seed({ "ListingGenerator1Batches/batch_job272": { ...seed["ListingGenerator1Batches/batch_job272"], state: "JOB_STATE_FAILED", collected: false, providerError: "x", responsesFile: null } });
  await sleep(4);
  await p.api.refresh();                                       // a whole read holds the failed job (seeded outside the handler)
  await sleep(4);
  const queued = await call({ kind: "batch_retry_queue", sessionId: "sess_aaaaaaaa" });
  assert.equal(queued.queued, 1);
  p.sent.length = 0;
  await p.api.refresh({ delta: true });
  assert.deepEqual(p.sent.map((s) => [s.since, s.delta]), [[true, true]]);
  assert.equal(p.api.list().find((b) => b.batchName === "batch_job272").retryRequested, true);
  await sameAsWholeRead(p, "a retry is queued");

  // 6. A job the list does not hold (a new submission): the small answer names a record the page cannot place, so it reads the whole list at once.
  await sleep(4);
  const submitted = await call({ kind: "batch_submit", enqueueOnly: true, sessionId: "sess_aaaaaaaa", displayName: "lg1-Charms-303sets-x-part304of303",
    sets: [{ category: "Charms", setN: 304, outputBasePath: "listing-generator-1/Charms/Ready_To_List/Set_304", tasks: [task(0)] }] });
  assert.equal(submitted.queued, true, JSON.stringify(submitted));
  p.sent.length = 0;
  await p.api.refresh({ delta: true });
  assert.deepEqual(p.sent.map((s) => [s.since, s.delta]), [[true, true], [false, false]], "a small answer, then the whole list");
  assert(p.api.list().some((b) => b.displayName === "lg1-Charms-303sets-x-part304of303"), "the new job is on screen");
  await sameAsWholeRead(p, "a new job");

  // 7. A restart pointer: a queued record that now names the job sent for it, and the job it replaced.
  await sleep(4);
  m.db.seed({ ["ListingGenerator1Batches/batch_local_" + hex(2)]: { ...m.db.dump()["ListingGenerator1Batches/batch_local_" + hex(2)], retryBatchName: "batch_job273", rev: TS.now() } });
  p.sent.length = 0;
  await p.api.refresh({ delta: true });
  assert.deepEqual(p.sent.map((s) => [s.since, s.delta]), [[true, true], [false, false]], "a pointer changes other records' links: whole read");
  assert(!p.api.list().some((b) => b.batchName === "batch_local_" + hex(2)), "the pointer is left out");
  await sameAsWholeRead(p, "a restart pointer");

  // 8. A write to a record older than the list (the window is truncated): a whole read leaves it out, so the merge does too.
  await sleep(4);
  m.db.seed({ "ListingGenerator1Batches/batch_old3": { ...m.db.dump()["ListingGenerator1Batches/batch_old3"], filesPurgedAt: TS.now(), rev: TS.now() } });
  p.sent.length = 0;
  await p.api.refresh({ delta: true });
  assert.deepEqual(p.sent.map((s) => [s.since, s.delta]), [[true, true]], "an old record outside the list does not force a whole read");
  assert.equal(p.api.list().some((b) => b.batchName === "batch_old3"), false);
  await sameAsWholeRead(p, "an old record");

  // 9. Another reader replaced the list (the Charm Maker panel): the token no longer matches it, so the next pass reads the whole list.
  await sleep(4);
  p.api.setList(p.api.list().slice());
  p.sent.length = 0;
  await p.api.refresh({ delta: true });
  assert.deepEqual(p.sent.map((s) => [s.since, s.delta]), [[false, false]]);

  // 10. At least every ten minutes the whole list is read, whatever the token says.
  await sleep(4);
  await p.api.refresh({ delta: true });
  assert.equal(p.sent[p.sent.length - 1].delta, true, "back on small answers");
  clock += 11 * 60 * 1000;
  p.sent.length = 0;
  await p.api.refresh({ delta: true });
  assert.deepEqual(p.sent.map((s) => [s.since, s.delta]), [[false, false]], "ten minutes on: the whole list");
  clock -= 11 * 60 * 1000;

  // 11. A write that did not set `rev` (a writer this change does not know) is not seen by small answers, and the next whole read shows it.
  await sleep(4);
  await p.api.refresh();
  const doc = m.db.dump()["ListingGenerator1Batches/batch_job274"];
  m.db.seed({ "ListingGenerator1Batches/batch_job274": { ...doc, state: "JOB_STATE_FAILED" } });
  await sleep(4);
  await p.api.refresh({ delta: true });
  assert.equal(p.api.list().find((b) => b.batchName === "batch_job274").state, "JOB_STATE_RUNNING", "known limit: an unmarked write waits for the next whole read");
  await p.api.refresh();
  assert.equal(p.api.list().find((b) => b.batchName === "batch_job274").state, "JOB_STATE_FAILED", "the whole read (at most ten minutes on, or any Refresh) shows it");

  // 12. More than 300 records written since: the server answers with the whole list itself, the page takes it.
  await sleep(4);
  await p.api.refresh();
  await sleep(4);
  for (let n = 1; n <= 301; n++) { const d = m.db.dump()["ListingGenerator1Batches/batch_job" + n]; m.db.seed({ ["ListingGenerator1Batches/batch_job" + n]: { ...d, rev: TS.now() } }); }
  p.sent.length = 0;
  await p.api.refresh({ delta: true });
  assert.deepEqual(p.sent.map((s) => [s.since, s.delta]), [[true, false]], "the server gave the whole list in the same call");
  await sameAsWholeRead(p, "more than 300 changes");

  // 13. An answer without a token (an older server) is never followed by a small request.
  const old = panel({ dropToken: true });
  await old.api.refresh();
  await sleep(4);
  await old.api.refresh({ delta: true });
  assert.deepEqual(old.sent.map((s) => s.since), [false, false]);

  console.log("list delta: small answers merge to exactly the whole list after provider checks, success, cancel, retry queue; new jobs, pointers, >300 changes, a replaced list and ten minutes read the whole list");
})().catch((err) => { console.error(err); process.exitCode = 1; });
