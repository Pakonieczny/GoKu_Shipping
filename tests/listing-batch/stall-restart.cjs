"use strict";

// OpenAI sometimes accepts a listing job and never starts it. On 2026-09-27
// and 28 thirty such jobs held every place in the queue for most of a day.
// The collector now cancels a job that has done nothing for three hours and
// queues its set again: only the images the set still lacks, never twice,
// never while another job covers the set, and never a set a person cancelled
// or approved.
// No network: every provider, storage and Firestore call is a stub.
// Usage: node tests/listing-batch/stall-restart.cjs [background.js] [page.html]

const assert = require("node:assert/strict");
const fs = require("node:fs");
const vm = require("node:vm");
const lib = require("../../netlify/functions/lib/listingBatchAdmission.cjs");

const server = fs.readFileSync(process.argv[2] || "netlify/functions/geminiImageProxy-background.js", "utf8");
const page = fs.readFileSync(process.argv[3] || "Listing_Generator_1.html", "utf8");
const cut = (src, from, to) => {
  const start = src.indexOf(from);
  const end = src.indexOf(to, start);
  assert(start > 0 && end > start, `found ${from.trim().split("\n")[0]}`);
  return src.slice(start, end);
};
const branches = {
  batch_sweep: cut(server, '  if (kind === "batch_sweep") {', '  if (kind === "job_status") {'),
  batch_retry_missing: cut(server, '    if (kind === "batch_retry_missing") {', '    if (kind === "batch_stall_cancel") {'),
  batch_stall_cancel: cut(server, '    if (kind === "batch_stall_cancel") {', '    if (kind === "batch_restart_stalled") {'),
  batch_restart_stalled: cut(server, '    if (kind === "batch_restart_stalled") {', '    if (["batch_status", "batch_collect", "batch_cancel"].includes(kind) &&'),
  batch_status: cut(server, '    if (kind === "batch_status") {', '    if (kind === "batch_collect") {'),
  batch_cancel: cut(server, '    if (kind === "batch_cancel") {\n      const apiKey', '    if (kind === "files_cleanup")'),
};
const provider = vm.runInNewContext(`${cut(server, "function normalizeOpenAIBatch(raw) {", "// Cleanup is scoped")};
  ({ normalizeOpenAIBatch, batchFailureDetails })`, {});

const HOUR = 60 * 60 * 1000;
const START = Date.parse("2026-09-29T02:00:00Z");
const at = (ms) => ({ toMillis: () => ms });
const folder = (n) => `listing-generator-1/Generated_Listing_Sets/Ready_To_List/Beady_Necklace_Set_${n}`;
const set = (n, setKind = null) => ({ category: "Beady_Necklace", setN: n, setKind, outputBasePath: folder(n),
  tasks: [0, 1, 2, 3, 4, 5].map((slotIndex) => ({ slotIndex, type: slotIndex === 5 ? "copy" : "edit",
    source_storage_path: `listing-generator-1/Sources/${n}.png` })) });
const job = (batchName, n, extra = {}) => ({ batchName, docId: batchName, sessionId: "sess_1",
  displayName: `lg1-Beady_Necklace-1sets-${n}`, state: "JOB_STATE_RUNNING", providerStatus: "in_progress",
  collected: false, createdAt: at(START - 4 * HOUR), model: "gpt-image-2.5-sunburst", imageSize: "2K",
  sets: [set(n)], ...extra });
const openai = (status, completed = 0, failed = 0, extra = {}) =>
  ({ status, request_counts: { total: 6, completed, failed }, ...extra });

// One project: Firestore, storage and OpenAI fakes, and the real handler
// branches dispatched through module.exports.handler as in production.
function world({ records = [], jobs = {}, files = [] } = {}) {
  let now = START;
  class Clock extends Date { static now() { return now; } }
  const store = new Map(records.map((r) => [`batches/${r.batchName}`, { ...r }]));
  const live = new Map(Object.entries(jobs));
  const bucketFiles = new Set(files);
  const calls = [], cancels = [], submits = [], collects = [], forwarded = [];
  const write = (key, value, opts) => store.set(key, opts?.merge ? { ...(store.get(key) || {}), ...value } : { ...value });
  const docRef = (coll, id) => ({ coll, id, get: async () => {
    const d = store.get(`${coll}/${id}`);
    return { id, exists: !!d, data: () => (d ? { ...d } : undefined) };
  }, set: async (value, opts) => write(`${coll}/${id}`, value, opts) });
  const query = (coll, filters = []) => {
    const q = { where: (f, op, v) => query(coll, [...filters, [f, op, v]]), orderBy: () => q, limit: () => q,
      startAfter: () => q, get: async () => {
        const docs = [...store.entries()].filter(([k]) => k.startsWith(`${coll}/`))
          .map(([k, v]) => ({ id: k.slice(coll.length + 1), data: () => ({ ...v }) }))
          .filter((d) => filters.every(([f, op, v]) => (op === "in" ? v.includes(d.data()[f]) : d.data()[f] === v)));
        return { size: docs.length, docs, forEach: (fn) => docs.forEach(fn) };
      } };
    return q;
  };
  const db = { collection: (coll) => ({ doc: (id) => docRef(coll, id), where: (f, op, v) => query(coll, [[f, op, v]]) }),
    runTransaction: async (fn) => fn({ get: (ref) => ref.get(), set: (ref, value, opts) => write(`${ref.coll}/${ref.id}`, value, opts) }) };
  const bucket = {
    getFiles: async ({ prefix }) => [[...bucketFiles].filter((name) => name.startsWith(prefix)).map((name) => ({ name }))],
    file: (name) => ({ name, exists: async () => [bucketFiles.has(name)], copy: async (dest) => { bucketFiles.add(dest.name); } }),
  };
  const json = (statusCode, value) => ({ statusCode, body: JSON.stringify(value) });
  const handler = async (event) => {
    const body = JSON.parse(event.body);
    calls.push(body);
    if (body.kind === "batch_cancel" && body.batchName.startsWith("batch_local_")) {
      forwarded.push(body.batchName);
      return json(200, { ok: true, state: "JOB_STATE_CANCELLED" });
    }
    if (body.kind === "batch_submit") {
      // Stands in for OpenAI accepting the job: a record for it, and the
      // queued record pointing at it, as admission's complete() writes.
      submits.push(body);
      const batchName = `batch_new_${submits.length}`;
      live.set(batchName, openai("in_progress"));
      write(`batches/${batchName}`, { batchName, docId: batchName, sessionId: body.sessionId, displayName: body.displayName,
        state: "JOB_STATE_PENDING", collected: false, sets: body.sets, retryOf: body.retryOf,
        stallRestarts: Number(body.stallRestarts || 0), createdAt: at(now) });
      write(`batches/${body.retryOf}`, { retryBatchName: batchName, retryStatus: "submitted" }, { merge: true });
      return json(200, { ok: true, batchName });
    }
    if (body.kind === "batch_collect") {
      // Saves the images OpenAI finished (the first `completed` slots).
      collects.push(body);
      const record = store.get(`batches/${body.batchName}`);
      const done = live.get(body.batchName).request_counts.completed;
      for (let i = 1; i <= done; i++) bucketFiles.add(`${record.sets[0].outputBasePath}/Slot_${i}.png`);
      if (!body.importOnly) write(`batches/${body.batchName}`, { collected: true }, { merge: true });
      return json(200, { ok: true, imported: done });
    }
    if (!branches[body.kind]) throw new Error(`unexpected call ${body.kind}`);
    try {
      return await vm.runInNewContext(`(async () => { ${branches[body.kind]} })()`, { ...lib, ...provider,
        kind: body.kind, body, getDb: () => db, BATCHES_COLL: "batches", ORCH_COLL: "orchestrations",
        batchDocIdFromName: (x) => x, batchApiKey: () => "key", firestoreRetry: (fn) => fn(),
        getGeminiBatchJob: async (_key, name) => provider.normalizeOpenAIBatch(live.get(name)),
        cancelGeminiBatchJob: async (_key, name) => {
          cancels.push(name);
          live.get(name).status = "cancelling";
          return provider.normalizeOpenAIBatch(live.get(name));
        },
        admissionControl: () => ({ reconcile: async () => {}, rejected: async () => {} }),
        assertAllowedOutputBase: () => {},
        admin: { storage: () => ({ bucket: () => bucket }),
          firestore: { FieldPath: { documentId: () => "__name__" }, FieldValue: { serverTimestamp: () => at(now) } } },
        module: { exports: { handler } }, json, safeErr: (err) => ({ message: String(err?.message || err) }),
        console: { warn: () => {}, error: () => {} }, process: { env: {} }, Date: Clock,
        setTimeout: (fn, ms) => { now += ms; fn(); }, fetch: async () => { throw new Error("no network"); },
      });
    } catch (err) {
      return json(500, { error: { message: String(err?.message || err) } });
    }
  };
  const call = async (payload) => {
    const res = await handler({ httpMethod: "POST", headers: {}, body: JSON.stringify(payload) });
    return { statusCode: res.statusCode, ...JSON.parse(res.body) };
  };
  return { call, store, live, bucketFiles, calls, cancels, submits, collects, forwarded,
    get: (name) => store.get(`batches/${name}`), later: (ms) => { now += ms; } };
}

(async () => {
  // 1. What counts as never started.
  const stuck = job("batch_a", 1);
  const seen = (raw) => { const n = provider.normalizeOpenAIBatch(raw); return { providerStatus: n.providerStatus, batchStats: n.metadata.batchStats }; };
  assert(lib.neverStarted(stuck, seen(openai("in_progress")), START), "four hours, nothing done");
  assert(lib.neverStarted(stuck, seen(openai("validating")), START), "still validating after four hours");
  assert(lib.neverStarted({ ...stuck, stallRestarts: 4 }, seen(openai("in_progress")), START), "a fifth restart is allowed");
  for (const [why, record, raw] of [
    ["under three hours", { ...stuck, createdAt: at(START - 2.9 * HOUR) }, openai("in_progress")],
    ["one image done", stuck, openai("in_progress", 1)],
    ["one image refused", stuck, openai("in_progress", 0, 1)],
    ["finishing", stuck, openai("finalizing")],
    ["already stopping", stuck, openai("cancelling")],
    ["a Charm Maker job", { ...stuck, sets: [set(1, "charm_maker")] }, openai("in_progress")],
    ["a two-set job", { ...stuck, sets: [set(1), set(2)] }, openai("in_progress")],
    ["five restarts used", { ...stuck, stallRestarts: 5 }, openai("in_progress")],
    ["no send time", { ...stuck, createdAt: null }, openai("in_progress")],
    ["a queued record", { ...stuck, batchName: `batch_local_${"a".repeat(40)}`, locallyQueued: true }, openai("in_progress")],
  ]) assert(!lib.neverStarted(record, seen(raw), START), `not restarted: ${why}`);

  // 2. The collector, over three rounds.
  const w = world({
    records: [
      job("batch_stuck", 1),
      job("batch_young", 2, { createdAt: at(START - HOUR) }),
      job("batch_busy", 3),
      job("batch_charm", 4, { sets: [set(4, "charm_maker")] }),
      job("batch_person", 5, { state: "JOB_STATE_CANCELLED", providerStatus: "cancelled" }),
      // Stopped earlier, but a second job for the same set is still working.
      job("batch_twin_a", 6, { state: "JOB_STATE_CANCELLED", providerStatus: "cancelled", stallCancelRequestedAt: at(START - HOUR) }),
      job("batch_twin_b", 6),
      // Stopped earlier, then cancelled by a person.
      job("batch_blocked", 7, { state: "JOB_STATE_CANCELLED", providerStatus: "cancelled",
        stallCancelRequestedAt: at(START - HOUR), stallRestartBlocked: true }),
    ],
    jobs: { batch_stuck: openai("in_progress"), batch_young: openai("in_progress"), batch_busy: openai("in_progress", 2),
      batch_charm: openai("in_progress"), batch_person: openai("cancelled"), batch_twin_a: openai("cancelled"),
      batch_twin_b: openai("in_progress", 1), batch_blocked: openai("cancelled") },
    // Set 1 already has its first two images from an earlier job.
    files: [`${folder(1)}/Slot_1.png`, `${folder(1)}/Slot_2.png`],
  });
  const first = await w.call({ kind: "batch_sweep" });
  assert.equal(first.statusCode, 200, first.error?.message);
  assert.deepEqual(w.cancels, ["batch_stuck"], "only the job that never started is cancelled");
  assert.equal(first.stalledCancelled, 1);
  assert.equal(first.stalledRestarted, 0, "its set waits until OpenAI confirms the cancel");
  assert.equal(w.get("batch_stuck").retryStatus, "stalled");
  assert(w.get("batch_stuck").stallCancelRequestedAt, "the stop is recorded as the collector's");
  assert.equal(w.submits.length, 0);

  w.live.get("batch_stuck").status = "cancelled";
  w.later(10 * 60 * 1000);
  const second = await w.call({ kind: "batch_sweep" });
  assert.equal(second.statusCode, 200, second.error?.message);
  assert.equal(second.stalledRestarted, 1);
  const restart = w.get("batch_stuck").retryBatchName;
  assert.match(restart, /^batch_local_[a-f0-9]{40}$/);
  assert.equal(w.get("batch_stuck").retryStatus, "restarted");
  assert.equal(w.get("batch_stuck").retryRequested, false);
  const queued = w.get(restart);
  assert.equal(queued.retryOf, "batch_stuck");
  assert.equal(queued.stallRestarts, 1);
  assert.equal(queued.sessionId, "sess_1", "the restart stays in the same batch on the dashboard");
  assert(w.bucketFiles.has(`${folder(1)}/Slot_6.png`), "the missing copied image is copied, not generated");
  assert.equal(w.submits.length, 1, "the restarted set is submitted in the same round");
  const sent = w.submits[0];
  assert.deepEqual(sent.sets[0].tasks.filter((t) => t.type !== "copy").map((t) => t.slotIndex), [2, 3, 4],
    "only the images the set still lacks are generated");
  assert.equal(sent.sets[0].allTasks.length, 6);
  assert.equal(sent.stallRestarts, 1);
  assert.match(sent.displayName, /^stall-restart-Beady_Necklace-Set_1-/);
  assert.equal(w.get("batch_new_1").stallRestarts, 1, "the new job carries the restart count");
  for (const name of ["batch_young", "batch_busy", "batch_charm", "batch_person", "batch_twin_a", "batch_blocked"]) {
    assert(!w.get(name).retryBatchName, `${name} is left alone`);
  }

  w.later(10 * 60 * 1000);
  const third = await w.call({ kind: "batch_sweep" });
  assert.equal(third.statusCode, 200, third.error?.message);
  assert.equal(w.submits.length, 1, "never submitted twice");
  assert.equal(w.cancels.length, 1);
  assert.equal(w.calls.filter((c) => c.kind === "batch_restart_stalled" && c.batchName === "batch_stuck").length, 1);

  // 3. Restart guards, called directly.
  const stopped = (n, extra = {}) => job(`batch_s${n}`, n, { state: "JOB_STATE_RUNNING", providerStatus: "cancelling",
    stallCancelRequestedAt: at(START - HOUR), ...extra });
  const g = world({
    records: [stopped(10), stopped(11), stopped(12), stopped(13), stopped(14, { stallRestartBlocked: true })],
    jobs: { batch_s10: openai("cancelled"), batch_s11: openai("cancelled", 2, 0, { output_file_id: "file-out" }),
      batch_s12: openai("cancelled"), batch_s13: openai("cancelling"), batch_s14: openai("cancelled") },
    files: [
      "listing-generator-1/Generated_Listing_Sets/Completed_Listing_Sets/Beady_Necklace_Set_10/Slot_1.png",
      ...[1, 2, 3, 4, 5, 6].map((i) => `${folder(12)}/Slot_${i}.png`),
    ],
  });
  const approved = await g.call({ kind: "batch_restart_stalled", batchName: "batch_s10" });
  assert.equal(approved.protected, true, "an approved set is never refilled");
  assert.equal(g.get("batch_s10").stallRestartClosed, true);
  const partial = await g.call({ kind: "batch_restart_stalled", batchName: "batch_s11" });
  assert.equal(partial.created, true);
  assert.equal(g.collects[0]?.importOnly, true, "images OpenAI finished while stopping are saved first");
  assert.equal(g.get("batch_s11").collected, false, "that import does not close the set");
  assert.deepEqual(g.get(partial.batchName).sets[0].tasks.map((t) => t.slotIndex), [2, 3, 4, 5]);
  const full = await g.call({ kind: "batch_restart_stalled", batchName: "batch_s12" });
  assert.equal(full.complete, true, "a set with every image needs no new job");
  assert.equal(g.get("batch_s12").retryStatus, "complete");
  const early = await g.call({ kind: "batch_restart_stalled", batchName: "batch_s13" });
  assert.equal(early.waiting, true, "nothing is queued before OpenAI confirms the cancel");
  const blocked = await g.call({ kind: "batch_restart_stalled", batchName: "batch_s14" });
  assert.equal(blocked.statusCode, 409, "a set a person cancelled is not restarted");
  assert.equal([...g.store.keys()].filter((k) => k.includes("batch_local_")).length, 1, "one queued record in all");

  // 4. A person cancelling a job the collector is restarting.
  const c = world({
    records: [stopped(20), stopped(21, { retryBatchName: `batch_local_${"b".repeat(40)}` }),
      job("batch_s22", 22, { stallCancelRequestedAt: at(START - HOUR) })],
    jobs: { batch_s20: openai("cancelling"), batch_s21: openai("cancelled"), batch_s22: openai("in_progress") },
  });
  const stop20 = await c.call({ kind: "batch_cancel", batchName: "batch_s20" });
  assert.equal(stop20.restartCancelled, true);
  assert.equal(c.get("batch_s20").stallRestartBlocked, true, "its set is not queued again");
  assert.equal((await c.call({ kind: "batch_restart_stalled", batchName: "batch_s20" })).statusCode, 409);
  await c.call({ kind: "batch_cancel", batchName: "batch_s21" });
  assert.deepEqual(c.forwarded, [`batch_local_${"b".repeat(40)}`], "an already queued restart is cancelled instead");
  await c.call({ kind: "batch_cancel", batchName: "batch_s22" });
  assert.deepEqual(c.cancels, ["batch_s22"], "a job whose stop did not reach OpenAI is cancelled there");
  assert.equal(c.get("batch_s22").stallRestartBlocked, true);

  // 5. The Batch Progress card.
  const render = cut(page, "    function _renderSessionBlock(session) {", "    async function _cancelSession(sessionId) {");
  const helper = cut(page, "    function _awaitingStallRestart(b) {", "    function _formatDuration(ms) {");
  const card = (batches) => vm.runInNewContext(`${helper}; ${render}; _renderSessionBlock({ sessionId: "sess_1", batches, earliest: Date.now(), latest: Date.now() })`, {
    batches, Date, _batchRetryLimit: 30, _batchSweepInfo: null, _batchNextSweepAt: null,
    _normBatchState: (x) => String(x || "").replace(/^BATCH_STATE_/, "JOB_STATE_"), _batchSafeText: (x) => String(x),
    _formatDuration: () => "4h", normalizeImageModelId: (x) => x, getImageModelConfig: () => ({ label: "Sunburst" }),
    DEFAULT_IMAGE_MODEL: "m",
  });
  const row = (name, extra) => ({ batchName: name, sessionId: "sess_1", displayName: `lg1-Beady_Necklace-2sets-x-part${name.slice(-1)}of2`,
    setsCount: 1, collected: false, createdAt: Date.now() - 4 * HOUR, updatedAt: Date.now(),
    batchStats: { requestCount: 6, successfulRequestCount: 0, failedRequestCount: 0, pendingRequestCount: 6 }, ...extra });
  const stopping = card([
    row("batch_p1", { state: "JOB_STATE_RUNNING", providerStatus: "cancelling", stallRestart: "pending" }),
    row("batch_p2", { state: "JOB_STATE_CANCELLED", providerStatus: "cancelled", stallRestart: "pending" }),
  ]);
  assert.match(stopping, /Restarting stalled jobs/);
  assert.match(stopping, /2 restarting/);
  assert.match(stopping, /never started · restarting/);
  assert.doesNotMatch(stopping, /Stopping jobs|need attention|All batches cancelled/);
  const requeued = card([
    row("batch_p1", { state: "JOB_STATE_CANCELLED", providerStatus: "cancelled", stallRestart: "restarted", retryBatchName: "batch_local_q" }),
    row("batch_local_q", { batchName: "batch_local_q", state: "JOB_STATE_QUEUED", retryRequested: true, retryOf: "batch_p1", batchStats: null }),
  ]);
  assert.match(requeued, /never started · queued again/);
  assert.match(requeued, /1 queued/);
  assert.doesNotMatch(requeued, /need attention|All batches cancelled/);
  console.log("Listing batch: a job OpenAI never started is cancelled after three hours and only its missing images are queued again, once, with every guard and the card");
})().catch((err) => { console.error(err); process.exitCode = 1; });
