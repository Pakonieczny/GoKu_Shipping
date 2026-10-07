"use strict";

// Firebase cost (FC12): a scheduled batch sweep must not re-read every batch record and every saved file of a
// listing session that has nothing running, and must not repeat the check at the end of a run in which nothing
// changed. A person's own sweep keeps checking everything, exactly as before.

const assert = require("node:assert/strict");
const fs = require("node:fs");
const vm = require("node:vm");

const source = fs.readFileSync("netlify/functions/geminiImageProxy-background.js", "utf8");
const start = source.indexOf('  if (kind === "batch_sweep") {');
const end = source.indexOf('  if (kind === "job_status") {', start);
assert(start > 0 && end > start, "scheduled sweep handler exists");
const lib = require("../../netlify/functions/lib/listingBatchAdmission.cjs");

const T0 = Date.parse("2026-10-07T10:00:00Z");
const SESSIONS = ["sess_aaaaaaaa", "sess_bbbbbbbb", "sess_cccccccc"];

async function runSweep({ body = {}, summaries = {}, jobs = [], statusAnswer = "JOB_STATE_RUNNING", summaryError = false } = {}) {
  const reconciled = [], summaryReads = [];
  const guard = {};
  const db = {
    getAll: async (ref, opts) => {
      summaryReads.push({ id: ref.id, fieldMask: opts && opts.fieldMask });
      if (summaryError) throw new Error("unavailable");
      const d = summaries[ref.id];
      return [{ exists: !!d, data: () => d }];
    },
    collection: (name) => {
      if (name === "LG1_Config") return { doc: () => ({
        get: async () => ({ exists: false, data: () => guard }),
        set: async (value) => { Object.assign(guard, value); },
      }) };
      if (name === "sessions") return { doc: (id) => ({ id }) };
      if (name === "orchestrations") return { where: () => ({ limit: () => ({ get: async () => ({ forEach: () => {} }) }) }) };
      if (name === "batches") return {
        orderBy: () => ({ limit: () => ({ select: () => ({ get: async () => ({
          docs: SESSIONS.map(sessionId => ({ data: () => ({ sessionId }) })) }) }) }) }),
        where: (field) => field === "repairPending"
          ? { limit: () => ({ select: () => ({ get: async () => ({ forEach: () => {} }) }) }) }
          : { orderBy: () => ({ limit: () => ({ select: () => ({ get: async () => {
              const docs = jobs.map((job, i) => ({ id: i, data: () => ({ ...job }) }));
              return { size: docs.length, docs, forEach: fn => docs.forEach(fn) };
            } }) }) }) },
        doc: () => ({ set: async () => {} }),
      };
      throw new Error(`unexpected collection ${name}`);
    },
  };
  let clock = T0;
  class Clock extends Date { static now() { return clock; } }
  const handler = async (event) => {
    const payload = JSON.parse(event.body);
    if (payload.kind === "batch_status") return { body: JSON.stringify({ ok: true, state: statusAnswer }) };
    throw new Error(`unexpected call ${payload.kind}`);
  };
  const result = await vm.runInNewContext(`(async () => { ${source.slice(start, end)} })()`, {
    kind: "batch_sweep", getDb: () => db, body,
    SESSIONS_COLL: "sessions",
    reconcileSession: async ({ sessionId }) => { reconciled.push(sessionId); return {}; },
    admin: { storage: () => ({ bucket: () => ({}) }), firestore: { FieldPath: { documentId: () => "__name__" },
      FieldValue: { serverTimestamp: () => clock } } },
    admissionControl: () => ({ reconcile: async () => {} }),
    quotaFailure: lib.quotaFailure, neverStarted: lib.neverStarted, stallRestartPending: lib.stallRestartPending,
    SWEEP_CALL_LIMITS: lib.SWEEP_CALL_LIMITS, withLimit: lib.withLimit, stallCutoffMs: lib.stallCutoffMs,
    VALIDATION_WAIT_MS: lib.VALIDATION_WAIT_MS, PREPARATION_RESERVATION_MS: lib.PREPARATION_RESERVATION_MS,
    BATCHES_COLL: "batches", ORCH_COLL: "orchestrations",
    module: { exports: { handler } },
    json: (statusCode, b) => ({ statusCode, ...b }),
    console: { warn: () => {}, error: () => {} },
    process: { env: { URL: "https://example.test" } },
    Date: Clock,
    setTimeout: (fn, ms) => { clock += ms; fn(); },
    fetch: async () => ({ ok: true, status: 202 }),
    batchDocIdFromName: (x) => x,
    safeErr: (err) => ({ message: err.message }),
  });
  return { result, reconciled, summaryReads, guard };
}

const idle = (extra = {}) => ({ status: "completed_with_issues", active: 0, queued: 0, saving: 0, checkedAt: T0 - 5 * 60000, ...extra });

(async () => {
  // Scheduled, three sessions with nothing running, all checked minutes ago: no record or file is read at all.
  const quiet = await runSweep({ body: { cronTriggered: true }, summaries: { [SESSIONS[0]]: idle(), [SESSIONS[1]]: idle(), [SESSIONS[2]]: idle({ status: "completed" }) } });
  assert.equal(quiet.result.statusCode, 200);
  assert.deepEqual(quiet.reconciled, [], "idle sessions are not re-read every ten minutes");
  assert.equal(quiet.summaryReads.length, 3, "one small summary read per session decides");
  assert(quiet.summaryReads.every(r => Array.isArray(r.fieldMask) && !r.fieldMask.includes("sets")), "the summary is read with a field mask, never its set list");

  // A session with a job running, queued or being saved is still checked every run (start of run only: nothing changed).
  const live = await runSweep({ body: { cronTriggered: true }, summaries: { [SESSIONS[0]]: idle({ active: 2 }), [SESSIONS[1]]: idle({ saving: 1 }), [SESSIONS[2]]: idle({ queued: 1 }) } });
  assert.deepEqual(live.reconciled, SESSIONS, "sessions with live work keep their ten-minute check");

  // Idle for half an hour: due again. Completed: only after twelve hours.
  const due = await runSweep({ body: { cronTriggered: true }, summaries: {
    [SESSIONS[0]]: idle({ checkedAt: T0 - 31 * 60000 }), [SESSIONS[1]]: idle({ status: "completed", checkedAt: T0 - 11 * 3600000 }),
    [SESSIONS[2]]: idle({ status: "completed", checkedAt: T0 - 13 * 3600000 }) } });
  assert.deepEqual(due.reconciled, [SESSIONS[0], SESSIONS[2]]);

  // Never checked (no summary yet) and an unreadable summary both check at once.
  const fresh = await runSweep({ body: { cronTriggered: true }, summaries: {} });
  assert.deepEqual(fresh.reconciled, SESSIONS, "a session never checked is checked");
  const unreadable = await runSweep({ body: { cronTriggered: true }, summaries: { [SESSIONS[0]]: idle() }, summaryError: true });
  assert.deepEqual(unreadable.reconciled, SESSIONS, "an unreadable summary falls back to the full check");

  // A run in which a job changed state repeats the check at the end for every session, as before.
  const moved = await runSweep({ body: { cronTriggered: true }, statusAnswer: "JOB_STATE_FAILED",
    jobs: [{ batchName: "batch_one", state: "JOB_STATE_RUNNING", collected: false }],
    summaries: { [SESSIONS[0]]: idle(), [SESSIONS[1]]: idle(), [SESSIONS[2]]: idle() } });
  assert.deepEqual(moved.reconciled, SESSIONS, "after a change the end-of-run check still runs");

  // A person's own sweep (no cronTriggered flag) checks every session at the start and again at the end, with no summary read.
  const manual = await runSweep({ body: {}, summaries: { [SESSIONS[0]]: idle(), [SESSIONS[1]]: idle(), [SESSIONS[2]]: idle() } });
  assert.deepEqual(manual.reconciled, [...SESSIONS, ...SESSIONS], "a manual sweep is unchanged");
  assert.equal(manual.summaryReads.length, 0);

  // A person's sweep for one session (right after a submit) always checks that session even when scheduled flags are present.
  const named = await runSweep({ body: { cronTriggered: true, sessionId: SESSIONS[1] }, summaries: { [SESSIONS[0]]: idle(), [SESSIONS[1]]: idle(), [SESSIONS[2]]: idle() } });
  assert.deepEqual(named.reconciled, [SESSIONS[1]], "the named session is always checked");
  console.log("sweep idle cost: ok");
})().catch((err) => { console.error(err); process.exit(1); });
