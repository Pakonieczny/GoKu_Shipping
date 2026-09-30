"use strict";

// The Batch Progress card lost its progress bar on 2026-09-29 and showed only
// "Earlier history · partial view". The list holds the newest 1000 records; a
// big submission with restarts fills it with pointer records (the queued record
// each restart leaves, which only names the job sent for it) and the start of
// the submission fell out of view. Now:
//   server - restart pointers are left out, a record that pointed at one points
//            at the job it led to, and `truncated` is true only when older
//            records may exist beyond what was read
//   page   - the oldest visible submission is a "partial view" only when the
//            server says records were left out
// No network: Firestore is a stub. Usage: node tests/listing-batch/list-window.cjs

const assert = require("node:assert/strict");
const fs = require("node:fs");
const vm = require("node:vm");

const server = fs.readFileSync("netlify/functions/geminiImageProxy-background.js", "utf8");
const page = fs.readFileSync("Listing_Generator_1.html", "utf8");
const cut = (src, from, to) => {
  const start = src.indexOf(from);
  const end = src.indexOf(to, start);
  assert(start > 0 && end > start, `found ${from.trim().split("\n")[0]}`);
  return src.slice(start, end);
};
const branch = cut(server, '    if (kind === "batch_list") {', '    if (kind === "batch_cancel") {\n      const apiKey');

const at = (ms) => ({ toMillis: () => ms });
// Newest first, as the query returns them.
function list(docs, body) {
  const asked = [];
  let selectedFields = [];
  const sorted = [...docs].sort((a, b) => b.createdAt - a.createdAt);
  const db = { collection: (name) => ["LG1_Config", "sessions"].includes(name)
    ? { doc: () => ({ get: async () => ({ exists: false, data: () => undefined }) }) }
    : { orderBy: () => ({ limit: (n) => ({ select: (...fields) => { selectedFields = fields; return { get: async () => {
        asked.push(n);
        const rows = sorted.slice(0, n).map((d) => ({ id: d.batchName, data: () => Object.fromEntries(
          Object.entries({ ...d, createdAt: at(d.createdAt) }).filter(([key]) => fields.includes(key))) }));
        return { size: rows.length, forEach: (fn) => rows.forEach(fn) };
      } }; } }) }) } };
  return vm.runInNewContext(`(async () => { ${branch} })()`, {
    kind: "batch_list", SESSIONS_COLL: "sessions", body, getDb: () => db, BATCHES_COLL: "batches", clampNumber: (n, min, max, fb) => {
      const v = Number(n); return Number.isFinite(v) ? Math.min(max, Math.max(min, v)) : fb; },
    capacityRefusals: () => 0, preferredCharmRenderModelId: () => "m", json: (statusCode, value) => ({ statusCode, ...value }),
    console: { warn: () => {} },
  }).then((res) => JSON.parse(JSON.stringify({ ...res, asked, selectedFields })));
}

const job = (n, createdAt, extra = {}) => ({ batchName: `batch_${n}`, sessionId: "sess_1", state: "JOB_STATE_RUNNING",
  collected: false, createdAt, ...extra });
const pointer = (n, createdAt, target) => ({ batchName: `batch_local_${n}`, sessionId: "sess_1", state: "JOB_STATE_QUEUED",
  locallyQueued: true, collected: false, retryRequested: false, retryBatchName: target, createdAt });

(async () => {
  // A set restarted twice: job A (cancelled) -> pointer P1 -> job B (cancelled) -> pointer P2 -> job C.
  const docs = [
    job("A", 1000, { state: "JOB_STATE_CANCELLED", retryBatchName: "batch_local_P1", stallCancelRequestedAt: at(1) }),
    pointer("P1", 2000, "batch_B"),
    job("B", 2100, { state: "JOB_STATE_CANCELLED", retryBatchName: "batch_local_P2", retryOf: "batch_local_P1" }),
    pointer("P2", 3000, "batch_C"),
    job("C", 3100, { retryOf: "batch_local_P2", collected: true }),
    { ...job("Q", 3200), state: "JOB_STATE_QUEUED", locallyQueued: true, retryRequested: true },
  ];
  const r = await list(docs, { limit: 50, includeCollected: true });
  assert.equal(r.statusCode, 200);
  for (const heavy of ['sets', 'routes', 'preparedSubmission', 'inputJsonl'])
    assert(!r.selectedFields.includes(heavy), `progress never downloads ${heavy}`);
  for (const compact of ['setKeys', 'setsCount', 'requestCount', 'results', 'repairPending'])
    assert(r.selectedFields.includes(compact), `progress retains ${compact}`);
  assert.deepEqual(r.batches.map((b) => b.batchName), ["batch_Q", "batch_C", "batch_B", "batch_A"],
    "the pointers are left out; the record still waiting to be sent stays");
  assert.equal(r.batches.find((b) => b.batchName === "batch_A").retryBatchName, "batch_B", "A now points at the job that followed it");
  assert.equal(r.batches.find((b) => b.batchName === "batch_B").retryBatchName, "batch_C");
  assert.equal(r.batches.find((b) => b.batchName === "batch_C").retryBatchName, null);
  assert.equal(r.truncated, false, "everything was read");
  assert.deepEqual(r.asked, [50], "a small list reads what it shows");

  // The 2026-09-29 shape: 1000 newest records, a third of them pointers, the
  // submission's first jobs beyond the 1000th.
  const big = [];
  for (let i = 0; i < 700; i++) big.push(job(`j${i}`, 10000 + i * 10, { collected: true }));
  for (let i = 0; i < 360; i++) big.push(pointer(`x${i}`, 10005 + i * 20, `batch_j${i}`));
  const wide = await list(big, { limit: 1000, includeCollected: true });
  assert.equal(wide.asked[0], 1500, "reads half as many again as it shows");
  assert.equal(wide.batches.length, 700, "every job of the submission is in the answer");
  assert.equal(wide.batches[wide.batches.length - 1].batchName, "batch_j0", "including the first one");
  assert.equal(wide.truncated, false, "nothing older exists, so the submission is not a partial view");
  const narrow = await list(big, { limit: 400, includeCollected: true });
  assert(narrow.batches.length > 300 && narrow.batches.length <= 400, "a smaller list shows the jobs among the newest records it read");
  assert.equal(narrow.truncated, true, "older jobs were left out");

  // More than the read ceiling: older records may exist.
  const many = Array.from({ length: 1600 }, (_, i) => job(`k${i}`, 50000 + i, { collected: true }));
  const deep = await list(many, { limit: 1000, includeCollected: true });
  assert.equal(deep.batches.length, 1000);
  assert.equal(deep.truncated, true);

  // A collected record is still left out when asked not to include those.
  const open = await list(docs, { limit: 50 });
  assert(!open.batches.some((b) => b.collected), "includeCollected false still hides the saved");

  // ---- page: partial view only when the server says so -------------------------
  const loader = cut(page, "    async function refreshBatchJobsPanel() {", "    // Normalize state for both legacy");
  const dashboard = cut(page, "    function _renderBatchDashboard(sessions, response) {", "    async function refreshBatchJobsPanel() {");
  const shown = async (resp) => {
    const seen = [];
    const el = { innerHTML: "", querySelectorAll: () => [] };
    const nodes = { batchJobsList: el, batchRefreshStatus: { textContent: "", style: {} } };
    const api = vm.runInNewContext(`let _batchListSeq = 0, _batchListDataSeq = 0, _batchPanelSeq = 0, _lastBatchList = [],
      _batchSweepInfo = null, _batchNextSweepAt = null, _batchRetryLimit = 30, _batchListLoadedAt = 0;
      ${dashboard}\n${loader}; ({ refresh: refreshBatchJobsPanel })`, {
      document: { getElementById: (id) => nodes[id] || null }, postJson: async () => resp, console, Date,
      _renderSessionBlock: (s) => { seen.push({ id: s.sessionId, truncated: !!s.historyTruncated }); return ""; },
      _startBatchAutoPoll: () => {}, _batchSafeText: String,
      _normBatchState: x => x, _awaitingStallRestart: () => false, _batchInFlight: false,
    });
    await api.refresh();
    return JSON.parse(JSON.stringify(seen));
  };
  const rows = (n) => Array.from({ length: n }, (_, i) => ({ batchName: `batch_${i}`, sessionId: "sess_1", createdAt: 1000 - i }));
  assert.deepEqual(await shown({ ok: true, batches: rows(1000), truncated: false }), [{ id: "sess_1", truncated: false }],
    "1000 records with nothing left out is a complete view");
  assert.deepEqual(await shown({ ok: true, batches: rows(700), truncated: true }), [{ id: "sess_1", truncated: true }],
    "records left out: the oldest submission is a partial view");
  assert.deepEqual(await shown({ ok: true, batches: rows(1000) }), [{ id: "sess_1", truncated: true }],
    "an answer without the flag keeps the old rule");
  assert.deepEqual(await shown({ ok: true, batches: rows(50) }), [{ id: "sess_1", truncated: false }]);

  console.log("Listing batch list: restart pointers left out, chains kept, partial view only when records were left out");
})().catch((err) => { console.error(err); process.exitCode = 1; });
