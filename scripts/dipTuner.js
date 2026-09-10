#!/usr/bin/env node
/*  scripts/dipTuner.js — the dip-rule tuner runner. Runs on your own computer.
 *
 *    node scripts/dipTuner.js --site https://goldenspike.app --key dtk_…
 *
 *  After the first start the site and key are remembered in
 *  ~/.investor-ai/tuner/config.json, so later it is just `node scripts/dipTuner.js`.
 *
 *  What it does, in order:
 *    1. asks the site (investorTuner) for the tuner settings you saved in the console
 *    2. downloads the historical price library once (cached under ~/.investor-ai/tuner/bars)
 *    3. computes the parameter-independent work once (every candidate fall)
 *    4. evaluates parameter sets across CPU cores, honouring your night window
 *       and effort levels, reporting progress and the leaderboard every minute
 *    5. when "apply automatically" is on, copies the most successful set that
 *       also holds up on the held-out months into the live dip lane
 *
 *  Stop it with Ctrl-C; start it again and it carries on where it left off.
 */
"use strict";
const fs = require("fs"), path = require("path"), os = require("os"), zlib = require("zlib");
const { Worker } = require("worker_threads");
const https = require("https"), http = require("http");
process.env.FIREBASE_PRIVATE_KEY = process.env.FIREBASE_PRIVATE_KEY || "";   // the shared modules must not need Firestore here
let TUNER; try { TUNER = require("../netlify/functions/_investorDipTuner"); } catch (e) { TUNER = require("./dipTunerDefs"); }   // the standalone bundle ships the defs beside this file
const E = require("./dipTunerEngine");

const HOME = path.join(os.homedir(), ".investor-ai", "tuner");
const CONFIG = path.join(HOME, "config.json");
const BARS_DIR = path.join(HOME, "bars");
const RESULTS_DIR = path.join(HOME, "results");
const REPORT_EVERY_MS = 60000, POLL_EVERY_MS = 30000, BATCH = 4;

/* ── config ─────────────────────────────────────────────────────────────── */
function argOf(name) { const i = process.argv.indexOf("--" + name); return i >= 0 ? process.argv[i + 1] : null; }
function loadConfig() {
  fs.mkdirSync(HOME, { recursive: true }); fs.mkdirSync(BARS_DIR, { recursive: true }); fs.mkdirSync(RESULTS_DIR, { recursive: true });
  let cfg = {}; try { cfg = JSON.parse(fs.readFileSync(CONFIG, "utf8")); } catch { cfg = {}; }
  const site = argOf("site") || process.env.INVESTOR_SITE || cfg.site, key = argOf("key") || process.env.INVESTOR_TUNER_KEY || cfg.key;
  if (!site || !key) { console.error("Usage: node scripts/dipTuner.js --site https://your-site --key dtk_…  (create the key in the console: Simulation → Dip tuner)"); process.exit(2); }
  fs.writeFileSync(CONFIG, JSON.stringify({ site: site.replace(/\/+$/, ""), key }, null, 2));
  return { site: site.replace(/\/+$/, ""), key };
}
const CFG = loadConfig();

/* ── HTTP ───────────────────────────────────────────────────────────────── */
function call(action, payload = {}, { timeoutMs = 120000 } = {}) {
  return new Promise((resolve, reject) => {
    const url = new URL(CFG.site + "/.netlify/functions/investorTuner");
    const body = JSON.stringify({ action, ...payload });
    const req = (url.protocol === "https:" ? https : http).request(url, { method: "POST", headers: { "content-type": "application/json", authorization: "Bearer " + CFG.key, "content-length": Buffer.byteLength(body) }, timeout: timeoutMs }, (res) => {
      const chunks = []; res.on("data", (c) => chunks.push(c)); res.on("end", () => {
        const text = Buffer.concat(chunks).toString();
        let json = null; try { json = JSON.parse(text); } catch { /* not JSON */ }
        if (res.statusCode !== 200) return reject(new Error(`${action}: HTTP ${res.statusCode} ${json && json.error ? json.error : text.slice(0, 120)}`));
        resolve(json);
      });
    });
    req.on("timeout", () => req.destroy(new Error(action + ": timed out")));
    req.on("error", reject); req.end(body);
  });
}
async function withRetry(fn, tries = 4) { let last; for (let i = 0; i < tries; i++) { try { return await fn(); } catch (e) { last = e; await sleep(1500 * (i + 1)); } } throw last; }
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
function log(...a) { console.log(new Date().toLocaleTimeString(), ...a); }

/* ── library sync ───────────────────────────────────────────────────────── */
async function syncLibrary(library) {
  const want = library.filter((d) => d.bars > 0);
  const missing = want.filter((d) => { const f = path.join(BARS_DIR, d.id + ".gz"); try { const st = JSON.parse(fs.readFileSync(f + ".json", "utf8")); return !(fs.existsSync(f) && st.fetchedAtMs === d.fetchedAtMs); } catch { return true; } });
  if (missing.length) log(`downloading ${missing.length} of ${want.length} company-months from the library…`);
  let done = 0;
  const queue = missing.slice();
  const worker = async () => {
    while (queue.length) {
      const batch = queue.splice(0, 25);
      const r = await withRetry(() => call("bars", { ids: batch.map((d) => d.id) }));
      for (const doc of r.docs) {
        if (doc.missing || !doc.data) continue;
        fs.writeFileSync(path.join(BARS_DIR, doc.id + ".gz"), Buffer.from(doc.data, "base64"));
        fs.writeFileSync(path.join(BARS_DIR, doc.id + ".gz.json"), JSON.stringify({ fetchedAtMs: doc.fetchedAtMs, bars: doc.bars, days: doc.days, complete: doc.complete }));
      }
      done += batch.length;
      if (done % 250 < 25) log(`  library ${done}/${missing.length}`);
      STATE.phase = `downloading the library (${done} of ${missing.length})`;
    }
  };
  await Promise.all([worker(), worker(), worker(), worker()]);
  return { downloaded: missing.length, total: want.length };
}
/** Stream every cached company-month straight into the dataset builder so
 *  memory stays at the typed arrays (about 20 bytes a bar). */
function loadLibrary(library) {
  const docs = library.filter((d) => fs.existsSync(path.join(BARS_DIR, d.id + ".gz")));
  const symbols = [...new Set(docs.map((d) => d.symbol))].sort();
  const b = E.datasetBuilder({ symbols, totalBars: docs.reduce((n, d) => n + (d.bars || 0), 0) + 1000, totalSymbolDays: docs.reduce((n, d) => n + (d.days || 25), 0) + 100, shared: true });
  let bars = 0, months = 0;
  for (const d of docs) {
    let compact; try { compact = JSON.parse(zlib.gunzipSync(fs.readFileSync(path.join(BARS_DIR, d.id + ".gz"))).toString()); } catch { continue; }
    for (const [date, rows] of Object.entries(compact)) { b.add(d.symbol, date, rows.map((r) => ({ i: r[0], o: r[1] / 100, h: r[2] / 100, l: r[3] / 100, c: r[4] / 100 }))); bars += rows.length; }
    months++;
    if (months % 500 === 0) STATE.phase = `loading the library (${months} of ${docs.length} company-months)`;
  }
  return { ds: b.finish(), bars, months };
}

/* ── the study: one grid, its results, its leaderboard ──────────────────── */
const STATE = { phase: "starting", control: null, dip: null, grid: null, ds: null, pairs: null, first: null, splitDay: 0, evaluated: null, results: 0, top: [], agg: null, applied: null, workers: [], effort: 0, window: "off", startedAtMs: Date.now(), rate: 0, lastAppliedAtMs: 0, datasetInfo: null, busy: 0 };
function windowNow(control) {
  const h = new Date().getHours(), s = control.nightStartHour, e = control.nightEndHour;
  const night = s === e ? true : s < e ? (h >= s && h < e) : (h >= s || h < e);
  return night ? "night" : "day";
}
function effortNow(control) { if (!control.enabled) return 0; return windowNow(control) === "night" ? control.nightEffortPct : control.dayEffortPct; }
function workersFor(effortPct) { const cores = os.cpus().length; return effortPct <= 0 ? 0 : Math.max(1, Math.min(cores, Math.round(cores * effortPct / 100))); }

function resultsFile(specHash) { return path.join(RESULTS_DIR, specHash + ".jsonl"); }
function scoreEntry(e, control) { return TUNER.scoreOf(e.train, control.minTrades); }
function rankTop(top) { top.sort((a, b) => (b.score == null ? -Infinity : b.score) - (a.score == null ? -Infinity : a.score)); top.forEach((e, i) => { e.rank = i + 1; }); return top; }
function record(entry) {
  const st = STATE;
  inflight.delete(entry.index);
  if ((st.evaluated[entry.index >> 3] >> (entry.index & 7)) & 1) return;   // already recorded
  st.evaluated[entry.index >> 3] |= 1 << (entry.index & 7); st.results++;
  entry.score = scoreEntry(entry, st.control);
  fs.appendFileSync(resultsFile(st.grid.specHash), JSON.stringify(entry) + "\n");
  addToBoard(entry);
}
function addToBoard(entry) {
  const st = STATE;
  if (entry.score != null) { st.top.push(entry); if (st.top.length > 400) { rankTop(st.top); st.top.length = 200; } }
  for (const a of st.grid.axes) {
    const v = entry.params[a.key], slot = st.agg[a.key][v] = st.agg[a.key][v] || { value: v, n: 0, sumScore: 0, sumTest: 0, sumTrades: 0, best: null, scored: 0 };
    slot.n++; slot.sumTest += entry.test.pnlUsd; slot.sumTrades += entry.train.trades;
    if (entry.score != null) { slot.scored++; slot.sumScore += entry.score; if (slot.best == null || entry.score > slot.best) slot.best = entry.score; }
  }
}
function loadResults() {
  const st = STATE; st.top = []; st.agg = {}; st.results = 0; st.evaluated = new Uint8Array(Math.ceil(st.grid.size / 8));
  for (const a of st.grid.axes) st.agg[a.key] = {};
  const f = resultsFile(st.grid.specHash);
  if (!fs.existsSync(f)) return;
  for (const line of fs.readFileSync(f, "utf8").split("\n")) {
    if (!line) continue;
    try { const e = JSON.parse(line); if (e.index < st.grid.size) { st.evaluated[e.index >> 3] |= 1 << (e.index & 7); st.results++; e.score = scoreEntry(e, st.control); addToBoard(e); } } catch { /* skip a torn line */ }
  }
  rankTop(st.top);
  log(`resumed ${st.results} evaluated sets for grid ${st.grid.specHash}`);
}
const inflight = new Set();
function isDone(i) { return ((STATE.evaluated[i >> 3] >> (i & 7)) & 1) || inflight.has(i); }
/** Next grid indexes to evaluate: neighbours of the current leaders first
 *  (one step along one axis), then the rest of the grid in a scrambled
 *  order so early results are representative. */
let cursor = 0, stride = 1;
function nextIndexes(n) {
  const st = STATE, out = [];
  rankTop(st.top);
  for (const leader of st.top.slice(0, 10)) {
    let mult = 1;
    for (const a of st.grid.axes) {
      const k = a.values.indexOf(leader.params[a.key]);
      for (const dk of [-1, 1]) { const kk = k + dk; if (kk < 0 || kk >= a.values.length) continue; const idx = leader.index + dk * mult; if (!isDone(idx) && !out.includes(idx)) out.push(idx); if (out.length >= n) return out; }
      mult *= a.values.length;
    }
  }
  const size = st.grid.size;
  let guard = 0;
  while (out.length < n && guard++ < size) {
    const idx = Number((BigInt(cursor) * BigInt(stride)) % BigInt(size)); cursor++;
    if (cursor > size) break;
    if (!isDone(idx) && !out.includes(idx)) out.push(idx);
  }
  return out;
}
function pickStride(size) { const cands = [7919, 104729, 1299709, 15485863, 32452843]; for (const c of cands) if (gcd(c, size) === 1 && c < size * 1000) return c % size || 1; return 1; }
function gcd(a, b) { while (b) { [a, b] = [b, a % b]; } return a; }

/* ── workers ────────────────────────────────────────────────────────────── */
const WORKER_SRC = `
const { parentPort, workerData } = require("worker_threads");
const E = require(workerData.enginePath);
const ds = workerData.ds, pairs = workerData.pairs, first = {};
for (const w of Object.keys(pairs)) first[w] = E.pairIndex(pairs[w], ds.count);
parentPort.on("message", (msg) => {
  const out = [];
  for (const job of msg.jobs) {
    try { const r = E.evaluate(ds, pairs, first, job.params, { splitDay: workerData.splitDay, budgetUsd: workerData.budgetUsd }); out.push({ index: job.index, params: job.params, train: r.train, test: r.test }); }
    catch (e) { out.push({ index: job.index, error: e.message }); }
  }
  parentPort.postMessage(out);
});`;
function spawnWorker() {
  const st = STATE;
  const w = new Worker(WORKER_SRC, { eval: true, workerData: { enginePath: path.join(__dirname, "dipTunerEngine.js"), ds: st.ds, pairs: st.pairs, splitDay: st.splitDay, budgetUsd: st.dip.budgetUsd } });
  w.busy = false;
  w.on("message", (out) => { for (const e of out) { if (e.error) { inflight.delete(e.index); log("evaluate failed", e.error); } else record(e); } w.busy = false; st.busy--; feed(); });
  w.on("error", (e) => { log("worker error", e.message); w.dead = true; st.workers = st.workers.filter((x) => x !== w); st.busy = Math.max(0, st.busy - 1); });
  return w;
}
function feed() {
  const st = STATE;
  if (!st.ds) return;
  for (const w of st.workers) {
    if (w.busy || w.dead) continue;
    const idxs = nextIndexes(BATCH);
    if (!idxs.length) { st.phase = "every practical variation evaluated"; return; }
    w.busy = true; st.busy++; for (const i of idxs) inflight.add(i);
    w.postMessage({ jobs: idxs.map((i) => ({ index: i, params: TUNER.paramsAt(st.grid, i) })) });
  }
}
function setWorkerCount(n) {
  const st = STATE;
  while (st.workers.length > n) { const w = st.workers.pop(); w.terminate(); if (w.busy) st.busy = Math.max(0, st.busy - 1); }
  if (!n) inflight.clear();
  while (st.workers.length < n) st.workers.push(spawnWorker());
  try { os.setPriority(process.pid, n && windowNow(st.control) === "day" ? 10 : 0); } catch { /* not allowed on every system */ }
  feed();
}

/* ── dataset build ──────────────────────────────────────────────────────── */
async function buildStudy(hello) {
  const st = STATE;
  st.phase = "loading the library into memory";
  const lib = loadLibrary(hello.library.filter((d) => d.bars > 0));
  if (!lib.months) throw new Error("the price library is empty; build it in the console first (Simulation → Historical price library)");
  log(`loaded ${lib.months} company-months, ${lib.bars.toLocaleString()} bars`);
  st.ds = lib.ds;
  const holdoutFrom = st.ds.dates.filter((d) => d >= monthsAgo(st.control.holdoutMonths))[0];
  st.splitDay = holdoutFrom ? st.ds.dates.indexOf(holdoutFrom) : st.ds.dates.length;
  st.datasetInfo = { symbols: st.ds.symbols.length, days: st.ds.dates.length, bars: st.ds.bars, from: st.ds.dates[0], to: st.ds.dates[st.ds.dates.length - 1], holdoutFrom: holdoutFrom || null, trainDays: st.splitDay, testDays: st.ds.dates.length - st.splitDay };
  log("dataset", JSON.stringify(st.datasetInfo));
  st.pairs = {};
  const windows = st.grid.axes.find((a) => a.key === "dropWindowMin") ? st.grid.axes.find((a) => a.key === "dropWindowMin").values : [st.grid.fixed.dropWindowMin];
  const minDrop = Math.min(...st.control.variables.dropBps.values, st.grid.fixed.dropBps || Infinity) / 100;
  for (const w of windows) {
    st.phase = `finding every candidate fall (window ${w} min)`;
    const t0 = Date.now();
    const cores = Math.max(1, workersFor(effortNow(st.control)) || 1);
    const parts = await Promise.all(splitRanges(st.ds.count, cores).map(([a, b]) => runPairs(w, minDrop, a, b)));
    st.pairs[w] = E.sharePairs(E.concatPairs(parts));
    log(`window ${w}: ${st.pairs[w].count.toLocaleString()} candidate falls in ${Math.round((Date.now() - t0) / 1000)} s`);
  }
  st.first = {}; for (const w of Object.keys(st.pairs)) st.first[w] = E.pairIndex(st.pairs[w], st.ds.count);
}
function splitRanges(n, parts) { const out = []; const step = Math.ceil(n / parts); for (let a = 0; a < n; a += step) out.push([a, Math.min(n, a + step)]); return out; }
function runPairs(windowMin, minDrop, from, to) {
  return new Promise((resolve, reject) => {
    const src = `const { parentPort, workerData } = require("worker_threads"); const E = require(workerData.enginePath); const p = E.computePairs(workerData.ds, workerData.windowMin, workerData.minDrop, workerData.from, workerData.to); parentPort.postMessage(p);`;
    const w = new Worker(src, { eval: true, workerData: { enginePath: path.join(__dirname, "dipTunerEngine.js"), ds: STATE.ds, windowMin, minDrop, from, to } });
    w.on("message", (p) => { resolve(p); w.terminate(); }); w.on("error", reject);
  });
}
function monthsAgo(months) { const d = new Date(); d.setMonth(d.getMonth() - months); return d.toISOString().slice(0, 10); }

/* ── reporting and automatic apply ──────────────────────────────────────── */
let lastReportAt = 0, lastResults = 0, lastRateAt = Date.now();
async function report(force) {
  const st = STATE;
  if (!force && Date.now() - lastReportAt < REPORT_EVERY_MS) return;
  lastReportAt = Date.now();
  const dt = (Date.now() - lastRateAt) / 3600000; if (dt > 0.01) { st.rate = Math.round((st.results - lastResults) / dt); lastResults = st.results; lastRateAt = Date.now(); }
  rankTop(st.top);
  const remaining = st.grid ? st.grid.size - st.results : 0;
  const status = { phase: st.phase, window: st.window, effortPct: st.effort, workers: st.workers.length, cores: os.cpus().length, host: os.hostname(), evaluated: st.results, gridSize: st.grid ? st.grid.size : 0, specHash: st.grid ? st.grid.specHash : null, ratePerHour: st.rate, etaHours: st.rate > 0 ? Math.round(remaining / st.rate * 10) / 10 : null, dataset: st.datasetInfo, startedAtMs: st.startedAtMs, best: st.top[0] ? { index: st.top[0].index, score: st.top[0].score, params: st.top[0].params, train: st.top[0].train, test: st.top[0].test } : null };
  const leaderboard = st.grid ? { specHash: st.grid.specHash, axes: st.grid.axes.map((a) => a.key), fixed: st.grid.fixed, minTrades: st.control.minTrades, evaluated: st.results, gridSize: st.grid.size, entries: st.top.slice(0, 100).map((e) => ({ rank: e.rank, index: e.index, score: e.score, params: e.params, train: e.train, test: e.test })) } : null;
  const outcomes = st.grid ? { specHash: st.grid.specHash, evaluated: st.results, byVariable: Object.fromEntries(st.grid.axes.map((a) => [a.key, { values: a.values.map((v) => { const s = st.agg[a.key][v]; return s ? { value: v, n: s.n, avgScore: s.scored ? Math.round(s.sumScore / s.scored) : null, avgTestPnl: Math.round(s.sumTest / s.n), avgTrades: Math.round(s.sumTrades / s.n), best: s.best } : { value: v, n: 0 }; }) }])) } : null;
  try {
    const r = await call("report", { status, leaderboard, outcomes });
    if (r && r.control) applyControl(r.control);
  } catch (e) { log("report failed:", e.message); }
  await maybeAutoApply();
}
async function maybeAutoApply() {
  const st = STATE;
  if (!st.control || !st.control.autoApply || !st.top.length) return;
  if (Date.now() - st.lastAppliedAtMs < 60 * 60000) return;
  const minTest = Math.max(20, Math.round(st.control.minTrades / 5));
  const cand = st.top.find((e) => e.score != null && e.test.pnlUsd > 0 && e.test.trades >= minTest);
  if (!cand) return;
  const cur = st.control.applied;
  if (cur && cur.specHash === st.grid.specHash && Number(cur.index) === cand.index) return;
  if (cur && cur.score != null && cur.specHash === st.grid.specHash && !(cand.score > cur.score * 1.05)) return;
  try {
    const r = await call("apply", { index: cand.index, specHash: st.grid.specHash, params: cand.params, score: cand.score, rank: cand.rank });
    st.lastAppliedAtMs = Date.now();
    if (r && r.applied) { st.control.applied = r.applied; log(`applied set #${cand.index} (score ${cand.score}, held-out ${cand.test.pnlUsd}) to the live dip lane`); }
  } catch (e) { log("apply failed:", e.message); }
}
function applyControl(raw) {
  const st = STATE;
  const control = TUNER.normalizeControl(raw);
  const prevHash = st.grid ? st.grid.specHash : null;
  st.control = control;
  const grid = TUNER.gridOf(control, st.dip || {});
  if (grid.specHash !== prevHash) {
    st.grid = grid; stride = pickStride(grid.size); cursor = 0;
    loadResults();
    if (prevHash) { log(`the variables changed: new grid of ${grid.size.toLocaleString()} sets`); st.needRebuild = true; }
  }
  const want = workersFor(effortNow(control));
  st.window = control.enabled ? windowNow(control) : "off"; st.effort = effortNow(control);
  if (st.ds && want !== st.workers.length) { setWorkerCount(want); log(`${st.window}: ${want} of ${os.cpus().length} cores`); }
}

/* ── main loop ──────────────────────────────────────────────────────────── */
async function main() {
  log("dip tuner runner · site", CFG.site, "· cores", os.cpus().length);
  let hello = await withRetry(() => call("hello"));
  STATE.dip = hello.dip;
  applyControl(hello.control);
  log(`grid: ${STATE.grid.size.toLocaleString()} variations across ${STATE.grid.axes.map((a) => a.key).join(", ")}`);
  await syncLibrary(hello.library);
  await buildStudy(hello);
  setWorkerCount(workersFor(effortNow(STATE.control)));
  STATE.phase = STATE.workers.length ? "evaluating" : (STATE.control.enabled ? "waiting for an active window" : "paused in the console");
  await report(true);
  let lastHello = Date.now();
  for (;;) {
    await sleep(POLL_EVERY_MS);
    if (STATE.workers.length && STATE.results < STATE.grid.size) STATE.phase = "evaluating";
    else if (STATE.results >= STATE.grid.size) STATE.phase = "every practical variation evaluated";
    else STATE.phase = STATE.control.enabled ? "waiting for an active window" : "paused in the console";
    try {
      if (Date.now() - lastHello > 3600000) { hello = await call("hello"); lastHello = Date.now(); STATE.dip = hello.dip; const got = await syncLibrary(hello.library); if (got.downloaded) STATE.needRebuild = true; applyControl(hello.control); }
      else applyControl((await call("report", { status: { phase: STATE.phase, window: STATE.window, effortPct: STATE.effort, workers: STATE.workers.length, cores: os.cpus().length, evaluated: STATE.results, gridSize: STATE.grid.size, specHash: STATE.grid.specHash, ratePerHour: STATE.rate, dataset: STATE.datasetInfo, startedAtMs: STATE.startedAtMs, best: STATE.top[0] ? { index: STATE.top[0].index, score: STATE.top[0].score, params: STATE.top[0].params, train: STATE.top[0].train, test: STATE.top[0].test } : null } })).control);
    } catch (e) { log("poll failed:", e.message); }
    if (STATE.needRebuild) { STATE.needRebuild = false; setWorkerCount(0); await buildStudy(hello); setWorkerCount(workersFor(effortNow(STATE.control))); }
    feed();
    await report(false);
  }
}
process.on("SIGINT", () => { log("stopping"); process.exit(0); });
main().catch((e) => { console.error("dip tuner stopped:", e.message); process.exit(1); });
