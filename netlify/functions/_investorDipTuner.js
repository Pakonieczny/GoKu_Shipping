/*  netlify/functions/_investorDipTuner.js
 *  Shared definitions for the dip-rule tuner: the variables it may sweep,
 *  the schedule/effort control the console edits, and the documents the
 *  local runner (scripts/dipTuner.js, on the operator's own computer)
 *  reports into. No Firestore access happens here except through the
 *  admin handle callers pass in. No model gateway.
 *
 *  Documents (collection InvestorAI_DipTuner):
 *    control      what the operator asked for (schedule, effort, variables, auto-apply, runner key hash)
 *    status       the runner's heartbeat and progress
 *    leaderboard  the best parameter sets so far, most successful first
 *    variables    what every value of every variable did on average
 */
"use strict";
const crypto = require("crypto");

const COL = "InvestorAI_DipTuner";
const VERSION = "dip-tuner.v1";

/** Every setting in the buy → size → sell decision chain, with the
 *  practical values the sweep tries. Values are whole numbers in the same
 *  units the dip settings use. */
const VARIABLES = Object.freeze([
  { key: "dropBps",              group: "buy",  label: "Fall needed",                 unit: "bps",        values: [75, 100, 150, 200, 300] },
  { key: "dropWindowMin",        group: "buy",  label: "Fall must happen within",     unit: "min",        values: [15, 30, 60] },
  { key: "zMinTenths",           group: "buy",  label: "How unusual the fall must be",unit: "tenths of σ",values: [20, 25, 30, 40] },
  { key: "speedFactorPct",       group: "buy",  label: "Faster fall → deeper & longer settle", unit: "%", values: [0, 50, 100] },
  { key: "noEntryBeforeMin",     group: "buy",  label: "No buying after the open for",unit: "min",        values: [0, 15, 30, 45, 60] },
  { key: "settleBars",           group: "buy",  label: "Steady bars before buying",   unit: "bars",       values: [2, 3, 5] },
  { key: "stabilitySigmaTenths", group: "buy",  label: "Steady band above the low",   unit: "tenths of σ",values: [10, 20, 30] },
  { key: "minTradeUsd",          group: "size", label: "Smallest trade",              unit: "$",          values: [5000] },
  { key: "maxTradeUsd",          group: "size", label: "Largest trade",               unit: "$",          values: [10000, 25000] },
  { key: "retracePct",           group: "sell", label: "Sell target",                 unit: "% of fall",  values: [50, 65, 80, 100] },
  { key: "stopPct",              group: "sell", label: "Stop below the low",          unit: "% of fall",  values: [30, 60, 100] },
  { key: "maxHoldMin",           group: "sell", label: "Give up after",               unit: "min",        values: [30, 60, 90, 150] },
  { key: "cooldownMin",          group: "sell", label: "Pause after a trade",         unit: "min",        values: [20] },
]);
const VARIABLE_KEYS = VARIABLES.map((v) => v.key);
const GROUP_LABELS = Object.freeze({ buy: "Whether to buy", size: "How much to buy", sell: "When to sell" });

const DEFAULT_CONTROL = Object.freeze({
  enabled: false,
  nightStartHour: 22,       // runner's local clock
  nightEndHour: 7,
  nightEffortPct: 90,       // share of CPU cores the runner may use at night
  dayEffortPct: 25,         // and during the day
  autoApply: false,         // copy the most successful settings into the live dip lane
  holdoutMonths: 12,        // the newest months are never tuned on; they are the honest test
  minTrades: 150,           // a parameter set with fewer trades on the tuning period is not trusted
  variables: {},            // key → { tune: bool, values: [...] }
});
const CONTROL_BOUNDS = Object.freeze({ nightStartHour: [0, 23], nightEndHour: [0, 23], nightEffortPct: [5, 100], dayEffortPct: [0, 100], holdoutMonths: [3, 36], minTrades: [10, 5000] });

function num(v, fb) { const n = Number(v); return Number.isFinite(n) ? n : fb; }
function clampInt(v, lo, hi, fb) { return Math.min(hi, Math.max(lo, Math.round(num(v, fb)))); }

/** Bounds each variable's values must respect (the dip lane's own bounds). */
function variableBounds() {
  const DIP = require("./_investorDipReversal");
  return DIP.BOUNDS;
}
function normalizeControl(raw) {
  const r = raw && typeof raw === "object" ? raw : {};
  const c = { ...DEFAULT_CONTROL };
  c.enabled = !!r.enabled; c.autoApply = !!r.autoApply;
  for (const k of Object.keys(CONTROL_BOUNDS)) { const [lo, hi] = CONTROL_BOUNDS[k]; c[k] = clampInt(r[k], lo, hi, DEFAULT_CONTROL[k]); }
  const bounds = variableBounds();
  const vars = {};
  for (const v of VARIABLES) {
    const given = r.variables && r.variables[v.key];
    const [lo, hi] = bounds[v.key] || [-1e9, 1e9];
    let values = Array.isArray(given && given.values) ? given.values.map((x) => clampInt(x, lo, hi, v.values[0])) : v.values.slice();
    values = [...new Set(values)].sort((a, b) => a - b).slice(0, 12);
    if (!values.length) values = v.values.slice();
    vars[v.key] = { tune: given ? given.tune !== false : true, values };
  }
  c.variables = vars;
  c.keyHash = typeof r.keyHash === "string" ? r.keyHash : null;
  c.keyIssuedAtMs = num(r.keyIssuedAtMs, null);
  c.version = num(r.version, 0);
  c.updatedAtMs = num(r.updatedAtMs, null);
  c.applied = r.applied && typeof r.applied === "object" ? r.applied : null;   // the last set the tuner applied
  return c;
}
/** The grid: every combination of the tuned variables' values; untuned
 *  variables hold their first value (or the live setting when given). */
function gridOf(control, base = {}) {
  const axes = [];
  const fixed = {};
  for (const v of VARIABLES) {
    const spec = control.variables[v.key];
    if (spec.tune && spec.values.length > 1) axes.push({ key: v.key, values: spec.values });
    else fixed[v.key] = base[v.key] != null ? base[v.key] : spec.values[0];
  }
  const size = axes.reduce((n, a) => n * a.values.length, 1);
  return { axes, fixed, size, specHash: crypto.createHash("sha256").update(JSON.stringify({ axes, fixed, v: VERSION })).digest("hex").slice(0, 16) };
}
/** Grid index → parameter set (mixed radix). */
function paramsAt(grid, index) {
  const out = { ...grid.fixed };
  let i = index;
  for (const a of grid.axes) { out[a.key] = a.values[i % a.values.length]; i = Math.floor(i / a.values.length); }
  return out;
}
/** Parameter set → grid index, or -1 when it is off the grid. */
function indexOf(grid, params) {
  let index = 0, mult = 1;
  for (const a of grid.axes) { const k = a.values.indexOf(Number(params[a.key])); if (k < 0) return -1; index += k * mult; mult *= a.values.length; }
  return index;
}

/* ── the runner key: shown once, only its hash is stored ── */
function issueKey() { return "dtk_" + crypto.randomBytes(24).toString("base64url"); }
function keyHash(key) { return crypto.createHash("sha256").update(String(key)).digest("hex"); }
function keyMatches(key, hash) {
  if (!key || !hash) return false;
  const a = Buffer.from(keyHash(key)), b = Buffer.from(String(hash));
  return a.length === b.length && crypto.timingSafeEqual(a, b);
}

/* ── scoring: what "most successful" means ── */
/** Profit on the tuning period after a penalty for the deepest drawdown,
 *  so a set that made the same money with less pain ranks higher. Sets with
 *  too few trades to trust score as "not trusted" (null). */
function scoreOf(train, minTrades) {
  if (!train || !(train.trades >= minTrades)) return null;
  return Math.round((train.pnlUsd - 0.5 * train.maxDrawdownUsd) * 100) / 100;
}

/* ── documents ── */
async function readControl(D) { const s = await D.col(COL).doc("control").get(); return normalizeControl(s.exists ? s.data() : {}); }
async function writeControl(D, patch, nowMs = Date.now()) {
  const ref = D.col(COL).doc("control");
  await D.runTransaction(async (tx) => {
    const s = await tx.get(ref); const cur = s.exists ? s.data() : {};
    tx.set(ref, { ...cur, ...patch, version: (Number(cur.version) || 0) + 1, updatedAtMs: nowMs }, { merge: false });
  });
  return readControl(D);
}
async function readDoc(D, name) { const s = await D.col(COL).doc(name).get(); return s.exists ? s.data() : null; }
async function writeDoc(D, name, data) { await D.col(COL).doc(name).set({ ...data, version: VERSION, updatedAtMs: Date.now() }, { merge: false }); }

/** Everything the console shows in one read. */
async function view(D, ctrlDip) {
  const [control, status, leaderboard, variables] = await Promise.all([readControl(D), readDoc(D, "status"), readDoc(D, "leaderboard"), readDoc(D, "variables")]);
  const grid = gridOf(control, ctrlDip || {});
  const nowMs = Date.now();
  const online = !!(status && status.lastSeenMs && nowMs - Number(status.lastSeenMs) < 3 * 60000);
  const { keyHash: _h, ...safeControl } = control;
  return {
    control: { ...safeControl, hasKey: !!control.keyHash },
    variables: VARIABLES.map((v) => ({ ...v, group: v.group, groupLabel: GROUP_LABELS[v.group], live: ctrlDip && ctrlDip[v.key] != null ? ctrlDip[v.key] : null, ...control.variables[v.key] })),
    grid: { size: grid.size, axes: grid.axes.map((a) => a.key), specHash: grid.specHash },
    runner: { online, ...(status || {}), lastSeenMs: status ? status.lastSeenMs || null : null },
    leaderboard: leaderboard && leaderboard.specHash === grid.specHash ? leaderboard : (leaderboard ? { ...leaderboard, stale: true } : null),
    outcomes: variables && variables.specHash === grid.specHash ? variables : null,
    scoring: "Score = profit on the tuning period minus half of its worst drawdown. The newest months are held out and never tuned on; their result is shown beside every set as the honest check.",
    asOfMs: nowMs,
  };
}

/** Copy a parameter set into the live dip lane settings (paper). Only the
 *  tuner's variables change; budget, symbols and the on/off switch stay. */
async function applyParams(D, { params, source, actorId = "dip-tuner", reason = "dip tuner", nowMs = Date.now() }) {
  const V2 = require("./_investorApiV2"), DIP = require("./_investorDipReversal");
  const cur = await D.col(D.COL.control).doc("control").get();
  const ctrl = cur.exists ? cur.data() : {};
  const prior = DIP.normalizeSettings(ctrl.dip);
  const picked = {}; for (const k of VARIABLE_KEYS) if (params[k] != null) picked[k] = params[k];
  const next = DIP.normalizeSettings({ ...prior, ...picked });
  const dip = { ...next, version: (Number(prior.version) || 0) + 1, updatedAtMs: nowMs, updatedBy: actorId, tunedBy: { ...source, atMs: nowMs } };
  await V2.transitionControl(D, { expectedVersion: null, patch: { dip }, action: "dipTunerApply", actorId, reason, nowMs, correlationId: null, mutationId: null });
  try { V2.forgetMemo(""); } catch (e) { /* memo is best effort */ }
  const applied = { ...source, params: picked, atMs: nowMs, by: actorId };
  await writeControl(D, { applied }, nowMs);
  return { dip, applied };
}

module.exports = { applyParams, COL, VERSION, VARIABLES, VARIABLE_KEYS, GROUP_LABELS, DEFAULT_CONTROL, CONTROL_BOUNDS, normalizeControl, gridOf, paramsAt, indexOf, issueKey, keyHash, keyMatches, scoreOf, readControl, writeControl, readDoc, writeDoc, view };
