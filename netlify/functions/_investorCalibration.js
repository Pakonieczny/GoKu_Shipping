/* Investor_AI — chronological, embargoed calibration and untouched holdout gate. */
"use strict";

const crypto = require("crypto");
const A = require("./_investorAdmin");
const AL = require("./_investorAllocator");

function value(row, key = "mean") {
  if (!row) return null;
  const raw = row[key] != null ? row[key]
    : (key === "mean" ? row.portfolioNetBps : null);
  return Number.isFinite(Number(raw)) ? Number(raw) : null;
}

function cleanRows(rows) {
  const byDate = new Map();
  for (const r of rows || []) {
    const date = String(r && r.date || "").slice(0, 10);
    const mean = value(r, "mean");
    if (!/^\d{4}-\d{2}-\d{2}$/.test(date) || mean == null) continue;
    byDate.set(date, { ...r, date, mean,
      cost: value(r, "cost") ?? value(r, "portfolioCostBps") ?? 0,
      worstCaseMean: value(r, "worstCaseMean") ?? mean });
  }
  return [...byDate.values()].sort((a, b) => a.date.localeCompare(b.date));
}

/**
 * The split is by observed trading session, never random. Embargoed sessions
 * between partitions prevent an outcome from a multi-day position leaking
 * across adjacent train/calibration/holdout boundaries.
 */
function chronologicalSplit(rows, opts = {}) {
  const all = cleanRows(rows);
  const embargo = Math.max(0, Math.floor(opts.embargoSessions ?? 15));
  const minTrain = Math.max(1, Math.floor(opts.minTrain ?? 252));
  const minCalibration = Math.max(1, Math.floor(opts.minCalibration ?? 126));
  const minHoldout = Math.max(1, Math.floor(opts.minHoldout ?? 126));
  const needed = minTrain + minCalibration + minHoldout + 2 * embargo;
  if (all.length < needed) {
    return { pass: false, all, train: [], calibration: [], holdout: [], embargo,
      needed, reason: `${all.length} chronological sessions available; ${needed} required including embargoes` };
  }

  const usable = all.length - 2 * embargo;
  let nTrain = Math.max(minTrain, Math.floor(usable * (opts.trainFraction ?? 0.6)));
  let nCal = Math.max(minCalibration, Math.floor(usable * (opts.calibrationFraction ?? 0.2)));
  if (nTrain + nCal + minHoldout > usable) {
    nTrain = minTrain; nCal = minCalibration;
  }
  const train = all.slice(0, nTrain);
  const calibrationStart = nTrain + embargo;
  const calibration = all.slice(calibrationStart, calibrationStart + nCal);
  const holdoutStart = calibrationStart + nCal + embargo;
  const holdout = all.slice(holdoutStart);
  if (holdout.length < minHoldout) {
    return { pass: false, all, train, calibration, holdout, embargo, needed,
      reason: `only ${holdout.length} untouched sessions remain; ${minHoldout} required` };
  }
  return {
    pass: true, all, train, calibration, holdout, embargo, needed,
    boundaries: {
      train: [train[0].date, train.at(-1).date],
      calibration: [calibration[0].date, calibration.at(-1).date],
      holdout: [holdout[0].date, holdout.at(-1).date],
    },
  };
}

/** Return only data that is legally available for model/policy selection.
 *  The embargoed confirmation tail is described by dates/counts but its
 *  values are deliberately not returned. This makes it harder for a caller to
 *  call a holdout "untouched" after using it to choose the winner. */
function developmentOnly(rows, opts = {}) {
  const split = chronologicalSplit(rows, opts);
  if (!split.pass) return { pass: false, development: [], reason: split.reason,
    sessions: split.all.length, embargoSessions: split.embargo, needed: split.needed };
  return {
    pass: true,
    development: [...split.train, ...split.calibration]
      .sort((a, b) => a.date.localeCompare(b.date)),
    boundaries: { train: split.boundaries.train, calibration: split.boundaries.calibration },
    confirmation: {
      startDate: split.boundaries.holdout[0], endDate: split.boundaries.holdout[1],
      sessions: split.holdout.length, embargoSessions: split.embargo,
      dataThroughDate: split.all.at(-1).date,
    },
  };
}

function lowerBound(values, { alpha = 0.01, k = 8 } = {}) {
  const x = (values || []).map(Number).filter(Number.isFinite);
  if (x.length < 2) return { n: x.length, meanBps: null, hacSeBps: null,
    critical: null, lowerBoundBps: null, pass: false };
  const mean = x.reduce((a, b) => a + b, 0) / x.length;
  const variance = AL.hacMeanVariance(x);
  const se = Number.isFinite(variance) ? Math.sqrt(variance) : Infinity;
  const critical = -AL.zQuantile(alpha / Math.max(1, k));
  const lb = se === 0 ? mean : mean - critical * se;
  return { n: x.length, meanBps: Number(mean.toFixed(5)),
    hacSeBps: Number.isFinite(se) ? Number(se.toFixed(5)) : null,
    critical: Number(critical.toFixed(4)),
    lowerBoundBps: Number.isFinite(lb) ? Number(lb.toFixed(5)) : null,
    pass: Number.isFinite(lb) && lb > 0 };
}

/**
 * `net` already includes the modeled cost once. `doubleCost` subtracts the
 * observed friction a second time, so passage is not dependent on a perfect
 * slippage estimate. `worstCase` retains unresolved total-loss stresses.
 */
function calibrate(rows, opts = {}) {
  const split = chronologicalSplit(rows, opts);
  if (!split.pass) return { pass: false, calibrated: false, reason: split.reason,
    sessions: split.all.length, split: { embargo: split.embargo, needed: split.needed } };

  const train = lowerBound(split.train.map((r) => r.mean), opts);
  const calibration = lowerBound(split.calibration.map((r) => r.mean), opts);
  const holdout = lowerBound(split.holdout.map((r) => r.mean), opts);
  const doubleCost = lowerBound(split.holdout.map((r) => r.mean - (Number(r.cost) || 0)), opts);
  const worstCase = lowerBound(split.holdout.map((r) => Number(r.worstCaseMean)), opts);
  const pass = train.pass && calibration.pass && holdout.pass && doubleCost.pass && worstCase.pass;
  return {
    pass, calibrated: true, sessions: split.all.length, boundaries: split.boundaries,
    embargoSessions: split.embargo, train, calibration, holdout, doubleCost, worstCase,
    calibratedNetLowerBoundBps: pass ? Math.min(holdout.lowerBoundBps,
      doubleCost.lowerBoundBps, worstCase.lowerBoundBps) : null,
    reason: pass
      ? "positive selection-adjusted lower bound on untouched holdout under normal, doubled-cost, and unresolved-outcome stress"
      : "one or more chronological lower-bound gates are non-positive",
  };
}

function calibrateAll(dailyByVariant, opts = {}) {
  const variants = {};
  for (const [variantId, rows] of Object.entries(dailyByVariant || {})) {
    variants[variantId] = calibrate(rows, opts);
  }
  return { variants, passCount: Object.values(variants).filter((v) => v.pass).length };
}

function forwardLockTemplate({ experimentHash, leaderId, dataThroughDate,
  variantsHash, simulatorVersion, lockedAtMs = Date.now(), embargoSessions = 15,
  requiredSessions = 126 } = {}) {
  if (!/^[a-f0-9]{64}$/.test(String(experimentHash || "")) || !leaderId
      || !/^\d{4}-\d{2}-\d{2}$/.test(String(dataThroughDate || ""))) {
    throw new Error("forward lock requires experiment, leader, and data-through date");
  }
  const identity = { experimentHash, leaderId, dataThroughDate,
    variantsHash: variantsHash || null, simulatorVersion: simulatorVersion || null,
    embargoSessions: Math.max(0, Math.floor(embargoSessions)),
    requiredSessions: Math.max(30, Math.floor(requiredSessions)) };
  return { ...identity, lockHash: crypto.createHash("sha256")
    .update(JSON.stringify(identity)).digest("hex"), lockedAtMs: Number(lockedAtMs),
    state: "collecting_untouched_forward_paper" };
}

/**
 * Read only observations produced after the immutable lock date. The first
 * sessions are embargoed, and exactly the next requiredSessions are frozen;
 * later evidence cannot improve or damage a consumed confirmation window.
 */
function evaluateForward(rows, lock, opts = {}) {
  if (!lock || !/^\d{4}-\d{2}-\d{2}$/.test(String(lock.dataThroughDate || ""))) {
    return { pass: false, complete: false, state: "not_locked", reason: "forward policy is not locked" };
  }
  const afterLock = cleanRows(rows).filter((r) => r.date > lock.dataThroughDate);
  const embargo = Math.max(0, Math.floor(lock.embargoSessions ?? 15));
  const required = Math.max(30, Math.floor(lock.requiredSessions ?? 126));
  const available = Math.max(0, afterLock.length - embargo);
  if (available < required) return { pass: false, complete: false,
    state: "collecting_untouched_forward_paper", lockHash: lock.lockHash,
    postLockSessions: afterLock.length, embargoSessions: embargo,
    confirmationSessions: available, requiredSessions: required,
    progressPct: Number((Math.min(1, available / required) * 100).toFixed(1)),
    reason: `${available} untouched post-embargo sessions collected; ${required} required` };
  const confirmation = afterLock.slice(embargo, embargo + required);
  const boundOpts = { ...opts, k: Math.max(1, Number(opts.k) || 1) };
  const normal = lowerBound(confirmation.map((r) => r.mean), boundOpts);
  const doubleCost = lowerBound(confirmation.map((r) => r.mean - (Number(r.cost) || 0)), boundOpts);
  const worstCase = lowerBound(confirmation.map((r) => Number(r.worstCaseMean)), boundOpts);
  const pass = normal.pass && doubleCost.pass && worstCase.pass;
  return { pass, complete: true, state: pass ? "passed" : "failed",
    lockHash: lock.lockHash, postLockSessions: afterLock.length,
    embargoSessions: embargo, requiredSessions: required,
    boundaries: { firstPostLock: afterLock[0].date,
      confirmation: [confirmation[0].date, confirmation.at(-1).date] },
    normal, doubleCost, worstCase,
    calibratedNetLowerBoundBps: pass
      ? Math.min(normal.lowerBoundBps, doubleCost.lowerBoundBps, worstCase.lowerBoundBps) : null,
    reason: pass
      ? "locked winner passed untouched forward paper under normal, doubled-cost, and unresolved-outcome stress"
      : "locked forward confirmation completed with a non-positive stress lower bound" };
}

async function persist(experimentHash, result) {
  if (!/^[a-f0-9]{64}$/.test(String(experimentHash || ""))) {
    throw new Error("calibration persist requires experimentHash");
  }
  await A.col(A.COL.calibration).doc(experimentHash).set({
    experimentHash, ...result, updatedAt: A.FV.serverTimestamp(),
    ...A.envelope({ created_by: "calibration" }),
  }, { merge: true });
  return result;
}

async function read(experimentHash) {
  if (!/^[a-f0-9]{64}$/.test(String(experimentHash || ""))) return null;
  const s = await A.col(A.COL.calibration).doc(experimentHash).get();
  return s.exists ? s.data() : null;
}

module.exports = { value, cleanRows, chronologicalSplit, developmentOnly, lowerBound, calibrate,
  calibrateAll, forwardLockTemplate, evaluateForward, persist, read };
