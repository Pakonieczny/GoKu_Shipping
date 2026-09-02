/*  netlify/functions/_investorSufficiency.js
 *  ---------------------------------------------------------------------------
 *  Investor_AI — data sufficiency: how much the desk actually knows about a
 *  name at the instant it decides.
 *
 *  The gate stack answers "is anything wrong with this trade?". Under paper
 *  relaxation an ABSENCE (no dossier, no earnings window, no long history,
 *  no correlation regime reading) is admitted at reduced size, so the stack
 *  no longer answers "how much did we know?". This module does, with one
 *  0-100 score built from the inputs the decision actually had, and it is:
 *
 *    · recorded on every decision, order and closed trade;
 *    · used to size exploratory entries (thin data → smaller, never zero, so
 *      the desk still learns from thin-data names instead of ignoring them);
 *    · used as a mild ordering penalty when the desk has more qualified
 *      candidates than it will take this scan;
 *    · split out on the scoreboard, so the question "do thin-data trades
 *      do worse?" is answered by outcomes rather than assumed.
 *
 *  It never admits a candidate the gates refused and never touches the strict
 *  verdict. Weights are declared here, not tuned: they are the prior about
 *  which inputs matter most, and the scoreboard split is how that prior gets
 *  checked.
 * ---------------------------------------------------------------------------
 */

"use strict";

const DEFAULT_POLICY = Object.freeze({
  version: "data-sufficiency-v2",
  weights: Object.freeze({
    intradayBars: 20,     // enough SAME-SESSION bars to measure the move
    dailyHistory: 20,     // long-horizon context: volatility, trend, tails
    earningsWindow: 15,   // a dated hazard the desk can see coming
    intelligence: 15,     // fresh, complete company dossier
    correlationRegime: 10,// COR3M known
    liquidity: 10,        // measured (not provisional) dollar volume
    priceQuality: 10,     // feed grade
  }),
  buckets: Object.freeze({ high: 75, medium: 45 }),   // low is everything below medium
  sizeFloor: 0.6,
  orderingPenaltyBpsPerPoint: 0.2,
  barsForFullCredit: 60,
  barsMinimum: 24,
  historyDaysForFullCredit: 252,
});
const WEIGHTS = DEFAULT_POLICY.weights;
const BUCKETS = DEFAULT_POLICY.buckets;
const SIZE_FLOOR = DEFAULT_POLICY.sizeFloor;

function clamp01(x) { return Math.max(0, Math.min(1, Number(x) || 0)); }
function clampNum(v, lo, hi, dflt) { const n = Number(v); return Number.isFinite(n) ? Math.min(hi, Math.max(lo, n)) : dflt; }

/** The policy in force, read from the FROZEN strategy (exploratoryAuto.activity
 *  .sufficiency) with clamped defaults. Anything not in the strategy hash is
 *  a default here, so the effective policy is reproducible from the two. */
function policyFrom(strategy) {
  const raw = (strategy && strategy.exploratoryAuto && strategy.exploratoryAuto.activity
    && strategy.exploratoryAuto.activity.sufficiency) || {};
  const weights = {};
  for (const [k, dflt] of Object.entries(DEFAULT_POLICY.weights)) {
    weights[k] = clampNum(raw.weights && raw.weights[k], 0, 50, dflt);
  }
  return {
    version: typeof raw.version === "string" && raw.version ? raw.version : DEFAULT_POLICY.version,
    weights,
    buckets: { high: clampNum(raw.buckets && raw.buckets.high, 50, 100, DEFAULT_POLICY.buckets.high),
      medium: clampNum(raw.buckets && raw.buckets.medium, 0, 74, DEFAULT_POLICY.buckets.medium) },
    sizeFloor: clampNum(raw.sizeFloor, 0.25, 1, DEFAULT_POLICY.sizeFloor),
    orderingPenaltyBpsPerPoint: clampNum(raw.orderingPenaltyBpsPerPoint, 0, 1, DEFAULT_POLICY.orderingPenaltyBpsPerPoint),
    barsForFullCredit: Math.round(clampNum(raw.barsForFullCredit, 24, 390, DEFAULT_POLICY.barsForFullCredit)),
    barsMinimum: Math.round(clampNum(raw.barsMinimum, 6, 120, DEFAULT_POLICY.barsMinimum)),
    historyDaysForFullCredit: Math.round(clampNum(raw.historyDaysForFullCredit, 40, 1300, DEFAULT_POLICY.historyDaysForFullCredit)),
  };
}

/**
 * @param {object} i
 *   sessionBarCount     SAME-SESSION 5-minute bars (prior-session stored bars do not count)
 *   historyDays         daily bars of long-horizon history (null if none)
 *   historyOk           the history module accepted the context
 *   earningsKnown       a dated earnings window exists
 *   intelligenceState   "fresh_complete" | "present" | "none"
 *   cor3mKnown          COR3M is a finite number
 *   advMeasured         dollar volume measured from bars, not provisional
 *   grade               price quality grade A/B/C/F
 * @param {object} policy  from policyFrom(strategy); defaults when omitted
 */
function score(i = {}, policy = DEFAULT_POLICY) {
  const P = policy && policy.weights ? policy : DEFAULT_POLICY;
  const parts = {};
  const bars = Number(i.sessionBarCount != null ? i.sessionBarCount : i.barCount) || 0;
  const full = P.barsForFullCredit, min = P.barsMinimum;
  parts.intradayBars = bars >= full ? 1 : bars >= min ? 0.6 + 0.4 * (bars - min) / Math.max(1, full - min)
    : clamp01(bars / min) * 0.6;
  const days = Number(i.historyDays) || 0;
  parts.dailyHistory = !i.historyOk ? clamp01(days / 40) * 0.25
    : days >= P.historyDaysForFullCredit ? 1 : days >= 120 ? 0.75 : 0.5;
  parts.earningsWindow = i.earningsKnown ? 1 : 0;
  parts.intelligence = i.intelligenceState === "fresh_complete" ? 1
    : i.intelligenceState === "present" ? 0.45 : 0;
  parts.correlationRegime = i.cor3mKnown ? 1 : 0;
  parts.liquidity = i.advMeasured ? 1 : 0.4;
  parts.priceQuality = { A: 1, B: 1, C: 0.5 }[String(i.grade || "").toUpperCase()] || 0;
  let total = 0, weightTotal = 0;
  const components = {};
  for (const [k, w] of Object.entries(P.weights)) {
    const v = clamp01(parts[k]);
    components[k] = { weight: w, fraction: Number(v.toFixed(3)), points: Number((w * v).toFixed(1)) };
    total += w * v; weightTotal += w;
  }
  const s = weightTotal > 0 ? Math.round(100 * total / weightTotal) : 0;
  const bucket = s >= P.buckets.high ? "high" : s >= P.buckets.medium ? "medium" : "low";
  const missing = Object.entries(components).filter(([, c]) => c.fraction < 0.5).map(([k]) => k);
  return { score: s, bucket, components, missing,
    sizeMultiplier: Number((P.sizeFloor + (1 - P.sizeFloor) * s / 100).toFixed(3)),
    orderingPenaltyBps: Number(((100 - s) * P.orderingPenaltyBpsPerPoint).toFixed(1)),
    version: P.version, kind: "heuristic_coverage_score" };
}

function bucketOf(s, policy = DEFAULT_POLICY) {
  const n = Number(s);
  if (!Number.isFinite(n)) return null;
  const b = policy && policy.buckets ? policy.buckets : DEFAULT_POLICY.buckets;
  return n >= b.high ? "high" : n >= b.medium ? "medium" : "low";
}

/** Read the sufficiency recorded on an order/position/trade. */
function ofRecord(rec) {
  const ctx = rec && rec.decisionContext || {};
  const d = ctx.dataSufficiency || rec && rec.dataSufficiency || null;
  if (!d || !Number.isFinite(Number(d.score))) return null;
  return { score: Number(d.score), bucket: d.bucket || bucketOf(d.score), version: d.version || null };
}

module.exports = { DEFAULT_POLICY, WEIGHTS, BUCKETS, SIZE_FLOOR, policyFrom, score, bucketOf, ofRecord };
