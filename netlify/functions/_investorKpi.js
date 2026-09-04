/*  netlify/functions/_investorKpi.js
 *  ---------------------------------------------------------------------------
 *  Investor_AI — the PURE KPI library (blueprint §16.3 definitions, §16.4
 *  portfolio formulas, and the kpi-daily.v1 aggregation shape).
 *
 *  No Firestore, no network, no clock: every date, count and `nowMs` is an
 *  input, so the same inputs always produce the same row.
 *
 *  NUMERIC CONVENTIONS (declared once, here):
 *    · Money/price/return inputs are canonical integer strings from
 *      _investorMoney (…Minor cents, …Micros, …Bps, …Ppm). JSON numbers are
 *      refused for those (NOT_CANONICAL_INTEGER). Structural counts (touches,
 *      meetings, bins) may be safe JS integers or canonical strings.
 *    · Every money / return / probability OUTPUT is an exact canonical string,
 *      rounded ONCE with ROUNDING.HALF_EVEN after exact rational arithmetic.
 *      Intermediate linking uses unbounded BigInt rationals (the money
 *      module's 2^120 bound applies to inputs and to the rounded outputs).
 *    · JS floating point is used ONLY for inherently real-valued statistics
 *      (Sharpe, Sortino, Calmar, deflated Sharpe, CRPS/Brier float mirrors,
 *      effective sample size). Such fields end in `Float` or sit inside a
 *      `statistics` object carrying `precision: "float64_display_only"`.
 *      Exact fields inside `statistics` keep their `Bps`/`Micros` suffix.
 *    · Returns: bps (10,000 = 100 %). Probabilities: ppm (1,000,000 = 1).
 *    · Ratios (…Ppm) are null, never 0, when the denominator is zero.
 *    · Dates are "YYYY-MM-DD" strings compared lexically; series are sorted.
 *
 *  PORTFOLIO CONVENTIONS (§16.4, echoed in portfolioStatistics().conventions):
 *    · Sampling: one NAV per trading day at the close.
 *    · Cash flows: time-weighted subperiod linking. A flow dated d (positive =
 *      contribution) adjusts the START of the subperiod ending on the first
 *      observed date ≥ d:  r_t = NAV_t / (NAV_{t-1} + flow) − 1. Flows dated
 *      on/before the first observation or after the last are ignored and
 *      counted in `ignoredCashFlows`.
 *    · TWR = Π(1 + r_t) − 1, linked as exact rationals, rounded once.
 *    · Drawdown is measured on the linked growth index (flow-neutral).
 *    · CVaR95 = mean of the worst ceil(5 % × n) daily returns (at least 1).
 *    · Annualised return = (1 + TWR)^(periodsPerYear / n) − 1, computed with
 *      an exact integer n-th root so the HALF_EVEN bps value is exact.
 *    · Sharpe/Sortino annualise by sqrt(periodsPerYear); Sharpe uses the
 *      sample (n − 1) standard deviation of excess returns; Sortino uses the
 *      full-sample downside deviation below the MAR.
 *    · Deflated Sharpe (Bailey & López de Prado 2014) uses the per-period SR,
 *      moment-based skew/kurtosis and the expected maximum null SR over
 *      `trials`; null with a reason below 30 observations.
 *    · Effective sample size = n × (1 − ρ₁) / (1 + ρ₁), ρ₁ = lag-1 autocorr.
 *    · missingDayPolicy: "carry_forward" fills a null navMinor with the prior
 *      NAV (a 0 bps day); "drop" removes it.
 *
 *  Exports: KPI_VERSION, KPI_LAYERS, KPI_DEFINITIONS, CODES,
 *    portfolioStatistics, crps, quantileLoss, brier, calibrationTable,
 *    matchedSelectionLift, missedOpportunityRate, abstentionQuality,
 *    triggerCapture, slippageBps, protectionLatency, integrityCounts,
 *    tradeExpectancy, concentration, unitEconomics, operatingCostDragBps,
 *    costPerIncrementalBp, unexplainedFlipRate, regressionRecurrence,
 *    ratioPpm, dailyAggregate, selfCheck
 * ---------------------------------------------------------------------------
 */

"use strict";

const M = require("./_investorMoney");

const KPI_VERSION = "kpi.v1";
const DAILY_SCHEMA = "kpi-daily.v1";
const FLOAT_MARK = "float64_display_only";
const { HALF_EVEN } = M.ROUNDING;
const PPM = M.PPM, BPS = M.BPS_PER_UNIT;
const MISSING_DAY_POLICIES = new Set(["carry_forward", "drop"]);

const CODES = Object.freeze({
  INPUT_INVALID: "INPUT_INVALID", DATE_INVALID: "DATE_INVALID", DATE_DUPLICATE: "DATE_DUPLICATE",
  NAV_NOT_POSITIVE: "NAV_NOT_POSITIVE", POLICY_UNKNOWN: "POLICY_UNKNOWN", SIDE_UNKNOWN: "SIDE_UNKNOWN",
  QUANTILES_NOT_MONOTONE: "QUANTILES_NOT_MONOTONE", PRICE_NOT_POSITIVE: "PRICE_NOT_POSITIVE",
  DIVIDE_BY_ZERO: "DIVIDE_BY_ZERO",
});
const err = (code, message) => Object.assign(new Error(message), { code });

/* ── input helpers ──────────────────────────────────────────────────────── */

const int = (v, name) => M.parseInteger(v, { name });                 // BigInt, bounded
const S = (x) => M.toCanonical(x);                                    // BigInt → wire string
const DATE_RE = /^\d{4}-\d{2}-\d{2}$/;
function dateOf(v, name) {
  if (typeof v !== "string" || !DATE_RE.test(v)) throw err(CODES.DATE_INVALID, `${name}: expected "YYYY-MM-DD", got ${JSON.stringify(v)}`);
  return v;
}
function list(v, name) {
  if (!Array.isArray(v)) throw err(CODES.INPUT_INVALID, `${name}: expected an array`);
  return v;
}
function bool(v, name) {
  if (typeof v !== "boolean") throw err(CODES.INPUT_INVALID, `${name}: expected a boolean`);
  return v;
}
/** Structural count: safe non-negative Number or canonical string → BigInt. */
function count(v, name) {
  const x = typeof v === "number" ? M.fromSafeInteger(v, { name }) : int(v, name);
  if (x < 0n) throw err(CODES.INPUT_INVALID, `${name}: count ${x} is negative`);
  return x;
}
const stats = (o) => ({ ...o, precision: FLOAT_MARK });

/* ── unbounded exact rationals (intermediate linking only) ──────────────── */

function gcd(a, b) { a = a < 0n ? -a : a; b = b < 0n ? -b : b; while (b) { [a, b] = [b, a % b]; } return a; }
function rat(num, den = 1n) {
  if (den === 0n) throw err(CODES.DIVIDE_BY_ZERO, "rational: zero denominator");
  if (den < 0n) { num = -num; den = -den; }
  const g = gcd(num, den) || 1n;
  return { num: num / g, den: den / g };
}
const ONE = rat(1n), ZERO = rat(0n);
const rAdd = (a, b) => rat(a.num * b.den + b.num * a.den, a.den * b.den);
const rSub = (a, b) => rat(a.num * b.den - b.num * a.den, a.den * b.den);
const rMul = (a, b) => rat(a.num * b.num, a.den * b.den);
const rDiv = (a, b) => rat(a.num * b.den, a.den * b.num);
const rCmp = (a, b) => { const l = a.num * b.den, r = b.num * a.den; return l < r ? -1 : l > r ? 1 : 0; };
/** THE single rounding step: n/d → BigInt, HALF_EVEN, then range-checked. */
function halfEven(n, d) {
  if (d < 0n) { n = -n; d = -d; }
  const q = n / d, r = n % d;
  if (r === 0n) return M.parseInteger(q);
  const away = q + (n < 0n ? -1n : 1n), twice = (r < 0n ? -r : r) * 2n;
  const out = twice > d ? away : twice < d ? q : (q % 2n === 0n ? q : away);
  return M.parseInteger(out);
}
const rRound = (r, scale = 1n) => halfEven(r.num * scale, r.den);
const rBps = (r) => rRound(r, BPS), rPpm = (r) => rRound(r, PPM);
const meanRat = (xs) => rat(xs.reduce((a, x) => a + x, 0n), BigInt(xs.length));
/** numerator / denominator in ppm, or null when the denominator is zero. */
function ratioPpm({ numerator, denominator } = {}) {
  const n = count(numerator, "numerator"), d = count(denominator, "denominator");
  return { numerator: S(n), denominator: S(d), ppm: d === 0n ? null : S(rPpm(rat(n, d))), reason: d === 0n ? "zero_denominator" : null };
}

/* ── float statistics (display only) ────────────────────────────────────── */

const mean = (xs) => xs.length ? xs.reduce((a, b) => a + b, 0) / xs.length : NaN;
const sampleSd = (xs, m = mean(xs)) => xs.length > 1 ? Math.sqrt(xs.reduce((a, x) => a + (x - m) ** 2, 0) / (xs.length - 1)) : NaN;
const bpsFloat = (b) => Number(b) / 1e4;
function erf(x) {
  const s = x < 0 ? -1 : 1, a = Math.abs(x), t = 1 / (1 + 0.3275911 * a);
  const y = 1 - (((((1.061405429 * t - 1.453152027) * t) + 1.421413741) * t - 0.284496736) * t + 0.254829592) * t * Math.exp(-a * a);
  return s * y;
}
const normalCdf = (x) => 0.5 * (1 + erf(x / Math.SQRT2));
function normalQuantile(p) {                                           // bisection on the monotone CDF
  let lo = -40, hi = 40;
  for (let i = 0; i < 200; i += 1) { const mid = (lo + hi) / 2; if (normalCdf(mid) < p) lo = mid; else hi = mid; }
  return (lo + hi) / 2;
}
const EULER_GAMMA = 0.5772156649015329;
function expectedMaxNullSharpe(trials) {
  const k = Math.max(1, Math.floor(Number(trials) || 1));
  return k <= 1 ? 0 : (1 - EULER_GAMMA) * normalQuantile(1 - 1 / k) + EULER_GAMMA * normalQuantile(1 - 1 / (k * Math.E));
}
/** Nearest-rank percentile on an ascending array of Numbers. */
const percentile = (sorted, q) => sorted.length ? sorted[Math.max(0, Math.ceil(q * sorted.length) - 1)] : null;

/* ── 1. KPI definitions (§16.3) ─────────────────────────────────────────── */

const KPI_LAYERS = Object.freeze(["evidence", "extraction", "research", "manager", "selection", "portfolio", "execution", "operations", "cost", "governance"]);
const k = (id, layer, name, definition, unit, target, notes) => Object.freeze({ id, layer, name, definition, unit, target, notes });
const KPI_DEFINITIONS = Object.freeze([
  k("universeCoverage", "evidence", "Universe coverage", "Names with a fresh dossier ÷ names in the eligible universe.", "ppm", "≥ 950000", "Fresh = within the lane's freshness window."),
  k("materialClaimSupport", "evidence", "Material claim support", "Material claims with at least one verified primary citation ÷ material claims.", "ppm", "1000000", "Unsupported material claims block a mandate."),
  k("freshnessByLane", "evidence", "Freshness by lane", "Per lane: items inside the freshness window ÷ items.", "ppm per lane", "≥ 990000 per lane", "Lanes: filings, prices, news, transcripts."),
  k("conflictRate", "evidence", "Conflict rate", "Claims contradicted by another admitted source ÷ claims.", "ppm", "≤ 20000", "A conflict is logged, never silently resolved."),
  k("numericReconciliation", "extraction", "Numeric reconciliation", "Extracted numbers that reconcile to the filing ÷ extracted numbers.", "ppm", "≥ 995000", "Exact string compare after unit normalisation."),
  k("highImpactRecall", "extraction", "High-impact recall", "High-impact events detected before the decision ÷ high-impact events.", "ppm", "≥ 990000", "Missed events are audited individually."),
  k("reuseEfficiency", "research", "Reuse efficiency", "Research jobs answered from cache ÷ research jobs.", "ppm", "informational", "Higher is cheaper; never traded against freshness."),
  k("distributionScore", "research", "Distribution score (CRPS)", "Mean discrete CRPS of terminal-price distributions, in bps of the realised price.", "bps", "declining", "See crps(); lower is better."),
  k("calibration", "research", "Calibration", "Per-bin forecast probability vs observed frequency; ECE weighted by count.", "ppm", "ECE ≤ 50000", "See calibrationTable()."),
  k("forecastError", "research", "Forecast error (pinball)", "Mean quantile (pinball) loss of the p10/p50/p90 forecast in bps of the realised price.", "bps", "declining", "See quantileLoss()."),
  k("sameEvidenceConsistency", "manager", "Same-evidence consistency", "Decision pairs with identical evidence that agree ÷ such pairs.", "ppm", "≥ 990000", "Replays with the same manifest hash."),
  k("mandateValidityClampRate", "manager", "Mandate validity clamp rate", "Mandates whose validity was clamped by policy ÷ mandates.", "ppm", "≤ 100000", "A high rate means Sol overreaches on horizon."),
  k("matchedSelectionLift", "selection", "Matched selection lift", "Mean return of selected names minus the mean of matched controls.", "bps", "> 0", "Matched on sector and size bucket by default."),
  k("missedOpportunityRate", "selection", "Missed opportunity rate", "Eligible unselected names above the threshold ÷ eligible unselected names.", "ppm", "informational", "Only tradable, risk-eligible names count."),
  k("abstentionQuality", "selection", "Abstention quality", "Errors avoided vs opportunities missed among abstentions; net bps of abstaining.", "count, bps", "net ≥ 0", "Value of an abstention is 0 − later return."),
  k("investmentReturn", "portfolio", "Investment return (TWR)", "Time-weighted return over the window, linked from daily subperiods.", "bps", "> benchmark", "§16.4 conventions."),
  k("ownerEconomicReturn", "portfolio", "Owner economic return", "Investment return minus operating cost drag.", "bps", "> 0", "What the owner actually keeps."),
  k("excessReturn", "portfolio", "Excess return", "TWR minus the benchmark buy-and-hold return over the same window.", "bps", "> 0", "Benchmark closes carried forward to NAV dates."),
  k("sharpeSortinoCalmar", "portfolio", "Sharpe / Sortino / Calmar", "Annualised risk-adjusted ratios of daily returns.", "float", "Sharpe ≥ 1 after 1y", "Display only; see conventions."),
  k("maxDrawdownCvar", "portfolio", "Max drawdown / CVaR95", "Worst peak-to-trough of the growth index; mean of the worst 5 % of days.", "bps", "MDD ≥ −2000", "Exact strings."),
  k("expectancyPayoffProfitFactor", "portfolio", "Expectancy / payoff / profit factor", "Mean P&L per closed trade; avg win ÷ avg loss; gross wins ÷ gross losses.", "minor, float", "PF > 1.3", "See tradeExpectancy()."),
  k("turnoverExposureConcentration", "portfolio", "Turnover / exposure / concentration", "Traded notional ÷ NAV; gross exposure ÷ NAV; top-N weights and HHI.", "bps, ppm", "policy limits", "See concentration()."),
  k("triggerCapture", "execution", "Trigger capture", "Touch detection ppm and fill capture ppm, reported separately.", "ppm", "≥ 990000 / ≥ 950000", "A touch is not a fill."),
  k("slippage", "execution", "Slippage", "Fill vs benchmark and vs authorised price, signed so positive is worse.", "bps", "≤ 10 vs benchmark", "Any fill outside the authorised price is an integrity failure."),
  k("protectionLatency", "execution", "Protection latency", "Seconds from fill to acknowledged protective order: p50/p95/max; unprotected intervals.", "seconds", "p95 ≤ 5, unprotected 0", "Null sample = never protected."),
  k("integrity", "operations", "Integrity", "Duplicate, orphan, rejected, stale, over-quantity and unprotected counts.", "count", "0", "Any non-zero total fails."),
  k("completionLatency", "operations", "Completion latency", "Seconds from job creation to completion: p50/p95/max.", "seconds", "p95 within SLA", "Same percentile convention as protectionLatency."),
  k("unitEconomics", "cost", "Unit economics", "Cost per meeting, name, research job, actionable / activated / fill-producing mandate.", "minor", "declining", "Never a single headline; null on a zero denominator."),
  k("operatingCostDragBps", "cost", "Operating cost drag", "Operating cost ÷ average NAV.", "bps", "≤ 50 per year", "Exact HALF_EVEN."),
  k("costPerIncrementalBp", "cost", "Cost per incremental bp", "Operating cost ÷ incremental net return bps.", "minor per bp", "declining", "Null when incremental return ≤ 0."),
  k("unexplainedFlipRate", "governance", "Unexplained flip rate", "Flips without new evidence or a declared change ÷ flips.", "ppm", "0", "A flip is prior ≠ current."),
  k("regressionRecurrence", "governance", "Regression recurrence", "Fixed error classes that recurred after the fix ÷ fixed error classes.", "ppm", "0", "Each recurrence reopens the class."),
]);

/* ── 2. portfolio statistics (§16.4) ────────────────────────────────────── */

/** floor(N^(1/n)) for BigInt N ≥ 0, n ≥ 1: float-seeded integer Newton from above. */
function iroot(N, n) {
  if (N < 2n || n === 1n) return N;
  const bin = N.toString(2), bits = bin.length;
  const log2N = Math.log2(Number("0b" + bin.slice(0, 53))) + Math.max(0, bits - 53);
  const seed = 2 ** (log2N / Number(n));
  let x = Number.isFinite(seed) ? BigInt(Math.floor(seed * (1 + 1e-9))) + 2n : 1n << BigInt(Math.ceil(bits / Number(n)));
  const n1 = n - 1n;
  for (;;) { const y = (n1 * x + N / x ** n1) / n; if (y >= x) return x; x = y; }
}
/** Exact HALF_EVEN bps of growth^(ppy/n) − 1 (growth = num/den > 0). */
function annualizedBps(growth, ppy, n) {
  const bitLen = (x) => x.toString(2).length;
  if (growth.num <= 0n) return { value: null, reason: "non_positive_growth" };
  if ((bitLen(growth.num) + bitLen(growth.den)) * ppy + 14 * n > 2000000) return { value: null, reason: "exact_annualization_too_large" };
  const P = BigInt(ppy), N = BigInt(n);
  const A = BPS ** N * growth.num ** P, B = growth.den ** P;          // (A/B)^(1/n) = 10000 × growth^(ppy/n)
  const q = iroot(A / B, N), c = (A * (1n << N)) - B * (2n * q + 1n) ** N;   // sign of A/B − (q + ½)^n
  const rounded = c > 0n ? q + 1n : c < 0n ? q : (q % 2n === 0n ? q : q + 1n);
  return { value: S(M.parseInteger(rounded - BPS)), reason: null };
}
/** Bailey & López de Prado deflated Sharpe on per-period excess returns (floats). */
function deflatedSharpe(excess, trials) {
  const n = excess.length;
  if (n < 30) return { deflatedSharpeFloat: null, deflatedSharpeReason: "insufficient_observations" };
  const m = mean(excess), sd = sampleSd(excess, m);
  if (!(sd > 0)) return { deflatedSharpeFloat: null, deflatedSharpeReason: "zero_variance" };
  const popSd = Math.sqrt(excess.reduce((a, x) => a + (x - m) ** 2, 0) / n);
  const skew = excess.reduce((a, x) => a + ((x - m) / popSd) ** 3, 0) / n;
  const kurt = excess.reduce((a, x) => a + ((x - m) / popSd) ** 4, 0) / n;
  const sr = m / sd, sr0 = expectedMaxNullSharpe(trials) / Math.sqrt(n - 1);
  const se = Math.sqrt(Math.max(1e-12, (1 - skew * sr + ((kurt - 1) / 4) * sr * sr) / (n - 1)));
  return { deflatedSharpeFloat: normalCdf((sr - sr0) / se), deflatedSharpeReason: null, expectedMaxNullSharpeFloat: sr0, skewFloat: skew, kurtosisFloat: kurt };
}
function effectiveSampleSize(xs) {
  const n = xs.length; if (n < 3) return n;
  const m = mean(xs), v = xs.reduce((a, x) => a + (x - m) ** 2, 0);
  if (!(v > 0)) return n;
  let c = 0; for (let i = 1; i < n; i += 1) c += (xs[i] - m) * (xs[i - 1] - m);
  const rho = c / v;
  return Math.min(n, Math.max(1, n * (1 - rho) / (1 + rho)));
}
/** Last close on or before `date` from an ascending [{date, closeMicros}] series. */
function closeOnOrBefore(series, date) {
  let hit = null;
  for (const b of series) { if (b.date <= date) hit = b.closeMicros; else break; }
  return hit;
}

function portfolioStatistics({ navSeries, cashFlows = [], riskFreeBpsAnnual = "0", marBpsAnnual = "0",
  benchmarkSeries = null, periodsPerYear = 252, missingDayPolicy = "carry_forward", trials = 1 } = {}) {
  if (!MISSING_DAY_POLICIES.has(missingDayPolicy)) throw err(CODES.POLICY_UNKNOWN, `missingDayPolicy ${JSON.stringify(missingDayPolicy)} unknown`);
  if (!Number.isSafeInteger(periodsPerYear) || periodsPerYear < 1) throw err(CODES.INPUT_INVALID, "periodsPerYear: expected a positive integer");
  const rf = int(riskFreeBpsAnnual, "riskFreeBpsAnnual"), mar = int(marBpsAnnual, "marBpsAnnual");
  const rows = list(navSeries, "navSeries").map((r, i) => ({ date: dateOf(r && r.date, `navSeries[${i}].date`),
    nav: r.navMinor == null ? null : int(r.navMinor, `navSeries[${i}].navMinor`) })).sort((a, b) => (a.date < b.date ? -1 : a.date > b.date ? 1 : 0));
  rows.forEach((r, i) => { if (i && r.date === rows[i - 1].date) throw err(CODES.DATE_DUPLICATE, `navSeries: duplicate date ${r.date}`); });
  const series = []; let carriedDays = 0, droppedDays = 0;
  for (const r of rows) {
    if (r.nav !== null) series.push(r);
    else if (missingDayPolicy === "carry_forward" && series.length) { series.push({ date: r.date, nav: series[series.length - 1].nav }); carriedDays += 1; }
    else droppedDays += 1;
  }
  const flowAt = new Map(); let ignoredCashFlows = 0;
  list(cashFlows, "cashFlows").forEach((f, i) => {
    const d = dateOf(f && f.date, `cashFlows[${i}].date`), a = int(f.amountMinor, `cashFlows[${i}].amountMinor`);
    const idx = series.findIndex((s) => s.date >= d);
    if (idx <= 0) ignoredCashFlows += 1; else flowAt.set(idx, (flowAt.get(idx) || 0n) + a);
  });
  let growth = ONE, peak = ONE, maxDd = ZERO;
  const daily = [];
  for (let i = 1; i < series.length; i += 1) {
    const start = series[i - 1].nav + (flowAt.get(i) || 0n);
    if (start <= 0n) throw err(CODES.NAV_NOT_POSITIVE, `navSeries: subperiod ending ${series[i].date} starts at ${start}`);
    const r = rat(series[i].nav - start, start);
    daily.push(rBps(r));
    growth = rMul(growth, rAdd(ONE, r));
    if (rCmp(growth, peak) > 0) peak = growth;
    const dd = rSub(rDiv(growth, peak), ONE);
    if (rCmp(dd, maxDd) < 0) maxDd = dd;
  }
  const n = daily.length, ppy = periodsPerYear;
  const twr = n ? rSub(growth, ONE) : null;
  const rets = daily.map(bpsFloat), rfP = bpsFloat(rf) / ppy, marP = bpsFloat(mar) / ppy;
  const excess = rets.map((x) => x - rfP), below = rets.map((x) => Math.min(x - marP, 0));
  const exMean = mean(excess), exSd = sampleSd(excess, exMean);
  const downside = n ? Math.sqrt(below.reduce((a, x) => a + x * x, 0) / n) : NaN;
  const ann = n ? annualizedBps(growth, ppy, n) : { value: null, reason: "no_observations" };
  const sorted = daily.slice().sort((a, b) => (a < b ? -1 : a > b ? 1 : 0));
  const worst = sorted.slice(0, Math.max(1, Math.ceil(n / 20)));
  const mddBps = rBps(maxDd), annF = ann.value === null ? null : bpsFloat(ann.value);
  const st = stats({
    sharpeFloat: n < 2 ? null : exSd > 0 ? (exMean / exSd) * Math.sqrt(ppy) : null,
    sharpeReason: n < 2 ? "insufficient_observations" : exSd > 0 ? null : "zero_variance",
    sortinoFloat: n && downside > 0 ? ((mean(rets) - marP) / downside) * Math.sqrt(ppy) : null,
    sortinoReason: !n ? "no_observations" : downside > 0 ? null : "zero_downside_deviation",
    calmarFloat: annF !== null && mddBps < 0n ? annF / Math.abs(bpsFloat(mddBps)) : null,
    calmarReason: annF === null ? ann.reason : mddBps < 0n ? null : "zero_drawdown",
    maxDrawdownBps: S(mddBps), cvar95Bps: n ? S(halfEven(worst.reduce((a, x) => a + x, 0n), BigInt(worst.length))) : null,
    annualizedReturnBps: ann.value, annualizedReturnReason: ann.reason,
    ...deflatedSharpe(excess, trials), effectiveSampleSize: effectiveSampleSize(rets), observations: n, trials,
  });
  let benchmarkReturnBps = null, benchmarkExcessBps = null, benchmarkReason = benchmarkSeries === null ? "no_benchmark" : null;
  if (benchmarkSeries !== null && n) {
    const bench = list(benchmarkSeries, "benchmarkSeries").map((b, i) => ({ date: dateOf(b && b.date, `benchmarkSeries[${i}].date`),
      closeMicros: int(b.closeMicros, `benchmarkSeries[${i}].closeMicros`) })).sort((a, b) => (a.date < b.date ? -1 : 1));
    const from = closeOnOrBefore(bench, series[0].date), to = closeOnOrBefore(bench, series[series.length - 1].date);
    if (from === null || to === null || from <= 0n) benchmarkReason = "benchmark_missing_at_window_edge";
    else { const b = M.returnBps({ fromMicros: from, toMicros: to, mode: HALF_EVEN }); benchmarkReturnBps = S(b); benchmarkExcessBps = S(rBps(twr) - b); }
  } else if (benchmarkSeries !== null) benchmarkReason = "no_observations";
  return {
    kpiVersion: KPI_VERSION,
    conventions: { mar: `${S(mar)} bps/yr`, riskFree: `${S(rf)} bps/yr`, samplingFrequency: "daily_close",
      cashFlowTreatment: "time_weighted_subperiod_linking", annualization: `sqrt(${ppy})`, missingDayPolicy,
      benchmark: benchmarkSeries === null ? null : "buy_and_hold_close_carried_forward", periodsPerYear: ppy },
    observations: n, timeWeightedReturnBps: twr ? S(rBps(twr)) : null, dailyReturns: daily.map(S), statistics: st,
    benchmarkReturnBps, benchmarkExcessBps, benchmarkReason, smallSampleWarning: n < 60,
    carriedDays, droppedDays, ignoredCashFlows,
  };
}

/* ── 3. forecast scoring ────────────────────────────────────────────────── */

/** Discrete CRPS = ∫ (F(x) − 1{x ≥ y})² dx with F the step CDF over the bucket
 *  terminal prices (support points) and y the realised price; integrated
 *  exactly over the partition {support} ∪ {y}. Units: micros of price. */
function crps({ buckets, realizedMicros } = {}) {
  const chk = M.assertPpmDistribution(buckets, { requireIds: false });
  if (!chk.ok) throw err(chk.error.code, chk.error.message);
  const y = int(realizedMicros, "realizedMicros");
  const pts = buckets.map((b, i) => ({ x: int(b.terminalPriceMicros, `buckets[${i}].terminalPriceMicros`), p: int(b.probabilityPpm) }));
  const grid = [...new Set([...pts.map((b) => b.x), y])].sort((a, b) => (a < b ? -1 : a > b ? 1 : 0));
  let total = 0n;                                                     // in micros × PPM²
  for (let i = 0; i + 1 < grid.length; i += 1) {
    const a = grid[i], F = pts.reduce((s, b) => (b.x <= a ? s + b.p : s), 0n), ind = a >= y ? PPM : 0n;
    total += (F - ind) ** 2n * (grid[i + 1] - a);
  }
  const micros = halfEven(total, PPM * PPM);
  return { crpsMicros: S(micros), crpsBpsOfRealized: y > 0n ? S(halfEven(total * BPS, PPM * PPM * y)) : null,
    statistics: stats({ crpsFloat: Number(total) / Number(PPM * PPM) / 1e6 }), convention: "discrete_step_cdf_over_bucket_support" };
}
/** Pinball loss ρ_τ(y − q) for τ ∈ {0.1, 0.5, 0.9}, exact in micros. */
function quantileLoss({ quantiles, realizedMicros } = {}) {
  const y = int(realizedMicros, "realizedMicros");
  const q = { p10: int(quantiles && quantiles.p10, "quantiles.p10"), p50: int(quantiles && quantiles.p50, "quantiles.p50"), p90: int(quantiles && quantiles.p90, "quantiles.p90") };
  if (!(q.p10 <= q.p50 && q.p50 <= q.p90)) throw err(CODES.QUANTILES_NOT_MONOTONE, "quantiles: p10 ≤ p50 ≤ p90 required");
  const TAU = { p10: 100000n, p50: 500000n, p90: 900000n };
  const loss = {}; let total = 0n;
  for (const key of ["p10", "p50", "p90"]) {
    const d = y - q[key], v = d >= 0n ? TAU[key] * d : (TAU[key] - PPM) * d;
    loss[`${key}LossMicros`] = S(halfEven(v, PPM)); total += v;
  }
  return { ...loss, totalLossMicros: S(halfEven(total, PPM)), totalLossBpsOfRealized: y > 0n ? S(halfEven(total * BPS, PPM * y)) : null,
    coverage: y < q.p10 ? "below_p10" : y > q.p90 ? "above_p90" : "within_p10_p90" };
}
/** Brier: (p − 1{occurred})² exact in ppm², float mirror under statistics. */
function brier({ probabilityPpm, occurred } = {}) {
  const p = int(probabilityPpm, "probabilityPpm");
  if (p < 0n || p > PPM) throw err(M.CODES.PPM_OUT_OF_RANGE, `probabilityPpm ${p} outside [0, ${PPM}]`);
  const e = p - (bool(occurred, "occurred") ? PPM : 0n);
  return { squaredErrorPpm2: S(e * e), statistics: stats({ brierFloat: Number(e * e) / Number(PPM * PPM) }) };
}
/** Per-bin mean forecast vs observed frequency (ppm) with counts; ECE weighted by count. */
function calibrationTable({ events, bins = 10 } = {}) {
  const B = Number(count(bins, "bins")); if (B < 1) throw err(CODES.INPUT_INVALID, "bins: expected ≥ 1");
  const acc = Array.from({ length: B }, (_, i) => ({ bin: i, lowerPpm: S(halfEven(PPM * BigInt(i), BigInt(B))), upperPpm: S(halfEven(PPM * BigInt(i + 1), BigInt(B))), count: 0n, sumPpm: 0n, occurred: 0n }));
  list(events, "events").forEach((e, i) => {
    const p = int(e && e.probabilityPpm, `events[${i}].probabilityPpm`), hit = bool(e.occurred, `events[${i}].occurred`);
    if (p < 0n || p > PPM) throw err(M.CODES.PPM_OUT_OF_RANGE, `events[${i}]: probabilityPpm ${p} out of range`);
    const a = acc[Math.min(B - 1, Number((p * BigInt(B)) / PPM))];
    a.count += 1n; a.sumPpm += p; if (hit) a.occurred += 1n;
  });
  const n = acc.reduce((s, a) => s + a.count, 0n); let eceNum = 0n;
  const rows = acc.map((a) => {
    if (a.count === 0n) return { bin: a.bin, lowerPpm: a.lowerPpm, upperPpm: a.upperPpm, count: "0", occurred: "0", meanForecastPpm: null, observedFrequencyPpm: null, gapPpm: null };
    const f = halfEven(a.sumPpm, a.count), o = halfEven(a.occurred * PPM, a.count), gap = o - f;
    eceNum += a.count * (gap < 0n ? -gap : gap);
    return { bin: a.bin, lowerPpm: a.lowerPpm, upperPpm: a.upperPpm, count: S(a.count), occurred: S(a.occurred), meanForecastPpm: S(f), observedFrequencyPpm: S(o), gapPpm: S(gap) };
  });
  return { bins: B, events: S(n), rows, expectedCalibrationErrorPpm: n ? S(halfEven(eceNum, n)) : null };
}

/* ── 4. selection ───────────────────────────────────────────────────────── */

/** Mean return of matched selected names minus the mean (over those names) of
 *  each name's matched-control mean. A selected name matches a control when
 *  every matchKey is present on both and equal (own symbol excluded). */
function matchedSelectionLift({ selected, controls, matchKeys = ["sector", "sizeBucket"] } = {}) {
  const ctl = list(controls, "controls").map((c, i) => ({ ...c, ret: int(c && c.returnBps, `controls[${i}].returnBps`) }));
  const unmatchedSelected = [], selRets = [], ctlMeans = [];
  list(selected, "selected").forEach((s, i) => {
    const ret = int(s && s.returnBps, `selected[${i}].returnBps`);
    const complete = list(matchKeys, "matchKeys").every((key) => s[key] !== undefined && s[key] !== null);
    const group = complete ? ctl.filter((c) => c.symbol !== s.symbol && matchKeys.every((key) => c[key] === s[key])) : [];
    if (!group.length) { unmatchedSelected.push(s.symbol); return; }
    selRets.push(ret); ctlMeans.push(meanRat(group.map((c) => c.ret)));
  });
  if (!selRets.length) return { liftBps: null, reason: "no_matched_selected", selectedMeanBps: null, matchedControlMeanBps: null, matchedCount: 0, unmatchedSelected, matchKeys };
  const selMean = meanRat(selRets), ctlMean = ctlMeans.reduce((a, r) => rAdd(a, r), ZERO);
  const ctlAvg = rat(ctlMean.num, ctlMean.den * BigInt(ctlMeans.length));
  return { liftBps: S(rRound(rSub(selMean, ctlAvg))), reason: null, selectedMeanBps: S(rRound(selMean)), matchedControlMeanBps: S(rRound(ctlAvg)),
    matchedCount: selRets.length, unmatchedSelected, matchKeys };
}
/** Eligible (tradable AND riskEligible) unselected names at/above the threshold ÷ eligible. */
function missedOpportunityRate({ unselected, thresholdBps } = {}) {
  const t = int(thresholdBps, "thresholdBps"); let eligible = 0n, notTradable = 0, notRiskEligible = 0; const missedSymbols = [];
  list(unselected, "unselected").forEach((u, i) => {
    const r = int(u && u.adjustedReturnBps, `unselected[${i}].adjustedReturnBps`);
    if (!bool(u.tradable, `unselected[${i}].tradable`)) { notTradable += 1; return; }
    if (!bool(u.riskEligible, `unselected[${i}].riskEligible`)) { notRiskEligible += 1; return; }
    eligible += 1n; if (r >= t) missedSymbols.push(u.symbol);
  });
  const missed = BigInt(missedSymbols.length);
  return { thresholdBps: S(t), eligibleCount: S(eligible), missedCount: S(missed), missedOpportunityPpm: eligible ? S(halfEven(missed * PPM, eligible)) : null,
    reason: eligible ? null : "no_eligible_unselected", missedSymbols, excludedNotTradable: notTradable, excludedNotRiskEligible: notRiskEligible };
}
/** Value of abstaining on a name = 0 − laterReturnBps. Labels (BUY = should
 *  have bought, AVOIDED = abstention dodged a loss) default to the realised sign. */
function abstentionQuality({ abstentions } = {}) {
  let errorsAvoided = 0, opportunitiesMissed = 0, neutral = 0, avoided = 0n, forgone = 0n, labelDisagreements = 0;
  const rows = list(abstentions, "abstentions");
  rows.forEach((a, i) => {
    const r = int(a && a.laterReturnBps, `abstentions[${i}].laterReturnBps`);
    const realised = r < 0n ? "AVOIDED" : r > 0n ? "BUY" : "NEUTRAL";
    const label = a.wouldHaveBeen === undefined || a.wouldHaveBeen === null ? realised : a.wouldHaveBeen;
    if (!["BUY", "AVOIDED", "NEUTRAL"].includes(label)) throw err(CODES.INPUT_INVALID, `abstentions[${i}].wouldHaveBeen: ${JSON.stringify(label)}`);
    if (label !== realised) labelDisagreements += 1;
    if (label === "AVOIDED") { errorsAvoided += 1; avoided -= r; } else if (label === "BUY") { opportunitiesMissed += 1; forgone += r; } else neutral += 1;
  });
  const net = avoided - forgone;
  return { count: rows.length, errorsAvoided, opportunitiesMissed, neutral, avoidedLossBps: S(avoided), forgoneGainBps: S(forgone), netBps: S(net),
    meanNetBps: rows.length ? S(halfEven(net, BigInt(rows.length))) : null, labelDisagreements };
}

/* ── 5. execution ───────────────────────────────────────────────────────── */

/** A touch is not a fill: detection ppm and capture ppm are reported apart. */
function triggerCapture({ touches, detectedTouches, eligibleFills, capturedFills } = {}) {
  const t = ratioPpm({ numerator: detectedTouches, denominator: touches }), f = ratioPpm({ numerator: capturedFills, denominator: eligibleFills });
  return { touches: t.denominator, detectedTouches: t.numerator, touchDetectionPpm: t.ppm, eligibleFills: f.denominator, capturedFills: f.numerator, fillCapturePpm: f.ppm };
}
/** Signed so that positive = worse for the desk. withinAuthorized is the integrity test. */
function slippageBps({ fillMicros, benchmarkMicros, authorizedMicros, side } = {}) {
  if (side !== "BUY" && side !== "SELL") throw err(CODES.SIDE_UNKNOWN, `side ${JSON.stringify(side)} must be BUY or SELL`);
  const fill = int(fillMicros, "fillMicros"), bench = int(benchmarkMicros, "benchmarkMicros"), auth = int(authorizedMicros, "authorizedMicros");
  if (bench <= 0n || auth <= 0n) throw err(CODES.PRICE_NOT_POSITIVE, "benchmarkMicros and authorizedMicros must be positive");
  const sgn = side === "BUY" ? 1n : -1n;
  return { side, vsBenchmarkBps: S(halfEven(sgn * (fill - bench) * BPS, bench)), vsAuthorizedBps: S(halfEven(sgn * (fill - auth) * BPS, auth)),
    withinAuthorized: side === "BUY" ? fill <= auth : fill >= auth };
}
/** Seconds are structural measurements (Numbers). null/non-finite/negative = never protected. */
function latencyPercentiles(samples, name, slaSeconds) {
  const finite = [], sla = slaSeconds == null ? null : Number(slaSeconds); let unprotected = 0;
  list(samples, name).forEach((x) => { if (typeof x === "number" && Number.isFinite(x) && x >= 0) finite.push(x); else unprotected += 1; });
  finite.sort((a, b) => a - b);
  return { count: finite.length, p50Seconds: percentile(finite, 0.5), p95Seconds: percentile(finite, 0.95), maxSeconds: finite.length ? finite[finite.length - 1] : null,
    unprotectedIntervals: unprotected, slaSeconds: sla, slaBreaches: sla === null ? null : finite.filter((x) => x > sla).length + unprotected };
}
const protectionLatency = ({ samples, slaSeconds = null } = {}) => latencyPercentiles(samples, "samples", slaSeconds);
function integrityCounts({ duplicate = 0, orphan = 0, rejected = 0, stale = 0, overQuantity = 0, unprotected = 0 } = {}) {
  const c = { duplicate, orphan, rejected, stale, overQuantity, unprotected }; let total = 0n; const byClass = {};
  for (const key of Object.keys(c)) { const v = count(c[key], key); byClass[key] = S(v); total += v; }
  return { byClass, total: S(total), pass: total === 0n };
}

/* ── 6. portfolio extras: trades and concentration ──────────────────────── */

function tradeExpectancy({ trades } = {}) {
  const pnl = list(trades, "trades").map((t, i) => int(t && t.pnlMinor, `trades[${i}].pnlMinor`));
  const wins = pnl.filter((x) => x > 0n), losses = pnl.filter((x) => x < 0n), n = BigInt(pnl.length);
  const grossWin = wins.reduce((a, x) => a + x, 0n), grossLoss = -losses.reduce((a, x) => a + x, 0n);
  const avgWin = wins.length ? halfEven(grossWin, BigInt(wins.length)) : null, avgLoss = losses.length ? halfEven(grossLoss, BigInt(losses.length)) : null;
  return { trades: pnl.length, wins: wins.length, losses: losses.length, winRatePpm: n ? S(halfEven(BigInt(wins.length) * PPM, n)) : null,
    expectancyMinor: n ? S(halfEven(pnl.reduce((a, x) => a + x, 0n), n)) : null, averageWinMinor: avgWin === null ? null : S(avgWin),
    averageLossMinor: avgLoss === null ? null : S(avgLoss), grossWinMinor: S(grossWin), grossLossMinor: S(grossLoss),
    statistics: stats({ payoffRatioFloat: avgWin !== null && avgLoss ? Number(avgWin) / Number(avgLoss) : null,
      profitFactorFloat: grossLoss ? Number(grossWin) / Number(grossLoss) : null }) };
}
/** Turnover and gross exposure vs NAV (bps); top-1/top-5 weights and HHI (ppm, Σw²). */
function concentration({ tradedNotionalMinor = "0", grossExposureMinor = "0", averageNavMinor, positions = [] } = {}) {
  const nav = int(averageNavMinor, "averageNavMinor");
  if (nav <= 0n) return { turnoverBps: null, grossExposureBps: null, top1WeightBps: null, top5WeightBps: null, hhiPpm: null, reason: "nav_not_positive" };
  const mv = list(positions, "positions").map((p, i) => { const v = int(p && p.marketValueMinor, `positions[${i}].marketValueMinor`); return v < 0n ? -v : v; })
    .sort((a, b) => (a < b ? 1 : a > b ? -1 : 0));
  const gross = mv.reduce((a, x) => a + x, 0n), top = (m) => S(halfEven(mv.slice(0, m).reduce((a, x) => a + x, 0n) * BPS, nav));
  const hhi = gross ? S(halfEven(mv.reduce((a, x) => a + x * x, 0n) * PPM, gross * gross)) : null;
  return { turnoverBps: S(M.bpsOf(int(tradedNotionalMinor, "tradedNotionalMinor"), nav, HALF_EVEN)), grossExposureBps: S(M.bpsOf(int(grossExposureMinor, "grossExposureMinor"), nav, HALF_EVEN)),
    top1WeightBps: top(1), top5WeightBps: top(5), hhiPpm: hhi, positions: mv.length, reason: null };
}

/* ── 7. cost ────────────────────────────────────────────────────────────── */

const UNIT_DENOMINATORS = [["meetings", "costPerMeetingMinor"], ["namesCovered", "costPerNameCoveredMinor"], ["researchJobs", "costPerResearchJobMinor"],
  ["actionableMandates", "costPerActionableMandateMinor"], ["activatedMandates", "costPerActivatedMandateMinor"], ["fillProducingMandates", "costPerFillProducingMandateMinor"]];
/** One figure per denominator, never a single headline; null on a zero denominator. */
function unitEconomics(input = {}) {
  const cost = int(input.costMinor, "costMinor"), out = { costMinor: S(cost), denominators: {} };
  for (const [key, outKey] of UNIT_DENOMINATORS) {
    const d = input[key] === undefined ? 0n : count(input[key], key);
    out.denominators[key] = S(d); out[outKey] = d ? S(halfEven(cost, d)) : null;
  }
  return out;
}
function operatingCostDragBps({ costMinor, averageNavMinor } = {}) {
  const cost = int(costMinor, "costMinor"), nav = int(averageNavMinor, "averageNavMinor");
  return nav > 0n ? { dragBps: S(M.bpsOf(cost, nav, HALF_EVEN)), reason: null } : { dragBps: null, reason: "nav_not_positive" };
}
function costPerIncrementalBp({ costMinor, incrementalNetReturnBps } = {}) {
  const cost = int(costMinor, "costMinor"), inc = int(incrementalNetReturnBps, "incrementalNetReturnBps");
  return inc > 0n ? { costPerBpMinor: S(halfEven(cost, inc)), reason: null } : { costPerBpMinor: null, reason: "no_incremental_return" };
}

/* ── 8. governance ──────────────────────────────────────────────────────── */

/** A flip is prior ≠ current; unexplained when neither newEvidence nor declaredChange. Rate is over flips. */
function unexplainedFlipRate({ decisions } = {}) {
  let flips = 0n, unexplained = 0n; const unexplainedSymbols = [];
  list(decisions, "decisions").forEach((d, i) => {
    if (d.prior === d.current) return;
    flips += 1n;
    if (!bool(d.newEvidence, `decisions[${i}].newEvidence`) && !bool(d.declaredChange, `decisions[${i}].declaredChange`)) { unexplained += 1n; unexplainedSymbols.push(d.symbol); }
  });
  return { decisions: decisions.length, flips: S(flips), unexplainedFlips: S(unexplained), unexplainedFlipPpm: flips ? S(halfEven(unexplained * PPM, flips)) : null,
    reason: flips ? null : "no_flips", unexplainedSymbols };
}
function regressionRecurrence({ errorClasses } = {}) {
  const fixed = list(errorClasses, "errorClasses").filter((e) => e.fixedInVersion !== undefined && e.fixedInVersion !== null);
  const recurred = fixed.filter((e, i) => bool(e.recurredAfter, `errorClasses[${i}].recurredAfter`));
  return { errorClasses: errorClasses.length, fixed: fixed.length, recurred: recurred.length, recurredIds: recurred.map((e) => e.id),
    recurrencePpm: fixed.length ? S(halfEven(BigInt(recurred.length) * PPM, BigInt(fixed.length))) : null, reason: fixed.length ? null : "no_fixed_classes" };
}

/* ── 9. daily aggregation (kpi-daily.v1) ────────────────────────────────── */

const pick = (o, keys) => Object.fromEntries(keys.map((key) => [key, o[key]]));
/** HALF_EVEN mean of one exact field across per-row results (nulls skipped). */
function meanExact(rows, fn, field) {
  const vals = list(rows, "rows").map(fn).map((r) => r[field]).filter((v) => v !== null).map((v) => BigInt(v));
  return vals.length ? S(halfEven(vals.reduce((a, x) => a + x, 0n), BigInt(vals.length))) : null;
}
const RATIO_KPIS = ["universeCoverage", "materialClaimSupport", "conflictRate", "numericReconciliation", "highImpactRecall",
  "reuseEfficiency", "sameEvidenceConsistency", "mandateValidityClampRate"];
const d = (id, fn, key = id) => [id, [key], (i) => fn(i[key])];
/** [id, requiredInputKeys, compute(inputs, ctx)] — the input key names ARE the contract of kpi-daily.v1. */
const AGGREGATE_SPEC = [
  ...RATIO_KPIS.map((id) => d(id, ratioPpm)),
  d("freshnessByLane", (x) => Object.fromEntries(Object.entries(x.lanes || {}).map(([lane, v]) => [lane, ratioPpm(v)]))),
  d("distributionScore", (x) => ({ forecasts: x.length, meanCrpsBpsOfRealized: meanExact(x, crps, "crpsBpsOfRealized") }), "forecasts"),
  d("calibration", (x) => calibrationTable({ events: x }), "calibrationEvents"),
  d("forecastError", (x) => ({ forecasts: x.length, meanPinballBpsOfRealized: meanExact(x, quantileLoss, "totalLossBpsOfRealized") }), "quantileForecasts"),
  d("matchedSelectionLift", matchedSelectionLift), d("missedOpportunityRate", missedOpportunityRate), d("abstentionQuality", abstentionQuality),
  ["investmentReturn", ["portfolio"], (i, c) => ({ ...pick(c.portfolio(), ["timeWeightedReturnBps", "observations", "smallSampleWarning"]), annualizedReturnBps: c.portfolio().statistics.annualizedReturnBps })],
  ["ownerEconomicReturn", ["portfolio", "operatingCostDrag"], (i, c) => {
    const twr = c.portfolio().timeWeightedReturnBps, drag = operatingCostDragBps(i.operatingCostDrag).dragBps;
    return { investmentReturnBps: twr, operatingCostDragBps: drag, ownerEconomicReturnBps: twr !== null && drag !== null ? S(BigInt(twr) - BigInt(drag)) : null };
  }],
  ["excessReturn", ["portfolio"], (i, c) => pick(c.portfolio(), ["timeWeightedReturnBps", "benchmarkReturnBps", "benchmarkExcessBps", "benchmarkReason"])],
  ["sharpeSortinoCalmar", ["portfolio"], (i, c) => stats(pick(c.portfolio().statistics, ["sharpeFloat", "sortinoFloat", "calmarFloat", "deflatedSharpeFloat", "effectiveSampleSize", "observations"]))],
  ["maxDrawdownCvar", ["portfolio"], (i, c) => pick(c.portfolio().statistics, ["maxDrawdownBps", "cvar95Bps", "observations"])],
  d("expectancyPayoffProfitFactor", (x) => tradeExpectancy({ trades: x }), "trades"),
  d("turnoverExposureConcentration", concentration, "exposure"),
  d("triggerCapture", triggerCapture),
  d("slippage", (x) => { const rows = list(x, "fills").map(slippageBps); return { fills: rows.length, meanVsBenchmarkBps: meanExact(rows, (r) => r, "vsBenchmarkBps"),
    meanVsAuthorizedBps: meanExact(rows, (r) => r, "vsAuthorizedBps"), outsideAuthorized: rows.filter((r) => !r.withinAuthorized).length }; }, "fills"),
  d("protectionLatency", protectionLatency),
  d("integrity", integrityCounts),
  d("completionLatency", (x) => latencyPercentiles(x.samples, "completionLatency.samples", x.slaSeconds)),
  d("unitEconomics", unitEconomics),
  d("operatingCostDragBps", operatingCostDragBps, "operatingCostDrag"),
  d("costPerIncrementalBp", costPerIncrementalBp),
  d("unexplainedFlipRate", (x) => unexplainedFlipRate({ decisions: x }), "decisions"),
  d("regressionRecurrence", (x) => regressionRecurrence({ errorClasses: x }), "errorClasses"),
];

/** One kpi-daily.v1 row from whatever inputs exist. A KPI whose inputs are
 *  absent is null and listed in `missing`; one whose inputs are malformed is
 *  null and listed in `errors` (the row itself never fails on one KPI). */
function dailyAggregate({ date, accountId, inputs = {} } = {}) {
  const has = (key) => inputs[key] !== undefined && inputs[key] !== null;
  let P; const ctx = { portfolio: () => (P || (P = portfolioStatistics(inputs.portfolio))) };
  const kpis = {}, missing = [], errors = [];
  for (const [id, keys, fn] of AGGREGATE_SPEC) {
    if (!keys.every(has)) { kpis[id] = null; missing.push(id); continue; }
    try { kpis[id] = fn(inputs, ctx); } catch (e) { kpis[id] = null; errors.push({ id, code: e.code || CODES.INPUT_INVALID, message: String(e.message || e) }); }
  }
  return { schema: DAILY_SCHEMA, kpiVersion: KPI_VERSION, date: dateOf(date, "date"), accountId: String(accountId),
    computedAtMs: Number.isSafeInteger(inputs.nowMs) ? inputs.nowMs : null, kpis, missing, errors };
}

/* ── 10. self check ─────────────────────────────────────────────────────── */

function selfCheck() {
  const failures = [];
  const t = (name, actual, expected) => { if (JSON.stringify(actual) !== JSON.stringify(expected)) failures.push({ name, actual, expected }); };
  const nav = (...xs) => xs.map((x, i) => ({ date: `2026-01-${String(i + 1).padStart(2, "0")}`, navMinor: x }));
  try {
    const ids = KPI_DEFINITIONS.map((x) => x.id);
    t("definitions", [ids.length, new Set(ids).size, KPI_DEFINITIONS.every((x) => KPI_LAYERS.includes(x.layer)), AGGREGATE_SPEC.map((x) => x[0]).sort().join() === ids.slice().sort().join()], [32, 32, true, true]);
    const flow = portfolioStatistics({ navSeries: nav("10000", "11000", "16500"), cashFlows: [{ date: "2026-01-03", amountMinor: "5000" }, { date: "2026-01-01", amountMinor: "999" }] });
    t("twr_one_flow", [flow.timeWeightedReturnBps, flow.dailyReturns, flow.ignoredCashFlows, flow.smallSampleWarning], ["1344", ["1000", "312"], 1, true]);
    const dd = portfolioStatistics({ navSeries: nav("100", "120", "90", "130") });
    t("max_drawdown", [dd.statistics.maxDrawdownBps, dd.timeWeightedReturnBps, dd.statistics.cvar95Bps], ["-2500", "3000", "-2500"]);
    const flat = portfolioStatistics({ navSeries: nav("10000", "10100", "10201", "10303") });
    t("sharpe_zero_variance", [flat.statistics.sharpeFloat, flat.statistics.sharpeReason, flat.statistics.sortinoReason], [null, "zero_variance", "zero_downside_deviation"]);
    t("annualized_exact_square", portfolioStatistics({ navSeries: nav("10000", "11000", "12100"), periodsPerYear: 4 }).statistics.annualizedReturnBps, "4641");
    t("annualized_exact_root", portfolioStatistics({ navSeries: nav("10000", "10500", "11000"), periodsPerYear: 252 }).statistics.annualizedReturnBps, String(Math.round((1.1 ** 126 - 1) * 1e4)));
    const long = portfolioStatistics({ navSeries: nav(...Array.from({ length: 40 }, (_, i) => String(100000 + i * 300 + ((i * 7919) % 11) * 90))), trials: 5 });
    t("deflated_sharpe_range", [typeof long.statistics.deflatedSharpeFloat === "number" && long.statistics.deflatedSharpeFloat >= 0 && long.statistics.deflatedSharpeFloat <= 1, long.statistics.precision], [true, FLOAT_MARK]);
    t("crps_point_mass", crps({ buckets: [{ probabilityPpm: "1000000", terminalPriceMicros: "50000000" }], realizedMicros: "50000000" }).crpsMicros, "0");
    t("crps_two_point", crps({ buckets: [{ probabilityPpm: "500000", terminalPriceMicros: "60000000" }, { probabilityPpm: "500000", terminalPriceMicros: "40000000" }], realizedMicros: "50000000" }).crpsMicros, "5000000");
    t("quantile_loss", quantileLoss({ quantiles: { p10: "90000000", p50: "100000000", p90: "110000000" }, realizedMicros: "105000000" }).totalLossMicros, "4500000");
    const b = brier({ probabilityPpm: "250000", occurred: false });
    t("brier", [b.squaredErrorPpm2, b.statistics.brierFloat], ["62500000000", 0.0625]);
    const cal = calibrationTable({ events: [{ probabilityPpm: "100000", occurred: true }, { probabilityPpm: "100000", occurred: false }, { probabilityPpm: "950000", occurred: true }] });
    t("calibration_bins", [cal.expectedCalibrationErrorPpm, cal.rows[1].count, cal.rows[1].observedFrequencyPpm, cal.rows[9].gapPpm, cal.rows[0].meanForecastPpm], ["283333", "2", "500000", "50000", null]);
    const lift = matchedSelectionLift({ selected: [{ symbol: "A", returnBps: "500", sector: "T", sizeBucket: "L" }, { symbol: "B", returnBps: "100", sector: "F", sizeBucket: "S" }],
      controls: [{ symbol: "X", returnBps: "100", sector: "T", sizeBucket: "L" }, { symbol: "Y", returnBps: "300", sector: "T", sizeBucket: "L" }, { symbol: "Z", returnBps: "900", sector: "F", sizeBucket: "L" }] });
    t("matched_lift", [lift.liftBps, lift.matchedCount, lift.unmatchedSelected], ["300", 1, ["B"]]);
    t("missed_opportunity", missedOpportunityRate({ unselected: [{ symbol: "A", adjustedReturnBps: "600", tradable: true, riskEligible: true }, { symbol: "B", adjustedReturnBps: "600", tradable: false, riskEligible: true },
      { symbol: "C", adjustedReturnBps: "100", tradable: true, riskEligible: true }], thresholdBps: "500" }).missedOpportunityPpm, "500000");
    const ab = abstentionQuality({ abstentions: [{ symbol: "A", laterReturnBps: "-200" }, { symbol: "B", laterReturnBps: "300", wouldHaveBeen: "BUY" }, { symbol: "C", laterReturnBps: "0" }] });
    t("abstention", [ab.errorsAvoided, ab.opportunitiesMissed, ab.neutral, ab.netBps], [1, 1, 1, "-100"]);
    const tc = triggerCapture({ touches: 10, detectedTouches: 8, eligibleFills: "4", capturedFills: "3" });
    t("trigger_capture", [tc.touchDetectionPpm, tc.fillCapturePpm], ["800000", "750000"]);
    const sl = slippageBps({ fillMicros: "10050000", benchmarkMicros: "10000000", authorizedMicros: "10020000", side: "BUY" });
    t("slippage", [sl.vsBenchmarkBps, sl.vsAuthorizedBps, sl.withinAuthorized], ["50", "30", false]);
    const pl = protectionLatency({ samples: [1, 2, 3, null, 10], slaSeconds: 5 });
    t("protection_latency", [pl.p50Seconds, pl.p95Seconds, pl.maxSeconds, pl.unprotectedIntervals, pl.slaBreaches], [2, 10, 10, 1, 2]);
    t("integrity", [integrityCounts({}).pass, integrityCounts({ duplicate: 1 }).total], [true, "1"]);
    const ue = unitEconomics({ costMinor: "1000", meetings: 4, researchJobs: 0 });
    t("unit_economics", [ue.costPerMeetingMinor, ue.costPerResearchJobMinor, "costPerNameCoveredMinor" in ue, ue.costPerNameCoveredMinor], ["250", null, true, null]);
    t("cost_drag", operatingCostDragBps({ costMinor: "1000", averageNavMinor: "1000000" }).dragBps, "10");
    t("cost_per_bp", [costPerIncrementalBp({ costMinor: "1000", incrementalNetReturnBps: "0" }).costPerBpMinor, costPerIncrementalBp({ costMinor: "1000", incrementalNetReturnBps: "40" }).costPerBpMinor], [null, "25"]);
    t("flip_rate", unexplainedFlipRate({ decisions: [{ symbol: "A", prior: "BUY", current: "HOLD", newEvidence: false, declaredChange: false }, { symbol: "B", prior: "HOLD", current: "HOLD" },
      { symbol: "C", prior: "BUY", current: "AVOID", newEvidence: true, declaredChange: false }, { symbol: "D", prior: "HOLD", current: "BUY", newEvidence: false, declaredChange: true }] }).unexplainedFlipPpm, "333333");
    t("regression", regressionRecurrence({ errorClasses: [{ id: "e1", fixedInVersion: "v2", recurredAfter: true }, { id: "e2", fixedInVersion: "v2", recurredAfter: false }, { id: "e3", fixedInVersion: null }] }).recurrencePpm, "500000");
    const row = dailyAggregate({ date: "2026-09-04", accountId: "acct", inputs: { nowMs: 1757000000000, triggerCapture: { touches: 10, detectedTouches: 8, eligibleFills: 4, capturedFills: 3 }, operatingCostDrag: { costMinor: "1000", averageNavMinor: "1000000" }, integrity: { duplicate: "x" } } });
    t("daily_aggregate", [row.schema, row.computedAtMs, row.kpis.triggerCapture.touchDetectionPpm, row.kpis.operatingCostDragBps.dragBps, Object.keys(row.kpis).length, row.missing.length, row.errors[0].id, row.missing.includes("investmentReturn")],
      [DAILY_SCHEMA, 1757000000000, "800000", "10", 32, 29, "integrity", true]);
  } catch (e) { failures.push({ name: "threw", code: e.code || null, message: String(e.message || e) }); }
  return { pass: failures.length === 0, failures };
}

module.exports = {
  KPI_VERSION, KPI_LAYERS, KPI_DEFINITIONS, CODES,
  portfolioStatistics, crps, quantileLoss, brier, calibrationTable,
  matchedSelectionLift, missedOpportunityRate, abstentionQuality,
  triggerCapture, slippageBps, protectionLatency, integrityCounts,
  tradeExpectancy, concentration, unitEconomics, operatingCostDragBps, costPerIncrementalBp,
  unexplainedFlipRate, regressionRecurrence, ratioPpm, dailyAggregate, selfCheck,
};
