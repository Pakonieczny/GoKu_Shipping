/* Investor_AI — development-only Deflated Sharpe Ratio and CSCV/PBO guards. */
"use strict";

const V = require("./_investorVariants");

const EULER_GAMMA = 0.5772156649015329;

function mean(values) { return values.length ? values.reduce((a, b) => a + b, 0) / values.length : 0; }
function variance(values, m = mean(values)) {
  return values.length > 1 ? values.reduce((a, x) => a + (x - m) ** 2, 0) / (values.length - 1) : 0;
}
function moments(values) {
  const x = values.map(Number).filter(Number.isFinite), n = x.length, m = mean(x);
  const sd = Math.sqrt(variance(x, m));
  if (!(sd > 0) || n < 3) return { n, mean: m, sd, skew: 0, kurtosis: 3 };
  const z = x.map((v) => (v - m) / sd);
  return { n, mean: m, sd,
    skew: n / ((n - 1) * (n - 2)) * z.reduce((a, q) => a + q ** 3, 0),
    kurtosis: n > 3
      ? ((n * (n + 1)) / ((n - 1) * (n - 2) * (n - 3))) * z.reduce((a, q) => a + q ** 4, 0)
        - (3 * (n - 1) ** 2) / ((n - 2) * (n - 3)) + 3
      : 3 };
}
function erf(x) {
  const sign = x < 0 ? -1 : 1, a = Math.abs(x);
  const t = 1 / (1 + 0.3275911 * a);
  const y = 1 - (((((1.061405429 * t - 1.453152027) * t) + 1.421413741) * t
    - 0.284496736) * t + 0.254829592) * t * Math.exp(-a * a);
  return sign * y;
}
function normalCdf(x) { return 0.5 * (1 + erf(x / Math.sqrt(2))); }

/* Acklam approximation; local to keep the research guard pure and auditable. */
function normalQuantile(q) {
  const a = [-39.6968302866538, 220.946098424521, -275.928510446969,
    138.357751867269, -30.6647980661472, 2.50662827745924];
  const b = [-54.4760987982241, 161.585836858041, -155.698979859887,
    66.8013118877197, -13.2806815528857];
  const c = [-0.00778489400243029, -0.322396458041136, -2.40075827716184,
    -2.54973253934373, 4.37466414146497, 2.93816398269878];
  const d = [0.00778469570904146, 0.32246712907004, 2.445134137143, 3.75440866190742];
  if (!(q > 0 && q < 1)) return q === 0 ? -Infinity : q === 1 ? Infinity : NaN;
  if (q < 0.02425) { const r = Math.sqrt(-2 * Math.log(q));
    return (((((c[0]*r+c[1])*r+c[2])*r+c[3])*r+c[4])*r+c[5])
      / ((((d[0]*r+d[1])*r+d[2])*r+d[3])*r+1); }
  if (q > 0.97575) return -normalQuantile(1 - q);
  const r = q - 0.5, s = r * r;
  return (((((a[0]*s+a[1])*s+a[2])*s+a[3])*s+a[4])*s+a[5])*r
    / (((((b[0]*s+b[1])*s+b[2])*s+b[3])*s+b[4])*s+1);
}

function expectedMaxNullSharpe(trials) {
  const k = Math.max(1, Math.floor(Number(trials) || 1));
  if (k <= 1) return 0;
  return (1 - EULER_GAMMA) * normalQuantile(1 - 1 / k)
    + EULER_GAMMA * normalQuantile(1 - 1 / (k * Math.E));
}

/** Bailey & López de Prado DSR probability, using every preregistered trial. */
function deflatedSharpe(values, { trials = V.VARIANTS.length, effectiveN = null } = {}) {
  const m = moments(values || []), n = Math.max(0, Math.min(m.n,
    effectiveN != null && Number.isFinite(Number(effectiveN)) ? Number(effectiveN) : m.n));
  if (n < 30 || !(m.sd > 0)) return { pass: false, status: "warming", observations: m.n,
    effectiveN: n, probability: 0, reason: "at least 30 non-degenerate development days required" };
  const sharpe = m.mean / m.sd;
  const benchmark = expectedMaxNullSharpe(trials) / Math.sqrt(Math.max(1, n - 1));
  const seTerm = Math.max(1e-12,
    (1 - m.skew * sharpe + ((m.kurtosis - 1) / 4) * sharpe * sharpe) / (n - 1));
  const probability = normalCdf((sharpe - benchmark) / Math.sqrt(seTerm));
  return { pass: probability >= 0.95 && sharpe > 0, status: "ready", observations: m.n,
    effectiveN: Number(n.toFixed(3)), trials: Math.max(1, Math.floor(trials)),
    dailySharpe: Number(sharpe.toFixed(5)), annualizedSharpe: Number((sharpe * Math.sqrt(252)).toFixed(4)),
    expectedMaxNullDailySharpe: Number(benchmark.toFixed(5)), skew: Number(m.skew.toFixed(4)),
    kurtosis: Number(m.kurtosis.toFixed(4)), probability: Number(probability.toFixed(6)), threshold: 0.95 };
}

function combinations(n, choose) {
  const out = [];
  const walk = (start, acc) => {
    if (acc.length === choose) { out.push(acc.slice()); return; }
    for (let i = start; i <= n - (choose - acc.length); i += 1) { acc.push(i); walk(i + 1, acc); acc.pop(); }
  };
  walk(0, []); return out;
}
function sharpe(values) { const m = mean(values), sd = Math.sqrt(variance(values, m)); return sd > 0 ? m / sd : (m > 0 ? 99 : m < 0 ? -99 : 0); }

/** Deterministic 8-block combinatorially symmetric cross-validation (70 splits). */
function probabilityBacktestOverfit(dailyByVariant, { blocks = 8, minAligned = 64 } = {}) {
  const ids = V.VARIANTS.map((v) => v.id);
  const maps = Object.fromEntries(ids.map((id) => [id, new Map((dailyByVariant[id] || [])
    .map((d) => [d.date, Number(d.mean != null ? d.mean : d.portfolioNetBps)]).filter((x) => Number.isFinite(x[1])))]));
  const dates = ids.length ? [...maps[ids[0]].keys()].filter((date) => ids.every((id) => maps[id].has(date))).sort() : [];
  if (dates.length < Math.max(minAligned, blocks * 4)) return { pass: false, status: "warming",
    alignedDays: dates.length, requiredAlignedDays: Math.max(minAligned, blocks * 4), pbo: null,
    reason: "insufficient complete aligned development portfolio-days" };
  const blockRows = Array.from({ length: blocks }, () => []);
  dates.forEach((date, i) => blockRows[Math.min(blocks - 1, Math.floor(i * blocks / dates.length))].push(date));
  const splits = combinations(blocks, blocks / 2), logits = [], selections = {};
  for (const chosen of splits) {
    const trainBlocks = new Set(chosen), trainDates = [], testDates = [];
    blockRows.forEach((rows, i) => (trainBlocks.has(i) ? trainDates : testDates).push(...rows));
    const trainScores = Object.fromEntries(ids.map((id) => [id, sharpe(trainDates.map((d) => maps[id].get(d)))]));
    const leader = ids.slice().sort((a, b) => trainScores[b] - trainScores[a] || a.localeCompare(b))[0];
    selections[leader] = (selections[leader] || 0) + 1;
    const testScores = Object.fromEntries(ids.map((id) => [id, sharpe(testDates.map((d) => maps[id].get(d)))]));
    const ordered = ids.slice().sort((a, b) => testScores[b] - testScores[a] || a.localeCompare(b));
    const rank = ordered.indexOf(leader) + 1;
    const percentile = (ids.length - rank + 1) / (ids.length + 1);
    logits.push(Math.log(percentile / (1 - percentile)));
  }
  const pbo = logits.filter((x) => x <= 0).length / Math.max(1, logits.length);
  return { pass: pbo <= 0.20, status: "ready", alignedDays: dates.length,
    blocks, splits: splits.length, pbo: Number(pbo.toFixed(6)), threshold: 0.20,
    medianLogit: Number([...logits].sort((a, b) => a - b)[Math.floor(logits.length / 2)].toFixed(5)),
    inSampleSelections: selections };
}

function researchGuard(dailyByVariant, { effectiveNByVariant = {}, trials = V.VARIANTS.length } = {}) {
  const dsrByVariant = Object.fromEntries(V.VARIANTS.map((v) => [v.id,
    deflatedSharpe((dailyByVariant[v.id] || []).map((d) => Number(d.mean != null ? d.mean : d.portfolioNetBps)),
      { trials, effectiveN: effectiveNByVariant[v.id] })]));
  const pbo = probabilityBacktestOverfit(dailyByVariant);
  return { schemaVersion: "development-overfit-guard-v1", developmentOnly: true,
    trialCount: trials, dsrByVariant, pbo };
}
function passesGuard(guard, leaderId) {
  return !!(guard && guard.dsrByVariant && guard.dsrByVariant[leaderId]
    && guard.dsrByVariant[leaderId].pass && guard.pbo && guard.pbo.pass);
}

module.exports = { EULER_GAMMA, moments, normalCdf, normalQuantile,
  expectedMaxNullSharpe, deflatedSharpe, combinations, probabilityBacktestOverfit,
  researchGuard, passesGuard };
