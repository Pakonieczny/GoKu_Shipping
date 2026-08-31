/*  netlify/functions/_investorAllocator.js  (v2.0)
 *  ---------------------------------------------------------------------------
 *  Investor_AI — conservative full-information comparison of frozen variants.
 *
 *  WHAT IT DOES IN ONE LINE
 *  ------------------------------------------------------------------------
 *  Selects at most one incumbent policy after selection-corrected, serially
 *  robust inference.  Comparison weights are diagnostics, never capital
 *  allocations; all variants receive the same opportunity stream in shadow.
 *
 *  WHAT THE DISPLAY WEIGHTS MEAN
 *  ------------------------------------------------------------------------
 *  Every frozen policy is simulated on the same opportunity union. Softmax
 *  weights summarize their selection-track lower bounds for the dashboard;
 *  they never allocate cash, suppress an arm, or create bandit feedback bias.
 *  The only executable output is zero or one leader after every absolute,
 *  incumbent-relative, power, provenance, and chronological-holdout gate.
 *
 *  THE SKEPTICAL PRIOR
 *  ------------------------------------------------------------------------
 *  Every variant starts from "probably zero edge". Evidence has to drag it
 *  away from zero. Three wins out of four does not become a 75% edge; it
 *  becomes "still probably about zero, slightly less sure". This shrinkage is
 *  the single most important line of defence against fitting noise, and it is
 *  why the allocator gets more reliable with data rather than more overfit.
 *
 *  THE POWER GATE
 *  ------------------------------------------------------------------------
 *  Below the required evidence threshold the allocator refuses to name a
 *  leader. Equal diagnostic weights are not an equal capital split. Acting on
 *  an underpowered result is exactly how a system convinces itself it learned.
 * ---------------------------------------------------------------------------
 */

"use strict";

const V = require("./_investorVariants");

/* ── how much evidence is enough? ──────────────────────────────────────── */
/**
 * n = (t * sd / effect)^2 — the standard sample size for detecting a mean.
 * With a ~30bp edge against ~250bp per-trade noise and the t>3 hurdle the
 * literature demands, this lands near 625 effective observations.
 */
function requiredN({ effectBps = 30, sdBps = 250, t = 3.0 } = {}) {
  return Math.ceil(Math.pow((t * sdBps) / effectBps, 2));
}

/**
 * THE SELECTION-CORRECTED GATE.
 *
 * requiredN() answers "how many days to test ONE strategy". This system does
 * something harder: it races a frozen family and promotes the best. The best
 * of several zero-skill strategies looks good by chance alone, so the evidence bar must rise with
 * the number of candidates (Bailey & López de Prado 2014, "The Deflated
 * Sharpe Ratio"; White 2000; Hansen 2005).
 *
 * The standard correction: replace the single-test z with the Bonferroni
 * z at alpha/K, and add a power term so a real edge is actually detectable:
 *
 *     n = ((z_{alpha/K} + z_power) * sd / effect)^2
 *
 * At alpha=0.04, K=14, and 90% power the current family requires 1,137
 * day-clustered observations versus the naive 625. The policies are highly
 * correlated because they see the same days, so the paired incumbent test
 * below performs the fine discrimination while this remains the conservative
 * selection gate.
 */
function zQuantile(q) {
  // Acklam's rational approximation — good to ~1e-9, no dependencies
  const a = [-3.969683028665376e+01, 2.209460984245205e+02, -2.759285104469687e+02,
             1.383577518672690e+02, -3.066479806614716e+01, 2.506628277459239e+00];
  const b = [-5.447609879822406e+01, 1.615858368580409e+02, -1.556989798598866e+02,
             6.680131188771972e+01, -1.328068155288572e+01];
  const c = [-7.784894002430293e-03, -3.223964580411365e-01, -2.400758277161838e+00,
             -2.549732539343734e+00, 4.374664141464968e+00, 2.938163982698783e+00];
  const d = [7.784695709041462e-03, 3.224671290700398e-01, 2.445134137142996e+00,
             3.754408661907416e+00];
  const pl = 0.02425;
  if (q < pl) { const u = Math.sqrt(-2 * Math.log(q));
    return (((((c[0]*u+c[1])*u+c[2])*u+c[3])*u+c[4])*u+c[5]) / ((((d[0]*u+d[1])*u+d[2])*u+d[3])*u+1); }
  if (q > 1 - pl) return -zQuantile(1 - q);
  const u = q - 0.5, t = u * u;
  return (((((a[0]*t+a[1])*t+a[2])*t+a[3])*t+a[4])*t+a[5])*u / (((((b[0]*t+b[1])*t+b[2])*t+b[3])*t+b[4])*t+1);
}

function requiredNSelect({ effectBps = 30, sdBps = 250, alpha = 0.04,
  k = V.VARIANTS.length, power = 0.9 } = {}) {
  const zAlpha = -zQuantile(alpha / k);       // upper-tail critical value
  const zPow = -zQuantile(1 - power);
  return Math.ceil(Math.pow(((zAlpha + zPow) * sdBps) / effectBps, 2));
}

/**
 * THE PAIRED PROMOTION TEST.
 *
 * Every variant scores the same names on the same days, so their daily P&L
 * series are strongly correlated — which is hostile to naive "eight
 * independent races" inference but a gift to a matched design: differencing
 * challenger minus incumbent day by day cancels the shared market noise, and
 * the question "is the challenger actually better than what we are running"
 * is answered on the differences directly. This is the correct object for
 * promotion; the diagnostic evidence weights only say who looks best, while this says whether
 * the difference is real.
 */
function dayValue(d) {
  const v = d && (d.mean != null ? d.mean : d.meanNetBps);
  return Number.isFinite(Number(v)) ? Number(v) : null;
}

/** Newey-West variance of the sample mean, with Bartlett weights. */
function hacMeanVariance(values, lag) {
  const x = (values || []).map(Number).filter(Number.isFinite);
  const n = x.length;
  if (n < 2) return Infinity;
  const mean = x.reduce((a, b) => a + b, 0) / n;
  const u = x.map((v) => v - mean);
  const L = Math.max(0, Math.min(n - 1,
    lag == null ? Math.floor(4 * Math.pow(n / 100, 2 / 9)) : Math.floor(lag)));
  let longRun = u.reduce((a, v) => a + v * v, 0) / n;
  for (let h = 1; h <= L; h++) {
    let cov = 0;
    for (let t = h; t < n; t++) cov += u[t] * u[t - h];
    cov /= n;
    longRun += 2 * (1 - h / (L + 1)) * cov;
  }
  return Math.max(0, longRun) / n;
}

/**
 * Newey-West variance of a WEIGHTED mean.
 *
 * `aggregateDays({ discounted:true })` estimates an exponentially weighted
 * mean.  Passing the unweighted series to hacMeanVariance() estimates the
 * uncertainty of a different statistic and makes the recent-regime estimate
 * look more precise than its ~166-session effective memory supports.  With
 * normalized weights a[t], Var(sum a[t]x[t]) is estimated directly from
 * a[t]a[t-h]u[t]u[t-h].  Equal weights reduce to hacMeanVariance().
 */
function weightedHacMeanVariance(values, weights, lag) {
  const pairs = (values || []).map((v, i) => [Number(v), Number(weights && weights[i])])
    .filter(([v, w]) => Number.isFinite(v) && Number.isFinite(w) && w > 0);
  const n = pairs.length;
  if (n < 2) return Infinity;
  const sw = pairs.reduce((sum, x) => sum + x[1], 0);
  if (!(sw > 0)) return Infinity;
  const x = pairs.map((p) => p[0]);
  const a = pairs.map((p) => p[1] / sw);
  const mean = x.reduce((sum, value, i) => sum + value * a[i], 0);
  const u = x.map((value) => value - mean);
  const L = Math.max(0, Math.min(n - 1,
    lag == null ? Math.floor(4 * Math.pow(n / 100, 2 / 9)) : Math.floor(lag)));
  let variance = u.reduce((sum, value, i) => sum + a[i] * a[i] * value * value, 0);
  for (let h = 1; h <= L; h += 1) {
    let covariance = 0;
    for (let t = h; t < n; t += 1) {
      covariance += a[t] * a[t - h] * u[t] * u[t - h];
    }
    variance += 2 * (1 - h / (L + 1)) * covariance;
  }
  return Math.max(0, variance);
}

function pairedTest(challengerDays, incumbentDays,
                    { minShared = 60, alpha = 0.04, k = 8, lag = null,
                      minAbsoluteEdgeBps = 0, discountGamma = null } = {}) {
  const inc = new Map((incumbentDays || []).map((d) => [d.date, dayValue(d)]));
  const diffs = [];
  for (const d of challengerDays || []) {
    const cv = dayValue(d), iv = inc.get(d.date);
    if (cv != null && iv != null) diffs.push(cv - iv);
  }
  if (diffs.length < minShared) {
    return { pass: false, n: diffs.length,
             note: `only ${diffs.length} shared days — needs ${minShared} before the comparison means anything` };
  }
  const weights = diffs.map((_, i) => Number(discountGamma) > 0 && Number(discountGamma) < 1
    ? Math.pow(Number(discountGamma), diffs.length - 1 - i) : 1);
  const sw = weights.reduce((a, b) => a + b, 0);
  const m = diffs.reduce((a, b, i) => a + b * weights[i], 0) / sw;
  const variance = weightedHacMeanVariance(diffs, weights, lag);
  const se = Number.isFinite(variance) ? Math.sqrt(variance) : Infinity;
  const t = se > 0 && Number.isFinite(se) ? m / se : (m !== 0 ? Math.sign(m) * 99 : 0);
  const critical = -zQuantile(alpha / Math.max(1, k));
  const lower = m - critical * (Number.isFinite(se) ? se : Infinity);
  const pass = m > minAbsoluteEdgeBps && lower > minAbsoluteEdgeBps;
  return {
    pass, t: Number(t.toFixed(2)), n: diffs.length,
    meanDiffBps: Number(m.toFixed(2)),
    hacSeBps: Number(se.toFixed(3)),
    lowerBoundBps: Number(lower.toFixed(3)),
    critical: Number(critical.toFixed(3)),
    note: pass
      ? `selection-adjusted lower bound is +${lower.toFixed(2)}bp/day on ${diffs.length} shared days`
      : `paired lower bound is ${Number.isFinite(lower) ? lower.toFixed(2) : "unavailable"}bp/day on ${diffs.length} shared days`,
  };
}

/* ── Normal-Normal conjugate posterior over each variant's mean edge ───── */
function posterior(stat, prior) {
  const priorMean = prior.meanBps ?? 0;          // skeptical: assume no edge
  const priorSd = prior.sdBps ?? 40;             // but allow a real one to show
  const obsSd = stat.sdBps > 0 ? stat.sdBps : 250;
  const n = Math.max(0, stat.effectiveN || 0);

  const priorPrec = 1 / (priorSd * priorSd);
  const dataPrec = n > 0 ? n / (obsSd * obsSd) : 0;
  const postPrec = priorPrec + dataPrec;
  const postMean = (priorPrec * priorMean + dataPrec * (stat.meanNetBps || 0)) / postPrec;
  const postSd = Math.sqrt(1 / postPrec);
  return { mean: postMean, sd: postSd, n };
}

/* Box-Muller. Seedable so a given evidence set always produces the same
   allocation — an allocator you cannot reproduce is one you cannot audit. */
function makeRng(seed) {
  let s = (seed >>> 0) || 1;
  return function () { s = (s * 1664525 + 1013904223) >>> 0; return s / 4294967296; };
}
function normalSample(mean, sd, rng) {
  const u1 = Math.max(rng(), 1e-12), u2 = rng();
  return mean + sd * Math.sqrt(-2 * Math.log(u1)) * Math.cos(2 * Math.PI * u2);
}

/**
 * @param stats  output of shadow.variantStats().variants
 * @param opts   { draws, prior, effectBps, sdBps, tHurdle, seed, minPerVariant }
 * @returns      { weights, powered, requiredEffectiveN, posteriors, note }
 */
function allocate(stats, opts = {}) {
  const prior = opts.prior || { meanBps: 0, sdBps: 40 };
  const need = requiredNSelect({
    effectBps: opts.effectBps, sdBps: opts.sdBps,
    alpha: opts.alpha, k: V.VARIANTS.length, power: opts.power,
  });
  const ids = V.VARIANTS.map((v) => v.id);

  const posts = {};
  const lowerBounds = {};
  let minN = Infinity;
  for (const id of ids) {
    const st = stats[id] || { effectiveN: 0, meanNetBps: 0, sdBps: 0 };
    const gateStat = (opts.powerStats && opts.powerStats[id]) || st;
    posts[id] = posterior(st, prior);
    minN = Math.min(minN, Math.max(0, Number(gateStat.effectiveN) || 0));
    const se = Number.isFinite(Number(st.hacSeBps))
      ? Number(st.hacSeBps)
      : ((Number(st.effectiveN) > 1 && Number(st.sdBps) >= 0)
          ? Number(st.sdBps) / Math.sqrt(Number(st.effectiveN)) : Infinity);
    const critical = -zQuantile((opts.alpha ?? 0.04) / ids.length);
    lowerBounds[id] = Number.isFinite(se)
      ? Number(st.meanNetBps || 0) - critical * se
      : -Infinity;
  }
  if (!Number.isFinite(minN)) minN = 0;

  // Full-information means every arm must clear the gate, not merely the arm
  // with the largest row count. A missing arm is evidence missing, not zero.
  if (minN < need) {
    const w = {}; for (const id of ids) w[id] = 1 / ids.length;
    return {
      weights: w, powered: false,
      requiredEffectiveN: need, bestEffectiveN: minN,
      progressPct: Number((Math.min(1, minN / need) * 100).toFixed(1)),
      posteriors: posts, lowerBounds, leaderId: null,
      note: `Not enough complete evidence — the least-observed arm has ${minN} of ${need} trading days. No policy change.`,
    };
  }

  // These softmax values summarize comparative evidence for the dashboard.
  // They never size positions; every arm remains a full-information shadow.
  const finite = ids.map((id) => lowerBounds[id]).filter(Number.isFinite);
  const scale = Math.max(5, Number(opts.comparisonTemperatureBps) || 20);
  const anchor = finite.length ? Math.max(...finite) : 0;
  const raw = Object.fromEntries(ids.map((id) => [id,
    Number.isFinite(lowerBounds[id]) ? Math.exp((lowerBounds[id] - anchor) / scale) : 0]));
  const rawSum = Object.values(raw).reduce((a, b) => a + b, 0) || 1;
  const weights = Object.fromEntries(ids.map((id) => [id, Number((raw[id] / rawSum).toFixed(4))]));
  const rounded = Object.values(weights).reduce((a, b) => a + b, 0);
  const residual = Number((1 - rounded).toFixed(6));
  if (residual !== 0) {
    const biggest = ids.reduce((a, b) => (weights[a] >= weights[b] ? a : b));
    weights[biggest] = Number((weights[biggest] + residual).toFixed(6));
  }

  const bestId = ids.slice().sort((a, b) => lowerBounds[b] - lowerBounds[a])[0];
  const incumbentId = ids.includes(opts.incumbentId) ? opts.incumbentId : "A";
  let comparison = null;
  let leaderId = null;
  if (bestId && lowerBounds[bestId] > (opts.minAbsoluteEdgeBps ?? 0)) {
    if (bestId === incumbentId) leaderId = bestId;
    else if (opts.dailyByVariant) {
      comparison = pairedTest(opts.dailyByVariant[bestId], opts.dailyByVariant[incumbentId], {
        minShared: opts.minShared, alpha: opts.alpha, k: ids.length,
        minAbsoluteEdgeBps: opts.minRelativeEdgeBps ?? 0,
        discountGamma: opts.discountGamma,
      });
      if (comparison.pass) leaderId = bestId;
    }
  }

  return {
    weights, powered: true,
    requiredEffectiveN: need, bestEffectiveN: minN, progressPct: 100,
    posteriors: posts, lowerBounds, comparison, incumbentId, leaderId,
    note: leaderId
      ? `${leaderId} clears the absolute and incumbent-relative lower-bound gates.`
      : "The comparison is powered, but no challenger clears every positive lower-bound gate; the incumbent stays in place.",
  };
}

/* ── plain-English read-out for the dashboard ──────────────────────────── */
function explain(alloc, stats) {
  const rows = V.VARIANTS.map((v) => {
    const st = stats[v.id] || {};
    const p = alloc.posteriors[v.id] || {};
    return {
      id: v.id, name: v.name, plain: v.plain,
      evidenceWeightPct: Number(((alloc.weights[v.id] || 0) * 100).toFixed(1)),
      weightPct: Number(((alloc.weights[v.id] || 0) * 100).toFixed(1)), // compatibility only
      trades: st.trades || 0,
      independentDays: st.effectiveN || 0,
      avgNetBps: st.meanNetBps ?? 0,
      avgCostBps: st.meanCostBps ?? 0,
      winRate: st.winRate,
      tStat: st.tStat,
      beliefBps: p.mean != null ? Number(p.mean.toFixed(2)) : null,
      beliefRangeBps: p.sd != null
        ? [Number((p.mean - 2 * p.sd).toFixed(1)), Number((p.mean + 2 * p.sd).toFixed(1))] : null,
      lowerBoundBps: Number.isFinite(alloc.lowerBounds && alloc.lowerBounds[v.id])
        ? Number(alloc.lowerBounds[v.id].toFixed(2)) : null,
      verdict: !alloc.powered ? "still gathering evidence"
             : (alloc.leaderId === v.id) ? "passes absolute and relative promotion gates"
             : (Number(alloc.lowerBounds && alloc.lowerBounds[v.id]) > 0)
               ? "positive lower bound, but not a validated replacement"
               : "no robust positive edge shown",
    };
  });
  /* WEIGHTS MUST BE PART OF THE RETURN.
   *
   * They were not, and that single omission broke the entire feedback loop:
   * the cycle called explain() and persisted its result, so the weight vector
   * — the actual output of all this machinery — went out of scope and was
   * never stored. The dashboard rendered a column headed "Share of money"
   * derived from weightPct while no money was ever split by variant, because
   * nothing downstream had a weight to read.
   *
   * `leader` is the variant the live path should adopt. It is null until the
   * power gate opens, which is what keeps the system from acting on noise. */
  const leader = alloc.leaderId ? rows.find((r) => r.id === alloc.leaderId) : null;

  return {
    rows,
    weights: alloc.weights,
    leaderId: leader ? leader.id : null,
    leaderWeightPct: leader ? leader.evidenceWeightPct : null,
    powered: alloc.powered, note: alloc.note,
    requiredEffectiveN: alloc.requiredEffectiveN, bestEffectiveN: alloc.bestEffectiveN,
    progressPct: alloc.progressPct,
  };
}

module.exports = { requiredN, requiredNSelect, zQuantile, hacMeanVariance,
  weightedHacMeanVariance, pairedTest,
  posterior, allocate, explain, makeRng, normalSample };
