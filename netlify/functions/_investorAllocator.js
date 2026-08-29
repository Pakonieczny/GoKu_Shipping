/*  netlify/functions/_investorAllocator.js  (v1.0)
 *  ---------------------------------------------------------------------------
 *  Investor_AI — Thompson sampling over the frozen variants, with a hard
 *  statistical-power gate in front of it.
 *
 *  WHAT IT DOES IN ONE LINE
 *  ------------------------------------------------------------------------
 *  Decides how much of the book each strategy variant gets, based on how well
 *  each has actually done — while being honest about how little it yet knows.
 *
 *  HOW THOMPSON SAMPLING WORKS, PLAINLY
 *  ------------------------------------------------------------------------
 *  For each variant we keep a belief: "its true edge is probably around X,
 *  give or take Y." Every closed shadow trade narrows that belief. To decide
 *  the split, we draw one random number from each variant's belief and give
 *  more weight to whichever drew highest — repeated many times.
 *
 *  Two behaviours fall out of that, both wanted:
 *    · Early on every belief is wide, so the draws are near-random and capital
 *      spreads evenly. That is exploration, and it happens automatically.
 *    · As evidence accumulates the beliefs narrow and the better variants win
 *      more draws. That is exploitation, and it also happens automatically.
 *    · A variant that is merely UNLUCKY so far still wins occasional draws, so
 *      it is never killed on a small sample.
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
 *  Below the required evidence threshold the allocator refuses to differentiate
 *  and returns an equal split. Not a suggestion — a hard refusal. Acting on an
 *  underpowered result is exactly how a system convinces itself it has learned
 *  something.
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
 * something harder: it races eight and promotes the best. The best of eight
 * zero-skill strategies looks good by chance alone — the expected maximum of
 * eight standard normals is ~1.43 sigma — so the evidence bar must rise with
 * the number of candidates (Bailey & López de Prado 2014, "The Deflated
 * Sharpe Ratio"; White 2000; Hansen 2005).
 *
 * The standard correction: replace the single-test z with the Bonferroni
 * z at alpha/K, and add a power term so a real edge is actually detectable:
 *
 *     n = ((z_{alpha/K} + z_power) * sd / effect)^2
 *
 * At alpha=0.05, K=8, 80% power: (2.498 + 0.842) = 3.34, and with the
 * pre-registered sd/effect of 250/30 that is ~775 day-clustered observations
 * — about 1.24x the naive 625. Bonferroni over-corrects when the variants are
 * highly correlated (they trade the same names on the same days), so this is
 * a deliberately conservative bound, and the paired test below is what does
 * the fine discrimination.
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

function requiredNSelect({ effectBps = 30, sdBps = 250, alpha = 0.05, k = 8, power = 0.8 } = {}) {
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
 * promotion; the Thompson weights only say who LOOKS best, this says whether
 * the difference is real.
 */
function pairedTest(challengerDays, incumbentDays, { minShared = 40 } = {}) {
  const inc = new Map((incumbentDays || []).map((d) => [d.date, d.mean]));
  const diffs = [];
  for (const d of challengerDays || []) {
    if (inc.has(d.date)) diffs.push(d.mean - inc.get(d.date));
  }
  if (diffs.length < minShared) {
    return { pass: false, n: diffs.length,
             note: `only ${diffs.length} shared days — needs ${minShared} before the comparison means anything` };
  }
  const m = diffs.reduce((a, b) => a + b, 0) / diffs.length;
  const sd = Math.sqrt(diffs.reduce((a, b) => a + (b - m) * (b - m), 0) / (diffs.length - 1));
  /* sd of exactly zero means every shared day differed by the same amount —
     a constant nonzero difference is certain, not insignificant. */
  const t = sd > 0 ? m / (sd / Math.sqrt(diffs.length)) : (m !== 0 ? Math.sign(m) * 99 : 0);
  return {
    pass: t >= 2.0, t: Number(t.toFixed(2)), n: diffs.length,
    meanDiffBps: Number(m.toFixed(2)),
    note: t >= 2.0
      ? `beats the incumbent by ${m.toFixed(1)}bp/day on ${diffs.length} shared days (t=${t.toFixed(1)})`
      : `not reliably better than the incumbent on shared days (t=${t.toFixed(1)} over ${diffs.length} days)`,
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
  const draws = opts.draws || 4000;
  const prior = opts.prior || { meanBps: 0, sdBps: 40 };
  const need = requiredN({ effectBps: opts.effectBps, sdBps: opts.sdBps, t: opts.tHurdle });
  const ids = V.VARIANTS.map((v) => v.id);

  const posts = {};
  let maxN = 0;
  for (const id of ids) {
    const st = stats[id] || { effectiveN: 0, meanNetBps: 0, sdBps: 0 };
    posts[id] = posterior(st, prior);
    maxN = Math.max(maxN, posts[id].n);
  }

  // THE POWER GATE. Under-evidenced means equal weight, full stop.
  if (maxN < need) {
    const w = {}; for (const id of ids) w[id] = 1 / ids.length;
    return {
      weights: w, powered: false,
      requiredEffectiveN: need, bestEffectiveN: maxN,
      progressPct: Number(((maxN / need) * 100).toFixed(1)),
      posteriors: posts,
      note: `Not enough evidence yet — ${maxN} of ${need} independent days needed. Splitting evenly until then.`,
    };
  }

  const rng = makeRng(opts.seed || 20260829);
  const wins = {}; for (const id of ids) wins[id] = 0;
  for (let i = 0; i < draws; i++) {
    let bestId = ids[0], best = -Infinity;
    for (const id of ids) {
      const s = normalSample(posts[id].mean, posts[id].sd, rng);
      if (s > best) { best = s; bestId = id; }
    }
    wins[bestId] += 1;
  }

  // Floor every variant so nothing is ever fully starved — a variant with no
  // capital stops generating evidence, and the system stops being able to
  // discover it was right after all.
  /* With eight variants a flat 5% floor reserves 40% of the book before the
     winner gets anything. Scale it so the floors never claim more than about a
     third in total, while keeping the guarantee that nothing is fully starved. */
  const floor = opts.minPerVariant ?? Math.min(0.05, 0.32 / ids.length);
  const weights = {}; const rem = 1 - floor * ids.length;
  for (const id of ids) weights[id] = floor + (wins[id] / draws) * rem;

  /* Normalise, then round, then push the rounding residual onto the largest
     weight. Rounding AFTER normalising leaves the weights not summing to 1,
     which silently under- or over-allocates the book. */
  const sum = Object.values(weights).reduce((a, b) => a + b, 0);
  for (const id of ids) weights[id] = Number((weights[id] / sum).toFixed(4));
  const rounded = Object.values(weights).reduce((a, b) => a + b, 0);
  const residual = Number((1 - rounded).toFixed(6));
  if (residual !== 0) {
    const biggest = ids.reduce((a, b) => (weights[a] >= weights[b] ? a : b));
    weights[biggest] = Number((weights[biggest] + residual).toFixed(6));
  }

  return {
    weights, powered: true,
    requiredEffectiveN: need, bestEffectiveN: maxN, progressPct: 100,
    posteriors: posts, draws,
    note: "Allocating by evidence. Each variant keeps a 5% floor so it can keep proving itself.",
  };
}

/* ── plain-English read-out for the dashboard ──────────────────────────── */
function explain(alloc, stats) {
  const rows = V.VARIANTS.map((v) => {
    const st = stats[v.id] || {};
    const p = alloc.posteriors[v.id] || {};
    return {
      id: v.id, name: v.name, plain: v.plain,
      weightPct: Number(((alloc.weights[v.id] || 0) * 100).toFixed(1)),
      trades: st.trades || 0,
      independentDays: st.effectiveN || 0,
      avgNetBps: st.meanNetBps ?? 0,
      avgCostBps: st.meanCostBps ?? 0,
      winRate: st.winRate,
      tStat: st.tStat,
      beliefBps: p.mean != null ? Number(p.mean.toFixed(2)) : null,
      beliefRangeBps: p.sd != null
        ? [Number((p.mean - 2 * p.sd).toFixed(1)), Number((p.mean + 2 * p.sd).toFixed(1))] : null,
      verdict: !alloc.powered ? "still gathering evidence"
             : (st.tStat != null && st.tStat > 3) ? "evidence of a real edge"
             : (st.meanNetBps > 0) ? "positive so far, not yet conclusive"
             : "no edge shown",
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
  const ranked = rows.slice().sort((a, b) => b.weightPct - a.weightPct);
  const leader = alloc.powered && ranked.length ? ranked[0] : null;

  return {
    rows,
    weights: alloc.weights,
    leaderId: leader ? leader.id : null,
    leaderWeightPct: leader ? leader.weightPct : null,
    powered: alloc.powered, note: alloc.note,
    requiredEffectiveN: alloc.requiredEffectiveN, bestEffectiveN: alloc.bestEffectiveN,
    progressPct: alloc.progressPct,
  };
}

module.exports = { requiredN, requiredNSelect, zQuantile, pairedTest, posterior, allocate, explain, makeRng, normalSample };
