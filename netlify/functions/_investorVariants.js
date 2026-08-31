/*  netlify/functions/_investorVariants.js  (v2.0)
 *  ---------------------------------------------------------------------------
 *  Investor_AI — frozen strategy and decision-matrix policies.
 *
 *  WHY THESE ARE FROZEN
 *  ------------------------------------------------------------------------
 *  A system that continuously tunes its own numbers will always look like it
 *  is improving, because it is fitting the noise in whatever data it has seen.
 *  Tune weekly for a year and you have tried 52 hypotheses; the luckiest one
 *  looks excellent by chance alone.
 *
 *  These policies are committed BEFORE any experiment data arrives. They never
 *  change in place. The
 *  system learns only which of them works — not what their numbers should be.
 *  That is the difference between selecting among pre-registered options
 *  (statistically sound) and searching for one that fits (not).
 *
 *  To add a policy: add it here with a NEW id and let it start from zero
 *  evidence. Never edit an existing one — editing silently invalidates every
 *  observation already recorded against it.
 * ---------------------------------------------------------------------------
 */

"use strict";

const crypto = require("crypto");
const NEUTRAL = Object.freeze({ ...require("./_investorStrategy").parameters });

const VARIANTS = [
  {
    id: "A",
    name: "Baseline",
    plain: "The standard setup. Waits for a stock to fall further than its sector explains, then holds until it climbs back to the middle of the pack.",
    params: { entryRank: 0.10, minAbsZ: 2.0, exitRank: 0.50, maxHoldDays: 10, noCauseConfidence: 0.5 },
  },
  {
    id: "B",
    name: "Choosier",
    plain: "Same idea, but only acts on bigger, rarer drops. Fewer trades, each one a stronger case.",
    params: { entryRank: 0.05, minAbsZ: 2.5, exitRank: 0.50, maxHoldDays: 10, noCauseConfidence: 0.5 },
  },
  {
    id: "C",
    name: "Quick exit",
    plain: "Enters like the baseline but takes profit earlier instead of waiting for a full recovery. Less time exposed, smaller wins.",
    params: { entryRank: 0.10, minAbsZ: 2.0, exitRank: 0.35, maxHoldDays: 6, noCauseConfidence: 0.5 },
  },
  {
    id: "D",
    name: "Patient",
    plain: "Enters like the baseline but waits longer for the bounce. More time exposed, bigger wins when it works.",
    params: { entryRank: 0.10, minAbsZ: 2.0, exitRank: 0.65, maxHoldDays: 14, noCauseConfidence: 0.5 },
  },
  {
    id: "E",
    name: "Crowd-only",
    plain: "Only trades drops that look like crowd panic with no real news behind them. Skips anything where we cannot find a cause at all.",
    params: { entryRank: 0.10, minAbsZ: 2.0, exitRank: 0.50, maxHoldDays: 10, noCauseConfidence: 0.0 },
  },
  {
    id: "F",
    name: "Wide net",
    plain: "Acts on smaller drops too. Many more trades, each with a weaker case — a test of whether volume of trades beats quality of trades.",
    params: { entryRank: 0.20, minAbsZ: 1.5, exitRank: 0.50, maxHoldDays: 10, noCauseConfidence: 0.5 },
  },

  /* G and H were added when the daily-history layer went in. They exist to
     settle one question with evidence instead of opinion: does a stock's
     six-month picture help or hurt a short-term bounce trade?

     They take opposite sides on purpose. G buys dips only in names still in an
     uptrend — "buy strength that stumbled". H buys only names already badly
     beaten down — "buy what everyone has given up on". Textbook mean reversion
     favours H; the momentum literature favours G; they cannot both be right,
     and the horse race will say which. Both start from zero evidence. */
  {
    id: "G",
    name: "Dips in uptrends",
    plain: "Only buys the dip when the company's stock has still been rising over the last six months. The idea being tested: a stumble in something healthy bounces back, but a stumble in something already sliding just keeps sliding.",
    params: { entryRank: 0.10, minAbsZ: 2.0, exitRank: 0.50, maxHoldDays: 10, noCauseConfidence: 0.5,
              requireAboveSma200: true, blockDowntrends: true },
  },
  {
    id: "H",
    name: "Deeply beaten down",
    plain: "The opposite bet. Only buys names already well below their six-month high, on the theory that the biggest bounces come from the most abandoned stocks. Deliberately takes the other side of G so the system can find out which view is actually right.",
    params: { entryRank: 0.10, minAbsZ: 2.0, exitRank: 0.50, maxHoldDays: 10, noCauseConfidence: 0.5,
              requireDrawdownPct: -15, blockDowntrends: false },
  },
  /* I/J vary the central formation horizon while leaving every other baseline
     decision fixed. C/A/D already preregister 6/10/14-session holding
     horizons, so the family now tests both sides of the signal/holding premise
     rather than only peripheral entry refinements. */
  {
    id: "I",
    name: "Fast formation",
    plain: "Measures the abnormal fall over six 5-minute bars. It tests whether the reversal signal forms faster than the 12-bar baseline assumes.",
    params: { entryRank: 0.10, minAbsZ: 2.0, exitRank: 0.50, maxHoldDays: 10,
      noCauseConfidence: 0.5, signalWindow: 6 },
  },
  {
    id: "J",
    name: "Slow formation",
    plain: "Measures the abnormal fall over 24 5-minute bars. It tests whether a more persistent dislocation is more reliable than the 12-bar baseline.",
    params: { entryRank: 0.10, minAbsZ: 2.0, exitRank: 0.50, maxHoldDays: 10,
      noCauseConfidence: 0.5, signalWindow: 24 },
  },
  /* K-N are one-change-at-a-time challengers for the decision matrix itself.
     Hard safety blocks are invariant: missing/stale required data, unresolved
     adverse leads, raw event risk >=70, and critical-exit thresholds cannot be
     weakened by a challenger. Only non-blocking haircuts differ. */
  {
    id: "K",
    name: "Temporal cautious",
    plain: "Applies 25% more weight to non-blocking temporal risk while preserving every hard data and event block.",
    params: { entryRank: 0.10, minAbsZ: 2.0, exitRank: 0.50, maxHoldDays: 10,
      noCauseConfidence: 0.5, decisionMatrixPolicy: { temporalRiskScale: 1.25 } },
  },
  {
    id: "L",
    name: "Intelligence cautious",
    plain: "Applies 25% more weight to non-blocking company-intelligence risk while preserving every hard data and event block.",
    params: { entryRank: 0.10, minAbsZ: 2.0, exitRank: 0.50, maxHoldDays: 10,
      noCauseConfidence: 0.5, decisionMatrixPolicy: { intelligenceRiskScale: 1.25 } },
  },
  {
    id: "M",
    name: "Multiplicative sizing",
    plain: "Combines independent non-zero size haircuts by multiplication, providing a frozen comparison with the ranked-haircut baseline.",
    params: { entryRank: 0.10, minAbsZ: 2.0, exitRank: 0.50, maxHoldDays: 10,
      noCauseConfidence: 0.5, sizeAggregation: "product" },
  },
  {
    id: "N",
    name: "Material-context threshold",
    plain: "Ignores small non-blocking context scores below 25, testing whether minor warnings add value or only suppress good trades. Hard blocks remain unchanged.",
    params: { entryRank: 0.10, minAbsZ: 2.0, exitRank: 0.50, maxHoldDays: 10,
      noCauseConfidence: 0.5, decisionMatrixPolicy: { nonBlockingRiskFloor: 25 } },
  },
];

/* A hash over the parameters. If it changes, the variants were edited and
   every observation recorded against them is invalid. The dashboard shows it
   so a silent edit cannot go unnoticed. */
function stable(value) {
  if (Array.isArray(value)) return value.map(stable);
  if (value && typeof value === "object") {
    return Object.fromEntries(Object.keys(value).sort().map((k) => [k, stable(value[k])]));
  }
  return value;
}

function materialized() {
  return VARIANTS.map((v) => ({
    id: v.id,
    name: v.name,
    params: { ...NEUTRAL, ...v.params },
  }));
}

function variantsHash() {
  const canon = JSON.stringify(stable({ schema: "frozen-variants-v3-controlled-learning", variants: materialized() }));
  return crypto.createHash("sha256").update(canon).digest("hex");
}

function byId(id) { return VARIANTS.find((v) => v.id === id) || null; }

/**
 * Materialize a variant from the same immutable neutral policy every time.
 *
 * `baseCfg` is intentionally ignored.  Overlaying a challenger on whichever
 * policy happened to be live made experiments path-dependent: after B became
 * incumbent, evaluating C accidentally produced "B plus C".  A frozen race is
 * meaningful only when every arm is complete and independent of the leader.
 */
function configFor(id, baseCfg) { // eslint-disable-line no-unused-vars
  const v = byId(id);
  return v ? { ...NEUTRAL, ...v.params } : { ...NEUTRAL };
}

module.exports = { VARIANTS, NEUTRAL, stable, materialized, variantsHash, byId, configFor };
