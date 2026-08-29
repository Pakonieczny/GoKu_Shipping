/*  netlify/functions/_investorVariants.js  (v1.1)
 *  ---------------------------------------------------------------------------
 *  Investor_AI — the eight frozen strategy variants.
 *
 *  WHY THESE ARE FROZEN
 *  ------------------------------------------------------------------------
 *  A system that continuously tunes its own numbers will always look like it
 *  is improving, because it is fitting the noise in whatever data it has seen.
 *  Tune weekly for a year and you have tried 52 hypotheses; the luckiest one
 *  looks excellent by chance alone.
 *
 *  These six are committed BEFORE any data arrives. They never change. The
 *  system learns only which of them works — not what their numbers should be.
 *  That is the difference between selecting among pre-registered options
 *  (statistically sound) and searching for one that fits (not).
 *
 *  To add a seventh: add it here with a NEW id and let it start from zero
 *  evidence. Never edit an existing one — editing silently invalidates every
 *  observation already recorded against it.
 * ---------------------------------------------------------------------------
 */

"use strict";

const crypto = require("crypto");

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
];

/* A hash over the parameters. If it changes, the variants were edited and
   every observation recorded against them is invalid. The dashboard shows it
   so a silent edit cannot go unnoticed. */
function variantsHash() {
  const canon = VARIANTS.map((v) => `${v.id}:${JSON.stringify(v.params)}`).join("|");
  return crypto.createHash("sha256").update(canon).digest("hex").slice(0, 16);
}

function byId(id) { return VARIANTS.find((v) => v.id === id) || null; }

/** Merge a variant's params over the base strategy config. */
function configFor(id, baseCfg) {
  const v = byId(id);
  return v ? { ...baseCfg, ...v.params } : { ...baseCfg };
}

module.exports = { VARIANTS, variantsHash, byId, configFor };
