/*  netlify/functions/_investorStrategy.js
 *  ---------------------------------------------------------------------------
 *  Investor_AI — frozen strategy configuration.
 *
 *  This is a .js module rather than a .json data file on purpose. Two builds
 *  failed because the config lived somewhere the bundler could not reach: once
 *  in a sibling investor/ folder that never made it into the repo, and once as
 *  a .json that was not copied alongside the code. A .js file in this folder
 *  travels with every other _investor* helper and esbuild resolves it with
 *  certainty, so that class of failure is gone.
 *
 *  EDIT THIS FILE to retune. It is read at runtime by investorCycle-background
 *  and investorApi. To run a different version, copy to _investorStrategy.v2.js,
 *  register it in Firestore, and switch versions from the control room — never
 *  edit an active version in place.
 * ---------------------------------------------------------------------------
 */

"use strict";

module.exports = {
  "version": "v1",
  "name": "Residual reversal with evidence classification",
  "frozenAt": "2026-08-29",
  "status": "research",
  "hypothesis": "In liquid US large caps, an abnormal NEGATIVE residual return (market- and sector-adjusted) that is NOT explained by a fundamental filing in covered sources reverts partially over the following 1-10 trading days, and that reversion exceeds modelled round-trip frictions by a margin.",
  "preRegistration": {
    "declaredBefore": "any collection",
    "minimumEvents": 200,
    "primaryMetric": "mean net return per closed position, in basis points, after modelled slippage",
    "significanceHurdle": "t > 3.0",
    "significanceRationale": "Harvey, Liu & Zhu: hundreds of proposed factors mean a new one needs t > 3.0, not 2.0. Anything below is treated as noise regardless of how attractive the equity curve looks.",
    "decayHaircut": 0.5,
    "decayRationale": "Chen & Velikov: roughly half of in-sample gross returns are eliminated by data-mining bias and environment change. Any gross figure is halved before comparison to the cost budget.",
    "failureCondition": "If cumulative modelled friction exceeds cumulative gross edge over 200+ closed positions, the hypothesis is rejected and the book is retired rather than re-tuned.",
    "multipleTestingDisclosure": "Every strategy version tried is retained in this directory. Reporting only the surviving version would be the exact error the pre-registration exists to prevent."
  },
  "parameters": {
    "barTimeframe": "5Min",
    "signalWindow": 12,
    "blockDowntrends": true,
    "entryRank": 0.1,
    "exitRank": 0.5,
    "minAbsZ": 2.0,
    "maxHoldDays": 10,
    "stopLossPct": -8,
    "trailingStopPct": -4,
    "trailingArmsAtPct": 3,
    "takeProfitPct": null,
    "exitBeforeEarningsDays": 2,
    "_exitRankNote": "The buy/hold spread. de Groot/Huij/Zhou: identical signal on the 100 largest names nets +31.5bp/wk with standard rebalancing and +53.1bp/wk when the exit is deferred to the 50% rank, with turnover cut from ~700% to ~330%. Novy-Marx & Velikov independently call this the single most effective simple cost mitigation. The exit rule is worth more than any entry refinement.",
    "minAdvUsd": 300000000,
    "_minAdvNote": "Below roughly $300M/day the round trip runs 10-30bp; at 400% monthly turnover that is 40-120bp/month of drag against an edge measured in tens of basis points.",
    "blackoutDays": 2,
    "blackoutDaysEstimated": 5,
    "postEarningsDays": 4,
    "turnoverPctileCap": 0.90,
    "_blackoutEstimatedNote": "Earnings windows are projected from each company own EDGAR 10-Q/10-K cadence because no free keyless feed of forward earnings dates exists. A projection needs a wider window than an exact date, and a symbol with NO derivable window is blocked outright rather than traded blind.",
    "_blackoutNote": "AMD's 95th-percentile earnings move is +/-21.2% against a +/-9.9% median; CRWD +/-22.7% against +/-8.9%. The tail lives on ~4 scheduled dates a year, so a 2-day window solves essentially the whole gap problem by calendar.",
    "volScalerFloor": 0.35,
    "volScalerCeiling": 1.75,
    "_volNote": "Nagel: in liquid names reversal is near-zero unconditionally, but a 1pp rise in normalised VIX predicts +0.22pp of daily return with a monthly R^2 of 56%. Size with volatility rather than running a flat book.",
    "corCaution": 35,
    "corStandDown": 45,
    "_corNote": "Cboe implied correlation. When dispersion collapses every name trades as one asset and an idiosyncratic strategy has nothing to work with. No free programmatic feed exists, so this is operator-entered and a stale value reduces size rather than silently passing.",
    "noCauseConfidence": 0.5,
    "_causeNote": "Ben-Rephael/Da/Easton/Israelsen: 8-Ks drawing institutional attention show no under- or over-reaction (already priced); those drawing retail attention reverse, peaking days t+7..t+8. A fundamental cause means DO NOT FADE. 'No cause detected' is traded at half size because the system cannot bound sources it does not cover.",
    "decayHaircut": 0.5,
    "reversionCapture": 0.35,
    "costMarginMultiple": 2.0,
    "_costNote": "A candidate must expect at least twice its modelled round-trip friction before it is proposed at all.",
    "positionPctOfNav": 0.03,
    "executionLatencyMs": 60000,
    "_crowdingNote": "A sector-wide selloff leaves residual dispersion inside that sector because member betas differ, so its low-beta members look idiosyncratically oversold. Taking several is one bet wearing several tickers. The cap is a MULTIPLE of the tail threshold rather than an absolute number, because the tail threshold is itself the random baseline: if the tail is the bottom 25% of the cross-section, ~25% of any sector lands there by chance. 1.4x flags real over-representation without firing on ordinary dispersion. The portfolio-level correlated-cluster cap remains the primary defence; this is the early warning.",
    "sectorCrowdingMultiple": 1.4,
    "expectedEdgeBps": 30,
    "perTradeSdBps": 250,
    "tHurdle": 3.0,
    "_learningNote": "These three set how much evidence is required before the system is allowed to favour one strategy variant over another. Required independent days = (tHurdle x perTradeSdBps / expectedEdgeBps)^2, which at these values is 625. Below that the allocator refuses to differentiate and splits evenly. Lowering tHurdle makes it act sooner on weaker evidence - that is the knob that decides whether this system learns or fools itself."
  },
  "portfolioControls": {
    "maxOpenPositions": 12,
    "maxGrossExposurePct": 60,
    "minCashPct": 40,
    "ordinaryPositionPctOfNav": 3,
    "riskBudgetPerTradePctOfNav": 0.25,
    "sectorExposurePctOfNav": 20,
    "correlatedClusterPctOfNav": 10,
    "_clusterNote": "The cap binds on economic cluster, not sector label. Apple exposure through two different suppliers is one bet, not two.",
    "oneDayLossPausePctOfNav": 1,
    "drawdownFreezePctFromHigh": 6,
    "instruments": "Long US-listed common shares only. ADRs, options, shorts and leverage disabled.",
    "realMoney": "EXCLUDED. No broker is integrated and no code path can place a live order.",
    "_enforcement": "Every number in this block is enforced in _investorRisk.js and checked in investorCycle-background.js before any order is proposed. This note exists because for a long time it was NOT: the whole block was configuration that no code read, while the comment above called the cluster cap the primary defence. If you add a control here, add its check there in the same change."
  },
  /* Operator ceiling: how far the system may promote ITSELF on measured
     evidence. It can climb to this and no further; every rung still requires
     its gates to pass. Raise it in the control room when you are ready to let
     it go further. This is the one knob that decides how autonomous it is. */
  "operatorCeiling": "approval",
  "operatorHold": false,

  "autoApproval": {
    "enabled": true,
    "maxOrderPctOfNav": 1.5,
    "maxOrdersPerDay": 6,
    "minCostRatio": 1.5,
    "allowNoCause": false,
    "_note": "Only active at the limited_auto stage. Deliberately tighter than the ordinary position caps, so the operator's approval queue holds the exceptions - unusually large orders, marginal cost ratios, and anything in the no-cause book - rather than every routine trade."
  },

  "automationLadder": [
    {
      "stage": "research",
      "description": "Candidates and no-trade records only. No orders written.",
      "current": true
    },
    {
      "stage": "approval",
      "description": "Operator approves every entry and exit."
    },
    {
      "stage": "shadow",
      "description": "System records hypothetical auto decisions beside the approved ledger."
    },
    {
      "stage": "limited_auto",
      "description": "One validated book, tiny allocation, strict daily and position caps."
    }
  ],
  "promotionGates": {
    "toApproval": "Ledger rebuilds exactly from the journal; duplicate invocations cannot duplicate a fill; every fill references a post-decision eligible bar; golden fixtures pass.",
    "toShadow": "Citation validity above 0.9 on the model lane; entity resolution unambiguous; no state discrepancies across a full week of cycles.",
    "toLimitedAuto": "200+ closed positions out of sample, t > 3.0 on net return, cumulative gross edge exceeding cumulative friction, AND a completed known-cause recall benchmark for the no-cause label.",
    "blocking": "The recall benchmark is BLOCKING for any automation of the no-cause book. The system cannot quantify sources it does not cover, so the failure mode - fading into a real cause it never saw - must be measured before it is trusted."
  },
  /* Why the system keeps months of daily history, in plain words. Surfaced on
     the "Settings explained" view so the reasoning is never only in the code. */
  "memory": {
    "whatIsStored": "Every roster name gets about 13 months of daily open/high/low/close/volume, kept in one compact record per company, plus the 5-minute bars from every cycle. The daily history is backfilled automatically the first time the system runs, so it does not start blind.",
    "whyItMatters": "The same 3% drop means completely different things depending on the company. In a stock that normally moves 0.8% a day it is a real dislocation; in one that routinely swings 4% it is an ordinary Tuesday. Without months of history there is no way to tell those apart, and the system would trade noise in volatile names while ignoring genuine opportunities in calm ones.",
    "howItChangesDecisions": [
      "The yardstick. How unusual a move is used to be measured against the last two days, which on a quiet stretch makes ordinary moves look extraordinary. It is now measured 60% against the name's own six-month volatility and 40% against right now.",
      "The downtrend gate. A name below its 200-day average, more than 20% off its six-month high, with its 50-day line still falling, is refused. Buying dips works on stable companies having a bad week, not on companies in a long slide.",
      "Its own bounce record. How this specific company behaved after similar drops in the past adjusts how much is expected from the trade — but only within plus or minus 30%, because ten past events is not a track record.",
      "Two of the eight strategies are defined entirely by the six-month picture, so the system measures whether long-term context helps rather than assuming it."
    ],
    "theLeakageRule": "Every historical number used to decide on a given day is computed only from sessions BEFORE that day. Never the day itself. A system that lets today's move help justify today's trade will look excellent in testing and lose money in practice; the test suite checks this explicitly by altering the future and confirming the past-derived numbers do not budge.",
    "honestLimit": "Six months of daily bars is a real memory but not a long one. A company's history through a full market cycle would be better; this is what a free data tier provides, and the shrinkage arithmetic is set so that thin evidence is treated as thin rather than as proof."
  },

  "learning": {
    "method": "Thompson sampling over eight frozen strategy variants, fed by counterfactual shadow trades — with two evidence tracks and changepoint-triggered forgetting",
    "twoTracks": "The system keeps two ledgers of evidence. The POWER track counts every day ever observed and gates whether any comparison is statistically meaningful — at the selection-corrected bar of ~775 independent days, because picking the best of eight is harder than testing one (the best of eight zero-skill strategies looks good by pure chance). The SELECTION track starts over at the last detected break in a variant's performance and fades old days (about eight months of effective memory), so what the system BELIEVES tracks the present, while what it has OBSERVED is never thrown away.",
    "forgetting": "Published trading edges decay — roughly 26% out of sample and 58% after publication (McLean & Pontiff, Journal of Finance 2016). A learner that accumulates forever would ride a dead strategy for years. A changepoint detector (Page-Hinkley) watches each variant's daily results; when a variant's performance genuinely breaks, its believed edge restarts from the break and the racing re-opens automatically. Calibrated so plain noise almost never trips it, while a real break is caught within about four trading days.",
    "promotion": "A variant takes over live trading only when three things hold at once: the power track has ~775 independent days, the selection track ranks it first, and it has beaten the CURRENTLY RUNNING strategy on the days they share (a paired test that cancels market noise). Looking best is not enough; it must be demonstrably better than what is already running.",
    "whyFrozen": "Variants never change. A system that tunes its own numbers always looks like it is improving, because it is fitting the noise in whatever data it has seen. These eight were committed before any data arrived, so the system is SELECTING among pre-registered options rather than SEARCHING for one that fits. That distinction is what makes it statistically valid.",
    "shadowHarness": "Every variant is scored against every ranked name on every cycle, traded or not. Real trades are rare and cost money; shadow trades cost nothing and carry the same information about which variant works.",
    "independenceCorrection": "Trades opened on the same day are not independent - they win or lose together in the same market move. The system counts DISTINCT DAYS, not trades. A day with nine positions counts once. Without this you would think you had 5,000 samples when you had 400.",
    "skepticalPrior": "Every variant starts from 'probably no edge' and evidence has to drag it away from zero. Three wins from four does not become a 75% edge. This shrinkage is the main defence against fitting noise.",
    "floor": "Every variant keeps at least 5% of the book so it never stops generating evidence and can still prove itself later."
  }
};
