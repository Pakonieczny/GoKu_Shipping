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
 *  DO NOT retune an active version in place. To test a new hypothesis, copy the
 *  configuration under a new version, register the immutable object and its
 *  full content hash in Firestore, and explicitly activate that version. Every
 *  attempted version remains part of the multiple-testing record.
 * ---------------------------------------------------------------------------
 */

"use strict";

module.exports = {
  "version": "v6",
  "supersedes": "v5",
  "name": "Residual reversal with controlled decision-feedback and locked forward confirmation",
  "frozenAt": "2026-08-31",
  "status": "research",
  "hypothesis": "In liquid US large caps, an abnormal NEGATIVE residual return (market- and sector-adjusted) may revert partially over 1-10 trading days only when current, required public-source company intelligence finds no corroborated material adverse event, the move is not explained by covered fundamentals, and the conservatively estimated reversion exceeds modelled round-trip frictions.",
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
    "_blackoutEstimatedNote": "Earnings windows prefer each issuer's EDGAR 8-K Item 2.02 results-announcement cadence. If that is unavailable, 10-Q/10-K filing cadence is a wider fallback. No free keyless forward calendar is assumed; projections retain their uncertainty and a symbol with no derivable window is blocked rather than traded blind.",
    "_blackoutNote": "A confirmed blackout reduces scheduled announcement risk but cannot eliminate unscheduled events or overnight gaps. Projected dates use wider windows and never masquerade as confirmed dates.",
    "volScalerFloor": 0.35,
    "volScalerCeiling": 1,
    "_volNote": "Volatility may condition expected edge, but risk sizing never increases dollars as volatility rises. Unknown or elevated risk scales exposure down.",
    "corCaution": 35,
    "corStandDown": 45,
    "_corNote": "Cboe implied correlation. When dispersion collapses every name trades as one asset and an idiosyncratic strategy has nothing to work with. No free programmatic feed exists, so this is operator-entered and a stale value reduces size rather than silently passing.",
    "abnormalActivityConfidence": 0.65,
    "requireCompanyIntelligence": true,
    "requireTemporalContext": true,
    "temporalMaxAgeHours": 6,
    "temporalSeasonalityMinTradingDays": 756,
    "_temporalNote": "Scheduled non-earnings events, quote-grounded geographic/commodity exposures, active provenance-bound NWS hazards, recurring cycles and stable rolling drivers enter one deterministic risk-only layer with continuous sizing. Earnings are enforced once by the existing signal blackout. Same-month seasonality remains visible but cannot alter size below eight completed observations. Named five-day drivers require strong company-specific evidence and coherent stable correlation. Missing required earnings, exposure inventory, weather or price provenance still blocks new risk; temporal context cannot increase size or independently liquidate.",
    "intelligenceMaxAgeHours": 6,
    "intelligenceMaxFocus": 24,
    "intelligenceLookbackDays": 550,
    "intelligenceCompaniesPerSweep": 4,
    "_intelligenceNote": "New risk requires a fresh point-in-time dossier across company-specific public-source lanes. Discovery indexes never confirm. Documents corroborate only when they share a concrete event subject; unrelated regulator matters remain separate. Syndicated copies count once, adverse sizing is continuous, positive events cannot increase base risk, and missing coverage blocks. A strongly corroborated material downside can request an exit review; no model can directly trade.",
    "_causeNote": "Volume is an abnormal-activity feature, not a retail/institutional classifier. Fundamental causes are not faded; no-cause automation stays locked until externally labelled recall is measured.",
    "decayHaircut": 0.5,
    "reversionCapture": 0.35,
    "costMarginMultiple": 2.0,
    "requireCalibratedEdge": true,
    "calibratedExpectedEdgeBps": null,
    "_costNote": "A candidate must expect at least twice its modelled round-trip friction before it is proposed at all.",
    "executionLatencyMs": 60000,
    "_crowdingNote": "A sector-wide selloff leaves residual dispersion inside that sector because member betas differ, so its low-beta members look idiosyncratically oversold. Taking several is one bet wearing several tickers. The cap is a MULTIPLE of the tail threshold rather than an absolute number, because the tail threshold is itself the random baseline: if the tail is the bottom 25% of the cross-section, ~25% of any sector lands there by chance. 1.4x flags real over-representation without firing on ordinary dispersion. The portfolio-level correlated-cluster cap remains the primary defence; this is the early warning.",
    "sectorCrowdingMultiple": 1.4,
    "expectedEdgeBps": 30,
    "perTradeSdBps": 250,
    "tHurdle": 3.0,
    "_learningNote": "The legacy single-test arithmetic gives 625 independent days, but production corrects across all 14 preregistered policies at 90% power: about 1,137 complete trading days at the current assumptions. DSR, CSCV/PBO, historical stress, and 126 locked future paper sessions are additional blocking gates. Diagnostic evidence weights are never capital allocations."
  },
  "portfolioControls": {
    "maxOpenPositions": 12,
    "maxGrossExposurePct": 60,
    "minCashPct": 40,
    "ordinaryPositionPctOfNav": 3,
    "riskBudgetPerTradePctOfNav": 0.25,
    "sectorExposurePctOfNav": 20,
    "correlatedClusterPctOfNav": 10,
    "dynamicCorrelationExposurePctOfNav": 10,
    "dynamicCorrelationThreshold": 0.65,
    "requireDynamicCorrelation": true,
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
    "toLimitedAuto": "A selection-corrected leader with DSR >=0.95 and PBO <=0.20, positive historical stress bounds, 126 untouched locked forward-paper sessions passing normal/doubled-cost/worst-case bounds, 200+ closed paper positions, gross edge above friction, and the external known-cause recall benchmark.",
    "blocking": "The recall benchmark is BLOCKING for any automation of the no-cause book. The system cannot quantify sources it does not cover, so the failure mode - fading into a real cause it never saw - must be measured before it is trusted."
  },
  /* Why the system keeps months of daily history, in plain words. Surfaced on
     the "Settings explained" view so the reasoning is never only in the code. */
  "memory": {
    "whatIsStored": "Every roster name and required economic-driver proxy gets up to about five trading years of daily open/high/low/close/volume, kept in one compact record per symbol, plus the 5-minute bars from every cycle. Exact target coverage and minimum depth are checked before the backfill is called complete.",
    "whyItMatters": "The same 3% drop means completely different things depending on the company. In a stock that normally moves 0.8% a day it is a real dislocation; in one that routinely swings 4% it is an ordinary Tuesday. Without months of history there is no way to tell those apart, and the system would trade noise in volatile names while ignoring genuine opportunities in calm ones.",
    "howItChangesDecisions": [
      "The yardstick. How unusual a move is used to be measured against the last two days, which on a quiet stretch makes ordinary moves look extraordinary. It is now measured 60% against the name's own six-month volatility and 40% against right now.",
      "The downtrend gate. A name below its 200-day average, more than 20% off its six-month high, with its 50-day line still falling, is refused. Buying dips works on stable companies having a bad week, not on companies in a long slide.",
      "Its own bounce record. How this specific company behaved after similar drops in the past adjusts how much is expected from the trade — but only within plus or minus 30%, because ten past events is not a track record.",
      "Two policies test the six-month picture, two test 6/24-bar formation against the 12-bar baseline, and the 6/10/14-day holding family tests whether the central horizon premise is actually right.",
      "The temporal layer displays same-calendar-month context after three trading years but gives it zero sizing authority below eight completed observations. Named commodity moves require strong company-specific exposure evidence plus a current, provenance-bound, directionally coherent and stable correlation."
    ],
    "theLeakageRule": "Every historical number used to decide on a given day is computed only from sessions BEFORE that day. Never the day itself. A system that lets today's move help justify today's trade will look excellent in testing and lose money in practice; the test suite checks this explicitly by altering the future and confirming the past-derived numbers do not budge.",
    "honestLimit": "Five trading years improve recurring-pattern and regime estimates but may still omit a full economic cycle. Seasonal estimates are heavily shrunk, correlations can break, and neither is treated as causal proof or permission to increase risk."
  },

  "learning": {
    "method": "Conservative full-information comparison of 14 frozen, self-financing strategy and decision-matrix policies, with separate lifetime-power, current-regime, and locked-forward evidence tracks",
    "twoTracks": "The POWER track counts only complete self-financing portfolio days and gates whether comparison is meaningful — about 1,137 days for the current 14-policy family. The discounted SELECTION track uses a matching weighted HAC uncertainty estimate and restarts at a development-only break. Once a leader is forward-locked, all later data is excluded from both tracks and belongs only to its untouched confirmation stream.",
    "forgetting": "Published trading edges decay — roughly 26% out of sample and 58% after publication (McLean & Pontiff, Journal of Finance 2016). A learner that accumulates forever would ride a dead strategy for years. A changepoint detector (Page-Hinkley) watches each variant's daily results; when a variant's performance genuinely breaks, its believed edge restarts from the break and the racing re-opens automatically. Calibrated so plain noise almost never trips it, while a real break is caught within about four trading days.",
    "promotion": "A policy can replace the incumbent only after selection-corrected weighted inference, a positive incumbent-relative bound, DSR >=0.95, PBO <=0.20, and historical stress. The winner is then transactionally locked at the current data boundary. After a 15-session embargo it must pass exactly 126 genuinely future self-financing paper days under normal, doubled-cost, and unresolved-total-loss stress. Operators can cap or demote but cannot force an upgrade.",
    "whyFrozen": "Policies never rewrite themselves. The 14 complete definitions were committed before their new experiment begins: A-H retain the entry/exit/trend family; I/J test formation; K/L test temporal and intelligence weighting; M tests size aggregation; N tests the non-blocking event threshold. Every edit requires a new identity and restarts evidence.",
    "shadowHarness": "Every frozen policy owns persistent cash, equity, costs, realized P&L, high-water mark, and drawdown. Actual eligible fills debit its cash, exits return net proceeds, and missing trustworthy marks exclude the entire portfolio-day. Every candidate stores each policy's trade/no-trade counterfactual; future outcomes label missed opportunities, correct avoidances, signal direction, cost erosion, sizing, context, exit giveback, sector, and regime. Attribution remains observational until the frozen comparison and locked forward gate pass.",
    "independenceCorrection": "Trades opened on the same day are not independent - they win or lose together in the same market move. The system counts DISTINCT DAYS, not trades. A day with nine positions counts once. Without this you would think you had 5,000 samples when you had 400.",
    "skepticalPrior": "Every variant starts from 'probably no edge' and evidence has to drag it away from zero. Three wins from four does not become a 75% edge. This shrinkage is the main defence against fitting noise.",
    "floor": "Evidence weights rank frozen challengers; they are not simultaneous capital allocations. Until the promotion gates pass, the baseline remains the only permissible policy."
  }
};
