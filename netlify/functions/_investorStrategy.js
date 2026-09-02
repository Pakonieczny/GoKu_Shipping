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

/* ── PAPER LEARNING MODE ───────────────────────────────────────────────────
 * The published parameters are the strict comparison policy. On this virtual
 * account, applying every strict promotion gate to the paper ledger can cost
 * the one thing the desk needs early on: labelled outcomes. This lets an
 * operator loosen it deliberately, in the open, and only where loosening is
 * defensible.
 *
 * Two rules keep it honest.
 *
 * 1. This application has no broker integration and every position is paper.
 *    `dryRun` distinguishes observation-only evaluation from the simulated
 *    paper ledger; it is not a real-money boundary.
 *
 * 2. The strict verdict is still computed and recorded on every decision, so
 *    the relaxed run never destroys the measurement it is loosening. "Would
 *    this have passed the strict gates?" stays answerable for every trade, and
 *    the two populations can be scored separately afterwards.
 *
 * Overrides are CLAMPED, not trusted: a typed-in zero cost margin cannot turn
 * the desk into a random-entry generator.
 */
const RELAX_LIMITS = {
  costMarginMultiple:     { min: 0.25, max: 2,   dflt: 2 },
  minAbsZ:                { min: 1.0,  max: 3,   dflt: 2 },
  entryRank:              { min: 0.05, max: 0.5, dflt: 0.1 },
  exitRank:               { min: 0.2,  max: 0.9, dflt: 0.5 },
  maxHoldDays:            { min: 0.25, max: 14,  dflt: 10 },
  sectorCrowdingMultiple: { min: 1.0,  max: 4,   dflt: 1.4 },
  minAdvUsd:              { min: 5e7,  max: 3e8, dflt: 3e8 },
  /* Operator sizing dial for the exploratory paper lane. Multiplies the
     signal/coverage size scaler before the ordinary per-position cap; the
     cap, the per-trade risk budget and the one-share floor all still apply. */
  positionScale:          { min: 1,    max: 5,   dflt: 1 },
};

function clampRelax(key, value) {
  const lim = RELAX_LIMITS[key];
  if (!lim) return null;
  const n = Number(value);
  if (!Number.isFinite(n)) return null;
  return Math.min(lim.max, Math.max(lim.min, n));
}

/**
 * @param {object} base     the strategy/variant config the cycle would have used
 * @param {object} ctrl     the control document
 * @returns {{cfg:object, active:boolean, refused:string|null, applied:object}}
 */
function paperLearningConfig(base, ctrl = {}) {
  const req = ctrl.paperLearning || {};
  const cfg = { ...base };
  if (req.enabled !== true) {
    return { cfg, active: false, refused: null, applied: {} };
  }
  const applied = {};
  /* The strict policy requires a positive, selection-corrected calibration
     before it can propose anything. That is appropriate for promotion, but it
     is circular for an exploratory paper ledger: no observations means no
     calibration, and no calibration means no observations. Paper learning
     breaks only that loop. The untouched strict configuration is evaluated and
     stored alongside every relaxed decision by the cycle worker. */
  if (cfg.requireCalibratedEdge !== false) {
    applied.requireCalibratedEdge = { from: cfg.requireCalibratedEdge !== false, to: false };
    cfg.requireCalibratedEdge = false;
  }
  for (const key of Object.keys(RELAX_LIMITS)) {
    if (req[key] == null) continue;
    const v = clampRelax(key, req[key]);
    if (v == null || v === cfg[key]) continue;
    applied[key] = { from: cfg[key], to: v,
      ...(Number(req[key]) !== v ? { clampedFrom: Number(req[key]) } : {}) };
    cfg[key] = v;
  }
  if (req.abstainOnMissingInfo === true) {
    cfg.paperAbstainOnMissingInfo = true;
    applied.abstainOnMissingInfo = { from: false, to: true };
    /* Independent missing-data haircuts can legitimately compound to zero even
       after every hard gate has passed. A zero-size "pass" produces no outcome,
       so exploratory paper decisions get a small, explicit floor. Portfolio,
       cash, loss, duplication and finding-based gates still run afterwards. */
    cfg.paperObservationSizeFloor = 0.10;
    applied.paperObservationSizeFloor = { from: 0, to: 0.10 };
  }
  /* A paper learner cannot bootstrap a calibrated lower bound before it has
     generated paper outcomes. Requiring that bound here made the mode
     circular: no order -> no outcome -> no calibration -> no order. The
     ordinary expected-edge-versus-friction hurdle remains fully active, and
     every simulated-ledger result is isolated from validated promotion data. */
  if (cfg.requireCalibratedEdge !== false) {
    applied.requireCalibratedEdge = { from: cfg.requireCalibratedEdge, to: false,
      scope: "paper_learning_only" };
    cfg.requireCalibratedEdge = false;
  }
  return { cfg, active: true, refused: null, applied };
}

module.exports = {
  paperLearningConfig, RELAX_LIMITS, clampRelax,
  "version": "v10",
  "supersedes": "v9",
  "name": "Residual reversal with controlled decision-feedback and locked forward confirmation",
  "frozenAt": "2026-09-01",
  "_versionNote": "v10 carries the v8/v9 strict hypothesis and strict parameters UNCHANGED. It differs from v9 only in the labelled exploratory paper cohort (exploratory-auto-v4: frozen data-sufficiency policy, signal lifetime, identity-bound scoreboard, pacing 10/scan, control 5/session, exit rank 0.6). v9 remains in the StrategyVersions record; a frozen identity is never edited in place, so the exploratory changes are a new version rather than a mutation of v9.",
  "immutable": true,
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
    "intelligenceMaxFocus": 304,
    "intelligenceLookbackDays": 550,
    "intelligenceCompaniesPerSweep": 6,
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
  /* Operator ceiling: manual paper approval may be selected after the current
     build identity is attested; the entry path still rechecks ledger and
     execution invariants. Shadow and limited-auto remain measured promotions.
     The system can climb to the selected ceiling and no further. */
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

  /* Immediate autonomous PAPER exploration. This is deliberately separate
     from limited_auto: it gathers labelled outcomes and may lose virtual
     money, but it never claims the strategy has passed the statistical
     promotion ladder. The complete strict verdict remains beside every
     exploratory verdict so these populations can never be pooled silently.

     v4 (v3 plus a frozen sufficiency policy, signal lifetime and an
     identity-bound scoreboard) widens only this labelled paper-learning
     cohort and makes it CONTINUOUSLY ACTIVE: the bottom half of the cross-section may be
     examined once the residual is at least 1 standard deviation unusual, the
     cost margin is the clamp floor, rank recovery exits once the name is
     back ABOVE the median (60%; with entries admitted up to the median an
     exit at 40% closed most positions on the very next scan, which measures
     noise, not reversion), and a three-session time stop turns
     observations over. Entries are paced per
     cycle, a small unconditional control cohort runs beside the signal
     cohort, and Thompson sampling over the frozen policy family orders
     which qualified candidates are taken first. Strict v10 remains at rank
     10% / |z| 2 / ten sessions, so increased paper activity cannot
     masquerade as validation. */
  "exploratoryAuto": {
    "version": "exploratory-auto-v4",
    "enabled": true,
    "autoStartAfterSuccessfulBootstrap": true,
    "startingNavUsd": 100000,
    "evidenceCohort": "exploratory_auto_unvalidated",
    "paperLearningDefaults": {
      "enabled": true,
      "abstainOnMissingInfo": true,
      "costMarginMultiple": 0.25,
      "minAbsZ": 1.0,
      "entryRank": 0.5,
      "exitRank": 0.6,
      "maxHoldDays": 3,
      "sectorCrowdingMultiple": 4,
      "minAdvUsd": 50000000
    },
    /* The activity layer (_investorExplore.js). Every knob is clamped there.
       - maxNewEntriesPerCycle: new signal entries one cycle may open, so
         activity is spread across the session rather than one 09:45 burst.
       - minimumShareFloor: a sized order that rounds to zero shares because
         the stock price exceeds its risk-sized notional may take ONE share,
         provided one share is inside the ordinary position cap. Paper money;
         the outcome in basis points is what is being learned.
       - controlCohort: names that pass every HAZARD gate but not the signal
         gates, taken unconditionally and at random, at reduced size. They
         are the baseline the signal cohort is measured against.
       - reservationHeadroomBps: cash reserved above gross + modelled
         friction so a fill half an hour later is not refused because the
         price rose 0.3%. Refusing those fills selected the recorded sample
         toward names that kept falling. Unspent headroom returns at fill.
       - expireUnfilledEntriesAtSessionClose: an exploratory entry that did
         not fill in its own session is released, never carried to the next
         open — a mean-reversion signal is stale by then.
       - policySelection: Thompson sampling over the frozen policies whose
         entry rule fired for each candidate; "utility" restores the plain
         expected-edge ordering. */
    "activity": {
      "enabled": true,
      "maxNewEntriesPerCycle": 10,
      "minimumShareFloor": 1,
      "reservationHeadroomBps": 150,
      "expireUnfilledEntriesAtSessionClose": true,
      "controlCohort": {
        "enabled": true,
        "maxOpenPositions": 10,
        "maxNewPerSession": 5,
        "sizeMultiplier": 0.5,
        "evidenceCohort": "exploratory_control_unconditional"
      },
      "policySelection": {
        "method": "thompson",
        "priorMeanBps": 0,
        "priorSdBps": 150,
        "tradeSdBps": 250,
        "minClosedForObservedSd": 8
      },
      "scoreboardLookbackTrades": 1000,
      /* A proposal is an opinion about a price at an instant. It stays
         approvable for this long, and never past the session close. */
      "signalLifetimeMinutes": 120,
      /* DATA SUFFICIENCY (_investorSufficiency.js). A heuristic coverage
         score of what the desk knew at decision time. It never refuses a
         candidate; in the exploratory lane it scales size (never below the
         floor) and adds a mild ordering penalty, and the scoreboard splits
         outcomes by it. Everything here is part of the frozen identity so a
         change is a new experiment, not a silent drift. The floor is
         deliberately generous for the kick-start phase: thin-data names are
         taken at 60% size, not refused, because the desk has to learn
         whether thin data matters before it can justify a stricter floor. */
      "sufficiency": {
        "version": "data-sufficiency-v2",
        "weights": { "intradayBars": 20, "dailyHistory": 20, "earningsWindow": 15,
          "intelligence": 15, "correlationRegime": 10, "liquidity": 10, "priceQuality": 10 },
        "buckets": { "high": 75, "medium": 45 },
        "sizeFloor": 0.6,
        "orderingPenaltyBpsPerPoint": 0.2,
        "barsForFullCredit": 60,
        "barsMinimum": 24,
        "historyDaysForFullCredit": 252
      }
    },
    "portfolioControls": {
      "maxOpenPositions": 304,
      "maxGrossExposurePct": 100,
      "minCashPct": 0,
      "ordinaryPositionPctOfNav": 5,
      "riskBudgetPerTradePctOfNav": 0.5,
      "sectorExposurePctOfNav": 100,
      "correlatedClusterPctOfNav": 100,
      "dynamicCorrelationExposurePctOfNav": 100,
      "dynamicCorrelationThreshold": 0.65,
      "requireDynamicCorrelation": false,
      "oneDayLossPausePctOfNav": 100,
      "drawdownFreezePctFromHigh": 100,
      "instruments": "Long US-listed common shares only; virtual cash only; no leverage or broker route.",
      "_note": "All available virtual cash may be deployed across independently qualified opportunities. Duplicate-symbol, provenance, cash, lifecycle, reconciliation and executable-clock controls still apply."
    },
    "autoApproval": {
      "unlimitedOrdersPerDay": true,
      "useFullPortfolioCapacity": true,
      "minimumOrderUsd": 1,
      "_note": "No daily or automatic-position quota. Approval continues until there are no qualifying proposals or insufficient free virtual cash."
    }
  },

  "automationLadder": [
    {
      "stage": "research",
      "description": "Candidates and no-trade records only. No orders written.",
      "current": true
    },
    {
      "stage": "approval",
      "description": "Operator approves every paper entry; rule-based and protective exits continue automatically."
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
