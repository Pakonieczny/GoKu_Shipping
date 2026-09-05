/*  netlify/functions/_investorPolicy.js  (fund-manager-v1)
 *  ---------------------------------------------------------------------------
 *  Investor AI — the versioned policy of the AI Fund Manager.
 *
 *  WHY THIS MODULE EXISTS
 *  ------------------------------------------------------------------------
 *  Blueprint v2 §3: "There should be no model deciding which model gets to
 *  make a decision. The routing policy is code and changes only through a
 *  versioned, evaluated release." Everything an investment decision is bound
 *  to — which model judged, at what reasoning effort, under which risk
 *  mandate, against which schema, at which prices, on which exchange calendar
 *  — lives here as data with a stable hash. A mandate (§7) carries
 *  `policyHash`, `riskPolicyHash`, `schemaHash` and `model`; a later replay
 *  can therefore prove which policy produced it.
 *
 *  WHAT IS HERE
 *   · ROLE_MODELS      fixed model routing (§3): Luna extracts, Sol decides.
 *   · MODEL_RATES      published token prices (§9.2) as nano-dollars per token
 *                      — integer strings, never floats — and the cost formula
 *                      of §9.3.
 *   · CUTOFFS_ET       the daily cadence (§6.1): 08:30 freeze, 09:15 holding
 *                      deadline, 09:20 expansion soft deadline, one overnight
 *                      ingest pass at perSweep 8 (D-11).
 *   · BUDGET           §9.4/§9.5: a reservation, not a cap on correctness.
 *   · RISK_MANDATE     §15.1: paper defaults, labelled assumptions; every
 *                      number a canonical integer string in basis points or
 *                      minor units.
 *   · EMERGENCY_RISK_POLICY_TEMPLATE  §8.7: inactive until owner-approved.
 *   · SCHEMAS          the discriminated decision contracts (§6.3, §6.7, §7):
 *                      MandateProposal.v1, UniverseReview.v1,
 *                      HoldingRevision.v1, EntryRevision.v1, ResearchMemo.v1,
 *                      PortfolioSynthesis.v1, FactExtraction.v1,
 *                      ClaimVerification.v1, EventRevision.v1, Postmortem.v1.
 *                      Inline, as §12.1 requires; the strict model subset is
 *                      GENERATED from the same source (strictOutputSchema).
 *   · loadActive / validateHashSet / policyIdentity.
 *
 *  WHAT IS NOT HERE
 *   No Firestore client at load. No prompt text (that is the gateway's).
 *   No investment judgment: the policy bounds the manager; it never chooses.
 * ---------------------------------------------------------------------------
 */

"use strict";

const crypto = require("crypto");

const POLICY_VERSION = "fund-manager-v1";
const CALENDAR_ID = "XNYS";
const ACCOUNT_CURRENCY = "USD";

/* ── canonical hashing (identical convention to _investorSelftest) ─────── */
function canonical(v) {
  if (Array.isArray(v)) return v.map(canonical);
  if (v && typeof v === "object") {
    return Object.fromEntries(Object.keys(v).sort().map((k) => [k, canonical(v[k])]));
  }
  return v === undefined ? null : v;
}
function sha256(v) {
  return crypto.createHash("sha256").update(typeof v === "string" ? v : JSON.stringify(canonical(v))).digest("hex");
}
function deepFreeze(o) {
  if (o && typeof o === "object" && !Object.isFrozen(o)) {
    Object.freeze(o);
    for (const k of Object.keys(o)) deepFreeze(o[k]);
  }
  return o;
}

/* ── §3 FIXED MODEL ROUTING ────────────────────────────────────────────── */
const ROLE_MODELS = deepFreeze({
  facts: { model: "gpt-5.6-luna", authority: "extract_source_bound_facts_only" },
  verification: { model: "gpt-5.6-luna", authority: "independent_claim_verification_only" },
  manager: { model: "gpt-5.6-sol", reasoning: { effort: "high" },
    authority: "investment_decision_and_standing_mandate" },
  postmortem: { model: "gpt-5.6-sol", reasoning: { effort: "medium" }, authority: "offline_evaluation_only" },
});
/* Terra is removed from the investment path (§3). No cheaper model may
   filter or rank the eligible roster before Sol sees it. */
const FORBIDDEN_INVESTMENT_MODELS = Object.freeze(["gpt-5.6-terra"]);
const ROSTER_FILTER_MODELS_ALLOWED = Object.freeze([]);

/* ── §9.2 PUBLISHED TOKEN PRICES, nano-dollars per token ───────────────── */
/* $4.00 per million tokens is 4,000 nano-dollars per token. Integer strings,
   so no audited cost is ever a float. The UI reads these; it never hardcodes
   the paragraph. */
const MODEL_RATES = deepFreeze({
  "gpt-5.6-sol": {
    inputNanoPerToken: "4000", cacheWriteNanoPerToken: "5000",
    cachedReadNanoPerToken: "400", outputNanoPerToken: "20000",
    longContextThresholdTokens: 272000,
    longContextInputMultiplierBps: "20000", longContextOutputMultiplierBps: "15000",
    pricingAsOf: "2026-09-03", promotionalThrough: "2026-11-21",
    source: "https://developers.openai.com/api/docs/models/gpt-5.6-sol",
  },
  "gpt-5.6-luna": {
    inputNanoPerToken: "200", cacheWriteNanoPerToken: "250",
    cachedReadNanoPerToken: "20", outputNanoPerToken: "1200",
    longContextThresholdTokens: null,
    longContextInputMultiplierBps: "10000", longContextOutputMultiplierBps: "10000",
    pricingAsOf: "2026-09-03", promotionalThrough: null,
    source: "https://developers.openai.com/api/docs/models/gpt-5.6-luna",
  },
});
const NANO_PER_MINOR = 10000000n;   // 1 cent = 10,000,000 nano-dollars

function bi(v, name) {
  if (typeof v === "bigint") return v;
  if (typeof v === "number" && Number.isSafeInteger(v) && v >= 0) return BigInt(v);
  if (typeof v === "string" && /^(0|[1-9][0-9]*)$/.test(v)) return BigInt(v);
  if (v === undefined || v === null) return 0n;
  throw Object.assign(new Error(`${name || "value"} must be a non-negative canonical integer`), { code: "NOT_CANONICAL_INTEGER" });
}

/** §9.3 cost formula, exact. Reasoning tokens are a subset of `outputTokens`
 *  and are recorded separately for diagnostics only — never added twice.
 *  Ordinary input excludes cache writes and cached reads (no double count). */
function costNanoUsd({ model, ordinaryInputTokens = 0, cacheWriteTokens = 0, cachedReadTokens = 0,
  outputTokens = 0, totalInputTokens = null }) {
  const r = MODEL_RATES[model];
  if (!r) throw Object.assign(new Error(`no published rate for model ${model}`), { code: "UNPRICED_MODEL" });
  const u = bi(ordinaryInputTokens, "ordinaryInputTokens"), w = bi(cacheWriteTokens, "cacheWriteTokens");
  const c = bi(cachedReadTokens, "cachedReadTokens"), o = bi(outputTokens, "outputTokens");
  const total = totalInputTokens == null ? u + w + c : bi(totalInputTokens, "totalInputTokens");
  const longContext = r.longContextThresholdTokens != null && total > BigInt(r.longContextThresholdTokens);
  const inMult = longContext ? BigInt(r.longContextInputMultiplierBps) : 10000n;
  const outMult = longContext ? BigInt(r.longContextOutputMultiplierBps) : 10000n;
  const input = (u * BigInt(r.inputNanoPerToken) + w * BigInt(r.cacheWriteNanoPerToken)
    + c * BigInt(r.cachedReadNanoPerToken)) * inMult / 10000n;
  const output = o * BigInt(r.outputNanoPerToken) * outMult / 10000n;
  return { nanoUsd: input + output, inputNanoUsd: input, outputNanoUsd: output, longContext,
    tokens: { ordinaryInput: u.toString(), cacheWrite: w.toString(), cachedRead: c.toString(), output: o.toString() } };
}
/** Cost in minor units (cents) as a canonical string, rounded half-up. */
function costMinor(usage) {
  const { nanoUsd, ...rest } = costNanoUsd(usage);
  const minor = (nanoUsd + NANO_PER_MINOR / 2n) / NANO_PER_MINOR;
  return { amountMinor: minor.toString(), currency: ACCOUNT_CURRENCY, nanoUsd: nanoUsd.toString(), ...rest };
}

/* ── §6.1 DAILY CADENCE (minutes after midnight, New York) ─────────────── */
const CUTOFFS_ET = deepFreeze({
  calendarId: CALENDAR_ID,
  evidenceFreezeMin: 8 * 60 + 30,          // freeze the initial evidence manifest
  managerStartMin: 8 * 60 + 30,            // start the meeting immediately
  holdingHardDeadlineMin: 9 * 60 + 15,     // holding protection/revision committed
  expansionSoftDeadlineMin: 9 * 60 + 20,   // later validFrom rather than rushed reasoning
  regularOpenMin: 9 * 60 + 30,
  ingest: {
    perSweep: 8,                            // largest value inside one 15-minute invocation
    worstCaseCompanySeconds: 110,
    freshnessWindowHours: 6,
    completeBeforeMin: 8 * 60 + 30,         // the pass COMPLETES at the freeze
    daytimeRescan: false,                   // intraday evidence uses event_revision (D-11)
  },
  antiAnchoringEveryTradingDays: 20,       // starting policy, measured not assumed
  background: {
    pollSeconds: 60, degradedAfterSeconds: 120, terminalOutputPersistenceSlaSeconds: 480,
    providerRetentionSeconds: 600,
  },
  platform: { functionCapSeconds: 900, dispatchBudgetSeconds: 20, maxJobsPerTick: 4,
    runLeaseTtlSeconds: 600 },             // shorter than the function cap (D-10)
});

/* ── §9.4 / §9.5 BUDGET — a reservation, not a cap on correctness ──────── */
function envMinor(name, fallbackUsd) {
  const raw = process.env[name];
  const usd = raw !== undefined && raw !== "" && Number.isFinite(Number(raw)) ? Number(raw) : fallbackUsd;
  return String(Math.max(0, Math.round(usd * 100)));
}
function budgetPolicy() {
  return deepFreeze({
    schemaVersion: "budget-policy.v1", currency: ACCOUNT_CURRENCY,
    normalDayMinor: { low: "400", high: "1000" },
    busyDayMinor: { low: "1000", high: "2000" },
    dailyReservationMinor: envMinor("INVESTOR_OPENAI_DAILY_USD", 10),
    cycleCallCeiling: Number(process.env.INVESTOR_OPENAI_CYCLE_CALLS || 40),
    bootstrapCeilingMinor: envMinor("INVESTOR_BOOTSTRAP_USD", 200),
    contextGuardrailTokens: 220000,
    zeroHitRateProvisioning: true,           // cache hit rate is measured, not assumed
    degradation: [
      "never cancel existing protective orders to save AI cost",
      "finish the current holding revision before considering new opportunities",
      "defer low-priority research and mark it visibly",
      "never substitute Luna/Terra for Sol investment judgment",
      "never fall back to v18 residual buys",
      "incomplete frozen-roster coverage activates no new BUY",
      "exhaustion never becomes a trade: abstain under evidence_unavailable",
    ],
  });
}

/* ── §6.5 READ-ONLY TOOL POLICY ────────────────────────────────────────── */
const TOOL_POLICY = deepFreeze({
  schemaVersion: "tool-policy.v1",
  allowlist: ["getFilingFactsAsOf", "getSourceSpans", "searchDecisionData", "getMarketContextAsOf",
    "runValuation", "getPortfolioSnapshot", "runPortfolioScenarios"],
  forbidden: ["browse", "submitOrder", "cancelOrder", "setControl"],
  maxCallsPerJob: 24, maxArgumentBytes: 4096, maxResultBytes: 200000, maxWallSeconds: 600,
  maxResearchConcurrency: 3, symbolScoped: true, asOfScoped: true, urlAllowlisted: true,
});

/* ── §15.1 HARD-RISK MANDATE — paper defaults, labelled assumptions ────── */
const RISK_MANDATE = deepFreeze({
  schemaVersion: "risk-mandate.v1",
  currency: ACCOUNT_CURRENCY, navBasis: "MARKED_NAV_INCLUDING_SETTLED_CASH",
  assumption: "paper defaults; live operation requires the owner's capital, loss tolerance, liquidity needs, tax/account constraints, jurisdiction and broker rules (§15.1)",
  weights: {
    maxSingleNameWeightBps: "1000", maxSectorWeightBps: "2500", maxCorrelatedClusterWeightBps: "2000",
    maxGrossExposureBps: "9500", maxNetExposureBps: "9500", minSettledCashReserveBps: "500",
    maxOvernightExposureBps: "9500", maxOpenOrderNotionalBps: "3000",
  },
  losses: {
    maxPlannedLossPerPositionBps: "100", maxAggregatePlannedLossBps: "500",
    maxStressedLossPerPositionBps: "250", maxAggregateStressedLossBps: "1000",
    dailyLossFreezeBps: "200",
    drawdownStates: [
      { fromPeakBps: "600", state: "FREEZE_EXPANSION" },
      { fromPeakBps: "1200", state: "EMERGENCY_REVIEW" },
    ],
  },
  liquidity: {
    maxOrderPctOfAdvBps: "100", maxPositionPctOfAdvBps: "500", maxSpreadBps: "50",
    minAdvMinor: "5000000000",             // $50M rolling median dollar volume
    /* Paper mode has no quoted spread feed: the assumed spread is a labelled
       assumption (§15.1), applied as a hard input to the size authority. A
       live adapter must replace it with a measured quote before LIMITED_LIVE. */
    paperAssumedSpreadBps: "10", spreadSource: "paper_assumption",
  },
  stress: {
    gapHaltAdverseBps: "1500",             // a stop is not a promised fill
    stressCostPerShareMicros: "20000",
    overnightGapBps: "800",
  },
  instruments: {
    longOnly: true, commonEquityOnly: true, wholeSharesOnly: true, quantityScale: 0,
    regularSessionOnly: true, extendedHours: false, fractionalShares: false,
    leverage: false, shortSelling: false, options: false, marginExpansion: false,
    averagingDownFromDeltaReview: false, increaseAction: false,
  },
  orders: {
    entryOrderType: "LIMIT", entryTimeInForce: "DAY", maxAuthorizedSessionsPerEntry: 3,
    protectionTimeInForce: "GTC", protectionPersistentAtBroker: true, protectionCoverage: "REGULAR_ONLY",
    appExpiredGtcProhibited: true, exchangeCalendar: CALENDAR_ID,
  },
  clampMateriality: {
    minDeltaUnits: "1", deltaPctOfProposalBps: "500", deltaNavBps: "25", boundaryEqualityMaterial: true,
  },
  activation: { maxActionableBuysPerRun: 8, requireUniqueCapitalRank: true, allOrNoneBasket: true },
});
/* Absolute bounds an owner override must respect. Tightening is always
   allowed; loosening past these is refused. */
const RISK_MANDATE_BOUNDS = deepFreeze({
  "weights.maxSingleNameWeightBps": { min: "100", max: "2500" },
  "weights.maxSectorWeightBps": { min: "500", max: "5000" },
  "weights.maxCorrelatedClusterWeightBps": { min: "500", max: "5000" },
  "weights.maxGrossExposureBps": { min: "1000", max: "10000" },
  "weights.maxNetExposureBps": { min: "1000", max: "10000" },
  "weights.minSettledCashReserveBps": { min: "0", max: "5000" },
  "weights.maxOvernightExposureBps": { min: "0", max: "10000" },
  "weights.maxOpenOrderNotionalBps": { min: "0", max: "10000" },
  "losses.maxPlannedLossPerPositionBps": { min: "10", max: "300" },
  "losses.maxAggregatePlannedLossBps": { min: "50", max: "1000" },
  "losses.maxStressedLossPerPositionBps": { min: "25", max: "600" },
  "losses.maxAggregateStressedLossBps": { min: "100", max: "2000" },
  "losses.dailyLossFreezeBps": { min: "50", max: "500" },
  "liquidity.maxOrderPctOfAdvBps": { min: "10", max: "500" },
  "liquidity.maxPositionPctOfAdvBps": { min: "50", max: "2000" },
  "liquidity.maxSpreadBps": { min: "5", max: "200" },
  "stress.gapHaltAdverseBps": { min: "500", max: "5000" },
});

/* ── §8.7 EMERGENCY RISK POLICY — inactive until owner-approved ────────── */
const EMERGENCY_RISK_POLICY_TEMPLATE = deepFreeze({
  schemaVersion: "emergency-risk-policy.v1",
  status: "UNAPPROVED", approvedBy: null, approvedAtMs: null, effectiveFrom: null, policyHash: null,
  label: "EMERGENCY_RISK",
  authority: "Deterministic code may reduce/flatten prohibited exposure or neutralize an accidental short; it never seeks alpha, opens a long, rotates or averages down.",
  triggers: [
    { id: "stale_broker_truth", metric: "brokerTruthAgeSeconds", op: "gt", threshold: "600", persistenceSeconds: 120,
      action: "FREEZE_EXPANSION_AND_CANCEL_UNFILLED_ENTRIES" },
    { id: "unresolved_reconciliation", metric: "reconciliationUnresolved", op: "eq", threshold: "1", persistenceSeconds: 0,
      action: "FREEZE_EXPANSION_AND_CANCEL_UNFILLED_ENTRIES" },
    { id: "daily_loss", metric: "dayLossBps", op: "gte", threshold: "200", persistenceSeconds: 60,
      action: "FREEZE_EXPANSION_AND_CANCEL_UNFILLED_ENTRIES" },
    { id: "drawdown", metric: "drawdownFromPeakBps", op: "gte", threshold: "600", persistenceSeconds: 60,
      action: "FREEZE_EXPANSION_AND_CANCEL_UNFILLED_ENTRIES" },
    { id: "buying_power_breach", metric: "buyingPowerShortfallMinor", op: "gt", threshold: "0", persistenceSeconds: 0,
      action: "REDUCE_TO_LEGAL_LIMIT" },
    { id: "concentration_breach", metric: "singleNameWeightBps", op: "gt", threshold: "1200", persistenceSeconds: 300,
      action: "REDUCE_TO_LIMIT" },
    { id: "gross_exposure_breach", metric: "grossExposureBps", op: "gt", threshold: "10000", persistenceSeconds: 300,
      action: "REDUCE_TO_LIMIT" },
    { id: "stressed_loss_breach", metric: "aggregateStressedLossBps", op: "gt", threshold: "1500", persistenceSeconds: 300,
      action: "REDUCE_TO_LIMIT" },
    { id: "protection_sla_breach", metric: "unprotectedSeconds", op: "gt", threshold: "300", persistenceSeconds: 0,
      action: "REDUCE_OR_FLATTEN_UNPROTECTED" },
    { id: "accidental_short", metric: "shortQuantityUnits", op: "gt", threshold: "0", persistenceSeconds: 0,
      action: "BUY_TO_COVER_TO_ZERO" },
  ],
  reductionPriority: ["ACCIDENTAL_SHORT_FIRST", "UNCOVERED_OR_MARGIN_CAUSING_SECOND",
    "SMALLEST_LIQUID_SET_WITH_GREATEST_BINDING_STRESS_RELIEF_PER_DOLLAR_USING_SOL_EMERGENCY_RANKS"],
  permittedOperations: ["FREEZE_EXPANSION", "CANCEL_UNFILLED_ENTRY", "REDUCE", "FLATTEN", "BUY_TO_COVER_ACCIDENTAL_SHORT"],
  forbiddenOperations: ["OPEN_LONG", "ROTATE", "AVERAGE_DOWN", "INCREASE_EXPOSURE", "IMPROVE_PRIORITY", "SUBSTITUTE_SYMBOL"],
  orderRule: { orderType: "MARKETABLE_LIMIT", collarBps: "100", session: "REGULAR_ONLY",
    onHaltOrNonfill: "KEEP_PROTECTION_AND_ALERT", maxQuantityRule: "EXACT_BREACH_REMOVAL_WHOLE_SHARES" },
  alwaysAlertOperator: true, forcesReconciliation: true, forcesLaterSolReview: true,
});

/* ── THE DECISION VOCABULARY (§15, §6.3, §6.7, §7.2) ───────────────────── */
const DECISIONS = Object.freeze(["BUY", "WATCH", "IGNORE", "HOLD", "REDUCE", "SELL", "ABSTAIN"]);
const EXECUTABLE_DECISIONS = Object.freeze(["BUY", "HOLD", "REDUCE", "SELL"]);
const REVIEW_DIRECTIVES = Object.freeze(["RESEARCH_NOW", "UPDATE_EXISTING", "REUSE_CURRENT", "NONE"]);
const PROVISIONAL_DISPOSITIONS = Object.freeze(["WATCH", "IGNORE", "ABSTAIN", "HOLD_CANDIDATE", "REDUCE_CANDIDATE", "SELL_CANDIDATE"]);
const REASON_CODES = Object.freeze(["DATA_INCOMPLETE", "EVIDENCE_CONFLICT", "UNCERTAINTY", "MODEL_FAILURE",
  "RESEARCH_DEFERRED", "UNFUNDED", "NONE"]);
const COMPLETION_CLASSES = Object.freeze(["HOLDING_REQUIRED", "BUY_REQUIRED", "OPTIONAL_DISCOVERY"]);
const OPPORTUNITY_ROUTES = Object.freeze(["quality_compounder", "fundamental_inflection", "expectations_gap",
  "misunderstood_event", "credible_catalyst", "asset_value", "special_situation", "portfolio_replacement", "none"]);
const ACTION_KINDS = Object.freeze(["BUY", "HOLD_PROTECT", "REDUCE", "SELL", "NONE"]);
const REVISION_RESULTS = Object.freeze(["UNCHANGED", "REVISED", "PROTECTION_ONLY"]);
const RESEARCH_DIRECTIVES = Object.freeze(["NONE", "FULL_REUNDERWRITE"]);
const ENTRY_REVISIONS = Object.freeze(["KEEP", "REVISE", "REVOKE"]);
const INVALIDATOR_TYPES = Object.freeze(["EVENT_CLASS", "FACT_EVENT", "METRIC_CROSSES", "SOURCE_CORRECTION", "DATA_STALE", "PRICE_GAP", "DATE_REACHED"]);
const INVALIDATOR_CONSEQUENCES = Object.freeze(["PAUSE_ENTRY_AND_QUEUE_REVISION", "KEEP_PROTECTION_AND_QUEUE_REVISION", "FREEZE_EXPANSION"]);
const THESIS_HEALTH = Object.freeze(["IMPROVING", "INTACT", "WEAKENED", "BROKEN", "UNKNOWN"]);
const UNCERTAINTY_LEVELS = Object.freeze(["LOW", "MEDIUM", "HIGH"]);
const VERIFICATION_VERDICTS = Object.freeze(["SUPPORTED", "CONTRADICTED", "INSUFFICIENT"]);

/* ── SCHEMAS — inline, versioned, hashed (§7.1, §12.1) ─────────────────── */
const INT = { type: "string", pattern: "^-?(0|[1-9][0-9]*)$", description: "canonical base-10 integer string" };
const UINT = { type: "string", pattern: "^(0|[1-9][0-9]*)$", description: "canonical non-negative integer string" };
const NULLABLE_UINT = { type: ["string", "null"], pattern: "^(0|[1-9][0-9]*)$" };
const INSTANT = { type: "string", pattern: "^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}(\\.\\d+)?Z$", description: "RFC 3339 UTC" };
const SESSION_DATE = { type: "string", pattern: "^\\d{4}-\\d{2}-\\d{2}$" };
const SYMBOL = { type: "string", pattern: "^[A-Z][A-Z0-9.-]{0,9}$" };
const TEXT = (max) => ({ type: "string", maxLength: max });
const ID = { type: "string", minLength: 1, maxLength: 160 };
const STR_ENUM = (values) => ({ type: "string", enum: [...values] });
const OBJ = (properties, extra = {}) => ({ type: "object", additionalProperties: false,
  required: Object.keys(properties), properties, ...extra });
const ARR = (items, max) => ({ type: "array", items, ...(max ? { maxItems: max } : {}) });

const SOURCE_MANIFEST_ITEM = OBJ({ claimId: ID, documentVersionId: ID, publishedAt: INSTANT, retrievedAt: INSTANT });
const OUTCOME_BUCKET = OBJ({ id: TEXT(24), interval: TEXT(40), probabilityPpm: UINT, terminalPriceMicros: UINT });
const FORECAST_BASIS = OBJ({
  referencePriceMicros: UINT,
  referencePriceType: STR_ENUM(["BUY_LIMIT", "LAST_TRADE", "EXECUTABLE_BID", "MARK"]),
  referenceTime: INSTANT, currency: STR_ENUM([ACCOUNT_CURRENCY]),
  returnConvention: STR_ENUM(["ARITHMETIC_TOTAL_RETURN", "LOG_TOTAL_RETURN", "ARITHMETIC_PRICE_RETURN"]),
  conditionalOn: STR_ENUM(["ENTRY_FILLED_AT_REFERENCE_PRICE", "OPPORTUNITY_FROM_REFERENCE_PRICE", "HELD_FROM_MARK"]),
  corporateActionPolicy: STR_ENUM(["SPLIT_AND_CASH_DISTRIBUTION_ADJUSTED", "SPLIT_ADJUSTED_ONLY"]),
  horizonTradingDays: { type: "integer", minimum: 1, maximum: 260 },
  estimatedRoundTripCostMicrosPerShare: UINT,
});
const FORECAST = OBJ({
  basis: FORECAST_BASIS,
  fillProbabilityByExpiryPpm: UINT,
  noFillOutcome: STR_ENUM(["CASH_RETURN_OVER_AUTHORIZED_SESSIONS", "NOT_APPLICABLE"]),
  horizonTradingDays: { type: "integer", minimum: 1, maximum: 260 },
  outcomeBuckets: ARR(OUTCOME_BUCKET, 8),
  uncertaintyLevel: STR_ENUM(UNCERTAINTY_LEVELS),
});
const THESIS = OBJ({
  summary: TEXT(1200), whyNow: TEXT(1200),
  evidenceFor: ARR(ID, 24), evidenceAgainst: ARR(ID, 24), uncertainties: ARR(TEXT(300), 12),
});
const ALLOCATION = OBJ({
  capitalRank: { type: "integer", minimum: 1, maximum: 1000 },
  targetWeightBps: UINT, maxWeightBps: UINT, proposedQuantityUnits: UINT,
  quantityScale: { type: "integer", enum: [0] },
  maxCapitalMinor: UINT, maxPlannedLossMinor: UINT, currency: STR_ENUM([ACCOUNT_CURRENCY]),
});
const ENTRY = OBJ({
  orderType: STR_ENUM(["LIMIT"]), limitPriceMicros: UINT, timeInForce: STR_ENUM(["DAY"]),
  regularSessionOnly: { type: "boolean", enum: [true] }, exchangeCalendar: STR_ENUM([CALENDAR_ID]),
  validFrom: INSTANT, authorizedSessionDates: ARR(SESSION_DATE, 3),
  restageEachAuthorizedSession: { type: "boolean" },
});
const TIME_EXIT = OBJ({
  triggerAfterSessionDate: SESSION_DATE,
  submitAt: STR_ENUM(["NEXT_ELIGIBLE_REGULAR_SESSION_OPEN"]),
  orderType: STR_ENUM(["MARKETABLE_LIMIT", "LIMIT"]), collarBps: UINT,
  onHaltOrNonfill: STR_ENUM(["KEEP_PROTECTION_AND_ALERT"]),
});
const TRAILING = OBJ({
  enabled: { type: "boolean" },
  activationPriceMicros: NULLABLE_UINT,
  offsetType: { type: ["string", "null"], enum: ["ABSOLUTE_USD", "PERCENT", null] },
  offsetValueMicrosOrBps: NULLABLE_UINT,
  peakBasis: { type: ["string", "null"], enum: ["LAST_TRADE", "BROKER_HIGH_WATER_MARK", null] },
});
const PROTECTION = OBJ({
  takeProfitPriceMicros: UINT, lossBoundaryPriceMicros: UINT,
  lossOrderType: STR_ENUM(["STOP", "STOP_LIMIT"]), brokerTimeInForce: STR_ENUM(["GTC"]),
  persistentAtBroker: { type: "boolean", enum: [true] }, coverageSession: STR_ENUM(["REGULAR_ONLY"]),
  timeExit: TIME_EXIT, lifecycle: STR_ENUM(["UNTIL_POSITION_FLAT_OR_REPLACED"]), trailing: TRAILING,
});
const REDUCE_OR_SELL_TERMS = OBJ({
  quantityUnits: UINT, orderType: STR_ENUM(["LIMIT", "MARKETABLE_LIMIT"]), limitPriceMicros: NULLABLE_UINT,
  collarBps: NULLABLE_UINT, authorizedSessionDates: ARR(SESSION_DATE, 3),
  onHaltOrNonfill: STR_ENUM(["KEEP_PROTECTION_AND_ALERT", "RETRY_NEXT_ELIGIBLE_SESSION"]),
});
const ACTION = OBJ({
  kind: STR_ENUM(ACTION_KINDS),
  entry: { anyOf: [ENTRY, { type: "null" }] },
  protection: { anyOf: [PROTECTION, { type: "null" }] },
  exit: { anyOf: [REDUCE_OR_SELL_TERMS, { type: "null" }] },
});
const INVALIDATOR = OBJ({
  predicateType: STR_ENUM(INVALIDATOR_TYPES),
  eventTypes: ARR(TEXT(60), 12), sourceClasses: ARR(TEXT(40), 8),
  operand: { anyOf: [OBJ({ metric: TEXT(60), op: STR_ENUM(["gt", "gte", "lt", "lte"]), value: INT, hysteresisBps: UINT }), { type: "null" }] },
  consequence: STR_ENUM(INVALIDATOR_CONSEQUENCES), humanText: TEXT(300),
});
const REVIEW_TRIGGER = OBJ({
  type: STR_ENUM(["EVENT_CLASS", "RESEARCH_DUE_AFTER_SESSION", "PRICE_GAP"]),
  values: ARR(TEXT(60), 12), value: { type: ["string", "null"], maxLength: 40 },
});

/** §7.1 — strict model output. Sol emits only this; the server binds lineage
 *  and the validator writes the envelope. It must never ask the model to
 *  invent version IDs, hashes, reservation state or validator results. */
const MANDATE_PROPOSAL_V1 = OBJ({
  schemaVersion: STR_ENUM(["mandate-proposal.v1"]),
  symbol: SYMBOL,
  decision: STR_ENUM(DECISIONS),
  opportunityRoute: STR_ENUM(OPPORTUNITY_ROUTES),
  asOf: INSTANT,
  reasonCode: { type: ["string", "null"], enum: [...REASON_CODES, null] },
  thesis: THESIS,
  forecast: { anyOf: [FORECAST, { type: "null" }] },
  allocation: { anyOf: [ALLOCATION, { type: "null" }] },
  action: ACTION,
  invalidators: ARR(INVALIDATOR, 12),
  reviewTriggers: ARR(REVIEW_TRIGGER, 8),
  sourceManifest: ARR(SOURCE_MANIFEST_ITEM, 64),
});

const COVERAGE_ROW = OBJ({
  symbol: SYMBOL,
  reviewDirective: STR_ENUM(REVIEW_DIRECTIVES),
  provisionalDisposition: STR_ENUM(PROVISIONAL_DISPOSITIONS),
  reason: TEXT(300), changedSincePrior: { type: "boolean" },
  reasonCode: { type: ["string", "null"], enum: [...REASON_CODES, null] },
});
const RESEARCH_REQUEST = OBJ({
  symbol: SYMBOL, researchPriority: { type: "integer", minimum: 1, maximum: 1000 },
  completionClass: STR_ENUM(COMPLETION_CLASSES), reason: TEXT(300),
  reviewDirective: STR_ENUM(["RESEARCH_NOW", "UPDATE_EXISTING"]),
});
const EMERGENCY_RANK = OBJ({
  emergencyReductionRank: { type: "integer", minimum: 1, maximum: 1000 },
  emergencyRankAsOf: INSTANT, emergencyRankExpiresAfterSession: SESSION_DATE, rationale: TEXT(300),
});
const HOLDING_ANALYSIS = OBJ({
  symbol: SYMBOL,
  decision: STR_ENUM(["HOLD", "REDUCE", "SELL", "ABSTAIN"]),
  reasonCode: { type: ["string", "null"], enum: [...REASON_CODES, null] },
  revisionResult: STR_ENUM(REVISION_RESULTS),
  researchDirective: STR_ENUM(RESEARCH_DIRECTIVES),
  thesisHealth: STR_ENUM(THESIS_HEALTH),
  emergency: EMERGENCY_RANK,
  rationale: TEXT(600),
  mandate: { anyOf: [MANDATE_PROPOSAL_V1, { type: "null" }] },
});
/** §6.3 — the coverage contract. Exactly one row per frozen roster symbol. */
const UNIVERSE_REVIEW_V1 = OBJ({
  schemaVersion: STR_ENUM(["universe-review.v1"]),
  universeVersion: TEXT(40), universeHash: { type: "string", pattern: "^[a-f0-9]{64}$" },
  eligibleCount: { type: "integer", minimum: 0, maximum: 5000 },
  coverage: ARR(COVERAGE_ROW, 5000),
  holdingAnalysis: ARR(HOLDING_ANALYSIS, 500),
  researchRequests: ARR(RESEARCH_REQUEST, 200),
  managerNote: TEXT(1200),
});
const COVERAGE_REPAIR_V1 = OBJ({
  schemaVersion: STR_ENUM(["coverage-repair.v1"]),
  coverage: ARR(COVERAGE_ROW, 5000),
});
/** §6.7 — a later material-event revision of a held position. */
const HOLDING_REVISION_V1 = OBJ({
  schemaVersion: STR_ENUM(["holding-revision.v1"]),
  ...HOLDING_ANALYSIS.properties,
});
/** §7.2 — a revision of an unfilled or partially filled BUY. */
const ENTRY_REVISION_V1 = OBJ({
  schemaVersion: STR_ENUM(["entry-revision.v1"]),
  symbol: SYMBOL,
  entryRevision: STR_ENUM(ENTRY_REVISIONS),
  heldDecision: { type: ["string", "null"], enum: ["HOLD", "REDUCE", "SELL", "ABSTAIN", null] },
  decision: STR_ENUM(DECISIONS),
  reasonCode: { type: ["string", "null"], enum: [...REASON_CODES, null] },
  rationale: TEXT(600),
  mandate: { anyOf: [MANDATE_PROPOSAL_V1, { type: "null" }] },
});
const FACTUAL_PREMISE = OBJ({ premiseId: TEXT(40), text: TEXT(400), claimId: ID, documentVersionId: ID });
const INFERENCE = OBJ({ inferenceId: TEXT(40), text: TEXT(600), premiseIds: ARR(TEXT(40), 16), label: STR_ENUM(["INFERENCE"]) });
const VALUATION_SCENARIO = OBJ({ id: STR_ENUM(["bear", "base", "bull"]), probabilityPpm: UINT,
  assumptions: ARR(OBJ({ name: TEXT(60), value: INT, unit: TEXT(24) }), 24), terminalPriceMicros: UINT });
const VALUATION = OBJ({
  method: STR_ENUM(["reverse_dcf", "dcf", "comparable_multiples", "sum_of_the_parts", "residual_income", "unit_economics", "event_tree"]),
  methodRationale: TEXT(400),
  forwardInputBasis: STR_ENUM(["issuer_guidance", "trailing_and_assumption", "assumption_only"]),
  guidanceClaimId: { type: ["string", "null"], maxLength: 160 },
  scenarios: ARR(VALUATION_SCENARIO, 3),
  preferredValuationZone: { anyOf: [OBJ({ lowMicros: UINT, highMicros: UINT }), { type: "null" }] },
});
/** §6.5 — focused underwriting. Arithmetic is the calculator's; the memo
 *  supplies assumptions and probabilities only. */
const RESEARCH_MEMO_V1 = OBJ({
  schemaVersion: STR_ENUM(["research-memo.v1"]),
  symbol: SYMBOL, asOf: INSTANT,
  checklist: OBJ({
    business: TEXT(1200), whatChanged: TEXT(1200), agreementDisagreement: TEXT(1200), risks: TEXT(1200),
    valuationFramework: TEXT(600), disconfirmingEvidence: TEXT(800), returnAndHorizon: TEXT(800),
    versusAlternatives: TEXT(800), mandateOrAbstain: TEXT(600),
  }),
  factualPremises: ARR(FACTUAL_PREMISE, 64),
  inferences: ARR(INFERENCE, 32),
  valuation: { anyOf: [VALUATION, { type: "null" }] },
  bearCase: TEXT(1200),
  thesisHealth: STR_ENUM(THESIS_HEALTH),
  proposedDecision: STR_ENUM(DECISIONS),
  reasonCode: { type: ["string", "null"], enum: [...REASON_CODES, null] },
  mandate: { anyOf: [MANDATE_PROPOSAL_V1, { type: "null" }] },
});
const FINAL_DECISION_ROW = OBJ({
  symbol: SYMBOL, decision: STR_ENUM(DECISIONS),
  capitalRank: { type: ["integer", "null"], minimum: 1, maximum: 1000 },
  reasonCode: { type: ["string", "null"], enum: [...REASON_CODES, null] },
  fundingState: STR_ENUM(["FUNDED", "UNFUNDED", "NOT_APPLICABLE"]),
  reason: TEXT(300),
});
/** §6.8 — the jointly selected, uniquely ranked expansion basket. */
const PORTFOLIO_SYNTHESIS_V1 = OBJ({
  schemaVersion: STR_ENUM(["portfolio-synthesis.v1"]),
  planClass: STR_ENUM(["EXPANSION"]),
  decisions: ARR(FINAL_DECISION_ROW, 500),
  expansionMandates: ARR(MANDATE_PROPOSAL_V1, 50),
  comparisonNote: TEXT(1200),
});
const EXTRACTED_CLAIM = OBJ({
  claimType: STR_ENUM(["FACT", "GUIDANCE", "EARNINGS_DATE", "MANAGEMENT_CHANGE", "RISK_FACTOR", "CATALYST", "CONTRADICTION"]),
  text: TEXT(400), quote: TEXT(600), documentRef: TEXT(160),
  effectivePeriod: { type: ["string", "null"], maxLength: 40 },
  metric: { type: ["string", "null"], maxLength: 60 },
  lowValue: { type: ["string", "null"], pattern: "^-?(0|[1-9][0-9]*)$" },
  highValue: { type: ["string", "null"], pattern: "^-?(0|[1-9][0-9]*)$" },
  unit: { type: ["string", "null"], maxLength: 24 },
  date: { type: ["string", "null"], pattern: "^\\d{4}-\\d{2}-\\d{2}$" },
  confirmed: { type: ["boolean", "null"] },
  supersedesHint: { type: ["string", "null"], maxLength: 160 },
});
/** §5.3 / §5.6 — Luna extraction: source-bound candidate facts only. */
const FACT_EXTRACTION_V1 = OBJ({
  schemaVersion: STR_ENUM(["fact-extraction.v1"]),
  claims: ARR(EXTRACTED_CLAIM, 40),
  contradictions: ARR(TEXT(300), 8),
  abstained: { type: "boolean" }, abstainReason: TEXT(200),
});
/** §7.3 — independent claim verification; the generation never self-attests. */
const CLAIM_VERIFICATION_V1 = OBJ({
  schemaVersion: STR_ENUM(["claim-verification.v1"]),
  verdicts: ARR(OBJ({ claimId: ID, verdict: STR_ENUM(VERIFICATION_VERDICTS), spanIds: ARR(ID, 8), note: TEXT(200) }), 64),
});
/** Appendix B — finalizeEventRevision. */
const EVENT_REVISION_V1 = OBJ({
  schemaVersion: STR_ENUM(["event-revision.v1"]),
  symbol: SYMBOL,
  decision: STR_ENUM(DECISIONS),
  reasonCode: { type: ["string", "null"], enum: [...REASON_CODES, null] },
  materiality: STR_ENUM(["MATERIAL", "NOT_MATERIAL", "UNDETERMINED"]),
  entryRevision: { type: ["string", "null"], enum: [...ENTRY_REVISIONS, null] },
  rationale: TEXT(800),
  mandate: { anyOf: [MANDATE_PROPOSAL_V1, { type: "null" }] },
});
const POSTMORTEM_V1 = OBJ({
  schemaVersion: STR_ENUM(["postmortem.v1"]),
  decisionId: ID,
  errorAttribution: STR_ENUM(["evidence", "retrieval", "interpretation", "valuation", "sizing", "timing", "execution", "exogenous_surprise", "none"]),
  lesson: TEXT(800), proposedEvalCase: TEXT(600), operatorAdjudicationRequired: { type: "boolean" },
});

const SCHEMAS = deepFreeze({
  "mandate-proposal.v1": MANDATE_PROPOSAL_V1,
  "universe-review.v1": UNIVERSE_REVIEW_V1,
  "coverage-repair.v1": COVERAGE_REPAIR_V1,
  "holding-revision.v1": HOLDING_REVISION_V1,
  "entry-revision.v1": ENTRY_REVISION_V1,
  "research-memo.v1": RESEARCH_MEMO_V1,
  "portfolio-synthesis.v1": PORTFOLIO_SYNTHESIS_V1,
  "fact-extraction.v1": FACT_EXTRACTION_V1,
  "claim-verification.v1": CLAIM_VERIFICATION_V1,
  "event-revision.v1": EVENT_REVISION_V1,
  "postmortem.v1": POSTMORTEM_V1,
});
function schemaHash(schemaOrVersion) {
  const schema = typeof schemaOrVersion === "string" ? SCHEMAS[schemaOrVersion] : schemaOrVersion;
  if (!schema) throw Object.assign(new Error(`unknown schema ${schemaOrVersion}`), { code: "UNKNOWN_SCHEMA" });
  return sha256(schema);
}
function schemaHashes() {
  return Object.fromEntries(Object.keys(SCHEMAS).map((k) => [k, schemaHash(k)]));
}

/** GENERATE the OpenAI Strict Structured Outputs subset from the canonical
 *  schema (§12.1: never hand-maintained alongside it). Every object lists
 *  every property as required with additionalProperties false; optional
 *  values are expressed as nullable types, which the canonical schemas
 *  already do. Keywords the strict mode does not accept are removed. */
const STRICT_UNSUPPORTED = new Set(["description", "$comment", "examples", "default", "minLength", "maxLength",
  "minItems", "maxItems", "pattern", "format", "minimum", "maximum", "multipleOf"]);
function strictOutputSchema(schema, { name = "output" } = {}) {
  const walk = (node) => {
    if (Array.isArray(node)) return node.map(walk);
    if (!node || typeof node !== "object") return node;
    const out = {};
    for (const [k, v] of Object.entries(node)) {
      if (STRICT_UNSUPPORTED.has(k)) continue;
      out[k] = k === "properties"
        ? Object.fromEntries(Object.entries(v).map(([pk, pv]) => [pk, walk(pv)]))
        : walk(v);
    }
    if (out.type === "object" || (out.properties && !out.type)) {
      out.type = "object";
      out.additionalProperties = false;
      out.required = Object.keys(out.properties || {});
    }
    return out;
  };
  const strict = walk(schema);
  return { name, strict: true, schema: strict, schemaHash: sha256(schema), strictHash: sha256(strict) };
}

/* ── A MINIMAL, EXACT VALIDATOR for the canonical schemas ──────────────────
   Enough for the server's own checks and the fixtures without a dependency;
   _investorApiSchemas.js compiles the same schemas through pinned Ajv 2020
   at cold start (§12.1) and is the authority at the request boundary. */
function typeOf(v) {
  if (v === null) return "null";
  if (Array.isArray(v)) return "array";
  if (typeof v === "number") return Number.isInteger(v) ? "integer" : "number";
  return typeof v;
}
function validateAgainst(schema, value, path = "$", errors = [], depth = 0) {
  if (depth > 40) { errors.push({ path, error: "schema depth exceeded" }); return errors; }
  if (!schema || typeof schema !== "object") return errors;
  if (schema.anyOf) {
    const ok = schema.anyOf.some((s) => validateAgainst(s, value, path, [], depth + 1).length === 0);
    if (!ok) errors.push({ path, error: "matches no anyOf branch" });
    return errors;
  }
  if (schema.const !== undefined && JSON.stringify(value) !== JSON.stringify(schema.const)) {
    errors.push({ path, error: `must equal ${JSON.stringify(schema.const)}` }); return errors;
  }
  if (schema.type) {
    const types = Array.isArray(schema.type) ? schema.type : [schema.type];
    const t = typeOf(value);
    const ok = types.includes(t) || (t === "integer" && types.includes("number"));
    if (!ok) { errors.push({ path, error: `expected ${types.join("|")}, got ${t}` }); return errors; }
  }
  if (schema.enum && !schema.enum.some((e) => e === value)) errors.push({ path, error: `not in enum` });
  if (typeof value === "string") {
    if (schema.pattern && !new RegExp(schema.pattern).test(value)) errors.push({ path, error: `pattern ${schema.pattern}` });
    if (schema.minLength != null && value.length < schema.minLength) errors.push({ path, error: "too short" });
    if (schema.maxLength != null && value.length > schema.maxLength) errors.push({ path, error: "too long" });
  }
  if (typeof value === "number") {
    if (schema.minimum != null && value < schema.minimum) errors.push({ path, error: `below minimum ${schema.minimum}` });
    if (schema.maximum != null && value > schema.maximum) errors.push({ path, error: `above maximum ${schema.maximum}` });
  }
  if (Array.isArray(value)) {
    if (schema.maxItems != null && value.length > schema.maxItems) errors.push({ path, error: "too many items" });
    if (schema.minItems != null && value.length < schema.minItems) errors.push({ path, error: "too few items" });
    if (schema.items) value.forEach((v, i) => validateAgainst(schema.items, v, `${path}[${i}]`, errors, depth + 1));
  }
  if (value && typeof value === "object" && !Array.isArray(value)) {
    const props = schema.properties || {};
    for (const k of schema.required || []) if (!(k in value)) errors.push({ path: `${path}.${k}`, error: "required" });
    for (const [k, v] of Object.entries(value)) {
      if (props[k]) validateAgainst(props[k], v, `${path}.${k}`, errors, depth + 1);
      else if (schema.additionalProperties === false) errors.push({ path: `${path}.${k}`, error: "unknown field" });
    }
  }
  return errors;
}
function validate(schemaVersion, value) {
  const schema = SCHEMAS[schemaVersion];
  if (!schema) return { ok: false, errors: [{ path: "$", error: `unknown schema ${schemaVersion}` }] };
  const errors = validateAgainst(schema, value);
  return { ok: errors.length === 0, errors: errors.slice(0, 40), schemaVersion, schemaHash: schemaHash(schemaVersion) };
}

/* ── §7.1 worked example, used by fixtures and the arithmetic cross-check ─ */
const EXAMPLE_MANDATE_PROPOSAL = deepFreeze({
  schemaVersion: "mandate-proposal.v1", symbol: "XYZ", decision: "BUY", opportunityRoute: "expectations_gap",
  asOf: "2026-09-03T12:55:00Z", reasonCode: null,
  thesis: { summary: "Plain-language investment case", whyNow: "What changed and why price is attractive now",
    evidenceFor: ["claim_1", "claim_2"], evidenceAgainst: ["claim_3"], uncertainties: ["Consensus estimates unavailable"] },
  forecast: {
    basis: { referencePriceMicros: "43250000", referencePriceType: "BUY_LIMIT", referenceTime: "2026-09-03T12:55:00Z",
      currency: "USD", returnConvention: "ARITHMETIC_TOTAL_RETURN", conditionalOn: "ENTRY_FILLED_AT_REFERENCE_PRICE",
      corporateActionPolicy: "SPLIT_AND_CASH_DISTRIBUTION_ADJUSTED", horizonTradingDays: 40,
      estimatedRoundTripCostMicrosPerShare: "40000" },
    fillProbabilityByExpiryPpm: "650000", noFillOutcome: "CASH_RETURN_OVER_AUTHORIZED_SESSIONS", horizonTradingDays: 40,
    outcomeBuckets: [
      { id: "bear", interval: "[-INF,-500bps)", probabilityPpm: "250000", terminalPriceMicros: "36000000" },
      { id: "base", interval: "[-500bps,2000bps)", probabilityPpm: "500000", terminalPriceMicros: "48000000" },
      { id: "bull", interval: "[2000bps,+INF]", probabilityPpm: "250000", terminalPriceMicros: "55000000" },
    ],
    uncertaintyLevel: "MEDIUM",
  },
  allocation: { capitalRank: 3, targetWeightBps: "600", maxWeightBps: "700", proposedQuantityUnits: "86",
    quantityScale: 0, maxCapitalMinor: "372000", maxPlannedLossMinor: "52000", currency: "USD" },
  action: {
    kind: "BUY",
    entry: { orderType: "LIMIT", limitPriceMicros: "43250000", timeInForce: "DAY", regularSessionOnly: true,
      exchangeCalendar: "XNYS", validFrom: "2026-09-03T13:35:00Z",
      authorizedSessionDates: ["2026-09-03", "2026-09-04"], restageEachAuthorizedSession: true },
    protection: { takeProfitPriceMicros: "49000000", lossBoundaryPriceMicros: "37250000", lossOrderType: "STOP",
      brokerTimeInForce: "GTC", persistentAtBroker: true, coverageSession: "REGULAR_ONLY",
      timeExit: { triggerAfterSessionDate: "2026-10-30", submitAt: "NEXT_ELIGIBLE_REGULAR_SESSION_OPEN",
        orderType: "MARKETABLE_LIMIT", collarBps: "75", onHaltOrNonfill: "KEEP_PROTECTION_AND_ALERT" },
      lifecycle: "UNTIL_POSITION_FLAT_OR_REPLACED",
      trailing: { enabled: false, activationPriceMicros: null, offsetType: null, offsetValueMicrosOrBps: null, peakBasis: null } },
    exit: null,
  },
  invalidators: [{ predicateType: "FACT_EVENT", eventTypes: ["GUIDANCE_WITHDRAWN"], sourceClasses: ["SEC", "ISSUER_IR"],
    operand: null, consequence: "PAUSE_ENTRY_AND_QUEUE_REVISION", humanText: "Company withdraws its current guidance" }],
  reviewTriggers: [
    { type: "EVENT_CLASS", values: ["EARNINGS", "NEW_8K", "CEO_OR_CFO_CHANGE", "MATERIAL_REGULATORY_EVENT"], value: null },
    { type: "RESEARCH_DUE_AFTER_SESSION", values: [], value: "2026-10-01" },
  ],
  sourceManifest: [
    { claimId: "claim_1", documentVersionId: "docv_1", publishedAt: "2026-08-28T20:01:00Z", retrievedAt: "2026-08-28T20:02:00Z" },
    { claimId: "claim_2", documentVersionId: "docv_2", publishedAt: "2026-08-29T12:00:00Z", retrievedAt: "2026-08-29T12:03:00Z" },
    { claimId: "claim_3", documentVersionId: "docv_3", publishedAt: "2026-09-02T14:00:00Z", retrievedAt: "2026-09-02T14:04:00Z" },
  ],
});

/* ── OWNER OVERRIDES (setRiskMandate, setEmergencyRiskPolicy, setBudget) ─ */
function getPath(o, p) { return p.split(".").reduce((a, k) => (a == null ? undefined : a[k]), o); }
function setPath(o, p, v) {
  const keys = p.split("."); let cur = o;
  for (const k of keys.slice(0, -1)) { if (!cur[k] || typeof cur[k] !== "object") cur[k] = {}; cur = cur[k]; }
  cur[keys[keys.length - 1]] = v;
}
/** PURE. Apply owner overrides to the risk mandate inside the absolute
 *  bounds; every applied and refused override is reported. */
function applyRiskMandateOverrides(overrides = null) {
  const effective = JSON.parse(JSON.stringify(RISK_MANDATE));
  const applied = [], refused = [];
  for (const [path, raw] of Object.entries(overrides || {})) {
    const bound = RISK_MANDATE_BOUNDS[path];
    if (!bound) { refused.push({ path, reason: "not an overridable risk parameter" }); continue; }
    if (typeof raw !== "string" || !/^(0|[1-9][0-9]*)$/.test(raw)) { refused.push({ path, reason: "must be a canonical integer string" }); continue; }
    const v = BigInt(raw);
    if (v < BigInt(bound.min) || v > BigInt(bound.max)) { refused.push({ path, reason: `outside [${bound.min}, ${bound.max}]` }); continue; }
    const declared = getPath(RISK_MANDATE, path);
    if (declared === raw) continue;
    setPath(effective, path, raw);
    applied.push({ path, declared, effective: raw });
  }
  return { riskMandate: effective, applied, refused };
}
/** PURE. Validate an owner-approved emergency policy document. It is active
 *  only when APPROVED by a named owner with a hash over its own content, and
 *  its permitted operations can never grow past the template's. */
function activeEmergencyPolicy(stored = null) {
  if (!stored || typeof stored !== "object") return { active: false, policy: EMERGENCY_RISK_POLICY_TEMPLATE, reason: "no owner-approved policy; template is inactive" };
  const problems = [];
  if (stored.schemaVersion !== EMERGENCY_RISK_POLICY_TEMPLATE.schemaVersion) problems.push("schemaVersion");
  if (stored.status !== "APPROVED" || !stored.approvedBy || !Number.isFinite(Number(stored.approvedAtMs))) problems.push("approval");
  const permitted = Array.isArray(stored.permittedOperations) ? stored.permittedOperations : [];
  if (permitted.some((op) => !EMERGENCY_RISK_POLICY_TEMPLATE.permittedOperations.includes(op))) problems.push("permittedOperations widened");
  if (EMERGENCY_RISK_POLICY_TEMPLATE.forbiddenOperations.some((op) => permitted.includes(op))) problems.push("forbidden operation permitted");
  const { policyHash, ...content } = stored;
  if (!policyHash || policyHash !== sha256(content)) problems.push("policyHash");
  if (problems.length) return { active: false, policy: EMERGENCY_RISK_POLICY_TEMPLATE, reason: `stored policy rejected: ${problems.join(", ")}` };
  return { active: true, policy: stored, reason: null };
}

/* ── IDENTITY ──────────────────────────────────────────────────────────── */
function policyIdentity({ riskMandate = RISK_MANDATE, emergencyPolicy = null } = {}) {
  const riskPolicyHash = sha256({ schemaVersion: riskMandate.schemaVersion, riskMandate });
  const modelPolicyHash = sha256({ ROLE_MODELS, FORBIDDEN_INVESTMENT_MODELS, ROSTER_FILTER_MODELS_ALLOWED });
  const costPolicyHash = sha256(MODEL_RATES);
  const cutoffsHash = sha256(CUTOFFS_ET);
  const hashes = schemaHashes();
  const policyHash = sha256({ POLICY_VERSION, riskPolicyHash, modelPolicyHash, costPolicyHash, cutoffsHash,
    toolPolicyHash: sha256(TOOL_POLICY), schemaHashes: hashes, calendarId: CALENDAR_ID });
  return { policyVersion: POLICY_VERSION, policyHash, riskPolicyHash, modelPolicyHash, costPolicyHash, cutoffsHash,
    toolPolicyHash: sha256(TOOL_POLICY), schemaHashes: hashes, calendarId: CALENDAR_ID,
    emergencyPolicyHash: emergencyPolicy && emergencyPolicy.policyHash ? emergencyPolicy.policyHash : null };
}

/** Load the active policy: the frozen defaults plus validated owner
 *  overrides from the control document. Pass `control` to keep it pure;
 *  without it the control document is read. */
async function loadActive({ control = null } = {}) {
  let ctrl = control;
  if (!ctrl) {
    const A = require("./_investorAdmin");
    const snap = await A.col(A.COL.control).doc("control").get();
    ctrl = snap.exists ? snap.data() : {};
  }
  return loadActiveSync(ctrl);
}
function loadActiveSync(ctrl = {}) {
  const risk = applyRiskMandateOverrides(ctrl.riskMandateOverrides || null);
  const emergency = activeEmergencyPolicy(ctrl.emergencyRiskPolicy || null);
  const identity = policyIdentity({ riskMandate: risk.riskMandate, emergencyPolicy: emergency.active ? emergency.policy : null });
  const budget = budgetPolicy();
  return {
    ...identity,
    roleModels: ROLE_MODELS, forbiddenInvestmentModels: FORBIDDEN_INVESTMENT_MODELS,
    rates: MODEL_RATES, cutoffs: CUTOFFS_ET, budget, toolPolicy: TOOL_POLICY,
    riskMandate: risk.riskMandate, riskOverridesApplied: risk.applied, riskOverridesRefused: risk.refused,
    emergencyRiskPolicy: emergency.policy, emergencyRiskPolicyActive: emergency.active,
    emergencyRiskPolicyReason: emergency.reason,
    maxConcurrentResearchJobs: TOOL_POLICY.maxResearchConcurrency,
    loadedAtMs: Date.now(),
  };
}
/** PURE. Does a recorded hash set match the active policy exactly? */
function validateHashSet(expected = {}, active = null) {
  const a = active || loadActiveSync({});
  const mismatches = [];
  for (const k of ["policyHash", "riskPolicyHash", "modelPolicyHash", "costPolicyHash"]) {
    if (expected[k] !== undefined && expected[k] !== a[k]) mismatches.push({ key: k, expected: expected[k], active: a[k] });
  }
  if (expected.schemaHash !== undefined) {
    const known = Object.values(a.schemaHashes || {});
    if (!known.includes(expected.schemaHash)) mismatches.push({ key: "schemaHash", expected: expected.schemaHash, active: null });
  }
  return { ok: mismatches.length === 0, mismatches };
}

module.exports = {
  POLICY_VERSION, CALENDAR_ID, ACCOUNT_CURRENCY,
  canonical, sha256,
  ROLE_MODELS, FORBIDDEN_INVESTMENT_MODELS, ROSTER_FILTER_MODELS_ALLOWED,
  MODEL_RATES, NANO_PER_MINOR, costNanoUsd, costMinor,
  CUTOFFS_ET, budgetPolicy, TOOL_POLICY,
  RISK_MANDATE, RISK_MANDATE_BOUNDS, applyRiskMandateOverrides,
  EMERGENCY_RISK_POLICY_TEMPLATE, activeEmergencyPolicy,
  DECISIONS, EXECUTABLE_DECISIONS, REVIEW_DIRECTIVES, PROVISIONAL_DISPOSITIONS, REASON_CODES, COMPLETION_CLASSES,
  OPPORTUNITY_ROUTES, ACTION_KINDS, REVISION_RESULTS, RESEARCH_DIRECTIVES, ENTRY_REVISIONS,
  INVALIDATOR_TYPES, INVALIDATOR_CONSEQUENCES, THESIS_HEALTH, UNCERTAINTY_LEVELS, VERIFICATION_VERDICTS,
  SCHEMAS, schemaHash, schemaHashes, strictOutputSchema, validate, validateAgainst,
  EXAMPLE_MANDATE_PROPOSAL,
  policyIdentity, loadActive, loadActiveSync, validateHashSet,
};
