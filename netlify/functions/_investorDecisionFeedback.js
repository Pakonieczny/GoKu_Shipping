/* Investor_AI — frozen decision-matrix counterfactuals and forward outcomes. */
"use strict";

const crypto = require("crypto");
const A = require("./_investorAdmin");
const M = require("./_investorMarket");
const S = require("./_investorSignal");
const V = require("./_investorVariants");

const OBSERVATIONS = A.COL.shadowObservations;
const HORIZONS = Object.freeze([1, 3, 5, 10, 14]);

function sha(value) {
  return crypto.createHash("sha256").update(String(value)).digest("hex");
}
function round(value, places = 4) {
  const p = 10 ** places;
  return Number((Math.round(Number(value) * p) / p).toFixed(places));
}
function componentFor(variantId) {
  return ({ I: "formation_horizon", J: "formation_horizon", K: "temporal_weighting",
    L: "intelligence_weighting", M: "size_aggregation", N: "event_threshold" })[variantId]
    || "entry_exit_policy";
}

/** Score the complete frozen family, including explicit no-trade outputs. */
function evaluateFrozenPolicies({ baseInput, signalContexts = {} } = {}) {
  if (!baseInput || !baseInput.symbol) throw new Error("counterfactual baseInput.symbol required");
  const policies = {};
  for (const variant of V.VARIANTS) {
    const cfg = { ...V.configFor(variant.id), requireCalibratedEdge: false };
    const sc = signalContexts[cfg.signalWindow || 12] || {};
    const rank = sc.ranks && sc.ranks[baseInput.symbol];
    const zStat = sc.zBySymbol && sc.zBySymbol[baseInput.symbol];
    const crowd = S.sectorCrowding(sc.ranks || {}, S.sectorOf,
      cfg.entryRank == null ? S.ENTRY_RANK : cfg.entryRank);
    const result = S.evaluateCandidate({ ...baseInput, rank, zStat, cfg,
      sectorTailFraction: crowd.fractionInTail[S.sectorOf(baseInput.symbol)] ?? 0 });
    policies[variant.id] = {
      variantId: variant.id, component: componentFor(variant.id),
      action: result.pass ? "trade" : "no_trade",
      firstBlock: result.firstBlock || null, blockedBy: result.blockedBy || [],
      signalWindow: cfg.signalWindow || 12, maxHoldDays: cfg.maxHoldDays,
      rank: Number.isFinite(Number(rank)) ? round(rank, 6) : null,
      z: result.z, cumulativeResidualBps: result.cumResidualBps,
      sizeMultiplier: Number(result.sizing && result.sizing.combined) || 0,
      sizeAggregation: result.sizing && result.sizing.combinationRule || null,
      intelligenceRawRisk: Number(result.intelligencePolicy
        && result.intelligencePolicy.adverseRiskScore) || 0,
      intelligenceDecisionRisk: Number(result.intelligencePolicy
        && result.intelligencePolicy.decisionRiskScore) || 0,
      temporalRawRisk: Number(result.intelligencePolicy && result.intelligencePolicy.temporalPolicy
        && result.intelligencePolicy.temporalPolicy.rawRiskScore) || 0,
      temporalDecisionRisk: Number(result.intelligencePolicy && result.intelligencePolicy.temporalPolicy
        && result.intelligencePolicy.temporalPolicy.decisionRiskScore) || 0,
      expectedGrossBps: Number(result.cost && result.cost.expectedGrossBps) || 0,
      roundTripCostBps: Number(result.cost && result.cost.roundTripBps) || 0,
      expectedCause: baseInput.cause || null,
      hardSafetyBlocksPreserved: true,
    };
  }
  return {
    schemaVersion: "frozen-decision-counterfactual-v1",
    variantsHash: V.variantsHash(), policyCount: V.VARIANTS.length,
    includesNoTrade: true, policies,
  };
}

function observationId(experimentHash, cycleId, symbol) {
  return sha(`${experimentHash}|${cycleId}|${symbol}`).slice(0, 40);
}

async function recordObservation({ experimentHash, cycleId, symbol, decisionAtMs,
  decisionDate, initialPrice, marketProvenance, counterfactuals, sector = "other",
  regime = null, decisionManifestHash = null, manifestCoverage = null,
  configurationIdentity = null, cause = null, activeVerdict = null,
  strictVerdict = null, operatingState = null,
  modeledRoundTripBps = null } = {}) {
  if (!/^[a-f0-9]{64}$/.test(String(experimentHash || ""))) {
    throw new Error("decision observation requires experimentHash");
  }
  if (!(Number(initialPrice) > 0) || !counterfactuals) return { recorded: false, reason: "missing_price_or_policies" };
  const ref = A.col(OBSERVATIONS).doc(observationId(experimentHash, cycleId, symbol));
  const missingInformation = !!(manifestCoverage
    && (Number(manifestCoverage.missing) > 0 || Number(manifestCoverage.stale) > 0
      || Number(manifestCoverage.invalid) > 0));
  const strictEligible = !!(strictVerdict && strictVerdict.pass);
  const activeEligible = !!(activeVerdict && activeVerdict.pass);
  const cohorts = {
    strict: strictEligible ? "strict_eligible" : "strict_rejected",
    exploratory: activeEligible && !strictEligible ? "relaxed_only"
      : (activeEligible ? "strict_and_active" : "active_rejected"),
    information: missingInformation ? "missing_or_stale_information" : "complete_information",
    finding: cause === S.CAUSE.HARD_NEWS ? "adverse_finding" : "no_adverse_finding",
    execution: operatingState === "exploratory_auto"
      ? "exploratory_auto_unvalidated"
      : (operatingState === "limited_auto"
        ? "validated_limited_auto" : (operatingState || "observation")),
  };
  const row = {
    schemaVersion: "decision-outcome-observation-v2", experimentHash, cycleId, symbol,
    decisionAtMs: Number(decisionAtMs), decisionDate, initialPrice: Number(initialPrice),
    marketProvenance, variantsHash: counterfactuals.variantsHash,
    policies: counterfactuals.policies, sector, regime: regime ? {
      vixNorm: Number(regime.vixNorm) || null, cor3m: Number(regime.cor3m) || null,
    } : null,
    decisionManifestHash, manifestCoverage, configurationIdentity,
    causeAtDecision: cause, activeVerdict, strictVerdict, operatingState,
    modeledRoundTripBps: Number.isFinite(Number(modeledRoundTripBps))
      ? Number(modeledRoundTripBps) : null,
    cohorts, populationKey: Object.values(cohorts).join("|"),
    horizons: HORIZONS, marks: {}, status: "pending", affectsPolicy: false,
    ...A.envelope({ created_by: "decisionFeedback.recordObservation" }),
  };
  /* Real Firestore supports create(), which is one write and cannot overwrite
     marks on a worker retry. The deterministic in-memory harness uses the
     transaction fallback with the same create-once semantics. */
  if (typeof ref.create === "function") {
    try { await ref.create(row); return { recorded: true, id: ref.id }; }
    catch (e) {
      if ([6, "6", "already-exists", "ALREADY_EXISTS"].includes(e && (e.code || e.status))) {
        return { recorded: true, duplicate: true, id: ref.id };
      }
      throw e;
    }
  }
  return A.runTransaction(async (tx) => {
    const existing = await tx.get(ref);
    if (existing.exists) return { recorded: true, duplicate: true, id: ref.id };
    tx.set(ref, row);
    return { recorded: true, id: ref.id };
  });
}

function policyOutcome(policy, grossBps) {
  const netBps = grossBps - (Number(policy.roundTripCostBps) || 0);
  let lesson;
  if (policy.action === "trade") {
    lesson = grossBps > 0 && netBps <= 0 ? "signal_right_cost_erased_edge"
      : netBps > 0 ? "profitable_trade" : "failed_trade";
  } else {
    lesson = netBps > 0 ? "missed_opportunity" : "correct_avoidance";
  }
  return { action: policy.action, grossBps: round(grossBps), netOpportunityBps: round(netBps), lesson };
}

function mean(values) {
  const clean = values.map(Number).filter(Number.isFinite);
  return clean.length ? clean.reduce((sum, x) => sum + x, 0) / clean.length : null;
}

function eventTime(event) {
  for (const value of [event && event.effectiveAtMs, event && event.eventAtMs,
    event && event.publishedAtMs, event && event.asOfMs]) {
    if (Number.isFinite(Number(value))) return Number(value);
    const parsed = Date.parse(value || ""); if (Number.isFinite(parsed)) return parsed;
  }
  return null;
}

/** A conservative follow-up label. It never turns absence of later evidence
 * into proof; it says supported, contradicted, not contradicted, or unknown. */
function assessCause(row, snapshot, horizonGrossBps = null) {
  if (!snapshot || !(Number(snapshot.asOfMs) >= Number(row.decisionAtMs))) {
    return { status: "unknown", reason: "no later point-in-time dossier" };
  }
  const relevant = (snapshot.events || []).filter((event) => {
    const at = eventTime(event);
    const truth = Number(event.truthProbability ?? event.confidence ?? 0);
    const adverse = event.direction === "adverse" || event.adverse === true
      || Number(event.riskScore ?? event.materialityScore ?? 0) > 0;
    return at != null && at >= Number(row.decisionAtMs) - 24 * 3600e3
      && at <= Number(row.decisionAtMs) + 14 * 86400e3 && truth >= 0.7 && adverse;
  });
  const original = row.causeAtDecision;
  if (original === S.CAUSE.HARD_NEWS) {
    return relevant.length
      ? { status: "appeared_supported", eventIds: relevant.slice(0, 5)
        .map((e) => e.eventId || e.id).filter(Boolean) }
      : { status: "unconfirmed", reason: "no later corroborated adverse event in the horizon" };
  }
  if ([S.CAUSE.NONE, S.CAUSE.ABNORMAL_ACTIVITY].includes(original)) {
    if (relevant.length) return { status: "appeared_contradicted",
      eventIds: relevant.slice(0, 5).map((e) => e.eventId || e.id).filter(Boolean) };
    return { status: "not_contradicted", outcomeDirection:
      Number(horizonGrossBps) > 0 ? "reversal" : "no_reversal",
      reason: "absence of a later corroborated event is not proof of no cause" };
  }
  return relevant.length
    ? { status: "later_adverse_cause_found",
      eventIds: relevant.slice(0, 5).map((e) => e.eventId || e.id).filter(Boolean) }
    : { status: "remained_unresolved" };
}

/** Add one immutable daily mark and resolve frozen 1/3/5/10/14-session outcomes. */
async function updateObservations({ experimentHash, session, lastPrice = {},
  marketProvenanceBySymbol = {}, intelligenceBySymbol = {} } = {}) {
  if (!(session && session.tradingDay && session.dailyFinalized === true && session.date)) {
    return { updated: 0, resolved: 0, skipped: "daily_session_not_finalized" };
  }
  /* Bound the scan to this immutable experiment and unresolved observations.
     The paired composite index is shipped in the deployment manifest. */
  const snap = await A.col(OBSERVATIONS)
    .where("experimentHash", "==", experimentHash)
    .where("status", "==", "pending").get();
  let updated = 0, resolved = 0;
  const working = [];
  for (const doc of snap.docs) {
    const row = doc.data();
    if (session.date <= row.decisionDate) continue;
    const sessionOffset = M.tradingSessionsBetween(row.decisionDate, session.date,
      { maxSessions: HORIZONS.at(-1) + 5 });
    if (!(sessionOffset >= 1)) continue;
    const price = Number(lastPrice[row.symbol]);
    const provenance = marketProvenanceBySymbol[row.symbol];
    const markValid = price > 0 && provenance
      && /^[a-f0-9]{64}$/.test(String(provenance.sourceSha256 || ""));
    const marks = { ...(row.marks || {}) };
    if (markValid) marks[session.date] = { price, sessionOffset,
      marketProvenance: provenance };
    const dates = Object.keys(marks).sort();
    const outcomes = { ...(row.outcomes || {}) };
    const byOffset = new Map(dates.map((date) => [Number(marks[date].sessionOffset),
      { date, ...marks[date] }]).filter(([offset]) => offset >= 1));
    for (const horizon of HORIZONS) {
      const mark = byOffset.get(horizon);
      if (outcomes[horizon] && outcomes[horizon].status !== "missing_mark") continue;
      if (!mark) {
        if (sessionOffset >= horizon && !outcomes[horizon]) outcomes[horizon] = {
          status: "missing_mark", sessionOffset: horizon, date: null, price: null,
          grossBps: null, netModeledBps: null,
          reason: "no provenance-bound finalized mark for the exact trading-session horizon",
        };
        continue;
      }
      const grossBps = (Number(mark.price) / Number(row.initialPrice) - 1) * 1e4;
      const path = Array.from({ length: horizon }, (_, i) => byOffset.get(i + 1));
      const pathComplete = path.every(Boolean);
      const pathBps = path.filter(Boolean)
        .map((item) => (Number(item.price) / Number(row.initialPrice) - 1) * 1e4);
      const modeledCostBps = Number(row.modeledRoundTripBps) || 0;
      outcomes[horizon] = {
        status: "observed", sessionOffset: horizon,
        date: mark.date, price: Number(mark.price), grossBps: round(grossBps),
        netModeledBps: round(grossBps - modeledCostBps), modeledCostBps: round(modeledCostBps),
        pathComplete,
        maximumFavorableExcursionBps: pathComplete ? round(Math.max(...pathBps)) : null,
        maximumAdverseExcursionBps: pathComplete ? round(Math.min(...pathBps)) : null,
        causeAssessment: assessCause(row, intelligenceBySymbol[row.symbol], grossBps),
        policies: Object.fromEntries(Object.entries(row.policies || {})
          .map(([id, policy]) => [id, policyOutcome(policy, grossBps)])),
      };
    }
    const done = sessionOffset >= HORIZONS.at(-1)
      && HORIZONS.every((horizon) => !!outcomes[horizon]);
    working.push({ doc, row, marks, dates, outcomes, done, sessionOffset });
  }

  /* The equal-weight eligible universe and same-sector cohort are explicit
     paper benchmarks. Cohorts are keyed to the same cycle and horizon, so
     strict/relaxed/missing/adverse populations remain labels on the record
     and are never pooled into a strategy-effectiveness statistic. */
  const marketGroups = new Map(), sectorGroups = new Map();
  for (const item of working) {
    for (const horizon of HORIZONS) {
      const outcome = item.outcomes[horizon];
      if (!outcome || outcome.status !== "observed" || !Number.isFinite(Number(outcome.grossBps))) continue;
      const key = `${item.row.cycleId}|${horizon}|${outcome.date}`;
      const sectorKey = `${key}|${item.row.sector || "other"}`;
      if (!marketGroups.has(key)) marketGroups.set(key, []);
      if (!sectorGroups.has(sectorKey)) sectorGroups.set(sectorKey, []);
      marketGroups.get(key).push(Number(outcome.grossBps));
      sectorGroups.get(sectorKey).push(Number(outcome.grossBps));
    }
  }
  for (const item of working) {
    for (const horizon of HORIZONS) {
      const outcome = item.outcomes[horizon];
      if (!outcome || outcome.status !== "observed" || !Number.isFinite(Number(outcome.grossBps))) continue;
      const key = `${item.row.cycleId}|${horizon}|${outcome.date}`;
      const sectorKey = `${key}|${item.row.sector || "other"}`;
      const peers = marketGroups.get(key) || [];
      const sectorPeers = sectorGroups.get(sectorKey) || [];
      const marketBps = mean(peers), sectorBps = mean(sectorPeers);
      outcome.benchmarks = {
        marketKind: "equal_weight_ranked_universe", marketPeerCount: peers.length,
        marketGrossBps: marketBps == null ? null : round(marketBps),
        sectorKind: "equal_weight_same_sector", sectorPeerCount: sectorPeers.length,
        sectorGrossBps: sectorBps == null ? null : round(sectorBps),
      };
      outcome.marketAdjustedGrossBps = marketBps == null
        ? null : round(outcome.grossBps - marketBps);
      outcome.sectorAdjustedGrossBps = sectorBps == null
        ? null : round(outcome.grossBps - sectorBps);
      outcome.marketAdjustedNetModeledBps = marketBps == null
        ? null : round(outcome.netModeledBps - marketBps);
      outcome.sectorAdjustedNetModeledBps = sectorBps == null
        ? null : round(outcome.netModeledBps - sectorBps);
    }
    await item.doc.ref.set({ marks: item.marks, outcomes: item.outcomes,
      sessionsObserved: item.dates.length,
      lastSessionOffset: item.sessionOffset,
      horizonCoverage: { observed: HORIZONS.filter((h) =>
        item.outcomes[h] && item.outcomes[h].status === "observed").length,
      missing: HORIZONS.filter((h) =>
        item.outcomes[h] && item.outcomes[h].status === "missing_mark").length,
      required: HORIZONS.length },
      status: item.done ? "resolved" : "pending",
      resolvedAtMs: item.done ? Date.now() : null,
      updatedAt: A.FV.serverTimestamp() }, { merge: true });
    updated += 1; if (item.done) resolved += 1;
  }
  return { updated, resolved };
}

function attributeClosed(position, { grossBps, netBps, costBps, exitPrice,
  exitReason, heldDays } = {}) {
  const features = position && position.contextObservation && position.contextObservation.features || {};
  const peak = Math.max(Number(position && position.peakPrice) || 0, Number(exitPrice) || 0);
  const entry = Number(position && position.entryPrice) || 0;
  const peakBps = entry > 0 ? (peak / entry - 1) * 1e4 : null;
  const givebackBps = peakBps == null ? null : Math.max(0, peakBps - Number(grossBps || 0));
  const sizeMult = Number(position && position.sizeMult);
  return {
    schemaVersion: "decision-component-attribution-v1", mode: "observational_not_causal",
    signal: Number(grossBps) > 0 ? "direction_correct" : "direction_wrong",
    causalInterpretation: { classifiedCause: position && position.cause || "unknown",
      outcomeConsistency: Number(netBps) > 0 ? "reversal_after_classification" : "no_profitable_reversal_after_classification" },
    costs: Number(grossBps) > 0 && Number(netBps) <= 0
      ? "gross_edge_erased" : (Number(costBps) > 0 ? "edge_after_costs_recorded" : "no_modelled_cost"),
    sizing: Number.isFinite(sizeMult) && sizeMult < 1
      ? (Number(netBps) > 0 ? "haircut_reduced_profitable_exposure" : "haircut_reduced_loss")
      : "full_policy_size",
    temporal: Number(features.temporalRisk) > 0 ? (Number(netBps) > 0
      ? "temporal_haircut_preceded_profit" : "temporal_haircut_preceded_loss") : "inactive",
    intelligence: Number(features.intelligenceAdverseRisk) > 0 ? (Number(netBps) > 0
      ? "intelligence_haircut_preceded_profit" : "intelligence_haircut_preceded_loss") : "inactive",
    exit: { reason: exitReason || null, heldDays: Number(heldDays) || 0,
      peakBps: peakBps == null ? null : round(peakBps), givebackBps: givebackBps == null ? null : round(givebackBps) },
    execution: { modeledRoundTripCostBps: round(Number(costBps) || 0),
      netAfterModeledCostsBps: round(Number(netBps) || 0) },
    concentration: { sector: position && position.sector || "other",
      regime: position && position.regimeAtEntry || null },
    limitation: "Component labels describe the recorded decision path; frozen policy comparisons and locked forward data are required for causal promotion claims.",
  };
}

module.exports = { OBSERVATIONS, HORIZONS, componentFor, evaluateFrozenPolicies,
  observationId, recordObservation, policyOutcome, assessCause,
  updateObservations, attributeClosed };
