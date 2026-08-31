/* Investor_AI — frozen decision-matrix counterfactuals and forward outcomes. */
"use strict";

const crypto = require("crypto");
const A = require("./_investorAdmin");
const S = require("./_investorSignal");
const V = require("./_investorVariants");

const OBSERVATIONS = A.COL.shadowObservations;
const HORIZONS = Object.freeze([1, 3, 5, 10]);

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
  regime = null } = {}) {
  if (!/^[a-f0-9]{64}$/.test(String(experimentHash || ""))) {
    throw new Error("decision observation requires experimentHash");
  }
  if (!(Number(initialPrice) > 0) || !counterfactuals) return { recorded: false, reason: "missing_price_or_policies" };
  const ref = A.col(OBSERVATIONS).doc(observationId(experimentHash, cycleId, symbol));
  const row = {
    schemaVersion: "decision-outcome-observation-v1", experimentHash, cycleId, symbol,
    decisionAtMs: Number(decisionAtMs), decisionDate, initialPrice: Number(initialPrice),
    marketProvenance, variantsHash: counterfactuals.variantsHash,
    policies: counterfactuals.policies, sector, regime: regime ? {
      vixNorm: Number(regime.vixNorm) || null, cor3m: Number(regime.cor3m) || null,
    } : null,
    horizons: HORIZONS, marks: {}, status: "pending", affectsPolicy: false,
    ...A.envelope({ created_by: "decisionFeedback.recordObservation" }),
  };
  await ref.set(row, { merge: true });
  return { recorded: true, id: ref.id };
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

/** Add one point-in-time daily mark and resolve frozen 1/3/5/10-session outcomes. */
async function updateObservations({ experimentHash, session, lastPrice = {},
  marketProvenanceBySymbol = {} } = {}) {
  if (!(session && session.tradingDay && session.date)) return { updated: 0, resolved: 0 };
  /* Bound the scan to this immutable experiment and unresolved observations.
     The paired composite index is shipped in the deployment manifest. */
  const snap = await A.col(OBSERVATIONS)
    .where("experimentHash", "==", experimentHash)
    .where("status", "==", "pending").get();
  let updated = 0, resolved = 0;
  for (const doc of snap.docs) {
    const row = doc.data();
    if (session.date <= row.decisionDate) continue;
    const price = Number(lastPrice[row.symbol]);
    const provenance = marketProvenanceBySymbol[row.symbol];
    if (!(price > 0) || !provenance || !/^[a-f0-9]{64}$/.test(String(provenance.sourceSha256 || ""))) continue;
    const marks = { ...(row.marks || {}), [session.date]: { price, marketProvenance: provenance } };
    const dates = Object.keys(marks).sort();
    const outcomes = { ...(row.outcomes || {}) };
    for (const horizon of HORIZONS) {
      if (dates.length < horizon || outcomes[horizon]) continue;
      const mark = marks[dates[horizon - 1]];
      const grossBps = (Number(mark.price) / Number(row.initialPrice) - 1) * 1e4;
      outcomes[horizon] = {
        date: dates[horizon - 1], price: Number(mark.price), grossBps: round(grossBps),
        policies: Object.fromEntries(Object.entries(row.policies || {})
          .map(([id, policy]) => [id, policyOutcome(policy, grossBps)])),
      };
    }
    const done = !!outcomes[HORIZONS.at(-1)];
    await doc.ref.set({ marks, outcomes, sessionsObserved: dates.length,
      status: done ? "resolved" : "pending", resolvedAtMs: done ? Date.now() : null,
      updatedAt: A.FV.serverTimestamp() }, { merge: true });
    updated += 1; if (done) resolved += 1;
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
  observationId, recordObservation, policyOutcome, updateObservations, attributeClosed };
