"use strict";

/**
 * Build the cycle workset from the current residual universe plus every open
 * position. Open positions are management obligations, not entry candidates:
 * they remain reachable even when roster, factor, or synchronous-coverage
 * eligibility changes.
 */
function buildManagementWorkset(signalSymbols, positions) {
  const bySymbol = new Map();
  for (const p of (positions || [])) {
    if (p && p.open && p.symbol) bySymbol.set(p.symbol, p);
  }
  const ordered = [];
  const seen = new Set();
  for (const sym of (signalSymbols || [])) {
    if (!sym || seen.has(sym)) continue;
    seen.add(sym);
    ordered.push({ symbol: sym, position: bySymbol.get(sym) || null, entryEligible: true });
  }
  for (const [sym, position] of bySymbol.entries()) {
    if (seen.has(sym)) continue;
    seen.add(sym);
    ordered.push({ symbol: sym, position, entryEligible: false });
  }
  return ordered;
}

/** Current execution data may come from another trusted provider/feed without
 * erasing an already-authorized economic exit. Adjustment identity is kept
 * stable because a split-adjustment regime change can alter the price basis
 * and requires separate corporate-action reconciliation. */
function exitExecutionSourcePolicy(decisionProvenance, currentProvenance, quality) {
  if (!currentProvenance || !/^[a-f0-9]{64}$/.test(String(currentProvenance.sourceSha256 || ""))) {
    return { pass: false, reason: "missing_execution_provenance" };
  }
  if (!quality || quality.tradable !== true) {
    return { pass: false, reason: "execution_source_not_tradable" };
  }
  const d = decisionProvenance || {};
  const oldAdj = d.adjustment == null ? null : String(d.adjustment);
  const newAdj = currentProvenance.adjustment == null ? null : String(currentProvenance.adjustment);
  if (oldAdj === null || newAdj === null) {
    return { pass: false, reason: "missing_adjustment_identity" };
  }
  if (oldAdj !== newAdj) {
    return { pass: false, reason: "adjustment_identity_changed" };
  }
  const transitioned = (d.provider || null) !== (currentProvenance.provider || null)
    || (d.feed || null) !== (currentProvenance.feed || null);
  return { pass: true, transitioned, reason: transitioned ? "trusted_source_transition" : "same_source" };
}

function preferredAdvUsd(measuredMeta, rosterMeta) {
  const measured = Number(measuredMeta && measuredMeta.advUsd);
  if (measured > 0) return measured;
  const roster = Number(rosterMeta && rosterMeta.advUsd);
  return roster > 0 ? roster : 0;
}

module.exports = { buildManagementWorkset, exitExecutionSourcePolicy, preferredAdvUsd };
