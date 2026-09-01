/* Investor_AI — fail-closed corporate-action basis protection.
 *
 * Adjusted market data can jump to a new per-share basis before a virtual
 * position's quantity/cost fields do. Treating that jump as a return creates
 * false stops, false P&L, and poisoned learning rows. Corporate actions are
 * also legally and economically varied (cash-in-lieu, mergers, dividends), so
 * this module never guesses an accounting treatment. It detects common split
 * ratios, quarantines the mark, and provides a deterministic split patch only
 * after an authenticated operator confirms the ratio.
 */
"use strict";

const COMMON_SHARE_RATIOS = Object.freeze([
  1.5, 2, 3, 4, 5, 6, 7, 8, 10, 15, 20,
  1 / 1.5, 1 / 2, 1 / 3, 1 / 4, 1 / 5, 1 / 6, 1 / 7, 1 / 8, 1 / 10,
]);
const RATIO_TOLERANCE = 0.06;

function nearestShareRatio(previousPrice, currentPrice) {
  const previous = Number(previousPrice), current = Number(currentPrice);
  if (!(previous > 0) || !(current > 0)) return null;
  const implied = previous / current;
  let best = null;
  for (const candidate of COMMON_SHARE_RATIOS) {
    const error = Math.abs(implied - candidate) / candidate;
    if (error <= RATIO_TOLERANCE && (!best || error < best.error)) {
      best = { shareRatio: candidate, impliedRatio: implied, error };
    }
  }
  return best;
}

function assessPositionMark({ position = {}, currentPrice, currentProvenance = null } = {}) {
  if (position.corporateActionPending && position.corporateActionPending.status !== "resolved") {
    return { ...position.corporateActionPending, quarantine: true,
      reason: position.corporateActionPending.reason || "corporate action awaits operator reconciliation" };
  }
  const previousPrice = Number(position.lastMarkUsd != null
    ? position.lastMarkUsd : (position.lastMarkPrice != null
      ? position.lastMarkPrice : (position.entryPriceUsd != null
        ? position.entryPriceUsd : position.entryPrice)));
  const current = Number(currentPrice);
  const priorProv = position.lastMarkProvenance || position.entryBarProvenance
    || position.entryMarketProvenance || null;
  if (priorProv && currentProvenance && priorProv.adjustment != null
      && currentProvenance.adjustment != null
      && priorProv.adjustment !== currentProvenance.adjustment) {
    return {
      quarantine: true, kind: "adjustment_identity_changed", shareRatio: null,
      previousPrice, currentPrice: current,
      previousAdjustment: priorProv.adjustment, currentAdjustment: currentProvenance.adjustment,
      reason: "market-data adjustment identity changed; position basis requires reconciliation",
    };
  }
  const ratio = nearestShareRatio(previousPrice, current);
  if (!ratio || Math.abs(current / previousPrice - 1) < 0.35) {
    return { quarantine: false, kind: null, shareRatio: null, previousPrice, currentPrice: current };
  }
  const shareRatio = Number(ratio.shareRatio.toFixed(8));
  return {
    quarantine: true,
    kind: shareRatio > 1 ? "possible_forward_split" : "possible_reverse_split",
    shareRatio,
    impliedRatio: Number(ratio.impliedRatio.toFixed(8)),
    ratioErrorPct: Number((ratio.error * 100).toFixed(4)),
    previousPrice, currentPrice: current,
    reason: `per-share price moved near a ${shareRatio}:1 share-basis ratio; stop, P&L, and learning marks are quarantined pending confirmation`,
  };
}

function splitReconciliationPatch(position, pending, confirmedShareRatio, event = {}) {
  const ratio = Number(confirmedShareRatio);
  if (!(ratio > 0.04 && ratio < 25)) throw new Error("confirmed split ratio is outside supported bounds");
  const implied = Number(pending && pending.shareRatio);
  if (!(implied > 0) || Math.abs(ratio - implied) / implied > RATIO_TOLERANCE) {
    throw new Error("confirmed split ratio does not match the quarantined price-basis change");
  }
  const scalePrice = (value) => Number.isFinite(Number(value))
    ? Number((Number(value) / ratio).toFixed(8)) : value;
  const patch = {
    qty: Number((Number(position.qty) * ratio).toFixed(8)),
    corporateActionPending: null,
    exitIntent: null,
    corporateActionHistory: [...(Array.isArray(position.corporateActionHistory)
      ? position.corporateActionHistory : []), {
      type: ratio >= 1 ? "forward_split" : "reverse_split",
      shareRatio: ratio,
      effectiveDate: event.effectiveDate || null,
      confirmedBy: event.operator || "operator",
      confirmedAtMs: Number(event.confirmedAtMs) || Date.now(),
      sourceRef: event.sourceRef || null,
      priorQty: Number(position.qty),
      reconciledQty: Number((Number(position.qty) * ratio).toFixed(8)),
    }].slice(-20),
  };
  for (const key of ["entryPriceUsd", "avgPrice", "entryBarOpenUsd", "peakPriceUsd",
    "lastMarkUsd", "entryPrice", "peakPrice", "lastMarkPrice", "dayStartPrice"]) {
    if (position[key] != null) patch[key] = scalePrice(position[key]);
  }
  /* Cost basis and entry notional intentionally do not change: a split alters
     units and per-unit prices, not the economic dollars originally invested. */
  return patch;
}

module.exports = { COMMON_SHARE_RATIOS, RATIO_TOLERANCE, nearestShareRatio,
  assessPositionMark, splitReconciliationPatch };

