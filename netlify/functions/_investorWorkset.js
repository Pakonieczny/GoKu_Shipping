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

/* ── AI Fund Manager workset (blueprint §6.3) ────────────────────────────
 * The management workset is
 *     frozen eligible snapshot ∪ held symbols ∪ symbols with working/pending broker state.
 * Off-roster, halted, acquired or delisted holdings remain managed and are
 * reported separately until flat. A symbol with ANY owned quantity cannot
 * receive BUY and HOLD cannot increase exposure (no scaling in, v1). */
function buildManaged({ roster, positions = [], pending = [], nowMs = Date.now() } = {}) {
  const eligible = new Set(((roster && roster.symbols) || []).map((s) => String(s).toUpperCase()));
  const held = new Map();
  for (const p of positions || []) {
    if (!p || !p.symbol) continue;
    const qty = Number(p.qty ?? p.quantity ?? p.shares ?? 0);
    const open = p.open === true || (p.open == null && qty > 0);
    if (!open && !(qty > 0)) continue;
    held.set(String(p.symbol).toUpperCase(), { qty: qty > 0 ? qty : (open ? 1 : 0), position: p });
  }
  const pendingBy = new Map();
  for (const o of pending || []) {
    if (!o || !o.symbol) continue;
    const s = String(o.symbol).toUpperCase();
    pendingBy.set(s, [...(pendingBy.get(s) || []), { orderId: o.orderId || o.id || null, status: o.status || null, side: o.side || null }]);
  }
  const rows = [];
  const seen = new Set();
  const rowFor = (symbol) => {
    const isEligible = eligible.has(symbol), h = held.get(symbol) || null, pend = pendingBy.get(symbol) || [];
    const heldQty = h ? h.qty : 0;
    return {
      symbol, eligible: isEligible, held: heldQty > 0, heldQty, pending: pend.length > 0, pendingOrders: pend,
      offRoster: !isEligible,
      /* only a flat, on-roster name may be a new-entry candidate */
      entryEligible: isEligible && heldQty === 0 && !pend.some((x) => /buy/i.test(String(x.side || ""))),
      scalingInForbidden: heldQty > 0,
      managedReason: isEligible ? (heldQty > 0 ? "eligible_and_held" : pend.length ? "eligible_with_pending" : "eligible")
        : (heldQty > 0 ? "held_off_roster" : "pending_off_roster"),
    };
  };
  for (const s of [...eligible].sort()) { seen.add(s); rows.push(rowFor(s)); }
  for (const s of [...held.keys()].sort()) if (!seen.has(s)) { seen.add(s); rows.push(rowFor(s)); }
  for (const s of [...pendingBy.keys()].sort()) if (!seen.has(s)) { seen.add(s); rows.push(rowFor(s)); }
  const managedPositionSymbols = rows.filter((r) => r.held).map((r) => r.symbol);
  const managedOffRoster = rows.filter((r) => r.offRoster).map((r) => ({ symbol: r.symbol, reason: r.managedReason }));
  return {
    schemaVersion: "managed-workset.v1", asOfMs: nowMs,
    universeVersion: roster && roster.universeVersion || null, universeHash: roster && roster.universeHash || null,
    eligibleCount: eligible.size, symbols: rows.map((r) => r.symbol), rows,
    eligibleSymbols: [...eligible].sort(), managedPositionSymbols, managedOffRoster,
    entryCandidateSymbols: rows.filter((r) => r.entryEligible).map((r) => r.symbol),
    counts: { total: rows.length, eligible: eligible.size, held: managedPositionSymbols.length,
      pending: rows.filter((r) => r.pending).length, offRoster: managedOffRoster.length },
  };
}
/** PURE. Is a proposed action allowed by the v1 no-scaling-in rule? */
function actionAllowedForRow(row, decision) {
  if (!row) return { ok: false, reason: "symbol_not_in_workset" };
  if (decision === "BUY" && row.heldQty > 0) return { ok: false, reason: "SCALING_IN_FORBIDDEN" };
  if (decision === "BUY" && !row.eligible) return { ok: false, reason: "OFF_ROSTER_ENTRY_FORBIDDEN" };
  if (["HOLD", "REDUCE", "SELL"].includes(decision) && !(row.heldQty > 0)) return { ok: false, reason: "NOT_HELD" };
  return { ok: true, reason: null };
}
/** PURE. A fair dossier refresh queue: never-built first, then stalest,
 *  held names ahead of ties, and a rotation offset so the same tail is not
 *  always cut when a pass is bounded. */
function fairDossierQueue({ workset, pointers = {}, nowMs = Date.now(), rotation = 0 } = {}) {
  const rows = (workset && workset.rows || []).map((r) => {
    const p = pointers[r.symbol] || null;
    const asOf = p && Number(p.asOfMs);
    return { symbol: r.symbol, held: r.held, never: !p || !Number.isFinite(asOf), ageMs: Number.isFinite(asOf) ? Math.max(0, nowMs - asOf) : Infinity };
  });
  rows.sort((a, b) => (Number(b.never) - Number(a.never)) || (Number(b.held) - Number(a.held)) || (b.ageMs - a.ageMs) || a.symbol.localeCompare(b.symbol));
  const n = rows.length;
  if (!n) return [];
  /* rotation applies within equal-age bands only, so staleness order is preserved */
  const off = Math.abs(Number(rotation) || 0) % n;
  const bands = new Map();
  for (const r of rows) { const k = `${r.never}|${r.held}|${r.ageMs === Infinity ? "inf" : Math.floor(r.ageMs / 3600000)}`; bands.set(k, [...(bands.get(k) || []), r]); }
  const out = [];
  for (const band of bands.values()) { const o = off % band.length; out.push(...band.slice(o), ...band.slice(0, o)); }
  return out.map((r) => r.symbol);
}

module.exports = { buildManagementWorkset, exitExecutionSourcePolicy, preferredAdvUsd,
  buildManaged, actionAllowedForRow, fairDossierQueue };
