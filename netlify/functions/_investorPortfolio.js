/*  netlify/functions/_investorPortfolio.js  (fund-manager-v1)
 *  ---------------------------------------------------------------------------
 *  Investor AI — point-in-time portfolio state (blueprint §6.5, §6.8, §12.1).
 *
 *  Three readers, one shape:
 *
 *    snapshot()                       — the planning view at a cutoff: positions,
 *                                       working orders, settled cash, reservations,
 *                                       active mandates, exposures inputs. Read by
 *                                       Sol through getPortfolioSnapshot and by the
 *                                       manager to build packets.
 *    captureActivationSnapshot()      — IMMUTABLE broker-reconciled truth written
 *                                       immediately before a plan commit: it carries
 *                                       the portfolio version, reservation-account
 *                                       version and writer epoch the activation
 *                                       transaction compares against (CAS, §6.8).
 *                                       The morning cutoff is never reused as
 *                                       execution truth.
 *    symbolState()                    — the Appendix B state kind for one symbol:
 *                                       NONHOLDING_NO_ENTRY, UNFILLED_ENTRY,
 *                                       PARTIALLY_FILLED_ENTRY, HELD.
 *
 *  Money on the wire is canonical integer strings (minor = cents, micros for
 *  prices). Legacy ledger fields (cents as Numbers, prices as USD floats) are
 *  converted at this boundary exactly once and never re-derived downstream.
 *  Nothing here decides anything: it reports.
 * ---------------------------------------------------------------------------
 */

"use strict";

const crypto = require("crypto");
const A = require("./_investorAdmin");
const POLICY = require("./_investorPolicy");

const SNAPSHOT_SCHEMA = "portfolio-snapshot.v1";
const ACTIVATION_SCHEMA = "activation-snapshot.v1";
const OPEN_ORDER_STATUSES = Object.freeze(["proposed", "approved", "working", "partially_filled", "pending_cancel"]);

function sha256(v) { return crypto.createHash("sha256").update(typeof v === "string" ? v : JSON.stringify(POLICY.canonical(v))).digest("hex"); }
function minorFromCents(c) { const n = Math.round(Number(c) || 0); return String(n); }
function microsFromUsd(usd) { const n = Number(usd); return Number.isFinite(n) ? String(Math.round(n * 1e6)) : null; }
function unitsOf(q) { const n = Math.floor(Number(q) || 0); return String(Math.max(0, n)); }
function rows(snap) { const out = []; if (snap && typeof snap.forEach === "function") snap.forEach((d) => out.push(d.data())); else if (snap && Array.isArray(snap.docs)) snap.docs.forEach((d) => out.push(d.data())); return out; }

/* ── raw reads ─────────────────────────────────────────────────────────── */
async function readPositions(accountId, { admin = null } = {}) {
  const D = admin || A;
  return rows(await D.col(D.COL.positions).where("accountId", "==", accountId).where("open", "==", true).get());
}
async function readOpenOrders(accountId, { admin = null } = {}) {
  const D = admin || A;
  const out = [];
  for (const status of OPEN_ORDER_STATUSES) {
    try { out.push(...rows(await D.col(D.COL.orders).where("accountId", "==", accountId).where("status", "==", status).get())); } catch {}
  }
  return out;
}
async function readBalances(accountId, { admin = null } = {}) {
  const D = admin || A;
  const s = await D.col(D.COL.accounts).doc(accountId).get();
  const a = s.exists ? s.data() : {};
  const cents = a.balanceCents || {};
  return { cashMinor: minorFromCents(cents.cash), reservedMinor: minorFromCents(cents.reserved), positionsMinor: minorFromCents(cents.positions),
    startingNavMinor: a.startingNavCents != null ? minorFromCents(a.startingNavCents) : null,
    netCapitalMinor: cents.contributed_capital != null ? String(-BigInt(minorFromCents(cents.contributed_capital))) : null,
    balanceRevision: Number(a.balanceRevision) || 0,
    portfolioVersion: Number(a.portfolioVersion) || Number(a.balanceRevision) || 0, writerEpoch: Number(a.writerEpoch) || 0, exists: s.exists };
}
async function readActiveMandates(accountId, { admin = null } = {}) {
  const D=admin || A, SC=require("./_investorStorageCodec");
  const decode=d=>d && d._codec ? SC.decode(d) : d;
  const pointers=rows(await D.col(D.COL.activeMandates).where("accountId","==",accountId).get());
  return Promise.all(pointers.map(async pointer=>{
    if(!pointer.appliedVersionId) return pointer;
    const b=await D.col(D.COL.mandates).doc(pointer.appliedVersionId).get();
    if(!b.exists) return pointer;
    const binding=decode(b.data());
    if(!binding.proposalId) return pointer;
    const p=await D.col(D.COL.mandateProposals).doc(binding.proposalId).get();
    if(!p.exists) return pointer;
    const proposal=decode(p.data()).proposal;
    return {...pointer,appliedProposal:proposal,action:proposal.action,allocation:proposal.allocation,researchVersionId:binding.researchVersionId || null};
  }));
}

async function readReservationAccount(accountId, { admin = null } = {}) {
  const D = admin || A;
  const s = await D.col(D.COL.reservationAccounts).doc(accountId).get();
  const r = s.exists ? s.data() : {};
  return { reservedNotionalMinor: String(r.reservedNotionalMinor || "0"), reservedPlannedLossMinor: String(r.reservedPlannedLossMinor || "0"),
    reservedStressLossMinor: String(r.reservedStressLossMinor || "0"), version: Number(r.version) || 0,
    committedPortfolioPlanId: r.committedPortfolioPlanId || null, exists: s.exists };
}

/* ── the planning snapshot ─────────────────────────────────────────────── */
function positionView(p, mandateBySymbol) {
  const qty = unitsOf(p.qty);
  const mark = Number(p.lastMarkUsd) > 0 ? Number(p.lastMarkUsd) : Number(p.entryPriceUsd) || 0;
  const m = mandateBySymbol[p.symbol] || null;
  return {
    symbol: p.symbol, quantityUnits: qty, markMicros: microsFromUsd(mark), markAt: p.lastMarkAt || null,
    entryPriceMicros: microsFromUsd(p.entryPriceUsd), costBasisMinor: minorFromCents(p.costBasisCents),
    marketValueMinor: String(Math.round(Number(qty) * mark * 100)),
    unrealisedMinor: String(Math.round(Number(qty) * (mark - (Number(p.entryPriceUsd) || 0)) * 100)),
    openedAt: p.openedAt || null, lifecycleId: p.lifecycleId || null, positionLifecycleId: p.positionLifecycleId || null,
    sector: p.sector || null, engine: p.mandateVersionId || (m && m.appliedVersionId) ? "manager" : "legacy",
    mandateVersionId: p.mandateVersionId || (m && m.appliedVersionId) || null,
    lossBoundaryPriceMicros: p.lossBoundaryPriceMicros || (m && m.lossBoundaryPriceMicros) || null,
    takeProfitPriceMicros: p.takeProfitPriceMicros || (m && m.takeProfitPriceMicros) || null,
    protectionAcknowledged: p.protectionAcknowledged === true || (m && m.protectionAcknowledged === true) || false,
    protectionState: p.protectionState || (m && m.protectionState) || (p.lossBoundaryPriceMicros ? "ACTIVE" : "LEGACY_GUARD"),
  };
}
function orderView(o) {
  const requested = unitsOf(o.qty), filled = unitsOf(o.qtyFilled);
  return {
    orderId: o.orderId, symbol: o.symbol, side: o.side || "buy", status: o.status,
    quantityUnits: requested, filledUnits: filled, remainingUnits: String(Math.max(0, Number(requested) - Number(filled))),
    limitPriceMicros: o.limitPriceMicros || microsFromUsd(o.refPriceUsd), reservedMinor: minorFromCents((Number(o.grossCents) || 0) + (Number(o.frictionCents) || 0) + (Number(o.reservationHeadroomCents) || 0)),
    expiresAtMs: Number(o.expiresAtMs) || null, decisionAtMs: Number(o.decisionAtMs) || null,
    mandateVersionId: o.mandateVersionId || null, orderSetId: o.orderSetId || null, pausedReason: o.pausedReason || null,
  };
}
async function snapshot({ accountId, asOfMs = A.now(), admin = null, sectorOf = null } = {}) {
  const acct = String(accountId || "paper-1");
  const [positions, orders, balances, mandates, reservation] = await Promise.all([
    readPositions(acct, { admin }), readOpenOrders(acct, { admin }), readBalances(acct, { admin }), readActiveMandates(acct, { admin }), readReservationAccount(acct, { admin }),
  ]);
  const mandateBySymbol = Object.fromEntries(mandates.map((m) => [m.symbol, m]));
  const sector = sectorOf || defaultSectorOf();
  const pos = positions.map((p) => ({ ...positionView(p, mandateBySymbol), sector: p.sector || sector(p.symbol) }));
  const open = orders.map(orderView).map((o) => ({ ...o, sector: sector(o.symbol) }));
  const investedMinor = pos.reduce((n, p) => n + BigInt(p.marketValueMinor), 0n);
  const navMinor = BigInt(balances.cashMinor) + BigInt(balances.reservedMinor) + investedMinor;
  const workingBuyNotionalMinor = open.filter((o) => o.side === "buy").reduce((n, o) => n + (o.limitPriceMicros ? BigInt(o.limitPriceMicros) * BigInt(o.remainingUnits) / 10000n : 0n), 0n);
  const out = {
    schemaVersion: SNAPSHOT_SCHEMA, accountId: acct, asOfMs: Number(asOfMs), asOf: new Date(Number(asOfMs)).toISOString(),
    navMinor: navMinor.toString(), settledCashMinor: balances.cashMinor, reservedMinor: balances.reservedMinor, investedMinor: investedMinor.toString(),
    startingNavMinor: balances.startingNavMinor, netCapitalMinor: balances.netCapitalMinor, currency: "USD",
    positions: pos, workingOrders: open, activeMandates: mandates.map((m) => ({ symbol: m.symbol, desiredProposalHash:m.desiredProposalHash || null, status: m.status || null, desiredVersionId: m.desiredVersionId || null,
      appliedVersionId: m.appliedVersionId || null, capitalRank: m.capitalRank == null ? null : m.capitalRank, expiresAtMs: m.expiresAtMs || null, planClass: m.planClass || null,
      /* the applied terms travel with the pointer so a delta review can be checked against them (anti-goalpost, §6.7) */
      action: m.action || null, allocation: m.allocation || null, appliedProposal:m.appliedProposal || null,
      thesis:m.appliedProposal && m.appliedProposal.thesis || null, forecast:m.appliedProposal && m.appliedProposal.forecast || null, sourceManifest:m.appliedProposal && m.appliedProposal.sourceManifest || [], researchVersionId:m.researchVersionId || null,
      lossBoundaryPriceMicros: m.lossBoundaryPriceMicros || (m.action && m.action.protection && m.action.protection.lossBoundaryPriceMicros) || null,
      takeProfitPriceMicros: m.takeProfitPriceMicros || (m.action && m.action.protection && m.action.protection.takeProfitPriceMicros) || null })),
    reservationAccount: reservation,
    aggregates: { openPositions: pos.length, workingOrders: open.length, workingBuyNotionalMinor: workingBuyNotionalMinor.toString(),
      heldSymbols: pos.map((p) => p.symbol).sort(), pendingSymbols: [...new Set(open.map((o) => o.symbol))].sort(),
      unprotected: pos.filter((p) => !p.lossBoundaryPriceMicros).map((p) => p.symbol) },
    versions: { portfolioVersion: balances.portfolioVersion, balanceRevision: balances.balanceRevision, writerEpoch: balances.writerEpoch, reservationAccountVersion: reservation.version },
  };
  out.contentHash = sha256({ ...out, asOfMs: undefined, asOf: undefined });
  return out;
}
function defaultSectorOf() {
  let U = null;
  try { U = require("./_investorUniverse"); } catch { return () => null; }
  const by = new Map([...(U.tradeTier || []), ...(U.researchTier || []), ...(U.excludedTier || [])].map((r) => [r.symbol, r.sector]));
  return (s) => by.get(String(s || "").toUpperCase()) || null;
}

/* ── the activation snapshot (immutable; CAS anchor) ───────────────────── */
async function captureActivationSnapshot({ accountId, reason = "activation", admin = null, reconcile = null } = {}) {
  const D = admin || A;
  const acct = String(accountId || "paper-1");
  let reconciliation = null;
  if (reconcile) { try { reconciliation = await reconcile(acct); } catch (e) { reconciliation = { ok: false, error: String(e.message).slice(0, 120) }; } }
  const snap = await snapshot({ accountId: acct, admin });
  const id = `act_${sha256({ acct, contentHash: snap.contentHash, versions: snap.versions, t: A.now() }).slice(0, 32)}`;
  const doc = {
    schemaVersion: ACTIVATION_SCHEMA, activationSnapshotId: id, accountId: acct, reason, capturedAtMs: A.now(),
    navMinor: snap.navMinor, settledCashMinor: snap.settledCashMinor, reservedMinor: snap.reservedMinor, investedMinor: snap.investedMinor,
    positions: snap.positions.map((p) => ({ symbol: p.symbol, quantityUnits: p.quantityUnits, markMicros: p.markMicros, lossBoundaryPriceMicros: p.lossBoundaryPriceMicros, protectionState: p.protectionState, sector: p.sector })),
    workingOrders: snap.workingOrders.map((o) => ({ orderId: o.orderId, symbol: o.symbol, side: o.side, remainingUnits: o.remainingUnits, limitPriceMicros: o.limitPriceMicros, status: o.status, sector: o.sector })),
    reservationAccount: snap.reservationAccount, contentHash: snap.contentHash,
    portfolioVersion: snap.versions.portfolioVersion, reservationAccountVersion: snap.versions.reservationAccountVersion, writerEpoch: snap.versions.writerEpoch,
    reconciliation: reconciliation ? { ok: reconciliation.ok !== false, pass: reconciliation.pass !== false, summary: String(reconciliation.summary || reconciliation.error || "").slice(0, 200) } : null,
    brokerTruthAgeSeconds: reconciliation && Number.isFinite(Number(reconciliation.brokerTruthAgeSeconds)) ? Number(reconciliation.brokerTruthAgeSeconds) : null,
    ...D.envelope({ created_by: "portfolio.captureActivationSnapshot" }),
  };
  await D.col(D.COL.activationSnapshots).doc(id).set(doc);
  return { ...doc, id, snapshot: snap };
}

/* ── Appendix B symbol state ───────────────────────────────────────────── */
async function symbolState({ accountId, symbol, admin = null } = {}) {
  const sym = String(symbol || "").toUpperCase();
  const snap = await snapshot({ accountId, admin });
  const position = snap.positions.find((p) => p.symbol === sym) || null;
  const entries = snap.workingOrders.filter((o) => o.symbol === sym && o.side === "buy");
  const mandate = snap.activeMandates.find((m) => m.symbol === sym) || null;
  const owned = position ? BigInt(position.quantityUnits) : 0n;
  const remaining = entries.reduce((n, o) => n + BigInt(o.remainingUnits), 0n);
  let kind;
  if (owned > 0n && remaining > 0n) kind = "PARTIALLY_FILLED_ENTRY";
  else if (owned > 0n) kind = "HELD";
  else if (remaining > 0n) kind = "UNFILLED_ENTRY";
  else kind = "NONHOLDING_NO_ENTRY";
  return { kind, symbol: sym, accountId: snap.accountId, ownedQuantityUnits: owned.toString(), remainingEntryUnits: remaining.toString(),
    position, entries, appliedMandate: mandate, portfolio: snap };
}

/* ── the read-only tool ────────────────────────────────────────────────── */
async function getSnapshot({ accountId, asOfMs = null, admin = null } = {}) {
  const s = await snapshot({ accountId, asOfMs: asOfMs || A.now(), admin });
  /* the tool view is bounded: no ids the model could confuse for authority */
  return { schemaVersion: s.schemaVersion, accountId: s.accountId, asOf: s.asOf, navMinor: s.navMinor, settledCashMinor: s.settledCashMinor,
    reservedMinor: s.reservedMinor, investedMinor: s.investedMinor, currency: s.currency,
    positions: s.positions.map((p) => ({ symbol: p.symbol, quantityUnits: p.quantityUnits, markMicros: p.markMicros, marketValueMinor: p.marketValueMinor,
      unrealisedMinor: p.unrealisedMinor, sector: p.sector, lossBoundaryPriceMicros: p.lossBoundaryPriceMicros, takeProfitPriceMicros: p.takeProfitPriceMicros, openedAt: p.openedAt })),
    workingOrders: s.workingOrders.map((o) => ({ symbol: o.symbol, side: o.side, remainingUnits: o.remainingUnits, limitPriceMicros: o.limitPriceMicros, status: o.status })),
    reservations: { reservedNotionalMinor: s.reservationAccount.reservedNotionalMinor, reservedPlannedLossMinor: s.reservationAccount.reservedPlannedLossMinor },
    aggregates: s.aggregates, opportunityCostSet: s.positions.map((p) => p.symbol) };
}

module.exports = {
  SNAPSHOT_SCHEMA, ACTIVATION_SCHEMA, OPEN_ORDER_STATUSES,
  readPositions, readOpenOrders, readBalances, readActiveMandates, readReservationAccount,
  snapshot, captureActivationSnapshot, symbolState, getSnapshot, positionView, orderView,
};
