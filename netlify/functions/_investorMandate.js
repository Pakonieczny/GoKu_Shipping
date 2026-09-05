/*  netlify/functions/_investorMandate.js  (fund-manager-v1)
 *  ---------------------------------------------------------------------------
 *  Investor AI — mandate lineage, activation envelopes, reservations and the
 *  desired-state outbox (blueprint §6.8, §7, §8.6, §10.5, Appendix B).
 *
 *  THREE RECORDS, THREE HASHES
 *    MandateProposal.v1      Sol's strict output, stored unmodified.
 *    MandateServerBinding.v1 allocated here: series, monotonic version,
 *                            immutable version id, expected active version,
 *                            manager run, dossier/research versions, prompt/
 *                            schema/policy/context/source-manifest hashes.
 *    ActivationEnvelope.v1   the validator's verdict: status, activation
 *                            snapshot, risk policy hash, authorized quantity
 *                            (smaller or equal), reserved notional, planned
 *                            loss at the boundary, binding gap-stress loss,
 *                            clamp reason. It never edits the proposal.
 *
 *  stagePortfolioPlan() is the activation invariant: every network, model,
 *  verification and broker read has finished before it enters one retryable
 *  Firestore transaction that compares the activation snapshot's versions
 *  against live state, re-checks the immutable claim verdict ids and hashes,
 *  asserts all-or-none and no-increase, allocates bindings and writes the
 *  committed plan, bindings, envelopes, reservations, desired order sets and
 *  outbox transitions — or writes only a rejected-plan audit record. It never
 *  claims the broker has applied anything (§10.5): workers apply desired
 *  state through the outbox and advance applied pointers from broker truth.
 *
 *  Code here may refuse, pause or shrink. It may not choose a company, raise
 *  a quantity, widen a boundary, substitute a symbol or cancel protection.
 * ---------------------------------------------------------------------------
 */

"use strict";

const crypto = require("crypto");
const A = require("./_investorAdmin");
const POLICY = require("./_investorPolicy");
const M = require("./_investorMoney");

const MANDATE_VERSION = "mandate.v1";
const PLAN_CLASSES = Object.freeze(["RISK_MAINTENANCE", "EXPANSION"]);
const ENVELOPE_STATUSES = Object.freeze(["ALLOW", "CLAMP", "REJECT"]);
const POINTER_STATUSES = Object.freeze(["DESIRED", "BROKER_SYNC_PENDING", "WORKING", "PARTIALLY_FILLED", "PROTECTION_PENDING", "PROTECTED_RTH",
  "OVERNIGHT_GAP_EXPOSED", "FILLED", "CANCEL_PENDING", "ENTRY_EXPIRED", "CANCELLED", "PAUSED_OPERATIONAL", "PAUSED_EVIDENCE", "SUPERSEDED",
  "RECONCILING", "OVERSELL_INCIDENT", "ACTION_REQUIRED", "ERROR", "UNPROTECTED", "CLOSED"]);
const MAX_AUTHORIZED_SESSIONS = 3;
const HALF_CLOSE_MIN = 13 * 60, CLOSE_MIN = 16 * 60;

function sha(v) { return crypto.createHash("sha256").update(typeof v === "string" ? v : JSON.stringify(POLICY.canonical(v))).digest("hex"); }
function lazy(m) { try { return require(m); } catch { return null; } }
function db(admin) { return admin || A; }
function typed(code, message, extra = {}) { return Object.assign(new Error(message || code), { code, ...extra }); }
function big(v) { return BigInt(String(v)); }
function isInt(v) { return typeof v === "string" && /^-?(0|[1-9][0-9]*)$/.test(v); }
function seriesIdFor(accountId, symbol) { return `${accountId}_${String(symbol).toUpperCase()}`; }
function proposalIdFor(proposal) { return `prop_${sha(proposal).slice(0, 32)}`; }
function planIdFor(planClass, planHash) { return `plan_${planClass}_${planHash.slice(0, 24)}`; }
function hashPortfolioPlanProposal(planProposal, proposals) { return sha({ plan: { ...planProposal, actionableMandates: undefined, proposals: undefined }, proposals: (proposals || []).map((p) => sha(p)) }); }

/* ── session arithmetic ────────────────────────────────────────────────── */
function sessionCloseMs(date) {
  const MK = lazy("./_investorMarket");
  if (!MK || !/^\d{4}-\d{2}-\d{2}$/.test(String(date || ""))) return null;
  let half = false;
  try { half = MK.marketCalendar(Number(date.slice(0, 4))).halfDays.has(date); } catch {}
  return MK.nyWallClockToUtcMs(date, half ? HALF_CLOSE_MIN : CLOSE_MIN);
}
/** PURE. An entry authorization ends at the close of its last authorized session. */
function entryExpiresAtMs(proposal) {
  const dates = proposal && proposal.action && proposal.action.entry && proposal.action.entry.authorizedSessionDates || [];
  if (!dates.length) return null;
  return sessionCloseMs([...dates].sort().pop());
}

/* ── semantic validation (§7.3) — pure ─────────────────────────────────── */
function validateProposal(proposal, { eligibleSymbols = null, heldSymbols = [], pendingSymbols = [], nowMs = Date.now(), policy = null } = {}) {
  const errors = [];
  const v = POLICY.validate("mandate-proposal.v1", proposal);
  if (!v.ok) return { ok: false, errors: v.errors.map((e) => `schema:${e.path}:${e.error}`), derived: null };
  const sym = proposal.symbol;
  const held = new Set(heldSymbols.map((s) => String(s).toUpperCase()));
  const pending = new Set(pendingSymbols.map((s) => String(s).toUpperCase()));
  const eligible = eligibleSymbols ? new Set(eligibleSymbols.map((s) => String(s).toUpperCase())) : null;
  if (eligible && !eligible.has(sym) && !held.has(sym) && !pending.has(sym)) errors.push("SYMBOL_NOT_MANAGED");
  const asOfMs = Date.parse(proposal.asOf);
  if (!Number.isFinite(asOfMs) || asOfMs > nowMs + 60000) errors.push("AS_OF_IN_FUTURE");
  for (const m of proposal.sourceManifest || []) {
    if (Date.parse(m.publishedAt) > nowMs + 60000 || Date.parse(m.retrievedAt) > nowMs + 60000) { errors.push("EVIDENCE_TIME_IN_FUTURE"); break; }
  }
  const { decision, action, allocation, forecast } = proposal;
  const kind = action && action.kind;
  const orderFields = !!(action && (action.entry || action.protection || action.exit));
  if (["WATCH", "IGNORE", "ABSTAIN"].includes(decision)) {
    if (kind !== "NONE" || orderFields) errors.push("NON_EXECUTABLE_DECISION_CARRIES_ORDER_FIELDS");
    if (decision === "ABSTAIN" && !proposal.reasonCode) errors.push("ABSTAIN_REQUIRES_REASON_CODE");
    if (allocation) errors.push("NON_EXECUTABLE_DECISION_CARRIES_ALLOCATION");
  } else if (decision === "BUY") {
    if (kind !== "BUY") errors.push("BUY_ACTION_KIND_MISMATCH");
    if (held.has(sym)) errors.push("SCALING_IN_FORBIDDEN");
    if (!action || !action.entry) errors.push("BUY_REQUIRES_ENTRY");
    if (!action || !action.protection) errors.push("BUY_REQUIRES_PROTECTION");
    if (!allocation) errors.push("BUY_REQUIRES_ALLOCATION");
    if (!forecast) errors.push("BUY_REQUIRES_FORECAST");
    if (!proposal.thesis || !(proposal.thesis.evidenceAgainst || []).length) errors.push("BUY_REQUIRES_BEAR_CASE_EVIDENCE");
    if (!(proposal.invalidators || []).length) errors.push("BUY_REQUIRES_INVALIDATORS");
    if (action && action.entry && action.protection) {
      const limit = big(action.entry.limitPriceMicros), tp = big(action.protection.takeProfitPriceMicros), lb = big(action.protection.lossBoundaryPriceMicros);
      if (!(tp > limit)) errors.push("TARGET_NOT_ABOVE_LIMIT");
      if (!(lb < limit)) errors.push("LOSS_BOUNDARY_NOT_BELOW_LIMIT");
      if (action.entry.timeInForce !== "DAY" || action.entry.regularSessionOnly !== true) errors.push("ENTRY_MUST_BE_DAY_REGULAR_SESSION");
      const dates = action.entry.authorizedSessionDates || [];
      if (!dates.length || dates.length > MAX_AUTHORIZED_SESSIONS) errors.push("AUTHORIZED_SESSIONS_OUT_OF_RANGE");
      if (dates.some((d) => Date.parse(`${d}T23:59:59Z`) < asOfMs - 86400000 * 2)) errors.push("AUTHORIZED_SESSION_IN_PAST");
      if (action.protection.brokerTimeInForce !== "GTC" || action.protection.persistentAtBroker !== true) errors.push("PROTECTION_MUST_BE_PERSISTENT_GTC");
      const t = action.protection.trailing;
      if (t && t.enabled === true && (!t.offsetType || t.offsetValueMicrosOrBps == null || !t.peakBasis)) errors.push("TRAILING_INCOMPLETE");
      if (action.exit) errors.push("BUY_CARRIES_EXIT_TERMS");
    }
    if (allocation) {
      if (allocation.quantityScale !== 0 || !isInt(allocation.proposedQuantityUnits) || big(allocation.proposedQuantityUnits) <= 0n) errors.push("QUANTITY_INVALID");
      if (big(allocation.maxWeightBps) < big(allocation.targetWeightBps)) errors.push("MAX_WEIGHT_BELOW_TARGET");
    }
  } else if (decision === "HOLD") {
    if (kind !== "HOLD_PROTECT") errors.push("HOLD_ACTION_KIND_MISMATCH");
    if (!held.has(sym)) errors.push("HOLD_REQUIRES_HELD_POSITION");
    if (action && action.entry) errors.push("HOLD_CARRIES_ENTRY");
    if (!action || !action.protection) errors.push("HOLD_REQUIRES_PROTECTION");
  } else if (decision === "REDUCE" || decision === "SELL") {
    if (kind !== decision) errors.push(`${decision}_ACTION_KIND_MISMATCH`);
    if (!held.has(sym)) errors.push(`${decision}_REQUIRES_HELD_POSITION`);
    if (!action || !action.exit) errors.push(`${decision}_REQUIRES_EXIT_TERMS`);
    if (action && action.entry) errors.push(`${decision}_CARRIES_ENTRY`);
    if (action && action.exit && (!isInt(action.exit.quantityUnits) || big(action.exit.quantityUnits) <= 0n)) errors.push("EXIT_QUANTITY_INVALID");
    if (action && action.exit && action.exit.orderType === "LIMIT" && action.exit.limitPriceMicros == null) errors.push("LIMIT_EXIT_REQUIRES_PRICE");
    if (action && action.exit && action.exit.orderType === "MARKETABLE_LIMIT" && action.exit.collarBps == null) errors.push("MARKETABLE_LIMIT_REQUIRES_COLLAR");
    if (decision === "REDUCE" && (!action || !action.protection)) errors.push("REDUCE_REQUIRES_REMAINDER_PROTECTION");
  }
  /* forecast arithmetic: the calculator derives, the proposal only declares */
  let derived = null;
  if (forecast) {
    const V = lazy("./_investorValuation");
    try {
      derived = V ? V.forecastFromScenarios({ basis: forecast.basis, outcomeBuckets: forecast.outcomeBuckets, fillProbabilityByExpiryPpm: forecast.fillProbabilityByExpiryPpm,
        estimatedRoundTripCostMicrosPerShare: forecast.basis.estimatedRoundTripCostMicrosPerShare, cashReturnBpsOverHorizon: "0" }) : null;
    } catch (e) { errors.push(`FORECAST_${e.code || "INVALID"}`); }
    const ids = (forecast.outcomeBuckets || []).map((b) => b.id);
    if (new Set(ids).size !== ids.length) errors.push("OUTCOME_BUCKET_IDS_NOT_UNIQUE");
    if (forecast.horizonTradingDays !== forecast.basis.horizonTradingDays) errors.push("FORECAST_HORIZON_MISMATCH");
    if (decision === "BUY") {
      if (forecast.basis.referencePriceType !== "BUY_LIMIT" || forecast.basis.referencePriceMicros !== (action && action.entry && action.entry.limitPriceMicros)) errors.push("BUY_FORECAST_REFERENCE_MISMATCH");
      if (derived && BigInt(derived.costAdjustedExpectedReturnBps) <= 0n) errors.push("BUY_EXPECTED_RETURN_NOT_POSITIVE_AFTER_COSTS");
      if (BigInt(forecast.fillProbabilityByExpiryPpm) <= 0n) errors.push("BUY_FILL_PROBABILITY_ZERO");
    }
  }
  return { ok: errors.length === 0, errors, derived };
}

/* ── activation envelopes (risk authority; pure) ───────────────────────── */
/** For each proposal the validator's smaller-or-equal authorization and why.
 *  The proposal itself is never edited. */
function buildPortfolioActivationEnvelopes({ portfolioPlanProposal, proposals = [], activationSnapshot, riskMandate = POLICY.RISK_MANDATE, marks = {}, advBySymbol = {}, sectorOf = null, clusterOf = null, validation = null } = {}) {
  const PR = lazy("./_investorPortfolioRisk");
  const snap = activationSnapshot || {};
  const sector = sectorOf || ((s) => { const p = (snap.positions || []).find((x) => x.symbol === s); return p ? p.sector || null : null; });
  const portfolio = { navMinor: snap.navMinor, settledCashMinor: snap.settledCashMinor, reservedMinor: snap.reservedMinor || "0",
    positions: (snap.positions || []).map((p) => ({ symbol: p.symbol, quantityUnits: p.quantityUnits, markMicros: p.markMicros, lossBoundaryPriceMicros: p.lossBoundaryPriceMicros || null, sector: p.sector || sector(p.symbol) })),
    workingOrders: (snap.workingOrders || []).map((o) => ({ symbol: o.symbol, side: o.side, quantityUnits: o.remainingUnits || o.quantityUnits, limitPriceMicros: o.limitPriceMicros, sector: o.sector || sector(o.symbol) })) };
  const owned = Object.fromEntries((snap.positions || []).map((p) => [p.symbol, big(p.quantityUnits)]));
  const envelopes = [];
  const candidates = [];
  for (const p of proposals) {
    const base = { schemaVersion: "activation-envelope.v1", symbol: p.symbol, decision: p.decision, proposalHash: sha(p), activationSnapshotId: snap.activationSnapshotId || snap.id || null,
      riskPolicyHash: sha({ schemaVersion: riskMandate.schemaVersion, riskMandate }), proposedQuantityUnits: null, authorizedQuantityUnits: "0",
      reservedNotionalMinor: "0", plannedLossAtBoundaryMinor: "0", bindingGapStressLossMinor: "0", clampReason: null, materialClamp: false, reasons: [], status: "REJECT" };
    const errs = validation && validation[p.symbol] && !validation[p.symbol].ok ? validation[p.symbol].errors : [];
    if (errs.length) { envelopes.push({ ...base, status: "REJECT", clampReason: "SEMANTIC_INVALID", reasons: errs.slice(0, 12) }); continue; }
    if (p.decision === "BUY") {
      const entry = p.action.entry, prot = p.action.protection;
      const candidate = PR.candidateFromResearch({ symbol: p.symbol, sector: sector(p.symbol), mandate: p, liquidity: marks[p.symbol] || {}, costPerShareMicros: riskMandate.stress.stressCostPerShareMicros }, advBySymbol);
      candidates.push(candidate);
      if (!PR) { envelopes.push({ ...base, proposedQuantityUnits: candidate.proposedQuantityUnits, clampReason: "RISK_MODULE_UNAVAILABLE", reasons: ["RISK_MODULE_UNAVAILABLE"] }); continue; }
      let q;
      try { q = PR.quantityAuthority({ candidate, portfolio, policy: riskMandate, marks: {}, advBySymbol, sectorOf: sector, clusterOf }); }
      catch (e) { envelopes.push({ ...base, proposedQuantityUnits: candidate.proposedQuantityUnits, clampReason: "RISK_EVALUATION_FAILED", reasons: [String(e.code || e.message).slice(0, 80)] }); continue; }
      const authorized = big(q.authorizedQuantityUnits), proposed = big(candidate.proposedQuantityUnits);
      if (authorized > proposed) throw typed("ENVELOPE_INCREASES_PROPOSAL", `${p.symbol}: authorized ${authorized} > proposed ${proposed}`);
      const limit = big(entry.limitPriceMicros), boundary = big(prot.lossBoundaryPriceMicros), cost = big(candidate.costPerShareMicros);
      const gapFill = q.perShare && q.perShare.gapHaltStressFillMicros ? big(q.perShare.gapHaltStressFillMicros) : limit - limit * big(riskMandate.stress.gapHaltAdverseBps) / 10000n;
      const toMinor = (micros) => M.toCanonical(M.divRound(micros, 10000n, M.ROUNDING.CEIL));
      const clamp = PR.clampMateriality({ proposedQuantityUnits: candidate.proposedQuantityUnits, authorizedQuantityUnits: q.authorizedQuantityUnits, limitPriceMicros: entry.limitPriceMicros, navMinor: snap.navMinor, policy: riskMandate });
      const maxCapital = big(p.allocation.maxCapitalMinor), reserved = authorized * (limit + cost);
      const status = authorized === 0n ? "REJECT" : (clamp.material ? "CLAMP" : "ALLOW");
      envelopes.push({ ...base, proposedQuantityUnits: candidate.proposedQuantityUnits, authorizedQuantityUnits: q.authorizedQuantityUnits,
        reservedNotionalMinor: toMinor(reserved), plannedLossAtBoundaryMinor: toMinor(authorized * (limit - boundary + cost)),
        bindingGapStressLossMinor: toMinor(authorized * (limit - gapFill + big(riskMandate.stress.stressCostPerShareMicros))),
        clampReason: status === "ALLOW" ? null : (q.refused ? (q.reasons[0] && q.reasons[0].code) || "REFUSED" : clamp.reason), materialClamp: clamp.material === true && authorized !== 0n,
        bindingConstraint: q.bindingConstraint, capacities: q.capacities, reasons: (q.reasons || []).map((r) => r.code || String(r)),
        withinDeclaredCapital: big(toMinor(authorized * limit)) <= maxCapital,   /* declared capital is notional at the limit; the reservation adds the cost allowance */ withinDeclaredLoss: big(toMinor(authorized * (limit - boundary + cost))) <= big(p.allocation.maxPlannedLossMinor),
        expiresAtMs: entryExpiresAtMs(p), status });
    } else if (p.decision === "HOLD") {
      const q = owned[p.symbol] || 0n;
      const prot = p.action.protection;
      const mark = (snap.positions || []).find((x) => x.symbol === p.symbol);
      const markMicros = mark && mark.markMicros ? big(mark.markMicros) : null;
      const reasons = [];
      if (q <= 0n) reasons.push("NOT_HELD");
      if (markMicros != null && big(prot.lossBoundaryPriceMicros) >= markMicros) reasons.push("LOSS_BOUNDARY_AT_OR_ABOVE_MARK");
      envelopes.push({ ...base, proposedQuantityUnits: q.toString(), authorizedQuantityUnits: q > 0n ? q.toString() : "0",
        plannedLossAtBoundaryMinor: markMicros != null && q > 0n ? M.toCanonical(M.divRound(q * (markMicros - big(prot.lossBoundaryPriceMicros) > 0n ? markMicros - big(prot.lossBoundaryPriceMicros) : 0n), 10000n, M.ROUNDING.CEIL)) : "0",
        status: q > 0n && !reasons.length ? "ALLOW" : "REJECT", clampReason: reasons[0] || null, reasons });
    } else if (p.decision === "REDUCE" || p.decision === "SELL") {
      const q = owned[p.symbol] || 0n;
      const want = big(p.action.exit.quantityUnits);
      const authorized = p.decision === "SELL" ? q : (want < q ? want : q);
      const reasons = [];
      if (q <= 0n) reasons.push("NOT_HELD");
      if (p.decision === "SELL" && want !== q) reasons.push("SELL_QUANTITY_MUST_EQUAL_OWNED");
      if (p.decision === "REDUCE" && want >= q) reasons.push("REDUCE_QUANTITY_NOT_BELOW_OWNED");
      envelopes.push({ ...base, proposedQuantityUnits: want.toString(), authorizedQuantityUnits: q > 0n && !reasons.length ? authorized.toString() : "0",
        status: q > 0n && !reasons.length ? "ALLOW" : "REJECT", clampReason: reasons[0] || null, reasons, materialClamp: false });
    } else {
      envelopes.push({ ...base, status: "REJECT", clampReason: "NOT_EXECUTABLE", reasons: ["NOT_EXECUTABLE_DECISION"] });
    }
  }
  /* the basket together: aggregate constraints at authorized quantities */
  let alternatives = null;
  if (PR && candidates.length) {
    try { alternatives = PR.runScenarios({ portfolio, candidates, policy: riskMandate, marks: {}, advBySymbol, sectorOf: sector, clusterOf }); } catch (e) { alternatives = { error: String(e.code || e.message).slice(0, 120) }; }
  }
  const basketFeasible = !alternatives || alternatives.error ? true : (alternatives.set ? alternatives.set.basketFeasible !== false : true);
  const hasMaterialClamp = envelopes.some((e) => e.materialClamp === true);
  const allAllow = envelopes.length > 0 && envelopes.every((e) => e.status === "ALLOW") && basketFeasible;
  return { envelopes, hasMaterialClamp, allAllow, basketFeasible, alternatives, planHash: hashPortfolioPlanProposal(portfolioPlanProposal || {}, proposals),
    rejected: envelopes.filter((e) => e.status !== "ALLOW").map((e) => ({ symbol: e.symbol, status: e.status, reason: e.clampReason, reasons: e.reasons })) };
}

/* ── desired order sets (§8.3) — pure ──────────────────────────────────── */
function desiredOrderSet({ proposal, envelope, mandateVersionId, accountId, reservationId }) {
  const orderSetId = `os_${mandateVersionId}`;
  const legs = [];
  const qty = envelope.authorizedQuantityUnits;
  const act = proposal.action;
  if (proposal.decision === "BUY") {
    legs.push({ legId: `${orderSetId}_ENTRY`, role: "ENTRY", side: "buy", type: "LIMIT", priceMicros: act.entry.limitPriceMicros, quantityUnits: qty, timeInForce: "DAY",
      sessionDates: act.entry.authorizedSessionDates, regularSessionOnly: true, restageEachAuthorizedSession: act.entry.restageEachAuthorizedSession === true });
    legs.push({ legId: `${orderSetId}_TARGET`, role: "TARGET", side: "sell", type: "LIMIT", priceMicros: act.protection.takeProfitPriceMicros, quantityUnits: qty, timeInForce: "GTC", ocoGroup: `${orderSetId}_OCO`, activatesOn: "ENTRY_FILL" });
    legs.push({ legId: `${orderSetId}_STOP`, role: "STOP", side: "sell", type: act.protection.lossOrderType, stopMicros: act.protection.lossBoundaryPriceMicros, quantityUnits: qty, timeInForce: "GTC", ocoGroup: `${orderSetId}_OCO`, activatesOn: "ENTRY_FILL",
      trailing: act.protection.trailing && act.protection.trailing.enabled ? act.protection.trailing : null });
    legs.push({ legId: `${orderSetId}_TIME_EXIT`, role: "SELL", side: "sell", type: act.protection.timeExit.orderType, collarBps: act.protection.timeExit.collarBps, quantityUnits: qty,
      triggerAfterSessionDate: act.protection.timeExit.triggerAfterSessionDate, submitAt: act.protection.timeExit.submitAt, activatesOn: "TIME_EXIT" });
  } else if (proposal.decision === "HOLD") {
    legs.push({ legId: `${orderSetId}_TARGET`, role: "TARGET", side: "sell", type: "LIMIT", priceMicros: act.protection.takeProfitPriceMicros, quantityUnits: qty, timeInForce: "GTC", ocoGroup: `${orderSetId}_OCO` });
    legs.push({ legId: `${orderSetId}_STOP`, role: "STOP", side: "sell", type: act.protection.lossOrderType, stopMicros: act.protection.lossBoundaryPriceMicros, quantityUnits: qty, timeInForce: "GTC", ocoGroup: `${orderSetId}_OCO` });
    legs.push({ legId: `${orderSetId}_TIME_EXIT`, role: "SELL", side: "sell", type: act.protection.timeExit.orderType, collarBps: act.protection.timeExit.collarBps, quantityUnits: qty,
      triggerAfterSessionDate: act.protection.timeExit.triggerAfterSessionDate, submitAt: act.protection.timeExit.submitAt, activatesOn: "TIME_EXIT" });
  } else {
    legs.push({ legId: `${orderSetId}_${proposal.decision}`, role: proposal.decision, side: "sell", type: act.exit.orderType, priceMicros: act.exit.limitPriceMicros || null, collarBps: act.exit.collarBps || null,
      quantityUnits: qty, timeInForce: "DAY", sessionDates: act.exit.authorizedSessionDates, onHaltOrNonfill: act.exit.onHaltOrNonfill });
    if (proposal.decision === "REDUCE" && act.protection) {
      legs.push({ legId: `${orderSetId}_TARGET`, role: "TARGET", side: "sell", type: "LIMIT", priceMicros: act.protection.takeProfitPriceMicros, quantityUnits: null, remainderOf: `${orderSetId}_${proposal.decision}`, timeInForce: "GTC", ocoGroup: `${orderSetId}_OCO` });
      legs.push({ legId: `${orderSetId}_STOP`, role: "STOP", side: "sell", type: act.protection.lossOrderType, stopMicros: act.protection.lossBoundaryPriceMicros, quantityUnits: null, remainderOf: `${orderSetId}_${proposal.decision}`, timeInForce: "GTC", ocoGroup: `${orderSetId}_OCO` });
    }
  }
  const purpose = proposal.decision === "BUY" ? "ENTRY_WITH_PROTECTION" : proposal.decision === "HOLD" ? "PROTECT" : proposal.decision;
  return { schemaVersion: "order-set.v1", orderSetId, accountId, symbol: proposal.symbol, mandateVersionId, desiredMandateVersionId: mandateVersionId, appliedMandateVersionId: null,
    purpose, status: "DESIRED", brokerGroupId: null, reservationId, legs, legIds: legs.map((l) => l.legId), createdAtMs: null };
}

/* ── the activation transaction (§6.8, Appendix B) ─────────────────────── */
async function readActivationCASState(tx, accountId, symbols, D) {
  const [acct, res] = await Promise.all([tx.get(D.col(D.COL.accounts).doc(accountId)), tx.get(D.col(D.COL.reservationAccounts).doc(accountId))]);
  const pointers = {};
  for (const s of symbols) { const p = await tx.get(D.col(D.COL.activeMandates).doc(seriesIdFor(accountId, s))); pointers[s] = p.exists ? p.data() : null; }
  const a = acct.exists ? acct.data() : {}, r = res.exists ? res.data() : {};
  return { portfolioVersion: Number(a.portfolioVersion) || Number(a.balanceRevision) || 0, writerEpoch: Number(a.writerEpoch) || 0,
    reservationAccountVersion: Number(r.version) || 0, reservation: r, pointers, accountExists: acct.exists };
}
function assertSnapshotStillCurrent({ live, activationSnapshot }) {
  const problems = [];
  if (live.portfolioVersion !== Number(activationSnapshot.portfolioVersion || 0)) problems.push(`portfolioVersion ${live.portfolioVersion} != ${activationSnapshot.portfolioVersion}`);
  if (live.reservationAccountVersion !== Number(activationSnapshot.reservationAccountVersion || 0)) problems.push(`reservationAccountVersion ${live.reservationAccountVersion} != ${activationSnapshot.reservationAccountVersion}`);
  if (live.writerEpoch !== Number(activationSnapshot.writerEpoch || 0)) problems.push(`writerEpoch ${live.writerEpoch} != ${activationSnapshot.writerEpoch}`);
  if (problems.length) throw typed("ACTIVATION_SNAPSHOT_STALE", problems.join("; "), { problems });
}
function assertAllOrNoneSemantics({ proposals, envelopes }) {
  if (envelopes.length !== proposals.length) throw typed("ENVELOPE_COUNT_MISMATCH", `${envelopes.length} envelopes for ${proposals.length} proposals`);
  const bad = envelopes.filter((e) => e.status !== "ALLOW");
  if (bad.length) throw typed("BASKET_NOT_ALL_ALLOW", `${bad.map((e) => `${e.symbol}:${e.clampReason || e.status}`).join(", ")}`);
}
function assertNoEnvelopeIncreasesProposal({ proposals, envelopes }) {
  for (const e of envelopes) {
    const p = proposals.find((x) => x.symbol === e.symbol);
    if (!p) throw typed("ENVELOPE_WITHOUT_PROPOSAL", e.symbol);
    if (e.proposalHash !== sha(p)) throw typed("ENVELOPE_PROPOSAL_HASH_MISMATCH", e.symbol);
    if (p.decision === "BUY" && big(e.authorizedQuantityUnits) > big(p.allocation.proposedQuantityUnits)) throw typed("ENVELOPE_INCREASES_PROPOSAL", e.symbol);
  }
}

/**
 * Stage one plan class atomically. Returns { status: COMMITTED | REJECTED |
 * NEEDS_SOL_RESYNTHESIS | STALE | EMPTY, ... }. Never touches the broker.
 */
async function stagePortfolioPlan({ planClass, portfolioPlanProposal = {}, proposals = [], verifiedProposalClaims = null, activationSnapshot, accountId, cutoff = null, policy = null,
  managerRunId = null, admin = null, eligibleSymbols = null, marks = {}, advBySymbol = {}, sectorOf = null, clusterOf = null, lineage = {}, verifiedValuations = {}, nowMs = Date.now() } = {}) {
  const D = db(admin);
  const SC = lazy("./_investorStorageCodec");
  const CV = lazy("./_investorClaimVerifier");
  if (!PLAN_CLASSES.includes(planClass)) return { status: "REJECTED", reason: "UNKNOWN_PLAN_CLASS" };
  if (!proposals.length) return { status: "EMPTY", reason: "no_proposals", planClass };
  if (!activationSnapshot || !activationSnapshot.activationSnapshotId) return { status: "REJECTED", reason: "ACTIVATION_SNAPSHOT_REQUIRED" };
  const riskMandate = (policy && policy.riskMandate) || POLICY.RISK_MANDATE;
  const heldSymbols = (activationSnapshot.positions || []).map((p) => p.symbol);
  const pendingSymbols = (activationSnapshot.workingOrders || []).map((o) => o.symbol);
  const validation = {};
  for (const p of proposals) {
    validation[p.symbol] = validateProposal(p, { eligibleSymbols, heldSymbols, pendingSymbols, nowMs, policy });
    if (p.decision === "BUY") {
      try { require("./_investorDecisionValidation").assertMandate(p, verifiedValuations[p.symbol]); }
      catch (e) { validation[p.symbol].ok = false; validation[p.symbol].errors.push(e.code || "VALUATION_BINDING_FAILED"); }
    }
  }
  const dup = proposals.map((p) => p.symbol).filter((s, i, a) => a.indexOf(s) !== i);
  if (dup.length) return { status: "REJECTED", reason: "DUPLICATE_SYMBOL_IN_PLAN", symbols: dup };
  if (planClass === "EXPANSION" && proposals.some((p) => p.decision !== "BUY")) return { status: "REJECTED", reason: "EXPANSION_PLAN_CONTAINS_NON_BUY" };
  if (planClass === "RISK_MAINTENANCE" && proposals.some((p) => p.decision === "BUY")) return { status: "REJECTED", reason: "MAINTENANCE_PLAN_CONTAINS_BUY" };
  const envelopes = buildPortfolioActivationEnvelopes({ portfolioPlanProposal, proposals, activationSnapshot, riskMandate, marks, advBySymbol, sectorOf, clusterOf, validation });
  const planHash = envelopes.planHash;
  const planId = planIdFor(planClass, planHash);
  if (envelopes.hasMaterialClamp && planClass === "EXPANSION") {
    await persistPlanAudit(D, { planId, planClass, planHash, status: "NEEDS_SOL_RESYNTHESIS", accountId, managerRunId, activationSnapshotId: activationSnapshot.activationSnapshotId, envelopes: envelopes.envelopes, reason: "MATERIAL_ACTIVATION_CLAMP" });
    return { status: "NEEDS_SOL_RESYNTHESIS", reason: "MATERIAL_ACTIVATION_CLAMP", planId, revisedFeasibleAlternatives: envelopes.alternatives, envelopes: envelopes.envelopes };
  }
  if (!envelopes.allAllow) {
    await persistPlanAudit(D, { planId, planClass, planHash, status: "REJECTED", accountId, managerRunId, activationSnapshotId: activationSnapshot.activationSnapshotId, envelopes: envelopes.envelopes, reason: envelopes.basketFeasible ? "ENVELOPE_REJECTED" : "BASKET_INFEASIBLE", rejected: envelopes.rejected });
    return { status: "REJECTED", reason: envelopes.basketFeasible ? "ENVELOPE_REJECTED" : "BASKET_INFEASIBLE", planId, rejected: envelopes.rejected, envelopes: envelopes.envelopes };
  }
  const symbols = proposals.map((p) => p.symbol);
  const idempotencyKey = `${planHash}:${activationSnapshot.activationSnapshotId}:DESIRED`;
  try {
    const out = await D.runTransaction(async (tx) => {
      const live = await readActivationCASState(tx, accountId, symbols, D);
      assertSnapshotStillCurrent({ live, activationSnapshot });
      if (CV && verifiedProposalClaims && Object.keys(verifiedProposalClaims.byProposal || {}).length) {
        await CV.assertImmutableClaimVerdicts(tx, verifiedProposalClaims, { requireSupported: true, admin: D });
      } else if (planClass === "EXPANSION") throw typed("CLAIM_VERDICTS_REQUIRED", "an expansion plan requires immutable claim verdicts");
      assertAllOrNoneSemantics({ proposals, envelopes: envelopes.envelopes });
      assertNoEnvelopeIncreasesProposal({ proposals, envelopes: envelopes.envelopes });
      if (live.reservation && live.reservation.lastIdempotencyKey === idempotencyKey) throw typed("PLAN_ALREADY_COMMITTED", "same plan and snapshot already committed", { planId });
      const bindings = [];
      let reservedNotional = big(live.reservation.reservedNotionalMinor || "0"), reservedPlanned = big(live.reservation.reservedPlannedLossMinor || "0"), reservedStress = big(live.reservation.reservedStressLossMinor || "0");
      for (const p of proposals) {
        const env = envelopes.envelopes.find((e) => e.symbol === p.symbol);
        const pointer = live.pointers[p.symbol] || {};
        if (pointer.desiredIdempotencyKey === idempotencyKey) throw typed("MANDATE_ALREADY_DESIRED", p.symbol);
        const seriesId = seriesIdFor(accountId, p.symbol);
        const version = (Number(pointer.desiredVersion) || 0) + 1;
        const proposalHash = sha(p), proposalId = proposalIdFor(p);
        const mandateVersionId = `${seriesId}_v${String(version).padStart(4, "0")}_${proposalHash.slice(0, 12)}`;
        const reservationId = `res_${mandateVersionId}`;
        const binding = { schemaVersion: "mandate-binding.v1", mandateVersionId, mandateSeriesId: seriesId, version, previousVersionId: pointer.desiredVersionId || null,
          expectedActiveVersion: Number(pointer.desiredVersion) || 0, proposalId, proposalHash, portfolioPlanId: planId, planClass, managerRunId, accountId, symbol: p.symbol, decision: p.decision,
          dossierVersionId: lineage.dossierVersionId || (lineage.bySymbol && lineage.bySymbol[p.symbol] && lineage.bySymbol[p.symbol].dossierVersionId) || null,
          researchVersionId: (lineage.bySymbol && lineage.bySymbol[p.symbol] && lineage.bySymbol[p.symbol].researchVersionId) || null,
          model: lineage.model || POLICY.ROLE_MODELS.manager.model, reasoningEffort: lineage.reasoningEffort || POLICY.ROLE_MODELS.manager.reasoning.effort, promptHash: lineage.promptHash || null, schemaHash: POLICY.schemaHash("mandate-proposal.v1"),
          policyHash: (policy && policy.policyHash) || null, contextManifestHash: lineage.contextManifestHash || null, sourceManifestHash: sha(p.sourceManifest || []),
          verifiedValuation:verifiedValuations[p.symbol] || null,
          cutoffMs: cutoff && cutoff.cutoffMs != null ? cutoff.cutoffMs : null, boundAtMs: nowMs, idempotencyKey };
        const envelope = { ...env, mandateVersionId, portfolioPlanId: planId, reservationId, validatedAtMs: nowMs };
        const orderSet = { ...desiredOrderSet({ proposal: p, envelope, mandateVersionId, accountId, reservationId }), createdAtMs: nowMs, planId };
        const reservation = { schemaVersion: "capital-reservation.v1", reservationId, accountId, symbol: p.symbol, mandateVersionId, portfolioPlanId: planId, status: "ACTIVE",
          reservedNotionalMinor: env.reservedNotionalMinor, plannedLossMinor: env.plannedLossAtBoundaryMinor, stressLossMinor: env.bindingGapStressLossMinor, createdAtMs: nowMs, releasedAtMs: null, currency: "USD" };
        const transitionId = `${idempotencyKey}:${mandateVersionId}`;
        const outbox = { transitionId, accountId, symbol: p.symbol, mandateVersionId, orderSetId: orderSet.orderSetId, planId, kind: "APPLY_DESIRED_ORDER_SET", status: "PENDING",
          idempotencyKey: transitionId, attempts: 0, createdAtMs: nowMs, claimedBy: null, claimedAtMs: null, authority: "SOL_MANDATE" };
        const pointerNext = { accountId, symbol: p.symbol, desiredProposalHash:proposalHash, mandateSeriesId: seriesId, desiredVersion: version, desiredVersionId: mandateVersionId, desiredIdempotencyKey: idempotencyKey,
          appliedVersion: Number(pointer.appliedVersion) || 0, appliedVersionId: pointer.appliedVersionId || null, status: "DESIRED", planClass, decision: p.decision,
          capitalRank: p.allocation ? p.allocation.capitalRank : null, expiresAtMs: env.expiresAtMs || null, lossBoundaryPriceMicros: p.action.protection ? p.action.protection.lossBoundaryPriceMicros : (pointer.lossBoundaryPriceMicros || null),
          takeProfitPriceMicros: p.action.protection ? p.action.protection.takeProfitPriceMicros : (pointer.takeProfitPriceMicros || null),
          action: p.action, allocation: p.allocation || null, authorizedQuantityUnits: env.authorizedQuantityUnits, protectionAcknowledged: pointer.protectionAcknowledged === true,
          protectionState: pointer.protectionState || null, pausedReason: null, updatedAtMs: nowMs };
        bindings.push({ p, binding, envelope, orderSet, reservation, outbox, pointerNext, seriesId, mandateVersionId, proposalId, proposalHash, version });
        reservedNotional += big(env.reservedNotionalMinor); reservedPlanned += big(env.plannedLossAtBoundaryMinor); reservedStress += big(env.bindingGapStressLossMinor);
      }
      const enc = (doc) => { if (!SC) return doc; try { return SC.encode(doc); } catch (e) { throw typed("CODEC_REJECTED", `${doc.schemaVersion}: ${e.message}`); } };
      for (const b of bindings) {
        tx.set(D.col(D.COL.mandateProposals).doc(b.proposalId), enc({ schemaVersion: "mandate-proposal.v1", proposalId: b.proposalId, proposalHash: b.proposalHash, symbol: b.p.symbol, decision: b.p.decision, proposal: b.p, ...D.envelope({ created_by: "mandate.stagePortfolioPlan" }) }));
        tx.set(D.col(D.COL.mandates).doc(b.mandateVersionId), enc({ ...b.binding, ...D.envelope({ created_by: "mandate.stagePortfolioPlan" }) }));
        tx.set(D.col(D.COL.activationEnvelopes).doc(b.mandateVersionId), enc({ ...b.envelope, ...D.envelope({ created_by: "mandate.stagePortfolioPlan" }) }));
        tx.set(D.col(D.COL.capitalReservations).doc(b.reservation.reservationId), enc({ ...b.reservation, ...D.envelope({ created_by: "mandate.stagePortfolioPlan" }) }));
        /* order sets and legs are LIFECYCLE records the executor advances in place (status, fills, broker ids), so they are
           stored plain; the immutable records above carry the codec's content hash */
        tx.set(D.col(D.COL.orderSets).doc(b.orderSet.orderSetId), { ...b.orderSet, ...D.envelope({ created_by: "mandate.stagePortfolioPlan" }) });
        for (const leg of b.orderSet.legs) tx.set(D.col(D.COL.orderLegs).doc(leg.legId), { schemaVersion: "order-leg.v1", ...leg, orderSetId: b.orderSet.orderSetId, accountId, symbol: b.p.symbol, mandateVersionId: b.mandateVersionId, filledUnits: "0", remainingUnits: leg.quantityUnits, brokerOrderId: null, status: "DESIRED", createdAtMs: nowMs });
        tx.set(D.col(D.COL.executionOutbox).doc(b.outbox.transitionId), b.outbox);
        tx.set(D.col(D.COL.activeMandates).doc(b.seriesId), b.pointerNext, { merge: true });
        tx.set(D.col(D.COL.mandateEvents).doc(`${b.mandateVersionId}_${String(1).padStart(4, "0")}`), { mandateVersionId: b.mandateVersionId, mandateSeriesId: b.seriesId, sequence: 1, accountId, symbol: b.p.symbol,
          kind: "VALIDATED_AND_DESIRED", planId, planClass, activationSnapshotId: activationSnapshot.activationSnapshotId, authorizedQuantityUnits: b.envelope.authorizedQuantityUnits, atMs: nowMs });
      }
      tx.set(D.col(D.COL.portfolioPlans).doc(planId), { schemaVersion: "portfolio-plan.v1", planId, planClass, planHash, status: "COMMITTED", accountId, managerRunId, activationSnapshotId: activationSnapshot.activationSnapshotId,
        idempotencyKey, mandateVersionIds: bindings.map((b) => b.mandateVersionId), symbols, proposal: { ...portfolioPlanProposal, actionableMandates: undefined }, committedAtMs: nowMs, ...D.envelope({ created_by: "mandate.stagePortfolioPlan" }) });
      tx.set(D.col(D.COL.reservationAccounts).doc(accountId), { accountId, version: live.reservationAccountVersion + 1, reservedNotionalMinor: reservedNotional.toString(), reservedPlannedLossMinor: reservedPlanned.toString(),
        reservedStressLossMinor: reservedStress.toString(), committedPortfolioPlanId: planId, lastIdempotencyKey: idempotencyKey, updatedAtMs: nowMs }, { merge: true });
      return { status: "COMMITTED", planId, planHash, mandateVersionIds: bindings.map((b) => b.mandateVersionId), reservationAccountVersion: live.reservationAccountVersion + 1,
        activationSnapshotId: activationSnapshot.activationSnapshotId, reservedNotionalMinor: reservedNotional.toString() };
    });
    return out;
  } catch (e) {
    const status = e.code === "ACTIVATION_SNAPSHOT_STALE" ? "STALE" : (e.code === "PLAN_ALREADY_COMMITTED" || e.code === "MANDATE_ALREADY_DESIRED" ? "DUPLICATE" : "REJECTED");
    await persistPlanAudit(D, { planId, planClass, planHash, status, accountId, managerRunId, activationSnapshotId: activationSnapshot.activationSnapshotId, envelopes: envelopes.envelopes, reason: e.code || "TRANSACTION_FAILED", message: String(e.message).slice(0, 200) }).catch(() => {});
    return { status, reason: e.code || "TRANSACTION_FAILED", message: String(e.message).slice(0, 200), planId, problems: e.problems || null };
  }
}
async function persistPlanAudit(D, fields) {
  const id = `${fields.planId}_${fields.status}_${Date.now()}`;
  await D.col(D.COL.portfolioPlans).doc(id).set({ schemaVersion: "portfolio-plan.v1", auditOnly: true, ...fields, atMs: Date.now(), ...D.envelope({ created_by: "mandate.persistPlanAudit" }) });
  return { auditId: id };
}

/* ── pointer operations used by the router, the manager and the executor ─ */
async function readPointer(accountId, symbol, { admin = null } = {}) {
  const D = db(admin);
  const s = await D.col(D.COL.activeMandates).doc(seriesIdFor(accountId, symbol)).get();
  return s.exists ? s.data() : null;
}
async function appendEvent(D, pointer, kind, fields = {}) {
  const seq = (Number(pointer.eventSequence) || 1) + 1;
  await D.col(D.COL.mandateEvents).doc(`${pointer.desiredVersionId || pointer.mandateSeriesId}_${String(seq).padStart(4, "0")}`).set({ mandateVersionId: pointer.desiredVersionId || null, mandateSeriesId: pointer.mandateSeriesId, sequence: seq, accountId: pointer.accountId, symbol: pointer.symbol, kind, atMs: Date.now(), ...fields });
  await D.col(D.COL.activeMandates).doc(pointer.mandateSeriesId).set({ eventSequence: seq }, { merge: true });
  return seq;
}
async function hasActiveMandate(symbol, { accountId = null, admin = null } = {}) {
  const D = db(admin);
  const acct = accountId || (await D.col(D.COL.control).doc("control").get().then((s) => (s.exists ? s.data().accountId : null)).catch(() => null)) || "paper-1";
  const p = await readPointer(acct, symbol, { admin });
  return !!(p && p.desiredVersionId && !["CANCELLED", "SUPERSEDED", "ENTRY_EXPIRED", "FILLED"].includes(p.status) && (p.decision === "BUY" || p.decision === "HOLD" || p.decision === "REDUCE" || p.decision === "SELL"));
}
/** Appendix B: a high-impact verified delta pauses an UNFILLED entry. Protection on owned shares is never touched. */
async function pauseUnfilledEntry(symbol, eventId, { accountId = null, admin = null } = {}) {
  const D = db(admin);
  const acct = accountId || (await D.col(D.COL.control).doc("control").get().then((s) => (s.exists ? s.data().accountId : null)).catch(() => null)) || "paper-1";
  const p = await readPointer(acct, symbol, { admin });
  if (!p || p.decision !== "BUY" || !p.desiredVersionId) return { paused: false, reason: "no_unfilled_entry" };
  if (["FILLED", "CANCELLED", "SUPERSEDED", "ENTRY_EXPIRED"].includes(p.status)) return { paused: false, reason: `status_${p.status}` };
  const transitionId = `${p.desiredVersionId}:PAUSE_ENTRY:${eventId}`;
  await D.col(D.COL.executionOutbox).doc(transitionId).set({ transitionId, accountId: acct, symbol, mandateVersionId: p.desiredVersionId, orderSetId: `os_${p.desiredVersionId}`, kind: "CANCEL_UNFILLED_ENTRY",
    reason: "material_event_pending_review", eventId, status: "PENDING", idempotencyKey: transitionId, attempts: 0, createdAtMs: Date.now(), authority: "EVIDENCE_PAUSE" });
  await D.col(D.COL.activeMandates).doc(p.mandateSeriesId).set({ status: "PAUSED_EVIDENCE", pausedReason: `material_event:${eventId}`, pausedAtMs: Date.now() }, { merge: true });
  await appendEvent(D, p, "PAUSED_EVIDENCE", { eventId });
  return { paused: true, mandateVersionId: p.desiredVersionId, transitionId };
}
async function requestEntryCancelWithOutbox({ state, eventId, accountId, admin = null } = {}) {
  const D = db(admin);
  const p = await readPointer(accountId, state.symbol, { admin });
  if (!p || !p.desiredVersionId) return { status: "NOT_APPLIED", reason: "no_pointer" };
  const transitionId = `${p.desiredVersionId}:REVOKE_ENTRY:${eventId || "manager"}`;
  await D.col(D.COL.executionOutbox).doc(transitionId).set({ transitionId, accountId, symbol: state.symbol, mandateVersionId: p.desiredVersionId, orderSetId: `os_${p.desiredVersionId}`, kind: "CANCEL_UNFILLED_ENTRY",
    reason: "entry_revoked_by_sol", eventId: eventId || null, status: "PENDING", idempotencyKey: transitionId, attempts: 0, createdAtMs: Date.now(), authority: "SOL_REVISION", releaseReservationOnTerminal: true });
  await D.col(D.COL.activeMandates).doc(p.mandateSeriesId).set({ status: "CANCEL_PENDING", pausedReason: "revoked", updatedAtMs: Date.now() }, { merge: true });
  await appendEvent(D, p, "ENTRY_REVOKED", { eventId: eventId || null });
  return { status: "CANCEL_PENDING", transitionId, note: "reservation stays binding until broker truth proves the remainder terminal" };
}
async function keepEvidencePausedAndCancelRemainder({ state, eventId, accountId, admin = null } = {}) {
  const out = await requestEntryCancelWithOutbox({ state, eventId, accountId, admin });
  const D = db(admin);
  const p = await readPointer(accountId, state.symbol, { admin });
  if (p) await D.col(D.COL.activeMandates).doc(p.mandateSeriesId).set({ status: "PAUSED_EVIDENCE", pausedReason: `abstain_after_event:${eventId || ""}` }, { merge: true });
  return { ...out, status: "PAUSED_EVIDENCE" };
}
async function retainProtectionActionRequired({ state, assessment, accountId, admin = null } = {}) {
  const D = db(admin);
  const p = await readPointer(accountId, state.symbol, { admin });
  if (p) { await D.col(D.COL.activeMandates).doc(p.mandateSeriesId).set({ status: "ACTION_REQUIRED", actionRequiredReason: `ABSTAIN:${(assessment && assessment.reasonCode) || "UNCERTAINTY"}`, updatedAtMs: Date.now() }, { merge: true }); await appendEvent(D, p, "ACTION_REQUIRED", { reasonCode: assessment && assessment.reasonCode || null }); }
  await raiseAlert(D, { conditionId: `action_required_${accountId}_${state.symbol}`, severity: "critical", kind: "ACTION_REQUIRED", accountId, symbol: state.symbol, reason: `held position ABSTAIN: ${(assessment && assessment.reasonCode) || "UNCERTAINTY"}; protection retained` });
  return { status: "ACTION_REQUIRED", protectionRetained: true };
}
async function retainPriorAcknowledgedProtectionAndAlert({ accountId, affectedSymbols = [], reason, admin = null } = {}) {
  const D = db(admin);
  for (const s of affectedSymbols) {
    const p = await readPointer(accountId, s, { admin });
    if (p) { await D.col(D.COL.activeMandates).doc(p.mandateSeriesId).set({ status: "ACTION_REQUIRED", actionRequiredReason: `MAINTENANCE_NOT_COMMITTED:${reason}`, updatedAtMs: Date.now() }, { merge: true }); await appendEvent(D, p, "ACTION_REQUIRED", { reason }); }
    await raiseAlert(D, { conditionId: `maintenance_not_committed_${accountId}_${s}`, severity: "critical", kind: "ACTION_REQUIRED", accountId, symbol: s, reason: `holding maintenance not committed (${reason}); prior acknowledged protection retained` });
  }
  return { retained: affectedSymbols.length, reason };
}
async function raiseAlert(D, { conditionId, severity, kind, accountId, symbol = null, reason }) {
  try { await D.col(D.COL.alerts).doc(conditionId).set({ conditionId, severity, kind, accountId, symbol, reason: String(reason).slice(0, 300), active: true, raisedAtMs: Date.now(), acknowledgedAtMs: null, resolvedAtMs: null }, { merge: true }); } catch {}
}

module.exports = {
  MANDATE_VERSION, PLAN_CLASSES, ENVELOPE_STATUSES, POINTER_STATUSES, MAX_AUTHORIZED_SESSIONS,
  seriesIdFor, proposalIdFor, planIdFor, hashPortfolioPlanProposal, entryExpiresAtMs, sessionCloseMs,
  validateProposal, buildPortfolioActivationEnvelopes, desiredOrderSet,
  readActivationCASState, assertSnapshotStillCurrent, assertAllOrNoneSemantics, assertNoEnvelopeIncreasesProposal,
  stagePortfolioPlan, persistPlanAudit,
  readPointer, hasActiveMandate, pauseUnfilledEntry, requestEntryCancelWithOutbox, keepEvidencePausedAndCancelRemainder,
  retainProtectionActionRequired, retainPriorAcknowledgedProtectionAndAlert, raiseAlert, appendEvent,
};
