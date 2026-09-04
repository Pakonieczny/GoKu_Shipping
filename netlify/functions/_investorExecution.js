/*  netlify/functions/_investorExecution.js  (fund-manager-v1)
 *  ---------------------------------------------------------------------------
 *  Investor AI — deterministic execution that does not call AI (blueprint
 *  §8.1, §8.3–§8.6, §10.5, invariant I-2).
 *
 *  THE SIMULATOR (pure, §8.5 — a hard rule for the whole system)
 *    A bar touching a level is a TOUCH, not automatically a FILL. A buy
 *    limit marketable at the open fills at the open; a touched unmarketable
 *    limit fills at the limit subject to volume participation; a sell stop
 *    gapped through fills at the open, else at the stop less modelled
 *    slippage; a stop-limit may trigger and stay unfilled; a marketable
 *    limit obeys its collar; a halted bar (no volume) fills nothing. When
 *    one OHLC bar could execute both the target and the stop with no finer
 *    sequence, the ADVERSE path is taken and the observation is marked
 *    AMBIGUOUS; a bar that first becomes entry-fillable and also crosses an
 *    exit never awards the favourable exit. Every level is tested against
 *    the bar's high and low, never the close.
 *
 *  THE SAGA (§8.6, §10.5)
 *    Committed desired state reaches the broker through the outbox:
 *    claim → apply via the bound adapter → advance the applied pointer from
 *    the acknowledgement. Fills are immutable events; positions are rebuilt
 *    from fills; the ledger stays balanced; reservations release only when
 *    the entry is terminal. First partial fill: cancel the remainder, await
 *    the terminal result, recompute owned quantity, attach protection for
 *    exactly that quantity; PROTECTION_PENDING until acknowledged.
 *
 *  Nothing here calls a model, chooses a company, or changes a price. A
 *  touch executes the already-authorized leg; $0 of AI.
 * ---------------------------------------------------------------------------
 */

"use strict";

const crypto = require("crypto");
const A = require("./_investorAdmin");
const M = require("./_investorMoney");
const POLICY = require("./_investorPolicy");

const ENGINE_VERSION = "manager";
const DECISION_AUTHORITY = "SOL";
const DEFAULT_PARTICIPATION_BPS = 1000n;     // fill at most 10% of a bar's volume
const STOP_SLIPPAGE_BPS = 10n;               // modelled adverse print past a triggered stop
const PROTECTION_SLA_SECONDS = 300;
const MICROS = 1000000n;

function sha(v) { return crypto.createHash("sha256").update(typeof v === "string" ? v : JSON.stringify(POLICY.canonical(v))).digest("hex"); }
function big(v) { return BigInt(String(v == null ? 0 : v)); }
function micros(usd) { const n = Number(usd); if (!Number.isFinite(n) || n <= 0) return null; return BigInt(Math.round(n * 1e6)); }
function minorOf(qty, priceMicros, mode = M.ROUNDING.HALF_EVEN) { return M.divRound(big(qty) * big(priceMicros), 10000n, mode); }
function db(admin) { return admin || A; }
function lazy(m) { try { return require(m); } catch { return null; } }
function rows(snap) { const out = []; if (snap && typeof snap.forEach === "function") snap.forEach((d) => out.push(d.data())); return out; }
function typed(code, message, extra = {}) { return Object.assign(new Error(message || code), { code, ...extra }); }

/* ── the simulator (PURE) ──────────────────────────────────────────────── */
function participationCap(bar, participationBps) {
  const v = Number(bar.v);
  if (!Number.isFinite(v) || v <= 0) return 0n;
  return BigInt(Math.floor(v)) * participationBps / 10000n;
}
/** One leg against one bar. Returns { touch, fill, reason, triggered }. */
function simulateLegOnBar({ leg, bar, participationBps = DEFAULT_PARTICIPATION_BPS, stopSlippageBps = STOP_SLIPPAGE_BPS } = {}) {
  const o = micros(bar.o), h = micros(bar.h), l = micros(bar.l);
  const remaining = big(leg.remainingUnits != null ? leg.remainingUnits : leg.quantityUnits);
  const none = (reason, extra = {}) => ({ touch: false, fill: null, reason, triggered: false, ...extra });
  if (o == null || h == null || l == null) return none("bar_unpriced");
  if (remaining <= 0n) return none("nothing_remaining");
  const halted = !(Number(bar.v) > 0) || bar.halted === true;
  if (halted) return none("halted_no_volume", { touch: false });
  const cap = participationCap(bar, participationBps);
  const qtyCapped = (q) => (cap > 0n ? (q < cap ? q : cap) : 0n);
  const fillAt = (priceMicros, q, basis, touch = true, extra = {}) => ({ touch, fill: q > 0n ? { quantityUnits: q.toString(), priceMicros: priceMicros.toString(), basis } : null, reason: q > 0n ? basis : "participation_exhausted", triggered: true, ...extra });
  const type = String(leg.type || "").toUpperCase();
  const side = String(leg.side || "").toLowerCase();
  if (leg.role === "ENTRY" && side === "buy" && type === "LIMIT") {
    const limit = big(leg.priceMicros);
    if (o <= limit) return fillAt(o < limit ? o : limit, qtyCapped(remaining), "buy_limit_marketable_at_open");
    if (l <= limit) return fillAt(limit, qtyCapped(remaining), "buy_limit_touched_queue_participation");
    return none("above_limit");
  }
  if (leg.role === "TARGET" || (side === "sell" && type === "LIMIT" && leg.role !== "STOP")) {
    const limit = big(leg.priceMicros);
    if (o >= limit) return fillAt(o, qtyCapped(remaining), "sell_limit_gapped_above_fills_at_open");
    if (h >= limit) return fillAt(limit, qtyCapped(remaining), "sell_limit_touched_queue_participation");
    return none("below_target");
  }
  if (leg.role === "STOP" && type === "STOP") {
    const stop = big(leg.stopMicros);
    if (l > stop) return none("above_stop");
    /* a stop-market fills fully: gapped through at the open, else at the stop less an adverse print */
    if (o < stop) return fillAt(o, remaining, "sell_stop_gapped_through_fills_at_open");
    const adverse = stop - stop * stopSlippageBps / 10000n;
    return fillAt(adverse > l ? adverse : l, remaining, "sell_stop_triggered_adverse_print");
  }
  if (leg.role === "STOP" && type === "STOP_LIMIT") {
    const stop = big(leg.stopMicros), limit = big(leg.priceMicros || leg.stopMicros);
    if (l > stop) return none("above_stop");
    const c = micros(bar.c);
    if (c != null && c >= limit) return fillAt(limit, qtyCapped(remaining), "sell_stop_limit_triggered_and_filled");
    return { touch: true, fill: null, reason: "stop_limit_triggered_unfilled", triggered: true };
  }
  if (type === "MARKETABLE_LIMIT") {
    /* the collar is measured from the reference at submission (the prior close when known, else this open) */
    const collar = big(leg.collarBps || "0");
    const ref = micros(bar.prevClose) || big(leg.referencePriceMicros || 0) || o;
    const bound = side === "sell" ? ref - ref * collar / 10000n : ref + ref * collar / 10000n;
    /* the marketable limit is submitted at the open of its eligible session and fills there inside the collar */
    const px = side === "sell" ? (o >= bound ? o : null) : (o <= bound ? o : null);
    if (px == null) return { touch: false, fill: null, reason: "outside_collar", triggered: true };
    return fillAt(px, qtyCapped(remaining), "marketable_limit_at_open_within_collar");
  }
  return none("unsupported_leg_type");
}
/** PURE. The predeclared adverse convention for one bar across an order set's legs. */
function resolveBarCollisions({ entry = null, target = null, stop = null } = {}) {
  const out = { entry, target, stop, ambiguous: false, notes: [] };
  const entryFilled = !!(entry && entry.fill);
  const targetHit = !!(target && target.fill), stopHit = !!(stop && stop.fill);
  if (entryFilled && targetHit) { out.target = { ...target, fill: null, reason: "favourable_same_bar_exit_not_awarded", suppressed: true }; out.notes.push("entry and target in one bar: target deferred"); }
  if (entryFilled && stopHit) { out.ambiguous = true; out.notes.push("entry and stop in one bar: adverse path, stop fills"); }
  if (!entryFilled && targetHit && stopHit) { out.target = { ...target, fill: null, reason: "adverse_path_stop_preferred", suppressed: true }; out.ambiguous = true; out.notes.push("target and stop in one bar: adverse path, stop fills"); }
  return out;
}

/* ── journal (admin-injectable; mirrors _investorLedger.post) ─────────── */
const ACCT = Object.freeze({ CASH: "cash", POSITIONS: "positions", FRICTION: "friction", REALIZED_PL: "realized_pl" });
function assertBalanced(legs) { const sum = legs.reduce((n, l) => n + Number(l.amountCents), 0); if (!Number.isInteger(sum) || sum !== 0) throw typed("LEDGER_UNBALANCED", `legs sum to ${sum}`); }
async function postJournal({ admin = null, accountId, kind, idParts, legs, meta = {} }) {
  const D = db(admin);
  assertBalanced(legs);
  const id = crypto.createHash("sha256").update([accountId, kind, ...idParts].map(String).join("|")).digest("hex").slice(0, 32);
  const lref = D.col(D.COL.ledger).doc(id), aref = D.col(D.COL.accounts).doc(accountId);
  return D.runTransaction(async (tx) => {
    const [l, a] = await Promise.all([tx.get(lref), tx.get(aref)]);
    if (l.exists) return { id, duplicate: true };
    if (!a.exists) throw typed("ACCOUNT_MISSING", accountId);
    const acct = a.data();
    const balance = { ...(acct.balanceCents || {}) };
    for (const leg of legs) balance[leg.account] = (Number(balance[leg.account]) || 0) + Number(leg.amountCents);
    tx.set(lref, { txnId: id, accountId, kind, legs, meta, engineVersion: ENGINE_VERSION, postedAtMs: Date.now(), ...D.envelope({ created_by: "execution.postJournal" }) });
    tx.set(aref, { balanceCents: balance, balanceRevision: (Number(acct.balanceRevision) || 0) + 1, balanceUpdatedAtMs: Date.now() }, { merge: true });
    return { id, duplicate: false };
  });
}
/** Conservation: the account projection equals the sum of its journal. */
async function assertConservation(accountId, { admin = null } = {}) {
  const D = db(admin);
  const journal = rows(await D.col(D.COL.ledger).where("accountId", "==", accountId).get());
  const rebuilt = {};
  for (const t of journal) for (const leg of t.legs || []) rebuilt[leg.account] = (rebuilt[leg.account] || 0) + Number(leg.amountCents);
  const a = await D.col(D.COL.accounts).doc(accountId).get();
  const projected = (a.exists && a.data().balanceCents) || {};
  const keys = [...new Set([...Object.keys(rebuilt), ...Object.keys(projected)])];
  const discrepancies = keys.filter((k) => (rebuilt[k] || 0) !== (projected[k] || 0)).map((k) => ({ account: k, rebuiltCents: rebuilt[k] || 0, projectedCents: projected[k] || 0 }));
  return { pass: discrepancies.length === 0, discrepancies, transactions: journal.length };
}

/* ── fills → legs → positions → ledger ─────────────────────────────────── */
function positionDocId(accountId, symbol) { return `${accountId}_${symbol}`; }
async function readPosition(D, accountId, symbol) { const s = await D.col(D.COL.positions).doc(positionDocId(accountId, symbol)).get(); return s.exists ? s.data() : null; }
/** Record one immutable fill and its consequences. Idempotent by fill id. */
async function recordFill({ admin = null, accountId, orderSet, leg, fill, bar = null, provenance = null, feeMinor = "0", nowMs = Date.now(), source = "paper" } = {}) {
  const D = db(admin);
  const fillId = `fill_${sha([leg.legId, bar ? bar.t : fill.eventId || nowMs, fill.quantityUnits, fill.priceMicros].join("|")).slice(0, 32)}`;
  const fref = D.col(D.COL.fills).doc(fillId);
  const existing = await fref.get();
  if (existing.exists) return { fillId, duplicate: true };
  const qty = big(fill.quantityUnits), px = big(fill.priceMicros);
  const notionalMinor = minorOf(qty, px, M.ROUNDING.HALF_EVEN);
  const fee = big(feeMinor);
  const sideSell = String(leg.side).toLowerCase() === "sell";
  const symbol = orderSet.symbol;
  const pos = await readPosition(D, accountId, symbol);
  let legsJournal, positionNext, realizedMinor = 0n, closed = false;
  if (!sideSell) {
    const prevQty = pos && pos.open ? big(pos.quantityUnits || pos.qty || 0) : 0n;
    const prevCost = pos && pos.open ? big(pos.costBasisMinor || pos.costBasisCents || 0) : 0n;
    const nextQty = prevQty + qty, nextCost = prevCost + notionalMinor + fee;
    legsJournal = [{ account: ACCT.CASH, amountCents: -Number(notionalMinor + fee), memo: `${symbol} buy ${qty}@${px}` }, { account: ACCT.POSITIONS, amountCents: Number(notionalMinor), memo: `${symbol} position` }, { account: ACCT.FRICTION, amountCents: Number(fee), memo: "fees" }];
    positionNext = { accountId, symbol, open: true, quantityUnits: nextQty.toString(), qty: Number(nextQty), costBasisMinor: nextCost.toString(), costBasisCents: Number(nextCost),
      entryPriceUsd: Number(nextCost) / 100 / Number(nextQty), avgCostMicros: (nextCost * 10000n / nextQty).toString(), lastMarkUsd: Number(px) / 1e6, lastMarkAt: bar ? bar.t : new Date(nowMs).toISOString(),
      openedAt: (pos && pos.open && pos.openedAt) || (bar ? bar.t : new Date(nowMs).toISOString()), positionLifecycleId: (pos && pos.open && pos.positionLifecycleId) || sha([accountId, symbol, orderSet.mandateVersionId, fillId].join("|")).slice(0, 32),
      engineVersion: ENGINE_VERSION, decisionAuthority: DECISION_AUTHORITY, mandateVersionId: orderSet.mandateVersionId, orderSetId: orderSet.orderSetId, schemaVersion: "position.v2",
      lossBoundaryPriceMicros: (pos && pos.lossBoundaryPriceMicros) || null, takeProfitPriceMicros: (pos && pos.takeProfitPriceMicros) || null, protectionState: (pos && pos.protectionState) || "PROTECTION_PENDING", updatedAtMs: nowMs, sector: (pos && pos.sector) || orderSet.sector || null };
  } else {
    if (!pos || !pos.open) throw typed("SELL_WITHOUT_POSITION", `${symbol} has no open position`);
    const prevQty = big(pos.quantityUnits || pos.qty || 0), prevCost = big(pos.costBasisMinor || pos.costBasisCents || 0);
    if (qty > prevQty) throw typed("OVERSELL", `${symbol}: selling ${qty} of ${prevQty}`);
    const costOut = M.divRound(prevCost * qty, prevQty, M.ROUNDING.HALF_EVEN);
    const proceeds = notionalMinor - fee;
    realizedMinor = proceeds - costOut;
    legsJournal = [{ account: ACCT.CASH, amountCents: Number(proceeds), memo: `${symbol} sell ${qty}@${px}` }, { account: ACCT.POSITIONS, amountCents: -Number(costOut), memo: `${symbol} cost relieved` }, { account: ACCT.FRICTION, amountCents: Number(fee), memo: "fees" }, { account: ACCT.REALIZED_PL, amountCents: -Number(realizedMinor), memo: `${symbol} realized` }];
    const nextQty = prevQty - qty, nextCost = prevCost - costOut;
    closed = nextQty === 0n;
    positionNext = { ...pos, quantityUnits: nextQty.toString(), qty: Number(nextQty), costBasisMinor: nextCost.toString(), costBasisCents: Number(nextCost), open: !closed, closedAt: closed ? (bar ? bar.t : new Date(nowMs).toISOString()) : null,
      lastMarkUsd: Number(px) / 1e6, lastMarkAt: bar ? bar.t : new Date(nowMs).toISOString(), realizedMinor: (big(pos.realizedMinor || 0) + realizedMinor).toString(), updatedAtMs: nowMs, engineVersion: ENGINE_VERSION };
  }
  const fillDoc = { schemaVersion: "fill.v2", fillId, accountId, symbol, orderSetId: orderSet.orderSetId, legId: leg.legId, role: leg.role, side: leg.side, mandateVersionId: orderSet.mandateVersionId,
    quantityUnits: qty.toString(), priceMicros: px.toString(), notionalMinor: notionalMinor.toString(), feeMinor: fee.toString(), basis: fill.basis || null, source, brokerFillId: fill.eventId || null,
    eventAtMs: bar ? Date.parse(bar.t) : nowMs, receivedAtMs: nowMs, bar: bar ? { t: bar.t, o: bar.o, h: bar.h, l: bar.l, c: bar.c, v: bar.v } : null, provenance: provenance || null,
    engineVersion: ENGINE_VERSION, decisionAuthority: DECISION_AUTHORITY, ambiguous: fill.ambiguous === true, ...D.envelope({ created_by: "execution.recordFill" }) };
  await fref.set(fillDoc);
  await postJournal({ admin: D, accountId, kind: "manager_fill", idParts: [fillId], legs: legsJournal, meta: { fillId, symbol, legId: leg.legId, orderSetId: orderSet.orderSetId, mandateVersionId: orderSet.mandateVersionId } });
  await D.col(D.COL.positions).doc(positionDocId(accountId, symbol)).set(positionNext, { merge: true });
  const filled = big(leg.filledUnits || 0) + qty, remaining = big(leg.remainingUnits != null ? leg.remainingUnits : leg.quantityUnits) - qty;
  await D.col(D.COL.orderLegs).doc(leg.legId).set({ filledUnits: filled.toString(), remainingUnits: remaining.toString(), status: remaining <= 0n ? "FILLED" : "PARTIALLY_FILLED", lastFillId: fillId, lastFillAtMs: nowMs }, { merge: true });
  if (closed) {
    const tradeId = `trade_${sha([accountId, symbol, pos.positionLifecycleId || fillId].join("|")).slice(0, 32)}`;
    await D.col(D.COL.trades).doc(tradeId).set({ schemaVersion: "trade.v2", tradeId, accountId, symbol, positionLifecycleId: pos.positionLifecycleId || null, mandateVersionId: orderSet.mandateVersionId,
      openedAt: pos.openedAt || null, closedAt: positionNext.closedAt, realizedMinor: positionNext.realizedMinor, exitRole: leg.role, exitBasis: fill.basis || null, ambiguous: fill.ambiguous === true,
      engineVersion: ENGINE_VERSION, decisionAuthority: DECISION_AUTHORITY, ...D.envelope({ created_by: "execution.recordFill" }) });
  }
  return { fillId, duplicate: false, closed, remainingUnits: remaining.toString(), realizedMinor: realizedMinor.toString(), positionQuantityUnits: positionNext.quantityUnits };
}

/* ── pointer / order-set helpers ───────────────────────────────────────── */
async function readOrderSet(D, orderSetId) {
  const s = await D.col(D.COL.orderSets).doc(orderSetId).get();
  if (!s.exists) return null;
  const SC = lazy("./_investorStorageCodec");
  const d = s.data();
  const os = d._codec && SC ? SC.decode(d) : d;
  const legs = rows(await D.col(D.COL.orderLegs).where("orderSetId", "==", orderSetId).get()).map((l) => (l._codec && SC ? SC.decode(l) : l));
  return { ...os, legs };
}
async function setPointer(D, accountId, symbol, fields) { await D.col(D.COL.activeMandates).doc(`${accountId}_${symbol}`).set({ ...fields, updatedAtMs: Date.now() }, { merge: true }); }
async function event(D, { accountId, symbol, mandateVersionId, kind, fields = {} }) {
  const pref = D.col(D.COL.activeMandates).doc(`${accountId}_${symbol}`);
  const p = await pref.get();
  const seq = (Number(p.exists ? p.data().eventSequence : 0) || 1) + 1;
  await D.col(D.COL.mandateEvents).doc(`${mandateVersionId}_${String(seq).padStart(4, "0")}`).set({ mandateVersionId, mandateSeriesId: `${accountId}_${symbol}`, sequence: seq, accountId, symbol, kind, atMs: Date.now(), ...fields });
  await pref.set({ eventSequence: seq }, { merge: true });
}
async function releaseReservation(D, { accountId, reservationId, reason }) {
  if (!reservationId) return { released: false };
  const rref = D.col(D.COL.capitalReservations).doc(reservationId), aref = D.col(D.COL.reservationAccounts).doc(accountId);
  return D.runTransaction(async (tx) => {
    const [r, a] = await Promise.all([tx.get(rref), tx.get(aref)]);
    if (!r.exists || r.data().status !== "ACTIVE") return { released: false, reason: "not_active" };
    const res = r.data(), acct = a.exists ? a.data() : {};
    const sub = (k, v) => { const n = big(acct[k] || 0) - big(v || 0); return (n < 0n ? 0n : n).toString(); };
    tx.set(rref, { status: "RELEASED", releasedAtMs: Date.now(), releaseReason: reason }, { merge: true });
    tx.set(aref, { reservedNotionalMinor: sub("reservedNotionalMinor", res.reservedNotionalMinor), reservedPlannedLossMinor: sub("reservedPlannedLossMinor", res.plannedLossMinor), reservedStressLossMinor: sub("reservedStressLossMinor", res.stressLossMinor), version: (Number(acct.version) || 0) + 1, updatedAtMs: Date.now() }, { merge: true });
    return { released: true };
  });
}

/* ── the outbox saga (§10.5) ───────────────────────────────────────────── */
async function claimTransition(D, t, worker) {
  const ref = D.col(D.COL.executionOutbox).doc(t.transitionId);
  return D.runTransaction(async (tx) => {
    const cur = await tx.get(ref);
    if (!cur.exists) return false;
    const d = cur.data();
    if (d.status !== "PENDING" && !(d.status === "CLAIMED" && Number(d.claimedAtMs) < Date.now() - 10 * 60000)) return false;
    tx.set(ref, { status: "CLAIMED", claimedBy: worker, claimedAtMs: Date.now(), attempts: (Number(d.attempts) || 0) + 1 }, { merge: true });
    return true;
  });
}
async function finishTransition(D, t, status, fields = {}) { await D.col(D.COL.executionOutbox).doc(t.transitionId).set({ status, finishedAtMs: Date.now(), ...fields }, { merge: true }); }
async function applyTransition({ admin = null, adapter, transition: t, control = {}, nowMs = Date.now(), worker = "executor" } = {}) {
  const D = db(admin);
  const B = lazy("./_investorBroker");
  if (!(await claimTransition(D, t, worker))) return { transitionId: t.transitionId, skipped: true };
  try {
    if (t.kind === "APPLY_DESIRED_ORDER_SET") {
      const os = await readOrderSet(D, t.orderSetId);
      if (!os) throw typed("ORDER_SET_MISSING", t.orderSetId);
      const caps = await adapter.getCapabilities();
      const v = B ? B.validateOrderSetAgainstCapabilities(os, caps) : { ok: true };
      if (!v.ok) { await setPointer(D, t.accountId, t.symbol, { status: "ACTION_REQUIRED", actionRequiredReason: `UNSUPPORTED:${v.problems.join(",")}` }); await finishTransition(D, t, "FAILED", { error: v.problems.join(",") }); return { transitionId: t.transitionId, applied: false, reason: v.problems.join(",") }; }
      await setPointer(D, t.accountId, t.symbol, { status: "BROKER_SYNC_PENDING" });
      const ack = await adapter.submitOrderSet(os, t.idempotencyKey);
      if (!ack.accepted) throw typed("BROKER_REJECTED", ack.reason || "rejected");
      await D.col(D.COL.orderSets).doc(os.orderSetId).set({ status: "WORKING", brokerGroupId: ack.brokerGroupId || null, appliedMandateVersionId: os.mandateVersionId, acknowledgedAtMs: nowMs }, { merge: true });
      const hasEntry = (os.legs || []).some((l) => l.role === "ENTRY");
      await setPointer(D, t.accountId, t.symbol, { status: hasEntry ? "WORKING" : "PROTECTED_RTH", appliedVersionId: os.mandateVersionId, appliedVersion: null, brokerGroupId: ack.brokerGroupId || null, protectionState: hasEntry ? null : "PROTECTED_RTH", protectionAcknowledged: !hasEntry });
      if (!hasEntry) await D.col(D.COL.positions).doc(positionDocId(t.accountId, t.symbol)).set({ protectionState: "PROTECTED_RTH", protectionAcknowledged: true, mandateVersionId: os.mandateVersionId,
        lossBoundaryPriceMicros: (os.legs.find((l) => l.role === "STOP") || {}).stopMicros || null, takeProfitPriceMicros: (os.legs.find((l) => l.role === "TARGET") || {}).priceMicros || null }, { merge: true });
      await event(D, { accountId: t.accountId, symbol: t.symbol, mandateVersionId: os.mandateVersionId, kind: "BROKER_ACKNOWLEDGED", fields: { brokerGroupId: ack.brokerGroupId || null, duplicate: ack.duplicate === true } });
      await finishTransition(D, t, "COMPLETE", { brokerGroupId: ack.brokerGroupId || null });
      return { transitionId: t.transitionId, applied: true, duplicate: ack.duplicate === true };
    }
    if (t.kind === "CANCEL_UNFILLED_ENTRY") {
      const os = await readOrderSet(D, t.orderSetId);
      const entry = os && os.legs.find((l) => l.role === "ENTRY");
      if (!entry) { await finishTransition(D, t, "COMPLETE", { note: "no entry leg" }); return { transitionId: t.transitionId, applied: true, noop: true }; }
      const res = await adapter.cancelOrderSet(os.orderSetId, t.reason || "cancel_entry", { legIds: [entry.legId] });
      const owned = await readPosition(D, t.accountId, t.symbol);
      const ownedQty = owned && owned.open ? big(owned.quantityUnits || owned.qty || 0) : 0n;
      if (res.terminal) {
        await releaseReservation(D, { accountId: t.accountId, reservationId: os.reservationId, reason: t.reason || "entry_cancelled" });
        /* protection on owned shares is never touched; an empty entry closes the pointer */
        await setPointer(D, t.accountId, t.symbol, ownedQty > 0n ? { entryState: "CANCELLED" } : { status: t.authority === "EVIDENCE_PAUSE" ? "PAUSED_EVIDENCE" : "CANCELLED", entryState: "CANCELLED" });
        await event(D, { accountId: t.accountId, symbol: t.symbol, mandateVersionId: os.mandateVersionId, kind: "ENTRY_CANCELLED", fields: { reason: t.reason || null, authority: t.authority || null } });
        await finishTransition(D, t, "COMPLETE");
        return { transitionId: t.transitionId, applied: true, terminal: true };
      }
      await setPointer(D, t.accountId, t.symbol, { status: "CANCEL_PENDING" });
      await finishTransition(D, t, "PENDING", { note: "awaiting terminal cancel", claimedBy: null });
      return { transitionId: t.transitionId, applied: false, pending: true };
    }
    if (t.authority === "EMERGENCY_RISK") {
      const side = t.kind === "BUY_TO_COVER_ACCIDENTAL_SHORT" ? "buy" : "sell";
      const orderSetId = `os_em_${sha(t.transitionId).slice(0, 24)}`;
      const leg = { legId: `${orderSetId}_${t.kind}`, role: side === "buy" ? "BUY_TO_COVER" : (t.kind === "FLATTEN" ? "SELL" : "REDUCE"), side, type: "MARKETABLE_LIMIT", collarBps: t.collarBps || "100", quantityUnits: t.quantityUnits, remainingUnits: t.quantityUnits, filledUnits: "0", timeInForce: "DAY", status: "DESIRED", orderSetId, accountId: t.accountId, symbol: t.symbol, authority: "EMERGENCY_RISK" };
      const os = { schemaVersion: "order-set.v1", orderSetId, accountId: t.accountId, symbol: t.symbol, mandateVersionId: `EMERGENCY_RISK:${t.triggerId || "policy"}`, purpose: "EMERGENCY_RISK", status: "DESIRED", legs: [leg], legIds: [leg.legId], authority: "EMERGENCY_RISK", createdAtMs: nowMs };
      await D.col(D.COL.orderSets).doc(orderSetId).set(os);
      await D.col(D.COL.orderLegs).doc(leg.legId).set(leg);
      const ack = await adapter.submitOrderSet(os, t.idempotencyKey);
      await D.col(D.COL.orderSets).doc(orderSetId).set({ status: "WORKING", brokerGroupId: ack.brokerGroupId || null }, { merge: true });
      await finishTransition(D, t, "COMPLETE", { orderSetId, brokerGroupId: ack.brokerGroupId || null });
      return { transitionId: t.transitionId, applied: true, emergency: true, orderSetId };
    }
    await finishTransition(D, t, "FAILED", { error: `unknown transition kind ${t.kind}` });
    return { transitionId: t.transitionId, applied: false, reason: "unknown_kind" };
  } catch (e) {
    await finishTransition(D, t, Number(t.attempts) >= 5 ? "DEAD" : "PENDING", { error: String(e.code || e.message).slice(0, 160), claimedBy: null, lastErrorAtMs: nowMs });
    return { transitionId: t.transitionId, applied: false, error: String(e.code || e.message).slice(0, 160) };
  }
}
async function applyOutbox({ admin = null, adapter, accountId, control = {}, nowMs = Date.now(), limit = 20 } = {}) {
  const D = db(admin);
  const pending = rows(await D.col(D.COL.executionOutbox).where("status", "==", "PENDING").get()).filter((t) => !accountId || t.accountId === accountId)
    .sort((a, b) => (a.authority === "EMERGENCY_RISK" ? -1 : 0) - (b.authority === "EMERGENCY_RISK" ? -1 : 0) || Number(a.createdAtMs) - Number(b.createdAtMs)).slice(0, limit);
  const results = [];
  for (const t of pending) results.push(await applyTransition({ admin: D, adapter, transition: t, control, nowMs }));
  return { applied: results.filter((r) => r.applied).length, results };
}

/* ── paper fills over stored bars ──────────────────────────────────────── */
async function simulatePaperFills({ admin = null, adapter, accountId, barsBySymbol = {}, nowMs = Date.now(), provenanceBySymbol = {} } = {}) {
  const D = db(admin);
  const legs = rows(await D.col(D.COL.orderLegs).where("accountId", "==", accountId).get()).filter((l) => ["WORKING", "ARMED", "PARTIALLY_FILLED"].includes(l.status));
  const bySet = new Map();
  for (const l of legs) bySet.set(l.orderSetId, [...(bySet.get(l.orderSetId) || []), l]);
  const out = { fills: [], protectionAttached: [], closed: [], ambiguous: 0, cancelledRemainders: [] };
  for (const [orderSetId, setLegs] of bySet) {
    const os = await readOrderSet(D, orderSetId);
    if (!os) continue;
    const symbol = os.symbol;
    const bars = (barsBySymbol[symbol] || []).filter((b) => b && b.t);
    for (const bar of bars) {
      const barMs = Date.parse(bar.t);
      const fresh = (l) => !(Number(l.lastBarMs) >= barMs) && (!l.workingSinceMs || Number(l.workingSinceMs) <= barMs + 60000);
      const live = (await readOrderSet(D, orderSetId)).legs.filter((l) => ["WORKING", "PARTIALLY_FILLED"].includes(l.status) && fresh(l));
      if (!live.length) continue;
      const pick = (role) => live.find((l) => l.role === role) || null;
      const entry = pick("ENTRY"), target = pick("TARGET"), stop = pick("STOP");
      const other = live.filter((l) => !["ENTRY", "TARGET", "STOP"].includes(l.role));
      /* a time exit arms after its trigger session: it works from the next eligible open for the owned quantity */
      const nyDate = (() => { try { return require("./_investorMarket").nyParts(new Date(bar.t)).date; } catch { return bar.t.slice(0, 10); } })();
      for (const te of (await readOrderSet(D, orderSetId)).legs.filter((l) => l.activatesOn === "TIME_EXIT" && l.status === "ARMED" && l.triggerAfterSessionDate && nyDate > l.triggerAfterSessionDate)) {
        const owned = await readPosition(D, accountId, symbol);
        const q = owned && owned.open ? big(owned.quantityUnits || owned.qty || 0) : 0n;
        if (q > 0n) await D.col(D.COL.orderLegs).doc(te.legId).set({ status: "WORKING", quantityUnits: q.toString(), remainingUnits: q.toString(), workingSinceMs: barMs, activatedAtMs: nowMs }, { merge: true });
      }
      const r = resolveBarCollisions({ entry: entry ? simulateLegOnBar({ leg: entry, bar }) : null, target: target ? simulateLegOnBar({ leg: target, bar }) : null, stop: stop ? simulateLegOnBar({ leg: stop, bar }) : null });
      if (r.ambiguous) out.ambiguous += 1;
      const apply = async (leg, sim) => {
        if (!sim || !sim.fill) return null;
        const rec = await recordFill({ admin: D, accountId, orderSet: os, leg, fill: { ...sim.fill, ambiguous: r.ambiguous }, bar, provenance: provenanceBySymbol[symbol] || null, nowMs });
        if (!rec.duplicate) out.fills.push({ symbol, legId: leg.legId, role: leg.role, ...sim.fill, closed: rec.closed });
        return rec;
      };
      if (entry) {
        const rec = await apply(entry, r.entry);
        if (rec && !rec.duplicate) {
          const remaining = big(rec.remainingUnits);
          if (remaining > 0n) {
            /* first partial fill: cancel the remainder, await the terminal result, then protect exactly the owned quantity */
            const cancel = await adapter.cancelOrderSet(orderSetId, "partial_fill_remainder", { legIds: [entry.legId] });
            out.cancelledRemainders.push({ symbol, remainingUnits: remaining.toString(), terminal: cancel.terminal === true });
            if (cancel.terminal) await releaseReservation(D, { accountId, reservationId: os.reservationId, reason: "partial_fill_remainder_cancelled" });
            await setPointer(D, accountId, symbol, { status: "PROTECTION_PENDING", entryState: "PARTIALLY_FILLED_REMAINDER_CANCELLED" });
          } else { await releaseReservation(D, { accountId, reservationId: os.reservationId, reason: "entry_filled" }); await setPointer(D, accountId, symbol, { status: "PROTECTION_PENDING", entryState: "FILLED" }); }
          const owned = await readPosition(D, accountId, symbol);
          const ownedQty = owned ? big(owned.quantityUnits || owned.qty || 0) : 0n;
          const attached = [];
          for (const p of (await readOrderSet(D, orderSetId)).legs.filter((l) => ["TARGET", "STOP"].includes(l.role) && l.status === "ARMED")) {
            await D.col(D.COL.orderLegs).doc(p.legId).set({ status: "WORKING", quantityUnits: ownedQty.toString(), remainingUnits: ownedQty.toString(), workingSinceMs: barMs + 1, activatedAtMs: nowMs }, { merge: true });
            attached.push(p.role);
          }
          /* adverse same-bar convention: the entry bar's low at or below the boundary fills the stop on this bar, marked ambiguous */
          const stopLeg = (await readOrderSet(D, orderSetId)).legs.find((l) => l.role === "STOP" && l.status === "WORKING");
          if (stopLeg && ownedQty > 0n && micros(bar.l) != null && micros(bar.l) <= big(stopLeg.stopMicros)) {
            const s = simulateLegOnBar({ leg: { ...stopLeg, remainingUnits: ownedQty.toString() }, bar });
            if (s.fill) {
              out.ambiguous += 1;
              const rec = await recordFill({ admin: D, accountId, orderSet: os, leg: stopLeg, fill: { ...s.fill, ambiguous: true }, bar, provenance: provenanceBySymbol[symbol] || null, nowMs });
              if (!rec.duplicate) { out.fills.push({ symbol, legId: stopLeg.legId, role: "STOP", ...s.fill, closed: rec.closed, ambiguous: true }); const tgt = (await readOrderSet(D, orderSetId)).legs.find((l) => l.role === "TARGET"); if (tgt) await adapter.cancelOrderSet(orderSetId, "oco_sibling_filled", { legIds: [tgt.legId] }); if (rec.closed) { await setPointer(D, accountId, symbol, { status: "CLOSED", closedReason: "STOP", closedAtMs: nowMs, ambiguous: true }); out.closed.push({ symbol, role: "STOP", realizedMinor: rec.realizedMinor, ambiguous: true }); } }
              continue;
            }
          }
          if (attached.length === 2) {
            await D.col(D.COL.positions).doc(positionDocId(accountId, symbol)).set({ protectionState: "PROTECTED_RTH", protectionAcknowledged: true, lossBoundaryPriceMicros: (stop || (await readOrderSet(D, orderSetId)).legs.find((l) => l.role === "STOP") || {}).stopMicros || null, takeProfitPriceMicros: (target || (await readOrderSet(D, orderSetId)).legs.find((l) => l.role === "TARGET") || {}).priceMicros || null, mandateVersionId: os.mandateVersionId }, { merge: true });
            await setPointer(D, accountId, symbol, { status: "PROTECTED_RTH", protectionState: "PROTECTED_RTH", protectionAcknowledged: true, protectedQuantityUnits: ownedQty.toString() });
            out.protectionAttached.push({ symbol, quantityUnits: ownedQty.toString() });
            await event(D, { accountId, symbol, mandateVersionId: os.mandateVersionId, kind: "PROTECTION_ACKNOWLEDGED", fields: { quantityUnits: ownedQty.toString(), label: "PROTECTED_RTH" } });
          }
        }
      }
      for (const [leg, sim] of [[target, r.target], [stop, r.stop]]) {
        if (!leg || !sim || !sim.fill) continue;
        const rec = await apply(leg, sim);
        if (rec && !rec.duplicate) {
          /* OCO: the sibling covers the same owned quantity; a fill on one cancels the other */
          const sibling = leg.role === "STOP" ? target : stop;
          const siblingId = sibling ? sibling.legId : ((await readOrderSet(D, orderSetId)).legs.find((l) => l.role === (leg.role === "STOP" ? "TARGET" : "STOP")) || {}).legId;
          if (siblingId) await adapter.cancelOrderSet(orderSetId, "oco_sibling_filled", { legIds: [siblingId] });
          if (rec.closed) { await setPointer(D, accountId, symbol, { status: "CLOSED", closedReason: leg.role, closedAtMs: nowMs }); await D.col(D.COL.orderSets).doc(orderSetId).set({ status: "CLOSED" }, { merge: true }); await event(D, { accountId, symbol, mandateVersionId: os.mandateVersionId, kind: "POSITION_CLOSED", fields: { role: leg.role, realizedMinor: rec.realizedMinor, ambiguous: r.ambiguous } }); out.closed.push({ symbol, role: leg.role, realizedMinor: rec.realizedMinor }); }
        }
      }
      for (const leg of other) {
        const sim = simulateLegOnBar({ leg, bar });
        const rec = await apply(leg, sim);
        if (rec && !rec.duplicate && rec.closed) { await setPointer(D, accountId, symbol, { status: "CLOSED", closedReason: leg.role, closedAtMs: nowMs }); out.closed.push({ symbol, role: leg.role, realizedMinor: rec.realizedMinor }); }
      }
      for (const l of live) await D.col(D.COL.orderLegs).doc(l.legId).set({ lastBarMs: barMs }, { merge: true });
    }
  }
  return out;
}

/* ── the per-tick loop (§8.6) ──────────────────────────────────────────── */
async function tick({ admin = null, adapter, accountId, control = {}, barsBySymbol = {}, nowMs = Date.now(), metrics = {}, ranks = null } = {}) {
  const D = db(admin);
  const R = lazy("./_investorRisk"), ER = lazy("./_investorEmergencyRisk"), MD = lazy("./_investorMandate"), DOSSIER = lazy("./_investorDossier"), P = lazy("./_investorPortfolio");
  const summary = { accountId, nowMs, expired: [], paused: [], operational: null, emergency: null, outbox: null, fills: null, conservation: null };
  const pointers = rows(await D.col(D.COL.activeMandates).where("accountId", "==", accountId).get());
  for (const p of pointers) {
    /* an unfilled entry past its last authorized session is cancelled; protection is untouched */
    if (p.decision === "BUY" && ["DESIRED", "WORKING", "BROKER_SYNC_PENDING", "PAUSED_EVIDENCE", "PAUSED_OPERATIONAL"].includes(p.status) && Number(p.expiresAtMs) && nowMs > Number(p.expiresAtMs)) {
      const id = `${p.desiredVersionId}:EXPIRE_ENTRY`;
      await D.col(D.COL.executionOutbox).doc(id).set({ transitionId: id, accountId, symbol: p.symbol, mandateVersionId: p.desiredVersionId, orderSetId: `os_${p.desiredVersionId}`, kind: "CANCEL_UNFILLED_ENTRY", reason: "entry_authorization_expired", status: "PENDING", idempotencyKey: id, attempts: 0, createdAtMs: nowMs, authority: "EXECUTOR_EXPIRY" }, { merge: true });
      await setPointer(D, accountId, p.symbol, { status: "ENTRY_EXPIRED" });
      summary.expired.push(p.symbol);
      continue;
    }
    /* a high-impact delta not yet reviewed pauses an unfilled entry (Appendix B, §8.6) */
    if (p.decision === "BUY" && ["WORKING", "DESIRED", "BROKER_SYNC_PENDING"].includes(p.status) && DOSSIER && MD) {
      const pend = await DOSSIER.pendingChanges(p.symbol, { cutoffMs: nowMs, admin: D }).catch(() => []);
      const hi = pend.find((d) => d.safetyClass === "high_impact");
      if (hi) { const r = await MD.pauseUnfilledEntry(p.symbol, hi.deltaId || hi.eventId, { accountId, admin: D }); if (r.paused) summary.paused.push({ symbol: p.symbol, deltaId: hi.deltaId }); }
    }
  }
  /* operational revalidation: safety and exposure, never company attractiveness */
  const portfolio = P ? await P.snapshot({ accountId, asOfMs: nowMs, admin: D }) : null;
  if (R && portfolio) {
    summary.operational = R.revalidateOperationalLimits({ portfolio, control, riskMandate: POLICY.loadActiveSync(control).riskMandate, brokerTruthAgeSeconds: metrics.brokerTruthAgeSeconds, reconciliationUnresolved: metrics.reconciliationUnresolved === true, dayLossBps: metrics.dayLossBps, drawdownFromPeakBps: metrics.drawdownFromPeakBps, nowMs });
    if (!summary.operational.allowExpansion && control.buyState !== "FROZEN") await D.col(D.COL.control).doc("control").set({ buyState: "FROZEN", freezeNewBuys: true, freezeReason: summary.operational.reason, frozenAtMs: nowMs }, { merge: true });
    if (summary.operational.hardBreach && ER) summary.emergency = await ER.enforceBoundedPolicy({ accountId, observed: { ...metrics, grossExposureBps: summary.operational.exposures && summary.operational.exposures.grossExposureBps, aggregateStressedLossBps: summary.operational.exposures && summary.operational.exposures.stressedLossAggregateBps }, portfolio, control, ranks, admin: D, nowMs });
  }
  summary.outbox = await applyOutbox({ admin: D, adapter, accountId, control, nowMs });
  if (adapter.adapter === "paper") summary.fills = await simulatePaperFills({ admin: D, adapter, accountId, barsBySymbol, nowMs });
  summary.conservation = await assertConservation(accountId, { admin: D });
  if (!summary.conservation.pass) { await D.col(D.COL.control).doc("control").set({ executorState: "PAUSED_SAFETY", executorPauseReason: "LEDGER_CONSERVATION_FAILED", executorPausedAtMs: nowMs }, { merge: true }); }
  return summary;
}

module.exports = { ENGINE_VERSION, DECISION_AUTHORITY, DEFAULT_PARTICIPATION_BPS, STOP_SLIPPAGE_BPS, PROTECTION_SLA_SECONDS, ACCT,
  simulateLegOnBar, resolveBarCollisions, postJournal, assertConservation, recordFill, readOrderSet, releaseReservation,
  applyTransition, applyOutbox, simulatePaperFills, tick, positionDocId };
