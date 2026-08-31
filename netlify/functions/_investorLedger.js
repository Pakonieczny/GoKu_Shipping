/* Investor_AI — atomic, fixed-point paper ledger. No broker is connected. */
"use strict";

const crypto = require("crypto");
const A = require("./_investorAdmin");
const M = require("./_investorMarket");

const CENTS = 100;
const PRICE_SCALE = 1e6;
const toCents = (usd) => Math.round(Number(usd) * CENTS);
const fromCents = (c) => Number(c) / CENTS;
const toPrice = (usd) => Math.round(Number(usd) * PRICE_SCALE);
const fromPrice = (p) => Number(p) / PRICE_SCALE;
const notionalCents = (qty, priceInt) =>
  Math.round((Number(qty) * Number(priceInt)) / (PRICE_SCALE / CENTS));

const ACCT = {
  CASH: "cash", POSITIONS: "positions", RESERVED: "reserved",
  REALIZED_PL: "realized_pl", FRICTION: "friction", CONTRIB: "contributed_capital",
};

function txnId(parts) {
  return crypto.createHash("sha256").update(parts.map(String).join("|")).digest("hex").slice(0, 32);
}
function assertBalanced(legs, ref = "transaction") {
  let sum = 0;
  for (const l of legs || []) {
    if (!Number.isInteger(l.amountCents)) throw new Error(`${ref}: non-integer ${l.account} leg`);
    sum += l.amountCents;
  }
  if (sum !== 0) throw new Error(`${ref}: ledger legs do not balance (${sum} cents)`);
  return true;
}
function applyLegs(base, legs) {
  const out = { ...(base || {}) };
  for (const l of legs) out[l.account] = (Number(out[l.account]) || 0) + l.amountCents;
  return out;
}
function projectionUpdate(account, legs) {
  return { balanceCents: applyLegs(account && account.balanceCents, legs),
    balanceRevision: (Number(account && account.balanceRevision) || 0) + 1,
    balanceUpdatedAt: A.FV.serverTimestamp() };
}
function ledgerRecord({ id, accountId, kind, legs, meta = {}, supersedes = null }) {
  assertBalanced(legs, id);
  return { txnId: id, accountId, kind, legs, meta, supersedes,
    postedAt: A.FV.serverTimestamp(), ...A.envelope({ created_by: "ledger" }) };
}
function accountRef(accountId) { return A.col(A.COL.accounts).doc(accountId); }
function orderLockRef(accountId, symbol) {
  return A.col(A.COL.control).doc(`orderlock_${txnId([accountId, symbol])}`);
}

async function openAccount({ accountId, startingNavUsd, baseCurrency = "USD", strategyVersion }) {
  const aref = accountRef(accountId);
  const lid = txnId([accountId, "capital_contribution", "genesis"]);
  const lref = A.col(A.COL.ledger).doc(lid);
  const startCents = toCents(startingNavUsd);
  const legs = [
    { account: ACCT.CASH, amountCents: startCents, memo: "opening virtual balance" },
    { account: ACCT.CONTRIB, amountCents: -startCents, memo: "contributed capital" },
  ];
  return A.runTransaction(async (tx) => {
    const [a, l] = await Promise.all([tx.get(aref), tx.get(lref)]);
    if (a.exists) return { accountId, existing: true, ...a.data() };
    if (l.exists) throw new Error(`openAccount ${accountId}: orphan genesis transaction`);
    const account = { accountId, baseCurrency, startingNavCents: startCents,
      strategyVersion, mode: "research", balanceCents: applyLegs({}, legs), balanceRevision: 1,
      openedAt: A.FV.serverTimestamp(), ...A.envelope({ created_by: "ledger.openAccount" }) };
    tx.set(aref, account);
    tx.set(lref, ledgerRecord({ id: lid, accountId, kind: "capital_contribution", legs,
      meta: { note: "virtual money only; no broker is connected" } }));
    return { accountId, existing: false, startingNavCents: startCents };
  });
}

async function post({ accountId, kind, idParts, legs, meta = {}, supersedes = null }) {
  const id = txnId([accountId, kind, ...idParts]);
  assertBalanced(legs, id);
  const lref = A.col(A.COL.ledger).doc(id), aref = accountRef(accountId);
  return A.runTransaction(async (tx) => {
    const [l, a] = await Promise.all([tx.get(lref), tx.get(aref)]);
    if (l.exists) return { id, duplicate: true };
    if (!a.exists) throw new Error(`post ${id}: account ${accountId} missing`);
    tx.set(lref, ledgerRecord({ id, accountId, kind, legs, meta, supersedes }));
    tx.set(aref, projectionUpdate(a.data(), legs), { merge: true });
    return { id, duplicate: false };
  });
}

async function rebuildBalances(accountId, { persist = true } = {}) {
  const snap = await A.col(A.COL.ledger).where("accountId", "==", accountId).get();
  let cents = {}, txns = 0, invalid = 0;
  snap.forEach((d) => {
    const legs = d.data().legs || [];
    try { assertBalanced(legs, d.id); } catch { invalid += 1; }
    cents = applyLegs(cents, legs); txns += 1;
  });
  if (persist) await accountRef(accountId).set({ balanceCents: cents,
    balanceRevision: txns, balanceRebuiltAt: A.FV.serverTimestamp() }, { merge: true });
  return { accountId, txns, invalid, cents,
    usd: Object.fromEntries(Object.entries(cents).map(([k, v]) => [k, fromCents(v)])) };
}
async function balances(accountId) {
  const a = await accountRef(accountId).get();
  if (!a.exists) throw new Error(`balances: account ${accountId} not found`);
  const cents = a.data().balanceCents;
  if (!cents) return rebuildBalances(accountId);
  return { accountId, txns: Number(a.data().balanceRevision) || null, cents,
    usd: Object.fromEntries(Object.entries(cents).map(([k, v]) => [k, fromCents(v)])) };
}
async function auditLedger(accountId) {
  const [rebuilt, a] = await Promise.all([rebuildBalances(accountId, { persist: false }), accountRef(accountId).get()]);
  const projected = (a.exists && a.data().balanceCents) || {};
  const keys = [...new Set([...Object.keys(rebuilt.cents), ...Object.keys(projected)])];
  const discrepancies = keys.filter((k) => (rebuilt.cents[k] || 0) !== (projected[k] || 0))
    .map((k) => ({ account: k, rebuiltCents: rebuilt.cents[k] || 0, projectedCents: projected[k] || 0 }));
  return { accountId, journalTransactions: rebuilt.txns, invalidTransactions: rebuilt.invalid,
    discrepancies, pass: rebuilt.invalid === 0 && discrepancies.length === 0 };
}

function timeMs(value) { return typeof value === "number" ? value : Date.parse(value); }
function executionClockCheck({ decisionAtMs, eligibleAfterMs, barOpenAt }) {
  const decision = Number(decisionAtMs), eligible = Number(eligibleAfterMs), barAt = timeMs(barOpenAt);
  if (!Number.isFinite(decision)) return { pass: false, reason: "missing_decision_time" };
  if (!Number.isFinite(eligible) || eligible < decision) return { pass: false, reason: "invalid_eligibility_time" };
  if (!Number.isFinite(barAt)) return { pass: false, reason: "invalid_bar_time" };
  if (!(barAt > decision && barAt >= eligible)) return { pass: false, reason: "ineligible_fill_clock" };
  return { pass: true, decisionAtMs: decision, eligibleAfterMs: eligible, barOpenAtMs: barAt };
}
function assertExecutionClock(v, context = "execution") {
  const c = executionClockCheck(v); if (!c.pass) throw new Error(`${context}: ${c.reason}`); return c;
}
function assertBar(bar, provenance, context) {
  if (!bar || !(Number(bar.o) > 0)) throw new Error(`${context}: invalid executable bar open`);
  if (!provenance || !provenance.provider || provenance.barOpenAt !== bar.t) {
    throw new Error(`${context}: missing or inconsistent bar provenance`);
  }
  if (!/^[a-f0-9]{64}$/.test(String(provenance.sourceSha256 || ""))) {
    throw new Error(`${context}: source response hash is required`);
  }
}

function controlAllowsEntry(control, identity) {
  const c = control || {}, e = c.safetyEpoch || {};
  const commit = process.env.COMMIT_REF || process.env.DEPLOY_ID || "local";
  const fail = (reason) => ({ pass: false, reason });
  if (c.enabled === false || c.killSwitch || c.entriesFrozen === true
      || c.dryRun !== false || (c.mode || "research") === "research") {
    return fail(c.entriesFrozen === true ? "new entries frozen by operator" : "entry controls closed");
  }
  if (c.fixturesPass !== true || c.fixturesCommit !== commit) return fail("current build fixtures not attested");
  for (const k of ["accountId","strategyVersion","universeVersion","strategyHash","universeHash","variantsHash"]) {
    if (!identity[k] || e[k] !== identity[k]) return fail(`safety epoch mismatch: ${k}`);
  }
  if (e.commit !== commit) return fail("safety epoch commit mismatch");
  return { pass: true };
}

async function proposeOrder(input) {
  const { accountId, symbol, side, decisionId, strategyVersion, universeVersion,
    universeHash, strategyHash, variantsHash, qty, refPriceUsd, slippageBps,
    sizing, gates, cause, evidenceRefs = [], decisionAtMs, quality,
    variantId = "baseline", cost = null, portfolioRisk = null, decisionContext = null,
    decisionMarketProvenance = null, executionCostContext = null,
    executionLatencyMs = 60000 } = input;
  if (![universeHash, strategyHash, variantsHash].every((h) => /^[a-f0-9]{64}$/.test(String(h || "")))) {
    throw new Error("proposeOrder: complete policy hashes are required");
  }
  if (!decisionMarketProvenance || !decisionMarketProvenance.provider
      || !/^[a-f0-9]{64}$/.test(String(decisionMarketProvenance.sourceSha256 || ""))) {
    throw new Error("proposeOrder: decision market provenance is required");
  }
  const orderId = txnId([accountId, symbol, decisionId]);
  const ref = A.col(A.COL.orders).doc(orderId), lockRef = orderLockRef(accountId, symbol);
  const controlRef = A.col(A.COL.control).doc("control");
  const priceInt = toPrice(refPriceUsd), grossCents = notionalCents(qty, priceInt);
  const frictionCents = Math.max(1, Math.round(grossCents * Number(slippageBps) / 10000));
  return A.runTransaction(async (tx) => {
    const [cur, lock, controlSnap] = await Promise.all([tx.get(ref), tx.get(lockRef), tx.get(controlRef)]);
    if (cur.exists) return { orderId, duplicate: true, status: cur.data().status };
    const permission = controlAllowsEntry(controlSnap.exists ? controlSnap.data() : {}, {
      accountId, strategyVersion, universeVersion, universeHash, strategyHash, variantsHash,
    });
    if (!permission.pass) return { orderId, blocked: permission.reason, status: "not_proposed" };
    if (lock.exists) return { orderId, blocked: "an active order already owns this symbol", status: "not_proposed" };
    const row = { orderId, accountId, symbol, side, decisionId, strategyVersion, universeVersion,
      universeHash, strategyHash, variantsHash, qty, refPriceInt: priceInt,
      refPriceUsd: fromPrice(priceInt), grossCents, frictionCents, slippageBps,
      sizing, gates, cause, evidenceRefs, variantId, portfolioRisk, decisionContext,
      decisionMarketProvenance, executionCostContext,
      cost: cost ? { ratio: cost.ratio, expectedGrossBps: cost.expectedGrossBps,
        halfTripBps: cost.halfTripBps, roundTripBps: cost.roundTripBps,
        requiredBps: cost.requiredBps, calibratedNetLowerBoundBps: cost.calibratedNetLowerBoundBps } : null,
      quality, status: "proposed", decisionAtMs: Number(decisionAtMs),
      executionLatencyMs: Math.max(0, Number(executionLatencyMs) || 60000),
      order_committed_at: A.FV.serverTimestamp(), ...A.envelope({ created_by: "ledger.proposeOrder" }) };
    tx.set(ref, row);
    tx.set(lockRef, { accountId, symbol, orderId, status: "proposed", createdAtMs: Date.now() });
    return { orderId, grossCents, frictionCents, qty, refPriceUsd: fromPrice(priceInt), status: "proposed" };
  });
}

async function approveOrder(orderId, operator) {
  const ref = A.col(A.COL.orders).doc(orderId);
  const controlRef = A.col(A.COL.control).doc("control");
  return A.runTransaction(async (tx) => {
    const first = await tx.get(ref); if (!first.exists) throw new Error(`approveOrder: ${orderId} not found`);
    const o = first.data(), aref = accountRef(o.accountId), lid = txnId([o.accountId,"reserve",orderId,"reserve"]);
    const lref = A.col(A.COL.ledger).doc(lid), [a, l, controlSnap] = await Promise.all([
      tx.get(aref), tx.get(lref), tx.get(controlRef),
    ]);
    if (o.status !== "proposed") return { orderId, status: o.status, noop: true };
    const permission = controlAllowsEntry(controlSnap.exists ? controlSnap.data() : {}, {
      accountId: o.accountId, strategyVersion: o.strategyVersion, universeVersion: o.universeVersion,
      universeHash: o.universeHash, strategyHash: o.strategyHash, variantsHash: o.variantsHash,
    });
    if (!permission.pass) {
      return { orderId, status: "proposed", noop: true,
        refused: `entry controls changed before approval: ${permission.reason}` };
    }
    const amount = o.grossCents + o.frictionCents;
    if (!a.exists || ((a.data().balanceCents || {})[ACCT.CASH] || 0) < amount) {
      return { orderId, status: "proposed", noop: true, refused: "insufficient free cash" };
    }
    const approvedAtMs = Date.now(), feedCfg = M.providerConfig(
      o.decisionMarketProvenance && o.decisionMarketProvenance.provider,
      o.decisionMarketProvenance && o.decisionMarketProvenance.feed), legs = [
      { account: ACCT.CASH, amountCents: -amount, memo: `reserve ${o.symbol}` },
      { account: ACCT.RESERVED, amountCents: amount, memo: `reserved for ${orderId}` },
    ];
    if (l.exists) throw new Error(`approveOrder ${orderId}: reservation exists before state transition`);
    tx.set(lref, ledgerRecord({ id: lid, accountId: o.accountId, kind: "reserve", legs,
      meta: { orderId, symbol: o.symbol } }));
    tx.set(aref, projectionUpdate(a.data(), legs), { merge: true });
    tx.set(ref, { status: "approved", approvedBy: operator || "operator", approvedAtMs,
      fillEligibleAfterMs: approvedAtMs + feedCfg.delayMinutes * 60000
        + (Number(o.executionLatencyMs) || 60000),
      operator_approved_at: A.FV.serverTimestamp() }, { merge: true });
    tx.set(orderLockRef(o.accountId, o.symbol), { status: "approved", orderId }, { merge: true });
    return { orderId, status: "approved", approvedAtMs };
  });
}

async function releaseOrder(orderId, reason) {
  const ref = A.col(A.COL.orders).doc(orderId);
  return A.runTransaction(async (tx) => {
    const s = await tx.get(ref); if (!s.exists) return { orderId, noop: true, reason: "not found" };
    const o = s.data(); if (o.status !== "approved") return { orderId, noop: true, status: o.status };
    const amount = o.grossCents + o.frictionCents, aref = accountRef(o.accountId);
    const lid = txnId([o.accountId,"release",orderId,"release"]), lref = A.col(A.COL.ledger).doc(lid);
    const lr = orderLockRef(o.accountId,o.symbol), [a, l, lock] = await Promise.all([tx.get(aref), tx.get(lref), tx.get(lr)]);
    if (l.exists) throw new Error(`releaseOrder ${orderId}: journal/state mismatch`);
    const legs = [
      { account: ACCT.RESERVED, amountCents: -amount, memo: `release ${orderId}` },
      { account: ACCT.CASH, amountCents: amount, memo: `returned ${o.symbol}` },
    ];
    tx.set(lref, ledgerRecord({ id: lid, accountId: o.accountId, kind: "release", legs,
      meta: { orderId, symbol: o.symbol, reason: reason || "released" } }));
    tx.set(aref, projectionUpdate(a.data(), legs), { merge: true });
    tx.set(ref, { status: "expired", expireReason: reason || "released", expired_at: A.FV.serverTimestamp() }, { merge: true });
    if (lock.exists && lock.data().orderId === orderId) tx.delete(lr);
    return { orderId, status: "expired", releasedCents: amount };
  });
}

async function rejectOrder(orderId, reason, operator) {
  const ref = A.col(A.COL.orders).doc(orderId), first = await ref.get();
  if (!first.exists) return { orderId, noop: true, reason: "not found" };
  if (first.data().status === "approved") return releaseOrder(orderId, reason || "operator rejected");
  return A.runTransaction(async (tx) => {
    const s = await tx.get(ref); if (!s.exists) return { orderId, noop: true };
    const o = s.data(), lr = orderLockRef(o.accountId,o.symbol), lock = await tx.get(lr);
    if (o.status !== "proposed") return { orderId, status: o.status, noop: true };
    tx.set(ref, { status: "rejected", rejectReason: reason || "operator rejected",
      rejectedBy: operator || "operator", rejected_at: A.FV.serverTimestamp() }, { merge: true });
    if (lock.exists && lock.data().orderId === orderId) tx.delete(lr);
    return { orderId, status: "rejected" };
  });
}

function fillAmounts(qtyRequested, openInt, fillInt, reservedCents) {
  let qty = Math.max(0, Math.floor(Number(qtyRequested) || 0));
  let baseCents = 0, fillCents = 0, frictionCents = 0;
  while (qty > 0) {
    baseCents = notionalCents(qty, openInt); fillCents = notionalCents(qty, fillInt);
    frictionCents = Math.abs(fillCents - baseCents);
    if (baseCents + frictionCents <= reservedCents) break;
    qty -= 1;
  }
  return { qty, baseCents, fillCents, frictionCents, totalCents: baseCents + frictionCents };
}

async function recordFill({ orderId, bar, barProvenance }) {
  assertBar(bar, barProvenance, "recordFill");
  const oref = A.col(A.COL.orders).doc(orderId);
  return A.runTransaction(async (tx) => {
    const os = await tx.get(oref); if (!os.exists) throw new Error(`recordFill: order ${orderId} not found`);
    const o = os.data();
    if (o.status === "filled") return { orderId, duplicate: true, status: "filled" };
    if (["cancelled", "expired", "rejected"].includes(o.status)) {
      return { orderId, noop: true, status: o.status };
    }
    if (o.status !== "approved") throw new Error(`recordFill: order ${orderId} is ${o.status}, not approved`);
    const clock = assertExecutionClock({ decisionAtMs: Math.max(Number(o.decisionAtMs)||0, Number(o.approvedAtMs)||0),
      eligibleAfterMs: o.fillEligibleAfterMs, barOpenAt: bar.t }, `recordFill ${orderId}`);
    const openInt = toPrice(bar.o), adverse = o.side === "buy" ? 1 : -1;
    const fillInt = openInt + adverse * Math.round(openInt * o.slippageBps / 10000);
    const reservedCents = o.grossCents + o.frictionCents;
    const amounts = fillAmounts(o.qty, openInt, fillInt, reservedCents);
    const fillId = txnId([orderId,"fill",bar.t]), lid = txnId([o.accountId,"fill",orderId,"fill",bar.t]);
    const releaseId = txnId([o.accountId,"release",orderId,"fill_abort"]);
    const aref = accountRef(o.accountId), pref = A.col(A.COL.positions).doc(`${o.accountId}_${o.symbol}`);
    const fref = A.col(A.COL.fills).doc(fillId), lref = A.col(A.COL.ledger).doc(lid);
    const releaseRef = A.col(A.COL.ledger).doc(releaseId);
    const lockRef = orderLockRef(o.accountId,o.symbol), controlRef = A.col(A.COL.control).doc("control");
    const [a,p,f,l,releaseJournal,lock,control] = await Promise.all([
      tx.get(aref),tx.get(pref),tx.get(fref),tx.get(lref),tx.get(releaseRef),tx.get(lockRef),tx.get(controlRef)
    ]);
    const gate = controlAllowsEntry(control.exists ? control.data() : {}, o);
    if (f.exists || l.exists) return { orderId, duplicate: true, status: "filled" };

    const abortReason = !gate.pass
      ? `entry safety closed before fill: ${gate.reason}`
      : ((p.exists && p.data().open) ? "position already open at fill time"
        : (amounts.qty < 1 ? "unfillable without exceeding reservation" : null));
    if (abortReason) {
      if (releaseJournal.exists) return { orderId, duplicate: true, status: "expired", released: true };
      const releaseLegs = [
        { account: ACCT.RESERVED, amountCents: -reservedCents, memo: `release ${orderId}` },
        { account: ACCT.CASH, amountCents: reservedCents, memo: `returned ${o.symbol}` },
      ];
      tx.set(releaseRef, ledgerRecord({ id: releaseId, accountId:o.accountId, kind:"release", legs:releaseLegs,
        meta:{ orderId, symbol:o.symbol, reason:abortReason, phase:"fill_abort" } }));
      tx.set(aref, projectionUpdate(a.data(), releaseLegs), { merge:true });
      tx.set(oref, { status:"expired", expireReason:abortReason, expired_at:A.FV.serverTimestamp() }, { merge:true });
      if (lock.exists && lock.data().orderId===orderId) tx.delete(lockRef);
      return { orderId, status:"expired", released:true, releasedCents:reservedCents, reason:abortReason };
    }
    const positionLifecycleId = txnId([o.accountId, o.symbol, orderId, "position"]);
    const legs = [
      { account: ACCT.RESERVED, amountCents: -reservedCents, memo: "release reservation" },
      { account: ACCT.CASH, amountCents: reservedCents - amounts.totalCents, memo: "unspent reservation" },
      { account: ACCT.POSITIONS, amountCents: amounts.baseCents, memo: `${o.symbol} executable open` },
      { account: ACCT.FRICTION, amountCents: amounts.frictionCents, memo: "entry implementation shortfall" },
    ];
    tx.set(fref, { fillId, orderId, accountId:o.accountId, symbol:o.symbol, side:o.side,
      qty:amounts.qty, requestedQty:o.qty, trimmedQty:o.qty-amounts.qty,
      fillPriceInt:fillInt, fillPriceUsd:fromPrice(fillInt), barOpenUsd:bar.o, barOpenAt:bar.t,
      orderDecisionAtMs:Number(o.decisionAtMs), approvedAtMs:Number(o.approvedAtMs),
      executionDecisionAtMs:clock.decisionAtMs, eligibleAfterMs:clock.eligibleAfterMs,
      grossCents:amounts.baseCents, fillNotionalCents:amounts.fillCents,
      frictionCents:amounts.frictionCents, barProvenance, positionLifecycleId,
      fill_confirmed_at:A.FV.serverTimestamp(), ...A.envelope({created_by:"ledger.recordFill"}) });
    tx.set(lref, ledgerRecord({ id:lid, accountId:o.accountId, kind:"fill", legs,
      meta:{orderId,fillId,symbol:o.symbol,barOpenAt:bar.t,barProvenance} }));
    tx.set(aref, projectionUpdate(a.data(),legs), {merge:true});
    tx.set(oref,{status:"filled",fillId,positionLifecycleId,qtyFilled:amounts.qty,
      trimmedQty:o.qty-amounts.qty,first_eligible_bar_at:bar.t},{merge:true});
    tx.set(pref,{accountId:o.accountId,symbol:o.symbol,open:true,qty:amounts.qty,
      costBasisCents:amounts.baseCents,entryFillNotionalCents:amounts.fillCents,
      entryFrictionCents:amounts.frictionCents,entryBarOpenUsd:bar.o,entryPriceUsd:fromPrice(fillInt),
      openedAt:bar.t,openOrderId:orderId,positionLifecycleId,lastMarkUsd:bar.o,lastMarkAt:bar.t,
      entryExecutionCostContext:o.executionCostContext||null,
      entryBarProvenance:barProvenance,lastMarkProvenance:barProvenance,
      variantId:o.variantId||"baseline",strategyVersion:o.strategyVersion,universeVersion:o.universeVersion,
      strategyHash:o.strategyHash,universeHash:o.universeHash,variantsHash:o.variantsHash,
      updated_at:A.FV.serverTimestamp()});
    if (lock.exists && lock.data().orderId===orderId) tx.delete(lockRef);
    return {orderId,fillId,positionLifecycleId,fillPriceUsd:fromPrice(fillInt),frictionCents:amounts.frictionCents,
      qty:amounts.qty,status:"filled"};
  });
}

async function closePosition({accountId,symbol,bar,slippageBps,reason,barProvenance,
  decisionAtMs,eligibleAfterMs,executionEligibleAfterMs=eligibleAfterMs,
  executionCostContext=null,closedBy="unknown"}) {
  assertBar(bar,barProvenance,"closePosition");
  if (!(Number(slippageBps)>=0) || Number(slippageBps)>1000) throw new Error("closePosition: invalid slippage");
  const intentClock=assertExecutionClock({decisionAtMs,eligibleAfterMs,barOpenAt:bar.t},`closePosition ${accountId}/${symbol}`);
  const executionClock=assertExecutionClock({decisionAtMs,
    eligibleAfterMs:Math.max(Number(eligibleAfterMs)||0,Number(executionEligibleAfterMs)||0),
    barOpenAt:bar.t},`closePosition execution ${accountId}/${symbol}`);
  const pref=A.col(A.COL.positions).doc(`${accountId}_${symbol}`);
  return A.runTransaction(async(tx)=>{
    const ps=await tx.get(pref); if(!ps.exists||!ps.data().open)return{symbol,noop:true,reason:"no open position"};
    const p=ps.data(),intent=p.exitIntent||{};
    if(Number(intent.decisionAtMs)!==intentClock.decisionAtMs||Number(intent.eligibleAfterMs)!==intentClock.eligibleAfterMs)
      throw new Error(`closePosition ${accountId}/${symbol}: exit intent changed or incomplete`);
    const openInt=toPrice(bar.o),fillInt=openInt-Math.round(openInt*slippageBps/10000);
    const exitBaseCents=notionalCents(p.qty,openInt),proceedsCents=notionalCents(p.qty,fillInt);
    const exitFrictionCents=Math.abs(exitBaseCents-proceedsCents),grossRealizedCents=exitBaseCents-p.costBasisCents;
    const lid=txnId([accountId,"close",symbol,"exit",bar.t]);
    const positionLifecycleId = p.positionLifecycleId
      || (p.openOrderId ? txnId([accountId, symbol, p.openOrderId, "position"])
        : txnId([accountId, symbol, p.openedAt || "legacy", p.entryPriceUsd || null,
          p.costBasisCents || null, intentClock.decisionAtMs, "position"]));
    const tradeId=txnId([accountId,positionLifecycleId,"trade"]);
    const lref=A.col(A.COL.ledger).doc(lid),tref=A.col(A.COL.trades).doc(tradeId),aref=accountRef(accountId);
    const [l,t,a]=await Promise.all([tx.get(lref),tx.get(tref),tx.get(aref)]);
    if(l.exists||t.exists)return{symbol,noop:true,duplicate:true};
    const legs=[
      {account:ACCT.POSITIONS,amountCents:-p.costBasisCents,memo:`close ${symbol} at cost`},
      {account:ACCT.CASH,amountCents:proceedsCents,memo:"exit proceeds"},
      {account:ACCT.FRICTION,amountCents:exitFrictionCents,memo:"exit implementation shortfall"},
      {account:ACCT.REALIZED_PL,amountCents:-grossRealizedCents,memo:`gross realized P&L ${symbol}`},
    ];
    const entryBarOpenUsd=Number(p.entryBarOpenUsd)||(p.qty>0?fromCents(p.costBasisCents)/p.qty:Number(p.entryPriceUsd)||0);
    const entryFillUsd=Number(p.entryPriceUsd)||entryBarOpenUsd,exitFillUsd=fromPrice(fillInt);
    const grossBps=entryBarOpenUsd>0?(bar.o-entryBarOpenUsd)/entryBarOpenUsd*1e4:0;
    const netBps=entryFillUsd>0?(exitFillUsd-entryFillUsd)/entryFillUsd*1e4:0;
    const costBps=grossBps-netBps,entryFillNotional=Number(p.entryFillNotionalCents)||p.costBasisCents+(Number(p.entryFrictionCents)||0);
    const netRealizedCents=proceedsCents-entryFillNotional,heldDays=p.openedAt?(Date.parse(bar.t)-Date.parse(p.openedAt))/864e5:null;
    const trade={tradeId,positionLifecycleId,accountId,symbol,qty:p.qty,openOrderId:p.openOrderId||null,openedAt:p.openedAt||null,
      closedAt:bar.t,heldDays,exitDecisionAtMs:intentClock.decisionAtMs,
      exitEligibleAfterMs:intentClock.eligibleAfterMs,
      exitExecutionEligibleAfterMs:executionClock.eligibleAfterMs,
      entryBarOpenUsd,entryFillUsd,exitBarOpenUsd:bar.o,exitFillUsd,
      entryFrictionCents:Number(p.entryFrictionCents)||0,exitFrictionCents,grossRealizedCents,netRealizedCents,
      grossBps:Number(grossBps.toFixed(2)),costBps:Number(costBps.toFixed(2)),netBps:Number(netBps.toFixed(2)),
      closeReason:reason||null,closedBy,
      entryExecutionCostContext:p.entryExecutionCostContext||null,
      exitExecutionCostContext:executionCostContext||null,
      entryBarProvenance:p.entryBarProvenance||null,
      exitDecisionMarketProvenance:intent.decisionMarketProvenance||null,
      exitExecutionMarketProvenance:barProvenance,exitBarProvenance:barProvenance,
      variantId:p.variantId||"baseline",strategyVersion:p.strategyVersion||null,universeVersion:p.universeVersion||null,
      strategyHash:p.strategyHash||null,universeHash:p.universeHash||null,variantsHash:p.variantsHash||null,
      ...A.envelope({created_by:"ledger.closePosition"})};
    tx.set(lref,ledgerRecord({id:lid,accountId,kind:"close",legs,meta:{symbol,tradeId,reason,barOpenAt:bar.t,barProvenance}}));
    tx.set(aref,projectionUpdate(a.data(),legs),{merge:true}); tx.set(tref,trade);
    tx.set(pref,{open:false,closedAt:bar.t,closeReason:reason,closedBy,tradeId,positionLifecycleId,exitPriceUsd:exitFillUsd,
      exitExecutionCostContext:executionCostContext||null,
      grossRealizedCents,netRealizedCents,grossBps:trade.grossBps,costBps:trade.costBps,netBps:trade.netBps,
      exitIntent:null,updated_at:A.FV.serverTimestamp()},{merge:true});
    return{symbol,tradeId,realizedCents:netRealizedCents,realizedUsd:fromCents(netRealizedCents),exitPriceUsd:exitFillUsd,
      frictionCents:(Number(p.entryFrictionCents)||0)+exitFrictionCents,variantId:p.variantId||"baseline",
      openedDate:String(p.openedAt||"").slice(0,10),heldDays,grossBps:trade.grossBps,costBps:trade.costBps,netBps:trade.netBps};
  });
}

async function costMeter(accountId){
  const b=await balances(accountId),frictionCents=Number(b.cents[ACCT.FRICTION])||0;
  const grossCents=b.cents[ACCT.REALIZED_PL]?-b.cents[ACCT.REALIZED_PL]:0,netCents=grossCents-frictionCents;
  const s=(await accountRef(accountId).get()).data()||{},startCents=s.startingNavCents||0;
  return{accountId,frictionUsd:fromCents(frictionCents),grossEdgeUsd:fromCents(grossCents),
    netEdgeUsd:fromCents(netCents),netRealizedUsd:fromCents(netCents),
    coverageRatio:frictionCents>0?Number((grossCents/frictionCents).toFixed(2)):null,
    frictionPctOfNav:startCents?Number((frictionCents/startCents*100).toFixed(3)):null,
    verdict:grossCents>frictionCents?"gross edge exceeds frictions":grossCents>0?"positive gross, frictions dominate":"no gross edge"};
}

async function executionAudit(accountId){
  const [fills,ordersSnap,trades]=await Promise.all([
    A.col(A.COL.fills).where("accountId","==",accountId).get(),A.col(A.COL.orders).where("accountId","==",accountId).get(),
    A.col(A.COL.trades).where("accountId","==",accountId).get()]);
  const orders=new Map();ordersSnap.forEach((d)=>orders.set(d.data().orderId,d.data()));const seen=new Set(),violations=[];
  fills.forEach((d)=>{const f=d.data(),o=orders.get(f.orderId);if(seen.has(f.orderId))violations.push({orderId:f.orderId,kind:"duplicate_fill"});seen.add(f.orderId);
    if(!o)violations.push({orderId:f.orderId,kind:"missing_order"});else{const c=executionClockCheck({decisionAtMs:Math.max(Number(o.decisionAtMs)||0,Number(o.approvedAtMs)||0),eligibleAfterMs:o.fillEligibleAfterMs,barOpenAt:f.barOpenAt});if(!c.pass)violations.push({orderId:f.orderId,kind:c.reason});}
    if(!f.barProvenance||f.barProvenance.barOpenAt!==f.barOpenAt||!/^[a-f0-9]{64}$/.test(String(f.barProvenance.sourceSha256||"")))violations.push({orderId:f.orderId,kind:"invalid_bar_provenance"});});
  trades.forEach((d)=>{const t=d.data(),c=executionClockCheck({decisionAtMs:t.exitDecisionAtMs,eligibleAfterMs:t.exitExecutionEligibleAfterMs||t.exitEligibleAfterMs,barOpenAt:t.closedAt});if(!c.pass)violations.push({tradeId:d.id,kind:`exit_${c.reason}`});
    const p=t.exitBarProvenance;if(!p||p.barOpenAt!==t.closedAt||!/^[a-f0-9]{64}$/.test(String(p.sourceSha256||"")))violations.push({tradeId:d.id,kind:"exit_invalid_bar_provenance"});});
  return{accountId,entryFills:fills.size,exits:trades.size,auditedExecutions:fills.size+trades.size,violations,pass:violations.length===0};
}

module.exports={ACCT,CENTS,PRICE_SCALE,toCents,fromCents,toPrice,fromPrice,notionalCents,txnId,assertBalanced,applyLegs,
  fillAmounts,executionClockCheck,assertExecutionClock,assertBar,controlAllowsEntry,orderLockRef,
  openAccount,post,balances,rebuildBalances,auditLedger,executionAudit,
  proposeOrder,approveOrder,releaseOrder,rejectOrder,recordFill,closePosition,costMeter};
