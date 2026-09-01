/* Investor_AI — atomic, fixed-point paper ledger. No broker is connected. */
"use strict";

const crypto = require("crypto");
const A = require("./_investorAdmin");
const M = require("./_investorMarket");
const STATE = require("./_investorState");

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
  REALIZED_PL: "realized_pl", DIVIDEND_INCOME: "dividend_income",
  FRICTION: "friction", CONTRIB: "contributed_capital",
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

function cashDividendLegs(eligibleQty, perShareUsd) {
  const qty = Number(eligibleQty), perShare = Number(perShareUsd);
  if (!(qty > 0) || !(perShare > 0)) throw new Error("cash dividend requires positive quantity and per-share amount");
  const amountCents = Math.round(qty * perShare * 100);
  if (amountCents < 1) throw new Error("cash dividend rounds below one cent");
  return { amountCents, legs: [
    { account: ACCT.CASH, amountCents, memo: "cash dividend received" },
    { account: ACCT.DIVIDEND_INCOME, amountCents: -amountCents, memo: "cash dividend income" },
  ] };
}

/** Operator-confirmed dividend booking. Announcements may be delayed or
 * corrected, so the source corporate-action id is mandatory and supplies the
 * immutable idempotency key; price action is never used to invent cash. */
async function recordCashDividend({ accountId, symbol, corporateActionId,
  positionLifecycleId, eligibleQty, perShareUsd, recordDate, payDate,
  sourceRef = null, operator = "operator" }) {
  symbol = String(symbol || "").toUpperCase();
  corporateActionId = String(corporateActionId || "");
  positionLifecycleId = String(positionLifecycleId || "");
  recordDate = String(recordDate || "");
  payDate = String(payDate || "");
  if (!/^[A-Z][A-Z0-9.-]{0,9}$/.test(symbol)) throw new Error("valid symbol required");
  if (!/^[A-Za-z0-9:_-]{6,120}$/.test(corporateActionId)) {
    throw new Error("provider corporateActionId is required for idempotency");
  }
  if (!/^\d{4}-\d{2}-\d{2}$/.test(recordDate)
      || !/^\d{4}-\d{2}-\d{2}$/.test(payDate)) {
    throw new Error("recordDate and payDate are required");
  }
  if (recordDate > payDate) throw new Error("payDate cannot precede recordDate");
  if (payDate > M.nyParts(new Date()).date) {
    throw new Error("future dividends cannot be posted before their payable date");
  }
  if (!/^[A-Za-z0-9:_-]{8,128}$/.test(positionLifecycleId)) {
    throw new Error("positionLifecycleId is required to bind the entitlement");
  }
  const financial = cashDividendLegs(eligibleQty, perShareUsd);
  const id = txnId([accountId, "cash_dividend", corporateActionId, positionLifecycleId]);
  const lref = A.col(A.COL.ledger).doc(id), aref = accountRef(accountId);
  const pref = A.col(A.COL.positions).doc(`${accountId}_${symbol}`);
  return A.runTransaction(async (tx) => {
    const [ledger, account, position] = await Promise.all([
      tx.get(lref), tx.get(aref), tx.get(pref),
    ]);
    if (ledger.exists) return { id, duplicate: true, amountCents: financial.amountCents };
    if (!account.exists) throw new Error(`account ${accountId} not found`);
    if (!position.exists) throw new Error(`position history for ${symbol} not found`);
    const p = position.data();
    if (p.positionLifecycleId !== positionLifecycleId) {
      throw new Error("position lifecycle does not match the confirmed entitlement");
    }
    const openedDate = String(p.openedAt || "").slice(0, 10);
    const closedDate = String(p.closedAt || "").slice(0, 10);
    if (openedDate && openedDate > recordDate) throw new Error("position opened after dividend record date");
    if (closedDate && closedDate < recordDate) throw new Error("position closed before dividend record date");
    const meta = { symbol, corporateActionId, positionLifecycleId,
      eligibleQty: Number(eligibleQty),
      perShareUsd: Number(perShareUsd), recordDate, payDate,
      sourceRef: String(sourceRef || "").slice(0, 500) || null, operator };
    tx.set(lref, ledgerRecord({ id, accountId, kind: "cash_dividend",
      legs: financial.legs, meta }));
    tx.set(aref, projectionUpdate(account.data(), financial.legs), { merge: true });
    tx.set(pref, { dividendHistory: [...(Array.isArray(p.dividendHistory) ? p.dividendHistory : []),
      { ...meta, amountCents: financial.amountCents }].slice(-100),
      updated_at: A.FV.serverTimestamp() }, { merge: true });
    return { id, duplicate: false, amountCents: financial.amountCents,
      amountUsd: fromCents(financial.amountCents), symbol, corporateActionId };
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

function snapshotRows(snap) {
  const rows = [];
  if (snap && typeof snap.forEach === "function") snap.forEach((d) => rows.push(d.data()));
  else if (snap && Array.isArray(snap.docs)) snap.docs.forEach((d) => rows.push(d.data()));
  return rows;
}
function validLifecycleId(value) { return /^[a-f0-9]{32}$/.test(String(value || "")); }
function validProvenance(value) {
  return !!value && !!value.provider
    && /^[a-f0-9]{64}$/.test(String(value.sourceSha256 || ""));
}

/** Verify that the operational documents form one immutable order lifecycle.
 * This is independent of journal balancing: a perfectly balanced duplicate
 * fill is still wrong and must stop new entries. */
async function lifecycleAudit(accountId) {
  const [orderSnap, fillSnap, positionSnap, tradeSnap, ledgerSnap] = await Promise.all([
    A.col(A.COL.orders).where("accountId", "==", accountId).get(),
    A.col(A.COL.fills).where("accountId", "==", accountId).get(),
    A.col(A.COL.positions).where("accountId", "==", accountId).get(),
    A.col(A.COL.trades).where("accountId", "==", accountId).get(),
    A.col(A.COL.ledger).where("accountId", "==", accountId).get(),
  ]);
  const orders = snapshotRows(orderSnap), fills = snapshotRows(fillSnap);
  const positions = snapshotRows(positionSnap), trades = snapshotRows(tradeSnap);
  const journals = snapshotRows(ledgerSnap);
  const violations = [], orderById = new Map(), fillByOrder = new Map();
  const lifecycleOwners = new Map();
  for (const o of orders) {
    orderById.set(o.orderId, o);
    if (!validLifecycleId(o.lifecycleId)) {
      violations.push({ kind: "order_missing_lifecycle", orderId: o.orderId || null });
    } else if (lifecycleOwners.has(o.lifecycleId) && lifecycleOwners.get(o.lifecycleId) !== o.orderId) {
      violations.push({ kind: "lifecycle_reused_by_orders", lifecycleId: o.lifecycleId });
    } else lifecycleOwners.set(o.lifecycleId, o.orderId);
  }
  for (const f of fills) {
    const list = fillByOrder.get(f.orderId) || [];
    list.push(f); fillByOrder.set(f.orderId, list);
    const o = orderById.get(f.orderId);
    if (!o) violations.push({ kind: "fill_missing_order", fillId: f.fillId || null });
    if (!validLifecycleId(f.lifecycleId)
        || (o && f.lifecycleId !== o.lifecycleId)) {
      violations.push({ kind: "fill_lifecycle_mismatch", orderId: f.orderId || null });
    }
    if (!validProvenance(f.barProvenance)) {
      violations.push({ kind: "fill_provenance_invalid", orderId: f.orderId || null });
    }
  }
  for (const o of orders) {
    const count = (fillByOrder.get(o.orderId) || []).length;
    if (count > 1) violations.push({ kind: "duplicate_fill", orderId: o.orderId, count });
    if (o.status === "filled" && count !== 1) {
      violations.push({ kind: "filled_order_count_mismatch", orderId: o.orderId, count });
    }
  }
  for (const p of positions) {
    const lifecycleId = p.lifecycleId || p.positionLifecycleId;
    const order = p.openOrderId && orderById.get(p.openOrderId);
    if (!validLifecycleId(lifecycleId)) {
      violations.push({ kind: "position_missing_lifecycle", symbol: p.symbol || null });
    } else if (order && order.lifecycleId !== lifecycleId) {
      violations.push({ kind: "position_lifecycle_mismatch", symbol: p.symbol || null });
    }
    if (p.open && (!order || order.status !== "filled")) {
      violations.push({ kind: "open_position_missing_filled_order", symbol: p.symbol || null });
    }
  }
  for (const t of trades) {
    const lifecycleId = t.lifecycleId || t.positionLifecycleId;
    const order = t.openOrderId && orderById.get(t.openOrderId);
    if (!validLifecycleId(lifecycleId)) {
      violations.push({ kind: "trade_missing_lifecycle", tradeId: t.tradeId || null });
    } else if (order && order.lifecycleId !== lifecycleId) {
      violations.push({ kind: "trade_lifecycle_mismatch", tradeId: t.tradeId || null });
    }
    if (!validProvenance(t.exitBarProvenance || t.exitExecutionMarketProvenance)) {
      violations.push({ kind: "trade_provenance_invalid", tradeId: t.tradeId || null });
    }
  }
  for (const j of journals) {
    if (!["reserve", "release", "fill", "close"].includes(j.kind)) continue;
    if (!validLifecycleId(j.meta && j.meta.lifecycleId)) {
      violations.push({ kind: "journal_missing_lifecycle", txnId: j.txnId || null, journalKind: j.kind });
    }
  }
  return { accountId, orders: orders.length, fills: fills.length,
    positions: positions.length, trades: trades.length, violations,
    pass: violations.length === 0 };
}

/** Recompute cash, reservations, cost basis and marked NAV from independent
 * records. All values are integer cents. A failure freezes entries but never
 * exits, and a later passing audit does not silently unfreeze the operator's
 * desk. */
async function reconcileAccount(accountId, { marks = null, expectedNavUsd = null,
  context = "manual" } = {}) {
  const checkedAtMs = Date.now();
  let result;
  try {
    const [account, journal, lifecycle, orderSnap, positionSnap] = await Promise.all([
      accountRef(accountId).get(), auditLedger(accountId), lifecycleAudit(accountId),
      A.col(A.COL.orders).where("accountId", "==", accountId).get(),
      A.col(A.COL.positions).where("accountId", "==", accountId).get(),
    ]);
    if (!account.exists) throw new Error(`account ${accountId} not found`);
    const a = account.data(), b = a.balanceCents || {};
    const orders = snapshotRows(orderSnap), positions = snapshotRows(positionSnap);
    const open = positions.filter((p) => p.open === true);
    const approved = orders.filter((o) => o.status === "approved");
    const openCostCents = open.reduce((sum, p) => sum + (Number(p.costBasisCents) || 0), 0);
    const reservedOrdersCents = approved.reduce((sum, o) => sum
      + (Number(o.grossCents) || 0) + (Number(o.frictionCents) || 0), 0);
    const capitalCents = -(Number(b[ACCT.CONTRIB]) || 0);
    const realizedCents = -(Number(b[ACCT.REALIZED_PL]) || 0);
    const dividendCents = -(Number(b[ACCT.DIVIDEND_INCOME]) || 0);
    const costCents = Number(b[ACCT.FRICTION]) || 0;
    const expectedCashCents = capitalCents + realizedCents + dividendCents
      - costCents - openCostCents - reservedOrdersCents;
    const equations = {
      cash: { actualCents: Number(b[ACCT.CASH]) || 0, expectedCents: expectedCashCents },
      positions: { actualCents: Number(b[ACCT.POSITIONS]) || 0, expectedCents: openCostCents },
      reservations: { actualCents: Number(b[ACCT.RESERVED]) || 0,
        expectedCents: reservedOrdersCents },
      /* Equivalent to starting capital + realized P/L + dividends - costs.
         Open cost basis and reservations remain assets, so they belong on the
         cash side of the conservation equation rather than disappearing. */
      cashConservation: {
        actualCents: (Number(b[ACCT.CASH]) || 0)
          + (Number(b[ACCT.RESERVED]) || 0) + openCostCents,
        expectedCents: capitalCents + realizedCents + dividendCents - costCents,
      },
    };
    for (const x of Object.values(equations)) x.pass = x.actualCents === x.expectedCents;

    let marketValueCents = 0;
    const markViolations = [];
    for (const p of open) {
      const supplied = marks && marks[p.symbol];
      const priceUsd = Number(supplied && typeof supplied === "object"
        ? supplied.priceUsd : (supplied != null ? supplied : p.lastMarkUsd));
      const provenance = supplied && typeof supplied === "object"
        ? supplied.provenance : p.lastMarkProvenance;
      if (!(priceUsd > 0)) {
        markViolations.push({ symbol: p.symbol, kind: "missing_mark" }); continue;
      }
      if (!validProvenance(provenance)) {
        markViolations.push({ symbol: p.symbol, kind: "mark_provenance_invalid" }); continue;
      }
      marketValueCents += Math.round(Number(p.qty) * priceUsd * 100);
    }
    const liquidCashCents = (Number(b[ACCT.CASH]) || 0) + (Number(b[ACCT.RESERVED]) || 0);
    const computedNavCents = liquidCashCents + marketValueCents;
    const displayedNavCents = expectedNavUsd == null ? null : toCents(expectedNavUsd);
    const nav = { liquidCashCents, marketValueCents, computedNavCents,
      displayedNavCents, marksComplete: markViolations.length === 0,
      pass: markViolations.length === 0
        && (displayedNavCents == null || Math.abs(displayedNavCents - computedNavCents) <= 1) };
    const equationPass = Object.values(equations).every((x) => x.pass);
    result = { accountId, context, checkedAtMs, journal, lifecycle, equations,
      markViolations, nav, pass: journal.pass && lifecycle.pass && equationPass && nav.pass };
  } catch (error) {
    result = { accountId, context, checkedAtMs, pass: false,
      error: String(error && error.message || error).slice(0, 300) };
  }

  const compact = { pass: result.pass, context, checkedAtMs,
    discrepancyCount: (result.journal && result.journal.discrepancies || []).length
      + (result.lifecycle && result.lifecycle.violations || []).length
      + (result.markViolations || []).length,
    computedNavCents: result.nav && result.nav.computedNavCents,
    displayedNavCents: result.nav && result.nav.displayedNavCents,
    error: result.error || null };
  await A.col(A.COL.invariants).doc(`reconciliation_${accountId}`).set({
    ...result, checkedAt: A.FV.serverTimestamp(),
    ...A.envelope({ created_by: "ledger.reconcileAccount" }),
  }, { merge: false });
  const controlRef = A.col(A.COL.control).doc("control");
  const controlSnap = await controlRef.get();
  if (!controlSnap.exists || (controlSnap.data().accountId || "paper-1") === accountId) {
    const ctrl = controlSnap.exists ? controlSnap.data() : { accountId };
    const patch = { ledgerReconciliation: compact,
      reconciliationFailure: result.pass ? null : compact };
    if (!result.pass && !STATE.describe(ctrl).paused) {
      Object.assign(patch, STATE.legacyPatch(STATE.STATES.ENTRY_FROZEN, ctrl), {
        operatingStateReason: "automatic paper-ledger reconciliation failed",
        operatingStateSource: "reconciliation",
      });
    }
    await controlRef.set(patch, { merge: true });
  }
  return result;
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
  const operating = STATE.describe(c);
  if (c.reconciliationFailure || (c.ledgerReconciliation
      && c.ledgerReconciliation.pass === false)) {
    return fail("paper-ledger reconciliation is unresolved");
  }
  if (!operating.entriesAllowed) {
    return fail(operating.entriesFrozen ? "new entries frozen by operator"
      : (operating.paused ? "entry controls closed" : "observation-only mode cannot write the paper ledger"));
  }
  if (c.fixturesPass !== true || c.fixturesCommit !== commit) return fail("current build fixtures not attested");
  for (const k of ["accountId","strategyVersion","universeVersion","strategyHash","universeHash","variantsHash"]) {
    if (!identity[k] || e[k] !== identity[k]) return fail(`safety epoch mismatch: ${k}`);
  }
  if (e.commit !== commit) return fail("safety epoch commit mismatch");
  return { pass: true, scope: "paper_ledger", operatingState: operating.state };
}

async function proposeOrder(input) {
  const { accountId, symbol, side, decisionId, strategyVersion, universeVersion,
    universeHash, strategyHash, variantsHash, qty, refPriceUsd, slippageBps,
    sizing, gates, cause, evidenceRefs = [], decisionAtMs, quality,
    variantId = "baseline", cost = null, portfolioRisk = null, decisionContext = null,
    decisionManifestHash = null, portfolioDecisionManifestHash = null,
    decisionMarketProvenance = null, executionCostContext = null,
    paperLearningOnly = false,
    operatingStateAtDecision = null, learningCohort = null,
    exploratoryPolicyVersion = null,
    executionLatencyMs = 60000 } = input;
  if (![universeHash, strategyHash, variantsHash].every((h) => /^[a-f0-9]{64}$/.test(String(h || "")))) {
    throw new Error("proposeOrder: complete policy hashes are required");
  }
  if (!/^[a-f0-9]{64}$/.test(String(decisionManifestHash || ""))) {
    throw new Error("proposeOrder: decision input manifest hash is required");
  }
  if (!/^[a-f0-9]{64}$/.test(String(portfolioDecisionManifestHash || ""))) {
    throw new Error("proposeOrder: portfolio decision manifest hash is required");
  }
  if (!decisionMarketProvenance || !decisionMarketProvenance.provider
      || !/^[a-f0-9]{64}$/.test(String(decisionMarketProvenance.sourceSha256 || ""))) {
    throw new Error("proposeOrder: decision market provenance is required");
  }
  const orderId = txnId([accountId, symbol, decisionId]);
  const lifecycleId = txnId([accountId, symbol, orderId, "lifecycle"]);
  const ref = A.col(A.COL.orders).doc(orderId), lockRef = orderLockRef(accountId, symbol);
  const controlRef = A.col(A.COL.control).doc("control");
  const priceInt = toPrice(refPriceUsd), grossCents = notionalCents(qty, priceInt);
  const frictionCents = Math.max(1, Math.round(grossCents * Number(slippageBps) / 10000));
  return A.runTransaction(async (tx) => {
    const [cur, lock, controlSnap] = await Promise.all([tx.get(ref), tx.get(lockRef), tx.get(controlRef)]);
    if (cur.exists) {
      const existing = cur.data();
      return { orderId, lifecycleId: existing.lifecycleId || lifecycleId,
        grossCents: existing.grossCents, frictionCents: existing.frictionCents,
        qty: existing.qty, refPriceUsd: existing.refPriceUsd,
        duplicate: true, status: existing.status };
    }
    const permission = controlAllowsEntry(controlSnap.exists ? controlSnap.data() : {}, {
      accountId, strategyVersion, universeVersion, universeHash, strategyHash, variantsHash,
    });
    if (!permission.pass) return { orderId, blocked: permission.reason, status: "not_proposed" };
    if (lock.exists) return { orderId, blocked: "an active order already owns this symbol", status: "not_proposed" };
    const row = { orderId, lifecycleId, accountId, symbol, side, decisionId, strategyVersion, universeVersion,
      universeHash, strategyHash, variantsHash, qty, refPriceInt: priceInt,
      refPriceUsd: fromPrice(priceInt), grossCents, frictionCents, slippageBps,
      sizing, gates, cause, evidenceRefs, variantId, portfolioRisk, decisionContext,
      decisionManifestHash, portfolioDecisionManifestHash,
      decisionMarketProvenance, executionCostContext,
      cost: cost ? { ratio: cost.ratio, expectedGrossBps: cost.expectedGrossBps,
        halfTripBps: cost.halfTripBps, roundTripBps: cost.roundTripBps,
        requiredBps: cost.requiredBps, calibratedNetLowerBoundBps: cost.calibratedNetLowerBoundBps } : null,
      quality, paperLearningOnly: paperLearningOnly === true,
      operatingStateAtDecision, learningCohort,
      exploratoryPolicyVersion,
      status: "proposed", decisionAtMs: Number(decisionAtMs),
      executionLatencyMs: Math.max(0, Number(executionLatencyMs) || 60000),
      order_committed_at: A.FV.serverTimestamp(), ...A.envelope({ created_by: "ledger.proposeOrder" }) };
    tx.set(ref, row);
    tx.set(lockRef, { accountId, symbol, orderId, lifecycleId,
      status: "proposed", createdAtMs: Date.now() });
    return { orderId, lifecycleId, grossCents, frictionCents, qty,
      refPriceUsd: fromPrice(priceInt), status: "proposed" };
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
    if (o.status !== "proposed") return { orderId,
      lifecycleId: o.lifecycleId || null, status: o.status,
      approvedAtMs: o.approvedAtMs || null, duplicate: o.status === "approved", noop: true };
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
      meta: { orderId, lifecycleId: o.lifecycleId || null, symbol: o.symbol } }));
    tx.set(aref, projectionUpdate(a.data(), legs), { merge: true });
    tx.set(ref, { status: "approved", approvedBy: operator || "operator", approvedAtMs,
      fillEligibleAfterMs: approvedAtMs + feedCfg.delayMinutes * 60000
        + (Number(o.executionLatencyMs) || 60000),
      operator_approved_at: A.FV.serverTimestamp() }, { merge: true });
    tx.set(orderLockRef(o.accountId, o.symbol), {
      status: "approved", orderId, lifecycleId: o.lifecycleId || null }, { merge: true });
    return { orderId, lifecycleId: o.lifecycleId || null, status: "approved", approvedAtMs };
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
      meta: { orderId, lifecycleId: o.lifecycleId || null,
        symbol: o.symbol, reason: reason || "released" } }));
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
  const result = await A.runTransaction(async (tx) => {
    const os = await tx.get(oref); if (!os.exists) throw new Error(`recordFill: order ${orderId} not found`);
    const o = os.data();
    if (o.status === "filled") {
      const existing = o.fillId ? await tx.get(A.col(A.COL.fills).doc(o.fillId)) : null;
      const f = existing && existing.exists ? existing.data() : {};
      return { orderId, accountId: o.accountId, fillId: o.fillId || f.fillId || null,
        lifecycleId: o.lifecycleId || f.lifecycleId || null,
        positionLifecycleId: o.lifecycleId || f.positionLifecycleId || null,
        fillPriceUsd: f.fillPriceUsd ?? null, frictionCents: f.frictionCents ?? null,
        qty: f.qty ?? o.qtyFilled ?? null, duplicate: true, status: "filled" };
    }
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
    if (f.exists || l.exists) {
      const existing = f.exists ? f.data() : {};
      return { orderId, accountId:o.accountId, fillId:existing.fillId || fillId,
        lifecycleId:existing.lifecycleId || o.lifecycleId || null,
        positionLifecycleId:existing.positionLifecycleId || o.lifecycleId || null,
        fillPriceUsd:existing.fillPriceUsd ?? null,
        frictionCents:existing.frictionCents ?? null, qty:existing.qty ?? o.qtyFilled ?? null,
        duplicate:true,status:"filled" };
    }

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
        meta:{ orderId, lifecycleId:o.lifecycleId || null,
          symbol:o.symbol, reason:abortReason, phase:"fill_abort" } }));
      tx.set(aref, projectionUpdate(a.data(), releaseLegs), { merge:true });
      tx.set(oref, { status:"expired", expireReason:abortReason, expired_at:A.FV.serverTimestamp() }, { merge:true });
      if (lock.exists && lock.data().orderId===orderId) tx.delete(lockRef);
      return { orderId, status:"expired", released:true, releasedCents:reservedCents, reason:abortReason };
    }
    const lifecycleId = o.lifecycleId || txnId([o.accountId, o.symbol, orderId, "lifecycle"]);
    const positionLifecycleId = lifecycleId;
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
      frictionCents:amounts.frictionCents, barProvenance, lifecycleId, positionLifecycleId,
      decisionId:o.decisionId||null,decisionManifestHash:o.decisionManifestHash||null,
      portfolioDecisionManifestHash:o.portfolioDecisionManifestHash||null,
      operatingStateAtDecision:o.operatingStateAtDecision||null,
      learningCohort:o.learningCohort||null,
      exploratoryPolicyVersion:o.exploratoryPolicyVersion||null,
      fill_confirmed_at:A.FV.serverTimestamp(), ...A.envelope({created_by:"ledger.recordFill"}) });
    tx.set(lref, ledgerRecord({ id:lid, accountId:o.accountId, kind:"fill", legs,
      meta:{orderId,fillId,lifecycleId,symbol:o.symbol,barOpenAt:bar.t,barProvenance} }));
    tx.set(aref, projectionUpdate(a.data(),legs), {merge:true});
    tx.set(oref,{status:"filled",fillId,lifecycleId,positionLifecycleId,qtyFilled:amounts.qty,
      trimmedQty:o.qty-amounts.qty,first_eligible_bar_at:bar.t},{merge:true});
    tx.set(pref,{accountId:o.accountId,symbol:o.symbol,open:true,qty:amounts.qty,
      costBasisCents:amounts.baseCents,entryFillNotionalCents:amounts.fillCents,
      entryFrictionCents:amounts.frictionCents,entryBarOpenUsd:bar.o,entryPriceUsd:fromPrice(fillInt),
      openedAt:bar.t,openOrderId:orderId,lifecycleId,positionLifecycleId,lastMarkUsd:bar.o,lastMarkAt:bar.t,
      entryExecutionCostContext:o.executionCostContext||null,
      entryBarProvenance:barProvenance,lastMarkProvenance:barProvenance,
      decisionId:o.decisionId||null,
      decisionManifestHash:o.decisionManifestHash||null,
      portfolioDecisionManifestHash:o.portfolioDecisionManifestHash||null,
      decisionContext:o.decisionContext||null,decisionGates:o.gates||[],
      decisionSizing:o.sizing||null,portfolioRisk:o.portfolioRisk||null,
      cause:o.cause||null,paperLearningOnly:o.paperLearningOnly===true,
      operatingStateAtDecision:o.operatingStateAtDecision||null,
      learningCohort:o.learningCohort||null,
      exploratoryPolicyVersion:o.exploratoryPolicyVersion||null,
      variantId:o.variantId||"baseline",strategyVersion:o.strategyVersion,universeVersion:o.universeVersion,
      strategyHash:o.strategyHash,universeHash:o.universeHash,variantsHash:o.variantsHash,
      updated_at:A.FV.serverTimestamp()});
    if (lock.exists && lock.data().orderId===orderId) tx.delete(lockRef);
    return {orderId,accountId:o.accountId,fillId,lifecycleId,positionLifecycleId,fillPriceUsd:fromPrice(fillInt),frictionCents:amounts.frictionCents,
      qty:amounts.qty,status:"filled"};
  });
  if (result.status === "filled" && result.accountId) {
    const reconciliation = await reconcileAccount(result.accountId, { context: "post_fill" });
    return { ...result, reconciliation };
  }
  return result;
}

function closeResultFromTrade(t, duplicate = false) {
  return { accountId: t.accountId, symbol: t.symbol, tradeId: t.tradeId,
    lifecycleId: t.lifecycleId || t.positionLifecycleId,
    realizedCents: t.netRealizedCents,
    realizedUsd: fromCents(t.netRealizedCents), exitPriceUsd: t.exitFillUsd,
    frictionCents: (Number(t.entryFrictionCents) || 0) + (Number(t.exitFrictionCents) || 0),
    variantId: t.variantId || "baseline", openedDate: String(t.openedAt || "").slice(0, 10),
    learningCohort: t.learningCohort || null,
    heldDays: t.heldDays, grossBps: t.grossBps, costBps: t.costBps,
    netBps: t.netBps, duplicate };
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
  const result = await A.runTransaction(async(tx)=>{
    const ps=await tx.get(pref);
    if(!ps.exists)return{accountId,symbol,noop:true,reason:"no open position"};
    if(!ps.data().open) {
      const closed = ps.data();
      if (closed.tradeId) {
        const existingTrade = await tx.get(A.col(A.COL.trades).doc(closed.tradeId));
        if (existingTrade.exists) return closeResultFromTrade(existingTrade.data(), true);
      }
      return{accountId,symbol,noop:true,reason:"no open position"};
    }
    const p=ps.data(),intent=p.exitIntent||{};
    if(Number(intent.decisionAtMs)!==intentClock.decisionAtMs||Number(intent.eligibleAfterMs)!==intentClock.eligibleAfterMs)
      throw new Error(`closePosition ${accountId}/${symbol}: exit intent changed or incomplete`);
    const openInt=toPrice(bar.o),fillInt=openInt-Math.round(openInt*slippageBps/10000);
    const exitBaseCents=notionalCents(p.qty,openInt),proceedsCents=notionalCents(p.qty,fillInt);
    const exitFrictionCents=Math.abs(exitBaseCents-proceedsCents),grossRealizedCents=exitBaseCents-p.costBasisCents;
    const lid=txnId([accountId,"close",symbol,"exit",bar.t]);
    const lifecycleId = p.lifecycleId || p.positionLifecycleId
      || (p.openOrderId ? txnId([accountId, symbol, p.openOrderId, "position"])
        : txnId([accountId, symbol, p.openedAt || "legacy", p.entryPriceUsd || null,
          p.costBasisCents || null, intentClock.decisionAtMs, "position"]));
    const positionLifecycleId = lifecycleId;
    const tradeId=txnId([accountId,positionLifecycleId,"trade"]);
    const lref=A.col(A.COL.ledger).doc(lid),tref=A.col(A.COL.trades).doc(tradeId),aref=accountRef(accountId);
    const [l,t,a]=await Promise.all([tx.get(lref),tx.get(tref),tx.get(aref)]);
    if (t.exists) return closeResultFromTrade(t.data(), true);
    if (l.exists) throw new Error(`closePosition ${accountId}/${symbol}: close journal exists without trade`);
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
    const netRealizedCents=proceedsCents-entryFillNotional,heldDays=p.openedAt?M.tradingDaysHeld(p.openedAt,bar.t):null;
    const trade={tradeId,lifecycleId,positionLifecycleId,accountId,symbol,qty:p.qty,openOrderId:p.openOrderId||null,openedAt:p.openedAt||null,
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
      decisionId:p.decisionId||null,
      decisionManifestHash:p.decisionManifestHash||null,
      portfolioDecisionManifestHash:p.portfolioDecisionManifestHash||null,
      decisionContext:p.decisionContext||null,decisionGates:p.decisionGates||[],
      decisionSizing:p.decisionSizing||null,portfolioRisk:p.portfolioRisk||null,
      cause:p.cause||null,paperLearningOnly:p.paperLearningOnly===true,
      operatingStateAtDecision:p.operatingStateAtDecision||null,
      learningCohort:p.learningCohort||null,
      exploratoryPolicyVersion:p.exploratoryPolicyVersion||null,
      variantId:p.variantId||"baseline",strategyVersion:p.strategyVersion||null,universeVersion:p.universeVersion||null,
      strategyHash:p.strategyHash||null,universeHash:p.universeHash||null,variantsHash:p.variantsHash||null,
      ...A.envelope({created_by:"ledger.closePosition"})};
    tx.set(lref,ledgerRecord({id:lid,accountId,kind:"close",legs,
      meta:{symbol,tradeId,lifecycleId,reason,barOpenAt:bar.t,barProvenance}}));
    tx.set(aref,projectionUpdate(a.data(),legs),{merge:true}); tx.set(tref,trade);
    tx.set(pref,{open:false,closedAt:bar.t,closeReason:reason,closedBy,tradeId,lifecycleId,positionLifecycleId,exitPriceUsd:exitFillUsd,
      exitExecutionCostContext:executionCostContext||null,
      grossRealizedCents,netRealizedCents,grossBps:trade.grossBps,costBps:trade.costBps,netBps:trade.netBps,
      exitIntent:null,updated_at:A.FV.serverTimestamp()},{merge:true});
    return{accountId,symbol,tradeId,lifecycleId,realizedCents:netRealizedCents,realizedUsd:fromCents(netRealizedCents),exitPriceUsd:exitFillUsd,
      frictionCents:(Number(p.entryFrictionCents)||0)+exitFrictionCents,variantId:p.variantId||"baseline",
      openedDate:String(p.openedAt||"").slice(0,10),heldDays,grossBps:trade.grossBps,costBps:trade.costBps,netBps:trade.netBps};
  });
  if (result.tradeId) {
    const reconciliation = await reconcileAccount(accountId, { context: "post_close" });
    return { ...result, reconciliation };
  }
  return result;
}

async function costMeter(accountId){
  const b=await balances(accountId),frictionCents=Number(b.cents[ACCT.FRICTION])||0;
  const realizedCents=b.cents[ACCT.REALIZED_PL]?-b.cents[ACCT.REALIZED_PL]:0;
  const dividendCents=b.cents[ACCT.DIVIDEND_INCOME]?-b.cents[ACCT.DIVIDEND_INCOME]:0;
  const grossCents=realizedCents+dividendCents,netCents=grossCents-frictionCents;
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
  openAccount,post,cashDividendLegs,recordCashDividend,balances,rebuildBalances,auditLedger,
  lifecycleAudit,reconcileAccount,validLifecycleId,validProvenance,executionAudit,
  proposeOrder,approveOrder,releaseOrder,rejectOrder,recordFill,closePosition,costMeter};
