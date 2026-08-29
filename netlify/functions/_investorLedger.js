/*  netlify/functions/_investorLedger.js  (v1.0)
 *  ---------------------------------------------------------------------------
 *  Investor_AI — double-entry paper ledger.
 *
 *  NO REAL MONEY EVER MOVES THROUGH THIS FILE. There is no broker, no API key
 *  for an executing venue, and no code path that could place an order. This is
 *  a simulation ledger whose only purpose is to measure whether a predeclared
 *  rule produced a positive result after realistic frictions.
 *
 *  INVARIANTS
 *  ------------------------------------------------------------------------
 *  1. FIXED-POINT ARITHMETIC. Cash is integer cents. Quantities are integer
 *     shares. Prices are integer 1e-6 dollars. JavaScript binary floats cannot
 *     represent 0.1 exactly and a ledger that drifts is worthless as evidence.
 *  2. BALANCED LEGS. Every transaction's debits equal its credits, checked
 *     before the write, and the transaction is rejected if they do not.
 *  3. DETERMINISTIC IDS. txn id = hash(orderId, kind, sequence). A duplicate
 *     background invocation writing the same fill is a no-op, not a double
 *     position. Netlify background functions retry on failure; this is the
 *     only thing standing between a retry and a corrupted book.
 *  4. CREATE-ONLY. A posted transaction is never updated or deleted. A
 *     correction is a new reversing transaction plus a new correct one, with
 *     an explicit supersedes pointer. This is an application invariant, not a
 *     claim that a shared Admin credential cannot rewrite the database.
 *  5. COST IS RECORDED SEPARATELY FROM PRICE. Slippage and spread post to
 *     their own accounts so the cost meter can answer the only question that
 *     matters at 400% monthly turnover: did gross edge exceed frictions?
 * ---------------------------------------------------------------------------
 */

"use strict";

const crypto = require("crypto");
const A = require("./_investorAdmin");

/* ── fixed point ───────────────────────────────────────────────────────── */
const CENTS = 100;              // cash: integer cents
const PRICE_SCALE = 1e6;        // price: integer micro-dollars

const toCents = (usd) => Math.round(Number(usd) * CENTS);
const fromCents = (c) => Number(c) / CENTS;
const toPrice = (usd) => Math.round(Number(usd) * PRICE_SCALE);
const fromPrice = (p) => Number(p) / PRICE_SCALE;
/** notional in cents for qty shares at an integer micro-dollar price */
const notionalCents = (qty, priceInt) =>
  Math.round((Number(qty) * Number(priceInt)) / (PRICE_SCALE / CENTS));

/* ── chart of accounts ─────────────────────────────────────────────────── */
const ACCT = {
  CASH: "cash",
  POSITIONS: "positions",         // market value at cost
  RESERVED: "reserved",           // cash committed to an approved, unfilled order
  REALIZED_PL: "realized_pl",
  FRICTION: "friction",           // slippage + modelled spread — the cost meter
  CONTRIB: "contributed_capital",
};

function txnId(parts) {
  return crypto.createHash("sha256").update(parts.join("|")).digest("hex").slice(0, 32);
}

/** Reject anything that does not balance. Called before every write. */
function assertBalanced(legs, ref) {
  let sum = 0;
  for (const l of legs) {
    if (!Number.isInteger(l.amountCents)) {
      throw new Error(`ledger ${ref}: leg ${l.account} amount ${l.amountCents} is not an integer`);
    }
    sum += l.amountCents;
  }
  if (sum !== 0) {
    throw new Error(`ledger ${ref}: legs do not balance, residual ${sum} cents`);
  }
  return true;
}

/* ── account bootstrap ─────────────────────────────────────────────────── */
async function openAccount({ accountId, startingNavUsd, baseCurrency = "USD", strategyVersion }) {
  const ref = A.col(A.COL.accounts).doc(accountId);
  const snap = await ref.get();
  if (snap.exists) return { accountId, existing: true, ...snap.data() };

  const startCents = toCents(startingNavUsd);
  await ref.set({
    accountId, baseCurrency,
    startingNavCents: startCents,
    openedAt: A.FV.serverTimestamp(),
    strategyVersion,
    mode: "research",            // research -> approval -> shadow -> limited_auto
    ...A.envelope({ created_by: "ledger.openAccount" }),
  });

  await post({
    accountId,
    kind: "capital_contribution",
    idParts: [accountId, "genesis"],
    legs: [
      { account: ACCT.CASH, amountCents: startCents, memo: "opening virtual balance" },
      { account: ACCT.CONTRIB, amountCents: -startCents, memo: "contributed capital" },
    ],
    meta: { note: "virtual money only — no broker is connected to this system" },
  });
  return { accountId, existing: false, startingNavCents: startCents };
}

/* ── the single write path ─────────────────────────────────────────────── */
async function post({ accountId, kind, idParts, legs, meta = {}, supersedes = null }) {
  const id = txnId([accountId, kind, ...idParts]);
  assertBalanced(legs, id);

  const ref = A.col(A.COL.ledger).doc(id);
  const written = await A.runTransaction(async (tx) => {
    const cur = await tx.get(ref);
    if (cur.exists) return { id, duplicate: true };   // idempotent: retry is a no-op
    tx.set(ref, {
      txnId: id, accountId, kind, legs, meta, supersedes,
      postedAt: A.FV.serverTimestamp(),
      ...A.envelope({ created_by: "ledger.post" }),
    });
    return { id, duplicate: false };
  });
  return written;
}

/* ── balances, rebuilt from the journal ────────────────────────────────── */
async function balances(accountId) {
  const snap = await A.col(A.COL.ledger).where("accountId", "==", accountId).get();
  const acc = {};
  let txns = 0;
  snap.forEach((d) => {
    txns += 1;
    for (const l of d.data().legs || []) acc[l.account] = (acc[l.account] || 0) + l.amountCents;
  });
  return {
    accountId, txns,
    cents: acc,
    usd: Object.fromEntries(Object.entries(acc).map(([k, v]) => [k, fromCents(v)])),
  };
}

/* ── order lifecycle ───────────────────────────────────────────────────── */
/**
 * Create an immutable order. Size is computed from DECISION-TIME information
 * only — trailing volatility, trailing dollar volume, cash, risk state. The
 * bar that will fill it has not begun. Nothing about that bar's high, low,
 * close or total volume may influence this record, now or later.
 */
async function proposeOrder({
  accountId, symbol, side, decisionId, strategyVersion,
  qty, refPriceUsd, slippageBps, sizing, gates, cause, evidenceRefs = [],
  decisionAtMs, quality, variantId = "baseline", cost = null,
}) {
  const orderId = txnId([accountId, symbol, decisionId]);
  const ref = A.col(A.COL.orders).doc(orderId);
  const priceInt = toPrice(refPriceUsd);
  const grossCents = notionalCents(qty, priceInt);
  const frictionCents = Math.max(1, Math.round((grossCents * slippageBps) / 10000));

  await ref.set({
    orderId, accountId, symbol, side, decisionId, strategyVersion,
    qty, refPriceInt: priceInt, refPriceUsd: fromPrice(priceInt),
    grossCents, frictionCents, slippageBps,
    sizing, gates, cause, evidenceRefs, variantId,
    /* The cost-hurdle verdict rides on the order so auto-approval can refuse
       marginal passes. It previously read order.cost.ratio — a field nothing
       wrote — so the "never auto-approve a marginal pass" guard was dead code. */
    cost: cost ? { ratio: cost.ratio, expectedGrossBps: cost.expectedGrossBps, requiredBps: cost.requiredBps } : null,
    quality,
    status: "proposed",
    decisionAtMs,
    order_committed_at: A.FV.serverTimestamp(),
    ...A.envelope({ created_by: "ledger.proposeOrder" }),
  }, { merge: false });

  return { orderId, grossCents, frictionCents, qty, refPriceUsd: fromPrice(priceInt) };
}

async function approveOrder(orderId, operator) {
  const ref = A.col(A.COL.orders).doc(orderId);
  const snap = await ref.get();
  if (!snap.exists) throw new Error(`approveOrder: ${orderId} not found`);
  const o = snap.data();
  if (o.status !== "proposed") return { orderId, status: o.status, noop: true };

  /* The reservation only prevents over-commitment if something actually checks
     the balance. Nothing did: stacked approvals could drive CASH negative with
     no error. Refuse an approval the account cannot fund. */
  const bal = await balances(o.accountId);
  const cashCents = bal.cents[ACCT.CASH] || 0;
  const needCents = o.grossCents + o.frictionCents;
  if (needCents > cashCents) {
    return { orderId, status: "proposed", noop: true,
             refused: `needs ${fromCents(needCents)} but only ${fromCents(cashCents)} cash is free` };
  }

  // Reserve the cash so two concurrent approvals cannot over-commit.
  await post({
    accountId: o.accountId, kind: "reserve",
    idParts: [orderId, "reserve"],
    legs: [
      { account: ACCT.CASH, amountCents: -(o.grossCents + o.frictionCents), memo: `reserve ${o.symbol}` },
      { account: ACCT.RESERVED, amountCents: (o.grossCents + o.frictionCents), memo: `reserved for ${orderId}` },
    ],
    meta: { orderId, symbol: o.symbol },
  });

  await ref.set({ status: "approved", approvedBy: operator || "operator",
                  operator_approved_at: A.FV.serverTimestamp() }, { merge: true });
  return { orderId, status: "approved" };
}

/**
 * Release an approved-but-unfilled order and return its reserved cash.
 *
 * approveOrder debits CASH into RESERVED so two concurrent approvals cannot
 * over-commit. Nothing ever reversed that. An order whose symbol stopped
 * returning bars was skipped by settlement forever, and its cash sat in
 * RESERVED permanently — an account could quietly reserve itself into
 * paralysis with no error and no way back through the UI.
 */
async function releaseOrder(orderId, reason) {
  const ref = A.col(A.COL.orders).doc(orderId);
  const snap = await ref.get();
  if (!snap.exists) return { orderId, noop: true, reason: "not found" };
  const o = snap.data();
  if (o.status !== "approved") return { orderId, noop: true, status: o.status };

  await post({
    accountId: o.accountId, kind: "release",
    idParts: [orderId, "release"],
    legs: [
      { account: ACCT.RESERVED, amountCents: -(o.grossCents + o.frictionCents), memo: `release ${orderId}` },
      { account: ACCT.CASH, amountCents: (o.grossCents + o.frictionCents), memo: `returned unfilled ${o.symbol}` },
    ],
    meta: { orderId, symbol: o.symbol, reason: reason || "released" },
  });

  await ref.set({ status: "expired", expireReason: reason || "released",
                  expired_at: A.FV.serverTimestamp() }, { merge: true });
  return { orderId, status: "expired", releasedCents: o.grossCents + o.frictionCents };
}

async function rejectOrder(orderId, reason, operator) {
  const ref = A.col(A.COL.orders).doc(orderId);
  await ref.set({ status: "rejected", rejectReason: reason || "operator rejected",
                  rejectedBy: operator || "operator",
                  rejected_at: A.FV.serverTimestamp() }, { merge: true });
  return { orderId, status: "rejected" };
}

/**
 * Record a simulated fill. The caller must have obtained `bar` from
 * _investorMarket.firstEligibleBar() — a bar that opened strictly after the
 * decision and that the feed had actually published. The fill price is the
 * bar's OPEN plus adverse slippage. Never the high, low, close, or VWAP: those
 * are only knowable after the interval and using them is the classic
 * look-ahead that makes a backtest lie.
 */
async function recordFill({ orderId, bar, barProvenance }) {
  const oref = A.col(A.COL.orders).doc(orderId);
  const snap = await oref.get();
  if (!snap.exists) throw new Error(`recordFill: order ${orderId} not found`);
  const o = snap.data();
  if (o.status === "filled") return { orderId, duplicate: true, status: "filled" };
  if (o.status !== "approved") throw new Error(`recordFill: order ${orderId} is ${o.status}, not approved`);

  /* DOUBLE-FILL GUARD. The position doc is written with {merge:true}, so a
     second fill for the same symbol would OVERWRITE qty and cost basis while
     the ledger credited POSITIONS twice — cash spent twice, the book showing
     one position, and the excess cost stranded forever (closePosition reverses
     only the recorded basis). Refuse the fill and release its reservation. */
  const existing = await A.col(A.COL.positions).doc(`${o.accountId}_${o.symbol}`).get();
  if (existing.exists && existing.data().open) {
    await post({
      accountId: o.accountId, kind: "release",
      idParts: [orderId, "release_dup"],
      legs: [
        { account: ACCT.RESERVED, amountCents: -(o.grossCents + o.frictionCents), memo: `duplicate ${o.symbol}` },
        { account: ACCT.CASH, amountCents: (o.grossCents + o.frictionCents), memo: `returned: already holding ${o.symbol}` },
      ],
      meta: { orderId, symbol: o.symbol, reason: "already holding this symbol" },
    });
    await oref.set({ status: "cancelled", cancelReason: "already holding this symbol — duplicate fill refused" }, { merge: true });
    return { orderId, duplicate: true, status: "cancelled", reason: "position already open" };
  }

  const openInt = toPrice(bar.o);
  const adverse = o.side === "buy" ? 1 : -1;
  const fillInt = openInt + adverse * Math.round((openInt * o.slippageBps) / 10000);
  const grossCents = notionalCents(o.qty, fillInt);
  const baseCents = notionalCents(o.qty, openInt);
  const frictionCents = Math.abs(grossCents - baseCents);
  const reservedCents = o.grossCents + o.frictionCents;

  const fillId = txnId([orderId, "fill", bar.t]);
  await A.col(A.COL.fills).doc(fillId).set({
    fillId, orderId, accountId: o.accountId, symbol: o.symbol, side: o.side,
    qty: o.qty,
    fillPriceInt: fillInt, fillPriceUsd: fromPrice(fillInt),
    barOpenUsd: bar.o, barOpenAt: bar.t,
    grossCents, frictionCents,
    barProvenance,
    fill_confirmed_at: A.FV.serverTimestamp(),
    ...A.envelope({ created_by: "ledger.recordFill" }),
  }, { merge: false });

  // Release the reservation, book the position at the traded price, and post
  // friction to its own account so the cost meter can read it directly.
  await post({
    accountId: o.accountId, kind: "fill",
    idParts: [orderId, "fill", bar.t],
    legs: [
      { account: ACCT.RESERVED, amountCents: -reservedCents, memo: "release reservation" },
      { account: ACCT.CASH, amountCents: reservedCents - baseCents - frictionCents, memo: "unspent reservation returned" },
      { account: ACCT.POSITIONS, amountCents: baseCents, memo: `${o.symbol} at bar open` },
      { account: ACCT.FRICTION, amountCents: frictionCents, memo: `slippage ${o.slippageBps}bp` },
    ],
    meta: { orderId, fillId, symbol: o.symbol, barOpenAt: bar.t },
  });

  await oref.set({ status: "filled", fillId, first_eligible_bar_at: bar.t }, { merge: true });

  await A.col(A.COL.positions).doc(`${o.accountId}_${o.symbol}`).set({
    accountId: o.accountId, symbol: o.symbol,
    open: true, qty: o.qty,
    costBasisCents: baseCents,
    entryPriceUsd: fromPrice(fillInt),
    openedAt: bar.t, openOrderId: orderId,
    /* Carried from the order so that when this position closes, its realized
       result can be attributed to the variant that chose it. Real trades are
       the scarcest and most expensive evidence the system will ever get; not
       tagging them meant they taught it nothing. */
    variantId: o.variantId || "baseline",
    entrySlippageBps: o.slippageBps || 0,
    updated_at: A.FV.serverTimestamp(),
  }, { merge: true });

  return { orderId, fillId, fillPriceUsd: fromPrice(fillInt), frictionCents, status: "filled" };
}

/** Close a position at an eligible bar's open plus adverse slippage. */
async function closePosition({ accountId, symbol, bar, slippageBps, reason, barProvenance }) {
  const pref = A.col(A.COL.positions).doc(`${accountId}_${symbol}`);
  const snap = await pref.get();
  if (!snap.exists || !snap.data().open) return { symbol, noop: true, reason: "no open position" };
  const p = snap.data();

  const openInt = toPrice(bar.o);
  const fillInt = openInt - Math.round((openInt * slippageBps) / 10000);   // selling: adverse is down
  const proceedsCents = notionalCents(p.qty, fillInt);
  const baseCents = notionalCents(p.qty, openInt);
  const frictionCents = Math.abs(baseCents - proceedsCents);
  const realized = baseCents - p.costBasisCents;

  const exitId = txnId([accountId, symbol, "exit", bar.t]);
  await post({
    accountId, kind: "close",
    idParts: [symbol, "exit", bar.t],
    legs: [
      { account: ACCT.POSITIONS, amountCents: -p.costBasisCents, memo: `close ${symbol} at cost` },
      { account: ACCT.CASH, amountCents: baseCents - frictionCents, memo: "proceeds net of friction" },
      { account: ACCT.FRICTION, amountCents: frictionCents, memo: `exit slippage ${slippageBps}bp` },
      { account: ACCT.REALIZED_PL, amountCents: -realized, memo: `realized P&L ${symbol}` },
    ],
    meta: { symbol, exitId, reason, barOpenAt: bar.t, barProvenance },
  });

  const entryUsd = Number(p.entryPriceUsd) || 0;
  const exitUsd = fromPrice(fillInt);
  const grossBps = entryUsd > 0 ? ((exitUsd - entryUsd) / entryUsd) * 1e4 : 0;
  const costBps = (Number(p.entrySlippageBps) || 0) + (Number(slippageBps) || 0);

  await pref.set({
    open: false, closedAt: bar.t, closeReason: reason,
    exitPriceUsd: exitUsd, realizedCents: realized,
    grossBps: Number(grossBps.toFixed(2)),
    costBps: Number(costBps.toFixed(2)),
    netBps: Number((grossBps - costBps).toFixed(2)),
    updated_at: A.FV.serverTimestamp(),
  }, { merge: true });

  return { symbol, realizedCents: realized, realizedUsd: fromCents(realized),
           exitPriceUsd: exitUsd, frictionCents,
           variantId: p.variantId || "baseline",
           openedDate: String(p.openedAt || "").slice(0, 10),
           heldDays: p.openedAt ? (Date.parse(bar.t) - Date.parse(p.openedAt)) / 864e5 : null,
           grossBps: Number(grossBps.toFixed(2)),
           costBps: Number(costBps.toFixed(2)),
           netBps: Number((grossBps - costBps).toFixed(2)) };
}

/* ── THE COST METER ────────────────────────────────────────────────────── */
/* The research question in one number. At ~400% monthly one-sided turnover a
   mega-cap book spends ~2.4%/yr on frictions and a mid-cap book ~9.6%. If
   cumulative friction exceeds cumulative gross edge, the strategy does not
   work no matter how good the dashboard looks. This is surfaced on every
   panel and in the portfolio header, deliberately. */
async function costMeter(accountId) {
  const b = await balances(accountId);
  const frictionCents = b.cents[ACCT.FRICTION] || 0;
  const realizedCents = b.cents[ACCT.REALIZED_PL] ? -b.cents[ACCT.REALIZED_PL] : 0;
  const grossCents = realizedCents + frictionCents;   // realized is already net of friction
  const start = (await A.col(A.COL.accounts).doc(accountId).get()).data() || {};
  const startCents = start.startingNavCents || 0;
  return {
    accountId,
    frictionUsd: fromCents(frictionCents),
    grossEdgeUsd: fromCents(grossCents),
    netRealizedUsd: fromCents(realizedCents),
    coverageRatio: frictionCents > 0 ? Number((grossCents / frictionCents).toFixed(2)) : null,
    frictionPctOfNav: startCents ? Number(((frictionCents / startCents) * 100).toFixed(3)) : null,
    verdict: grossCents > frictionCents ? "gross edge exceeds frictions"
           : grossCents > 0 ? "positive gross, frictions dominate"
           : "no gross edge",
  };
}

module.exports = {
  releaseOrder,
  ACCT, CENTS, PRICE_SCALE,
  toCents, fromCents, toPrice, fromPrice, notionalCents,
  txnId, assertBalanced,
  openAccount, post, balances,
  proposeOrder, approveOrder, rejectOrder, recordFill, closePosition,
  costMeter,
};
