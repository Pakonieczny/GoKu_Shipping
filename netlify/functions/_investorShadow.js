/* Investor_AI — feasible, provenance-bound full-information shadow simulator. */
"use strict";

const crypto = require("crypto");
const A = require("./_investorAdmin");
const S = require("./_investorSignal");
const M = require("./_investorMarket");
const V = require("./_investorVariants");
const R = require("./_investorRisk");
const I = require("./_investorIntelligence");
const T = require("./_investorTemporal");
const AL = require("./_investorAllocator");
const DF = require("./_investorDecisionFeedback");
const STRATEGY = require("./_investorStrategy");
const CA = require("./_investorCorporateActions");

const OPEN = A.COL.shadowOpen;
const CLOSED = A.COL.shadowClosed;
const STATS = A.COL.shadowDays;
const ACCOUNTS = A.COL.shadowAccounts;
const SIMULATOR_VERSION = "self-financing-counterfactual-v11-pending-exposure-whole-shares";
const DISCOUNT_GAMMA = 0.988; // trading-session weighting; asymptotic ESS ~= 166
const PH_DELTA_FRAC = 0.25;
const PH_LAMBDA_SD = 12;

function stable(value) {
  if (Array.isArray(value)) return value.map(stable);
  if (value && typeof value === "object") {
    return Object.fromEntries(Object.keys(value).sort().map((k) => [k, stable(value[k])]));
  }
  return value;
}
function sha(value) {
  return crypto.createHash("sha256").update(typeof value === "string" ? value : JSON.stringify(stable(value))).digest("hex");
}

/** A result from another universe, policy, feed, or simulator is another experiment. */
function experimentIdentity(ctx = {}) {
  const p = ctx.marketIdentity || ctx.marketProvenance || {};
  const identity = {
    simulatorVersion: SIMULATOR_VERSION,
    universeHash: ctx.universeHash || null,
    strategyHash: ctx.strategyHash || null,
    variantsHash: ctx.variantsHash || V.variantsHash(),
    buildCommit: ctx.buildCommit || process.env.COMMIT_REF || process.env.DEPLOY_ID || "local",
    intelligenceConfigHash: ctx.intelligenceConfigHash
      || sha(ctx.intelligenceConfig == null ? null : ctx.intelligenceConfig),
    provider: p.provider || ctx.providerId || null,
    feed: p.feed || ctx.feed || null,
    adjustment: p.adjustment || ctx.adjustment || null,
  };
  return { identity, experimentHash: sha(identity) };
}
function assertExperiment(ctx = {}) {
  if (/^[a-f0-9]{64}$/.test(String(ctx.experimentHash || ""))) {
    return { identity: ctx.experimentIdentity || null, experimentHash: ctx.experimentHash };
  }
  const out = experimentIdentity(ctx);
  for (const k of ["universeHash", "strategyHash", "variantsHash"]) {
    if (!/^[a-f0-9]{64}$/.test(String(out.identity[k] || ""))) {
      throw new Error(`shadow experiment missing valid ${k}`);
    }
  }
  if (!out.identity.provider || !out.identity.adjustment) {
    throw new Error("shadow experiment missing provider/adjustment identity");
  }
  return out;
}
function provenanceFor(ctx, symbol) {
  return (ctx.marketProvenanceBySymbol && ctx.marketProvenanceBySymbol[symbol])
    || ctx.marketProvenance || null;
}
function validProvenance(p) {
  return !!(p && p.provider && /^[a-f0-9]{64}$/.test(String(p.sourceSha256 || "")));
}
function openId(experimentHash, variantId, symbol) {
  return `${experimentHash.slice(0, 20)}_${variantId}_${symbol}`;
}
function closedId(experimentHash, variantId, symbol, openedAt) {
  return sha([experimentHash, variantId, symbol, openedAt].join("|")).slice(0, 40);
}
function dayDocId(experimentHash, variantId, date) {
  return `${experimentHash.slice(0, 20)}_${variantId}_${date}`;
}
function accountDocId(experimentHash, variantId) {
  return `${experimentHash.slice(0, 20)}_${variantId}`;
}
function initialAccount(experimentHash, variantId, startingNavUsd) {
  const nav = Math.max(1, Number(startingNavUsd) || 100000);
  return {
    experimentHash, variantId, simulatorVersion: SIMULATOR_VERSION,
    startingNavUsd: nav, cashUsd: nav, equityUsd: nav, highWaterUsd: nav,
    drawdownPct: 0, realizedPnlUsd: 0, cumulativeCostsUsd: 0,
    dayCostDate: null, dayCostsUsd: 0, lastCompleteDate: null,
    ...A.envelope({ created_by: "shadow.initialAccount" }),
  };
}
async function ensureAccount(experimentHash, variantId, startingNavUsd = 100000) {
  const ref = A.col(ACCOUNTS).doc(accountDocId(experimentHash, variantId));
  return A.runTransaction(async (tx) => {
    const snap = await tx.get(ref);
    if (snap.exists) return { id: ref.id, ...snap.data() };
    const row = initialAccount(experimentHash, variantId, startingNavUsd);
    tx.set(ref, row);
    return { id: ref.id, ...row };
  });
}
function accountCostPatch(account, date, costUsd) {
  const sameDay = account.dayCostDate === date;
  return {
    dayCostDate: date,
    dayCostsUsd: (sameDay ? Number(account.dayCostsUsd) || 0 : 0) + Math.max(0, Number(costUsd) || 0),
    cumulativeCostsUsd: (Number(account.cumulativeCostsUsd) || 0) + Math.max(0, Number(costUsd) || 0),
  };
}

/** Fill and cash debit are one transaction. A price gap cannot create cash. */
async function fillPendingAtomic(doc, pending, eligible, provenance, date) {
  const accountRef = A.col(ACCOUNTS).doc(accountDocId(pending.experimentHash, pending.variantId));
  return A.runTransaction(async (tx) => {
    const [openSnap, accountSnap] = await Promise.all([tx.get(doc.ref), tx.get(accountRef)]);
    if (!openSnap.exists || openSnap.data().pendingEntry !== true) return { duplicate: true };
    const account = accountSnap.exists ? accountSnap.data()
      : initialAccount(pending.experimentHash, pending.variantId, pending.startingNavUsd);
    const price = Number(eligible.bar && eligible.bar.o);
    const slipBps = Math.max(0, Number(pending.entrySlippageBps) || 0);
    const unitDebit = price * (1 + slipBps / 1e4);
    const requestedQty = Math.max(0, Math.floor(Number(pending.qty) || 0));
    const qty = unitDebit > 0
      ? Math.min(requestedQty, Math.floor(Math.max(0, Number(account.cashUsd) || 0) / unitDebit)) : 0;
    const notionalUsd = qty * price;
    if (!(notionalUsd >= 100) || qty < 1) {
      tx.delete(doc.ref);
      if (!accountSnap.exists) tx.set(accountRef, account);
      return { cancelled: true, reason: "insufficient self-financing cash at eligible fill" };
    }
    const entryCostUsd = notionalUsd * slipBps / 1e4;
    const cashDebitUsd = notionalUsd + entryCostUsd;
    const equityBeforeFillUsd = Math.max(1, Number(account.equityUsd) || Number(account.startingNavUsd));
    const patch = {
      pendingEntry: false, entryPrice: price, filledAt: eligible.barOpenAt,
      entryMarketProvenance: provenance, entryEligibleAfterMs: eligible.availableFromMs,
      markDate: date, dayStartPrice: price, lastMarkPrice: price,
      lastMarkAt: eligible.barOpenAt, lastMarkProvenance: provenance,
      qty, notionalUsd: Number(notionalUsd.toFixed(2)),
      entryCostUsd: Number(entryCostUsd.toFixed(6)),
      entryCashDebitUsd: Number(cashDebitUsd.toFixed(6)),
      equityBeforeFillUsd: Number(equityBeforeFillUsd.toFixed(6)),
      weightAtEntry: notionalUsd / equityBeforeFillUsd,
    };
    tx.set(doc.ref, patch, { merge: true });
    tx.set(accountRef, {
      ...(accountSnap.exists ? {} : account),
      dayStartDate: account.dayStartDate === date ? account.dayStartDate : date,
      dayStartEquityUsd: account.dayStartDate === date
        ? Number(account.dayStartEquityUsd) : Number(equityBeforeFillUsd.toFixed(6)),
      cashUsd: Number(((Number(account.cashUsd) || 0) - cashDebitUsd).toFixed(6)),
      equityUsd: Number(Math.max(0, equityBeforeFillUsd - entryCostUsd).toFixed(6)),
      ...accountCostPatch(account, date, entryCostUsd),
      updatedAt: A.FV.serverTimestamp(),
    }, { merge: true });
    return { duplicate: false, patch };
  });
}

function riskConfig(params, ctx) {
  return {
    ...STRATEGY,
    parameters: params,
    portfolioControls: ctx.portfolioControls || STRATEGY.portfolioControls,
  };
}
/* Pending shadow entries are exposure the arm is committed to; they used to
   reduce cash but vanish from position count, sector, cluster and correlation
   when the next cycle rebuilt the book — the same cross-cycle cap leak the
   executable ledger had. A pending row enters the book at its requested
   whole-share quantity and signal price until the eligible fill replaces it. */
function positionRows(openRows) {
  return openRows.filter((p) => Number(p.qty) > 0).map((p) => ({
    open: true, symbol: p.symbol, qty: Number(p.qty),
    entryPriceUsd: Number(p.pendingEntry ? p.signalPrice : p.entryPrice), sector: p.sector,
    pending: p.pendingEntry === true,
  }));
}
function updateBook(book, { symbol, sector, usd, price, cluster }) {
  book.count += 1;
  book.grossUsd += usd;
  book.grossPct = book.navUsd > 0 ? 100 * book.grossUsd / book.navUsd : 0;
  const pct = book.navUsd > 0 ? 100 * usd / book.navUsd : 0;
  book.bySectorPct[sector] = (book.bySectorPct[sector] || 0) + pct;
  book.byClusterPct[cluster] = (book.byClusterPct[cluster] || 0) + pct;
  book.rows.push({ symbol, sector, cluster, valueUsd: usd, mark: price, marked: true });
}

function dynamicCorrelationMap(symbol, book, dailySeriesBySymbol, dailyProvenanceBySymbol, asOfMs) {
  const out = {};
  for (const held of book.rows || []) out[held.symbol] = T.pairwiseCorrelation(
    dailySeriesBySymbol && dailySeriesBySymbol[symbol] || [],
    dailySeriesBySymbol && dailySeriesBySymbol[held.symbol] || [], asOfMs, {
      provenanceA: dailyProvenanceBySymbol && dailyProvenanceBySymbol[symbol] || null,
      provenanceB: dailyProvenanceBySymbol && dailyProvenanceBySymbol[held.symbol] || null,
      requireProvenance: true,
    });
  return out;
}

/**
 * Score every frozen arm against the same opportunity union, then construct a
 * feasible cash/exposure-constrained portfolio in deterministic utility order.
 * Calibration is not required to generate research outcomes; it is required
 * later to promote them or admit live risk.
 */
async function evaluateEntries(ctx) {
  const exp = assertExperiment(ctx);
  const nowMs = Number(ctx.decisionAtMs) || Date.now();
  const ranks = ctx.ranks || {}, zBySymbol = ctx.zBySymbol || {};
  const opportunitySymbols = [...new Set([
    ...Object.keys(ranks),
    ...Object.values(ctx.signalContexts || {}).flatMap((x) => Object.keys(x && x.ranks || {})),
  ])].sort();
  const openSnap = await A.col(OPEN).get();
  const existing = [];
  openSnap.forEach((d) => {
    const r = d.data();
    if (r.experimentHash === exp.experimentHash) existing.push({ id: d.id, ...r });
  });
  const isOpen = new Set(existing.map((p) => `${p.variantId}_${p.symbol}`));
  const byVariant = {}, opened = [];
  let evaluated = 0;

  for (const variant of V.VARIANTS) {
    const params = { ...V.configFor(variant.id), requireCalibratedEdge: false };
    const rcfg = riskConfig(params, ctx);
    const mine = existing.filter((p) => p.variantId === variant.id);
    const account = await ensureAccount(exp.experimentHash, variant.id,
      Math.max(1, Number(ctx.shadowNavUsd) || 100000));
    const navUsd = Math.max(1, Number(account.equityUsd) || Number(account.startingNavUsd));
    const marks = ctx.lastPrice || {};
    const book = R.summarise(positionRows(mine), marks, S.sectorOf, navUsd);
    book.navUsd = navUsd;
    const reserved = mine.filter((p) => p.pendingEntry).reduce((a, p) => a + (Number(p.notionalUsd) || 0), 0);
    let cashUsd = Math.max(0, (Number(account.cashUsd) || 0) - reserved);
    byVariant[variant.id] = { considered: 0, opened: 0, blocked: {}, feasibleRejected: 0,
      equityUsd: Number(navUsd.toFixed(2)), cashAvailableUsd: Number(cashUsd.toFixed(2)) };
    const candidates = [];

    for (const symbol of opportunitySymbols) {
      const signalContext = ctx.signalContexts && ctx.signalContexts[params.signalWindow || 12];
      const variantRanks = signalContext && signalContext.ranks || ranks;
      const variantZ = signalContext && signalContext.zBySymbol || zBySymbol;
      const variantCrowd = S.sectorCrowding(variantRanks, S.sectorOf,
        params.entryRank == null ? S.ENTRY_RANK : params.entryRank);
      const z = variantZ[symbol];
      if (!z || isOpen.has(`${variant.id}_${symbol}`)) continue;
      evaluated += 1; byVariant[variant.id].considered += 1;
      const meta = (ctx.meta && ctx.meta[symbol]) || {};
      const ew = (ctx.earnings && ctx.earnings[symbol]) || {};
      const result = S.evaluateCandidate({
        symbol, rank: variantRanks[symbol], zStat: z,
        quality: ctx.quality && ctx.quality[symbol], advUsd: meta.advUsd || 0,
        earningsDates: ew.dates || [], earningsEstimated: !!ew.estimated,
        earningsUncertaintyDays: Number(ew.uncertaintyDays) || null,
        nowMs, cause: (ctx.causeBySymbol && ctx.causeBySymbol[symbol]) || S.CAUSE.PENDING,
        coverage: ctx.coverageBySymbol && ctx.coverageBySymbol[symbol],
        attentionScore: ctx.attentionBySymbol && ctx.attentionBySymbol[symbol],
        vixNorm: ctx.regime && ctx.regime.vixNorm,
        cor3m: ctx.regime && ctx.regime.cor3m,
        sectorTailFraction: variantCrowd && variantCrowd.fractionInTail
          ? (variantCrowd.fractionInTail[S.sectorOf(symbol)] ?? 0) : 0,
        session: ctx.session, cfg: params, position: null,
        intelligence: ctx.intelligenceBySymbol && ctx.intelligenceBySymbol[symbol],
        historyContext: (ctx.historyCtx && ctx.historyCtx[symbol]) || null,
        reversion: (ctx.reversion && ctx.reversion[symbol]) || null,
        turnoverPctile: ctx.turnoverPctile && ctx.turnoverPctile[symbol],
      });
      if (!result.pass) {
        const why = result.blockedBy[0] || "unknown";
        byVariant[variant.id].blocked[why] = (byVariant[variant.id].blocked[why] || 0) + 1;
        continue;
      }
      const price = Number(ctx.lastPrice && ctx.lastPrice[symbol]);
      const provenance = provenanceFor(ctx, symbol);
      if (!(price > 0) || !validProvenance(provenance)) {
        const why = !(price > 0) ? "missing_price" : "missing_market_provenance";
        byVariant[variant.id].blocked[why] = (byVariant[variant.id].blocked[why] || 0) + 1;
        continue;
      }
      const hc = (ctx.historyCtx && ctx.historyCtx[symbol]) || {};
      const sizing = R.positionSizeUsd({
        navUsd, atrPct: hc.atrPct,
        expectedShortfall5dPct: hc.expectedShortfall5dPct,
        overnightGapEsPct: hc.overnightGapEsPct,
        signalScaler: result.sizing.combined, cfg: rcfg,
      });
      const utility = Number(result.cost && result.cost.expectedGrossBps || 0)
        - Number(result.cost && result.cost.requiredBps || 0);
      candidates.push({ symbol, result, price, provenance, sizing, utility,
        rank: variantRanks[symbol], sector: S.sectorOf(symbol), meta });
    }

    candidates.sort((a, b) => b.utility - a.utility
      || (a.rank - b.rank) || a.symbol.localeCompare(b.symbol));
    for (const c of candidates) {
      const dynamicCorrelations = dynamicCorrelationMap(c.symbol, book, ctx.dailySeriesBySymbol,
        ctx.dailyProvenanceBySymbol, nowMs);
      const add = R.checkAdd({ symbol: c.symbol, sector: c.sector,
        proposedUsd: c.sizing.usd, book, navUsd, cashUsd, cfg: rcfg, dynamicCorrelations });
      const permittedNotionalUsd = add.allow ? c.sizing.usd : (add.allowTrimmed ? add.permittedUsd : 0);
      const wholeQty = c.price > 0 ? Math.floor(permittedNotionalUsd / c.price) : 0;
      const wholeNotionalUsd = wholeQty * c.price;
      const notionalUsd = wholeNotionalUsd;
      if (!(notionalUsd >= 100) || wholeQty < 1) {
        byVariant[variant.id].feasibleRejected += 1;
        const why = add.blockedBy[0] || (wholeQty < 1 ? "zero_whole_shares" : "minimum_notional");
        byVariant[variant.id].blocked[why] = (byVariant[variant.id].blocked[why] || 0) + 1;
        continue;
      }
      const slip = M.slippageBps({ advUsd: c.meta.advUsd || 0,
        grade: (ctx.quality[c.symbol] || {}).grade || "F",
        wideSpreadWindow: !!(ctx.session && ctx.session.wideSpreadWindow),
        vixNorm: ctx.regime && ctx.regime.vixNorm });
      const intelligence = ctx.intelligenceBySymbol && ctx.intelligenceBySymbol[c.symbol];
      const temporalObservation = intelligence && intelligence.temporalContext
        && intelligence.temporalContext.shadowCalibration;
      const contextObservation = {
        schemaVersion: "shadow-context-outcome-v1",
        mode: "measurement_only", calibrationOutputAffectsDecision: false,
        observedAtMs: nowMs,
        features: {
          residualRank: Number((ctx.signalContexts && ctx.signalContexts[params.signalWindow || 12]
            && ctx.signalContexts[params.signalWindow || 12].ranks[c.symbol]) ?? ranks[c.symbol]),
          residualZ: Number(c.result.z),
          cumulativeResidualBps: Number(c.result.cumResidualBps),
          intelligenceAdverseRisk: Number(c.result.intelligencePolicy
            && c.result.intelligencePolicy.adverseRiskScore) || 0,
          temporalRisk: Number(c.result.intelligencePolicy && c.result.intelligencePolicy.temporalPolicy
            && c.result.intelligencePolicy.temporalPolicy.riskScore) || 0,
          ...Object.fromEntries(Object.entries(temporalObservation && temporalObservation.features || {})
            .map(([key, value]) => ["temporal_" + key, Number(value) || 0])),
        },
      };
      const row = {
        experimentHash: exp.experimentHash, experimentIdentity: exp.identity,
        simulatorVersion: SIMULATOR_VERSION, variantId: variant.id, symbol: c.symbol,
        cycleId: ctx.cycleId, pendingEntry: true, decisionAtMs: nowMs,
        decisionMarketProvenance: c.provenance,
        signalPrice: c.price, openedAt: new Date(nowMs).toISOString(),
        openedDate: ctx.session.date, entryPrice: null,
        /* Whole shares, like the executable ledger. A fractional shadow share
           made the counterfactual book cheaper to enter than the real one. */
        qty: wholeQty, notionalUsd: Number(wholeNotionalUsd.toFixed(2)),
        weightAtEntry: wholeNotionalUsd / navUsd, entrySlippageBps: slip,
        rank: c.result.rank, z: c.result.z, cumResidualBps: c.result.cumResidualBps,
        cause: (ctx.causeBySymbol && ctx.causeBySymbol[c.symbol]) || S.CAUSE.PENDING,
        intelligenceDossierHash: ctx.intelligenceBySymbol && ctx.intelligenceBySymbol[c.symbol]
          ? ctx.intelligenceBySymbol[c.symbol].dossierHash : null,
        sector: c.sector, cluster: add.cluster, sizeMult: c.result.sizing.combined,
        regimeAtEntry: ctx.regime ? { vixNorm: Number(ctx.regime.vixNorm) || null,
          cor3m: Number(ctx.regime.cor3m) || null } : null,
        sizing: c.sizing, cost: c.result.cost, dynamicCorrelation: add.dynamicCorrelation,
        contextObservation,
        ...A.envelope({ created_by: "shadow.evaluateEntries" }),
      };
      await A.col(OPEN).doc(openId(exp.experimentHash, variant.id, c.symbol)).set(row);
      isOpen.add(`${variant.id}_${c.symbol}`);
      cashUsd -= notionalUsd;
      updateBook(book, { symbol: c.symbol, sector: c.sector, usd: notionalUsd,
        price: c.price, cluster: add.cluster });
      opened.push({ variantId: variant.id, symbol: c.symbol, pending: true, notionalUsd,
        accountEquityUsd: navUsd });
      byVariant[variant.id].opened += 1;
    }
  }
  return { opened: opened.length, evaluated, byVariant, experimentHash: exp.experimentHash,
    detail: opened.slice(0, 80) };
}

/** Atomic upsert of one outcome into one policy-day portfolio return. */
async function accumulate(variantId, row, experimentHashArg = null) {
  if (!variantId || !row) return { ignored: true };
  const experimentHash = experimentHashArg || row.experimentHash;
  if (!/^[a-f0-9]{64}$/.test(String(experimentHash || ""))) {
    throw new Error("shadow accumulate requires experimentHash");
  }
  const date = String(row.closedDate || row.date || row.openedDate || "").slice(0, 10);
  if (!/^\d{4}-\d{2}-\d{2}$/.test(date)) throw new Error("shadow accumulate requires ISO trading date");
  const outcomeId = String(row.outcomeId || sha(stable(row))).slice(0, 120);
  const ref = A.col(STATS).doc(dayDocId(experimentHash, variantId, date));
  return A.runTransaction(async (tx) => {
    const snap = await tx.get(ref);
    const old = snap.exists ? snap.data() : {};
    /* A finalized session is immutable evidence. A late/manual cycle may
       still manage risk, but it may not rewrite the observation already used
       by selection or calibration. */
    if (old.dailyFinalized === true) {
      return { ignored: true, reason: "daily_observation_finalized", outcomeId, date };
    }
    const outcomes = Array.isArray(old.outcomes) ? old.outcomes.slice() : [];
    const next = {
      id: outcomeId,
      netBps: Number(row.netContributionBps != null ? row.netContributionBps : row.netBps) || 0,
      grossBps: Number(row.grossContributionBps != null ? row.grossContributionBps : row.grossBps) || 0,
      costBps: Number(row.costContributionBps != null ? row.costContributionBps : row.costBps) || 0,
      unresolved: !!row.unresolved,
      worstCaseNetBps: Number(row.worstCaseNetBps) || 0,
    };
    const at = outcomes.findIndex((o) => o.id === outcomeId);
    if (at >= 0) outcomes[at] = next; else outcomes.push(next);
    outcomes.sort((a, b) => a.id.localeCompare(b.id));
    const included = outcomes.filter((o) => !o.unresolved);
    const sum = (k, arr = included) => arr.reduce((a, o) => a + (Number(o[k]) || 0), 0);
    const payload = {
      experimentHash, variantId, date, simulatorVersion: SIMULATOR_VERSION,
      outcomes, nOutcomes: included.length,
      portfolioNetBps: Number(sum("netBps").toFixed(8)),
      portfolioGrossBps: Number(sum("grossBps").toFixed(8)),
      portfolioCostBps: Number(sum("costBps").toFixed(8)),
      unresolvedCount: outcomes.filter((o) => o.unresolved).length,
      worstCaseNetBps: Number((sum("netBps") + sum("worstCaseNetBps", outcomes.filter((o) => o.unresolved))).toFixed(8)),
      completePortfolioDay: false,
      marksComplete: false,
      provisional: true,
      excludedReason: "session has not reached buffered daily finalization",
      updatedAt: A.FV.serverTimestamp(),
      ...(snap.exists ? {} : A.envelope({ created_by: "shadow.accumulate" })),
    };
    tx.set(ref, payload, { merge: true });
    return { duplicate: at >= 0, outcomeId, date };
  });
}

async function closeAtomic(doc, p, closeRow, financial = null) {
  const cref = A.col(CLOSED).doc(closedId(p.experimentHash, p.variantId, p.symbol, p.openedAt));
  const accountRef = A.col(ACCOUNTS).doc(accountDocId(p.experimentHash, p.variantId));
  return A.runTransaction(async (tx) => {
    const [open, closed, accountSnap] = await Promise.all([
      tx.get(doc.ref), tx.get(cref), tx.get(accountRef),
    ]);
    if (closed.exists) { if (open.exists) tx.delete(doc.ref); return { duplicate: true, id: cref.id }; }
    if (!open.exists) return { duplicate: true, id: cref.id };
    tx.set(cref, closeRow);
    tx.delete(doc.ref);
    if (financial) {
      const account = accountSnap.exists ? accountSnap.data()
        : initialAccount(p.experimentHash, p.variantId, p.startingNavUsd);
      const credit = Math.max(0, Number(financial.cashCreditUsd) || 0);
      const realized = Number(financial.realizedPnlUsd) || 0;
      const exitCost = Math.max(0, Number(financial.exitCostUsd) || 0);
      tx.set(accountRef, {
        ...(accountSnap.exists ? {} : account),
        dayStartDate: account.dayStartDate === closeRow.closedDate
          ? account.dayStartDate : closeRow.closedDate,
        dayStartEquityUsd: account.dayStartDate === closeRow.closedDate
          ? Number(account.dayStartEquityUsd) : Number(account.equityUsd),
        cashUsd: Number(((Number(account.cashUsd) || 0) + credit).toFixed(6)),
        realizedPnlUsd: Number(((Number(account.realizedPnlUsd) || 0) + realized).toFixed(6)),
        ...accountCostPatch(account, closeRow.closedDate, exitCost),
        updatedAt: A.FV.serverTimestamp(),
      }, { merge: true });
    }
    return { duplicate: false, id: cref.id };
  });
}

/**
 * Value each frozen policy as an actual self-financing portfolio. Only a day
 * with trustworthy marks for every filled position becomes learning evidence.
 * Pending decisions hold no asset and reserve cash only for feasibility.
 */
async function markAccounts(ctx, exp) {
  if (!(ctx.session && ctx.session.tradingDay && ctx.session.dailyFinalized === true
      && /^\d{4}-\d{2}-\d{2}$/.test(ctx.session.date))) {
    return { marked: 0, complete: 0, skipped: "daily_session_not_finalized" };
  }
  const date = ctx.session.date;
  const openSnap = await A.col(OPEN).get();
  const rows = openSnap.docs.map((d) => d.data())
    .filter((p) => p.experimentHash === exp.experimentHash && !p.pendingEntry && Number(p.qty) > 0);
  const result = { marked: 0, complete: 0, incomplete: 0, byVariant: {} };
  for (const variant of V.VARIANTS) {
    const account = await ensureAccount(exp.experimentHash, variant.id,
      Math.max(1, Number(ctx.shadowNavUsd) || 100000));
    const mine = rows.filter((p) => p.variantId === variant.id);
    const missing = [], markSources = [];
    let marketValueUsd = 0;
    for (const p of mine) {
      if (p.corporateActionPending) {
        missing.push(`${p.symbol}:corporate_action_pending`); continue;
      }
      const price = Number(ctx.lastPrice && ctx.lastPrice[p.symbol]);
      const provenance = provenanceFor(ctx, p.symbol);
      if (!(price > 0) || !validProvenance(provenance)) {
        missing.push(p.symbol); continue;
      }
      marketValueUsd += price * Number(p.qty);
      markSources.push({ symbol: p.symbol, price: Number(price.toFixed(8)),
        provider: provenance.provider, feed: provenance.feed || null,
        adjustment: provenance.adjustment || null,
        barOpenAt: provenance.barOpenAt || null,
        sourceSha256: provenance.sourceSha256 });
    }
    markSources.sort((a, b) => a.symbol.localeCompare(b.symbol));
    const markSetSha256 = sha(stable(markSources));
    const dayRef = A.col(STATS).doc(dayDocId(exp.experimentHash, variant.id, date));
    if (missing.length) {
      await dayRef.set({
        experimentHash: exp.experimentHash, variantId: variant.id, date,
        simulatorVersion: SIMULATOR_VERSION, completePortfolioDay: false,
        dailyFinalized: true, provisional: false, marksComplete: false,
        markSources, markSetSha256,
        finalizedAt: A.FV.serverTimestamp(),
        excludedReason: "missing trustworthy mark for every open position",
        missingSymbols: [...new Set(missing)].sort(), updatedAt: A.FV.serverTimestamp(),
      }, { merge: true });
      result.incomplete += 1;
      result.byVariant[variant.id] = { complete: false, missingSymbols: missing };
      continue;
    }
    const cashUsd = Number(account.cashUsd) || 0;
    const equityUsd = Math.max(0, cashUsd + marketValueUsd);
    const sameDay = account.dayStartDate === date;
    const dayStartEquityUsd = sameDay
      ? Math.max(1, Number(account.dayStartEquityUsd) || Number(account.equityUsd) || 1)
      : Math.max(1, Number(account.equityUsd) || Number(account.startingNavUsd) || 1);
    /* How many trading sessions this return actually compounds across.
     *
     * A day on which any position lacked a trustworthy mark writes no account
     * update, so the next complete day starts from the last COMPLETE day's
     * equity — its return spans the gap while still being one row in a series
     * that calibration slices by position and the Deflated Sharpe annualises
     * by sqrt(252). Left unmarked, an unchanged edge measured 33% stronger at
     * a 40% incomplete-day rate, and incomplete marks correlate with the
     * stressed sessions whose returns are worst. The equity below is real and
     * is always recorded; only the statistical admissibility of the ROW
     * depends on the span, so a single gap costs exactly one observation and
     * cannot cascade. */
    const priorDate = sameDay ? account.priorCompleteDate : account.lastCompleteDate;
    const sessionsSpanned = priorDate
      ? M.tradingSessionsBetween(priorDate, date) : 1;
    const spanKnown = Number.isFinite(Number(sessionsSpanned)) && Number(sessionsSpanned) > 0;
    const spanned = spanKnown ? Number(sessionsSpanned) : 1;
    const contiguous = !priorDate || (spanKnown && spanned === 1);
    const dayCostsUsd = account.dayCostDate === date ? Math.max(0, Number(account.dayCostsUsd) || 0) : 0;
    const portfolioNetBps = (equityUsd / dayStartEquityUsd - 1) * 1e4;
    const portfolioCostBps = dayCostsUsd / dayStartEquityUsd * 1e4;
    const portfolioGrossBps = portfolioNetBps + portfolioCostBps;
    const highWaterUsd = Math.max(Number(account.highWaterUsd) || 0, equityUsd);
    const drawdownPct = highWaterUsd > 0 ? 100 * (equityUsd / highWaterUsd - 1) : 0;
    await dayRef.set({
      experimentHash: exp.experimentHash, variantId: variant.id, date,
      simulatorVersion: SIMULATOR_VERSION,
      completePortfolioDay: contiguous,
      marksComplete: true,
      dailyFinalized: true,
      provisional: false,
      markSources,
      markSetSha256,
      missingSymbols: [],
      finalizedAt: A.FV.serverTimestamp(),
      priorCompleteDate: priorDate || null,
      sessionsSpanned: spanned,
      excludedReason: contiguous ? null : (spanKnown
        ? `return compounds across ${spanned} trading sessions and is not a daily observation`
        : "prior complete session could not be resolved"),
      accountingMethod: "self_financing_equity_return",
      dayStartEquityUsd: Number(dayStartEquityUsd.toFixed(6)),
      endEquityUsd: Number(equityUsd.toFixed(6)), cashUsd: Number(cashUsd.toFixed(6)),
      marketValueUsd: Number(marketValueUsd.toFixed(6)),
      portfolioNetBps: Number(portfolioNetBps.toFixed(8)),
      portfolioGrossBps: Number(portfolioGrossBps.toFixed(8)),
      portfolioCostBps: Number(portfolioCostBps.toFixed(8)),
      worstCaseNetBps: Number(portfolioNetBps.toFixed(8)),
      updatedAt: A.FV.serverTimestamp(),
      ...A.envelope({ created_by: "shadow.markAccounts" }),
    }, { merge: true });
    /* The account always advances: the equity is real whether or not the row
       is admissible evidence. This is what stops one missing mark from
       spanning every later row. */
    await A.col(ACCOUNTS).doc(accountDocId(exp.experimentHash, variant.id)).set({
      equityUsd: Number(equityUsd.toFixed(6)), highWaterUsd: Number(highWaterUsd.toFixed(6)),
      drawdownPct: Number(drawdownPct.toFixed(6)), lastCompleteDate: date,
      priorCompleteDate: sameDay ? (account.priorCompleteDate || null)
        : (account.lastCompleteDate || null),
      dayStartDate: date, dayStartEquityUsd: Number(dayStartEquityUsd.toFixed(6)),
      updatedAt: A.FV.serverTimestamp(),
    }, { merge: true });
    result.marked += 1;
    if (contiguous) result.complete += 1; else result.spanned = (result.spanned || 0) + 1;
    result.byVariant[variant.id] = { complete: contiguous, sessionsSpanned: spanned,
      equityUsd: Number(equityUsd.toFixed(2)),
      cashUsd: Number(cashUsd.toFixed(2)), drawdownPct: Number(drawdownPct.toFixed(3)),
      ...(contiguous ? {} : { excluded: "non_contiguous_return" }) };
  }
  return result;
}

async function evaluateExits(ctx) {
  const exp = assertExperiment(ctx);
  const snap = await A.col(OPEN).get();
  const docs = snap.docs.filter((d) => d.data().experimentHash === exp.experimentHash);
  const closed = [], unresolved = [];
  const nowMs = Number(ctx.decisionAtMs) || Date.now();

  if (ctx.session && ctx.session.tradingDay) {
    await Promise.all(V.VARIANTS.map((v) => accumulate(v.id, {
      experimentHash: exp.experimentHash, date: ctx.session.date,
      outcomeId: `${v.id}:cash:${ctx.session.date}`, netContributionBps: 0,
    })));
  }

  for (const doc of docs) {
    const p = doc.data();
    const params = { ...V.configFor(p.variantId), requireCalibratedEdge: false };
    /* A formation-horizon arm owns its corresponding residual rank for its
       entire frozen policy. Falling back to the incumbent's 12-bar rank here
       would let I/J enter on 6/24 bars but exit on a different signal. */
    const exitSignalContext = ctx.signalContexts
      && ctx.signalContexts[params.signalWindow || 12];
    const rank = exitSignalContext && exitSignalContext.ranks
      ? exitSignalContext.ranks[p.symbol]
      : ctx.ranks && ctx.ranks[p.symbol];
    const px = Number(ctx.lastPrice && ctx.lastPrice[p.symbol]);
    /* Holding age runs from the FILL bar, as it does in the executable
       ledger; counting from the decision instant aged a shadow position by
       the feed delay before it existed. */
    const heldDays = M.tradingDaysHeld(p.filledAt || p.openedAt, nowMs) ?? 0;
    const provenance = provenanceFor(ctx, p.symbol);
    const bars = (ctx.panel && ctx.panel[p.symbol]) || [];

    if (!p.pendingEntry && px > 0) {
      const corporateAction = CA.assessPositionMark({ position: p,
        currentPrice: px, currentProvenance: provenance });
      if (corporateAction.quarantine) {
        const pending = { ...corporateAction, status: "pending_operator_confirmation",
          detectedAtMs: corporateAction.detectedAtMs || nowMs, currentProvenance: provenance };
        p.corporateActionPending = pending;
        await doc.ref.set({ corporateActionPending: pending,
          updatedAt: A.FV.serverTimestamp() }, { merge: true });
        continue;
      }
    }

    if (p.pendingEntry) {
      if (!validProvenance(provenance)) continue;
      const eligible = M.firstEligibleBar(bars, {
        decisionAtMs: p.decisionAtMs, provider: provenance.provider, feed: provenance.feed,
        executionLatencyMs: params.executionLatencyMs || 60000,
      });
      if (eligible.bar && Number(eligible.bar.o) > 0) {
        const fill = await fillPendingAtomic(doc, p, eligible, provenance, ctx.session.date);
        if (fill.cancelled || fill.duplicate || !fill.patch) continue;
        Object.assign(p, fill.patch);
        await accumulate(p.variantId, {
          experimentHash: exp.experimentHash, date: ctx.session.date,
          outcomeId: `${doc.id}:mark:${ctx.session.date}`,
          netContributionBps: -(Number(p.entrySlippageBps) || 0) * Number(p.weightAtEntry || 0),
          grossContributionBps: 0,
          costContributionBps: (Number(p.entrySlippageBps) || 0) * Number(p.weightAtEntry || 0),
        });
      } else if (heldDays > 2) {
        await doc.ref.delete();
      } else continue;
    }
    /* A price without the exact provider-response hash is not a market mark.
       Missing-price aging may still resolve as an explicit stressed outcome,
       but no P/L, exit, or stored mark may consume an unattributed price. */
    if (px > 0 && !validProvenance(provenance)) continue;
    if (!(Number(p.entryPrice) > 0)) continue;

    if (rank == null || !(px > 0)) {
      if (heldDays < (params.maxHoldDays ?? 10) + 5) continue;
      const weight = Number(p.weightAtEntry) || 0;
      const row = { experimentHash: exp.experimentHash, simulatorVersion: SIMULATOR_VERSION,
        variantId: p.variantId, symbol: p.symbol, openedAt: p.openedAt,
        openedDate: p.openedDate, closedAt: new Date(nowMs).toISOString(),
        closedDate: ctx.session.date, heldDays: Number(heldDays.toFixed(2)),
        unresolved: true, excludeFromStats: true,
        contextObservation: p.contextObservation || null,
        measurementOutcome: { available: false, reason: "unresolved_symbol" },
        worstCaseNetBps: -10000 * weight,
        componentAttribution: DF.attributeClosed(p, { grossBps: -10000,
          netBps: -10000, costBps: Number(p.entrySlippageBps) || 0,
          exitPrice: 0, exitReason: "unresolved_total_loss_stress", heldDays }),
        exitReason: "symbol unavailable beyond hold horizon; outcome excluded and total-loss stress retained",
        ...A.envelope({ created_by: "shadow.evaluateExits" }) };
      const saved = await closeAtomic(doc, p, row, {
        cashCreditUsd: 0,
        realizedPnlUsd: -(Number(p.entryCashDebitUsd) || Number(p.notionalUsd) || 0),
        exitCostUsd: 0,
      });
      if (!saved.duplicate) {
        await accumulate(p.variantId, { ...row, outcomeId: saved.id }, exp.experimentHash);
        unresolved.push({ variantId: p.variantId, symbol: p.symbol });
      }
      continue;
    }

    let dayStartPrice = Number(p.dayStartPrice) || Number(p.entryPrice);
    if (p.markDate !== ctx.session.date) {
      dayStartPrice = Number(p.lastMarkPrice) || Number(p.entryPrice);
      await doc.ref.set({ markDate: ctx.session.date, dayStartPrice }, { merge: true });
      p.markDate = ctx.session.date; p.dayStartPrice = dayStartPrice;
    }
    const weight = Number(p.weightAtEntry) || 0;
    const dailyGross = dayStartPrice > 0 ? ((px - dayStartPrice) / dayStartPrice) * 1e4 * weight : 0;
    const entryCost = p.openedDate === ctx.session.date ? (Number(p.entrySlippageBps) || 0) * weight : 0;
    await accumulate(p.variantId, {
      experimentHash: exp.experimentHash, date: ctx.session.date,
      outcomeId: `${doc.id}:mark:${ctx.session.date}`,
      grossContributionBps: dailyGross, costContributionBps: entryCost,
      netContributionBps: dailyGross - entryCost,
    });
    await doc.ref.set({ lastMarkPrice: px,
      lastMarkAt: provenance && provenance.barOpenAt || new Date(nowMs).toISOString(),
      lastMarkProvenance: provenance }, { merge: true });

    const ew = (ctx.earnings && ctx.earnings[p.symbol]) || {};
    const soon = (ew.dates || []).map((d) => (Date.parse(d) - nowMs) / 864e5)
      .filter((d) => d >= 0).sort((a, b) => a - b)[0];
    const peak = Math.max(Number(p.peakPrice) || 0, px);
    if (peak > Number(p.peakPrice || 0)) await doc.ref.set({ peakPrice: peak }, { merge: true });
    let intent = p.exitIntent;
    if (!intent) {
      const intelligence = ctx.intelligenceBySymbol && ctx.intelligenceBySymbol[p.symbol];
      const intelligencePolicy = intelligence ? I.decisionPolicy({ coverage: intelligence.coverage,
        events: intelligence.events, temporalContext: intelligence.temporalContext,
        requireTemporalContext: false, asOfMs: nowMs,
        maxAgeHours: params.intelligenceMaxAgeHours,
        temporalMaxAgeHours: params.temporalMaxAgeHours }) : null;
      const signal = S.exitSignal(rank, heldDays, params, {
        mark: px, entry: p.entryPrice, peak, earningsInDays: soon == null ? null : soon,
        intelligencePolicy,
      });
      if (!signal.exit) continue;
      if (!validProvenance(provenance)) continue;
      intent = { decisionAtMs: nowMs, reason: signal.reason, kind: signal.kind || "signal",
        decisionMarketProvenance: provenance };
      await doc.ref.set({ exitIntent: intent }, { merge: true });
      continue;
    }
    if (!validProvenance(provenance)) continue;
    const eligible = M.firstEligibleBar(bars, { decisionAtMs: intent.decisionAtMs,
      provider: provenance.provider, feed: provenance.feed,
      executionLatencyMs: params.executionLatencyMs || 60000 });
    if (!eligible.bar || !(Number(eligible.bar.o) > 0)) continue;

    const meta = (ctx.meta && ctx.meta[p.symbol]) || {};
    const exitSlip = M.slippageBps({ advUsd: meta.advUsd || 0,
      grade: (ctx.quality && ctx.quality[p.symbol] || {}).grade || "F",
      wideSpreadWindow: !!(ctx.session && ctx.session.wideSpreadWindow),
      vixNorm: ctx.regime && ctx.regime.vixNorm });
    const exitPrice = Number(eligible.bar.o);
    const exitProceedsUsd = Math.max(0, Number(p.qty) || 0) * exitPrice;
    const exitCostUsd = exitProceedsUsd * exitSlip / 1e4;
    const cashCreditUsd = Math.max(0, exitProceedsUsd - exitCostUsd);
    const realizedPnlUsd = cashCreditUsd
      - (Number(p.entryCashDebitUsd) || (Number(p.qty) || 0) * Number(p.entryPrice));
    const grossBps = ((exitPrice - Number(p.entryPrice)) / Number(p.entryPrice)) * 1e4;
    const costBps = (Number(p.entrySlippageBps) || 0) + exitSlip;
    const netBps = grossBps - costBps;
    const exitDailyGross = dayStartPrice > 0 ? ((exitPrice - dayStartPrice) / dayStartPrice) * 1e4 * weight : 0;
    const dailyCost = exitSlip * weight + entryCost;
    const closeRow = {
      experimentHash: exp.experimentHash, experimentIdentity: p.experimentIdentity,
      simulatorVersion: SIMULATOR_VERSION, variantId: p.variantId, symbol: p.symbol,
      openedAt: p.openedAt, openedDate: p.openedDate,
      closedAt: new Date(nowMs).toISOString(), closedDate: ctx.session.date,
      heldDays: Number(heldDays.toFixed(2)), entryPrice: Number(p.entryPrice), exitPrice,
      qty: Number(p.qty), notionalUsd: Number(p.notionalUsd), weightAtEntry: weight,
      grossBps: Number(grossBps.toFixed(4)), costBps: Number(costBps.toFixed(4)),
      netBps: Number(netBps.toFixed(4)), exitReason: intent.reason, exitKind: intent.kind,
      exitProceedsUsd: Number(exitProceedsUsd.toFixed(6)),
      exitCostUsd: Number(exitCostUsd.toFixed(6)),
      cashCreditUsd: Number(cashCreditUsd.toFixed(6)),
      realizedPnlUsd: Number(realizedPnlUsd.toFixed(6)),
      entryMarketProvenance: p.entryMarketProvenance,
      exitDecisionMarketProvenance: intent.decisionMarketProvenance,
      exitMarketProvenance: provenance, exitBarOpenAt: eligible.barOpenAt,
      rank: p.rank, z: p.z, cause: p.cause, sector: p.sector,
      contextObservation: p.contextObservation || null,
      measurementOutcome: { available: true, grossBps: Number(grossBps.toFixed(4)),
        netBps: Number(netBps.toFixed(4)), costBps: Number(costBps.toFixed(4)),
        heldDays: Number(heldDays.toFixed(2)) },
      componentAttribution: DF.attributeClosed(p, { grossBps, netBps, costBps,
        exitPrice, exitReason: intent.reason, heldDays }),
      ...A.envelope({ created_by: "shadow.evaluateExits" }),
    };
    const saved = await closeAtomic(doc, p, closeRow, {
      cashCreditUsd, realizedPnlUsd, exitCostUsd,
    });
    if (!saved.duplicate) {
      await accumulate(p.variantId, {
        experimentHash: exp.experimentHash, date: ctx.session.date,
        outcomeId: `${doc.id}:mark:${ctx.session.date}`,
        grossContributionBps: exitDailyGross, costContributionBps: dailyCost,
        netContributionBps: exitDailyGross - dailyCost,
      });
      closed.push({ variantId: p.variantId, symbol: p.symbol, netBps: Number(netBps.toFixed(2)) });
    }
  }
  const accountMarks = await markAccounts(ctx, exp);
  return { closed: closed.length, dataLoss: unresolved.length, unresolved,
    accountMarks, experimentHash: exp.experimentHash, detail: closed.slice(0, 80) };
}

function discountEffectiveN(weights) {
  const w = (weights || []).map(Number).filter((x) => Number.isFinite(x) && x > 0);
  const s = w.reduce((a, b) => a + b, 0), ss = w.reduce((a, b) => a + b * b, 0);
  return ss > 0 ? (s * s) / ss : 0;
}

/** Returns the latest Page-Hinkley break, resetting after every alarm. */
function pageHinkley(dayMeans, sd) {
  if (!Array.isArray(dayMeans) || dayMeans.length < 20 || !(sd > 0)) return { break: false, at: null };
  const delta = PH_DELTA_FRAC * sd, lambda = PH_LAMBDA_SD * sd;
  let mean = 0, n = 0, down = 0, up = 0, minDown = 0, minUp = 0, latest = null;
  for (const d of dayMeans) {
    const x = Number(d.mean != null ? d.mean : d.portfolioNetBps);
    if (!Number.isFinite(x)) continue;
    n += 1; mean += (x - mean) / n;
    down += mean - x - delta; up += x - mean - delta;
    minDown = Math.min(minDown, down); minUp = Math.min(minUp, up);
    if (n >= 20 && (down - minDown > lambda || up - minUp > lambda)) {
      latest = d.date; mean = x; n = 1; down = up = minDown = minUp = 0;
    }
  }
  return { break: latest != null, at: latest };
}

function aggregateDays(days, { discounted = false } = {}) {
  if (!days.length) return null;
  const vals = days.map((d) => Number(d.portfolioNetBps) || 0);
  const weights = days.map((_, i) => discounted ? Math.pow(DISCOUNT_GAMMA, days.length - 1 - i) : 1);
  const sw = weights.reduce((a, b) => a + b, 0);
  const mean = vals.reduce((a, x, i) => a + x * weights[i], 0) / sw;
  const variance = vals.reduce((a, x, i) => a + weights[i] * Math.pow(x - mean, 2), 0) / sw;
  const effectiveN = discountEffectiveN(weights);
  const hacVariance = AL.weightedHacMeanVariance(vals, weights);
  const hacSe = Number.isFinite(hacVariance) ? Math.sqrt(hacVariance) : null;
  const gross = days.reduce((a, d, i) => a + (Number(d.portfolioGrossBps) || 0) * weights[i], 0) / sw;
  const cost = days.reduce((a, d, i) => a + (Number(d.portfolioCostBps) || 0) * weights[i], 0) / sw;
  const unresolved = days.reduce((a, d) => a + (Number(d.unresolvedCount) || 0), 0);
  return {
    trades: days.reduce((a, d) => a + (Number(d.nOutcomes) || 0), 0),
    independentDays: days.length, effectiveN: Number(effectiveN.toFixed(3)),
    meanNetBps: Number(mean.toFixed(4)), meanGrossBps: Number(gross.toFixed(4)),
    meanCostBps: Number(cost.toFixed(4)), sdBps: Number(Math.sqrt(variance).toFixed(4)),
    hacSeBps: hacSe == null ? null : Number(hacSe.toFixed(4)),
    tStat: hacSe > 0 ? Number((mean / hacSe).toFixed(3)) : null,
    winRate: Number((vals.filter((x) => x > 0).length / vals.length).toFixed(4)),
    unresolved, unresolvedRate: Number((unresolved / Math.max(1, days.length)).toFixed(5)),
    worstCaseMeanNetBps: Number((days.reduce((a, d) => a + (Number(d.worstCaseNetBps) || 0), 0) / days.length).toFixed(4)),
  };
}

async function rollUpStats({ experimentHash } = {}) {
  if (!/^[a-f0-9]{64}$/.test(String(experimentHash || ""))) {
    throw new Error("rollUpStats requires experimentHash; experiments are never pooled");
  }
  const snap = await A.col(STATS).where("experimentHash", "==", experimentHash).orderBy("date", "asc").get();
  const byVariant = {};
  snap.forEach((d) => {
    const r = d.data();
    if (!r.variantId || !r.date) return;
    (byVariant[r.variantId] ||= []).push(r);
  });
  const power = {}, selection = {}, dailyByVariant = {}, resets = {}, admissibility = {};
  for (const v of V.VARIANTS) {
    const all = byVariant[v.id] || [];
    const days = all.filter((d) => d.completePortfolioDay !== false)
      .sort((a, b) => a.date.localeCompare(b.date));
    /* Exclusions are counted, not merely applied. A series that silently
       loses a third of its sessions still reports a lower bound; one that
       reports the loss can be judged. */
    const missingMarks = all.filter((d) => d.completePortfolioDay === false
      && d.marksComplete !== true).length;
    const nonContiguous = all.filter((d) => d.completePortfolioDay === false
      && d.marksComplete === true).length;
    admissibility[v.id] = { observed: all.length, admissible: days.length,
      excludedMissingMarks: missingMarks, excludedNonContiguous: nonContiguous,
      admissibleRate: all.length ? Number((days.length / all.length).toFixed(4)) : null,
      maxSessionsSpanned: all.reduce((a, d) =>
        Math.max(a, Number(d.sessionsSpanned) || 0), 0) || null };
    dailyByVariant[v.id] = days.map((d) => ({ date: d.date, mean: Number(d.portfolioNetBps) || 0,
      cost: Number(d.portfolioCostBps) || 0,
      worstCaseMean: Number(d.worstCaseNetBps) || 0, unresolved: Number(d.unresolvedCount) || 0 }));
    const p = aggregateDays(days, { discounted: false });
    if (p) power[v.id] = { ...p, source: "power" };
    const ph = pageHinkley(dailyByVariant[v.id], p ? p.sdBps : 0);
    const selected = ph.break ? days.filter((d) => d.date >= ph.at) : days;
    if (ph.break) resets[v.id] = ph.at;
    const s = aggregateDays(selected, { discounted: true });
    if (s) selection[v.id] = { ...s, source: "selection", resetAt: ph.at };
  }
  return { power, selection, dailyByVariant, resets, admissibility, experimentHash,
    discountGamma: DISCOUNT_GAMMA, simulatorVersion: SIMULATOR_VERSION };
}

function effectiveSampleSize(rows) {
  const days = new Set((rows || []).map((r) => r && (r.date || r.openedDate)).filter(Boolean));
  return { nominal: (rows || []).length, effective: days.size, distinctDays: days.size };
}

function measurementDiagnostics(rows, { minObservations = 30 } = {}) {
  const usable = (rows || []).filter((r) => !r.unresolved && !r.excludeFromStats
    && r.contextObservation && r.contextObservation.mode === "measurement_only"
    && r.measurementOutcome && r.measurementOutcome.available === true);
  const featureNames = [...new Set(usable.flatMap((r) =>
    Object.keys(r.contextObservation.features || {})))].sort();
  const mean = (values) => values.length
    ? values.reduce((sum, value) => sum + value, 0) / values.length : null;
  const correlation = (pairs) => {
    if (pairs.length < 3) return null;
    const mx = mean(pairs.map((x) => x[0])), my = mean(pairs.map((x) => x[1]));
    const numerator = pairs.reduce((sum, x) => sum + (x[0] - mx) * (x[1] - my), 0);
    const dx = Math.sqrt(pairs.reduce((sum, x) => sum + (x[0] - mx) ** 2, 0));
    const dy = Math.sqrt(pairs.reduce((sum, x) => sum + (x[1] - my) ** 2, 0));
    return dx > 0 && dy > 0 ? numerator / (dx * dy) : null;
  };
  const features = {};
  for (const name of featureNames) {
    const pairs = usable.map((r) => [Number(r.contextObservation.features[name]),
      Number(r.measurementOutcome.grossBps), Number(r.measurementOutcome.netBps)])
      .filter((x) => x.every(Number.isFinite));
    const active = pairs.filter((x) => x[0] > 0), inactive = pairs.filter((x) => x[0] === 0);
    const corr = correlation(pairs.map((x) => [x[0], x[1]]));
    features[name] = {
      observations: pairs.length,
      status: pairs.length >= minObservations ? "observational_ready" : "warming",
      activeObservations: active.length,
      meanGrossBpsWhenActive: active.length ? Number(mean(active.map((x) => x[1])).toFixed(3)) : null,
      meanNetBpsWhenActive: active.length ? Number(mean(active.map((x) => x[2])).toFixed(3)) : null,
      meanGrossBpsWhenInactive: inactive.length ? Number(mean(inactive.map((x) => x[1])).toFixed(3)) : null,
      adverseDirectionHitRate: active.length
        ? Number((active.filter((x) => x[1] < 0).length / active.length).toFixed(4)) : null,
      correlationWithForwardGross: corr == null ? null : Number(corr.toFixed(4)),
    };
  }
  return {
    schemaVersion: "shadow-feature-diagnostics-v1",
    mode: "measurement_only", affectsDecision: false,
    observations: usable.length, minObservations, features,
    limitation: "Observational shadow diagnostics are selection-affected and cannot alter policy without a separately frozen validation process.",
  };
}

async function variantStats({ experimentHash } = {}) {
  const roll = await rollUpStats({ experimentHash });
  const closedSnap = await A.col(CLOSED).where("experimentHash", "==", experimentHash).get();
  let excluded = 0; const closedRows = [];
  closedSnap.forEach((d) => {
    const row = d.data(); closedRows.push(row);
    if (row.unresolved || row.excludeFromStats) excluded += 1;
  });
  const out = {};
  for (const v of V.VARIANTS) {
    out[v.id] = { id: v.id, name: v.name, plain: v.plain,
      ...(roll.selection[v.id] || { trades: 0, effectiveN: 0, meanNetBps: 0, sdBps: 0 }) };
  }
  return { variants: out, totalClosed: closedSnap.size - excluded, excluded,
    contextMeasurement: Object.fromEntries(V.VARIANTS.map((v) => [v.id,
      measurementDiagnostics(closedRows.filter((r) => r.variantId === v.id))])),
    capped: false, experimentHash };
}

async function openCount(experimentHash = null) {
  const snap = await A.col(OPEN).get(); const byVariant = {}; let total = 0;
  snap.forEach((d) => { const r = d.data(); if (experimentHash && r.experimentHash !== experimentHash) return;
    total += 1; byVariant[r.variantId] = (byVariant[r.variantId] || 0) + 1; });
  return { total, byVariant, experimentHash };
}

module.exports = {
  OPEN, CLOSED, STATS, ACCOUNTS, SIMULATOR_VERSION, DISCOUNT_GAMMA,
  stable, experimentIdentity, assertExperiment, dynamicCorrelationMap, discountEffectiveN, pageHinkley,
  initialAccount, ensureAccount, accountDocId, markAccounts,
  evaluateEntries, evaluateExits, accumulate, aggregateDays, effectiveSampleSize,
  measurementDiagnostics, rollUpStats, variantStats, openCount, positionRows,
};
