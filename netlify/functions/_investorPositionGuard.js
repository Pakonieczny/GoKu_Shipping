/* Investor_AI — held-position guard.
 *
 * This path deliberately reads only open positions, their bars, and the
 * already-stored deterministic intelligence snapshot. It never fetches a
 * source, scans the opportunity roster, calls a model, opens positions,
 * advances the learner, or changes the automation ladder. Its writes either
 * reduce risk or report why risk could not be evaluated.
 */

"use strict";

const A = require("./_investorAdmin");
const M = require("./_investorMarket");
const S = require("./_investorSignal");
const I = require("./_investorIntelligence");
const L = require("./_investorLedger");
const W = require("./_investorWorkset");

const SHA256 = /^[a-f0-9]{64}$/;

function trustedDecisionSource(provenance, quality) {
  if (!provenance || !provenance.provider
      || !SHA256.test(String(provenance.sourceSha256 || ""))) {
    return { pass: false, reason: "missing_market_provenance" };
  }
  if (provenance.adjustment == null) {
    return { pass: false, reason: "missing_adjustment_identity" };
  }
  if (!quality || quality.tradable !== true) {
    return { pass: false, reason: "decision_source_not_tradable" };
  }
  return { pass: true };
}

/** Arm an exit exactly once. Full cycles and guard cycles share this helper,
 * so concurrent stop observations serialize on the position document and can
 * never re-stamp the decision clock or overwrite its original provenance. */
async function armExitIntent({ accountId, symbol, intent, decisionId, decisionRecord }) {
  const source = trustedDecisionSource(intent && intent.decisionMarketProvenance,
    intent && intent.decisionQuality);
  if (!source.pass) return { armed: false, blocked: source.reason };
  if (!(Number(intent.decisionAtMs) > 0)
      || !(Number(intent.eligibleAfterMs) >= Number(intent.decisionAtMs))) {
    return { armed: false, blocked: "invalid_exit_clock" };
  }
  const positionRef = A.col(A.COL.positions).doc(`${accountId}_${symbol}`);
  const decisionRef = decisionId
    ? A.col(A.COL.decisions).doc(decisionId) : null;
  return A.runTransaction(async (tx) => {
    const snap = await tx.get(positionRef);
    if (!snap.exists || !snap.data().open) {
      return { armed: false, noop: true, reason: "position_not_open" };
    }
    const existing = snap.data().exitIntent;
    if (existing && Number(existing.decisionAtMs) > 0) {
      return { armed: false, noop: true, existing };
    }
    const stored = { ...intent };
    delete stored.decisionQuality;
    tx.set(positionRef, { exitIntent: stored,
      updated_at: A.FV.serverTimestamp() }, { merge: true });
    if (decisionRef && decisionRecord) {
      tx.set(decisionRef, { ...decisionRecord,
        decisionAtMs: stored.decisionAtMs,
        eligibleAfterMs: stored.eligibleAfterMs,
        decisionMarketProvenance: stored.decisionMarketProvenance,
        ...A.envelope({ created_by: "positionGuard.armExitIntent" }) },
      { merge: true });
    }
    return { armed: true, intent: stored };
  });
}

function earningsDistance(window, nowMs) {
  if (!window || !Array.isArray(window.dates)) return null;
  const values = window.dates.map((d) => (Date.parse(d) - nowMs) / 864e5)
    .filter((d) => Number.isFinite(d) && d >= 0).sort((a, b) => a - b);
  return values.length ? values[0] : null;
}

async function readControl() {
  const snap = await A.col(A.COL.control).doc("control").get();
  return snap.exists ? snap.data() : {};
}

async function readStrategy(version) {
  const fallback = require("./_investorStrategy.js");
  const snap = await A.col(A.COL.strategies).doc(version || fallback.version || "v2").get();
  return snap.exists ? snap.data() : fallback;
}

async function readEarnings() {
  const snap = await A.col(A.COL.control).doc("earnings").get();
  return snap.exists ? (snap.data().windows || {}) : {};
}

async function readVixNorm() {
  const snap = await A.col(A.COL.control).doc("regime").get();
  const d = snap.exists ? snap.data() : {};
  const age = d.vixFetchedAt ? Date.now() - Date.parse(d.vixFetchedAt) : Infinity;
  return d.vixHealthy === true && age <= 36 * 3600e3 && Number(d.vix) > 0 && Number(d.vixMedian) > 0
    ? Number(d.vix) / Number(d.vixMedian) : 1;
}

async function releaseClosedEntryQueue(ctrl, accountId, settled) {
  const explicitlyClosed = ctrl.killSwitch || ctrl.enabled === false
    || ctrl.entriesFrozen === true;
  if (explicitlyClosed) {
    const proposed = await A.col(A.COL.orders).where("accountId", "==", accountId)
      .where("status", "==", "proposed").limit(100).get();
    for (const doc of proposed.docs) {
      const order = doc.data();
      try {
        const result = await L.rejectOrder(order.orderId,
          "entry controls closed during position guard", "position-guard");
        if (!result.noop) settled.rejectedEntries.push(order.orderId);
      } catch (e) { settled.skipped.push({ orderId: order.orderId,
        why: "could not reject entry proposal" }); }
    }
  }

  const approved = await A.col(A.COL.orders).where("accountId", "==", accountId)
    .where("status", "==", "approved").limit(100).get();
  const cutoffMs = Date.now() - 24 * 3600e3;
  for (const doc of approved.docs) {
    const order = doc.data();
    const stale = Number(order.decisionAtMs) <= cutoffMs;
    if (!explicitlyClosed && !stale) continue;
    try {
      const reason = explicitlyClosed
        ? "entry controls closed during position guard"
        : "expired — no eligible fill within 24h";
      const result = await L.releaseOrder(order.orderId, reason);
      if (!result.noop) settled.releasedEntries.push(order.orderId);
    } catch (e) { settled.skipped.push({ orderId: order.orderId,
      why: "could not release approved entry" }); }
  }
}

async function runGuard(jobId) {
  const startedAt = Date.now();
  const ctrl = await readControl();
  const accountId = ctrl.accountId || "paper-1";
  const strategy = await readStrategy(ctrl.strategyVersion);
  const cfg = { ...(strategy.parameters || {}) };
  const session = M.sessionState(new Date());
  const runRef = A.col(A.COL.runs).doc(jobId);
  await runRef.set({ jobId, kind: "guard", startedAt: A.FV.serverTimestamp(),
    accountId, session, ...A.envelope({ created_by: "positionGuard" }) },
  { merge: true });

  const positionSnap = await A.col(A.COL.positions)
    .where("accountId", "==", accountId).where("open", "==", true).limit(100).get();
  const positions = positionSnap.docs.map((d) => d.data());
  const symbols = [...new Set(positions.map((p) => p.symbol).filter(Boolean))];
  const intelligenceBySymbol = {};
  await Promise.all(symbols.map(async (symbol) => {
    try {
      const snap = await A.col(A.COL.intelligence).doc(symbol).get();
      if (snap.exists) intelligenceBySymbol[symbol] = snap.data();
    } catch {}
  }));
  const settled = { closed: [], skipped: [], releasedEntries: [], rejectedEntries: [] };
  await releaseClosedEntryQueue(ctrl, accountId, settled);

  /* Bars are aged in MARKET time. Outside the regular session the most
     recent stored bar IS the market, so grading it against wall-clock
     staleness is what previously reduced this guard to 25% coverage with a
     17.5-hour hole across every overnight. */
  const marketNowMs = session.open ? Date.now()
    : (M.lastRegularOpenMs(new Date()) || Date.now());

  let panel = {}, provenance = {}, quality = {}, providerNote = null;
  if (!session.open && symbols.length) {
    /* Evaluate-only pass: read what the last in-session pass stored, arm an
       exit intent if the position now warrants one, and execute nothing.
       No provider is called, so this costs a Firestore read per holding. */
    for (const symbol of symbols) {
      try {
        const stored = await M.readRecentBarsWithMeta(symbol, 2);
        if (stored.bars.length && stored.provenance) {
          panel[symbol] = stored.bars;
          provenance[symbol] = stored.provenance;
          quality[symbol] = M.gradeSeries(stored.bars,
            { ...stored.provenance, nowMs: marketNowMs });
        }
      } catch (e) { /* reported as unpriced below */ }
    }
    providerNote = "evaluate_only_outside_session";
  }
  if (session.open && symbols.length) {
    const provider = M.activeProvider();
    try {
      const fetched = await M.fetchBars(symbols,
        { timeframe: cfg.barTimeframe || "5Min", limit: 120 });
      panel = fetched.bars || {};
      providerNote = fetched.note || null;
      for (const symbol of symbols) {
        const sha = (fetched.symbolSha256 || {})[symbol]
          || fetched.manifestSha256 || fetched.sha256 || null;
        if (panel[symbol] && sha) provenance[symbol] = {
          provider: fetched.provider, feed: fetched.feed || null,
          adjustment: fetched.adjustment || null, sourceSha256: sha,
          fetchedAt: fetched.fetchedAt || null,
        };
      }
    } catch (e) { providerNote = String(e.code || e.message).slice(0, 160); }

    for (const symbol of symbols) {
      if (!panel[symbol] || panel[symbol].length < 20) {
        try {
          const stored = await M.readRecentBarsWithMeta(symbol, 2);
          if (stored.bars.length && stored.provenance) {
            panel[symbol] = stored.bars;
            provenance[symbol] = stored.provenance;
          }
        } catch (e) { /* reported as unpriced below */ }
      }
      const p = provenance[symbol] || { provider: provider.id,
        feed: provider.feed || null, adjustment: null, sourceSha256: null };
      quality[symbol] = M.gradeSeries(panel[symbol] || [], { ...p, nowMs: marketNowMs });
      if ((panel[symbol] || []).length) {
        try {
          await M.writeBars(symbol, session.date, panel[symbol], {
            ...p, grade: quality[symbol].grade,
            gradeReasons: quality[symbol].reasons,
            feedDelayMinutes: quality[symbol].feedDelayMinutes,
          });
        } catch (e) { /* storage failure does not block protection */ }
      }
    }
  }

  const [earnings, vixNorm] = await Promise.all([
    readEarnings().catch(() => ({})), readVixNorm().catch(() => 1),
  ]);
  const evaluated = new Set(), unpriced = [], untrusted = [], exitSignals = [];
  const nowMs = Date.now();
  /* EVALUATE — every guard tick, in session or not. A position that breaches
     its stop at 16:05 must not wait until 09:30 to have that recorded. The
     intent's decision clock starts here; M.firstEligibleBar still refuses to
     fill it until a bar exists that opened after the decision, so arming
     outside the session cannot execute at a price nobody could have got. */
  {
    for (const position of positions) {
      const symbol = position.symbol;
      const bars = panel[symbol] || [];
      const last = bars[bars.length - 1];
      if (!last) { unpriced.push(symbol); continue; }
      const source = trustedDecisionSource(provenance[symbol], quality[symbol]);
      if (!source.pass) { untrusted.push({ symbol, reason: source.reason }); continue; }

      const peak = Math.max(Number(position.peakPriceUsd) || 0, Number(last.c) || 0);
      const priorCost = position.lastExecutionCostContext || position.entryExecutionCostContext || {};
      const currentExecutionCostContext = M.executionCostContext({
        advUsd: Number(priorCost.advUsd) || 0,
        grade: quality[symbol].grade,
        wideSpreadWindow: session.wideSpreadWindow, vixNorm,
      });
      await A.col(A.COL.positions).doc(`${accountId}_${symbol}`).set({
        peakPriceUsd: peak, lastMarkUsd: last.c, lastMarkAt: last.t,
        lastMarkProvenance: { ...provenance[symbol], barOpenAt: last.t },
        markQuality: quality[symbol], updated_at: A.FV.serverTimestamp(),
        lastExecutionCostContext: currentExecutionCostContext,
      }, { merge: true });

      const openedMs = Date.parse(position.openedAt);
      const heldDays = Number.isFinite(openedMs) ? (nowMs - openedMs) / 864e5 : 0;
      const entry = Number(position.entryPriceUsd != null
        ? position.entryPriceUsd : position.avgPrice);
      const intelligence = intelligenceBySymbol[symbol] || null;
      const intelligencePolicy = intelligence ? I.decisionPolicy({ coverage: intelligence.coverage,
        events: intelligence.events, temporalContext: intelligence.temporalContext,
        requireTemporalContext: false, asOfMs: nowMs,
        maxAgeHours: cfg.intelligenceMaxAgeHours,
        temporalMaxAgeHours: cfg.temporalMaxAgeHours }) : null;
      const exit = S.exitSignal(undefined, heldDays, cfg, {
        mark: last.c, entry, peak,
        earningsInDays: earningsDistance(earnings[symbol], nowMs),
        intelligencePolicy,
      });
      evaluated.add(symbol);
      if (!exit.exit) continue;

      const decisionAtMs = Date.now();
      const feedCfg = M.providerConfig(provenance[symbol].provider,
        provenance[symbol].feed);
      const eligibleAfterMs = decisionAtMs + feedCfg.delayMinutes * 60000
        + (cfg.executionLatencyMs || 60000);
      const result = await armExitIntent({ accountId, symbol,
        decisionId: `${jobId}_${symbol}_exit`,
        intent: { decisionAtMs, eligibleAfterMs,
          decisionAt: new Date(decisionAtMs).toISOString(), reason: exit.reason,
          kind: exit.kind, urgent: !!exit.urgent, pnlPctAtSignal: exit.pnlPct,
          decisionMarketProvenance: provenance[symbol],
          armedOutsideSession: !session.open,
          markAsOf: last.t,
          decisionQuality: quality[symbol] },
        decisionRecord: { cycleId: jobId, symbol, kind: "exit_signal",
          exitKind: exit.kind, reason: exit.reason, urgent: !!exit.urgent,
          pnlPct: exit.pnlPct, markUsd: last.c, entryUsd: entry || null,
          heldDays, source: session.open
            ? "held_position_guard" : "held_position_guard_closed_market",
          intelligenceDossierHash: intelligence && intelligence.dossierHash || null,
          intelligenceEventId: exit.intelligenceEventId || null },
      });
      exitSignals.push({ symbol, kind: exit.kind, urgent: !!exit.urgent,
        armed: result.armed, blocked: result.blocked || null });
    }

  }

  /* EXECUTE — regular session only. Pre- and post-market bars are not priced
     under regular-session spread assumptions, so nothing fills here. */
  if (session.open) {
    /* Re-read after arming so a newly-created intent and a concurrent full
       cycle's intent are both settled from canonical position state. */
    const liveSnap = await A.col(A.COL.positions)
      .where("accountId", "==", accountId).where("open", "==", true).limit(100).get();
    for (const doc of liveSnap.docs) {
      const position = doc.data(), intent = position.exitIntent;
      if (!intent || !(Number(intent.decisionAtMs) > 0)) continue;
      const bars = panel[position.symbol] || [];
      const current = provenance[position.symbol];
      const sourcePolicy = W.exitExecutionSourcePolicy(
        intent.decisionMarketProvenance, current, quality[position.symbol]);
      if (!sourcePolicy.pass) {
        settled.skipped.push({ symbol: position.symbol,
          why: `exit pending — ${sourcePolicy.reason}` });
        continue;
      }
      const eligible = M.firstEligibleBar(bars, {
        decisionAtMs: intent.decisionAtMs,
        provider: current.provider, feed: current.feed,
        executionLatencyMs: cfg.executionLatencyMs || 60000,
      });
      if (!eligible.bar) {
        settled.skipped.push({ symbol: position.symbol,
          why: "exit pending — no post-decision executable bar" });
        continue;
      }
      try {
        const priorCost = position.lastExecutionCostContext || position.entryExecutionCostContext || {};
        const executionCostContext = M.executionCostContext({
          advUsd: Number(priorCost.advUsd) || 0,
          grade: quality[position.symbol].grade,
          wideSpreadWindow: session.wideSpreadWindow, vixNorm,
        });
        const result = await L.closePosition({ accountId, symbol: position.symbol,
          bar: eligible.bar,
          slippageBps: executionCostContext.slippageBps,
          reason: intent.reason, decisionAtMs: intent.decisionAtMs,
          eligibleAfterMs: intent.eligibleAfterMs,
          executionEligibleAfterMs: eligible.availableFromMs,
          executionCostContext,
          barProvenance: { ...current, barOpenAt: eligible.barOpenAt },
          closedBy: "position_guard",
        });
        if (!result.noop) settled.closed.push({ symbol: position.symbol,
          reason: intent.reason, kind: intent.kind, netBps: result.netBps });
      } catch (e) { settled.skipped.push({ symbol: position.symbol,
        why: String(e.message).slice(0, 140) }); }
    }
  }

  let audits = { ledger: null, execution: null };
  try {
    const [ledger, execution] = await Promise.all([
      L.auditLedger(accountId), L.executionAudit(accountId),
    ]);
    audits = { ledger: ledger.pass, execution: execution.pass };
  } catch (e) { audits = { ledger: false, execution: false,
    error: String(e.message).slice(0, 140) }; }

  const finalSnap = await A.col(A.COL.positions)
    .where("accountId", "==", accountId).where("open", "==", true).limit(100).get();
  const openRows = finalSnap.docs.map((d) => d.data());
  const coverage = {
    open: openRows.length,
    evaluated: openRows.filter((p) => evaluated.has(p.symbol)).length,
    unevaluated: openRows.filter((p) => !evaluated.has(p.symbol)).map((p) => p.symbol),
    unpriced: [...new Set(unpriced)], untrusted,
    pendingUrgentExits: openRows
      .filter((p) => p.exitIntent && p.exitIntent.urgent && p.exitIntent.decisionAtMs)
      .map((p) => ({ symbol: p.symbol, kind: p.exitIntent.kind,
        waitingMinutes: Math.max(0,
          Math.round((Date.now() - Number(p.exitIntent.decisionAtMs)) / 60000)) }))
      .filter((p) => p.waitingMinutes >= 30),
  };
  const summary = { jobId, kind: "guard", accountId,
    session: { date: session.date, phase: session.phase, open: session.open },
    marketDeferred: !session.open, executionDeferred: !session.open,
    marketAsOf: new Date(marketNowMs).toISOString(), symbols: symbols.length,
    exitSignals, positionCoverage: coverage, settlement: {
      closed: settled.closed.length, releasedEntries: settled.releasedEntries.length,
      rejectedEntries: settled.rejectedEntries.length,
      skipped: settled.skipped.length, detail: settled,
    }, audits, providerNote, elapsedMs: Date.now() - startedAt };
  await runRef.set({ ...summary, status: "complete",
    finishedAt: A.FV.serverTimestamp() }, { merge: true });
  await A.col(A.COL.control).doc("control").set({
    lastGuardSummary: summary, lastGuardFinishedAt: A.FV.serverTimestamp(),
  }, { merge: true });
  return summary;
}

module.exports = { runGuard, armExitIntent, trustedDecisionSource };
