/* Investor_AI — the STRIKE PASS (held-position guard + armed-level strikes).
 *
 * This is the fast tier. It runs every minute while the exchange is open and
 * reads only the held, planned and pending symbols, their bars, and the
 * already-stored deterministic intelligence snapshot. It never fetches a
 * source, never scans the opportunity roster, never calls a model, and never
 * advances the learner or the automation ladder. What it does, in order:
 *   1. marks every holding and writes its preset exit levels;
 *   2. arms rule exits (stop, trailing stop, target, earnings, time) and
 *      operator sell requests — the same decision clock every exit obeys;
 *   3. STRIKES armed entry levels written by the deep scan (_investorStrike);
 *   4. auto-approves fresh exploratory proposals and fills approved orders on
 *      their first eligible bar, so a buy no longer waits for the next scan;
 *   5. settles armed exits on their first eligible bar.
 * Strikes and fills go through the same ledger paths as the deep scan and are
 * idempotent, so the two tiers may overlap safely.
 */

"use strict";

const A = require("./_investorAdmin");
const M = require("./_investorMarket");
const S = require("./_investorSignal");
const I = require("./_investorIntelligence");
const L = require("./_investorLedger");
const W = require("./_investorWorkset");
const CA = require("./_investorCorporateActions");
const XP = require("./_investorExplore");
const STATE = require("./_investorState");
const ST = require("./_investorStrike");
const PA = require("./_investorPatience");
const EXITS = require("./_investorExitPolicy");

async function loadPendingOrders(accountId) {
  const out = [];
  for (const st of ["proposed", "approved"]) {
    const q = await A.col(A.COL.orders).where("accountId", "==", accountId).where("status", "==", st).get();
    q.forEach((d) => out.push(d.data()));
  }
  return out;
}

const SHA256 = /^[a-f0-9]{64}$/;

async function guardProgress(runRef, { phase, label, detail = null, pct = 0,
  completed = null, total = null, currentItem = null } = {}) {
  const nowMs = Date.now();
  const safeTotal = Number.isFinite(Number(total)) ? Math.max(0, Number(total)) : null;
  const safeCompleted = Number.isFinite(Number(completed))
    ? Math.max(0, Math.min(safeTotal == null ? Infinity : safeTotal, Number(completed))) : null;
  try {
    await runRef.set({ status: "running", updatedAtMs: nowMs,
      updatedAt: A.FV.serverTimestamp(), progress: {
        phase: String(phase || "protect_positions"), label: String(label || "Protecting holdings"),
        detail: detail == null ? null : String(detail).slice(0, 240),
        pct: Math.max(0, Math.min(100, Math.round(Number(pct) || 0))),
        completed: safeCompleted, total: safeTotal,
        remaining: safeTotal != null && safeCompleted != null
          ? Math.max(0, safeTotal - safeCompleted) : null,
        currentItem: currentItem == null ? null : String(currentItem).slice(0, 80),
        updatedAtMs: nowMs,
      } }, { merge: true });
  } catch {}
  await require("./_investorLease").heartbeat(Date.now());
}

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
      .where("status", "==", "proposed").get();
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
    .where("status", "==", "approved").get();
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
  /* The same relaxed exit parameters the cycle applies (maxHoldDays,
     exitRank) — a guard that timed positions out on the strict ten sessions
     while the cycle used three was two different books. */
  const paperLearning = require("./_investorStrategy.js")
    .paperLearningConfig({ ...(strategy.parameters || {}) }, ctrl);
  const cfg = paperLearning.cfg;
  const operating = STATE.describe(ctrl);
  const activity = XP.activityPolicy(strategy);
  const exploratoryPolicy = strategy.exploratoryAuto || {};
  const activePortfolioControls = operating.exploratoryAuto
    ? { ...(strategy.portfolioControls || {}), ...(exploratoryPolicy.portfolioControls || {}) }
    : (strategy.portfolioControls || {});
  const policyIdentity = { accountId, strategyVersion: ctrl.strategyVersion || strategy.version,
    universeVersion: ctrl.universeVersion || null, strategyHash: ctrl.strategyHash || null,
    universeHash: ctrl.universeHash || null, variantsHash: ctrl.variantsHash || null };
  /* The strike pass prices on finer bars than the signal is measured on
     (1Min by default; control.strikeBarTimeframe). Stops react on the
     minute, fills land on the first eligible minute bar instead of the first
     five-minute one. Bars are persisted to the day store ONLY when they are
     the strategy's own timeframe, so the 5-minute record the deep scan and
     the nightly archive build on is never mixed with minute bars. */
  const strikeTimeframe = /^(1|5)Min$/.test(String(ctrl.strikeBarTimeframe || "")) ? ctrl.strikeBarTimeframe : "1Min";
  const persistBars = strikeTimeframe === (cfg.barTimeframe || "5Min");
  const session = M.sessionState(new Date());
  const runRef = A.col(A.COL.runs).doc(jobId);
  await runRef.set({ jobId, kind: "guard", status: "running",
    startedAt: A.FV.serverTimestamp(), startedAtMs: startedAt,
    accountId, session, strikeBarTimeframe: strikeTimeframe,
    ...A.envelope({ created_by: "positionGuard" }) },
  { merge: true });
  await guardProgress(runRef, { phase: "load_positions",
    label: "Loading holdings, armed levels and pending orders", pct: 8,
    detail: "The strike pass protects holdings, strikes armed entry levels and fills approved orders; it never ranks the roster." });

  const positionSnap = await A.col(A.COL.positions)
    .where("accountId", "==", accountId).where("open", "==", true).get();
  const positions = positionSnap.docs.map((d) => d.data());
  let plans = [], pendingOrders = [];
  try { plans = await ST.loadPlans(accountId, session.date); } catch (e) { plans = []; }
  try { pendingOrders = await loadPendingOrders(accountId); } catch (e) { pendingOrders = []; }
  /* Outside the session nothing strikes or fills; price only the holdings. */
  const strikeSymbols = session.open
    ? [...plans.map((p) => p.symbol), ...pendingOrders.map((o) => o.symbol)] : [];
  const symbols = [...new Set([...positions.map((p) => p.symbol), ...strikeSymbols].filter(Boolean))];
  await guardProgress(runRef, { phase: "price_holdings",
    label: "Refreshing prices for holdings, armed levels and pending orders", pct: 22,
    completed: 0, total: symbols.length,
    detail: `${positions.length} holdings, ${plans.length} armed levels and ${pendingOrders.length} pending orders need a ${strikeTimeframe} mark.` });
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
        { timeframe: strikeTimeframe, limit: 120 });
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
      const fetchedBars = panel[symbol] || [];
      if (fetchedBars.length < 20) {
        try {
          const stored = await M.readRecentBarsWithMeta(symbol, 2);
          if (stored.bars.length && stored.provenance) {
            const fresh = provenance[symbol] || null;
            if (!fetchedBars.length) {
              panel[symbol] = stored.bars;
              provenance[symbol] = stored.provenance;
            } else if (persistBars && fresh
                && fresh.provider === stored.provenance.provider
                && (fresh.feed || null) === (stored.provenance.feed || null)
                && (fresh.adjustment || null) === (stored.provenance.adjustment || null)) {
              /* Preserve the newest in-session observation while adding enough
                 same-feed history for stops and ranks.

                 ONLY WHEN THE TIMEFRAMES AGREE. The day store holds the
                 strategy's own 5-minute bars; this pass may be fetching
                 1-minute ones. Concatenating them produced a single array of
                 mixed bar spacing — routine in the first twenty minutes of a
                 session, when fewer than 20 one-minute bars exist — and
                 S.attentionZ compares a recent window against a per-slot
                 historical baseline, so inconsistent spacing silently biases
                 the volume statistic that gates a strike. Identical
                 provider/feed/adjustment is not enough; the interval has to
                 match too. When it does not, the wholesale replacement above
                 still applies and yields a homogeneous (coarser) series. */
              panel[symbol] = M.normalizeBars([...stored.bars, ...fetchedBars]);
            }
          }
        } catch (e) { /* reported as unpriced below */ }
      }
      const p = provenance[symbol] || { provider: provider.id,
        feed: provider.feed || null, adjustment: null, sourceSha256: null };
      quality[symbol] = M.gradeSeries(panel[symbol] || [], { ...p, nowMs: marketNowMs });
      if (persistBars && (panel[symbol] || []).length) {
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
  await guardProgress(runRef, { phase: "evaluate_exits",
    label: "Checking stops and exit rules", pct: 52,
    completed: 0, total: positions.length,
    detail: "Checking price stops, time limits, earnings risk, rank recovery and critical company findings." });

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
    const guardProgressStride = Math.max(1, Math.ceil(positions.length / 5));
    for (let positionIndex = 0; positionIndex < positions.length; positionIndex += 1) {
      const position = positions[positionIndex];
      const symbol = position.symbol;
      if (positionIndex === 0 || positionIndex % guardProgressStride === 0) {
        await guardProgress(runRef, { phase: "evaluate_exits",
          label: "Checking stops and exit rules",
          pct: 52 + Math.round(23 * positionIndex / Math.max(1, positions.length)),
          completed: positionIndex, total: positions.length, currentItem: symbol,
          detail: `Evaluating every protective exit condition for ${symbol}.` });
      }
      const bars = panel[symbol] || [];
      const last = bars[bars.length - 1];
      if (!last) { unpriced.push(symbol); continue; }
      const source = trustedDecisionSource(provenance[symbol], quality[symbol]);
      if (!source.pass) { untrusted.push({ symbol, reason: source.reason }); continue; }

      const corporateAction = CA.assessPositionMark({ position,
        currentPrice: last.c, currentProvenance: provenance[symbol] });
      if (corporateAction.quarantine) {
        const pending = { ...corporateAction, status: "pending_operator_confirmation",
          detectedAtMs: corporateAction.detectedAtMs || Date.now(), currentMarkAt: last.t,
          currentProvenance: provenance[symbol] };
        await A.col(A.COL.positions).doc(`${accountId}_${symbol}`).set({
          corporateActionPending: pending,
          markQuarantinedAt: A.FV.serverTimestamp(), updated_at: A.FV.serverTimestamp(),
        }, { merge: true });
        untrusted.push({ symbol, reason: "corporate_action_basis_quarantined" });
        continue;
      }

      const peak = Math.max(Number(position.peakPriceUsd) || 0, Number(last.c) || 0);
      const priorCost = position.lastExecutionCostContext || position.entryExecutionCostContext || {};
      const currentExecutionCostContext = M.executionCostContext({
        advUsd: Number(priorCost.advUsd) || 0,
        grade: quality[symbol].grade,
        wideSpreadWindow: session.wideSpreadWindow, vixNorm,
      });
      const heldDays = M.tradingDaysHeld(position.openedAt, nowMs) ?? 0;
      const entry = Number(position.entryPriceUsd != null
        ? position.entryPriceUsd : position.avgPrice);
      const earningsInDays = earningsDistance(earnings[symbol], nowMs);
      /* The preset levels — stop, trailing arm, target, sessions left — are
         written beside the mark so the console shows exactly what this pass
         is watching for. exitSignal below stays the authority. */
      const exitLevels = EXITS.exitLevels(cfg, { entry, peak, heldDays, earningsInDays });
      await A.col(A.COL.positions).doc(`${accountId}_${symbol}`).set({
        peakPriceUsd: peak, lastMarkUsd: last.c, lastMarkAt: last.t,
        lastMarkProvenance: { ...provenance[symbol], barOpenAt: last.t },
        markQuality: quality[symbol], updated_at: A.FV.serverTimestamp(),
        lastExecutionCostContext: currentExecutionCostContext,
        exitLevels, exitLevelsTimeframe: strikeTimeframe,
      }, { merge: true });
      const intelligence = intelligenceBySymbol[symbol] || null;
      const intelligencePolicy = intelligence ? I.decisionPolicy({ coverage: intelligence.coverage,
        events: intelligence.events, temporalContext: intelligence.temporalContext,
        requireTemporalContext: false, asOfMs: nowMs,
        maxAgeHours: cfg.intelligenceMaxAgeHours,
        temporalMaxAgeHours: cfg.temporalMaxAgeHours }) : null;
      /* An operator can ask to sell a holding by hand from the console. The
         request is only ever an INTENT: it enters here, at the same place a
         rule-driven exit enters, so it inherits the identical decision clock,
         provenance requirement and first-eligible-bar fill. Nothing about a
         manual sell can execute at a price the desk already knew. */
      const manualReq = position.manualExitRequest;
      const manualPending = !!(manualReq && manualReq.status === "requested");
      const pnlPctNow = entry > 0 && Number.isFinite(last.c)
        ? Number((((last.c - entry) / entry) * 100).toFixed(2)) : null;
      const exit = manualPending
        ? { exit: true, urgent: true, kind: "manual", pnlPct: pnlPctNow,
            reason: "manual_operator_sell" }
        : S.exitSignal(undefined, heldDays, cfg, {
          mark: last.c, entry, peak, earningsInDays,
          intelligencePolicy,
          /* The guard passes no rank, so the rank exit cannot fire here in
             any case; what patience changes on this path is the time stop. */
          patience: PA.exitTerms(position, { heldDays, pnlPct: pnlPctNow }),
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
        armed: result.armed, blocked: result.blocked || null,
        manual: manualPending || undefined });
      /* Record what became of the operator's request so the console can stop
         saying "requested" once the exit is actually armed (or say why not). */
      if (manualPending) {
        const armedNow = result.armed === true
          || (result.noop === true && !!result.existing);
        await A.col(A.COL.positions).doc(`${accountId}_${symbol}`).set({
          manualExitRequest: { ...manualReq,
            status: armedNow ? "armed" : "blocked",
            armedAtMs: armedNow ? decisionAtMs : null,
            eligibleAfterMs: armedNow ? eligibleAfterMs : null,
            blockedReason: armedNow ? null
              : (result.blocked || result.reason || "not_armed"),
            attemptedAtMs: Date.now() },
          updated_at: A.FV.serverTimestamp() }, { merge: true });
      }
    }

  }
  /* ── THE STRIKE TIER ─────────────────────────────────────────────────
     Armed levels first, then the entry settlement that the deep scan used
     to own: fresh proposals (a strike's or the scan's) are auto-approved in
     the exploratory paper state, and approved orders fill on their first
     eligible bar of THIS pass. Everything here is idempotent against a deep
     scan running at the same moment. */
  let strikes = null, entries = null, entryLock = { acquired: false };
  if (session.open) {
    /* The same mutex the deep scan takes. Losing it costs this pass its
       strikes and its approvals; both retry on the next minute. Exits above
       have already run and are never gated on it. */
    const lockOwner = `guard:${jobId}:${Math.random().toString(36).slice(2, 8)}`;
    entryLock = await require("./_investorLease").acquireEntryLock(accountId, lockOwner);
    entryLock.owner = lockOwner;
    await guardProgress(runRef, { phase: "strike_levels",
      label: entryLock.acquired ? "Checking armed entry levels and pending orders"
        : "Waiting for the deep scan to finish creating entries", pct: 66,
      completed: 0, total: plans.length + pendingOrders.length,
      detail: entryLock.acquired
        ? `${plans.length} armed levels against ${strikeTimeframe} prices; ${pendingOrders.length} orders awaiting approval or an eligible bar.`
        : "A scan is creating entries right now. Striking waits a minute so the two cannot both take the last free slot in the book." });
    if (entryLock.acquired) {
      try {
        strikes = await ST.evaluateStrikes({ jobId, ctrl, accountId, operating, strategy, cfg, activity,
          activePortfolioControls, session, plans, panel, provenance, quality, positions, pendingOrders,
          earnings, vixNorm, paperLearningActive: paperLearning.active === true, exploratoryPolicy, policyIdentity });
      } catch (e) { strikes = { error: String(e.message || e).slice(0, 160) }; }
      try {
        /* Re-read: strikes above may have created and approved orders whose
           first eligible bar could already be in this pass's panel. */
        const fresh = await loadPendingOrders(accountId);
        entries = await ST.settleEntries({ accountId, operating, strategy, cfg, activity, session, panel,
          provenance, quality, pendingOrders: fresh, positions, ctrl, policyIdentity });
      } catch (e) { entries = { error: String(e.message || e).slice(0, 160) }; }
      finally {
        try { await require("./_investorLease").releaseEntryLock(accountId, entryLock.owner); } catch {}
      }
    } else {
      strikes = { skipped: [], struck: [], waiting: [], blocked: [], expired: [],
        deferred: "entry creation is locked by a running scan" };
    }
  } else if (plans.length) {
    /* Nothing strikes while the exchange is shut; levels past their window
       are retired so tomorrow's scan starts clean. */
    const nowMs2 = Date.now();
    for (const plan of plans) {
      if (Number.isFinite(Number(plan.expiresAtMs)) && nowMs2 > Number(plan.expiresAtMs)) {
        try { await ST.markPlan(plan, { status: "expired", expiredAtMs: nowMs2, expiryReason: "session_closed" }); } catch {}
      }
    }
  }
  await guardProgress(runRef, { phase: "settle_exits",
    label: "Settling eligible exits", pct: 78,
    completed: positions.length, total: positions.length,
    detail: `${exitSignals.length} exit signals are armed or awaiting an eligible post-decision market bar.` });

  /* EXECUTE — regular session only. Pre- and post-market bars are not priced
     under regular-session spread assumptions, so nothing fills here. */
  if (session.open) {
    /* Re-read after arming so a newly-created intent and a concurrent full
       cycle's intent are both settled from canonical position state. */
    const liveSnap = await A.col(A.COL.positions)
      .where("accountId", "==", accountId).where("open", "==", true).get();
    for (const doc of liveSnap.docs) {
      const position = doc.data(), intent = position.exitIntent;
      if (!intent || !(Number(intent.decisionAtMs) > 0)) continue;
      if (position.corporateActionPending) {
        settled.skipped.push({ symbol: position.symbol,
          why: "exit pending — corporate-action price basis awaits operator reconciliation" });
        continue;
      }
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
  await guardProgress(runRef, { phase: "audit_protection",
    label: "Auditing the protected paper book", pct: 92,
    detail: `${settled.closed.length} positions closed in this guard pass; verifying ledger and execution records.` });

  let audits = { ledger: null, execution: null };
  try {
    const [ledger, execution] = await Promise.all([
      L.auditLedger(accountId), L.executionAudit(accountId),
    ]);
    audits = { ledger: ledger.pass, execution: execution.pass };
  } catch (e) { audits = { ledger: false, execution: false,
    error: String(e.message).slice(0, 140) }; }

  const finalSnap = await A.col(A.COL.positions)
    .where("accountId", "==", accountId).where("open", "==", true).get();
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
    strikeBarTimeframe: strikeTimeframe, barsPersisted: persistBars,
    entryLock: { acquired: entryLock.acquired === true, heldBy: entryLock.heldBy || null },
    plans: { armed: plans.length,
      struck: strikes && strikes.struck ? strikes.struck.length : 0,
      waiting: strikes && strikes.waiting ? strikes.waiting.length : 0,
      skipped: strikes && strikes.skipped ? strikes.skipped.length : 0,
      blocked: strikes && strikes.blocked ? strikes.blocked.length : 0,
      detail: strikes },
    entries: entries ? { autoApproved: (entries.autoApproved || []).length,
      filled: (entries.filled || []).length, released: (entries.released || []).length,
      skipped: (entries.skipped || []).length, detail: entries } : null,
    exitSignals, positionCoverage: coverage, settlement: {
      closed: settled.closed.length, releasedEntries: settled.releasedEntries.length,
      rejectedEntries: settled.rejectedEntries.length,
      skipped: settled.skipped.length, detail: settled,
    }, audits, providerNote, elapsedMs: Date.now() - startedAt };
  await runRef.set({ ...summary, status: "complete",
    finishedAt: A.FV.serverTimestamp(), updatedAt: A.FV.serverTimestamp(),
    updatedAtMs: Date.now(), progress: {
      phase: "complete", label: "Strike pass complete",
      detail: `${coverage.evaluated} of ${coverage.open} open positions evaluated; ${settled.closed.length} closed`
        + `${strikes && strikes.struck && strikes.struck.length ? `; ${strikes.struck.length} level${strikes.struck.length === 1 ? "" : "s"} struck` : ""}`
        + `${entries && entries.filled && entries.filled.length ? `; ${entries.filled.length} entr${entries.filled.length === 1 ? "y" : "ies"} filled` : ""}.`,
      pct: 100, completed: coverage.evaluated, total: coverage.open,
      remaining: Math.max(0, coverage.open - coverage.evaluated), currentItem: null,
      updatedAtMs: Date.now(),
    } }, { merge: true });
  /* Account value, marked to the prices this run just refreshed. Display
     only; a failure here never fails the guard. */
  try {
    const NAV = require("./_investorNav");
    const snap = await NAV.snapshot(accountId);
    await NAV.record(accountId, snap, { source: "guard" });
    summary.nav = { navUsd: snap.navUsd, unrealisedUsd: snap.unrealisedUsd, open: snap.openPositions };
    await A.col(A.COL.control).doc("control").set({ navLive: snap }, { merge: true });
  } catch (e) { summary.navError = String(e.message || e).slice(0, 120); }
  await A.col(A.COL.control).doc("control").set({
    lastGuardSummary: summary, lastGuardFinishedAt: A.FV.serverTimestamp(),
  }, { merge: true });
  return summary;
}

module.exports = { guardProgress, runGuard, armExitIntent, trustedDecisionSource };
