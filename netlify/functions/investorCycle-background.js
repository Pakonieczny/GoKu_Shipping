/*  netlify/functions/investorCycle-background.js  (v1.0)
 *  ---------------------------------------------------------------------------
 *  Investor_AI — the cycle worker. Background function, 15-minute ceiling.
 *
 *  THE INVERTED PIPELINE. The order of these steps is the cost architecture:
 *
 *    1. pull bars for the whole trade tier            (one provider call)
 *    2. compute residual returns across the panel     (pure maths, free)
 *    3. rank cross-sectionally, find threshold breaches
 *    4. ONLY for breaches: look for a cause in the evidence lanes
 *    5. ONLY if still ambiguous: ask the model
 *    6. run the gate stack, propose or record a no-trade decision
 *    7. evaluate open positions against the buy/hold exit rule
 *
 *  Steps 1-3 cost nothing but a data call. Step 5 is the expensive one and is
 *  reached by a handful of names per cycle instead of the whole roster, which
 *  is the difference between ~$285/month and ~$40/month.
 *
 *  IDEMPOTENCE. Netlify background functions return 202 immediately and retry
 *  after failure. Every write below is either deterministic-id (ledger, fills)
 *  or a merge on a stable key (candidates keyed by cycle+symbol), so a retry
 *  re-derives the same state rather than duplicating it.
 *
 *  This worker NEVER places a real order. It writes virtual proposals; the
 *  explicit exploratory-auto paper state may approve them immediately, a
 *  human may approve them, or a validated book may auto-approve within its
 *  measured caps. There is no broker integration.
 * ---------------------------------------------------------------------------
 */

"use strict";

/* Shared ceiling for the exploratory observation size floor. Repeating the
   literal in three files is how one of them ends up out of step. */
const ST_FLOOR_MAX = require("./_investorStrategy").PAPER_OBSERVATION_FLOOR_MAX;

const crypto = require("crypto");
const A = require("./_investorAdmin");
const AUTH = require("./_investorAuth");
const { verifyWorkerNonce, redact } = AUTH;
const M = require("./_investorMarket");
const H = require("./_investorHistory");
const W = require("./_investorWorkset");
const PG = require("./_investorPositionGuard");
const S = require("./_investorSignal");
const E = require("./_investorEvidence");
const IS = require("./_investorIntelligenceSources");
const I = require("./_investorIntelligence");
const T = require("./_investorTemporal");
const O = require("./_investorOpenai");
const L = require("./_investorLedger");
const B = require("./_investorBootstrap");
const SH = require("./_investorShadow");
const AL = require("./_investorAllocator");
const V = require("./_investorVariants");
const R = require("./_investorRisk");
const LD = require("./_investorLadder");
const C = require("./_investorCalibration");
const DF = require("./_investorDecisionFeedback");
const CA = require("./_investorCorporateActions");
const RS = require("./_investorResearchStats");
const STATE = require("./_investorState");
const DM = require("./_investorDecisionManifest");
const SOAK = require("./_investorSoak");
const XP = require("./_investorExplore");
const DS = require("./_investorSufficiency");
const ST = require("./_investorStrike");
const PA = require("./_investorPatience");
const EXITS = require("./_investorExitPolicy");

function sha256Json(value) {
  return crypto.createHash("sha256").update(JSON.stringify(value)).digest("hex");
}

const FN_NAME = "investorCycle-background";
/* The platform ceiling for a background function is 15 minutes. A lease that
   expired at 14 let a slow cycle overlap its successor for its final minute,
   and nothing renewed it while the worker was alive. The lease now exceeds
   the ceiling (the worker is killed before the lease lapses, so overlap is
   impossible) and is renewed on every progress report, so the job document
   also shows a live heartbeat an operator can distinguish from a dead run. */
const WORKER_LEASE_TTL_MS = 16 * 60 * 1000;
const GUARD_LEASE_TTL_MS = 4 * 60 * 1000;
const LEASE = require("./_investorLease");

/* Progress is operational evidence, not decoration. Each update names the
   actual phase the worker has reached and, when the work is countable, records
   completed and remaining units. A telemetry write is deliberately
   non-blocking: losing a progress update must never lose a fill, exit or
   decision record. */
async function reportRunProgress(runRef, { phase, label, detail = null,
  pct = 0, completed = null, total = null, currentItem = null } = {}) {
  const nowMs = Date.now();
  const safePct = Math.max(0, Math.min(100, Math.round(Number(pct) || 0)));
  const safeTotal = Number.isFinite(Number(total)) ? Math.max(0, Number(total)) : null;
  const safeCompleted = Number.isFinite(Number(completed))
    ? Math.max(0, Math.min(safeTotal == null ? Infinity : safeTotal, Number(completed)))
    : null;
  const remaining = safeTotal != null && safeCompleted != null
    ? Math.max(0, safeTotal - safeCompleted) : null;
  try {
    await runRef.set({ status: "running", updatedAtMs: nowMs,
      updatedAt: A.FV.serverTimestamp(),
      ...(LIVE.runRef === runRef ? { live: liveSnapshot() } : {}),
      progress: {
        phase: String(phase || "working"), label: String(label || "Working"),
        detail: detail == null ? null : String(detail).slice(0, 240),
        pct: safePct, completed: safeCompleted, total: safeTotal, remaining,
        currentItem: currentItem == null ? null : String(currentItem).slice(0, 80),
        updatedAtMs: nowMs,
      } }, { merge: true });
  } catch (e) {
    console.warn("run progress write failed", redact({ phase, error: e.message }));
  }
  await LEASE.heartbeat(nowMs);
}

/* ── LIVE FEED: what the scan is doing, company by company ──────────────
   The progress bar says how far along the scan is; this says what happened
   to each name as it went through — priced, ranked, signal or not, which
   gate stopped it, proposed, approved — so the console can animate the
   pipeline instead of showing a bar that moves and a list that appears at
   the end. Flushed to the run document at most every 2 seconds. */
const LIVE_RECENT = 40, LIVE_FLUSH_MS = 2000;
const LIVE = { runRef: null, counters: {}, blockedBy: {}, recent: [], lastFlushMs: 0, pending: false };
function liveReset(runRef) {
  LIVE.runRef = runRef; LIVE.counters = {}; LIVE.blockedBy = {}; LIVE.recent = [];
  LIVE.lastFlushMs = 0; LIVE.pending = false;
}
function liveSnapshot() {
  return { counters: { ...LIVE.counters }, blockedBy: { ...LIVE.blockedBy },
    recent: LIVE.recent.slice(-LIVE_RECENT), updatedAtMs: Date.now() };
}
async function liveFlush(force = false) {
  if (!LIVE.runRef || (!LIVE.pending && !force)) return;
  const nowMs = Date.now();
  if (!force && nowMs - LIVE.lastFlushMs < LIVE_FLUSH_MS) return;
  LIVE.lastFlushMs = nowMs; LIVE.pending = false;
  try { await LIVE.runRef.set({ live: liveSnapshot(), updatedAtMs: nowMs }, { merge: true }); }
  catch (e) { /* the live feed is display only */ }
}
/** Record one event: `stage` is a short token the console maps to a column. */
function liveEvent(stage, symbol, note = null, { count = true } = {}) {
  if (count) LIVE.counters[stage] = (LIVE.counters[stage] || 0) + 1;
  if (stage === "blocked" && note) LIVE.blockedBy[note] = (LIVE.blockedBy[note] || 0) + 1;
  LIVE.recent.push({ s: stage, sym: symbol || null, n: note == null ? null : String(note).slice(0, 60), t: Date.now() });
  if (LIVE.recent.length > LIVE_RECENT * 2) LIVE.recent = LIVE.recent.slice(-LIVE_RECENT);
  LIVE.pending = true;
  return liveFlush(false);
}

function executionSourceEligible(quality) {
  return !!quality && quality.tradable === true;
}

/* ── universe + strategy loading ───────────────────────────────────────── */
async function loadUniverse(version) {
  /* Default to the version the shipped roster declares, not a hardcoded "v1".
     Bootstrap writes the CIK-resolved roster to doc(base.version) — currently
     "v2" — so reading "v1" silently returned the unresolved fallback with every
     cik null, which disabled the entire evidence lane without any error. */
  const fallbackModule = require("./_investorUniverse.js");
  const snap = await A.col(A.COL.universe).doc(version || fallbackModule.version || "v1").get();
  if (snap.exists) return snap.data();
  const fallback = require("./_investorUniverse.js");
  return fallback;
}

async function loadStrategy(version) {
  const fallback = require("./_investorStrategy.js");
  const snap = await A.col(A.COL.strategies).doc(version || fallback.version).get();
  if (snap.exists) return snap.data();
  return fallback;
}

async function controlDoc() {
  const s = await A.col(A.COL.control).doc("control").get();
  return s.exists ? s.data() : {};
}

/* ── regime inputs ─────────────────────────────────────────────────────── */
/* VIX and COR3M drive sizing and the stand-down gate. Both are cached in
   Firestore by the evidence sweep; the cycle reads whatever is current and
   degrades to reduced size rather than failing when they are stale. */
async function regime() {
  const s = await A.col(A.COL.control).doc("regime").get();
  const d = s.exists ? s.data() : {};
  const vixAge = d.vixFetchedAt ? Date.now() - Date.parse(d.vixFetchedAt) : Infinity;
  const corAge = d.corFetchedAt ? Date.now() - Date.parse(d.corFetchedAt) : Infinity;
  const vixHealthy = d.vixHealthy === true && vixAge <= 36 * 3600e3;
  const corHealthy = d.corHealthy === true && corAge <= 36 * 3600e3;
  return {
    vix: vixHealthy ? (Number(d.vix) || null) : null,
    vixMedian: Number(d.vixMedian) || 18,
    vixNorm: vixHealthy && d.vix && d.vixMedian ? d.vix / d.vixMedian : null,
    /* An unset COR3M used to become NaN, and every comparison against NaN is
       false, so the dispersion stand-down — the only automatic risk-off switch
       in the system — could never fire unless a human typed a number in.
       Absent correlation data now reads as the neutral default and is flagged
       stale rather than silently disabling the gate. */
    cor3m: corHealthy && Number.isFinite(Number(d.cor3m)) ? Number(d.cor3m) : null,
    asOf: d.asOf || null,
    rawVix: Number.isFinite(Number(d.vix)) ? Number(d.vix) : null,
    rawCor3m: Number.isFinite(Number(d.cor3m)) ? Number(d.cor3m) : null,
    vixFetchedAt: d.vixFetchedAt || null, corFetchedAt: d.corFetchedAt || null,
    vixSource: d.vixSource || null, corSource: d.corSource || null,
    vixSourceSha256: d.vixSourceSha256 || d.sourceSha256 || null,
    corSourceSha256: d.corSourceSha256 || null,
    vixHealthy, corHealthy, vixAgeMs: vixAge, corAgeMs: corAge,
    stale: !vixHealthy || !corHealthy,
  };
}

/* ── attention proxy ───────────────────────────────────────────────────── */
/* A true retail-order-flow measure needs TAQ, which is paid and out of scope.
   The tradable proxy the research supports is abnormal VOLUME against the
   symbol's own baseline. Intraday volume has a strong U-shape, so comparing an
   opening window with every bar of the day manufactures "attention". Remove
   that deterministic seasonality first: each recent log-volume observation is
   compared only with the same New York clock slot in prior sessions. The
   residuals are standardized with the pooled, de-seasonalized history. We do
   not divide by sqrt(window): adjacent intraday volumes are dependent and that
   nominal precision would be anti-conservative. Named honestly so nobody
   mistakes this for Robinhood-style holdings or classified investor flow. */
/* attentionZ lives in _investorSignal so the strike pass can reuse it
   without importing this worker. Re-exported below for existing callers. */
const attentionZ = S.attentionZ;

function barTimeframeMs(timeframe) {
  const match = String(timeframe || "5Min").match(/^(\d+)\s*(Min|Hour|Day)$/i);
  if (!match) return 300000;
  const count = Math.max(1, Number(match[1]) || 1);
  const unit = match[2].toLowerCase();
  return count * (unit === "min" ? 60000 : unit === "hour" ? 3600000 : 86400000);
}

/* ── measured dollar volume ────────────────────────────────────────────── */
/* Hardcoded volume goes stale within weeks and the liquidity gate is the main
   defence against cost drag, so it is measured from the bars we already have.
   Scaled from the bar interval up to a full session. */
function measuredAdvUsd(bars, barsPerSession = 78) {
  if (!bars || bars.length < 10) return 0;
  const recent = bars.slice(-Math.min(bars.length, 200));
  const perBar = S.mean(recent.map((b) => (b.v || 0) * (b.c || 0)));
  return perBar * barsPerSession;
}

function advFor(sym, bars, provenance, historyCtx) {
  const ctx = historyCtx && historyCtx[sym] || {};
  const dailyRaw = Number(ctx.advUsd20);
  const dailyProv = ctx.dailyMarketProvenance || null;
  const currentIdentityKnown = !!(provenance && provenance.provider);
  const currentShare = currentIdentityKnown
    ? M.feedVolumeShare(provenance.provider, provenance.feed) : 1;
  const intradayRaw = measuredAdvUsd(bars, 78);
  const components = [];

  if (dailyRaw > 0) {
    /* Only inflate a feed-limited daily number when the stored daily record is
       explicitly homogeneous and carries its own provider/feed identity. A
       current IEX label must never be retroactively applied to old SIP volume. */
    const trustedIdentity = dailyProv && dailyProv.homogeneous === true
      && dailyProv.provider;
    const dailyShare = trustedIdentity
      ? M.feedVolumeShare(dailyProv.provider, dailyProv.feed) : 1;
    components.push({ source: "daily_20d", value: dailyRaw / dailyShare,
      provisional: !trustedIdentity || dailyShare < 1,
      feedVolumeShare: dailyShare, provenance: dailyProv });
  }
  if (intradayRaw > 0) {
    components.push({ source: "intraday_extrapolated",
      value: intradayRaw / currentShare,
      provisional: !currentIdentityKnown || currentShare < 1,
      feedVolumeShare: currentShare, provenance: provenance || null });
  }
  if (!components.length) return { advUsd: 0, source: "none", provisional: true,
    feedVolumeShare: currentShare, components: [] };

  /* A liquidity overstatement is more dangerous than an understatement. Use
     the lowest independently available normalized estimate and retain all
     components so the dashboard can explain the choice. */
  const selected = [...components].sort((a, b) => a.value - b.value)[0];
  return { advUsd: selected.value, source: selected.source,
    provisional: selected.provisional, feedVolumeShare: selected.feedVolumeShare,
    components };
}

function managedSymbolUnion(signalSymbols, positions) {
  const held = (positions || []).filter((p) => p && p.open && p.symbol).map((p) => p.symbol);
  return [...new Set([...(signalSymbols || []), ...held])];
}

/* ── main cycle ────────────────────────────────────────────────────────── */
/**
 * Withhold a leader that has not cleared the development overfit guard.
 *
 * This is the enforcement point for the Deflated Sharpe and CSCV/PBO gates.
 * It lives here, exported and pure, because inline enforcement inside a
 * 700-line cycle could be deleted without a single test or fixture noticing —
 * removing it, and the ladder's `overfit_guard` rung, left the whole suite
 * green. A guard that is not attested at the point it acts is decoration.
 *
 * Mutates and returns `alloc`. A leader is cleared unless BOTH the leader's
 * DSR and the family PBO pass; a warming or absent guard withholds too.
 */
function applyOverfitGuard(alloc, overfitGuard) {
  if (!alloc || !alloc.leaderId) return alloc;
  if (RS.passesGuard(overfitGuard, alloc.leaderId)) return alloc;
  const dsr = overfitGuard && overfitGuard.dsrByVariant
    && overfitGuard.dsrByVariant[alloc.leaderId];
  const pbo = overfitGuard && overfitGuard.pbo;
  alloc.note = `${alloc.note || ""} Overfit guard withheld promotion: `
    + `DSR probability ${dsr && dsr.probability != null ? dsr.probability : "warming"}; `
    + `PBO ${!pbo || pbo.pbo == null ? "warming" : pbo.pbo}.`;
  alloc.overfitWithheld = { leaderId: alloc.leaderId,
    dsrPass: !!(dsr && dsr.pass), pboPass: !!(pbo && pbo.pass) };
  alloc.leaderId = null;
  return alloc;
}

async function runCycle(jobId, { manual = false } = {}) {
  const startedAt = Date.now();
  const runRef = A.col(A.COL.runs).doc(jobId);
  liveReset(runRef);
  await runRef.set({ jobId, kind: "cycle", status: "running",
    startedAt: A.FV.serverTimestamp(), startedAtMs: startedAt,
    ...A.envelope({ created_by: FN_NAME }) }, { merge: true });
  /* Which scheduled slot, if any, this run satisfies. Read from the job the
     kick claimed; stamped onto control only when this worker completes. */
  let planSlotKey = null;
  try {
    const jobSnap = await A.col(A.COL.jobs).doc(jobId).get();
    if (jobSnap.exists) planSlotKey = jobSnap.data().planSlot || null;
  } catch { /* a missing slot key only costs one retryable scan */ }
  await reportRunProgress(runRef, { phase: "bootstrap",
    label: "Checking the frozen build and paper account", pct: 2,
    detail: "Validating the universe, strategy identity and ledger before any market decision." });

  /* Self-bootstrap. First cycle freezes the universe, resolves CIKs from SEC,
     opens the paper account, derives earnings windows and fetches VIX. Every
     cycle after that this is a single read plus the slow-clock refreshes. */
  let bootstrap = null;
  try { bootstrap = await B.ensureBootstrapped({ enrich: false }); }
  catch (e) { bootstrap = { error: String(e.message).slice(0, 160) }; }
  await reportRunProgress(runRef, { phase: "load_policy",
    label: "Loading the active policy and account state", pct: 6,
    detail: "The frozen definitions are available; loading positions, controls and research snapshots." });

  const ctrl = await controlDoc();
  const operating = STATE.describe(ctrl);
  const strategy = await loadStrategy(ctrl.strategyVersion);
  const exploratoryPolicy = strategy.exploratoryAuto || {};
  /* The exploratory ACTIVITY layer: pacing, Thompson selection among frozen
     policies, the control cohort. Clamped in _investorExplore. It steers
     order and pace among candidates the exploratory gates admitted; it
     cannot admit one they refused. */
  const activity = XP.activityPolicy(strategy);
  const sufficiencyPolicy = DS.policyFrom(strategy);
  const activePortfolioControls = operating.exploratoryAuto
    ? { ...(strategy.portfolioControls || {}),
        ...(exploratoryPolicy.portfolioControls || {}) }
    : (strategy.portfolioControls || {});
  const activeRiskStrategy = { ...strategy,
    portfolioControls: activePortfolioControls };
  const universe = await loadUniverse(ctrl.universeVersion);
  const universeHash = universe.contentHash || B.universeHash(universe);
  const strategyHash = strategy.contentHash || B.strategyHash(strategy);
  const variantsHash = V.variantsHash();
  const policyIdentity = {
    accountId: ctrl.accountId || "paper-1",
    strategyVersion: strategy.version, universeVersion: universe.version,
    strategyHash, universeHash, variantsHash,
  };
  const frozenIdentityValid = universe.immutable === true && strategy.immutable === true
    && B.validateFrozenUniverse(universe, universe.version).ok
    && B.strategyHash(strategy) === strategyHash;
  /* ── THE LOOP CLOSES HERE ──────────────────────────────────────────────
   *
   * This read is what makes the system self-adjusting rather than merely
   * self-measuring. Until it existed the allocator ran every cycle, produced a
   * verdict about which frozen policy was working, wrote it to
   * Firestore, and NOTHING read it back — live trading used the hand-edited
   * baseline forever, whatever the evidence said. The dashboard column headed
   * "Share of money" described a split that did not happen.
   *
   * Now: once the selection-corrected power, absolute, incumbent-relative, and
   * DSR/PBO, historical stress and locked forward gates all open, one leading policy becomes the live
   * configuration. Learner confidence never multiplies position size above the
   * deterministic risk budget. Before validation, `leaderId` is null and the
   * baseline is used unchanged.
   *
   * The read happens HERE, before any candidate is evaluated. Reading it after
   * the trading decisions — where the allocator block sits — would mean the
   * verdict always arrived one cycle too late to matter. */
  const baseParams = strategy.parameters || {};
  let liveVariant = null, allocationRead = null;
  try {
    const aSnap = await A.col(A.COL.control).doc("allocation").get();
    if (aSnap.exists) {
      const a = aSnap.data();
      /* A variants-file edit invalidates every observation recorded against
         the old definitions, so a stale hash must not be allowed to steer the
         paper ledger. Fail back to the baseline and say so. */
      const hashOk = !a.variantsHash || a.variantsHash === V.variantsHash();
      allocationRead = {
        leaderId: a.leaderId || null, powered: !!a.powered,
        leaderWeightPct: a.leaderWeightPct ?? null,
        calibratedNetLowerBoundBps: a.calibratedNetLowerBoundBps ?? null,
        experimentHash: a.experimentHash || null,
        holdoutLock: a.holdoutLock || null,
        forwardLock: a.forwardLock || null,
        forwardResult: a.forwardResult || null,
        overfitGuard: a.overfitGuard || null,
        hashOk, bestEffectiveN: a.bestEffectiveN ?? 0,
        requiredEffectiveN: a.requiredEffectiveN ?? null,
      };
      if (a.powered && a.leaderId && hashOk && a.forwardResult && a.forwardResult.pass === true
          && Number(a.calibratedNetLowerBoundBps) > 0) {
        liveVariant = V.byId(a.leaderId);
      }
    }
  } catch (e) { allocationRead = { error: String(e.message).slice(0, 120) }; }

  /* The live configuration IS the winning variant once there is enough
     evidence to name one. This is the line the whole learning apparatus
     exists to produce. */
  let cfg = liveVariant
    ? { ...V.configFor(liveVariant.id), calibratedExpectedEdgeBps: allocationRead.calibratedNetLowerBoundBps }
    : { ...baseParams };
  let cfgSource = liveVariant
    ? `variant ${liveVariant.id} (${liveVariant.name}) — chosen on ${allocationRead.bestEffectiveN} independent days of evidence`
    : (allocationRead && allocationRead.powered === false
        ? `baseline — only ${allocationRead.bestEffectiveN || 0} of ${allocationRead.requiredEffectiveN || "?"} independent days collected, not enough to favour any variant yet`
        : "baseline — no allocation recorded yet");

  /* Paper learning mode. `strictCfg` is kept untouched so the published
     calibration can be scored alongside whatever the operator loosened. */
  let strictCfg = { ...cfg };
  let paperLearning = require("./_investorStrategy.js").paperLearningConfig(cfg, ctrl);
  cfg = paperLearning.cfg;
  if (paperLearning.active) {
    cfgSource += ` · paper learning mode: ${Object.keys(paperLearning.applied).join(", ") || "no change"}`;
  } else if (paperLearning.refused) {
    cfgSource += ` · ${paperLearning.refused}`;
  }

  const accountId = ctrl.accountId || "paper-1";
  let entryControl = L.controlAllowsEntry(ctrl, policyIdentity);
  const session = M.sessionState(new Date());
  const dailyFinalization = M.dailyFinalizationState(session);
  session.dailyFinalized = dailyFinalization.ready === true
    && ctrl.lastDailyFinalizeDate !== session.date;
  const reg = await regime();

  const tradeTier = universe.tradeTier || [];
  const tradeSymbols = tradeTier.map((t) => t.symbol);

  /* Open positions are part of the management universe even if a symbol has
     fallen out of the current trade roster. This prevents an eligibility,
     coverage, or roster change from making an existing position unreachable
     to marks, stops, and exit settlement. */
  const posSnapAll = await A.col(A.COL.positions).where("accountId", "==", accountId).get();
  const allPositions = [];
  posSnapAll.forEach((d) => allPositions.push(d.data()));
  const positionBySymbol = Object.fromEntries(allPositions.map((p) => [p.symbol, p]));
  const symbols = managedSymbolUnion(tradeSymbols, allPositions);
  const intelligenceBySymbol = {};
  await Promise.all(symbols.map(async (symbol) => {
    try {
      const snapshot = await I.readSnapshot(symbol);
      if (snapshot) intelligenceBySymbol[symbol] = snapshot;
    } catch {}
  }));

  /* Sectors come from the roster, not a hardcoded table — a 350-name roster
     against a stale map would dump most names into one bucket and break the
     factor model. */
  const sectorMap = {};
  for (const t of tradeTier) sectorMap[t.symbol] = t.sector || "other";
  for (const p of allPositions) if (p && p.symbol && !sectorMap[p.symbol]) sectorMap[p.symbol] = p.sector || "other";
  S.setSectorMap(sectorMap);

  await runRef.set({
    strategyVersion: strategy.version, universeVersion: universe.version,
    strategyHash, universeHash, variantsHash, frozenIdentityValid,
    session, regime: reg, symbolCount: symbols.length,
  }, { merge: true });
  await reportRunProgress(runRef, { phase: "market_data",
    label: "Fetching current prices", pct: 10, completed: 0, total: symbols.length,
    detail: `Requesting current bars and source identity for ${symbols.length} managed companies.` });

  /* 1. bars ------------------------------------------------------------- */
  const provider = M.activeProvider();
  let panel = {}, fetchMeta = {}, marketProvenanceBySymbol = {};
  try {
    const got = await M.fetchBarsChunked(symbols, { timeframe: cfg.barTimeframe || "5Min", limit: 120 },
      { chunkSize: 60, retries: 1 });
    panel = got.bars || {};
    fetchMeta = { provider: got.provider, feed: got.feed || null,
      adjustment: got.adjustment || null, fetchedAt: got.fetchedAt,
      manifestSha256: got.manifestSha256 || got.sha256 || null,
      symbolSha256: got.symbolSha256 || {}, note: got.note || null,
      failureCount: got.failureCount || 0,
      chunks: got.chunks || null,
      failedSymbols: got.failedSymbols || [],
      feedRequested: got.feedRequested || null, feedFallback: got.feedFallback || null,
      /* Names the provider answered for the chunk but omitted: delisted,
         renamed, or not on this feed. Surfaced, never silently dropped. */
      missingSymbols: got.missingSymbols || [] };
    for (const sym of symbols) {
      const sourceSha256 = fetchMeta.symbolSha256[sym] || fetchMeta.manifestSha256;
      if (panel[sym] && sourceSha256) marketProvenanceBySymbol[sym] = {
        provider: got.provider, feed: got.feed || null, adjustment: got.adjustment || null,
        sourceSha256, fetchedAt: got.fetchedAt,
      };
    }
  } catch (e) {
    fetchMeta = { provider: provider.id, feed: provider.feed || null,
      adjustment: null, error: String(e.code || e.message).slice(0, 160) };
  }
  await reportRunProgress(runRef, { phase: "market_data",
    label: "Validating current prices", pct: 18, completed: symbols.length,
    total: symbols.length,
    detail: "The market response arrived; validating freshness, feed identity and provenance hashes." });

  /* Fill short responses from storage without throwing away the newest
     current-session quote. The old replacement branch did exactly that during
     the first ~100 minutes of the session: a response with fewer than 20 fresh
     bars was replaced wholesale by yesterday's stored panel. Candidate cards
     therefore showed yesterday even though the provider had supplied today. */
  for (const sym of symbols) {
    const fetchedBars = panel[sym] || [];
    if (fetchedBars.length >= 20) continue;
    try {
      const stored = await M.readRecentBarsWithMeta(sym, 2);
      if (!stored.bars.length || !stored.provenance) continue;
      const fresh = marketProvenanceBySymbol[sym] || null;
      if (!fetchedBars.length) {
        panel[sym] = stored.bars;
        marketProvenanceBySymbol[sym] = stored.provenance;
        continue;
      }
      const sameIdentity = fresh
        && fresh.provider === stored.provenance.provider
        && (fresh.feed || null) === (stored.provenance.feed || null)
        && (fresh.adjustment || null) === (stored.provenance.adjustment || null);
      if (!sameIdentity) continue;
      panel[sym] = M.normalizeBars([...stored.bars, ...fetchedBars]);
      const hashes = [stored.provenance.sourceSha256, fresh.sourceSha256]
        .filter((h) => /^[a-f0-9]{64}$/.test(String(h))).sort();
      marketProvenanceBySymbol[sym] = {
        ...fresh,
        sourceSha256: hashes.length ? sha256Json(hashes) : null,
      };
    } catch {}
  }

  // Persist bars (one doc per symbol per day, bar array inside).
  const today = session.date;
  const quality = {};
  await Promise.all(symbols.map(async (sym) => {
    const bars = panel[sym] || [];
    const mp = marketProvenanceBySymbol[sym] || {};
    quality[sym] = M.gradeSeries(bars, { provider: mp.provider || fetchMeta.provider || provider.id,
      feed: mp.feed || fetchMeta.feed || null, sourceSha256: mp.sourceSha256 || null });
    if (bars.length) {
      try {
        await M.writeBars(sym, today, bars, {
          provider: mp.provider || fetchMeta.provider || provider.id,
          feed: mp.feed || fetchMeta.feed || null,
          adjustment: mp.adjustment || fetchMeta.adjustment || null,
          sourceSha256: mp.sourceSha256 || null,
          grade: quality[sym].grade,
          gradeReasons: quality[sym].reasons,
          feedDelayMinutes: quality[sym].feedDelayMinutes,
        });
      } catch (e) { /* storage failure must not kill the cycle */ }
    }
  }));
  await reportRunProgress(runRef, { phase: "historical_context",
    label: "Loading each company's prior price context", pct: 25,
    completed: 0, total: symbols.length,
    detail: "Current prices are stored; loading completed-session history without using today's future marks." });

  /* 1b. THE LONG MEMORY --------------------------------------------------
     Load each name's daily history and reduce it to the handful of numbers the
     decision path uses: how much this stock normally moves, where it sits in
     its own six-month range, whether it is in a downtrend, and how it has
     behaved after similar drops before.

     `asOf` is today's date, and contextFor() discards every bar dated today or
     later. The six-month picture a decision uses is therefore built only from
     completed sessions before today — today's own move can never help justify
     itself. That cut is what keeps this honest, and history.test.js proves it. */
  const historyCtx = {};      // symbol -> context
  const dailySeriesBySymbol = {}; // retained for point-in-time correlation network
  const dailyProvenanceBySymbol = {}; // feed identity required before correlation can affect risk
  const revRaw = {};          // symbol -> raw reversion events
  let historyDays = 0, historyNames = 0;
  await Promise.all(symbols.map(async (sym) => {
    try {
      const daily = await H.readDailyWithMeta(sym);
      const series = daily.series;
      if (!series.length) return;
      dailySeriesBySymbol[sym] = series;
      dailyProvenanceBySymbol[sym] = daily.provenance || null;
      const ctx = { ...H.contextFor(series, today),
        dailyMarketProvenance: daily.provenance };
      historyCtx[sym] = ctx;
      if (ctx.ok) { historyNames += 1; historyDays += ctx.days; }
      revRaw[sym] = H.reversionEvents(series, today);
    } catch (e) { /* a name without history simply gets no long-horizon read */ }
  }));
  await reportRunProgress(runRef, { phase: "rank_companies",
    label: "Separating company moves from market and sector moves", pct: 35,
    completed: historyNames, total: symbols.length,
    detail: `${historyNames} of ${symbols.length} companies have usable long-horizon context.` });

  /* Shrink every name's own bounce-back record toward the roster-wide average.
     Ten events is not a track record; the shrinkage says so arithmetically. */
  const revShrunk = H.shrinkReversion(revRaw);
  const reversionBySymbol = {};
  for (const [sym, r] of Object.entries(revShrunk.perSymbol)) {
    reversionBySymbol[sym] = {
      ...r,
      multiplier: H.reversionMultiplier(r.shrunkPct, r.pooledPct),
    };
  }

  /* 2 + 3. residuals and ranks ------------------------------------------ */
  /* Ranking is measurement, not execution. A valid single-venue series stays
     in the cross-section in every operating state: switching the paper ledger
     on must not make the same market observations disappear. The old coupling
     (`!operating.paperLedger`) did exactly that, so an Alpaca-IEX panel ranked
     normally in observation mode and fell to zero names as soon as automatic
     paper mode was enabled.

     Nothing here relaxes execution — `_investorWorkset`, the proposal source
     check and `_investorPositionGuard` still refuse anything that is not
     `tradable`. Research-grade input can therefore produce a labelled rating,
     never an executable paper fill.

     The record stays honest by construction rather than by promise: the market
     identity is already part of the shadow experiment hash, so IEX-graded
     observations accumulate under their own experiment and can never be
     blended into consolidated-feed history to satisfy a promotion gate. */
  const allowResearchGrade = true;
  const admits = (q) => !!q && (q.tradable === true
    || (allowResearchGrade && q.researchEligible === true));

  const identityCounts = new Map();
  for (const sym of symbols) {
    const p = marketProvenanceBySymbol[sym];
    if (!p || !admits(quality[sym])) continue;
    const key = JSON.stringify([p.provider, p.feed || null, p.adjustment || null]);
    identityCounts.set(key, (identityCounts.get(key) || 0) + 1);
  }
  const dominantIdentityKey = [...identityCounts.entries()]
    .sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0]))[0]?.[0] || null;
  const dominant = dominantIdentityKey ? JSON.parse(dominantIdentityKey) : [];
  const marketIdentity = dominantIdentityKey ? {
    provider: dominant[0], feed: dominant[1], adjustment: dominant[2],
  } : { provider: fetchMeta.provider || provider.id, feed: fetchMeta.feed || null,
    adjustment: fetchMeta.adjustment || "unknown" };
  const shadowExperiment = SH.experimentIdentity({ universeHash, strategyHash, variantsHash,
    buildCommit: process.env.COMMIT_REF || process.env.DEPLOY_ID || "local",
    intelligenceConfig: {
      watchlist: IS.configuredSymbols(ctrl),
      profiles: ctrl.intelligenceProfiles || {},
      sourceRegistry: IS.SOURCE_REGISTRY,
    },
    marketIdentity });
  if (liveVariant && allocationRead && allocationRead.experimentHash !== shadowExperiment.experimentHash) {
    liveVariant = null;
    /* Re-apply the operator's relaxation after the reset. Without this the
       leader-mismatch branch silently reverted to the published calibration
       and the desk would go quiet again with the mode still showing as on. */
    strictCfg = { ...baseParams };
    paperLearning = require("./_investorStrategy.js").paperLearningConfig(strictCfg, ctrl);
    cfg = paperLearning.cfg;
    cfgSource = "baseline — the stored leader belongs to a different market-data experiment"
      + (paperLearning.active ? " · paper learning mode still applied" : "");
  }
  const measurementPanel = {};
  const rankingInputExclusions = {
    tooFewBars: 0, unusableQuality: 0, marketIdentityMismatch: 0,
  };
  for (const sym of tradeSymbols) {
    const p = marketProvenanceBySymbol[sym];
    const key = p ? JSON.stringify([p.provider, p.feed || null, p.adjustment || null]) : null;
    if ((panel[sym] || []).length < 24) {
      rankingInputExclusions.tooFewBars += 1;
      continue;
    }
    if (!admits(quality[sym])) {
      rankingInputExclusions.unusableQuality += 1;
      continue;
    }
    if (!dominantIdentityKey || key !== dominantIdentityKey) {
      rankingInputExclusions.marketIdentityMismatch += 1;
      continue;
    }
    measurementPanel[sym] = panel[sym];
  }
  /* Build every preregistered formation horizon from the same point-in-time
     panel. A 6-bar or 24-bar challenger must own its residual fit, z-score and
     cross-sectional rank; applying a different threshold to the 12-bar score
     would be a mislabeled experiment. */
  const signalWindows = [...new Set([cfg.signalWindow || 12,
    ...V.VARIANTS.map((v) => V.configFor(v.id).signalWindow || 12)])].sort((a, b) => a - b);
  const signalContexts = {};
  for (const signalWindow of signalWindows) {
    const panelResult = S.residualPanel(measurementPanel, {
      signalWindow, minCoverageRatio: 0.65, minSymbolCoverageRatio: 0.80,
      intervalMs: barTimeframeMs(cfg.barTimeframe || "5Min"), quality,
      allowResearchGrade,
    });
    const windowZ = {};
    for (const sym of managedSymbolUnion(panelResult.symbols, allPositions)) {
      const z = S.residualZ(panelResult.residuals[sym], signalWindow, historyCtx[sym]);
      if (z) windowZ[sym] = z;
    }
    const rankedWindow = S.crossSectionalRanks(windowZ);
    signalContexts[signalWindow] = { rp: panelResult, zBySymbol: windowZ,
      ranks: rankedWindow.ranks, n: rankedWindow.n };
  }
  const liveSignalContext = signalContexts[cfg.signalWindow || 12]
    || signalContexts[12] || { rp: { symbols: [], residuals: {}, betas: {} }, zBySymbol: {}, ranks: {}, n: 0 };
  const rp = liveSignalContext.rp;
  const zBySymbol = liveSignalContext.zBySymbol;
  const ranks = liveSignalContext.ranks;
  const ranked = liveSignalContext.n;
  /* Keep one compact market-coverage row for every frozen trade-tier name.
     Ranked candidate documents remain reserved for actual scores, while this
     roster makes an exclusion visible instead of making the company vanish. */
  const residualSymbolSet = new Set(rp.symbols || []);
  const rosterBySymbol = Object.fromEntries(tradeTier.map((row) => [row.symbol, row]));
  const quoteCoverage = tradeSymbols.map((sym) => {
    const bars = panel[sym] || [], last = bars.at(-1) || null;
    const q = quality[sym] || {};
    const p = marketProvenanceBySymbol[sym] || {};
    const identityKey = p.provider
      ? JSON.stringify([p.provider, p.feed || null, p.adjustment || null]) : null;
    let status = "ranked", exclusionReason = null;
    if ((bars || []).length < 24) {
      status = "too_few_bars";
      exclusionReason = bars.length ? `only ${bars.length} usable bars; 24 required` : "no bars returned";
    } else if (!admits(q)) {
      status = "unusable_quality";
      exclusionReason = (q.reasons || []).join(", ") || "price freshness or provenance failed";
    } else if (!dominantIdentityKey || identityKey !== dominantIdentityKey) {
      status = "market_identity_mismatch";
      exclusionReason = "provider/feed/adjustment differs from the dominant cross-section";
    } else if (!residualSymbolSet.has(sym)) {
      status = "synchronous_coverage";
      exclusionReason = "not enough timestamps overlap the rest of the roster";
    } else if (!Object.prototype.hasOwnProperty.call(ranks, sym)) {
      status = "signal_history";
      exclusionReason = "not enough residual history to calculate a z-score";
    }
    const quoteSessionDate = last ? M.nyParts(new Date(last.t)).date : null;
    const row = rosterBySymbol[sym] || {};
    return {
      symbol: sym, company: row.company || sym, sector: row.sector || "other",
      status, exclusionReason, ranked: status === "ranked",
      lastPrice: last && Number.isFinite(Number(last.c)) ? Number(last.c) : null,
      lastBarAt: last ? last.t : null, quoteSessionDate,
      currentTradingDay: quoteSessionDate === session.date,
      barCount: bars.length, grade: q.grade || "F",
      qualityReasons: q.reasons || [],
      researchEligible: q.researchEligible === true,
      executionEligible: q.tradable === true,
      provider: p.provider || q.provider || fetchMeta.provider || provider.id,
      feed: p.feed || q.feed || fetchMeta.feed || null,
      feedDelayMinutes: q.feedDelayMinutes ?? provider.delayMinutes ?? null,
    };
  });
  const rankingDiagnostics = {
    rosterChecked: tradeSymbols.length,
    barsReceived: quoteCoverage.filter((row) => row.barCount > 0).length,
    currentSessionPrices: quoteCoverage.filter((row) => row.currentTradingDay
      && row.lastPrice != null).length,
    researchEligible: quoteCoverage.filter((row) => row.researchEligible).length,
    executionEligible: quoteCoverage.filter((row) => row.executionEligible).length,
    admittedToPanel: Object.keys(measurementPanel).length,
    retainedAfterSynchronousCoverage: (rp.symbols || []).length,
    ranked,
    excludedBeforePanel: rankingInputExclusions,
    excludedForSynchronousCoverage: Math.max(0,
      Object.keys(measurementPanel).length - (rp.symbols || []).length),
    excludedForSignalHistory: Math.max(0, (rp.symbols || []).length - ranked),
    /* `ranked` counts the whole cross-section, held names included; held
       names are evaluated for EXIT and get no entry card. The console shows
       both numbers so "ranked 59" and "39 candidates" are not read as a
       discrepancy. */
    fetch: { chunks: fetchMeta.chunks || null, failedSymbols: (fetchMeta.failedSymbols || []).slice(0, 40),
      feedRequested: fetchMeta.feedRequested || null, feedFallback: fetchMeta.feedFallback || null,
      failedSymbolCount: (fetchMeta.failedSymbols || []).length,
      missingSymbols: (fetchMeta.missingSymbols || []).slice(0, 40),
      missingSymbolCount: (fetchMeta.missingSymbols || []).length, error: fetchMeta.error || null },
    heldRanked: Object.keys(ranks).filter((sym) => positionBySymbol[sym] && positionBySymbol[sym].open).length,
    entryCandidates: Math.max(0, ranked - Object.keys(ranks)
      .filter((sym) => positionBySymbol[sym] && positionBySymbol[sym].open).length),
    panelNote: rp.note || null,
    dominantMarketIdentity: marketIdentity,
  };
  const crowd = S.sectorCrowding(ranks, S.sectorOf, cfg.entryRank ?? S.ENTRY_RANK);
  await reportRunProgress(runRef, { phase: "evaluate_companies",
    label: "Evaluating companies and existing holdings", pct: 46,
    completed: 0, total: ranked,
    detail: `${ranked} companies were ranked; exits are checked before new-entry gates.` });

  /* 4-6. evaluate each symbol ------------------------------------------- */
  const cycleId = `${today}_${new Date().toISOString().slice(11, 16).replace(":", "")}`;
  const candidates = [], decisions = [], exits = [], portfolioBlocks = [], proposalQueue = [];
  const controlPool = [];
  /* Near-miss names for the strike tier: every hazard gate passed, only the
     signal (and the signal-dependent cost) still short. Collected here,
     selected and written after portfolio selection. */
  const planCandidates = [];
  /* THE PATIENCE SLEEVE. Resolved once per scan. `patienceClaims` tracks the
     grants this scan has already made: a grant is not yet in `pendingOrders`,
     so without it the sleeve cap would be measured against a stale book and
     one scan could over-fill it several times over. */
  const patiencePolicy = PA.policyFrom(strategy);
  const patienceClaims = [];
  const planSessionCloseMs = (() => {
    try { const c = M.sessionCloseMs ? M.sessionCloseMs(new Date()) : null; return Number.isFinite(Number(c)) ? Number(c) : null; }
    catch { return null; }
  })();
  const decisionManifestStats = { total: 0, complete: 0, invalid: 0 };
  let modelCalls = 0;

  const causeBySymbol = {};
  const coverageBySymbol = {};
  const attentionBySymbol = {};
  const lastPriceBySymbol = {};
  const corporateActionQuarantineBySymbol = {};
  const corporateActionWrites = [];
  const tierBySymbol = Object.fromEntries(tradeTier.map((t) => [t.symbol, t]));
  /* Meta carries MEASURED volume, overriding anything asserted in the file. */
  const metaBySymbol = {};
  const advProvisional = [];
  for (const sym of symbols) {
    const t = tierBySymbol[sym] || { symbol: sym, sector: sectorMap[sym] || "other" };
    const adv = advFor(sym, panel[sym], marketProvenanceBySymbol[sym], historyCtx);
    metaBySymbol[sym] = { ...t, advUsd: adv.advUsd, advSource: adv.source,
      advProvisional: adv.provisional, feedVolumeShare: adv.feedVolumeShare,
      advComponents: adv.components };
    if (adv.provisional && adv.advUsd > 0) advProvisional.push(sym);
  }
  /* Derived earnings windows, keyed by symbol. A symbol missing from this map
     has no known window and is BLOCKED — an unknown earnings date is the most
     dangerous state for this strategy, so the failure mode is "does not
     trade", never "trades blind". */
  const earningsWindows = await B.readEarnings();

  /* Trusted marks for every symbol come from the quality-gated dominant feed,
     BEFORE anything reads them. A positive stored price is not automatically
     a current executable mark: stale/F/non-consolidated data must remain
     absent so the account breaker halts new risk instead of hiding a loss.
     This map used to be filled inside the per-symbol loop below,
     which runs after the book is built — so the book was summarised against an
     empty map, every position was valued at cost, unrealised losses were
     invisible, and the drawdown breaker could not fire on an open loss. Worse,
     held symbols exited that loop early and never got a mark at all. */
  for (const sym of symbols) {
    const b = panel[sym];
    const p = marketProvenanceBySymbol[sym];
    const key = p ? JSON.stringify([p.provider, p.feed || null, p.adjustment || null]) : null;
    /* The shadow ledger prices its paper entries and exits from this map. Held
       to execution grade it stays empty on a single-venue feed, and a paper
       desk with no marks records nothing at all — which is the failure this
       release exists to remove. The source hash is still mandatory. */
    if (b && b.length && p && admits(quality[sym])
        && key === dominantIdentityKey
        && /^[a-f0-9]{64}$/.test(String(p.sourceSha256 || ""))) {
      const lastTrustedBar = b[b.length - 1];
      const currentPrice = lastTrustedBar.c;
      const markProvenance = { ...p, barOpenAt: lastTrustedBar.t };
      marketProvenanceBySymbol[sym] = markProvenance;
      const held = positionBySymbol[sym];
      const assessment = held && held.open
        ? CA.assessPositionMark({ position: held, currentPrice,
          currentProvenance: markProvenance })
        : { quarantine: false };
      if (assessment.quarantine) {
        const pending = { ...assessment, status: "pending_operator_confirmation",
          detectedAtMs: assessment.detectedAtMs || Date.now(), currentMarkAt: b[b.length - 1].t,
          currentProvenance: markProvenance };
        corporateActionQuarantineBySymbol[sym] = pending;
        corporateActionWrites.push(A.col(A.COL.positions).doc(`${accountId}_${sym}`).set({
          corporateActionPending: pending,
          markQuarantinedAt: A.FV.serverTimestamp(),
          updated_at: A.FV.serverTimestamp(),
        }, { merge: true }));
      } else {
        lastPriceBySymbol[sym] = currentPrice;
      }
    }
  }
  if (corporateActionWrites.length) await Promise.allSettled(corporateActionWrites);

  /* 1a-ter. TURNOVER PERCENTILES ----------------------------------------
     turnover = dollar volume / price / shares outstanding, ranked across the
     roster. The gate refuses reversal entries in the top decile — in those
     names losers keep falling (Medhat & Schmeling, RFS 2022). Names without
     shares data get null and the gate abstains for them. */
  const sharesBySymbol = await B.readShares().catch(() => ({}));
  const turnoverPctile = {};
  {
    const raw = [];
    for (const sym of symbols) {
      const sh = sharesBySymbol[sym];
      const advUsd = W.preferredAdvUsd(metaBySymbol[sym], tierBySymbol[sym]);
      const px = lastPriceBySymbol[sym];
      if (sh && sh.shares > 0 && advUsd > 0 && px > 0
          && !(metaBySymbol[sym] || {}).advProvisional) {
        raw.push([sym, (advUsd / px) / sh.shares]);
      }
    }
    raw.sort((a, b) => a[1] - b[1]);
    const n = raw.length;
    raw.forEach(([sym], i) => { turnoverPctile[sym] = n > 1 ? i / (n - 1) : 0.5; });
  }


  /* ── THE BOOK ────────────────────────────────────────────────────────
     Everything above this line asks "is this one name a good idea". Nothing
     did — until now — ask "given what we already hold, should we hold more".
     Nine individually-oversold semiconductors are one bet with nine times the
     size, and the strategy file's portfolioControls block was read by nothing
     at all. It is enforced from here. */
  const navSnapshot = await L.balances(accountId);
  const cashUsdNow = navSnapshot.usd[L.ACCT.CASH] || 0;
  const marked = R.markedBook(allPositions, lastPriceBySymbol, S.sectorOf, {
    cash: cashUsdNow, reserved: navSnapshot.usd[L.ACCT.RESERVED] || 0,
  });
  const book = marked.book;
  const navUsd = Math.max(1, marked.navUsd);
  let ledgerAuditResult = null, executionAuditResult = null;
  try {
    [ledgerAuditResult, executionAuditResult] = await Promise.all([
      L.auditLedger(accountId), L.executionAudit(accountId),
    ]);
    if (!ledgerAuditResult.pass || !executionAuditResult.pass) {
      entryControl = { pass: false, reason: !ledgerAuditResult.pass
        ? "ledger reconciliation failed" : "execution provenance audit failed" };
    }
  } catch (e) {
    entryControl = { pass: false, reason: "safety audit unavailable" };
  }

  /* Account-level circuit breakers, checked once. If the account is in
     trouble the right number of new positions is zero, however good the next
     idea looks. Exits are deliberately NOT blocked by a halt — being unable to
     sell during a drawdown is how a pause becomes a disaster. */
  const hwm = Math.max(Number(ctrl.highWaterMarkUsd) || 0, navUsd);
  /* On the first cycle of a new trading date, today's opening baseline is the
     current NAV. Reusing yesterday's baseline for this calculation made the
     first cycle report yesterday's move as today's P/L and could trip the
     one-day breaker before today's session had moved at all. */
  const storedStartOfDayNav = Number(ctrl.startOfDayNavUsd);
  const startOfDayNav = ctrl.startOfDayNavDate === today
    && Number.isFinite(storedStartOfDayNav) && storedStartOfDayNav > 0
    ? storedStartOfDayNav : navUsd;
  const riskState = {
    navUsd, hwmUsd: hwm,
    untrustedOpenMarks: marked.untrustedMarks,
    drawdownPct: hwm > 0 ? ((navUsd - hwm) / hwm) * 100 : 0,
    dayPnlPct: startOfDayNav > 0 ? ((navUsd - startOfDayNav) / startOfDayNav) * 100 : 0,
  };
  const breakers = R.accountBreakers(riskState, activeRiskStrategy);

  /* Symbols with an order already in flight. The duplicate check in the
     portfolio gate looks at POSITIONS, so a proposed-but-unapproved order was
     invisible to it and the same dip re-proposed the same symbol every five
     minutes — twelve stacked orders an hour for one name, each approvable. */
  const pendingOrderSymbols = new Set();
  const pendingOrders = [];
  let pendingExposureUnavailable = null;
  try {
    for (const st of ["proposed", "approved"]) {
      const q = await A.col(A.COL.orders)
        .where("accountId", "==", accountId).where("status", "==", st).get();
      q.forEach((d) => { pendingOrderSymbols.add(d.data().symbol); pendingOrders.push(d.data()); });
    }
  } catch (e) {
    /* FAIL CLOSED. If the committed exposure cannot be read, this cycle does
       not know what the book is committed to, so it must not add to it. The
       fills and exits below still run; only new entries are refused, and the
       console says why. */
    pendingExposureUnavailable = String(e.message || e).slice(0, 120);
    entryControl = { pass: false, reason: `pending_exposure_unavailable: ${pendingExposureUnavailable}` };
  }

  /* ── PENDING EXPOSURE IS EXPOSURE ──────────────────────────────────────
     An order proposed or approved in an earlier cycle is a position the book
     is committed to; on a 15-minute-delayed feed it stays pending for half an
     hour, i.e. across several cycles. It used to survive only as a duplicate
     guard, so each cycle's portfolio checks were run against the FILLED book
     alone and several individually-valid manifests could sum to a book above
     every declared cap once they filled. Every pending order is folded into
     the starting book here at its requested notional (qty x reference price),
     against whichever cap set is active, and its cash is treated as spent:
     reserved cash for approved orders is already outside CASH, and a
     proposed-but-unapproved order's notional is deducted below. The
     reservation is recorded on the run so the audit can reconcile it. */
  const pendingFold = R.foldPendingOrders(book, pendingOrders, navUsd, S.sectorOf);
  const pendingExposure = { orders: pendingFold.orders, usd: pendingFold.usd, symbols: pendingFold.symbols };
  const proposedUnreservedUsd = pendingFold.proposedUnreservedUsd;

  /* ── EXPLORATORY SCOREBOARD ───────────────────────────────────────────
     Closed exploratory paper trades, attributed to the frozen policies whose
     entry rule fired for them, become a shrunk posterior per policy. It is
     read here, before any candidate is evaluated, and it steers only the
     ORDER and PACE of exploratory entries this cycle. */
  let scoreboard = null, controlOpen = 0, controlNewThisSession = 0;
  if (operating.exploratoryAuto && activity.enabled) {
    try {
      /* Only outcomes from THIS experiment steer this experiment: same frozen
         strategy/universe/variants identity and the same exploratory policy
         version, newest first, bounded by the lookback. The admitted sample
         is hashed so the selection is reproducible from the record. */
      const tradeSnap = await A.col(A.COL.trades).where("accountId", "==", accountId).get();
      const allTrades = [];
      tradeSnap.forEach((d) => allTrades.push(d.data()));
      const admitted = XP.admitScoreboardTrades(allTrades, {
        strategyHash, universeHash, variantsHash,
        exploratoryPolicyVersion: exploratoryPolicy.version || null,
        sufficiencyVersion: sufficiencyPolicy.version,
        lookback: activity.scoreboardLookbackTrades });
      scoreboard = XP.buildScoreboard(admitted.trades, { policySelection: activity.policySelection,
        policyIds: V.VARIANTS.map((v) => v.id) });
      scoreboard.sample = { admitted: admitted.trades.length, considered: allTrades.length,
        excluded: admitted.excluded, sampleHash: admitted.sampleHash,
        identity: { strategyHash, universeHash, variantsHash,
          exploratoryPolicyVersion: exploratoryPolicy.version || null,
          sufficiencyVersion: sufficiencyPolicy.version } };
    } catch (e) {
      scoreboard = { error: String(e.message).slice(0, 120), policies: {}, cohorts: {} };
    }
    controlOpen = allPositions.filter((p) => p.open && XP.isControlOrder(p)).length
      + pendingOrders.filter((o) => XP.isControlOrder(o)).length;
    try {
      const controlToday = await A.col(A.COL.orders).where("accountId", "==", accountId)
        .where("cohortRole", "==", "control").where("decisionSessionDate", "==", today).get();
      controlNewThisSession = controlToday.size;
    } catch (e) { controlNewThisSession = activity.controlCohort.maxNewPerSession; }
  }

  /* One NAV mark per trading day is what every performance number is built
     from. Intraday rows are explicitly provisional. The finalized row is
     written again only after this cycle's fills and exits settle, otherwise a
     cycle that opens or closes a position would permanently attest the book
     that existed before its own transaction. */
  let navMarkWritten = false, navMarkResult = null;
  const dailyAlreadyFinalized = ctrl.lastDailyFinalizeDate === today;
  async function writeNavSnapshot({ positionRows, snapshotNavUsd, snapshotCashUsd,
    snapshotPositionsUsd, snapshotOpenPositions, snapshotUnrealisedUsd, finalized }) {
    const navMarkSources = [], missingMarkSymbols = [];
    const open = (positionRows || [])
      .filter((p) => p && p.open && Number(p.qty) > 0);
    for (const position of open) {
      const price = Number(lastPriceBySymbol[position.symbol]);
      const provenance = marketProvenanceBySymbol[position.symbol];
      if (!(price > 0) || !provenance
          || !/^[a-f0-9]{64}$/.test(String(provenance.sourceSha256 || ""))) {
        missingMarkSymbols.push(position.symbol); continue;
      }
      navMarkSources.push({ symbol: position.symbol, price: Number(price.toFixed(8)),
        provider: provenance.provider, feed: provenance.feed || null,
        adjustment: provenance.adjustment || null,
        barOpenAt: provenance.barOpenAt || null,
        sourceSha256: provenance.sourceSha256 });
    }
    navMarkSources.sort((a, b) => a.symbol.localeCompare(b.symbol));
    missingMarkSymbols.sort();
    const marksComplete = missingMarkSymbols.length === 0;
    const priorCompleteDate = ctrl.lastNavCompleteDate || null;
    const sessionsSpanned = priorCompleteDate
      ? M.tradingSessionsBetween(priorCompleteDate, today) : null;
    const returnAdmissible = marksComplete && Number(sessionsSpanned) === 1;
    const markSetSha256 = sha256Json(navMarkSources);
    await A.col(A.COL.control).doc("control").collection("navHistory").doc(today).set({
      date: today, navUsd: Number(snapshotNavUsd.toFixed(2)),
      cashUsd: Number(snapshotCashUsd.toFixed(2)),
      positionsUsd: Number(snapshotPositionsUsd.toFixed(2)),
      openPositions: snapshotOpenPositions,
      unrealisedUsd: Number(snapshotUnrealisedUsd.toFixed(2)),
      marksComplete,
      missingMarkSymbols,
      markSources: navMarkSources,
      markSetSha256,
      priorCompleteDate,
      sessionsSpanned: Number.isFinite(Number(sessionsSpanned))
        ? Number(sessionsSpanned) : null,
      returnAdmissible,
      ...(finalized === true
        ? { finalized: true, finalizedAt: A.FV.serverTimestamp() }
        : { finalized: false }),
      updatedAt: A.FV.serverTimestamp(),
    }, { merge: true });
    return { written: true, finalized: finalized === true, marksComplete,
      missingMarkSymbols, markSetSha256, priorCompleteDate,
      sessionsSpanned: Number.isFinite(Number(sessionsSpanned))
        ? Number(sessionsSpanned) : null,
      returnAdmissible };
  }

  /* A provisional row may be refreshed throughout the session. A final row
     is immutable once the combined NAV + learning finalization checkpoint is
     committed in control. */
  if (session.dailyFinalized !== true && !dailyAlreadyFinalized) {
    try {
      navMarkResult = await writeNavSnapshot({ positionRows: allPositions,
        snapshotNavUsd: navUsd, snapshotCashUsd: cashUsdNow,
        snapshotPositionsUsd: book.grossUsd, snapshotOpenPositions: book.count,
        snapshotUnrealisedUsd: book.unrealisedUsd, finalized: false });
    } catch (e) { /* a missed provisional NAV mark must not stop the cycle */ }
  }
  await A.col(A.COL.control).doc("control").set({
    highWaterMarkUsd: hwm,
    startOfDayNavUsd: startOfDayNav,
    startOfDayNavDate: today,
  }, { merge: true });

  const managementWorkset = W.buildManagementWorkset(rp.symbols, allPositions);
  const evaluatedHoldingSymbols = new Set();
  const unpricedHoldings = [];
  const orphanSymbols = allPositions
    .filter((p) => p && p.open && p.symbol && !tradeSymbols.includes(p.symbol))
    .map((p) => p.symbol);
  let lastEvaluationReportMs = 0;
  for (let workIndex = 0; workIndex < managementWorkset.length; workIndex += 1) {
    const work = managementWorkset[workIndex];
    const sym = work.symbol;
    if (workIndex === 0 || Date.now() - lastEvaluationReportMs >= LIVE_FLUSH_MS) {
      lastEvaluationReportMs = Date.now();
      await reportRunProgress(runRef, { phase: "evaluate_companies",
        label: "Evaluating companies and existing holdings",
        pct: 46 + Math.round(26 * workIndex / Math.max(1, managementWorkset.length)),
        completed: workIndex, total: managementWorkset.length, currentItem: sym,
        detail: `Checking ${sym}: held-position exits first, then data, evidence, cost and portfolio gates.` });
    }
    const meta = tierBySymbol[sym] || {};
    const bars = panel[sym] || [];
    const last = bars[bars.length - 1];
    const position = work.position || positionBySymbol[sym] || null;
    if (!last) {
      await liveEvent("unpriced", sym, position && position.open ? "held, no bars this scan" : "no bars this scan");
      if (position && position.open) {
        unpricedHoldings.push(sym);
        exits.push({ symbol: sym, kind: "unpriced", blocked: "no_bars_this_cycle",
          reason: "held position could not be evaluated because no bars were available this cycle" });
      }
      continue;
    }

    /* --- exits first: an open position is evaluated every cycle, which is
       the single highest-value use of the 5-minute loop. The research is
       explicit that the exit rule was worth more than any entry refinement. */
    if (position && position.open) {
      if (corporateActionQuarantineBySymbol[sym] || position.corporateActionPending) {
        const pending = corporateActionQuarantineBySymbol[sym] || position.corporateActionPending;
        exits.push({ symbol: sym, kind: "corporate_action_quarantine",
          blocked: "operator_confirmation_required", reason: pending.reason || null,
          shareRatio: pending.shareRatio || null });
        continue;
      }
      const heldDays = M.tradingDaysHeld(position.openedAt, Date.now()) ?? 0;
      const markProv = marketProvenanceBySymbol[sym];
      /* markQuality is written alongside, so a mark taken at research grade is
         readable as such forever rather than passing as a consolidated one. */
      if (lastPriceBySymbol[sym] != null && admits(quality[sym])
          && markProv && /^[a-f0-9]{64}$/.test(String(markProv.sourceSha256 || ""))) {
        try { await A.col(A.COL.positions).doc(`${accountId}_${sym}`).set({
          lastMarkUsd: last.c, lastMarkAt: last.t,
          lastMarkProvenance: { ...markProv, barOpenAt: last.t },
          markQuality: quality[sym] || null,
          lastExecutionCostContext: M.executionCostContext({
            advUsd: (metaBySymbol[sym] || {}).advUsd || 0,
            grade: quality[sym].grade,
            wideSpreadWindow: session.wideSpreadWindow,
            vixNorm: reg.vixNorm,
          }),
        }, { merge: true }); } catch {}
      }

      /* The exit now SEES THE PRICE. Previously exitSignal took only rank and
         holding time, so a position down 40% was invisible to it — and worse,
         a collapsing name's rank moves away from the exit threshold, meaning
         falling harder made a sale less likely. Stops come first, before any
         rank logic can defer them. */
      const ew = earningsWindows[sym];
      let earningsInDays = null;
      if (ew && ew.dates && ew.dates.length) {
        const soonest = ew.dates
          .map((d) => (Date.parse(d) - Date.now()) / 864e5)
          .filter((d) => d >= 0).sort((a, b) => a - b)[0];
        if (soonest != null) earningsInDays = soonest;
      }
      const peak = Math.max(Number(position.peakPriceUsd) || 0, last.c);
      if (peak > (Number(position.peakPriceUsd) || 0)) {
        try {
          await A.col(A.COL.positions).doc(`${accountId}_${sym}`)
            .set({ peakPriceUsd: peak }, { merge: true });
        } catch {}
      }

      /* `entryPriceUsd` is what recordFill actually writes. Reading `avgPrice`
         here silently produced NaN, which made havePrice false inside
         exitSignal and skipped the hard stop, the trailing stop, the profit
         target and the earnings exit — every price-based protection, on every
         real position. The end-to-end run caught it; nothing else did. */
      const entryUsd = Number(position.entryPriceUsd != null ? position.entryPriceUsd : position.avgPrice);
      const intelligence = intelligenceBySymbol[sym] || null;
      const intelligencePolicy = intelligence ? I.decisionPolicy({ coverage: intelligence.coverage,
        events: intelligence.events, temporalContext: intelligence.temporalContext,
        requireTemporalContext: false, asOfMs: Date.now(),
        maxAgeHours: cfg.intelligenceMaxAgeHours,
        temporalMaxAgeHours: cfg.temporalMaxAgeHours }) : null;
      /* A patient position's own terms: an extended time stop, and the rank
         exit suppressed while it is underwater inside that window. Computed
         from the stamp the position already carries, never re-decided. */
      const pnlPctNow = entryUsd > 0 && Number.isFinite(last.c)
        ? ((last.c - entryUsd) / entryUsd) * 100 : null;
      const patienceTerms = PA.exitTerms(position, { heldDays, pnlPct: pnlPctNow });
      const ex = S.exitSignal(ranks[sym], heldDays, cfg, {
        mark: last.c, entry: entryUsd, peak, earningsInDays, intelligencePolicy,
        patience: patienceTerms,
      });
      evaluatedHoldingSymbols.add(sym);
      /* Preset exit levels in dollars, for the console and the strike pass. */
      try {
        await A.col(A.COL.positions).doc(`${accountId}_${sym}`).set({
          exitLevels: EXITS.exitLevels(cfg, { entry: entryUsd, peak, heldDays, earningsInDays }),
          exitLevelsTimeframe: cfg.barTimeframe || "5Min" }, { merge: true });
      } catch {}
      if (ex.exit) {
        /* THE EXIT INTENT. The execution clock only fills on a bar printed
           after the decision, and with a 15-minute-delayed feed no such bar
           exists in THIS cycle's panel. The old code re-stamped the decision
           time as Date.now() on every cycle, so the clock restarted every five
           minutes and no exit — including the hard stop — could ever execute.
           The decision time is now recorded ONCE, on the position, exactly as
           an order records it for a buy: a later cycle's fresh bars become
           eligible against the original timestamp and the close goes through. */
        if (!position.exitIntent || !position.exitIntent.decisionAtMs) {
          const decisionAtMs = Date.now();
          const exitProvenance = marketProvenanceBySymbol[sym];
          const sourceCheck = PG.trustedDecisionSource(exitProvenance, quality[sym]);
          if (!sourceCheck.pass) {
            exits.push({ symbol: sym,
              reason: `${ex.reason}; exit intent waiting for a trusted adjustment-bound source`,
              kind: ex.kind, urgent: !!ex.urgent, blocked: sourceCheck.reason });
            continue;
          }
          const feedCfg = M.providerConfig(exitProvenance.provider,
            exitProvenance.feed);
          const eligibleAfterMs = decisionAtMs + feedCfg.delayMinutes * 60000
            + (cfg.executionLatencyMs || 60000);
          const armed = await PG.armExitIntent({ accountId, symbol: sym,
            decisionId: `${cycleId}_${sym}_exit`,
            intent: {
              decisionAtMs, eligibleAfterMs, decisionAt: new Date(decisionAtMs).toISOString(),
              reason: ex.reason, kind: ex.kind, urgent: !!ex.urgent,
              pnlPctAtSignal: ex.pnlPct,
              decisionMarketProvenance: exitProvenance,
              decisionQuality: quality[sym],
            },
            decisionRecord: { cycleId, symbol: sym, kind: "exit_signal",
              reason: ex.reason, exitKind: ex.kind, urgent: !!ex.urgent,
              pnlPct: ex.pnlPct, markUsd: last.c, entryUsd: entryUsd || null,
              rank: ranks[sym], heldDays,
              strategyVersion: strategy.version,
              universeVersion: universe.version,
              universeHash, strategyHash, variantsHash },
          });
          if (armed.blocked) {
            await liveEvent("exit_blocked", sym, armed.blocked);
            exits.push({ symbol: sym, reason: ex.reason, kind: ex.kind,
              urgent: !!ex.urgent, blocked: armed.blocked });
            continue;
          }
        }
        await liveEvent("exit_armed", sym, ex.kind || ex.reason);
        exits.push({
          symbol: sym, reason: ex.reason, kind: ex.kind, urgent: !!ex.urgent,
          pnlPct: ex.pnlPct, rank: ranks[sym], heldDays: Number(heldDays.toFixed(2)),
        });
      }
      if (!(exits.length && exits[exits.length - 1].symbol === sym)) await liveEvent("held", sym, "holding, no exit");
      continue;   // never propose an entry in a name we already hold
    }

    // Held-only symbols are included for protection, never for fresh entries.
    if (!work.entryEligible) continue;

    /* A halted account opens nothing. Exits above still run — being unable to
       sell during a drawdown is how a pause becomes a disaster. */
    if (breakers.halted) { causeBySymbol[sym] = S.CAUSE.PENDING; continue; }

    /* --- entries: only threshold breaches proceed past this line --------- */
    const z = zBySymbol[sym];
    const rank = ranks[sym];
    const breach = !!(z && S.entrySignal(rank, z.z, cfg, historyCtx[sym], { price: lastPriceBySymbol[sym] }).fire);
    const firingPolicies = V.VARIANTS.map((v) => {
      const policy = V.configFor(v.id);
      const sc = signalContexts[policy.signalWindow || 12] || liveSignalContext;
      const policyZ = sc.zBySymbol[sym], policyRank = sc.ranks[sym];
      return policyZ && S.entrySignal(policyRank, policyZ.z, policy, historyCtx[sym], { price: lastPriceBySymbol[sym] }).fire
        ? { variantId: v.id, policy, sc, z: policyZ, rank: policyRank } : null;
    }).filter(Boolean);
    const evidenceTrigger = firingPolicies.length > 0;
    const evidenceContext = (z ? { policy: cfg, sc: liveSignalContext, z, rank } : firingPolicies[0]) || null;

    const intelligence = intelligenceBySymbol[sym] || null;
    const intelligencePolicy = intelligence ? I.decisionPolicy({ coverage: intelligence.coverage,
      events: intelligence.events, temporalContext: intelligence.temporalContext,
      requireTemporalContext: cfg.requireTemporalContext === true, asOfMs: Date.now(),
      maxAgeHours: cfg.intelligenceMaxAgeHours,
      temporalMaxAgeHours: cfg.temporalMaxAgeHours }) : null;
    let cause = S.CAUSE.PENDING, causeDetail = null,
      coverage = intelligence && intelligence.coverage || null;
    causeBySymbol[sym] = cause;

    if (evidenceTrigger) {
      /* 4. a breach earns an evidence lookup. Nothing else does. */
      const evidenceWindow = evidenceContext.policy.signalWindow || 12;
      const evidenceZ = evidenceContext.z;
      const att = attentionZ(bars, evidenceWindow);
      attentionBySymbol[sym] = att;
      const moveStartedAtMs = Date.parse((evidenceContext.sc.rp.residualTimestamps[sym] || [])
        .slice(-(evidenceZ.window || evidenceWindow))[0] || "");
      let documents = [];
      try {
        const secCoverage = meta.cik ? await E.coverageRoster(sym, meta.cik) : null;
        coverage = intelligence && intelligence.coverage || secCoverage;
        coverageBySymbol[sym] = coverage;
        if (meta.cik) {
          const polled = await E.pollEdgar(meta.cik, { forms: "" });
          for (const d of (polled.entries || []).slice(0, 8)) {
            try {
              const body = await E.fetchFilingBody(d);
              await E.recordVersion({ symbol: sym, sourceId: "sec.latest", entry: d,
                rawSha256: polled.sha256, body: body.text, bodySha256: body.sha256 });
            } catch {}
          }
          coverage = intelligence && intelligence.coverage || await E.coverageRoster(sym, meta.cik);
        }
        documents = await E.documentsForMove(sym, moveStartedAtMs, Date.now());
        coverageBySymbol[sym] = coverage;
      } catch (e) { /* evidence lane failure degrades to "pending", never crashes */ }

      const pre = E.preClassify({
        symbol: sym, freshDocs: documents, residualZ: evidenceZ.z, attentionScore: att,
        hoursSinceMove: evidenceWindow * 5 / 60,
        moveStartedAtMs, decisionAtMs: Date.now(),
        coverageComplete: !!(intelligencePolicy && intelligencePolicy.fresh && intelligencePolicy.complete),
      });
      cause = pre.cause; causeDetail = pre;

      /* 5. escalate to the model ONLY when deterministic rules are ambiguous */
      if (pre.needsModel && modelCalls < O.CYCLE_CALL_CEILING) {
        modelCalls += 1;
        const llm = await O.classifyMove({
          symbol: sym, role: "classify", documents,
          moveSummary: `residual ${(evidenceZ.cumResidual * 100).toFixed(2)}% over ${evidenceZ.window} bars, z ${evidenceZ.z.toFixed(2)}, cross-sectional rank ${evidenceContext.rank.toFixed(3)}`,
          attentionScore: att,
          coverageComplete: !!(intelligencePolicy && intelligencePolicy.fresh && intelligencePolicy.complete),
        });
        if (llm.ok) { cause = llm.cause; causeDetail = { ...pre, model: llm }; }
      }
    }

    /* 6. the gate stack runs for every ranked symbol, so the dashboard can
       show why a name did NOT trade as readily as why one did. */
    causeBySymbol[sym] = cause;
    lastPriceBySymbol[sym] = last.c;

    const decisionAtMs = Date.now();
    const baseDecisionInput = {
      symbol: sym,
      quality: quality[sym], advUsd: (metaBySymbol[sym] || {}).advUsd || 0,
      earningsDates: (earningsWindows[sym] && earningsWindows[sym].dates) || meta.earningsDates || [],
      earningsEstimated: !!(earningsWindows[sym] && earningsWindows[sym].estimated),
      earningsUncertaintyDays: Number(earningsWindows[sym] && earningsWindows[sym].uncertaintyDays) || null,
      earningsKnown: !!(earningsWindows[sym] && earningsWindows[sym].dates && earningsWindows[sym].dates.length),
      nowMs: decisionAtMs, cause,
      coverage, attentionScore: attentionBySymbol[sym] ?? null,
      vixNorm: reg.vixNorm, cor3m: reg.cor3m,
      sectorTailFraction: crowd.fractionInTail[S.sectorOf(sym)] ?? 0,
      session, position: null,
      price: lastPriceBySymbol[sym] ?? null,
      intelligence,
      historyContext: historyCtx[sym] || null,
      reversion: reversionBySymbol[sym] || null,
      turnoverPctile: turnoverPctile[sym] ?? null,
    };
    /* Evaluate TWICE. `evalRes` is the verdict the desk acts on; `strictRes` is
       what the published strict calibration would have said. Recording
       both is what stops paper learning mode from destroying the measurement
       it loosens: every relaxed trade stays labelled with whether it would
       have cleared the real gates, so the two populations can be scored apart
       later instead of blurring into one unusable sample. When the mode is off
       the two configs are identical and this costs a pure recomputation. */
    const evalRes = S.evaluateCandidate({ ...baseDecisionInput, rank, zStat: z, cfg });
    const strictRes = paperLearning.active
      ? S.evaluateCandidate({ ...baseDecisionInput, rank, zStat: z, cfg: strictCfg })
      : evalRes;
    /* The strict verdict is structurally "no" until a calibrated lower bound
       exists (requireCalibratedEdge), so on its own it cannot tell an
       operator whether a relaxed trade would have cleared the strict SIGNAL,
       hazard and cost gates. This second counterfactual answers that: the
       strict policy with only the calibration requirement lifted. It is a
       record, not a permission. */
    const strictUncalibratedRes = paperLearning.active
      ? S.evaluateCandidate({ ...baseDecisionInput, rank, zStat: z,
          cfg: { ...strictCfg, requireCalibratedEdge: false } })
      : evalRes;
    /* How much the desk knew when it decided. Recorded everywhere the
       decision travels; sizes and orders exploratory entries; split on the
       scoreboard so thin-data outcomes are measured rather than assumed. */
    const dataSufficiency = DS.score({
      sessionBarCount: bars.filter((b) => M.nyParts(new Date(b.t)).date === session.date).length,
      barCount: bars.length,
      historyDays: historyCtx[sym] ? historyCtx[sym].days : null,
      historyOk: !!(historyCtx[sym] && historyCtx[sym].ok),
      earningsKnown: baseDecisionInput.earningsKnown,
      intelligenceState: intelligencePolicy && intelligencePolicy.fresh && intelligencePolicy.complete
        ? "fresh_complete" : (intelligence ? "present" : "none"),
      cor3mKnown: Number.isFinite(Number(reg.cor3m)),
      advMeasured: !!(metaBySymbol[sym] && !metaBySymbol[sym].advProvisional && metaBySymbol[sym].advUsd > 0),
      grade: quality[sym] && quality[sym].grade,
    }, sufficiencyPolicy);
    const frozenDecision = DF.evaluateFrozenPolicies({ baseInput: baseDecisionInput,
      signalContexts: Object.fromEntries(Object.entries(signalContexts).map(([window, value]) =>
        [window, { ranks: value.ranks, zBySymbol: value.zBySymbol }])) });
    const decisionManifest = DM.build({ decisionId: `${cycleId}_${sym}`,
      decisionAtMs, symbol: sym, input: baseDecisionInput, rank,
      zStat: z ? { ...z, lastPrice: last.c } : null,
      activeResult: evalRes, strictResult: strictRes,
      marketProvenance: marketProvenanceBySymbol[sym] || null,
      lastBarAt: last.t, regime: reg, earningsWindow: earningsWindows[sym] || null,
      meta: metaBySymbol[sym] || {}, betas: rp.betas[sym] || null,
      dailyProvenance: dailyProvenanceBySymbol[sym] || null,
      policyIdentity: { ...policyIdentity,
        variantId: liveVariant ? liveVariant.id : "A" },
      config: cfg, strictConfig: strictCfg,
      buildCommit: process.env.COMMIT_REF || process.env.DEPLOY_ID || "local",
      operatingState: operating.state });
    const manifestValidation = DM.validate(decisionManifest);
    decisionManifestStats.total += 1;
    if (manifestValidation.pass && decisionManifest.coverage.completeIdentity) {
      decisionManifestStats.complete += 1;
    }
    if (!manifestValidation.pass) decisionManifestStats.invalid += 1;

    const card = {
      cycleId, symbol: sym, company: meta.company || sym,
      sector: S.sectorOf(sym), tier: meta.tier || "trade",
      lastPrice: last.c, lastBarAt: last.t,
      quoteSessionDate: M.nyParts(new Date(last.t)).date,
      currentTradingDay: M.nyParts(new Date(last.t)).date === session.date,
      priceDelayMinutes: quality[sym] && quality[sym].feedDelayMinutes,
      marketProvider: (marketProvenanceBySymbol[sym] || {}).provider || null,
      marketFeed: (marketProvenanceBySymbol[sym] || {}).feed || null,
      rank, z: evalRes.z, cumResidualBps: evalRes.cumResidualBps,
      betas: rp.betas[sym] || null,
      quality: quality[sym],
      attentionZ: Number(attentionZ(bars, cfg.signalWindow || 12).toFixed(2)),
      cause, causeDetail: causeDetail ? redact(causeDetail) : null,
      coverage,
      intelligence: intelligence ? {
        asOf: intelligence.asOf, dossierHash: intelligence.dossierHash,
        documentCount: intelligence.documentCount, coverage: intelligence.coverage,
        policy: evalRes.intelligencePolicy,
        temporalContext: intelligence.temporalContext || null,
        topEvents: (intelligence.events || []).slice(0, 5),
      } : null,
      gates: evalRes.gates,
      history: evalRes.historyContext,
      reversion: evalRes.reversion,
      historyNotes: historyCtx[sym] ? H.describe(historyCtx[sym], reversionBySymbol[sym]) : null,
      sigmaBlend: z ? z.sigmaBlend : null,
      zShortOnly: z && isFinite(z.zShortOnly) ? Number(z.zShortOnly.toFixed(2)) : null,
      pass: evalRes.pass, blockedBy: evalRes.blockedBy, firstBlock: evalRes.firstBlock,
      /* Always present, and identical to the above when the mode is off. */
      strict: { pass: strictRes.pass, blockedBy: strictRes.blockedBy,
                firstBlock: strictRes.firstBlock,
                costRatio: strictRes.cost ? strictRes.cost.ratio : null },
      strictUncalibrated: { pass: strictUncalibratedRes.pass,
        firstBlock: strictUncalibratedRes.firstBlock },
      dataSufficiency: { score: dataSufficiency.score, bucket: dataSufficiency.bucket,
        missing: dataSufficiency.missing, version: dataSufficiency.version, kind: dataSufficiency.kind },
      paperRelaxed: paperLearning.active === true,
      frozenDecision,
      decisionManifestHash: decisionManifest.manifestHash,
      manifestCoverage: decisionManifest.coverage,
      decisionManifestCoverage: decisionManifest.coverage,
      decisionManifestValid: manifestValidation.pass,
      cost: evalRes.cost, sizing: evalRes.sizing,
      breach: !!breach, unionEvidenceTrigger: !!evidenceTrigger,
      sectorTailFraction: crowd.fractionInTail[S.sectorOf(sym)] ?? 0,
      strategyVersion: strategy.version,
      updated_at: A.FV.serverTimestamp(),
    };

    await A.col(A.COL.candidates).doc(`${cycleId}_${sym}`).set(card, { merge: true });
    if (evidenceTrigger) candidates.push(card);

    /* Every outcome is a decision record — the rejections are the dataset. */
    await A.col(A.COL.decisions).doc(`${cycleId}_${sym}`).set({
      cycleId, symbol: sym, kind: evalRes.pass
        ? (entryControl.pass && manifestValidation.pass
          ? "entry_eligible" : "eligible_observation")
        : "no_trade",
      noTradeReasons: evalRes.blockedBy, firstBlock: evalRes.firstBlock,
      rank, z: evalRes.z, cumResidualBps: evalRes.cumResidualBps,
      cause, direction: evalRes.direction, cost: evalRes.cost, sizing: evalRes.sizing,
      gates: evalRes.gates,
      strict: { pass: strictRes.pass, blockedBy: strictRes.blockedBy,
        firstBlock: strictRes.firstBlock, gates: strictRes.gates, cost: strictRes.cost,
        sizing: strictRes.sizing },
      strictUncalibrated: { pass: strictUncalibratedRes.pass,
        blockedBy: strictUncalibratedRes.blockedBy, firstBlock: strictUncalibratedRes.firstBlock },
      dataSufficiency,
      paperLearning: { active: paperLearning.active === true,
        applied: paperLearning.applied || {} },
      operatingState: operating.state,
      learningCohort: operating.exploratoryAuto
        ? (exploratoryPolicy.evidenceCohort || "exploratory_auto_unvalidated")
        : (paperLearning.active ? "relaxed_operator_paper" : "strict_policy"),
      inputs: {
        priceQuality: quality[sym] || null,
        advUsd: (metaBySymbol[sym] || {}).advUsd || 0,
        earningsDates: baseDecisionInput.earningsDates,
        earningsEstimated: baseDecisionInput.earningsEstimated,
        earningsUncertaintyDays: baseDecisionInput.earningsUncertaintyDays,
        attentionScore: baseDecisionInput.attentionScore,
        coverage: baseDecisionInput.coverage,
        vixNorm: baseDecisionInput.vixNorm,
        cor3m: baseDecisionInput.cor3m,
        sectorTailFraction: baseDecisionInput.sectorTailFraction,
        turnoverPctile: baseDecisionInput.turnoverPctile,
        session: { date: session.date, phase: session.phase, open: session.open,
          wideSpreadWindow: session.wideSpreadWindow },
        marketProvenance: marketProvenanceBySymbol[sym] || null,
        historyContext: evalRes.historyContext,
        reversion: evalRes.reversion,
      },
      intelligenceDossierHash: intelligence && intelligence.dossierHash || null,
      intelligencePolicy: evalRes.intelligencePolicy || null,
      frozenDecision,
      decisionInputManifest: decisionManifest,
      decisionManifestHash: decisionManifest.manifestHash,
      decisionManifestValid: manifestValidation.pass,
      temporalContextHash: intelligence && intelligence.temporalContext
        && intelligence.temporalContext.contextHash || null,
      strategyVersion: strategy.version, decisionAtMs,
      ...A.envelope({ created_by: FN_NAME }),
    }, { merge: true });

    await DF.recordObservation({ experimentHash: shadowExperiment.experimentHash,
      cycleId, symbol: sym, decisionAtMs, decisionDate: session.date,
      initialPrice: last.c, marketProvenance: marketProvenanceBySymbol[sym],
      counterfactuals: frozenDecision, sector: S.sectorOf(sym), regime: reg,
      decisionManifestHash: decisionManifest.manifestHash,
      manifestCoverage: decisionManifest.coverage,
      configurationIdentity: decisionManifest.configuration,
      cause, activeVerdict: decisionManifest.activeVerdict,
      strictVerdict: decisionManifest.strictVerdict,
      operatingState: operating.state,
      modeledRoundTripBps: evalRes.cost && evalRes.cost.roundTripBps });

    const explorationPolicyIds = firingPolicies.map((f) => f.variantId);
    /* THE STRIKE TIER'S INPUT. A name that cleared every hazard gate but
       has not yet fallen to the entry threshold gets an armed level rather
       than nothing: the price at which its residual would breach -minAbsZ
       with market and sector flat, provided the cost hurdle clears there
       too. The one-minute strike pass buys at that level; this scan does
       not have to be running when it is reached. Exploratory paper only. */
    if (operating.exploratoryAuto && activity.enabled && activity.strike && activity.strike.enabled
        && !evalRes.pass && entryControl.pass && manifestValidation.pass && !pendingOrderSymbols.has(sym)) {
      try {
        const moveStartedAtMs = z ? Date.parse((liveSignalContext.rp.residualTimestamps[sym] || [])
          .slice(-(z.window || cfg.signalWindow || 12))[0] || "") : NaN;
        const candidate = ST.planCandidate({ symbol: sym, evalRes, strictRes, strictUncalibratedRes, z, rank, last, cfg,
          policy: activity.strike, quality: quality[sym], advUsd: (metaBySymbol[sym] || {}).advUsd || 0,
          sector: S.sectorOf(sym), historyContext: historyCtx[sym] || null, reversion: reversionBySymbol[sym] || null,
          dataSufficiency, intelligence, cause, coverage, decisionManifest,
          marketProvenance: marketProvenanceBySymbol[sym] || null, sessionCloseMs: planSessionCloseMs,
          vixNorm: reg.vixNorm, session, policyIdentity, variantId: liveVariant ? liveVariant.id : "A",
          exploratoryPolicyVersion: exploratoryPolicy.version || null,
          cohortLabel: exploratoryPolicy.evidenceCohort || "exploratory_auto_unvalidated",
          paperLearningOnly: paperLearning.active === true,
          activePortfolioControls, activity, cycleId, strategyVersion: strategy.version,
          operatingState: operating.state, moveStartedAtMs,
          positionScale: Math.max(1, Math.min(5, Number(cfg.positionScale) || 1)) });
        planCandidates.push({ ...candidate, symbol: sym });
        if (candidate.ok) await liveEvent("armed", sym, `level ${candidate.plan.armBelowUsd} (${candidate.dropPct}% below)`);
      } catch (e) { planCandidates.push({ ok: false, symbol: sym, reason: `error:${String(e.message || e).slice(0, 60)}` }); }
    }
    if (evalRes.pass && entryControl.pass && manifestValidation.pass) {
      proposalQueue.push({ sym, last, evalRes, strictRes, strictUncalibratedRes, cause, causeDetail, rank,
        intelligence, decisionManifest, cohortRole: "signal", dataSufficiency,
        policyIds: explorationPolicyIds.length ? explorationPolicyIds : ["A"],
        utilityBps: Number(evalRes.cost.calibratedNetLowerBoundBps
          ?? (Number(evalRes.cost.expectedGrossBps) - Number(evalRes.cost.requiredBps))) });
    } else if (operating.exploratoryAuto && activity.enabled && activity.controlCohort.enabled
        && entryControl.pass && manifestValidation.pass && XP.controlEligible(evalRes)) {
      /* Passed every hazard gate, failed only signal-type gates: a control
         candidate. Chosen at random later, at reduced size, and labelled. */
      controlPool.push({ sym, last, evalRes, strictRes, strictUncalibratedRes, cause, causeDetail, rank,
        intelligence, decisionManifest, cohortRole: "control", dataSufficiency,
        policyIds: [], utilityBps: 0 });
    }
    /* One live outcome per name, in pipeline order: ranked → signal → gates. */
    if (evalRes.pass && entryControl.pass && manifestValidation.pass) await liveEvent("passed", sym, "cleared every gate");
    else if (!evidenceTrigger) await liveEvent("no_signal", sym, `rank ${Number(rank).toFixed(2)}`);
    else if (evalRes.pass) await liveEvent("blocked", sym, entryControl.pass ? "manifest" : "entry_control");
    else await liveEvent("blocked", sym, evalRes.blockedBy[0] || evalRes.firstBlock || "gate");
  }
  await reportRunProgress(runRef, { phase: "portfolio_selection",
    label: "Choosing among qualifying opportunities", pct: 73,
    completed: managementWorkset.length, total: managementWorkset.length,
    detail: `${proposalQueue.length} candidates cleared the company-level gates; applying cash and portfolio constraints.` });

  /* Portfolio decisions are a batch problem. Rank all passing ideas by their
     conservative net utility, then admit them one by one against the evolving
     book. Iterating ticker order would make the alphabet an allocation model.

     In exploratory paper mode the ORDER comes from Thompson sampling over the
     frozen policies whose entry rule fired for each candidate (plus the
     candidate's own expected edge), and the PACE is capped per cycle so
     activity spreads across the session. Neither changes which candidates
     qualified — that is the gate stack's verdict, recorded above. */
  const exploratorySelection = operating.exploratoryAuto && activity.enabled;
  let selectionRows = null;
  if (exploratorySelection) {
    selectionRows = XP.rankCandidates(proposalQueue.map((q) => ({ sym: q.sym,
      policyIds: q.policyIds, utilityBps: q.utilityBps,
      penaltyBps: q.dataSufficiency ? q.dataSufficiency.orderingPenaltyBps : 0 })), scoreboard,
      { seed: `${cycleId}|${shadowExperiment.experimentHash}`, policySelection: activity.policySelection });
    const orderIndex = new Map(selectionRows.map((r, i) => [r.sym, i]));
    proposalQueue.sort((a, b) => orderIndex.get(a.sym) - orderIndex.get(b.sym));
  } else {
    proposalQueue.sort((a, b) =>
      Number(b.evalRes.cost.calibratedNetLowerBoundBps || -Infinity)
        - Number(a.evalRes.cost.calibratedNetLowerBoundBps || -Infinity)
      || (Number(b.evalRes.cost.expectedGrossBps) - Number(b.evalRes.cost.requiredBps))
        - (Number(a.evalRes.cost.expectedGrossBps) - Number(a.evalRes.cost.requiredBps))
      || a.rank - b.rank || a.sym.localeCompare(b.sym));
  }
  const selectionBySym = new Map((selectionRows || []).map((r) => [r.sym, r]));

  let proposalCashUsd = Math.max(0, cashUsdNow - proposedUnreservedUsd);
  const signalLifetimeMs = Math.max(15 * 60e3, Math.min(6.5 * 3600e3,
    (Number(cfg.signalLifetimeMinutes) || activity.signalLifetimeMinutes || 120) * 60e3));
  const sessionCloseMs = (() => {
    try {
      const close = M.sessionCloseMs ? M.sessionCloseMs(new Date()) : null;
      return Number.isFinite(Number(close)) ? Number(close) : NaN;
    } catch { return NaN; }
  })();
  const activityLog = { signalQualified: proposalQueue.length, signalProposed: 0,
    signalPacedOut: 0, controlEligible: controlPool.length, controlProposed: 0,
    controlOpenBefore: controlOpen, controlNewThisSessionBefore: controlNewThisSession,
    minimumShareFloorApplied: 0, perCycleCap: exploratorySelection ? activity.maxNewEntriesPerCycle : null };
  const ordinaryCapUsd = navUsd * (Number(activePortfolioControls.ordinaryPositionPctOfNav) || 3) / 100;

  /* One proposal path for both cohorts. Returns true when an order was
     written. Everything it reads is this cycle's own evidence. */
  const proposeEntry = async (q, { queueIndex, batchSize }) => {
    const { sym, last, evalRes, cause, causeDetail, intelligence, decisionManifest } = q;
    const control = q.cohortRole === "control";
    const cohortLabel = control
      ? ((activity.controlCohort && activity.controlCohort.evidenceCohort) || XP.COHORT_CONTROL)
      : (operating.exploratoryAuto
        ? (exploratoryPolicy.evidenceCohort || "exploratory_auto_unvalidated")
        : (paperLearning.active ? "relaxed_operator_paper" : "strict_policy"));
    const selected = selectionBySym.get(sym) || null;
    if (pendingOrderSymbols.has(sym)) return false;
    try {
      const decisionAtMs = Date.now();
      /* Relaxed paper learning may score a research-only series, but a source
         that cannot satisfy the fill-time execution contract must never create
         an order or reserve cash. Keep the labelled outcome; refuse only the
         impossible lifecycle. */
      if (!executionSourceEligible(quality[sym])) {
        const q0 = quality[sym] || {};
        const reason = "market source is research-only and cannot produce an executable paper fill";
        activityLog.executionSourceRefused = (activityLog.executionSourceRefused || 0) + 1;
        activityLog.executionSourceReason = `${q0.provider || "?"}${q0.feed ? "/" + q0.feed : ""} grade ${q0.grade || "?"}`
          + (q0.consolidated === false ? " (non-consolidated feed)" : "") + ((q0.reasons || []).length ? ` ${q0.reasons.join(",")}` : "");
        await liveEvent("blocked", sym, `execution_source:${q0.provider || "?"}${q0.feed ? "/" + q0.feed : ""}${q0.consolidated === false ? " non-consolidated" : ""}`);
        await A.col(A.COL.decisions).doc(`${cycleId}_${sym}`).set({
          finalDecisionKind: "no_trade_execution_source",
          executionSourceEligible: false, executionSourceReason: reason,
        }, { merge: true });
        await A.col(A.COL.decisions).doc(`${cycleId}_${sym}_notrade_execution_source`).set({
          cycleId, symbol: sym, kind: "no_trade", stage: "execution_source",
          blockedBy: ["execution_source"], reason, quality: quality[sym] || null,
          decisionMarketProvenance: marketProvenanceBySymbol[sym] || null,
          parentDecisionManifestHash: decisionManifest.manifestHash,
          strategyVersion: strategy.version, ...A.envelope({ created_by: FN_NAME }),
        }, { merge: true });
        portfolioBlocks.push({ symbol: sym, blockedBy: ["execution_source"], reason });
        return false;
      }
      const hc = historyCtx[sym] || {};
      /* A control entry is sized like a signal entry whose size haircuts all
         landed at the paper floor, then reduced again by the control
         multiplier. It is a baseline, not a bet. */
      const sufficiencyMult = operating.exploratoryAuto && q.dataSufficiency
        ? Number(q.dataSufficiency.sizeMultiplier) || 1 : 1;
      /* positionScale is the operator's "how much per trade" dial: it lifts
         the haircut-reduced scaler toward 1 (never past it), so the ordinary
         cap and the risk budget remain the ceiling. */
      const positionScale = operating.exploratoryAuto
        ? Math.max(1, Math.min(5, Number(cfg.positionScale) || 1)) : 1;
      const signalScaler = Math.min(1, (control
        ? Math.max(Number(evalRes.sizing && evalRes.sizing.combined) || 0,
            Math.max(0, Math.min(ST_FLOOR_MAX, Number(cfg.paperObservationSizeFloor) || 0)))
          * activity.controlCohort.sizeMultiplier
        : evalRes.sizing.combined) * sufficiencyMult * positionScale);
      const sizing = R.positionSizeUsd({ navUsd, atrPct: hc.atrPct,
        expectedShortfall5dPct: hc.expectedShortfall5dPct,
        overnightGapEsPct: hc.overnightGapEsPct,
        signalScaler,
        cfg: { ...activeRiskStrategy, parameters: cfg } });
      const dynamicCorrelations = {};
      for (const held of book.rows) {
        dynamicCorrelations[held.symbol] = T.pairwiseCorrelation(
          dailySeriesBySymbol[sym] || [], dailySeriesBySymbol[held.symbol] || [], decisionAtMs, {
            provenanceA: dailyProvenanceBySymbol[sym] || null,
            provenanceB: dailyProvenanceBySymbol[held.symbol] || null,
            requireProvenance: true,
          });
      }
      const add = R.checkAdd({ symbol: sym, sector: S.sectorOf(sym),
        proposedUsd: sizing.usd, book, navUsd, cashUsd: proposalCashUsd,
        cfg: { ...activeRiskStrategy, parameters: cfg }, dynamicCorrelations });
      const portfolioManifest = DM.buildPortfolio({
        decisionId: `${cycleId}_${sym}_portfolio`, decisionAtMs, symbol: sym,
        parentManifestHash: decisionManifest.manifestHash, sizing,
        portfolioCheck: add, book, navUsd, cashUsd: proposalCashUsd,
        correlations: dynamicCorrelations, policyIdentity: { ...policyIdentity,
          variantId: liveVariant ? liveVariant.id : "A" },
        config: activePortfolioControls,
        buildCommit: process.env.COMMIT_REF || process.env.DEPLOY_ID || "local",
      });
      const portfolioManifestValidation = DM.validate(portfolioManifest);
      const permittedUsd = add.allow ? sizing.usd : (add.allowTrimmed ? add.permittedUsd : 0);
      let qty = portfolioManifestValidation.pass
        ? Math.max(0, Math.floor(permittedUsd / last.c)) : 0;
      /* MINIMUM SHARE FLOOR. A $500 risk-sized paper order in a $620 stock is
         zero shares, and zero shares was silently "no feasible notional" —
         every high-priced name in the roster was unlearnable. One share is
         admitted when it is inside the ordinary position cap and the
         portfolio check permitted a positive notional. The outcome is
         measured in basis points; the dollar size is not what is learned. */
      let minimumShareFloorApplied = false;
      if (qty <= 0 && portfolioManifestValidation.pass && operating.exploratoryAuto
          && activity.enabled && activity.minimumShareFloor >= 1
          && permittedUsd > 0 && last.c > 0 && last.c <= ordinaryCapUsd
          && last.c <= proposalCashUsd) {
        qty = 1; minimumShareFloorApplied = true;
        activityLog.minimumShareFloorApplied += 1;
      }
      await A.col(A.COL.decisions).doc(`${cycleId}_${sym}`).set({
        portfolioDecisionManifest: portfolioManifest,
        portfolioDecisionManifestHash: portfolioManifest.manifestHash,
        portfolioDecisionManifestValid: portfolioManifestValidation.pass,
        finalDecisionKind: qty > 0 ? "paper_order_candidate" : "no_trade_portfolio",
        cohortRole: q.cohortRole,
        ...(minimumShareFloorApplied ? { minimumShareFloorApplied: true } : {}),
      }, { merge: true });
      if (qty <= 0) {
        await A.col(A.COL.decisions).doc(`${cycleId}_${sym}_notrade_portfolio`).set({
          cycleId, symbol: sym, kind: "no_trade", stage: "portfolio",
          blockedBy: add.blockedBy, reason: add.firstBlock || "no feasible risk-sized notional",
          cluster: add.cluster, checks: add.checks, sizing,
          parentDecisionManifestHash: decisionManifest.manifestHash,
          portfolioDecisionManifest: portfolioManifest,
          portfolioDecisionManifestHash: portfolioManifest.manifestHash,
          portfolioDecisionManifestValid: portfolioManifestValidation.pass,
          bookCount: book.count, bookGrossPct: book.grossPct,
          cohortRole: q.cohortRole,
          strategyVersion: strategy.version, ...A.envelope({ created_by: FN_NAME }),
        }, { merge: true });
        portfolioBlocks.push({ symbol: sym, blockedBy: add.blockedBy, reason: add.firstBlock });
        await liveEvent("blocked", sym, `portfolio:${add.firstBlock || (permittedUsd > 0 ? "zero_whole_shares" : "no_notional")}`);
        return false;
      }
      const provenance = marketProvenanceBySymbol[sym];
      const executionCostContext = M.executionCostContext({
        advUsd: (metaBySymbol[sym] || {}).advUsd || 0,
        grade: quality[sym].grade, wideSpreadWindow: session.wideSpreadWindow,
        vixNorm: reg.vixNorm, measuredAtMs: decisionAtMs });
      const slip = executionCostContext.slippageBps;
      /* PATIENCE, decided here and never again. The evidence is this
         company's own five-year reversion record, which existed before the
         position did; the sleeve is checked at cost. A name that qualifies
         when the sleeve is full is simply taken on ordinary terms. */
      const patienceStamp = control ? null : PA.grant({
        reversion: reversionBySymbol[sym] || null,
        historyContext: historyCtx[sym] || null,
        policy: patiencePolicy,
        proposedUsd: qty * last.c,
        positions: allPositions,
        pendingOrders: [...pendingOrders, ...patienceClaims],
        navUsd, nowMs: decisionAtMs });
      if (patienceStamp) {
        patienceClaims.push({ status: "approved", symbol: sym, qty,
          refPriceUsd: last.c, patience: patienceStamp });
        await liveEvent("patient", sym, `held up to ${patienceStamp.grantSessions} sessions`);
      }
      const o = await L.proposeOrder({
        ...policyIdentity, accountId, symbol: sym, side: "buy",
        patience: patienceStamp,
        paperLearningOnly: paperLearning.active === true,
        operatingStateAtDecision: operating.state,
        learningCohort: cohortLabel,
        cohortRole: q.cohortRole,
        decisionSessionDate: session.date,
        exploratoryPolicyVersion: operating.exploratoryAuto
          ? (exploratoryPolicy.version || null) : null,
        decisionManifestHash: decisionManifest.manifestHash,
        portfolioDecisionManifestHash: portfolioManifest.manifestHash,
        decisionId: `${cycleId}_${sym}`, qty, refPriceUsd: last.c, slippageBps: slip,
        executionCostContext,
        sizing: { ...sizing, signal: evalRes.sizing, dataSufficiencyMultiplier: sufficiencyMult, positionScale,
          ...(control ? { controlSizeMultiplier: activity.controlCohort.sizeMultiplier } : {}),
          ...(minimumShareFloorApplied ? { minimumShareFloorApplied: true } : {}) },
        gates: evalRes.gates, cause,
        variantId: liveVariant ? liveVariant.id : "A", cost: evalRes.cost,
        portfolioRisk: { cluster: add.cluster, checks: add.checks,
          dynamicCorrelation: add.dynamicCorrelation,
          dynamicCorrelationPairs: dynamicCorrelations,
          bookGrossPctBefore: book.grossPct, cashUsdBefore: proposalCashUsd },
        decisionContext: {
          decisionManifestHash: decisionManifest.manifestHash,
          decisionManifestVersion: decisionManifest.version,
          decisionManifestCoverage: decisionManifest.coverage,
          portfolioDecisionManifestHash: portfolioManifest.manifestHash,
          portfolioDecisionManifestCoverage: portfolioManifest.coverage,
          paperLearningOnly: paperLearning.active === true,
          operatingState: operating.state,
          learningCohort: cohortLabel,
          cohortRole: q.cohortRole,
          explorationPolicyIds: q.policyIds || [],
          exploratoryPolicyVersion: operating.exploratoryAuto
            ? (exploratoryPolicy.version || null) : null,
          exploratorySelection: selected ? { method: activity.policySelection.method,
            sampledPolicyId: selected.sampledPolicyId, sampledBps: selected.sampledBps,
            score: selected.score, scoreboardClosed: scoreboard ? scoreboard.closedExploratoryTrades : null,
            scoreboardSampleHash: scoreboard && scoreboard.sample ? scoreboard.sample.sampleHash : null }
            : (control ? { method: "control_random", seed: cycleId } : null),
          strictVerdict: { pass: q.strictRes ? q.strictRes.pass : false,
            blockedBy: q.strictRes ? q.strictRes.blockedBy : ["strict_verdict_missing"],
            firstBlock: q.strictRes ? q.strictRes.firstBlock : "strict verdict missing" },
          activeVerdict: { pass: evalRes.pass === true, blockedBy: evalRes.blockedBy || [],
            firstBlock: evalRes.firstBlock || null },
          dataSufficiency: q.dataSufficiency ? { score: q.dataSufficiency.score, bucket: q.dataSufficiency.bucket,
            missing: q.dataSufficiency.missing, sizeMultiplier: q.dataSufficiency.sizeMultiplier,
            orderingPenaltyBps: q.dataSufficiency.orderingPenaltyBps,
            version: q.dataSufficiency.version, kind: q.dataSufficiency.kind } : null,
          decisionAgeMinutesAtProposal: 0,
          strictUncalibratedVerdict: q.strictUncalibratedRes ? { pass: q.strictUncalibratedRes.pass === true,
            blockedBy: q.strictUncalibratedRes.blockedBy || [], firstBlock: q.strictUncalibratedRes.firstBlock || null } : null,
          crossSectionRank: q.rank,
          queueRank: queueIndex + 1,
          eligibleBatchSize: batchSize,
          conservativeUtilityBps: Number(evalRes.cost.calibratedNetLowerBoundBps
            ?? (Number(evalRes.cost.expectedGrossBps) - Number(evalRes.cost.requiredBps))),
          topAlternatives: proposalQueue.slice(0, 5).map((x) => ({
            symbol: x.sym, rank: x.rank,
            conservativeUtilityBps: Number(x.evalRes.cost.calibratedNetLowerBoundBps
              ?? (Number(x.evalRes.cost.expectedGrossBps) - Number(x.evalRes.cost.requiredBps))),
          })),
          companyIntelligence: intelligence ? {
            dossierHash: intelligence.dossierHash, asOfMs: intelligence.asOfMs,
            adverseRiskScore: evalRes.intelligencePolicy.adverseRiskScore,
            sizeMultiplier: evalRes.intelligencePolicy.sizeMultiplier,
            temporalContextHash: intelligence.temporalContext && intelligence.temporalContext.contextHash || null,
            temporalRiskScore: evalRes.intelligencePolicy.temporalPolicy
              && evalRes.intelligencePolicy.temporalPolicy.riskScore || 0,
            contextMeasurement: intelligence.temporalContext
              && intelligence.temporalContext.shadowCalibration || null,
            calibrationOutputAffectsDecision: false,
            topEventIds: (intelligence.events || []).slice(0, 5).map((e) => e.eventId),
          } : null,
        },
        evidenceRefs: [
          ...((causeDetail && (causeDetail.fundamentalDocs
            || (causeDetail.drivers && causeDetail.drivers.fundamentalDocs))) || []),
          ...((intelligence && intelligence.events || []).slice(0, 5).flatMap((e) =>
            (e.corroboration && e.corroboration.sourceRefs || []).map((documentId) => ({ documentId, eventId: e.eventId })))),
          ...((intelligence && intelligence.temporalContext && intelligence.temporalContext.exposures || [])
            .map((x) => ({ documentId: x.support && x.support.documentRef,
              temporalExposureId: x.exposureId, temporalContextHash: intelligence.temporalContext.contextHash }))
            .filter((x) => x.documentId)),
        ].slice(0, 40),
        decisionAtMs, quality: quality[sym], decisionMarketProvenance: provenance,
        executionLatencyMs: cfg.executionLatencyMs || 60000,
        reservationHeadroomBps: operating.exploratoryAuto && activity.enabled
          ? activity.reservationHeadroomBps : 0,
        /* Signal lifetime: the proposal dies at the earlier of the configured
           lifetime and the end of this regular session. */
        expiresAtMs: Math.min(decisionAtMs + signalLifetimeMs,
          Number.isFinite(sessionCloseMs) ? sessionCloseMs : Infinity),
      });
      if (o.blocked) { await liveEvent("blocked", sym, `portfolio:${String(o.blocked).slice(0, 30)}`); portfolioBlocks.push({ symbol: sym, reason: o.blocked }); return false; }
      pendingOrderSymbols.add(sym);
      await liveEvent(control ? "control_proposed" : "proposed", sym, `${qty} sh @ $${Number(last.c).toFixed(2)}`);
      decisions.push({ symbol: sym, orderId: o.orderId, qty, refPriceUsd: o.refPriceUsd,
        trimmed: !add.allow, cluster: add.cluster, variantId: liveVariant ? liveVariant.id : "A",
        cohortRole: q.cohortRole, policyIds: q.policyIds || [] });
      const takenUsd = qty * last.c;
      proposalCashUsd -= takenUsd;
      book.count += 1; book.grossUsd += takenUsd;
      book.grossPct = navUsd > 0 ? 100 * book.grossUsd / navUsd : 0;
      const sec = S.sectorOf(sym), pct = navUsd > 0 ? 100 * takenUsd / navUsd : 0;
      book.bySectorPct[sec] = (book.bySectorPct[sec] || 0) + pct;
      book.byClusterPct[add.cluster] = (book.byClusterPct[add.cluster] || 0) + pct;
      book.rows.push({ symbol: sym, qty, entry: last.c, mark: last.c,
        valueUsd: takenUsd, sector: sec, cluster: add.cluster, pnlPct: 0, marked: true });
      return true;
    } catch (e) {
      console.error("propose failed", redact({ symbol: sym, error: e.message }));
      activityLog.proposeErrors = (activityLog.proposeErrors || 0) + 1;
      activityLog.lastProposeError = String(e.message || e).slice(0, 160);
      await liveEvent("blocked", sym, `error:${String(e.message || e).slice(0, 40)}`);
      return false;
    }
  };

  /* ENTRY CREATION IS SERIALIZED against the one-minute strike pass. Both
     size against a book snapshot they built themselves, and the approval
     transaction re-checks only cash — so without this both could admit a
     position against the same last free slot and the book would exceed its
     declared caps. Exits, marks and fills are outside the lock. */
  const entryLockOwner = `cycle:${cycleId}:${Math.random().toString(36).slice(2, 8)}`;
  const entryLock = entryControl.pass
    ? await LEASE.acquireEntryLock(accountId, entryLockOwner)
    : { acquired: false, reason: "entry controls closed" };
  if (entryControl.pass && !entryLock.acquired) {
    activityLog.entryLockUnavailable = entryLock.heldBy || entryLock.error || "held elsewhere";
    await liveEvent("blocked", "—", "entry lock held by the strike pass");
  }

  /* Signal cohort, paced. */
  for (let queueIndex = 0; entryLock.acquired && queueIndex < proposalQueue.length; queueIndex += 1) {
    if (exploratorySelection && activityLog.signalProposed >= activity.maxNewEntriesPerCycle) {
      activityLog.signalPacedOut = proposalQueue.length - queueIndex;
      for (const q of proposalQueue.slice(queueIndex)) await liveEvent("paced_out", q.sym, "waits for a later scan");
      break;
    }
    const wrote = await proposeEntry(proposalQueue[queueIndex],
      { queueIndex, batchSize: proposalQueue.length });
    if (wrote) activityLog.signalProposed += 1;
  }

  /* Control cohort: unconditional, random, reduced size, capped per session
     and by open control positions. Only in explicit exploratory paper mode
     and only while the session is one a signal entry could also use. */
  if (entryLock.acquired && exploratorySelection && activity.controlCohort.enabled && controlPool.length
      && session.open && !session.wideSpreadWindow && !breakers.halted) {
    const room = Math.min(
      Math.max(0, activity.controlCohort.maxOpenPositions - controlOpen),
      Math.max(0, activity.controlCohort.maxNewPerSession - controlNewThisSession));
    const picks = XP.selectControl(controlPool.filter((c) => !pendingOrderSymbols.has(c.sym)),
      { seed: `${cycleId}|${shadowExperiment.experimentHash}`, limit: room });
    for (let i = 0; i < picks.length; i += 1) {
      const wrote = await proposeEntry(picks[i], { queueIndex: i, batchSize: picks.length });
      if (wrote) activityLog.controlProposed += 1;
    }
    activityLog.controlRoom = room;
  }

  /* ── ARM THE LEVELS ──────────────────────────────────────────────────
     Written after the proposals so a name proposed this scan is never also
     armed. Closest levels first, capped by the frozen policy; levels this
     scan did not re-select are superseded. */
  let strikePlans = null;
  if (operating.exploratoryAuto && activity.enabled && activity.strike && activity.strike.enabled) {
    try {
      strikePlans = await ST.writePlans({ accountId, sessionDate: session.date, cycleId,
        candidates: planCandidates, policy: activity.strike,
        heldSymbols: allPositions.filter((p) => p && p.open).map((p) => p.symbol),
        pendingSymbols: [...pendingOrderSymbols] });
      strikePlans.policy = activity.strike;
    } catch (e) { strikePlans = { error: String(e.message || e).slice(0, 160), considered: planCandidates.length }; }
  } else {
    strikePlans = { disabled: true, considered: planCandidates.length,
      reason: operating.exploratoryAuto ? "strike tier disabled by the frozen policy" : "armed levels are an exploratory paper feature" };
  }
  await reportRunProgress(runRef, { phase: "settlement",
    label: "Approving and settling eligible paper orders", pct: 82,
    completed: decisions.length, total: proposalQueue.length,
    detail: `${decisions.length} paper order proposals were created; rechecking execution timing, cash and provenance.` });

  /* ── SETTLEMENT ───────────────────────────────────────────────────────
     Approved orders become fills, and exit signals become closes.

     This pass did not exist. `recordFill` and `closePosition` were fully
     written, tested by fixtures, and called by NOTHING — so an approved order
     debited cash into RESERVED and stayed there forever, no position document
     was ever created, `position.open` was never true, and the entire exit
     branch above was unreachable code. Every number on the dashboard was
     therefore structurally zero.

     Fills use firstEligibleBar, never the bar the decision was made on. That
     is the anti-look-ahead invariant: you cannot fill at a price you used to
     decide, because in reality that print had already happened. */
  let settled = { filled: [], closed: [], skipped: [], autoApproved: [] };
  try {
    /* AUTO-APPROVAL has two explicitly separate populations:
       - exploratory_auto gathers labelled PAPER outcomes immediately, with no
         daily order quota and the full frozen exploratory portfolio capacity;
       - limited_auto remains the statistically earned, tightly capped mode.
       The state and cohort are persisted on every order so evidence from one
       can never be presented as validation of the other. */
    const stageNow = operating.stage;
    if (operating.automaticPaperEntries && entryControl.pass) {
      let dayCount = 0;
      if (!operating.exploratoryAuto) {
        const todayAuto = await A.col(A.COL.orders)
          .where("accountId", "==", accountId)
          .where("autoApprovedDate", "==", today).limit(50).get();
        dayCount = todayAuto.size;
      }

      const proposedSnap = await A.col(A.COL.orders)
        .where("accountId", "==", accountId).where("status", "==", "proposed").get();

      for (const d of proposedSnap.docs) {
        const o = d.data();
        const verdict = operating.exploratoryAuto
          ? LD.exploratoryAutoApproval(o, {
            operatingState: operating.state, book, navUsd,
            cfg: strategy, nowMs: Date.now(), sessionDate: session.date,
          })
          : LD.autoApproval(o, {
            stage: stageNow, book, navUsd, cfg: strategy, dayCount,
            nowMs: Date.now(), sessionDate: session.date,
          });
        if (!verdict.approve) {
          if (operating.exploratoryAuto) {
            try {
              const rejected = await L.rejectOrder(o.orderId,
                `exploratory auto refusal: ${verdict.detail}`, "auto:exploratory");
              settled.skipped.push({ orderId: o.orderId,
                why: rejected.noop ? verdict.detail : `rejected automatically: ${verdict.detail}` });
            } catch (e) {
              settled.skipped.push({ orderId: o.orderId,
                why: `automatic refusal could not be persisted: ${String(e.message).slice(0, 100)}` });
            }
          } else {
            settled.skipped.push({ orderId: o.orderId, why: "left for you: " + verdict.detail });
          }
          continue;
        }
        try {
          const approved = await L.approveOrder(o.orderId,
            operating.exploratoryAuto ? "auto:exploratory" : "auto:validated");
          if (approved.status !== "approved" || approved.noop || approved.refused) {
            settled.skipped.push({ orderId: o.orderId,
              why: approved.refused || "auto approval did not transition the order" });
            continue;
          }
          await d.ref.set({ autoApproved: true, autoApprovedDate: today,
            autoApprovalDetail: verdict.detail,
            autoApprovalCohort: verdict.cohort || (operating.exploratoryAuto
              ? "exploratory_auto_unvalidated" : "validated_limited_auto"),
            autoApprovalOperatingState: operating.state }, { merge: true });
          if (!operating.exploratoryAuto) dayCount += 1;
          await liveEvent("approved", o.symbol, "auto-approved");
          settled.autoApproved.push({ orderId: o.orderId, symbol: o.symbol,
            cohort: verdict.cohort || null, detail: verdict.detail });
        } catch (e) { settled.skipped.push({ orderId: o.orderId, why: String(e.message).slice(0, 120) }); }
      }
    }

    /* Approvals are the last step that can breach a cap, so the lock is
       released here rather than at the end of settlement. Fills only move an
       already-counted order into an already-counted position. */
    if (entryLock.acquired) {
      await LEASE.releaseEntryLock(accountId, entryLockOwner);
      entryLock.acquired = false;
    }

    const approvedSnap = await A.col(A.COL.orders)
      .where("accountId", "==", accountId).where("status", "==", "approved").get();

    settled.expiredAtSessionClose = [];
    for (const d of approvedSnap.docs) {
      const o = d.data();
      if (!entryControl.pass) {
        settled.skipped.push({ orderId: o.orderId, why: `entry safety closed: ${entryControl.reason}` });
        continue;
      }
      /* Past its signal lifetime, an approved order is released whatever its
         cohort — the price it was an opinion about is gone. */
      if (Number.isFinite(Number(o.expiresAtMs)) && Date.now() > Number(o.expiresAtMs)) {
        try {
          const released = await L.releaseOrder(o.orderId,
            `expired — signal lifetime elapsed before an eligible fill (decision ${Math.round((Date.now() - Number(o.decisionAtMs)) / 60000)} min old)`);
          if (!released.noop) settled.expiredAtSessionClose.push({ orderId: o.orderId, symbol: o.symbol, why: "lifetime" });
        } catch (e) {
          settled.skipped.push({ orderId: o.orderId, why: `could not release expired entry: ${String(e.message).slice(0, 80)}` });
        }
        continue;
      }
      /* An exploratory entry that did not fill in its own session is not
         carried to the next open: a residual dip measured at 14:50 says
         nothing about the price at 09:46 tomorrow, and a fill there would
         be attributed to a signal that no longer exists. Release the cash. */
      if (activity.expireUnfilledEntriesAtSessionClose && o.paperLearningOnly === true
          && /^exploratory/.test(String(o.learningCohort || ""))
          && o.decisionSessionDate && o.decisionSessionDate !== session.date) {
        try {
          const released = await L.releaseOrder(o.orderId,
            `expired — exploratory entry from ${o.decisionSessionDate} is not carried into ${session.date}`);
          if (!released.noop) settled.expiredAtSessionClose.push({ orderId: o.orderId, symbol: o.symbol });
        } catch (e) {
          settled.skipped.push({ orderId: o.orderId, why: `could not release stale exploratory entry: ${String(e.message).slice(0, 80)}` });
        }
        continue;
      }
      const obars = panel[o.symbol] || [];
      if (!obars.length) { settled.skipped.push({ orderId: o.orderId, why: "no bars this cycle" }); continue; }
      const currentProv = marketProvenanceBySymbol[o.symbol];
      const decidedProv = o.decisionMarketProvenance || {};
      const identitySame = currentProv && ["provider", "feed", "adjustment"]
        .every((k) => (currentProv[k] || null) === (decidedProv[k] || null));
      if (!identitySame || !(quality[o.symbol] && quality[o.symbol].tradable)) {
        settled.skipped.push({ orderId: o.orderId, why: "fill-time market identity/quality check failed" });
        continue;
      }
      const elig = M.firstEligibleBar(obars, {
        decisionAtMs: Math.max(Number(o.decisionAtMs) || 0, Number(o.approvedAtMs) || 0),
        provider: currentProv.provider, feed: currentProv.feed,
        executionLatencyMs: cfg.executionLatencyMs || 60000,
      });
      if (!elig.bar) { settled.skipped.push({ orderId: o.orderId, why: elig.reason || "no eligible bar yet" }); continue; }
      try {
        const f = await L.recordFill({
          orderId: o.orderId, bar: elig.bar,
          barProvenance: { ...currentProv, barOpenAt: elig.barOpenAt },
        });
        if (!f.duplicate) { await liveEvent("filled", o.symbol, `filled on the ${String(elig.barOpenAt).slice(11, 16)}Z bar`); settled.filled.push({ orderId: o.orderId, symbol: o.symbol, at: elig.barOpenAt }); }
      } catch (e) { settled.skipped.push({ orderId: o.orderId, why: String(e.message).slice(0, 120) }); }
    }

    /* EXIT INTENTS are resolved from the POSITION DOCS, not from this cycle's
       transient signal list. The intent carries its original decision time, so
       an exit signalled twenty minutes ago fills on this cycle's fresh bars —
       exactly how buys work. Reading from the docs also means an intent
       recorded just before a crash still executes on the next healthy cycle.
       Urgent exits (stops) execute even while the account is halted — a halt
       must never trap a losing position. */
    for (const p of allPositions) {
      const intent = p.exitIntent;
      if (!p.open || !intent || !intent.decisionAtMs) continue;
      if (corporateActionQuarantineBySymbol[p.symbol] || p.corporateActionPending) {
        settled.skipped.push({ symbol: p.symbol,
          why: "exit pending — corporate-action price basis awaits operator reconciliation" });
        continue;
      }
      const xbars = panel[p.symbol] || [];
      if (!xbars.length) { settled.skipped.push({ symbol: p.symbol, why: "exit pending — no bars this cycle" }); continue; }
      const exitProv = marketProvenanceBySymbol[p.symbol];
      if (!exitProv || !/^[a-f0-9]{64}$/.test(String(exitProv.sourceSha256 || ""))) {
        settled.skipped.push({ symbol: p.symbol, why: "exit pending — source provenance unavailable" }); continue;
      }
      const intentProv = intent.decisionMarketProvenance || {};
      const sourcePolicy = W.exitExecutionSourcePolicy(intentProv, exitProv, quality[p.symbol]);
      if (!sourcePolicy.pass) {
        settled.skipped.push({ symbol: p.symbol,
          why: `exit pending — ${sourcePolicy.reason}` });
        continue;
      }
      /* Provider/feed changes do not re-authorize or re-time the economic
         decision. The original decisionAtMs remains authoritative; the current
         trusted source supplies only the executable observation. Adjustment
         identity remains locked because a split-basis change needs explicit
         corporate-action reconciliation. */
      const elig = M.firstEligibleBar(xbars, {
        decisionAtMs: intent.decisionAtMs,
        provider: exitProv.provider, feed: exitProv.feed,
        executionLatencyMs: cfg.executionLatencyMs || 60000,
      });
      if (!elig.bar) { settled.skipped.push({ symbol: p.symbol, why: "exit pending — feed has not yet delivered a post-decision bar" }); continue; }
      try {
        const executionCostContext = M.executionCostContext({
          advUsd: (metaBySymbol[p.symbol] || {}).advUsd || 0,
          grade: (quality[p.symbol] || {}).grade || "C",
          wideSpreadWindow: session.wideSpreadWindow, vixNorm: reg.vixNorm,
        });
        const slip = executionCostContext.slippageBps;
        const c = await L.closePosition({
          accountId, symbol: p.symbol, bar: elig.bar, slippageBps: slip,
          reason: intent.reason, decisionAtMs: intent.decisionAtMs,
          eligibleAfterMs: intent.eligibleAfterMs,
          executionEligibleAfterMs: elig.availableFromMs,
          executionCostContext,
          barProvenance: { ...exitProv, barOpenAt: elig.barOpenAt },
          closedBy: "full_cycle",
        });
        if (!c.noop) {
          await liveEvent("closed", p.symbol, intent.kind || intent.reason);
          settled.closed.push({ symbol: p.symbol, reason: intent.reason, kind: intent.kind,
                                netBps: c.netBps, variantId: c.variantId });
        }
      } catch (e) { settled.skipped.push({ symbol: p.symbol, why: String(e.message).slice(0, 120) }); }
    }

    /* Stale approved orders release their reserved cash after 24 hours. This
       logic existed only as an API action with zero callers — the exact
       "fully written, called by nothing" pathology this file keeps finding. */
    try {
      const cutoffMs = Date.now() - 24 * 3600e3;
      const staleSnap = await A.col(A.COL.orders)
        .where("accountId", "==", accountId).where("status", "==", "approved").get();
      for (const d of staleSnap.docs) {
        const o = d.data();
        if ((o.decisionAtMs || 0) > cutoffMs) continue;
        try {
          const r = await L.releaseOrder(o.orderId, "expired — no eligible fill within 24h");
          if (!r.noop) settled.skipped.push({ orderId: o.orderId, why: "expired, cash returned" });
        } catch {}
      }
    } catch {}
  } catch (e) {
    settled.error = String(e.message).slice(0, 200);
  } finally {
    /* A throw anywhere in settlement must not leave entries frozen until the
       lock's TTL. */
    if (entryLock.acquired) {
      try { await LEASE.releaseEntryLock(accountId, entryLockOwner); } catch {}
      entryLock.acquired = false;
    }
  }
  await reportRunProgress(runRef, { phase: "learning",
    label: "Recording outcomes and comparing frozen policies", pct: 89,
    detail: `${settled.filled.length} entries filled and ${settled.closed.length} positions closed in this settlement pass.` });

  /* ── SHADOW HARNESS ──────────────────────────────────────────────────
     Each frozen arm runs a complete feasible paper portfolio on the union of
     opportunities. Its daily marked return is an experiment-bound record;
     live paper outcomes never contaminate this counterfactual dataset. */
  let shadow = null, allocation = null, calibration = null;
  try {
    const observationFeedback = await DF.updateObservations({
      experimentHash: shadowExperiment.experimentHash, session,
      lastPrice: lastPriceBySymbol, marketProvenanceBySymbol,
      intelligenceBySymbol,
    });
    const shadowCtx = {
      cycleId, ranks, zBySymbol, quality, meta: metaBySymbol,
      signalContexts: Object.fromEntries(Object.entries(signalContexts).map(([window, value]) =>
        [window, { ranks: value.ranks, zBySymbol: value.zBySymbol }])),
      causeBySymbol, coverageBySymbol, attentionBySymbol,
      session, regime: reg,
      earnings: earningsWindows, crowd, lastPrice: lastPriceBySymbol,
      panel, marketIdentity, marketProvenanceBySymbol,
      historyCtx, dailySeriesBySymbol, dailyProvenanceBySymbol,
      reversion: reversionBySymbol, turnoverPctile,
      intelligenceBySymbol,
      universeHash, strategyHash, variantsHash,
      experimentHash: shadowExperiment.experimentHash,
      experimentIdentity: shadowExperiment.identity,
      portfolioControls: strategy.portfolioControls,
      shadowNavUsd: Number(ctrl.shadowNavUsd) || 100000,
    };
    const shadowExits = await SH.evaluateExits(shadowCtx);
    const entries = await SH.evaluateEntries(shadowCtx);
    const roll = await SH.rollUpStats({ experimentHash: shadowExperiment.experimentHash });
    const windowStats = await SH.variantStats({ experimentHash: shadowExperiment.experimentHash });
    /* The confirmation tail must not choose its own winner. Build every
       selection/power statistic only from train + calibration observations;
       developmentOnly() withholds holdout values. A leader is then locked in
       Firestore before exactly that leader's frozen confirmation tail is
       evaluated. Once consumed, the experiment cannot shop that holdout for a
       different winner. */
    const calibrationOpts = { alpha: 0.01, k: V.VARIANTS.length,
      embargoSessions: 15, minTrain: 252, minCalibration: 126, minHoldout: 126 };
    let forwardLock = allocationRead && allocationRead.forwardLock
      && allocationRead.forwardLock.experimentHash === shadowExperiment.experimentHash
      ? allocationRead.forwardLock : null;
    let forwardResult = allocationRead && allocationRead.forwardResult
      && forwardLock && allocationRead.forwardResult.lockHash === forwardLock.lockHash
      ? allocationRead.forwardResult : null;
    const powerStats = {}, selStats = {}, selectionDays = {}, developmentDays = {},
      developmentMeta = {}, developmentResets = {};
    for (const variant of V.VARIANTS) {
      const researchRows = (roll.dailyByVariant[variant.id] || []).filter((row) =>
        !forwardLock || row.date <= forwardLock.dataThroughDate);
      const dev = C.developmentOnly(researchRows, calibrationOpts);
      developmentMeta[variant.id] = dev;
      if (!dev.pass) continue;
      developmentDays[variant.id] = dev.development;
      const full = SH.aggregateDays(dev.development, { discounted: false });
      if (full) powerStats[variant.id] = { ...full, source: "development_power" };
      const ph = SH.pageHinkley(dev.development, full ? full.sdBps : 0);
      if (ph.break) developmentResets[variant.id] = ph.at;
      const selected = ph.break ? dev.development.filter((d) => d.date >= ph.at) : dev.development;
      selectionDays[variant.id] = selected;
      const current = SH.aggregateDays(selected, { discounted: true });
      if (current) selStats[variant.id] = { ...current, source: "development_selection", resetAt: ph.at };
    }

    const alloc = AL.allocate(selStats, {
      effectBps: cfg.expectedEdgeBps ?? 30,
      sdBps: cfg.perTradeSdBps ?? 250,
      alpha: 0.04, power: 0.9, powerStats,
      dailyByVariant: selectionDays,
      incumbentId: (liveVariant && liveVariant.id) || "A",
      discountGamma: SH.DISCOUNT_GAMMA,
    });
    const overfitGuard = RS.researchGuard(developmentDays, {
      trials: V.VARIANTS.length,
      effectiveNByVariant: Object.fromEntries(V.VARIANTS.map((v) =>
        [v.id, Number(powerStats[v.id] && powerStats[v.id].effectiveN) || 0])),
    });
    applyOverfitGuard(alloc, overfitGuard);
    let holdoutLock = allocationRead && allocationRead.holdoutLock
      && allocationRead.holdoutLock.experimentHash === shadowExperiment.experimentHash
      ? allocationRead.holdoutLock : null;
    let leaderCalibration = null;

    if (alloc.leaderId) {
      const dev = developmentMeta[alloc.leaderId];
      if (holdoutLock && holdoutLock.leaderId !== alloc.leaderId) {
        alloc.note = `${alloc.note} This experiment's confirmation set was already locked to ${holdoutLock.leaderId}; it cannot be reused to select ${alloc.leaderId}.`;
        alloc.leaderId = null;
      } else if (!holdoutLock && dev && dev.pass) {
        const proposedLock = C.holdoutLockTemplate({
          experimentHash: shadowExperiment.experimentHash,
          leaderId: alloc.leaderId, confirmation: dev.confirmation,
          variantsHash, simulatorVersion: SH.SIMULATOR_VERSION,
          calibrationOpts,
        });
        const allocationRef = A.col(A.COL.control).doc("allocation");
        holdoutLock = await A.runTransaction(async (tx) => {
          const snap = await tx.get(allocationRef);
          const existing = snap.exists ? snap.data().holdoutLock : null;
          if (existing && existing.experimentHash === shadowExperiment.experimentHash) return existing;
          tx.set(allocationRef, { holdoutLock: proposedLock, leaderId: null,
            updatedAt: A.FV.serverTimestamp() }, { merge: true });
          return proposedLock;
        });
        if (holdoutLock.leaderId !== alloc.leaderId) {
          alloc.note = `${alloc.note} A concurrent confirmation lock belongs to ${holdoutLock.leaderId}; no reselection is allowed.`;
          alloc.leaderId = null;
        }
      }

      if (alloc.leaderId && holdoutLock) {
        const storedCalibration = await C.read(shadowExperiment.experimentHash);
        const reusable = storedCalibration && storedCalibration.holdoutLock
          && /^[a-f0-9]{64}$/.test(String(holdoutLock.lockHash || ""))
          && storedCalibration.holdoutLock.lockHash === holdoutLock.lockHash
          && storedCalibration.evaluatedLeaderId === holdoutLock.leaderId;
        if (reusable) {
          calibration = storedCalibration;
          leaderCalibration = calibration.variants && calibration.variants[alloc.leaderId];
        } else {
          const frozenRows = (roll.dailyByVariant[alloc.leaderId] || [])
            .filter((r) => r.date <= holdoutLock.dataThroughDate);
          leaderCalibration = C.calibrate(frozenRows, calibrationOpts);
          calibration = { variants: { [alloc.leaderId]: leaderCalibration },
            passCount: leaderCalibration.pass ? 1 : 0, holdoutLock,
            evaluatedLeaderId: alloc.leaderId, evaluatedAtMs: Date.now() };
          await C.persist(shadowExperiment.experimentHash, calibration);
        }
      }

      if (!leaderCalibration || !leaderCalibration.pass) {
        alloc.note = `${alloc.note} Chronological calibration/holdout stress has not passed, so no policy changes.`;
        alloc.leaderId = null;
      } else {
        alloc.historicalNetLowerBoundBps = leaderCalibration.calibratedNetLowerBoundBps;
      }
    }

    /* Historical tails are robustness checks, not genuinely unseen evidence.
       A qualified policy is locked at the current data boundary and must then
       survive an embargo plus 126 future paper sessions. Once locked, every
       later row is excluded from development selection and belongs only to
       this confirmation. */
    if (alloc.leaderId && leaderCalibration && leaderCalibration.pass && !forwardLock) {
      const leaderRows = roll.dailyByVariant[alloc.leaderId] || [];
      const dataThroughDate = leaderRows.length ? leaderRows.at(-1).date : null;
      if (dataThroughDate) {
        const proposed = C.forwardLockTemplate({
          experimentHash: shadowExperiment.experimentHash, leaderId: alloc.leaderId,
          dataThroughDate, variantsHash, simulatorVersion: SH.SIMULATOR_VERSION,
          embargoSessions: 15, requiredSessions: 126,
        });
        const allocationRef = A.col(A.COL.control).doc("allocation");
        forwardLock = await A.runTransaction(async (tx) => {
          const snap = await tx.get(allocationRef);
          const existing = snap.exists ? snap.data().forwardLock : null;
          if (existing && existing.experimentHash === shadowExperiment.experimentHash) return existing;
          tx.set(allocationRef, { forwardLock: proposed, forwardResult: null,
            leaderId: null, calibratedNetLowerBoundBps: null,
            updatedAt: A.FV.serverTimestamp() }, { merge: true });
          return proposed;
        });
      }
    }
    if (forwardLock) {
      forwardResult = C.evaluateForward(roll.dailyByVariant[forwardLock.leaderId] || [],
        forwardLock, { alpha: 0.01, k: V.VARIANTS.length });
      if (alloc.leaderId !== forwardLock.leaderId) {
        alloc.note = `${alloc.note} The forward lock belongs to ${forwardLock.leaderId}; no other policy may use or replace its confirmation stream.`;
        alloc.leaderId = null;
      } else if (!forwardResult.pass) {
        alloc.note = `${alloc.note} Locked forward paper: ${forwardResult.reason}. No policy changes.`;
        alloc.leaderId = null;
      } else {
        alloc.calibratedNetLowerBoundBps = Math.min(
          Number(leaderCalibration.calibratedNetLowerBoundBps),
          Number(forwardResult.calibratedNetLowerBoundBps));
        alloc.note = `${alloc.note} The locked winner also passed untouched forward paper and all stress bounds.`;
      }
      await C.persist(shadowExperiment.experimentHash, { forwardLock, forwardResult });
    } else {
      alloc.leaderId = null;
      alloc.note = `${alloc.note} No policy can be promoted before a future-paper lock exists.`;
    }
    if (!calibration) calibration = { variants: {}, passCount: 0, holdoutLock,
      evaluatedLeaderId: null, reason: "no development-selected leader has a locked confirmation result" };
    allocation = AL.explain(alloc, selStats);
    allocation.calibratedNetLowerBoundBps = alloc.calibratedNetLowerBoundBps || null;
    allocation.comparison = alloc.comparison || null;
    allocation.holdoutLock = holdoutLock;
    allocation.holdoutResult = leaderCalibration;
    allocation.forwardLock = forwardLock;
    allocation.forwardResult = forwardResult;
    allocation.experimentHash = shadowExperiment.experimentHash;
    allocation.experimentIdentity = shadowExperiment.identity;
    allocation.resets = developmentResets;
    allocation.discountGamma = roll.discountGamma;
    allocation.overfitGuard = overfitGuard;

    const completeDays = V.VARIANTS.map((v) => Number(powerStats[v.id] && powerStats[v.id].independentDays) || 0);
    shadow = { opened: entries.opened, closed: shadowExits.closed,
      unresolved: shadowExits.dataLoss, evaluated: entries.evaluated,
      byVariant: entries.byVariant, totalClosed: windowStats.totalClosed,
      completeDays: Math.min(...completeDays),
      contextMeasurement: windowStats.contextMeasurement,
      decisionFeedback: observationFeedback,
      experimentHash: shadowExperiment.experimentHash };

    await A.col(A.COL.control).doc("allocation").set({
      ...allocation, variantsHash, universeHash, strategyHash,
      updatedAt: A.FV.serverTimestamp(),
    }, { merge: true });
  } catch (e) {
    shadow = { error: String(e.message).slice(0, 200) };
  }
  await reportRunProgress(runRef, { phase: "safety_review",
    label: "Checking promotion gates and account safeguards", pct: 94,
    detail: "Outcome records are updated; reviewing ledger, data coverage and automation-stage evidence." });

  /* ── THE LADDER ───────────────────────────────────────────────────────
     Measure every promotion gate against recorded state and move the system to
     the stage it has actually earned, capped by the ceiling the operator set.
     Before this, automationLadder and promotionGates were prose that nothing
     read: the dashboard claimed each stage "unlocks only after its gate is
     measured, not asserted", while the API accepted any stage from a dropdown
     with no check at all. */
  let ladder = null;
  try {
    /* The runs docs now DO record failures — the handler's catch writes
       status:"dead" to the run as well as the job (it previously only marked
       the job, which nothing read, so cycleErrorRate was structurally zero
       and this gate could never fail no matter how many cycles died). */
    const runsSnap = await A.col(A.COL.runs).orderBy("startedAt", "desc").limit(60).get();
    let runs = 0, bad = 0;
    runsSnap.forEach((d) => { runs += 1; if (d.data().status === "dead" || d.data().error) bad += 1; });

    /* PROMOTION EVIDENCE IS STRICT-COHORT ONLY. paper_sample and
       edge_beats_cost used to count every closed trade in the account, which
       after exploratory auto (and the control cohort) means unvalidated
       trades were being presented to the ladder as strict evidence. A trade
       is admissible only when it was taken under the strict policy (not
       paper-learning-relaxed, not exploratory, not control) and is bound to
       the CURRENT frozen strategy/universe/variants identity. */
    /* Bind to the allocation THIS cycle just computed (and the forward lock
       it just wrote), not the one read at the start of the cycle. On the
       cycle that first names a leader, the start-of-cycle read still says
       "no leader" and baseline trades would have satisfied the challenger's
       sample for one check. */
    const freshAllocation = (typeof allocation !== "undefined" && allocation) || allocationRead || null;
    const freshLock = (typeof forwardLock !== "undefined" && forwardLock)
      || (freshAllocation && freshAllocation.forwardLock) || null;
    const evidenceLeaderId = freshAllocation && freshAllocation.leaderId || null;
    const evidenceSinceMs = freshLock && Number(freshLock.lockedAtMs) > 0 ? Number(freshLock.lockedAtMs) : null;
    const strictTradeAdmit = (t) => LD.strictTradeAdmissible(t, { strategyHash, universeHash, variantsHash },
      { leaderId: evidenceLeaderId, sinceMs: evidenceSinceMs });
    const closedAllSnap = await A.col(A.COL.trades)
      .where("accountId", "==", accountId).get();
    let closedStrict = 0, closedAll = 0;
    closedAllSnap.forEach((d) => { closedAll += 1; if (strictTradeAdmit(d.data())) closedStrict += 1; });
    const closedReal = { size: closedStrict, all: closedAll };
    const commit = process.env.COMMIT_REF || process.env.DEPLOY_ID || "local";
    const feedCfg = M.providerConfig(marketIdentity.provider, marketIdentity.feed);
    const marketDataEligible = feedCfg.consolidated === true && feedCfg.liquidityEligible !== false
      && rp.symbols.length >= Math.ceil(symbols.length * 0.8);
    const leaderCalibration = allocation && allocation.leaderId && calibration
      ? calibration.variants[allocation.leaderId] : null;

    const gateEvidence = {
      ledgerBalanced: ledgerAuditResult ? ledgerAuditResult.pass : null,
      eligibleFillsOnly: executionAuditResult ? executionAuditResult.pass : null,
      cycleErrorRate: runs ? bad / runs : null,
      marketDataEligible,
      historyCoverage: symbols.length ? historyNames / symbols.length : 0,
      shadowDays: shadow && Number.isFinite(shadow.completeDays) ? shadow.completeDays : 0,
      shadowUnavailable: !!(shadow && shadow.error),
      closedRealTrades: closedReal.size,
      closedTradesAllCohorts: closedReal.all,
      evidenceBinding: { leaderId: evidenceLeaderId || "A", sinceMs: evidenceSinceMs,
        strategyHash, universeHash, variantsHash },
      allocation: allocation || {},
      calibration: leaderCalibration || { calibrated: calibration != null, pass: false },
      costMeter: await L.cohortCostMeter(accountId, { admit: strictTradeAdmit }).catch(() => ({})),
      accountCostMeter: await L.costMeter(accountId).catch(() => ({})),
    };

    /* Re-read and mutate control in ONE transaction. A kill, hold, ceiling or
       safety-epoch change during a long cycle now conflicts and retries this
       decision against the fresh state; a stale cycle cannot promote over it. */
    const controlRef = A.col(A.COL.control).doc("control");
    const applied = await A.runTransaction(async (tx) => {
      const freshSnap = await tx.get(controlRef);
      const fresh = freshSnap.exists ? freshSnap.data() : {};
      const freshEpoch = fresh.safetyEpoch || {};
      const freshSafetyEpochValid = frozenIdentityValid && freshEpoch.commit === commit
        && ["accountId", "strategyVersion", "universeVersion", "strategyHash", "universeHash", "variantsHash"]
          .every((k) => freshEpoch[k] === policyIdentity[k] && fresh[k] === policyIdentity[k]);
      const gates = LD.evaluateGates({ ...gateEvidence,
        fixturesPass: fresh.fixturesPass,
        fixturesCurrent: fresh.fixturesCommit === commit,
        safetyEpochValid: freshSafetyEpochValid,
        recallBenchmark: fresh.recallBenchmark ?? null,
      });
      const freshOperating = STATE.describe(fresh);
      const decision = LD.decideStage({
        current: freshOperating.stage, gates,
        operatorCeiling: fresh.operatorCeiling || strategy.operatorCeiling || "approval",
        operatorHold: fresh.operatorHold === true,
        promotionStreak: Number(fresh.ladderStreak) || 0,
        killSwitch: !!fresh.killSwitch,
      });
      const patch = { ladderStreak: decision.streak || 0 };
      let appliedStateChange = null;
      if (decision.changed && !freshOperating.paused && !freshOperating.entriesFrozen) {
        const stateChange = STATE.transition(fresh, STATE.stateForStage(decision.stage), {
          source: "ladder", validEpoch: freshSafetyEpochValid,
          earnedStage: LD.highestEarnedStage(gates), reason: decision.reason,
        });
        if (stateChange.ok) {
          Object.assign(patch, stateChange.patch, {
            stageChangedAt: A.FV.serverTimestamp(),
            stageChangeReason: decision.reason, stageChangedBy: "ladder",
          });
          appliedStateChange = stateChange;
        }
      }
      tx.set(controlRef, patch, { merge: true });
      return { decision, gates, fresh, freshOperating, appliedStateChange };
    });
    const { decision, gates, fresh, freshOperating, appliedStateChange } = applied;

    if (appliedStateChange) {
      await A.col(A.COL.audit).add({
        action: "stage_change", from: freshOperating.stage, to: decision.stage,
        direction: decision.direction, reason: decision.reason,
        at: A.FV.serverTimestamp(), ...A.envelope({ created_by: FN_NAME }),
      });
    }

    ladder = {
      stage: appliedStateChange ? decision.stage : freshOperating.stage,
      previous: freshOperating.stage,
      operatingState: appliedStateChange
        ? appliedStateChange.state.state : freshOperating.state,
      changed: !!appliedStateChange, direction: decision.direction, reason: decision.reason,
      earned: LD.highestEarnedStage(gates),
      ceiling: fresh.operatorCeiling || strategy.operatorCeiling || "approval",
      controlDryRun: appliedStateChange
        ? !appliedStateChange.state.paperLedger : !freshOperating.paperLedger,
      gates, notes: LD.describe(decision.stage, gates, decision),
    };
  } catch (e) { ladder = { error: String(e.message).slice(0, 200) }; }
  await reportRunProgress(runRef, { phase: "reconcile",
    label: "Reconciling cash, positions, trades and displayed NAV", pct: 97,
    detail: "Reading the post-settlement book and verifying that every displayed account total agrees." });

  /* Re-read the book AFTER settlement. The snapshot taken at the top of the
     cycle is what the entry gate must reason about, but reporting it would
     show the operator a book that predates this cycle's own fills — zero
     positions on the very cycle that opened one. */
  let finalBook = book, finalNav = navUsd, positionCoverage = null;
  let finalPositionRows = allPositions, finalCashUsd = cashUsdNow;
  let finalBookReadComplete = false, finalBookReadError = null;
  try {
    const ps = await A.col(A.COL.positions).where("accountId", "==", accountId).get();
    const rows = []; ps.forEach((d) => rows.push(d.data()));
    finalPositionRows = rows;
    const openRows = rows.filter((r) => r.open);
    const nowMs = Date.now();
    positionCoverage = {
      open: openRows.length,
      evaluated: openRows.filter((r) => evaluatedHoldingSymbols.has(r.symbol)).length,
      unevaluated: openRows.filter((r) => !evaluatedHoldingSymbols.has(r.symbol))
        .map((r) => r.symbol),
      unpriced: [...new Set(unpricedHoldings)],
      orphaned: [...new Set(orphanSymbols)],
      pendingUrgentExits: openRows
        .filter((r) => r.exitIntent && r.exitIntent.urgent && r.exitIntent.decisionAtMs)
        .map((r) => ({ symbol: r.symbol, kind: r.exitIntent.kind,
          waitingMinutes: Math.max(0,
            Math.round((nowMs - Number(r.exitIntent.decisionAtMs)) / 60000)) }))
        .filter((r) => r.waitingMinutes >= 30),
    };
    const b2 = await L.balances(accountId);
    finalCashUsd = b2.usd[L.ACCT.CASH] || 0;
    const marked2 = R.markedBook(rows, lastPriceBySymbol, S.sectorOf, {
      cash: finalCashUsd, reserved: b2.usd[L.ACCT.RESERVED] || 0,
    });
    finalNav = Math.max(1, marked2.navUsd);
    finalBook = marked2.book;
    finalBook.navUsd = finalNav;
    finalBookReadComplete = true;
  } catch (e) {
    finalBook = book; finalBook.navUsd = navUsd;
    finalBookReadError = String(e.message || e).slice(0, 160);
  }
  if (finalBook.navUsd == null) finalBook.navUsd = navUsd;

  /* Reconcile the exact book that will be displayed. This catches balanced
     but duplicated lifecycle records, projection drift, reservations that no
     longer have orders, and NAV marks without source-response hashes. */
  let finalReconciliation = null;
  try {
    const marks = Object.fromEntries(finalPositionRows.filter((p) => p.open).map((p) => {
      const priceUsd = lastPriceBySymbol[p.symbol] ?? p.lastMarkUsd;
      const provenance = marketProvenanceBySymbol[p.symbol] || p.lastMarkProvenance;
      return [p.symbol, { priceUsd, provenance }];
    }));
    finalReconciliation = await L.reconcileAccount(accountId, {
      marks, expectedNavUsd: finalNav, context: "cycle_final",
    });
  } catch (e) {
    finalReconciliation = { pass: false, error: String(e.message || e).slice(0, 200) };
  }
  const finalControl = await controlDoc().catch(() => ctrl);
  const finalOperating = STATE.describe(finalControl);

  /* Only the post-settlement book is admissible as final daily evidence. If
     the re-read or write fails, leave the checkpoint open so the scheduler
     retries instead of certifying a stale pre-settlement account value. */
  if (session.dailyFinalized === true && !dailyAlreadyFinalized
      && finalBookReadComplete) {
    try {
      navMarkResult = await writeNavSnapshot({ positionRows: finalPositionRows,
        snapshotNavUsd: finalNav, snapshotCashUsd: finalCashUsd,
        snapshotPositionsUsd: finalBook.grossUsd,
        snapshotOpenPositions: finalBook.count,
        snapshotUnrealisedUsd: finalBook.unrealisedUsd,
        finalized: true });
      navMarkWritten = navMarkResult.written === true;
    } catch (e) {
      navMarkResult = { written: false, finalized: true,
        error: String(e.message || e).slice(0, 160) };
    }
  }

  const dailyFinalizationCommitted = session.dailyFinalized === true
    && !dailyAlreadyFinalized && navMarkWritten === true
    && !!shadow && !shadow.error;
  const dailyFinalizationComplete = dailyAlreadyFinalized
    || dailyFinalizationCommitted;

  const summary = {
    jobId, cycleId,
    shadow: shadow ? { opened: shadow.opened, closed: shadow.closed,
                       totalClosed: shadow.totalClosed, error: shadow.error || null } : null,
    allocationPowered: allocation ? allocation.powered : null,
    liveConfig: {
      variantId: liveVariant ? liveVariant.id : null,
      variantName: liveVariant ? liveVariant.name : null,
      source: cfgSource,
      entryRank: cfg.entryRank, minAbsZ: cfg.minAbsZ,
      exitRank: cfg.exitRank, maxHoldDays: cfg.maxHoldDays,
    },
    settlement: { filled: settled.filled.length, closed: settled.closed.length,
                  autoApproved: settled.autoApproved.length,
                  skipped: settled.skipped.length, error: settled.error || null,
                  expiredAtSessionClose: (settled.expiredAtSessionClose || []).length },
    /* Whether this cycle was permitted to write the paper ledger at all, and
       why not when it was not. This was the single most common silent
       failure: "working now" on the console while every order was refused. */
    entryControl: { pass: entryControl.pass === true, reason: entryControl.pass ? null : (entryControl.reason || null) },
    exploration: operating.exploratoryAuto ? {
      policyVersion: exploratoryPolicy.version || null,
      activity: { ...activityLog, controlOpenAfter: controlOpen + activityLog.controlProposed },
      scoreboard: scoreboard ? {
        closedExploratoryTrades: scoreboard.closedExploratoryTrades ?? null,
        leadingPolicyId: scoreboard.leadingPolicyId || null,
        cohorts: scoreboard.cohorts || null,
        signalVsControl: scoreboard.signalVsControl || null,
        sufficiency: scoreboard.sufficiency || null,
        sample: scoreboard.sample || null,
        policies: scoreboard.policies || null,
        prior: scoreboard.prior || null,
        affectsDecision: scoreboard.affectsDecision || null,
        error: scoreboard.error || null,
      } : null,
      settings: { maxNewEntriesPerCycle: activity.maxNewEntriesPerCycle,
        reservationHeadroomBps: activity.reservationHeadroomBps,
        controlCohort: activity.controlCohort, policySelection: activity.policySelection,
        expireUnfilledEntriesAtSessionClose: activity.expireUnfilledEntriesAtSessionClose,
        minimumShareFloor: activity.minimumShareFloor },
    } : null,
    symbols: symbols.length, rosterSymbols: tradeSymbols.length,
    orphanSymbols, positionCoverage,
    liquidity: {
      provisionalAdvCount: advProvisional.length,
      provisionalAdvSample: advProvisional.slice(0, 10),
      feed: marketIdentity.feed,
      feedVolumeShare: M.feedVolumeShare(marketIdentity.provider, marketIdentity.feed),
      turnoverRanked: Object.keys(turnoverPctile).length,
    },
    panelCoverage: {
      excludedForCoverage: (rp.excludedForCoverage || []).slice(0, 20),
      excludedForCoverageCount: (rp.excludedForCoverage || []).length,
      minSymbolCoverage: rp.minSymbolCoverage != null ? rp.minSymbolCoverage : null,
      maxWindowSpanMultiple: rp.maxWindowSpanMultiple != null
        ? rp.maxWindowSpanMultiple : null,
    },
    rankingDiagnostics,
    quoteCoverage,
    ranked, breaches: candidates.length,
    proposals: decisions.length, strikePlans,
    entryLock: { acquired: !!entryLockOwner && activityLog.entryLockUnavailable == null,
      unavailable: activityLog.entryLockUnavailable || null },
    patience: { policy: patiencePolicy,
      grantedThisScan: patienceClaims.map((c) => c.symbol),
      sleeve: PA.sleeveUsage(allPositions, [...pendingOrders, ...patienceClaims], navUsd, patiencePolicy) },
    exitSignals: exits.length,
    modelCalls,
    decisionManifests: decisionManifestStats,
    bootstrap,
    earningsKnown: Object.keys(earningsWindows).length,
    /* How much memory the system actually had this cycle. If namesWithContext
       is far below symbols, the backfill has not finished and most names are
       still being judged on today alone — the dashboard says so plainly. */
    history: {
      namesWithContext: historyNames,
      avgDays: historyNames ? Math.round(historyDays / historyNames) : 0,
      pooledReversionPct: revShrunk.pooledPct,
      coveragePct: symbols.length ? Number(((historyNames / symbols.length) * 100).toFixed(1)) : 0,
    },
    risk: {
      navUsd: Number(finalBook.navUsd.toFixed(2)),
      openPositions: finalBook.count,
      pendingExposure: { orders: pendingExposure.orders, usd: Number(pendingExposure.usd.toFixed(2)),
        pctOfNav: navUsd > 0 ? Number((100 * pendingExposure.usd / navUsd).toFixed(2)) : 0,
        countedAgainstCaps: true, unavailable: pendingExposureUnavailable || null },
      /* The limits the backend actually enforced this cycle. The console must
         render THESE, not a hardcoded copy of the strict block. */
      limits: {
        capSet: operating.exploratoryAuto ? "exploratory" : "strict",
        maxOpenPositions: activePortfolioControls.maxOpenPositions ?? 12,
        maxGrossExposurePct: activePortfolioControls.maxGrossExposurePct ?? 60,
        minCashPct: activePortfolioControls.minCashPct ?? 40,
        sectorExposurePctOfNav: activePortfolioControls.sectorExposurePctOfNav ?? 20,
        correlatedClusterPctOfNav: activePortfolioControls.correlatedClusterPctOfNav ?? 10,
        oneDayLossPausePctOfNav: activePortfolioControls.oneDayLossPausePctOfNav ?? 1,
        drawdownFreezePctFromHigh: activePortfolioControls.drawdownFreezePctFromHigh ?? 6,
        ordinaryPositionPctOfNav: activePortfolioControls.ordinaryPositionPctOfNav ?? 3,
      },
      grossPct: finalBook.grossPct,
      unrealisedPct: finalBook.unrealisedPct,
      drawdownPct: Number(riskState.drawdownPct.toFixed(2)),
      dayPnlPct: Number(riskState.dayPnlPct.toFixed(2)),
      halted: breakers.halted,
      breakers: breakers.breakers,
      topClusters: Object.entries(finalBook.byClusterPct).sort((a, b) => b[1] - a[1]).slice(0, 5),
      markCoveragePct: finalBook.markCoveragePct,
      portfolioBlocks: portfolioBlocks.length,
      notes: R.describe(finalBook, breakers, activeRiskStrategy),
      reconciliation: finalReconciliation ? {
        pass: finalReconciliation.pass,
        discrepancyCount: (finalReconciliation.journal
          && finalReconciliation.journal.discrepancies || []).length
          + (finalReconciliation.lifecycle
            && finalReconciliation.lifecycle.violations || []).length
          + (finalReconciliation.markViolations || []).length,
        error: finalReconciliation.error || null,
      } : null,
    },
    provider: marketIdentity.provider,
    feed: marketIdentity.feed, adjustment: marketIdentity.adjustment,
    experimentHash: shadowExperiment.experimentHash,
    universeHash, strategyHash, variantsHash,
    providerNote: fetchMeta.note || fetchMeta.error || null,
    regime: { vixNorm: Number.isFinite(reg.vixNorm) ? Number(reg.vixNorm.toFixed(2)) : null,
      cor3m: reg.cor3m, stale: reg.stale, vixHealthy: reg.vixHealthy, corHealthy: reg.corHealthy },
    session: { phase: session.phase, date: session.date },
    dailyFinalization: {
      requested: session.dailyFinalized === true,
      complete: dailyFinalizationComplete,
      date: session.date,
      alreadyFinalized: dailyAlreadyFinalized,
      navFinalized: dailyAlreadyFinalized
        || (session.dailyFinalized === true && navMarkWritten === true),
      navMarksComplete: navMarkResult && navMarkResult.marksComplete === true,
      missingMarkSymbols: navMarkResult && navMarkResult.missingMarkSymbols || [],
      markSetSha256: navMarkResult && navMarkResult.markSetSha256 || null,
      postSettlementBookRead: finalBookReadComplete,
      postSettlementBookError: finalBookReadError,
      learningFinalized: dailyAlreadyFinalized
        || (session.dailyFinalized === true && !!shadow && !shadow.error),
    },
    operatingState: finalOperating.state,
    mode: finalOperating.stage,
    dryRun: !finalOperating.paperLedger,
    ladder,
    elapsedMs: Date.now() - startedAt,
  };

  try {
    summary.soakRecord = await SOAK.recordCycle({ jobId, accountId,
      date: session.date, manual, manifestTotal: decisionManifestStats.total,
      manifestComplete: decisionManifestStats.complete,
      manifestInvalid: decisionManifestStats.invalid,
      reconciliation: finalReconciliation, settlement: summary.settlement,
      dailyFinalization: summary.dailyFinalization });
  } catch (e) {
    summary.soakRecord = { recorded: false, error: String(e.message || e).slice(0, 160) };
  }
  if (session.dailyFinalized === true) {
    try { summary.soakStatus = await SOAK.status(accountId); }
    catch (e) { summary.soakStatus = { operationalSoakPass: false,
      error: String(e.message || e).slice(0, 160) }; }
  }

  await liveFlush(true);
  await runRef.set({
    ...summary, finishedAt: A.FV.serverTimestamp(), status: "complete",
    updatedAt: A.FV.serverTimestamp(), updatedAtMs: Date.now(),
    progress: { phase: "complete", label: "Opportunity cycle complete",
      detail: `${ranked} ranked, ${candidates.length} signal breaches, ${decisions.length} proposals, `
        + `${settled.filled.length} fills and ${settled.closed.length} closes.`,
      pct: 100, completed: symbols.length, total: symbols.length, remaining: 0,
      currentItem: null, updatedAtMs: Date.now() },
  }, { merge: true });

  /* SCAN SNAPSHOT. What the desk SAW this scan, not only what it chose: the
     z-score, rank and cumulative residual for every ranked name (all signal
     windows), the identity-filtered scoreboard that ordered the candidates,
     the exploration counters and the frozen identity. Candidates/Decisions
     record the names that reached the gate stack; this records the whole
     cross-section so a decision can be replayed against its peers later. */
  try {
    const panelOut = {};
    for (const [win, sc] of Object.entries(signalContexts)) {
      const rows = {};
      for (const [sym, zz] of Object.entries(sc.zBySymbol || {})) {
        rows[sym] = { z: Number(zz.z.toFixed(3)), r: Number((sc.ranks[sym] ?? NaN).toFixed(4)),
          cum: Number(((zz.cumResidual || 0) * 1e4).toFixed(1)) };
      }
      panelOut[win] = { n: sc.n || 0, symbols: rows };
    }
    await A.col(A.COL.scanSnapshots).doc(cycleId).set({
      cycleId, jobId, sessionDate: session.date, decisionAtMs: Date.now(),
      sessionPhase: session.phase, provider: marketIdentity.provider, feed: marketIdentity.feed || null,
      strategyVersion: strategy.version, strategyHash, universeHash, variantsHash,
      exploratoryPolicyVersion: (strategy.exploratoryAuto || {}).version || null,
      liveSignalWindow: cfg.signalWindow || 12,
      panel: panelOut,
      betas: rp && rp.betas ? rp.betas : null,
      scoreboard: scoreboard || null,
      exploration: summary.exploration || null,
      entryControl: summary.entryControl || null,
      counts: { symbols: symbols.length, ranked, candidates: candidates.length,
        proposals: decisions.length, fills: settled.filled.length, closes: settled.closed.length },
      ...A.envelope({ created_by: FN_NAME }),
    });
  } catch (e) { summary.snapshotError = String(e.message || e).slice(0, 160); }

  try {
    const NAV = require("./_investorNav");
    const snap = await NAV.snapshot(accountId);
    await NAV.record(accountId, snap, { source: "cycle" });
    summary.navLive = { navUsd: snap.navUsd, unrealisedUsd: snap.unrealisedUsd, open: snap.openPositions };
    await A.col(A.COL.control).doc("control").set({ navLive: snap }, { merge: true });
  } catch (e) { summary.navError = String(e.message || e).slice(0, 120); }

  await A.col(A.COL.control).doc("control").set({
    lastCycleSummary: summary, lastCycleFinishedAt: A.FV.serverTimestamp(),
    /* THE SCHEDULED SLOT IS SATISFIED HERE, not at dispatch. The kick stamps
       only its cadence clock; if this worker had died after Netlify accepted
       the invocation, the slot would still be due and the next kick would
       re-claim it (a dead job document is not a claimed status). */
    ...(planSlotKey ? { lastPlanKey: planSlotKey } : {}),
    ...(summary.soakStatus ? { lastSoakStatus: summary.soakStatus } : {}),
    ...(dailyFinalizationCommitted ? {
      lastDailyFinalizeDate: session.date,
      lastDailyFinalizedAt: A.FV.serverTimestamp(),
      ...(navMarkResult && navMarkResult.marksComplete === true
        ? { lastNavCompleteDate: session.date } : {}),
    } : {}),
  }, { merge: true });

  return summary;
}

/* ── evidence sweep (slower clock) ─────────────────────────────────────── */
/* NIGHTLY INTRADAY ARCHIVE. The scan stores the bars it fetched for the names
   the provider returned; a name the provider omitted, or a session where the
   scan did not run, leaves a hole. After the close this walks every frozen
   trade-tier name, finds session documents short of a full day's bars, and
   fills them from the provider, so the intraday record is complete
   regardless of which names were ranked or viewed. Display/archive only:
   it feeds no decision. */
const FULL_SESSION_BARS = 78, HALF_SESSION_BARS = 42;
async function runArchive(jobId) {
  const startedMs = Date.now();
  const runRef = A.col(A.COL.archiveRuns).doc(jobId);
  const session = M.sessionState(new Date());
  const provider = M.activeProvider();
  const ctrlSnap = await A.col(A.COL.control).doc("control").get();
  const ctrl = ctrlSnap.exists ? ctrlSnap.data() : {};
  const universe = await loadUniverse(ctrl.universeVersion);
  const symbols = (universe.tradeTier || []).map((t) => t.symbol);
  const expected = session.isHalfDay ? HALF_SESSION_BARS : FULL_SESSION_BARS;
  const summary = { jobId, kind: "archive", sessionDate: session.date, provider: provider.id,
    feed: provider.feed || null, symbols: symbols.length, expectedBars: expected,
    short: 0, fetched: 0, completed: 0, stillShort: [], chunks: null, error: null };
  await runRef.set({ ...summary, startedAt: A.FV.serverTimestamp(), status: "running",
    ...A.envelope({ created_by: FN_NAME }) }, { merge: true });
  try {
    if (!session.tradingDay) {
      summary.note = "not a trading day";
    } else if (provider.id === "manual") {
      summary.note = "manual provider: nothing to fetch";
    } else {
      const counts = {};
      const snaps = await Promise.all(symbols.map((sym) =>
        A.col(A.COL.marketLatest).doc(M.barDocId(sym, session.date)).get()));
      snaps.forEach((d, i) => { counts[symbols[i]] = d.exists ? Number(d.data().barCount || (d.data().bars || []).length) : 0; });
      const short = symbols.filter((sym) => counts[sym] < expected - 2);
      summary.short = short.length;
      if (short.length) {
        const got = await M.fetchBarsChunked(short, { timeframe: "5Min", limit: 90 },
          { chunkSize: 60, retries: 1 });
        summary.chunks = got.chunks || null;
        const bars = got.bars || {};
        for (const sym of short) {
          const b = bars[sym] || [];
          if (!b.length) continue;
          summary.fetched += 1;
          try {
            await M.writeBars(sym, session.date, b, { provider: got.provider, feed: got.feed || null,
              adjustment: got.adjustment || null,
              sourceSha256: (got.symbolSha256 && got.symbolSha256[sym]) || got.sha256 || null,
              grade: null, gradeReasons: ["nightly_archive"], feedDelayMinutes: provider.delayMinutes });
          } catch (e) { summary.writeErrors = (summary.writeErrors || 0) + 1; }
        }
        const after = await Promise.all(short.map((sym) =>
          A.col(A.COL.marketLatest).doc(M.barDocId(sym, session.date)).get()));
        after.forEach((d, i) => {
          const n = d.exists ? Number(d.data().barCount || (d.data().bars || []).length) : 0;
          if (n >= expected - 2) summary.completed += 1; else summary.stillShort.push(`${short[i]}:${n}`);
        });
        summary.stillShort = summary.stillShort.slice(0, 40);
      }
    }
  } catch (e) { summary.error = String(e.message || e).slice(0, 200); }
  summary.elapsedMs = Date.now() - startedMs;
  await runRef.set({ ...summary, status: summary.error ? "dead" : "complete",
    finishedAt: A.FV.serverTimestamp() }, { merge: true });
  await A.col(A.COL.control).doc("control").set({
    lastArchiveDate: session.date, lastArchiveSummary: summary,
    lastArchiveFinishedAt: A.FV.serverTimestamp() }, { merge: true });
  return summary;
}

async function runEvidence(jobId) {
  const startedAt = Date.now();
  const runRef = A.col(A.COL.runs).doc(jobId);
  await runRef.set({ jobId, kind: "evidence", status: "running",
    startedAt: A.FV.serverTimestamp(), startedAtMs: startedAt,
    ...A.envelope({ created_by: FN_NAME }) }, { merge: true });
  await reportRunProgress(runRef, { phase: "refresh_sources",
    label: "Refreshing shared company-research inputs", pct: 3,
    detail: "Refreshing SEC identity, earnings, shares, market regime and daily-history inputs used by every dossier." });
  /* Long-running SEC, earnings, shares and daily-history enrichment belongs to
     this background lane. Price cycles only attest the frozen identity, so a
     slow public source can never postpone the next opportunity scan. */
  const bootstrap = await B.ensureBootstrapped({ enrich: true })
    .catch((e) => ({ bootstrapped: false, error: String(e.message).slice(0, 160) }));
  await reportRunProgress(runRef, { phase: "choose_research",
    label: "Choosing which companies to research next", pct: 15,
    detail: "Shared inputs are refreshed; prioritizing holdings, leading candidates and stale dossiers." });
  const ctrl = await controlDoc();
  const universe = await loadUniverse(ctrl.universeVersion);
  const strategy = await loadStrategy(ctrl.strategyVersion);
  const earningsWindows = await B.readEarnings().catch(() => ({}));
  const accountId = ctrl.accountId || "paper-1";
  const positionSnap = await A.col(A.COL.positions).where("accountId", "==", accountId)
    .where("open", "==", true).get();
  const positions = positionSnap.docs.map((d) => d.data());
  let recentCandidates = [];
  try {
    const latestCandidateCycle = ctrl.lastCycleSummary && ctrl.lastCycleSummary.cycleId;
    if (latestCandidateCycle) {
      const candidateSnap = await A.col(A.COL.candidates)
        .where("cycleId", "==", latestCandidateCycle).get();
      recentCandidates = candidateSnap.docs.map((d) => d.data());
    }
  } catch {}
  const allRows = [...(universe.tradeTier || []), ...(universe.researchTier || [])];
  const bySymbol = Object.fromEntries(allRows.map((row) => [row.symbol, row]));
  const focus = IS.focusSymbols({ ctrl, positions,
    researchTier: universe.researchTier || [], candidates: recentCandidates,
    max: (strategy.parameters || {}).intelligenceMaxFocus || IS.MAX_FOCUS });
  const perSweep = Math.max(1, Math.min(8, Number(process.env.INVESTOR_INTELLIGENCE_COMPANIES_PER_SWEEP)
    || Number((strategy.parameters || {}).intelligenceCompaniesPerSweep) || 4));
  const cursor = focus.length ? Math.max(0, Number(ctrl.intelligenceCursor) || 0) % focus.length : 0;
  const selected = [];
  for (let i = 0; i < Math.min(perSweep, focus.length); i += 1) selected.push(focus[(cursor + i) % focus.length]);
  const results = [];
  await reportRunProgress(runRef, { phase: "research_companies",
    label: "Researching selected companies", pct: 20, completed: 0,
    total: selected.length,
    detail: selected.length
      ? `${selected.length} companies selected from a rotating priority queue of ${focus.length}.`
      : "No company currently qualifies for this research sweep." });
  for (let selectedIndex = 0; selectedIndex < selected.length; selectedIndex += 1) {
    const focusRow = selected[selectedIndex];
    const t = bySymbol[focusRow.symbol] || { symbol: focusRow.symbol };
    const asOfMs = Date.now();
    await reportRunProgress(runRef, { phase: "research_companies",
      label: `Researching ${t.symbol}`, pct: 20 + Math.round(68 * selectedIndex / Math.max(1, selected.length)),
      completed: selectedIndex, total: selected.length, currentItem: t.symbol,
      detail: `Checking identity, SEC filings, official sources, relationships, events and cited adverse findings for ${t.symbol}.` });
    try {
      const prior = await I.readSnapshot(t.symbol).catch(() => null);
      const configuredProfile = ctrl.intelligenceProfiles && ctrl.intelligenceProfiles[t.symbol] || {};
      let profile = await IS.resolveIdentity({ ...t, ...configuredProfile,
        relatedEntities: prior && prior.relatedEntities || configuredProfile.relatedEntities || [],
        temporalExposures: prior && prior.temporalContext && prior.temporalContext.exposures
          || configuredProfile.temporalExposures || [] });
      let secHealthy = false, secFresh = 0, secError = null;
      if (profile.cik) {
        const history = await E.pollSubmissionsHistory(profile.cik, {
          lookbackDays: Number((strategy.parameters || {}).intelligenceLookbackDays) || 180,
          limit: 120,
        });
        const latest = await E.pollEdgar(profile.cik, { forms: "" });
        secHealthy = !history.error;
        const combined = [...(history.entries || []), ...(latest.entries || [])]
          .filter((d, i, a) => d.accession && a.findIndex((x) => x.accession === d.accession) === i);
        const bodyAccessions = new Set([...combined].sort((a, b) => {
          const weight = (x) => Number(E.FORM_WEIGHT[x.form] || 0);
          return weight(b) - weight(a) || Date.parse(b.updated || 0) - Date.parse(a.updated || 0);
        }).slice(0, 16).map((x) => x.accession));
        secFresh = combined.length;
        secError = history.error || latest.error || null;
        for (const d of combined.slice(0, 120)) {
          let body = { text: "", sha256: null };
          if (bodyAccessions.has(d.accession)) { try { body = await E.fetchFilingBody(d); } catch {} }
          await E.recordVersion({ symbol: t.symbol, sourceId: d.sourceId || "sec.latest", entry: d,
            rawSha256: history.sha256 || latest.sha256, body: body.text, bodySha256: body.sha256 });
        }
      }
      let documents = await E.documentsForCompany(profile.symbol, Date.now(),
        Number((strategy.parameters || {}).intelligenceLookbackDays) || I.LOOKBACK_DAYS, 260);
      profile = IS.enrichProfile(profile, documents, configuredProfile.officialDomains || []);
      const publicPoll = await IS.pollCompany(profile, {
        budgetMs: Math.max(30000, Math.min(150000,
          Number(process.env.INVESTOR_INTELLIGENCE_COMPANY_BUDGET_MS) || 110000)),
      });
      let dossierAsOfMs = Date.now();
      let coverage = I.sourceCoverage(profile, publicPoll.results, { secHealthy, asOfMs: dossierAsOfMs,
        sourceRegistry: IS.SOURCE_REGISTRY });
      documents = await E.documentsForCompany(profile.symbol, dossierAsOfMs,
        Number((strategy.parameters || {}).intelligenceLookbackDays) || I.LOOKBACK_DAYS, 260);
      let dailyBars = [], companyDailyProvenance = null;
      try {
        const daily = await H.readDailyWithMeta(profile.symbol);
        dailyBars = daily.series || []; companyDailyProvenance = daily.provenance || null;
      } catch {}
      const deterministic = I.extractEvents(documents, dossierAsOfMs);
      const synthesis = await O.synthesizeIntelligence({ profile, documents,
        priceContext: I.priceContext(dailyBars, dossierAsOfMs), coverage, asOfMs: dossierAsOfMs });
      const modelEvents = synthesis.ok ? (synthesis.rawEvents || []) : [];
      const rawEvents = I.mergeEventHypotheses(modelEvents, deterministic);
      const priorTemporalExposures = profile.temporalExposures || [];
      const deterministicTemporalExposures = T.extractDeterministicExposures(documents, dossierAsOfMs);
      const rawTemporalExposures = [...(synthesis.ok ? synthesis.temporalExposures || [] : []),
        ...deterministicTemporalExposures, ...priorTemporalExposures].filter((x, index, rows) => x && x.exposureType
          && rows.findIndex((y) => y && y.exposureType === x.exposureType
            && String(y.support && y.support.documentRef || "") === String(x.support && x.support.documentRef || "")
            && String(y.support && y.support.quote || "") === String(x.support && x.support.quote || "")) === index);
      const normalizedExposures = T.normalizeExposures(rawTemporalExposures, documents, dossierAsOfMs);
      let nws = publicPoll.results.find((x) => x.sourceId === "nws.alerts") || null;
      const neededStates = [...new Set(normalizedExposures.flatMap((x) => x.states || []))];
      const queriedStates = new Set(nws && nws.statesQueried || []);
      if (neededStates.some((state) => !queriedStates.has(state))) {
        nws = await IS.pollSource({ ...profile, temporalExposures: normalizedExposures }, "nws.alerts");
        const at = publicPoll.results.findIndex((x) => x.sourceId === "nws.alerts");
        if (at >= 0) publicPoll.results[at] = nws; else publicPoll.results.push(nws);
      }
      const driverSymbols = new Set([T.DRIVER_BY_SECTOR[profile.sector] || "SPY"]);
      for (const x of normalizedExposures) if (x.driverSymbol) driverSymbols.add(x.driverSymbol);
      const driverSeries = {}, driverProvenance = {};
      for (const symbol of driverSymbols) {
        try {
          const daily = await H.readDailyWithMeta(symbol);
          driverSeries[symbol] = daily.series || []; driverProvenance[symbol] = daily.provenance || null;
        }
        catch { driverSeries[symbol] = []; }
      }
      dossierAsOfMs = Date.now();
      coverage = I.sourceCoverage(profile, publicPoll.results, { secHealthy, asOfMs: dossierAsOfMs,
        sourceRegistry: IS.SOURCE_REGISTRY });
      const snapshot = I.buildSnapshot({ profile, documents, dailyBars, rawEvents,
        coverage, requireTemporalContext: (strategy.parameters || {}).requireTemporalContext === true,
        temporalInputs: { driverSeries, driverProvenance, companyProvenance: companyDailyProvenance,
          earningsWindow: earningsWindows[profile.symbol] || null,
          rawExposures: rawTemporalExposures, activeHazards: nws && nws.temporalHazards || [],
          temporalSourceHealth: {
            exposureInventory: synthesis.ok === true,
            nws: !!(nws && nws.healthy && nws.coverageComplete !== false),
            requireNwsProvenance: true,
            priceProvenance: !!(companyDailyProvenance && companyDailyProvenance.provider
              && companyDailyProvenance.adjustment
              && companyDailyProvenance.homogeneous === true
              && /^[a-f0-9]{64}$/.test(String(companyDailyProvenance.sourceSha256 || ""))),
            nwsResponses: nws && nws.responses || [],
          } },
        asOfMs: dossierAsOfMs,
        maxAgeHours: Number((strategy.parameters || {}).intelligenceMaxAgeHours) || 6,
        temporalMaxAgeHours: Number((strategy.parameters || {}).temporalMaxAgeHours) || 6,
        minSeasonalityDays: Number((strategy.parameters || {}).temporalSeasonalityMinTradingDays) || T.MIN_SEASONAL_DAYS });
      snapshot.focusReason = focusRow.reason;
      snapshot.identity = { cik: profile.cik, sic: profile.sic,
        sicDescription: profile.sicDescription, identitySource: profile.identitySource,
        officialDomains: profile.officialDomains, domainResolution: profile.domainResolution || [],
        sectorPack: profile.sectorPack };
      snapshot.relatedEntities = [...(synthesis.relatedEntities || []), ...(profile.relatedEntities || [])]
        .filter((x, index, rows) => x && x.name && rows.findIndex((y) =>
          String(y.name).toLowerCase() === String(x.name).toLowerCase()
          && y.relationship === x.relationship) === index)
        .sort((a, b) => Number(b.searchPriority || b.confidence || 0)
          - Number(a.searchPriority || a.confidence || 0)).slice(0, 20);
      snapshot.synthesis = {
        usedModel: synthesis.ok === true && modelEvents.length > 0,
        abstained: synthesis.abstained === true,
        error: synthesis.ok ? null : synthesis.error || null,
        citationValidity: synthesis.citationValidity ?? null,
        provenance: synthesis.provenance || null,
      };
      await I.storeSnapshot(snapshot);
      results.push({ symbol: t.symbol, focusReason: focusRow.reason,
        documents: documents.length, events: snapshot.events.length,
        coverageComplete: coverage.complete, missingRoles: coverage.missingRoles,
        adverseRiskScore: snapshot.policy.adverseRiskScore,
        temporalRiskScore: snapshot.policy.temporalPolicy && snapshot.policy.temporalPolicy.riskScore,
        entryAllowed: snapshot.policy.entryAllowed, criticalExit: snapshot.policy.criticalExit,
        secFresh, secError, sourceHealthy: coverage.healthySources.length,
        sourceFailed: coverage.failedSources.length, sourceDeferred: coverage.deferredSources.length,
        dossierHash: snapshot.dossierHash });
    } catch (e) {
      results.push({ symbol: t.symbol, error: String(e.code || e.message).slice(0, 120) });
    }
    await reportRunProgress(runRef, { phase: "research_companies",
      label: "Researching selected companies",
      pct: 20 + Math.round(68 * (selectedIndex + 1) / Math.max(1, selected.length)),
      completed: selectedIndex + 1, total: selected.length,
      detail: `${selectedIndex + 1} of ${selected.length} company dossiers completed in this sweep.` });
  }
  await reportRunProgress(runRef, { phase: "publish_research",
    label: "Publishing research results for the decision engine", pct: 92,
    completed: results.length, total: selected.length,
    detail: "Saving the new focus cursor and making completed dossiers available to the next opportunity cycle." });
  const nextCursor = focus.length ? (cursor + selected.length) % focus.length : 0;
  const summary = { jobId, kind: "evidence", bootstrap,
    focusCount: focus.length,
    selected: selected.map((x) => x.symbol), swept: results.length, nextCursor,
    publicSourceMode: true, results, elapsedMs: Date.now() - startedAt };
  await A.col(A.COL.control).doc("control").set({ intelligenceCursor: nextCursor,
    intelligenceFocus: focus.map((x) => x.symbol), lastIntelligenceSummary: summary,
    lastIntelligenceAt: A.FV.serverTimestamp() }, { merge: true });
  await runRef.set({
    ...summary, finishedAt: A.FV.serverTimestamp(), status: "complete",
    updatedAt: A.FV.serverTimestamp(), updatedAtMs: Date.now(),
    progress: { phase: "complete", label: "Company-research sweep complete",
      detail: `${results.length} of ${selected.length} selected company dossiers completed.`,
      pct: 100, completed: results.length, total: selected.length,
      remaining: Math.max(0, selected.length - results.length), currentItem: null,
      updatedAtMs: Date.now() },
  }, { merge: true });
  return summary;
}

/* ── handler ───────────────────────────────────────────────────────────── */
exports.handler = async (event) => {
  let body = {};
  try { body = JSON.parse(event.body || "{}"); } catch {}
  const { jobId, task, nonce } = body;

  /* The nonce is verified against the session secret, so it must resolve
     first. If it cannot, verifyWorkerNonce returns null and this public URL
     rejects everything — including its own scheduler. */
  await AUTH.loadAuthSecrets();

  // A background function is a public URL. Only a nonce minted by the
  // scheduler for THIS job and THIS function is accepted.
  const claim = verifyWorkerNonce(nonce, FN_NAME);
  if (!claim || claim.jobId !== jobId) {
    console.warn("investorCycle: rejected unauthenticated invocation");
    return { statusCode: 403, body: JSON.stringify({ error: "invalid or missing worker nonce" }) };
  }

  if (!jobId || !["cycle", "guard", "evidence", "archive"].includes(task)) {
    return { statusCode: 400, body: JSON.stringify({ error: "invalid job shape" }) };
  }
  /* Resolve provider/feed before any market call. A failed read falls back to
     the environment and then to manual, which is grade C and cannot trade. */
  await M.loadMarketSettings();
  const jobRef = A.col(A.COL.jobs).doc(jobId);
  const leaseOwner = `${process.env.AWS_LAMBDA_LOG_STREAM_NAME || "worker"}:${Date.now()}:${Math.random().toString(36).slice(2, 8)}`;
  let accountCycleLeaseRef = null;
  try {
    const lease = await A.runTransaction(async (tx) => {
      const snap = await tx.get(jobRef);
      if (!snap.exists) return { claim: false, reason: "job_not_dispatched" };
      const j = snap.data();
      if (j.task !== task) return { claim: false, reason: "task_mismatch" };
      if (j.status === "complete") return { claim: false, duplicate: true };
      if (j.status === "running" && Number(j.workerLeaseExpiresAt) > Date.now()) {
        return { claim: false, inFlight: true };
      }
      /* Job IDs change every cadence slot, so a job-only lease still permits
         slot N+1 to overlap a slow slot N and size from the same stale book.
         Claim one additional lease for the account's entire price cycle. */
      if (task === "cycle" || task === "guard") {
        const account = String(j.accountId || "");
        if (!/^[a-zA-Z0-9_-]{3,40}$/.test(account)) {
          return { claim: false, reason: "invalid_job_account" };
        }
        const leaseId = task === "cycle"
          ? `account_cycle_lease_${account}` : `account_guard_lease_${account}`;
        accountCycleLeaseRef = A.col(A.COL.jobs).doc(leaseId);
        const accountLease = await tx.get(accountCycleLeaseRef);
        const active = accountLease.exists ? accountLease.data() : {};
        if (Number(active.leaseExpiresAt) > Date.now() && active.jobId !== jobId) {
          return { claim: false, inFlight: true, reason: "account_cycle_in_flight" };
        }
        const accountLeaseTtl = task === "guard"
          ? GUARD_LEASE_TTL_MS : WORKER_LEASE_TTL_MS;
        tx.set(accountCycleLeaseRef, { kind: `account_${task}_lease`, accountId: account,
          jobId, leaseOwner, leaseExpiresAt: Date.now() + accountLeaseTtl,
          claimedAt: A.FV.serverTimestamp() }, { merge: true });
      }
      tx.set(jobRef, { status: "running", leaseOwner,
        workerLeaseExpiresAt: Date.now() + WORKER_LEASE_TTL_MS,
        startedAt: A.FV.serverTimestamp(), attempts: A.FV.increment(1) }, { merge: true });
      return { claim: true, accountLease: !!accountCycleLeaseRef,
        manual: j.manual === true };
    });
    if (lease.duplicate) {
      return { statusCode: 200, body: JSON.stringify({ ok: true, duplicate: true, jobId }) };
    }
    if (!lease.claim) {
      /* A dispatch refused because the account's previous scan is still
         running used to stay "queued" forever, and the console showed it as
         live work at 0% for twenty minutes. Mark it so the desk can say what
         it is: a duplicate slot that yielded to the scan in flight. */
      if (lease.inFlight && lease.reason === "account_cycle_in_flight") {
        try {
          await jobRef.set({ status: "yielded", yieldedAt: A.FV.serverTimestamp(),
            yieldReason: "a scan for this account was already running" }, { merge: true });
        } catch { /* best effort */ }
      }
      return { statusCode: lease.inFlight ? 202 : 409,
        body: JSON.stringify({ ok: false, jobId, reason: lease.reason || "already running" }) };
    }
    LEASE.setActive({ jobRef, accountLeaseRef: accountCycleLeaseRef, leaseOwner,
      ttlMs: task === "guard" ? GUARD_LEASE_TTL_MS : WORKER_LEASE_TTL_MS });

    const out = task === "evidence" ? await runEvidence(jobId)
      : task === "archive" ? await runArchive(jobId)
      : task === "guard" ? await PG.runGuard(jobId)
        : await runCycle(jobId, { manual: lease.manual });

    await A.runTransaction(async (tx) => {
      const current = await tx.get(jobRef);
      const accountLease = accountCycleLeaseRef ? await tx.get(accountCycleLeaseRef) : null;
      if (!current.exists || current.data().leaseOwner !== leaseOwner) {
        throw new Error("worker lease lost before completion");
      }
      if (accountLease && (!accountLease.exists || accountLease.data().leaseOwner !== leaseOwner)) {
        throw new Error("account cycle lease lost before completion");
      }
      tx.set(jobRef, { status: "complete", finishedAt: A.FV.serverTimestamp(),
        workerLeaseExpiresAt: 0, summary: out }, { merge: true });
      if (accountCycleLeaseRef) tx.set(accountCycleLeaseRef, { leaseExpiresAt: 0,
        releasedAt: A.FV.serverTimestamp(), lastJobId: jobId }, { merge: true });
    });
    LEASE.clear();
    console.log("investorCycle done", JSON.stringify(redact(out)));
    return { statusCode: 200, body: JSON.stringify({ ok: true, ...out }) };
  } catch (e) {
    LEASE.clear();
    console.error("investorCycle failed", redact({ jobId, task, error: e.message, stack: (e.stack || "").slice(0, 400) }));
    try {
      await A.runTransaction(async (tx) => {
        const current = await tx.get(jobRef);
        const accountLease = accountCycleLeaseRef ? await tx.get(accountCycleLeaseRef) : null;
        if (current.exists && current.data().leaseOwner === leaseOwner) {
          tx.set(jobRef, { status: "dead", error: String(e.message).slice(0, 300),
            workerLeaseExpiresAt: 0, finishedAt: A.FV.serverTimestamp() }, { merge: true });
        }
        if (accountLease && accountLease.exists && accountLease.data().leaseOwner === leaseOwner) {
          tx.set(accountCycleLeaseRef, { leaseExpiresAt: 0,
            releasedAt: A.FV.serverTimestamp(), lastJobId: jobId }, { merge: true });
        }
      });
    } catch {}
    /* The run manifest is what the dashboard and the cycle_health gate read;
       marking only the job left failures invisible to both. */
    try {
      await A.col(A.COL.runs).doc(jobId).set({
        status: "dead", error: String(e.message).slice(0, 300),
        finishedAt: A.FV.serverTimestamp(),
      }, { merge: true });
    } catch {}
    return { statusCode: 500, body: JSON.stringify({ ok: false, error: String(e.message).slice(0, 200) }) };
  }
};

exports.runCycle = runCycle;
exports.runArchive = runArchive;
exports.reportRunProgress = reportRunProgress;
exports.WORKER_LEASE_TTL_MS = WORKER_LEASE_TTL_MS;
exports.applyOverfitGuard = applyOverfitGuard;
exports.attentionZ = attentionZ;
exports.executionSourceEligible = executionSourceEligible;
/* Exported for deterministic invariant tests. These are pure helpers; the
   worker entry point remains runCycle. */
exports.advFor = advFor;
exports.barTimeframeMs = barTimeframeMs;
exports.WORKER_LEASE_TTL_MS = WORKER_LEASE_TTL_MS;
exports.GUARD_LEASE_TTL_MS = GUARD_LEASE_TTL_MS;
