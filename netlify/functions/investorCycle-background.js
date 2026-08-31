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
 *  This worker NEVER places a real order. It writes proposals; a human
 *  approves them in the console, or in a later phase a validated book may
 *  auto-approve within its own caps. There is no broker integration.
 * ---------------------------------------------------------------------------
 */

"use strict";

const A = require("./_investorAdmin");
const { verifyWorkerNonce, redact } = require("./_investorAuth");
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
const RS = require("./_investorResearchStats");

const FN_NAME = "investorCycle-background";
const WORKER_LEASE_TTL_MS = 14 * 60 * 1000;
const GUARD_LEASE_TTL_MS = 4 * 60 * 1000;

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
function attentionZ(bars, window = 12) {
  const series = M.normalizeBars(bars).filter((b) => M.validateBar(b).length === 0);
  if (series.length < window + 24) return 0;

  const tagged = series.map((b) => ({
    clock: M.nyParts(new Date(b.t)),
    logVolume: Math.log1p(Math.max(0, Number(b.v) || 0)),
  }));
  const latestDate = tagged[tagged.length - 1].clock.date;
  const recent = tagged.filter((x) => x.clock.date === latestDate).slice(-window);
  if (recent.length < Math.min(window, 6)) return 0;

  const priorBySlot = new Map();
  for (const x of tagged) {
    if (x.clock.date === latestDate) continue;
    const key = x.clock.minutes;
    if (!priorBySlot.has(key)) priorBySlot.set(key, []);
    priorBySlot.get(key).push(x.logVolume);
  }

  /* Requiring three prior sessions per slot prevents a single print from
     defining its own benchmark; 18 pooled residuals keep a partial cache
     neutral. The pooled scale stabilizes sparse per-slot estimates. */
  const slotMeans = new Map(), baselineResiduals = [];
  for (const [slot, values] of priorBySlot.entries()) {
    if (values.length < 3) continue;
    const mu = S.mean(values);
    slotMeans.set(slot, mu);
    for (const value of values) baselineResiduals.push(value - mu);
  }
  if (baselineResiduals.length < 18) return 0;
  const sd = S.stdev(baselineResiduals);
  if (!(sd > 1e-6)) return 0;

  const residuals = recent
    .filter((x) => slotMeans.has(x.clock.minutes))
    .map((x) => x.logVolume - slotMeans.get(x.clock.minutes));
  if (residuals.length < Math.min(recent.length, 6)) return 0;
  return Math.max(-8, Math.min(8, S.mean(residuals) / sd));
}

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

async function runCycle(jobId) {
  const startedAt = Date.now();

  /* Self-bootstrap. First cycle freezes the universe, resolves CIKs from SEC,
     opens the paper account, derives earnings windows and fetches VIX. Every
     cycle after that this is a single read plus the slow-clock refreshes. */
  let bootstrap = null;
  try { bootstrap = await B.ensureBootstrapped(); }
  catch (e) { bootstrap = { error: String(e.message).slice(0, 160) }; }

  const ctrl = await controlDoc();
  const strategy = await loadStrategy(ctrl.strategyVersion);
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
         the old definitions, so a stale hash must not be allowed to steer
         live money. Fail back to the baseline and say so. */
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

  const accountId = ctrl.accountId || "paper-1";
  let entryControl = L.controlAllowsEntry(ctrl, policyIdentity);
  const session = M.sessionState(new Date());
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

  const runRef = A.col(A.COL.runs).doc(jobId);
  await runRef.set({
    jobId, kind: "cycle", startedAt: A.FV.serverTimestamp(),
    strategyVersion: strategy.version, universeVersion: universe.version,
    strategyHash, universeHash, variantsHash, frozenIdentityValid,
    session, regime: reg, symbolCount: symbols.length,
    ...A.envelope({ created_by: FN_NAME }),
  }, { merge: true });

  /* 1. bars ------------------------------------------------------------- */
  const provider = M.activeProvider();
  let panel = {}, fetchMeta = {}, marketProvenanceBySymbol = {};
  try {
    const got = await M.fetchBars(symbols, { timeframe: cfg.barTimeframe || "5Min", limit: 120 });
    panel = got.bars || {};
    fetchMeta = { provider: got.provider, feed: got.feed || null,
      adjustment: got.adjustment || null, fetchedAt: got.fetchedAt,
      manifestSha256: got.manifestSha256 || got.sha256 || null,
      symbolSha256: got.symbolSha256 || {}, note: got.note || null,
      failureCount: got.failureCount || 0 };
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

  // Fall back to stored bars for any symbol the provider did not return.
  for (const sym of symbols) {
    if (!panel[sym] || panel[sym].length < 20) {
      try {
        const stored = await M.readRecentBarsWithMeta(sym, 2);
        if (stored.bars.length && stored.provenance) {
          panel[sym] = stored.bars;
          marketProvenanceBySymbol[sym] = stored.provenance;
        }
      } catch {}
    }
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
  const identityCounts = new Map();
  for (const sym of symbols) {
    const p = marketProvenanceBySymbol[sym];
    if (!p || !quality[sym] || !quality[sym].tradable) continue;
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
    marketIdentity });
  if (liveVariant && allocationRead && allocationRead.experimentHash !== shadowExperiment.experimentHash) {
    liveVariant = null;
    cfg = { ...baseParams };
    cfgSource = "baseline — the stored leader belongs to a different market-data experiment";
  }
  const tradable = {};
  for (const sym of tradeSymbols) {
    const p = marketProvenanceBySymbol[sym];
    const key = p ? JSON.stringify([p.provider, p.feed || null, p.adjustment || null]) : null;
    if ((panel[sym] || []).length >= 24 && quality[sym] && quality[sym].tradable
        && key === dominantIdentityKey) tradable[sym] = panel[sym];
  }
  /* Build every preregistered formation horizon from the same point-in-time
     panel. A 6-bar or 24-bar challenger must own its residual fit, z-score and
     cross-sectional rank; applying a different threshold to the 12-bar score
     would be a mislabeled experiment. */
  const signalWindows = [...new Set([cfg.signalWindow || 12,
    ...V.VARIANTS.map((v) => V.configFor(v.id).signalWindow || 12)])].sort((a, b) => a - b);
  const signalContexts = {};
  for (const signalWindow of signalWindows) {
    const panelResult = S.residualPanel(tradable, {
      signalWindow, minCoverageRatio: 0.65, minSymbolCoverageRatio: 0.80,
      intervalMs: barTimeframeMs(cfg.barTimeframe || "5Min"), quality,
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
  const crowd = S.sectorCrowding(ranks, S.sectorOf, cfg.entryRank ?? S.ENTRY_RANK);

  /* 4-6. evaluate each symbol ------------------------------------------- */
  const cycleId = `${today}_${new Date().toISOString().slice(11, 16).replace(":", "")}`;
  const candidates = [], decisions = [], exits = [], portfolioBlocks = [], proposalQueue = [];
  let modelCalls = 0;

  const causeBySymbol = {};
  const coverageBySymbol = {};
  const attentionBySymbol = {};
  const lastPriceBySymbol = {};
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
    if (b && b.length && quality[sym] && quality[sym].tradable
        && key === dominantIdentityKey
        && /^[a-f0-9]{64}$/.test(String(p.sourceSha256 || ""))) {
      lastPriceBySymbol[sym] = b[b.length - 1].c;
    }
  }

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
  const startOfDayNav = Number(ctrl.startOfDayNavUsd) || navUsd;
  const riskState = {
    navUsd, hwmUsd: hwm,
    untrustedOpenMarks: marked.untrustedMarks,
    drawdownPct: hwm > 0 ? ((navUsd - hwm) / hwm) * 100 : 0,
    dayPnlPct: startOfDayNav > 0 ? ((navUsd - startOfDayNav) / startOfDayNav) * 100 : 0,
  };
  const breakers = R.accountBreakers(riskState, strategy);

  /* Symbols with an order already in flight. The duplicate check in the
     portfolio gate looks at POSITIONS, so a proposed-but-unapproved order was
     invisible to it and the same dip re-proposed the same symbol every five
     minutes — twelve stacked orders an hour for one name, each approvable. */
  const pendingOrderSymbols = new Set();
  try {
    for (const st of ["proposed", "approved"]) {
      const q = await A.col(A.COL.orders)
        .where("accountId", "==", accountId).where("status", "==", st).limit(100).get();
      q.forEach((d) => pendingOrderSymbols.add(d.data().symbol));
    }
  } catch (e) { /* on failure, the recordFill double-fill guard still holds the line */ }

  /* One NAV mark per trading day, which is what every performance number is
     built from. Without this there is no equity curve, so no drawdown, no
     volatility, and no way to answer "did this beat just holding the names". */
  try {
    await A.col(A.COL.control).doc("control").collection("navHistory").doc(today).set({
      date: today, navUsd: Number(navUsd.toFixed(2)),
      cashUsd: Number(cashUsdNow.toFixed(2)),
      positionsUsd: Number(book.grossUsd.toFixed(2)),
      openPositions: book.count,
      unrealisedUsd: Number(book.unrealisedUsd.toFixed(2)),
      updatedAt: A.FV.serverTimestamp(),
    }, { merge: true });
  } catch (e) { /* a missed NAV mark must not stop the cycle */ }
  await A.col(A.COL.control).doc("control").set({
    highWaterMarkUsd: hwm,
    startOfDayNavUsd: (ctrl.startOfDayNavDate === today) ? startOfDayNav : navUsd,
    startOfDayNavDate: today,
  }, { merge: true });

  const managementWorkset = W.buildManagementWorkset(rp.symbols, allPositions);
  const evaluatedHoldingSymbols = new Set();
  const unpricedHoldings = [];
  const orphanSymbols = allPositions
    .filter((p) => p && p.open && p.symbol && !tradeSymbols.includes(p.symbol))
    .map((p) => p.symbol);
  for (const work of managementWorkset) {
    const sym = work.symbol;
    const meta = tierBySymbol[sym] || {};
    const bars = panel[sym] || [];
    const last = bars[bars.length - 1];
    const position = work.position || positionBySymbol[sym] || null;
    if (!last) {
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
      const heldDays = (Date.now() - Date.parse(position.openedAt)) / 864e5;
      const markProv = marketProvenanceBySymbol[sym];
      if (lastPriceBySymbol[sym] != null && quality[sym] && quality[sym].tradable
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
      const ex = S.exitSignal(ranks[sym], heldDays, cfg, {
        mark: last.c, entry: entryUsd, peak, earningsInDays, intelligencePolicy,
      });
      evaluatedHoldingSymbols.add(sym);
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
            exits.push({ symbol: sym, reason: ex.reason, kind: ex.kind,
              urgent: !!ex.urgent, blocked: armed.blocked });
            continue;
          }
        }
        exits.push({
          symbol: sym, reason: ex.reason, kind: ex.kind, urgent: !!ex.urgent,
          pnlPct: ex.pnlPct, rank: ranks[sym], heldDays: Number(heldDays.toFixed(2)),
        });
      }
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
    const breach = !!(z && S.entrySignal(rank, z.z, cfg, historyCtx[sym]).fire);
    const firingPolicies = V.VARIANTS.map((v) => {
      const policy = V.configFor(v.id);
      const sc = signalContexts[policy.signalWindow || 12] || liveSignalContext;
      const policyZ = sc.zBySymbol[sym], policyRank = sc.ranks[sym];
      return policyZ && S.entrySignal(policyRank, policyZ.z, policy, historyCtx[sym]).fire
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
      intelligence,
      historyContext: historyCtx[sym] || null,
      reversion: reversionBySymbol[sym] || null,
      turnoverPctile: turnoverPctile[sym] ?? null,
    };
    const evalRes = S.evaluateCandidate({ ...baseDecisionInput, rank, zStat: z, cfg });
    const frozenDecision = DF.evaluateFrozenPolicies({ baseInput: baseDecisionInput,
      signalContexts: Object.fromEntries(Object.entries(signalContexts).map(([window, value]) =>
        [window, { ranks: value.ranks, zBySymbol: value.zBySymbol }])) });

    const card = {
      cycleId, symbol: sym, company: meta.company || sym,
      sector: S.sectorOf(sym), tier: meta.tier || "trade",
      lastPrice: last.c, lastBarAt: last.t,
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
      frozenDecision,
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
      cycleId, symbol: sym, kind: evalRes.pass ? "propose" : "no_trade",
      noTradeReasons: evalRes.blockedBy, firstBlock: evalRes.firstBlock,
      rank, z: evalRes.z, cause, cost: evalRes.cost, sizing: evalRes.sizing,
      intelligenceDossierHash: intelligence && intelligence.dossierHash || null,
      intelligencePolicy: evalRes.intelligencePolicy || null,
      frozenDecision,
      temporalContextHash: intelligence && intelligence.temporalContext
        && intelligence.temporalContext.contextHash || null,
      strategyVersion: strategy.version, decisionAtMs,
      ...A.envelope({ created_by: FN_NAME }),
    }, { merge: true });

    if (evidenceTrigger) {
      await DF.recordObservation({ experimentHash: shadowExperiment.experimentHash,
        cycleId, symbol: sym, decisionAtMs, decisionDate: session.date,
        initialPrice: last.c, marketProvenance: marketProvenanceBySymbol[sym],
        counterfactuals: frozenDecision, sector: S.sectorOf(sym), regime: reg });
    }

    if (evalRes.pass && entryControl.pass) {
      proposalQueue.push({ sym, last, evalRes, cause, causeDetail, rank, intelligence });
    }
  }

  /* Portfolio decisions are a batch problem. Rank all passing ideas by their
     conservative net utility, then admit them one by one against the evolving
     book. Iterating ticker order would make the alphabet an allocation model. */
  proposalQueue.sort((a, b) =>
    Number(b.evalRes.cost.calibratedNetLowerBoundBps || -Infinity)
      - Number(a.evalRes.cost.calibratedNetLowerBoundBps || -Infinity)
    || (Number(b.evalRes.cost.expectedGrossBps) - Number(b.evalRes.cost.requiredBps))
      - (Number(a.evalRes.cost.expectedGrossBps) - Number(a.evalRes.cost.requiredBps))
    || a.rank - b.rank || a.sym.localeCompare(b.sym));

  let proposalCashUsd = cashUsdNow;
  for (let queueIndex = 0; queueIndex < proposalQueue.length; queueIndex += 1) {
    const q = proposalQueue[queueIndex];
    const { sym, last, evalRes, cause, causeDetail, intelligence } = q;
    if (pendingOrderSymbols.has(sym)) continue;
    try {
      const hc = historyCtx[sym] || {};
      const sizing = R.positionSizeUsd({ navUsd, atrPct: hc.atrPct,
        expectedShortfall5dPct: hc.expectedShortfall5dPct,
        overnightGapEsPct: hc.overnightGapEsPct,
        signalScaler: evalRes.sizing.combined,
        cfg: { ...strategy, parameters: cfg } });
      const dynamicCorrelations = {};
      for (const held of book.rows) {
        dynamicCorrelations[held.symbol] = T.pairwiseCorrelation(
          dailySeriesBySymbol[sym] || [], dailySeriesBySymbol[held.symbol] || [], Date.now(), {
            provenanceA: dailyProvenanceBySymbol[sym] || null,
            provenanceB: dailyProvenanceBySymbol[held.symbol] || null,
            requireProvenance: true,
          });
      }
      const add = R.checkAdd({ symbol: sym, sector: S.sectorOf(sym),
        proposedUsd: sizing.usd, book, navUsd, cashUsd: proposalCashUsd,
        cfg: { ...strategy, parameters: cfg }, dynamicCorrelations });
      const permittedUsd = add.allow ? sizing.usd : (add.allowTrimmed ? add.permittedUsd : 0);
      const qty = Math.max(0, Math.floor(permittedUsd / last.c));
      if (qty <= 0) {
        await A.col(A.COL.decisions).doc(`${cycleId}_${sym}_notrade_portfolio`).set({
          cycleId, symbol: sym, kind: "no_trade", stage: "portfolio",
          blockedBy: add.blockedBy, reason: add.firstBlock || "no feasible risk-sized notional",
          cluster: add.cluster, checks: add.checks, sizing,
          bookCount: book.count, bookGrossPct: book.grossPct,
          strategyVersion: strategy.version, ...A.envelope({ created_by: FN_NAME }),
        }, { merge: true });
        portfolioBlocks.push({ symbol: sym, blockedBy: add.blockedBy, reason: add.firstBlock });
        continue;
      }
      const provenance = marketProvenanceBySymbol[sym];
      const decisionAtMs = Date.now();
      const executionCostContext = M.executionCostContext({
        advUsd: (metaBySymbol[sym] || {}).advUsd || 0,
        grade: quality[sym].grade, wideSpreadWindow: session.wideSpreadWindow,
        vixNorm: reg.vixNorm, measuredAtMs: decisionAtMs });
      const slip = executionCostContext.slippageBps;
      const o = await L.proposeOrder({
        ...policyIdentity, accountId, symbol: sym, side: "buy",
        decisionId: `${cycleId}_${sym}`, qty, refPriceUsd: last.c, slippageBps: slip,
        executionCostContext,
        sizing: { ...sizing, signal: evalRes.sizing }, gates: evalRes.gates, cause,
        variantId: liveVariant ? liveVariant.id : "A", cost: evalRes.cost,
        portfolioRisk: { cluster: add.cluster, checks: add.checks,
          dynamicCorrelation: add.dynamicCorrelation,
          dynamicCorrelationPairs: dynamicCorrelations,
          bookGrossPctBefore: book.grossPct, cashUsdBefore: proposalCashUsd },
        decisionContext: {
          crossSectionRank: q.rank,
          queueRank: queueIndex + 1,
          eligibleBatchSize: proposalQueue.length,
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
      });
      if (o.blocked) { portfolioBlocks.push({ symbol: sym, reason: o.blocked }); continue; }
      pendingOrderSymbols.add(sym);
      decisions.push({ symbol: sym, orderId: o.orderId, qty, refPriceUsd: o.refPriceUsd,
        trimmed: !add.allow, cluster: add.cluster, variantId: liveVariant ? liveVariant.id : "A" });
      const takenUsd = qty * last.c;
      proposalCashUsd -= takenUsd;
      book.count += 1; book.grossUsd += takenUsd;
      book.grossPct = navUsd > 0 ? 100 * book.grossUsd / navUsd : 0;
      const sec = S.sectorOf(sym), pct = navUsd > 0 ? 100 * takenUsd / navUsd : 0;
      book.bySectorPct[sec] = (book.bySectorPct[sec] || 0) + pct;
      book.byClusterPct[add.cluster] = (book.byClusterPct[add.cluster] || 0) + pct;
      book.rows.push({ symbol: sym, qty, entry: last.c, mark: last.c,
        valueUsd: takenUsd, sector: sec, cluster: add.cluster, pnlPct: 0, marked: true });
    } catch (e) {
      console.error("propose failed", redact({ symbol: sym, error: e.message }));
    }
  }

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
    /* AUTO-APPROVAL. There was no path of any kind: every buy waited on a
       human click, so the system could sell by itself but never buy by itself.
       That is not autonomy, it is an elaborate alarm clock.

       Only at limited_auto, only within caps deliberately tighter than the
       ordinary portfolio limits, and never for a marginal pass or a no-cause
       trade. The operator's queue then holds the exceptions worth looking at
       rather than every routine order. */
    const stageNow = ctrl.mode || "research";
    if (stageNow === "limited_auto" && ctrl.dryRun === false && !ctrl.killSwitch && entryControl.pass) {
      const todayAuto = await A.col(A.COL.orders)
        .where("accountId", "==", accountId)
        .where("autoApprovedDate", "==", today).limit(50).get();
      let dayCount = todayAuto.size;

      const proposedSnap = await A.col(A.COL.orders)
        .where("accountId", "==", accountId).where("status", "==", "proposed").limit(25).get();

      for (const d of proposedSnap.docs) {
        const o = d.data();
        const verdict = LD.autoApproval(o, {
          stage: stageNow, book, navUsd, cfg: strategy, dayCount,
        });
        if (!verdict.approve) {
          settled.skipped.push({ orderId: o.orderId, why: "left for you: " + verdict.detail });
          continue;
        }
        try {
          await L.approveOrder(o.orderId, "auto");
          await d.ref.set({ autoApproved: true, autoApprovedDate: today,
                            autoApprovalDetail: verdict.detail }, { merge: true });
          dayCount += 1;
          settled.autoApproved.push({ orderId: o.orderId, symbol: o.symbol, detail: verdict.detail });
        } catch (e) { settled.skipped.push({ orderId: o.orderId, why: String(e.message).slice(0, 120) }); }
      }
    }

    const approvedSnap = await A.col(A.COL.orders)
      .where("accountId", "==", accountId).where("status", "==", "approved").limit(50).get();

    for (const d of approvedSnap.docs) {
      const o = d.data();
      if (!entryControl.pass) {
        settled.skipped.push({ orderId: o.orderId, why: `entry safety closed: ${entryControl.reason}` });
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
        if (!f.duplicate) settled.filled.push({ orderId: o.orderId, symbol: o.symbol, at: elig.barOpenAt });
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
        .where("accountId", "==", accountId).where("status", "==", "approved").limit(50).get();
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
  }

  /* ── SHADOW HARNESS ──────────────────────────────────────────────────
     Each frozen arm runs a complete feasible paper portfolio on the union of
     opportunities. Its daily marked return is an experiment-bound record;
     live paper outcomes never contaminate this counterfactual dataset. */
  let shadow = null, allocation = null, calibration = null;
  try {
    const observationFeedback = await DF.updateObservations({
      experimentHash: shadowExperiment.experimentHash, session,
      lastPrice: lastPriceBySymbol, marketProvenanceBySymbol,
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
        const proposedLock = { experimentHash: shadowExperiment.experimentHash,
          leaderId: alloc.leaderId, ...dev.confirmation, lockedAtMs: Date.now() };
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
          && storedCalibration.holdoutLock.leaderId === holdoutLock.leaderId
          && storedCalibration.holdoutLock.dataThroughDate === holdoutLock.dataThroughDate;
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

    const closedReal = await A.col(A.COL.trades)
      .where("accountId", "==", accountId).get();
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
      allocation: allocation || {},
      calibration: leaderCalibration || { calibrated: calibration != null, pass: false },
      costMeter: await L.costMeter(accountId).catch(() => ({})),
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
      const decision = LD.decideStage({
        current: fresh.mode || "research", gates,
        operatorCeiling: fresh.operatorCeiling || strategy.operatorCeiling || "approval",
        operatorHold: fresh.operatorHold === true,
        promotionStreak: Number(fresh.ladderStreak) || 0,
        killSwitch: !!fresh.killSwitch,
      });
      const patch = { ladderStreak: decision.streak || 0 };
      if (decision.changed) Object.assign(patch, {
        mode: decision.stage, stageChangedAt: A.FV.serverTimestamp(),
        stageChangeReason: decision.reason, stageChangedBy: "ladder",
      });
      tx.set(controlRef, patch, { merge: true });
      return { decision, gates, fresh };
    });
    const { decision, gates, fresh } = applied;

    if (decision.changed) {
      await A.col(A.COL.audit).add({
        action: "stage_change", from: fresh.mode || "research", to: decision.stage,
        direction: decision.direction, reason: decision.reason,
        at: A.FV.serverTimestamp(), ...A.envelope({ created_by: FN_NAME }),
      });
    }

    ladder = {
      stage: decision.stage, previous: fresh.mode || "research",
      changed: decision.changed, direction: decision.direction, reason: decision.reason,
      earned: LD.highestEarnedStage(gates),
      ceiling: fresh.operatorCeiling || strategy.operatorCeiling || "approval",
      controlDryRun: fresh.dryRun !== false,
      gates, notes: LD.describe(decision.stage, gates, decision),
    };
  } catch (e) { ladder = { error: String(e.message).slice(0, 200) }; }

  /* Re-read the book AFTER settlement. The snapshot taken at the top of the
     cycle is what the entry gate must reason about, but reporting it would
     show the operator a book that predates this cycle's own fills — zero
     positions on the very cycle that opened one. */
  let finalBook = book, finalNav = navUsd, positionCoverage = null;
  try {
    const ps = await A.col(A.COL.positions).where("accountId", "==", accountId).get();
    const rows = []; ps.forEach((d) => rows.push(d.data()));
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
    const marked2 = R.markedBook(rows, lastPriceBySymbol, S.sectorOf, {
      cash: b2.usd[L.ACCT.CASH] || 0, reserved: b2.usd[L.ACCT.RESERVED] || 0,
    });
    finalNav = Math.max(1, marked2.navUsd);
    finalBook = marked2.book;
    finalBook.navUsd = finalNav;
  } catch (e) { finalBook = book; finalBook.navUsd = navUsd; }
  if (finalBook.navUsd == null) finalBook.navUsd = navUsd;

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
                  skipped: settled.skipped.length, error: settled.error || null },
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
    ranked, breaches: candidates.length,
    proposals: decisions.length, exitSignals: exits.length,
    modelCalls,
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
      grossPct: finalBook.grossPct,
      unrealisedPct: finalBook.unrealisedPct,
      drawdownPct: Number(riskState.drawdownPct.toFixed(2)),
      dayPnlPct: Number(riskState.dayPnlPct.toFixed(2)),
      halted: breakers.halted,
      breakers: breakers.breakers,
      topClusters: Object.entries(finalBook.byClusterPct).sort((a, b) => b[1] - a[1]).slice(0, 5),
      markCoveragePct: finalBook.markCoveragePct,
      portfolioBlocks: portfolioBlocks.length,
      notes: R.describe(finalBook, breakers, strategy),
    },
    provider: marketIdentity.provider,
    feed: marketIdentity.feed, adjustment: marketIdentity.adjustment,
    experimentHash: shadowExperiment.experimentHash,
    universeHash, strategyHash, variantsHash,
    providerNote: fetchMeta.note || fetchMeta.error || null,
    regime: { vixNorm: Number.isFinite(reg.vixNorm) ? Number(reg.vixNorm.toFixed(2)) : null,
      cor3m: reg.cor3m, stale: reg.stale, vixHealthy: reg.vixHealthy, corHealthy: reg.corHealthy },
    session: { phase: session.phase, date: session.date },
    mode: (ladder && ladder.stage) || ctrl.mode || "research",
    dryRun: ladder && ladder.controlDryRun != null ? ladder.controlDryRun : ctrl.dryRun !== false,
    ladder,
    elapsedMs: Date.now() - startedAt,
  };

  await runRef.set({
    ...summary, finishedAt: A.FV.serverTimestamp(), status: "complete",
  }, { merge: true });

  await A.col(A.COL.control).doc("control").set({
    lastCycleSummary: summary, lastCycleFinishedAt: A.FV.serverTimestamp(),
  }, { merge: true });

  return summary;
}

/* ── evidence sweep (slower clock) ─────────────────────────────────────── */
async function runEvidence(jobId) {
  const startedAt = Date.now();
  const ctrl = await controlDoc();
  const universe = await loadUniverse(ctrl.universeVersion);
  const strategy = await loadStrategy(ctrl.strategyVersion);
  const earningsWindows = await B.readEarnings().catch(() => ({}));
  const accountId = ctrl.accountId || "paper-1";
  const positionSnap = await A.col(A.COL.positions).where("accountId", "==", accountId)
    .where("open", "==", true).limit(100).get();
  const positions = positionSnap.docs.map((d) => d.data());
  let recentCandidates = [];
  try {
    const candidateSnap = await A.col(A.COL.candidates).orderBy("updated_at", "desc").limit(120).get();
    recentCandidates = candidateSnap.docs.map((d) => d.data());
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
  for (const focusRow of selected) {
    const t = bySymbol[focusRow.symbol] || { symbol: focusRow.symbol };
    const asOfMs = Date.now();
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
  }
  const nextCursor = focus.length ? (cursor + selected.length) % focus.length : 0;
  const summary = { jobId, kind: "evidence", focusCount: focus.length,
    selected: selected.map((x) => x.symbol), swept: results.length, nextCursor,
    publicSourceMode: true, results, elapsedMs: Date.now() - startedAt };
  await A.col(A.COL.control).doc("control").set({ intelligenceCursor: nextCursor,
    intelligenceFocus: focus.map((x) => x.symbol), lastIntelligenceSummary: summary,
    lastIntelligenceAt: A.FV.serverTimestamp() }, { merge: true });
  await A.col(A.COL.runs).doc(jobId).set({
    ...summary, finishedAt: A.FV.serverTimestamp(), status: "complete",
    ...A.envelope({ created_by: FN_NAME }),
  }, { merge: true });
  return summary;
}

/* ── handler ───────────────────────────────────────────────────────────── */
exports.handler = async (event) => {
  let body = {};
  try { body = JSON.parse(event.body || "{}"); } catch {}
  const { jobId, task, nonce } = body;

  // A background function is a public URL. Only a nonce minted by the
  // scheduler for THIS job and THIS function is accepted.
  const claim = verifyWorkerNonce(nonce, FN_NAME);
  if (!claim || claim.jobId !== jobId) {
    console.warn("investorCycle: rejected unauthenticated invocation");
    return { statusCode: 403, body: JSON.stringify({ error: "invalid or missing worker nonce" }) };
  }

  if (!jobId || !["cycle", "guard", "evidence"].includes(task)) {
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
      return { claim: true, accountLease: !!accountCycleLeaseRef };
    });
    if (lease.duplicate) {
      return { statusCode: 200, body: JSON.stringify({ ok: true, duplicate: true, jobId }) };
    }
    if (!lease.claim) {
      return { statusCode: lease.inFlight ? 202 : 409,
        body: JSON.stringify({ ok: false, jobId, reason: lease.reason || "already running" }) };
    }

    const out = task === "evidence" ? await runEvidence(jobId)
      : task === "guard" ? await PG.runGuard(jobId)
        : await runCycle(jobId);

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
    console.log("investorCycle done", JSON.stringify(redact(out)));
    return { statusCode: 200, body: JSON.stringify({ ok: true, ...out }) };
  } catch (e) {
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
exports.applyOverfitGuard = applyOverfitGuard;
exports.attentionZ = attentionZ;
/* Exported for deterministic invariant tests. These are pure helpers; the
   worker entry point remains runCycle. */
exports.advFor = advFor;
exports.barTimeframeMs = barTimeframeMs;
exports.WORKER_LEASE_TTL_MS = WORKER_LEASE_TTL_MS;
exports.GUARD_LEASE_TTL_MS = GUARD_LEASE_TTL_MS;
