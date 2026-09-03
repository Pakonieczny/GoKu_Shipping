/*  netlify/functions/investorApi.js  (v1.0)
 *  ---------------------------------------------------------------------------
 *  Investor_AI — the console API. Synchronous HTTP, NOT scheduled.
 *
 *  Split from investorKick.js because a scheduled Netlify function cannot be
 *  reached over HTTP: Netlify answers a direct request with a bare 403 and no
 *  body. Putting a console on a cron'd function makes it unreachable, which is
 *  precisely the failure the legacy Google Ads autopilot hit.
 *
 *  Every action requires a verified operator. CORS is an exact-origin
 *  allowlist and is treated as a browser mechanic, never as authorization.
 *  Nothing here fetches a URL supplied by the browser, and no action can
 *  reach a real broker because none exists.
 * ---------------------------------------------------------------------------
 */

"use strict";

const A = require("./_investorAdmin");
const AUTH = require("./_investorAuth");
const M = require("./_investorMarket");
const H = require("./_investorHistory");
const R = require("./_investorRisk");
const S = require("./_investorSignal");
const L = require("./_investorLedger");
const E = require("./_investorEvidence");
const IS = require("./_investorIntelligenceSources");
const PR = require("./_investorPlainReason");
const PA = require("./_investorPatience");
const I = require("./_investorIntelligence");
/* SH/AL/V were used by the "learning" action and never imported — the third
   occurrence of this exact bug class. Every click on the Learning tab was a
   500. The end-to-end harness now loads this module and calls the action. */
const SH = require("./_investorShadow");
const AL = require("./_investorAllocator");
const V = require("./_investorVariants");
const LD = require("./_investorLadder");
const O = require("./_investorOpenai");
const B = require("./_investorBootstrap");
const C = require("./_investorCalibration");
const ST = require("./_investorStrategy");
const CA = require("./_investorCorporateActions");
const STATE = require("./_investorState");
const SOAK = require("./_investorSoak");

const MAX_BODY = 64 * 1024;
const ISO_DATE = /^\d{4}-\d{2}-\d{2}$/;
const SYMBOL = /^[A-Z][A-Z0-9.-]{0,9}$/;

function commitId() { return process.env.COMMIT_REF || process.env.DEPLOY_ID || "local"; }
function validEpoch(ctrl) {
  const e = ctrl.safetyEpoch || {};
  return ctrl.fixturesPass === true && ctrl.fixturesCommit === commitId()
    && e.commit === commitId()
    && ["accountId", "strategyVersion", "universeVersion", "strategyHash", "universeHash", "variantsHash"]
      .every((k) => e[k] && e[k] === ctrl[k]);
}

async function ctrlDoc() {
  const s = await A.col(A.COL.control).doc("control").get();
  return s.exists ? s.data() : {};
}

/* Read the exact frozen strategy selected by control. Falling back to the
   bundled copy keeps a fresh install usable, while a valid stored version is
   what the cycle and dashboard must both describe. */
async function activeStrategy(ctrl = {}) {
  const version = ctrl.strategyVersion || ST.version;
  try {
    const s = await A.col(A.COL.strategies).doc(version).get();
    const value = s.exists ? s.data() : null;
    if (value && value.parameters) return value;
  } catch {}
  return ST;
}

function isoTime(value) {
  if (!value) return null;
  try {
    const d = value.toDate ? value.toDate() : new Date(value);
    return Number.isFinite(d.getTime()) ? d.toISOString() : null;
  } catch { return null; }
}

function closedTradeForUi(value) {
  const x = value || {};
  const numeric = (n) => n !== null && n !== undefined && n !== ""
    && Number.isFinite(Number(n)) ? Number(n) : null;
  const cents = (n) => numeric(n) === null ? null : L.fromCents(numeric(n));
  const entryPriceUsd = numeric(x.entryPriceUsd) ?? numeric(x.entryFillUsd)
    ?? numeric(x.entryUsd) ?? numeric(x.entryPrice) ?? numeric(x.fillPriceUsd)
    ?? cents(x.entryPriceCents) ?? numeric(x.entryBarOpenUsd);
  /* The ledger writes netRealizedCents / exitFillUsd. Reading only the older
     aliases left every closed trade in the console with a blank P&L and no
     sell price, which is exactly the history the operator asked to see. */
  const realisedPnlUsd = numeric(x.realisedPnlUsd) ?? numeric(x.realizedPnlUsd)
    ?? numeric(x.netPnlUsd) ?? cents(x.netPnlCents) ?? cents(x.netRealizedCents);
  const exitPriceUsd = numeric(x.exitPriceUsd) ?? numeric(x.exitFillUsd)
    ?? numeric(x.exitUsd) ?? cents(x.exitPriceCents);
  /* The five-word labels a person actually reads. Computed here, from the
     recorded decision fields, so the console never has to infer a rationale
     and every surface shows the same words for the same rule. */
  /* The ledger writes `heldDays` on a closed trade; `heldTradingDays` is the
     open-position field. Read both so history and holdings agree. */
  const heldDays = numeric(x.heldDays) ?? numeric(x.heldTradingDays);
  return { ...x, entryPriceUsd, realisedPnlUsd, exitPriceUsd,
    qty: numeric(x.qty),
    buyReason: PR.buyReason(x),
    sellReason: PR.sellReason({ ...x, open: false }),
    heldTradingDays: heldDays,
    heldText: heldDays != null ? PR.heldText(heldDays) : null,
    openedAt: isoTime(x.openedAt || x.entryAt || x.openedAtMs),
    closedAt: isoTime(x.closedAt || x.exitAt || x.closedAtMs),
    exitKind: x.exitKind || null,
    manualClose: x.manualClose === true || x.exitKind === "manual"
      || x.closeReason === "manual_operator_sell",
    exitReason: x.exitReason || x.closeReason || x.reason || null };
}

async function closeEntryQueue(accountId, reason, operator) {
  let cancelled = 0, released = 0;
  const failures = [];
  const proposed = await A.col(A.COL.orders)
    .where("accountId", "==", accountId).where("status", "==", "proposed").get();
  for (const d of proposed.docs) {
    const o = d.data();
    try {
      const result = await L.rejectOrder(o.orderId, reason, operator || "operator");
      if (!result.noop) cancelled += 1;
    } catch (e) { failures.push({ orderId: o.orderId, phase: "reject" }); }
  }
  const approved = await A.col(A.COL.orders)
    .where("accountId", "==", accountId).where("status", "==", "approved").get();
  for (const d of approved.docs) {
    const o = d.data();
    try {
      const result = await L.releaseOrder(o.orderId, reason);
      if (!result.noop) released += 1;
    } catch (e) { failures.push({ orderId: o.orderId, phase: "release" }); }
  }
  return { cancelledProposals: cancelled, releasedApproved: released,
    failures: failures.slice(0, 20) };
}

/* Disabling the paper learner must also disarm the orders it created.  These
   queries deliberately use the already-required account/status indexes and
   filter the paper-only tag in memory, avoiding a new deployment-time index
   dependency while still draining every active row (not an arbitrary page). */
async function closePaperLearningQueue(accountId, reason, operator) {
  let cancelled = 0, released = 0;
  const failures = [];
  const proposed = await A.col(A.COL.orders)
    .where("accountId", "==", accountId).where("status", "==", "proposed").get();
  for (const d of proposed.docs) {
    const o = d.data();
    if (o.paperLearningOnly !== true) continue;
    try {
      const result = await L.rejectOrder(o.orderId, reason, operator || "operator");
      if (!result.noop) cancelled += 1;
    } catch (e) { failures.push({ orderId: o.orderId, phase: "reject" }); }
  }
  const approved = await A.col(A.COL.orders)
    .where("accountId", "==", accountId).where("status", "==", "approved").get();
  for (const d of approved.docs) {
    const o = d.data();
    if (o.paperLearningOnly !== true) continue;
    try {
      const result = await L.releaseOrder(o.orderId, reason);
      if (!result.noop) released += 1;
    } catch (e) { failures.push({ orderId: o.orderId, phase: "release" }); }
  }
  return { cancelledPaperProposals: cancelled, releasedPaperApproved: released,
    failures: failures.slice(0, 20) };
}

function latestCycleId(rows) {
  const ids = [...new Set(rows.map((r) => r.cycleId).filter(Boolean))].sort();
  return ids[ids.length - 1] || null;
}

function progressForUi(value) {
  if (!value || typeof value !== "object") return null;
  const numberOrNull = (x) => x == null || x === ""
    ? null : (Number.isFinite(Number(x)) ? Number(x) : null);
  const total = numberOrNull(value.total);
  const completed = numberOrNull(value.completed);
  return {
    phase: String(value.phase || "working"),
    label: String(value.label || "Working"),
    detail: value.detail == null ? null : String(value.detail).slice(0, 240),
    pct: Math.max(0, Math.min(100, numberOrNull(value.pct) ?? 0)),
    completed, total,
    remaining: numberOrNull(value.remaining) ?? (total != null && completed != null
      ? Math.max(0, total - completed) : null),
    currentItem: value.currentItem == null ? null : String(value.currentItem).slice(0, 80),
    updatedAtMs: numberOrNull(value.updatedAtMs),
  };
}

/* A cycle can be terminal in Firestore yet still have skipped most of the
   ranked universe (the retired account-breaker shortcut did exactly that).
   Keep a separate semantic completion check so the console never presents a
   one-holding trace as a completed full scan. */
function liveEvaluatedCount(live) {
  const c = live && live.counters || {};
  if (Number.isFinite(Number(c.evaluated))) return Math.max(0, Number(c.evaluated));
  return ["no_signal", "blocked", "passed", "held", "unpriced",
    "exit_armed", "exit_blocked", "company_error"]
    .reduce((sum, key) => sum + Math.max(0, Number(c[key]) || 0), 0);
}

function fullScanIntegrity(run) {
  const r = run || {};
  if (r.kind !== "cycle") return { pass: r.status === "complete", evaluated: null, expected: null };
  const evaluated = liveEvaluatedCount(r.live);
  const expected = Number.isFinite(Number(r.ranked)) ? Math.max(0, Number(r.ranked))
    : (r.progress && Number.isFinite(Number(r.progress.total))
      ? Math.max(0, Number(r.progress.total)) : null);
  const hasLive = !!(r.live && r.live.counters);
  const covered = !hasLive || expected == null || expected === 0 || evaluated >= expected;
  /* THE HONEST DENOMINATOR. `expected` above is what the scan managed to rank,
     so a fetch that returned a third of the roster still measured itself as
     fully covered and reported a clean scan. The roster the scan set out to
     cover is recorded on the run itself; coverage is measured against that,
     and a materially short scan is named rather than passed over. */
  const cov = r.marketCoverage || {};
  const roster = Number.isFinite(Number(cov.roster)) ? Number(cov.roster)
    : (Number.isFinite(Number(r.symbolCount)) ? Number(r.symbolCount) : null);
  const priced = Number.isFinite(Number(cov.priced)) ? Number(cov.priced) : null;
  const coveragePct = roster > 0 && priced != null
    ? Number(((priced / roster) * 100).toFixed(1)) : null;
  return { pass: r.status === "complete" && covered, evaluated, expected,
    roster, priced, coveragePct,
    /* Below this the scan looked at so little of the roster that reporting it
       as a full scan would be misleading. */
    rosterCovered: coveragePct == null ? null : coveragePct >= 80,
    coverageReason: cov.error || null,
    failedSymbols: cov.failedSample || [], missingSymbols: cov.missingSample || [] };
}

/* ── actions ───────────────────────────────────────────────────────────── */
let quotesCache = { at: 0, quotes: {}, asOf: null };
const F = require("./_investorFetch");

/* ── RESPONSE SIZE ──────────────────────────────────────────────────────────
 * The platform rejects a handler response over 6 MB with
 * "RequestEntityTooLarge", which reaches the browser as an opaque 502 — the
 * console cannot even log in, and nothing in the page says why. The dashboard
 * carries one card per evaluated company, so restoring genuine full-universe
 * scans (277 companies instead of the handful a halted account used to
 * evaluate) multiplied that section by an order of magnitude and crossed the
 * limit. Two defences, in order:
 *
 *   1. Do not ship what nobody reads. Every field below is absent from the
 *      console; the stored candidate record keeps them all, and the per-symbol
 *      `candidate` action still returns the complete card.
 *   2. Degrade, never fail. If the response is still too large it is trimmed
 *      in a defined order and says so, so the desk stays usable.
 */
const DASHBOARD_CANDIDATE_OMIT = Object.freeze(["frozenDecision", "historyNotes",
  "manifestCoverage", "decisionManifestCoverage", "decisionManifestHash",
  "decisionManifestValid", "sigmaBlend", "zShortOnly", "sectorTailFraction",
  "marketProvider", "marketFeed", "buildCommit", "unionEvidenceTrigger"]);

/** The candidate card as the console reads it. Pure. */
function dashboardCandidateRow(card) {
  if (!card || typeof card !== "object") return card;
  const out = {};
  for (const [k, v] of Object.entries(card)) {
    if (!DASHBOARD_CANDIDATE_OMIT.includes(k)) out[k] = v;
  }
  return out;
}

/** The smallest row that still supports the ranked list, the block tally and
 *  the "which companies were ranked" views. Used only when a full payload
 *  would exceed the platform limit. Pure. */
function minimalCandidateRow(card) {
  if (!card || typeof card !== "object") return card;
  const { symbol, company, sector, tier, rank, z, cumResidualBps, cause, pass,
    blockedBy, firstBlock, breach, lastPrice, currentTradingDay, cycleId,
    dataSufficiency, strict } = card;
  return { symbol, company, sector, tier, rank, z, cumResidualBps, cause, pass,
    blockedBy, firstBlock, breach, lastPrice, currentTradingDay, cycleId,
    dataSufficiency, strict, detailTrimmed: true };
}

/* Leaves room for headers and the platform's own framing under the 6 MB cap. */
const RESPONSE_SOFT_LIMIT_BYTES = 4_500_000;
const RESPONSE_HARD_LIMIT_BYTES = 5_800_000;

function payloadBytes(value) {
  try { return Buffer.byteLength(JSON.stringify(value)); }
  catch { return Number.POSITIVE_INFINITY; }
}

/**
 * Trim a dashboard payload until it fits, in a defined order, and record what
 * was dropped. Returns the same object when it already fits. Pure apart from
 * mutating the copy it returns.
 */
function boundDashboardPayload(body, limit = RESPONSE_SOFT_LIMIT_BYTES) {
  let bytes = payloadBytes(body);
  if (bytes <= limit) return body;
  const out = { ...body };
  const applied = [];
  const steps = [
    ["candidate detail beyond the first 60 by rank", () => {
      const rows = Array.isArray(out.candidates) ? out.candidates : [];
      if (rows.length <= 60) return false;
      out.candidates = rows.map((c, i) => (i < 60 ? c : minimalCandidateRow(c)));
      return true;
    }],
    ["all candidate detail", () => {
      const rows = Array.isArray(out.candidates) ? out.candidates : [];
      if (!rows.length || rows.every((c) => c && c.detailTrimmed)) return false;
      out.candidates = rows.map(minimalCandidateRow);
      return true;
    }],
    ["closed-trade history beyond the most recent 25", () => {
      const rows = Array.isArray(out.closedTrades) ? out.closedTrades : [];
      if (rows.length <= 25) return false;
      out.closedTrades = rows.slice(0, 25);
      return true;
    }],
    ["company intelligence dossiers", () => {
      const rows = Array.isArray(out.intelligence) ? out.intelligence : [];
      if (!rows.length) return false;
      out.intelligence = [];
      return true;
    }],
    ["quote coverage detail", () => {
      const rows = Array.isArray(out.quoteCoverage) ? out.quoteCoverage : [];
      if (!rows.length) return false;
      out.quoteCoverage = [];
      return true;
    }],
  ];
  for (const [label, apply] of steps) {
    if (bytes <= limit) break;
    if (!apply()) continue;
    applied.push(label);
    bytes = payloadBytes(out);
  }
  out.payloadTrimmed = { bytes, limit, dropped: applied,
    note: "the scan is complete; this response was trimmed to fit the platform's response limit. "
      + "Open a company for its full record." };
  return out;
}

const ACTIONS = {

  /* The single call the dashboard polls. One round trip, everything on it. */
  async dashboard() {
    const ctrl = await ctrlDoc();
    const operating = STATE.describe(ctrl);
    const accountId = ctrl.accountId || "paper-1";
    const session = M.sessionState(new Date());

    const uMod = require("./_investorUniverse.js");
    const [strategy, candSnap, proposedSnap, approvedSnap, posSnap, tradeSnap,
      regSnap, costSnap, runSnap, jobSnap, universeSnap, intelligenceSnap] = await Promise.all([
      activeStrategy(ctrl),
      A.col(A.COL.candidates).orderBy("updated_at", "desc").limit(500).get(),
      /* Query active states directly. Filtering an unordered 400-row account
         sample in memory eventually starves new active records behind old
         terminal history, which is a silent control-room failure. The exact
         composite indexes ship with this project. */
      A.col(A.COL.orders).where("accountId", "==", accountId)
        .where("status", "==", "proposed").get(),
      A.col(A.COL.orders).where("accountId", "==", accountId)
        .where("status", "==", "approved").get(),
      A.col(A.COL.positions).where("accountId", "==", accountId)
        .where("open", "==", true).get(),
      /* Completed outcomes live in the append-only trades collection. Position
         documents are one mutable row per symbol and cannot represent repeat
         round trips, so using them made the Results page structurally empty. */
      A.col(A.COL.trades).where("accountId", "==", accountId).get(),
      A.col(A.COL.control).doc("regime").get(),
      A.col(A.COL.costs).doc(`openai_${new Date().toISOString().slice(0, 10)}`).get(),
      A.col(A.COL.runs).orderBy("startedAt", "desc").limit(8).get(),
      A.col(A.COL.jobs).orderBy("dispatchedAt", "desc").limit(12).get(),
      A.col(A.COL.universe).doc(ctrl.universeVersion || uMod.version).get(),
      A.col(A.COL.intelligence).get(),
    ]);

    const candidates = [];
    candSnap.forEach((d) => candidates.push(d.data()));
    /* The evidence binding the ladder applies (leader + forward-lock start),
       read once so the strict sample counter agrees with the ladder. */
    let allocationLeaderId = null, allocationSinceMs = null;
    try {
      const allocForEvidence = await A.col(A.COL.control).doc("allocation").get();
      if (allocForEvidence.exists) {
        const a = allocForEvidence.data();
        allocationLeaderId = a.leaderId || null;
        allocationSinceMs = a.forwardLock && Number(a.forwardLock.lockedAtMs) > 0
          ? Number(a.forwardLock.lockedAtMs) : null;
      }
    } catch { /* evidence binding falls back to the baseline */ }
    /* A completed cycle can legitimately rank zero companies and therefore
       write no candidate documents. Deriving "latest" only from candidate
       rows leaves the dashboard pinned to an older non-empty cycle forever.
       Control is the authoritative completion record; candidate timestamps
       are only a compatibility fallback for pre-summary deployments. */
    const completedCycleId = ctrl.lastCycleSummary && ctrl.lastCycleSummary.cycleId;
    const cycleId = completedCycleId || latestCycleId(candidates);
    const current = candidates.filter((c) => c.cycleId === cycleId);

    const orders = [];
    proposedSnap.forEach((d) => orders.push(d.data()));
    approvedSnap.forEach((d) => orders.push(d.data()));
    const positions = [];
    posSnap.forEach((d) => {
      const p = d.data();
      const openedAt = p.openedAt && typeof p.openedAt.toDate === "function"
        ? p.openedAt.toDate() : p.openedAt;
      const heldTradingDays = openedAt ? M.tradingDaysHeld(openedAt, Date.now()) : null;
      positions.push({ ...p,
        openedAt: openedAt instanceof Date ? openedAt.toISOString() : (openedAt || null),
        heldTradingDays,
        buyReason: PR.buyReason(p),
        sellReason: PR.sellReason({ ...p, open: true }),
        heldText: heldTradingDays != null ? PR.heldText(heldTradingDays) : null });
    });
    const closedTrades = []; tradeSnap.forEach((d) => closedTrades.push(closedTradeForUi(d.data())));
    closedTrades.sort((a, b) => String(b.closedAt || "").localeCompare(String(a.closedAt || "")));
    const recentClosedTrades = closedTrades.slice(0, 400);
    const runs = []; runSnap.forEach((d) => {
      const r = d.data();
      const startedAt = isoTime(r.startedAt);
      const finishedAt = isoTime(r.finishedAt);
      runs.push({ jobId: r.jobId || d.id, cycleId: r.cycleId || null, kind: r.kind,
        status: r.status || (finishedAt ? "complete" : "running"),
        breaches: r.breaches, ranked: r.ranked, symbols: r.symbols || r.symbolCount,
        proposals: r.proposals, modelCalls: r.modelCalls, elapsedMs: r.elapsedMs,
        evaluation: r.evaluation || null,
        selected: r.selected || [], swept: r.swept,
        settlement: r.settlement || null, positionCoverage: r.positionCoverage || null,
        diagnostics: r.rankingDiagnostics ? (() => {
          const d = r.rankingDiagnostics, f = d.fetch || {}, failed = (f.chunks && f.chunks.failed) || [];
          return { rosterChecked: d.rosterChecked ?? null, barsReceived: d.barsReceived ?? null,
            researchEligible: d.researchEligible ?? null, admittedToPanel: d.admittedToPanel ?? null,
            tooFewBars: d.excludedBeforePanel && d.excludedBeforePanel.tooFewBars || 0,
            unusableQuality: d.excludedBeforePanel && d.excludedBeforePanel.unusableQuality || 0,
            fetchFailed: failed.length, fetchTotal: f.chunks && f.chunks.total || null,
            fetchError: failed.length ? String(failed[0].error || "").slice(0, 120) : null };
        })() : null,
        error: r.error || null, progress: progressForUi(r.progress),
        live: r.live && typeof r.live === "object" ? {
          counters: r.live.counters || {}, blockedBy: r.live.blockedBy || {},
          recent: Array.isArray(r.live.recent) ? r.live.recent.slice(-40) : [],
          updatedAtMs: Number(r.live.updatedAtMs) || null } : null,
        startedAt, finishedAt,
        startedAtMs: Number(r.startedAtMs) || (startedAt ? Date.parse(startedAt) : null),
        updatedAtMs: Number(r.updatedAtMs) || null });
    });
    const jobs = []; jobSnap.forEach((d) => {
      const j = d.data();
      if (!["cycle", "guard", "evidence"].includes(j.task)) return;
      const dispatchedAt = isoTime(j.dispatchedAt);
      const startedAt = isoTime(j.startedAt);
      const finishedAt = isoTime(j.finishedAt);
      jobs.push({ jobId: j.jobId || d.id, kind: j.task, status: j.status || "queued",
        manual: j.manual === true, attempts: Number(j.attempts) || 0,
        upstream: j.upstream ?? null, error: j.error || null,
        workerLeaseExpiresAt: Number(j.workerLeaseExpiresAt) || Number(j.leaseExpiresAt) || null,
        dispatchedAt, startedAt, finishedAt,
        dispatchedAtMs: dispatchedAt ? Date.parse(dispatchedAt) : null,
        startedAtMs: startedAt ? Date.parse(startedAt) : null });
    });
    const intelligence = intelligenceSnap.docs.map((d) => {
      const x = d.data();
      const policy = I.decisionPolicy({ coverage: x.coverage, events: x.events,
        temporalContext: x.temporalContext, requireTemporalContext: true,
        asOfMs: Date.now(), maxAgeHours: strategy.parameters.intelligenceMaxAgeHours,
        temporalMaxAgeHours: strategy.parameters.temporalMaxAgeHours });
      return { symbol: x.symbol, companyName: x.companyName, sectorPack: x.sectorPack,
        asOf: x.asOf, asOfMs: x.asOfMs, dossierHash: x.dossierHash,
        documentCount: x.documentCount, coverage: x.coverage, policy,
        priceContext: x.priceContext || null,
        identity: x.identity || null, relatedEntities: x.relatedEntities || [],
        temporalContext: x.temporalContext || null,
        events: (x.events || []).slice(0, 6) };
    }).sort((a, b) => a.symbol.localeCompare(b.symbol));

    let balances = null, cost = null;
    try { balances = await L.balances(accountId); } catch {}
    try { cost = await L.costMeter(accountId); } catch {}

    const reg = regSnap.exists ? regSnap.data() : {};
    const openai = costSnap.exists ? costSnap.data() : { usd: 0, calls: 0 };

    /* The dashboard used to read the provider WITHOUT loading the Firestore
       market settings first, so a fresh API process reported the environment
       fallback ("manual · 1440m delay") while the worker was actually on
       Alpaca. The Today card, footer and Health card all quoted that. */
    await M.loadMarketSettings();
    const provider = M.activeProvider();
    const frozenUniverse = universeSnap.exists ? universeSnap.data() : uMod;
    const expectedCandidates = (frozenUniverse.tradeTier || []).length;
    const latestSummary = ctrl.lastCycleSummary || {};
    const summaryMatchesCycle = !!cycleId && latestSummary.cycleId === cycleId;
    const rosterChecked = summaryMatchesCycle
      ? Math.max(0, Number(latestSummary.rosterSymbols ?? latestSummary.symbols) || 0)
      : current.length;
    const rankedInCycle = summaryMatchesCycle
      ? Math.max(0, Number(latestSummary.ranked) || 0)
      : current.length;
    let scanComplete = summaryMatchesCycle && expectedCandidates > 0
      && rosterChecked >= expectedCandidates;
    const decisionSetComplete = summaryMatchesCycle
      ? current.length === rankedInCycle : current.length > 0;
    const paperPreview = ST.paperLearningConfig(strategy.parameters || ST.parameters, ctrl);
    const nowMs = Date.now();
    const jobById = Object.fromEntries(jobs.map((j) => [j.jobId, j]));
    const activeCutoffMs = nowMs - 20 * 60 * 1000;
    const terminalStatuses = new Set(["complete", "dead", "dispatch_failed", "cancelled", "yielded"]);
    /* The run and job documents are two views of one worker. A platform kill
       can mark only the job terminal, while a lost worker can leave the run
       saying "running" forever. Reconcile them before returning either view
       to the console, then attest whether a cycle actually covered its ranked
       workset rather than trusting the word "complete" alone. */
    for (const run of runs) {
      const matchingJob = jobById[run.jobId];
      if (matchingJob && terminalStatuses.has(matchingJob.status)) {
        run.status = matchingJob.status;
        run.error = run.error || matchingJob.error || null;
      }
      const lastSeenMs = run.updatedAtMs || run.startedAtMs || 0;
      if (["queued", "running"].includes(run.status) && lastSeenMs < activeCutoffMs) {
        run.status = "stalled";
        run.error = run.error || "the worker stopped updating before the full scan completed";
      }
      const integrity = fullScanIntegrity(run);
      run.evaluated = integrity.evaluated;
      run.fullScanExpected = integrity.expected;
      run.fullScanComplete = integrity.pass;
      /* Coverage against the managed roster, so a scan that could only price a
         fraction of it says so instead of reporting a clean full scan. */
      run.rosterSize = integrity.roster;
      run.pricedCompanies = integrity.priced;
      run.coveragePct = integrity.coveragePct;
      run.rosterCovered = integrity.rosterCovered;
      run.coverageReason = integrity.coverageReason;
    }
    const summaryRun = runs.find((run) => run.kind === "cycle"
      && (run.jobId === latestSummary.jobId || (latestSummary.cycleId
        && run.cycleId === latestSummary.cycleId)));
    if (summaryRun && summaryRun.fullScanComplete === false) scanComplete = false;
    const activeWork = [];
    for (const run of runs) {
      const matchingJob = jobById[run.jobId];
      const lastSeenMs = run.updatedAtMs || run.startedAtMs || 0;
      const effectiveStatus = matchingJob && terminalStatuses.has(matchingJob.status)
        ? matchingJob.status : run.status;
      if (!["queued", "running"].includes(effectiveStatus) || lastSeenMs < activeCutoffMs) continue;
      activeWork.push({ ...run, status: effectiveStatus,
        dispatchedAt: matchingJob && matchingJob.dispatchedAt || null,
        progress: run.progress || {
          phase: "starting", label: `Starting ${run.kind || "background"} work`,
          detail: "The worker has claimed the job and is loading its first inputs.",
          pct: 2, completed: null, total: null, remaining: null,
          currentItem: null, updatedAtMs: lastSeenMs,
        } });
    }
    for (const job of jobs) {
      if (!["queued", "running"].includes(job.status)) continue;
      if (runs.some((r) => r.jobId === job.jobId)) continue;
      const lastSeenMs = job.startedAtMs || job.dispatchedAtMs || 0;
      if (lastSeenMs < activeCutoffMs) continue;
      /* A queued job older than three minutes while another job of the same
         kind is running is a refused duplicate, not pending work. */
      if (job.status === "queued" && nowMs - lastSeenMs > 3 * 60000
          && activeWork.some((w) => w.kind === job.kind && w.status === "running")) continue;
      activeWork.push({ ...job, progress: {
        phase: job.status === "queued" ? "queued" : "starting",
        label: job.status === "queued"
          ? `Queued ${job.kind} work` : `Starting ${job.kind} work`,
        detail: job.status === "queued"
          ? "Netlify accepted the job; it is waiting for the background worker to claim it."
          : "The background worker has claimed the job and is loading its first inputs.",
        pct: job.status === "queued" ? 0 : 2,
        completed: null, total: null, remaining: null,
        currentItem: null, updatedAtMs: lastSeenMs,
      } });
    }
    const kindPriority = { cycle: 30, evidence: 20, guard: 10 };
    activeWork.sort((a, b) => (kindPriority[b.kind] || 0) - (kindPriority[a.kind] || 0)
      || (b.startedAtMs || b.dispatchedAtMs || 0) - (a.startedAtMs || a.dispatchedAtMs || 0));

    const lastIntelligence = ctrl.lastIntelligenceSummary || {};
    const learningTarget = Math.max(1,
      Number(strategy.preRegistration && strategy.preRegistration.minimumEvents) || 200);
    const readyDossiers = intelligence.filter((x) => x.policy
      && x.policy.fresh === true && x.policy.complete === true).length;
    const lastCycleDispatchAtMs = Number(ctrl.lastCycleAt) || null;
    const cycleSeconds = Number(ctrl.cycleSeconds) || 300;
    const scanEligible = !operating.paused && session.tradingDay === true
      && (session.open === true || session.phase === "premarket");
    /* Two-tier cadence as the scheduler itself sees it, so the console never
       describes a clock the dispatcher is not actually running. */
    const K = require("./investorKick");
    /* Apply the same one-time cadence policy the cron applies. The API may be
       read in the few seconds before the next cron persists the migration;
       it must not keep advertising the retired twice-daily default then. */
    const planMode = K.effectivePlanMode(ctrl);
    const planTimesEt = K.normalizePlanTimes(ctrl.planTimesEt) || [...K.DEFAULT_PLAN_TIMES_ET];
    const kickCtrl = { planMode, planTimesEt, lastPlanKey: ctrl.lastPlanKey || null };
    const guardSeconds = Math.min(600, Math.max(60, Number(ctrl.guardSeconds) || 60));
    const guardSecondsClosed = Math.min(3600, Math.max(300, Number(ctrl.guardSecondsClosed) || 900));
    const lastGuardAtMs = Number(ctrl.lastGuardAt) || null;
    const strikeBarTimeframe = /^(1|5)Min$/.test(String(ctrl.strikeBarTimeframe || "")) ? ctrl.strikeBarTimeframe : "1Min";
    let plansToday = [];
    try {
      const planSnap = await A.col(A.COL.plans).where("accountId", "==", accountId)
        .where("sessionDate", "==", session.date).get();
      planSnap.forEach((d) => {
        const p = d.data();
        plansToday.push({ planId: p.planId, symbol: p.symbol, sector: p.sector, status: p.status,
          armBelowUsd: p.armBelowUsd, floorUsd: p.floorUsd, refPriceUsd: p.refPriceUsd,
          requiredDropPct: p.requiredDropPct, zAtPlan: p.zAtPlan, rankAtPlan: p.rankAtPlan,
          lastSeenUsd: p.lastSeenUsd ?? null, lastSeenAt: p.lastSeenAt || null, distancePct: p.distancePct ?? null,
          armedAtMs: p.armedAtMs || null, refreshedAtMs: p.refreshedAtMs || null, expiresAtMs: p.expiresAtMs || null,
          cycleId: p.cycleId, costAtLevel: p.costAtLevel || null, causeAtPlan: p.causeAtPlan || null,
          lastBlock: p.lastBlock || null, skipReason: p.skipReason || null, skipDetail: p.skipDetail || null,
          struckAtMs: p.struckAtMs || null, strikePriceUsd: p.strikePriceUsd ?? null, orderId: p.orderId || null,
          qty: p.qty ?? null, approval: p.approval || null, dataSufficiency: p.dataSufficiency || null });
      });
      plansToday.sort((a, b) => (a.status === "armed" ? 0 : 1) - (b.status === "armed" ? 0 : 1)
        || (a.requiredDropPct || 0) - (b.requiredDropPct || 0));
    } catch (e) { plansToday = []; }
    const operations = {
      state: activeWork.length ? "working"
        : operating.paused ? "paused"
          : operating.entriesFrozen ? "entry_frozen" : "waiting",
      active: activeWork[0] || null,
      activeAll: activeWork,
      jobs,
      scan: { cycleId, completed: Math.min(rosterChecked, expectedCandidates),
        checked: rosterChecked, total: expectedCandidates,
        remaining: Math.max(0, expectedCandidates - rosterChecked),
        pct: expectedCandidates ? Math.min(100, 100 * rosterChecked / expectedCandidates) : 0,
        complete: scanComplete,
        ranked: rankedInCycle, recorded: current.length,
        excludedFromRanking: Math.max(0, expectedCandidates - rankedInCycle),
        decisionSetComplete,
        evaluation: latestSummary.evaluation || null,
        rankingDiagnostics: latestSummary.rankingDiagnostics || null },
      research: { focusCount: (ctrl.intelligenceFocus || []).length,
        selected: lastIntelligence.selected || [],
        swept: Number(lastIntelligence.swept) || 0,
        latestResults: (lastIntelligence.results || []).slice(0, 8),
        dossierCount: intelligence.length, readyDossiers },
      /* The preregistered minimum counts STRICT-policy closes only. Exploratory
         and control closes are reported beside it, never inside it. */
      learning: (() => {
        /* Same admission rule the promotion ladder uses: strict cohort, current
           frozen identity, bound to the leader (or the baseline before one
           exists), so the console cannot run ahead of the ladder. */
        const identity = { strategyHash: ctrl.strategyHash, universeHash: ctrl.universeHash,
          variantsHash: ctrl.variantsHash };
        const strictClosed = closedTrades.filter((t) => LD.strictTradeAdmissible(t, identity,
          { leaderId: allocationLeaderId, sinceMs: allocationSinceMs })).length;
        const exploratoryClosed = closedTrades.filter((t) => t.paperLearningOnly === true
          && /^exploratory/.test(String(t.learningCohort || "")) && t.cohortRole !== "control").length;
        const controlClosed = closedTrades.filter((t) => t.cohortRole === "control").length;
        return { completed: strictClosed, target: learningTarget,
          remaining: Math.max(0, learningTarget - strictClosed),
          pct: Math.min(100, 100 * strictClosed / learningTarget),
          exploratoryClosed, controlClosed, allClosed: closedTrades.length,
          countsOnlyStrictCohort: true };
      })(),
      cadence: { cycleSeconds, lastCycleDispatchAtMs,
        nextCycleDueAtMs: planMode === "scheduled"
          ? K.nextPlanAtMs(kickCtrl, nowMs)
          : (lastCycleDispatchAtMs ? lastCycleDispatchAtMs + cycleSeconds * 1000 : nowMs),
        planMode, planTimesEt, lastPlanKey: ctrl.lastPlanKey || null,
        duePlanSlot: K.duePlanSlot(kickCtrl, session),
        nextPlanAtMs: K.nextPlanAtMs(kickCtrl, nowMs),
        strikeSeconds: guardSeconds, strikeSecondsClosed: guardSecondsClosed,
        strikeBarTimeframe, lastGuardAtMs,
        nextGuardDueAtMs: lastGuardAtMs
          ? lastGuardAtMs + (session.open ? guardSeconds : guardSecondsClosed) * 1000 : nowMs,
        scanEligible, sessionPhase: session.phase },
      plans: { armed: plansToday.filter((p) => p.status === "armed").length,
        struck: plansToday.filter((p) => p.status === "struck").length,
        today: plansToday },
      /* The patience sleeve, measured live from the current book rather than
         read off the last scan's summary, so the console cannot show room
         that a fill since then has already taken. */
      patience: (() => {
        const policy = PA.policyFrom(strategy);
        const usage = PA.sleeveUsage(positions, orders, balances && balances.usd
          ? (Number(balances.usd.cash) || 0) + (Number(balances.usd.reserved) || 0)
            + positions.reduce((sum, p) => sum + (Number(p.qty) || 0)
              * (Number(p.lastMarkUsd) || Number(p.entryPriceUsd) || 0), 0)
          : 0, policy);
        return { policy, ...usage,
          holdings: positions.filter((p) => PA.isPatient(p)).map((p) => ({
            symbol: p.symbol, grantSessions: p.patience.grantSessions,
            score: p.patience.score, evidence: p.patience.evidence,
            heldTradingDays: p.heldTradingDays, heldText: p.heldText })) };
      })(),
    };
    const payload = {
      ok: true,
      now: new Date().toISOString(),
      session,
      control: {
        /* These compatibility fields are projections of one authoritative
           state, so the existing UI cannot display contradictory controls. */
        operatingState: operating.state,
        operatingLabel: operating.label,
        paperLedger: operating.paperLedger,
        manualApproval: operating.manualApproval,
        exploratoryAuto: operating.exploratoryAuto,
        automaticPaperEntries: operating.automaticPaperEntries,
        autoExploratoryAuthorized: ctrl.autoExploratoryAuthorized === true,
        exploratoryPolicyVersion: ctrl.exploratoryPolicyVersion || null,
        enabled: !operating.paused,
        mode: operating.stage,
        dryRun: !operating.paperLedger,
        killSwitch: operating.paused,
        accountId,
        strategyVersion: ctrl.strategyVersion || strategy.version,
        universeVersion: ctrl.universeVersion || null,
        universeHash: ctrl.universeHash || null,
        strategyHash: ctrl.strategyHash || null,
        variantsHash: ctrl.variantsHash || null,
        fixturesPass: ctrl.fixturesPass === true,
        fixturesCurrent: ctrl.fixturesCommit === commitId(),
        safetyEpochValid: validEpoch(ctrl),
        operatorCeiling: ctrl.operatorCeiling || "approval",
        operatorHold: ctrl.operatorHold === true,
        entriesFrozen: operating.entriesFrozen,
        operatingStateChangedAtMs: Number(ctrl.operatingStateChangedAtMs) || null,
        /* WHY the desk is in this state. Every automatic transition (ladder
           rollback, reconciliation freeze, bootstrap, fixture failure) records
           its reason; the console used to show only the state, so an operator
           who pressed Start and found the desk off minutes later had nothing
           to read. */
        operatingStateReason: ctrl.operatingStateReason || null,
        operatingStateSource: ctrl.operatingStateSource || null,
        stageChangeReason: ctrl.stageChangeReason || null,
        safetyClosedReason: ctrl.safetyClosedReason || null,
        cycleSeconds: ctrl.cycleSeconds || 300,
        planMode, planTimesEt, strikeBarTimeframe, lastPlanKey: ctrl.lastPlanKey || null,
        fullScanCadenceVersion: Number(ctrl.fullScanCadenceVersion)
          || K.FULL_SCAN_CADENCE_VERSION,
        guardSecondsClosed,
        paperLearning: { stored: ctrl.paperLearning || null, active: paperPreview.active,
          refused: paperPreview.refused, applied: paperPreview.applied, limits: ST.RELAX_LIMITS,
          defaults: Object.fromEntries(Object.entries(ST.RELAX_LIMITS)
            .map(([k, v]) => [k, v.dflt])) },
        exploratoryPolicy: strategy.exploratoryAuto || null,
        guardSeconds,
        intelligenceSymbols: IS.configuredSymbols(ctrl),
        intelligenceFocus: ctrl.intelligenceFocus || [],
        lastIntelligenceSummary: ctrl.lastIntelligenceSummary || null,
        lastCycleFinishedAt: ctrl.lastCycleFinishedAt && ctrl.lastCycleFinishedAt.toDate
          ? ctrl.lastCycleFinishedAt.toDate().toISOString() : null,
        lastCycleSummary: ctrl.lastCycleSummary || null,
        lastGuardFinishedAt: ctrl.lastGuardFinishedAt && ctrl.lastGuardFinishedAt.toDate
          ? ctrl.lastGuardFinishedAt.toDate().toISOString() : null,
        lastGuardSummary: ctrl.lastGuardSummary || null,
        ledgerReconciliation: ctrl.ledgerReconciliation || null,
        reconciliationFailure: ctrl.reconciliationFailure || null,
        soakStatus: ctrl.lastSoakStatus || null,
      },
      market: {
        provider: provider.id, feed: provider.feed || null,
        consolidated: !!provider.consolidated,
        liquidityEligible: provider.liquidityEligible !== false,
        exactFifteenMinuteDelay: provider.id === "alpaca"
          && provider.feed === "delayed_sip" && provider.delayMinutes === 15,
        feedDelayMinutes: provider.delayMinutes, maxGrade: provider.maxGrade,
      },
      regime: {
        vix: reg.vixHealthy === true ? (reg.vix || null) : null, vixMedian: reg.vixMedian || null,
        vixNorm: reg.vixHealthy === true && reg.vix && reg.vixMedian ? Number((reg.vix / reg.vixMedian).toFixed(2)) : null,
        cor3m: reg.corHealthy === true ? (reg.cor3m || null) : null,
        gate: S.effectiveDispersionGate(reg.corHealthy === true ? Number(reg.cor3m) : NaN,
          paperPreview.cfg),
        strictGate: S.dispersionGate(reg.corHealthy === true ? Number(reg.cor3m) : NaN,
          strategy.parameters || ST.parameters),
        vixHealthy: reg.vixHealthy === true, corHealthy: reg.corHealthy === true,
        asOf: reg.asOf || null,
      },
      bootstrap: {
        version: ctrl.bootstrapVersion || null,
        at: ctrl.bootstrappedAt && ctrl.bootstrappedAt.toDate ? ctrl.bootstrappedAt.toDate().toISOString() : null,
        report: ctrl.bootstrapReport || null,
      },
      cycleId,
      candidateTotal: current.length,
      candidateExpected: expectedCandidates,
      candidateRankedExpected: rankedInCycle,
      candidateSetComplete: decisionSetComplete,
      navLive: await require("./_investorNav").snapshot(accountId, { positions, balances, cost })
        .catch((e) => ({ error: String(e.message || e).slice(0, 120) })),
      /* Breaches are selected from the stored cards, then both sets are
         projected: `unionEvidenceTrigger` is a server-side selector the
         console never reads. */
      candidates: current.slice().sort((a, b) => (a.rank ?? 1) - (b.rank ?? 1))
        .map(dashboardCandidateRow),
      quoteCoverage: summaryMatchesCycle && Array.isArray(latestSummary.quoteCoverage)
        ? latestSummary.quoteCoverage : [],
      breaches: current.filter((c) => c.unionEvidenceTrigger || c.breach)
        .map(dashboardCandidateRow),
      orders, positions, closedTrades: recentClosedTrades, closedTradeTotal: closedTrades.length,
      balances, cost,
      openaiToday: { usd: Number((openai.usd || 0).toFixed(4)), calls: openai.calls || 0,
                     ceiling: O.DAILY_USD_CEILING },
      intelligence,
      runs,
      operations,
    };
    return boundDashboardPayload(payload);
  },

  /* The learning view: what each frozen variant has done and whether one has
     earned leader status. Evidence weights are diagnostics, never book shares. */
  async learning() {
    /* The dashboard reports THE STORED VERDICT — what the cycle actually
       computed and acted on — never a fresh recompute with different
       parameters. The old version recomputed with allocator defaults, so a
       strategy-file change would have made the dashboard silently disagree
       with the cycle about who was winning and whether the gate was open. */
    const allocSnap = await A.col(A.COL.control).doc("allocation").get();
    const stored = allocSnap.exists ? allocSnap.data() : null;
    const experimentHash = stored && stored.experimentHash;
    let stats = { variants: {}, totalClosed: 0 }, open = { total: 0, byVariant: {} }, roll = null,
      calibration = null, shadowAccounts = [], decisionFeedback = { pending: 0, resolved: 0 };
    if (/^[a-f0-9]{64}$/.test(String(experimentHash || ""))) {
      [stats, open, roll, calibration] = await Promise.all([
        SH.variantStats({ experimentHash }), SH.openCount(experimentHash),
        SH.rollUpStats({ experimentHash }), C.read(experimentHash),
      ]);
      const [accountSnap, observationSnap] = await Promise.all([
        A.col(A.COL.shadowAccounts).get(), A.col(A.COL.shadowObservations).get(),
      ]);
      accountSnap.forEach((d) => { const row = d.data();
        if (row.experimentHash === experimentHash) shadowAccounts.push(row); });
      observationSnap.forEach((d) => { const row = d.data();
        if (row.experimentHash === experimentHash) {
          if (row.status === "resolved") decisionFeedback.resolved += 1;
          else decisionFeedback.pending += 1;
        } });
    }
    const equalWeightPct = Number((100 / V.VARIANTS.length).toFixed(4));
    const zeroRows = V.VARIANTS.map((v) => ({ id: v.id, name: v.name, plain: v.plain,
      evidenceWeightPct: equalWeightPct, trades: 0, independentDays: 0, avgNetBps: 0,
      verdict: "no experiment observations yet" }));
    const view = stored || { rows: zeroRows, powered: false, leaderId: null,
      requiredEffectiveN: AL.requiredNSelect({ k: V.VARIANTS.length }), bestEffectiveN: 0, progressPct: 0,
      note: "No provenance-bound shadow experiment has been recorded yet." };

    return {
      ok: true,
      variants: V.VARIANTS.map((v) => ({ id: v.id, name: v.name, plain: v.plain, params: v.params })),
      variantsHash: V.variantsHash(),
      storedHash: stored ? stored.variantsHash : null,
      experimentHash: experimentHash || null,
      experimentIdentity: view.experimentIdentity || null,
      rows: view.rows || zeroRows,
      powered: !!view.powered,
      leaderId: view.leaderId || null,
      requiredEffectiveN: view.requiredEffectiveN ?? AL.requiredNSelect({ k: V.VARIANTS.length }),
      bestEffectiveN: view.bestEffectiveN ?? 0,
      progressPct: view.progressPct ?? 0,
      note: view.note,
      comparison: view.comparison || null,
      /* Only development-partition resets may be displayed. roll.resets sees
         the complete stored series, including the locked confirmation tail. */
      resets: view.resets && Object.keys(view.resets).length ? view.resets : null,
      discountGamma: view.discountGamma ?? (roll ? roll.discountGamma : null),
      totalClosed: stats.totalClosed,
      openShadow: open,
      shadowAccounts: shadowAccounts.sort((a, b) => String(a.variantId).localeCompare(String(b.variantId))),
      decisionFeedback,
      overfitGuard: view.overfitGuard || null,
      forwardLock: view.forwardLock || null,
      forwardResult: view.forwardResult || null,
      calibration,
    };
  },

  /* Every knob, in plain English, with what raising or lowering it does. */
  async knobs() {
    const st = require("./_investorStrategy.js");
    const p = st.parameters;
    const K = (key, label, what, higher, lower) => ({
      key, label, value: p[key], what, higher, lower,
    });
    const PK = (key, label, what, higher, lower) => ({
      key, label, value: st.portfolioControls[key], what, higher, lower,
    });
    return { ok: true, strategyVersion: st.version, memory: st.memory || null, groups: [
      { group: "When to buy", items: [
        K("entryRank", "How oversold before we look",
          "A stock must be in the bottom 10% of today's ranking, after stripping out what its sector did.",
          "0.20 = act on milder drops, many more trades, weaker case each",
          "0.05 = only the worst drops, far fewer trades"),
        K("minAbsZ", "How unusual the drop must be",
          "How far below its own normal range the stock has moved. 2.0 means roughly a 1-in-40 move.",
          "3.0 = only extreme moves", "1.5 = ordinary dips also qualify"),
      ]},
      { group: "When to sell", items: [
        K("exitRank", "When we let go",
          "Hold until the stock climbs back to the middle of the pack. This one rule was worth more than any entry change in the research.",
          "0.65 = wait for a fuller recovery, longer holds",
          "0.35 = take profit earlier, shorter holds"),
        K("maxHoldDays", "Longest we will hold",
          "A hard stop on time. If the bounce has not come by then, we are wrong and we leave.",
          "more patience, more exposure", "faster turnover, more cost"),
      ]},
      { group: "Safety", items: [
        K("blackoutDays", "Days avoided around a known earnings date",
          "Earnings can move a stock 20% overnight. We simply do not hold through them.",
          "safer, fewer opportunities", "riskier, more opportunities"),
        K("blackoutDaysEstimated", "Days avoided around an ESTIMATED earnings date",
          "Earnings dates are projected from each company's filing rhythm, so the window is wider to absorb the guess.",
          "safer", "riskier - the estimate can be several days off"),
        K("minAdvUsd", "Minimum daily trading activity",
          "Thinly traded stocks cost far more to get in and out of. This blocks them.",
          "only the most liquid names, lower costs", "more names, higher costs"),
        K("sectorCrowdingMultiple", "Sector crowding limit",
          "If most of a sector is falling together, that is a sector event, not an opportunity in one company.",
          "more tolerant of sector-wide moves", "stricter, fewer correlated bets"),
      ]},
      { group: "Sizing", items: [
        PK("riskBudgetPerTradePctOfNav", "Maximum planned loss per idea",
          "Position dollars are divided by the worst of stop distance, two ATRs, five-day expected shortfall, overnight-gap expected shortfall, and 0.5%.",
          "larger planned loss budget", "smaller planned loss budget"),
        PK("ordinaryPositionPctOfNav", "Ordinary notional ceiling",
          "A second cap prevents a low-volatility estimate from creating an outsized position.",
          "larger notional ceiling", "smaller notional ceiling"),
        K("volScalerFloor", "Minimum volatility scaler",
          "Unknown or adverse volatility can only reduce risk; the scaler never exceeds one.",
          "less reduction in adverse regimes", "more reduction"),
        K("volScalerCeiling", "Maximum volatility scaler",
          "This is hard-capped at one: market stress never increases dollars at risk.",
          "must remain at or below one", "more conservative"),
        K("corStandDown", "Stop trading when stocks move as one",
          "When everything moves together there is no company-specific opportunity left to find.",
          "keeps trading in correlated markets", "stands down sooner"),
      ]},
      { group: "Cost discipline", items: [
        K("costMarginMultiple", "Required reward vs cost",
          "An idea must expect at least this many times its own trading cost before we act.",
          "much stricter, far fewer trades", "looser, more trades that may not cover costs"),
        K("decayHaircut", "Discount applied to expectations",
          "Published edges shrink by roughly half once known. We assume ours does too.",
          "more optimistic", "more conservative"),
        K("reversionCapture", "How much of the bounce we expect to catch",
          "We do not assume a full recovery - only a fraction of it.",
          "more optimistic", "more conservative"),
      ]},
      { group: "History and memory", items: [
        K("turnoverPctileCap", "Skip the most heavily traded names",
          "Reversal only works in names that are NOT being traded furiously. In the top tenth by turnover, falling stocks tend to keep falling rather than bounce (Review of Financial Studies, 2022) — so those are refused.",
          "0.95 = only the very busiest names are skipped",
          "0.80 = skip the busiest fifth, fewer but safer trades"),
        K("postEarningsDays", "Days avoided AFTER an earnings report",
          "A drop just after earnings usually reflects real information, and such moves keep drifting in the same direction for days. Fading them is the classic way a bounce strategy loses money.",
          "safer, misses some genuine overreactions",
          "riskier — trades against post-earnings drift"),
        K("blockDowntrends", "Refuse to buy dips in downtrends",
          "A stock below its 200-day average, more than 20% off its six-month high, with its 50-day line still falling, is not oversold — it is sliding. All three must be true before a name is called a downtrend.",
          "on = skip these names entirely (safer)",
          "off = treat a big drop the same whatever the six-month picture"),
        K("signalWindow", "How long a 'move' is",
          "How many 5-minute bars are added up to measure the drop. 12 bars is about an hour.",
          "smoother, slower to notice", "twitchier, more false alarms"),
      ]},
      { group: "Company intelligence", items: [
        K("intelligenceMaxAgeHours", "Maximum dossier age",
          "New risk is blocked when its point-in-time public-source dossier is older than this.",
          "accepts older context and more stale-event risk", "refreshes must succeed more often; more conservative"),
        K("intelligenceLookbackDays", "SEC and event lookback",
          "How far the dossier considers eligible point-in-time documents. The frozen policy keeps enough history to retain annual filing exposures while the price path remains strictly pre-decision.",
          "more history and more chance of stale event confusion", "less context and faster reads"),
        K("temporalMaxAgeHours", "Maximum temporal-context age",
          "Scheduled events, active public hazards, seasonal estimates, and rolling drivers must be refreshed inside this age before new risk can pass.",
          "accepts older hazard and calendar state", "requires more frequent successful refreshes"),
        K("temporalSeasonalityMinTradingDays", "Minimum seasonal history",
          "Same-calendar-month behavior abstains until this many point-in-time daily observations exist; the frozen value is about three trading years.",
          "more repeated years but slower warm-up", "thinner and less reliable seasonal evidence"),
        K("intelligenceCompaniesPerSweep", "Companies researched per evidence sweep",
          "The rotating number of monitored companies whose public lanes are refreshed in one slower evidence job.",
          "fresher coverage but more source load and runtime", "lower load but dossiers take longer to rotate"),
      ]},
      { group: "Learning", items: [
        K("tHurdle", "Proof required before favouring a variant",
          `Legacy single-test reference only. Promotion uses a best-of-${V.VARIANTS.length} power gate, serially robust paired comparison, DSR/PBO, historical stress, and locked future paper.`,
          "demands more proof, learns slower but more truthfully",
          "acts on weaker evidence - this is the knob that decides whether the system learns or fools itself"),
        K("expectedEdgeBps", "Edge we are looking for",
          "How big an effect we think exists, in hundredths of a percent. Used only to work out how much evidence is needed.",
          "assumes a bigger effect, needs less evidence", "assumes smaller, needs more"),
        K("perTradeSdBps", "How noisy a single trade is",
          "The spread of outcomes on any one trade. Bigger noise means more evidence needed.",
          "needs more evidence", "needs less"),
      ]},
    ]};
  },

  /* Full detail for one name — the per-candidate panel. */
  async candidate({ symbol, cycleId }) {
    symbol = String(symbol || "").toUpperCase();
    if (!SYMBOL.test(symbol)) return { error: "valid symbol required" };
    if (cycleId != null && !/^\d{4}-\d{2}-\d{2}_\d{4}$/.test(String(cycleId))) {
      return { error: "invalid cycleId" };
    }
    let cid = cycleId;
    if (!cid) {
      /* where+orderBy on different fields needs a composite index that no
         config in this repo creates — in real Firestore this threw
         FAILED_PRECONDITION and the candidate panel 500'd. Equality-only
         query, newest picked in memory. */
      const s = await A.col(A.COL.candidates).where("symbol", "==", symbol).limit(40).get();
      if (s.empty) return { error: `no candidate record for ${symbol}` };
      let newest = null;
      s.forEach((d) => { const x = d.data(); if (!newest || String(x.cycleId) > String(newest.cycleId)) newest = x; });
      cid = newest.cycleId;
    }
    const [cSnap, pSnap] = await Promise.all([
      A.col(A.COL.candidates).doc(`${cid}_${symbol}`).get(),
      A.col(A.COL.positions).doc(`${(await ctrlDoc()).accountId || "paper-1"}_${symbol}`).get(),
    ]);
    if (!cSnap.exists) return { error: `no candidate record for ${symbol} in ${cid}` };

    const session = M.sessionState(new Date());
    let bars = [];
    try { bars = await M.readBars(symbol, session.date); } catch {}
    if (!bars.length) { try { bars = await M.readRecentBars(symbol, 2); } catch {} }

    const docs = [];
    try {
      const dsnap = await A.col(A.COL.documents).where("symbol", "==", symbol).limit(60).get();
      const dall = [];
      dsnap.forEach((d) => dall.push(d.data()));
      dall.sort((a, b) => String(b.first_seen_at || "") < String(a.first_seen_at || "") ? -1 : 1);
      for (const x of dall.slice(0, 12)) {
        docs.push({
          documentId: x.documentId, form: x.form, title: x.title, link: x.link,
          accession: x.accession,
          source_published_at: x.source_published_at,
          first_seen_at: x.first_seen_at && x.first_seen_at.toDate ? x.first_seen_at.toDate().toISOString() : null,
        });
      }
    } catch {}

    /* Repair an incomplete store on demand and return the full immutable daily
       series. The browser derives every shorter range from this one response. */
    let daily = [], history = null, reversion = null, dailyMeta = null;
    try {
      const ensured = await H.ensureDailyHistory(symbol);
      const series = ensured.series;
      daily = H.chartSeries(series, { throughDate: session.date });
      dailyMeta = {
        tradingDays: daily.length,
        firstDate: daily.length ? daily[0].d : null,
        lastDate: daily.length ? daily[daily.length - 1].d : null,
        backfillAttempted: ensured.attempted === true,
        repairedOnRead: ensured.repaired === true,
        backfillComplete: ensured.backfill && ensured.backfill.complete === true,
        note: ensured.note || null,
      };
      const ctx = H.contextFor(series, session.date);
      if (ctx.ok) {
        history = ctx;
        const rev = H.reversionEvents(series, session.date);
        reversion = rev.n ? rev : null;
        history.notes = H.describe(ctx, (cSnap.data() || {}).reversion || null);
      } else {
        history = { ok: false, days: ctx.days, reason: ctx.reason };
      }
    } catch {}

    return { ok: true, candidate: cSnap.data(), bars, documents: docs,
             daily, dailyMeta, history, reversion,
             position: pSnap.exists ? pSnap.data() : null };
  },

  /* Performance: the equity curve, how deep the worst hole got, and the one
     comparison that actually matters — did this beat holding the same names. */
  async performance() {
    const ctrl = await ctrlDoc();
    const accountId = ctrl.accountId || "paper-1";
    const navSnap = await A.col(A.COL.control).doc("control")
      .collection("navHistory").orderBy("date", "desc").limit(400).get();
    const nav = [], excludedNav = [];
    navSnap.forEach((d) => {
      const row = d.data();
      if (row.finalized === true && row.marksComplete === true
          && /^[a-f0-9]{64}$/.test(String(row.markSetSha256 || ""))) nav.push(row);
      else excludedNav.push({ date: row.date || d.id,
        reason: row.finalized !== true ? "not_finalized"
          : (row.marksComplete !== true ? "incomplete_marks" : "missing_mark_hash") });
    });
    nav.reverse();   // fetched newest-first so the window slides; stats want oldest-first
    const equity = R.equityStats(nav);
    equity.excludedNavMarks = excludedNav.length;
    equity.excludedNavSample = excludedNav.slice(0, 12);

    let bench = { ok: false, reason: "not enough daily history yet" };
    if (equity.ok) {
      const uMod = require("./_investorUniverse.js");
      const uSnap = await A.col(A.COL.universe).doc(ctrl.universeVersion || uMod.version || "v1").get();
      const u = uSnap.exists ? uSnap.data() : uMod;
      const syms = (u.tradeTier || []).map((t) => t.symbol);
      const daily = {};
      /* This endpoint is operator-initiated, but 300+ simultaneous Firestore
         reads can still burst quotas or time out. Bounded batches preserve the
         exact frozen benchmark without turning it into a first-120 sample. */
      for (let i = 0; i < syms.length; i += 16) {
        await Promise.all(syms.slice(i, i + 16).map(async (sym) => {
          try { const ser = await H.readDaily(sym); if (ser.length) daily[sym] = ser; } catch {}
        }));
      }
      bench = R.benchmarkReturn(daily, nav[0].date, nav[nav.length - 1].date);
      bench.universeVersion = ctrl.universeVersion || u.version || null;
      bench.universeHash = ctrl.universeHash || u.contentHash || null;
      bench.expectedNames = syms.length;
    }

    const book = await (async () => {
      const ps = await A.col(A.COL.positions).where("accountId", "==", accountId)
        .where("open", "==", true).get();
      const rows = []; ps.forEach((d) => rows.push(d.data()));
      const b = await L.balances(accountId);
      const marks = {};
      for (const p of rows) {
        const prov = p.lastMarkProvenance || p.entryBarProvenance;
        if (Number(p.lastMarkUsd) > 0 && prov
            && /^[a-f0-9]{64}$/.test(String(prov.sourceSha256 || ""))) marks[p.symbol] = Number(p.lastMarkUsd);
      }
      return R.markedBook(rows, marks, null, { cash: b.usd[L.ACCT.CASH] || 0,
        reserved: b.usd[L.ACCT.RESERVED] || 0 });
    })();

    return {
      ok: true, equity, benchmark: bench,
      attribution: R.attribution(equity, bench),
      navHistory: nav.map((n) => ({ date: n.date, navUsd: n.navUsd })),
      book: { navUsd: book.navUsd, count: book.book.count, grossPct: book.book.grossPct,
        byClusterPct: book.book.byClusterPct, untrustedMarks: book.untrustedMarks },
      costMeter: await L.costMeter(accountId).catch(() => null),
    };
  },

  /* Standalone history read for any roster name, including the full stored
     window needed by the All view and 200-day calculations. */
  async history({ symbol }) {
    symbol = String(symbol || "").toUpperCase();
    if (!SYMBOL.test(symbol)) return { error: "valid symbol required" };
    const session = M.sessionState(new Date());
    const ensured = await H.ensureDailyHistory(symbol);
    const series = ensured.series;
    if (!series.length) return { ok: true, symbol, daily: [], history: null,
      dailyMeta: { tradingDays: 0, firstDate: null, lastDate: null,
        backfillAttempted: ensured.attempted === true,
        backfillComplete: ensured.backfill && ensured.backfill.complete === true,
        note: ensured.note || null },
      note: "no daily history is available from the configured provider" };
    const ctx = H.contextFor(series, session.date);
    const rev = H.reversionEvents(series, session.date);
    const daily = H.chartSeries(series, { throughDate: session.date });
    return {
      ok: true, symbol,
      daily,
      dailyMeta: { tradingDays: daily.length,
        firstDate: daily.length ? daily[0].d : null,
        lastDate: daily.length ? daily[daily.length - 1].d : null,
        backfillAttempted: ensured.attempted === true,
        repairedOnRead: ensured.repaired === true,
        backfillComplete: ensured.backfill && ensured.backfill.complete === true,
        note: ensured.note || null },
      history: ctx.ok ? ctx : { ok: false, days: ctx.days, reason: ctx.reason },
      reversion: rev.n ? rev : null,
      notes: H.describe(ctx, null),
    };
  },

  /* Intraday 5-minute bars for the company chart: the last N sessions from
     the stored per-session documents the cycle writes, topped up from the
     configured provider when a session is missing or today's document lags
     the delayed edge. Read-only for the desk's decisions: nothing here feeds
     a decision, so a display fetch can never change what the desk does. */
  /* Latest trade prices for the symbols on screen. DISPLAY ONLY: Alpaca's
     IEX lane is real-time on every plan, so the console can show holdings
     moving second by second, while every ledger mark, fill and decision keeps
     using the consolidated delayed tape. The response says which it is. */
  async quotes({ symbols }) {
    const list = [...new Set((Array.isArray(symbols) ? symbols : String(symbols || "").split(","))
      .map((x) => String(x || "").trim().toUpperCase()).filter((x) => SYMBOL.test(x)))].slice(0, 120);
    if (!list.length) return { ok: true, quotes: {}, feed: null };
    await M.loadMarketSettings();
    if (!M.providerCredentialed("alpaca")) return { ok: false, error: "no Alpaca credentials", quotes: {} };
    const nowMs = Date.now();
    if (quotesCache.at && nowMs - quotesCache.at < 3000 && list.every((x) => quotesCache.quotes[x])) {
      return { ok: true, quotes: Object.fromEntries(list.map((x) => [x, quotesCache.quotes[x]])),
        feed: "iex", displayOnly: true, cached: true, asOf: quotesCache.asOf };
    }
    const creds = M.providerCredentials("alpaca");
    const url = `https://data.alpaca.markets/v2/stocks/trades/latest?symbols=${encodeURIComponent(list.join(","))}&feed=iex`;
    let r;
    try {
      r = await F.fetchPublic(url, { sourceId: "alpaca.trades.latest", accept: ["json"], timeoutMs: 8000,
        headers: { "APCA-API-KEY-ID": creds.keyId, "APCA-API-SECRET-KEY": creds.secretKey } });
    } catch (e) { return { ok: false, error: String(e.message || e).slice(0, 160), quotes: {} }; }
    const trades = (r.json && r.json.trades) || {};
    const quotes = {};
    for (const [sym, t] of Object.entries(trades)) {
      if (t && Number(t.p) > 0) quotes[sym] = { p: Number(t.p), t: t.t || null, s: Number(t.s) || null };
    }
    quotesCache = { at: nowMs, quotes: { ...quotesCache.quotes, ...quotes }, asOf: new Date(nowMs).toISOString() };
    return { ok: true, quotes, feed: "iex", displayOnly: true, asOf: quotesCache.asOf,
      missing: list.filter((x) => !quotes[x]) };
  },

  /* Account value over time for the console chart. Display only. */
  async navSeries({ range }) {
    const ctrl = await ctrlDoc();
    const accountId = ctrl.accountId || "paper-1";
    const NAV = require("./_investorNav");
    const key = Object.prototype.hasOwnProperty.call(NAV.RANGES, String(range)) ? String(range) : "today";
    const out = await NAV.series(accountId, key);
    const live = await NAV.snapshot(accountId).catch(() => null);
    return { ok: true, accountId, ...out, live,
      session: (() => { const st = M.sessionState(new Date()); return { date: st.date, open: !!st.open, phase: st.phase }; })() };
  },

  async intraday({ symbol, sessions }) {
    symbol = String(symbol || "").toUpperCase();
    if (!SYMBOL.test(symbol)) return { error: "valid symbol required" };
    const want = Math.max(1, Math.min(14, Math.round(Number(sessions) || 1)));
    await M.loadMarketSettings();
    const provider = M.activeProvider();
    const nowMs = Date.now();
    /* Trailing trading sessions, oldest first (today included when it is a
       trading day — before the open it simply has no bars yet). */
    const dates = [];
    const cursor = new Date(nowMs);
    let guard = 0;
    while (dates.length < want && guard < 40) {
      const st = M.sessionState(cursor);
      if (st.tradingDay) dates.push(st.date);
      cursor.setUTCDate(cursor.getUTCDate() - 1);
      guard += 1;
    }
    dates.reverse();
    const readSession = async (date) => {
      const snap = await A.col(A.COL.marketLatest).doc(M.barDocId(symbol, date)).get();
      return snap.exists ? snap.data() : null;
    };
    let docs = await Promise.all(dates.map(readSession));
    const delayMs = (Number(provider.delayMinutes) || 0) * 60000;
    const todaySt = M.sessionState(new Date(nowMs));
    const lastDate = dates[dates.length - 1];
    const lastDoc = docs[docs.length - 1];
    const lastBarMs = lastDoc && (lastDoc.bars || []).length
      ? Date.parse(lastDoc.bars[lastDoc.bars.length - 1].t) : 0;
    const delayedEdgeMs = nowMs - delayMs;
    const todayLagging = lastDate === todaySt.date && todaySt.open
      && delayedEdgeMs - lastBarMs > 12 * 60000;
    const missing = dates.filter((d, i) => !docs[i] || !(docs[i].bars || []).length);
    let fetched = null;
    if ((missing.length || todayLagging) && provider.id !== "manual") {
      try {
        const limit = Math.min(9000, want * 80);
        const got = await M.fetchBars([symbol], { timeframe: "5Min", limit });
        const bars = (got.bars && got.bars[symbol]) || [];
        fetched = { provider: got.provider, feed: got.feed || null, bars: bars.length };
        if (bars.length) {
          try {
            await M.writeBars(symbol, todaySt.date, bars, {
              provider: got.provider, feed: got.feed || null,
              adjustment: got.adjustment || null, sourceSha256: got.sha256 || null,
              grade: null, gradeReasons: ["chart_backfill"], feedDelayMinutes: provider.delayMinutes,
            });
          } catch (e) { fetched.writeError = String(e.message).slice(0, 120); }
          docs = await Promise.all(dates.map(readSession));
        }
      } catch (e) { fetched = { error: String(e.message).slice(0, 160) }; }
    }
    const out = dates.map((date, i) => {
      const d = docs[i];
      const bars = (d && d.bars || []).map((b) => ({ t: b.t, o: b.o, h: b.h, l: b.l, c: b.c, v: b.v }));
      return { date, bars, provider: d && d.provider || null, feed: d && d.feed || null };
    });
    return { ok: true, symbol, sessions: out, requested: want,
      delayMinutes: provider.delayMinutes, provider: provider.id, feed: provider.feed || null,
      consolidated: !!provider.consolidated, asOf: new Date(nowMs).toISOString(),
      session: { date: todaySt.date, open: !!todaySt.open, phase: todaySt.phase, tradingDay: !!todaySt.tradingDay },
      fetched };
  },

  async approve({ orderId, operator }) {
    if (!/^[a-f0-9]{32}$/.test(String(orderId || ""))) return { error: "valid orderId required" };
    const r = await L.approveOrder(orderId, operator || "operator");
    const success = r.status === "approved" && !r.noop && !r.refused;
    await A.col(A.COL.audit).add({ action: success ? "approve" : "approve_refused",
      orderId, resultingStatus: r.status || null, refusal: r.refused || null,
      operator: operator || "operator",
      at: A.FV.serverTimestamp(), ...A.envelope({ created_by: "investorApi" }) });
    return { ...r, ok: success, transitioned: success };
  },

  /* Approved orders whose symbol stops returning bars sit forever with cash
     debited into RESERVED and no way back — settlement skips them silently and
     nothing expires them. This releases them. */
  async expireStaleOrders({ olderThanHours = 24 } = {}) {
    const ctrl = await ctrlDoc();
    const accountId = ctrl.accountId || "paper-1";
    const cutoff = Date.now() - olderThanHours * 3600e3;
    const snap = await A.col(A.COL.orders)
      .where("accountId", "==", accountId).where("status", "==", "approved").get();
    const released = [];
    for (const d of snap.docs) {
      const o = d.data();
      const at = o.decisionAtMs || Date.parse(o.created_at) || 0;
      if (at && at > cutoff) continue;
      try {
        const r = await L.releaseOrder(o.orderId, "expired — never became fillable");
        if (!r.noop) released.push({ orderId: o.orderId, symbol: o.symbol });
      } catch (e) { /* skip */ }
    }
    return { ok: true, released: released.length, detail: released.slice(0, 20) };
  },

  async reject({ orderId, reason, operator }) {
    if (!/^[a-f0-9]{32}$/.test(String(orderId || ""))) return { error: "valid orderId required" };
    const r = await L.rejectOrder(orderId, reason, operator || "operator");
    const success = ["rejected", "expired"].includes(r.status) && !r.noop;
    await A.col(A.COL.audit).add({ action: success ? "reject" : "reject_refused",
      orderId, resultingStatus: r.status || null, reason: String(reason || "").slice(0, 300) || null,
      operator: operator || "operator", at: A.FV.serverTimestamp(),
      ...A.envelope({ created_by: "investorApi" }) });
    return { ...r, ok: success, transitioned: success };
  },

  /* Kill switch cancels proposed and unfilled orders and blocks new decisions.
     It does NOT delete evidence, history, or the ledger. */
  async kill({ operator }) {
    const ctrl = await ctrlDoc();
    const accountId = ctrl.accountId || "paper-1";
    const stateChange = STATE.transition(ctrl, STATE.STATES.PAUSED, {
      source: "operator", reason: "operator engaged the paper-desk kill switch",
    });
    await A.col(A.COL.control).doc("control").set({
      ...stateChange.patch, killedBy: operator || "operator",
      killedAt: A.FV.serverTimestamp(),
      safetyEpoch: null, ladderStreak: 0,
    }, { merge: true });
    /* The control document selects one paper account. A kill on that account
       must never cancel another isolated research account sharing the same
       project, so both order scans bind accountId as well as status. */
    const snap = await A.col(A.COL.orders)
      .where("accountId", "==", accountId).where("status", "==", "proposed").get();
    let cancelled = 0;
    for (const d of snap.docs) {
      try { const r = await L.rejectOrder(d.data().orderId, "kill switch", operator); if (!r.noop) cancelled += 1; }
      catch {}
    }

    /* APPROVED orders must be released too, not only proposed ones. An
       approved order holds cash in RESERVED and would still FILL after a
       resume — a kill switch that leaves live buy orders armed is not a kill
       switch. releaseOrder returns the reserved cash through the ledger. */
    let released = 0;
    const appSnap = await A.col(A.COL.orders)
      .where("accountId", "==", accountId).where("status", "==", "approved").get();
    for (const d of appSnap.docs) {
      try { const r = await L.releaseOrder(d.data().orderId, "kill switch"); if (!r.noop) released += 1; }
      catch (e) { /* an unreleasable order is left for expiry */ }
    }

    await A.col(A.COL.audit).add({ action: "kill", accountId, cancelled, released,
      operator: operator || "operator", at: A.FV.serverTimestamp(),
      ...A.envelope({ created_by: "investorApi" }) });
    return { killSwitch: true, cancelledProposals: cancelled, releasedApproved: released };
  },

  async resume({ operator }) {
    const ctrl = await ctrlDoc();
    const stateChange = STATE.transition(ctrl, STATE.STATES.OBSERVATION, {
      source: "operator", clearPause: true,
      reason: "operator resumed monitoring in observation-only state",
    });
    await A.col(A.COL.control).doc("control").set({
      ...stateChange.patch, resumedBy: operator || "operator",
      resumedAt: A.FV.serverTimestamp(),
      safetyEpoch: null, ladderStreak: 0,
    }, { merge: true });
    return { ...stateChange.state, killSwitch: false, enabled: true, dryRun: true, mode: "research",
      note: "monitoring resumed; entries remain closed until a new safety epoch is activated" };
  },

  async activateSafety({ operator, exploratoryAuto = false }) {
    let ctrl = await ctrlDoc();
    if (ctrl.fixturesPass !== true || ctrl.fixturesCommit !== commitId()) {
      /* A new Netlify deployment changes the build identity before the next
         scheduled/background cycle has necessarily run. Starting the desk
         must not deadlock on an attestation that this same deployed runtime
         can produce immediately. Run the existing deterministic fixtures in
         this function, bind their result to the current deploy identity, and
         continue only when every fixture passes. */
      let fixtures;
      try { fixtures = require("./_investorSelftest").runFixtures(); }
      catch (e) {
        fixtures = { pass: false, fixtureHash: null,
          error: String(e && e.message || e).slice(0, 200), cases: [] };
      }
      const fixturePatch = {
        fixturesPass: fixtures.pass === true,
        fixturesCommit: commitId(),
        fixtureHash: fixtures.fixtureHash || null,
        fixturesCheckedAt: A.FV.serverTimestamp(),
        fixtureFailures: (fixtures.cases || []).filter((x) => !x.pass).slice(0, 10),
      };
      await A.col(A.COL.control).doc("control").set(fixturePatch, { merge: true });
      if (!fixtures.pass) {
        return { error: "current-build fixture attestation failed",
          fixtureFailures: fixturePatch.fixtureFailures,
          fixtureError: fixtures.error || null };
      }
      ctrl = { ...ctrl, ...fixturePatch };
    }
    let u = null, s = null;
    if (ctrl.universeVersion && ctrl.strategyVersion && ctrl.accountId) {
      [u, s] = await Promise.all([
        A.col(A.COL.universe).doc(ctrl.universeVersion).get(),
        A.col(A.COL.strategies).doc(ctrl.strategyVersion).get(),
      ]);
    }

    /* Start Desk is the operator's explicit authorization point, so it must
       also make a clean deployment usable immediately. Previously it assumed
       the scheduled worker had already registered the bundled immutable
       universe and strategy; pressing Start before that first successful cron
       cycle produced "frozen universe or strategy is missing" indefinitely.
       Bootstrap only the critical identity/account/ledger path here. Slow
       company enrichment remains in the background evidence worker. */
    if (!ctrl.universeVersion || !ctrl.strategyVersion || !ctrl.accountId
        || !u || !s || !u.exists || !s.exists) {
      let bootstrap;
      try {
        bootstrap = await B.ensureBootstrapped({ force: true, enrich: false });
      } catch (e) {
        return { error: "automatic frozen-policy bootstrap failed",
          bootstrapError: String(e && e.message || e).slice(0, 200) };
      }
      if (!bootstrap || bootstrap.bootstrapped !== true) {
        return { error: "automatic frozen-policy bootstrap failed",
          bootstrapFailures: (bootstrap && bootstrap.steps || [])
            .filter((step) => step && step.ok === false).slice(0, 10) };
      }
      ctrl = await ctrlDoc();
      if (!ctrl.universeVersion || !ctrl.strategyVersion || !ctrl.accountId) {
        return { error: "bootstrap identity is incomplete" };
      }
      [u, s] = await Promise.all([
        A.col(A.COL.universe).doc(ctrl.universeVersion).get(),
        A.col(A.COL.strategies).doc(ctrl.strategyVersion).get(),
      ]);
    }
    if (!u.exists || !s.exists) return { error: "frozen universe or strategy is missing" };
    const uv = B.validateFrozenUniverse(u.data(), ctrl.universeVersion);
    const sh = B.strategyHash(s.data());
    if (!uv.ok || sh !== s.data().contentHash || s.data().immutable !== true) {
      return { error: "frozen policy hash validation failed" };
    }
    const safetyEpoch = { accountId: ctrl.accountId, strategyVersion: ctrl.strategyVersion,
      universeVersion: ctrl.universeVersion, universeHash: uv.actual, strategyHash: sh,
      variantsHash: V.variantsHash(), commit: commitId(), activatedAtMs: Date.now(),
      activatedBy: operator || "operator" };
    const exploratoryPolicy = s.data().exploratoryAuto || {};
    const startExploratory = exploratoryAuto === true
      && exploratoryPolicy.enabled === true;
    if (startExploratory) {
      const reconciliation = await L.reconcileAccount(ctrl.accountId, {
        context: "exploratory_auto_activation",
      });
      if (!reconciliation.pass) {
        return { error: "paper ledger did not reconcile; automatic entries remain frozen",
          reconciliation };
      }
    }
    /* The ladder re-measures the approval rung at the end of every cycle
       (ledger audit, execution-provenance audit, fixtures, epoch) and rolls
       an unearned stage back to research. Starting a state that the next
       cycle will undo is worse than refusing: the operator sees "started",
       then finds the desk off with no visible cause. Measure the same two
       audits here and refuse with their findings instead. */
    if (startExploratory) {
      const [ledgerAudit, executionAudit] = await Promise.all([
        L.auditLedger(ctrl.accountId).catch((e) => ({ pass: false, error: String(e.message).slice(0, 160) })),
        L.executionAudit(ctrl.accountId).catch((e) => ({ pass: false, error: String(e.message).slice(0, 160) })),
      ]);
      if (!ledgerAudit.pass) {
        return { error: "paper-ledger audit failed; the promotion ladder would roll this start back on the next scan",
          ledgerAudit: { pass: false, error: ledgerAudit.error || null,
            discrepancies: (ledgerAudit.discrepancies || []).slice(0, 10) } };
      }
      if (!executionAudit.pass) {
        return { error: "execution-provenance audit failed on stored fills/trades; the ladder would roll this start back on the next scan — open a fresh paper account to leave the legacy records behind",
          executionAudit: { pass: false, error: executionAudit.error || null,
            violations: (executionAudit.violations || []).slice(0, 10) } };
      }
    }
    const target = startExploratory
      ? STATE.STATES.EXPLORATORY_AUTO : STATE.STATES.OBSERVATION;
    const selected = STATE.transition(ctrl, target, {
      source: "operator", validEpoch: true, clearPause: true,
      reason: startExploratory
        ? "operator authorized immediate automatic exploratory paper trading"
        : "operator activated the frozen build identity in observation-only mode",
    });
    if (!selected.ok) return { error: selected.reason };
    await A.col(A.COL.control).doc("control").set({ safetyEpoch,
      universeHash: uv.actual, strategyHash: sh, variantsHash: V.variantsHash(),
      ...selected.patch, ladderStreak: 0,
      ...(startExploratory ? {
        autoExploratoryAuthorized: true,
        exploratoryPolicyVersion: exploratoryPolicy.version || null,
        operatorCeiling: "approval",
        paperLearning: { ...(exploratoryPolicy.paperLearningDefaults || {}) },
      } : {}) }, { merge: true });
    await A.col(A.COL.audit).add({ action: "activate_safety_epoch", safetyEpoch,
      requestedOperatingState: target,
      at: A.FV.serverTimestamp(), ...A.envelope({ created_by: "investorApi" }) });
    return { ok: true, safetyEpoch, operatingState: target,
      dryRun: !startExploratory, automaticPaperEntries: startExploratory,
      note: startExploratory
        ? "identity activated; autonomous exploratory paper entries are enabled immediately without claiming measured limited-auto validation"
        : "identity activated; observation-only remains on until the operator chooses a paper-ledger state" };
  },

  async setControl({ patch, operator }) {
    const ALLOW = ["dryRun", "cycleSeconds", "evidenceEverySeconds",
      "operatorCeiling", "operatorHold", "afterHoursCycles", "entriesFrozen",
      "guardSeconds", "operatingState",
      /* Two-tier cadence: the deep-scan schedule and the strike pass. */
      "planMode", "planTimesEt", "strikeBarTimeframe"];
    const MODES = ["research", "approval", "shadow", "limited_auto"];
    const clean = {}, refused = {};
    const ctrl = await ctrlDoc();
    const before = STATE.describe(ctrl);
    for (const k of ALLOW) {
      if (patch && patch[k] !== undefined) {
        if (k === "operatorCeiling" && !MODES.includes(patch[k])) { refused[k] = "invalid stage"; continue; }
        if (k === "operatorCeiling") {
          clean[k] = patch[k]; clean.ladderStreak = 0;
          continue;
        }
        if (k === "operatingState") {
          if (![STATE.STATES.OBSERVATION, STATE.STATES.MANUAL_APPROVAL,
            STATE.STATES.EXPLORATORY_AUTO, STATE.STATES.ENTRY_FROZEN].includes(patch[k])) {
            refused[k] = "operators may select observation, manual_approval, exploratory_auto, or entry_frozen";
          }
          continue;
        }
        if (k === "cycleSeconds") {
          const n = Number(patch[k]);
          if (Number.isFinite(n) && n >= 60 && n <= 86400) clean[k] = n;
          else refused[k] = "must be between 60 and 86400 seconds";
          continue;
        }
        if (k === "guardSeconds") {
          const n = Number(patch[k]);
          if (Number.isFinite(n) && n >= 60 && n <= 600) clean[k] = n;
          else refused[k] = "must be between 60 and 600 seconds";
          continue;
        }
        if (k === "planMode") {
          if (patch[k] === "scheduled" || patch[k] === "interval") {
            clean[k] = patch[k];
            clean.fullScanCadenceVersion = require("./investorKick").FULL_SCAN_CADENCE_VERSION;
            clean.planModeSource = "operator";
          }
          else refused[k] = "must be 'scheduled' (deep scans at set New York times) or 'interval' (every cycleSeconds)";
          continue;
        }
        if (k === "planTimesEt") {
          const K = require("./investorKick");
          const times = K.normalizePlanTimes(patch[k]);
          if (times) {
            clean[k] = times;
            if (patch.planMode === undefined) clean.planMode = "scheduled";
            clean.fullScanCadenceVersion = require("./investorKick").FULL_SCAN_CADENCE_VERSION;
            clean.planModeSource = "operator";
          }
          else refused[k] = "give one to six HH:MM New York times between 09:45 and 15:30";
          continue;
        }
        if (k === "strikeBarTimeframe") {
          if (patch[k] === "1Min" || patch[k] === "5Min") clean[k] = patch[k];
          else refused[k] = "must be 1Min or 5Min";
          continue;
        }
        if (k === "evidenceEverySeconds") {
          const n = Number(patch[k]);
          if (Number.isFinite(n) && n >= 300 && n <= 86400) clean[k] = n;
          else refused[k] = "must be between 300 and 86400 seconds";
          continue;
        }
        if (k === "dryRun") {
          clean[k] = !!patch[k];
          continue;
        }
        if (k === "afterHoursCycles" || k === "operatorHold" || k === "entriesFrozen") {
          clean[k] = !!patch[k]; continue;
        }
      }
    }
    for (const k of Object.keys(patch || {})) if (!ALLOW.includes(k)) refused[k] = "not operator-settable";

    /* Resolve every legacy control gesture into one state transition. A ceiling
       is a cap, not permission to promote. Lowering a ceiling can demote an
       earned automatic stage, while raising it only changes the future cap. */
    let target = null;
    if (patch && patch.operatingState !== undefined && !refused.operatingState) {
      target = patch.operatingState;
    }
    const requestedCeiling = clean.operatorCeiling || ctrl.operatorCeiling || "approval";
    const base = before.baseState;
    const baseRank = LD.stageIndex(STATE.STAGE[base] || "research");
    const ceilingRank = LD.stageIndex(requestedCeiling);
    if (clean.operatorCeiling === "research") target = STATE.STATES.OBSERVATION;
    else if (clean.operatorCeiling && ceilingRank < baseRank) {
      target = STATE.stateForStage(requestedCeiling);
    }
    if (patch && patch.dryRun === true) target = STATE.STATES.OBSERVATION;
    if (patch && patch.dryRun === false) {
      if (requestedCeiling === "research") {
        refused.dryRun = "observation-only ceiling cannot open the paper ledger";
      } else if (before.paused) {
        refused.dryRun = "resume the desk before opening the paper ledger";
      } else if (before.entriesFrozen) {
        refused.dryRun = "unfreeze entries before opening the paper ledger";
      } else {
        target = (base === STATE.STATES.SHADOW || base === STATE.STATES.LIMITED_AUTO)
          ? base : STATE.STATES.MANUAL_APPROVAL;
      }
    }
    if (patch && patch.entriesFrozen === true) target = STATE.STATES.ENTRY_FROZEN;
    if (patch && patch.entriesFrozen === false && before.entriesFrozen) {
      if (ctrl.reconciliationFailure) {
        refused.entriesFrozen = "run a passing ledger reconciliation before unfreezing entries";
      } else target = STATE.resumeState(ctrl);
    }

    if (!Object.keys(refused).length && target) {
      const changed = STATE.transition(ctrl, target, {
        source: "operator", validEpoch: validEpoch(ctrl),
        reason: target === STATE.STATES.OBSERVATION
          ? "operator selected observation-only paper research"
          : target === STATE.STATES.ENTRY_FROZEN
            ? "operator froze new paper entries"
            : "operator selected a validated paper-ledger state",
      });
      if (!changed.ok) {
        const sourceKey = patch && patch.operatingState !== undefined ? "operatingState"
          : patch && patch.dryRun !== undefined ? "dryRun"
            : patch && patch.entriesFrozen !== undefined ? "entriesFrozen"
              : "operatorCeiling";
        refused[sourceKey] = changed.reason;
      }
      else Object.assign(clean, changed.patch, {
        stageChangeReason: changed.patch.operatingStateReason,
      });
    }

    let entryCleanup = null;
    if (Object.keys(refused).length === 0 && Object.keys(clean).length) {
      await A.col(A.COL.control).doc("control").set(clean, { merge: true });
      /* Close the gate first, then drain pre-existing entry orders. Approval
         and fill transactions re-read this same control document, so an order
         racing the cleanup fails closed and releases its reservation. */
      if (clean.entriesFrozen === true || clean.operatingState === STATE.STATES.OBSERVATION) {
        entryCleanup = await closeEntryQueue(ctrl.accountId || "paper-1",
          clean.entriesFrozen === true ? "operator entry freeze" : "observation-only state",
          operator);
      }
      await A.col(A.COL.audit).add({ action: "setControl", patch: clean, entryCleanup,
        operator: operator || "operator", at: A.FV.serverTimestamp(),
        ...A.envelope({ created_by: "investorApi" }) });
    }
    const refusedKeys = Object.keys(refused);
    const after = refusedKeys.length ? before : STATE.describe({ ...ctrl, ...clean });
    return { ok: refusedKeys.length === 0,
      patched: refusedKeys.length === 0 ? clean : {}, refused, entryCleanup,
      operating: after,
      error: refusedKeys.length
        ? refusedKeys.map((k) => `${k}: ${refused[k]}`).join("; ")
        : undefined };
  },

  async recordRecallBenchmark({ truePositives, falseNegatives, labelSetSha256, operator }) {
    const tp = Math.floor(Number(truePositives)), fn = Math.floor(Number(falseNegatives));
    const n = tp + fn;
    if (!(tp >= 0 && fn >= 0 && n >= 100)) return { error: "at least 100 externally labelled cause events are required" };
    if (!/^[a-f0-9]{64}$/.test(String(labelSetSha256 || ""))) return { error: "labelSetSha256 must be a full SHA-256" };
    const p = tp / n, z = 1.959963984540054;
    const den = 1 + z * z / n;
    const lowerBound = ((p + z * z / (2 * n)) - z * Math.sqrt((p * (1 - p) + z * z / (4 * n)) / n)) / den;
    const row = { source: "external_labeled", n, truePositives: tp, falseNegatives: fn,
      recall: Number(p.toFixed(6)), lowerBound: Number(lowerBound.toFixed(6)),
      labelSetSha256, recordedAtMs: Date.now(), recordedBy: operator || "operator" };
    await A.col(A.COL.control).doc("control").set({ recallBenchmark: row }, { merge: true });
    await A.col(A.COL.audit).add({ action: "record_recall_benchmark", ...row,
      at: A.FV.serverTimestamp(), ...A.envelope({ created_by: "investorApi" }) });
    return { ok: true, recallBenchmark: row };
  },

  /* Regime inputs. Entered by the operator or written by a future adapter —
     COR3M has no free programmatic feed, so a manual value beats a fabricated
     one, and a stale value degrades sizing rather than silently passing. */
  async setRegime({ vix, vixMedian, cor3m, operator }) {
    const now = new Date().toISOString();
    const patch = { asOf: now, setBy: operator || "operator" };
    if (Number(vix) >= 5 && Number(vix) <= 100) {
      patch.vix = Number(vix); patch.vixHealthy = true; patch.vixFetchedAt = now; patch.vixSource = "operator";
    }
    if (Number(vixMedian) >= 5 && Number(vixMedian) <= 100) patch.vixMedian = Number(vixMedian);
    if (Number(cor3m) >= 0 && Number(cor3m) <= 100) {
      patch.cor3m = Number(cor3m); patch.corHealthy = true; patch.corFetchedAt = now; patch.corSource = "operator";
    }
    if (patch.vix == null && patch.cor3m == null) return { error: "no valid regime value supplied" };
    await A.col(A.COL.control).doc("regime").set(patch, { merge: true });
    return { regime: patch, gate: S.dispersionGate(patch.cor3m, {}) };
  },

  async openAccount({ accountId, startingNavUsd }) {
    const ctrl = await ctrlDoc();
    const id = String(accountId || ctrl.accountId || "paper-1");
    if (!/^[a-zA-Z0-9_-]{3,40}$/.test(id)) return { error: "accountId must be 3-40 letters, numbers, underscores, or hyphens" };
    const nav = Number(startingNavUsd) || 100000;
    if (!(nav >= 1000 && nav <= 100000000)) return { error: "startingNavUsd outside allowed range" };
    const r = await L.openAccount({
      accountId: id,
      startingNavUsd: nav,
      strategyVersion: ctrl.strategyVersion || require("./_investorStrategy").version,
    });
    const accountStart = r.startingNavCents != null ? L.fromCents(r.startingNavCents) : nav;
    await A.col(A.COL.control).doc("control").set({ accountId: id,
      safetyEpoch: null, ...STATE.legacyPatch(STATE.STATES.OBSERVATION, ctrl), ladderStreak: 0,
      highWaterMarkUsd: accountStart, startOfDayNavUsd: accountStart, startOfDayNavDate: null,
      accountChangedAt: A.FV.serverTimestamp() }, { merge: true });
    return { ...r, safetyReset: true, dryRun: true, mode: "research" };
  },

  async ledger({ accountId, limit }) {
    const ctrl = await ctrlDoc();
    const id = accountId || ctrl.accountId || "paper-1";
    /* Equality-only, sorted in memory — where+orderBy needs a composite index
       that does not exist and threw FAILED_PRECONDITION in real Firestore. */
    const snap = await A.col(A.COL.ledger).where("accountId", "==", id).limit(600).get();
    const rows = [];
    const lim = Math.min(Number(limit) || 50, 200);
    const all = [];
    snap.forEach((d) => all.push(d.data()));
    all.sort((a, b) => String(b.postedAt || "") < String(a.postedAt || "") ? -1 : 1);
    for (const x of all.slice(0, lim)) {
      rows.push({ txnId: x.txnId, kind: x.kind, legs: x.legs, meta: x.meta,
                  postedAt: x.postedAt && x.postedAt.toDate ? x.postedAt.toDate().toISOString() : null });
    }
    return { ok: true, accountId: id, rows,
             balances: await L.balances(id), cost: await L.costMeter(id) };
  },

  async reconcile({ accountId, operator }) {
    const ctrl = await ctrlDoc();
    const id = accountId || ctrl.accountId || "paper-1";
    if (id !== (ctrl.accountId || "paper-1")) {
      return { error: "only the active paper account can be reconciled from the console" };
    }
    const reconciliation = await L.reconcileAccount(id, { context: "operator_diagnostic" });
    await A.col(A.COL.audit).add({ action: "reconcile_paper_ledger", accountId: id,
      pass: reconciliation.pass, operator: operator || "operator",
      at: A.FV.serverTimestamp(), ...A.envelope({ created_by: "investorApi" }) });
    return { ok: reconciliation.pass, reconciliation,
      note: reconciliation.pass
        ? "The ledger, lifecycle records, marks and computed NAV agree."
        : "New entries are frozen; exits and diagnostics remain available." };
  },

  async soakStatus({ accountId }) {
    const ctrl = await ctrlDoc();
    const id = accountId || ctrl.accountId || "paper-1";
    if (id !== (ctrl.accountId || "paper-1")) {
      return { error: "only the active paper account can be evaluated" };
    }
    return { ok: true, soak: await SOAK.status(id) };
  },

  /* No-trade reasons are the most valuable dataset this system produces. */
  async decisions({ limit, cycleId }) {
    if (cycleId != null && !/^\d{4}-\d{2}-\d{2}_\d{4}$/.test(String(cycleId))) {
      return { error: "invalid cycleId" };
    }
    let q = A.col(A.COL.decisions);
    q = cycleId ? q.where("cycleId", "==", cycleId) : q.orderBy("decisionAtMs", "desc");
    const snap = await q.limit(Math.min(Number(limit) || 60, 250)).get();
    const rows = []; snap.forEach((d) => rows.push(d.data()));
    const tally = {};
    for (const r of rows) for (const b of (r.noTradeReasons || [])) tally[b] = (tally[b] || 0) + 1;
    return { ok: true, rows, blockTally: tally };
  },

  async sources() {
    const snap = await A.col(A.COL.sourceState).limit(500).get();
    const state = {}; snap.forEach((d) => {
      const x = d.data();
      state[d.id] = {
        consecutiveFailures: x.consecutiveFailures || 0,
        lastError: x.lastError || null,
        lastSuccessAt: x.lastSuccessAt && x.lastSuccessAt.toDate ? x.lastSuccessAt.toDate().toISOString() : null,
      };
    });
    const universe = require("./_investorUniverse.js");
    const byReason = {};
    for (const row of universe.excludedTier || []) {
      const reason = row.reason || row.exclusionReason || "unstated";
      (byReason[reason] ||= []).push(row.symbol);
    }
    return { ok: true, registry: { ...E.SOURCES, ...IS.SOURCE_REGISTRY }, state,
      eligibility: {
        declared: universe.declaredTradeTierCount,
        eligible: (universe.tradeTier || []).length,
        excluded: (universe.excludedTier || []).length,
        policyVersion: (universe.enforcement || {}).exclusionPolicyVersion || null,
        byReason: Object.entries(byReason)
          .map(([reason, symbols]) => ({ reason, count: symbols.length,
            symbols: symbols.sort() }))
          .sort((a, b) => b.count - a.count),
      } };
  },

  async intelligence({ symbol, limit }) {
    const requested = symbol == null ? null : String(symbol).trim().toUpperCase();
    if (requested && !SYMBOL.test(requested)) return { error: "invalid symbol" };
    const ctrl = await ctrlDoc();
    const strategy = require("./_investorStrategy");
    let snapshots = [];
    if (requested) {
      const snap = await A.col(A.COL.intelligence).doc(requested).get();
      if (snap.exists) snapshots.push(snap.data());
    } else {
      const snap = await A.col(A.COL.intelligence)
        .limit(Math.min(Number(limit) || 24, IS.MAX_FOCUS)).get();
      snapshots = snap.docs.map((d) => d.data());
    }
    const rows = snapshots.map((x) => ({ ...x,
      policy: I.decisionPolicy({ coverage: x.coverage, events: x.events,
        temporalContext: x.temporalContext,
        requireTemporalContext: strategy.parameters.requireTemporalContext === true,
        asOfMs: Date.now(), maxAgeHours: strategy.parameters.intelligenceMaxAgeHours,
        temporalMaxAgeHours: strategy.parameters.temporalMaxAgeHours }) }))
      .sort((a, b) => a.symbol.localeCompare(b.symbol));
    return { ok: true, watchlist: IS.configuredSymbols(ctrl),
      focus: ctrl.intelligenceFocus || [], rows,
      sourceMode: "authoritative_public_only",
      coverageMeaning: "Healthy bounded lanes are reported explicitly; complete never means every fact on the internet was found." };
  },

  async setIntelligenceWatchlist({ symbols, profiles, operator }) {
    if (!Array.isArray(symbols)) return { error: "symbols must be an array" };
    const cleaned = [...new Set(symbols.map((x) => String(x || "").trim().toUpperCase()))];
    if (!cleaned.length || cleaned.length > IS.MAX_WATCHLIST || cleaned.some((x) => !SYMBOL.test(x))) {
      return { error: `watchlist must contain 1-${IS.MAX_WATCHLIST} valid unique symbols` };
    }
    const ctrl = await ctrlDoc(), uMod = require("./_investorUniverse");
    const uSnap = await A.col(A.COL.universe).doc(ctrl.universeVersion || uMod.version).get();
    const universe = uSnap.exists ? uSnap.data() : uMod;
    const eligible = new Set([...(universe.tradeTier || []), ...(universe.researchTier || [])]
      .map((x) => x.symbol));
    const unknown = cleaned.filter((x) => !eligible.has(x));
    if (unknown.length) return { error: `symbol(s) outside the frozen eligible universe: ${unknown.join(", ")}` };
    const nextProfiles = { ...(ctrl.intelligenceProfiles || {}) };
    for (const symbol of cleaned) {
      const proposed = profiles && profiles[symbol] || {};
      const domains = [...new Set((proposed.officialDomains || []).map(IS.cleanDomain).filter(Boolean))].slice(0, 6);
      nextProfiles[symbol] = domains.length ? { officialDomains: domains,
        domainBasis: "operator_verified" } : (nextProfiles[symbol] || {});
    }
    const queue = await closeEntryQueue(ctrl.accountId || "paper-1",
      "company-intelligence watchlist changed; policy re-attestation required", operator);
    await A.col(A.COL.control).doc("control").set({ intelligenceSymbols: cleaned,
      intelligenceProfiles: nextProfiles, intelligenceCursor: 0,
      safetyEpoch: null, ...STATE.legacyPatch(STATE.STATES.OBSERVATION, ctrl),
      intelligenceWatchlistUpdatedAt: A.FV.serverTimestamp() }, { merge: true });
    await A.col(A.COL.audit).add({ action: "set_intelligence_watchlist", symbols: cleaned,
      verifiedDomainSymbols: cleaned.filter((x) => (nextProfiles[x].officialDomains || []).length),
      operator: operator || "operator", at: A.FV.serverTimestamp(),
      ...A.envelope({ created_by: "investorApi" }) });
    return { ok: true, symbols: cleaned, profiles: nextProfiles, queue,
      safetyReset: true, note: "All companies use the same SEC identity, sector/SIC routing, relationship graph and public-source policy." };
  },

  async universe() {
    const ctrl = await ctrlDoc();
    const uMod = require("./_investorUniverse.js");
    const snap = await A.col(A.COL.universe).doc(ctrl.universeVersion || uMod.version || "v1").get();
    if (snap.exists) return { ok: true, universe: snap.data(), source: "firestore" };
    return { ok: true, universe: require("./_investorUniverse.js"), source: "repo" };
  },

  /* ── Manual sell ───────────────────────────────────────────────────────
     The operator can decide, by hand, that a holding should go. That decision
     is honoured — but it is honoured the same way every other exit is, and
     that distinction is the whole point of this action.

     What this does NOT do is sell. It records an INTENT on the position. The
     position guard picks it up on its next pass, stamps the decision clock,
     attaches the market provenance in force at that moment, and lets
     M.firstEligibleBar choose the fill — the first bar that OPENS after the
     decision plus the feed delay. So a manual sell fills at a price nobody,
     including the operator clicking the button, could have known when they
     clicked it. Allowing an instant fill at the last displayed price would let
     a human trade on a 15-minute-old quote as if it were live and would make
     every number this desk reports about itself a lie.

     A manually closed trade is a real ledger fact and appears in the history,
     but _investorLadder.strictTradeAdmissible excludes it: it is the
     operator's judgement, not evidence about the strategy. */
  async requestSell({ symbol, note, operator }) {
    const ctrl = await ctrlDoc();
    const accountId = ctrl.accountId || "paper-1";
    const sym = String(symbol || "").trim().toUpperCase();
    if (!/^[A-Z][A-Z.\-]{0,9}$/.test(sym)) {
      return { ok: false, refused: "not a symbol this desk can act on" };
    }
    const ref = A.col(A.COL.positions).doc(`${accountId}_${sym}`);
    const snap = await ref.get();
    if (!snap.exists || snap.data().open !== true) {
      return { ok: false, refused: `nothing open in ${sym}` };
    }
    const p = snap.data();
    if (p.corporateActionPending) {
      return { ok: false, refused: `${sym} is held for corporate-action reconciliation`
        + " — its price basis is not trustworthy until that is resolved" };
    }
    const existingIntent = p.exitIntent && Number(p.exitIntent.decisionAtMs) > 0
      ? p.exitIntent : null;
    const already = p.manualExitRequest && p.manualExitRequest.status === "requested";
    const request = already ? p.manualExitRequest : {
      status: "requested", requestedAtMs: Date.now(),
      requestedAt: new Date().toISOString(),
      operator: operator || "operator",
      note: note ? String(note).slice(0, 200) : null,
      markAtRequestUsd: Number(p.lastMarkUsd) || null,
      entryPriceUsd: Number(p.entryPriceUsd != null ? p.entryPriceUsd : p.avgPrice) || null,
      qty: Number(p.qty) || null,
      armedAtMs: null, eligibleAfterMs: null, blockedReason: null };
    if (!already) {
      await ref.set({ manualExitRequest: request,
        updated_at: A.FV.serverTimestamp() }, { merge: true });
      await A.col(A.COL.audit).add({ action: "manual_sell_requested", symbol: sym,
        accountId, operator: operator || "operator",
        note: request.note, markUsd: request.markAtRequestUsd,
        at: A.FV.serverTimestamp(), ...A.envelope({ created_by: "investorApi" }) });
    }

    /* Nudge the guard so the request is acted on in seconds rather than
       waiting out the cadence. Best effort — the scheduled guard is the
       guarantee, this is only the courtesy. */
    let nudged = false;
    try {
      const operating = STATE.describe(ctrl);
      if (!operating.paused) {
        const K = require("./investorKick");
        const session = M.sessionState(new Date());
        const r = await K.dispatch("guard", { ...ctrl, enabled: true,
          mode: operating.stage, dryRun: !operating.paperLedger, killSwitch: false,
          accountId, cycleSeconds: Number(ctrl.cycleSeconds) || 300,
          guardSeconds: Number(ctrl.guardSeconds) || 60,
          guardSecondsClosed: Number(ctrl.guardSecondsClosed) || 900,
          evidenceEverySeconds: Number(ctrl.evidenceEverySeconds) || 900,
        }, session, { manual: true });
        nudged = !!(r && r.jobId);
      }
    } catch { /* the scheduled guard will pick it up */ }

    const session = M.sessionState(new Date());
    const delay = M.providerConfig(M.activeProvider().id).delayMinutes;
    return { ok: true, symbol: sym, request,
      alreadyRequested: already, nudged,
      existingExitArmed: !!existingIntent,
      existingExitReason: existingIntent ? existingIntent.reason || null : null,
      marketOpen: session.open === true,
      note: existingIntent
        ? `${sym} was already on its way out (${existingIntent.reason || "exit armed"}).`
          + " Your request is recorded; the armed exit fills first."
        : session.open === true
          ? `Sell recorded for ${sym}. The desk arms the exit on its next protection`
            + ` pass and fills it on the first price bar that opens after that —`
            + ` about ${delay + 5}–${delay + 20} minutes on this ${delay}-minute feed.`
            + " A paper fill is never taken at a price already on your screen."
          : `Sell recorded for ${sym}. The exchange is shut, so it arms now and`
            + " fills on the first eligible bar after the next open." };
  },

  /* Withdraw a manual sell that has not yet been armed. Once the guard has
     armed the exit the decision clock is running and the intent is immutable —
     un-arming it would let an operator cancel a decision after seeing the
     price it would have filled at, which is the same look-ahead the fill rule
     exists to prevent. */
  async cancelSell({ symbol, operator }) {
    const ctrl = await ctrlDoc();
    const accountId = ctrl.accountId || "paper-1";
    const sym = String(symbol || "").trim().toUpperCase();
    const ref = A.col(A.COL.positions).doc(`${accountId}_${sym}`);
    const snap = await ref.get();
    if (!snap.exists) return { ok: false, refused: `nothing open in ${sym}` };
    const p = snap.data();
    if (p.exitIntent && Number(p.exitIntent.decisionAtMs) > 0) {
      return { ok: false, refused: `the exit for ${sym} is already armed and its`
        + " decision clock is running — it cannot be withdrawn" };
    }
    if (!p.manualExitRequest || p.manualExitRequest.status !== "requested") {
      return { ok: true, noop: true, symbol: sym, note: `no pending sell request for ${sym}` };
    }
    await ref.set({ manualExitRequest: A.FV.delete(),
      updated_at: A.FV.serverTimestamp() }, { merge: true });
    await A.col(A.COL.audit).add({ action: "manual_sell_cancelled", symbol: sym,
      accountId, operator: operator || "operator", at: A.FV.serverTimestamp(),
      ...A.envelope({ created_by: "investorApi" }) });
    return { ok: true, symbol: sym, note: `Sell request for ${sym} withdrawn.` };
  },

  /* Withdraw an armed entry level. The level is a plan, not an order — no
     cash is reserved and nothing has been decided — so cancelling it costs
     nothing and is recorded so the next deep scan does not re-arm the name
     today. A level already struck has become an order and is handled there. */
  async cancelPlan({ symbol, operator }) {
    const ctrl = await ctrlDoc();
    const accountId = ctrl.accountId || "paper-1";
    const sym = String(symbol || "").trim().toUpperCase();
    if (!SYMBOL.test(sym)) return { ok: false, refused: "not a symbol this desk can act on" };
    const session = M.sessionState(new Date());
    const ST = require("./_investorStrike");
    const ref = A.col(A.COL.plans).doc(ST.planId(accountId, session.date, sym));
    const snap = await ref.get();
    if (!snap.exists) return { ok: false, refused: `no armed level for ${sym} today` };
    const p = snap.data();
    if (p.status !== "armed") return { ok: true, noop: true, symbol: sym, status: p.status,
      note: `the level for ${sym} is already ${p.status}` };
    /* Compare-and-set. Between the read above and this write, the one-minute
       strike pass may have struck this level; cancelling it then would erase a
       live order's audit row and tell the operator the level was withdrawn
       when it had in fact been bought. */
    const ST2 = require("./_investorStrike");
    const applied = await ST2.markPlan({ planId: ref.id },
      { status: "cancelled", cancelledAtMs: Date.now(), cancelledBy: operator || "operator" });
    if (!applied.applied) {
      return { ok: false, symbol: sym, status: applied.reason,
        refused: applied.reason === "struck"
          ? `${sym} was bought moments ago — the level is no longer pending. Sell it from What you own if you want out.`
          : `the level for ${sym} moved to ${applied.reason} before this could be withdrawn` };
    }
    await A.col(A.COL.audit).add({ action: "entry_plan_cancelled", symbol: sym, accountId,
      operator: operator || "operator", armBelowUsd: p.armBelowUsd, at: A.FV.serverTimestamp(),
      ...A.envelope({ created_by: "investorApi" }) });
    return { ok: true, symbol: sym, status: "cancelled",
      note: `Armed level for ${sym} withdrawn. It will not be re-armed today.` };
  },

  /* Start a cycle now.
     A scheduled Netlify function cannot be reached over HTTP, so without this
     the only way to start a cycle is to wait for cron — and if the schedule is
     missing from netlify.toml the system is silent forever with no symptom
     other than an empty dashboard. This is the operator's way to ask, and the
     only privileged thing it does is mint the same worker nonce the scheduler
     mints. Every safety decision still belongs to the worker, which re-reads
     the control document after this returns. */
  async runCycleNow({ operator, task }) {
    const wanted = ["cycle", "guard", "evidence", "archive"].includes(task) ? task : "cycle";
    const ctrl = await ctrlDoc();
    const operating = STATE.describe(ctrl);
    if (operating.paused) return { ok: false, refused: "the paper desk is paused" };

    /* One manual run at a time. Without this an impatient click becomes a
       queue of overlapping workers competing for the same job documents. */
    const lastAt = Number(ctrl.lastManualRunAtMs) || 0;
    const sinceMs = Date.now() - lastAt;
    if (lastAt && sinceMs < 60000) {
      return { ok: false, refused: `a manual run started ${Math.round(sinceMs / 1000)}s ago`,
        retryInSeconds: Math.ceil((60000 - sinceMs) / 1000) };
    }
    const manualStartedAtMs = Date.now();
    const controlRef = A.col(A.COL.control).doc("control");
    await controlRef.set({ lastManualRunAtMs: manualStartedAtMs }, { merge: true });

    const K = require("./investorKick");
    const session = M.sessionState(new Date());
    let result;
    try {
      result = await K.dispatch(wanted, {
        ...ctrl, enabled: true, mode: operating.stage,
        planMode: K.effectivePlanMode(ctrl),
        fullScanCadenceVersion: K.FULL_SCAN_CADENCE_VERSION,
        dryRun: !operating.paperLedger, killSwitch: false,
        accountId: ctrl.accountId || "paper-1",
        cycleSeconds: Number(ctrl.cycleSeconds) || 300,
        guardSeconds: Number(ctrl.guardSeconds) || 60,
        guardSecondsClosed: Number(ctrl.guardSecondsClosed) || 900,
        evidenceEverySeconds: Number(ctrl.evidenceEverySeconds) || 900,
      }, session, { manual: true });
    } catch (e) {
      result = { error: String(e && e.message || e).slice(0, 160), upstream: 0 };
    }

    const upstream = Number(result && result.upstream);
    const accepted = !!result && result.duplicateSlot !== true
      && upstream >= 200 && upstream < 300;
    if (!accepted) {
      /* A failed dispatch did not start work, so it must not consume the
         operator's one-minute manual-run allowance. Compare-and-set avoids
         clearing a newer click that raced this response. */
      try {
        await A.runTransaction(async (tx) => {
          const current = await tx.get(controlRef);
          if (current.exists
              && Number(current.data().lastManualRunAtMs) === manualStartedAtMs) {
            tx.set(controlRef, { lastManualRunAtMs: 0 }, { merge: true });
          }
        });
      } catch { /* reporting the dispatch failure is still more important */ }
      const refused = result && result.duplicateSlot
        ? "that manual scan was already claimed"
        : result && result.error
          ? `the scan worker could not be dispatched: ${result.error}`
          : `the scan worker refused the request${upstream ? ` (HTTP ${upstream})` : ""}`;
      try {
        await A.col(A.COL.audit).add({ action: "run_cycle_now_dispatch_failed", task: wanted,
          operator: operator || "operator", jobId: result && result.jobId || null,
          upstream: Number.isFinite(upstream) ? upstream : null, refused,
          at: A.FV.serverTimestamp(), ...A.envelope({ created_by: "investorApi" }) });
      } catch { /* best effort */ }
      return { ok: false, task: wanted, jobId: result && result.jobId || null,
        upstream: Number.isFinite(upstream) ? upstream : null, refused };
    }

    await A.col(A.COL.audit).add({ action: "run_cycle_now", task: wanted,
      operator: operator || "operator", jobId: result.jobId || null,
      upstream: result.upstream ?? null, at: A.FV.serverTimestamp(),
      ...A.envelope({ created_by: "investorApi" }) });
    return { ok: true, task: wanted, jobId: result.jobId || null,
      upstream: result.upstream ?? null,
      marketOpen: session.open === true,
      note: session.open === true
        ? "A cycle is running. The dashboard fills in as it finishes."
        : "A cycle is running. The exchange is shut, so it will re-read stored "
          + "bars and refresh evidence rather than fetch new prices." };
  },

  /* A suspected split is never applied from price shape alone. This explicit,
     authenticated reconciliation changes share units and per-share prices
     while preserving the dollar cost basis, then clears any false exit intent
     that may have been armed before quarantine. */
  async confirmSplit({ symbol, shareRatio, effectiveDate, sourceRef, operator }) {
    symbol = String(symbol || "").toUpperCase();
    if (!SYMBOL.test(symbol)) return { error: "valid symbol required" };
    const ratio = Number(shareRatio);
    if (!(ratio > 0.04 && ratio < 25)) return { error: "valid shareRatio required" };
    if (effectiveDate != null && !ISO_DATE.test(String(effectiveDate))) {
      return { error: "effectiveDate must be YYYY-MM-DD" };
    }
    const ctrl = await ctrlDoc();
    const accountId = ctrl.accountId || "paper-1";
    const ref = A.col(A.COL.positions).doc(`${accountId}_${symbol}`);
    const confirmedAtMs = Date.now();
    const main = await A.runTransaction(async (tx) => {
      const snap = await tx.get(ref);
      if (!snap.exists || !snap.data().open) return { error: "open paper position not found" };
      const position = snap.data();
      const pending = position.corporateActionPending;
      if (!pending) return { error: "position has no quarantined corporate action" };
      let patch;
      try {
        patch = CA.splitReconciliationPatch(position, pending, ratio, {
          effectiveDate: effectiveDate || null, operator: operator || "operator",
          confirmedAtMs, sourceRef: String(sourceRef || "").slice(0, 500) || null,
        });
      } catch (e) { return { error: String(e.message).slice(0, 180) }; }
      if (Number(pending.currentPrice) > 0 && L.validProvenance(pending.currentProvenance)) {
        patch.lastMarkUsd = Number(pending.currentPrice);
        patch.lastMarkAt = pending.currentMarkAt || null;
        patch.lastMarkProvenance = pending.currentProvenance;
      }
      patch.updated_at = A.FV.serverTimestamp();
      tx.set(ref, patch, { merge: true });
      return { ok: true, priorQty: Number(position.qty), qty: patch.qty,
        costBasisCents: Number(position.costBasisCents) || null };
    });
    if (!main.ok) return main;

    let shadowReconciled = 0;
    try {
      const shadows = await A.col(A.COL.shadowOpen).where("symbol", "==", symbol).limit(100).get();
      for (const doc of shadows.docs) {
        const row = doc.data(), pending = row.corporateActionPending;
        if (!pending) continue;
        try {
          const patch = CA.splitReconciliationPatch(row, pending, ratio, {
            effectiveDate: effectiveDate || null, operator: operator || "operator",
            confirmedAtMs, sourceRef: String(sourceRef || "").slice(0, 500) || null,
          });
          if (Number(pending.currentPrice) > 0 && L.validProvenance(pending.currentProvenance)) {
            patch.lastMarkPrice = Number(pending.currentPrice);
            patch.lastMarkAt = pending.currentMarkAt || null;
            patch.lastMarkProvenance = pending.currentProvenance;
          }
          patch.updatedAt = A.FV.serverTimestamp();
          await doc.ref.set(patch, { merge: true });
          shadowReconciled += 1;
        } catch {}
      }
    } catch {}
    await A.col(A.COL.audit).add({ action: "confirm_split_reconciliation",
      accountId, symbol, shareRatio: ratio, effectiveDate: effectiveDate || null,
      sourceRef: String(sourceRef || "").slice(0, 500) || null,
      operator: operator || "operator", main, shadowReconciled,
      at: A.FV.serverTimestamp(), ...A.envelope({ created_by: "investorApi" }) });
    return { ...main, symbol, shareRatio: ratio, shadowReconciled,
      note: "share units and per-share fields were rebased; dollar cost basis was preserved" };
  },

  async confirmCashDividend({ symbol, corporateActionId, positionLifecycleId, eligibleQty,
    perShareUsd, recordDate, payDate, sourceRef, operator }) {
    symbol = String(symbol || "").toUpperCase();
    if (!SYMBOL.test(symbol)) return { error: "valid symbol required" };
    const ctrl = await ctrlDoc();
    let result;
    try {
      result = await L.recordCashDividend({
        accountId: ctrl.accountId || "paper-1", symbol,
        corporateActionId: String(corporateActionId || ""),
        positionLifecycleId: String(positionLifecycleId || ""),
        eligibleQty: Number(eligibleQty), perShareUsd: Number(perShareUsd),
        recordDate: String(recordDate || ""), payDate: String(payDate || ""),
        sourceRef: String(sourceRef || "").slice(0, 500) || null,
        operator: operator || "operator",
      });
    } catch (e) {
      return { error: String(e && e.message || e).slice(0, 220) };
    }
    await A.col(A.COL.audit).add({ action: "confirm_cash_dividend",
      accountId: ctrl.accountId || "paper-1", symbol,
      corporateActionId: String(corporateActionId || ""),
      positionLifecycleId: String(positionLifecycleId || ""), result,
      operator: operator || "operator", at: A.FV.serverTimestamp(),
      ...A.envelope({ created_by: "investorApi" }) });
    return { ok: true, ...result,
      note: "cash and dividend income were posted as one balanced, idempotent ledger transaction" };
  },

  /* Paper learning mode. Values are stored as requested and CLAMPED at read
     time by the strategy module, so what the dashboard shows is what the cycle
     will actually use rather than what was typed. */
  async setPaperLearning({ operator, patch }) {
    const p = patch && typeof patch === "object" ? patch : {};
    const clean = { enabled: p.enabled === true,
      abstainOnMissingInfo: p.abstainOnMissingInfo === true };
    for (const key of Object.keys(ST.RELAX_LIMITS)) {
      if (p[key] == null) continue;
      const v = ST.clampRelax(key, p[key]);
      if (v != null) clean[key] = v;
    }
    const before = await ctrlDoc();
    let statePatch = {};
    if (!clean.enabled && STATE.describe(before).exploratoryAuto) {
      const demoted = STATE.transition(before, STATE.STATES.OBSERVATION, {
        source: "operator",
        reason: "paper learning disabled; exploratory automatic entries closed",
      });
      if (demoted.ok) statePatch = demoted.patch;
    }
    await A.col(A.COL.control).doc("control")
      .set({ paperLearning: clean, ...statePatch }, { merge: true });
    const ctrl = await ctrlDoc();
    const queue = !clean.enabled
      ? await closePaperLearningQueue(ctrl.accountId || "paper-1",
        "paper learning disabled by operator", operator)
      : null;
    await A.col(A.COL.audit).add({ action: "set_paper_learning",
      operator: operator || "operator", patch: clean, queue,
      at: A.FV.serverTimestamp(),
      ...A.envelope({ created_by: "investorApi" }) });
    const strategy = await activeStrategy(ctrl);
    const preview = ST.paperLearningConfig(strategy.parameters || ST.parameters, ctrl);
    return { ok: true, stored: clean, active: preview.active, queue,
      refused: preview.refused, applied: preview.applied,
      limits: ST.RELAX_LIMITS,
      note: preview.active
        ? "Relaxed gates apply to both observation-only and simulated-ledger paper decisions. Every decision still records what the strict calibration would have said."
        : (preview.refused || "Paper learning mode is off — the published calibration is in force.") };
  },

  /* Configure the market lane without exposing credentials back to the
     browser. delayed_sip is Alpaca's explicit consolidated 15-minute feed and
     is the recommended paper-execution source for Basic accounts. */
  async setMarketConfig({ operator, provider, feed, alpacaKeyId, alpacaSecretKey }) {
    /* Resolve a previously stored credential pair before deciding whether blank
       form fields mean "keep it" or "missing". */
    await M.loadMarketSettings({ force: true });
    const requestedProvider = String(provider || "").trim().toLowerCase();
    const requestedFeed = String(feed || (requestedProvider === "alpaca"
      ? "delayed_sip" : "iex")).trim().toLowerCase();
    const choice = M.normalizeMarketChoice(requestedProvider, requestedFeed);
    if (!choice.providerRecognised) {
      return { ok: false, error: "provider must be alpaca, massive, or manual" };
    }
    if (!choice.feedRecognised) {
      return { ok: false, error: "feed must be delayed_sip, iex, sip, or otc" };
    }
    if (requestedProvider === "alpaca"
        && !["delayed_sip", "iex", "sip"].includes(requestedFeed)) {
      return { ok: false, error: "the selected Alpaca feed is not supported" };
    }

    const keyId = typeof alpacaKeyId === "string" ? alpacaKeyId.trim() : "";
    const secretKey = typeof alpacaSecretKey === "string" ? alpacaSecretKey.trim() : "";
    if (!!keyId !== !!secretKey) {
      return { ok: false, error: "provide both Alpaca API key ID and secret, or leave both blank" };
    }
    const updatingCredentials = !!(keyId && secretKey);
    if (requestedProvider === "alpaca" && !updatingCredentials
        && !M.providerCredentialed("alpaca")) {
      return { ok: false,
        error: "Alpaca credentials are required for current-session delayed SIP prices" };
    }

    const patch = {
      provider: requestedProvider,
      feed: requestedFeed,
      /* Realtime SIP must be enabled only by an explicit entitlement outside
         this form; the Basic-plan-safe default remains false. */
      alpacaSipRealtime: false,
      ...(updatingCredentials
        ? { alpacaKeyId: keyId, alpacaSecretKey: secretKey } : {}),
      updatedAt: A.FV.serverTimestamp(),
      updatedBy: operator || "operator",
    };
    await A.col(A.COL.control).doc(M.MARKET_SETTINGS_DOC).set(patch, { merge: true });
    const settings = await M.loadMarketSettings({ force: true });
    const active = M.activeProvider();
    await A.col(A.COL.audit).add({
      action: "set_market_config", operator: operator || "operator",
      provider: requestedProvider, feed: requestedFeed,
      credentialPairUpdated: updatingCredentials,
      at: A.FV.serverTimestamp(), ...A.envelope({ created_by: "investorApi" }),
    });
    return {
      ok: true,
      configured: { provider: settings.provider, feed: settings.feed },
      active: { provider: active.id, feed: active.feed || null,
        delayMinutes: active.delayMinutes, consolidated: !!active.consolidated,
        liquidityEligible: active.liquidityEligible !== false,
        degradedFrom: active.degradedFrom || null, reason: active.reason || null },
      credentialsPresent: M.providerCredentialed("alpaca"),
      note: active.id === "alpaca" && active.feed === "delayed_sip"
        ? "Exact 15-minute delayed consolidated prices are active. Run a cycle to rebuild coverage."
        : "Market configuration saved.",
    };
  },

  /* Freeze a universe version so additions can never be backdated. */
  async freezeUniverse({ operator }) {
    const { frozen, report } = await B.resolveCiksAndFreeze();
    await A.col(A.COL.audit).add({ action: "verify_universe_freeze",
      operator: operator || "operator", version: frozen.version, contentHash: frozen.contentHash,
      at: A.FV.serverTimestamp(), ...A.envelope({ created_by: "investorApi" }) });
    return { ok: true, frozen: true, version: frozen.version,
      contentHash: frozen.contentHash, tradeTier: frozen.tradeTier.length, report };
  },

  /* Resolve tickers to CIKs from SEC's own authoritative map. A guessed CIK
     silently polls the WRONG filer, which is worse than no CIK at all — so the
     universe ships with them null and this fills them in from the source. */
  async resolveCiks({ operator }) {
    const { frozen, report } = await B.resolveCiksAndFreeze();
    return { ok: true, version: frozen.version, contentHash: frozen.contentHash,
      resolved: report.resolved, missing: report.missing, mismatched: report.mismatched,
      note: "SEC resolution was verified against the immutable frozen content hash" };
  },

  async health() {
    const settings = await M.loadMarketSettings({ force: true });
    const auth = await AUTH.loadAuthSecrets({ force: true });
    const p = M.activeProvider();
    return {
      ok: true,
      provider: { id: p.id, feed: p.feed || null, consolidated: !!p.consolidated,
                  liquidityEligible: p.liquidityEligible !== false,
                  exactFifteenMinuteDelay: p.id === "alpaca"
                    && p.feed === "delayed_sip" && p.delayMinutes === 15,
                  delayMinutes: p.delayMinutes, maxGrade: p.maxGrade,
                  degradedFrom: p.degradedFrom || null, reason: p.reason || null },
      env: {
        firebase: !!(process.env.FIREBASE_PRIVATE_KEY && process.env.FIREBASE_PROJECT_ID),
        openai: !!process.env.OPENAI_API_KEY,
      },
      /* Which invariant failed, not merely that one did. Names only — the
         fixture bodies are code, and the operator needs the label to act. */
      fixtures: (() => {
        try {
          const r = require("./_investorSelftest").runFixtures();
          return { pass: r.pass, passed: r.passed, total: r.total,
            fixtureHash: r.fixtureHash, schema: r.schema || null,
            failed: r.cases.filter((c) => !c.pass).map((c) => c.name) };
        } catch (e) { return { pass: false, error: String(e.message).slice(0, 200) }; }
      })(),
      /* What the CYCLE attested (written by the bootstrap from inside the
         worker) beside what this API process just ran. A different hash means
         the two deployed bundles are not the same code — a mixed upload. */
      attestation: await (async () => {
        try {
          const c = await ctrlDoc();
          return { commit: commitId(), fixturesPass: c.fixturesPass === true,
            fixturesCommit: c.fixturesCommit || null, fixtureHash: c.fixtureHash || null,
            bootstrapVersion: c.bootstrapVersion || null,
            fixtureFailures: (c.fixtureFailures || []).map((f) => f.name || f),
            checkedAt: c.fixturesCheckedAt && c.fixturesCheckedAt.toDate
              ? c.fixturesCheckedAt.toDate().toISOString() : null };
        } catch (e) { return { error: String(e.message).slice(0, 200) }; }
      })(),
      /* Presence and origin only. No secret value is ever returned here. */
      secrets: {
        passcode: !!(auth.passcode || "").length,
        sessionSecret: (auth.sessionSecret || "").length >= 32,
        authSource: auth.source,
        alpacaCredentials: M.providerCredentialed("alpaca"),
        credentialSource: settings.credentialSource,
        ...(auth.note ? { authNote: auth.note } : {}),
      },
      /* provider/feed are operator settings in InvestorAI_Control/marketConfig,
         not environment variables; `source` says which layer answered. */
      marketConfig: { provider: settings.provider, feed: settings.feed,
        source: settings.source, document: `${A.COL.control}/${M.MARKET_SETTINGS_DOC}`,
        credentialSource: settings.credentialSource || "unset",
        credentialNames: settings.credentialNames || null,
        credentialLookup: settings.credentialLookup || null,
        ...(settings.providerNote ? { providerNote: settings.providerNote } : {}),
        ...(settings.note ? { note: settings.note } : {}),
        ...(settings.feedNote ? { feedNote: settings.feedNote } : {}) },
      session: M.sessionState(new Date()),
      openaiCeilingUsd: O.DAILY_USD_CEILING,
    };
  },
};

/* ── handler ───────────────────────────────────────────────────────────── */
exports.handler = async (event) => {
  if (event.httpMethod === "OPTIONS") {
    return { statusCode: 204, headers: AUTH.corsHeaders(event), body: "" };
  }
  if (event.httpMethod !== "POST") {
    return AUTH.json(event, 405, { error: "POST only" });
  }
  if ((event.body || "").length > MAX_BODY) {
    return AUTH.json(event, 413, { error: "request too large" });
  }
  /* Resolve the operator secrets before the guard reads them. A failed read
     leaves requireOperator answering AUTH_NOT_CONFIGURED, never open. Nothing
     may escape this handler as an unhandled rejection: the platform turns
     that into an opaque 502 with no body, which is undiagnosable from the
     console. */
  try { await AUTH.loadAuthSecrets(); }
  catch (e) {
    console.error("investorApi loadAuthSecrets", String(e && e.message || e));
    return AUTH.json(event, 500, { error: `auth secrets unavailable: ${String(e && e.message || e).slice(0, 160)}` });
  }

  let body = {};
  try { body = JSON.parse(event.body || "{}"); }
  catch { return AUTH.json(event, 400, { error: "invalid JSON body" }); }

  const guard = AUTH.requireOperator(event, body);
  if (!guard.ok) return guard.response;

  const action = String(body.action || "dashboard");
  const fn = ACTIONS[action];
  if (!fn) return AUTH.json(event, 400, { error: `unknown action "${action}"` });

  try {
    const out = await fn({ ...body, operator: guard.subject || "operator" });
    /* The auth token is returned as authToken, NEVER as "session".
       The dashboard action already returns `session` meaning the MARKET
       session state, and the collision meant that on the second request the
       client overwrote its bearer token with a market-state object, sent
       "[object Object]" as the header, and locked itself out. */
    const extra = guard.session ? { authToken: guard.session } : {};
    const status = out && out.ok === false ? 409 : (out && out.error ? 400 : 200);
    const full = { ...out, ...extra };
    /* LAST RESORT. A body over the platform's 6 MB cap is not returned at all:
       the runtime answers RequestEntityTooLarge and the browser sees an opaque
       502 with no way to tell an oversized payload from a crash. Answer with a
       small, honest body instead — the operator can still sign in and every
       other action keeps working. */
    const bytes = payloadBytes(full);
    if (bytes > RESPONSE_HARD_LIMIT_BYTES) {
      console.error("investorApi oversized response", AUTH.redact({ action, bytes }));
      /* 413, not 200: the console's error path shows `refused` verbatim, so the
         operator reads the actual reason at the sign-in gate instead of an
         empty desk or an opaque 502. */
      return AUTH.json(event, 413, { ...extra, ok: false, action, responseBytes: bytes,
        refused: `the ${action} response came to ${(bytes / 1e6).toFixed(1)} MB, over the `
          + `${(RESPONSE_HARD_LIMIT_BYTES / 1e6).toFixed(1)} MB platform limit, so it was not sent`,
        payloadTrimmed: { bytes, limit: RESPONSE_HARD_LIMIT_BYTES, dropped: ["the entire response body"],
          note: "the backend is healthy; this response could not be delivered at its full size" } });
    }
    return AUTH.json(event, status, full);
  } catch (e) {
    console.error("investorApi", action, AUTH.redact({ error: e.message, stack: (e.stack || "").slice(0, 300) }));
    return AUTH.json(event, 500, { error: String(e.message).slice(0, 200), action });
  }
};

exports.ACTIONS = ACTIONS;
/* Exported for the runtime fixtures: response bounding is what keeps an
   oversized dashboard from reaching the browser as an opaque 502. */
exports.dashboardCandidateRow = dashboardCandidateRow;
exports.minimalCandidateRow = minimalCandidateRow;
exports.boundDashboardPayload = boundDashboardPayload;
exports.payloadBytes = payloadBytes;
exports.DASHBOARD_CANDIDATE_OMIT = DASHBOARD_CANDIDATE_OMIT;
exports.RESPONSE_HARD_LIMIT_BYTES = RESPONSE_HARD_LIMIT_BYTES;
/* Exported for the runtime fixtures: the console's whole history view is
   this projection, so it is tested against real ledger field names. */
exports.closedTradeForUi = closedTradeForUi;
exports.liveEvaluatedCount = liveEvaluatedCount;
exports.fullScanIntegrity = fullScanIntegrity;
/* Not an HTTP action. Exposed only so the deployed runtime attestation can
   inspect the exact queue-draining implementation after esbuild bundling,
   without assuming the original source file still exists on disk. */
exports._closeEntryQueueForAttestation = closeEntryQueue;
exports.config = {
  path: "/.netlify/functions/investorApi",
  rateLimit: { windowLimit: 60, windowSize: 60, aggregateBy: ["ip", "domain"] },
};
