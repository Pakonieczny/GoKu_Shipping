/*  netlify/functions/investorIngest-background.js  (fund-manager-v1)
 *  ---------------------------------------------------------------------------
 *  Investor AI — the bounded evidence ingest job (blueprint §11.1, §6.1; D-11).
 *
 *  ONE OVERNIGHT PASS, NOT A ROLLING RESCAN
 *  ------------------------------------------------------------------------
 *  The legacy evidence sweep ran continuously all day on a five-minute clock,
 *  four companies at a time — roughly three to four full passes per day that
 *  the target design never asks for, and 6.3 hours per pass against a 6-hour
 *  freshness window. This job runs ONE full-roster pass per night at
 *  perSweep 8, scheduled by investorKick so that the pass COMPLETES
 *  immediately before the 08:30 ET evidence freeze; intraday evidence
 *  arrives through the material-event route, never through a daytime rescan.
 *
 *  SEGMENTS, CHECKPOINTS, ONE RUN LEASE
 *  ------------------------------------------------------------------------
 *  A pass is one logical run (runId = ingest_<account>_<tradingDate>) that
 *  spans many 15-minute invocations. Each invocation claims the job attempt
 *  with an atomically consumed nonce, claims the run-scoped lease (D-10),
 *  processes up to perSweep companies while it has invocation budget,
 *  checkpoints the roster cursor, and either yields (the next kick
 *  re-dispatches it) or completes the pass. Per-company elapsed time is
 *  recorded so the pass duration is a MEASUREMENT (§6.1), and the completed
 *  pass reports the oldest card age at the freeze.
 *
 *  The per-company work is _investorIntelligence.refreshCompanyEvidence —
 *  the same routine the legacy sweep used — run under one sweep context so
 *  global sources are fetched once per segment and fanned out (D-6). This
 *  job decides nothing about investment merit.
 * ---------------------------------------------------------------------------
 */

"use strict";

const A = require("./_investorAdmin");
const AUTH = require("./_investorAuth");
const M = require("./_investorMarket");
const JOBS = require("./_investorJobs");
const POLICY = require("./_investorPolicy");
const IS = require("./_investorIntelligenceSources");
const I = require("./_investorIntelligence");
const B = require("./_investorBootstrap");
const { redact } = require("./_investorAuth");
const FUND = require("./_investorFundamentals");
const ROUTER = require("./_investorEventRouter");

const FN_NAME = "investorIngest-background";
const TASK = "ingest";
const PER_SEGMENT = POLICY.CUTOFFS_ET.ingest.perSweep;               // 8 (§6.1 sanity check: 8 × 110 s < 15 min)
const COMPANY_BUDGET_MS = Math.max(30000, Math.min(150000,
  Number(process.env.INVESTOR_INTELLIGENCE_COMPANY_BUDGET_MS) || POLICY.CUTOFFS_ET.ingest.worstCaseCompanySeconds * 1000));
const MIN_REMAINING_FOR_COMPANY_MS = COMPANY_BUDGET_MS + 20000;
const ELAPSED_HISTORY_MAX = 400;

async function controlDoc() {
  const s = await A.col(A.COL.control).doc("control").get();
  return s.exists ? s.data() : {};
}
async function loadUniverse(version) {
  const bundled = require("./_investorUniverse.js");
  if (!version) return bundled;
  try {
    const s = await A.col(A.COL.universe).doc(version).get();
    if (s.exists && Array.isArray(s.data().tradeTier)) return s.data();
  } catch {}
  return bundled;
}
async function loadStrategy(version) {
  const bundled = require("./_investorStrategy.js");
  if (!version) return bundled;
  try {
    const s = await A.col(A.COL.strategies).doc(version).get();
    if (s.exists && s.data().parameters) return s.data();
  } catch {}
  return bundled;
}

/** PURE. The roster a pass covers: every eligible symbol, plus every held or
 *  pending symbol (managed off-roster names stay managed until flat), in a
 *  deterministic order rotated by the pass so no name is always last. */
function passRoster({ universe, positions = [], pendingOrders = [], rotation = 0 }) {
  const eligible = (universe.tradeTier || []).map((r) => r.symbol);
  const held = positions.filter((p) => p && p.open).map((p) => p.symbol);
  const pending = pendingOrders.map((o) => o && o.symbol).filter(Boolean);
  const seen = new Set(), ordered = [];
  for (const s of [...eligible, ...held, ...pending]) { if (s && !seen.has(s)) { seen.add(s); ordered.push(s); } }
  const n = ordered.length;
  if (!n) return [];
  const start = ((Number(rotation) || 0) % n + n) % n;
  return [...ordered.slice(start), ...ordered.slice(0, start)];
}

/** PURE. Elapsed statistics for the pass so far. */
function elapsedStats(entries) {
  const ms = entries.map((e) => Number(e.ms)).filter(Number.isFinite).sort((a, b) => a - b);
  const q = (p) => ms.length ? ms[Math.min(ms.length - 1, Math.floor(p * ms.length))] : null;
  return { count: ms.length, p50Ms: q(0.5), p95Ms: q(0.95), maxMs: ms.length ? ms[ms.length - 1] : null,
    totalMs: ms.reduce((a, b) => a + b, 0), failed: entries.filter((e) => e.ok === false).length };
}

/** One invocation's worth of work. Returns {yielded, checkpoint} or {summary}. */
async function runIngestSegment(claim) {
  const ctrl = await controlDoc();
  const accountId = ctrl.accountId || "paper-1";
  const [universe, strategy] = await Promise.all([loadUniverse(ctrl.universeVersion), loadStrategy(ctrl.strategyVersion)]);
  const earningsWindows = await B.readEarnings().catch(() => ({}));
  const payload = claim.payload || {};
  const tradingDate = payload.tradingDate || M.sessionState(new Date()).date;

  /* First segment of the pass: freeze the roster order in the checkpoint so
     later segments walk the same list even if positions change. */
  let cp = claim.checkpoint;
  if (!cp || cp.stage !== "roster" || !cp.data || !Array.isArray(cp.data.order)) {
    const [positionSnap, orderSnap] = await Promise.all([
      A.col(A.COL.positions).where("accountId", "==", accountId).where("open", "==", true).get(),
      A.col(A.COL.orders).where("accountId", "==", accountId).where("status", "in", ["proposed", "approved"]).get().catch(() => ({ docs: [] })),
    ]);
    const positions = positionSnap.docs.map((d) => d.data());
    const pendingOrders = (orderSnap.docs || []).map((d) => d.data());
    const rotation = Number(String(tradingDate).replace(/-/g, "")) || 0;
    const order = passRoster({ universe, positions, pendingOrders, rotation });
    cp = { stage: "roster", cursor: 0, data: { passId: claim.runId || JOBS.runIdFor({ task: TASK, accountId, tradingDate }),
      tradingDate, order, total: order.length, eligibleCount: (universe.tradeTier || []).length,
      universeVersion: universe.version || ctrl.universeVersion || null,
      startedAtMs: Date.now(), segments: 0, elapsed: [], completed: 0, failed: 0 } };
    await JOBS.checkpoint(claim, cp);
    await A.col(A.COL.control).doc("control").set({ ingestPassState: { passId: cp.data.passId, tradingDate,
      cursor: 0, total: order.length, startedAtMs: cp.data.startedAtMs, updatedAtMs: Date.now(), status: "running" } }, { merge: true });
  }
  const data = cp.data;
  const bySymbol = Object.fromEntries([...(universe.tradeTier || []), ...(universe.researchTier || [])].map((r) => [r.symbol, r]));
  let cursor = Number(cp.cursor) || 0;
  const batch = data.order.slice(cursor, cursor + PER_SEGMENT);
  const sweep = IS.createSweep({ companies: batch });
  data.segments = (Number(data.segments) || 0) + 1;

  for (const symbol of batch) {
    if (JOBS.segmentBudgetRemainingMs(claim) < MIN_REMAINING_FOR_COMPANY_MS) break;
    const started = Date.now();
    let entry;
    try {
      const refreshed = await I.refreshCompanyEvidence({ symbol, rosterRow: bySymbol[symbol] || { symbol },
        focusReason: (universe.tradeTier || []).some((r) => r.symbol === symbol) ? "eligible_roster" : "managed_position",
        ctrl, strategy, earningsWindows, sweep });
      entry = { symbol, ok: true, ms: refreshed.elapsedMs, coverageComplete: refreshed.result.coverageComplete === true,
        dossierHash: refreshed.result.dossierHash || null };
      /* One overnight sweep (D-11): the numerical filing plane refreshes here,
         once per name per pass; an unchanged payload is a manifest check only. */
      if (refreshed.cik) {
        try {
          const facts = await FUND.ingestCompanyFacts({ cik: refreshed.cik });
          entry.facts = { newVersions: facts.newVersions || 0, unchanged: facts.unchanged === true, rejected: (facts.rejected || []).length };
        } catch (e) { entry.facts = { error: String(e.code || e.message).slice(0, 80) }; }
      }
      /* New filing versions route once: high-impact classes pause an
         unfilled entry and enqueue one event revision; everything else is a
         recorded delta for the next Manager Meeting. */
      let routed = 0, highImpact = 0;
      for (const v of refreshed.newVersions || []) {
        try {
          const r = await ROUTER.routeEvidenceEvent({ symbol, accountId, document: v });
          if (r.isNewVerifiedDelta) routed += 1;
          if (r.route && r.route.action === "high_impact") highImpact += 1;
        } catch (e) { entry.routeError = String(e.code || e.message).slice(0, 80); }
      }
      entry.routed = routed; entry.highImpact = highImpact;
      data.completed += 1;
    } catch (e) {
      entry = { symbol, ok: false, ms: Date.now() - started, error: String(e.code || e.message).slice(0, 120) };
      data.failed += 1;
    }
    data.elapsed = [...(data.elapsed || []), entry].slice(-ELAPSED_HISTORY_MAX);
    cursor += 1;
    await JOBS.checkpoint(claim, { stage: "roster", cursor, data,
      progress: { completed: cursor, total: data.total, currentItem: symbol } });
  }
  const scope = IS.assertSweepBudget(sweep);
  if (!scope.ok) {
    console.error("investorIngest: source scope breach", JSON.stringify(redact(scope.alert)));
    try { await A.col(A.COL.control).doc("control").set({ sourceScopeBreach: scope.alert }, { merge: true }); } catch {}
  }
  await A.col(A.COL.control).doc("control").set({ ingestPassState: { passId: data.passId, tradingDate: data.tradingDate,
    cursor, total: data.total, startedAtMs: data.startedAtMs, updatedAtMs: Date.now(),
    status: cursor < data.total ? "running" : "complete", segments: data.segments } }, { merge: true });

  if (cursor < data.total) {
    return { yielded: true, reason: "segment_batch_complete", checkpoint: { stage: "roster", cursor, data } };
  }
  const stats = elapsedStats(data.elapsed || []);
  const completedAtMs = Date.now();
  const freezeMs = M.nyWallClockToUtcMs(data.tradingDate, POLICY.CUTOFFS_ET.evidenceFreezeMin);
  const summary = {
    kind: "ingest_pass", passId: data.passId, tradingDate: data.tradingDate, accountId,
    universeVersion: data.universeVersion, eligibleCount: data.eligibleCount,
    companies: data.total, completed: data.completed, failed: data.failed, segments: data.segments,
    startedAtMs: data.startedAtMs, completedAtMs, elapsedMs: completedAtMs - data.startedAtMs,
    perCompany: stats, perSegment: PER_SEGMENT,
    /* §6.1: the oldest card at the freeze is the pass duration; inside the
       freshness window or not is a measurement, never an estimate. */
    freshness: { windowHours: POLICY.CUTOFFS_ET.ingest.freshnessWindowHours,
      oldestCardAgeAtFreezeSeconds: Number.isFinite(freezeMs) && freezeMs >= completedAtMs
        ? Math.round((freezeMs - data.startedAtMs) / 1000) : Math.round((completedAtMs - data.startedAtMs) / 1000),
      finishedBeforeFreeze: Number.isFinite(freezeMs) ? completedAtMs <= freezeMs : null,
      insideWindow: (completedAtMs - data.startedAtMs) <= POLICY.CUTOFFS_ET.ingest.freshnessWindowHours * 3600e3 },
    sourceScope: { ok: scope.ok, budget: scope.budget, breach: scope.ok ? null : scope.alert },
    failures: (data.elapsed || []).filter((e) => e.ok === false).slice(0, 40),
  };
  await A.col(A.COL.control).doc("control").set({ lastIngestPass: summary, lastIngestAt: completedAtMs,
    lastIngestTradingDate: data.tradingDate }, { merge: true });
  return { summary };
}

exports.handler = async (event) => {
  let body = {};
  try { body = JSON.parse(event.body || "{}"); } catch { return { statusCode: 400, body: JSON.stringify({ error: "invalid JSON" }) }; }
  const { jobId, task, nonce, payload = {} } = body;
  if (task !== TASK || !jobId) return { statusCode: 400, body: JSON.stringify({ error: "invalid job shape" }) };
  await AUTH.loadAuthSecrets();
  const claimed = await JOBS.claimOnce({ jobId, task: TASK, targetFunction: FN_NAME, token: nonce, payload });
  if (!claimed.claimed) {
    return { statusCode: claimed.httpStatus || 409, body: JSON.stringify({ ok: false, reason: claimed.reason }) };
  }
  const claim = claimed.claim;
  await M.loadMarketSettings();
  try {
    const runId = claim.runId || claim.payload.runId || JOBS.runIdFor({ task: TASK, accountId: claim.payload.accountId || "paper-1",
      tradingDate: claim.payload.tradingDate || M.sessionState(new Date()).date });
    const lease = await JOBS.claimRunLease(claim, { runId });
    if (!lease.claimed) {
      await JOBS.yieldSegment(claim, { reason: `run_in_flight:${lease.heldBy || "unknown"}`, resumeAtMs: Date.now() + 60000 });
      return { statusCode: 202, body: JSON.stringify({ ok: true, yielded: true, reason: "run_in_flight" }) };
    }
    const out = await runIngestSegment(claim);
    if (out.yielded) {
      await JOBS.yieldSegment(claim, { reason: out.reason, checkpoint: out.checkpoint, resumeAtMs: Date.now() + 5000 });
      return { statusCode: 202, body: JSON.stringify({ ok: true, yielded: true, cursor: out.checkpoint.cursor, total: out.checkpoint.data.total }) };
    }
    await JOBS.complete(claim, out.summary);
    console.log("investorIngest pass complete", JSON.stringify(redact({ ...out.summary, failures: undefined })));
    return { statusCode: 200, body: JSON.stringify({ ok: true, summary: out.summary }) };
  } catch (e) {
    console.error("investorIngest segment failed", redact({ jobId, error: e.message, stack: (e.stack || "").slice(0, 400) }));
    await JOBS.failClosed(claim, { code: "INGEST_SEGMENT_FAILED", message: e.message, retryable: true }).catch(() => ({}));
    return { statusCode: 500, body: JSON.stringify({ ok: false, error: String(e.message).slice(0, 200) }) };
  }
};

exports.FN_NAME = FN_NAME;
exports.PER_SEGMENT = PER_SEGMENT;
exports.passRoster = passRoster;
exports.elapsedStats = elapsedStats;
exports.runIngestSegment = runIngestSegment;
