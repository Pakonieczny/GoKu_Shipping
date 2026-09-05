/*  netlify/functions/investorPostclose-background.js  (fund-manager-v1)
 *  ---------------------------------------------------------------------------
 *  Investor AI — the post-close pass (blueprint §5.1 post-close row, §11.1,
 *  §12.1, §16.1 steps 1–4). Runs once per trading day after the official
 *  close and the finalization buffer. It asks no model anything and trades
 *  nothing: it marks, attributes, freezes forecasts, resolves what the
 *  point-in-time clock can resolve, aggregates KPIs and derives alerts.
 *
 *  Order of work (each step bounded and recorded even when a later one fails):
 *    1. final marks     — every open position marked at the session's final
 *                         daily close from the stored daily series;
 *    2. NAV             — one final-mark NAV row for the account-day;
 *    3. freeze          — a forecast/outcome record for every decision the
 *                         morning Manager Meeting persisted for this date;
 *    4. resolve         — pending forecasts advanced by the bars that now
 *                         exist; matched counterfactuals per run/horizon;
 *    5. KPI             — the kpi-daily.v1 row from the day's inputs;
 *    6. alerts          — typed alerts derived from the underlying state.
 *  `Control.lastPostcloseDate` flips only when the pass completes.
 * ---------------------------------------------------------------------------
 */

"use strict";

/* invariant: the post-close pass never loads the model gateway (measured
   against what this module's own imports bring in; the fixture set walks
   the source graph as the second lock). */
const preloadedModules = new Set(Object.keys(require.cache));
const A = require("./_investorAdmin");
const AUTH = require("./_investorAuth");
const M = require("./_investorMarket");
const JOBS = require("./_investorJobs");
const LEARN = require("./_investorLearning");
const KPI = require("./_investorKpi");
const ALERTS = require("./_investorAlerts");
const PORTFOLIO = require("./_investorPortfolio");
const H = require("./_investorHistory");
const NAV = require("./_investorNav");
const { redact } = require("./_investorAuth");

const FN_NAME = "investorPostclose-background";
const TASK = "postclose";
const BENCHMARK = "SPY";
const MAX_MARK_SYMBOLS = 200;
if (Object.keys(require.cache).some((k) => !preloadedModules.has(k) && /_investorOpenai\.js$/.test(k))) throw new Error("postclose must not load the model gateway");

function lazy(m) { try { return require(m); } catch { return null; } }
function rows(snap) { const out = []; if (snap && typeof snap.forEach === "function") snap.forEach((d) => out.push(d.data())); return out; }
function decode(d) { const SC = lazy("./_investorStorageCodec"); if (!SC || !d || !d.contentHash) return d; try { return SC.decode(d); } catch { return d; } }
function microsOfUsd(v) { const n = Number(v); return Number.isFinite(n) && n > 0 ? String(Math.round(n * 1e6)) : null; }
function minorToUsd(minor) { return Number(BigInt(String(minor || 0))) / 100; }
async function controlDoc(D) { const s = await D.col(D.COL.control).doc("control").get(); return s.exists ? s.data() : {}; }

/** The session's final daily bar for a symbol: the bar dated `tradingDate`, else the last bar on or before it. */
function finalBar(series, tradingDate) {
  const bars = (series || []).filter((b) => b && b.date && String(b.date) <= String(tradingDate) && Number(b.c) > 0);
  if (!bars.length) return null;
  bars.sort((a, b) => String(a.date).localeCompare(String(b.date)));
  const last = bars[bars.length - 1];
  return { ...last, exact: String(last.date) === String(tradingDate) };
}

/* 1. final marks ───────────────────────────────────────────────────────── */
async function markPositions({ D, accountId, tradingDate, seriesReader, nowMs }) {
  const positions = await PORTFOLIO.readPositions(accountId, { admin: D });
  const out = { marked: 0, unmarked: [], closeBySymbol: {}, positions: positions.length };
  for (const p of positions.slice(0, MAX_MARK_SYMBOLS)) {
    let series = [];
    try { series = (await seriesReader(p.symbol)).series || []; } catch { series = []; }
    const bar = finalBar(series, tradingDate);
    if (!bar) { out.unmarked.push(p.symbol); continue; }
    const closeMicros = microsOfUsd(bar.c);
    out.closeBySymbol[p.symbol] = closeMicros;
    await D.col(D.COL.positions).doc(`${accountId}_${p.symbol}`).set({ lastMarkUsd: Number(bar.c), lastMarkAt: `${bar.date}T20:00:00.000Z`, lastMarkMicros: closeMicros,
      finalMark: { tradingDate, date: bar.date, closeMicros, exact: bar.exact, source: "daily_close", atMs: nowMs } }, { merge: true });
    out.marked += 1;
  }
  return out;
}

/* 2. NAV ───────────────────────────────────────────────────────────────── */
async function recordNav({ D, accountId, tradingDate, snapshot, nowMs, real }) {
  const finalMark = { source: "postclose", tradingDate, atMs: nowMs, navMinor: snapshot.navMinor, cashMinor: snapshot.settledCashMinor, reservedMinor: snapshot.reservedMinor,
    investedMinor: snapshot.investedMinor, startingNavMinor: snapshot.startingNavMinor || null, openPositions: snapshot.aggregates.openPositions, portfolioVersion: snapshot.versions.portfolioVersion, contentHash: snapshot.contentHash };
  await D.col(D.COL.navMarks).doc(`${accountId}_${tradingDate}`).set({ accountId, date: tradingDate, finalMark, engineVersion: "manager" }, { merge: true });
  if (real) {
    /* the legacy chart series learns the same mark as a point (display only) */
    const unreal = snapshot.positions.reduce((n, p) => n + Number(BigInt(p.unrealisedMinor || 0)), 0) / 100;
    try { await NAV.record(accountId, { tMs: nowMs, navUsd: minorToUsd(snapshot.navMinor), cashUsd: minorToUsd(snapshot.settledCashMinor), reservedUsd: minorToUsd(snapshot.reservedMinor),
      investedUsd: minorToUsd(snapshot.investedMinor), unrealisedUsd: unreal, realisedUsd: null, openPositions: snapshot.aggregates.openPositions, winnersUsd: null, losersUsd: null,
      startingNavUsd: snapshot.startingNavMinor ? minorToUsd(snapshot.startingNavMinor) : null }, { source: "postclose" }); } catch (e) { return { ...finalMark, legacyPointError: String(e.message).slice(0, 120) }; }
  }
  return finalMark;
}

/* 3. freeze the day's decisions as outcome records ─────────────────────── */
async function freezeDecisions({ D, accountId, tradingDate, control, seriesReader, closeBySymbol, benchmarkCloseMicros, nowMs }) {
  const run = control.lastManagerRun || null;
  if (!run || run.tradingDate !== tradingDate) return { skipped: true, reason: run ? "manager_run_not_for_date" : "no_manager_run", managerRunId: run ? run.managerRunId : null };
  if (run.status !== "complete") return { skipped: true, reason: `manager_run_${run.status || "unknown"}`, managerRunId: run.managerRunId };
  const decisions = rows(await D.col(D.COL.managerDecisions).where("managerRunId", "==", run.managerRunId).get()).map(decode).filter((d) => d && d.symbol);
  if (!decisions.length) return { skipped: true, reason: "no_decision_rows", managerRunId: run.managerRunId };
  /* proposals and envelopes travel with the committed plan(s) of the run */
  const plans = rows(await D.col(D.COL.portfolioPlans).where("managerRunId", "==", run.managerRunId).get()).filter((p) => p.status === "COMMITTED");
  const proposalsBySymbol = {}, envelopesBySymbol = {};
  for (const plan of plans) {
    for (const mvId of plan.mandateVersionIds || []) {
      const es = await D.col(D.COL.activationEnvelopes).doc(mvId).get();
      if (!es.exists) continue;
      const env = decode(es.data());
      if (env && env.symbol) envelopesBySymbol[env.symbol] = env;
    }
    const p = plan.proposal || {};
    for (const m of (p.mandates || p.entries || [])) if (m && m.symbol) proposalsBySymbol[m.symbol] = m;
  }
  for (const mv of Object.values(envelopesBySymbol)) {
    if (proposalsBySymbol[mv.symbol]) continue;
    const ps = rows(await D.col(D.COL.mandateProposals).where("symbol", "==", mv.symbol).get()).map(decode).filter((x) => x && x.proposalHash === (mv.proposalHash || null));
    if (ps[0] && ps[0].proposal) proposalsBySymbol[mv.symbol] = ps[0].proposal;
  }
  /* reference closes for every decided symbol, not only the held ones */
  const closes = { ...closeBySymbol };
  for (const d of decisions) {
    if (closes[d.symbol]) continue;
    try { const bar = finalBar((await seriesReader(d.symbol)).series || [], tradingDate); closes[d.symbol] = bar ? microsOfUsd(bar.c) : null; } catch { closes[d.symbol] = null; }
  }
  const frozen = await LEARN.freezeDecisionOutcomeRecords({ managerRunId: run.managerRunId, tradingDate, cutoffMs: run.cutoffMs || null, decisions, proposalsBySymbol, envelopesBySymbol, closeBySymbol: closes,
    benchmarkCloseMicros, versions: { policyHash: run.policyHash || null, universeHash: run.universeHash || null, universeVersion: run.universeVersion || null, contextManifestHash: run.contextManifestHash || null }, admin: D });
  return { managerRunId: run.managerRunId, decisions: decisions.length, plans: plans.length, envelopes: Object.keys(envelopesBySymbol).length, proposals: Object.keys(proposalsBySymbol).length,
    missingClose: decisions.filter((d) => !closes[d.symbol]).length, ...frozen, atMs: nowMs };
}

/* 5. the day's KPI inputs, from what the pass and the stores already hold ── */
async function buildKpiInputs({ D, accountId, tradingDate, control, snapshot, benchmarkSeries, fills, trades, nowMs, marks, freeze, resolve }) {
  const run = control.lastManagerRun && control.lastManagerRun.tradingDate === tradingDate ? control.lastManagerRun : null;
  const inputs = { nowMs };
  /* portfolio: every final-mark NAV this account has recorded, today's included */
  const navDocs = rows(await D.col(D.COL.navMarks).where("accountId", "==", accountId).get()).filter((d) => d.finalMark && d.finalMark.navMinor != null && d.date && String(d.date) <= tradingDate);
  const navSeries = navDocs.map((d) => ({ date: String(d.date), navMinor: String(d.finalMark.navMinor) })).sort((a, b) => a.date.localeCompare(b.date));
  if (navSeries.length) {
    const bench = (benchmarkSeries || []).filter((b) => b && b.date && Number(b.c) > 0).map((b) => ({ date: String(b.date), closeMicros: microsOfUsd(b.c) }));
    inputs.portfolio = { navSeries, cashFlows: [], benchmarkSeries: bench.length ? bench : null, periodsPerYear: 252 };
    const avg = navSeries.reduce((n, r) => n + BigInt(r.navMinor), 0n) / BigInt(navSeries.length);
    const cost = await D.col(D.COL.costs).doc(`openai_${tradingDate}`).get();
    const costMinor = cost.exists ? String(BigInt(String(cost.data().spentMinor || 0))) : "0";
    inputs.operatingCostDrag = { costMinor, averageNavMinor: avg.toString() };
    inputs.unitEconomics = { costMinor, meetings: run ? 1 : 0, namesCovered: run && run.coverage ? Number(run.coverage.completedCount) || 0 : 0,
      researchJobs: run && run.research ? Number(run.research.completed) || 0 : 0, actionableMandates: run && run.maintenance ? Number(run.maintenance.actionable) || 0 : 0,
      activatedMandates: run && run.activation && run.activation.status === "COMMITTED" ? (run.activation.mandateVersionIds || run.activation.mandates || []).length : 0, fillProducingMandates: new Set((fills || []).map((f) => f.mandateVersionId).filter(Boolean)).size };
    if (snapshot) inputs.exposure = { averageNavMinor: avg.toString(), grossExposureMinor: snapshot.investedMinor, tradedNotionalMinor: (fills || []).reduce((n, f) => n + BigInt(String(f.notionalMinor || 0)), 0n).toString(),
      positions: (snapshot.positions || []).map((p) => ({ symbol: p.symbol, marketValueMinor: p.marketValueMinor })) };
  }
  if (run && run.coverage) inputs.universeCoverage = { numerator: Number(run.coverage.completedCount) || 0, denominator: Number(run.coverage.eligibleCount) || 0 };
  if (run && Number.isFinite(Number(run.elapsedMs))) inputs.completionLatency = { samples: [Math.round(Number(run.elapsedMs) / 1000)], slaSeconds: 5400 };
  inputs.integrity = { duplicate: run && run.coverage && Array.isArray(run.coverage.duplicates) ? run.coverage.duplicates.length : 0,
    unprotected: snapshot && snapshot.aggregates ? (snapshot.aggregates.unprotected || []).length : 0, stale: marks ? (marks.unmarked || []).length : 0 };
  if (trades && trades.length) inputs.trades = trades.map((t) => ({ symbol: t.symbol, pnlMinor: String(t.realizedMinor || 0) }));
  /* distribution scoring: every record the resolve step scored today */
  const scored = rows(await D.col(D.COL.forecasts).where("lastResolutionAtMs", "==", nowMs).get()).filter((f) => f.scoring && f.scoring.crps && !f.scoring.crps.error && f.outcomeBuckets);
  if (scored.length) inputs.forecasts = scored.map((f) => ({ buckets: f.outcomeBuckets, realizedMicros: f.resolved[`h${f.scoring.horizon}`].closeMicros }));
  inputs.steps = { marks: marks ? { marked: marks.marked, unmarked: (marks.unmarked || []).length } : null, freeze: freeze ? { written: freeze.written, skipped: freeze.skipped } : null, resolve: resolve ? { advanced: resolve.advanced, completed: resolve.completed } : null };
  return inputs;
}

/* the whole pass, admin- and reader-injectable so the deploy attestation can run it ── */
async function runPostclose({ accountId, tradingDate, admin = null, seriesReader = null, nowMs = Date.now(), kpiInputs = {} } = {}) {
  const D = admin || A;
  const real = !admin;
  const reader = seriesReader || ((s) => H.readDailyWithMeta(s));
  const startedMs = nowMs;
  const summary = { fnName: FN_NAME, accountId, tradingDate, steps: {}, errors: [] };
  const step = async (name, fn) => { try { summary.steps[name] = await fn(); } catch (e) { summary.errors.push({ step: name, code: e.code || null, message: String(e.message).slice(0, 160) }); summary.steps[name] = { failed: true, code: e.code || null }; } };
  const control = await controlDoc(D);
  let benchmarkCloseMicros = null;
  try { const b = finalBar((await reader(BENCHMARK)).series || [], tradingDate); benchmarkCloseMicros = b ? microsOfUsd(b.c) : null; } catch { benchmarkCloseMicros = null; }
  summary.benchmark = { symbol: BENCHMARK, closeMicros: benchmarkCloseMicros };

  await step("marks", () => markPositions({ D, accountId, tradingDate, seriesReader: reader, nowMs }));
  const closeBySymbol = (summary.steps.marks && summary.steps.marks.closeBySymbol) || {};
  let snapshot = null;
  await step("snapshot", async () => { snapshot = await PORTFOLIO.snapshot({ accountId, asOfMs: nowMs, admin: D }); return { navMinor: snapshot.navMinor, openPositions: snapshot.aggregates.openPositions, unprotected: snapshot.aggregates.unprotected }; });
  if (snapshot) await step("nav", () => recordNav({ D, accountId, tradingDate, snapshot, nowMs, real }));
  await step("freeze", () => freezeDecisions({ D, accountId, tradingDate, control, seriesReader: reader, closeBySymbol, benchmarkCloseMicros, nowMs }));
  await step("resolve", () => LEARN.resolveDue({ admin: D, seriesReader: async (s) => (await reader(s)).series || [], benchmarkReader: async (s) => (await reader(s)).series || [], nowMs }));
  await step("kpi", async () => {
    const dayStartMs = nowMs - 36 * 3600 * 1000;
    const fills = rows(await D.col(D.COL.fills).where("accountId", "==", accountId).get()).filter((f) => Number(f.receivedAtMs || f.atMs) >= dayStartMs);
    const trades = rows(await D.col(D.COL.trades).where("accountId", "==", accountId).get()).filter((t) => t.engineVersion === "manager" && t.closedAt && Date.parse(t.closedAt) >= dayStartMs);
    let benchmarkSeries = [];
    try { benchmarkSeries = (await reader(BENCHMARK)).series || []; } catch { benchmarkSeries = []; }
    const inputs = { ...(await buildKpiInputs({ D, accountId, tradingDate, control, snapshot, benchmarkSeries, fills, trades, nowMs, marks: summary.steps.marks, freeze: summary.steps.freeze, resolve: summary.steps.resolve })), ...(kpiInputs || {}) };
    const row = KPI.dailyAggregate({ date: tradingDate, accountId, inputs });
    await D.col(D.COL.kpiDaily).doc(`${accountId}_${tradingDate}`).set({ ...row, accountId, date: tradingDate, computedAtMs: nowMs, inputsSummary: { keys: Object.keys(inputs).sort(), navObservations: inputs.portfolio ? inputs.portfolio.navSeries.length : 0, fills: fills.length, trades: trades.length }, ...D.envelope({ created_by: FN_NAME }) }, { merge: true });
    return { written: true, computed: Object.keys(row.kpis || {}).filter((k) => row.kpis[k] !== null).length, missing: (row.missing || []).length, errors: row.errors || [], kpiVersion: row.kpiVersion || null };
  });
  await step("alerts", async () => {
    const derived = ALERTS.deriveConditions({ control, portfolio: snapshot, manager: control.lastManagerRun || null, execution: control.lastExecutionTick || null, ingest: control.lastIngestPass || null, nowMs });
    const up = await ALERTS.upsertAlerts({ admin: D, accountId, derived, nowMs });
    return { ...up, conditions: derived.map((c) => c.conditionId).slice(0, 40) };
  });
  summary.elapsedMs = Date.now() - startedMs;
  summary.ok = summary.errors.length === 0;
  const controlPatch = { lastPostclose: { ...summary, steps: Object.fromEntries(Object.entries(summary.steps).map(([k, v]) => [k, v && typeof v === "object" ? { ...v, closeBySymbol: undefined } : v])) }, lastPostcloseAtMs: nowMs };
  if (summary.ok) controlPatch.lastPostcloseDate = tradingDate;
  await D.col(D.COL.control).doc("control").set(controlPatch, { merge: true });
  return summary;
}

exports.handler = async (event) => {
  let body = {};
  try { body = JSON.parse(event.body || "{}"); } catch { return { statusCode: 400, body: JSON.stringify({ error: "invalid JSON" }) }; }
  const { jobId, task, nonce, payload = {} } = body;
  if (task !== TASK || !jobId) return { statusCode: 400, body: JSON.stringify({ error: "invalid job shape" }) };
  await AUTH.loadAuthSecrets();
  const claimed = await JOBS.claimOnce({ jobId, task: TASK, targetFunction: FN_NAME, token: nonce, payload });
  if (!claimed.claimed) return { statusCode: claimed.httpStatus || 409, body: JSON.stringify({ ok: false, reason: claimed.reason }) };
  const claim = claimed.claim;
  await M.loadMarketSettings();
  const ctrl = await controlDoc(A);
  const accountId = String(payload.accountId || ctrl.accountId || "paper-1");
  const tradingDate = String(payload.tradingDate || M.sessionState(new Date()).date);
  try {
    if (ctrl.engineMode !== "manager") { await JOBS.complete(claim, { skipped: true, reason: "engine_mode_legacy" }); return { statusCode: 200, body: JSON.stringify({ ok: true, skipped: true }) }; }
    const summary = await runPostclose({ accountId, tradingDate, nowMs: Date.now() });
    const compact = { ...summary, steps: Object.fromEntries(Object.entries(summary.steps).map(([k, v]) => [k, v && typeof v === "object" ? { ...v, closeBySymbol: undefined } : v])) };
    if (summary.ok) await JOBS.complete(claim, compact);
    else await JOBS.failClosed(claim, { code: "POSTCLOSE_STEP_FAILED", message: summary.errors.map((e) => `${e.step}:${e.code || e.message}`).join("; ").slice(0, 300), retryable: true, data: compact });
    return { statusCode: 200, body: JSON.stringify({ ok: summary.ok, summary: compact }) };
  } catch (e) {
    console.error("investorPostclose failed", redact({ jobId, error: e.message, stack: (e.stack || "").slice(0, 400) }));
    await JOBS.failClosed(claim, { code: e.code || "POSTCLOSE_FAILED", message: e.message, retryable: true }).catch(() => ({}));
    return { statusCode: 500, body: JSON.stringify({ ok: false, error: String(e.message).slice(0, 200) }) };
  }
};

exports.FN_NAME = FN_NAME;
exports.finalBar = finalBar;
exports.runPostclose = runPostclose;
exports.markPositions = markPositions;
exports.freezeDecisions = freezeDecisions;
