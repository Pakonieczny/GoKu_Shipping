/*  netlify/functions/_investorLearning.js  (fund-manager-v1)
 *  ---------------------------------------------------------------------------
 *  Investor AI — controlled memory and evaluation (blueprint §16.1, §16.2,
 *  §16.5, §12.1).
 *
 *  Learning here means: freeze exactly what the manager saw and predicted;
 *  observe outcomes only AFTER the forecast timestamp; compare selected
 *  names with eligible unselected names and declared benchmarks; attribute
 *  errors; feed validated failures to the eval set. It is never an agent
 *  editing its own prompt after a loss.
 *
 *    freezeDecisionOutcomeRecords  one immutable forecast/outcome shell per
 *                                  decision row (every decision, not only BUY)
 *    resolveDue                    point-in-time resolution at 1/5/20/60 and
 *                                  the declared horizon; MFE/MAE, touches,
 *                                  expected vs realized after cost, distribution
 *                                  scores; counterfactuals vs unselected
 *    antiAnchoringSample           the predeclared random sample whose prior
 *                                  conclusion is hidden in an isolated review
 *    retrievalMemory               version-aware memory by POINTER
 *    writePostmortem               Sol's offline attribution, never a policy edit
 *
 *  Numbers on the wire are canonical integer strings; scores that are
 *  inherently real-valued live under `statistics` from _investorKpi.
 * ---------------------------------------------------------------------------
 */

"use strict";

const crypto = require("crypto");
const A = require("./_investorAdmin");
const POLICY = require("./_investorPolicy");
const M = require("./_investorMoney");

const FORECAST_SCHEMA = "forecast-outcome.v1";
const COUNTERFACTUAL_SCHEMA = "counterfactual.v1";
const POSTMORTEM_SCHEMA = "postmortem-record.v1";
const HORIZONS = Object.freeze([1, 5, 20, 60]);
const ANTI_ANCHOR_PPM = 20000;                // 2% of non-holdings per meeting

function sha(v) { return crypto.createHash("sha256").update(typeof v === "string" ? v : JSON.stringify(POLICY.canonical(v))).digest("hex"); }
function big(v) { return BigInt(String(v == null ? 0 : v)); }
function db(admin) { return admin || A; }
function lazy(m) { try { return require(m); } catch { return null; } }
function rows(snap) { const out = []; if (snap && typeof snap.forEach === "function") snap.forEach((d) => out.push(d.data())); return out; }
function microsOf(usd) { const n = Number(usd); return Number.isFinite(n) && n > 0 ? BigInt(Math.round(n * 1e6)) : null; }
function retBps(fromMicros, toMicros) { return M.toCanonical(M.returnBps({ fromMicros, toMicros, mode: M.ROUNDING.HALF_EVEN })); }

/* ── the §16.2 record, PURE ────────────────────────────────────────────── */
function forecastIdFor({ managerRunId, symbol }) { return `fc_${sha(`${managerRunId}|${symbol}`).slice(0, 32)}`; }
/** Build the frozen shell for one decision row. Reference is the executable
 *  limit for a BUY, else the cutoff close; horizons resolve on the exchange
 *  calendar from the decision session. */
function outcomeRecordFor({ managerRunId, tradingDate, decision, proposal = null, envelope = null, referenceCloseMicros = null, benchmarkCloseMicros = null, cutoffMs = null, versions = {} } = {}) {
  const fc = proposal && proposal.forecast;
  const isBuy = decision.decision === "BUY" && proposal && proposal.action && proposal.action.entry;
  const referencePriceMicros = isBuy ? proposal.action.entry.limitPriceMicros : referenceCloseMicros;
  const declared = fc ? Number(fc.horizonTradingDays) : null;
  const horizons = [...new Set([...HORIZONS, ...(declared ? [declared] : [])])].sort((a, b) => a - b);
  return {
    schemaVersion: FORECAST_SCHEMA, forecastId: forecastIdFor({ managerRunId, symbol: decision.symbol }), managerRunId, tradingDate, symbol: decision.symbol,
    decision: decision.decision, reasonCode: decision.reasonCode || null, capitalRank: decision.capitalRank == null ? null : decision.capitalRank, held: decision.held === true,
    referencePriceMicros: referencePriceMicros || null, referencePriceType: isBuy ? "BUY_LIMIT" : "CUTOFF_CLOSE", referenceTime: cutoffMs ? new Date(cutoffMs).toISOString() : null, currency: "USD",
    returnConvention: fc ? fc.basis.returnConvention : "ARITHMETIC_PRICE_RETURN", corporateActionPolicy: fc ? fc.basis.corporateActionPolicy : "SPLIT_ADJUSTED_ONLY",
    conditionalOn: fc ? fc.basis.conditionalOn : "OPPORTUNITY_FROM_REFERENCE_PRICE", fillProbabilityByExpiryPpm: fc ? fc.fillProbabilityByExpiryPpm : null, noFillOutcome: fc ? fc.noFillOutcome : "NOT_APPLICABLE",
    estimatedRoundTripCostMicrosPerShare: fc ? fc.basis.estimatedRoundTripCostMicrosPerShare : "0", horizons, declaredHorizonTradingDays: declared,
    outcomeBuckets: fc ? fc.outcomeBuckets : null, expectedTerminalPriceMicros: envelope && envelope.expectedTerminalPriceMicros || null, uncertaintyLevel: fc ? fc.uncertaintyLevel : null,
    targetPriceMicros: isBuy ? proposal.action.protection.takeProfitPriceMicros : null, lossBoundaryPriceMicros: isBuy ? proposal.action.protection.lossBoundaryPriceMicros : null,
    invalidators: proposal ? (proposal.invalidators || []).map((i) => ({ predicateType: i.predicateType, consequence: i.consequence })) : [],
    benchmark: { symbol: "SPY", referenceCloseMicros: benchmarkCloseMicros || null },
    versions: { model: versions.model || null, promptHash: versions.promptHash || null, schemaHash: versions.schemaHash || null, policyHash: versions.policyHash || null, contextManifestHash: versions.contextManifestHash || null, researchMemoId: decision.researchMemoId || null, mandateProposalHash: proposal ? sha(proposal) : null },
    costs: { aiMinor: versions.aiCostMinor || null, dataMinor: null, spreadBps: null, slippageBps: null },
    operatorOverride: null, resolutionProcedure: "daily_close_after_forecast_time_touch_on_high_low_exchange_calendar", status: "PENDING", resolved: {}, frozenAtMs: cutoffMs || null,
  };
}

/* ── resolution, PURE core ─────────────────────────────────────────────── */
/** Bars strictly after the reference time, in date order. */
function barsAfter(series, referenceMs) {
  const day = new Date(referenceMs).toISOString().slice(0, 10);
  return (series || []).filter((b) => b && b.date > day && Number(b.c) > 0).sort((a, b) => String(a.date).localeCompare(String(b.date)));
}
/** Resolve every horizon the bars can reach; touches on high/low; MFE/MAE. */
function resolveAgainstBars({ record, series, benchmarkSeries = null }) {
  const ref = record.referencePriceMicros ? big(record.referencePriceMicros) : null;
  const refMs = Date.parse(record.referenceTime || `${record.tradingDate}T20:00:00Z`);
  const after = barsAfter(series, refMs);
  if (!ref || !after.length) return { resolved: record.resolved || {}, complete: false, reason: !ref ? "no_reference_price" : "no_bars_after_reference" };
  const bench = benchmarkSeries ? barsAfter(benchmarkSeries, refMs) : [];
  const benchRef = record.benchmark && record.benchmark.referenceCloseMicros ? big(record.benchmark.referenceCloseMicros) : null;
  const resolved = { ...(record.resolved || {}) };
  let mfe = 0n, mae = 0n, targetTouchedOn = null, boundaryTouchedOn = null;
  const target = record.targetPriceMicros ? big(record.targetPriceMicros) : null, boundary = record.lossBoundaryPriceMicros ? big(record.lossBoundaryPriceMicros) : null;
  after.forEach((b, i) => {
    const h = microsOf(b.h) || microsOf(b.c), l = microsOf(b.l) || microsOf(b.c);
    if (h > ref && h - ref > mfe) mfe = h - ref;
    if (l < ref && ref - l > mae) mae = ref - l;
    if (target && !targetTouchedOn && h >= target) targetTouchedOn = b.date;
    if (boundary && !boundaryTouchedOn && l <= boundary) boundaryTouchedOn = b.date;
    const n = i + 1;
    if (record.horizons.includes(n) && !resolved[`h${n}`]) {
      const c = microsOf(b.c);
      const bb = bench[i] ? microsOf(bench[i].c) : null;
      resolved[`h${n}`] = { date: b.date, closeMicros: c.toString(), returnBps: retBps(ref, c), benchmarkReturnBps: bb && benchRef ? retBps(benchRef, bb) : null,
        excessBps: bb && benchRef ? M.toCanonical(BigInt(retBps(ref, c)) - BigInt(retBps(benchRef, bb))) : null, sessions: n };
    }
  });
  const maxH = Math.max(...record.horizons);
  const complete = after.length >= maxH;
  const excursions = { mfeBps: M.toCanonical(mfe * 10000n / ref), maeBps: M.toCanonical(mae * 10000n / ref), targetTouchedOn, boundaryTouchedOn,
    /* the adverse convention: a bar that touches both resolves as the boundary */
    firstTouch: targetTouchedOn && boundaryTouchedOn ? (targetTouchedOn < boundaryTouchedOn ? "TARGET" : "BOUNDARY") : (targetTouchedOn ? "TARGET" : (boundaryTouchedOn ? "BOUNDARY" : null)) };
  let scoring = null;
  const declared = record.declaredHorizonTradingDays;
  if (declared && resolved[`h${declared}`] && record.outcomeBuckets) {
    const K = lazy("./_investorKpi");
    const realized = big(resolved[`h${declared}`].closeMicros);
    const cost = big(record.estimatedRoundTripCostMicrosPerShare || 0);
    const expected = record.expectedTerminalPriceMicros ? big(record.expectedTerminalPriceMicros) : null;
    scoring = { horizon: declared, realizedReturnBps: resolved[`h${declared}`].returnBps, realizedAfterCostBps: retBps(ref, realized - cost),
      expectedReturnBps: expected ? retBps(ref, expected) : null, expectedAfterCostBps: expected ? retBps(ref, expected - cost) : null,
      errorBps: expected ? M.toCanonical(BigInt(retBps(ref, realized)) - BigInt(retBps(ref, expected))) : null,
      crps: K && K.crps ? safe(() => K.crps({ buckets: record.outcomeBuckets, realizedMicros: realized.toString() })) : null,
      quantileLoss: K && K.quantileLoss && record.quantiles ? safe(() => K.quantileLoss({ quantiles: record.quantiles, realizedMicros: realized.toString() })) : null };
  }
  return { resolved, excursions, scoring, complete, sessionsObserved: after.length };
}
function safe(fn) { try { return fn(); } catch (e) { return { error: String(e.code || e.message).slice(0, 80) }; } }
/** PURE. Selected vs eligible unselected at one horizon (§16.5 arm comparison). */
function counterfactualRow({ managerRunId, tradingDate, horizon, selected = [], unselected = [], benchmarkReturnBps = null }) {
  const mean = (list) => { const v = list.map((x) => BigInt(x.returnBps)).filter((x) => x !== undefined); return v.length ? M.toCanonical(M.divRound(v.reduce((a, b) => a + b, 0n), BigInt(v.length), M.ROUNDING.HALF_EVEN)) : null; };
  const s = mean(selected), u = mean(unselected);
  return { schemaVersion: COUNTERFACTUAL_SCHEMA, counterfactualId: `cf_${sha(`${managerRunId}|${horizon}`).slice(0, 24)}`, managerRunId, tradingDate, horizonTradingDays: horizon,
    selectedCount: selected.length, unselectedCount: unselected.length, selectedMeanBps: s, unselectedMeanBps: u, selectedMinusUnselectedBps: s != null && u != null ? M.toCanonical(BigInt(s) - BigInt(u)) : null,
    benchmarkReturnBps, selectedMinusBenchmarkBps: s != null && benchmarkReturnBps != null ? M.toCanonical(BigInt(s) - BigInt(benchmarkReturnBps)) : null,
    note: "point-in-time eligible controls; removed and delisted names retained; no imputed membership" };
}
/** PURE. The predeclared anti-anchoring sample: deterministic by hash. */
function antiAnchoringSample({ symbols = [], tradingDate, heldSymbols = [], ppm = ANTI_ANCHOR_PPM } = {}) {
  const held = new Set(heldSymbols);
  return symbols.filter((s) => !held.has(s) && parseInt(sha(`${tradingDate}|${s}`).slice(0, 6), 16) % 1000000 < ppm).sort();
}
/** PURE. Reconcile the independent review against the hidden prior conclusion. */
function reconcileAntiAnchoring({ prior = [], independent = [] } = {}) {
  const byPrior = new Map(prior.map((r) => [r.symbol, r.decision]));
  const rowsOut = independent.map((r) => ({ symbol: r.symbol, prior: byPrior.get(r.symbol) || null, independent: r.decision, agree: byPrior.get(r.symbol) === r.decision }));
  const n = rowsOut.length, agree = rowsOut.filter((r) => r.agree).length;
  return { rows: rowsOut, sampled: n, agreementPpm: n ? String(Math.round(agree * 1000000 / n)) : null, disagreements: rowsOut.filter((r) => !r.agree).map((r) => r.symbol) };
}

/* ── persistence ───────────────────────────────────────────────────────── */
async function freezeDecisionOutcomeRecords({ managerRunId, tradingDate, cutoffMs = null, decisions = [], proposalsBySymbol = {}, envelopesBySymbol = {}, closeBySymbol = {}, benchmarkCloseMicros = null, versions = {}, admin = null } = {}) {
  const D = db(admin);
  let written = 0, skipped = 0;
  for (const d of decisions) {
    const rec = outcomeRecordFor({ managerRunId, tradingDate, decision: d, proposal: proposalsBySymbol[d.symbol] || null, envelope: envelopesBySymbol[d.symbol] || null, referenceCloseMicros: closeBySymbol[d.symbol] || null, benchmarkCloseMicros, cutoffMs, versions });
    const ref = D.col(D.COL.forecasts).doc(rec.forecastId);
    const wrote = await D.runTransaction(async (tx) => { const cur = await tx.get(ref); if (cur.exists) return false; tx.set(ref, { ...rec, createdAtMs: Date.now(), ...D.envelope({ created_by: "learning.freeze" }) }); return true; });
    if (wrote) written += 1; else skipped += 1;
  }
  return { written, skipped };
}
async function pendingForecasts({ admin = null, limit = 400 } = {}) {
  const D = db(admin);
  return rows(await D.col(D.COL.forecasts).where("status", "==", "PENDING").get()).slice(0, limit);
}
/** Resolve every pending record the supplied series can advance. `seriesReader(symbol)` returns daily bars. */
async function resolveDue({ admin = null, seriesReader, benchmarkReader = null, nowMs = Date.now(), limit = 400 } = {}) {
  const D = db(admin);
  const pending = await pendingForecasts({ admin: D, limit });
  const out = { examined: pending.length, advanced: 0, completed: 0, byRun: {} };
  const bench = benchmarkReader ? await benchmarkReader("SPY").catch(() => null) : null;
  for (const rec of pending) {
    let series;
    try { series = await seriesReader(rec.symbol); } catch { continue; }
    const r = resolveAgainstBars({ record: rec, series, benchmarkSeries: bench });
    if (!r.resolved || Object.keys(r.resolved).length === Object.keys(rec.resolved || {}).length && !r.complete) continue;
    await D.col(D.COL.forecasts).doc(rec.forecastId).set({ resolved: r.resolved, excursions: r.excursions || null, scoring: r.scoring || null, sessionsObserved: r.sessionsObserved || 0,
      status: r.complete ? "RESOLVED" : "PENDING", resolvedAtMs: r.complete ? nowMs : null, lastResolutionAtMs: nowMs }, { merge: true });
    out.advanced += 1;
    if (r.complete) out.completed += 1;
    const run = (out.byRun[rec.managerRunId] = out.byRun[rec.managerRunId] || { selected: {}, unselected: {} });
    for (const h of Object.keys(r.resolved)) { const bucket = rec.decision === "BUY" ? run.selected : run.unselected; (bucket[h] = bucket[h] || []).push({ symbol: rec.symbol, returnBps: r.resolved[h].returnBps, benchmarkReturnBps: r.resolved[h].benchmarkReturnBps }); }
  }
  /* counterfactuals per run and horizon, from the same point-in-time clock */
  for (const [managerRunId, run] of Object.entries(out.byRun)) {
    for (const h of new Set([...Object.keys(run.selected), ...Object.keys(run.unselected)])) {
      const horizon = Number(h.slice(1));
      const sel = run.selected[h] || [], un = run.unselected[h] || [];
      if (!sel.length && !un.length) continue;
      const b = (sel[0] || un[0] || {}).benchmarkReturnBps || null;
      const row = counterfactualRow({ managerRunId, tradingDate: pending.find((p) => p.managerRunId === managerRunId)?.tradingDate || null, horizon, selected: sel, unselected: un, benchmarkReturnBps: b });
      await D.col(D.COL.counterfactuals).doc(row.counterfactualId).set({ ...row, updatedAtMs: nowMs }, { merge: true });
    }
  }
  return out;
}
/** Version-aware memory by pointer: the latest memo, decisions and outcomes for a symbol. */
async function retrievalMemory({ symbol, admin = null, limit = 12 } = {}) {
  const D = db(admin);
  const sym = String(symbol || "").toUpperCase();
  const R = lazy("./_investorResearch");
  const memo = R ? await R.latest(sym, { admin: D }).catch(() => null) : null;
  const decisions = rows(await D.col(D.COL.managerDecisions).where("symbol", "==", sym).get()).map((d) => (d._codec ? safeDecode(d) : d)).filter(Boolean).sort((a, b) => Number(b.asOfMs) - Number(a.asOfMs)).slice(0, limit);
  const forecasts = rows(await D.col(D.COL.forecasts).where("symbol", "==", sym).get()).sort((a, b) => String(b.tradingDate).localeCompare(String(a.tradingDate))).slice(0, limit);
  return { symbol: sym, memo: memo ? { memoId: memo.memoId, researchVersion: memo.researchVersion, asOf: memo.asOf, proposedDecision: memo.proposedDecision, thesisHealth: memo.thesisHealth, dossierVersionId: memo.dossierVersionId, label: "PRIOR_INTERPRETATION_NOT_EVIDENCE" } : null,
    decisions: decisions.map((d) => ({ managerRunId: d.managerRunId, decision: d.decision, reasonCode: d.reasonCode, tradingDate: d.tradingDate, asOfMs: d.asOfMs })),
    outcomes: forecasts.map((f) => ({ forecastId: f.forecastId, tradingDate: f.tradingDate, decision: f.decision, status: f.status, resolved: f.resolved || {}, excursions: f.excursions || null, scoring: f.scoring || null })) };
}
function safeDecode(d) { try { return require("./_investorStorageCodec").decode(d); } catch { return null; } }
/** Sol's offline attribution for a selected case; adjudication stays with the operator. */
async function writePostmortem({ decisionId, frozenDecision, laterOutcome, gateway = null, admin = null, nowMs = Date.now() } = {}) {
  const D = db(admin);
  const O = gateway || lazy("./_investorOpenai");
  if (!O || typeof O.writePostmortem !== "function") return { written: false, reason: "gateway_unavailable" };
  const r = await O.writePostmortem({ frozenDecision: { decisionId, ...frozenDecision }, laterOutcome });
  const id = `pm_${sha(`${decisionId}|${sha(laterOutcome || {})}`).slice(0, 24)}`;
  const doc = { schemaVersion: POSTMORTEM_SCHEMA, postmortemId: id, decisionId, ok: r.ok === true, postmortem: r.ok ? r.postmortem : null, error: r.ok ? null : r.error, model: r.model || null, costMinor: r.costMinor || null,
    operatorAdjudicationRequired: r.ok ? r.postmortem.operatorAdjudicationRequired === true : true, adjudicated: false, promotesAnything: false, createdAtMs: nowMs, ...D.envelope({ created_by: "learning.postmortem" }) };
  await D.col(D.COL.postmortems).doc(id).set(doc, { merge: true });
  return { written: true, postmortemId: id, ok: r.ok === true, evalCase: r.ok ? r.postmortem.proposedEvalCase : null };
}

module.exports = { FORECAST_SCHEMA, COUNTERFACTUAL_SCHEMA, POSTMORTEM_SCHEMA, HORIZONS, ANTI_ANCHOR_PPM,
  forecastIdFor, outcomeRecordFor, barsAfter, resolveAgainstBars, counterfactualRow, antiAnchoringSample, reconcileAntiAnchoring,
  freezeDecisionOutcomeRecords, pendingForecasts, resolveDue, retrievalMemory, writePostmortem };
