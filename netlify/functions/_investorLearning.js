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
/** Build the frozen shell for one decision row. Reference is the same frozen decision-time market mark for every name; horizons resolve on the exchange
 *  calendar from the decision session. */
function outcomeRecordFor({ managerRunId, tradingDate, decision, proposal = null, envelope = null, referenceCloseMicros = null, benchmarkCloseMicros = null, cutoffMs = null, versions = {} } = {}) {
  const fc = proposal && proposal.forecast;
  const isBuy = decision.decision === "BUY" && proposal && proposal.action && proposal.action.entry;
  const referencePriceMicros = referenceCloseMicros;
  const declared = fc ? Number(fc.horizonTradingDays) : null;
  const horizons = [...new Set([...HORIZONS, ...(declared ? [declared] : [])])].sort((a, b) => a - b);
  return {
    schemaVersion: FORECAST_SCHEMA, forecastId: forecastIdFor({ managerRunId, symbol: decision.symbol }), managerRunId, tradingDate, symbol: decision.symbol,
    decision: decision.decision, reasonCode: decision.reasonCode || null, capitalRank: decision.capitalRank == null ? null : decision.capitalRank, held: decision.held === true,
    referencePriceMicros: referencePriceMicros || null, referencePriceType: "COMMON_DECISION_MARK", referenceTime: cutoffMs ? new Date(cutoffMs).toISOString() : null, currency: "USD",
    returnConvention: fc ? fc.basis.returnConvention : "ARITHMETIC_PRICE_RETURN", corporateActionPolicy: fc ? fc.basis.corporateActionPolicy : "SPLIT_ADJUSTED_ONLY",
    conditionalOn: "OPPORTUNITY_FROM_REFERENCE_PRICE", fillProbabilityByExpiryPpm: fc ? fc.fillProbabilityByExpiryPpm : null, noFillOutcome: fc ? fc.noFillOutcome : "NOT_APPLICABLE",
    estimatedRoundTripCostMicrosPerShare: "0", opportunityCostConvention:"GROSS_SAME_MARK_SAME_HORIZON", conditionalForecast:fc || null, horizons, declaredHorizonTradingDays: declared,
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
  const market=require("./_investorMarket");
  return (series || []).filter(b=>b && b.date && Number(b.c)>0 && market.sessionCloseMs(new Date(`${b.date}T16:00:00Z`))>referenceMs).sort((a,b)=>a.date.localeCompare(b.date));
}
/** Resolve every horizon the bars can reach; touches on high/low; MFE/MAE. */
function resolveAgainstBars({ record, series, benchmarkSeries = null, nowMs = Date.now() }) {
  const ref = record.referencePriceMicros ? big(record.referencePriceMicros) : null;
  const refMs = Date.parse(record.referenceTime || `${record.tradingDate}T20:00:00Z`);
  const after = barsAfter(require("./_investorDecisionContext").completedDaily(series,nowMs), refMs).slice(0,Math.max(...record.horizons));
  if (!ref || !after.length) return { resolved: record.resolved || {}, complete: false, reason: !ref ? "no_reference_price" : "no_bars_after_reference" };
  const bench = benchmarkSeries ? barsAfter(require("./_investorDecisionContext").completedDaily(benchmarkSeries,nowMs), refMs) : [];
  const benchRef = record.benchmark && record.benchmark.referenceCloseMicros ? big(record.benchmark.referenceCloseMicros) : null;
  const resolved = { ...(record.resolved || {}) };
  let mfe = 0n, mae = 0n, targetTouchedOn = null, boundaryTouchedOn = null;
  const target = record.targetPriceMicros ? big(record.targetPriceMicros) : null, boundary = record.lossBoundaryPriceMicros ? big(record.lossBoundaryPriceMicros) : null;
  after.forEach((b, i) => {
    const openMs=require("./_investorMarket").nyWallClockToUtcMs(b.date,570);
    // A daily high/low includes pre-fill prices on an intraday entry day.
    const fullSession=refMs<=openMs;
    const h = microsOf(fullSession ? b.h : b.c) || microsOf(b.c), l = microsOf(fullSession ? b.l : b.c) || microsOf(b.c);
    if (h > ref && h - ref > mfe) mfe = h - ref;
    if (l < ref && ref - l > mae) mae = ref - l;
    if (target && !targetTouchedOn && h >= target) targetTouchedOn = b.date;
    if (boundary && !boundaryTouchedOn && l <= boundary) boundaryTouchedOn = b.date;
    const n = i + 1;
    if (record.horizons.includes(n) && !resolved[`h${n}`]) {
      const c = microsOf(b.c);
      const matchingBenchmark = bench.find(x=>x.date === b.date);
      const bb = matchingBenchmark ? microsOf(matchingBenchmark.c) : null;
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
  if (record.conditionalOn !== "OPPORTUNITY_FROM_REFERENCE_PRICE" && declared && resolved[`h${declared}`] && record.outcomeBuckets) {
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
  const pairs = selected.flatMap(s=>{
    const sc=s.characteristics || {};
    if (!sc.sector || sc.volatilityBps == null) return [];
    const choices=unselected.filter(u=>u.characteristics && u.characteristics.sector===sc.sector && u.characteristics.volatilityBps != null &&
      Math.abs(Number(u.characteristics.volatilityBps)-Number(sc.volatilityBps))<=Math.max(500,Number(sc.volatilityBps)*0.25))
      .sort((a,b)=>Math.abs(Number(a.characteristics.volatilityBps)-Number(sc.volatilityBps))-Math.abs(Number(b.characteristics.volatilityBps)-Number(sc.volatilityBps)) || a.symbol.localeCompare(b.symbol));
    return choices[0] ? [{selected:s,control:choices[0]}] : [];
  });
  const sm=mean(pairs.map(p=>p.selected)), um=mean(pairs.map(p=>p.control));
  return { schemaVersion:COUNTERFACTUAL_SCHEMA,counterfactualId:`cf_${sha(`${managerRunId}|${horizon}`).slice(0,24)}`,managerRunId,tradingDate,horizonTradingDays:horizon,
    selectedCount:selected.length,unselectedCount:unselected.length,matchedCount:pairs.length,
    selectedMeanBps:sm,unselectedMeanBps:um,selectedMinusUnselectedBps:sm!=null && um!=null?(BigInt(sm)-BigInt(um)).toString():null,
    unmatchedSelectedCount:selected.length-pairs.length,pairs:pairs.map(p=>({selected:p.selected.symbol,control:p.control.symbol})),
    benchmarkReturnBps,selectedMinusBenchmarkBps:sm!=null && benchmarkReturnBps!=null?(BigInt(sm)-BigInt(benchmarkReturnBps)).toString():null,
    note:"Descriptive comparison, not causal alpha: same run, reference convention and horizon; sector match and nearest 20-day volatility within 25% or 500 bps. No match means no selection-lift claim." };

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
async function freezeDecisionOutcomeRecords({ managerRunId, tradingDate, cutoffMs = null, decisions = [], proposalsBySymbol = {}, envelopesBySymbol = {}, closeBySymbol = {}, characteristicsBySymbol = {}, benchmarkCloseMicros = null, marketBySymbol = {}, benchmarkObservation = null, versions = {}, accountId = "paper-1", admin = null } = {}) {
  const D = db(admin);
  let written = 0, skipped = 0;
  for (const d of decisions) {
    const rec = outcomeRecordFor({ managerRunId, tradingDate, decision: d, proposal: proposalsBySymbol[d.symbol] || null, envelope: envelopesBySymbol[d.symbol] || null, referenceCloseMicros: closeBySymbol[d.symbol] || null, benchmarkCloseMicros, cutoffMs, versions });
    rec.accountId=accountId;
    rec.adjustmentAnchor=marketBySymbol[d.symbol] && marketBySymbol[d.symbol].adjustmentAnchor || null;
    rec.benchmark.adjustmentAnchor=benchmarkObservation && benchmarkObservation.adjustmentAnchor || null;
    rec.opportunityReturnConvention=rec.adjustmentAnchor && rec.adjustmentAnchor.adjustment === "split_and_dividend" ? "ARITHMETIC_TOTAL_RETURN" : "ARITHMETIC_PRICE_RETURN";
    const sessions=proposalsBySymbol[d.symbol] && proposalsBySymbol[d.symbol].action && proposalsBySymbol[d.symbol].action.entry && proposalsBySymbol[d.symbol].action.entry.authorizedSessionDates || [];
    rec.entryExpiryMs=sessions.length ? require("./_investorMarket").sessionCloseMs(new Date(`${sessions.at(-1)}T16:00:00Z`)) : null;
    rec.characteristics=characteristicsBySymbol[d.symbol] || {};
    const pointer=await D.col(D.COL.activeMandates).doc(`${accountId}_${d.symbol}`).get();
    rec.mandateVersionId=d.decision === "BUY" && pointer.exists ? pointer.data().desiredVersionId || pointer.data().appliedVersionId || null : null;
    const ref = D.col(D.COL.forecasts).doc(rec.forecastId);
    const wrote = await D.runTransaction(async (tx) => { const cur = await tx.get(ref); if (cur.exists) return false; tx.set(ref, { ...rec, createdAtMs: Date.now(), ...D.envelope({ created_by: "learning.freeze" }) }); return true; });
    if (wrote) written += 1; else skipped += 1;
  }
  return { written, skipped };
}
async function pendingForecasts({ admin = null, limit = 400 } = {}) {
  const D = db(admin);
  return rows(await D.col(D.COL.forecasts).where("status", "==", "PENDING").get()).sort((a,b)=>(Number(a.lastResolutionAtMs)||0)-(Number(b.lastResolutionAtMs)||0)).slice(0,limit);
}
/** Resolve every pending record the supplied series can advance. `seriesReader(symbol)` returns daily bars. */
async function resolveDue({ admin = null, seriesReader, benchmarkReader = null, nowMs = Date.now(), limit = 400 } = {}) {
  const D = db(admin);
  const pending = await pendingForecasts({ admin: D, limit });
  const out = { examined: pending.length, advanced: 0, completed: 0, byRun: {} };
  const benchPack = benchmarkReader ? await benchmarkReader("SPY").catch(() => null) : null;
  for (const rec of pending) {
    let series;
    try { series = await seriesReader(rec.symbol); } catch { continue; }
    const adjusted=rebaseSeries(series,rec.adjustmentAnchor);
    const benchmark=rebaseSeries(benchPack,rec.benchmark && rec.benchmark.adjustmentAnchor);
    const r = adjusted.ok ? resolveAgainstBars({ record: rec, series:adjusted.series, benchmarkSeries: benchmark.ok ? benchmark.series : null, nowMs }) : {resolved:rec.resolved || {},complete:false,reason:adjusted.reason};
    const actualTrade=await actualTradeOutcome({record:rec,series,nowMs,admin:D});
    const complete=r.complete && (!actualTrade || (actualTrade.status !== "FILLED" && (actualTrade.status !== "CLOSED" || actualTrade.horizonResolved)));

    if (!r.resolved) continue;
    await D.col(D.COL.forecasts).doc(rec.forecastId).set({ actualTrade, resolved: r.resolved, excursions: r.excursions || null, scoring: r.scoring || null, sessionsObserved: r.sessionsObserved || 0,
      resolutionIssue:r.reason || null, status: complete ? "RESOLVED" : "PENDING", resolvedAtMs: complete ? nowMs : null, lastResolutionAtMs: nowMs }, { merge: true });
    if (sha({resolved:r.resolved,actualTrade}) !== sha({resolved:rec.resolved || {},actualTrade:rec.actualTrade || null})) out.advanced += 1;
    if (complete) out.completed += 1;
    const run = (out.byRun[rec.managerRunId] = out.byRun[rec.managerRunId] || { selected: {}, unselected: {} });
    for (const h of Object.keys(r.resolved)) { const bucket = rec.decision === "BUY" ? run.selected : run.unselected; (bucket[h] = bucket[h] || []).push({ symbol: rec.symbol, returnBps: r.resolved[h].returnBps, benchmarkReturnBps: r.resolved[h].benchmarkReturnBps }); }
  }
  /* counterfactuals per run and horizon, from the same point-in-time clock */
  for (const managerRunId of Object.keys(out.byRun)) {
    const run={selected:{},unselected:{}};
    const all=rows(await D.col(D.COL.forecasts).where("managerRunId","==",managerRunId).get());
    for(const f of all.filter(f=>f.referencePriceType === "COMMON_DECISION_MARK" && !f.held)) for(const [h,v] of Object.entries(f.resolved || {})) {
      const bucket=f.decision === "BUY"?run.selected:run.unselected;
      (bucket[h]=bucket[h] || []).push({symbol:f.symbol,returnBps:v.returnBps,benchmarkReturnBps:v.benchmarkReturnBps,characteristics:f.characteristics || {}});
    }
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
async function retrievalMemory({ symbol, admin = null, limit = 12, cutoffMs = Date.now() } = {}) {
  const D = db(admin);
  const sym = String(symbol || "").toUpperCase();
  const R = lazy("./_investorResearch");
  const memo = R ? await R.latest(sym, { admin: D, cutoffMs }).catch(() => null) : null;
  const decisions = rows(await D.col(D.COL.managerDecisions).where("symbol", "==", sym).get()).map((d) => (d._codec ? safeDecode(d) : d)).filter(d=>d && Number(d.asOfMs)<=cutoffMs).sort((a, b) => Number(b.asOfMs) - Number(a.asOfMs)).slice(0, limit);
  const forecasts = rows(await D.col(D.COL.forecasts).where("symbol", "==", sym).get()).filter(f=>Number(f.lastResolutionAtMs)<=cutoffMs && f.referencePriceType === "COMMON_DECISION_MARK").sort((a, b) => String(b.tradingDate).localeCompare(String(a.tradingDate)));
  const calibration=calibrationSummary(forecasts.filter(f=>f.versions && f.versions.model===POLICY.ROLE_MODELS.manager.model));
  const lessons=rows(await D.col(D.COL.postmortems).where("adjudicated","==",true).get()).filter(p=>p.approved === true && Number(p.createdAtMs)<=cutoffMs && (p.symbol===sym || p.scope === "all")).slice(-5).map(p=>({postmortemId:p.postmortemId,lesson:p.postmortem && p.postmortem.lesson || null}));
  return { calibration, approvedLessons:lessons, symbol: sym, memo: memo ? { memoId: memo.memoId, researchVersion: memo.researchVersion, asOf: memo.asOf, proposedDecision: memo.proposedDecision, thesisHealth: memo.thesisHealth, dossierVersionId: memo.dossierVersionId, label: "PRIOR_INTERPRETATION_NOT_EVIDENCE" } : null,
    decisions: decisions.map((d) => ({ managerRunId: d.managerRunId, decision: d.decision, reasonCode: d.reasonCode, tradingDate: d.tradingDate, asOfMs: d.asOfMs })),
    outcomes: forecasts.slice(0,limit).map((f) => ({ forecastId: f.forecastId, tradingDate: f.tradingDate, decision: f.decision, status: f.status, resolved: f.resolved || {}, excursions: f.excursions || null, scoring: f.scoring || null })) };
}
function calibrationSummary(forecasts) {
  const horizons=[...new Set(forecasts.filter(f=>f.actualTrade && f.actualTrade.scoring).map(f=>f.actualTrade.scoring.horizon))];
  if(horizons.length>1) return {byHorizon:Object.fromEntries(horizons.map(h=>[h,calibrationSummary(forecasts.filter(f=>f.actualTrade && f.actualTrade.scoring && f.actualTrade.scoring.horizon===h))])),use:"Compare like horizons only; never pool short and long forecasts."};
  const measured=forecasts.map(f=>f.actualTrade).filter(t=>t && t.scoring && t.scoring.errorBps != null);
  const n=measured.length, errors=measured.map(t=>Number(t.scoring.errorBps));
  const bias=n?errors.reduce((a,b)=>a+b,0)/n:0, weight=n/(n+30);
  const sd=n>1?Math.sqrt(errors.reduce((a,e)=>a+(e-bias)**2,0)/(n-1)):null;
  return {sampleSize:n,minimumForCalibration:30,calibrated:n>=30,priorWeight:30,
    shrinkageAdjustedErrorBps:n?String(Math.round(weight*bias)):null,
    standardErrorBps:sd==null?null:String(Math.round(sd/Math.sqrt(n))),
    use:"Descriptive forecast error only. Samples overlap and are not independent; never multiply capital by self-reported confidence or this small-sample summary."};
}
async function actualTradeOutcome({record,series,nowMs,admin}) {
  if(record.decision !== "BUY" || !record.conditionalForecast) return null;
  if(!record.mandateVersionId) return {status:"UNKNOWN_ENTRY_LINEAGE",scoring:null};
  const D=db(admin), all=rows(await D.col(D.COL.fills).where("symbol","==",record.symbol).get())
    .filter(f=>f.accountId===record.accountId && Number(f.receivedAtMs || f.eventAtMs)<=nowMs);
  const entries=all.filter(f=>f.mandateVersionId===record.mandateVersionId && f.role === "ENTRY").sort((a,b)=>a.eventAtMs-b.eventAtMs);
  const fillScoring=record.entryExpiryMs && nowMs>=record.entryExpiryMs ? {probabilityPpm:record.fillProbabilityByExpiryPpm,occurred:entries.some(f=>Number(f.eventAtMs)<=record.entryExpiryMs),basis:"ENTRY_FILLED_BY_AUTHORIZED_EXPIRY"} : null;
  if(!entries.length) return {status:"NOT_FILLED",scoring:null,fillScoring,fillProbabilityByExpiryPpm:record.fillProbabilityByExpiryPpm,
    note:"A proposed limit is not a fill. Do not score its conditional return as trade performance."};
  const qty=entries.reduce((n,f)=>n+BigInt(f.quantityUnits),0n), cost=entries.reduce((n,f)=>n+BigInt(f.quantityUnits)*BigInt(f.priceMicros)+BigInt(f.feeMinor || 0)*10000n,0n);
  const entry=(entries.reduce((n,f)=>n+BigInt(f.quantityUnits)*BigInt(f.priceMicros),0n)/qty).toString(), lifecycle=entries[0].positionLifecycleId;
  const exits=lifecycle?all.filter(f=>f.positionLifecycleId===lifecycle && f.side === "sell"):[];
  const sold=exits.reduce((n,f)=>n+BigInt(f.quantityUnits),0n), proceeds=exits.reduce((n,f)=>n+BigInt(f.quantityUnits)*BigInt(f.priceMicros)-BigInt(f.feeMinor || 0)*10000n,0n);
  const fc=record.conditionalForecast;
  const derived=require("./_investorValuation").forecastFromScenarios({basis:fc.basis,outcomeBuckets:fc.outcomeBuckets,fillProbabilityByExpiryPpm:fc.fillProbabilityByExpiryPpm});
  const raw=Array.isArray(series)?series:series && series.series || [], provenance=!Array.isArray(series) && series && series.provenance;
  const entryDate=require("./_investorMarket").sessionState(new Date(entries[0].eventAtMs)).date;
  const firstClose=require("./_investorDecisionContext").completedDaily(raw,nowMs).find(b=>b.date===entryDate);
  // An anchor can be established on the entry date. A late reconstruction
  // cannot know whether intervening corporate actions restated that close.
  const anchor=record.actualTrade && record.actualTrade.adjustmentAnchor || (firstClose && new Date(nowMs).toISOString().slice(0,10)===entryDate && provenance ?
    {date:firstClose.date,closeMicros:microsOf(firstClose.c).toString(),provider:provenance.provider,adjustment:provenance.adjustment}:null);
  const adjusted=rebaseSeries(series,anchor);
  const compatible=anchor && ((fc.basis.returnConvention==="ARITHMETIC_TOTAL_RETURN" && anchor.adjustment==="split_and_dividend") ||
    (fc.basis.returnConvention==="ARITHMETIC_PRICE_RETURN" && anchor.adjustment==="split_only"));
  const resolved=adjusted.ok && compatible ? resolveAgainstBars({record:{...record,referencePriceMicros:entry,resolved:{},referenceTime:new Date(entries[0].eventAtMs).toISOString(),conditionalOn:"ENTRY_FILLED_AT_REFERENCE_PRICE",expectedTerminalPriceMicros:derived.expectedTerminalPriceMicros,
    estimatedRoundTripCostMicrosPerShare:fc.basis.estimatedRoundTripCostMicrosPerShare},series:adjusted.series,nowMs}) : {scoring:null,resolved:{},excursions:null};
  return {status:exits.some(f=>f.closedPosition===true) || sold===qty?"CLOSED":"FILLED",adjustmentAnchor:anchor,fillScoring,horizonResolved:!!resolved.resolved[`h${fc.horizonTradingDays}`],calibrationIssue:compatible && adjusted.ok ? null : "POINT_IN_TIME_ADJUSTMENT_BASIS_UNAVAILABLE",filledUnits:qty.toString(),soldUnits:sold.toString(),averageEntryMicros:entry,
    entryFillIds:entries.map(f=>f.fillId),exitFillIds:exits.map(f=>f.fillId),
    realizedAfterFeesMinor:exits.length && exits.every(f=>f.realizedMinor!=null) ? exits.reduce((n,f)=>n+BigInt(f.realizedMinor),0n).toString() : null,
    scoring:resolved.scoring,excursions:resolved.excursions,forecastOutcomes:resolved.resolved,
    distinction:"Forecast calibration is terminal mark after an actual fill; realized trading P&L uses recorded exit fills and fees, including stops/targets."};
}
// Rebase a restated split/dividend-adjusted history onto its frozen price
// units. Comparing a new adjusted close directly to an old raw quote is wrong.
function rebaseSeries(pack,anchor) {
  const series=Array.isArray(pack)?pack:pack && pack.series || [];
  if(!anchor) return {ok:true,series}; // legacy records remain explicitly uncalibrated
  const provenance=!Array.isArray(pack) && pack && pack.provenance;
  if(!provenance || !anchor.provider || provenance.provider!==anchor.provider || provenance.adjustment!==anchor.adjustment)
    return {ok:false,series:[],reason:"MARKET_IDENTITY_NOT_COMPARABLE"};
  const reference=series.find(b=>b.date===anchor.date), current=reference && microsOf(reference.c);
  if(!current) return {ok:false,series:[],reason:"ADJUSTMENT_ANCHOR_MISSING"};
  const old=BigInt(anchor.closeMicros);
  return {ok:true,series:series.map(b=>({...b,...Object.fromEntries(["o","h","l","c"].filter(k=>microsOf(b[k])).map(k=>[k,Number(microsOf(b[k])*old/current)/1e6]))}))};
}
function safeDecode(d) { try { return require("./_investorStorageCodec").decode(d); } catch { return null; } }
/** Sol's offline attribution for a selected case; adjudication stays with the operator. */
async function writePostmortem({ decisionId, frozenDecision, laterOutcome, gateway = null, admin = null, nowMs = Date.now() } = {}) {
  const D = db(admin);
  const O = gateway || lazy("./_investorOpenai");
  if (!O || typeof O.writePostmortem !== "function") return { written: false, reason: "gateway_unavailable" };
  const r = await O.writePostmortem({ frozenDecision: { decisionId, ...frozenDecision }, laterOutcome });
  const id = `pm_${sha(`${decisionId}|${sha(laterOutcome || {})}`).slice(0, 24)}`;
  const doc = { schemaVersion: POSTMORTEM_SCHEMA, postmortemId: id, decisionId, symbol:frozenDecision && frozenDecision.symbol || null, ok: r.ok === true, postmortem: r.ok ? r.postmortem : null, error: r.ok ? null : r.error, model: r.model || null, costMinor: r.costMinor || null,
    operatorAdjudicationRequired: r.ok ? r.postmortem.operatorAdjudicationRequired === true : true, adjudicated: false, promotesAnything: false, createdAtMs: nowMs, ...D.envelope({ created_by: "learning.postmortem" }) };
  await D.col(D.COL.postmortems).doc(id).set(doc, { merge: true });
  return { written: true, postmortemId: id, ok: r.ok === true, evalCase: r.ok ? r.postmortem.proposedEvalCase : null };
}

module.exports = { FORECAST_SCHEMA, COUNTERFACTUAL_SCHEMA, POSTMORTEM_SCHEMA, HORIZONS, ANTI_ANCHOR_PPM,
  forecastIdFor, outcomeRecordFor, barsAfter, resolveAgainstBars, counterfactualRow, antiAnchoringSample, reconcileAntiAnchoring,
  rebaseSeries, actualTradeOutcome, calibrationSummary, freezeDecisionOutcomeRecords, pendingForecasts, resolveDue, retrievalMemory, writePostmortem };
