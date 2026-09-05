"use strict";

/* Decision inputs are immutable facts, never a stock score or an order. */
const crypto = require("crypto");
const A = require("./_investorAdmin");
const P = require("./_investorPolicy");
const hash = (v) => crypto.createHash("sha256").update(typeof v === "string" ? v : JSON.stringify(P.canonical(v))).digest("hex");
const fail = (code) => Object.assign(new Error(code), { code });

// Preserve the issuer's original value/unit; arithmetic consumes the normalized value.
function moneyMicros(value, unit) {
  if (value == null || !/^-?(0|[1-9][0-9]*)$/.test(String(value))) return null;
  const scale = { USD: 1000000n, USD_millions: 1000000000000n, USD_billions: 1000000000000000n,
    USD_thousands: 1000000000n, USD_per_share: 1000000n, USD_micros: 1n }[unit];
  return scale == null ? null : (BigInt(value) * scale).toString();
}
function riskInputs(liquidity = {}, portfolio = {}) {
  return { marks: Object.fromEntries((portfolio.positions || []).filter(p => p.markMicros).map(p => [p.symbol, p.markMicros])),
    advBySymbol: Object.fromEntries(Object.entries(liquidity).map(([s, x]) => [s, x.advMinor])), liquidity };
}
function availableAt(row) {
  return Math.max(Number(row.knownAtMs || row.createdAtMs || row.asOfMs) || 0,
    Number(row.firstSeenAtMs || row.fetchedAtMs) || 0);
}
function unavailableCard(symbol, cutoffMs, reason) {
  return { symbol, asOf: new Date(cutoffMs).toISOString(), unavailable: true, price: null, dossierHash: null,
    dossierVersionId: null, changes: [], dataQuality: { complete: false, missing: [reason || "no_dossier"] },
    instruction: "Evidence unavailable: ABSTAIN for a new investment. Review any existing holding using its preserved thesis and protection." };
}

// Chunked JSON keeps every Firestore document below its size limit. A transaction
// publishes the manifest and chunks together. Subsequent reads verify the bytes.
async function freeze({ runId, context, admin = null }) {
  const D = admin || A, col = D.col(D.COL.decisionInputs || `${D.COL.managerRuns}Inputs`);
  const json = JSON.stringify(context), contentHash = hash(json), chunks = [];
  for (let i = 0; i < json.length; i += 180000) chunks.push(json.slice(i, i + 180000));
  const ref = col.doc(runId);
  await D.runTransaction(async tx => {
    const old = await tx.get(ref);
    if (old.exists) return;
    chunks.forEach((text, i) => tx.set(col.doc(`${runId}_${i}`), { runId, index: i, text }));
    tx.set(ref, { runId, contentHash, chunks: chunks.length, createdAtMs: A.now(), schemaVersion: "decision-inputs.v1" });
  });
  return read({ runId, admin: D });
}
async function read({ runId, admin = null }) {
  const D = admin || A, col = D.col(D.COL.decisionInputs || `${D.COL.managerRuns}Inputs`);
  const h = await col.doc(runId).get();
  if (!h.exists) throw fail("FROZEN_INPUTS_MISSING");
  const manifest = h.data(), parts = [];
  for (let i = 0; i < manifest.chunks; i++) {
    const part = await col.doc(`${runId}_${i}`).get();
    if (!part.exists) throw fail("FROZEN_INPUT_CHUNK_MISSING");
    parts.push(part.data().text);
  }
  const json = parts.join("");
  if (hash(json) !== manifest.contentHash) throw fail("FROZEN_INPUT_HASH_MISMATCH");
  return { ...JSON.parse(json), contentHash: manifest.contentHash };
}

function completedDaily(series, cutoffMs) {
  const M = require("./_investorMarket");
  return (series || []).filter(b => b && b.date && Number(b.c) > 0 &&
    M.sessionCloseMs(new Date(`${b.date}T16:00:00Z`)) != null && M.sessionCloseMs(new Date(`${b.date}T16:00:00Z`)) + 20*60000 <= cutoffMs).sort((a,b) => a.date.localeCompare(b.date));
}
function barTime(b) { const n = Number(b.t); return Number.isFinite(n) ? (n < 1e12 ? n * 1000 : n) : Date.parse(b.t || b.timestamp); }
async function marketObservation(symbol, { cutoffMs, deps = {} }) {
  const key = `${symbol}|${cutoffMs}`;
  const cache = deps.decisionMarketCache;
  if (cache && cache.has(key)) return cache.get(key);
  const task = (async () => {
    const H = deps.history || require("./_investorHistory"), M = deps.market || require("./_investorMarket");
    let daily = {}, recent = {};
    try { daily = H.readDailyWithMetaFor ? await H.readDailyWithMetaFor(deps.admin || null, symbol) : await H.readDailyWithMeta(symbol); } catch {}
    try { recent = await M.readRecentBarsWithMeta(symbol, 1, {asOfMs:cutoffMs}); } catch {}
    const bars = completedDaily(daily.series, cutoffMs), last = bars.at(-1);
    const active=M.activeProvider ? M.activeProvider() : {};
    const delayMinutes=recent.provenance && recent.provenance.feed === "delayed_sip" ? 15 : Number(active.delayMinutes ?? 15);
    const delayMs = delayMinutes * 60000; // Existing delayed paper feed; never label its ticks real-time.
    const intraday = (recent.bars || []).filter(b => Number(b.c) > 0 && barTime(b) + 5*60000 + delayMs <= cutoffMs &&
      (Number(b.knownAtMs || b.fetchedAtMs) > 0 && Number(b.knownAtMs || b.fetchedAtMs) <= cutoffMs)).sort((a,b) => barTime(a) - barTime(b));
    const tick = intraday.at(-1);
    const currentDay = require("./_investorMarket").sessionState(new Date(cutoffMs)).date;
    const usableTick = tick && require("./_investorMarket").sessionState(new Date(barTime(tick))).date === currentDay ? tick : null;
    const price = usableTick ? Number(usableTick.c) : last ? Number(last.c) : null;
    const returns = bars.slice(-21).slice(1).map((b,i) => Math.log(Number(b.c)/Number(bars.slice(-21)[i].c)));
    const avg = returns.length ? returns.reduce((a,b) => a+b,0)/returns.length : 0;
    const vol = returns.length >= 10 ? Math.sqrt(returns.reduce((a,b) => a+(b-avg)**2,0)/(returns.length-1)*252) : null;
    const avgVolume = bars.slice(-20).reduce((a,b) => a+(Number(b.v)||0),0)/(Math.min(20,bars.length)||1);
    const lastTime = usableTick ? barTime(usableTick) : last ? require("./_investorMarket").sessionCloseMs(new Date(`${last.date}T16:00:00Z`)) : null;
    return { symbol, cutoffMs, priceMicros: price ? String(Math.round(price*1e6)) : null,
      previousCloseMicros: last ? String(Math.round(Number(last.c)*1e6)) : null,
      adjustmentAnchor:last ? {date:last.date,closeMicros:String(Math.round(Number(last.c)*1e6)),provider:daily.provenance && daily.provenance.provider || null,adjustment:daily.provenance && daily.provenance.adjustment || null} : null,
      observedAtMs: lastTime, availableAtMs: lastTime == null ? null : lastTime + (usableTick ? 5*60000+delayMs : 20*60000),
      freshness: usableTick ? (cutoffMs-lastTime <= 25*60000 ? "DELAYED_INTRADAY" : "STALE_INTRADAY") : last ? "COMPLETED_DAILY_ONLY" : "MISSING",
      feedDelayMinutes: delayMinutes, intradayReturnBps: usableTick && last ? String(Math.round((price/Number(last.c)-1)*10000)) : null,
      realizedVolatility20dBps: vol == null ? null : String(Math.round(vol*10000)),
      latestDailyVolumeRatioMilli: avgVolume && last ? String(Math.round(Number(last.v)/avgVolume*1000)) : null,
      provenance: usableTick ? recent.provenance || null : daily.provenance || null,
      returns: require("./_investorDossier").returnsBps(bars, { asOfMs: cutoffMs }), bars: bars.slice(-60) };
  })();
  if (cache) cache.set(key, task);
  return task;
}
async function marketState({ cards = [], cutoffMs, deps = {} }) {
  const drivers = [...new Set(["SPY", "QQQ", "HYG", "LQD", "IEF", "TLT", "GLD", "UUP", ...Object.values(require("./_investorTemporal").DRIVER_BY_SECTOR || {})])];
  const observations = await Promise.all(drivers.map(symbol => marketObservation(symbol, { cutoffMs, deps })));
  let macro=[];
  try {macro=await (deps.evidence || require("./_investorEvidence")).documentsForCompany("SPY",cutoffMs,30,8,{admin:deps.admin});} catch {}
  const measured = cards.map(c => c.marketObservation).filter(x => x && x.intradayReturnBps != null && x.freshness === "DELAYED_INTRADAY");
  return { schemaVersion: "market-state.v1", cutoffMs, observations: observations.map(({ bars, ...x }) => x),
    breadth: { universeCount: cards.length, observedCount: measured.length, advancing: measured.filter(x=>BigInt(x.intradayReturnBps)>0n).length,
      declining: measured.filter(x=>BigInt(x.intradayReturnBps)<0n).length, basis: "available_delayed_intraday_only", missingCount: cards.length-measured.length },
    macroEvidence: [...macro.filter(d=>Number(d.decisionKnownAtMs)<=cutoffMs).map(d=>({sourceId:d.sourceId,documentVersionId:d.versionId,publishedAt:d.source_published_at || null,knownAtMs:d.decisionKnownAtMs,title:d.title || null,summary:String(d.canonicalText || d.summary || "").slice(0,3000),link:d.link || null})), ...cards.flatMap(c => (c.changes || []).filter(x => /MACRO|RATE|CREDIT/i.test(x.eventClass || x.type || "")).map(x => ({symbol:c.symbol,...x})))].slice(0,12),
    creditAndRates: { available:observations.some(x=>["HYG","LQD","IEF","TLT"].includes(x.symbol) && x.priceMicros), basis:"ETF price proxies only: HYG/LQD credit appetite, IEF/TLT duration, GLD gold, UUP dollar; these are not observed credit spreads or policy rates.", directRatesAndSpreadsAvailable:false },
    interpretationRequired: "Distinguish broad risk appetite, sector leadership, company change, priced-in expectations and uncertainty. Explain the effect on entry, horizon, size or waiting. Incomplete breadth is not whole-market breadth." };
}
module.exports = { barTime, hash, moneyMicros, riskInputs, availableAt, unavailableCard, freeze, read, completedDaily, marketObservation, marketState };
