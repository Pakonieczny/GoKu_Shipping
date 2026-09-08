/*  netlify/functions/investorExecution-background.js  (fund-manager-v1)
 *  ---------------------------------------------------------------------------
 *  Investor AI — the executor (blueprint §8.1, §8.6, §11.1). Runs every
 *  minute inside the execute window as the reconciliation fallback for
 *  broker-native orders. It imports NO model code: a price touch executes
 *  the already-authorized leg; nothing here asks a model anything.
 *
 *  Per tick: expire unfilled entries past their authorized sessions, pause
 *  entries with an unreviewed high-impact delta, revalidate operational
 *  limits (freeze expansion on a breach, emergency policy on a hard
 *  breach), apply pending outbox transitions through the bound adapter,
 *  simulate paper fills over the latest stored bars, and assert ledger
 *  conservation — pausing the executor for safety if it fails.
 * ---------------------------------------------------------------------------
 */

"use strict";

/* invariant: the executor never loads the model gateway. Measured against
   what THIS module's imports bring in (a host that already holds the
   gateway, such as the attestation runner, is not the executor's doing);
   the source-level dependency walk in the fixture set is the second lock. */
const preloadedModules = new Set(Object.keys(require.cache));
const A = require("./_investorAdmin");
const AUTH = require("./_investorAuth");
const M = require("./_investorMarket");
const JOBS = require("./_investorJobs");
const X = require("./_investorExecution");
const B = require("./_investorBroker");
const { redact } = require("./_investorAuth");

const FN_NAME = "investorExecution-background";
const TASK = "execute";
if (Object.keys(require.cache).some((k) => !preloadedModules.has(k) && /_investorOpenai\.js$/.test(k))) throw new Error("executor must not load the model gateway");

async function controlDoc() { const s = await A.col(A.COL.control).doc("control").get(); return s.exists ? s.data() : {}; }

async function barsForOpenSymbols(accountId) {
  const [pointers,positions,sets]=await Promise.all([A.col(A.COL.activeMandates).where('accountId','==',accountId).get(),A.col(A.COL.positions).where('accountId','==',accountId).where('open','==',true).get(),A.col(A.COL.orderSets).where('accountId','==',accountId).get()]);
  const rows=s=>s.docs.map(d=>d.data());
  const active=rows(sets).filter(x=>x.coreVersion&&!x.closed&&!x.entryExpired);
  const symbols=[...new Set([...rows(pointers).filter(p=>!['CLOSED','CANCELLED','SUPERSEDED'].includes(p.status)),...rows(positions),...active].map(p=>p.symbol))];
  const overrides=rows(sets).filter(x=>!x.coreVersion&&!['CLOSED','CANCELLED','FILLED','COMPLETE','REJECTED'].includes(x.status));
  const legacySymbols=[...new Set([...symbols.filter(s=>!active.some(x=>x.symbol===s)),...overrides.map(x=>x.symbol)])];
  const barsBySymbol={},provenanceBySymbol={},sharedBarsBySymbol={},sharedProvenanceBySymbol={};
  // Execution data refresh is independent of paid research and review pauses.
  // Keep legacy minute bars and shared five-minute bars in separate lanes.
  if(legacySymbols.length){
    const data=await M.fetchBarsChunked(legacySymbols,{timeframe:'1Min',limit:390,recentMinutes:360});
    for(const [s,bars] of Object.entries(data.bars||{})){barsBySymbol[s]=bars;provenanceBySymbol[s]={provider:data.provider,feed:data.feed||null,adjustment:data.adjustment||null};}
  }
  if(active.length){
    const data=await M.fetchBarsChunked([...new Set(active.map(x=>x.symbol))],{timeframe:'5Min',limit:390});
    for(const [s,bars] of Object.entries(data.bars||{})){sharedBarsBySymbol[s]=bars;sharedProvenanceBySymbol[s]={provider:data.provider,feed:data.feed||null,adjustment:data.adjustment||null,timeframe:'5Min',sourceSha256:data.symbolSha256?.[s]||data.sha256||null};}
  }
  return {barsBySymbol,provenanceBySymbol,sharedBarsBySymbol,sharedProvenanceBySymbol,symbols};
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
  const ctrl = await controlDoc();
  const accountId = String(payload.accountId || ctrl.accountId || "paper-1");
  try {
    if (ctrl.engineMode !== "manager") { await JOBS.complete(claim, { skipped: true, reason: "engine_mode_legacy" }); return { statusCode: 200, body: JSON.stringify({ ok: true, skipped: true }) }; }
    if (ctrl.executorState === "PAUSED_SAFETY" || ctrl.executorEnabled === false) { await JOBS.complete(claim, { skipped: true, reason: "executor_paused_safety" }); return { statusCode: 200, body: JSON.stringify({ ok: true, skipped: true, reason: "executor_paused_safety" }) }; }
    const adapter = B.adapterFor({ control: ctrl });
    const { barsBySymbol, provenanceBySymbol, sharedBarsBySymbol, sharedProvenanceBySymbol, symbols } = await barsForOpenSymbols(accountId);
    const brokerSnap = await adapter.getAccountSnapshot(accountId).catch((e) => ({ error: String(e.code || e.message).slice(0, 80), truthAgeSeconds: null }));
    const summary = await X.tick({ adapter, accountId, control: ctrl, barsBySymbol, provenanceBySymbol, sharedBarsBySymbol, sharedProvenanceBySymbol, nowMs: Date.now(),
      metrics: { brokerTruthAgeSeconds: brokerSnap.truthAgeSeconds, reconciliationUnresolved: !!brokerSnap.error, ...(ctrl.executorMetrics || {}) } });
    const compact = { ...summary, outbox: summary.outbox ? { applied: summary.outbox.applied, results: (summary.outbox.results || []).slice(0, 20) } : null,
      fills: summary.fills ? { fills: summary.fills.fills.length+(summary.shared?.fills||0), protectionAttached: summary.fills.protectionAttached, closed: summary.fills.closed, ambiguous: summary.fills.ambiguous } : null, symbols: symbols.length, adapter: adapter.adapter };
    await A.col(A.COL.control).doc("control").set({ lastExecutionTick: { atMs: Date.now(), accountId, adapter: adapter.adapter, expired: summary.expired, paused: summary.paused.length, allowExpansion: summary.operational ? summary.operational.allowExpansion : null,
      reasons: summary.operational ? summary.operational.reasons : [], fills: compact.fills, shared:summary.shared?{plans:summary.shared.plans}:null, conservation: summary.conservation, outboxApplied: summary.outbox ? summary.outbox.applied : 0 } }, { merge: true });
    await JOBS.complete(claim, compact);
    return { statusCode: 200, body: JSON.stringify({ ok: true, summary: compact }) };
  } catch (e) {
    await A.col(A.COL.control).doc('control').set({lastExecutionError:{atMs:Date.now(),code:e.code||'EXECUTION_TICK_FAILED',message:String(e.message).slice(0,240)}},{merge:true}).catch(()=>{});
    console.error("investorExecution tick failed", redact({ jobId, error: e.message, stack: (e.stack || "").slice(0, 400) }));
    await JOBS.failClosed(claim, { code: e.code || "EXECUTION_TICK_FAILED", message: e.message, retryable: true }).catch(() => ({}));
    return { statusCode: 500, body: JSON.stringify({ ok: false, error: String(e.message).slice(0, 200) }) };
  }
};

exports.FN_NAME = FN_NAME;
exports.barsForOpenSymbols = barsForOpenSymbols;
