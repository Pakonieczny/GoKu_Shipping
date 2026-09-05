/*  netlify/functions/investorManager-background.js  (fund-manager-v1)
 *  ---------------------------------------------------------------------------
 *  Investor AI — the resumable manager handler (blueprint §6.2, §11.1,
 *  §12.1, Appendix B). Four tasks share one worker, separate from the
 *  executor (which imports no model code):
 *
 *    premarket_manager    the one logical Manager Meeting, a state machine
 *                         that checkpoints after every stage and yields on
 *                         the segment budget or a pending Sol background
 *                         response; the run lease (D-10) spans segments
 *    event_revision       a later material-event revision of one symbol
 *    focused_research     one standalone research job (operator-requested
 *                         or a deferred request re-run); never a BUY on its
 *                         own
 *    portfolio_synthesis  the event-driven re-synthesis after a non-holding
 *                         event research: the completed memo set is compared
 *                         together again and, if a basket is chosen, staged
 *                         against a fresh activation snapshot
 *
 *  Every invocation is claimed once with a bound nonce; nothing here trades:
 *  activation goes through _investorMandate.stagePortfolioPlan and the
 *  executor applies committed desired state.
 * ---------------------------------------------------------------------------
 */

"use strict";

const A = require("./_investorAdmin");
const AUTH = require("./_investorAuth");
const M = require("./_investorMarket");
const JOBS = require("./_investorJobs");
const POLICY = require("./_investorPolicy");
const MANAGER = require("./_investorManager");
const { redact } = require("./_investorAuth");

const FN_NAME = "investorManager-background";
const TASKS = Object.freeze(["premarket_manager", "event_revision", "focused_research", "portfolio_synthesis"]);
const SEGMENT_SAFETY_MS = 45000;

async function controlDoc() {
  const s = await A.col(A.COL.control).doc("control").get();
  return s.exists ? s.data() : {};
}
function budgetFor(claim) {
  return () => JOBS.segmentBudgetRemainingMs(claim, { safetyMs: SEGMENT_SAFETY_MS });
}

async function runFocusedResearch(claim, ctrl, partial={}) {
  const deps={...MANAGER.defaultDeps(partial.admin || null),...partial,decisionMarketCache:new Map()};
  const payload=claim.payload || {}, accountId=String(payload.accountId || ctrl.accountId || "paper-1"), symbol=String(payload.symbol || "").toUpperCase();
  if(!symbol) return {ok:false,reason:"symbol_required"};
  const C=require("./_investorDecisionContext"),contextId=`${claim.jobId || claim.runId}_focused`,R=deps.research;
  let input;
  try {input=await C.read({runId:contextId,admin:deps.admin});}
  catch(e) {
    if(e.code!=="FROZEN_INPUTS_MISSING") throw e;
    const cutoffMs=Number(payload.cutoff) || deps.now(), policy=POLICY.loadActiveSync(ctrl);
    const prior=await R.latest(symbol,{admin:deps.admin,cutoffMs});
    const packet=await R.buildPacket({symbol,cutoff:cutoffMs,prior,directive:payload.directive || "RESEARCH_NOW",admin:deps.admin,deps});
    const portfolio=await deps.portfolio.getSnapshot({accountId,asOfMs:deps.now(),admin:deps.admin});
    const marks=await MANAGER.liquidityMarks([symbol],deps,{policy,cutoffMs});
    input=await C.freeze({runId:contextId,admin:deps.admin,context:{cutoffMs,policy,packet,portfolio,marks}});
  }
  const {cutoffMs,policy,packet,portfolio,marks}=input;
  if(!packet.ok) return {ok:false,symbol,reason:packet.reason,decision:"ABSTAIN",reasonCode:"DATA_INCOMPLETE"};
  const bound=deps.tools.allowlisted(deps.tools.productionBindings({accountId,admin:deps.admin,policy,portfolio,marks,decisionPacket:packet,sectorOf:MANAGER.sectorLookup(deps)}),policy.toolPolicy,{symbol,cutoffMs});
  const r = await deps.gateway.researchCompany({ dossier: packet, delta: packet.delta, portfolio, tools: bound.tools, directive: payload.directive || "RESEARCH_NOW", prior: packet.prior, cutoffMs });
  if(r.pending) return {pending:true};
  const persisted = await R.persistImmutable({ ...r, dossierVersionId: packet.dossierVersionId, dossierHash: packet.dossierHash }, { admin:deps.admin, managerRunId: claim.runId || null, directive: payload.directive || "RESEARCH_NOW", cutoffMs });
  if (persisted.persisted && deps.claimVerifier) { try { await deps.claimVerifier.verifyAndPersist({ premises: persisted.factualPremises, sourceManifest: persisted.sourceManifest, admin:deps.admin }); } catch {} }
  /* a standalone research job never creates a BUY: a BUY proposal waits for a portfolio synthesis */
  return { ok: persisted.persisted === true, symbol, memoId: persisted.memoId || null, proposedDecision: persisted.proposedDecision || null,
    reasonCode: persisted.reasonCode || null, standaloneBuy: false, costMinor: r.costMinor || "0", toolCalls: bound.log.length, error: persisted.persisted ? null : persisted.reason };
}

/** Event-driven re-synthesis: the morning run's researched set plus the
 *  changed symbol, compared together once more (Appendix B). */
async function runPortfolioSynthesis(claim, ctrl, partial = {}) {
  const deps={...MANAGER.defaultDeps(partial.admin || null),...partial,decisionMarketCache:new Map()};
  const D=deps.admin || A, C=require("./_investorDecisionContext"), payload=claim.payload || {};
  const accountId=String(payload.accountId || ctrl.accountId || "paper-1"), contextId=`${claim.jobId || claim.runId}_synthesis`;
  if(ctrl.freezeNewBuys || ctrl.managerState === "PAUSED" || ctrl.killSwitch) return {ok:true,skipped:true,reason:"FREEZE_STATE"};
  if(claim.payload && claim.payload.awaitEntryCancellation) {
    const live=await deps.portfolio.symbolState({accountId,symbol:claim.payload.awaitEntryCancellation,admin:D});
    if((live.entries || []).length) return {pending:true,reason:"AWAITING_ENTRY_CANCEL_ACK"};
    if(live.ownedQuantityUnits !== "0") return {ok:false,reason:"ENTRY_FILLED_DURING_REVISION_REVIEW"};
  }
  let context;
  try { context=await C.read({runId:contextId,admin:D}); }
  catch(e) {
    if(e.code !== "FROZEN_INPUTS_MISSING") throw e;
    const cutoffMs=deps.now(), policy=POLICY.loadActiveSync(ctrl), morning=ctrl.lastManagerRun || null;
    const portfolio=await deps.portfolio.snapshot({accountId,admin:D,asOfMs:cutoffMs});
    const symbols=new Set([payload.changedSymbol].filter(Boolean)), coverage=[];
    if(morning && morning.managerRunId) (await D.col(D.COL.managerDecisions).where("managerRunId","==",morning.managerRunId).get()).forEach(d=>{
      const x=d.data()._codec?require("./_investorStorageCodec").decode(d.data()):d.data();coverage.push(x);
      if(["WATCH","BUY"].includes(x.decision) && x.researchMemoId) symbols.add(x.symbol);
    });
    const held=portfolio.positions.map(p=>p.symbol), researchResults=[];
    for(const symbol of symbols) {
      if(held.includes(symbol)) continue;
      const memo=await deps.research.latest(symbol,{admin:D,cutoffMs});
      if(memo && memo.memo) researchResults.push({symbol,memo:memo.memo,memoId:memo.memoId,verifiedValuation:memo.verifiedValuation || null,factualPremises:memo.factualPremises || []});
    }
    const holdings=await deps.dossier.expandedHoldingDeltas({symbols:held,cutoff:cutoffMs,admin:D,deps,
      portfolioBySymbol:Object.fromEntries(held.map(s=>[s,{held:true,position:portfolio.positions.find(p=>p.symbol===s),mandate:portfolio.activeMandates.find(m=>m.symbol===s) || null}]))});
    const marks=await MANAGER.liquidityMarks(researchResults.map(r=>r.symbol),deps,{policy,cutoffMs});
    const marketState=await C.marketState({cards:holdings.packets.map(p=>p.card),cutoffMs,deps});
    const roster=deps.universe.freezeEligibleSnapshot({tradingDate:M.sessionState(new Date(cutoffMs)).date,nowMs:cutoffMs,removed:ctrl.universeRemovals || []});
    context=await C.freeze({runId:contextId,admin:D,context:{cutoffMs,policy,portfolio,researchResults,holdings,marks,marketState,coverage,roster,
      expansionBlocked:!morning || !morning.coverage || !morning.coverage.ok || morning.tradingDate !== M.sessionState(new Date(cutoffMs)).date}});
  }
  const {policy,cutoffMs,researchResults,holdings,marks,marketState,roster,coverage}=context;
  if(!researchResults.length && !holdings.packets.length) return {ok:true,skipped:true,reason:"NO_RESEARCH_TO_COMPARE"};
  const state=claim.checkpoint && claim.checkpoint.data || {attempt:0};
  let staged={status:"EMPTY"}, synthesis;
  for(;state.attempt<2;state.attempt++) {
    // Freeze the comparison book per attempt, including changed capacities on
    // the one permitted re-synthesis. Activation always rechecks fresh truth.
    const bookId=`${contextId}_book_${state.attempt}`;
    let book;
    try{book=(await C.read({runId:bookId,admin:D})).portfolio;}catch(e){if(e.code!=="FROZEN_INPUTS_MISSING")throw e;book=(await C.freeze({runId:bookId,admin:D,context:{portfolio:await deps.portfolio.snapshot({accountId,admin:D,asOfMs:deps.now()})}})).portfolio;}
    const feasible=deps.portfolioRisk.buildFeasibleAlternatives({researchResults:researchResults.map(r=>({...r,mandate:r.memo.mandate,liquidity:marks[r.symbol]})),holdings:holdings.packets,portfolio:book,policy,...C.riskInputs(marks,book),sectorOf:MANAGER.sectorLookup(deps)});
    const r=await deps.gateway.finalizePortfolio({coverage,researchResults,holdings:holdings.packets,portfolio:book,feasibleAlternatives:feasible,
      resynthesisFeedback:state.feedback || null,marketState,expansionBlocked:context.expansionBlocked,policy,contextManifestHash:C.hash({context:context.contentHash,book,attempt:state.attempt})});
    if(r.pending) return {pending:true,checkpoint:{stage:"synthesis",data:state}};
    if(!r.ok) return {ok:false,reason:r.error};
    synthesis=r.synthesis;
    const maintenance=await MANAGER.stageHoldingAnalyses({analyses:synthesis.holdingAnalysis || [],holdings:holdings.packets.map(p=>p.symbol),accountId,cutoff:{cutoffMs},policy,managerRunId:claim.runId || claim.jobId,deps,nowMs:deps.now()});
    if(maintenance.actionRequired.length) return {ok:false,reason:"HOLDING_ACTION_REQUIRED",maintenance};
    const mandates=synthesis.expansionMandates;
    if(!mandates.length || context.expansionBlocked) break;
    const verified=await deps.claimVerifier.verifyAndPersistBatch({proposals:mandates,claimsById:Object.fromEntries(researchResults.flatMap(r=>r.factualPremises.map(p=>[p.claimId,p]))),admin:D});
    if(!verified.allSupported) return {ok:false,reason:"CLAIMS_UNSUPPORTED"};
    const activation=await deps.portfolio.captureActivationSnapshot({accountId,reason:"EVENT_EXPANSION",admin:D});
    staged=await deps.mandate.stagePortfolioPlan({planClass:"EXPANSION",portfolioPlanProposal:synthesis,proposals:mandates,verifiedProposalClaims:verified,
      verifiedValuations:Object.fromEntries(researchResults.map(r=>[r.symbol,r.verifiedValuation])),activationSnapshot:activation,accountId,cutoff:{cutoffMs},policy,
      eligibleSymbols:roster.symbols,marks,sectorOf:MANAGER.sectorLookup(deps),managerRunId:claim.runId || claim.jobId,admin:D,nowMs:deps.now()});
    if(staged.status === "NEEDS_SOL_RESYNTHESIS" || ["BASKET_INFEASIBLE","ENVELOPE_REJECTED"].includes(staged.reason)) {
      state.feedback={reason:staged.reason,rejectedBasket:mandates,envelopes:staged.envelopes || [],alternatives:staged.revisedFeasibleAlternatives || null};
      continue;
    }
    break;
  }
  const decisions=(synthesis && synthesis.decisions || []).map(d=>d.decision === "BUY" && staged.status !== "COMMITTED" ? {...d,decision:"WATCH",fundingState:"UNFUNDED",reasonCode:"UNFUNDED",reason:`BUY not activated: ${staged.reason || staged.status}`} : d);
  await MANAGER.persistDecisionRows({managerRunId:claim.runId || claim.jobId,tradingDate:M.sessionState(new Date(cutoffMs)).date,accountId,decisions,contextManifestHash:context.contentHash,expansionPortfolioPlanId:staged.planId || null,cutoffMs,admin:D});
  return {ok:true,staged,decisions};
}

exports.handler = async (event) => {
  let body = {};
  try { body = JSON.parse(event.body || "{}"); } catch { return { statusCode: 400, body: JSON.stringify({ error: "invalid JSON" }) }; }
  const { jobId, task, nonce, payload = {} } = body;
  if (!TASKS.includes(task) || !jobId) return { statusCode: 400, body: JSON.stringify({ error: "invalid job shape" }) };
  await AUTH.loadAuthSecrets();
  const claimed = await JOBS.claimOnce({ jobId, task, targetFunction: FN_NAME, token: nonce, payload });
  if (!claimed.claimed) return { statusCode: claimed.httpStatus || 409, body: JSON.stringify({ ok: false, reason: claimed.reason }) };
  const claim = claimed.claim;
  await M.loadMarketSettings();
  const ctrl = await controlDoc();
  try {
    if (ctrl.engineMode !== "manager") {
      await JOBS.failClosed(claim, { code: "ENGINE_MODE_LEGACY", message: "manager engine is not the active engine", retryable: false });
      return { statusCode: 409, body: JSON.stringify({ ok: false, reason: "engine_mode_legacy" }) };
    }
    if (task === "premarket_manager") {
      const runId = claim.runId || JOBS.runIdFor({ task, accountId: payload.accountId || ctrl.accountId || "paper-1", tradingDate: payload.tradingDate || M.sessionState(new Date()).date });
      const lease = await JOBS.claimRunLease(claim, { runId });
      if (!lease.claimed) {
        await JOBS.yieldSegment(claim, { reason: `run_in_flight:${lease.heldBy || "unknown"}`, resumeAtMs: Date.now() + 60000 });
        return { statusCode: 202, body: JSON.stringify({ ok: true, yielded: true, reason: "run_in_flight" }) };
      }
      const out = await MANAGER.runManagerMeeting({ claim: { ...claim, runId }, budget: budgetFor(claim), control: ctrl });
      if (out.yielded) {
        await JOBS.yieldSegment(claim, { reason: out.reason, checkpoint: out.checkpoint, resumeAtMs: out.resumeAtMs || Date.now() + 5000 });
        return { statusCode: 202, body: JSON.stringify({ ok: true, yielded: true, stage: out.checkpoint.stage, reason: out.reason }) };
      }
      if (out.failed) {
        await JOBS.failClosed(claim, { code: out.reason || "MANAGER_FAILED_CLOSED", message: JSON.stringify(out.summary && out.summary.noBuyReasons || []).slice(0, 280), retryable: false, data: out.summary || null });
        try { await A.col(A.COL.control).doc("control").set({ lastManagerRunDate: payload.tradingDate || null, lastManagerRun: { ...(out.summary || {}), status: "failed_closed" } }, { merge: true }); } catch {}
        return { statusCode: 200, body: JSON.stringify({ ok: false, failedClosed: true, reason: out.reason, noBuyReasons: out.summary && out.summary.noBuyReasons }) };
      }
      await JOBS.complete(claim, out.summary);
      console.log("investorManager meeting complete", JSON.stringify(redact({ ...out.summary, requestIds: undefined })));
      return { statusCode: 200, body: JSON.stringify({ ok: true, summary: out.summary }) };
    }
    let result;
    if (task === "event_revision") result = await MANAGER.runEventRevision({ claim, control: ctrl });
    else if (task === "focused_research") result = await runFocusedResearch(claim, ctrl);
    else result = await runPortfolioSynthesis(claim, ctrl);
    if(result.pending) {await JOBS.yieldSegment(claim,{reason:"model_background_pending",checkpoint:result.checkpoint || claim.checkpoint,resumeAtMs:Date.now()+15000});return{statusCode:202,body:JSON.stringify({ok:true,pending:true})};}
    await JOBS.complete(claim, result);
    return { statusCode: 200, body: JSON.stringify({ ok: true, result }) };
  } catch (e) {
    console.error("investorManager segment failed", redact({ jobId, task, error: e.message, stack: (e.stack || "").slice(0, 400) }));
    await JOBS.failClosed(claim, { code: e.code || "MANAGER_SEGMENT_FAILED", message: e.message, retryable: !/CODEC|COVERAGE/.test(String(e.code)) }).catch(() => ({}));
    return { statusCode: 500, body: JSON.stringify({ ok: false, error: String(e.message).slice(0, 200) }) };
  }
};

exports.FN_NAME = FN_NAME;
exports.TASKS = TASKS;
exports.runFocusedResearch = runFocusedResearch;
exports.runPortfolioSynthesis = runPortfolioSynthesis;
