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

async function runFocusedResearch(claim, ctrl) {
  const deps = MANAGER.defaultDeps(null);
  const payload = claim.payload || {};
  const accountId = String(payload.accountId || ctrl.accountId || "paper-1");
  const symbol = String(payload.symbol || "").toUpperCase();
  if (!symbol) return { ok: false, reason: "symbol_required" };
  const policy = POLICY.loadActiveSync(ctrl);
  const cutoffMs = Number(payload.cutoff) || Date.now();
  const R = deps.research;
  const prior = await R.latest(symbol);
  const packet = await R.buildPacket({ symbol, cutoff: cutoffMs, prior, directive: payload.directive || "RESEARCH_NOW" });
  if (!packet.ok) return { ok: false, symbol, reason: packet.reason, decision: "ABSTAIN", reasonCode: "DATA_INCOMPLETE" };
  const portfolio = await deps.portfolio.getSnapshot({ accountId, asOfMs: cutoffMs });
  const bound = deps.tools.allowlisted(deps.tools.productionBindings({ accountId }), policy.toolPolicy, { symbol, cutoffMs });
  const r = await deps.gateway.researchCompany({ dossier: packet, delta: packet.delta, portfolio, tools: bound.tools, directive: payload.directive || "RESEARCH_NOW", prior: packet.prior, cutoffMs });
  const persisted = await R.persistImmutable({ ...r, dossierVersionId: packet.dossierVersionId, dossierHash: packet.dossierHash }, { managerRunId: claim.runId || null, directive: payload.directive || "RESEARCH_NOW", cutoffMs });
  if (persisted.persisted && deps.claimVerifier) { try { await deps.claimVerifier.verifyAndPersist({ premises: persisted.factualPremises, sourceManifest: persisted.sourceManifest }); } catch {} }
  /* a standalone research job never creates a BUY: a BUY proposal waits for a portfolio synthesis */
  return { ok: persisted.persisted === true, symbol, memoId: persisted.memoId || null, proposedDecision: persisted.proposedDecision || null,
    reasonCode: persisted.reasonCode || null, standaloneBuy: false, costMinor: r.costMinor || "0", toolCalls: bound.log.length, error: persisted.persisted ? null : persisted.reason };
}

/** Event-driven re-synthesis: the morning run's researched set plus the
 *  changed symbol, compared together once more (Appendix B). */
async function runPortfolioSynthesis(claim, ctrl) {
  const deps = MANAGER.defaultDeps(null);
  const payload = claim.payload || {};
  const accountId = String(payload.accountId || ctrl.accountId || "paper-1");
  const policy = POLICY.loadActiveSync(ctrl);
  const cutoffMs = Number(payload.cutoff) || Date.now();
  const R = deps.research;
  const morning = ctrl.lastManagerRun || null;
  if (ctrl.freezeNewBuys || ctrl.managerState === "PAUSED" || ctrl.killSwitch) return { ok: true, skipped: true, reason: "FREEZE_STATE" };
  const symbols = new Set([String(payload.changedSymbol || "").toUpperCase()].filter(Boolean));
  if (morning && morning.managerRunId) {
    const snap = await A.col(A.COL.managerDecisions).where("managerRunId", "==", morning.managerRunId).where("decision", "in", ["WATCH", "BUY"]).get().catch(() => ({ forEach: () => {} }));
    snap.forEach((d) => { const x = d.data(); if (x && x.researchMemoId) symbols.add(x.symbol); });
  }
  const portfolio = await deps.portfolio.snapshot({ accountId, asOfMs: cutoffMs });
  const held = new Set(portfolio.positions.map((p) => p.symbol));
  const researchResults = [];
  for (const s of symbols) { if (held.has(s)) continue; const m = await R.latest(s); if (m && m.memo) researchResults.push({ symbol: s, memo: m.memo, memoId: m.memoId, factualPremises: m.factualPremises || [] }); }
  if (!researchResults.length) return { ok: true, skipped: true, reason: "NO_RESEARCH_TO_COMPARE" };
  const holdings = await deps.dossier.expandedHoldingDeltas({ symbols: [...held], cutoff: cutoffMs });
  const feasible = deps.portfolioRisk && typeof deps.portfolioRisk.buildFeasibleAlternatives === "function"
    ? (() => { try { return deps.portfolioRisk.buildFeasibleAlternatives({ researchResults: researchResults.map((r) => ({ symbol: r.symbol, memo: r.memo, mandate: r.memo.mandate || null })), holdings: holdings.packets, portfolio, policy, marks: {}, advBySymbol: {}, sectorOf: () => null, clusterOf: null }); } catch (e) { return { error: String(e.message).slice(0, 120) }; } })() : null;
  const r = await deps.gateway.finalizePortfolio({ coverage: [], researchResults: researchResults.map((x) => ({ symbol: x.symbol, memo: x.memo })), holdings: holdings.packets, portfolio, feasibleAlternatives: feasible,
    policy: { policyHash: policy.policyHash, riskPolicyHash: policy.riskPolicyHash, riskMandate: policy.riskMandate }, contextManifestHash: null, background: false });
  if (!r.ok) return { ok: false, reason: r.error, decision: "ABSTAIN", reasonCode: "MODEL_FAILURE" };
  const claimsById = Object.fromEntries(researchResults.flatMap((x) => x.factualPremises.map((p) => [p.claimId, p])));
  const mandates = r.synthesis.expansionMandates.filter((m) => !held.has(m.symbol));
  if (!mandates.length) return { ok: true, staged: { status: "EMPTY" }, decisions: r.synthesis.decisions, costMinor: r.costMinor };
  const verified = await deps.claimVerifier.verifyAndPersistBatch({ proposals: mandates, claimsById });
  const legal = mandates.filter((m) => !(verified.blockingSymbols || []).includes(m.symbol));
  if (!legal.length) return { ok: true, staged: { status: "EMPTY", reason: "CLAIMS_UNSUPPORTED" }, decisions: r.synthesis.decisions, costMinor: r.costMinor };
  const activation = await deps.portfolio.captureActivationSnapshot({ accountId, reason: "EVENT_EXPANSION" });
  const staged = deps.mandate && typeof deps.mandate.stagePortfolioPlan === "function"
    ? await deps.mandate.stagePortfolioPlan({ planClass: "EXPANSION", portfolioPlanProposal: { planClass: "EXPANSION", decisions: r.synthesis.decisions, comparisonNote: r.synthesis.comparisonNote, event: payload.eventId || null }, proposals: legal, verifiedProposalClaims: verified, activationSnapshot: activation, accountId, cutoff: { cutoffMs }, policy, managerRunId: claim.runId || null })
    : { status: "NOT_COMMITTED", reason: "mandate_module_unavailable" };
  return { ok: true, staged: { status: staged.status, reason: staged.reason || null, planId: staged.planId || null }, decisions: r.synthesis.decisions, mandates: legal.map((m) => m.symbol), costMinor: r.costMinor };
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
    if (ctrl.engineMode !== "manager" && task === "premarket_manager") {
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
