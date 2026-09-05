/*  netlify/functions/_investorManager.js  (fund-manager-v1)
 *  ---------------------------------------------------------------------------
 *  Investor AI — the single investment authority's process (blueprint §6.2,
 *  §6.3, §6.7, §6.8, §17.4, Appendix B). There is no committee module: this
 *  file runs the one logical Manager Meeting as a resumable state machine.
 *
 *    freeze      universe / evidence / portfolio manifests at the cutoff
 *    review      Sol covers every frozen roster symbol exactly once
 *    coverage    exact structural validation; one bounded repair of MISSING
 *                rows only; the repaired object is the effective coverage
 *    maintenance Sol-authored HOLD/REDUCE/SELL change-set validated and
 *                staged by the 09:15 hard deadline, or prior acknowledged
 *                protection retained and the holding marked ACTION_REQUIRED
 *    research    bounded pool, smallest unique priority first, suffix-only
 *                deferral, barrier
 *    synthesis   one Sol comparison of the completed research set together;
 *                unique capital ranks; claims verified; a material clamp or
 *                a failed claim returns to ONE Sol continuation
 *    activation  fresh post-maintenance activation snapshot; the EXPANSION
 *                plan commits all-or-none or not at all
 *    persist     exactly one immutable decision row per managed symbol
 *
 *  What code decides here: structure, arithmetic, safety and lineage. What
 *  it never decides: which company, which size, which price. A run that
 *  activates no BUY says WHY in typed reasons (D-12): coverage incomplete,
 *  stale cards, research deferred, model failure, claims unsupported,
 *  budget exhausted, or nothing worth buying — visibly different states.
 * ---------------------------------------------------------------------------
 */

"use strict";

const crypto = require("crypto");
const A = require("./_investorAdmin");
const POLICY = require("./_investorPolicy");

const C = require("./_investorDecisionContext");
const MANAGER_VERSION = "manager.v2";
const RUN_SCHEMA = "manager-run.v1";
const DECISION_SCHEMA = "manager-decision.v1";
const STAGES = Object.freeze(["freeze", "review", "coverage", "maintenance", "research", "synthesis", "activation", "persist", "complete"]);
const CANONICAL = new Set(["BUY", "WATCH", "IGNORE", "HOLD", "REDUCE", "SELL", "ABSTAIN"]);
const REVIEW_DIRECTIVES = new Set(["RESEARCH_NOW", "UPDATE_EXISTING", "REUSE_CURRENT", "NONE"]);
const PROVISIONAL = new Set(["WATCH", "IGNORE", "ABSTAIN", "HOLD_CANDIDATE", "REDUCE_CANDIDATE", "SELL_CANDIDATE"]);
const NO_BUY_REASONS = Object.freeze(["COVERAGE_INCOMPLETE", "STALE_CARDS", "RESEARCH_DEFERRED", "MODEL_FAILURE", "CLAIMS_UNSUPPORTED",
  "BUDGET_EXHAUSTED", "ACTIVATION_REJECTED", "FREEZE_STATE", "NOTHING_ATTRACTIVE", "NO_RESEARCH_REQUESTED", "SCALING_IN_FORBIDDEN", "UNFUNDED"]);

function sha(v) { return crypto.createHash("sha256").update(typeof v === "string" ? v : JSON.stringify(POLICY.canonical(v))).digest("hex"); }
function lazy(m) { try { return require(m); } catch { return null; } }
function db(admin) { return admin || A; }
function typed(code, message, extra = {}) { return Object.assign(new Error(message || code), { code, ...extra }); }

/* ── freeze ────────────────────────────────────────────────────────────── */
/** PURE. The decision cutoff: evidence known by this instant is in; later
 *  evidence waits for an event revision or the next meeting. Inclusion uses
 *  publication time when reliable, else the immutable first-seen time. */
function freezeDecisionCutoff({ runStartedAtMs, tradingDate, inclusionRule = "published_and_observed_by_cutoff", nowMs = Date.now() } = {}) {
  const M = lazy("./_investorMarket");
  const freezeMs = M && tradingDate ? M.nyWallClockToUtcMs(tradingDate, POLICY.CUTOFFS_ET.evidenceFreezeMin) : null;
  const started = Number(runStartedAtMs) || nowMs;
  const cutoffMs = Number.isFinite(freezeMs) && freezeMs <= started ? freezeMs : started;
  return { cutoffMs, cutoff: new Date(cutoffMs).toISOString(), inclusionRule, tradingDate: tradingDate || null,
    freezeMinuteEt: POLICY.CUTOFFS_ET.evidenceFreezeMin, source: Number.isFinite(freezeMs) && freezeMs <= started ? "scheduled_freeze" : "run_start" };
}

/* ── coverage contract (§6.3, D-12) ───────────────────────────────────── */
/** PURE. Cards must cover the roster exactly before Sol is asked anything. */
function assertExactCoverageInput(cards, roster) {
  const U = lazy("./_investorUniverse");
  const rows = (cards && cards.cards) || cards || [];
  const match = U ? U.coverageMatches(roster, rows) : null;
  const missingCards = (cards && cards.missing) || [];
  if (!match || !match.ok || missingCards.length) {
    throw typed("COVERAGE_INPUT_INCOMPLETE", `cards do not cover the frozen roster: ${match ? `${match.missing.length} missing, ${match.duplicates.length} duplicate, ${match.unknown.length} unknown` : "no matcher"}`,
      { missing: [...(match ? match.missing : []), ...missingCards.map((m) => m.symbol)], duplicates: match ? match.duplicates : [], unknown: match ? match.unknown : [] });
  }
  return { ok: true, count: rows.length, universeHash: roster.universeHash };
}
/** PURE. Sol's coverage against the frozen snapshot: exact symbol, hash and
 *  count equality, valid row vocabulary. Never a hard-coded 304. */
function validateCoverage(coverage, roster) {
  const U = lazy("./_investorUniverse");
  const rows = Array.isArray(coverage) ? coverage : [];
  const structural = U ? U.coverageMatches(roster, rows) : { ok: false, missing: roster.symbols || [], duplicates: [], unknown: [], completedCount: 0, eligibleCount: roster.eligibleCount };
  const invalid = rows.filter((r) => !r || !REVIEW_DIRECTIVES.has(r.reviewDirective) || !PROVISIONAL.has(r.provisionalDisposition) || CANONICAL.has(r.provisionalDisposition) && !PROVISIONAL.has(r.provisionalDisposition) || typeof r.changedSincePrior !== "boolean" || !r.reason).map((r) => r && r.symbol);
  const ok = structural.ok && invalid.length === 0 && structural.completedCount === roster.eligibleCount;
  return { ok, missing: structural.missing, duplicates: structural.duplicates, unknown: structural.unknown, invalidRows: invalid,
    completedCount: structural.completedCount, eligibleCount: roster.eligibleCount, universeHash: roster.universeHash, universeVersion: roster.universeVersion };
}
/** PURE. Structural repair may ADD missing rows only. It never changes a
 *  returned disposition and never adds a symbol that was not missing. */
function mergeStructuralCoverageRepair({ original, repairedRows = [], expectedMissing = [] }) {
  const wanted = new Set(expectedMissing.map((s) => String(s).toUpperCase()));
  const have = new Set((original.coverage || []).map((r) => r.symbol));
  const added = [], ignored = [];
  for (const row of repairedRows) {
    if (!row || !wanted.has(row.symbol) || have.has(row.symbol)) { ignored.push(row && row.symbol); continue; }
    added.push({ ...row, repaired: true }); have.add(row.symbol);
  }
  return { ...original, coverage: [...(original.coverage || []), ...added], repair: { added: added.map((r) => r.symbol), ignored: ignored.filter(Boolean), expected: [...wanted] }, effective: true };
}

/* ── holding maintenance (§6.7, §6.8) ─────────────────────────────────── */
/** PURE. Anti-goalpost: from a delta review a mandate may tighten protection
 *  or shorten duration; it may not widen the loss boundary, extend a losing
 *  thesis, add to a loser, or raise maximum dollar risk. */
function antiGoalpostViolations(mandate, prior) {
  if (!mandate || !prior) return [];
  const v = [];
  const p = mandate.action && mandate.action.protection;
  const q = (prior.action && prior.action.protection) || (prior.lossBoundaryPriceMicros != null ? { lossBoundaryPriceMicros: prior.lossBoundaryPriceMicros } : null);
  if (p && q && p.lossBoundaryPriceMicros != null && q.lossBoundaryPriceMicros != null && BigInt(p.lossBoundaryPriceMicros) < BigInt(q.lossBoundaryPriceMicros)) v.push("LOSS_BOUNDARY_WIDENED");
  const a = mandate.allocation, b = prior.allocation;
  if (a && b && a.maxPlannedLossMinor != null && b.maxPlannedLossMinor != null && BigInt(a.maxPlannedLossMinor) > BigInt(b.maxPlannedLossMinor)) v.push("MAX_PLANNED_LOSS_RAISED");
  if (a && b && a.maxCapitalMinor != null && b.maxCapitalMinor != null && BigInt(a.maxCapitalMinor) > BigInt(b.maxCapitalMinor)) v.push("MAX_CAPITAL_RAISED");
  if (mandate.decision === "BUY" || (mandate.action && mandate.action.kind === "BUY")) v.push("ADD_TO_POSITION_FROM_DELTA_REVIEW");
  const pt = p && p.timeExit && p.timeExit.triggerAfterSessionDate, qt = q && q.timeExit && q.timeExit.triggerAfterSessionDate;
  if (pt && qt && pt > qt) v.push("DURATION_EXTENDED");
  return v;
}
/** PURE. The RISK_MAINTENANCE change-set from Sol's holding analyses. */
function buildRiskMaintenancePlan({ holdingAnalysis = [], holdings = [], priorMandateBySymbol = {}, hardDeadlineMs = null, nowMs = Date.now(), tradingDate = null } = {}) {
  const heldSymbols = holdings.map((h) => String(h.symbol).toUpperCase());
  const byHeld = new Map();
  const duplicates = [];
  for (const a of holdingAnalysis) { if (!a || !a.symbol) continue; if (byHeld.has(a.symbol)) duplicates.push(a.symbol); else byHeld.set(a.symbol, a); }
  const actionableMandates = [], abstained = [], actionRequired = [], unchanged = [], violations = [], emergency = [];
  const ranks = new Set();
  for (const symbol of heldSymbols) {
    const a = byHeld.get(symbol);
    if (!a) { actionRequired.push({ symbol, reason: "NO_HOLDING_ANALYSIS" }); continue; }
    if (a.emergency && Number.isInteger(a.emergency.emergencyReductionRank)) {
      if (ranks.has(a.emergency.emergencyReductionRank)) violations.push({ symbol, code: "EMERGENCY_RANK_DUPLICATE" });
      ranks.add(a.emergency.emergencyReductionRank);
      emergency.push({ symbol, rank: a.emergency.emergencyReductionRank, asOf: a.emergency.emergencyRankAsOf, expiresAfterSession: a.emergency.emergencyRankExpiresAfterSession });
    } else violations.push({ symbol, code: "EMERGENCY_RANK_MISSING" });
    if (a.decision === "ABSTAIN") { abstained.push({ symbol, reasonCode: a.reasonCode || "UNCERTAINTY" }); actionRequired.push({ symbol, reason: `ABSTAIN:${a.reasonCode || "UNCERTAINTY"}` }); continue; }
    if (!["HOLD", "REDUCE", "SELL"].includes(a.decision)) { violations.push({ symbol, code: "DECISION_INVALID" }); actionRequired.push({ symbol, reason: "DECISION_INVALID" }); continue; }
    if (a.mandate) {
      if (a.mandate.symbol !== symbol) { violations.push({ symbol, code: "MANDATE_SYMBOL_MISMATCH" }); actionRequired.push({ symbol, reason: "MANDATE_SYMBOL_MISMATCH" }); continue; }
      const bad = antiGoalpostViolations(a.mandate, priorMandateBySymbol[symbol]);
      if (bad.length) { violations.push({ symbol, code: "ANTI_GOALPOST", details: bad }); actionRequired.push({ symbol, reason: `ANTI_GOALPOST:${bad.join(",")}` }); continue; }
      if (a.decision === "SELL" && a.mandate.action && a.mandate.action.kind !== "SELL") { violations.push({ symbol, code: "SELL_WITHOUT_SELL_ACTION" }); actionRequired.push({ symbol, reason: "SELL_WITHOUT_SELL_ACTION" }); continue; }
      actionableMandates.push({ ...a.mandate, decision: a.decision });
    } else if (a.decision === "HOLD" && ["UNCHANGED", "PROTECTION_ONLY"].includes(a.revisionResult)) unchanged.push(symbol);
    else if (a.decision === "HOLD") unchanged.push(symbol);
    else { violations.push({ symbol, code: `${a.decision}_WITHOUT_MANDATE` }); actionRequired.push({ symbol, reason: `${a.decision}_WITHOUT_MANDATE` }); }
  }
  for (const d of duplicates) violations.push({ symbol: d, code: "DUPLICATE_HOLDING_ANALYSIS" });
  return { planClass: "RISK_MAINTENANCE", tradingDate, symbols: heldSymbols, actionableMandates, unchanged, abstained, actionRequired, violations, emergency,
    deadlineMissed: hardDeadlineMs != null && nowMs > Number(hardDeadlineMs), planHash: sha({ actionableMandates, unchanged, abstained }) };
}

/* ── research ──────────────────────────────────────────────────────────── */
function validateUniqueResearchPriority(requests = []) {
  const R = lazy("./_investorResearch");
  return R ? R.orderRequests(requests) : requests;
}
/** PURE. A requested name whose research is incomplete cannot become BUY. */
function forceNonExecutableForIncompleteResearch({ researchRequests = [], completed = [], deferred = [], failed = [] } = {}) {
  const done = new Set(completed.map((r) => r.request ? r.request.symbol : r.symbol));
  const out = {};
  for (const r of [...deferred, ...failed]) { const s = r.request ? r.request.symbol : r.symbol; if (!done.has(s)) out[s] = { decision: "WATCH", reasonCode: "RESEARCH_DEFERRED", researchState: deferred.some((d) => (d.request ? d.request.symbol : d.symbol) === s) ? "DEFERRED" : "FAILED" }; }
  for (const r of researchRequests) if (!done.has(r.symbol) && !out[r.symbol]) out[r.symbol] = { decision: "WATCH", reasonCode: "RESEARCH_DEFERRED", researchState: "NOT_RUN" };
  return out;
}

/* ── decisions ─────────────────────────────────────────────────────────── */
const PROVISIONAL_TO_CANONICAL = Object.freeze({ WATCH: "WATCH", IGNORE: "IGNORE", ABSTAIN: "ABSTAIN", HOLD_CANDIDATE: "WATCH", REDUCE_CANDIDATE: "WATCH", SELL_CANDIDATE: "WATCH" });
/** PURE. Exactly one canonical decision row per managed symbol. Workflow
 *  values never reach the decision column; held names take the holding
 *  analysis; researched names take the final synthesis; a held name is
 *  never BUY. */
function composeFinalDecisionRows({ roster, workset, effective, maintenance = null, research = null, synthesis = null, overrides = {}, claimGate = {} } = {}) {
  const rowsBySymbol = new Map((effective && effective.coverage || []).map((r) => [r.symbol, r]));
  const holdingBy = new Map([...(effective && effective.holdingAnalysis || []), ...(synthesis && synthesis.holdingAnalysis || [])].map((a) => [a.symbol, a]));
  const finalBy = new Map((synthesis && synthesis.decisions || []).map((d) => [d.symbol, d]));
  const mandateBy = new Map((synthesis && synthesis.expansionMandates || []).map((m) => [m.symbol, m]));
  const memoBy = new Map(((research && research.completed) || []).map((r) => [r.request ? r.request.symbol : r.symbol, r.result || r]));
  const maintenanceAR = new Set(((maintenance && maintenance.actionRequired) || []).map((x) => x.symbol));
  const symbols = workset && workset.symbols ? workset.symbols : (roster.symbols || []);
  const out = [];
  for (const symbol of symbols) {
    const w = workset && workset.rows ? workset.rows.find((r) => r.symbol === symbol) : { symbol, eligible: true, held: false, heldQty: 0, entryEligible: true };
    const cov = rowsBySymbol.get(symbol) || null;
    const hold = holdingBy.get(symbol) || null;
    const fin = finalBy.get(symbol) || null;
    const ov = overrides[symbol] || null;
    const gate = claimGate[symbol] || null;
    let decision, reasonCode = null, reason = cov ? cov.reason : "", capitalRank = null, fundingState = "NOT_APPLICABLE", source;
    if (w.held) {
      if (hold && ["HOLD", "REDUCE", "SELL", "ABSTAIN"].includes(hold.decision) && !maintenanceAR.has(symbol)) { decision = hold.decision; reasonCode = hold.reasonCode || null; reason = hold.rationale || reason; source = "holding_analysis"; }
      else if (hold && hold.decision === "ABSTAIN") { decision = "ABSTAIN"; reasonCode = hold.reasonCode || "UNCERTAINTY"; reason = hold.rationale || reason; source = "holding_analysis"; }
      else { decision = "ABSTAIN"; reasonCode = "DATA_INCOMPLETE"; reason = maintenanceAR.has(symbol) ? "holding maintenance not accepted; prior protection retained (ACTION REQUIRED)" : "no holding analysis returned; prior protection retained (ACTION REQUIRED)"; source = "maintenance_gate"; }
    } else if (ov) { decision = ov.decision; reasonCode = ov.reasonCode; reason = `research ${String(ov.researchState || "").toLowerCase()}`; source = "research_gate"; }
    else if (gate && gate.blocking) { decision = gate.forceAbstain ? "ABSTAIN" : "WATCH"; reasonCode = gate.forceAbstain ? "EVIDENCE_CONFLICT" : "DATA_INCOMPLETE"; reason = gate.forceAbstain ? "a material premise was contradicted by its source" : "a material premise could not be verified"; source = "claim_gate"; }
    else if (fin) {
      decision = CANONICAL.has(fin.decision) ? fin.decision : "ABSTAIN";
      if (decision === "BUY" && (!w.entryEligible || !mandateBy.has(symbol))) { decision = "WATCH"; reasonCode = !w.entryEligible ? "NONE" : "UNCERTAINTY"; reason = !w.entryEligible ? "not entry-eligible (held, pending or off-roster)" : "BUY without a complete mandate"; }
      else { reasonCode = fin.reasonCode || null; reason = fin.reason || reason; capitalRank = fin.capitalRank == null ? null : fin.capitalRank; fundingState = fin.fundingState || (decision === "BUY" ? "FUNDED" : "NOT_APPLICABLE"); }
      source = "final_synthesis";
    } else if (memoBy.has(symbol)) {
      const m = memoBy.get(symbol);
      const pd = m.proposedDecision || (m.memo && m.memo.proposedDecision) || "WATCH";
      decision = pd === "BUY" ? "WATCH" : (CANONICAL.has(pd) ? pd : "WATCH");
      reasonCode = pd === "BUY" ? "RESEARCH_DEFERRED" : (m.reasonCode || null); reason = pd === "BUY" ? "researched BUY not compared in final synthesis" : reason; source = "research_memo";
    } else if (cov) { decision = PROVISIONAL_TO_CANONICAL[cov.provisionalDisposition] || "ABSTAIN"; reasonCode = cov.reasonCode || null; source = "coverage"; }
    else { decision = "ABSTAIN"; reasonCode = "DATA_INCOMPLETE"; reason = "no coverage row"; source = "missing"; }
    if (decision === "BUY" && w.held) { decision = "WATCH"; reasonCode = "NONE"; reason = "scaling in is forbidden"; }
    if (!CANONICAL.has(decision)) { decision = "ABSTAIN"; reasonCode = "MODEL_FAILURE"; }
    out.push({ symbol, decision, reasonCode, reason: String(reason || "").slice(0, 300), capitalRank, fundingState,
      reviewDirective: cov ? cov.reviewDirective : null, provisionalDisposition: cov ? cov.provisionalDisposition : null,
      changedSincePrior: cov ? cov.changedSincePrior === true : false, held: !!w.held, eligible: w.eligible !== false, offRoster: !!w.offRoster,
      mandate: decision === "BUY" ? mandateBy.get(symbol) || null : (w.held && hold && hold.mandate ? hold.mandate : null),
      researchMemoId: memoBy.has(symbol) ? (memoBy.get(symbol).memoId || null) : null, source });
  }
  return out;
}
/** PURE. Unique capital ranks and the run's BUY ceiling; throws. */
function validateUniqueCapitalRanksAndFeasibility(decisions, portfolio = null, policy = null) {
  const buys = decisions.filter((d) => d.decision === "BUY");
  const ranks = buys.map((d) => d.capitalRank);
  if (ranks.some((r) => !Number.isInteger(r) || r < 1)) throw typed("CAPITAL_RANK_MISSING", "a BUY lacks a capital rank");
  if (new Set(ranks).size !== ranks.length) throw typed("CAPITAL_RANK_DUPLICATE", "capital ranks are not unique");
  const max = Number((policy && policy.riskMandate && policy.riskMandate.activation && policy.riskMandate.activation.maxActionableBuysPerRun) || POLICY.RISK_MANDATE.activation.maxActionableBuysPerRun);
  if (buys.length > max) throw typed("TOO_MANY_ACTIONABLE_BUYS", `${buys.length} BUYs exceed the run ceiling ${max}`);
  if (buys.some((d) => !d.mandate)) throw typed("BUY_WITHOUT_MANDATE", "a BUY row has no mandate");
  return { buys: buys.length, ranks: ranks.sort((a, b) => a - b) };
}
/** PURE. Why did this run activate no BUY? Typed and visible (D-12). */
function noBuyReasons({ coverage, freshness = null, research = null, synthesis = null, activation = null, control = null, budget = null, decisions = [], prior = [] } = {}) {
  const reasons = [...(prior || [])];
  const has = (code) => reasons.some((r) => r && r.code === code);
  if (coverage && !coverage.ok && !has("COVERAGE_INCOMPLETE")) reasons.push({ code: "COVERAGE_INCOMPLETE", missing: coverage.missing, duplicates: coverage.duplicates, unknown: coverage.unknown, completedCount: coverage.completedCount, eligibleCount: coverage.eligibleCount });
  if (freshness && freshness.stale && freshness.stale.length) reasons.push({ code: "STALE_CARDS", stale: freshness.stale.slice(0, 40), count: freshness.stale.length });
  if (research && research.deferred && research.deferred.length) reasons.push({ code: "RESEARCH_DEFERRED", symbols: research.deferred.map((r) => r.request ? r.request.symbol : r.symbol) });
  if (research && research.failed && research.failed.length) reasons.push({ code: "MODEL_FAILURE", symbols: research.failed.map((r) => r.request ? r.request.symbol : r.symbol) });
  if (synthesis && synthesis.ok === false) reasons.push({ code: synthesis.budgetBlocked ? "BUDGET_EXHAUSTED" : "MODEL_FAILURE", error: synthesis.error || null });
  if (synthesis && synthesis.claimBlocked && synthesis.claimBlocked.length) reasons.push({ code: "CLAIMS_UNSUPPORTED", symbols: synthesis.claimBlocked });
  if (activation && activation.status && activation.status !== "COMMITTED" && activation.status !== "EMPTY") reasons.push({ code: "ACTIVATION_REJECTED", status: activation.status, reason: activation.reason || null });
  if (control && (control.freezeNewBuys || control.managerState === "PAUSED" || control.killSwitch) && !has("FREEZE_STATE")) reasons.push({ code: "FREEZE_STATE", reason: "operator freeze, pause or kill state" });
  if (budget && budget.blocked) reasons.push({ code: "BUDGET_EXHAUSTED" });
  const unfunded = decisions.filter((d) => d.fundingState === "UNFUNDED").map((d) => d.symbol);
  if (unfunded.length) reasons.push({ code: "UNFUNDED", symbols: unfunded });
  if (!reasons.length) reasons.push({ code: research && research.completed && research.completed.length ? "NOTHING_ATTRACTIVE" : "NO_RESEARCH_REQUESTED" });
  return reasons;
}

/* ── persistence ───────────────────────────────────────────────────────── */
async function persistDecisionRows({ managerRunId, tradingDate, accountId, decisions = [], contextManifestHash = null, maintenancePortfolioPlanId = null, expansionPortfolioPlanId = null, admin = null, cutoffMs = null } = {}) {
  const D = db(admin);
  const SC = lazy("./_investorStorageCodec");
  let written = 0;
  const priorBy = {};
  for (const d of decisions) {
    const id = `${managerRunId}_${d.symbol}`;
    const row = { schemaVersion: DECISION_SCHEMA, managerRunId, symbol: d.symbol, decision: d.decision, reasonCode: d.reasonCode, reason: d.reason,
      capitalRank: d.capitalRank, fundingState: d.fundingState, reviewDirective: d.reviewDirective, provisionalDisposition: d.provisionalDisposition,
      changedSincePrior: d.changedSincePrior, held: d.held, eligible: d.eligible, offRoster: d.offRoster, source: d.source,
      mandateProposalHash: d.mandate ? sha(d.mandate) : null, researchMemoId: d.researchMemoId || null,
      tradingDate, accountId, asOfMs: cutoffMs || Date.now(), contextManifestHash, maintenancePortfolioPlanId, expansionPortfolioPlanId, createdAtMs: Date.now(),
      ...D.envelope({ created_by: "manager.persistDecisionRows" }) };
    let doc = row;
    if (SC) { try { doc = SC.encode(row); } catch (e) { throw typed("DECISION_CODEC_REJECTED", e.message); } }
    await D.col(D.COL.managerDecisions).doc(id).set(doc);
    written += 1;
    priorBy[d.symbol] = { decision: d.decision, reasonCode: d.reasonCode, managerRunId, asOfMs: row.asOfMs };
  }
  /* the dossier pointers learn the manager review by pointer only */
  for (const d of decisions) {
    try { await D.col(D.COL.dossiers).doc(d.symbol).set({ managerDecisionId: `${managerRunId}_${d.symbol}`, lastManagerReviewAtMs: Date.now(),
      standingView: { status: d.decision, decision: d.decision, reasonCode: d.reasonCode, managerRunId, asOfMs: Date.now(), researchVersion: null } }, { merge: true }); } catch {}
  }
  return { written, ids: decisions.map((d) => `${managerRunId}_${d.symbol}`) };
}
async function writeRun({ managerRunId, admin = null, ...fields }) {
  const D = db(admin);
  const SC = lazy("./_investorStorageCodec");
  const ref = D.col(D.COL.managerRuns).doc(managerRunId);
  const cur = await ref.get();
  const prior = cur.exists ? (cur.data()._codec && SC ? SC.decode(cur.data()) : cur.data()) : {};
  const row = { ...prior, schemaVersion: RUN_SCHEMA, managerRunId, updatedAtMs: Date.now(), ...fields, ...D.envelope({ created_by: "manager.writeRun" }) };
  let doc = row;
  if (SC) { try { doc = SC.encode(row); } catch (e) { throw typed("RUN_CODEC_REJECTED", e.message); } }
  await ref.set(doc);
  return row;
}
async function readRun(managerRunId, { admin = null } = {}) {
  const D = db(admin);
  const SC = lazy("./_investorStorageCodec");
  const s = await D.col(D.COL.managerRuns).doc(managerRunId).get();
  if (!s.exists) return null;
  const d = s.data();
  return d._codec && SC ? SC.decode(d) : d;
}

/* ── the state machine ─────────────────────────────────────────────────── */
function defaultDeps(admin = null) {
  return {
    admin, gateway: lazy("./_investorOpenai"), dossier: lazy("./_investorDossier"), universe: lazy("./_investorUniverse"),
    portfolio: lazy("./_investorPortfolio"), research: lazy("./_investorResearch"), claimVerifier: lazy("./_investorClaimVerifier"),
    mandate: lazy("./_investorMandate"), portfolioRisk: lazy("./_investorPortfolioRisk"), tools: lazy("./_investorResearchTools"),
    workset: lazy("./_investorWorkset"), contentStore: lazy("./_investorContentStore"), market: lazy("./_investorMarket"), now: Date.now,
  };
}
/* ── liquidity marks for the size authority (§15.1): ADV measured from the
   stored daily series over the last 20 sessions at or before the cutoff;
   the spread is the policy's labelled paper assumption until a quote feed
   exists. A symbol without 10 priced sessions gets advMinor null, which
   the risk service refuses as ADV_UNKNOWN — never a silent default. ── */
async function liquidityMarks(symbols, deps, { policy = null, cutoffMs = Date.now() } = {}) {
  const H = deps.history || lazy("./_investorHistory");
  const liq = (policy && policy.riskMandate && policy.riskMandate.liquidity) || POLICY.RISK_MANDATE.liquidity;
  const spreadBps = String(liq.paperAssumedSpreadBps || "10");
  const marks = {};
  for (const symbol of [...new Set((symbols || []).filter(Boolean))]) {
    let series = [];
    try { const r = H && typeof H.readDailyWithMetaFor === "function" ? await H.readDailyWithMetaFor(deps.admin || null, symbol) : await H.readDailyWithMeta(symbol); series = (r && r.series) || []; } catch { series = []; }
    const bars = C.completedDaily(series, cutoffMs).slice(-20);
    let acc = 0n, n = 0;
    for (const b of bars) { const dv = Number(b.c) * (Number(b.v) || 0); if (Number.isFinite(dv) && dv > 0) { acc += BigInt(Math.round(dv * 100)); n += 1; } }
    marks[symbol] = { advMinor: n >= 10 ? (acc / BigInt(n)).toString() : null, advSessions: n, advSource: "measured_daily_series_20", spreadBps, spreadSource: liq.spreadSource || "paper_assumption", asOfDate: bars.length ? bars[bars.length - 1].date : null };
  }
  return marks;
}

/** Run the meeting from its checkpoint. Returns { done, yielded, checkpoint,
 *  summary }. `budget()` says how many ms of invocation time remain. */
async function runManagerMeeting({ claim, deps: partial = {}, budget = () => 10 * 60 * 1000, minStageMs = 90 * 1000, control = null } = {}) {
  const deps = { ...defaultDeps(partial.admin || null), ...partial, decisionMarketCache: new Map() };
  const now = deps.now;
  const payload = claim.payload || {};
  const accountId = String(payload.accountId || "paper-1");
  const tradingDate = payload.tradingDate || (deps.market ? deps.market.sessionState(new Date(now())).date : new Date(now()).toISOString().slice(0, 10));
  const managerRunId = claim.runId || `run_premarket_manager_${accountId}_${tradingDate}`;
  const cp = claim.checkpoint && claim.checkpoint.stage && claim.checkpoint.data ? claim.checkpoint : null;
  const st = cp ? cp.data : { managerRunId, accountId, tradingDate, startedAtMs: now(), stage: "freeze", costMinor: "0", requestIds: [] };
  let stage = cp ? cp.stage : "freeze";
  const ctrl = control || {};
  const policy = st.policySnapshot || POLICY.loadActiveSync(ctrl);
  const yieldNow = (reason) => ({ done: false, yielded: true, reason, checkpoint: { stage, data: st } });
  const addCost = (c) => { st.costMinor = (BigInt(st.costMinor || "0") + BigInt(c || "0")).toString(); };
  const record = async (fields) => { try { await writeRun({ managerRunId, admin: deps.admin, tradingDate, accountId, status: fields.status || "running", stage, costMinor: st.costMinor, ...fields }); } catch (e) { console.error("manager run record", e.message); } };

  while (stage !== "complete") {
    if (budget() < minStageMs) return yieldNow("segment_budget");
    if (stage === "freeze") {
      const snapshotRoster = deps.universe.freezeEligibleSnapshot({ tradingDate, nowMs: now(), removed: ctrl.universeRemovals || [] });
      const portfolio = await deps.portfolio.snapshot({ accountId, asOfMs: now(), admin: deps.admin });
      const workset = deps.workset.buildManaged({ roster: snapshotRoster, positions: portfolio.positions.map((p) => ({ symbol: p.symbol, qty: Number(p.quantityUnits), open: true })), pending: portfolio.workingOrders.map((o) => ({ symbol: o.symbol, orderId: o.orderId, status: o.status, side: o.side })), nowMs: now() });
      const cutoff = freezeDecisionCutoff({ runStartedAtMs: st.startedAtMs, tradingDate, nowMs: now() });
      const portfolioBySymbol = Object.fromEntries(workset.rows.map((r) => [r.symbol, { held: r.held, pending: r.pending, activeMandate: portfolio.activeMandates.some((m) => m.symbol === r.symbol), position: portfolio.positions.find(p => p.symbol === r.symbol) || null, mandate: portfolio.activeMandates.find(m => m.symbol === r.symbol) || null }]));
      const cards = await deps.dossier.compactCards({ symbols: snapshotRoster.symbols, cutoff, admin: deps.admin, portfolioBySymbol, deps });
      const freshness = await deps.dossier.dossierHealth({ symbols: snapshotRoster.symbols, admin: deps.admin, nowMs: cutoff.cutoffMs });
      let coverageInput;
      try { coverageInput = assertExactCoverageInput(cards, snapshotRoster); }
      catch (e) {
        st.noBuyReasons = [{ code: "COVERAGE_INCOMPLETE", missing: e.missing || [], duplicates: e.duplicates || [], unknown: e.unknown || [], phase: "cards" }];
        st.expansionBlocked = true;
        coverageInput = { ok:false, missing:e.missing || [], duplicates:e.duplicates || [], unknown:e.unknown || [] };
        const by = new Map(cards.cards.filter(c=>snapshotRoster.symbols.includes(c.symbol)).map(c=>[c.symbol,c]));
        cards.cards = snapshotRoster.symbols.map(s=>by.get(s) || C.unavailableCard(s,cutoff.cutoffMs,"coverage_missing"));
        cards.count = cards.cards.length;
      }
      const holdingPackets = await deps.dossier.expandedHoldingDeltas({ symbols: workset.managedPositionSymbols, cutoff, admin: deps.admin, portfolioBySymbol, deps });
      const learning = deps.learning || lazy("./_investorLearning");
      st.independentSymbols = learning ? learning.antiAnchoringSample({ symbols:snapshotRoster.symbols, tradingDate, heldSymbols:workset.managedPositionSymbols }) : [];
      cards.cards = cards.cards.map(c => st.independentSymbols.includes(c.symbol) ? {...c,standingView:null,independentReview:true} : c);
      const marketState = await C.marketState({ cards:cards.cards, cutoffMs:cutoff.cutoffMs, deps });
      const frozen = await C.freeze({ runId:managerRunId, context:{ cards, holdingPackets, portfolio:{...portfolio, observedAtMs:now()}, marketState, policy, cutoff, roster:snapshotRoster }, admin:deps.admin });
      const contextManifestHash = frozen.contentHash;
      st.policySnapshot = frozen.policy;
      Object.assign(st, { roster: { universeVersion: snapshotRoster.universeVersion, universeHash: snapshotRoster.universeHash, eligibleCount: snapshotRoster.eligibleCount, symbols: snapshotRoster.symbols, managedOffRoster: snapshotRoster.managedOffRoster },
        workset: { symbols: workset.symbols, rows: workset.rows.map((r) => ({ symbol: r.symbol, eligible: r.eligible, held: r.held, heldQty: r.heldQty, pending: r.pending, entryEligible: r.entryEligible, offRoster: r.offRoster })), managedPositionSymbols: workset.managedPositionSymbols, counts: workset.counts },
        cutoff, contextManifestHash, policyHash: policy.policyHash, cardsCount: cards.count, coverageInput, freshness: { stale: freshness.stale, missing: freshness.missing, complete: freshness.complete, present: freshness.present },
        portfolioHash: portfolio.contentHash, holdingPacketCount: holdingPackets.packets.length, priorMandates: Object.fromEntries(portfolio.activeMandates.map((m) => [m.symbol, m])) });
      await record({ status: "running", universeVersion: st.roster.universeVersion, universeHash: st.roster.universeHash, eligibleCount: st.roster.eligibleCount, contextManifestHash, cutoffMs: cutoff.cutoffMs, policyHash: policy.policyHash, cardsCount: cards.count, heldCount: workset.managedPositionSymbols.length, marketContext: { cutoffMs:frozen.marketState.cutoffMs, breadth:frozen.marketState.breadth, observations:(frozen.marketState.observations || []).map(({returns, adjustmentAnchor, ...observation})=>observation), macroEvidence:(frozen.marketState.macroEvidence || []).slice(0,8).map(x=>({title:x.title || null,link:x.link || null,summary:String(x.summary || "").slice(0,600),publishedAt:x.publishedAt || null,knownAtMs:x.knownAtMs || null})) } });
      stage = "review";
      continue;
    }
    if (stage === "review") {
      const { cards, holdingPackets, portfolio, marketState } = await rebuildContext(st, deps, accountId);
      const r = await deps.gateway.reviewUniverse({ cards: cards.cards, universeManifest: st.roster, holdings: holdingPackets.packets, portfolio: portfolio, marketState, policy: { policyHash: policy.policyHash, riskPolicyHash: policy.riskPolicyHash, riskMandate: policy.riskMandate }, contextManifestHash: st.contextManifestHash, waitMs: Math.max(30000, Math.min(budget() - minStageMs, 8 * 60 * 1000)) });
      if (r.pending) { st.pendingRequest = r.requestId || null; return { done: false, yielded: true, reason: "sol_background_pending", checkpoint: { stage, data: st }, resumeAtMs: now() + 60000 }; }
      if (!r.ok) {
        st.review = { ok: false, error: r.error, budgetBlocked: r.budgetBlocked === true };
        st.noBuyReasons = [{ code: r.budgetBlocked ? "BUDGET_EXHAUSTED" : "MODEL_FAILURE", error: r.error }];
        await record({ status: "failed_closed", noBuyReasons: st.noBuyReasons, failure: { code: "REVIEW_FAILED", message: r.error } });
        return { done: true, failed: true, reason: "REVIEW_FAILED", checkpoint: { stage, data: st }, summary: { managerRunId, status: "failed_closed", noBuyReasons: st.noBuyReasons } };
      }
      addCost(r.costMinor);
      st.requestIds = [...(st.requestIds || []), ...(r.requestIds || [])];
      st.review = { ok: true, coverage: r.coverage, holdingAnalysis: r.holdingAnalysis, researchRequests: r.researchRequests, managerNote: r.managerNote, plan: r.plan, responseIds: r.responseIds };
      stage = "coverage";
      continue;
    }
    if (stage === "coverage") {
      let effective = { coverage: st.review.coverage, holdingAnalysis: st.review.holdingAnalysis, researchRequests: st.review.researchRequests };
      let v = validateCoverage(effective.coverage, st.roster);
      if (!v.ok && v.missing.length && !v.duplicates.length && !v.unknown.length && !v.invalidRows.length && !st.repairAttempted) {
        st.repairAttempted = true;
        const { cards } = await rebuildContext(st, deps, accountId);
        const rep = await deps.gateway.repairCoverageStructure({ responseId: (st.review.responseIds || [])[0] || null, missing: v.missing, cards: cards.cards, universeManifest: st.roster, contextManifestHash: st.contextManifestHash });
        if (rep.ok) { addCost(rep.costMinor); effective = mergeStructuralCoverageRepair({ original: effective, repairedRows: rep.coverage, expectedMissing: v.missing }); v = validateCoverage(effective.coverage, st.roster); }
        st.repair = { ok: rep.ok === true, repaired: rep.repaired || 0, error: rep.ok ? null : rep.error };
      }
      st.coverage = v;
      st.effective = effective;
      if (!v.ok) {
        st.noBuyReasons = [{ code: "COVERAGE_INCOMPLETE", missing: v.missing, duplicates: v.duplicates, unknown: v.unknown, invalidRows: v.invalidRows, completedCount: v.completedCount, eligibleCount: v.eligibleCount }];
        st.expansionBlocked = true;
      }
      await record({ status: "running", coverage: { ok: v.ok, completedCount: v.completedCount, eligibleCount: v.eligibleCount, missing: v.missing, duplicates: v.duplicates, unknown: v.unknown, repaired: st.repair ? st.repair.repaired : 0 } });
      stage = "maintenance";
      continue;
    }
    if (stage === "maintenance") {
      const held = st.workset.managedPositionSymbols.map((symbol) => ({ symbol }));
      const hardDeadlineMs = deps.market ? deps.market.nyWallClockToUtcMs(tradingDate, POLICY.CUTOFFS_ET.holdingHardDeadlineMin) : null;
      const plan = buildRiskMaintenancePlan({ holdingAnalysis: st.effective.holdingAnalysis || [], holdings: held, priorMandateBySymbol: st.priorMandates || {}, hardDeadlineMs, nowMs: now(), tradingDate });
      let staged = { status: "EMPTY", reason: "no_actionable_mandates" };
      let claims = null;
      if (plan.actionableMandates.length) {
        claims = deps.claimVerifier ? await deps.claimVerifier.verifyAndPersistBatch({ proposals: plan.actionableMandates, admin: deps.admin }) : { allSupported: false, blockingSymbols: plan.actionableMandates.map((m) => m.symbol), byProposal: {} };
        const blocked = new Set(claims.blockingSymbols || []);
        const accepted = plan.actionableMandates.filter((m) => !blocked.has(m.symbol));
        for (const s of blocked) plan.actionRequired.push({ symbol: s, reason: "CLAIMS_UNSUPPORTED" });
        if (accepted.length) {
          const activation = await deps.portfolio.captureActivationSnapshot({ accountId, reason: "RISK_MAINTENANCE", admin: deps.admin, reconcile: deps.reconcile || null });
          staged = deps.mandate && typeof deps.mandate.stagePortfolioPlan === "function"
            ? await deps.mandate.stagePortfolioPlan({ planClass: "RISK_MAINTENANCE", portfolioPlanProposal: { ...plan, actionableMandates: accepted }, proposals: accepted, verifiedProposalClaims: claims, activationSnapshot: activation, accountId, cutoff: st.cutoff, policy, managerRunId, admin: deps.admin,
              eligibleSymbols: st.roster.symbols, sectorOf:sectorLookup(deps), lineage:{model:POLICY.ROLE_MODELS.manager.model,reasoningEffort:"high",contextManifestHash:st.contextManifestHash,promptHash:deps.gateway.promptHash?deps.gateway.promptHash("finalizePortfolio"):null,bySymbol:Object.fromEntries((st.research && st.research.completed || []).map(c=>[c.symbol,{researchVersionId:c.memoId,dossierVersionId:c.dossierVersionId}]))}, verifiedValuations:Object.fromEntries((st.research && st.research.completed || []).map(c=>[c.symbol,c.verifiedValuation])), marks: await liquidityMarks(accepted.map((p) => p.symbol), deps, { policy, cutoffMs: st.cutoff.cutoffMs }), nowMs: now() })
            : { status: "NOT_COMMITTED", reason: "mandate_module_unavailable" };
        }
        if (staged.status !== "COMMITTED" && staged.status !== "EMPTY") {
          for (const m of accepted) plan.actionRequired.push({ symbol: m.symbol, reason: `NOT_COMMITTED:${staged.reason || staged.status}` });
          if (deps.mandate && typeof deps.mandate.retainPriorAcknowledgedProtectionAndAlert === "function") { try { await deps.mandate.retainPriorAcknowledgedProtectionAndAlert({ accountId, affectedSymbols: accepted.map((m) => m.symbol), reason: staged.reason || staged.status, admin: deps.admin }); } catch {} }
        }
      }
      st.maintenance = { planHash: plan.planHash, actionable: plan.actionableMandates.map((m) => m.symbol), unchanged: plan.unchanged, abstained: plan.abstained, actionRequired: plan.actionRequired, violations: plan.violations, emergency: plan.emergency,
        deadlineMissed: plan.deadlineMissed, staged: { status: staged.status, reason: staged.reason || null, planId: staged.planId || null }, claims: claims ? { allSupported: claims.allSupported, blocking: claims.blockingSymbols } : null };
      if (plan.actionRequired.length && ctrl.freezeExpansionOnUnsafeHolding !== false && plan.actionRequired.some((x) => /ANTI_GOALPOST|CLAIMS_UNSUPPORTED|NOT_COMMITTED/.test(x.reason))) { st.expansionBlocked = true; st.noBuyReasons = [...(st.noBuyReasons || []), { code: "FREEZE_STATE", reason: "unresolved unsafe holding freezes expansion", symbols: plan.actionRequired.map((x) => x.symbol) }]; }
      await record({ status: "running", maintenance: { actionable: st.maintenance.actionable.length, actionRequired: st.maintenance.actionRequired, staged: st.maintenance.staged.status, deadlineMissed: plan.deadlineMissed } });
      stage = "research";
      continue;
    }
    if (stage === "research") {
      let requests = [];
      try { requests = validateUniqueResearchPriority(st.effective.researchRequests || []); }
      catch (e) { st.researchInvalid = { code: e.code, message: e.message }; requests = []; }
      const R = deps.research;
      const pool = R.createPool({ now });
      const holdingDeadlineMs = deps.market ? deps.market.nyWallClockToUtcMs(tradingDate, 20 * 60) : null;
      const frozenContext = await rebuildContext(st, deps, accountId);
      const already = new Map((st.research && st.research.completed || []).map((r) => [r.symbol, r]));
      const worker = async (request) => {
        if (already.has(request.symbol)) return { ok: true, ...already.get(request.symbol), reused: true };
        const packetId = `${managerRunId}_research_${request.symbol}`;
        let packet;
        try { packet = (await C.read({runId:packetId,admin:deps.admin})).packet; }
        catch (e) {
          if (e.code !== "FROZEN_INPUTS_MISSING") throw e;
          packet = await R.buildPacket({ symbol: request.symbol, cutoff: st.cutoff, directive: request.reviewDirective, admin: deps.admin, deps:{...deps,frozenMarketState:frozenContext.marketState,independentReview:(st.independentSymbols || []).includes(request.symbol)} });
          packet = (await C.freeze({runId:packetId,context:{packet,marks:await liquidityMarks([request.symbol],deps,{policy,cutoffMs:st.cutoff.cutoffMs})},admin:deps.admin})).packet;
        }
        if (!packet.ok) return { ok: false, symbol: request.symbol, error: packet.reason };
        const portfolio = frozenContext.portfolio;
        const pinnedResearch=await C.read({runId:packetId,admin:deps.admin});
        const marks=pinnedResearch.marks || {};
        const bound = deps.tools ? deps.tools.allowlisted(deps.toolBindings || deps.tools.productionBindings({ accountId, admin:deps.admin, policy, portfolio, marks, decisionPacket:packet, sectorOf:sectorLookup(deps) }), policy.toolPolicy, { symbol: request.symbol, cutoffMs: st.cutoff.cutoffMs }) : { tools: null, log: [] };
        const r = await deps.gateway.researchCompany({ dossier: packet, delta: packet.delta, portfolio, tools: bound.tools, directive: request.reviewDirective, prior: packet.prior, cutoffMs: st.cutoff.cutoffMs });
        if (r.pending) return {ok:false,pending:true,symbol:request.symbol};
        if (r.ok) addCost(r.costMinor);
        const persisted = await R.persistImmutable({ ...r, dossierVersionId: packet.dossierVersionId, dossierHash: packet.dossierHash }, { admin: deps.admin, managerRunId, directive: request.reviewDirective, cutoffMs: st.cutoff.cutoffMs });
        if (persisted.persisted && deps.claimVerifier) { try { await deps.claimVerifier.verifyAndPersist({ premises: persisted.factualPremises, sourceManifest: persisted.sourceManifest, admin: deps.admin }); } catch {} }
        return { ok: persisted.persisted === true, symbol: request.symbol, memoId: persisted.memoId || null, dossierVersionId:packet.dossierVersionId, proposedDecision: persisted.proposedDecision || null, reasonCode: persisted.reasonCode || null,
          verifiedValuation:persisted.verifiedValuation || null, mandate: persisted.mandate || null, memo: persisted.memo || null, factualPremises: persisted.factualPremises || [], error: persisted.persisted ? null : persisted.reason, toolCalls: bound.log.length, costMinor: r.costMinor || "0" };
      };
      const out = await pool.run({ requests, concurrency: policy.maxConcurrentResearchJobs, worker, deadlineMs: holdingDeadlineMs,
        budgetRemaining: () => budget() > minStageMs, onResult: async (row) => { st.research = st.research || { completed: [], failed: [], deferred: [] }; if (row.ok) st.research.completed.push({ symbol: row.request.symbol, verifiedValuation:row.result.verifiedValuation || null, memoId: row.result.memoId, dossierVersionId:row.result.dossierVersionId, proposedDecision: row.result.proposedDecision, reasonCode: row.result.reasonCode, mandate: row.result.mandate, memo: row.result.memo, factualPremises: row.result.factualPremises, request: row.request }); } });
      st.research = { completed: out.completed.map((r) => ({ symbol: r.request.symbol, request: r.request, verifiedValuation:r.result.verifiedValuation || null, memoId: r.result.memoId, dossierVersionId:r.result.dossierVersionId, proposedDecision: r.result.proposedDecision, reasonCode: r.result.reasonCode, mandate: r.result.mandate, memo: r.result.memo, factualPremises: r.result.factualPremises })),
        failed: out.failed.map((r) => ({ symbol: r.request.symbol, request: r.request, error: r.error || (r.result && r.result.error) || null })), deferred: out.deferred.map((r) => ({ symbol: r.symbol, request: r, reason: r.deferredReason })),
        ranges: out.ranges, launchedOrder: out.launchedOrder, concurrency: out.concurrency, invalid: st.researchInvalid || null };
      if (out.failed.some(r=>r.result && r.result.pending) || out.deferred.some(r=>r.deferredReason === "budget")) return yieldNow("research_background_pending");
      st.overrides = forceNonExecutableForIncompleteResearch({ researchRequests: requests, completed: out.completed, deferred: out.deferred, failed: out.failed });
      await record({ status: "running", research: { requested: requests.length, completed: st.research.completed.length, failed: st.research.failed.length, deferred: st.research.deferred.length, ranges: out.ranges } });
      stage = "synthesis";
      continue;
    }
    if (stage === "synthesis") {
      const candidates = (st.research.completed || []).filter((r) => r.memo);
      if (!candidates.length && !st.workset.managedPositionSymbols.length) {
        st.synthesis = { skipped: true, reason: st.expansionBlocked ? "expansion_blocked" : "no_completed_research" };
        stage = "activation"; continue;
      }
      const { holdingPackets } = await rebuildContext(st, deps, accountId, { cardsNeeded: false });
      const synthesisInputId=`${managerRunId}_synthesis_${st.synthesisAttempt || 0}`;
      let input;
      try{input=await C.read({runId:synthesisInputId,admin:deps.admin});}
      catch(e){if(e.code!=="FROZEN_INPUTS_MISSING")throw e;input=await C.freeze({runId:synthesisInputId,admin:deps.admin,context:{
        portfolio:await deps.portfolio.snapshot({accountId,asOfMs:now(),admin:deps.admin}),
        marks:await liquidityMarks(candidates.map(c=>c.symbol),deps,{policy,cutoffMs:st.cutoff.cutoffMs})}});}
      const {portfolio,marks}=input;
      const feasible = deps.portfolioRisk && typeof deps.portfolioRisk.buildFeasibleAlternatives === "function"
        ? safeCall(() => deps.portfolioRisk.buildFeasibleAlternatives({ researchResults: candidates.map((c) => ({ symbol: c.symbol, memo: c.memo, mandate: c.mandate, liquidity:marks[c.symbol] })), holdings: holdingPackets.packets, portfolio, policy, ...C.riskInputs(marks,portfolio), sectorOf: sectorLookup(deps), clusterOf: null })) : null;
      const claimsById = Object.fromEntries(candidates.flatMap((c) => (c.factualPremises || []).map((p) => [p.claimId, { claimId: p.claimId, documentVersionId: p.documentVersionId, text: p.text }])));
      const r = await deps.gateway.finalizePortfolio({ coverage: st.effective.coverage, researchResults: candidates.map((c) => ({ symbol: c.symbol, memo: c.memo, verifiedValuation:c.verifiedValuation })), holdings: holdingPackets.packets, portfolio, feasibleAlternatives: feasible, resynthesisFeedback:st.resynthesisFeedback || null, expansionBlocked:!!st.expansionBlocked, marketState:(await rebuildContext(st,deps,accountId)).marketState, policy: { policyHash: policy.policyHash, riskPolicyHash: policy.riskPolicyHash, riskMandate: policy.riskMandate }, contextManifestHash: sha({ c: st.contextManifestHash, memos: candidates.map((c) => c.memoId), attempt: st.synthesisAttempt || 0 }), waitMs: Math.max(30000, Math.min(budget() - minStageMs, 8 * 60 * 1000)) });
      if (r.pending) return { done: false, yielded: true, reason: "sol_background_pending", checkpoint: { stage, data: st }, resumeAtMs: now() + 60000 };
      if (!r.ok) { st.synthesis = { ok: false, error: r.error, budgetBlocked: r.budgetBlocked === true }; stage = "activation"; continue; }
      addCost(r.costMinor);
      const synth = r.synthesis;
      /* claims of every BUY mandate must be supported by an independent verdict (§7.3, §17.4) */
      let claimGate = {}, claimBlocked = [];
      if (synth.expansionMandates.length && deps.claimVerifier) {
        const verified = await deps.claimVerifier.verifyAndPersistBatch({ proposals: synth.expansionMandates, claimsById, admin: deps.admin });
        claimGate = verified.byProposal || {}; claimBlocked = verified.blockingSymbols || [];
        st.verifiedProposalClaims = { byProposal: Object.fromEntries(Object.entries(claimGate).map(([s, v]) => [s, { verdictIds: v.verdictIds, allSupported: v.allSupported, blocking: v.blocking, forceAbstain: v.forceAbstain }])), allSupported: verified.allSupported };
      }
      const finalMaintenance = await stageHoldingAnalyses({ analyses:synth.holdingAnalysis || [], holdings:st.workset.managedPositionSymbols,
        accountId, cutoff:st.cutoff, policy, managerRunId, deps, nowMs:now() });
      st.finalMaintenance = finalMaintenance;
      if (finalMaintenance.actionRequired.length) st.expansionBlocked = true;
      st.synthesis = { ok: true, holdingAnalysis:finalMaintenance.acceptedAnalysis, decisions: synth.decisions, expansionMandates: synth.expansionMandates.filter((m) => !claimBlocked.includes(m.symbol)), comparisonNote: synth.comparisonNote, claimGate, claimBlocked, requestId: r.requestId, attempt: (st.synthesisAttempt || 0) + 1 };
      stage = "activation";
      continue;
    }
    if (stage === "activation") {
      let activation = { status: "EMPTY", reason: "no_expansion_mandates" };
      const mandates = st.synthesis && st.synthesis.ok ? st.synthesis.expansionMandates : [];
      const heldSet = new Set(st.workset.managedPositionSymbols);
      const legal = mandates.filter((m) => !heldSet.has(m.symbol) && st.workset.rows.some((r) => r.symbol === m.symbol && r.entryEligible));
      if (legal.length && !st.expansionBlocked) {
        const snap = await deps.portfolio.captureActivationSnapshot({ accountId, reason: "EXPANSION", admin: deps.admin, reconcile: deps.reconcile || null });
        activation = deps.mandate && typeof deps.mandate.stagePortfolioPlan === "function"
          ? await deps.mandate.stagePortfolioPlan({ planClass: "EXPANSION", portfolioPlanProposal: { planClass: "EXPANSION", decisions: st.synthesis.decisions, comparisonNote: st.synthesis.comparisonNote, planHash: sha(legal) }, proposals: legal, verifiedProposalClaims: st.verifiedProposalClaims || { byProposal: {} }, activationSnapshot: snap, accountId, cutoff: st.cutoff, policy, managerRunId, admin: deps.admin,
              eligibleSymbols: st.roster.symbols, sectorOf:sectorLookup(deps), lineage:{model:POLICY.ROLE_MODELS.manager.model,reasoningEffort:"high",contextManifestHash:st.contextManifestHash,promptHash:deps.gateway.promptHash?deps.gateway.promptHash("finalizePortfolio"):null,bySymbol:Object.fromEntries((st.research && st.research.completed || []).map(c=>[c.symbol,{researchVersionId:c.memoId,dossierVersionId:c.dossierVersionId}]))}, verifiedValuations:Object.fromEntries((st.research && st.research.completed || []).map(c=>[c.symbol,c.verifiedValuation])), marks: await liquidityMarks(legal.map((p) => p.symbol), deps, { policy, cutoffMs: st.cutoff.cutoffMs }), nowMs: now() })
          : { status: "NOT_COMMITTED", reason: "mandate_module_unavailable" };
        if ((activation.status === "NEEDS_SOL_RESYNTHESIS" || ["BASKET_INFEASIBLE","ENVELOPE_REJECTED"].includes(activation.reason)) && !(st.synthesisAttempt >= 1)) {
          st.synthesisAttempt = (st.synthesisAttempt || 0) + 1;
          st.resynthesisReason = activation.reason || "MATERIAL_ACTIVATION_CLAMP";
          st.resynthesisFeedback = {reason:st.resynthesisReason, rejectedBasket:legal, alternatives:activation.revisedFeasibleAlternatives || null, envelopes:activation.envelopes || [], rejected:activation.rejected || []};
          st.priorSynthesis = { decisions: st.synthesis.decisions, mandates: st.synthesis.expansionMandates.map((m) => m.symbol) };
          stage = "synthesis"; continue;
        }
      }
      st.activation = { status: activation.status, reason: activation.reason || null, planId: activation.planId || null, activationSnapshotId: activation.activationSnapshotId || null, mandates: legal.map((m) => m.symbol), attempts: st.synthesisAttempt || 0 };
      await record({ status: "running", activation: st.activation, investmentNote: st.synthesis && st.synthesis.ok ? String(st.synthesis.comparisonNote || "").slice(0,1600) : null });
      stage = "persist";
      continue;
    }
    if (stage === "persist") {
      const committed = st.activation && st.activation.status === "COMMITTED";
      const synthesisForRows = st.synthesis && st.synthesis.ok ? { holdingAnalysis:st.synthesis.holdingAnalysis || [], decisions: st.synthesis.decisions, expansionMandates: committed ? st.synthesis.expansionMandates : [] } : null;
      let decisions = composeFinalDecisionRows({ roster: st.roster, workset: st.workset, effective: st.effective, maintenance: { ...st.maintenance, actionRequired:[...(st.maintenance && st.maintenance.actionRequired || []).filter(x=>!(st.synthesis && st.synthesis.holdingAnalysis || []).some(h=>h.symbol===x.symbol && h.mandate)), ...(st.finalMaintenance && st.finalMaintenance.actionRequired || [])] }, research: { completed: (st.research && st.research.completed || []).map((c) => ({ request: c.request, result: c })) }, synthesis: synthesisForRows, overrides: st.overrides || {}, claimGate: st.synthesis && st.synthesis.claimGate || {} });
      if (!committed) decisions = decisions.map((d) => (d.decision === "BUY" ? { ...d, decision: "WATCH", reasonCode: st.activation && st.activation.status === "EMPTY" ? "UNFUNDED" : "UNCERTAINTY", fundingState: "UNFUNDED", reason: `BUY not activated: ${st.activation ? st.activation.reason || st.activation.status : "no activation"}` } : d));
      try { validateUniqueCapitalRanksAndFeasibility(decisions, null, policy); } catch (e) { decisions = decisions.map((d) => (d.decision === "BUY" ? { ...d, decision: "ABSTAIN", reasonCode: "MODEL_FAILURE", reason: e.message } : d)); st.rankFailure = e.code; }
      const persisted = await persistDecisionRows({ managerRunId, tradingDate, accountId, decisions, contextManifestHash: st.contextManifestHash, maintenancePortfolioPlanId: st.maintenance && st.maintenance.staged.planId, expansionPortfolioPlanId: st.activation && st.activation.planId, admin: deps.admin, cutoffMs: st.cutoff.cutoffMs });
      const frozen = await rebuildContext(st,deps,accountId);
      const learning = deps.learning || lazy("./_investorLearning");
      if (learning) await learning.freezeDecisionOutcomeRecords({accountId,managerRunId,tradingDate,cutoffMs:st.cutoff.cutoffMs,decisions,
        proposalsBySymbol:Object.fromEntries((st.synthesis && st.synthesis.expansionMandates || []).map(m=>[m.symbol,m])),
        closeBySymbol:Object.fromEntries(frozen.cards.cards.map(c=>[c.symbol,c.marketObservation && c.marketObservation.priceMicros || c.price && c.price.closeMicros || null])),
        marketBySymbol:Object.fromEntries(frozen.cards.cards.map(c=>[c.symbol,c.marketObservation])),benchmarkObservation:frozen.marketState.observations.find(x=>x.symbol==="SPY") || null,
        characteristicsBySymbol:Object.fromEntries(frozen.cards.cards.map(c=>[c.symbol,{sector:c.identity && c.identity.sector || null,volatilityBps:c.marketObservation && c.marketObservation.realizedVolatility20dBps || null}])),
        benchmarkCloseMicros:frozen.marketState.observations.find(x=>x.symbol === "SPY")?.priceMicros || null,
        versions:{model:POLICY.ROLE_MODELS.manager.model,policyHash:policy.policyHash,contextManifestHash:st.contextManifestHash},admin:deps.admin});
      for (const d of decisions.filter(d=>d.decision !== "ABSTAIN")) {
        const c=frozen.cards.cards.find(c=>c.symbol===d.symbol);
        if (c && deps.dossier.markDeltasReviewed) await deps.dossier.markDeltasReviewed({symbol:d.symbol,deltaIds:(c.changes || []).map(x=>x.eventId).filter(Boolean),managerRunId,admin:deps.admin});
      }
      st.decisions = { count: persisted.written, byDecision: countBy(decisions, "decision"), buys: decisions.filter((d) => d.decision === "BUY").map((d) => ({ symbol: d.symbol, capitalRank: d.capitalRank })) };
      st.noBuyReasons = st.decisions.buys.length ? [] : noBuyReasons({ coverage: st.coverage, freshness: st.freshness, research: st.research, synthesis: st.synthesis, activation: st.activation, control: ctrl, decisions, prior: st.noBuyReasons || [] });
      const summary = { managerRunId, status: "complete", tradingDate, accountId, universeVersion: st.roster.universeVersion, universeHash: st.roster.universeHash, eligibleCount: st.roster.eligibleCount,
        decisionCount: persisted.written, byDecision: st.decisions.byDecision, buys: st.decisions.buys, contextManifestHash: st.contextManifestHash, policyHash: st.policyHash, cutoffMs: st.cutoff.cutoffMs,
        coverage: { ok: st.coverage.ok, completedCount: st.coverage.completedCount, eligibleCount: st.coverage.eligibleCount, repaired: st.repair ? st.repair.repaired : 0 },
        maintenance: st.maintenance ? { actionable: st.maintenance.actionable.length, actionRequired: st.maintenance.actionRequired, staged: st.maintenance.staged } : null,
        research: st.research ? { completed: st.research.completed.length, failed: st.research.failed.length, deferred: st.research.deferred.length, ranges: st.research.ranges } : null,
        activation: st.activation, noBuyReasons: st.noBuyReasons, costMinor: st.costMinor, requestIds: st.requestIds, elapsedMs: now() - st.startedAtMs };
      await record({ ...summary, status: "complete", completedAtMs: now() });
      try { await db(deps.admin).col(db(deps.admin).COL.control).doc("control").set({ lastManagerRunDate: tradingDate, lastManagerRunId: managerRunId, lastManagerRun: { ...summary, requestIds: undefined } }, { merge: true }); } catch {}
      stage = "complete";
      return { done: true, summary, checkpoint: { stage, data: st } };
    }
    throw typed("UNKNOWN_STAGE", `unknown stage ${stage}`);
  }
  return { done: true, checkpoint: { stage, data: st }, summary: { managerRunId, status: "complete" } };
}
async function rebuildContext(st, deps, accountId) {
  return C.read({runId:st.managerRunId,admin:deps.admin});
}
async function stageHoldingAnalyses({ analyses, holdings, accountId, cutoff, policy, managerRunId, deps, nowMs = Date.now() }) {
  const portfolio = await deps.portfolio.snapshot({accountId,admin:deps.admin,asOfMs:nowMs});
  const priorMandateBySymbol = Object.fromEntries(portfolio.activeMandates.map(m=>[m.symbol,m]));
  const plan = buildRiskMaintenancePlan({holdingAnalysis:analyses,holdings:holdings.map(symbol=>({symbol})),priorMandateBySymbol,nowMs});
  const unchangedDesired=plan.actionableMandates.filter(m=>priorMandateBySymbol[m.symbol] && priorMandateBySymbol[m.symbol].desiredProposalHash===sha(m));
  plan.actionableMandates=plan.actionableMandates.filter(m=>!unchangedDesired.includes(m));
  let staged = {status:unchangedDesired.length ? "COMMITTED" : "EMPTY",duplicate:unchangedDesired.length>0};
  if (plan.actionableMandates.length) {
    const claims = deps.claimVerifier ? await deps.claimVerifier.verifyAndPersistBatch({proposals:plan.actionableMandates,admin:deps.admin}) : {allSupported:false};
    if (claims.allSupported) {
      const snap = await deps.portfolio.captureActivationSnapshot({accountId,reason:"FINAL_HOLDING_REVISION",admin:deps.admin,reconcile:deps.reconcile || null});
      staged = await deps.mandate.stagePortfolioPlan({planClass:"RISK_MAINTENANCE",portfolioPlanProposal:plan,proposals:plan.actionableMandates,
        verifiedProposalClaims:claims,activationSnapshot:snap,accountId,cutoff,policy,managerRunId,admin:deps.admin,nowMs,sectorOf:sectorLookup(deps),
        marks:await liquidityMarks(holdings,deps,{policy,cutoffMs:cutoff.cutoffMs})});
    } else staged={status:"REJECTED",reason:"CLAIMS_UNSUPPORTED"};
    if (staged.status !== "COMMITTED") plan.actionableMandates.forEach(m=>plan.actionRequired.push({symbol:m.symbol,reason:staged.reason || staged.status}));
  }
  const bad = new Set(plan.actionRequired.map(x=>x.symbol));
  return { staged, actionRequired:plan.actionRequired, acceptedAnalysis:analyses.filter(a=>!bad.has(a.symbol)) };
}

function sectorLookup(deps) {
  const U = deps.universe;
  const by = new Map([...(U && U.tradeTier || []), ...(U && U.researchTier || [])].map((r) => [r.symbol, r.sector]));
  return (s) => by.get(s) || null;
}
function safeCall(fn) { try { return fn(); } catch (e) { return { error: String(e.code || e.message).slice(0, 120) }; } }
function countBy(rows, key) { const out = {}; for (const r of rows) out[r[key]] = (out[r[key]] || 0) + 1; return out; }

/* ── event revision (Appendix B) ───────────────────────────────────────── */
async function runEventRevision({ claim, deps: partial = {}, control = null } = {}) {
  const deps = { ...defaultDeps(partial.admin || null), ...partial, decisionMarketCache: new Map() };
  const payload = claim.payload || {};
  const accountId = String(payload.accountId || "paper-1");
  const symbol = String(payload.symbol || "").toUpperCase();
  const R = deps.research, inputId=`${claim.jobId || claim.runId || payload.eventId}_event_inputs`;
  let frozen;
  try{frozen=await C.read({runId:inputId,admin:deps.admin});}
  catch(e){
    if(e.code!=="FROZEN_INPUTS_MISSING") throw e;
    const cutoffMs=deps.now(), policy=POLICY.loadActiveSync(control || {});
    const state=await deps.portfolio.symbolState({accountId,symbol,admin:deps.admin});
    const prior=await R.latest(symbol,{admin:deps.admin,cutoffMs});
    const packet=await R.buildPacket({symbol,cutoff:cutoffMs,prior,directive:"DELTA_REVISION",admin:deps.admin,deps});
    const portfolio=await deps.portfolio.getSnapshot({accountId,asOfMs:cutoffMs,admin:deps.admin});
    const marks=await liquidityMarks([symbol],deps,{policy,cutoffMs});
    frozen=await C.freeze({runId:inputId,admin:deps.admin,context:{cutoffMs,policy,state,prior,packet,portfolio,marks,thesisHistory:await R.history(symbol,{admin:deps.admin,cutoffMs,openedAtMs:Date.parse(state.position && state.position.openedAt || "") || null})}});
  }
  const {cutoffMs,policy,state,prior,packet,portfolio,marks}=frozen;
  if(!packet.ok) return {ok:false,symbol,reason:packet.reason,decision:"ABSTAIN",reasonCode:"DATA_INCOMPLETE"};
  const bound = deps.tools ? deps.tools.allowlisted(deps.toolBindings || deps.tools.productionBindings({ accountId, admin:deps.admin, policy, portfolio, marks, decisionPacket:packet, sectorOf:sectorLookup(deps) }), policy.toolPolicy, { symbol, cutoffMs }) : { tools: null, log: [] };
  const persistResearch = async (r) => R.persistImmutable({ ...r, dossierVersionId: packet.dossierVersionId, dossierHash: packet.dossierHash }, { admin: deps.admin, managerRunId: claim.runId || null, directive: "EVENT", cutoffMs });
  if (state.kind === "NONHOLDING_NO_ENTRY") {
    const memo = await deps.gateway.researchCompany({ dossier: packet, delta: packet.delta, portfolio, tools: bound.tools, directive: "RESEARCH_NOW", prior: packet.prior, cutoffMs });
    if (memo.pending) return {pending:true};
    const persisted = await persistResearch(memo);
    if (persisted.persisted && deps.claimVerifier) { try { await deps.claimVerifier.verifyAndPersist({ premises: persisted.factualPremises, sourceManifest: persisted.sourceManifest, admin: deps.admin }); } catch {} }
    const JOBS = deps.jobs || lazy("./_investorJobs");
    if (persisted.persisted && JOBS && typeof JOBS.enqueueOnce === "function") {
      await JOBS.enqueueOnce({ task: "portfolio_synthesis", dedupeId: `event-portfolio:${payload.eventId || symbol}`, accountId, priority: 60,
        payload: { accountId, changedSymbol: symbol, researchVersionId: persisted.memoId, eventId: payload.eventId || null, cutoff: cutoffMs } });
    }
    return { ok: persisted.persisted === true, symbol, state: state.kind, disposition: "RESEARCHED_AWAITING_PORTFOLIO_SYNTHESIS", memoId: persisted.memoId || null, standaloneBuy: false };
  }
  let assessment;
  const baseline = {...(state.appliedMandate || {}), priorMemo:prior && prior.memo || null, thesisHistory:frozen.thesisHistory};
  const evidenceDelta = {...packet.delta,claims:packet.claims,pendingChanges:packet.pendingChanges,marketState:packet.marketState,marketObservation:packet.marketObservation,learning:packet.learning};
  if (state.kind === "UNFILLED_ENTRY" || state.kind === "PARTIALLY_FILLED_ENTRY") {
    const r = await deps.gateway.reviseEntry({ baseline: { symbol, ...baseline }, delta: evidenceDelta, state: { kind: state.kind, ownedQuantityUnits: state.ownedQuantityUnits, remainingEntryUnits: state.remainingEntryUnits, entries: state.entries }, portfolio });
    assessment = r.ok ? { ...r.revision, ok: true } : { ok: false, symbol, decision: "ABSTAIN", reasonCode: "MODEL_FAILURE", entryRevision: "KEEP", error: r.error };
  } else {
    const r = await deps.gateway.reviseHolding({ baseline: { symbol, ...baseline }, delta: evidenceDelta, position: state.position, mandate: state.appliedMandate });
    assessment = r.ok ? { ...r.revision, ok: true } : { ok: false, symbol, decision: "ABSTAIN", reasonCode: "MODEL_FAILURE", researchDirective: "NONE", error: r.error };
  }
  let expanded = null;
  if (assessment.ok && assessment.researchDirective === "FULL_REUNDERWRITE") {
    const full = {...packet,directive:"FULL_REUNDERWRITE"};
    const memo = await deps.gateway.researchCompany({ dossier: full.ok ? full : packet, delta: packet.delta, portfolio, tools: bound.tools, directive: "FULL_REUNDERWRITE", prior: packet.prior, cutoffMs });
    if (memo.pending) return {pending:true};
    expanded = await persistResearch(memo);
    if (expanded.persisted && deps.claimVerifier) { try { await deps.claimVerifier.verifyAndPersist({ premises: expanded.factualPremises, sourceManifest: expanded.sourceManifest, admin: deps.admin }); } catch {} }
    const scenarios = deps.portfolioRisk && typeof deps.portfolioRisk.runScenarios === "function" ? safeCall(() => deps.portfolioRisk.runScenarios({ portfolio, candidates: [], policy, ...C.riskInputs(marks,portfolio), sectorOf: sectorLookup(deps), clusterOf: null })) : null;
    const fin = await deps.gateway.finalizeEventRevision({ priorAssessment: { symbol, ...assessment }, expandedResearch: expanded.persisted ? expanded.memo : null, state: { kind: state.kind, ownedQuantityUnits: state.ownedQuantityUnits }, portfolio, scenarios });
    assessment = fin.ok ? { ...fin.revision, ok: true, entryRevision: fin.revision.entryRevision || assessment.entryRevision || null } : { ok: false, symbol, decision: "ABSTAIN", reasonCode: "MODEL_FAILURE", error: fin.error, materiality: "UNDETERMINED" };
  }
  const MANDATE = deps.mandate;
  const eventId = payload.eventId || null;
  if ((state.kind === "UNFILLED_ENTRY" || state.kind === "PARTIALLY_FILLED_ENTRY") && assessment.entryRevision === "REVOKE") {
    const out = MANDATE && typeof MANDATE.requestEntryCancelWithOutbox === "function" ? await MANDATE.requestEntryCancelWithOutbox({ state, eventId, accountId, admin: deps.admin }) : { status: "NOT_APPLIED", reason: "mandate_module_unavailable" };
    return { ok: true, symbol, state: state.kind, action: "REVOKE_ENTRY", decision: assessment.decision, outcome: out };
  }
  if(state.kind === "UNFILLED_ENTRY" && assessment.ok && assessment.decision !== "ABSTAIN" && ["KEEP","REVISE"].includes(assessment.entryRevision)) {
    const researched=await deps.gateway.researchCompany({dossier:packet,delta:packet.delta,portfolio,tools:bound.tools,directive:"ENTRY_REUNDERWRITE",prior:packet.prior,cutoffMs});
    if(researched.pending) return {pending:true};
    const saved=await persistResearch(researched);
    if(!saved.persisted) return {ok:false,decision:"ABSTAIN",reason:researched.error || "ENTRY_RESEARCH_FAILED"};
    const cancelled=await MANDATE.requestEntryCancelWithOutbox({state,eventId,accountId,admin:deps.admin});
    const jobs=deps.jobs || lazy("./_investorJobs");
    await jobs.enqueueOnce({task:"portfolio_synthesis",dedupeId:`entry-revision:${eventId || claim.jobId}`,accountId,priority:60,
      payload:{accountId,changedSymbol:symbol,researchVersionId:saved.memoId,eventId,awaitEntryCancellation:symbol,cutoff:cutoffMs}});
    return {ok:true,symbol,action:"ENTRY_REVIEWED_AWAITING_PORTFOLIO_SYNTHESIS",cancelled,memoId:saved.memoId,standaloneBuy:false};
  }
  if(state.kind === "PARTIALLY_FILLED_ENTRY" && assessment.heldDecision) assessment.decision=assessment.heldDecision;
  if (assessment.decision === "ABSTAIN" && state.kind === "UNFILLED_ENTRY") {
    const out = MANDATE && typeof MANDATE.keepEvidencePausedAndCancelRemainder === "function" ? await MANDATE.keepEvidencePausedAndCancelRemainder({ state, eventId, accountId, admin: deps.admin }) : { status: "NOT_APPLIED", reason: "mandate_module_unavailable" };
    return { ok: true, symbol, state: state.kind, action: "KEEP_PAUSED_CANCEL_REMAINDER", decision: "ABSTAIN", reasonCode: assessment.reasonCode || "UNCERTAINTY", outcome: out };
  }
  if (assessment.decision === "ABSTAIN" && state.ownedQuantityUnits !== "0") {
    const out = MANDATE && typeof MANDATE.retainProtectionActionRequired === "function" ? await MANDATE.retainProtectionActionRequired({ state, assessment, accountId, admin: deps.admin }) : { status: "ACTION_REQUIRED", reason: "mandate_module_unavailable" };
    return { ok: true, symbol, state: state.kind, action: "RETAIN_PROTECTION_ACTION_REQUIRED", decision: "ABSTAIN", reasonCode: assessment.reasonCode || "UNCERTAINTY", outcome: out };
  }
  if (assessment.mandate && state.ownedQuantityUnits !== "0" && antiGoalpostViolations(assessment.mandate,state.appliedMandate).length) return {ok:false,action:"RETAIN_PROTECTION_ACTION_REQUIRED",reason:"ANTI_GOALPOST",symbol};
  if (!assessment.mandate) return { ok: true, symbol, state: state.kind, action: "NO_CHANGE", decision: assessment.decision, reasonCode: assessment.reasonCode || null, materiality: assessment.materiality || null };
  if (assessment.mandate.decision === "BUY" && state.ownedQuantityUnits !== "0") return { ok: false, symbol, state: state.kind, action: "REJECTED", reason: "SCALING_IN_FORBIDDEN", decision: "ABSTAIN", reasonCode: "UNCERTAINTY" };
  const claims = deps.claimVerifier ? await deps.claimVerifier.verifyAndPersistBatch({ proposals: [assessment.mandate], admin: deps.admin }) : { allSupported: false, blockingSymbols: [symbol], byProposal: {} };
  if (!claims.allSupported) return { ok: true, symbol, state: state.kind, action: "RETAIN_PROTECTION_ACTION_REQUIRED", decision: "ABSTAIN", reasonCode: claims.forceAbstainSymbols && claims.forceAbstainSymbols.length ? "EVIDENCE_CONFLICT" : "DATA_INCOMPLETE", claims: { blocking: claims.blockingSymbols } };
  const activation = await deps.portfolio.captureActivationSnapshot({ accountId, reason: "EVENT_REVISION", admin: deps.admin, reconcile: deps.reconcile || null });
  const staged = MANDATE && typeof MANDATE.stagePortfolioPlan === "function"
    ? await MANDATE.stagePortfolioPlan({ planClass: "RISK_MAINTENANCE", portfolioPlanProposal: { planClass: "RISK_MAINTENANCE", single: true, symbol, assessment: { decision: assessment.decision, materiality: assessment.materiality || null } }, proposals: [assessment.mandate], verifiedProposalClaims: claims, activationSnapshot: activation, accountId, cutoff: { cutoffMs }, policy, sectorOf:sectorLookup(deps), marks: await liquidityMarks([symbol], deps, { policy, cutoffMs }), managerRunId: claim.runId || null, admin: deps.admin })
    : { status: "NOT_COMMITTED", reason: "mandate_module_unavailable" };
  return { ok: true, symbol, state: state.kind, action: "STAGED_REVISION", decision: assessment.decision, reasonCode: assessment.reasonCode || null, materiality: assessment.materiality || null, staged: { status: staged.status, reason: staged.reason || null, planId: staged.planId || null } };
}

module.exports = {
  liquidityMarks, stageHoldingAnalyses, sectorLookup,
  MANAGER_VERSION, RUN_SCHEMA, DECISION_SCHEMA, STAGES, NO_BUY_REASONS,
  freezeDecisionCutoff, assertExactCoverageInput, validateCoverage, mergeStructuralCoverageRepair,
  antiGoalpostViolations, buildRiskMaintenancePlan, validateUniqueResearchPriority, forceNonExecutableForIncompleteResearch,
  composeFinalDecisionRows, validateUniqueCapitalRanksAndFeasibility, noBuyReasons,
  persistDecisionRows, writeRun, readRun, defaultDeps, runManagerMeeting, runEventRevision,
};
