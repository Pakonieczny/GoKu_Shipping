/*  netlify/functions/_investorAlerts.js  (fund-manager-v1)
 *  ---------------------------------------------------------------------------
 *  Investor AI — typed alerts with stable condition ids (blueprint §12.1,
 *  §14.7, §14.8).
 *
 *  An alert is DERIVED from an underlying condition and resolves only when
 *  the condition clears — never by being dismissed. Acknowledgement is a
 *  separate, recorded act that silences nothing. Condition ids are stable
 *  so the same problem never appears as a new alert every tick.
 * ---------------------------------------------------------------------------
 */

"use strict";

const A = require("./_investorAdmin");

const SEVERITIES = Object.freeze(["info", "warning", "critical"]);
const CONDITIONS = Object.freeze({
  fixtures_failing:          { severity: "critical", title: "Deployed build attestation failing", action: "Trading is halted on this build until the fixture set passes." },
  coverage_incomplete:       { severity: "critical", title: "Manager coverage incomplete", action: "No new BUY activates; inspect the missing, duplicate or unknown symbols." },
  stale_cards:               { severity: "warning", title: "Stale dossier cards at the freeze", action: "Check the overnight ingest pass timing and source health." },
  claims_unsupported:        { severity: "warning", title: "Material claims not supported", action: "Verification blocked activation for the named proposals." },
  research_deferred:         { severity: "info", title: "Research deferred by budget or deadline", action: "Deferred names finished as WATCH with RESEARCH_DEFERRED." },
  budget_exhausted:          { severity: "critical", title: "Daily model reservation exhausted", action: "Model work abstains under evidence_unavailable until the reservation resets or is raised." },
  manager_missed_deadline:   { severity: "warning", title: "Holding maintenance missed the hard deadline", action: "Prior acknowledged protection retained; review ACTION REQUIRED holdings." },
  manager_failed_closed:     { severity: "critical", title: "Manager Meeting failed closed", action: "See the run's typed reasons; no decisions were persisted." },
  action_required:           { severity: "critical", title: "Holding requires action", action: "Protection retained; a valid Sol holding mandate or an operator decision is required." },
  unprotected_position:      { severity: "critical", title: "Position without acknowledged protection", action: "Executor is attaching protection; if the SLA lapses the emergency path applies." },
  protection_pending:        { severity: "warning", title: "Protection pending acknowledgement", action: "Waiting for the broker to acknowledge the protective legs." },
  emergency_engaged:         { severity: "critical", title: "Emergency risk policy engaged", action: "Reconcile, review the labelled actions, and let Sol re-review before new exposure." },
  stale_broker_truth:        { severity: "critical", title: "Broker truth is stale", action: "Expansion is frozen until reconciliation succeeds." },
  reconciliation_unresolved: { severity: "critical", title: "Reconciliation unresolved", action: "Executor paused for safety until the ledger and broker agree." },
  ledger_conservation_failed:{ severity: "critical", title: "Ledger conservation failed", action: "Executor paused; audit the journal before any new order." },
  buys_frozen:               { severity: "warning", title: "New buys frozen", action: "See the freeze reason; expansion resumes when it clears." },
  source_scope_breach:       { severity: "warning", title: "Source scope budget breached", action: "A global feed was fetched per company; inspect the sweep." },
  ingest_late:               { severity: "warning", title: "Overnight ingest did not finish before the freeze", action: "Cards may be stale; the coverage gate reports the effect." },
  oversell_incident:         { severity: "critical", title: "Oversell incident", action: "Both OCO exits filled; cover-to-zero engaged; reconcile before new exposure." },
  live_adapter_disabled:     { severity: "info", title: "Live broker adapter disabled", action: "Paper execution only; enable under LIMITED_LIVE with the environment flag." },
});

function idFor(code, scope = null) { return scope ? `${code}:${scope}` : code; }
/** PURE. Which conditions hold right now, from the underlying state. */
function deriveConditions({ control = {}, manager = null, portfolio = null, execution = null, ingest = null, fixtures = null, budget = null, nowMs = Date.now() } = {}) {
  const out = [];
  const add = (code, scope, detail) => out.push({ conditionId: idFor(code, scope), code, scope, severity: CONDITIONS[code].severity, title: CONDITIONS[code].title, action: CONDITIONS[code].action, detail: detail || null, derivedAtMs: nowMs });
  if (fixtures && fixtures.pass === false) add("fixtures_failing", null, { failing: fixtures.failing || null });
  const run = manager || control.lastManagerRun || null;
  if (run) {
    if (run.status === "failed_closed") add("manager_failed_closed", run.managerRunId, { reasons: run.noBuyReasons || null });
    const reasons = Array.isArray(run.noBuyReasons) ? run.noBuyReasons : [];
    for (const r of reasons) {
      if (r.code === "COVERAGE_INCOMPLETE") add("coverage_incomplete", run.managerRunId, { missing: r.missing, duplicates: r.duplicates, unknown: r.unknown, completedCount: r.completedCount, eligibleCount: r.eligibleCount });
      if (r.code === "STALE_CARDS") add("stale_cards", run.managerRunId, { count: r.count });
      if (r.code === "CLAIMS_UNSUPPORTED") add("claims_unsupported", run.managerRunId, { symbols: r.symbols });
      if (r.code === "RESEARCH_DEFERRED") add("research_deferred", run.managerRunId, { symbols: r.symbols });
      if (r.code === "BUDGET_EXHAUSTED") add("budget_exhausted", null, null);
    }
    if (run.maintenance && run.maintenance.deadlineMissed) add("manager_missed_deadline", run.managerRunId, null);
    for (const ar of (run.maintenance && run.maintenance.actionRequired) || []) add("action_required", ar.symbol, { reason: ar.reason });
  }
  if (budget && budget.blocked) add("budget_exhausted", null, { spentMinor: budget.spentMinor, ceilingMinor: budget.ceilingMinor });
  if (control.emergencyState === "ENGAGED") add("emergency_engaged", null, { actions: (control.emergencyLastActions || []).slice(0, 8) });
  if (control.buyState === "FROZEN" || control.freezeNewBuys) add("buys_frozen", null, { reason: control.freezeReason || null });
  if (control.executorState === "PAUSED_SAFETY") add(control.executorPauseReason === "LEDGER_CONSERVATION_FAILED" ? "ledger_conservation_failed" : "reconciliation_unresolved", null, { reason: control.executorPauseReason || null });
  if (control.sourceScopeBreach) add("source_scope_breach", null, { breach: control.sourceScopeBreach });
  const pass = ingest || control.lastIngestPass || null;
  if (pass && pass.freshness && pass.freshness.finishedBeforeFreeze === false) add("ingest_late", pass.tradingDate, { oldestCardAgeAtFreezeSeconds: pass.freshness.oldestCardAgeAtFreezeSeconds });
  const tick = execution || control.lastExecutionTick || null;
  if (tick && tick.reasons && tick.reasons.includes("STALE_BROKER_TRUTH")) add("stale_broker_truth", null, null);
  if (tick && tick.conservation && tick.conservation.pass === false) add("ledger_conservation_failed", null, { discrepancies: tick.conservation.discrepancies });
  for (const p of (portfolio && portfolio.positions) || []) {
    if (!p.lossBoundaryPriceMicros && p.protectionState !== "EMERGENCY_PLAN") add("unprotected_position", p.symbol, { quantityUnits: p.quantityUnits });
    else if (p.protectionState === "PROTECTION_PENDING") add("protection_pending", p.symbol, null);
  }
  for (const m of (portfolio && portfolio.activeMandates) || []) {
    if (m.status === "ACTION_REQUIRED") add("action_required", m.symbol, { reason: m.actionRequiredReason || null });
    if (m.status === "OVERSELL_INCIDENT") add("oversell_incident", m.symbol, null);
  }
  const byId = new Map();
  for (const c of out) if (!byId.has(c.conditionId)) byId.set(c.conditionId, c);
  return [...byId.values()];
}
/** Raise what holds, keep what is acknowledged, resolve only what cleared. */
async function upsertAlerts({ admin = null, accountId, derived = [], nowMs = Date.now() } = {}) {
  const D = admin || A;
  const active = [];
  (await D.col(D.COL.alerts).where("active", "==", true).get()).forEach((d) => active.push(d.data()));
  const derivedIds = new Set(derived.map((c) => c.conditionId));
  let raised = 0, kept = 0, resolved = 0;
  for (const c of derived) {
    const cur = active.find((a) => a.conditionId === c.conditionId);
    if (cur) { kept += 1; await D.col(D.COL.alerts).doc(c.conditionId).set({ detail: c.detail, lastSeenAtMs: nowMs, occurrences: (Number(cur.occurrences) || 1) + 1 }, { merge: true }); continue; }
    raised += 1;
    await D.col(D.COL.alerts).doc(c.conditionId).set({ conditionId: c.conditionId, code: c.code, scope: c.scope, severity: c.severity, kind: c.code.toUpperCase(), title: c.title, action: c.action, detail: c.detail, accountId, active: true, raisedAtMs: nowMs, lastSeenAtMs: nowMs, acknowledgedAtMs: null, acknowledgedBy: null, resolvedAtMs: null, occurrences: 1 }, { merge: true });
  }
  for (const a of active) {
    if (derivedIds.has(a.conditionId)) continue;
    if (a.accountId && accountId && a.accountId !== accountId) continue;
    resolved += 1;
    await D.col(D.COL.alerts).doc(a.conditionId).set({ active: false, resolvedAtMs: nowMs, resolvedBy: "condition_cleared" }, { merge: true });
  }
  return { raised, kept, resolved, active: derived.length };
}
/** Acknowledge records who saw it; the alert stays active until its condition clears. */
async function acknowledge({ admin = null, conditionId, by, nowMs = Date.now() } = {}) {
  const D = admin || A;
  const ref = D.col(D.COL.alerts).doc(String(conditionId));
  const s = await ref.get();
  if (!s.exists) return { acknowledged: false, reason: "unknown_condition" };
  await ref.set({ acknowledgedAtMs: nowMs, acknowledgedBy: String(by || "operator").slice(0, 80) }, { merge: true });
  return { acknowledged: true, stillActive: s.data().active === true };
}
async function listActive({ admin = null, accountId = null } = {}) {
  const D = admin || A;
  const out = [];
  (await D.col(D.COL.alerts).where("active", "==", true).get()).forEach((d) => { const a = d.data(); if (!accountId || !a.accountId || a.accountId === accountId) out.push(a); });
  const rank = { critical: 0, warning: 1, info: 2 };
  return out.sort((a, b) => (rank[a.severity] ?? 3) - (rank[b.severity] ?? 3) || Number(b.raisedAtMs) - Number(a.raisedAtMs));
}

module.exports = { SEVERITIES, CONDITIONS, idFor, deriveConditions, upsertAlerts, acknowledge, listActive };
