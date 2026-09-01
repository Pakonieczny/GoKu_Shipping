/* Investor_AI — measured production-paper soak evidence and acceptance gates. */
"use strict";

const A = require("./_investorAdmin");
const M = require("./_investorMarket");
const L = require("./_investorLedger");

const VERSION = "paper-soak-v1";
const REQUIRED_SESSIONS = 10;
const REQUIRED_CYCLE_SUCCESS = 0.99;
const REQUIRED_NATURAL_CLOSES = 200;

function rows(snap) {
  const out = []; if (snap && typeof snap.forEach === "function") snap.forEach((d) => out.push(d.data()));
  return out;
}
function consecutiveSessions(dates) {
  const sorted = [...new Set(dates)].sort();
  for (let i = 1; i < sorted.length; i += 1) {
    if (M.tradingSessionsBetween(sorted[i - 1], sorted[i]) !== 1) return false;
  }
  return sorted.length > 0;
}

async function recordCycle({ jobId, accountId, date, manual = false,
  manifestTotal = 0, manifestComplete = 0, manifestInvalid = 0,
  reconciliation = null, settlement = null, dailyFinalization = null } = {}) {
  if (!jobId || !accountId || !/^\d{4}-\d{2}-\d{2}$/.test(String(date || ""))) {
    return { recorded: false, reason: "incomplete_cycle_identity" };
  }
  const ref = A.col(A.COL.soakCycles).doc(jobId);
  const row = { version: VERSION, jobId, accountId, date, manual: manual === true,
    status: "complete", manifestTotal: Number(manifestTotal) || 0,
    manifestComplete: Number(manifestComplete) || 0,
    manifestInvalid: Number(manifestInvalid) || 0,
    reconciliationPass: reconciliation && reconciliation.pass === true,
    settlement: settlement || null, dailyFinalization: dailyFinalization || null,
    completedAt: A.FV.serverTimestamp(), ...A.envelope({ created_by: "soak.recordCycle" }) };
  await ref.set(row, { merge: false });
  return { recorded: true, id: ref.id };
}

async function status(accountId, { requiredSessions = REQUIRED_SESSIONS } = {}) {
  const [cycleSnap, jobSnap, orderSnap, fillSnap, positionSnap, tradeSnap,
    execution, journal, lifecycle] = await Promise.all([
    A.col(A.COL.soakCycles).where("accountId", "==", accountId).get(),
    A.col(A.COL.jobs).where("accountId", "==", accountId).get(),
    A.col(A.COL.orders).where("accountId", "==", accountId).get(),
    A.col(A.COL.fills).where("accountId", "==", accountId).get(),
    A.col(A.COL.positions).where("accountId", "==", accountId).get(),
    A.col(A.COL.trades).where("accountId", "==", accountId).get(),
    L.executionAudit(accountId), L.auditLedger(accountId), L.lifecycleAudit(accountId),
  ]);
  const cycles = rows(cycleSnap).filter((x) => x.status === "complete" && x.manual !== true);
  const dates = [...new Set(cycles.map((x) => x.date).filter(Boolean))].sort();
  const recentDates = dates.slice(-Math.max(1, Number(requiredSessions) || REQUIRED_SESSIONS));
  const recentSet = new Set(recentDates);
  const recentCycles = cycles.filter((x) => recentSet.has(x.date));
  const jobs = rows(jobSnap).filter((x) => x.task === "cycle" && x.manual !== true
    && (!recentSet.size || !x.sessionDate || recentSet.has(x.sessionDate)));
  const successes = jobs.filter((x) => x.status === "complete").length;
  const cycleSuccessRate = jobs.length ? successes / jobs.length
    : (recentCycles.length ? 1 : 0);
  const manifestTotal = recentCycles.reduce((s, x) => s + (Number(x.manifestTotal) || 0), 0);
  const manifestComplete = recentCycles.reduce((s, x) => s + (Number(x.manifestComplete) || 0), 0);
  const manifestInvalid = recentCycles.reduce((s, x) => s + (Number(x.manifestInvalid) || 0), 0);
  const reconciled = recentCycles.every((x) => x.reconciliationPass === true);
  const orders = rows(orderSnap), fills = rows(fillSnap), positions = rows(positionSnap), trades = rows(tradeSnap);
  const validMark = positions.some((p) => Number(p.lastMarkUsd) > 0
    && L.validProvenance(p.lastMarkProvenance));
  const naturalClosed = trades.filter((t) => t.forcedTest !== true).length;
  const lifecycleExamples = {
    entryDecision: orders.some((o) => !!o.decisionManifestHash),
    approval: orders.some((o) => Number(o.approvedAtMs) > 0),
    fill: fills.length > 0,
    mark: validMark,
    exit: trades.some((t) => !!t.exitDecisionAtMs),
    closedTrade: trades.length > 0,
  };
  const criteria = {
    consecutiveSessions: recentDates.length >= requiredSessions && consecutiveSessions(recentDates),
    scheduledCycleSuccess: cycleSuccessRate >= REQUIRED_CYCLE_SUCCESS,
    completeDecisionIdentity: manifestTotal > 0 && manifestComplete === manifestTotal
      && manifestInvalid === 0,
    reconciledLedger: recentCycles.length > 0 && reconciled && journal.pass
      && execution.pass && lifecycle.pass,
    completeLifecycleExample: Object.values(lifecycleExamples).every(Boolean),
  };
  return { version: VERSION, accountId, requiredSessions, sessionsObserved: dates.length,
    recentDates, cycleSuccessRate: Number(cycleSuccessRate.toFixed(6)),
    scheduledCycles: jobs.length || recentCycles.length,
    manifest: { total: manifestTotal, complete: manifestComplete, invalid: manifestInvalid,
      coveragePct: manifestTotal ? Number((100 * manifestComplete / manifestTotal).toFixed(4)) : 0 },
    audits: { journal, execution, lifecycle }, lifecycleExamples,
    naturalClosedObservations: naturalClosed,
    strategyEffectivenessReady: naturalClosed >= REQUIRED_NATURAL_CLOSES,
    strategyEffectivenessMinimum: REQUIRED_NATURAL_CLOSES,
    criteria, operationalSoakPass: Object.values(criteria).every(Boolean) };
}

module.exports = { VERSION, REQUIRED_SESSIONS, REQUIRED_CYCLE_SUCCESS,
  REQUIRED_NATURAL_CLOSES, consecutiveSessions, recordCycle, status };
