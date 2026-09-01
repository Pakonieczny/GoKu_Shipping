/*  netlify/functions/_investorLadder.js  (v1.0)
 *  ---------------------------------------------------------------------------
 *  Investor_AI — the automation ladder, made real.
 *
 *  WHAT WAS WRONG
 *  ------------------------------------------------------------------------
 *  `automationLadder` and `promotionGates` were prose in a config object with
 *  ZERO readers anywhere in the codebase. The dashboard told the operator that
 *  each stage "unlocks only after its promotion gate is measured, not
 *  asserted", while the API accepted any stage from a dropdown with no check
 *  whatsoever. The gates were asserted and nothing else.
 *
 *  This file measures them. Every gate is a function of recorded state, so a
 *  stage can be refused with a specific reason and the operator can see exactly
 *  what is still missing rather than a locked door.
 *
 *  THE SHAPE OF THE OVERSIGHT
 *  ------------------------------------------------------------------------
 *  The operator's job is to supervise BY EXCEPTION, not to click Approve on
 *  every trade. That means:
 *    · the system may promote itself UP the ladder only when the evidence for
 *      the next stage is actually measured and passes;
 *    · it demotes itself immediately when evidence stops supporting the stage
 *      it is on — demotion needs no permission and no quorum;
 *    · the operator can veto, hold, set a maximum ceiling, or immediately lower
 *      autonomy, but cannot force an unearned higher stage.
 *
 *  Self-promotion is deliberately conservative and self-demotion deliberately
 *  trigger-happy. Those asymmetries are the whole safety design: the cost of
 *  being slow to automate is some foregone paper profit, and the cost of being
 *  slow to stop is unbounded.
 * ---------------------------------------------------------------------------
 */

"use strict";

const STAGES = ["research", "approval", "shadow", "limited_auto"];

function stageIndex(s) { const i = STAGES.indexOf(String(s || "research")); return i < 0 ? 0 : i; }

/**
 * Measure every promotion gate against recorded state.
 *
 * `ev` carries what the caller has already gathered:
 *   fixturesPass      — golden fixtures green on this deploy
 *   ledgerBalanced    — every journal entry balances
 *   closedRealTrades  — count of real closed positions
 *   allocation        — the stored allocator verdict (powered, bestEffectiveN)
 *   costMeter         — cumulative gross edge vs cumulative friction
 *   citationValidity  — model lane citation pass-rate, null if unmeasured
 *   recallBenchmark   — known-cause recall measurement, null if never run
 *   cycleErrorRate    — share of recent cycles that failed
 *   historyCoverage   — share of the roster with real long-horizon context
 */
function evaluateGates(ev = {}) {
  const g = [];
  const add = (id, stage, pass, detail, { blocking = true, measured = true,
    critical = false } = {}) => g.push({ id, stage, pass: !!pass, detail,
      blocking, measured, critical });

  /* ── to APPROVAL: the accounting has to be trustworthy ─────────────── */
  add("ledger_balanced", "approval", ev.ledgerBalanced === true,
      ev.ledgerBalanced === true ? "journal projection exactly matches its immutable legs"
        : "ledger reconstruction was absent or discrepant",
      { measured: ev.ledgerBalanced != null, critical: true });
  add("fixtures", "approval", ev.fixturesPass === true && ev.fixturesCurrent === true,
      ev.fixturesPass === true && ev.fixturesCurrent === true
        ? "golden fixtures are attested to the running commit"
        : "fixtures are failing, absent, or attested to a different commit",
      { measured: ev.fixturesPass != null && ev.fixturesCurrent != null, critical: true });
  add("execution_clock", "approval", ev.eligibleFillsOnly === true,
      ev.eligibleFillsOnly === true
        ? "every fill has a post-decision clock and immutable source hash"
        : "execution provenance audit is absent or failing",
      { measured: ev.eligibleFillsOnly != null, critical: true });
  add("safety_epoch", "approval", ev.safetyEpochValid === true,
      ev.safetyEpochValid === true ? "account and frozen policy hashes match the activated safety epoch"
        : "the active account/policy identity is not safety-epoch bound",
      { measured: ev.safetyEpochValid != null, critical: true });

  /* ── to SHADOW: the machinery has to be stable ─────────────────────── */
  add("cycle_health", "shadow", Number.isFinite(ev.cycleErrorRate) && ev.cycleErrorRate < 0.05,
      Number.isFinite(ev.cycleErrorRate)
        ? `${Math.round(ev.cycleErrorRate * 100)}% of recent cycles failed (needs under 5%)`
        : "recent cycle health is unmeasured",
      { measured: Number.isFinite(ev.cycleErrorRate) });
  add("market_provenance", "shadow", ev.marketDataEligible === true,
      ev.marketDataEligible === true ? "consolidated, hash-bound market data is execution eligible"
        : "market feed is non-consolidated, stale, or lacks source provenance",
      { measured: ev.marketDataEligible != null, critical: true });
  add("history_coverage", "shadow", Number.isFinite(ev.historyCoverage) && ev.historyCoverage >= 0.8,
      Number.isFinite(ev.historyCoverage)
        ? `${Math.round(ev.historyCoverage * 100)}% of roster has pre-decision long history (needs 80%)`
        : "history coverage is unmeasured",
      { measured: Number.isFinite(ev.historyCoverage) });
  add("shadow_flowing", "shadow", Number(ev.shadowDays) >= 200,
      ev.shadowUnavailable ? "shadow portfolio-day ledger is unavailable"
        : `${Number(ev.shadowDays) || 0} complete shadow portfolio days (needs 200 for machinery validation)`,
      { measured: !ev.shadowUnavailable });

  /* ── to LIMITED_AUTO: the strategy itself has to have shown something ─ */
  const alloc = ev.allocation || {};
  add("powered", "limited_auto", alloc.powered === true && !!alloc.leaderId,
      alloc.powered && alloc.leaderId
        ? `${alloc.leaderId} passed the selection-corrected absolute and incumbent-relative tests`
        : `no validated leader; least-observed arm has ${alloc.bestEffectiveN ?? 0} of ${alloc.requiredEffectiveN ?? "?"} required days`,
      { measured: alloc.powered != null });
  const overfit = alloc.overfitGuard || {};
  const guardLeaderId = alloc.leaderId || (alloc.forwardLock && alloc.forwardLock.leaderId);
  const leaderDsr = overfit.dsrByVariant && overfit.dsrByVariant[guardLeaderId];
  add("overfit_guard", "limited_auto", !!(leaderDsr && leaderDsr.pass
      && overfit.pbo && overfit.pbo.pass),
      leaderDsr && overfit.pbo
        ? `DSR probability ${Number(leaderDsr.probability || 0).toFixed(3)} (needs .950); PBO ${overfit.pbo.pbo == null ? "warming" : Number(overfit.pbo.pbo).toFixed(3)} (needs <=.200)`
        : "development-only DSR/PBO guard is unavailable or still warming",
      { measured: !!(leaderDsr && overfit.pbo), critical: true });
  const cal = ev.calibration || {};
  add("chronological_holdout", "limited_auto", cal.pass === true
      && Number(cal.calibratedNetLowerBoundBps) > 0,
      cal.pass ? `untouched holdout lower bound +${Number(cal.calibratedNetLowerBoundBps).toFixed(2)}bp/day`
        : "chronological embargoed holdout, doubled-cost, or unresolved-outcome stress is not positive",
      { measured: cal.calibrated != null, critical: true });
  const forward = alloc.forwardResult || {};
  add("locked_forward_paper", "limited_auto", forward.complete === true
      && forward.pass === true && Number(forward.calibratedNetLowerBoundBps) > 0,
      forward.complete
        ? (forward.pass ? `locked future-paper stress lower bound +${Number(forward.calibratedNetLowerBoundBps).toFixed(2)}bp/day`
          : "locked future-paper confirmation failed one or more stress bounds")
        : `${Number(forward.confirmationSessions) || 0} of ${Number(forward.requiredSessions) || 126} untouched post-embargo sessions collected`,
      { measured: !!alloc.forwardLock, critical: true });
  add("paper_sample", "limited_auto", Number(ev.closedRealTrades) >= 200,
      `${Number(ev.closedRealTrades) || 0} immutable closed paper trades (needs 200 out of sample)`,
      { measured: ev.closedRealTrades != null });
  const cm = ev.costMeter || {};
  add("edge_beats_cost", "limited_auto",
      Number(cm.grossEdgeUsd) > Number(cm.frictionUsd) && Number(cm.netEdgeUsd) > 0,
      cm.grossEdgeUsd != null
        ? `gross $${Number(cm.grossEdgeUsd).toFixed(2)}, friction $${Number(cm.frictionUsd).toFixed(2)}, net $${Number(cm.netEdgeUsd || 0).toFixed(2)}`
        : "realized paper cost meter is unavailable",
      { measured: cm.grossEdgeUsd != null });

  /* THE BLOCKING ONE. The strategy file is explicit that this gate blocks any
     automation of the no-cause book, because the failure mode — fading into a
     real cause the system never saw — cannot be bounded until recall against
     known causes is measured. It has never been run, so it fails, and it is
     correct that it fails. */
  const rb = ev.recallBenchmark || {};
  add("recall_benchmark", "limited_auto",
      rb.source === "external_labeled" && Number(rb.n) >= 100
        && Number(rb.lowerBound) >= 0.8,
      !ev.recallBenchmark
        ? "the known-cause recall benchmark has never been run — the system cannot yet quantify what its sources miss"
        : `externally labelled recall lower bound ${Math.round(Number(rb.lowerBound || 0) * 100)}% on ${Number(rb.n) || 0} events (needs >=80% on >=100)`,
      { measured: !!ev.recallBenchmark, critical: true });

  return g;
}

/** The highest stage whose gates — and every earlier stage's gates — all pass. */
function highestEarnedStage(gates) {
  let earned = "research";
  for (let i = 1; i < STAGES.length; i++) {
    const stage = STAGES[i];
    const required = gates.filter((x) => stageIndex(x.stage) <= i && x.blocking);
    if (required.every((x) => x.pass)) earned = stage; else break;
  }
  return earned;
}

/**
 * Decide what stage the system should be on right now.
 *
 * Promotion is capped by `operatorCeiling` — the operator's standing statement
 * of how far they are willing to let it go. The system may climb to that
 * ceiling on measured evidence and no further, which is what makes this
 * supervision by exception rather than supervision by clicking.
 */
function decideStage({ current, gates, operatorCeiling, operatorHold, killSwitch,
                       promotionStreak = 0, streakNeeded = 3 }) {
  const earned = highestEarnedStage(gates);
  const cur = stageIndex(current);
  const ceil = stageIndex(operatorCeiling || "approval");
  const target = Math.min(stageIndex(earned), ceil);

  if (killSwitch) {
    return { stage: "research", changed: current !== "research", direction: "halt", streak: 0,
             reason: "kill switch engaged — everything drops to research" };
  }

  // Measured failures demote immediately. Missing measurements also demote
  // when the invariant is critical (ledger, clock, source, holdout, recall).
  const failedBelowCur = gates.filter((x) => x.blocking && !x.pass
    && (x.measured !== false || x.critical) && stageIndex(x.stage) <= cur);
  if ((failedBelowCur.length || target < cur) && target < cur) {
    return {
      stage: STAGES[target], changed: true, direction: "down", streak: 0,
      reason: `automatic rollback from ${current} to ${STAGES[target]} — ${failedBelowCur.map((f) => f.detail).join("; ")}`,
    };
  }

  // PROMOTION: one rung at a time, only after the evidence has held steady.
  if (target > cur) {
    if (operatorHold) {
      return { stage: current, changed: false, direction: "held", streak: 0,
               reason: `${STAGES[target]} is earned but the operator has the ladder on hold` };
    }
    const streak = promotionStreak + 1;
    if (streak < streakNeeded) {
      return { stage: current, changed: false, direction: "arming", streak,
               reason: `gates for ${STAGES[cur + 1]} passing (${streak} of ${streakNeeded} consecutive checks) — promoting only when the evidence holds, not the first time it flickers green` };
    }
    const next = STAGES[cur + 1];
    return {
      stage: next, changed: true, direction: "up", streak: 0,
      reason: `every gate for ${next} measured and passing for ${streakNeeded} consecutive checks — promoted one rung from ${current}`,
    };
  }

  return { stage: current, changed: false, direction: "hold", streak: 0,
           reason: `${current} is the correct stage: ` +
             (ceil <= cur ? "at the ceiling the operator set" : "the next rung's gates are not passing yet") };
}

/* ── AUTO-APPROVAL ─────────────────────────────────────────────────────────
 * There was no auto-approve path of any kind. Every buy waited on a click, so
 * the system could sell by itself but never buy by itself — which is not
 * autonomy, it is a very elaborate alarm clock.
 *
 * Auto-approval is allowed only at limited_auto, only within caps that are far
 * tighter than the ordinary portfolio limits, and never for an order the gate
 * stack passed only marginally. Anything outside those bounds still goes to
 * the operator, so the queue holds the exceptions rather than everything.
 */
function autoApproval(order, { stage, book, navUsd, cfg, dayCount }) {
  const pc = (cfg && cfg.portfolioControls) || {};
  const auto = (cfg && cfg.autoApproval) || {};
  const reasons = [];

  if (stage !== "limited_auto") reasons.push(`stage is ${stage}, auto-approval only runs at limited_auto`);
  if (auto.enabled === false) reasons.push("auto-approval is switched off");
  if (![order.universeHash, order.strategyHash, order.variantsHash]
      .every((h) => /^[a-f0-9]{64}$/.test(String(h || "")))) {
    reasons.push("order is not bound to complete frozen policy hashes");
  }
  const mp = order.decisionMarketProvenance || {};
  if (!mp.provider || !/^[a-f0-9]{64}$/.test(String(mp.sourceSha256 || ""))) {
    reasons.push("order lacks immutable market-source provenance");
  }

  const maxUsd = navUsd * ((auto.maxOrderPctOfNav ?? 1.5) / 100);
  const orderUsd = (order.qty || 0) * (order.refPriceUsd || 0);
  if (orderUsd > maxUsd) reasons.push(`$${orderUsd.toFixed(0)} exceeds the $${maxUsd.toFixed(0)} auto-approval size cap`);

  const maxPerDay = auto.maxOrdersPerDay ?? 6;
  if ((dayCount || 0) >= maxPerDay) reasons.push(`${dayCount} orders auto-approved today, cap ${maxPerDay}`);

  if ((book.count || 0) >= (auto.maxOpenForAuto ?? Math.max(1, (pc.maxOpenPositions ?? 12) - 4))) {
    reasons.push(`${book.count} positions open — auto-approval stops short of the full position cap`);
  }

  /* Only clean passes. A candidate that squeaked through on a marginal cost
     ratio is exactly the kind that deserves a human glance. */
  const ratio = order.cost && order.cost.ratio;
  if (ratio != null && ratio < (auto.minCostRatio ?? 1.5)) {
    reasons.push(`cost ratio ${ratio} is below the ${auto.minCostRatio ?? 1.5} required for hands-off approval`);
  }
  if (order.cause === "no_cause_detected_in_covered_sources" && auto.allowNoCause !== true) {
    reasons.push("no cause was found in covered sources — this book is never auto-approved until the recall benchmark exists");
  }
  if (!(Number(order.cost && order.cost.calibratedNetLowerBoundBps) > 0)) {
    reasons.push("calibrated net lower bound is not positive");
  }
  if (!Array.isArray(order.gates) || order.gates.some((g) => g.blocking && !g.pass)) {
    reasons.push("one or more persisted decision gates is missing or failed");
  }

  return {
    approve: reasons.length === 0,
    reasons,
    detail: reasons.length === 0
      ? `auto-approved: $${orderUsd.toFixed(0)}, ${dayCount + 1} of ${maxPerDay} today`
      : reasons.join("; "),
  };
}

/**
 * Auto-approval for the INTERNAL exploratory paper learner. This path deliberately
 * does not require a calibrated lower bound—the observations it creates are
 * what make later calibration possible. It does require the ordinary
 * friction hurdle, complete provenance, every active relaxed gate, and a
 * stored strict counterfactual verdict and an execution-eligible market source.
 * Ledger control independently proves the authoritative paper-only state at
 * proposal, approval, and fill.
 */
function exploratoryAutoApproval(order, { operatingState = null,
  book = {}, navUsd = 0, cfg = {} } = {}) {
  const policy = (cfg && cfg.exploratoryAuto) || {};
  const auto = policy.autoApproval || {};
  const reasons = [];
  if (operatingState !== "exploratory_auto") {
    reasons.push(`operating state is ${operatingState || "unknown"}, not exploratory_auto`);
  }
  if (order.paperLearningOnly !== true) reasons.push("order is not tagged paper-learning-only");
  if (!(order.quality && order.quality.tradable === true)) {
    reasons.push("decision market source is not execution-eligible");
  }
  if (![order.universeHash, order.strategyHash, order.variantsHash]
      .every((h) => /^[a-f0-9]{64}$/.test(String(h || "")))) {
    reasons.push("order is not bound to complete frozen policy hashes");
  }
  const mp = order.decisionMarketProvenance || {};
  if (!mp.provider || mp.adjustment == null
      || !/^[a-f0-9]{64}$/.test(String(mp.sourceSha256 || ""))) {
    reasons.push("order lacks immutable market-source provenance");
  }
  const strict = order.decisionContext && order.decisionContext.strictVerdict;
  if (!strict || typeof strict.pass !== "boolean") {
    reasons.push("strict counterfactual verdict was not persisted");
  }
  if (!Array.isArray(order.gates) || order.gates.some((g) => g.blocking && !g.pass)) {
    reasons.push("one or more persisted paper gates is missing or failed");
  }
  /* The active decision gate already enforces the operator-selected paper
     cost hurdle. Requiring a second stricter ratio here made "Active" mode
     appear enabled while silently refusing every order it was meant to learn
     from. The strict ratio remains stored as the counterfactual. */
  const ratio = Number(order.cost && order.cost.ratio);
  if (!Number.isFinite(ratio) || ratio < 0) reasons.push("paper cost ratio is invalid");
  const orderUsd = Math.max(0, Number(order.qty) || 0) * Math.max(0, Number(order.refPriceUsd) || 0);
  const minimumOrderUsd = Math.max(0.01, Number(auto.minimumOrderUsd) || 1);
  if (!(orderUsd >= minimumOrderUsd)) {
    reasons.push(`$${orderUsd.toFixed(2)} is below the $${minimumOrderUsd.toFixed(2)} minimum executable paper order`);
  }
  if (!(Number(navUsd) > 0)) reasons.push("reconciled paper NAV is unavailable");
  return { approve: reasons.length === 0, reasons,
    detail: reasons.length ? reasons.join("; ")
      : `exploratory paper auto-approved: $${orderUsd.toFixed(0)}; no daily order quota; `
        + `${Number(book.grossPct || 0).toFixed(1)}% invested before this order`,
    cohort: policy.evidenceCohort || "exploratory_auto_unvalidated",
    unlimitedOrdersPerDay: auto.unlimitedOrdersPerDay === true };
}

/* Compatibility name retained for existing fixtures and older callers. */
function paperLearningApproval(order, context = {}) {
  return exploratoryAutoApproval(order, {
    operatingState: context.operatingState || "exploratory_auto", ...context,
  });
}

function describe(stage, gates, decision) {
  const lines = [];
  const idx = stageIndex(stage);
  lines.push(`Currently at "${stage}" — ${
    stage === "research" ? "watching and recording only, no orders are written."
    : stage === "approval" ? "proposing orders that wait for you to approve each one."
    : stage === "shadow" ? "proposing orders for approval while recording what it would have done unattended."
    : "opening small positions by itself within tight caps, and asking you about anything unusual."}`);
  const next = STAGES[idx + 1];
  if (next) {
    const blocking = gates.filter((g) => stageIndex(g.stage) === idx + 1 && g.blocking && !g.pass);
    lines.push(blocking.length
      ? `To reach "${next}" it still needs: ${blocking.map((b) => b.detail).join("; ")}.`
      : `Every requirement for "${next}" is met.`);
  }
  if (decision && decision.changed) lines.push(decision.reason);
  return lines;
}

module.exports = { STAGES, stageIndex, evaluateGates, highestEarnedStage, decideStage,
  autoApproval, exploratoryAutoApproval, paperLearningApproval, describe };
