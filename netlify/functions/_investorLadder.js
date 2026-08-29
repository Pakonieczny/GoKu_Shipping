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
 *    · the operator can veto, freeze, or force a stage at any time, and a
 *      manual setting always wins over an automatic one.
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
  /* `measured: false` marks a gate whose input was UNAVAILABLE this cycle —
     a Firestore blip, a thrown harness — as distinct from measured-and-
     failing. An unmeasured gate blocks promotion (conservative) but never
     triggers demotion: a five-minute outage must not bounce the stage. */
  const add = (id, stage, pass, detail, blocking = true, measured = true) =>
    g.push({ id, stage, pass: !!pass, detail, blocking, measured });

  /* ── to APPROVAL: the accounting has to be trustworthy ─────────────── */
  add("ledger_balanced", "approval", ev.ledgerBalanced !== false,
      ev.ledgerBalanced === false ? "the journal does not balance" : "every journal entry balances");
  add("fixtures", "approval", ev.fixturesPass !== false,
      ev.fixturesPass === false ? "golden fixtures are failing" : "golden fixtures pass on this deploy");
  add("execution_clock", "approval", ev.eligibleFillsOnly !== false,
      "every fill references a post-decision eligible bar");

  /* ── to SHADOW: the machinery has to be stable ─────────────────────── */
  add("cycle_health", "shadow", (ev.cycleErrorRate ?? 0) < 0.05,
      `${Math.round((ev.cycleErrorRate ?? 0) * 100)}% of recent cycles failed (needs under 5%)`);
  add("history_coverage", "shadow", (ev.historyCoverage ?? 0) >= 0.8,
      `${Math.round((ev.historyCoverage ?? 0) * 100)}% of the roster has a six-month record (needs 80%)`);
  add("shadow_flowing", "shadow", (ev.shadowClosed ?? 0) >= 200,
      ev.shadowUnavailable
        ? "shadow harness did not report this cycle — holding rather than judging"
        : `${ev.shadowClosed ?? 0} shadow trades closed (needs 200 before the harness is believable)`,
      true, !ev.shadowUnavailable);

  /* ── to LIMITED_AUTO: the strategy itself has to have shown something ─ */
  const alloc = ev.allocation || {};
  add("powered", "limited_auto", !!alloc.powered,
      alloc.powered
        ? `the allocator has enough evidence to name a leader (${alloc.bestEffectiveN} independent days)`
        : `only ${alloc.bestEffectiveN ?? 0} of ${alloc.requiredEffectiveN ?? "?"} independent days collected`);
  add("real_sample", "limited_auto", (ev.closedRealTrades ?? 0) >= 200,
      `${ev.closedRealTrades ?? 0} real closed positions (needs 200 out of sample)`);
  const cm = ev.costMeter || {};
  add("edge_beats_cost", "limited_auto",
      Number(cm.grossEdgeUsd) > Number(cm.frictionUsd),
      cm.grossEdgeUsd != null
        ? `gross edge $${Number(cm.grossEdgeUsd).toFixed(2)} vs friction $${Number(cm.frictionUsd).toFixed(2)}`
        : "cost meter has no realized trades yet");

  /* THE BLOCKING ONE. The strategy file is explicit that this gate blocks any
     automation of the no-cause book, because the failure mode — fading into a
     real cause the system never saw — cannot be bounded until recall against
     known causes is measured. It has never been run, so it fails, and it is
     correct that it fails. */
  add("recall_benchmark", "limited_auto",
      ev.recallBenchmark != null && Number(ev.recallBenchmark) >= 0.8,
      ev.recallBenchmark == null
        ? "the known-cause recall benchmark has never been run — the system cannot yet quantify what its sources miss"
        : `recall ${Math.round(Number(ev.recallBenchmark) * 100)}% against known causes (needs 80%) — OPERATOR-RECORDED, no automated benchmark exists yet, so this gate is your assertion, not the system's measurement`);

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
                       operatorPinned = false, promotionStreak = 0, streakNeeded = 3 }) {
  const earned = highestEarnedStage(gates);
  const cur = stageIndex(current);
  const ceil = stageIndex(operatorCeiling || "approval");
  const target = Math.min(stageIndex(earned), ceil);

  if (killSwitch) {
    return { stage: "research", changed: current !== "research", direction: "halt", streak: 0,
             reason: "kill switch engaged — everything drops to research" };
  }

  /* DEMOTION: immediate — but only on gates that were actually MEASURED
     failing. A measurement outage blocks promotion; it must not flap the
     stage down and back up every five minutes (that was possible before:
     one Firestore blip zeroed shadowClosed, demoted shadow→approval, and the
     next cycle promoted straight back — a mode write and audit row per flap). */
  const measuredFailedBelowCur = gates.filter(
    (x) => x.blocking && !x.pass && x.measured !== false && stageIndex(x.stage) <= cur);
  if (measuredFailedBelowCur.length && target < cur) {
    return {
      stage: STAGES[target], changed: true, direction: "down", streak: 0,
      reason: `dropped from ${current} to ${STAGES[target]} — ${measuredFailedBelowCur.map((f) => f.detail).join("; ")}`,
    };
  }

  /* THE OPERATOR'S WORD WINS UPWARD. When the operator has manually set the
     stage, the ladder never promotes away from it — before this rule the
     dropdown was decoration: an operator who set "research" to quiet the
     system was re-promoted to "approval" on the very next cycle. Safety still
     wins downward (the demotion branch above runs first). The pin clears when
     the operator raises the ceiling or sets a new stage. */
  if (operatorPinned && target > cur) {
    return { stage: current, changed: false, direction: "pinned", streak: 0,
             reason: `${STAGES[target]} is earned, but you set the stage to ${current} yourself — it stays until you change it or raise the ceiling` };
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

  return {
    approve: reasons.length === 0,
    reasons,
    detail: reasons.length === 0
      ? `auto-approved: $${orderUsd.toFixed(0)}, ${dayCount + 1} of ${maxPerDay} today`
      : reasons.join("; "),
  };
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

module.exports = { STAGES, stageIndex, evaluateGates, highestEarnedStage, decideStage, autoApproval, describe };
