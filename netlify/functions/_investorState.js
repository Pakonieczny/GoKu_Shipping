/* Investor_AI — authoritative operating-state model for the paper desk. */
"use strict";

const STATES = Object.freeze({
  OBSERVATION: "observation",
  MANUAL_APPROVAL: "manual_approval",
  EXPLORATORY_AUTO: "exploratory_auto",
  SHADOW: "shadow",
  LIMITED_AUTO: "limited_auto",
  PAUSED: "paused",
  ENTRY_FROZEN: "entry_frozen",
});

const ACTIVE = Object.freeze([
  STATES.OBSERVATION,
  STATES.MANUAL_APPROVAL,
  STATES.EXPLORATORY_AUTO,
  STATES.SHADOW,
  STATES.LIMITED_AUTO,
]);
const ALL = Object.freeze([...ACTIVE, STATES.PAUSED, STATES.ENTRY_FROZEN]);
const STAGE = Object.freeze({
  [STATES.OBSERVATION]: "research",
  [STATES.MANUAL_APPROVAL]: "approval",
  /* Exploratory auto is parallel to the approval rung: the machinery has to
     be attested, but statistical effectiveness is deliberately not claimed. */
  [STATES.EXPLORATORY_AUTO]: "approval",
  [STATES.SHADOW]: "shadow",
  [STATES.LIMITED_AUTO]: "limited_auto",
});
const FROM_STAGE = Object.freeze({
  research: STATES.OBSERVATION,
  approval: STATES.MANUAL_APPROVAL,
  exploratory_auto: STATES.EXPLORATORY_AUTO,
  shadow: STATES.SHADOW,
  limited_auto: STATES.LIMITED_AUTO,
});
const RANK = Object.freeze({ research: 0, approval: 1, shadow: 2, limited_auto: 3 });

function valid(value) { return ALL.includes(value); }
function active(value) { return ACTIVE.includes(value); }

function inferredActive(ctrl = {}) {
  if (active(ctrl.operatingState)) return ctrl.operatingState;
  const stage = String(ctrl.mode || "research");
  if (ctrl.dryRun !== false || stage === "research") return STATES.OBSERVATION;
  return FROM_STAGE[stage] || STATES.MANUAL_APPROVAL;
}

function stateOf(ctrl = {}) {
  /* Safety overlays win over a stale stored label. This keeps old control
     documents migratable and makes contradictory combinations fail closed. */
  if (ctrl.killSwitch === true || ctrl.enabled === false) return STATES.PAUSED;
  if (ctrl.entriesFrozen === true) return STATES.ENTRY_FROZEN;
  if (valid(ctrl.operatingState)) return ctrl.operatingState;
  return inferredActive(ctrl);
}

function resumeState(ctrl = {}) {
  if (active(ctrl.resumeOperatingState)) return ctrl.resumeOperatingState;
  const inferred = inferredActive(ctrl);
  return active(inferred) ? inferred : STATES.OBSERVATION;
}

function describe(ctrl = {}) {
  const state = stateOf(ctrl);
  const base = active(state) ? state : resumeState(ctrl);
  const stage = STAGE[base] || "research";
  const paused = state === STATES.PAUSED;
  const frozen = state === STATES.ENTRY_FROZEN;
  const observation = base === STATES.OBSERVATION;
  const exploratoryAuto = base === STATES.EXPLORATORY_AUTO;
  const paperLedger = !paused && !frozen && !observation;
  return {
    state,
    baseState: base,
    stage,
    paused,
    entriesFrozen: frozen,
    observationOnly: !paused && !frozen && observation,
    paperLedger,
    entriesAllowed: paperLedger,
    manualApproval: paperLedger && (base === STATES.MANUAL_APPROVAL || base === STATES.SHADOW),
    exploratoryAuto: paperLedger && exploratoryAuto,
    automaticPaperEntries: paperLedger
      && (exploratoryAuto || base === STATES.LIMITED_AUTO),
    guardActive: true,
    label: ({
      [STATES.OBSERVATION]: "Watching only",
      [STATES.MANUAL_APPROVAL]: "Manual paper approval",
      [STATES.EXPLORATORY_AUTO]: "Automatic exploratory paper trading",
      [STATES.SHADOW]: "Shadow evaluation",
      [STATES.LIMITED_AUTO]: "Limited automatic paper trading",
      [STATES.PAUSED]: "Paused — exits still monitored",
      [STATES.ENTRY_FROZEN]: "New entries frozen — exits still monitored",
    })[state],
  };
}

function legacyPatch(target, ctrl = {}) {
  if (target === STATES.PAUSED) {
    const resume = active(stateOf(ctrl)) ? stateOf(ctrl) : resumeState(ctrl);
    return { operatingState: target, resumeOperatingState: resume,
      enabled: false, killSwitch: true, entriesFrozen: false, dryRun: true,
      mode: "research" };
  }
  if (target === STATES.ENTRY_FROZEN) {
    const resume = active(stateOf(ctrl)) ? stateOf(ctrl) : resumeState(ctrl);
    const stage = STAGE[resume] || "research";
    return { operatingState: target, resumeOperatingState: resume,
      enabled: true, killSwitch: false, entriesFrozen: true,
      dryRun: resume === STATES.OBSERVATION, mode: stage };
  }
  const stage = STAGE[target] || "research";
  return { operatingState: target, resumeOperatingState: target,
    enabled: true, killSwitch: false, entriesFrozen: false,
    dryRun: target === STATES.OBSERVATION, mode: stage };
}

function transition(ctrl = {}, target, opts = {}) {
  if (!valid(target)) return { ok: false, reason: `unknown operating state: ${target}` };
  const from = stateOf(ctrl);
  const source = opts.source || "operator";
  if ([STATES.MANUAL_APPROVAL, STATES.EXPLORATORY_AUTO,
      STATES.SHADOW, STATES.LIMITED_AUTO].includes(target)
      && opts.validEpoch !== true) {
    return { ok: false, from, target,
      reason: "paper-ledger operation requires the current safety epoch identity" };
  }
  /* Exploratory auto is explicitly operator-authorized paper learning and is
     therefore selectable without pretending it earned the measured ladder.
     Shadow and limited-auto retain the original promotion-only contract. */
  if ((target === STATES.SHADOW || target === STATES.LIMITED_AUTO) && source !== "ladder") {
    const fromBase = active(from) ? from : resumeState(ctrl);
    const fromRank = RANK[STAGE[fromBase] || "research"] ?? -1;
    const targetRank = RANK[STAGE[target] || "research"] ?? 99;
    if (targetRank >= fromRank) {
      return { ok: false, from, target,
        reason: `${target} can only be entered by the measured promotion ladder` };
    }
  }
  if (source === "ladder") {
    const earned = String(opts.earnedStage || "research");
    const targetStage = STAGE[target] || "research";
    const fromBase = active(from) ? from : resumeState(ctrl);
    const isPromotion = (RANK[targetStage] ?? 99)
      > (RANK[STAGE[fromBase] || "research"] ?? -1);
    if (isPromotion && (RANK[earned] ?? -1) < (RANK[targetStage] ?? 99)) {
      return { ok: false, from, target, reason: `promotion evidence earned ${earned}, not ${targetStage}` };
    }
  }
  if (target !== STATES.PAUSED && ctrl.killSwitch === true && opts.clearPause !== true) {
    return { ok: false, from, target, reason: "the desk is paused" };
  }
  const patch = legacyPatch(target, ctrl);
  patch.operatingStateChangedAtMs = Number(opts.atMs) || Date.now();
  patch.operatingStateReason = String(opts.reason || `${source} transition`).slice(0, 240);
  patch.operatingStateSource = source;
  return { ok: true, from, target, patch, state: describe({ ...ctrl, ...patch }) };
}

function stateForStage(stage) { return FROM_STAGE[stage] || STATES.OBSERVATION; }

module.exports = { STATES, ACTIVE, ALL, STAGE, RANK, valid, active,
  stateOf, resumeState, describe, legacyPatch, transition, stateForStage };
