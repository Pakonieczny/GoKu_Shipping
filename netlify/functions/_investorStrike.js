/*  netlify/functions/_investorStrike.js  (v1.0)
 *  ---------------------------------------------------------------------------
 *  Investor_AI — the strike tier: armed entry levels and fast execution.
 *
 *  WHY THIS FILE EXISTS
 *  ------------------------------------------------------------------------
 *  The desk used to do its whole analysis — roster bars, factor regression,
 *  ranking, the gate stack for every company, evidence, model calls, portfolio
 *  selection, learning — every five minutes, and could act no faster than
 *  that pipeline could run. Most of that work does not change minute to
 *  minute: whether a company is in a downtrend, whether earnings are near,
 *  whether it is liquid enough, what its dossier says. What DOES change
 *  minute to minute is the price. So the work is split:
 *
 *    THE DEEP SCAN (investorCycle-background, a few times a session) decides
 *    WHAT is worth buying and AT WHAT PRICE, and writes that down:
 *      · ordinary proposals for names whose signal already fired;
 *      · ARMED LEVELS for names that pass every hazard gate but have not yet
 *        fallen far enough — the price at which the residual would breach the
 *        entry threshold (market and sector held flat) and the cost hurdle
 *        would clear;
 *      · preset exit levels (stop, trailing arm, target) on every holding.
 *
 *    THE STRIKE PASS (_investorPositionGuard, every minute) prices only the
 *    held, planned and pending names — one provider call — and acts:
 *      · stops, targets and rule exits on holdings (already there);
 *      · a STRIKE when a planned name trades at or below its armed level;
 *      · auto-approval of fresh proposals and the FILL of approved orders on
 *        their first eligible bar, so a buy decided at 10:01 is not waiting
 *        for a 10:05 scan to notice.
 *
 *  WHAT A STRIKE RE-CHECKS AND WHAT IT TRUSTS
 *  ------------------------------------------------------------------------
 *  Trusted from the plan (they do not change intraday): trend, liquidity,
 *  earnings window, dossier verdict, turnover, sector crowding, the decision
 *  input manifest. Re-checked at the strike (they do): session phase and
 *  price quality, the entry control, account breakers, whether the name is
 *  already held or pending, a deterministic cause check against the
 *  documents the desk ALREADY holds (no fetch, no model), the cost hurdle at
 *  the struck price, and the whole portfolio gate stack with a fresh
 *  portfolio manifest. A strike NEVER calls the model and NEVER fetches a
 *  source: if the price has gapped through the strike band the plan is left
 *  for the next deep scan, whose evidence lane exists for exactly that case.
 *
 *  THE EXECUTION CLOCK IS UNCHANGED. A struck order is proposed and approved
 *  at the observation, and fills on the first bar that OPENS after the
 *  decision plus the feed delay plus the execution latency — the same rule
 *  every other order obeys. On a 15-minute-delayed feed the desk still
 *  cannot react to a print it has not yet received; what this tier removes
 *  is the five minutes of scheduling latency stacked on top of that.
 * ---------------------------------------------------------------------------
 */

"use strict";

/* Shared ceiling for the exploratory observation size floor. Repeating the
   literal in three files is how one of them ends up out of step. */
const ST_FLOOR_MAX = require("./_investorStrategy").PAPER_OBSERVATION_FLOOR_MAX;

const A = require("./_investorAdmin");
const M = require("./_investorMarket");
const S = require("./_investorSignal");
const L = require("./_investorLedger");
const R = require("./_investorRisk");
const DM = require("./_investorDecisionManifest");
const E = require("./_investorEvidence");
const LD = require("./_investorLadder");
const H = require("./_investorHistory");
const T = require("./_investorTemporal");
const I = require("./_investorIntelligence");
const W = require("./_investorWorkset");
const PA = require("./_investorPatience");
const { redact } = require("./_investorAuth");

const PLAN_STATUSES = Object.freeze(["armed", "struck", "skipped", "expired", "superseded", "cancelled"]);
const SHA256 = /^[a-f0-9]{64}$/;
const round = (x, d = 4) => Number(Number(x).toFixed(d));

function planId(accountId, sessionDate, symbol) {
  return `${accountId}_${sessionDate}_${symbol}`;
}

/* ── PURE: the armed level ─────────────────────────────────────────────────
 * residualZ() scores the sum of the last `window` residual returns against a
 * blended sigma: z = cum / sd. The entry rule fires at z <= -minAbsZ, i.e. at
 * cum <= -minAbsZ * sd. If the market and sector factors stay where they are,
 * the company's own price has to move by (target - cum) for the residual to
 * get there, so the level is lastPrice * (1 + (target - cum)). The
 * linearisation and the flat-factor assumption are recorded on the plan; the
 * strike re-checks the cost hurdle at the actual struck price, and the whole
 * plan is re-derived by the next deep scan. */
function armLevel({ lastPrice, cumResidual, residVol, minAbsZ, maxArmDropPct = 5 }) {
  const px = Number(lastPrice), cum = Number(cumResidual), sd = Number(residVol), z = Number(minAbsZ);
  if (!(px > 0) || !Number.isFinite(cum) || !(sd > 0) || !(z > 0)) {
    return { ok: false, reason: "inputs_incomplete" };
  }
  const targetCumResidual = -z * sd;
  const requiredResidual = targetCumResidual - cum;
  if (!(requiredResidual < 0)) return { ok: false, reason: "already_breached", requiredResidual };
  const dropPct = -requiredResidual * 100;
  if (dropPct > Number(maxArmDropPct)) return { ok: false, reason: "too_far", dropPct: round(dropPct, 2) };
  return { ok: true,
    armBelowUsd: round(px * (1 + requiredResidual)),
    dropPct: round(dropPct, 2),
    requiredResidual: round(requiredResidual, 6),
    targetCumResidual: round(targetCumResidual, 6),
    assumption: "market and sector factors flat between the plan and the strike" };
}

/* ── PURE: is this observation a strike? ─────────────────────────────────── */
function strikeVerdict(price, plan) {
  const px = Number(price), arm = Number(plan && plan.armBelowUsd), floor = Number(plan && plan.floorUsd);
  if (!(px > 0) || !(arm > 0)) return { strike: false, reason: "unpriced", distancePct: null };
  const distancePct = round(((px - arm) / arm) * 100, 3);
  if (px > arm) return { strike: false, reason: "above_level", distancePct };
  if (floor > 0 && px < floor) return { strike: false, reason: "gap_below_band", distancePct, gap: true };
  return { strike: true, reason: "at_or_below_level", distancePct };
}

/* ── PURE: choose which plans to arm ─────────────────────────────────────── */
function selectPlans(candidates, policy, { exclude = new Set() } = {}) {
  const max = Math.max(0, Math.round(Number(policy && policy.maxArmedPlans) || 0));
  const usable = (candidates || []).filter((c) => c && c.ok && c.symbol && !exclude.has(c.symbol));
  usable.sort((a, b) => a.dropPct - b.dropPct || a.symbol.localeCompare(b.symbol));
  return { selected: usable.slice(0, max), passedOver: usable.slice(max).map((c) => c.symbol) };
}

/* ── the deep scan's side: build a plan candidate from a decision ────────── */
/* Called once per evaluated company. Returns { ok:false, reason } for names
   that are not near-miss candidates, or a complete plan document (minus
   status/timestamps) for one that is. Everything it reads is this scan's own
   evidence about the company; nothing is fetched. */
/* ── BUY IT OR DO NOT BUY IT ───────────────────────────────────────────────
 * The armed-level tier existed so a company that passed every hazard gate but
 * had not yet fallen to the signal threshold could be bought LATER, at the
 * price where the signal would have fired. In practice that parked qualified
 * companies in a waiting state over a fraction of a percent — the operator
 * watched levels arm, get superseded by the next scan and retire, while the
 * book bought nothing. There is now one entry path: a company either qualifies
 * at the price on the screen and is bought, or it does not qualify.
 *
 * This forgives EXACTLY ONE THING relative to an ordinary entry: |z| may be
 * short of minAbsZ, and only down to an explicit floor. Every hazard gate must
 * still pass, the rank must already qualify, and — unlike the armed level,
 * which tested the cost hurdle at a price the market had not reached — the
 * cost hurdle must clear at the price actually being paid. PURE.
 */
function nearMissEntryEligible({ evalRes, z, rank, cfg, policy, quality,
  decisionManifest, marketProvenance }) {
  if (!policy || policy.enabled !== true) return { ok: false, reason: "immediate_entry_disabled" };
  if (!evalRes) return { ok: false, reason: "no_evaluation" };
  /* A full pass belongs to the ordinary path, which has already queued it. */
  if (evalRes.pass) return { ok: false, reason: "already_passes" };
  const blocked = evalRes.blockedBy || [];
  /* ONLY the signal may be short. The cost gate is never forgiven here: a
     trade that cannot clear its own frictions at today's price has no edge
     left to buy, which is precisely why the armed level tested cost at the
     level rather than at the market. */
  if (blocked.length !== 1 || blocked[0] !== "signal") {
    return { ok: false, reason: `blocked_by_${blocked.filter((g) => g !== "signal")[0] || "nothing"}` };
  }
  if (!z || !Number.isFinite(Number(z.z)) || !Number.isFinite(Number(rank))) {
    return { ok: false, reason: "no_signal_statistic" };
  }
  const minAbsZ = Number(cfg.minAbsZ ?? 2);
  /* Short on z, not on rank. A name outside the qualifying part of the
     cross-section is not a near miss, it is a different trade. */
  if (Number(z.z) <= -minAbsZ) return { ok: false, reason: "signal_failed_on_rank_not_z" };
  const floor = Number(policy.minAbsZFloor);
  if (!Number.isFinite(floor) || !(Number(z.z) <= -floor)) {
    return { ok: false, reason: `z_${Number(z.z).toFixed(2)}_short_of_floor_${floor}` };
  }
  if (Number(rank) > Number(policy.rankCeiling)) return { ok: false, reason: "rank_above_entry_ceiling" };
  if (!quality || quality.tradable !== true) return { ok: false, reason: "execution_source_not_tradable" };
  if (!decisionManifest || !SHA256.test(String(decisionManifest.manifestHash || ""))) {
    return { ok: false, reason: "no_decision_manifest" };
  }
  if (!marketProvenance || !SHA256.test(String(marketProvenance.sourceSha256 || ""))) {
    return { ok: false, reason: "no_market_provenance" };
  }
  return { ok: true, reason: "near_miss_bought_at_market",
    zShortfall: Number((Number(z.z) + minAbsZ).toFixed(3)) };
}

function planCandidate({ symbol, evalRes, strictRes, strictUncalibratedRes, z, rank, last, cfg,
  policy, quality, advUsd, sector, historyContext, reversion, dataSufficiency, intelligence,
  cause, coverage, decisionManifest, marketProvenance, sessionCloseMs, vixNorm, session,
  policyIdentity, variantId, exploratoryPolicyVersion, cohortLabel, paperLearningOnly,
  activePortfolioControls, activity, cycleId, strategyVersion, operatingState, moveStartedAtMs,
  positionScale }) {
  if (!policy || policy.enabled === false || !(policy.maxArmedPlans > 0)) return { ok: false, reason: "strike_disabled" };
  if (!evalRes || evalRes.pass) return { ok: false, reason: "not_a_near_miss" };
  if (!z || !Number.isFinite(Number(z.z)) || !Number.isFinite(Number(rank))) return { ok: false, reason: "no_signal_statistic" };
  const blocked = evalRes.blockedBy || [];
  /* Only the signal and the (signal-dependent) cost gate may be failing.
     Anything else is a hazard finding or an absence the deep scan refused
     to trade through, and a price move does not cure it. */
  if (!blocked.length || blocked.some((g) => g !== "signal" && g !== "cost")) {
    return { ok: false, reason: `blocked_by_${blocked.filter((g) => g !== "signal" && g !== "cost")[0] || "nothing"}` };
  }
  const minAbsZ = Number(cfg.minAbsZ ?? 2);
  if (Number(z.z) <= -minAbsZ) return { ok: false, reason: "signal_failed_on_rank_not_z" };
  if (Number(rank) > Number(policy.planRankCeiling)) return { ok: false, reason: "rank_above_plan_ceiling" };
  if (!quality || quality.tradable !== true) return { ok: false, reason: "execution_source_not_tradable" };
  if (!decisionManifest || !SHA256.test(String(decisionManifest.manifestHash || ""))) return { ok: false, reason: "no_decision_manifest" };
  if (!marketProvenance || !SHA256.test(String(marketProvenance.sourceSha256 || ""))) return { ok: false, reason: "no_market_provenance" };
  const level = armLevel({ lastPrice: last.c, cumResidual: z.cumResidual, residVol: z.residVol,
    minAbsZ, maxArmDropPct: policy.maxArmDropPct });
  if (!level.ok) return { ok: false, reason: level.reason, dropPct: level.dropPct };
  /* The cost hurdle at the level itself. A name that could not clear its
     own frictions even after falling to the threshold is not worth arming. */
  const costAtLevel = S.costHurdle({ cumResidual: level.targetCumResidual, advUsd,
    grade: quality.grade, wideSpreadWindow: false, vixNorm, cfg,
    reversionMult: reversion ? reversion.multiplier : 1 });
  if (!costAtLevel.pass) return { ok: false, reason: "cost_fails_at_level", costAtLevel };
  const floorUsd = round(level.armBelowUsd * (1 - Number(policy.strikeBandPct) / 100));
  const closeMs = Number.isFinite(Number(sessionCloseMs)) ? Number(sessionCloseMs) : NaN;
  const sizing = evalRes.sizing || {};
  const hc = historyContext || {};
  return {
    ok: true, symbol, dropPct: level.dropPct,
    plan: {
      accountId: policyIdentity.accountId, symbol, sector: sector || S.sectorOf(symbol),
      sessionDate: session.date, cycleId, decisionId: `${cycleId}_${symbol}`,
      decisionManifestHash: decisionManifest.manifestHash,
      decisionManifestVersion: decisionManifest.version,
      armBelowUsd: level.armBelowUsd, floorUsd, strikeBandPct: Number(policy.strikeBandPct),
      requiredDropPct: level.dropPct, requiredResidual: level.requiredResidual,
      targetCumResidual: level.targetCumResidual, levelAssumption: level.assumption,
      refPriceUsd: Number(last.c), refBarAt: last.t,
      planMarketProvenance: marketProvenance,
      zAtPlan: round(z.z, 3), rankAtPlan: round(rank, 4), cumResidualAtPlan: round(z.cumResidual, 6),
      residVol: round(z.residVol, 6), minAbsZ, signalWindow: z.window || cfg.signalWindow || 12,
      moveStartedAtMs: Number.isFinite(Number(moveStartedAtMs)) ? Number(moveStartedAtMs) : null,
      /* Expire before the closing auction: nothing opens in it anyway. */
      expiresAtMs: Number.isFinite(closeMs) ? closeMs - 15 * 60000 : null,
      gatesAtPlan: evalRes.gates || [], blockedByAtPlan: blocked,
      costAtLevel: { ratio: costAtLevel.ratio, expectedGrossBps: costAtLevel.expectedGrossBps,
        requiredBps: costAtLevel.requiredBps, roundTripBps: costAtLevel.roundTripBps,
        halfTripBps: costAtLevel.halfTripBps },
      sizingParts: { volScaler: sizing.volScaler, volNote: sizing.volNote || null,
        dispersionMult: sizing.dispersionMult, causeConfidence: sizing.causeConfidence,
        intelligenceMult: sizing.intelligenceMult, rule: cfg.sizeAggregation || null,
        combinedAtPlan: sizing.combined },
      sufficiencyMultiplier: dataSufficiency ? Number(dataSufficiency.sizeMultiplier) || 1 : 1,
      dataSufficiency: dataSufficiency ? { score: dataSufficiency.score, bucket: dataSufficiency.bucket,
        missing: dataSufficiency.missing, sizeMultiplier: dataSufficiency.sizeMultiplier,
        orderingPenaltyBps: dataSufficiency.orderingPenaltyBps,
        version: dataSufficiency.version, kind: dataSufficiency.kind } : null,
      historyContext: { atrPct: hc.atrPct ?? null, expectedShortfall5dPct: hc.expectedShortfall5dPct ?? null,
        overnightGapEsPct: hc.overnightGapEsPct ?? null, days: hc.days ?? null },
      reversionMultiplier: reversion ? reversion.multiplier : 1,
      /* The whole record, not just the multiplier: the patience grant needs
         the event count, win rate and own-weight at strike time. */
      reversionRecord: reversion || null,
      advUsd: Number(advUsd) || 0,
      qualityAtPlan: quality,
      causeAtPlan: cause || null, coverageAtPlan: coverage || null,
      intelligenceDossierHash: intelligence && intelligence.dossierHash || null,
      intelligencePolicyAtPlan: evalRes.intelligencePolicy ? {
        adverseRiskScore: evalRes.intelligencePolicy.adverseRiskScore,
        sizeMultiplier: evalRes.intelligencePolicy.sizeMultiplier,
        fresh: evalRes.intelligencePolicy.fresh, complete: evalRes.intelligencePolicy.complete } : null,
      strictVerdict: { pass: strictRes ? strictRes.pass === true : false,
        blockedBy: strictRes ? strictRes.blockedBy : ["strict_verdict_missing"],
        firstBlock: strictRes ? strictRes.firstBlock : "strict verdict missing" },
      strictUncalibratedVerdict: strictUncalibratedRes ? { pass: strictUncalibratedRes.pass === true,
        blockedBy: strictUncalibratedRes.blockedBy || [], firstBlock: strictUncalibratedRes.firstBlock || null } : null,
      policyIdentity: { ...policyIdentity, variantId: variantId || "A" },
      variantId: variantId || "A",
      exploratoryPolicyVersion: exploratoryPolicyVersion || null,
      learningCohort: cohortLabel, paperLearningOnly: paperLearningOnly === true,
      operatingStateAtPlan: operatingState, strategyVersion,
      activeConfig: cfg, portfolioControls: activePortfolioControls,
      activity: { reservationHeadroomBps: activity.reservationHeadroomBps,
        minimumShareFloor: activity.minimumShareFloor,
        signalLifetimeMinutes: activity.signalLifetimeMinutes || null,
        strike: policy },
      positionScale: Number(positionScale) || 1,
    },
  };
}

/* ── write / refresh today's plans ───────────────────────────────────────── */
async function writePlans({ accountId, sessionDate, cycleId, candidates, policy, heldSymbols = [], pendingSymbols = [] }) {
  const exclude = new Set([...heldSymbols, ...pendingSymbols]);
  const existingSnap = await A.col(A.COL.plans)
    .where("accountId", "==", accountId).where("sessionDate", "==", sessionDate).get();
  const existing = {};
  existingSnap.forEach((d) => { existing[d.data().symbol] = d.data(); });
  /* A symbol already struck (or cancelled by the operator) today is not
     re-armed by a later scan: one strike per name per session. */
  for (const [sym, p] of Object.entries(existing)) {
    if (["struck", "cancelled"].includes(p.status)) exclude.add(sym);
  }
  const { selected, passedOver } = selectPlans(candidates, policy, { exclude });
  const now = Date.now();
  const armed = [], refused = [];
  /* ONE TRANSACTION PER PLAN, not a batch of merge:false writes.
     The read above and the write below are separated by two Firestore round
     trips, and the one-minute strike pass is writing these same documents
     throughout. A blind merge:false could overwrite a plan the guard had just
     marked "struck" back to "armed", erasing struckAtMs, orderId and the
     approval record — the order itself survives (exclusion comes from the
     live order and position collections, not from plan.status), but the audit
     trail would then contradict the ledger. Each write re-reads inside a
     transaction and refuses to re-arm a plan that has since been struck or
     cancelled. */
  for (const c of selected) {
    const ref = A.col(A.COL.plans).doc(planId(accountId, sessionDate, c.symbol));
    try {
      const outcome = await A.runTransaction(async (tx) => {
        const snap = await tx.get(ref);
        const prev = snap.exists ? snap.data() : null;
        if (prev && ["struck", "cancelled"].includes(prev.status)) {
          return { applied: false, reason: prev.status };
        }
        tx.set(ref, { ...c.plan, planId: ref.id, status: "armed",
          armedAtMs: prev && prev.status === "armed" && prev.armedAtMs ? prev.armedAtMs : now,
          refreshedAtMs: now, refreshedByCycleId: cycleId,
          refreshCount: prev ? (Number(prev.refreshCount) || 0) + 1 : 0,
          lastSeenUsd: prev && prev.lastSeenUsd != null ? prev.lastSeenUsd : c.plan.refPriceUsd,
          lastSeenAt: (prev && prev.lastSeenAt) || c.plan.refBarAt,
          lastBlock: null,
          updated_at: A.FV.serverTimestamp(),
          ...(prev ? {} : A.envelope({ created_by: "investorCycle.plans" })) }, { merge: false });
        return { applied: true, refreshed: !!prev };
      });
      if (outcome.applied) {
        armed.push({ symbol: c.symbol, armBelowUsd: c.plan.armBelowUsd, refPriceUsd: c.plan.refPriceUsd,
          dropPct: c.dropPct, refreshed: !!outcome.refreshed });
      } else {
        refused.push({ symbol: c.symbol, reason: outcome.reason });
      }
    } catch (e) {
      refused.push({ symbol: c.symbol, reason: String(e.message || e).slice(0, 120) });
    }
  }
  /* An armed plan this scan did not re-select is superseded: the name no
     longer qualifies, is held or pending, or sits behind closer levels. */
  const superseded = [];
  const keep = new Set(selected.map((c) => c.symbol));
  for (const [sym, p] of Object.entries(existing)) {
    if (p.status !== "armed" || keep.has(sym)) continue;
    /* Same compare-and-set: a plan struck since the read must stay struck. */
    const applied = await markPlan({ planId: p.planId || planId(accountId, sessionDate, sym) },
      { status: "superseded", supersededAtMs: now, supersededByCycleId: cycleId });
    if (applied.applied) superseded.push(sym);
  }
  return { armed, superseded, passedOver, refused,
    considered: (candidates || []).length,
    rejected: (candidates || []).filter((c) => c && !c.ok).reduce((acc, c) => {
      acc[c.reason || "unknown"] = (acc[c.reason || "unknown"] || 0) + 1; return acc; }, {}) };
}

async function loadPlans(accountId, sessionDate, { statuses = ["armed"] } = {}) {
  const snap = await A.col(A.COL.plans)
    .where("accountId", "==", accountId).where("sessionDate", "==", sessionDate).get();
  const out = [];
  snap.forEach((d) => { const p = d.data(); if (!statuses || statuses.includes(p.status)) out.push(p); });
  return out.sort((a, b) => (a.requiredDropPct || 0) - (b.requiredDropPct || 0));
}

/* Benign field updates (last seen price, a block note) merge freely. A STATUS
   transition is compare-and-set: only a plan still "armed" may move, so two
   writers cannot both claim the same plan and the later one cannot undo the
   earlier. Returns whether the patch was applied. */
function planPatchChangesStatus(patch) {
  return !!patch && patch.status !== undefined;
}

async function markPlan(plan, patch, { expect = "armed" } = {}) {
  const ref = A.col(A.COL.plans).doc(plan.planId);
  const body = { ...patch, updated_at: A.FV.serverTimestamp() };
  if (!planPatchChangesStatus(patch)) {
    await ref.set(body, { merge: true });
    return { applied: true };
  }
  try {
    return await A.runTransaction(async (tx) => {
      const snap = await tx.get(ref);
      if (!snap.exists) return { applied: false, reason: "plan_missing" };
      const current = snap.data().status;
      if (expect && current !== expect) return { applied: false, reason: current };
      tx.set(ref, body, { merge: true });
      return { applied: true, from: current };
    });
  } catch (e) {
    return { applied: false, reason: String(e.message || e).slice(0, 120) };
  }
}

function trustedSource(provenance, quality) {
  if (!provenance || !provenance.provider || !SHA256.test(String(provenance.sourceSha256 || ""))) {
    return { pass: false, reason: "missing_market_provenance" };
  }
  if (provenance.adjustment == null) return { pass: false, reason: "missing_adjustment_identity" };
  if (!quality || quality.tradable !== true) return { pass: false, reason: "decision_source_not_tradable" };
  return { pass: true };
}

/* ── the marked book at strike time ──────────────────────────────────────── */
async function bookNow({ accountId, positions, marks, pendingOrders }) {
  const balances = await L.balances(accountId);
  const cashUsd = balances.usd[L.ACCT.CASH] || 0;
  const reservedUsd = balances.usd[L.ACCT.RESERVED] || 0;
  const marked = R.markedBook(positions, marks, S.sectorOf, { cash: cashUsd, reserved: reservedUsd });
  const navUsd = Math.max(1, marked.navUsd);
  const fold = R.foldPendingOrders(marked.book, pendingOrders, navUsd, S.sectorOf);
  return { book: marked.book, navUsd, cashUsd, reservedUsd,
    untrustedMarks: marked.untrustedMarks,
    proposalCashUsd: Math.max(0, cashUsd - fold.proposedUnreservedUsd),
    pendingExposure: { orders: fold.orders, usd: fold.usd, symbols: fold.symbols } };
}

/* ── STRIKES ─────────────────────────────────────────────────────────────── */
/**
 * Evaluate every armed plan against this pass's prices and strike the ones
 * whose level has been reached. Returns a summary; every outcome is also
 * written to the plan document so the console can show what happened.
 */
async function evaluateStrikes({ jobId, ctrl, accountId, operating, strategy, cfg, activity,
  activePortfolioControls, session, plans, panel, provenance, quality, positions, pendingOrders,
  earnings = {}, vixNorm = 1, paperLearningActive = false, exploratoryPolicy = {}, policyIdentity }) {
  const out = { considered: plans.length, struck: [], skipped: [], waiting: [], blocked: [], expired: [] };
  if (!plans.length) return out;
  const nowMs = Date.now();
  const heldSymbols = new Set(positions.filter((p) => p && p.open).map((p) => p.symbol));
  const pendingSymbols = new Set(pendingOrders.map((o) => o.symbol));
  const strikePolicy = activity.strike || {};
  let bookState = null, correlationCache = null;

  for (const plan of plans) {
    const sym = plan.symbol;
    try {
      if (Number.isFinite(Number(plan.expiresAtMs)) && nowMs > Number(plan.expiresAtMs)) {
        await markPlan(plan, { status: "expired", expiredAtMs: nowMs, expiryReason: "session_window_closed" });
        out.expired.push(sym); continue;
      }
      const bars = panel[sym] || [];
      const last = bars[bars.length - 1];
      if (!last) { out.waiting.push({ symbol: sym, reason: "no_bars" }); continue; }
      const verdict = strikeVerdict(last.c, plan);
      const seen = { lastSeenUsd: Number(last.c), lastSeenAt: last.t, distancePct: verdict.distancePct,
        lastCheckedAtMs: nowMs, lastCheckJobId: jobId };
      if (!verdict.strike) {
        if (verdict.gap) {
          /* Through the band: a fall that size is more likely news than
             noise. Leave it to the deep scan's evidence lane. */
          await markPlan(plan, { ...seen, status: "skipped", skippedAtMs: nowMs,
            skipReason: "gap_below_band",
            skipDetail: `price ${Number(last.c).toFixed(2)} is below the strike floor ${Number(plan.floorUsd).toFixed(2)} — left for the next deep scan` });
          out.skipped.push({ symbol: sym, reason: "gap_below_band", price: last.c });
        } else {
          await markPlan(plan, seen);
          out.waiting.push({ symbol: sym, reason: verdict.reason, distancePct: verdict.distancePct, price: last.c });
        }
        continue;
      }

      /* ── the level is reached: re-check what changes intraday ── */
      const hold = async (reason, detail = null) => {
        await markPlan(plan, { ...seen, lastBlock: { reason, detail, atMs: nowMs } });
        out.blocked.push({ symbol: sym, reason, detail, price: last.c });
      };
      const skip = async (reason, detail = null) => {
        await markPlan(plan, { ...seen, status: "skipped", skippedAtMs: nowMs, skipReason: reason, skipDetail: detail });
        out.skipped.push({ symbol: sym, reason, detail, price: last.c });
      };
      if (!session.open || session.wideSpreadWindow) { await hold("session", `market ${session.phase}`); continue; }
      const source = trustedSource(provenance[sym], quality[sym]);
      if (!source.pass) { await hold(source.reason); continue; }
      if (heldSymbols.has(sym) || pendingSymbols.has(sym)) { await skip("already_committed", "held or an order is already in flight"); continue; }
      if (!operating.paperLedger) { await hold("observation_only", "the paper ledger is not enabled in this operating state"); continue; }
      const entryControl = L.controlAllowsEntry(ctrl, policyIdentity);
      if (!entryControl.pass) { await hold("entry_control", entryControl.reason); continue; }

      /* Book, NAV and breakers — once per pass, refreshed after each strike. */
      if (!bookState) {
        const marks = {};
        for (const p of positions) {
          const b = panel[p.symbol]; const l = b && b[b.length - 1];
          const q = quality[p.symbol];
          if (l && q && (q.tradable === true || q.researchEligible === true)) marks[p.symbol] = l.c;
          else if (Number(p.lastMarkUsd) > 0) marks[p.symbol] = Number(p.lastMarkUsd);
        }
        bookState = await bookNow({ accountId, positions, marks, pendingOrders });
      }
      const hwm = Math.max(Number(ctrl.highWaterMarkUsd) || 0, bookState.navUsd);
      const sod = ctrl.startOfDayNavDate === session.date && Number(ctrl.startOfDayNavUsd) > 0
        ? Number(ctrl.startOfDayNavUsd) : bookState.navUsd;
      const breakers = R.accountBreakers({ navUsd: bookState.navUsd, hwmUsd: hwm,
        untrustedOpenMarks: bookState.untrustedMarks,
        drawdownPct: hwm > 0 ? ((bookState.navUsd - hwm) / hwm) * 100 : 0,
        dayPnlPct: sod > 0 ? ((bookState.navUsd - sod) / sod) * 100 : 0,
      }, { portfolioControls: activePortfolioControls });
      if (breakers.halted) { await hold("account_halted", breakers.breakers.map((b) => b.reason).join("; ")); continue; }

      /* Earnings can become known between the plan and the strike. */
      const ew = earnings[sym];
      const bo = S.earningsBlackout(sym, (ew && ew.dates) || [], nowMs, cfg,
        { estimated: !!(ew && ew.estimated), uncertaintyDays: Number(ew && ew.uncertaintyDays) || null });
      if (bo.blocked && !(bo.unknown && cfg.paperAbstainOnMissingInfo === true)) { await skip("earnings_blackout", bo.reason); continue; }

      /* Deterministic cause check against what the desk already knows. No
         fetch, no model: the deep scan owns those. A fundamental filing in
         the window is a FINDING and is never traded through. */
      let cause = plan.causeAtPlan || S.CAUSE.PENDING, causeDetail = null;
      let intelligenceSnapshot = null, intelPolicy = null;
      try { intelligenceSnapshot = await I.readSnapshot(sym); } catch {}
      if (intelligenceSnapshot) {
        intelPolicy = I.decisionPolicy({ coverage: intelligenceSnapshot.coverage, events: intelligenceSnapshot.events,
          temporalContext: intelligenceSnapshot.temporalContext, requireTemporalContext: false, asOfMs: nowMs,
          maxAgeHours: cfg.intelligenceMaxAgeHours, temporalMaxAgeHours: cfg.temporalMaxAgeHours });
        if (intelPolicy.criticalExit === true || intelPolicy.hardBlock === true) {
          await skip("intelligence_finding", (intelPolicy.reasons || []).join("; ")); continue;
        }
      }
      try {
        const moveStart = Number(plan.moveStartedAtMs) || (nowMs - (Number(plan.signalWindow) || 12) * 5 * 60000);
        const documents = await E.documentsForMove(sym, moveStart, nowMs);
        const att = S.attentionZ(bars, Number(plan.signalWindow) || 12);
        const pre = E.preClassify({ symbol: sym, freshDocs: documents,
          residualZ: Number(plan.targetCumResidual) / Math.max(1e-9, Number(plan.residVol) || 1),
          attentionScore: att, hoursSinceMove: (Number(plan.signalWindow) || 12) * 5 / 60,
          moveStartedAtMs: moveStart, decisionAtMs: nowMs,
          coverageComplete: !!(intelPolicy && intelPolicy.fresh && intelPolicy.complete) });
        cause = pre.cause; causeDetail = { ...pre, mode: "stored_documents_only", attentionScore: att };
      } catch (e) { causeDetail = { error: String(e.message || e).slice(0, 120), mode: "stored_documents_only" }; }
      const coverage = intelligenceSnapshot && intelligenceSnapshot.coverage || plan.coverageAtPlan || null;
      const dir = S.directionFromCause(cause, cfg, causeDetail && causeDetail.attentionScore, coverage);
      if (!dir.trade) { await skip("evidence", dir.reason); continue; }

      /* Cost at the struck price: the residual is at least the level's. */
      const extraFall = Math.min(0, (Number(last.c) - Number(plan.armBelowUsd)) / Number(plan.armBelowUsd));
      const cost = S.costHurdle({ cumResidual: Number(plan.targetCumResidual) + extraFall,
        advUsd: Number(plan.advUsd) || 0, grade: quality[sym].grade,
        wideSpreadWindow: session.wideSpreadWindow, vixNorm, cfg,
        reversionMult: Number(plan.reversionMultiplier) || 1 });
      if (!cost.pass) { await hold("cost", `expect ${cost.expectedGrossBps}bp vs ${cost.requiredBps}bp required`); continue; }

      /* Size with the plan's independent haircuts and the strike's cause. */
      const parts = plan.sizingParts || {};
      const rawSizing = S.combineSizeMultipliers({ volScaler: parts.volScaler, volNote: parts.volNote,
        dispersionMult: parts.dispersionMult, causeConfidence: dir.confidence,
        intelligenceMult: intelPolicy ? Math.max(Number(intelPolicy.sizeMultiplier) || 0,
          Number(plan.intelligencePolicyAtPlan && plan.intelligencePolicyAtPlan.sizeMultiplier) || 0) : parts.intelligenceMult,
        rule: parts.rule });
      const paperFloor = Math.max(0, Math.min(ST_FLOOR_MAX, Number(cfg.paperObservationSizeFloor) || 0));
      const combined = cfg.paperAbstainOnMissingInfo === true && paperFloor > 0 && rawSizing.combined < paperFloor
        ? paperFloor : rawSizing.combined;
      const positionScale = operating.exploratoryAuto ? Math.max(1, Math.min(5, Number(plan.positionScale) || 1)) : 1;
      const signalScaler = Math.min(1, combined * (Number(plan.sufficiencyMultiplier) || 1) * positionScale);
      const hc = plan.historyContext || {};
      const riskCfg = { ...strategy, portfolioControls: activePortfolioControls, parameters: cfg };
      const sizing = R.positionSizeUsd({ navUsd: bookState.navUsd, atrPct: hc.atrPct,
        expectedShortfall5dPct: hc.expectedShortfall5dPct, overnightGapEsPct: hc.overnightGapEsPct,
        signalScaler, cfg: riskCfg });

      /* Point-in-time correlations against the held book, read once. */
      if (!correlationCache) {
        correlationCache = {};
        const need = [...new Set([sym, ...bookState.book.rows.map((r) => r.symbol)])];
        await Promise.all(need.map(async (s) => {
          try { correlationCache[s] = await H.readDailyWithMeta(s); } catch { correlationCache[s] = null; }
        }));
      } else if (!correlationCache[sym]) {
        try { correlationCache[sym] = await H.readDailyWithMeta(sym); } catch { correlationCache[sym] = null; }
      }
      const dynamicCorrelations = {};
      for (const held of bookState.book.rows) {
        const a = correlationCache[sym], b = correlationCache[held.symbol];
        dynamicCorrelations[held.symbol] = T.pairwiseCorrelation(a ? a.series : [], b ? b.series : [], nowMs, {
          provenanceA: a ? a.provenance : null, provenanceB: b ? b.provenance : null, requireProvenance: true });
      }
      const add = R.checkAdd({ symbol: sym, sector: S.sectorOf(sym), proposedUsd: sizing.usd,
        book: bookState.book, navUsd: bookState.navUsd, cashUsd: bookState.proposalCashUsd,
        cfg: riskCfg, dynamicCorrelations });
      const decisionAtMs = Date.now();
      const decisionId = `${plan.decisionId}_strike`;
      const portfolioManifest = DM.buildPortfolio({ decisionId: `${decisionId}_portfolio`, decisionAtMs, symbol: sym,
        parentManifestHash: plan.decisionManifestHash, sizing, portfolioCheck: add,
        book: bookState.book, navUsd: bookState.navUsd, cashUsd: bookState.proposalCashUsd,
        correlations: dynamicCorrelations, policyIdentity: plan.policyIdentity,
        config: activePortfolioControls,
        buildCommit: process.env.COMMIT_REF || process.env.DEPLOY_ID || "local" });
      const portfolioValidation = DM.validate(portfolioManifest);
      const permittedUsd = add.allow ? sizing.usd : (add.allowTrimmed ? add.permittedUsd : 0);
      let qty = portfolioValidation.pass ? Math.max(0, Math.floor(permittedUsd / last.c)) : 0;
      let minimumShareFloorApplied = false;
      const ordinaryCapUsd = bookState.navUsd * (Number(activePortfolioControls.ordinaryPositionPctOfNav) || 3) / 100;
      if (qty <= 0 && portfolioValidation.pass && operating.exploratoryAuto && activity.minimumShareFloor >= 1
          && permittedUsd > 0 && last.c > 0 && last.c <= ordinaryCapUsd && last.c <= bookState.proposalCashUsd) {
        qty = 1; minimumShareFloorApplied = true;
      }
      if (qty <= 0) {
        if ((add.blockedBy || []).includes("duplicate")) { await skip("portfolio_duplicate", add.firstBlock); continue; }
        await hold("portfolio", add.firstBlock || (permittedUsd > 0 ? "zero whole shares" : "no feasible risk-sized notional"));
        continue;
      }

      const executionCostContext = M.executionCostContext({ advUsd: Number(plan.advUsd) || 0,
        grade: quality[sym].grade, wideSpreadWindow: session.wideSpreadWindow, vixNorm, measuredAtMs: decisionAtMs });
      const signalLifetimeMs = Math.max(15 * 60e3, Math.min(6.5 * 3600e3,
        (Number(cfg.signalLifetimeMinutes) || Number(plan.activity && plan.activity.signalLifetimeMinutes) || 120) * 60e3));
      const closeMs = M.sessionCloseMs ? M.sessionCloseMs(new Date()) : null;
      /* The persisted gates: hazard gates as the plan recorded them, the
         signal gate satisfied by the strike, evidence and cost as re-checked
         now. Auto-approval requires every blocking gate to pass. */
      const gates = (plan.gatesAtPlan || []).map((g) => {
        if (g.id === "signal") return { ...g, pass: true, detail: `strike: ${Number(last.c).toFixed(2)} at or below the armed level ${Number(plan.armBelowUsd).toFixed(2)} (projected z ≤ -${plan.minAbsZ}; planned at z ${plan.zAtPlan}, rank ${plan.rankAtPlan})` };
        if (g.id === "cost") return { ...g, pass: cost.pass, detail: `expect ${cost.expectedGrossBps}bp vs ${cost.requiredBps}bp required (${cost.ratio}x) at the struck price` };
        if (g.id === "evidence") return { ...g, pass: dir.trade, detail: `${dir.reason} (stored documents only, at the strike)` };
        if (g.id === "session") return { ...g, pass: true, detail: `regular session (${session.phase}) at the strike` };
        if (g.id === "quality") return { ...g, pass: true, detail: `grade ${quality[sym].grade} at the strike` };
        return g;
      });
      const cohortLabel = plan.learningCohort || (exploratoryPolicy.evidenceCohort || "exploratory_auto_unvalidated");
      /* Same grant, same evidence, same sleeve — the plan carries the
         company's reversion record from the deep scan that armed it. */
      const patienceStamp = PA.grant({
        reversion: plan.reversionRecord || null,
        historyContext: plan.historyContext || null,
        policy: PA.policyFrom(strategy),
        proposedUsd: qty * last.c,
        positions, pendingOrders, navUsd: bookState.navUsd, nowMs: decisionAtMs });
      const o = await L.proposeOrder({
        ...plan.policyIdentity, accountId, symbol: sym, side: "buy",
        paperLearningOnly: paperLearningActive === true || plan.paperLearningOnly === true,
        operatingStateAtDecision: operating.state,
        learningCohort: cohortLabel, cohortRole: "signal",
        decisionSessionDate: session.date,
        exploratoryPolicyVersion: operating.exploratoryAuto ? (exploratoryPolicy.version || plan.exploratoryPolicyVersion || null) : null,
        decisionManifestHash: plan.decisionManifestHash,
        portfolioDecisionManifestHash: portfolioManifest.manifestHash,
        decisionId, qty, refPriceUsd: last.c, slippageBps: executionCostContext.slippageBps,
        executionCostContext, patience: patienceStamp,
        sizing: { ...sizing, signal: { ...rawSizing, combined }, dataSufficiencyMultiplier: plan.sufficiencyMultiplier,
          positionScale, ...(minimumShareFloorApplied ? { minimumShareFloorApplied: true } : {}) },
        gates, cause, variantId: plan.variantId || "A",
        cost: { ratio: cost.ratio, expectedGrossBps: cost.expectedGrossBps, halfTripBps: cost.halfTripBps,
          roundTripBps: cost.roundTripBps, requiredBps: cost.requiredBps,
          calibratedNetLowerBoundBps: cost.calibratedNetLowerBoundBps ?? null },
        portfolioRisk: { cluster: add.cluster, checks: add.checks, dynamicCorrelation: add.dynamicCorrelation,
          dynamicCorrelationPairs: dynamicCorrelations, bookGrossPctBefore: bookState.book.grossPct,
          cashUsdBefore: bookState.proposalCashUsd },
        decisionContext: {
          entryPath: "strike",
          planId: plan.planId, planCycleId: plan.cycleId, planArmedAtMs: plan.armedAtMs || null,
          strikeObservation: { priceUsd: Number(last.c), barOpenAt: last.t, armBelowUsd: plan.armBelowUsd,
            floorUsd: plan.floorUsd, distancePct: verdict.distancePct, jobId },
          decisionManifestHash: plan.decisionManifestHash,
          decisionManifestVersion: plan.decisionManifestVersion || DM.VERSION,
          portfolioDecisionManifestHash: portfolioManifest.manifestHash,
          portfolioDecisionManifestCoverage: portfolioManifest.coverage,
          paperLearningOnly: paperLearningActive === true || plan.paperLearningOnly === true,
          operatingState: operating.state, learningCohort: cohortLabel, cohortRole: "signal",
          explorationPolicyIds: ["A"],
          exploratoryPolicyVersion: operating.exploratoryAuto ? (exploratoryPolicy.version || null) : null,
          strictVerdict: plan.strictVerdict || { pass: false, blockedBy: ["strict_verdict_missing"], firstBlock: "strict verdict missing" },
          strictUncalibratedVerdict: plan.strictUncalibratedVerdict || null,
          activeVerdict: { pass: true, blockedBy: [], firstBlock: null },
          dataSufficiency: plan.dataSufficiency || null,
          crossSectionRank: plan.rankAtPlan, zAtPlan: plan.zAtPlan,
          causeDetail: causeDetail ? redact(causeDetail) : null,
          decisionAgeMinutesAtProposal: 0,
          conservativeUtilityBps: Number(cost.calibratedNetLowerBoundBps
            ?? (Number(cost.expectedGrossBps) - Number(cost.requiredBps))),
        },
        evidenceRefs: ((causeDetail && causeDetail.drivers && causeDetail.drivers.fundamentalDocs) || []).slice(0, 20),
        decisionAtMs, quality: quality[sym], decisionMarketProvenance: provenance[sym],
        executionLatencyMs: cfg.executionLatencyMs || 60000,
        reservationHeadroomBps: operating.exploratoryAuto ? activity.reservationHeadroomBps : 0,
        expiresAtMs: Math.min(decisionAtMs + signalLifetimeMs, Number.isFinite(Number(closeMs)) ? Number(closeMs) : Infinity),
      });
      if (o.blocked || o.duplicate) {
        await hold("order", o.blocked || "an order already exists for this decision");
        continue;
      }
      /* Immediate approval in the exploratory paper state; a human decides in
         manual approval. Either way the fill waits for the eligible bar. */
      let approval = { approved: false, detail: "left for the operator" };
      if (operating.exploratoryAuto && operating.automaticPaperEntries) {
        const orderSnap = await A.col(A.COL.orders).doc(o.orderId).get();
        const orderDoc = orderSnap.exists ? orderSnap.data() : null;
        const verdict = orderDoc ? LD.exploratoryAutoApproval(orderDoc, { operatingState: operating.state,
          book: bookState.book, navUsd: bookState.navUsd, cfg: strategy, nowMs: Date.now(), sessionDate: session.date })
          : { approve: false, detail: "order document unavailable after proposal" };
        if (verdict.approve) {
          const approved = await L.approveOrder(o.orderId, "auto:exploratory_strike");
          approval = { approved: approved.status === "approved" && !approved.noop && !approved.refused,
            detail: approved.refused || verdict.detail };
          if (approval.approved) {
            await A.col(A.COL.orders).doc(o.orderId).set({ autoApproved: true, autoApprovedDate: session.date,
              autoApprovalDetail: verdict.detail, autoApprovalCohort: verdict.cohort || cohortLabel,
              autoApprovalOperatingState: operating.state, entryPath: "strike" }, { merge: true });
          }
        } else {
          approval = { approved: false, detail: verdict.detail };
          try { await L.rejectOrder(o.orderId, `exploratory auto refusal at strike: ${verdict.detail}`, "auto:exploratory_strike"); } catch {}
        }
      }
      await A.col(A.COL.decisions).doc(decisionId).set({
        cycleId: plan.cycleId, symbol: sym, kind: "entry_strike", entryPath: "strike",
        planId: plan.planId, orderId: o.orderId, qty, refPriceUsd: last.c,
        armBelowUsd: plan.armBelowUsd, floorUsd: plan.floorUsd, distancePct: verdict.distancePct,
        cause, causeDetail: causeDetail ? redact(causeDetail) : null, direction: dir, cost, sizing, gates,
        portfolioDecisionManifest: portfolioManifest, portfolioDecisionManifestHash: portfolioManifest.manifestHash,
        portfolioDecisionManifestValid: portfolioValidation.pass,
        parentDecisionManifestHash: plan.decisionManifestHash,
        approval, operatingState: operating.state, learningCohort: cohortLabel,
        strategyVersion: plan.strategyVersion || strategy.version, decisionAtMs, sessionDate: session.date,
        marketProvenance: provenance[sym], ...A.envelope({ created_by: "positionGuard.strike" }),
      }, { merge: true });
      const claimed = await markPlan(plan, { ...seen, status: "struck", struckAtMs: decisionAtMs,
        strikePriceUsd: Number(last.c), strikeJobId: jobId, orderId: o.orderId, qty, cause,
        approval, lastBlock: null });
      if (!claimed.applied) {
        /* Another writer moved this plan between the strike decision and here.
           The ORDER is already created and is the source of truth — the
           per-symbol order lock in proposeOrder guarantees there is only one —
           so the trade stands; only the plan's audit row lost the race. */
        out.blocked.push({ symbol: sym, reason: "plan_status_race",
          detail: `order ${o.orderId} stands; the plan document had already moved to ${claimed.reason}` });
      }
      /* The book now carries this exposure for the next plan in the loop. */
      const takenUsd = qty * last.c;
      bookState.book.count += 1; bookState.book.grossUsd += takenUsd;
      bookState.book.grossPct = bookState.navUsd > 0 ? 100 * bookState.book.grossUsd / bookState.navUsd : 0;
      const sec = S.sectorOf(sym), pct = bookState.navUsd > 0 ? 100 * takenUsd / bookState.navUsd : 0;
      bookState.book.bySectorPct[sec] = (bookState.book.bySectorPct[sec] || 0) + pct;
      bookState.book.byClusterPct[add.cluster] = (bookState.book.byClusterPct[add.cluster] || 0) + pct;
      bookState.book.rows.push({ symbol: sym, qty, entry: last.c, mark: last.c, valueUsd: takenUsd,
        sector: sec, cluster: add.cluster, pnlPct: 0, marked: true, pending: true });
      bookState.proposalCashUsd = Math.max(0, bookState.proposalCashUsd - takenUsd);
      pendingSymbols.add(sym);
      out.struck.push({ symbol: sym, orderId: o.orderId, qty, priceUsd: last.c, armBelowUsd: plan.armBelowUsd,
        approved: approval.approved, cause });
    } catch (e) {
      console.error("strike failed", redact({ symbol: sym, error: e.message }));
      out.blocked.push({ symbol: sym, reason: "error", detail: String(e.message || e).slice(0, 160) });
      try { await markPlan(plan, { lastBlock: { reason: "error", detail: String(e.message || e).slice(0, 160), atMs: Date.now() } }); } catch {}
    }
  }
  return out;
}

/* ── ENTRY SETTLEMENT on the fast clock ───────────────────────────────────
 * The deep scan proposes and approves; until now only the NEXT deep scan
 * filled. This mirrors that settlement so an approved order fills on the
 * first eligible bar the strike pass sees. recordFill is transactional and
 * keyed by order + bar, so a concurrent deep scan cannot double-fill. */
async function settleEntries({ accountId, operating, strategy, cfg, activity, session, panel, provenance,
  quality, pendingOrders, positions, ctrl, policyIdentity }) {
  const out = { autoApproved: [], filled: [], released: [], skipped: [] };
  if (!session.open) return out;
  const entryControl = L.controlAllowsEntry(ctrl, policyIdentity);
  const nowMs = Date.now();
  let bookState = null;

  /* 1. exploratory auto-approval of proposals the deep scan or a strike left. */
  if (operating.exploratoryAuto && operating.automaticPaperEntries && entryControl.pass) {
    for (const o of pendingOrders.filter((x) => x.status === "proposed")) {
      try {
        if (!bookState) {
          const marks = {};
          for (const p of positions) {
            const b = panel[p.symbol]; const l = b && b[b.length - 1];
            if (l) marks[p.symbol] = l.c; else if (Number(p.lastMarkUsd) > 0) marks[p.symbol] = Number(p.lastMarkUsd);
          }
          bookState = await bookNow({ accountId, positions, marks, pendingOrders: pendingOrders.filter((x) => x.status === "approved") });
        }
        const verdict = LD.exploratoryAutoApproval(o, { operatingState: operating.state, book: bookState.book,
          navUsd: bookState.navUsd, cfg: strategy, nowMs, sessionDate: session.date });
        if (!verdict.approve) {
          const rejected = await L.rejectOrder(o.orderId, `exploratory auto refusal: ${verdict.detail}`, "auto:exploratory");
          out.skipped.push({ orderId: o.orderId, symbol: o.symbol, why: rejected.noop ? verdict.detail : `rejected automatically: ${verdict.detail}` });
          continue;
        }
        const approved = await L.approveOrder(o.orderId, "auto:exploratory");
        if (approved.status !== "approved" || approved.noop || approved.refused) {
          out.skipped.push({ orderId: o.orderId, symbol: o.symbol, why: approved.refused || "auto approval did not transition the order" });
          continue;
        }
        await A.col(A.COL.orders).doc(o.orderId).set({ autoApproved: true, autoApprovedDate: session.date,
          autoApprovalDetail: verdict.detail, autoApprovalCohort: verdict.cohort || o.learningCohort || null,
          autoApprovalOperatingState: operating.state, approvedByPass: "strike" }, { merge: true });
        o.status = "approved"; o.approvedAtMs = approved.approvedAtMs;
        out.autoApproved.push({ orderId: o.orderId, symbol: o.symbol });
      } catch (e) { out.skipped.push({ orderId: o.orderId, symbol: o.symbol, why: String(e.message).slice(0, 120) }); }
    }
  }

  /* 2. fills of approved orders on their first eligible bar. */
  for (const o of pendingOrders.filter((x) => x.status === "approved")) {
    try {
      if (!entryControl.pass) { out.skipped.push({ orderId: o.orderId, symbol: o.symbol, why: `entry safety closed: ${entryControl.reason}` }); continue; }
      if (Number.isFinite(Number(o.expiresAtMs)) && nowMs > Number(o.expiresAtMs)) {
        const released = await L.releaseOrder(o.orderId,
          `expired — signal lifetime elapsed before an eligible fill (decision ${Math.round((nowMs - Number(o.decisionAtMs)) / 60000)} min old)`);
        if (!released.noop) out.released.push({ orderId: o.orderId, symbol: o.symbol, why: "lifetime" });
        continue;
      }
      if (activity.expireUnfilledEntriesAtSessionClose && o.paperLearningOnly === true
          && /^exploratory/.test(String(o.learningCohort || "")) && o.decisionSessionDate
          && o.decisionSessionDate !== session.date) {
        const released = await L.releaseOrder(o.orderId,
          `expired — exploratory entry from ${o.decisionSessionDate} is not carried into ${session.date}`);
        if (!released.noop) out.released.push({ orderId: o.orderId, symbol: o.symbol, why: "session" });
        continue;
      }
      const bars = panel[o.symbol] || [];
      if (!bars.length) { out.skipped.push({ orderId: o.orderId, symbol: o.symbol, why: "no bars this pass" }); continue; }
      const cur = provenance[o.symbol], decided = o.decisionMarketProvenance || {};
      const identitySame = cur && ["provider", "feed", "adjustment"].every((k) => (cur[k] || null) === (decided[k] || null));
      if (!identitySame || !(quality[o.symbol] && quality[o.symbol].tradable)) {
        out.skipped.push({ orderId: o.orderId, symbol: o.symbol, why: "fill-time market identity/quality check failed" }); continue;
      }
      const elig = M.firstEligibleBar(bars, {
        decisionAtMs: Math.max(Number(o.decisionAtMs) || 0, Number(o.approvedAtMs) || 0),
        provider: cur.provider, feed: cur.feed, executionLatencyMs: cfg.executionLatencyMs || 60000 });
      if (!elig.bar) { out.skipped.push({ orderId: o.orderId, symbol: o.symbol, why: elig.reason || "no eligible bar yet" }); continue; }
      const f = await L.recordFill({ orderId: o.orderId, bar: elig.bar, barProvenance: { ...cur, barOpenAt: elig.barOpenAt } });
      if (!f.duplicate && f.status === "filled") out.filled.push({ orderId: o.orderId, symbol: o.symbol, at: elig.barOpenAt, fillPriceUsd: f.fillPriceUsd });
      else if (f.status === "expired") out.released.push({ orderId: o.orderId, symbol: o.symbol, why: f.reason || "fill aborted" });
    } catch (e) { out.skipped.push({ orderId: o.orderId, symbol: o.symbol, why: String(e.message).slice(0, 120) }); }
  }
  return out;
}

module.exports = { PLAN_STATUSES, planId, armLevel, strikeVerdict, selectPlans, planCandidate,
  nearMissEntryEligible,
  planPatchChangesStatus, writePlans, loadPlans, markPlan, evaluateStrikes, settleEntries,
  trustedSource };
