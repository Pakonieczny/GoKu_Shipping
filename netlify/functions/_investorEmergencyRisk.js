/*  netlify/functions/_investorEmergencyRisk.js  (fund-manager-v1)
 *  ---------------------------------------------------------------------------
 *  Investor AI — the only deterministic authority that may originate a
 *  REDUCE or SELL outside an active AI mandate (blueprint §8.7, §15.1,
 *  EMERGENCY_RISK_POLICY_TEMPLATE in _investorPolicy).
 *
 *  Authority is NARROW and HASHED. An owner-approved EmergencyRiskPolicy
 *  lets code: freeze new exposure and cancel unfilled entries on stale
 *  broker truth, unresolved reconciliation, daily-loss/drawdown, buying-
 *  power, concentration, gross-exposure, stressed-loss or protection-SLA
 *  breach; neutralise an accidental short to zero; reduce or flatten an
 *  unprotected position after its acknowledgement deadline; bring an
 *  account back under an exact hard ceiling. It cannot rotate into another
 *  company, average down, improve returns or open a long. Without an
 *  approved policy the template is INACTIVE: code may only freeze and
 *  alert.
 *
 *  Every action carries label EMERGENCY_RISK, the trigger that fired, the
 *  rule used for reduction order, an exact whole-share quantity, a
 *  marketable-limit collar and REGULAR_ONLY session. It alerts the
 *  operator, forces reconciliation and forces a later Sol review. It is
 *  never recorded or scored as a Sol decision.
 *
 *  An inherited or thesis-less position gets an EmergencyProtectionPlan —
 *  KEEP_PROTECTION / REDUCE / SELL only, visible as ACTION REQUIRED — never
 *  a fabricated AI mandate.
 * ---------------------------------------------------------------------------
 */

"use strict";

const A = require("./_investorAdmin");
const POLICY = require("./_investorPolicy");
const M = require("./_investorMoney");

const LABEL = "EMERGENCY_RISK";
const PLAN_SCHEMA = "emergency-protection-plan.v1";
const FORBIDDEN = Object.freeze(["OPEN_LONG", "ROTATE", "AVERAGE_DOWN", "INCREASE_EXPOSURE", "IMPROVE_PRIORITY", "SUBSTITUTE_SYMBOL"]);
const ACTION_OPS = Object.freeze({
  FREEZE_EXPANSION_AND_CANCEL_UNFILLED_ENTRIES: ["FREEZE_EXPANSION", "CANCEL_UNFILLED_ENTRY"],
  REDUCE_TO_LEGAL_LIMIT: ["FREEZE_EXPANSION", "REDUCE"],
  REDUCE_TO_LIMIT: ["FREEZE_EXPANSION", "REDUCE"],
  REDUCE_OR_FLATTEN_UNPROTECTED: ["FREEZE_EXPANSION", "REDUCE", "FLATTEN"],
  BUY_TO_COVER_TO_ZERO: ["BUY_TO_COVER_ACCIDENTAL_SHORT"],
});

function lazy(m) { try { return require(m); } catch { return null; } }
function db(admin) { return admin || A; }
function big(v) { return BigInt(String(v == null ? 0 : v)); }
function cmp(op, value, threshold) {
  const a = big(value), b = big(threshold);
  return op === "gt" ? a > b : op === "gte" ? a >= b : op === "lt" ? a < b : op === "lte" ? a <= b : op === "eq" ? a === b : false;
}

/* ── triggers (pure) ───────────────────────────────────────────────────── */
/** Evaluate every trigger against the observed metrics with persistence.
 *  `persistence` maps triggerId → firstSeenAtMs from the previous tick. */
function evaluateTriggers({ policy, metrics = {}, persistence = {}, nowMs = A.now() } = {}) {
  const triggers = (policy && policy.triggers) || [];
  const fired = [], pending = [], next = {};
  for (const t of triggers) {
    const value = metrics[t.metric];
    if (value === undefined || value === null) continue;
    const hit = cmp(t.op, value, t.threshold);
    if (!hit) continue;
    const firstSeen = Number(persistence[t.id]) || nowMs;
    next[t.id] = firstSeen;
    const persisted = Math.floor((nowMs - firstSeen) / 1000);
    const row = { id: t.id, action: t.action, metric: t.metric, value: String(value), threshold: String(t.threshold), op: t.op, persistedSeconds: persisted, requiredSeconds: Number(t.persistenceSeconds) || 0 };
    if (persisted >= (Number(t.persistenceSeconds) || 0)) fired.push(row); else pending.push(row);
  }
  return { fired, pending, persistence: next, evaluatedAtMs: nowMs };
}

/* ── the bounded plan (pure) ───────────────────────────────────────────── */
function permitted(policy, op) { return Array.isArray(policy && policy.permittedOperations) && policy.permittedOperations.includes(op) && !FORBIDDEN.includes(op); }
function orderTerms(policy) {
  const r = (policy && policy.orderRule) || {};
  return { orderType: r.orderType || "MARKETABLE_LIMIT", collarBps: String(r.collarBps || "100"), session: r.session || "REGULAR_ONLY", onHaltOrNonfill: r.onHaltOrNonfill || "KEEP_PROTECTION_AND_ALERT" };
}
/** Exact whole shares to remove a concentration or gross breach at the mark. */
function unitsToRemove({ excessMicros, markMicros }) {
  if (excessMicros <= 0n || markMicros <= 0n) return 0n;
  return M.divRound(excessMicros, markMicros, M.ROUNDING.CEIL);
}
function planActions({ fired = [], portfolio, policy, riskMandate = POLICY.RISK_MANDATE, ranks = null, marks = {}, advBySymbol = {}, nowMs = A.now(), policyActive = true } = {}) {
  const PR = lazy("./_investorPortfolioRisk");
  const terms = orderTerms(policy);
  const actions = [];
  const push = (op, fields) => {
    if (FORBIDDEN.includes(op)) throw Object.assign(new Error(`forbidden emergency operation ${op}`), { code: "EMERGENCY_OP_FORBIDDEN" });
    if (!policyActive && op !== "FREEZE_EXPANSION") return;
    if (policyActive && !permitted(policy, op)) return;
    actions.push({ label: LABEL, op, ...fields, ...(["REDUCE", "FLATTEN", "BUY_TO_COVER_ACCIDENTAL_SHORT"].includes(op) ? terms : {}), atMs: nowMs });
  };
  const positions = (portfolio && portfolio.positions) || [];
  const working = (portfolio && portfolio.workingOrders) || [];
  const navMinor = big(portfolio && portfolio.navMinor);
  const markOf = (p) => big((marks[p.symbol] && marks[p.symbol].markMicros) || p.markMicros || 0);
  let freeze = false;
  for (const f of fired) {
    const ops = ACTION_OPS[f.action] || [];
    if (ops.includes("FREEZE_EXPANSION") && !freeze) { freeze = true; push("FREEZE_EXPANSION", { triggerId: f.id, reason: `${f.metric} ${f.op} ${f.threshold} (observed ${f.value})` }); }
    if (ops.includes("CANCEL_UNFILLED_ENTRY")) {
      for (const o of working.filter((w) => w.side === "buy" && big(w.remainingUnits || w.quantityUnits) > 0n)) push("CANCEL_UNFILLED_ENTRY", { triggerId: f.id, symbol: o.symbol, orderId: o.orderId || null, quantityUnits: String(o.remainingUnits || o.quantityUnits) });
    }
    if (f.action === "BUY_TO_COVER_TO_ZERO") {
      for (const p of positions.filter((x) => big(x.quantityUnits) < 0n)) push("BUY_TO_COVER_ACCIDENTAL_SHORT", { triggerId: f.id, symbol: p.symbol, quantityUnits: (-big(p.quantityUnits)).toString(), targetQuantityUnits: "0", ruleUsed: "ACCIDENTAL_SHORT_FIRST", neverPositive: true });
    }
    if (f.action === "REDUCE_TO_LIMIT" || f.action === "REDUCE_TO_LEGAL_LIMIT") {
      if (f.metric === "singleNameWeightBps") {
        const cap = big(riskMandate.weights.maxSingleNameWeightBps);
        for (const p of positions) {
          const q = big(p.quantityUnits); if (q <= 0n) continue;
          const mark = markOf(p); const valueMicros = q * mark; const capMicros = navMinor * 10000n * cap / 10000n;
          if (valueMicros > capMicros) { const units = unitsToRemove({ excessMicros: valueMicros - capMicros, markMicros: mark }); if (units > 0n) push("REDUCE", { triggerId: f.id, symbol: p.symbol, quantityUnits: (units > q ? q : units).toString(), ruleUsed: "EXACT_BREACH_REMOVAL_WHOLE_SHARES", metric: f.metric }); }
        }
      } else {
        /* gross exposure, stressed loss, buying power: the smallest liquid set with the greatest binding relief per dollar, in the policy's reduction order */
        const order = PR ? PR.emergencyReductionOrder({ positions: positions.filter((p) => big(p.quantityUnits) > 0n).map((p) => ({ ...p, markMicros: markOf(p).toString() })), ranks, marks, advBySymbol, policy: riskMandate, nowMs }) : { order: positions.map((p) => p.symbol), ruleUsed: "POSITION_ORDER_FALLBACK" };
        const listed = Array.isArray(order.order) ? order.order : (order.symbols || []);
        const excessBps = f.metric === "grossExposureBps" ? big(f.value) - big(riskMandate.weights.maxGrossExposureBps) : (f.metric === "aggregateStressedLossBps" ? big(f.value) - big(riskMandate.losses.maxAggregateStressedLossBps) : 0n);
        let remainingMicros = f.metric === "buyingPowerShortfallMinor" ? big(f.value) * 10000n : (excessBps > 0n ? navMinor * 10000n * excessBps / 10000n : 0n);
        for (const entry of listed) {
          if (remainingMicros <= 0n) break;
          const symbol = typeof entry === "string" ? entry : entry.symbol;
          const p = positions.find((x) => x.symbol === symbol); if (!p) continue;
          const q = big(p.quantityUnits), mark = markOf(p);
          const units = unitsToRemove({ excessMicros: remainingMicros, markMicros: mark });
          const take = units > q ? q : units;
          if (take <= 0n) continue;
          push("REDUCE", { triggerId: f.id, symbol, quantityUnits: take.toString(), ruleUsed: order.ruleUsed || "SMALLEST_LIQUID_SET_GREATEST_STRESS_RELIEF_PER_DOLLAR", metric: f.metric });
          remainingMicros -= take * mark;
        }
      }
    }
    if (f.action === "REDUCE_OR_FLATTEN_UNPROTECTED") {
      for (const p of positions.filter((x) => big(x.quantityUnits) > 0n && !x.lossBoundaryPriceMicros && (x.protectionState == null || !/PROTECTED/.test(String(x.protectionState))))) {
        push("FLATTEN", { triggerId: f.id, symbol: p.symbol, quantityUnits: String(p.quantityUnits), ruleUsed: "UNPROTECTED_AFTER_SLA" });
      }
    }
  }
  for (const a of actions) if (a.op === "BUY_TO_COVER_ACCIDENTAL_SHORT" && big(a.quantityUnits) <= 0n) throw Object.assign(new Error("cover quantity must be positive"), { code: "EMERGENCY_INVALID_QUANTITY" });
  return { actions, freeze, label: LABEL, policyActive, forcesReconciliation: true, forcesLaterSolReview: actions.length > 0, alwaysAlertOperator: true };
}

/* ── persistence ───────────────────────────────────────────────────────── */
async function enforceBoundedPolicy({ accountId, observed = {}, portfolio, control = {}, ranks = null, marks = {}, advBySymbol = {}, admin = null, nowMs = A.now() } = {}) {
  const D = db(admin);
  const active = POLICY.activeEmergencyPolicy(control.emergencyRiskPolicy || null);
  const policy = active.policy;
  const riskMandate = POLICY.applyRiskMandateOverrides(control.riskMandateOverrides || null).riskMandate;
  const persistence = (control.emergencyPersistence) || {};
  const ev = evaluateTriggers({ policy, metrics: observed, persistence, nowMs });
  const plan = planActions({ fired: ev.fired, portfolio, policy, riskMandate, ranks, marks, advBySymbol, nowMs, policyActive: active.active });
  const written = [];
  if (plan.actions.length) {
    for (const a of plan.actions) {
      if (a.op === "FREEZE_EXPANSION") continue;
      const id = `${LABEL}:${accountId}:${a.symbol || "account"}:${a.op}:${a.triggerId}:${Math.floor(nowMs / 60000)}`;
      await D.col(D.COL.executionOutbox).doc(id).set({ transitionId: id, accountId, symbol: a.symbol || null, kind: a.op, quantityUnits: a.quantityUnits || null, orderType: a.orderType || null, collarBps: a.collarBps || null,
        session: a.session || null, onHaltOrNonfill: a.onHaltOrNonfill || null, status: "PENDING", idempotencyKey: id, attempts: 0, createdAtMs: nowMs, authority: LABEL, triggerId: a.triggerId, ruleUsed: a.ruleUsed || null, policyHash: policy.policyHash || null });
      written.push(id);
    }
    const controlPatch = { emergencyState: "ENGAGED", buyState: "FROZEN", freezeNewBuys: true, emergencyEngagedAtMs: nowMs, emergencyLastActions: plan.actions.slice(0, 20), emergencyPersistence: ev.persistence, emergencyForcesSolReview: true };
    await D.col(D.COL.control).doc("control").set(controlPatch, { merge: true });
    try { await D.col(D.COL.alerts).doc(`emergency_${accountId}`).set({ conditionId: `emergency_${accountId}`, severity: "critical", kind: LABEL, accountId, reason: plan.actions.map((a) => `${a.op}${a.symbol ? " " + a.symbol : ""}${a.quantityUnits ? " ×" + a.quantityUnits : ""} (${a.triggerId})`).join("; ").slice(0, 300), active: true, raisedAtMs: nowMs, acknowledgedAtMs: null, resolvedAtMs: null, policyActive: active.active }, { merge: true }); } catch {}
  } else {
    await D.col(D.COL.control).doc("control").set({ emergencyPersistence: ev.persistence, emergencyLastEvaluatedAtMs: nowMs }, { merge: true }).catch(() => {});
  }
  return { ...plan, fired: ev.fired, pending: ev.pending, policyActive: active.active, policyReason: active.reason, outbox: written };
}

/* ── the EmergencyProtectionPlan for a thesis-less position (§8.7) ─────── */
function emergencyProtectionPlan({ accountId, symbol, position, reason, creator = "SYSTEM_POLICY", hardLossCeilingBps = null, riskMandate = POLICY.RISK_MANDATE, version = 1, nowMs = A.now(), reviewAfterMs = 5 * 86400000 } = {}) {
  const qty = big(position && position.quantityUnits);
  if (qty <= 0n) throw Object.assign(new Error("protection plan requires an owned quantity"), { code: "NOT_HELD" });
  const mark = big(position.markMicros);
  const ceiling = big(hardLossCeilingBps != null ? hardLossCeilingBps : riskMandate.losses.maxStressedLossPerPositionBps);
  const stopMicros = mark - mark * ceiling / 10000n;
  const collar = String((riskMandate.orders && riskMandate.orders.emergencyCollarBps) || "100");
  return {
    schemaVersion: PLAN_SCHEMA, planId: `${accountId}_${symbol}_ep${String(version).padStart(3, "0")}`, accountId, symbol, version,
    issuer: position.issuer || null, basisMicros: position.entryPriceMicros || null, quantityUnits: qty.toString(), markMicros: mark.toString(),
    hardLossCeilingBps: ceiling.toString(), protectiveOrder: { kind: "STOP", stopMicros: stopMicros.toString(), quantityUnits: qty.toString(), timeInForce: "GTC", session: "REGULAR_ONLY", collarBps: collar },
    creator, reason: String(reason || "").slice(0, 300), createdAtMs: nowMs, expiresAtMs: nowMs + reviewAfterMs, reviewAtMs: nowMs + Math.floor(reviewAfterMs / 2),
    permittedOperations: ["KEEP_PROTECTION", "REDUCE", "SELL"], forbiddenOperations: ["BUY", ...FORBIDDEN], status: "ACTION_REQUIRED", replacedBy: null, replaceableBy: ["VALID_SOL_HOLDING_MANDATE", "OPERATOR_LIQUIDATION"], label: LABEL,
  };
}
async function persistProtectionPlan(plan, { admin = null } = {}) {
  const D = db(admin);
  await D.col(D.COL.emergencyPlans).doc(plan.planId).set({ ...plan, ...D.envelope({ created_by: "emergencyRisk.persistProtectionPlan" }) });
  await D.col(D.COL.activeMandates).doc(`${plan.accountId}_${plan.symbol}`).set({ accountId: plan.accountId, symbol: plan.symbol, mandateSeriesId: `${plan.accountId}_${plan.symbol}`, status: "ACTION_REQUIRED", emergencyPlanId: plan.planId, lossBoundaryPriceMicros: plan.protectiveOrder.stopMicros, protectionState: "EMERGENCY_PLAN", updatedAtMs: A.now() }, { merge: true });
  try { await D.col(D.COL.alerts).doc(`emergency_plan_${plan.accountId}_${plan.symbol}`).set({ conditionId: `emergency_plan_${plan.accountId}_${plan.symbol}`, severity: "critical", kind: "ACTION_REQUIRED", accountId: plan.accountId, symbol: plan.symbol, reason: `emergency protection plan v${plan.version}: ${plan.reason}`, active: true, raisedAtMs: A.now(), acknowledgedAtMs: null, resolvedAtMs: null }, { merge: true }); } catch {}
  return { persisted: true, planId: plan.planId };
}

module.exports = { LABEL, PLAN_SCHEMA, FORBIDDEN, ACTION_OPS, evaluateTriggers, planActions, enforceBoundedPolicy, emergencyProtectionPlan, persistProtectionPlan, unitsToRemove };
