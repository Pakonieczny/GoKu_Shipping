/*  netlify/functions/_investorExplore.js
 *  ---------------------------------------------------------------------------
 *  Investor_AI — the exploratory paper-learning ACTIVITY layer.
 *
 *  The strict policy and its promotion ladder are deliberately slow: nothing
 *  there changes a decision before ~1,137 complete portfolio days, 200 closed
 *  positions, DSR/PBO and a locked forward window. That is right for
 *  validation and useless for a desk that has to produce labelled outcomes
 *  today. This module owns the fast lane, and only the fast lane:
 *
 *    1. PACING    — how many new exploratory entries a single cycle may open,
 *                   so activity is spread across the session instead of one
 *                   burst at 09:45 that wins or loses on one market move.
 *    2. SELECTION — a Thompson-sampling scoreboard over the FROZEN policy
 *                   family. Each closed exploratory trade is attributed to the
 *                   frozen policies whose entry rule fired for it. The sampled
 *                   posterior of those policies steers which qualified
 *                   candidates are taken first. Before evidence exists the
 *                   prior dominates and selection is deliberately diverse.
 *    3. CONTROL   — a small, labelled, unconditional cohort: names that pass
 *                   every HAZARD gate (data quality, session, known earnings
 *                   window, fundamental cause, downtrend, adverse
 *                   intelligence, liquidity) but NOT the signal gates. They
 *                   are the baseline the signal cohort is measured against.
 *                   Without a control group "the signal made +12bp" is not a
 *                   finding; with one it is a difference with an error bar.
 *
 *  What this module never does: touch the strict verdict, the shadow
 *  experiment, the allocator, the calibration or the ladder. Every order it
 *  influences is already tagged paperLearningOnly with an exploratory cohort,
 *  and the strict counterfactual is stored beside it. Learning here changes
 *  ORDER and PACE among candidates the exploratory gates already admitted; it
 *  cannot admit a candidate the gates refused.
 * ---------------------------------------------------------------------------
 */

"use strict";

const crypto = require("crypto");
const DS = require("./_investorSufficiency");

/* Gates a CONTROL entry may fail. Everything else is a hazard finding and
   still blocks a control entry exactly as it blocks a signal entry. */
const CONTROL_MAY_FAIL = Object.freeze(["signal", "cost", "crowding", "turnover"]);

const DEFAULTS = Object.freeze({
  enabled: true,
  maxNewEntriesPerCycle: 6,
  minimumShareFloor: 1,
  reservationHeadroomBps: 150,
  expireUnfilledEntriesAtSessionClose: true,
  controlCohort: Object.freeze({
    enabled: true,
    maxOpenPositions: 6,
    maxNewPerSession: 3,
    sizeMultiplier: 0.5,
  }),
  policySelection: Object.freeze({
    method: "thompson",
    priorMeanBps: 0,
    priorSdBps: 150,
    tradeSdBps: 250,
    minClosedForObservedSd: 8,
  }),
  scoreboardLookbackTrades: 1000,
});

const COHORT_SIGNAL = "exploratory_auto_unvalidated";
const COHORT_CONTROL = "exploratory_control_unconditional";

function clampNum(v, lo, hi, dflt) {
  const n = Number(v);
  if (!Number.isFinite(n)) return dflt;
  return Math.min(hi, Math.max(lo, n));
}

/** The activity policy, merged from the frozen strategy with clamped defaults.
 *  Every knob is bounded so a typed-in value cannot turn the desk into a
 *  random-entry generator or a zero-activity one. */
function activityPolicy(strategy) {
  const raw = (strategy && strategy.exploratoryAuto && strategy.exploratoryAuto.activity) || {};
  const control = raw.controlCohort || {};
  const sel = raw.policySelection || {};
  return {
    enabled: raw.enabled !== false,
    maxNewEntriesPerCycle: Math.round(clampNum(raw.maxNewEntriesPerCycle, 1, 40,
      DEFAULTS.maxNewEntriesPerCycle)),
    minimumShareFloor: Math.round(clampNum(raw.minimumShareFloor, 0, 1, DEFAULTS.minimumShareFloor)),
    reservationHeadroomBps: Math.round(clampNum(raw.reservationHeadroomBps, 0, 1000, DEFAULTS.reservationHeadroomBps)),
    expireUnfilledEntriesAtSessionClose: raw.expireUnfilledEntriesAtSessionClose !== false,
    controlCohort: {
      enabled: control.enabled !== false,
      maxOpenPositions: Math.round(clampNum(control.maxOpenPositions, 0, 40,
        DEFAULTS.controlCohort.maxOpenPositions)),
      maxNewPerSession: Math.round(clampNum(control.maxNewPerSession, 0, 20,
        DEFAULTS.controlCohort.maxNewPerSession)),
      sizeMultiplier: clampNum(control.sizeMultiplier, 0.1, 1, DEFAULTS.controlCohort.sizeMultiplier),
      evidenceCohort: typeof control.evidenceCohort === "string" && /^exploratory_/.test(control.evidenceCohort)
        ? control.evidenceCohort : COHORT_CONTROL,
    },
    policySelection: {
      method: sel.method === "utility" ? "utility" : "thompson",
      priorMeanBps: clampNum(sel.priorMeanBps, -100, 100, DEFAULTS.policySelection.priorMeanBps),
      priorSdBps: clampNum(sel.priorSdBps, 10, 1000, DEFAULTS.policySelection.priorSdBps),
      tradeSdBps: clampNum(sel.tradeSdBps, 25, 2000, DEFAULTS.policySelection.tradeSdBps),
      minClosedForObservedSd: Math.round(clampNum(sel.minClosedForObservedSd, 3, 100,
        DEFAULTS.policySelection.minClosedForObservedSd)),
    },
    scoreboardLookbackTrades: Math.round(clampNum(raw.scoreboardLookbackTrades, 50, 5000,
      DEFAULTS.scoreboardLookbackTrades)),
  };
}

/* ── deterministic randomness ──────────────────────────────────────────────
   Netlify background functions retry after failure. A retry must reach the
   same selection, so every random draw is seeded from the cycle id. */
function seededRandom(seed) {
  const h = crypto.createHash("sha256").update(String(seed)).digest();
  let a = h.readUInt32LE(0) >>> 0;
  return function next() {                 // mulberry32
    a = (a + 0x6D2B79F5) >>> 0;
    let t = a;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

function gaussian(rng) {
  let u = 0, v = 0;
  while (u === 0) u = rng();
  while (v === 0) v = rng();
  return Math.sqrt(-2 * Math.log(u)) * Math.cos(2 * Math.PI * v);
}

/* ── the scoreboard ──────────────────────────────────────────────────────── */
function isExploratoryTrade(t) {
  return !!t && t.paperLearningOnly === true
    && /^exploratory/.test(String(t.learningCohort || ""));
}

function tradePolicies(t) {
  const ctx = t && t.decisionContext || {};
  const ids = Array.isArray(ctx.explorationPolicyIds) ? ctx.explorationPolicyIds : [];
  if (ids.length) return ids.map(String);
  return t && t.variantId ? [String(t.variantId)] : [];
}

function tradeCohortRole(t) {
  const ctx = t && t.decisionContext || {};
  if (ctx.cohortRole === "control") return "control";
  if (String(t && t.learningCohort || "") === COHORT_CONTROL) return "control";
  return "signal";
}

function posterior(stat, sel) {
  const n = stat.n;
  const priorVar = sel.priorSdBps * sel.priorSdBps;
  const observedVar = n >= sel.minClosedForObservedSd && n > 1
    ? Math.max(1, (stat.sumSq - (stat.sum * stat.sum) / n) / (n - 1)) : null;
  const tradeVar = observedVar != null ? Math.max(observedVar, 25 * 25) : sel.tradeSdBps * sel.tradeSdBps;
  const postVar = 1 / (1 / priorVar + n / tradeVar);
  const postMean = postVar * (sel.priorMeanBps / priorVar + stat.sum / tradeVar);
  return { posteriorMeanBps: Number(postMean.toFixed(2)),
    posteriorSdBps: Number(Math.sqrt(postVar).toFixed(2)),
    tradeSdBpsUsed: Number(Math.sqrt(tradeVar).toFixed(1)),
    observedSd: observedVar != null };
}

function summarize(stat, sel) {
  const n = stat.n;
  return {
    n,
    meanNetBps: n ? Number((stat.sum / n).toFixed(2)) : null,
    hitRate: n ? Number((stat.wins / n).toFixed(3)) : null,
    sumNetBps: Number(stat.sum.toFixed(1)),
    ...posterior(stat, sel),
  };
}

/**
 * Build the exploratory scoreboard from closed paper trades.
 * @param {object[]} trades      closed trade docs (any cohort; filtered here)
 * @param {object}   opts        { policySelection, policyIds, asOfMs }
 */
function buildScoreboard(trades, { policySelection = DEFAULTS.policySelection,
  policyIds = [], asOfMs = Date.now() } = {}) {
  const sel = { ...DEFAULTS.policySelection, ...(policySelection || {}) };
  const blank = () => ({ n: 0, sum: 0, sumSq: 0, wins: 0 });
  const byPolicy = {};
  for (const id of policyIds) byPolicy[String(id)] = blank();
  const byCohort = { signal: blank(), control: blank() };
  const bySufficiency = { high: blank(), medium: blank(), low: blank(), unknown: blank() };
  let closed = 0, skipped = 0;
  for (const t of Array.isArray(trades) ? trades : []) {
    if (!isExploratoryTrade(t)) continue;
    const net = Number(t.netBps);
    if (!Number.isFinite(net)) { skipped += 1; continue; }
    closed += 1;
    const role = tradeCohortRole(t);
    const c = byCohort[role];
    c.n += 1; c.sum += net; c.sumSq += net * net; if (net > 0) c.wins += 1;
    if (role !== "signal") continue;
    const suff = DS.ofRecord(t);
    const sb = bySufficiency[suff ? suff.bucket : "unknown"] || bySufficiency.unknown;
    sb.n += 1; sb.sum += net; sb.sumSq += net * net; if (net > 0) sb.wins += 1;
    for (const id of tradePolicies(t)) {
      if (!byPolicy[id]) byPolicy[id] = blank();
      const s = byPolicy[id];
      s.n += 1; s.sum += net; s.sumSq += net * net; if (net > 0) s.wins += 1;
    }
  }
  const policies = Object.fromEntries(Object.entries(byPolicy)
    .map(([id, s]) => [id, summarize(s, sel)]));
  const cohorts = { signal: summarize(byCohort.signal, sel), control: summarize(byCohort.control, sel) };
  /* Signal minus control, with a pooled standard error. Observational, and
     said so: it steers exploratory pacing/selection only. */
  let signalVsControl = null;
  if (cohorts.signal.n > 0 && cohorts.control.n > 0) {
    const varS = Math.pow(cohorts.signal.tradeSdBpsUsed, 2) / cohorts.signal.n;
    const varC = Math.pow(cohorts.control.tradeSdBpsUsed, 2) / cohorts.control.n;
    const diff = cohorts.signal.meanNetBps - cohorts.control.meanNetBps;
    const se = Math.sqrt(varS + varC);
    signalVsControl = { differenceBps: Number(diff.toFixed(2)), standardErrorBps: Number(se.toFixed(2)),
      t: se > 0 ? Number((diff / se).toFixed(2)) : null, mode: "observational_not_causal" };
  }
  const sufficiency = Object.fromEntries(Object.entries(bySufficiency)
    .map(([k, v]) => [k, summarize(v, sel)]));
  const leader = Object.entries(policies).filter(([, v]) => v.n > 0)
    .sort((a, b) => b[1].posteriorMeanBps - a[1].posteriorMeanBps)[0];
  return { asOfMs, closedExploratoryTrades: closed, skippedUnscored: skipped,
    policies, cohorts, signalVsControl, sufficiency,
    leadingPolicyId: leader ? leader[0] : null,
    affectsDecision: "exploratory_selection_and_pacing_only",
    prior: { meanBps: sel.priorMeanBps, sdBps: sel.priorSdBps, tradeSdBps: sel.tradeSdBps } };
}

/** Admit only outcomes from THIS experiment to the exploratory scoreboard:
 *  same frozen identity, same exploratory policy version, and (when the
 *  trade recorded one) the same sufficiency policy version. Newest first,
 *  bounded by lookback, with a hash of the admitted trade ids so the sample
 *  that steered a cycle is reproducible from the record. */
function admitScoreboardTrades(trades, { strategyHash, universeHash, variantsHash,
  exploratoryPolicyVersion = null, sufficiencyVersion = null, lookback = 1000 } = {}) {
  const excluded = { notExploratory: 0, identity: 0, policyVersion: 0, sufficiencyVersion: 0, lookback: 0 };
  const rows = [];
  const identityComplete = [strategyHash, universeHash, variantsHash].every((h) => /^[a-f0-9]{64}$/.test(String(h || "")))
    && !!exploratoryPolicyVersion && !!sufficiencyVersion;
  if (!identityComplete) {
    /* No complete current identity → no trade can match it → empty sample. */
    return { trades: [], excluded: { ...excluded, identity: (trades || []).length }, count: 0,
      sampleHash: crypto.createHash("sha256").update("").digest("hex"), incompleteIdentity: true };
  }
  for (const t of Array.isArray(trades) ? trades : []) {
    if (!isExploratoryTrade(t)) { excluded.notExploratory += 1; continue; }
    if (t.strategyHash !== strategyHash || t.universeHash !== universeHash
        || t.variantsHash !== variantsHash) { excluded.identity += 1; continue; }
    /* EXACT versions, and a missing version is a mismatch: a trade that
       cannot say which exploratory or sufficiency policy produced it must
       not steer the current one. */
    if (!t.exploratoryPolicyVersion || t.exploratoryPolicyVersion !== exploratoryPolicyVersion) {
      excluded.policyVersion += 1; continue;
    }
    const ds = t.decisionContext && t.decisionContext.dataSufficiency;
    if (!ds || !ds.version || ds.version !== sufficiencyVersion) {
      excluded.sufficiencyVersion += 1; continue;
    }
    rows.push(t);
  }
  rows.sort((a, b) => String(b.closedAt || "").localeCompare(String(a.closedAt || ""))
    || String(a.tradeId || "").localeCompare(String(b.tradeId || "")));
  const cap = Math.max(1, Number(lookback) || 1000);
  if (rows.length > cap) { excluded.lookback = rows.length - cap; rows.length = cap; }
  const ids = rows.map((t) => String(t.tradeId || `${t.symbol}|${t.closedAt}`)).sort();
  const sampleHash = crypto.createHash("sha256").update(ids.join("|")).digest("hex");
  return { trades: rows, excluded, sampleHash, count: rows.length };
}

/* ── Thompson selection ──────────────────────────────────────────────────── */
/**
 * Order qualified candidates for this cycle.
 * @param {object[]} candidates  [{ sym, policyIds: [...], utilityBps }]
 * @param {object}   scoreboard  from buildScoreboard (may be null)
 * @param {object}   opts        { seed, policySelection }
 * Returns a new array, best first, each row with { score, sampledPolicyId, sampledBps }.
 */
function rankCandidates(candidates, scoreboard, { seed = "seed",
  policySelection = DEFAULTS.policySelection } = {}) {
  const sel = { ...DEFAULTS.policySelection, ...(policySelection || {}) };
  const rows = (candidates || []).map((c) => ({ ...c, utilityBps: Number(c.utilityBps) || 0 }));
  if (sel.method === "utility") {
    return rows.map((r) => ({ ...r, score: r.utilityBps - Math.max(0, Number(r.penaltyBps) || 0),
      sampledPolicyId: null, sampledBps: null }))
      .sort((a, b) => b.score - a.score || String(a.sym).localeCompare(String(b.sym)));
  }
  const rng = seededRandom(seed);
  const policies = (scoreboard && scoreboard.policies) || {};
  /* One draw per policy per cycle, shared by every candidate that policy
     fired for — that is what makes the draw a belief about the policy rather
     than noise per ticker. */
  const draws = {};
  const drawFor = (id) => {
    if (draws[id] != null) return draws[id];
    const p = policies[id];
    const mean = p ? p.posteriorMeanBps : sel.priorMeanBps;
    const sd = p ? p.posteriorSdBps : sel.priorSdBps;
    draws[id] = mean + sd * gaussian(rng);
    return draws[id];
  };
  /* Deterministic per-candidate jitter breaks ties without letting the
     alphabet become an allocation model. */
  const jitter = (sym) => (seededRandom(`${seed}|${sym}`)() - 0.5) * 1e-3;
  return rows.map((r) => {
    const ids = Array.isArray(r.policyIds) && r.policyIds.length ? r.policyIds.map(String) : ["A"];
    let best = null;
    for (const id of ids) {
      const v = drawFor(id);
      if (!best || v > best.v) best = { id, v };
    }
    const penalty = Math.max(0, Number(r.penaltyBps) || 0);
    return { ...r, sampledPolicyId: best.id, sampledBps: Number(best.v.toFixed(2)), penaltyBps: penalty,
      score: Number((best.v + r.utilityBps - penalty + jitter(r.sym)).toFixed(4)) };
  }).sort((a, b) => b.score - a.score || String(a.sym).localeCompare(String(b.sym)));
}

/* ── control cohort ─────────────────────────────────────────────────────── */
/** A candidate is control-eligible when every gate it failed is a signal-type
 *  gate. Any hazard failure (quality, session, blackout, evidence, trend,
 *  intelligence, dispersion, liquidity) disqualifies it exactly as it would
 *  disqualify a signal entry. */
function controlEligible(evalRes) {
  if (!evalRes || evalRes.kind !== "entry") return false;
  const blocked = Array.isArray(evalRes.blockedBy) ? evalRes.blockedBy : [];
  if (!blocked.length) return false;                 // it passed — it is a signal entry
  return blocked.every((id) => CONTROL_MAY_FAIL.includes(id));
}

/** Pick control names deterministically at random for this cycle. */
function selectControl(pool, { seed = "seed", limit = 0 } = {}) {
  if (!(limit > 0) || !Array.isArray(pool) || !pool.length) return [];
  const keyed = pool.map((p) => ({ p,
    key: crypto.createHash("sha256").update(`${seed}|control|${p.sym}`).digest("hex") }));
  keyed.sort((a, b) => a.key.localeCompare(b.key));
  return keyed.slice(0, limit).map((k) => k.p);
}

/** Gate failures a persisted CONTROL order may carry and still be approved. */
function controlOrderGatesAcceptable(order) {
  const gates = Array.isArray(order && order.gates) ? order.gates : null;
  if (!gates) return false;
  return gates.every((g) => !g.blocking || g.pass || CONTROL_MAY_FAIL.includes(g.id));
}

function isControlOrder(order) {
  const ctx = order && order.decisionContext || {};
  return ctx.cohortRole === "control" || String(order && order.learningCohort || "") === COHORT_CONTROL;
}

module.exports = {
  DEFAULTS, CONTROL_MAY_FAIL, COHORT_SIGNAL, COHORT_CONTROL,
  activityPolicy, seededRandom, gaussian,
  buildScoreboard, rankCandidates, admitScoreboardTrades,
  controlEligible, selectControl, controlOrderGatesAcceptable, isControlOrder,
  isExploratoryTrade, tradePolicies, tradeCohortRole,
};
