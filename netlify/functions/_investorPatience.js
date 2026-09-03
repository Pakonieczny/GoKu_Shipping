/*  netlify/functions/_investorPatience.js  (v1.0)
 *  ---------------------------------------------------------------------------
 *  Investor_AI — the patience sleeve: a bounded carve-out for names whose own
 *  history says they recover over days rather than hours.
 *
 *  THE PROBLEM IT SOLVES
 *  ------------------------------------------------------------------------
 *  The rank exit closes a position when the gap it was trading has closed
 *  RELATIVE to the cross-section. That gap can close because the company
 *  recovered, or because everything else fell too — and in the second case the
 *  position is sold at a loss while the company itself has not yet done
 *  anything. On a short clock that is most of the book: a session's exits can
 *  average a fraction of a percent, held for hours, where the round trip is
 *  mostly friction.
 *
 *  Some of those names would have been positive two or three sessions later.
 *  Not all of them, and not by opinion — the desk already measures which ones,
 *  per company, from up to five years of its own daily history:
 *  H.reversionEvents() finds every drop of at least 1.5 sigma, enters the day
 *  after, and records the return five trading days later. That yields, for
 *  each name, how many such events it has had, the mean forward return, and
 *  how often it was positive. H.shrinkReversion() then pulls each name toward
 *  the roster-wide pooled mean, so eight events is not treated as a track
 *  record.
 *
 *  That statistic was already computed on every cycle and used only to scale
 *  the expected edge inside the cost hurdle. This module gives it a second
 *  job: deciding which positions have earned a longer leash.
 *
 *  WHAT PATIENCE GRANTS, AND WHAT IT NEVER TOUCHES
 *  ------------------------------------------------------------------------
 *  GRANTS, and only these two things:
 *    · the time stop moves out to the horizon the evidence was measured over;
 *    · the RANK exit is suppressed WHILE THE POSITION IS UNDERWATER and still
 *      inside that window.
 *  Everything protective is untouched and fires exactly as before: the hard
 *  stop, the trailing stop, the profit target, the earnings exit and a
 *  corroborated adverse company finding. Patience is permission to wait out a
 *  relative-rank recovery, never permission to sit in a real loss.
 *
 *  The rank exit still fires immediately for a patient position that is UP.
 *  The complaint patience answers is selling at a loss because the rest of the
 *  market moved; a profitable rank exit is the strategy working.
 *
 *  THE HONESTY RULE THAT MAKES THIS SAFE
 *  ------------------------------------------------------------------------
 *  Patience is decided ONCE, at entry, from evidence that existed before the
 *  position did, and is then immutable. It can never be granted to a position
 *  that is already losing. Deciding later would turn this into "move the
 *  losers somewhere they cannot be sold", which is the precise behaviour the
 *  hard stop exists to prevent, and it would quietly destroy the measured
 *  record by letting hindsight choose which trades get more time.
 *
 *  It is also bounded by CAPITAL, not by count: the sum of what patient
 *  positions cost may not exceed a declared share of the account. A name that
 *  qualifies when the sleeve is full is simply taken on ordinary terms.
 * ---------------------------------------------------------------------------
 */

"use strict";

const H = require("./_investorHistory");

/* Defaults are clamped, never trusted. The grant is tied to the horizon the
   reversion evidence is actually measured over — granting more sessions than
   the statistic covers would be extrapolating past the measurement. */
const DEFAULTS = Object.freeze({
  enabled: true,
  sleevePctOfNav: 12,
  minEvents: 8,
  minWinRate: 0.55,
  minForwardPct: 0.5,
  minOwnWeight: 0.25,
  grantSessions: H.REVERSION_HORIZON || 5,
});

const LIMITS = Object.freeze({
  sleevePctOfNav: { min: 0, max: 25 },
  minEvents: { min: 3, max: 60 },
  minWinRate: { min: 0.4, max: 0.95 },
  minForwardPct: { min: 0, max: 10 },
  minOwnWeight: { min: 0, max: 0.9 },
  /* Never beyond twice the measured horizon, whatever the config says. */
  grantSessions: { min: 1, max: 2 * (H.REVERSION_HORIZON || 5) },
});

function clamp(key, value, dflt) {
  const lim = LIMITS[key];
  const n = Number(value);
  if (!lim) return dflt;
  if (!Number.isFinite(n)) return dflt;
  return Math.min(lim.max, Math.max(lim.min, n));
}

/** The frozen policy, merged from the strategy with clamped defaults. */
function policyFrom(strategy) {
  const raw = (strategy && strategy.exploratoryAuto
    && strategy.exploratoryAuto.activity
    && strategy.exploratoryAuto.activity.patience) || {};
  return {
    enabled: raw.enabled !== false,
    sleevePctOfNav: clamp("sleevePctOfNav", raw.sleevePctOfNav, DEFAULTS.sleevePctOfNav),
    minEvents: Math.round(clamp("minEvents", raw.minEvents, DEFAULTS.minEvents)),
    minWinRate: clamp("minWinRate", raw.minWinRate, DEFAULTS.minWinRate),
    minForwardPct: clamp("minForwardPct", raw.minForwardPct, DEFAULTS.minForwardPct),
    minOwnWeight: clamp("minOwnWeight", raw.minOwnWeight, DEFAULTS.minOwnWeight),
    grantSessions: Math.round(clamp("grantSessions", raw.grantSessions, DEFAULTS.grantSessions)),
    horizonSessions: H.REVERSION_HORIZON || 5,
  };
}

/* ── PURE: does this company's own record earn patience? ─────────────────
 * `reversion` is one entry of H.shrinkReversion().perSymbol, with the
 * multiplier the cycle attaches: { n, rawPct, winRate, pooledPct, shrunkPct,
 * weightOwn }. Every threshold is about the company's OWN evidence — a name
 * carried entirely by the pooled average has demonstrated nothing. */
function assess(reversion, policy, { historyContext = null } = {}) {
  const reasons = [];
  const p = policy || policyFrom(null);
  if (!p.enabled) return { eligible: false, reasons: ["patience sleeve is disabled"], score: 0 };
  if (!reversion || !Number.isFinite(Number(reversion.n))) {
    return { eligible: false, reasons: ["no reversion record for this company"], score: 0 };
  }
  const n = Number(reversion.n);
  const winRate = Number(reversion.winRate);
  const shrunkPct = Number(reversion.shrunkPct);
  const weightOwn = Number(reversion.weightOwn);

  if (!(n >= p.minEvents)) {
    reasons.push(`only ${n} comparable drop${n === 1 ? "" : "s"} on record, needs ${p.minEvents}`);
  }
  if (!(weightOwn >= p.minOwnWeight)) {
    reasons.push(`its own record carries only ${Math.round((weightOwn || 0) * 100)}% of the estimate, needs ${Math.round(p.minOwnWeight * 100)}%`);
  }
  if (!Number.isFinite(winRate) || !(winRate >= p.minWinRate)) {
    reasons.push(`recovered ${Number.isFinite(winRate) ? Math.round(winRate * 100) : 0}% of the time, needs ${Math.round(p.minWinRate * 100)}%`);
  }
  if (!Number.isFinite(shrunkPct) || !(shrunkPct >= p.minForwardPct)) {
    reasons.push(`expected ${Number.isFinite(shrunkPct) ? shrunkPct.toFixed(2) : "0.00"}% over ${p.horizonSessions} sessions, needs ${p.minForwardPct}%`);
  }
  /* A name in a genuine downtrend is not in a "short-term slump"; the entry
     gate already refuses those, but patience must not be the way one slips
     through if that gate is ever relaxed. */
  if (historyContext && historyContext.ok && historyContext.downtrend === true) {
    reasons.push("it is in a months-long downtrend, not a short-term slump");
  }

  const eligible = reasons.length === 0;
  /* A 0-100 reading of how far past the bars it sits, for display and for
     ordering when the sleeve has room for some but not all. */
  const score = eligible ? Math.max(0, Math.min(100, Math.round(
    40 * Math.min(1, (winRate - p.minWinRate) / Math.max(0.01, 0.95 - p.minWinRate))
    + 40 * Math.min(1, (shrunkPct - p.minForwardPct) / Math.max(0.01, 3))
    + 20 * Math.min(1, weightOwn)))) : 0;

  return {
    eligible, score,
    reasons: eligible ? [
      `recovered ${Math.round(winRate * 100)}% of the time after ${n} similar drops`,
      `averaging ${shrunkPct.toFixed(2)}% over the following ${p.horizonSessions} sessions`,
    ] : reasons,
    evidence: { events: n, winRate: Number.isFinite(winRate) ? winRate : null,
      expectedPct: Number.isFinite(shrunkPct) ? shrunkPct : null,
      ownWeight: Number.isFinite(weightOwn) ? weightOwn : null,
      horizonSessions: p.horizonSessions },
    grantSessions: p.grantSessions,
  };
}

/* ── how much of the sleeve is already committed ─────────────────────────
 * Measured at COST, not at market: a patient position that has fallen must
 * not free up room for another one, or a bad day quietly widens the sleeve. */
function sleeveUsage(positions, pendingOrders, navUsd, policy) {
  const p = policy || policyFrom(null);
  const nav = Math.max(0, Number(navUsd) || 0);
  const capUsd = nav * p.sleevePctOfNav / 100;
  let usedUsd = 0;
  const symbols = [];
  for (const row of (positions || [])) {
    if (!row || row.open !== true || !isPatient(row)) continue;
    const qty = Number(row.qty) || 0;
    const entry = Number(row.entryPriceUsd != null ? row.entryPriceUsd : row.avgPrice) || 0;
    usedUsd += qty * entry;
    symbols.push(row.symbol);
  }
  /* An order that has not filled yet is still a claim on the sleeve. */
  for (const o of (pendingOrders || [])) {
    if (!o || !["proposed", "approved"].includes(o.status) || !isPatient(o)) continue;
    usedUsd += (Number(o.qty) || 0) * (Number(o.refPriceUsd) || 0);
    symbols.push(o.symbol);
  }
  const roomUsd = Math.max(0, capUsd - usedUsd);
  return { capUsd: Number(capUsd.toFixed(2)), usedUsd: Number(usedUsd.toFixed(2)),
    roomUsd: Number(roomUsd.toFixed(2)),
    usedPctOfNav: nav > 0 ? Number((100 * usedUsd / nav).toFixed(2)) : 0,
    sleevePctOfNav: p.sleevePctOfNav, symbols };
}

/** Is this order or position stamped patient? */
function isPatient(row) {
  return !!(row && row.patience && row.patience.granted === true);
}

/* ── the grant, decided once at entry ────────────────────────────────────
 * Returns the immutable stamp to store on the order, or null when the name
 * does not qualify or the sleeve has no room for this size. */
function grant({ reversion, historyContext, policy, proposedUsd, positions, pendingOrders, navUsd, nowMs = Date.now() }) {
  const p = policy || policyFrom(null);
  if (!p.enabled) return null;
  const verdict = assess(reversion, p, { historyContext });
  if (!verdict.eligible) return null;
  const usage = sleeveUsage(positions, pendingOrders, navUsd, p);
  /* Partial admission would put a position in the sleeve at a size the sleeve
     cannot actually hold, so the whole notional has to fit. */
  if (!(Number(proposedUsd) > 0) || Number(proposedUsd) > usage.roomUsd) return null;
  return {
    granted: true,
    grantedAtMs: nowMs,
    grantSessions: p.grantSessions,
    score: verdict.score,
    evidence: verdict.evidence,
    reasons: verdict.reasons,
    policy: { sleevePctOfNav: p.sleevePctOfNav, minEvents: p.minEvents,
      minWinRate: p.minWinRate, minForwardPct: p.minForwardPct,
      minOwnWeight: p.minOwnWeight, grantSessions: p.grantSessions },
    sleeveAtGrant: { usedUsd: usage.usedUsd, capUsd: usage.capUsd, proposedUsd: Number(proposedUsd) },
  };
}

/* ── what the exit policy needs to know ──────────────────────────────────
 * Translated into the two concessions exitSignal understands, and nothing
 * else. `rankSuppressed` is deliberately conditional on being UNDERWATER: a
 * patient position that is up takes its rank exit like any other, because the
 * complaint patience answers is selling at a loss when the rest of the market
 * moved, not banking a gain. */
function exitTerms(position, { heldDays = 0, pnlPct = null } = {}) {
  if (!isPatient(position)) return null;
  const stamp = position.patience;
  const grantSessions = Math.max(1, Number(stamp.grantSessions) || DEFAULTS.grantSessions);
  const withinWindow = Number(heldDays) < grantSessions;
  const underwater = Number.isFinite(Number(pnlPct)) && Number(pnlPct) < 0;
  return {
    maxHoldDays: grantSessions,
    rankSuppressed: withinWindow && underwater,
    withinWindow, underwater, grantSessions,
    note: withinWindow
      ? (underwater
        ? `held for its recovery window — ${grantSessions} sessions, ${Number(heldDays).toFixed(1)} used`
        : "in the patience sleeve, but it is up, so the ordinary exits apply")
      : "its recovery window has passed; ordinary exits apply",
  };
}

module.exports = { DEFAULTS, LIMITS, policyFrom, assess, sleeveUsage, isPatient,
  grant, exitTerms };
