"use strict";

const EXIT_RANK = 0.50;

function exitSignal(rank, heldDays, cfg, opts = {}) {
  const exitRank = cfg.exitRank ?? EXIT_RANK;
  const maxDays = cfg.maxHoldDays ?? 10;
  const { mark, entry, peak, earningsInDays } = opts;

  const havePrice = Number.isFinite(mark) && Number.isFinite(entry) && entry > 0;
  const haveRank = Number.isFinite(rank);
  const pnlPct = havePrice ? ((mark - entry) / entry) * 100 : null;

  if (havePrice) {
    // 1. HARD STOP. The floor under every position.
    const stopPct = cfg.stopLossPct ?? -8;
    if (pnlPct <= stopPct) {
      return { exit: true, urgent: true, reason: `down ${pnlPct.toFixed(1)}% — hard stop at ${stopPct}%`,
               kind: "stop_loss", pnlPct: Number(pnlPct.toFixed(2)) };
    }

    // 2. TRAILING STOP, once a position has actually made money. Protects a
    //    winner from round-tripping back to flat, which is the most common way
    //    a short-horizon reversion trade wastes a correct call.
    const trailPct = cfg.trailingStopPct ?? -4;
    if (Number.isFinite(peak) && peak > entry) {
      const fromPeak = ((mark - peak) / peak) * 100;
      const peakGainPct = ((peak - entry) / entry) * 100;
      if (peakGainPct >= (cfg.trailingArmsAtPct ?? 3) && fromPeak <= trailPct) {
        return { exit: true, urgent: true,
                 reason: `up ${peakGainPct.toFixed(1)}% at best, now ${fromPeak.toFixed(1)}% off that peak — trailing stop`,
                 kind: "trailing_stop", pnlPct: Number(pnlPct.toFixed(2)) };
      }
    }

    // 3a. STRUCTURAL PULLBACK EXITS (policy Q). The leg recorded at entry
    //     defines both ends of the trade: the prior swing high is the target
    //     ("if it continues to here, great"), and giving back more than the
    //     declared share of the leg means the level failed ("if not, it most
    //     likely reverses"). Only a variant that declares pullbackExit and a
    //     position that recorded its leg reach this branch.
    const pe = cfg.pullbackExit, leg = opts.pullbackLeg;
    if (pe && leg && Number(leg.legHigh) > Number(leg.legLow)) {
      if (mark >= Number(leg.legHigh)) {
        return { exit: true, urgent: false, reason: `reached the prior swing high ${Number(leg.legHigh).toFixed(2)} — pullback target`,
                 kind: "pullback_target", pnlPct: Number(pnlPct.toFixed(2)) };
      }
      const retr = (Number(leg.legHigh) - mark) / (Number(leg.legHigh) - Number(leg.legLow));
      const failAt = Number(pe.failRetracement) || 0.786;
      if (retr >= failAt) {
        return { exit: true, urgent: true, reason: `gave back ${Math.round(retr * 100)}% of the up-leg (limit ${Math.round(failAt * 100)}%) — the level failed`,
                 kind: "pullback_failed", pnlPct: Number(pnlPct.toFixed(2)) };
      }
    }

    // 3. PROFIT TARGET. The research expects partial reversion, not full.
    const takePct = cfg.takeProfitPct ?? null;
    if (takePct != null && pnlPct >= takePct) {
      return { exit: true, urgent: false, reason: `up ${pnlPct.toFixed(1)}% — profit target ${takePct}%`,
               kind: "take_profit", pnlPct: Number(pnlPct.toFixed(2)) };
    }
  }

  // 4. A stored company-intelligence dossier may request a risk-reducing exit
  // only after deterministic corroboration has met the high critical bar.
  // The model never reaches this branch directly.
  const intelligencePolicy = opts.intelligencePolicy;
  if (intelligencePolicy && intelligencePolicy.criticalExit === true
      && intelligencePolicy.monitored === true
      && intelligencePolicy.fresh === true
      && intelligencePolicy.complete === true) {
    return { exit: true, urgent: true,
      reason: intelligencePolicy.exitReason || "corroborated material adverse company event",
      kind: "intelligence_critical", intelligenceEventId: intelligencePolicy.exitEventId || null,
      pnlPct: pnlPct != null ? Number(pnlPct.toFixed(2)) : null };
  }

  /* 5. EARNINGS. The entry gate refuses to OPEN near an earnings date, but for
        most of this system's life nothing made it CLOSE before one. Holding a
        mean-reversion position through an earnings print is a coin flip with a
        20% standard deviation attached, and it undoes the entry gate entirely. */
  if (Number.isFinite(earningsInDays) && earningsInDays <= (cfg.exitBeforeEarningsDays ?? 2)) {
    return { exit: true, urgent: true, reason: `earnings in ${earningsInDays} day(s) — closing before the print`,
             kind: "earnings_exit", pnlPct: pnlPct != null ? Number(pnlPct.toFixed(2)) : null };
  }

  // 6. The signal exit: the move we were trading has reverted.
  if (haveRank && rank >= exitRank) {
    return { exit: true, urgent: false, reason: `rank ${rank.toFixed(3)} crossed exit ${exitRank}`,
             kind: "signal", pnlPct: pnlPct != null ? Number(pnlPct.toFixed(2)) : null };
  }

  // 7. Time stop.
  if (heldDays >= maxDays) {
    return { exit: true, urgent: false, reason: `max hold ${maxDays}d reached`,
             kind: "time", pnlPct: pnlPct != null ? Number(pnlPct.toFixed(2)) : null };
  }

  return {
    exit: false, kind: null,
    pnlPct: pnlPct != null ? Number(pnlPct.toFixed(2)) : null,
    priceRulesEvaluated: havePrice,
    reason: `${haveRank ? `rank ${rank.toFixed(3)} below exit ${exitRank}` : "cross-sectional rank unavailable"}, held ${heldDays}d`
          + (havePrice ? `, ${pnlPct >= 0 ? "up" : "down"} ${Math.abs(pnlPct).toFixed(1)}%`
                       : ", NO PRICE AVAILABLE so the stop could not be checked"),
  };
}

module.exports = { exitSignal };
