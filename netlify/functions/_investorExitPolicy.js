"use strict";

const EXIT_RANK = 0.50;

function exitSignal(rank, heldDays, cfg, opts = {}) {
  const exitRank = cfg.exitRank ?? EXIT_RANK;
  /* THE PATIENCE SLEEVE (_investorPatience). A position whose own history says
     it recovers over days rather than hours may be granted two concessions,
     decided at entry and immutable afterwards: a time stop moved out to the
     horizon that evidence was measured over, and suppression of the RANK exit
     while it is underwater inside that window.

     Nothing below section 6 changes. The hard stop, the trailing stop, the
     profit target, the earnings exit and a corroborated adverse finding all
     fire exactly as they would for any other position — patience is
     permission to wait out a relative-rank recovery, never permission to sit
     in a real loss. */
  const patience = opts.patience || null;
  const maxDays = patience && Number(patience.maxHoldDays) > 0
    ? Number(patience.maxHoldDays) : (cfg.maxHoldDays ?? 10);
  const { mark, entry, peak, earningsInDays } = opts;
  /* TOUCH BEFORE CLOSE (blueprint §8.5, invariant I-2; D-2). A bar can trade
     through a level and close back on the other side of it, so every
     protective level is tested against the bar's LOW and every target
     against its HIGH. `mark` (the close) is retained only for reporting
     pnlPct. A caller that supplies no high/low falls back to the close and
     the verdict says so, so a close-only evaluation can never be mistaken
     for a touch-based one. */
  const haveHigh = Number.isFinite(Number(opts.barHigh)), haveLow = Number.isFinite(Number(opts.barLow));
  const barHigh = haveHigh ? Number(opts.barHigh) : mark;
  const barLow = haveLow ? Number(opts.barLow) : mark;
  const touchBasis = haveHigh || haveLow ? "bar_high_low" : "close_only";

  const havePrice = Number.isFinite(mark) && Number.isFinite(entry) && entry > 0;
  const haveRank = Number.isFinite(rank);
  const pnlPct = havePrice ? ((mark - entry) / entry) * 100 : null;
  const r2 = (x) => Number(Number(x).toFixed(2));

  if (havePrice) {
    const lowPnlPct = ((barLow - entry) / entry) * 100;
    const highPnlPct = ((barHigh - entry) / entry) * 100;
    const stopPct = cfg.stopLossPct ?? -8;
    const trailPct = cfg.trailingStopPct ?? -4;
    const takePct = cfg.takeProfitPct ?? null;
    const pe = cfg.pullbackExit, leg = opts.pullbackLeg;
    const legOk = !!(pe && leg && Number(leg.legHigh) > Number(leg.legLow));

    /* Every level is tested FIRST and the verdict chosen AFTERWARDS, so a bar
       that touched both an adverse level and a favourable one resolves
       adversely — never favourably — and is flagged UNSCORABLE for
       performance scoring. The precedence inside each group is unchanged:
       hard stop, trailing stop, pullback failure; pullback target, profit
       target. */
    const adverse = [], favourable = [];

    // 1. HARD STOP. The floor under every position, tested at the bar low.
    if (lowPnlPct <= stopPct) {
      adverse.push({ kind: "stop_loss", urgent: true,
        reason: `touched ${lowPnlPct.toFixed(1)}% at the bar low — hard stop at ${stopPct}%` });
    }
    // 2. TRAILING STOP, once a position has actually made money. Protects a
    //    winner from round-tripping back to flat. The peak is the observed
    //    high-water mark; the giveback is measured at the bar low.
    if (Number.isFinite(peak) && peak > entry) {
      const fromPeak = ((barLow - peak) / peak) * 100;
      const peakGainPct = ((peak - entry) / entry) * 100;
      if (peakGainPct >= (cfg.trailingArmsAtPct ?? 3) && fromPeak <= trailPct) {
        adverse.push({ kind: "trailing_stop", urgent: true,
          reason: `up ${peakGainPct.toFixed(1)}% at best, bar low ${fromPeak.toFixed(1)}% off that peak — trailing stop` });
      }
    }
    // 3a. STRUCTURAL PULLBACK EXITS (policy Q). The leg recorded at entry
    //     defines both ends of the trade: the prior swing high is the target
    //     (tested at the bar high) and giving back more than the declared
    //     share of the leg (tested at the bar low) means the level failed.
    if (legOk) {
      const retr = (Number(leg.legHigh) - barLow) / (Number(leg.legHigh) - Number(leg.legLow));
      const failAt = Number(pe.failRetracement) || 0.786;
      if (retr >= failAt) {
        adverse.push({ kind: "pullback_failed", urgent: true,
          reason: `gave back ${Math.round(retr * 100)}% of the up-leg at the bar low (limit ${Math.round(failAt * 100)}%) — the level failed` });
      }
      if (barHigh >= Number(leg.legHigh)) {
        favourable.push({ kind: "pullback_target", urgent: false,
          reason: `reached the prior swing high ${Number(leg.legHigh).toFixed(2)} at the bar high — pullback target` });
      }
    }
    // 3. PROFIT TARGET, tested at the bar high.
    if (takePct != null && highPnlPct >= takePct) {
      favourable.push({ kind: "take_profit", urgent: false,
        reason: `touched ${highPnlPct.toFixed(1)}% at the bar high — profit target ${takePct}%` });
    }

    const collision = adverse.length && favourable.length
      ? { adverse: adverse[0].kind, favourable: favourable[0].kind, resolution: "adverse", scoring: "UNSCORABLE" }
      : null;
    const chosen = adverse[0] || favourable[0] || null;
    if (chosen) {
      return { exit: true, urgent: chosen.urgent, kind: chosen.kind,
        reason: chosen.reason + (collision ? " — same-bar collision with a favourable level, resolved adversely" : ""),
        pnlPct: r2(pnlPct), touchBasis, barHigh, barLow,
        touchedPct: r2(adverse.length ? lowPnlPct : highPnlPct),
        ...(collision ? { sameBarCollision: true, collision, scorable: false } : {}) };
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

  /* 6. The signal exit: the move we were trading has reverted.

     A LOSS IS NEVER CLOSED BY A STATISTIC. This exit used to fire on the rank
     alone, so a position bought an hour earlier was sold because the gap it
     was trading had closed RELATIVE TO THE CROSS-SECTION — which happens just
     as readily when every other company falls further as when this one
     recovers. The record it produced is the whole argument: thirty-eight round
     trips, twenty up and eighteen down, holds of one to six hours, and a net
     result of roughly zero before friction. Nearly every one of them exited
     here, at a fraction of a percent either side of flat.

     A position that is DOWN is now held until a rule about MONEY closes it —
     the hard stop, the earnings print, a corroborated adverse finding, or the
     time stop. A position that is UP still takes this exit, because banking a
     gain when the reason for the trade has gone is exactly right. */
  /* Two reasons the rank exit may be withheld, and NEITHER may skip the time
     stop below — that would turn "do not realise a loss on a statistic" into
     "hold a loser forever", which is worse than the behaviour being fixed.
     Both paths fall through to section 7. */
  let heldReason = null, patienceHeld = false;
  if (haveRank && rank >= exitRank) {
    /* Patience first: it has the more specific explanation. Granted at entry
       from this company's own recovery record, and only while it is DOWN and
       inside its window. */
    if (patience && patience.rankSuppressed === true) {
      patienceHeld = true;
      heldReason = `rank ${rank.toFixed(3)} crossed exit ${exitRank}, but this company recovers from drops like this`
        + ` — ${patience.note || `holding for up to ${maxDays} sessions`}`;
    } else if (havePrice && pnlPct < 0 && cfg.holdLosersThroughRankExit !== false) {
      heldReason = `rank ${rank.toFixed(3)} crossed exit ${exitRank}, but this position is down `
        + `${Math.abs(pnlPct).toFixed(1)}% — a loss is closed by the stop, an earnings date or the `
        + `time limit, never by the ranking alone`;
    } else {
      return { exit: true, urgent: false, reason: `rank ${rank.toFixed(3)} crossed exit ${exitRank}`,
               kind: "signal", pnlPct: pnlPct != null ? Number(pnlPct.toFixed(2)) : null };
    }
  }

  // 7. Time stop — reached whether or not the rank exit was suppressed.
  if (heldDays >= maxDays) {
    return { exit: true, urgent: false,
             reason: patience ? `max hold ${maxDays} sessions reached (extended recovery window used up)`
               : `max hold ${maxDays}d reached`,
             kind: "time", pnlPct: pnlPct != null ? Number(pnlPct.toFixed(2)) : null };
  }

  return {
    exit: false, kind: null,
    pnlPct: pnlPct != null ? Number(pnlPct.toFixed(2)) : null,
    priceRulesEvaluated: havePrice, touchBasis,
    /* A withheld rank exit has a specific explanation and must not be
       overwritten by the generic "nothing fired" line. */
    ...(patienceHeld ? { patienceHeld: true } : {}),
    ...(heldReason && !patienceHeld ? { heldThroughRank: true } : {}),
    reason: heldReason
      || `${haveRank ? `rank ${rank.toFixed(3)} below exit ${exitRank}` : "cross-sectional rank unavailable"}, held ${heldDays}d`
         + (havePrice ? `, ${pnlPct >= 0 ? "up" : "down"} ${Math.abs(pnlPct).toFixed(1)}%`
                      : ", NO PRICE AVAILABLE so the stop could not be checked"),
  };
}

/* THE PRESET LEVELS, in dollars.
 *
 * exitSignal() above is the rule engine and stays authoritative. This turns
 * the same configuration into the price levels a person can read off a
 * position — "stop at 91.20, trailing stop arms above 102.10, target none,
 * two sessions left" — so the strike pass can display exactly what it is
 * watching for and the operator can see the preset targets rather than infer
 * them from percentages. Display and telemetry only: no decision reads these
 * numbers back; exitSignal recomputes from the configuration every pass. */
function exitLevels(cfg = {}, { entry, peak = null, heldDays = null, earningsInDays = null } = {}) {
  const e = Number(entry);
  if (!(e > 0)) return null;
  const r = (x) => Number(x.toFixed(4));
  const stopPct = cfg.stopLossPct ?? -8;
  const trailPct = cfg.trailingStopPct ?? -4;
  const armPct = cfg.trailingArmsAtPct ?? 3;
  const takePct = cfg.takeProfitPct ?? null;
  const maxDays = cfg.maxHoldDays ?? 10;
  const pk = Number(peak) > e ? Number(peak) : null;
  const trailArmed = pk != null && ((pk - e) / e) * 100 >= armPct;
  return {
    stopUsd: r(e * (1 + stopPct / 100)), stopPct,
    trailArmUsd: r(e * (1 + armPct / 100)), trailArmed,
    trailStopUsd: trailArmed ? r(pk * (1 + trailPct / 100)) : null, trailPct,
    peakUsd: pk,
    targetUsd: takePct != null ? r(e * (1 + takePct / 100)) : null, takePct,
    maxHoldDays: maxDays,
    sessionsLeft: Number.isFinite(Number(heldDays)) ? Math.max(0, Number((maxDays - Number(heldDays)).toFixed(2))) : null,
    earningsInDays: Number.isFinite(Number(earningsInDays)) ? Number(earningsInDays) : null,
    exitBeforeEarningsDays: cfg.exitBeforeEarningsDays ?? 2,
    computedAtMs: Date.now(),
  };
}

module.exports = { exitSignal, exitLevels };
