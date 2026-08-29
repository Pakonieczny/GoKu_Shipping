/*  netlify/functions/_investorSignal.js  (v1.0)
 *  ---------------------------------------------------------------------------
 *  Investor_AI — the signal engine.
 *
 *  Every number in this file traces to a specific finding in the research
 *  thread. The citations are inline because a future maintainer must be able
 *  to tell a calibrated parameter from a guess.
 *
 *  1. RESIDUAL, NOT RAW RETURNS.
 *     Blitz/Huij/Lansdorp/Verbeek: controlling for factor exposure roughly
 *     doubles the Sharpe (0.62 -> 1.28) and lifts break-even cost from 10bp to
 *     56bp. Da/Liu/Schaumburg: raw reversal alpha 0.33%/mo (t=1.37, NOT
 *     significant) vs within-industry residual 1.34%/mo (t=9.28). A 4% raw
 *     move on a day the sector rallied 3.7% is a 0.3% residual move and is
 *     not a signal. This is the single highest-value decision in the design.
 *
 *  2. BUY/HOLD SPREAD ON THE EXIT.
 *     de Groot/Huij/Zhou: identical signal, 100 largest names, standard
 *     rebalancing nets +31.5bp/wk; entering at the decile threshold but not
 *     exiting until rank crosses 50% nets +53.1bp/wk with turnover cut from
 *     ~700% to ~330%. Novy-Marx/Velikov independently call this the single
 *     most effective simple cost mitigation (~41% turnover, ~42% cost).
 *     The exit rule is worth more than any entry refinement.
 *
 *  3. VOLATILITY CONDITIONING.
 *     Nagel: in liquid names reversal is near-zero UNCONDITIONALLY but a 1pp
 *     rise in normalized VIX predicts +0.22pp of daily return, monthly R^2 56%.
 *     So size with volatility rather than trading a flat book.
 *
 *  4. DISPERSION REGIME GATE.
 *     When implied correlation spikes, every name trades as one asset and an
 *     idiosyncratic strategy has nothing to work with. COR3M is the dial.
 *
 *  5. ATTENTION CLASSIFICATION DECIDES DIRECTION.
 *     Ben-Rephael/Da/Easton/Israelsen: 8-Ks drawing INSTITUTIONAL attention
 *     show no under- or over-reaction (already priced); 8-Ks drawing RETAIL
 *     attention reverse, peaking days t+7..t+8. Same filing, opposite trade.
 *     Barber/Huang/Odean/Schwarz: top retail-herded names -4.7% over 20 days.
 *
 *  6. COST IS THE BINDING CONSTRAINT, NOT SIGNAL QUALITY.
 *     ~5-day holds are ~400% one-sided monthly turnover. Every candidate
 *     carries its own modelled round-trip cost and must clear it by a margin
 *     before it is proposed at all.
 * ---------------------------------------------------------------------------
 */

"use strict";

const M = require("./_investorMarket");
const H = require("./_investorHistory");

/* ── small numeric helpers (fixed, defensive, no deps) ─────────────────── */
const mean = (a) => (a.length ? a.reduce((s, x) => s + x, 0) / a.length : 0);
function stdev(a) {
  if (a.length < 2) return 0;
  const m = mean(a);
  return Math.sqrt(a.reduce((s, x) => s + (x - m) * (x - m), 0) / (a.length - 1));
}
function pctReturns(closes) {
  const out = [];
  for (let i = 1; i < closes.length; i++) {
    const p = closes[i - 1], c = closes[i];
    if (p > 0 && c > 0) out.push((c - p) / p); else out.push(0);
  }
  return out;
}
/** OLS slope/intercept of y on x. Returns beta and alpha. */
function ols(y, x) {
  const n = Math.min(y.length, x.length);
  if (n < 10) return { beta: 1, alpha: 0, n };
  const ys = y.slice(-n), xs = x.slice(-n);
  const my = mean(ys), mx = mean(xs);
  let num = 0, den = 0;
  for (let i = 0; i < n; i++) { num += (xs[i] - mx) * (ys[i] - my); den += (xs[i] - mx) ** 2; }
  const beta = den > 0 ? num / den : 1;
  return { beta, alpha: my - beta * mx, n };
}

/* ── sector resolution ─────────────────────────────────────────────────
 * Sectors come from the UNIVERSE file, not from a table hardcoded here — the
 * roster is the thing that changes, and a 350-name roster against a 46-name
 * hardcoded map silently dumped 300 names into a single "other" bucket, which
 * destroys the factor model. The cycle calls setSectorMap() once per run.
 */
let SECTOR = {};
function setSectorMap(map) { SECTOR = map || {}; return Object.keys(SECTOR).length; }
function sectorOf(sym) { return SECTOR[sym] || "other"; }

/* ── 1. RESIDUAL RETURN ENGINE ─────────────────────────────────────────── */
/**
 * Build market and sector factor return series from the whole panel, then
 * regress each symbol on them and return the residual series. Equal-weighted
 * cross-sectional means are the factors — crude versus a full risk model, but
 * it is the same shape as within-industry residual reversal and it uses only
 * data the system actually has.
 */
function residualPanel(panel, opts = {}) {
  // panel: { SYM: [{t,o,h,l,c,v}, ...] }  aligned by index (same cadence)
  const symbols = Object.keys(panel).filter((s) => (panel[s] || []).length > 12);
  if (symbols.length < 4) {
    return { symbols: [], residuals: {}, betas: {}, note: "insufficient_panel_breadth" };
  }
  const rets = {};
  let minLen = Infinity;
  for (const s of symbols) {
    rets[s] = pctReturns(panel[s].map((b) => b.c));
    minLen = Math.min(minLen, rets[s].length);
  }
  if (!isFinite(minLen) || minLen < 12) {
    return { symbols: [], residuals: {}, betas: {}, note: "insufficient_history" };
  }
  for (const s of symbols) rets[s] = rets[s].slice(-minLen);

  /* MINIMUM SECTOR SIZE. A sector holding one or two roster names would take a
     full 1/k slice of the sector-balanced market factor, so a single stock
     could become ~20% of "the market" — the same over-weighting bug the
     sector-balanced factor exists to fix, arriving from the other direction.
     Undersized sectors are pooled into a shared "misc" bucket instead. */
  const MIN_SECTOR = 3;
  const rawSectors = {};
  for (const s of symbols) (rawSectors[sectorOf(s)] ||= []).push(s);
  const effSector = {};
  for (const [sec, members] of Object.entries(rawSectors)) {
    for (const s of members) effSector[s] = members.length >= MIN_SECTOR ? sec : "misc";
  }
  const bySector = {};
  for (const s of symbols) (bySector[effSector[s]] ||= []).push(s);
  const sectorIds = Object.keys(bySector);

  /* ESTIMATION WINDOW vs EVENT WINDOW — the correction that matters most.
   *
   * Fitting betas on the WHOLE series, event included, lets the event set the
   * beta: a name that drops 1.2%/bar while the market drops 0.47%/bar simply
   * regresses to beta 2.5, and its residual collapses to nothing. The self-test
   * caught exactly this — COIN's deliberate idiosyncratic drop produced a market
   * beta of 2.43 and a residual of -3bp, i.e. the signal erased itself.
   *
   * Standard event-study practice, applied here: betas are estimated ONLY on
   * the estimation window (everything before the signal window), then applied
   * to the event window to produce residuals. The event can no longer influence
   * the coefficients used to measure it.
   */
  const evWin = Math.max(1, Number(opts.signalWindow) || 12);
  const estEnd = Math.max(12, minLen - evWin);   // index where the event window begins
  const est = (arr) => arr.slice(0, estEnd);

  /* FACTOR CONSTRUCTION — two corrections that the self-test forced.
   *
   * (a) LEAVE-ONE-OUT. A naive mean includes the symbol being regressed, so a
   *     name with a large idiosyncratic move partly IS the factor and its beta
   *     explodes (the first self-test produced a market beta of 5.5).
   *
   * (b) SECTOR-BALANCED MARKET FACTOR. An equal-weighted mean across NAMES lets
   *     an over-represented sector dominate: with 8 semis in an 18-name panel a
   *     sector-wide semi selloff leaked into the "market" factor, market betas
   *     came out near 2.2, and the sector move was never removed — a whole
   *     sector looked idiosyncratically oversold. The market factor is
   *     therefore the mean OF SECTOR MEANS, so each sector contributes once
   *     regardless of how many roster names it holds.
   */
  const sectorMeanAt = {};                       // sec -> [meanRet per step]
  const sectorSumAt  = {};
  for (const [sec, members] of Object.entries(bySector)) {
    const sums = [], means = [];
    for (let i = 0; i < minLen; i++) {
      let acc = 0; for (const s of members) acc += rets[s][i];
      sums.push(acc); means.push(acc / members.length);
    }
    sectorSumAt[sec] = sums; sectorMeanAt[sec] = means;
  }

  const residuals = {}, betas = {};
  for (const s of symbols) {
    const sec = effSector[s];
    const members = bySector[sec];
    const peers = members.length - 1;

    // (b)+(a): market factor = mean of sector means, with s removed from its own
    // sector's mean. Sectors that consist only of s drop out entirely.
    const mkt = [];
    for (let i = 0; i < minLen; i++) {
      let acc = 0, k = 0;
      for (const sid of sectorIds) {
        if (sid === sec) {
          if (peers >= 1) { acc += (sectorSumAt[sid][i] - rets[s][i]) / peers; k += 1; }
        } else { acc += sectorMeanAt[sid][i]; k += 1; }
      }
      mkt.push(k ? acc / k : 0);
    }

    // sector factor excluding s, orthogonalised against that same market factor
    let secResid = null;
    if (peers >= 2) {
      const secF = [];
      for (let i = 0; i < minLen; i++) secF.push((sectorSumAt[sec][i] - rets[s][i]) / peers);
      // Orthogonalisation coefficient ALSO from the estimation window. Fitting it
      // on the full series lets a sector-wide event inflate it (the event makes
      // the sector and the market co-move), which distorts the sector factor
      // exactly when it is needed most.
      const bSM = ols(est(secF), est(mkt)).beta;
      secResid = secF.map((f, i) => f - bSM * mkt[i]);
    }

    // Betas from the estimation window; residuals over the full series.
    const bM = ols(est(rets[s]), est(mkt)).beta;
    const afterMkt = rets[s].map((r, i) => r - bM * mkt[i]);
    let bS = 0;
    if (secResid) bS = ols(est(afterMkt), est(secResid)).beta;
    residuals[s] = secResid ? afterMkt.map((r, i) => r - bS * secResid[i]) : afterMkt;

    betas[s] = {
      market: Number(bM.toFixed(3)),
      sector: Number(bS.toFixed(3)),
      sectorId: sec,
      declaredSector: sectorOf(s),
      sectorPeers: peers,
      sectorAdjusted: !!secResid,
      residVol: Number(stdev(residuals[s]).toFixed(6)),
    };
  }
  return {
    symbols, residuals, betas, length: minLen,
    sectors: sectorIds.length,
    estimationBars: estEnd,
    eventBars: evWin,
    method: "leave-one-out, sector-balanced market factor, betas fit on the estimation window only",
  };
}

/** Cumulative residual move over the trailing `window` steps, and its z-score
 *  against that symbol's own residual volatility. */
function residualZ(residSeries, window = 12, ctx = null) {
  if (!residSeries || residSeries.length < window + 12) return null;
  const recent = residSeries.slice(-window);
  const cum = recent.reduce((s, r) => s + r, 0);
  // Baseline volatility comes from the estimation window only. Including the
  // event would inflate sigma with the very move being scored and shrink its z.
  const baseline = residSeries.slice(0, -window);
  const shortSd = stdev(baseline) * Math.sqrt(window);
  if (!(shortSd > 0)) return null;

  /* BLEND IN THE LONG RECORD.
   *
   * shortSd is estimated from a couple of sessions. That makes it both noisy
   * and short-sighted: on a quiet two days it is far too small, so an ordinary
   * move scores as a three-sigma event, and the system trades noise. Measured
   * on synthetic data, a genuinely volatile name's two-day sigma came out
   * roughly five times too small, turning a z of 0.9 into a z of 3.
   *
   * H.blendedSigma mixes it with the name's own 60-day daily volatility,
   * rescaled to this window by root-time: 40% on what is happening now, 60% on
   * the long record. Those weights are pre-registered constants, not tuned.
   * With no history the short sigma is used alone and `blended` is false, so
   * the card can say which it was.
   */
  let sd = shortSd, blend = { blended: false };
  if (ctx && ctx.ok) {
    blend = H.blendedSigma(shortSd, ctx, { window });
    if (blend.sigma > 0) sd = blend.sigma;
  }

  return {
    cumResidual: cum,
    z: cum / sd,
    residVol: sd,
    shortResidVol: shortSd,
    sigmaBlend: blend,
    zShortOnly: cum / shortSd,
    window,
  };
}

/* ── 2. RANK + BUY/HOLD SPREAD ─────────────────────────────────────────── */
/** Cross-sectional rank of each symbol's residual z, 0 = most negative. */
function crossSectionalRanks(zBySymbol) {
  const entries = Object.entries(zBySymbol).filter(([, v]) => v && isFinite(v.z));
  entries.sort((a, b) => a[1].z - b[1].z);
  const n = entries.length;
  const ranks = {};
  entries.forEach(([sym], i) => { ranks[sym] = n > 1 ? i / (n - 1) : 0.5; });
  return { ranks, n };
}

/* SECTOR CROWDING GUARD.
 * A large sector-wide move leaves residual dispersion inside that sector,
 * because member betas differ — the low-beta members look idiosyncratically
 * oversold when the whole sector simply fell together. The neutrality fixture
 * demonstrates this directly. Taking several of those at once is one bet
 * wearing several tickers, which is exactly what the plan's correlated-cluster
 * cap exists to prevent. So a name is penalised when its own sector is
 * over-represented among the oversold.
 */
function sectorCrowding(ranks, sectorFor, threshold = 0.25) {
  const inTail = {}, total = {};
  for (const [sym, r] of Object.entries(ranks)) {
    const sec = sectorFor(sym);
    total[sec] = (total[sec] || 0) + 1;
    if (r <= threshold) inTail[sec] = (inTail[sec] || 0) + 1;
  }
  const frac = {};
  for (const sec of Object.keys(total)) frac[sec] = (inTail[sec] || 0) / total[sec];
  return { fractionInTail: frac, countInTail: inTail, sectorSize: total };
}

const ENTRY_RANK = 0.10;   // bottom decile of residual return = oversold
const EXIT_RANK  = 0.50;   // the buy/hold spread: hold until the median

function entrySignal(rank, z, cfg, ctx) {
  const entryRank = cfg.entryRank ?? ENTRY_RANK;
  const minZ = cfg.minAbsZ ?? 2.0;
  if (rank > entryRank) return { fire: false, reason: `rank ${rank.toFixed(3)} above entry ${entryRank}` };
  if (!(z <= -minZ)) return { fire: false, reason: `z ${z.toFixed(2)} not below -${minZ}` };

  /* Variant-specific long-horizon conditions (G and H). Both require history:
     with none, the condition cannot be evaluated and the variant declines to
     trade rather than guessing. A variant that fires on missing data is a
     variant whose record means nothing. */
  if (cfg.requireAboveSma200) {
    if (!ctx || !ctx.ok) return { fire: false, reason: "needs 6-month history to confirm the uptrend; none yet" };
    if (ctx.aboveSma200 !== true) return { fire: false, reason: "below its 200-day average — this variant only buys dips in names still trending up" };
  }
  if (cfg.requireDrawdownPct != null) {
    if (!ctx || !ctx.ok) return { fire: false, reason: "needs 6-month history to measure the drawdown; none yet" };
    if (!(ctx.drawdown6mPct <= cfg.requireDrawdownPct)) {
      return { fire: false, reason: `only ${ctx.drawdown6mPct}% off its 6-month high — this variant wants at least ${cfg.requireDrawdownPct}%` };
    }
  }

  return { fire: true, reason: `rank ${rank.toFixed(3)} <= ${entryRank} and z ${z.toFixed(2)} <= -${minZ}` };
}

/**
 * WHEN TO SELL.
 *
 * The original version of this function took only (rank, heldDays, cfg) — it
 * could not see price at all. That was the single most dangerous hole in the
 * system, and not because a stop was merely missing: the exit trigger is a
 * cross-sectional rank crossing the median, and a collapsing stock's residual
 * goes MORE negative, which pushes its rank AWAY from the exit. Falling harder
 * made a position less likely to be sold, and a name down 40% simply sat there
 * until the 10-day timer expired.
 *
 * Exits are now checked in order of severity, and the price-based ones come
 * first so that no amount of rank behaviour can defer them.
 *
 * `mark` is the current price, `entry` the fill price. If either is missing the
 * price rules cannot be evaluated, and that fact is reported rather than
 * silently treated as "no stop triggered".
 */
function exitSignal(rank, heldDays, cfg, opts = {}) {
  const exitRank = cfg.exitRank ?? EXIT_RANK;
  const maxDays = cfg.maxHoldDays ?? 10;
  const { mark, entry, peak, earningsInDays } = opts;

  const havePrice = Number.isFinite(mark) && Number.isFinite(entry) && entry > 0;
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

    // 3. PROFIT TARGET. The research expects partial reversion, not full.
    const takePct = cfg.takeProfitPct ?? null;
    if (takePct != null && pnlPct >= takePct) {
      return { exit: true, urgent: false, reason: `up ${pnlPct.toFixed(1)}% — profit target ${takePct}%`,
               kind: "take_profit", pnlPct: Number(pnlPct.toFixed(2)) };
    }
  }

  /* 4. EARNINGS. The entry gate refuses to OPEN near an earnings date, but for
        most of this system's life nothing made it CLOSE before one. Holding a
        mean-reversion position through an earnings print is a coin flip with a
        20% standard deviation attached, and it undoes the entry gate entirely. */
  if (Number.isFinite(earningsInDays) && earningsInDays <= (cfg.exitBeforeEarningsDays ?? 2)) {
    return { exit: true, urgent: true, reason: `earnings in ${earningsInDays} day(s) — closing before the print`,
             kind: "earnings_exit", pnlPct: pnlPct != null ? Number(pnlPct.toFixed(2)) : null };
  }

  // 5. The signal exit: the move we were trading has reverted.
  if (rank >= exitRank) {
    return { exit: true, urgent: false, reason: `rank ${rank.toFixed(3)} crossed exit ${exitRank}`,
             kind: "signal", pnlPct: pnlPct != null ? Number(pnlPct.toFixed(2)) : null };
  }

  // 6. Time stop.
  if (heldDays >= maxDays) {
    return { exit: true, urgent: false, reason: `max hold ${maxDays}d reached`,
             kind: "time", pnlPct: pnlPct != null ? Number(pnlPct.toFixed(2)) : null };
  }

  return {
    exit: false, kind: null,
    pnlPct: pnlPct != null ? Number(pnlPct.toFixed(2)) : null,
    priceRulesEvaluated: havePrice,
    reason: `rank ${rank.toFixed(3)} below exit ${exitRank}, held ${heldDays}d`
          + (havePrice ? `, ${pnlPct >= 0 ? "up" : "down"} ${Math.abs(pnlPct).toFixed(1)}%`
                       : ", NO PRICE AVAILABLE so the stop could not be checked"),
  };
}

/* ── 3 + 4. REGIME GATES ───────────────────────────────────────────────── */
/** Nagel: scale exposure with normalized volatility. vixNorm = VIX / its own
 *  trailing median. Below 1 the liquidity-provision premium is thin. */
function volatilityScaler(vixNorm, cfg) {
  const lo = cfg.volScalerFloor ?? 0.35;
  const hi = cfg.volScalerCeiling ?? 1.75;
  if (!isFinite(vixNorm) || vixNorm <= 0) return { scaler: lo, note: "vix_unknown_min_size" };
  const s = Math.max(lo, Math.min(hi, vixNorm));
  return { scaler: Number(s.toFixed(3)), note: `vixNorm ${vixNorm.toFixed(2)}` };
}

/** Dispersion gate. High implied correlation = everything is one asset. */
function dispersionGate(cor3m, cfg) {
  const stand = cfg.corStandDown ?? 45;     // COR3M index points
  const caution = cfg.corCaution ?? 35;
  if (!isFinite(cor3m)) return { pass: true, state: "unknown", note: "COR3M unavailable — proceeding at reduced size", sizeMult: 0.5 };
  if (cor3m >= stand) return { pass: false, state: "stand_down", note: `COR3M ${cor3m} >= ${stand}`, sizeMult: 0 };
  if (cor3m >= caution) return { pass: true, state: "caution", note: `COR3M ${cor3m} elevated`, sizeMult: 0.6 };
  return { pass: true, state: "dispersed", note: `COR3M ${cor3m}`, sizeMult: 1 };
}

/* ── EARNINGS BLACKOUT ─────────────────────────────────────────────────── */
/* The research is unambiguous: the fat tail lives almost entirely on ~4
   scheduled dates a year. AMD's 95th-percentile earnings move is +/-21.2% on a
   +/-9.9% median; CRWD +/-22.7% on +/-8.9%. Excluding a 2-day window converts
   those names from dangerous to ordinary and is the whole gap-risk solution. */
function earningsBlackout(symbol, earningsDates, nowMs, cfg, opts = {}) {
  /* An UNKNOWN window blocks. Previously an empty array passed the gate
     trivially, which is the single most dangerous default in the system:
     AMD's 95th-percentile earnings move is +/-21.2% against a +/-9.9% median,
     and a strategy taking hundreds of positions a year cannot absorb that
     blind. Fail closed. */
  if (!earningsDates || !earningsDates.length) {
    return { blocked: true, reason: "no earnings window known for this symbol — blocked until derived", unknown: true };
  }
  /* Derived windows are projections from EDGAR filing cadence, not exact
     dates, so they get a wider blackout to absorb the projection error. */
  const days = opts.estimated ? (cfg.blackoutDaysEstimated ?? 5) : (cfg.blackoutDays ?? 2);
  /* The window is ASYMMETRIC. Before earnings the risk is the print itself.
     After it, the risk is different and subtler: prices DRIFT in the direction
     of an earnings surprise for days (post-earnings announcement drift), so a
     post-earnings drop is disproportionately a move with real information
     behind it — the exact kind a reversion trade must not fade. The
     announcement-stripped construction in Medhat & Novy-Marx (NBER w30917)
     more than tripled the usable signal, which is why the post window extends
     beyond the pre window here. */
  const postDays = Math.max(days, cfg.postEarningsDays ?? (opts.estimated ? 6 : 4));
  for (const d of (earningsDates || [])) {
    const t = Date.parse(d);
    if (!isFinite(t)) continue;
    const diff = nowMs - t;                     // positive = after the print
    if (diff >= -days * 864e5 && diff <= postDays * 864e5) {
      return { blocked: true, date: d,
               reason: diff >= 0
                 ? `earnings ${d} was ${Math.ceil(diff / 864e5)}d ago — post-earnings moves carry information and drift, not noise`
                 : `earnings ${d} within ${days}d` };
    }
  }
  return { blocked: false, reason: "outside earnings blackout" };
}

/* ── 5. EVIDENCE CLASSIFICATION -> DIRECTION ───────────────────────────── */
/* The classifier's output decides whether a residual drop is a fade or a skip.
   These are the three states the dashboard must show, and the ONLY three.  */
const CAUSE = {
  HARD_NEWS: "cause_detected_fundamental",
  ATTENTION: "attention_driven",
  NONE: "no_cause_detected_in_covered_sources",
  PENDING: "evidence_pending",
};

function directionFromCause(cause, cfg) {
  switch (cause) {
    case CAUSE.HARD_NEWS:
      // Fundamental repricing. Not a reversal candidate — the information is
      // real and the move is the market doing its job.
      return { trade: false, side: null, confidence: 0, reason: "fundamental cause — no fade" };
    case CAUSE.ATTENTION:
      // The strongest documented case: retail-attention moves reverse over
      // days t+2..t+8. This is the primary book.
      return { trade: true, side: "long", confidence: 1.0, reason: "attention-driven move — fade" };
    case CAUSE.NONE:
      // Liquidity-provision premium, but recall risk: a real cause may exist
      // outside covered sources. Traded smaller until the recall benchmark
      // demonstrates acceptable coverage.
      return { trade: true, side: "long", confidence: cfg.noCauseConfidence ?? 0.5,
               reason: "no cause detected in covered sources — reduced size" };
    default:
      return { trade: false, side: null, confidence: 0, reason: "evidence still pending" };
  }
}

/* ── 6. THE COST HURDLE ────────────────────────────────────────────────── */
/**
 * A candidate must beat its own frictions by a margin before it is proposed.
 * Expected edge is the mean-reversion of the residual toward zero, haircut by
 * the post-publication decay the literature documents (Chen & Velikov find
 * roughly half of in-sample gross returns vanish; we take 50%).
 */
function costHurdle({ cumResidual, advUsd, grade, wideSpreadWindow, vixNorm, cfg, reversionMult }) {
  const halfTripBps = M.slippageBps({ advUsd, grade, wideSpreadWindow, vixNorm });
  const roundTripBps = halfTripBps * 2;
  const decay = cfg.decayHaircut ?? 0.5;
  const capture = cfg.reversionCapture ?? 0.35;      // we do not expect full reversion
  /* This name's own bounce-back record, shrunk hard toward the roster average
     and clamped to [0.7, 1.3] upstream. It can make a marginal idea pass or
     fail the cost test, which is the correct amount of influence for a number
     estimated from roughly ten events. It can never open a closed gate. */
  const revMult = (typeof reversionMult === "number" && isFinite(reversionMult))
    ? Math.max(0.7, Math.min(1.3, reversionMult)) : 1;
  const expectedBps = Math.abs(cumResidual) * 1e4 * capture * decay * revMult;
  const marginMult = cfg.costMarginMultiple ?? 2.0;  // demand 2x the frictions
  const required = roundTripBps * marginMult;
  return {
    halfTripBps, roundTripBps,
    expectedGrossBps: Number(expectedBps.toFixed(1)),
    requiredBps: Number(required.toFixed(1)),
    pass: expectedBps >= required,
    ratio: required > 0 ? Number((expectedBps / required).toFixed(2)) : 0,
    reversionMult: revMult,
    assumptions: { decayHaircut: decay, reversionCapture: capture, costMarginMultiple: marginMult, reversionMult: revMult },
  };
}

/* ── THE GATE STACK ────────────────────────────────────────────────────── */
/**
 * Runs every gate in a fixed order and returns the full result set. A rejected
 * candidate is as valuable as an accepted one: the dashboard shows exactly
 * which chip stopped it, and every no-trade reason is persisted.
 */
function evaluateCandidate(input) {
  const {
    symbol, rank, zStat, quality, advUsd, earningsDates, nowMs,
    cause, vixNorm, cor3m, session, cfg, position,
  } = input;
  const ctx = input.historyContext || null;      // six-month picture for this name
  const rev = input.reversion || null;           // its own bounce-back record

  const gates = [];
  const add = (id, label, pass, detail, blocking = true) =>
    gates.push({ id, label, pass, detail, blocking });

  // 0. If we already hold it, this is an exit evaluation, not an entry one.
  if (position && position.open) {
    const heldDays = (nowMs - Date.parse(position.openedAt)) / 864e5;
    const ex = exitSignal(rank, heldDays, cfg);
    return {
      symbol, kind: "exit", exit: ex.exit, reason: ex.reason,
      rank, heldDays: Number(heldDays.toFixed(2)), gates: [],
    };
  }

  // 1. Data quality — an F grade freezes the symbol entirely.
  add("quality", "Price quality", quality && quality.tradable,
      quality
        ? `grade ${quality.grade}${(quality.reasons && quality.reasons.length) ? " — " + quality.reasons.join(", ") : ""}`
        : "no data");

  // 2. Session — never open in the wide-spread auction windows.
  const sessionOk = session.open && !session.wideSpreadWindow;
  add("session", "Session", sessionOk,
      session.open ? (session.wideSpreadWindow ? "auction window — spreads 2-5x wider" : `regular session (${session.phase})`)
                   : `market ${session.phase}`);

  // 3. Dispersion regime.
  const disp = dispersionGate(cor3m, cfg);
  add("dispersion", "Dispersion regime", disp.pass, disp.note);

  // 4. Earnings blackout.
  const bo = earningsBlackout(symbol, earningsDates, nowMs, cfg, { estimated: input.earningsEstimated });
  add("blackout", "Earnings blackout", !bo.blocked,
      bo.reason + (input.earningsEstimated && !bo.unknown ? " (estimated window, widened)" : ""));

  // 5. Liquidity floor — cost is the binding constraint.
  const minAdv = cfg.minAdvUsd ?? 3e8;
  add("liquidity", "Liquidity floor", advUsd >= minAdv,
      `$${(advUsd / 1e6).toFixed(0)}M/day vs $${(minAdv / 1e6).toFixed(0)}M floor`);

  // 6. The signal itself.
  const sig = zStat ? entrySignal(rank, zStat.z, cfg, ctx) : { fire: false, reason: "no z statistic" };
  add("signal", "Residual signal", sig.fire, sig.reason);

  // 6b. Sector crowding — is this one idea, or one sector falling together?
  /* Calibration: the tail threshold is itself the random baseline. If the tail
     is the bottom 25% of the cross-section then ~25% of ANY sector lands there
     by chance, so the cap is expressed as a multiple of that baseline rather
     than as an absolute number. 1.4x flags genuine over-representation without
     firing on ordinary dispersion. */
  /* CALIBRATION FIX. The tail fraction is measured at cfg.entryRank (0.10),
     but this baseline used Math.max(entryRank, 0.25) = 0.25, so a statistic
     whose chance expectation is 10% was compared against a 35% cap. That
     needed 3.5x over-representation to fire, not the 1.4x the comment claims,
     and with 39 semis in the roster it would effectively never trip. The
     baseline must be the same threshold the fraction was measured at. */
  const tailBase = cfg.entryRank != null ? cfg.entryRank : 0.25;
  const crowdMult = cfg.sectorCrowdingMultiple ?? 1.4;
  const crowdMax = Math.min(0.9, Math.max(0.15, tailBase * crowdMult));
  const crowdFrac = input.sectorTailFraction;
  if (crowdFrac != null) {
    add("crowding", "Sector crowding", crowdFrac <= crowdMax,
        `${Math.round(crowdFrac * 100)}% of ${sectorOf(symbol)} is in the oversold tail ` +
        `vs ${Math.round(crowdMax * 100)}% cap (${crowdMult}x the ${Math.round(tailBase * 100)}% baseline)`);
  }

  /* 6c. LONG-HORIZON TREND. The gate this system was missing entirely.
     A drop is only a dislocation relative to a stable base. A name that is
     below its 200-day average, more than 20% off its six-month high, and whose
     50-day line is still falling is not oversold — it is in a downtrend, and
     mean-reversion strategies bleed in exactly those names. All three
     conditions must hold, so ordinary weakness does not trip it.

     If the name has too little history the gate ABSTAINS rather than passing
     silently, and says so on the card. An unknown is reported as an unknown. */
  if (ctx && ctx.ok) {
    const blockTrend = cfg.blockDowntrends !== false;
    add("trend", "Long-horizon trend", !(blockTrend && ctx.downtrend),
        ctx.downtrend
          ? `downtrend: ${ctx.drawdown6mPct}% off its 6-month high, below the 200-day, 50-day line falling ${ctx.sma50SlopePct}%`
          : `${Math.round(ctx.rangePct6m * 100)}% up its 6-month range, ${ctx.aboveSma200 === true ? "above" : "below"} the 200-day (${ctx.downtrendFlags}/3 downtrend flags)`);
  } else {
    add("trend", "Long-horizon trend", true,
        ctx ? `no long-horizon read yet — ${ctx.reason}` : "history not loaded for this name yet");
  }

  /* 6d. TURNOVER CONDITIONING — the strongest published conditioning result
     for this exact strategy. Medhat & Schmeling (Review of Financial Studies,
     2022) split the reversal effect by share turnover: in LOW-turnover names
     last month's losers reliably bounce (-1.41%/mo continuation of the classic
     effect), while in the HIGHEST-turnover names losers keep falling and
     winners keep winning — short-term MOMENTUM, the exact opposite trade.
     Fading a heavily-traded name's drop is not contrarian, it is standing in
     front of informed flow.

     Turnover = shares traded / shares outstanding, cross-sectional. Names in
     the top decile are refused. Unknown shares outstanding abstains with a
     note rather than blocking — the gate cannot fail closed on data it may
     never have for every name, and the cause/evidence gate still stands
     behind it. */
  const toPctile = input.turnoverPctile;
  if (toPctile != null && isFinite(toPctile)) {
    const cap = cfg.turnoverPctileCap ?? 0.90;
    add("turnover", "Turnover conditioning", toPctile < cap,
        toPctile >= cap
          ? `top-decile turnover (${Math.round(toPctile * 100)}th pctile) — heavily-traded losers continue falling rather than bouncing (Medhat–Schmeling RFS 2022)`
          : `${Math.round(toPctile * 100)}th percentile turnover — inside the range where reversal actually works`);
  } else {
    add("turnover", "Turnover conditioning", true,
        "shares outstanding not yet known for this name — gate abstains", true);
  }

  // 7. Evidence classification decides direction.
  const dir = directionFromCause(cause, cfg);
  add("evidence", "Evidence classification", dir.trade, dir.reason);

  // 8. Cost hurdle.
  const cost = zStat ? costHurdle({
    cumResidual: zStat.cumResidual, advUsd, grade: quality ? quality.grade : "F",
    wideSpreadWindow: session.wideSpreadWindow, vixNorm, cfg,
    reversionMult: rev ? rev.multiplier : 1,
  }) : { pass: false, expectedGrossBps: 0, requiredBps: 0, roundTripBps: 0, ratio: 0 };
  add("cost", "Cost hurdle", cost.pass,
      `expect ${cost.expectedGrossBps}bp vs ${cost.requiredBps}bp required (${cost.ratio}x)`);

  const blocked = gates.filter((g) => g.blocking && !g.pass);
  const vol = volatilityScaler(vixNorm, cfg);

  return {
    symbol, kind: "entry",
    pass: blocked.length === 0,
    blockedBy: blocked.map((g) => g.id),
    firstBlock: blocked[0] ? blocked[0].label : null,
    gates,
    rank, z: zStat ? Number(zStat.z.toFixed(2)) : null,
    cumResidualBps: zStat ? Number((zStat.cumResidual * 1e4).toFixed(1)) : null,
    cause, direction: dir,
    historyContext: ctx && ctx.ok ? {
      days: ctx.days, vol60Pct: ctx.vol60Pct, atrPct: ctx.atrPct,
      rangePct6m: ctx.rangePct6m, drawdown6mPct: ctx.drawdown6mPct,
      aboveSma200: ctx.aboveSma200, sma50SlopePct: ctx.sma50SlopePct,
      downtrend: ctx.downtrend, downtrendFlags: ctx.downtrendFlags,
      gapFreqPct: ctx.gapFreqPct, advUsd20: ctx.advUsd20,
    } : null,
    reversion: rev || null,
    cost,
    sizing: {
      volScaler: vol.scaler, volNote: vol.note,
      dispersionMult: disp.sizeMult,
      causeConfidence: dir.confidence,
      combined: Number((vol.scaler * disp.sizeMult * dir.confidence).toFixed(3)),
    },
  };
}

module.exports = {
  mean, stdev, pctReturns, ols,
  sectorOf, setSectorMap,
  residualPanel, residualZ, crossSectionalRanks,
  entrySignal, exitSignal, ENTRY_RANK, EXIT_RANK, sectorCrowding,
  volatilityScaler, dispersionGate, earningsBlackout,
  CAUSE, directionFromCause, costHurdle,
  evaluateCandidate,
};
