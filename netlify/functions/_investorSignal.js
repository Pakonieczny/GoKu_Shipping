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

const T = require("./_investorTemporal");
const M = require("./_investorMarket");
const H = require("./_investorHistory");
const XP = require("./_investorExitPolicy");
const I = require("./_investorIntelligence");

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

/** Exact-clock log returns. A missing five-minute interval stays missing; it
 * must never be converted into a longer return and joined to a five-minute
 * factor observation. */
function timestampReturns(bars, intervalMs = 5 * 60 * 1000) {
  const series = M.normalizeBars(bars);
  const out = new Map();
  for (let i = 1; i < series.length; i++) {
    const p = series[i - 1], c = series[i];
    if (Date.parse(c.t) - Date.parse(p.t) !== intervalMs) continue;
    if (!(p.c > 0 && c.c > 0)) continue;
    out.set(c.t, Math.log(c.c / p.c));
  }
  return out;
}

function solve3(a, b) {
  const m = a.map((r, i) => [...r, b[i]]);
  for (let i = 0; i < 3; i++) {
    let p = i;
    for (let r = i + 1; r < 3; r++) if (Math.abs(m[r][i]) > Math.abs(m[p][i])) p = r;
    if (Math.abs(m[p][i]) < 1e-12) return null;
    [m[i], m[p]] = [m[p], m[i]];
    const d = m[i][i]; for (let j = i; j < 4; j++) m[i][j] /= d;
    for (let r = 0; r < 3; r++) if (r !== i) {
      const q = m[r][i]; for (let j = i; j < 4; j++) m[r][j] -= q * m[i][j];
    }
  }
  return m.map((r) => r[3]);
}

function jointOls(rows) {
  if (rows.length < 12) return { alpha: 0, market: 1, sector: 0, n: rows.length };
  const xtx = [[0,0,0],[0,0,0],[0,0,0]], xty = [0,0,0];
  for (const r of rows) {
    const x = [1, r.m, r.s];
    for (let i = 0; i < 3; i++) {
      xty[i] += x[i] * r.y;
      for (let j = 0; j < 3; j++) xtx[i][j] += x[i] * x[j];
    }
  }
  const ridge = 1e-10; xtx[1][1] += ridge; xtx[2][2] += ridge;
  const v = solve3(xtx, xty);
  return v ? { alpha: v[0], market: v[1], sector: v[2], n: rows.length }
    : { alpha: mean(rows.map((r) => r.y)), market: 0, sector: 0, n: rows.length };
}

/* ── sector resolution ─────────────────────────────────────────────────
 * Sectors come from the UNIVERSE file, not from a table hardcoded here — the
 * roster is the thing that changes, and a 350-name roster against a 46-name
 * hardcoded map silently dumped 300 names into a single "other" bucket, which
 * destroys the factor model. The cycle calls setSectorMap() once per run.
 */
let SECTOR = {};
function setSectorMap(map) { SECTOR = map || {}; return Object.keys(SECTOR).length; }
function getSectorMap() { return { ...SECTOR }; }
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
  const quality = opts.quality || {};
  /* Measurement admission, not execution admission. With allowResearchGrade a
     series that is valid but single-venue is admitted to the CROSS-SECTION so
     the desk can rank and record; nothing downstream treats that as permission
     to price an order. Default stays execution-grade, so every caller that
     does not deliberately opt in behaves exactly as before. */
  const admit = opts.allowResearchGrade
    ? (q) => !q || q.tradable === true || q.researchEligible === true
    : (q) => !q || q.tradable === true;
  let symbols = Object.keys(panel).filter((s) => (panel[s] || []).length > 12
    && admit(quality[s]));
  if (symbols.length < 4) return { symbols: [], residuals: {}, betas: {}, note: "insufficient_panel_breadth" };
  const rets = Object.fromEntries(symbols.map((s) => [s, timestampReturns(panel[s])]));

  /* A synchronous timestamp grid alone is not sufficient. A sparse symbol can
     appear on only a few of those timestamps and still contaminate the market
     and sector factors used for every other name. Build the grid and remove
     under-covered symbols BEFORE factor construction, repeating after each
     removal because the breadth threshold changes with the population. */
  const minCoverageRatio = Math.max(0.5, Math.min(1,
    Number(opts.minCoverageRatio) || 0.65));
  const requestedSymbolCoverage = opts.minSymbolCoverage != null
    ? Number(opts.minSymbolCoverage) : Number(opts.minSymbolCoverageRatio);
  const minSymbolCoverage = Math.max(0.5, Math.min(1,
    Number.isFinite(requestedSymbolCoverage) ? requestedSymbolCoverage : 0.80));
  const intervalMs = Math.max(1, Number(opts.intervalMs) || 300000);
  const maxWindowSpanMultiple = Math.max(1,
    Number(opts.maxWindowSpanMultiple) || 1.5);
  const symbolCoverage = {};
  const coverageExclusions = new Map();
  let timestamps = [];
  let minCoverage = 4;
  while (symbols.length >= 4) {
    const coverage = new Map();
    for (const s of symbols) {
      for (const t of rets[s].keys()) coverage.set(t, (coverage.get(t) || 0) + 1);
    }
    minCoverage = Math.max(4, Math.ceil(symbols.length * minCoverageRatio));
    timestamps = [...coverage.entries()].filter(([, n]) => n >= minCoverage)
      .map(([t]) => t).sort((a, b) => Date.parse(a) - Date.parse(b));
    if (timestamps.length < 24) {
      return { symbols: [], residuals: {}, betas: {}, symbolCoverage,
        excludedForCoverage: [...coverageExclusions.values()],
        note: "insufficient_synchronous_history" };
    }
    const covered = [];
    for (const s of symbols) {
      let seen = 0;
      for (const t of timestamps) if (Number.isFinite(rets[s].get(t))) seen += 1;
      const ratio = seen / timestamps.length;
      symbolCoverage[s] = Number(ratio.toFixed(3));
      if (ratio < minSymbolCoverage) {
        coverageExclusions.set(s, { symbol: s, coverage: symbolCoverage[s],
          reason: `reported ${(ratio * 100).toFixed(0)}% of the synchronised grid, below the ${(minSymbolCoverage * 100).toFixed(0)}% floor` });
      } else covered.push(s);
    }
    if (covered.length === symbols.length) break;
    symbols = covered;
  }
  if (symbols.length < 4) {
    return { symbols: [], residuals: {}, betas: {}, symbolCoverage,
      excludedForCoverage: [...coverageExclusions.values()],
      note: "insufficient_covered_breadth" };
  }

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
  const residuals = {}, residualTimestamps = {}, betas = {};
  const evWin = Math.max(1, Number(opts.signalWindow) || 12);
  for (const s of symbols) {
    const sec = effSector[s];
    const peers = bySector[sec].filter((x) => x !== s);
    const rows = [];
    for (const t of timestamps) {
      const y = rets[s].get(t); if (!Number.isFinite(y)) continue;
      const sectorValues = peers.map((x) => rets[x].get(t)).filter(Number.isFinite);
      if (sectorValues.length < 2) continue;
      const sectorFactor = mean(sectorValues);
      const sectorMeans = [];
      for (const sid of sectorIds) {
        const vals = bySector[sid].filter((x) => x !== s).map((x) => rets[x].get(t)).filter(Number.isFinite);
        if (vals.length) sectorMeans.push(mean(vals));
      }
      if (sectorMeans.length < 2) continue;
      rows.push({ t, y, m: mean(sectorMeans), s: sectorFactor });
    }
    const rowCoverage = rows.length / timestamps.length;
    if (rows.length < evWin + 12 || rowCoverage < minSymbolCoverage) {
      coverageExclusions.set(s, { symbol: s, coverage: symbolCoverage[s],
        factorRowCoverage: Number(rowCoverage.toFixed(3)),
        reason: "insufficient synchronous factor rows after leave-one-out construction" });
      continue;
    }

    /* Array indices are observations, not clocks. Refuse an event window whose
       trailing observations span an overnight boundary or a long outage while
       still presenting themselves as (for example) twelve five-minute bars. */
    const eventRows = rows.slice(-evWin);
    const spanMs = Date.parse(eventRows[eventRows.length - 1].t)
      - Date.parse(eventRows[0].t);
    const expectedSpanMs = (evWin - 1) * intervalMs;
    if (evWin > 1 && spanMs > expectedSpanMs * maxWindowSpanMultiple) {
      coverageExclusions.set(s, { symbol: s, coverage: symbolCoverage[s],
        windowSpanMinutes: Math.round(spanMs / 60000),
        expectedSpanMinutes: Math.round(expectedSpanMs / 60000),
        reason: `event window spans ${Math.round(spanMs / 60000)} minutes for a ${evWin}-bar signal expected to span ${Math.round(expectedSpanMs / 60000)}` });
      continue;
    }
    const fitRows = rows.slice(0, -evWin);
    const fit = jointOls(fitRows);
    residuals[s] = rows.map((r) => r.y - fit.alpha - fit.market * r.m - fit.sector * r.s);
    residualTimestamps[s] = rows.map((r) => r.t);
    betas[s] = {
      alpha: Number(fit.alpha.toFixed(8)),
      market: Number(fit.market.toFixed(3)),
      sector: Number(fit.sector.toFixed(3)),
      sectorId: sec,
      declaredSector: sectorOf(s),
      sectorPeers: peers.length,
      sectorAdjusted: true,
      residVol: Number(stdev(residuals[s]).toFixed(6)),
      estimationN: fit.n,
    };
  }
  const usable = Object.keys(residuals);
  return {
    symbols: usable, residuals, residualTimestamps, betas, length: timestamps.length,
    sectors: sectorIds.length,
    estimationBars: Math.max(0, timestamps.length - evWin),
    eventBars: evWin,
    synchronizedCoverageRequired: minCoverage,
    perSymbolCoverageRatioRequired: minSymbolCoverage,
    symbolCoverage,
    excludedForCoverage: [...coverageExclusions.values()],
    minSymbolCoverage,
    maxWindowSpanMultiple,
    method: "exact-clock log returns; leave-one-out sector-balanced factors; joint OLS with intercept fit pre-event; pre-factor per-symbol coverage and event-window clock-span floors",
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
  const mu = mean(baseline);
  const maxLag = Math.min(window - 1, 6, baseline.length - 2);
  let longRunVar = mean(baseline.map((x) => (x - mu) ** 2));
  for (let lag = 1; lag <= maxLag; lag++) {
    let cov = 0;
    for (let i = lag; i < baseline.length; i++) cov += (baseline[i] - mu) * (baseline[i - lag] - mu);
    cov /= baseline.length;
    longRunVar += 2 * (1 - lag / (maxLag + 1)) * cov;
  }
  const shortSd = Math.sqrt(Math.max(0, longRunVar) * window);
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
    hacLags: maxLag,
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
function exitSignal(rank, heldDays, cfg, opts = {}) { return XP.exitSignal(rank, heldDays, cfg, opts); }

/* ── 3 + 4. REGIME GATES ───────────────────────────────────────────────── */
/** Nagel: scale exposure with normalized volatility. vixNorm = VIX / its own
 *  trailing median. Below 1 the liquidity-provision premium is thin. */
function volatilityScaler(vixNorm, cfg) {
  const lo = cfg.volScalerFloor ?? 0.35;
  const hi = Math.min(1, cfg.volScalerCeiling ?? 1);
  if (!isFinite(vixNorm) || vixNorm <= 0) return { scaler: lo, note: "vix_unknown_min_size" };
  const s = Math.max(lo, Math.min(hi, vixNorm));
  return { scaler: Number(s.toFixed(3)), note: `vixNorm ${vixNorm.toFixed(2)}` };
}

/** Dispersion gate. High implied correlation = everything is one asset. */
function dispersionGate(cor3m, cfg) {
  const stand = cfg.corStandDown ?? 45;     // COR3M index points
  const caution = cfg.corCaution ?? 35;
  if (!isFinite(cor3m)) return { pass: false, state: "unknown", note: "COR3M unavailable — new risk blocked", sizeMult: 0 };
  if (cor3m >= stand) return { pass: false, state: "stand_down", note: `COR3M ${cor3m} >= ${stand}`, sizeMult: 0 };
  if (cor3m >= caution) return { pass: true, state: "caution", note: `COR3M ${cor3m} elevated`, sizeMult: 0.6 };
  return { pass: true, state: "dispersed", note: `COR3M ${cor3m}`, sizeMult: 1 };
}

function effectiveDispersionGate(cor3m, cfg = {}) {
  const raw = dispersionGate(cor3m, cfg);
  if (cfg.paperAbstainOnMissingInfo === true && raw.state === "unknown") {
    return { ...raw, pass: true, sizeMult: Math.max(cfg.volScalerFloor ?? 0.35, 0.20),
      relaxedMissingInformation: true,
      note: "COR3M unavailable — paper observation admitted at minimum size" };
  }
  return raw;
}

/* ── EARNINGS BLACKOUT ─────────────────────────────────────────────────── */
/* The research is unambiguous: the fat tail lives almost entirely on ~4
   scheduled dates a year. AMD's 95th-percentile earnings move is +/-21.2% on a
   +/-9.9% median; CRWD +/-22.7% on +/-8.9%. A blackout reduces one known
   event risk; it does not eliminate unscheduled or overnight gap risk. */
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
  const days = opts.estimated
    ? Math.max(cfg.blackoutDaysEstimated ?? 5, Number(opts.uncertaintyDays) || 0)
    : (cfg.blackoutDays ?? 2);
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
  ABNORMAL_ACTIVITY: "abnormal_activity_without_covered_fundamental_event",
  ATTENTION: "abnormal_activity_without_covered_fundamental_event",
  NONE: "no_cause_detected_in_covered_sources",
  PENDING: "evidence_pending",
};

function directionFromCause(cause, cfg = {}, attentionScore = null, coverage = null) {
  switch (cause) {
    case CAUSE.HARD_NEWS:
      // Fundamental repricing. Not a reversal candidate — the information is
      // real and the move is the market doing its job.
      return { trade: false, side: null, confidence: 0, reason: "fundamental cause — no fade" };
    case CAUSE.ABNORMAL_ACTIVITY:
      // The strongest documented case: retail-attention moves reverse over
      // days t+2..t+8. This is the primary book.
      if (!coverage || coverage.complete !== true) {
        return { trade: false, side: null, confidence: 0, reason: "required source coverage incomplete" };
      }
      return { trade: true, side: "long", confidence: Math.min(0.75, cfg.abnormalActivityConfidence ?? 0.65),
               reason: `abnormal activity (${Number(attentionScore || 0).toFixed(1)}z) without a covered fundamental event` };
    case CAUSE.NONE:
      // Liquidity-provision premium, but recall risk: a real cause may exist
      // outside covered sources. Traded smaller until the recall benchmark
      // demonstrates acceptable coverage.
      if (cfg.paperAbstainOnMissingInfo === true) {
        return { trade: true, side: "long", confidence: 0.2, relaxed: true,
                 reason: "no cause found in covered sources — taken as a paper observation at reduced confidence" };
      }
      return { trade: false, side: null, confidence: 0,
               reason: "no-cause automation locked until external known-cause recall is measured" };
    default:
      /* Pending means the evidence sweep has not reached this name yet. That is
         an absence of information, not a finding against the trade, so a paper
         desk may record it. HARD_NEWS above is a FINDING and never relaxes:
         fading a real repricing is the known-losing trade, and taking it would
         teach the system something false. */
      if (cfg.paperAbstainOnMissingInfo === true) {
        return { trade: true, side: "long", confidence: 0.2, relaxed: true,
                 reason: "evidence still pending — taken as a paper observation at reduced confidence" };
      }
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
  const calibratedRaw = cfg.calibratedExpectedEdgeBps;
  const calibrated = Number(calibratedRaw);
  /* Number(null) and Number("") are both zero. Treating either as a measured
     calibration made the API claim a zero lower bound existed when the field
     was actually unknown. */
  const calibrationKnown = calibratedRaw !== null && calibratedRaw !== undefined
    && calibratedRaw !== "" && Number.isFinite(calibrated);
  const calibratedNetLowerBoundBps = calibrationKnown ? calibrated : null;
  const calibratedPass = cfg.requireCalibratedEdge === false
    ? true : (calibrationKnown && calibrated > 0);
  return {
    halfTripBps, roundTripBps,
    expectedGrossBps: Number(expectedBps.toFixed(1)),
    requiredBps: Number(required.toFixed(1)),
    calibratedNetLowerBoundBps,
    pass: expectedBps >= required && calibratedPass,
    ratio: required > 0 ? Number((expectedBps / required).toFixed(2)) : 0,
    reversionMult: revMult,
    assumptions: { decayHaircut: decay, reversionCapture: capture, costMarginMultiple: marginMult,
      reversionMult: revMult, calibrationRequired: cfg.requireCalibratedEdge !== false },
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
    const heldDays = M.tradingDaysHeld(position.openedAt, nowMs) ?? 0;
    const intelligencePolicy = input.intelligence
      ? I.decisionPolicy({ coverage: input.intelligence.coverage,
        events: input.intelligence.events, temporalContext: input.intelligence.temporalContext,
        requireTemporalContext: false, asOfMs: nowMs,
        maxAgeHours: cfg.intelligenceMaxAgeHours,
        temporalMaxAgeHours: cfg.temporalMaxAgeHours,
        decisionMatrixPolicy: cfg.decisionMatrixPolicy }) : null;
    const ex = exitSignal(rank, heldDays, cfg, { intelligencePolicy });
    return {
      symbol, kind: "exit", exit: ex.exit, reason: ex.reason,
      rank, heldDays: Number(heldDays.toFixed(2)), gates: [],
    };
  }

  // 1. Data quality — an F grade freezes the symbol entirely.
  const qualityOk = !!quality && (quality.tradable === true
    || (cfg.paperAbstainOnMissingInfo === true && quality.researchEligible === true));
  add("quality", "Price quality", qualityOk,
      quality
        ? `grade ${quality.grade}${(quality.reasons && quality.reasons.length) ? " — " + quality.reasons.join(", ") : ""}`
        : "no data");

  // 2. Session — never open in the wide-spread auction windows.
  const sessionOk = session.open && !session.wideSpreadWindow;
  add("session", "Session", sessionOk,
      session.open ? (session.wideSpreadWindow ? "auction window — spreads 2-5x wider" : `regular session (${session.phase})`)
                   : `market ${session.phase}`);

  // 3. Dispersion regime.
  /* Missing COR3M is missing information, not evidence of a crowded market.
     Exploratory paper learning may therefore observe it at the minimum size.
     A known stand-down reading remains a hard finding and is never relaxed. */
  const disp = effectiveDispersionGate(cor3m, cfg);
  add("dispersion", "Dispersion regime", disp.pass, disp.note);

  // 4. Earnings blackout.
  const bo = earningsBlackout(symbol, earningsDates, nowMs, cfg, {
    estimated: input.earningsEstimated, uncertaintyDays: input.earningsUncertaintyDays });
  /* A KNOWN blackout still blocks under every setting — that is a dated hazard.
     Only "no window derivable yet" relaxes, because that is missing data. */
  const blackoutOk = !bo.blocked || (cfg.paperAbstainOnMissingInfo === true && bo.unknown === true);
  add("blackout", "Earnings blackout", blackoutOk,
      bo.reason + (blackoutOk && bo.unknown ? "; paper observation admitted while it is derived" : "")
        + (input.earningsEstimated && !bo.unknown ? " (estimated window, widened)" : ""));

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
    const trendUnknownOk = cfg.blockDowntrends === false || cfg.paperAbstainOnMissingInfo === true;
    add("trend", "Long-horizon trend", trendUnknownOk,
        (ctx ? `no long-horizon read yet — ${ctx.reason}` : "history not loaded")
          + (trendUnknownOk ? "; paper observation admitted while the backfill completes" : "; new risk blocked"));
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
    add("turnover", "Turnover conditioning", cfg.paperAbstainOnMissingInfo === true,
        "shares outstanding not known — "
          + (cfg.paperAbstainOnMissingInfo === true ? "paper observation admitted" : "new risk blocked"), true);
  }

  // 7. Company intelligence is a required, point-in-time risk gate. It may
  // block or reduce size, but positive research never increases base risk.
  const intelPolicy = input.intelligence
    ? I.decisionPolicy({ coverage: input.intelligence.coverage,
      events: input.intelligence.events, temporalContext: input.intelligence.temporalContext,
      requireTemporalContext: cfg.requireTemporalContext === true, asOfMs: nowMs,
      maxAgeHours: cfg.intelligenceMaxAgeHours,
      temporalMaxAgeHours: cfg.temporalMaxAgeHours,
      decisionMatrixPolicy: cfg.decisionMatrixPolicy })
    : { monitored: false, fresh: false, complete: false,
        entryAllowed: cfg.requireCompanyIntelligence !== true, sizeMultiplier: cfg.requireCompanyIntelligence === true ? 0 : 1,
        adverseRiskScore: 0, reasons: ["no current company-intelligence dossier"] };
  /* Missing, stale or incomplete coverage is an absence. A current dossier
     that found adverse material is evidence. Paper learning may observe the
     former at reduced size but must never erase the latter. */
  const intelIncomplete = !input.intelligence || intelPolicy.monitored !== true
    || intelPolicy.fresh !== true || intelPolicy.complete !== true;
  const temporalPolicy = intelPolicy.temporalPolicy || {};
  const intelHasFinding = Number(intelPolicy.adverseRiskScore) > 0
    || intelPolicy.criticalExit === true || intelPolicy.hardBlock === true
    || temporalPolicy.hardBlock === true
    || (temporalPolicy.entryAllowed === false && temporalPolicy.complete === true
        && temporalPolicy.fresh !== false);
  const intelRelaxed = cfg.requireCompanyIntelligence === true
    && cfg.paperAbstainOnMissingInfo === true && intelPolicy.entryAllowed !== true
    && intelIncomplete && !intelHasFinding;
  const intelReasons = Array.isArray(intelPolicy.reasons) ? intelPolicy.reasons : [];
  const rawIntelSize = Number(intelPolicy.sizeMultiplier);
  const effectiveIntelPolicy = intelRelaxed ? {
    ...intelPolicy,
    entryAllowed: true,
    originalEntryAllowed: intelPolicy.entryAllowed === true,
    sizeMultiplier: Math.max(Number.isFinite(rawIntelSize) ? rawIntelSize : 0, 0.20),
    relaxedMissingInformation: true,
    reasons: [...intelReasons, "incomplete intelligence admitted as a reduced-size paper observation"],
  } : { ...intelPolicy, reasons: intelReasons };
  const intelRequired = cfg.requireCompanyIntelligence === true;
  add("intelligence", "Company intelligence", !intelRequired || effectiveIntelPolicy.entryAllowed,
      `${effectiveIntelPolicy.reasons.join("; ")}; size ${Math.round(effectiveIntelPolicy.sizeMultiplier * 100)}%`);

  // 8. Evidence classification decides direction.
  const dir = directionFromCause(cause, cfg, input.attentionScore, input.coverage);
  add("evidence", "Evidence classification", dir.trade, dir.reason);

  // 9. Cost hurdle.
  const cost = zStat ? costHurdle({
    cumResidual: zStat.cumResidual, advUsd, grade: quality ? quality.grade : "F",
    wideSpreadWindow: session.wideSpreadWindow, vixNorm, cfg,
    reversionMult: rev ? rev.multiplier : 1,
  }) : { pass: false, expectedGrossBps: 0, requiredBps: 0, roundTripBps: 0, ratio: 0 };
  add("cost", "Cost hurdle", cost.pass,
      `expect ${cost.expectedGrossBps}bp vs ${cost.requiredBps}bp required (${cost.ratio}x)`);

  const blocked = gates.filter((g) => g.blocking && !g.pass);
  const vol = volatilityScaler(vixNorm, cfg);
  const rawSizing = combineSizeMultipliers({
    volScaler: vol.scaler, volNote: vol.note,
    dispersionMult: disp.sizeMult,
    causeConfidence: dir.confidence,
    intelligenceMult: effectiveIntelPolicy.sizeMultiplier,
    rule: cfg.sizeAggregation,
  });
  const paperSizeFloor = Math.max(0, Math.min(0.25,
    Number(cfg.paperObservationSizeFloor) || 0));
  const sizing = cfg.paperAbstainOnMissingInfo === true && blocked.length === 0
    && paperSizeFloor > 0 && rawSizing.combined < paperSizeFloor
    ? { ...rawSizing, combinedBeforePaperFloor: rawSizing.combined,
        combined: paperSizeFloor, paperObservationFloorApplied: true }
    : rawSizing;

  return {
    symbol, kind: "entry",
    pass: blocked.length === 0,
    blockedBy: blocked.map((g) => g.id),
    firstBlock: blocked[0] ? blocked[0].label : null,
    gates,
    rank, z: zStat ? Number(zStat.z.toFixed(2)) : null,
    cumResidualBps: zStat ? Number((zStat.cumResidual * 1e4).toFixed(1)) : null,
    cause, direction: dir,
    intelligencePolicy: effectiveIntelPolicy,
    historyContext: ctx && ctx.ok ? {
      days: ctx.days, vol60Pct: ctx.vol60Pct, atrPct: ctx.atrPct,
      rangePct6m: ctx.rangePct6m, drawdown6mPct: ctx.drawdown6mPct,
      aboveSma200: ctx.aboveSma200, sma50SlopePct: ctx.sma50SlopePct,
      downtrend: ctx.downtrend, downtrendFlags: ctx.downtrendFlags,
      gapFreqPct: ctx.gapFreqPct, advUsd20: ctx.advUsd20,
      expectedShortfall5dPct: ctx.expectedShortfall5dPct,
      overnightGapEsPct: ctx.overnightGapEsPct,
    } : null,
    reversion: rev || null,
    cost,
    sizing,
  };
}

/* Combine independent size haircuts.
 *
 * v8.4 multiplied them: volScaler x dispersionMult x causeConfidence x
 * intelligenceMult. Each factor is individually calibrated as "the right size
 * given THIS concern alone", so the product is far below every one of them —
 * four mild 0.8-ish haircuts compound to 0.36, and the measured median
 * compound multiplier of 22.7% put the 60% gross-exposure cap out of reach no
 * matter how good the opportunities were. The product also double-counts:
 * high volatility depresses volScaler, widens dispersion AND lowers cause
 * confidence, so one market condition is charged three times.
 *
 * The rule here is the one the temporal policy already uses and documents:
 * the strongest concern applies in full, additional independent concerns at
 * sharply decreasing weight. A zero from any single factor still zeroes the
 * size, so every hard block behaves exactly as before. The weights are
 * T.TEMPORAL_WEIGHTS.combination, shared rather than re-declared, and a
 * runtime fixture asserts the two paths stay in step. */
function combineSizeMultipliers(parts) {
  const weights = T.TEMPORAL_WEIGHTS.combination;
  const factors = [
    ["volScaler", parts.volScaler],
    ["dispersionMult", parts.dispersionMult],
    ["causeConfidence", parts.causeConfidence],
    ["intelligenceMult", parts.intelligenceMult],
  ].map(([id, v]) => ({ id, value: Math.max(0, Math.min(1, Number(v) || 0)) }));

  if (factors.some((f) => f.value <= 0)) {
    return { ...parts, combined: 0, combinationRule: parts.rule === "product"
      ? PRODUCT_COMBINATION_RULE : SIZE_COMBINATION_RULE,
      binding: factors.filter((f) => f.value <= 0).map((f) => f.id) };
  }
  if (parts.rule === "product") {
    const combined = factors.reduce((value, factor) => value * factor.value, 1);
    return { ...parts, combined: Number(combined.toFixed(3)),
      combinationRule: PRODUCT_COMBINATION_RULE,
      binding: factors.filter((f) => f.value < 1)
        .sort((a, b) => a.value - b.value).slice(0, 2).map((f) => f.id) };
  }
  const ranked = factors.map((f) => ({ ...f, haircut: 1 - f.value }))
    .sort((a, b) => b.haircut - a.haircut);
  const total = ranked.reduce((sum, f, i) =>
    sum + f.haircut * (weights[i] == null ? 0.03 : weights[i]), 0);
  const combined = Math.max(0, Math.min(1, 1 - total));
  return { ...parts, combined: Number(combined.toFixed(3)),
    combinationRule: SIZE_COMBINATION_RULE,
    binding: ranked.filter((f) => f.haircut > 0).slice(0, 2).map((f) => f.id) };
}

const SIZE_COMBINATION_RULE =
  "strongest haircut in full, then 1.00/0.35/0.20/0.10/0.05 capped additive weights; "
  + "any zero factor zeroes the size";
const PRODUCT_COMBINATION_RULE =
  "product of all independent multipliers; any zero factor zeroes the size";

module.exports = {
  combineSizeMultipliers,
  mean, stdev, pctReturns, timestampReturns, ols, jointOls,
  sectorOf, setSectorMap, getSectorMap,
  residualPanel, residualZ, crossSectionalRanks,
  entrySignal, exitSignal, ENTRY_RANK, EXIT_RANK, sectorCrowding,
  volatilityScaler, dispersionGate, effectiveDispersionGate, earningsBlackout,
  CAUSE, directionFromCause, costHurdle,
  evaluateCandidate,
};
