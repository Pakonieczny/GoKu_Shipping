/*  netlify/functions/_investorHistory.js  (v1.0)
 *  ---------------------------------------------------------------------------
 *  Investor_AI — the long memory.
 *
 *  WHY THIS EXISTS
 *  ------------------------------------------------------------------------
 *  Before this module the system decided using about two days of 5-minute bars.
 *  That is enough to see that a stock dropped, and nowhere near enough to know
 *  whether the drop MEANS anything. The same -3% move is:
 *
 *    · a genuine dislocation, if the name normally moves 0.8% a day and has
 *      been flat-to-up for six months; or
 *    · an unremarkable Tuesday, if the name routinely swings 4%; or
 *    · the continuation of a seven-month slide, in which case buying it is not
 *      mean reversion at all, it is standing under a falling object.
 *
 *  Telling those apart needs months of daily history, not hours of intraday.
 *  This module stores that history and turns it into a small set of numbers the
 *  decision path can actually use.
 *
 *  WHAT IS STORED
 *  ------------------------------------------------------------------------
 *  One document per symbol in InvestorAI_MarketDaily, holding parallel arrays
 *  of up to five trading years: date, open, high, low, close, volume. Arrays
 *  rather than an array-of-objects because 1,300 days x 6 fields as columns is a
 *  few KB, comfortably inside Firestore's 1MB document limit, and reads back in
 *  one get() instead of 180.
 *
 *  Intraday 5-minute bars keep going to MarketLatest, one doc per symbol per
 *  day, exactly as before. This is the daily spine underneath them.
 *
 *  THE LEAKAGE RULE — READ THIS BEFORE CHANGING ANYTHING
 *  ------------------------------------------------------------------------
 *  Every number this module produces for a decision on day D is computed from
 *  bars strictly BEFORE day D. Not "up to and including" — before. A context
 *  feature that has peeked at the day it is being used to trade will make the
 *  whole system look brilliant in testing and lose money in front of you. The
 *  `asOf` argument on contextFor() is not optional decoration; it is the cut,
 *  and history.test.js exists to prove the cut holds.
 * ---------------------------------------------------------------------------
 */

"use strict";

const A = require("./_investorAdmin");
const M = require("./_investorMarket");

const DAILY_COL = A.COL.marketDaily;
const KEEP_DAYS = 1300;         // ~5 years: annual seasonality needs repeated cycles
/* Symbols per daily-backfill request. Exported so the page-budget invariant
   can be attested against the batch that is actually sent. */
const DAILY_CHUNK_SYMBOLS = 25;
const CONTEXT_MIN_DAYS = 40;    // below this we refuse to produce context at all

/* ── small stats helpers ───────────────────────────────────────────────── */
function mean(a) { return a.length ? a.reduce((s, x) => s + x, 0) / a.length : 0; }
function stdev(a) {
  if (a.length < 2) return 0;
  const m = mean(a);
  return Math.sqrt(a.reduce((s, x) => s + (x - m) * (x - m), 0) / (a.length - 1));
}
/* ── THE LAST UP-LEG, AND HOW MUCH OF IT HAS BEEN GIVEN BACK ──────────────
 * Policy Q ("Pullback in trend") needs one structural fact the statistical
 * layer does not carry: where the current price sits inside the most recent
 * swing. The leg is the lowest close in the lookback window to the highest
 * close AFTER it; the retracement is the share of that leg surrendered by the
 * latest close. It is computed from the same split-repaired, as-of-cut series
 * as everything else here, so it cannot see the future either.
 *
 * Nothing is fitted: the lookback and the minimum leg size are declared by the
 * variant that consumes them, and the scoreboard says whether they matter. */
function pullbackLeg(closes, opts = {}) {
  const lookback = Math.max(20, Math.round(Number(opts.lookbackDays) || 60));
  const win = (closes || []).filter((c) => Number.isFinite(c) && c > 0).slice(-lookback);
  if (win.length < 20) return null;
  let i0 = 0;
  for (let i = 1; i < win.length; i++) if (win[i] < win[i0]) i0 = i;
  if (i0 >= win.length - 2) return null;            // the low is (almost) now: no leg yet
  let i1 = i0 + 1;
  for (let i = i0 + 1; i < win.length; i++) if (win[i] > win[i1]) i1 = i;
  const legLow = win[i0], legHigh = win[i1];
  if (!(legHigh > legLow)) return null;
  const last = win[win.length - 1];
  const retracement = (legHigh - last) / (legHigh - legLow);
  return {
    lookbackDays: lookback,
    legLow: Number(legLow.toFixed(4)), legHigh: Number(legHigh.toFixed(4)),
    legPct: Number(((legHigh / legLow - 1) * 100).toFixed(2)),
    legDays: i1 - i0,
    daysSinceHigh: win.length - 1 - i1,
    level50: Number((legHigh - 0.5 * (legHigh - legLow)).toFixed(4)),
    retracementPct: Number((retracement * 100).toFixed(1)),   // of the latest CLOSE
  };
}

/** Retracement of a leg at an arbitrary price (the live intraday mark). */
function retracementAt(leg, price) {
  if (!leg || !(leg.legHigh > leg.legLow) || !(Number(price) > 0)) return null;
  return (leg.legHigh - Number(price)) / (leg.legHigh - leg.legLow);
}

function sma(arr, n) {
  if (!arr || arr.length < n) return null;
  return mean(arr.slice(-n));
}
function pctRets(closes) {
  const r = [];
  for (let i = 1; i < closes.length; i++) {
    const p = closes[i - 1];
    r.push(p > 0 ? (closes[i] - p) / p : 0);
  }
  return r;
}

/** Positive loss magnitude in the worst (1-alpha) fraction. This is a
 * historical tail estimate, not a Gaussian extrapolation. */
function expectedShortfallLoss(returns, alpha = 0.95) {
  const losses = (returns || []).filter(Number.isFinite).map((r) => Math.max(0, -r))
    .sort((a, b) => b - a);
  if (!losses.length) return null;
  /* Decimal tails such as 1-.95 are not exact in binary. Without the epsilon,
     100 observations produced ceil(5.000000000000004)=6 and diluted the very
     loss tail this function is intended to preserve. */
  const n = Math.max(1, Math.ceil((1 - alpha) * losses.length - 1e-12));
  return mean(losses.slice(0, n));
}

/* ── storage ───────────────────────────────────────────────────────────── */
function dailyRef(symbol) { return A.col(DAILY_COL).doc(String(symbol).toUpperCase()); }

function seriesFromDailyDoc(d = {}) {
  const date = d.date || [];
  const out = [];
  for (let i = 0; i < date.length; i++) {
    out.push({
      date: date[i],
      o: (d.o || [])[i], h: (d.h || [])[i], l: (d.l || [])[i],
      c: (d.c || [])[i], v: (d.v || [])[i],
    });
  }
  return out;
}

function timestampMs(value) {
  if (Number.isFinite(Number(value))) return Number(value);
  if (value && typeof value.toMillis === "function") return value.toMillis();
  if (value && typeof value.toDate === "function") return value.toDate().getTime();
  const parsed = Date.parse(value);
  return Number.isFinite(parsed) ? parsed : null;
}

/** Read the daily series and the market identity that produced its volume.
 * Keeping these together prevents a current feed label from being applied to
 * historical volume captured through a different feed. */
async function readDailyWithMeta(symbol) {
  const s = await dailyRef(symbol).get();
  if (!s.exists) return { series: [], provenance: null };
  const d = s.data() || {};
  const provider = d.provider || null;
  const feed = d.feed != null ? d.feed : null;
  const adjustment = d.adjustment || null;
  const sourceSha256 = d.sourceSha256 || null;
  return {
    series: seriesFromDailyDoc(d),
    provenance: provider ? {
      provider, feed, adjustment, sourceSha256,
      feedVolumeShare: Number.isFinite(Number(d.feedVolumeShare))
        ? Number(d.feedVolumeShare) : null,
      fetchedAt: d.marketFetchedAt || null,
      homogeneous: d.volumeProvenanceHomogeneous === true,
    } : null,
    backfill: {
      complete: d.backfillComplete === true,
      truncated: d.backfillTruncated === true,
      requestedDays: Number(d.backfillRequestedDays) || null,
      lastAttemptAtMs: timestampMs(d.lastBackfillAttemptAtMs || d.lastBackfillAttemptAt),
      lastError: d.lastBackfillError || null,
    },
  };
}

/** Read one symbol's daily series as an array of bar objects, oldest first. */
async function readDaily(symbol) {
  return (await readDailyWithMeta(symbol)).series;
}

/** Merge new daily bars into a symbol's series, de-duplicated by date. */
async function writeDaily(symbol, bars, meta = {}) {
  const sym = String(symbol).toUpperCase();
  const prior = await readDailyWithMeta(sym);
  const existing = prior.series;
  const byDate = new Map();
  for (const b of existing) byDate.set(b.date, b);
  for (const b of bars) {
    if (!b || !b.date) continue;
    byDate.set(b.date, b);           // newer wins — corrections and splits land here
  }
  const merged = [...byDate.values()]
    .filter((b) => isFinite(b.c) && b.c > 0)
    .sort((a, b) => (a.date < b.date ? -1 : 1))
    .slice(-KEEP_DAYS);

  const incomingDates = new Set((bars || []).filter((b) => b && b.date).map((b) => b.date));
  const replacesPrior = existing.length === 0
    || existing.every((b) => incomingDates.has(b.date))
    || (bars || []).length >= Math.min(KEEP_DAYS,
      Math.max(CONTEXT_MIN_DAYS, Math.ceil(existing.length * 0.9)));
  const priorProv = prior.provenance;
  const identityFields = ["provider", "feed", "adjustment"];
  const identitySame = !!priorProv && identityFields.every((k) =>
    (priorProv[k] || null) === (meta[k] || null));
  const volumeProvenanceHomogeneous = replacesPrior
    || (!!priorProv && priorProv.homogeneous === true && identitySame);

  await dailyRef(sym).set({
    symbol: sym,
    date: merged.map((b) => b.date),
    o: merged.map((b) => b.o), h: merged.map((b) => b.h),
    l: merged.map((b) => b.l), c: merged.map((b) => b.c),
    v: merged.map((b) => b.v),
    days: merged.length,
    firstDate: merged.length ? merged[0].date : null,
    lastDate: merged.length ? merged[merged.length - 1].date : null,
    ...meta,
    volumeProvenanceHomogeneous,
    updated_at: A.FV.serverTimestamp(),
  }, { merge: true });

  return merged.length;
}

/* ── provider fetch for daily bars ─────────────────────────────────────── */
/**
 * Alpaca takes up to ~100 symbols per bars request, so 342 names is four calls,
 * not 342. Massive is one call per ticker but has no call ceiling. Either way
 * the backfill is cheap enough to run inside one background invocation.
 */
async function fetchDailyWithMeta(symbols, { days = KEEP_DAYS } = {}) {
  const barsBySymbol = {}, provenanceBySymbol = {}, statusBySymbol = {};
  /* 90 symbols x 1300 daily bars is 117k bars — twelve full 10k pages, fetched
     one after another inside a single invocation. Narrower chunks keep each
     backfill step inside its time budget and let the cursor resume cleanly;
     the symbol count is unchanged, only how many go per request. */
  const chunkSize = DAILY_CHUNK_SYMBOLS;
  for (let i = 0; i < symbols.length; i += chunkSize) {
    const chunk = symbols.slice(i, i + chunkSize);
    let res;
    try {
      res = await M.fetchBars(chunk, { timeframe: "1Day", limit: days });
    } catch (e) {
      for (const sym of chunk) statusBySymbol[sym] = {
        complete: false, truncated: false, requestedDays: days,
        error: String(e.code || e.message).slice(0, 160),
      };
      continue;                       // a failed chunk is retried on the next run
    }
    for (const sym of chunk) {
      const returned = Array.isArray((res.bars || {})[sym]) && (res.bars || {})[sym].length > 0;
      statusBySymbol[sym] = {
      complete: res.truncated !== true && returned,
      truncated: res.truncated === true,
      requestedDays: days,
      pages: Number(res.pages) || null,
      window: res.window || null,
      ...(returned ? {} : { error: "no_bars_returned" }),
    };
    }
    for (const [sym, arr] of Object.entries(res.bars || {})) {
      barsBySymbol[sym] = (arr || []).map((b) => ({
        date: String(b.t).slice(0, 10),
        o: b.o, h: b.h, l: b.l, c: b.c, v: b.v,
      })).filter((b) => isFinite(b.c) && b.c > 0);
      const sourceSha256 = (res.symbolSha256 || {})[sym]
        || res.manifestSha256 || res.sha256 || null;
      provenanceBySymbol[sym] = {
        provider: res.provider || "manual",
        feed: res.feed || null,
        adjustment: res.adjustment || null,
        sourceSha256,
        feedVolumeShare: M.feedVolumeShare(res.provider, res.feed),
        fetchedAt: res.fetchedAt || null,
      };
    }
  }
  return { barsBySymbol, provenanceBySymbol, statusBySymbol };
}

async function fetchDaily(symbols, opts = {}) {
  return (await fetchDailyWithMeta(symbols, opts)).barsBySymbol;
}

function mergeDailySeries(prior, incoming) {
  const byDate = new Map();
  for (const b of [...(prior || []), ...(incoming || [])]) {
    if (!b || !/^\d{4}-\d{2}-\d{2}$/.test(String(b.date || ""))) continue;
    if (!(Number(b.c) > 0)) continue;
    byDate.set(b.date, b);
  }
  return [...byDate.values()].sort((a, b) => a.date.localeCompare(b.date)).slice(-KEEP_DAYS);
}

/** Full, validated series for charts. No hidden six-month truncation. */
function chartSeries(series, { throughDate = null } = {}) {
  return mergeDailySeries([], series)
    .filter((b) => !throughDate || b.date <= throughDate)
    .map((b) => ({ d: b.date, c: Number(b.c),
      h: Number.isFinite(Number(b.h)) ? Number(b.h) : Number(b.c),
      l: Number.isFinite(Number(b.l)) ? Number(b.l) : Number(b.c),
      v: Math.max(0, Number(b.v) || 0) }));
}

/**
 * Repair the historic one-row backfill defect on the first company/history
 * read. A completed provider window is remembered, so recent IPOs with fewer
 * than KEEP_DAYS observations do not refetch forever. Failed attempts cool
 * down briefly to stop repeated chart clicks from hammering the provider.
 * Dependency hooks keep the policy independently testable.
 */
async function ensureDailyHistory(symbol, opts = {}) {
  const sym = String(symbol || "").toUpperCase();
  if (!/^[A-Z][A-Z0-9.-]{0,9}$/.test(sym)) throw new Error("valid symbol required");
  const targetDays = Math.max(CONTEXT_MIN_DAYS,
    Math.min(KEEP_DAYS, Number(opts.targetDays) || KEEP_DAYS));
  const nowMs = Number.isFinite(Number(opts.nowMs)) ? Number(opts.nowMs) : Date.now();
  const cooldownMs = Math.max(60_000, Number(opts.cooldownMs) || 30 * 60_000);
  const read = opts.read || readDailyWithMeta;
  const fetch = opts.fetch || fetchDailyWithMeta;
  const write = opts.write || writeDaily;
  const initial = await read(sym);
  const prior = initial || { series: [], provenance: null, backfill: null };
  const backfill = prior.backfill || {};
  const providerCoveredWindow = backfill.complete === true
    && Number(backfill.requestedDays) >= targetDays;
  const cooled = Number(backfill.lastAttemptAtMs) > 0
    && nowMs - Number(backfill.lastAttemptAtMs) < cooldownMs;
  const needsWindow = !providerCoveredWindow && (prior.series || []).length < targetDays;
  if (!needsWindow || cooled) {
    return { symbol: sym, series: mergeDailySeries([], prior.series),
      provenance: prior.provenance || null, backfill, attempted: false,
      repaired: false, note: cooled && needsWindow ? "backfill_retry_cooldown" : "stored_window_available" };
  }

  try {
    const fetched = await fetch([sym], { days: targetDays });
    const incoming = (fetched.barsBySymbol && fetched.barsBySymbol[sym]) || [];
    const provenance = (fetched.provenanceBySymbol && fetched.provenanceBySymbol[sym])
      || prior.provenance || {};
    const status = (fetched.statusBySymbol && fetched.statusBySymbol[sym]) || {
      complete: false, truncated: false, requestedDays: targetDays,
    };
    const merged = mergeDailySeries(prior.series, incoming);
    await write(sym, incoming, {
      ...provenance,
      lastBackfillAttemptAtMs: nowMs,
      backfillComplete: status.complete === true,
      backfillTruncated: status.truncated === true,
      backfillRequestedDays: targetDays,
      lastBackfillError: status.error || null,
    });
    return { symbol: sym, series: merged, provenance, attempted: true,
      repaired: merged.length > (prior.series || []).length,
      backfill: { ...status, lastAttemptAtMs: nowMs },
      note: status.error ? "backfill_failed" : (status.complete ? "provider_window_complete" : "provider_window_partial") };
  } catch (e) {
    return { symbol: sym, series: mergeDailySeries([], prior.series),
      provenance: prior.provenance || null, attempted: true, repaired: false,
      backfill: { ...backfill, complete: false, lastAttemptAtMs: nowMs,
        lastError: String(e.code || e.message).slice(0, 160) },
      note: "backfill_failed" };
  }
}

/* ── SPLIT DETECTION ───────────────────────────────────────────────────────
 * The roster file already documents the hazard in a comment: "KLAC is live but
 * executed a 10-for-1 split ~May 2026 - unadjusted history shows a spurious
 * -90% gap." Nothing acted on it.
 *
 * The danger is not the split itself — it is the SEAM. Backfill runs once per
 * symbol and the daily top-up only refreshes the last week, so after a split
 * the recent days carry adjusted prices while the older ones keep unadjusted
 * ones. The series then contains a fabricated -90% single-day return, which
 * flows straight into 60-day volatility, the 200-day average, the drawdown,
 * the reversion statistics, and — through blendedSigma — 60% of the z-score
 * denominator for that name. A stock would look impossibly cheap and
 * impossibly volatile at the same time.
 *
 * A one-day move beyond this threshold is not a market event. Even a
 * catastrophic single-session collapse rarely exceeds 60%; common split ratios
 * (2:1, 3:1, 4:1, 10:1) all produce moves far outside it.
 */
const SPLIT_RATIOS = [2, 3, 4, 5, 6, 7, 8, 10, 15, 20, 1.5];
const SPLIT_MOVE_THRESHOLD = 0.35;   // |one-day return| above this is suspect

function detectSplitSeams(series) {
  const seams = [];
  for (let i = 1; i < series.length; i++) {
    const prev = series[i - 1].c, cur = series[i].c;
    if (!(prev > 0) || !(cur > 0)) continue;
    const r = (cur - prev) / prev;
    if (Math.abs(r) < SPLIT_MOVE_THRESHOLD) continue;

    // Does the jump land close to a common split ratio in either direction?
    const ratio = prev / cur;
    let best = null;
    for (const k of SPLIT_RATIOS) {
      for (const cand of [k, 1 / k]) {
        const err = Math.abs(ratio - cand) / cand;
        if (err < 0.06 && (!best || err < best.err)) best = { ratio: cand, err };
      }
    }
    seams.push({
      index: i, date: series[i].date,
      movePct: Number((r * 100).toFixed(2)),
      likelySplit: !!best,
      impliedRatio: best ? Number(best.ratio.toFixed(4)) : null,
    });
  }
  return seams;
}

/**
 * Repair a seam by rescaling everything BEFORE it onto the post-seam basis.
 * Returns the repaired series plus what was done, so it can be reported rather
 * than silently applied — a silent price rewrite is its own kind of hazard.
 */
function repairSplits(series) {
  const seams = detectSplitSeams(series);
  const splits = seams.filter((x) => x.likelySplit);
  if (!splits.length) return { series, repaired: [], suspicious: seams.filter((x) => !x.likelySplit) };

  const out = series.map((b) => ({ ...b }));
  const repaired = [];
  // apply latest first so earlier factors compose correctly
  for (const sp of splits.slice().reverse()) {
    const f = sp.impliedRatio;
    for (let i = 0; i < sp.index; i++) {
      out[i].o /= f; out[i].h /= f; out[i].l /= f; out[i].c /= f;
      if (out[i].v) out[i].v *= f;
    }
    repaired.push({ date: sp.date, ratio: f, movePct: sp.movePct });
  }
  return { series: out, repaired, suspicious: seams.filter((x) => !x.likelySplit) };
}

/* ── THE CONTEXT FEATURES ──────────────────────────────────────────────── */
/**
 * Turn a daily series into the numbers the decision path consults.
 *
 * `asOf` is the trading date the decision is being made for. Every bar dated
 * asOf or later is discarded before anything is computed. See the leakage rule
 * at the top of this file.
 */
function contextFor(series, asOf) {
  /* ORDER MATTERS, AND IT IS SUBTLE.
   *
   * Cut to the decision date FIRST, then repair splits within that window.
   *
   * Repairing first looks harmless and is not: repairSplits rescales every bar
   * BEFORE a seam, so a split occurring after the cut would reach back and
   * rewrite the history a past decision was based on. The leakage test caught
   * exactly that — tripling the future changed the past-derived context, which
   * is the one thing this file promises can never happen.
   *
   * The cost of this ordering is that a split is only repaired once it is
   * inside the window being looked at, which is precisely the information a
   * decision at that date could legitimately have had. */
  const cut = (series || []).filter((b) => !asOf || b.date < asOf);
  const fixed = repairSplits(cut);
  const hist = fixed.series;
  if (hist.length < CONTEXT_MIN_DAYS) {
    return {
      ok: false,
      days: hist.length,
      reason: `only ${hist.length} days of history — need ${CONTEXT_MIN_DAYS} before this name gets long-horizon context`,
    };
  }

  const closes = hist.map((b) => b.c);
  const highs  = hist.map((b) => b.h);
  const lows   = hist.map((b) => b.l);
  const vols   = hist.map((b) => b.v || 0);
  const last   = closes[closes.length - 1];
  const rets   = pctRets(closes);

  /* ── how much does this name normally move? ──────────────────────────
     This is the number that fixes the two-day sigma problem. A 12-bar
     intraday sigma is estimated from ~12 observations of one afternoon;
     these are estimated from months. */
  const vol20  = stdev(rets.slice(-20));
  const vol60  = stdev(rets.slice(-60));
  const vol120 = stdev(rets.slice(-120));

  /* Average true range as a share of price — the everyday swing, in the
     units a person actually thinks in. */
  let atrSum = 0, atrN = 0;
  for (let i = Math.max(1, hist.length - 20); i < hist.length; i++) {
    const tr = Math.max(
      highs[i] - lows[i],
      Math.abs(highs[i] - closes[i - 1]),
      Math.abs(lows[i] - closes[i - 1]),
    );
    if (isFinite(tr)) { atrSum += tr; atrN += 1; }
  }
  const atrPct = atrN && last > 0 ? (atrSum / atrN) / last : null;

  /* ── where is it, relative to its own last six months? ───────────────── */
  const win6m = closes.slice(-126);
  const hi6m = Math.max(...win6m), lo6m = Math.min(...win6m);
  const rangePct6m = hi6m > lo6m ? (last - lo6m) / (hi6m - lo6m) : 0.5;
  const drawdown6m = hi6m > 0 ? (last - hi6m) / hi6m : 0;

  const sma20 = sma(closes, 20), sma50 = sma(closes, 50), sma200 = sma(closes, 200);
  const aboveSma200 = sma200 != null ? last > sma200 : null;
  const aboveSma50  = sma50  != null ? last > sma50  : null;

  /* Slope of the 50-day line over the last month, in percent. Direction of
     travel, not just position. */
  let sma50Slope = null;
  if (closes.length >= 70) {
    const now = sma(closes, 50);
    const then = sma(closes.slice(0, -20), 50);
    if (now && then) sma50Slope = (now - then) / then;
  }

  /* ── is it a downtrend, or a dip? ────────────────────────────────────
     Deliberately conservative: all three of position, level and direction
     must agree before a name is called a downtrend. One indicator saying
     "weak" is noise; three saying it is a trend. */
  const downtrendFlags = [
    aboveSma200 === false,
    drawdown6m <= -0.20,
    sma50Slope != null && sma50Slope < -0.02,
  ].filter(Boolean).length;
  const downtrend = downtrendFlags >= 3;
  const sma50Rising = sma50Slope != null ? sma50Slope > 0 : null;
  const pullback = pullbackLeg(closes, { lookbackDays: 60 });

  /* ── liquidity, measured rather than assumed ─────────────────────────── */
  let advUsd20 = null;
  if (hist.length >= 20) {
    let acc = 0, n = 0;
    for (let i = hist.length - 20; i < hist.length; i++) {
      const dv = (vols[i] || 0) * closes[i];
      if (isFinite(dv) && dv > 0) { acc += dv; n += 1; }
    }
    advUsd20 = n ? acc / n : null;
  }

  /* ── how often does this name gap? ───────────────────────────────────
     A name that regularly opens 3% away from yesterday's close is one where
     an overnight hold is a coin flip, not a position. */
  let gaps = 0, gapN = 0;
  const signedGaps = [];
  for (let i = Math.max(1, hist.length - 120); i < hist.length; i++) {
    const signed = closes[i - 1] > 0 ? (hist[i].o - closes[i - 1]) / closes[i - 1] : 0;
    const g = Math.abs(signed);
    if (isFinite(signed)) signedGaps.push(signed);
    if (isFinite(g)) { gapN += 1; if (g > 0.02) gaps += 1; }
  }
  const gapFreq = gapN ? gaps / gapN : null;
  const fiveDayReturns = [];
  for (let i = Math.max(5, closes.length - 120); i < closes.length; i++) {
    if (closes[i - 5] > 0) fiveDayReturns.push(closes[i] / closes[i - 5] - 1);
  }
  const es5d = expectedShortfallLoss(fiveDayReturns, 0.95);
  const gapEs = expectedShortfallLoss(signedGaps, 0.95);

  return {
    ok: true,
    asOf: asOf || null,
    days: hist.length,
    splitsRepaired: fixed.repaired.length ? fixed.repaired : null,
    suspiciousMoves: fixed.suspicious.length ? fixed.suspicious.slice(-3) : null,
    lastDate: hist[hist.length - 1].date,
    lastClose: Number(last.toFixed(4)),
    vol20Pct: vol20 != null ? Number((vol20 * 100).toFixed(3)) : null,
    vol60Pct: vol60 != null ? Number((vol60 * 100).toFixed(3)) : null,
    vol120Pct: vol120 != null ? Number((vol120 * 100).toFixed(3)) : null,
    atrPct: atrPct != null ? Number((atrPct * 100).toFixed(3)) : null,
    rangePct6m: Number(rangePct6m.toFixed(4)),
    drawdown6mPct: Number((drawdown6m * 100).toFixed(2)),
    high6m: Number(hi6m.toFixed(4)), low6m: Number(lo6m.toFixed(4)),
    sma20: sma20 != null ? Number(sma20.toFixed(4)) : null,
    sma50: sma50 != null ? Number(sma50.toFixed(4)) : null,
    sma200: sma200 != null ? Number(sma200.toFixed(4)) : null,
    aboveSma50, aboveSma200,
    sma50SlopePct: sma50Slope != null ? Number((sma50Slope * 100).toFixed(2)) : null,
    downtrend, downtrendFlags, sma50Rising,
    pullback,
    advUsd20: advUsd20 != null ? Math.round(advUsd20) : null,
    gapFreqPct: gapFreq != null ? Number((gapFreq * 100).toFixed(1)) : null,
    expectedShortfall5dPct: es5d != null ? Number((es5d * 100).toFixed(3)) : null,
    overnightGapEsPct: gapEs != null ? Number((gapEs * 100).toFixed(3)) : null,
  };
}

/* ── PER-NAME REVERSION HISTORY ────────────────────────────────────────── */
/**
 * "When THIS name has dropped hard before, did it bounce?"
 *
 * The honest version of a very tempting idea. Six months gives maybe eight to
 * twelve qualifying events per name, which is almost no information — left raw
 * it would be pure noise dressed up as insight, and the system would chase it.
 *
 * So two disciplines apply:
 *   1. Walk-forward only. Events are found in bars before `asOf`, and each
 *      event's forward return uses bars after the event but still before asOf.
 *   2. Empirical-Bayes shrinkage toward the pooled average across the whole
 *      roster. With n events and shrink constant k, the name's own number gets
 *      weight n/(n+k). At k=20 a name with 10 events keeps one third of its own
 *      signal and borrows two thirds from the crowd. That is the correct
 *      humility for this sample size.
 *
 * The result is allowed to SCALE an expected edge within bounds. It is never
 * allowed to open a gate that would otherwise be closed.
 */
const REVERSION_SHRINK_K = 20;      // pre-registered, not tuned
const REVERSION_TRIGGER_SD = 1.5;   // how big a drop counts as an event
const REVERSION_HORIZON = 5;        // trading days forward, matches the hold

function reversionEvents(series, asOf, { horizon = REVERSION_HORIZON } = {}) {
  // cut first, then repair — see the ordering note in contextFor()
  const hist = repairSplits((series || []).filter((b) => !asOf || b.date < asOf)).series;
  if (hist.length < 80) return { n: 0, meanFwdPct: null, winRate: null };

  const closes = hist.map((b) => b.c);
  const rets = pctRets(closes);
  const out = [];

  // start at 60 so the trailing sigma is estimated on real history, and stop
  // `horizon` short of the end so every event has a full forward window
  for (let i = 60; i < rets.length - horizon; i++) {
    const trailing = rets.slice(Math.max(0, i - 60), i);   // strictly before the event
    const sd = stdev(trailing);
    if (!(sd > 0)) continue;
    if (rets[i] >= -REVERSION_TRIGGER_SD * sd) continue;   // not a big enough drop

    const entry = closes[i + 1];                            // enter the day after
    const exit = closes[Math.min(closes.length - 1, i + 1 + horizon)];
    if (!(entry > 0) || !isFinite(exit)) continue;
    out.push((exit - entry) / entry);
  }

  if (!out.length) return { n: 0, meanFwdPct: null, winRate: null };
  return {
    n: out.length,
    meanFwdPct: Number((mean(out) * 100).toFixed(3)),
    winRate: Number((out.filter((x) => x > 0).length / out.length).toFixed(3)),
  };
}

/**
 * Shrink each name's raw reversion number toward the roster-wide pooled mean.
 * `raw` is { SYM: {n, meanFwdPct} } as produced by reversionEvents().
 */
function shrinkReversion(raw, { k = REVERSION_SHRINK_K } = {}) {
  const usable = Object.values(raw).filter((r) => r && r.n > 0 && r.meanFwdPct != null);
  const totalN = usable.reduce((s, r) => s + r.n, 0);
  const pooled = totalN
    ? usable.reduce((s, r) => s + r.meanFwdPct * r.n, 0) / totalN
    : 0;

  const out = {};
  for (const [sym, r] of Object.entries(raw)) {
    if (!r || !r.n) { out[sym] = { n: 0, pooledPct: Number(pooled.toFixed(3)), shrunkPct: Number(pooled.toFixed(3)), weightOwn: 0 }; continue; }
    const w = r.n / (r.n + k);
    const shrunk = w * r.meanFwdPct + (1 - w) * pooled;
    out[sym] = {
      n: r.n,
      rawPct: r.meanFwdPct,
      winRate: r.winRate,
      pooledPct: Number(pooled.toFixed(3)),
      shrunkPct: Number(shrunk.toFixed(3)),
      weightOwn: Number(w.toFixed(3)),
    };
  }
  return { pooledPct: Number(pooled.toFixed(3)), perSymbol: out, shrinkK: k };
}

/**
 * Turn the shrunk reversion number into a bounded multiplier on expected edge.
 * Clamped to [0.7, 1.3] so a name's own history can nudge sizing but can never
 * be the reason a trade happens. The clamp is the point.
 */
function reversionMultiplier(shrunkPct, pooledPct) {
  if (shrunkPct == null || pooledPct == null) return 1;
  const base = Math.abs(pooledPct) > 1e-6 ? pooledPct : 1;
  const ratio = 1 + (shrunkPct - pooledPct) / Math.abs(base);
  return Number(Math.max(0.7, Math.min(1.3, ratio)).toFixed(3));
}

/* ── blended sigma: the fix for the two-day denominator ────────────────── */
/**
 * The intraday z-score divides a cumulative move by a sigma estimated from a
 * couple of days. That sigma is both noisy and short-sighted. Blend it with the
 * name's 60-day daily volatility, rescaled to the intraday window, so "unusual"
 * means unusual for this stock over months.
 *
 * Weights are pre-registered: 40% on what is happening now, 60% on the long
 * record. Today's regime matters, but it is the smaller voice.
 */
const SIGMA_W_SHORT = 0.4;
const BARS_PER_SESSION = 78;        // 6.5h of 5-minute bars

function blendedSigma(shortSigma, ctx, { window = 12, barsPerSession = BARS_PER_SESSION } = {}) {
  if (!ctx || !ctx.ok || ctx.vol60Pct == null) {
    return { sigma: shortSigma, blended: false, reason: "no long history yet — using short-window sigma alone" };
  }
  const dailySigma = ctx.vol60Pct / 100;
  // scale a daily sigma down to a `window`-bar sigma by root-time
  const longSigma = dailySigma * Math.sqrt(window / barsPerSession);
  if (!(shortSigma > 0)) return { sigma: longSigma, blended: true, shortSigma, longSigma, wShort: 0 };
  const sigma = SIGMA_W_SHORT * shortSigma + (1 - SIGMA_W_SHORT) * longSigma;
  return {
    sigma,
    blended: true,
    shortSigma: Number(shortSigma.toExponential(4)),
    longSigma: Number(longSigma.toExponential(4)),
    wShort: SIGMA_W_SHORT,
    ratio: Number((shortSigma / longSigma).toFixed(3)),
  };
}

/* ── plain-English read-out for the dashboard ──────────────────────────── */
function describe(ctx, rev) {
  if (!ctx || !ctx.ok) return [`Not enough history yet (${ctx ? ctx.days : 0} days). This name is judged on today only until it has ${CONTEXT_MIN_DAYS}.`];
  const lines = [];
  lines.push(`Normally moves about ${ctx.atrPct}% a day. ${ctx.vol60Pct > ctx.vol120Pct * 1.25 ? "Currently more jumpy than its own normal." : ctx.vol60Pct < ctx.vol120Pct * 0.8 ? "Currently calmer than its own normal." : "Moving about as much as it usually does."}`);
  const pos = Math.round(ctx.rangePct6m * 100);
  lines.push(`Sitting ${pos}% of the way up its own six-month range — ${ctx.drawdown6mPct}% below its six-month high.`);
  if (ctx.aboveSma200 === true) lines.push("Above its 200-day average, so the longer trend is still up.");
  else if (ctx.aboveSma200 === false) lines.push("Below its 200-day average, so the longer trend is down.");
  if (ctx.downtrend) lines.push("Flagged as a DOWNTREND: below the 200-day, more than 20% off its high, and the 50-day line is still falling. Drops in a name like this have not historically been bargains.");
  if (ctx.gapFreqPct != null && ctx.gapFreqPct > 15) lines.push(`Opens more than 2% away from the previous close ${ctx.gapFreqPct}% of the time — an overnight hold here is genuinely risky.`);
  if (rev && rev.n) {
    lines.push(`After similar drops in the past ${rev.n} times, this name averaged ${rev.rawPct}% over the next week; blended with the roster average that becomes ${rev.shrunkPct}%. Only ${Math.round(rev.weightOwn * 100)}% of that is its own record — the sample is too small to trust further.`);
  }
  return lines;
}

module.exports = {
  DAILY_CHUNK_SYMBOLS,
  detectSplitSeams, repairSplits, SPLIT_MOVE_THRESHOLD,
  DAILY_COL, KEEP_DAYS, CONTEXT_MIN_DAYS,
  REVERSION_SHRINK_K, REVERSION_TRIGGER_SD, REVERSION_HORIZON,
  SIGMA_W_SHORT, BARS_PER_SESSION,
  dailyRef, readDaily, readDailyWithMeta, writeDaily, fetchDaily, fetchDailyWithMeta,
  ensureDailyHistory, chartSeries, mergeDailySeries,
  expectedShortfallLoss, contextFor, pullbackLeg, retracementAt, reversionEvents, shrinkReversion, reversionMultiplier,
  blendedSigma, describe,
  _internal: { mean, stdev, sma, pctRets },
};
