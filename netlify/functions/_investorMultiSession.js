"use strict";

/**
 * Investor_AI — FIVE-SESSION SELECTION.
 *
 * WHY THIS FILE EXISTS
 * --------------------------------------------------------------------------
 * The desk used to choose companies from ONE HOUR of trading: 12 five-minute
 * bars, scored against a baseline drawn from the same day and a half. That is
 * not enough information to know why a company fell or what its trend is. A
 * stock sliding for four straight sessions and a stock that dropped once on
 * news three days ago and has been flat since look identical through a
 * one-hour window, and the desk bought neither for a reason it could state.
 *
 * Selection now happens on FIVE COMPLETED TRADING SESSIONS, and the hour is
 * demoted to a timer: it only confirms the name is still soft right now.
 *
 * COMPLETED SESSIONS ONLY, AND WHY THAT MATTERS
 * --------------------------------------------------------------------------
 * Every number here comes from daily closes strictly BEFORE the decision date.
 * Today's price never enters. That is a deliberate choice and it removes three
 * whole classes of defect at once:
 *
 *   · no partial-session drift — a company cannot read "quiet" at 09:45 and
 *     "dislocated" at 15:45 because of the clock alone;
 *   · no bridge return off a live price, so a split taking effect TODAY —
 *     which a series cut at yesterday structurally cannot see — cannot
 *     manufacture a fifty-percent phantom fall;
 *   · the statistic is stable for the whole session, so the shortlist is
 *     fixed at the open and every name on it gets the whole day for the hour
 *     to confirm, rather than the two of them chasing each other.
 *
 * WHAT IT MEASURES
 * --------------------------------------------------------------------------
 * How far a company fell over five sessions RELATIVE TO ITS OWN SECTOR, scored
 * against its own normal five-day swing. A company down 6% in a sector that
 * fell 6% has not dislocated; it went where its industry went. The same fall
 * in a flat sector is the company's own.
 *
 * Peers are removed by MEDIAN, not mean, and always leave-one-out: one peer
 * mid-corporate-action cannot drag the leg it is being measured against, and
 * a company can never be part of its own benchmark.
 *
 * HONEST LIMITATION, STATED ONCE
 * --------------------------------------------------------------------------
 * This demeans by sector; it does not regress out a beta. A high-beta name in
 * a falling sector will look more dislocated than it is. A true daily residual
 * panel is the right eventual answer, but the intraday one cannot be reused —
 * its clock-span guard refuses any window crossing an overnight boundary — and
 * building one belongs in its own change with its own evidence. The sector
 * crowding cap is what bounds the damage in the meantime.
 *
 * PURE. No Firestore, no network, no clock. Every input is passed in, so every
 * branch is reachable from a fixture.
 */

const H = require("./_investorHistory");

/* Five completed sessions of return needs six closes. */
const SESSIONS = 5;
const CLOSES_NEEDED = SESSIONS + 1;
/* Enough history to estimate a sixty-return volatility that ENDS before the
   window being scored, plus room for the window itself. */
const MIN_DAILY_BARS = 90;
/* Below this a "sector" is one or two names and its median is that name. */
const MIN_SECTOR = 3;
/* Returns used for the scale estimate, ending strictly before the window. */
const VOL_LOOKBACK = 60;

function mean(a) { return a.length ? a.reduce((s, x) => s + x, 0) / a.length : 0; }

function median(a) {
  if (!a.length) return null;
  const v = [...a].sort((x, y) => x - y);
  const mid = v.length >> 1;
  return v.length % 2 ? v[mid] : (v[mid - 1] + v[mid]) / 2;
}

function stdev(a) {
  if (a.length < 2) return 0;
  const mu = mean(a);
  return Math.sqrt(a.reduce((s, x) => s + (x - mu) ** 2, 0) / (a.length - 1));
}

function closesOf(series) {
  return series.map((b) => Number(b.c)).filter((c) => Number.isFinite(c) && c > 0);
}

function dailyReturns(series) {
  const out = [];
  for (let i = 1; i < series.length; i += 1) {
    const prev = Number(series[i - 1].c), cur = Number(series[i].c);
    if (prev > 0 && cur > 0) out.push(cur / prev - 1);
  }
  return out;
}

/**
 * Is this daily series identified well enough to rank on?
 *
 * It must name its provider and carry the hash of the response it came from,
 * so the number is traceable to an attested fetch.
 *
 * It must NOT be required to be volume-provenance homogeneous. That flag says
 * every bar's VOLUME came from one continuous feed identity, and it latches
 * false forever the first time the feed changes — which happens routinely
 * here, because the provider rejects `delayed_sip` on this account and the
 * desk falls back to embargoed consolidated SIP. This statistic is computed
 * from CLOSES and never reads volume, so demanding volume homogeneity
 * rejected almost the whole roster for a property the measurement does not
 * use. It is reported instead, and the checks that genuinely depend on volume
 * identity — the correlation network, and execution provenance on the order
 * itself — are untouched and still enforce it.
 */
function trustedDailyProvenance(p) {
  return !!p && !!p.provider && /^[a-f0-9]{64}$/.test(String(p.sourceSha256 || ""));
}

/**
 * Cut a daily series to the decision date and repair splits INSIDE that cut,
 * in that order. Repairing first would rescale bars before a seam, letting a
 * split that happens after the decision reach back and rewrite the history the
 * decision was made on. This mirrors contextFor's ordering deliberately.
 */
function cutAndRepair(series, asOfDate) {
  const cut = (series || []).filter((b) => b && b.date && b.c != null
    && (!asOfDate || b.date < asOfDate));
  if (!cut.length) return [];
  return H.repairSplits(cut).series;
}

/**
 * Build the per-symbol five-session picture for the whole roster.
 *
 * Returns, per symbol, either a usable measurement or a NAMED reason it is
 * absent — "no history", "stale", "unprovenanced" and "too short" are four
 * very different problems and collapsing them into a silent skip is how a
 * coverage failure gets mistaken for "nothing qualified today".
 */
function buildSessionPanel(dailyBySymbol, provenanceBySymbol, opts = {}) {
  const asOfDate = opts.asOfDate || null;
  const sectorOf = typeof opts.sectorOf === "function" ? opts.sectorOf : () => "other";
  const symbols = Object.keys(dailyBySymbol || {});

  /* ── pass 1: usable spines, and how current each one is ─────────────── */
  const prepared = new Map();
  const rejected = { noHistory: [], tooShort: [], stale: [], provenance: [] };
  const lastDates = [];

  for (const sym of symbols) {
    const series = cutAndRepair(dailyBySymbol[sym], asOfDate);
    if (!series.length) { rejected.noHistory.push(sym); continue; }
    if (series.length < MIN_DAILY_BARS) { rejected.tooShort.push(sym); continue; }
    const closes = closesOf(series);
    if (closes.length < CLOSES_NEEDED) { rejected.tooShort.push(sym); continue; }
    prepared.set(sym, { series, closes, lastDate: series[series.length - 1].date });
    lastDates.push(series[series.length - 1].date);
  }

  /* The freshness test with no trading calendar: the date most of the roster
     agrees is the last completed session. A spine behind that is stale, and a
     stale spine is exactly how a five-day-old fall gets scored as today's. */
  const tally = new Map();
  for (const d of lastDates) tally.set(d, (tally.get(d) || 0) + 1);
  let modalLastDate = null, best = 0;
  for (const [d, n] of tally.entries()) {
    if (n > best || (n === best && d > modalLastDate)) { modalLastDate = d; best = n; }
  }

  /* ── pass 2: the five-session return for every fresh, provenanced name ── */
  const raw = new Map();
  for (const [sym, p] of prepared.entries()) {
    if (modalLastDate && p.lastDate !== modalLastDate) { rejected.stale.push(sym); continue; }
    if (!trustedDailyProvenance(provenanceBySymbol && provenanceBySymbol[sym])) {
      rejected.provenance.push(sym); continue;
    }
    const c = p.closes;
    const from = c[c.length - CLOSES_NEEDED], to = c[c.length - 1];
    if (!(from > 0) || !(to > 0)) { rejected.tooShort.push(sym); continue; }
    /* Scale from sixty returns ENDING BEFORE the window. Including the window
       inflates sigma with the very move being scored and shrinks its own
       score — and it does so hardest for exactly the names being selected. */
    const rets = dailyReturns(p.series);
    const baseline = rets.slice(-(VOL_LOOKBACK + SESSIONS), -SESSIONS);
    if (baseline.length < 30) { rejected.tooShort.push(sym); continue; }
    const sd60 = stdev(baseline);
    if (!(sd60 > 0)) { rejected.tooShort.push(sym); continue; }
    raw.set(sym, { r5: to / from - 1, sd5: sd60 * Math.sqrt(SESSIONS), sd60,
      volumeHomogeneous: !!(provenanceBySymbol && provenanceBySymbol[sym]
        && provenanceBySymbol[sym].homogeneous === true),
      anchorDate: p.series[p.series.length - CLOSES_NEEDED].date, lastDate: p.lastDate });
  }

  /* ── pass 3: sector legs, leave-one-out ─────────────────────────────── */
  const bySector = new Map();
  for (const sym of raw.keys()) {
    const sec = sectorOf(sym) || "other";
    if (!bySector.has(sec)) bySector.set(sec, []);
    bySector.get(sec).push(sym);
  }
  /* A sector of one or two is not a benchmark; pool the thin ones so those
     companies are still measured against something rather than themselves. */
  const sectorFor = new Map();
  const pooled = [];
  for (const [sec, members] of bySector.entries()) {
    if (members.length >= MIN_SECTOR) for (const s of members) sectorFor.set(s, sec);
    else pooled.push(...members);
  }
  for (const s of pooled) sectorFor.set(s, "misc");
  const groups = new Map();
  for (const [sym, sec] of sectorFor.entries()) {
    if (!groups.has(sec)) groups.set(sec, []);
    groups.get(sec).push(sym);
  }

  /* The market leg is the median of the SECTOR medians, so one crowded sector
     cannot stand in for the market. */
  const sectorMedianAll = new Map();
  for (const [sec, members] of groups.entries()) {
    sectorMedianAll.set(sec, median(members.map((s) => raw.get(s).r5)));
  }
  const marketLeg = median([...sectorMedianAll.values()].filter((x) => x != null));

  const bySymbol = {};
  for (const [sym, r] of raw.entries()) {
    const sec = sectorFor.get(sym) || "misc";
    const members = groups.get(sec) || [];
    /* Leave-one-out: a company is never part of the benchmark it is scored
       against, which otherwise flatters every name in a thin sector. */
    const peers = members.filter((s) => s !== sym).map((s) => raw.get(s).r5);
    const sectorLeg = peers.length >= MIN_SECTOR - 1 ? median(peers) : marketLeg;
    const m5 = marketLeg == null ? 0 : marketLeg;
    const p5 = sectorLeg == null ? m5 : sectorLeg;
    const x5 = r.r5 - p5;
    const sessionZ = r.sd5 > 0 ? x5 / r.sd5 : null;
    bySymbol[sym] = {
      ok: Number.isFinite(sessionZ),
      reason: Number.isFinite(sessionZ) ? null : "no_scale",
      sector: sec,
      sessions: SESSIONS,
      volumeHomogeneous: r.volumeHomogeneous,
      anchorDate: r.anchorDate, lastDate: r.lastDate,
      r5Pct: Number((r.r5 * 100).toFixed(2)),
      sessionZ: Number.isFinite(sessionZ) ? Number(sessionZ.toFixed(2)) : null,
      normalSwingPct: Number((r.sd5 * 100).toFixed(2)),
      /* The three parts sum to the total fall by construction. This is the
         operator's question — how much of it was the market, how much the
         industry, how much this company — answered arithmetically. */
      marketPartPct: Number((m5 * 100).toFixed(2)),
      sectorPartPct: Number(((p5 - m5) * 100).toFixed(2)),
      companyPartPct: Number((x5 * 100).toFixed(2)),
    };
  }

  /* Rank on the same measure, worst first, so "bottom fifth by five-session
     excess" is a statement about this roster on this day. */
  const scored = Object.entries(bySymbol)
    .filter(([, v]) => v.ok)
    .sort((a, b) => a.sessionZ - b.sessionZ);
  scored.forEach(([sym], i) => {
    bySymbol[sym].sessionRank = scored.length > 1
      ? Number((i / (scored.length - 1)).toFixed(4)) : 0;
  });

  return {
    bySymbol, modalLastDate, sessions: SESSIONS,
    measured: scored.length,
    considered: symbols.length,
    rejected: {
      noHistory: rejected.noHistory.length,
      tooShort: rejected.tooShort.length,
      stale: rejected.stale.length,
      provenance: rejected.provenance.length,
      staleSample: rejected.stale.slice(0, 10),
      provenanceSample: rejected.provenance.slice(0, 10),
    },
  };
}

/**
 * Does this company's five-session picture admit it to the shortlist?
 *
 * The fall must be real, it must be the company's own rather than its sector's,
 * and the company must still be in a rising longer trend — a five-session fall
 * inside a downtrend is a falling knife, not a pullback. PURE.
 */
function sessionAdmits(move, historyContext, policy = {}) {
  const minZ = Number(policy.minSessionMoveZ);
  const rankCap = Number(policy.sessionRankCap);
  if (!move || move.ok !== true) {
    return { pass: false, reason: move && move.reason ? move.reason : "no_five_session_measure" };
  }
  /* THE FALL MUST BE A FALL. Without this a company UP 4% in a sector up 8%
     has a large negative excess, clears every other test, and the card would
     announce a fall that never happened — the exact question this change
     exists to answer, answered wrongly. */
  if (!(move.r5Pct < 0)) {
    return { pass: false, reason: `up ${move.r5Pct}% over five sessions` };
  }
  if (!Number.isFinite(move.sessionZ) || !(move.sessionZ <= -minZ)) {
    return { pass: false, reason: `five-session move ${move.sessionZ} not past -${minZ}` };
  }
  if (Number.isFinite(rankCap) && Number.isFinite(move.sessionRank) && move.sessionRank > rankCap) {
    return { pass: false, reason: `five-session rank ${move.sessionRank} above ${rankCap}` };
  }
  const ctx = historyContext || {};
  if (ctx.ok !== true) return { pass: false, reason: "no six-month history to read the trend" };
  /* EITHER confirmation, not both. Demanding a company be above its 200-day
     average AND have a rising 50-day line, on top of a five-session fall,
     admitted ONE company out of 270 measured — the three conditions are
     negatively correlated by construction, because a company that just fell
     hard is the least likely to have a pristine trend. One confirmation plus
     the downtrend-warning cap still refuses a falling knife. */
  const mode = policy.sessionTrendMode === "both" ? "both" : "either";
  const above = ctx.aboveSma200 === true, rising = ctx.sma50Rising === true;
  if (mode === "both" && !(above && rising)) {
    return { pass: false, reason: !above
      ? "below its 200-day average — the longer trend is down"
      : "50-day line is not rising — this is a decline, not a pullback" };
  }
  if (mode === "either" && !above && !rising) {
    return { pass: false,
      reason: "below its 200-day average and its 50-day line is falling — a decline, not a pullback" };
  }
  const flags = Array.isArray(ctx.downtrendFlags) ? ctx.downtrendFlags.length
    : Number(ctx.downtrendFlags) || 0;
  if (flags > 1) return { pass: false, reason: `${flags} downtrend warnings` };
  return { pass: true, reason: "five-session fall in a rising trend" };
}

/** One sentence an owner can read, built only from numbers already computed. */
function describeMove(symbol, move) {
  if (!move || move.ok !== true) return null;
  const fell = move.r5Pct < 0;
  const size = Math.abs(move.r5Pct);
  const own = Math.abs(move.companyPartPct);
  const times = move.normalSwingPct > 0
    ? (own / move.normalSwingPct).toFixed(1) : null;
  return `${symbol} is ${fell ? "down" : "up"} ${size}% over the last five trading days. `
    + `Its sector moved ${move.sectorPartPct >= 0 ? "+" : ""}${move.sectorPartPct}% and the wider `
    + `market ${move.marketPartPct >= 0 ? "+" : ""}${move.marketPartPct}%, so about `
    + `${own} points of that is this company alone`
    + (times ? ` — roughly ${times} times its normal five-day swing.` : ".");
}

module.exports = {
  SESSIONS, MIN_DAILY_BARS, MIN_SECTOR, VOL_LOOKBACK, CLOSES_NEEDED,
  buildSessionPanel, sessionAdmits, describeMove,
  trustedDailyProvenance, cutAndRepair,
  _internal: { mean, median, stdev, dailyReturns },
};
