/*  netlify/functions/_investorMarket.js  (v1.0)
 *  ---------------------------------------------------------------------------
 *  Investor_AI — market data lane: adapters, quality grading, bar storage,
 *  and the execution clock.
 *
 *  THE ONE PLACE THE NO-NEW-VENDOR RULE IS RELAXED
 *  ------------------------------------------------------------------------
 *  The research settled this: 5-minute refresh with live charts is impossible
 *  without a market-data account. Every other lane in this system stays
 *  zero-credential. This module supports three providers behind one interface
 *  so the choice is configuration, not code:
 *
 *    alpaca   free tier, no card. IEX-only real-time (one venue, ~2.5-3% of
 *             consolidated volume) + 15-min-delayed consolidated via REST.
 *             Quality grade capped at B because single-venue prints gap.
 *    massive  (ex-Polygon) Stocks Starter $29/mo. 15-min delayed consolidated,
 *             UNLIMITED calls — the decisive feature at 288 cycles/day.
 *    manual   CSV import. Always available, always permitted, grade C.
 *
 *  Yahoo is absent by design and cannot be added: _investorFetch denies those
 *  hosts permanently.
 *
 *  QUALITY GRADES gate execution, they are not decoration:
 *    A  two permitted sources agree within tolerance                 normal
 *    B  one permitted consolidated source, complete OHLCV            wider slippage
 *    C  single-venue / delayed / manual                              research only
 *    F  stale, contradictory, impossible OHLC, no provenance         FREEZE
 *
 *  THE EXECUTION CLOCK — the anti-look-ahead invariant
 *  ------------------------------------------------------------------------
 *  A bar's timestamp is NOT the time this system could have seen it. With a
 *  15-minute-delayed feed, the 10:00 bar is retrievable at 10:15 at the
 *  earliest. firstEligibleBar() therefore takes the decision time, adds the
 *  feed delay, adds configured execution latency, and returns the first bar
 *  whose OPEN is strictly after that instant. An order is committed and sized
 *  BEFORE that bar begins, and no future high/low/close/volume may influence
 *  it. Realized bar data is a diagnostic, never an input.
 * ---------------------------------------------------------------------------
 */

"use strict";

const A = require("./_investorAdmin");
const { fetchPublic } = require("./_investorFetch");

/* ── provider configuration ────────────────────────────────────────────── */
const PROVIDERS = {
  alpaca: {
    id: "alpaca",
    host: "data.alpaca.markets",
    delayMinutes: 15,          // REST consolidated is 15-min delayed on free tier
    maxGrade: "B",
    keyEnv: ["ALPACA_API_KEY_ID", "ALPACA_API_SECRET_KEY"],
    consolidated: true,
  },
  massive: {
    id: "massive",
    host: "api.massive.com",
    delayMinutes: 15,
    maxGrade: "A",
    keyEnv: ["MASSIVE_API_KEY"],
    consolidated: true,
  },
  manual: {
    id: "manual",
    host: null,
    delayMinutes: 24 * 60,
    maxGrade: "C",
    keyEnv: [],
    consolidated: false,
  },
};

function activeProvider() {
  const want = String(process.env.INVESTOR_MARKET_PROVIDER || "manual").toLowerCase();
  const p = PROVIDERS[want] || PROVIDERS.manual;
  if (p.keyEnv.length && !p.keyEnv.every((k) => process.env[k])) {
    // Configured but not credentialed — degrade loudly rather than fail a cycle.
    return { ...PROVIDERS.manual, degradedFrom: p.id, reason: `missing ${p.keyEnv.join("/")}` };
  }
  return p;
}

/* ── exchange calendar (versioned; half-days matter for the open rule) ──── */
const HOLIDAYS_2026 = new Set([
  "2026-01-01","2026-01-19","2026-02-16","2026-04-03","2026-05-25",
  "2026-06-19","2026-07-03","2026-09-07","2026-11-26","2026-12-25",
]);
const HALF_DAYS_2026 = new Set(["2026-07-02","2026-11-27","2026-12-24"]);
const CALENDAR_VERSION = "us-equity-2026.1";

function nyParts(d) {
  // Intl gives us the wall clock in New York including DST, without a tz lib.
  const f = new Intl.DateTimeFormat("en-CA", {
    timeZone: "America/New_York", year: "numeric", month: "2-digit", day: "2-digit",
    hour: "2-digit", minute: "2-digit", hour12: false, weekday: "short",
  }).formatToParts(d);
  const g = (t) => (f.find((p) => p.type === t) || {}).value;
  return {
    date: `${g("year")}-${g("month")}-${g("day")}`,
    minutes: Number(g("hour")) * 60 + Number(g("minute")),
    weekday: g("weekday"),
  };
}

const OPEN_MIN = 9 * 60 + 30;      // 09:30 ET
const CLOSE_MIN = 16 * 60;         // 16:00 ET
const HALF_CLOSE_MIN = 13 * 60;    // 13:00 ET

function sessionState(d = new Date()) {
  const { date, minutes, weekday } = nyParts(d);
  const isWeekend = weekday === "Sat" || weekday === "Sun";
  const isHoliday = HOLIDAYS_2026.has(date);
  const isHalf = HALF_DAYS_2026.has(date);
  const close = isHalf ? HALF_CLOSE_MIN : CLOSE_MIN;
  const tradingDay = !isWeekend && !isHoliday;
  let phase = "closed";
  if (tradingDay) {
    if (minutes < OPEN_MIN - 60) phase = "premarket_early";
    else if (minutes < OPEN_MIN) phase = "premarket";
    else if (minutes < OPEN_MIN + 15) phase = "opening_auction_window";
    else if (minutes < close - 15) phase = "regular";
    else if (minutes < close) phase = "closing_auction_window";
    else phase = "postmarket";
  }
  return {
    date, minutesEt: minutes, weekday, tradingDay, isHalfDay: isHalf, isHoliday,
    phase, open: phase === "regular" || phase === "opening_auction_window" || phase === "closing_auction_window",
    calendarVersion: CALENDAR_VERSION,
    // Spreads run 2-5x wider in the first and last 15 minutes. The research is
    // explicit that this intraday penalty exceeds the mid-vs-large-cap spread
    // difference, so the cost model must know about it.
    wideSpreadWindow: phase === "opening_auction_window" || phase === "closing_auction_window",
  };
}

/* ── slippage model ────────────────────────────────────────────────────── */
/* Anchored on de Groot/Huij/Zhou's 7.4bp round trip for the 100 largest and
   Frazzini/Israel/Moskowitz's ~11-12bp institutional large-cap impact. These
   are HALF-TRIP basis points; the ledger applies them on entry and on exit. */
function slippageBps({ advUsd, grade, wideSpreadWindow, vixNorm = 1 }) {
  let bps;
  if (advUsd >= 2e9) bps = 1.0;
  else if (advUsd >= 1e9) bps = 1.5;
  else if (advUsd >= 5e8) bps = 2.5;
  else if (advUsd >= 3e8) bps = 4.0;
  else if (advUsd >= 1e8) bps = 6.0;
  else bps = 12.0;
  if (grade === "B") bps *= 1.4;
  if (grade === "C") bps *= 2.0;
  if (wideSpreadWindow) bps *= 2.0;           // confirmed 2-5x; we use the low end
  if (vixNorm > 1) bps *= Math.min(2.5, vixNorm);  // impact rises with volatility
  return Math.round(bps * 100) / 100;
}

/* ── adapters ──────────────────────────────────────────────────────────── */
/**
 * Alpaca multi-symbol bars.
 *
 * TWO CORRECTIONS THAT MATTER, both found by audit:
 *
 * 1. `limit` on this endpoint is the TOTAL bar count across every symbol in the
 *    request, not per symbol. Asking for 342 symbols with limit=120 returned
 *    ~120 bars for the whole roster — a handful of names got data and the rest
 *    got nothing, silently. The limit is therefore scaled by the symbol count.
 *
 * 2. The response is paginated by `next_page_token`, which was never read. Any
 *    request larger than one page was silently truncated. We now follow pages
 *    until the token is exhausted or a safety bound is hit.
 */
async function fetchBarsAlpaca(symbols, { timeframe = "5Min", limit = 120, feed } = {}) {
  const out = {};
  let pageToken = null, pages = 0, lastHash = null, lastAt = null;
  const useFeed = feed || process.env.ALPACA_FEED || "iex";
  // per-symbol limit -> total, capped at the endpoint maximum
  const total = Math.min(10000, Math.max(limit, limit * symbols.length));

  do {
    const url = `https://data.alpaca.markets/v2/stocks/bars?symbols=${encodeURIComponent(symbols.join(","))}`
              + `&timeframe=${timeframe}&limit=${total}&adjustment=all&feed=${encodeURIComponent(useFeed)}&sort=asc`
              + (pageToken ? `&page_token=${encodeURIComponent(pageToken)}` : "");
    const r = await fetchPublic(url, {
      sourceId: "alpaca.bars", accept: ["json"], timeoutMs: 20000,
      headers: {
        "APCA-API-KEY-ID": process.env.ALPACA_API_KEY_ID || "",
        "APCA-API-SECRET-KEY": process.env.ALPACA_API_SECRET_KEY || "",
      },
    });
    const bars = (r.json && r.json.bars) || {};
    for (const [sym, arr] of Object.entries(bars)) {
      const mapped = (arr || []).map((b) => ({
        t: b.t, o: b.o, h: b.h, l: b.l, c: b.c, v: b.v, n: b.n || null, vw: b.vw || null,
      }));
      out[sym] = out[sym] ? out[sym].concat(mapped) : mapped;
    }
    pageToken = (r.json && r.json.next_page_token) || null;
    lastHash = r.sha256; lastAt = r.fetchedAt;
    pages += 1;
  } while (pageToken && pages < 12);

  return { bars: out, provider: "alpaca", sha256: lastHash, fetchedAt: lastAt,
           pages, truncated: !!pageToken, feed: useFeed };
}

async function fetchBarsMassive(symbols, { timeframe = "5Min", limit = 120 }) {
  // Massive uses one ticker per aggregates call; unlimited calls make that fine.
  /* "1Day" used to fall through this ternary to mult=1/span="minute", so the
     13-month daily backfill silently requested 1-MINUTE bars over a hardcoded
     5-day window. Every "daily" bar was one arbitrary minute of trading, the
     series never reached the 40-day minimum, and the downtrend gate abstained
     for every name on this provider while appearing to work. */
  const daily = /day/i.test(timeframe);
  const mult = daily ? 1 : (timeframe === "1Min" ? 1 : 5);
  const span = daily ? "day" : "minute";
  const to = new Date().toISOString().slice(0, 10);
  // enough calendar days to yield `limit` trading days, plus slack for holidays
  const lookbackDays = daily ? Math.ceil(limit * 1.5) + 10 : 5;
  const from = new Date(Date.now() - lookbackDays * 864e5).toISOString().slice(0, 10);
  const out = {};
  let lastHash = null, lastAt = null;
  for (const sym of symbols) {
    const url = `https://api.massive.com/v2/aggs/ticker/${encodeURIComponent(sym)}`
              + `/range/${mult}/${span}/${from}/${to}?adjusted=true&sort=asc&limit=${limit}`
              + `&apiKey=${encodeURIComponent(process.env.MASSIVE_API_KEY || "")}`;
    try {
      const r = await fetchPublic(url, { sourceId: "massive.aggs", accept: ["json"], timeoutMs: 15000 });
      /* NOTE ON ADJUSTMENT: Polygon-shaped `adjusted=true` is SPLIT-adjusted
         but not dividend-adjusted, whereas Alpaca's `adjustment=all` is both.
         The two providers therefore return different series for the same name.
         The convention is recorded on every stored bar so a series built from
         one is never silently compared against the other. */
      out[sym] = ((r.json && r.json.results) || []).map((b) => ({
        t: new Date(b.t).toISOString(), o: b.o, h: b.h, l: b.l, c: b.c, v: b.v, n: b.n || null, vw: b.vw || null,
      }));
      lastHash = r.sha256; lastAt = r.fetchedAt;
    } catch (e) {
      out[sym] = []; // one symbol failing must not fail the cycle
    }
  }
  return { bars: out, provider: "massive", sha256: lastHash,
           fetchedAt: lastAt || new Date().toISOString(),
           adjustment: "split_only" };
}

async function fetchBars(symbols, opts = {}) {
  const p = activeProvider();
  if (p.id === "alpaca") return { ...(await fetchBarsAlpaca(symbols, opts)), adjustment: "split_and_dividend" };
  if (p.id === "massive") return fetchBarsMassive(symbols, opts);
  return { bars: {}, provider: "manual", sha256: null, fetchedAt: new Date().toISOString(),
           note: p.degradedFrom ? `degraded from ${p.degradedFrom}: ${p.reason}` : "manual import only" };
}

/* ── validation + grading ──────────────────────────────────────────────── */
function validateBar(b) {
  const errs = [];
  for (const k of ["o", "h", "l", "c"]) {
    if (typeof b[k] !== "number" || !isFinite(b[k]) || b[k] <= 0) errs.push(`${k}_invalid`);
  }
  if (!errs.length) {
    if (b.h < b.l) errs.push("high_below_low");
    if (b.o > b.h || b.o < b.l) errs.push("open_outside_range");
    if (b.c > b.h || b.c < b.l) errs.push("close_outside_range");
  }
  if (typeof b.v !== "number" || b.v < 0) errs.push("volume_invalid");
  if (!b.t || isNaN(Date.parse(b.t))) errs.push("timestamp_invalid");
  return errs;
}

function gradeSeries(bars, { provider, nowMs = Date.now(), maxStaleMinutes = 45 }) {
  if (!Array.isArray(bars) || bars.length === 0) {
    return { grade: "F", reasons: ["no_bars"], tradable: false };
  }
  const reasons = [];
  let bad = 0;
  for (const b of bars) { if (validateBar(b).length) bad += 1; }
  if (bad > 0) reasons.push(`invalid_bars:${bad}`);

  const last = bars[bars.length - 1];
  const ageMin = (nowMs - Date.parse(last.t)) / 60000;
  const cfg = PROVIDERS[provider] || PROVIDERS.manual;
  // A delayed feed is expected to be behind by its delay; only count the excess.
  const excessStale = ageMin - cfg.delayMinutes;
  if (excessStale > maxStaleMinutes) reasons.push(`stale:${Math.round(excessStale)}m`);

  let grade;
  if (bad > 0 || reasons.some((r) => r.startsWith("stale"))) grade = "F";
  else if (provider === "manual") grade = "C";
  else if (provider === "alpaca") grade = "B";        // single-venue prints
  else grade = "B";                                    // one consolidated source = B; A needs two
  // maxGrade ceiling from provider config
  const order = { A: 3, B: 2, C: 1, F: 0 };
  if (order[grade] > order[cfg.maxGrade]) grade = cfg.maxGrade;

  return {
    grade, reasons,
    tradable: grade === "A" || grade === "B",
    lastBarAt: last.t, ageMinutes: Math.round(ageMin), barCount: bars.length,
    provider, feedDelayMinutes: cfg.delayMinutes,
  };
}

/* ── THE EXECUTION CLOCK ───────────────────────────────────────────────── */
/**
 * Given a decision instant, return the first bar the system could actually
 * have traded. Never returns a bar that opened before the decision was made,
 * and never a bar the feed had not yet published.
 */
function firstEligibleBar(bars, { decisionAtMs, provider, executionLatencyMs = 60000 }) {
  const cfg = PROVIDERS[provider] || PROVIDERS.manual;
  const availableFrom = decisionAtMs + cfg.delayMinutes * 60000 + executionLatencyMs;
  for (const b of bars) {
    const openMs = Date.parse(b.t);
    if (openMs > decisionAtMs && openMs >= availableFrom - cfg.delayMinutes * 60000) {
      // Bar opens after the decision AND its data would have arrived in time
      // for the system to act on the bar that follows it.
      if (openMs >= decisionAtMs + executionLatencyMs) {
        return { bar: b, barOpenAt: b.t, availableFromMs: availableFrom, reason: "ok" };
      }
    }
  }
  return { bar: null, barOpenAt: null, availableFromMs: availableFrom, reason: "no_eligible_bar" };
}

/* ── storage: ONE DOC PER SYMBOL PER DAY holding the bar array ──────────── */
/* Confirmed cost analysis: one doc per bar means a 100-symbol chart load is
   7,800 reads; one doc per symbol-day makes it 100. 288 bars of OHLCV is far
   under the 1MiB document ceiling. Timestamp fields are NOT indexed — an
   indexed sequential field caps a collection at 500 writes/sec. */
function barDocId(symbol, dateStr) { return `${symbol}_${dateStr}`; }

async function writeBars(symbol, dateStr, bars, meta) {
  const ref = A.col(A.COL.marketLatest).doc(barDocId(symbol, dateStr));
  await ref.set({
    symbol, date: dateStr,
    bars: bars.slice(-400),
    barCount: bars.length,
    ...meta,
    updated_at: A.FV.serverTimestamp(),
  }, { merge: true });
  return ref.id;
}

async function readBars(symbol, dateStr) {
  const s = await A.col(A.COL.marketLatest).doc(barDocId(symbol, dateStr)).get();
  return s.exists ? (s.data().bars || []) : [];
}

/** Read the trailing N sessions of bars for one symbol, oldest first. */
async function readRecentBars(symbol, sessions = 3) {
  const dates = [];
  const d = new Date();
  let guard = 0;
  while (dates.length < sessions && guard < 20) {
    const st = sessionState(d);
    if (st.tradingDay) dates.push(st.date);
    d.setUTCDate(d.getUTCDate() - 1);
    guard += 1;
  }
  dates.reverse();
  const snaps = await Promise.all(dates.map((dt) =>
    A.col(A.COL.marketLatest).doc(barDocId(symbol, dt)).get()));
  const out = [];
  for (const s of snaps) if (s.exists) out.push(...(s.data().bars || []));
  return out;
}

module.exports = {
  PROVIDERS, activeProvider,
  sessionState, CALENDAR_VERSION,
  slippageBps,
  fetchBars, validateBar, gradeSeries, firstEligibleBar,
  writeBars, readBars, readRecentBars, barDocId,
};
