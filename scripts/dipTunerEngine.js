/*  scripts/dipTunerEngine.js
 *  The fast replay engine behind the dip-rule tuner. Pure computation, no
 *  network, no Firestore: it is loaded by the local runner's worker threads.
 *
 *  Why it is fast: the expensive work (every "high then a lower low" pair
 *  inside a window, the stock's noise before each fall) does not depend on
 *  the settings being tuned, so it is computed once per window size and
 *  stored in typed arrays. Evaluating one parameter set is then a filter
 *  over those pairs plus a short walk forward for each entry.
 *
 *  It mirrors netlify/functions/_investorDipSim.js replayDate bar for bar:
 *  minute closes as prints, the same noise formula, settle rules, sizing,
 *  stop/target/time exits, 5 bps slippage and $0.005 a share each way.
 *  scripts/dipTunerCheck.js proves the two agree on the same bars.
 */
"use strict";

const SLIPPAGE = 0.0005, FEE_PER_SHARE = 0.005;
const SESSION_MINUTES = 390;
const DEFAULT_SIGMA = 0.08, MIN_SIGMA = 0.02;

/* ── dataset: bars in shared typed arrays ─────────────────────────────── */
/** byMonth: array of { symbol, byDate: { date: [{i,o,h,l,c}, …] } } → dataset.
 *  Layout: for symbol-day k, bars occupy positions off[k] .. off[k+1]-1 in
 *  minute (0..389), and o/h/l/c in cents; sigmaAt[p] is the stock's noise
 *  from the closes up to and including bar p (the live formula). */
function buildDataset(byMonth, { shared = true } = {}) {
  const symbols = [...new Set(byMonth.map((m) => m.symbol))].sort();
  let total = 0, count = 0;
  for (const m of byMonth) for (const bars of Object.values(m.byDate || {})) { total += bars.length; count++; }
  const b = datasetBuilder({ symbols, totalBars: total, totalSymbolDays: count, shared });
  for (const m of byMonth) for (const [date, bars] of Object.entries(m.byDate || {})) b.add(m.symbol, date, bars);
  return b.finish();
}
/** Streaming builder: add one company-day at a time (any order), then
 *  finish(). Memory is the typed arrays only. Capacities are upper bounds. */
function datasetBuilder({ symbols, totalBars, totalSymbolDays, shared = true }) {
  const symIndex = new Map(symbols.map((s, i) => [s, i]));
  const Alloc = (T, n) => new T(shared ? new SharedArrayBuffer(Math.max(1, n) * T.BYTES_PER_ELEMENT) : new ArrayBuffer(Math.max(1, n) * T.BYTES_PER_ELEMENT));
  const cap = totalSymbolDays + 1;
  const sdSym = Alloc(Int16Array, cap), off = Alloc(Int32Array, cap + 1);
  const minute = Alloc(Int16Array, totalBars), o = Alloc(Int32Array, totalBars), h = Alloc(Int32Array, totalBars), l = Alloc(Int32Array, totalBars), c = Alloc(Int32Array, totalBars), sigmaAt = Alloc(Float32Array, totalBars);
  const sdDate = [];
  let k = 0, p = 0;
  const closes = new Float64Array(SESSION_MINUTES + 8);
  return {
    add(symbol, date, bars) {
      if (!symIndex.has(symbol) || !bars || bars.length < 5 || k >= cap) return;
      const rows = bars.slice().sort((a, b) => a.i - b.i);
      const startP = p; let n = 0;
      for (const b of rows) {
        if (!(b.i >= 0 && b.i < SESSION_MINUTES) || !(b.c > 0) || p >= totalBars) continue;
        if (n && minute[p - 1] === b.i) continue;
        minute[p] = b.i; o[p] = Math.round(b.o * 100); h[p] = Math.round(b.h * 100); l[p] = Math.round(b.l * 100); c[p] = Math.round(b.c * 100);
        closes[n] = b.c; sigmaAt[p] = sigmaOfCloses(closes, n + 1);
        p++; n++;
      }
      if (n < 5) { p = startP; return; }
      sdSym[k] = symIndex.get(symbol); off[k] = startP; sdDate[k] = date; k++;
    },
    finish() {
      off[k] = p;
      const dates = [...new Set(sdDate)].sort();
      const dayIndex = new Map(dates.map((d, i) => [d, i]));
      const sdDay = Alloc(Int16Array, Math.max(1, k));
      for (let i = 0; i < k; i++) sdDay[i] = dayIndex.get(sdDate[i]);
      // symbol-days grouped by day, in symbol order, like the simulator's loop
      const order = Alloc(Int32Array, Math.max(1, k));
      const idx = Array.from({ length: k }, (_, i) => i).sort((a, b) => (sdDay[a] - sdDay[b]) || (sdSym[a] - sdSym[b]));
      order.set(idx);
      const dayStart = Alloc(Int32Array, dates.length + 1);
      let at = 0; for (let d = 0; d < dates.length; d++) { dayStart[d] = at; while (at < k && sdDay[order[at]] === d) at++; } dayStart[dates.length] = k;
      return { symbols, dates, count: k, sdSym, sdDay, off, order, dayStart, minute, o, h, l, c, sigmaAt, bars: p };
    },
  };
}
/** minuteSigmaPct over closes[0..n-1] (the last 61 closes → 60 returns). */
function sigmaOfCloses(closes, n) {
  const from = Math.max(0, n - 61), m = n - from;
  if (m - 1 < 5) return DEFAULT_SIGMA;
  let sum = 0; const rs = new Float64Array(m - 1);
  for (let i = 1; i < m; i++) { rs[i - 1] = Math.log(closes[from + i] / closes[from + i - 1]); sum += rs[i - 1]; }
  const mean = sum / rs.length; let v = 0;
  for (let i = 0; i < rs.length; i++) v += (rs[i] - mean) * (rs[i] - mean);
  v /= rs.length - 1;
  return Math.max(MIN_SIGMA, Math.sqrt(v) * 100);
}
/** The live rule: noise from completed bars up to the fall's high; if
 *  fewer than 6 exist, from every completed bar so far (positions < p). */
function sigmaFor(ds, base, hi, p) {
  if (hi + 1 >= 6) return ds.sigmaAt[base + hi];
  if (p >= 6) return ds.sigmaAt[base + p - 1];
  return DEFAULT_SIGMA;
}

/* ── pairs: every (high, later low) that is current at some minute ────── */
/** For one window size, over symbol-days [from, to): the pair (hi, lo)
 *  the live detector would see at each minute, run-length encoded as
 *  [start, end] bar positions. Only falls of at least minDropPct are kept. */
function computePairs(ds, windowMin, minDropPct, from = 0, to = ds.count) {
  const sdA = [], hiA = [], loA = [], stA = [], enA = [];
  const deque = new Int32Array(SESSION_MINUTES + 8);
  for (let k = from; k < to; k++) {
    const base = ds.off[k], n = ds.off[k + 1] - base;
    let head = 0, tail = 0;
    let curHi = -1, curLo = -1, curStart = -1;
    let loVal = 0;
    const flush = (endPos) => {
      if (curHi >= 0 && curLo > curHi) {
        const high = ds.h[base + curHi], low = ds.l[base + curLo];
        if ((high - low) / high * 100 >= minDropPct) { sdA.push(k); hiA.push(curHi); loA.push(curLo); stA.push(curStart); enA.push(endPos); }
      }
    };
    for (let p = 0; p < n; p++) {
      const m = ds.minute[base + p], minMinute = m + 1 - windowMin;
      // sliding maximum of highs (earliest bar wins ties)
      while (tail > head && ds.h[base + deque[tail - 1]] < ds.h[base + p]) tail--;
      deque[tail++] = p;
      while (tail > head && ds.minute[base + deque[head]] < minMinute) head++;
      const hi = deque[head];
      let lo;
      if (hi === curHi && curLo >= 0) { lo = curLo; if (ds.l[base + p] < loVal) { lo = p; } }
      else { lo = -1; for (let q = hi; q <= p; q++) if (lo < 0 || ds.l[base + q] < ds.l[base + lo]) lo = q; }
      const recentCount = p - firstInWindow(ds, base, p, minMinute) + 1;
      const valid = recentCount >= 3 && lo > hi;
      if (!valid) { flush(p - 1); curHi = -1; curLo = -1; curStart = -1; continue; }
      if (hi !== curHi || lo !== curLo) { flush(p - 1); curHi = hi; curLo = lo; curStart = p; loVal = ds.l[base + lo]; }
    }
    flush(n - 1);
  }
  return { windowMin, count: sdA.length, sd: Int32Array.from(sdA), hi: Int16Array.from(hiA), lo: Int16Array.from(loA), start: Int16Array.from(stA), end: Int16Array.from(enA) };
}
function firstInWindow(ds, base, p, minMinute) { let q = p; while (q > 0 && ds.minute[base + q - 1] >= minMinute) q--; return q; }
/** Copy a pairs object into shared memory so worker threads can read it. */
function sharePairs(pairs) {
  const S = (arr, T) => { const out = new T(new SharedArrayBuffer(arr.length * T.BYTES_PER_ELEMENT)); out.set(arr); return out; };
  return { windowMin: pairs.windowMin, count: pairs.count, sd: S(pairs.sd, Int32Array), hi: S(pairs.hi, Int16Array), lo: S(pairs.lo, Int16Array), start: S(pairs.start, Int16Array), end: S(pairs.end, Int16Array) };
}
function concatPairs(parts) {
  const cat = (T, key) => { const n = parts.reduce((s, x) => s + x[key].length, 0); const out = new T(n); let at = 0; for (const x of parts) { out.set(x[key], at); at += x[key].length; } return out; };
  return { windowMin: parts[0].windowMin, count: parts.reduce((s, x) => s + x.count, 0), sd: cat(Int32Array, "sd"), hi: cat(Int16Array, "hi"), lo: cat(Int16Array, "lo"), start: cat(Int16Array, "start"), end: cat(Int16Array, "end") };
}
/** Index: first pair position for each symbol-day (pairs are in sd order). */
function pairIndex(pairs, sdCount) {
  const first = new Int32Array(sdCount + 1).fill(-1);
  for (let i = pairs.count - 1; i >= 0; i--) first[pairs.sd[i]] = i;
  first[sdCount] = pairs.count;
  for (let k = sdCount - 1; k >= 0; k--) if (first[k] < 0) first[k] = first[k + 1];
  return first;
}

/* ── one parameter set over the whole dataset ─────────────────────────── */
function newPeriod() { return { trades: 0, wins: 0, pnlUsd: 0, grossWin: 0, grossLoss: 0, maxDrawdownUsd: 0, peak: 0, equity: 0, exits: { TARGET: 0, STOP: 0, TIME_LIMIT: 0 }, holdMin: 0 }; }
function finishPeriod(pr) {
  const r2 = (v) => Math.round(v * 100) / 100;
  return { trades: pr.trades, wins: pr.wins, winRatePct: pr.trades ? Math.round(pr.wins / pr.trades * 1000) / 10 : 0, pnlUsd: r2(pr.pnlUsd), maxDrawdownUsd: r2(pr.maxDrawdownUsd), profitFactor: pr.grossLoss > 0 ? Math.round(pr.grossWin / pr.grossLoss * 100) / 100 : (pr.grossWin > 0 ? 99 : 0), avgTradeUsd: pr.trades ? r2(pr.pnlUsd / pr.trades) : 0, avgHoldMin: pr.trades ? Math.round(pr.holdMin / pr.trades) : 0, exits: pr.exits };
}
/** Replay every day with one parameter set. `pairsByWindow[windowMin]`
 *  must hold the pairs for params.dropWindowMin. `splitDay` is the first
 *  day index of the held-out period (cash restarts there). */
function evaluate(ds, pairsByWindow, pairFirstByWindow, params, { splitDay = ds.dates.length, budgetUsd = 50000, collect = false } = {}) {
  const trades = collect ? [] : null;
  const pairs = pairsByWindow[params.dropWindowMin], first = pairFirstByWindow[params.dropWindowMin];
  if (!pairs) throw new Error("no pairs for window " + params.dropWindowMin);
  const zMin = params.zMinTenths / 10, speedFactor = params.speedFactorPct / 100, bandSig = params.stabilitySigmaTenths / 10;
  const flattenMinute = SESSION_MINUTES - params.closeBeforeCloseMin;         // nowMinute (m+1) >= this → flatten
  const noEntryAfter = flattenMinute - params.maxHoldMin / 3;                  // nowMinute >= this → no new entries
  const train = newPeriod(), test = newPeriod();
  let cash = budgetUsd, period = train;
  // per symbol-day state is local to the day; per symbol cooldown carries across the day only
  const cooldownUntil = new Float64Array(ds.symbols.length);                   // in nowMinute units within the day
  const exits = [];                                                            // pending exits this day: {minute, sym, cashBack}
  for (let d = 0; d < ds.dates.length; d++) {
    if (d === splitDay) { period = test; cash = budgetUsd; }
    cooldownUntil.fill(-1);
    exits.length = 0;

    // candidate entries, earliest first, re-scanned as positions open and close
    const heap = [];                                                           // [nowMinute, k, pairPos, fromPos]
    const before = (a, b) => a[0] < b[0] || (a[0] === b[0] && ds.sdSym[a[1]] < ds.sdSym[b[1]]);
    const push = (e) => { heap.push(e); let i = heap.length - 1; while (i > 0) { const j = (i - 1) >> 1; if (!before(heap[i], heap[j])) break; [heap[i], heap[j]] = [heap[j], heap[i]]; i = j; } };
    const pop = () => { const top = heap[0], last = heap.pop(); if (heap.length) { heap[0] = last; let i = 0; for (;;) { const a = 2 * i + 1, b = a + 1; let s = i; if (a < heap.length && before(heap[a], heap[s])) s = a; if (b < heap.length && before(heap[b], heap[s])) s = b; if (s === i) break; [heap[i], heap[s]] = [heap[s], heap[i]]; i = s; } } return top; };
    const tradedLo = new Set();
    for (let i = ds.dayStart[d]; i < ds.dayStart[d + 1]; i++) { const kk = ds.order[i]; const cand = nextCandidate(kk, first[kk], 0); if (cand) push(cand); }
    let dayPnl = 0;
    while (heap.length) {
      const cand = pop();
      const [nowMinute, kk, pairPos, pos] = cand;
      // settle exits due before this entry so their cash is back
      const base = ds.off[kk], sym = ds.sdSym[kk];
      for (let i = 0; i < exits.length; i++) if (exits[i] && (exits[i].minute < nowMinute || (exits[i].minute === nowMinute && exits[i].sym < sym))) { cash += exits[i].cashBack; exits[i] = null; }
      const lo = pairs.lo[pairPos];
      if (tradedLo.has(kk * 1024 + lo)) { const n = nextCandidate(kk, pairPos, pos + 1); if (n) push(n); continue; }
      const free = cash - 1;
      if (free < params.minTradeUsd) { const n = nextCandidate(kk, pairPos, pos + 1); if (n) push(n); continue; }
      const f = cand[4];                                                       // features from the candidate scan
      const tradeUsd = Math.min(sizeFor(f.confidence, params), Math.floor(free));
      const entryPx = f.close * (1 + SLIPPAGE);
      const qty = Math.floor(tradeUsd / (entryPx + FEE_PER_SHARE));
      tradedLo.add(kk * 1024 + lo);
      if (qty < 1) { const n = nextCandidate(kk, pairPos, pos + 1); if (n) push(n); continue; }
      const entryFee = qty * FEE_PER_SHARE;
      cash -= entryPx * qty + entryFee;
      const fall = f.high - f.low;
      const stop = Math.max(f.low - fall * params.stopPct / 100, entryPx - fall * params.retracePct / 100);
      const target = entryPx + fall * params.retracePct / 100;
      const deadline = nowMinute + params.maxHoldMin;
      // walk forward to the exit
      const n = ds.off[kk + 1] - base;
      let exitPx = null, exitMinute = null, role = "TIME_LIMIT";
      let lastClose = f.close, lastMinute = nowMinute;
      for (let q = pos + 1; q < n; q++) {
        const mq = ds.minute[base + q] + 1, bl = ds.l[base + q] / 100, bh = ds.h[base + q] / 100, bo = ds.o[base + q] / 100, bc = ds.c[base + q] / 100;
        lastClose = bc; lastMinute = mq;
        if (bl <= stop) { role = "STOP"; exitPx = Math.min(stop, bo); exitMinute = mq; break; }
        if (bh >= target) { role = "TARGET"; exitPx = target; exitMinute = mq; break; }
        if (mq >= deadline) { role = "TIME_LIMIT"; exitPx = bc; exitMinute = mq; break; }
        if (mq >= flattenMinute) { role = "TIME_LIMIT"; exitPx = bc; exitMinute = mq; break; }
      }
      if (exitPx == null) { exitPx = lastClose; exitMinute = Math.max(lastMinute, SESSION_MINUTES); }
      const px = exitPx * (1 - SLIPPAGE), exitFee = qty * FEE_PER_SHARE;
      const pnl = Math.round(((px - entryPx) * qty - exitFee - entryFee) * 100) / 100;
      exits.push({ minute: exitMinute, sym, cashBack: px * qty - exitFee });
      period.trades++; if (pnl >= 0) { period.wins++; period.grossWin += pnl; } else period.grossLoss -= pnl;
      period.pnlUsd += pnl; period.exits[role]++; period.holdMin += exitMinute - nowMinute; dayPnl += pnl;
      cooldownUntil[sym] = exitMinute + params.cooldownMin;
      if (trades) trades.push({ symbol: ds.symbols[sym], date: ds.dates[d], enteredAt: "T" + hhmm(nowMinute), qty, entry: Math.round(entryPx * 100) / 100, exit: Math.round(px * 100) / 100, role, pnlUsd: pnl, holdMin: exitMinute - nowMinute, confidence: f.confidence });
      // this symbol is free again after the exit (and its cooldown)
      const resume = firstPosAtOrAfter(kk, Math.max(exitMinute + 1, exitMinute + params.cooldownMin));
      const next = nextCandidate(kk, pairPos, resume);
      if (next) push(next);
    }
    for (const e of exits) if (e) cash += e.cashBack;
    period.equity += dayPnl;
    if (period.equity > period.peak) period.peak = period.equity;
    const dd = period.peak - period.equity; if (dd > period.maxDrawdownUsd) period.maxDrawdownUsd = dd;
  }
  return { train: finishPeriod(train), test: finishPeriod(test), trades };

  /** first bar position of symbol-day kk whose nowMinute (minute+1) ≥ target */
  function firstPosAtOrAfter(kk, targetNow) {
    const base = ds.off[kk], n = ds.off[kk + 1] - base;
    let lo = 0, hi = n;
    while (lo < hi) { const mid = (lo + hi) >> 1; if (ds.minute[base + mid] + 1 < targetNow) lo = mid + 1; else hi = mid; }
    return lo;
  }
  /** The next minute at or after bar position `fromPos` where symbol-day kk
   *  would buy: a current, qualifying pair that has settled, outside the
   *  blackout and the end-of-day cutoff. Returns [nowMinute, kk, pairPos, pos, features]. */
  function nextCandidate(kk, pairPosFrom, fromPos) {
    const base = ds.off[kk], n = ds.off[kk + 1] - base, sym = ds.sdSym[kk];
    const pEnd = first[kk + 1];
    let pp = Math.max(pairPosFrom, first[kk]);
    while (pp < pEnd && pairs.end[pp] < fromPos) pp++;
    for (; pp < pEnd; pp++) {
      const hi = pairs.hi[pp], lo = pairs.lo[pp], end = pairs.end[pp];
      const high = ds.h[base + hi] / 100, low = ds.l[base + lo] / 100, dropPct = (high - low) / high * 100;
      const minutes = Math.max(1, ds.minute[base + lo] - ds.minute[base + hi]);
      const lowV = ds.l[base + lo];
      if (hi + 1 >= 6) {                                                    // noise is fixed for this pair: judge it once
        const sg = ds.sigmaAt[base + hi], z0 = dropPct / (sg * Math.sqrt(minutes)), sp0 = (dropPct / minutes) / sg;
        const req0 = Math.min(params.dropBps / 100 * 3, params.dropBps / 100 * (1 + speedFactor * Math.max(0, sp0 - 1)));
        if (!(dropPct >= req0) || !(z0 >= zMin)) continue;
      }
      for (let p = Math.max(fromPos, pairs.start[pp]); p <= end; p++) {
        const nowMinute = ds.minute[base + p] + 1;
        if (nowMinute >= noEntryAfter) return null;                            // nothing later today either
        if (nowMinute < cooldownUntil[sym]) continue;
        if (nowMinute < params.noEntryBeforeMin) continue;
        const sigma = sigmaFor(ds, base, hi, p);
        const z = dropPct / (sigma * Math.sqrt(minutes)), speed = (dropPct / minutes) / sigma;
        const requiredPct = Math.min(params.dropBps / 100 * 3, params.dropBps / 100 * (1 + speedFactor * Math.max(0, speed - 1)));
        if (!(dropPct >= requiredPct) || !(z >= zMin)) continue;
        const need = Math.min(params.settleBars * 4, Math.max(params.settleBars, Math.round(params.settleBars * (1 + speedFactor * Math.max(0, speed - 1)))));
        // settle: completed bars after the low are positions lo+1 .. p-1
        const after = p - 1 - lo;
        if (after < need) continue;
        const bandPct = bandSig * sigma, band = low * (1 + bandPct / 100);
        let ok = true, cMax = -Infinity, cMin = Infinity;
        for (let q = p - need; q <= p - 1; q++) {
          const ql = ds.l[base + q], qc = ds.c[base + q] / 100;
          if (ql < lowV) { ok = false; break; }
          if (qc > band * (1 + bandPct / 100)) { ok = false; break; }
          if (qc > cMax) cMax = qc; if (qc < cMin) cMin = qc;
        }
        if (!ok) continue;
        const firstC = ds.c[base + p - need] / 100, lastC = ds.c[base + p - 1] / 100;
        if (lastC < firstC) continue;
        if (!(ds.c[base + p - 1] > lowV) || (need >= 2 && !(ds.c[base + p - 2] > lowV))) continue;
        const close = ds.c[base + p] / 100;
        if (!(close > low)) continue;
        const stability = Math.max(0, 1 - (cMax - cMin) / Math.max(1e-9, low * bandPct / 100));
        const zPart = Math.max(0, Math.min(1, (z - zMin) / (2 * zMin)));
        const speedPen = Math.max(0.5, Math.min(1, 1 - 0.15 * Math.max(0, speed - 1)));
        const confidence = Math.round((0.5 * zPart + 0.5 * Math.max(0, Math.min(1, stability))) * speedPen * 100) / 100;
        return [nowMinute, kk, pp, p, { high, low, close, confidence, z, speed, dropPct }];
      }
    }
    return null;
  }
}
function hhmm(nowMinute) { const t = 570 + nowMinute; return String(Math.floor(t / 60)).padStart(2, "0") + ":" + String(t % 60).padStart(2, "0"); }
function sizeFor(confidence, s) { return Math.round(s.minTradeUsd + (s.maxTradeUsd - s.minTradeUsd) * Math.max(0, Math.min(1, confidence))); }

module.exports = { SLIPPAGE, FEE_PER_SHARE, buildDataset, datasetBuilder, sigmaOfCloses, computePairs, sharePairs, concatPairs, pairIndex, evaluate, sizeFor };
