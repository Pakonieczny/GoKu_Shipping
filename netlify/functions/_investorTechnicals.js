"use strict";
/* Pre-cutoff technical context computed from completed daily bars. PURE.
   Every value is derived only from bars strictly before the decision cutoff, so
   nothing here can leak later prices. Values are integer basis points relative to
   the latest close unless the name says otherwise. Missing inputs yield null. */
const VERSION = "technicals.v1";
const num = (x) => { const n = Number(x); return Number.isFinite(n) ? n : null; };
// Keys ending in "Bps" cross the storage codec money boundary, so they are canonical integer strings.
const bps = (ratio) => (ratio == null || !Number.isFinite(ratio) ? null : String(Math.round(ratio * 10000)));
const mean = (xs) => (xs.length ? xs.reduce((a, b) => a + b, 0) / xs.length : null);
function clean(bars) {
  return (bars || []).map((b) => ({ date: b.date, o: num(b.o), h: num(b.h), l: num(b.l), c: num(b.c), v: num(b.v) || 0 }))
    .filter((b) => b.c > 0 && (b.h == null || b.h >= b.c || b.h > 0));
}
/* Average true range over n sessions, as bps of the latest close. */
function atrBps(rows, n) {
  if (rows.length < n + 1) return null;
  const last = rows.slice(-(n + 1)), tr = [];
  for (let i = 1; i < last.length; i++) {
    const b = last[i], prev = last[i - 1].c, h = b.h ?? b.c, l = b.l ?? b.c;
    tr.push(Math.max(h - l, Math.abs(h - prev), Math.abs(l - prev)));
  }
  return bps(mean(tr) / rows.at(-1).c);
}
/* Overnight gaps: open versus the previous close, absolute, in bps. */
function gaps(rows, n) {
  const last = rows.slice(-(n + 1)), out = [];
  for (let i = 1; i < last.length; i++) if (last[i].o > 0) out.push(Math.abs(last[i].o / last[i - 1].c - 1));
  return out;
}
function maBps(rows, n) {
  if (rows.length < n) return null;
  const ma = mean(rows.slice(-n).map((b) => b.c));
  return bps(rows.at(-1).c / ma - 1);
}
function retBps(rows, n) { return rows.length > n ? bps(rows.at(-1).c / rows[rows.length - 1 - n].c - 1) : null; }
function pct(xs, p) { if (!xs.length) return null; const s = [...xs].sort((a, b) => a - b); return s[Math.min(s.length - 1, Math.floor(p * s.length))]; }
/** Summarize completed daily bars; `market` is the benchmark's completed bars for relative strength. */
function summarize(bars, { market = null } = {}) {
  const rows = clean(bars);
  if (rows.length < 2) return { schemaVersion: VERSION, bars: rows.length, available: false, reason: "insufficient_daily_bars" };
  const mrows = clean(market);
  const rel = (n) => { const a = retBps(rows, n), b = mrows.length > n ? retBps(mrows, n) : null; return a == null || b == null ? null : String(Number(a) - Number(b)); };
  const gap20 = gaps(rows, 20), gap60 = gaps(rows, 60), last20 = rows.slice(-20);
  const ranges20 = last20.filter((b) => b.h != null && b.l != null).map((b) => (b.h - b.l) / b.c);
  const ma50Now = rows.length >= 50 ? mean(rows.slice(-50).map((b) => b.c)) : null;
  const ma50Then = rows.length >= 70 ? mean(rows.slice(-70, -20).map((b) => b.c)) : null;
  const dollarVolume20 = last20.map((b) => b.c * b.v);
  const upDays20 = last20.filter((b, i, a) => i > 0 && b.c > a[i - 1].c).length;
  const high252 = Math.max(...rows.slice(-252).map((b) => b.h ?? b.c)), low252 = Math.min(...rows.slice(-252).map((b) => b.l ?? b.c));
  return {
    schemaVersion: VERSION, bars: rows.length, available: true, asOfDate: rows.at(-1).date,
    basis: "Completed daily bars before the cutoff only. Basis points of the latest close. ATR = average true range. Relative values subtract the market benchmark's return over the same sessions.",
    volatility: { atr14Bps: atrBps(rows, 14), atr5Bps: atrBps(rows, 5), avgDailyRange20Bps: bps(mean(ranges20)), maxDailyRange20Bps: ranges20.length ? bps(Math.max(...ranges20)) : null,
      gapAbsMean20Bps: bps(mean(gap20)), gapAbsP90_60Bps: bps(pct(gap60, 0.9)), gapAbsMax60Bps: gap60.length ? bps(Math.max(...gap60)) : null },
    trend: { closeVsMa20Bps: maBps(rows, 20), closeVsMa50Bps: maBps(rows, 50), closeVsMa200Bps: maBps(rows, 200),
      ma50Slope20dBps: ma50Now && ma50Then ? bps(ma50Now / ma50Then - 1) : null, upDays20, closeVs52wHighBps: bps(rows.at(-1).c / high252 - 1), closeVs52wLowBps: bps(rows.at(-1).c / low252 - 1) },
    momentum: { return5dBps: retBps(rows, 5), return20dBps: retBps(rows, 20), return60dBps: retBps(rows, 60), return120dBps: retBps(rows, 120),
      relativeToMarket5dBps: rel(5), relativeToMarket20dBps: rel(20), relativeToMarket60dBps: rel(60) },
    liquidity: { avgDollarVolume20Usd: dollarVolume20.length ? Math.round(mean(dollarVolume20)) : null, minDollarVolume20Usd: dollarVolume20.length ? Math.round(Math.min(...dollarVolume20)) : null,
      latestVolumeRatio: rows.length > 20 ? Math.round(rows.at(-1).v / (mean(rows.slice(-21, -1).map((b) => b.v)) || 1) * 100) / 100 : null },
  };
}
/** Compact row for screening tables. */
function screeningRow(t) {
  if (!t || !t.available) return null;
  return { atr14Bps: t.volatility.atr14Bps, gapAbsMean20Bps: t.volatility.gapAbsMean20Bps, closeVsMa50Bps: t.trend.closeVsMa50Bps, closeVsMa200Bps: t.trend.closeVsMa200Bps,
    return20dBps: t.momentum.return20dBps, relativeToMarket20dBps: t.momentum.relativeToMarket20dBps, relativeToMarket60dBps: t.momentum.relativeToMarket60dBps, avgDollarVolume20Usd: t.liquidity.avgDollarVolume20Usd };
}
module.exports = { VERSION, summarize, screeningRow };
