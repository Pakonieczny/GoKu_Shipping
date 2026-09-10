/*  scripts/dipTunerCheck.js
 *  Proves the fast tuner engine matches the exact dip simulator on the same
 *  bars: random-walk days with injected falls, replayed by both, trade by
 *  trade. Run: node scripts/dipTunerCheck.js
 */
"use strict";
process.env.FIREBASE_PRIVATE_KEY = process.env.FIREBASE_PRIVATE_KEY || "";
const SIM = require("../netlify/functions/_investorDipSim");
const DIP = require("../netlify/functions/_investorDipReversal");
const M = require("../netlify/functions/_investorMarket");
const E = require("./dipTunerEngine");

let seed = Number(process.argv[2] || 7);
function rnd() { seed = (seed * 1103515245 + 12345) % 2147483648; return seed / 2147483648; }
function gauss() { return Math.sqrt(-2 * Math.log(rnd() + 1e-12)) * Math.cos(2 * Math.PI * rnd()); }
function day(px, sigma) {
  const bars = []; let p = px;
  const dips = [Math.floor(20 + rnd() * 100), Math.floor(150 + rnd() * 150)];
  for (let i = 0; i < 390; i++) {
    if (rnd() < 0.02) continue;                       // a missing minute here and there
    let drift = 0;
    for (const d of dips) { if (i >= d && i < d + 8) drift = -sigma * 2.5; if (i >= d + 12 && i < d + 30) drift = sigma * 0.8; }
    const o = p, c = p * (1 + (drift + gauss() * sigma) / 100);
    const h = Math.max(o, c) * (1 + Math.abs(gauss()) * sigma / 300), l = Math.min(o, c) * (1 - Math.abs(gauss()) * sigma / 300);
    bars.push({ i, o: +o.toFixed(2), h: +h.toFixed(2), l: +l.toFixed(2), c: +c.toFixed(2), v: 1000 });
    p = c;
  }
  return bars;
}
const symbols = ["AAA", "BBB", "CCC", "DDD", "EEE", "FFF"];
const dates = SIM.tradingDates("2026-06-01", "2026-06-30");
const byMonth = [];
const bySymbolDate = {};
for (const s of symbols) { const byDate = {}; let px = 20 + rnd() * 300; for (const d of dates) { byDate[d] = day(px, 0.05 + rnd() * 0.15); px = byDate[d][byDate[d].length - 1].c; } byMonth.push({ symbol: s, byDate }); bySymbolDate[s] = byDate; }

const settings = DIP.normalizeSettings({ dropBps: 100, zMinTenths: 20, noEntryBeforeMin: 0, settleBars: 2, retracePct: 80, stopPct: 60, maxHoldMin: 60, minTradeUsd: 5000, maxTradeUsd: 25000, budgetUsd: 50000 });
/* exact */
const state = { cash: settings.budgetUsd }; const exact = [];
for (const d of dates) {
  const barsBySymbol = {}; for (const s of symbols) barsBySymbol[s] = (bySymbolDate[s][d] || []).map((b) => ({ ...b, t: M.nyWallClockToUtcMs(d, 570) + b.i * 60000, done: true }));
  const r = SIM.replayDate({ date: d, barsBySymbol, settings, state });
  exact.push(...r.trades);
}
/* fast */
const ds = E.buildDataset(byMonth, { shared: false });
const pairs = { [settings.dropWindowMin]: E.computePairs(ds, settings.dropWindowMin, 0.5) };
const first = { [settings.dropWindowMin]: E.pairIndex(pairs[settings.dropWindowMin], ds.count) };
const fast = E.evaluate(ds, pairs, first, settings, { splitDay: ds.dates.length, budgetUsd: settings.budgetUsd });
const exactPnl = Math.round(exact.reduce((n, t) => n + t.pnlUsd, 0) * 100) / 100;
console.log("exact  trades", exact.length, "pnl", exactPnl, "wins", exact.filter((t) => t.pnlUsd >= 0).length);
console.log("fast   trades", fast.train.trades, "pnl", fast.train.pnlUsd, "wins", fast.train.wins, fast.train.exits);
const ok = exact.length === fast.train.trades && Math.abs(exactPnl - fast.train.pnlUsd) < 0.05 * Math.max(1, exact.length);
console.log(ok ? "MATCH" : "MISMATCH");
if (!ok && !process.env.DIFF) { console.log(exact.slice(0, 12).map((t) => [t.symbol, t.date, t.enteredAt.slice(11, 16), t.qty, t.entry, t.exit, t.role, t.pnlUsd].join(" "))); process.exit(1); }
/* trade-level diff when asked */
if (process.env.DIFF) {
  const fastTrades = E.evaluate(ds, pairs, first, settings, { splitDay: ds.dates.length, budgetUsd: settings.budgetUsd, collect: true }).trades || [];
  const key = (t) => t.symbol + " " + t.date + " " + (t.enteredAt.length > 6 ? new Intl.DateTimeFormat("en-US",{timeZone:"America/New_York",hour12:false,hour:"2-digit",minute:"2-digit"}).format(new Date(t.enteredAt)) : t.enteredAt.slice(1));
  const fm = new Map(fastTrades.map((t) => [key(t), t]));
  let shown = 0;
  for (const t of exact) { const f = fm.get(key(t)); if (!f || Math.abs(f.pnlUsd - t.pnlUsd) > 0.02 || f.qty !== t.qty) { console.log("exact", key(t), t.qty, t.entry, t.exit, t.role, t.pnlUsd, "| fast", f ? [f.qty, f.entry, f.exit, f.role, f.pnlUsd].join(" ") : "missing"); if (++shown > 10) break; } }
}
