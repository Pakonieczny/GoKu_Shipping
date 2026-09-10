"use strict";
/*  _investorDipSim.js — historical replay of the dip-reversal lane.
 *
 *  Deterministic and AI-free: the same detector, settle test, confidence
 *  sizing and exit rules as the live lane (_investorDipReversal) are replayed
 *  minute by minute over stored SIP one-minute bars for a chosen list of
 *  companies and a date range. Fills are the bar close plus the same slippage
 *  and fee allowance the live lane pays. Regular session only; every position
 *  is flattened before the close, exactly like the live lane.
 */
const A = require("./_investorAdmin");
const M = require("./_investorMarket");
const DIP = require("./_investorDipReversal");
const crypto = require("crypto");

const VERSION = "dip-sim.v1";
const COL = "InvestorAI_DipSimulations";
const SLIPPAGE = 0.0005;          // 5 bps each way
const FEE_PER_SHARE = 0.005;
const CHUNK_DAYS = 5;             // trading days fetched per symbol per request
const MAX_TRADES_STORED = 2500;

function sha(s) { return crypto.createHash("sha256").update(String(s)).digest("hex"); }
function num(v, fb) { const n = Number(v); return Number.isFinite(n) ? n : fb; }
function r2(v) { return Math.round(v * 100) / 100; }

/** Trading dates in [from, to], oldest first. */
function tradingDates(from, to, max = 260) {
  const out = [];
  const iso = /^\d{4}-\d{2}-\d{2}$/;
  if (!iso.test(from) || !iso.test(to) || to < from) return out;
  let d = new Date(from + "T12:00:00Z");
  const end = new Date(to + "T12:00:00Z");
  let guard = 0;
  while (d <= end && guard++ < 400 && out.length < max) {
    const st = M.sessionState(new Date(M.nyWallClockToUtcMs(d.toISOString().slice(0, 10), 600)));
    if (st.tradingDay) out.push(st.date);
    d = new Date(d.getTime() + 86400000);
  }
  return out;
}

/* ── historical bars: SIP one-minute, regular hours, partitioned by NY date ── */
async function fetchMinuteBars(symbol, fromDate, toDate, { fetchImpl = globalThis.fetch } = {}) {
  await M.loadMarketSettings();
  const creds = M.providerCredentials("alpaca");
  if (!creds.keyId || !creds.secretKey) throw Object.assign(new Error("Alpaca historical-data credentials are required"), { code: "HISTORICAL_BARS_MISSING" });
  const start = new Date(M.nyWallClockToUtcMs(fromDate, 570)).toISOString();
  const end = new Date(M.nyWallClockToUtcMs(toDate, 960)).toISOString();
  const byDate = {};
  let pageToken = null, pages = 0;
  do {
    const qs = new URLSearchParams({ symbols: symbol, timeframe: "1Min", start, end, feed: "sip", adjustment: "raw", limit: "10000", sort: "asc" });
    if (pageToken) qs.set("page_token", pageToken);
    const ac = new AbortController(), timer = setTimeout(() => ac.abort(), 20000);
    let res, json;
    try { res = await fetchImpl("https://data.alpaca.markets/v2/stocks/bars?" + qs, { headers: { "APCA-API-KEY-ID": creds.keyId, "APCA-API-SECRET-KEY": creds.secretKey }, signal: ac.signal }); json = await res.json(); }
    finally { clearTimeout(timer); }
    if (!res.ok) throw Object.assign(new Error(`Historical price provider returned ${res.status}`), { code: "HISTORICAL_DATA_UNAVAILABLE" });
    for (const b of (json.bars && json.bars[symbol]) || []) {
      const t = Date.parse(b.t); if (!Number.isFinite(t)) continue;
      const ny = M.nyParts(new Date(t));
      if (ny.minutes < 570 || ny.minutes >= 960) continue;
      (byDate[ny.date] = byDate[ny.date] || []).push({ t, o: Number(b.o), h: Number(b.h), l: Number(b.l), c: Number(b.c), v: Number(b.v) || 0, done: true });
    }
    pageToken = json.next_page_token || null;
  } while (pageToken && ++pages < 20);
  return byDate;
}

/* ── the replay ────────────────────────────────────────────────────────── */
/** Replay one trading date for every symbol. `state` carries cash and the
 *  cumulative tallies across dates; returns the day summary and trades. */
function replayDate({ date, barsBySymbol, settings, state }) {
  const symbols = Object.keys(barsBySymbol).filter((s) => (barsBySymbol[s] || []).length >= 5);
  const openMs = M.nyWallClockToUtcMs(date, 570), closeMs = M.nyWallClockToUtcMs(date, 960), flattenMs = closeMs - settings.closeBeforeCloseMin * 60000;
  const open = {}, trades = [], curve = [];
  const cooldown = {}, tradedKey = {};
  const startCash = state.cash;
  let dayPnl = 0, wins = 0, losses = 0;
  const priceAt = {};
  for (let minute = 0; minute < 390; minute++) {
    const nowMs = openMs + (minute + 1) * 60000;   // the print arrives at the end of the bar
    for (const symbol of symbols) {
      const all = barsBySymbol[symbol];
      const idx = all.findIndex((b) => b.t >= openMs + minute * 60000 && b.t < openMs + (minute + 1) * 60000);
      if (idx < 0) continue;
      const bar = all[idx];
      priceAt[symbol] = bar.c;
      const seen = all.slice(0, idx + 1).map((b, k, arr) => (k === arr.length - 1 ? { ...b, done: false } : b));
      const pos = open[symbol];
      if (pos) {
        let role = null, fill = null, why = null;
        if (bar.l <= pos.stop) { role = "STOP"; fill = Math.min(pos.stop, bar.o); why = "price broke below the fall's low"; }
        else if (bar.h >= pos.target) { role = "TARGET"; fill = Math.max(pos.target, Math.min(bar.o, pos.target)); why = `recovered ${settings.retracePct}% of the fall`; }
        else if (nowMs >= pos.deadlineMs) { role = "TIME_LIMIT"; fill = bar.c; why = `held ${settings.maxHoldMin} min without reaching the target`; }
        else if (nowMs >= flattenMs) { role = "TIME_LIMIT"; fill = bar.c; why = "flattened before the close"; }
        if (role) {
          const exitPx = fill * (1 - SLIPPAGE), fee = pos.qty * FEE_PER_SHARE;
          const pnl = r2((exitPx - pos.entry) * pos.qty - fee - pos.fee);
          state.cash += exitPx * pos.qty - fee; dayPnl += pnl; if (pnl >= 0) wins++; else losses++;
          trades.push({ symbol, date, enteredAt: new Date(pos.enteredMs).toISOString(), exitedAt: new Date(nowMs).toISOString(), qty: pos.qty, entry: r2(pos.entry), exit: r2(exitPx), pnlUsd: pnl, role, why, holdMin: Math.round((nowMs - pos.enteredMs) / 60000), confidence: pos.confidence, tradeUsd: pos.tradeUsd, dropPct: pos.dropPct, z: pos.z, speed: pos.speed });
          delete open[symbol]; cooldown[symbol] = nowMs + settings.cooldownMin * 60000;
        }
        continue;
      }
      if (nowMs >= flattenMs - settings.maxHoldMin * 60000 / 3) continue;
      if (cooldown[symbol] && nowMs < cooldown[symbol]) continue;
      const drop = DIP.measureDrop(seen, settings, nowMs);
      if (!drop || !drop.qualifies) continue;
      const key = symbol + "_" + drop.lowAt;
      if (tradedKey[key]) continue;
      const st = DIP.settled(seen, drop, settings, bar.c);
      if (!st.ok) continue;
      const conf = DIP.confidenceOf(drop, st, settings);
      const free = state.cash - 1;
      const tradeUsd = Math.min(DIP.sizeFor(conf.confidence, settings), Math.floor(free));
      const entryPx = bar.c * (1 + SLIPPAGE);
      const qty = Math.floor(tradeUsd / (entryPx + FEE_PER_SHARE));
      tradedKey[key] = true;
      if (qty < 1) continue;
      const fee = qty * FEE_PER_SHARE;
      state.cash -= entryPx * qty + fee;
      open[symbol] = { qty, entry: entryPx, fee, stop: drop.low - (drop.high - drop.low) * settings.stopPct / 100, target: entryPx + (drop.high - drop.low) * settings.retracePct / 100, deadlineMs: nowMs + settings.maxHoldMin * 60000, enteredMs: nowMs, confidence: conf.confidence, tradeUsd, dropPct: r2(drop.dropPct), z: Math.round(drop.z * 10) / 10, speed: r2(drop.speed) };
    }
    if (minute % 5 === 4 || minute === 389) {
      const marked = Object.keys(open).reduce((n, s) => n + open[s].qty * (priceAt[s] || open[s].entry), 0);
      curve.push({ t: nowMs, equity: r2(state.cash + marked) });
    }
  }
  /* anything still open at the last bar (no flatten bar seen) closes at the last price */
  for (const symbol of Object.keys(open)) {
    const pos = open[symbol], px = (priceAt[symbol] || pos.entry) * (1 - SLIPPAGE), fee = pos.qty * FEE_PER_SHARE, pnl = r2((px - pos.entry) * pos.qty - fee - pos.fee);
    state.cash += px * pos.qty - fee; dayPnl += pnl; if (pnl >= 0) wins++; else losses++;
    trades.push({ symbol, date, enteredAt: new Date(pos.enteredMs).toISOString(), exitedAt: new Date(closeMs).toISOString(), qty: pos.qty, entry: r2(pos.entry), exit: r2(px), pnlUsd: pnl, role: "TIME_LIMIT", why: "closed at the last bar of the day", holdMin: Math.round((closeMs - pos.enteredMs) / 60000), confidence: pos.confidence, tradeUsd: pos.tradeUsd, dropPct: pos.dropPct, z: pos.z, speed: pos.speed });
  }
  return { day: { date, pnlUsd: r2(dayPnl), trades: trades.length, wins, losses, startEquity: r2(startCash), endEquity: r2(state.cash), symbolsWithBars: symbols.length }, trades, curve };
}

/* ── persistence + orchestration ───────────────────────────────────────── */
function simRef(D, simId) { return D.col(COL).doc(simId); }
async function create(D, { owner, symbols, from, to, settings, source, nowMs = A.now() }) {
  const dates = tradingDates(from, to);
  if (!dates.length) throw Object.assign(new Error("no trading days in that range"), { code: "SEMANTIC_REJECTED" });
  if (dates.length > 130) throw Object.assign(new Error("at most about six months (130 trading days) per simulation"), { code: "SEMANTIC_REJECTED" });
  const simId = `dipsim_${sha([owner, symbols.join(","), from, to, nowMs].join("|")).slice(0, 24)}`;
  const doc = { simId, version: VERSION, owner, source, symbols, from, to, dates, settings, status: "queued", cursor: 0, createdAtMs: nowMs, updatedAtMs: nowMs,
    state: { cash: settings.budgetUsd }, days: [], trades: [], curve: [], stats: null, error: null, ...A.envelope({ created_by: "dipSim" }) };
  await simRef(D, simId).set(doc);
  return doc;
}
function statsOf(doc) {
  const trades = doc.trades || [], days = doc.days || [];
  const pnl = r2(trades.reduce((n, t) => n + (t.pnlUsd || 0), 0)), wins = trades.filter((t) => t.pnlUsd >= 0).length;
  let peak = doc.settings.budgetUsd, maxDd = 0; for (const p of doc.curve || []) { peak = Math.max(peak, p.equity); maxDd = Math.max(maxDd, peak - p.equity); }
  const gross = trades.filter((t) => t.pnlUsd > 0).reduce((n, t) => n + t.pnlUsd, 0), grossLoss = -trades.filter((t) => t.pnlUsd < 0).reduce((n, t) => n + t.pnlUsd, 0);
  return { pnlUsd: pnl, returnPct: r2(pnl / doc.settings.budgetUsd * 100), trades: trades.length, wins, losses: trades.length - wins, winRatePct: trades.length ? r2(wins / trades.length * 100) : null, avgTradeUsd: trades.length ? r2(pnl / trades.length) : null, maxDrawdownUsd: r2(maxDd), profitFactor: grossLoss > 0 ? r2(gross / grossLoss) : null, daysDone: days.length, daysTotal: (doc.dates || []).length, bestDay: days.length ? Math.max(...days.map((d) => d.pnlUsd)) : null, worstDay: days.length ? Math.min(...days.map((d) => d.pnlUsd)) : null };
}
/** Advance one simulation by up to `budgetMs` of work. Returns the doc. */
async function advance(D, simId, { budgetMs = 11 * 60000, fetchImpl = globalThis.fetch, nowMs = A.now() } = {}) {
  const snap = await simRef(D, simId).get();
  if (!snap.exists) throw Object.assign(new Error("simulation not found"), { code: "NOT_FOUND" });
  let doc = snap.data();
  if (["complete", "cancelled", "failed"].includes(doc.status)) return doc;
  const startedAt = Date.now();
  await simRef(D, simId).set({ status: "running", startedAtMs: doc.startedAtMs || nowMs, leaseUntilMs: Date.now() + budgetMs + 60000, updatedAtMs: Date.now() }, { merge: true });
  const settings = DIP.normalizeSettings(doc.settings);
  try {
    while (doc.cursor < doc.dates.length) {
      if (Date.now() - startedAt > budgetMs) break;
      const chunk = doc.dates.slice(doc.cursor, doc.cursor + CHUNK_DAYS);
      const barsBySymbolByDate = {};
      for (const symbol of doc.symbols) {
        try { const byDate = await fetchMinuteBars(symbol, chunk[0], chunk[chunk.length - 1], { fetchImpl }); for (const [date, bars] of Object.entries(byDate)) (barsBySymbolByDate[date] = barsBySymbolByDate[date] || {})[symbol] = bars; }
        catch (e) { doc.warnings = (doc.warnings || []).concat([`${symbol} ${chunk[0]}: ${String(e.message).slice(0, 80)}`]).slice(-30); }
      }
      for (const date of chunk) {
        const out = replayDate({ date, barsBySymbol: barsBySymbolByDate[date] || {}, settings, state: doc.state });
        doc.days.push(out.day); doc.trades = doc.trades.concat(out.trades).slice(-MAX_TRADES_STORED); doc.curve = doc.curve.concat(out.curve);
        doc.cursor += 1;
      }
      doc.stats = statsOf(doc);
      const fresh = await simRef(D, simId).get();
      if (fresh.exists && fresh.data().status === "cancelled") { doc.status = "cancelled"; break; }
      await simRef(D, simId).set({ cursor: doc.cursor, state: doc.state, days: doc.days, trades: doc.trades, curve: doc.curve, stats: doc.stats, warnings: doc.warnings || [], updatedAtMs: Date.now(), leaseUntilMs: Date.now() + budgetMs }, { merge: true });
    }
    if (doc.status !== "cancelled") doc.status = doc.cursor >= doc.dates.length ? "complete" : "running";
    await simRef(D, simId).set({ status: doc.status, completedAtMs: doc.status === "complete" ? Date.now() : null, leaseUntilMs: doc.status === "running" ? Date.now() + 120000 : null, updatedAtMs: Date.now() }, { merge: true });
    return doc;
  } catch (e) {
    await simRef(D, simId).set({ status: "failed", error: String(e.message).slice(0, 240), leaseUntilMs: null, updatedAtMs: Date.now() }, { merge: true });
    throw e;
  }
}
async function list(D, owner, limit = 30) {
  const snap = await D.col(COL).where("owner", "==", owner).get();
  const rows = snap.docs.map((d) => d.data()).sort((a, b) => Number(b.createdAtMs) - Number(a.createdAtMs)).slice(0, limit);
  return rows.map((d) => summary(d));
}
function summary(d) { return { simId: d.simId, status: d.status, source: d.source, symbols: d.symbols, symbolCount: (d.symbols || []).length, from: d.from, to: d.to, daysTotal: (d.dates || []).length, cursor: d.cursor, progressPct: (d.dates || []).length ? Math.round(d.cursor / d.dates.length * 100) : 0, settings: d.settings, stats: d.stats, error: d.error || null, warnings: (d.warnings || []).length, createdAtMs: d.createdAtMs, updatedAtMs: d.updatedAtMs, completedAtMs: d.completedAtMs || null, leaseUntilMs: d.leaseUntilMs || null }; }
async function detail(D, owner, simId) {
  const snap = await simRef(D, simId).get();
  if (!snap.exists || snap.data().owner !== owner) throw Object.assign(new Error("simulation not found"), { code: "NOT_FOUND" });
  const d = snap.data();
  return { ...summary(d), days: d.days || [], trades: (d.trades || []).slice(-400).reverse(), curve: d.curve || [], warningsList: d.warnings || [] };
}
async function control(D, owner, simId, command) {
  const snap = await simRef(D, simId).get();
  if (!snap.exists || snap.data().owner !== owner) throw Object.assign(new Error("simulation not found"), { code: "NOT_FOUND" });
  if (command === "delete") { await simRef(D, simId).delete(); return { deleted: true }; }
  if (command === "cancel") { await simRef(D, simId).set({ status: "cancelled", leaseUntilMs: null, updatedAtMs: Date.now() }, { merge: true }); return { cancelled: true }; }
  throw Object.assign(new Error("unknown command"), { code: "SEMANTIC_REJECTED" });
}
module.exports = { VERSION, COL, tradingDates, fetchMinuteBars, replayDate, statsOf, create, advance, list, detail, control, summary };
