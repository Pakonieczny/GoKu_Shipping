"use strict";
/*  _investorDipReversal.js  — paper-only intraday "dip reversal" lane.
 *
 *  A deterministic detector that watches a short list of symbols on the
 *  real-time IEX trade lane every few seconds, waits for a sharp fall that
 *  then settles (no new low for a few one-minute bars), buys a small paper
 *  position, and sells when the price retraces a set share of the fall, when
 *  it breaks below the fall's low, or when time runs out. Every signal and
 *  every trade is written down so the experiment can be judged honestly.
 *
 *  Boundaries: paper account only; it never touches the AI gateway, never
 *  changes a manager plan, and fills at the IEX print plus a slippage
 *  allowance. IEX is a thin slice of the tape, so prints can be stale — that
 *  is part of what this experiment measures.
 */
const A = require("./_investorAdmin");
const M = require("./_investorMarket");
const F = require("./_investorFetch");
const X = require("./_investorExecution");
const crypto = require("crypto");

const VERSION = "dip-reversal.v1";
const SLIPPAGE_BPS = 5n;                 // paid on every paper fill, both ways
const FEE_PER_SHARE_MICROS = 5000n;      // $0.005 / share, same as the shared paper lane
const MAX_BARS = 90;                     // one-minute bars kept per symbol

const DEFAULTS = Object.freeze({
  enabled: false,
  symbols: [],              // your chosen symbols (uppercase), up to 50
  watchShortlist: true,     // fill the list up to 50 from the AI's latest daily shortlist
  budgetUsd: 50000,         // the lane's own capital; the AI lane never sizes into it
  allocationUsd: 5000,      // paper dollars per entry
  maxOpen: 5,               // dip positions open at once
  maxTradesPerDay: 20,
  /* The fall. `dropBps` is the floor for a calm stock; the real requirement
     rises with how hard the stock is falling relative to its own normal
     minute-to-minute noise (see measureDrop). */
  dropBps: 100,             // minimum fall, high to low, in basis points (100 = 1.0%)
  dropWindowMin: 30,        // the fall must happen within this many minutes
  zMinTenths: 20,           // the fall must be at least z = 2.0 noise units (sigma * sqrt(minutes))
  speedFactorPct: 50,       // each extra unit of fall speed above normal adds 50% to the required fall and settle
  /* The settle. Base bars for a normal fall; scaled up for fast falls. */
  settleBars: 3,            // completed one-minute bars with no new low
  stabilitySigmaTenths: 20, // settle closes must sit within 2.0 sigma above the low
  retracePct: 80,           // sell when this share of the fall is recovered
  stopPct: 60,              // stop this share of the fall below the low
  maxHoldMin: 90,           // time exit
  cooldownMin: 20,          // per-symbol pause after an exit
  closeBeforeCloseMin: 5,   // flatten this many minutes before the close
});
const BOUNDS = Object.freeze({
  budgetUsd: [1000, 95000], allocationUsd: [500, 95000], maxOpen: [1, 10], maxTradesPerDay: [1, 80], dropBps: [30, 1500], dropWindowMin: [3, 120], zMinTenths: [5, 80], speedFactorPct: [0, 300],
  settleBars: [1, 15], stabilitySigmaTenths: [5, 100], retracePct: [10, 150], stopPct: [10, 200], maxHoldMin: [5, 390], cooldownMin: [0, 390], closeBeforeCloseMin: [0, 60],
});

function sha(s) { return crypto.createHash("sha256").update(String(s)).digest("hex"); }
function big(v) { try { return BigInt(String(v == null || v === "" ? 0 : v)); } catch { return 0n; } }
function micros(usd) { return BigInt(Math.round(Number(usd) * 1e6)); }
function usdOf(m) { return Number(m) / 1e6; }
function num(v, fb) { const n = Number(v); return Number.isFinite(n) ? n : fb; }

/** Settings with defaults and bounds applied. Unknown keys are dropped. */
function normalizeSettings(raw) {
  const s = { ...DEFAULTS };
  const r = raw && typeof raw === "object" ? raw : {};
  s.enabled = r.enabled === true;
  s.watchShortlist = r.watchShortlist !== false;
  s.symbols = [...new Set((Array.isArray(r.symbols) ? r.symbols : String(r.symbols || "").split(/[\s,]+/)).map((x) => String(x || "").trim().toUpperCase()).filter((x) => /^[A-Z][A-Z0-9.-]{0,9}$/.test(x)))].slice(0, 50);
  for (const k of Object.keys(BOUNDS)) { const [lo, hi] = BOUNDS[k]; s[k] = Math.min(hi, Math.max(lo, num(r[k], DEFAULTS[k]))); }
  for (const k of Object.keys(BOUNDS)) s[k] = Math.round(s[k]);
  s.version = Number(r.version) || 0;
  s.updatedAtMs = Number(r.updatedAtMs) || null;
  return s;
}

/* ── market data: real-time IEX prints and seed bars ───────────────────── */
async function latestTrades(symbols) {
  if (!symbols.length) return {};
  await M.loadMarketSettings();
  if (!M.providerCredentialed("alpaca")) throw Object.assign(new Error("no Alpaca credentials"), { code: "PROVIDER_UNAVAILABLE" });
  const creds = M.providerCredentials("alpaca");
  const url = `https://data.alpaca.markets/v2/stocks/trades/latest?symbols=${encodeURIComponent(symbols.join(","))}&feed=iex`;
  const r = await F.fetchPublic(url, { sourceId: "alpaca.trades.latest.dip", accept: ["json"], timeoutMs: 6000, headers: { "APCA-API-KEY-ID": creds.keyId, "APCA-API-SECRET-KEY": creds.secretKey } });
  const out = {};
  for (const [sym, t] of Object.entries((r.json && r.json.trades) || {})) { const p = Number(t && t.p), ms = Date.parse(t && t.t); if (p > 0 && Number.isFinite(ms)) out[sym] = { p, ms, s: Number(t.s) || null }; }
  return out;
}
async function seedBars(symbols) {
  const out = {};
  if (!symbols.length) return out;
  try {
    const got = await M.fetchBars(symbols, { timeframe: "1Min", limit: MAX_BARS, feed: "iex", recentMinutes: MAX_BARS + 5 });
    for (const [sym, bars] of Object.entries(got.bars || {})) out[sym] = (bars || []).map((b) => ({ t: Date.parse(b.t), o: Number(b.o), h: Number(b.h), l: Number(b.l), c: Number(b.c), v: Number(b.v) || 0, done: true })).filter((b) => Number.isFinite(b.t) && b.c > 0).slice(-MAX_BARS);
  } catch (e) { /* the loop builds its own bars from prints */ }
  return out;
}

/* ── one-minute bar builder from prints ────────────────────────────────── */
function minuteOf(ms) { return Math.floor(ms / 60000) * 60000; }
function applyPrint(bars, print) {
  const t = minuteOf(print.ms), last = bars[bars.length - 1];
  if (last && last.t === t) { last.h = Math.max(last.h, print.p); last.l = Math.min(last.l, print.p); last.c = print.p; last.v += print.s || 0; return bars; }
  if (last && last.t > t) return bars;                 // an older print: ignore
  if (last) last.done = true;
  bars.push({ t, o: print.p, h: print.p, l: print.p, c: print.p, v: print.s || 0, done: false });
  if (bars.length > MAX_BARS) bars.splice(0, bars.length - MAX_BARS);
  return bars;
}

/* ── the detector: a sharp fall, then a settle, then an entry ──────────── */
/** The stock's own noise: the standard deviation of one-minute log returns
 *  over the last hour of completed bars, in percent per minute. This is the
 *  yardstick every threshold is measured against, so a jumpy stock needs a
 *  bigger fall than a quiet one. */
function minuteSigmaPct(bars, beforeMs = Infinity) {
  let done = bars.filter((b) => b.done && b.c > 0 && b.t <= beforeMs).slice(-61);
  if (done.length < 6) done = bars.filter((b) => b.done && b.c > 0).slice(-61);
  const r = []; for (let i = 1; i < done.length; i++) r.push(Math.log(done[i].c / done[i - 1].c));
  if (r.length < 5) return 0.08;
  const mean = r.reduce((a, b) => a + b, 0) / r.length;
  const v = r.reduce((a, b) => a + (b - mean) * (b - mean), 0) / (r.length - 1);
  return Math.max(0.02, Math.sqrt(v) * 100);
}
/** Look back over the window for the highest high and the lowest low after
 *  it, then judge the fall against the stock's noise:
 *    z        = fall% / (sigma% * sqrt(minutes))   how unusual the fall is
 *    speed    = (fall% / minutes) / sigma%          how hard it fell, in noise units per minute
 *    required = dropBps * (1 + speedFactor * max(0, speed - 1))  a faster fall must go further before it counts
 *  Returns null when there is no fall, or the fall is too small for its speed
 *  or too ordinary for the stock. */
function measureDrop(bars, settings, nowMs) {
  const windowStart = nowMs - settings.dropWindowMin * 60000;
  const recent = bars.filter((b) => b.t >= windowStart);
  if (recent.length < 3) return null;
  let hiIdx = 0; for (let i = 1; i < recent.length; i++) if (recent[i].h > recent[hiIdx].h) hiIdx = i;
  let loIdx = -1; for (let i = hiIdx; i < recent.length; i++) if (loIdx < 0 || recent[i].l < recent[loIdx].l) loIdx = i;
  if (loIdx <= hiIdx) return null;
  const high = recent[hiIdx].h, low = recent[loIdx].l, dropPct = (high - low) / high * 100;
  const minutes = Math.max(1, (recent[loIdx].t - recent[hiIdx].t) / 60000);
  const sigmaPct = minuteSigmaPct(bars, recent[hiIdx].t);   // the stock's noise before the fall began
  const z = dropPct / (sigmaPct * Math.sqrt(minutes));
  const speed = (dropPct / minutes) / sigmaPct;
  const speedFactor = settings.speedFactorPct / 100;
  const requiredPct = Math.min(settings.dropBps / 100 * 3, settings.dropBps / 100 * (1 + speedFactor * Math.max(0, speed - 1)));
  const settleBarsNeeded = Math.min(settings.settleBars * 4, Math.max(settings.settleBars, Math.round(settings.settleBars * (1 + speedFactor * Math.max(0, speed - 1)))));
  const out = { high, low, highAt: recent[hiIdx].t, lowAt: recent[loIdx].t, dropPct, minutes, speedPctPerMin: dropPct / minutes, sigmaPct, z, speed, requiredPct, settleBarsNeeded, barsSinceLow: recent.length - 1 - loIdx };
  if (!(dropPct >= requiredPct)) return { ...out, qualifies: false, why: `fell ${dropPct.toFixed(2)}%, needs ${requiredPct.toFixed(2)}% at this speed` };
  if (!(z >= settings.zMinTenths / 10)) return { ...out, qualifies: false, why: `fall is ordinary for this stock (z ${z.toFixed(1)} < ${(settings.zMinTenths / 10).toFixed(1)})` };
  return { ...out, qualifies: true };
}
/** The settle, scaled to the fall: the required number of completed bars
 *  after the low (more for a fast fall), none making a new low, closes held
 *  inside `stabilitySigma` noise units above the low, not drifting down, and
 *  the latest print above the low. Returns a stability score 0..1 (1 = the
 *  closes barely moved). */
function settled(bars, drop, settings, lastPrice) {
  const need = drop.settleBarsNeeded || settings.settleBars;
  const after = bars.filter((b) => b.t > drop.lowAt && b.done);
  if (after.length < need) return { ok: false, why: `settling ${after.length}/${need} bars` };
  const tail = after.slice(-need);
  const bandPct = (settings.stabilitySigmaTenths / 10) * drop.sigmaPct;
  const band = drop.low * (1 + bandPct / 100);
  if (tail.some((b) => b.l < drop.low)) return { ok: false, why: "made a new low" };
  if (tail.some((b) => b.c > band * (1 + bandPct / 100))) return { ok: false, why: "already ran away from the low" };
  const closes = tail.map((b) => b.c), range = Math.max(...closes) - Math.min(...closes);
  const stability = Math.max(0, 1 - range / Math.max(1e-9, drop.low * bandPct / 100));
  if (closes[closes.length - 1] < closes[0]) return { ok: false, why: "still drifting down", stability };
  if (!(lastPrice > drop.low)) return { ok: false, why: "still at the low", stability };
  return { ok: true, why: `${tail.length} steady bars, stability ${Math.round(stability * 100)}%`, stability };
}

/* ── persistence ───────────────────────────────────────────────────────── */
function stateRef(D, accountId) { return D.col(D.COL.dipState).doc(accountId); }
async function loadState(D, accountId) { const s = await stateRef(D, accountId).get(); return s.exists ? s.data() : { accountId, symbols: {}, open: {}, day: null, tradesToday: 0 }; }
async function saveState(D, state) { await stateRef(D, state.accountId).set({ ...state, version: VERSION, updatedAtMs: A.now() }, { merge: false }); }
async function logSignal(D, accountId, fields) {
  const at = fields.atMs || A.now();
  const id = `dip_${sha([accountId, fields.symbol, fields.kind, at, fields.detail || ""].join("|")).slice(0, 28)}`;
  await D.col(D.COL.dipSignals).doc(id).set({ signalId: id, accountId, atMs: at, at: new Date(at).toISOString(), version: VERSION, ...fields, ...A.envelope({ created_by: "dipReversal" }) });
}

/* ── paper fills through the shared ledger primitive ───────────────────── */
async function cashCents(D, accountId) { const a = await D.col(D.COL.accounts).doc(accountId).get(); return a.exists ? Number((a.data().balanceCents || {}).cash || 0) : 0; }
function legsFor(setId, symbol, qty, stopMicros, targetMicros, deadlineMs) {
  const q = qty.toString();
  return [
    { legId: `${setId}_ENTRY`, orderSetId: setId, symbol, role: "ENTRY", side: "buy", type: "MARKET", status: "WORKING", quantityUnits: q, remainingUnits: q },
    { legId: `${setId}_STOP`, orderSetId: setId, symbol, role: "STOP", side: "sell", type: "STOP", status: "ARMED", quantityUnits: q, remainingUnits: q, stopMicros: stopMicros.toString() },
    { legId: `${setId}_TARGET`, orderSetId: setId, symbol, role: "TARGET", side: "sell", type: "LIMIT", status: "ARMED", quantityUnits: q, remainingUnits: q, priceMicros: targetMicros.toString() },
    { legId: `${setId}_TIME_LIMIT`, orderSetId: setId, symbol, role: "TIME_LIMIT", side: "sell", type: "MARKET", status: "ARMED", quantityUnits: q, remainingUnits: q, submitAtMs: deadlineMs },
  ];
}
async function enter({ D, accountId, symbol, price, drop, settings, nowMs, reason, laneAvailableUsd: lane = Infinity }) {
  const cash = await cashCents(D, accountId);
  const budgetCents = Math.min(Math.round(settings.allocationUsd * 100), Math.round(lane * 100), cash - 100);
  const fillMicros = micros(price) + micros(price) * SLIPPAGE_BPS / 10000n;
  const perShareCents = Number(fillMicros / 10000n) + 1;
  const qty = Math.floor(budgetCents / perShareCents);
  if (qty < 1) return { entered: false, why: "not enough paper cash for one share" };
  const targetMicros = fillMicros + micros((drop.high - drop.low) * settings.retracePct / 100);
  const stopMicros = micros(drop.low) - micros((drop.high - drop.low) * settings.stopPct / 100);
  const deadlineMs = nowMs + settings.maxHoldMin * 60000;
  const setId = `os_dip_${sha([accountId, symbol, nowMs].join("|")).slice(0, 24)}`;
  const set = { orderSetId: setId, accountId, symbol, purpose: "DIP_REVERSAL", authority: "DIP_REVERSAL", status: "ENTERED", entered: true, createdAtMs: nowMs, expiresAtMs: deadlineMs, mandateVersionId: null, sector: null, version: 1,
    dip: { high: drop.high, low: drop.low, dropPct: drop.dropPct, minutes: drop.minutes, highAt: drop.highAt, lowAt: drop.lowAt, retracePct: settings.retracePct, stopPct: settings.stopPct, settingsVersion: settings.version }, ...A.envelope({ created_by: "dipReversal" }) };
  const legs = legsFor(setId, symbol, qty, stopMicros, targetMicros, deadlineMs);
  await D.col(D.COL.orderSets).doc(setId).set(set);
  for (const l of legs) await D.col(D.COL.orderLegs).doc(l.legId).set({ ...l, accountId, createdAtMs: nowMs });
  const fee = (BigInt(qty) * FEE_PER_SHARE_MICROS + 9999n) / 10000n;
  const rec = await X.recordFill({ admin: D, accountId, orderSet: set, leg: legs[0], fill: { quantityUnits: String(qty), priceMicros: fillMicros.toString(), basis: "dip_iex_print_plus_slippage", eventId: `${setId}_entry` }, bar: null, provenance: { provider: "alpaca", feed: "iex", lane: "dip" }, feeMinor: fee.toString(), nowMs, source: "paper_dip" });
  await D.col(D.COL.positions).doc(`${accountId}_${symbol}`).set({ lossBoundaryPriceMicros: stopMicros.toString(), takeProfitPriceMicros: targetMicros.toString(), protectionState: "PROTECTED_RTH", protectionAcknowledged: true, engine: "dip", dipOrderSetId: setId, updatedAtMs: nowMs }, { merge: true });
  await D.col(D.COL.orderLegs).doc(legs[1].legId).set({ status: "WORKING" }, { merge: true });
  await D.col(D.COL.orderLegs).doc(legs[2].legId).set({ status: "WORKING" }, { merge: true });
  return { entered: true, orderSetId: setId, qty, entryMicros: fillMicros.toString(), entry: usdOf(fillMicros), stop: usdOf(stopMicros), target: usdOf(targetMicros), deadlineMs, fillId: rec.fillId, reason };
}
async function exit({ D, accountId, open, price, role, nowMs, why }) {
  const set = await X.readOrderSet(D, open.orderSetId);
  if (!set) return { exited: false, why: "order set missing" };
  const leg = (set.legs || []).find((l) => l.role === role) || (set.legs || []).find((l) => l.role === "TIME_LIMIT");
  if (!leg) return { exited: false, why: "exit leg missing" };
  const fillMicros = micros(price) - micros(price) * SLIPPAGE_BPS / 10000n;
  const qty = BigInt(open.qty);
  const fee = (qty * FEE_PER_SHARE_MICROS + 9999n) / 10000n;
  const rec = await X.recordFill({ admin: D, accountId, orderSet: set, leg: { ...leg, quantityUnits: qty.toString(), remainingUnits: qty.toString() }, fill: { quantityUnits: qty.toString(), priceMicros: fillMicros.toString(), basis: `dip_${role.toLowerCase()}_iex_print_minus_slippage`, eventId: `${set.orderSetId}_${role}_${nowMs}` }, bar: null, provenance: { provider: "alpaca", feed: "iex", lane: "dip" }, feeMinor: fee.toString(), nowMs, source: "paper_dip" });
  for (const l of set.legs || []) if (l.legId !== leg.legId && ["ARMED", "WORKING"].includes(l.status)) await D.col(D.COL.orderLegs).doc(l.legId).set({ status: "CANCELLED", cancelledAtMs: nowMs }, { merge: true });
  await D.col(D.COL.orderSets).doc(set.orderSetId).set({ status: "CLOSED", closed: true, closedAtMs: nowMs, exitRole: role, exitWhy: why }, { merge: true });
  const pnl = (usdOf(fillMicros) - open.entry) * Number(qty) - Number(fee) / 100 - (open.feeUsd || 0);
  return { exited: true, fillId: rec.fillId, exit: usdOf(fillMicros), pnlUsd: Math.round(pnl * 100) / 100, role, why };
}

/* ── one pass over every watched symbol ────────────────────────────────── */
/** Up to 50 names: your list first, then the AI's latest daily shortlist.
 *  Companies the AI lane currently holds are left out so the two lanes never
 *  trade the same stock; the dip lane's own open positions are always kept. */
async function watchlist(D, accountId, settings, { control = null, state = null } = {}) {
  const set = new Set(settings.symbols);
  if (settings.watchShortlist && set.size < 50) {
    let short = control && control.lastManagerRun && control.lastManagerRun.shortlist && control.lastManagerRun.shortlist.symbols;
    if (!Array.isArray(short) && control && control.lastManagerRunId) { try { const run = await D.col(D.COL.managerRuns).doc(String(control.lastManagerRunId)).get(); short = run.exists && run.data().shortlist ? run.data().shortlist.symbols : null; } catch (e) { short = null; } }
    for (const s of (Array.isArray(short) ? short : [])) { if (set.size >= 50) break; const u = String(s || "").toUpperCase(); if (/^[A-Z][A-Z0-9.-]{0,9}$/.test(u)) set.add(u); }
  }
  const snap = await D.col(D.COL.positions).where("accountId", "==", accountId).where("open", "==", true).get();
  const aiHeld = new Set(); snap.docs.forEach((d) => { const p = d.data(); if (p.symbol && p.engine !== "dip") aiHeld.add(String(p.symbol).toUpperCase()); });
  for (const s of aiHeld) set.delete(s);
  for (const s of Object.keys((state && state.open) || {})) set.add(s);
  return { symbols: [...set].slice(0, 50), excludedAiHeld: [...aiHeld] };
}
/** Lane capital: the budget plus what the lane has made or lost, minus what
 *  it holds right now. */
function laneAvailableUsd(settings, state) {
  const openUsd = Object.values((state && state.open) || {}).reduce((n, o) => n + (Number(o.entry) || 0) * (Number(o.qty) || 0), 0);
  return settings.budgetUsd + (Number(state && state.realizedUsd) || 0) - openUsd;
}
function newDay(state, date) { if (state.day !== date) { state.day = date; state.tradesToday = 0; for (const s of Object.values(state.symbols || {})) { s.cooldownUntilMs = null; } } }

/** Evaluate every symbol once with fresh prints. `bars` is the in-memory
 *  bar store the loop keeps between passes. Returns the events produced. */
async function evaluate({ D, accountId, settings, state, bars, prints, session, nowMs }) {
  const events = [];
  newDay(state, session.date);
  let closeMs = null; try { closeMs = Number(M.sessionCloseMs(new Date(nowMs))) || null; } catch (e) { closeMs = null; }
  const flattenFromMs = closeMs ? closeMs - settings.closeBeforeCloseMin * 60000 : Infinity;
  state.symbols = state.symbols || {}; state.open = state.open || {};
  for (const symbol of Object.keys(bars)) {
    const print = prints[symbol]; if (!print) continue;
    applyPrint(bars[symbol], print);
    const sym = state.symbols[symbol] = state.symbols[symbol] || { status: "watching" };
    sym.last = { p: print.p, ms: print.ms };
    const open = state.open[symbol];
    if (open) {
      sym.status = "holding";
      let role = null, why = null;
      if (print.p <= open.stop) { role = "STOP"; why = `price ${print.p.toFixed(2)} at or below the stop ${open.stop.toFixed(2)}`; }
      else if (print.p >= open.target) { role = "TARGET"; why = `price ${print.p.toFixed(2)} reached the target ${open.target.toFixed(2)} (${settings.retracePct}% of the fall recovered)`; }
      else if (nowMs >= open.deadlineMs) { role = "TIME_LIMIT"; why = `held ${settings.maxHoldMin} min without reaching the target`; }
      else if (nowMs >= flattenFromMs) { role = "TIME_LIMIT"; why = "flattened before the close"; }
      if (role) {
        const r = await exit({ D, accountId, open, price: print.p, role, nowMs, why });
        if (r.exited) { delete state.open[symbol]; state.realizedUsd = Math.round(((Number(state.realizedUsd) || 0) + r.pnlUsd) * 100) / 100; sym.status = "cooldown"; sym.cooldownUntilMs = nowMs + settings.cooldownMin * 60000; sym.lastExit = { atMs: nowMs, role, pnlUsd: r.pnlUsd, exit: r.exit };
          const ev = { kind: "EXIT", symbol, atMs: nowMs, role, price: r.exit, quantity: open.qty, entry: open.entry, pnlUsd: r.pnlUsd, detail: why, orderSetId: open.orderSetId, fillId: r.fillId, holdMin: Math.round((nowMs - open.enteredAtMs) / 60000) };
          events.push(ev); await logSignal(D, accountId, ev); }
        else { sym.note = r.why; }
      } else sym.note = `holding · stop ${open.stop.toFixed(2)} · target ${open.target.toFixed(2)} · ${Math.max(0, Math.round((open.deadlineMs - nowMs) / 60000))} min left`;
      continue;
    }
    if (sym.cooldownUntilMs && nowMs < sym.cooldownUntilMs) { sym.status = "cooldown"; sym.note = `cooling down ${Math.ceil((sym.cooldownUntilMs - nowMs) / 60000)} min`; sym.drop = null; continue; }
    const drop = measureDrop(bars[symbol], settings, nowMs);
    sym.drop = drop ? { high: drop.high, low: drop.low, dropPct: Math.round(drop.dropPct * 100) / 100, minutes: Math.round(drop.minutes), speed: Math.round(drop.speed * 100) / 100, z: Math.round(drop.z * 10) / 10, sigmaPct: Math.round(drop.sigmaPct * 1000) / 1000, requiredPct: Math.round(drop.requiredPct * 100) / 100, settleBarsNeeded: drop.settleBarsNeeded, qualifies: drop.qualifies, lowAt: drop.lowAt } : null;
    if (!drop) { sym.status = "watching"; sym.note = "no fall in the last " + settings.dropWindowMin + " min"; continue; }
    if (!drop.qualifies) { sym.status = "watching"; sym.note = drop.why; continue; }
    const key = `${symbol}_${drop.lowAt}`;
    if (sym.seenDropKey !== key) { sym.seenDropKey = key; const ev = { kind: "DROP", symbol, atMs: nowMs, price: print.p, high: drop.high, low: drop.low, dropPct: Math.round(drop.dropPct * 100) / 100, minutes: Math.round(drop.minutes), z: Math.round(drop.z * 10) / 10, speed: Math.round(drop.speed * 100) / 100, sigmaPct: Math.round(drop.sigmaPct * 1000) / 1000, settleBarsNeeded: drop.settleBarsNeeded, detail: `fell ${drop.dropPct.toFixed(2)}% in ${Math.round(drop.minutes)} min (z ${drop.z.toFixed(1)}, speed ${drop.speed.toFixed(1)}× normal) · needs ${drop.settleBarsNeeded} steady bars` }; events.push(ev); await logSignal(D, accountId, ev); }
    const st = settled(bars[symbol], drop, settings, print.p);
    if (!st.ok) { sym.status = "settling"; sym.note = st.why; continue; }
    const blockers = [];
    if (!session.open) blockers.push("market closed");
    if (nowMs >= flattenFromMs - settings.maxHoldMin * 60000 / 3) blockers.push("too close to the end of the session");
    if (Object.keys(state.open).length >= settings.maxOpen) blockers.push(`${settings.maxOpen} dip position(s) already open`);
    if (state.tradesToday >= settings.maxTradesPerDay) blockers.push("daily trade limit reached");
    const laneUsd = laneAvailableUsd(settings, state);
    if (laneUsd < 500) blockers.push(`lane budget used up (${Math.round(laneUsd)} left of ${settings.budgetUsd})`);
    if (sym.enteredDropKey === key) blockers.push("already traded this fall");
    if (blockers.length) { sym.status = "armed"; sym.note = "ready but " + blockers.join("; "); continue; }
    const r = await enter({ D, accountId, symbol, price: print.p, drop, settings, nowMs, laneAvailableUsd: laneUsd, reason: `Fell ${drop.dropPct.toFixed(2)}% in ${Math.round(drop.minutes)} min (${drop.speed.toFixed(1)}× its normal pace, z ${drop.z.toFixed(1)}), then held above ${drop.low.toFixed(2)} for ${drop.settleBarsNeeded} bars (${st.why}); buying the settle with a target ${settings.retracePct}% back up the fall and a stop ${settings.stopPct}% of the fall below the low.` });
    sym.enteredDropKey = key;
    if (!r.entered) { sym.status = "armed"; sym.note = r.why; const ev = { kind: "SKIP", symbol, atMs: nowMs, price: print.p, detail: r.why }; events.push(ev); await logSignal(D, accountId, ev); continue; }
    state.tradesToday += 1;
    state.open[symbol] = { orderSetId: r.orderSetId, qty: r.qty, entry: r.entry, stop: r.stop, target: r.target, deadlineMs: r.deadlineMs, enteredAtMs: nowMs, drop: sym.drop, stability: st.stability == null ? null : Math.round(st.stability * 100) / 100, feeUsd: Math.round(r.qty * 0.005 * 100) / 100 };
    sym.status = "holding"; sym.note = `bought ${r.qty} @ ${r.entry.toFixed(2)}`;
    const ev = { kind: "ENTER", symbol, atMs: nowMs, price: r.entry, quantity: r.qty, stop: r.stop, target: r.target, dropPct: sym.drop.dropPct, minutes: sym.drop.minutes, detail: r.reason, orderSetId: r.orderSetId, fillId: r.fillId };
    events.push(ev); await logSignal(D, accountId, ev);
  }
  return events;
}

/* ── status for the console ────────────────────────────────────────────── */
async function status(D, accountId, settingsRaw) {
  const settings = normalizeSettings(settingsRaw);
  const state = await loadState(D, accountId);
  const sigSnap = await D.col(D.COL.dipSignals).where("accountId", "==", accountId).orderBy("atMs", "desc").limit(60).get().catch(async () => D.col(D.COL.dipSignals).where("accountId", "==", accountId).limit(60).get());
  const signals = sigSnap.docs.map((d) => d.data()).sort((a, b) => Number(b.atMs) - Number(a.atMs)).slice(0, 60);
  const today = M.sessionState(new Date()).date;
  const exits = signals.filter((s) => s.kind === "EXIT");
  const todayExits = exits.filter((s) => new Date(Number(s.atMs)).toISOString().slice(0, 10) === today);
  const sum = (xs) => Math.round(xs.reduce((n, s) => n + (Number(s.pnlUsd) || 0), 0) * 100) / 100;
  return { settings, state: { day: state.day || null, tradesToday: Number(state.tradesToday) || 0, updatedAtMs: state.updatedAtMs || null, loopAliveAtMs: state.loopAliveAtMs || null, loopNote: state.loopNote || null, symbols: state.symbols || {}, open: state.open || {}, realizedUsd: Number(state.realizedUsd) || 0, laneAvailableUsd: Math.round(laneAvailableUsd(settings, state)), excludedAiHeld: state.excludedAiHeld || [] },
    signals, results: { todayPnlUsd: sum(todayExits), todayTrades: todayExits.length, allPnlUsd: sum(exits), allTrades: exits.length, wins: exits.filter((s) => Number(s.pnlUsd) > 0).length } };
}

module.exports = { VERSION, DEFAULTS, BOUNDS, normalizeSettings, latestTrades, seedBars, applyPrint, minuteSigmaPct, measureDrop, settled, loadState, saveState, watchlist, laneAvailableUsd, evaluate, status, enter, exit };
