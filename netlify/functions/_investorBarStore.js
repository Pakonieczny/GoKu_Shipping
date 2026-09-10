"use strict";
/*  _investorBarStore.js — the historical price library.
 *
 *  One-minute, regular-hours SIP bars for every company in the universe,
 *  stored as one gzipped document per company per month
 *  (InvestorAI_MinuteBars/{SYMBOL}_{YYYY-MM}). Built once by a background
 *  job that reports its progress, then read by every simulation; a month a
 *  simulation needs that is not in the library yet is fetched on demand and
 *  saved for next time. Deterministic, AI-free, prices only.
 */
const zlib = require("zlib");
const A = require("./_investorAdmin");
const M = require("./_investorMarket");

const COL = "InvestorAI_MinuteBars";
const STATUS_COL = "InvestorAI_BarRepository";
const STATUS_DOC = "status";
const MONTHS_BACK = 60;               // five years
const RECENT_MONTHS = 5;              // stage one: about the last 100 trading days, built first
const REQUESTS_PER_MINUTE = 190;      // Alpaca Basic allows 200 per key; workers share it
const CONCURRENCY = 6;                // parallel fetches inside one worker
const VERSION = "bars.v1";

function pad(n) { return String(n).padStart(2, "0"); }
function ymOf(dateStr) { return dateStr.slice(0, 7); }
function monthsList(back = MONTHS_BACK, nowMs = A.now()) {
  const out = [];
  const ny = M.nyParts(new Date(nowMs));
  let y = Number(ny.date.slice(0, 4)), m = Number(ny.date.slice(5, 7));
  for (let i = 0; i < back; i++) { out.push(`${y}-${pad(m)}`); m -= 1; if (m === 0) { m = 12; y -= 1; } }
  return out;                          // newest first
}
function monthBounds(ym) {
  const y = Number(ym.slice(0, 4)), m = Number(ym.slice(5, 7));
  const lastDay = new Date(Date.UTC(y, m, 0)).getUTCDate();
  return { first: `${ym}-01`, last: `${ym}-${pad(lastDay)}` };
}
function universeSymbols() {
  const U = require("./_investorUniverse");
  return [...new Set([...(U.tradeTier || []), ...(U.researchTier || [])].map((r) => String(r.symbol || "").toUpperCase()).filter((s) => /^[A-Z][A-Z0-9.-]{0,9}$/.test(s)))].sort();
}

/* ── encoding: {date: [[minuteIndex, o, h, l, c, v], …]} → gzip ── */
function pack(byDate) {
  const compact = {};
  for (const [date, bars] of Object.entries(byDate)) compact[date] = bars.map((b) => [b.i, Math.round(b.o * 100), Math.round(b.h * 100), Math.round(b.l * 100), Math.round(b.c * 100), Math.round(b.v)]);
  return zlib.gzipSync(Buffer.from(JSON.stringify(compact)));
}
function unpack(buf, symbolMonth) {
  const raw = Buffer.isBuffer(buf) ? buf : Buffer.from(buf.buffer || buf);
  const compact = JSON.parse(zlib.gunzipSync(raw).toString());
  const out = {};
  for (const [date, rows] of Object.entries(compact)) {
    const openMs = M.nyWallClockToUtcMs(date, 570);
    out[date] = rows.map((r) => ({ t: openMs + r[0] * 60000, i: r[0], o: r[1] / 100, h: r[2] / 100, l: r[3] / 100, c: r[4] / 100, v: r[5], done: true }));
  }
  return out;
}

/* ── provider fetch: one company, one month ── */
/* token bucket shared by every concurrent fetch in this process */
const bucket = { tokens: REQUESTS_PER_MINUTE, refilledAt: Date.now() };
async function pace() {
  for (;;) {
    const now = Date.now(), elapsed = now - bucket.refilledAt;
    if (elapsed > 0) { bucket.tokens = Math.min(REQUESTS_PER_MINUTE, bucket.tokens + elapsed * REQUESTS_PER_MINUTE / 60000); bucket.refilledAt = now; }
    if (bucket.tokens >= 1) { bucket.tokens -= 1; return; }
    await new Promise((r) => setTimeout(r, 250));
  }
}
async function fetchMonth(symbol, ym, { fetchImpl = globalThis.fetch } = {}) {
  await M.loadMarketSettings();
  const creds = M.providerCredentials("alpaca");
  if (!creds.keyId || !creds.secretKey) throw Object.assign(new Error("Alpaca historical-data credentials are required"), { code: "HISTORICAL_BARS_MISSING" });
  const { first, last } = monthBounds(ym);
  const start = new Date(M.nyWallClockToUtcMs(first, 570)).toISOString();
  const end = new Date(Math.min(M.nyWallClockToUtcMs(last, 960), Date.now() - 20 * 60000)).toISOString();
  const byDate = {};
  let pageToken = null, pages = 0, bars = 0;
  do {
    await pace();
    const qs = new URLSearchParams({ symbols: symbol, timeframe: "1Min", start, end, feed: "sip", adjustment: "raw", limit: "10000", sort: "asc" });
    if (pageToken) qs.set("page_token", pageToken);
    const ac = new AbortController(), timer = setTimeout(() => ac.abort(), 25000);
    let res, json;
    try { res = await fetchImpl("https://data.alpaca.markets/v2/stocks/bars?" + qs, { headers: { "APCA-API-KEY-ID": creds.keyId, "APCA-API-SECRET-KEY": creds.secretKey }, signal: ac.signal }); json = await res.json(); }
    finally { clearTimeout(timer); }
    if (res.status === 429) { await new Promise((r) => setTimeout(r, 15000)); continue; }
    if (!res.ok) throw Object.assign(new Error(`Historical price provider returned ${res.status}`), { code: "HISTORICAL_DATA_UNAVAILABLE" });
    for (const b of (json.bars && json.bars[symbol]) || []) {
      const t = Date.parse(b.t); if (!Number.isFinite(t)) continue;
      const ny = M.nyParts(new Date(t));
      if (ny.minutes < 570 || ny.minutes >= 960) continue;
      (byDate[ny.date] = byDate[ny.date] || []).push({ i: ny.minutes - 570, o: Number(b.o), h: Number(b.h), l: Number(b.l), c: Number(b.c), v: Number(b.v) || 0 });
      bars += 1;
    }
    pageToken = json.next_page_token || null;
  } while (pageToken && ++pages < 12);
  return { byDate, bars, days: Object.keys(byDate).length };
}

/* ── library reads and writes ── */
function docId(symbol, ym) { return `${symbol}_${ym}`; }
async function readMonth(D, symbol, ym) {
  const s = await D.col(COL).doc(docId(symbol, ym)).get();
  if (!s.exists) return null;
  const d = s.data();
  return { byDate: d.data ? unpack(d.data, docId(symbol, ym)) : {}, days: d.days || 0, bars: d.bars || 0, complete: d.complete !== false, fetchedAtMs: d.fetchedAtMs || null };
}
async function writeMonth(D, symbol, ym, fetched, { complete = true } = {}) {
  await D.col(COL).doc(docId(symbol, ym)).set({ symbol, month: ym, version: VERSION, days: fetched.days, bars: fetched.bars, complete, fetchedAtMs: Date.now(), data: pack(fetched.byDate) });
}
/** The month is "complete" once the month has ended (with a day's grace for
 *  late corrections); the current month is refreshed on every read. */
function monthComplete(ym, nowMs = Date.now()) { const { last } = monthBounds(ym); return M.nyWallClockToUtcMs(last, 960) + 36 * 3600000 < nowMs; }
/** Bars by date for one company over a date range, from the library; any
 *  missing month is fetched and saved. */
async function barsFor(D, symbol, fromDate, toDate, { fetchImpl = globalThis.fetch, onFetch = null } = {}) {
  const out = {};
  const months = [];
  for (let ym = ymOf(fromDate); ym <= ymOf(toDate); ) { months.push(ym); let y = Number(ym.slice(0, 4)), m = Number(ym.slice(5, 7)) + 1; if (m > 12) { m = 1; y += 1; } ym = `${y}-${pad(m)}`; }
  for (const ym of months) {
    let month = await readMonth(D, symbol, ym);
    if (!month || (!month.complete && !monthComplete(ym))) {
      const fetched = await fetchMonth(symbol, ym, { fetchImpl });
      await writeMonth(D, symbol, ym, fetched, { complete: monthComplete(ym) });
      if (onFetch) await onFetch(symbol, ym, fetched);
      month = { byDate: unpack(pack(fetched.byDate)), complete: monthComplete(ym) };
    }
    for (const [date, bars] of Object.entries(month.byDate)) if (date >= fromDate && date <= toDate) out[date] = bars;
  }
  return out;
}

/* ── the library builder: newest month first, every company ── */
async function status(D) {
  const s = await D.col(STATUS_COL).doc(STATUS_DOC).get();
  const d = s.exists ? s.data() : {};
  const symbols = universeSymbols(), months = monthsList();
  return { version: VERSION, symbols: symbols.length, months: months.length, total: symbols.length * months.length, done: Number(d.done) || 0, failed: Number(d.failed) || 0, recentMonths: RECENT_MONTHS, recentTotal: symbols.length * RECENT_MONTHS, recentDone: Number(d.recentDone) || 0, status: d.status || "idle", current: d.current || null, startedAtMs: d.startedAtMs || null, updatedAtMs: d.updatedAtMs || null, completedAtMs: d.completedAtMs || null, ratePerMin: d.ratePerMin || null, oldestMonth: months[months.length - 1], newestMonth: months[0], cursor: d.cursor || null, recentErrors: (d.recentErrors || []).slice(-5), note: d.note || null };
}
async function setStatus(D, patch) { await D.col(STATUS_COL).doc(STATUS_DOC).set({ ...patch, updatedAtMs: Date.now() }, { merge: true }); }
/** Work through (month, symbol) pairs newest month first, skipping what the
 *  library already holds, for up to `budgetMs`. Returns whether more remains. */
async function buildSegment(D, { budgetMs = 11 * 60000, fetchImpl = globalThis.fetch } = {}) {
  const symbols = universeSymbols(), months = monthsList();
  const startedAt = Date.now();
  const st = await status(D);
  if (st.status === "paused") return { more: false, paused: true };
  const errors = [];
  /* what the library already holds, counted once per segment so the bar is honest */
  const existing = new Set();
  const snap = await D.col(COL).select("symbol", "month", "fetchedAtMs", "complete").get();
  const fetchedAt = {};
  snap.docs.forEach((d) => { const x = d.data(); const id = docId(x.symbol, x.month); existing.add(id); fetchedAt[id] = { at: Number(x.fetchedAtMs) || 0, complete: x.complete !== false }; });
  const recentSet = new Set(months.slice(0, RECENT_MONTHS));
  const countDone = () => { let done = 0, recent = 0; for (const id of existing) { done += 1; if (recentSet.has(id.slice(id.lastIndexOf("_") + 1))) recent += 1; } return { done, recent }; };
  let counts = countDone(), failed = 0, fetched = 0;
  await setStatus(D, { status: "running", startedAtMs: st.startedAtMs || Date.now(), done: counts.done, recentDone: counts.recent, total: symbols.length * months.length });
  /* work list: stage one is the newest months for every company, then the rest, newest first */
  const work = [];
  for (const ym of months) for (const symbol of symbols) {
    const id = docId(symbol, ym), have = existing.has(id), complete = monthComplete(ym);
    if (have && complete) continue;
    if (have && !complete && fetchedAt[id] && Date.now() - fetchedAt[id].at < 6 * 3600000) continue;
    work.push({ symbol, ym, id });
  }
  if (!work.length) { await setStatus(D, { status: "complete", done: counts.done, recentDone: counts.recent, current: null, completedAtMs: Date.now() }); return { more: false, done: counts.done, fetched: 0 }; }
  let cursor = 0, stop = false, lastStatusAt = Date.now(), current = null;
  const worker = async () => {
    while (!stop && cursor < work.length) {
      if (Date.now() - startedAt > budgetMs || await A.isStopped()) { stop = true; break; }
      const item = work[cursor++]; current = item;
      try {
        const got = await fetchMonth(item.symbol, item.ym, { fetchImpl });
        await writeMonth(D, item.symbol, item.ym, got, { complete: monthComplete(item.ym) });
        if (!existing.has(item.id)) existing.add(item.id);
        fetched += 1;
      } catch (e) {
        failed += 1; errors.push(`${item.symbol} ${item.ym}: ${String(e.message).slice(0, 60)}`);
        if (e.code === "HISTORICAL_BARS_MISSING") { stop = true; await setStatus(D, { status: "failed", note: e.message, recentErrors: errors.slice(-5) }); return; }
      }
      if (Date.now() - lastStatusAt > 6000) { lastStatusAt = Date.now(); counts = countDone(); await setStatus(D, { done: counts.done, recentDone: counts.recent, failed: st.failed + failed, current: { symbol: current.symbol, month: current.ym }, ratePerMin: Math.round(fetched / Math.max(0.1, (Date.now() - startedAt) / 60000) * 10) / 10, recentErrors: errors.slice(-5) }); }
    }
  };
  await Promise.all(Array.from({ length: CONCURRENCY }, () => worker()));
  counts = countDone();
  const more = cursor < work.length || stop;
  const finished = !more && work.every((w) => existing.has(w.id));
  await setStatus(D, { status: finished ? "complete" : "running", done: counts.done, recentDone: counts.recent, failed: st.failed + failed, current: finished ? null : current && { symbol: current.symbol, month: current.ym }, ratePerMin: Math.round(fetched / Math.max(0.1, (Date.now() - startedAt) / 60000) * 10) / 10, completedAtMs: finished ? Date.now() : null, recentErrors: errors.slice(-5) });
  return { more: !finished, done: counts.done, fetched };
}
module.exports = { COL, STATUS_COL, VERSION, MONTHS_BACK, monthsList, monthBounds, universeSymbols, pack, unpack, fetchMonth, readMonth, writeMonth, barsFor, status, setStatus, buildSegment, monthComplete };
