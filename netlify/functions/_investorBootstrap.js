/*  netlify/functions/_investorBootstrap.js  (v1.0)
 *  ---------------------------------------------------------------------------
 *  Investor_AI — self-bootstrap and auto-population.
 *
 *  Everything that used to be a button in the control room now happens by
 *  itself on the first cycle, and re-checks cheaply on every cycle after.
 *  The operator's job is reduced to deciding a market-data provider and
 *  approving trades.
 *
 *  WHAT THIS AUTOMATES
 *  ------------------------------------------------------------------------
 *   1. UNIVERSE FREEZE   stamps the roster version so additions cannot be
 *                        backdated.
 *   2. CIK RESOLUTION    pulls SEC's own ticker->CIK map. 29 of 45 names ship
 *                        with cik:null deliberately — a guessed CIK silently
 *                        polls the wrong filer, which is worse than none.
 *   3. PAPER ACCOUNT     opens the virtual account and posts the balanced
 *                        capital-contribution entry.
 *   4. EARNINGS WINDOWS  derived from each company's own EDGAR filing cadence.
 *                        Zero new credentials. See the honesty note below.
 *   5. REGIME            VIX from Cboe's public CSV, with its own trailing
 *                        median computed from the same file. COR3M attempted
 *                        from the sibling path and degraded safely if absent.
 *
 *  HONESTY NOTE ON DERIVED EARNINGS DATES
 *  ------------------------------------------------------------------------
 *  There is no free, keyless feed of forward earnings dates. What EDGAR does
 *  give is every past 10-Q and 10-K filing date, and those recur on a tight
 *  ~91-day cadence. The next date is projected from the median gap.
 *
 *  That projection is an ESTIMATE and is labelled as one everywhere it is
 *  used. Two consequences follow, both deliberate:
 *    · the blackout window is WIDER than the ±2 days an exact date would need,
 *      because it must absorb the projection error;
 *    · a symbol whose window cannot be derived is BLOCKED rather than allowed.
 *      An unknown earnings date is the single most dangerous state for this
 *      strategy — AMD's 95th-percentile earnings move is ±21.2% against a
 *      ±9.9% median — so the failure mode has to be "does not trade", never
 *      "trades blind".
 * ---------------------------------------------------------------------------
 */

"use strict";

const A = require("./_investorAdmin");
const { fetchPublic } = require("./_investorFetch");
const L = require("./_investorLedger");
const H = require("./_investorHistory");
const U_FALLBACK = require("./_investorUniverse.js");

const BOOTSTRAP_VERSION = 4;   // bump to force a re-run after a config change

/* ── 1 + 2. universe freeze and CIK resolution ─────────────────────────── */
async function resolveCiksAndFreeze() {
  const base = require("./_investorUniverse.js");
  const report = { version: base.version, resolved: 0, missing: [], mismatched: [] };

  let map = null;
  try {
    const r = await fetchPublic("https://www.sec.gov/files/company_tickers.json", {
      sourceId: "sec.tickers", accept: ["json"], timeoutMs: 25000,
    });
    if (r.json) {
      map = {};
      for (const v of Object.values(r.json)) {
        if (v && v.ticker) map[String(v.ticker).toUpperCase()] = String(v.cik_str);
      }
    }
  } catch (e) {
    report.error = `SEC ticker map unavailable: ${String(e.code || e.message).slice(0, 120)}`;
  }

  const apply = (row) => {
    if (!map) return row;
    const got = map[row.symbol];
    if (!got) { report.missing.push(row.symbol); return row; }
    if (row.cik && row.cik !== got) report.mismatched.push({ symbol: row.symbol, had: row.cik, sec: got });
    report.resolved += 1;
    return { ...row, cik: got, cikSource: "sec_company_tickers" };
  };

  /* Drop anything SEC cannot match. A delisted or renamed ticker otherwise
     sits in the roster producing stale data forever — exactly what happened
     with X (delisted) and EA (taken private) until a fixture caught them.
     Self-correction beats me maintaining a list by hand. */
  const resolvedTrade = (base.tradeTier || []).map(apply).filter((r) => !!r.cik);
  const dropped = (base.tradeTier || []).length - resolvedTrade.length;

  const frozen = {
    ...base,
    tradeTier: resolvedTrade,
    researchTier: (base.researchTier || []).map(apply).filter((r) => !!r.cik),
    unresolvedDropped: report.missing,
    droppedCount: dropped,
    cikResolvedAt: new Date().toISOString(),
  };
  report.dropped = dropped;

  await A.col(A.COL.universe).doc(base.version).set({
    ...frozen,
    frozenAt: A.FV.serverTimestamp(),
    frozenBy: "bootstrap",
    ...A.envelope({ created_by: "bootstrap.resolveCiksAndFreeze" }),
  }, { merge: true });

  return { frozen, report };
}

/* ── 4. earnings windows from EDGAR filing cadence ─────────────────────── */
const MS_DAY = 864e5;

function median(xs) {
  if (!xs.length) return null;
  const s = [...xs].sort((a, b) => a - b);
  const m = Math.floor(s.length / 2);
  return s.length % 2 ? s[m] : (s[m - 1] + s[m]) / 2;
}

/**
 * Project the next earnings date from the company's own 10-Q / 10-K cadence.
 * @returns {{dates:string[], estimated:true, cadenceDays:number, basis:string}|null}
 */
async function deriveEarningsWindow(cik) {
  if (!cik) return null;
  const padded = String(cik).padStart(10, "0");
  let r;
  try {
    r = await fetchPublic(`https://data.sec.gov/submissions/CIK${padded}.json`, {
      sourceId: "sec.submissions", accept: ["json"], timeoutMs: 20000,
    });
  } catch { return null; }
  if (!r.json || !r.json.filings || !r.json.filings.recent) return null;

  const rec = r.json.filings.recent;
  const forms = rec.form || [], dates = rec.filingDate || [];
  const periodic = [];
  for (let i = 0; i < forms.length; i++) {
    if (forms[i] === "10-Q" || forms[i] === "10-K") {
      const t = Date.parse(dates[i]);
      if (isFinite(t)) periodic.push(t);
    }
  }
  if (periodic.length < 4) return null;

  periodic.sort((a, b) => b - a);               // newest first
  const gaps = [];
  for (let i = 0; i + 1 < Math.min(periodic.length, 9); i++) {
    const g = (periodic[i] - periodic[i + 1]) / MS_DAY;
    if (g > 40 && g < 140) gaps.push(g);         // ignore amendments and gaps
  }
  const cadence = median(gaps);
  if (!cadence) return null;

  // Project forward from the most recent periodic filing until we are in the
  // future, then keep the next two so a blackout is never missed at a boundary.
  const out = [];
  let t = periodic[0];
  let guard = 0;
  while (out.length < 3 && guard < 12) {
    t += cadence * MS_DAY;
    if (t > Date.now() - 7 * MS_DAY) out.push(new Date(t).toISOString().slice(0, 10));
    guard += 1;
  }
  if (!out.length) return null;

  return {
    dates: out,
    estimated: true,
    cadenceDays: Math.round(cadence),
    basis: `projected from ${gaps.length + 1} recent 10-Q/10-K filings, median cadence ${Math.round(cadence)}d`,
    lastPeriodicFiling: new Date(periodic[0]).toISOString().slice(0, 10),
    derivedAt: new Date().toISOString(),
  };
}

async function populateEarnings(universe) {
  const rows = universe.tradeTier || [];
  const out = {}; let derived = 0, failed = [];
  for (const row of rows) {
    if (!row.cik) { failed.push(row.symbol); continue; }
    try {
      const w = await deriveEarningsWindow(row.cik);
      if (w) { out[row.symbol] = w; derived += 1; } else failed.push(row.symbol);
    } catch { failed.push(row.symbol); }
  }
  await A.col(A.COL.control).doc("earnings").set({
    windows: out, derived, failed,
    method: "EDGAR 10-Q/10-K cadence projection",
    estimated: true,
    updatedAt: A.FV.serverTimestamp(),
  }, { merge: true });
  return { derived, failed };
}

async function readEarnings() {
  const s = await A.col(A.COL.control).doc("earnings").get();
  return s.exists ? (s.data().windows || {}) : {};
}

/* ── 5. regime: VIX and implied correlation ────────────────────────────── */
function parseCboeCsv(text) {
  // Cboe daily-price CSVs are: DATE,OPEN,HIGH,LOW,CLOSE
  const rows = [];
  for (const line of String(text || "").split(/\r?\n/)) {
    const p = line.split(",");
    if (p.length < 5) continue;
    const d = Date.parse(p[0]);
    const c = Number(p[4]);
    if (isFinite(d) && isFinite(c) && c > 0) rows.push({ t: d, close: c });
  }
  rows.sort((a, b) => a.t - b.t);
  return rows;
}

async function refreshRegime() {
  const out = { asOf: new Date().toISOString(), setBy: "bootstrap" };

  // VIX — confirmed public CSV, no credential.
  try {
    const r = await fetchPublic(
      "https://cdn.cboe.com/api/global/us_indices/daily_prices/VIX_History.csv",
      { sourceId: "cboe.vix", accept: ["csv", "text", "octet"], timeoutMs: 25000, maxBytes: 4e6 });
    const rows = parseCboeCsv(r.text);
    if (rows.length > 60) {
      out.vix = Number(rows[rows.length - 1].close.toFixed(2));
      // Trailing one-year median, from the same file — this is the
      // normalisation Nagel's conditioning needs.
      const last = rows.slice(-252).map((x) => x.close);
      out.vixMedian = Number(median(last).toFixed(2));
      out.vixAsOf = new Date(rows[rows.length - 1].t).toISOString().slice(0, 10);
      out.vixSource = "cboe VIX_History.csv";
    }
  } catch (e) {
    out.vixError = String(e.code || e.message).slice(0, 120);
  }

  // COR3M — same path convention. Not confirmed to exist; if it 404s the
  // dispersion gate degrades to "unknown" and halves position size rather
  // than failing, which is the correct conservative default.
  for (const sym of ["COR3M", "COR1M"]) {
    if (out.cor3m) break;
    try {
      const r = await fetchPublic(
        `https://cdn.cboe.com/api/global/us_indices/daily_prices/${sym}_History.csv`,
        { sourceId: `cboe.${sym.toLowerCase()}`, accept: ["csv", "text", "octet"], timeoutMs: 20000, maxBytes: 4e6 });
      const rows = parseCboeCsv(r.text);
      if (rows.length) {
        out.cor3m = Number(rows[rows.length - 1].close.toFixed(2));
        out.corSource = `cboe ${sym}_History.csv`;
        out.corAsOf = new Date(rows[rows.length - 1].t).toISOString().slice(0, 10);
      }
    } catch (e) {
      out.corError = `${sym}: ${String(e.code || e.message).slice(0, 80)}`;
    }
  }

  await A.col(A.COL.control).doc("regime").set(out, { merge: true });
  return out;
}

/* ── the entry point the cycle calls ───────────────────────────────────── */
/**
 * Idempotent. Expensive work runs once; after that this is a single read.
 * Regime and earnings refresh on their own slower clocks.
 */
/* ── 6. DAILY HISTORY BACKFILL ─────────────────────────────────────────────
 * Pull ~13 months of daily bars for every roster name so the system has a
 * six-month memory from its very first cycle rather than earning one slowly
 * over months of running.
 *
 * Resumable by design. A Netlify background function gets 15 minutes; 342
 * symbols of daily bars fits comfortably inside that, but a provider hiccup
 * must not mean starting over. A cursor in control/history records how far it
 * got, and the next cycle picks up from there. Symbols already holding enough
 * days are skipped, which is what makes the daily top-up cheap.
 */
async function backfillDailyHistory(universe, { budgetMs = 240000, maxSymbols = 400 } = {}) {
  const started = Date.now();
  const tier = (universe && universe.tradeTier) || [];
  const symbols = tier.map((t) => t.symbol).filter(Boolean);

  const curRef = A.col(A.COL.control).doc("history");
  const curSnap = await curRef.get();
  const cur = curSnap.exists ? curSnap.data() : {};
  const doneSet = new Set(cur.completed || []);

  const todo = symbols.filter((s) => !doneSet.has(s)).slice(0, maxSymbols);
  if (!todo.length) {
    return { complete: true, symbols: symbols.length, fetched: 0, note: "daily history already present for every roster name" };
  }

  let fetched = 0, failed = 0, days = 0;
  const chunk = 90;
  for (let i = 0; i < todo.length; i += chunk) {
    if (Date.now() - started > budgetMs) break;      // leave room for the rest of the run
    const part = todo.slice(i, i + chunk);
    let got = {};
    try { got = await H.fetchDaily(part, { days: H.KEEP_DAYS }); }
    catch (e) { failed += part.length; continue; }

    for (const sym of part) {
      const bars = got[sym] || [];
      if (!bars.length) { failed += 1; continue; }
      try {
        const n = await H.writeDaily(sym, bars, { source: "bootstrap.backfill" });
        fetched += 1; days += n; doneSet.add(sym);
      } catch (e) { failed += 1; }
    }
  }

  await curRef.set({
    completed: [...doneSet],
    completedCount: doneSet.size,
    rosterCount: symbols.length,
    lastRunAt: A.FV.serverTimestamp(),
    ...A.envelope({ created_by: "investorBootstrap" }),
  }, { merge: true });

  return {
    complete: doneSet.size >= symbols.length,
    symbols: symbols.length, done: doneSet.size,
    fetched, failed, avgDays: fetched ? Math.round(days / fetched) : 0,
  };
}

/* Append the most recent sessions to every symbol already backfilled. Cheap:
   a 5-day pull per chunk, merged by date, so re-running is harmless. */
async function topUpDailyHistory(universe, { budgetMs = 90000 } = {}) {
  const started = Date.now();
  const tier = (universe && universe.tradeTier) || [];
  const symbols = tier.map((t) => t.symbol).filter(Boolean);
  let updated = 0;
  const chunk = 90;
  for (let i = 0; i < symbols.length; i += chunk) {
    if (Date.now() - started > budgetMs) break;
    const part = symbols.slice(i, i + chunk);
    try {
      const got = await H.fetchDaily(part, { days: 7 });
      for (const sym of part) {
        const bars = got[sym] || [];
        if (bars.length) { await H.writeDaily(sym, bars, { source: "cycle.topup" }); updated += 1; }
      }
    } catch (e) { /* a failed top-up is retried next session */ }
  }
  return { updated, symbols: symbols.length };
}

/* ── SHARES OUTSTANDING, FROM THE SEC ──────────────────────────────────────
 * Needed for the turnover-conditioning gate (Medhat & Schmeling, RFS 2022):
 * turnover = shares traded / shares outstanding, and reversal only works
 * outside the top-turnover decile. dei:EntityCommonStockSharesOutstanding
 * from the companyconcept endpoint is a small, per-company JSON — not the
 * multi-megabyte companyfacts blob. Resumable with a cursor, same pattern as
 * the price-history backfill; SEC's 4 requests/second budget is enforced by
 * the fetch layer.
 */
async function backfillSharesOutstanding(universe, { budgetMs = 90000 } = {}) {
  const started = Date.now();
  const tier = (universe && universe.tradeTier) || [];
  const withCik = tier.filter((t) => t.cik);

  const ref = A.col(A.COL.control).doc("shares");
  const snap = await ref.get();
  const cur = snap.exists ? snap.data() : {};
  const have = cur.bySymbol || {};

  let fetched = 0, failed = 0;
  for (const t of withCik) {
    if (Date.now() - started > budgetMs) break;
    const existing = have[t.symbol];
    // refresh quarterly; shares outstanding moves on buybacks and issuance
    if (existing && existing.asOf && Date.now() - Date.parse(existing.asOf) < 90 * 864e5) continue;
    try {
      const cik10 = String(t.cik).padStart(10, "0");
      const url = `https://data.sec.gov/api/xbrl/companyconcept/CIK${cik10}/dei/EntityCommonStockSharesOutstanding.json`;
      const r = await fetchPublic(url, { sourceId: "sec.companyconcept", accept: ["json"], timeoutMs: 10000 });
      const units = (r.json && r.json.units) || {};
      const arr = units.shares || Object.values(units)[0] || [];
      const latest = arr.filter((x) => x && x.val > 0)
        .sort((a, b) => String(a.end || "") < String(b.end || "") ? 1 : -1)[0];
      if (latest) {
        have[t.symbol] = { shares: latest.val, end: latest.end, asOf: new Date().toISOString() };
        fetched += 1;
      } else failed += 1;
    } catch (e) { failed += 1; }
  }

  await ref.set({
    bySymbol: have, count: Object.keys(have).length,
    lastRunAt: A.FV.serverTimestamp(),
    ...A.envelope({ created_by: "investorBootstrap" }),
  }, { merge: true });

  return { fetched, failed, total: Object.keys(have).length, of: withCik.length };
}

async function readShares() {
  const snap = await A.col(A.COL.control).doc("shares").get();
  return snap.exists ? (snap.data().bySymbol || {}) : {};
}

async function ensureBootstrapped({ force = false } = {}) {
  const ref = A.col(A.COL.control).doc("control");
  const snap = await ref.get();
  const c = snap.exists ? snap.data() : {};
  const done = c.bootstrapVersion === BOOTSTRAP_VERSION;

  const steps = [];
  let universe = null;

  if (!done || force) {
    // 1 + 2
    try {
      const { frozen, report } = await resolveCiksAndFreeze();
      universe = frozen;
      steps.push({ step: "universe", ok: true, ...report });
    } catch (e) { steps.push({ step: "universe", ok: false, error: String(e.message).slice(0, 160) }); }

    // 3
    try {
      const accountId = c.accountId || "paper-1";
      const r = await L.openAccount({
        accountId,
        startingNavUsd: Number(c.startingNavUsd) || 100000,
        strategyVersion: c.strategyVersion || "v1",
      });
      steps.push({ step: "account", ok: true, accountId, existing: !!r.existing });
    } catch (e) { steps.push({ step: "account", ok: false, error: String(e.message).slice(0, 160) }); }

    // 4
    try {
      const u = universe || require("./_investorUniverse.js");
      const r = await populateEarnings(u);
      steps.push({ step: "earnings", ok: true, ...r });
    } catch (e) { steps.push({ step: "earnings", ok: false, error: String(e.message).slice(0, 160) }); }

    // 5
    try {
      const r = await refreshRegime();
      steps.push({ step: "regime", ok: !!r.vix, vix: r.vix || null, cor3m: r.cor3m || null,
                   note: r.corError || r.vixError || null });
    } catch (e) { steps.push({ step: "regime", ok: false, error: String(e.message).slice(0, 160) }); }

    // 6b — shares outstanding, for the turnover gate. Best-effort; the gate
    //      abstains per-name until this fills in, and it resumes each cycle.
    try {
      const u = universe || require("./_investorUniverse.js");
      const r = await backfillSharesOutstanding(u, { budgetMs: 60000 });
      steps.push({ step: "sharesOutstanding", ok: r.fetched > 0 || r.total > 0, ...r });
    } catch (e) { steps.push({ step: "sharesOutstanding", ok: false, error: String(e.message).slice(0, 160) }); }

    // 6 — the long memory. Without this the system starts blind.
    try {
      const u = universe || require("./_investorUniverse.js");
      const r = await backfillDailyHistory(u);
      steps.push({ step: "dailyHistory", ok: r.fetched > 0 || r.complete, ...r });
    } catch (e) { steps.push({ step: "dailyHistory", ok: false, error: String(e.message).slice(0, 160) }); }

    /* DO NOT STAMP SUCCESS OVER FAILURE.
       This wrote bootstrapVersion unconditionally, so if SEC or Cboe was
       unreachable on the first cycle the degraded state became permanent:
       every roster name whose CIK failed to resolve was silently dropped, the
       truncated universe was frozen, and the only recovery was editing
       BOOTSTRAP_VERSION and redeploying. A failed critical step now leaves the
       version unstamped so the next cycle retries. */
    const criticalFailed = steps.filter((x) =>
      !x.ok && ["universe", "account"].includes(x.step));
    if (criticalFailed.length) {
      await ref.set({
        bootstrapAttemptedAt: A.FV.serverTimestamp(),
        bootstrapReport: steps,
        bootstrapIncomplete: criticalFailed.map((x) => x.step),
      }, { merge: true });
      return { bootstrapped: false, retryNext: true, steps,
               note: `bootstrap incomplete (${criticalFailed.map((x) => x.step).join(", ")} failed) — will retry next cycle rather than freezing a degraded state` };
    }

    await ref.set({
      bootstrapVersion: BOOTSTRAP_VERSION,
      bootstrapIncomplete: null,
      bootstrappedAt: A.FV.serverTimestamp(),
      bootstrapReport: steps,
      accountId: c.accountId || "paper-1",
      universeVersion: (universe && universe.version) || c.universeVersion || U_FALLBACK.version || "v1",
      strategyVersion: c.strategyVersion || "v1",
    }, { merge: true });

    return { bootstrapped: true, steps };
  }

  /* Already bootstrapped — keep the slow-moving inputs fresh. */
  const now = Date.now();
  const reg = await A.col(A.COL.control).doc("regime").get();
  const regAge = reg.exists && reg.data().asOf ? now - Date.parse(reg.data().asOf) : Infinity;
  if (regAge > 12 * 3600e3) {
    try { const r = await refreshRegime(); steps.push({ step: "regime.refresh", ok: !!r.vix, vix: r.vix }); }
    catch (e) { steps.push({ step: "regime.refresh", ok: false, error: String(e.message).slice(0, 120) }); }
  }

  const earn = await A.col(A.COL.control).doc("earnings").get();
  const earnAge = earn.exists && earn.data().updatedAt && earn.data().updatedAt.toDate
    ? now - earn.data().updatedAt.toDate().getTime() : Infinity;
  if (earnAge > 7 * 24 * 3600e3) {
    try {
      const uSnap = await A.col(A.COL.universe).doc(c.universeVersion || U_FALLBACK.version || "v1").get();
      const u = uSnap.exists ? uSnap.data() : require("./_investorUniverse.js");
      const r = await populateEarnings(u);
      steps.push({ step: "earnings.refresh", ok: true, ...r });
    } catch (e) { steps.push({ step: "earnings.refresh", ok: false, error: String(e.message).slice(0, 120) }); }
  }

  /* Finish any unfinished backfill, then keep the daily spine current. The
     backfill is idempotent and skips completed names, so this costs almost
     nothing once it has caught up. */
  try {
    const uSnap = await A.col(A.COL.universe).doc(c.universeVersion || U_FALLBACK.version || "v1").get();
    const u = uSnap.exists ? uSnap.data() : require("./_investorUniverse.js");

    const hSnap = await A.col(A.COL.control).doc("history").get();
    const h = hSnap.exists ? hSnap.data() : {};
    const rosterN = ((u.tradeTier || []).length) || 0;

    if ((h.completedCount || 0) < rosterN) {
      const r = await backfillDailyHistory(u, { budgetMs: 120000 });
      steps.push({ step: "dailyHistory.resume", ok: true, ...r });
    } else {
      const lastTop = h.lastTopUpAt && h.lastTopUpAt.toDate ? h.lastTopUpAt.toDate().getTime() : 0;
      if (now - lastTop > 12 * 3600e3) {
        const r = await topUpDailyHistory(u);
        await A.col(A.COL.control).doc("history").set(
          { lastTopUpAt: A.FV.serverTimestamp() }, { merge: true });
        steps.push({ step: "dailyHistory.topUp", ok: true, ...r });
      }
    }
  } catch (e) { steps.push({ step: "dailyHistory", ok: false, error: String(e.message).slice(0, 120) }); }

  try {
    const uSnap2 = await A.col(A.COL.universe).doc(c.universeVersion || U_FALLBACK.version || "v1").get();
    const u2 = uSnap2.exists ? uSnap2.data() : require("./_investorUniverse.js");
    const sSnap = await A.col(A.COL.control).doc("shares").get();
    const sCount = sSnap.exists ? (sSnap.data().count || 0) : 0;
    const withCik = ((u2.tradeTier || []).filter((t) => t.cik)).length;
    if (sCount < withCik) {
      const r = await backfillSharesOutstanding(u2, { budgetMs: 45000 });
      steps.push({ step: "sharesOutstanding.resume", ok: true, ...r });
    }
  } catch (e) { steps.push({ step: "sharesOutstanding", ok: false, error: String(e.message).slice(0, 120) }); }

  return { bootstrapped: false, steps };
}

module.exports = {
  backfillDailyHistory, topUpDailyHistory,
  backfillSharesOutstanding, readShares,
  BOOTSTRAP_VERSION,
  ensureBootstrapped, resolveCiksAndFreeze,
  deriveEarningsWindow, populateEarnings, readEarnings,
  refreshRegime, parseCboeCsv, median,
};
