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

const crypto = require("crypto");
const A = require("./_investorAdmin");
const { fetchPublic } = require("./_investorFetch");
const L = require("./_investorLedger");
const H = require("./_investorHistory");
const T = require("./_investorTemporal");
const U_FALLBACK = require("./_investorUniverse.js");
const S_FALLBACK = require("./_investorStrategy.js");
const V = require("./_investorVariants");
const STATE = require("./_investorState");

const BOOTSTRAP_VERSION = 15;
const DAILY_PROVENANCE_VERSION = 5; // v5 re-attests complete windows to the selected 15-minute SIP identity

function stable(value) {
  if (Array.isArray(value)) return value.map(stable);
  if (value && typeof value === "object") return Object.fromEntries(Object.keys(value).sort()
    .filter((k) => !["contentHash","frozenAt","cikResolvedAt","created_at","updated_at"].includes(k))
    .map((k) => [k, stable(value[k])]));
  return value;
}
function hashObject(value) { return crypto.createHash("sha256").update(JSON.stringify(stable(value))).digest("hex"); }
function universeHash(u) { return hashObject({ version:u.version, tradeTier:u.tradeTier||[], researchTier:u.researchTier||[],
  excludedTier:u.excludedTier||[], enforcement:u.enforcement||{} }); }
function strategyHash(s) { return hashObject({ version:s.version, parameters:s.parameters||{},
  portfolioControls:s.portfolioControls||{}, autoApproval:s.autoApproval||{},
  exploratoryAuto:s.exploratoryAuto||{}, operatorCeiling:s.operatorCeiling||"research" }); }

/* _investorStrategy is both a runtime module and a frozen configuration: it
   exports helper functions used by the cycle as well as the data that belongs
   in Firestore. Real Firestore rejects JavaScript functions. The old in-memory
   store silently discarded them, allowing an impossible write to pass every
   local lifecycle test. Persist only the configuration portion, and reject a
   future nested function/cycle before a transaction is attempted. */
function unsupportedStoragePath(value, path = "strategy", ancestors = new Set()) {
  if (typeof value === "function" || typeof value === "symbol") return path;
  if (!value || typeof value !== "object") return null;
  if (ancestors.has(value)) return path;
  ancestors.add(value);
  const entries = Array.isArray(value)
    ? value.map((item, index) => [String(index), item]) : Object.entries(value);
  for (const [key, item] of entries) {
    const bad = unsupportedStoragePath(item, `${path}.${key}`, ancestors);
    if (bad) { ancestors.delete(value); return bad; }
  }
  ancestors.delete(value);
  return null;
}
function strategyDocument(runtimeModule) {
  const document = Object.fromEntries(Object.entries(runtimeModule || {})
    .filter(([, value]) => typeof value !== "function" && value !== undefined));
  if (!document.version || !document.parameters || !document.exploratoryAuto) {
    throw new Error("strategy configuration export is incomplete");
  }
  const bad = unsupportedStoragePath(document);
  if (bad) throw new Error(`strategy configuration is not Firestore-serializable at ${bad}`);
  return document;
}
function validateFrozenUniverse(u, version) {
  if (!u || u.version !== version || u.immutable !== true) return { ok:false, reason:"version is not immutable" };
  const actual=universeHash(u); return {ok:actual===u.contentHash,actual,expected:u.contentHash||null};
}

/* ── 1 + 2. universe freeze and CIK resolution ─────────────────────────── */
const SEC_RENAMED_TICKERS = Object.freeze({
  FB: "META", GPS: "GAP", ANTM: "ELV", PKI: "RVTY", RE: "EG", FISV: "FI",
});
function canonicalSecTicker(value) {
  return String(value || "").trim().toUpperCase().replace(/[./]/g, "-");
}
function buildSecTickerMap(payload) {
  const map = new Map(), ambiguous = new Set();
  for (const value of Object.values(payload || {})) {
    if (!value || !value.ticker || !/^\d+$/.test(String(value.cik_str || ""))) continue;
    const key = canonicalSecTicker(value.ticker);
    const row = { cik: String(value.cik_str), company: String(value.title || "").trim() || null,
      secTicker: String(value.ticker).toUpperCase() };
    if (map.has(key) && map.get(key).cik !== row.cik) ambiguous.add(key);
    else map.set(key, row);
  }
  return { map, ambiguous };
}
function resolveSecTicker(index, requestedSymbol) {
  const requested = String(requestedSymbol || "").trim().toUpperCase();
  const canonical = canonicalSecTicker(requested);
  if (!canonical) return null;
  if (index.ambiguous && index.ambiguous.has(canonical)) {
    return { error: "ambiguous_sec_ticker", requestedSymbol: requested, canonical };
  }
  const direct = index.map && index.map.get(canonical);
  if (direct) return { ...direct, requestedSymbol: requested,
    resolvedSymbol: requested, canonical, resolutionKind:
      canonical === requested ? "exact" : "punctuation_alias" };
  const renamed = SEC_RENAMED_TICKERS[requested];
  const renamedCanonical = canonicalSecTicker(renamed);
  if (!renamedCanonical || (index.ambiguous && index.ambiguous.has(renamedCanonical))) return null;
  const current = index.map && index.map.get(renamedCanonical);
  return current ? { ...current, requestedSymbol: requested, resolvedSymbol: renamed,
    canonical: renamedCanonical, resolutionKind: "renamed_ticker" } : null;
}

/* The deploy archive carries a provenance-bound SEC identity snapshot beside
   the frozen roster. Prefer that immutable snapshot when it is complete. A
   fresh paper deployment can then register its decision identity immediately,
   without waiting for cron or making activation depend on SEC availability at
   that exact moment. The live SEC map remains the fail-closed fallback for an
   older bundle that does not contain a complete snapshot. */
function validateBundledUniverseSnapshot(base) {
  const trade = Array.isArray(base && base.tradeTier) ? base.tradeTier : [];
  const research = Array.isArray(base && base.researchTier) ? base.researchTier : [];
  const snapshot = base && base.identitySnapshot;
  const symbols = trade.map((row) => String(row && row.symbol || ""));
  const duplicates = symbols.filter((symbol, i) => symbol && symbols.indexOf(symbol) !== i);
  const unresolved = [...trade, ...research]
    .filter((row) => !row || !row.symbol || !/^\d+$/.test(String(row.cik || "")))
    .map((row) => row && row.symbol || null);
  const expected = Number(base && base.enforcement && base.enforcement.eligibleCount);
  const provenanceOk = !!(snapshot
    && snapshot.schema === "sec-company-identity-snapshot-v1"
    && snapshot.source
    && /^[a-f0-9]{64}$/.test(String(snapshot.source.responseSha256 || ""))
    && /^[a-f0-9]{64}$/.test(String(snapshot.snapshotSha256 || "")));
  const ok = !!(base && base.immutable === true && base.version
    && trade.length > 0 && expected === trade.length
    && unresolved.length === 0 && duplicates.length === 0 && provenanceOk);
  return { ok, eligible: trade.length, expected,
    unresolved: unresolved.slice(0, 20),
    duplicates: [...new Set(duplicates)].slice(0, 20),
    provenanceOk };
}

async function freezeBundledUniverse() {
  const base = require("./_investorUniverse.js");
  const check = validateBundledUniverseSnapshot(base);
  if (!check.ok) {
    throw new Error(`bundled universe identity is incomplete: ${JSON.stringify(check)}`);
  }
  const frozen = { ...base, immutable: true };
  frozen.contentHash = universeHash(frozen);
  const ref = A.col(A.COL.universe).doc(base.version);
  await A.runTransaction(async (tx) => {
    const cur = await tx.get(ref);
    if (cur.exists) {
      const current = validateFrozenUniverse(cur.data(), base.version);
      if (!current.ok || cur.data().contentHash !== frozen.contentHash) {
        throw new Error(`universe ${base.version} already frozen with different content`);
      }
      return;
    }
    tx.set(ref, { ...frozen, frozenAt: A.FV.serverTimestamp(),
      frozenBy: "bootstrap:bundled-sec-snapshot",
      ...A.envelope({ created_by: "bootstrap.freezeBundledUniverse" }) });
  });
  return { frozen, report: { version: base.version, resolved: check.eligible,
    missing: [], aliases: [], renamed: [], ambiguous: [], dropped: 0,
    sourceId: "bundled.sec-company-identity-snapshot-v1",
    sourceSha256: base.identitySnapshot.source.responseSha256,
    snapshotSha256: base.identitySnapshot.snapshotSha256 } };
}

async function resolveCiksAndFreeze() {
  const base = require("./_investorUniverse.js");
  if (validateBundledUniverseSnapshot(base).ok) {
    return freezeBundledUniverse();
  }
  const report = { version: base.version, resolved: 0, missing: [], mismatched: [],
    aliases: [], renamed: [], ambiguous: [] };

  let index = null;
  try {
    const r = await fetchPublic("https://www.sec.gov/files/company_tickers.json", {
      sourceId: "sec.tickers", accept: ["json"], timeoutMs: 25000,
    });
    if (r.json) {
      index = buildSecTickerMap(r.json);
      report.sourceSha256 = r.sha256 || null;
      report.sourceId = "sec.tickers";
    }
  } catch (e) {
    report.error = `SEC ticker map unavailable: ${String(e.code || e.message).slice(0, 120)}`;
  }
  if (!index) throw new Error(report.error || "SEC ticker map unavailable; refusing degraded universe freeze");

  const apply = (row) => {
    const resolved = resolveSecTicker(index, row.symbol);
    if (resolved && resolved.error) {
      report.ambiguous.push(row.symbol); return row;
    }
    const got = resolved && resolved.cik;
    if (!got) { report.missing.push(row.symbol); return row; }
    if (row.cik && row.cik !== got) report.mismatched.push({ symbol: row.symbol, had: row.cik, sec: got });
    if (resolved.resolutionKind === "punctuation_alias") {
      report.aliases.push({ symbol: row.symbol, secTicker: resolved.secTicker });
    }
    if (resolved.resolutionKind === "renamed_ticker") {
      report.renamed.push({ previousSymbol: row.symbol, symbol: resolved.resolvedSymbol });
    }
    report.resolved += 1;
    return { ...row, symbol: resolved.resolvedSymbol, cik: got,
      ...(resolved.resolutionKind === "renamed_ticker" ? { previousSymbol: row.symbol } : {}),
      company: resolved.company || row.company || null,
      companySource: "sec_company_tickers", cikSource: "sec_company_tickers" };
  };

  /* Resolve all 304 frozen eligible names. A missing current SEC identity is
     never silently dropped because that changes the experimental population
     behind the declared version. The operator must publish a new immutable
     roster version with an auditable replacement instead. */
  const resolvedTrade = (base.tradeTier || []).map(apply).filter((r) => !!r.cik);
  const duplicateSymbols = resolvedTrade.map((r) => r.symbol)
    .filter((symbol, i, rows) => rows.indexOf(symbol) !== i);
  if (duplicateSymbols.length) {
    throw new Error(`SEC rename produced duplicate symbol(s): ${[...new Set(duplicateSymbols)].join(", ")}`);
  }
  const dropped = (base.tradeTier || []).length - resolvedTrade.length;
  if (dropped > 0 || report.missing.length > 0) {
    throw new Error(`SEC entity resolution incomplete: ${report.missing.length} unresolved ticker(s): ${report.missing.slice(0, 20).join(", ")}`);
  }

  const frozen = {
    ...base,
    tradeTier: resolvedTrade,
    researchTier: (base.researchTier || []).map(apply).filter((r) => !!r.cik),
    unresolvedDropped: report.missing,
    droppedCount: dropped,
    cikResolvedAt: new Date().toISOString(),
    immutable: true,
  };
  frozen.contentHash = universeHash(frozen);
  report.dropped = dropped;

  const ref=A.col(A.COL.universe).doc(base.version);
  await A.runTransaction(async(tx)=>{const cur=await tx.get(ref);
    if(cur.exists){const check=validateFrozenUniverse(cur.data(),base.version);
      if(!check.ok||cur.data().contentHash!==frozen.contentHash)throw new Error(`universe ${base.version} already frozen with different content`);
      return;}
    tx.set(ref,{...frozen,frozenAt:A.FV.serverTimestamp(),frozenBy:"bootstrap",
      ...A.envelope({created_by:"bootstrap.resolveCiksAndFreeze"})});});

  return { frozen, report };
}

async function freezeStrategy() {
  const s=strategyDocument(require("./_investorStrategy.js")),contentHash=strategyHash(s),ref=A.col(A.COL.strategies).doc(s.version);
  await A.runTransaction(async(tx)=>{const cur=await tx.get(ref);
    if(cur.exists){if(cur.data().contentHash!==contentHash)throw new Error(`strategy ${s.version} immutable content mismatch`);return;}
    tx.set(ref,{...s,immutable:true,contentHash,frozenAt:A.FV.serverTimestamp(),
      ...A.envelope({created_by:"bootstrap.freezeStrategy"})});});
  return{version:s.version,contentHash};
}

/* ── 4. earnings windows from EDGAR filing cadence ─────────────────────── */
const MS_DAY = 864e5;

function median(xs) {
  if (!xs.length) return null;
  const s = [...xs].sort((a, b) => a - b);
  const m = Math.floor(s.length / 2);
  return s.length % 2 ? s[m] : (s[m - 1] + s[m]) / 2;
}

/** Prefer issuer 8-K Item 2.02 result-announcement cadence. Periodic filing
 * dates are retained only as a broad, explicitly wider fallback. */
function projectEarningsWindow(rec, nowMs = Date.now()) {
  if (!rec) return null;
  const forms = rec.form || [], dates = rec.filingDate || [], items = rec.items || [];
  const periodic = [], resultAnnouncements = [];
  for (let i = 0; i < forms.length; i++) {
    if (forms[i] === "8-K" && /(?:^|[,\s])2\.02(?:[,\s]|$)/.test(String(items[i] || ""))) {
      const t = Date.parse(dates[i]);
      if (isFinite(t)) resultAnnouncements.push(t);
    }
    if (forms[i] === "10-Q" || forms[i] === "10-K") {
      const t = Date.parse(dates[i]);
      if (isFinite(t)) periodic.push(t);
    }
  }
  const useAnnouncements = resultAnnouncements.length >= 4;
  const anchors = useAnnouncements ? resultAnnouncements : periodic;
  if (anchors.length < 4) return null;

  anchors.sort((a, b) => b - a);               // newest first
  const gaps = [];
  for (let i = 0; i + 1 < Math.min(anchors.length, 9); i++) {
    const g = (anchors[i] - anchors[i + 1]) / MS_DAY;
    if (g > 40 && g < 140) gaps.push(g);         // ignore amendments and gaps
  }
  const cadence = median(gaps);
  if (!cadence) return null;

  // Project forward from the most recent periodic filing until we are in the
  // future, then keep the next two so a blackout is never missed at a boundary.
  const out = [];
  let t = anchors[0];
  let guard = 0;
  while (out.length < 3 && guard < 12) {
    t += cadence * MS_DAY;
    if (t > nowMs - 7 * MS_DAY) out.push(new Date(t).toISOString().slice(0, 10));
    guard += 1;
  }
  if (!out.length) return null;

  return {
    dates: out,
    estimated: true,
    cadenceDays: Math.round(cadence),
    basisKind: useAnnouncements ? "issuer_8k_item_2_02" : "periodic_filing_fallback",
    uncertaintyDays: useAnnouncements ? 7 : 14,
    basis: useAnnouncements
      ? `projected from ${gaps.length + 1} issuer 8-K Item 2.02 results announcements, median cadence ${Math.round(cadence)}d`
      : `broad fallback projected from ${gaps.length + 1} 10-Q/10-K filing dates, median cadence ${Math.round(cadence)}d`,
    lastAnchorDate: new Date(anchors[0]).toISOString().slice(0, 10),
    derivedAt: new Date(nowMs).toISOString(),
  };
}

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
  const projected = projectEarningsWindow(r.json.filings.recent);
  return projected ? { ...projected, source: "sec.submissions",
    sourceSha256: r.sha256 || null } : null;
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
    method: "issuer 8-K Item 2.02 cadence; broad 10-Q/10-K fallback",
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
  const out = { attemptedAt: new Date().toISOString(), setBy: "bootstrap",
    vixHealthy:false, corHealthy:false };

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
      out.vixSourceSha256 = r.sha256 || null;
      out.vixFetchedAt = new Date().toISOString(); out.vixHealthy = true;
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
        out.corSourceSha256 = r.sha256 || null;
        out.corAsOf = new Date(rows[rows.length - 1].t).toISOString().slice(0, 10);
        out.corFetchedAt = new Date().toISOString(); out.corHealthy = true;
      }
    } catch (e) {
      out.corError = `${sym}: ${String(e.code || e.message).slice(0, 80)}`;
    }
  }

  if (out.vixHealthy || out.corHealthy) out.asOf = new Date().toISOString();
  await A.col(A.COL.control).doc("regime").set(out, { merge: true });
  return out;
}

/* ── the entry point the cycle calls ───────────────────────────────────── */
/**
 * Idempotent. Expensive work runs once; after that this is a single read.
 * Regime and earnings refresh on their own slower clocks.
 */
/* ── 6. DAILY HISTORY BACKFILL ─────────────────────────────────────────────
 * Pull up to five trading years of daily bars for every roster name and
 * required economic proxy so the temporal layer has repeated annual cycles
 * and the original six-month context from its first healthy cycle.
 *
 * Resumable by design. A Netlify background function gets 15 minutes; 342
 * symbols of daily bars fits comfortably inside that, but a provider hiccup
 * must not mean starting over. A cursor in control/history records how far it
 * got, and the next cycle picks up from there. Symbols already holding enough
 * days are skipped, which is what makes the daily top-up cheap.
 */
function expectedHistorySymbols(universe) {
  const tier = (universe && universe.tradeTier) || [];
  return [...new Set([...tier.map((t) => t.symbol).filter(Boolean),
    ...Object.values(T.DRIVER_BY_SECTOR), ...T.DRIVER_KEYWORDS.map((x) => x.symbol)])];
}

function historyCursorNeedsReconcile(expectedSymbols, history = {}) {
  const expected = expectedSymbols || [];
  const completed = Array.isArray(history.completed) ? history.completed : [];
  const targets = Array.isArray(history.targetSymbols) ? history.targetSymbols : [];
  const completedSet = new Set(completed);
  return completed.length !== expected.length
    || expected.some((symbol) => !completedSet.has(symbol))
    || targets.length !== expected.length
    || targets.some((symbol, index) => symbol !== expected[index]);
}

async function backfillDailyHistory(universe, { budgetMs = 240000, maxSymbols = 400 } = {}) {
  const started = Date.now();
  const symbols = expectedHistorySymbols(universe);

  const curRef = A.col(A.COL.control).doc("history");
  const curSnap = await curRef.get();
  const cur = curSnap.exists ? curSnap.data() : {};
  /* v8.1 binds volume to its historical feed. A completion cursor from the
     older schema cannot prove that identity, so one resumable full refresh is
     required instead of relabelling old volume during a seven-day top-up. */
  const doneSet = new Set(cur.dailyProvenanceVersion === DAILY_PROVENANCE_VERSION
    ? (cur.completed || []) : []);

  const todo = symbols.filter((s) => !doneSet.has(s)).slice(0, maxSymbols);
  if (!todo.length) {
    const completed = symbols.filter((symbol) => doneSet.has(symbol));
    const priorTargets = Array.isArray(cur.targetSymbols) ? cur.targetSymbols : [];
    const cursorChanged = completed.length !== doneSet.size
      || priorTargets.length !== symbols.length
      || priorTargets.some((symbol, i) => symbol !== symbols[i]);
    if (cursorChanged) await curRef.set({
      completed, completedCount: completed.length, rosterCount: symbols.length,
      targetSymbols: symbols, dailyProvenanceVersion: DAILY_PROVENANCE_VERSION,
      lastRunAt: A.FV.serverTimestamp(),
      ...A.envelope({ created_by: "investorBootstrap" }),
    }, { merge: true });
    return { complete: true, symbols: symbols.length, done: completed.length,
      fetched: 0, reconciled: cursorChanged,
      note: "daily history already present for every current roster/proxy target" };
  }

  let fetched = 0, failed = 0, days = 0;
  const chunk = 90;
  for (let i = 0; i < todo.length; i += chunk) {
    if (Date.now() - started > budgetMs) break;      // leave room for the rest of the run
    const part = todo.slice(i, i + chunk);
    let got = { barsBySymbol: {}, provenanceBySymbol: {} };
    try { got = await H.fetchDailyWithMeta(part, { days: H.KEEP_DAYS }); }
    catch (e) { failed += part.length; continue; }

    for (const sym of part) {
      const bars = got.barsBySymbol[sym] || [];
      if (!bars.length) { failed += 1; continue; }
      try {
        const provenance = got.provenanceBySymbol[sym] || {};
        const fetchStatus = got.statusBySymbol && got.statusBySymbol[sym] || {};
        const n = await H.writeDaily(sym, bars, {
          source: "bootstrap.backfill",
          provider: provenance.provider || null,
          feed: provenance.feed || null,
          adjustment: provenance.adjustment || null,
          sourceSha256: provenance.sourceSha256 || null,
          feedVolumeShare: provenance.feedVolumeShare ?? null,
          marketFetchedAt: provenance.fetchedAt || null,
          lastBackfillAttemptAtMs: Date.now(),
          backfillComplete: fetchStatus.complete === true,
          backfillTruncated: fetchStatus.truncated === true,
          backfillRequestedDays: H.KEEP_DAYS,
          lastBackfillError: fetchStatus.error || null,
        });
        fetched += 1; days += n;
        /* Completion describes the requested provider window, not issuer age.
           A recent IPO cannot acquire three years of history by refetching its
           lifetime every cycle; each feature still enforces its own minimum. */
        if (fetchStatus.complete === true) doneSet.add(sym);
        else failed += 1;
      } catch (e) { failed += 1; }
    }
  }

  const completed = symbols.filter((symbol) => doneSet.has(symbol));
  await curRef.set({
    completed,
    completedCount: completed.length,
    rosterCount: symbols.length,
    targetSymbols: symbols,
    dailyProvenanceVersion: DAILY_PROVENANCE_VERSION,
    lastRunAt: A.FV.serverTimestamp(),
    ...A.envelope({ created_by: "investorBootstrap" }),
  }, { merge: true });

  return {
    complete: completed.length === symbols.length,
    symbols: symbols.length, done: completed.length,
    fetched, failed, avgDays: fetched ? Math.round(days / fetched) : 0,
  };
}

/* Append the most recent sessions to every symbol already backfilled. Cheap:
   a 5-day pull per chunk, merged by date, so re-running is harmless. */
async function topUpDailyHistory(universe, { budgetMs = 90000 } = {}) {
  const started = Date.now();
  const symbols = expectedHistorySymbols(universe);
  let updated = 0;
  const chunk = 90;
  for (let i = 0; i < symbols.length; i += chunk) {
    if (Date.now() - started > budgetMs) break;
    const part = symbols.slice(i, i + chunk);
    try {
      const got = await H.fetchDailyWithMeta(part, { days: 7 });
      for (const sym of part) {
        const bars = got.barsBySymbol[sym] || [];
        if (bars.length) {
          const provenance = got.provenanceBySymbol[sym] || {};
          await H.writeDaily(sym, bars, {
            source: "cycle.topup",
            provider: provenance.provider || null,
            feed: provenance.feed || null,
            adjustment: provenance.adjustment || null,
            sourceSha256: provenance.sourceSha256 || null,
            feedVolumeShare: provenance.feedVolumeShare ?? null,
            marketFetchedAt: provenance.fetchedAt || null,
          });
          updated += 1;
        }
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

/** PURE. May the safety epoch's commit be rolled to the running build?
 *  Only when every identity the epoch attests is byte-identical to the stored
 *  and the code-side frozen identity, and the account was already authorised
 *  for automatic exploratory paper trading. Anything else needs a real
 *  re-bootstrap. Exported so a runtime fixture can attest it. */
function epochRolloverEligible(control, { commit, codeStrategy, variantsHash }) {
  const c = control || {}, epoch = c.safetyEpoch || null;
  const fail = (reason) => ({ eligible: false, reason });
  if (!epoch || !epoch.commit) return fail("no safety epoch");
  if (epoch.commit === commit) return fail("epoch already on this commit");
  const operating = STATE.describe(c);
  if (c.autoExploratoryAuthorized !== true) return fail("exploratory auto not authorised");
  if (operating.exploratoryAuto !== true) return fail(`operating state is ${operating.state}`);
  if (epoch.accountId !== (c.accountId || "paper-1")) return fail("accountId");
  if (!c.strategyVersion || epoch.strategyVersion !== c.strategyVersion) return fail("strategyVersion");
  if (!codeStrategy || codeStrategy.version !== c.strategyVersion) return fail("code strategy version");
  if (!c.strategyHash || epoch.strategyHash !== c.strategyHash) return fail("strategyHash");
  if (strategyHash(codeStrategy) !== c.strategyHash) return fail("code strategy hash");
  if (!c.universeVersion || epoch.universeVersion !== c.universeVersion) return fail("universeVersion");
  if (!c.universeHash || epoch.universeHash !== c.universeHash) return fail("universeHash");
  if (!c.variantsHash || epoch.variantsHash !== c.variantsHash) return fail("variantsHash");
  if (variantsHash !== c.variantsHash) return fail("code variants hash");
  return { eligible: true, reason: "identity unchanged" };
}

async function ensureBootstrapped({ force = false, enrich = true } = {}) {
  const ref = A.col(A.COL.control).doc("control");
  const snap = await ref.get();
  const c = snap.exists ? snap.data() : {};
  const done = c.bootstrapVersion === BOOTSTRAP_VERSION;

  /* Attest pure invariants on the exact running commit. A stored `true` from
     an older deploy is not evidence about this one. */
  const commit = process.env.COMMIT_REF || process.env.DEPLOY_ID || "local";
  let fixtures;
  try { fixtures = require("./_investorSelftest").runFixtures(); }
  catch (e) { fixtures = { pass: false, fixtureHash: null, error: String(e.message).slice(0, 200) }; }
  await ref.set({ fixturesPass: fixtures.pass === true, fixturesCommit: commit,
    fixtureHash: fixtures.fixtureHash || null, fixturesCheckedAt: A.FV.serverTimestamp(),
    fixtureFailures: (fixtures.cases || []).filter((x) => !x.pass).slice(0, 10) }, { merge: true });
  if (!fixtures.pass) {
    await ref.set({ ...STATE.legacyPatch(STATE.STATES.ENTRY_FROZEN, c),
      safetyClosedReason: "runtime fixture attestation failed" }, { merge: true });
  }

  /* DEPLOY ROLLOVER. The safety epoch binds every entry to the commit that
     was attested when the operator (or the bootstrap) activated it. A deploy
     that does not bump BOOTSTRAP_VERSION used to leave that binding pointing
     at the previous commit, so controlAllowsEntry refused every order with
     "safety epoch commit mismatch" and nothing on the desk said why — the
     console showed "working now" while no order could ever be written.

     The rollover re-stamps ONLY the commit, and only when everything the
     epoch actually attests is unchanged: fixtures pass on this build, the
     frozen strategy/universe/variants identities are byte-identical to what
     the epoch names, and the account was already authorised for automatic
     exploratory paper trading. Any identity change still requires a real
     re-bootstrap (a BOOTSTRAP_VERSION bump), exactly as before. The event is
     audited so the evidence record shows which build each entry ran on. */
  let epochRollover = null;
  try {
    const epoch = c.safetyEpoch || null;
    if (done && !force && fixtures.pass && epoch && epoch.commit && epoch.commit !== commit) {
      const operatingNow = STATE.describe(c);
      const verdict = epochRolloverEligible(c, { commit,
        codeStrategy: strategyDocument(S_FALLBACK), variantsHash: V.variantsHash() });
      if (verdict.eligible) {
        const rolled = { ...epoch, commit, previousCommit: epoch.commit,
          rolledOverAtMs: Date.now(), rolledOverBy: "bootstrap:deploy_rollover" };
        await ref.set({ safetyEpoch: rolled }, { merge: true });
        await A.col(A.COL.audit).add({ action: "safety_epoch_deploy_rollover",
          previousCommit: epoch.commit, commit, fixtureHash: fixtures.fixtureHash || null,
          operatingState: operatingNow.state, atMs: Date.now(),
          ...A.envelope({ created_by: "investorBootstrap" }) });
        c.safetyEpoch = rolled;
        epochRollover = { rolled: true, previousCommit: epoch.commit, commit };
      } else {
        epochRollover = { rolled: false, previousCommit: epoch.commit, commit,
          reason: `frozen identity or authorisation changed (${verdict.reason}) — a full re-bootstrap is required` };
      }
    }
  } catch (e) {
    epochRollover = { rolled: false, error: String(e.message).slice(0, 160) };
  }

  const steps = [];
  if (epochRollover) steps.push({ step: "safetyEpochRollover", ok: epochRollover.rolled === true, ...epochRollover });

  /* MIGRATION. Proposals written before expiries existed are ageless and,
     under manual approval, were approvable forever. On every bootstrap pass
     any proposed order without an expiry is rejected (nothing is reserved
     for a proposal, so nothing is released) and the event is audited. */
  try {
    const accountForMigration = c.accountId || "paper-1";
    const legacy = await A.col(A.COL.orders).where("accountId", "==", accountForMigration)
      .where("status", "==", "proposed").get();
    let rejected = 0;
    for (const d of legacy.docs) {
      const o = d.data();
      if (Number.isFinite(Number(o.expiresAtMs))) continue;
      try {
        const r = await L.rejectOrder(o.orderId, "legacy proposal without an expiry rejected at bootstrap", "bootstrap:migration");
        if (!r.noop) rejected += 1;
      } catch { /* one stuck row must not block the bootstrap */ }
    }
    if (rejected) {
      steps.push({ step: "legacyProposalMigration", ok: true, rejected });
      await A.col(A.COL.audit).add({ action: "legacy_proposals_rejected", rejected, commit,
        atMs: Date.now(), ...A.envelope({ created_by: "investorBootstrap" }) });
    }
  } catch (e) {
    steps.push({ step: "legacyProposalMigration", ok: false, error: String(e.message).slice(0, 120) });
  }
  let universe = null;

  if (!done || force) {
    // 1 + 2
    try {
      const { frozen, report } = await resolveCiksAndFreeze();
      universe = frozen;
      steps.push({ step: "universe", ok: true, ...report });
    } catch (e) { steps.push({ step: "universe", ok: false, error: String(e.message).slice(0, 160) }); }

    let frozenStrategy=null;
    try { frozenStrategy=await freezeStrategy(); steps.push({step:"strategy",ok:true,...frozenStrategy}); }
    catch(e){steps.push({step:"strategy",ok:false,error:String(e.message).slice(0,160)});}

    // 3
    try {
      const accountId = c.accountId || "paper-1";
      const r = await L.openAccount({
        accountId,
        startingNavUsd: Number(c.startingNavUsd) || 100000,
        strategyVersion: S_FALLBACK.version,
      });
      steps.push({ step: "account", ok: true, accountId, existing: !!r.existing });
    } catch (e) { steps.push({ step: "account", ok: false, error: String(e.message).slice(0, 160) }); }

    try {
      const accountId = c.accountId || "paper-1";
      const reconciliation = await L.reconcileAccount(accountId, {
        context: "bootstrap_exploratory_auto",
      });
      steps.push({ step: "ledger", ok: reconciliation.pass === true,
        reconciliation: { pass: reconciliation.pass === true,
          discrepancyCount: (reconciliation.lifecycle && reconciliation.lifecycle.violations || []).length
            + (reconciliation.journal && reconciliation.journal.discrepancies || []).length
            + (reconciliation.markViolations || []).length } });
    } catch (e) {
      steps.push({ step: "ledger", ok: false, error: String(e.message).slice(0, 160) });
    }

    /* Commit the critical, executable identity BEFORE long best-effort data
       enrichment. SEC history, earnings projections and multi-year bars can
       take minutes; none of those jobs should leave a reconciled paper account
       stuck in observation merely because a background worker reaches its
       platform time limit. Missing per-company inputs still fail or size down
       at the actual decision gate. */
    if (!fixtures.pass) {
      const failed = (fixtures.cases || []).filter((x) => !x.pass).map((x) => x.name);
      steps.push({ step: "fixtures", ok: false,
        error: fixtures.error
          || (failed.length ? `runtime invariant failed: ${failed.slice(0, 6).join(", ")}` : "runtime invariant failed"),
        failed, fixtureHash: fixtures.fixtureHash || null });
    } else {
      steps.push({ step: "fixtures", ok: true, passed: fixtures.passed, total: fixtures.total,
        fixtureHash: fixtures.fixtureHash });
    }
    const criticalFailed = steps.filter((x) =>
      !x.ok && ["universe", "strategy", "account", "ledger"].includes(x.step));
    if (!fixtures.pass) criticalFailed.push({ step: "fixtures", ok: false });
    if (criticalFailed.length) {
      await ref.set({
        bootstrapAttemptedAt: A.FV.serverTimestamp(),
        bootstrapReport: steps,
        bootstrapIncomplete: criticalFailed.map((x) => x.step),
      }, { merge: true });
      return { bootstrapped: false, retryNext: true, steps,
        note: `bootstrap incomplete (${criticalFailed.map((x) => x.step).join(", ")} failed) — will retry next cycle rather than freezing a degraded state` };
    }

    const accountId = c.accountId || "paper-1";
    const universeVersion = (universe && universe.version)
      || c.universeVersion || U_FALLBACK.version || "v1";
    const strategyVersion = S_FALLBACK.version;
    const universeHashValue = universe && universe.contentHash;
    const strategyHashValue = frozenStrategy && frozenStrategy.contentHash;
    const variantsHash = V.variantsHash();
    const exploratory = S_FALLBACK.exploratoryAuto || {};
    const autoAuthorized = exploratory.enabled === true
      && exploratory.autoStartAfterSuccessfulBootstrap === true;
    const priorOperating = STATE.describe(c);
    const priorFreeze = priorOperating.entriesFrozen || !!c.reconciliationFailure
      || !!(c.ledgerReconciliation && c.ledgerReconciliation.pass === false);
    const mayStart = autoAuthorized && !priorOperating.paused && !priorFreeze;
    let bootstrapOperating;
    if (priorOperating.paused) {
      bootstrapOperating = { ...STATE.legacyPatch(STATE.STATES.PAUSED, c),
        resumeOperatingState: autoAuthorized
          ? STATE.STATES.EXPLORATORY_AUTO : STATE.STATES.OBSERVATION };
    } else if (priorFreeze) {
      bootstrapOperating = { ...STATE.legacyPatch(STATE.STATES.ENTRY_FROZEN, c),
        resumeOperatingState: autoAuthorized
          ? STATE.STATES.EXPLORATORY_AUTO : STATE.STATES.OBSERVATION };
    } else {
      bootstrapOperating = STATE.legacyPatch(mayStart
        ? STATE.STATES.EXPLORATORY_AUTO : STATE.STATES.OBSERVATION, c);
    }
    const safetyEpoch = mayStart ? {
      accountId, strategyVersion, universeVersion,
      universeHash: universeHashValue, strategyHash: strategyHashValue,
      variantsHash, commit, activatedAtMs: Date.now(),
      activatedBy: "bootstrap:exploratory_auto",
    } : null;
    steps.push({ step: "exploratoryAuto", ok: true,
      enabled: autoAuthorized, started: mayStart,
      state: bootstrapOperating.operatingState,
      note: mayStart
        ? "automatic exploratory paper trading activated on the frozen build identity"
        : (priorOperating.paused || priorFreeze
          ? "authorization retained but a prior pause/freeze remains authoritative"
          : "exploratory auto is not enabled by the frozen strategy") });
    await ref.set({
      bootstrapVersion: BOOTSTRAP_VERSION,
      bootstrapIncomplete: null,
      bootstrappedAt: A.FV.serverTimestamp(),
      bootstrapReport: steps,
      accountId,
      universeVersion,
      strategyVersion,
      universeHash: universeHashValue,
      strategyHash: strategyHashValue,
      variantsHash,
      safetyEpoch,
      autoExploratoryAuthorized: autoAuthorized,
      exploratoryPolicyVersion: exploratory.version || null,
      paperLearning: autoAuthorized
        ? { ...(exploratory.paperLearningDefaults || {}) }
        : (c.paperLearning || null),
      operatorCeiling: autoAuthorized ? "approval"
        : (c.operatorCeiling || S_FALLBACK.operatorCeiling || "approval"),
      ...bootstrapOperating,
    }, { merge: true });

    if (!enrich) {
      return { bootstrapped: true, enrichmentDeferred: true, steps,
        fixtures: { pass: fixtures.pass, fixtureHash: fixtures.fixtureHash, commit } };
    }

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

    await ref.set({ bootstrapReport: steps,
      enrichmentUpdatedAt: A.FV.serverTimestamp() }, { merge: true });
    return { bootstrapped: true, steps, fixtures: { pass: fixtures.pass,
      fixtureHash: fixtures.fixtureHash, commit } };
  }

  /* Price cycles use this fast path. They verify the immutable identity but
     never wait behind 304-company enrichment; the evidence worker owns those
     resumable, slower jobs. */
  if (!enrich) {
    const [uFast, sFast] = await Promise.all([
      A.col(A.COL.universe).doc(c.universeVersion || U_FALLBACK.version).get(),
      A.col(A.COL.strategies).doc(c.strategyVersion || S_FALLBACK.version).get(),
    ]);
    if (!uFast.exists || !sFast.exists) throw new Error("frozen policy identity missing after bootstrap");
    const uvFast = validateFrozenUniverse(uFast.data(), c.universeVersion || U_FALLBACK.version);
    const shFast = strategyHash(sFast.data());
    if (!uvFast.ok || shFast !== sFast.data().contentHash) {
      throw new Error("frozen policy content hash mismatch");
    }
    await ref.set({ universeHash: uvFast.actual, strategyHash: shFast,
      variantsHash: V.variantsHash() }, { merge: true });
    return { bootstrapped: true, already: true, enrichmentDeferred: true, steps,
      fixtures: { pass: fixtures.pass, fixtureHash: fixtures.fixtureHash, commit } };
  }

  /* Evidence workers keep the slow-moving inputs fresh. */
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
    const expected = expectedHistorySymbols(u);
    if (h.dailyProvenanceVersion !== DAILY_PROVENANCE_VERSION
        || historyCursorNeedsReconcile(expected, h)) {
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

  const [uFinal, sFinal] = await Promise.all([
    A.col(A.COL.universe).doc(c.universeVersion || U_FALLBACK.version).get(),
    A.col(A.COL.strategies).doc(c.strategyVersion || S_FALLBACK.version).get(),
  ]);
  if (!uFinal.exists || !sFinal.exists) throw new Error("frozen policy identity missing after bootstrap");
  const uv = validateFrozenUniverse(uFinal.data(), c.universeVersion || U_FALLBACK.version);
  const sh = strategyHash(sFinal.data());
  if (!uv.ok || sh !== sFinal.data().contentHash) throw new Error("frozen policy content hash mismatch");
  await ref.set({ universeHash: uv.actual, strategyHash: sh, variantsHash: V.variantsHash() }, { merge: true });
  return { bootstrapped: true, already: true, steps,
    fixtures: { pass: fixtures.pass, fixtureHash: fixtures.fixtureHash, commit } };
}

module.exports = {
  epochRolloverEligible,
  expectedHistorySymbols, historyCursorNeedsReconcile, backfillDailyHistory, topUpDailyHistory,
  backfillSharesOutstanding, readShares,
  BOOTSTRAP_VERSION, DAILY_PROVENANCE_VERSION,
  ensureBootstrapped, resolveCiksAndFreeze, freezeBundledUniverse,
  validateBundledUniverseSnapshot, freezeStrategy,
  SEC_RENAMED_TICKERS, canonicalSecTicker, buildSecTickerMap, resolveSecTicker,
  universeHash, strategyHash, strategyDocument, validateFrozenUniverse,
  projectEarningsWindow, deriveEarningsWindow, populateEarnings, readEarnings,
  refreshRegime, parseCboeCsv, median,
};
