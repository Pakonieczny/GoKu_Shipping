/*  netlify/functions/_investorRisk.js  (v1.0)
 *  ---------------------------------------------------------------------------
 *  Investor_AI — the portfolio-level risk layer.
 *
 *  WHY THIS FILE EXISTS
 *  ------------------------------------------------------------------------
 *  _investorStrategy.js has always declared a `portfolioControls` block: gross
 *  exposure caps, a minimum cash floor, per-sector limits, a correlated-cluster
 *  cap, a one-day loss pause and a drawdown freeze. An audit found that every
 *  one of those numbers was read by nothing whatsoever. The file's own comment
 *  called the cluster cap "the primary defence"; only the early warning had
 *  been built, and it was calibrated so that it could effectively never fire.
 *
 *  The distinction that was missing, and that this file exists to enforce:
 *
 *    · The SIGNAL gates in _investorSignal.js ask "is this one name a good
 *      idea?" They look at the cross-section of today's candidates.
 *    · These checks ask "given what we already hold, should we hold more?"
 *      They look at the BOOK.
 *
 *  A system with only the first kind will happily buy nine semiconductors on
 *  the morning the whole sector gaps down, because each one looks individually
 *  oversold. Nine correlated positions are one bet with three times the size,
 *  not nine independent ones, and that is how a book with a positive edge still
 *  ends up with a catastrophic day.
 *
 *  EVERY CHECK HERE IS A HARD REFUSAL, NOT A SIZE ADJUSTMENT.
 *  A cap that merely shrinks a position still lets the count grow without
 *  limit. Each of these returns a reason, and every refusal is recorded so the
 *  no-trade log shows portfolio reasons beside signal reasons.
 * ---------------------------------------------------------------------------
 */

"use strict";

/* ── economic clusters ─────────────────────────────────────────────────────
 * The cap has to bind on shared economic exposure, not on the sector label.
 * Two different Apple suppliers in "hardware" and "semis" are one bet on Apple.
 * A semiconductor-equipment maker and a foundry customer rise and fall on the
 * same capex cycle whatever their GICS bucket says.
 *
 * This is a deliberately coarse, hand-drawn map — it does not need to be right
 * about everything, only right about the big obvious co-movements that would
 * otherwise be counted as diversification.
 */
const CLUSTERS = {
  semicap:      ["AMAT","LRCX","KLAC","TER","ACLS","UCTT","ICHR","ONTO","ENTG","MKSI","AEIS","COHU"],
  ai_compute:   ["NVDA","AMD","AVGO","MRVL","MU","SMCI","DELL","ANET","VRT","CRDO","ALAB","WDC","STX","COHR","LITE"],
  megacap_ads:  ["GOOGL","META","PINS","SNAP","TTD","APP","RDDT"],
  cloud_infra:  ["MSFT","AMZN","ORCL","SNOW","MDB","DDOG","NET","CFLT"],
  cyber:        ["PANW","CRWD","ZS","OKTA","FTNT","S","GEN","TENB","QLYS","RPD","VRNS","CYBR"],
  fintech_rate: ["SOFI","HOOD","COIN","PYPL","SQ","AFRM","UPST","LC"],
  big_banks:    ["JPM","BAC","WFC","C","GS","MS","STT","NTRS","SCHW"],
  alt_managers: ["BX","KKR","APO","ARES","BLK","TROW","BEN","IVZ"],
  exchanges:    ["ICE","CME","NDAQ","SPGI","MCO","MSCI"],
  managed_care: ["UNH","ELV","CI","CVS","HUM","CNC"],
  drug_dist:    ["MCK","COR","CAH"],
  med_device:   ["ABT","BSX","SYK","MDT","ISRG","EW","DXCM","PODD","ZBH","BDX","BAX","RMD","ALGN"],
  life_sci:     ["TMO","DHR","A","WAT","MTD","IQV","CRL"],
  big_pharma:   ["LLY","MRK","PFE","ABBV","BMY","AMGN","GILD","JNJ"],
  smid_biotech: ["INCY","NBIX","ALNY","BMRN","EXEL","UTHR","JAZZ","HALO","SRPT","IONS","RARE","FOLD","ITCI","AXSM","CORT","MRNA","VRTX","REGN","BIIB"],
  defense:      ["LMT","NOC","GD","LHX","HII","RTX","TXT","TDG","HEI","CW","LDOS","BAH","CACI","SAIC"],
  eandc:        ["PSN","KBR","J","ACM","EME","PWR","MAS","BLDR","URI"],
  shale_ep:     ["OXY","DVN","FANG","EOG","HES","APA","CHRD","MTDR","AR","RRC","CTRA","COP"],
  oil_services: ["SLB","HAL","BKR","NOV","FTI"],
  midstream:    ["OKE","WMB","KMI","TRGP","LNG"],
  ipp_power:    ["CEG","VST","NRG","TLN","AES"],
  reg_utility:  ["NEE","DUK","SO","D","AEP","EXC","XEL","ED","WEC","ES","PEG","SRE","PCG","ETR","FE","CNP","CMS"],
  big_box:      ["WMT","COST","TGT","HD","LOW","BJ"],
  discount:     ["DG","DLTR","KR","ROST","TJX","BURL"],
  specialty_ret:["ULTA","DKS","BBY","AZO","ORLY","AAP","TSCO","CHWY","W","ETSY","EBAY","SHOP"],
  footwear:     ["NKE","LULU","DECK","CROX","SKX","RL","PVH","VFC"],
  travel:       ["BKNG","EXPE","ABNB","UBER","LYFT","DASH"],
  streaming:    ["NFLX","DIS","SPOT","RBLX","TTWO","EA"],
  base_metals:  ["FCX","NUE","STLD","CLF","AA","MP"],
  chemicals:    ["LIN","APD","SHW","ECL","DD","DOW","LYB","PPG","ALB","CE","EMN"],
  ag_inputs:    ["CF","MOS","NTR"],
  packaging:    ["IP","PKG","WRK"],
  machinery:    ["CAT","DE","HON","GE","ETN","EMR","ROK","PH","DOV","IEX","XYL","AME","FAST","GWW"],
};

const CLUSTER_OF = (() => {
  const m = {};
  for (const [id, syms] of Object.entries(CLUSTERS)) for (const s of syms) m[s] = id;
  return m;
})();

/** Cluster for a symbol; falls back to its sector so an unmapped name is never
 *  treated as uncorrelated with everything. Unknown must not mean "safe". */
function clusterOf(symbol, sector) {
  return CLUSTER_OF[String(symbol || "").toUpperCase()] || (sector ? `sector:${sector}` : "unmapped");
}

/* ── the book, summarised ──────────────────────────────────────────────── */
/**
 * Build a picture of what is currently held, marked to market where a price is
 * available. Marking matters: the previous code reported positions at cost
 * basis only, which meant an open loss was invisible to every number on the
 * dashboard until the position closed.
 */
function summarise(positions, marks, sectorOf, navUsd) {
  const open = (positions || []).filter((p) => p && p.open && (p.qty || 0) > 0);
  const bySector = {}, byCluster = {};
  let grossUsd = 0, costUsd = 0, unrealisedUsd = 0, marked = 0, unmarked = 0;

  const rows = open.map((p) => {
    const sym = p.symbol;
    const mark = Number(marks && marks[sym]);
    /* FIELD-NAME TRAP. The ledger writes `entryPriceUsd` on a position; this
       read `avgPrice`, which does not exist, so entry was NaN, cost was 0, and
       a real open position was reported as 0% of the account. The same
       mismatch in the exit path meant `havePrice` was false and THE STOP LOSS
       COULD NEVER FIRE on a real position. Caught only by running the thing
       end to end — every static read of both files looked correct. */
    const entry = Number(p.entryPriceUsd != null ? p.entryPriceUsd : p.avgPrice);
    const qty = Number(p.qty) || 0;
    const cost = entry > 0 ? entry * qty : 0;
    const haveMark = Number.isFinite(mark) && mark > 0;
    const value = haveMark ? mark * qty : cost;      // fall back to cost, and count it
    if (haveMark) marked += 1; else unmarked += 1;

    grossUsd += value; costUsd += cost;
    if (haveMark && entry > 0) unrealisedUsd += (mark - entry) * qty;

    const sec = sectorOf ? sectorOf(sym) : (p.sector || "other");
    const cl = clusterOf(sym, sec);
    bySector[sec] = (bySector[sec] || 0) + value;
    byCluster[cl] = (byCluster[cl] || 0) + value;

    return {
      symbol: sym, qty, entry, mark: haveMark ? mark : null, valueUsd: value,
      sector: sec, cluster: cl,
      pnlPct: haveMark && entry > 0 ? Number((((mark - entry) / entry) * 100).toFixed(2)) : null,
      marked: haveMark,
    };
  });

  const nav = Number(navUsd) || 0;
  const pct = (v) => (nav > 0 ? (v / nav) * 100 : 0);
  return {
    count: rows.length, rows,
    grossUsd, costUsd, unrealisedUsd,
    grossPct: Number(pct(grossUsd).toFixed(2)),
    unrealisedPct: Number(pct(unrealisedUsd).toFixed(2)),
    bySectorPct: Object.fromEntries(Object.entries(bySector).map(([k, v]) => [k, Number(pct(v).toFixed(2))])),
    byClusterPct: Object.fromEntries(Object.entries(byCluster).map(([k, v]) => [k, Number(pct(v).toFixed(2))])),
    marked, unmarked,
    markCoveragePct: rows.length ? Number(((marked / rows.length) * 100).toFixed(1)) : 100,
  };
}

/** Revalue the account from executable marks. Cost basis is accounting state,
 * not risk NAV; using it hides open losses from breakers and sizing. */
function markedBook(positions, marks, sectorOf, balancesUsd = {}) {
  const open = (positions || []).filter((p) => p && p.open && Number(p.qty) > 0);
  let marketValueUsd = 0, untrusted = 0;
  for (const p of open) {
    const mark = Number(marks && marks[p.symbol]);
    if (mark > 0) marketValueUsd += mark * Number(p.qty);
    else {
      untrusted += 1;
      marketValueUsd += Number(p.entryPriceUsd || p.avgPrice || 0) * Number(p.qty);
    }
  }
  const cashUsd = Number(balancesUsd.cash) || 0;
  const reservedUsd = Number(balancesUsd.reserved) || 0;
  const navUsd = cashUsd + reservedUsd + marketValueUsd;
  return { navUsd, cashUsd, reservedUsd, marketValueUsd, untrustedMarks: untrusted,
    book: summarise(open, marks, sectorOf, navUsd) };
}

function positionSizeUsd({ navUsd, atrPct, expectedShortfall5dPct, overnightGapEsPct,
                           signalScaler = 1, cfg }) {
  const pc = (cfg && cfg.portfolioControls) || {};
  const p = (cfg && cfg.parameters) || cfg || {};
  const riskBudgetPct = Math.max(0, Number(pc.riskBudgetPerTradePctOfNav) || 0.25);
  const ordinaryPct = Math.max(0, Number(pc.ordinaryPositionPctOfNav) || 3);
  const distances = [Math.abs(Number(p.stopLossPct) || 8),
    2 * Math.max(0, Number(atrPct) || 0),
    Math.max(0, Number(expectedShortfall5dPct) || 0),
    Math.max(0, Number(overnightGapEsPct) || 0), 0.5];
  const riskDistancePct = Math.max(...distances);
  const riskBudgetUsd = Math.max(0, Number(navUsd) || 0) * riskBudgetPct / 100;
  const riskLimitedUsd = riskDistancePct > 0 ? riskBudgetUsd / (riskDistancePct / 100) : 0;
  const ordinaryCapUsd = Math.max(0, Number(navUsd) || 0) * ordinaryPct / 100;
  const scaler = Math.max(0, Math.min(1, Number(signalScaler) || 0));
  const usd = Math.max(0, Math.min(riskLimitedUsd, ordinaryCapUsd) * scaler);
  return { usd: Number(usd.toFixed(2)), scaler, riskDistancePct,
    riskBudgetUsd: Number(riskBudgetUsd.toFixed(2)), ordinaryCapUsd: Number(ordinaryCapUsd.toFixed(2)),
    maxPlannedLossUsd: Number((usd * riskDistancePct / 100).toFixed(2)),
    tailInputs: { atrPct: Number(atrPct) || null,
      expectedShortfall5dPct: Number(expectedShortfall5dPct) || null,
      overnightGapEsPct: Number(overnightGapEsPct) || null } };
}

/* ── the account-level circuit breakers ────────────────────────────────── */
/**
 * These stop the whole book, not one trade. Checked once per cycle before any
 * candidate is considered, because if the account is in trouble the correct
 * number of new positions is zero regardless of how good the next idea looks.
 */
function accountBreakers(state, cfg) {
  const pc = cfg.portfolioControls || {};
  const out = [];

  if (Number(state.untrustedOpenMarks) > 0) {
    out.push({ id: "untrusted_open_marks", halt: true,
      reason: `${state.untrustedOpenMarks} open position(s) lack a validated current mark` });
  }

  const ddLimit = -Math.abs(pc.drawdownFreezePctFromHigh ?? 6);
  if (Number.isFinite(state.drawdownPct) && state.drawdownPct <= ddLimit) {
    out.push({ id: "drawdown_freeze", halt: true,
      reason: `account is ${state.drawdownPct.toFixed(2)}% below its high-water mark, past the ${ddLimit}% freeze` });
  }

  const dayLimit = -Math.abs(pc.oneDayLossPausePctOfNav ?? 1);
  if (Number.isFinite(state.dayPnlPct) && state.dayPnlPct <= dayLimit) {
    out.push({ id: "day_loss_pause", halt: true,
      reason: `down ${state.dayPnlPct.toFixed(2)}% today, past the ${dayLimit}% daily pause` });
  }

  return { halted: out.some((o) => o.halt), breakers: out };
}

/* ── the per-candidate portfolio checks ────────────────────────────────── */
/**
 * Decide whether ONE proposed position may be added, given the book. Returns
 * an allow/deny with a plain reason, plus the size actually permitted after
 * every cap is applied.
 *
 * Order matters: refusals that are about the book as a whole come before the
 * ones that trim a single position, so the reason recorded is the most
 * fundamental one rather than whichever fired first by accident.
 */
function checkAdd({ symbol, sector, proposedUsd, book, navUsd, cashUsd, cfg, dynamicCorrelations = null }) {
  const pc = cfg.portfolioControls || {};
  const nav = Number(navUsd) || 0;
  const pct = (v) => (nav > 0 ? (v / nav) * 100 : 0);
  const checks = [];
  const deny = (id, reason) => { checks.push({ id, pass: false, reason }); };
  const pass = (id, reason) => { checks.push({ id, pass: true, reason }); };

  // 1. how many positions are we willing to run at once?
  const maxPos = pc.maxOpenPositions ?? 12;
  if (book.count >= maxPos) deny("max_positions", `already holding ${book.count} of a maximum ${maxPos} positions`);
  else pass("max_positions", `${book.count} of ${maxPos} positions used`);

  // 2. never hold the same name twice
  if (book.rows.some((r) => r.symbol === symbol)) deny("duplicate", `already holding ${symbol}`);
  else pass("duplicate", "not already held");

  // 3. gross exposure ceiling
  const maxGross = pc.maxGrossExposurePct ?? 60;
  const grossAfter = pct(book.grossUsd + proposedUsd);
  if (grossAfter > maxGross) deny("gross_exposure", `would put ${grossAfter.toFixed(1)}% of the account at work, above the ${maxGross}% ceiling`);
  else pass("gross_exposure", `${grossAfter.toFixed(1)}% invested after this, ceiling ${maxGross}%`);

  // 4. cash floor — never fully invested
  const minCash = pc.minCashPct ?? 40;
  const cashAfterPct = pct((Number(cashUsd) || 0) - proposedUsd);
  if (cashAfterPct < minCash) deny("cash_floor", `would leave ${cashAfterPct.toFixed(1)}% cash, below the ${minCash}% floor`);
  else pass("cash_floor", `${cashAfterPct.toFixed(1)}% cash left, floor ${minCash}%`);

  // 5. sector cap
  const maxSector = pc.sectorExposurePctOfNav ?? 20;
  const secAfter = (book.bySectorPct[sector] || 0) + pct(proposedUsd);
  if (secAfter > maxSector) deny("sector_cap", `${sector} would reach ${secAfter.toFixed(1)}% of the account, above the ${maxSector}% cap`);
  else pass("sector_cap", `${sector} at ${secAfter.toFixed(1)}% after this, cap ${maxSector}%`);

  /* 6. CLUSTER CAP — the one the strategy file calls the primary defence.
        Binds on shared economic exposure rather than sector label, so two
        suppliers to the same customer count as one bet. */
  const maxCluster = pc.correlatedClusterPctOfNav ?? 10;
  const cl = clusterOf(symbol, sector);
  const clAfter = (book.byClusterPct[cl] || 0) + pct(proposedUsd);
  if (clAfter > maxCluster) {
    const held = book.rows.filter((r) => r.cluster === cl).map((r) => r.symbol);
    deny("cluster_cap", `${cl} would reach ${clAfter.toFixed(1)}%, above the ${maxCluster}% cap`
       + (held.length ? ` — already holding ${held.join(", ")} in the same cluster` : ""));
  } else pass("cluster_cap", `${cl} at ${clAfter.toFixed(1)}% after this, cap ${maxCluster}%`);

  /* 7. Point-in-time correlation network. The hand-drawn cluster remains a
     prior, while this gate catches relationships that changed with the current
     regime. Missing pair data fails closed once the book already has risk. */
  const corrThreshold = pc.dynamicCorrelationThreshold ?? 0.65;
  const dynamicCap = pc.dynamicCorrelationExposurePctOfNav ?? maxCluster;
  const requireDynamic = pc.requireDynamicCorrelation === true;
  const corrRows = dynamicCorrelations || {};
  const missingCorr = book.rows.filter((r) => !(corrRows[r.symbol] && corrRows[r.symbol].status === "ready"));
  const correlatedRows = book.rows.filter((r) => corrRows[r.symbol]
    && corrRows[r.symbol].status === "ready"
    && Number(corrRows[r.symbol].stressedCorrelation) >= corrThreshold);
  const dynamicHeldUsd = correlatedRows.reduce((sum, r) => sum + Number(r.valueUsd || 0), 0);
  const dynamicAfter = pct(dynamicHeldUsd + proposedUsd);
  if (book.rows.length && requireDynamic && missingCorr.length) {
    deny("dynamic_correlation_unknown", `point-in-time correlation unavailable versus ${missingCorr.map((x) => x.symbol).join(", ")}`);
  } else if (dynamicAfter > dynamicCap) {
    deny("dynamic_correlation_cap", `current correlation network would reach ${dynamicAfter.toFixed(1)}%, above the ${dynamicCap}% cap`
      + (correlatedRows.length ? ` — linked to ${correlatedRows.map((x) => x.symbol).join(", ")}` : ""));
  } else pass("dynamic_correlation_cap", `${dynamicAfter.toFixed(1)}% in the current correlation network, cap ${dynamicCap}%`);

  const denied = checks.filter((c) => !c.pass);

  /* The largest size that would satisfy every cap, so a candidate that is
     merely too BIG is trimmed rather than dropped — but one that breaches a
     count or duplicate rule is refused outright, since no size fixes those. */
  const dynamicHeadroom = nav * (dynamicCap / 100) - dynamicHeldUsd;
  const headroomUsd = Math.max(0, Math.min(
    nav * (maxGross / 100) - book.grossUsd,
    (Number(cashUsd) || 0) - nav * (minCash / 100),
    nav * (maxSector / 100) - (book.bySectorPct[sector] || 0) * nav / 100,
    nav * (maxCluster / 100) - (book.byClusterPct[cl] || 0) * nav / 100,
    dynamicHeadroom,
  ));
  const hardBlock = denied.some((d) => d.id === "max_positions" || d.id === "duplicate"
    || d.id === "dynamic_correlation_unknown");

  return {
    allow: denied.length === 0,
    allowTrimmed: !hardBlock && headroomUsd > 0,
    permittedUsd: hardBlock ? 0 : Math.min(proposedUsd, headroomUsd),
    headroomUsd,
    cluster: cl,
    dynamicCorrelation: { threshold: corrThreshold, capPct: dynamicCap,
      correlatedSymbols: correlatedRows.map((x) => x.symbol), missingSymbols: missingCorr.map((x) => x.symbol) },
    checks,
    blockedBy: denied.map((d) => d.id),
    firstBlock: denied[0] ? denied[0].reason : null,
  };
}

/* ── plain-English summary for the dashboard ───────────────────────────── */
function describe(book, breakers, cfg) {
  const pc = cfg.portfolioControls || {};
  const lines = [];
  lines.push(`Holding ${book.count} positions worth ${book.grossPct}% of the account (ceiling ${pc.maxGrossExposurePct ?? 60}%).`);
  if (book.unmarked > 0) {
    lines.push(`${book.unmarked} of them have no current price, so their value is shown at what was paid — the real number could be better or worse.`);
  }
  if (book.count) {
    lines.push(`Unrealised ${book.unrealisedPct >= 0 ? "gain" : "loss"} of ${Math.abs(book.unrealisedPct)}% of the account.`);
    const topCluster = Object.entries(book.byClusterPct).sort((a, b) => b[1] - a[1])[0];
    if (topCluster) lines.push(`Biggest single concentration: ${topCluster[0]} at ${topCluster[1]}% (cap ${pc.correlatedClusterPctOfNav ?? 10}%).`);
  }
  if (breakers && breakers.halted) {
    lines.push(`TRADING PAUSED — ${breakers.breakers.map((b) => b.reason).join("; ")}. Existing positions are still managed and can still be sold; no new ones will be opened.`);
  }
  return lines;
}

/* ── PERFORMANCE MEASUREMENT ───────────────────────────────────────────────
 * What was measured before this: cumulative friction against cumulative gross
 * realised edge, and a three-word verdict. That answers "did edge beat costs"
 * and nothing else. It says nothing about how bumpy the ride was, how deep the
 * worst hole got, or — the question that actually decides whether any of this
 * was worth doing — whether it beat simply buying the same names and waiting.
 *
 * A strategy that returns 8% while its own universe returned 12% has not made
 * money. It has lost 4% and charged itself commissions for the privilege. That
 * comparison was entirely absent, which is a strange omission in a system whose
 * whole thesis is RESIDUAL return: it measured alpha at signal time and then
 * evaluated results in raw terms.
 */
function equityStats(navSeries) {
  const pts = (navSeries || []).filter((p) => p && Number.isFinite(p.navUsd) && p.navUsd > 0)
    .sort((a, b) => (a.date < b.date ? -1 : 1));
  if (pts.length < 2) {
    return { ok: false, days: pts.length, reason: "need at least two daily NAV marks before performance can be measured" };
  }

  const rets = [];
  for (let i = 1; i < pts.length; i++) {
    /* A reliable mark after a missing session is still valid for total return
       and drawdown, but the multi-session jump is not one daily observation. */
    if (pts[i].returnAdmissible === false) continue;
    rets.push((pts[i].navUsd - pts[i - 1].navUsd) / pts[i - 1].navUsd);
  }
  const m = rets.length ? rets.reduce((a, b) => a + b, 0) / rets.length : null;
  const sd = rets.length > 1
    ? Math.sqrt(rets.reduce((a, b) => a + (b - m) * (b - m), 0) / (rets.length - 1)) : 0;

  // worst peak-to-trough on the actual path, not just start-to-end
  let peak = pts[0].navUsd, maxDd = 0, maxDdDate = null;
  for (const p of pts) {
    if (p.navUsd > peak) peak = p.navUsd;
    const dd = (p.navUsd - peak) / peak;
    if (dd < maxDd) { maxDd = dd; maxDdDate = p.date; }
  }

  const first = pts[0].navUsd, last = pts[pts.length - 1].navUsd;
  const totalPct = ((last - first) / first) * 100;
  const annualised = m != null && sd > 0 ? (m / sd) * Math.sqrt(252) : null;   // Sharpe, cash rate ignored

  return {
    ok: true,
    days: pts.length, returnDays: rets.length,
    startNavUsd: Number(first.toFixed(2)), endNavUsd: Number(last.toFixed(2)),
    totalReturnPct: Number(totalPct.toFixed(3)),
    dailyVolPct: rets.length ? Number((sd * 100).toFixed(3)) : null,
    annualisedVolPct: rets.length ? Number((sd * Math.sqrt(252) * 100).toFixed(2)) : null,
    maxDrawdownPct: Number((maxDd * 100).toFixed(2)),
    maxDrawdownDate: maxDdDate,
    sharpe: annualised != null ? Number(annualised.toFixed(2)) : null,
    winDays: rets.filter((r) => r > 0).length,
    lossDays: rets.filter((r) => r < 0).length,
  };
}

/**
 * The benchmark: an equal-weighted buy-and-hold of the same roster over the
 * same window. Deliberately not the S&P — the honest comparison is against the
 * universe this system actually chooses from, since beating the market by
 * picking a better universe is not the same as the strategy adding anything.
 */
function benchmarkReturn(dailyBySymbol, fromDate, toDate) {
  const per = [];
  for (const [sym, series] of Object.entries(dailyBySymbol || {})) {
    const win = (series || []).filter((b) => b.date >= fromDate && b.date <= toDate && b.c > 0);
    if (win.length < 2) continue;
    per.push({ symbol: sym, pct: ((win[win.length - 1].c - win[0].c) / win[0].c) * 100 });
  }
  if (!per.length) return { ok: false, reason: "no overlapping daily history for a benchmark" };
  const mean = per.reduce((a, b) => a + b.pct, 0) / per.length;
  return {
    ok: true, names: per.length, fromDate, toDate,
    equalWeightedPct: Number(mean.toFixed(3)),
    bestPct: Number(Math.max(...per.map((p) => p.pct)).toFixed(2)),
    worstPct: Number(Math.min(...per.map((p) => p.pct)).toFixed(2)),
  };
}

/** Strategy vs benchmark, stated the way a person would ask it. */
function attribution(equity, bench) {
  if (!equity || !equity.ok) return { ok: false, reason: (equity && equity.reason) || "no equity history" };
  if (!bench || !bench.ok) {
    return { ok: false, strategyPct: equity.totalReturnPct, reason: bench ? bench.reason : "no benchmark" };
  }
  const excess = equity.totalReturnPct - bench.equalWeightedPct;
  return {
    ok: true,
    strategyPct: equity.totalReturnPct,
    benchmarkPct: bench.equalWeightedPct,
    excessPct: Number(excess.toFixed(3)),
    verdict: excess > 0
      ? `ahead of simply holding the same ${bench.names} names by ${excess.toFixed(2)}%`
      : `BEHIND simply holding the same ${bench.names} names by ${Math.abs(excess).toFixed(2)}% — the trading has subtracted value so far`,
    maxDrawdownPct: equity.maxDrawdownPct,
    sharpe: equity.sharpe,
  };
}

module.exports = {
  CLUSTERS, clusterOf, summarise, markedBook, positionSizeUsd, accountBreakers, checkAdd, describe,
  equityStats, benchmarkReturn, attribution,
};
