/*  netlify/functions/_investorDossier.js  (fund-manager-v1)
 *  ---------------------------------------------------------------------------
 *  Investor AI — persistent, sector-aware company dossiers (blueprint §5.2,
 *  §5.3, §5.4, §5.5, §10.2, §14.8).
 *
 *  THREE LAYERS, THREE RECORDS
 *  ------------------------------------------------------------------------
 *  A DossierVersion is the FACTS layer only: identity, point-in-time SEC
 *  facts (by fact id), guidance and earnings-date claims (by claim id), the
 *  document versions in the lookback window (by version id) and the
 *  sector-specific block derived from those same facts. It carries POINTERS
 *  to the interpretation layer (ResearchMemo, ManagerDecision) and to the
 *  forecast layer (Forecast) and never embeds either: an old AI opinion can
 *  therefore never re-enter tomorrow's prompt as if it were evidence.
 *
 *  The content hash covers the facts layer alone. A daily close changes the
 *  card, not the dossier: a new version is appended only when a fact, claim
 *  or document version changed. Price, relative returns, standing view and
 *  portfolio flags are attached at card time from live state.
 *
 *  POINT IN TIME
 *  ------------------------------------------------------------------------
 *  Every read takes a cutoff. A fact is visible from its filing's
 *  availability instant, a claim from its publication or first-seen time,
 *  a document from its first-seen time when publication is unreliable —
 *  never earlier. compactCards and expandedHoldingDeltas are built at the
 *  Manager Meeting's frozen decision cutoff, so two meetings with the same
 *  cutoff see the same evidence.
 *
 *  Numbers on the wire are canonical integer strings (§7.1). No composite
 *  score is ever computed here: Sol receives raw components and an explicit
 *  missing-data record (§5.4).
 * ---------------------------------------------------------------------------
 */

"use strict";

const crypto = require("crypto");
const A = require("./_investorAdmin");
const POLICY = require("./_investorPolicy");
const M = require("./_investorMoney");

const DOSSIER_SCHEMA = "dossier-version.v1";
const CARD_SCHEMA = "universe-card.v1";
const DELTA_SCHEMA = "evidence-delta.v1";
const BUILDER_VERSION = "dossier-builder.v1";
const LOOKBACK_DAYS = 180;
const MAX_DOCUMENT_REFS = 60;
const MAX_CHANGES_ON_CARD = 6;
const MAX_TOKENS_PER_CARD = 500;          // §5.4 target 300–500 tokens
const CHARS_PER_TOKEN_ESTIMATE = 4;
const MARKET_SYMBOL = "SPY";

function sha256(v) { return crypto.createHash("sha256").update(typeof v === "string" ? v : JSON.stringify(POLICY.canonical(v))).digest("hex"); }
function canonicalInt(v) { return v == null ? null : (typeof v === "bigint" ? v.toString() : (/^-?(0|[1-9][0-9]*)$/.test(String(v)) ? String(v) : null)); }
function isoDay(ms) { return new Date(ms).toISOString().slice(0, 10); }

/* ── sector blocks (§5.4) ──────────────────────────────────────────────────
 * The universe's sector codes map onto the blueprint's sector-specific
 * blocks. Each block names the concepts it draws from the point-in-time
 * facts and the claims it looks for in extracted evidence. A metric the
 * filings do not contain is null with a note — never estimated. */
const SECTOR_BLOCKS = Object.freeze({
  financials: Object.freeze({
    sectors: ["fin"],
    label: "banks / insurers / capital markets",
    metrics: Object.freeze({
      depositsMinor: { concepts: ["Deposits"], kind: "instant", scale: 2, unit: "USD" },
      loansNetMinor: { concepts: ["LoansAndLeasesReceivableNetReportedAmount", "NotesReceivableNet"], kind: "instant", scale: 2, unit: "USD" },
      creditProvisionTtmMinor: { concepts: ["ProvisionForLoanLeaseAndOtherLosses", "ProvisionForLoanLossesExpensed", "ProvisionForCreditLosses"], kind: "ttm", scale: 2, unit: "USD" },
      equityMinor: { concepts: ["StockholdersEquity"], kind: "instant", scale: 2, unit: "USD" },
      netInterestIncomeTtmMinor: { concepts: ["InterestIncomeExpenseNet"], kind: "ttm", scale: 2, unit: "USD" },
      premiumsEarnedTtmMinor: { concepts: ["PremiumsEarnedNet"], kind: "ttm", scale: 2, unit: "USD" },
      lossReservesMinor: { concepts: ["LiabilityForClaimsAndClaimsAdjustmentExpense"], kind: "instant", scale: 2, unit: "USD" },
    }),
    claimTypes: ["GUIDANCE", "REGULATORY_STATUS"],
  }),
  software: Object.freeze({
    sectors: ["sw", "plat"],
    label: "software / platforms",
    metrics: Object.freeze({
      deferredRevenueMinor: { concepts: ["ContractWithCustomerLiabilityCurrent", "DeferredRevenueCurrent"], kind: "instant", scale: 2, unit: "USD" },
      researchDevelopmentTtmMinor: { concepts: ["ResearchAndDevelopmentExpense"], kind: "ttm", scale: 2, unit: "USD" },
      stockCompensationTtmMinor: { concepts: ["ShareBasedCompensation", "AllocatedShareBasedCompensationExpense"], kind: "ttm", scale: 2, unit: "USD" },
      grossProfitTtmMinor: { concepts: ["GrossProfit"], kind: "ttm", scale: 2, unit: "USD" },
      salesMarketingTtmMinor: { concepts: ["SellingAndMarketingExpense"], kind: "ttm", scale: 2, unit: "USD" },
    }),
    claimTypes: ["GUIDANCE", "KPI"],
  }),
  resources: Object.freeze({
    sectors: ["energy", "power", "mat"],
    label: "energy / power / materials",
    metrics: Object.freeze({
      capexTtmMinor: { concepts: ["PaymentsToAcquirePropertyPlantAndEquipment", "PaymentsToAcquireProductiveAssets", "PaymentsToAcquireOilAndGasPropertyAndEquipment"], kind: "ttm", scale: 2, unit: "USD" },
      depletionTtmMinor: { concepts: ["DepreciationDepletionAndAmortization"], kind: "ttm", scale: 2, unit: "USD" },
      ppeNetMinor: { concepts: ["PropertyPlantAndEquipmentNet"], kind: "instant", scale: 2, unit: "USD" },
      longTermDebtMinor: { concepts: ["LongTermDebtNoncurrent", "LongTermDebt"], kind: "instant", scale: 2, unit: "USD" },
    }),
    claimTypes: ["GUIDANCE", "PRODUCTION", "RESERVES"],
    driverSensitivity: true,
  }),
  life_sciences: Object.freeze({
    sectors: ["pharma", "health"],
    label: "biotech / pharma / health",
    metrics: Object.freeze({
      researchDevelopmentTtmMinor: { concepts: ["ResearchAndDevelopmentExpense"], kind: "ttm", scale: 2, unit: "USD" },
      cashMinor: { concepts: ["CashAndCashEquivalentsAtCarryingValue"], kind: "instant", scale: 2, unit: "USD" },
      operatingCashFlowTtmMinor: { concepts: ["NetCashProvidedByUsedInOperatingActivities"], kind: "ttm", scale: 2, unit: "USD" },
      runwayMonths: { derived: "runway", scale: 0 },
    }),
    claimTypes: ["GUIDANCE", "TRIAL_STATUS", "REGULATORY_STATUS"],
  }),
  industrial: Object.freeze({
    sectors: ["semi", "hw", "indus", "cons"],
    label: "semis / hardware / industrials / consumer",
    metrics: Object.freeze({
      inventoryMinor: { concepts: ["InventoryNet"], kind: "instant", scale: 2, unit: "USD" },
      costOfRevenueTtmMinor: { concepts: ["CostOfRevenue", "CostOfGoodsAndServicesSold"], kind: "ttm", scale: 2, unit: "USD" },
      capexTtmMinor: { concepts: ["PaymentsToAcquirePropertyPlantAndEquipment", "PaymentsToAcquireProductiveAssets"], kind: "ttm", scale: 2, unit: "USD" },
      inventoryDays: { derived: "inventoryDays", scale: 0 },
    }),
    claimTypes: ["GUIDANCE", "BACKLOG"],
  }),
  general: Object.freeze({ sectors: [], label: "general", metrics: Object.freeze({}), claimTypes: ["GUIDANCE"] }),
});
const BLOCK_BY_SECTOR = Object.freeze(Object.fromEntries(Object.entries(SECTOR_BLOCKS)
  .flatMap(([name, b]) => b.sectors.map((s) => [s, name]))));
function sectorBlockFor(sector) { return BLOCK_BY_SECTOR[String(sector || "").toLowerCase()] || "general"; }

/** PURE. Derive a sector block from point-in-time facts. Every metric lists
 *  the fact ids it came from; every null carries a note. */
function buildSectorBlock({ sector, facts = [], asOfMs }) {
  const F = require("./_investorFundamentals");
  const name = sectorBlockFor(sector);
  const spec = SECTOR_BLOCKS[name];
  const pit = F.pointInTime(facts, { asOfMs });
  const metrics = {}, factIds = {}, notes = [];
  const value = (key, def) => {
    if (def.derived) return null;
    const pick = F.pickConcept(pit, def.concepts, { unit: def.unit || "USD", instant: def.kind === "instant" });
    if (!pick.concept) { notes.push(`${key}: no ${def.concepts[0]} fact in point-in-time facts`); return null; }
    if (def.kind === "instant") {
      const inst = F.latestInstant(pick.rows);
      if (!inst) { notes.push(`${key}: no instant fact`); return null; }
      factIds[`${key}_facts`] = [inst.factId];
      return F.scaledCanonical(F.factRational(inst), def.scale);
    }
    const series = F.quarterSeries(pick.rows);
    const end = F.latestDurationEnd(series);
    const ttm = end ? F.ttmAt(series, end) : null;
    if (!ttm) { notes.push(`${key}: no trailing-twelve-month window`); return null; }
    factIds[`${key}_facts`] = ttm.factIds.slice();
    return F.scaledCanonical(ttm.value, def.scale);
  };
  for (const [key, def] of Object.entries(spec.metrics)) metrics[key] = value(key, def);
  /* derived metrics from the block's own inputs; exact rational arithmetic */
  if (spec.metrics.runwayMonths) {
    const cash = metrics.cashMinor, ocf = metrics.operatingCashFlowTtmMinor;
    if (cash != null && ocf != null && BigInt(ocf) < 0n) {
      metrics.runwayMonths = M.toCanonical(M.ratToInteger(M.ratDiv(M.rational(BigInt(cash) * 12n, 1n), M.rational(-BigInt(ocf), 1n)), M.ROUNDING.HALF_EVEN));
      factIds.runwayMonths_facts = [...(factIds.cashMinor_facts || []), ...(factIds.operatingCashFlowTtmMinor_facts || [])];
    } else notes.push(ocf != null && BigInt(ocf) >= 0n ? "runwayMonths: operating cash flow is not negative (no burn)" : "runwayMonths: cash or operating cash flow missing");
  }
  if (spec.metrics.inventoryDays) {
    const inv = metrics.inventoryMinor, cogs = metrics.costOfRevenueTtmMinor;
    if (inv != null && cogs != null && BigInt(cogs) > 0n) {
      metrics.inventoryDays = M.toCanonical(M.ratToInteger(M.ratDiv(M.rational(BigInt(inv) * 365n, 1n), M.rational(BigInt(cogs), 1n)), M.ROUNDING.HALF_EVEN));
      factIds.inventoryDays_facts = [...(factIds.inventoryMinor_facts || []), ...(factIds.costOfRevenueTtmMinor_facts || [])];
    } else notes.push("inventoryDays: inventory or cost of revenue missing");
  }
  return { block: name, label: spec.label, metrics, factIds, notes, claimTypes: spec.claimTypes.slice(),
    missing: Object.keys(spec.metrics).filter((k) => metrics[k] == null) };
}

/* ── price and relative returns (card-time, never hashed) ───────────────── */
function closeMicrosOf(bar) {
  const c = Number(bar && bar.c);
  if (!(c > 0)) return null;
  /* provider bars are floats; conversion to micros happens once, here, at the boundary */
  return BigInt(Math.round(c * 1e6));
}
/** PURE. Returns in bps from completed daily bars strictly before the cutoff day. */
function returnsBps(series, { asOfMs }) {
  const day = isoDay(asOfMs);
  const rows = (series || []).filter((b) => b && b.date && b.date < day && Number(b.c) > 0)
    .sort((a, b) => String(a.date).localeCompare(String(b.date)));
  if (!rows.length) return { ok: false, reason: "no_completed_bars", closeMicros: null, asOfDate: null, bps: {} };
  const last = rows[rows.length - 1];
  const lastMicros = closeMicrosOf(last);
  const at = (n) => (rows.length > n ? closeMicrosOf(rows[rows.length - 1 - n]) : null);
  const bps = {};
  for (const [label, n] of [["1d", 1], ["5d", 5], ["3m", 63], ["1y", 252]]) {
    const from = at(n);
    bps[label] = from ? M.toCanonical(M.returnBps({ fromMicros: from, toMicros: lastMicros, mode: M.ROUNDING.HALF_EVEN })) : null;
  }
  const window = rows.slice(-252);
  let high = 0n;
  for (const b of window) { const m = closeMicrosOf(b); if (m > high) high = m; }
  const drawdownBps = high > 0n ? M.toCanonical(M.returnBps({ fromMicros: high, toMicros: lastMicros, mode: M.ROUNDING.HALF_EVEN })) : null;
  return { ok: true, closeMicros: M.toCanonical(lastMicros), asOfDate: last.date, bps, drawdownBps, bars: rows.length };
}

/* ── the facts layer ───────────────────────────────────────────────────── */
/** PURE. Compose an immutable DossierVersion from already-read inputs. */
function composeVersion({ symbol, identity, asOfMs, facts = [], fundamentals = null, claims = [], documents = [],
  pointers = {}, dataQuality = null, priorVersion = 0 }) {
  const E = require("./_investorEvidence");
  const sym = String(symbol || "").toUpperCase();
  const sectorBlock = buildSectorBlock({ sector: identity && identity.sector, facts, asOfMs });
  const guidance = E.latestGuidanceFrom(claims, { asOfMs });
  const nextEarnings = E.nextEarningsFrom(claims, { asOfMs });
  const docRefs = (documents || []).filter((d) => d && d.documentId)
    .sort((a, b) => (Number(b.decisionKnownAtMs || b.firstSeenAtMs) || 0) - (Number(a.decisionKnownAtMs || a.firstSeenAtMs) || 0))
    .slice(0, MAX_DOCUMENT_REFS)
    .map((d) => ({ documentId: d.documentId, versionId: d.versionId || d.latestVersionId || null,
      contentHash: d.canonicalContentSha256 || d.canonical_content_sha256 || null, sourceId: d.sourceId || null,
      form: d.form || null, knownAtMs: Number(d.decisionKnownAtMs || d.firstSeenAtMs) || null }));
  const factsLayer = {
    identity: { symbol: sym, name: identity && identity.name || null, sector: identity && identity.sector || null,
      cik: identity && identity.cik || null, sic: identity && identity.sic || null },
    fundamentals: fundamentals ? {
      revenueGrowthBps: canonicalInt(fundamentals.revenueGrowthBps), fcfMarginBps: canonicalInt(fundamentals.fcfMarginBps),
      netDebtEbitdaMilli: canonicalInt(fundamentals.netDebtEbitdaMilli), sharesOutstanding: canonicalInt(fundamentals.sharesOutstanding),
      epsTrailingMicros: canonicalInt(fundamentals.epsTrailingMicros), netIncomeTtmMinor: canonicalInt(fundamentals.netIncomeTtmMinor),
      revenueTtmMinor: canonicalInt(fundamentals.revenueTtmMinor),
      factIds: fundamentals.basis && fundamentals.basis.factIds || {}, periods: fundamentals.basis && fundamentals.basis.periods || {},
      notes: fundamentals.basis && fundamentals.basis.notes || [] } : null,
    sectorBlock,
    guidance: guidance.map((g) => ({ claimId: g.claimId, documentVersionId: g.documentVersionId, metric: g.metric, periodLabel: g.periodLabel,
      lowValue: canonicalInt(g.lowValue), highValue: canonicalInt(g.highValue), unit: g.unit || null, issuedAt: g.issuedAt || null, supersedes: g.supersedes || null })),
    nextEarnings: nextEarnings ? { date: nextEarnings.date, confirmed: nextEarnings.confirmed === true, claimId: nextEarnings.claimId,
      documentVersionId: nextEarnings.documentVersionId, sourceClass: nextEarnings.sourceClass || "company_primary" } : null,
    claimIds: [...new Set((claims || []).map((c) => c.claimId).filter(Boolean))].sort(),
    documents: docRefs,
    factIds: [...new Set((facts || []).map((f) => f.factId).filter(Boolean))].sort(),
  };
  const contentHash = sha256(factsLayer);
  const quality = dataQuality || {};
  const missing = [...new Set([...(quality.missing || []),
    ...(!factsLayer.fundamentals ? ["fundamentals"] : Object.entries(factsLayer.fundamentals)
      .filter(([k, v]) => ["revenueGrowthBps", "fcfMarginBps", "netDebtEbitdaMilli"].includes(k) && v == null).map(([k]) => `fundamentals.${k}`)),
    ...sectorBlock.missing.map((k) => `sectorBlock.${k}`),
    ...(guidance.length ? [] : ["guidance"]), ...(nextEarnings ? [] : ["nextEarnings.confirmed"])])];
  return {
    schemaVersion: DOSSIER_SCHEMA, builderVersion: BUILDER_VERSION, symbol: sym,
    version: Number(priorVersion) + 1, contentHash, asOfMs: Number(asOfMs), asOf: new Date(Number(asOfMs)).toISOString(),
    lookbackDays: LOOKBACK_DAYS, ...factsLayer,
    /* interpretation and forecast layers by POINTER only (§5.3) */
    pointers: { researchMemoId: pointers.researchMemoId || null, researchVersion: pointers.researchVersion || null,
      managerDecisionId: pointers.managerDecisionId || null, forecastIds: (pointers.forecastIds || []).slice(0, 12),
      mandateSeriesId: pointers.mandateSeriesId || null },
    dataQuality: { complete: missing.length === 0, missing, sourceCoverage: quality.sourceCoverage || null,
      note: "no consensus vendor by design; forward view is issuer guidance" },
  };
}

/** PURE. A typed difference between two dossier versions — never an opinion. */
function buildDelta(prev, next) {
  const F = require("./_investorFundamentals");
  const pv = prev || null;
  const pFacts = new Set(pv ? pv.factIds || [] : []), nFacts = new Set(next.factIds || []);
  const pClaims = new Set(pv ? pv.claimIds || [] : []), nClaims = new Set(next.claimIds || []);
  const pDocs = new Map((pv ? pv.documents || [] : []).map((d) => [d.documentId, d]));
  const nDocs = new Map((next.documents || []).map((d) => [d.documentId, d]));
  const added = [], revised = [], contradicted = [], expired = [];
  for (const id of nFacts) if (!pFacts.has(id)) added.push({ kind: "fact", id });
  for (const id of pFacts) if (!nFacts.has(id)) expired.push({ kind: "fact", id });
  for (const id of nClaims) if (!pClaims.has(id)) added.push({ kind: "claim", id });
  for (const id of pClaims) if (!nClaims.has(id)) expired.push({ kind: "claim", id });
  for (const [id, d] of nDocs) {
    const p = pDocs.get(id);
    if (!p) added.push({ kind: "document", id, versionId: d.versionId });
    else if (p.versionId !== d.versionId || p.contentHash !== d.contentHash) revised.push({ kind: "document", id, from: p.versionId, to: d.versionId });
  }
  for (const id of pDocs.keys()) if (!nDocs.has(id)) expired.push({ kind: "document", id });
  /* guidance supersession is a revision of the metric/period; a new guide
     that moves the range in the opposite direction from the previous move
     is still a revision — contradiction is reserved for two LIVE statements
     of the same metric and period that disagree at the same instant. */
  const nGuide = next.guidance || [];
  for (const g of nGuide) if (g.supersedes && pClaims.has(g.supersedes)) revised.push({ kind: "guidance", id: g.claimId, from: g.supersedes, metric: g.metric, periodLabel: g.periodLabel });
  const seen = new Map();
  for (const g of nGuide) {
    const key = `${g.metric}|${g.periodLabel}`;
    const other = seen.get(key);
    if (other && (other.lowValue !== g.lowValue || other.highValue !== g.highValue)) contradicted.push({ kind: "guidance", ids: [other.claimId, g.claimId], metric: g.metric, periodLabel: g.periodLabel });
    seen.set(key, g);
  }
  /* fundamentals: same metric, different value → revised */
  if (pv && pv.fundamentals && next.fundamentals) {
    for (const k of ["revenueGrowthBps", "fcfMarginBps", "netDebtEbitdaMilli", "sharesOutstanding", "epsTrailingMicros", "revenueTtmMinor", "netIncomeTtmMinor"]) {
      if (pv.fundamentals[k] != null && next.fundamentals[k] != null && pv.fundamentals[k] !== next.fundamentals[k]) revised.push({ kind: "fundamental", id: k, from: pv.fundamentals[k], to: next.fundamentals[k] });
    }
  }
  if (pv && pv.nextEarnings && next.nextEarnings && pv.nextEarnings.date !== next.nextEarnings.date) revised.push({ kind: "earningsDate", from: pv.nextEarnings.date, to: next.nextEarnings.date });
  const changed = added.length + revised.length + contradicted.length + expired.length > 0;
  return {
    schemaVersion: DELTA_SCHEMA, kind: "dossier_delta", symbol: next.symbol,
    fromVersionId: pv ? versionDocId(pv) : null, toVersionId: versionDocId(next),
    fromHash: pv ? pv.contentHash : null, toHash: next.contentHash, changed,
    added, revised, contradicted, expired,
    /* objective only: a contradiction among required facts is an integrity event (§5.5) */
    safetyClass: contradicted.length ? "integrity" : (changed ? "routine" : "none"),
    counts: { added: added.length, revised: revised.length, contradicted: contradicted.length, expired: expired.length },
    asOfMs: next.asOfMs, lineageKeyNote: F.lineageKey ? "fact lineage by taxonomy|concept|unit|period" : null,
  };
}
function versionDocId(v) { return `${v.symbol}_v${String(v.version).padStart(4, "0")}_${String(v.contentHash).slice(0, 16)}`; }

/* ── freshness matrix (§14.8: "fresh" is not one global boolean) ──────── */
const FRESHNESS_WINDOWS_S = Object.freeze({
  source: 36 * 3600, dossier: 36 * 3600, managerReview: 2 * 86400, mandate: 5 * 86400, marketMark: 26 * 3600,
});
/** PURE. Each dimension carries its own as-of, age and window. */
function freshnessMatrix(pointer = {}, { nowMs = Date.now(), windows = FRESHNESS_WINDOWS_S } = {}) {
  const dim = (key, asOfMs) => {
    const at = Number(asOfMs);
    const known = Number.isFinite(at) && at > 0;
    const ageSeconds = known ? Math.max(0, Math.round((nowMs - at) / 1000)) : null;
    return { asOfMs: known ? at : null, ageSeconds, windowSeconds: windows[key], fresh: known && ageSeconds <= windows[key],
      state: !known ? "never" : ageSeconds <= windows[key] ? "fresh" : "stale" };
  };
  const out = {
    source: dim("source", pointer.lastSourceAtMs), dossier: dim("dossier", pointer.asOfMs),
    managerReview: dim("managerReview", pointer.lastManagerReviewAtMs), mandate: dim("mandate", pointer.lastMandateAtMs),
    marketMark: dim("marketMark", pointer.lastMarketMarkAtMs),
  };
  out.oldestSeconds = Math.max(...Object.values(out).map((d) => (d && typeof d === "object" && d.ageSeconds != null ? d.ageSeconds : 0)));
  return out;
}

/* ── the §5.4 compact card (PURE from a version + live inputs) ─────────── */
function cardFromVersion(version, { cutoffMs, price = null, sectorPrice = null, marketPrice = null, changes = [],
  standingView = null, portfolio = null, valuation = null } = {}) {
  const fu = version.fundamentals || {};
  const g = (version.guidance || [])[0] || null;
  const asOf = new Date(Number(cutoffMs || version.asOfMs)).toISOString();
  const rel = {
    sector5dBps: sectorPrice && sectorPrice.ok ? sectorPrice.bps["5d"] : null,
    market5dBps: marketPrice && marketPrice.ok ? marketPrice.bps["5d"] : null,
    drawdownBps: price && price.ok ? price.drawdownBps : null,
  };
  const missing = [...(version.dataQuality && version.dataQuality.missing || [])];
  if (!price || !price.ok) missing.push("price");
  const card = {
    schemaVersion: CARD_SCHEMA, symbol: version.symbol,
    identity: { name: version.identity.name, sector: version.identity.sector, cik: version.identity.cik },
    asOf,
    price: price && price.ok ? { currency: "USD", closeMicros: price.closeMicros, asOfDate: price.asOfDate, returnBps: price.bps } : null,
    relative: rel,
    fundamentals: { revenueGrowthBps: fu.revenueGrowthBps ?? null, fcfMarginBps: fu.fcfMarginBps ?? null, netDebtEbitdaMilli: fu.netDebtEbitdaMilli ?? null },
    sectorBlock: { block: version.sectorBlock.block, metrics: version.sectorBlock.metrics },
    valuation: { methodHints: valuation && valuation.methodHints || methodHintsFor(version.sectorBlock.block),
      trailingMultipleMilli: valuation && valuation.trailingMultipleMilli || trailingMultipleMilli(version, price),
      forwardMultipleMilli: null, note: "trailing multiple is derived from filings and price; forward multiple requires guidance" },
    guidance: g ? { periodLabel: g.periodLabel, metric: g.metric, lowMicros: g.lowValue == null ? null : M.toCanonical(BigInt(g.lowValue) * 1000000n),
      highMicros: g.highValue == null ? null : M.toCanonical(BigInt(g.highValue) * 1000000n), unit: g.unit, issuedAt: g.issuedAt,
      claimId: g.claimId, documentVersionId: g.documentVersionId, supersedes: g.supersedes } : null,
    expectations: { revision30dBps: null, coverage: "no_consensus_vendor" },
    nextEarnings: version.nextEarnings ? { date: version.nextEarnings.date, confirmed: version.nextEarnings.confirmed,
      claimId: version.nextEarnings.claimId, sourceClass: version.nextEarnings.sourceClass } : null,
    changes: (changes || []).slice(0, MAX_CHANGES_ON_CARD).map((c) => ({ eventId: c.eventId || c.deltaId, type: c.form || c.eventClass || c.type || null,
      eventClass: c.eventClass || null, safetyClass: c.safetyClass || null, managerMateriality: c.managerMateriality || "pending" })),
    standingView: standingView ? { status: standingView.status || null, researchVersion: standingView.researchVersion || null,
      ageTradingDays: standingView.ageTradingDays == null ? null : standingView.ageTradingDays, decision: standingView.decision || null } : { status: null, researchVersion: null, ageTradingDays: null, decision: null },
    portfolio: { held: !!(portfolio && portfolio.held), activeMandate: !!(portfolio && portfolio.activeMandate),
      pending: !!(portfolio && portfolio.pending) },
    dataQuality: { complete: missing.length === 0, missing: [...new Set(missing)], note: version.dataQuality && version.dataQuality.note || "no consensus vendor by design; forward view is issuer guidance" },
    dossierVersionId: versionDocId(version), dossierHash: version.contentHash,
  };
  if (g && g.metric === "revenue" && fu.revenueTtmMinor && price && price.ok && fu.sharesOutstanding) {
    /* the one forward number the filings actually give: price / guided revenue per share midpoint */
    try {
      const mid = (BigInt(g.lowValue) + BigInt(g.highValue)) / 2n;                       // whole units
      const mcapMinor = BigInt(price.closeMicros) * BigInt(fu.sharesOutstanding) / 10000n; // micros×shares → minor
      if (mid > 0n) card.valuation.forwardMultipleMilli = M.toCanonical(mcapMinor * 1000n / (mid * 100n));
      card.valuation.forwardBasis = "price_to_guided_revenue_midpoint";
    } catch { /* leave null */ }
  }
  card.tokenEstimate = Math.ceil(JSON.stringify(card).length / CHARS_PER_TOKEN_ESTIMATE);
  return card;
}
function methodHintsFor(block) {
  return { financials: ["p_tbv", "p_e", "roe_cost_of_equity"], software: ["ev_revenue", "rule_of_40_context", "dcf"],
    resources: ["ev_ebitda", "nav_reserves", "fcf_yield"], life_sciences: ["rnav", "cash_runway", "ev_revenue"],
    industrial: ["ev_ebitda", "p_e", "fcf_yield"], general: ["ev_ebitda", "dcf"] }[block] || ["ev_ebitda", "dcf"];
}
/** PURE. price / trailing diluted EPS in milli, or null with no invention. */
function trailingMultipleMilli(version, price) {
  const fu = version.fundamentals || {};
  if (!price || !price.ok || fu.epsTrailingMicros == null) return null;
  const eps = BigInt(fu.epsTrailingMicros);
  if (eps <= 0n) return null;
  return M.toCanonical(BigInt(price.closeMicros) * 1000n / eps);
}

/* ── storage ───────────────────────────────────────────────────────────── */
function db(admin) { return admin || A; }
async function current(symbol, { admin = null } = {}) {
  const D = db(admin);
  const s = await D.col(D.COL.dossiers).doc(String(symbol).toUpperCase()).get();
  return s.exists ? s.data() : null;
}
async function readVersion(versionId, { admin = null } = {}) {
  const D = db(admin);
  const s = await D.col(D.COL.dossierVersions).doc(String(versionId)).get();
  if (!s.exists) return null;
  const d = s.data();
  if (d && d._codec) { try { return require("./_investorStorageCodec").decode(d); } catch { return null; } }
  return d;
}
/** Append a version only when the facts hash changed; the pointer stays small. */
async function persistVersion(version, { admin = null, sourceAtMs = null } = {}) {
  const D = db(admin);
  const SC = require("./_investorStorageCodec");
  const pointerRef = D.col(D.COL.dossiers).doc(version.symbol);
  const result = await D.runTransaction(async (tx) => {
    const cur = await tx.get(pointerRef);
    const pointer = cur.exists ? cur.data() : {};
    if (pointer.contentHash === version.contentHash) {
      tx.set(pointerRef, { asOfMs: version.asOfMs, lastCheckedAtMs: Date.now(), lastSourceAtMs: sourceAtMs || pointer.lastSourceAtMs || null,
        unchangedRuns: (Number(pointer.unchangedRuns) || 0) + 1 }, { merge: true });
      return { appended: false, versionId: pointer.currentVersionId, version: pointer.version, contentHash: pointer.contentHash };
    }
    const next = { ...version, version: (Number(pointer.version) || 0) + 1 };
    const versionId = versionDocId(next);
    const vref = D.col(D.COL.dossierVersions).doc(versionId);
    let doc = { ...next, versionId, previousVersionId: pointer.currentVersionId || null, ...D.envelope({ created_by: "dossier.persistVersion" }) };
    try { doc = SC.encode(doc); } catch (e) { throw Object.assign(new Error(`dossier version rejected by codec: ${e.message}`), { code: "DOSSIER_CODEC_REJECTED" }); }
    tx.set(vref, doc);
    tx.set(pointerRef, {
      symbol: next.symbol, currentVersionId: versionId, previousVersionId: pointer.currentVersionId || null,
      version: next.version, contentHash: next.contentHash, asOfMs: next.asOfMs, builderVersion: BUILDER_VERSION,
      sector: next.identity.sector, block: next.sectorBlock.block, cik: next.identity.cik,
      dataQuality: next.dataQuality, lastSourceAtMs: sourceAtMs || pointer.lastSourceAtMs || null,
      lastCheckedAtMs: Date.now(), unchangedRuns: 0, updatedAtMs: Date.now(),
      pendingDeltaCount: Number(pointer.pendingDeltaCount) || 0, recentDeltaIds: pointer.recentDeltaIds || [],
      lastManagerReviewAtMs: pointer.lastManagerReviewAtMs || null, lastMandateAtMs: pointer.lastMandateAtMs || null,
      lastMarketMarkAtMs: pointer.lastMarketMarkAtMs || null, standingView: pointer.standingView || null,
      ...D.envelope({ created_by: "dossier.persistVersion" }),
    }, { merge: true });
    return { appended: true, versionId, version: next.version, contentHash: next.contentHash, previousVersionId: pointer.currentVersionId || null };
  });
  return result;
}

/** Appendix B: every verified delta reaches the next Sol packet. The delta
 *  document itself is written by the router; here the pointer learns of it. */
async function recordRoutineDelta(match, { admin = null } = {}) {
  const D = db(admin);
  if (!match || !match.symbol || !match.deltaId) return { recorded: false, reason: "no_delta" };
  const ref = D.col(D.COL.dossiers).doc(String(match.symbol).toUpperCase());
  await D.runTransaction(async (tx) => {
    const cur = await tx.get(ref);
    const p = cur.exists ? cur.data() : {};
    const recent = [...(p.recentDeltaIds || []).filter((id) => id !== match.deltaId), match.deltaId].slice(-12);
    tx.set(ref, { symbol: String(match.symbol).toUpperCase(), pendingDeltaCount: (Number(p.pendingDeltaCount) || 0) + 1,
      recentDeltaIds: recent, lastDeltaAtMs: Number(match.firstSeenAt) || Date.now(), lastDeltaEventClass: match.eventClass || null,
      lastDeltaSafetyClass: match.safetyClass || null, lastSourceAtMs: Number(match.firstSeenAt) || Date.now(), updatedAtMs: Date.now() }, { merge: true });
  });
  return { recorded: true, deltaId: match.deltaId };
}

/** Deltas still awaiting the manager's materiality call, visible at the cutoff. */
async function pendingChanges(symbol, { cutoffMs = Date.now(), admin = null, limit = 50 } = {}) {
  const D = db(admin);
  const snap = await D.col(D.COL.evidenceDeltas).where("symbol", "==", String(symbol).toUpperCase()).where("managerMateriality", "==", "pending").get();
  const rows = [];
  snap.forEach((d) => { const x = d.data(); if (Number(x.inclusionAtMs ?? x.firstSeenAtMs ?? x.createdAtMs) <= Number(cutoffMs)) rows.push(x); });
  rows.sort((a, b) => (Number(b.inclusionAtMs) || 0) - (Number(a.inclusionAtMs) || 0));
  return rows.slice(0, limit);
}
/** The manager records its materiality call; the pointer's pending count drops. */
async function markDeltasReviewed({ symbol, deltaIds = [], managerRunId, materiality = "reviewed", admin = null }) {
  const D = db(admin);
  let n = 0;
  for (const id of deltaIds) {
    await D.col(D.COL.evidenceDeltas).doc(id).set({ managerMateriality: materiality, reviewedAtMs: Date.now(), reviewedByRunId: managerRunId || null }, { merge: true });
    n += 1;
  }
  if (n) {
    const ref = D.col(D.COL.dossiers).doc(String(symbol).toUpperCase());
    await D.runTransaction(async (tx) => {
      const cur = await tx.get(ref);
      const p = cur.exists ? cur.data() : {};
      tx.set(ref, { pendingDeltaCount: Math.max(0, (Number(p.pendingDeltaCount) || 0) - n), lastManagerReviewAtMs: Date.now() }, { merge: true });
    });
  }
  return { reviewed: n };
}

/* ── building from live state ──────────────────────────────────────────── */
/** Read everything the facts layer needs, at the cutoff, and compose. */
async function buildBaseline({ symbol, rosterRow = null, asOfMs = Date.now(), admin = null, deps = {} } = {}) {
  const sym = String(symbol || "").toUpperCase();
  const E = deps.evidence || require("./_investorEvidence");
  const F = deps.fundamentals || require("./_investorFundamentals");
  const U = deps.universe || require("./_investorUniverse");
  const row = rosterRow || (U.tradeTier || []).find((r) => r.symbol === sym) || (U.researchTier || []).find((r) => r.symbol === sym) || { symbol: sym };
  const identity = { symbol: sym, name: row.company || row.name || null, sector: row.sector || null, cik: row.cik ? F.cik10Of(row.cik) : null, sic: row.sic || null };
  let facts = [], fundamentals = null, factsError = null;
  if (identity.cik) {
    try {
      const block = SECTOR_BLOCKS[sectorBlockFor(identity.sector)];
      const concepts = [...new Set([...F.DEFAULT_CONCEPTS, ...Object.values(block.metrics).flatMap((m) => m.concepts || [])])];
      const r = await F.getFilingFactsAsOf({ cik: identity.cik, concepts, asOfMs, limit: 2000 });
      facts = r.facts || [];
      fundamentals = F.deriveMetrics(facts, { asOfMs });
    } catch (e) { factsError = String(e.code || e.message).slice(0, 120); }
  }
  const claims = await E.claimsForCompany(sym, { asOfMs, admin }).catch(() => []);
  const documents = await E.documentsForCompany(sym, asOfMs, LOOKBACK_DAYS, 260).catch(() => []);
  const pointer = await current(sym, { admin }).catch(() => null);
  const pointers = { researchMemoId: pointer && pointer.researchMemoId || null, researchVersion: pointer && pointer.researchVersion || null,
    managerDecisionId: pointer && pointer.managerDecisionId || null, forecastIds: pointer && pointer.forecastIds || [], mandateSeriesId: pointer && pointer.mandateSeriesId || null };
  const missing = [];
  if (!identity.cik) missing.push("identity.cik");
  if (factsError) missing.push(`facts:${factsError}`);
  const version = composeVersion({ symbol: sym, identity, asOfMs, facts, fundamentals, claims, documents, pointers,
    dataQuality: { missing }, priorVersion: pointer ? Number(pointer.version) || 0 : 0 });
  const prior = pointer && pointer.currentVersionId ? await readVersion(pointer.currentVersionId, { admin }).catch(() => null) : null;
  const delta = buildDelta(prior, version);
  return { version, delta, prior, pointer };
}

/** Card-time market inputs for a symbol, sector driver and market. */
async function marketInputs(symbol, sector, { cutoffMs, deps = {} } = {}) {
  const H = deps.history || require("./_investorHistory");
  const T = deps.temporal || require("./_investorTemporal");
  const driver = (T.DRIVER_BY_SECTOR || {})[sector] || MARKET_SYMBOL;
  const read = async (s) => { try { const d = await H.readDailyWithMeta(s); return returnsBps(d.series || [], { asOfMs: cutoffMs }); } catch { return { ok: false, reason: "read_failed", bps: {} }; } };
  const [price, sectorPrice, marketPrice] = await Promise.all([read(symbol), read(driver), driver === MARKET_SYMBOL ? null : read(MARKET_SYMBOL)]);
  return { price, sectorPrice, marketPrice: marketPrice || sectorPrice, driver };
}

/** §5.4 — one compact card per symbol at the cutoff; symbols without a
 *  dossier are reported, never silently dropped (the coverage gate needs
 *  to see them). */
async function compactCards({ symbols = [], cutoff, admin = null, deps = {}, portfolioBySymbol = {}, standingViews = {} } = {}) {
  const cutoffMs = Number(cutoff && cutoff.cutoffMs != null ? cutoff.cutoffMs : cutoff) || Date.now();
  const cards = [], missing = [];
  const marketCache = new Map();
  for (const symbol of symbols) {
    const sym = String(symbol).toUpperCase();
    const pointer = await current(sym, { admin });
    const version = pointer && pointer.currentVersionId ? await readVersion(pointer.currentVersionId, { admin }) : null;
    if (!version) { missing.push({ symbol: sym, reason: pointer ? "dossier_version_unreadable" : "no_dossier" }); continue; }
    const key = `${sym}`;
    if (!marketCache.has(key)) marketCache.set(key, await marketInputs(sym, version.identity.sector, { cutoffMs, deps }));
    const mk = marketCache.get(key);
    const changes = await pendingChanges(sym, { cutoffMs, admin }).catch(() => []);
    const card = cardFromVersion(version, { cutoffMs, price: mk.price, sectorPrice: mk.sectorPrice, marketPrice: mk.marketPrice, changes,
      standingView: standingViews[sym] || pointer.standingView || null, portfolio: portfolioBySymbol[sym] || null });
    card.freshness = freshnessMatrix(pointer, { nowMs: cutoffMs });
    cards.push(card);
  }
  return { cards, missing, cutoffMs, count: cards.length };
}

/** Holding packets: the card plus the full typed delta since the last
 *  research memo's dossier version and every pending change (§6.7). */
async function expandedHoldingDeltas({ symbols = [], cutoff, admin = null, deps = {}, portfolioBySymbol = {}, memoBySymbol = {} } = {}) {
  const cutoffMs = Number(cutoff && cutoff.cutoffMs != null ? cutoff.cutoffMs : cutoff) || Date.now();
  const packets = [], missing = [];
  for (const symbol of symbols) {
    const sym = String(symbol).toUpperCase();
    const pointer = await current(sym, { admin });
    const version = pointer && pointer.currentVersionId ? await readVersion(pointer.currentVersionId, { admin }) : null;
    if (!version) { missing.push({ symbol: sym, reason: pointer ? "dossier_version_unreadable" : "no_dossier" }); continue; }
    const memo = memoBySymbol[sym] || null;
    const baseVersionId = memo && memo.dossierVersionId || version.previousVersionId || null;
    const base = baseVersionId && baseVersionId !== (version.versionId || versionDocId(version)) ? await readVersion(baseVersionId, { admin }) : null;
    const delta = buildDelta(base, version);
    const mk = await marketInputs(sym, version.identity.sector, { cutoffMs, deps });
    const changes = await pendingChanges(sym, { cutoffMs, admin }).catch(() => []);
    const card = cardFromVersion(version, { cutoffMs, price: mk.price, sectorPrice: mk.sectorPrice, marketPrice: mk.marketPrice, changes,
      standingView: pointer.standingView || null, portfolio: { held: true, ...(portfolioBySymbol[sym] || {}) } });
    packets.push({ symbol: sym, card, delta, pendingChanges: changes, priorMemo: memo ? { memoId: memo.memoId || memo.researchMemoId || null,
      researchVersion: memo.researchVersion || null, dossierVersionId: memo.dossierVersionId || null, asOfMs: memo.asOfMs || null } : null,
      freshness: freshnessMatrix(pointer, { nowMs: cutoffMs }), dossierVersionId: version.versionId || versionDocId(version), dossierHash: version.contentHash });
  }
  return { packets, missing, cutoffMs };
}

/* ── health ────────────────────────────────────────────────────────────── */
async function dossierHealth({ symbols = [], admin = null, nowMs = Date.now() } = {}) {
  const out = { total: symbols.length, present: 0, missing: [], stale: [], complete: 0, pendingDeltas: 0, oldestAsOfMs: null };
  for (const s of symbols) {
    const p = await current(s, { admin });
    if (!p) { out.missing.push(s); continue; }
    out.present += 1;
    if (p.dataQuality && p.dataQuality.complete) out.complete += 1;
    out.pendingDeltas += Number(p.pendingDeltaCount) || 0;
    const fm = freshnessMatrix(p, { nowMs });
    if (!fm.dossier.fresh) out.stale.push({ symbol: s, ageSeconds: fm.dossier.ageSeconds });
    if (out.oldestAsOfMs === null || Number(p.asOfMs) < out.oldestAsOfMs) out.oldestAsOfMs = Number(p.asOfMs) || out.oldestAsOfMs;
  }
  return out;
}

module.exports = {
  DOSSIER_SCHEMA, CARD_SCHEMA, DELTA_SCHEMA, BUILDER_VERSION, LOOKBACK_DAYS, MAX_TOKENS_PER_CARD,
  SECTOR_BLOCKS, FRESHNESS_WINDOWS_S, sectorBlockFor, buildSectorBlock,
  returnsBps, composeVersion, buildDelta, versionDocId, freshnessMatrix, cardFromVersion, trailingMultipleMilli, methodHintsFor,
  current, readVersion, persistVersion, recordRoutineDelta, pendingChanges, markDeltasReviewed,
  buildBaseline, marketInputs, compactCards, expandedHoldingDeltas, dossierHealth,
};
