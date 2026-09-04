/*  netlify/functions/_investorFundamentals.js  (v1.0)
 *  ---------------------------------------------------------------------------
 *  Investor_AI — the numerical filing plane (blueprint §5.1, §5.2, §5.4, §5.6,
 *  §6.5).
 *
 *  WHAT THIS MODULE DOES
 *  ------------------------------------------------------------------------
 *  It ingests SEC XBRL `companyfacts` (and, optionally, `frames`), normalises
 *  every reported value into an immutable FinancialFactVersion, and answers
 *  the one question a research model is allowed to ask of the numbers:
 *  "what did the filings say, as of this instant?" (getFilingFactsAsOf, the
 *  read-only tool of §6.5). Metrics for the §5.4 card are derived from those
 *  point-in-time facts with exact rational arithmetic — never a JS double,
 *  never a value the filings did not contain.
 *
 *  POINT-IN-TIME DISCIPLINE
 *  ------------------------------------------------------------------------
 *  A restated number must not be visible before the restatement was filed.
 *  Every version carries `asOfAvailableMs` (filed date at 22:00 ET — the end
 *  of EDGAR dissemination for the day; see availabilityMs) and `pointInTime`
 *  keeps, for each (taxonomy, concept, unit, period), the latest version that
 *  had been filed by the requested instant. Restatement lineage is explicit:
 *  the later version carries `supersedesFactId`, and the earlier version's
 *  `supersededByFactId` is derived at read time (stored documents are never
 *  mutated — the storage codec's content hash would break).
 *
 *  VALUE REPRESENTATION
 *  ------------------------------------------------------------------------
 *  SEC values arrive as JSON numbers. Each is converted to an exact decimal
 *  string, then stored as `valueScaled` (canonical integer string) together
 *  with `scale` (decimal places). Currency units use at least the currency's
 *  minor scale (USD → 2, i.e. cents); `shares`, `pure` and ratio units use
 *  scale 0 unless the reported value has fractional digits. A value with more
 *  than 6 fractional digits is rejected rather than rounded.
 *
 *  TTM CONVENTION (documented once, used everywhere)
 *  ------------------------------------------------------------------------
 *  Issuers file three 10-Qs and one 10-K; there is no Q4 10-Q, and cash-flow
 *  statements in 10-Qs are year-to-date. Quarterly values are therefore
 *  derived generically as "cumulative period minus the contiguous quarters
 *  already known that start on the same day": FY − Q1 − Q2 − Q3 yields Q4,
 *  9M − (Q1 + Q2) yields Q3, 6M − Q1 yields Q2. Trailing twelve months at a
 *  quarter end is an annual fact ending there when one exists, otherwise the
 *  sum of the four contiguous quarters ending there. A derived quarter lists
 *  every fact id it was computed from.
 *
 *  This module never invents an input: a metric whose inputs are missing is
 *  null, with the reason in `basis.note`.
 * ---------------------------------------------------------------------------
 */

"use strict";

const M = require("./_investorMoney");
const P = require("./_investorPolicy");

const SCHEMA_VERSION = "financial-fact.v1";
const NORMALIZER_VERSION = "companyfacts-normalizer.v1";
const TAXONOMIES = Object.freeze(["us-gaap", "dei", "ifrs-full"]);
const FISCAL_PERIODS = new Set(["FY", "Q1", "Q2", "Q3", "Q4"]);
const MAX_FRACTION_DIGITS = 6;
const SOURCE_ID = "sec.companyfacts";

/* Day-length bands (inclusive day counts) used to classify duration facts.
   13-week quarters are 91 days, 14-week quarters 98, February quarters 89/90;
   52/53-week fiscal years are 364/371 days. */
const QUARTER_DAYS = Object.freeze({ min: 80, max: 100 });
const ANNUAL_DAYS = Object.freeze({ min: 350, max: 380 });

/* Concept preference lists for the §5.4 card. The first concept with a
   usable series wins; the winner is reported in basis.concepts. */
const CONCEPTS = Object.freeze({
  revenue: ["Revenues", "RevenueFromContractWithCustomerExcludingAssessedTax", "SalesRevenueNet"],
  operatingCashFlow: ["NetCashProvidedByUsedInOperatingActivities"],
  capex: ["PaymentsToAcquirePropertyPlantAndEquipment", "PaymentsToAcquireProductiveAssets"],
  longTermDebt: ["LongTermDebt", "LongTermDebtNoncurrent"],
  debtCurrent: ["DebtCurrent", "LongTermDebtCurrent"],
  cash: ["CashAndCashEquivalentsAtCarryingValue"],
  operatingIncome: ["OperatingIncomeLoss"],
  depreciationAmortization: ["DepreciationDepletionAndAmortization", "DepreciationAndAmortization"],
  netIncome: ["NetIncomeLoss"],
  epsDiluted: ["EarningsPerShareDiluted"],
  sharesOutstanding: ["EntityCommonStockSharesOutstanding"],
});
const DEFAULT_CONCEPTS = Object.freeze([...new Set(Object.values(CONCEPTS).flat())]);

/* ── small helpers ─────────────────────────────────────────────────────── */
const DATE_RE = /^\d{4}-\d{2}-\d{2}$/;
const ACCESSION_RE = /^\d{10}-\d{2}-\d{6}$/;
const CURRENCY_RE = /^[A-Z]{3}$/;

function cik10Of(cik) {
  const digits = String(cik == null ? "" : cik).replace(/\D/g, "");
  if (!digits || digits.length > 10) return null;
  return digits.padStart(10, "0");
}
function isDate(s) {
  if (typeof s !== "string" || !DATE_RE.test(s)) return false;
  const t = Date.parse(s + "T00:00:00Z");
  return Number.isFinite(t) && new Date(t).toISOString().slice(0, 10) === s;
}
function dayNumber(dateStr) { return Math.round(Date.parse(dateStr + "T00:00:00Z") / 864e5); }
function dateOfDay(n) { return new Date(n * 864e5).toISOString().slice(0, 10); }
function nextDay(dateStr) { return dateOfDay(dayNumber(dateStr) + 1); }
function prevDay(dateStr) { return dateOfDay(dayNumber(dateStr) - 1); }
/** Inclusive day count of a duration period. */
function periodDays(start, end) { return dayNumber(end) - dayNumber(start) + 1; }
function inBand(n, band) { return n >= band.min && n <= band.max; }

/** Filed date → availability instant. SEC disseminates filings accepted up
 *  to 22:00 ET on the filing date; a fixed −04:00 offset is a deliberate
 *  simplification (EST days are treated one hour late — still never early,
 *  which is the direction that matters for point-in-time correctness). */
function availabilityMs(filedDate) {
  return Date.parse(filedDate + "T22:00:00-04:00");
}

/** SEC `val` (JSON number or decimal string) → exact decimal string, or null. */
function decimalStringOf(val) {
  let s;
  if (typeof val === "number") {
    if (!Number.isFinite(val)) return null;
    if (Number.isInteger(val) && Math.abs(val) <= Number.MAX_SAFE_INTEGER) return BigInt(val).toString();
    s = String(val);
  } else if (typeof val === "string") {
    s = val.trim();
  } else {
    return null;
  }
  const exp = /^(-?)(\d+)(?:\.(\d+))?[eE]([+-]?\d+)$/.exec(s);
  if (exp) {
    const sign = exp[1], intPart = exp[2], frac = exp[3] || "", e = Number(exp[4]);
    const digits = intPart + frac;
    const shift = e - frac.length;
    if (shift >= 0) s = sign + digits + "0".repeat(shift);
    else {
      const cut = digits.length + shift;
      s = sign + (cut > 0 ? digits.slice(0, cut) : "0") + "." + (cut > 0 ? digits.slice(cut) : "0".repeat(-cut) + digits);
    }
  }
  const m = /^(-?)(\d+)(?:\.(\d+))?$/.exec(s);
  if (!m) return null;
  const intPart = m[2].replace(/^0+(?=\d)/, "");
  const frac = (m[3] || "").replace(/0+$/, "");
  const out = (m[1] || "") + intPart + (frac ? "." + frac : "");
  return out === "-0" ? "0" : out;
}

/** Unit → { kind, baseScale }. */
function unitInfo(unit) {
  const u = String(unit || "");
  if (CURRENCY_RE.test(u)) {
    const minor = M.MINOR_SCALE_BY_CURRENCY[u];
    return { kind: "currency", baseScale: Number.isInteger(minor) ? minor : 2 };
  }
  if (u === "shares") return { kind: "shares", baseScale: 0 };
  if (u === "pure") return { kind: "pure", baseScale: 0 };
  if (u.includes("/")) return { kind: "ratio", baseScale: 0 };
  return { kind: "other", baseScale: 0 };
}

/** Exact decimal string + unit → { valueScaled, scale } or { error }. */
function scaleValue(decimal, unit) {
  const m = /^(-?)(\d+)(?:\.(\d+))?$/.exec(decimal);
  if (!m) return { error: "value_not_decimal" };
  const frac = m[3] || "";
  if (frac.length > MAX_FRACTION_DIGITS) return { error: `value_precision_exceeds_${MAX_FRACTION_DIGITS}dp` };
  const scale = Math.max(unitInfo(unit).baseScale, frac.length);
  let v = BigInt(m[2] + frac.padEnd(scale, "0"));
  if (m[1]) v = -v;
  return { valueScaled: M.toCanonical(v), scale };
}

/** Fact → exact rational value (unit-denominated, scale-free). */
function factRational(fact) {
  return M.rational(fact.valueScaled, 10n ** BigInt(fact.scale));
}
function sameValue(a, b) { return M.ratCmp(factRational(a), factRational(b)) === 0; }

/** asOfAvailableMs as a finite number, or null (frames carry none). */
function availableAtMs(f) {
  const v = f == null ? null : f.asOfAvailableMs;
  return typeof v === "number" && Number.isFinite(v) ? v : null;
}
function optionalMs(v) { return typeof v === "number" && Number.isFinite(v) ? v : null; }

function lineageKey(f) {
  return [f.taxonomy, f.concept, f.unit, f.periodStart || "I", f.periodEnd].join("|");
}
function periodKey(start, end) { return start ? `${start}/${end}` : `I/${end}`; }

/** Ordering of versions of one period: by filed date, then accession.
 *  Later in this order = later knowledge. */
function compareFiled(a, b) {
  if (a.filedDate !== b.filedDate) return a.filedDate < b.filedDate ? -1 : 1;
  if (a.accession !== b.accession) return a.accession < b.accession ? -1 : 1;
  return 0;
}
function sortFacts(facts) {
  return facts.slice().sort((a, b) =>
    (a.taxonomy < b.taxonomy ? -1 : a.taxonomy > b.taxonomy ? 1 : 0)
    || (a.concept < b.concept ? -1 : a.concept > b.concept ? 1 : 0)
    || (a.unit < b.unit ? -1 : a.unit > b.unit ? 1 : 0)
    || (a.periodEnd < b.periodEnd ? -1 : a.periodEnd > b.periodEnd ? 1 : 0)
    || ((a.periodStart || "") < (b.periodStart || "") ? -1 : (a.periodStart || "") > (b.periodStart || "") ? 1 : 0)
    || compareFiled(a, b));
}

/* ── 1. normalisation ─────────────────────────────────────────────────── */

/** Restatement lineage inside one batch: for each period, versions ordered
 *  by filing; a version whose value differs from the previous one supersedes
 *  it. A comparative re-report with the same value is not a restatement. */
function linkRestatements(facts) {
  const groups = new Map();
  for (const f of facts) {
    const k = lineageKey(f);
    if (!groups.has(k)) groups.set(k, []);
    groups.get(k).push(f);
  }
  for (const group of groups.values()) {
    group.sort(compareFiled);
    for (let i = 1; i < group.length; i += 1) {
      const prev = group[i - 1], cur = group[i];
      if (!sameValue(prev, cur)) {
        cur.supersedesFactId = prev.factId;
        prev.supersededByFactId = cur.factId;
      }
    }
  }
  return facts;
}

/** Build one fact version from one companyfacts entry. Returns
 *  { fact } or { reason }. PURE. */
function factFromEntry({ cik10, taxonomy, concept, unit, entry, retrievedAtMs, sourceUrl }) {
  if (!entry || typeof entry !== "object") return { reason: "entry_not_object" };
  const end = entry.end, start = entry.start == null ? null : entry.start;
  if (!isDate(end)) return { reason: "bad_end_date" };
  if (start !== null && !isDate(start)) return { reason: "bad_start_date" };
  if (start !== null && dayNumber(start) > dayNumber(end)) return { reason: "start_after_end" };
  const accession = typeof entry.accn === "string" ? entry.accn : null;
  if (!accession || !ACCESSION_RE.test(accession)) return { reason: "bad_accession" };
  const filed = entry.filed;
  if (!isDate(filed)) return { reason: "bad_filed_date" };
  const fy = Number(entry.fy);
  if (!Number.isInteger(fy) || fy < 1990 || fy > 2100) return { reason: "bad_fiscal_year" };
  const fp = typeof entry.fp === "string" ? entry.fp.toUpperCase() : null;
  if (!fp || !FISCAL_PERIODS.has(fp)) return { reason: "bad_fiscal_period" };
  const form = typeof entry.form === "string" && entry.form.trim() ? entry.form.trim() : null;
  if (!form) return { reason: "bad_form" };
  const decimal = decimalStringOf(entry.val);
  if (decimal === null) return { reason: "bad_value" };
  const scaled = scaleValue(decimal, unit);
  if (scaled.error) return { reason: scaled.error };
  const frame = typeof entry.frame === "string" && entry.frame ? entry.frame : null;
  const instant = start === null;
  /* Identity. periodStart is part of the hash: a 10-Q reports the 3-month
     and the year-to-date period with the same end, fy, fp and accession, and
     they are distinct facts. */
  const factId = P.sha256([cik10, taxonomy, concept, unit, fy, fp, start || "", end, accession].join("|"));
  const asOfAvailableMs = availabilityMs(filed);
  if (!Number.isFinite(asOfAvailableMs)) return { reason: "bad_filed_date" };
  const material = {
    cik: cik10, taxonomy, concept, unit, valueScaled: scaled.valueScaled, scale: scaled.scale,
    periodStart: start, periodEnd: end, instant, fiscalYear: fy, fiscalPeriod: fp,
    form, accession, filedDate: filed, frame,
  };
  const fact = {
    schemaVersion: SCHEMA_VERSION,
    factId,
    ...material,
    period: periodKey(start, end),
    asOfVersion: accession,
    isAmendment: /\/A$/i.test(form),
    asOfAvailableMs,
    retrievedAtMs: optionalMs(retrievedAtMs),
    sourceUrl: sourceUrl || null,
    contentHash: P.sha256(material),
    unitKind: unitInfo(unit).kind,
    normalizerVersion: NORMALIZER_VERSION,
    supersedesFactId: null,
    supersededByFactId: null,
  };
  return { fact };
}

/**
 * Normalise a companyfacts payload into fact versions. PURE.
 *
 * Assumed payload shape (SEC "Company Facts" API):
 *   { cik: 320193, entityName, facts: { "us-gaap": { <Concept>: { label,
 *     description, units: { <unit>: [ { start?, end, val, accn, fy, fp,
 *     form, filed, frame? } ] } } }, dei: {…}, "ifrs-full": {…} } }
 * Instants have no `start`. Only the three taxonomies in TAXONOMIES are
 * read; anything else is counted as rejected with reason "taxonomy_ignored".
 *
 * @returns {{ facts: object[], rejected: {reason,taxonomy,concept,unit,index}[],
 *             counts: {entries, facts, duplicates, rejected} }}
 */
function normalizeCompanyFacts(payload, { cik = null, retrievedAtMs = null, sourceUrl = null } = {}) {
  const cik10 = cik10Of(cik != null ? cik : payload && payload.cik);
  const rejected = [];
  if (!cik10) return { facts: [], rejected: [{ reason: "bad_cik" }], counts: { entries: 0, facts: 0, duplicates: 0, rejected: 1 } };
  const factsRoot = payload && typeof payload === "object" && payload.facts && typeof payload.facts === "object" ? payload.facts : null;
  if (!factsRoot) return { facts: [], rejected: [{ reason: "payload_has_no_facts" }], counts: { entries: 0, facts: 0, duplicates: 0, rejected: 1 } };
  const url = sourceUrl || `https://data.sec.gov/api/xbrl/companyfacts/CIK${cik10}.json`;

  const byId = new Map();
  let entries = 0, duplicates = 0;
  for (const taxonomy of Object.keys(factsRoot)) {
    if (!TAXONOMIES.includes(taxonomy)) { rejected.push({ reason: "taxonomy_ignored", taxonomy }); continue; }
    const concepts = factsRoot[taxonomy];
    if (!concepts || typeof concepts !== "object") { rejected.push({ reason: "taxonomy_not_object", taxonomy }); continue; }
    for (const concept of Object.keys(concepts)) {
      const units = concepts[concept] && concepts[concept].units;
      if (!units || typeof units !== "object") { rejected.push({ reason: "concept_has_no_units", taxonomy, concept }); continue; }
      for (const unit of Object.keys(units)) {
        const list = units[unit];
        if (!Array.isArray(list)) { rejected.push({ reason: "unit_not_array", taxonomy, concept, unit }); continue; }
        list.forEach((entry, index) => {
          entries += 1;
          const r = factFromEntry({ cik10, taxonomy, concept, unit, entry, retrievedAtMs, sourceUrl: url });
          if (r.reason) { rejected.push({ reason: r.reason, taxonomy, concept, unit, index }); return; }
          const prior = byId.get(r.fact.factId);
          if (prior) {
            duplicates += 1;
            /* Exact duplicate context: keep the latest filed (same accession
               ⇒ same filed date; the later occurrence wins deterministically). */
            if (compareFiled(r.fact, prior) >= 0) byId.set(r.fact.factId, r.fact);
            return;
          }
          byId.set(r.fact.factId, r.fact);
        });
      }
    }
  }
  const facts = sortFacts(linkRestatements([...byId.values()]));
  return { facts, rejected, counts: { entries, facts: facts.length, duplicates, rejected: rejected.length } };
}

/**
 * Normalise one SEC `frames` payload (cross-sectional: one value per issuer
 * for one concept/unit/calendar period). PURE. Frames carry no fiscal
 * period, form or filed date, so these versions have `asOfAvailableMs: null`
 * and are excluded from pointInTime — they are for cross-sectional
 * comparison only, never for the point-in-time research tool.
 *
 * Assumed shape: { taxonomy, tag, ccp, uom, pts: [{ accn, cik, entityName,
 *   loc, start?, end, val }] }.
 */
function normalizeFrames(payload, { retrievedAtMs = null, sourceUrl = null } = {}) {
  const rejected = [], facts = [];
  if (!payload || typeof payload !== "object" || !Array.isArray(payload.pts)) {
    return { facts, rejected: [{ reason: "payload_has_no_pts" }] };
  }
  const taxonomy = String(payload.taxonomy || ""), concept = String(payload.tag || ""), unit = String(payload.uom || "");
  const frame = String(payload.ccp || "") || null;
  if (!TAXONOMIES.includes(taxonomy) || !concept || !unit) return { facts, rejected: [{ reason: "bad_frame_header" }] };
  payload.pts.forEach((pt, index) => {
    const cik10 = cik10Of(pt && pt.cik);
    const end = pt && pt.end, start = pt && pt.start != null ? pt.start : null;
    const accession = pt && typeof pt.accn === "string" && ACCESSION_RE.test(pt.accn) ? pt.accn : null;
    if (!cik10 || !isDate(end) || (start !== null && !isDate(start)) || !accession) { rejected.push({ reason: "bad_point", index }); return; }
    const decimal = decimalStringOf(pt.val);
    if (decimal === null) { rejected.push({ reason: "bad_value", index }); return; }
    const scaled = scaleValue(decimal, unit);
    if (scaled.error) { rejected.push({ reason: scaled.error, index }); return; }
    const material = { cik: cik10, taxonomy, concept, unit, valueScaled: scaled.valueScaled, scale: scaled.scale,
      periodStart: start, periodEnd: end, instant: start === null, fiscalYear: null, fiscalPeriod: null,
      form: null, accession, filedDate: null, frame };
    facts.push({
      schemaVersion: SCHEMA_VERSION,
      factId: P.sha256([cik10, taxonomy, concept, unit, "frame", frame || "", start || "", end, accession].join("|")),
      ...material, period: periodKey(start, end), asOfVersion: accession, isAmendment: false,
      asOfAvailableMs: null, retrievedAtMs: optionalMs(retrievedAtMs),
      sourceUrl: sourceUrl || null, contentHash: P.sha256(material), unitKind: unitInfo(unit).kind,
      normalizerVersion: NORMALIZER_VERSION, supersedesFactId: null, supersededByFactId: null,
    });
  });
  return { facts, rejected };
}

/* ── 2. point in time ─────────────────────────────────────────────────── */

/**
 * Versions available at `asOfMs`, reduced to the latest-filed version per
 * (taxonomy, concept, unit, period). PURE. A restatement filed after
 * `asOfMs` is invisible; the original stays in force until then.
 */
function pointInTime(facts, { asOfMs } = {}) {
  const at = Number(asOfMs);
  if (!Number.isFinite(at)) throw Object.assign(new Error("pointInTime: asOfMs is required"), { code: "AS_OF_REQUIRED" });
  const latest = new Map();
  for (const f of facts || []) {
    const available = availableAtMs(f);
    if (available === null || available > at) continue;
    const k = lineageKey(f);
    const prior = latest.get(k);
    if (!prior || compareFiled(f, prior) > 0) latest.set(k, f);
  }
  return sortFacts([...latest.values()]);
}

/* ── 3. derived metrics ───────────────────────────────────────────────── */

/** Facts for the first concept (in preference order) with any facts. */
function pickConcept(facts, candidates, { taxonomy = "us-gaap", unit = null, instant = null } = {}) {
  for (const concept of candidates) {
    const rows = facts.filter((f) => f.taxonomy === taxonomy && f.concept === concept
      && (unit === null || f.unit === unit) && (instant === null || f.instant === instant));
    if (rows.length) return { concept, rows };
  }
  return { concept: null, rows: [] };
}

/**
 * Quarter series for one concept's duration facts (see the TTM convention
 * in the header). Returns { quarters: Map<end, q>, annual: Map<end, a> }
 * where q = { start, end, value: rational, factIds, derived }.
 */
function quarterSeries(rows) {
  const durations = rows.filter((f) => !f.instant && f.periodStart);
  const quarters = new Map(), annual = new Map(), cumulative = [];
  for (const f of durations) {
    const days = periodDays(f.periodStart, f.periodEnd);
    const item = { start: f.periodStart, end: f.periodEnd, value: factRational(f), factIds: [f.factId], derived: false, days };
    if (inBand(days, QUARTER_DAYS)) { if (!quarters.has(f.periodEnd)) quarters.set(f.periodEnd, item); }
    else if (days > QUARTER_DAYS.max) {
      cumulative.push(item);
      if (inBand(days, ANNUAL_DAYS) && !annual.has(f.periodEnd)) annual.set(f.periodEnd, item);
    }
  }
  /* Derive the missing last quarter of each cumulative period from the
     contiguous known quarters that start on the same day. Several passes:
     Q2 from 6M needs Q1; Q3 from 9M needs Q1 and Q2; Q4 from FY needs all. */
  for (let pass = 0; pass < 4; pass += 1) {
    let changed = false;
    for (const cum of cumulative) {
      if (quarters.has(cum.end)) continue;
      let cursor = cum.start, sum = M.rational(0n, 1n), ids = cum.factIds.slice(), ok = true;
      while (true) {
        const gap = dayNumber(cum.end) - dayNumber(cursor) + 1;
        if (inBand(gap, QUARTER_DAYS)) break;                 // cursor..cum.end is the missing quarter
        if (gap < QUARTER_DAYS.min) { ok = false; break; }
        const q = [...quarters.values()].find((x) => x.start === cursor);
        if (!q) { ok = false; break; }
        sum = M.ratAdd(sum, q.value); ids = ids.concat(q.factIds); cursor = nextDay(q.end);
      }
      if (!ok) continue;
      quarters.set(cum.end, { start: cursor, end: cum.end, value: M.ratSub(cum.value, sum), factIds: [...new Set(ids)], derived: true,
        days: dayNumber(cum.end) - dayNumber(cursor) + 1 });
      changed = true;
    }
    if (!changed) break;
  }
  return { quarters, annual };
}

/** Trailing twelve months ending at `end`: an annual fact ending there, else
 *  four contiguous quarters ending there. Null when either is unavailable. */
function ttmAt(series, end) {
  const a = series.annual.get(end);
  if (a) return { start: a.start, end: a.end, value: a.value, factIds: a.factIds.slice(), method: "annual_fact" };
  const parts = [];
  let cursor = end;
  for (let i = 0; i < 4; i += 1) {
    const q = series.quarters.get(cursor);
    if (!q) return null;
    parts.unshift(q);
    cursor = prevDay(q.start);
  }
  const span = periodDays(parts[0].start, end);
  if (!inBand(span, ANNUAL_DAYS)) return null;
  return { start: parts[0].start, end, value: parts.reduce((acc, q) => M.ratAdd(acc, q.value), M.rational(0n, 1n)),
    factIds: [...new Set(parts.flatMap((q) => q.factIds))], method: parts.some((q) => q.derived) ? "sum_of_quarters_with_derived_q" : "sum_of_quarters" };
}
function latestDurationEnd(series) {
  const ends = [...series.quarters.keys(), ...series.annual.keys()];
  return ends.length ? ends.sort().pop() : null;
}
function latestInstant(rows) {
  const inst = rows.filter((f) => f.instant);
  if (!inst.length) return null;
  return inst.slice().sort((a, b) => (a.periodEnd < b.periodEnd ? -1 : a.periodEnd > b.periodEnd ? 1 : compareFiled(a, b))).pop();
}
const HALF_EVEN = M.ROUNDING.HALF_EVEN;
function scaledCanonical(rat, scale) { return M.toCanonical(M.ratToInteger(M.ratMul(rat, M.rational(10n ** BigInt(scale), 1n)), HALF_EVEN)); }

/**
 * §5.4 card fields from point-in-time XBRL. PURE. Every numeric output is a
 * canonical integer string or null; `basis` names the concepts, periods and
 * fact ids behind each field and explains every null.
 */
function deriveMetrics(facts, { asOfMs } = {}) {
  const pit = pointInTime(facts, { asOfMs });
  const basis = { concepts: {}, periods: {}, factIds: {}, notes: [], asOfMs: Number(asOfMs),
    scales: { revenueTtmMinor: 2, netIncomeTtmMinor: 2, epsTrailingMicros: 6, sharesOutstanding: 0 },
    ttmConvention: "annual fact at period end, else four contiguous quarters; missing quarters derived as cumulative minus known contiguous quarters (FY − Q1 − Q2 − Q3)" };
  const out = { revenueGrowthBps: null, fcfMarginBps: null, netDebtEbitdaMilli: null, sharesOutstanding: null,
    epsTrailingMicros: null, netIncomeTtmMinor: null, revenueTtmMinor: null, basis };
  const note = (s) => basis.notes.push(s);
  const usd = (candidates) => pickConcept(pit, candidates, { unit: "USD", instant: false });

  /* revenue TTM and growth */
  const rev = usd(CONCEPTS.revenue);
  let revTtm = null, revEnd = null;
  if (!rev.concept) note("revenue: no us-gaap revenue concept in point-in-time facts");
  else {
    const series = quarterSeries(rev.rows);
    revEnd = latestDurationEnd(series);
    revTtm = revEnd ? ttmAt(series, revEnd) : null;
    basis.concepts.revenue = rev.concept;
    if (!revTtm) note(`revenue: no trailing-twelve-month window ending ${revEnd}`);
    else {
      out.revenueTtmMinor = scaledCanonical(revTtm.value, 2);
      basis.periods.revenueTtm = { start: revTtm.start, end: revTtm.end, method: revTtm.method };
      basis.factIds.revenueTtm = revTtm.factIds;
      const prior = ttmAt(series, prevDay(revTtm.start));
      if (!prior) note(`revenueGrowthBps: no prior-year window ending ${prevDay(revTtm.start)}`);
      else if (M.ratCmp(prior.value, M.rational(0n, 1n)) <= 0) note("revenueGrowthBps: prior-year revenue is not positive");
      else {
        out.revenueGrowthBps = M.toCanonical(M.ratToInteger(
          M.ratMul(M.ratDiv(M.ratSub(revTtm.value, prior.value), prior.value), M.rational(M.BPS_PER_UNIT, 1n)), HALF_EVEN));
        basis.periods.revenuePrior = { start: prior.start, end: prior.end, method: prior.method };
        basis.factIds.revenuePrior = prior.factIds;
      }
    }
  }

  /* free-cash-flow margin, same window as revenue */
  if (revTtm) {
    const cfo = usd(CONCEPTS.operatingCashFlow), capex = usd(CONCEPTS.capex);
    const cfoTtm = cfo.concept ? ttmAt(quarterSeries(cfo.rows), revTtm.end) : null;
    const capexTtm = capex.concept ? ttmAt(quarterSeries(capex.rows), revTtm.end) : null;
    if (!cfoTtm) note(`fcfMarginBps: no operating cash flow TTM ending ${revTtm.end}`);
    if (!capexTtm) note(`fcfMarginBps: no capex TTM ending ${revTtm.end}`);
    if (cfoTtm && capexTtm) {
      if (M.ratCmp(revTtm.value, M.rational(0n, 1n)) <= 0) note("fcfMarginBps: revenue TTM is not positive");
      else {
        out.fcfMarginBps = M.toCanonical(M.ratToInteger(
          M.ratMul(M.ratDiv(M.ratSub(cfoTtm.value, capexTtm.value), revTtm.value), M.rational(M.BPS_PER_UNIT, 1n)), HALF_EVEN));
        basis.concepts.operatingCashFlow = cfo.concept; basis.concepts.capex = capex.concept;
        basis.periods.fcf = { start: revTtm.start, end: revTtm.end, cfoMethod: cfoTtm.method, capexMethod: capexTtm.method };
        basis.factIds.fcf = [...cfoTtm.factIds, ...capexTtm.factIds];
      }
    }
  } else if (rev.concept) note("fcfMarginBps: requires the revenue TTM window");

  /* net debt / EBITDA */
  {
    const ltd = pickConcept(pit, CONCEPTS.longTermDebt, { unit: "USD", instant: true });
    const dc = pickConcept(pit, CONCEPTS.debtCurrent, { unit: "USD", instant: true });
    const cash = pickConcept(pit, CONCEPTS.cash, { unit: "USD", instant: true });
    const oi = usd(CONCEPTS.operatingIncome), da = usd(CONCEPTS.depreciationAmortization);
    const missing = [["longTermDebt", ltd], ["debtCurrent", dc], ["cash", cash], ["operatingIncome", oi], ["depreciationAmortization", da]]
      .filter(([, x]) => !x.concept).map(([k]) => k);
    if (missing.length) note(`netDebtEbitdaMilli: missing concept(s) ${missing.join(", ")}`);
    else {
      const l = latestInstant(ltd.rows), d = latestInstant(dc.rows), c = latestInstant(cash.rows);
      const bsEnd = l.periodEnd;
      if (d.periodEnd !== bsEnd || c.periodEnd !== bsEnd) note(`netDebtEbitdaMilli: balance-sheet dates differ (${l.periodEnd}, ${d.periodEnd}, ${c.periodEnd})`);
      else {
        const oiSeries = quarterSeries(oi.rows), daSeries = quarterSeries(da.rows);
        const oiEnd = latestDurationEnd(oiSeries);
        const oiTtm = oiEnd ? ttmAt(oiSeries, oiEnd) : null, daTtm = oiEnd ? ttmAt(daSeries, oiEnd) : null;
        if (!oiTtm || !daTtm) note(`netDebtEbitdaMilli: no EBITDA TTM window ending ${oiEnd}`);
        else {
          const netDebt = M.ratSub(M.ratAdd(factRational(l), factRational(d)), factRational(c));
          const ebitda = M.ratAdd(oiTtm.value, daTtm.value);
          if (M.ratCmp(ebitda, M.rational(0n, 1n)) <= 0) note("netDebtEbitdaMilli: EBITDA TTM is not positive");
          else {
            out.netDebtEbitdaMilli = M.toCanonical(M.ratToInteger(M.ratMul(M.ratDiv(netDebt, ebitda), M.rational(1000n, 1n)), HALF_EVEN));
            Object.assign(basis.concepts, { longTermDebt: ltd.concept, debtCurrent: dc.concept, cash: cash.concept,
              operatingIncome: oi.concept, depreciationAmortization: da.concept });
            basis.periods.netDebt = { balanceSheetDate: bsEnd, ebitdaStart: oiTtm.start, ebitdaEnd: oiTtm.end };
            basis.factIds.netDebtEbitda = [l.factId, d.factId, c.factId, ...oiTtm.factIds, ...daTtm.factIds];
          }
        }
      }
    }
  }

  /* shares outstanding (cover page, dei) */
  {
    const so = pickConcept(pit, CONCEPTS.sharesOutstanding, { taxonomy: "dei", unit: "shares", instant: true });
    const latest = so.concept ? latestInstant(so.rows) : null;
    if (!latest) note("sharesOutstanding: no dei:EntityCommonStockSharesOutstanding instant");
    else {
      out.sharesOutstanding = scaledCanonical(factRational(latest), 0);
      basis.concepts.sharesOutstanding = "dei:" + so.concept;
      basis.periods.sharesOutstanding = { asOf: latest.periodEnd };
      basis.factIds.sharesOutstanding = [latest.factId];
    }
  }

  /* trailing pieces for a multiple the caller prices */
  {
    const ni = usd(CONCEPTS.netIncome);
    const niSeries = ni.concept ? quarterSeries(ni.rows) : null;
    const niEnd = niSeries ? latestDurationEnd(niSeries) : null;
    const niTtm = niEnd ? ttmAt(niSeries, niEnd) : null;
    if (!niTtm) note("netIncomeTtmMinor: no net income TTM window");
    else {
      out.netIncomeTtmMinor = scaledCanonical(niTtm.value, 2);
      basis.concepts.netIncome = ni.concept;
      basis.periods.netIncomeTtm = { start: niTtm.start, end: niTtm.end, method: niTtm.method };
      basis.factIds.netIncomeTtm = niTtm.factIds;
    }
    const eps = pickConcept(pit, CONCEPTS.epsDiluted, { unit: "USD/shares", instant: false });
    const epsSeries = eps.concept ? quarterSeries(eps.rows) : null;
    const epsEnd = epsSeries ? latestDurationEnd(epsSeries) : null;
    const epsTtm = epsEnd ? ttmAt(epsSeries, epsEnd) : null;
    if (!epsTtm) note("epsTrailingMicros: no diluted EPS TTM window");
    else {
      /* Summing four quarterly diluted EPS figures is the market convention;
         it is exact only when the diluted share count is constant. */
      out.epsTrailingMicros = scaledCanonical(epsTtm.value, 6);
      basis.concepts.epsDiluted = eps.concept;
      basis.periods.epsTrailing = { start: epsTtm.start, end: epsTtm.end, method: epsTtm.method };
      basis.factIds.epsTrailing = epsTtm.factIds;
    }
  }
  basis.note = basis.notes.join("; ") || "all fields derived from point-in-time filings";
  return out;
}

/* ── 4. ingestion (network + Firestore) ───────────────────────────────── */

/* SEC fair-access: ≤ 10 requests/second across the whole operator. The
   fetch layer's token bucket already enforces 4/s for data.sec.gov; this
   module-level spacing is a second, independent floor for its own calls. */
const SEC_MIN_INTERVAL_MS = 125;
let secQueue = Promise.resolve(), secLastAt = 0;
function secThrottle() {
  const turn = secQueue.then(async () => {
    const wait = secLastAt + SEC_MIN_INTERVAL_MS - Date.now();
    if (wait > 0) await new Promise((r) => setTimeout(r, wait));
    secLastAt = Date.now();
  });
  secQueue = turn.catch(() => {});
  return turn;
}
function secUserAgent() {
  const F = require("./_investorFetch");
  return process.env.INVESTOR_SEC_USER_AGENT || F.UA;
}
function companyFactsUrl(cik10) { return `https://data.sec.gov/api/xbrl/companyfacts/CIK${cik10}.json`; }
function manifestId(cik10) { return `manifest_${cik10}`; }
const WRITE_BATCH = 400;

/** Lazily bound Firestore handles — the module loads without credentials. */
function store() {
  const A = require("./_investorAdmin");
  const SC = require("./_investorStorageCodec");
  return { A, SC, col: A.col(A.COL.financialFacts) };
}

/**
 * Fetch companyfacts for one CIK through the public-HTTP boundary, normalise,
 * and write NEW fact versions only. A per-CIK manifest document
 * (InvestorAI_FinancialFacts/manifest_<cik10>) records the payload hash and
 * the accessions already ingested: an unchanged payload is skipped outright,
 * and — because a fact id includes its accession and a filed accession never
 * changes — only facts from unseen accessions are written.
 *
 * @returns {{ cik, fetched, newVersions, skipped, rejected, payloadHash, unchanged }}
 */
async function ingestCompanyFacts({ cik, fetchImpl = null, asOfMs = Date.now() } = {}) {
  const cik10 = cik10Of(cik);
  if (!cik10) throw Object.assign(new Error("ingestCompanyFacts: cik is required"), { code: "BAD_CIK" });
  const url = companyFactsUrl(cik10);
  const { A, SC, col } = store();
  const manifestRef = col.doc(manifestId(cik10));
  const manifestSnap = await manifestRef.get();
  const manifest = manifestSnap.exists ? manifestSnap.data() : {};

  await secThrottle();
  const doFetch = fetchImpl || require("./_investorFetch").fetchPublic;
  const res = await doFetch(url, { sourceId: SOURCE_ID, accept: ["json"], timeoutMs: 60000,
    maxBytes: 64 * 1024 * 1024, headers: { "User-Agent": secUserAgent() } });
  const payloadHash = res.sha256 || SC.sha256Hex(typeof res.text === "string" ? res.text : JSON.stringify(res.json || null));
  const base = { cik: cik10, fetched: true, payloadHash, sourceUrl: url };
  if (manifest.payloadHash === payloadHash) {
    await manifestRef.set({ lastCheckedAtMs: Date.now(), lastCheckedUnchanged: true }, { merge: true });
    return { ...base, newVersions: 0, skipped: Number(manifest.versions || 0), rejected: [], unchanged: true };
  }
  const payload = res.json || (typeof res.text === "string" ? JSON.parse(res.text) : null);
  const { facts, rejected, counts } = normalizeCompanyFacts(payload, { cik: cik10, retrievedAtMs: asOfMs, sourceUrl: url });
  const known = new Set(Array.isArray(manifest.knownAccessions) ? manifest.knownAccessions : []);
  const fresh = facts.filter((f) => !known.has(f.accession));
  let written = 0;
  for (let i = 0; i < fresh.length; i += WRITE_BATCH) {
    const batch = A.batch();
    for (const f of fresh.slice(i, i + WRITE_BATCH)) {
      /* supersededByFactId is derived at read time; never stored, so a
         later restatement never has to mutate an earlier immutable version. */
      const record = { ...f, supersededByFactId: null };
      batch.set(col.doc(f.factId), { ...SC.encode(record), ...A.envelope({ created_by: "fundamentals.ingest" }) });
      written += 1;
    }
    await batch.commit();
  }
  const accessions = [...new Set([...known, ...facts.map((f) => f.accession)])].sort();
  const latestFiledDate = facts.reduce((m, f) => (f.filedDate > m ? f.filedDate : m), manifest.latestFiledDate || "");
  await manifestRef.set({
    kind: "manifest", cik: cik10, sourceId: SOURCE_ID, sourceUrl: url, payloadHash,
    knownAccessions: accessions, versions: facts.length, entries: counts.entries, rejected: rejected.length,
    latestFiledDate: latestFiledDate || null, entityName: payload && payload.entityName || null,
    normalizerVersion: NORMALIZER_VERSION, lastIngestAtMs: Date.now(), lastCheckedAtMs: Date.now(), lastCheckedUnchanged: false,
    ...A.envelope({ created_by: "fundamentals.ingest" }),
  }, { merge: true });
  return { ...base, newVersions: written, skipped: facts.length - fresh.length, rejected, unchanged: false };
}

/* ── 5. read-only research tool ───────────────────────────────────────── */

function chunk(list, n) { const out = []; for (let i = 0; i < list.length; i += n) out.push(list.slice(i, i + n)); return out; }

/** Read stored, codec-verified fact versions for one CIK. No network. */
async function readFactsForCik(cik10, { concepts = [] } = {}) {
  const { SC, col } = store();
  const wanted = [...new Set((concepts || []).map(String).filter(Boolean))];
  const queries = wanted.length
    ? chunk(wanted, 10).map((c) => col.where("cik", "==", cik10).where("schemaVersion", "==", SCHEMA_VERSION).where("concept", "in", c))
    : [col.where("cik", "==", cik10).where("schemaVersion", "==", SCHEMA_VERSION)];
  const facts = [];
  let undecodable = 0;
  for (const q of queries) {
    const snap = await q.get();
    snap.forEach((d) => {
      const data = d.data();
      if (!data || data.kind === "manifest") return;
      try { facts.push(SC.decode(data, { schemaVersion: SCHEMA_VERSION })); }
      catch { undecodable += 1; }
    });
  }
  return { facts, undecodable };
}

/**
 * §6.5 read-only tool. Facts for `cik` as they were known at `asOfMs`, with
 * the restatement lineage of everything visible at that instant. No writes,
 * no network. With no `concepts`, DEFAULT_CONCEPTS (the card's inputs) are
 * read — a whole issuer's history is thousands of documents.
 */
async function getFilingFactsAsOf({ cik, concepts = [], asOfMs, limit = 200 } = {}) {
  const cik10 = cik10Of(cik);
  if (!cik10) throw Object.assign(new Error("getFilingFactsAsOf: cik is required"), { code: "BAD_CIK" });
  const at = Number(asOfMs);
  if (!Number.isFinite(at)) throw Object.assign(new Error("getFilingFactsAsOf: asOfMs is required"), { code: "AS_OF_REQUIRED" });
  const wanted = Array.isArray(concepts) && concepts.length ? concepts : DEFAULT_CONCEPTS;
  const { facts: all, undecodable } = await readFactsForCik(cik10, { concepts: wanted });
  return { ...selectAsOf(all, { asOfMs: at, limit }), cik: cik10, undecodable };
}

/** PURE core of getFilingFactsAsOf, shared with selfCheck. */
function selectAsOf(all, { asOfMs, limit = 200 } = {}) {
  const at = Number(asOfMs);
  const known = linkRestatements(all.map((f) => ({ ...f, supersedesFactId: null, supersededByFactId: null })))
    .filter((f) => availableAtMs(f) !== null && availableAtMs(f) <= at);
  const visible = pointInTime(known, { asOfMs: at });
  const cap = Math.max(1, Math.min(2000, Number(limit) || 200));
  const lineage = sortFacts(known).map((f) => ({ factId: f.factId, accession: f.accession, filedDate: f.filedDate,
    supersedesFactId: f.supersedesFactId || null, supersededByFactId: f.supersededByFactId || null }));
  return { asOfMs: at, facts: visible.slice(0, cap), truncated: visible.length > cap, lineage };
}

/* ── 6. health ────────────────────────────────────────────────────────── */
async function financialFactsHealth({ cik } = {}) {
  const cik10 = cik10Of(cik);
  if (!cik10) return { cik: null, versions: 0, latestFiledDate: null, manifestHash: null, error: "bad_cik" };
  const { col } = store();
  const snap = await col.doc(manifestId(cik10)).get();
  const m = snap.exists ? snap.data() : {};
  return { cik: cik10, versions: Number(m.versions || 0), latestFiledDate: m.latestFiledDate || null,
    manifestHash: m.payloadHash || null, lastIngestAtMs: m.lastIngestAtMs || null, lastCheckedAtMs: m.lastCheckedAtMs || null };
}

/* ── 7. self check ────────────────────────────────────────────────────── */

/** Miniature companyfacts payload: calendar fiscal year, USD integers. */
function fixturePayload() {
  const U = (start, end, val, accn, fy, fp, form, filed) => ({ start, end, val, accn, fy, fp, form, filed });
  const I = (end, val, accn, fy, fp, form, filed) => ({ end, val, accn, fy, fp, form, filed });
  const Q1_23 = ["0000123456-23-000010", 2023, "Q1", "10-Q", "2023-05-05"];
  const Q2_23 = ["0000123456-23-000020", 2023, "Q2", "10-Q", "2023-08-04"];
  const Q3_23 = ["0000123456-23-000030", 2023, "Q3", "10-Q", "2023-11-03"];
  const FY23 = ["0000123456-24-000010", 2023, "FY", "10-K", "2024-02-15"];
  const Q1_24 = ["0000123456-24-000020", 2024, "Q1", "10-Q", "2024-05-03"];
  const Q2_24 = ["0000123456-24-000030", 2024, "Q2", "10-Q", "2024-08-02"];
  const Q3_24 = ["0000123456-24-000040", 2024, "Q3", "10-Q", "2024-11-01"];
  const FY24 = ["0000123456-25-000010", 2024, "FY", "10-K", "2025-02-14"];
  const Q1_25 = ["0000123456-25-000020", 2025, "Q1", "10-Q", "2025-05-02"];
  const FY24A = ["0000123456-25-000030", 2024, "FY", "10-K/A", "2025-06-20"];
  const usd = (list) => ({ units: { USD: list } });
  return {
    cik: 123456, entityName: "Fixture Corp",
    facts: {
      "us-gaap": {
        Revenues: usd([
          U("2023-01-01", "2023-03-31", 200, ...Q1_23), U("2023-04-01", "2023-06-30", 250, ...Q2_23), U("2023-07-01", "2023-09-30", 250, ...Q3_23),
          U("2023-01-01", "2023-12-31", 1000, ...FY23),
          U("2024-01-01", "2024-03-31", 260, ...Q1_24), U("2024-01-01", "2024-03-31", 260, ...Q1_24),   // exact duplicate context
          U("2024-04-01", "2024-06-30", 300, ...Q2_24), U("2024-07-01", "2024-09-30", 310, ...Q3_24),
          U("2024-01-01", "2024-12-31", 1200, ...FY24), U("2023-01-01", "2023-12-31", 1000, ...FY24),   // comparative, same value
          U("2025-01-01", "2025-03-31", 320, ...Q1_25),
          U("2024-01-01", "2024-12-31", 1250, ...FY24A),                                                  // restatement
          { start: "2024-01-01", end: "2024-12-31", val: 1, fy: 2024, fp: "FY", form: "10-K", filed: "2025-02-14" }, // malformed: no accn
        ]),
        NetCashProvidedByUsedInOperatingActivities: usd([
          U("2024-01-01", "2024-03-31", 50, ...Q1_24), U("2024-01-01", "2024-06-30", 110, ...Q2_24), U("2024-01-01", "2024-09-30", 180, ...Q3_24),
          U("2024-01-01", "2024-12-31", 250, ...FY24), U("2025-01-01", "2025-03-31", 60, ...Q1_25),
        ]),
        PaymentsToAcquirePropertyPlantAndEquipment: usd([
          U("2024-01-01", "2024-03-31", 10, ...Q1_24), U("2024-01-01", "2024-06-30", 25, ...Q2_24), U("2024-01-01", "2024-09-30", 40, ...Q3_24),
          U("2024-01-01", "2024-12-31", 60, ...FY24), U("2025-01-01", "2025-03-31", 15, ...Q1_25),
        ]),
        LongTermDebt: usd([I("2024-12-31", 520, ...FY24), I("2025-03-31", 500, ...Q1_25)]),
        DebtCurrent: usd([I("2024-12-31", 90, ...FY24), I("2025-03-31", 100, ...Q1_25)]),
        CashAndCashEquivalentsAtCarryingValue: usd([I("2024-12-31", 180, ...FY24), I("2025-03-31", 200, ...Q1_25)]),
        OperatingIncomeLoss: usd([
          U("2024-01-01", "2024-03-31", 40, ...Q1_24), U("2024-04-01", "2024-06-30", 50, ...Q2_24), U("2024-07-01", "2024-09-30", 55, ...Q3_24),
          U("2024-01-01", "2024-12-31", 200, ...FY24), U("2025-01-01", "2025-03-31", 45, ...Q1_25),
        ]),
        DepreciationDepletionAndAmortization: usd([
          U("2024-01-01", "2024-03-31", 10, ...Q1_24), U("2024-01-01", "2024-06-30", 20, ...Q2_24), U("2024-01-01", "2024-09-30", 30, ...Q3_24),
          U("2024-01-01", "2024-12-31", 40, ...FY24), U("2025-01-01", "2025-03-31", 10, ...Q1_25),
        ]),
        NetIncomeLoss: usd([
          U("2024-01-01", "2024-03-31", 30, ...Q1_24), U("2024-04-01", "2024-06-30", 35, ...Q2_24), U("2024-07-01", "2024-09-30", 40, ...Q3_24),
          U("2024-01-01", "2024-12-31", 150, ...FY24), U("2025-01-01", "2025-03-31", 35, ...Q1_25),
        ]),
        EarningsPerShareDiluted: { units: { "USD/shares": [
          U("2024-01-01", "2024-03-31", 0.03, ...Q1_24), U("2024-04-01", "2024-06-30", 0.035, ...Q2_24), U("2024-07-01", "2024-09-30", 0.04, ...Q3_24),
          U("2024-01-01", "2024-12-31", 0.15, ...FY24), U("2025-01-01", "2025-03-31", 0.035, ...Q1_25),
        ] } },
      },
      dei: {
        EntityCommonStockSharesOutstanding: { units: { shares: [I("2025-02-07", 1010000, ...FY24), I("2025-04-25", 1000000, ...Q1_25)] } },
      },
    },
  };
}

function selfCheck() {
  const failures = [];
  const check = (name, fn) => {
    try { const r = fn(); if (r === false) failures.push(`${name}: returned false`); }
    catch (e) { failures.push(`${name}: ${String(e && e.message || e)}`); }
  };
  const isCanon = (s) => M.isCanonicalIntegerString(s);
  const retrievedAtMs = Date.parse("2025-07-01T00:00:00Z");
  const norm = normalizeCompanyFacts(fixturePayload(), { cik: "123456", retrievedAtMs });
  const facts = norm.facts;
  const find = (concept, end, accn, start) => facts.find((f) => f.concept === concept && f.periodEnd === end && f.accession === accn && (start === undefined || f.periodStart === start));
  const BEFORE = Date.parse("2025-06-01T00:00:00Z"), AFTER = Date.parse("2025-07-01T00:00:00Z");

  check("normalisation shapes", () => {
    const f = find("Revenues", "2024-12-31", "0000123456-25-000010", "2024-01-01");
    if (!f) throw new Error("FY2024 revenue missing");
    const expect = { schemaVersion: SCHEMA_VERSION, cik: "0000123456", taxonomy: "us-gaap", unit: "USD", valueScaled: "120000", scale: 2,
      periodStart: "2024-01-01", periodEnd: "2024-12-31", instant: false, fiscalYear: 2024, fiscalPeriod: "FY", form: "10-K",
      filedDate: "2025-02-14", frame: null, isAmendment: false, period: "2024-01-01/2024-12-31", asOfVersion: "0000123456-25-000010",
      retrievedAtMs, unitKind: "currency" };
    for (const [k, v] of Object.entries(expect)) if (f[k] !== v) throw new Error(`${k}: ${JSON.stringify(f[k])} ≠ ${JSON.stringify(v)}`);
    if (!/^[0-9a-f]{64}$/.test(f.factId) || !/^[0-9a-f]{64}$/.test(f.contentHash)) throw new Error("ids must be sha256 hex");
    if (f.asOfAvailableMs !== Date.parse("2025-02-14T22:00:00-04:00")) throw new Error("asOfAvailableMs");
    if (f.sourceUrl !== "https://data.sec.gov/api/xbrl/companyfacts/CIK0000123456.json") throw new Error("sourceUrl");
    const inst = find("LongTermDebt", "2025-03-31", "0000123456-25-000020");
    if (!inst || inst.instant !== true || inst.periodStart !== null || inst.period !== "I/2025-03-31") throw new Error("instant shape");
    const eps = find("EarningsPerShareDiluted", "2024-06-30", "0000123456-24-000030");
    if (!eps || eps.valueScaled !== "35" || eps.scale !== 3 || eps.unitKind !== "ratio") throw new Error(`eps scaling ${eps && eps.valueScaled}/${eps && eps.scale}`);
    const sh = find("EntityCommonStockSharesOutstanding", "2025-04-25", "0000123456-25-000020");
    if (!sh || sh.taxonomy !== "dei" || sh.valueScaled !== "1000000" || sh.scale !== 0) throw new Error("shares shape");
    return true;
  });
  check("malformed entries are rejected with reasons", () =>
    norm.rejected.length === 1 && norm.rejected[0].reason === "bad_accession" && norm.rejected[0].concept === "Revenues");
  check("duplicate contexts collapse", () =>
    norm.counts.duplicates === 1 && facts.filter((f) => f.concept === "Revenues" && f.periodEnd === "2024-03-31").length === 1);
  check("fact ids are deterministic", () => {
    const again = normalizeCompanyFacts(fixturePayload(), { cik: "123456", retrievedAtMs });
    return again.facts.map((f) => f.factId).join() === facts.map((f) => f.factId).join();
  });
  check("amendment lineage", () => {
    const orig = find("Revenues", "2024-12-31", "0000123456-25-000010", "2024-01-01");
    const amended = find("Revenues", "2024-12-31", "0000123456-25-000030", "2024-01-01");
    if (!amended.isAmendment || amended.form !== "10-K/A") throw new Error("amendment flag");
    if (amended.supersedesFactId !== orig.factId || orig.supersededByFactId !== amended.factId) throw new Error("supersedes links");
    const comparative = find("Revenues", "2023-12-31", "0000123456-25-000010", "2023-01-01");
    const original23 = find("Revenues", "2023-12-31", "0000123456-24-000010", "2023-01-01");
    if (comparative.supersedesFactId || original23.supersededByFactId) throw new Error("same-value comparative must not be a restatement");
    return true;
  });
  check("point in time hides the restatement before its filed date and shows it after", () => {
    const pick = (at) => pointInTime(facts, { asOfMs: at }).find((f) => f.concept === "Revenues" && f.period === "2024-01-01/2024-12-31");
    const before = pick(BEFORE), after = pick(AFTER), atFiling = pick(Date.parse("2025-06-20T21:59:00-04:00"));
    if (!before || before.valueScaled !== "120000" || before.accession !== "0000123456-25-000010") throw new Error("before");
    if (!atFiling || atFiling.valueScaled !== "120000") throw new Error("not yet disseminated");
    if (!after || after.valueScaled !== "125000" || after.accession !== "0000123456-25-000030") throw new Error("after");
    if (pointInTime(facts, { asOfMs: Date.parse("2023-01-01T00:00:00Z") }).length !== 0) throw new Error("nothing before first filing");
    return true;
  });
  check("selectAsOf lineage is bounded to what was known", () => {
    const before = selectAsOf(facts, { asOfMs: BEFORE }), after = selectAsOf(facts, { asOfMs: AFTER });
    if (before.lineage.some((l) => l.accession === "0000123456-25-000030")) throw new Error("future accession leaked");
    const row = after.lineage.find((l) => l.accession === "0000123456-25-000030" && l.supersedesFactId);
    return !!row && before.facts.length > 0;
  });
  check("TTM arithmetic (FY − Q1 − Q2 − Q3, year-to-date differences)", () => {
    const pit = pointInTime(facts, { asOfMs: AFTER });
    const rev = quarterSeries(pit.filter((f) => f.concept === "Revenues"));
    const q4 = rev.quarters.get("2024-12-31");
    if (!q4 || !q4.derived || M.ratCmp(q4.value, M.rational(380n, 1n)) !== 0) throw new Error(`Q4 derived ${q4 && q4.value.num}`);
    if (q4.factIds.length !== 4) throw new Error("derived quarter must cite its four inputs");
    const ttm = ttmAt(rev, "2025-03-31");
    if (!ttm || M.ratCmp(ttm.value, M.rational(1310n, 1n)) !== 0 || ttm.start !== "2024-04-01") throw new Error("revenue TTM");
    const cfo = quarterSeries(pit.filter((f) => f.concept === "NetCashProvidedByUsedInOperatingActivities"));
    const cfoTtm = ttmAt(cfo, "2025-03-31");
    if (!cfoTtm || M.ratCmp(cfoTtm.value, M.rational(260n, 1n)) !== 0) throw new Error("YTD-derived CFO TTM");
    if (ttmAt(rev, "2024-12-31").method !== "annual_fact") throw new Error("annual fact preferred at fiscal year end");
    return true;
  });
  check("revenueGrowthBps exact", () => {
    const after = deriveMetrics(facts, { asOfMs: AFTER }), before = deriveMetrics(facts, { asOfMs: BEFORE });
    if (after.revenueGrowthBps !== "2358") throw new Error(`after ${after.revenueGrowthBps}`);   // 250/1060
    if (before.revenueGrowthBps !== "1887") throw new Error(`before ${before.revenueGrowthBps}`); // 200/1060
    if (after.revenueTtmMinor !== "131000" || before.revenueTtmMinor !== "126000") throw new Error("revenue TTM minor");
    if (after.basis.concepts.revenue !== "Revenues" || after.basis.periods.revenuePrior.end !== "2024-03-31") throw new Error("basis");
    return true;
  });
  check("fcfMarginBps exact", () => deriveMetrics(facts, { asOfMs: AFTER }).fcfMarginBps === "1489");          // 195/1310
  check("netDebtEbitdaMilli exact", () => {
    const m = deriveMetrics(facts, { asOfMs: AFTER });
    if (m.netDebtEbitdaMilli !== "1633") throw new Error(m.netDebtEbitdaMilli);                                   // 400/245
    return m.basis.periods.netDebt.balanceSheetDate === "2025-03-31" && m.basis.factIds.netDebtEbitda.length === 13;
  });
  check("shares, net income and EPS pieces", () => {
    const m = deriveMetrics(facts, { asOfMs: AFTER });
    if (m.sharesOutstanding !== "1000000") throw new Error("shares");
    if (m.netIncomeTtmMinor !== "15500") throw new Error(`net income ${m.netIncomeTtmMinor}`);
    if (m.epsTrailingMicros !== "155000") throw new Error(`eps ${m.epsTrailingMicros}`);
    return true;
  });
  check("missing inputs yield null, never invented", () => {
    const m = deriveMetrics(facts.filter((f) => f.concept !== "DebtCurrent"), { asOfMs: AFTER });
    return m.netDebtEbitdaMilli === null && /debtCurrent/.test(m.basis.note) && m.revenueGrowthBps === "2358";
  });
  check("no JS number where a canonical string is required", () => {
    for (const f of facts) {
      if (!isCanon(f.valueScaled) || typeof f.scale !== "number") throw new Error(`fact ${f.concept} value`);
      if (typeof f.asOfAvailableMs !== "number") throw new Error("asOfAvailableMs is a timestamp number");
    }
    const m = deriveMetrics(facts, { asOfMs: AFTER });
    for (const k of ["revenueGrowthBps", "fcfMarginBps", "netDebtEbitdaMilli", "sharesOutstanding", "epsTrailingMicros", "netIncomeTtmMinor", "revenueTtmMinor"]) {
      if (m[k] !== null && !isCanon(m[k])) throw new Error(`${k} is ${typeof m[k]}`);
    }
    return true;
  });
  check("value scaling refuses > 6 dp and expands exponents", () => {
    if (decimalStringOf(1.5e9) !== "1500000000" || decimalStringOf(2.5e-3) !== "0.0025" || decimalStringOf("0100.50") !== "100.5") throw new Error("decimal");
    if (!scaleValue("1.1234567", "USD").error) throw new Error("precision");
    const s = scaleValue("12.5", "USD");
    return s.valueScaled === "1250" && s.scale === 2 && scaleValue("-3", "shares").valueScaled === "-3";
  });
  check("storage codec accepts a fact version", () => {
    const SC = require("./_investorStorageCodec");
    const enc = SC.encode(facts[0]);
    const dec = SC.decode(JSON.parse(JSON.stringify(enc)));
    return dec.factId === facts[0].factId && dec.valueScaled === facts[0].valueScaled;
  });
  check("frames normalise without availability", () => {
    const r = normalizeFrames({ taxonomy: "us-gaap", tag: "Revenues", ccp: "CY2024", uom: "USD",
      pts: [{ accn: "0000123456-25-000010", cik: 123456, entityName: "Fixture", loc: "US-CA", start: "2024-01-01", end: "2024-12-31", val: 1200 }] });
    return r.facts.length === 1 && r.facts[0].frame === "CY2024" && r.facts[0].asOfAvailableMs === null
      && pointInTime(r.facts, { asOfMs: AFTER }).length === 0;
  });
  return { pass: failures.length === 0, failures };
}

module.exports = {
  SCHEMA_VERSION, NORMALIZER_VERSION, TAXONOMIES, CONCEPTS, DEFAULT_CONCEPTS, SOURCE_ID,
  QUARTER_DAYS, ANNUAL_DAYS, SEC_MIN_INTERVAL_MS,
  normalizeCompanyFacts, normalizeFrames, pointInTime, deriveMetrics, selectAsOf,
  quarterSeries, ttmAt, decimalStringOf, scaleValue, availabilityMs, companyFactsUrl, cik10Of,
  ingestCompanyFacts, getFilingFactsAsOf, financialFactsHealth,
  selfCheck,
};
