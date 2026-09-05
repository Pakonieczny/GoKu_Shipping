/*  netlify/functions/_investorValuation.js  (valuation.v1)
 *  ---------------------------------------------------------------------------
 *  Investor_AI — the deterministic valuation CALCULATOR (blueprint §6.6).
 *
 *  Sol (the model) chooses a method and declares scenario ASSUMPTIONS and
 *  PROBABILITIES; this module turns those declared inputs into numbers.
 *  It never concludes anything ("P/E below X means buy" is not a formula
 *  here) and it never trusts arithmetic the model wrote itself: any input
 *  carrying a pre-derived field (expectedValue, expectedTerminalPriceMicros,
 *  impliedReturnBps, valueMicros, quantiles) is rejected with
 *  MODEL_DERIVED_ARITHMETIC_REJECTED. Expected value, quantiles and the
 *  cost-adjusted return are derived here exactly once.
 *
 *  Methods: dcf, reverse_dcf, comparable_multiples, sum_of_the_parts,
 *  residual_income, unit_economics, event_tree — the enum of the policy's
 *  VALUATION schema. Each has an ASSUMPTION_SPECS entry naming the inputs it
 *  consumes; per-year series are written as `<name>` (constant for every
 *  year) or `<name>Y1..<name>Yn` (one entry per horizon year, n ≤ 10).
 *  Structured inputs use dotted names: `segment.<id>.metricMinor`,
 *  `branch.<id>[.<childId>].probabilityPpm` — or explicit `segments` /
 *  `branches` arrays.
 *
 *  Numeric contract (§7.1): every money / ratio value on the wire is a
 *  canonical base-10 integer STRING with the unit in the field name —
 *  ...Micros (1e-6 currency units), ...Minor (cents), ...Bps (basis points),
 *  ...Ppm (parts per million), ...Milli (1e-3). Never a JS float, never a
 *  JSON number. Internally everything is an exact rational over BigInt and
 *  is rounded ONCE, HALF_EVEN, at the output edge. Intermediates (a 10-year
 *  discount factor on cents) exceed the 2^120 bound _investorMoney places on
 *  its own rationals, so the rational arithmetic here is local and unbounded;
 *  the bound still applies to every value that leaves (toCanonical) and the
 *  local rounding is verified against money's DIV_ROUND_MATRIX in selfCheck.
 *
 *  Pure: no Firestore, no network, no clock. selfCheck() runs hand-verified
 *  cases and is deploy-gating.
 * ---------------------------------------------------------------------------
 */

"use strict";

const crypto = require("crypto");
const Money = require("./_investorMoney");

const { ROUNDING, PPM, BPS_PER_UNIT, parseInteger, toCanonical, isCanonicalIntegerString } = Money;

const VALUATION_VERSION = "valuation.v1";
const METHODS = Object.freeze(["reverse_dcf", "dcf", "comparable_multiples", "sum_of_the_parts",
  "residual_income", "unit_economics", "event_tree"]);
const MAX_HORIZON_YEARS = 10;
const MILLI = 1000n;
const DERIVED_FIELDS = Object.freeze(["expectedValue", "expectedTerminalPriceMicros", "impliedReturnBps",
  "valueMicros", "quantiles", "expectedReturnBps", "costAdjustedExpectedReturnBps"]);
const SUFFIX_UNITS = Object.freeze([["Micros", "micros"], ["Minor", "minor"], ["Bps", "bps"], ["Ppm", "ppm"], ["Milli", "milli"]]);
const DECLARED_TOLERANCE_BPS = 500n;
const REVERSE_DCF_BRACKET = Object.freeze({ lowBps: -9000n, highBps: 30000n, maxIterations: 64 });

const fail = (code, msg) => Object.assign(new Error(msg), { code });

/* ── local exact rationals (unbounded intermediates) ────────────────────── */

function gcd(a, b) {
  a = a < 0n ? -a : a; b = b < 0n ? -b : b;
  while (b !== 0n) { const t = a % b; a = b; b = t; }
  return a;
}
function q(num, den = 1n) {
  if (typeof num === "object" && num !== null && "num" in num) return num;
  let n = typeof num === "bigint" ? num : parseInteger(num, { name: "rational" });
  let d = typeof den === "bigint" ? den : parseInteger(den, { name: "rational.den" });
  if (d === 0n) throw fail("DIVIDE_BY_ZERO", "rational: denominator is zero");
  if (d < 0n) { n = -n; d = -d; }
  const g = gcd(n, d);
  return g > 1n ? { num: n / g, den: d / g } : { num: n, den: d };
}
const ZERO = q(0n), ONE = q(1n);
const qAdd = (a, b) => q(a.num * b.den + b.num * a.den, a.den * b.den);
const qSub = (a, b) => q(a.num * b.den - b.num * a.den, a.den * b.den);
const qMul = (a, b) => q(a.num * b.num, a.den * b.den);
const qDiv = (a, b) => q(a.num * b.den, a.den * b.num);
const qCmp = (a, b) => { const l = a.num * b.den, r = b.num * a.den; return l < r ? -1 : l > r ? 1 : 0; };
function qPow(base, n) { let out = ONE; for (let i = 0; i < n; i += 1) out = qMul(out, base); return out; }
/** bps → exact rate (500 → 1/20). */
const rate = (bps) => q(bps, BPS_PER_UNIT);
const onePlus = (bps) => q(BPS_PER_UNIT + bps, BPS_PER_UNIT);

/** Same rounding semantics as Money.divRound, without the intermediate bound
 *  (verified against Money.DIV_ROUND_MATRIX in selfCheck). */
function divRoundLocal(n, d, mode) {
  if (!mode || !Object.values(ROUNDING).includes(mode)) throw fail("ROUNDING_MODE_REQUIRED", "a rounding mode is required");
  if (d === 0n) throw fail("DIVIDE_BY_ZERO", "division by zero");
  if (d < 0n) { n = -n; d = -d; }
  const t = n / d, r = n % d;
  if (r === 0n) return t;
  const away = t + (n < 0n ? -1n : 1n);
  switch (mode) {
    case ROUNDING.DOWN: return t;
    case ROUNDING.UP: return away;
    case ROUNDING.FLOOR: return n < 0n ? away : t;
    case ROUNDING.CEIL: return n < 0n ? t : away;
    default: {
      const twice = (r < 0n ? -r : r) * 2n;
      if (twice > d) return away;
      if (twice < d) return t;
      if (mode === ROUNDING.HALF_UP) return away;
      if (mode === ROUNDING.HALF_DOWN) return t;
      return t % 2n === 0n ? t : away;
    }
  }
}
/** The single rounding step for a rational; result is bound-checked on output. */
const qRound = (r, mode) => divRoundLocal(r.num, r.den, mode || ROUNDING.HALF_EVEN);
const wire = (x) => toCanonical(typeof x === "bigint" ? x : qRound(x));

/* ── small helpers ──────────────────────────────────────────────────────── */

const int = (v, name) => parseInteger(v, { name });
function years(v) {
  const n = typeof v === "number" && Number.isSafeInteger(v) ? BigInt(v) : int(v, "horizonYears");
  if (n < 1n || n > BigInt(MAX_HORIZON_YEARS)) throw fail("UNIT_INVALID", `horizonYears must be 1..${MAX_HORIZON_YEARS}, got ${n}`);
  return Number(n);
}
function positive(v, name) {
  const x = int(v, name);
  if (x <= 0n) throw fail("UNIT_INVALID", `${name} must be positive, got ${x}`);
  return x;
}
/** Constant-or-array series → array of N BigInt (bps). */
function series(v, n, name) {
  if (v === undefined || v === null) throw fail("MISSING_ASSUMPTION", `${name} is required`);
  if (Array.isArray(v)) {
    if (v.length !== n) throw fail("MISSING_ASSUMPTION", `${name} needs ${n} yearly values (Y1..Y${n}), got ${v.length}`);
    return v.map((x, i) => int(x, `${name}Y${i + 1}`));
  }
  const c = int(v, name);
  return Array.from({ length: n }, () => c);
}
/** Equity (rational, minor units) → per-share micros (rational). */
function perShare(equityMinor, netDebtMinor, sharesOutstanding, minorScale) {
  const scale = minorScale === undefined || minorScale === null ? 2 : Number(minorScale);
  if (!(scale >= 0 && scale <= 6)) throw fail("UNIT_INVALID", `minorScale ${minorScale} outside 0..6`);
  const shares = positive(sharesOutstanding, "sharesOutstanding");
  const equity = qSub(equityMinor, q(netDebtMinor === undefined || netDebtMinor === null ? 0n : int(netDebtMinor, "netDebtMinor")));
  return qDiv(qMul(equity, q(10n ** BigInt(6 - scale))), q(shares));
}
function canonicalJson(v) {
  if (typeof v === "bigint") return v.toString();
  if (Array.isArray(v)) return v.map(canonicalJson);
  if (v && typeof v === "object") return Object.fromEntries(Object.keys(v).sort().map((k) => [k, canonicalJson(v[k])]));
  return v === undefined ? null : v;
}
const sha256 = (v) => crypto.createHash("sha256").update(JSON.stringify(canonicalJson(v))).digest("hex");

/** Throws MODEL_DERIVED_ARITHMETIC_REJECTED when any (nested) key is a derived field. */
function rejectDerived(value, path = "input", depth = 0) {
  if (!value || typeof value !== "object" || depth > 6) return;
  if (Array.isArray(value)) { value.forEach((v, i) => rejectDerived(v, `${path}[${i}]`, depth + 1)); return; }
  for (const k of Object.keys(value)) {
    if (DERIVED_FIELDS.includes(k)) {
      throw fail("MODEL_DERIVED_ARITHMETIC_REJECTED", `${path}.${k}: derived arithmetic must come from the calculator, not the model`);
    }
    rejectDerived(value[k], `${path}.${k}`, depth + 1);
  }
}

/* ── assumption specs (what each method consumes) ───────────────────────── */

const A = (name, unit, meaning, extra) => Object.freeze({ name, unit, meaning, ...(extra || {}) });
const COMMON = {
  horizon: A("horizonYears", "years", `explicit forecast years, 1..${MAX_HORIZON_YEARS}`),
  shares: A("sharesOutstanding", "count", "diluted shares; equity value / shares = per-share value"),
  netDebt: A("netDebtMinor", "minor", "net debt (negative = net cash) deducted from enterprise value"),
  discount: A("discountRateBps", "bps", "discount rate / WACC"),
  terminalGrowth: A("terminalGrowthBps", "bps", "perpetual growth after the horizon; must be < discount rate"),
};
const DCF_INPUTS = {
  required: [COMMON.discount, COMMON.terminalGrowth, COMMON.horizon, COMMON.shares],
  either: [["fcfTtmMinor"], ["revenueTtmMinor", "fcfMarginBps"]],
  optional: [
    A("fcfTtmMinor", "minor", "trailing free cash flow (driver grows at revenueGrowthBps)"),
    A("revenueTtmMinor", "minor", "trailing revenue; used with fcfMarginBps"),
    A("fcfMarginBps", "bps", "FCF margin on revenue, constant or Y1..Yn", { series: true }),
    COMMON.netDebt,
  ],
};
/** Per method: required / optional names with unit and meaning, `either` groups
 *  (at least one group fully present), and the two sensitivity keys. */
const ASSUMPTION_SPECS = Object.freeze({
  dcf: Object.freeze({
    ...DCF_INPUTS,
    required: [...DCF_INPUTS.required, A("revenueGrowthBps", "bps", "driver growth per year, constant or Y1..Yn", { series: true })],
    sensitivity: ["discountRateBps", "terminalGrowthBps"],
    formula: "Σ FCF_t/(1+r)^t + FCF_n(1+g)/((r−g)(1+r)^n) − netDebt, per share",
  }),
  reverse_dcf: Object.freeze({
    ...DCF_INPUTS,
    required: [...DCF_INPUTS.required, A("priceMicros", "micros", "market price the DCF must reproduce")],
    sensitivity: ["discountRateBps", "terminalGrowthBps"],
    formula: "integer bisection on constant growth g (bps) until dcf(g) brackets priceMicros",
  }),
  comparable_multiples: Object.freeze({
    required: [A("multipleMilli", "milli", "applied multiple ×1000 (12.5x = 12500)")],
    either: [["metricPerShareMicros"], ["metricMinor", "sharesOutstanding"]],
    optional: [A("metricMinor", "minor", "company-level metric (EBITDA, earnings, sales)"),
      A("metricPerShareMicros", "micros", "per-share metric (EPS, FCF/share)"), COMMON.shares, COMMON.netDebt,
      A("bandLowMultipleMilli", "milli", "historical band low"), A("bandHighMultipleMilli", "milli", "historical band high")],
    sensitivity: ["multipleMilli", "metricMinor"],
    formula: "metric × multiple/1000 − netDebt, per share; band = same at band multiples",
  }),
  sum_of_the_parts: Object.freeze({
    required: [COMMON.shares],
    optional: [COMMON.netDebt, A("segment.<id>.metricMinor", "minor", "segment metric"),
      A("segment.<id>.multipleMilli", "milli", "segment multiple ×1000")],
    sensitivity: ["netDebtMinor", "sharesOutstanding"],
    formula: "Σ segment metric × multiple/1000 − netDebt, per share",
  }),
  residual_income: Object.freeze({
    required: [A("bookValueMinor", "minor", "opening book equity"), A("roeBps", "bps", "return on opening equity per year", { series: true }),
      A("costOfEquityBps", "bps", "cost of equity"), COMMON.horizon, COMMON.shares],
    optional: [A("payoutBps", "bps", "dividend payout of earnings (default 0 = full retention)"),
      A("terminalGrowthBps", "bps", "growth of residual income after the horizon (absent = fades to zero)")],
    sensitivity: ["costOfEquityBps", "roeBps"],
    formula: "B0 + Σ B_{t−1}(ROE_t − k)/(1+k)^t + RI_n(1+g)/((k−g)(1+k)^n), per share",
  }),
  unit_economics: Object.freeze({
    required: [A("units", "count", "trailing units / customers / cohorts"), A("contributionPerUnitMinor", "minor", "contribution per unit per year"),
      A("unitGrowthBps", "bps", "unit growth per year, constant or Y1..Yn", { series: true }), A("fixedCostMinor", "minor", "annual fixed cost"),
      COMMON.horizon, COMMON.discount, A("multipleMilli", "milli", "exit multiple on year-n profit ×1000"), COMMON.shares],
    optional: [COMMON.netDebt],
    sensitivity: ["contributionPerUnitMinor", "multipleMilli"],
    formula: "Σ (units_t × contribution − fixed)/(1+r)^t + profit_n × multiple/(1+r)^n − netDebt, per share",
  }),
  event_tree: Object.freeze({
    required: [],
    optional: [A("branch.<id>.probabilityPpm", "ppm", "branch probability; each level sums to 1,000,000"),
      A("branch.<id>.terminalPriceMicros", "micros", "leaf outcome"), A("branch.<id>.<child>.probabilityPpm", "ppm", "one nesting level"),
      A("branch.<id>.<child>.terminalPriceMicros", "micros", "nested leaf outcome")],
    sensitivity: [],
    formula: "Σ p_i × (terminal_i | Σ p_ij × terminal_ij)",
  }),
});
const SPEC_UNITS = {};
for (const m of Object.values(ASSUMPTION_SPECS)) for (const a of [...m.required, ...m.optional]) SPEC_UNITS[a.name] = a.unit;

/* ── assumption parsing ─────────────────────────────────────────────────── */

const SERIES_RE = /^(.*?)Y([1-9]|10)$/;
const SEGMENT_RE = /^segment\.([A-Za-z0-9_-]+)\.(metricMinor|multipleMilli)$/;
const BRANCH_RE = /^branch\.([A-Za-z0-9_-]+)(?:\.([A-Za-z0-9_-]+))?\.(probabilityPpm|terminalPriceMicros)$/;

/** Unit implied by a name: suffix first, then the spec table; null when unknown. */
function expectedUnit(name) {
  const leaf = name.split(".").pop().replace(SERIES_RE, "$1");
  for (const [suffix, unit] of SUFFIX_UNITS) if (leaf.endsWith(suffix)) return unit;
  return SPEC_UNITS[leaf] || null;
}

/** [{ name, value, unit }] or { name: value } → Map(name → BigInt); validates
 *  canonical values and units. Later entries override earlier ones. */
function assumptionMap(list, into, where) {
  const map = into || new Map();
  if (list === undefined || list === null) return map;
  const entries = Array.isArray(list) ? list : Object.keys(list).map((name) => ({ name, value: list[name] }));
  entries.forEach((e, i) => {
    if (!e || typeof e !== "object" || typeof e.name !== "string" || !e.name) throw fail("UNIT_INVALID", `${where}[${i}]: assumption needs a name`);
    if (!isCanonicalIntegerString(e.value) && typeof e.value !== "bigint") {
      throw fail("UNIT_INVALID", `${where}[${i}] ${e.name}: value must be a canonical integer string`);
    }
    const unit = expectedUnit(e.name);
    if (e.unit !== undefined && e.unit !== null && unit && e.unit !== unit) {
      throw fail("UNIT_INVALID", `${where}[${i}] ${e.name}: unit must be "${unit}", got "${e.unit}"`);
    }
    map.set(e.name, int(e.value, e.name));
  });
  return map;
}
/** Constant or Y1..Yn series from the map → value for calculators (string | array | undefined). */
function seriesFromMap(map, name, n) {
  const yearly = Array.from({ length: n }, (_, i) => (map.has(`${name}Y${i + 1}`) ? map.get(`${name}Y${i + 1}`) : map.get(name)));
  return yearly.every((v) => v === undefined) ? undefined : yearly;
}
function segmentsFromMap(map) {
  const by = new Map();
  for (const [k, v] of map) {
    const m = SEGMENT_RE.exec(k);
    if (m) { if (!by.has(m[1])) by.set(m[1], { name: m[1] }); by.get(m[1])[m[2]] = v; }
  }
  return [...by.values()];
}
function branchesFromMap(map) {
  const top = new Map();
  for (const [k, v] of map) {
    const m = BRANCH_RE.exec(k);
    if (!m) continue;
    if (!top.has(m[1])) top.set(m[1], { id: m[1] });
    let node = top.get(m[1]);
    if (m[2]) {
      node.branches = node.branches || [];
      let child = node.branches.find((b) => b.id === m[2]);
      if (!child) { child = { id: m[2] }; node.branches.push(child); }
      node = child;
    }
    node[m[3]] = v;
  }
  return [...top.values()];
}

/** Build calculator params for `method` from a merged assumption map; throws
 *  MISSING_ASSUMPTION with the missing names. Unknown names are reported. */
function paramsFor(method, map, extras) {
  const spec = ASSUMPTION_SPECS[method];
  if (!spec) throw fail("METHOD_UNKNOWN", `unknown valuation method ${JSON.stringify(method)}`);
  const n = map.has("horizonYears") ? years(map.get("horizonYears")) : 1;
  const params = {}, missing = [], used = new Set();
  const take = (a) => {
    const v = a.series ? seriesFromMap(map, a.name, n) : map.get(a.name);
    if (v === undefined || (Array.isArray(v) && v.includes(undefined))) return false;
    params[a.name] = v; used.add(a.name);
    if (a.series) for (let t = 1; t <= n; t += 1) used.add(`${a.name}Y${t}`);
    return true;
  };
  for (const a of spec.required) if (!take(a)) missing.push(a.name);
  for (const a of spec.optional) take(a);
  if (spec.either && !spec.either.some((group) => group.every((name) => name in params))) {
    missing.push(spec.either.map((g) => g.join("+")).join(" | "));
  }
  if (method === "sum_of_the_parts") {
    params.segments = (extras && extras.segments) || segmentsFromMap(map);
    for (const k of map.keys()) if (SEGMENT_RE.test(k)) used.add(k);
    if (!params.segments.length) missing.push("segments (segment.<id>.metricMinor / multipleMilli)");
  }
  if (method === "event_tree") {
    params.branches = (extras && extras.branches) || branchesFromMap(map);
    for (const k of map.keys()) if (BRANCH_RE.test(k)) used.add(k);
    if (!params.branches.length) missing.push("branches (branch.<id>.probabilityPpm / terminalPriceMicros)");
  }
  if (missing.length) throw fail("MISSING_ASSUMPTION", `${method}: missing ${missing.join(", ")}`);
  params.unknownAssumptions = [...map.keys()].filter((k) => !used.has(k) && !SERIES_RE.test(k)).sort();
  return params;
}

/* ── calculators (pure; rational in, rational out; *Micros wrappers round once) ── */

function dcfValueRational(p) {
  const n = years(p.horizonYears);
  const r = rate(int(p.discountRateBps, "discountRateBps")), g = rate(int(p.terminalGrowthBps, "terminalGrowthBps"));
  if (qCmp(r, g) <= 0) throw fail("FORMULA_INVALID", "dcf: discountRateBps must exceed terminalGrowthBps");
  const growth = series(p.revenueGrowthBps, n, "revenueGrowthBps");
  const direct = p.fcfTtmMinor !== undefined && p.fcfTtmMinor !== null;
  let driver = q(int(direct ? p.fcfTtmMinor : p.revenueTtmMinor, direct ? "fcfTtmMinor" : "revenueTtmMinor"));
  const margins = direct ? null : series(p.fcfMarginBps, n, "fcfMarginBps");
  const disc = qAdd(ONE, r);
  let df = ONE, pv = ZERO, cf = driver;
  for (let t = 0; t < n; t += 1) {
    df = qMul(df, disc);
    driver = qMul(driver, onePlus(growth[t]));
    cf = direct ? driver : qMul(driver, rate(margins[t]));
    pv = qAdd(pv, qDiv(cf, df));
  }
  const terminal = qDiv(qMul(cf, qAdd(ONE, g)), qSub(r, g));
  return perShare(qAdd(pv, qDiv(terminal, df)), p.netDebtMinor, p.sharesOutstanding, p.minorScale);
}
/** Per-share DCF value in micros (BigInt). p.mode defaults to HALF_EVEN. */
const dcfValueMicros = (p) => qRound(dcfValueRational(p), p.mode);

/** Constant growth (bps, BigInt) at which the DCF reproduces p.priceMicros.
 *  Integer bisection over [lowBps, highBps]; value is monotone in g because
 *  terminal growth is held fixed. Throws REVERSE_DCF_UNBRACKETED. */
function reverseDcfImpliedGrowthBps(p) {
  const target = q(positive(p.priceMicros, "priceMicros"));
  let lo = p.lowBps === undefined ? REVERSE_DCF_BRACKET.lowBps : int(p.lowBps, "lowBps");
  let hi = p.highBps === undefined ? REVERSE_DCF_BRACKET.highBps : int(p.highBps, "highBps");
  const f = (g) => dcfValueRational({ ...p, revenueGrowthBps: g });
  if (qCmp(f(lo), target) >= 0 || qCmp(f(hi), target) < 0) {
    throw fail("REVERSE_DCF_UNBRACKETED", `reverse_dcf: no growth in [${lo}, ${hi}] bps reproduces ${target.num}`);
  }
  for (let i = 0; i < REVERSE_DCF_BRACKET.maxIterations && hi - lo > 1n; i += 1) {
    const mid = divRoundLocal(lo + hi, 2n, ROUNDING.FLOOR);
    if (qCmp(f(mid), target) < 0) lo = mid; else hi = mid;
  }
  const gapLo = qSub(target, f(lo)), gapHi = qSub(f(hi), target);
  return qCmp(gapLo, gapHi) < 0 ? lo : hi;
}

function comparableMultiplesRational(p) {
  const multiple = q(int(p.multipleMilli, "multipleMilli"), MILLI);
  if (p.metricPerShareMicros !== undefined && p.metricPerShareMicros !== null) {
    return qMul(q(int(p.metricPerShareMicros, "metricPerShareMicros")), multiple);
  }
  return perShare(qMul(q(int(p.metricMinor, "metricMinor")), multiple), p.netDebtMinor, p.sharesOutstanding, p.minorScale);
}
const comparableMultiplesValueMicros = (p) => qRound(comparableMultiplesRational(p), p.mode);

function sumOfThePartsRational(p) {
  if (!Array.isArray(p.segments) || !p.segments.length) throw fail("MISSING_ASSUMPTION", "sum_of_the_parts: segments are required");
  let ev = ZERO;
  p.segments.forEach((s, i) => {
    ev = qAdd(ev, qMul(q(int(s.metricMinor, `segments[${i}].metricMinor`)), q(int(s.multipleMilli, `segments[${i}].multipleMilli`), MILLI)));
  });
  return perShare(ev, p.netDebtMinor, p.sharesOutstanding, p.minorScale);
}
const sumOfThePartsValueMicros = (p) => qRound(sumOfThePartsRational(p), p.mode);

function residualIncomeRational(p) {
  const n = years(p.horizonYears);
  const k = rate(int(p.costOfEquityBps, "costOfEquityBps"));
  const roe = series(p.roeBps, n, "roeBps");
  const retain = qSub(ONE, rate(p.payoutBps === undefined || p.payoutBps === null ? 0n : int(p.payoutBps, "payoutBps")));
  let book = q(int(p.bookValueMinor, "bookValueMinor"));
  const value0 = book;
  const disc = qAdd(ONE, k);
  let df = ONE, pv = ZERO, ri = ZERO;
  for (let t = 0; t < n; t += 1) {
    df = qMul(df, disc);
    const earnings = qMul(book, rate(roe[t]));
    ri = qSub(earnings, qMul(book, k));
    pv = qAdd(pv, qDiv(ri, df));
    book = qAdd(book, qMul(earnings, retain));
  }
  if (p.terminalGrowthBps !== undefined && p.terminalGrowthBps !== null) {
    const g = rate(int(p.terminalGrowthBps, "terminalGrowthBps"));
    if (qCmp(k, g) <= 0) throw fail("FORMULA_INVALID", "residual_income: costOfEquityBps must exceed terminalGrowthBps");
    pv = qAdd(pv, qDiv(qDiv(qMul(ri, qAdd(ONE, g)), qSub(k, g)), df));
  }
  return perShare(qAdd(value0, pv), null, p.sharesOutstanding, p.minorScale);
}
const residualIncomeValueMicros = (p) => qRound(residualIncomeRational(p), p.mode);

function unitEconomicsRational(p) {
  const n = years(p.horizonYears);
  const r = rate(int(p.discountRateBps, "discountRateBps"));
  const growth = series(p.unitGrowthBps, n, "unitGrowthBps");
  const contribution = q(int(p.contributionPerUnitMinor, "contributionPerUnitMinor"));
  const fixed = q(int(p.fixedCostMinor, "fixedCostMinor"));
  let units = q(int(p.units, "units"));
  const disc = qAdd(ONE, r);
  let df = ONE, pv = ZERO, profit = ZERO;
  for (let t = 0; t < n; t += 1) {
    df = qMul(df, disc);
    units = qMul(units, onePlus(growth[t]));
    profit = qSub(qMul(units, contribution), fixed);
    pv = qAdd(pv, qDiv(profit, df));
  }
  pv = qAdd(pv, qDiv(qMul(profit, q(int(p.multipleMilli, "multipleMilli"), MILLI)), df));
  return perShare(pv, p.netDebtMinor, p.sharesOutstanding, p.minorScale);
}
const unitEconomicsValueMicros = (p) => qRound(unitEconomicsRational(p), p.mode);

/** Validated tree → { expected (rational micros), leaves:[{ path, probability, terminalPriceMicros }] }.
 *  Every level must sum to exactly PPM; at most one nesting level. */
function eventTreeRational(p, depth = 0, prefix = "") {
  const branches = p.branches;
  const chk = Money.assertPpmDistribution(branches, { requireIds: true });
  if (!chk.ok) throw fail("PROBABILITY_SUM_INVALID", `event_tree${prefix ? " " + prefix : ""}: ${chk.error.message}`);
  let expected = ZERO;
  const leaves = [];
  for (const b of branches) {
    const prob = q(int(b.probabilityPpm, `${b.id}.probabilityPpm`), PPM);
    const path = prefix ? `${prefix}.${b.id}` : b.id;
    if (Array.isArray(b.branches) && b.branches.length) {
      if (depth >= 1) throw fail("EVENT_TREE_TOO_DEEP", `event_tree: ${path} nests deeper than one level`);
      const sub = eventTreeRational({ branches: b.branches }, depth + 1, path);
      expected = qAdd(expected, qMul(prob, sub.expected));
      for (const l of sub.leaves) leaves.push({ ...l, probability: qMul(prob, l.probability) });
    } else {
      const t = q(int(b.terminalPriceMicros, `${path}.terminalPriceMicros`));
      expected = qAdd(expected, qMul(prob, t));
      leaves.push({ path, probability: prob, terminalPriceMicros: t.num });
    }
  }
  return { expected, leaves };
}
const eventTreeExpectedMicros = (p) => qRound(eventTreeRational(p).expected, p.mode);

const CALCULATORS = Object.freeze({
  dcf: dcfValueRational,
  reverse_dcf: (p) => dcfValueRational({ ...p, revenueGrowthBps: reverseDcfImpliedGrowthBps(p) }),
  comparable_multiples: comparableMultiplesRational,
  sum_of_the_parts: sumOfThePartsRational,
  residual_income: residualIncomeRational,
  unit_economics: unitEconomicsRational,
  event_tree: (p) => eventTreeRational(p).expected,
});

/* ── discrete distributions: expectation, quantiles, downside ───────────── */

/** points = [{ p: rational probability, v: rational micros }] → exact expectation,
 *  quantiles (smallest v whose cumulative probability ≥ q) and P(v < reference). */
function distribution(points, referenceMicros) {
  const sorted = [...points].sort((a, b) => qCmp(a.v, b.v));
  let expected = ZERO, downside = ZERO;
  for (const x of points) {
    expected = qAdd(expected, qMul(x.p, x.v));
    if (referenceMicros !== undefined && referenceMicros !== null && qCmp(x.v, q(referenceMicros)) < 0) downside = qAdd(downside, x.p);
  }
  const quantile = (ppm) => {
    let cum = ZERO;
    for (const x of sorted) { cum = qAdd(cum, x.p); if (qCmp(cum, q(ppm, PPM)) >= 0) return x.v; }
    return sorted[sorted.length - 1].v;
  };
  const quantiles = { p10: wire(quantile(100000n)), p50: wire(quantile(500000n)), p90: wire(quantile(900000n)) };
  return { expected, quantiles, downsidePpm: qMul(downside, q(PPM)) };
}
/** (to − from) / from in bps, exact rational; from must be positive. */
function returnRational(fromMicros, to) {
  const from = q(positive(fromMicros, "referencePriceMicros"));
  return qMul(qDiv(qSub(q(to), from), from), q(BPS_PER_UNIT));
}
/** Scenario/bucket list → validated; PPM_SUM_MISMATCH becomes PROBABILITY_SUM_INVALID. */
function checkedBuckets(list, where) {
  const chk = Money.assertPpmDistribution(list, { requireIds: true });
  if (!chk.ok) throw fail(chk.error.code === "PPM_SUM_MISMATCH" ? "PROBABILITY_SUM_INVALID" : "PROBABILITY_INVALID", `${where}: ${chk.error.message}`);
  return list;
}

/* ── sensitivity ────────────────────────────────────────────────────────── */

/** Deterministic grid over the method's two sensitivity keys: bps keys move by
 *  100 bps per step, everything else by 10 % of the base value per step.
 *  Cells are per-share micros (impliedGrowthBps for reverse_dcf); a cell whose
 *  formula is invalid at that point (e.g. r ≤ g) is null. */
function sensitivityTable({ method, assumptions, segments, branches, sharesOutstanding, priceMicros, steps } = {}) {
  const spec = ASSUMPTION_SPECS[method];
  if (!spec) throw fail("METHOD_UNKNOWN", `unknown valuation method ${JSON.stringify(method)}`);
  const map = assumptionMap(assumptions, new Map(), "assumptions");
  if (sharesOutstanding !== undefined && sharesOutstanding !== null) map.set("sharesOutstanding", int(sharesOutstanding, "sharesOutstanding"));
  if (priceMicros !== undefined && priceMicros !== null) map.set("priceMicros", int(priceMicros, "priceMicros"));
  const metric = method === "reverse_dcf" ? "impliedGrowthBps" : "perShareValueMicros";
  const [rowKey = null, colKey = null] = spec.sensitivity;
  if (!rowKey) return { metric, rowKey, colKey, rowValues: [], colValues: [], cells: [] };
  const k = Math.min(3, Math.max(1, Number.isSafeInteger(steps) ? steps : 1));
  const offsets = Array.from({ length: 2 * k + 1 }, (_, i) => BigInt(i - k));
  const baseOf = (name) => {
    const v = map.has(name) ? map.get(name) : map.get(`${name}Y1`);
    if (v === undefined) throw fail("MISSING_ASSUMPTION", `sensitivity: ${name} is required`);
    return v;
  };
  const stepOf = (name, v) => {
    if (name.endsWith("Bps")) return 100n;
    const rel = divRoundLocal(v < 0n ? -v : v, 10n, ROUNDING.HALF_EVEN);
    return rel > 0n ? rel : 1n;
  };
  const shifted = (name, delta) => {
    const m = new Map(map);
    if (m.has(name)) m.set(name, m.get(name) + delta);
    for (const key of m.keys()) if (key.startsWith(name + "Y") && SERIES_RE.test(key)) m.set(key, m.get(key) + delta);
    return m;
  };
  const rowBase = baseOf(rowKey), colBase = baseOf(colKey);
  const rowStep = stepOf(rowKey, rowBase), colStep = stepOf(colKey, colBase);
  const rowValues = offsets.map((o) => rowBase + o * rowStep), colValues = offsets.map((o) => colBase + o * colStep);
  const cells = rowValues.map((rv) => colValues.map((cv) => {
    const m = shifted(colKey, cv - colBase);
    const m2 = new Map(m);
    for (const [key, val] of shifted(rowKey, rv - rowBase)) if (key === rowKey || key.startsWith(rowKey + "Y")) m2.set(key, val);
    try {
      const params = paramsFor(method, m2, { segments, branches });
      return method === "reverse_dcf" ? toCanonical(reverseDcfImpliedGrowthBps(params)) : wire(CALCULATORS[method](params));
    } catch (e) {
      if (e.code === "FORMULA_INVALID" || e.code === "REVERSE_DCF_UNBRACKETED" || e.code === "UNIT_INVALID") return null;
      throw e;
    }
  }));
  return { metric, rowKey, colKey, rowValues: rowValues.map(toCanonical), colValues: colValues.map(toCanonical), cells };
}

/* ── run: the one entry point Sol's VALUATION block goes through ────────── */

/** { method, assumptions, scenarios:[{ id, probabilityPpm, assumptions, terminalPriceMicros? }],
 *    sharesOutstanding?, priceMicros?, segments?, branches?, facts?, forwardInputBasis?, steps? }
 *  → the calculated valuation. Scenario `terminalPriceMicros` is the model's
 *  DECLARED outcome; the calculator's `valueMicros` is what the expectation,
 *  quantiles and implied return are built from, and the declared figure is
 *  cross-checked against it (declaredDivergenceBps, ±500 bps tolerance). */
function run(input) {
  if (!input || typeof input !== "object") throw fail("INPUT_INVALID", "run: expected an input object");
  const { facts, ...authored } = input;
  rejectDerived(authored);
  const method = input.method;
  if (!METHODS.includes(method)) throw fail("METHOD_UNKNOWN", `unknown valuation method ${JSON.stringify(method)}`);
  if (!Array.isArray(input.scenarios) || !input.scenarios.length) throw fail("MISSING_ASSUMPTION", "run: scenarios are required");
  checkedBuckets(input.scenarios, "scenarios");
  const shared = assumptionMap(input.assumptions, new Map(), "assumptions");
  if (input.sharesOutstanding !== undefined && input.sharesOutstanding !== null) shared.set("sharesOutstanding", int(input.sharesOutstanding, "sharesOutstanding"));
  const price = input.priceMicros === undefined || input.priceMicros === null ? null : positive(input.priceMicros, "priceMicros");
  if (price !== null && method === "reverse_dcf" && !shared.has("priceMicros")) shared.set("priceMicros", price);
  const notes = [], assumptionsUsed = {}, unknown = new Set();
  let declaredOk = true;

  const evaluated = input.scenarios.map((sc) => {
    const map = assumptionMap(sc.assumptions, new Map(shared), `scenarios.${sc.id}.assumptions`);
    const extras = { segments: sc.segments || input.segments, branches: sc.branches || input.branches };
    const params = paramsFor(method, map, extras);
    params.unknownAssumptions.forEach((k) => unknown.add(k));
    const impliedGrowthBps = method === "reverse_dcf" ? reverseDcfImpliedGrowthBps(params) : null;
    const value = method === "reverse_dcf" ? dcfValueRational({ ...params, revenueGrowthBps: impliedGrowthBps }) : CALCULATORS[method](params);
    const valueMicros = qRound(value);
    const used = [...map].map(([name, v]) => ({ name, value: toCanonical(v), unit: expectedUnit(name) })).sort((a, b) => (a.name < b.name ? -1 : 1));
    assumptionsUsed[sc.id] = used;
    const declared = sc.terminalPriceMicros === undefined || sc.terminalPriceMicros === null ? null : positive(sc.terminalPriceMicros, `scenarios.${sc.id}.terminalPriceMicros`);
    const divergence = declared === null ? null : qRound(returnRational(declared, valueMicros));
    if (divergence !== null && (divergence > DECLARED_TOLERANCE_BPS || divergence < -DECLARED_TOLERANCE_BPS)) {
      declaredOk = false;
      notes.push(`scenario ${sc.id}: declared terminal ${declared} diverges ${divergence} bps from calculated ${valueMicros}`);
    }
    const inputsHash = sha256({ valuationVersion: VALUATION_VERSION, method, scenarioId: sc.id, assumptions: used,
      segments: params.segments || null, branches: params.branches || null });
    return { id: sc.id, probabilityPpm: toCanonical(int(sc.probabilityPpm, "probabilityPpm")), terminalPriceMicros: toCanonical(valueMicros),
      valueMicros: toCanonical(valueMicros), declaredTerminalPriceMicros: declared === null ? null : toCanonical(declared),
      declaredDivergenceBps: divergence === null ? null : toCanonical(divergence),
      impliedGrowthBps: impliedGrowthBps === null ? null : toCanonical(impliedGrowthBps), inputsHash, _p: q(BigInt(sc.probabilityPpm), PPM), _v: value, _map: map, _extras: extras };
  });

  let base = evaluated.find((s) => s.id === "base");
  if (!base) { base = evaluated.reduce((a, b) => (qCmp(b._p, a._p) > 0 ? b : a)); notes.push(`no "base" scenario; perShareValueMicros uses "${base.id}"`); }
  const dist = distribution(evaluated.map((s) => ({ p: s._p, v: s._v })), price);
  let sensitivity = null;
  try {
    sensitivity = sensitivityTable({ method, assumptions: Object.fromEntries([...base._map].map(([k, v]) => [k, toCanonical(v)])),
      segments: base._extras.segments, branches: base._extras.branches, steps: input.steps });
  } catch (e) { notes.push(`sensitivity unavailable: ${e.code || e.message}`); }
  if (unknown.size) notes.push(`ignored assumptions: ${[...unknown].join(", ")}`);
  if (input.forwardInputBasis) notes.push(`forwardInputBasis: ${input.forwardInputBasis}`);
  return {
    ok: true, forecastEligible: method !== "reverse_dcf", interpretation: method === "reverse_dcf" ? "MARKET_IMPLIED_EXPECTATIONS_ONLY" : "INDEPENDENT_FORWARD_ASSUMPTIONS", method, valuationVersion: VALUATION_VERSION, formula: ASSUMPTION_SPECS[method].formula,
    perShareValueMicros: base.valueMicros, baseScenarioId: base.id,
    scenarios: evaluated.map(({ _p, _v, _map, _extras, ...s }) => s),
    expectedTerminalPriceMicros: wire(dist.expected),
    impliedReturnBps: price === null ? null : wire(returnRational(price, dist.expected)),
    priceMicros: price === null ? null : toCanonical(price),
    quantiles: dist.quantiles, downsideProbabilityPpm: price === null ? null : wire(dist.downsidePpm),
    sensitivity,
    checks: { probabilitiesSumToOne: true, unitsValid: true, formulasValid: true, declaredWithinTolerance: declaredOk },
    assumptionsUsed, notes,
  };
}

/* ── forecastFromScenarios: the FORECAST block's arithmetic, exactly once ── */

/** { basis:{ referencePriceMicros, returnConvention?, estimatedRoundTripCostMicrosPerShare? },
 *    outcomeBuckets? | scenarios?: [{ id, probabilityPpm, terminalPriceMicros }],
 *    fillProbabilityByExpiryPpm? (default PPM), estimatedRoundTripCostMicrosPerShare? (overrides basis),
 *    cashReturnBpsOverHorizon? (default 0) }
 *  All returns are arithmetic vs the reference price (LOG_TOTAL_RETURN is not
 *  representable exactly and is refused). The unconditional return is
 *  fill × conditional + (1 − fill) × cash, taken from the UNROUNDED conditional. */
function forecastFromScenarios(input) {
  if (!input || typeof input !== "object") throw fail("INPUT_INVALID", "forecastFromScenarios: expected an input object");
  rejectDerived(input);
  const basis = input.basis || {};
  if (basis.returnConvention === "LOG_TOTAL_RETURN") throw fail("RETURN_CONVENTION_UNSUPPORTED", "log returns are not exact; use an arithmetic convention");
  const buckets = checkedBuckets(input.outcomeBuckets || input.scenarios, "outcomeBuckets");
  const ref = positive(basis.referencePriceMicros, "basis.referencePriceMicros");
  const costRaw = input.estimatedRoundTripCostMicrosPerShare !== undefined && input.estimatedRoundTripCostMicrosPerShare !== null
    ? input.estimatedRoundTripCostMicrosPerShare : basis.estimatedRoundTripCostMicrosPerShare;
  const cost = costRaw === undefined || costRaw === null ? 0n : int(costRaw, "estimatedRoundTripCostMicrosPerShare");
  const fillPpm = input.fillProbabilityByExpiryPpm === undefined || input.fillProbabilityByExpiryPpm === null ? PPM : int(input.fillProbabilityByExpiryPpm, "fillProbabilityByExpiryPpm");
  if (fillPpm < 0n || fillPpm > PPM) throw fail("PROBABILITY_INVALID", `fillProbabilityByExpiryPpm ${fillPpm} outside [0, ${PPM}]`);
  const cash = q(input.cashReturnBpsOverHorizon === undefined || input.cashReturnBpsOverHorizon === null ? 0n : int(input.cashReturnBpsOverHorizon, "cashReturnBpsOverHorizon"));
  const points = buckets.map((b, i) => ({ p: q(int(b.probabilityPpm, `bucket[${i}].probabilityPpm`), PPM), v: q(int(b.terminalPriceMicros, `bucket[${i}].terminalPriceMicros`)) }));
  const dist = distribution(points, ref);
  const refQ = q(ref), bps = q(BPS_PER_UNIT);
  const gross = qMul(qDiv(qSub(dist.expected, refQ), refQ), bps);
  const conditional = qMul(qDiv(qSub(qSub(dist.expected, refQ), q(cost)), refQ), bps);
  const fill = q(fillPpm, PPM);
  const unconditional = qAdd(qMul(fill, conditional), qMul(qSub(ONE, fill), cash));
  return {
    ok: true, valuationVersion: VALUATION_VERSION, returnConvention: basis.returnConvention || "ARITHMETIC_PRICE_RETURN",
    referencePriceMicros: toCanonical(ref), expectedTerminalPriceMicros: wire(dist.expected),
    expectedReturnBps: wire(gross), costAdjustedExpectedReturnBps: wire(conditional),
    conditionalOnFillExpectedReturnBps: wire(conditional), unconditionalExpectedReturnBps: wire(unconditional),
    fillProbabilityByExpiryPpm: toCanonical(fillPpm), cashReturnBpsOverHorizon: wire(cash),
    estimatedRoundTripCostMicrosPerShare: toCanonical(cost),
    quantiles: dist.quantiles, downsideProbabilityPpm: wire(dist.downsidePpm),
    checks: { probabilitiesSumToOne: true, referencePricePositive: true, fillProbabilityInRange: true, arithmeticConvention: true },
  };
}

/* ── self check: hand-verified cases (deploy-gating) ────────────────────── */

const BUCKETS = [
  { id: "bear", probabilityPpm: "250000", terminalPriceMicros: "36000000" },
  { id: "base", probabilityPpm: "500000", terminalPriceMicros: "48000000" },
  { id: "bull", probabilityPpm: "250000", terminalPriceMicros: "55000000" },
];
const DCF_BASE = { fcfTtmMinor: "10000", discountRateBps: "1000", terminalGrowthBps: "0", horizonYears: "5", sharesOutstanding: "1" };
const expectThrow = (fn, code) => { try { fn(); return "no throw"; } catch (e) { return e.code; } };

/** [name, expected, actual-thunk]. Expected values are worked by hand:
 *  · FCF $100, g 0 %, r 10 %, g∞ 0, 5 y: annuity + perpetuity = 100/0.1 = $1000 exactly.
 *  · FCF $100, g 5 % ×2 y, r 10 %, g∞ 2 %: 1050/11 + 11025/121 + (110.25×1.02/0.08)/1.21
 *    = 652575/484 = 1348.2954545… → 1348295454.545… micros → HALF_EVEN 1348295455.
 *  · Event tree: 0.6×50 + 0.4×(0.5×20 + 0.5×40) = 42.
 *  · Forecast: E = 46.75, ref 40, cost 0.10 → gross 1687.5→1688, net 1662.5→1662;
 *    fill 50 %, cash 50 bps → 0.5×1662.5 + 0.5×50 = 856.25 → 856. */
const CASES = [
  ["dcf annuity+perpetuity = 1000", "1000000000", () => toCanonical(dcfValueMicros({ ...DCF_BASE, revenueGrowthBps: "0" }))],
  ["dcf 2y growth+terminal", "1348295455", () => toCanonical(dcfValueMicros({ ...DCF_BASE, horizonYears: 2, revenueGrowthBps: "500", terminalGrowthBps: "200" }))],
  ["dcf revenue×margin equals fcf path", "1000000000", () => toCanonical(dcfValueMicros({ ...DCF_BASE, fcfTtmMinor: undefined, revenueTtmMinor: "100000", fcfMarginBps: "1000", revenueGrowthBps: "0" }))],
  ["dcf r ≤ g rejected", "FORMULA_INVALID", () => expectThrow(() => dcfValueMicros({ ...DCF_BASE, terminalGrowthBps: "1000", revenueGrowthBps: "0" }))],
  ["reverse dcf round trip within 1 bp", true, () => {
    const p = { ...DCF_BASE, terminalGrowthBps: "200", sharesOutstanding: "1000" };
    const price = toCanonical(dcfValueMicros({ ...p, revenueGrowthBps: "500" }));
    const g = reverseDcfImpliedGrowthBps({ ...p, priceMicros: price });
    return g >= 499n && g <= 501n;
  }],
  ["reverse dcf unbracketed", "REVERSE_DCF_UNBRACKETED", () => expectThrow(() => reverseDcfImpliedGrowthBps({ ...DCF_BASE, priceMicros: "1" }))],
  ["comparable multiples 12.5x on $2 EPS", "25000000", () => toCanonical(comparableMultiplesValueMicros({ metricPerShareMicros: "2000000", multipleMilli: "12500" }))],
  ["sum of the parts ($10k EV − $1k debt)/100 sh", "90000000", () => toCanonical(sumOfThePartsValueMicros({ segments: [{ metricMinor: "100000", multipleMilli: "8000" }, { metricMinor: "50000", multipleMilli: "4000" }], netDebtMinor: "100000", sharesOutstanding: "100" }))],
  ["residual income ROE = k adds nothing", "50000000", () => toCanonical(residualIncomeValueMicros({ bookValueMinor: "500000", roeBps: "1000", costOfEquityBps: "1000", horizonYears: 3, sharesOutstanding: "100" }))],
  ["unit economics 1y", "20000000", () => toCanonical(unitEconomicsValueMicros({ units: "100", contributionPerUnitMinor: "1000", unitGrowthBps: "0", fixedCostMinor: "50000", horizonYears: 1, discountRateBps: "0", multipleMilli: "3000", sharesOutstanding: "100" }))],
  ["event tree expected value exact", "42000000", () => toCanonical(eventTreeExpectedMicros({ branches: [
    { id: "A", probabilityPpm: "600000", terminalPriceMicros: "50000000" },
    { id: "B", probabilityPpm: "400000", branches: [{ id: "B1", probabilityPpm: "500000", terminalPriceMicros: "20000000" }, { id: "B2", probabilityPpm: "500000", terminalPriceMicros: "40000000" }] }] }))],
  ["event tree level sum rejected", "PROBABILITY_SUM_INVALID", () => expectThrow(() => eventTreeExpectedMicros({ branches: [{ id: "A", probabilityPpm: "600000", terminalPriceMicros: "1" }] }))],
  ["probability sum rejected", "PROBABILITY_SUM_INVALID", () => expectThrow(() => run({ method: "dcf", assumptions: DCF_BASE,
    scenarios: [{ id: "base", probabilityPpm: "500000", assumptions: [] }, { id: "bull", probabilityPpm: "499999", assumptions: [] }] }))],
  ["model-derived arithmetic rejected", "MODEL_DERIVED_ARITHMETIC_REJECTED", () => expectThrow(() => run({ method: "dcf", assumptions: DCF_BASE,
    scenarios: [{ id: "base", probabilityPpm: "1000000", assumptions: [], valueMicros: "1" }] }))],
  ["missing assumption", "MISSING_ASSUMPTION", () => expectThrow(() => run({ method: "dcf", assumptions: { discountRateBps: "1000" },
    scenarios: [{ id: "base", probabilityPpm: "1000000", assumptions: [] }] }))],
  ["unit mismatch", "UNIT_INVALID", () => expectThrow(() => run({ method: "dcf", assumptions: DCF_BASE,
    scenarios: [{ id: "base", probabilityPpm: "1000000", assumptions: [{ name: "revenueGrowthBps", value: "0", unit: "ppm" }] }] }))],
  ["run dcf scenarios end to end", "1348295455|1000000000|1348295455|1174147727", () => {
    const out = run({ method: "dcf", assumptions: { ...DCF_BASE, terminalGrowthBps: "200" }, priceMicros: "1000000000",
      scenarios: [{ id: "bear", probabilityPpm: "500000", assumptions: [{ name: "revenueGrowthBps", value: "0", unit: "bps" }, { name: "terminalGrowthBps", value: "0", unit: "bps" }] },
        { id: "base", probabilityPpm: "500000", assumptions: { revenueGrowthBps: "500", horizonYears: "2" } }] });
    return [out.perShareValueMicros, out.scenarios[0].valueMicros, out.scenarios[1].valueMicros, out.expectedTerminalPriceMicros].join("|");
  }],
  ["forecast unconditional formula exact", "46750000|1688|1662|856|250000", () => {
    const f = forecastFromScenarios({ basis: { referencePriceMicros: "40000000" }, outcomeBuckets: BUCKETS,
      fillProbabilityByExpiryPpm: "500000", estimatedRoundTripCostMicrosPerShare: "100000", cashReturnBpsOverHorizon: "50" });
    return [f.expectedTerminalPriceMicros, f.expectedReturnBps, f.costAdjustedExpectedReturnBps, f.unconditionalExpectedReturnBps, f.downsideProbabilityPpm].join("|");
  }],
  ["forecast bucket sum rejected", "PROBABILITY_SUM_INVALID", () => expectThrow(() => forecastFromScenarios({ basis: { referencePriceMicros: "1" }, outcomeBuckets: BUCKETS.slice(0, 2) }))],
  ["quantiles on 3 buckets", "36000000|48000000|55000000", () => {
    const qs = forecastFromScenarios({ basis: { referencePriceMicros: "40000000" }, outcomeBuckets: BUCKETS }).quantiles;
    return [qs.p10, qs.p50, qs.p90].join("|");
  }],
  ["local rounding matches Money.divRound", true, () => Money.DIV_ROUND_MATRIX.every((row) =>
    Object.values(ROUNDING).every((mode) => divRoundLocal(BigInt(row[0]), BigInt(row[1]), mode) === Money.divRound(row[0], row[1], mode)))],
];

/** → { pass, failures:[{ name, expected, actual }], checks }. Deterministic. */
function selfCheck() {
  const failures = [];
  for (const [name, expected, thunk] of CASES) {
    let actual;
    try { actual = thunk(); } catch (e) { actual = { threw: e.code || null, message: e.message }; }
    if (actual !== expected) failures.push({ name, expected, actual });
  }
  return { pass: failures.length === 0, failures, checks: CASES.length };
}

module.exports = {
  VALUATION_VERSION, METHODS, ASSUMPTION_SPECS, DERIVED_FIELDS,
  run, forecastFromScenarios, sensitivityTable,
  dcfValueMicros, reverseDcfImpliedGrowthBps, comparableMultiplesValueMicros, sumOfThePartsValueMicros,
  residualIncomeValueMicros, unitEconomicsValueMicros, eventTreeExpectedMicros,
  selfCheck,
};
