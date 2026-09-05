/*  netlify/functions/_investorMoney.js
 *  ---------------------------------------------------------------------------
 *  Investor_AI — the exact-arithmetic primitive (blueprint §7.1 numeric contract).
 *
 *  WHY. Every number the desk is audited on — a price, a cash amount, a share
 *  count, a probability, a weight or a return — has to survive three trips
 *  unchanged: into Firestore, across the HTTP boundary, and onto the screen.
 *  Binary floating point cannot make that promise (0.1 + 0.2 !== 0.3, and a
 *  JSON parser is free to lose digits above 2^53), so the ledger's old habit
 *  of `Math.round(usd * 100)` is exactly the class of bug this module exists
 *  to make impossible. The v1 rule is simple and mechanical:
 *
 *    on the wire / in storage : a canonical base-10 integer STRING
 *    in memory                : a BigInt
 *    never                    : a JS Number for any audited value
 *
 *  Representation (v1):
 *    · price       priceMicros      = USD × 1,000,000                (string)
 *    · money       amountMinor      = ISO-4217 minor units (cents)   (string)
 *                  + currency (mandatory, "USD") + minorScale (2 for USD)
 *    · quantity    quantityUnits    (string) + quantityScale (v1: must be 0)
 *    · probability probabilityPpm   ∈ [0, 1,000,000]; a full distribution
 *                  sums to EXACTLY 1,000,000
 *    · rate/weight basisPoints      signed, 100 bps = 1 %
 *    · instants    RFC 3339 UTC strings ("2026-09-04T13:30:00Z"), validated only
 *
 *  Rules enforced here:
 *    · Canonical integers only: /^-?(0|[1-9][0-9]*)$/, no "+", no whitespace,
 *      no leading zeros, and "-0" is rejected. JSON numbers are refused with
 *      code NOT_CANONICAL_INTEGER so a float can never sneak in through a
 *      forgiving parser.
 *    · Every BigInt result is bounded by MAX_MAGNITUDE (2^120); exceeding it
 *      throws MAGNITUDE_OVERFLOW instead of silently growing.
 *    · Every division states its rounding mode. There is no default — a
 *      missing mode throws ROUNDING_MODE_REQUIRED.
 *    · Rationals stay exact until the one final rounding.
 *    · Intl formatting happens ONLY at the display edge and is fed an exact
 *      decimal string, never a divided float.
 *
 *  Pure: no Firestore, no I/O, no dependencies, deterministic. selfCheck() is
 *  run by the deploy-gating fixture and must stay fast (< 50 ms).
 *
 *  Exports (see the JSDoc on each):
 *    constants   SCHEMA_VERSION, MICROS_PER_UNIT, PPM, BPS_PER_UNIT,
 *                MAX_MAGNITUDE, V1_QUANTITY_SCALE, ROUNDING, TICK_DIRECTION,
 *                CODES, MINOR_SCALE_BY_CURRENCY
 *    integers    isCanonicalIntegerString, parseInteger, fromSafeInteger,
 *                toCanonical
 *    arithmetic  add, sub, mul, neg, abs, cmp, min, max, isZero, sign, divRound
 *    rationals   rational, ratFromInteger, ratAdd, ratSub, ratMul, ratDiv,
 *                ratCmp, ratToInteger
 *    decimals    priceMicrosFromDecimal, priceMicrosToDecimal,
 *                amountMinorFromDecimal, amountMinorToDecimal,
 *                bpsToDecimalPercent
 *    values      money, price, quantity, validateMoney, validatePrice,
 *                validateQuantity, validatePpm, validateBps,
 *                assertSameCurrency, isRfc3339Utc, validateInstant
 *    notional    notionalMinor, plannedLossMinor, bpsOf, applyBps, weightBps,
 *                returnBps
 *    probability assertPpmDistribution, expectedTerminalPriceMicros,
 *                expectedReturnBps
 *    tick/lot    tickAlign, isOnTick, lotFloor, toBrokerDecimalPrice,
 *                fromBrokerDecimalPrice, assertLosslessRoundTrip
 *    firestore   sortableEncoding, fromSortable, int64Mirror
 *    display     formatMoney, formatPriceMicros
 *    testing     TEST_VECTORS, DIV_ROUND_MATRIX, selfCheck
 * ---------------------------------------------------------------------------
 */

"use strict";

/* ── constants ──────────────────────────────────────────────────────────── */

/** Wire schema tag; bump when any representation rule above changes. */
const SCHEMA_VERSION = "investor-money.v1";
/** priceMicros per one unit of currency. */
const MICROS_PER_UNIT = 1000000n;
/** Parts per million: a complete probability distribution sums to exactly PPM. */
const PPM = 1000000n;
/** Basis points per 1.0 (100 % = 10,000 bps). */
const BPS_PER_UNIT = 10000n;
/** Hard bound on |x| for every BigInt that passes through this module. */
const MAX_MAGNITUDE = 2n ** 120n;
/** v1 quantities are whole shares. */
const V1_QUANTITY_SCALE = 0;
/** Widest scale accepted for money minor units / decimal fractions. */
const MAX_SCALE = 18;
/** Fixed digit width of the Firestore sortable encoding (10^40 > 2^120). */
const SORTABLE_DIGITS = 40;
const SORTABLE_MODULUS = 10n ** BigInt(SORTABLE_DIGITS);

/** Explicit rounding modes. HALF_UP/HALF_DOWN break ties away from / toward
 *  zero; DOWN truncates toward zero; UP rounds away from zero. */
const ROUNDING = Object.freeze({
  HALF_EVEN: "HALF_EVEN", HALF_UP: "HALF_UP", HALF_DOWN: "HALF_DOWN",
  DOWN: "DOWN", UP: "UP", FLOOR: "FLOOR", CEIL: "CEIL",
});
const ROUNDING_SET = new Set(Object.values(ROUNDING));

/** tickAlign directions. "Less aggressive" always moves the price AWAY from
 *  the counterparty: a buy rounds down, a sell rounds up. */
const TICK_DIRECTION = Object.freeze({
  LESS_AGGRESSIVE_BUY: "LESS_AGGRESSIVE_BUY",
  LESS_AGGRESSIVE_SELL: "LESS_AGGRESSIVE_SELL",
  EXACT: "EXACT",
});

/** Known ISO-4217 minor scales; validateMoney fills or cross-checks these. */
const MINOR_SCALE_BY_CURRENCY = Object.freeze({ USD: 2, CAD: 2, EUR: 2, GBP: 2, JPY: 0 });

/** Every error code this module throws or returns, for callers that switch. */
const CODES = Object.freeze({
  NOT_CANONICAL_INTEGER: "NOT_CANONICAL_INTEGER",
  NOT_SAFE_INTEGER: "NOT_SAFE_INTEGER",
  NOT_BIGINT: "NOT_BIGINT",
  MAGNITUDE_OVERFLOW: "MAGNITUDE_OVERFLOW",
  DIVIDE_BY_ZERO: "DIVIDE_BY_ZERO",
  ROUNDING_MODE_REQUIRED: "ROUNDING_MODE_REQUIRED",
  ROUNDING_MODE_UNKNOWN: "ROUNDING_MODE_UNKNOWN",
  NOT_DECIMAL: "NOT_DECIMAL",
  SCALE_EXCEEDED: "SCALE_EXCEEDED",
  SCALE_INVALID: "SCALE_INVALID",
  CURRENCY_INVALID: "CURRENCY_INVALID",
  CURRENCY_MISMATCH: "CURRENCY_MISMATCH",
  MINOR_SCALE_MISMATCH: "MINOR_SCALE_MISMATCH",
  PRICE_NEGATIVE: "PRICE_NEGATIVE",
  QUANTITY_SCALE_UNSUPPORTED: "QUANTITY_SCALE_UNSUPPORTED",
  QUANTITY_NEGATIVE: "QUANTITY_NEGATIVE",
  PPM_OUT_OF_RANGE: "PPM_OUT_OF_RANGE",
  PPM_BUCKETS_INVALID: "PPM_BUCKETS_INVALID",
  PPM_ID_INVALID: "PPM_ID_INVALID",
  PPM_ID_DUPLICATE: "PPM_ID_DUPLICATE",
  PPM_SUM_MISMATCH: "PPM_SUM_MISMATCH",
  LOSS_BOUNDARY_ABOVE_LIMIT: "LOSS_BOUNDARY_ABOVE_LIMIT",
  REFERENCE_PRICE_NOT_POSITIVE: "REFERENCE_PRICE_NOT_POSITIVE",
  TICK_NOT_POSITIVE: "TICK_NOT_POSITIVE",
  TICK_DIRECTION_UNKNOWN: "TICK_DIRECTION_UNKNOWN",
  OFF_TICK: "OFF_TICK",
  LOT_NOT_POSITIVE: "LOT_NOT_POSITIVE",
  DECIMALS_INSUFFICIENT: "DECIMALS_INSUFFICIENT",
  ROUND_TRIP_LOSSY: "ROUND_TRIP_LOSSY",
  NOT_SORTABLE: "NOT_SORTABLE",
  INSTANT_INVALID: "INSTANT_INVALID",
  VALUE_INVALID: "VALUE_INVALID",
});

/* ── internals ──────────────────────────────────────────────────────────── */

const err = (code, message) => Object.assign(new Error(message), { code });
const CANONICAL_RE = /^-?(0|[1-9][0-9]*)$/;
const DECIMAL_RE = /^(-)?(0|[1-9][0-9]*)(?:\.([0-9]+))?$/;
const CURRENCY_RE = /^[A-Z]{3}$/;
const nameOf = (o, fallback) => (o && typeof o.name === "string" && o.name) || fallback;

/** Bound check; the single choke point for MAGNITUDE_OVERFLOW. */
function checked(x, name = "value") {
  if (typeof x !== "bigint") throw err(CODES.NOT_BIGINT, `${name}: expected a BigInt, got ${typeof x}`);
  if (x > MAX_MAGNITUDE || x < -MAX_MAGNITUDE) {
    throw err(CODES.MAGNITUDE_OVERFLOW, `${name}: |${x}| exceeds MAX_MAGNITUDE (2^120)`);
  }
  return x;
}

/** Accept a BigInt (memory form) or a canonical string (wire form). Never a Number. */
function big(value, name = "value") {
  return typeof value === "bigint" ? checked(value, name) : parseInteger(value, { name });
}

/** Structural scale: a small non-negative count, as a safe Number or canonical string. */
function parseScale(value, name, max = MAX_SCALE) {
  let n;
  if (typeof value === "number" && Number.isSafeInteger(value)) n = value;
  else if (typeof value === "string" && CANONICAL_RE.test(value) && value !== "-0" && value.length <= 3) n = Number(value);
  else throw err(CODES.SCALE_INVALID, `${name}: scale must be an integer 0..${max}`);
  if (n < 0 || n > max) throw err(CODES.SCALE_INVALID, `${name}: scale ${n} outside 0..${max}`);
  return n;
}

const pow10 = (scale) => 10n ** BigInt(scale);

/** "43.25" at scale 6 → 43250000n. Strict: canonical integer part, 1..scale fraction digits. */
function decimalToScaled(str, scale, name) {
  if (typeof str !== "string") throw err(CODES.NOT_DECIMAL, `${name}: decimal must be a string (JSON numbers are forbidden)`);
  const m = DECIMAL_RE.exec(str);
  if (!m) throw err(CODES.NOT_DECIMAL, `${name}: "${str}" is not a canonical decimal`);
  const frac = m[3] || "";
  if (frac.length > scale) throw err(CODES.SCALE_EXCEEDED, `${name}: "${str}" has ${frac.length} fractional digits; at most ${scale} allowed`);
  let v = BigInt(m[2] + frac.padEnd(scale, "0"));
  if (m[1]) v = -v;
  return checked(v, name);
}

/** 43250000n at scale 6 → "43.250000" (fixed) or "43.25" (minimal). Exact. */
function scaledToDecimal(x, scale, { minimal = false } = {}) {
  const neg = x < 0n;
  const s = (neg ? -x : x).toString().padStart(scale + 1, "0");
  const int = s.slice(0, s.length - scale);
  let frac = s.slice(s.length - scale);
  if (minimal) frac = frac.replace(/0+$/, "");
  return (neg ? "-" : "") + int + (frac ? "." + frac : "");
}

function gcd(a, b) {
  a = a < 0n ? -a : a; b = b < 0n ? -b : b;
  while (b !== 0n) { const t = a % b; a = b; b = t; }
  return a;
}

/** Validator wrapper: any typed throw becomes { ok:false, code, message }. */
function guarded(fn) {
  try { return { ok: true, value: fn() }; }
  catch (e) { return { ok: false, code: e.code || CODES.VALUE_INVALID, message: String(e.message || e) }; }
}

/* ── 1. canonical integer strings ───────────────────────────────────────── */

/** True only for /^-?(0|[1-9][0-9]*)$/ strings, excluding "-0". */
function isCanonicalIntegerString(s) {
  return typeof s === "string" && CANONICAL_RE.test(s) && s !== "-0";
}

/** Canonical string (or BigInt) → BigInt within MAX_MAGNITUDE. Anything else —
 *  a JSON number, "+1", " 1", "007", "1.0", null — throws NOT_CANONICAL_INTEGER. */
function parseInteger(value, opts) {
  const name = nameOf(opts, "value");
  if (typeof value === "bigint") return checked(value, name);
  if (typeof value === "number") {
    throw err(CODES.NOT_CANONICAL_INTEGER, `${name}: JSON numbers are forbidden on the wire; send a canonical integer string`);
  }
  if (!isCanonicalIntegerString(value)) {
    throw err(CODES.NOT_CANONICAL_INTEGER, `${name}: ${JSON.stringify(value)} is not a canonical integer string`);
  }
  return checked(BigInt(value), name);
}

/** Bounded structural counts only (array lengths, retry counts): safe Number → BigInt. */
function fromSafeInteger(n, opts) {
  const name = nameOf(opts, "value");
  if (!Number.isSafeInteger(n)) throw err(CODES.NOT_SAFE_INTEGER, `${name}: ${String(n)} is not a safe integer`);
  return BigInt(n);
}

/** BigInt → canonical wire string. */
function toCanonical(x) {
  return checked(x, "toCanonical").toString();
}

/* ── 2. checked arithmetic ──────────────────────────────────────────────── */

/** a + b, bounded. Operands may be BigInt or canonical strings. */
function add(a, b) { return checked(big(a, "add.a") + big(b, "add.b"), "add"); }
/** a − b, bounded. */
function sub(a, b) { return checked(big(a, "sub.a") - big(b, "sub.b"), "sub"); }
/** a × b, bounded. */
function mul(a, b) { return checked(big(a, "mul.a") * big(b, "mul.b"), "mul"); }
/** −a. */
function neg(a) { return -big(a, "neg"); }
/** |a|. */
function abs(a) { const x = big(a, "abs"); return x < 0n ? -x : x; }
/** −1 | 0 | 1 as a Number (a comparison result, not an audited value). */
function cmp(a, b) { const x = big(a, "cmp.a"), y = big(b, "cmp.b"); return x < y ? -1 : x > y ? 1 : 0; }
/** Smaller of two. */
function min(a, b) { const x = big(a, "min.a"), y = big(b, "min.b"); return x <= y ? x : y; }
/** Larger of two. */
function max(a, b) { const x = big(a, "max.a"), y = big(b, "max.b"); return x >= y ? x : y; }
/** a === 0n. */
function isZero(a) { return big(a, "isZero") === 0n; }
/** −1 | 0 | 1 as a Number. */
function sign(a) { const x = big(a, "sign"); return x < 0n ? -1 : x > 0n ? 1 : 0; }

/* ── 3. division with an explicit rounding mode ─────────────────────────── */

/** numerator / denominator rounded per `mode` (one of ROUNDING). Correct for
 *  every sign combination. Throws DIVIDE_BY_ZERO, ROUNDING_MODE_REQUIRED,
 *  ROUNDING_MODE_UNKNOWN. */
function divRound(numerator, denominator, mode) {
  if (mode == null) throw err(CODES.ROUNDING_MODE_REQUIRED, "divRound: a rounding mode is required (no default)");
  if (!ROUNDING_SET.has(mode)) throw err(CODES.ROUNDING_MODE_UNKNOWN, `divRound: unknown rounding mode ${JSON.stringify(mode)}`);
  let n = big(numerator, "divRound.numerator"), d = big(denominator, "divRound.denominator");
  if (d === 0n) throw err(CODES.DIVIDE_BY_ZERO, "divRound: denominator is zero");
  if (d < 0n) { n = -n; d = -d; }
  const q = n / d, r = n % d;                     // truncated; r carries the sign of n
  if (r === 0n) return checked(q, "divRound");
  const away = q + (n < 0n ? -1n : 1n);
  let out;
  switch (mode) {
    case ROUNDING.DOWN: out = q; break;
    case ROUNDING.UP: out = away; break;
    case ROUNDING.FLOOR: out = n < 0n ? away : q; break;
    case ROUNDING.CEIL: out = n < 0n ? q : away; break;
    default: {
      const twice = (r < 0n ? -r : r) * 2n;
      if (twice > d) out = away;
      else if (twice < d) out = q;
      else if (mode === ROUNDING.HALF_UP) out = away;
      else if (mode === ROUNDING.HALF_DOWN) out = q;
      else out = (q % 2n === 0n) ? q : away;     // HALF_EVEN
    }
  }
  return checked(out, "divRound");
}

/* ── 4. exact rationals ─────────────────────────────────────────────────── */

/** Normalised { num, den }: den > 0, reduced by gcd, both BigInt. */
function rational(num, den = 1n) {
  let n = big(num, "rational.num"), d = big(den, "rational.den");
  if (d === 0n) throw err(CODES.DIVIDE_BY_ZERO, "rational: denominator is zero");
  if (d < 0n) { n = -n; d = -d; }
  const g = gcd(n, d);
  if (g > 1n) { n /= g; d /= g; }
  return Object.freeze({ num: n, den: d });
}
/** Integer → rational with den 1. */
function ratFromInteger(x) { return rational(big(x, "ratFromInteger"), 1n); }
/** Accept a rational, an integer BigInt, or a canonical string. */
function asRational(x, name) {
  if (x && typeof x === "object" && "num" in x && "den" in x) return rational(x.num, x.den);
  return rational(big(x, name), 1n);
}
/** a + b, exact. */
function ratAdd(a, b) {
  const x = asRational(a, "ratAdd.a"), y = asRational(b, "ratAdd.b");
  return rational(add(mul(x.num, y.den), mul(y.num, x.den)), mul(x.den, y.den));
}
/** a − b, exact. */
function ratSub(a, b) {
  const x = asRational(a, "ratSub.a"), y = asRational(b, "ratSub.b");
  return rational(sub(mul(x.num, y.den), mul(y.num, x.den)), mul(x.den, y.den));
}
/** a × b, exact. */
function ratMul(a, b) {
  const x = asRational(a, "ratMul.a"), y = asRational(b, "ratMul.b");
  return rational(mul(x.num, y.num), mul(x.den, y.den));
}
/** a ÷ b, exact; b = 0 throws DIVIDE_BY_ZERO. */
function ratDiv(a, b) {
  const x = asRational(a, "ratDiv.a"), y = asRational(b, "ratDiv.b");
  return rational(mul(x.num, y.den), mul(x.den, y.num));
}
/** −1 | 0 | 1 as a Number. */
function ratCmp(a, b) {
  const x = asRational(a, "ratCmp.a"), y = asRational(b, "ratCmp.b");
  return cmp(mul(x.num, y.den), mul(y.num, x.den));
}
/** The one and only rounding step: rational → BigInt per `mode`. */
function ratToInteger(r, mode) {
  const x = asRational(r, "ratToInteger");
  return divRound(x.num, x.den, mode);
}

/* ── 5. exact decimal string conversions ────────────────────────────────── */

/** "43.25" → "43250000". At most 6 fractional digits; leading "-" allowed. */
function priceMicrosFromDecimal(str) {
  return toCanonical(decimalToScaled(str, 6, "priceMicros"));
}
/** "43250000" → "43.25" (minimal exact form; "43000000" → "43", "-1" → "-0.000001"). */
function priceMicrosToDecimal(micros) {
  return scaledToDecimal(big(micros, "priceMicros"), 6, { minimal: true });
}
/** "372.00" with minorScale 2 → "37200". Rejects more than minorScale fraction digits. */
function amountMinorFromDecimal(str, opts) {
  const scale = parseScale(opts && opts.minorScale, "minorScale");
  return toCanonical(decimalToScaled(str, scale, "amountMinor"));
}
/** "37200" with minorScale 2 → "372.00" (fixed width: money always shows every minor digit). */
function amountMinorToDecimal(minor, opts) {
  const scale = parseScale(opts && opts.minorScale, "minorScale");
  return scaledToDecimal(big(minor, "amountMinor"), scale);
}
/** "600" → "6.00" percent; "-25" → "-0.25". */
function bpsToDecimalPercent(bps) {
  return scaledToDecimal(big(bps, "basisPoints"), 2);
}

/* ── 6. value objects and validators ────────────────────────────────────── */

function parseCurrency(c) {
  if (typeof c !== "string" || !CURRENCY_RE.test(c)) {
    throw err(CODES.CURRENCY_INVALID, `currency: ${JSON.stringify(c)} is not a 3-letter uppercase ISO-4217 code`);
  }
  return c;
}

/** { currency, amountMinor, minorScale } → { ok:true, value } | { ok:false, code, message }.
 *  minorScale may be omitted for a currency in MINOR_SCALE_BY_CURRENCY; when
 *  given for a known currency it must agree (MINOR_SCALE_MISMATCH). */
function validateMoney(m) {
  return guarded(() => {
    if (!m || typeof m !== "object") throw err(CODES.VALUE_INVALID, "money: expected an object");
    const currency = parseCurrency(m.currency);
    const known = MINOR_SCALE_BY_CURRENCY[currency];
    let minorScale;
    if (m.minorScale === undefined || m.minorScale === null) {
      if (known === undefined) throw err(CODES.SCALE_INVALID, `money: minorScale is required for ${currency}`);
      minorScale = known;
    } else {
      minorScale = parseScale(m.minorScale, "minorScale");
      if (known !== undefined && minorScale !== known) {
        throw err(CODES.MINOR_SCALE_MISMATCH, `money: ${currency} uses minorScale ${known}, got ${minorScale}`);
      }
    }
    const amountMinor = toCanonical(big(m.amountMinor, "amountMinor"));
    return Object.freeze({ currency, amountMinor, minorScale });
  });
}
/** { currency, priceMicros } → validated; prices are never negative in v1. */
function validatePrice(p) {
  return guarded(() => {
    if (!p || typeof p !== "object") throw err(CODES.VALUE_INVALID, "price: expected an object");
    const currency = parseCurrency(p.currency);
    const micros = big(p.priceMicros, "priceMicros");
    if (micros < 0n) throw err(CODES.PRICE_NEGATIVE, `price: priceMicros ${micros} is negative`);
    return Object.freeze({ currency, priceMicros: toCanonical(micros) });
  });
}
/** { quantityUnits, quantityScale } → validated; v1 requires quantityScale 0.
 *  Sign is not constrained (a fill delta may be negative). */
function validateQuantity(q) {
  return guarded(() => {
    if (!q || typeof q !== "object") throw err(CODES.VALUE_INVALID, "quantity: expected an object");
    const scale = q.quantityScale === undefined || q.quantityScale === null
      ? V1_QUANTITY_SCALE : parseScale(q.quantityScale, "quantityScale");
    if (scale !== V1_QUANTITY_SCALE) {
      throw err(CODES.QUANTITY_SCALE_UNSUPPORTED, `quantity: v1 requires quantityScale ${V1_QUANTITY_SCALE}, got ${scale}`);
    }
    const units = toCanonical(big(q.quantityUnits, "quantityUnits"));
    return Object.freeze({ quantityUnits: units, quantityScale: scale });
  });
}
/** "250000" or { probabilityPpm } → { ok, value:{ probabilityPpm } }; range [0, PPM]. */
function validatePpm(v) {
  return guarded(() => {
    const raw = v && typeof v === "object" ? v.probabilityPpm : v;
    const ppm = big(raw, "probabilityPpm");
    if (ppm < 0n || ppm > PPM) throw err(CODES.PPM_OUT_OF_RANGE, `probabilityPpm ${ppm} outside [0, ${PPM}]`);
    return Object.freeze({ probabilityPpm: toCanonical(ppm) });
  });
}
/** "600" or { basisPoints } → { ok, value:{ basisPoints } }; signed, bounded. */
function validateBps(v) {
  return guarded(() => {
    const raw = v && typeof v === "object" ? v.basisPoints : v;
    return Object.freeze({ basisPoints: toCanonical(big(raw, "basisPoints")) });
  });
}
/** Throwing builders: the validator's code becomes the Error's code. */
function unwrap(r) { if (r.ok) return r.value; throw err(r.code, r.message); }
/** money({ currency, amountMinor, minorScale }) → frozen normalised value (wire shape). */
function money(m) { return unwrap(validateMoney(m)); }
/** price({ currency, priceMicros }) → frozen normalised value. */
function price(p) { return unwrap(validatePrice(p)); }
/** quantity({ quantityUnits, quantityScale }) → frozen normalised value. */
function quantity(q) { return unwrap(validateQuantity(q)); }

/** Throws CURRENCY_MISMATCH / MINOR_SCALE_MISMATCH; returns the shared currency. */
function assertSameCurrency(a, b) {
  const x = money(a), y = money(b);
  if (x.currency !== y.currency) throw err(CODES.CURRENCY_MISMATCH, `currency mismatch: ${x.currency} vs ${y.currency}`);
  if (x.minorScale !== y.minorScale) throw err(CODES.MINOR_SCALE_MISMATCH, `minorScale mismatch: ${x.minorScale} vs ${y.minorScale}`);
  return x.currency;
}

const INSTANT_RE = /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})(?:\.(\d{1,9}))?Z$/;
/** RFC 3339 UTC instant ("2026-09-04T13:30:00Z", optional fraction). Validate only. */
function isRfc3339Utc(s) {
  if (typeof s !== "string") return false;
  const m = INSTANT_RE.exec(s);
  if (!m) return false;
  const [y, mo, d, h, mi, sec] = m.slice(1, 7).map(Number);
  const t = new Date(Date.UTC(y, mo - 1, d, h, mi, sec));
  return t.getUTCFullYear() === y && t.getUTCMonth() === mo - 1 && t.getUTCDate() === d
    && t.getUTCHours() === h && t.getUTCMinutes() === mi && t.getUTCSeconds() === sec;
}
/** { ok:true, value:{ instant } } | { ok:false, code:"INSTANT_INVALID" }. */
function validateInstant(s) {
  return guarded(() => {
    if (!isRfc3339Utc(s)) throw err(CODES.INSTANT_INVALID, `instant: ${JSON.stringify(s)} is not an RFC 3339 UTC string`);
    return Object.freeze({ instant: s });
  });
}

/* ── 7. notional and risk arithmetic ────────────────────────────────────── */

/** price × quantity → minor units:
 *  priceMicros × quantityUnits × 10^minorScale / (10^6 × 10^quantityScale), rounded per mode. */
function notionalMinor({ priceMicros, quantityUnits, quantityScale = V1_QUANTITY_SCALE, minorScale = 2, mode } = {}) {
  const p = big(priceMicros, "priceMicros"), q = big(quantityUnits, "quantityUnits");
  const qs = parseScale(quantityScale, "quantityScale"), ms = parseScale(minorScale, "minorScale");
  const numerator = mul(mul(p, q), pow10(ms));
  return divRound(numerator, mul(MICROS_PER_UNIT, pow10(qs)), mode);
}

/** Planned loss for a LONG whose worst authorised fill is the limit:
 *  (limit − lossBoundary + costPerShare) × quantity, in minor units. The
 *  boundary must not sit above the limit (LOSS_BOUNDARY_ABOVE_LIMIT). Use
 *  CEIL/UP to overstate the loss for a risk gate. */
function plannedLossMinor({ limitPriceMicros, lossBoundaryPriceMicros, quantityUnits,
  costPerShareMicros = 0n, minorScale = 2, mode } = {}) {
  const limit = big(limitPriceMicros, "limitPriceMicros");
  const boundary = big(lossBoundaryPriceMicros, "lossBoundaryPriceMicros");
  const q = big(quantityUnits, "quantityUnits");
  const cost = big(costPerShareMicros, "costPerShareMicros");
  const ms = parseScale(minorScale, "minorScale");
  if (boundary > limit) throw err(CODES.LOSS_BOUNDARY_ABOVE_LIMIT, `plannedLoss: boundary ${boundary} above limit ${limit}`);
  if (q < 0n) throw err(CODES.QUANTITY_NEGATIVE, `plannedLoss: quantity ${q} is negative`);
  const perShare = add(sub(limit, boundary), cost);
  return divRound(mul(mul(perShare, q), pow10(ms)), MICROS_PER_UNIT, mode);
}

/** part / whole in basis points. */
function bpsOf(partMinor, wholeMinor, mode) {
  const part = big(partMinor, "partMinor"), whole = big(wholeMinor, "wholeMinor");
  if (whole === 0n) throw err(CODES.DIVIDE_BY_ZERO, "bpsOf: whole is zero");
  return divRound(mul(part, BPS_PER_UNIT), whole, mode);
}
/** amount × bps / 10000. */
function applyBps(amountMinor, bps, mode) {
  return divRound(mul(big(amountMinor, "amountMinor"), big(bps, "basisPoints")), BPS_PER_UNIT, mode);
}
/** Position weight vs NAV in bps. */
function weightBps(positionMinor, navMinor, mode) {
  const nav = big(navMinor, "navMinor");
  if (nav === 0n) throw err(CODES.DIVIDE_BY_ZERO, "weightBps: NAV is zero");
  return divRound(mul(big(positionMinor, "positionMinor"), BPS_PER_UNIT), nav, mode);
}
/** (to − from) / from in bps. */
function returnBps({ fromMicros, toMicros, mode } = {}) {
  const from = big(fromMicros, "fromMicros"), to = big(toMicros, "toMicros");
  if (from <= 0n) throw err(CODES.REFERENCE_PRICE_NOT_POSITIVE, `returnBps: fromMicros ${from} is not positive`);
  return divRound(mul(sub(to, from), BPS_PER_UNIT), from, mode);
}

/* ── 8. probability distributions ───────────────────────────────────────── */

function checkDistribution(buckets, { requireIds }) {
  if (!Array.isArray(buckets) || buckets.length === 0) {
    throw err(CODES.PPM_BUCKETS_INVALID, "distribution: buckets must be a non-empty array");
  }
  const ids = new Set();
  let sum = 0n;
  const parsed = buckets.map((b, i) => {
    if (!b || typeof b !== "object") throw err(CODES.PPM_BUCKETS_INVALID, `distribution: bucket[${i}] is not an object`);
    if (requireIds || b.id !== undefined) {
      if (typeof b.id !== "string" || !b.id) throw err(CODES.PPM_ID_INVALID, `distribution: bucket[${i}] needs a non-empty string id`);
      if (ids.has(b.id)) throw err(CODES.PPM_ID_DUPLICATE, `distribution: duplicate bucket id ${JSON.stringify(b.id)}`);
      ids.add(b.id);
    }
    const ppm = unwrapPpm(b.probabilityPpm, i);
    sum = add(sum, ppm);
    return { id: b.id, ppm, raw: b };
  });
  return { parsed, sum };
}
function unwrapPpm(raw, i) {
  const r = validatePpm(raw);
  if (!r.ok) throw err(r.code, `bucket[${i}]: ${r.message}`);
  return BigInt(r.value.probabilityPpm);
}

/** buckets = [{ id, probabilityPpm }] → { ok, sumPpm, error }. Sum must be
 *  exactly 1,000,000, every entry in range, ids unique. Returns, never throws. */
function assertPpmDistribution(buckets, opts) {
  const requireIds = !(opts && opts.requireIds === false);
  let sum = 0n;
  try {
    const r = checkDistribution(buckets, { requireIds });
    sum = r.sum;
    if (sum !== PPM) throw err(CODES.PPM_SUM_MISMATCH, `distribution: probabilities sum to ${sum}, expected ${PPM}`);
    return { ok: true, sumPpm: toCanonical(sum), error: null };
  } catch (e) {
    return { ok: false, sumPpm: toCanonical(sum), error: { code: e.code || CODES.VALUE_INVALID, message: String(e.message || e) } };
  }
}

/** Exact Σ(p_i × terminalPriceMicros_i) as a rational over PPM (validated distribution). */
function expectationRational(buckets) {
  const chk = assertPpmDistribution(buckets, { requireIds: false });
  if (!chk.ok) throw err(chk.error.code, chk.error.message);
  let num = 0n;
  buckets.forEach((b, i) => {
    const t = big(b.terminalPriceMicros, `bucket[${i}].terminalPriceMicros`);
    num = add(num, mul(BigInt(validatePpm(b.probabilityPpm).value.probabilityPpm), t));
  });
  return rational(num, PPM);
}

/** E[terminal price] in micros, exact then HALF_EVEN. Blueprint example:
 *  250000×36000000 + 500000×48000000 + 250000×55000000 → 46750000n. */
function expectedTerminalPriceMicros(buckets) {
  return ratToInteger(expectationRational(buckets), ROUNDING.HALF_EVEN);
}

/** Gross and net expected return vs a reference price, in bps, one rounding at the end:
 *  gross = (E[t] − ref) / ref;  net = (E[t] − ref − cost) / ref. */
function expectedReturnBps({ referencePriceMicros, buckets, costMicrosPerShare = 0n, mode } = {}) {
  const ref = big(referencePriceMicros, "referencePriceMicros");
  const cost = big(costMicrosPerShare, "costMicrosPerShare");
  if (ref <= 0n) throw err(CODES.REFERENCE_PRICE_NOT_POSITIVE, `expectedReturnBps: reference ${ref} is not positive`);
  const e = expectationRational(buckets);                       // Σ p·t / PPM
  const refR = ratFromInteger(ref), bpsR = ratFromInteger(BPS_PER_UNIT);
  const gross = ratMul(ratDiv(ratSub(e, refR), refR), bpsR);
  const net = ratMul(ratDiv(ratSub(ratSub(e, refR), ratFromInteger(cost)), refR), bpsR);
  return Object.freeze({
    grossBps: ratToInteger(gross, mode),
    netBps: ratToInteger(net, mode),
    expectedTerminalPriceMicros: ratToInteger(e, ROUNDING.HALF_EVEN),
  });
}

/* ── 9. tick and lot ────────────────────────────────────────────────────── */

function parseTick(tickMicros) {
  const t = big(tickMicros, "tickMicros");
  if (t <= 0n) throw err(CODES.TICK_NOT_POSITIVE, `tickMicros ${t} is not positive`);
  return t;
}
/** True when priceMicros is a whole multiple of tickMicros. */
function isOnTick(priceMicros, tickMicros) {
  return big(priceMicros, "priceMicros") % parseTick(tickMicros) === 0n;
}
/** Snap to the tick grid: LESS_AGGRESSIVE_BUY rounds down, LESS_AGGRESSIVE_SELL
 *  rounds up, EXACT throws OFF_TICK when not already on the grid. */
function tickAlign(priceMicros, tickMicros, direction) {
  const p = big(priceMicros, "priceMicros"), t = parseTick(tickMicros);
  switch (direction) {
    case TICK_DIRECTION.LESS_AGGRESSIVE_BUY: return mul(divRound(p, t, ROUNDING.FLOOR), t);
    case TICK_DIRECTION.LESS_AGGRESSIVE_SELL: return mul(divRound(p, t, ROUNDING.CEIL), t);
    case TICK_DIRECTION.EXACT:
      if (p % t !== 0n) throw err(CODES.OFF_TICK, `price ${p} is not on tick ${t}`);
      return p;
    default: throw err(CODES.TICK_DIRECTION_UNKNOWN, `tickAlign: unknown direction ${JSON.stringify(direction)}`);
  }
}
/** Largest whole-lot quantity not exceeding |quantityUnits| (toward zero for negatives). */
function lotFloor(quantityUnits, lotSize) {
  const q = big(quantityUnits, "quantityUnits"), lot = big(lotSize, "lotSize");
  if (lot <= 0n) throw err(CODES.LOT_NOT_POSITIVE, `lotSize ${lot} is not positive`);
  return mul(divRound(q, lot, ROUNDING.DOWN), lot);
}
/** priceMicros → broker decimal string with exactly `decimals` fraction digits
 *  (0..6). Throws OFF_TICK when tickMicros is given and the price is off-grid,
 *  DECIMALS_INSUFFICIENT when the price needs more digits than allowed. */
function toBrokerDecimalPrice(priceMicros, opts) {
  const p = big(priceMicros, "priceMicros");
  const decimals = parseScale(opts && opts.decimals, "decimals", 6);
  if (opts && opts.tickMicros !== undefined && opts.tickMicros !== null && !isOnTick(p, opts.tickMicros)) {
    throw err(CODES.OFF_TICK, `price ${p} is not on tick ${opts.tickMicros}`);
  }
  const drop = pow10(6 - decimals);
  if (p % drop !== 0n) throw err(CODES.DECIMALS_INSUFFICIENT, `price ${p} cannot be written exactly with ${decimals} decimals`);
  return scaledToDecimal(p / drop, decimals);
}
/** Broker decimal string → priceMicros BigInt (exact inverse). */
function fromBrokerDecimalPrice(decimalString) {
  return decimalToScaled(decimalString, 6, "brokerPrice");
}
/** to → from must reproduce priceMicros exactly; returns the decimal string, else throws ROUND_TRIP_LOSSY. */
function assertLosslessRoundTrip(priceMicros, tickMicros, decimals) {
  const p = big(priceMicros, "priceMicros");
  const s = toBrokerDecimalPrice(p, { tickMicros, decimals });
  const back = fromBrokerDecimalPrice(s);
  if (back !== p) throw err(CODES.ROUND_TRIP_LOSSY, `price ${p} → "${s}" → ${back}`);
  return s;
}

/* ── 10. Firestore helpers ──────────────────────────────────────────────── */

/** Fixed-width string that sorts lexicographically in numeric order:
 *  "p" + 40 zero-padded digits for x ≥ 0, "n" + (10^40 − |x|) for x < 0. */
function sortableEncoding(x) {
  const v = big(x, "sortableEncoding");
  if (v >= 0n) return "p" + v.toString().padStart(SORTABLE_DIGITS, "0");
  return "n" + (SORTABLE_MODULUS + v).toString().padStart(SORTABLE_DIGITS, "0");
}
/** Inverse of sortableEncoding; throws NOT_SORTABLE on malformed input. */
function fromSortable(s) {
  if (typeof s !== "string" || !/^[np][0-9]{40}$/.test(s)) {
    throw err(CODES.NOT_SORTABLE, `fromSortable: ${JSON.stringify(s)} is not a sortable encoding`);
  }
  const digits = BigInt(s.slice(1));
  if (s[0] === "p") return checked(digits, "fromSortable");
  if (digits === 0n) throw err(CODES.NOT_SORTABLE, "fromSortable: negative zero is not encodable");
  return checked(digits - SORTABLE_MODULUS, "fromSortable");
}
/** Convenience mirror for range queries: a JS Number when |x| ≤ MAX_SAFE_INTEGER, else null. Never lossy. */
function int64Mirror(x) {
  const v = big(x, "int64Mirror");
  const lim = BigInt(Number.MAX_SAFE_INTEGER);
  return v > lim || v < -lim ? null : Number(v);
}

/* ── 11. display edge ───────────────────────────────────────────────────── */

/** Human string via Intl.NumberFormat fed the EXACT decimal string. Display only. */
function formatMoney(m, opts) {
  const v = money(m);
  const locale = (opts && opts.locale) || "en-US";
  const decimal = scaledToDecimal(BigInt(v.amountMinor), v.minorScale);
  return new Intl.NumberFormat(locale, { style: "currency", currency: v.currency,
    minimumFractionDigits: v.minorScale, maximumFractionDigits: v.minorScale }).format(decimal);
}
/** Price display: at least 2 and at most 6 fraction digits, exact input. */
function formatPriceMicros(priceMicros, opts) {
  const o = opts || {};
  const decimal = priceMicrosToDecimal(priceMicros);
  const minF = o.minFractionDigits === undefined ? 2 : parseScale(o.minFractionDigits, "minFractionDigits", 6);
  const maxF = o.maxFractionDigits === undefined ? 6 : parseScale(o.maxFractionDigits, "maxFractionDigits", 6);
  const fmt = { minimumFractionDigits: minF, maximumFractionDigits: Math.max(minF, maxF) };
  if (o.currency !== null) { fmt.style = "currency"; fmt.currency = parseCurrency(o.currency === undefined ? "USD" : o.currency); }
  return new Intl.NumberFormat(o.locale || "en-US", fmt).format(decimal);
}

/* ── 12. test vectors (JSON-serialisable; shared with the browser UI) ───── */

const BLUEPRINT_BUCKETS = [
  { id: "bear", probabilityPpm: "250000", terminalPriceMicros: "36000000" },
  { id: "base", probabilityPpm: "500000", terminalPriceMicros: "48000000" },
  { id: "bull", probabilityPpm: "250000", terminalPriceMicros: "55000000" },
];
const MAX_MAGNITUDE_STR = "1329227995784915872903807060280344576";   // 2^120

/** { op, input:[...args], expected }. BigInt results are compared as canonical
 *  strings; an object `expected` matches when every key it lists matches
 *  (messages are free text and left out); `{ throws: CODE }` expects a typed throw. */
const TEST_VECTORS = Object.freeze([
  // canonical strings
  { op: "isCanonicalIntegerString", input: ["0"], expected: true },
  { op: "isCanonicalIntegerString", input: ["-0"], expected: false },
  { op: "isCanonicalIntegerString", input: ["+1"], expected: false },
  { op: "isCanonicalIntegerString", input: ["007"], expected: false },
  { op: "isCanonicalIntegerString", input: [" 1"], expected: false },
  { op: "isCanonicalIntegerString", input: ["-9007199254740993"], expected: true },
  { op: "parseInteger", input: [42], expected: { throws: "NOT_CANONICAL_INTEGER" } },
  { op: "parseInteger", input: ["1.0"], expected: { throws: "NOT_CANONICAL_INTEGER" } },
  { op: "parseInteger", input: ["-17"], expected: "-17" },
  { op: "fromSafeInteger", input: [9007199254740992], expected: { throws: "NOT_SAFE_INTEGER" } },
  // checked arithmetic
  { op: "add", input: [MAX_MAGNITUDE_STR, "1"], expected: { throws: "MAGNITUDE_OVERFLOW" } },
  { op: "sub", input: ["-" + MAX_MAGNITUDE_STR, "1"], expected: { throws: "MAGNITUDE_OVERFLOW" } },
  { op: "add", input: [MAX_MAGNITUDE_STR, "0"], expected: MAX_MAGNITUDE_STR },
  { op: "mul", input: ["-3", "7"], expected: "-21" },
  { op: "abs", input: ["-21"], expected: "21" },
  { op: "sign", input: ["-21"], expected: -1 },
  { op: "cmp", input: ["2", "10"], expected: -1 },
  // division
  { op: "divRound", input: ["7", "2", "HALF_EVEN"], expected: "4" },
  { op: "divRound", input: ["-5", "2", "HALF_EVEN"], expected: "-2" },
  { op: "divRound", input: ["1", "0", "FLOOR"], expected: { throws: "DIVIDE_BY_ZERO" } },
  { op: "divRound", input: ["1", "2"], expected: { throws: "ROUNDING_MODE_REQUIRED" } },
  { op: "divRound", input: ["1", "2", "NEAREST"], expected: { throws: "ROUNDING_MODE_UNKNOWN" } },
  // rationals
  { op: "ratToInteger", input: [{ num: "1", den: "3" }, "CEIL"], expected: "1" },
  { op: "ratAdd", input: [{ num: "1", den: "3" }, { num: "1", den: "6" }], expected: { num: "1", den: "2" } },
  { op: "ratCmp", input: [{ num: "-1", den: "3" }, { num: "1", den: "-3" }], expected: 0 },
  // decimal conversions
  { op: "priceMicrosFromDecimal", input: ["43.25"], expected: "43250000" },
  { op: "priceMicrosFromDecimal", input: ["-0.000001"], expected: "-1" },
  { op: "priceMicrosFromDecimal", input: ["1.2345678"], expected: { throws: "SCALE_EXCEEDED" } },
  { op: "priceMicrosFromDecimal", input: ["abc"], expected: { throws: "NOT_DECIMAL" } },
  { op: "priceMicrosFromDecimal", input: [43.25], expected: { throws: "NOT_DECIMAL" } },
  { op: "priceMicrosToDecimal", input: ["43250000"], expected: "43.25" },
  { op: "priceMicrosToDecimal", input: ["43000000"], expected: "43" },
  { op: "priceMicrosToDecimal", input: ["-1"], expected: "-0.000001" },
  { op: "amountMinorFromDecimal", input: ["372.00", { minorScale: "2" }], expected: "37200" },
  { op: "amountMinorFromDecimal", input: ["372.005", { minorScale: "2" }], expected: { throws: "SCALE_EXCEEDED" } },
  { op: "amountMinorToDecimal", input: ["-5", { minorScale: "2" }], expected: "-0.05" },
  { op: "bpsToDecimalPercent", input: ["600"], expected: "6.00" },
  { op: "bpsToDecimalPercent", input: ["-25"], expected: "-0.25" },
  // value objects
  { op: "validateMoney", input: [{ currency: "USD", amountMinor: "100", minorScale: "2" }],
    expected: { ok: true, value: { currency: "USD", amountMinor: "100", minorScale: 2 } } },
  { op: "validateMoney", input: [{ currency: "usd", amountMinor: "100", minorScale: "2" }], expected: { ok: false, code: "CURRENCY_INVALID" } },
  { op: "validateMoney", input: [{ currency: "USD", amountMinor: 100, minorScale: "2" }], expected: { ok: false, code: "NOT_CANONICAL_INTEGER" } },
  { op: "validateMoney", input: [{ currency: "USD", amountMinor: "100", minorScale: "3" }], expected: { ok: false, code: "MINOR_SCALE_MISMATCH" } },
  { op: "validatePrice", input: [{ currency: "USD", priceMicros: "-1" }], expected: { ok: false, code: "PRICE_NEGATIVE" } },
  { op: "validateQuantity", input: [{ quantityUnits: "10", quantityScale: "1" }], expected: { ok: false, code: "QUANTITY_SCALE_UNSUPPORTED" } },
  { op: "validateQuantity", input: [{ quantityUnits: "10", quantityScale: "0" }], expected: { ok: true, value: { quantityUnits: "10", quantityScale: 0 } } },
  { op: "validatePpm", input: ["1000001"], expected: { ok: false, code: "PPM_OUT_OF_RANGE" } },
  { op: "validateBps", input: ["+5"], expected: { ok: false, code: "NOT_CANONICAL_INTEGER" } },
  { op: "assertSameCurrency", input: [{ currency: "USD", amountMinor: "1" }, { currency: "CAD", amountMinor: "1" }],
    expected: { throws: "CURRENCY_MISMATCH" } },
  { op: "validateInstant", input: ["2026-09-04T13:30:00Z"], expected: { ok: true } },
  { op: "validateInstant", input: ["2026-09-04T13:30:00+00:00"], expected: { ok: false, code: "INSTANT_INVALID" } },
  { op: "validateInstant", input: ["2026-02-30T00:00:00Z"], expected: { ok: false, code: "INSTANT_INVALID" } },
  // notional / risk
  { op: "notionalMinor", input: [{ priceMicros: "43250000", quantityUnits: "8", quantityScale: "0", minorScale: "2", mode: "HALF_EVEN" }], expected: "34600" },
  { op: "notionalMinor", input: [{ priceMicros: "10000005", quantityUnits: "1", quantityScale: "0", minorScale: "2", mode: "UP" }], expected: "1001" },
  { op: "notionalMinor", input: [{ priceMicros: "10000005", quantityUnits: "1", quantityScale: "0", minorScale: "2", mode: "HALF_EVEN" }], expected: "1000" },
  { op: "notionalMinor", input: [{ priceMicros: "43250000", quantityUnits: "8", quantityScale: "0", minorScale: "2" }], expected: { throws: "ROUNDING_MODE_REQUIRED" } },
  { op: "plannedLossMinor", input: [{ limitPriceMicros: "43250000", lossBoundaryPriceMicros: "40000000", quantityUnits: "8", costPerShareMicros: "10000", mode: "CEIL" }], expected: "2608" },
  { op: "plannedLossMinor", input: [{ limitPriceMicros: "40000000", lossBoundaryPriceMicros: "43250000", quantityUnits: "8", mode: "CEIL" }], expected: { throws: "LOSS_BOUNDARY_ABOVE_LIMIT" } },
  { op: "bpsOf", input: ["372", "6200", "HALF_EVEN"], expected: "600" },
  { op: "applyBps", input: ["37200", "600", "HALF_EVEN"], expected: "2232" },
  { op: "weightBps", input: ["2500", "10000", "HALF_EVEN"], expected: "2500" },
  { op: "returnBps", input: [{ fromMicros: "40000000", toMicros: "46750000", mode: "HALF_EVEN" }], expected: "1688" },
  { op: "returnBps", input: [{ fromMicros: "0", toMicros: "1", mode: "HALF_EVEN" }], expected: { throws: "REFERENCE_PRICE_NOT_POSITIVE" } },
  // distributions
  { op: "assertPpmDistribution", input: [BLUEPRINT_BUCKETS], expected: { ok: true, sumPpm: "1000000", error: null } },
  { op: "assertPpmDistribution", input: [[{ id: "a", probabilityPpm: "500000" }, { id: "b", probabilityPpm: "499999" }]],
    expected: { ok: false, sumPpm: "999999", error: { code: "PPM_SUM_MISMATCH" } } },
  { op: "assertPpmDistribution", input: [[{ id: "a", probabilityPpm: "500000" }, { id: "a", probabilityPpm: "500000" }]],
    expected: { ok: false, error: { code: "PPM_ID_DUPLICATE" } } },
  { op: "assertPpmDistribution", input: [[{ id: "a", probabilityPpm: 500000 }, { id: "b", probabilityPpm: "500000" }]],
    expected: { ok: false, error: { code: "NOT_CANONICAL_INTEGER" } } },
  { op: "expectedTerminalPriceMicros", input: [BLUEPRINT_BUCKETS], expected: "46750000" },
  { op: "expectedReturnBps", input: [{ referencePriceMicros: "40000000", buckets: BLUEPRINT_BUCKETS, costMicrosPerShare: "100000", mode: "HALF_EVEN" }],
    expected: { grossBps: "1688", netBps: "1662", expectedTerminalPriceMicros: "46750000" } },
  // tick / lot
  { op: "tickAlign", input: ["43254000", "10000", "LESS_AGGRESSIVE_BUY"], expected: "43250000" },
  { op: "tickAlign", input: ["43254000", "10000", "LESS_AGGRESSIVE_SELL"], expected: "43260000" },
  { op: "tickAlign", input: ["43254000", "10000", "EXACT"], expected: { throws: "OFF_TICK" } },
  { op: "tickAlign", input: ["43250000", "0", "EXACT"], expected: { throws: "TICK_NOT_POSITIVE" } },
  { op: "isOnTick", input: ["43250000", "10000"], expected: true },
  { op: "isOnTick", input: ["43254000", "10000"], expected: false },
  { op: "lotFloor", input: ["107", "100"], expected: "100" },
  { op: "lotFloor", input: ["-107", "100"], expected: "-100" },
  { op: "toBrokerDecimalPrice", input: ["43250000", { tickMicros: "10000", decimals: "2" }], expected: "43.25" },
  { op: "toBrokerDecimalPrice", input: ["43254000", { tickMicros: "10000", decimals: "2" }], expected: { throws: "OFF_TICK" } },
  { op: "toBrokerDecimalPrice", input: ["500100", { tickMicros: "100", decimals: "2" }], expected: { throws: "DECIMALS_INSUFFICIENT" } },
  { op: "toBrokerDecimalPrice", input: ["500100", { tickMicros: "100", decimals: "4" }], expected: "0.5001" },
  { op: "fromBrokerDecimalPrice", input: ["43.25"], expected: "43250000" },
  { op: "assertLosslessRoundTrip", input: ["43250000", "10000", "2"], expected: "43.25" },
  // firestore helpers
  { op: "sortableEncoding", input: ["42"], expected: "p0000000000000000000000000000000000000042" },
  { op: "sortableEncoding", input: ["-1"], expected: "n9999999999999999999999999999999999999999" },
  { op: "sortableEncoding", input: ["-42"], expected: "n9999999999999999999999999999999999999958" },
  { op: "fromSortable", input: ["n9999999999999999999999999999999999999958"], expected: "-42" },
  { op: "fromSortable", input: ["x0000000000000000000000000000000000000042"], expected: { throws: "NOT_SORTABLE" } },
  { op: "int64Mirror", input: ["9007199254740991"], expected: 9007199254740991 },
  { op: "int64Mirror", input: ["9007199254740992"], expected: null },
  // display edge
  { op: "formatMoney", input: [{ currency: "USD", amountMinor: "37200", minorScale: "2" }, { locale: "en-US" }], expected: "$372.00" },
  { op: "formatMoney", input: [{ currency: "USD", amountMinor: "-5" }, { locale: "en-US" }], expected: "-$0.05" },
  { op: "formatPriceMicros", input: ["43250001", { locale: "en-US" }], expected: "$43.250001" },
]);

/** Exhaustive divRound matrix: [n, d, HALF_EVEN, HALF_UP, HALF_DOWN, DOWN, UP, FLOOR, CEIL]. */
const DIV_ROUND_MATRIX = Object.freeze([
  ["7", "2", "4", "4", "3", "3", "4", "3", "4"],
  ["-7", "2", "-4", "-4", "-3", "-3", "-4", "-4", "-3"],
  ["7", "-2", "-4", "-4", "-3", "-3", "-4", "-4", "-3"],
  ["-7", "-2", "4", "4", "3", "3", "4", "3", "4"],
  ["5", "2", "2", "3", "2", "2", "3", "2", "3"],
  ["-5", "2", "-2", "-3", "-2", "-2", "-3", "-3", "-2"],
  ["3", "2", "2", "2", "1", "1", "2", "1", "2"],
  ["-3", "2", "-2", "-2", "-1", "-1", "-2", "-2", "-1"],
  ["1", "3", "0", "0", "0", "0", "1", "0", "1"],
  ["-1", "3", "0", "0", "0", "0", "-1", "-1", "0"],
  ["2", "3", "1", "1", "1", "0", "1", "0", "1"],
  ["-2", "3", "-1", "-1", "-1", "0", "-1", "-1", "0"],
  ["0", "5", "0", "0", "0", "0", "0", "0", "0"],
  ["6", "2", "3", "3", "3", "3", "3", "3", "3"],
  ["-6", "2", "-3", "-3", "-3", "-3", "-3", "-3", "-3"],
  ["1", "1000000", "0", "0", "0", "0", "1", "0", "1"],
  ["-1", "1000000", "0", "0", "0", "0", "-1", "-1", "0"],
]);
const MATRIX_MODES = [ROUNDING.HALF_EVEN, ROUNDING.HALF_UP, ROUNDING.HALF_DOWN,
  ROUNDING.DOWN, ROUNDING.UP, ROUNDING.FLOOR, ROUNDING.CEIL];

/* ── 13. self check ─────────────────────────────────────────────────────── */

/** BigInt → canonical string, recursively, so results compare as wire values. */
function wire(v) {
  if (typeof v === "bigint") return v.toString();
  if (Array.isArray(v)) return v.map(wire);
  if (v && typeof v === "object") { const o = {}; for (const k of Object.keys(v)) o[k] = wire(v[k]); return o; }
  return v;
}
function matches(expected, actual) {
  if (Array.isArray(expected)) {
    return Array.isArray(actual) && actual.length === expected.length && expected.every((e, i) => matches(e, actual[i]));
  }
  if (expected && typeof expected === "object") {
    if (!actual || typeof actual !== "object") return false;
    return Object.keys(expected).every((k) => matches(expected[k], actual[k]));
  }
  return Object.is(expected, actual);
}
function runVector(api, v) {
  const fn = api[v.op];
  if (typeof fn !== "function") return { ...v, actual: null, error: `unknown op ${v.op}` };
  const wantThrow = v.expected && typeof v.expected === "object" && typeof v.expected.throws === "string";
  let actual, thrown = null;
  try { actual = wire(fn.apply(null, v.input)); } catch (e) { thrown = e; }
  if (wantThrow) {
    if (thrown && thrown.code === v.expected.throws) return null;
    return { ...v, actual: thrown ? { threw: thrown.code || null, message: thrown.message } : actual };
  }
  if (thrown) return { ...v, actual: { threw: thrown.code || null, message: thrown.message } };
  return matches(v.expected, actual) ? null : { ...v, actual };
}

/** Runs every TEST_VECTOR plus the divRound matrix. Deterministic, < 50 ms.
 *  → { pass, failures:[{ op, input, expected, actual }], vectors, checks, ms }. */
function selfCheck() {
  const t0 = Date.now();
  const failures = [];
  let checks = 0;
  for (const v of TEST_VECTORS) {
    checks += 1;
    const f = runVector(API, v);
    if (f) failures.push(f);
  }
  for (const row of DIV_ROUND_MATRIX) {
    MATRIX_MODES.forEach((mode, i) => {
      checks += 1;
      const expected = row[2 + i];
      let actual;
      try { actual = toCanonical(divRound(row[0], row[1], mode)); }
      catch (e) { actual = { threw: e.code || null, message: e.message }; }
      if (actual !== expected) failures.push({ op: "divRound", input: [row[0], row[1], mode], expected, actual });
    });
  }
  return { pass: failures.length === 0, failures, vectors: TEST_VECTORS.length, checks, ms: Date.now() - t0 };
}

const API = {
  SCHEMA_VERSION, MICROS_PER_UNIT, PPM, BPS_PER_UNIT, MAX_MAGNITUDE, V1_QUANTITY_SCALE,
  ROUNDING, TICK_DIRECTION, CODES, MINOR_SCALE_BY_CURRENCY,
  isCanonicalIntegerString, parseInteger, fromSafeInteger, toCanonical,
  add, sub, mul, neg, abs, cmp, min, max, isZero, sign, divRound,
  rational, ratFromInteger, ratAdd, ratSub, ratMul, ratDiv, ratCmp, ratToInteger,
  priceMicrosFromDecimal, priceMicrosToDecimal, amountMinorFromDecimal, amountMinorToDecimal, bpsToDecimalPercent,
  money, price, quantity, validateMoney, validatePrice, validateQuantity, validatePpm, validateBps,
  assertSameCurrency, isRfc3339Utc, validateInstant,
  notionalMinor, plannedLossMinor, bpsOf, applyBps, weightBps, returnBps,
  assertPpmDistribution, expectedTerminalPriceMicros, expectedReturnBps,
  tickAlign, isOnTick, lotFloor, toBrokerDecimalPrice, fromBrokerDecimalPrice, assertLosslessRoundTrip,
  sortableEncoding, fromSortable, int64Mirror,
  formatMoney, formatPriceMicros,
  TEST_VECTORS, DIV_ROUND_MATRIX, selfCheck,
};

module.exports = API;
