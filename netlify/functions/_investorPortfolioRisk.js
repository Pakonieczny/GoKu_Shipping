/*  netlify/functions/_investorPortfolioRisk.js  (v1.0)
 *  ---------------------------------------------------------------------------
 *  Investor_AI — portfolio risk scenarios and the §15.1 quantity authority.
 *
 *  AUTHORITY BOUNDARY
 *  Sol (the AI fund manager) selects companies and proposes BUY mandates: a
 *  quantity, a limit price and a loss boundary. This module is the
 *  deterministic counterpart and it is deliberately narrow: code may cancel
 *  expansion or reduce authorization, never select a replacement company,
 *  raise exposure, widen a boundary, or cancel protection. Nothing here ranks,
 *  scores or substitutes a candidate. runScenarios() hands Sol measurements
 *  and booleans so that SOL can compare feasible baskets; quantityAuthority()
 *  applies the fixed precedence formula of §15.1, so the authorized quantity is
 *  always ≤ the proposed one (asserted, INVARIANT_VIOLATION otherwise).
 *
 *  PURE. No Firestore, no network, no model calls, no clock (emergency rank
 *  expiry is checked against a caller-supplied nowMs). Every audited number is
 *  a BigInt in memory and a canonical integer string on the wire
 *  (…Micros = 1e-6 USD, …Minor = cents, …Bps, …Units = whole shares, scale 0).
 *  A JS float never enters or leaves — see _investorMoney.js.
 *
 *  §15.1 quantity authority for a long BUY (whole shares, floor):
 *    Q = lotFloor( min[ Q_AI,
 *          cashCapacity        / (limit + costPerShare),
 *          nameCapacity        / limit,
 *          sectorCapacity      / limit,
 *          clusterCapacity     / limit,
 *          plannedLossCapacity / (limit − lossBoundary + costPerShare),
 *          stressLossCapacity  / (limit − gapHaltStressFill + stressCostPerShare),
 *          ADV order capacity, ADV position capacity,
 *          overnightCapacity   / limit ] )
 *  plus hard refusals (authorized 0): spread > maxSpreadBps, ADV < minAdvMinor,
 *  limit ≤ lossBoundary, no protection, not long-only, an increase on a name
 *  already in the book when instruments.increaseAction is false. The gross /
 *  net / open-order caps are basket-level checks (constraintsBreached).
 *
 *  INPUT SHAPES (money as canonical strings or BigInt; never JSON numbers)
 *    portfolio = { navMinor, settledCashMinor, reservedMinor,
 *      positions: [{ symbol, quantityUnits, markMicros, lossBoundaryPriceMicros|null, sector }],
 *      workingOrders: [{ symbol, side:"buy"|"sell", quantityUnits, limitPriceMicros,
 *                        lossBoundaryPriceMicros?, costPerShareMicros?, sector }] }
 *      reservedMinor = cash held for reasons OTHER than working entries; working
 *      BUY entries are counted from workingOrders at limit + cost.
 *    candidate = { symbol, sector, proposedQuantityUnits, limitPriceMicros,
 *                  lossBoundaryPriceMicros, costPerShareMicros, advMinor, spreadBps }
 *    policy    = RISK_MANDATE-shaped object (policy.riskMandate || policy).
 *    marks     = { SYMBOL: markMicros } overrides position.markMicros.
 *    advBySymbol = { SYMBOL: advMinor } fallback for candidate.advMinor.
 *    sectorOf(symbol) → sector; clusterOf(symbol, sector) → cluster id. Default
 *    cluster map is _investorRisk.clusterOf (an unmapped name clusters with its
 *    sector — unknown never means "uncorrelated").
 * ---------------------------------------------------------------------------
 */

"use strict";

const M = require("./_investorMoney");
const { clusterOf: defaultClusterOf } = require("./_investorRisk");

const SCHEMA_VERSION = "investor-portfolio-risk.v1";
const { ROUNDING } = M;
const BPS = M.BPS_PER_UNIT;                 // 10000n
const MICROS_PER_MINOR = 10000n;            // 1e6 micros per USD ÷ 100 minor per USD

const CODES = Object.freeze({
  POLICY_INVALID: "POLICY_INVALID", POLICY_FIELD_MISSING: "POLICY_FIELD_MISSING",
  PORTFOLIO_INVALID: "PORTFOLIO_INVALID", CANDIDATE_INVALID: "CANDIDATE_INVALID",
  NAV_NOT_POSITIVE: "NAV_NOT_POSITIVE", MARK_MISSING: "MARK_MISSING",
  INVARIANT_VIOLATION: "INVARIANT_VIOLATION",
});
/** Reason codes on a quantityAuthority result; RULE_REFUSALS zero the quantity outright. */
const REASONS = Object.freeze({
  LONG_ONLY: "LONG_ONLY", PROPOSAL_NOT_POSITIVE: "PROPOSAL_NOT_POSITIVE", LIMIT_NOT_POSITIVE: "LIMIT_NOT_POSITIVE",
  PROTECTION_REQUIRED: "PROTECTION_REQUIRED", BOUNDARY_NOT_POSITIVE: "BOUNDARY_NOT_POSITIVE",
  LIMIT_NOT_ABOVE_BOUNDARY: "LIMIT_NOT_ABOVE_BOUNDARY", SPREAD_UNKNOWN: "SPREAD_UNKNOWN",
  SPREAD_TOO_WIDE: "SPREAD_TOO_WIDE", ADV_UNKNOWN: "ADV_UNKNOWN", ADV_BELOW_MINIMUM: "ADV_BELOW_MINIMUM",
  INCREASE_NOT_PERMITTED: "INCREASE_NOT_PERMITTED",
  CLAMPED: "CLAMPED", LOT_FLOOR: "LOT_FLOOR", ZERO_CAPACITY: "ZERO_CAPACITY",
});
const RULE_REFUSALS = new Set(Object.values(REASONS).filter((c) => !["CLAMPED", "LOT_FLOOR", "ZERO_CAPACITY"].includes(c)));
const EMERGENCY_RULES = Object.freeze({
  SOL_RANKS: "ACCIDENTAL_SHORT_FIRST_THEN_SOL_EMERGENCY_RANKS",
  FALLBACK: "ACCIDENTAL_SHORT_FIRST_THEN_SMALLEST_LIQUID_SET_GREATEST_STRESS_RELIEF_PER_DOLLAR",
});

/* ── helpers ────────────────────────────────────────────────────────────── */
const err = (code, message) => Object.assign(new Error(message), { code });
const big = (v, name) => M.parseInteger(v, { name });
const bigOr = (v, dflt, name) => (v === undefined || v === null ? dflt : big(v, name));
const wire = (x) => M.toCanonical(x);
const clamp0 = (x) => (x < 0n ? 0n : x);
const minBig = (a, b) => (a <= b ? a : b);
const absBig = (x) => (x < 0n ? -x : x);
const upper = (s) => String(s == null ? "" : s).trim().toUpperCase();
/** amount × bps / 10000 rounded DOWN: a capacity is never overstated. */
const share = (amount, bps) => M.applyBps(amount, bps, ROUNDING.DOWN);
const floorUnits = (capacity, perShare) => (capacity <= 0n || perShare <= 0n ? 0n : M.divRound(capacity, perShare, ROUNDING.DOWN));
const ceilDiv = (n, d) => M.divRound(n, d, ROUNDING.CEIL);
const bpsOfNav = (micros, navMicros, mode) => M.divRound(micros * BPS, navMicros, mode || ROUNDING.HALF_EVEN);
const addTo = (m, k, v) => { m[k] = (m[k] || 0n) + v; };
const at = (m, k) => m[k] || 0n;
const mapWire = (m, f) => { const o = {}; for (const k of Object.keys(m).sort()) o[k] = f(m[k]); return o; };
const get = (o, path) => path.split(".").reduce((x, k) => (x && typeof x === "object" ? x[k] : undefined), o);
const first = (...vals) => { for (const v of vals) if (v !== undefined && v !== null) return v; return null; };

/* ── policy ─────────────────────────────────────────────────────────────── */
/** RISK_MANDATE (or { riskMandate }) → frozen BigInt view. Missing fields throw POLICY_FIELD_MISSING. */
function normalizePolicy(policy) {
  const p = policy && typeof policy === "object" ? (policy.riskMandate || policy) : null;
  if (!p || typeof p !== "object") throw err(CODES.POLICY_INVALID, "policy: expected a RISK_MANDATE-shaped object");
  if (p.__normalized) return p;
  const f = (section, key) => {
    const v = p[section] && p[section][key];
    if (v === undefined || v === null) throw err(CODES.POLICY_FIELD_MISSING, `policy.${section}.${key} is required`);
    return big(v, `policy.${section}.${key}`);
  };
  const inst = p.instruments || {}, clamp = p.clampMateriality || {};
  const maxBuys = get(p, "activation.maxActionableBuysPerRun");
  return Object.freeze({
    __normalized: true,
    weights: {
      maxSingleName: f("weights", "maxSingleNameWeightBps"), maxSector: f("weights", "maxSectorWeightBps"),
      maxCluster: f("weights", "maxCorrelatedClusterWeightBps"), maxGross: f("weights", "maxGrossExposureBps"),
      maxNet: f("weights", "maxNetExposureBps"), minCashReserve: f("weights", "minSettledCashReserveBps"),
      maxOvernight: f("weights", "maxOvernightExposureBps"), maxOpenOrder: f("weights", "maxOpenOrderNotionalBps"),
    },
    losses: {
      perPositionPlanned: f("losses", "maxPlannedLossPerPositionBps"), aggregatePlanned: f("losses", "maxAggregatePlannedLossBps"),
      perPositionStressed: f("losses", "maxStressedLossPerPositionBps"), aggregateStressed: f("losses", "maxAggregateStressedLossBps"),
    },
    liquidity: {
      maxOrderPctOfAdv: f("liquidity", "maxOrderPctOfAdvBps"), maxPositionPctOfAdv: f("liquidity", "maxPositionPctOfAdvBps"),
      maxSpread: f("liquidity", "maxSpreadBps"), minAdvMinor: f("liquidity", "minAdvMinor"),
    },
    stress: { gapHaltAdverse: f("stress", "gapHaltAdverseBps"), stressCostPerShare: f("stress", "stressCostPerShareMicros") },
    clamp: {
      minDeltaUnits: bigOr(clamp.minDeltaUnits, 1n, "policy.clampMateriality.minDeltaUnits"),
      deltaPctOfProposal: bigOr(clamp.deltaPctOfProposalBps, 500n, "policy.clampMateriality.deltaPctOfProposalBps"),
      deltaNav: bigOr(clamp.deltaNavBps, 25n, "policy.clampMateriality.deltaNavBps"),
      boundaryEqualityMaterial: clamp.boundaryEqualityMaterial !== false,
    },
    instruments: { longOnly: inst.longOnly !== false, increaseAction: inst.increaseAction === true,
      lotSize: bigOr(inst.lotSize, 1n, "policy.instruments.lotSize") },
    maxActionableBuysPerRun: Number.isSafeInteger(maxBuys) && maxBuys > 0 ? maxBuys : 8,
  });
}

/* ── the book ───────────────────────────────────────────────────────────── */
/** portfolio (wire) → { navMinor, navMicros, settledCashMinor, reservedMinor, positions, workingOrders, classify }. */
function normalizeBook(portfolio, ctx = {}) {
  if (!portfolio || typeof portfolio !== "object") throw err(CODES.PORTFOLIO_INVALID, "portfolio: expected an object");
  const navMinor = big(portfolio.navMinor, "portfolio.navMinor");
  if (navMinor <= 0n) throw err(CODES.NAV_NOT_POSITIVE, `portfolio.navMinor ${navMinor} is not positive`);
  const marks = ctx.marks || {};
  const sectorOf = typeof ctx.sectorOf === "function" ? ctx.sectorOf : null;
  const clusterFn = typeof ctx.clusterOf === "function" ? ctx.clusterOf : defaultClusterOf;
  const classify = (symbol, sector) => {
    const sec = String(sector || (sectorOf && sectorOf(symbol)) || "unknown");
    return { sector: sec, cluster: String(clusterFn(symbol, sec) || `sector:${sec}`) };
  };
  const positions = (portfolio.positions || []).map((p, i) => {
    const symbol = upper(p && p.symbol);
    if (!symbol) throw err(CODES.PORTFOLIO_INVALID, `positions[${i}]: symbol is required`);
    const markRaw = first(marks[symbol], p.markMicros);
    if (markRaw === null) throw err(CODES.MARK_MISSING, `positions[${i}] ${symbol}: no mark`);
    const mark = big(markRaw, `positions[${i}].markMicros`);
    if (mark <= 0n) throw err(CODES.MARK_MISSING, `positions[${i}] ${symbol}: mark ${mark} is not positive`);
    const boundary = p.lossBoundaryPriceMicros === undefined || p.lossBoundaryPriceMicros === null
      ? null : big(p.lossBoundaryPriceMicros, `positions[${i}].lossBoundaryPriceMicros`);
    return { symbol, quantity: big(p.quantityUnits, `positions[${i}].quantityUnits`), mark, boundary, ...classify(symbol, p.sector) };
  });
  const workingOrders = (portfolio.workingOrders || []).map((w, i) => {
    const symbol = upper(w && w.symbol), side = String(w && w.side || "").toLowerCase();
    if (!symbol || (side !== "buy" && side !== "sell")) throw err(CODES.PORTFOLIO_INVALID, `workingOrders[${i}]: symbol and side buy|sell are required`);
    const quantity = big(w.quantityUnits, `workingOrders[${i}].quantityUnits`);
    if (quantity < 0n) throw err(CODES.PORTFOLIO_INVALID, `workingOrders[${i}]: negative quantity`);
    const boundary = w.lossBoundaryPriceMicros === undefined || w.lossBoundaryPriceMicros === null
      ? null : big(w.lossBoundaryPriceMicros, `workingOrders[${i}].lossBoundaryPriceMicros`);
    return { symbol, side, quantity, limit: big(w.limitPriceMicros, `workingOrders[${i}].limitPriceMicros`), boundary,
      cost: bigOr(w.costPerShareMicros, 0n, `workingOrders[${i}].costPerShareMicros`), ...classify(symbol, w.sector) };
  });
  return {
    navMinor, navMicros: navMinor * MICROS_PER_MINOR,
    settledCashMinor: bigOr(portfolio.settledCashMinor, 0n, "portfolio.settledCashMinor"),
    reservedMinor: bigOr(portfolio.reservedMinor, 0n, "portfolio.reservedMinor"),
    positions, workingOrders, classify,
  };
}

/** Exact BigInt totals of a book. Notionals are micro-notional (priceMicros × units);
 *  stressed losses are kept ×BPS ("scaled") so limit × gapHalt/10000 never rounds early. */
function bookTotals(book, P) {
  const gap = P.stress.gapHaltAdverse, sc = P.stress.stressCostPerShare;
  const stressScaled = (price, qty) => (price * gap + sc * BPS) * qty;
  const T = { longMicros: 0n, shortMicros: 0n, workingBuyMicros: 0n, workingBuyCashMicros: 0n, workingBuyCount: 0,
    byName: {}, byNameHeld: {}, bySector: {}, byCluster: {}, plannedByName: {}, stressedScaledByName: {},
    plannedAggMicros: 0n, stressedAggScaled: 0n, unprotected: [], shorts: [] };
  const book1 = (symbol, sector, cluster, notional, stressed, planned) => {
    addTo(T.byName, symbol, notional); addTo(T.bySector, sector, notional); addTo(T.byCluster, cluster, notional);
    addTo(T.stressedScaledByName, symbol, stressed); T.stressedAggScaled += stressed;
    addTo(T.plannedByName, symbol, planned); T.plannedAggMicros += planned;
  };
  for (const p of book.positions) {
    const q = absBig(p.quantity), notional = p.mark * q, stressed = stressScaled(p.mark, q);
    if (p.quantity < 0n) { T.shortMicros += notional; T.shorts.push(p.symbol); } else T.longMicros += notional;
    addTo(T.byNameHeld, p.symbol, notional);
    let planned;
    if (p.boundary !== null && p.quantity > 0n) planned = clamp0(p.mark - p.boundary) * q;
    else { planned = ceilDiv(stressed, BPS); T.unprotected.push(p.symbol); }
    book1(p.symbol, p.sector, p.cluster, notional, stressed, planned);
  }
  for (const w of book.workingOrders) {
    if (w.side !== "buy" || w.quantity === 0n) continue;
    const notional = w.limit * w.quantity, stressed = stressScaled(w.limit, w.quantity);
    T.workingBuyMicros += notional; T.workingBuyCashMicros += (w.limit + w.cost) * w.quantity; T.workingBuyCount += 1;
    let planned;
    if (w.boundary !== null && w.boundary <= w.limit) planned = (w.limit - w.boundary + w.cost) * w.quantity;
    else { planned = ceilDiv(stressed, BPS); T.unprotected.push(`${w.symbol}(entry)`); }
    book1(w.symbol, w.sector, w.cluster, notional, stressed, planned);
  }
  T.grossMicros = T.longMicros + T.shortMicros;
  T.netMicros = T.longMicros - T.shortMicros;
  T.grossWithWorkingMicros = T.grossMicros + T.workingBuyMicros;
  T.overnightMicros = T.grossWithWorkingMicros;                  // a DAY entry may still fill before the close
  T.stressedAggMicros = ceilDiv(T.stressedAggScaled, BPS);
  T.settledCashMicros = book.settledCashMinor * MICROS_PER_MINOR;
  T.cashAfterWorkingMicros = (book.settledCashMinor - book.reservedMinor) * MICROS_PER_MINOR - T.workingBuyCashMicros;
  return T;
}

/** Totals → wire exposures (weights HALF_EVEN, losses CEIL so a gate never understates). */
function exposuresFromTotals(book, T) {
  const nav = book.navMicros;
  const bps = (m) => wire(bpsOfNav(m, nav));
  const bpsCeil = (m) => wire(bpsOfNav(m, nav, ROUNDING.CEIL));
  return {
    schemaVersion: SCHEMA_VERSION, navMinor: wire(book.navMinor),
    grossExposureBps: bps(T.grossMicros), netExposureBps: bps(T.netMicros),
    grossWithWorkingBps: bps(T.grossWithWorkingMicros), overnightExposureBps: bps(T.overnightMicros),
    openOrderNotionalBps: bps(T.workingBuyMicros),
    settledCashBps: bps(T.settledCashMicros), cashAfterWorkingBps: bps(T.cashAfterWorkingMicros),
    cashAfterWorkingMinor: wire(M.divRound(T.cashAfterWorkingMicros, MICROS_PER_MINOR, ROUNDING.FLOOR)),
    nameWeightsBps: mapWire(T.byName, bps), sectorWeightsBps: mapWire(T.bySector, bps), clusterWeightsBps: mapWire(T.byCluster, bps),
    plannedLossByNameBps: mapWire(T.plannedByName, bpsCeil), plannedLossAggregateBps: bpsCeil(T.plannedAggMicros),
    stressedLossByNameBps: mapWire(T.stressedScaledByName, (s) => bpsCeil(ceilDiv(s, BPS))),
    stressedLossAggregateBps: bpsCeil(T.stressedAggMicros),
    unprotectedSymbols: T.unprotected.slice(), shortSymbols: T.shorts.slice(),
    positionCount: book.positions.length, workingBuyCount: T.workingBuyCount,
  };
}

/** Every mandate cap the book (with working buys) breaches: [{ constraint, subject, valueBps, limitBps }]. */
function checkConstraints(book, T, P) {
  const nav = book.navMicros, out = [];
  const cap = (constraint, subject, valueMicros, limitBps) => {
    if (valueMicros * BPS > limitBps * nav) out.push({ constraint, subject, valueBps: wire(bpsOfNav(valueMicros, nav, ROUNDING.CEIL)), limitBps: wire(limitBps) });
  };
  cap("maxGrossExposureBps", "book", T.grossWithWorkingMicros, P.weights.maxGross);
  cap("maxNetExposureBps", "book", T.netMicros + T.workingBuyMicros, P.weights.maxNet);
  cap("maxOvernightExposureBps", "book", T.overnightMicros, P.weights.maxOvernight);
  cap("maxOpenOrderNotionalBps", "book", T.workingBuyMicros, P.weights.maxOpenOrder);
  for (const k of Object.keys(T.byName).sort()) cap("maxSingleNameWeightBps", k, T.byName[k], P.weights.maxSingleName);
  for (const k of Object.keys(T.bySector).sort()) cap("maxSectorWeightBps", k, T.bySector[k], P.weights.maxSector);
  for (const k of Object.keys(T.byCluster).sort()) cap("maxCorrelatedClusterWeightBps", k, T.byCluster[k], P.weights.maxCluster);
  for (const k of Object.keys(T.plannedByName).sort()) cap("maxPlannedLossPerPositionBps", k, T.plannedByName[k], P.losses.perPositionPlanned);
  for (const k of Object.keys(T.stressedScaledByName).sort()) cap("maxStressedLossPerPositionBps", k, ceilDiv(T.stressedScaledByName[k], BPS), P.losses.perPositionStressed);
  cap("maxAggregatePlannedLossBps", "book", T.plannedAggMicros, P.losses.aggregatePlanned);
  cap("maxAggregateStressedLossBps", "book", T.stressedAggMicros, P.losses.aggregateStressed);
  if (T.cashAfterWorkingMicros * BPS < P.weights.minCashReserve * nav) {
    out.push({ constraint: "minSettledCashReserveBps", subject: "cash",
      valueBps: wire(bpsOfNav(T.cashAfterWorkingMicros, nav, ROUNDING.FLOOR)), limitBps: wire(P.weights.minCashReserve) });
  }
  return out;
}

/** Public: exposures of a book. `policy` is needed for the gap-halt stress used on unprotected names. */
function exposures({ positions, workingOrders, marks, navMinor, settledCashMinor, reservedMinor, sectorOf, clusterOf, policy } = {}) {
  const P = normalizePolicy(policy);
  const book = normalizeBook({ navMinor, settledCashMinor, reservedMinor, positions, workingOrders }, { marks, sectorOf, clusterOf });
  const T = bookTotals(book, P);
  return { ...exposuresFromTotals(book, T), constraintsBreached: checkConstraints(book, T, P) };
}

/* ── §15.1 quantity authority ───────────────────────────────────────────── */
function normalizeCandidate(candidate, book, advBySymbol) {
  if (!candidate || typeof candidate !== "object") throw err(CODES.CANDIDATE_INVALID, "candidate: expected an object");
  const symbol = upper(candidate.symbol);
  if (!symbol) throw err(CODES.CANDIDATE_INVALID, "candidate.symbol is required");
  const bnd = candidate.lossBoundaryPriceMicros, adv = first(candidate.advMinor, advBySymbol && advBySymbol[symbol]);
  return {
    symbol, side: String(candidate.side || "buy").toLowerCase(), ...book.classify(symbol, candidate.sector),
    proposed: big(candidate.proposedQuantityUnits, `${symbol}.proposedQuantityUnits`),
    limit: big(candidate.limitPriceMicros, `${symbol}.limitPriceMicros`),
    boundary: bnd === undefined || bnd === null ? null : big(bnd, `${symbol}.lossBoundaryPriceMicros`),
    cost: bigOr(candidate.costPerShareMicros, 0n, `${symbol}.costPerShareMicros`),
    adv: adv === null ? null : big(adv, `${symbol}.advMinor`),
    spread: candidate.spreadBps === undefined || candidate.spreadBps === null ? null : big(candidate.spreadBps, `${symbol}.spreadBps`),
  };
}

/** The formula, against already-normalised inputs. Order of `terms` is the precedence on ties. */
function authorize(c, book, T, P) {
  const reasons = [], refuse = (code, detail) => reasons.push({ code, detail });
  if (P.instruments.longOnly && c.side !== "buy") refuse(REASONS.LONG_ONLY, `side ${c.side}`);
  if (c.proposed <= 0n) refuse(REASONS.PROPOSAL_NOT_POSITIVE, `proposed ${c.proposed}`);
  if (c.limit <= 0n) refuse(REASONS.LIMIT_NOT_POSITIVE, `limit ${c.limit}`);
  if (c.boundary === null) refuse(REASONS.PROTECTION_REQUIRED, "no lossBoundaryPriceMicros");
  else if (c.boundary <= 0n) refuse(REASONS.BOUNDARY_NOT_POSITIVE, `boundary ${c.boundary}`);
  else if (c.limit <= c.boundary) refuse(REASONS.LIMIT_NOT_ABOVE_BOUNDARY, `limit ${c.limit} ≤ boundary ${c.boundary}`);
  if (c.spread === null) refuse(REASONS.SPREAD_UNKNOWN, "spreadBps missing");
  else if (c.spread > P.liquidity.maxSpread) refuse(REASONS.SPREAD_TOO_WIDE, `${c.spread} > ${P.liquidity.maxSpread} bps`);
  if (c.adv === null) refuse(REASONS.ADV_UNKNOWN, "advMinor missing");
  else if (c.adv < P.liquidity.minAdvMinor) refuse(REASONS.ADV_BELOW_MINIMUM, `${c.adv} < ${P.liquidity.minAdvMinor} minor`);
  if (!P.instruments.increaseAction && at(T.byName, c.symbol) > 0n) refuse(REASONS.INCREASE_NOT_PERMITTED, "name already held or working");
  const base = { symbol: c.symbol, sector: c.sector, cluster: c.cluster, proposedQuantityUnits: wire(c.proposed) };
  if (reasons.length) {
    return { ...base, authorizedQuantityUnits: "0", bindingConstraint: "REFUSED", capacities: {}, perShare: {}, reasons, refused: true, ruleRefusal: true };
  }
  const nav = book.navMicros, gap = P.stress.gapHaltAdverse, sc = P.stress.stressCostPerShare;
  const perShareCash = c.limit + c.cost;
  const perSharePlanned = c.limit - c.boundary + c.cost;
  const perShareStressScaled = c.limit * gap + sc * BPS;             // (limit − gapHaltFill + stressCost) × BPS
  const advMicros = c.adv * MICROS_PER_MINOR;
  const cashCap = (book.settledCashMinor - book.reservedMinor) * MICROS_PER_MINOR - T.workingBuyCashMicros - share(nav, P.weights.minCashReserve);
  const plannedCap = minBig(share(nav, P.losses.perPositionPlanned), share(nav, P.losses.aggregatePlanned) - T.plannedAggMicros);
  const stressCap = minBig(share(nav, P.losses.perPositionStressed), share(nav, P.losses.aggregateStressed) - T.stressedAggMicros);
  const terms = [
    ["proposal", c.proposed],
    ["cash", floorUnits(cashCap, perShareCash)],
    ["name", floorUnits(share(nav, P.weights.maxSingleName) - at(T.byName, c.symbol), c.limit)],
    ["sector", floorUnits(share(nav, P.weights.maxSector) - at(T.bySector, c.sector), c.limit)],
    ["cluster", floorUnits(share(nav, P.weights.maxCluster) - at(T.byCluster, c.cluster), c.limit)],
    ["plannedLoss", floorUnits(plannedCap, perSharePlanned)],
    ["stressedLoss", stressCap <= 0n ? 0n : floorUnits(stressCap * BPS, perShareStressScaled)],
    ["advOrder", floorUnits(share(advMicros, P.liquidity.maxOrderPctOfAdv), c.limit)],
    ["advPosition", floorUnits(share(advMicros, P.liquidity.maxPositionPctOfAdv) - at(T.byName, c.symbol), c.limit)],
    ["overnight", floorUnits(share(nav, P.weights.maxOvernight) - T.overnightMicros, c.limit)],
  ];
  let binding = "proposal", least = c.proposed;
  for (const [k, v] of terms) if (v < least) { least = v; binding = k; }
  const authorized = M.lotFloor(least, P.instruments.lotSize);
  if (authorized > c.proposed || authorized < 0n) throw err(CODES.INVARIANT_VIOLATION, `authorized ${authorized} vs proposed ${c.proposed}`);
  if (binding !== "proposal") reasons.push({ code: REASONS.CLAMPED, detail: `${binding} capacity ${least} < proposed ${c.proposed}` });
  if (authorized < least) reasons.push({ code: REASONS.LOT_FLOOR, detail: `${least} → ${authorized} (lot ${P.instruments.lotSize})` });
  if (authorized === 0n) reasons.push({ code: REASONS.ZERO_CAPACITY, detail: `${binding} capacity is zero` });
  const capacities = {}; for (const [k, v] of terms) capacities[k] = wire(v);
  return {
    ...base, authorizedQuantityUnits: wire(authorized), bindingConstraint: binding, capacities,
    perShare: { cashMicros: wire(perShareCash), plannedLossMicros: wire(perSharePlanned),
      stressedLossMicros: wire(ceilDiv(perShareStressScaled, BPS)), gapHaltStressFillMicros: wire(c.limit - share(c.limit, gap)) },
    reasons, refused: authorized === 0n, ruleRefusal: false,
  };
}

/** Public: Q_authorized for one candidate against the current book (§15.1). Only ever smaller than proposed. */
function quantityAuthority({ candidate, portfolio, policy, marks, advBySymbol, sectorOf, clusterOf } = {}) {
  const P = normalizePolicy(policy);
  const book = normalizeBook(portfolio, { marks, sectorOf, clusterOf });
  return authorize(normalizeCandidate(candidate, book, advBySymbol), book, bookTotals(book, P), P);
}

/* ── §6.8 clamp materiality ─────────────────────────────────────────────── */
/** Material: a rejection/zeroing; any change of action, rank, price, boundary, session or
 *  membership (boundary equality counts when boundaryEqualityMaterial); or a whole-share
 *  delta ≥ minDeltaUnits with deltaPct ≥ 5 % of the proposal OR deltaNav ≥ 25 bps.
 *  Only a smaller whole-share rounding below both relative thresholds is silent. */
function clampMateriality({ proposedQuantityUnits, authorizedQuantityUnits, limitPriceMicros, navMinor, policy,
  boundaryChanged = false, membershipChanged = false, actionChanged = false, rankChanged = false,
  priceChanged = false, sessionChanged = false, boundaryTouched = false } = {}) {
  const P = normalizePolicy(policy);
  const proposed = big(proposedQuantityUnits, "proposedQuantityUnits"), authorized = big(authorizedQuantityUnits, "authorizedQuantityUnits");
  const limit = big(limitPriceMicros, "limitPriceMicros"), nav = big(navMinor, "navMinor") * MICROS_PER_MINOR;
  if (nav <= 0n) throw err(CODES.NAV_NOT_POSITIVE, "navMinor must be positive");
  if (authorized < 0n || authorized > proposed) throw err(CODES.INVARIANT_VIOLATION, `authorized ${authorized} outside [0, proposed ${proposed}]`);
  const delta = proposed - authorized, deltaNavMicros = delta * limit;
  const deltaPctBps = proposed > 0n ? M.divRound(delta * BPS, proposed, ROUNDING.HALF_EVEN) : 0n;
  const deltaNavBps = bpsOfNav(deltaNavMicros, nav);
  const changed = Object.entries({ actionChanged, rankChanged, priceChanged, boundaryChanged, sessionChanged, membershipChanged,
    boundaryTouched: boundaryTouched && P.clamp.boundaryEqualityMaterial }).filter(([, v]) => v).map(([k]) => k);
  let material, reason;
  if (proposed > 0n && authorized === 0n) { material = true; reason = "REJECTED"; }
  else if (changed.length) { material = true; reason = `CHANGED:${changed.join(",")}`; }
  else if (delta === 0n) { material = false; reason = "NO_CHANGE"; }
  else if (delta >= P.clamp.minDeltaUnits && (delta * BPS >= P.clamp.deltaPctOfProposal * proposed || deltaNavMicros * BPS >= P.clamp.deltaNav * nav)) {
    material = true; reason = "DELTA_ABOVE_THRESHOLD";
  } else { material = false; reason = "SILENT_ROUNDING"; }
  return { material, silent: !material, deltaUnits: wire(delta), deltaPctOfProposalBps: wire(deltaPctBps), deltaNavBps: wire(deltaNavBps), reason,
    thresholds: { minDeltaUnits: wire(P.clamp.minDeltaUnits), deltaPctOfProposalBps: wire(P.clamp.deltaPctOfProposal), deltaNavBps: wire(P.clamp.deltaNav) } };
}

/* ── scenarios (read-only measurements for Sol; no ranking, no scores) ──── */
function scenarioOf(book, P, entries) {
  const added = entries.filter((e) => e.quantity > 0n).map((e) => ({ symbol: e.c.symbol, side: "buy", sector: e.c.sector, cluster: e.c.cluster,
    quantity: e.quantity, limit: e.c.limit, boundary: e.c.boundary, cost: e.c.cost }));
  const b = { ...book, workingOrders: book.workingOrders.concat(added) };
  const T = bookTotals(b, P), constraintsBreached = checkConstraints(b, T, P);
  return { T, exposures: exposuresFromTotals(b, T), constraintsBreached, feasible: constraintsBreached.length === 0 };
}

function candidateView(book, P, c, quantity, current, authority) {
  const s = scenarioOf(book, P, [{ c, quantity }]);
  const d = (a, b2) => wire(bpsOfNav(a - b2, book.navMicros));
  const orderMicros = c.limit * quantity, positionMicros = at(current.T.byName, c.symbol) + orderMicros;
  const advMicros = c.adv === null || c.adv <= 0n ? null : c.adv * MICROS_PER_MINOR;
  const pctAdv = (m) => (advMicros === null ? null : wire(M.divRound(m * BPS, advMicros, ROUNDING.CEIL)));
  const minReserveMicros = share(book.navMicros, P.weights.minCashReserve);
  return {
    quantityUnits: wire(quantity), exposures: s.exposures, constraintsBreached: s.constraintsBreached, feasible: s.feasible,
    marginal: { nameBps: d(at(s.T.byName, c.symbol), at(current.T.byName, c.symbol)), sectorBps: d(at(s.T.bySector, c.sector), at(current.T.bySector, c.sector)),
      clusterBps: d(at(s.T.byCluster, c.cluster), at(current.T.byCluster, c.cluster)), grossBps: d(s.T.grossWithWorkingMicros, current.T.grossWithWorkingMicros),
      plannedLossBps: d(s.T.plannedAggMicros, current.T.plannedAggMicros), stressedLossBps: d(s.T.stressedAggMicros, current.T.stressedAggMicros) },
    liquidity: { orderPctOfAdvBps: pctAdv(orderMicros), positionPctOfAdvBps: pctAdv(positionMicros),
      daysToExitAt10PctAdv: advMicros === null ? null : wire(ceilDiv(positionMicros * 10n, advMicros)), advMinor: c.adv === null ? null : wire(c.adv),
      spreadBps: c.spread === null ? null : wire(c.spread) },
    cashDrag: { cashAfterBps: s.exposures.cashAfterWorkingBps, minReserveBps: wire(P.weights.minCashReserve),
      idleCashBps: wire(bpsOfNav(s.T.cashAfterWorkingMicros - minReserveMicros, book.navMicros, ROUNDING.FLOOR)),
      idleCashMinor: wire(M.divRound(s.T.cashAfterWorkingMicros - minReserveMicros, MICROS_PER_MINOR, ROUNDING.FLOOR)) },
    stress: { gapHaltStressFillMicros: authority.perShare.gapHaltStressFillMicros || null,
      namePlannedLossBps: s.exposures.plannedLossByNameBps[c.symbol] || "0", nameStressedLossBps: s.exposures.stressedLossByNameBps[c.symbol] || "0",
      aggregatePlannedLossBps: s.exposures.plannedLossAggregateBps, aggregateStressedLossBps: s.exposures.stressedLossAggregateBps },
  };
}

/** Public: the current book, each candidate alone (at proposed and at authorized quantity) and the
 *  candidate SET together. Authority is computed against the CURRENT book for every candidate —
 *  never sequentially, which would imply an order. Output holds measurements and booleans only. */
function runScenarios({ portfolio, candidates, policy, marks, advBySymbol, sectorOf, clusterOf } = {}) {
  const P = normalizePolicy(policy);
  const book = normalizeBook(portfolio, { marks, sectorOf, clusterOf });
  const cs = (candidates || []).map((c) => normalizeCandidate(c, book, advBySymbol));
  const current = scenarioOf(book, P, []);
  const rows = cs.map((c) => {
    const authority = authorize(c, book, current.T, P);
    const authorized = BigInt(authority.authorizedQuantityUnits);
    return { symbol: c.symbol, sector: c.sector, cluster: c.cluster, authority,
      atProposed: candidateView(book, P, c, c.proposed, current, authority),
      atAuthorized: candidateView(book, P, c, authorized, current, authority) };
  });
  const setAt = (pick) => { const s = scenarioOf(book, P, cs.map((c, i) => ({ c, quantity: pick(c, rows[i]) }))); delete s.T; return s; };
  const atProposed = setAt((c) => c.proposed), atAuthorized = setAt((c, r) => BigInt(r.authority.authorizedQuantityUnits));
  const refusedSymbols = rows.filter((r) => r.authority.refused).map((r) => r.symbol);
  const tooMany = cs.length > P.maxActionableBuysPerRun;
  const setBreaches = atAuthorized.constraintsBreached.concat(tooMany
    ? [{ constraint: "maxActionableBuysPerRun", subject: "basket", valueBps: String(cs.length), limitBps: String(P.maxActionableBuysPerRun) }] : []);
  return {
    schemaVersion: SCHEMA_VERSION, navMinor: wire(book.navMinor),
    current: { exposures: current.exposures, constraintsBreached: current.constraintsBreached, feasible: current.feasible },
    candidates: rows,
    set: { count: cs.length, maxActionableBuysPerRun: P.maxActionableBuysPerRun, atProposed, atAuthorized, refusedSymbols,
      constraintsBreached: setBreaches, basketFeasibleAtProposed: atProposed.feasible && !tooMany,
      basketFeasible: cs.length > 0 && refusedSymbols.length === 0 && setBreaches.length === 0 },
  };
}

/** researchResult → candidate when it carries a BUY mandate, else null. Tolerant of
 *  { mandate | proposal | flat } with allocation / entry / protection / liquidity blocks. */
function candidateFromResearch(r, advBySymbol) {
  if (!r || typeof r !== "object") return null;
  const m = r.mandate || r.proposal || r;
  if (!/BUY/.test(upper(m.action || m.decision || r.action))) return null;
  const symbol = upper(first(r.symbol, m.symbol));
  return {
    symbol, sector: first(r.sector, m.sector), side: "buy",
    proposedQuantityUnits: first(get(m, "allocation.proposedQuantityUnits"), m.proposedQuantityUnits),
    limitPriceMicros: first(get(m, "entry.limitPriceMicros"), m.limitPriceMicros),
    lossBoundaryPriceMicros: first(get(m, "protection.lossBoundaryPriceMicros"), m.lossBoundaryPriceMicros),
    costPerShareMicros: first(get(m, "entry.costPerShareMicros"), m.costPerShareMicros, r.costPerShareMicros, "0"),
    advMinor: first(r.advMinor, get(r, "liquidity.advMinor"), get(m, "liquidity.advMinor"), advBySymbol && advBySymbol[symbol]),
    spreadBps: first(r.spreadBps, get(r, "liquidity.spreadBps"), get(m, "liquidity.spreadBps")),
  };
}

/** Public: for every researched BUY mandate, its authority + clamp materiality, plus the
 *  set-level scenario. Input order is preserved; nothing is chosen, dropped or reordered. */
function buildFeasibleAlternatives({ researchResults, holdings, portfolio, policy, marks, advBySymbol, sectorOf, clusterOf } = {}) {
  const held = new Set((holdings || []).map((h) => upper(typeof h === "string" ? h : h && h.symbol)).filter(Boolean));
  const skipped = [], candidates = [];
  (researchResults || []).forEach((r, i) => {
    const c = candidateFromResearch(r, advBySymbol);
    if (c && c.symbol) candidates.push(c); else skipped.push({ index: i, symbol: upper(r && r.symbol) || null, reason: "NOT_A_BUY_MANDATE" });
  });
  const scenarios = runScenarios({ portfolio, candidates, policy, marks, advBySymbol, sectorOf, clusterOf });
  const alternatives = scenarios.candidates.map((row, i) => ({
    symbol: row.symbol, sector: row.sector, cluster: row.cluster, action: "BUY", alreadyHeld: held.has(row.symbol),
    candidate: candidates[i], authority: row.authority,
    materiality: clampMateriality({ proposedQuantityUnits: row.authority.proposedQuantityUnits, authorizedQuantityUnits: row.authority.authorizedQuantityUnits,
      limitPriceMicros: candidates[i].limitPriceMicros, navMinor: scenarios.navMinor, policy }),
    atProposed: row.atProposed, atAuthorized: row.atAuthorized,
  }));
  return { schemaVersion: SCHEMA_VERSION, navMinor: scenarios.navMinor, current: scenarios.current, alternatives, skipped,
    set: scenarios.set, basketFeasible: scenarios.set.basketFeasible };
}

/* ── §8.7 emergency reduction order ─────────────────────────────────────── */
const msBig = (v) => (typeof v === "number" && Number.isSafeInteger(v) ? BigInt(v) : M.isCanonicalIntegerString(v) ? BigInt(v) : null);

/** Validate Sol's emergency ranks: unique positive ranks, known symbols, unexpired vs nowMs. → { ok, reason, byLabel }. */
function validateRanks(ranks, known, nowMs) {
  if (!Array.isArray(ranks) || ranks.length === 0) return { ok: false, reason: "RANKS_ABSENT", bySymbol: {} };
  const now = msBig(nowMs), bySymbol = {}, seen = new Set();
  for (const r of ranks) {
    const symbol = upper(r && r.symbol), rank = msBig(r && r.rank), exp = msBig(r && first(r.expiresAtMs, r.expiresAt));
    if (!symbol || !known.has(symbol)) return { ok: false, reason: `RANK_SYMBOL_UNKNOWN:${symbol || "?"}`, bySymbol: {} };
    if (rank === null || rank <= 0n) return { ok: false, reason: `RANK_INVALID:${symbol}`, bySymbol: {} };
    if (seen.has(rank) || bySymbol[symbol]) return { ok: false, reason: `RANK_DUPLICATE:${symbol}`, bySymbol: {} };
    if (exp === null || now === null || exp <= now) return { ok: false, reason: `RANK_EXPIRED_OR_UNVERIFIABLE:${symbol}`, bySymbol: {} };
    seen.add(rank); bySymbol[symbol] = rank;
  }
  return { ok: true, reason: null, bySymbol };
}

/** Public, PURE: the order in which to reduce. Accidental shorts always come first (BUY_TO_COVER);
 *  then Sol's valid ranks, or the fallback: smallest liquid set with greatest binding stressed-loss
 *  relief per dollar (breaching names first, liquid before illiquid, relief/dollar desc, days-to-exit asc). */
function emergencyReductionOrder({ positions, ranks, marks, advBySymbol, policy, navMinor, nowMs } = {}) {
  const P = normalizePolicy(policy), mk = marks || {}, adv = advBySymbol || {};
  const nav = navMinor === undefined || navMinor === null ? null : big(navMinor, "navMinor") * MICROS_PER_MINOR;
  const rows = (positions || []).map((p, i) => {
    const symbol = upper(p && p.symbol), quantity = big(p.quantityUnits, `positions[${i}].quantityUnits`);
    const markRaw = first(mk[symbol], p.markMicros);
    if (!symbol || markRaw === null) throw err(CODES.MARK_MISSING, `positions[${i}] ${symbol || "?"}: no mark`);
    const mark = big(markRaw, `positions[${i}].markMicros`), q = absBig(quantity), notional = mark * q;
    const advRaw = first(adv[symbol], p.advMinor), advMicros = advRaw === null ? null : big(advRaw, `${symbol}.advMinor`) * MICROS_PER_MINOR;
    const stressedScaled = (mark * P.stress.gapHaltAdverse + P.stress.stressCostPerShare * BPS) * q;
    return {
      symbol, short: quantity < 0n, quantity: q, notional, stressedScaled,
      reliefPerDollarBps: notional > 0n ? M.divRound(stressedScaled, notional, ROUNDING.HALF_EVEN) : 0n,
      liquid: advMicros !== null && advMicros >= P.liquidity.minAdvMinor * MICROS_PER_MINOR,
      daysToExit: advMicros === null || advMicros <= 0n ? null : ceilDiv(notional * 10n, advMicros),
      breaching: nav !== null && stressedScaled > P.losses.perPositionStressed * nav,
    };
  }).filter((r) => r.quantity > 0n);
  const v = validateRanks(ranks, new Set(rows.map((r) => r.symbol)), nowMs);
  const cmpBig = (a, b) => (a < b ? -1 : a > b ? 1 : 0);
  const fallback = (a, b) => (Number(b.breaching) - Number(a.breaching)) || (Number(b.liquid) - Number(a.liquid))
    || cmpBig(b.reliefPerDollarBps, a.reliefPerDollarBps)
    || (a.daysToExit === null ? (b.daysToExit === null ? 0 : 1) : b.daysToExit === null ? -1 : cmpBig(a.daysToExit, b.daysToExit))
    || cmpBig(b.notional, a.notional) || (a.symbol < b.symbol ? -1 : a.symbol > b.symbol ? 1 : 0);
  const byRank = (a, b) => {
    const ra = v.bySymbol[a.symbol] || null, rb = v.bySymbol[b.symbol] || null;
    if (ra !== null && rb !== null) return cmpBig(ra, rb);
    if (ra !== null || rb !== null) return ra !== null ? -1 : 1;
    return fallback(a, b);
  };
  const shorts = rows.filter((r) => r.short).sort((a, b) => cmpBig(b.notional, a.notional) || (a.symbol < b.symbol ? -1 : 1));
  const longs = rows.filter((r) => !r.short).sort(v.ok ? byRank : fallback);
  const order = shorts.concat(longs).map((r, i) => ({
    sequence: i + 1, symbol: r.symbol, side: r.short ? "BUY_TO_COVER" : "SELL", quantityUnits: wire(r.quantity),
    notionalMinor: wire(M.divRound(r.notional, MICROS_PER_MINOR, ROUNDING.HALF_EVEN)),
    stressedLossMinor: wire(ceilDiv(r.stressedScaled, BPS * MICROS_PER_MINOR)), reliefPerDollarBps: wire(r.reliefPerDollarBps),
    daysToExitAt10PctAdv: r.daysToExit === null ? null : wire(r.daysToExit), liquid: r.liquid, perPositionStressBreached: r.breaching,
    solRank: v.ok && v.bySymbol[r.symbol] ? wire(v.bySymbol[r.symbol]) : null,
    reason: r.short ? "ACCIDENTAL_SHORT" : v.ok && v.bySymbol[r.symbol] ? "SOL_EMERGENCY_RANK" : "STRESS_RELIEF_PER_DOLLAR",
  }));
  return { schemaVersion: SCHEMA_VERSION, ruleUsed: v.ok ? EMERGENCY_RULES.SOL_RANKS : EMERGENCY_RULES.FALLBACK,
    ranksAccepted: v.ok, ranksRejectedReason: v.reason, order };
}

/* ── self check ─────────────────────────────────────────────────────────── */
/** Mirrors the §15.1 paper defaults of _investorPolicy.RISK_MANDATE (inlined so the exact
 *  expected unit counts below cannot drift with a policy edit). */
const TEST_POLICY = Object.freeze({
  weights: { maxSingleNameWeightBps: "1000", maxSectorWeightBps: "2500", maxCorrelatedClusterWeightBps: "2000", maxGrossExposureBps: "9500",
    maxNetExposureBps: "9500", minSettledCashReserveBps: "500", maxOvernightExposureBps: "9500", maxOpenOrderNotionalBps: "3000" },
  losses: { maxPlannedLossPerPositionBps: "100", maxAggregatePlannedLossBps: "500", maxStressedLossPerPositionBps: "250", maxAggregateStressedLossBps: "1000" },
  liquidity: { maxOrderPctOfAdvBps: "100", maxPositionPctOfAdvBps: "500", maxSpreadBps: "50", minAdvMinor: "5000000000" },
  stress: { gapHaltAdverseBps: "1500", stressCostPerShareMicros: "20000", overnightGapBps: "800" },
  instruments: { longOnly: true, wholeSharesOnly: true, increaseAction: false },
  clampMateriality: { minDeltaUnits: "1", deltaPctOfProposalBps: "500", deltaNavBps: "25", boundaryEqualityMaterial: true },
  activation: { maxActionableBuysPerRun: 8 },
});

function selfCheck() {
  const t0 = Date.now(), failures = [];
  const check = (name, ok, actual) => { if (!ok) failures.push({ name, actual }); };
  const policy = TEST_POLICY;
  const book = (over) => ({ navMinor: "10000000", settledCashMinor: "10000000", reservedMinor: "0", positions: [], workingOrders: [], ...over });
  const cand = (over) => ({ symbol: "AMAT", sector: "tech", proposedQuantityUnits: "1000", limitPriceMicros: "50000000",
    lossBoundaryPriceMicros: "45000000", costPerShareMicros: "10000", advMinor: "10000000000", spreadBps: "20", ...over });
  const qa = (candidate, portfolio) => quantityAuthority({ candidate, portfolio, policy });
  const bound = (name, r, units, binding) => check(name, r.authorizedQuantityUnits === units && r.bindingConstraint === binding && !r.refused,
    { authorized: r.authorizedQuantityUnits, binding: r.bindingConstraint, capacities: r.capacities });
  try {
    // §15.1 binding terms — exact expected whole-share counts, hand computed.
    bound("cash-bound", qa(cand({ proposedQuantityUnits: "100" }), book({ settledCashMinor: "800000" })), "59", "cash");
    bound("name-bound", qa(cand({ lossBoundaryPriceMicros: "48000000" }), book()), "200", "name");
    bound("planned-loss-bound", qa(cand({ lossBoundaryPriceMicros: "40000000" }), book()), "99", "plannedLoss");
    bound("adv-bound", qa(cand({ proposedQuantityUnits: "15000", advMinor: "6000000000" }),
      book({ navMinor: "1000000000", settledCashMinor: "1000000000" })), "12000", "advOrder");
    bound("sector-bound", qa(cand({ lossBoundaryPriceMicros: "48000000" }), book({ settledCashMinor: "8000000",
      positions: [{ symbol: "NVDA", sector: "tech", quantityUnits: "400", markMicros: "50000000", lossBoundaryPriceMicros: "45000000" }] })), "100", "sector");
    const unclamped = qa(cand({ proposedQuantityUnits: "50" }), book());
    bound("proposal-not-clamped", unclamped, "50", "proposal");
    check("proposal-not-clamped has no reasons", unclamped.reasons.length === 0, unclamped.reasons);
    // hard refusals
    const refusal = (name, c, code) => { const r = qa(cand(c), book()); check(name, r.refused && r.ruleRefusal && r.authorizedQuantityUnits === "0"
      && r.reasons.some((x) => x.code === code), r); };
    refusal("spread refusal", { spreadBps: "60" }, REASONS.SPREAD_TOO_WIDE);
    refusal("adv refusal", { advMinor: "4000000000" }, REASONS.ADV_BELOW_MINIMUM);
    refusal("limit ≤ boundary refusal", { lossBoundaryPriceMicros: "50000000" }, REASONS.LIMIT_NOT_ABOVE_BOUNDARY);
    refusal("protection required", { lossBoundaryPriceMicros: null }, REASONS.PROTECTION_REQUIRED);
    // §6.8 clamp materiality, 1/2/3-share fixtures and the silent case
    const cm = (p, a, extra) => clampMateriality({ proposedQuantityUnits: p, authorizedQuantityUnits: a, limitPriceMicros: "50000000", navMinor: "10000000", policy, ...extra });
    const cmCase = (name, p, a, material, reason, extra) => { const r = cm(p, a, extra); check(name, r.material === material && r.silent === !material && r.reason === reason, r); };
    cmCase("clamp 3→2 material (33%)", "3", "2", true, "DELTA_ABOVE_THRESHOLD");
    cmCase("clamp 2→1 material (50%)", "2", "1", true, "DELTA_ABOVE_THRESHOLD");
    cmCase("clamp 1→0 rejection", "1", "0", true, "REJECTED");
    cmCase("clamp 3→3 no change", "3", "3", false, "NO_CHANGE");
    cmCase("clamp 100→96 silent (4%, 20 bps)", "100", "96", false, "SILENT_ROUNDING");
    cmCase("clamp 100→95 material (5%)", "100", "95", true, "DELTA_ABOVE_THRESHOLD");
    cmCase("boundary change material", "3", "3", true, "CHANGED:boundaryChanged", { boundaryChanged: true });
    cmCase("boundary equality material", "3", "3", true, "CHANGED:boundaryTouched", { boundaryTouched: true });
    check("clamp 100→96 deltas", JSON.stringify([cm("100", "96").deltaPctOfProposalBps, cm("100", "96").deltaNavBps]) === JSON.stringify(["400", "20"]), cm("100", "96"));
    // exposures sums
    const ex = exposures({ policy, navMinor: "10000000", settledCashMinor: "9000000", positions: [
      { symbol: "AAA", sector: "tech", quantityUnits: "100", markMicros: "50000000", lossBoundaryPriceMicros: "45000000" },
      { symbol: "BBB", sector: "energy", quantityUnits: "200", markMicros: "25000000", lossBoundaryPriceMicros: null }] });
    const sumBps = (m) => Object.values(m).reduce((s, v) => s + BigInt(v), 0n);
    check("exposures gross/net", ex.grossExposureBps === "1000" && ex.netExposureBps === "1000" && ex.settledCashBps === "9000", ex);
    check("exposures name weights sum to gross", sumBps(ex.nameWeightsBps) === 1000n && ex.nameWeightsBps.AAA === "500", ex.nameWeightsBps);
    check("exposures sector/cluster sums", sumBps(ex.sectorWeightsBps) === 1000n && sumBps(ex.clusterWeightsBps) === 1000n, ex);
    check("exposures planned loss (protected 50 bps + unprotected at gap-halt 76 bps)", ex.plannedLossByNameBps.AAA === "50"
      && ex.plannedLossByNameBps.BBB === "76" && ex.plannedLossAggregateBps === "126" && ex.unprotectedSymbols.join() === "BBB", ex);
    check("exposures stressed loss", ex.stressedLossByNameBps.AAA === "76" && ex.stressedLossAggregateBps === "151", ex);
    // basket: each candidate passes alone, the set breaches the sector cap
    const three = ["NVDA", "AMAT", "MSFT"].map((symbol) => cand({ symbol, proposedQuantityUnits: "200", lossBoundaryPriceMicros: "48000000" }));
    const sc = runScenarios({ portfolio: book(), candidates: three, policy });
    check("basket singles feasible", sc.candidates.every((r) => r.atAuthorized.feasible && r.authority.authorizedQuantityUnits === "200"), sc.candidates.map((r) => r.authority));
    check("basket set infeasible on sector cap", !sc.set.basketFeasible && sc.set.constraintsBreached.length === 1
      && sc.set.constraintsBreached[0].constraint === "maxSectorWeightBps" && sc.set.constraintsBreached[0].subject === "tech", sc.set.constraintsBreached);
    check("scenario marginal sector 1000 bps", sc.candidates[0].atProposed.marginal.sectorBps === "1000" && sc.candidates[0].atProposed.liquidity.orderPctOfAdvBps === "1", sc.candidates[0].atProposed);
    check("scenario output carries no score/rank", !JSON.stringify(sc).match(/"(score|rank|ranking|preferred)"/), null);
    const alt = buildFeasibleAlternatives({ policy, portfolio: book(), researchResults: [
      { symbol: "NVDA", sector: "tech", mandate: { action: "BUY", allocation: { proposedQuantityUnits: "3" }, entry: { limitPriceMicros: "50000000", costPerShareMicros: "10000" },
        protection: { lossBoundaryPriceMicros: "48000000" }, liquidity: { advMinor: "10000000000", spreadBps: "20" } } },
      { symbol: "XOM", sector: "energy", mandate: { action: "HOLD" } }] });
    check("alternatives: one BUY kept, HOLD skipped, order preserved", alt.alternatives.length === 1 && alt.alternatives[0].symbol === "NVDA"
      && alt.skipped.length === 1 && alt.alternatives[0].materiality.reason === "NO_CHANGE" && alt.basketFeasible === true, alt);
    // emergency order: fallback labelling and Sol ranks
    const positions = [{ symbol: "XXX", quantityUnits: "-10", markMicros: "20000000" }, { symbol: "AAA", quantityUnits: "100", markMicros: "50000000" },
      { symbol: "BBB", quantityUnits: "200", markMicros: "25000000" }];
    const advBySymbol = { AAA: "10000000000", BBB: "10000000000", XXX: "10000000000" };
    const fb = emergencyReductionOrder({ positions, advBySymbol, policy, nowMs: 1000, navMinor: "10000000",
      ranks: [{ symbol: "AAA", rank: "1", expiresAtMs: 2000 }, { symbol: "BBB", rank: "1", expiresAtMs: 2000 }] });
    check("emergency fallback on duplicate ranks", fb.ruleUsed === EMERGENCY_RULES.FALLBACK && !fb.ranksAccepted && fb.ranksRejectedReason === "RANK_DUPLICATE:BBB"
      && fb.order.map((o) => o.symbol).join() === "XXX,BBB,AAA" && fb.order[0].side === "BUY_TO_COVER", fb);
    const ok = emergencyReductionOrder({ positions, advBySymbol, policy, nowMs: 1000,
      ranks: [{ symbol: "AAA", rank: "1", expiresAtMs: 2000 }, { symbol: "BBB", rank: "2", expiresAtMs: 2000 }] });
    check("emergency Sol ranks honoured after shorts", ok.ruleUsed === EMERGENCY_RULES.SOL_RANKS && ok.order.map((o) => o.symbol).join() === "XXX,AAA,BBB", ok);
    const expired = emergencyReductionOrder({ positions, advBySymbol, policy, nowMs: 3000, ranks: [{ symbol: "AAA", rank: "1", expiresAtMs: 2000 }] });
    check("emergency expired ranks fall back", expired.ruleUsed === EMERGENCY_RULES.FALLBACK, expired);
  } catch (e) {
    failures.push({ name: "threw", actual: { code: e.code || null, message: String(e.message || e) } });
  }
  return { pass: failures.length === 0, failures, ms: Date.now() - t0 };
}

module.exports = {
  SCHEMA_VERSION, CODES, REASONS, RULE_REFUSALS, EMERGENCY_RULES, TEST_POLICY,
  normalizePolicy, exposures, quantityAuthority, clampMateriality, runScenarios,
  candidateFromResearch, buildFeasibleAlternatives, emergencyReductionOrder, selfCheck,
};
