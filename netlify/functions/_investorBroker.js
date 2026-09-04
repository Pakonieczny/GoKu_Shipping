/*  netlify/functions/_investorBroker.js  (fund-manager-v1)
 *  ---------------------------------------------------------------------------
 *  Investor AI — the broker-independent interface and both adapters
 *  (blueprint §8.2, §8.4, §12.1, §19.3).
 *
 *  One interface, two adapters:
 *    PaperBrokerAdapter   runs under PAPER_AI. Orders live in Firestore
 *                         (OrderLegs); fills come from the deterministic OHLC
 *                         simulator in _investorExecution against stored bars.
 *    AlpacaBrokerAdapter  the live candidate. Bound only when the operator
 *                         switches to LIMITED_LIVE and the environment names
 *                         it (INVESTOR_LIVE_BROKER=alpaca). Until then every
 *                         call throws LIVE_ADAPTER_DISABLED. Polling and
 *                         reconciliation based: broker-native orders catch the
 *                         price between polls; no streaming in v1.
 *
 *  Every adapter publishes a versioned capability record; configuration may
 *  only disable a capability, never claim one. The validator refuses any
 *  mandate/order combination the bound adapter cannot implement exactly.
 *  Nothing here decides anything about companies, sizes or prices.
 * ---------------------------------------------------------------------------
 */

"use strict";

const crypto = require("crypto");
const A = require("./_investorAdmin");

const INTERFACE_VERSION = "broker-adapter.v1";
const METHODS = Object.freeze(["getCapabilities", "getAccountSnapshot", "getSessionCalendar", "submitOrderSet", "replaceOrderSet", "cancelOrderSet",
  "getOrderSet", "listOpenOrders", "listPositions", "listFills", "reconcile"]);

/* ── capability matrix (§8.2) — a record, never a hope ─────────────────── */
const CAPABILITY_MATRIX = Object.freeze({
  paper: Object.freeze({
    adapter: "paper", capabilityVersion: "paper-capabilities.v1",
    dayLimitEntry: true, nativeOcoForOwnedPosition: true, bracketProtectsPartialFill: "simulatable",
    trailingStopInsideOco: "policy_gated", extendedHoursProtection: "simulatable_with_data", atomicGroupReplacement: true,
    reduceOnlyEquityExit: true, streamingFillNotification: "deterministic_simulator", gtdOrders: false,
    protectionStatusLabel: "PROTECTED_RTH", regularSessionOnly: true, wholeSharesOnly: true, fractionalShares: false,
  }),
  alpaca: Object.freeze({
    adapter: "alpaca", capabilityVersion: "alpaca-capabilities.v1",
    dayLimitEntry: true, nativeOcoForOwnedPosition: true, bracketProtectsPartialFill: false,
    trailingStopInsideOco: false, extendedHoursProtection: false, atomicGroupReplacement: false,
    reduceOnlyEquityExit: false, streamingFillNotification: false, gtdOrders: false,
    protectionStatusLabel: "PROTECTED_RTH", regularSessionOnly: true, wholeSharesOnly: true, fractionalShares: false,
    warnings: ["both OCO exits can fill before cancellation in extreme markets", "bracket exits activate only after complete entry fill", "bracket/OCO regular session only"],
  }),
});
/** PURE. Configuration may disable a capability, never enable one. */
function applyCapabilityConfig(base, config = {}) {
  const out = { ...base, disabledByConfig: [] };
  for (const [k, v] of Object.entries(config || {})) {
    if (!(k in base)) continue;
    if (v === false && base[k] !== false) { out[k] = false; out.disabledByConfig.push(k); }
    if (v === true && base[k] === false) throw Object.assign(new Error(`configuration may not claim capability ${k}`), { code: "CAPABILITY_CLAIM_REFUSED" });
  }
  return out;
}
/** PURE. Can this adapter implement the desired order set exactly? */
function validateOrderSetAgainstCapabilities(orderSet, caps) {
  const problems = [];
  const legs = orderSet.legs || [];
  const entry = legs.find((l) => l.role === "ENTRY");
  const stop = legs.find((l) => l.role === "STOP"), target = legs.find((l) => l.role === "TARGET");
  if (entry && entry.timeInForce === "DAY" && !caps.dayLimitEntry) problems.push("DAY_LIMIT_ENTRY_UNSUPPORTED");
  if (entry && entry.timeInForce === "GTD" && !caps.gtdOrders) problems.push("GTD_UNSUPPORTED");
  if (stop && target && !caps.nativeOcoForOwnedPosition) problems.push("NATIVE_OCO_UNSUPPORTED");
  if (stop && stop.trailing && stop.trailing.enabled && target && caps.trailingStopInsideOco !== true && caps.trailingStopInsideOco !== "policy_gated") problems.push("TRAILING_INSIDE_OCO_UNSUPPORTED");
  if (legs.some((l) => l.session === "EXTENDED") && !caps.extendedHoursProtection) problems.push("EXTENDED_HOURS_UNSUPPORTED");
  if (legs.some((l) => l.quantityUnits != null && !/^(0|[1-9][0-9]*)$/.test(String(l.quantityUnits)))) problems.push("FRACTIONAL_OR_INVALID_QUANTITY");
  return { ok: problems.length === 0, problems, protectionStatusLabel: caps.protectionStatusLabel || "PROTECTED_RTH" };
}

function sha(v) { return crypto.createHash("sha256").update(typeof v === "string" ? v : JSON.stringify(v)).digest("hex"); }
function typed(code, message, extra = {}) { return Object.assign(new Error(message || code), { code, ...extra }); }
function rows(snap) { const out = []; if (snap && typeof snap.forEach === "function") snap.forEach((d) => out.push(d.data())); return out; }

/* ── PaperBrokerAdapter ────────────────────────────────────────────────── */
function createPaperAdapter({ admin = null, now = Date.now, capabilityConfig = {} } = {}) {
  const D = admin || A;
  const caps = applyCapabilityConfig(CAPABILITY_MATRIX.paper, capabilityConfig);
  const legRef = (legId) => D.col(D.COL.orderLegs).doc(legId);
  const eventId = (parts) => `pb_${sha(parts.join("|")).slice(0, 32)}`;
  async function recordEvent(kind, fields) {
    const id = eventId([kind, fields.orderSetId || "", fields.legId || "", fields.idempotencyKey || "", String(fields.atMs || now())]);
    await D.col(D.COL.brokerEvents).doc(id).set({ brokerEventId: id, provider: "paper", kind, dedupeKey: fields.idempotencyKey || id, atMs: now(), ...fields });
    return id;
  }
  return {
    interfaceVersion: INTERFACE_VERSION, adapter: "paper",
    async getCapabilities() { return caps; },
    async getAccountSnapshot(accountId) {
      const s = await D.col(D.COL.accounts).doc(accountId).get();
      const a = s.exists ? s.data() : {};
      const cents = a.balanceCents || {};
      return { provider: "paper", accountId, cashMinor: String(Math.round(Number(cents.cash) || 0)), buyingPowerMinor: String(Math.round(Number(cents.cash) || 0)),
        reservedMinor: String(Math.round(Number(cents.reserved) || 0)), observedAtMs: now(), truthAgeSeconds: 0, currency: "USD" };
    },
    async getSessionCalendar(calendarId, range) {
      const MK = require("./_investorMarket");
      const out = [];
      const from = new Date(range.from), to = new Date(range.to);
      for (let d = new Date(from); d <= to; d.setUTCDate(d.getUTCDate() + 1)) { const st = MK.sessionState(new Date(d.toISOString().slice(0, 10) + "T15:00:00Z")); if (st.tradingDay) out.push({ date: st.date, halfDay: st.isHalfDay, calendarVersion: st.calendarVersion }); }
      return { calendarId, sessions: out, calendarVersion: MK.CALENDAR_VERSION };
    },
    /** Legs become WORKING (entry) or ARMED (protection waiting on the entry fill). Idempotent by key. */
    async submitOrderSet(desired, idempotencyKey) {
      const existing = rows(await D.col(D.COL.brokerEvents).where("dedupeKey", "==", idempotencyKey).get());
      if (existing.length) return { accepted: true, duplicate: true, brokerGroupId: existing[0].brokerGroupId, legs: existing[0].legs || [] };
      const brokerGroupId = `pg_${sha(idempotencyKey).slice(0, 24)}`;
      const acks = [];
      for (const leg of desired.legs || []) {
        const activatesLater = leg.activatesOn === "ENTRY_FILL" || leg.activatesOn === "TIME_EXIT" || leg.remainderOf;
        const status = activatesLater ? "ARMED" : "WORKING";
        await legRef(leg.legId).set({ brokerOrderId: `paper_${leg.legId}`, brokerGroupId, status, acknowledgedAtMs: now(), workingSinceMs: status === "WORKING" ? now() : null }, { merge: true });
        acks.push({ legId: leg.legId, brokerOrderId: `paper_${leg.legId}`, status });
      }
      await recordEvent("SUBMIT_ACK", { orderSetId: desired.orderSetId, idempotencyKey, brokerGroupId, legs: acks, atMs: now() });
      return { accepted: true, duplicate: false, brokerGroupId, legs: acks, acknowledgedAtMs: now() };
    },
    async replaceOrderSet(observed, desired, idempotencyKey) {
      if (!caps.atomicGroupReplacement) return { accepted: false, reason: "ATOMIC_REPLACE_UNSUPPORTED" };
      for (const leg of (observed && observed.legs) || []) await legRef(leg.legId).set({ status: "REPLACED", replacedAtMs: now() }, { merge: true });
      const out = await this.submitOrderSet(desired, idempotencyKey);
      await recordEvent("REPLACE_ACK", { orderSetId: desired.orderSetId, idempotencyKey, replaced: (observed && observed.orderSetId) || null, atMs: now() });
      return { ...out, replaced: (observed && observed.orderSetId) || null, atomic: true };
    },
    async cancelOrderSet(orderSetId, reason, { legIds = null } = {}) {
      const legs = rows(await D.col(D.COL.orderLegs).where("orderSetId", "==", orderSetId).get());
      const cancelled = [];
      for (const leg of legs) {
        if (legIds && !legIds.includes(leg.legId)) continue;
        if (["FILLED", "CANCELLED", "EXPIRED", "REPLACED"].includes(leg.status)) continue;
        await legRef(leg.legId).set({ status: "CANCELLED", cancelledAtMs: now(), cancelReason: String(reason || "").slice(0, 80) }, { merge: true });
        cancelled.push(leg.legId);
      }
      await recordEvent("CANCEL_ACK", { orderSetId, idempotencyKey: `cancel:${orderSetId}:${reason}:${legIds ? legIds.join(",") : "all"}`, cancelled, atMs: now() });
      return { accepted: true, terminal: true, cancelled };
    },
    async getOrderSet(orderSetId) {
      const legs = rows(await D.col(D.COL.orderLegs).where("orderSetId", "==", orderSetId).get());
      return { orderSetId, legs: legs.map((l) => ({ legId: l.legId, brokerOrderId: l.brokerOrderId || null, status: l.status, filledUnits: l.filledUnits || "0", remainingUnits: l.remainingUnits, role: l.role })), observedAtMs: now() };
    },
    async listOpenOrders(accountId) {
      const legs = rows(await D.col(D.COL.orderLegs).where("accountId", "==", accountId).get());
      return legs.filter((l) => ["WORKING", "ARMED", "PARTIALLY_FILLED"].includes(l.status)).map((l) => ({ legId: l.legId, orderSetId: l.orderSetId, symbol: l.symbol, role: l.role, status: l.status, remainingUnits: l.remainingUnits }));
    },
    async listPositions(accountId) {
      const P = require("./_investorPortfolio");
      return (await P.readPositions(accountId, { admin: D })).map((p) => ({ symbol: p.symbol, quantityUnits: String(Math.floor(Number(p.qty) || 0)), source: "paper" }));
    },
    async listFills(accountId, sinceCursor = null) {
      const fills = rows(await D.col(D.COL.fills).where("accountId", "==", accountId).get()).filter((f) => f.engineVersion === "manager");
      const after = sinceCursor ? fills.filter((f) => Number(f.eventAtMs) > Number(sinceCursor)) : fills;
      after.sort((a, b) => Number(a.eventAtMs) - Number(b.eventAtMs));
      return { fills: after, cursor: after.length ? String(after[after.length - 1].eventAtMs) : sinceCursor };
    },
    async reconcile({ accountId, desiredState, observedState }) {
      const open = await this.listOpenOrders(accountId);
      const observedIds = new Set(open.map((o) => o.legId));
      const missing = (desiredState && desiredState.workingLegIds || []).filter((id) => !observedIds.has(id));
      return { ok: missing.length === 0, missing, observedOpen: open.length, observedAtMs: now(), truthAgeSeconds: 0 };
    },
  };
}

/* ── AlpacaBrokerAdapter (feature-flagged; polling based) ──────────────── */
function alpacaEnabled({ control = {}, env = process.env } = {}) {
  return String(env.INVESTOR_LIVE_BROKER || "").toLowerCase() === "alpaca" && control.accountMode === "LIMITED_LIVE";
}
function createAlpacaAdapter({ credentials = null, env = process.env, control = {}, fetchImpl = null, baseUrl = null, now = Date.now, capabilityConfig = {} } = {}) {
  const caps = applyCapabilityConfig(CAPABILITY_MATRIX.alpaca, capabilityConfig);
  const enabled = alpacaEnabled({ control, env });
  const url = baseUrl || env.ALPACA_TRADING_BASE_URL || "https://paper-api.alpaca.markets";
  const creds = credentials || (() => { try { return require("./_investorMarket").providerCredentials("alpaca"); } catch { return { keyId: "", secretKey: "" }; } })();
  const transport = fetchImpl || ((...a) => globalThis.fetch(...a));
  function assertEnabled() { if (!enabled) throw typed("LIVE_ADAPTER_DISABLED", "the Alpaca adapter is bound only under LIMITED_LIVE with INVESTOR_LIVE_BROKER=alpaca"); if (!creds.keyId || !creds.secretKey) throw typed("LIVE_CREDENTIALS_MISSING", "Alpaca trading credentials are not configured"); }
  async function call(method, path, body = null) {
    assertEnabled();
    const res = await transport(`${url}${path}`, { method, headers: { "APCA-API-KEY-ID": creds.keyId, "APCA-API-SECRET-KEY": creds.secretKey, "Content-Type": "application/json" }, body: body ? JSON.stringify(body) : undefined });
    let data = null; try { data = await res.json(); } catch {}
    if (!res.ok) throw typed(`ALPACA_HTTP_${res.status}`, String(data && data.message || res.status).slice(0, 200), { status: res.status });
    return data;
  }
  /* exact, on-tick, lossless: an off-tick price is refused rather than rounded (§7.1) */
  const toDecimal = (micros) => require("./_investorMoney").toBrokerDecimalPrice(micros, { decimals: 2, tickMicros: "10000" });
  return {
    interfaceVersion: INTERFACE_VERSION, adapter: "alpaca", enabled,
    async getCapabilities() { return { ...caps, enabled }; },
    async getAccountSnapshot(accountId) { const a = await call("GET", "/v2/account"); return { provider: "alpaca", accountId, cashMinor: String(Math.round(Number(a.cash) * 100)), buyingPowerMinor: String(Math.round(Number(a.buying_power) * 100)), observedAtMs: now(), truthAgeSeconds: 0, currency: a.currency || "USD", raw: { status: a.status, tradingBlocked: a.trading_blocked } }; },
    async getSessionCalendar(calendarId, range) { const days = await call("GET", `/v2/calendar?start=${range.from}&end=${range.to}`); return { calendarId, sessions: (days || []).map((d) => ({ date: d.date, open: d.open, close: d.close })), source: "alpaca" }; },
    /** One order per leg group: entry as a DAY limit; protection as an OCO (sell limit + sell stop) after the entry fills. */
    async submitOrderSet(desired, idempotencyKey) {
      const v = validateOrderSetAgainstCapabilities(desired, caps);
      if (!v.ok) throw typed("ORDER_SET_UNSUPPORTED", v.problems.join(","), { problems: v.problems });
      const acks = [];
      const entry = (desired.legs || []).find((l) => l.role === "ENTRY");
      if (entry) {
        const o = await call("POST", "/v2/orders", { symbol: desired.symbol, qty: String(entry.quantityUnits), side: "buy", type: "limit", time_in_force: "day", limit_price: toDecimal(entry.priceMicros), extended_hours: false, client_order_id: `${idempotencyKey}:${entry.legId}`.slice(0, 48) });
        acks.push({ legId: entry.legId, brokerOrderId: o.id, status: "WORKING" });
      }
      const exits = (desired.legs || []).filter((l) => (l.role === "REDUCE" || l.role === "SELL") && !l.activatesOn);
      for (const x of exits) {
        const o = await call("POST", "/v2/orders", { symbol: desired.symbol, qty: String(x.quantityUnits), side: "sell", type: "limit", time_in_force: "day", limit_price: toDecimal(x.priceMicros || x.collarPriceMicros), extended_hours: false, client_order_id: `${idempotencyKey}:${x.legId}`.slice(0, 48) });
        acks.push({ legId: x.legId, brokerOrderId: o.id, status: "WORKING" });
      }
      return { accepted: true, duplicate: false, brokerGroupId: idempotencyKey, legs: acks, acknowledgedAtMs: now(), protectionDeferredUntil: "ENTRY_FILL" };
    },
    /** Protection for an owned quantity: one OCO order class with a limit (target) and a stop (loss boundary). */
    async submitProtection({ symbol, quantityUnits, takeProfitPriceMicros, lossBoundaryPriceMicros, idempotencyKey }) {
      const o = await call("POST", "/v2/orders", { symbol, qty: String(quantityUnits), side: "sell", type: "limit", time_in_force: "gtc", order_class: "oco",
        take_profit: { limit_price: toDecimal(takeProfitPriceMicros) }, stop_loss: { stop_price: toDecimal(lossBoundaryPriceMicros) }, client_order_id: `${idempotencyKey}:oco`.slice(0, 48) });
      return { accepted: true, brokerGroupId: o.id, legs: (o.legs || []).map((l) => ({ brokerOrderId: l.id, role: l.type === "stop" ? "STOP" : "TARGET", status: "WORKING" })), acknowledgedAtMs: now() };
    },
    async replaceOrderSet() { return { accepted: false, reason: "ATOMIC_REPLACE_UNSUPPORTED", consequence: "keep prior protection or set ACTION_REQUIRED" }; },
    async cancelOrderSet(orderSetId, reason, { brokerOrderIds = [] } = {}) { for (const id of brokerOrderIds) await call("DELETE", `/v2/orders/${id}`); return { accepted: true, terminal: false, cancelled: brokerOrderIds, note: "confirm terminal state by polling before treating the remainder as cancelled" }; },
    async getOrderSet(orderSetId, { brokerOrderIds = [] } = {}) { const legs = []; for (const id of brokerOrderIds) { const o = await call("GET", `/v2/orders/${id}`); legs.push({ brokerOrderId: id, status: String(o.status).toUpperCase(), filledUnits: String(o.filled_qty || "0"), remainingUnits: String(Number(o.qty) - Number(o.filled_qty || 0)), filledAvgPrice: o.filled_avg_price || null }); } return { orderSetId, legs, observedAtMs: now() }; },
    async listOpenOrders() { const o = await call("GET", "/v2/orders?status=open&limit=500"); return (o || []).map((x) => ({ brokerOrderId: x.id, symbol: x.symbol, side: x.side, type: x.type, status: String(x.status).toUpperCase(), remainingUnits: String(Number(x.qty) - Number(x.filled_qty || 0)), clientOrderId: x.client_order_id })); },
    async listPositions() { const p = await call("GET", "/v2/positions"); return (p || []).map((x) => ({ symbol: x.symbol, quantityUnits: String(Math.trunc(Number(x.qty))), markMicros: String(Math.round(Number(x.current_price) * 1e6)), source: "alpaca" })); },
    async listFills(accountId, sinceCursor = null) { const q = sinceCursor ? `&after=${encodeURIComponent(new Date(Number(sinceCursor)).toISOString())}` : ""; const acts = await call("GET", `/v2/account/activities/FILL?direction=asc&page_size=100${q}`); const fills = (acts || []).map((a) => ({ fillId: a.id, brokerOrderId: a.order_id, symbol: a.symbol, side: a.side, quantityUnits: String(Math.trunc(Number(a.qty))), priceMicros: String(Math.round(Number(a.price) * 1e6)), eventAtMs: Date.parse(a.transaction_time), source: "alpaca" })); return { fills, cursor: fills.length ? String(fills[fills.length - 1].eventAtMs) : sinceCursor }; },
    async reconcile({ accountId, desiredState }) { const open = await this.listOpenOrders(accountId); const ids = new Set(open.map((o) => o.brokerOrderId)); const missing = (desiredState && desiredState.brokerOrderIds || []).filter((id) => !ids.has(id)); return { ok: missing.length === 0, missing, observedOpen: open.length, observedAtMs: now(), truthAgeSeconds: 0 }; },
  };
}

/** Pick the bound adapter for the account mode. Paper unless live is enabled. */
function adapterFor({ control = {}, env = process.env, admin = null, now = Date.now } = {}) {
  if (alpacaEnabled({ control, env })) return createAlpacaAdapter({ env, control, now });
  return createPaperAdapter({ admin, now, capabilityConfig: (control.brokerCapabilityConfig) || {} });
}
/** PURE. Startup test: every adapter implements the interface and its matrix is sane. */
function selfCheck() {
  const failures = [];
  for (const [name, caps] of Object.entries(CAPABILITY_MATRIX)) {
    if (!caps.regularSessionOnly || !caps.wholeSharesOnly || caps.fractionalShares) failures.push(`${name}: instrument rules`);
    if (caps.gtdOrders) failures.push(`${name}: GTD must stay off until proven`);
  }
  try { applyCapabilityConfig(CAPABILITY_MATRIX.alpaca, { atomicGroupReplacement: true }); failures.push("config claimed an unsupported capability"); } catch (e) { if (e.code !== "CAPABILITY_CLAIM_REFUSED") failures.push(`claim refusal code ${e.code}`); }
  const paper = createPaperAdapter({ admin: { col: () => ({ doc: () => ({}), where: () => ({}) }), COL: {} } });
  for (const m of METHODS) if (typeof paper[m] !== "function") failures.push(`paper lacks ${m}`);
  const live = createAlpacaAdapter({ env: {}, control: {} });
  for (const m of METHODS) if (typeof live[m] !== "function") failures.push(`alpaca lacks ${m}`);
  if (live.enabled !== false) failures.push("alpaca enabled without the flag");
  const v = validateOrderSetAgainstCapabilities({ legs: [{ role: "ENTRY", timeInForce: "DAY", quantityUnits: "5" }, { role: "TARGET", quantityUnits: "5" }, { role: "STOP", quantityUnits: "5", trailing: { enabled: true } }] }, CAPABILITY_MATRIX.alpaca);
  if (v.ok || !v.problems.includes("TRAILING_INSIDE_OCO_UNSUPPORTED")) failures.push("trailing inside OCO accepted for alpaca");
  return { pass: failures.length === 0, failures };
}

module.exports = { INTERFACE_VERSION, METHODS, CAPABILITY_MATRIX, applyCapabilityConfig, validateOrderSetAgainstCapabilities, createPaperAdapter, createAlpacaAdapter, alpacaEnabled, adapterFor, selfCheck };
