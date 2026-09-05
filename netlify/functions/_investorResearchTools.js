/*  netlify/functions/_investorResearchTools.js  (fund-manager-v1)
 *  ---------------------------------------------------------------------------
 *  Investor AI — the read-only, source-bound research tools (blueprint §6.5,
 *  TOOL_POLICY in _investorPolicy).
 *
 *  Seven tools, one shape. Each has a strict argument schema, a server-side
 *  dispatch that scopes every call to the job's symbol and cutoff, bounded
 *  argument/result bytes, and a hashed audit line. There is no browser and
 *  no order tool; a missing entitled source is returned as missing, never
 *  filled from memory. The tools are DATA for the model: they never contain
 *  a score, a rank or a recommendation.
 *
 *  allowlisted({ evidence, filings, decisionData, market, valuation,
 *  portfolio, portfolioRisk }, toolPolicy) binds the implementations the
 *  caller supplies; the gateway enforces the allowlist and the bounds again
 *  at call time, so a tool that is not in TOOL_POLICY.allowlist can never be
 *  offered even by a bug here.
 * ---------------------------------------------------------------------------
 */

"use strict";

const crypto = require("crypto");
const POLICY = require("./_investorPolicy");

const TOOLS_VERSION = "research-tools.v1";
const S = (props, required = Object.keys(props)) => ({ type: "object", additionalProperties: false, required, properties: props });
const STR = { type: "string" }, NUM = { type: "number" }, NULLSTR = { type: ["string", "null"] };

/** Strict argument schemas (every property required; nullable where optional). */
const TOOL_SCHEMAS = Object.freeze({
  getFilingFactsAsOf: { description: "Point-in-time SEC XBRL facts for the company as of the cutoff: normalized fact versions with filing/accession lineage. concepts: us-gaap/dei concept names, or [] for the default set.",
    parameters: S({ symbol: STR, asOfMs: NUM, concepts: { type: "array", items: STR } }) },
  getSourceSpans: { description: "Exact stored document spans (canonical text) for the given document version ids, with publication and first-seen metadata.",
    parameters: S({ symbol: STR, asOfMs: NUM, documentVersionIds: { type: "array", items: STR } }) },
  searchDecisionData: { description: "Entitled point-in-time decision data for the company: filings, news, guidance and earnings-date claims. Consensus and transcripts are declared unavailable, never inferred.",
    parameters: S({ symbol: STR, asOfMs: NUM, kinds: { type: "array", items: STR }, limit: NUM }) },
  getMarketContextAsOf: { description: "Price, volume, spread proxy, benchmark and sector-driver context as of the cutoff. Descriptors only; no buy score.",
    parameters: S({ symbol: STR, asOfMs: NUM, lookbackDays: NUM }) },
  runValuation: { description: "Exact scenario arithmetic from YOUR declared method, assumptions and probabilities. Returns per-scenario values, expected terminal price, implied return and quantiles. Reject-on-error: fix the assumptions and call again.",
    parameters: S({ symbol: STR, asOfMs: NUM, method: STR, assumptions: { type: "array", items: S({ name: STR, value: STR, unit: STR }) },
      scenarios: { type: "array", items: S({ id: STR, probabilityPpm: STR, assumptions: { type: "array", items: S({ name: STR, value: STR, unit: STR }) }, terminalPriceMicros: NULLSTR }) },
      priceMicros: NULLSTR, sharesOutstanding: NULLSTR }) },
  getPortfolioSnapshot: { description: "Positions, working orders, settled cash, reservations, exposures and the opportunity-cost set as of now.",
    parameters: S({ symbol: STR, asOfMs: NUM }) },
  runPortfolioScenarios: { description: "Read-only exposure, liquidity, marginal-risk, cash-drag and gap/halt stress scenarios for a proposed quantity of this symbol against the current book. Measurements and feasibility booleans only.",
    parameters: S({ symbol: STR, asOfMs: NUM, proposedQuantityUnits: STR, limitPriceMicros: STR, lossBoundaryPriceMicros: STR }) },
});

function sha(v) { return crypto.createHash("sha256").update(typeof v === "string" ? v : JSON.stringify(POLICY.canonical(v))).digest("hex"); }
function bounded(value, maxBytes) {
  const text = JSON.stringify(value === undefined ? null : value);
  if (Buffer.byteLength(text, "utf8") <= maxBytes) return { value, truncated: false, bytes: Buffer.byteLength(text, "utf8") };
  return { value: { truncated: true, note: `result exceeded ${maxBytes} bytes; narrow the request`, head: text.slice(0, Math.floor(maxBytes / 2)) }, truncated: true, bytes: Buffer.byteLength(text, "utf8") };
}

/** Bind implementations into gateway-shaped tools with scope and audit. */
function allowlisted(impl = {}, toolPolicy = POLICY.TOOL_POLICY, { symbol = null, cutoffMs = null, audit = null } = {}) {
  const allow = new Set(toolPolicy.allowlist || []);
  const log = [];
  const record = (line) => { log.push(line); if (typeof audit === "function") { try { audit(line); } catch {} } };
  const wrap = (name, fn) => ({
    description: TOOL_SCHEMAS[name].description, parameters: TOOL_SCHEMAS[name].parameters,
    execute: async (args) => {
      const started = Date.now();
      const scoped = { ...args, symbol: String(symbol || args.symbol || "").toUpperCase(),
        asOfMs: cutoffMs != null ? Math.min(Number(cutoffMs), Number(args.asOfMs) || Number(cutoffMs)) : Number(args.asOfMs) };
      if (symbol && scoped.symbol !== String(symbol).toUpperCase()) throw Object.assign(new Error("symbol out of scope"), { code: "TOOL_SYMBOL_OUT_OF_SCOPE" });
      let out;
      try { out = await fn(scoped); }
      catch (e) { record({ tool: name, ok: false, error: String(e.code || e.message).slice(0, 80), argsHash: sha(scoped), elapsedMs: Date.now() - started }); return { error: String(e.code || e.message).slice(0, 120), missing: true }; }
      const b = bounded(out, toolPolicy.maxResultBytes);
      record({ tool: name, ok: true, argsHash: sha(scoped), resultHash: sha(b.value), bytes: b.bytes, truncated: b.truncated, elapsedMs: Date.now() - started });
      return b.value;
    },
  });
  const tools = {};
  const add = (name, fn) => { if (allow.has(name) && typeof fn === "function") tools[name] = wrap(name, fn); };
  add("getFilingFactsAsOf", async (a) => (impl.filings ? impl.filings({ symbol: a.symbol, asOfMs: a.asOfMs, concepts: a.concepts || [] }) : { missing: true, reason: "filings tool unavailable" }));
  add("getSourceSpans", async (a) => (impl.evidence ? impl.evidence({ symbol: a.symbol, asOfMs: a.asOfMs, documentVersionIds: (a.documentVersionIds || []).slice(0, 32) }) : { missing: true, reason: "evidence tool unavailable" }));
  add("searchDecisionData", async (a) => (impl.decisionData ? impl.decisionData({ symbol: a.symbol, asOfMs: a.asOfMs, kinds: a.kinds || [], limit: Math.min(50, Number(a.limit) || 20) }) : { missing: true, reason: "decision data tool unavailable" }));
  add("getMarketContextAsOf", async (a) => (impl.market ? impl.market({ symbol: a.symbol, asOfMs: a.asOfMs, lookbackDays: Math.min(400, Number(a.lookbackDays) || 260) }) : { missing: true, reason: "market tool unavailable" }));
  add("runValuation", async (a) => (impl.valuation ? impl.valuation({ symbol: a.symbol, asOfMs: a.asOfMs, method: a.method, assumptions: a.assumptions || [], scenarios: a.scenarios || [], priceMicros: a.priceMicros, sharesOutstanding: a.sharesOutstanding }) : { missing: true, reason: "valuation tool unavailable" }));
  add("getPortfolioSnapshot", async (a) => (impl.portfolio ? impl.portfolio({ symbol: a.symbol, asOfMs: a.asOfMs }) : { missing: true, reason: "portfolio tool unavailable" }));
  add("runPortfolioScenarios", async (a) => (impl.portfolioRisk ? impl.portfolioRisk({ symbol: a.symbol, asOfMs: a.asOfMs, proposedQuantityUnits: a.proposedQuantityUnits, limitPriceMicros: a.limitPriceMicros, lossBoundaryPriceMicros: a.lossBoundaryPriceMicros }) : { missing: true, reason: "portfolio risk tool unavailable" }));
  return { tools, log, toolsVersion: TOOLS_VERSION, offered: Object.keys(tools) };
}

/** The production bindings: each tool is the owning module's read-only function. */
function productionBindings({ accountId, admin = null, policy = POLICY.loadActiveSync({}), portfolio = null, sectorOf = null, marks = null, decisionPacket = null } = {}) {
  const lazy = (m) => { try { return require(m); } catch { return null; } };
  const F = lazy("./_investorFundamentals"), E = lazy("./_investorEvidence"), DP = lazy("./_investorDataProviders");
  const H = lazy("./_investorHistory"), D = lazy("./_investorDossier"), V = lazy("./_investorValuation");
  const P = lazy("./_investorPortfolio"), PR = lazy("./_investorPortfolioRisk"), T = lazy("./_investorTemporal"), U = lazy("./_investorUniverse");
  const rosterRow = (s) => (U ? [...(U.tradeTier || []), ...(U.researchTier || [])].find((r) => r.symbol === s) : null) || null;
  return {
    filings: F ? async ({ symbol, asOfMs, concepts }) => {
      const row = rosterRow(symbol);
      if (!row || !row.cik) return { missing: true, reason: "no CIK resolved for symbol" };
      const r = await F.getFilingFactsAsOf({ cik: row.cik, concepts, asOfMs, limit: 400 });
      const visible=r.facts.filter(f=>Number(f.retrievedAtMs)>0 && Number(f.retrievedAtMs)<=asOfMs);
      return { cik: r.cik, asOfMs: r.asOfMs, truncated: r.truncated, facts: visible.map((f) => ({ factId: f.factId, taxonomy: f.taxonomy, concept: f.concept, unit: f.unit, period: f.period, valueScaled: f.valueScaled, scale: f.scale, form: f.form, filedDate: f.filedDate, accession: f.accession })), lineage: r.lineage.filter(l=>visible.some(f=>f.factId===l.factId)).map(l=>({...l,supersededByFactId:visible.some(f=>f.factId===l.supersededByFactId)?l.supersededByFactId:null})).slice(0, 200) };
    } : null,
    evidence: E ? async ({ symbol, asOfMs, documentVersionIds }) => {
      const spans = await E.sourceSpans({ documentVersionIds });
      const out = {};
      for (const [id, v] of Object.entries(spans)) {
        if (!v) { out[id] = null; continue; }
        if (String(v.symbol || "").toUpperCase() !== symbol) { out[id] = { missing: true, reason: "document belongs to another symbol" }; continue; }
        if (Number(v.fetchedAtMs) > Number(asOfMs)) { out[id] = { missing: true, reason: "document version not yet known at the cutoff" }; continue; }
        out[id] = { versionId: v.versionId, documentId: v.documentId, sourceId: v.sourceId, sourcePublishedAt: v.sourcePublishedAt, fetchedAtMs: v.fetchedAtMs, contentHash: v.contentHash, canonicalText: String(v.canonicalText || "").slice(0, 40000) };
      }
      return { spans: out };
    } : null,
    decisionData: DP ? async ({ symbol, asOfMs, kinds, limit }) => DP.searchDecisionData({ symbol, kinds, asOfMs, limit }) : null,
    market: H && D ? async ({ symbol, asOfMs, lookbackDays }) => {
      if(decisionPacket && decisionPacket.symbol===symbol) return {symbol,asOfMs,observation:decisionPacket.marketObservation || null,marketState:decisionPacket.marketState || null,source:"FROZEN_RESEARCH_PACKET"};
      const observation = await require("./_investorDecisionContext").marketObservation(symbol,{cutoffMs:asOfMs,deps:{admin}});
      const row = rosterRow(symbol);
      const driver = (T && T.DRIVER_BY_SECTOR && row ? T.DRIVER_BY_SECTOR[row.sector] : null) || "SPY";
      const read = async (s) => { try { const d = await H.readDailyWithMeta(s); return { returns: D.returnsBps(d.series || [], { asOfMs }), provenance: d.provenance || null, bars: (d.series || []).filter((b) => b.date < new Date(asOfMs).toISOString().slice(0, 10)).slice(-Math.min(400, lookbackDays)).map((b) => ({ date: b.date, o: b.o, h: b.h, l: b.l, c: b.c, v: b.v })) }; } catch { return { returns: { ok: false, reason: "read_failed" }, provenance: null, bars: [] }; } };
      const [own, drv, mkt] = await Promise.all([read(symbol), read(driver), driver === "SPY" ? null : read("SPY")]);
      return { observation, symbol, sector: row ? row.sector : null, sectorDriver: driver, price: own.returns, provenance: own.provenance, bars: own.bars.slice(-60),
        sectorDriverReturns: drv.returns, marketReturns: (mkt || drv).returns, note: "descriptors only; no signal, score or gate" };
    } : null,
    valuation: V ? async ({ symbol, asOfMs, method, assumptions, scenarios, priceMicros, sharesOutstanding }) => {
      /* the calculator consumes the schema's {name, value, unit} arrays directly; every error is typed and returned as data */
      try { return V.run({ method, assumptions: assumptions || [], scenarios: scenarios || [], priceMicros: priceMicros || null, sharesOutstanding: sharesOutstanding || null, symbol, asOfMs }); }
      catch (e) { return { ok: false, error: String(e.code || "VALUATION_FAILED"), message: String(e.message).slice(0, 200), hint: "fix the assumptions and call again; arithmetic is never accepted from the model" }; }
    } : null,
    portfolio: P ? async ({ asOfMs }) => portfolio || P.getSnapshot({ accountId, asOfMs, admin }) : null,
    portfolioRisk: P && PR ? async ({ symbol, asOfMs, proposedQuantityUnits, limitPriceMicros, lossBoundaryPriceMicros }) => {
      const snap = portfolio || await P.snapshot({ accountId, asOfMs, admin });
      const row = rosterRow(symbol);
      const sector = sectorOf || ((s) => { const r = rosterRow(s); return r ? r.sector : null; });
      return PR.runScenarios({ portfolio: snap, candidates: [{ symbol, sector: row ? row.sector : null, proposedQuantityUnits, limitPriceMicros, lossBoundaryPriceMicros,
        costPerShareMicros: policy.riskMandate.stress.stressCostPerShareMicros, advMinor: (marks && marks[symbol] && marks[symbol].advMinor) || null, spreadBps: (marks && marks[symbol] && marks[symbol].spreadBps) || null }],
        policy, ...require("./_investorDecisionContext").riskInputs(marks || {},snap), sectorOf: sector, clusterOf: null });
    } : null,
  };
}

module.exports = { TOOLS_VERSION, TOOL_SCHEMAS, allowlisted, productionBindings, bounded };
