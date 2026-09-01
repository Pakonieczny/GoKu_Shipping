/* Investor_AI — immutable, policy-aware decision-input manifests. */
"use strict";

const crypto = require("crypto");

const VERSION = "decision-input-manifest-v1";
const EFFECTS = Object.freeze(["blocked", "reduced_size", "no_effect", "informational"]);
const POLICY_IDENTITY_KEYS = Object.freeze([
  "accountId", "strategyVersion", "universeVersion",
  "strategyHash", "universeHash", "variantsHash", "variantId",
]);

function stable(value) {
  if (Array.isArray(value)) return value.map(stable);
  if (value && typeof value === "object") return Object.fromEntries(Object.keys(value)
    .sort().filter((k) => value[k] !== undefined).map((k) => [k, stable(value[k])]));
  if (typeof value === "number" && !Number.isFinite(value)) return null;
  return value;
}
function sha256(value) {
  return crypto.createHash("sha256").update(JSON.stringify(stable(value))).digest("hex");
}
function present(value) {
  return value !== null && value !== undefined && value !== ""
    && (!Array.isArray(value) || value.length > 0);
}
function inputStatus({ value, valid = true, stale = false }) {
  if (!present(value)) return "missing";
  if (!valid) return "invalid";
  return stale ? "stale" : "fresh";
}
function gate(result, id) {
  return result && Array.isArray(result.gates)
    ? result.gates.find((g) => g.id === id) || null : null;
}
function effect(result, gateId, sizeKey = null, informational = false) {
  const g = gateId ? gate(result, gateId) : null;
  if (g && g.blocking !== false && g.pass !== true) {
    return { kind: "blocked", detail: g.detail || g.label || gateId };
  }
  if (sizeKey && result && result.sizing) {
    const value = Number(result.sizing[sizeKey]);
    if (Number.isFinite(value) && value < 1) {
      return { kind: "reduced_size", multiplier: Math.max(0, value),
        detail: `${sizeKey} reduced the position-size ceiling` };
    }
  }
  return { kind: informational ? "informational" : "no_effect",
    detail: g ? g.detail || g.label || gateId : "considered; no binding effect" };
}
function descriptor({ id, label, value, asOfMs, source, sourceHash = null,
  valid = true, stale = false, activeEffect, strictEffect }) {
  const status = inputStatus({ value, valid, stale });
  const normalizedAsOf = Number.isFinite(Number(asOfMs)) ? Number(asOfMs) : null;
  const provenanceHash = sha256({ id, status, value, asOfMs: normalizedAsOf,
    source: source || "unavailable", sourceHash: sourceHash || null });
  return { id, label, value: stable(value), asOfMs: normalizedAsOf,
    source: source || "unavailable", sourceHash: sourceHash || null,
    provenanceHash, status,
    activePolicyEffect: activeEffect,
    strictPolicyEffect: strictEffect };
}

function build({ decisionId, decisionAtMs, symbol, input, rank, zStat,
  activeResult, strictResult, marketProvenance = null, lastBarAt = null,
  regime = {}, earningsWindow = null, meta = {}, betas = null,
  dailyProvenance = null, policyIdentity = {}, config = {}, strictConfig = {},
  buildCommit = "local", operatingState = "observation" }) {
  const marketHash = marketProvenance && marketProvenance.sourceSha256 || null;
  const marketSource = marketProvenance
    ? `${marketProvenance.provider || "unknown"}/${marketProvenance.feed || "unknown"}`
    : "market feed unavailable";
  const barAtMs = Number.isFinite(Date.parse(lastBarAt)) ? Date.parse(lastBarAt) : null;
  const intel = input.intelligence || null;
  const intelPolicy = activeResult && activeResult.intelligencePolicy || null;
  const history = input.historyContext || null;
  const active = (gateId, sizeKey, info) => effect(activeResult, gateId, sizeKey, info);
  const strict = (gateId, sizeKey, info) => effect(strictResult, gateId, sizeKey, info);
  const rows = [
    descriptor({ id: "market_price_quality", label: "Market price and quality",
      value: { quality: input.quality || null,
        lastPrice: zStat && zStat.lastPrice != null ? zStat.lastPrice : null },
      asOfMs: barAtMs, source: marketSource, sourceHash: marketHash,
      valid: !!input.quality, stale: !!(input.quality && input.quality.stale),
      activeEffect: active("quality"), strictEffect: strict("quality") }),
    descriptor({ id: "residual_signal", label: "Residual rank and z-statistic",
      value: { rank: rank ?? null, z: (zStat && zStat.z) ?? null,
        cumulativeResidual: (zStat && zStat.cumResidual) ?? null,
        window: (zStat && zStat.window) ?? config.signalWindow ?? null },
      asOfMs: barAtMs, source: "derived from provenance-bound market panel",
      sourceHash: marketHash, valid: Number.isFinite(Number(rank)) && !!zStat,
      activeEffect: active("signal"), strictEffect: strict("signal") }),
    descriptor({ id: "factor_model", label: "Market and sector factor model",
      value: { betas: betas || null, dailyContext: dailyProvenance || null },
      asOfMs: barAtMs, source: "joint OLS residual panel", sourceHash: sha256({ betas, dailyProvenance }),
      valid: !!betas, activeEffect: active(null, null, true),
      strictEffect: strict(null, null, true) }),
    descriptor({ id: "session", label: "Exchange session state",
      value: input.session || null, asOfMs: decisionAtMs, source: "NYSE calendar and New York clock",
      sourceHash: sha256(input.session || null), valid: !!(input.session && input.session.date),
      activeEffect: active("session"), strictEffect: strict("session") }),
    descriptor({ id: "dispersion", label: "Cross-asset dispersion regime",
      value: regime.rawCor3m ?? input.cor3m ?? null,
      asOfMs: Date.parse(regime.corFetchedAt || regime.asOf || ""),
      source: regime.corSource || "regime cache", sourceHash: regime.corSourceSha256 || null,
      valid: input.cor3m == null || Number.isFinite(Number(input.cor3m)), stale: !regime.corHealthy,
      activeEffect: active("dispersion", "dispersionMult"),
      strictEffect: strict("dispersion", "dispersionMult") }),
    descriptor({ id: "volatility", label: "Volatility regime",
      value: { rawVix: regime.rawVix ?? regime.vix ?? null,
        median: regime.vixMedian ?? null, normalized: input.vixNorm ?? null },
      asOfMs: Date.parse(regime.vixFetchedAt || regime.asOf || ""),
      source: regime.vixSource || "regime cache", sourceHash: regime.vixSourceSha256 || null,
      valid: input.vixNorm == null || Number.isFinite(Number(input.vixNorm)), stale: !regime.vixHealthy,
      activeEffect: active(null, "volScaler"), strictEffect: strict(null, "volScaler") }),
    descriptor({ id: "earnings", label: "Earnings risk window",
      value: earningsWindow || { dates: input.earningsDates || [],
        estimated: input.earningsEstimated, uncertaintyDays: input.earningsUncertaintyDays },
      asOfMs: Date.parse(earningsWindow && (earningsWindow.derivedAt
        || earningsWindow.asOf || earningsWindow.updatedAt) || ""),
      source: earningsWindow && earningsWindow.source || "EDGAR filing cadence",
      sourceHash: earningsWindow && (earningsWindow.sourceSha256 || earningsWindow.contentHash) || null,
      valid: Array.isArray(input.earningsDates),
      activeEffect: active("blackout"), strictEffect: strict("blackout") }),
    descriptor({ id: "liquidity", label: "Effective dollar volume",
      value: { advUsd: input.advUsd ?? null, source: meta.advSource || null,
        provisional: meta.advProvisional === true, components: meta.advComponents || null },
      asOfMs: barAtMs, source: meta.advSource || "derived market volume",
      sourceHash: marketHash, valid: Number.isFinite(Number(input.advUsd)) && Number(input.advUsd) >= 0,
      activeEffect: active("liquidity"), strictEffect: strict("liquidity") }),
    descriptor({ id: "sector_crowding", label: "Sector tail crowding",
      value: input.sectorTailFraction ?? null, asOfMs: barAtMs,
      source: "cross-sectional residual ranks", sourceHash: marketHash,
      valid: input.sectorTailFraction == null
        || (Number(input.sectorTailFraction) >= 0 && Number(input.sectorTailFraction) <= 1),
      activeEffect: active("crowding"), strictEffect: strict("crowding") }),
    descriptor({ id: "long_horizon_history", label: "Long-horizon price context",
      value: history, asOfMs: Date.parse(history && (history.asOf || history.lastDate) || ""),
      source: "point-in-time daily history", sourceHash: sha256({ history, dailyProvenance }),
      valid: !history || history.ok !== false,
      activeEffect: active("trend"), strictEffect: strict("trend") }),
    descriptor({ id: "reversion_history", label: "Shrunk historical reversion",
      value: input.reversion || null, asOfMs: decisionAtMs,
      source: "pre-decision historical events", sourceHash: sha256(input.reversion || null),
      activeEffect: active("cost"), strictEffect: strict("cost") }),
    descriptor({ id: "turnover", label: "Cross-sectional share turnover",
      value: input.turnoverPctile ?? null, asOfMs: barAtMs,
      source: "effective ADV divided by shares outstanding", sourceHash: sha256({
        turnoverPctile: input.turnoverPctile, advSource: meta.advSource,
      }), valid: input.turnoverPctile == null
        || (Number(input.turnoverPctile) >= 0 && Number(input.turnoverPctile) <= 1),
      activeEffect: active("turnover"), strictEffect: strict("turnover") }),
    descriptor({ id: "evidence", label: "Cause and source coverage",
      value: { cause: input.cause || null, attentionScore: input.attentionScore ?? null,
        coverage: input.coverage || null }, asOfMs: decisionAtMs,
      source: "point-in-time evidence classifier", sourceHash: sha256({
        cause: input.cause, attentionScore: input.attentionScore, coverage: input.coverage,
      }), activeEffect: active("evidence", "causeConfidence"),
      strictEffect: strict("evidence", "causeConfidence") }),
    descriptor({ id: "company_intelligence", label: "Company intelligence dossier",
      value: intel ? { dossierHash: intel.dossierHash || null, coverage: intel.coverage || null,
        events: (intel.events || []).map((e) => e.eventId || e.id).filter(Boolean),
        policy: intelPolicy } : null,
      asOfMs: intel && intel.asOfMs, source: "provenance-bound company dossier",
      sourceHash: intel && intel.dossierHash || null,
      stale: !!(intel && intelPolicy && intelPolicy.fresh !== true),
      activeEffect: active("intelligence", "intelligenceMult"),
      strictEffect: strict("intelligence", "intelligenceMult") }),
    descriptor({ id: "temporal_context", label: "Dated and recurring exposure context",
      value: intel && intel.temporalContext || null,
      asOfMs: intel && intel.temporalContext && intel.temporalContext.asOfMs,
      source: "point-in-time temporal context",
      sourceHash: intel && intel.temporalContext && intel.temporalContext.contextHash || null,
      stale: !!(intelPolicy && intelPolicy.temporalPolicy
        && intelPolicy.temporalPolicy.fresh === false),
      activeEffect: active("intelligence", "intelligenceMult"),
      strictEffect: strict("intelligence", "intelligenceMult") }),
    descriptor({ id: "execution_cost", label: "Modeled implementation cost",
      value: activeResult && activeResult.cost || null, asOfMs: decisionAtMs,
      source: "feed/ADV/session slippage model", sourceHash: sha256({
        quality: input.quality, advUsd: input.advUsd, session: input.session,
      }), activeEffect: active("cost"), strictEffect: strict("cost") }),
  ];
  const counts = rows.reduce((acc, row) => {
    acc[row.status] = (acc[row.status] || 0) + 1; return acc;
  }, { fresh: 0, stale: 0, missing: 0, invalid: 0 });
  const manifest = {
    version: VERSION, decisionId, decisionAtMs: Number(decisionAtMs), symbol,
    operatingState,
    policyIdentity: stable(policyIdentity),
    /* Values live once in the immutable strategy/variant documents. Repeating
       the full config on hundreds of decisions every five minutes would make
       traceability itself operationally wasteful; hashes + frozen identity are
       sufficient to reproduce the exact values. */
    configuration: { activeHash: sha256(config), strictHash: sha256(strictConfig),
      activeKeys: Object.keys(config || {}).sort(),
      strictKeys: Object.keys(strictConfig || {}).sort() },
    code: { buildCommit, engine: "_investorSignal.evaluateCandidate",
      manifestBuilder: VERSION },
    inputs: rows,
    coverage: { configuredInputs: rows.length, ...counts,
      withProvenanceIdentity: rows.filter((r) => /^[a-f0-9]{64}$/.test(r.provenanceHash)).length,
      completeIdentity: POLICY_IDENTITY_KEYS.every((key) => !!policyIdentity[key])
        && rows.every((r) => /^[a-f0-9]{64}$/.test(r.provenanceHash)) },
    activeVerdict: { pass: activeResult && activeResult.pass === true,
      blockedBy: activeResult && activeResult.blockedBy || [] },
    strictVerdict: { pass: strictResult && strictResult.pass === true,
      blockedBy: strictResult && strictResult.blockedBy || [] },
  };
  manifest.manifestHash = sha256(manifest);
  return manifest;
}

function buildPortfolio({ decisionId, decisionAtMs, symbol, parentManifestHash,
  sizing, portfolioCheck, book, navUsd, cashUsd, correlations, policyIdentity,
  config, buildCommit = "local" }) {
  const pass = !!(portfolioCheck && (portfolioCheck.allow || portfolioCheck.allowTrimmed));
  const portfolioEffect = portfolioCheck && portfolioCheck.allowTrimmed
    ? { kind: "reduced_size", detail: portfolioCheck.firstBlock || "trimmed to portfolio capacity" }
    : pass ? { kind: "no_effect", detail: "portfolio admitted the requested paper risk" }
      : { kind: "blocked", detail: portfolioCheck && portfolioCheck.firstBlock
        || "portfolio constraints blocked the order" };
  const same = (x) => ({ ...x });
  const rows = [
    descriptor({ id: "risk_sizing", label: "Tail-risk position sizing", value: sizing || null,
      asOfMs: decisionAtMs, source: "fixed risk-budget model",
      sourceHash: sha256({ sizing, configHash: sha256(config || {}) }),
      activeEffect: same(portfolioEffect), strictEffect: same(portfolioEffect) }),
    descriptor({ id: "available_cash", label: "Available paper cash",
      value: { cashUsd, navUsd }, asOfMs: decisionAtMs, source: "reconciled paper ledger",
      sourceHash: sha256({ cashUsd, navUsd, parentManifestHash }),
      valid: Number(cashUsd) >= 0 && Number(navUsd) > 0,
      activeEffect: same(portfolioEffect), strictEffect: same(portfolioEffect) }),
    descriptor({ id: "portfolio_exposure", label: "Existing portfolio exposures",
      value: book ? { count: book.count, grossPct: book.grossPct,
        bySectorPct: book.bySectorPct, byClusterPct: book.byClusterPct } : null,
      asOfMs: decisionAtMs, source: "marked paper book",
      sourceHash: sha256({ book, parentManifestHash }),
      activeEffect: same(portfolioEffect), strictEffect: same(portfolioEffect) }),
    descriptor({ id: "dynamic_correlation", label: "Dynamic position correlations",
      value: correlations || null, asOfMs: decisionAtMs,
      source: "point-in-time provenance-matched daily returns",
      sourceHash: sha256(correlations || null),
      activeEffect: same(portfolioEffect), strictEffect: same(portfolioEffect) }),
    descriptor({ id: "portfolio_constraints", label: "Portfolio constraint verdict",
      value: portfolioCheck || null, asOfMs: decisionAtMs,
      source: "frozen portfolio-control policy",
      sourceHash: sha256({ portfolioCheck, configHash: sha256(config || {}) }),
      activeEffect: same(portfolioEffect), strictEffect: same(portfolioEffect) }),
  ];
  const counts = rows.reduce((acc, row) => {
    acc[row.status] = (acc[row.status] || 0) + 1; return acc;
  }, { fresh: 0, stale: 0, missing: 0, invalid: 0 });
  const manifest = { version: VERSION, subtype: "portfolio", decisionId,
    decisionAtMs: Number(decisionAtMs), symbol, parentManifestHash,
    policyIdentity: stable(policyIdentity),
    configuration: { portfolioHash: sha256(config || {}),
      keys: Object.keys(config || {}).sort() },
    code: { buildCommit, engine: "_investorRisk.checkAdd", manifestBuilder: VERSION },
    inputs: rows, coverage: { configuredInputs: rows.length, ...counts,
      withProvenanceIdentity: rows.filter((r) =>
        /^[a-f0-9]{64}$/.test(String(r.provenanceHash || ""))).length,
      completeIdentity: /^[a-f0-9]{64}$/.test(String(parentManifestHash || ""))
        && POLICY_IDENTITY_KEYS.every((key) => !!policyIdentity[key]) },
    activeVerdict: { pass, blockedBy: pass ? []
      : (portfolioCheck && portfolioCheck.blockedBy || ["portfolio_constraints"]) },
    strictVerdict: { pass, blockedBy: pass ? []
      : (portfolioCheck && portfolioCheck.blockedBy || ["portfolio_constraints"]) },
  };
  manifest.manifestHash = sha256(manifest);
  return manifest;
}

function validate(manifest) {
  if (!manifest || manifest.version !== VERSION) return { pass: false, reason: "wrong_version" };
  if (!Array.isArray(manifest.inputs) || !manifest.inputs.length) return { pass: false, reason: "inputs_missing" };
  if (manifest.inputs.some((x) => !["fresh", "stale", "missing", "invalid"].includes(x.status)
      || !EFFECTS.includes(x.activePolicyEffect && x.activePolicyEffect.kind)
      || !EFFECTS.includes(x.strictPolicyEffect && x.strictPolicyEffect.kind)
      || !/^[a-f0-9]{64}$/.test(String(x.provenanceHash || "")))) {
    return { pass: false, reason: "invalid_input_record" };
  }
  if (!manifest.coverage || manifest.coverage.configuredInputs !== manifest.inputs.length
      || manifest.coverage.withProvenanceIdentity !== manifest.inputs.length
      || manifest.coverage.completeIdentity !== true) {
    return { pass: false, reason: "incomplete_identity" };
  }
  const expected = sha256(Object.fromEntries(Object.entries(manifest)
    .filter(([k]) => k !== "manifestHash")));
  return { pass: expected === manifest.manifestHash,
    reason: expected === manifest.manifestHash ? null : "manifest_hash_mismatch" };
}

module.exports = { VERSION, EFFECTS, POLICY_IDENTITY_KEYS, stable, sha256, inputStatus,
  build, buildPortfolio, validate };
