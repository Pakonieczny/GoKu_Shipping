"use strict";
const V = require("./_investorValuation");
const C = require("./_investorDecisionContext");
const fail = (code) => { throw Object.assign(new Error(code), { code }); };

/* Recompute from declared assumptions on the server. Model-supplied hashes or
 * calculator-success flags have no authority. Forward values are separate from
 * reverse-DCF market-implied expectations. */
function verifyResearch(memo, dossier = {}) {
  const mandate = memo && memo.mandate;
  if (!memo || !memo.valuation) {
    if (memo && memo.proposedDecision === "BUY") fail("BUY_REQUIRES_VERIFIED_VALUATION");
    return null;
  }
  const v = memo.valuation, fc = mandate && mandate.forecast;
  if (v.method === "reverse_dcf") fail("REVERSE_DCF_IS_EXPECTATIONS_NOT_FORWARD_FORECAST");
  if (v.forwardInputBasis === "issuer_guidance" && (!v.guidanceClaimId || !(dossier.claimIds || []).includes(v.guidanceClaimId))) fail("GUIDANCE_SOURCE_NOT_IN_PACKET");
  if (new Set(v.scenarios.map(s => s.id)).size !== 3 || !["bear","base","bull"].every(id => v.scenarios.some(s=>s.id===id))) fail("VALUATION_REQUIRES_THREE_SCENARIOS");
  const price = fc && fc.basis.referencePriceMicros || dossier.card && dossier.card.price && dossier.card.price.closeMicros;
  if (!price || BigInt(price) <= 0n) fail("VALUATION_REFERENCE_MISSING");
  const input = { method:v.method, priceMicros:price,
    sharesOutstanding:dossier.fundamentals && dossier.fundamentals.sharesOutstanding || null,
    scenarios:v.scenarios.map(s=>({id:s.id,probabilityPpm:s.probabilityPpm,assumptions:s.assumptions,terminalPriceMicros:null})) };
  const result = V.run(input);
  const scenarios = result.scenarios.map((s,i) => {
    const authored = v.scenarios[i], fraction = BigInt(authored.realizationPpm);
    if (fraction < 0n || fraction > 1000000n) fail("REALIZATION_OUT_OF_RANGE");
    const terminal = BigInt(price) + (BigInt(s.valueMicros)-BigInt(price))*fraction/1000000n;
    if (terminal <= 0n || terminal.toString() !== authored.terminalPriceMicros) fail("TERMINAL_PRICE_NOT_CALCULATOR_BOUND");
    return { id:s.id, probabilityPpm:s.probabilityPpm, terminalPriceMicros:terminal.toString(), fairValueMicros:s.valueMicros,
      realizationPpm:authored.realizationPpm, inputsHash:s.inputsHash };
  });
  const binding = { schemaVersion:"verified-valuation.v1", symbol:memo.symbol, method:v.method,
    horizonTradingDays:v.horizonTradingDays, referencePriceMicros:price, scenarios,
    assumptionsHash:C.hash(input), dossierHash:dossier.dossierHash || dossier.contentHash || null, calculatorVersion:V.VALUATION_VERSION };
  binding.bindingHash = C.hash(binding);
  if (mandate && fc) {
    assertMandate(mandate, binding);
    if(memo.proposedDecision==="BUY") {
      const derived=V.forecastFromScenarios({basis:fc.basis,outcomeBuckets:fc.outcomeBuckets,fillProbabilityByExpiryPpm:fc.fillProbabilityByExpiryPpm});
      if(BigInt(derived.costAdjustedExpectedReturnBps)<=0n) fail("BUY_EXPECTED_RETURN_NOT_POSITIVE_AFTER_COSTS");
      if(BigInt(fc.fillProbabilityByExpiryPpm)<=0n) fail("BUY_FILL_PROBABILITY_ZERO");
    }
  }
  return binding;
}
function assertMandate(mandate, binding) {
  if (!binding || binding.symbol !== mandate.symbol) fail("VERIFIED_VALUATION_REQUIRED");
  const { bindingHash, ...payload } = binding;
  if (C.hash(payload) !== bindingHash) fail("VALUATION_BINDING_CORRUPT");
  const f = mandate.forecast;
  if (!f || f.horizonTradingDays !== binding.horizonTradingDays) fail("VALUATION_FORECAST_HORIZON_MISMATCH");
  if (f.basis.referencePriceMicros !== binding.referencePriceMicros) fail("VALUATION_FORECAST_REFERENCE_MISMATCH");
  const a = (f.outcomeBuckets || []).map(s=>({id:s.id,probabilityPpm:s.probabilityPpm,terminalPriceMicros:s.terminalPriceMicros})).sort((a,b)=>a.id.localeCompare(b.id));
  const b = binding.scenarios.map(s=>({id:s.id,probabilityPpm:s.probabilityPpm,terminalPriceMicros:s.terminalPriceMicros})).sort((a,b)=>a.id.localeCompare(b.id));
  if (C.hash(a) !== C.hash(b)) fail("FORECAST_NOT_BOUND_TO_RESEARCH");
  return true;
}
module.exports = { verifyResearch, assertMandate };
