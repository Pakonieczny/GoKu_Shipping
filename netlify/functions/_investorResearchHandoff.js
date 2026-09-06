"use strict";
// Simulation-only preparation and joint underwriting. Schemas live outside the
// global policy identity so existing paid requests keep their original keys.
const P = require('./_investorPolicy');
const C = require('./_investorDecisionContext');
const VERSION = 'luna-astra-handoff.v1';
const SECTIONS = ['business','financialHealth','recentChanges','valuationInputs','catalysts','risks','contradictions','marketContext','missingInformation'];
const obj = properties => ({type:'object',additionalProperties:false,required:Object.keys(properties),properties});
const text = maxLength => ({type:'string',maxLength});
const array = (items,maxItems) => ({type:'array',items,maxItems});
const section = obj({summary:text(600),evidenceIds:array(text(200),12),missing:array(text(250),6)});
const SCHEMAS = Object.freeze({
  'prepared-research-document.v1':obj({schemaVersion:{type:'string',enum:['prepared-research-document.v1']},symbol:text(12),sections:obj(Object.fromEntries(SECTIONS.map(k=>[k,section])))}),
  'prepared-investment-decision.v1':obj({schemaVersion:{type:'string',enum:['prepared-investment-decision.v1']},research:array(P.SCHEMAS['research-memo.v1'],2),allocation:P.SCHEMAS['portfolio-synthesis.v1']}),
});
const fail = code => {throw Object.assign(Error(code),{code});};
function sourcePacket(packet, supplemental = {}) {
  const catalog=[];
  for(const claim of packet.claims||[]) if(claim.claimId && claim.documentVersionId) {
    if(claim.publishedAtMs && claim.publishedAtMs>packet.cutoffMs)fail('HANDOFF_FUTURE_EVIDENCE');
    catalog.push({id:'claim:'+claim.claimId,kind:'claim',...claim});
  }
  for(const fact of supplemental.filings?.facts||[]) {
    if(!fact.factId)continue;
    if(fact.filedDate && fact.filedDate>new Date(packet.cutoffMs).toISOString().slice(0,10))fail('HANDOFF_FUTURE_EVIDENCE');
    catalog.push({id:'fact:'+fact.factId,kind:'financial_fact',...fact,lineage:(supplemental.filings.lineage||[]).filter(l=>l.factId===fact.factId)});
  }
  const ids=new Set();for(const row of catalog){if(ids.has(row.id))fail('HANDOFF_DUPLICATE_EVIDENCE');ids.add(row.id);}
  const baseline=Object.fromEntries(['symbol','cutoffMs','dossierVersionId','dossierHash','identity','card','fundamentals','guidance','nextEarnings','sectorBlock','marketObservation','freshness','dataQuality','pendingChanges','learning','prior','documents'].map(k=>[k,packet[k]??null]));
  const missing=[];
  for(const [key,value] of Object.entries(supplemental))if(!value||value.missing||value.error||value.truncated)missing.push({source:key,reason:value?.reason||value?.error||'Unavailable or incomplete',truncated:!!value?.truncated});
  return {version:VERSION,baseline,catalog,decisionData:supplemental.decisionData||null,missing};
}
function bindDocument(output, source) {
  const errors=P.validateAgainst(SCHEMAS['prepared-research-document.v1'],output);
  if(errors.length)fail('HANDOFF_SCHEMA_INVALID');
  if(output.symbol!==source.baseline.symbol)fail('HANDOFF_SYMBOL_MISMATCH');
  const byId=new Map(source.catalog.map(x=>[x.id,x])),ids=[...new Set([...Object.values(output.sections).flatMap(x=>x.evidenceIds),...source.catalog.filter(x=>x.kind==='claim'&&['GUIDANCE','EARNINGS_DATE','RISK_FACTOR','CONTRADICTION'].includes(x.claimType)).map(x=>x.id)])];
  if(ids.some(id=>!byId.has(id)))fail('HANDOFF_UNKNOWN_EVIDENCE');
  const document={version:VERSION,symbol:output.symbol,cutoffMs:source.baseline.cutoffMs,
    preparationLabel:'Luna summaries are navigation and interpretation, not independently verified evidence. Use the attached exact source records.',
    sections:output.sections,baseline:source.baseline,evidence:ids.map(id=>byId.get(id)),missing:source.missing,
    decisionData:source.decisionData,sourceHash:C.hash(source)};
  document.documentHash=C.hash(document);return document;
}
function assertDocument(document) {
  const {documentHash,...content}=document;
  if(document.version!==VERSION||C.hash(content)!==documentHash)fail('HANDOFF_DOCUMENT_CORRUPT');
  return document;
}
function validateJoint(output, packets, heldSymbols=[], expansionBlocked=false) {
  if(P.validateAgainst(SCHEMAS['prepared-investment-decision.v1'],output).length)fail('HANDOFF_DECISION_SCHEMA_INVALID');
  const symbols=packets.map(p=>p.symbol),got=output.research.map(m=>m.symbol);
  if(!symbols.length||symbols.length>2||new Set(got).size!==got.length||got.length!==symbols.length||got.some(s=>!symbols.includes(s)))fail('HANDOFF_RESEARCH_COVERAGE_INVALID');
  const V=require('./_investorDecisionValidation'),verified={};
  for(const memo of output.research) {
    const packet=packets.find(p=>p.symbol===memo.symbol),claims=new Map((packet.claims||[]).map(c=>[c.claimId,c]));
    if(Date.parse(memo.asOf)!==packet.cutoffMs)fail('HANDOFF_AS_OF_MISMATCH');
    if(memo.proposedDecision==='BUY'&&!memo.mandate)fail('HANDOFF_BUY_WITHOUT_MANDATE');
    if(memo.mandate&&memo.mandate.symbol!==memo.symbol)fail('HANDOFF_MANDATE_SYMBOL_MISMATCH');
    if(memo.mandate&&Date.parse(memo.mandate.asOf)>packet.cutoffMs)fail('HANDOFF_FUTURE_MANDATE');
    for(const source of memo.mandate?.sourceManifest||[])if(claims.get(source.claimId)?.documentVersionId!==source.documentVersionId)fail('HANDOFF_SOURCE_MISMATCH');
    for(const premise of memo.factualPremises||[])if(claims.get(premise.claimId)?.documentVersionId!==premise.documentVersionId)fail('HANDOFF_SOURCE_MISMATCH');
    verified[memo.symbol]=V.verifyResearch(memo,packet);
  }
  const a=output.allocation,expected=[...new Set([...symbols,...heldSymbols])],decisions=a.decisions.map(x=>x.symbol);
  if(decisions.length!==expected.length||new Set(decisions).size!==decisions.length||decisions.some(s=>!expected.includes(s)))fail('HANDOFF_DECISION_COVERAGE_INVALID');
  const buys=a.decisions.filter(d=>d.decision==='BUY'),ranks=buys.map(d=>d.capitalRank),mandates=a.expansionMandates.map(m=>m.symbol);
  if(ranks.some(r=>r===null)||new Set(ranks).size!==ranks.length||new Set(mandates).size!==mandates.length)fail('HANDOFF_RANK_OR_MANDATE_DUPLICATE');
  if(expansionBlocked&&mandates.length)fail('EXPANSION_BLOCKED');
  for(const m of a.expansionMandates) {
    const d=a.decisions.find(d=>d.symbol===m.symbol),memo=output.research.find(r=>r.symbol===m.symbol);
    if(!symbols.includes(m.symbol)||heldSymbols.includes(m.symbol)||m.decision!=='BUY'||d?.decision!=='BUY'||d.fundingState!=='FUNDED'||memo?.proposedDecision!=='BUY')fail('HANDOFF_UNAUTHORIZED_MANDATE');
    // One authored set of terms: synthesis may not silently rewrite underwriting.
    if(C.hash(m)!==C.hash(memo.mandate))fail('HANDOFF_CONFLICTING_MANDATE');
    if(m.allocation.capitalRank!==d.capitalRank)fail('HANDOFF_RANK_MISMATCH');
    V.assertMandate(m,verified[m.symbol]);
  }
  if(buys.some(d=>heldSymbols.includes(d.symbol)||d.fundingState!=='FUNDED'||!mandates.includes(d.symbol)))fail('HANDOFF_UNFUNDED_BUY');
  const hs=a.holdingAnalysis.map(h=>h.symbol);
  if(hs.length!==heldSymbols.length||new Set(hs).size!==hs.length||hs.some(s=>!heldSymbols.includes(s))||a.holdingAnalysis.some(h=>!a.decisions.some(d=>d.symbol===h.symbol&&d.decision===h.decision)))fail('FINAL_HOLDING_COVERAGE_INVALID');
  return verified;
}
module.exports={VERSION,SECTIONS,SCHEMAS,sourcePacket,bindDocument,assertDocument,validateJoint};
