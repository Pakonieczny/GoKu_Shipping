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
    catalog.push({...claim,id:'claim:'+claim.claimId,kind:'claim'});
  }
  for(const fact of supplemental.filings?.facts||[]) {
    if(!fact.factId)continue;
    if(fact.filedDate && fact.filedDate>new Date(packet.cutoffMs).toISOString().slice(0,10))fail('HANDOFF_FUTURE_EVIDENCE');
    catalog.push({...fact,id:'fact:'+fact.factId,kind:'financial_fact',lineage:(supplemental.filings.lineage||[]).filter(l=>l.factId===fact.factId)});
  }
  const ids=new Set();for(const row of catalog){if(ids.has(row.id))fail('HANDOFF_DUPLICATE_EVIDENCE');ids.add(row.id);}
  const baseline=Object.fromEntries(['symbol','cutoffMs','dossierVersionId','dossierHash','identity','card','fundamentals','guidance','nextEarnings','sectorBlock','marketObservation','freshness','dataQuality','pendingChanges','learning','prior','documents'].map(k=>[k,packet[k]??null]));
  const missing=[];
  for(const [key,value] of Object.entries(supplemental))if(!value||value.missing||value.error||value.truncated)missing.push({source:key,reason:value?.reason||value?.error||'Unavailable or incomplete',truncated:!!value?.truncated});
  return {version:VERSION,baseline,catalog,decisionData:supplemental.decisionData||null,missing};
}
// Citation handles are deterministic for the frozen packet. Baseline entries
// point to data already attached to both models; they do not manufacture claims.
function citationCatalog(source) {
  const catalog=[...source.catalog];
  for(const [key,value] of Object.entries(source.baseline))if(value!==null&&value!==undefined)
    catalog.push({id:'baseline:'+key,kind:'baseline',path:'baseline.'+key});
  if(source.decisionData)catalog.push({id:'supplemental:decisionData',kind:'supplemental',path:'decisionData'});
  if(new Set(catalog.map(x=>x.id)).size!==catalog.length)fail('HANDOFF_DUPLICATE_EVIDENCE');
  return catalog.map((row,i)=>({...row,citationId:'source:'+i}));
}
function preparationWire(source) {
  const catalog=citationCatalog(source),ids=catalog.map(x=>x.citationId),schema=JSON.parse(JSON.stringify(SCHEMAS['prepared-research-document.v1']));
  // One shared definition avoids repeating hundreds of enum values nine times.
  // Keep long original IDs in the packet, outside the constrained schema.
  schema.$defs={evidenceId:ids.length<999?{type:'string',enum:ids.length?ids:['no_source_available']}:{type:'string',pattern:'^(?:'+ids.join('|')+')$'}};
  for(const section of Object.values(schema.properties.sections.properties)) {
    section.properties.evidenceIds.items={$ref:'#/$defs/evidenceId'};
    if(!ids.length)section.properties.evidenceIds.maxItems=0;
  }
  schema.properties.symbol={type:'string',enum:[source.baseline.symbol]};
  return {source:{...source,catalog},schema,instructions:'In every evidenceIds array, select only the exact citationId values (source:0, source:1, etc.) from this packet. Never write raw claimId, factId, document IDs or invented names there. Baseline and supplemental entries point to the attached fields. Leave evidenceIds empty when no supplied record supports the summary, and describe the missing information. This citation format supersedes the earlier catalog-id instruction.'};
}
function bindDocument(output, source, {recoverReferences=false}={}) {
  const errors=P.validateAgainst(SCHEMAS['prepared-research-document.v1'],output);
  if(errors.length)fail('HANDOFF_SCHEMA_INVALID');
  if(output.symbol!==source.baseline.symbol)fail('HANDOFF_SYMBOL_MISMATCH');
  const catalog=citationCatalog(source),byId=new Map(catalog.map(x=>[x.id,x])),aliases=new Map();
  const alias=(key,id)=>{if(!key)return;const matches=aliases.get(key)||new Set();matches.add(id);aliases.set(key,matches);};
  for(const row of catalog) {
    alias(row.id,row.id);alias(row.citationId,row.id);
    if(recoverReferences){alias(row.claimId,row.id);alias(row.factId,row.id);if(row.kind==='baseline'){alias(row.path,row.id);alias(row.path.slice(9),row.id);}}
  }
  const recovery={version:'citation-recovery.v1',normalizedReferences:[],discardedSections:[],unknownEvidenceIds:[]},sections={};
  for(const [key,section] of Object.entries(output.sections)) {
    const ids=[],unknown=[];
    for(const original of section.evidenceIds) {
      const token=recoverReferences?original.trim():original,matches=aliases.get(token);
      if(matches?.size!==1){unknown.push(original);continue;}
      const id=[...matches][0];ids.push(id);
      if(id!==original&&!token.startsWith('source:'))recovery.normalizedReferences.push({from:original,to:id});
    }
    if(unknown.length&&!recoverReferences)fail('HANDOFF_UNKNOWN_EVIDENCE');
    if(unknown.length) {
      recovery.discardedSections.push(key);recovery.unknownEvidenceIds.push(...unknown);
      // Never keep unsupported prose while silently removing its bad citations.
      sections[key]={summary:'Summary omitted because its source references could not be verified. Analyze the attached original evidence directly.',evidenceIds:[...new Set(ids)],missing:['The original source records require direct assessment by Astra.']};
    } else sections[key]={...section,evidenceIds:[...new Set(ids)]};
  }
  const recovered=recovery.discardedSections.length>0;
  const ids=[...new Set([...Object.values(sections).flatMap(x=>x.evidenceIds),...catalog.filter(x=>recovered||x.kind==='claim'&&['GUIDANCE','EARNINGS_DATE','RISK_FACTOR','CONTRADICTION'].includes(x.claimType)).map(x=>x.id)])];
  const document={version:VERSION,symbol:output.symbol,cutoffMs:source.baseline.cutoffMs,
    preparationLabel:'Luna summaries are navigation and interpretation, not independently verified evidence. Use the attached exact source records.',
    sections,baseline:source.baseline,evidence:ids.map(id=>byId.get(id)),missing:source.missing,
    decisionData:source.decisionData,sourceHash:C.hash(source)};
  if(recovered||recovery.normalizedReferences.length)document.referenceRecovery=recovery;
  document.documentHash=C.hash(document);return document;
}
function assertDocument(document) {
  const {documentHash,...content}=document;
  if(document.version!==VERSION||C.hash(content)!==documentHash)fail('HANDOFF_DOCUMENT_CORRUPT');
  return document;
}
function jointSchema(count) {
  if(!Number.isInteger(count)||count<1||count>304)fail("HANDOFF_RESEARCH_COVERAGE_INVALID");
  const schema=JSON.parse(JSON.stringify(SCHEMAS["prepared-investment-decision.v1"]));
  schema.properties.research.minItems=count;schema.properties.research.maxItems=count;return schema;
}
function validateJoint(output, packets, heldSymbols=[], expansionBlocked=false, schema=null) {
  if(P.validateAgainst(schema||SCHEMAS['prepared-investment-decision.v1'],output).length)fail('HANDOFF_DECISION_SCHEMA_INVALID');
  const symbols=packets.map(p=>p.symbol),got=output.research.map(m=>m.symbol);
  if(!symbols.length||symbols.length>(schema?304:2)||new Set(got).size!==got.length||got.length!==symbols.length||got.some(s=>!symbols.includes(s)))fail('HANDOFF_RESEARCH_COVERAGE_INVALID');
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
// Versioned, simulation-only policy. It never grants authority to the paper desk.
const INVESTMENT_POLICY = Object.freeze({version:'required-investment.v1',minUsd:5000,maxUsd:30000,maxCompanies:2,entry:'FIRST_AVAILABLE_SESSION_PRICE'});
const DIVERSIFIED_POLICY = Object.freeze({version:'required-investment.v2',minUsd:5000,maxUsd:30000,minCompanies:4,maxCompanies:7,maxTotalUsd:95000,entry:'FIRST_AVAILABLE_SESSION_PRICE'});
function supportedInvestmentPolicy(policy) {
  if(policy?.version==='shared-investment.v1'){try{const canonical=require('./_investorSimulationHorizon').policyFor(policy.companyRange,policy.strategy||null,{shared:true,riskMandate:policy.riskMandate,cashPolicy:Object.hasOwn(policy,'cashPolicy')?policy.cashPolicy:null});return C.hash(canonical)===C.hash(policy)?canonical:null;}catch{return null;}}
  if(policy?.version==='required-investment.v4'){try{const canonical=require('./_investorSimulationHorizon').policyFor(policy.companyRange,policy.strategy);return C.hash(canonical)===C.hash(policy)?canonical:null;}catch{return null;}}
  return [INVESTMENT_POLICY,DIVERSIFIED_POLICY,...Object.keys(require('./_investorSimulationHorizon').RANGES).map(r=>require('./_investorSimulationHorizon').policyFor(r))].find(p=>policy?.version===p.version&&C.hash(policy)===C.hash(p))||null;
}
function assertInvestmentAllocations(investments,policy) {
  if(!supportedInvestmentPolicy(policy))fail('SIMULATION_INVESTMENT_POLICY_INVALID');
  const all=Object.values(investments||{}),cashAllowed=policy.minCompanies===0||!!policy.coreVersion;
  if(policy.maxHoldingSessions)all.forEach(r=>require('./_investorSimulationHorizon').validate(r,policy));
  // With a cash-allowed policy every finalist carries an explicit BUY or PASS; PASS rows reserve nothing.
  if(cashAllowed&&all.some(r=>!['BUY','PASS'].includes(r?.decision)||(r.decision==='PASS'&&r.allocationUsd!==0)))fail('SIMULATION_ALLOCATION_INVALID');
  if(all.length>policy.maxCompanies)fail('SIMULATION_FINALISTS_INVALID');
  const rows=cashAllowed?all.filter(r=>r.decision==='BUY'):all;
  if(rows.length<(cashAllowed?0:(policy.minCompanies||1)))fail('SIMULATION_FINALISTS_INVALID');
  if(rows.some(r=>!Number.isSafeInteger(r?.allocationUsd)||r.allocationUsd<policy.minUsd||r.allocationUsd>policy.maxUsd))fail('SIMULATION_ALLOCATION_INVALID');
  if(rows.reduce((n,r)=>n+r.allocationUsd,0)>(policy.maxTotalUsd||policy.maxCompanies*policy.maxUsd))fail('SIMULATION_TOTAL_ALLOCATION_INVALID');
}
function investmentSchema(documents,policy=INVESTMENT_POLICY) {
  const investment=obj({allocationUsd:{type:'integer',minimum:5000,maximum:30000},conviction:{type:'string',enum:['LOW','MEDIUM','HIGH']},
    sizingReason:text(600),assessment:obj(Object.fromEntries(SECTIONS.map(k=>[k,text(policy.version!==INVESTMENT_POLICY.version?600:1200)]))),
    outlook:text(1200),takeProfitBps:{type:'integer',minimum:1,maximum:100000},stopLossBps:{type:'integer',minimum:1,maximum:9500},
    evidenceIds:array(text(200),24)});
  if(policy.maxHoldingSessions) {
    const estimate=obj({expectedReturnBps:{type:'integer',minimum:-10000,maximum:100000},downsideBps:{type:'integer',minimum:0,maximum:10000},opportunityCostBps:{type:'integer',minimum:0,maximum:10000},uncertaintyPenaltyBps:{type:'integer',minimum:0,maximum:10000},evidenceConfidence:{type:'string',enum:['LOW','MEDIUM','HIGH']},reason:{type:'string',minLength:1,maxLength:300}});
    Object.assign(investment.properties,{holdingSessions:{type:'integer',minimum:1,maximum:3},holdingReason:{type:'string',minLength:1,maxLength:600},horizonAnalysis:obj({session1:estimate,session2:estimate,session3:estimate})});
    if(policy.minCompanies===0||policy.coreVersion){investment.properties.allocationUsd={type:'integer',minimum:0,maximum:30000};Object.assign(investment.properties,{decision:{type:'string',enum:['BUY','PASS']},decisionReason:{type:'string',minLength:1,maxLength:400}});}
    investment.required=Object.keys(investment.properties);
  }
  return obj({schemaVersion:{type:'string',enum:['simulation-investment-plan.v1']},comparisonNote:text(1200),
    investments:obj(Object.fromEntries(documents.map(d=>{const row=JSON.parse(JSON.stringify(investment));
      const ids=[...new Set([...d.evidence.map(e=>e.id),...Object.keys(d.baseline).map(k=>'baseline:'+k)])];
      row.properties.evidenceIds.items={type:'string',enum:ids};return [d.symbol,row];}))) });
}
function validateInvestmentPlan(output,documents,policy=INVESTMENT_POLICY) {
  if(documents.length<(policy.minCompanies||1)||documents.length>policy.maxCompanies||new Set(documents.map(d=>d.symbol)).size!==documents.length)fail('SIMULATION_FINALISTS_INVALID');
  documents.forEach(assertDocument);
  if(P.validateAgainst(investmentSchema(documents,policy),output).length)fail('SIMULATION_INVESTMENT_PLAN_INVALID');
  assertInvestmentAllocations(output.investments,policy);
  if(policy.coreVersion&&documents.some(d=>output.investments[d.symbol]?.decision==='BUY'&&(!d.baseline.dossierVersionId||!output.investments[d.symbol].evidenceIds.some(id=>d.evidence.some(e=>e.id===id&&['claim','financial_fact'].includes(e.kind))))))fail('HANDOFF_BUY_WITHOUT_EVIDENCE');
  if(documents.some(d=>d.cutoffMs!==documents[0].cutoffMs))fail('HANDOFF_AS_OF_MISMATCH');
  const plan={...output,policy,cutoffMs:documents[0].cutoffMs,...(policy.maxHoldingSessions?{sessions:require('./_investorSimulationHorizon').sessions(require('./_investorMarket').nyParts(new Date(documents[0].cutoffMs)).date)}:{}),
    ...(policy.coreVersion?{liquidityBySymbol:Object.fromEntries(documents.map(d=>[d.symbol,{advMinor:String(Math.round(Number((d.baseline.marketObservation?.technicals?.bars>=10?d.baseline.marketObservation.technicals.liquidity?.avgDollarVolume20Usd:0)||0)*100))}]))}:{}),
    documentHashes:Object.fromEntries(documents.map(d=>[d.symbol,d.documentHash]))};
  return {...plan,planHash:C.hash(plan)};
}
module.exports={jointSchema,VERSION,SECTIONS,SCHEMAS,sourcePacket,citationCatalog,preparationWire,bindDocument,assertDocument,validateJoint,INVESTMENT_POLICY,DIVERSIFIED_POLICY,supportedInvestmentPolicy,assertInvestmentAllocations,investmentSchema,validateInvestmentPlan};
