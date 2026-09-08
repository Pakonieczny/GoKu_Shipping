'use strict';
// Current-source adapter. No model calls: collect public evidence once, then
// the shared Luna/Astra pipeline prepares and decides against a frozen cutoff.
const A=require('./_investorAdmin');
async function prepare(symbol,{admin=A,now=Date.now,sources=require('./_investorIntelligenceSources'),fundamentals=require('./_investorFundamentals'),dossier=require('./_investorDossier')}={}){
  if(A.currentScope())throw Object.assign(Error('Current source preparation is not permitted in a simulation'),{code:'SIMULATION_SOURCE_FORBIDDEN'});
  const U=require('./_investorUniverse'),row=[...U.tradeTier,...U.researchTier].find(r=>r.symbol===symbol)||{symbol};
  const profile=await sources.resolveIdentity(row),errors=[];
  const poll=await sources.pollCompany(profile,{budgetMs:45000});
  for(const r of poll.results||[])if(r.error)errors.push({source:r.sourceId,error:String(r.error).slice(0,160)});
  if(profile.cik)try{await fundamentals.ingestCompanyFacts({cik:profile.cik,asOfMs:now()});}catch(e){errors.push({source:'SEC facts',error:String(e.message).slice(0,160)});}
  const built=await dossier.buildBaseline({symbol,rosterRow:{...row,cik:profile.cik||row.cik},asOfMs:now(),admin});
  await dossier.persistVersion(built.version,{admin,nowMs:now()});
  return {symbol,completedAtMs:now(),errors};
}
module.exports={prepare};
