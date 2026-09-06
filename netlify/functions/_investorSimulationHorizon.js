"use strict";
// Ex-ante horizon policy. Historical outcomes never select or extend a deadline.
// Research: Garleanu/Pedersen, Dynamic Trading with Predictable Returns and
// Transaction Costs (https://www.nber.org/papers/w15205): signal persistence,
// risk and trading costs matter jointly. The half-downside penalty and 25bps
// margin below are explicit experiment heuristics, not calibrated probabilities
// or parameters estimated by that paper. FINRA stop-order guidance informs
// adverse gap execution; NYSE calendars govern regular-session deadlines.
const M=require('./_investorMarket');
const RANGES=Object.freeze({range1:{min:1,max:2},range2:{min:3,max:5},range3:{min:6,max:8}});
const VERSION='required-investment.v3';
const fail=(code,message=code)=>{throw Object.assign(Error(message),{code});};
function policyFor(range) {
  if(typeof range!=='string'||!Object.hasOwn(RANGES,range))fail('BAD_REQUEST','Choose a company range before starting simulations.');
  return {version:VERSION,companyRange:range,minUsd:5000,maxUsd:30000,minCompanies:RANGES[range].min,maxCompanies:RANGES[range].max,maxTotalUsd:95000,maxHoldingSessions:3,entry:'FIRST_AVAILABLE_SESSION_PRICE',horizonMarginBps:'25',downsideWeightBps:'5000'};
}
function sessions(date,count=3) {
  const out=[];
  for(let i=0;i<15&&out.length<count;i++) {
    const d=new Date(Date.parse(date+'T12:00:00Z')+i*86400000),s=M.sessionState(d);
    if(s.tradingDay)out.push({date:s.date,openMs:M.nyWallClockToUtcMs(s.date,570),closeMs:M.sessionCloseMs(d)});
  }
  if(out.length!==count||out[0].date!==date)fail('HISTORICAL_SESSION_CLOSED');
  return out;
}
function replaySteps(schedule) {return schedule.flatMap(s=>Array.from({length:(s.closeMs-s.openMs)/300000+5},(_,i)=>s.openMs+i*300000));}
function score(x) {return x.expectedReturnBps-x.opportunityCostBps-Math.ceil(x.downsideBps/2)-x.uncertaintyPenaltyBps;}
function choose(horizons) {
  let best=1;
  for(let n=2;n<=3;n++) {
    const h=horizons['session'+n];
    if(h.evidenceConfidence!=='LOW'&&score(h)>score(horizons['session'+best])+25)best=n;
  }
  return best;
}
function validate(investment) {
  const h=investment.horizonAnalysis;
  if(!h||![1,2,3].includes(investment.holdingSessions))fail('SIMULATION_HORIZON_INVALID');
  for(let n=1;n<=3;n++) {
    const x=h['session'+n];
    if(!x||!Number.isInteger(x.expectedReturnBps)||x.expectedReturnBps < -10000||x.expectedReturnBps>100000||
      ['downsideBps','opportunityCostBps','uncertaintyPenaltyBps'].some(k=>!Number.isInteger(x[k])||x[k]<0||x[k]>10000)||
      !['LOW','MEDIUM','HIGH'].includes(x.evidenceConfidence)||!x.reason?.trim())fail('SIMULATION_HORIZON_INVALID');
  }
  if(h.session1.opportunityCostBps!==0||investment.holdingSessions!==choose(h)||!investment.holdingReason?.trim())fail('SIMULATION_HORIZON_INVALID');
}
module.exports={RANGES,VERSION,policyFor,sessions,replaySteps,score,choose,validate};
