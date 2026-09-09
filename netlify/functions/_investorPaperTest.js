'use strict';
// Explicit operator integration test. Never an AI recommendation or live order.
const P=require('./_investorPolicy'),H=require('./_investorSimulationHorizon'),A=require('./_investorAdmin');
const fail=message=>{throw Object.assign(Error(message),{code:'STATE_CONFLICT'});};
async function submit({admin,accountId,symbol,sourcePlanId,actorId,nowMs}) {
 if(A.currentScope())fail('Paper tests cannot run in a simulation');
 const ctrl=(await admin.col(admin.COL.control).doc('control').get()).data()||{};
 if(accountId!=='paper-1'||ctrl.engineMode!=='manager'||(ctrl.accountMode||ctrl.mode)!=='PAPER_AI'||ctrl.killSwitch||ctrl.freezeNewBuys||ctrl.buyState==='FROZEN'||ctrl.executorState==='PAUSED_SAFETY'||ctrl.executorEnabled===false||ctrl.emergencyState==='ENGAGED')fail('Paper execution must be enabled with buys open');
 const session=require('./_investorMarket').sessionState(new Date(nowMs));
 if(!session.open)fail('Run this test during the regular market session');
 const key='operator_paper_test_'+accountId+'_'+session.date,ref=admin.col(admin.COL.portfolioPlans).doc(key);
 const prior=(await ref.get()).data();
 if(prior){if(prior.symbol!==symbol||prior.sourcePlanId!==sourcePlanId)fail('Only one paper purchase test is permitted per account per trading day');return finish(prior);}
 const source=(await admin.col(admin.COL.portfolioPlans).doc(sourcePlanId).get()).data();
 if(!source||source.accountId!==accountId||!source.policy?.coreVersion||source.operatorTest||!source.investments?.[symbol])fail('Choose a finalist from a saved shared paper decision');
 const {planHash,accountId:unused,managerRunId,createdAtMs,...content}=source;
 if(P.sha256(content)!==planHash||require('./_investorMarket').nyParts(new Date(source.cutoffMs)).date!==session.date)fail('The saved decision must be intact and from today');
 const liquidity=source.liquidityBySymbol?.[symbol];
 if(!liquidity||!/^\d+$/.test(String(liquidity.advMinor))||BigInt(liquidity.advMinor)<=0n)fail('The saved finalist lacks observed liquidity needed by the shared risk checks');
 const policy=H.policyFor('top1',null,{shared:true,riskMandate:P.loadActiveSync(ctrl).riskMandate});
 const original=source.investments[symbol];
 const investment={...original,decision:'BUY',allocationUsd:5000,stopLossBps:Math.max(300,original.stopLossBps||0),takeProfitBps:Math.max(600,original.takeProfitBps||0),
   decisionReason:'OPERATOR PAPER TEST: purchase requested to validate execution; not approved by AI.',sizingReason:'Maximum $5,000 paper allocation, reduced by normal cash and risk checks. No AI spend.',holdingSessions:1,holdingReason:'Operator test closes by this session deadline; no overnight extension.',
   horizonAnalysis:Object.fromEntries([1,2,3].map(n=>['session'+n,{expectedReturnBps:0,downsideBps:0,opportunityCostBps:0,uncertaintyPenaltyBps:0,evidenceConfidence:'LOW',reason:'No investment forecast: operator integration test.'}]))};
 const test={schemaVersion:'simulation-investment-plan.v1',comparisonNote:investment.decisionReason,policy,cutoffMs:nowMs,sessions:H.sessions(session.date),investments:{[symbol]:investment},liquidityBySymbol:{[symbol]:liquidity},documentHashes:{},
   operatorTest:{version:'operator-paper-test.v1',actorId,sourcePlanId,originalDecision:original.decision,originalReason:original.decisionReason,maximumAllocationUsd:5000,excludeFromLearning:true}};
 const plan={...test,planHash:P.sha256(test)};
 const record={symbol,sourcePlanId,plan,createdAtMs:nowMs};
 await admin.runTransaction(async tx=>{if((await tx.get(ref)).exists)fail('A paper test was just requested; refresh its status');tx.set(ref,record);});
 return finish(record);
 async function finish(record){const result=await require('./_investorExecution').saveSharedPaperPlan({plan:record.plan,admin,accountId,managerRunId:key,nowMs:record.createdAtMs});return {...result,symbol:record.symbol,authority:'OPERATOR_PAPER_TEST',maximumAllocationUsd:5000,aiCostMinor:'0',status:'AWAITING_OBSERVED_MARKET_BAR',earliestBarAtMs:Math.ceil(record.createdAtMs/300000)*300000,feedDelayMinutes:20};}
}
module.exports={submit};
