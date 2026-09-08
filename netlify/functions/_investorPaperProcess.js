'use strict';
// Paper-only decision context: never changes Firestore's simulation namespace.
const {AsyncLocalStorage}=require('async_hooks');
const A=require('./_investorAdmin'),H=require('./_investorSimulationHorizon'),S=require('./_investorSimulationStrategy');
const context=new AsyncLocalStorage(),VERSION='paper-decision-process.v1';
const current=()=>context.getStore()||null;
const fail=(message)=>Object.assign(Error(message),{code:'SEMANTIC_REJECTED'});
const defaults=()=>({companyRange:'flex3',strategyVersionId:'active',revision:0,owner:null});
function validate(settings){if(!Object.hasOwn(H.RANGES,settings.companyRange)||typeof settings.strategyVersionId!=='string'||!settings.strategyVersionId||settings.strategyVersionId.length>100)throw fail('Choose a valid company range and strategy.');return settings;}
function create({admin=A,now=Date.now}={}){
 const ref=admin.col('InvestorAI_PaperDecisionSettings').doc('paper-1');
 const learning=()=>require('./_investorSimulationLearning').create({admin});
 async function settings(){return {...defaults(),...((await ref.get()).data()||{})};}
 async function resolve(s){validate(s);const strategy=s.owner?await learning().resolve(s.owner,s.strategyVersionId==='active'?undefined:s.strategyVersionId):null;return {version:VERSION,companyRange:s.companyRange,...H.RANGES[s.companyRange],strategy,settingsRevision:s.revision,preparedResearch:true,investmentPolicy:H.policyFor(s.companyRange,strategy,{shared:true,riskMandate:require('./_investorPolicy').loadActiveSync((await admin.col(admin.COL.control).doc('control').get()).data()||{}).riskMandate})};}
 async function save(owner,p){validate(p);if(p.strategyVersionId!=='active')await learning().resolve(owner,p.strategyVersionId);const tx=admin.runTransaction?f=>admin.runTransaction(f):f=>admin.db().runTransaction(f);await tx(async t=>{const old=(await t.get(ref)).data()||defaults();if(old.revision!==p.revision)throw Object.assign(Error('Paper settings changed in another window. Reload and try again.'),{code:'VERSION_CONFLICT'});t.set(ref,{companyRange:p.companyRange,strategyVersionId:p.strategyVersionId,revision:old.revision+1,owner,updatedAtMs:now()});});return settings();}
 return {settings,resolve,save};
}
function assertSnapshot(p){if(p.version!==VERSION||!H.RANGES[p.companyRange]||p.min!==H.RANGES[p.companyRange].min||p.max!==H.RANGES[p.companyRange].max)throw fail('Invalid saved paper decision process');if(p.strategy){S.validate(p.strategy.rules);if(S.hash(p.strategy.rules)!==p.strategy.rulesHash)throw fail('Saved strategy integrity check failed');}if(p.investmentPolicy&&!require('./_investorResearchHandoff').supportedInvestmentPolicy(p.investmentPolicy))throw fail('Invalid saved shared investment policy');return p;}
function instructions(p,fn){
 assertSnapshot(p);if(p.investmentPolicy?.coreVersion===H.CORE_VERSION)return H.sharedInstructions(p.investmentPolicy,fn);const s=p.strategy?.rules||S.DEFAULT;
 if(fn==='shortlistCandidates')return `Screen exactly the requested shortlist count (normally 50). The operator's finalist range applies to Astra's later underwriting, not this screening step. Apply this generalized selection guidance to the dated cards: ${s.selectionInstructions}. Never use training outcomes as current evidence.`;
 if(fn==='prepareResearchDocument')return 'Prepare every supplied finalist faithfully; do not rank investments or alter source facts to fit a strategy.';
 if(!['reviewUniverse','decidePreparedPortfolio','finalizePortfolio','researchCompany'].includes(fn))return '';
 let text=`PAPER DECISION PROCESS: ${VERSION}. Luna screens the eligible universe to 50 and prepares source documents; Astra makes one combined research and investment decision. For new entry-eligible companies choose ${p.min}–${p.max} finalists (${p.companyRange}), or all remaining eligible names when fewer exist. Existing holdings and pending orders do not consume this quota and must all remain managed. Never buy an owned or pending symbol. A finalist is not a forced purchase: missing evidence, nonpositive expected returns or insufficient paper account capacity require WATCH/ABSTAIN. Research requests outside the supplied candidates are forbidden. Keep coverage reasons under 15 words and all memo prose concise. Reason jointly about dated catalysts, downside, liquidity, correlated exposures, capital lockup and cash. A lower quote or hope of recovery alone is not grounds to sell immediately or extend a losing position. Match stops to observed volatility and the planned horizon; no same-bar hindsight, no averaging down. Preserve existing protection and risk mandates. New decisions use strategy ${p.strategy?.versionId||'baseline'} (${p.strategy?.rulesHash||S.hash(s)}). Historical performance informs uncertain hypotheses, never guarantees future returns.\n`;
 text+='Compare ALL supplied finalists together, even when there are more than two; complete the memo and allocation coverage for every supplied symbol.\n';
 text+=`STRATEGY RULES (apply to new decisions only): ${JSON.stringify(s)}\nUse selectionInstructions, allocationInstructions and exitInstructions as investment guidance, subject to source honesty and fixed paper risk limits. Compare 1, 2 and 3 trading sessions before entry: score=(expectedReturnBps*returnWeight - downsideBps*downsideWeight - opportunityCostBps*opportunityWeight - uncertaintyPenaltyBps*uncertaintyWeight)/100. Session 1 opportunity cost is zero. Start at 1; allow a longer horizon only with MEDIUM/HIGH dated evidence and improvement strictly greater than horizonMarginBps, and never beyond maxHoldingSessions. Use this comparison when setting the forecast horizon and time exit. In the paper order contract timeExit triggers at the next eligible open after triggerAfterSessionDate; budget that overnight exposure explicitly within your chosen horizon. Do not automatically extend existing holdings. Size the conviction-based allocation by allocationScalePct/100, and conviction/volatility-based stop and target distances by stopScalePct/100 and targetScalePct/100, exactly once, before authoring the FINAL mandate. The paper executor receives final terms, not unscaled simulation inputs. Set validFrom no earlier than session open plus entryDelayMinutes, entry LIMIT at least pullbackBps below the supplied reference when nonzero, and authorize entry only in the initial session. Avoid entry after entryWindowMinutes; expired or missed entries remain cash. Paper whole-share cash, concentration, risk, source verification and persistent protective-order checks remain binding. Explain the selected horizon, weighted comparison, base and scaled allocation/stop/target, entry timing and retained cash in the memo and comparisonNote. Never copy training-day future prices or outcomes into current evidence.`;
 return text;
}
function researchBounds(requests,p,entrySymbols,heldSymbols){
 const held=new Set(heldSymbols),eligible=new Set(entrySymbols),fresh=requests.filter(r=>!held.has(r.symbol));
 if(new Set(requests.map(r=>r.symbol)).size!==requests.length||fresh.some(r=>!eligible.has(r.symbol))||requests.some(r=>!eligible.has(r.symbol)&&!held.has(r.symbol))||fresh.length>p.max||fresh.length<Math.min(p.min,eligible.size))throw fail('Astra finalist selection does not match the saved company range');
 return requests;
}
function entryTiming(proposal,p=current()){
 if(!p||proposal.decision!=='BUY')return null;
 const entry=proposal.action.entry,dates=entry.authorizedSessionDates,r=p.strategy?.rules||S.DEFAULT;
 if(dates.length!==1)throw fail('New paper entries must be limited to their initial session');
 const session=H.sessions(dates[0],1)[0],declared=Date.parse(entry.validFrom);
 const validFromMs=Math.max(session.openMs+r.entryDelayMinutes*60000,declared),expiresAtMs=Math.min(session.closeMs,session.openMs+r.entryWindowMinutes*60000);
 if(!Number.isFinite(validFromMs)||validFromMs>=expiresAtMs)throw fail('Paper entry timing falls outside the selected strategy window');
 return {validFromMs,expiresAtMs};
}
module.exports={VERSION,current,withProcess:(p,fn)=>context.run(p,fn),defaults,validate,create,assertSnapshot,instructions,researchBounds,entryTiming};
