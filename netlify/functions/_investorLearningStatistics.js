'use strict';
// Exploratory statistics, calculated without AI. No significance or causal claims.
// Clustering motivation: Abadie et al., QJE 2023, doi:10.1093/qje/qjac038.
// Selection-bias caution: Bailey & Lopez de Prado, J Portfolio Management 2014,
// doi:10.3905/jpm.2014.40.5.094. DSR/PBO are NOT estimated from sparse run summaries.
const S=require('./_investorSimulationStrategy');
const finite=Number.isFinite,mean=a=>a.length?a.reduce((s,x)=>s+x,0)/a.length:null;
const quantile=(a,p)=>{if(!a.length)return null;const x=[...a].sort((a,b)=>a-b),i=(x.length-1)*p,k=Math.floor(i);return x[k]+(x[Math.ceil(i)]-x[k])*(i-k);};
function windowOf(r){const dates=[r.date,...(r.sessions||[]).map(s=>s.date)].filter(d=>typeof d==='string'&&/^\d{4}-\d{2}-\d{2}$/.test(d)).sort();return dates.length?[dates[0],dates.at(-1)]:null;}
function blocks(rows){
 const out=[];for(const x of rows.map(r=>({r,w:windowOf(r)})).filter(x=>x.w).sort((a,b)=>a.w[0].localeCompare(b.w[0])||a.w[1].localeCompare(b.w[1]))){let b=out.at(-1);if(!b||x.w[0]>b.end){b={start:x.w[0],end:x.w[1],rows:[]};out.push(b);}b.end=b.end>x.w[1]?b.end:x.w[1];b.rows.push(x.r);}return out;
}
function uncertainty(rows,metric='returnBps'){
 const values=blocks(rows).map(b=>mean(b.rows.map(r=>r[metric]).filter(finite))).filter(finite);
 const n=values.length,estimate=mean(values);let interval95=null;
 // Equal weight per non-overlapping connected date block; repeated runs cannot
 // increase the block count. An exploratory percentile bootstrap, not a test.
 if(n>=20){let seed=0x1234abcd;const random=()=>{seed=(Math.imul(1664525,seed)+1013904223)>>>0;return seed/4294967296;};const samples=[];for(let i=0;i<2000;i++){let sum=0;for(let j=0;j<n;j++)sum+=values[Math.floor(random()*n)];samples.push(sum/n);}interval95=[quantile(samples,.025),quantile(samples,.975)];}
 const omit=n>1?values.map(x=>(estimate*n-x)/(n-1)):[];
 return {blocks:n,blockWeightedMeanBps:estimate,interval95Bps:interval95,leaveOneBlockOutRangeBps:omit.length?[Math.min(...omit),Math.max(...omit)]:null,method:'Equal-weight overlap-block means; 2,000 deterministic bootstrap resamples. Exploratory 95% interval only at 20+ blocks; this threshold is a display safeguard, not proof of adequate data. Serial dependence between disjoint blocks may remain.'};
}
function summarize(rows){const a=rows.map(r=>r.returnBps).filter(finite),excess=rows.filter(r=>finite(r.benchmarkReturnBps)).map(r=>({...r,excess:r.returnBps-r.benchmarkReturnBps}));return {count:rows.length,uniqueDates:new Set(rows.map(r=>r.date).filter(Boolean)).size,meanReturnBps:mean(a),medianReturnBps:quantile(a,.5),p10ReturnBps:quantile(a,.1),p90ReturnBps:quantile(a,.9),wins:a.filter(x=>x>0).length,losses:a.filter(x=>x<0).length,flat:a.filter(x=>x===0).length,worstDrawdownBps:rows.some(r=>finite(r.maxDrawdownBps))?Math.max(...rows.map(r=>r.maxDrawdownBps).filter(finite)):null,benchmarkCount:excess.length,meanExcessBps:mean(excess.map(r=>r.excess)),uncertainty:uncertainty(rows),excessUncertainty:uncertainty(excess,'excess')};}
function group(rows,key){const map=new Map();for(const r of rows){const k=String(key(r)??'unknown');if(!map.has(k))map.set(k,[]);map.get(k).push(r);}return [...map].sort(([a],[b])=>a.localeCompare(b)).map(([key,rows])=>({key,...summarize(rows),supportingRunIds:rows.slice(0,8).map(r=>r.runId)}));}
function analyze(rows,baseVersion='baseline'){
 const completed=rows.filter(r=>r.status==='complete'&&finite(r.returnBps)),eligible=completed.filter(r=>r.eligible),diagnostic=completed.filter(r=>!r.eligible);
 const matched=new Map();for(const r of eligible){const w=windowOf(r);if(!w)continue;const key=JSON.stringify([w,r.companyRange||r.cohort?.split('|')[2],r.execution,r.evaluationMode||'unclassified']);if(!matched.has(key))matched.set(key,[]);matched.get(key).push(r);}
 const comparisons=new Map();for(const rs of matched.values()){const base=rs.filter(r=>(r.strategyVersionId||'baseline')===baseVersion);if(!base.length)continue;for(const version of new Set(rs.map(r=>r.strategyVersionId||'baseline'))){if(version===baseVersion)continue;const other=rs.filter(r=>(r.strategyVersionId||'baseline')===version);const delta={...other[0],returnBps:mean(other.map(r=>r.returnBps))-mean(base.map(r=>r.returnBps))};if(!comparisons.has(version))comparisons.set(version,[]);comparisons.get(version).push(delta);}}
 const positions=eligible.flatMap(r=>(r.positions||[]).filter(p=>finite(p.returnBps)).map(p=>({...r,...p,runId:r.runId,date:r.date,sessions:r.sessions,benchmarkReturnBps:null,maxDrawdownBps:null})));
 return {version:'learning-statistics.v1',eligible:summarize(eligible),diagnostic:summarize(diagnostic),cohorts:group(eligible,r=>r.cohort),byCompanyRange:group(eligible,r=>r.companyRange||r.cohort?.split('|')[2]),byHoldingSessions:group(positions,r=>r.holdingSessions),byConviction:group(positions,r=>r.conviction),byExitReason:group(positions,r=>r.exitReason),byStrategy:group(eligible,r=>r.strategyVersionId||'baseline'),matchedStrategies:[...comparisons].map(([version,rs])=>({version,baseline:baseVersion,matchedWindows:rs.length,...summarize(rs)})),missing:{dates:eligible.filter(r=>!windowOf(r)).length,benchmark:eligible.filter(r=>!finite(r.benchmarkReturnBps)).length,positions:eligible.filter(r=>!r.positions?.length).length,research:eligible.filter(r=>!r.preparedDocuments?.length).length},notes:['All views are exploratory; bucket differences are confounded, not causal effects.','Overlapping session windows form connected blocks; contiguous overlapping runs can become one block.','Matched strategies compare mean returns on identical observed windows, company ranges and execution/evaluation modes; they are not randomized experiments.','Position views are unweighted position returns, not portfolio returns. Missing benchmarks are not zero.','Completed losing and zero-return runs are retained. Failed operational runs are excluded; survivor bias remains possible.','No Sharpe significance, DSR, PBO, p-values or multiple-testing-adjusted claims: a full trial history and suitable return matrix are unavailable.','Any proposed change requires future manually started out-of-sample evaluation; this dataset is training evidence.']};
}
function packet(rows,baseVersion){
 const all=rows.filter(r=>r.status==='complete'&&finite(r.returnBps)).sort((a,b)=>String(a.runId).localeCompare(String(b.runId))),stats=analyze(all,baseVersion),ruleMap=new Map(),sources=new Map();
 const records=all.map(r=>{const ruleId=S.hash(r.strategyRules||{}).slice(0,12);ruleMap.set(ruleId,r.strategyRules||null);return {id:r.runId,date:r.date,window:windowOf(r),eligible:!!r.eligible,cohort:r.cohort,ruleId,returnBps:r.returnBps,benchmarkBps:r.benchmarkReturnBps??null,drawdownBps:r.maxDrawdownBps??null,noEntry:r.noEntryReason||null,positions:(r.positions||[]).map(p=>[p.symbol,p.returnBps??null,p.allocationUsd??null,p.holdingSessions??null,p.heldMs??null,p.conviction??null,p.stopLossBps??null,p.takeProfitBps??null,p.exitReason??null,p.exitBenefitMinor??null]),rationale:(r.positions||[]).map(p=>[p.symbol,...['outlook','sizingReason','holdingReason'].map(k=>String(p[k]||'').slice(0,180))]),gaps:r.gaps||[]};});
 // Reuse saved evidence, deduplicated across outcomes; never download per case.
 for(const r of all)for(const d of r.preparedDocuments||[]){const value=JSON.stringify(d),id=S.hash(d);if(!sources.has(id))sources.set(id,{id,excerpt:value.slice(0,1600),runIds:[]});const s=sources.get(id);if(s.runIds.length<8)s.runIds.push(r.runId);}
 const out={statistics:stats,coverage:{total:all.length,mode:'all compact outcome rows',omittedNarrative:false,savedDocumentCount:sources.size},positionColumns:['symbol','returnBps','allocationUsd','holdingSessions','heldMs','conviction','stopLossBps','takeProfitBps','exitReason','exitBenefitMinor'],records,rules:Object.fromEntries(ruleMap),savedEvidence:[...sources.values()].slice(0,24)};
 const size=()=>Buffer.byteLength(JSON.stringify(out));
 if(size()>160000){out.records=records.map(({rationale,...r})=>r);out.coverage.omittedNarrative=true;}
 if(size()>160000){out.savedEvidence=[];out.coverage.omittedNarrative=true;out.records=out.records.map(({positions,gaps,...r})=>r);out.coverage.mode='all outcome rows; position detail summarized in statistics';}
 if(size()>160000){out.records=[];out.rules={};out.coverage.mode='all outcomes aggregated; individual rows exceed packet size';out.coverage.omittedNarrative=true;}
 // High-cardinality cohort statistics remain complete locally. Bound the AI
 // packet explicitly, preserving all-outcome overall statistics and coverage.
 if(size()>160000){for(const k of ['cohorts','byStrategy','matchedStrategies']){out.statistics[k]=out.statistics[k].slice(0,40);}out.coverage.groupTablesLimited=true;}
 out.coverage.savedDocumentsIncluded=out.savedEvidence.length;out.coverage.packetBytes=size();return out;
}
module.exports={analyze,packet,blocks,uncertainty,summarize};
