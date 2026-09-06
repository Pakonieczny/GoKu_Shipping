'use strict';
// Descriptive, reproducible summaries. No model calls or strategy promotion.
const Money=require('./_investorMoney');
const VERSION='simulation-outcomes.v2';
function progress(run,work=run.work||{}) {
  if(run.status==='complete')return 100;
  const ranges={screening_profiles:[0,15],shortlist:[15,25],load_research:[25,35],account:[35,36],manager_freeze:[36,38],manager_review:[38,50],manager_coverage:[50,52],manager_document:[52,65],manager_decision:[65,78],manager_validation:[78,80],manager_complete:[80,80],replay:[80,99],finalize:[99,99]};
  const range=ranges[work.stage]||[0,0],fraction=work.total>0?Math.max(0,Math.min(1,Number(work.done||0)/work.total)):0;
  return Math.min(99,Math.max(run.overallProgress||0,run.managerDone?80+.19*Math.min(100,run.progress||0):run.shortlistRef?25:0,range[0]+(range[1]-range[0])*fraction));
}
function analyze({run,plan,prices,fills=[],closeMs}) {
  const investments=(run.investments||[]).map(x=>{
    const p=plan?.investments?.[x.symbol],bars=prices?.symbols?.[x.symbol]?.bars||[];
    const final=bars.find(b=>Date.parse(b.t)+300000===closeMs);
    const qty=Number(x.boughtShares),entryEvents=fills.filter(f=>f.symbol===x.symbol&&String(f.side).toLowerCase()==='buy');
    // Fixed alternative: same recorded entry and quantity, ignore exits, sell at
    // the final observed close with the same spread and fee convention.
    const usable=final&&!final.halted&&Number(final.c)>0&&Number(final.v)*.1>=qty&&entryEvents.length===1;
    const closeMicros=usable?BigInt(Math.round(final.c*1e6)):0n;
    const bid=closeMicros-closeMicros*10n/20000n;
    const quantity=BigInt(qty),fee=(quantity*5000n+9999n)/10000n;
    const heldToCloseMinor=usable?Number(Money.divRound(quantity*bid,10000n,Money.ROUNDING.HALF_EVEN)-fee)-x.investedMinor:null;
    const exits=fills.filter(f=>f.symbol===x.symbol&&String(f.side).toLowerCase()==='sell');
    return {symbol:x.symbol,planHash:plan?.planHash||null,documentHash:plan?.documentHashes?.[x.symbol]||null,
      holdingSessions:p?.holdingSessions||1,holdingReason:p?.holdingReason||null,horizonAnalysis:p?.horizonAnalysis||null,conviction:p?.conviction||'UNKNOWN',allocationUsd:p?.allocationUsd??null,sizingReason:p?.sizingReason||null,outlook:p?.outlook||null,
      takeProfitBps:p?.takeProfitBps??null,stopLossBps:p?.stopLossBps??null,evidenceIds:p?.evidenceIds||[],
      investedMinor:x.investedMinor,shares:qty,entryPriceMicros:x.entryPriceMicros,exitPriceMicros:x.exitPriceMicros,
      entryAtMs:x.entryAtMs,exitAtMs:x.exitAtMs,heldMs:x.heldMs,status:x.status,pnlMinor:x.pnlMinor,
      returnBps:x.returnBps,exitReason:exits.at(-1)?.role||(x.status==='OPEN'?'OPEN_AT_CLOSE':'UNKNOWN'),
      heldToCloseMinor,exitBenefitMinor:heldToCloseMinor==null||x.pnlMinor==null?null:x.pnlMinor-heldToCloseMinor,
      comparisonUnavailable:usable?null:'Requires one recorded entry and a valid, sufficiently liquid final five-minute bar.'};
  });
  return {version:VERSION,runId:run.runId,batchId:run.batchId,date:run.date,policyVersion:plan?.policy?.version||null,planHash:plan?.planHash||null,
    pnlMinor:run.pnlMinor||0,aiCostNano:run.spentNano||0,maxDrawdownBps:run.maxDrawdownBps||0,benchmarkReturnBps:run.benchmarkReturnBps??null,
    investments,comparison:'Same entry and shares; remove both stop and target; exit at observed first-session close with 10 bps spread and $0.005/share fee. Hindsight comparison, not a recommended rule.'};
}
function summarize(runs) {
  const completed=runs.filter(r=>r.status==='complete'),sum=(a,f)=>a.reduce((n,x)=>n+f(x),0);
  const positions=completed.flatMap(r=>(r.outcomeAnalysis?.investments||r.investments||[]).map(x=>({...x,date:r.date,runId:r.runId})));
  const closed=positions.filter(x=>x.status==='CLOSED'&&x.pnlMinor!=null),wins=completed.filter(r=>r.pnlMinor>0).length,losses=completed.filter(r=>r.pnlMinor<0).length;
  const gains=sum(closed,x=>Math.max(0,x.pnlMinor)),loss=sum(closed,x=>Math.max(0,-x.pnlMinor));
  const compared=positions.filter(x=>x.exitBenefitMinor!=null&&x.status==='CLOSED');
  return {completed:completed.length,total:runs.length,wins,losses,flat:completed.length-wins-losses,failed:runs.filter(r=>['incomplete','unavailable','cancelled'].includes(r.status)).length,
    pnlMinor:sum(completed,r=>r.pnlMinor||0),aiCostNano:sum(runs,r=>r.spentNano||0),completedAiCostNano:sum(completed,r=>r.spentNano||0),
    tradeWins:closed.filter(x=>x.pnlMinor>0).length,tradeLosses:closed.filter(x=>x.pnlMinor<0).length,tradeFlat:closed.filter(x=>x.pnlMinor===0).length,
    open:positions.filter(x=>x.status==='OPEN').length,profitFactor:loss?gains/loss:null,
    avgWinMinor:wins?sum(completed.filter(r=>r.pnlMinor>0),r=>r.pnlMinor)/wins:null,avgLossMinor:losses?sum(completed.filter(r=>r.pnlMinor<0),r=>r.pnlMinor)/losses:null,
    worstDrawdownBps:completed.length?Math.max(...completed.map(r=>r.maxDrawdownBps||0)):null,
    compared:compared.length,exitBenefitMinor:sum(compared,x=>x.exitBenefitMinor),positions,
    convictions:['LOW','MEDIUM','HIGH','UNKNOWN'].map(c=>{const a=positions.filter(x=>(x.conviction||'UNKNOWN')===c&&x.pnlMinor!=null);const invested=sum(a,x=>x.investedMinor);return {conviction:c,count:a.length,pnlMinor:sum(a,x=>x.pnlMinor),investedMinor:invested,returnBps:invested?10000*sum(a,x=>x.pnlMinor)/invested:null};}).filter(x=>x.count)};
}
module.exports={VERSION,progress,analyze,summarize};
