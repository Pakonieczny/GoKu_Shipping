const assert=require('node:assert/strict'),fs=require('node:fs'),vm=require('node:vm');
const S=require('../../charm-nest-solver');
const p={id:'probe',w:12,h:10,scale:1,bits:new Uint8Array(120).fill(1)};
const fine=S.makeSheetGrid({wPt:40,hPt:30,insetPt:1},0,2).trackMaterial(true);fine.buildSAT();
const coarse=S.coarseFromFine(fine,4);coarse.trackMaterial();coarse.buildSAT();
const variants=[0,90].map(angle=>{const v=S.prepareVariant(p,angle,0,2),c=S.resample(v.fine.bits,v.fine.w,v.fine.h,2,.5),r=S.ring(v.fine.bits,v.fine.w,v.fine.h,4);return {...v,coarse:{...c,pm:S.packShifted(c.bits,c.w,c.h)},ringFine:S.packShifted(r.bits,r.w,r.h),ringPad:4};});
let fineChecks=0,coarseChecks=0;const fits=fine.fits,box=coarse.boxSum;
fine.fits=function(...args){fineChecks++;return fits.apply(this,args);};
coarse.boxSum=function(...args){coarseChecks++;return box.apply(this,args);};
const metrics={positions:0};S.search({variants},fine,coarse,4,.35,0,S.rng(1),0,0,null,false,null,null,metrics);
assert.equal(metrics.positions,coarseChecks+fineChecks,'counter matches actual coarse rejections and fine fits, including wall passes');

const html=fs.readFileSync('charm-nest-1.html','utf8'),start=html.indexOf('function updateSearchMetrics('),end=html.indexOf('async function startServerNest(',start);
let renders=0;const ctx=vm.createContext({renderProgress(){renders++;},log(){},activeCharms:()=>[],CharmNestSolver:S});
vm.runInContext(html.slice(start,end),ctx);
const sh={jobId:'current',status:'nesting',pool:{n:2,trials:[0,0]},trials:0};
const send=(idx,values,id='current')=>ctx.onWorkerMessage(sh,{jobId:id+':'+idx,type:'metrics',metrics:values},idx);
send(0,{layouts:2,positions:120,gpuPositions:0,searches:5});send(1,{layouts:3,positions:200,gpuPositions:160,searches:8});
assert.equal(sh.trials,5);assert.equal(sh.searchMetrics.positions,320);assert.equal(sh.searchMetrics.gpuPositions,160);
send(1,{layouts:1,positions:100});send(0,{layouts:2,positions:120});
assert.equal(sh.trials,5);assert.equal(sh.searchMetrics.positions,320,'duplicates and late lower snapshots cannot inflate or lower counts');
send(0,{layouts:999,positions:99999},'old');assert.equal(sh.trials,5,'stale job ignored');
sh.pool.completed=[true,false];send(0,{layouts:999,positions:99999});assert.equal(sh.trials,5,'completed worker ignored');
ctx.updateSearchMetrics(sh,1,{layouts:4,positions:260,gpuPositions:190,searches:9});
assert.equal(sh.trials,6);assert.equal(sh.searchMetrics.positions,380,'final unreported work is merged once');
sh.pool.finished=true;send(1,{layouts:999,positions:99999});assert.equal(sh.trials,6);
assert(renders>0);

(async()=>{
 const updates=[],trials=[];
 const job={sheet:{wPt:60,hPt:40,insetPt:1},pieces:[p,{...p,id:'second'}],angles:[0,90],fineRes:2,coarseRes:.5,timeBudgetMs:1500,maxTrials:2,maxFill:.8};
 const result=await S.solve(job,{onMetrics:m=>updates.push(m),onTrial:m=>trials.push(m)});
 assert.equal(result.trials,trials.filter(t=>t.completed).length);assert.deepEqual(result.metrics,updates.at(-1));
 for(let i=1;i<updates.length;i++)for(const key of Object.keys(result.metrics))assert(updates[i][key]>=updates[i-1][key],key);
 assert(result.metrics.searches>=job.pieces.length*result.trials);assert.equal(result.metrics.gpuPositions,0);
 let halt=false;const interrupted=await S.solve(job,{onPlaced:()=>{halt=true;},shouldStop:()=>halt});assert.equal(interrupted.trials,0,'interrupted construction is not a completed layout trial');
 const stopped=await S.solve(job,{shouldStop:()=>true});assert.equal(stopped.trials,0);assert.equal(stopped.metrics.positions,0);
 console.log('Search metrics OK: observed checks, completed trials, monotonic multiworker totals, final merge, stale/duplicate filtering and cancellation');
})().catch(e=>{console.error(e);process.exitCode=1;});
