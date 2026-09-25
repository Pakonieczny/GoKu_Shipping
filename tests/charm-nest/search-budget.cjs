const assert=require('node:assert/strict'),fs=require('node:fs'),vm=require('node:vm');
const html=fs.readFileSync('charm-nest-1.html','utf8');
const settings={budgetS:180,maxFill:.8,seed:1};
const ctx=vm.createContext({S:{settings},stockFor:()=>({wPt:30,hPt:20}),activeCharms:s=>s.charms,angleSet:()=>[0]});
vm.runInContext(html.slice(html.indexOf('/* A big batch goes onto a sheet'),html.indexOf('/* A stopped run starts none')),ctx);
vm.runInContext(html.slice(html.indexOf('const CAREFUL_ANGLES'),html.indexOf('function packingKey(')),ctx);
const piece={id:'one',w:4,h:4,scale:1,bits:new Uint8Array(16).fill(1)};
for(const phase of ['fill','repack','final']){
 const job=ctx.buildJob({intakePhase:phase,intakeBudgetMs:phase==='fill'?12000:180000,charms:[piece]});
 assert.equal(job.timeBudgetMs,phase==='fill'?12000:180000);
 assert.equal(job.fullBudget,phase!=='fill');
}
const nodes={prog:{hidden:false,parentElement:{classList:{contains:()=>false}}},stage:{},overlay:{}};
Object.assign(ctx,{$:s=>nodes[/data-r="([^"]+)"/.exec(s)[1]],performance:{now:()=>5000},fmt:{s:n=>Math.round(n/1000)+'s',pct:n=>Math.round(n*100)+'%'},esc:s=>s});
ctx.S.sources=[];
vm.runInContext(html.slice(html.indexOf('function renderProgress('),html.indexOf('function renderSat(')),ctx);
ctx.usableArea=()=>100;vm.runInContext(html.slice(html.indexOf('function carefulSoFar(sh) {'),html.indexOf('/** Inner cut lines of a charm')),ctx);
const sheet={el:{},status:'nesting',startedAt:0,intakeBudgetMs:12000,stage:'Searching',charms:[piece],placements:[],trials:3,searchMetrics:{positions:50,gpuPositions:10},gpuProgress:{active:true,reason:'GPU enabled'}};
// placed one charm at a time, the card shows the time spent and no ceiling (the solver's only guards against a runaway)
ctx.renderProgress(sheet);assert(nodes.stage.innerHTML.includes('5s'));assert(!nodes.stage.innerHTML.includes('/ 12s'));assert(nodes.stage.innerHTML.includes('GPU + CPU'));
// the learned search still shows its ceiling
sheet.nestFlow='learned';ctx.S.settings.learnedBudgetS=600;ctx.renderProgress(sheet);assert(nodes.stage.innerHTML.includes('/ 600s ceiling'));delete sheet.nestFlow;
sheet.gpuProgress={active:false,reason:'CPU fallback: GPU device lost',phase:'fallback'};ctx.renderProgress(sheet);
assert(nodes.stage.innerHTML.includes('GPU unavailable'));assert(nodes.stage.title.includes('GPU device lost'));

async function solve(fullBudget,stopAt=Infinity){
 let clock=0,completed=0;
 const context=vm.createContext({module:{exports:{}},require:()=>require('../../charm-nest-rose'),performance:{now:()=>clock},setTimeout,clearTimeout});
 vm.runInContext(fs.readFileSync('charm-nest-solver.js','utf8'),context);
 const S=context.module.exports;
 const job={sheet:{wPt:30,hPt:20,insetPt:1},pieces:[piece],angles:[0,90],fineRes:2,coarseRes:.5,timeBudgetMs:180000,stallMs:60000,maxTrials:1000000,maxFill:.8,fullBudget};
 const result=await S.solve(job,{yield:async()=>{clock+=1000;},shouldStop:()=>clock>=stopAt,onTrial:t=>{if(t.completed)completed++;}});
 assert.equal(result.trials,completed,'only completed construction passes count');
 assert(S.verify(job,result.placements,4).ok);
 return result;
}
(async()=>{
 const brief=await solve(false),full=await solve(true),cancelled=await solve(true,10000);
 assert(brief.elapsedMs<170000,'legacy bounded search may stop early');
 assert(full.elapsedMs>=170000,'full optimization uses the search budget before final refinement');
 assert(full.trials>brief.trials,'a sparse fully seated sheet continues trying layouts');
 assert(cancelled.elapsedMs<170000,'Stop still interrupts full-budget work');
 console.log(`Search budget OK: time shown without a ceiling one at a time and with the learned ceiling, GPU diagnostics, full optimization ${full.elapsedMs/1000}s vs early finish ${brief.elapsedMs/1000}s on a controlled clock, accurate trials and cancellation`);
})().catch(e=>{console.error(e);process.exitCode=1;});
