const assert=require('node:assert/strict'),S=require('../../charm-nest-solver.js');
const sheet={wPt:100,hPt:50,insetPt:1};
const layout=(extent,contact)=>({placements:[{id:'a',xPt:1,yPt:1,wPt:extent-1,hPt:48}],contactQuality:contact,density:.3});
// The previous epsilon comparison could walk backwards in 0.1 mm steps.
const a=layout(30,1),b=layout(30.1,2),c=layout(30.2,3);
assert(!S.betterLayout(b,a,sheet));assert(!S.betterLayout(c,b,sheet));assert(S.betterLayout(a,c,sheet));
assert(S.betterLayout(layout(30,2),a,sheet),'contact breaks exact offcut ties');
const piece=(id,w,h)=>({id,order:id,orderDate:+id+1,w,h,scale:1,bits:new Uint8Array(w*h).fill(1),areaPt2:w*h});
const job={sheet,clearancePt:0,angles:[0,90],fineRes:1,coarseRes:1,maxFill:.74,timeBudgetMs:2500,maxTrials:1,seed:1,pieces:[piece('0',30,10),piece('1',30,10)]};
(async()=>{
 const r=await S.solve(job,{yield:()=>Promise.resolve()});
 assert.equal(r.placements.length,2);assert(S.verify(job,r.placements,2).ok);
 const incumbent=r.placements.map(p=>({...p}));
 let retained=false;
 const next=await S.solve({...job,seed:938,initialLayout:incumbent},{yield:()=>Promise.resolve(),onBest:b=>{retained ||= !!b.retainedInitial;}});
 assert(retained,'an existing legal layout is considered before new proposals');
 assert(!S.betterLayout(r,next,sheet),'re-nesting cannot discard its better incumbent');
 assert(S.verify(job,next.placements,2).ok);
 for(const initialLayout of [
  [incumbent[0],incumbent[0]], // duplicate id
  [{...incumbent[0],cxPt:-50}], // outside
  [{...incumbent[1]}], // bypasses older order
  [{...incumbent[0],id:'unknown'}],
  [{...incumbent[0],angle:NaN}]
 ]){
  let accepted=false;await S.solve({...job,initialLayout},{yield:()=>Promise.resolve(),onBest:b=>{accepted ||= !!b.retainedInitial;}});
  assert(!accepted,'invalid/stale incumbent must be discarded');
 }
 // A pin changed after the previous run invalidates that previous placement.
 let accepted=false;
 await S.solve({...job,pieces:[{...job.pieces[0],pinned:{cxPt:70,cyPt:20,angle:0}},job.pieces[1]],initialLayout:incumbent},{yield:()=>Promise.resolve(),onBest:b=>{accepted ||= !!b.retainedInitial;}});
 assert(!accepted);
 console.log('Packing quality OK: monotonic ranking, incumbent retention, current geometry, FIFO, pins and malformed layouts');
})().catch(e=>{console.error(e);process.exitCode=1;});

// Saved four/twelve-angle settings must not override the production 2° default.
{
 const fs=require('node:fs'),vm=require('node:vm');
 const html=fs.readFileSync(require('node:path').join(__dirname,'../../charm-nest-1.html'),'utf8');
 const a=html.indexOf('function loadSettings()'),b=html.indexOf('function saveSettings()',a);
 for(const step of [90,45,30,15,10,5]){
  const c=vm.createContext({DEFAULTS:{stock:{},angleStep:2},localStorage:{getItem:()=>JSON.stringify({v:18,angleStep:step})}});
  vm.runInContext(html.slice(a,b),c);assert.equal(vm.runInContext('loadSettings().angleStep',c),2);
 }
 const c=vm.createContext({S:{settings:{angleStep:90}}});
 const a2=html.indexOf('function angleSet()'),b2=html.indexOf('\n',a2);
 vm.runInContext(html.slice(a2,b2),c);assert.equal(vm.runInContext('angleSet().length',c),180);
 assert.equal(vm.runInContext('angleSet().join(",")',c),Array.from({length:180},(_,i)=>i*2).join(','));
}
{
 const fs=require('node:fs'),vm=require('node:vm');
 const html=fs.readFileSync(require('node:path').join(__dirname,'../../charm-nest-1.html'),'utf8');
 const a=html.indexOf('function parallelCount()'),b=html.indexOf('function ensureWorker(',a);
 for(const [cores,want,expected] of [[8,0,1],[16,0,3],[2,0,1],[8,2,2]]){
  const c=vm.createContext({S:{settings:{parallel:want}},navigator:{hardwareConcurrency:cores},angleSet:()=>Array(180)});
  vm.runInContext(html.slice(a,b),c);assert.equal(vm.runInContext('parallelCount()',c),expected);
 }
}
