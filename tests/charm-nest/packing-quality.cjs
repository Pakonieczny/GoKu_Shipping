const assert=require('node:assert/strict'),S=require('../../charm-nest-solver.js');
const sheet={wPt:100,hPt:50,insetPt:1};
const layout=(extent,contact)=>({placements:[{id:'a',xPt:1,yPt:1,wPt:extent-1,hPt:48}],contactQuality:contact,density:.3});
// The previous epsilon comparison could walk backwards in 0.1 mm steps.
const a=layout(30,1),b=layout(30.1,2),c=layout(30.2,3);
assert(!S.betterLayout(b,a,sheet));assert(!S.betterLayout(c,b,sheet));assert(S.betterLayout(a,c,sheet));
assert(S.betterLayout(layout(30,2),a,sheet),'contact breaks exact offcut ties');
assert(S.betterLayout(layout(98,.8),layout(97,.6),sheet),'an unusable edge sliver cannot defeat better packing on a nearly full sheet');
assert(S.betterLayout(layout(85,.4),layout(98,.8),sheet),'a useful partial-sheet offcut still takes priority');
assert(!S.betterLayout({...layout(98,10),placements:[{id:'a'}]},layout(98,.8),sheet),'missing geometry cannot outrank a valid full sheet');
const fullCandidates=[layout(99,.6),layout(98,.7),layout(97,.8),layout(96,.9)];
for(let i=0;i<fullCandidates.length;i++)for(let j=i+1;j<fullCandidates.length;j++)assert(S.betterLayout(fullCandidates[j],fullCandidates[i],sheet),'full-sheet ranking remains transitive');
const unknown={...a,contactQuality:undefined,density:.9};
assert(!S.betterLayout(unknown,a,sheet),'missing metrics cannot create a non-transitive replacement cycle');
const copy=S.publicLayout(a);a.placements[0].xPt=4;assert.equal(copy.placements[0].xPt,1);a.placements[0].xPt=1;
assert.equal(S.bestResult({...layout(50,9),endedBy:'budget'},copy,sheet).placements[0].wPt,29);
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
 // Expiry halfway through a multi-piece order must not replace a legal
 // incumbent with an incomplete order that final cleanup then removes.
 const fs=require('node:fs'), vm=require('node:vm');
 let clock=0;
 const realm=vm.createContext({module:{exports:{}},performance:{now:()=>clock}});
 vm.runInContext(fs.readFileSync(require('node:path').join(__dirname,'../../charm-nest-solver.js'),'utf8'),realm);
 const Expiring=realm.module.exports;
 const pairJob={...job,maxTrials:8,angles:[0],initialLayout:[{id:'solo',cxPt:30,cyPt:5,angle:0}],pieces:[
  {...piece('solo',2,2),order:'solo',orderDate:0},
  {...piece('pair-a',10,10),order:'pair',orderDate:0},
  {...piece('pair-b',10,10),order:'pair',orderDate:0}
 ]};
 const published=[];
 const expired=await Expiring.solve(pairJob,{yield:()=>Promise.resolve(),onPlaced:()=>{clock=10000;},onBest:b=>published.push(structuredClone(S.publicLayout(b)))});
 assert.equal(expired.endedBy,'budget');assert.equal(expired.placements.length,1);assert.equal(expired.placements[0].id,'solo');
 assert(published.every(b=>b.placements.every(p=>p.id==='solo')),'incomplete orders cannot be promoted as the best layout');
 assert(published.every(b=>!S.betterLayout(b,expired,sheet)),'the saved result cannot regress from a published valid best');
 assert(S.verify(pairJob,expired.placements,2).ok);
 console.log('Packing quality OK: monotonic ranking, incumbent retention, current geometry, FIFO, pins and malformed layouts');
})().catch(e=>{console.error(e);process.exitCode=1;});

// Migrate the released 2° settings back to the requested 10° default.
{
 const fs=require('node:fs'),vm=require('node:vm');
 const html=fs.readFileSync(require('node:path').join(__dirname,'../../charm-nest-1.html'),'utf8');
 const a=html.indexOf('function loadSettings()'),b=html.indexOf('function saveSettings()',a);
 for(const step of [90,45,30,15,10,5,2]){
  const c=vm.createContext({DEFAULTS:{stock:{},angleStep:10},localStorage:{getItem:()=>JSON.stringify({v:20,angleStep:step})}});
  vm.runInContext(html.slice(a,b),c);assert.equal(vm.runInContext('loadSettings().angleStep',c),10);
 }
 const c=vm.createContext({S:{settings:{angleStep:90}}});
 const a2=html.indexOf('function angleSet()'),b2=html.indexOf('\n',a2);
 vm.runInContext(html.slice(a2,b2),c);assert.equal(vm.runInContext('angleSet().length',c),36);
 assert.equal(vm.runInContext('angleSet().join(",")',c),Array.from({length:36},(_,i)=>i*10).join(','));
}
{
 const fs=require('node:fs'),vm=require('node:vm');
 const html=fs.readFileSync(require('node:path').join(__dirname,'../../charm-nest-1.html'),'utf8');
 const a=html.indexOf('function parallelCount()'),b=html.indexOf('function ensureWorker(',a);
 for(const [cores,want,expected] of [[8,0,1],[16,0,1],[32,0,1],[2,0,1],[8,2,2]]){
  const c=vm.createContext({S:{settings:{parallel:want}},navigator:{hardwareConcurrency:cores},angleSet:()=>Array(180)});
  vm.runInContext(html.slice(a,b),c);assert.equal(vm.runInContext('parallelCount()',c),expected);
 }
 for(const [cores,expected] of [[2,1],[8,2],[32,2]]){
  const c=vm.createContext({S:{settings:{parallel:0}},navigator:{hardwareConcurrency:cores},angleSet:()=>Array(36)});
  vm.runInContext(html.slice(a,b),c);assert.equal(vm.runInContext('parallelCount()',c),expected);
 }
}
