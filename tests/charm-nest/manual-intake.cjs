const assert=require('node:assert/strict'),fs=require('node:fs'),vm=require('node:vm');
const O=require('../../charm-nest-orders');
const html=fs.readFileSync('charm-nest-1.html','utf8'),bridge=fs.readFileSync('charm-nest-bridge.js','utf8');
function setup(){
 const old={id:'old',areaPt2:20},placement={id:'old',cxPt:10,cyPt:10,angle:0};
 const sheet={metal:'gold',page:1,runId:'existing',charms:[old],placements:[placement],status:'complete',intakeOptimized:true,intakeOptimizedCount:1,density:.5,trials:23,jobId:'active-job',best:{placements:[placement]},outputs:{ai:'saved'},persistedDone:true};
 const pages=[sheet],starts=[];
 sheet.pages=pages;
 const c={window:{},S:{sources:[],unassigned:[],settings:{autoNest:'on',maxFill:.8,finalOptimizeCount:85,budgetS:180},sheets:{gold:sheet}},METALS:[{key:'gold'}],uid:()=> 'new-run',activePage:()=>pages[sheet.active||0],pagesOf:()=>pages,allSheets:()=>pages,activeCharms:p=>p.charms.filter(x=>!x.excluded),O,Sets:{ofRun:()=>[]},usableArea:()=>100,inflatedArea:p=>p.areaPt2||1,
  addPage:()=>{const p={metal:'gold',page:pages.length+1,runId:sheet.runId,charms:[],placements:[],status:'idle'};pages.push(p);return p;},showPage:(m,i)=>{sheet.active=i;},computeSaturation(){},renderCard(){},renderRail(){},updateTopSub(){},toast(){},startNest:p=>{starts.push(p);p.status='nesting';}};
 vm.createContext(c);
 vm.runInContext(html.slice(html.indexOf('function assignSource('),html.indexOf('function removeSource(')),c);
 const a=bridge.indexOf('const LiveNest ='),b=bridge.indexOf('  async function add(run)',a);
 vm.runInContext(bridge.slice(a,b)+'return {prepareSheet,closed};})();',c);
 function source(id,count=1){const s={id,runId:'drop-'+id,metal:'gold',state:'ready',charms:Array.from({length:count},(_,i)=>({id:id+':'+i,sourceId:id,areaPt2:1}))};c.S.sources.push(s);return s;}
 return {c,sheet,pages,starts,source,live:c.window.LiveNest};
}
{
 const {c,sheet,starts,source,live}=setup(),before=JSON.stringify(sheet.placements),best=sheet.best;
 c.assignSource(source('incoming',3),'gold');assert.equal(sheet.charms.length,4);assert.equal(JSON.stringify(sheet.placements),before);assert.equal(sheet.best,best);assert.equal(sheet.trials,23);assert.equal(sheet.runId,'existing');assert.equal(starts.length,1);
 assert.equal(live.prepareSheet(sheet).phase,'fill');assert(sheet.charms[0].arrivalPin);assert.deepEqual({...sheet.charms[0].pinned},{cxPt:10,cyPt:10,angle:0});
}
{
 const {c,sheet,starts,source}=setup();sheet.status='nesting';const initial={placements:sheet.placements,best:sheet.best,jobId:sheet.jobId,trials:sheet.trials};
 const a=source('a',2),b=source('b');c.assignSource(a,'gold');c.assignSource(b,'gold');
 assert.equal(sheet.charms.length,1);assert.equal(starts.length,0);assert.equal(sheet.placements,initial.placements);assert.equal(sheet.best,initial.best);assert.equal(sheet.jobId,initial.jobId);assert.equal(sheet.trials,initial.trials);
 sheet.status='finishing';c.flushManualIntake();assert.equal(sheet.charms.length,1);
 sheet.status='complete';sheet.persisted=Promise.resolve();sheet.persistedDone=false;c.flushManualIntake();assert.equal(sheet.charms.length,1,'wait for the existing output save');
 sheet.persistedDone=true;c.flushManualIntake();assert.equal(sheet.charms.length,4);assert.equal(starts.length,1);
 c.assignSource(a,'gold');assert.equal(sheet.charms.length,4,'an assigned source cannot be inserted twice');assert.equal(starts.length,1);
}
for(const lock of ['intakeFinalized','releaseFull','roseCutAt','recalled','runHold']){
 const {c,sheet,pages,source}=setup();sheet[lock]=true;c.assignSource(source(lock),'gold');assert.equal(pages.length,2,lock);assert.equal(sheet.charms.length,1);assert.equal(sheet.placements[0].id,'old');assert.equal(pages[1].charms.length,1);
}
{
 const {c,sheet,source,starts}=setup(),s=source('review');s.review={state:'running'};c.assignSource(s,'gold');assert.equal(sheet.charms.length,1);s.review.state='done';c.flushManualIntake();assert.equal(sheet.charms.length,2);assert.equal(starts.length,1);
}
{
 const {c,sheet,source,starts}=setup(),s=source('single');c.S.settings.autoNest='off';c.S.unassigned.push(s.charms[0]);c.assignCharm(s.charms[0],'gold');assert.equal(sheet.charms.length,2);assert.equal(sheet.placements.length,1);assert.equal(starts.length,0);
}
for(const count of [84,85,86,100])assert.equal(O.intakePlan({count,density:.74,force:true}).phase,'fill','74% skips automatic full searches');
assert.equal(O.intakePlan({count:85,density:.739}).phase,'final');
assert.equal(O.intakePlan({count:85,density:.5,append:true}).phase,'fill');
assert.equal(O.intakePlan({count:40,density:.5,force:true}).phase,'repack');
assert.equal(O.intakePlan({count:40,density:.5,optimized:true,area:99,capacity:100}).phase,'fill');
// Exercise the actual completion decision before PDF writing, with no API calls.
const decision=html.slice(html.indexOf('  const belowTarget=result.density'),html.indexOf('  sh._beforeLearned = null;',html.indexOf('  const belowTarget=result.density')));
function finish({count=40,density=.5,optimized=false,rejects=[],phase='fill',endedBy='complete',last=0}={}){
 const sh={intakePhase:phase,intakeOptimized:optimized,intakeOptimizedCount:last,status:'nesting',charms:[]},items=Array.from({length:count},()=>({}));let restarts=0;
 const c={sh,items,result:{density,rejects,endedBy,placements:[{id:'kept'}],placedPt2:50,usablePt2:100,freePt2:50},S:{settings:{maxFill:.8,finalOptimizeCount:85}},startNest(){restarts++;},log(){}};vm.createContext(c);vm.runInContext('(function(){'+decision+'})();',c);return {sh,restarts};
}
assert.equal(finish().restarts,1,'an initial layout below 74% gets a repack');
assert.equal(finish({phase:'repack'}).restarts,0,'the same sparse batch cannot loop forever');
assert.equal(finish({optimized:true}).restarts,0,'after repacking, new pieces fill gaps');
assert.equal(finish({count:85,optimized:true}).restarts,1,'below target at 85 gets the final attempt');
assert.equal(finish({count:85,density:.74,optimized:true,rejects:['overflow']}).restarts,0,'at target, spill without another full search');
assert.equal(finish({endedBy:'stopped'}).restarts,0,'operator stop is respected');
assert.equal(finish({phase:'final',count:85,rejects:['overflow']}).restarts,0,'final attempt never loops');
{
 const {c,sheet,pages}=setup(),next=c.addPage();next.charms=[{id:'next-existing'}];next.placements=[{id:'next-existing',cxPt:4,cyPt:5}];next.status='complete';
 const a={id:'extra-a',order:'pair',arrivalPin:true,pinned:{cxPt:1}},b={id:'extra-b',order:'pair'};sheet.charms.push(a,b);sheet.rejects=[a.id,b.id];sheet.verification={ok:true};
 Object.assign(c,{agent(){},labelOf:()=> 'Gold',orderSummary:()=>({text:'one order'})});
 vm.runInContext(html.slice(html.indexOf('function overflowToNextSheet('),html.indexOf('function inflatedArea(')),c);
 c.overflowToNextSheet(sheet);assert.equal(sheet.placements[0].id,'old');assert.equal(sheet.charms.length,1);assert.deepEqual(Array.from(next.charms,x=>x.id),['next-existing','extra-a','extra-b']);assert.equal(next.placements[0].id,'next-existing');assert.equal(a.pinned,null);assert.equal(pages.length,2);
}
(async()=>{
 const {c,sheet}=setup();let id=0;Object.assign(c,{uid:()=> 'file-'+(++id),performance,pumpParse(){},routeByName:()=>null});
 vm.runInContext(html.slice(html.indexOf('async function intake('),html.indexOf('let parsing =')),c);
 await c.intake([{name:'single.ai',type:'application/postscript',arrayBuffer:async()=>new Uint8Array([1,2]).buffer},{name:'many.ai',type:'application/postscript',arrayBuffer:async()=>new Uint8Array([3,4]).buffer}],'gold');
 assert.equal(c.S.sources.length,2);assert(c.S.sources.every(s=>s.forceNest),'explicit sheet drops are not diverted into the master catalogue');assert.equal(sheet.charms.length,1);assert.equal(sheet.placements[0].id,'old');
 await c.intake([{name:'restore.ai',arrayBuffer:async()=>new ArrayBuffer(0)}],'gold',{deferNest:true});assert(c.S.sources.at(-1).deferNest,'restore can rebuild saved pins before starting any job');
 const S=require('../../charm-nest-solver'),shape=(id,w,h,date)=>({id,w,h,scale:1,bits:new Uint8Array(w*h).fill(1),order:id,orderDate:date});
 const old={...shape('old',12,10,1),pinned:{cxPt:7,cyPt:6,angle:0}},fresh=shape('fresh',6,6,2),large=shape('large',50,50,3);
 const job={sheet:{wPt:30,hPt:20,insetPt:1},pieces:[old,fresh,large],angles:[0,90],fineRes:2,coarseRes:.5,maxFill:.8,timeBudgetMs:400,maxTrials:2};
 const result=await S.solve(job,{});assert(S.verify(job,result.placements,4).ok);assert.equal(result.placements.find(p=>p.id==='old').cxPt,7);assert(result.placements.some(p=>p.id==='fresh'));assert(result.rejects.includes('large'));
 console.log('Manual intake OK: file batches, running progress, identity, save/review waits, frozen stock, pins, real gap filling, overflow, 74%/85 decisions and bounded retries');
})().catch(e=>{console.error(e);process.exitCode=1;});
