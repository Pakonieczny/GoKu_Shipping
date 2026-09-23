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
{
 const {c,sheet,pages,source,live}=setup();sheet.metal='rose';c.METALS[0].key='rose';c.S.sheets.rose=sheet;
 sheet.rosePlan={profile:{},shapes:[{id:'old'}],lines:[[[0,0],[1,1]]]};sheet.runHold=true;sheet.intakeFinalized=true;
 c.window.RoseStock={protect:p=>{p.roseProtected={placements:p.placements.map(x=>({...x})),profile:p.rosePlan.profile};}};
 const before=JSON.stringify(sheet.placements),src=source('rose-drop',3);src.metal='rose';c.assignSource(src,'rose');
 assert.equal(pages.length,1,'planned contour continues on the existing page even when marked held');assert.equal(sheet.charms.length,4);assert.equal(JSON.stringify(sheet.placements),before);
 sheet.intakeForceFinal=true;assert.equal(live.prepareSheet(sheet).phase,'fill','protected contours never trigger a full repack');assert(!live.closed(sheet),'automatic intake can keep using an uncut protected sheet');
}
for(const metal of ['gold','silver','gold10k','gold14k','rose']){
 const {c,sheet,pages,source,starts}=setup();sheet.metal=metal;c.METALS[0].key=metal;c.S.sheets[metal]=sheet;
 const second=c.addPage();second.metal=metal;second.charms=[{id:'second'}];second.placements=[{id:'second',cxPt:4,cyPt:5,angle:0}];second.persistedDone=true;second.status='complete';
 const first=JSON.stringify(sheet.placements),src=source('latest');src.metal=metal;c.assignSource(src,metal);
 assert.equal(starts[0],second,metal+' always chooses newest partial even while the first page is selected');assert.equal(second.charms.length,2);assert.equal(JSON.stringify(sheet.placements),first);assert(second.appendOnly);
 second.status='complete';second.releaseFull=true;const next=source('full');next.metal=metal;c.assignSource(next,metal);
 assert.equal(pages.length,3,metal+' creates a new page when the newest is full');assert.equal(sheet.charms.length,1);assert.equal(second.charms.length,2);
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
{
 const {c,sheet,source}=setup(),s=source('saved');sheet.charms[0].sourceId=s.id;sheet.metal='rose';sheet.roseProtected={placements:[{...sheet.placements[0]}]};
 const old=JSON.stringify(sheet.placements),warnings=[];c.toast=m=>warnings.push(m);
 vm.runInContext(html.slice(html.indexOf('function removeSource('),html.indexOf('async function persistSource(')),c);
 c.assignCharm(sheet.charms[0],'silver');c.removeSource(s);
 assert.equal(JSON.stringify(sheet.placements),old);assert.equal(sheet.charms.length,1);assert(c.S.sources.includes(s));assert.equal(warnings.length,2,'moving or deleting a placed charm is refused before changing the local sheet');
}
for(const count of [84,85,86,100])assert.equal(O.intakePlan({count,density:.74,force:true}).phase,'fill','74% skips automatic full searches');
assert.equal(O.intakePlan({count:85,density:.739}).phase,'final');
assert.equal(O.intakePlan({count:85,density:.5,append:true}).phase,'fill');
assert.equal(O.intakePlan({count:40,density:.5,force:true}).phase,'repack');
assert.equal(O.intakePlan({count:40,density:.5,optimized:true,area:99,capacity:100}).phase,'fill');
// Exercise the actual completion decision before PDF writing, with no API calls.
const decision=html.slice(html.indexOf('  const belowTarget=result.density'),html.indexOf('  sh._beforeLearned = null;',html.indexOf('  const belowTarget=result.density')));
function finish({count=40,density=.5,optimized=false,rejects=[],phase='fill',endedBy='complete',last=0,protectedRose=false,metal='gold',appendOnly=false}={}){
 const sh={metal,appendOnly,roseProtected:protectedRose,intakePhase:phase,intakeOptimized:optimized,intakeOptimizedCount:last,status:'nesting',charms:[]},items=Array.from({length:count},()=>({}));let restarts=0;
 const c={sh,items,result:{density,rejects,endedBy,placements:[{id:'kept'}],placedPt2:50,usablePt2:100,freePt2:50},S:{settings:{maxFill:.8,finalOptimizeCount:85}},startNest(){restarts++;},log(){}};vm.createContext(c);vm.runInContext('(function(){'+decision+'})();',c);return {sh,restarts};
}
assert.equal(finish({protectedRose:true,count:100}).restarts,0,'protected remainder never restarts under 74% or over 85 charms');
assert.equal(finish().restarts,1,'an initial layout below 74% gets a repack');
assert.equal(finish({phase:'repack'}).restarts,0,'the same sparse batch cannot loop forever');
assert.equal(finish({optimized:true}).restarts,0,'after repacking, new pieces fill gaps');
assert.equal(finish({count:85,optimized:true}).restarts,1,'below target at 85 gets the final attempt');
assert.equal(finish({count:85,density:.74,optimized:true,rejects:['overflow']}).restarts,0,'at target, spill without another full search');
assert.equal(finish({endedBy:'stopped'}).restarts,0,'operator stop is respected');
assert.equal(finish({phase:'final',count:85,rejects:['overflow']}).restarts,0,'final attempt never loops');
// an arrival that misses the gaps of an appended Gold or Silver sheet below 74% gets one fresh arrangement before it moves on
for(const metal of ['gold','silver']){
 const missed=finish({metal,appendOnly:true,optimized:true,rejects:['arrival'],endedBy:'budget'});
 assert.equal(missed.restarts,1,metal+': an append that overflows below the target repacks the sheet first');
 assert.equal(missed.sh.appendOnly,undefined,'with every piece free to move');assert.equal(missed.sh.intakeForceFinal,true);
}
assert.equal(finish({appendOnly:true,optimized:true}).restarts,0,'an append that fits keeps the saved layout');
assert.equal(finish({appendOnly:true,optimized:true,rejects:['arrival'],endedBy:'trials'}).restarts,1,'a gap search that used up its trials still gets the fresh arrangement: the saved gaps do not show how full the sheet can get');
assert.equal(finish({appendOnly:true,optimized:true,density:.74,rejects:['arrival']}).restarts,0,'at the target the sheet is full, not repacked');
assert.equal(finish({appendOnly:true,optimized:true,rejects:['arrival'],endedBy:'stopped'}).restarts,0,'operator stop is respected');
assert.equal(finish({metal:'rose',appendOnly:true,optimized:true,rejects:['arrival']}).restarts,0,'Rose Gold keeps its append-only layout');
assert.equal(finish({metal:'gold10k',appendOnly:true,optimized:true,rejects:['arrival']}).restarts,0,'solid gold keeps its append-only layout');
assert.equal(finish({phase:'repack',optimized:true,rejects:['arrival']}).restarts,0,'the repack itself never loops');
// what counts as full for release: a fresh arrangement that runs its whole budget and still leaves out an order that fits
// an empty sheet has finished its search, so the sheet is full even below the 74% target; the quick gap search has not
{
 const ctx={S:{settings:{maxFill:.8,clearancePt:0}}};vm.createContext(ctx);
 vm.runInContext(html.slice(html.indexOf('function inflatedArea('),html.indexOf('function usableArea(')),ctx);
 const items=[{id:'kept',order:'o1',areaPt2:65,widthPt:8,heightPt:8},{id:'arrival',order:'o2',areaPt2:10,widthPt:3,heightPt:3},{id:'huge',order:'o3',areaPt2:90,widthPt:10,heightPt:9}];
 const full=({phase='repack',endedBy='budget',density=.65,rejects=['arrival'],placedPt2=65}={})=>ctx.sheetFull({intakePhase:phase,rejects,verification:{ok:true},placements:[{id:'kept'}]},{endedBy,density,usablePt2:100,placedPt2},items);
 assert.equal(full(),true,'a fresh arrangement that ran its budget and still left an order out is full below the target');
 assert.equal(full({phase:'final'}),true,'so is the final arrangement');
 assert.equal(full({phase:'fill'}),false,'the quick search of the gaps ending on time is not enough below the target');
 assert.equal(full({phase:'fill',endedBy:'trials'}),true,'a gap search that used up its trials finished');
 assert.equal(full({phase:'fill',density:.74}),true,'from the target, any overflow is full');
 assert.equal(full({rejects:['huge']}),false,'an order too big for an empty sheet does not make a sheet full');
 assert.equal(full({endedBy:'stopped'}),false,'a stopped search never releases a sheet');
 assert.equal(full({rejects:[]}),false,'a sheet that took everything is not full');
 assert.equal(full({rejects:[],placedPt2:80}),true,'the ceiling is full');
}
{
 const {c,sheet,pages}=setup(),next=c.addPage();next.charms=[{id:'next-existing'}];next.placements=[{id:'next-existing',cxPt:4,cyPt:5}];next.status='complete';
 const a={id:'extra-a',order:'pair',arrivalPin:true,pinned:{cxPt:1}},b={id:'extra-b',order:'pair'};sheet.charms.push(a,b);sheet.rejects=[a.id,b.id];sheet.verification={ok:true};
 Object.assign(c,{agent(){},labelOf:()=> 'Gold',orderSummary:()=>({text:'one order'})});
 vm.runInContext(html.slice(html.indexOf('function overflowToNextSheet('),html.indexOf('function inflatedArea(')),c);
 c.overflowToNextSheet(sheet);assert.equal(sheet.placements[0].id,'old');assert.equal(sheet.charms.length,1);assert.deepEqual(Array.from(next.charms,x=>x.id),['next-existing','extra-a','extra-b']);assert.equal(next.placements[0].id,'next-existing');assert.equal(a.pinned,null);assert.equal(pages.length,2);
}
{
 // Gold and Silver: an overflow goes to the next open sheet in line, and later arrivals try the earliest open sheet first
 const {c,sheet,pages,live}=setup(),second=c.addPage(),third=c.addPage();
 for(const [p,id] of [[second,'second'],[third,'third']]){p.charms=[{id}];p.placements=[{id,cxPt:1,cyPt:1}];p.status='complete';}
 const x={id:'x',order:'late'};sheet.charms.push(x);sheet.rejects=['x'];sheet.verification={ok:true};
 Object.assign(c,{agent(){},labelOf:()=> 'Gold',orderSummary:()=>({text:'one order'})});
 vm.runInContext(html.slice(html.indexOf('function manualSheetClosed('),html.indexOf('/** New artwork waits outside the live job.')),c);
 vm.runInContext(html.slice(html.indexOf('function overflowToNextSheet('),html.indexOf('function inflatedArea(')),c);
 c.overflowToNextSheet(sheet);assert.deepEqual(second.charms.map(p=>p.id),['second','x'],'the overflow joins the next sheet in line');assert.equal(third.charms.length,1);assert.equal(pages.length,3);
 vm.runInContext(bridge.slice(bridge.indexOf('  function intakePage(m, run)'),bridge.indexOf('  async function add(run)')).replace(/\bclosed\(/g,'window.LiveNest.closed('),c);
 const run={runId:'existing'};
 assert.equal(c.intakePage('gold',run),sheet,'arrivals try the earliest open sheet of the run');
 sheet.releaseFull=true;assert.equal(c.intakePage('gold',run),second,'a full sheet is skipped');
 sheet.releaseFull=false;assert.equal(c.intakePage('gold',{runId:'another'}),third,'another run starts from the newest sheet');
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

(async()=>{
 const old={metal:'rose',sheetId:'earlier',charms:[{id:'first'}],placements:[{id:'first',cxPt:1,cyPt:2,angle:0}],status:'complete'},pages=[old],notices=[];
 const saved={id:'saved-rose',metal:'rose',folder:'Saved',runId:'saved-run',sources:[{name:'saved.ai',url:'https://example.test/saved.ai'}],placements:[{id:'saved-id',hash:'same-hash',source:'saved.ai',index:0,cxPt:12,cyPt:13,angle:0,scale:.975}],roseStockId:'physical-rose'};
 let makeCharms=true;const state={sources:[],sheets:{rose:old}};
 const c={S:state,window:{RoseStock:{async restore(target){target.roseStock={id:'physical-rose'};target.rosePlan={profile:{},lines:[[[1,1],[2,2]]],shapes:[{id:'saved-id'}]};},protect(target){assert(target.charms.some(x=>x.id==='saved-id'));assert.equal(target.placements[0].scale,.975);target.roseProtected={placements:target.placements.map(p=>({...p})),lines:target.rosePlan.lines};}}},
  pagesOf:()=>pages,activeCharms:p=>p.charms,labelOf:()=> 'Rose Gold',toast:m=>notices.push(m),closeDlg(){},setMode(){},$(){return null;},confirm:()=>true,
  CharmNestAssets:{bytes:async()=>new Uint8Array([1])},File:class{constructor(bytes,name){this.name=name;}},
  async intake(files,metal,opts){assert(opts.deferNest);state.sources.push({id:'new-source',ingestReady:true,state:'ready',charms:makeCharms?[{id:'temporary-id',hash:'same-hash',sourceName:'saved.ai',index:0}]:[]});},
  setInterval:fn=>setTimeout(fn,0),clearInterval:clearTimeout,addPage(){const page={metal:'rose',charms:[],placements:[],status:'idle'};pages.push(page);return page;},showPage(){},computeSaturation(){},renderCard(){},drawPreview(){},renderRail(){}};
 c.RoseStock=c.window.RoseStock;vm.createContext(c);vm.runInContext(html.slice(html.indexOf('async function restoreSheet('),html.indexOf('async function loadCharms(')),c);
 await c.restoreSheet(saved);assert.equal(pages.length,2);assert.equal(old.sheetId,'earlier');assert.equal(old.placements[0].id,'first');assert.equal(pages[1].placements[0].id,'saved-id');assert.equal(pages[1].placements[0].scale,.975);assert.deepEqual({...pages[1].roseProtected.placements[0]}, {...pages[1].placements[0]});
 makeCharms=false;await assert.rejects(()=>c.restoreSheet({...saved,id:'missing-rose'}),/protected charms could not be restored/);
 assert.equal(pages.length,2,'a failed restore never clears or replaces an earlier sheet');
})().catch(e=>{console.error(e);process.exitCode=1;});
