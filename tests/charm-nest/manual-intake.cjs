const assert=require('node:assert/strict'),fs=require('node:fs'),vm=require('node:vm');
const O=require('../../charm-nest-orders');
const html=fs.readFileSync('charm-nest-1.html','utf8'),bridge=fs.readFileSync('charm-nest-bridge.js','utf8');
// a big batch goes on a sheet a few orders at a time (feedTurn, nestItems, feedOn)
const FEED=html.slice(html.indexOf('/* A big batch goes onto a sheet'),html.indexOf('/* A stopped run starts none'));
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
// The layout stands as placed: finishing a nest never starts another search of the same sheet, not after a miss, not at
// 85 charms, not below 74% (Paul, 24 Sep: a sheet re-arranged loses room). An order that misses moves to the next sheet.
{
 const at=html.indexOf('async function finishNest('),fn=html.slice(at,html.indexOf('\n}\n',at)+2);
 assert(fn.length>2000,'finishNest found');
 // the one search it may start is a second try of a sheet a time-out left with nothing placed (nothing to re-arrange)
 const calls=fn.match(/\bstartNest(Ready)?\(/g)||[];
 assert(calls.length<=1,'finishing a nest starts no second search');
 if(calls.length){const i=fn.search(/\bstartNest(Ready)?\(/),guard=fn.slice(Math.max(0,i-400),i);assert(/!result\.placements\.length/.test(guard)&&/timeoutRetried/.test(guard)&&/timedOut/.test(guard),'only a sheet a time-out left empty is searched again, once');}
 assert(!/intakeForceFinal\s*=\s*true|delete sh\.appendOnly|missRearranged\s*=\s*true|needsRepack|appendMissed/.test(fn),'and never frees the placed charms to move');
}
// A Gold or Silver sheet filling its gaps: a try that places nothing keeps the files, verification and record it has; a
// try that places something writes the sheet again (Paul, 24 Sep: no waiting between charms)
{
 const counts={build:0,verify:0,persist:0,put:[],overflow:0,done:0};
 const fx={
  S:{settings:{maxFill:.8,clearancePt:0,runMode:'auto'},mode:'nest'},window:{B:{run:{runId:'run-1',status:'processed'}}},performance,Map,Set,JSON,Math,Promise,Object,Error,
  document:{hidden:false},Notification:undefined,
  activeCharms:p=>p.charms.filter(x=>!x.excluded),agent(){},agentUpdate(){},log(){},renderCard(){},renderProgress(){},drawPreview(){},computeSaturation(){},toast(){},ding(){},$$:()=>[],
  stockFor:()=>({wPt:100,hPt:50}),labelOf:m=>m,sheetName:()=>'Gold sheet 1',endedText:e=>e,sheetT0:()=>0,fmt:{pct:v=>Math.round(v*100)+'%',s:v=>v+'ms',mm:v=>v+'mm'},
  CharmNestSolver:{bestResult:r=>r,publicLayout:r=>r},acceptBestLayout(){},
  CharmNestPDF:{buildSheet:async()=>{counts.build++;return new Uint8Array([1]);},verifyRendered:async()=>{counts.verify++;return {ok:true};}},
  ensurePdfJs:async()=>{},sourceOf:()=>({parsed:{}}),previewPng:async()=>new Uint8Array([2]),buildReport:()=>({rejects:[],calibration:{}}),buildJob:()=>({}),
  ensureWorker:sh=>({postMessage:()=>setTimeout(()=>sh._geomVerify({ok:true,overlappingPairs:[],res:6,minGapPt:1,minEdgePt:1}),0)}),
  nextSheetSeq:async sh=>sh.seq||1,sheetFileBase:sh=>'GF_Sheet-'+(sh.seq||1),
  persistSheet:async()=>{counts.persist++;},api:async(fn,body)=>{counts.put.push(body.sheet);return {ok:true};},
  overflowToNextSheet:sh=>{counts.overflow++;sh.charms=sh.charms.filter(c=>!sh.rejects.includes(c.id));},pumpNestQueue(){},flushManualIntake(){},RunCtl:{onSheetDone:()=>counts.done++},
  keepOrdersWhole(){},sheetFull:()=>true
 };
 fx.window.RunCtl=fx.RunCtl;fx.setTimeout=setTimeout;
 vm.createContext(fx);
 vm.runInContext(FEED,fx);
 vm.runInContext(html.slice(html.indexOf('/* Gap fill (Paul, 24 Sep)'),html.indexOf('function usableArea(')),fx);
 vm.runInContext(html.slice(html.indexOf('const CHARM_SCALE'),html.indexOf('/** One charm at a time on every sheet')),fx);
 vm.runInContext(html.slice(html.indexOf('async function finishNest('),html.indexOf('function pumpNestQueue()')),fx);
 const charms=Array.from({length:40},(_,i)=>({id:'p'+i,order:'o'+i,hash:'h'+i,name:'n'+i}));
 const sh={metal:'gold',runId:'run-1',sheetId:'s1',seq:1,page:1,charms:charms.slice(),placements:[],rejects:[],log:[],status:'nesting',nestFlow:'standard',intakePhase:'fill',appendOnly:true,verification:null,outputs:null};
 const place=ids=>ids.map((id,i)=>({id,cxPt:5+i,cyPt:5,angle:0,scale:.975}));
 const res=(placements,rejects,density)=>({placements,rejects,density,placedPt2:density*100,usablePt2:100,freePt2:(1-density)*100,endedBy:rejects.length?'no-room':'complete',trials:1,elapsedMs:10,params:{},smallRoom:true});
 const finish=async r=>{sh.status='nesting';sh.persistedDone=false;sh.jobId=Math.random().toString(36);sh.nestInitial=sh.placements.map(p=>({...p}));await fx.finishNest(sh,{result:r});await sh.persisted;};
 (async()=>{
  // the first write: files built, verified and saved
  await finish(res(place(charms.map(c=>c.id)),[],.70));
  assert.equal(counts.build,2);assert.equal(counts.persist,1);assert(sh._written,'kept once saved');
  // a new order misses: nothing new placed, so nothing is written again; the record's state is updated and the order moves on
  sh.charms.push({id:'late',order:'late',hash:'hl',name:'late'});
  await finish(res(sh.placements.map(p=>({...p})),['late'],.70));
  assert.equal(counts.build,2,'no second .ai');assert.equal(counts.verify,1,'no second render check');assert.equal(counts.persist,1,'no second upload');
  assert.equal(counts.put.length,1);assert.equal(counts.put[0].id,'s1');assert(!('placements' in counts.put[0]),'only the record state goes up');
  assert.equal(counts.overflow,1,'the order moves on to the next sheet');assert.equal(counts.done,2,'the run hears the sheet is done');assert.equal(sh.status,'partial');assert(sh.verification.ok);assert.equal(sh.outputs.ai[0],1);
  assert(sh.topup&&!sh.topup.closedAt,'the sheet is filling its gaps');
  // an order that fits: the sheet is written again
  sh.charms.push({id:'small',order:'small',hash:'hs',name:'small'});
  await finish(res([...sh.placements.map(p=>({...p})),{id:'small',cxPt:90,cyPt:40,angle:0}],[],.72));
  assert.equal(counts.build,4,'a placed charm writes the sheet again');assert.equal(counts.persist,2);
  // renamed artwork is written again even with nothing new placed
  sh.charms.find(c=>c.id==='p3').name='renamed';sh.charms.push({id:'late2',order:'late2',hash:'hl2',name:'late2'});
  await finish(res(sh.placements.map(p=>({...p})),['late2'],.72));
  assert.equal(counts.build,6,'a renamed charm is written again');
  console.log('Gap-fill tries that place nothing keep the written sheet OK');
 })().catch(e=>{console.error(e);process.exitCode=1;});
}
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
 assert.equal(full({phase:'fill',endedBy:'no-room'}),true,'a graded gap search that found no spot at any angle for the order finished');
 assert.equal(full({phase:'fill',density:.74}),true,'from the target, any overflow is full');
 assert.equal(full({rejects:['huge']}),false,'an order too big for an empty sheet does not make a sheet full');
 assert.equal(full({endedBy:'stopped'}),false,'a stopped search never releases a sheet');
 assert.equal(full({rejects:[]}),false,'a sheet that took everything is not full');
 assert.equal(full({rejects:[],placedPt2:80}),true,'the ceiling is full');
}
// gap fill: a nearly full Gold or Silver sheet whose next order missed stays first in line while the next 35 orders try its
// gaps; it goes to the laser at 75% shown full, once those 35 have tried it, or at once when not even one of the shop's
// smallest charms would still fit (Paul, 24 Sep)
{
 const logs=[],ctx={S:{settings:{maxFill:.8,runMode:'auto'}},window:{B:{run:{runId:'run-1',status:'processed'}}},fmt:{pct:v=>Math.round(v*100)+'%'},log:(sh,m)=>logs.push(m),renderCard(){},activeCharms:p=>p.charms.filter(x=>!x.excluded)};vm.createContext(ctx);
 vm.runInContext(FEED,ctx);
 vm.runInContext(html.slice(html.indexOf('/* Gap fill (Paul, 24 Sep)'),html.indexOf('function usableArea(')),ctx);
 const sheet=(extra={})=>{const charms=Array.from({length:60},(_,i)=>({id:'p'+i,order:'o'+i}));return {metal:'gold',runId:'run-1',rejects:['late'],charms,placements:charms.map(c=>({id:c.id})),verification:{ok:true},...extra};};
 const res=(density,extra={})=>({density,placedPt2:density*100,usablePt2:100,freePt2:(1-density)*100,endedBy:'no-room',smallRoom:true,...extra});
 // one update of the live run: the orders that reach the sheet try its gaps; those that miss move on to the next sheet
 const update=(sh,orders,placed=[])=>{sh.nestInitial=sh.placements.map(p=>({...p}));sh.rejects=[];for(const o of orders){sh.charms.push({id:'c-'+o,order:o});if(placed.includes(o))sh.placements.push({id:'c-'+o});else sh.rejects.push('c-'+o);}};
 const moveOn=sh=>{sh.charms=sh.charms.filter(c=>!sh.rejects.includes(c.id));};
 const sh=sheet();
 assert.equal(ctx.topupSettle(sh,res(.70),true),false,'the nearly full sheet stays open');
 assert.equal(sh.topup.base,.7);assert.equal(sh.topup.tried.length,0);assert.match(logs.at(-1),/next 35 orders try this sheet first/);
 moveOn(sh);update(sh,['a','b'],['a']);
 assert.equal(ctx.topupSettle(sh,res(.705),false),false,'two orders tried, one placed: still open');assert.deepEqual([...sh.topup.tried].sort(),['a','b']);assert.equal(sh.topup.placed,61);
 moveOn(sh);update(sh,[]);assert.equal(ctx.topupSettle(sh,res(.705),false),false,'an update that brings nothing counts nothing');assert.equal(sh.topup.tried.length,2);
 for(let i=0;i<32;i++){moveOn(sh);update(sh,['x'+i]);assert.equal(ctx.topupSettle(sh,res(.705),false),false);}
 assert.equal(sh.topup.tried.length,34);assert(!sh.topup.closedAt,'34 orders tried: still waiting');
 moveOn(sh);update(sh,['x0']);assert.equal(ctx.topupSettle(sh,res(.705),false),false,'the same order again is not a new try');
 moveOn(sh);update(sh,['last']);assert.equal(ctx.topupSettle(sh,res(.705),false),true,'the 35th order releases it');assert(sh.topup.closedAt);assert.equal(sh.topup.density,.705);assert.match(logs.at(-1),/35 later orders tried/);
 assert.equal(ctx.topupSettle(sh,res(.705),false),false,'a finished gap fill does not start again');
 const full=sheet();ctx.topupSettle(full,res(.70),true);moveOn(full);update(full,['a','b'],['a','b']);
 assert.equal(ctx.topupSettle(full,res(.745),false),true,'75% shown full releases it at once');assert.match(logs.at(-1),/75% full/);
 const short=sheet();ctx.topupSettle(short,res(.70),true);moveOn(short);update(short,['a'],['a']);
 assert.equal(ctx.topupSettle(short,res(.7449),false),false,'74% shown is not yet 75%');
 assert.equal(ctx.topupSettle(sheet(),res(.76),true),true,'a sheet already at 75% is released as full');
 assert.equal(ctx.topupSettle(sheet(),res(.70,{smallRoom:false}),true),true,'no room for even the smallest charms: no 35-order wait');
 const tight=sheet();ctx.topupSettle(tight,res(.70),true);moveOn(tight);update(tight,['a'],['a']);
 assert.equal(ctx.topupSettle(tight,res(.72,{smallRoom:false}),false),true,'and a gap fill ends once the smallest charms no longer fit');assert.match(logs.at(-1),/no room left for even the smallest charms/);
 assert.equal(ctx.topupSettle(sheet(),res(.70,{smallRoom:undefined}),true),false,'a search that did not look (no room check) keeps the 35-order rule');
 assert.equal(ctx.topupSettle(sheet(),res(.797),true),true,'a sheet at the ceiling is simply full');
 assert.equal(ctx.topupSettle(sheet({metal:'rose'}),res(.70),true),true,'Rose Gold keeps its green-line flow');
 assert.equal(ctx.topupSettle(sheet({metal:'gold10k'}),res(.70),true),true,'solid gold is released as before');
 assert.equal(ctx.topupSettle(sheet({roseProtected:{}}),res(.70),true),true);
 assert.equal(ctx.topupSettle(sheet(),res(.70,{endedBy:'stopped'}),false),false,'a stopped search starts nothing');
 assert.equal(ctx.topupSettle(sheet({verification:{ok:false}}),res(.70),true),true);
 assert.equal(ctx.topupSettle(sheet(),res(.60),false),false,'not full, no gap fill');
 assert.equal(ctx.topupSettle(sheet({rejects:[]}),res(.70),true),true,'nothing missed: nothing to wait for');
 ctx.S.settings.topup='off';assert.equal(ctx.topupSettle(sheet(),res(.70),true),true,'the setting turns it off');delete ctx.S.settings.topup;
 assert.equal(ctx.topupSettle(sheet({runId:'other'}),res(.70),true),true,'only the live run brings later orders');
 ctx.S.settings.runMode='manual';assert.equal(ctx.topupSettle(sheet(),res(.70),true),true,'Manual mode releases as before');
 const switched=sheet();ctx.S.settings.runMode='auto';ctx.topupSettle(switched,res(.70),true);ctx.S.settings.runMode='manual';
 assert.equal(ctx.topupSettle(switched,res(.70),false),true,'leaving Auto ends a gap fill: the sheet is released as it would have been');ctx.S.settings.runMode='auto';
 ctx.window.B.run.status='complete';const ended=sheet();assert.equal(ctx.topupSettle(ended,res(.70),true),true,'a finished run brings no more orders');ctx.window.B.run.status='processed';
 // while it fills its gaps: any later order that fits may go in, not only the oldest, up to the usual fill ceiling
 const jc={S:{settings:{maxFill:.8,clearancePt:0,insetPt:1.5,budgetS:180,packingAI:'off'}},stockFor:()=>({wPt:283,hPt:142}),activeCharms:s=>s.charms,angleSet:()=>[0,10],packingKey:()=>''};vm.createContext(jc);
 vm.runInContext(FEED,jc);
 vm.runInContext(html.slice(html.indexOf('const CAREFUL_ANGLES'),html.indexOf('function packingKey(')),jc);
 const topping={metal:'gold',intakePhase:'fill',appendOnly:true,placements:[{id:'old'}],nestInitial:[{id:'old'}],topup:{base:.7,tried:[]},charms:[{id:'old',orderDate:5},{id:'new',orderDate:9}]};
 let job=jc.buildJob(topping);assert.equal(job.maxFill,.8,'the gap fill is not capped below the ceiling');assert.deepEqual(job.pieces.map(p=>p.orderDate),[0,0]);
 // one charm at a time at 2° steps on every sheet (Paul, 24 Sep), Rose Gold and a sheet's first charms included
 assert.equal(job.angles.length,180,'a Gold or Silver arrival is fitted at 2° steps');assert.equal(job.angles[1],2);assert.equal(job.careful,true);
 assert.equal(jc.buildJob({...topping,appendOnly:false}).angles.length,180,'so is a sheet that starts empty');assert.equal(jc.buildJob({...topping,metal:'rose'}).angles.length,180,'and Rose Gold');
 assert.equal(jc.buildJob({...topping,nestFlow:'learned'}).angles.length,2,'the learned flow keeps the set step');assert.equal(jc.buildJob({...topping,nestFlow:'learned'}).careful,false);
 assert.equal(job.roomCheck,true,'Gold and Silver report whether the smallest charms still fit');assert.equal(jc.buildJob({...topping,metal:'rose'}).roomCheck,false);
 assert.deepEqual(job.lockedPlacements.map(p=>p.id),['old'],'the charms already placed stay where they are');
 assert.deepEqual(jc.buildJob({...topping,appendOnly:false}).lockedPlacements.map(p=>p.id),['old'],'on every sheet, not only an appended one');
 assert.equal(job.nearFullContact,true,'a near-full Gold or Silver queue may be built from the contacts');assert.equal(jc.buildJob({...topping,metal:'rose'}).nearFullContact,false,'Rose Gold keeps its left-to-right build');
 assert.deepEqual(jc.buildJob({...topping,appendOnly:false,intakePhase:'repack'}).pieces.map(p=>p.orderDate),[5,9],'oldest first again outside the gap fill');
 topping.topup.closedAt=1;job=jc.buildJob(topping);assert.equal(job.maxFill,.8);assert.deepEqual(job.pieces.map(p=>p.orderDate),[5,9]);
 const kc={agent(){}};vm.createContext(kc);vm.runInContext(html.slice(html.indexOf('function keepOrdersWhole('),html.indexOf('function orderSummary(')),kc);
 const byId=new Map([['old',{id:'old',order:'o1',orderDate:1}],['miss',{id:'miss',order:'o2',orderDate:2}],['young',{id:'young',order:'o3',orderDate:3}]]);
 const kept={placements:[{id:'old'},{id:'young'}],rejects:['miss'],topup:{base:.7,tried:[]}};kc.keepOrdersWhole(kept,byId);assert.deepEqual(kept.placements.map(p=>p.id),['old','young'],'a younger order that fits is kept while topping up');
 const fifo={placements:[{id:'old'},{id:'young'}],rejects:['miss']};kc.keepOrdersWhole(fifo,byId);assert.deepEqual(fifo.placements.map(p=>p.id),['old'],'otherwise oldest first, as before');
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
 console.log('Manual intake OK: file batches, running progress, identity, save/review waits, frozen stock, pins, real gap filling, overflow, no re-arrangement, 35-order gap fill and 2° steps');
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
