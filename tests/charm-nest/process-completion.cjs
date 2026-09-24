// Exercise the real run controller: pending work is retained, ready work continues,
// and failures in persistence still stop the run. No network or production writes.
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const O = require('../../charm-nest-orders.js');
const source = fs.readFileSync('charm-nest-bridge.js', 'utf8');
let controller = source.slice(source.indexOf('const RunCtl ='), source.indexOf('/* ═══ 24 · Review'));
// Use the actual controller and save queue; only DOM rendering is irrelevant here.
const renderStart = controller.indexOf('  function renderBanner()');
const renderEnd = controller.indexOf('  return { optionsChanged', renderStart);
controller = controller.slice(0, renderStart) + '  function renderBanner() {}\n' + controller.slice(renderEnd);
controller = controller.replace('return { optionsChanged', 'return { _loop:loop, _nest:nestAll, optionsChanged');
function fixture({pending=1, review=1, autoCommit='on', pages=[], sets, rows}={}) {
  const calls=[], jobs=new Map();
  const state={pending,review,fail:null};
  const r={runId:'run',step:'nest',status:'running',mode:'auto',sheets:{},errors:[],orders:['order'],workspaceRestored:true};
  rows ||= [{key:'order:1',order:{receiptId:'order'},state:'written',poolIds:['p']}];
  sets ||= [{setId:'pending',sheetIds:['a'],blocked:true},{setId:'ready',sheetIds:['b']}];
  const asyncCall=name=>async()=>{calls.push(name);};
  const ctx={window:{},B:{run:r},S:{cloud:{ok:true},settings:{autoCommit}},O,Date,Promise,Map,Set,JSON,queueMicrotask,setTimeout,clearTimeout,setInterval,clearInterval,
    allSheets:()=>pages,Orders:{rows:()=>rows,lineRecord:x=>[x.key,x],revalidate:async()=>{calls.push('revalidate');return{changed:[]};},unclaim:asyncCall('unclaim'),pull:async()=>rows},
    Pool:{sheetOf:()=>null,addAll:async()=>0},Engrave:{items:()=>jobs,pendingCount:()=>state.pending,classifyAll:asyncCall('classify'),fitAll:asyncCall('fit'),
      // the words are read and fitted beside the run: the step only starts that work (see Engrave.background)
      background:()=>{calls.push('classify');calls.push('fit');return Promise.resolve();},saveBacks:asyncCall('backs')},
    Review:{count:()=>state.review},Gate:{flush:asyncCall('flush'),nestable:()=>true,modern:()=>true,assemble:asyncCall('assemble'),upgrade:asyncCall('upgrade')},
    LiveNest:{finish:asyncCall('finish')},Sets:{ofRun:()=>sets,releaseIssue:s=>s.blocked?'Approval pending':null,
      save:asyncCall('set-save'),finalize:async s=>{calls.push('labels:'+s.setId);if(state.fail)throw Error(state.fail);},
      commit:async s=>{assert(!s.blocked,'unready set cannot commit');calls.push('commit:'+s.setId);s.committedAt=Date.now();return{completed:[s.setId],refused:[]};}},
    api:async(_,b)=>{calls.push('save');return{};},Session:{schedule(){}},LiveStrip:{render(){}},Arrivals:{start:()=>calls.push('arrivals')},
    agent(){},toast(){},notifyPerson(){},ding(){},CN:{renderCard(){}},sheetName:s=>s.metal,
    startNest:p=>{calls.push('nest:'+p.sheetId);p.status='nesting';},sheetDirty:p=>{delete p.runHold;p.problem=null;p.dirty=true;p.status='ready';},METALS:[]};
  vm.createContext(ctx);vm.runInContext(controller,ctx);
  const ctl=ctx.window.RunCtl;
  return {ctx,ctl,r,calls,state,pages,sets,rows,jobs};
}
const tick=()=>new Promise(resolve=>setImmediate(resolve));
async function settled(f){for(let i=0;i<20;i++){await tick();if(f.r.status!=='running')return;}throw Error('Controller did not settle');}
function sheet(id,patch={}){return{sheetId:id,runId:'run',metal:id,page:1,charms:[{id,poolId:'p'}],placements:[{id}],rejects:[],status:'complete',persistedDone:true,fileBase:id,outputs:{},verification:{ok:true},...patch};}
(async()=>{
  const f=fixture();await f.ctl._loop();
  assert.equal(f.r.status,'processed');assert.equal(f.r.processingComplete,true);
  assert(f.calls.includes('fit'));assert(f.calls.includes('labels:ready'));assert(f.calls.includes('commit:ready'));
  assert(!f.calls.includes('labels:pending'));assert(!f.calls.includes('commit:pending'));assert(f.calls.includes('unclaim'));
  assert.equal(f.r.pendingWork.engraving,1);assert.equal(f.r.pendingWork.review,1);assert.equal(f.r.pendingWork.sets,1);
  const before=f.calls.length;f.ctl.poke();await tick();assert.equal(f.calls.length,before,'unchanged pending work never creates a retry loop');
  f.state.pending=0;f.state.review=0;f.sets[0].blocked=false;f.rows[0].engrave={approved:true,state:'written'};f.ctl.poke();await settled(f);
  assert.equal(f.r.status,'complete');assert(f.calls.includes('commit:pending'));assert.equal(f.calls.filter(x=>x==='commit:ready').length,1,'already committed set stays untouched');

  const pages=[sheet('flagged',{verification:{ok:false}}),sheet('good')];const n=fixture({pages,sets:[],pending:0,review:0});
  const nesting=n.ctl._loop();n.ctl.onSheetDone(pages[0]);n.ctl.onSheetDone(pages[1]);await nesting;
  assert.equal(n.r.status,'processed');assert(pages[0].runHold);assert(!pages[1].runHold);assert(n.calls.includes('fit'));assert.equal(n.r.pendingWork.sheets,1);

  const stuck=sheet('too-large',{status:'ready',placements:[],fileBase:null});const h=fixture({pages:[stuck],sets:[],pending:0,review:0});
  const held=h.ctl._loop();h.ctl.onSheetDone(stuck,Object.assign(Error('Order cannot fit'),{sheetPending:true}));await held;
  assert.equal(h.r.status,'processed');assert.equal(stuck.runHold,'Order cannot fit');assert(h.calls.includes('fit'));
  // Persistence can finish after a no-fit result; that callback must retain the hold.
  stuck.fileBase='too-large';h.ctl.onSheetDone(stuck);assert.equal(stuck.runHold,'Order cannot fit');
  let finishWrite;
  const saving=sheet('saving',{verification:{ok:false},persistedDone:false,persisted:new Promise(r=>finishWrite=r)});
  const w=fixture({pages:[saving],sets:[]});const savingRun=w.ctl._loop();w.ctl.onSheetDone(saving);
  await tick();assert.equal(w.r.status,'running');assert(!w.calls.includes('fit'),'held sheets still await outstanding writes');
  finishWrite();await savingRun;assert.equal(w.r.status,'processed');

  const pausedSheet=sheet('stopped',{status:'ready',endedBy:'stopped'});const s=fixture({pages:[pausedSheet],sets:[]});
  const stoppedSheetRun=s.ctl._loop();s.ctl.onSheetDone(pausedSheet);await stoppedSheetRun;assert.equal(s.r.status,'processed');

  const manual=fixture({autoCommit:'off',pending:0,review:0,sets:[{setId:'ready',sheetIds:['b']}]});await manual.ctl._loop();
  assert.equal(manual.r.status,'processed');assert.equal(manual.r.awaitCommit,true);assert(manual.calls.includes('unclaim'));assert(!manual.calls.includes('commit:ready'));
  await manual.ctl.resume();await settled(manual);assert.equal(manual.r.status,'processed');assert(!manual.calls.includes('commit:ready'),'Retry pending does not authorize a manual commit');
  await manual.ctl.commitNow();await settled(manual);assert.equal(manual.r.status,'complete');assert(manual.calls.includes('commit:ready'));

  const options=fixture();await options.ctl._loop();options.ctl.optionsChanged();await settled(options);
  assert.equal(options.r.status,'processed');assert.equal(options.r.errors.length,0);assert.equal(options.r.membershipRevision,1);

  const critical=fixture();critical.r.status='stopped';critical.r.step='labels';critical.state.fail='Storage permission denied';
  await critical.ctl.resume();await settled(critical);assert.equal(critical.r.status,'stopped');assert.match(critical.r.stoppedBy,/Storage permission denied/);assert(!critical.calls.includes('commit:ready'));
  const saveFailure=fixture({pages:[sheet('broken')]});const failedNest=saveFailure.ctl._loop();saveFailure.ctl.onSheetDone(saveFailure.pages[0],Object.assign(Error('write failed'),{stage:'persistence',critical:true}));await failedNest;
  assert.equal(saveFailure.r.status,'stopped');assert(!saveFailure.calls.includes('fit'));


  for(const stage of ['shape-analysis','solver','export']){
    const failed=sheet(stage,{status:'ready',fileBase:null,outputs:null});const next=sheet('other-metal',{status:'ready'});
    const local=fixture({pages:[failed,next],sets:[],pending:0,review:0});const task=local.ctl._loop();
    local.ctl.onSheetDone(failed,Object.assign(Error(stage+' unavailable'),{stage}));
    assert.equal(local.r.status,'running','one held sheet cannot stop another worker');
    next.status='complete';local.ctl.onSheetDone(next);await task;
    assert.equal(local.r.status,'processed');assert(failed.runHold);assert(local.calls.includes('fit'));assert(local.calls.includes('unclaim'));
  }
  // Recover the exact legacy stop in Paul's screenshot without re-nesting it.
  const bad=sheet('gold',{verification:{ok:false},problem:'Verification flagged',page:2});
  const old=fixture({pages:[bad,sheet('silver')],sets:[],pending:1,review:1});
  Object.assign(old.r,{status:'stopped',stoppedBy:'GF 14/20 · sheet 2 needs a look',at:{metal:'gold',page:2}});
  assert.equal(await old.ctl.recoverReviewStop(),true);await settled(old);
  assert.equal(old.r.status,'processed');assert(bad.runHold);assert(!old.calls.includes('nest:gold'));assert(old.calls.includes('fit'));
  const operator=fixture();operator.r.status='stopped';operator.r.stoppedBy='stopped by the operator';
  assert.equal(await operator.ctl.recoverReviewStop(),false);assert.equal(operator.r.status,'stopped');

  const empty=fixture({rows:[],sets:[],pending:0,review:0});empty.r.step='pull';await empty.ctl._loop();assert.equal(empty.r.status,'complete');assert.equal(empty.r.errors.length,0);
  const noEligible=fixture({sets:[],pending:0,review:1});noEligible.r.step='pool';await noEligible.ctl._loop();assert.equal(noEligible.r.status,'processed');assert.equal(noEligible.r.errors.length,0);

  // Actual release validator distinguishes expected pending work from unexpected failures.
  const a=source.indexOf('  function validateRelease(set)'),b=source.indexOf('  async function finalize(',a);
  const v={window:{CharmNestReadiness:{decisions:()=>({}),set:()=>({ready:false})}},sheetsOf:()=>[],Orders:{rows:()=>[]},Gate:{modern:()=>false}};
  vm.createContext(v);vm.runInContext(source.slice(a,b),v);assert.match(v.releaseIssue({}),/not ready/);
  v.window.CharmNestReadiness.set=()=>{throw Error('unexpected failure');};assert.throws(()=>v.releaseIssue({}),/unexpected failure/);
  // A cancelled order no longer holds its released sheet back (its piece is cut and set aside); a changed order still waits for review
  const goneSheet={sheetId:'s1',setId:'set-1',metal:'gold',status:'complete',verification:{ok:true},cloud:{ai:{url:'https://f/a.ai'},preview:{url:'https://f/p.png'}},
    placements:[{id:'c1'},{id:'c2'}],charms:[{id:'c1',poolId:'p1'},{id:'c2',poolId:'p2'}],label:{files:[{path:'l.png',url:'https://f/l.png',payload:'x'}]},backPool:[]};
  const goneLines=[{order:{receiptId:'1'},state:'written',poolIds:['p1'],engrave:{needed:false,approved:true,state:'none'}},
    {order:{receiptId:'2'},state:'gone',reason:'cancelled',poolIds:['p2'],engrave:{needed:true,approved:false,state:'review'}}];
  const gv={window:{CharmNestReadiness:require('../../charm-nest-readiness.js')},sheetsOf:()=>[goneSheet],Orders:{rows:()=>goneLines},Gate:{modern:()=>false}};
  vm.createContext(gv);vm.runInContext(source.slice(a,b),gv);
  assert.equal(gv.releaseIssue({sheetIds:['s1']}),null,'a cancelled order does not hold its released sheet back');
  Object.assign(goneLines[1],{state:'written',changePending:true});assert.match(gv.releaseIssue({sheetIds:['s1']}),/changed or needs review/,'a changed order still waits for review');
  console.log('Process completion OK: pending approvals, sheet holds, independent ready sets, follow-up wake, manual commit, options changes, critical failures, release guards and cancelled orders');
})().catch(e=>{console.error(e);process.exitCode=1;});
