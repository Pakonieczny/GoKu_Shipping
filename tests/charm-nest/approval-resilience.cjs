// Real Engrave state transitions with deterministic file, cloud and stamp boundaries.
// No browser automation and no production orders or remote storage are touched.
const assert=require('node:assert/strict'),fs=require('node:fs'),vm=require('node:vm');
const E=require('../../charm-nest-engraving-seals.js');
const A=require('../../charm-nest-activity.js');
const source=fs.readFileSync('charm-nest-bridge.js','utf8');
const begin=source.indexOf('const Engrave = window.Engrave = (() => {');
const end=source.indexOf('/* ═══ 22 · Sets',begin);
assert(begin>=0 && end>begin,'load the production engraving subsystem');
const returnAt=source.lastIndexOf('  return {',end);
assert(returnAt>begin,'instrument only the production subsystem’s final export');
const instrumentation=`
  loadFonts=async()=>{F_.ok=true;return F_;};
  verifyBackFile=(bytes,job)=>approvalFixture.verify(bytes,job);
  renderBack=(job,px)=>approvalFixture.preview(job,px);
  refreshBacks=()=>Session.schedule();
  render=()=>approvalFixture.paint();
  scheduleBackOutputs=sh=>approvalFixture.index(sh);
  window.__approvalTest={EG,decidedJobs,queuedJobs};
`;
const script=source.slice(begin,returnAt)+instrumentation+source.slice(returnAt,end);
const sessionCopy=source.slice(source.indexOf('  const OMIT = new Set('),source.indexOf('  function open() {',source.indexOf('  const OMIT = new Set(')));
const button=()=>({disabled:false,textContent:'Approved',attrs:new Map(),setAttribute(k,v){this.attrs.set(k,v);},getAttribute(k){return this.attrs.get(k)??null;},removeAttribute(k){this.attrs.delete(k);}});
const until=async predicate=>{for(let n=0;n<100;n++){if(predicate())return;await new Promise(setImmediate);}assert.fail('the controlled pipeline did not reach its boundary');};

function fixture({copies=['p1'],state='approved',by='Paul',at=123456}={}){
  const row={key:'order:line',state:'pooled',poolIds:copies.slice(),order:{receiptId:'12345001'},line:{transactionId:'tx-1',sku:'FROG'},spec:{designSku:'FROG',personalization:['A']},engrave:{needed:true,state,approved:state==='approved',approvedBy:state==='approved'?by:null,approvedAt:state==='approved'?at:null}};
  const charm={sourceId:'source-1',outline:{},members:[]};
  const first={sheetId:'sheet-1',runId:'run-1',fileBase:'SS_1',folderPath:'fixture/SS_1',backPool:[]};
  const second={sheetId:'sheet-2',runId:'run-1',fileBase:'SS_2',folderPath:'fixture/SS_2',backPool:[]};
  const mapping=new Map(copies.map(id=>[id,first])),calls={built:[],uploaded:[],recorded:[],reviews:[],schedule:0,checkpoint:[],paint:[],settled:0,presses:0,indexed:[]};
  const job={key:row.key,row,state,copies:copies.slice(),text:'A',lines:['A'],lineInput:['A'],lineMode:'preserve',lineGap:.18,fit:{size:2,capMm:1,weight:'Regular',glyphs:[],centre:[0,0],angle:0},view:{cx:0,cy:0,cutMembers:[],members:[]},verify:{geometry:{ok:true}},approvedBy:state==='approved'?by:null,approvedAt:state==='approved'?at:null,backs:[],engravingSeals:[{how:'engraveApproved',at:1000,by:'Seth'}]};
  if(state==='approved')E.keep(job);
  const jobs=new Map([[job.key,job]]);
  const window={addEventListener(){},Seal:{whenIdle:async()=>{},defer:()=>false},CharmNestInteraction:{idle:async()=>{},defer:()=>false}};
  let timer=0;
  const timers=new Map();
  const context={Date,Promise,Map,Set,WeakMap,WeakSet,Blob,ArrayBuffer,DataView,Uint8Array,TextEncoder,performance,
    window,document:{getElementById:()=>null,querySelectorAll:()=>[],activeElement:null},localStorage:{getItem:()=>null},
    setTimeout(fn,ms){const id=++timer;timers.set(id,{fn,ms});return id;},clearTimeout:id=>timers.delete(id),
    B:{engrave:{fonts:{ok:true},items:jobs},pool:{rows:new Map(copies.map(id=>[id,{copy:+id.replace(/\D/g,'')||1}]))},run:{runId:'run-1'}},
    S:{settings:{},cloud:{ok:true},library:{rows:[]}},PT:72/25.4,MM:25.4/72,Pool:{charmOf:()=>charm,sheetOf:id=>mapping.get(id),update:async()=>{}},
    sourceOf:()=>({parsed:{}}),allSheets:()=>[first,second],Master:{entryFor:()=>({})},CNEngravingSeals:{...E,press:async()=>{calls.presses++;}},CNListActivity:A,
    P:{buildBackFile:async options=>{calls.built.push(options.meta);return {bytes:new Uint8Array([1]),reference:{redrawn:true},wPt:20,hPt:20};}},
    employeeName:()=>by,askEmployee:()=>by,toast(){},agent(){},esc:String,
    Review:{remove(){},add:r=>calls.reviews.push(r),items:()=>[],render(){}},Orders:{rows:()=>[row],render(){}},
    RunCtl:{poke(){},backgroundSettled(){calls.settled++;},stopIfRunning(){throw Error('one back must not stop the entire run');}},
    Session:{schedule(){calls.schedule++;},async flush(){this.flushNow();},flushNow(){calls.checkpoint.push(snapshot());},copy:x=>context.copy(x)},
    api:async(name,payload)=>{if(payload.op==='backPut'){calls.recorded.push(payload.back);return {ok:true};}if(payload.op==='getSheet')return {sheet:{id:first.sheetId,backPool:first.backPool}};return {ok:true};},
    uploadBytes:async(path)=>{calls.uploaded.push(path);return {path,url:'https://fixture.invalid/'+path};},
    approvalFixture:{verify:async()=>({ok:true}),preview:()=>({_sizePt:{w:20,h:20},toBlob:fn=>fn(new Blob(['png']))}),paint(){calls.paint.push({state:job.state,decided:window.__approvalTest?.decidedJobs().length||0});},index:sh=>calls.indexed.push(sh.sheetId)}
  };
  vm.createContext(context);vm.runInContext(sessionCopy,context);vm.runInContext(script,context);
  function snapshot(){context.__snapshotJson=JSON.stringify({jobs:[job],orders:{rows:[row]}});return vm.runInContext('copy(JSON.parse(__snapshotJson))',context);}
  return {context,E:window.Engrave,job,row,jobs,charm,first,second,mapping,calls,timers,by,at,snapshot};
}
function assertDecision(f,message){
  assert(['approved','written'].includes(f.job.state),message+' keeps the order in Decided');
  assert.equal(f.row.engrave.approved,true,message+' keeps the recorded approval');
  assert.equal(f.job.approvedAt,f.at,message+' preserves the exact original approval time');
  assert.equal(f.job.approvedBy,f.by,message+' preserves the exact original signer');
  assert.equal(f.E.reviewedCount(),1,message+' counts the order once');
  assert.deepEqual(E.list(f.job).map(s=>[s.how,s.at,s.by]),[['engraveApproved',1000,'Seth'],['engraveApproved',f.at,f.by]],message+' never replaces or repeats historical seals');
}
async function retry(f){f.context.S.cloud.ok=true;f.E.resumeBacks(true);await until(()=>!f.job.backSaving);}

const cases=[];
async function check(name,run){try{await run();cases.push({name,ok:true});console.log('PASS: '+name);}catch(error){cases.push({name,ok:false,error:error.message});console.error('FAIL: '+name+' — '+error.message);}}
(async()=>{
  await check('temporary missing sheet preserves the decision and resumes without another approval',async()=>{
    const f=fixture();f.mapping.clear();await f.E.saveBacks(f.job);
    assertDecision(f,'missing sheet');assert(f.job.backPending,'a temporary dependency is recorded for recovery');assert.equal(f.calls.reviews.length,0);
    f.mapping.set('p1',f.first);await retry(f);assert.equal(f.job.state,'written');assert.equal(f.job.backs.length,1);assert.equal(f.calls.presses,0);assertDecision(f,'dependency recovery');
  });
  await check('moving a charm during an upload retries its current sheet with the original decision',async()=>{
    const f=fixture();let moved=false;
    f.context.uploadBytes=async(path)=>{f.calls.uploaded.push(path);if(!moved){moved=true;f.mapping.set('p1',f.second);}return {path,url:'https://fixture.invalid/'+path};};
    await f.E.saveBacks(f.job);assertDecision(f,'movement');assert(f.job.backPending);assert.equal(f.calls.recorded.length,0,'stale sheet never receives the back');
    await retry(f);assert.equal(f.job.state,'written');assert.equal(f.calls.recorded.length,1);assert.equal(f.calls.recorded[0].sheetId,'sheet-2');assertDecision(f,'current-sheet recovery');
  });
  await check('one temporarily unplaced copy cannot falsely finish the whole engraving job',async()=>{
    const f=fixture({copies:['p1','p2']});f.mapping.delete('p2');await f.E.saveBacks(f.job);
    assert.equal(f.job.state,'approved','only a complete set of saved copies may become written');assertDecision(f,'one missing copy');assert(f.job.backPending,'the missing copy remains recoverable');
    const finished=f.job.backs.find(b=>b.poolId==='p1');f.mapping.set('p2',f.second);await retry(f);
    assert.equal(f.job.state,'written');assert.equal(f.job.backs.length,2);assert.equal(f.job.backs.find(b=>b.poolId==='p1'),finished);
    assert.equal(f.calls.recorded.filter(b=>b.poolId==='p1').length,1);assert.equal(f.calls.recorded.find(b=>b.poolId==='p2').sheetId,'sheet-2');assertDecision(f,'missing copy restored');
  });
  await check('an unclassified cloud save failure never asks for a second approval',async()=>{
    const f=fixture();let fail=true;
    f.context.api=async(name,payload)=>{if(payload.op==='backPut'){if(fail)throw Error('Storage authorization temporarily unavailable (HTTP 401)');f.calls.recorded.push(payload.back);}return {ok:true};};
    await f.E.saveBacks(f.job);assertDecision(f,'cloud error');assert(f.job.backPending);assert.equal(f.calls.reviews.length,0);
    fail=false;await retry(f);assert.equal(f.job.state,'written');assert.equal(f.calls.presses,0);assertDecision(f,'cloud retry');
  });
  await check('partial-copy cloud retry keeps recorded files and writes only unfinished copies',async()=>{
    const f=fixture({copies:['p1','p2']});let fail=true;
    f.context.api=async(name,payload)=>{if(payload.op==='backPut'){if(fail && payload.back.poolId==='p2')throw Error('The storage service is not ready');f.calls.recorded.push(payload.back);}return {ok:true};};
    await f.E.saveBacks(f.job);assertDecision(f,'partial save');assert.deepEqual(Array.from(f.job.backs,b=>b.poolId),['p1']);const first=f.job.backs[0];
    fail=false;await retry(f);assert.equal(f.job.state,'written');assert(f.job.backs.find(b=>b.poolId==='p1')===first,'a finished copy retains its exact saved record');
    assert.equal(f.calls.recorded.filter(b=>b.poolId==='p1').length,1,'completed copy is not sent to backPut again');
    assert.equal(f.calls.recorded.filter(b=>b.poolId==='p2').length,1);assertDecision(f,'partial retry');
    assert.equal(new Set(f.job.backs.map(b=>b.poolId)).size,2);assert.equal(f.calls.presses,0);
  });
  await check('moving one already-written copy refreshes its ownership and preserves unchanged copies',async()=>{
    const f=fixture({copies:['p1','p2']});await f.E.saveBacks(f.job);assert.equal(f.job.state,'written');
    const unchanged=f.job.backs.find(b=>b.poolId==='p2');f.mapping.set('p1',f.second);await f.E.writeBacks(f.job);
    assert.equal(f.job.backs.find(b=>b.poolId==='p1').sheetId,'sheet-2','a back is never reused from a different sheet');
    assert(f.job.backs.find(b=>b.poolId==='p2')===unchanged,'a copy still on its sheet retains its file');
    assert.equal(f.calls.recorded.filter(b=>b.poolId==='p1').length,2);assert.equal(f.calls.recorded.filter(b=>b.poolId==='p2').length,1);
    assert(!f.first.backPool.some(b=>b.poolId==='p1'));assert.equal(f.second.backPool.filter(b=>b.poolId==='p1').length,1);assertDecision(f,'moved written copy');
  });
  await check('genuine exported geometry failure stays actionable without inventing another seal',async()=>{
    const f=fixture();f.context.approvalFixture.verify=async()=>({ok:false,why:'engraving intersects a cut-out or cut-edge clearance'});
    await f.E.saveBacks(f.job);assert(['review','blocked'].includes(f.job.state),'invalid geometry needs a placement correction');assert.equal(f.row.engrave.approved,false);
    assert.equal(f.calls.recorded.length,0);assert.equal(f.calls.presses,0);assert.equal(E.list(f.job).length,2,'the prior actual approval remains historical');
  });
  await check('pre-approval file failure records no new decision and allows a corrected retry',async()=>{
    const f=fixture({state:'review'});f.context.approvalFixture.verify=async()=>({ok:false,why:'no engraving paths in the file'});
    await f.E.approve(f.job,'Paul',button());assert.equal(f.job.state,'review');assert.equal(f.calls.presses,0);assert.equal(f.job.approvedAt,null);assert.equal(E.list(f.job).length,1);assert.equal(f.calls.recorded.length,0);assert.equal(f.job.approvalPreparing,false);
  });
  await check('a preview redraw exception cannot leave an approved file permanently marked as saving',async()=>{
    const f=fixture();let count=0;f.context.approvalFixture.paint=()=>{if(!count++)throw Error('The closed preview is unavailable');};
    await f.E.saveBacks(f.job);assert.equal(f.job.backSaving,false);assert.equal(f.job.state,'written');assertDecision(f,'preview redraw recovery');assert.equal(f.calls.recorded.length,1);
  });
  await check('a checkpoint during the stamp resumes the verified decision without a second press',async()=>{
    const original=fixture({state:'review'});let finish;original.context.CNEngravingSeals.press=async()=>{original.calls.presses++;await new Promise(r=>finish=r);};
    const task=original.E.approve(original.job,'Paul',button());await until(()=>original.calls.presses===1);
    try{
      assert.equal(original.job.state,'review','the running page still waits for the complete animation');
      const snap=original.snapshot(),saved=snap.jobs[0];assert.equal(saved.approvalIntent?.phase,'verified','approval intent survives the real checkpoint copier');
      assert.equal(saved.approvalIntent.by,'Paul');assert.equal(saved.stamping,undefined);assert.equal(saved.approvalPreparing,undefined);
      assert(original.calls.checkpoint.some(s=>s.jobs[0].approvalIntent?.at===saved.approvalIntent.at),'the durable checkpoint starts before the press');
      const reopened=fixture({state:'review'});Object.assign(reopened.row,snap.orders.rows[0]);Object.assign(reopened.job,saved,{row:reopened.row});reopened.context.employeeName=()=> 'Different viewer';
      reopened.E.resumeBacks();await until(()=>!reopened.job.backSaving);
      assert.equal(reopened.job.state,'written');assert.equal(reopened.job.approvedAt,saved.approvalIntent.at);assert.equal(reopened.job.approvedBy,'Paul');assert.equal(reopened.row.engrave.approved,true);
      assert.equal(reopened.E.reviewedCount(),1);assert.equal(reopened.calls.presses,0,'history is not animated or re-approved on recovery');assert.equal(reopened.calls.recorded.length,1);
      assert.equal(E.list(reopened.job).length,2);assert.equal(E.list(reopened.job).at(-1).at,saved.approvalIntent.at);assert.equal(E.list(reopened.job).at(-1).by,'Paul');assert.equal(reopened.job.approvalIntent,undefined);
    }finally{finish();await task;}
  });
  await check('one stamp completes before Decided, records the captured signer, and cannot be duplicated',async()=>{
    const f=fixture({state:'review'});let finish;f.context.CNEngravingSeals.press=async()=>{f.calls.presses++;await new Promise(r=>finish=r);};
    const b=button(),task=f.E.approve(f.job,'Paul',b);await until(()=>f.calls.presses===1);
    assert.equal(f.job.state,'review');assert.equal(f.E.reviewedCount(),0);assert.equal(f.calls.recorded.length,0);
    f.context.employeeName=()=> 'Later viewer';await f.E.approve(f.job,'Later viewer',b);assert.equal(f.calls.presses,1);
    finish();await task;assert.equal(f.job.state,'written');assert.equal(f.job.approvedBy,'Paul');assert.equal(f.E.reviewedCount(),1);assert.equal(f.calls.recorded.length,1);assert.equal(f.job.approvalPreparing,false);
    const signatures=E.list(f.job).map(s=>[s.how,s.at,s.by]);assert.equal(signatures.length,2);assert.equal(signatures[1][2],'Paul');
    await f.E.approve(f.job,'Later viewer',b);assert.equal(f.calls.presses,1);assert.equal(E.list(f.job).length,2);
  });
  const failed=cases.filter(x=>!x.ok);console.log(`${cases.length-failed.length}/${cases.length} engraving approval resilience scenarios passed`);
  if(failed.length)process.exitCode=1;
})().catch(error=>{console.error(error);process.exitCode=1;});
