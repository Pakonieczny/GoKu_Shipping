const assert=require('node:assert/strict'),fs=require('node:fs'),vm=require('node:vm');
const R=require('../../charm-nest-readiness.js');
const clone=x=>JSON.parse(JSON.stringify(x));
const back=id=>({poolId:id,sheetId:'sheet1',approvedAt:10,approvedBy:'Paul',verified:{geometry:{ok:true},file:{ok:true}},outputs:{ai:{path:id+'.ai',url:'https://example.com/'+id+'.ai'}}});
const ready={id:'sheet1',setId:'set1',runId:'run1',status:'complete',poolIds:['copy1','copy2'],placedCount:2,verification:{ok:true},outputs:{ai:{url:'https://example.com/front.ai'},preview:{url:'https://example.com/front.png'}},orders:['order1'],label:{files:[{path:'qr.png',url:'https://example.com/qr.png',payload:'order1',orders:['order1']}]},backPool:[back('copy1'),back('copy2')]};
assert.equal(R.sheet(ready).ready,true);
const pending=clone(ready);pending.backPool=[];assert.equal(R.sheet(pending).waiting,2);assert.equal(R.sheet(pending).ready,false,'solver complete never implies approval');
pending.engraving={copy1:{needed:true,state:'approved',approved:true}};assert.equal(R.sheet(pending).approved,1);assert.equal(R.sheet(pending).saving,1);assert.equal(R.sheet(pending).ready,false,'approval alone cannot release');
pending.engraving.copy2={needed:false,state:'none',approved:true};pending.backPool=[back('copy1')];assert.equal(R.sheet(pending).ready,true);assert.equal(R.sheet(pending).plain,1);
for(const mutate of [s=>s.backPool[0].verified.file.ok=false,s=>s.backPool[0].outputs.ai.path=null,s=>s.backPool[0].invalidated=true,s=>s.backPool[0].sheetId='elsewhere',s=>s.label.files=[],s=>s.label.files[0].orders=[],s=>s.verification.ok=false,s=>s.draft=true,s=>s.archived=true,s=>s.solidIncluded=false,s=>s.dirty=true,s=>s.placedCount=3]){const s=clone(ready);mutate(s);assert.equal(R.sheet(s).ready,false);}
const dup=clone(ready);dup.poolIds.push('copy1');dup.backPool.push(back('unplaced'));assert.equal(R.sheet(dup).approved,2,'count physical placed copies once');
assert.equal(R.set({sheetIds:['sheet1','missing']},[ready]).ready,false,'filtered/missing sheet blocks the set');
assert.equal(R.set({sheetIds:['sheet1']},[ready]).ready,true);
assert.equal(R.decisions([{poolIds:['c'],engrave:null}]).c.approved,false,'unknown is never plain');
// a cancelled order's piece left on a released sheet is set aside and waits on no engraving; an approved back stays as it was
assert.deepEqual(R.decisions([{state:'gone',poolIds:['g'],engrave:{needed:true,state:'review',approved:false}}]).g,{needed:false,state:'none',approved:true});
assert.deepEqual(R.decisions([{state:'gone',poolIds:['s'],engrave:{needed:true,state:'written',approved:true}}]).s,{needed:true,state:'written',approved:true});
// Exercise the real server transaction gate and hydration with a deterministic store.
const source=fs.readFileSync('netlify/functions/charmNestLibrary.js','utf8');
const data=new Map([['sheets/sheet1',ready],['sets/set1',{setId:'set1',sheetIds:['sheet1'],status:'open'}],['runs/run1',{lines:{}}]]);
let writes=0;
const snap=ref=>({id:ref.split('/')[1],exists:data.has(ref),data:()=>clone(data.get(ref))});
const context=vm.createContext({Readiness:R,str:String,isId:()=>true,SETS:'sets',SHEETS:'sheets',RUNS:'runs',col:n=>({doc:id=>n+'/'+id}),FV:{serverTimestamp:()=>100},db:{runTransaction:async fn=>fn({get:async ref=>snap(ref),set:(ref,patch)=>{writes++;data.set(ref,{...data.get(ref),...patch});},update:(ref,patch)=>{writes++;data.set(ref,{...data.get(ref),...patch});}}),getAll:async(...refs)=>refs.map(snap)}});
vm.runInContext(source.slice(source.indexOf('async function op_setUpdate'),source.indexOf('async function op_setGet')),context);
vm.runInContext(source.slice(source.indexOf('async function readinessRecords'),source.indexOf('async function op_laserStatus')),context);
(async()=>{
 await context.op_setUpdate({setId:'set1',patch:{status:'complete'}});assert.equal(writes,1);
 data.get('sheets/sheet1').backPool=[];
 await assert.rejects(context.op_setUpdate({setId:'set1',patch:{status:'complete'}}),/cannot be completed/);assert.equal(writes,1,'failed gate never writes completion');
 const records=await context.readinessRecords([data.get('sheets/sheet1')]);assert.equal(records[0].laser.waiting,2);
 data.set('runs/run1',{lines:{a:{poolIds:['copy1'],engrave:{needed:false,state:'none',approved:true}}}});
 const hydrated=await context.readinessRecords([data.get('sheets/sheet1')]);assert.equal(hydrated[0].laser.plain,1);assert.equal(hydrated[0].laser.waiting,1);
 // A line with nothing to engrave reads as plain on the server too, before its engraving check has run. The page let a
 // set with such a line commit at the station, and the server then refused to record the set as complete.
 const bridge=fs.readFileSync('charm-nest-bridge.js','utf8'),la=bridge.indexOf('  const cap = (v, n)'),lb=bridge.indexOf('  /** The other direction',la);
 const lines=vm.createContext({});vm.runInContext(bridge.slice(la,lb),lines);
 const row=spec=>({key:'order1_t2',state:'written',poolIds:['copy2'],engrave:null,spec,line:{transactionId:'t2',variations:[]},order:{receiptId:'order1'}});
 const [,plainLine]=lines.lineRecord(row({engraveCandidate:false,designSku:'BR-1',material:'rose',quantity:1}));
 assert.deepEqual(R.decisions([plainLine]).copy2,R.decisions([row({engraveCandidate:false})]).copy2,'the page and the server read the line alike');
 data.get('sheets/sheet1').backPool=[back('copy1')];
 data.set('runs/run1',{lines:{a:{poolIds:['copy1'],engrave:{needed:true,state:'written',approved:true}},b:plainLine}});
 await context.op_setUpdate({setId:'set1',patch:{status:'complete'}});assert.equal(writes,2,'a set with a plain line not yet checked completes');
 for(const b of [{...plainLine,engraveCandidate:true},{...plainLine,engraveCandidate:undefined}]){
   data.set('runs/run1',{lines:{a:{poolIds:['copy1'],engrave:{needed:true,state:'written',approved:true}},b}});
   await assert.rejects(context.op_setUpdate({setId:'set1',patch:{status:'complete'}}),/cannot be completed/,'a line to engrave, or one not read yet, still waits');
 }
 console.log('Laser readiness OK: per-copy counts, all five gates, exclusions, missing sheets, server completion guard and run hydration, plain lines read alike on the page and the server');
})().catch(e=>{console.error(e);process.exitCode=1;});
