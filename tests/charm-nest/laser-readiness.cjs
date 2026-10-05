const assert=require('node:assert/strict'),fs=require('node:fs'),vm=require('node:vm');
const R=require('../../charm-nest-readiness.js');
const clone=x=>JSON.parse(JSON.stringify(x));
const back=id=>({poolId:id,sheetId:'sheet1',approvedAt:10,approvedBy:'Paul',verified:{geometry:{ok:true},file:{ok:true}},outputs:{ai:{path:id+'.ai',url:'https://example.com/'+id+'.ai'}}});
const ready={id:'sheet1',setId:'set1',runId:'run1',status:'complete',poolIds:['copy1','copy2'],placedCount:2,verification:{ok:true},outputs:{ai:{url:'https://example.com/front.ai'},preview:{url:'https://example.com/front.png'}},orders:['order1'],orderReadiness:{order1:{ready:true}},label:{files:[{path:'qr.png',url:'https://example.com/qr.png',payload:'order1',orders:['order1']}]},backPool:[back('copy1'),back('copy2')]};
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
const data=new Map([['sheets/sheet1',ready],['sets/set1',{setId:'set1',sheetIds:['sheet1'],status:'open'}],['runs/run1',{lines:{a:{orderId:'order1',state:'written',poolIds:['copy1']},b:{orderId:'order1',state:'written',poolIds:['copy2']}}}]]);
let writes=0;
const snap=ref=>({id:ref.path.split('/')[1],exists:data.has(ref.path),data:()=>clone(data.get(ref.path))});
const noRuns={isQuery:true,select:()=>noRuns,get:async()=>({docs:[]})};   // (the runs that list an order: none but the sheet's own)
const context=vm.createContext({Readiness:R,process,console,str:String,num:Number,isId:()=>true,PREFIX:'',SETS:'sets',SHEETS:'sheets',RUNS:'runs',RUN_LINES:'run_lines',col:n=>({where:()=>noRuns,doc:id=>({path:n+'/'+id,get:async()=>snap({path:n+'/'+id})})}),FV:{serverTimestamp:()=>100},db:{runTransaction:async fn=>fn({get:async ref=>ref.isQuery?ref.get():snap(ref),set:(ref,patch)=>{writes++;data.set(ref.path,{...data.get(ref.path),...patch});},update:(ref,patch)=>{writes++;data.set(ref.path,{...data.get(ref.path),...patch});}}),getAll:async(...refs)=>refs.filter(x=>x.path).map(snap)}});
vm.runInContext(source.slice(source.indexOf('async function op_setUpdate'),source.indexOf('async function op_setGet')),context);
vm.runInContext(source.slice(source.indexOf('async function filingRecords'),source.indexOf('async function op_laserStatus')),context);
const broken=vm.createContext({Readiness:R,process,console:{warn(){},error(){},log(){}},str:String,num:Number,isId:()=>true,PREFIX:'',SETS:'sets',SHEETS:'sheets',RUNS:'runs',RUN_LINES:'run_lines',col:n=>({where:()=>{throw new Error('the runs cannot be listed');},doc:id=>({path:n+'/'+id,get:async()=>snap({path:n+'/'+id})})}),FV:{serverTimestamp:()=>100},db:{runTransaction:async fn=>fn({get:async ref=>ref.isQuery?ref.get():snap(ref),set:(ref,patch)=>{writes++;data.set(ref.path,{...data.get(ref.path),...patch});},update:(ref,patch)=>{writes++;data.set(ref.path,{...data.get(ref.path),...patch});}}),getAll:async(...refs)=>refs.filter(x=>x.path).map(snap)}});
vm.runInContext(source.slice(source.indexOf('async function op_setUpdate'),source.indexOf('async function op_setGet')),broken);
vm.runInContext(source.slice(source.indexOf('async function filingRecords'),source.indexOf('async function op_laserStatus')),broken);
(async()=>{
 await context.op_setUpdate({setId:'set1',patch:{status:'complete'}});assert.equal(writes,1);
 data.get('sheets/sheet1').backPool=[];
 await assert.rejects(context.op_setUpdate({setId:'set1',patch:{status:'complete'}}),/cannot be completed/);assert.equal(writes,1,'failed gate never writes completion');
 const records=await context.readinessRecords([data.get('sheets/sheet1')]);assert.equal(records[0].laser.waiting,2);
 assert.equal(records[0].laser.stages.orders,true,'its own unapproved engravings are its own steps, never its order check');assert.equal(records[0].orderReadiness.order1.ready,true,'both pieces of order1 are on this sheet: nothing else holds it');
 data.set('runs/run1',{lines:{a:{orderId:'order1',state:'written',poolIds:['copy1'],engrave:{needed:false,state:'none',approved:true}}}});
 const hydrated=await context.readinessRecords([data.get('sheets/sheet1')]);assert.equal(hydrated[0].laser.plain,1);assert.equal(hydrated[0].laser.waiting,1);
 // A line with nothing to engrave reads as plain on the server too, before its engraving check has run. The page let a
 // set with such a line commit at the station, and the server then refused to record the set as complete.
 const bridge=fs.readFileSync('charm-nest-bridge.js','utf8'),la=bridge.indexOf('  const cap = (v, n)'),lb=bridge.indexOf('  /** The other direction',la);
 const lines=vm.createContext({});vm.runInContext(bridge.slice(la,lb),lines);
 const row=spec=>({key:'order1_t2',state:'written',poolIds:['copy2'],engrave:null,spec,line:{transactionId:'t2',variations:[]},order:{receiptId:'order1'}});
 const [,plainLine]=lines.lineRecord(row({engraveCandidate:false,designSku:'BR-1',material:'rose',quantity:1}));
 assert.deepEqual(R.decisions([plainLine]).copy2,R.decisions([row({engraveCandidate:false})]).copy2,'the page and the server read the line alike');
 data.get('sheets/sheet1').backPool=[back('copy1')];
 data.set('runs/run1',{lines:{a:{orderId:'order1',state:'written',poolIds:['copy1'],engrave:{needed:true,state:'written',approved:true}},b:plainLine}});
 await context.op_setUpdate({setId:'set1',patch:{status:'complete'}});assert.equal(writes,2,'a set with a plain line not yet checked completes');
 for(const b of [{...plainLine,engraveCandidate:true},{...plainLine,engraveCandidate:undefined}]){
   data.set('runs/run1',{lines:{a:{orderId:'order1',state:'written',poolIds:['copy1'],engrave:{needed:true,state:'written',approved:true}},b}});
   await assert.rejects(context.op_setUpdate({setId:'set1',patch:{status:'complete'}}),/cannot be completed/,'a line to engrave, or one not read yet, still waits');
 }
 // The runs that list an order cannot be asked (an index not ready, a read refused): no order is read as whole, so a sheet is neither shown ready nor completed on it
 data.set('sheets/sheet1',clone(ready));data.set('sets/set1',{setId:'set1',sheetIds:['sheet1'],status:'open'});
 data.set('runs/run1',{lines:{a:{orderId:'order1',state:'written',poolIds:['copy1'],engraveCandidate:false},b:{orderId:'order1',state:'written',poolIds:['copy2'],engraveCandidate:false}}});
 assert.equal((await context.readinessRecords([data.get('sheets/sheet1')]))[0].orderReadiness.order1.ready,true,'with the runs listed, the order is whole');
 const before=writes,unread=await broken.readinessRecords([data.get('sheets/sheet1')]);
 assert.deepEqual(clone(unread[0].orderReadiness.order1),{ready:false,why:'Order readiness has not been verified'},'the runs cannot be listed: the order is not verified, never whole');
 await assert.rejects(broken.op_setUpdate({setId:'set1',patch:{status:'complete'}}),/cannot be completed/,'and a set is not completed on an unverified order');assert.equal(writes,before,'the failed gate writes nothing');
 console.log('Laser readiness OK: per-copy counts, physical and whole-order gates, exclusions, missing sheets, server completion guard and run hydration, plain lines read alike on the page and the server, an order whose runs cannot be listed stays unverified');
})().catch(e=>{console.error(e);process.exitCode=1;});
