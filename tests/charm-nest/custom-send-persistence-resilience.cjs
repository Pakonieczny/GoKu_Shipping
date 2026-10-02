// Execute the production CustomSheet factory with controlled IDB, cloud, pool and stamp failures.
// No browser automation, network, credentials or shop records are used.
'use strict';
const assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),vm=require('node:vm');
const source=fs.readFileSync(path.join(__dirname,'../../charm-nest-bridge.js'),'utf8');
const start=source.indexOf('const CustomSheet = window.CustomSheet = (() => {'),end=source.indexOf('/* ═══ 23b',start);
assert(start>0 && end>start);
const settle=async(n=35)=>{for(let i=0;i<n;i++)await Promise.resolve();};
function deferred(){let resolve;const promise=new Promise(r=>{resolve=r;});return {promise,resolve};}
function fixture({holdStamp=false,holdRead=false,offline=false}={}){
 const stamp=deferred(),read=deferred();
 const control={presses:0,checkpoints:[],checkpointFail:false,poolCalls:[],cloudCalls:[],cloud:new Map(),events:new Map(),lossSent:false,failPool:false,partialPool:false,holdStamp,holdRead,uploads:0,redraws:0,toasts:[],done:0};
 const c=vm.createContext({console,Promise,Date,Map,Set,WeakMap,Uint8Array,performance,setTimeout,clearTimeout,queueMicrotask,control,stampGate:stamp.promise,readGate:read.promise});
 vm.runInContext(`
 const fakeNode=()=>({isConnected:true,style:{},setAttribute(){},remove(){this.isConnected=false;},querySelector(){return {getAnimations:()=>[]};},appendChild(){},getClientRects:()=>[{}]});
 const document={hidden:false,addEventListener(){},querySelector:()=>null,querySelectorAll:()=>[],getElementById:()=>null,createElement:fakeNode};
 const addEventListener=()=>{},requestAnimationFrame=f=>f(0),window={};
 const METALS=[{key:'gold',label:'GF',color:'#a90'},{key:'silver',label:'SS',color:'#aaa'}],MM=25.4/72;
 const S={mode:'review',cloud:{ok:${!offline}},settings:{},poolSources:{}};
 const rows=[1,2].map(n=>({key:'4174476673_'+n,order:{receiptId:'4174476673',createTs:1},line:{transactionId:n,sku:'CUSTOM_6673'},spec:{quantity:1,material:'gold',designSku:'CUSTOM_6673',form:'necklace'},state:'unmatched',poolIds:[],problems:[{kind:'unmatchedSku'}]}));
 const files=[{id:'file-test',name:'Customer.ai',kind:'ai',size:3,hash:'abc123',cloud:{path:'charmnest/custom/4174476673/test.pdf',url:'https://saved.example/test.pdf'},metal:'gold',qty:2,pieces:2,wMm:10,hMm:12,maxPt:10,minPt:10,maxAreaPt2:100,state:'ready',bytes:new Uint8Array([1,2,3])}];
 const e={ck:'custom:4174476673:CUSTOM_6673',rid:'4174476673',at:Date.now()-1000,files,sent:null};
 const B={orders:{rows,byKey:new Map(rows.map(r=>[r.key,r]))},customDesigns:{[e.ck]:e},pool:{rows:new Map()},maps:{},run:{runId:'run-test'}};
 const chars=new Map(),sheet={sheetId:'sheet-test',charms:[],placements:[]};
 const allSheets=()=>[sheet],labelOf=m=>m==='gold'?'GF':'SS',stockFor=()=>({wPt:100,hPt:100});
 const employeeName=()=> 'Paul',fmtT=at=>String(at),esc=s=>String(s||''),agent=()=>{},toast=(s)=>control.toasts.push(s),el=fakeNode;
 const CNEngravingSeals={merge:()=>[]},O={poolId:(o,l,n)=>o.receiptId+'_'+l.transactionId+'_'+n};
 const P={parseSource:async()=>{if(control.holdRead)await readGate;return {};},groupCharmsAsync:async()=>({orphans:[],charms:[0,1].map(n=>({id:'raw-'+n,bbox:[0,0,10,10],members:[],outline:{subpaths:[]},widthPt:10,heightPt:10,areaPt2:100,hash:'piece-'+n}))}),integrateRings:()=>({left:[]}),buildSilhouettes:async()=>{}};
 const CharmNestAssets={bytes:async()=>new Uint8Array([1,2,3])};
 const uploadBytes=async(path)=>{control.uploads++;return {path,url:'https://saved.example/'+path};};
 const Session={schedule(){},flushNow:async()=>{if(control.checkpointFail)return false;control.checkpoints.push(JSON.parse(JSON.stringify({customDesigns:B.customDesigns,rows})));return true;}};
 const TL={line:(r,type,event)=>control.events.set(event.id,{type,...event})};
 const api=async(name,body)=>{control.cloudCalls.push(JSON.parse(JSON.stringify(body)));if(body.op==='customSheetPut'){control.cloud.set(body.record.ck,JSON.parse(JSON.stringify(body.record)));if(body.record.phase==='sent' && control.lossSent){control.lossSent=false;throw new Error('Lost cloud acknowledgement');}return {record:JSON.parse(JSON.stringify(body.record))};}if(body.op==='customSheetGet'){return {records:Object.fromEntries([...control.cloud].map(([ck,rec])=>[ck,JSON.parse(JSON.stringify(rec))]))};}return {url:'https://saved.example/file.pdf'};};
 const Pool={charmOf:id=>chars.get(id),sheetOf:id=>chars.has(id)?sheet:null,onSheets:r=>r.poolIds.length && r.poolIds.every(id=>chars.has(id)),settle:r=>{r.state=r.poolIds.every(id=>B.pool.rows.get(id)?.sheetId)?'written':'pooled';r.reason=null;return true;}};
 const Review={render(){control.redraws++;},cardKey:r=>'custom:'+r.order.receiptId+':CUSTOM_6673',repool:async r=>{
 control.poolCalls.push(r.key);const prep=await CustomSheet.prepare(r,B.run);if(!prep)return;
 if(control.failPool && r===rows[1]){control.failPool=false;r.state='held';r.reason='Network timeout';return;}
 if(control.partialPool && r===rows[0]){control.partialPool=false;const pc=prep.pools[0],ch=prep.charms[0];chars.set(pc.poolId,ch);B.pool.rows.set(pc.poolId,pc);sheet.charms.push(ch);r.poolIds=[pc.poolId];r.state='held';r.reason='Pool reply lost';return;}
 for(let i=0;i<prep.pools.length;i++){const pc=prep.pools[i],ch=prep.charms[i];if(!chars.has(pc.poolId)){chars.set(pc.poolId,ch);sheet.charms.push(ch);}B.pool.rows.set(pc.poolId,pc);}
 r.poolIds=prep.pools.map(p=>p.poolId);r.state='pooled';r.problems=[];r.hold=null;
 }};
 const Orders={rows:()=>B.orders.rows};
 const Motion=window.Motion={reduced:()=>false,layer:()=>fakeNode()};
 const Seal=window.Seal={stampOn:async(host,spec)=>{control.presses++;control.stamp=JSON.parse(JSON.stringify(spec.stamp));if(control.holdStamp)await stampGate;},busy:()=>false,defer:()=>false};
 const it={key:'ord:'+e.ck,rows,row:rows[0],onDone:()=>{control.done++;}};
 const button=fakeNode();
 `,c);
 vm.runInContext(source.slice(start,end),c);
 return {c,control,stamp,read,run:code=>vm.runInContext(code,c)};
}
(async()=>{
 const failures=[],checks=[];
 async function check(name,fn){try{await fn();checks.push(name);console.log('PASS:',name);}catch(e){failures.push({name,e});console.error('FAIL:',name,e.stack);}}
 await check('one durable original intent precedes stamp and placement; full stamp gates departure',async()=>{
  const f=fixture({holdStamp:true}),p=f.run('CustomSheet.send(it,{from:button})');await settle();
  assert.equal(f.control.presses,1);assert.equal(f.control.poolCalls.length,0);assert.equal(f.control.done,0);
  const saved=Object.values(f.control.checkpoints[0].customDesigns)[0];assert(saved.sendIntent?.id);assert.equal(saved.sendIntent.by,'Paul');assert.equal(saved.sent,null);
  assert.equal(f.run('CustomSheet.decisionOf(rows[0])'),null);assert.equal(f.run('CustomSheet.sending(rows[0])'),true);
  assert.equal(await f.run('CustomSheet.prepare(rows[0],B.run)'),null,'background intake cannot place pending designs while the wooden stamp is still down');
  const p2=f.run('CustomSheet.send(it,{from:button})');await settle();assert.equal(f.control.presses,1);
  f.stamp.resolve();await Promise.all([p,p2]);assert.equal(f.run('e.sent.by'),'Paul');assert.equal(f.run('e.sent.at'),saved.sendIntent.at);assert.equal(f.run('sheet.charms.length'),4);assert.equal(f.control.done,1);assert.equal(f.control.events.size,2);
  await f.run('CustomSheet.send(it,{from:button})');assert.equal(f.control.presses,1);assert.equal(f.run('sheet.charms.length'),4);
  assert(f.control.cloudCalls.filter(x=>x.op==='customSheetPut').every(x=>!Object.hasOwn(x.record.files[0],'bytes')),'remote metadata contains no artwork bytes');
 });
 await check('failed checkpoint creates no stamp, pool copy or false decision and retry keeps original time',async()=>{
  const f=fixture();f.control.checkpointFail=true;await f.run('CustomSheet.send(it,{from:button})');
  assert.equal(f.control.presses,0);assert.equal(f.control.poolCalls.length,0);assert.equal(f.run('e.sent'),null);const at=f.run('e.sendIntent.at');
  assert.match(f.run('CustomSheet.buttonsHtml(it,true)'),/Retry send/);f.control.checkpointFail=false;await f.run('CustomSheet.send(it,{from:button})');assert.equal(f.run('e.sent.at'),at);assert.equal(f.control.presses,1);
 });
 await check('partial multi-line failure preserves first row and retries only unfinished row without another stamp',async()=>{
  const f=fixture();f.control.failPool=true;await f.run('CustomSheet.send(it,{from:button})');
  const at=f.run('e.sendIntent.at');assert.equal(f.run('e.sent'),null);assert.equal(f.run('sheet.charms.length'),2);assert.match(f.run('CustomSheet.buttonsHtml(it,true)'),/Retry send/);
  await f.run('CustomSheet.send(it,{from:button})');assert.equal(f.run('sheet.charms.length'),4);assert.equal(f.run('new Set(sheet.charms.map(c=>c.poolId)).size'),4);assert.equal(f.control.presses,1);assert.equal(f.run('e.sent.at'),at);assert.deepEqual(f.control.poolCalls,['4174476673_1','4174476673_2','4174476673_2']);
 });
 await check('partial copy lost reply keeps original copy numbers and never adds a duplicate charm',async()=>{
  const f=fixture();f.control.partialPool=true;await f.run('CustomSheet.send(it,{from:button})');assert.equal(f.run('sheet.charms.length'),1);
  await f.run('CustomSheet.recover()');assert.equal(f.run('sheet.charms.length'),4);assert.equal(f.run('new Set(sheet.charms.map(c=>c.poolId)).size'),4);assert.equal(f.control.presses,1);
 });
 await check('lost final cloud reply is completed through cloud recovery with original signer/time and no re-pool',async()=>{
  const f=fixture();f.control.lossSent=true;await f.run('CustomSheet.send(it,{from:button})');const at=f.run('e.sendIntent.at');
  assert.equal(f.run('e.sent'),null);assert.equal(f.run('sheet.charms.length'),4);const calls=f.control.poolCalls.length;
  await f.run('CustomSheet.load({force:true})');assert.equal(f.run('e.sent.at'),at);assert.equal(f.run('e.sent.by'),'Paul');assert.equal(f.control.presses,1);assert.equal(f.control.poolCalls.length,calls);
 });
 await check('reopening a mid-stamp checkpoint resumes the same immutable assignment without replaying ink',async()=>{
  const f=fixture({holdStamp:true}),p=f.run('CustomSheet.send(it,{from:button})');await settle();const saved=f.control.checkpoints[0];
  const recovered=fixture();recovered.c.savedJSON=JSON.stringify(saved);recovered.run('const data=JSON.parse(savedJSON);B.customDesigns=data.customDesigns;B.orders.rows=data.rows;B.orders.byKey=new Map(data.rows.map(r=>[r.key,r]));');
  await recovered.run('CustomSheet.recover()');const dec=recovered.run('CustomSheet.decisionOf(B.orders.rows[0])');assert.equal(dec.by,'Paul');assert.equal(dec.at,Object.values(saved.customDesigns)[0].sendIntent.at);assert.equal(recovered.control.presses,0);assert.equal(recovered.run('sheet.charms.length'),4);
  f.stamp.resolve();await p;
 });
 await check('stale source read cannot send after workspace or row ownership was replaced',async()=>{
  for(const replace of ['B.customDesigns={};','B.orders.byKey.set(rows[0].key,{...rows[0]});']){
   const f=fixture({holdRead:true}),p=f.run('CustomSheet.send(it,{from:button})');await settle();f.run(replace);f.read.resolve();await p;assert.equal(f.control.presses,0);assert.equal(f.run('sheet.charms.length'),0);assert.equal(f.control.cloudCalls.length,0);
  }
 });
 await check('legacy sends retain actor/time and become durable without another approval or new copies',async()=>{
  const f=fixture();const at=Date.now()-123456;f.c.legacyAt=at;f.run('e.sent={at:legacyAt,by:"Seth",lines:{[rows[0].key]:[{f:files[0].id,i:0}]}};');
  assert.equal(f.run('CustomSheet.decisionOf(rows[0]).by'),'Seth');await f.run('CustomSheet.recover()');assert.equal(f.run('e.sent.at'),at);assert.equal(f.control.presses,0);assert.equal(f.control.poolCalls.length,0);assert.equal(f.run('e.sendCloudPending'),false);assert.equal(f.control.cloudCalls[0].record.sent.by,'Seth');
 });
 await check('offline send remains locally complete and synchronizes the same identity when connection returns',async()=>{
  const f=fixture({offline:true});f.run('files[0].cloud=null;');await f.run('CustomSheet.send(it,{from:button})');const id=f.run('e.sent.id');assert.equal(f.run('e.sendCloudPending'),true);assert.equal(f.control.cloudCalls.length,0);
  f.run('S.cloud.ok=true;');await f.run('CustomSheet.recover()');assert.equal(f.run('e.sent.id'),id);assert.equal(f.control.uploads,1);assert.equal(f.control.presses,1);assert.equal(f.run('sheet.charms.length'),4);assert.equal(f.run('e.sendCloudPending'),false);
 });
 await check('cleanup removals survive cloud merge and removed copies are never prepared again',async()=>{
  const f=fixture();await f.run('CustomSheet.send(it,{from:button})');const file=f.run('e.files[0].bytes');
  assert.equal(f.run('CustomSheet.dropPieces([rows[0].key+"_1"],"cleanup-test")'),1);await f.run('CustomSheet.load({force:true})');
  assert.equal(f.run('e.sent.lines[rows[0].key][0].removed'),'cleanup-test');assert.equal(f.run('e.files[0].bytes'),file,'cloud enrichment does not drop local artwork bytes');const prep=await f.run('CustomSheet.prepare(rows[0],B.run)');assert.deepEqual(Array.from(prep.pools,p=>p.copy),[2],'copy2 keeps its original identity after copy1 was intentionally removed');
 });
 await check('remote pending records and original cloud file sources recover after a different card grouping',async()=>{
  const f=fixture();f.control.failPool=true;await f.run('CustomSheet.send(it,{from:button})');const pending=JSON.parse(JSON.stringify(f.control.cloud.get(f.run('e.ck'))));
  const resumed=fixture();resumed.run('B.customDesigns={};');resumed.control.cloud.set(pending.ck,pending);await resumed.run('CustomSheet.load({force:true})');const sent=resumed.run('CustomSheet.decisionOf(rows[0])');assert.equal(sent.at,pending.sent.at);assert.equal(resumed.control.presses,0);assert.equal(resumed.run('sheet.charms.length'),4);
 });
 await check('metadata-only historical popup hydration exposes original files without live intake or animations',async()=>{
  const f=fixture();await f.run('CustomSheet.send(it,{from:button})');const record=JSON.parse(JSON.stringify(f.control.cloud.get(f.run('e.ck'))));
  const history=fixture();history.c.historyJSON=JSON.stringify(record);history.run('B.customDesigns={};B.orders.rows=[];B.orders.byKey=new Map();const historicalRow={...rows[0]};CustomSheet.hydrate([JSON.parse(historyJSON)]);');
  assert.equal(history.run('CustomSheet.decisionOf(historicalRow).by'),'Paul');assert.equal(history.run('CustomSheet.cardOf({key:"csent:changed-group",row:historicalRow}).files[0].cloud.path'),record.files[0].cloud.path);assert.equal(history.control.poolCalls.length,0);assert.equal(history.control.presses,0);assert.equal(history.control.cloudCalls.length,0);
 });
 await check('known-key lookup coalesces and throttles but explicit reconnect refresh is permitted',async()=>{
  const f=fixture();await Promise.all([f.run('CustomSheet.load()'),f.run('CustomSheet.load()')]);await f.run('CustomSheet.load()');assert.equal(f.control.cloudCalls.filter(x=>x.op==='customSheetGet').length,1);
  await f.run('CustomSheet.load({force:true})');assert.equal(f.control.cloudCalls.filter(x=>x.op==='customSheetGet').length,2);assert(f.control.cloudCalls.filter(x=>x.op==='customSheetGet').every(x=>Array.isArray(x.keys)&&Array.isArray(x.lineKeys)));
 });
 if(failures.length)throw new Error(failures.map(x=>x.name).join('\n'));console.log('Custom Send persistence resilience OK:',checks.length,'cases');
})().catch(e=>{console.error(e);process.exitCode=1;});
