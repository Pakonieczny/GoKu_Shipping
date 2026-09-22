'use strict';
const assert=require('node:assert/strict'),fs=require('node:fs'),vm=require('node:vm');
const bridge=fs.readFileSync('charm-nest-bridge.js','utf8'),station=fs.readFileSync('design-1.html','utf8'),server=fs.readFileSync('netlify/functions/charmNestLibrary.js','utf8');
(async()=>{
 // Run actual handler code against a store that rejects collection downloads.
 let aggregates=0,documentReads=0;
 const query=()=>({where:()=>query(),count:()=>({get:async()=>{aggregates++;return{data:()=>({count:420})}}}),get:async()=>{throw Error('Full collection read forbidden');},doc:()=>({get:async()=>{documentReads++;return {exists:false}}})});
 const c={Promise,Map,Date,db:{collection:query,getAll:async()=>[]},col:query,SHEETS:'sheets',LIB:'charms',SANDBOX:'sandbox',SANDBOXED:['pool','sheets','runs'],num:Number};vm.createContext(c);
 for(const [start,end] of [['async function op_ping','async function op_lookupCharms'],['async function op_sandboxStatus','async function op_sandboxReset'],['async function op_arrivalRecord','// ── runs']])vm.runInContext(server.slice(server.indexOf(start),server.indexOf(end)),c);
 const counts=await c.op_ping({calibration:false});assert.equal(counts.sheets,420);assert.equal(aggregates,2);assert.equal(documentReads,0);
 await c.op_sandboxStatus();assert.equal(documentReads,1);assert.equal(aggregates,5);
 const arrivals=await c.op_arrivalRecord({orders:[]});assert.equal(arrivals.count24,420);assert.equal(arrivals.count1,420);assert.equal(aggregates,7);assert.equal(documentReads,1,'no downloads of all historical arrivals');
 // Simultaneous sandbox receipt calls share one metadata read and Storage download.
 let metaReads=0,downloads=0;const sandbox={exports:{},console,Date,require:name=>{
  if(name==='./firebaseAdmin')return{firestore:()=>({collection:()=>({doc:()=>({get:async()=>{metaReads++;return{exists:true,data:()=>({path:'snapshot.json'})}}})})}),storage:()=>({bucket:()=>({file:()=>({download:async()=>{downloads++;return[Buffer.from(JSON.stringify([{receipt_id:123,transactions:[]}]))]}})})})};
  if(name==='./_charmNestAuth')return{CORS:{}};throw Error('Unexpected dependency '+name);
 }};vm.runInNewContext(fs.readFileSync('netlify/functions/etsySandbox.js','utf8'),sandbox);
 await Promise.all(Array.from({length:100},()=>sandbox.exports.handler({queryStringParameters:{fn:'etsyOrderProxy',orderId:'123'}})));assert.equal(metaReads,1);assert.equal(downloads,1);
 // Browser photo cache survives reload and shields BOTH services.
 const loader=station.slice(station.indexOf('const __imagesCache'),station.indexOf('/** Route an external image'));const saved=new Map();let requests=0;
 const page=()=>{const ctx={Map,NS:':sandbox',Date,JSON,FN:'/fn',localStorage:{getItem:k=>saved.get(k),setItem:(k,v)=>saved.set(k,v)},runImageTask:fn=>fn(),fetch:async()=>{requests++;return{ok:true,status:200,json:async()=>({results:[{url_570xN:'saved.jpg'}]})}}};vm.runInNewContext(loader+';this.load=fetchListingImages;',ctx);return ctx;};
 await page().load('123');await page().load('123');assert.equal(requests,1,'reload reuses saved photos without function/Firebase calls');
 // Only visible production cards can poll; quiet/hidden tabs make zero reads.
 const visible={_laserSheets:['visible'],_laserSet:{setId:'set'},getBoundingClientRect:()=>({top:10,bottom:200,width:100,height:190}),querySelectorAll:()=>[]};const offscreen={...visible,_laserSheets:['offscreen'],getBoundingClientRect:()=>({top:2000,bottom:2200,width:100,height:200})};
 const p={polling:false,lastPoll:0,Date:{now:()=>1000000},S:{mode:'library',cloud:{ok:true}},document:{hidden:false,querySelectorAll:()=>[visible,offscreen]},innerHeight:800,Set,records:new Map(),changed:()=>{},console};let polls=[];p.api=async(_,b)=>{polls.push(b);return {sheets:[],sets:[]}};
 const start=bridge.indexOf('  async function poll(force=false)');vm.runInNewContext(bridge.slice(start,bridge.indexOf('  function saved(sh)',start))+';this.poll=poll;',p);
 await p.poll();await p.poll();assert.equal(polls.length,1);assert.deepEqual(Array.from(polls[0].sheetIds),['visible']);p.document.hidden=true;await p.poll(true);assert.equal(polls.length,1);
 // Known arrivals are not re-read on later checks.
 const a={Date,Object,Set,state:{seen:{}},S:{cloud:{ok:true}},at:id=>a.state.seen[id],interval:()=>600000,save:()=>{},paint:()=>{}};let sent=[];a.api=async(_,b)=>{sent.push(b.orders);return {firstSeen:Object.fromEntries(b.orders.map(o=>[o.id,123])),count24:2,count1:2}};
 const ar=bridge.indexOf('  async function record(orders)',bridge.indexOf('const Arrivals'));vm.runInNewContext(bridge.slice(ar,bridge.indexOf('  function paint()',ar))+';this.record=record;',a);
 await a.record([{receiptId:'123'},{receiptId:'456'}]);await a.record([{receiptId:'123'},{receiptId:'456'}]);assert.equal(sent[0].length,2);assert.equal(sent[1].length,0);assert.equal(a.state.seen['123'],123);
 a.S.cloud.ok=false;await a.record([{receiptId:'789'}]);a.S.cloud.ok=true;await a.record([{receiptId:'789'}]);assert.equal(sent.at(-1)[0].id,'789','offline first arrivals are still recorded when cloud returns');
 console.log('Firebase thrift OK: aggregation-only counts, 100 concurrent sandbox reads share one lookup/download, browser reload cache, visible-only throttled polling, known-arrival reuse. No live Firebase calls.');
})().catch(e=>{console.error(e);process.exitCode=1});
// The linked station's completion checks are viewport-bounded, not all rendered rows.
{
 const rows=Array.from({length:120},(_,i)=>({offsetTop:i*100,offsetHeight:90,dataset:{receipt:String(i)}}));
 const c={document:{hidden:false},$:()=>({scrollTop:1000,clientHeight:400}),$$:()=>rows};
 const start=station.indexOf('function visibleReceiptIds('),end=station.indexOf('function removeCompletedRows',start);
 vm.runInNewContext(station.slice(start,end)+';this.visible=visibleReceiptIds;',c);
 assert.deepEqual(Array.from(c.visible()),Array.from({length:13},(_,i)=>String(i+6)));c.document.hidden=true;assert.equal(c.visible().length,0);
 const startPoll=station.indexOf('async function pollCompletedTargeted'),endPoll=station.indexOf('function startCompletionPolling',startPoll);
 const hidden={document:{hidden:true},visibleReceiptIds:()=>{throw Error('Hidden page scanned orders');},fetch:()=>{throw Error('Hidden page made a request');}};
 vm.runInNewContext(station.slice(startPoll,endPoll)+';this.poll=pollCompletedTargeted;',hidden);hidden.poll().then(r=>assert.equal(r,false));
}
(async()=>{
 const source=fs.readFileSync('netlify/functions/firebaseOrders.js','utf8');
 const start=source.indexOf('      if (event.queryStringParameters?.dcFor)'),end=source.indexOf('      /* ?staffNotesFor',start);
 let queries=0;
 const ctx={CORS:{},COMPLETED_COLL:'completed',parseIds:s=>s.split(','),admin:{firestore:{FieldPath:{documentId:()=> '__name__'}}}};
 ctx.col=()=>({where:(field,op,ids)=>{
   assert.equal(field,'__name__');assert.equal(op,'in');assert(ids.length<=10);
   return {get:async()=>{queries++;return {docs:ids.filter(id=>['3','14'].includes(id)).map(id=>({id}))};}};
 }});

 vm.runInNewContext('this.lookup=async function(event){'+source.slice(start,end)+'};',ctx);
 const result=await ctx.lookup({queryStringParameters:{dcFor:Array.from({length:20},(_,i)=>String(i)).join(',')}});
 assert.equal(queries,2);assert.deepEqual(JSON.parse(result.body).orderNumbers,['3','14']);
})().catch(e=>{console.error(e);process.exitCode=1});

(async()=>{
 const calls=[],ctx={require:()=>({readMany:async(ids,opts)=>Object.fromEntries(ids.map(id=>{calls.push({id,...opts});return [id,{images:opts.cacheOnly?[]:[{url_570xN:'https://i.etsystatic.com/'+id+'.jpg'}],source:opts.cacheOnly?'cache-only':'etsy',etsyCalls:opts.cacheOnly?0:1}];}))})};
 const start=server.indexOf('async function op_listingPhotos'),end=server.indexOf('async function op_ping',start);
 vm.runInNewContext(server.slice(start,end)+';this.photos=op_listingPhotos;',ctx);
 const cached=await ctx.photos({listingIds:['123','123','invalid']});assert.equal(calls.length,1);assert.equal(calls[0].cacheOnly,true);assert.equal(cached.etsyCalls,0);
 const prepared=await ctx.photos({listingIds:['123'],prepare:true});assert.equal(calls[1].cacheOnly,false);assert.equal(prepared.etsyCalls,1);assert.match(prepared.images['123'],/^https:/);
})().catch(e=>{console.error(e);process.exitCode=1});
