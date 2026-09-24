// Real production handlers/transport exercised with controlled Etsy responses,
// not sandbox endpoints. Tokens here are inert fixture strings.
const assert=require('node:assert/strict'),fs=require('node:fs'),vm=require('node:vm');
(async()=>{
 const response=(status,body={},retry)=>({ok:status<400,status,headers:{get:k=>k==='retry-after'?retry:null},json:async()=>body,text:async()=>JSON.stringify(body)});
 for(const name of ['listOpenOrders','etsyOrderProxy']){
   let upstream=response(200,{results:[]}),calls=[];
   const fetch=async(url,init)=>{calls.push({url,init});return upstream;};
   const ctx={exports:{},global:{fetch},require:()=>fetch,URLSearchParams,process:{env:{SHOP_ID:'shop',CLIENT_ID:'key',CLIENT_SECRET:'secret'}}};vm.createContext(ctx);vm.runInContext(fs.readFileSync('netlify/functions/'+name+'.js','utf8'),ctx);
   const req={headers:{'Access-Token':'fixture-token'},queryStringParameters:{offset:'100',orderId:'123'}};
   assert.equal((await ctx.exports.handler({...req,headers:{}})).statusCode,400);assert.equal(calls.length,0,'no unauthenticated upstream call');
   assert.equal((await ctx.exports.handler(req)).statusCode,200);assert.equal(calls[0].init.headers.Authorization,'Bearer fixture-token');
   if(name==='listOpenOrders'){const url=new URL(calls[0].url);assert.equal(url.searchParams.get('offset'),'100');assert.equal(url.searchParams.get('was_paid'),'true');assert.equal(url.searchParams.get('was_shipped'),'false');assert.equal(url.searchParams.get('was_canceled'),'false');}
   if(name==='etsyOrderProxy'){
     upstream=response(200,{receipt_id:123,message_from_buyer:'engrave',transactions:[{transaction_id:9,quantity:1}]});
     const receipt=JSON.parse((await ctx.exports.handler(req)).body);assert.equal(receipt.receipt.receipt_id,123);assert.equal(receipt.receipt_id,123);assert.equal(receipt.transactions[0].transaction_id,9);assert.equal(receipt.receipt.message_from_buyer,'engrave');
   }
   upstream=response(401,{error:'expired'});assert.equal((await ctx.exports.handler(req)).statusCode,401);
   upstream=response(429,{error:'limited'},'17');const limited=await ctx.exports.handler(req);assert.equal(limited.statusCode,429);assert.equal(limited.headers['Retry-After'],'17');
 }
 const station=fs.readFileSync('design-1.html','utf8');let replies=[response(401),response(200)],refreshes=0,waits=[],tokens=[],stored='expired';
 const pauses=new Map();
 const c={ensureFreshToken:async()=>{},localStorage:{getItem:k=>/\.pause$/.test(k)?pauses.get(k)??null:stored,setItem:(k,v)=>pauses.set(k,v),removeItem:()=>{stored='';}},TOKEN_KEYS:{access:'access'},FN:'https://fixture',SANDBOX:false,ETSY_LS:'meter',ETSY_GUARD:{brakeMs:300000},etsyMeter:{brakeUntil:0},Response,isEtsyPath:()=>true,runEtsyTask:fn=>fn(),fetch:async(_,init)=>{tokens.push(init.headers['access-token']);return replies.shift();},refreshAccessToken:async()=>{refreshes++;stored='renewed';},setConn(){},startOAuth(){},sleep:async ms=>waits.push(ms)};
 vm.createContext(c);vm.runInContext(station.slice(station.indexOf('async function apiFetch('),station.indexOf('/** Receipt fetch with retries')),c);
 assert.equal((await c.apiFetch('/listOpenOrders')).status,200);assert.equal(refreshes,1);assert.deepEqual(tokens,['expired','renewed']);
 // a 429 is not retried: its Retry-After pauses every Etsy read of the page, and a read during the pause is not sent
 const before=Date.now();replies=[response(429,{},'17'),response(200)];assert.equal((await c.apiFetch('/listOpenOrders')).status,429);assert.deepEqual(waits,[]);
 assert(c.etsyMeter.brakeUntil>=before+17000&&c.etsyMeter.brakeUntil<=Date.now()+17000,'Retry-After sets the pause');assert.equal(pauses.get('meter.pause'),String(c.etsyMeter.brakeUntil),'and it survives a reload');
 const sent=tokens.length;const paused=await c.apiFetch('/listOpenOrders');assert.equal(paused.status,429);assert.equal(tokens.length,sent,'a paused read never reaches Etsy');assert.equal(paused.headers.get('Retry-After'),'17');
 c.etsyMeter.brakeUntil=0;pauses.clear();
 replies=[response(401),response(401)];await assert.rejects(c.apiFetch('/listOpenOrders'),/Unauthorized/);
 // Snapshot emulator must respect the production paid/open filter, including edited snapshot records.
 const sb=fs.readFileSync('netlify/functions/etsySandbox.js','utf8');const a=sb.indexOf('    if (fn === "listOpenOrders")'),b=sb.indexOf('    if (fn === "etsyOrderProxy")',a);
 const fake={receipts:[{receipt_id:1,is_paid:true},{receipt_id:2,is_paid:false},{receipt_id:3,is_shipped:true},{receipt_id:4,status:'Canceled'}],q:{offset:0},PAGE:100,json:(status,body)=>({status,body})};vm.createContext(fake);vm.runInContext('var page=(()=>{const fn="listOpenOrders";'+sb.slice(a,b)+'})();',fake);assert.equal(fake.page.body.results.length,1);
 console.log('Production Etsy boundaries OK: paid/open filtering, pagination, auth required, 401 refresh, 429 Retry-After pause, and sandbox filter parity');
})().catch(e=>{console.error(e);process.exitCode=1;});
(async()=>{
 const bridge=fs.readFileSync('charm-nest-bridge.js','utf8');const start=bridge.indexOf('  async function revalidate(run, why)'),end=bridge.indexOf('  const STATE_PILL',start);
 const row={key:'1:a',order:{receiptId:'1',updateTs:1},line:{transactionId:'a',quantity:1},spec:{quantity:1,designSku:'A',material:'gold',problems:[]},problems:[],state:'written',poolIds:['p'],engrave:{needed:false,approved:true}};
 let mode='list-fail',invalidated=0,review=0;
 const c={rowsOf:()=>[row],DesignLink:{etsyBudgetOk:()=>true,meter(){},call:async type=>{if(type==='orders.check'){if(mode==='list-fail')throw Error('disconnected');return{orders:{'1':{open:true,touched:true}}};}if(mode==='detail-fail')throw Error('detail unavailable');if(mode==='malformed')return{};if(mode==='gone')return{gone:true,reason:'cancelled'};return{order:{receiptId:'1',updateTs:2,lines:[{transactionId:'a',quantity:2}]}};}},O:{interpretLine:(_,line)=>({quantity:line.quantity,designSku:'A',material:'gold',problems:[]})},ctx:()=>({}),Review:{add:()=>review++,syncOrderItems(){}},Engrave:{invalidate:()=>invalidated++},agent(){},agentLiveLine(){},render(){}};
 // a sheet still filling and one already released, each holding a piece of the order that will be cancelled
 const filling={metal:'gold',status:'complete',charms:[{id:'c1',poolId:'p'},{id:'c2',poolId:'x'}],placements:[{id:'c1'},{id:'c2'}]};
 const released={metal:'silver',status:'complete',releaseFull:true,charms:[{id:'c3',poolId:'q'}],placements:[{id:'c3'}]};
 const dirty=[],updates=[];
 Object.assign(c,{allSheets:()=>[filling,released],LiveNest:{closed:sh=>!!sh.releaseFull},sheetDirty:sh=>dirty.push(sh),renderCard:()=>{},sheetName:sh=>sh.metal,
   Pool:{update:async(ids,patch)=>updates.push([ids,patch])},B:{pool:{rows:new Map([['p',{}],['q',{}],['x',{}]])}}});c.window={LiveNest:c.LiveNest};
 vm.createContext(c);vm.runInContext(bridge.slice(start,end),c);
 const reads=[],call=c.DesignLink.call;c.DesignLink.call=async(type,a)=>{reads.push(type==='orders.check'?'check:'+a.receiptIds.join(','):type);return call(type,a);};
 await assert.rejects(c.revalidate(null,'test'),/Cannot verify Etsy open orders/);
 mode='detail-fail';await assert.rejects(c.revalidate(null,'test'),/Cannot verify Etsy order/);
 mode='malformed';await assert.rejects(c.revalidate(null,'test'),/no verifiable state/);assert.equal(row.state,'written','an unreadable response cannot delete an order');
 mode='changed';await c.revalidate(null,'test');assert.equal(row.changePending,true);assert.equal(row.repoolChanged,true);assert.equal(invalidated,1);assert.equal(review,1);
 assert.equal(dirty.length,0,'an order that is still open leaves every sheet as it is');
 row.poolIds=['p','q'];mode='gone';await c.revalidate(null,'test');assert.equal(row.state,'gone');
 // the cancelled order comes off the sheet that is still filling, so its piece is not cut; the other charms stay where
 // they are (Paul, 24 Sep: a sheet is never arranged again) and the sheet is written again without it
 assert.deepEqual(filling.charms.map(x=>x.id),['c2']);assert.deepEqual(filling.placements.map(x=>x.id),['c2']);assert.deepEqual(dirty,[]);
 assert(filling.appendOnly&&filling.dirty&&filling.status==='ready','the rest stay where they are and the sheet is written again');
 assert.equal(JSON.stringify(updates),JSON.stringify([[['p'],{state:'abandoned',sheetId:null,setId:null}]]));assert(!c.B.pool.rows.has('p'));
 // a piece on a released sheet stays: it is cut with the sheet and set aside, and the order no longer holds the sheet back
 assert.deepEqual(released.charms.map(x=>x.id),['c3']);assert.equal(JSON.stringify(row.poolIds),'["q"]');assert(c.B.pool.rows.has('q'));
 reads.length=0;await c.revalidate(null,'test');assert.equal(dirty.length,0,'a second check takes nothing more off');assert.deepEqual(filling.placements.map(x=>x.id),['c2']);
 assert.deepEqual(reads,['check:'],'and an order found gone is not read from Etsy again: '+reads.join(' '));
 // A committed line is cut and its order design-complete at the station, which lists it as no longer open. It is not read
 // from Etsy again, so its shipping or a later edit cannot send a finished order back to review or mark it gone.
 const done={...row,key:'2:b',order:{receiptId:'2',updateTs:1},state:'committed',poolIds:['z'],changePending:false};
 c.rowsOf=()=>[row,done];reads.length=0;await c.revalidate(null,'test');
 assert.deepEqual(reads,['check:'],'a committed order is not checked or read again: '+reads.join(' '));assert.equal(done.state,'committed');
 // An order with a line still open is read as before, and only that line follows what Etsy says.
 const cutLine={...done,key:'4:c',order:{receiptId:'4',updateTs:1},poolIds:[]},openLine={...cutLine,key:'4:d',state:'written'};
 c.rowsOf=()=>[row,done,cutLine,openLine];reads.length=0;await c.revalidate(null,'test');
 assert.deepEqual(reads,['check:4','orders.detail']);assert.equal(openLine.state,'gone');assert.equal(cutLine.state,'committed','the line already cut stays committed');
 console.log('Order revalidation OK: failed reads block release, changed quantity requires re-pooling/review, cancelled receipts leave production and the sheets still filling, and committed orders are not read again');
})().catch(e=>{console.error(e);process.exitCode=1;});

(async()=>{
 const station=fs.readFileSync('design-1.html','utf8');let status=500,body={};
 const c={apiFetch:async()=>({ok:status===200,status,json:async()=>body}),resolveTxMetal(){},console:{error(){}}};vm.createContext(c);
 vm.runInContext(station.slice(station.indexOf('async function pullEtsyOrderDetails('),station.indexOf('/* Listing images')),c);
 await assert.rejects(c.pullEtsyOrderDetails('1',{strict:true}),/500/);
 status=404;assert.equal((await c.pullEtsyOrderDetails('1',{strict:true})).gone,true);
 status=200;await assert.rejects(c.pullEtsyOrderDetails('1',{strict:true}),/incomplete/);
 body={receipt:{receipt_id:'1'},transactions:[]};assert.equal((await c.pullEtsyOrderDetails('1',{strict:true})).receipt.receipt_id,'1');
 console.log('Station receipt reads OK: missing is distinct from failed or malformed');
})().catch(e=>{console.error(e);process.exitCode=1;});
