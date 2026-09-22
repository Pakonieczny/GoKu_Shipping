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
 const c={ensureFreshToken:async()=>{},localStorage:{getItem:()=>stored,removeItem:()=>{stored='';}},TOKEN_KEYS:{access:'access'},FN:'https://fixture',isEtsyPath:()=>true,runEtsyTask:fn=>fn(),fetch:async(_,init)=>{tokens.push(init.headers['access-token']);return replies.shift();},refreshAccessToken:async()=>{refreshes++;stored='renewed';},setConn(){},startOAuth(){},sleep:async ms=>waits.push(ms)};
 vm.createContext(c);vm.runInContext(station.slice(station.indexOf('async function apiFetch('),station.indexOf('/** Receipt fetch with retries')),c);
 assert.equal((await c.apiFetch('/listOpenOrders')).status,200);assert.equal(refreshes,1);assert.deepEqual(tokens,['expired','renewed']);
 replies=[response(429,{},'17'),response(200)];await c.apiFetch('/listOpenOrders');assert.deepEqual(waits,[17000]);
 replies=[response(401),response(401)];await assert.rejects(c.apiFetch('/listOpenOrders'),/Unauthorized/);
 // Snapshot emulator must respect the production paid/open filter, including edited snapshot records.
 const sb=fs.readFileSync('netlify/functions/etsySandbox.js','utf8');const a=sb.indexOf('    if (fn === "listOpenOrders")'),b=sb.indexOf('    if (fn === "etsyOrderProxy")',a);
 const fake={receipts:[{receipt_id:1,is_paid:true},{receipt_id:2,is_paid:false},{receipt_id:3,is_shipped:true},{receipt_id:4,status:'Canceled'}],q:{offset:0},PAGE:100,json:(status,body)=>({status,body})};vm.createContext(fake);vm.runInContext('var page=(()=>{const fn="listOpenOrders";'+sb.slice(a,b)+'})();',fake);assert.equal(fake.page.body.results.length,1);
 console.log('Production Etsy boundaries OK: paid/open filtering, pagination, auth required, 401 refresh, 429 Retry-After, and sandbox filter parity');
})().catch(e=>{console.error(e);process.exitCode=1;});
(async()=>{
 const bridge=fs.readFileSync('charm-nest-bridge.js','utf8');const start=bridge.indexOf('  async function revalidate(run, why)'),end=bridge.indexOf('  const STATE_PILL',start);
 const row={key:'1:a',order:{receiptId:'1',updateTs:1},line:{transactionId:'a',quantity:1},spec:{quantity:1,designSku:'A',material:'gold',problems:[]},problems:[],state:'written',poolIds:['p'],engrave:{needed:false,approved:true}};
 let mode='list-fail',invalidated=0,review=0;
 const c={rowsOf:()=>[row],DesignLink:{etsyBudgetOk:()=>true,meter(){},call:async type=>{if(type==='orders.check'){if(mode==='list-fail')throw Error('disconnected');return{orders:{'1':{open:true,touched:true}}};}if(mode==='detail-fail')throw Error('detail unavailable');if(mode==='malformed')return{};if(mode==='gone')return{gone:true,reason:'cancelled'};return{order:{receiptId:'1',updateTs:2,lines:[{transactionId:'a',quantity:2}]}};}},O:{interpretLine:(_,line)=>({quantity:line.quantity,designSku:'A',material:'gold',problems:[]})},ctx:()=>({}),Review:{add:()=>review++,syncOrderItems(){}},Engrave:{invalidate:()=>invalidated++},agent(){},agentLiveLine(){},render(){}};
 vm.createContext(c);vm.runInContext(bridge.slice(start,end),c);
 await assert.rejects(c.revalidate(null,'test'),/Cannot verify Etsy open orders/);
 mode='detail-fail';await assert.rejects(c.revalidate(null,'test'),/Cannot verify Etsy order/);
 mode='malformed';await assert.rejects(c.revalidate(null,'test'),/no verifiable state/);assert.equal(row.state,'written','an unreadable response cannot delete an order');
 mode='changed';await c.revalidate(null,'test');assert.equal(row.changePending,true);assert.equal(row.repoolChanged,true);assert.equal(invalidated,1);assert.equal(review,1);
 mode='gone';await c.revalidate(null,'test');assert.equal(row.state,'gone');
 console.log('Order revalidation OK: failed reads block release, changed quantity requires re-pooling/review, and cancelled receipts leave production');
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
