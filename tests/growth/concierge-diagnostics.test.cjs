'use strict';
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path');
const diagnostics=require('../../netlify/functions/_britesConciergeDiagnostics'),core=require('../../netlify/functions/_britesGrowth'),policy=require('../../netlify/functions/_britesConcierge');
const source=fs.readFileSync(path.join(__dirname,'../../netlify/functions/britesConcierge.js'),'utf8')
  .replace(/^import (?:core|claude|policy|diagnostics) from .*;\s*$/gm,'')
  .replace('export default async (req,context) => {','return async (req,context) => {')
  .replace(/export const config = [\s\S]*$/,'');
const PRIVATE='PRIVATE_SHOPPER_CREDENTIAL_EXCEPTION_AND_URL';
function secretError(code){const error=new Error(PRIVATE+' https://private.example/secret');if(code!==undefined)error.code=code;error.stack=PRIVATE;return error;}
function product(){return {id:'gid://shopify/Product/1',handle:'bunny-necklace',url:'https://britesjewelry.com/products/bunny-necklace',title:'Bunny Necklace',description:'Includes a sterling silver chain.',type:'Necklace',tags:['bunny'],currency:'USD',options:[{name:'Necklace Length',values:['18 Inches']}],variants:[{id:'gid://shopify/ProductVariant/101',numericId:'101',title:'Sterling Silver / 18 Inches',price:54,available:true,options:[{name:'Metal',value:'Sterling Silver'}]}],variantsComplete:true,checkedAt:Date.now()};}
function fixture({failure=null,error=secretError(),diagnosticFailure=false,diagnosticHang=false,messageHang=false,allowed=true,partialPolicy=false,conciergeFailure=false}={}){
  const calls={},writes=[],attempts=[],products=[product()];
  function touch(stage){calls[stage]=(calls[stage]||0)+1;if(failure===stage)throw error;}
  const collection=name=>({doc:id=>({set:async record=>{touch('diagnostic_write');attempts.push({name,id,record});if(diagnosticFailure)throw secretError();if(diagnosticHang)return new Promise(()=>{});writes.push({name,id,record});}})});
  const db={collection};
  const service={namespace:'Brites_Growth_Sandbox',col:suffix=>collection('Brites_Growth_Sandbox_'+suffix),rateLimit:async()=>{touch('rate_limit');return allowed;},setup:async()=>{touch('control_state');return {aiEnabled:false};},saveProducts:async()=>touch('catalogue_persist'),productIssues:async()=>{touch('holds_read');return [];},research:async()=>{touch('knowledge_read');return [];},event:async type=>{touch(type==='message'?'message_event':'client_event');if(type==='message'&&messageHang)return new Promise(()=>{});return {ok:true};}};
  const shopify={search:async()=>{touch('catalogue_search');return {products};},byHandle:async()=>{touch('catalogue_product');return products[0];}};
  const injectedCore={...core,makeDb:()=>{touch('storage_connect');return db;},createShopify:()=>{touch('catalogue_connect');return shopify;},createGrowthService:()=>{touch('service_connect');return service;},concierge:async args=>{touch('conversation');if(conciergeFailure)throw error;return core.concierge(args);}};
  const injectedPolicy={...policy,createPolicyGuide:()=>({answer:async args=>{touch('policy_read');if(partialPolicy)return policy.unavailableAnswer(args);return {reply:'The published policy lists production separately from transit.',question:'Which country is the gift going to?',policyOnly:policy.classify(args.message,args.history).policyOnly,policyKnowledge:{status:'verified',sources:[{title:'Shipping policy',url:policy.POLICIES.shipping.url,checkedAt:Date.now()}]},policyUnavailable:false,policyLinks:[{label:'Shipping policy',url:policy.POLICIES.shipping.url}]};}})};
  const handler=new Function('core','claude','policy','diagnostics','Netlify',source)(injectedCore,{createClaudeClient:()=>{touch('paid_model');throw secretError();}},injectedPolicy,{...diagnostics,createRecorder:()=>diagnostics.createRecorder({writeTimeoutMs:5,optionalTimeoutMs:5})},{env:{get:key=>key==='BRITES_GROWTH_NAMESPACE'?'Brites_Growth_Sandbox':PRIVATE}});
  return {handler,calls,writes,attempts};
}
async function call(f,body={message:'Find a silver bunny necklace under $60 USD'},context={ip:PRIVATE}){
  const response=await f.handler(new Request('https://brites-growth-sandbox.netlify.app/api/concierge',{method:'POST',headers:{Origin:'https://britesjewelry.com','Content-Type':'application/json'},body:JSON.stringify({...body,privateOwnerData:PRIVATE})}),context);
  return {status:response.status,body:await response.json(),headers:Object.fromEntries(response.headers)};
}
const stage=(record,name)=>record.stages.find(x=>x.stage===name);
function assertPrivateSafe(value){assert.ok(!JSON.stringify(value).includes(PRIVATE));assert.ok(!JSON.stringify(value).includes('private.example'));}
function assertPublicFailure(r){assert.equal(r.status,503);assert.equal(r.body.retryable,true);assert.deepEqual(Object.keys(r.body).sort(),['error','reference','retryable']);assert.match(r.body.reference,/^[0-9a-f-]{36}$/);assert.equal(r.headers['x-concierge-reference'],r.body.reference);assertPublicSafe(r);}
function assertPublicSafe(r){assertPrivateSafe(r);for(const field of ['failureStage','statusCode','categories','durationMs','stages','kind'])assert.equal(Object.hasOwn(r.body,field),false);}

test('successful live gift verification records only timings/enums and opaque correlation',async()=>{
  const f=fixture(),r=await call(f,{message:'Find a silver bunny necklace under $60 USD',preferences:{privateKey:PRIVATE},context:{privateData:PRIVATE}});
  assert.equal(r.status,200);assert.equal(r.body.live,true);assert.equal(r.body.products.length,1);assert.equal(r.body.aiUsed,false);assert.equal(f.calls.paid_model,undefined);assert.equal(f.calls.policy_read,undefined);
  assert.equal(f.writes.length,1);const {name,id,record}=f.writes[0];assert.equal(name,'Brites_Growth_Sandbox_ConciergeDiagnostics');assert.equal(id,r.headers['x-concierge-reference']);assert.equal(record.reference,id);assert.equal(record.outcome,'ok');assert.equal(record.failureStage,null);assert.equal(stage(record,'catalogue_search').completed,1);assert.equal(stage(record,'knowledge_read').completed,1);assertPrivateSafe(record);assertPublicSafe(r);
  assert.deepEqual(Object.keys(record).sort(),['at','durationMs','failureStage','kind','outcome','reference','schema','stages','statusCode']);assert.equal(JSON.stringify(record).includes('bunny'),false);assert.equal(JSON.stringify(record).includes('britesjewelry.com'),false);assert.equal(JSON.stringify(record).includes('shopify'),false);
});

for(const failure of ['storage_connect','catalogue_connect','service_connect','rate_limit','control_state','catalogue_search','catalogue_product','catalogue_persist','holds_read','conversation','client_event'])test('critical '+failure+' failure remains honest and is never retried',async()=>{
  const f=fixture({failure}),body=failure==='catalogue_product'?{message:'Tell me the story of the first necklace',context:{productHandles:['bunny-necklace']}}:failure==='client_event'?{event:'opened'}:undefined,r=await call(f,body);
  assertPublicFailure(r);assert.equal(f.calls[failure],1);assert.equal(f.calls.paid_model,undefined);
  if(failure==='storage_connect'){assert.equal(f.writes.length,0);return;}
  assert.equal(f.writes.length,1);const d=f.writes[0].record;assert.equal(d.outcome,'failed');assert.equal(d.statusCode,503);assert.equal(d.failureStage,failure);assert.equal(stage(d,failure).failed,1);assert.deepEqual(stage(d,failure).categories,['unclassified']);assertPrivateSafe(d);
  if(failure==='catalogue_product')assert.equal(f.calls.catalogue_search,undefined);
  if(failure==='holds_read')assert.equal(f.calls.knowledge_read,undefined);
});

test('optional knowledge failure retains verified live pieces but does not invent stories',async()=>{
  const f=fixture({failure:'knowledge_read',error:secretError(14)}),r=await call(f,{message:'Tell me the story of the first necklace',context:{productHandles:['bunny-necklace']}});
  assert.equal(r.status,200);assert.equal(r.body.live,true);assert.equal(r.body.products.length,1);assert.deepEqual(r.body.meanings,[]);assert.equal(r.body.knowledgeUnavailable,true);assert.match(r.body.reply,/don’t yet have reviewed/);assert.equal(f.calls.knowledge_read,1);assert.equal(f.writes[0].record.outcome,'degraded');assert.deepEqual(stage(f.writes[0].record,'knowledge_read').categories,['unavailable']);assertPrivateSafe(f.writes[0].record);assertPublicSafe(r);
});
test('policy exception is privately classified while public fallback admits missing verification',async()=>{
  const f=fixture({failure:'policy_read',error:secretError('ETIMEDOUT')}),r=await call(f,{message:'Shipping timing to Canada?'});
  assert.equal(r.status,200);assert.equal(r.body.live,false);assert.equal(r.body.policyUnavailable,true);assert.equal(r.body.policyKnowledge.status,'partial');assert.deepEqual(r.body.policyKnowledge.sources,[]);assert.equal(f.calls.catalogue_search,undefined);assert.equal(f.calls.policy_read,1);const d=f.writes[0].record;assert.equal(d.outcome,'degraded');assert.equal(stage(d,'policy_read').failed,1);assert.deepEqual(stage(d,'policy_read').categories,['timeout']);assertPrivateSafe(d);assertPublicSafe(r);
});
test('a partial policy result records unverified coverage without fabricating its underlying cause',async()=>{
  const f=fixture({partialPolicy:true}),r=await call(f,{message:'Shipping timing to Canada?'});assert.equal(r.status,200);assert.equal(r.body.live,false);const p=stage(f.writes[0].record,'policy_read');assert.equal(p.completed,1);assert.equal(p.failed,0);assert.equal(p.degraded,1);assert.deepEqual(p.categories,['policy_unverified']);assert.equal(f.writes[0].record.outcome,'degraded');
});
test('a rejected optional activity write cannot discard a live answer or repeat the write',async()=>{
  const f=fixture({failure:'message_event',error:secretError('PERMISSION_DENIED')}),r=await call(f);assert.equal(r.status,200);assert.equal(r.body.live,true);assert.equal(r.body.products.length,1);assert.equal(f.calls.message_event,1);const d=f.writes[0].record;assert.equal(d.outcome,'degraded');assert.equal(d.failureStage,null);assert.deepEqual(stage(d,'message_event').categories,['access']);assertPublicSafe(r);assertPrivateSafe(d);
});
test('a stalled optional activity write is bounded and cannot hold a verified response indefinitely',{timeout:2000},async()=>{
  const f=fixture({messageHang:true}),r=await call(f);assert.equal(r.status,200);assert.equal(r.body.products.length,1);assert.equal(f.calls.message_event,1);const d=f.writes[0].record;assert.equal(d.outcome,'degraded');assert.equal(stage(d,'message_event').failed,1);assert.deepEqual(stage(d,'message_event').categories,['timeout']);assert.equal(stage(d,'message_event').inFlight,0);assertPrivateSafe(d);
});
for(const mode of ['failure','hang'])for(const originalFailure of [false,true])test('diagnostic '+mode+' cannot replace '+(originalFailure?'an original live-check failure':'a successful answer'),{timeout:2000},async()=>{
  const f=fixture({diagnosticFailure:mode==='failure',diagnosticHang:mode==='hang',failure:originalFailure?'catalogue_search':null,error:secretError('ECONNRESET')}),r=await call(f);
  if(originalFailure)assertPublicFailure(r);else{assert.equal(r.status,200);assert.equal(r.body.live,true);assert.equal(r.body.products.length,1);}
  assert.equal(f.calls.diagnostic_write,1);assert.equal(f.writes.length,0);assert.equal(f.attempts.length,1);assertPrivateSafe(f.attempts[0].record);if(originalFailure){assert.equal(f.attempts[0].record.failureStage,'catalogue_search');assert.deepEqual(stage(f.attempts[0].record,'catalogue_search').categories,['network']);}
});
test('a quota rejection preserves429 and records no live-policy/product/provider work',async()=>{
  const f=fixture({allowed:false}),r=await call(f,{message:'Find bunny jewelry and explain shipping'});assert.equal(r.status,429);assert.equal(f.calls.catalogue_search,undefined);assert.equal(f.calls.policy_read,undefined);assert.equal(f.calls.control_state,undefined);assert.equal(f.calls.paid_model,undefined);assert.equal(f.writes[0].record.outcome,'limited');assert.equal(f.writes[0].record.failureStage,null);assert.equal(f.writes[0].record.statusCode,429);assertPublicSafe(r);
});

test('timings and nested failures preserve the original exception and innermost fixed stage',async()=>{
  let clock=0;const writes=[],r=diagnostics.createRecorder({now:()=>1790916609085,clock:()=>clock});r.attach({namespace:'Brites_Growth_Sandbox',col:suffix=>({doc:id=>({set:async value=>writes.push({suffix,id,value})})})});const error=secretError('ECONNRESET');
  await assert.rejects(r.run('conversation',()=>r.run('catalogue_product',async()=>{clock=17.2;throw error;})),value=>value===error);
  await r.flush({outcome:'failed',statusCode:503,error});const d=writes[0].value;assert.equal(d.durationMs,17);assert.equal(d.failureStage,'catalogue_product');assert.equal(stage(d,'catalogue_product').durationMs,17);assert.equal(stage(d,'conversation').durationMs,17);assert.deepEqual(stage(d,'conversation').categories,['network']);assertPrivateSafe(d);
});
test('concurrent unfinished reads are marked pending rather than reported as failed',async()=>{
  const writes=[],r=diagnostics.createRecorder();r.attach({namespace:'Brites_Growth_Sandbox',col:()=>({doc:()=>({set:async d=>writes.push(d)})})});
  r.run('policy_read',()=>new Promise(()=>{}));const error=secretError();await assert.rejects(r.run('catalogue_search',()=>{throw error;}));await r.flush({outcome:'failed',statusCode:503,error});const p=stage(writes[0],'policy_read');assert.equal(p.inFlight,1);assert.equal(p.failed,0);assert.deepEqual(p.categories,[]);assert.equal(writes[0].failureStage,'catalogue_search');
});
test('diagnostic flush is at most once and refuses unvalidated collection namespaces',async()=>{
  let writes=0;const r=diagnostics.createRecorder();r.attach({namespace:'Brites_Growth_Sandbox',col:suffix=>{assert.equal(suffix,'ConciergeDiagnostics');return {doc:()=>({set:async()=>writes++})};}});await r.run('catalogue_search',async()=>({rawPrivateResult:PRIVATE}));await Promise.all([r.flush(),r.flush(),r.flush()]);assert.equal(writes,1);
  for(const namespace of ['Products','Brites_Growth_Sandbox_Private','Brites_Growth_Sandbox/Secrets',undefined]){const bad=diagnostics.createRecorder();bad.attach({namespace,col:()=>{throw Error('Must not select a collection');}});assert.deepEqual(await bad.flush(),{saved:false});}
});
test('error classification reads only typed fields and never touches messages or stack text',()=>{
  let rawReads=0;const e={code:'ETIMEDOUT'};for(const key of ['message','stack','cause','url'])Object.defineProperty(e,key,{get(){rawReads++;throw Error(PRIVATE);}});assert.equal(diagnostics.errorCategory(e),'timeout');assert.equal(rawReads,0);
  const hostile={};for(const key of ['name','code','status','statusCode'])Object.defineProperty(hostile,key,{get(){throw Error(PRIVATE);}});assert.equal(diagnostics.errorCategory(hostile),'unclassified');assert.equal(diagnostics.errorCategory(PRIVATE),'unclassified');
  for(const [error,category] of [[{name:'TimeoutError'},'timeout'],[{code:8},'rate_limited'],[{status:403},'access'],[{code:'EAI_AGAIN'},'network'],[{statusCode:503},'unavailable'],[{name:'SyntaxError'},'invalid_data'],[{message:'timeout '+PRIVATE},'unclassified']])assert.equal(diagnostics.errorCategory(error),category);
});

function storedRecord(){return {schema:1,kind:'concierge_diagnostic',reference:'b4b9f16b-230c-4035-9c92-1f981f3acabe',at:1790916609085,durationMs:12,outcome:'failed',statusCode:503,failureStage:'catalogue_product',stages:[{stage:'catalogue_product',calls:1,completed:0,failed:1,degraded:0,inFlight:0,durationMs:12,maxDurationMs:12,categories:['network']}]};}
function readDb(documents){const calls=[];return {calls,collection:name=>{calls.push({name});return {orderBy:(field,direction)=>{calls.push({field,direction});return {limit:limit=>{calls.push({limit});return {get:async()=>({docs:documents})};}};}};}};}
const document=data=>({id:data.reference,data:()=>data});
test('private read revalidates every field and excludes malformed/old records and hidden exception data',async()=>{
  const safe=storedRecord(),dirty={...safe,privateMessage:PRIVATE,exception:{message:PRIVATE},failureStage:PRIVATE,durationMs:Infinity,stages:[{...safe.stages[0],durationMs:PRIVATE,categories:['network',PRIVATE,'network'],rawContext:PRIVATE},{stage:PRIVATE,calls:1},{...safe.stages[0],categories:[PRIVATE]}]};
  const db=readDb([document(dirty),document({...safe,reference:PRIVATE}),document({...safe,schema:0}),document({...safe,outcome:PRIVATE}),{id:'00000000-0000-4000-8000-000000000000',data:()=>safe},{id:safe.reference,data:()=>{throw Error(PRIVATE);}}]);
  const result=await diagnostics.read({db,namespace:'Brites_Growth_Sandbox',limit:20});assert.equal(result.diagnostics.length,1);assertPrivateSafe(result);const d=result.diagnostics[0];assert.equal(d.failureStage,null);assert.equal(d.durationMs,0);assert.equal(d.stages.length,1);assert.deepEqual(d.stages[0].categories,['network']);assert.equal(d.stages[0].durationMs,0);assert.deepEqual(db.calls,[{name:'Brites_Growth_Sandbox_ConciergeDiagnostics'},{field:'at',direction:'desc'},{limit:20}]);assert.deepEqual(Object.keys(d.stages[0]).sort(),['calls','categories','completed','degraded','durationMs','failed','inFlight','maxDurationMs','stage']);
});
test('private reads have a fixed collection, bounded row count and reject unknown namespaces before access',async()=>{
  const docs=Array.from({length:60},()=>document(storedRecord())),db=readDb(docs);const r=await diagnostics.read({db,namespace:'Brites_Growth_Live',limit:10000});assert.equal(r.diagnostics.length,50);assert.equal(db.calls[0].name,'Brites_Growth_Live_ConciergeDiagnostics');assert.equal(db.calls[2].limit,50);
  for(const namespace of ['Brites_Growth_Sandbox/Secrets','Secrets',PRIVATE,null])await assert.rejects(diagnostics.read({db,namespace}));assert.equal(db.calls.length,3);
  const negative=readDb(docs);assert.equal((await diagnostics.read({db:negative,namespace:'Brites_Growth_Sandbox',limit:-2})).diagnostics.length,1);
});
test('stored numeric fields and lists are bounded without copying unapproved data',()=>{
  const d=storedRecord();d.durationMs=1e9;d.stages=[{...d.stages[0],calls:1e9,completed:-5,failed:NaN,degraded:Infinity,inFlight:0.6,durationMs:1e9,maxDurationMs:1e9,categories:[PRIVATE,'network','access','network']}];const safe=diagnostics.sanitize(d);assert.equal(safe.durationMs,300000);assert.equal(safe.stages[0].calls,1000);assert.equal(safe.stages[0].completed,0);assert.equal(safe.stages[0].failed,0);assert.equal(safe.stages[0].degraded,0);assert.equal(safe.stages[0].inFlight,1);assert.equal(safe.stages[0].maxDurationMs,300000);assert.deepEqual(safe.stages[0].categories,['network','access']);assertPrivateSafe(safe);
});
