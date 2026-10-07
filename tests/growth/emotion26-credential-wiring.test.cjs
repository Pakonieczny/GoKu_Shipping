'use strict';
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path');
const core=require('../../netlify/functions/_britesGrowth.js'),demo=require('../../netlify/functions/_britesConciergeDemoTurn.js');
const voice=require('../../netlify/functions/_britesConciergeVoice.js');
const AsyncFunction=Object.getPrototypeOf(async function(){}).constructor;
const source=fs.readFileSync(path.join(__dirname,'../../netlify/functions/britesConciergeDemoTurn.js'),'utf8').replace(/^import \w+ from .*;\s*$/gm,'').replace('export default async(req,context)=>{','return async(req,context)=>{').replace(/export const config=\{[\s\S]*$/,'');
const voiceSource=fs.readFileSync(path.join(__dirname,'../../netlify/functions/britesConciergeVoice.js'),'utf8').replace(/^import \w+ from .*;\s*$/gm,'').replace('export default async(req)=>{','return async(req)=>{').replace(/export const config=\{[\s\S]*$/,'');
const request=body=>new Request('https://preview.test/api/concierge-demo-turn',{method:'POST',headers:{Origin:'https://preview.test','Content-Type':'application/json'},body:JSON.stringify(body)});
async function fixture(values={},automatic={}){
  const settings={BRITES_GROWTH_NAMESPACE:'Brites_Growth_Sandbox',BRITES_CONCIERGE_REALTIME_DEMO_USD_CAP:'10',...values},configured=[],calls=[],allocations=[];
  const mockDemo={...demo,createGatewayTone:options=>{
    configured.push({...options});
    if(!demo.createGatewayTone(options))return null;
    return async input=>{calls.push({config:options,input});return JSON.stringify({reply:'We can chat at your pace.',tone:'warm'});};
  }};
  const service={rateLimit:async()=>true};
  const handler=await new AsyncFunction('core','demo','voice','Netlify','globalThis',source)({...core,makeDb:()=>({}),createGrowthService:()=>service},mockDemo,{createDemoBudgetReservation:(actual,options)=>{assert.equal(actual,service);assert.deepEqual(options,{capUsd:10});return async(...args)=>{allocations.push(args);return {allocatedUsd:.05};};}},{env:{get:name=>settings[name]}},{process:{env:automatic}});
  return {handler,configured,calls,allocations};
}
// Actual voice wrapper and handler; storage, authentication and transport are
// synthetic. A diagnostic read must not run inference or change held money.
async function disabledVoiceFixture(){
  const settings={BRITES_GROWTH_NAMESPACE:'Brites_Growth_Sandbox',BRITES_GROWTH_ADMIN_KEY:'synthetic-operator-key',BRITES_CONCIERGE_REALTIME_ENABLED:'0',BRITES_CONCIERGE_REALTIME_DEMO_USD_CAP:'10',BRITES_CONCIERGE_REALTIME_RESERVE_USD:'1'};
  const counts={db:0,service:0,authReads:0,reads:0,writes:0,setup:0,provider:0},queries=[];
  const rows=new Map([
    ['VoiceUsage/preview-budget',{reservedCents:1000,spentCents:0,calls:10,allocationCapCents:1000}],
    ['VoiceUsage/session-'+('a'.repeat(64)),{allocatedCents:100,startedAt:1000000,reconcile:'provider_evidence_required'}],
    ['VoiceDeadlines/rtc_synthetic',{callId:'rtc_synthetic',at:1000000,expiresAt:1120000,state:'closed',closedAt:1120000}],
    ['VoiceDiagnostics/last-start',{at:1000000,stage:'provider',providerStatus:429,code:'insufficient_quota'}]
  ]);
  const forbiddenWrite=()=>{counts.writes++;throw Error('Unexpected synthetic storage write.');};
  const db={collection(name){assert.equal(name,'config');return {doc(id){assert.equal(id,'editPasscode');return {async get(){counts.authReads++;return {exists:true,data:()=>({passcode:'synthetic-edit-passcode'})};}};}};}};
  const service={namespace:'Brites_Growth_Sandbox',async setup(){counts.setup++;throw Error('Unexpected voice runtime setup.');},col(name){
    return {doc(id){const key=name+'/'+id;return {async get(){counts.reads++;return {exists:rows.has(key),data:()=>structuredClone(rows.get(key))};},set:forbiddenWrite};},orderBy(field,direction){return {limit(limit){queries.push({name,field,direction,limit});return {async get(){counts.reads++;const docs=[...rows].filter(([key,value])=>key.startsWith(name+'/')&&Object.hasOwn(value,field)).sort((a,b)=>direction==='desc'?b[1][field]-a[1][field]:a[1][field]-b[1][field]).slice(0,limit).map(([key,value])=>({id:key.slice(name.length+1),data:()=>structuredClone(value)}));return {docs,size:docs.length};}};}};}};
  }};
  const provider=async()=>{counts.provider++;throw Error('Unexpected synthetic provider call.');};
  const handler=await new AsyncFunction('core','voice','Netlify','fetch',voiceSource)(
    {...core,makeDb:()=>{counts.db++;return db;},createGrowthService:options=>{assert.equal(options.db,db);counts.service++;return service;}},
    {...voice,createHandler:options=>voice.createHandler({...options,fetch:provider})},
    {env:{get:name=>settings[name]}},provider
  );
  const request=(body,headers={})=>new Request('https://preview.test/api/concierge-voice',{method:'POST',headers:{Origin:'https://preview.test','Content-Type':'application/json',...headers},body:JSON.stringify(body)});
  return {handler,request,counts,queries,rows};
}
test('dedicated concierge credential alone enables capabilities and bounded general conversation on canonical OpenAI endpoint',async()=>{
  const f=await fixture({BRITES_CONCIERGE_OPENAI_API_KEY:'synthetic-dedicated-secret'});
  const capabilities=await (await f.handler(request({action:'capabilities'}),{})).json();
  assert.equal(capabilities.aiAvailable,true);assert.equal(f.calls.length,0);assert.equal(f.allocations.length,0);
  const response=await f.handler(request({action:'conversation',message:'Can we chat?'}),{}),value=await response.json();
  assert.equal(value.aiUsed,true);assert.equal(f.calls.length,1);assert.deepEqual(f.calls[0].config,{base:'https://api.openai.com/v1',key:'synthetic-dedicated-secret'});
  assert.equal(f.calls[0].input.model,'gpt-4o-mini');assert.equal(f.calls[0].input.maxTokens,180);assert.equal(f.allocations[0][0],.05);
  assert.equal(JSON.stringify([capabilities,value]).includes('synthetic-dedicated-secret'),false);
});
test('a complete gateway credential pair retains priority over dedicated OpenAI credentials',async()=>{
  const f=await fixture({NETLIFY_AI_GATEWAY_URL:'https://gateway.example/v1',NETLIFY_AI_GATEWAY_KEY:'synthetic-gateway-secret',BRITES_CONCIERGE_OPENAI_API_KEY:'synthetic-dedicated-secret'});
  await f.handler(request({action:'conversation',message:'Can we chat?'}),{});
  assert.deepEqual(f.calls[0].config,{base:'https://gateway.example/v1',key:'synthetic-gateway-secret'});assert.equal(f.configured.length,1);
});
test('automatic runtime gateway pair has the existing priority over configured gateway pair',async()=>{
  const f=await fixture({NETLIFY_AI_GATEWAY_URL:'https://configured.example/v1',NETLIFY_AI_GATEWAY_KEY:'configured-secret',BRITES_CONCIERGE_OPENAI_API_KEY:'synthetic-dedicated-secret'},{NETLIFY_AI_GATEWAY_URL:'https://runtime.example/v1',NETLIFY_AI_GATEWAY_KEY:'runtime-secret'});
  await f.handler(request({action:'conversation',message:'Can we chat?'}),{});
  assert.deepEqual(f.calls[0].config,{base:'https://runtime.example/v1',key:'runtime-secret'});
});
for(const settings of [{NETLIFY_AI_GATEWAY_URL:'https://gateway.example/v1'},{NETLIFY_AI_GATEWAY_KEY:'synthetic-gateway-secret'},{NETLIFY_AI_GATEWAY_URL:'http://gateway.example/v1',NETLIFY_AI_GATEWAY_KEY:'synthetic-gateway-secret'}])test('incomplete/invalid gateway uses dedicated key only on OpenAI endpoint: '+JSON.stringify(Object.keys(settings)),async()=>{
  const f=await fixture({...settings,BRITES_CONCIERGE_OPENAI_API_KEY:'synthetic-dedicated-secret'});
  await f.handler(request({action:'conversation',message:'Can we chat?'}),{});
  assert.deepEqual(f.calls[0].config,{base:'https://api.openai.com/v1',key:'synthetic-dedicated-secret'});
  assert.equal(f.configured.some(c=>c.base==='https://gateway.example/v1'&&c.key==='synthetic-dedicated-secret'),false);
});
test('existing complete OpenAI custom-base pair remains supported without borrowing a gateway credential',async()=>{
  const f=await fixture({OPENAI_BASE_URL:'https://legacy.example/v1',OPENAI_API_KEY:'synthetic-legacy-secret',NETLIFY_AI_GATEWAY_URL:'https://gateway.example/v1',BRITES_CONCIERGE_OPENAI_API_KEY:'synthetic-dedicated-secret'});
  await f.handler(request({action:'conversation',message:'Can we chat?'}),{});
  assert.deepEqual(f.calls[0].config,{base:'https://legacy.example/v1',key:'synthetic-legacy-secret'});
});
test('no qualifying endpoint-key pair exposes no AI capability and does not reserve or infer',async()=>{
  const f=await fixture({NETLIFY_AI_GATEWAY_URL:'https://gateway.example/v1',OPENAI_API_KEY:'synthetic-unpaired-secret'});
  const capabilities=await (await f.handler(request({action:'capabilities'}),{})).json();assert.equal(capabilities.aiAvailable,false);
  const value=await (await f.handler(request({action:'conversation',message:'Can we chat?'}),{})).json();assert.equal(value.aiUsed,false);assert.equal(value.providerUnavailable,true);assert.equal(f.calls.length,0);assert.equal(f.allocations.length,0);
  assert.equal(JSON.stringify([capabilities,value]).includes('synthetic-unpaired-secret'),false);
});
test('dedicated credential wiring leaves fast hello provider-free and refuses cross-origin inference',async()=>{
  const f=await fixture({BRITES_CONCIERGE_OPENAI_API_KEY:'synthetic-dedicated-secret'});
  const value=await (await f.handler(request({action:'conversation',message:'Hello'}),{})).json();assert.equal(value.aiUsed,false);assert.equal(f.calls.length,0);assert.equal(f.allocations.length,0);
  const response=await f.handler(new Request('https://preview.test/api/concierge-demo-turn',{method:'POST',headers:{Origin:'https://elsewhere.test'},body:JSON.stringify({action:'conversation',message:'Can we chat?'})}),{});
  assert.equal(response.status,403);assert.equal(f.calls.length,0);assert.equal(f.allocations.length,0);
});
test('disabled voice capabilities and shopper actions do not initialize storage, provider access or allocations',async()=>{
  const f=await disabledVoiceFixture();
  for(const action of ['capabilities','start','stop','readiness']){
    const response=await f.handler(f.request({action})),value=await response.json();
    assert.equal(response.status,action==='capabilities'?200:503);assert.equal(value.enabled,false);assert.equal(value.code,'VOICE_DISABLED');
  }
  assert.deepEqual(f.counts,{db:0,service:0,authReads:0,reads:0,writes:0,setup:0,provider:0});assert.deepEqual(f.queries,[]);
});
for(const [header,credential,authReads] of [['X-Growth-Key','synthetic-operator-key',0],['X-Edit-Passcode','synthetic-edit-passcode',1]])test('disabled voice allows fixed operator allocation read through '+header+' without a provider credential',async()=>{
  const f=await disabledVoiceFixture(),before=structuredClone([...f.rows]);
  const response=await f.handler(f.request({action:'allocation'},{[header]:credential})),value=await response.json();
  assert.equal(response.status,200);assert.equal(response.headers.get('Cache-Control'),'no-store');assert.equal(value.readOnly,true);assert.equal(value.evidenceComplete,false);
  assert.equal(value.historical,true);assert.equal(value.nativeVoiceUsesAllocation,false);assert.equal(value.budget.reservedCents,1000);assert.equal(value.budget.recordedLimitCents,1000);assert.equal(value.budget.configurationValid,false);assert.equal(value.budget.nextReservationFits,null);assert.equal(value.recordWindow.limit,50);assert.equal(value.deadlineWindow.limit,20);assert.equal(value.recordWindow.unattributed,1);
  assert.deepEqual(value.lastStart,{at:1000000,stage:'provider',providerStatus:429,code:'insufficient_quota'});
  assert.deepEqual(f.queries,[{name:'VoiceUsage',field:'startedAt',direction:'desc',limit:50},{name:'VoiceDeadlines',field:'at',direction:'desc',limit:20},{name:'VoiceContinuations',field:'issuedAt',direction:'desc',limit:50}]);
  assert.deepEqual(f.counts,{db:1,service:1,authReads,reads:7,writes:0,setup:0,provider:0});assert.deepEqual([...f.rows],before);
  assert.equal(value.providerCalls,0);assert.equal(value.financialWrites,0);assert.equal(value.refunds,0);assert.doesNotMatch(JSON.stringify(value),/synthetic-operator-key|synthetic-edit-passcode/);
});
test('disabled voice rejects non-fixed allocation bodies before initializing diagnostic storage',async()=>{
  for(const extra of [{limit:1000},{refund:true},{token:'synthetic-token'},{padding:'x'.repeat(66000)}]){
    const f=await disabledVoiceFixture(),response=await f.handler(f.request({action:'allocation',...extra},{'X-Growth-Key':'synthetic-operator-key'}));
    assert.ok([400,413].includes(response.status));assert.deepEqual(f.counts,{db:0,service:0,authReads:0,reads:0,writes:0,setup:0,provider:0});assert.deepEqual(f.queries,[]);
  }
});
