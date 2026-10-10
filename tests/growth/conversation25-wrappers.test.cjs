'use strict';
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path');
const core=require('../../netlify/functions/_britesGrowth.js'),demo=require('../../netlify/functions/_britesConciergeDemoTurn.js');
const AsyncFunction=Object.getPrototypeOf(async function(){}).constructor;
function source(name){return fs.readFileSync(path.join(__dirname,'../../netlify/functions/'+name),'utf8').replace(/^import \w+ from .*;\s*$/gm,'').replace('export default async (req,context) => {','return async (req,context) => {').replace('export default async(req,context)=>{','return async(req,context)=>{').replace(/export const config = [\s\S]*$/,'').replace(/export const config= [\s\S]*$/,'').replace(/export const config=\{[\s\S]*$/,'');}
const req=body=>new Request('https://preview.test/api/concierge',{method:'POST',headers:{Origin:'https://preview.test','Content-Type':'application/json'},body:JSON.stringify(body)});
test('public concierge social turn skips policy, catalogue reads and runtime setup while preserving rate limiting',async()=>{
  const counts={limit:0,message:0},preferences=core.intentFrom('Bunny silver necklace under 60 CAD');
  const recorder={reference:'synthetic',attach(){},run:async(stage,fn)=>fn(),optionalMessage:async fn=>fn(),flush:async()=>{},degraded(){}};
  const service={rateLimit:async()=>{counts.limit++;return true;},event:async()=>{counts.message++;},setup:async()=>{throw Error('Must not inspect AI control for greetings.');}};
  const mockCore={...core,makeDb:()=>({}),createGrowthService:()=>service,createShopify:()=>({search:async()=>{throw Error('Must not search.');},byHandle:async()=>{throw Error('Must not inspect.');}})};
  const handler=await new AsyncFunction('core','claude','policy','diagnostics','shoppingGuide','Netlify',source('britesConcierge.js'))(mockCore,{}, {createPolicyGuide:()=>({answer:async()=>{throw Error('Must not read policies.');}}),classify:()=>{throw Error('Must not classify a policy after a social match.');}}, {createRecorder:()=>recorder},require('../../brites-concierge-shopping-guide.js'),{env:{get:()=>undefined}});
  const response=await handler(req({message:'Hello, how are you?',preferences,context:{productHandles:['bunny-necklace']}}),{ip:'synthetic'}),value=await response.json();
  assert.equal(response.status,200);assert.equal(value.conversationOnly,true);assert.equal(value.needsModelConversation,false);assert.deepEqual(value.preferences,preferences);assert.equal(counts.limit,1);assert.equal(counts.message,1);
});
test('social wrapper still refuses cross-origin, invalid and rate-limited requests before a response',async()=>{
  let storage=0;const recorder={reference:'synthetic',attach(){},run:async(stage,fn)=>fn(),flush:async()=>{},optionalMessage:async()=>{}};
  const mockCore={...core,makeDb:()=>{storage++;return {};},createShopify:()=>({}),createGrowthService:()=>({rateLimit:async()=>false})};
  const handler=await new AsyncFunction('core','claude','policy','diagnostics','shoppingGuide','Netlify',source('britesConcierge.js'))(mockCore,{}, {createPolicyGuide:()=>({})}, {createRecorder:()=>recorder},require('../../brites-concierge-shopping-guide.js'),{env:{get:()=>undefined}});
  assert.equal((await handler(new Request('https://preview.test/api/concierge',{method:'POST',headers:{Origin:'https://evil.test'},body:'{"message":"hello"}'}),{})).status,403);
  assert.equal((await handler(req({message:123}),{})).status,400);assert.equal(storage,0);
  assert.equal((await handler(req({message:'hello'}),{})).status,429);assert.equal(storage,1);
});
test('bounded general-conversation wrapper cannot call its model without the existing preview allocation',async()=>{
  for(const cap of [undefined,'0','26','not-a-number']){
    let calls=0,reservations=0;const mockCore={...core,makeDb:()=>({}),createGrowthService:()=>({rateLimit:async()=>true})};
    const mockDemo={...demo,createGatewayTone:()=>async()=>{calls++;return {reply:'We can chat at your pace.',tone:'warm'};}};
    const native={createDemoBudgetReservation:()=>{reservations++;return async()=>({allocatedUsd:0.05});}};
    const handler=await new AsyncFunction('core','demo','voice','Netlify',source('britesConciergeDemoTurn.js'))(mockCore,mockDemo,native,{env:{get:name=>name==='BRITES_CONCIERGE_REALTIME_DEMO_USD_CAP'?cap:undefined}});
    const response=await handler(req({action:'conversation',message:'Can we chat?'}),{}),value=await response.json();assert.equal(value.aiUsed,false);assert.equal(value.code,'CONVERSATION_ALLOCATION_UNAVAILABLE');assert.equal(calls,0);assert.equal(reservations,0);
  }
});
test('configured general-conversation wrapper uses the same preview allocation without enabling research inference',async()=>{
  let observed,allocations=[];const service={rateLimit:async()=>true};const mockCore={...core,makeDb:()=>({}),createGrowthService:()=>service};
  const mockDemo={...demo,createGatewayTone:()=>async()=>({reply:'We can chat at your pace.',tone:'warm'})};
  const native={createDemoBudgetReservation:(actual,options)=>{assert.equal(actual,service);observed=options;return async(...args)=>{allocations.push(args);return {allocatedUsd:0.05};};}};
  const handler=await new AsyncFunction('core','demo','voice','Netlify',source('britesConciergeDemoTurn.js'))(mockCore,mockDemo,native,{env:{get:name=>name==='BRITES_CONCIERGE_REALTIME_DEMO_USD_CAP'?'10':undefined}});
  const value=await (await handler(req({action:'conversation',message:'Can we chat?'}),{})).json();assert.equal(value.aiUsed,true);assert.deepEqual(observed,{capUsd:10});assert.equal(allocations[0][0],0.05);
});
