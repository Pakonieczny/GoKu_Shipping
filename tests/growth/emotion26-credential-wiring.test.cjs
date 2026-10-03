'use strict';
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path');
const core=require('../../netlify/functions/_britesGrowth.js'),demo=require('../../netlify/functions/_britesConciergeDemoTurn.js');
const AsyncFunction=Object.getPrototypeOf(async function(){}).constructor;
const source=fs.readFileSync(path.join(__dirname,'../../netlify/functions/britesConciergeDemoTurn.js'),'utf8').replace(/^import \w+ from .*;\s*$/gm,'').replace('export default async(req,context)=>{','return async(req,context)=>{').replace(/export const config=\{[\s\S]*$/,'');
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
