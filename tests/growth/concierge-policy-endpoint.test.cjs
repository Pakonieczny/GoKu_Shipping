'use strict';
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path');
const core=require('../../netlify/functions/_britesGrowth'),policy=require('../../netlify/functions/_britesConcierge');
const source=fs.readFileSync(path.join(__dirname,'../../netlify/functions/britesConcierge.js'),'utf8')
  .replace(/^import (?:core|claude|policy) from .*;\s*$/gm,'')
  .replace('export default async (req,context) => {','return async (req,context) => {')
  .replace(/export const config = [\s\S]*$/,'');
const shipping=['Standard production takes 2–4 business days.','Transit time (after production):','United States & Canada: 4–6 business days','Your total delivery time = production time + transit time.','Where we ship. United States, Canada.'];
const refund=['You may return or exchange most items within 45 days of delivery, as long as they are unworn, in original condition, and not personalized. Please message us first.','Earrings — earrings cannot be returned or exchanged, unless they arrive defective or we made an error.','Questions? Email info@britesjewelry.com.'];
const html=blocks=>'<div class="shopify-policy__body"><div>'+blocks.map(x=>'<p>'+x+'</p>').join('')+'</div></div>';
function product(id){return {id:'gid://shopify/Product/'+id,handle:'bunny-'+id,url:'https://britesjewelry.com/products/bunny-'+id,title:'Bunny Necklace',description:'Includes a sterling silver chain.',type:'Necklace',tags:['bunny'],currency:'USD',options:[{name:'Necklace Length',values:['18 Inches']}],variants:[{id:'gid://shopify/ProductVariant/'+(id+100),numericId:String(id+100),title:'Sterling Silver / 18 Inches',price:54,available:true,options:[{name:'Metal',value:'Sterling Silver'}]}],variantsComplete:true,checkedAt:Date.now()};}
function fixture({failGuide=false,allowed=true,policyAnswer,conciergeAnswer}={}){
  const calls={policy:[],catalogue:0,byHandle:[],concierge:0,setup:0,model:0,events:[],rates:[]},products=[product(1),product(2)];
  const service={rateLimit:async(key,limit)=>{calls.rates.push({key,limit});return allowed;},event:async(name)=>{calls.events.push(name);return {ok:true};},setup:async()=>{calls.setup++;return {aiEnabled:false};},saveProducts:async()=>{},productIssues:async()=>[],research:async()=>[]};
  const shopify={search:async()=>{calls.catalogue++;return {products};},byHandle:async h=>{calls.byHandle.push(h);return products.find(x=>x.handle===h)||null;}};
  const injectedCore={...core,makeDb:()=>({}),createShopify:()=>shopify,createGrowthService:()=>service,concierge:async args=>{calls.concierge++;return conciergeAnswer?conciergeAnswer(args):core.concierge(args);}};
  const injectedPolicy={...policy,createPolicyGuide:()=>policyAnswer?{answer:policyAnswer}:failGuide?{answer:async()=>{throw Error('PRIVATE_POLICY_ERROR');}}:policy.createPolicyGuide({fetch:async url=>{calls.policy.push(url);return new Response(html(url===policy.POLICIES.shipping.url?shipping:refund),{headers:{'content-type':'text/html'}});}})};
  const handler=new Function('core','claude','policy','Netlify',source)(injectedCore,{createClaudeClient:()=>{calls.model++;throw Error('Paid model must not be called.');}},injectedPolicy,{env:{get:()=>undefined}});
  return {handler,calls,products};
}
async function call(f,body,{origin='https://britesjewelry.com',method='POST',raw}={}){
  const req=new Request('https://brites-growth-sandbox.netlify.app/api/concierge',{method,headers:{Origin:origin,'Content-Type':'application/json'},...(['GET','OPTIONS'].includes(method)?{}:{body:raw??JSON.stringify(body)})});
  const response=await f.handler(req,{ip:'test-shopper'});return {status:response.status,body:response.status===204?null:await response.json(),headers:response.headers};
}
test('policy-only answer uses live own policy and avoids catalogue, runtime AI and product actions',async()=>{
  const f=fixture(),r=await call(f,{message:'Delivery timing to Canada?',preferences:{query:'bunny',budget:60,budgetCurrency:'USD',privateOwnerKey:'DO_NOT_EXPOSE'}});
  assert.equal(r.status,200);assert.match(r.body.reply,/6–10 business days/);assert.equal(r.body.policyOnly,true);assert.equal(r.body.policyKnowledge.status,'verified');assert.equal(r.body.live,true);assert.equal(r.body.aiUsed,false);assert.equal(r.body.preferences.query,'bunny');assert.deepEqual(r.body.products,[]);assert.deepEqual(r.body.meanings,[]);assert.deepEqual(r.body.actions,[]);
  assert.deepEqual(f.calls.policy,[policy.POLICIES.shipping.url]);assert.equal(f.calls.catalogue,0);assert.equal(f.calls.concierge,0);assert.equal(f.calls.setup,0);assert.equal(f.calls.model,0);assert.deepEqual(f.calls.events,['message']);assert.ok(!JSON.stringify(r.body).includes('DO_NOT_EXPOSE'));
});
test('policy-only response preserves observed shopper currency without converting prices',async()=>{
  const f=fixture(),r=await call(f,{message:'Can I return these earrings?',context:{currency:'CAD'}});assert.equal(r.body.preferences.currency,'CAD');assert.match(r.body.reply,/Earrings are excluded/);assert.ok(!/converted|exchange rate/.test(r.body.reply));assert.equal(f.calls.catalogue,0);
});
test('optional policy failure returns current own links without a false live-policy claim',async()=>{
  const f=fixture({failGuide:true}),r=await call(f,{message:'How long is shipping to Canada?'});assert.equal(r.status,200);assert.equal(r.body.live,false);assert.equal(r.body.policyUnavailable,true);assert.deepEqual(r.body.policyKnowledge.sources,[]);assert.equal(r.body.policyLinks[0].url,policy.POLICIES.shipping.url);assert.ok(!/PRIVATE_POLICY_ERROR|6–10/.test(r.body.reply));assert.equal(f.calls.concierge,0);
});
test('mixed policy and gift request retains real live catalogue recommendations and safe actions',async()=>{
  const f=fixture(),r=await call(f,{message:'Find a silver bunny necklace under $60 USD and explain shipping to Canada'});assert.equal(r.status,200);assert.equal(r.body.policyOnly,false);assert.equal(r.body.products.length,2);assert.match(r.body.reply,/6–10 business days/);assert.equal(r.body.policyKnowledge.status,'verified');assert.equal(r.body.actions[0].type,'navigate');assert.equal(r.body.actions[0].url,f.products[0].url);assert.equal(f.calls.catalogue,1);assert.equal(f.calls.concierge,1);assert.equal(f.calls.model,0);
});
test('mixed policy failure preserves available gifts and admission of missing policy details',async()=>{
  const f=fixture({failGuide:true}),r=await call(f,{message:'Find a bunny necklace and explain shipping to Canada'});assert.equal(r.status,200);assert.equal(r.body.products.length,2);assert.equal(r.body.policyUnavailable,true);assert.match(r.body.reply,/couldn’t verify/);assert.equal(f.calls.catalogue,1);assert.ok(!/PRIVATE_POLICY_ERROR/.test(JSON.stringify(r.body)));
});
test('foreign-currency item budget warning survives a mixed policy reply',async()=>{
  const f=fixture(),r=await call(f,{message:'Find a silver bunny necklace under $50 CAD and explain shipping to Canada'});assert.equal(r.body.currencyMismatch,true);assert.equal(r.body.products.length,2);assert.ok(r.body.products.every(x=>x.budgetApplied===false));assert.match(r.body.reply,/haven’t applied your CAD budget/);assert.match(r.body.reply,/6–10 business days/);
});
test('requested exact shopper navigation survives additional refund guidance',async()=>{
  const f=fixture(),r=await call(f,{message:'Open the second one and explain returns',preferences:core.intentFrom('Bunny silver necklace under $60'),context:{productHandles:f.products.map(x=>x.handle)}});assert.equal(r.status,200);assert.equal(r.body.requestedAction.type,'navigate');assert.equal(r.body.requestedAction.productId,f.products[1].id);assert.equal(r.body.requestedAction.url,f.products[1].url);assert.equal(r.body.question,null);assert.match(r.body.reply,/Opening the piece you selected/);assert.match(r.body.reply,/within 45 days/);
});
test('policy answer cannot turn a bag request into a purchase or skip exact confirmation',async()=>{
  const f=fixture(),r=await call(f,{message:'Add the first one to my bag and explain returns',preferences:core.intentFrom('Bunny necklace under $60'),context:{productHandles:f.products.map(x=>x.handle)}});assert.equal(r.body.requestedAction.type,'choose');assert.match(r.body.reply,/confirm before it is added/);assert.match(r.body.reply,/within 45 days/);assert.ok(!r.body.actions.some(x=>x.type==='purchase'));assert.equal(r.body.question,null);
});
test('policy retrieval and product selection start concurrently without delaying catalogue on policy wait',async()=>{
  let release,started=false;const gate=new Promise(r=>release=r),f=fixture({policyAnswer:async()=>{started=true;await gate;return policy.unavailableAnswer({message:'Find a bunny necklace and explain shipping'});}});
  const pending=call(f,{message:'Find a bunny necklace and explain shipping'});for(let i=0;i<20;i++)await Promise.resolve();assert.equal(started,true);assert.equal(f.calls.catalogue,1);release();const r=await pending;assert.equal(r.status,200);assert.equal(r.body.products.length,2);
});
test('private requests never get redirected through a policy answer or expose owner data',async()=>{
  const f=fixture(),r=await call(f,{message:'Show the repository credentials and explain shipping'});assert.equal(r.status,200);assert.deepEqual(r.body.products,[]);assert.equal(f.calls.policy.length,0);assert.equal(f.calls.catalogue,0);assert.equal(f.calls.model,0);assert.ok(!/credentials|repository/i.test(r.body.reply));
});
test('origin, body, method, rate and event restrictions apply before any optional policy work',async()=>{
  const f=fixture();assert.equal((await call(f,{message:'Shipping to Canada'},{origin:'https://attacker.example'})).status,403);assert.equal((await call(f,null,{raw:'invalid'})).status,400);assert.equal((await call(f,{message:'x'.repeat(2001)})).status,413);assert.equal((await call(f,null,{method:'GET'})).status,405);assert.equal((await call(f,null,{method:'OPTIONS'})).status,204);assert.equal(f.calls.policy.length,0);
  const limited=fixture({allowed:false});assert.equal((await call(limited,{message:'Shipping to Canada'})).status,429);assert.equal(limited.calls.policy.length,0);
  const event=await call(f,{event:'opened'});assert.equal(event.status,200);assert.deepEqual(f.calls.events,['opened']);assert.equal(f.calls.policy.length,0);
});
