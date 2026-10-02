'use strict';
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),vm=require('node:vm');
const {JSDOM}=require('jsdom');
const root=path.resolve(__dirname,'../..'),core=require('../../netlify/functions/_britesGrowth'),projection=require('../../netlify/functions/googleAdsAdDesignResearch');
const id='gid://shopify/Product/10',foreign='gid://shopify/Product/99',version='a'.repeat(64);
function fixture(){
  const at=Date.now(),dossier={schema:1,productId:id,handle:'fixture-bunny',status:'approved',version,savedAt:at,sources:[{id:'shop',url:'https://example.com/fixture',title:'Fixture',excerpt:'Synthetic fixture only.',reviewed:true,checkedAt:at}],facts:[],meanings:[],competitors:[],buyerIntents:['Fixture gift intent'],recommendations:[{channel:'keywords',basis:'hypothesis',action:'Test fixture gift intent.',measure:'Qualified visits.',sourceIds:['shop'],keywords:['fixture gift']}]};
  const state={passcode:'fixture-owner',dossier,product:{id,handle:dossier.handle},issues:[],issueFailure:false,reads:[],providerCalls:0,dbWrites:0,engineCalls:0,serviceLoads:0};
  state.demand={productId:id,savedAt:at,evidence:{productId:id,handle:dossier.handle,dossierVersion:version,at,readOnly:true,sandboxReadOnly:true}};
  const service={
    status:async()=>({counts:{ranked:100}}),
    research:async ids=>{state.reads.push({type:'research',ids});return [state.dossier,{...state.dossier,productId:foreign}];},
    getProduct:async productId=>productId===id?state.product:null,
    productIssues:async ids=>{state.reads.push({type:'issues',ids});if(state.issueFailure)throw Error('Private issue reader failed.');return state.issues;},
    col:name=>{assert.equal(name,'Demand');return {doc:()=>({get:async()=>({exists:true,data:()=>state.demand}),set:()=>{state.dbWrites++;throw Error('Writes forbidden');}})};}
  };
  const exports={},engine=new Proxy({},{get:()=>{state.engineCalls++;throw Error('Legacy engine dispatch forbidden');}});
  const requireStub=name=>{
    if(name==='node-fetch')return async()=>{state.providerCalls++;throw Error('Provider request forbidden');};
    if(name==='./googleAdsAutopilot')return engine;
    if(name==='./_editPasscode')return {resolve:async()=>({value:state.passcode}),sameSecret:(a,b)=>!!a&&a===b,envPasscode:()=>state.passcode};
    if(name==='./firebaseAdmin')return {firestore:()=>({})};
    if(name==='./_britesGrowth')return {...core,createGrowthService:()=>{state.serviceLoads++;return service;}};
    if(name==='./_britesGrowthDemandStore')return require('../../netlify/functions/_britesGrowthDemandStore');
    if(name==='./googleAdsAdDesignResearch')return projection;
    throw Error('Unexpected require: '+name);
  };
  vm.runInNewContext(fs.readFileSync(path.join(root,'netlify/functions/googleAdsAutopilotKick.js'),'utf8'),{require:requireStub,exports,module:{exports},process:{env:{}},console,Buffer,URL,Date,Set,Map},{filename:'googleAdsAutopilotKick.js'});
  const api={exports:{}};
  vm.runInNewContext(fs.readFileSync(path.join(root,'netlify/functions/googleAdsAutopilotApi.js'),'utf8'),{module:api,exports:api.exports,require:name=>{assert.equal(name,'./googleAdsAutopilotKick');return exports;}},{filename:'googleAdsAutopilotApi.js'});
  const post=async(body,passcode='fixture-owner')=>api.exports.handler({httpMethod:'POST',headers:{'X-Edit-Passcode':passcode},body:JSON.stringify(body)});
  return {state,post,exports};
}
function ui(){const dom=new JSDOM('<main></main>',{url:'https://sandbox.invalid'});vm.runInNewContext(fs.readFileSync(path.join(root,'brites-growth.js'),'utf8'),{window:dom.window,document:dom.window.document,URL});return {dom,api:dom.window.BritesGrowth};}
test('existing HTTP handler returns a usable exact-version operator packet without touching provider engine',async()=>{
  const f=fixture(),r=await f.post({action:'growthResearchDossiers',productIds:['10',id,'10']});assert.equal(r.statusCode,200);const body=JSON.parse(r.body);
  assert.equal(body.dossiers.length,1);assert.equal(body.productIssueState,'available');assert.equal(body.operatorReviews[0].state,'pending_operator_review');assert.equal(body.operatorReviews[0].dossierVersion,version);
  const u=ui(),product={productId:id,handle:f.state.dossier.handle,dossierVersion:version};assert.ok(u.api.operatorReviewFor(body,product,f.state.dossier));
  assert.equal(u.api.operatorReviewFor({dossiers:body.dossiers,productIssues:[]},product,f.state.dossier),null,'legacy response shape cannot drive the updated app review queue');u.dom.window.close();
  assert.equal(f.state.engineCalls,0);assert.equal(f.state.providerCalls,0);assert.equal(f.state.dbWrites,0);assert.ok(f.state.reads.every(r=>r.ids.length===1&&r.ids[0]===id));
});
test('dossier identity, approval, reviewed-source and proposal failures withhold whole packets',async()=>{
  const changes=[f=>f.state.product.handle='foreign',f=>f.state.product.id=foreign,f=>f.state.dossier.status='draft',f=>f.state.dossier.proposalOnly=true,f=>f.state.dossier.reviewStatus='proposed',f=>f.state.dossier.version='invalid',f=>f.state.dossier.sources[0].reviewed=false,f=>f.state.dossier.sources[0].checkedAt=Date.now()-31*86400000,f=>f.state.dossier.sources[0].url='javascript:alert(1)',f=>f.state.dossier.recommendations[0].sourceIds=['missing']];
  for(const change of changes){const f=fixture();change(f);const r=await f.post({action:'growthResearchDossiers',productIds:[id]});assert.equal(r.statusCode,200);const body=JSON.parse(r.body);assert.equal(body.operatorReviews[0].state,'unavailable');assert.equal(body.operatorReviews[0].operatorReviewPacket,undefined);assert.equal(f.state.providerCalls,0);assert.equal(f.state.dbWrites,0);}
});
test('active critical/meaning holds and unavailable issues remain held; foreign issues stay isolated',async()=>{
  for(const kind of ['identity','material','style','history','content']){const f=fixture();f.state.issues=[{productId:id,issues:[{kind,status:'open'}]}];const body=JSON.parse((await f.post({action:'growthResearchDossiers',productIds:[id]})).body);assert.equal(body.operatorReviews[0].state,'held');assert.equal(body.operatorReviews[0].operatorReviewPacket,undefined);}
  const f=fixture();f.state.issueFailure=true;let body=JSON.parse((await f.post({action:'growthResearchDossiers',productIds:[id]})).body);assert.equal(body.productIssueState,'unavailable');assert.equal(body.operatorReviews[0].state,'held');assert.ok(!JSON.stringify(body).includes('Private issue reader failed'));
  f.state.issueFailure=false;f.state.issues=[{productId:foreign,issues:[{kind:'material',status:'open'}]}];body=JSON.parse((await f.post({action:'growthResearchDossiers',productIds:[id]})).body);assert.equal(body.productIssues.length,0);assert.equal(body.operatorReviews[0].state,'pending_operator_review');
});
test('saved demand uses actual read-only store with current version/holds, never a new provider research call',async()=>{
  const f=fixture();let r=await f.post({action:'growthProductDemand',productIds:['10',id],force:true,planner:true,seeds:['unrelated']});assert.equal(r.statusCode,200);let products=JSON.parse(r.body).products;assert.equal(products.length,1);assert.equal(products[0].state,'current');assert.equal(products[0].promotionAllowed,true);
  f.state.dossier.version='b'.repeat(64);products=JSON.parse((await f.post({action:'growthProductDemand',productIds:[id]})).body).products;assert.equal(products[0].state,'stale');assert.equal(products[0].promotionAllowed,false);
  f.state.dossier.version=version;f.state.issues=[{productId:id,issues:[{kind:'material',status:'open'}]}];products=JSON.parse((await f.post({action:'growthProductDemand',productIds:[id]})).body).products;assert.equal(products[0].promotionAllowed,false);
  f.state.issues=[];f.state.issueFailure=true;products=JSON.parse((await f.post({action:'growthProductDemand',productIds:[id]})).body).products;assert.equal(products[0].productIssueState,'unavailable');assert.equal(products[0].promotionAllowed,false);
  f.state.demand.productId=foreign;products=JSON.parse((await f.post({action:'growthProductDemand',productIds:[id]})).body).products;assert.deepEqual(products,[]);
  assert.equal(f.state.engineCalls,0);assert.equal(f.state.providerCalls,0);assert.equal(f.state.dbWrites,0);
});
test('private growth reads require configured owner passcode; other legacy read behavior remains unchanged',async()=>{
  for(const action of ['growthResearchStatus','growthResearchDossiers','growthProductDemand']){const f=fixture();const body={action,productIds:[id]};let r=await f.post(body,'wrong');assert.equal(r.statusCode,401);assert.equal(f.state.serviceLoads,0);f.state.passcode=null;r=await f.post(body,'');assert.equal(r.statusCode,403);assert.equal(JSON.parse(r.body).code,'EDIT_PASSCODE_NOT_SET');assert.equal(f.state.serviceLoads,0);}
  const f=fixture();f.state.passcode=null;const r=await f.post({action:'dashboard'},'');assert.equal(r.statusCode,500,'ordinary read still reaches its existing engine path');assert.equal(f.state.engineCalls,1);
});
test('exact Product ID bounds are checked before loading database; Variant IDs and malformed input are rejected',async()=>{
  for(const action of ['growthResearchDossiers','growthProductDemand'])for(const productIds of [undefined,{},['gid://shopify/ProductVariant/10'],['garbage'],Array.from({length:21},()=>id)]){const f=fixture();const r=await f.post({action,productIds});assert.equal(r.statusCode,400);assert.equal(f.state.serviceLoads,0);assert.equal(f.state.engineCalls,0);assert.equal(f.state.providerCalls,0);}
});
test('copy of legitimate review packet remains atomic and mutation actions keep their owner gate',async()=>{
  const f=fixture(),body=JSON.parse((await f.post({action:'growthResearchDossiers',productIds:[id]})).body),packet=body.operatorReviews[0].operatorReviewPacket;
  for(const key of ['providerWrites','campaignWrites','budgetWrites','automaticActivation'])assert.equal(packet[key],false);
  assert.equal(packet.candidates[0].sourceIds[0],'shop');assert.equal(packet.positiveKeywords[0].candidateIds[0],packet.candidates[0].candidateId);
  for(const action of ['setBudget','approve','runNow','startAdDesign']){const r=await f.post({action},'wrong');assert.equal(r.statusCode,401);}
  assert.equal(f.state.engineCalls,0);assert.equal(f.state.providerCalls,0);assert.equal(f.state.dbWrites,0);
});
