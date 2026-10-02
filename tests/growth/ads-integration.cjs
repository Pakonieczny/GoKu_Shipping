'use strict';
const assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),vm=require('node:vm');
const {pathToFileURL}=require('node:url');
const root=path.resolve(__dirname,'../..');
const core=require('../../netlify/functions/_britesGrowth');
const {createAdDesignResearch}=require('../../netlify/functions/googleAdsAdDesignResearch');
const {readOnlyFirestore}=require('../../netlify/functions/_britesGrowthAdsReadOnly');
let checks=0;const eq=(a,b,m)=>{assert.deepEqual(a,b,m);checks++;},ok=(x,m)=>{assert.ok(x,m);checks++;};
const product={id:'10',title:'Bunny Necklace',url:'https://britesjewelry.com/products/bunny-necklace',description:'Bunny charm necklace.',images:[{id:'bunny-photo',url:'https://cdn.shopify.com/bunny.jpg'}]};
const group={key:'bunny',ref:'customers/9/ads/11',name:'Bunny necklaces',channel:'search',url:product.url,keywords:['bunny necklace']};
const approved={schema:1,productId:'gid://shopify/Product/10',handle:'bunny-necklace',status:'approved',version:'v1',savedAt:Date.now(),facts:[],sources:[{id:'shop',title:'Bunny necklace',url:product.url,excerpt:'Bunny charm necklace.',checkedAt:Date.now()}],buyerIntents:['rabbit lover gift'],competitors:[],recommendations:[{channel:'keywords',basis:'hypothesis',keywords:['rabbit lover gift'],action:'Test rabbit necklace gift intent.',measure:'Qualified clicks and validated purchases.',sourceIds:['shop']}],meanings:[]};
const collectInput={campaignId:'42',sourceVersion:1,group,selectedProducts:[product],settings:{productId:'10',sourceImageId:'bunny-photo'}};
async function collect(dossiers){let ids;const api=createAdDesignResearch({creativeFetch:async()=>'<h1>Bunny Necklace</h1><p>'+('This is the current Brites bunny necklace and its available product choices. '.repeat(4))+'</p>',conversionHealth:async()=>({validated:true}),sharedProductResearch:async values=>{ids=values;return dossiers;}});const evidence=await api.collect(collectInput);return {api,evidence,ids};}
async function endpoint(){const file=path.join(root,'netlify/functions/britesGrowthAds.js');let source=fs.readFileSync(file,'utf8');for(const name of ['_britesGrowth.js','_britesGrowthDemand.js'])source=source.replace("'./"+name+"'",JSON.stringify(pathToFileURL(path.join(root,'netlify/functions',name)).href));return import('data:text/javascript;base64,'+Buffer.from(source).toString('base64'));}
class Doc { constructor(id='one'){this.id=id;this.path='production/'+id;} async get(){return new Snap(this);} set(){throw Error('real write reached');} update(){throw Error('real write reached');} delete(){throw Error('real write reached');} }
class Snap { constructor(ref){this.ref=ref;this.id=ref.id;this.exists=true;} data(){return {orders:3};} }
class Query { doc(id){return new Doc(id);} where(){return this;} orderBy(){return this;} limit(){return this;} async get(){return new QuerySnap();} add(){throw Error('real write reached');} }
class QuerySnap { get docs(){return [new Snap(new Doc())];} forEach(fn){this.docs.forEach(fn);} }
class Tx { async get(ref){assert.ok(ref instanceof Doc,'read cursor unwrapped before native SDK');return ref.get();} set(){throw Error('real write reached');} }
class Db { collection(){return new Query();} doc(){return new Doc();} async runTransaction(fn){return fn(new Tx());} batch(){throw Error('real write reached');} }
(async()=>{
  const own=await collect([approved,{...approved,productId:'gid://shopify/Product/99',handle:'wrong-product'}]);
  eq(own.ids,['10'],'collector keeps original source identity while requesting saved knowledge');
  const shared=own.evidence.sources.find(s=>s.id==='sharedProductKnowledge');eq(shared.status,'available');eq(shared.data.dossiers.length,1,'another product never enters ad evidence');eq(shared.data.dossiers[0].productId,approved.productId,'GID dossier matches a short context Product ID');
  ok(/Current product\/landing sources remain authoritative/.test(shared.data.rules),'competitor research cannot establish commercial facts for the advertised item');
  const request=own.api.buildRequest({evidence:own.evidence,mode:'copy'});
  ok(request.input[0].content[0].text.includes('Test rabbit necklace gift intent.'),'the actual saved recommendation reaches the existing ad brief');
  const factIds=request.text.format.schema.properties.factClaims.items.properties.sourceId.enum;
  ok(!factIds.includes('sharedProductKnowledge'),'a shared competitor/meaning dossier cannot authorize commercial claim citations');
  for(const dossiers of [[],[{...approved,status:'draft'}],[{...approved,productId:'gid://shopify/Product/99'}]]){
    const e=(await collect(dossiers)).evidence;eq(e.sources.find(s=>s.id==='sharedProductKnowledge').status,'unavailable','missing, partial or foreign knowledge stays unavailable');ok(e.sources.some(s=>s.id==='product:10'&&s.status==='available'),'missing deep research does not replace the current exact product page');
  }
  const window={};vm.runInNewContext(fs.readFileSync(path.join(root,'brites-growth.js'),'utf8'),{window,document:{querySelector:()=>null},URL});const ui=window.BritesGrowth;
  eq(ui.productId('10'),approved.productId);eq(ui.productId('gid://shopify/ProductVariant/10'),null);
  eq(ui.selectDossier({dossiers:[{...approved,productId:'gid://shopify/Product/99'},approved]},{productId:'10',handle:'bunny-necklace'}),approved,'workspace selects the matching dossier instead of the first result');
  eq(ui.selectDossier({dossiers:[approved]},{productId:'10',handle:'some-other-product'}),null,'workspace rejects a handle mismatch');
  eq(ui.selectDossier({dossiers:[{...approved,status:'rejected'}]},{productId:'10',handle:approved.handle}),null);
  eq(ui.selectDossier({dossiers:[{...approved,status:'draft'}]},{productId:'10',handle:approved.handle}).status,'draft','partial dossier remains explicitly labelled partial');
  eq(ui.safeLink('javascript:alert(1)'),null);eq(ui.safeLink('https://user:secret@example.com/'),null);ok(ui.briefFor(approved,product.title).includes(product.url),'copyable test brief retains its source URLs');
  const heldIssue={productId:approved.productId,issues:[{id:'material-check',kind:'material',status:'open',detail:'Verify contradictory material.',blocks:[]}]},foreignIssue={productId:'gid://shopify/Product/99',issues:[{id:'foreign-style',kind:'style',status:'open',detail:'Another product.'}]};
  const ownIssues=ui.selectProductIssues({productIssues:[foreignIssue,heldIssue]},{productId:'10'});eq(ownIssues.issues.length,1,'only the selected product issues enter the research card');eq(ui.issueHolds(ownIssues).recommendationHold,true);eq(ui.issueHolds(ownIssues).cartHold,true,'critical legacy issue kinds hold promotion even with missing block arrays');
  ok(ui.briefFor(approved,product.title,ownIssues).includes('CORRECTIVE RESEARCH ONLY'),'held products copy corrective notes instead of a promotable ad brief');ok(ui.briefFor(approved,product.title,ownIssues).includes(approved.recommendations[0].action),'corrective hypotheses remain available for research');eq(ui.issueHolds({issues:[{...heldIssue.issues[0],status:'resolved'}]}).recommendationHold,false,'resolved issues do not retain a promotion hold');
  ok(ui.briefFor(approved,product.title,null,'unavailable').includes('CORRECTIVE RESEARCH ONLY'),'missing issue check does not implicitly clear a known hold');
  const db=readOnlyFirestore(new Db()),snap=await db.collection('production').doc('one').get();eq(snap.data(),{orders:3});
  assert.throws(()=>snap.ref.update({orders:99}),/writes are disabled/);checks++;
  assert.throws(()=>db.batch(),/writes are disabled/);checks++;
  const query=await db.collection('production').where('uploaded','==',false).limit(5).get();let visited=0;query.forEach(doc=>{visited++;assert.throws(()=>doc.ref.delete(),/writes are disabled/);});eq(visited,1,'query snapshots cannot leak writable document references');
  await assert.rejects(()=>db.runTransaction(async tx=>{const d=await tx.get(db.doc('production/one'));eq(d.data(),{orders:3});tx.set(db.doc('production/one'),{});}),/writes are disabled/);checks++;
  const {createHandler,READ_ACTIONS}=await endpoint(),env={BRITES_GROWTH_SANDBOX:'1',BRITES_GROWTH_NAMESPACE:'Brites_Growth_Sandbox',BRITES_GROWTH_ADMIN_KEY:'test-operator-key'};
  let loads=0,captured=null,knowledgeIds=null;
  const handler=createHandler({environment:()=>env,savedPasscode:async()=> 'existing-owner-passcode',readSavedDemand:async()=>[{productId:approved.productId,state:'current'},{productId:'gid://shopify/Product/999',state:'current'}],loadEngine:async()=>{loads++;return {gaql:async()=>[],keywordResearch:async()=>{throw Error('Planner cannot run without observed targeting.');},dashboard:async b=>{captured=b;return {campaignInventory:{ok:true}};},conversionHealth:async b=>{captured=b;return {validated:false};},campaignGoalEvidence:async(...args)=>{captured=args;return {complete:true};},opportunitiesWithStatus:async b=>{captured=b;return {opportunities:[]};}};},researchService:()=>({status:async()=>({counts:{ranked:100}}),getProduct:async id=>({...product,id,handle:approved.handle,variants:[]}),research:async ids=>{knowledgeIds=ids;return [approved,{...approved,productId:'gid://shopify/Product/999'}];}})});
  const call=(body,extra={})=>handler(new Request('https://sandbox.example/api/growth-ads',{method:'POST',headers:{'Content-Type':'application/json','X-Growth-Key':'test-operator-key',...extra},body:JSON.stringify(body)}));
  for(const action of ['approve','apply','setBudget','setStatus','syncConversions','measureNow','enforceCeiling','monthlyGuard','startAdDesign','generate','runNow','deleteCampaign','distill'])eq((await call({action})).status,403,'mutation and paid action '+action+' is refused');
  eq(loads,0,'rejected actions never load the legacy Ads engine');
  eq((await call({action:'opportunities',force:true})).status,403,'a forced scan is refused instead of silently billed');
  eq((await call({action:'conversionHealth',force:true})).status,200);eq(captured,{force:false},'conversion refresh cannot reconcile production receipts');
  eq((await call({action:'campaignGoalEvidence',force:true,patch:{primaryForGoal:false}})).status,200);eq(captured,[],'goal auditing accepts no mutation/override fields');
  eq((await call({action:'dashboard',activityOnly:true,force:true,patch:{enabled:true},budget:500})).status,200);eq(captured,{activityOnly:true},'only explicitly read fields reach the engine');
  const saved=await call({action:'growthResearchDossiers',productIds:['10']});eq(saved.status,200);eq(knowledgeIds,[approved.productId],'bridge canonicalizes short IDs');eq((await saved.json()).dossiers.length,1,'bridge filters foreign product records');
  eq((await call({action:'growthResearchDossiers',productIds:['10',approved.productId,'10']})).status,200,'equivalent Product ID formats are safely deduplicated');eq(knowledgeIds,[approved.productId]);
  eq((await call({action:'growthResearchDossiers',productIds:['gid://shopify/ProductVariant/10']})).status,400);
  const loadedBeforeDemand=loads,savedDemand=await call({action:'growthProductDemand',productIds:['10']});eq(savedDemand.status,200);eq((await savedDemand.json()).products.length,1,'saved demand cannot substitute another product');eq(loads,loadedBeforeDemand,'opening saved demand never loads provider readers');
  const demand=await call({action:'productDemandEvidence',productId:'10',planner:true,geoIds:['arbitrary'],force:true,seeds:['unrelated query']});eq(demand.status,200);const demandValue=await demand.json();eq(demandValue.hypothesisSeeds,['rabbit lover gift'],'caller fields cannot replace the approved product hypotheses');eq(demandValue.planner.state,'unavailable','missing observed market does not use a default geography');
  eq((await call({action:'productDemandEvidence',productId:'99'})).status,409,'another product dossier cannot fill the exact product evidence gap');
  eq((await call({action:'productDemandEvidence',productId:'gid://shopify/ProductVariant/10'})).status,400);
  eq((await call({action:'dashboard'},{'X-Growth-Key':'wrong'})).status,401);
  eq((await call({action:'dashboard'},{'X-Growth-Key':'existing-owner-passcode'})).status,200,'the existing owner passcode works without creating a new credential');
  eq((await call({action:'dashboard'},{Origin:'https://other.example'})).status,403);
  env.BRITES_GROWTH_NAMESPACE='Brites_Growth_Live';eq((await call({action:'dashboard'})).status,503,'sandbox adapter refuses the live namespace');env.BRITES_GROWTH_NAMESPACE='Brites_Growth_Sandbox';
  env.BRITES_GROWTH_SANDBOX='0';eq((await call({action:'dashboard'})).status,503,'sandbox mode is mandatory');
  ok(!READ_ACTIONS.includes('syncConversions')&&!READ_ACTIONS.includes('startAdDesign'),'write/AI routes are absent from the compiled allowlist');
  console.log('Growth Ads integration: '+checks+' checks passed.');
})().catch(error=>{console.error(error);process.exitCode=1;});
