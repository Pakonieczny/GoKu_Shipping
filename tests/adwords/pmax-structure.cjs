// Performance Max structure: a product joins an existing Performance Max campaign as one more asset group (with
// the combined budget) instead of splitting a small budget; campaign negative keywords; the Brites brand exclusion.
// Each is a draft in Approvals. Dry runs and validation send the new operations to a recording stand-in for Google
// (validate only), and every money check counts spendable budgets once: a shared budget once, an ended campaign
// not at all. Offline: no Google request, no paid AI call.
'use strict';
const assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),vm=require('node:vm');
const file=path.resolve(__dirname,'../../netlify/functions/googleAdsAutopilot.js'),realRequire=require('node:module').createRequire(file);
const S=realRequire('./_googleAdsPmaxStructure'),R=realRequire('./googleAdsCampaignStyles'),CS=require('../../brites-campaign-styles');
const clone=v=>v==null?v:JSON.parse(JSON.stringify(v)),DAY=86400000;let n=0;const check=(v,m)=>{assert.ok(v,m);n++;};

// ---------- offline stand-ins ----------
// The engine with a recording transport in place of node-fetch; gaql and the token are bound to the fake account.
function engine(transport){
  const sent=[],fetch=async(url,opts={})=>{const body=opts.body&&typeof opts.body==='string'?JSON.parse(opts.body):null;sent.push({url:String(url),body});return transport(String(url),body);};
  const context=vm.createContext({module:{exports:{}},exports:{},require:x=>x==='node-fetch'?fetch:realRequire(x),process:{env:{GADS_CUSTOMER_ID:'123'}},console,Buffer,Date,Intl,Map,Set,URL,URLSearchParams,setTimeout,clearTimeout});
  vm.runInContext(fs.readFileSync(file,'utf8'),context);
  return {E:context.module.exports,sent,get:x=>vm.runInContext(x,context),bind(values){context.__m=values;vm.runInContext(Object.keys(values).map(k=>k+'=__m.'+k).join('\n'),context);}};
}
// Firestore with the queries the engine uses (==, in, limit), transactions and stored files.
function memory(){
  const docs=new Map(),files=new Map();let seq=0;
  const read=(o,k)=>k.split('.').reduce((v,p)=>v==null?v:v[p],o);
  const snap=p=>({id:p.split('/').pop(),exists:docs.has(p),ref:doc(p),data:()=>clone(docs.get(p))});
  const doc=p=>({id:p.split('/').pop(),path:p,get:async()=>snap(p),collection:x=>collection(p+'/'+x),delete:async()=>{docs.delete(p);},
    set:async(v,o)=>{docs.set(p,o&&o.merge?{...(docs.get(p)||{}),...clone(v)}:clone(v));},
    update:async v=>{if(!docs.has(p))throw Error('No document '+p);const next=clone(docs.get(p));for(const [k,val] of Object.entries(clone(v))){const parts=k.split('.');let at=next;for(const x of parts.slice(0,-1))at=at[x]||(at[x]={});at[parts.at(-1)]=val;}docs.set(p,next);}});
  const collection=(p,filters=[],max=Infinity)=>({path:p,
    where:(f,op,v)=>collection(p,[...filters,[f,op,v]],max),limit:m=>collection(p,filters,m),orderBy(){return this;},select(){return this;},
    doc:x=>doc(p+'/'+(x==null?'auto'+(++seq):x)),add:async v=>{const id='auto'+(++seq);docs.set(p+'/'+id,clone(v));return doc(p+'/'+id);},
    get:async()=>{const list=[...docs.keys()].filter(k=>k.startsWith(p+'/')&&!k.slice(p.length+1).includes('/')).filter(k=>filters.every(([f,op,v])=>{const x=read(docs.get(k),f);return op==='=='?x===v:op==='in'?v.includes(x):true;})).slice(0,max).map(snap);
      return {docs:list,size:list.length,empty:!list.length,forEach:fn=>list.forEach(fn)};}});
  return {docs,files,admin:{storage:()=>({bucket:()=>({file:p=>({download:async()=>{if(!files.has(p))throw Error('Saved file missing');return [files.get(p)];}})})})},
    db:{collection,runTransaction:async fn=>fn({get:r=>r.get(),set:(r,v,o)=>r.set(v,o),update:(r,v)=>r.update(v),delete:r=>docs.delete(r.path)})},FV:{serverTimestamp:()=>Date.now()}};
}
const B=id=>'customers/123/campaignBudgets/'+id,C=id=>'customers/123/campaigns/'+id;
const pmax=(id,name,o={})=>({id,name,channel:'PERFORMANCE_MAX',status:'ENABLED',servingStatus:'SERVING',primaryStatus:'ELIGIBLE',merchantId:'555',feedLabel:'US',brand:false,bidding:'MAXIMIZE_CONVERSION_VALUE',groups:[],items:[],geo:['2840'],shared:false,...o});
const search=(id,name,o={})=>({id,name,channel:'SEARCH',status:'ENABLED',servingStatus:'SERVING',groups:[],items:[],geo:[],shared:false,...o});
// The account as Google reports it. Spendable now: 101, 104, 105, 107, 108 and the shared Search budget once = 53.
// 103 has ended (50 not counted), 102 is paused (15 not counted).
function account(){
  const W={merchant:'555',brandUnreadable:false,queries:[],mtd:0,terms:[],negatives:[],themes:[],lists:[],brandExclusions:[],goals:[],suggestCalls:0,
    brands:[{id:'b-777',name:'Brites',urls:['https://britesjewelry.com'],state:'APPROVED'},{id:'b-900',name:'Brites Paint Co',urls:['https://britespaint.example'],state:'APPROVED'}],
    campaigns:[
      pmax('101','BA · Necklaces PMax',{budgetRes:B(901),budget:20,groups:['AG · Corgi necklace','AG · Charms'],items:['shopify_US_99_999']}),
      pmax('102','BA · Rings PMax',{status:'PAUSED',brand:true,budgetRes:B(902),budget:15,groups:['AG · Rings']}),
      pmax('103','BA · Old PMax',{servingStatus:'ENDED',primaryStatus:'ENDED',budgetRes:B(903),budget:50}),
      pmax('104','BA · CA PMax',{feedLabel:'CA',budgetRes:B(904),budget:10,geo:['2124']}),
      pmax('105','BA · Other store',{merchantId:'777',budgetRes:B(905),budget:5}),
      pmax('107','BA · Full PMax',{budgetRes:B(907),budget:3,groups:Array.from({length:100},(_,i)=>'AG · '+i)}),
      pmax('108','BA · Deleted PMax',{budgetRes:B(908),budget:7}),
      pmax('109','BA · No budget PMax',{status:'PAUSED',budgetRes:null,budget:0}),
      search('201','BA · brand-search',{budgetRes:B(950),budget:8,shared:true}),
      search('202','BA · rings-search',{budgetRes:B(950),budget:8,shared:true})]};
  const camp=id=>W.campaigns.find(c=>c.id===id);W.camp=camp;
  const money=v=>String(Math.round(v*1e6));
  W.gaql=async q=>{W.queries.push(q);const one=/campaign\.id = (\d+)/.exec(q),list=/campaign\.id IN \(([^)]*)\)/.exec(q),ids=list?list[1].split(',').map(s=>s.trim()):null;
    const scope=all=>all.filter(c=>(!one||c.id===one[1])&&(!ids||ids.includes(c.id))),byId=rows=>rows.filter(x=>(!one||x.campaignId===one[1])&&(!ids||ids.includes(x.campaignId)));
    if(q.includes('campaign.shopping_setting.merchant_id')&&q.includes('FROM campaign WHERE')){
      if(W.brandUnreadable&&q.includes('brand_guidelines_enabled'))throw Error('[gads] search failed: UNRECOGNIZED_FIELD');
      let rows=scope(W.campaigns);if(q.includes("advertising_channel_type = 'PERFORMANCE_MAX'"))rows=rows.filter(c=>c.channel==='PERFORMANCE_MAX'&&c.status!=='REMOVED');
      // Google omits false booleans and absent messages.
      return rows.map(c=>({campaign:{id:c.id,name:c.name,status:c.status,servingStatus:c.servingStatus,primaryStatus:c.primaryStatus,advertisingChannelType:c.channel,biddingStrategyType:c.bidding,...(c.merchantId?{shoppingSetting:{merchantId:c.merchantId,feedLabel:c.feedLabel}}:{}),...(c.brand&&q.includes('brand_guidelines_enabled')?{brandGuidelinesEnabled:true}:{})},
        ...(c.budgetRes?{campaignBudget:{resourceName:c.budgetRes,amountMicros:money(c.budget),...(c.shared?{explicitlyShared:true}:{})}}:{})}));
    }
    if(q.startsWith('SELECT asset_group.id, asset_group.name FROM asset_group WHERE campaign.id ='))return scope(W.campaigns).flatMap(c=>c.groups.map((name,i)=>({assetGroup:{id:String(i+1),name}})));
    if(q.includes('FROM asset_group_listing_group_filter'))return scope(W.campaigns).flatMap(c=>[{assetGroupListingGroupFilter:{type:'SUBDIVISION'}},...c.items.map(v=>({assetGroupListingGroupFilter:{type:'UNIT_INCLUDED',caseValue:{productItemId:{value:v}}}})),{assetGroupListingGroupFilter:{type:'UNIT_EXCLUDED',caseValue:{productItemId:{value:'shopify_US_11_101-excluded'}}}}]);
    if(q.startsWith('SELECT campaign.id, asset_group.id FROM asset_group WHERE campaign.id IN'))return scope(W.campaigns).flatMap(c=>c.groups.map((g,i)=>({campaign:{id:c.id},assetGroup:{id:String(i+1)}})));
    if(q.includes("campaign_criterion.type = 'LOCATION'"))return scope(W.campaigns).flatMap(c=>c.geo.map((g,i)=>({campaign:{id:c.id},campaignCriterion:{criterionId:String(i+1),location:{geoTargetConstant:'geoTargetConstants/'+g}}})));
    if(/^SELECT campaign\.id, campaign\.serving_status, .*campaign_budget\.amount_micros.* FROM campaign WHERE campaign\.status = 'ENABLED'$/.test(q))
      return W.campaigns.filter(c=>c.status==='ENABLED').map(c=>({campaign:{id:c.id,servingStatus:c.servingStatus},campaignBudget:c.budgetRes?{resourceName:c.budgetRes,amountMicros:money(c.budget)}:{}}));
    if(/^SELECT campaign\.id, .*campaign_budget\.amount_micros.* FROM campaign WHERE campaign\.id = \d+$/.test(q))return scope(W.campaigns).map(c=>({campaign:{id:c.id},campaignBudget:{resourceName:c.budgetRes,amountMicros:money(c.budget)}}));
    if(q.includes('FROM campaign_search_term_view'))return byId(W.terms).map(t=>({campaign:{id:t.campaignId},campaignSearchTermView:{searchTerm:t.term},metrics:{clicks:String(t.clicks),costMicros:money(t.cost),conversions:t.conversions}}));
    if(q.includes("campaign_criterion.type = 'KEYWORD'"))return byId(W.negatives).map(x=>({campaign:{id:x.campaignId},campaignCriterion:{keyword:{text:x.text,matchType:x.matchType}}}));
    if(q.includes('FROM asset_group_signal'))return byId(W.themes).map(x=>({campaign:{id:x.campaignId},assetGroupSignal:{searchTheme:{text:x.text}}}));
    if(q.includes('FROM shared_set WHERE'))return W.lists.map(l=>({sharedSet:{resourceName:l.resourceName,name:l.name}}));
    if(q.includes('FROM shared_criterion'))return W.lists.flatMap(l=>l.brands.map(e=>({sharedSet:{resourceName:l.resourceName},sharedCriterion:{brand:{entityId:e}}})));
    if(q.includes("campaign_criterion.type = 'BRAND_LIST'"))return byId(W.brandExclusions).map(x=>({campaign:{id:x.campaignId},campaignCriterion:{brandList:{sharedSet:x.list}}}));
    if(q.startsWith('SELECT campaign.name FROM campaign WHERE'))return W.campaigns.filter(c=>c.status!=='REMOVED').map(c=>({campaign:{name:c.name}}));
    if(q.includes('FROM customer_conversion_goal'))return W.goals.map(g=>({customerConversionGoal:g}));
    if(q.includes("campaign.advertising_channel_type = 'SEARCH'"))return W.campaigns.filter(c=>c.channel==='SEARCH'&&c.status!=='REMOVED').map(c=>({campaign:{id:c.id,name:c.name,status:c.status}}));
    throw Error('Unexpected query '+q);};
  return W;
}
// Google's side of a request: SuggestBrands answers from the account; mutations answer validate-only requests,
// and a real publication only where a test allows it.
const reply=data=>({ok:true,status:200,json:async()=>data,headers:{get:()=>null}});
const google=W=>(url,body)=>{
  if(/\/customers\/123:suggestBrands$/.test(url)){W.suggestCalls++;return reply({brands:W.brands});}
  if(/\/customers\/123\/[A-Za-z]+:mutate$/.test(url)){
    if(W.onMutate)W.onMutate(url,body);
    if(body.validateOnly===true)return reply({});
    if(!W.publish)throw Error('A live publication was attempted in a validate-only test.');
    const ops=body.mutateOperations||body.operations;
    return reply(body.mutateOperations?{mutateOperationResponses:ops.map((o,i)=>({[Object.keys(o)[0].replace(/Operation$/,'Result')]:{resourceName:'customers/123/published/'+(i+1)}}))}:{results:ops.map((o,i)=>({resourceName:'customers/123/published/'+(i+1)}))});
  }
  throw Error('Unexpected request '+url);
};
const COUNTRIES=[{id:'2840',name:'United States',code:'US'},{id:'2124',name:'Canada',code:'CA'}];
const OFFERS=[{itemId:'shopify_US_11_101',title:'Corgi necklace',feedLabel:'US',link:'https://britesjewelry.com/products/corgi-necklace',targetCountries:['US']},{itemId:'shopify_US_22_201',title:'Fox necklace',feedLabel:'US',link:'https://britesjewelry.com/products/fox-necklace',targetCountries:['US']}];
function setup({W=account(),mem=memory(),ctrl={maxDailyBudgetTotal:62,maxMonthlySpend:1000,budgetCurrency:'CAD',defaultCountries:['2840']}}={}){
  const e=engine(google(W)),copyCalls=[];
  mem.docs.set('Brites_GAds_State/adArchive/campaigns/108',{status:'REMOVED'});
  e.bind({fb:()=>mem,gaql:W.gaql,control:async()=>ctrl,mintToken:async()=>'offline-token',_accountCurrency:async()=>'CAD',merchantCenterId:async()=>W.merchant,listCountries:async()=>COUNTRIES,_accountTz:async()=>'America/Toronto',
    _mtdSpend:async()=>({mtd:W.mtd,fxIncomplete:false}),_recordMutationVersions:async()=>{},
    // The product side of a PMax draft; copy is never bought (the deterministic copy is used).
    _pmaxResearchCandidate:()=>({}),getCollections:async()=>[{handle:'animal-necklaces',title:'Animal necklaces'}],collectionProfiles:async()=>({list:[]}),
    merchantProducts:async({itemIds=[]}={})=>OFFERS.filter(o=>itemIds.some(x=>String(x).toLowerCase()===o.itemId.toLowerCase())),_pmaxIsEligible:()=>true,
    shopifyGql:async()=>({nodes:[{id:'gid://shopify/Product/11',handle:'corgi-necklace',status:'ACTIVE'},{id:'gid://shopify/Product/22',handle:'fox-necklace',status:'ACTIVE'}]}),
    discoverPmaxAudienceResource:async()=>null,validatePmaxAudienceResource:async()=>({resource:null}),_pmaxAdCopy:async(...a)=>{copyCalls.push(a);return null;}});
  return {e,W,mem,ctrl,copyCalls,approval:id=>mem.docs.get('Brites_GAds_Approvals/'+id),approvals:()=>[...mem.docs.entries()].filter(([k])=>/^Brites_GAds_Approvals\/[^/]+$/.test(k)).map(([k,v])=>({id:k.split('/').pop(),...v}))};
}
const creates=(ops,key)=>(ops||[]).filter(o=>o[key]&&o[key].create).map(o=>o[key].create);
const fields=(ops,f)=>creates(ops,'assetGroupAssetOperation').filter(c=>c.fieldType===f);
const unlinked=ops=>{const used=JSON.stringify(ops.filter(o=>!o.assetOperation));return creates(ops,'assetOperation').filter(c=>!used.includes(JSON.stringify(c.resourceName)));};
const mutations=sent=>sent.filter(s=>/:mutate$/.test(s.url));

(async()=>{
  // ===== 1. Pure rules =====
  const D=S.PMAX_DEFAULT_NEGATIVES.map(x=>x.text);
  check(D.length===16&&!D.includes('free')&&!D.includes('bulk')&&S.PMAX_DEFAULT_NEGATIVES.every(x=>S.validKeyword(x.text)&&['BROAD','PHRASE'].includes(x.matchType)),'the standard exclusions keep "free" (nickel free) and "bulk" (team gifts) searchable');
  check(S.isBrandSearch('Brites necklace')&&S.isBrandSearch('britesjewelry.com')&&S.isBrandSearch('BRITES')&&!S.isBrandSearch('bright necklace')&&!S.isBrandSearch('brightest charms'),'a search for Brites or britesjewelry is a brand search');
  {const campaigns=[{id:'11',name:'Necklaces'},{id:'22',name:'Rings'}],t=(campaignId,term,clicks,cost,conversions=0)=>({campaignId,term,clicks,cost,conversions});
   const plan=S.negativePlan({customerId:'123',campaigns,existing:new Map([['11',new Set(['diy|BROAD','wholesale|PHRASE'])]]),waiting:new Set(['customers/123/campaigns/11|tutorial']),themes:new Map([['11',new Set(['corgi necklace'])]]),
     terms:[t('11','Brites necklace',50,30),t('11','Corgi Necklace',60,40),t('11','cheap necklace chain',20,6),t('11','cheap necklace chain',15,4),t('11','gold necklace',100,50,1),t('11','a b c d e f g h i j k',40,20),t('22','dog tag',29,100),t('22','rings wholesale',31,8.5)]});
   const p11=plan.find(p=>p.campaignId==='11'),p22=plan.find(p=>p.campaignId==='22'),texts=p=>p.adds.map(a=>a.text);
   check(p11.adds.filter(a=>a.reason==='default').length===13&&!['diy','wholesale','tutorial'].some(x=>texts(p11).includes(x)),'a campaign gets only the standard exclusions it lacks and none already waiting in a draft');
   check(JSON.stringify(p11.adds.filter(a=>a.reason==='waste'))===JSON.stringify([{text:'cheap necklace chain',matchType:'EXACT',reason:'waste',clicks:35,cost:10}]),'a search is excluded (exact) only with 30+ clicks and 8+ spent over the window and no sale, summed across rows');
   check(!texts(p11).some(x=>/brites|corgi|gold/.test(x))&&!texts(p11).includes('a b c d e f g h i j k'),'brand searches, search themes, converting searches and keywords Google cannot take are never excluded');
   check(p22.adds.length===17&&texts(p22).includes('rings wholesale')&&!texts(p22).includes('dog tag'),'each campaign is planned from its own searches');
   const many=S.negativePlan({customerId:'123',campaigns:[{id:'11',name:'N'}],terms:Array.from({length:60},(_,i)=>t('11','term '+i,40,10+i))});
   const w=many[0].adds.filter(a=>a.reason==='waste');check(w.length===50&&w[0].text==='term 59'&&w[49].text==='term 10','at most 50 wasted searches per draft, the costliest first');}
  {const spend=S.brandSpend([{campaignId:'11',term:'brites necklace',clicks:10,cost:5.5,conversions:1},{campaignId:'11',term:'Brites',clicks:4,cost:2.06},{campaignId:'22',term:'fox necklace',clicks:9,cost:9}]);
   check(spend.length===1&&spend[0].campaignId==='11'&&spend[0].clicks===14&&spend[0].cost===7.56&&spend[0].terms.length===2,'brand spend is summed per campaign from brand searches only');}
  {const b=(id,name,urls,state)=>({id,name,urls,state});
   check(S.pickBrand([b('9','Brites Paint Co',['britespaint.example']),b('1','Brites',['https://www.britesjewelry.com'],'APPROVED')]).entityId==='1','Google\'s brand on the store domain is chosen');
   check(S.pickBrand([b('1','Brites',['https://britesjewelry.com'],'DEPRECATED')])===null&&S.pickBrand([b('1','Brites',[]),b('2','Brites Studio',[])])===null&&S.pickBrand([b('3','The Brites',['britesjewelry.com'])])===null,'a retired, ambiguous or differently named brand is never guessed');
   check(S.pickBrand([b('4','Brites',[])]).entityId==='4','a single Brites brand without web addresses is used');}
  {const brand={entityId:'b-777'},ops=S.brandExclusionOps({customerId:'123',list:null,brand,campaignIds:['11','22']});
   check(JSON.stringify(ops)===JSON.stringify([{sharedSetOperation:{create:{resourceName:'customers/123/sharedSets/-1',name:'Brites · brand exclusions',type:'BRANDS'}}},{sharedCriterionOperation:{create:{sharedSet:'customers/123/sharedSets/-1',brand:{entityId:'b-777'}}}},{campaignCriterionOperation:{create:{campaign:C(11),negative:true,brandList:{sharedSet:'customers/123/sharedSets/-1'}}}},{campaignCriterionOperation:{create:{campaign:C(22),negative:true,brandList:{sharedSet:'customers/123/sharedSets/-1'}}}}]),'a new BRANDS list holds the brand and each campaign excludes the list');
   const reuse=S.brandExclusionOps({customerId:'123',list:{resourceName:'customers/123/sharedSets/5',hasBrand:true},brand,campaignIds:['11']}),fill=S.brandExclusionOps({customerId:'123',list:{resourceName:'customers/123/sharedSets/5',hasBrand:false},brand,campaignIds:['11']});
   check(reuse.length===1&&reuse[0].campaignCriterionOperation.create.brandList.sharedSet==='customers/123/sharedSets/5'&&fill.length===2&&fill[0].sharedCriterionOperation.create.sharedSet==='customers/123/sharedSets/5','a list already holding the brand is only excluded; an empty one is filled first');
   check(S.brandExclusionOps({customerId:'123',list:null,brand,campaignIds:['11'],listName:'Brites · brand exclusions 2'})[0].sharedSetOperation.create.name==='Brites · brand exclusions 2','a new list can take a free name');}
  {const t=(o={})=>({id:'1',merchantId:'555',feedLabel:'US',status:'ENABLED',assetGroups:2,countryCodes:['US'],countries:['2840'],budget:20,name:'A',...o});
   check(S.eligibleTarget(t())&&!S.eligibleTarget(t({servingStatus:'ENDED'}))&&!S.eligibleTarget(t({primaryStatus:'ENDED'}))&&!S.eligibleTarget(t({assetGroups:100}))&&!S.eligibleTarget(t({merchantId:null}))&&!S.eligibleTarget(t({status:'REMOVED'})),'an ended, full, removed or feedless campaign is never offered');
   check(CS.pmaxTargetFits(t(),{feedLabel:'us'})&&CS.pmaxTargetFits(t({feedLabel:null}),{feedLabel:'CA'})&&!CS.pmaxTargetFits(t(),{feedLabel:'CA'})&&!CS.pmaxTargetFits(t(),{feedLabel:'US',merchantId:'777'}),'a campaign fits a product on its own feed (a campaign without a feed label advertises every feed)');
   const list=[t({id:'1',status:'PAUSED',budget:50}),t({id:'2',budget:10}),t({id:'3',budget:30}),t({id:'4',budget:90,countryCodes:['US','CA'],countries:['2840','2124']}),t({id:'5',budget:99,feedLabel:null})];
   check(CS.pmaxDefaultTarget(list,{feedLabel:'US',countryCodes:['US']}).id==='3'&&CS.pmaxDefaultTarget(list,{feedLabel:'US',countries:['2840']}).id==='3','the default is a running campaign on the same feed and exactly the same countries, the larger budget first');
   check(CS.pmaxDefaultTarget(list,{feedLabel:'US',countries:['2124']})===null&&CS.pmaxDefaultTarget(list,{feedLabel:'US'})===null,'no default without the same countries');
   check(CS.pmaxJoinText(t(),5,'CAD')==='CAD 20.00 + 5.00 = 25.00/day combined, shared by 3 product groups.'&&CS.pmaxJoinText(t({status:'PAUSED',sharedBudget:true}),0,'')==='20.00/day combined, shared by 3 product groups and every campaign using this shared budget. The campaign is paused; the product runs once you enable it.','the combined budget reads in plain words');}
  {check(R.selection(['pmax'],{pmax:0},['2840'],{},'101').pmaxTarget==='101'&&R.selection(['pmax'],{pmax:0},['2840'],{},'101').totalDaily===0,'joining a campaign may add nothing to its budget');
   assert.throws(()=>R.selection(['pmax'],{pmax:-1},['2840'],{},'101'),/what to add to the campaign’s daily budget/);assert.throws(()=>R.selection(['pmax'],{pmax:0},['2840'],{pmax:30})),assert.throws(()=>R.selection(['pmax'],{pmax:5},['2840'],{},'12x'),/Choose an existing Performance Max campaign/);
   assert.throws(()=>R.selection(['responsive_display'],{responsive_display:5},['2840'],{responsive_display:30},'101'),/Choose an existing Performance Max campaign/);
   const mixed=R.selection(['responsive_display','pmax'],{responsive_display:5,pmax:2.5},['2840'],{responsive_display:30},'101');
   check(JSON.stringify(mixed.durations)==='{"responsive_display":30}'&&mixed.totalDaily===7.5&&!('pmaxTarget' in R.selection(['pmax'],{pmax:5},['2840'],{pmax:30})),'a joined campaign keeps its own schedule; a new one still needs a positive budget and a run length');n+=3;}
  // The publication guard, from Google's facts at publication against the reviewed draft.
  {const live={id:'101',name:'N',status:'ENABLED',channel:'PERFORMANCE_MAX',merchantId:'555',feedLabel:'US',brandGuidelinesEnabled:false,budgetRes:B(901),budget:20,assetGroupCount:2,assetGroupNames:['AG · Charms'],includedItemIds:['shopify_US_99_999']};
   const guard={campaignId:'101',name:'N',merchantId:'555',feedLabel:'US',brandGuidelinesEnabled:false,budgetRes:B(901),budgetBefore:20,budgetAfter:25,itemIds:['shopify_US_11_101']};
   const group={assetGroupOperation:{create:{resourceName:'customers/123/assetGroups/-3',campaign:C(101),name:'AG · Corgi necklace',status:'ENABLED'}}},raise={campaignBudgetOperation:{update:{resourceName:B(901),amountMicros:25e6},updateMask:'amount_micros'}};
   const P=(l={},g={},ops=[group,raise])=>S.existingTargetProblem({live:{...live,...l},guard:{...guard,...g},ops,customerId:'123',money:v=>'CAD '+Number(v).toFixed(2)});
   check(P()===null&&P({},{budgetAfter:20},[group])===null&&P({budget:21},{budgetAfter:20},[group])===null,'a draft that still describes Google passes (without a raise the budget may move)');
   const cases=[[P({status:'REMOVED'}),/no longer in Google Ads/],[P({servingStatus:'ENDED'}),/has ended/],[P({merchantId:'777'}),/different product feed/],[P({feedLabel:'CA'}),/different product feed/],[P({brandGuidelinesEnabled:null}),/did not confirm the brand guidelines/],[P({brandGuidelinesEnabled:true}),/brand guidelines setting of “N” changed/],
     [P({},{},[group,raise,{campaignOperation:{update:{resourceName:C(101),status:'ENABLED'},updateMask:'status'}}]),/may only add product groups and the reviewed budget/],[P({},{},[group,{campaignBudgetOperation:{update:{resourceName:B(901),amountMicros:26e6},updateMask:'amount_micros'}}]),/may only add/],[P({},{},[group,raise,raise]),/may only add/],[P({},{},[group]),/may only add/],
     [P({},{},[group,raise,{campaignCriterionOperation:{create:{campaign:C(101),negative:true,keyword:{text:'x',matchType:'BROAD'}}}}]),/may only add/],[P({},{},[raise]),/may only add/],
     [P({assetGroupCount:100}),/already has 100 asset groups; Google allows 100/],[P({assetGroupNames:['ag · corgi NECKLACE']}),/same name/],[P({includedItemIds:['SHOPIFY_US_11_101']}),/already advertises one of these products/],
     [P({budgetRes:B(999)}),/now uses a different budget/],[P({budget:22}),/changed after this draft was prepared \(now CAD 22\.00, reviewed from CAD 20\.00\).*never re-based/]];
   check(cases.every(([m,re])=>re.test(String(m))),'every change in Google since review stops publication, in Paul\'s words');
   check(P({},{},[{campaignOperation:{create:{resourceName:C(-2),name:'Display'}}},{campaignBudgetOperation:{create:{resourceName:B(-1),amountMicros:5e6}}},group,raise])===null,'other lanes of a combined plan may create their own new campaigns');}

  // ===== 2. Destinations and the checks before a draft =====
  {const T=setup(),{e,W}=T,targets=await e.E.pmaxCampaignTargets();
   check(JSON.stringify(targets.map(t=>t.id))==='["101","102","104"]','destinations: live Performance Max campaigns of this store with room and a readable budget (not ended, full, deleted, budgetless or another Merchant account)');
   const t=targets[0];check(t.name==='BA · Necklaces PMax'&&t.budget===20&&t.assetGroups===2&&JSON.stringify(t.countryCodes)==='["US"]'&&JSON.stringify(t.countryNames)==='["United States"]'&&t.bidding==='Maximize conversion value'&&t.brandGuidelinesEnabled===false&&t.sharedBudget===false&&!('budgetRes' in t),'each destination carries what the card shows; its budget resource stays on the server');
   check(targets[1].brandGuidelinesEnabled===true&&targets[1].status==='PAUSED'&&targets[2].feedLabel==='CA','brand guidelines and status are read from Google');
   W.brandUnreadable=true;check((await e.E.pmaxCampaignTargets()).length===0&&W.queries.filter(q=>q.includes('campaign.shopping_setting.merchant_id')).slice(-2).map(q=>q.includes('brand_guidelines_enabled')).join()==='true,false','when Google cannot confirm brand guidelines no destination is offered');W.brandUnreadable=false;
   const draft=o=>e.get('_pmaxExistingDraft')({campaignId:'101',merchantId:'555',feedLabel:'US',itemIds:['shopify_US_11_101'],addBudget:5,...o});
   const refusals=[[{campaignId:'108'},/This campaign was deleted/],[{campaignId:'103'},/“BA · Old PMax” has ended, so the product would never show/],[{campaignId:'105'},/advertises another Merchant Center account/],[{campaignId:'104'},/advertises the CA feed, not this product's feed/],
     [{campaignId:'107'},/already has 100 asset groups; Google allows 100/],[{itemIds:['SHOPIFY_US_99_999']},/already advertises this product in another asset group/],[{campaignId:'109'},/budget of “BA · No budget PMax” could not be verified/],[{campaignId:'999'},/no longer in Google Ads/],[{campaignId:'201'},/no longer in Google Ads/],[{addBudget:200000},/Enter what to add/],[{campaignId:''},/Choose an existing Performance Max campaign/]];
   for(const [o,re] of refusals)await assert.rejects(()=>draft(o),re);
   W.brandUnreadable=true;await assert.rejects(()=>draft({}),/did not confirm whether “BA · Necklaces PMax” uses brand guidelines/);W.brandUnreadable=false;
   check(refusals.length===11,'every destination that cannot take the product is refused before anything is written');
   const ok=await draft({}),u=ok.raise.campaignBudgetOperation;
   check(ok.add===5&&ok.after===25&&ok.spendable===true&&ok.enabledTotal===53&&u.update.resourceName===B(901)&&u.update.amountMicros===25e6&&u.updateMask==='amount_micros'&&Object.keys(u.update).length===2,'the raise is one amount_micros update of that campaign\'s budget; spendable budgets total 53 (shared once, ended excluded)');
   check(JSON.stringify(ok.guard)===JSON.stringify({campaignId:'101',name:'BA · Necklaces PMax',merchantId:'555',feedLabel:'US',brandGuidelinesEnabled:false,budgetRes:B(901),budgetBefore:20,budgetAfter:25,itemIds:['shopify_US_11_101']}),'the draft keeps what publication checks again');
   check((await draft({campaignId:'102'})).spendable===false&&(await draft({addBudget:0})).raise===null&&(await draft({addBudget:0})).guard.budgetAfter===20,'a paused campaign\'s budget cannot spend yet; adding 0 changes no budget');
   const budgets=await e.get('_enabledBudgets')();check(budgets.total===53&&budgets.budgets.size===6&&budgets.budgets.get(B(950))===8&&!budgets.budgets.has(B(903))&&!budgets.budgets.has(B(902)),'the spendable budgets: the shared budget once, the ended and paused campaigns not at all');}

  // ===== 3. The opportunity draft joins a campaign =====
  const G=setup(),gen=(o={})=>G.e.E.generatePmaxApproval({handle:'animal-necklaces',dailyBudget:12,itemIds:['shopify_US_11_101'],productTitles:['Corgi necklace'],feedLabel:'US',existingCampaignId:'101',addBudget:5,...o});
  const joined=await gen(),ja=G.approval(joined.approvalId),jops=ja.payload.mutateOperations,jm=ja.payload.meta;
  check(joined.joined&&joined.joined.campaignId==='101'&&joined.joined.dailyBudget===25&&joined.joined.addedDaily===5,'the draft reports the campaign it joins and the combined budget');
  check(ja.type==='pmax'&&ja.status==='PENDING'&&ja.creative&&ja.creative.phase==='not_started'&&ja.tag===joined.tag,'it waits in Approvals for the creative review like any product ad');
  check(/^PMax · add Animal necklaces to BA · Necklaces PMax · CAD 20\.00 \+ 5\.00 = CAD 25\.00\/day · 1 proven GMC offers · GMC 555$/.test(ja.summary),'the summary names the campaign and the combined budget');
  check(!creates(jops,'campaignOperation').length&&!creates(jops,'campaignBudgetOperation').length&&!jops.some(o=>o.campaignCriterionOperation||o.campaignAssetOperation||o.campaignOperation),'nothing at campaign level is created: no campaign, budget, countries, language, negatives or sitelinks');
  const jg=creates(jops,'assetGroupOperation');
  check(jg.length===1&&jg[0].campaign===C(101)&&jg[0].status==='ENABLED'&&jg[0].name==='AG · Corgi necklace · 2','one product group in the existing campaign, renamed clear of its existing group names');
  check(creates(jops,'assetGroupListingGroupFilterOperation').filter(f=>f.type==='UNIT_INCLUDED').map(f=>f.caseValue.productItemId.value).join()==='shopify_US_11_101','its listing group filter includes exactly the chosen product');
  const jr=jops.filter(o=>o.campaignBudgetOperation);
  check(jr.length===1&&jr[0].campaignBudgetOperation.update.resourceName===B(901)&&jr[0].campaignBudgetOperation.update.amountMicros===25e6,'the reviewed raise is the only budget operation');
  check(fields(jops,'LOGO').length===1&&fields(jops,'BUSINESS_NAME').length===1&&!unlinked(jops).length,'without brand guidelines the group links its own logo and business name; no asset is left unlinked');
  check(jm.kind==='pmaxAddToCampaign'&&jm.existingCampaignId==='101'&&jm.existingCampaignName==='BA · Necklaces PMax'&&jm.existingCampaignStatus==='ENABLED'&&jm.brandGuidelinesEnabled===false&&jm.budgetBefore===20&&jm.addedDaily===5&&jm.dailyBudget===25&&jm.existingAssetGroups===2&&JSON.stringify(jm.targetCountries)==='["United States"]'&&jm.bidding==='Maximize conversion value'&&jm.budgetCurrency==='CAD'&&JSON.stringify(jm.groupNames)==='["AG · Corgi necklace · 2"]','Approvals gets what changes and what stays');
  check(S.existingTargetProblem({live:await G.e.get('_pmaxTargetFacts')('101'),guard:ja.payload.existingPmaxGuard,ops:jops,customerId:'123'})===null,'the draft passes the publication guard as prepared');
  check(G.copyCalls.length===1,'copy is written once (offline, deterministic)');
  // Refusals cost nothing: the destination is checked before copy is written.
  const before=G.approvals().length;
  await assert.rejects(()=>gen({addBudget:9.01}),/Over your daily ceiling: these budgets total CAD 9\.01, but only CAD 9\.00 of your CAD 62\.00 ceiling is free \(enabled campaigns use CAD 53\.00\)/);
  await assert.rejects(()=>gen({existingCampaignId:'105'}),/another Merchant Center account/);
  check(G.copyCalls.length===1&&G.approvals().length===before,'a refused destination or budget writes no copy and no draft');
  check(!!(await gen({addBudget:9})).approvalId,'a raise that fits the ceiling to the cent is drafted');
  const brand=await gen({existingCampaignId:'102',addBudget:20}),bops=G.approval(brand.approvalId).payload.mutateOperations;
  check(!['LOGO','BUSINESS_NAME','LANDSCAPE_LOGO'].some(f=>fields(bops,f).length)&&!creates(bops,'assetOperation').some(c=>c.imageAsset)&&!unlinked(bops).length,'with brand guidelines the group links no logo or business name (they stay on the campaign) and no logo is created');
  check(G.approval(brand.approvalId).payload.meta.brandGuidelinesEnabled===true&&bops.find(o=>o.campaignBudgetOperation).campaignBudgetOperation.update.amountMicros===35e6,'a paused campaign\'s raise is drafted; it is counted when that campaign is enabled');
  const keep=await gen({addBudget:0}),kops=G.approval(keep.approvalId).payload.mutateOperations;
  check(!kops.some(o=>o.campaignBudgetOperation)&&/CAD 20\.00\/day unchanged/.test(G.approval(keep.approvalId).summary),'adding 0 keeps the budget and says so');

  // ===== 4. Publication of a joined product group =====
  const resolve=G.e.get('_resolveBrandLogo');
  G.e.bind({assertCreativeReviewed:()=>{},materializeReviewedCreative:async it=>resolve(clone(it.payload.mutateOperations))});
  const approve=id=>{G.approval(id).status='APPROVED';G.approval(id).lastError=null;},publish=async(id,o={})=>G.e.E.applyApproval(id,{...G.ctrl,...o});
  approve(joined.approvalId);let sent=mutations(G.e.sent).length;
  let res=await publish(joined.approvalId,{dryRun:true}),m=mutations(G.e.sent).slice(sent);
  check(res.status==='VALIDATED'&&m.length===1&&m[0].body.validateOnly===true&&/\/customers\/123\/googleAds:mutate$/.test(m[0].url),'dry run: Google validates the product group and the raise without creating anything');
  const body=m[0].body.mutateOperations;
  check(body.some(o=>o.assetGroupOperation&&o.assetGroupOperation.create.campaign===C(101))&&body.filter(o=>o.campaignBudgetOperation).length===1&&!JSON.stringify(body).includes(G.e.get('BRAND_LOGO_DATA'))&&creates(body,'assetOperation').some(c=>c.imageAsset&&c.name.startsWith('Brites reviewed 1024x1024')),'the validated request carries the new operations with the rendered Brites logo');
  check(G.approval(joined.approvalId).status==='APPROVED'&&G.approval(joined.approvalId).validatedAt>0,'a dry run leaves the draft approved');
  // Live: validated first, the campaign read again, then published.
  G.W.publish=true;sent=mutations(G.e.sent).length;res=await publish(joined.approvalId,{dryRun:false});m=mutations(G.e.sent).slice(sent);
  const labels=[...G.mem.docs.values()].filter(d=>d.kind==='mutateAll').map(d=>d.label).slice(-2);
  check(res.status==='APPLIED'&&m.length===2&&m[0].body.validateOnly===true&&!m[1].body.validateOnly&&labels[0]==='validate-existing-pmax:'+joined.approvalId&&labels[1]==='reviewed-approval:'+joined.approvalId,'publication validates with Google, checks the campaign again, then sends the same request');
  G.W.publish=false;
  // The campaign changes between validation and publication: nothing is sent.
  const second=await gen({addBudget:5});approve(second.approvalId);sent=mutations(G.e.sent).length;
  G.W.onMutate=(url,b)=>{if(b.validateOnly)G.W.camp('101').budget=21;};
  await assert.rejects(()=>publish(second.approvalId,{dryRun:false}),/The budget of “BA · Necklaces PMax” changed after this draft was prepared \(now CAD 21\.00, reviewed from CAD 20\.00\)\. Nothing was changed\. Delete this draft and prepare it again; reviewed budgets are never re-based\./);
  check(mutations(G.e.sent).length===sent+1&&G.approval(second.approvalId).status==='APPROVED'&&/never re-based/.test(G.approval(second.approvalId).lastError),'a budget edited after validation stops publication; only the validation was sent');
  G.W.onMutate=null;G.W.camp('101').budget=20;
  // Every refusal at publication, before any request.
  const refuse=async(change,undo,re,label)=>{change();sent=mutations(G.e.sent).length;await assert.rejects(()=>publish(second.approvalId,{dryRun:true}),re);check(mutations(G.e.sent).length===sent,label);undo();};
  await refuse(()=>{G.W.camp('101').budget=22;},()=>{G.W.camp('101').budget=20;},/changed after this draft was prepared \(now CAD 22\.00, reviewed from CAD 20\.00\)/,'a reviewed budget is never re-based');
  await refuse(()=>{G.W.camp('101').brand=true;},()=>{G.W.camp('101').brand=false;},/brand guidelines setting of “BA · Necklaces PMax” changed/,'a campaign switched to brand guidelines is refused');
  await refuse(()=>{G.W.camp('101').servingStatus='ENDED';},()=>{G.W.camp('101').servingStatus='SERVING';},/has ended, so the product would never show/,'an ended campaign is refused');
  await refuse(()=>{G.W.camp('101').items.push('shopify_US_11_101');},()=>{G.W.camp('101').items.pop();},/already advertises one of these products/,'a product another group now advertises is refused');
  await refuse(()=>{G.W.camp('101').groups.push('AG · Corgi necklace · 2');},()=>{G.W.camp('101').groups.pop();},/now has an asset group with the same name/,'a group name taken since review is refused');
  await refuse(()=>{G.mem.docs.set('Brites_GAds_State/adArchive/campaigns/101',{status:'REMOVED'});},()=>{G.mem.docs.delete('Brites_GAds_State/adArchive/campaigns/101');},/This campaign was deleted/,'a campaign deleted in the console is refused');
  await refuse(()=>{G.W.camp('104').budget=14.5;},()=>{G.W.camp('104').budget=10;},/Over your daily ceiling: these budgets total CAD 5\.00, but only CAD 4\.50 of your CAD 62\.00 ceiling is free \(enabled campaigns use CAD 57\.50\)\..*Nothing was published/,'the raise counts against the daily ceiling with live figures');
  await refuse(()=>{G.W.mtd=1000;},()=>{G.W.mtd=0;},/Month-to-date spend \(USD 1000\.00\) has reached your monthly stop threshold of USD 1000/,'no budget is raised once the monthly stop is reached');
  // A paused campaign's raise waits for enabling; adding nothing needs no room.
  approve(brand.approvalId);G.W.mtd=1000;
  check((await publish(brand.approvalId,{dryRun:true})).status==='VALIDATED','a paused campaign\'s raise is not counted now (it cannot spend)');
  approve(keep.approvalId);check((await publish(keep.approvalId,{dryRun:true})).status==='VALIDATED','a product group that adds no budget publishes past the monthly stop');G.W.mtd=0;
  G.W.camp('102').budget=35;
  await assert.rejects(()=>G.e.get('_assertSpendLimits')('102',{ctrl:G.ctrl,enabling:true}),/This would raise the daily budgets that can spend to CAD 88\.00, over your daily ceiling of CAD 62\.00/);
  check(true,'enabling the paused campaign later counts its raised budget against the ceiling');G.W.camp('102').budget=15;

  // ===== 5. Complete-ad chooser: preparing a plan that joins a campaign =====
  {const T=setup(),{e,W,mem}=T,sharp=realRequire('sharp'),root=mem.db.collection('workspaces').doc('test'),assets={};
   for(const [shape,width,height] of [['square',600,600],['landscape',1200,628],['portrait',600,750]]){const bytes=await sharp({create:{width,height,channels:3,background:'#ddd'}}).jpeg().toBuffer(),p='Brites_GAds_Creative/test/'+shape+'.jpg';assets[shape]={path:p,width,height,bytes:bytes.length,hash:e.E.creativeHash(bytes.toString('base64'))};mem.files.set(p,bytes);}
   const copy={headlines:['Peach Charm','For Your Favorite Foodie','A Playful Gift'],longHeadlines:['Give a playful peach charm'],descriptions:['Shop the peach charm at Brites Jewelry.','Choose your favorite metal.']};
   const w={context:{itemIds:['shopify_US_1_2'],handle:'charms',feedLabel:'US',countries:['2840']},settings:{productId:'1',groupRef:'g'},job:{result:{assets}}};await root.set(w);
   const product={id:'1',title:'Peach Charm',url:'https://britesjewelry.com/products/peach',offerIds:['shopify_US_1_2']},item={sourceHash:'source',designReview:{workspaceId:'test',copy}};
   e.bind({_adDesignWorkspaceRef:()=>root,_reportContext:async()=>({budgetCurrency:'CAD'}),_saveCreativeAsset:async(ws,bytes,name,meta)=>{const p='Brites_GAds_Creative/test/'+name+'.jpg';mem.files.set(p,bytes);return {...meta,path:p,bytes:bytes.length,hash:e.E.creativeHash(bytes.toString('base64'))};}});
   const draft=o=>e.get('_pmaxExistingDraft')({campaignId:'101',merchantId:'555',feedLabel:'US',itemIds:['shopify_US_1_2'],addBudget:5,...o}),prep=(choice,join,id='c')=>e.get('_prepareCampaignStyles')({item,context:{w,product},choice,identity:id.repeat(64),join});
   const join=await draft({}),plan=await prep(R.selection(['responsive_display','pmax'],{responsive_display:5,pmax:5},['2840'],{responsive_display:30},'101'),join),ops=plan.payload.mutateOperations;
   const pj=plan.summary.campaigns.find(c=>c.joins),pd=plan.summary.campaigns.find(c=>!c.joins);
   check(pj&&pj.name==='BA · Necklaces PMax'&&pj.existingCampaignId==='101'&&pj.campaignStatus==='ENABLED'&&pj.budgetBefore===20&&pj.addedDaily===5&&pj.dailyBudget===25&&JSON.stringify(pj.countries)==='["United States"]'&&pj.bidding==='Maximize conversion value · unchanged'&&pd.style==='responsive_display'&&plan.summary.pmaxTarget==='101','the plan shows the joined campaign with its combined budget beside the new Display campaign');
   check(creates(ops,'campaignOperation').length===1&&creates(ops,'campaignOperation')[0].advertisingChannelType==='DISPLAY'&&creates(ops,'campaignBudgetOperation').length===1&&ops.filter(o=>o.campaignBudgetOperation&&o.campaignBudgetOperation.update).length===1,'only the Display campaign is created; Performance Max gets one product group and one budget update');
   const g=creates(ops,'assetGroupOperation');check(g.length===1&&g[0].campaign===C(101)&&!['AG · Corgi necklace','AG · Charms'].includes(g[0].name)&&['LOGO','BUSINESS_NAME','LANDSCAPE_LOGO'].every(f=>fields(ops,f).some(c=>c.assetGroup===g[0].resourceName)),'the product group joins the campaign with its own logos and business name (brand guidelines off)');
   const made=new Set([...ops.map(o=>(Object.values(o)[0]||{}).create&&Object.values(o)[0].create.resourceName).filter(Boolean),...plan.payload.generatedAssets.map(a=>a.tempResourceName)]);
   check([...JSON.stringify(ops).matchAll(/customers\/123\/(?:assets|campaigns|campaignBudgets|assetGroups|adGroups)\/-\d+/g)].every(x=>made.has(x[0])),'every temporary reference resolves inside the plan');
   check(S.existingTargetProblem({live:await e.get('_pmaxTargetFacts')('101'),guard:plan.payload.existingPmaxGuard,ops,customerId:'123'})===null&&plan.payload.meta.existingCampaignId==='101','the combined plan passes the publication guard');
   check(plan.summary.note==='Only “BA · Necklaces PMax” changes: it gains this product group and the reviewed budget, and the group can serve as soon as it is published because the campaign is running. Other campaigns are unchanged.','the plan says plainly what changes and when it serves');
   const join2=await draft({campaignId:'102',addBudget:0}),plan2=await prep(R.selection(['pmax'],{pmax:0},['2840'],{},'102'),join2,'d'),ops2=plan2.payload.mutateOperations;
   check(!['LOGO','BUSINESS_NAME','LANDSCAPE_LOGO'].some(f=>fields(ops2,f).length)&&!ops2.some(o=>o.campaignBudgetOperation||o.campaignOperation)&&plan2.payload.generatedAssets.every(a=>!(a.asset.kind==='brand logo')),'with brand guidelines no logo is linked or uploaded, and adding 0 sends no budget change');
   check(plan2.summary.note==='Only “BA · Rings PMax” changes: it gains this product group; the group runs once you enable that campaign. Other campaigns are unchanged.','a paused destination says the group runs once the campaign is enabled');
   const mat=await e.get('materializeReviewedCreative')({type:'adDesignSubmission',payload:plan2.payload,pipelinePlan:plan2,pipelineReview:{hash:plan2.hash}});
   check(creates(mat,'assetOperation').filter(c=>c.imageAsset).length===plan2.payload.generatedAssets.length&&!unlinked(mat).length&&S.existingTargetProblem({live:await e.get('_pmaxTargetFacts')('102'),guard:plan2.payload.existingPmaxGuard,ops:mat,customerId:'123'})===null,'materialized for publication, every uploaded image is linked and the guard still passes');
   // Preparing: the ceiling counts a running campaign's raise, a changed campaign is prepared again.
   const id='design-review-'+'a'.repeat(32),reviewHash='r'.repeat(64),prepared=[];
   await mem.db.collection('Brites_GAds_Approvals').doc(id).set({type:'adDesignSubmission',status:'PENDING',reviewHash,sourceHash:e.get('_adDesignSelectionHash')(w),designReview:{workspaceId:'test',context:w.context}});
   e.bind({_adDesignPublicationContext:async()=>({w,product}),_prepareCampaignStyles:async({identity,join})=>{prepared.push(join);return {identity,hash:'plan-'+prepared.length,payload:{},summary:{campaigns:[]}};}});
   const ask=(target,budgets,styles=['pmax'],durations={pmax:0})=>e.E.publishAdDesignSubmission({id,hash:reviewHash,prepareOnly:true,styles,budgets,countries:['2840'],durations,pmaxTarget:target});
   await assert.rejects(()=>ask('101',{pmax:9.01}),/Over your daily ceiling: these budgets total CAD 9\.01, but only CAD 9\.00 of your CAD 62\.00 ceiling is free \(enabled campaigns use CAD 53\.00\)/);
   await assert.rejects(()=>ask('101',{responsive_display:5,pmax:4.01},['responsive_display','pmax'],{responsive_display:30}),/these budgets total CAD 9\.01/);
   await assert.rejects(()=>ask('105',{pmax:1}),/another Merchant Center account/);await assert.rejects(()=>ask('1x',{pmax:1}),/Choose an existing Performance Max campaign, or a new campaign/);
   check(prepared.length===0,'over the ceiling or to a wrong destination, nothing is prepared');
   check((await ask('101',{pmax:9})).planHash==='plan-1'&&prepared[0].target.id==='101'&&prepared[0].add===9,'a raise that fits is prepared with the campaign as read');
   check((await ask('101',{pmax:9})).cached===true&&prepared.length===1,'the same choice on an unchanged campaign reuses its plan');
   W.camp('101').groups.push('AG · Earrings');
   check((await ask('101',{pmax:9})).planHash==='plan-2'&&prepared.length===2,'a campaign that changed since is prepared again');
   check((await ask('102',{pmax:30})).planHash==='plan-3','a paused campaign\'s raise is not counted while preparing (it is checked when enabling)');}

  // ===== 6. Mining Performance Max search terms =====
  const M=(()=>{const W=account();W.campaigns=W.campaigns.filter(c=>['101','102','103','108','201','202'].includes(c.id));
    const t=(campaignId,term,clicks,cost,conversions=0)=>({campaignId,term,clicks,cost,conversions});
    W.terms=[t('101','brites necklace',40,25),t('101','Corgi Necklace',50,30),t('101','cheap chain',20,7),t('101','cheap chain',15,2),t('101','gold necklace',100,60,2),t('101','britesjewelry',31,9),t('102','brites rings',10,5),t('103','ended search',99,99),t('108','deleted search',99,99)];
    W.negatives=[{campaignId:'101',text:'diy',matchType:'BROAD'},{campaignId:'101',text:'wholesale',matchType:'PHRASE'}];W.themes=[{campaignId:'101',text:'corgi necklace'}];
    const T=setup({W}),neg=(campaign,text)=>({create:{campaign:C(campaign),negative:true,keyword:{text,matchType:'BROAD'}}});
    T.mem.docs.set('Brites_GAds_Approvals/waiting',{type:'negatives',status:'PENDING',payload:{service:'campaignCriteria',operations:[neg('102','tutorial')],meta:{kind:'pmaxNegatives'}}});
    T.mem.docs.set('Brites_GAds_Approvals/declined',{type:'negatives',status:'REJECTED',deletedAt:Date.now()-5*DAY,payload:{service:'campaignCriteria',operations:[neg('101','repair')],meta:{kind:'pmaxNegatives'}}});
    T.mem.docs.set('Brites_GAds_Approvals/old',{type:'negatives',status:'REJECTED',deletedAt:Date.now()-40*DAY,payload:{service:'campaignCriteria',operations:[neg('101','jobs')],meta:{kind:'pmaxNegatives'}}});
    T.mem.docs.set('Brites_GAds_Approvals/archived',{type:'negatives',status:'PENDING',archivedAt:Date.now(),payload:{service:'campaignCriteria',operations:[neg('101','hiring')],meta:{kind:'pmaxNegatives'}}});
    return T;})();
  const mined=await M.e.E.minePmaxSearchTerms({ctrl:M.ctrl}),drafts=M.approvals().filter(a=>a.payload.meta&&a.payload.meta.kind==='pmaxNegatives'&&a.status==='PENDING'&&!a.archivedAt&&a.id!=='waiting');
  check(mined.campaigns===2&&mined.queued===3&&mined.negatives===29&&drafts.length===2,'one exclusions draft per live campaign (ended and deleted campaigns skipped), plus the brand exclusion');
  const d101=drafts.find(a=>a.payload.meta.existingCampaignId==='101'),d102=drafts.find(a=>a.payload.meta.existingCampaignId==='102'),words=a=>a.payload.operations.map(o=>o.create.keyword.text);
  check(d101.type==='negatives'&&d101.vetted===false&&d101.creative===null&&d101.summary==='PMax negatives · BA · Necklaces PMax · 13 standard exclusions + 1 search that spent without a sale','the draft says what it excludes and why');
  check(d101.payload.service==='campaignCriteria'&&d101.payload.operations.every(o=>o.create.campaign===C(101)&&o.create.negative===true&&o.create.keyword.text&&o.create.keyword.matchType)&&d101.payload.operations.find(o=>o.create.keyword.text==='cheap chain').create.keyword.matchType==='EXACT','each exclusion is a negative keyword on that campaign; a wasted search is excluded exactly');
  check(['diy','wholesale','repair'].every(x=>!words(d101).includes(x))&&['jobs','hiring'].every(x=>words(d101).includes(x))&&!words(d102).includes('tutorial')&&words(d102).length===15,'nothing already excluded, waiting, or deleted in the last 30 days is proposed; older or archived ones are');
  check(![...words(d101),...words(d102)].some(x=>/brites|corgi|gold/.test(x)),'searches for Brites, search themes and converting searches are never excluded');
  const m101=d101.payload.meta;check(m101.campaignName==='BA · Necklaces PMax'&&m101.days===90&&m101.wasteCost===8&&m101.wasteClicks===30&&m101.budgetCurrency==='CAD'&&m101.terms.find(x=>x.text==='cheap chain').clicks===35,'the draft keeps the evidence Approvals shows');
  const tq=M.W.queries.find(q=>q.includes('FROM campaign_search_term_view'));
  check(/campaign\.id IN \(101, 102\)/.test(tq)&&/segments\.date BETWEEN '\d{4}-\d{2}-\d{2}' AND '\d{4}-\d{2}-\d{2}'/.test(tq)&&!M.W.queries.some(q=>/FROM search_term_view/.test(q)),'Performance Max search terms come from campaign_search_term_view over 90 days');
  const again=await M.e.E.minePmaxSearchTerms({ctrl:M.ctrl});
  check(again.queued===0&&again.negatives===0&&/already waiting/.test(again.brand.skipped),'mining again proposes nothing that is already waiting');
  // Dry run of the exclusions: Google validates the exact negative keywords.
  M.approval(d101.id).status='APPROVED';let ms=mutations(M.e.sent).length;
  check((await M.e.E.applyApproval(d101.id,{...M.ctrl,dryRun:true})).status==='VALIDATED','the exclusions validate in dry run');
  let mm=mutations(M.e.sent).slice(ms);
  check(mm.length===1&&/\/customers\/123\/campaignCriteria:mutate$/.test(mm[0].url)&&mm[0].body.validateOnly===true&&JSON.stringify(mm[0].body.operations)===JSON.stringify(d101.payload.operations),'dry run sends exactly the reviewed negative keywords, validate-only');
  // No exclusion goes twice: one added to the campaign since the draft was prepared is left out.
  M.W.negatives.push({campaignId:'101',text:'JOBS',matchType:'BROAD'});M.approval(d101.id).status='APPROVED';ms=mutations(M.e.sent).length;
  check((await M.e.E.applyApproval(d101.id,{...M.ctrl,dryRun:true})).status==='VALIDATED','the exclusions still validate when one is already on the campaign');mm=mutations(M.e.sent).slice(ms);
  check(mm.length===1&&JSON.stringify(mm[0].body.operations)===JSON.stringify(d101.payload.operations.filter(o=>o.create.keyword.text!=='jobs')),'the search excluded since is left out; the others go as reviewed');
  M.W.negatives.push(...d101.payload.operations.map(o=>({campaignId:'101',text:o.create.keyword.text,matchType:o.create.keyword.matchType})));M.approval(d101.id).status='APPROVED';ms=mutations(M.e.sent).length;
  await assert.rejects(()=>M.e.E.applyApproval(d101.id,{...M.ctrl,dryRun:true}),/Every search in this draft is already excluded from “BA · Necklaces PMax”\. Nothing was changed\. Delete this draft\./);
  check(mutations(M.e.sent).length===ms,'when every search is already excluded, nothing is sent');M.W.negatives=M.W.negatives.slice(0,2);

  // ===== 7. Brand exclusion =====
  const bx=M.approvals().find(a=>a.payload.meta&&a.payload.meta.kind==='pmaxBrandExclusion'),bm=bx.payload.meta,suggest=M.e.sent.filter(s=>/:suggestBrands$/.test(s.url));
  check(suggest.length===1&&suggest[0].body.brandPrefix==='Brites'&&/^https:\/\/googleads\.googleapis\.com\/v24\/customers\/123:suggestBrands$/.test(suggest[0].url)&&M.W.suggestCalls===1,'Google\'s brand for Brites is looked up once with SuggestBrands (read-only)');
  check(JSON.stringify(bx.payload.mutateOperations)===JSON.stringify(S.brandExclusionOps({customerId:'123',list:null,brand:{entityId:'b-777'},campaignIds:['101','102']})),'no brand list yet: one is created with the Brites brand and both campaigns exclude it');
  check(bx.type==='negatives'&&bx.creative===null&&bx.summary==='PMax brand exclusion · Brites · 2 campaigns · CAD 39.00 spent on brand searches in 90 days','the draft states the brand, the campaigns and what brand searches cost');
  check(bm.brand.entityId==='b-777'&&bm.brand.name==='Brites'&&bm.list.name==='Brites · brand exclusions'&&bm.list.reused===false&&bm.list.addsBrand===true&&JSON.stringify(bm.campaigns)==='[{"id":"101","name":"BA · Necklaces PMax","status":"ENABLED"},{"id":"102","name":"BA · Rings PMax","status":"PAUSED"}]'&&bm.brandCost===39&&bm.days===90&&bm.budgetCurrency==='CAD','Approvals gets the brand, the list and the campaigns');
  check(bm.brandSearch.state==='live'&&bm.brandSearch.name==='BA · brand-search','it pairs with the running brand Search campaign');
  M.approval(bx.id).status='APPROVED';ms=mutations(M.e.sent).length;
  check((await M.e.E.applyApproval(bx.id,{...M.ctrl,dryRun:true})).status==='VALIDATED','the brand exclusion validates in dry run');mm=mutations(M.e.sent).slice(ms);
  check(mm.length===1&&/googleAds:mutate$/.test(mm[0].url)&&mm[0].body.validateOnly===true&&JSON.stringify(mm[0].body.mutateOperations)===JSON.stringify(bx.payload.mutateOperations),'dry run sends exactly the reviewed list and exclusions, validate-only');
  M.W.lists=[{resourceName:'customers/123/sharedSets/501',name:'My brand',brands:['b-777']}];M.W.brandExclusions=[{campaignId:'101',list:'customers/123/sharedSets/501'}];M.approval(bx.id).status='APPROVED';ms=mutations(M.e.sent).length;
  check((await M.e.E.applyApproval(bx.id,{...M.ctrl,dryRun:true})).status==='VALIDATED','the brand exclusion still validates when one campaign already excludes Brites');mm=mutations(M.e.sent).slice(ms);
  check(mm.length===1&&JSON.stringify(mm[0].body.mutateOperations)===JSON.stringify(bx.payload.mutateOperations.filter(o=>!(o.campaignCriterionOperation&&o.campaignCriterionOperation.create.campaign===C(101)))),'a campaign that excludes Brites since the draft was prepared is left out; the other gets the list');
  M.W.brandExclusions.push({campaignId:'102',list:'customers/123/sharedSets/501'});M.approval(bx.id).status='APPROVED';ms=mutations(M.e.sent).length;
  await assert.rejects(()=>M.e.E.applyApproval(bx.id,{...M.ctrl,dryRun:true}),/Every campaign in this draft already excludes searches for Brites\. Nothing was changed\. Delete this draft\./);
  check(mutations(M.e.sent).length===ms,'when every campaign already excludes Brites, nothing is sent (not even the list)');M.W.lists=[];M.W.brandExclusions=[];
  {const T=setup(),{e,W}=T;W.lists=[{resourceName:'customers/123/sharedSets/503',name:'Brites · brand exclusions',brands:[]}];
   const r=await e.get('_draftPmaxBrandExclusion')({campaigns:[{id:'101',name:'BA · Necklaces PMax',status:'ENABLED'}],terms:[],state:{waiting:new Set(),brandWaiting:false,brandDeclined:false,brandSearchDraft:false},currency:'CAD'});
   check(T.approval(r.approvalId).payload.mutateOperations.length===2,'the console\'s empty list is to be filled, then excluded');
   W.lists[0].brands=['b-777'];T.approval(r.approvalId).status='APPROVED';const before=mutations(e.sent).length;
   check((await e.E.applyApproval(r.approvalId,{...T.ctrl,dryRun:true})).status==='VALIDATED'&&JSON.stringify(mutations(e.sent).slice(before)[0].body.mutateOperations)===JSON.stringify([{campaignCriterionOperation:{create:{campaign:C(101),negative:true,brandList:{sharedSet:'customers/123/sharedSets/503'}}}}]),'a list that holds the brand by publication is not filled again');}
  // A new Performance Max campaign copies the account's brand lists as it is created (the campaign options' step),
  // with its own purchase goals; the builder adds no brand exclusion of its own, and its standard exclusions go once.
  {const T=setup(),{e,W}=T;W.lists=[{resourceName:'customers/123/sharedSets/501',name:'Brites · brand exclusions',brands:['b-777']}];W.brandExclusions=[{campaignId:'101',list:'customers/123/sharedSets/501'}];
   W.goals=[{category:'PURCHASE',origin:'WEBSITE',biddable:true},{category:'ADD_TO_CART',origin:'WEBSITE',biddable:true}];
   const fresh=await e.E.generatePmaxApproval({handle:'animal-necklaces',dailyBudget:5,itemIds:['shopify_US_11_101'],productTitles:['Corgi necklace'],feedLabel:'US'}),built=T.approval(fresh.approvalId).payload.mutateOperations;
   check(!creates(built,'campaignCriterionOperation').some(c=>c.brandList),'the new campaign draft carries no brand exclusion of its own');
   const resolveLogo=e.get('_resolveBrandLogo');e.bind({assertCreativeReviewed:()=>{},materializeReviewedCreative:async it=>resolveLogo(clone(it.payload.mutateOperations))});
   T.approval(fresh.approvalId).status='APPROVED';const before=mutations(e.sent).length;
   check((await e.E.applyApproval(fresh.approvalId,{...T.ctrl,dryRun:true})).status==='VALIDATED','a new Performance Max draft validates with the goal and brand list reads answered');
   const sentOps=mutations(e.sent).slice(before)[0].body.mutateOperations,cc=creates(sentOps,'campaignCriterionOperation'),lists=cc.filter(c=>c.brandList);
   check(lists.length===1&&lists[0].campaign===C(-2)&&lists[0].negative===true&&lists[0].brandList.sharedSet==='customers/123/sharedSets/501','it excludes the account\'s Brites list once, copied as it is created');
   check(cc.filter(c=>c.negative&&c.keyword).length===16&&new Set(cc.filter(c=>c.keyword).map(c=>c.keyword.text)).size===16&&cc.filter(c=>c.language).length===1,'its 16 standard exclusions and English go once');
   check(sentOps.filter(o=>o.campaignConversionGoalOperation).map(o=>o.campaignConversionGoalOperation.update.biddable).join()==='true,false','its own goals: purchase stays biddable, add to cart is off');}
  // A Design Studio Performance Max campaign is built at publication: after its countries it serves on English
  // searches and carries the same standard exclusions, and its draft says so before then.
  {const T=setup(),{e}=T,G=i=>'customers/123/assetGroups/-'+(3+i),img=i=>'customers/123/assets/-'+(900001+i),groups={};
   [0,1,2].forEach(i=>{groups[G(i)]={logo:img(0),square:[img(1)],landscape:[img(2)],portrait:[img(3)]};});
   e.bind({_creativeImageOps:async()=>({ops:[],groups,searchGroups:{}}),_enabledBudgetTotal:async()=>40,takenTags:async()=>({}),buildDesignStudioSearchCampaignOps:()=>({ops:[],keywordSummary:{count:6},adGroupSummary:[]}),
     scanDesignStudioOpportunity:async()=>({blueprint:{measurement:{readiness:{apiOk:true,purchaseReady:true}},budget:{recommendedDaily:10,countries:['2124']},pmax:{groups:[{name:'Gifts',angle:'a',searchThemes:['custom charm gift'],headlines:['A'],longHeadlines:['B'],descriptions:['C']}]},search:{maxCpc:1.1},images:{}}})});
   const b=await e.E.buildDesignStudioPmaxCampaignOps({dailyBudget:5,startDate:'2026-10-01',endDate:'2026-12-30',countries:['2124','2840'],reviewedCreative:{groups:[]}},{ctrl:T.ctrl});
   const cc=creates(b.ops,'campaignCriterionOperation'),at=f=>b.ops.findIndex(o=>o.campaignCriterionOperation&&o.campaignCriterionOperation.create[f]),lastCountry=b.ops.map(o=>!!(o.campaignCriterionOperation&&o.campaignCriterionOperation.create.location)).lastIndexOf(true),lang=cc.find(c=>c.language);
   check(cc.filter(c=>c.location).length===2&&cc.filter(c=>c.language).length===1&&lang.campaign===C(-2)&&lang.language.languageConstant==='languageConstants/1000'&&at('language')>lastCountry,'a Studio Performance Max campaign serves on English searches, added after its countries');
   check(JSON.stringify(cc.filter(c=>c.keyword).map(c=>[c.campaign,c.negative,c.keyword.text,c.keyword.matchType]))===JSON.stringify(S.PMAX_DEFAULT_NEGATIVES.map(x=>[C(-2),true,x.text,x.matchType]))&&at('keyword')>at('language'),'and excludes the same 16 standard searches once');
   check(b.languages.join()==='English'&&JSON.stringify(b.negatives)===JSON.stringify(S.PMAX_DEFAULT_NEGATIVES.map(x=>x.text)),'the build reports its language and exclusions');
   await e.E.generateDesignStudioApprovals({pmaxDaily:6,searchDaily:4});const sm=((T.approvals().find(a=>((a.payload||{}).meta||{}).kind==='designStudioPmax')||{}).payload||{}).meta||{};
   check((sm.languages||[]).join()==='English'&&JSON.stringify(sm.negatives)===JSON.stringify(S.PMAX_DEFAULT_NEGATIVES.map(x=>x.text)),'the Studio draft names English and its exclusions for its card, before its operations exist');}
  {const T=setup(),{e,W,mem}=T,campaigns=[{id:'101',name:'BA · Necklaces PMax',status:'ENABLED'},{id:'102',name:'BA · Rings PMax',status:'PAUSED'}],fresh={waiting:new Set(),brandWaiting:false,brandDeclined:false,brandSearchDraft:false};
   const draftBrand=async(state={})=>{const r=await e.get('_draftPmaxBrandExclusion')({campaigns,terms:[],state:{...fresh,...state},currency:'CAD'});return r.approvalId?{...r,a:T.approval(r.approvalId)}:r;};
   const L=(id,name,brands)=>({resourceName:'customers/123/sharedSets/'+id,name,brands});
   W.lists=[L(501,'My brand',['b-777']),L(502,'Other brands',['b-777','b-900'])];let r=await draftBrand();
   check(r.a.payload.mutateOperations.length===2&&r.a.payload.mutateOperations.every(o=>o.campaignCriterionOperation.create.brandList.sharedSet==='customers/123/sharedSets/501')&&r.a.payload.meta.list.reused&&!r.a.payload.meta.list.addsBrand&&r.a.payload.meta.list.name==='My brand','a list holding only Brites is reused as it is');
   W.lists=[L(501,'My brand',['b-777']),L(503,'Brites · brand exclusions',['b-777'])];r=await draftBrand();
   check(r.a.payload.mutateOperations.every(o=>o.campaignCriterionOperation.create.brandList.sharedSet==='customers/123/sharedSets/503'),'the console\'s own list is preferred');
   W.lists=[L(502,'Other brands',['b-777','b-900'])];r=await draftBrand();
   check(r.a.payload.mutateOperations[0].sharedSetOperation.create.name==='Brites · brand exclusions','a list with other brands is never reused (it would exclude them too)');
   W.lists=[L(503,'Brites · brand exclusions',['b-777','b-900']),L(504,'Brites · brand exclusions 2',[])];r=await draftBrand();
   check(r.a.payload.mutateOperations[0].sharedSetOperation.create.name==='Brites · brand exclusions 3'&&r.a.payload.meta.list.name==='Brites · brand exclusions 3','a new list takes a free name when the console\'s name is taken');
   W.lists=[L(503,'Brites · brand exclusions',[])];r=await draftBrand();
   check(r.a.payload.mutateOperations[0].sharedCriterionOperation.create.sharedSet==='customers/123/sharedSets/503'&&r.a.payload.mutateOperations.length===3&&r.a.payload.meta.list.reused&&r.a.payload.meta.list.addsBrand,'the console\'s empty list is filled first');
   W.lists=[L(501,'My brand',['b-777'])];W.brandExclusions=[{campaignId:'101',list:'customers/123/sharedSets/501'}];r=await draftBrand();
   check(r.a.payload.mutateOperations.length===1&&r.a.payload.mutateOperations[0].campaignCriterionOperation.create.campaign===C(102)&&JSON.stringify(r.a.payload.meta.campaigns.map(c=>c.id))==='["102"]','a campaign that already excludes Brites is left as it is');
   W.brandExclusions.push({campaignId:'102',list:'customers/123/sharedSets/501'});const count=T.approvals().length;r=await draftBrand();
   check(/already excludes searches for Brites/.test(r.skipped)&&T.approvals().length===count,'nothing is drafted when every campaign already excludes Brites');
   check(/already waiting/.test((await draftBrand({brandWaiting:true})).skipped)&&/deleted in the last 30 days/.test((await draftBrand({brandDeclined:true})).skipped),'a waiting or recently deleted brand exclusion is not proposed again');
   W.brandExclusions=[];W.lists=[];W.camp('201').status='PAUSED';check((await draftBrand()).a.payload.meta.brandSearch.state==='paused','a paused brand Search campaign is named: enable it with the exclusion');
   W.camp('201').status='REMOVED';check((await draftBrand({brandSearchDraft:true})).a.payload.meta.brandSearch.state==='draft'&&(await draftBrand()).a.payload.meta.brandSearch.state==='none','a brand Search draft waiting in Approvals, or none at all, is stated');
   check(W.suggestCalls===1,'a found brand is remembered (30 days)');
   // Google has no Brites brand: nothing is drafted, the miss is logged and remembered for 7 days.
   const N=setup();N.W.brands=[];const miss=await N.e.get('_draftPmaxBrandExclusion')({campaigns,terms:[],state:fresh,currency:'CAD'});
   check(miss.brandMissing===true&&/no Brites brand for britesjewelry\.com yet/.test(miss.skipped)&&[...N.mem.docs.values()].some(d=>d.kind==='pmaxBrandExclusion'&&d.skipped==='brand not found')&&!N.approvals().length,'when Google lists no Brites brand, nothing is drafted and the activity log says why');
   await N.e.get('_draftPmaxBrandExclusion')({campaigns,terms:[],state:fresh,currency:'CAD'});check(N.W.suggestCalls===1,'a miss is not looked up again for 7 days');
   N.mem.docs.get('Brites_GAds_State/pmaxBrand').at=Date.now()-8*DAY;N.W.brands=[{id:'b-777',name:'Brites',urls:['https://britesjewelry.com'],state:'APPROVED'}];
   check(!!(await N.e.get('_draftPmaxBrandExclusion')({campaigns,terms:[],state:fresh,currency:'CAD'})).approvalId&&N.W.suggestCalls===2,'after 7 days Google is asked again');
   // What is waiting or declined, read from Approvals.
   const P=setup(),put=(id,v)=>P.mem.docs.set('Brites_GAds_Approvals/'+id,v),state=()=>P.e.get('_pmaxDraftState')();
   put('bs',{type:'brand',status:'PENDING',payload:{meta:{kind:'brand'}}});check((await state()).brandSearchDraft===true,'the campaign options\' brand Search draft is found by its type');
   P.mem.docs.delete('Brites_GAds_Approvals/bs');put('bt',{type:'search',tag:'brand-search',status:'APPROVED',payload:{}});check((await state()).brandSearchDraft===true,'or by its tag');
   put('bx',{type:'negatives',status:'REJECTED',deletedAt:Date.now()-40*DAY,payload:{meta:{kind:'pmaxBrandExclusion'}}});check((await state()).brandDeclined===false,'a brand exclusion deleted more than 30 days ago can be proposed again');
   put('bx',{type:'negatives',status:'REJECTED',deletedAt:Date.now()-DAY,payload:{meta:{kind:'pmaxBrandExclusion'}}});put('by',{type:'negatives',status:'PENDING',payload:{meta:{kind:'pmaxBrandExclusion'}}});
   const st=await state();check(st.brandDeclined===true&&st.brandWaiting===true,'a recently deleted or waiting brand exclusion is recognised');}

  // ===== 8. Wiring =====
  const bg=fs.readFileSync(path.resolve(__dirname,'../../netlify/functions/googleAdsAutopilot-background.js'),'utf8'),kick=fs.readFileSync(path.resolve(__dirname,'../../netlify/functions/googleAdsAutopilotKick.js'),'utf8');
  check(/task === "mine"\)\s*\{ result\.mine = await E\.mineSearchTerms\(\{ ctrl \}\);[\s\S]{0,300}result\.minePmax = await E\.minePmaxSearchTerms\(\{ ctrl \}\)/.test(bg),'"Mine search terms" also drafts the Performance Max exclusions and brand exclusion');
  const forward=/existingCampaignId:\/\^\\d\{1,20\}\$\/\.test\(String\(body\.existingCampaignId\|\|""\)\)\?String\(body\.existingCampaignId\):null,addBudget:Number\(body\.addBudget\)\|\|0/;
  check(forward.test(bg)&&forward.test(kick),'the destination campaign (digits only) and the added budget reach the draft builder');
  console.log('PASS '+n+' Performance Max structure checks: joining a campaign, exclusions and the brand exclusion');
})().catch(e=>{console.error(e);process.exit(1);});
