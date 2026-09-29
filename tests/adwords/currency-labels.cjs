// Currency labels: budgets, bids, CPC caps and ceilings are in the Google Ads account currency
// (read from the account, CAD here; never hard-coded), converted reports and the monthly stop
// stay USD, and the console never shows a bare "$" for an account-currency amount.
// Offline only: Google Ads, Firestore and OpenAI are local fakes; no paid calls.
const fs=require('fs'),vm=require('vm'),assert=require('assert/strict'),path=require('path');
const root=path.resolve(__dirname,'../..'),dir=root+'/netlify/functions/',realRequire=require('module').createRequire(dir+'googleAdsAutopilot.js');
const html=fs.readFileSync(root+'/brites-adwords.html','utf8');
const clone=x=>x==null?x:JSON.parse(JSON.stringify(x));
let passed=0;const check=(v,msg)=>{assert(v,msg);passed++;};

(async()=>{
 await engineChecks();
 legacyConsoleCheck();
 consoleChecks();
 console.log('PASS '+passed+' currency-label checks (approval summaries, Studio errors, AI prompts, reports, console amounts)');
})().catch(e=>{console.error(e);process.exit(1);});

function memory(){const docs=new Map();
 const doc=p=>({path:p,id:p.split('/').pop(),get:async()=>({exists:docs.has(p),id:p.split('/').pop(),data:()=>clone(docs.get(p))}),set:async(v,o)=>{docs.set(p,o&&o.merge?{...docs.get(p),...clone(v)}:clone(v));}});
 return {docs,db:{collection:c=>({doc:id=>doc(c+'/'+id)})},FV:{serverTimestamp:()=>Date.now()}};}

async function engineChecks(){
 // GADS_CURRENCY is unset, so the engine's store currency falls back to USD while the account is CAD.
 const cx=vm.createContext({module:{exports:{}},exports:{},require:n=>n==='node-fetch'?async()=>{throw Error('Live network forbidden');}:realRequire(n),process:{env:{GADS_CUSTOMER_ID:'123'}},console,Buffer,Date,Intl,Map,Set,URL,setTimeout:fn=>setImmediate(fn),clearTimeout});
 vm.runInContext(fs.readFileSync(dir+'googleAdsAutopilot.js','utf8'),cx);const E=cx.module.exports,get=n=>vm.runInContext(n,cx);
 const bind=v=>{cx.__m=v;vm.runInContext(Object.keys(v).map(k=>k+'=__m.'+k).join('\n'),cx);};
 const ctrl={enabled:true,dryRun:false,maxDailyBudgetTotal:100,maxMonthlySpend:500,budgetCurrency:'CAD',budgetCurrencyVerified:true,defaultCountries:['2124'],smartBidding:false};
 let approvals=[],enabled=40,prompts=[];
 const copy={headlines:['Bunny charm necklace','Handmade bunny gifts','Personalized charms'],descriptions:['Handmade bunny charm necklaces.','Made to order.']};
 bind({control:async()=>clone(ctrl),enqueueApproval:async a=>{approvals.push(clone(a));return 'd'+approvals.length;},_enabledBudgetTotal:async()=>enabled,
  _accountCurrency:async()=>'CAD',_accountTz:async()=>'America/Toronto',_fxRateToUsd:async()=>0.73,fb:()=>null,
  collectionMeta:async h=>({handle:h,title:'Bunny Charms'}),collectionProfiles:async()=>({list:[{handle:'bunny',typesDetail:[{type:'necklace'}]}]}),getCollections:async()=>[{handle:'bunny',title:'Bunny Charms'}],
  generateRSAAssets:async()=>clone(copy),researchOpportunity:async()=>({ok:false}),storeSignals:async()=>{throw Error('no store data offline');},accountCvr:async()=>({cvr:0.02,source:'benchmark'}),
  groundKeywordPlan:()=>{const k=['bunny necklace','bunny charm necklace','easter bunny necklace','personalized bunny necklace'].map(t=>({text:t,real:true,searches:90}));return {ok:true,keywords:k,groups:[{label:'Bunny',keywords:k}],confidence:90,evidence:{accepted:4,rejected:0},rejected:[]};},
  _bestSearchLandingUrl:()=>'https://britesjewelry.com/collections/bunny',accountWasteNegatives:async()=>[],recordOccasionUse:async()=>{},
  buildSearchCampaignOps:()=>({ops:[],tag:'BA · bunny',negatives:['free'],assetSummary:null,keywordSummary:{count:4,researched:true,exact:2},adGroupSummary:[]}),
  openaiJSON:async p=>{prompts.push(p);throw Error('AI is offline in tests');},playbookSlice:async()=>{throw Error('offline');}});

 // 1. Search draft: the Manual CPC cap in the approval summary is the account currency.
 let r=await get('generateForCollection')('bunny','Evergreen gifting',12,{ctrl:clone(ctrl),maxCpc:1.25,countries:['2124']});
 check(r.ok&&r.currency==='CAD','Search draft built for the CAD account');
 check(/Manual CPC ≤ CAD 1\.25\/click/.test(approvals[0].summary)&&!/USD|\$/.test(approvals[0].summary),'Search approval summary: CPC cap in CAD, never USD or a bare $ ('+approvals[0].summary+')');

 // 2. PMax draft: the daily budget in the approval summary is the account currency.
 const f=memory();f.docs.set('Brites_GAds_State/opportunities',{pmaxList:[]});approvals=[];
 bind({fb:()=>f,_assertOpportunityNotDeleted:async()=>{},_pmaxResearchCandidate:()=>null,merchantCenterId:async()=>'555',_pmaxIsEligible:()=>true,
  merchantProducts:async()=>[{itemId:'shopify_CA_1_2',title:'Bunny necklace',feedLabel:'CA'}],discoverPmaxAudienceResource:async()=>({resource:null}),listCountries:async()=>[],_pmaxAdCopy:async()=>null,
  buildPmaxCampaignOps:()=>({ops:[],scopedItemIds:['shopify_CA_1_2'],scopedTypes:[],assetMode:'feed',textAssets:{headlines:5,longHeadlines:1,descriptions:4},countries:['2124'],tag:'BA · PMax bunny',assetGroups:[],searchThemes:[],audienceSignal:null})});
 await E.generatePmaxApproval({handle:'bunny',dailyBudget:12,itemIds:['shopify_CA_1_2'],feedLabel:'CA'});
 check(/PMax · Bunny Charms · CAD 12\/day · 1 proven/.test(approvals[0].summary)&&!/\$/.test(approvals[0].summary),'PMax approval summary: budget in CAD ('+approvals[0].summary+')');

 // 3. Design Studio: refusals and both approval summaries use the account currency.
 approvals=[];bind({fb:()=>null,takenTags:async()=>({}),buildDesignStudioSearchCampaignOps:()=>({ops:[],keywordSummary:{count:6},adGroupSummary:[]}),
  scanDesignStudioOpportunity:async()=>({blueprint:{measurement:{readiness:{apiOk:true,purchaseReady:true}},budget:{recommendedDaily:10,countries:['2124']},pmax:{groups:[{name:'Gifts',angle:'a',searchThemes:[],headlines:[],longHeadlines:[],descriptions:[]}]},search:{maxCpc:1.1},images:{}}})});
 const studio=E.generateDesignStudioApprovals;
 enabled=98;await assert.rejects(()=>studio({}),/only CAD 2\.00 of headroom\. Free at least CAD 4\.00 /);check(true,'Studio headroom refusal in CAD');
 enabled=90;await assert.rejects(()=>studio({pmaxDaily:0.5,searchDaily:5}),/at least CAD 1\.00\./);check(true,'Studio per-lane minimum in CAD');
 await assert.rejects(()=>studio({pmaxDaily:2,searchDaily:1.5}),/at least CAD 4\.00\/day/);check(true,'Studio combined minimum in CAD');
 await assert.rejects(()=>studio({pmaxDaily:6,searchDaily:6}),/total CAD 12\.00\/day, but only CAD 10\.00\/day remains/);check(true,'Studio over-headroom refusal in CAD');
 await studio({pmaxDaily:4.8,searchDaily:3.2});const sums=approvals.map(a=>a.summary).join(' | ');
 check(/PMax discovery · CAD 4\.80\/day · 3 intent/.test(sums)&&/high-intent Search · CAD 3\.20\/day · 6 exact/.test(sums)&&!/\$/.test(sums),'Studio approval summaries: budgets in CAD ('+sums+')');

 // 4. Converted reporting still says USD: Studio learning evidence quotes USD-converted spend.
 bind({designStudioPerformance:async()=>({ok:true,currency:'USD',start:'2026-09-01',end:'2026-09-29',days:30,campaigns:[{lane:'pmax'}],overall:{clicks:60,impressions:3000,ctr:0.03,cost:80.5},
  purchase:{conversions:1,value:0,cpa:null,roas:null},funnel:{start:0,design:0,approve:0,cart:0,purchase:1},rates:{},readiness:{apiOk:true,purchaseReady:true,assistCoverage:4},assetGroups:[],searchInsights:[]})});
 const learn=await E.refreshDesignStudioLearning({days:30}),ev=learn.recommendations.map(x=>x.evidence||'').join(' | ');
 check(/60 clicks · USD 80\.50 spend · 1\.0 purchases/.test(ev)&&!/\$/.test(ev),'Studio learning evidence: converted spend labelled USD ('+ev+')');

 // 5. Ad Doctor prompt: the monthly stop threshold is USD; budgets and ceiling are the account currency.
 await get('_analyzeCampaignSet')({campaigns:[{id:'1',name:'A',status:'ENABLED',budget:20,recommendations:[]}]},clone(ctrl),[],{enabledTotal:20}).catch(()=>null);
 const doc=prompts.find(p=>/ACCOUNT GUARDRAILS/.test(p))||'';
 check(/monthly hard cap USD 500\b/.test(doc)&&/budget ceiling 100\b/.test(doc)&&/is in CAD/.test(doc),'Ad Doctor guardrails: monthly stop USD, ceiling and budgets CAD');

 // 6. PMax selector prompt quotes its budget range and ceiling in the account currency.
 const pmaxSrc=get('proposePmaxOpportunities').toString(),scanSrc=get('scanOpportunities').toString();
 check(!/\$6-\$15|ceiling \$\$\{/.test(pmaxSrc)&&/\$\{bc\} 6-15\/day, respecting total ceiling \$\{bc\} \$\{ceiling\}\/day/.test(pmaxSrc)&&/proposePmaxOpportunities\(\{[^)]*currency:ctrl\.budgetCurrency/.test(scanSrc),'PMax selector prompt names the account currency for budgets');
 check(/Daily ceiling \$\{ctrl\.budgetCurrency\|\|"UNVERIFIED"\}/.test(scanSrc),'scan audit never labels an unverified ceiling USD');
}

function legacyConsoleCheck(){
 // The legacy console served by googleAdsAutopilotApi shows the ceiling in the account currency.
 const src=fs.readFileSync(dir+'googleAdsAutopilotKick.js','utf8'),m=src.match(/const CONSOLE_HTML = (`[\s\S]*?`);/);assert(m,'legacy console HTML found');
 const page=vm.runInNewContext(m[1]),script=page.slice(page.indexOf('<script>')+8,page.lastIndexOf('</script>')),els={};
 const cx=vm.createContext({document:{getElementById:id=>els[id]||(els[id]={innerHTML:'',textContent:'',style:{},appendChild(){}}),createElement:()=>({})},fetch:()=>{throw Error('offline');},JSON});
 vm.runInContext(script,cx);cx.render({control:{enabled:true,dryRun:false,maxDailyBudgetTotal:100,budgetCurrency:'CAD',maxBudgetStepPct:20,budgetMoveApprovalPct:30},pending:[]});
 check(/ceiling CAD 100\/day/.test(els.ctrlbar.innerHTML)&&!/\$/.test(els.ctrlbar.innerHTML),'legacy console ceiling in CAD');
}

function consoleChecks(){
 const pick=name=>{const m=new RegExp('^function '+name+'\\(','m').exec(html);assert(m,name+' exists');const rest=html.slice(m.index),next=/\n(?:async )?function \w+\(/.exec(rest.slice(1));return next?rest.slice(0,next.index+1):rest;};
 const span=(a,b)=>{const i=html.indexOf(a),j=html.indexOf(b,i+1);assert(i>=0&&j>i,a);return html.slice(i,j);};
 const helpers=['money','ccyMark','moneyIn','acctMoney','acctMark'].map(pick).join('\n');
 const cx=vm.createContext({DASH:{budgetCurrency:'CAD'},console,Intl});vm.runInContext(helpers,cx);
 check(cx.acctMoney(12)==='CA$12'&&cx.acctMoney(-3.5)==='−CA$3.50'&&cx.acctMark()==='CA$','account-currency amounts read CA$, negatives keep their sign');
 check(cx.moneyIn(1234.5,'USD')==='US$1,234.50'&&cx.moneyIn(5,'EUR')==='EUR 5'&&cx.moneyIn(5,null)==='$5'&&cx.moneyIn(5,'currency unverified')==='$5','converted amounts read US$; another code is spelled out; an unknown one stays bare');
 cx.DASH={budgetCurrency:'USD'};check(cx.acctMoney(8)==='US$8','a USD account reads US$ (never hard-coded CAD)');cx.DASH={budgetCurrency:'CAD'};

 // PMax forecasts: CA$/US$ even when the browser's own locale (en-CA) would print CAD as "$".
 const IntlCA=Object.assign(Object.create(Intl),{NumberFormat:function(loc,o){return new Intl.NumberFormat(loc===undefined?'en-CA':loc,o);}});
 const px=vm.createContext({Intl:IntlCA});vm.runInContext(pick('pmaxNumber'),px);
 check(px.pmaxNumber(12,'money','CAD')==='CA$12.00'&&px.pmaxNumber(12,'money','USD')==='US$12.00','PMax money reads CA$/US$ in any browser locale');

 // Design Studio launch settings: every budget reads CA$; converted results read US$.
 const els={studioGrowth:{innerHTML:''}},sx=vm.createContext({DASH:{budgetCurrency:'CAD',control:{maxDailyBudgetTotal:100}},Intl,console,esc:s=>String(s==null?'':s),$:s=>els[s.slice(1)]||null,defCountries:()=>['2124'],ctyName:String,timeago:()=>'1h'});
 vm.runInContext(helpers+'\n'+span('var STUDIO_GROWTH=','async function loadDesignStudioGrowth('),sx);
 sx.STUDIO_GROWTH={blueprint:{budget:{recommendedDaily:10,ceiling:100,headroom:20,pmaxDaily:4,searchDaily:6,countries:['2124']},measurement:{readiness:{apiOk:true,purchaseReady:true}},pmax:{groups:[]},search:{},positioning:{},page:{}},lanes:{},
  performance:{ok:true,currency:'USD',start:'2026-09-01',end:'2026-09-29',overall:{cost:80.5,clicks:60},purchase:{cpa:40.25},funnel:{},rates:{}}};
 sx.renderDesignStudioGrowth();const sg=els.studioGrowth.innerHTML;
 check(/<b>Search<\/b> CA\$ <input/.test(sg)&&/<b>PMax<\/b> CA\$ <input/.test(sg)&&/CA\$10\.00\/day/.test(sg)&&/CA\$20\.00 available/.test(sg),'Studio launch budgets and available headroom read CA$');
 check(/US\$80\.50/.test(sg)&&/US\$40\.25/.test(sg)&&!/[^AS]\$\d|> \$ </.test(sg),'Studio spend and purchase CPA read US$, no bare $ left');

 // Opportunity cards: budget, CPC cap, spend and profit re-derive in the account currency.
 const texts={},ox=vm.createContext({DASH:{budgetCurrency:'CAD'},Intl,console,oppCpcOf:()=>1.25,oppBidOf:()=>false});
 vm.runInContext(helpers+'\n'+span('function fmtDate(','function bidView(')+'\nvar OPPS=[];',ox);
 ox.OPPS=[{recommendedDailyBudget:12,startDate:'2026-10-01',endDate:'2026-10-29',durationDays:28,plan:{cpc:{low:0.5,max:1.25},model:{eCpcMarket:0.8,eCpc:0.8,cpcLow:0.5,cpcHigh:1.25,cvr:0.02,aov:60,marginRate:0.65,uncertainty:0.4}}}];
 ox.oppRecalc({querySelector:()=>null,querySelectorAll:sel=>[{set textContent(v){texts[sel.split('[')[0]]=v;}}]},0);
 check(texts['.opPlanned']==='CA$12'&&texts['.planCpc']==='CA$1.25'&&texts['.fBud']==='CA$12/d'&&/^CA\$/.test(texts['.opTot'])&&/within a CA\$1\.25 max CPC/.test(texts['.planGoal']),'opportunity budget, cap, total and goal read CA$ ('+JSON.stringify(texts)+')');
}
