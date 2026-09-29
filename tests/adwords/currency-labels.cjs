// Currency labels: budgets, bids, CPC caps and ceilings are in the Google Ads account currency
// (read from the account, CAD here; never hard-coded), converted reports and the monthly stop
// stay USD, and console amounts name their currency (CA$ / US$ or the currency code).
// Offline only: Google Ads, Firestore and OpenAI are local fakes; no paid calls.
const fs=require('fs'),vm=require('vm'),assert=require('assert/strict'),path=require('path');
const root=path.resolve(__dirname,'../..'),dir=root+'/netlify/functions/',realRequire=require('module').createRequire(dir+'googleAdsAutopilot.js');
const html=fs.readFileSync(root+'/brites-adwords.html','utf8');
const clone=x=>x==null?x:JSON.parse(JSON.stringify(x));
let passed=0;const check=(v,msg)=>{assert(v,msg);passed++;};

(async()=>{
 await engineChecks();
 legacyConsoleCheck();
 modelChecks();
 await consoleChecks();
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
 const studioPerformance=get('designStudioPerformance'),planCampaign=get('planCampaign');
 const ctrl={enabled:true,dryRun:false,maxDailyBudgetTotal:100,maxMonthlySpend:500,budgetCurrency:'CAD',budgetCurrencyVerified:true,defaultCountries:['2124'],smartBidding:false};
 let approvals=[],enabled=40,prompts=[];
 const copy={headlines:['Bunny charm necklace','Handmade bunny gifts','Personalized charms'],descriptions:['Handmade bunny charm necklaces.','Made to order.']};
 bind({control:async()=>clone(ctrl),enqueueApproval:async a=>{approvals.push(clone(a));return 'd'+approvals.length;},_enabledBudgetTotal:async()=>enabled,
  _accountCurrency:async()=>'CAD',_accountTz:async()=>'America/Toronto',_fxRateToUsd:async()=>0.73,fb:()=>null,
  collectionMeta:async h=>({handle:h,title:'Bunny Charms'}),collectionProfiles:async()=>({list:[{handle:'bunny',typesDetail:[{type:'necklace'}]}]}),getCollections:async()=>[{handle:'bunny',title:'Bunny Charms'}],
  generateRSAAssets:async()=>clone(copy),researchOpportunity:async()=>({ok:false}),storeSignals:async()=>{throw Error('no store data offline');},accountCvr:async()=>({cvr:0.02,source:'benchmark'}),
  groundKeywordPlan:()=>{const k=['bunny necklace','bunny charm necklace','easter bunny necklace','personalized bunny necklace'].map(t=>({text:t,real:true,measured:true,searches:90}));return {ok:true,keywords:k,groups:[{label:'Bunny',keywords:k}],confidence:90,evidence:{accepted:4,rejected:0},rejected:[]};},
  _bestSearchLandingUrl:()=>'https://britesjewelry.com/collections/bunny',accountWasteNegatives:async()=>[],recordOccasionUse:async()=>{},
  buildSearchCampaignOps:()=>({ops:[{campaignOperation:{create:{startDateTime:'2026-10-01 00:00:00',endDateTime:'2026-10-28 23:59:59'}}}],tag:'BA · bunny',negatives:['free'],assetSummary:null,keywordSummary:{count:4,researched:true,exact:2,measured:4},adGroupSummary:[]}),
  openaiJSON:async p=>{prompts.push(p);throw Error('AI is offline in tests');},playbookSlice:async()=>{throw Error('offline');}});

 // 1. Search draft: the Manual CPC cap in the approval summary is the account currency.
 let r=await get('generateForCollection')('bunny','Evergreen gifting',12,{ctrl:clone(ctrl),maxCpc:1.25,countries:['2124']});
 check(r.ok&&r.currency==='CAD','Search draft built for the CAD account');
 check(/Manual CPC ≤ CAD 1\.25\/click/.test(approvals[0].summary)&&!/USD|\$/.test(approvals[0].summary),'Search approval summary: CPC cap in CAD, never USD or a bare $ ('+approvals[0].summary+')');

 // An unverified account currency is never claimed to be USD: the cap is Google's native amount.
 approvals=[];r=await get('generateForCollection')('bunny','Evergreen gifting',12,{ctrl:{...clone(ctrl),budgetCurrency:null,budgetCurrencyVerified:false},maxCpc:1.25,countries:['2124']});
 check(r.ok&&/Manual CPC ≤ 1\.25\/click/.test(approvals[0].summary)&&!/USD|\$/.test(approvals[0].summary),'Search approval summary with an unverified currency names none ('+approvals[0].summary+')');

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

 // 7. planCampaign: an unverified account currency is named as such, never assumed USD; every number is unchanged.
 const base={title:'Bunny Charms',occasion:'Evergreen gifting',ceiling:100,headroom:60,smartBidding:false,research:null,aov:60,cvrInfo:{cvr:0.02,source:'benchmark'}};
 const pu=planCampaign({...base,currency:null,nativeToUsd:null}),pd=planCampaign({...base,currency:'USD',nativeToUsd:null});
 const words=x=>JSON.stringify([x.goal,x.budget.basis,x.cpc.basis,x.duration.basis,x.caveats]),numbers=x=>JSON.stringify(x,(k,v)=>typeof v==='string'?undefined:v);
 check(pu.currency==='UNVERIFIED'&&!/\bUSD\b/.test(words(pu))&&/UNVERIFIED \d/.test(words(pu))&&numbers(pu)===numbers(pd),'plan with an unverified currency names it and keeps the same numbers ('+pu.goal+')');

 // 8. Studio results come from metricsRange's campaign rows ({ snapshot, currency }), so both Studio campaigns count.
 bind({designStudioConversionReadiness:async()=>({apiOk:true,purchaseReady:true,start:[],design:[],approve:[],cart:[],purchase:[]}),fb:()=>null,gaql:async()=>[],
  metricsRange:async()=>({ok:true,currency:'USD',snapshot:[{id:'11',name:'BA · design-studio-pmax',cost:30,clicks:20,impr:900,conv:1,value:60},{id:'22',name:'BA · design-studio-search',cost:20,clicks:15,impr:400,conv:0,value:0},{id:'33',name:'BA · bunny',cost:99,clicks:50,impr:5000,conv:2,value:120}]})});
 const perf=await studioPerformance({days:30});
 check(perf.campaigns.map(c=>c.lane).join()==='pmax,search'&&perf.overall.cost===50&&perf.overall.clicks===35&&perf.currency==='USD','Studio results include both Studio campaigns and nothing else ('+JSON.stringify(perf.overall)+')');

 // 9. The store's order value converts only from the currency its orders are in (storeSignals: GADS_CURRENCY). CAD orders on a
 //    CAD account keep their value (not ~37% more), USD orders convert, and a currency with no rate gives no revenue forecast.
 const cadStore=vm.createContext({module:{exports:{}},exports:{},require:n=>n==='node-fetch'?async()=>{throw Error('Live network forbidden');}:realRequire(n),process:{env:{GADS_CUSTOMER_ID:'123',GADS_CURRENCY:'CAD'}},console,Buffer,Date,Intl,Map,Set,URL,setTimeout:fn=>setImmediate(fn),clearTimeout});
 vm.runInContext(fs.readFileSync(dir+'googleAdsAutopilot.js','utf8'),cadStore);const planCad=vm.runInContext('planCampaign',cadStore);
 const acct={...base,currency:'CAD',nativeToUsd:0.73,economics:{marginRate:0.5}},same=planCad(acct),fromUsd=planCad({...acct,economics:{marginRate:0.5,currency:'USD'}});
 const usdStore=planCampaign(acct),noRate=planCad({...acct,economics:{marginRate:0.5,currency:'EUR'}});
 check(same.expected.aov===60&&same.model.aov===60&&same.expected.breakEvenCpa===30&&Math.abs(same.expected.revenue-same.expected.conversions*60)<0.01&&/× CAD 60\.00 average order/.test(same.caveats.join(' ')),'CAD orders on a CAD account keep their order value ('+same.expected.aov+')');
 check(Math.abs(fromUsd.expected.aov-60/0.73)<1e-9&&Math.abs(usdStore.expected.aov-60/0.73)<1e-9,'USD orders convert to the CAD account ('+fromUsd.expected.aov+', '+usdStore.expected.aov+')');
 check(noRate.expected.aov===null&&noRate.expected.revenue===null&&noRate.expected.breakEvenCpa===null&&noRate.expectedRoas===null,'orders in a currency with no rate give no revenue forecast, not a wrong one ('+noRate.expected.aov+')');

 // 10. A Studio PMax draft saved without dates starts and ends on the account's dates: at 23:30 in Toronto UTC is already tomorrow.
 const realNow=Date.now;Date.now=()=>Date.parse('2026-09-30T03:30:00Z');
 try{
  bind({_creativeImageOps:async()=>({ops:[],groups:{'customers/123/assetGroups/-3':{square:['sq'],landscape:['ls'],portrait:[],logo:'lg'}}}),_buildPmaxTextAssetOps:()=>({ops:[],ids:{headlines:[],longHeadlines:[],descriptions:[],businessName:'bn'}})});
  const b=await get('buildDesignStudioPmaxCampaignOps')({dailyBudget:5,groups:[{name:'Gifts',angle:'a',searchThemes:[],headlines:[],longHeadlines:[],descriptions:[]}],reviewedCreative:{}},{ctrl:clone(ctrl)}),c=b.ops.find(o=>o.campaignOperation).campaignOperation.create;
  check(!c.startDateTime&&c.endDateTime==='20261228 23:59:59','Studio draft starts today and ends 90 days later on the account calendar ('+[c.startDateTime,c.endDateTime]+')');
 }finally{Date.now=realNow;}
}

function modelChecks(){
 // A saved PMax suggestion that never recorded its budget currency is not forecast as if it were USD.
 const M=require(root+'/assets/pmax-recommendation.js'),paid={available:true,monetaryComplete:true,currency:'USD',days:90,impressions:10000,clicks:200,conversions:10,cost:100,value:500};
 const cand={recommendationSchema:1,itemIds:['shopify_US_11_101'],offerDetails:[{itemId:'shopify_US_11_101',title:'Corgi necklace',paidPerformance:paid,evidenceIds:[]}],demandEvidence:[],paidPerformance:paid,dailyBudget:12,days:30};
 const none=M.buildRecommendation(cand,{dailyBudget:12,days:30}).forecast,usd=M.buildRecommendation({...cand,budgetCurrency:'USD',budgetCurrencyVerified:true},{dailyBudget:12,days:30}).forecast;
 check(none.currency==='Unverified'&&!none.scenarios.length&&none.missing.some(x=>/could not be verified/.test(x))&&!none.missing.some(x=>/Unverified to USD/.test(x))&&usd.currency==='USD'&&usd.scenarios.length===3,'a PMax suggestion with no recorded budget currency is not forecast as USD');
}

function legacyConsoleCheck(){
 // The legacy console served by googleAdsAutopilotApi shows the ceiling in the account currency.
 const src=fs.readFileSync(dir+'googleAdsAutopilotKick.js','utf8'),m=src.match(/const CONSOLE_HTML = (`[\s\S]*?`);/);assert(m,'legacy console HTML found');
 const page=vm.runInNewContext(m[1]),script=page.slice(page.indexOf('<script>')+8,page.lastIndexOf('</script>')),els={};
 const cx=vm.createContext({document:{getElementById:id=>els[id]||(els[id]={innerHTML:'',textContent:'',style:{},appendChild(){}}),createElement:()=>({})},fetch:()=>{throw Error('offline');},JSON});
 vm.runInContext(script,cx);cx.render({control:{enabled:true,dryRun:false,maxDailyBudgetTotal:100,budgetCurrency:'CAD',maxBudgetStepPct:20,budgetMoveApprovalPct:30},pending:[]});
 check(/ceiling CAD 100\/day/.test(els.ctrlbar.innerHTML)&&!/\$/.test(els.ctrlbar.innerHTML),'legacy console ceiling in CAD');
}

async function consoleChecks(){
 const pick=name=>{const m=new RegExp('^(?:async )?function '+name+'\\(','m').exec(html);assert(m,name+' exists');const rest=html.slice(m.index),next=/\n(?:async )?function \w+\(/.exec(rest.slice(1));return next?rest.slice(0,next.index+1):rest;};
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

 // Design Studio: launch budgets name the account currency; converted results read US$.
 const els={studioGrowth:{innerHTML:''}},sx=vm.createContext({DASH:{budgetCurrency:'CAD',control:{maxDailyBudgetTotal:100}},Intl,console,esc:s=>String(s==null?'':s),$:s=>els[s.slice(1)]||null,defCountries:()=>['2124'],ctyName:String,timeago:()=>'1h'});
 vm.runInContext(helpers+'\n'+pick('cur')+'\n'+span('var STUDIO_GROWTH=','async function loadDesignStudioGrowth('),sx);
 sx.STUDIO_GROWTH={blueprint:{budget:{recommendedDaily:10,ceiling:100,headroom:20,pmaxDaily:4,searchDaily:6,countries:['2124']},measurement:{readiness:{apiOk:true,purchaseReady:true}},pmax:{groups:[]},search:{},positioning:{},page:{}},lanes:{},
  performance:{ok:true,currency:'USD',start:'2026-09-01',end:'2026-09-29',overall:{cost:80.5,clicks:60},purchase:{cpa:40.25},funnel:{},rates:{}}};
 sx.renderDesignStudioGrowth();const sg=els.studioGrowth.innerHTML;
 check(/Launch settings · budgets in CAD/.test(sg)&&/US\$80\.50<\/div><div class="l">Spend/.test(sg)&&/US\$40\.25<\/div><div class="l">Purchase CPA/.test(sg),'Studio launch budgets name CAD; converted spend and purchase CPA read US$');

 // Performance dialog chart: its axis names the report currency; a negative value keeps its sign in front.
 const fm=html.match(/chartOpts=\{[^;]*?fmt:(function\(v\)\{.*?\}),labels:/);assert(fm,'performance chart formatter found');
 vm.runInContext('var d={currency:"USD"};var perfFmt='+fm[1],cx);
 check(cx.perfFmt(250)==='US$250'&&cx.perfFmt(-50)==='\u2212US$50','performance chart axis reads US$');

 // Campaign tree: editing a budget confirms, relabels the button and reports in the budget currency.
 const said=[],bge={attrs:{'data-id':'7','data-b':'10','data-res':''},getAttribute(k){return this.attrs[k];},setAttribute(k,v){this.attrs[k]=v;},innerHTML:'$10 CAD'};
 const tx=vm.createContext({DASH:{budgetCurrency:'CAD',lastMetrics:[]},cmdReport:{budgetCurrency:'CAD'},BUDGET_OVERRIDES:{},Intl,console,esc:s=>String(s==null?'':s),
  prompt:()=>'15',confirm:m=>{said.push(m);return true;},toast:m=>said.push(m),api:async()=>({ok:true})});
 vm.runInContext(helpers+'\n'+pick('reportNumber')+'\n'+pick('wireCampRows'),tx);
 tx.wireCampRows({querySelectorAll:sel=>sel==='.bge'?[bge]:[]});await bge.onclick.call(bge,{stopPropagation(){}});
 check(/\(\$10 CAD \u2192 \$15 CAD\)/.test(said[0])&&/^\$15 CAD /.test(bge.innerHTML)&&/^Budget \$10 CAD \u2192 \$15 CAD/.test(said[1]),'campaign tree budget edit names CAD ('+said.join(' | ')+')');

 // Ad Doctor "Set" budget and enable confirmations name the account currency.
 const sm=[],bx=vm.createContext({DASH:{budgetCurrency:'CAD',control:{dryRun:false},lastMetrics:[]},Intl,console,confirm:m=>{sm.push(m);return true;},toast:m=>sm.push(m),
  btnBusy(){},actStart:()=>1,actEnd(){},api:async()=>({ok:true}),reload:async()=>{},renderAll(){},P:{expanded:{}}});
 vm.runInContext(helpers+'\n'+pick('setBudget')+'\n'+pick('campStatus'),bx);
 await bx.setBudget('7',15,'',{});await bx.campStatus('7','A',12,'ENABLED',{});
 check(/Set daily budget to CA\$15\?/.test(sm[0])&&/^Budget set to CA\$15$/.test(sm[1])&&/spending up to CA\$12\/day/.test(sm[2]),'Ad Doctor budget confirmations read CA$ ('+sm.join(' | ')+')');

 // PMax: a suggestion whose saved currency was never verified does not show that currency on its budget.
 const model=require(root+'/assets/pmax-recommendation.js'),out={},doc={getElementById:()=>null,createElement:()=>{const el={id:'',innerHTML:'',querySelectorAll:()=>[],querySelector:()=>null,addEventListener(){}};return el;}};
 const qx=vm.createContext({window:{BritesPmaxRecommendation:model},document:doc,Intl,Date,console,esc:s=>String(s==null?'':s),apvEnc:encodeURIComponent,friendlyResearchError:String,researchNeedsRefresh:()=>false,wireOpportunityDeletion(){},PMAXAT:1,PMAXERR:null});
 vm.runInContext(html.slice(html.indexOf('function pmaxProductChoices('),html.indexOf('function renderOpportunities(){')),qx);
 const host={parentNode:{insertBefore(el){out.html=el.innerHTML;}}},pmax=v=>({collectionTitle:'Pets',handle:'pets',itemIds:['shopify_US_11_101'],productTitles:['Corgi'],dailyBudget:12,days:30,...v});
 qx.PMAXOPPS=[pmax({budgetCurrency:'USD',budgetCurrencyVerified:false})];qx.renderPmaxSection(host);const unver=out.html;
 qx.PMAXOPPS=[pmax({budgetCurrency:'CAD',budgetCurrencyVerified:true})];qx.renderPmaxSection(host);const ver=out.html;
 check(/class="pmxBudget">\$<input/.test(unver)&&!/daily budget in USD/.test(unver)&&/class="pmxBudget">CAD \$<input/.test(ver),'PMax budget shows a verified currency only');

 // AI costs are billed in US dollars, and the edited scripts are re-fetched (new ?v= tags).
 const editor=fs.readFileSync(root+'/brites-ad-editor.js','utf8'),motion=fs.readFileSync(root+'/brites-ad-motion.js','utf8');
 check(/'AI cost'\)\+' · US\$'\+Number\(cost\.estimatedUsd\)/.test(editor)&&/about US\$2\.03/.test(motion)&&/Estimated cost US\$'\+Number/.test(motion)&&/video cost: US\$2\.03/.test(motion)&&!/[^S]\$(?:\d|'\+)/.test(editor+motion),'editor and motion AI costs read US$');
 check(!/brites-ad-editor\.js\?v=20260929-studio(?:-proofs)?'|brites-ad-motion\.js\?v=20260929-studio(?:-films)?"|pmax-recommendation\.js\?v=20260929-concise"/.test(html),'edited scripts carry new version tags');
}
