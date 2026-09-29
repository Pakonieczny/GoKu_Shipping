// Campaign options: a small brand Search campaign, sale-day bid adjustments (bidding seasonality),
// the new-customer acquisition goal and campaign total budgets, plus purchase-only conversion goals
// on every new Search and Performance Max campaign. Each option is a draft in Approvals and reaches
// Google only after approval, and every ceiling check counts a total budget per remaining day.
// Offline: Google Ads, Firestore and the lifecycle-goal endpoint are local fakes; no paid AI.
'use strict';
const fs=require('fs'),vm=require('vm'),assert=require('assert/strict'),path=require('path'),{JSDOM}=require('jsdom');
const ROOT=path.resolve(__dirname,'../..'),dir=path.join(ROOT,'netlify/functions')+'/',realRequire=require('module').createRequire(dir+'googleAdsAutopilot.js');
const O=require(dir+'_googleAdsCampaignOptions.js'),{addDays,daysInclusive}=O._dates;
const clone=x=>x==null?x:JSON.parse(JSON.stringify(x));
let passed=0;const check=(v,msg)=>{assert(v,msg);passed++;};
const eq=(a,b,msg)=>{assert.deepEqual(clone(a),clone(b),msg);passed++;};
const throwsRe=(fn,re,msg)=>{assert.throws(fn,re,msg);passed++;};
const rejects=async(p,re,msg)=>{await assert.rejects(p,re,msg);passed++;};
const B=n=>'customers/123/campaignBudgets/'+n,Cp=n=>'customers/123/campaigns/'+n;
const compact=d=>d.replace(/-/g,'');

(async()=>{
 moduleChecks();
 await engineChecks();
 await routerChecks();
 await uiChecks();
 console.log('PASS '+passed+' campaign-options checks (brand Search, sale-day adjustments, new-customer goal, total budgets, purchase-only goals, brand exclusions on new Performance Max)');
 require('./suite-guard.cjs').done();
})().catch(e=>{console.error(e);process.exit(1);});

/* ---------------- the rules (pure) ---------------- */
function moduleChecks(){
 // Brand Search: the brand terms, exact and phrase, under a manual CPC cap, served like the Search builder's campaigns.
 const ext=cRes=>({ops:[{assetOperation:{create:{resourceName:'customers/123/assets/-10',finalUrls:['https://britesjewelry.com/collections/all'],sitelinkAsset:{linkText:'Shop All Jewelry',description1:'Browse',description2:'Made to order'}}}},{campaignAssetOperation:{create:{asset:'customers/123/assets/-10',campaign:cRes,fieldType:'SITELINK'}}}],summary:{sitelinks:1,callouts:0,structuredSnippets:0}});
 const b=O.buildBrandSearchOps({cid:'123',dailyBudget:'5',maxCpc:0.5,countries:['2124','geoTargetConstants/2840','2124'],extensionOps:ext,stamp:1});
 const budget=b.ops[0].campaignBudgetOperation.create,camp=b.ops[1].campaignOperation.create,ag=b.ops[2].adGroupOperation.create,ad=b.ops[3].adGroupAdOperation.create.ad;
 check(budget.amountMicros===5e6&&budget.explicitlyShared===false&&budget.deliveryMethod==='STANDARD'&&camp.campaignBudget===budget.resourceName,'brand campaign: its own small daily budget, never shared');
 check(camp.name==='BA · brand-search'&&camp.status==='PAUSED'&&camp.advertisingChannelType==='SEARCH'&&camp.manualCpc.enhancedCpcEnabled===false,'starts paused, on manual CPC');
 check(camp.geoTargetTypeSetting.positiveGeoTargetType==='PRESENCE'&&camp.finalUrlSuffix===O.SEARCH_URL_SUFFIX&&camp.containsEuPoliticalAdvertising==='DOES_NOT_CONTAIN_EU_POLITICAL_ADVERTISING','presence targeting, the tracking suffix and the EU declaration');
 eq(camp.networkSettings,{targetGoogleSearch:true,targetSearchNetwork:false,targetPartnerSearchNetwork:false,targetContentNetwork:false},'Google Search only');
 check(ag.cpcBidMicros===5e5&&ag.type==='SEARCH_STANDARD'&&ag.campaign===Cp(-2),'the ad group max CPC is the bid cap');
 eq(b.ops.filter(o=>o.adGroupCriterionOperation).map(o=>o.adGroupCriterionOperation.create.keyword.matchType+':'+o.adGroupCriterionOperation.create.keyword.text).sort(),['EXACT:brites','EXACT:brites jewelry','PHRASE:brites','PHRASE:brites jewelry'],'"Brites" and "Brites Jewelry", exact and phrase');
 const crit=b.ops.filter(o=>o.campaignCriterionOperation).map(o=>o.campaignCriterionOperation.create);
 eq(crit.filter(c=>c.location).map(c=>c.location.geoTargetConstant),['geoTargetConstants/2124','geoTargetConstants/2840'],'the target countries, each once');
 check(crit.some(c=>c.language&&c.language.languageConstant==='languageConstants/1000'),'English, like the Search builder');
 const negs=crit.filter(c=>c.negative).map(c=>c.keyword.text);
 check(negs.length===O.BRAND_SEARCH.negatives.length&&negs.every(n=>!n.split(' ').some(w=>w==='brites'||w==='jewelry'))&&!negs.includes('free'),'negatives never block a brand term; "free shipping" searches stay');
 const hl=ad.responsiveSearchAd.headlines.map(h=>h.text),ds=ad.responsiveSearchAd.descriptions.map(d=>d.text);
 check(hl.length>=3&&hl.length<=15&&new Set(hl).size===hl.length&&hl.every(t=>t.length<=30)&&ds.length>=2&&ds.length<=4&&ds.every(t=>t.length<=90),'fixed brand copy within Google limits');
 check(ad.finalUrls[0]==='https://britesjewelry.com/'&&b.ops[b.ops.length-1].campaignAssetOperation.create.campaign===Cp(-2),'lands on the store; the Search builder\'s sitelinks attach to the new campaign');
 throwsRe(()=>O.buildBrandSearchOps({cid:'123',dailyBudget:0.5,maxCpc:0.2,countries:['2124']}),/Daily budget must be between 1 and 50/,'budget floor');
 throwsRe(()=>O.buildBrandSearchOps({cid:'123',dailyBudget:51,maxCpc:0.2,countries:['2124']}),/Daily budget must be between 1 and 50/,'budget cap');
 throwsRe(()=>O.buildBrandSearchOps({cid:'123',dailyBudget:'',maxCpc:0.2,countries:['2124']}),/Daily budget/,'an empty budget is not zero');
 throwsRe(()=>O.buildBrandSearchOps({cid:'123',dailyBudget:5,maxCpc:6,countries:['2124']}),/Max cost per click must be between 0.05 and 5/,'CPC cap');
 throwsRe(()=>O.buildBrandSearchOps({cid:'123',dailyBudget:2,maxCpc:3,countries:['2124']}),/can't be more than the daily budget/,'CPC above the budget');
 throwsRe(()=>O.buildBrandSearchOps({cid:'123',dailyBudget:5,maxCpc:0.5,countries:[]}),/target countries/,'no countries');
 throwsRe(()=>O.buildBrandSearchOps({cid:'',dailyBudget:5,maxCpc:0.5,countries:['2124']}),/not configured/,'no account');

 // Only an unmodified brand draft skips the creative review.
 const item=ops=>({type:'brand',payload:{mutateOperations:ops}}),mod=fn=>{const o=clone(b.ops);fn(o);return O.isBrandSearchDraft(item(o),'123');};
 check(O.isBrandSearchDraft(item(b.ops),'123'),'the unmodified brand draft is recognised');
 check(!mod(o=>{o[3].adGroupAdOperation.create.ad.responsiveSearchAd.headlines[0].text='Free gold today';}),'an edited headline needs review');
 check(!mod(o=>{o[3].adGroupAdOperation.create.ad.finalUrls=['https://example.com/'];}),'another landing page needs review');
 check(!mod(o=>{o.push({assetOperation:{create:{resourceName:'customers/123/assets/-99',imageAsset:{data:'AA=='}}}});}),'an image needs review');
 check(!mod(o=>{o[4].adGroupCriterionOperation.create.keyword.text='gold rings';}),'a non-brand keyword needs review');
 check(!mod(o=>{o.push(clone(o[1]));})&&!mod(o=>{o[1].campaignOperation.create.status='ENABLED';}),'a second or an enabled campaign is not the brand draft');
 check(!mod(o=>{o[o.length-2].assetOperation.create.finalUrls=['https://elsewhere.example/'];}),'a sitelink off the store needs review');
 check(!O.isBrandSearchDraft({type:'creative',payload:{mutateOperations:b.ops}},'123')&&!O.isBrandSearchDraft({type:'brand',payload:{mutateOperations:b.ops,designStudioSpec:{}}},'123')&&!O.isBrandSearchDraft(item(b.ops),'999'),'another type, a studio spec or another account keeps the review');

 // Sale windows from the occasion calendar: the weekend to Cyber Monday, the week before Valentine's Day and Mother's Day.
 const PEAKS={'cyber monday':['2025-12-01','2026-11-30','2027-11-29'],"valentine's day":['2026-02-14','2027-02-14'],"mother's day":['2026-05-10','2027-05-09']};
 const peakOf=(rule,from)=>(PEAKS[rule]||[]).find(d=>d>=from)||null;
 eq(O.saleWindow('bfcm',{today:'2026-09-29',peakOf}),{key:'bfcm',label:'Black Friday / Cyber Monday',start:'2026-11-27',end:'2026-11-30',days:4,peak:'2026-11-30',lastPeak:'2025-12-01',defaultPct:25},'Black Friday to Cyber Monday, with last year\'s date');
 let w=O.saleWindow('valentines',{today:'2026-09-29',peakOf});check(w.start==='2027-02-07'&&w.end==='2027-02-13'&&w.days===7&&w.lastPeak==='2026-02-14','the week before Valentine\'s Day');
 w=O.saleWindow('bfcm',{today:'2026-11-28',peakOf});check(w.start==='2026-11-29'&&w.end==='2026-11-30','a window already under way starts tomorrow');
 w=O.saleWindow('bfcm',{today:'2026-11-30',peakOf});check(w.start==='2027-11-26'&&w.peak==='2027-11-29','a passed window moves to next year');
 throwsRe(()=>O.saleWindow('mothers',{today:'2027-05-08',peakOf}),/No upcoming dates were found for Mother's Day/,'no known date: said plainly');
 throwsRe(()=>O.saleWindow('easter',{today:'2026-09-29',peakOf}),/Choose Black Friday/,'only the three dated occasions');
 check(O.checkSaleDates('2026-11-27','2026-11-30','2026-09-29')===4&&O.checkSaleDates('2026-11-01','2026-11-14','2026-09-29')===14,'1 to 14 days');
 throwsRe(()=>O.checkSaleDates('2026-09-29','2026-09-30','2026-09-29'),/must start after today/,'Google needs future dates');
 throwsRe(()=>O.checkSaleDates('2026-11-30','2026-11-27','2026-09-29'),/before the start date/,'end before start');
 throwsRe(()=>O.checkSaleDates('2026-11-01','2026-11-15','2026-09-29'),/at most 14 days/,'Google\'s 14-day limit');
 throwsRe(()=>O.checkSaleDates('2026-02-30','2026-03-01','2026-01-01'),/Choose a start and an end date/,'an impossible date');

 // The adjustment Google receives: CAMPAIGN scope, inclusive start, EXCLUSIVE end (midnight after the last day), a modifier.
 const base={cid:'123',label:'Black Friday / Cyber Monday',start:'2026-11-27',end:'2026-11-30',today:'2026-09-29',campaignIds:['101','103','101'],description:'d'};
 const s=O.seasonalityOperation({...base,changePct:'25'});
 eq(s.operation,{create:{name:'Brites · Black Friday / Cyber Monday · 2026-11-27 to 2026-11-30',description:'d',scope:'CAMPAIGN',campaigns:[Cp(101),Cp(103)],startDateTime:'2026-11-27 00:00:00',endDateTime:'2026-12-01 00:00:00',conversionRateModifier:1.25}},'the reviewed operation');
 check(s.days===4&&O.seasonalityOperation({...base,changePct:-30}).modifier===0.7,'+25% is 1.25; a lower expected rate is below 1');
 throwsRe(()=>O.seasonalityOperation({...base,changePct:0}),/0% leaves bidding unchanged/,'0% drafts nothing');
 for(const pct of [-51,151,'','x'])throwsRe(()=>O.seasonalityOperation({...base,changePct:pct}),/between -50% and \+150%/,'out of range: '+pct);
 throwsRe(()=>O.seasonalityOperation({...base,changePct:20,campaignIds:[]}),/at least one campaign/,'no campaign');
 throwsRe(()=>O.seasonalityOperation({...base,changePct:20,campaignIds:Array.from({length:2001},(x,i)=>String(i+1))}),/2,000 campaigns/,'Google\'s campaign limit');
 const el=(c,start='2026-11-27')=>O.seasonalityEligible(c,start);
 check(el({channel:'PERFORMANCE_MAX',bidding:'MAXIMIZE_CONVERSION_VALUE',status:'ENABLED'}).ok,'Performance Max with any bid strategy');
 check(el({channel:'SEARCH',bidding:'TARGET_ROAS',status:'ENABLED'}).ok&&el({channel:'SHOPPING',bidding:'TARGET_CPA',status:'PAUSED'}).ok&&el({channel:'DISPLAY',bidding:'TARGET_CPA',status:'ENABLED'}).ok,'Search, Shopping and Display on target ROAS or target CPA');
 check(el({channel:'SEARCH',bidding:'MAXIMIZE_CONVERSION_VALUE',targetRoas:3,status:'ENABLED'}).ok&&el({channel:'SEARCH',bidding:'MAXIMIZE_CONVERSIONS',targetCpaMicros:8e6,status:'ENABLED'}).ok,'Maximize strategies with a target');
 check(!el({channel:'SEARCH',bidding:'MAXIMIZE_CONVERSION_VALUE',status:'ENABLED'}).ok&&/target ROAS or target CPA/.test(el({channel:'SEARCH',bidding:'MANUAL_CPC',status:'ENABLED'}).reason),'no target (or manual CPC): left out');
 check(/not supported/.test(el({channel:'DEMAND_GEN',bidding:'TARGET_CPA',status:'ENABLED'}).reason),'other campaign types: left out');
 check(!el({channel:'SEARCH',bidding:'TARGET_ROAS',status:'ENABLED',servingStatus:'ENDED'}).ok&&!el({channel:'SEARCH',bidding:'TARGET_ROAS',status:'ENABLED',endDate:'2026-11-20'}).ok&&!el({channel:'SEARCH',bidding:'TARGET_ROAS',status:'REMOVED'}).ok&&!el(undefined).ok,'ended, ending first, removed or unknown campaigns: left out');
 check(!O.seasonalityOverlaps({start:'2026-11-27 00:00:00',endExclusive:'2026-12-01 00:00:00'},{start:'2026-12-01 00:00:00',endExclusive:'2026-12-03 00:00:00'})&&O.seasonalityOverlaps({start:'2026-11-27 00:00:00',endExclusive:'2026-12-01 00:00:00'},{start:'2026-11-30 00:00:00',endExclusive:'2026-12-02 00:00:00'}),'back-to-back windows do not overlap; shared days do');

 // Date-times have one layout, "yyyy-MM-dd HH:mm:ss": the v24 reference for Campaign and for BiddingSeasonalityAdjustment, the "Create
 // campaigns" guide and DateError all give it, and GAQL returns it. Some of Google's client samples send "yyyyMMdd HH:mm:ss" and it works, so
 // drafts saved earlier may hold it: it is read, and written back only in the documented layout.
 {const g=O.gadsDateTime,FULL='2026-11-06 23:59:59';
  eq([g('2026-11-06','23:59:59'),g('20261106','23:59:59'),g('20261106 23:59:59'),g(FULL),g('2026-11-06T23:59:59'),g(FULL,'00:00:00')],[FULL,FULL,FULL,FULL,FULL,FULL],'a date or a date-time, in either layout, is written as "yyyy-MM-dd HH:mm:ss"; a time already given is kept');
  check(g('2026-11-06')==='2026-11-06 00:00:00'&&g(' 20261106 ','07:30:00')==='2026-11-06 07:30:00'&&g('2026-11-06 9:05')==='2026-11-06 09:05:00'&&g('2026-11-06 09:05')==='2026-11-06 09:05:00','a date alone takes the time it is given (midnight by default); a short time is completed');
  check(g(g('20261106 23:59:59'))===FULL&&g(g('20261106','23:59:59'),'00:00:00')===FULL,'writing what was written changes nothing');
  check([null,undefined,'','nope','2026-13','2026-11-06 25','2026-11-06 23:59:59+00:00'].every(v=>g(v)===null),'anything else, a time zone offset included, is not a date-time');
  const win=O.seasonalityWindow('2026-11-27','2026-11-30');
  eq(win,{start:'2026-11-27 00:00:00',endExclusive:'2026-12-01 00:00:00'},'a sale\'s days: the first midnight to the (exclusive) midnight after the last day');
  eq([s.operation.create.startDateTime,s.operation.create.endDateTime],[win.start,win.endExclusive],'the strings the operation sends are the strings the overlap checks use');
  // A window read from Google, one saved in a draft and one built now overlap the same way in every layout they can arrive in.
  const compactDay=w=>({start:w.start.replace(/^(\d{4})-(\d{2})-(\d{2})/,'$1$2$3'),endExclusive:w.endExclusive.replace(/^(\d{4})-(\d{2})-(\d{2})/,'$1$2$3')}),dayOnly=w=>({start:w.start.slice(0,10),endExclusive:w.endExclusive.slice(0,10)});
  const layouts=w=>[w,compactDay(w),dayOnly(w)],shares={start:'2026-11-30 00:00:00',endExclusive:'2026-12-02 00:00:00'},later={start:'2026-12-01 00:00:00',endExclusive:'2026-12-03 00:00:00'};
  check(layouts(win).every(a=>layouts(shares).every(b=>O.seasonalityOverlaps(a,b)&&O.seasonalityOverlaps(b,a))&&layouts(later).every(b=>!O.seasonalityOverlaps(a,b)&&!O.seasonalityOverlaps(b,a))),'windows overlap the same way whether dashed, compact or a date alone: shared days overlap, back-to-back windows do not');
  check(['2026-11-06 23:59:59','20261106 23:59:59','2026-11-06','20261106','2026-11-06T23:59:59'].every(v=>O._dates.dateOnly(v)==='2026-11-06')&&O._dates.dateOnly('')===null,'the date part reads the same from every layout');}

 // The expected change is an estimate from the account's own history, or the occasion's starting estimate, said so.
 const hist=(ls,le,from,to,conv=10)=>{const rows=[];for(let d=from;d<=to;d=addDays(d,1))rows.push({date:d,clicks:200,conversions:d>=ls&&d<=le?conv:6});return rows;};
 const est=(rows,shiftDays=364)=>O.conversionRateEstimate(rows,{start:'2026-11-27',end:'2026-11-30',shiftDays,defaultPct:25});
 let e=est(hist('2025-11-28','2025-12-01','2025-10-01','2025-12-10'));
 check(e.measured&&e.pct===67&&/Estimate from last year: conversion rate 5\.0% on 2025-11-28 to 2025-12-01, against 3\.0% in the four weeks before/.test(e.basis),'measured: last year\'s dates converted 5.0% against 3.0% before, +67%');
 check(est(hist('2025-11-28','2025-12-01','2025-10-01','2025-12-10',100)).pct===150,'capped at +150%');
 e=est([]);check(!e.measured&&e.pct===25&&/Starting estimate: too little history/.test(e.basis),'too little history: the starting estimate');
 check(/could not be read/.test(est(null).basis)&&/dates for this occasion are unknown/.test(est([],0).basis),'unreadable or undated history: the starting estimate, said so');

 // New-customer acquisition: who can use which mode, and the exact request.
 const Ce=(ch,bid)=>({status:'ENABLED',channel:ch,bidding:bid}),ae=(c,m,o)=>O.acquisitionEligibility(c,m,o);
 check(ae(Ce('SEARCH','MAXIMIZE_CONVERSION_VALUE'),'BID_HIGHER_FOR_NEW_CUSTOMER').ok&&ae(Ce('PERFORMANCE_MAX','TARGET_CPA'),'TARGET_NEW_CUSTOMER').ok,'value bidding can bid higher; conversion bidding can target only new customers');
 check(/Maximize conversion value or target ROAS/.test(ae(Ce('SEARCH','TARGET_CPA'),'BID_HIGHER_FOR_NEW_CUSTOMER').reason)&&!ae(Ce('SEARCH','MANUAL_CPC'),'TARGET_NEW_CUSTOMER').ok,'bid strategy prerequisites');
 check(/Search, Performance Max, Shopping and Demand Gen/.test(ae(Ce('DISPLAY','TARGET_CPA'),'TARGET_NEW_CUSTOMER').reason),'campaign types Google offers it for');
 check(/Already off/.test(ae(Ce('SEARCH','TARGET_ROAS'),'TARGET_ALL_EQUALLY').reason)&&ae(Ce('SEARCH','TARGET_ROAS'),'TARGET_ALL_EQUALLY',{current:{mode:'TARGET_NEW_CUSTOMER'}}).ok,'off by default; "off" is offered only when it is on');
 check(/purchase conversion goal/.test(ae(Ce('SEARCH','TARGET_ROAS'),'BID_HIGHER_FOR_NEW_CUSTOMER',{purchaseGoal:false}).reason)&&!ae(Ce('SEARCH','TARGET_ROAS'),'SOMETHING').ok,'needs a purchase goal; unknown modes refused');
 eq(O.lifecycleGoalRequest({cid:'123',campaignId:'101',mode:'BID_HIGHER_FOR_NEW_CUSTOMER',value:'15',existing:null}),{operation:{create:{campaign:Cp(101),customerAcquisitionGoalSettings:{optimizationMode:'BID_HIGHER_FOR_NEW_CUSTOMER',valueSettings:{value:15}}}}},'create: the campaign and its settings');
 eq(O.lifecycleGoalRequest({cid:'123',campaignId:'103',mode:'TARGET_ALL_EQUALLY',existing:{mode:'BID_HIGHER_FOR_NEW_CUSTOMER',value:20,highLifetimeValue:50}}),{operation:{update:{resourceName:'customers/123/campaignLifecycleGoals/103',customerAcquisitionGoalSettings:{optimizationMode:'TARGET_ALL_EQUALLY'}},updateMask:'customer_acquisition_goal_settings.optimization_mode,customer_acquisition_goal_settings.value_settings'}},'update: by resource name without the campaign field; the value is cleared when off');
 check(O.lifecycleGoalRequest({cid:'123',campaignId:'103',mode:'BID_HIGHER_FOR_NEW_CUSTOMER',value:10,existing:{mode:'BID_HIGHER_FOR_NEW_CUSTOMER',value:20,highLifetimeValue:50}}).operation.update.customerAcquisitionGoalSettings.valueSettings.highLifetimeValue===50,'a high lifetime value set in Google Ads is kept');
 throwsRe(()=>O.lifecycleGoalRequest({cid:'123',campaignId:'101',mode:'BID_HIGHER_FOR_NEW_CUSTOMER',value:''}),/at least 0.01/,'bidding higher needs a value');
 throwsRe(()=>O.lifecycleGoalRequest({cid:'123',campaignId:'101',mode:'TARGET_NEW_CUSTOMER',value:5}),/applies only when bidding higher/,'a value only with bidding higher');
 const gErr=code=>({error:{code:400,message:'Request contains an invalid argument.',status:'INVALID_ARGUMENT',details:[{'@type':'type.googleapis.com/google.ads.googleads.v24.errors.GoogleAdsFailure',errors:[{errorCode:{campaignLifecycleGoalError:code},message:'x'}]}]}});
 check(O.lifecycleErrorText(gErr('CUSTOMER_ACQUISITION_MISSING_EXISTING_CUSTOMER_DEFINITION'),400)===O.ACQUISITION_PREREQUISITE+' Nothing was changed.','Google\'s prerequisite refusal in plain words');
 check(/^Google refused the new-customer setting: Backend error\.$/.test(O.lifecycleErrorText({error:{message:'Backend error'}},500)),'an unknown failure never claims nothing changed');

 // Purchase-only goals for new campaigns: PURCHASE stays biddable, every other category is switched off, explicitly.
 const goals=[{category:'PURCHASE',origin:'WEBSITE',biddable:true},{category:'PURCHASE',origin:'GOOGLE_HOSTED',biddable:true},{category:'ADD_TO_CART',origin:'WEBSITE',biddable:true},{category:'BEGIN_CHECKOUT',origin:'WEBSITE'},{category:'PURCHASE',origin:'STORE'},{category:'PAGE_VIEW',origin:'WEBSITE'},{category:'ADD_TO_CART',origin:'WEBSITE',biddable:true},{category:'UNKNOWN',origin:'WEBSITE',biddable:true}];
 const g=O.purchaseOnlyGoalOps({campaigns:[Cp(-2)],goals});
 eq(g.ops.map(o=>[o.campaignConversionGoalOperation.update.resourceName.split('/').pop(),o.campaignConversionGoalOperation.update.biddable,o.campaignConversionGoalOperation.updateMask]),[['-2~PURCHASE~WEBSITE',true,'biddable'],['-2~PURCHASE~GOOGLE_HOSTED',true,'biddable'],['-2~ADD_TO_CART~WEBSITE',false,'biddable'],['-2~BEGIN_CHECKOUT~WEBSITE',false,'biddable'],['-2~PAGE_VIEW~WEBSITE',false,'biddable']],'purchase kept biddable; add to cart, begin checkout and page view off; a purchase goal the account keeps out stays out');
 check(g.ops.every(o=>/^customers\/123\/campaignConversionGoals\/-2~/.test(o.campaignConversionGoalOperation.update.resourceName)),'addressed to the new campaign by its temporary ID');
 eq({off:g.off,kept:g.kept},{off:['ADD_TO_CART','BEGIN_CHECKOUT','PAGE_VIEW'],kept:['PURCHASE~WEBSITE','PURCHASE~GOOGLE_HOSTED']},'what was switched off and kept');
 throwsRe(()=>O.purchaseOnlyGoalOps({campaigns:[Cp(555)],goals}),/only for campaigns created in the same request/,'an existing campaign can never be addressed');
 check(O.purchaseOnlyGoalOps({campaigns:[Cp(-2),Cp(-9)],goals}).ops.length===10,'each new campaign gets its own goals');
 const none=O.purchaseOnlyGoalOps({campaigns:[Cp(-2)],goals:[{category:'ADD_TO_CART',origin:'WEBSITE',biddable:true}]});
 check(!none.ops.length&&/no biddable purchase goal/.test(none.note),'no biddable purchase goal: Smart Bidding keeps the account goals, and says so');

 // Campaign total budgets: CUSTOM_PERIOD with total_amount_micros, the same spend over the draft's fixed dates.
 const T0='2026-09-29',draft=(camp={},bud={amountMicros:10e6})=>[{campaignBudgetOperation:{create:{resourceName:B(-1),name:'b',deliveryMethod:'STANDARD',explicitlyShared:false,...bud}}},{campaignOperation:{create:{resourceName:Cp(-2),name:'BA · x',status:'PAUSED',advertisingChannelType:'SEARCH',campaignBudget:B(-1),manualCpc:{enhancedCpcEnabled:false},startDateTime:'20261120 00:00:00',endDateTime:'20261130 23:59:59',...camp}}}];
 const d0=draft(),on=O.setTotalBudget(d0,{on:true,today:T0}),nb=on.ops[0].campaignBudgetOperation.create;
 check(on.total===110&&on.days===11&&on.daily===10&&on.start==='2026-11-20'&&on.end==='2026-11-30'&&!on.startsOnEnable,'10 a day for 11 days: a total of 110');
 check(nb.period==='CUSTOM_PERIOD'&&nb.totalAmountMicros===110e6&&!('amountMicros' in nb)&&nb.explicitlyShared===false&&d0[0].campaignBudgetOperation.create.amountMicros===10e6,'CUSTOM_PERIOD with total_amount_micros only; the draft passed in is untouched');
 check(Math.abs(O.draftBudgetDaily(on.ops,{today:T0})-10)<1e-9,'the ceiling counts it as 10 a day');
 const off=O.setTotalBudget(on.ops,{on:false,today:T0,prior:{daily:10}}).ops[0].campaignBudgetOperation.create;
 check(off.amountMicros===10e6&&!('period' in off)&&!('totalAmountMicros' in off),'and back to the same daily budget');
 check(O.setTotalBudget(O.setTotalBudget(draft({},{amountMicros:12e6}),{on:true,today:T0}).ops,{on:false,today:T0}).daily===12,'without the saved amount: the total over the days');
 const now=O.setTotalBudget(draft({startDateTime:undefined,endDateTime:'20261008 23:59:59'}),{on:true,today:T0});
 check(now.days===10&&now.start===T0&&now.startsOnEnable,'no future start: counted from today');
 throwsRe(()=>O.setTotalBudget(draft({endDateTime:undefined}),{on:true,today:T0}),/Set an end date first/,'a total budget needs an end date');
 throwsRe(()=>O.setTotalBudget(draft({startDateTime:'20261001 00:00:00',endDateTime:'20261231 23:59:59'}),{on:true,today:T0}),/up to 90 days; these dates run 92 days/,'Google\'s 90-day limit');
 throwsRe(()=>O.setTotalBudget(draft({advertisingChannelType:'DISPLAY'}),{on:true,today:T0}),/not for this campaign type/,'not for Display');
 throwsRe(()=>O.setTotalBudget(draft({manualCpc:undefined,commission:{commissionRateMicros:1}}),{on:true,today:T0}),/bid strategy doesn't support/,'unsupported Search bidding');
 check(O.setTotalBudget(draft({advertisingChannelType:'PERFORMANCE_MAX',manualCpc:undefined,maximizeConversionValue:{}}),{on:true,today:T0}).total===110,'Performance Max on Maximize conversion value');
 throwsRe(()=>O.setTotalBudget(draft({advertisingChannelType:'PERFORMANCE_MAX',manualCpc:undefined,targetSpend:{}}),{on:true,today:T0}),/bid strategy/,'Performance Max on maximize clicks: not offered');
 throwsRe(()=>O.setTotalBudget(on.ops,{on:true,today:T0}),/already uses a total budget/,'already on');
 throwsRe(()=>O.setTotalBudget(draft().concat(draft()),{on:true,today:T0}),/one new campaign with its own budget/,'one campaign, its own budget');
 throwsRe(()=>O.setTotalBudget(draft({startDateTime:'20260801 00:00:00',endDateTime:'20260901 23:59:59'}),{on:true,today:T0}),/end date has passed/,'dates already over');
 const late=clone(on.ops);late[1].campaignOperation.create.startDateTime='20260901 00:00:00';late[1].campaignOperation.create.endDateTime='20260920 23:59:59';
 throwsRe(()=>O.draftBudgetDaily(late,{today:T0}),/end date has passed/,'a total budget whose dates passed is never published');
 check(O.draftBudgetDaily(draft().concat([{campaignBudgetOperation:{create:{resourceName:B(-5),amountMicros:2.5e6}}}]),{today:T0})===12.5,'daily budgets add up');
 const tb={period:'CUSTOM_PERIOD',totalAmountMicros:'300000000'};
 check(O.budgetDailyEquivalent({amountMicros:'20000000'},{})===20,'a daily budget is its amount');
 check(O.budgetDailyEquivalent(tb,{start:'2026-09-20',end:'2026-10-08',today:T0,spent:60})===24,'what is left (240) over the 10 days left');
 check(O.budgetDailyEquivalent(tb,{start:'2026-10-10',end:'2026-10-19',today:T0})===30,'a future flight over its own days');
 check(O.budgetDailyEquivalent(tb,{start:'2026-09-01',end:'2026-09-28',today:T0})===0&&O.budgetDailyEquivalent(tb,{start:'2026-09-01',end:'2026-10-08',today:T0,spent:400})===0,'ended or spent: nothing more');
 check(O.budgetDailyEquivalent(tb,{start:'2026-09-01',end:null,today:T0,spent:100})===200,'unknown end: all of what is left, never less');
}

/* ---------------- the engine against a fake account (CAD, ceiling 100) ---------------- */
function memory(){const docs=new Map();let n=0;
 const setPath=(o,k,v)=>{const p=k.split('.');for(let i=0;i<p.length-1;i++){if(!o[p[i]]||typeof o[p[i]]!=='object')o[p[i]]={};o=o[p[i]];}o[p[p.length-1]]=v;};
 const doc=p=>({path:p,id:p.split('/').pop(),get:async()=>({exists:docs.has(p),id:p.split('/').pop(),data:()=>clone(docs.get(p))}),set:async(v,o)=>{docs.set(p,o&&o.merge?{...docs.get(p),...clone(v)}:clone(v));},
   update:async v=>{if(!docs.has(p))throw Error('missing '+p);const cur=clone(docs.get(p));Object.entries(clone(v)).forEach(([k,x])=>setPath(cur,k,x));docs.set(p,cur);},delete:async()=>{docs.delete(p);},collection:c=>col(p+'/'+c)});
 const col=(p,fs=[])=>({doc:id=>doc(p+'/'+id),add:async v=>{const r=doc(p+'/auto'+(++n));await r.set(v);return r;},where:(k,op,v)=>col(p,fs.concat([[k,v]])),limit:()=>col(p,fs),orderBy:()=>col(p,fs),
   get:async()=>{const list=[...docs].filter(([k,v])=>k.startsWith(p+'/')&&!k.slice(p.length+1).includes('/')&&fs.every(([fk,fv])=>v[fk]===fv)).map(([k,v])=>({id:k.split('/').pop(),data:()=>clone(v)}));return {docs:list,empty:!list.length,size:list.length,forEach:fn=>list.forEach(fn)};}});
 return {docs,db:{collection:c=>col(c),runTransaction:async fn=>fn({get:r=>r.get(),set:(r,v,o)=>r.set(v,o),update:(r,v)=>r.update(v),delete:r=>docs.delete(r.path)})},FV:{serverTimestamp:()=>Date.now()}};}

async function engineChecks(){
 const env={GADS_CUSTOMER_ID:'123'},net=[];let reply=null;
 // The only direct HTTP call under test is the lifecycle-goal configure request; anything else is refused.
 const fakeFetch=async(url,opts={})=>{const body=opts.body?JSON.parse(opts.body):null;net.push({url:String(url),method:opts.method,headers:clone(opts.headers),body});if(!reply)throw Error('Live network forbidden: '+url);const r=reply(String(url),body);return {ok:r.status>=200&&r.status<300,status:r.status,json:async()=>clone(r.data)};};
 const cx=vm.createContext({module:{exports:{}},exports:{},require:n=>n==='node-fetch'?fakeFetch:realRequire(n),process:{env},console,Buffer,Date,Intl,Map,Set,URL,setTimeout:fn=>setImmediate(fn),clearTimeout});
 vm.runInContext(fs.readFileSync(dir+'googleAdsAutopilot.js','utf8'),cx);
 const E=cx.module.exports,get=n=>vm.runInContext(n,cx),bind=v=>{cx.__m=v;vm.runInContext(Object.keys(v).map(k=>k+'=__m.'+k).join('\n'),cx);};
 const tz='America/Toronto',today=get('_acctDateYmd')(tz,0),day=n=>addDays(today,n),ap=id=>f.docs.get('Brites_GAds_Approvals/'+id);
 let W,f,sent,ledgers,queries;
 const reset=()=>{W={camps:{
     101:{name:'BA · rings-evergreen',status:'ENABLED',serving:'SERVING',channel:'SEARCH',bidding:'MAXIMIZE_CONVERSION_VALUE',targetRoas:3,res:B(11),budget:20},
     102:{name:'BA · charms-manual',status:'ENABLED',serving:'SERVING',channel:'SEARCH',bidding:'MANUAL_CPC',res:B(12),budget:10},
     103:{name:'Brites PMax · all products',status:'ENABLED',serving:'SERVING',channel:'PERFORMANCE_MAX',bidding:'MAXIMIZE_CONVERSION_VALUE',res:B(13),budget:30},
     104:{name:'Display · returning visitors',status:'PAUSED',serving:'NONE',channel:'DISPLAY',bidding:'TARGET_CPA',res:B(14),budget:5},
     105:{name:'BA · summer-sale',status:'ENABLED',serving:'ENDED',channel:'SEARCH',bidding:'TARGET_ROAS',res:B(15),budget:40,end:day(-5)}},
   // The account's goals: Add to cart and Begin checkout biddable beside purchases (Google omits a false biddable).
   goals:[{category:'PURCHASE',origin:'WEBSITE',biddable:true},{category:'ADD_TO_CART',origin:'WEBSITE',biddable:true},{category:'BEGIN_CHECKOUT',origin:'WEBSITE',biddable:true},{category:'PAGE_VIEW',origin:'WEBSITE'},{category:'PURCHASE',origin:'STORE'}],
   lifecycle:{103:{mode:'BID_HIGHER_FOR_NEW_CUSTOMER',value:20,high:50}},purchase:{101:true,102:false,103:true},adjustments:[],spend:{},fail:[],history:null,ctrl:{},mutateError:null,
   brandLists:[],sharedSets:[]};
   f=memory();sent=[];ledgers=[];queries=[];net.length=0;reply=null;};
 const factRow=id=>{const c=W.camps[id];return {campaign:{resourceName:Cp(id),id:String(id),name:c.name,status:c.status,servingStatus:c.serving,advertisingChannelType:c.channel,biddingStrategyType:c.bidding,...(c.targetRoas?{maximizeConversionValue:{targetRoas:c.targetRoas}}:{}),endDateTime:(c.end||'2037-12-30')+' 23:59:59'}};};
 const budgetRow=id=>{const c=W.camps[id];return {campaign:{resourceName:Cp(id),id:String(id),name:c.name,status:c.status,servingStatus:c.serving,startDateTime:(c.start||'2026-01-05')+' 00:00:00',endDateTime:(c.end||'2037-12-30')+' 23:59:59'},
   campaignBudget:c.total!=null?{resourceName:c.res,period:'CUSTOM_PERIOD',totalAmountMicros:String(c.total*1e6)}:{resourceName:c.res,period:'DAILY',amountMicros:String(c.budget*1e6)}};};
 const live=()=>Object.keys(W.camps).filter(id=>W.camps[id].status!=='REMOVED');
 const gaql=async q=>{queries.push(q);assert.match(q.trim(),/^SELECT\s/,'the fake account only answers reads');
   if(W.fail.some(re=>re.test(q)))throw Error('Google Ads is unavailable right now');
   if(q.includes('FROM customer_conversion_goal'))return W.goals.map(x=>({customerConversionGoal:clone(x)}));
   if(q.includes('FROM campaign_lifecycle_goal'))return Object.entries(W.lifecycle).map(([id,x])=>({campaignLifecycleGoal:{resourceName:'customers/123/campaignLifecycleGoals/'+id,campaign:Cp(id),customerAcquisitionGoalSettings:{optimizationMode:x.mode,...(x.value!=null?{valueSettings:{value:x.value,...(x.high!=null?{highLifetimeValue:x.high}:{})}}:{})}}}));
   if(q.includes('FROM campaign_conversion_goal'))return Object.entries(W.purchase).map(([id,bid])=>({campaign:{id},campaignConversionGoal:{category:'PURCHASE',origin:'WEBSITE',...(bid?{biddable:true}:{})}}));
   if(q.includes('FROM bidding_seasonality_adjustment'))return W.adjustments.map(a=>({biddingSeasonalityAdjustment:clone(a)}));
   if(q.includes('FROM campaign_criterion'))return W.brandLists.map(([id,s])=>({campaign:{id},campaignCriterion:{brandList:{sharedSet:s}}}));
   if(q.includes('FROM shared_set'))return W.sharedSets.map(s=>({sharedSet:{resourceName:s}}));
   if(q.includes('metrics.clicks, metrics.conversions FROM customer')){const m=q.match(/BETWEEN '([\d-]+)' AND '([\d-]+)'/);return W.history?W.history(m[1],m[2]):[];}
   if(q.includes('campaign.bidding_strategy_type'))return live().map(factRow);
   if(q.includes('metrics.cost_micros FROM campaign WHERE campaign.id IN')){const ids=q.match(/IN \(([^)]*)\)/)[1].split(',').map(x=>x.trim());return ids.filter(id=>W.spend[id]!=null).map(id=>({campaign:{id},metrics:{costMicros:String(W.spend[id]*1e6)}}));}
   if(/campaign\.start_date_time.* FROM campaign WHERE campaign\.id IN \(/.test(q)){const ids=q.match(/IN \(([^)]*)\)/)[1].split(',').map(x=>x.trim());return ids.filter(id=>W.camps[id]).map(budgetRow);}
   if(q.includes("FROM campaign WHERE campaign.status = 'ENABLED'"))return Object.keys(W.camps).filter(id=>W.camps[id].status==='ENABLED').map(budgetRow);
   if(q.includes("campaign.status != 'REMOVED'"))return live().map(id=>({campaign:{id,name:W.camps[id].name,status:W.camps[id].status}}));
   const m=q.match(/FROM campaign WHERE campaign\.id = (\d+)/);if(m)return W.camps[m[1]]?[budgetRow(m[1])]:[];
   throw Error('unexpected query '+q);};
 const dispatch=(service,ops,o)=>{const vo=o.validateOnly!=null?!!o.validateOnly:!!(o.ctrl&&o.ctrl.dryRun);if(o.onDispatch)o.onDispatch();sent.push({service,ops:clone(ops),validateOnly:vo,label:o.label||''});if(W.mutateError)throw W.mutateError;return vo;};
 const mutate=async(service,ops,o={})=>dispatch(service,ops,o)?{}:{results:ops.map((x,i)=>({resourceName:`customers/123/${service}/${900+i}`}))};
 const mutateAll=async(ops,o={})=>dispatch('all',ops,o)?{}:{mutateOperationResponses:ops.map(x=>x.campaignOperation&&x.campaignOperation.create?{campaignResult:{resourceName:Cp(777)}}:{anyResult:{resourceName:'customers/123/x/1'}})};
 const COLLS=[{handle:'best-sellers',title:'Best Sellers'},{handle:'celestial',title:'Celestial'},{handle:'all',title:'All'},{handle:'bar-engraved',title:'Bar Engraved'},{handle:'nurse',title:'Nurse Charms'}];
 const C=(x={})=>({enabled:true,dryRun:false,maxDailyBudgetTotal:100,budgetCurrency:'CAD',defaultCountries:['2124','2840'],...W.ctrl,...x});
 bind({fb:()=>f,gaql,mutate,mutateAll,ledger:async x=>{ledgers.push(clone(x));return 'L'+ledgers.length;},_verifyLedger:async()=>{},control:async()=>C(),_accountCurrency:async()=>'CAD',_accountTz:async()=>tz,
   getCollections:async()=>clone(COLLS),mintToken:async()=>'test-token',_deletedCampaignIds:async()=>new Set(),_assertCampaignNotDeleted:async()=>{}});
 const peakOf=get('_nextOccasionPeak'),sw=O.saleWindow('bfcm',{today,peakOf}),shift=daysInclusive(sw.lastPeak,sw.peak)-1,ls=addDays(sw.start,-shift),le=addDays(sw.end,-shift);
 // Last year's Black Friday weekend converted 5% of clicks; the rest of the year 3%.
 const history=(from,to)=>{const rows=[];for(let d=from;d<=to;d=addDays(d,1))rows.push({segments:{date:d},metrics:{clicks:'200',conversions:d>=ls&&d<=le?10:6}});return rows;};

 // Brand campaign settings match the console's Search builder.
 {const S=E.buildSearchCampaignOps({handle:'rings',title:'Rings'},null,{headlines:['Rings for her','Gold rings','Silver rings'],descriptions:['Handmade rings.','Made to order.']},{dailyBudget:5,countries:['2124'],maxCpc:0.5,smartBidding:false,keywordPlan:['rings for her','gold rings','silver rings','ring gifts'],withAssets:false});
  const sc=S.ops[1].campaignOperation.create,bc=O.buildBrandSearchOps({cid:'123',dailyBudget:5,maxCpc:0.5,countries:['2124']}).ops[1].campaignOperation.create;
  for(const k of ['status','advertisingChannelType','containsEuPoliticalAdvertising','manualCpc','networkSettings','finalUrlSuffix','geoTargetTypeSetting'])eq(bc[k],sc[k],'brand '+k+' matches the Search builder');
  check(S.ops.some(o=>o.campaignCriterionOperation&&o.campaignCriterionOperation.create.language&&o.campaignCriterionOperation.create.language.languageConstant==='languageConstants/1000'),'both target English');}

 // The panel: read-only.
 reset();W.history=history;
 let st=await E.campaignOptionsStatus();
 check(st.ok&&st.today===today&&st.currency==='CAD'&&!st.warnings.length,'campaign options read');
 check(st.brand.campaign===null&&st.brand.draft===null&&clone(st.brand.countries).join()==='2124,2840'&&st.brand.defaults.dailyBudget===5&&st.brand.pairing===O.BRAND_SEARCH.pairing,'brand option: none yet, the account\'s countries, the Performance Max pairing');
 const bf=st.sale.occasions.find(o=>o.key==='bfcm');
 check(st.sale.occasions.length===3&&bf.start===sw.start&&bf.end===sw.end&&bf.estimate.measured&&bf.estimate.pct===67,'three dated occasions from the opportunity calendar; last year\'s results give +67% (an estimate)');
 eq(bf.campaigns.map(c=>c.id).sort(),['101','103','104'],'offered for smart-bidding campaigns only (target ROAS, Performance Max, Display on target CPA)');
 const cu=st.customers.campaigns,byId=id=>cu.find(c=>c.id===id);
 check(!byId('104')&&byId('101').mode==='TARGET_ALL_EQUALLY'&&byId('101').allowed.BID_HIGHER_FOR_NEW_CUSTOMER===true&&/Already off/.test(byId('101').allowed.TARGET_ALL_EQUALLY),'new-customer goal: off by default, not offered for Display');
 check(/Maximize conversion value or target ROAS/.test(byId('102').allowed.BID_HIGHER_FOR_NEW_CUSTOMER)&&byId('103').mode==='BID_HIGHER_FOR_NEW_CUSTOMER'&&byId('103').value===20&&byId('103').allowed.TARGET_ALL_EQUALLY===true,'prerequisites and Google\'s current setting shown');
 check(/^Off by default\./.test(st.customers.tradeoff),'the one-line tradeoff');
 W.fail=[/FROM campaign_lifecycle_goal/,/FROM bidding_seasonality_adjustment/];st=await E.campaignOptionsStatus();W.fail=[];
 check(st.ok&&st.warnings.length===2,'unreadable optional data is a warning, never a broken panel');
 check(queries.every(q=>/^SELECT\s/.test(q.trim()))&&!sent.length&&!net.length&&!f.docs.size,'the panel only reads');

 // Brand Search draft.
 reset();
 let r=await E.draftBrandSearch({dailyBudget:'6',maxCpc:'0.4'}),doc=ap(r.approvalId);
 check(r.ok&&doc.status==='PENDING'&&doc.type==='brand'&&doc.creative===null&&doc.vetted===false&&!E.needsCreativeReview(doc),'brand draft waits in Approvals; its fixed brand copy needs no paid AI and no creative production');
 check(/manual CPC up to CAD 0\.40 a click; CAD 6\.00\/day; people in the target countries only; starts PAUSED/.test(doc.summary)&&doc.summary.includes(O.BRAND_SEARCH.pairing),'summary: CPC cap, budget, presence, paused, and the pairing with the Performance Max brand exclusion');
 const sl=doc.payload.mutateOperations.filter(o=>o.assetOperation&&o.assetOperation.create.sitelinkAsset).map(o=>o.assetOperation.create.finalUrls[0]);
 check(sl.length===4&&['all','best-sellers','bar-engraved','celestial'].every(h=>sl.includes('https://britesjewelry.com/collections/'+h))&&doc.payload.mutateOperations.some(o=>o.assetOperation&&o.assetOperation.create.calloutAsset),'sitelinks and callouts from the Search builder, real store pages only');
 check(clone(doc.payload.countries).join()==='2124,2840'&&doc.payload.maxCpc===0.4&&!sent.length&&!net.length,'drafting sends nothing to Google');
 await rejects(E.draftBrandSearch({}),/already waiting in Approvals/,'one brand draft at a time');
 reset();W.ctrl={maxDailyBudgetTotal:62};
 await rejects(E.draftBrandSearch({dailyBudget:6,maxCpc:0.4}),/CAD 6\.00\/day budget doesn't fit: enabled campaigns already use CAD 60\.00 of the CAD 62\.00\/day ceiling/,'the ceiling counts spendable budgets (ended ones excluded)');
 check(![...f.docs.keys()].some(k=>k.startsWith('Brites_GAds_Approvals/')),'over the ceiling: no draft');
 reset();W.camps[106]={name:'BA · brand-search',status:'PAUSED',serving:'NONE',channel:'SEARCH',bidding:'MANUAL_CPC',res:B(16),budget:5};
 await rejects(E.draftBrandSearch({}),/already exists/,'never a second brand campaign');
 reset();await rejects(E.draftBrandSearch({dailyBudget:0}),/Daily budget must be between 1 and 50/,'budget limits');

 // Publishing a new campaign: purchase-only goals ride in the same request, after the campaign.
 reset();const bid=(await E.draftBrandSearch({})).approvalId,stored=clone(ap(bid).payload.mutateOperations);await E.markApprovalApproved(bid);
 check(ap(bid).status==='APPROVED','approved without a creative review');
 W.fail=[/FROM customer_conversion_goal/];
 await rejects(E.applyApproval(bid,C()),/conversion goals could not be read, so the new campaign was not sent/,'goals unreadable');
 check(!sent.length&&ap(bid).status==='APPROVED'&&/could not be read/.test(ap(bid).lastError)&&!f.docs.has('Brites_GAds_State/publicationLease'),'goals unreadable: nothing sent, the draft stays approved to retry, the lease released');
 W.fail=[];let res=await E.applyApproval(bid,C({dryRun:true}));
 check(res.status==='VALIDATED'&&sent.length===1&&sent[0].validateOnly&&sent[0].ops.some(o=>o.campaignConversionGoalOperation)&&ap(bid).status==='APPROVED'&&!ap(bid).purchaseGoals,'dry run validates the whole request, goals included');
 sent.length=0;res=await E.applyApproval(bid,C());
 const [val,pub]=sent,firstGoal=pub.ops.findIndex(o=>o.campaignConversionGoalOperation);
 check(res.status==='APPLIED'&&sent.length===2&&val.validateOnly&&val.label==='validate-new-campaign:'+bid&&!pub.validateOnly&&pub.label==='reviewed-approval:'+bid,'validated, then published once');
 eq(pub.ops.slice(0,firstGoal),stored,'exactly the reviewed operations first');
 eq(pub.ops.slice(firstGoal).map(o=>o.campaignConversionGoalOperation),[
   {update:{resourceName:'customers/123/campaignConversionGoals/-2~PURCHASE~WEBSITE',biddable:true},updateMask:'biddable'},
   {update:{resourceName:'customers/123/campaignConversionGoals/-2~ADD_TO_CART~WEBSITE',biddable:false},updateMask:'biddable'},
   {update:{resourceName:'customers/123/campaignConversionGoals/-2~BEGIN_CHECKOUT~WEBSITE',biddable:false},updateMask:'biddable'},
   {update:{resourceName:'customers/123/campaignConversionGoals/-2~PAGE_VIEW~WEBSITE',biddable:false},updateMask:'biddable'}],'then the new campaign\'s own goals: purchase biddable, add to cart, begin checkout and page view off');
 const pg=ap(bid).purchaseGoals;
 check(ap(bid).status==='APPLIED'&&clone(ap(bid).publishedCampaignIds).join()==='777'&&pg.campaigns===1&&clone(pg.off).join()==='ADD_TO_CART,BEGIN_CHECKOUT,PAGE_VIEW'&&clone(pg.kept).join()==='PURCHASE~WEBSITE','the approval records which goals were switched off');
 reset();W.goals=[{category:'ADD_TO_CART',origin:'WEBSITE',biddable:true}];
 {const id=(await E.draftBrandSearch({})).approvalId;await E.markApprovalApproved(id);await E.applyApproval(id,C());
  check(!sent[1].ops.some(o=>o.campaignConversionGoalOperation)&&/no biddable purchase goal/.test(ap(id).purchaseGoals.note),'no biddable purchase goal: the account goals are kept, and the approval says so');}
 {const pgo=get('_purchaseGoalOps');reset();
  let x=await pgo([{campaignOperation:{update:{resourceName:Cp(101),status:'PAUSED'}}},{adGroupAdOperation:{create:{}}}]);
  check(x.campaigns===0&&!x.ops.length&&!queries.length,'changes to existing campaigns: nothing read, nothing added, no live campaign touched');
  x=await pgo([{campaignOperation:{create:{resourceName:Cp(-7),advertisingChannelType:'DISPLAY'}}}]);check(x.campaigns===0&&!x.ops.length,'Display is left as it is');
  x=await pgo([{campaignOperation:{create:{resourceName:Cp(-5),advertisingChannelType:'PERFORMANCE_MAX'}}},{campaignOperation:{create:{resourceName:Cp(-9),advertisingChannelType:'SEARCH'}}}]);
  check(x.campaigns===2&&x.ops.length===8&&x.ops.every(o=>/campaignConversionGoals\/-(5|9)~/.test(o.campaignConversionGoalOperation.update.resourceName)),'new Performance Max and Search campaigns both bid for purchases only');}
 // New Performance Max campaigns keep the account's Performance Max brand exclusions.
 {const bx=get('_pmaxBrandExclusionOps'),pm=[{campaignOperation:{create:{resourceName:Cp(-5),advertisingChannelType:'PERFORMANCE_MAX'}}}];reset();
  let x=await bx([{campaignOperation:{create:{resourceName:Cp(-2),advertisingChannelType:'SEARCH'}}}]);
  check(!x.ops.length&&!queries.length,'Search campaigns get no brand exclusion (the brand Search campaign must bid on the brand)');
  x=await bx(pm);check(!x.ops.length&&x.campaigns===1&&queries.length===2,'no brand exclusion in the account yet: nothing added');
  W.brandLists=[['103','customers/123/sharedSets/55'],['107','customers/123/sharedSets/55'],['103','customers/123/sharedSets/56'],['103','customers/999/sharedSets/57']];W.sharedSets=['customers/123/sharedSets/55'];
  x=await bx(pm);eq(x.ops,[{campaignCriterionOperation:{create:{campaign:Cp(-5),negative:true,brandList:{sharedSet:'customers/123/sharedSets/55'}}}}],'the brand list the other Performance Max campaigns exclude, once; removed lists and other accounts ignored');
  x=await bx(pm.concat([{campaignCriterionOperation:{create:{campaign:Cp(-5),negative:true,brandList:{sharedSet:'customers/123/sharedSets/-1'}}}}]));check(!x.ops.length,'a campaign built with its own brand exclusion is left as built');
  W.fail=[/FROM shared_set/];await rejects(bx(pm),/brand exclusions could not be read, so the new campaign was not sent/,'unreadable brand exclusions: nothing is sent');W.fail=[];}

 // Sale-day bid adjustments.
 reset();W.history=history;
 const sale=(x={})=>E.draftSeasonalityAdjustment({occasion:'bfcm',startDate:sw.start,endDate:sw.end,campaignIds:['101'],...x});
 await rejects(sale({campaignIds:['101','102']}),/“BA · charms-manual”: needs target ROAS or target CPA bidding\. Sale-day adjustments apply only to smart-bidding campaigns\. No draft was created/,'manual CPC refused');
 await rejects(sale({campaignIds:['105']}),/ends before these dates/,'an ended campaign refused');
 await rejects(sale({startDate:today}),/must start after today/,'today refused');
 await rejects(sale({startDate:day(1),endDate:day(15)}),/at most 14 days/,'15 days refused');
 W.adjustments=[{name:'Google Black Friday',scope:'CAMPAIGN',campaigns:[Cp(103)],startDateTime:sw.start+' 00:00:00',endDateTime:addDays(sw.start,2)+' 00:00:00',conversionRateModifier:1.3}];
 await rejects(sale({campaignIds:['101','103']}),/already has the sale-day adjustment “Google Black Friday”/,'an overlapping Google adjustment refused');
 W.adjustments=[{name:'Channel-wide',scope:'CHANNEL',campaigns:[],startDateTime:sw.start+' 00:00:00',endDateTime:addDays(sw.start,1)+' 00:00:00',conversionRateModifier:1.2}];
 await rejects(sale(),/already has the sale-day adjustment “Channel-wide”/,'a channel-wide adjustment on those dates counts as overlapping');
 W.adjustments=[];check(!f.docs.size&&!sent.length,'refusals create nothing');
 const sd=await sale({changePct:'',campaignIds:['101','103','101']}),sdoc=ap(sd.approvalId),sop=sdoc.payload.seasonalityAdjustment.operation.create;
 check(sdoc.status==='PENDING'&&sdoc.type==='seasonality'&&sdoc.creative===null,'sale-day draft waits in Approvals');
 eq([sop.scope,sop.campaigns,sop.startDateTime,sop.endDateTime,sop.conversionRateModifier],['CAMPAIGN',[Cp(101),Cp(103)],sw.start+' 00:00:00',addDays(sw.end,1)+' 00:00:00',1.67],'campaign scope, the sale days (end exclusive), +67% as 1.67');
 check(/expected conversion rate \+67% \(estimate\)/.test(sdoc.summary)&&sdoc.payload.campaignOption.estimate.measured&&/\(estimate\)\. Estimate from last year/.test(sop.description),'stated as an estimate from last year, in Approvals and in Google');
 await rejects(sale({changePct:10,campaignIds:['103']}),/already waiting in Approvals/,'an overlapping draft refused');
 await E.markApprovalApproved(sd.approvalId);
 res=await E.applyApproval(sd.approvalId,C({dryRun:true}));
 check(res.status==='VALIDATED'&&sent.length===1&&sent[0].service==='biddingSeasonalityAdjustments'&&sent[0].validateOnly&&ap(sd.approvalId).status==='APPROVED','dry run validates only');
 sent.length=0;res=await E.applyApproval(sd.approvalId,C());
 check(res.status==='APPLIED'&&sent.length===1&&sent[0].service==='biddingSeasonalityAdjustments'&&!sent[0].validateOnly&&sent[0].label==='reviewed-approval:'+sd.approvalId,'published once through biddingSeasonalityAdjustments:mutate');
 eq(sent[0].ops,[sdoc.payload.seasonalityAdjustment.operation],'exactly the reviewed operation');
 reset();
 {const id=(await sale({changePct:20})).approvalId;await E.markApprovalApproved(id);W.camps[101].bidding='MANUAL_CPC';delete W.camps[101].targetRoas;
  await rejects(E.applyApproval(id,C()),/no longer uses smart bidding/,'bidding changed since the draft');check(!sent.length&&ap(id).status==='APPROVED','nothing sent');
  // Stored drafts edited behind the console's back are refused before anything is sent.
  const edit=fn=>fn(f.docs.get('Brites_GAds_Approvals/'+id).payload.seasonalityAdjustment.operation.create);
  W.camps[101].bidding='TARGET_ROAS';edit(x=>x.campaigns.push('customers/999/campaigns/1'));
  await rejects(E.applyApproval(id,C()),/unexpected operation/,'another account\'s campaign');
  edit(x=>{x.campaigns.pop();x.startDateTime=today+' 00:00:00';});
  await rejects(E.applyApproval(id,C()),/start date has passed/,'dates passed while waiting');check(!sent.length&&ap(id).status==='APPROVED','nothing sent for a stale draft');
  edit(x=>{x.startDateTime=sw.start+' 00:00:00';});W.mutateError=new Error('socket hang up');
  await rejects(E.applyApproval(id,C()),/could not be confirmed/,'unconfirmed result');check(ap(id).status==='APPLY_UNKNOWN','an unconfirmed result is never retried automatically');}

 // New-customer acquisition goal.
 reset();
 await rejects(E.draftCustomerGoal({campaignId:'102',mode:'BID_HIGHER_FOR_NEW_CUSTOMER',value:10}),/needs Maximize conversion value or target ROAS bidding\. No draft was created/,'manual CPC cannot bid higher');
 await rejects(E.draftCustomerGoal({campaignId:'101',mode:'TARGET_ALL_EQUALLY'}),/Already off/,'already off');
 await rejects(E.draftCustomerGoal({campaignId:'104',mode:'TARGET_NEW_CUSTOMER'}),/Search, Performance Max, Shopping and Demand Gen/,'not for Display');
 await rejects(E.draftCustomerGoal({campaignId:'999',mode:'TARGET_NEW_CUSTOMER'}),/not in Google Ads/,'unknown campaign');
 const cg=await E.draftCustomerGoal({campaignId:'101',mode:'BID_HIGHER_FOR_NEW_CUSTOMER',value:'15'}),cdoc=ap(cg.approvalId);
 eq(cdoc.payload.lifecycleGoal.request,{operation:{create:{campaign:Cp(101),customerAcquisitionGoalSettings:{optimizationMode:'BID_HIGHER_FOR_NEW_CUSTOMER',valueSettings:{value:15}}}}},'the exact request Paul reviews');
 check(cdoc.status==='PENDING'&&cdoc.type==='acquisition'&&/Off \(new and returning customers alike\) → Bid higher for new customers \(\+CAD 15\.00 per new customer\)/.test(cdoc.summary)&&cdoc.payload.campaignOption.tradeoff===O.ACQUISITION_TRADEOFF,'summary: from and to, with the tradeoff');
 await rejects(E.draftCustomerGoal({campaignId:'101',mode:'TARGET_NEW_CUSTOMER'}),/already waiting/,'one draft per campaign');
 eq(ap((await E.draftCustomerGoal({campaignId:'103',mode:'TARGET_ALL_EQUALLY'})).approvalId).payload.lifecycleGoal.request,{operation:{update:{resourceName:'customers/123/campaignLifecycleGoals/103',customerAcquisitionGoalSettings:{optimizationMode:'TARGET_ALL_EQUALLY'}},updateMask:'customer_acquisition_goal_settings.optimization_mode,customer_acquisition_goal_settings.value_settings'}},'switching it off updates the existing goal');
 await E.markApprovalApproved(cg.approvalId);
 reply=(url,body)=>({status:200,data:body.validateOnly?{}:{result:{resourceName:'customers/123/campaignLifecycleGoals/101'}}});
 res=await E.applyApproval(cg.approvalId,C({dryRun:true}));
 check(res.status==='VALIDATED'&&net.length===1&&net[0].body.validateOnly===true&&ap(cg.approvalId).status==='APPROVED','dry run: validate only');
 res=await E.applyApproval(cg.approvalId,C());
 check(res.status==='APPLIED'&&net.length===2&&net[1].method==='POST'&&net[1].url==='https://googleads.googleapis.com/v24/customers/123/campaignLifecycleGoal:configureCampaignLifecycleGoals'&&!('validateOnly' in net[1].body)&&net[1].headers.Authorization==='Bearer test-token','published once to configureCampaignLifecycleGoals');
 eq(net[1].body.operation,cdoc.payload.lifecycleGoal.request.operation,'exactly the reviewed operation');
 check(ledgers.filter(l=>l.kind==='lifecycleGoal').length===2&&!sent.length,'both calls are in the ledger; nothing else was sent');
 reset();
 {const id=(await E.draftCustomerGoal({campaignId:'101',mode:'TARGET_NEW_CUSTOMER'})).approvalId;await E.markApprovalApproved(id);
  reply=()=>({status:400,data:{error:{code:400,message:'Request contains an invalid argument.',status:'INVALID_ARGUMENT',details:[{'@type':'type.googleapis.com/google.ads.googleads.v24.errors.GoogleAdsFailure',errors:[{errorCode:{campaignLifecycleGoalError:'CUSTOMER_ACQUISITION_MISSING_EXISTING_CUSTOMER_DEFINITION'},message:'x'}]}]}}});
  await rejects(E.applyApproval(id,C()),e=>e.message===O.ACQUISITION_PREREQUISITE+' Nothing was changed.','Google\'s prerequisite refusal');
  check(ap(id).status==='APPROVED'&&ap(id).lastError===O.ACQUISITION_PREREQUISITE+' Nothing was changed.','refused until existing customers are defined: nothing changed, the reason shown');
  W.lifecycle[101]={mode:'TARGET_NEW_CUSTOMER',value:null};const n0=net.length;
  await rejects(E.applyApproval(id,C()),/changed in Google Ads after the draft was made/,'changed in Google since');check(net.length===n0,'nothing sent for a stale draft');
  delete W.lifecycle[101];reply=()=>({status:503,data:{error:{message:'Service unavailable'}}});
  await rejects(E.applyApproval(id,C()),/could not be confirmed/,'unconfirmed result');check(ap(id).status==='APPLY_UNKNOWN','never retried automatically');}

 // Campaign total budgets in every ceiling check.
 reset();W.camps[201]={name:'BA · holiday-flight',status:'ENABLED',serving:'SERVING',channel:'SEARCH',bidding:'MANUAL_CPC',res:B(21),total:300,start:day(-5),end:day(9)};W.spend[201]=60;
 let eb=await get('_enabledBudgets')();
 check(Math.abs(eb.budgets.get(B(21))-24)<1e-9&&Math.abs(eb.total-84)<1e-9,'what is left of a total budget (240) over its 10 remaining days counts 24 a day; ended campaigns excluded');
 check(queries.some(q=>q.includes(`metrics.cost_micros FROM campaign WHERE campaign.id IN (201) AND segments.date BETWEEN '${day(-5)}' AND '${today}'`)),'spend so far read over the flight');
 W.fail=[/metrics\.cost_micros FROM campaign WHERE campaign\.id IN/];eb=await get('_enabledBudgets')();W.fail=[];
 check(Math.abs(eb.budgets.get(B(21))-30)<1e-9,'unreadable spend: the whole total over the days left, never less');
 // The dashboard and reports read budgets without dates: a total budget shows per day as the ceiling counts it.
 {const perDay=get('_campaignBudgetDaily'),base=[101,201].map(id=>{const r=budgetRow(id);return {campaign:{id:r.campaign.id,name:r.campaign.name},campaignBudget:r.campaignBudget};});
  queries.length=0;let of=await perDay([base[0]]);check(of(base[0])===20&&!queries.length,'daily budgets: shown as before, nothing more read');
  of=await perDay(base);check(of(base[0])===20&&Math.abs(of(base[1])-24)<1e-9,'a total budget shows what is left per remaining day (24), as the ceiling counts it');
  W.fail=[/campaign\.start_date_time/];of=await perDay(base);W.fail=[];check(of(base[1])===300,'its dates unreadable: its whole total, never less');
  const src=fs.readFileSync(dir+'googleAdsAutopilot.js','utf8');
  check((src.match(/campaign_budget\.amount_micros, campaign_budget\.period, campaign_budget\.total_amount_micros FROM campaign(`| WHERE campaign\.status != 'REMOVED'`)/g)||[]).length===2&&/campaign_budget\.amount_micros,\s+campaign_budget\.period, campaign_budget\.total_amount_micros,\s+campaign_budget\.recommended_budget_amount_micros/.test(src)
   &&(src.match(/budget: budgetOf\(r\)/g)||[]).length===3,'the dashboard, the report and Ad Doctor read total budgets and show them per day');}
 await rejects(E.setCampaignBudget('201',50,{ctrl:C()}),/total budget for its fixed dates/,'a total budget is never rewritten as a daily one');check(!sent.length,'nothing sent');
 W.camps[201].status='PAUSED';W.ctrl={maxDailyBudgetTotal:80};
 await rejects(E.setCampaignStatus('201','ENABLED',{ctrl:C()}),/CAD 84\.00, over your daily ceiling of CAD 80\.00/,'enabling counts the total per remaining day');
 W.ctrl={maxDailyBudgetTotal:90};await E.setCampaignStatus('201','ENABLED',{ctrl:C()});check(sent.length===1&&sent[0].ops[0].update.status==='ENABLED','within the ceiling: enabled');
 W.camps[201].status='ENABLED';sent.length=0;
 await rejects(E.setCampaignEndDate('201',{endDate:day(3),ctrl:C()}),/CAD 120\.00, over your daily ceiling of CAD 90\.00/,'an earlier end date spends the same total over fewer days');check(!sent.length,'nothing sent');
 // A first enable that starts a planned run moves the end date in the same update: the total counts over those dates.
 {W.camps[201].status='PAUSED';W.spend[201]=0;W.ctrl={maxDailyBudgetTotal:80};f.docs.set('Brites_GAds_State/plannedFlight_201',{days:20,publishedAt:Date.now()-86400000});
  const r=await E.setCampaignStatus('201','ENABLED',{ctrl:C()}),u=sent[0]&&sent[0].ops[0];
  check(r.endDate===day(19)&&sent.length===1&&u.updateMask==='status,end_date_time'&&u.update.status==='ENABLED','a planned 20-day run: 300 counts 15 a day (75 of 80), not 30 over the old end date');
  f.docs.delete('Brites_GAds_State/plannedFlight_201');W.spend[201]=60;W.camps[201].status='ENABLED';W.ctrl={maxDailyBudgetTotal:90};sent.length=0;}
 W.ctrl={maxDailyBudgetTotal:70};const t=await E.enforceBudgetCeiling({ctrl:C()});
 const after={[B(11)]:20,[B(12)]:10,[B(13)]:30};sent[0].ops.forEach(o=>{after[o.update.resourceName]=o.update.amountMicros/1e6;});
 check(t.total===84&&sent.length===1&&sent[0].ops.every(o=>o.update.resourceName!==B(21))&&Object.values(after).reduce((a,v)=>a+v,0)+24<=70.001,'the trim lowers daily budgets only; the total budget counts and is never rewritten');
 // A draft with a total budget counts its total over its days at publish.
 const flight=total=>[{campaignBudgetOperation:{create:{resourceName:B(-1),name:'BA · flight · 1',deliveryMethod:'STANDARD',explicitlyShared:false,period:'CUSTOM_PERIOD',totalAmountMicros:total*1e6}}},{campaignOperation:{create:{resourceName:Cp(-2),name:'BA · flight',status:'PAUSED',advertisingChannelType:'SEARCH',campaignBudget:B(-1),manualCpc:{enhancedCpcEnabled:false},startDateTime:compact(day(1))+' 00:00:00',endDateTime:compact(day(10))+' 23:59:59'}}}];
 const put=(id,data)=>f.docs.set('Brites_GAds_Approvals/'+id,clone({vetted:false,createdAt:1,summary:'x',...data}));
 reset();put('big',{type:'keywords',status:'APPROVED',payload:{mutateOperations:flight(500)}});
 await rejects(E.applyApproval('big',C()),/these budgets total CAD 50\.00, but only CAD 40\.00 of your CAD 100\.00 ceiling is free/,'500 over 10 days counts 50 a day: over the ceiling');check(!sent.length,'nothing sent');
 put('fits',{type:'keywords',status:'APPROVED',payload:{mutateOperations:flight(300)}});res=await E.applyApproval('fits',C());
 check(res.status==='APPLIED'&&sent[1].ops[0].campaignBudgetOperation.create.totalAmountMicros===300e6&&sent[1].ops.some(o=>o.campaignConversionGoalOperation),'30 a day fits: published with its total budget and purchase-only goals');
 check(sent[1].ops.find(o=>o.campaignOperation).campaignOperation.create.endDateTime===compact(day(10))+' 23:59:59','a draft saved in the older layout goes out exactly as reviewed (Google still accepts it): nothing is rewritten after approval');
 // A planned run moves the end date at publication: the total counts over the planned days from today.
 {const stale=flight(300),sc=stale[1].campaignOperation.create;sc.startDateTime=compact(day(-4))+' 00:00:00';sc.endDateTime=compact(day(5))+' 23:59:59';
  put('run',{type:'keywords',status:'APPROVED',payload:{mutateOperations:stale,meta:{plannedDays:{[Cp(-2)]:10}}}});sent.length=0;res=await E.applyApproval('run',C());
  const pc=sent[1].ops.find(o=>o.campaignOperation).campaignOperation.create;
  check(res.status==='APPLIED'&&get('_dateOnly')(pc.endDateTime)===day(9),'published with the end date 10 days from today: 300 counts 30 a day (fits), not 50 over the stale dates');}
 reset();W.brandLists=[['103','customers/123/sharedSets/55']];W.sharedSets=['customers/123/sharedSets/55'];
 put('px',{type:'keywords',status:'APPROVED',payload:{mutateOperations:[{campaignBudgetOperation:{create:{resourceName:B(-1),name:'BA · px · 1',amountMicros:5e6,deliveryMethod:'STANDARD',explicitlyShared:false}}},{campaignOperation:{create:{resourceName:Cp(-2),name:'BA · px',status:'PAUSED',advertisingChannelType:'PERFORMANCE_MAX',campaignBudget:B(-1),maximizeConversionValue:{}}}}]}});
 res=await E.applyApproval('px',C());
 {const px=sent[1].ops,brand=px.filter(o=>o.campaignCriterionOperation&&o.campaignCriterionOperation.create.brandList);
  check(res.status==='APPLIED'&&brand.length===1&&brand[0].campaignCriterionOperation.create.campaign===Cp(-2)&&brand[0].campaignCriterionOperation.create.negative===true&&px.filter(o=>o.campaignConversionGoalOperation).length===4,'a new Performance Max campaign: purchase-only goals and the account\'s brand exclusion in the same request');
  check(clone(ap('px').brandExclusions.lists).join()==='customers/123/sharedSets/55'&&ap('px').purchaseGoals.campaigns===1,'both recorded on the approval');}
 // The approval-card switch: same spend, saved like the other draft settings, no new paid production.
 reset();
 const dated=(daily,{end=day(10),type='SEARCH'}={})=>[{campaignBudgetOperation:{create:{resourceName:B(-1),name:'BA · rings · 1',amountMicros:daily*1e6,deliveryMethod:'STANDARD',explicitlyShared:false}}},{campaignOperation:{create:{resourceName:Cp(-2),name:'BA · rings',status:'PAUSED',advertisingChannelType:type,campaignBudget:B(-1),...(type==='SEARCH'?{manualCpc:{enhancedCpcEnabled:false}}:{maximizeConversionValue:{}}),startDateTime:compact(day(1))+' 00:00:00',endDateTime:compact(end)+' 23:59:59'}}},{adGroupAdOperation:{create:{adGroup:'customers/123/adGroups/-3',ad:{finalUrls:['https://britesjewelry.com/collections/rings'],responsiveSearchAd:{headlines:[{text:'Rings'}],descriptions:[{text:'Made to order.'}]}}}}}];
 const H=v=>E.creativeHash(v),p0={meta:{budgetCurrency:'CAD'},mutateOperations:dated(12)};
 put('t1',{type:'creative',status:'PENDING',payload:p0,creative:{schema:1,phase:'ready',payloadHash:H(p0),sourceHash:H(p0),groups:[{ref:'g1'}],review:{at:1,payloadHash:H(p0)}}});
 let tb=await E.setApprovalTotalBudget({id:'t1',on:true}),t1=ap('t1'),nb=t1.payload.mutateOperations[0].campaignBudgetOperation.create;
 check(tb.on&&tb.total===120&&tb.days===10&&nb.period==='CUSTOM_PERIOD'&&nb.totalAmountMicros===120e6&&!('amountMicros' in nb)&&t1.payload.totalBudget.daily===12,'12 a day for 10 days becomes a total of 120');
 check(t1.creative.review===null&&t1.creative.payloadHash===H(t1.payload)&&t1.creative.sourceHash===H(t1.payload)&&t1.creative.groups.length===1,'the finished creative stays valid (no new paid production); only its approval is cleared');
 tb=await E.setApprovalTotalBudget({id:'t1',on:false});t1=ap('t1');
 check(!tb.on&&tb.daily===12&&t1.payload.mutateOperations[0].campaignBudgetOperation.create.amountMicros===12e6&&!t1.payload.totalBudget,'and back to 12 a day');
 put('pm',{type:'pmax',status:'PENDING',payload:{mutateOperations:dated(12,{type:'PERFORMANCE_MAX'})}});check((await E.setApprovalTotalBudget({id:'pm',on:true})).total===120,'Performance Max drafts too');
 put('st',{type:'studio',status:'PENDING',payload:{designStudioSpec:{},mutateOperations:dated(12)}});await rejects(E.setApprovalTotalBudget({id:'st',on:true}),/keeps a daily budget/,'Design Studio builds its budget at publish');
 put('ok',{type:'creative',status:'APPROVED',payload:{mutateOperations:dated(12)}});await rejects(E.setApprovalTotalBudget({id:'ok',on:true}),/Only a pending draft/,'approved drafts are not changed');
 put('dp',{type:'creative',status:'PENDING',payload:{mutateOperations:dated(12,{type:'DISPLAY'})}});await rejects(E.setApprovalTotalBudget({id:'dp',on:true}),/not for this campaign type/,'not for Display');
 put('lg',{type:'creative',status:'PENDING',payload:{mutateOperations:dated(12,{end:day(100)})}});await rejects(E.setApprovalTotalBudget({id:'lg',on:true}),/up to 90 days/,'Google\'s 90-day limit');
 await rejects(E.setApprovalTotalBudget({id:'../x',on:true}),/Choose a draft/,'draft id checked');
 check(!sent.length&&!net.length,'switching the budget type sends nothing to Google');

 // Date-times reach Google in one layout, "yyyy-MM-dd HH:mm:ss", whichever way a date arrived or was saved (drafts saved before this hold
 // "yyyyMMdd HH:mm:ss"). Every reader takes both layouts, and every comparison is between values in one layout.
 reset();
 {const RX=/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/,W8=get('_toGAdsDateTime'),FULL=day(3)+' 23:59:59';
  eq([W8(day(3),'23:59:59'),W8(compact(day(3)),'23:59:59'),W8(compact(day(3))+' 23:59:59','00:00:00'),W8(FULL,'00:00:00'),W8(' '+day(3)+' ','23:59:59')],[FULL,FULL,FULL,FULL,FULL],'the campaign schedule writer takes a date or a date-time in either layout and writes "yyyy-MM-dd HH:mm:ss"');
  check(W8(null,'23:59:59')===null&&W8('','23:59:59')===null&&W8('soon','23:59:59')===null&&W8(FULL+'+00:00')===FULL+'+00:00','no date, no value; a time in a layout it does not know passes through unchanged');
  const clock=get('_accountDateTime')(tz,0);
  check(RX.test(clock)&&W8(clock)===clock&&get('_acctDateYmd')(tz,0)>=clock.slice(0,10)&&clock.slice(0,10)>=today,'the account clock is in the same layout, so writing it changes nothing, and its date is the account\'s today');
  eq(get('_campaignScheduleFields')(day(3),day(10)),{startDateTime:day(3)+' 00:00:00',endDateTime:day(10)+' 23:59:59'},'a built campaign\'s schedule');
  check(['2026-11-06 23:59:59','20261106 23:59:59','2026-11-06','20261106'].every(v=>get('_dateOnly')(v)==='2026-11-06'&&O._dates.dateOnly(v)==='2026-11-06'),'the engine and the options module read the same date from every layout');

  // "Start now", "Run longer" and a draft's flight dates each send a date-time of their own.
  const started=await E.startCampaignNow('101',{ctrl:C()}),su=sent[0].ops[0];
  check(sent.length===1&&sent[0].service==='campaigns'&&su.updateMask==='start_date_time'&&RX.test(su.update.startDateTime),'"Start now" sends the account clock as "yyyy-MM-dd HH:mm:ss"');
  check(started.startDateTime===su.update.startDateTime&&started.startDate===su.update.startDateTime.slice(0,10)&&started.startDate>=today,'and reports that instant and its date');
  sent.length=0;await E.setCampaignEndDate('101',{endDate:day(30),ctrl:C()});
  const eu=sent[0].ops[0];
  check(sent.length===1&&eu.updateMask==='end_date_time'&&eu.update.endDateTime===day(30)+' 23:59:59','"Run longer" sends the end as the last second of its day, in the same layout');
  put('sd',{type:'creative',status:'PENDING',payload:{mutateOperations:dated(12)}}); // its flight is saved in the older layout
  await E.setApprovalDates('sd',day(2),day(12));
  const dc=ap('sd').payload.mutateOperations.find(o=>o.campaignOperation).campaignOperation.create;
  eq([dc.startDateTime,dc.endDateTime],[day(2)+' 00:00:00',day(12)+' 23:59:59'],'editing a draft\'s dates rewrites its flight as "yyyy-MM-dd HH:mm:ss"');
  check(!net.length,'none of them reached a live service');}

 // A sale-day adjustment whose (exclusive) end is midnight today is over: dates are compared with dates and date-times with date-times.
 reset();W.history=history;
 {const adj=(name,from,to,x={})=>({name,scope:'CAMPAIGN',campaigns:[Cp(101)],startDateTime:from+' 00:00:00',endDateTime:to+' 00:00:00',conversionRateModifier:1.2,...x});
  W.adjustments=[adj('Ended at midnight',day(-3),today),adj('Covers today',day(-2),day(1)),adj('Ahead',day(5),day(8))];
  const open=await get('_seasonalityAdjustments')(today);
  eq(open.map(a=>[a.name,a.start,a.endExclusive]),[['Covers today',day(-2)+' 00:00:00',day(1)+' 00:00:00'],['Ahead',day(5)+' 00:00:00',day(8)+' 00:00:00']],'one that ended at midnight today is over; one that covers today and one ahead are not');
  const panel=await E.campaignOptionsStatus();
  eq(panel.sale.upcoming.map(a=>[a.name,a.start,a.end]),[['Covers today',day(-2),today],['Ahead',day(5),day(7)]],'the panel shows each by its first and last day, and leaves out the one that ended');
  // The same windows read in the older layout come out the same, and still clash with a new draft.
  const older=a=>({...a,startDateTime:compact(a.startDateTime.slice(0,10))+a.startDateTime.slice(10),endDateTime:compact(a.endDateTime.slice(0,10))+a.endDateTime.slice(10)});
  W.adjustments=W.adjustments.map(older);
  eq(await get('_seasonalityAdjustments')(today),open,'windows read in the older layout come out the same');
  W.adjustments=[older(adj('Google Black Friday',sw.start,addDays(sw.start,2),{campaigns:[Cp(103)]}))];
  await rejects(sale({campaignIds:['101','103']}),/already has the sale-day adjustment “Google Black Friday”/,'an adjustment read in the older layout still clashes with the dates it covers');
  W.adjustments=[adj('Right before',addDays(sw.start,-3),sw.start,{campaigns:[Cp(103)]})];
  check((await sale({campaignIds:['101','103']})).ok,'and one that ends the moment the sale starts does not');}
}

/* ---------------- the router ---------------- */
async function routerChecks(){
 const calls=[],FE={control:async()=>({enabled:true,dryRun:false}),campaignOptionsStatus:async()=>{calls.push(['campaignOptionsStatus']);return {ok:true};},
   draftBrandSearch:async a=>{calls.push(['draftBrandSearch',a]);return {ok:true};},draftSeasonalityAdjustment:async a=>{calls.push(['draftSeasonalityAdjustment',a]);return {ok:true};},
   draftCustomerGoal:async a=>{calls.push(['draftCustomerGoal',a]);return {ok:true};},setApprovalTotalBudget:async a=>{calls.push(['setApprovalTotalBudget',a]);throw Error('Only a pending draft can change its budget type.');}};
 const m={exports:{}},kc={process:{env:{}},console,Date,Set,JSON,module:m,exports:m.exports,require:n=>n==='node-fetch'?async()=>{throw Error('Live network forbidden');}:n==='./googleAdsAutopilot'?FE:n==='./_editPasscode'?{sameSecret:()=>false}:n==='./firebaseAdmin'?(()=>{throw Error('no Firestore here');})():realRequire(n)};
 vm.createContext(kc);vm.runInContext(fs.readFileSync(dir+'googleAdsAutopilotKick.js','utf8'),kc);
 const reads=vm.runInContext('READ_ACTIONS',kc),isRead=vm.runInContext('isReadAction',kc);
 check(reads.has('campaignOptions')&&['draftBrandSearch','draftSeasonalityAdjustment','draftCustomerGoal','setApprovalTotalBudget'].every(a=>!isRead(a,{})),'the panel is a read; every draft and the budget-type switch is a change behind the passcode');
 const H=m.exports.handleAction;
 check((await H({action:'campaignOptions'})).ok&&calls[0][0]==='campaignOptionsStatus','panel route');
 await H({action:'draftBrandSearch',dailyBudget:'7',maxCpc:'0.6',status:'ENABLED'});eq(calls[1],['draftBrandSearch',{dailyBudget:'7',maxCpc:'0.6'}],'only the brand fields pass');
 await H({action:'draftSeasonalityAdjustment',occasion:'bfcm',startDate:'2026-11-27',endDate:'2026-11-30',changePct:'20',campaignIds:['101']});
 eq(calls[2],['draftSeasonalityAdjustment',{occasion:'bfcm',startDate:'2026-11-27',endDate:'2026-11-30',changePct:'20',campaignIds:['101']}],'sale-day route');
 await H({action:'draftCustomerGoal',campaignId:'101',mode:'TARGET_NEW_CUSTOMER',value:null});eq(calls[3],['draftCustomerGoal',{campaignId:'101',mode:'TARGET_NEW_CUSTOMER',value:null}],'new-customer route');
 const r=await H({action:'setApprovalTotalBudget',id:'a1',on:'true'});
 check(r.ok===false&&/Only a pending draft/.test(r.error)&&calls[4][1].on===false,'"on" must be exactly true; a refusal comes back as a message');
 const entries=JSON.parse(fs.readFileSync(path.join(ROOT,'scripts/netlify-function-entries.json'),'utf8'));
 check(entries.modules.includes('_googleAdsCampaignOptions.js')&&!entries.endpoints.includes('_googleAdsCampaignOptions.js'),'the helper module is bundled with the functions');
}

/* ---------------- the console ---------------- */
async function uiChecks(){
 const html=fs.readFileSync(path.join(ROOT,'brites-adwords.html'),'utf8');
 const inline=[...html.matchAll(/<script(?![^>]*\bsrc=)[^>]*>([\s\S]*?)<\/script>/gi)].map(x=>x[1]);
 inline.forEach((s,i)=>new vm.Script(s,{filename:'brites-adwords.html#inline'+i}));check(inline.length===2,'both inline scripts compile');
 check(html.includes('<details class="oppToolbox" id="campaignOptions">')&&/#v-bench:not\(\[data-lane="search"\]\):not\(\[data-lane="product"\]\) #campaignOptions\{display:none!important\}/.test(html),'Campaign options sits in Opportunities (Search and product lanes)');
 check(/Object\.assign\(LOADINFO,\{campaignOptions:/.test(html)&&/\["draftBrandSearch","draftSeasonalityAdjustment","draftCustomerGoal","setApprovalTotalBudget"\]\.forEach\(function\(a\)\{API_MUTATING\.add\(a\);\}\)/.test(html),'every wait is labelled; drafts refresh the console');
 check(/function requiresCreative\(a\)\{if\(isAdVersionApproval\(a\)\|\|a\.type==="brand"\)return false;/.test(html),'the brand draft card needs no creative review');
 const from=(a,b)=>{const i=html.indexOf(a),j=html.indexOf(b,i);assert(i>=0&&j>i,'source slice '+a);return html.slice(i,j);};
 const src=from('function parseCreative(','// One-line summary of the ad extensions')+'\n'+from('/* ---- Campaign options (Opportunities)','\nsetupGrowthLanes();');
 const dom=new JSDOM('<body><details id="campaignOptions"><div id="coBody"></div></details></body>'),d=dom.window.document,pending=[],apiCalls=[];
 const c={document:d,
   esc:s=>String(s==null?'':s).replace(/[&<>"]/g,ch=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[ch])),adAttr:s=>String(s==null?'':s).replace(/&/g,'&amp;').replace(/"/g,'&quot;'),
   apCountries:ids=>ids.join(', '),apFacts:rows=>rows.filter(x=>x&&x[1]).map(x=>'<div class="fact"><b>'+x[0]+'</b> '+x[1]+'</div>').join(''),apMoney:n=>'CAD '+Number(n).toFixed(2),apDay:v=>v,apCampaignName:x=>'Campaign '+String(x).split('/').pop(),
   btnBusy:b=>{b.disabled=true;return()=>{b.disabled=false;};},actStart:k=>k,actEnd:()=>{},toast:()=>{},reload:async()=>{},
   api:(action,body)=>{apiCalls.push([action,clone(body)]);return new Promise(res=>pending.push(res));}};
 vm.createContext(c);vm.runInContext(src,c);
 const body=()=>d.getElementById('coBody'),q=s=>body().querySelector(s),settle=()=>new Promise(res=>setImmediate(res));
 const status={ok:true,today:'2026-09-29',currency:'CAD',warnings:[],
   brand:{campaign:null,draft:null,defaults:{dailyBudget:5,maxCpc:0.5},limits:{dailyBudget:[1,50],maxCpc:[0.05,5]},countries:['2124','2840'],pairing:O.BRAND_SEARCH.pairing,keywords:['[brites]','"brites"','[brites jewelry]','"brites jewelry"'],ceiling:100},
   sale:{limits:{pct:[-50,150],maxDays:14},upcoming:[],drafts:[],occasions:[
     {key:'bfcm',label:'Black Friday / Cyber Monday',start:'2026-11-27',end:'2026-11-30',days:4,peak:'2026-11-30',estimate:{pct:25,measured:false,basis:'Starting estimate: too little history.'},campaigns:[{id:'101',name:'Rings',status:'ENABLED'},{id:'104',name:'Display',status:'PAUSED'}]},
     {key:'valentines',label:"Valentine's Day",start:'2027-02-07',end:'2027-02-13',days:7,peak:'2027-02-14',estimate:{pct:15,measured:false,basis:'Starting estimate.'},campaigns:[]}]},
   customers:{modes:O.ACQUISITION_MODES,tradeoff:O.ACQUISITION_TRADEOFF,prerequisite:O.ACQUISITION_PREREQUISITE,drafts:[],
     campaigns:[{id:'101',name:'Rings',status:'ENABLED',channel:'SEARCH',mode:'TARGET_ALL_EQUALLY',value:null,allowed:{TARGET_ALL_EQUALLY:'Already off for this campaign.',BID_HIGHER_FOR_NEW_CUSTOMER:true,TARGET_NEW_CUSTOMER:true}}]}};
 let load=c.coLoad();
 check(/Checking campaigns and goals/.test(body().textContent)&&q('.spin'),'a small spinner says what is loading');
 pending.shift()(clone(status));await load;
 check(body().querySelectorAll('.coOpt').length===3&&q('[data-cof="bBudget"]').value==='5'&&q('[data-cof="bCpc"]').value==='0.5','three plain options; the brand defaults');
 check(q('[data-cocamp][value="101"]').checked&&!q('[data-cocamp][value="104"]').checked&&q('[data-cof="sPct"]').value==='25'&&/Starting estimate/.test(body().textContent),'sale days: enabled smart-bidding campaigns preselected, the estimate and its basis shown');
 const goalBtn=()=>q('[data-co="customers"]');
 check(goalBtn().disabled&&/Now: Off \(new and returning customers alike\) · Already off for this campaign\./.test(body().textContent)&&body().textContent.includes(O.ACQUISITION_TRADEOFF),'new-customer goal: off, the tradeoff in one line');
 const mode=q('[data-cof="cMode"]');mode.value='BID_HIGHER_FOR_NEW_CUSTOMER';mode.onchange();
 const v=q('[data-cof="cValue"]');check(v&&!goalBtn().disabled,'bidding higher asks for the extra value');
 v.value='12';v.oninput();let clicked=goalBtn().onclick();
 eq(apiCalls[apiCalls.length-1],['draftCustomerGoal',{campaignId:'101',mode:'BID_HIGHER_FOR_NEW_CUSTOMER',value:'12'}],'the typed value is sent');
 check(goalBtn().disabled,'the button is busy while drafting');
 pending.shift()({ok:false,error:O.ACQUISITION_PREREQUISITE});await clicked;
 check(q('.coMsg.bad')&&q('.coMsg.bad').textContent===O.ACQUISITION_PREREQUISITE&&q('[data-cof="cValue"]').value==='12'&&!goalBtn().disabled,'a refusal is shown in place and the typed value is kept');
 const bb=q('[data-cof="bBudget"]');bb.value='7';bb.oninput();clicked=q('[data-co="brand"]').onclick();
 eq(apiCalls[apiCalls.length-1],['draftBrandSearch',{dailyBudget:'7',maxCpc:0.5}],'the brand draft sends the typed budget');
 pending.shift()({ok:true,approvalId:'b1'});await settle();
 eq(apiCalls[apiCalls.length-1],['campaignOptions',undefined],'then the options reload');
 pending.shift()({...clone(status),brand:{...clone(status.brand),draft:{id:'b1',status:'PENDING'}}});await clicked;
 check(/A brand Search draft is waiting in Approvals/.test(body().textContent)&&/Draft created\. Review and approve it in Approvals/.test(body().textContent),'the draft is confirmed and waits in Approvals');

 // Approval cards: the total-budget switch, what the option does, and the purchase-only goals note.
 const lt=new Date(),L=n=>{const x=new Date(lt.getFullYear(),lt.getMonth(),lt.getDate()+n);return x.getFullYear()+String(x.getMonth()+1).padStart(2,'0')+String(x.getDate()).padStart(2,'0');};
 const uiOps=(bud,type='SEARCH')=>[{campaignBudgetOperation:{create:{resourceName:B(-1),...bud}}},{campaignOperation:{create:{resourceName:Cp(-2),advertisingChannelType:type,campaignBudget:B(-1),manualCpc:{},startDateTime:L(1)+' 00:00:00',endDateTime:L(10)+' 23:59:59'}}}];
 const pend=(ops,x={})=>({id:'a1',status:'PENDING',type:'creative',payload:{meta:{budgetCurrency:'CAD'},mutateOperations:ops},...x});
 let h=c.campaignOptionApprovalHtml(pend(uiOps({amountMicros:12e6})));
 check(/Use a total budget/.test(h)&&/a total budget of CAD 120\.00 covers these 10 days with the same spend/.test(h)&&/Bids for purchases only/.test(h),'a dated Search draft offers a total budget (same spend) and says it bids for purchases only');
 h=c.campaignOptionApprovalHtml(pend(uiOps({period:'CUSTOM_PERIOD',totalAmountMicros:120e6})));
 check(/Total CAD 120\.00 for 10 days/.test(h)&&/Counts as CAD 12\.00 a day toward the daily ceiling/.test(h)&&/Use a daily budget/.test(h),'a total budget shows what the ceiling counts');
 const pmOps=[{campaignBudgetOperation:{create:{resourceName:B(-1),amountMicros:5e6}}},{campaignOperation:{create:{resourceName:Cp(-2),advertisingChannelType:'PERFORMANCE_MAX',campaignBudget:B(-1),maximizeConversionValue:{}}}}];
 check(/Brand exclusions on your other Performance Max campaigns apply to it too/.test(c.campaignOptionApprovalHtml(pend(pmOps,{type:'pmax'})))&&/Brand exclusions on your other/.test(c.campaignOptionApprovalHtml({id:'d',status:'PENDING',type:'studio',payload:{designStudioSpec:{},meta:{kind:'designStudioPmax'}}}))&&!/Brand exclusions/.test(c.campaignOptionApprovalHtml(pend(uiOps({amountMicros:12e6})))),'Performance Max cards say the account\'s brand exclusions apply; Search cards do not');
 const pc=c.parseCreative(uiOps({period:'CUSTOM_PERIOD',totalAmountMicros:120e6}));check(pc.totalBudget===120&&pc.budget===12,'the card reads a total budget as its daily share');
 check(![pend(uiOps({amountMicros:12e6},'DISPLAY')),pend(uiOps({amountMicros:12e6}),{type:'studio'}),pend(uiOps({amountMicros:12e6}),{status:'APPROVED'})].some(a=>/Budget type/.test(c.campaignOptionApprovalHtml(a))),'no switch for Display, Design Studio or a draft already approved');
 h=c.campaignOptionApprovalHtml({id:'s1',status:'PENDING',type:'seasonality',payload:{seasonalityAdjustment:{operation:{create:{}}},campaignOption:{kind:'seasonality',label:'Black Friday / Cyber Monday',start:'2026-11-27',end:'2026-11-30',days:4,changePct:25,estimate:{basis:'Starting estimate: too little history.'},campaigns:[{id:'101',name:'Rings'}]}}});
 check(/Smart bidding in 1 campaign/.test(h)&&/\+25% \(an estimate\)/.test(h)&&/Bidding returns to normal on its own/.test(h)&&!/Budget type|purchases only/.test(h),'sale-day card: campaigns, dates, the expected change as an estimate');
 h=c.campaignOptionApprovalHtml({id:'c1',status:'PENDING',type:'acquisition',payload:{meta:{budgetCurrency:'CAD'},lifecycleGoal:{campaignId:'101',campaignName:'Rings',mode:'BID_HIGHER_FOR_NEW_CUSTOMER',value:15,previous:null},campaignOption:{kind:'acquisition',tradeoff:O.ACQUISITION_TRADEOFF,prerequisite:O.ACQUISITION_PREREQUISITE}}});
 check(/Off → Bid higher for new customers/.test(h)&&/Extra value per new customer: CAD 15\.00/.test(h)&&h.includes(c.esc(O.ACQUISITION_TRADEOFF))&&h.includes(c.esc(O.ACQUISITION_PREREQUISITE)),'new-customer card: from and to, the value, the tradeoff and what Google needs');
 const brandOps=O.buildBrandSearchOps({cid:'123',dailyBudget:5,maxCpc:0.5,countries:['2124']}).ops;
 h=c.campaignOptionApprovalHtml(pend(brandOps,{type:'brand',payload:{mutateOperations:brandOps,campaignOption:{kind:'brand',pairing:O.BRAND_SEARCH.pairing}}}));
 check(h.includes(O.BRAND_SEARCH.pairing)&&/Bids for purchases only/.test(h)&&!/Budget type/.test(h),'brand card: the Performance Max pairing; no dates, so no total budget');
}
