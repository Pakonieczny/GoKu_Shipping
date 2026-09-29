// Opportunity research: occasion dates, Keyword Planner demand, forecast maths, ranking and the scan's
// date/eligibility gates. Offline: every Google, Shopify, Firestore and AI dependency is a fake.
const fs=require('fs'),vm=require('vm'),assert=require('assert'),path=require('path');
const file=process.env.RESEARCH_ENGINE||path.resolve(__dirname,'../../netlify/functions/googleAdsAutopilot.js');
const source=fs.readFileSync(file,'utf8');
const NOW=Date.parse('2026-09-29T15:00:00Z');
class FixedDate extends Date{constructor(...a){if(a.length)super(...a);else super(NOW);}static now(){return NOW;}}
function engine(fns={}){
 const sandbox={module:{exports:{}},process:{env:{GADS_CURRENCY:'USD',KP_BACKOFF_MS:'0'}},URL,URLSearchParams,Intl,Date:FixedDate,console,Buffer,setTimeout,clearTimeout,AbortController,
  require:name=>name==='node-fetch'?(()=>{throw Error('Unexpected network');}):require('module').createRequire(file)(name),fns};
 vm.createContext(sandbox);
 vm.runInContext(source+'\nmodule.exports.test={rule:_occasionRule,peak:_nextOccasionPeak,monthEnd:_kpMonthEnd,windowSearches:_windowSearches,plan:planCampaign,score:_oppScore,cls:opportunityClass,kwKey:_kwCacheKey,research:researchOpportunity};',sandbox);
 vm.runInContext('for(const k of Object.keys(fns))globalThis[k]=fns[k];',sandbox);
 return sandbox.module.exports;
}
const J=x=>JSON.stringify(x);
(async()=>{
 const t=engine().test;
 // ---- Occasion dates: moving observances, national variants, combined labels, no guesses ----
 assert.equal(t.peak("Mother's Day",'2026-01-01'),'2026-05-10');
 assert.equal(t.peak('Father’s Day','2026-01-01'),'2026-06-21');
 assert.equal(t.peak("World Teachers' Day thank-you gifts",'2026-09-29'),'2026-10-05','a bare "teacher" no longer maps to May');assert.equal(J(t.rule("World Teachers' Day",'2026-09-29').markets),'["CA","GB","US"]');
 assert.equal(t.peak("World Teachers' Day (Australia)",'2026-09-29'),'2026-10-30');assert.equal(t.peak("Australian World Teachers' Day",'2025-01-01'),'2025-10-31');assert.equal(J(t.rule("World Teachers' Day Australia",'2026-09-29').markets),'["AU"]');
 assert.equal(t.peak('Teacher Appreciation Week','2026-09-29'),'2027-05-07');
 assert.equal(t.peak('Canadian Thanksgiving','2026-09-29'),'2026-10-12');assert.equal(J(t.rule('Canadian Thanksgiving','2026-09-29').markets),'["CA"]');
 assert.equal(t.peak('Thanksgiving','2026-09-29'),'2026-11-26');assert.equal(J(t.rule('Thanksgiving','2026-09-29').markets),'["US"]');
 assert.equal(t.peak('Black Friday','2026-09-29'),'2026-11-27');assert.equal(t.peak('Cyber Monday deals','2026-09-29'),'2026-11-30');
 assert.equal(t.peak('Libra season, October birthdays and Halloween celestial style','2026-09-29'),'2026-10-31');
 assert.equal(t.peak('Christmas','2026-12-26'),'2027-12-25');assert.equal(t.peak('Christmas','2026-12-25'),'2026-12-25');
 assert.equal(t.peak('Nurses Week','2026-09-29'),'2027-05-12');
 assert.equal(t.peak('UK Mothering Sunday','2026-01-01'),null,'national variants need their own verified date');
 assert.equal(t.peak('National Daughters Day and Sons Day','2026-09-29'),null);assert.equal(t.peak('Evergreen gifting','2026-09-29'),null);
 assert.equal(t.rule('Back to school','2026-09-29').approx,true);
 // ---- Keyword Planner series: the last month is read, never assumed ----
 const months=['SEPTEMBER','OCTOBER','NOVEMBER','DECEMBER','JANUARY','FEBRUARY','MARCH','APRIL','MAY','JUNE','JULY','AUGUST'];
 const vols=months.map((m,i)=>({year:i<4?2025:2026,month:m,monthlySearches:'10'}));
 assert.equal(t.monthEnd(vols),'2026-08');assert.equal(t.monthEnd(vols.slice().reverse()),null);assert.equal(t.monthEnd(vols.slice(1)),null);
 assert.equal(t.monthEnd(vols.map((v,i)=>({year:v.year,month:(i+8)%12+2}))),'2026-08','numeric MonthOfYear enum');
 // Run-month demand uses last year's same month; plural variants count once.
 const monthly=[100,1200,100,100,100,100,100,100,100,100,100,100];
 const kw=[{text:'moon necklace gift',searches:200,monthly,monthlyEnd:'2026-08'},{text:'moon necklaces gift',searches:190}];
 const oct=t.windowSearches(kw,new Date(Date.UTC(2026,9,1)),new Date(Date.UTC(2026,9,31)));
 assert.equal(oct.monthly,1200);assert.equal(oct.seasonal,1);assert.equal(oct.keywords,1);
 assert.equal(t.windowSearches(kw).monthly,200);
 // Draft-path research keeps each measured series' end month, so plans read seasonality there too.
 const ro=await engine({keywordResearch:async()=>({ok:true,ideas:[{text:'Gold Moon Necklace',searches:200,competitionIndex:20,low:0.5,high:1.2,monthly,monthlyEnd:'2026-08'},{text:'moon charm',searches:90,monthly:null,monthlyEnd:null}]})}).test.research(['gold moon necklace'],['2124']);
 assert.equal(ro.keywords.find(k=>k.text==='Gold Moon Necklace').monthlyEnd,'2026-08');assert.equal(ro.keywords.find(k=>k.text==='moon charm').monthlyEnd,undefined);
 // ---- Cache keys cover every seed, the geography and the language ----
 const seeds=Array.from({length:30},(_,i)=>'moon necklace '+i);
 assert.notEqual(t.kwKey(seeds,'2124'),t.kwKey(seeds.concat('zzz'),'2124'));assert.notEqual(t.kwKey(seeds,'2124'),t.kwKey(seeds,'2840'));assert.notEqual(t.kwKey(seeds,'2124','1000'),t.kwKey(seeds,'2124','1002'));
 // ---- Forecast: a dated run ends ON the occasion; measured demand caps clicks and budget ----
 const research={ok:true,realCount:2,source:'google_keyword_planner',searchVolume:100,competitionIndex:40,cpc:{low:0.8,high:2.0},
  keywords:[{text:'moon necklace halloween',real:true,searches:60,low:0.8,high:2.0},{text:'gold star necklace gift',real:true,searches:40,low:0.7,high:1.8}]};
 const market={fit:1,fitWhy:'Moon motifs suit costume-season gifting.',demand:'rising',demandSource:'model'};
 const p=t.plan({title:'Celestial',occasion:'Halloween',peakDate:'2026-10-31',ceiling:100,headroom:80,research,aov:100,currency:'CAD',nativeToUsd:0.72,cvrInfo:{cvr:0.02,source:'benchmark'},market});
 assert.equal(p.duration.startDate,'2026-10-14');assert.equal(p.duration.endDate,'2026-10-31');assert.equal(p.duration.days,18,'inclusive: Google serves through the end date');
 assert.equal(p.cpc.target,1.26);assert.equal(p.model.maxClicksPerDay,0.16);assert.equal(p.expected.demandLimited,true);assert.equal(p.expected.windowSearches,100);
 assert.equal(p.budget.daily,2,'budget sized to the clicks demand supplies, never below one capped click');
 assert.equal(p.expected.clicksTotal,3);assert.equal(p.expected.conversions,0.06);assert.equal(p.expected.spendTotal,4);
 assert.equal(p.expected.revenue,8.33,'USD order value converted to the CAD account currency');
 assert(p.caveats.some(c=>/qualitative; it never changes these numbers/.test(c)));assert.equal(p.market.demandSource,'model','no measured seasonality: the model word stays labelled as opinion');
 assert(/Measured demand limits this/.test(p.budget.basis));
 // Measured seasonality replaces the model's demand word.
 const seasonal=JSON.parse(JSON.stringify(research));seasonal.keywords[0].monthly=monthly;seasonal.keywords[0].monthlyEnd='2026-08';
 const ps=t.plan({title:'Celestial',occasion:'Halloween',peakDate:'2026-10-31',ceiling:100,headroom:80,research:seasonal,aov:100,currency:'CAD',nativeToUsd:0.72,cvrInfo:{cvr:0.02},market:Object.assign({},market)});
 assert.equal(ps.market.demandSource,'measured');assert.equal(ps.market.demand,'rising');assert(ps.expected.windowSearches>1000);
 // Undated: 28 inclusive days from today. No Planner keywords: tier estimate in the account currency.
 const u=t.plan({title:'Celestial',occasion:'Evergreen gifting',ceiling:100,headroom:80,research:{ok:true,realCount:0,cpc:{low:0.77,high:1.72},keywords:[]},aov:100,currency:'CAD',nativeToUsd:0.72});
 assert.equal(u.duration.startDate,'2026-09-29');assert.equal(u.duration.endDate,'2026-10-26');assert.equal(u.duration.days,28);
 assert.equal(u.cpcSource,'estimate');assert.equal(u.cpc.max,1.46);assert.equal(u.model.maxClicksPerDay,null);assert(/No measured search volume/.test(u.budget.basis));
 // Competition-derived bands are never presented as Keyword Planner bids.
 const nb=t.plan({title:'Celestial',occasion:'Halloween',peakDate:'2026-10-31',ceiling:100,headroom:80,research:{ok:true,realCount:1,cpc:{low:0.9,high:2.0},competitionIndex:55,keywords:[{text:'moon necklace halloween',real:true,searches:500,low:null,high:null}]},aov:100,currency:'CAD',nativeToUsd:0.72});
 assert.equal(nb.cpcSource,'competition_estimate');assert(/No Keyword Planner bids/.test(nb.cpc.basis));
 // ---- Ranking: undated ideas get no urgency; zero-volume echoes are not evidence ----
 const base={confidence:{score:60},plan:{expected:{profit:10,profitLow:5}},research:{searchVolume:100,realCount:9},market:{fit:1},economics:{orders:0},daysOut:0,
  keywordData:[{real:true,searches:50},{real:true,searches:0},{real:true,searches:0}]};
 const undated=t.score(Object.assign({},base,{peakDate:null})),dated=t.score(Object.assign({},base,{peakDate:'2026-10-31'}));
 assert.equal(undated.rank,undated.score);assert(dated.rank>dated.score);
 assert.equal(t.cls(Object.assign({},base,{peakDate:null})),'controlled_experiment');assert.equal(t.cls(Object.assign({},base,{peakDate:'2026-10-31'})),'seasonal_high_confidence');
 // ---- Scan: dates are fixed before research, unverifiable or passed occasions never reach Paul ----
 const calls={prompt:null,opts:null,seeds:[]};
 const profile=(h,a,b)=>({handle:h,title:h,sampled:30,count:30,typesDetail:[{type:'Necklace',n:30}],motifs:[{t:a,n:20},{t:b,n:10}],mats:[{t:'gold',n:10}],personalization:['engraved'],topProducts:[{title:a+' necklace'}]});
 const kw4=tag=>['gold moon necklace '+tag,'engraved moon necklace '+tag,'gold star necklace '+tag,'engraved star necklace '+tag];
 const series=(peakIdx,peak)=>months.map((_,i)=>i===peakIdx?peak:peak/4);
 const idea=s=>({text:s,searches:s.includes('halloween')?1200:(s.includes('thanksgiving')?900:600),competition:'LOW',competitionIndex:30,low:0.6,high:1.4,monthly:series(1,s.includes('halloween')?3000:1200),monthlyEnd:'2026-08'});
 const hallo=['gold moon necklace halloween','gold moon necklaces halloween','engraved star necklace halloween','gold star necklace halloween','engraved moon necklace halloween','moon necklace halloween gift'];
 const proposals=[
  {collectionTitle:'celestial',occasion:'Halloween',peakDate:'2026-10-30',dateSource:'timeanddate.com',markets:['US','CA'],priority:'high',proven:true,market:{fitWhy:'Moon and star motifs.',demand:'rising',angle:'Glow all night'},rationale:'Costume season',keywords:hallo},
  {collectionTitle:'maple',occasion:'Canadian Thanksgiving',peakDate:'2026-10-12',dateSource:'canada.ca',markets:['CA','US'],priority:'medium',market:{},rationale:'Gratitude',keywords:['gold maple necklace thanksgiving','engraved maple necklace thanksgiving','gold leaf necklace thanksgiving','engraved leaf necklace thanksgiving']},
  {collectionTitle:'celestial',occasion:'National Daughters Day',peakDate:'2026-09-27',dateSource:'nationaltoday.com',priority:'high',market:{},rationale:'Passed',keywords:kw4('daughter')},
  {collectionTitle:'celestial',occasion:'World Animal Day',peakDate:'2026-10-04',dateSource:'worldanimalday.org.uk',priority:'test',market:{},rationale:'Too soon',keywords:kw4('animal lover')},
  {collectionTitle:'celestial',occasion:'Fire Prevention Week',peakDate:'2026-10-10',dateSource:'',priority:'test',market:{},rationale:'Unverified',keywords:kw4('firefighter')},
  {collectionTitle:'celestial',occasion:'Christmas',peakDate:'2026-12-25',dateSource:'calendar',priority:'test',market:{},rationale:'Too far',keywords:kw4('christmas')},
  {collectionTitle:'celestial',occasion:'Evergreen gifting',peakDate:null,dateSource:'',priority:'test',market:{},rationale:'Always on',keywords:['sparkle']}
 ];
 const fakes={fb:()=>null,
  control:async()=>({maxDailyBudgetTotal:100,budgetCurrency:'CAD',budgetCurrencyVerified:true,defaultCountries:['2036','2124','2826','2840'],smartBidding:false}),
  getCollections:async()=>[{handle:'celestial',title:'celestial'},{handle:'maple',title:'maple'}],fetchTopProducts:async()=>[],conversionHealth:async()=>({validated:true}),
  collectionProfiles:async()=>({list:[profile('celestial','moon','star'),profile('maple','maple','leaf')],at:NOW,salesBasis:'fixture'}),proposePmaxOpportunities:async()=>({list:[],at:NOW}),
  playbookSlice:async()=>null,playbookText:()=>'',_learningTrace:()=>null,
  storeSalesEvidence:async()=>({available:false,periods:{days30:null,days90:null},seasonality:{},merchant:{},warnings:[]}),
  _enabledBudgetTotal:async()=>20,storeSignals:async()=>({orders:10,totalRevenue:900,excludedCurrencyOrders:1,productRows:[]}),collectionAdsPerformance:async()=>({}),
  accountCvr:async()=>({cvr:0.02,source:'benchmark'}),_fxRateToUsd:async()=>0.72,_accountTz:async()=>'America/Toronto',listCountries:async()=>[],
  keywordResearch:async s=>{calls.seeds.push(...s);return {ok:true,status:200,ideas:s.map(idea)};},
  openaiJSON:async(prompt,opts)=>{calls.prompt=prompt;calls.opts=opts;return {opportunities:JSON.parse(JSON.stringify(proposals))};}};
 const scan=engine(fakes);
 const r=await scan.scanOpportunities({force:true});
 assert.equal(calls.opts.webSearch,5,'the scan call alone asks for web verification');
 assert(/between 2026-10-06 and 2026-11-13/.test(calls.prompt));assert(/Verify every date/.test(calls.prompt));assert(/AU, CA, GB, US/.test(calls.prompt));
 assert(!/recommendedDailyBudget|"cpcLow"|"searches":|startDate \(|proven \(bool/.test(calls.prompt),'the prompt no longer asks for fields the code discards');
 assert(!calls.seeds.some(s=>/daughter|animal lover|firefighter|christmas/.test(s)),'dropped occasions spend no Keyword Planner quota');
 const list=r.opportunities;assert.equal(J(list.map(o=>o.occasion).sort()),J(['Canadian Thanksgiving','Halloween']),J(r.scanAudit&&r.scanAudit.checks.filter(c=>/dates|grounding_summary/.test(c.id))));
 const h=list.find(o=>o.occasion==='Halloween'),c=list.find(o=>o.occasion==='Canadian Thanksgiving');
 assert.equal(h.peakDate,'2026-10-31');assert.equal(h.dateCheck.source,'calendar rule');assert.equal(h.dateCheck.proposedDate,'2026-10-30');
 assert.equal(h.endDate,'2026-10-31');assert.equal(h.startDate,'2026-10-14');assert.equal(h.proven,false,'memory has no success for this occasion');
 assert.equal(J(c.markets),'["CA"]');assert.equal(J(c.countries),'["2124"]');assert.equal(c.endDate,'2026-10-12');
 assert.equal(h.research.realCount,6);assert.equal(h.research.searchVolume,5*1200,'close variants of one phrase are counted once');
 assert(h.plan.expected.windowSearches>h.research.searchVolume,'October searches exceed the 12-month average');
 assert(list.every(o=>o.eligibility&&typeof o.eligibility.reason==='string'));
 const dates=r.scanAudit.checks.find(x=>x.id==='opportunity_dates');assert(dates&&/1 already passed, 1 under 7 days away, 1 too far ahead, 1 without a verified date/.test(dates.detail),dates&&dates.detail);
 // ---- Serve time: a passed occasion disappears even when its saved end date runs later ----
 const saved=[Object.assign({},h,{peakDate:'2026-09-27',endDate:'2026-10-09',occasion:'National Daughters Day'}),
  {occasion:'Halloween',collectionHandle:'celestial',startDate:'2026-09-20',endDate:'2026-11-05',durationDays:20,estTotalSpend:200,plan:{duration:{days:20},expected:{spendTotal:200,conversions:2}},eligibility:{ready:true}}];
 const serve=engine(Object.assign({},fakes,{scanOpportunities:async()=>({opportunities:saved,scannedAt:NOW-3600000,searchResearchVersion:3}),_deletedOpportunityTags:async()=>new Set(),takenTags:async()=>({})}));
 const s=await serve.opportunitiesWithStatus({});
 assert.equal(s.opportunities.length,1,'past occasion dropped');const legacy=s.opportunities[0];
 assert.equal(legacy.startDate,'2026-09-29');assert.equal(legacy.durationDays,38);assert.equal(legacy.estTotalSpend,380);assert.equal(legacy.plan.expected.conversions,3.8);
 console.log('opportunity-research: ok');
})().catch(e=>{console.error(e);process.exit(1);});
