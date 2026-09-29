// Opportunity research: occasion dates, Keyword Planner demand, forecast maths, ranking and the scan's
// date/eligibility gates, the draft built from a card, the calendar drafts and the scan start.
// Offline: every Google, Shopify, Firestore and AI dependency is a fake.
// RESEARCH_ENGINE, RESEARCH_KICK and RESEARCH_WORKER point the checks at other copies of the three functions.
const fs=require('fs'),vm=require('vm'),assert=require('assert'),path=require('path');
const FN=path.resolve(__dirname,'../../netlify/functions');
const file=process.env.RESEARCH_ENGINE||path.join(FN,'googleAdsAutopilot.js');
const source=fs.readFileSync(file,'utf8');
const NOW=Date.parse('2026-09-29T15:00:00Z');
const fixedDate=now=>class extends Date{constructor(...a){if(a.length)super(...a);else super(now);}static now(){return now;}};
// now = the fixed clock the engine sees (default NOW, 11:00 in Toronto).
function engine(fns={},now=NOW){
 const sandbox={module:{exports:{}},process:{env:{GADS_CURRENCY:'USD',KP_BACKOFF_MS:'0'}},URL,URLSearchParams,Intl,Date:fixedDate(now),console,Buffer,setTimeout,clearTimeout,AbortController,
  require:name=>name==='node-fetch'?(()=>{throw Error('Unexpected network');}):require('module').createRequire(path.join(FN,'googleAdsAutopilot.js'))(name),fns};
 vm.createContext(sandbox);
 vm.runInContext(source+'\nmodule.exports.test={rule:_occasionRule,peak:_nextOccasionPeak,monthEnd:_kpMonthEnd,windowSearches:_windowSearches,plan:planCampaign,score:_oppScore,cls:opportunityClass,kwKey:_kwCacheKey,research:researchOpportunity,schedule:_campaignScheduleFields,gdate:gAdsDate,seedsFor:_inventoryKeywordSeeds,clickShare:typeof searchClickShare==="function"?searchClickShare:null};',sandbox);
 vm.runInContext('for(const k of Object.keys(fns))globalThis[k]=fns[k];',sandbox);
 return sandbox.module.exports;
}
// A small in-memory Firestore: documents by "collection/id", shallow merge, transactions applied on return.
function store(init={}){const m=new Map(Object.entries(init)),c=v=>JSON.parse(JSON.stringify(v));
 const ref=p=>({id:p.split('/').pop(),get:async()=>({exists:m.has(p),data:()=>m.has(p)?c(m.get(p)):undefined}),set:async(v,o)=>{m.set(p,o&&o.merge?Object.assign({},m.get(p)||{},c(v)):c(v));}});
 const coll=n=>({doc:id=>ref(n+'/'+id),get:async()=>({empty:true,size:0,docs:[],forEach(){}}),where:()=>coll(n),limit:()=>coll(n),orderBy:()=>coll(n)});
 return {m,db:{collection:coll,runTransaction:async fn=>{const ops=[],tx={get:r=>r.get(),set:(r,v,o)=>{ops.push(()=>r.set(v,o));return tx;}};const out=await fn(tx);for(const op of ops)await op();return out;}}};}
// The console API (googleAdsAutopilotKick.js) and the background worker, loaded with fake neighbours.
function loadModule(fileName,envName,deps,env){const f=process.env[envName]||path.join(FN,fileName),sb={module:{exports:{}},process:{env:env||{}},console,Buffer,setTimeout,clearTimeout,URL,URLSearchParams,Date,
  require:n=>n in deps?deps[n]:/firebaseAdmin$/.test(n)?deps.firebaseAdmin:/^\.\/_\w+$/.test(n)?loadModule(n.slice(2)+'.js',null,deps,env):require(n)};
 sb.exports=sb.module.exports;vm.createContext(sb);vm.runInContext(fs.readFileSync(f,'utf8'),sb);return sb.module.exports;}
let passed=0;const failed=[];
// Each follow-up check runs on its own, so a run against an older engine names every check that fails there.
async function check(name,fn){try{await fn();passed++;console.log('PASS',name);}catch(e){failed.push(name);console.log('FAIL',name,'--',String(e&&e.message||e).split('\n')[0].slice(0,400));}}
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
 const calls={prompt:null,opts:null,seeds:[],geo:[]};
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
  control:async()=>({maxDailyBudgetTotal:100,budgetCurrency:'CAD',budgetCurrencyVerified:true,defaultCountries:['2036','2124','2826','2840'],smartBidding:false,orderCutoffDays:0}), // no order cutoff: runs end on the gift day
  getCollections:async()=>[{handle:'celestial',title:'celestial'},{handle:'maple',title:'maple'}],fetchTopProducts:async()=>[],conversionHealth:async()=>({validated:true}),
  collectionProfiles:async()=>({list:[profile('celestial','moon','star'),profile('maple','maple','leaf')],at:NOW,salesBasis:'fixture'}),proposePmaxOpportunities:async()=>({list:[],at:NOW}),
  playbookSlice:async()=>null,playbookText:()=>'',_learningTrace:()=>null,
  storeSalesEvidence:async()=>({available:false,periods:{days30:null,days90:null},seasonality:{},merchant:{},warnings:[]}),
  _enabledBudgetTotal:async()=>20,storeSignals:async()=>({orders:10,totalRevenue:900,excludedCurrencyOrders:1,productRows:[]}),collectionAdsPerformance:async()=>({}),
  accountCvr:async()=>({cvr:0.02,source:'benchmark'}),_fxRateToUsd:async()=>0.72,_accountTz:async()=>'America/Toronto',listCountries:async()=>[],
  keywordResearch:async(s,g)=>{calls.seeds.push(...s);calls.geo.push({seeds:s.slice(),geo:(g||[]).map(String)});return {ok:true,status:200,ideas:s.map(idea)};},
  // Like the real wrapper, the fake reports the pages its web search returned through opts.info.
  openaiJSON:async(prompt,opts)=>{calls.prompt=prompt;calls.opts=opts;if(opts&&opts.info)opts.info.sources=['https://www.timeanddate.com/holidays/us/halloween','https://www.canada.ca/en/thanksgiving.html','https://nationaltoday.com/national-daughters-day/','https://www.worldanimalday.org.uk/'].map(url=>({url,title:''}));return {opportunities:JSON.parse(JSON.stringify(proposals))};}};
 const scan=engine(fakes);
 const r=await scan.scanOpportunities({force:true});
 assert.equal(J(calls.opts.webSearch),J({maxUses:5,userLocation:{country:'US'}}),'the scan call alone asks for web verification, in the targeted market');
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
 // ---- Due calendar events: the daily task pays for a draft only when history says it is not already taken or refused ----
 {const rejected=[];const due=taken=>engine({loadCalendar:async()=>({celestial:{handle:'celestial',title:'Celestial',peaks:[{label:'Halloween',leadDays:32,angle:''}]}}),takenTags:async()=>taken,_accountTz:async()=>'America/Toronto',
   fb:()=>({db:{collection:()=>({where:(k,op,v)=>({limit:()=>({get:async()=>({forEach:fn=>rejected.filter(d=>d[k]===v).forEach(d=>fn({id:'r',data:()=>d}))})})})})}})}).dueEvents('2026-09-29');
  assert.equal(J((await due({})).map(d=>d.event.peakDate)),'["2026-10-31"]','Halloween is due 32 days out');
  const unreadable={};Object.defineProperty(unreadable,'_errors',{value:['Approval history could not be checked.'],enumerable:false});
  assert.equal((await due(unreadable)).length,0,'history that cannot be read drafts nothing today (tomorrow retries) instead of paying for duplicates');
  rejected.push({status:'REJECTED',tag:'celestial-halloween',deletedAt:NOW-86400000});
  assert.equal((await due({})).length,0,'an occasion Paul rejected yesterday is not drafted again in the same window');
  rejected[0].deletedAt=NOW-340*86400000;assert.equal((await due({})).length,1,'last year’s rejection does not block this year');}
 console.log('opportunity-research: ok');
 console.log('opportunity-research: core checks ok');

 // ======== Follow-ups. Each check failed on the engine before its fix (run with RESEARCH_ENGINE etc.). ========
 const ctrlG={maxDailyBudgetTotal:100,budgetCurrency:'CAD',budgetCurrencyVerified:true,defaultCountries:['2124','2840'],smartBidding:false,orderCutoffDays:0};
 // A draft built from a scanned card: every paid or outside call is a fake that counts.
 function drafting(over={}){const out={rsa:0,kp:0,approvals:[],built:[]},st=store(over.docs||{'Brites_GAds_State/opportunities':{list:[h]}});
  const fns=Object.assign({},fakes,{fb:()=>({db:st.db,FV:{serverTimestamp:()=>null}}),loadCalendar:async()=>({}),accountWasteNegatives:async()=>[],recordOccasionUse:async()=>{},gaql:async()=>[],
   generateRSAAssets:async()=>{out.rsa++;return {headlines:Array.from({length:15},(_,i)=>'Headline '+i),descriptions:Array.from({length:4},(_,i)=>'Description '+i)};},
   enqueueApproval:async item=>{out.approvals.push(item);return 'ap'+out.approvals.length;},
   buildSearchCampaignOps:(coll,event,assets,o)=>{out.built.push(o);return {ops:[{campaignOperation:{create:Object.assign({},o.startDate?{startDateTime:o.startDate+' 00:00:00'}:{},o.endDate?{endDateTime:o.endDate+' 23:59:59'}:{})}}],tag:'celestial-halloween',negatives:[],assetSummary:null,keywordSummary:{count:(o.keywordPlan||[]).length,measured:(o.keywordPlan||[]).length,researched:true,exact:0},adGroupSummary:[]};},
   keywordResearch:async s=>{out.kp++;return {ok:true,status:200,ideas:s.map(idea)};}},over.fns||{});
  return {e:engine(fns,over.now||NOW),out};}
 const cardArgs=(o={})=>Object.assign({ctrl:ctrlG,startDate:h.startDate,endDate:h.endDate,countries:['2124','2840'],maxCpc:h.maxCpc,peakDate:h.peakDate},o);

 // Item 7: every opportunity and draft date is the account's date. 02:00 UTC is 22:00 the day before in Toronto.
 await check('plans, schedule fields and date clamps use the account date, not the server UTC date',async()=>{
  const e=engine({_accountTz:async()=>'America/Toronto'},Date.parse('2026-09-29T02:00:00Z')).test;
  const u2=e.plan({title:'Celestial',occasion:'Evergreen gifting',ceiling:100,headroom:80,research:null,aov:100,currency:'CAD',nativeToUsd:0.72});
  assert.equal(J([u2.duration.startDate,u2.duration.endDate]),J(['2026-09-28','2026-10-25']),'an undated test starts on the account day');
  assert.equal(e.schedule('2026-09-29','2026-10-10').startDateTime,'2026-09-29 00:00:00','tomorrow in Toronto is a future start, so it is sent, as "yyyy-MM-dd HH:mm:ss" (the layout Google documents and returns)');
  assert.equal(e.schedule('2026-09-29','2026-10-10').endDateTime,'2026-10-10 23:59:59','the end is the last second of its day, in the same layout');
  assert.equal(e.schedule('2026-09-28','2026-10-10').startDateTime,undefined,'today in Toronto starts on enable');
  assert.equal(e.gdate('2026-09-20',true),'20260928','a past date is clamped to the account day');
  assert.equal(engine({},Date.parse('2026-12-26T02:00:00Z')).test.peak('Christmas'),'2026-12-25','Christmas evening in Toronto is still Christmas');
 });
 await check('calendar drafts count the days left from the account date',async()=>{
  const cal={celestial:{title:'Celestial',peaks:[{label:'Halloween',leadDays:23,angle:'glow'}]}};
  const due=await engine({loadCalendar:async()=>cal,_accountTz:async()=>'America/Toronto',takenTags:async()=>({})},Date.parse('2026-10-12T02:00:00Z')).dueEvents();
  assert.equal(J(due.map(d=>[d.coll.handle,d.event.daysLeft,d.event.peakDate])),J([['celestial',20,'2026-10-31']]),'Oct 11 in Toronto: 20 days left, inside the 20-23 day lead window');
 });
 await check('serving drops a passed occasion using the account date of the scan',async()=>{
  const old={occasion:'Halloween',collectionHandle:'celestial',startDate:'2026-10-20',endDate:'2026-11-05',durationDays:17,plan:{duration:{days:17},expected:{}},eligibility:{ready:true}}; // saved before peakDate existed
  const e=engine(Object.assign({},fakes,{scanOpportunities:async()=>({opportunities:[old],scannedAt:Date.parse('2026-11-01T02:00:00Z'),searchResearchVersion:3}),_deletedOpportunityTags:async()=>new Set(),takenTags:async()=>({})}),Date.parse('2026-11-01T15:00:00Z'));
  assert.equal((await e.opportunitiesWithStatus({})).opportunities.length,0,'scanned on Halloween evening in Toronto: that Halloween, not next year\'s');
 });
 // Serve-time clamp: the run-length sentence states the served length.
 await check('a shortened served window rewrites its run-length sentence',async()=>{
  const b='Runs 18 days and ends on the Halloween date (2026-10-31): gift searches stop converting once it has passed.';
  const o={occasion:'Halloween',collectionHandle:'celestial',peakDate:'2026-10-31',startDate:'2026-09-20',endDate:'2026-10-07',durationDays:18,estTotalSpend:180,plan:{duration:{days:18,startDate:'2026-09-20',endDate:'2026-10-07',basis:b},expected:{spendTotal:180}},eligibility:{ready:true}};
  const e=engine(Object.assign({},fakes,{scanOpportunities:async()=>({opportunities:[o],scannedAt:NOW-3600000,searchResearchVersion:3}),_deletedOpportunityTags:async()=>new Set(),takenTags:async()=>({})}));
  const x=(await e.opportunitiesWithStatus({})).opportunities[0];
  assert.equal(x.durationDays,9);assert.equal(x.plan.duration.basis,'Now runs 9 days (researched as 18) and ends on the Halloween date (2026-10-31): gift searches stop converting once it has passed.');
 });
 // Undated tests: the sentence gives the length after the competition adjustment.
 await check('an undated run states its length after the competition adjustment',async()=>{
  const kwc=ci=>({ok:true,realCount:1,competitionIndex:ci,cpc:{low:0.5,high:1.2},keywords:[{text:'gold moon necklace',real:true,searches:300,low:0.5,high:1.2,competitionIndex:ci}]});
  const hi=t.plan({title:'Celestial',occasion:'Evergreen gifting',ceiling:100,headroom:80,research:kwc(80),aov:100,currency:'CAD',nativeToUsd:0.72});
  const lo=t.plan({title:'Celestial',occasion:'Evergreen gifting',ceiling:100,headroom:80,research:kwc(20),aov:100,currency:'CAD',nativeToUsd:0.72});
  assert.equal(J([hi.duration.days,lo.duration.days]),J([33,25]));
  assert.match(hi.duration.basis,/^Runs 33 days .*extended for high keyword competition \(80\/100\)/);assert.match(lo.duration.basis,/^Runs 25 days .*trimmed for low keyword competition \(20\/100\)/);
 });
 // Item 4: order cutoff.
 await check('order cutoff: a dated run ends that many days before the gift date and says so',async()=>{
  const pc=t.plan({title:'Celestial',occasion:'Halloween',peakDate:'2026-10-31',ceiling:100,headroom:80,research,aov:100,currency:'CAD',nativeToUsd:0.72,cvrInfo:{cvr:0.02,source:'benchmark'},orderCutoffDays:7});
  assert.equal(J([pc.duration.startDate,pc.duration.endDate,pc.duration.days,pc.duration.lastOrderDate,pc.duration.orderCutoffDays]),J(['2026-10-07','2026-10-24',18,'2026-10-24',7]));
  assert.match(pc.duration.basis,/^Runs 18 days and ends on 2026-10-24, 7 days before the Halloween date \(2026-10-31\): the order cutoff in Controls, the last day an order can still arrive in time/);
  const saved=v=>engine({fb:()=>({db:store({'Brites_GAds_Control/control':{orderCutoffDays:v}}).db}),_accountCurrency:async()=>'CAD'}).control();
  assert.equal(J([(await saved('45')).orderCutoffDays,(await saved(3)).orderCutoffDays,(await saved(-2)).orderCutoffDays,(await engine({fb:()=>null,_accountCurrency:async()=>'CAD'}).control()).orderCutoffDays]),J([30,3,7,7]),'Controls value: whole days 0-30, default 7');
 });
 await check('order cutoff in the scan: the research window, prompt, card dates and lead-time filter move by it',async()=>{
  let prompt='';
  const e=engine(Object.assign({},fakes,{control:async()=>Object.assign({},await fakes.control(),{orderCutoffDays:7}),openaiJSON:async(p,opts)=>{prompt=p;return fakes.openaiJSON(p,opts);}}));
  const rc=await e.scanOpportunities({force:true});
  assert.match(prompt,/between 2026-10-13 and 2026-11-20/);assert.match(prompt,/ends 7 days before the occasion's date, the last day an order can still arrive in time/);
  const hc=rc.opportunities.find(o=>o.occasion==='Halloween');
  assert.equal(J([hc.startDate,hc.endDate,hc.durationDays,hc.plan.duration.lastOrderDate]),J(['2026-10-07','2026-10-24',18,'2026-10-24']));
  assert(!rc.opportunities.some(o=>o.occasion==='Canadian Thanksgiving'),'Oct 12 is 13 days away: its last order day is under a week out');
  assert.match(rc.scanAudit.checks.find(x=>x.id==='opportunity_dates').detail,/2 under 14 days away \(7-day order cutoff\)/);
 });
 await check('order cutoff at draft time: no draft once the last order day has passed, and nothing is paid for',async()=>{
  const {e,out}=drafting({now:Date.parse('2026-10-26T15:00:00Z')});
  const g=await e.generateForCollection('celestial','Halloween',5,cardArgs({ctrl:Object.assign({},ctrlG,{orderCutoffDays:7}),startDate:'2026-10-26',endDate:'2026-10-31'}));
  assert.equal(g.ok,false);assert.equal(g.reason,'The last day to order in time for Halloween (2026-10-24, 7 days before 2026-10-31) has passed. No draft was created.');
  assert.equal(J([out.rsa,out.kp,out.approvals.length]),J([0,0,0]));
  const d=drafting(),g2=await d.e.generateForCollection('celestial','Halloween',0,{ctrl:Object.assign({},ctrlG,{orderCutoffDays:7}),countries:['2124']});
  assert.equal(g2.ok,true,g2.reason);assert.equal(J([g2.startDate,g2.endDate]),J(['2026-10-07','2026-10-24']),'a draft without chosen dates ends at the order cutoff');
 });
 // Item 3: one Keyword Planner pool per country set.
 await check('Keyword Planner measures each occasion only in the countries where it is observed',async()=>{
  const thanks=calls.geo.filter(g=>g.seeds.some(x=>/thanksgiving/.test(x))),hal=calls.geo.filter(g=>g.seeds.some(x=>/halloween/.test(x)));
  assert(thanks.length&&thanks.every(g=>J(g.geo)==='["2124"]')&&thanks.every(g=>!g.seeds.some(x=>/halloween/.test(x))),'Canadian Thanksgiving is measured in Canada only: '+J(thanks.map(g=>g.geo)));
  assert(hal.length&&hal.every(g=>J(g.geo.slice().sort())==='["2124","2840"]'),'Halloween is measured in its two markets: '+J(hal.map(g=>g.geo)));
  const pool=r.scanAudit.checks.find(x=>x.id==='keyword_planner_pool');assert.equal(pool.meta.countrySets.length,3,'three country sets, one request batch each');
 });
 // Item 5: conflicts compare theme phrases and never cross different gift dates.
 await check('conflicts compare motif and occasion phrases, never materials, and never cross different gift dates',async()=>{
  const opp=(occasion,peakDate,texts)=>({collectionHandle:'celestial',occasion,peakDate,keywordData:texts.map(text=>({text}))});
  const kept=scan._util.resolveOpportunityConflicts([opp('Halloween','2026-10-31',['gold moon necklace halloween','engraved moon necklace halloween','gold star necklace halloween','engraved star necklace halloween']),
   opp('Evergreen gifting',null,['gold sun necklace','engraved sun necklace','gold comet necklace','engraved comet necklace']),
   opp('Diwali','2026-11-08',['gold moon necklace diwali','engraved moon necklace diwali','gold star necklace diwali','engraved star necklace diwali']),
   opp('Halloween party','2026-10-31',['gold moon necklace halloween','engraved star necklace halloween','moon necklace halloween gift','star necklace halloween'])]);
  assert.equal(J(kept.map(o=>o.occasion)),J(['Halloween','Evergreen gifting','Diwali']));
 });
 // Item 6: demand direction and Studio research count only the keywords asked for.
 await check('measured demand direction and Studio research count only the keywords asked for',async()=>{
  const lamp={text:'moon phase lamp',searches:5000,competitionIndex:20,low:.4,high:.9,monthly:[1000,1000,1000,1000,1000,1000,1000,1000,1000,9000,9000,9000]};
  const asked={text:'gold moon necklace',searches:100,competitionIndex:20,low:.5,high:1.2,monthly:Array(12).fill(100)};
  const e=engine({keywordResearch:async()=>({ok:true,ideas:[asked,lamp]})});
  const m=e.mergeKeywordResearch(['gold moon necklace'],{ok:true,ideas:[asked,lamp]});
  assert.equal(J([m.demandMeasured,m.demandSlopePct]),J(['steady',0]),'a related idea\'s trend is not this campaign\'s demand');
  const so=await e.test.research(['gold moon necklace'],['2124'],{seedsOnly:true});
  assert.equal(J(so.keywords.map(k=>k.text)),J(['gold moon necklace']),'Studio research keeps its own keywords only');
 });
 // Item 8: click share measured on the account when it has enough Search history.
 await check('click share is measured from Search impression share and CTR, else labelled an assumption',async()=>{
  const row=(i,c,is)=>({metrics:{impressions:String(i),clicks:String(c),searchImpressionShare:is}});
  const cs=rows=>{const e=engine({gaql:async()=>rows,_accountTz:async()=>'America/Toronto'});return e.test.clickShare?e.test.clickShare():null;};
  const m=await cs([row(8000,240,0.4),row(2000,60,0.2),row(500,5,0)]);
  assert.equal(J([m.share,m.source,m.clicks,m.impressions]),J([0.01,'measured',300,10000]),'300 clicks / (8000/0.4 + 2000/0.2) eligible impressions');
  assert.equal(J([(await cs([row(2000,50,0.3)])).source,(await cs([row(2000,50,0.3)])).share]),J(['assumption',0.05]),'too little history');
  assert.equal((await cs([row(3000,1500,0.9)])).share,0.15,'clamped to 15%');
  const base2={title:'Celestial',occasion:'Halloween',peakDate:'2026-10-31',ceiling:100,headroom:80,research,aov:100,currency:'CAD',nativeToUsd:0.72,cvrInfo:{cvr:0.02}};
  const measured=t.plan(Object.assign({clickShare:m},base2)),assumed=t.plan(base2);
  assert.equal(J([measured.model.clickShare,measured.model.clickShareSource,assumed.model.clickShare,assumed.model.clickShareSource]),J([0.01,'measured',0.05,'assumption']));
  assert(measured.model.maxClicksPerDay<assumed.model.maxClicksPerDay);assert.match(measured.budget.basis,/measured: your Search impression share/);assert.match(assumed.budget.basis,/\(assumption\)/);
  assert(r.scanAudit.checks.some(x=>x.id==='search_click_share'),'the scan records which share it used');
 });
 // Coordinator: a plan forecast to lose money is never ready.
 await check('a card forecast below break-even is not ready and says why',async()=>{
  const e=engine(Object.assign({},fakes,{storeSignals:async()=>({orders:10,totalRevenue:90,productRows:[]})}));
  const low=(await e.scanOpportunities({force:true})).opportunities.find(o=>o.occasion==='Halloween');
  assert.equal(J([low.eligibility.ready,low.eligibility.label,low.eligibility.projectedRoas,low.eligibility.breakEvenRoas]),J([false,'Forecast below break-even',0.27,1.54]));
  assert.match(low.eligibility.reason,/^Projected ROAS 0\.27x \(0\.2–0\.4x with conversion uncertainty\) is below the 1\.54x break-even at a 65% margin: this plan is forecast to lose CAD 118 over the run\.$/);
  assert.equal(h.eligibility.ready,true,'a profitable forecast stays ready');
 });
 // Coordinator: budget room is judged on the budget Paul chooses.
 await check('the recommended budget above the room left does not block a card; no room for one click does',async()=>{
  const some=(await engine(Object.assign({},fakes,{_enabledBudgetTotal:async()=>98.5})).scanOpportunities({force:true})).opportunities.find(o=>o.occasion==='Halloween');
  assert.equal(J([some.eligibility.ready,some.eligibility.budgetFits,some.recommendedDailyBudget]),J([true,false,5]));assert.match(some.eligibility.reason,/CAD 5\/day is above the CAD 1\.5\/day left under the daily ceiling: choose a budget that fits/);
  const none=(await engine(Object.assign({},fakes,{_enabledBudgetTotal:async()=>99})).scanOpportunities({force:true})).opportunities.find(o=>o.occasion==='Halloween');
  assert.equal(none.eligibility.ready,false);assert.match(none.eligibility.reason,/Only CAD 1\/day is left under the daily ceiling, not enough for one CAD 1\.4 click a day/);
 });
 await check('budget room is checked against the budget chosen for the draft, before any paid copy',async()=>{
  const {e,out}=drafting();
  const no=await e.generateForCollection('celestial','Halloween',90,cardArgs());
  assert.equal(no.ok,false);assert.equal(no.reason,'The CAD 90/day budget is above the CAD 80/day left under the CAD 100/day ceiling (enabled campaigns use CAD 20/day). Choose a budget that fits, raise the ceiling in Controls, or pause a campaign. No draft was created.');
  assert.equal(J([out.rsa,out.approvals.length]),J([0,0]),'no paid copy for a refused draft');
  const yes=await e.generateForCollection('celestial','Halloween',80,cardArgs());assert.equal(yes.ok,true,yes.reason);assert.equal(out.approvals[0].payload.plan.budget.daily,80);
 });
 // Item 1: the draft is planned after grounding, from the card's own keywords.
 await check('a draft from a card is costed from its grounded keywords: the card forecast, no generic research',async()=>{
  const {e,out}=drafting();
  const g=await e.generateForCollection('celestial','Halloween',h.recommendedDailyBudget,cardArgs());
  assert.equal(g.ok,true,g.reason);assert.equal(out.kp,0,'no Keyword Planner call for generic collection seeds');
  const dp=out.approvals[0].payload.plan;
  assert.equal(J([dp.duration.startDate,dp.duration.endDate,dp.duration.days,dp.budget.daily,dp.cpc.max,dp.model.maxClicksPerDay]),J([h.startDate,h.endDate,h.durationDays,h.recommendedDailyBudget,h.maxCpc,h.plan.model.maxClicksPerDay]));
  assert.equal(J(dp.expected),J(h.plan.expected),'the draft forecast is the card forecast');
  assert.equal(J(out.built[0].keywordPlan.map(k=>k.text)),J(h.keywordData.map(k=>k.text)),'the draft buys the card keywords');
 });
 await check('a custom draft is costed after inventory grounding: rejected Planner ideas never set its demand',async()=>{
  const junk={text:'celestial wallpaper',searches:50000,competition:'HIGH',competitionIndex:90,low:6,high:15,monthly:Array(12).fill(50000),monthlyEnd:'2026-08'};
  const {e,out}=drafting({docs:{},fns:{keywordResearch:async()=>({ok:true,status:200,ideas:[junk].concat(hallo.slice(0,5).map(idea))})}});
  const g=await e.generateForCollection('celestial','Halloween',0,{ctrl:ctrlG,countries:['2124']});
  assert.equal(g.ok,true,g.reason);const dp=out.approvals[0].payload.plan;
  assert.equal(dp.expected.searchVolume,4800,'four measured phrases (a plural counted once), not the rejected 50,000-search idea');
  assert(!out.built[0].keywordPlan.some(k=>k.text==='celestial wallpaper'));
 });
 // Coordinator: the Approvals summary.
 await check('the Approvals summary counts the chosen window inclusively and labels the cap in the account currency',async()=>{
  const {e,out}=drafting();
  const g=await e.generateForCollection('celestial','Halloween',5,cardArgs({startDate:'2026-10-20',endDate:'2026-10-29',maxCpc:1.1}));
  assert.equal(g.ok,true,g.reason);const a=out.approvals[0];
  assert.match(a.summary,/\(2026-10-20 → 2026-10-29, 10d\)/);assert.match(a.summary,/Manual CPC ≤ CAD 1\.10\/click/);
  assert.equal(J([a.payload.plan.duration.days,a.payload.plan.budget.daily,a.payload.plan.cpc.max]),J([10,5,1.1]),'the stored forecast describes the chosen window, budget and cap');
  assert.match(a.payload.plan.duration.basis,/^Runs 10 days, 2026-10-20 to 2026-10-29: the window chosen for this draft/);
 });
 // Item 2: calendar drafts size their own budget and stay inside the room left.
 await check('a calendar draft takes the room left under the ceiling, or none when one click does not fit',async()=>{
  const d=drafting({fns:{_enabledBudgetTotal:async()=>98.5}});
  const g=await d.e.generateForCollection('celestial','Halloween',0,{ctrl:ctrlG,peakDate:'2026-10-31',auto:true});
  assert.equal(g.ok,true,g.reason);assert.equal(J([g.budget,d.out.approvals[0].payload.plan.budget.daily]),J([1.5,1.5]),'the planner wanted CAD 5/day; CAD 1.5 was left');
  const n=drafting({fns:{_enabledBudgetTotal:async()=>99}});
  const g2=await n.e.generateForCollection('celestial','Halloween',0,{ctrl:ctrlG,peakDate:'2026-10-31',auto:true});
  assert.equal(g2.ok,false);assert.match(g2.reason,/^Only CAD 1\/day left under the CAD 100\/day ceiling \(enabled campaigns use CAD 99\/day\): not enough for one CAD 1\.4 click a day\./);assert.equal(n.out.rsa,0);
 });
 await check('calendar drafts use the planner budget unless GADS_NEW_CAMPAIGN_BUDGET is set',async()=>{
  const seen=[],E={control:async()=>({enabled:true}),dueEvents:async()=>[{coll:{handle:'celestial'},event:{label:'Halloween',peakDate:'2026-10-31'}}],generateForCollection:async(...a)=>{seen.push(a);return {ok:true};}};
  const run=async env=>{const W=loadModule('googleAdsAutopilot-background.js','RESEARCH_WORKER',{'./googleAdsAutopilot':E,'node-fetch':async()=>{throw Error('Unexpected network');}},Object.assign({EDIT_PASSCODE:'fixture-pass'},env));
   const res=await W.handler({httpMethod:'POST',headers:{},body:JSON.stringify({tasks:['events'],token:'fixture-pass'})});assert.equal(res.statusCode,200);return seen.pop();};
  const a=await run({}),b=await run({GADS_NEW_CAMPAIGN_BUDGET:'12'});
  assert.equal(J([a[2],a[3].auto,a[3].peakDate]),J([0,true,'2026-10-31']),'0 = the planner sizes it');assert.equal(J([b[2],b[3].auto]),J([12,true]));
 });
 // Coordinator: scores spread across realistic cards instead of all reading 99.
 await check('scores spread across realistic cards',async()=>{
  const card=(clicks,profit,profitLow)=>({confidence:{score:90},research:{searchVolume:6000},economics:{orders:10},market:{fit:1},peakDate:null,daysOut:0,
   keywordData:Array.from({length:12},()=>({real:true,searches:500})),plan:{expected:{clicksTotal:clicks,profit,profitLow}}});
  const s=[card(20,-40,-90),card(60,10,-30),card(150,80,20)].map(c=>t.score(c).score);
  assert(s[0]<s[1]&&s[1]<s[2]&&s[2]<99,J(s));
 });
 // Coordinator: an occasion seed carries the occasion's name, never a whole descriptive label.
 await check('inventory seeds use the occasion name, not a long occasion label',async()=>{
  const pr=profile('celestial','moon','star');
  assert(!t.seedsFor(pr,'Libra season, October birthdays and Halloween celestial style').some(x=>/season|birthdays|,/.test(x)));
  assert(t.seedsFor(pr,"Mother's Day").includes("moon necklace mother's day"));
 });
 // Coordinator: a scan whose worker finishes before the console API's second write stays finished.
 await check('the scan start never turns a finished scan back to "scanning"',async()=>{
  const COL={state:'S',control:'C',approvals:'A'},runKick=async fetchImpl=>{const st=store();
   const E={COL,OPPORTUNITY_ENGINE_VERSION:'fixture',control:async()=>({enabled:true}),opportunitiesWithStatus:async()=>({opportunities:[]})};
   const K=loadModule('googleAdsAutopilotKick.js','RESEARCH_KICK',{'./googleAdsAutopilot':E,'node-fetch':(u,o)=>fetchImpl(st,JSON.parse(o.body)),firebaseAdmin:{firestore:Object.assign(()=>st.db,{FieldValue:{serverTimestamp:()=>null}})}},{EDIT_PASSCODE:'fixture-pass',URL:'https://console.fixture.test'});
   const out=await K.handleAction({action:'opportunities',force:true});return {out,doc:st.m.get('S/opportunities'),audit:(st.m.get('S/opportunityScanAudit')||{}).scanAudit};};
  const fast=await runKick(async(st,b)=>{await st.db.collection('S').doc('opportunities').set({scanning:false,at:1,lastError:null},{merge:true});
   await st.db.collection('S').doc('opportunityScanAudit').set({scanAudit:{runId:b.scanRunId,status:'success',checks:[{id:'scan_result',status:'ok'}]}},{merge:true});return {status:202,ok:true};});
  assert.equal(J([fast.doc.scanning,fast.audit.status]),J([false,'success']),'the worker finished first: its result stands');
  const slow=await runKick(async()=>({status:202,ok:true}));
  assert.equal(J([slow.doc.scanning,slow.audit.status,slow.audit.checks[1].status,slow.out.started]),J([true,'running','ok',true]),'accepted and not yet started');
  const down=await runKick(async()=>({status:502,ok:false}));
  assert.equal(J([down.doc.scanning,down.doc.lastError,down.audit.status]),J([false,'background dispatch HTTP 502','failed']));
 });
 await check('Controls saves the order cutoff as whole days from 0 to 30',async()=>{
  const st=store(),E={COL:{state:'S',control:'C'},control:async()=>({enabled:true})};
  const K=loadModule('googleAdsAutopilotKick.js','RESEARCH_KICK',{'./googleAdsAutopilot':E,'node-fetch':async()=>{throw Error('Unexpected network');},firebaseAdmin:{firestore:Object.assign(()=>st.db,{FieldValue:{serverTimestamp:()=>null}})}},{EDIT_PASSCODE:'fixture-pass'});
  assert.equal(J((await K.handleAction({action:'setControl',patch:{orderCutoffDays:'4.6'}})).patched),J({orderCutoffDays:5}));assert.equal(st.m.get('C/control').orderCutoffDays,5);
  await assert.rejects(K.handleAction({action:'setControl',patch:{orderCutoffDays:31}}),/Order cutoff days must be a number from 0 to 30\. Nothing was saved\./);assert.equal(st.m.get('C/control').orderCutoffDays,5);
 });
 console.log(`opportunity-research: ${passed} follow-up checks passed${failed.length?`, ${failed.length} failed: ${failed.join(' | ')}`:''}.`);
 if(failed.length)process.exitCode=1;
 require('./suite-guard.cjs').done();
})().catch(e=>{console.error(e);process.exit(1);});
