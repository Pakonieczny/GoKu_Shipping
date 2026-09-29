// Opportunity cards: evidence at a glance, honest totals, expiry gating, tag-keyed edits, scan-start failures,
// and the card's live recompute matching the research engine (demand cap, inclusive run days, occasion countries).
const path=require('path'),fs=require('fs'),vm=require('vm'),assert=require('assert/strict');
const html=fs.readFileSync(path.resolve(__dirname,'../../brites-adwords.html'),'utf8');
function pick(name){const re=new RegExp('^(?:async )?function '+name+'\\(','m'),m=re.exec(html);assert(m,name);const rest=html.slice(m.index),next=/\n(?:async )?function \w+\(/.exec(rest);return next?rest.slice(0,next.index):rest;}
function line(name){const m=new RegExp('^function '+name+'\\(.*$','m').exec(html);assert(m,name);return m[0];}
let today='2026-10-01';const toasts=[],calls=[];
const nodes={};const node=id=>nodes[id]||(nodes[id]={id,innerHTML:'',textContent:'',disabled:false,style:{},dataset:{}});
const ctx={console,Date,Math,Number,String,Object,Array,JSON,isFinite,Promise,setTimeout,clearTimeout,setInterval,clearInterval,
  esc:s=>String(s==null?'':s).replace(/[&<>]/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;'}[c])),money:n=>{n=Number(n)||0;return '$'+(Math.round(n*100)/100).toLocaleString('en-US',{minimumFractionDigits:n%1?2:0,maximumFractionDigits:2});},
  cur:()=>'CAD',elFrom:h=>h,bidView:()=>false,performanceToday:()=>today,rpYmd:d=>d.toISOString().slice(0,10),toast:m=>toasts.push(String(m)),
  $:s=>node(s.replace(/^#/,'')),document:{getElementById:id=>node(id)},actStart:()=>1,actEnd:()=>{},renderScanAudit:()=>{},setOppMeta:()=>{},oppShowErr:()=>{},
  renderOpportunities:()=>calls.push('render'),renderScanProg:()=>{ctx.OPP_SCANNING=true;},hideScanProg:()=>calls.push('hideProg'),
  oppOverride:{},oppCountries:{},OPPS:[],PMAXOPPS:[],RESEARCH_STATUS:{},OPP_RECONCILIATION:null,SCAN_AUDIT:{engineVersion:'14.0.0'},OPPSAT:null,PMAXAT:null,OPP_LAST_ERROR:null,PMAXERR:null,OPP_SCANNING:false,OPP_PROGRESS:null,OPP_LEARNING:{},oppPollTimer:null};
vm.createContext(ctx);
// The shared modules the cards draw with; the page loads them as plain scripts before its own.
ctx.BritesCampaignStyles=require('../../brites-campaign-styles.js');ctx.BritesOppTimeline=require('../../assets/opportunity-timeline.js');ctx.BritesAdPreview=require('../../assets/ad-preview-thumbs.js');
const {safeMarkup,tags,text,inOrder}=require('./lib/markup-safety.cjs');
const vis=JSON.parse(fs.readFileSync(path.resolve(__dirname,'fixtures/opportunity-visuals-sample.json'),'utf8')),clone=x=>JSON.parse(JSON.stringify(x));
for(const n of ['defCountries','oppCty','fmtDateTime','timeago'])vm.runInContext(line(n),ctx);
for(const n of ['pmaxDayMs','pmaxTodayMs','pmaxDateText','pmaxDaysText','pmaxAttr','pmaxShort','pmaxTimingList','pmaxDaysAway','pmaxOccasionChip','oppvVerdict','oppvStyles','oppvChannelIcon','oppvToday','oppvYmd','oppvDayText','oppvSchedule','oppvChip','oppvChipHtml','oppvTypeHtml','oppvHelpHtml','oppvHeadHtml','oppvWhyHtml','oppvWhenHtml','oppvAdsInner','oppvAdsHtml','oppvFactRow','oppvSearchThemes'])vm.runInContext(pick(n),ctx);
for(const n of ['oppKey','oppBidOf','oppCpcOf','oppUi','oppWindow','oppExpiry','oppBlock','oppCpd','oppCpdLine','oppDemandNote','oppSigned','oppSalesTxt','oppModel','oppProj','oppRecalc','fmtDate','daysBtw','stratLine','compChip','tailChip','kwResearchPanel','oppSourceUrl','oppSourceName','planBlock','urgPill','oppBidSeg','oppCard','researchState','researchNeedsRefresh','updateOpportunityState','loadOpportunities','launchOpp'])vm.runInContext(pick(n),ctx);
// The research engine itself, offline, on the same fixed day as the console fixtures below.
const engineFile=path.resolve(__dirname,'../../netlify/functions/googleAdsAutopilot.js'),NOW=Date.parse('2026-09-29T15:00:00Z');
class FixedDate extends Date{constructor(...a){if(a.length)super(...a);else super(NOW);}static now(){return NOW;}}
const esb={module:{exports:{}},process:{env:{GADS_CURRENCY:'CAD',KP_BACKOFF_MS:'0'}},URL,URLSearchParams,Intl,Date:FixedDate,console,Buffer,setTimeout,clearTimeout,AbortController,
  require:name=>name==='node-fetch'?(()=>{throw Error('Unexpected network');}):require('module').createRequire(engineFile)(name)};
vm.createContext(esb);vm.runInContext(fs.readFileSync(engineFile,'utf8')+'\nmodule.exports.__plan=planCampaign;',esb);const planCampaign=esb.module.exports.__plan;
const base=()=>({tag:'fixture-a|fixture occasion',occasion:'Fixture occasion',collectionTitle:'Fixture collection',startDate:'2026-10-01',endDate:'2026-10-10',durationDays:10,recommendedDailyBudget:10,estTotalSpend:270,currency:'CAD',score:120,priority:'high',
  plan:{model:{eCpcMarket:1,eCpc:1,cpcLow:0.5,cpcHigh:2,cvr:0.02,aov:100,marginRate:0.6,uncertainty:0.4},cpc:{low:0.5,max:2},duration:{days:10,basis:'Starts 17 days before the peak.'},caveats:['Conversion rate — modeled at 2.0%.','Projected revenue ~CAD 540 over the run.'],expected:{marginRate:0.6,breakEvenRoas:1.67}},
  research:{source:'google_keyword_planner',searchVolume:90,cpc:{low:0.5,high:2}},keywordData:[{text:'k1',real:true,searches:60},{text:'k2',real:true,searches:30},{text:'k3',real:true,searches:0},{text:'k4'}],
  eligibility:{ready:false,measuredKeywords:2},negatives:['a','b','c']});
// A served opportunity built from the engine's own plan, as the scan would return it.
const kw=(text,searches)=>({text,real:true,searches,low:0.5,high:2,competitionIndex:40});
function served(research,extra){const plan=planCampaign({currency:'CAD',nativeToUsd:0.73,title:'Fixture collection',occasion:'Halloween',peakDate:'2026-10-31',ceiling:100,headroom:100,smartBidding:false,research,aov:60,cvrInfo:{cvr:0.02,source:'fixture'}});
  return Object.assign({tag:'fixture-e|halloween',occasion:'Halloween',collectionTitle:'Fixture collection',startDate:plan.duration.startDate,endDate:plan.duration.endDate,durationDays:plan.duration.days,recommendedDailyBudget:plan.budget.daily,maxCpc:plan.cpc.max,estTotalSpend:plan.expected.spendTotal,currency:'CAD',score:80,plan,
    research:research?{source:'google_keyword_planner',searchVolume:research.searchVolume,cpc:research.cpc,dataAt:Date.now()-2*86400000}:{source:'ai_estimate',searchVolume:null,cpc:null,dataAt:null},keywordData:research?research.keywords:[{text:'k1'}],
    eligibility:{ready:true,measuredKeywords:4,expectedSales:plan.expected.conversions,reason:'Inventory and measured demand support a controlled test; outcomes remain uncertain.'},peakDate:'2026-10-31',dateCheck:{source:'calendar rule',proposedDate:'2026-10-30',reference:null},markets:['CA'],countries:['2124']},extra||{});}
const research=monthly=>({ok:true,realCount:4,source:'google_keyword_planner',searchVolume:monthly,competitionIndex:40,cpc:{low:0.5,high:2},keywords:[kw('fixture star necklace',monthly*0.4),kw('fixture moon necklace',monthly*0.3),kw('fixture sun pendant',monthly*0.2),kw('fixture comet charm',monthly*0.1)]});
let passed=0;const test=async(name,fn)=>{await fn();passed++;console.log('PASS',name);};
(async()=>{
await test('card face shows measured evidence, currency and totals for the served window',()=>{ctx.RESEARCH_STATUS={search:{status:'ready',checkedAt:Date.now()}};ctx.OPPS=[base()];const h=ctx.oppCard(ctx.OPPS[0],0,100);
  for(const s of ['~90','Keyword Planner · 2 of 4 keywords measured','Expected CPC · CAD','Google range $0.50–$2','~100 clicks','≈2 sales in 10 days','$10/day','Budget · CAD','≈$100 est. spend','Score 99'])assert(h.includes(s),s);
  assert(!h.includes('$270'),'stale scan-time total is not shown');assert.match(h,/fDemand warn[^>]*>Exceeds measured demand: ~30 searches in 10 days/);assert(!h.includes('~28 non-buyer'));assert.match(h,/3 theme-conflict terms/);});
await test('blocked cards name their blocker and cannot launch',async()=>{const h=ctx.oppCard(ctx.OPPS[0],0,100);assert.match(h,/disabled>Needs more evidence<\/button>/);assert.match(h,/Only 2 of the 4 required keywords/);
  ctx.api=async()=>{throw new Error('must not call the API');};await ctx.launchOpp(0,{});assert.match(toasts.pop(),/Only 2 of the 4/);
  const why=r=>ctx.oppBlock(Object.assign(base(),{eligibility:{ready:false,measuredKeywords:5,expectedSales:0.3,reason:r}}));
  assert.deepEqual([why('Measured demand supports ~0.3 expected sales over the run, too few for a test.').label,why('Daily ceiling headroom (CAD 3) is below this plan’s CAD 5/day.').label,why('The ad account currency is not verified.').label],['Too few expected sales','Needs budget room','Currency unverified']);
  assert.equal(why('Only 3 inventory-matched keywords have measured Google searches; 4 are needed.').text,'Only 3 inventory-matched keywords have measured Google searches; 4 are needed.','the engine reason is shown as written');
  const legacy=ctx.oppBlock(Object.assign(base(),{eligibility:{ready:false,measuredKeywords:5,reason:'Verify at least four inventory-matched keywords and available account budget.'}}));assert.deepEqual([legacy.label,/budget ceiling/.test(legacy.text)],['Needs budget room',true],'an older list’s generic reason is explained from its keyword count');
  const stale=why('This research is more than 12 hours old. Refresh it before creating a draft.');assert.equal(stale.kind,'refresh');assert.equal(stale.text,'','the lane header already says the research is stale');});
await test('why-these-numbers drops stale totals and explains a shortened window',()=>{const h=ctx.planBlock(ctx.OPPS[0],0);assert(!h.includes('Projected revenue'));assert.match(h,/Conversion rate/);assert.match(h,/Researched as a 27-day run/);assert.match(h,/Forecast<\/b> — clicks = daily budget ÷ expected CPC × days \(start and end dates both count\)/);});
await test('ended windows and passed occasions are marked and never launchable',async()=>{today='2026-10-20';const o=base();o.eligibility={ready:true,measuredKeywords:4};ctx.OPPS=[o];
  assert.equal(ctx.oppExpiry(o).label,'Ended');const h=ctx.oppCard(o,0,100);assert.match(h,/is-expired/);assert.match(h,/ENDED/);assert.match(h,/disabled>Not launchable/);assert.deepEqual(JSON.parse(JSON.stringify(ctx.oppWindow(o))),{start:'2026-10-01',end:'2026-10-10',days:10});
  await ctx.launchOpp(0,{});assert.match(toasts.pop(),/no longer be launched/);
  today='2026-10-06';o.peakDate='2026-10-05';assert.equal(ctx.oppExpiry(o).label,'Occasion passed');assert.match(ctx.oppCard(o,0,100),/OCCASION PASSED/);today='2026-10-01';});
await test('per-card edits follow the opportunity tag, not its list position',()=>{const a=base(),b=Object.assign(base(),{tag:'fixture-b|other'});ctx.OPPS=[a,b];ctx.oppOverride[ctx.oppKey(1)]={maxCpc:3};ctx.oppUi(1).bud=14;
  ctx.OPPS=[b];assert.equal(ctx.oppCpcOf(0),3);assert.equal(ctx.oppUi(0).bud,14);assert.match(ctx.oppCard(b,0,100),/\$14\/day/);});
await test('the card forecast reproduces the engine plan: demand cap, inclusive days, spend that follows clicks',()=>{today='2026-09-29';
  for(const monthly of [300,30000]){const o=served(research(monthly)),p=o.plan,w=ctx.oppWindow(o);
    assert.equal(w.days,p.duration.days,'run days count the start and end date');
    const pr=ctx.oppProj(o,{bud:o.recommendedDailyBudget,days:w.days,cap:o.maxCpc,smart:false});
    assert.deepEqual([pr.clicks,pr.sales,pr.spend,pr.limited],[p.expected.clicksTotal,p.expected.conversions,p.expected.spendTotal,p.expected.demandLimited],'monthly '+monthly);}
  const capped=served(research(300));ctx.OPPS=[capped];const pr=ctx.oppProj(capped,{bud:25,days:ctx.oppWindow(capped).days,cap:capped.maxCpc,smart:false});
  assert.equal(pr.cpd,capped.plan.model.maxClicksPerDay,'a bigger budget does not buy clicks that searches cannot supply');assert(pr.spend<25*ctx.oppWindow(capped).days,'spend follows the capped clicks');
  const h=ctx.oppCard(capped,0,100);assert.match(h,/>Capped by measured searches</);assert(!/Exceeds measured demand|Upper bound/.test(h));assert.match(h,/the most measured searches supply/);
  const unmeasured=served(null);assert.equal(unmeasured.plan.model.maxClicksPerDay,null);ctx.OPPS=[unmeasured];const hu=ctx.oppCard(unmeasured,0,100);assert.match(hu,/fDemand warn[^>]*>Upper bound: no measured searches</);assert.match(hu,/Estimated range/);});
await test('occasion date, markets, data age and eligibility reason are on the card; details explain them',()=>{today='2026-10-21';const o=served(research(300),{durationDays:11,startDate:'2026-10-21'});ctx.OPPS=[o];
  ctx.DASH={control:{defaultCountries:['2124','2840']}};const h=ctx.oppCard(o,0,100);
  assert.match(h,/Occasion [^<]+<\/span>/);assert.match(h,/set by the calendar rule/);assert.match(h,/>CA only</);assert.match(h,/>Fetched 2d ago</);
  assert.deepEqual(ctx.oppCty(0),['2124'],'countries default to where the occasion is observed');ctx.oppCountries[ctx.oppKey(0)]=['2840'];assert.deepEqual(ctx.oppCty(0),['2840'],'an edit wins');delete ctx.oppCountries[ctx.oppKey(0)];
  const pb=ctx.planBlock(o,0);assert.match(pb,/Occasion date<\/b> — [^,]+, set by the calendar rule \(research proposed [^)]+\)\. Observed in CA\./);assert.match(pb,/Now runs 11 days \(researched as 18\) and ends on the Halloween date/);
  assert.match(pb,/<b>Budget<\/b> — .*Measured demand limits this/);assert.match(pb,/the lower of daily budget ÷ expected CPC and the ~0\.5 a day/);assert.match(pb,/data fetched /);
  const q=Object.assign({},o,{eligibility:{ready:false,measuredKeywords:4,expectedSales:0.2,reason:'Measured demand supports ~0.2 expected sales over the run, too few for a test.'}});ctx.OPPS=[q];
  const hq=ctx.oppCard(q,0,100);assert.match(hq,/disabled>Too few expected sales<\/button>/);assert.match(hq,/oppCardBlock">Measured demand supports ~0\.2 expected sales/);delete ctx.DASH;today='2026-10-01';});
await test('a web-verified occasion date links the page it was verified on: a small external link, http(s) only',()=>{today='2026-10-21';
  const card=dc=>{const o=served(research(300),{durationDays:11,startDate:'2026-10-21',dateCheck:dc});ctx.OPPS=[o];return ctx.oppCard(o,0,100);},web=url=>({source:'web-verified research',proposedDate:'2026-10-31',reference:'timeanddate.com',url});
  const h=card(web('https://www.timeanddate.com/holidays/canada/halloween?a=1&b="2"')),a=/verified on <a class="oppDateSrc" href="([^"]*)" target="_blank" rel="noopener noreferrer"[^>]*>([^<]*)<\/a>/.exec(h);
  assert(a,'source link');assert.equal(a[1],'https://www.timeanddate.com/holidays/canada/halloween?a=1&amp;b=&quot;2&quot;');assert.equal(a[2],'timeanddate.com ↗');assert.match(h,/confirmed by web research: timeanddate\.com/);
  assert(!card({source:'calendar rule',proposedDate:'2026-10-30',reference:null,url:null}).includes('oppDateSrc'),'a calendar-rule date has no source link');
  assert(!card({source:'web-verified research',proposedDate:'2026-10-31',reference:'timeanddate.com'}).includes('oppDateSrc'),'research saved before sources were kept shows no link');
  for(const url of ['javascript:alert(1)','https://evil.example@good.example/x','//cdn.example/x',' https://lead.example/x','ftp://files.example/x'])assert(!card(web(url)).includes('oppDateSrc'),url);
  today='2026-10-01';});
await test('editing the dates keeps a valid window: one-day runs allowed, an end before the start is corrected',()=>{today='2026-10-01';const o=base();o.eligibility={ready:true,measuredKeywords:4};ctx.OPPS=[o];
  const els={},el=s=>els[s]||(els[s]={value:'',textContent:'',title:'',min:'',classList:{toggle(){}}}),key=s=>s.replace(/\[data-i="\d+"\]/,''),cards={querySelector:s=>el(key(s)),querySelectorAll:s=>[el(key(s))]};
  el('.opBud').value='10';el('.opStart').value='2026-10-05';el('.opEnd').value='2026-10-05';ctx.oppRecalc(cards,0);assert.equal(el('.opEnd').value,'2026-10-05');assert.equal(el('.opDur').textContent,1);assert.equal(el('.opDurU').textContent,'day');
  el('.opEnd').value='2026-10-03';ctx.oppRecalc(cards,0);assert.equal(el('.opEnd').value,'2026-10-05','end moves to the start');assert.equal(el('.fTot').textContent,'≈$10 est. spend');
  el('.opEnd').value='2026-10-14';ctx.oppRecalc(cards,0);assert.equal(el('.opDur').textContent,10);assert.equal(el('.fClicks').textContent,'~100 clicks');});
await test('a research start failure keeps saved results and never polls',async()=>{ctx.OPPS=[base()];ctx.api=async()=>({opportunities:[],error:'start failed'});await ctx.loadOpportunities(true);
  assert.equal(ctx.OPPS.length,1);assert.equal(ctx.OPP_SCANNING,false);assert.equal(ctx.oppPollTimer,null);assert.match(toasts.pop(),/could not start.*Saved results are unchanged/);
  ctx.api=async()=>({opportunities:[base()],scannedAt:1,scanning:false,started:false,dispatchError:'background dispatch HTTP 502',lastError:'background dispatch HTTP 502'});await ctx.loadOpportunities(true);
  assert.equal(ctx.oppPollTimer,null);assert.match(toasts.pop(),/could not start/);assert.equal(node('oppScan').disabled,false);});
await test('a draft being written survives a re-render: its card stays busy and a second click starts no second paid generation',async()=>{today='2026-10-01';
  Object.assign(ctx,{OPP_SCANNING:false,OPP_LAST_ERROR:null,OPP_RECONCILIATION:null,RESEARCH_STATUS:{},OPPSAT:Date.now(),reload:()=>{},btnBusy:(b,l)=>{b.disabled=true;b.innerHTML=l;return()=>{};}});
  const o=base();o.eligibility={ready:true,measuredKeywords:4};ctx.OPPS=[o];
  const els={},el=s=>els[s]||(els[s]={value:'',innerHTML:'',textContent:''});nodes.oppCards={querySelector:s=>el(s.replace(/\[data-i="\d+"\]/,''))};el('.opBud').value='10';
  let gens=0,finish;ctx.generateAndWait=()=>{gens++;return new Promise(r=>{finish=r;});};
  const button=()=>({disabled:false,innerHTML:'',isConnected:true,classList:{add(){},remove(){}}}),first=button();
  const run=ctx.launchOpp('0',first);assert.equal(gens,1);
  // Another card finishes, the sort changes or a suggestion is deleted: the list is drawn again mid-generation.
  first.isConnected=false;const drawn=ctx.oppCard(o,0,100);
  assert.match(drawn,/class="btn gold sm opGen is-busy"[^>]*disabled>(?:<span[^>]*><\/span>)?Generating/,'the redrawn card shows the draft is still being written');
  await ctx.launchOpp('0',button());assert.equal(gens,1,'a click on the redrawn card starts no second paid generation');
  calls.length=0;finish({ok:false,reason:'Fewer than four inventory-matched keywords have measured demand.'});await run;
  assert.match(toasts.pop(),/Fewer than four/,'the outcome is shown although its card was redrawn');assert(calls.includes('render'),'the card is drawn again with the outcome');
  assert.match(ctx.oppCard(o,0,100),/>Create review draft</,'once the attempt ends the card can be used again');
  delete nodes.oppCards;delete ctx.generateAndWait;});
await test('Delete waits while the draft is being written, on the card and on a redrawn card',async()=>{today='2026-10-01';
  const o=base();o.eligibility={ready:true,measuredKeywords:4};ctx.OPPS=[o];
  const els={},el=s=>els[s]||(els[s]={value:'',innerHTML:'',textContent:''});nodes.oppCards={querySelector:s=>el(s.replace(/\[data-i="\d+"\]/,''))};el('.opBud').value='10';
  let finish;ctx.generateAndWait=()=>new Promise(r=>{finish=r;});
  const del={disabled:false},btn={disabled:false,innerHTML:'',isConnected:true,classList:{add(){},remove(){}},parentNode:{querySelector:s=>s==='[data-delete-opportunity]'?del:null}};
  const run=ctx.launchOpp('0',btn);assert.equal(del.disabled,true,'the card’s Delete is disabled while its paid draft is written');
  assert.match(ctx.oppCard(o,0,100),/data-delete-opportunity="search" data-i="0" disabled/,'a redrawn card keeps Delete disabled');
  finish({ok:false,reason:'Keyword Planner is unavailable.'});await run;assert.equal(del.disabled,false,'Delete is back once the attempt ends');
  assert.doesNotMatch(ctx.oppCard(o,0,100),/data-delete-opportunity="search" data-i="0" disabled/);
  delete nodes.oppCards;delete ctx.generateAndWait;});
await test('a demand-capped plan budget is the budget the draft gets: the slider can hold it',()=>{today='2026-09-29';
  // Thin demand on cheap keywords: the engine sizes the budget down to 1 a day. A slider floor above it would raise it at launch.
  const cheap=Object.assign(research(40),{cpc:{low:0.3,high:0.9}});cheap.keywords.forEach(k=>{k.low=0.3;k.high=0.9;});
  const o=served(cheap);assert.equal(o.recommendedDailyBudget,1,'engine budget');ctx.OPPS=[o];delete ctx.OPP_UI[ctx.oppKey(0)];
  const m=/<input type="range" class="opBud" data-i="0" min="([\d.]+)" max="([\d.]+)" value="([\d.]+)"/.exec(ctx.oppCard(o,0,100));assert(m,'budget slider');
  assert(+m[1]<=+m[3]&&+m[3]<=+m[2],'min '+m[1]+' ≤ value '+m[3]+' ≤ max '+m[2]);assert.equal(+m[3],1);});
await test('a plan forecast to lose money names that blocker and never tops Best match',()=>{ctx.RESEARCH_STATUS={search:{status:'ready',checkedAt:Date.now()}};const r='Projected ROAS 0.8x (0.6–1.0x with conversion uncertainty) is below the 2.5x break-even at a 40% margin: this plan is forecast to lose CAD 120 over the run.';
  const b=ctx.oppBlock(Object.assign(base(),{eligibility:{ready:false,measuredKeywords:5,expectedSales:2,reason:r}}));assert.deepEqual([b.kind,b.label,b.text],['evidence','Forecast below break-even',r]);
  vm.runInContext(pick('oppSorters'),ctx);const loss={o:{rank:90,eligibility:{ready:false}}},ok={o:{rank:40,eligibility:{ready:true}}},old={o:{rank:60}};
  assert.deepEqual([loss,old,ok].sort(ctx.oppSorters('best')).map(x=>x.o.rank),[40,90,60],'launchable first, then by rank');});
await test('the saved Product ads funnel arrives with the research state, and a run without one clears it',()=>{const funnel={at:1,verdict:'None qualified: the Merchant Center product list could not be read.',stages:[{key:'collections',label:'Collections looked at',count:9,note:''}],skipped:[]};
  ctx.updateOpportunityState({pmaxList:[],pmaxFunnel:funnel,pmaxAt:1});assert.equal(ctx.PMAXFUNNEL.verdict,funnel.verdict);assert.equal(ctx.PMAXFUNNEL.stages[0].count,9);
  ctx.updateOpportunityState({pmaxList:[],pmaxAt:1});assert.equal(ctx.PMAXFUNNEL,null,'an older saved run has no funnel');
  ctx.updateOpportunityState({pmaxList:[],pmaxFunnel:'text'});assert.equal(ctx.PMAXFUNNEL,null,'a malformed funnel is not kept');ctx.OPPS=[];ctx.PMAXOPPS=[];});
// ---- What each Search card shows first: its type, when it runs and the ads it would run (all drawn from data; nothing is generated or paid for) ----
const face=(o,open)=>{ctx.OPPS=[o];delete ctx.OPP_UI[ctx.oppKey(0)];if(open)ctx.oppUi(0).open=open;ctx.RESEARCH_STATUS={search:{status:'ready',checkedAt:Date.now()}};return ctx.oppCard(o,0,100);};
const visual=n=>clone(vis.searchOpportunities[n]),KIND=ctx.BritesCampaignStyles.opportunityKind('search');
const foldOf=h=>{const a=h.indexOf('<details class="oppvFold"');return a<0?'':h.slice(a,h.indexOf('</details>',a));};
await test('a Search card opens with its type: the icon and plain name, a "What is this?" button and the occasion; the title and one sentence follow',()=>{today='2026-09-29';
  const o=visual(0),h=face(o);
  inOrder(h,['class="oppvHead"','class="campaignBadge"','<span>Search text ad</span>','data-oppv-what="search"','class="oppvChip"','data-oppv-help','<h4>'+o.collectionTitle+'</h4>','class="oppCardWhy"','data-oppv-when="0"','data-oppv-ads="0"','class="pmxFacts oppvFacts"','class="oppCardMetrics"','class="oppCardActions"'],'Search card');
  assert.equal(h.split('class="oppCardWhy"').length,2,'exactly one short reason on the face');
  assert.match(h,/<button type="button" class="oppvWhat" data-oppv-what="search" data-i="0" aria-label="What is this\?" aria-expanded="false" aria-controls="oppvHelp-search-0">/);
  assert.match(h,/<div class="oppvHelp" id="oppvHelp-search-0" data-oppv-help hidden>/,'the three plain lines stay closed until asked, and the button names the panel it opens');
  // The words come from the one shared vocabulary, so the console and the server describe the type the same way.
  for(const w of [KIND.name,KIND.tagline,KIND.where,KIND.suits])assert(h.includes(w),w);
  assert.equal(KIND.name,'Search text ad');assert.match(h,/<span class="oppvChip" title="[^"]*\b25\b[^"]* · CA">Christmas · 87 days<\/span>/);
  safeMarkup(h,'Search card');});
await test('the panel ids are unique per card, and the open state is remembered with the card',()=>{today='2026-09-29';
  const a=face(visual(0)),b=ctx.oppCard(visual(1),1,100);assert(a.includes('id="oppvHelp-search-0"')&&b.includes('id="oppvHelp-search-1"')&&!b.includes('id="oppvHelp-search-0"'));
  const open=face(visual(0),{what:true});assert.match(open,/aria-expanded="true" aria-controls="oppvHelp-search-0"/);assert.match(open,/<div class="oppvHelp" id="oppvHelp-search-0" data-oppv-help>/,'no hidden attribute once opened');});
await test('the "What is this?" button opens and closes its own panel with one delegated listener, and a redraw keeps the choice',()=>{today='2026-09-29';
  const from=html.indexOf("if(typeof document!=='undefined'&&document&&typeof document.addEventListener==='function')document.addEventListener('click',function(e){var b=e.target&&e.target.closest?e.target.closest('[data-oppv-what]')"),endTok='ui.open.what=open;}});',to=html.indexOf(endTok,from);
  assert(from>0&&to>from,'the listener is in the page');const listener=html.slice(from,to+endTok.length);
  const realDoc=ctx.document;let handler=null,registered=0;ctx.document={addEventListener:(t,f)=>{registered++;assert.equal(t,'click');handler=f;}};vm.runInContext(listener,ctx);ctx.document=realDoc;
  assert.equal(registered,1,'one listener for every card on both tabs');
  face(visual(0));const panel={hidden:true},card={querySelector:s=>s==='[data-oppv-help]'?panel:null},attrs={'data-oppv-what':'search','data-i':'0','aria-expanded':'false'},
    button={getAttribute:n=>attrs[n],setAttribute:(n,v)=>{attrs[n]=v;},closest:s=>s==='[data-oppv-card]'?card:button},event=target=>({target});
  handler(event({closest:s=>s==='[data-oppv-what]'?button:null}));assert.deepEqual([panel.hidden,attrs['aria-expanded'],ctx.oppUi(0).open.what],[false,'true',true]);
  assert.match(ctx.oppCard(ctx.OPPS[0],0,100),/aria-expanded="true"/,'drawn again, the card is still open');
  handler(event({closest:s=>s==='[data-oppv-what]'?button:null}));assert.deepEqual([panel.hidden,attrs['aria-expanded'],ctx.oppUi(0).open.what],[true,'false',false]);
  handler(event({closest:()=>null}));handler({target:null});assert.equal(panel.hidden,true,'a click elsewhere on the page changes nothing');});
await test('the timeline headline and a plain verdict are always on the card; the reasons are one tap away in "Why these dates"',()=>{today='2026-09-29';
  const cases=[[0,'good','Good time to start','Runs Nov 14 to Dec 18 · 35 days'],[1,'tight','Tight timing','Runs Sep 29 to Oct 17 · 19 days'],[2,'evergreen','No deadline','Runs Sep 29 to Oct 26 · 28 days'],[4,'too_short','Too late for this event','Would run Sep 29 to Oct 15 · 17 days']];
  for(const [n,verdict,word,headline] of cases){const o=visual(n),h=face(o),fold=foldOf(h),face0=h.replace(fold,'');
    assert.equal(o.schedule.verdict,verdict);assert.equal(o.schedule.headline,headline);
    assert.match(h,new RegExp('<span class="oppvVerdict is-'+verdict+'">'+word+'</span>'));assert.match(h,new RegExp('class="oppTl__head">'+headline.replace(/[.*+?^${}()|[\]\\]/g,'\\$&')+'</span>'));
    assert(h.includes('data-verdict="'+verdict+'"'));assert(h.includes('<span>When it runs</span>'));
    assert.match(fold,/^<details class="oppvFold" data-ui="when"><summary>Why these dates<\/summary>/,'the fold is closed until opened');
    for(const w of o.schedule.why)assert(fold.includes(w.replace(/&/g,'&amp;')),verdict+' reason in the fold: '+w);
    assert(fold.includes('Learning period')&&fold.includes(o.schedule.learning.note),'the learning explanation is in the fold');
    assert(!/Learning period/.test(face0),'and only there');
    safeMarkup(h,verdict);}
  const open=foldOf(face(visual(0),{when:true}));assert.match(open,/^<details class="oppvFold" data-ui="when" open>/,'a card left open stays open when it is drawn again');});
await test('the timeline follows the console date, and the occasion chip counts days from it',()=>{today='2026-10-29';const h=face(visual(0));
  assert.match(h,/Christmas · 57 days</);assert.match(h,/Today · <b>Oct 29<\/b>/);assert.match(h,/class="oppTl__head">Runs Nov 14 to Dec 18 · 35 days</,'the run itself is the engine’s');
  today='2026-12-26';assert(!face(visual(0)).includes('oppvChip'),'a passed occasion has no chip');today='2026-09-29';});
await test('the fold carries the date sources and the timeline words; the face keeps the words-only legend',()=>{today='2026-09-29';const h=face(visual(0)),fold=foldOf(h);
  assert.match(fold,/class="oppCardMeta oppvSources"[\s\S]*Occasion [^<]*<\/span>[\s\S]*verified on <a class="oppDateSrc" href="https:\/\/www\.timeanddate\.com\/holidays\/us\/christmas-day" target="_blank" rel="noopener noreferrer"/);
  assert(h.replace(fold,'').includes('<span class="oppTl__lab">Learning</span>')&&h.includes('<span class="oppTl__lab">Order cutoff</span>'),'each colour of the bar has a word');});
await test('a card saved before schedules existed keeps the old window line and draws no bar or empty box',()=>{today='2026-09-29';const o=visual(3),h=face(o);
  assert(!/data-oppv-when|When it runs|oppvFold|oppTl/.test(h),'no timeline, no fold, no placeholder');assert.match(h,/<span class="opWin" data-i="0">/);assert.match(h,/class="opDur" data-i="0">38</);
  inOrder(h,['class="oppvHead"','<h4>Charm Bracelets</h4>','class="oppCardWhy"','class="opWin"','data-oppv-ads="0"'],'legacy Search card');
  assert.match(h,/Mother's Day · 222 days/,'the chip still comes from the saved peak date');assert(h.includes('data-oppv-ads="0"'),'the ads are still shown');
  assert.match(ctx.planBlock(o,0),/<b>Run length<\/b> — Starts 45 days before the peak\./);safeMarkup(h,'legacy');});
await test('an unusable schedule is treated as none: no throw, no half-drawn bar',()=>{today='2026-09-29';
  const junk=[null,'soon',7,[],{},{version:1},{version:1,start:'nope',end:'2026-12-01',days:3},{version:1,kind:'search',start:'2026-12-10',end:'2026-12-01',days:-8,phases:[],verdict:'good'}];
  for(const s of junk){const o=visual(0);o.schedule=s;const h=face(o);assert(!/data-oppv-when|oppTl|oppvFold/.test(h),JSON.stringify(s));assert.match(h,/class="opWin"/,'the window line stands in');assert.match(h,/Christmas · 87 days/,'the chip falls back to the saved peak date');safeMarkup(h,'junk');}
  assert.match(ctx.planBlock(Object.assign(visual(0),{schedule:{}}),0),/<b>Run length<\/b> — /,'the run-length line survives too');});
await test('the run-length line quotes the schedule’s own basis when there is one',()=>{today='2026-09-29';const o=visual(0),pb=ctx.planBlock(o,0);
  assert(pb.includes('<b>Run length</b> — '+o.schedule.basis.replace(/&/g,'&amp;')),o.schedule.basis);});
await test('the ads strip is labelled, holds the Search formats and uses only words from the opportunity',()=>{today='2026-09-29';const o=visual(0),h=face(o),ads=h.slice(h.indexOf('data-oppv-ads="0"'));
  assert(ads.includes('<span>The ads this would run</span>')&&ads.includes('Examples · select one to enlarge'));assert.match(ads,/data-abp-kind="search"/);assert.equal((ads.match(/class="abp-thumb"/g)||[]).length,2,'the text ad and the same ad with its extra links');
  assert(text(ads).includes(o.keywordData[0].text),'the search shown is one of this opportunity’s keywords');assert(!/<img/.test(ads.split('class="pmxFacts')[0]),'no product photo on a text ad');});
await test('the Search themes line lists the biggest searches first, four at most, with the rest counted',()=>{today='2026-09-29';const o=visual(0),h=face(o);
  const sorted=o.keywordData.slice().sort((a,b)=>b.searches-a.searches).map(k=>k.text);assert.deepEqual(ctx.oppvSearchThemes(o),sorted);
  const row=/<span class="pmxFactLabel">Search themes<\/span><span class="pmxFactVal">([\s\S]*?)<\/span><\/div>/.exec(h.slice(h.indexOf('class="pmxFacts oppvFacts"')));assert(row,'themes row');
  assert.deepEqual([...row[1].matchAll(/<span class="pmxFactItem" title="[^"]*">([^<]*)<\/span>/g)].map(m=>m[1]),sorted.slice(0,4));assert.match(row[1],/<span class="pmxFactMore" title="birthstone initial necklace">\+1 more<\/span>/);
  const few=face(visual(4));assert(!/pmxFactMore/.test(few)&&/Search themes/.test(few),'two themes: nothing counted');
  const none=visual(0);none.keywordData=[];assert(!/Search themes/.test(face(none)),'no keywords: no empty row');
  assert.deepEqual(ctx.oppvSearchThemes({keywordData:[{text:' b ',searches:1},{text:'B',searches:9},{text:'a'},{keyword:'c',volume:5},{},null]}),['c','b','a'],'blank and repeated rows are skipped, the first spelling stays');});
await test('every function the old card had is still there: budget, dates, keywords plan, draft, delete; the score sits in the details',()=>{today='2026-09-29';const h=face(visual(0));
  for(const s of ['class="opBud"','class="opStart"','class="opEnd"','data-ui="kw"','data-ui="why"','class="btn gold sm opGen"','data-delete-opportunity="search" data-i="0"','data-ui="dx"'])assert(h.includes(s),s);
  assert(h.indexOf('class="oppScore"')>h.indexOf('data-ui="dx"'),'the score pill is inside the Review & adjust details, not on the face');});
await test('editing the dates says how they differ from the recommended run, and hides the note when they match',()=>{today='2026-09-29';const o=visual(0);ctx.OPPS=[o];
  const els={},el=s=>els[s]||(els[s]={value:'',textContent:'',title:'',min:'',hidden:true,classList:{toggle(){}}}),key=s=>s.replace(/\[data-i="\d+"\]/,''),cards={querySelector:s=>el(key(s)),querySelectorAll:s=>[el(key(s))]};
  el('.opBud').value='10';el('.opStart').value='2026-11-20';el('.opEnd').value='2026-12-10';ctx.oppRecalc(cards,0);
  assert.equal(el('.oppvMine').hidden,false);assert.match(el('.oppvMine').textContent,/^Your dates: .+ to .+ · 21 days\. The bar shows the recommended run\.$/);
  el('.opStart').value=o.schedule.start;el('.opEnd').value=o.schedule.end;ctx.oppRecalc(cards,0);assert.deepEqual([el('.oppvMine').hidden,el('.oppvMine').textContent],[true,'']);
  const legacyCard=visual(3);ctx.OPPS=[legacyCard];el('.opStart').value='2027-03-30';el('.opEnd').value='2027-04-30';ctx.oppRecalc(cards,0);assert.equal(el('.oppvMine').hidden,true,'a card with no schedule has no note to show');});
await test('hostile text in every field the visuals read stays text',()=>{today='2026-09-29';const bad='<img src=x onerror=alert(1)>',quote='" onmouseover="alert(1)" x="',o=visual(0);
  Object.assign(o,{collectionTitle:bad+'Title',occasion:bad+'Occasion',rationale:bad+'Why',keywordData:[{text:bad+'kw',real:true,searches:5},{text:quote,real:true,searches:4}],keyPhrases:[bad+'phrase']});
  Object.assign(o.schedule,{headline:bad+'Runs',basis:bad+'basis',why:[bad+'why1',quote+'why2'],learning:Object.assign({},o.schedule.learning,{note:bad+'note'})});o.schedule.event=Object.assign({},o.schedule.event,{label:bad+'Christmas',market:quote});
  o.schedule.phases.forEach(p=>{p.label=bad+p.key;});
  const h=face(o);safeMarkup(h,'hostile Search card');assert(!/<img src=x/.test(h.replace(/<img class="abp-photo"[^>]*>/g,'')));
  assert(text(h).includes(bad+'Title'),'the words are shown, not run');assert(!tags(h).some(t=>'onmouseover' in t.attrs||'x' in t.attrs),'no injected attribute');});
await test('the section headers use the type icon, and fall back to their letter when the vocabulary is missing',()=>{
  const s=ctx.oppvChannelIcon('search','S'),p=ctx.oppvChannelIcon('pmax','P');assert.match(s,/^<span class="oppChannelIcon" data-kind="search" style="--campaign-accent:#[0-9a-f]{6}"><svg /);assert.match(p,/data-kind="pmax"/);
  assert(!/>S<|>P</.test(s+p),'no letter avatar once the icon exists');
  const saved=ctx.BritesCampaignStyles;delete ctx.BritesCampaignStyles;try{assert.equal(ctx.oppvChannelIcon('search','S'),'<span class="oppChannelIcon">S</span>');assert.equal(ctx.oppvTypeHtml('search',0,false),'');
    const h=face(visual(0));assert(!h.includes('oppvType')&&h.includes('<h4>')&&h.includes('data-oppv-when'),'the card still draws without the badge');safeMarkup(h,'no vocabulary');}finally{ctx.BritesCampaignStyles=saved;}});
await test('a missing timeline or previews module only removes its own part of the card',()=>{today='2026-09-29';
  const t=ctx.BritesOppTimeline,a=ctx.BritesAdPreview;try{delete ctx.BritesOppTimeline;let h=face(visual(0));assert(!/data-oppv-when|oppTl/.test(h)&&/class="opWin"/.test(h)&&/data-oppv-ads="0"/.test(h)&&/Christmas · 87 days/.test(h));
    ctx.BritesOppTimeline=t;delete ctx.BritesAdPreview;h=face(visual(0));assert(!/oppvAds|The ads this would run/.test(h)&&/data-oppv-when="0"/.test(h));safeMarkup(h,'no previews');}finally{ctx.BritesOppTimeline=t;ctx.BritesAdPreview=a;}});
console.log(`${passed} opportunity card checks passed.`);
require('./suite-guard.cjs').done();
})().catch(e=>{console.error(e);process.exitCode=1;});
