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
for(const n of ['defCountries','oppCty','fmtDateTime','timeago'])vm.runInContext(line(n),ctx);
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
console.log(`${passed} opportunity card checks passed.`);
require('./suite-guard.cjs').done();
})().catch(e=>{console.error(e);process.exitCode=1;});
