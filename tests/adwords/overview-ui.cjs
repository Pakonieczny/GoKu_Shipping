// Overview (Command Center): figures keep their date basis and currency, campaigns with no activity fold
// away, deleted campaigns keep the money they spent, and the activity feed reads in plain words.
const path=require('path'),REPO=path.resolve(__dirname,'../..');
const fs=require('fs'),vm=require('vm'),assert=require('assert/strict');
const html=fs.readFileSync(path.join(REPO,'brites-adwords.html'),'utf8');
for(const [,js] of html.matchAll(/<script[^>]*>([\s\S]*?)<\/script>/g))new Function(js);
function pick(name){const re=new RegExp('^(?:async )?function '+name+'\\(','m'),m=re.exec(html);assert(m,name);const rest=html.slice(m.index),next=/\n(?:async )?function \w+\(/.exec(rest.slice(1));return next?rest.slice(0,next.index+1):rest;}
const ctx={console,Number,String,Object,Array,Math,JSON,Set,Map,isFinite,URL,
  DASH:{currency:'USD',budgetCurrency:'CAD',lastMetrics:[]},cmdMetrics:null,cmdReport:{currency:'USD',budgetCurrency:'CAD'},convBasis:'conversion',cmdRangeLabel:'2026-09-01 → 2026-09-28',
  esc:s=>String(s??'').replace(/[&<>]/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;'}[c])),money:n=>'$'+Number(n||0).toFixed(2),
  campDates:c=>({s:c.startDate||'',e:c.endDate||''}),statusBadge:c=>'<span class="st">'+c.status+'</span>',campaignBadgeHtml:()=>'',
  historicalBudget:()=>'recorded budget',historicalSchedule:()=>'recorded schedule',fmtMon:x=>x};
vm.createContext(ctx);vm.runInContext(html.match(/var BASIS_NAMES=\{[^}]*\};/)[0],ctx);
for(const name of ['basisName','basisLabel','CV','totals','reportNumber','cmdAttr','cmdMoney','campaignWhy','campaignIdle','idleToggleText','campaignTable','groupReportMetrics','lcStep','lcTicks','feedLabel','feedError'])vm.runInContext(pick(name),ctx);
let passed=0;function test(name,fn){fn();passed++;console.log('PASS',name);}
const plain=x=>JSON.parse(JSON.stringify(x));

test('a conversion-date figure Google did not supply stays missing instead of borrowing the ad-click figure',()=>{
  ctx.convBasis='conversion';const gone={cost:10,conv:3,value:90,convCd:null,valueCd:null},ok={cost:5,conv:1,value:5,convCd:2,valueCd:40};
  assert.deepEqual(plain(ctx.CV(gone)),{conv:null,value:null});
  const t=ctx.totals([gone,ok]);assert.equal(t.cdMissing,1);assert.equal(t.conv,2);assert.equal(t.value,40);assert.equal(t.cost,15);
  ctx.convBasis='click';assert.deepEqual(plain(ctx.CV(gone)),{conv:3,value:90});assert.equal(ctx.totals([gone,ok]).cdMissing,0);ctx.convBasis='conversion';});

const camps=[
  {id:'1',name:'Paused, nothing spent',status:'PAUSED',primaryStatus:'PAUSED',cost:0,convCd:0,valueCd:0},
  {id:'2',name:'Small spender',status:'ENABLED',primaryStatus:'ELIGIBLE',cost:5,convCd:0,valueCd:0},
  {id:'3',name:'Big spender',status:'PAUSED',primaryStatus:'PAUSED',cost:50,convCd:1,valueCd:30},
  {id:'4',name:'Deleted after spending',status:'REMOVED',primaryStatus:'REMOVED',cost:12,convCd:1,valueCd:20,deleted:true,historicalOnly:true},
  {id:'5',name:'Live, quiet so far',status:'ENABLED',primaryStatus:'ELIGIBLE',cost:0}];
const table=ctx.campaignTable(camps);

test('campaigns are listed busiest first and the ones with no activity fold behind one labelled control',()=>{
  const busy=table.slice(0,table.indexOf('class="idleToggle"')),order=[...busy.matchAll(/<tr class="crow"[^>]*data-cid="(\d+)"/g)].map(m=>m[1]);
  assert.deepEqual(order,['3','4','2','5']);
  assert.match(table,/<tbody class="idleRows" hidden>[\s\S]*data-cid="1"/);
  assert.match(table,/Show 1 campaign with no activity in these dates/);});

test('a deleted campaign that spent keeps its row, reads Removed with the reason, and offers no edits',()=>{
  assert.match(table,/data-cid="4"[\s\S]*?<span class="st">REMOVED<\/span><small class="statusWhy">Deleted in Autopilot<\/small>/);
  assert.doesNotMatch(table,/class="btn ghost sm (bge|sce|cst)" data-id="4"/);
  const spend=[...table.matchAll(/data-label="Spend · USD">\$([\d.]+)</g)].reduce((a,m)=>a+Number(m[1]),0);
  assert.equal(spend,ctx.totals(camps).cost,'the table adds up to the account total');});

test('every money column names its currency and a missing conversion-date figure shows a dash',()=>{
  assert.match(table,/<th class="r">Spend <small>USD<\/small><\/th>/);
  const h=ctx.campaignTable([{id:'9',name:'No conversion dates',status:'ENABLED',primaryStatus:'ELIGIBLE',cost:10,conv:2,value:50,convCd:null,valueCd:null}]);
  assert.match(h,/data-label="Conv\. value · USD">—</);assert.match(h,/data-label="Conversions">—</);assert.doesNotMatch(h,/\$50/);});

test('group figures use the campaign row currency and basis, and say when they fall back to ad-click date',()=>{
  ctx.convBasis='conversion';const g={metrics:{spend:100,value:0,conversions:0},report:{currency:'USD',click:{spend:72,value:10,conversions:1},conversion:{spend:72,value:20,conversions:2}}};
  let x=ctx.groupReportMetrics(g,'CAD');assert.equal(x.currency,'USD');assert.equal(x.m.value,20);assert.equal(x.click,false);
  g.report.conversion=null;x=ctx.groupReportMetrics(g,'CAD');assert.equal(x.m.value,10);assert.equal(x.click,true);assert.ok(!x.native);
  x=ctx.groupReportMetrics({metrics:{spend:100}},'CAD');assert.equal(x.native,true);assert.equal(x.currency,'CAD');});

test('chart axes step on round numbers and count axes on whole numbers',()=>{
  assert.deepEqual(plain(ctx.lcTicks(0,283,4,false)),[0,100,200,300]);
  assert.deepEqual(plain(ctx.lcTicks(0,3,4,true)),[0,1,2,3]);});

test('activity entries read in plain words with the campaign name',()=>{
  ctx.DASH.lastMetrics=[{id:'77',name:'Fixture campaign'}];
  assert.equal(ctx.feedLabel({label:'setStatus:PAUSED'}),'Campaign paused');
  assert.equal(ctx.feedLabel({label:'delete-campaign:77'}),'Campaign deleted · Fixture campaign');
  assert.equal(ctx.feedLabel({kind:'enforceBudgetCeiling',trimmed:2,total:80,ceiling:60}),'Budgets trimmed to the daily ceiling · 2 changed · total was $80.00 CAD, ceiling $60.00 CAD');
  assert.equal(ctx.feedError('{"error":{"code":400,"message":"Budget amount is too low."}}'),'Budget amount is too low.');});

test('the daily chart subtitle sums unrounded daily figures, so it matches the tiles',()=>{
  const body=pick('renderDailyCharts');assert.match(body,/spend=ser\.map\(function\(r\)\{return \+r\.cost\|\|0;\}\)/);assert.match(body,/money\(tSpend\)/);});

console.log(passed+' Overview checks passed.');
