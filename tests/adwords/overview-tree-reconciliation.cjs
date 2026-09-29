// Overview campaign → group → listing tree, end to end on a synthetic account (no live data, no network):
// the server's campaign rows (metricsRange), group report (adGroups, reportingTree) and product report
// (dailyStats) feed the real Overview code. Every campaign row equals its group rows plus "Not tied to a
// group", and every group row equals its listing rows plus "Not tied to a listing", for Search and
// Performance Max, by ad-click date and by conversion date, in the campaign rows' currency at the same daily
// rates. Removed groups (and groups of removed or deleted campaigns) that had activity keep their rows.
// The activity feed reads the ledger by the chosen dates, one page at a time, and says what it shows.
const fs=require('fs'),vm=require('vm'),path=require('path'),assert=require('assert/strict'),{JSDOM}=require('jsdom');
const REPO=path.resolve(__dirname,'../..'),filepath=path.join(REPO,'netlify/functions/googleAdsAutopilot.js');
const NOW=Date.parse('2026-09-10T16:00:00Z');class FixedDate extends Date{constructor(...a){super(...(a.length?a:[NOW]));}static now(){return NOW;}}
const cx={module:{exports:{}},exports:{},require:n=>n==='node-fetch'?(async()=>{throw Error('network forbidden');}):require('module').createRequire(filepath)(n),process:{env:{GADS_CUSTOMER_ID:'123'}},console,Buffer,Date:FixedDate,Intl,Map,Set,URL,setTimeout,clearTimeout};
vm.createContext(cx);vm.runInContext(fs.readFileSync(filepath,'utf8')+`\nmodule.exports.test={COL,set:v=>{if(v.gaql)gaql=v.gaql;if(v.fx)_fxRateToUsd=v.fx;if(v.fb!==undefined)_fb=v.fb;if(v.deleted)_deletedCampaignIds=v.deleted;}};`,cx);
const E=cx.module.exports,T=E.test;let passed=0;const clone=x=>JSON.parse(JSON.stringify(x));
async function test(name,fn){await fn();passed++;console.log('PASS',name);}

// ---------- the synthetic account (CAD), two days at different daily rates ----------
const D1='2026-09-08',D2='2026-09-09',RATE={[D1]:0.7,[D2]:0.8},range={start:D1,end:D2};
const P={id:'501',name:'PMax · Fruit charms',status:'ENABLED',advertisingChannelType:'PERFORMANCE_MAX'},S={id:'502',name:'Search · Charms',status:'ENABLED',advertisingChannelType:'SEARCH'},
  R={id:'503',name:'PMax · Retired',status:'REMOVED',advertisingChannelType:'PERFORMANCE_MAX'},Q={id:'504',name:'Search · Overflow',status:'ENABLED',advertisingChannelType:'SEARCH'};
const ag=(id,name,status,campaign)=>({kind:'pmax',campaign,group:{resourceName:'customers/123/assetGroups/'+id,name,status,finalUrls:[]}}),adg=(id,name,status,campaign)=>({kind:'search',campaign,group:{resourceName:'customers/123/adGroups/'+id,name,status}});
const G={live:ag(71,'Fruit pool','ENABLED',P),old:ag(72,'Old pool','REMOVED',P),retired:ag(73,'Retired pool','ENABLED',R),peach:adg(81,'Peach searches','ENABLED',S),plum:adg(82,'Plum searches','REMOVED',S),listed:adg(84,'Listed group','ENABLED',Q),overflow:adg(83,'Overflow group','ENABLED',Q)};
// One day of Google's figures in CAD: cost, clicks, impressions, then conversions and value by ad-click date and by conversion date.
const m=(cost,clicks,impr,conv,value,convCd,valueCd)=>({cost,clicks,impr,conv,value,convCd,valueCd});
const offers=[[G.live,'shopify_US_1_11','Peach Charm',D1,m(4,2,40,1,30,0,0)],[G.live,'shopify_US_1_11','Peach Charm',D2,m(6,3,50,0,0,1,28)],[G.live,'shopify_US_2_21','Plum Charm',D2,m(5,2,30,1,25,1,25)],
  [G.old,'shopify_US_3_31','Pear Charm',D1,m(3,1,20,1,20,1,22)],[G.retired,'shopify_US_4_41','Kiwi Charm',D1,m(2,1,9,0,0,1,18)]];
const productOnly=[[G.live,'shopify_US_9_91','Fig Charm',D2,m(2.5,1,10,0,0,0.5,6)]]; // in the campaign's product report, not in any group's
const placements=[[G.live,D1,m(1.5,1,100,0.5,10,0,0)],[G.live,D2,m(2,1,80,0,0,1,12)]]; // PMax activity with no product
const ads=[[G.peach,'https://britesjewelry.com/products/peach-charm?utm_source=ads',D1,m(2,2,30,1,30,0,0)],[G.peach,'https://britesjewelry.com/products/peach-charm?utm_source=ads',D2,m(3,2,25,0,0,1,30)],
  [G.peach,'https://britesjewelry.com/products/peach-charm',D2,m(1,1,5,0,0,0,0)],[G.peach,'https://britesjewelry.com/collections/fruit',D1,m(1.2,1,12,0,0,0.5,8)],
  [G.plum,'https://britesjewelry.com/products/plum-charm',D1,m(2,1,10,1,25,1,25)],[G.listed,'https://britesjewelry.com/products/fig-charm',D2,m(1.5,1,7,0,0,0,0)],[G.overflow,'https://britesjewelry.com/products/kiwi-charm',D1,m(0.9,1,4,0,0,0,0)]];
const unassigned=[[P,D2,m(1,0,5,0,0,0,0)]]; // campaign activity Google reports under no group
const add=(a,b)=>a?m(a.cost+b.cost,a.clicks+b.clicks,a.impr+b.impr,a.conv+b.conv,a.value+b.value,a.convCd+b.convCd,a.valueCd+b.valueCd):b;
const groupDays=new Map(),campaignDays=new Map(),put=(map,key,row,x)=>{const prev=map.get(key);map.set(key,{...row,m:add(prev&&prev.m,x)});};
offers.concat(productOnly).forEach(([g,,,d,x])=>put(groupDays,g.group.resourceName+d,{g,date:d},x));placements.forEach(([g,d,x])=>put(groupDays,g.group.resourceName+d,{g,date:d},x));ads.forEach(([g,,d,x])=>put(groupDays,g.group.resourceName+d,{g,date:d},x));
[...groupDays.values()].forEach(({g,date,m:x})=>put(campaignDays,g.campaign.id+date,{c:g.campaign,date},x));unassigned.forEach(([c,d,x])=>put(campaignDays,c.id+d,{c,date:d},x));
const metrics=(x,q)=>Object.assign({costMicros:String(Math.round(x.cost*1e6)),clicks:String(x.clicks),impressions:String(x.impr),conversions:x.conv,conversionsValue:x.value},q.includes('by_conversion_date')?{conversionsByConversionDate:x.convCd,conversionsValueByConversionDate:x.valueCd}:{});
const live=g=>g.group.status!=='REMOVED'&&g.campaign.status!=='REMOVED';
let queries=[];
const gaql=async q=>{queries.push(q);const sel=q.split(/\sFROM\s/)[0],has=f=>sel.includes(f),from=v=>q.includes('FROM '+v+' ')||q.endsWith('FROM '+v);
  if(q.includes('customer.time_zone'))return [{customer:{timeZone:'America/Toronto',currencyCode:'CAD'}}];
  if(has('campaign.start_date'))return [];
  if(has('campaign_budget.amount_micros'))return [P,S,R,Q].filter(c=>!q.includes("campaign.status != 'REMOVED'")||c.status!=='REMOVED').map(c=>({campaign:{...c,primaryStatus:c.status==='REMOVED'?'REMOVED':'ELIGIBLE',primaryStatusReasons:[]},campaignBudget:{resourceName:'customers/123/campaignBudgets/'+c.id,amountMicros:'10000000'}}));
  if(from('shopping_performance_view'))return q.includes("'PURCHASE'")?[]:offers.concat(productOnly).map(([g,item,title,d,x])=>({campaign:{id:g.campaign.id,name:g.campaign.name},segments:{date:d,productItemId:item,productTitle:title,productMerchantId:'78',productFeedLabel:'US',productLanguage:'en',productChannel:'ONLINE',productCountry:'geoTargetConstants/2840'},metrics:metrics(x,q)}));
  if(from('asset_group_product_group_view'))return offers.map(([g,item,,d,x])=>({assetGroup:{resourceName:g.group.resourceName},assetGroupListingGroupFilter:{caseValue:{productItemId:{value:item}}},segments:{date:d},metrics:metrics(x,q)}));
  if(from('asset_group_listing_group_filter'))return ['shopify_US_1_11','shopify_US_2_21'].map(value=>({assetGroupListingGroupFilter:{assetGroup:G.live.group.resourceName,type:'UNIT_INCLUDED',caseValue:{productItemId:{value}}}}));
  // Named groups, removed or not (how the tree names groups Google no longer lists).
  if(q.includes('.resource_name IN (')){const asked=[...q.matchAll(/'(customers\/[^']+)'/g)].map(x=>x[1]);return Object.values(G).filter(g=>asked.includes(g.group.resourceName)).map(g=>g.kind==='pmax'?{campaign:g.campaign,assetGroup:g.group}:{campaign:g.campaign,adGroup:g.group});}
  // Live structure: removed groups and removed campaigns are filtered out, and the overflow group is missing as from a truncated list.
  if(from('asset_group')&&!has('metrics.'))return [G.live,G.old,G.retired].filter(live).map(g=>({campaign:g.campaign,assetGroup:{...g.group,primaryStatus:'ELIGIBLE'}}));
  if(from('ad_group')&&!has('metrics.'))return [G.peach,G.plum,G.listed].filter(live).map(g=>({campaign:g.campaign,adGroup:{...g.group,primaryStatus:'ELIGIBLE'}}));
  if(from('ad_group_ad')&&!has('metrics.'))return ads.filter(([g])=>live(g)).map(([g,url])=>({adGroup:{resourceName:g.group.resourceName},adGroupAd:{ad:{finalUrls:[url]}}}));
  // Dated metrics include every entity that had activity, removed or not.
  if(from('ad_group_ad'))return has('segments.date')?ads.map(([g,url,d,x])=>({adGroup:{resourceName:g.group.resourceName},adGroupAd:{ad:{finalUrls:[url]}},segments:{date:d},metrics:metrics(x,q)})):[];
  if(from('asset_group'))return has('segments.date')?[...groupDays.values()].filter(r=>r.g.kind==='pmax').map(({g,date,m:x})=>({campaign:g.campaign,assetGroup:g.group,segments:{date},metrics:metrics(x,q)})):[];
  if(from('ad_group'))return [...groupDays.values()].filter(r=>r.g.kind==='search').map(({g,date,m:x})=>({campaign:g.campaign,adGroup:g.group,segments:{date},metrics:metrics(x,q)}));
  if(from('campaign')&&has('segments.date'))return [...campaignDays.values()].map(({c,date,m:x})=>({campaign:c,segments:{date},metrics:metrics(x,q)}));
  return [];};

// ---------- the Overview code, in a browser-like page ----------
const html=fs.readFileSync(path.join(REPO,'brites-adwords.html'),'utf8');
function pick(name){const m=new RegExp('^(?:async )?function '+name+'\\(','m').exec(html);assert(m,name);const rest=html.slice(m.index),next=/\n(?:async )?function \w+\(/.exec(rest.slice(1));return next?rest.slice(0,next.index+1):rest;}
const dom=new JSDOM('<main id="host"></main><span id="feedSub"></span><span id="feedRangeMount"></span><div id="feed"></div>'),document=dom.window.document;
const styles=require(path.join(REPO,'brites-campaign-styles'));let uiApi=async()=>{throw Error('no request expected');},uiCalls=[];
const ui={document,URL,Map,Set,Date:FixedDate,console,setTimeout,encodeURIComponent,convBasis:'click',DASH:{lastMetrics:[],currency:'USD',budgetCurrency:'CAD'},cmdReport:null,cmdMetrics:null,DAILY:null,cmdRangeLabel:'Sep 8 – Sep 9',
  esc:s=>String(s??'').replace(/[&<>"']/g,x=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[x])),money:n=>'$'+Number(n).toFixed(6), // full precision, so the figures read back add exactly
  campaignPipeline:x=>x.channel,campDates:()=>({s:'',e:''}),statusBadge:c=>'<span>'+c.status+'</span>',dailyRangeYmd:()=>range,api:(a,x)=>{uiCalls.push([a,x]);return uiApi(a,x);},
  BritesCampaignStyles:styles,campaignBadgeHtml:(x,o)=>styles.badge(x,o),campaignStyleIcon:(k,s)=>styles.iconSvg(styles.describe(k).icon,s),
  $:s=>document.querySelector(s),rangePicker:()=>'',wireRangePickers:()=>{},fmtDateTime:d=>d.toISOString(),fmtMon:x=>x};
vm.createContext(ui);vm.runInContext(html.match(/var BASIS_NAMES=\{[^}]*\};/)[0]+html.match(/var feedRange=\{[^}]*\};/)[0]+html.match(/var FEED=\{[^\n]*\};/)[0],ui);
vm.runInContext(html.slice(html.indexOf('function reportNumber('),html.indexOf('function renderTimeline(')),ui);
for(const name of ['CV','basisName','basisLabel','rpYmd','rpParse','rpRangeFor','rpResolve','feedLabel','feedError','timeago','feedBounds','feedKey','feedAppend','feedAdopt','loadFeed','renderFeed'])vm.runInContext(pick(name),ui);
const flush=async()=>{for(let i=0;i<20;i++)await new Promise(r=>setImmediate(r));};
const num=el=>{const t=el.textContent.trim();return t==='—'?null:Number(t.replace(/[^0-9.\-]/g,''));};
const figs=el=>{const b=[...el.querySelectorAll(':scope > .reportMetrics b')].map(num);return {spend:b[0],value:b[1],conversions:b[2]};};
const near=(a,b,msg)=>assert(a!=null&&b!=null&&Math.abs(a-b)<1e-5,msg+': '+a+' vs '+b);
const sum=list=>list.reduce((a,f)=>({spend:a.spend+f.spend,value:a.value+(f.value??NaN),conversions:a.conversions+(f.conversions??NaN)}),{spend:0,value:0,conversions:0});

(async()=>{
  T.set({gaql,fb:false,fx:async d=>RATE[d],deleted:async()=>new Set(['503'])}); // campaign 503 was deleted from this console
  const report=await E.metricsRange(range),daily=await E.dailyStats(range),tree=await E.adGroups({...range,reportingTree:true});
  const group=ref=>tree.groups.find(g=>g.ref===ref);

  await test('removed groups, and groups of removed or deleted campaigns, keep the activity they had, flagged, in the campaign rows’ shape',()=>{
    for(const g of [G.old,G.retired,G.plum]){const x=group(g.group.resourceName);assert(x,'missing '+g.group.name);assert.equal(x.historicalOnly,true);assert.equal(x.serving,'Removed');
      assert.equal(x.report.currency,'USD');assert(x.report.click&&x.report.conversion,'both bases');assert.deepEqual(Object.keys(x.report.click).sort(),['clicks','conversions','impressions','spend','value']);}
    assert.equal(group(G.retired.group.resourceName).campaignStatus,'REMOVED');assert.equal(group(G.old.group.resourceName).mapping.exactOfferScope,false,'a removed group’s past listings are never read as an exact product scope');
    assert.deepEqual(group(G.plum.group.resourceName).listingMetrics.map(l=>l.url),['https://britesjewelry.com/products/plum-charm'],'a removed Search group keeps the page its ads led to');
    assert(!group(G.live.group.resourceName).historicalOnly&&!group(G.peach.group.resourceName).historicalOnly);
    near(group(G.old.group.resourceName).report.conversion.value,22*0.7,'a removed group’s conversion-date value at its day’s rate');
    assert(!group(G.overflow.group.resourceName),'an unlisted live group is never invented');assert(tree.warnings.some(w=>/activity for 1 group it did not list/.test(w)));
    assert.equal(tree.includesRemovedWithActivity,true);assert.equal(daily.products.every(p=>p.report&&p.report.currency==='USD'&&p.report.conversion),true);
    near(daily.products.find(p=>p.itemId==='shopify_US_1_11').report.click.spend,4*0.7+6*0.8,'a listing’s spend at each day’s rate');
    near(daily.products.find(p=>p.itemId==='shopify_US_1_11').report.conversion.value,28*0.8,'a listing’s conversion-date value at its day’s rate');});

  await test('batched reads only: one per report, and one per channel to name the groups Google no longer lists',async()=>{
    queries=[];await E.adGroups({...range,reportingTree:true,force:true});const reads=queries.filter(q=>!q.includes('customer.time_zone'));
    assert.equal(reads.length,10);assert.equal(reads.filter(q=>q.includes('.resource_name IN (')).length,2);
    queries=[];const plain=await E.adGroups({...range,force:true});assert(!plain.groups.some(g=>g.historicalOnly),'other views list live groups only');assert(!queries.some(q=>q.includes('.resource_name IN (')));});

  ui.cmdReport=clone(report);ui.cmdMetrics=ui.cmdReport.snapshot;ui.DAILY=clone(daily);uiApi=async a=>{assert.equal(a,'adGroups');return clone(tree);};
  const host=document.querySelector('#host');
  async function render(basis){ui.convBasis=basis;host.innerHTML=ui.campaignTable(ui.cmdMetrics);ui.wireCampRows(host);
    for(const box of host.querySelectorAll('.adlvl'))await ui.loadCampaignTree(box);
    for(const el of host.querySelectorAll('.reportGroup[data-group-ref]')){if(!el.open){el.open=true;el.dispatchEvent(new dom.window.Event('toggle'));}}await flush();}
  const campaignRow=id=>{const tds=host.querySelectorAll('.crow[data-cid="'+id+'"] td');return {spend:num(tds[2]),value:num(tds[3]),conversions:num(tds[4])};};
  const tree_=id=>host.querySelector('.adlvl[data-c="'+id+'"]');
  function reconcile(basis){
    for(const c of [P,S,R,Q]){const box=tree_(c.id),row=campaignRow(c.id),groups=[...box.querySelectorAll('.reportGroup[data-group-ref]')],gap=box.querySelector('.reportGap');
      const total=sum(groups.map(el=>figs(el.querySelector('summary'))).concat(gap?[figs(gap)]:[]));
      for(const k of ['spend','value','conversions'])near(total[k],row[k],c.name+' '+basis+' '+k+': groups + not tied to a group = campaign row');
      for(const el of groups){const g=figs(el.querySelector('summary')),rows=[...el.querySelectorAll('.reportListings > article.reportListing')];assert(rows.length,'every group lists its listings or its remainder');
        assert(!el.querySelector('.reportListings').textContent.includes('By ad-click date'),'every listing is on the group’s own basis');
        const l=sum(rows.map(figs));for(const k of ['spend','value','conversions'])near(l[k],g[k],c.name+' / '+el.querySelector('b').textContent+' '+basis+' '+k+': listings + not tied to a listing = group row');}}}

  await test('by ad-click date: campaign = groups (+ not tied to a group) = listings (+ not tied to a listing), for Search and Performance Max',async()=>{
    await render('click');reconcile('click');
    const gap=tree_(P.id).querySelector('.reportGap');assert(gap&&gap.textContent.includes('Not tied to a group'));near(figs(gap).spend,1*0.8,'PMax activity under no group');
    near(figs(tree_(Q.id).querySelector('.reportGap')).spend,0.9*0.7,'an unlisted group’s activity stays in the campaign as not tied to a group');
    assert(!tree_(S.id).querySelector('.reportGap'),'a campaign whose groups hold everything has no remainder row');
    const peach=tree_(S.id).querySelector('[data-group-ref="'+G.peach.group.resourceName+'"]'),rest=peach.querySelector('.reportRest');
    assert(rest.textContent.includes('Not tied to a listing')&&rest.textContent.includes('Ads that do not lead to one product page'));near(figs(rest).spend,1.2*0.7,'the collection-page ad');
    assert.equal(peach.querySelectorAll('.reportListing:not(.reportRest)').length,1,'two ads to one product page are one listing');near(figs(peach.querySelector('.reportListing')).spend,2*0.7+3*0.8+1*0.8,'the product page’s ads at their days’ rates');
    const fruit=tree_(P.id).querySelector('[data-group-ref="'+G.live.group.resourceName+'"]'),other=fruit.querySelector('.reportRest');
    assert(other.textContent.includes('Other activity'));near(figs(other).spend,2.5*0.8+1.5*0.7+2*0.8,'the fig listing Google reports only per campaign, plus placements with no product');near(figs(other).value,10*0.7,'their value');
    assert(tree_(P.id).querySelector('.reportProducts').textContent.includes('Fig Charm')&&tree_(P.id).querySelector('.reportProducts').textContent.includes('already counted in the groups above'));
    assert(tree_(P.id).textContent.includes('2 groups (1 removed)')&&tree_(S.id).textContent.includes('2 groups (1 removed)')&&tree_(R.id).textContent.includes('1 group (1 removed)'));
    for(const [c,g] of [[P,G.old],[S,G.plum],[R,G.retired]])assert(tree_(c.id).querySelector('[data-group-ref="'+g.group.resourceName+'"] summary small').textContent.includes('Removed'),g.group.name+' is marked removed');
    assert(fruit.textContent.includes('USD by ad-click date, like the group; the listings and the last row add up to it.'));});

  await test('by conversion date: the same tree adds up on conversion-date figures at the same daily rates',async()=>{
    await render('conversion');reconcile('conversion');
    const fruit=tree_(P.id).querySelector('[data-group-ref="'+G.live.group.resourceName+'"]'),other=fruit.querySelector('.reportRest');
    near(figs(other).conversions,0.5+1,'fig listing and placements, by conversion date');near(figs(other).value,6*0.8+12*0.8,'their conversion-date value');
    const peach=tree_(S.id).querySelector('[data-group-ref="'+G.peach.group.resourceName+'"]');near(figs(peach.querySelector('.reportRest')).value,8*0.7,'the collection-page ad’s conversion-date value');
    assert(fruit.textContent.includes('USD by conversion date, like the group'));assert(tree_(P.id).querySelector('.reportProducts').textContent.includes('by conversion date'));});

  // ---------- the activity feed ----------
  const ledger=new Map(),ts=ms=>({toMillis:()=>ms});
  for(let i=0;i<120;i++)ledger.set('recent'+String(i).padStart(3,'0'),{kind:'mutate',label:'setBudget:12',ok:true,at:ts(NOW-i*1800000)});
  for(let i=0;i<5;i++)ledger.set('older'+i,{kind:'mutate',label:'setStatus:PAUSED',ok:true,at:ts(NOW-20*86400000-i*3600000)});
  T.set({fb:{db:fakeDb({[T.COL.ledger]:ledger})}});
  const page=x=>E.dashboard({activityOnly:true,activity:x});

  await test('the server reads the ledger by the chosen dates, newest first, 50 at a time, reaching every entry once',async()=>{
    let r=await page({from:null,to:null}),all=[...r.activity.entries];assert.equal(r.activity.entries.length,50);assert.equal(r.activity.next,'recent049');
    while(r.activity.next){r=await page({from:null,to:null,before:r.activity.next});all.push(...r.activity.entries);}
    assert.equal(all.length,125);assert.equal(new Set(all.map(x=>x.id)).size,125);assert(all.every((x,i)=>!i||all[i-1].at>=x.at),'newest first');
    const day=(await page({from:NOW-86400000,to:NOW})).activity;assert.equal(day.entries.length,49);assert.equal(day.next,null);
    await assert.rejects(()=>page({from:5,to:1}),/valid activity dates/);await assert.rejects(()=>page({before:'../x'}),/Invalid activity page/);await assert.rejects(()=>page({before:'gone'}),/activity log changed/);
    const full=await E.dashboard({activity:{from:NOW-86400000,to:NOW}});assert.equal(full.activity.entries.length,49);assert.equal(full.recentLedger.length,0);
    assert.equal((await E.dashboard({})).recentLedger.length,20,'the plain Kick console keeps its latest 20');});

  await test('the feed shows every entry in its dates with “Show older entries”, says exactly what it shows, and waits with a labelled spinner',async()=>{
    uiApi=async(a,x)=>{assert.equal(a,'dashboard');assert.equal(x.activityOnly,true);return clone(await E.dashboard(x));};uiCalls=[];
    const feed=document.querySelector('#feed'),sub=document.querySelector('#feedSub'),more=()=>feed.querySelector('.feedMore');
    ui.renderFeed();assert(feed.querySelector('[role=status] .spin')&&feed.textContent.includes('Loading activity for these dates…'));await flush();
    assert.equal(feed.querySelectorAll('.feed__i').length,50);assert.equal(sub.textContent,'latest 50 in these dates');assert.equal(more().textContent,'Show older entries');
    more().click();assert(more().disabled&&more().textContent.includes('Loading older entries…'));await flush();assert.equal(feed.querySelectorAll('.feed__i').length,100);
    more().click();await flush();assert.equal(feed.querySelectorAll('.feed__i').length,120);assert.equal(sub.textContent,'120 entries in these dates');assert(!more(),'nothing older in these dates');
    ui.feedRange={preset:'30d'};ui.renderFeed();await flush();assert.equal(sub.textContent,'latest 50 in these dates');for(let i=0;i<2;i++){more().click();await flush();}
    assert.equal(sub.textContent,'125 entries in these dates');assert.equal(feed.querySelectorAll('.feed__i').length,125);
    ui.feedRange={preset:'all'};ui.renderFeed();await flush();assert.equal(sub.textContent,'latest 50');
    assert(uiCalls.every(([a,x])=>a==='dashboard'&&x.activityOnly),'the feed reads only the log');});

  await test('a dashboard refresh brings the first page without a second request and keeps older pages the viewer opened',async()=>{
    const feed=document.querySelector('#feed'),sub=document.querySelector('#feedSub');ui.feedRange={preset:'7d'};
    ui.DASH=Object.assign({},ui.DASH,{activity:clone((await page(ui.feedBounds())).activity)});uiCalls=[];ui.renderFeed();assert.equal(uiCalls.length,0);assert.equal(sub.textContent,'latest 50 in these dates');
    feed.querySelector('.feedMore').click();await flush();assert.equal(feed.querySelectorAll('.feed__i').length,100);
    for(let i=1;i<=3;i++)ledger.set('new'+i,{kind:'mutate',label:'setStatus:ENABLED',ok:true,at:ts(NOW+i*60000)});
    ui.DASH=Object.assign({},ui.DASH,{activity:clone((await page(ui.feedBounds())).activity)});uiCalls=[];ui.renderFeed();assert.equal(uiCalls.length,0);
    assert.equal(sub.textContent,'latest 103 in these dates');const ids=[...ui.FEED.entries].map(x=>x.id);assert.equal(new Set(ids).size,103);assert(ids.slice(0,3).every(x=>/^new/.test(x)));
    feed.querySelector('.feedMore').click();await flush();assert.equal(sub.textContent,'123 entries in these dates');});

  await test('a feed that cannot load says why and offers to try again',async()=>{
    const feed=document.querySelector('#feed');let fail=true;uiApi=async(a,x)=>{if(fail){fail=false;throw Error('Firestore is unavailable.');}return clone(await E.dashboard(x));};
    ui.feedRange={preset:'14d'};ui.renderFeed();await flush();assert(feed.querySelector('[role=alert]').textContent.includes('Firestore is unavailable.'));
    feed.querySelector('#feedRetry').click();await flush();assert.equal(feed.querySelectorAll('.feed__i').length,50);});

  console.log(passed+' Overview tree and activity feed checks passed.');
})().catch(e=>{console.error(e);process.exit(1);});

// A small in-memory Firestore: equality, in and range filters, one orderBy, startAfter a document, limit.
function fakeDb(data){const val=x=>x&&typeof x.toMillis==='function'?x.toMillis():x instanceof Date?x.getTime():x;
  const snap=(id,d)=>({id,exists:d!==undefined,data:()=>d,ref:{id}});
  const collection=name=>{const store=data[name]||(data[name]=new Map());
    const query=(filters,order,after,lim)=>({where:(f,op,v)=>query(filters.concat([[f,op,v]]),order,after,lim),orderBy:(f,dir)=>query(filters,[f,dir||'asc'],after,lim),startAfter:s=>query(filters,order,s,lim),limit:n=>query(filters,order,after,n),select:()=>query(filters,order,after,lim),
      get:async()=>{const ok=(d,[f,op,v])=>{const a=val(d[f]),b=Array.isArray(v)?v:val(v);return op==='=='?a===b:op==='in'?b.includes(a):a==null?false:op==='>='?a>=b:op==='<='?a<=b:op==='>'?a>b:op==='<'?a<b:false;};
        let docs=[...store.entries()].filter(([,d])=>filters.every(f=>ok(d,f))).map(([id,d])=>snap(id,d));
        if(order){const [f,dir]=order,s=dir==='desc'?-1:1;docs.sort((x,y)=>(val(x.data()[f])-val(y.data()[f]))*s||(x.id<y.id?-1:x.id>y.id?1:0)*s);}
        if(after){const i=docs.findIndex(d=>d.id===after.id);docs=docs.slice(i+1);}if(lim!=null)docs=docs.slice(0,lim);
        return {docs,size:docs.length,empty:!docs.length,forEach:fn=>docs.forEach(fn)};}});
    return Object.assign(query([],null,null,null),{doc:id=>({id,get:async()=>snap(id,store.get(id)),collection:sub=>collection(name+'/'+id+'/'+sub)})});};
  return {collection,batch:()=>({delete(){},set(){},commit:async()=>{}})};}
