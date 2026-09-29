// Removed groups, and the groups of removed or deleted campaigns, that spent money in the dates stay in the reporting
// tree as history, so every campaign's groups add up to its Overview row. Group states read in plain words.
const fs=require('fs'),vm=require('vm'),assert=require('assert'),path=require('path');
const {createGroupsService}=require('../../netlify/functions/googleAdsGroups');
const filepath=path.resolve(__dirname,'../../netlify/functions/googleAdsAutopilot.js');
const NOW=Date.parse('2026-09-10T16:00:00Z');class FixedDate extends Date{constructor(...a){super(...(a.length?a:[NOW]));}static now(){return NOW;}}
const cx={module:{exports:{}},exports:{},require:n=>n==='node-fetch'?(async()=>{throw Error('network forbidden')}):require('module').createRequire(filepath)(n),process:{env:{GADS_CUSTOMER_ID:'123'}},console,Buffer,Date:FixedDate,Intl,Map,Set,URL,URLSearchParams,setTimeout,clearTimeout};
vm.createContext(cx);vm.runInContext(fs.readFileSync(filepath,'utf8')+`\nmodule.exports.test={rates:_reportRates,set:v=>{if(v.gaql)gaql=v.gaql;if(v.fx)_fxRateToUsd=v.fx;if(v.fb!==undefined)_fb=v.fb;if(v.deleted)_deletedCampaignIds=v.deleted;}};`,cx);
const E=cx.module.exports,T=E.test;let count=0;const check=(v,m)=>{assert(v,m);count++;},near=(a,b)=>Math.abs(a-b)<1e-9;
const AG=n=>'customers/123/assetGroups/'+n,ADG=n=>'customers/123/adGroups/'+n;
const camp={P:{id:'42',name:'Necklaces',status:'ENABLED',advertisingChannelType:'PERFORMANCE_MAX'},R:{id:'60',name:'Old bracelets',status:'REMOVED',advertisingChannelType:'PERFORMANCE_MAX'},S:{id:'50',name:'Rings search',status:'ENABLED',advertisingChannelType:'SEARCH'},Z:{id:'70',name:'Deleted search',status:'REMOVED',advertisingChannelType:'SEARCH'},Y:{id:'80',name:'Being deleted',status:'PAUSED',advertisingChannelType:'SEARCH'},N:{id:'90',name:'Ended charms',status:'ENABLED',advertisingChannelType:'PERFORMANCE_MAX'}};
// Every group: its campaign, Google status, whether Google still lists it, and its daily figures.
const G={
  A:{ref:AG(1),c:'P',status:'ENABLED',live:true,primary:'ELIGIBLE',days:[['2026-09-09',10,2,50,1,30,5,100],['2026-09-10',20,1,20,3,70,3,80]]},
  W:{ref:AG(9),c:'P',status:'ENABLED',live:true,primary:'NOT_ELIGIBLE',reasons:['ASSET_GROUP_DISAPPROVED'],days:[]},
  B:{ref:AG(2),c:'P',status:'REMOVED',days:[['2026-09-10',4,0,0,1,10,2,30]]},
  D:{ref:AG(4),c:'P',status:'REMOVED',days:[['2026-09-10',0,0,0,0,0,0,0]]},
  C:{ref:AG(3),c:'R',status:'ENABLED',days:[['2026-09-09',7,1,35,1,35,4,60]]},
  M:{ref:AG(10),c:'N',status:'ENABLED',live:true,primary:'NOT_ELIGIBLE',reasons:['CAMPAIGN_ENDED'],days:[]},
  S1:{ref:ADG(5),c:'S',status:'ENABLED',live:true,primary:'ELIGIBLE',days:[['2026-09-09',6,1,40,0,0,2,20],['2026-09-10',5,0,0,1,25,1,15]]},
  S2:{ref:ADG(6),c:'S',status:'REMOVED',days:[['2026-09-10',3,0,0,0,0,2,12]]},
  Z1:{ref:ADG(7),c:'Z',status:'ENABLED',days:[['2026-09-09',9,1,20,1,20,3,40]]},
  Y1:{ref:ADG(8),c:'Y',status:'ENABLED',live:true,primary:'NOT_ELIGIBLE',reasons:['CAMPAIGN_PAUSED'],days:[]}};
const pm=k=>camp[G[k].c].advertisingChannelType==='PERFORMANCE_MAX';
const metric=([date,cost,conv,value,convCd,valueCd,clicks,impr])=>({segments:{date},metrics:{costMicros:String(cost*1e6),conversions:conv,conversionsValue:value,conversionsByConversionDate:convCd,conversionsValueByConversionDate:valueCd,clicks:String(clicks),impressions:String(impr)}});
const offers=[[G.A.ref,'shopify_US_1_11',['2026-09-09',6,1,30,0,0,2,40]],[G.B.ref,'shopify_US_2_22',['2026-09-10',4,0,0,0,0,2,30]]];
const cdOnly=(q,rows)=>rows.map(r=>q.includes('by_conversion_date')?r:{...r,metrics:{...r.metrics,conversionsByConversionDate:undefined,conversionsValueByConversionDate:undefined}});
const campaignDays=()=>{const out=new Map();for(const g of Object.values(G))for(const d of g.days){const c=camp[g.c],r=metric(d),k=c.id+d[0],x=out.get(k)||{campaign:{id:c.id},segments:r.segments,metrics:{}};for(const [f,v] of Object.entries(r.metrics))x.metrics[f]=(Number(x.metrics[f])||0)+Number(v);out.set(k,x);}return [...out.values()];};
const groupRow=(k,why)=>{const g=G[k],base={resourceName:g.ref,name:'Group '+k,status:g.status,...(g.primary?{primaryStatus:g.primary}:{}),...(why&&g.reasons?{primaryStatusReasons:g.reasons}:{})};return pm(k)?{campaign:camp[g.c],assetGroup:{...base,finalUrls:[]}}:{campaign:camp[g.c],adGroup:base};};
let queries=[],refuseReasons=false;
const gaql=async q=>{queries.push(q);
  if(q.includes('customer.time_zone'))return [{customer:{timeZone:'America/Toronto',currencyCode:'CAD'}}];
  if(q.includes('campaign.start_date'))return [];
  if(q.includes('campaign_budget.amount_micros'))return Object.values(camp).map(campaign=>({campaign,campaignBudget:{amountMicros:10000000}}));
  if(q.includes('FROM asset_group_product_group_view'))return offers.map(([ref,value,d])=>({assetGroup:{resourceName:ref},assetGroupListingGroupFilter:{caseValue:{productItemId:{value}}},...cdOnly(q,[metric(d)])[0]}));
  if(q.includes('FROM asset_group_listing_group_filter'))return [{assetGroupListingGroupFilter:{assetGroup:G.A.ref,type:'UNIT_INCLUDED',caseValue:{productItemId:{value:'shopify_US_1_11'}}}}];
  if(q.includes('FROM ad_group_ad'))return [];
  const asked=[...q.matchAll(/'(customers\/[^']+)'/g)].map(m=>m[1]);
  if(q.includes('.resource_name IN ('))return Object.keys(G).filter(k=>asked.includes(G[k].ref)).map(k=>groupRow(k));
  if(q.includes('metrics.')&&q.includes('FROM asset_group '))return cdOnly(q,Object.keys(G).filter(pm).flatMap(k=>G[k].days.map(d=>({assetGroup:{resourceName:G[k].ref},...metric(d)}))));
  if(q.includes('metrics.')&&q.includes('FROM ad_group '))return cdOnly(q,Object.keys(G).filter(k=>!pm(k)).flatMap(k=>G[k].days.map(d=>({adGroup:{resourceName:G[k].ref},...metric(d)}))));
  if(q.includes('metrics.')&&q.includes('FROM campaign '))return cdOnly(q,campaignDays());
  if(q.includes('primary_status_reasons')&&refuseReasons)throw Error('Unrecognized field primary_status_reasons');
  // Google lists only live groups of live campaigns here (the query excludes removed ones).
  const live=k=>G[k].live&&G[k].status!=='REMOVED'&&camp[G[k].c].status!=='REMOVED';
  if(q.includes('FROM asset_group '))return Object.keys(G).filter(k=>pm(k)&&live(k)).map(k=>groupRow(k,q.includes('primary_status_reasons')));
  if(q.includes('FROM ad_group '))return Object.keys(G).filter(k=>!pm(k)&&live(k)).map(k=>groupRow(k,q.includes('primary_status_reasons')));
  throw Error('Unexpected query '+q);};
const service=()=>createGroupsService({CID:'123',reportContext:async()=>({budgetCurrency:'CAD',accountToday:'2026-09-10',accountTimezone:'America/Toronto'}),validatedRange:i=>({start:i.start,end:i.end}),gaql,reportRates:T.rates,verifiedBasis:async()=>{throw Error('not needed');}});
const input={start:'2026-09-09',end:'2026-09-10',reportingTree:true,force:true};
(async()=>{
  T.set({gaql,fb:false,deleted:async()=>new Set(),fx:async d=>d.endsWith('09')?.7:.8});
  const r=await service().index(input),row=k=>r.groups.find(g=>g.ref===G[k].ref);
  // 1. History rows: removed groups and groups of removed campaigns with activity, never an idle one.
  check(['B','C','S2','Z1'].every(k=>row(k)&&row(k).historicalOnly===true&&row(k).serving==='Removed')&&!row('D'),'removed groups with activity in the dates are kept as history; an idle removed group is not');
  check(row('C').campaignStatus==='REMOVED'&&row('C').campaignName==='Old bracelets'&&row('Z1').channel==='search'&&row('B').metrics.spend===4&&row('B').report.currency==='USD'&&near(row('B').report.click.spend,3.2)&&row('B').report.conversion.conversions===1,'history rows carry their campaign, figures and report in both bases');
  check(row('B').listingMetrics.length===1&&row('B').mapping.itemIds[0]==='shopify_US_2_22'&&row('B').mapping.status==='historical'&&/Removed from Google/.test(row('B').mapping.reason),'a removed asset group names the listing it showed in these dates');
  check(['A','W','M','S1','Y1'].every(k=>row(k)&&!row(k).historicalOnly),'live groups are unchanged');
  // 2. Every campaign row of the Overview equals the sum of its groups, in both bases.
  const m=await E.metricsRange({start:'2026-09-09',end:'2026-09-10'});
  check(['42','60','50','70'].every(id=>m.snapshot.some(c=>c.id===id)),'the Overview keeps the removed campaigns that spent money');
  for(const c of m.snapshot){const gs=r.groups.filter(x=>x.campaignId===c.id),sum=f=>gs.reduce((n,x)=>n+f(x.report),0);
    check(gs.length&&near(sum(x=>x.click.spend),c.cost)&&near(sum(x=>x.click.value),c.value)&&sum(x=>x.click.conversions)===c.conv&&near(sum(x=>x.conversion.value),c.valueCd)&&sum(x=>x.conversion.conversions)===c.convCd,'the groups of '+c.name+' add up to its Overview row');}
  // 3. Editing lists never carry history, and a removed group cannot be opened.
  queries=[];const edit=await service().index({...input,reportingTree:false});
  check(!edit.groups.some(g=>g.historicalOnly)&&!queries.some(q=>q.includes('.resource_name IN (')),'the non-reporting group list has no history rows and no extra reads');
  await assert.rejects(()=>service().detail({...input,campaignId:'42',groupRef:G.B.ref}),/removed from Google/);count++;
  // 4. Deleted campaigns: their history stays, a live group of a campaign deleted in the console does not.
  T.set({deleted:async()=>new Set(['70','80'])});const wrapped=await E.adGroups(input);
  check(wrapped.groups.some(g=>g.ref===G.Z1.ref&&g.historicalOnly)&&!wrapped.groups.some(g=>g.ref===G.Y1.ref)&&wrapped.groups.some(g=>g.ref===G.B.ref),'a deleted campaign keeps only its history rows');
  // 5. Plain serving words, never Google's enums; the list still loads when Google refuses the reasons field.
  check(row('M').serving==='Campaign ended'&&row('W').serving==='Disapproved'&&row('A').serving==='Serving'&&row('Y1').serving==='Campaign paused'&&r.groups.every(g=>!/^[A-Z_]+$/.test(g.serving)),'group serving states are plain words');
  refuseReasons=true;const plain=await service().index(input);refuseReasons=false;
  check(plain.groups.find(g=>g.ref===G.M.ref).serving==='Not eligible'&&!plain.warnings.length&&plain.groups.filter(g=>!g.historicalOnly).length===5,'without reasons the groups still list, in plain words');
  // 6. The Products & groups view: history is shown as Removed, figures follow the Overview's basis.
  if(process.env.BRITES_EDITOR_DOM_RUNTIME){
    const {JSDOM}=require(path.join(process.env.BRITES_EDITOR_DOM_RUNTIME,'jsdom'));
    const dom=new JSDOM('<main><div id="groupContext"></div><section id="v-groups"></section></main>',{url:'https://example.test',runScripts:'outside-only'}),w=dom.window;
    w.eval(fs.readFileSync(path.resolve(__dirname,'../../brites-groups.js'),'utf8'));
    let basis='conversion';const view=w.document.getElementById('v-groups');
    w.BritesGroups.init({api:async action=>action==='adGroups'?JSON.parse(JSON.stringify(r)):{ok:false,error:'not needed'},dashboard:()=>({pending:[]}),basis:()=>basis,currentView:()=>'groups',go:()=>{},design:async()=>{},changed:()=>{},toast:()=>{},reload:async()=>{}});
    w.BritesGroups.show();await new Promise(res=>setTimeout(res,10));
    const section=id=>view.querySelector('.bg-campaign[data-bg-campaign-id="'+id+'"]'),order=[...view.querySelectorAll('.bg-campaign')].map(s=>s.dataset.bgCampaignId),groupEl=k=>view.querySelector('.bg-group[data-bg-group="'+camp[G[k].c].id+'|'+G[k].ref+'"]');
    const badgeOf=el=>el.querySelector(':scope > .bg-line .bg-state').textContent;
    check(order.indexOf('42')<order.indexOf('90')&&order.indexOf('90')<order.indexOf('60')&&order.indexOf('50')<order.indexOf('90')&&order.indexOf('90')<order.indexOf('70'),'serving campaigns come first, then ended or paused ones, then removed ones');
    check(badgeOf(section('60'))==='Removed'&&badgeOf(section('90'))==='Ended'&&badgeOf(groupEl('B'))==='Removed'&&badgeOf(groupEl('C'))===''&&badgeOf(groupEl('W'))==='Disapproved'&&badgeOf(groupEl('M'))==='','states read in plain words, and a removed campaign’s groups inherit its state');
    check(!groupEl('B').querySelector('[data-bg-open]')&&!groupEl('C').querySelector('[data-bg-open]')&&groupEl('A').querySelector('[data-bg-open]'),'a removed group has nothing to open');
    groupEl('B').querySelector('[data-bg-toggle=group]').onclick();
    check(groupEl('B').classList.contains('is-open')&&groupEl('B').querySelectorAll('.bg-leaf').length===1&&!groupEl('B').querySelector('[data-bg-design-direct]')&&/Removed from Google/.test(groupEl('B').textContent),'expanding a removed group shows the listing it served and why it is kept, with no design action');
    check(section('42').querySelector('.bg-camp-line small').textContent.includes('1 removed')&&[...view.querySelectorAll('[data-bg-campaign] option')].some(o=>o.textContent==='Old bracelets (removed)'),'the campaign line and filter say which groups and campaigns are removed');
    const cellsOf=el=>[...el.querySelector(':scope > .bg-line .bg-cells').children].map(c=>c.textContent.replace(/^(Spend|Clicks|Conv\.|Conv\. value) /,''));
    // Campaign 42 by conversion date: spend 23 + 3.2 USD, conversions 4 + 1.
    check(cellsOf(section('42'))[0]==='$26.20'&&cellsOf(section('42'))[2]==='5'&&/USD · Conversions by conversion date/.test(view.querySelector('.bg-scope').textContent),'figures are the Overview’s: USD and its conversion basis, removed groups included');
    basis='click';w.BritesGroups.show();
    check(cellsOf(section('42'))[2]==='3'&&/Conversions by ad-click date/.test(view.querySelector('.bg-scope').textContent),'switching the Overview to ad-click date changes the group figures with it');
    const text=view.textContent+[...view.querySelectorAll('[title]')].map(e=>e.title).join(' ');
    check(!/\b[A-Z]{3,}(?:_[A-Z]+)+\b/.test(text),'no raw Google enum appears anywhere in the view');
    await w.BritesGroups.openGroup('42',G.B.ref);
    check(!w.BritesGroups.state.selected&&w.BritesGroups.state.campaign==='42'&&groupEl('B').classList.contains('is-open'),'asking to open a removed group shows it expanded in its campaign instead');
  }
  console.log('PASS '+count+' removed-group history, campaign totals and plain status checks');
})().catch(e=>{console.error(e);process.exit(1)});
