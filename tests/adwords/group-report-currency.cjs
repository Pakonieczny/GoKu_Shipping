// Group and listing reporting figures reconcile with their campaign row: the same daily
// exchange rates (never a mixed-currency total) and both conversion bases where Google has them.
const fs=require('fs'),vm=require('vm'),assert=require('assert'),path=require('path');
const {createGroupsService}=require('../../netlify/functions/googleAdsGroups');
const filepath=path.resolve(__dirname,'../../netlify/functions/googleAdsAutopilot.js');
const NOW=Date.parse('2026-09-10T16:00:00Z');class FixedDate extends Date{constructor(...a){super(...(a.length?a:[NOW]));}static now(){return NOW;}}
const cx={module:{exports:{}},exports:{},require:n=>n==='node-fetch'?(async()=>{throw Error('network forbidden')}):require('module').createRequire(filepath)(n),process:{env:{GADS_CUSTOMER_ID:'123'}},console,Buffer,Date:FixedDate,Intl,Map,Set,URL,setTimeout,clearTimeout};
vm.createContext(cx);vm.runInContext(fs.readFileSync(filepath,'utf8')+`\nmodule.exports.test={rates:_reportRates,set:v=>{if(v.gaql)gaql=v.gaql;if(v.fx)_fxRateToUsd=v.fx;if(v.fb!==undefined)_fb=v.fb;if(v.deleted)_deletedCampaignIds=v.deleted;}};`,cx);
const E=cx.module.exports,T=E.test;let count=0;const check=(v,m)=>{assert(v,m);count++;},near=(a,b)=>Math.abs(a-b)<1e-9;
const A='customers/123/assetGroups/1',B='customers/123/assetGroups/2',C='customers/123/assetGroups/3',S='customers/123/adGroups/4';
const pmaxC={id:'42',name:'PMax',status:'ENABLED',advertisingChannelType:'PERFORMANCE_MAX'},searchC={id:'50',name:'Search',status:'ENABLED',advertisingChannelType:'SEARCH'};
const day=(date,cost,conv,value,convCd,valueCd,clicks,impr)=>({segments:{date},metrics:{costMicros:String(cost*1e6),conversions:conv,conversionsValue:value,conversionsByConversionDate:convCd,conversionsValueByConversionDate:valueCd,clicks:String(clicks),impressions:String(impr)}});
const groupDays={[A]:[day('2026-09-09',10,2,50,1,30,5,100),day('2026-09-10',20,1,20,3,70,3,80)],[B]:[day('2026-09-10',4,0,0,1,10,1,10)],[S]:[day('2026-09-09',6,1,40,0,0,2,20),day('2026-09-10',5,0,0,1,25,1,15)]};
const offers=[[A,'shopify_US_1_11',day('2026-09-09',6,1,30,0,0,2,40)],[A,'shopify_US_1_11',day('2026-09-10',12,0,0,0,0,1,30)],[A,'shopify_US_2_22',day('2026-09-10',8,1,20,0,0,1,20)]];
let queries=[],refuseCd=false;
const cdOnly=(q,rows)=>rows.map(r=>q.includes('by_conversion_date')?r:{...r,metrics:{...r.metrics,conversionsByConversionDate:undefined,conversionsValueByConversionDate:undefined}});
const campaignDays=()=>{const out=new Map();for(const [ref,rows] of Object.entries(groupDays))for(const r of rows){const c=ref===S?searchC:pmaxC,k=c.id+r.segments.date,x=out.get(k)||{campaign:c,segments:r.segments,metrics:{}};for(const [f,v] of Object.entries(r.metrics))x.metrics[f]=(Number(x.metrics[f])||0)+Number(v);out.set(k,x);}return [...out.values()];};
const gaql=async q=>{queries.push(q);
  if(q.includes('customer.time_zone'))return [{customer:{timeZone:'America/Toronto',currencyCode:'CAD'}}];
  if(q.includes('campaign.start_date'))return [];
  if(q.includes('campaign_budget.amount_micros'))return [pmaxC,searchC].map(campaign=>({campaign,campaignBudget:{amountMicros:10000000}}));
  if(q.includes('FROM asset_group_product_group_view'))return cdOnly(q,offers.map(([ref,value,r])=>({assetGroup:{resourceName:ref},assetGroupListingGroupFilter:{caseValue:{productItemId:{value}}},...r})));
  if(q.includes('FROM asset_group_listing_group_filter'))return [];
  if(q.includes('FROM ad_group_ad'))return [];
  if(q.includes('metrics.')&&q.includes('FROM asset_group '))return cdOnly(q,[A,B].flatMap(ref=>groupDays[ref].map(r=>({assetGroup:{resourceName:ref},...r}))));
  if(q.includes('metrics.')&&q.includes('FROM ad_group ')){if(refuseCd&&q.includes('by_conversion_date'))throw Error('conversion-date metrics refused');return cdOnly(q,groupDays[S].map(r=>({adGroup:{resourceName:S},...r})));}
  if(q.includes('metrics.')&&q.includes('FROM campaign '))return cdOnly(q,campaignDays());
  if(q.includes('FROM asset_group '))return [A,B,C].map((ref,i)=>({campaign:pmaxC,assetGroup:{resourceName:ref,name:'Group '+i,status:'ENABLED',finalUrls:[]}}));
  if(q.includes('FROM ad_group '))return [{campaign:searchC,adGroup:{resourceName:S,name:'Search group',status:'ENABLED'}}];
  throw Error('Unexpected query '+q);};
const service=reportRates=>createGroupsService({CID:'123',reportContext:async()=>({budgetCurrency:'CAD',accountToday:'2026-09-10',accountTimezone:'America/Toronto'}),validatedRange:i=>({start:i.start,end:i.end}),gaql,reportRates});
const input={start:'2026-09-09',end:'2026-09-10',reportingTree:true,force:true};
(async()=>{
  T.set({gaql,fb:false,deleted:async()=>new Set(),fx:async d=>d.endsWith('09')?.7:.8});
  let r=await service(T.rates).index(input);const g=ref=>r.groups.find(x=>x.ref===ref);
  check(r.currency==='CAD'&&r.basis==='Ad-click date'&&g(A).metrics.spend===30&&g(A).metrics.conversions===3&&g(A).metrics.value===70,'native click-date group metrics are unchanged daily sums');
  check(g(A).report.currency==='USD'&&near(g(A).report.click.spend,23)&&near(g(A).report.click.value,51)&&g(A).report.conversion.conversions===4&&near(g(A).report.conversion.value,77),'each group day uses its own rate, by click and by conversion date');
  check(g(C).metrics.spend===0&&g(C).report.click.spend===0&&g(C).report.conversion.conversions===0,'a group without activity reports a genuine zero in both bases');
  const listing=g(A).listingMetrics;check(listing.length===2&&listing.find(l=>l.itemId==='shopify_US_1_11').metrics.spend===18&&near(listing.find(l=>l.itemId==='shopify_US_1_11').report.click.spend,13.8)&&listing.every(l=>l.report.conversion===null),'daily listing rows merge into one row per listing, click date only');
  check(queries.filter(q=>/metrics\.cost_micros.* FROM (asset_group|ad_group) /.test(q)).every(q=>q.includes('segments.date,')&&q.includes('by_conversion_date'))&&queries.filter(q=>q.includes('FROM asset_group_product_group_view')).every(q=>q.includes('segments.date,')&&!q.includes('by_conversion_date')),'group reports ask Google for daily rows and the supported bases');
  const m=await E.metricsRange({start:'2026-09-09',end:'2026-09-10'});
  for(const c of m.snapshot){const gs=r.groups.filter(x=>x.campaignId===c.id),sum=f=>gs.reduce((n,x)=>n+f(x.report),0);
    check(m.currency===gs[0].report.currency&&near(sum(x=>x.click.spend),c.cost)&&near(sum(x=>x.click.value),c.value)&&sum(x=>x.click.conversions)===c.conv&&near(sum(x=>x.conversion.value),c.valueCd)&&sum(x=>x.conversion.conversions)===c.convCd,'groups add up to the Overview row of campaign '+c.id);}
  refuseCd=true;queries=[];r=await service(T.rates).index(input);
  check(g(S).report.conversion===null&&near(g(S).report.click.spend,6*.7+5*.8)&&g(A).report.conversion.conversions===4&&!r.warnings.length,'a refused conversion-date column leaves only that basis unavailable');refuseCd=false;
  T.set({fx:async d=>d.endsWith('09')?.7:NaN});r=await service(T.rates).index(input);
  check(g(A).report.currency==='CAD'&&g(A).report.click.spend===30&&listing.length===2&&g(A).listingMetrics[0].report.currency==='CAD'&&r.warnings.some(w=>/exchange rate/.test(w)),'a missing daily rate keeps every group figure in CAD, never mixed');
  r=await service(async()=>({currency:'USD',fxIncomplete:false,rate:()=>{throw Error('Missing exchange rate for a reporting date.')}})).index(input);check(g(A).report===undefined&&g(A).metrics.spend===30&&r.warnings.some(w=>/could not be converted/.test(w)),'a conversion failure drops only the converted figures, never the group report');
  r=await service().index(input);check(g(A).report===undefined&&g(A).metrics.spend===30,'without rates the native report is unchanged');
  console.log('PASS '+count+' group and listing reporting currency and basis checks');
  require('./suite-guard.cjs').done();
})().catch(e=>{console.error(e);process.exit(1)});
