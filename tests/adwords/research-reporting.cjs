const fs=require('fs'),vm=require('vm'),assert=require('assert');
const path=process.env.RESEARCH_ENGINE||require('path').resolve(__dirname,'../../netlify/functions/googleAdsAutopilot.js');
const source=fs.readFileSync(path,'utf8');
function engine(mocks={}){
 const sandbox={module:{exports:{}},process:{env:{GADS_CURRENCY:'USD',GMC_REFRESH_TOKEN:'test'}},URL,Intl,Date,console,Buffer,setTimeout,clearTimeout,AbortController,
 require:(name)=>name==='node-fetch'?(mocks.fetch||(()=>{throw Error('Unexpected network')})):require("module").createRequire(require('path').resolve(__dirname,'../../netlify/functions/googleAdsAutopilot.js'))(name),mocks};
 vm.createContext(sandbox);vm.runInContext(source+'\nmodule.exports.test={eligible:_pmaxIsEligible,organic:_merchantOrganic,match:_pmaxIdentifierMatch,row:_merchantProductRow,status:_opportunityResearchStatus};',sandbox);
 vm.runInContext('if(mocks.gaql)gaql=mocks.gaql;if(mocks.fx)_fxRateToUsd=mocks.fx;if(mocks.currency)_accountCurrency=mocks.currency;if(mocks.tz)_accountTz=mocks.tz;if(mocks.merchant)merchantCenterId=mocks.merchant;if(mocks.merchantToken)mintMerchantToken=mocks.merchantToken;if(mocks.fb)fb=mocks.fb;',sandbox);
 return sandbox.module.exports;
}
(async()=>{
 const api=engine();const t=api.test;
 assert.equal(t.organic({source:'google',medium:'organic'}),false);
 assert.equal(t.organic({source:'google',medium:'organic',campaign:'sag_organic'}),true);
 assert.equal(t.organic({source:'google',medium:'organic',campaign:'sag_organic',hasClickId:true}),false);
 assert.equal(t.eligible({status:'ELIGIBLE',availability:'UNKNOWN'}),false);
 const offer=t.row({merchantCenterId:'123',itemId:'offer',title:'Charm',status:'NOT_ELIGIBLE',availability:'IN_STOCK',issues:[{description:'No campaigns advertising this product',adsSeverity:'ERROR'},{description:'Missing color',adsSeverity:'WARNING'}]},'123');
 assert.equal(t.eligible(offer),true);
 offer.issueDetails.push({description:'Missing price',severity:'ERROR'});assert.equal(t.eligible(offer),false);
 assert.equal(t.match({itemId:'shopify_US_123456_222222'},{productId:'123456',variantId:'333333'}),false);
 const now=Date.now();assert.equal(t.status({pmaxAt:now,pmaxResearchVersion:3},now).pmax.status,'partial');
 let query='';const paid=engine({tz:async()=> 'America/Toronto',currency:async()=> 'CAD',merchant:async()=> '123',fx:async d=>d==='2026-09-01'?.75:null,
 gaql:async q=>{query=q;const select=q.split(/\bFROM\b/i)[0];const where=(q.split(/\bWHERE\b/i)[1]||'');for(const segment of where.match(/segments\.[a-z0-9_]+/g)||[]){if(!/segments\.(date|week|month|quarter|year)$/.test(segment))assert(select.includes(segment),'Filtered product dimension missing from SELECT: '+segment);}return [{segments:{date:'2026-09-01',productItemId:'offer'},metrics:{costMicros:'10000000',conversions:'1',conversionsValue:'100'}},{segments:{date:'2026-09-02',productItemId:'offer'},metrics:{costMicros:'20000000',conversions:'1',conversionsValue:'100'}}];}});
 const perf=await paid.pmaxProductPerformance({days:90});assert.equal(perf.complete,true,perf.error);assert.equal(perf.monetaryComplete,false);assert.equal(perf.rows[0].cost,null);assert.equal(perf.rows[0].cpa,null);assert.equal(perf.rows[0].roas,null);assert.equal(perf.rows[0].nativeCost,30);assert(!query.includes('LIMIT'));assert(query.includes('segments.product_merchant_id = 123'));assert(query.split(/\bFROM\b/i)[0].includes('segments.product_merchant_id'));
 const opts={collections:[{handle:'bird',title:'Birds'}],profiles:[{handle:'bird',topProducts:[{title:'Bird charm',productId:'123456'}]}],sig30:{topMerchantProducts:[{name:'Bird charm',productId:'123456',variantId:'222222',orders:2,revenue:80,estimatedProfit:40,units:2}]},sig90:{topMerchantProducts:[{name:'Bird charm',productId:'123456',variantId:'222222',orders:5,revenue:200,estimatedProfit:100,units:5}]},merchant:[{itemId:'shopify_US_123456_222222',title:'Bird charm',feedLabel:'US',status:'ELIGIBLE',availability:'IN_STOCK'},{itemId:'shopify_US_123456_333333',title:'Bird charm',feedLabel:'US',status:'ELIGIBLE',availability:'IN_STOCK'}],paid:perf};
 const candidates=api.pmaxCandidatesFromSignals(opts);assert.equal(candidates.length,1);assert.equal(candidates[0].itemIds.length,1);assert.equal(candidates[0].estimatedProfit30d,40);assert.equal(candidates[0].evidenceTotals.orders,5);assert.equal(candidates[0].evidenceTotals.orders30d,2);
 opts.profiles[0].topProducts[0].productId='999999';assert.equal(api.pmaxCandidatesFromSignals(opts).length,0);
 let reports=0;const merchant=engine({tz:async()=> 'America/Toronto',merchant:async()=> '123',merchantToken:async()=> 'mock',fetch:async(url,options)=>{
   if(url.includes('/accounts/v1/'))throw new Error('Merchant reports must not depend on optional display timezone metadata');
   assert(url.includes('/reports/v1/'));reports++;const body=JSON.parse(options.body);assert(body.query.includes('marketing_method = "ORGANIC"'));
   return {ok:true,status:200,json:async()=>({results:[{productPerformanceView:{offerId:'offer',clicks:'2',conversions:1,conversionValue:{amountMicros:'100000000',currencyCode:reports===1?'CAD':'USD'}}}],...(reports===1?{nextPageToken:'next'}:{})})};
 }});
 const free=await merchant.merchantFreeProductPerformance({days:30});assert.equal(free.complete,true);assert.equal(free.pages,2);assert.equal(free.rows[0].conversions,2);assert.equal(free.valueComplete,false);assert.equal(free.timeZone,null);assert.equal(free.dateSelectionTimeZone,'America/Toronto');assert.equal(free.reportingCalendar,'Merchant Center account reporting dates');assert(free.warning);assert.equal(free.rows[0].conversionRate,null);
 const failed=engine({tz:async()=> 'America/Toronto',merchant:async()=> '123',merchantToken:async()=> 'mock',fetch:async(url)=>url.includes('/accounts/v1/')?{ok:true,json:async()=>({timeZone:{id:'America/Toronto'}})}:{ok:false,status:403,json:async()=>({error:{status:'PERMISSION_DENIED'}})}});
 const missing=await failed.merchantFreeProductPerformance();assert.equal(missing.complete,false);assert.equal(missing.rows.length,0);assert.equal(missing.errorCode,'ACCESS_REQUIRED');assert.equal(missing.diagnostics.googleStatus,'PERMISSION_DENIED');
 const store=engine({fb:()=>({db:{collection:()=>({where(){return this},orderBy(){return this},limit(){return this},get:async()=>({forEach(fn){[{ts:Date.now()-1000,currency:'USD',value:100,source:'google',medium:'organic',items:[{title:'Bird charm',lineRevenue:100}]},{ts:Date.now()-1000,currency:'CAD',value:200,source:'google',campaign:'sag_organic',items:[{title:'Bird charm',lineRevenue:200}]}].forEach(x=>fn({data:()=>x}));}})})}})});
 const ss=await store.storeSignals();assert.equal(ss.orders,2);assert.equal(ss.merchantOrganicOrders,1);assert.equal(ss.organicRevenue,100);assert.equal(ss.monetaryComplete,false);assert.equal(ss.topMerchantProducts[0].revenue,0);
 console.log('PASS: research attribution, stock/severity, exact offer identity, period integrity, incomplete audit, paid FX/nulls, Merchant timezone/pagination/403, mixed-currency order evidence.');
})().catch(e=>{console.error(e);process.exit(1)});
