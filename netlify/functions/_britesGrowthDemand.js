'use strict';
// Bounded, read-only evidence for saved product hypotheses. No AI, mutations,
// uploads, forecasts, new geography assumptions or storage writes.
// Official references inspected 2026-10-02:
// https://developers.google.com/google-ads/api/fields/v24/search_term_view
// https://developers.google.com/google-ads/api/fields/v24/campaign_search_term_view
// https://developers.google.com/google-ads/api/fields/v24/campaign_criterion
// https://developers.google.com/google-ads/api/fields/v24/language_constant
// https://developers.google.com/google-ads/api/performance-max/reporting
// https://developers.google.com/google-ads/api/docs/ads/upgraded-urls/reports
// https://developers.google.com/google-ads/api/docs/keyword-planning/generate-keyword-ideas
// https://developers.google.com/google-ads/api/docs/best-practices/quotas
const crypto=require('node:crypto');
const LIMITS=Object.freeze({ads:2000,criteria:1000,shopping:500,terms:300,seeds:20,groups:20,campaigns:20,outputBytes:230000});
const RANGE='segments.date DURING LAST_30_DAYS';
const STOREFRONT_URL='https://britesjewelry.com/';
const QUERIES=Object.freeze({
  account:'SELECT customer.currency_code, customer.time_zone FROM customer LIMIT 2',
  ads:"SELECT campaign.id, campaign.name, campaign.status, campaign.advertising_channel_type, ad_group.id, ad_group_ad.ad.id, ad_group_ad.ad.final_urls, ad_group_ad.ad.final_mobile_urls FROM ad_group_ad WHERE campaign.status != 'REMOVED' AND ad_group_ad.status != 'REMOVED' AND campaign.advertising_channel_type = 'SEARCH' LIMIT 2001",
  criteria:"SELECT campaign.id, campaign.name, campaign.status, campaign.advertising_channel_type, campaign_criterion.criterion_id, campaign_criterion.type, campaign_criterion.status, campaign_criterion.negative, campaign_criterion.location.geo_target_constant, campaign_criterion.language.language_constant FROM campaign_criterion WHERE campaign.status != 'REMOVED' AND campaign_criterion.status != 'REMOVED' AND campaign_criterion.type IN ('LOCATION', 'LANGUAGE')"
});
const clean=(v,n=240)=>String(v??'').replace(/\u0000/g,'').trim().slice(0,n);
const numericId=value=>/^\d+$/.test(String(value??''))?String(value):null;
const productId=value=>/^gid:\/\/shopify\/Product\/\d+$/.test(String(value))?String(value):null;
const phrase=value=>{if(typeof value!=='string')return null;const s=value.trim().replace(/\s+/g,' ').toLowerCase();return s&&s.length<=80&&s.split(' ').length<=10&&!/[\n<>]/.test(s)?s:null;};
const unique=values=>[...new Set(values.filter(Boolean))];
function seedsFor(dossier){return unique((dossier.buyerIntents||[]).flatMap(intent=>Array.isArray(intent?.keywords)?intent.keywords.map(phrase):[phrase(intent?.query)]).concat((dossier.recommendations||[]).filter(r=>r.channel==='keywords').flatMap(r=>Array.isArray(r.keywords)?r.keywords.map(phrase):[]))).slice(0,LIMITS.seeds);}
function destination(value){try{const u=new URL(value),m=u.pathname.match(/^\/(?:[a-z]{2}(?:-[a-z]{2})?\/)?products\/([a-z0-9_-]+)\/?$/i);return u.protocol==='https:'&&['britesjewelry.com','www.britesjewelry.com'].includes(u.hostname)&&!u.username&&!u.password&&!u.port&&m?m[1]:null;}catch{return null;}}
function amount(value){if(value==null||value===''||typeof value==='boolean')return null;const n=Number(value);return Number.isFinite(n)&&n>=0?n:null;}
function metrics(row){const m=row?.metrics||{},cost=amount(m.costMicros);return {impressions:amount(m.impressions),clicks:amount(m.clicks),cost:cost==null?null:cost/1e6,reportedConversions:amount(m.conversions),reportedConversionValue:amount(m.conversionsValue)};}
function totals(rows,truncated=false){const out={};for(const key of ['impressions','clicks','cost','reportedConversions','reportedConversionValue'])out[key]=!truncated&&rows.length&&rows.every(r=>r[key]!=null)?rows.reduce((s,r)=>s+r[key],0):null;return out;}
function verifiedOffer(itemId,product){const m=String(itemId||'').match(/^shopify_([^_]+)_(\d+)_(\d+)$/i);if(!m||m[2]!==product.id.split('/').pop())return null;const variant=(product.variants||[]).find(v=>String(v.id||'')==='gid://shopify/ProductVariant/'+m[3]);return variant?{itemId:String(itemId),variantId:variant.id,marketCode:m[1]}:null;}
function termRows(rows,key){return rows.flatMap(row=>{const campaignId=numericId(row.campaign?.id),text=clean(row[key]?.searchTerm,300);return campaignId&&text?[{campaignId,groupId:numericId(row.adGroup?.id),text,...metrics(row)}]:[];});}
function ownStorefrontUrl(raw){try{const u=new URL(raw);return u.protocol==='https:'&&['britesjewelry.com','www.britesjewelry.com'].includes(u.hostname)&&!u.username&&!u.password&&!u.port?u.href:null;}catch{return null;}}
async function readOwnStorefrontLanguage({fetcher=globalThis.fetch,now=Date.now}={}){
  let current=STOREFRONT_URL;
  for(let attempt=0;attempt<4;attempt++){
    const response=await fetcher(current,{redirect:'manual',signal:AbortSignal.timeout(8000),headers:{Accept:'text/html'}});
    if([301,302,303,307,308].includes(response.status)){const location=response.headers?.get('location'),next=location&&ownStorefrontUrl(new URL(location,current).href);if(!next)throw Error('The storefront language source redirected outside the verified shop host.');current=next;continue;}
    if(!response.ok)throw Error('The own-storefront language source is unavailable (HTTP '+response.status+').');
    const html=await response.text();if(Buffer.byteLength(html,'utf8')>2000000)throw Error('The own-storefront HTML exceeded the bounded language-source size.');
    const match=html.match(/<html\b[^>]{0,2000}\blang\s*=\s*["']([a-z]{2,3}(?:-[a-z0-9]{2,8})*)["']/i);
    if(!match)throw Error('The own storefront has no verifiable HTML language tag.');
    return {state:'available',sourceUrl:STOREFRONT_URL,finalUrl:current,htmlLang:match[1].toLowerCase(),code:match[1].split('-')[0].toLowerCase(),quote:match[0].slice(0,300),checkedAt:now(),basis:'own_storefront_html_lang',campaignLanguageTargetingVerified:false};
  }
  throw Error('The own-storefront language source exceeded its bounded redirect count.');
}
function boundedDetails(evidence){
  evidence.payloadBounds={maxUtf8Bytes:LIMITS.outputBytes,state:'complete',omittedDetails:{}};
  const pools=[{key:'shoppingRows',rows:evidence.shopping.rows},{key:'searchTerms',rows:evidence.search.terms},{key:'pmaxTerms',rows:evidence.pmax.terms},{key:'targetCriteria',rows:evidence.targeting.criteria},
    ...evidence.search.groups.map(g=>({key:'adDestinationUrls',rows:g.urls})),
    ...evidence.planner.keywords.flatMap(k=>[{key:'plannerSearchMatches',rows:k.reportedSearchTermMatches},{key:'plannerCampaignMatches',rows:k.campaignContextTermMatches}])];
  const bytes=value=>Buffer.byteLength(JSON.stringify(value),'utf8');
  if(bytes(evidence)>LIMITS.outputBytes)evidence.notes.push('Some saved detail examples were omitted to fit the shared evidence limit; omitted counts are explicit. Report totals and provider coverage are calculated before this sampling.');
  while(bytes(evidence)>LIMITS.outputBytes){
    const largest=pools.filter(p=>p.rows.length).sort((a,b)=>bytes(b.rows)-bytes(a.rows))[0];
    if(!largest)throw Error('Report metadata exceeds the bounded shared evidence size.');
    const removed=largest.rows.splice(Math.floor(largest.rows.length/2)).length;
    evidence.payloadBounds.state='detail_sampled';
    evidence.payloadBounds.omittedDetails[largest.key]=(evidence.payloadBounds.omittedDetails[largest.key]||0)+removed;
  }
  return evidence;
}
function buildMarkets(rows,geos,campaignIds){
  const countries=new Map(geos.filter(r=>r.geoTargetConstant?.targetType==='Country'&&r.geoTargetConstant.status==='ENABLED').map(r=>[numericId(r.geoTargetConstant.id),r.geoTargetConstant]));
  const grouped=new Map(),criteria=[];
  for(const row of rows){const cid=numericId(row.campaign?.id),c=row.campaignCriterion;if(!cid||!c||campaignIds.size&&!campaignIds.has(cid))continue;const g=grouped.get(cid)||{campaignId:cid,status:row.campaign.status,name:clean(row.campaign.name),locations:[],languages:[],typeCounts:{LOCATION:0,LANGUAGE:0},negativeLanguage:false};grouped.set(cid,g);
    if(c.type==='LOCATION'||c.type==='LANGUAGE')g.typeCounts[c.type]++;
    criteria.push({campaignId:cid,criterionId:numericId(c.criterionId),type:clean(c.type,30),negative:c.negative===true?true:c.negative===false?false:null,targetResource:clean(c.location?.geoTargetConstant||c.language?.languageConstant,140),polarityEvidence:'explicit_google_report_predicate'});
    if(c.type==='LOCATION'){const match=/^geoTargetConstants\/(\d+)$/.exec(c.location?.geoTargetConstant||'');if(match)g.locations.push({id:match[1],negative:c.negative===true?true:c.negative===false?false:null});}
    if(c.type==='LANGUAGE'&&c.negative===true)g.negativeLanguage=true;
    if(c.type==='LANGUAGE'&&c.negative===false){const match=/^languageConstants\/(\d+)$/.exec(c.language?.languageConstant||'');if(match)g.languages.push(match[1]);}}
  const markets=new Map(),unresolved=[],countryTargets=[];
  for(const g of grouped.values()){
    const positives=g.locations.filter(x=>x.negative===false),excluded=new Set(g.locations.filter(x=>x.negative===true&&countries.has(x.id)).map(x=>x.id));
    const ids=unique(positives.filter(x=>countries.has(x.id)&&!excluded.has(x.id)).map(x=>x.id)).sort();
    const subregions=positives.filter(x=>!countries.has(x.id)).map(x=>x.id),uncertain=g.locations.some(x=>x.negative==null),languages=unique(g.languages).sort();
    countryTargets.push({campaignId:g.campaignId,status:g.status,countryGeoIds:ids,countries:ids.map(id=>({id,code:countries.get(id).countryCode||null,name:clean(countries.get(id).name)})),languageIds:languages,criterionTypeCounts:g.typeCounts,unsupportedPositiveGeoIds:subregions,polarityUncertain:uncertain});
    const reasons=[...(!ids.length?['no_verified_positive_country_targets']:[]),...(!languages.length?['no_explicit_language_target']:[]),...(uncertain?['unknown_criterion_polarity']:[]),...(subregions.length?['unsupported_positive_geography']:[]),...(g.negativeLanguage?['unsupported_negative_language_target']:[])];
    if(reasons.length){unresolved.push({campaignId:g.campaignId,reason:'Complete explicit country/language targeting could not be verified.',reasonCodes:reasons,observedCountryGeoIds:ids,observedLanguageIds:languages,criterionTypeCounts:g.typeCounts,unsupportedPositiveGeoIds:subregions});continue;}
    for(const languageId of languages){const key=crypto.createHash('sha256').update(JSON.stringify([ids,languageId])).digest('hex').slice(0,24),old=markets.get(key)||{key,countryGeoIds:ids,countries:ids.map(id=>({id,code:countries.get(id).countryCode||null,name:clean(countries.get(id).name)})),languageId,campaigns:[],countryMarketApproximation:true,exclusionsNotAppliedToPlanner:true};old.campaigns.push({id:g.campaignId,status:g.status});markets.set(key,old);}
  }
  return {markets:[...markets.values()].sort((a,b)=>a.key.localeCompare(b.key)),unresolved,countryTargets,criteria};
}
function pooledMarketFor(observed){
  if(observed.markets.length<2||observed.unresolved.length)return null;
  const languages=unique(observed.markets.map(m=>m.languageId));if(languages.length!==1)return null;
  const countryGeoIds=unique(observed.markets.flatMap(m=>m.countryGeoIds)).sort(),byCountry=new Map(observed.markets.flatMap(m=>m.countries).map(c=>[c.id,c]));
  if(!countryGeoIds.length||countryGeoIds.some(id=>!byCountry.has(id)))return null;
  return {key:crypto.createHash('sha256').update(JSON.stringify(['pooled_observed',countryGeoIds,languages[0]])).digest('hex').slice(0,24),
    basis:'pooled_observed_campaign_country_targets',pooled:true,countrySpecificPerformanceInferred:false,
    countryGeoIds,countries:countryGeoIds.map(id=>byCountry.get(id)),languageId:languages[0],
    campaigns:[...new Map(observed.markets.flatMap(m=>m.campaigns).map(c=>[c.id,c])).values()],
    sourceMarkets:observed.markets.map(m=>({key:m.key,countryGeoIds:m.countryGeoIds,languageId:m.languageId,campaigns:m.campaigns})),
    polarityEvidence:'separate_explicit_google_report_predicates',countryMarketApproximation:true,exclusionsNotAppliedToPlanner:true};
}
function researchLanguageMarketFor(observed,language){
  if(language?.state!=='available'||!numericId(language.languageId)||!observed.countryTargets.length||!observed.unresolved.length)return null;
  if(observed.unresolved.some(u=>u.reasonCodes.length!==1||u.reasonCodes[0]!=='no_explicit_language_target'))return null;
  if(observed.countryTargets.some(t=>!t.countryGeoIds.length||t.polarityUncertain||t.unsupportedPositiveGeoIds.length||t.languageIds.some(id=>id!==language.languageId)))return null;
  const ids=unique(observed.countryTargets.flatMap(t=>t.countryGeoIds)).sort(),byCountry=new Map(observed.countryTargets.flatMap(t=>t.countries).map(c=>[c.id,c]));
  return {key:crypto.createHash('sha256').update(JSON.stringify(['storefront_language_hypothesis',ids,language.languageId])).digest('hex').slice(0,24),
    basis:'observed_countries_storefront_research_language_hypothesis',pooled:ids.length>1,countrySpecificPerformanceInferred:false,campaignLanguageTargetingVerified:false,
    countryGeoIds:ids,countries:ids.map(id=>byCountry.get(id)),languageId:language.languageId,languageName:language.languageName,languageBasis:'research_hypothesis_from_verified_own_storefront',
    languageSource:{sourceUrl:language.sourceUrl,finalUrl:language.finalUrl,htmlLang:language.htmlLang,quote:language.quote,checkedAt:language.checkedAt},
    campaigns:observed.countryTargets.map(t=>({id:t.campaignId,status:t.status})),
    sourceCountryTargets:observed.countryTargets.map(t=>({campaignId:t.campaignId,countryGeoIds:t.countryGeoIds,observedCampaignLanguageIds:t.languageIds})),
    polarityEvidence:'separate_explicit_google_report_predicates',countryMarketApproximation:true,exclusionsNotAppliedToPlanner:true};
}
function createDemandEvidence({gaql,keywordResearch,readStorefrontLanguage=null,now=Date.now}={}){
  if(typeof gaql!=='function'||typeof keywordResearch!=='function')throw TypeError('Read-only Google Ads readers are required.');
  let common=null,commonAt=0,languagePromise=null,languageAt=0;
  async function source(query,limit){try{const rows=await gaql(query);if(!Array.isArray(rows)||rows.some(row=>!row||typeof row!=='object'||Array.isArray(row)))throw Error('Google did not return a complete report row array.');return {state:rows.length>limit?'partial':'available',rows:rows.slice(0,limit),rowCount:rows.length,truncated:rows.length>limit};}catch(error){return {state:'unavailable',rows:[],rowCount:0,truncated:false,error:clean(error.message)};}}
  async function criteriaSource(negative){
    const result=await source(`${QUERIES.criteria} AND campaign_criterion.negative = ${negative?'TRUE':'FALSE'} LIMIT 1001`,LIMITS.criteria);
    if(result.rows.some(row=>!row.campaignCriterion||row.campaignCriterion.negative!==undefined&&row.campaignCriterion.negative!==negative))return {state:'unavailable',rows:[],rowCount:result.rowCount,truncated:result.truncated,error:'Criterion polarity contradicts the explicit Google report predicate.'};
    // REST may omit default fields. Polarity is established by Google's explicit
    // selection predicate, never inferred merely from an absent boolean.
    return {...result,rows:result.rows.map(row=>({...row,campaignCriterion:{...row.campaignCriterion,negative}}))};
  }
  async function accountSources(){if(!common||now()-commonAt>300000){commonAt=now();common=Promise.all([source(QUERIES.account,1),source(QUERIES.ads,LIMITS.ads),criteriaSource(false),criteriaSource(true)]).then(([account,ads,criteriaPositive,criteriaNegative])=>{
      const rowCount=criteriaPositive.rowCount+criteriaNegative.rowCount,truncated=criteriaPositive.truncated||criteriaNegative.truncated||rowCount>LIMITS.criteria,unavailable=[criteriaPositive,criteriaNegative].some(s=>s.state==='unavailable');
      const criteria={state:unavailable?'unavailable':truncated?'partial':'available',rows:[...criteriaPositive.rows,...criteriaNegative.rows].slice(0,LIMITS.criteria),rowCount,truncated,error:unavailable?clean([criteriaPositive.error,criteriaNegative.error].filter(Boolean).join('; ')):null};
      return {account,ads,criteria,criteriaPositive,criteriaNegative,at:commonAt};
    });}return common;}
  async function researchLanguage(){
    if(!languagePromise||now()-languageAt>300000){languageAt=now();languagePromise=(async()=>{
      try{
        if(typeof readStorefrontLanguage!=='function')throw Error('An own-storefront language reader is unavailable.');
        const receipt=await readStorefrontLanguage();
        if(receipt?.state!=='available'||receipt.sourceUrl!==STOREFRONT_URL||!ownStorefrontUrl(receipt.finalUrl)||!/^[a-z]{2,3}(?:-[a-z0-9]{2,8})*$/.test(receipt.htmlLang||'')||receipt.code!==receipt.htmlLang.split('-')[0]||typeof receipt.checkedAt!=='number'||Math.abs(now()-receipt.checkedAt)>300000)throw Error('The own-storefront language receipt could not be verified.');
        const report=await source(`SELECT language_constant.id, language_constant.code, language_constant.name, language_constant.targetable FROM language_constant WHERE language_constant.code = '${receipt.code}' AND language_constant.targetable = TRUE LIMIT 2`,1);
        const language=report.rows[0]?.languageConstant;
        if(report.state!=='available'||report.rows.length!==1||!numericId(language?.id)||language.code!==receipt.code||language.targetable!==undefined&&language.targetable!==true)throw Error(report.error||'An exact targetable Google language constant could not be verified for the storefront language.');
        return {...receipt,state:'available',languageId:String(language.id),languageName:clean(language.name,80),basis:'research_language_hypothesis',googleLanguageSource:{state:report.state,rowCount:report.rowCount,code:language.code,targetableEvidence:'explicit_google_report_predicate'},campaignLanguageTargetingVerified:false};
      }catch(error){return {state:'unavailable',basis:'research_language_hypothesis',reason:clean(error.message),campaignLanguageTargetingVerified:false};}
    })();}return languagePromise;
  }
  async function read(product,dossier,{planner=false,marketKey=null}={}){
    const at=now();if(!productId(product?.id)||!product?.handle||destination(product.url)!==product.handle||dossier?.status!=='approved'||dossier.productId!==product.id||dossier.handle!==product.handle)throw Error('Exact catalogue product and approved matching research are required.');
    const seeds=seedsFor(dossier),pid=product.id.split('/').pop(),base=await accountSources();
    const currency=base.account.rows.length===1?/^[A-Z]{3}$/.test(base.account.rows[0].customer?.currencyCode||'')?base.account.rows[0].customer.currencyCode:null:null;
    const shopping=await source(`SELECT campaign.id, campaign.name, campaign.status, campaign.advertising_channel_type, segments.product_item_id, segments.product_country, segments.product_feed_label, metrics.impressions, metrics.clicks, metrics.cost_micros, metrics.conversions, metrics.conversions_value FROM shopping_performance_view WHERE ${RANGE} AND segments.product_item_id REGEXP_MATCH '(?i)^shopify_[^_]+_${pid}_[0-9]+$' ORDER BY metrics.cost_micros DESC LIMIT 501`,LIMITS.shopping);
    const offers=shopping.rows.flatMap(row=>{const match=verifiedOffer(row.segments?.productItemId,product),cid=numericId(row.campaign?.id);return match&&cid?[{...match,campaignId:cid,campaignStatus:row.campaign.status,channel:row.campaign.advertisingChannelType,productCountry:row.segments.productCountry||null,feedLabel:row.segments.productFeedLabel||null,...metrics(row)}]:[];});
    const groups=new Map();for(const row of base.ads.rows){const cid=numericId(row.campaign?.id),gid=numericId(row.adGroup?.id);if(!cid||!gid)continue;const key=cid+'/'+gid,g=groups.get(key)||{campaignId:cid,groupId:gid,urls:[],incomplete:false};groups.set(key,g);const urls=[...(row.adGroupAd?.ad?.finalUrls||[]),...(row.adGroupAd?.ad?.finalMobileUrls||[])];if(!urls.length)g.incomplete=true;g.urls.push(...urls);}
    const aligned=base.ads.truncated?[]:[...groups.values()].filter(g=>!g.incomplete&&g.urls.length&&g.urls.every(url=>destination(url)===product.handle)),selectedGroups=aligned.slice(0,LIMITS.groups),groupKeys=new Set(selectedGroups.map(g=>g.campaignId+'/'+g.groupId));
    const pmaxIds=unique(offers.filter(o=>o.channel==='PERFORMANCE_MAX').map(o=>o.campaignId)).slice(0,LIMITS.campaigns);
    const [search,pmax]=await Promise.all([
      selectedGroups.length?source(`SELECT campaign.id, ad_group.id, search_term_view.search_term, metrics.impressions, metrics.clicks, metrics.cost_micros, metrics.conversions, metrics.conversions_value FROM search_term_view WHERE ${RANGE} AND ad_group.id IN (${unique(selectedGroups.map(g=>g.groupId)).join(',')}) ORDER BY metrics.cost_micros DESC LIMIT 301`,LIMITS.terms):Promise.resolve({state:base.ads.state!=='available'?'unavailable':'not_applicable',rows:[],truncated:false,rowCount:0,error:base.ads.state!=='available'?'Complete ad destinations could not be checked.':null}),
      pmaxIds.length?source(`SELECT campaign.id, campaign_search_term_view.search_term, metrics.impressions, metrics.clicks, metrics.cost_micros, metrics.conversions, metrics.conversions_value FROM campaign_search_term_view WHERE ${RANGE} AND campaign.id IN (${pmaxIds.join(',')}) AND campaign.advertising_channel_type = 'PERFORMANCE_MAX' ORDER BY metrics.cost_micros DESC LIMIT 301`,LIMITS.terms):Promise.resolve({state:shopping.state==='unavailable'?'unavailable':'not_applicable',rows:[],truncated:false,rowCount:0})
    ]);
    const searchTerms=termRows(search.rows,'searchTermView').filter(r=>groupKeys.has(r.campaignId+'/'+r.groupId)),pmaxTerms=termRows(pmax.rows,'campaignSearchTermView').filter(r=>pmaxIds.includes(r.campaignId));
    const campaignIds=new Set([...offers.map(o=>o.campaignId),...aligned.map(g=>g.campaignId)]),geoIds=unique(base.criteria.rows.flatMap(r=>{const m=/^geoTargetConstants\/(\d+)$/.exec(r.campaignCriterion?.location?.geoTargetConstant||'');return m?[m[1]]:[];})).slice(0,1000);
    const geos=geoIds.length?await source(`SELECT geo_target_constant.id, geo_target_constant.name, geo_target_constant.country_code, geo_target_constant.target_type, geo_target_constant.status FROM geo_target_constant WHERE geo_target_constant.id IN (${geoIds.join(',')}) LIMIT 1001`,1000):{state:'not_applicable',rows:[],truncated:false,rowCount:0};
    const observed=buildMarkets(base.criteria.rows,geos.rows,campaignIds),pooledMarket=pooledMarketFor(observed);
    let planningLanguage={state:'not_requested',basis:'research_language_hypothesis',campaignLanguageTargetingVerified:false},researchMarket=null;
    if(planner&&!marketKey&&base.criteria.state==='available'&&geos.state==='available'&&seeds.length&&observed.unresolved.length&&observed.unresolved.every(u=>u.reasonCodes.length===1&&u.reasonCodes[0]==='no_explicit_language_target')){planningLanguage=await researchLanguage();researchMarket=researchLanguageMarketFor(observed,planningLanguage);}
    const market=marketKey?[...observed.markets,...(pooledMarket?[pooledMarket]:[])].find(m=>m.key===marketKey):researchMarket|| (observed.markets.length===1?observed.markets[0]:pooledMarket);
    let demand={state:planner?'unavailable':'not_requested',marketsObserved:observed.markets.length,market:null,keywords:[],relatedIdeas:[],provider:'Google Keyword Planner generateKeywordIdeas',network:'GOOGLE_SEARCH'};
    if(planner){
      if(base.criteria.state!=='available'||geos.state!=='available')demand.reason='Complete campaign geography and language evidence is unavailable.';
      else if(observed.unresolved.length&&!researchMarket)demand.reason='Some relevant campaign country/language targets remain uncertain; no pooled market is assumed.'+(planningLanguage.state==='unavailable'?' Research-language evidence: '+planningLanguage.reason:'');
      else if(!seeds.length)demand.reason='No bounded explicit keyword hypotheses are present in approved research.';
      else if(!market)demand.reason=marketKey?'Requested market is not present in actual campaign evidence.':!observed.markets.length?'No complete observed campaign country/language market is available; no default is assumed.':'Multiple observed campaign markets require an explicit selection; no default is assumed.';
      else {try{const result=await keywordResearch(seeds,market.countryGeoIds,{langId:market.languageId});demand.market=market;
        if(!result?.ok||!Array.isArray(result.ideas)){demand.reason=clean(result?.error||'Keyword Planner returned no usable evidence.');demand.status=result?.status??null;}
        else {const mapped=new Map(result.ideas.map(idea=>[phrase(idea.text),idea]));demand.state='available';demand.bidCurrency=['mcc','cid','none'].includes(result.authMode)?currency:null;demand.normalization='The existing Planner reader rounds missing/zero volumes and bids together; nonpositive values stay unavailable here.';
          demand.keywords=seeds.map(text=>{const idea=mapped.get(text),searches=amount(idea?.searches);return {text,state:idea?'returned_exact_text':'not_returned',averageMonthlySearches:searches>0?searches:null,competition:idea?.competition||null,competitionIndex:amount(idea?.competitionIndex),lowTopOfPageBid:amount(idea?.low)>0?Number(idea.low):null,highTopOfPageBid:amount(idea?.high)>0?Number(idea.high):null,monthlyEnd:idea?.monthlyEnd||null,reportedSearchTermMatches:searchTerms.filter(r=>phrase(r.text)===text),campaignContextTermMatches:pmaxTerms.filter(r=>phrase(r.text)===text)};});
          demand.relatedIdeas=result.ideas.filter(idea=>!seeds.includes(phrase(idea.text))).slice(0,20).map(idea=>({text:clean(idea.text,80),averageMonthlySearches:amount(idea.searches)>0?Number(idea.searches):null,competition:idea.competition||null,basis:'provider_related_idea'}));}}
        catch(error){demand.reason=clean(error.message);}}
    }
    const meta=src=>({state:src.state,rowCount:src.rowCount,truncated:src.truncated,error:src.error||null});
    return boundedDetails({schemaVersion:1,evidenceRevision:3,readOnly:true,evidenceKind:'product_demand_and_ad_outcomes',at,productId:product.id,handle:product.handle,catalogueCheckedAt:product.checkedAt||null,dossierVersion:dossier.version||null,range:'LAST_30_DAYS',currency,conversionValidation:'not_checked_in_this_read',limits:LIMITS,
      sources:{account:meta(base.account),ads:meta(base.ads),criteria:meta(base.criteria),criteriaPositive:meta(base.criteriaPositive),criteriaNegative:meta(base.criteriaNegative),shopping:meta(shopping),searchTerms:meta(search),pmaxTerms:meta(pmax),geography:meta(geos),commonFetchedAt:base.at},
      shopping:{scope:'verified_product_and_variant_offer_ids',rows:offers,totals:totals(offers,shopping.truncated),excludedUnverifiedRows:shopping.rows.length-offers.length,variantCatalogueComplete:product.variantsComplete===true},
      search:{scope:'currently_aligned_search_ad_groups',historicalProductAttributionVerified:false,groups:selectedGroups,groupsOmitted:Math.max(0,aligned.length-selectedGroups.length),terms:searchTerms},
      pmax:{scope:'campaign_context_only',productAttributionVerified:false,campaignIds:pmaxIds,terms:pmaxTerms},
      targeting:{scope:campaignIds.size?'product_related_campaigns':'observed_account_campaigns',polarityEvidence:'separate_explicit_google_report_predicates',markets:observed.markets,pooledMarket,countryTargets:observed.countryTargets,criteria:observed.criteria,unresolved:observed.unresolved},planningLanguage,hypothesisSeeds:seeds,planner:demand,
      notes:['No reported search term is not evidence of zero demand; Google reporting can omit terms and these reads are bounded.',
        'Shopping offer matches require both the exact Shopify Product ID and a catalogue Variant ID; other Merchant offer formats remain unverified.',
        'Current Search destinations do not prove the same destinations were used throughout the historical reporting period.',
        'Performance Max search terms describe campaigns; they cannot establish an individual product sale.',
        'Reported conversions and values may overlap across actions. They are not validated purchases, revenue or profit.',
        'Planner search volumes, competition and bid ranges are market estimates, not competitor spending or a return guarantee.',
        'A pooled observed country market combines verified campaign country targets sharing one language; pooled volumes cannot establish country-specific demand or performance.',
        'Where actual campaign language is unknown, a verified own-storefront HTML language can support a separately labelled research-language hypothesis. It does not prove or change campaign language targeting.',
        'Planner country market estimates do not reproduce location exclusions or campaign presence/interest settings.']});
  }
  return {read};
}
module.exports={createDemandEvidence,seedsFor,verifiedOffer,buildMarkets,readOwnStorefrontLanguage,QUERIES,LIMITS};
