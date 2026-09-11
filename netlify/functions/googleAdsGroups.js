// Product/group navigation and reports. No Google mutations run from these reads.
const crypto = require('crypto');
const {offerParts, destination, productId} = require('./googleAdsAdDesignContext');
const hash = value => crypto.createHash('sha256').update(JSON.stringify(value)).digest('hex');
const unique = rows => [...new Set(rows.filter(Boolean))];
const id = value => { const s=String(value||''); if(!/^\d+$/.test(s))throw Error('Choose a campaign.'); return s; };
const totals = m => ({impressions:Number(m.impressions||0),clicks:Number(m.clicks||0),spend:Number(m.costMicros||0)/1e6,conversions:Number(m.conversions||0),value:Number(m.conversionsValue||0)});
const ownRef = (value,cid) => {if(!new RegExp('^customers/'+cid+'/(assetGroups|adGroups)/\\d+$').test(String(value)))throw Error('Choose a group in this account.');return value;};
function mapping(group, filters, ads) {
  const urls=group.channel==='pmax'?group.urls:unique(ads.filter(a=>a.adGroup===group.ref).flatMap(a=>a.finalUrls||[]));
  const relevant=filters.filter(f=>f.assetGroup===group.ref),leaves=relevant.filter(f=>f.type==='UNIT_INCLUDED');
  const itemIds=unique(leaves.map(f=>f.caseValue?.productItemId?.value));
  const offerProducts=unique(itemIds.map(x=>offerParts(x)?.productId));
  const destinations=urls.map(destination).filter(Boolean),handles=unique(destinations.filter(d=>d.kind==='product').map(d=>d.handle));
  const exact=group.channel==='pmax'&&leaves.length>0&&leaves.every(f=>f.caseValue?.productItemId?.value&&offerParts(f.caseValue.productItemId.value));
  const broad=group.channel==='pmax'&&!exact;
  const mixed=offerProducts.length>1||handles.length>1;
  return {itemIds,productIds:offerProducts,handles,urls,exactOfferScope:exact,status:mixed?'mixed':broad?'unverified':handles.length===1||offerProducts.length===1?'focused':'unverified',reason:mixed?'Multiple product listings share this group.':broad?'The live product filter is broader than exact offer IDs.':!handles.length&&!offerProducts.length?'No exact listing destination is linked.':null};
}
function createGroupsService(D){
  const cache=new Map();
  async function index(input={}){
    const context=await D.reportContext(),range=D.validatedRange(input,context.accountToday,30),campaignId=input.campaignId?id(input.campaignId):null;
    const cacheKey=JSON.stringify([campaignId,range]);const prior=cache.get(cacheKey);if(!input.force&&prior&&Date.now()-prior.at<180000)return prior.result;
    const filter=campaignId?' AND campaign.id = '+campaignId:'';
    const warnings=[];const read=async(label,query)=>{try{const rows=await D.gaql(query);if(rows.length>=5000)throw Error('The report limit was reached. Choose one campaign.');return rows;}catch(e){warnings.push(label+': '+e.message);return null;}};
    const metricFields='metrics.impressions, metrics.clicks, metrics.cost_micros, metrics.conversions, metrics.conversions_value';
    const [pmax,search,pmaxMetrics,searchMetrics,filters,ads]=await Promise.all([
      read('PMax groups',`SELECT campaign.id, campaign.name, campaign.status, asset_group.resource_name, asset_group.name, asset_group.status, asset_group.primary_status, asset_group.final_urls FROM asset_group WHERE campaign.status != 'REMOVED' AND asset_group.status != 'REMOVED'${filter} LIMIT 5000`),
      read('Search groups',`SELECT campaign.id, campaign.name, campaign.status, ad_group.resource_name, ad_group.name, ad_group.status, ad_group.primary_status FROM ad_group WHERE campaign.advertising_channel_type = 'SEARCH' AND campaign.status != 'REMOVED' AND ad_group.status != 'REMOVED'${filter} LIMIT 5000`),
      read('PMax group metrics',`SELECT asset_group.resource_name, ${metricFields} FROM asset_group WHERE segments.date BETWEEN '${range.start}' AND '${range.end}'${filter} LIMIT 5000`),
      read('Search group metrics',`SELECT ad_group.resource_name, ${metricFields} FROM ad_group WHERE campaign.advertising_channel_type = 'SEARCH' AND segments.date BETWEEN '${range.start}' AND '${range.end}'${filter} LIMIT 5000`),
      read('Product filters',`SELECT asset_group_listing_group_filter.asset_group, asset_group_listing_group_filter.type, asset_group_listing_group_filter.case_value.product_item_id.value FROM asset_group_listing_group_filter WHERE asset_group.status != 'REMOVED'${filter} LIMIT 5000`),
      read('Search destinations',`SELECT ad_group.resource_name, ad_group_ad.ad.final_urls FROM ad_group_ad WHERE campaign.advertising_channel_type = 'SEARCH' AND ad_group_ad.status != 'REMOVED'${filter} LIMIT 5000`)
    ]);
    if(!pmax&&!search)throw Error('Group data could not be loaded. Refresh to retry.');
    const groupRows=[...(pmax||[]).map(r=>({r,g:r.assetGroup,channel:'pmax'})),...(search||[]).map(r=>({r,g:r.adGroup,channel:'search'}))];
    const metricMaps={pmax:new Map((pmaxMetrics||[]).map(r=>[r.assetGroup.resourceName,totals(r.metrics||{})])),search:new Map((searchMetrics||[]).map(r=>[r.adGroup.resourceName,totals(r.metrics||{})]))};
    const groups=groupRows.map(({r,g,channel})=>{const row={ref:g.resourceName,id:g.resourceName.split('/').pop(),name:g.name,channel,campaignId:String(r.campaign.id),campaignName:r.campaign.name,campaignStatus:r.campaign.status,status:g.status,primaryStatus:g.primaryStatus||null,urls:g.finalUrls||[]};row.mapping=mapping(row,(filters||[]).map(r=>({assetGroup:r.assetGroupListingGroupFilter.assetGroup,type:r.assetGroupListingGroupFilter.type,caseValue:r.assetGroupListingGroupFilter.caseValue})),(ads||[]).map(r=>({adGroup:r.adGroup.resourceName,finalUrls:r.adGroupAd.ad.finalUrls})));if(channel==='pmax'&&!filters||channel==='search'&&!ads)row.mapping={...row.mapping,status:'unavailable',reason:'Live product mapping is unavailable.'};row.metrics=(channel==='pmax'?pmaxMetrics:searchMetrics)?metricMaps[channel].get(row.ref)||totals({}):null;row.serving=row.campaignStatus!=='ENABLED'?'Campaign '+row.campaignStatus.toLowerCase():row.status!=='ENABLED'?row.status.toLowerCase():row.primaryStatus||'Enabled';return row;});
    groups.sort((a,b)=>a.campaignName.localeCompare(b.campaignName)||a.name.localeCompare(b.name));
    const result={ok:true,groups,range,currency:context.budgetCurrency,timeZone:context.accountTimezone,basis:'Ad-click date',checkedAt:Date.now(),warnings,complete:!!pmax&&!!search&&!!filters&&!!ads};cache.set(cacheKey,{at:Date.now(),result});if(cache.size>20)cache.delete(cache.keys().next().value);return result;
  }
  async function detail(input={}){
    const campaignId=id(input.campaignId),ref=ownRef(input.groupRef,D.CID),report=await index({...input,campaignId}),group=report.groups.find(g=>g.ref===ref);if(!group)throw Error('The group is outside the selected campaign.');
    const basis=await D.verifiedBasis({campaignId}),c=basis.snapshot.components||{},warnings=[...report.warnings];
    const searchAds=(c.searchAds||[]).filter(a=>a.adGroup===ref),links=(group.channel==='pmax'?c.assetLinks:c.searchImageLinks||[]).filter(a=>group.channel==='pmax'?a.assetGroup===ref:a.adGroup===ref);
    let keywords=group.channel==='search'?(c.keywords||[]).filter(k=>k.adGroup===ref).map(k=>({text:k.keyword?.text,matchType:k.keyword?.matchType,status:k.status,negative:!!k.negative})):[];
    let keywordStatus='available';if(group.channel==='pmax')try{const rows=await D.gaql(`SELECT asset_group_signal.search_theme.text FROM asset_group_signal WHERE asset_group.resource_name = '${ref}'`);keywords=rows.map(r=>({text:r.assetGroupSignal?.searchTheme?.text})).filter(k=>k.text);}catch(e){keywordStatus='unavailable';warnings.push('Search themes: '+e.message);}
    const creativeRef=group.channel==='pmax'?ref:searchAds.find(a=>a.status==='ENABLED')?.resourceName||searchAds[0]?.resourceName;
    const creativeRefs=group.channel==='pmax'?[ref]:searchAds.map(a=>a.resourceName);
    const context=creativeRef?await D.loadContext({campaignId,...(group.channel==='pmax'||creativeRefs.length===1?{groupRef:creativeRef}:{})}):null;
    const products=(context?.products||[]).filter(p=>(p.eligibleGroupRefs||[]).some(r=>creativeRefs.includes(r))).map(p=>({id:p.id,title:p.title,url:p.url,handle:p.handle,offerIds:p.offerIds||[p.itemId].filter(Boolean),image:p.images?.[0]?.url||null,description:p.description||''}));
    const mapped=mapping(group,c.listingGroups||[],searchAds);if(group.channel==='search'&&group.mapping.status==='mixed')mapped.status='mixed';
    const strings=type=>links.filter(l=>l.fieldType===type&&l.text).map(l=>l.text);
    const copy=group.channel==='pmax'?[{ref,name:group.name,headlines:strings('HEADLINE'),longHeadlines:strings('LONG_HEADLINE'),descriptions:strings('DESCRIPTION')}]:searchAds.map(a=>({ref:a.resourceName,name:'Ad '+a.resourceName.split('/').pop(),status:a.status,url:a.finalUrls?.[0],headlines:(a.responsiveSearchAd?.headlines||[]).map(x=>x.text),descriptions:(a.responsiveSearchAd?.descriptions||[]).map(x=>x.text)}));
    const images=links.filter(a=>a.imageUrl).map(a=>({asset:a.asset,url:a.imageUrl,fieldType:a.fieldType}));
    const scopedProducts=unique([...(mapped.productIds||[]),...products.map(p=>productId(p.id))]);
    const exactScope=group.channel==='pmax'?mapped.exactOfferScope:mapped.handles.length>1&&mapped.urls.every(url=>destination(url)?.kind==='product')&&mapped.handles.every(h=>products.some(p=>destination(p.url)?.handle===h));
    const splitAvailable=basis.snapshot.complete&&scopedProducts.length>1&&scopedProducts.length<=12&&exactScope&&products.length===scopedProducts.length;
    let splitProposals=[];if(D.fb&&D.COL){try{const saved=await D.fb().db.collection(D.COL.approvals).where('payload.meta.existingCampaignId','==',campaignId).limit(100).get();const owned=saved.docs.map(x=>({id:x.id,...x.data()}));for(const x of owned.filter(x=>x.status==='APPLIED'&&x.groupSplitPublication?.groups.some(g=>g.ref===ref))){if(D.linkDesignScopes)await D.linkDesignScopes({campaignId,sourceGroupRef:x.payload.groupSplitGuard.sourceGroupRef,groups:x.groupSplitPublication.groups});}splitProposals=owned.filter(x=>x.payload?.groupSplitGuard?.sourceGroupRef===ref&&['PENDING','APPROVED','APPLIED'].includes(x.status)).map(x=>({id:x.id,status:x.status,groups:x.payload.meta.assetGroups,publication:x.groupSplitPublication||null}));}catch(e){warnings.push('Saved splits: '+e.message);}}
    return {ok:true,group:{...group,mapping:mapped},products,copy,splitProposals,images,keywords,keywordStatus,creativeRef,range:report.range,currency:report.currency,timeZone:report.timeZone,basis:report.basis,warnings,sourceVersion:basis.version,snapshotHash:basis.snapshotHash,splitAvailable,splitReason:splitAvailable?null:group.channel==='search'?'Search groups can be split after each destination has a verified product and keyword plan.':!mapped.exactOfferScope?'Review exact Merchant offers before splitting a broad product filter.':products.length!==scopedProducts.length?'Load every linked listing before preparing a split.':scopedProducts.length>12?'Split this group in batches of up to 12 listings.':null};
  }
  async function draftSplit(input={}){
    const d=await detail({...input,force:true});if(!d.splitAvailable)throw Error(d.splitReason||'This group does not need a product split.');
    if(input.snapshotHash!==d.snapshotHash||input.expectedVersion!==d.sourceVersion)throw Error('The group changed. Refresh before preparing its split.');
    const existing=(d.splitProposals||[]).find(p=>['PENDING','APPROVED','APPLIED'].includes(p.status));if(existing)return {ok:true,approvalId:existing.id,groups:existing.groups,source:d.group,cached:true};
    const source=d.group,ops=[],groups=[];let n=0;
    for(const p of d.products){
      const offers=source.mapping.itemIds.filter(x=>offerParts(x)?.productId===productId(p.id));if(source.channel==='pmax'&&!offers.length)throw Error('A product has no exact offer in this group.');
      const seed={headlines:[p.title.slice(0,30),'Explore '+p.title.slice(0,22),'Brites Jewelry'],descriptions:['Discover '+p.title.slice(0,70)+'.','See the listing for details and available options.']};
      const built=source.channel==='search'?D.buildSearch({handle:p.handle||'product',title:p.title},null,seed,{dailyBudget:1,maxCpc:1,withAssets:false,adGroups:[{name:p.title,finalUrl:p.url,keywords:[p.title,'buy '+p.title,'shop '+p.title,p.title+' brites'].map(text=>({text:text.toLowerCase().slice(0,80),matchType:'EXACT'})),assets:seed}]}):D.buildPmax({handle:p.handle||destination(p.url)?.handle||'product',title:p.title},{dailyBudget:1,itemIds:offers,combinedCreativeGroup:true,productDestination:p.url,productTitle:p.title,offerDetails:offers.map(itemId=>({itemId,title:p.title})),searchThemes:[p.title],countries:[]});
      const remap=value=>{if(typeof value==='string'){const m=value.match(/^customers\/\d+\/(assetGroups|adGroups|ads|adGroupAds|adGroupCriteria|assets|assetGroupListingGroupFilters)\/(.*)$/);if(m)return value.replace(/-\d+/g,v=>String(Number(v)-n*10000-1000));return value;}if(Array.isArray(value))return value.map(remap);if(value&&typeof value==='object')return Object.fromEntries(Object.entries(value).map(([k,v])=>[k,remap(v)]));return value;};
      const part=remap(built.ops.filter(o=>['assetOperation','assetGroupOperation','assetGroupAssetOperation','assetGroupListingGroupFilterOperation','assetGroupSignalOperation','adGroupOperation','adGroupAdOperation','adGroupCriterionOperation'].includes(Object.keys(o)[0])));
      const g=source.channel==='pmax'?part.find(o=>o.assetGroupOperation)?.assetGroupOperation.create:part.find(o=>o.adGroupOperation)?.adGroupOperation.create;if(!g)throw Error('Product group could not be prepared.');g.campaign='customers/'+D.CID+'/campaigns/'+source.campaignId;g.status='PAUSED';g.name=('Product · '+p.title).slice(0,128);ops.push(...part);groups.push({name:g.name,ref:g.resourceName,productId:p.id,itemIds:offers,url:p.url});n++;
    }
    const meta={channel:source.channel,kind:'productGroupSplit',operation:'productGroupSplit',existingCampaignId:source.campaignId,sourceGroupRef:source.ref,sourceGroupName:source.name,sourceVersion:d.sourceVersion,snapshotHash:d.snapshotHash,assetGroups:groups,productTitles:d.products.map(p=>p.title),sourceProducts:undefined,adDesignId:hash([source.ref,d.snapshotHash,'split']).slice(0,32)};
    delete meta.sourceProducts;
    const approvalId=await D.enqueueApproval({type:source.channel==='pmax'?'pmax':'creative',summary:'Split '+source.name+' into '+groups.length+' product groups',payload:{mutateOperations:ops,meta,groupSplitGuard:{campaignId:source.campaignId,expectedVersion:d.sourceVersion,snapshotHash:d.snapshotHash,sourceGroupRef:source.ref,channel:source.channel},countries:[]}}, {id:'split-'+meta.adDesignId});
    return {ok:true,approvalId,groups,source,summary:'New groups start paused. The current group and campaign budget stay unchanged.'};
  }
  async function activationBasis(splitId){
    if(!/^[a-zA-Z0-9_-]{1,160}$/.test(String(splitId||'')))throw Error('Choose a completed product split.');
    const doc=await D.fb().db.collection(D.COL.approvals).doc(splitId).get(),split=doc.exists&&doc.data();
    if(!split||split.status!=='APPLIED'||!split.payload?.groupSplitGuard||!split.groupSplitPublication)throw Error('Create and review the paused product groups before switching traffic.');
    const source=split.payload.groupSplitGuard,refs=split.groupSplitPublication.groups||[],expected=split.payload.meta.assetGroups||[];
    if(refs.length!==expected.length||refs.length<2||new Set(refs.map(g=>g.ref)).size!==refs.length||refs.some(g=>!expected.some(p=>p.ref===g.temporaryRef&&p.productId===g.productId)))throw Error('The created group identities could not be verified. Refresh Google before switching traffic.');
    const campaignId=id(source.campaignId),pmax=source.channel!=='search',view=pmax?'asset_group':'ad_group';
    const rows=await D.gaql(`SELECT ${view}.resource_name, ${view}.name, ${view}.status FROM ${view} WHERE campaign.id = ${campaignId} AND ${view}.status != 'REMOVED'`);
    const all=rows.map(r=>pmax?r.assetGroup:r.adGroup),original=all.find(g=>g.resourceName===source.sourceGroupRef),targets=refs.map(g=>all.find(x=>x.resourceName===g.ref));
    if(!original||targets.some(g=>!g))throw Error('One of the product groups was removed or moved. Refresh the split.');
    if(original.status==='PAUSED'&&targets.every(g=>g.status==='ENABLED'))throw Error('This product split is already active.');
    if(!['PAUSED','ENABLED'].includes(original.status)||targets.some(g=>g.status!=='PAUSED'))throw Error('Group serving states changed. Review the current groups before switching.');
    const basis=await D.verifiedBasis({campaignId});
    const parts=basis.snapshot.components||{},same=(a,b)=>JSON.stringify(unique(a).sort())===JSON.stringify(unique(b).sort());
    if(pmax){const offers=ref=>(parts.listingGroups||[]).filter(f=>f.assetGroup===ref&&f.type==='UNIT_INCLUDED').map(f=>f.caseValue?.productItemId?.value||'*');if(!same(offers(source.sourceGroupRef),expected.flatMap(g=>g.itemIds||[])))throw Error('The source product selection changed. Prepare a new split.');for(const g of refs){const planned=expected.find(p=>p.ref===g.temporaryRef);if(!same(offers(g.ref),planned.itemIds||[]))throw Error('A replacement group no longer matches its reviewed product selection.');}}
    else {const urls=ref=>(parts.searchAds||[]).filter(a=>a.adGroup===ref).flatMap(a=>a.finalUrls||[]);if(!same(urls(source.sourceGroupRef),expected.map(g=>g.url)))throw Error('The source destinations changed. Prepare a new split.');for(const g of refs){const planned=expected.find(p=>p.ref===g.temporaryRef);if(!same(urls(g.ref),[planned.url]))throw Error('A replacement Search group points to another listing.');}}
    const before=[original,...targets].map(g=>({ref:g.resourceName,name:g.name,status:g.status}));
    const operation=pmax?'assetGroupOperation':'adGroupOperation';
    const operations=before.map((g,i)=>({[operation]:{update:{resourceName:g.ref,status:i?'ENABLED':'PAUSED'},updateMask:'status'}}));
    return {campaignId,before,operations,version:basis.version,snapshotHash:basis.snapshotHash,sourceGroupRef:source.sourceGroupRef};
  }
  async function draftActivation(input={}){
    const b=await activationBasis(input.splitId);
    const guard={splitId:input.splitId,campaignId:b.campaignId,expectedVersion:b.version,snapshotHash:b.snapshotHash,before:b.before};
    const approvalId=await D.enqueueApproval({type:'groupActivation',summary:'Switch to '+(b.before.length-1)+' product groups',payload:{mutateOperations:b.operations,groupActivationGuard:guard,meta:{existingCampaignId:b.campaignId,sourceGroupRef:b.sourceGroupRef,groupRefs:b.before.map(g=>g.ref),activationChanges:b.before.map((g,i)=>({...g,after:i?'ENABLED':'PAUSED'}))}}});
    return {ok:true,approvalId,changes:b.before};
  }
  return {index,detail,draftSplit,activationBasis,draftActivation};
}
module.exports={createGroupsService,mapping,totals};
