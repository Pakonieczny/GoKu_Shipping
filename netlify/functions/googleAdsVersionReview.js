// Exact review gate for updates to existing ads and restoration of saved versions.
// This module performs no network requests or writes.
const TYPES = new Set(["adAnalysisUpdate", "adDesignUpdate", "adVersionRestore"]);
const OP_TYPES = new Set(["adOperation", "adGroupAdOperation", "adGroupCriterionOperation", "campaignCriterionOperation", "assetOperation", "assetGroupOperation", "assetGroupAssetOperation", "assetGroupListingGroupFilterOperation", "campaignAssetOperation", "adGroupAssetOperation"]);
const SERVICES = {ads:"adOperation", adGroupAds:"adGroupAdOperation", adGroupCriteria:"adGroupCriterionOperation", campaignCriteria:"campaignCriterionOperation", assetGroupAssets:"assetGroupAssetOperation"};
function isVersionApproval(item) { return TYPES.has(item && item.type); }
function validateVersionApproval(item, customerId) {
  if (!isVersionApproval(item)) throw new Error("This is not a version update proposal.");
  const p=item.payload||{},g=p.versionGuard||{};
  if (!/^\d+$/.test(String(g.campaignId||"")) || !Number.isSafeInteger(g.expectedVersion) || g.expectedVersion<1 || !/^[a-f0-9]{64}$/.test(g.snapshotHash||"")) throw new Error("The proposal has no verified source version. Analyze the current ad again.");
  if (String((p.meta||{}).existingCampaignId||g.campaignId)!==String(g.campaignId)) throw new Error("The proposal references different campaigns.");
  if (!p.versionChange || typeof p.versionChange!=="object") throw new Error("The exact version changes are missing.");
  const change=p.versionChange;
  if(String(change.campaignId)!==String(g.campaignId)||change.sourceVersion!==g.expectedVersion||change.proposedVersion!==g.expectedVersion+1)throw new Error("The proposed version does not match its verified source version.");
  if(item.type==="adVersionRestore"&&(!Number.isSafeInteger(change.restoredFromVersion)||change.restoredFromVersion<1||change.restoredFromVersion>=g.expectedVersion))throw new Error("Choose a previous saved version for restoration.");
  if (p.merchantPatch) {
    throw new Error("Merchant product changes must be reviewed in the owning feed. Direct Merchant publication is not supported by this approval workflow.");
  }
  if (p.service && !SERVICES[p.service]) throw new Error("This version proposal uses an unsupported update service.");
  if (p.service && p.mutateOperations) throw new Error("The proposal contains ambiguous update operations.");
  if(p.generatedAssets!=null){
    if(item.type!=="adDesignUpdate"||!Array.isArray(p.generatedAssets)||p.generatedAssets.length>12||p.service)throw new Error("Saved image assets are only supported by an explicit ad design proposal.");
    const seen=new Set();for(const entry of p.generatedAssets){const a=entry&&entry.asset,ref=entry&&entry.tempResourceName;
      if(typeof ref!=="string"||!ref.startsWith("customers/"+customerId+"/assets/")||!/^-[1-9]\d*$/.test(ref.split("/").pop())||seen.has(ref)||!a||!/^Brites_GAds_Creative\/[a-zA-Z0-9_-]+\/[a-zA-Z0-9_-]+\.jpg$/.test(a.path||"")||!/^[a-f0-9]{64}$/.test(a.hash||"")||!Number.isInteger(a.bytes)||a.bytes<1||a.bytes>5*1024*1024||!Number.isInteger(a.width)||!Number.isInteger(a.height)||a.width<128||a.height<128)throw new Error("A generated image has an invalid saved identity, hash or dimensions.");
      seen.add(ref);
    }
  }
  const ops=p.service?(p.operations||[]).map(op=>({type:SERVICES[p.service],op})):(p.mutateOperations||[]).map(x=>({type:Object.keys(x||{})[0],op:Object.values(x||{})[0],keys:Object.keys(x||{}).length}));
  if (!ops.length || ops.length>500) throw new Error("A version update must contain a bounded, explicit change plan.");
  for (const {type,op,keys} of ops) {
    if (!OP_TYPES.has(type) || keys>1 || !op || ["create","update","remove"].filter(k=>op[k]!=null).length!==1) throw new Error("The version change contains an unsupported operation.");
    if ((op.create || op.remove) && ["adOperation","adGroupAdOperation","assetGroupOperation"].includes(type)) throw new Error("Version updates must preserve the existing ad and asset-group IDs.");
    if (type==="assetOperation" && !op.create) throw new Error("Existing asset resources cannot be rewritten.");
    const check=value=>{if(typeof value==="string"&&/^customers\//.test(value)&&!value.startsWith("customers/"+customerId+"/"))throw new Error("An operation belongs to a different Google Ads account.");if(value&&typeof value==="object")Object.values(value).forEach(check);};
    check(op);
  }
  return {campaignId:String(g.campaignId),provider:"googleAds"};
}
function assertVersionOperationScope(item, snapshot, customerId, savedTargetSnapshot) {
  const details=validateVersionApproval(item,customerId),p=item.payload,c=snapshot&&snapshot.components;
  if(!snapshot||!snapshot.complete||String(snapshot.campaignId)!==details.campaignId||!c)throw new Error("Complete settings for this campaign are required to verify the update scope.");
  const restoring=item.type==="adVersionRestore",target=restoring&&savedTargetSnapshot;
  if(restoring&&(!target||!target.complete||String(target.campaignId)!==details.campaignId||target.channel!==snapshot.channel))throw new Error("The original saved restoration settings must be verified before publishing.");
  const rows=key=>Array.isArray(c[key])?c[key]:[],by=key=>new Map(rows(key).map(row=>[row.resourceName,row]));
  const ads=by("searchAds"),keywords=by("keywords"),negatives=by("campaignNegatives"),groups=by("assetGroups"),links=by("assetLinks"),filters=by("listingGroups"),searchImages=by("searchImageLinks");
  const adLinks=new Map(rows("searchAds").map(row=>[row.adGroupAdResourceName,row]));
  const adGroups=new Set([...rows("searchAds"),...rows("keywords")].map(row=>row.adGroup).filter(Boolean));
  const campaign="customers/"+customerId+"/campaigns/"+details.campaignId;
  const prefix="customers/"+customerId+"/",assets=new Set([...rows("assetLinks"),...rows("searchImageLinks")].map(row=>row.asset));
  if(target)for(const row of [...((target.components&&target.components.assetLinks)||[]),...((target.components&&target.components.searchImageLinks)||[])])assets.add(row.asset);
  const ops=p.service?(p.operations||[]).map(op=>({type:SERVICES[p.service],op})):(p.mutateOperations||[]).map(x=>({type:Object.keys(x)[0],op:Object.values(x)[0]}));
  const temporaryAssets=new Set(),temporaryFilters=new Map(),usedAssets=new Set();
  for(const entry of p.generatedAssets||[])temporaryAssets.add(entry.tempResourceName);
  const known=(map,ref,label)=>{if(!map.has(ref))throw new Error("The "+label+" is outside this campaign's verified settings.");return map.get(ref);};
  const own=(ref,kind)=>typeof ref==="string"&&ref.startsWith(prefix+kind+"/");
  const fields=(row,allowed)=>{if(Object.keys(row).some(key=>!allowed.includes(key)))throw new Error("The update contains fields outside the reviewed creative scope.");};
  const mask=(op,allowed)=>{const paths=typeof op.updateMask==="string"?op.updateMask.split(",").map(s=>s.trim()):[];if(!paths.length||paths.some(path=>!allowed.includes(path)))throw new Error("The update mask exceeds the reviewed creative scope.");};
  const status=row=>{if(!["ENABLED","PAUSED"].includes(row.status))throw new Error("This update cannot remove an existing ad or change an unsupported status.");};
  for(const {type,op} of ops){
    if(type==="assetOperation"){
      const row=op.create;fields(row,["resourceName","textAsset","imageAsset","name"]);
      if(!own(row.resourceName,"assets")||!/^-[1-9]\d*$/.test(row.resourceName.split("/").pop())||temporaryAssets.has(row.resourceName)||!row.textAsset&&!row.imageAsset||row.textAsset&&row.imageAsset)throw new Error("A new creative asset must have a unique temporary identity.");
      temporaryAssets.add(row.resourceName);
    }
    if(type==="assetGroupListingGroupFilterOperation"&&op.create){const row=op.create;
      known(groups,row.assetGroup,"asset group");
      if(!own(row.resourceName,"assetGroupListingGroupFilters")||row.resourceName.split("/").pop().split("~")[0]!==row.assetGroup.split("/").pop()||!/^\d+~-[1-9]\d*$/.test(row.resourceName.split("/").pop())||temporaryFilters.has(row.resourceName))throw new Error("A product filter must belong to its existing asset group and have a unique temporary identity.");
      temporaryFilters.set(row.resourceName,row.assetGroup);
    }
  }
  for(const {type,op} of ops){const row=op.update||op.create;
    if(type==="assetOperation")continue;
    if(type==="adOperation"){
      known(ads,row.resourceName,"Search ad");fields(row,["resourceName","responsiveSearchAd","finalUrls","finalMobileUrls"]);mask(op,["responsive_search_ad.headlines","responsive_search_ad.descriptions","responsive_search_ad.path1","responsive_search_ad.path2","final_urls","final_mobile_urls"]);
      if(row.responsiveSearchAd)fields(row.responsiveSearchAd,["headlines","descriptions","path1","path2"]);
    }else if(type==="adGroupAdOperation"){
      known(adLinks,row.resourceName,"Search ad link");fields(row,["resourceName","status"]);mask(op,["status"]);status(row);
    }else if(type==="assetGroupOperation"){
      known(groups,row.resourceName,"asset group");fields(row,["resourceName","finalUrls","finalMobileUrls","status"]);mask(op,["final_urls","final_mobile_urls","status"]);if(row.status!=null)status(row);
    }else if(type==="adGroupCriterionOperation"||type==="campaignCriterionOperation"){
      const isKeyword=type==="adGroupCriterionOperation",set=isKeyword?keywords:negatives;
      if(op.remove){const prior=known(set,op.remove,"keyword");if(!prior.negative)throw new Error("Existing positive keywords must be paused rather than removed.");}
      else if(op.update){known(set,row.resourceName,"keyword");if(!isKeyword)throw new Error("Campaign negative criteria cannot be rewritten.");fields(row,["resourceName","status"]);mask(op,["status"]);status(row);}
      else{if(!restoring)throw new Error("Analysis updates cannot create new keyword criteria.");fields(row,["resourceName","adGroup","campaign","status","negative","keyword"]);
        if(isKeyword?!adGroups.has(row.adGroup):row.campaign!==campaign||row.negative!==true)throw new Error("The new keyword belongs to a different campaign or ad group.");
        if(!row.keyword||typeof row.keyword.text!=="string"||!["EXACT","PHRASE","BROAD"].includes(row.keyword.matchType))throw new Error("The keyword must have exact saved text and a supported match type.");
        if(isKeyword&&row.status!=null)status(row);
      }
    }else if(type==="assetGroupAssetOperation"){
      if(op.remove)known(links,op.remove,"creative asset link");
      else if(op.update){known(links,row.resourceName,"creative asset link");fields(row,["resourceName","status"]);mask(op,["status"]);status(row);}
      else{fields(row,["resourceName","assetGroup","asset","fieldType","status"]);known(groups,row.assetGroup,"asset group");
        if(!own(row.asset,"assets")||!assets.has(row.asset)&&!temporaryAssets.has(row.asset))throw new Error("This asset is not in the current campaign or its verified saved version.");
        usedAssets.add(row.asset);if(row.status!=null)status(row);
      }
    }else if(type==="adGroupAssetOperation"){
      if(snapshot.channel!=="SEARCH"||!Array.isArray(c.searchImageLinks))throw new Error("Verified Search image links are required before updating Search images.");
      if(op.remove)known(searchImages,op.remove,"Search image link");
      else if(op.update){known(searchImages,row.resourceName,"Search image link");fields(row,["resourceName","status"]);mask(op,["status"]);status(row);}
      else{fields(row,["resourceName","adGroup","asset","fieldType","status"]);if(!adGroups.has(row.adGroup)||row.fieldType!=="IMAGE"||!own(row.asset,"assets")||!assets.has(row.asset)&&!temporaryAssets.has(row.asset))throw new Error("The Search image does not belong to this verified ad group or saved version.");usedAssets.add(row.asset);if(row.status!=null)status(row);}
    }else if(type==="assetGroupListingGroupFilterOperation"){
      if(!restoring)throw new Error("Product filter updates require a saved restoration plan.");
      if(op.remove)known(filters,op.remove,"product filter");
      else if(op.create){fields(row,["resourceName","assetGroup","type","listingSource","parentListingGroupFilter","caseValue"]);
        if(!["SUBDIVISION","UNIT_INCLUDED","UNIT_EXCLUDED"].includes(row.type)||row.listingSource!=="SHOPPING")throw new Error("Unsupported saved product filter type.");
        if(row.parentListingGroupFilter){const parent=temporaryFilters.get(row.parentListingGroupFilter)||(filters.get(row.parentListingGroupFilter)||{}).assetGroup;if(parent!==row.assetGroup)throw new Error("A product filter parent belongs to another asset group.");}
      }else throw new Error("Immutable product filter partitions cannot be rewritten.");
    }else throw new Error("This resource is not covered by the saved ad settings and cannot be changed in a version update.");
  }
  if([...temporaryAssets].some(ref=>!usedAssets.has(ref)))throw new Error("The update contains a new asset without a reviewed campaign attachment.");
  return details;
}
function assertVersionReviewed(item, hash, customerId) {
  const details=validateVersionApproval(item,customerId),r=item.versionReview||{};
  if (!r.at || r.payloadHash!==hash(item.payload||{})) throw new Error("Review and approve the exact version changes before publishing.");
  return details;
}
module.exports={isVersionApproval,validateVersionApproval,assertVersionReviewed,assertVersionOperationScope};
