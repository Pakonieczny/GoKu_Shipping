// Ad Design workspaces: durable source selection, generation checkpoints and
// exact reviewed outputs. This module never publishes a Google Ads mutation.
const crypto = require("crypto");
const clean = v => JSON.parse(JSON.stringify(v));
const sha = v => crypto.createHash("sha256").update(typeof v === "string" ? v : JSON.stringify(v)).digest("hex");
const token = value => /^[a-zA-Z0-9_-]{1,100}$/.test(String(value || ""));
const active = job => job && Number(job.leaseUntil) > Date.now() && ["queued", "running"].includes(job.phase);
const MAX_UPLOAD = 4 * 1024 * 1024;
const productKey = id => String(id || '').split('/').pop();
function isSharedProductGroup(workspace, group) {
  if (group.requiresProductSplit) return true;
  const {offerParts, destination}=require('./googleAdsAdDesignContext'),parts=workspace.sourceSnapshot?.components||{};
  if(group.channel==='pmax')return new Set([...(group.productIds||[]).map(productKey),...(parts.listingGroups||[]).filter(f=>f.assetGroup===group.ref&&f.type==='UNIT_INCLUDED').map(f=>offerParts(f.caseValue?.productItemId?.value)?.productId)].filter(Boolean)).size>1;
  const own=(parts.searchAds||[]).find(a=>a.resourceName===group.ref),parent=group.adGroupRef||own?.adGroup;
  return !!parent&&new Set((parts.searchAds||[]).filter(a=>a.adGroup===parent).flatMap(a=>a.finalUrls||[]).map(destination).filter(d=>d?.kind==='product').map(d=>d.handle)).size>1;
}
function placementMatches(p, settings){return p.groupRef===settings.groupRef && (!p.productId && !(p.productIds||[]).length || productKey(p.productId||(p.productIds||[])[0])===productKey(settings.productId));}
function chosenPlacements(workspace){
  const rows=(workspace.placements||[]).filter(p=>placementMatches(p,workspace.settings));
  if(workspace.settings.deviceLinked===false)return rows;
  const out=rows.slice();for(const p of rows){const device=p.device==='desktop'?'mobile':'desktop';if(!out.some(r=>r.format===p.format&&r.device===device))out.push({...p,device,inherited:true});}return out;
}
const FORMATS = [
  { key: "square", label: "Square", width: 1024, height: 1024, requestSize: "1024x1024", fieldType: "SQUARE_MARKETING_IMAGE", required: true },
  { key: "landscape", label: "Landscape", width: 1956, height: 1024, requestSize: "1968x1024", fieldType: "MARKETING_IMAGE", required: true },
  { key: "portrait", label: "Portrait", width: 1024, height: 1280, requestSize: "1024x1280", fieldType: "PORTRAIT_MARKETING_IMAGE", required: true }
];
function settingsFor(raw, products, groups, references, formats, currentCreative = {}) {
  const settings = raw || {}, product = products.find(row => String(row.id) === String(settings.productId || "")) || (settings.productId ? null : products[0]);
  if (!product) throw new Error("No verified product is available for this design.");
  const group = groups.find(row => row.ref === settings.groupRef) || (settings.groupRef ? null : groups[0]);
  if (!group) throw new Error("Choose an ad or asset group before designing.");
  if (Array.isArray(product.eligibleGroupRefs) && !product.eligibleGroupRefs.includes(group.ref)) throw new Error("This product has not been verified for the selected ad group's destination. Refresh its product sources before designing.");
  if ((group.productIds || []).length && !(group.productIds || []).map(String).includes(String(product.id).split("/").pop())) throw new Error("Choose a product belonging to this ad group's verified selection.");
  if ((group.itemIds || []).length) { const productId = String(product.id).split("/").pop(), allowed = (group.itemIds || []).some(id => String(id) === String(product.itemId || product.offerId || "") || (String(id).match(/^shopify_[^_]+_(\d+)_/i) || [])[1] === productId); if (!allowed) throw new Error("Choose a product included in this asset group's verified product selection."); }
  const uploadedSource = references.find(row => row.id === settings.sourceImageId && row.role === "product" && String(row.productId) === String(product.id));
  const image = (product.images || []).find(row => row.id === settings.sourceImageId) || uploadedSource || (settings.sourceImageId ? null : (product.images || [])[0]) || (Array.isArray(settings.selectedImages) ? (product.images || [])[0] || {id:null} : null);
  if (!image) throw new Error("The selected source photograph does not belong to this product.");
  const currentAssetIds = [...new Set((settings.currentAssetIds || []).map(String))];
  if (currentAssetIds.some(id=>!(currentCreative.images||[]).some(a=>a.id===id&&a.groupRef===group.ref))) throw new Error("Choose current Google images belonging to this exact ad group.");
  const referenceIds = [...new Set((settings.referenceIds || []).map(String))];
  if (referenceIds.some(id => !references.some(row => row.id === id))) throw new Error("Choose saved reference images from this workspace.");
  const chosen = Array.isArray(settings.selectedImages) ? settings.selectedImages : (uploadedSource ? [] : [{ productId: String(product.id), imageId: image.id }]);
  if (!Array.isArray(settings.selectedImages) && uploadedSource && !referenceIds.includes(uploadedSource.id)) referenceIds.unshift(uploadedSource.id);
  const selectedImages = [...new Map(chosen.map(row => {
    const p = products.find(p => String(p.id) === String(row.productId)), photo = p && (p.images || []).find(i => i.id === row.imageId);
    if (!photo || Array.isArray(p.creativeGroupRefs) && !p.creativeGroupRefs.includes(group.ref) && !(p.eligibleGroupRefs || []).includes(group.ref)) throw new Error("Choose photos from this ad's listings or its saved related products.");
    return [String(p.id) + ":" + photo.id, { productId: String(p.id), imageId: photo.id }];
  })).values()];
  if (Buffer.byteLength(JSON.stringify({selectedImages,referenceIds,currentAssetIds})) > 128000) throw new Error("This selection is too large to save in one design. Split it into separate compositions.");
  const direction = String(settings.direction || "").trim(); if (direction.length > 2400) throw new Error("Keep the art direction under 2,400 characters.");
  const style = {}; for (const key of ["background", "border", "font", "scale"]) style[key] = String((settings.style || {})[key] || ({ background: "warm ivory", border: "none", font: "Montserrat", scale: "product-led" })[key]).slice(0, 100);
  for(const [key,fallback] of Object.entries({textColor:'#29231d',borderColor:'#d8cbb8',backgroundColor:'#fffdf9',buttonColor:'#33281f'})){const value=String((settings.style||{})[key]||fallback);style[key]=/^#[a-f0-9]{6}$/i.test(value)?value:fallback;}
  style.textSize=Math.max(16,Math.min(36,Number((settings.style||{}).textSize)||22));style.borderWidth=Math.max(0,Math.min(6,Number((settings.style||{}).borderWidth)||0));style.textPlacement=['above','below','beside'].includes((settings.style||{}).textPlacement)?settings.style.textPlacement:'below';
  const requested = Array.isArray(settings.formats) ? settings.formats : formats.filter(row => row.required).map(row => row.key);
  if (requested.some(key => !formats.some(row => row.key === key))) throw new Error("Choose supported Google Ads image formats.");
  return { productId: String(product.id), groupRef: group.ref, sourceImageId: image.id, selectedImages, referenceIds, currentAssetIds, direction, style, deviceLinked:settings.deviceLinked!==false,
    formats: [...new Set([...formats.filter(row => row.required).map(row => row.key), ...requested])] };
}
function refreshedSettings(raw,fallback,products,groups,references,formats,currentCreative={}){
  if(!raw)return {settings:settingsFor(fallback,products,groups,references,formats,currentCreative),warnings:[]};
  const candidate={...raw,productId:fallback.productId,groupRef:fallback.groupRef};
  try{return {settings:settingsFor(candidate,products,groups,references,formats,currentCreative),warnings:[]};}catch(_){/* Recheck saved source identities against the refreshed catalogue below. */}
  const primary=products.find(p=>String(p.id)===String(candidate.productId));
  if(!(primary?.images||[]).some(p=>p.id===candidate.sourceImageId)&&!references.some(r=>r.id===candidate.sourceImageId&&r.role==='product'&&productKey(r.productId)===productKey(candidate.productId)))delete candidate.sourceImageId;
  if(Array.isArray(candidate.selectedImages))candidate.selectedImages=candidate.selectedImages.flatMap(choice=>{const p=products.find(p=>productKey(p.id)===productKey(choice.productId));return p&&(p.images||[]).some(i=>i.id===choice.imageId)&&(!Array.isArray(p.creativeGroupRefs)||p.creativeGroupRefs.includes(candidate.groupRef)||(p.eligibleGroupRefs||[]).includes(candidate.groupRef))?[{productId:String(p.id),imageId:choice.imageId}]:[];});
  candidate.referenceIds=(candidate.referenceIds||[]).filter(id=>references.some(r=>r.id===id));
  candidate.currentAssetIds=(candidate.currentAssetIds||[]).filter(id=>(currentCreative.images||[]).some(a=>a.id===id&&a.groupRef===candidate.groupRef));
  candidate.formats=(candidate.formats||[]).filter(key=>formats.some(f=>f.key===key));
  let settings;try{settings=settingsFor(candidate,products,groups,references,formats,currentCreative);}catch(_){settings=settingsFor(fallback,products,groups,references,formats,currentCreative);}
  return {settings,warnings:['Some previously selected source photos or ad assets are no longer valid for this product and group. Valid selections and saved artwork were retained.']};
}
function responseText(result) {
  if (typeof result.output_text === "string") return result.output_text;
  return (result.output || []).flatMap(row => row.content || []).filter(row => row.type === "output_text").map(row => row.text || "").join("\n");
}
function formatAssets(result,key){
  const variants=result.placementAssets||{},assets=['desktop','mobile'].map(device=>variants[device]&&variants[device][key]).filter(Boolean);
  if(!assets.length&&result.assets&&result.assets[key])assets.push(result.assets[key]);
  return [...new Map(assets.map(a=>[a.hash,a])).values()];
}
function buildVersionDesignPayload({ workspaceId, jobId, workspace, group, product, result, customerId, selection={formats:FORMATS.map(f=>f.key),copy:true} }) {
  if(!Array.isArray(selection.formats)||selection.formats.some(k=>!FORMATS.some(f=>f.key===k))||!selection.formats.length&&!selection.copy)throw new Error('Choose an image format or messaging to approve.');
  if (result.publication && result.publication.ready === false) throw new Error(result.publication.reason || "Review the destination for these products before creating an approval.");
  const snapshot = workspace.sourceSnapshot, campaignId = String(workspace.context.campaignId || ""), copy = result.copy || {}, components = snapshot && snapshot.components;
  if (!snapshot || !snapshot.complete || String(snapshot.campaignId) !== campaignId || !Number.isSafeInteger(workspace.sourceVersion) || !/^[a-f0-9]{64}$/.test(workspace.snapshotHash || "")) throw new Error("The design must be tied to a complete saved version of this campaign.");
  if (selection.copy&&(!Array.isArray(copy.headlines) || !Array.isArray(copy.descriptions) || copy.headlines.length < 3 || copy.headlines.length > 15 || copy.descriptions.length < 2 || copy.descriptions.length > 5)) throw new Error("The generated copy is incomplete.");
  if (selection.copy&&(copy.headlines.some(text => typeof text !== "string" || !text.trim() || text.length > 30) || copy.descriptions.some(text => typeof text !== "string" || !text.trim() || text.length > 90))) throw new Error("The generated copy does not meet Google Ads text limits.");
  const prefix = "customers/" + customerId + "/", operations = [], generatedAssets = [], changes = [], notes = []; let nextText = -970001, nextImage = -980001;
  const imageDescriptor = (key,chosen) => {
    const asset = chosen || result.assets && result.assets[key];
    if (!asset || !/^Brites_GAds_Creative\/[a-zA-Z0-9_-]+\/[a-zA-Z0-9_-]+\.jpg$/.test(asset.path || "") || !/^[a-f0-9]{64}$/.test(asset.hash || "") || !Number.isFinite(asset.width) || !Number.isFinite(asset.height) || asset.width < 1 || asset.height < 1 || Number(asset.bytes) <= 0 || Number(asset.bytes) > 5120000) throw new Error("Every required image format must be saved and verified before a version proposal is created.");
    const tempResourceName = prefix + "assets/" + nextImage--; generatedAssets.push({ tempResourceName, asset: clean(asset) }); return { tempResourceName, asset };
  };
  if (group.channel === "search") {
    const ad = components.searchAds.find(row => row.resourceName === group.ref);
    if (!ad || !ad.resourceName.startsWith(prefix + "ads/")) throw new Error("The selected Search ad is outside the saved campaign.");
    if (selection.copy&&copy.descriptions.length > 4) throw new Error("Search ads support at most four descriptions.");
    const preservePins = field => {
      const prior = ad.responsiveSearchAd && ad.responsiveSearchAd[field] || [];
      if (prior.some(row => row.pinnedField && !["UNKNOWN", "UNSPECIFIED"].includes(row.pinnedField) && !copy[field].includes(row.text))) throw new Error("A pinned Search text must be kept unchanged in this design. Review a concept that preserves the existing pinned " + field + ".");
      return copy[field].map(text => { const pinned = prior.find(row => row.text === text && row.pinnedField && !["UNKNOWN", "UNSPECIFIED"].includes(row.pinnedField)); return { text, ...(pinned ? { pinnedField: pinned.pinnedField } : {}) }; });
    };
    if(selection.copy){operations.push({ adOperation: { update: { resourceName: ad.resourceName, responsiveSearchAd: { headlines: preservePins("headlines"), descriptions: preservePins("descriptions") } }, updateMask: "responsive_search_ad.headlines,responsive_search_ad.descriptions" } });
    changes.push({ category: "Search copy", target: group.ref, field: "responsiveSearchAd", before: ad.responsiveSearchAd, after: copy, reason: result.brief.rationale });
    }
    if(selection.formats.includes('portrait')){if(selection.formats.length===1)throw new Error('Search image assets support square and landscape, not portrait.');}
    if(selection.formats.some(k=>k!=='portrait')){
    if (!Array.isArray(components.searchImageLinks)) throw new Error("The Search ad group's image links must be captured before its images can be updated.");
    const oldImages = components.searchImageLinks.filter(row => row.adGroup === ad.adGroup && row.fieldType === "IMAGE" && (selection.formats.includes('square')&&selection.formats.includes('landscape')||selection.formats.some(k=>Math.abs(Number(row.width)/Number(row.height)-(k==='square'?1:1.91))<.04)));
    oldImages.forEach(row => operations.push({ adGroupAssetOperation: { remove: row.resourceName } }));
    for (const key of ["square", "landscape"].filter(k=>selection.formats.includes(k))) { const choices=formatAssets(result,key);if(!choices.length)imageDescriptor(key);for(const asset of choices){const image = imageDescriptor(key,asset); operations.push({ adGroupAssetOperation: { create: { adGroup: ad.adGroup, asset: image.tempResourceName, fieldType: "IMAGE" } } });} }
    changes.push({ category: "Search images", before: oldImages.map(row => ({ asset: row.asset, imageUrl: row.imageUrl || null })), after: generatedAssets.map(row => ({ path: row.asset.path, hash: row.asset.hash, width: row.asset.width, height: row.asset.height })), reason: "Attach the exact reviewed square and landscape photographs to this existing Search ad group." });
    }
    notes.push("The Search ad and ad-group IDs stay the same. Square and landscape images are attached to the existing ad group; portrait is a design preview because Search image assets do not use that format. Google controls whether an image serves.");
  } else if (group.channel === "pmax") {
    if (!components.assetGroups.some(row => row.resourceName === group.ref) || !String(group.ref).startsWith(prefix + "assetGroups/")) throw new Error("The selected asset group is outside the saved campaign.");
    const old = components.assetLinks.filter(row => row.assetGroup === group.ref), textFields = [["headlines", "HEADLINE"], ["longHeadlines", "LONG_HEADLINE"], ["descriptions", "DESCRIPTION"]];
    if (selection.copy&&(!Array.isArray(copy.longHeadlines) || copy.longHeadlines.length < 1 || copy.longHeadlines.length > 5 || copy.longHeadlines.some(text => typeof text !== "string" || !text.trim() || text.length > 90))) throw new Error("Performance Max long headlines are incomplete.");
    for (const [key, fieldType] of selection.copy?textFields:[]) {
      old.filter(row => row.fieldType === fieldType).forEach(row => operations.push({ assetGroupAssetOperation: { remove: row.resourceName } }));
      for (const text of copy[key]) { const resourceName = prefix + "assets/" + nextText--; operations.push({ assetOperation: { create: { resourceName, textAsset: { text } } } }, { assetGroupAssetOperation: { create: { assetGroup: group.ref, asset: resourceName, fieldType } } }); }
      changes.push({ category: key === "descriptions" ? "Descriptions" : "Headlines", target: group.ref, field: key, before: old.filter(row => row.fieldType === fieldType).map(row => row.text || row.asset), after: copy[key], reason: result.brief.rationale });
    }
    for (const format of FORMATS.filter(f=>selection.formats.includes(f.key))) {
      old.filter(row => row.fieldType === format.fieldType).forEach(row => operations.push({ assetGroupAssetOperation: { remove: row.resourceName } }));
      const choices=formatAssets(result,format.key);if(!choices.length)imageDescriptor(format.key);
      const after=choices.map(chosen=>{const {tempResourceName,asset}=imageDescriptor(format.key,chosen);operations.push({assetGroupAssetOperation:{create:{assetGroup:group.ref,asset:tempResourceName,fieldType:format.fieldType}}});return {format:format.key,path:asset.path,hash:asset.hash,width:asset.width,height:asset.height};});
      changes.push({ category: "Images", target: group.ref, field: format.fieldType, before: old.filter(row => row.fieldType === format.fieldType).map(row => ({ asset: row.asset, imageUrl: row.imageUrl || null })), after: after.length===1?after[0]:after, reason: "Use the reviewed " + format.label.toLowerCase() + " photograph of " + product.title + " on the same asset group." });
    }
    notes.push("Google keeps the existing campaign and asset-group IDs. New immutable image/text assets are linked in the same approved update; Merchant feed product photos stay unchanged.");
  } else throw new Error("Choose a Search ad or Performance Max asset group.");
  if(result.placementAssets)notes.push('Desktop and mobile choices are saved separately in this design. Google receives their distinct approved assets in the responsive asset pool and controls device delivery.');
  if(isSharedProductGroup(workspace,group))throw new Error('Split the shared group before publishing this product design.');
  const versionChange = { campaignId, sourceVersion: workspace.sourceVersion, proposedVersion: workspace.sourceVersion + 1, changes, identityPreserved: true, notes, merchantChanges: false,
    rationale: result.brief.rationale, hypothesis: result.brief.hypothesis, successMetric: result.brief.successMetric, measurementPlan: result.brief.measurementPlan || null };
  return { mutateOperations: operations, generatedAssets, versionGuard: { campaignId, expectedVersion: workspace.sourceVersion, snapshotHash: workspace.snapshotHash }, versionChange,
    adDesign: { workspaceId, jobId, productId: String(product.id), sourceImageId: workspace.settings.sourceImageId, settings: workspace.settings, evidenceHash: result.evidence && result.evidence.hash || null, sourceIds: result.sourceIds || [], productIds: result.productIds || [String(product.id)], inputCoverage: result.inputCoverage || null, quality: result.quality, previewOnlyFormats: group.channel === "search" ? ["portrait"] : [], generatedAt: Date.now() },
    meta: { existingCampaignId: campaignId, adDesignWorkspaceId: workspaceId, adDesignId: jobId, operation: "reviewedAdDesign" } };
}
function createAdDesignService(deps) {
  const f = () => { const value = deps.fb(); if (!value) throw new Error("Ad Design storage is unavailable."); return value; };
  const refFor = id => { if (!token(id)) throw new Error("Invalid design workspace."); return f().db.collection(deps.COL.state).doc("adDesign").collection("workspaces").doc(id); };
  const formats = () => deps.formats || FORMATS;
  const provider = () => ({ available: !!deps.env.OPENAI_API_KEY, label: "GPT Image 2.5 Sunburst", quality: "high", referenceLimit: 16, selectedPhotoLimit: 144, formats: formats(), ...deps.provider, model: "gpt-image-2.5-sunburst" });
  function settle(job, key, output) {
    const reservations = job.reservations || [], reservation = reservations.find(row => row.requestId === output.requestId) || [...reservations].reverse().find(row => row.key === key && !row.settled);
    if (!reservation) return;
    reservation.settled = true;
    if (output.costEstimated === false && Number.isFinite(Number(output.estimatedUsd)) && Number(output.estimatedUsd) >= 0) reservation.actualUsd = Number(output.estimatedUsd);
    const usage = { key, requestId: reservation.requestId, at: Date.now(), usage: output.usage || {}, providerModel: output.providerModel || null, reservedUsd: reservation.reservedUsd,
      estimatedUsd: reservation.actualUsd == null ? reservation.reservedUsd : reservation.actualUsd, costEstimated: reservation.actualUsd == null };
    job.usage = (job.usage || []).filter(row => row.requestId !== reservation.requestId).concat(usage);
  }
  const creativeFor = workspace => deps.currentCreative ? deps.currentCreative(workspace.sourceSnapshot,workspace.context) : workspace.context.currentCreative || {};
  async function read(id) { const ref=refFor(id),s = await ref.get(); if (!s.exists) throw new Error("Ad Design workspace was not found."); const value=s.data();
    if(value.archivedAt)throw new Error('This ad was deleted. Its saved design history is preserved.');
    // This precise legacy error was thrown by buildRequest before responses().
    // Recover it without pretending an unknown network request was uncharged.
    if(value.job&&value.job.inFlight&&value.job.inFlight.key==='copy'&&value.job.error==='Product research exceeds the bounded copy allowance; narrow the selected products.'){
      await f().db.runTransaction(async tx=>{const latest=await tx.get(ref),w=latest.data(),j=w.job;if(!j||!j.inFlight||j.id!==value.job.id||j.error!==value.job.error)return;const receipt=await tx.get(ref.collection('outputs').doc(j.id+'_copy'));if(receipt.exists)return;
        (j.reservations||[]).filter(r=>r.requestId===j.inFlight.requestId).forEach(r=>{r.settled=true;r.actualUsd=0;r.notDispatched=true;});j.inFlight=null;j.error='Research preparation has been repaired. Resume saved work; no provider request was sent for the failed step.';tx.update(ref,{job:j});value.job=j;
      });
    }return value; }
  async function productsFor(ref, workspace) { if (!workspace) { const s = await ref.get(); workspace = s.exists && s.data(); } if (!workspace || !token(workspace.sourceSetId)) throw new Error("Saved product sources are unavailable."); const result = await ref.collection("sourceSets").doc(workspace.sourceSetId).collection("products").get(); return result.docs.map(row => row.data()).sort((a, b) => (a.position || 0) - (b.position || 0)); }
  async function status({ workspaceId } = {}) {
    const ref = refFor(workspaceId), workspace = await read(workspaceId), products = await productsFor(ref, workspace), job = workspace.job || {}, references = [];
    for (const reference of workspace.references || []) references.push({ ...reference, url: await deps.signAsset(reference.asset) });
    const result = job.result ? clean(job.result) : null;
    const messaging=workspace.messaging&&workspace.messaging.groupRef===workspace.settings.groupRef&&productKey(workspace.messaging.productId)===productKey(workspace.settings.productId)?workspace.messaging:null;
    if(result&&messaging)result.copy=messaging.copy;
    if (result) for (const asset of Object.values(result.assets || {})) asset.url = await deps.signAsset(asset);
    if (result && result.logo) result.logo.url = await deps.signAsset(result.logo);
    if(result)for(const assets of Object.values(result.placementAssets||{}))for(const asset of Object.values(assets))asset.url=await deps.signAsset(asset);
    const library = await ref.collection("imageLibrary").get(), imageLibrary = [];
    for (const doc of library.docs) { const image=doc.data(); imageLibrary.push({...image,url:await deps.signAsset(image.asset),...(image.originalAsset?{originalUrl:await deps.signAsset(image.originalAsset)}:{})}); }
    // Index already-paid images from workspaces created before the image library existed.
    for(const [format,asset] of Object.entries(job.assets||{})){
      if((job.placements||[]).some(p=>p.asset.hash===asset.hash)||imageLibrary.some(i=>i.asset.hash===asset.hash&&i.kind==='generated'))continue;
      const id='generated_'+sha(asset.path).slice(0,32),image={id,kind:'generated',groupRef:workspace.settings.groupRef,format,title:(products.find(p=>String(p.id)===String(workspace.settings.productId))||{}).title||'Saved design',productIds:job.result&&job.result.productIds||[String(workspace.settings.productId)],asset,createdAt:job.completedAt||job.createdAt||Date.now(),jobId:job.id};
      await ref.collection('imageLibrary').doc(id).set(image);imageLibrary.push({...image,url:await deps.signAsset(asset)});
    }
    // AI originals belong to the listing/group, independent of workspace versions.
    for(const image of imageLibrary.filter(i=>i.kind==='generated'&&i.groupRef===workspace.settings.groupRef&&(productKey(i.ownerProductId)===productKey(workspace.settings.productId)||(i.productIds||[]).some(id=>productKey(id)===productKey(workspace.settings.productId)))))await archiveGenerated(workspace,image);
    for(const image of await generatedGallery(workspace)){
      const index=imageLibrary.findIndex(i=>i.id===image.id);if(index>=0)imageLibrary[index]=image;else imageLibrary.push(image);
      await ref.collection('imageLibrary').doc(image.id).set(clean(image));
    }
    let hasReceipt = false;
    if (job.inFlight && !active(job)) { const receipt = await ref.collection("outputs").doc(job.id + "_" + job.inFlight.key).get(); hasReceipt = !!(receipt.exists && (receipt.data().stageResult || receipt.data().rawResponse || receipt.data().asset)); }
    const approvalId = job.result && job.result.publication && job.result.publication.ready === false ? null : job.approvalId || workspace.context.approvalId || null, review = approvalId && deps.reviewStatus ? await deps.reviewStatus(approvalId) : null;
    const gallery=await savedDesignGallery(workspace),imageAccess=deps.imageAccessStatus?deps.imageAccessStatus():null,warnings=[...new Set([...(workspace.context.warnings||[]),...(workspace.refreshWarnings||[]),...(imageAccess?.message?[imageAccess.message]:[])])];
    return { ok: true, workspaceId, revision:Number(workspace.revision||0), sourceVersion: workspace.sourceVersion || null, snapshotHash: workspace.snapshotHash || null,
      context: {...workspace.context,warnings,currentCreative:creativeFor(workspace)}, imageAccess, products, references, imageLibrary, ...gallery, placements:chosenPlacements(workspace), settings: workspace.settings, messaging, jobMode:job.mode||null, publication:workspace.publication||null, status: job.phase || "draft", phase: job.phase || "draft",
      progress: job.progress || { pct: 0, label: "Choose your product, reference images and direction" }, result,
      approvalId, review, error: job.error || null,
      canRetry: !active(job) && (!job.inFlight || hasReceipt) && ["paused", "needs_attention", "queued", "running"].includes(job.phase), needsNewRequestApproval: !!job.inFlight && !active(job) && !hasReceipt,
      provider: provider(), formats: formats(), usage: job.usage || [], generatedAt: job.completedAt || null,
      controlsMeaning: "Background and scale guide the photograph. Font and border preview the composition; Google chooses responsive typography and layout." };
  }
  async function workspace(input = {}) {
    if (input.workspaceId&&!input.force) return status({ workspaceId: input.workspaceId });
    const existingId=input.workspaceId&&input.force?String(input.workspaceId):null,existing=existingId?await read(existingId):null;
    if(existing){
      // A refresh is bound to the saved workspace, including legacy unscoped
      // and version-derived IDs. Request fields cannot redirect its identity.
      const context=existing.context||{};
      input={...context,workspaceId:existingId,force:true,productId:context.scopeProductId||null,selectedProductId:existing.settings.productId,groupRef:(context.groups||[]).length===1?context.groups[0].ref:null};
    }
    const contextKey = { ...(input.productId?{productId:productKey(input.productId)}:{}), campaignId: String(input.campaignId || ""), approvalId: String(input.approvalId || ""), handle: String(input.handle || ""), groupRef: String(input.groupRef || ""),
      itemIds: input.campaignId || input.approvalId ? [] : [...new Set((input.itemIds || []).map(String))].sort(), feedLabel: input.campaignId || input.approvalId ? "" : String(input.feedLabel || "").toUpperCase() };
    if (!contextKey.campaignId && !contextKey.approvalId && !contextKey.handle) throw new Error("Open Ad Design from a campaign, draft or product opportunity.");
    let id = existingId||"design_" + sha(JSON.stringify(contextKey)).slice(0, 32), ref = refFor(id), saved = existing?{exists:true,data:()=>existing}:await ref.get();
    if (saved.exists && input.campaignId && !input.approvalId && !input.force) {
      const latest = await deps.verifyBasis({campaignId:String(input.campaignId)});
      if (latest && latest.snapshotHash && (Number(latest.version)!==Number(saved.data().sourceVersion)||latest.snapshotHash!==saved.data().snapshotHash)) {
        id = "design_" + sha({...contextKey,sourceVersion:latest.version,snapshotHash:latest.snapshotHash}).slice(0,32);ref=refFor(id);saved=await ref.get();
      }
    }
    if (saved.exists && !input.force) return status({ workspaceId: id });
    if (saved.exists && (active(saved.data().job) || saved.data().job && saved.data().job.inFlight)) throw new Error("Resolve the current or unconfirmed design request before refreshing its source context.");
    if(saved.exists&&saved.data().editorAI?.id){const ai=await ref.collection('editorAIJobs').doc(saved.data().editorAI.id).get();if(ai.exists&&(ai.data().inFlight||['queued','running'].includes(ai.data().phase)))throw new Error('Resolve the current AI editor request before refreshing product sources. Its paid work is preserved.');}
    const loaded = await deps.loadContext(input), products = loaded.products || [], groups = loaded.context.groups || [];
    if (!products.length) throw new Error("No verified product photographs were found for this context.");
    if(input.productId&&!products.some(p=>productKey(p.id)===productKey(input.productId)))throw new Error("This product is outside the selected group. Choose its own workspace.");
    loaded.context.scopeProductId=input.productId||null;
    if(input.productId&&input.campaignId&&deps.findLegacyEditorWorkspaces)loaded.context.legacyEditorWorkspaceIds=await deps.findLegacyEditorWorkspaces({campaignId:String(input.campaignId),groupRef:input.groupRef,workspaceId:id});
    const sourceSetId = crypto.randomUUID();
    await Promise.all(products.map((product, position) => ref.collection("sourceSets").doc(sourceSetId).collection("products").doc(sha(String(product.id)).slice(0, 32)).set(clean({ ...product, position }))));
    const prior = saved.exists ? saved.data() : null;
    const initialGroup = groups.find(group => group.ref === (input.groupRef||prior?.settings?.groupRef)) || groups[0], initialProduct = products.find(product => input.productId&&productKey(product.id)===productKey(input.productId)) || products.find(product=>prior?.settings?.groupRef===initialGroup.ref&&productKey(product.id)===productKey(prior.settings.productId)&&(!Array.isArray(product.eligibleGroupRefs)||product.eligibleGroupRefs.includes(initialGroup.ref))) || products.find(product => (product.images || []).length && (!(initialGroup.productIds || []).length || initialGroup.productIds.map(String).includes(String(product.id).split("/").pop()))) || products.find(product => (product.images || []).length) || products[0];
    const references = prior && prior.references || [],productDesigns=clean(prior?.productDesigns||{}),previousScope=prior?.settings,matchingPrior=previousScope?.groupRef===initialGroup.ref&&productKey(previousScope.productId)===productKey(initialProduct.id);
    if(previousScope){const key=sha([previousScope.groupRef,previousScope.productId]).slice(0,32),existing=productDesigns[key]||{};productDesigns[key]={...existing,settings:previousScope,messaging:prior.messaging||null,jobId:prior.job?.id||existing.jobId||null};}
    const cachedScope=matchingPrior?{settings:previousScope,messaging:prior.messaging}:Object.values(productDesigns).find(d=>d.settings?.groupRef===initialGroup.ref&&productKey(d.settings.productId)===productKey(initialProduct.id));
    const restored=refreshedSettings(cachedScope?.settings,{productId:initialProduct.id,groupRef:initialGroup.ref,currentAssetIds:((loaded.context.currentCreative||{}).images||[]).filter(a=>a.groupRef===initialGroup.ref&&!/LOGO/.test(a.fieldType)).map(a=>a.id)},products,groups,references,formats(),loaded.context.currentCreative),settings=restored.settings;
    const messaging=cachedScope?.messaging&&cachedScope.messaging.groupRef===settings.groupRef&&productKey(cachedScope.messaging.productId)===productKey(settings.productId)?{...cachedScope.messaging,productId:settings.productId}:null;
    await f().db.runTransaction(async tx => { const latest = await tx.get(ref), current = latest.exists ? latest.data() : null;
      if(current?.editorAI?.id){const ai=await tx.get(ref.collection('editorAIJobs').doc(current.editorAI.id));if(ai.exists&&(ai.data().inFlight||['queued','running'].includes(ai.data().phase)))throw new Error('The AI editor is still using these product sources. Its paid request is preserved.');}
      if (!!current !== !!prior || current && (Number(current.revision) !== Number(prior.revision) || (current.job && current.job.id || null) !== (prior.job && prior.job.id || null) || active(current.job) || current.job && current.job.inFlight)) throw new Error("The workspace changed while its sources were being refreshed. The current design was preserved.");
      if (current && current.job) tx.set(ref.collection("history").doc(current.job.id), current.job);
      tx.set(ref, clean({ schema: 1, workspaceId: id, context: loaded.context, sourceVersion: loaded.sourceVersion || null, snapshotHash: loaded.snapshotHash || null,
        sourceSnapshot: loaded.snapshot || null, sourceSetId, productsIds: products.map(row => row.id), settings, references, productDesigns, messaging, refreshWarnings:restored.warnings, placements:prior&&prior.placements||[], job: null, editorAI:current?.editorAI||null, revision: Number(prior && prior.revision || 0) + 1, createdAt: prior && prior.createdAt || Date.now(), updatedAt: Date.now() }));
    });
    return status({ workspaceId: id });
  }
  async function save(input = {}) {
    const ref = refFor(input.workspaceId), sourceWorkspace = await read(input.workspaceId), products = await productsFor(ref, sourceWorkspace);
    await f().db.runTransaction(async tx => { const s = await tx.get(ref); if (!s.exists) throw new Error("Workspace was not found."); const value = s.data();
      if (value.sourceSetId !== sourceWorkspace.sourceSetId) throw new Error("The gallery changed. Reload the workspace before saving these selections.");
      if (active(value.job)) throw new Error("Wait for the current design to finish before changing its direction.");
      if(value.context.scopeProductId&&input.productId&&productKey(value.context.scopeProductId)!==productKey(input.productId))throw new Error("Open the selected product’s own workspace.");
      const changingProduct=!!(input.productId&&productKey(input.productId)!==productKey(value.settings.productId)||input.groupRef&&input.groupRef!==value.settings.groupRef),designs=value.productDesigns||{},priorKey=sha([value.settings.groupRef,value.settings.productId]).slice(0,32),nextKey=sha([input.groupRef||value.settings.groupRef,String(input.productId||value.settings.productId)]).slice(0,32);
      const priorDesign=changingProduct&&designs[nextKey];
      const incoming = changingProduct?{...(priorDesign&&priorDesign.settings||{}),productId:input.productId||value.settings.productId,groupRef:input.groupRef||value.settings.groupRef,deviceLinked:priorDesign?priorDesign.settings.deviceLinked!==false:value.settings.deviceLinked!==false}:{ ...value.settings, ...input };
      // Older callers select a single photo using sourceImageId. New clients always send the exact multi-selection.
      if (!priorDesign&&!Object.prototype.hasOwnProperty.call(input, "selectedImages") && (input.sourceImageId && input.sourceImageId !== value.settings.sourceImageId || input.productId && input.productId !== value.settings.productId)) delete incoming.selectedImages;
      const settings = settingsFor(incoming, products, value.context.groups || [], value.references || [], formats(), creativeFor(value));
      if (sha(settings) === sha(value.settings)) return;
      let previousSettings=null;try{previousSettings=settingsFor(value.settings,products,value.context.groups||[],value.references||[],formats(),creativeFor(value));}catch(e){/* A valid new selection may replace an unavailable old photo. */}
      const generationSettings=s=>({...s,style:Object.fromEntries(['background','border','font','scale'].map(k=>[k,s.style[k]]))});
      // Display controls and newly introduced defaults do not discard paid work.
      if(!changingProduct&&previousSettings&&sha(generationSettings(settings))===sha(generationSettings(previousSettings))){const job=value.job?{...value.job,settingsHash:sha(settings)}:null;tx.update(ref,{settings,job,revision:Number(value.revision||0)+1,updatedAt:Date.now()});return;}
      if (value.job && value.job.inFlight) throw new Error("The previous paid request has an unconfirmed outcome. Resolve it before changing this workspace.");
      const priorJob=priorDesign&&priorDesign.jobId?await tx.get(ref.collection('history').doc(priorDesign.jobId)):null;
      const cachedJob=priorJob&&priorJob.exists?priorJob.data():priorDesign&&priorDesign.job||null,restored=cachedJob&&!cachedJob.resetAt&&!(value.lastFailureResetAt>=Number(cachedJob.updatedAt||cachedJob.createdAt||0)&&!cachedJob.inFlight&&['needs_attention','paused','queued','running'].includes(cachedJob.phase))&&(cachedJob.sourceVersion||null)===(value.sourceVersion||null)&&(cachedJob.snapshotHash||null)===(value.snapshotHash||null)&&cachedJob.settingsHash===sha(settings)?cachedJob:null;
      if (value.job) tx.set(ref.collection("history").doc(value.job.id), value.job);
      if(changingProduct)designs[priorKey]={settings:value.settings,messaging:value.messaging||null,jobId:value.job&&value.job.id||null};
      tx.update(ref, { settings, job: restored?{...restored,settingsHash:sha(settings)}:null,...(changingProduct?{productDesigns:designs,messaging:priorDesign&&priorDesign.messaging||null}:{}), revision: Number(value.revision || 0) + 1, updatedAt: Date.now() });
    }); return status({ workspaceId: input.workspaceId });
  }
  async function upload({ workspaceId, fileName, mimeType, dataBase64, role = "inspiration" } = {}) {
    if (!["image/jpeg", "image/png", "image/webp"].includes(mimeType) || !["inspiration", "product"].includes(role)) throw new Error("Upload a JPEG, PNG or WebP product or inspiration image.");
    if (typeof dataBase64 !== "string" || dataBase64.length > Math.ceil(MAX_UPLOAD * 4 / 3) + 4 || !/^[A-Za-z0-9+/]*={0,2}$/.test(dataBase64)) throw new Error("Reference images must be no larger than 4 MiB.");
    const original = Buffer.from(dataBase64, "base64"); if (!original.length || original.length > MAX_UPLOAD) throw new Error("Reference images must be no larger than 4 MiB.");
    const ref = refFor(workspaceId), workspace = await read(workspaceId); if (active(workspace.job)) throw new Error("Wait for the current design before adding a reference.");
    const normalized = await deps.normalizeUpload(original), id = "ref_" + sha([role, role === "product" ? workspace.settings.productId : null, sha(normalized.bytes.toString("base64"))]).slice(0, 24);
    const asset = await deps.saveAsset(workspaceId, normalized.bytes, id, { width: normalized.width, height: normalized.height, kind: role });
    const originalAsset = await deps.saveAsset(workspaceId, original, id+"_original", {kind:"original upload",mimeType});
    await f().db.runTransaction(async tx => { const s = await tx.get(ref); if (!s.exists || active(s.data().job) || role === "product" && s.data().settings.productId !== workspace.settings.productId) throw new Error("The workspace changed while the upload was being saved."); const refs = s.data().references || [];
      if (refs.some(row => row.id === id)) return; if (Buffer.byteLength(JSON.stringify(refs)) > 128000) throw new Error("This reference collection is too large for one workspace. Start another design to add more uploads.");
      tx.update(ref, { references: refs.concat({ id, fileName: String(fileName || "Reference image").slice(0, 160), role, productId: s.data().settings.productId, asset, originalAsset, uploadedAt: Date.now() }), revision: Number(s.data().revision || 0) + 1, updatedAt: Date.now() });
    }); return { ok: true, workspaceId, id, role, url: await deps.signAsset(asset), asset };
  }
  // Resolve only saved workspace sources. Originals remain available for crops
  // and the layer editor; thumbnail and provider reference derivatives are never masters.
  async function editorSource({workspaceId,productId,groupRef,source}={}) {
    const ref=refFor(workspaceId),w=await read(workspaceId);editorScope(w,{productId,groupRef});
    if(!source||!['product','upload','current','library'].includes(source.kind))throw new Error('Choose a photo from this workspace.');
    const id='photo_'+sha([groupRef,productId,source]).slice(0,32),target=ref.collection('editorSources').doc(id),cached=await target.get();
    if(cached.exists){const row=cached.data();return {ok:true,...row,url:await deps.signAsset(row.asset,{required:true})};}
    let bytes,title='Ad photo',productIds=[];
    if(source.kind==='product'){
      const products=await productsFor(ref,w),p=products.find(p=>String(p.id)===String(source.productId)),photo=p&&(p.images||[]).find(i=>i.id===source.imageId);
      if(!photo||(Array.isArray(p.creativeGroupRefs)?!p.creativeGroupRefs.includes(groupRef)&&!(p.eligibleGroupRefs||[]).includes(groupRef):Array.isArray(p.eligibleGroupRefs)&&!p.eligibleGroupRefs.includes(groupRef)))throw new Error('This photo is outside this ad’s product library.');
      bytes=await deps.fullSourceBytes(photo.url);title=p.title;productIds=[String(p.id)];
    }else if(source.kind==='upload'){
      const p=(w.references||[]).find(p=>p.id===source.imageId);if(!p||p.role==='product'&&productKey(p.productId)!==productKey(productId))throw new Error('This upload belongs to another product.');
      bytes=await deps.loadAsset(p.originalAsset||p.asset);title=p.fileName;productIds=p.role==='product'?[String(p.productId)]:[];
    }else if(source.kind==='current'){
      const p=(creativeFor(w).images||[]).find(p=>p.id===source.imageId&&p.groupRef===groupRef);if(!p)throw new Error('Current ad image was not found in this group.');
      bytes=await deps.fullSourceBytes(p.url);title=/LOGO/.test(p.fieldType||'')?'Brand logo':'Current ad image';
    }else{
      if(!token(source.imageId))throw new Error('Invalid saved image.');const saved=await ref.collection('imageLibrary').doc(source.imageId).get(),p=saved.exists&&saved.data();
      if(!p||p.groupRef!==groupRef)throw new Error('This saved image belongs to another ad group.');
      // A chosen crop is intentional; use its complete saved 2K pixels, not its thumbnail.
      bytes=await deps.loadAsset(p.asset);title=p.title;productIds=p.productIds||[];
    }
    const sharp=require('sharp'),input=sharp(bytes,{limitInputPixels:40000000}),meta=await input.metadata();
    if(!['jpeg','png','webp'].includes(meta.format)||Number(meta.pages||1)>1)throw new Error('Choose a still JPEG, PNG or WebP photo.');
    const out=await input.rotate().png().toBuffer({resolveWithObject:true});
    const asset=await deps.saveAsset(workspaceId,out.data,id,{width:out.info.width,height:out.info.height,mimeType:'image/png',kind:'editor source'});
    const row={id,groupRef,productId,productIds,title:String(title||'Ad photo').slice(0,200),source,asset,width:out.info.width,height:out.info.height,createdAt:Date.now()};
    await target.set(clean(row));return {ok:true,...row,url:await deps.signAsset(asset,{required:true})};
  }
  function editorScope(w,input){if(String(input.productId)!==String(w.settings.productId)||input.groupRef!==w.settings.groupRef)throw new Error('The advertised product or ad group changed. Reopen the editor for the selected product.');}
  function savedDesignScope(w){
    const context=w.context||{},owner=context.campaignId?'campaign:'+context.campaignId:context.approvalId?'approval:'+context.approvalId:'collection:'+context.handle;
    return f().db.collection(deps.COL.state).doc('adDesignSaved').collection('scopes').doc(sha([owner,w.settings.groupRef,productKey(w.settings.productId)]).slice(0,40));
  }
  function savedDesignsRef(w){return savedDesignScope(w).collection('designs');}
  async function galleryTargets(w){const scope=savedDesignScope(w),meta=await scope.get(),data=meta.exists?meta.data():{},aliases=data.sourceGroups||[],targets=[{scope,groupRef:w.settings.groupRef},...aliases.filter(g=>typeof g==='string'&&g!==w.settings.groupRef).slice(0,12).map(groupRef=>({scope:savedDesignScope({...w,settings:{...w.settings,groupRef}}),groupRef})),...(data.sourceScopes||[]).filter(x=>/^[a-f0-9]{40}$/.test(x.id)&&typeof x.groupRef==='string').slice(0,12).map(x=>({scope:f().db.collection(deps.COL.state).doc('adDesignSaved').collection('scopes').doc(x.id),groupRef:x.groupRef}))];return [...new Map(targets.map(t=>[t.scope.id,t])).values()];}
  async function savedDesignTargets(w){return (await galleryTargets(w)).map(t=>({ref:t.scope.collection('designs'),groupRef:t.groupRef}));}
  async function generatedTargets(w){return (await galleryTargets(w)).map(t=>({ref:t.scope.collection('images'),groupRef:t.groupRef}));}
  async function linkPublishedWorkspaceGallery({workspaceId,campaignId,groups}){const w=await read(workspaceId),source=savedDesignScope(w);for(const group of groups||[]){const scope=savedDesignScope({context:{campaignId},settings:{productId:w.settings.productId,groupRef:group.ref}});if(scope.id===source.id)continue;const row=await scope.get(),meta=row.exists?row.data():{},sourceScopes=[...new Map([...(meta.sourceScopes||[]),{id:source.id,groupRef:w.settings.groupRef}].map(x=>[x.id,x])).values()];await scope.set({...meta,sourceScopes,productId:w.settings.productId,groupRef:group.ref,campaignId});}return {ok:true};}
  async function archiveGenerated(w,image){if(image.originGroupRef&&image.originGroupRef!==w.settings.groupRef)return;const target=savedDesignScope(w).collection('images').doc(image.id);await f().db.runTransaction(async tx=>{const old=await tx.get(target);if(!old.exists)tx.set(target,clean({...image,ownerProductId:w.settings.productId,workspaceId:w.workspaceId}));else if(image.deletedAt&&!old.data().deletedAt)tx.set(target,{...old.data(),deletedAt:image.deletedAt});});}
  async function generatedGallery(w){const images=[];for(const target of await generatedTargets(w)){const result=await target.ref.get();for(const row of result.docs){const image=row.data();if(image.kind==='generated'&&productKey(image.ownerProductId)===productKey(w.settings.productId))images.push({...image,originGroupRef:target.groupRef,groupRef:w.settings.groupRef,url:await deps.signAsset(image.asset)});}}const unique=new Map();for(const image of images){const prior=unique.get(image.id);if(!prior||image.deletedAt||!prior.deletedAt)unique.set(image.id,image);}return [...unique.values()];}
  async function deleteGeneratedImage(input={}){
    const w=await read(input.workspaceId);editorScope(w,input);if(!token(input.id))throw new Error('Choose an AI image.');if(w.job&&(active(w.job)||w.job.inFlight||['queued','running'].includes(w.job.phase)))throw new Error('Wait for the current design to finish.');
    const targets=await generatedTargets(w);let found;
    for(const target of targets){const r=await target.ref.doc(input.id).get();if(r.exists){found={ref:target.ref.doc(input.id),image:r.data()};break;}}
    if(!found){const r=await refFor(input.workspaceId).collection('imageLibrary').doc(input.id).get();const image=r.exists&&r.data();if(image&&image.kind==='generated'&&image.groupRef===input.groupRef&&(productKey(image.ownerProductId)===productKey(input.productId)||(image.productIds||[]).some(id=>productKey(id)===productKey(input.productId)))){await archiveGenerated(w,image);found={ref:targets[0].ref.doc(input.id),image:{...image,ownerProductId:input.productId}};}}
    if(!found||found.image.kind!=='generated'||productKey(found.image.ownerProductId)!==productKey(input.productId))throw new Error('This AI image belongs to another product or group.');
    await f().db.runTransaction(async tx=>{const latest=await tx.get(refFor(input.workspaceId)),record=await tx.get(found.ref);editorScope(latest.data(),input);if(!record.exists)throw new Error('The image is unavailable.');const deleted={...record.data(),deletedAt:record.data().deletedAt||Date.now()};tx.set(found.ref,deleted);tx.set(refFor(input.workspaceId).collection('imageLibrary').doc(input.id),{...deleted,groupRef:input.groupRef});});
    // Existing placements, paid generation receipts and editable source copies retain their pixels.
    return {ok:true,id:input.id,deleted:true};
  }
  function appearanceSummary(document){const layers=[];function visit(items,level=0){for(const o of items||[]){if(layers.length>=100)return;const value={};for(const k of ['type','name','text','fontFamily','fontSize','fontWeight','fill','stroke','strokeWidth','left','top','width','height','scaleX','scaleY','angle','opacity','rx','ry','cropX','cropY'])if(typeof o[k]==='string'||typeof o[k]==='number')value[k]=o[k];layers.push({...value,level});visit(o.objects,level+1);}}visit(document&&document.objects);return {background:typeof document?.background==='string'?document.background:null,layers};}
  async function savedDesignRecord(w,id){for(const target of await savedDesignTargets(w)){const row=await target.ref.doc(id).get();if(row.exists)return {design:row.data(),ref:target.ref.doc(id),groupRef:target.groupRef};}return null;}
  // Called internally only after Google's complete publication receipt establishes
  // the real group IDs. The same saved design can follow its product to a new group;
  // deleting that design removes it from every linked view, without duplicating bytes.
  async function linkPublishedDesignScopes({campaignId,groups,sourceGroupRef}){
    for(const g of groups||[]){const w={context:{campaignId},settings:{productId:g.productId,groupRef:g.ref}},ref=savedDesignScope(w),row=await ref.get(),old=row.exists&&row.data().sourceGroups||[],sourceGroups=[...new Set([...old,sourceGroupRef,g.temporaryRef].filter(Boolean))];if(JSON.stringify(old)!==JSON.stringify(sourceGroups))await ref.set({sourceGroups,productId:g.productId,groupRef:g.ref,campaignId});}
    return {ok:true};
  }
  async function savedDesignGallery(w,before){
    const targets=await savedDesignTargets(w),results=await Promise.all(targets.map(t=>t.ref.select('id','name','productId','groupRef','artboard','device','asset','thumbnail','createdAt','deletedAt','destination','appearance').get()));
    const ordered=[...new Map(results.flatMap(rows=>rows.docs.map(d=>d.data())).filter(d=>!d.deletedAt&&(!before||d.createdAt<before)).map(d=>[d.id,d])).values()].sort((a,b)=>b.createdAt-a.createdAt||a.id.localeCompare(b.id)),page=ordered.slice(0,60);
    for(const item of page)if(!item.appearance){const found=await savedDesignRecord(w,item.id);if(found?.design.document){item.appearance=appearanceSummary(found.design.document);await found.ref.update({appearance:item.appearance});}}
    return {savedDesigns:await Promise.all(page.map(async d=>({...d,url:await deps.signAsset(d.asset),thumbnailUrl:await deps.signAsset(d.thumbnail||d.asset)}))),savedDesignCursor:ordered.length>60?page[page.length-1].createdAt:null,imageAccess:deps.imageAccessStatus?deps.imageAccessStatus():null};
  }
  async function editorSavedDesigns(input={}){const w=await read(input.workspaceId);editorScope(w,input);return {ok:true,...await savedDesignGallery(w,input.before)};}
  async function editorOpenSavedDesign(input={}){
    const w=await read(input.workspaceId);editorScope(w,input);if(!token(input.id))throw new Error('Choose a saved design.');
    const found=await savedDesignRecord(w,input.id),design=found&&found.design;if(!design||design.deletedAt)throw new Error('This saved design was deleted or is no longer available.');
    const sources=[];for(const id of design.sourceIds||[]){const source=await refFor(design.workspaceId).collection('editorSources').doc(id).get();if(!source.exists)throw new Error('A source photo for this design is unavailable.');const p=source.data();
      if(productKey(p.productId)!==productKey(input.productId)||p.groupRef!==found.groupRef)throw new Error('A saved photo belongs to another product or ad.');
      if(design.workspaceId!==input.workspaceId||p.groupRef!==input.groupRef)await refFor(input.workspaceId).collection('editorSources').doc(id).set({...p,productId:input.productId,groupRef:input.groupRef});sources.push({...p,groupRef:input.groupRef,url:await deps.signAsset(p.asset,{required:true})});
    }
    return {ok:true,design,sources};
  }
  async function editorDeleteSavedDesign(input={}){
    const w=await read(input.workspaceId);editorScope(w,input);if(!token(input.id))throw new Error('Choose a saved design.');const found=await savedDesignRecord(w,input.id);if(!found)throw new Error('This saved design is unavailable.');const target=found.ref;let design;
    await f().db.runTransaction(async tx=>{const latest=await tx.get(refFor(input.workspaceId)),row=await tx.get(target);editorScope(latest.data(),input);if(!row.exists)throw new Error('This saved design is unavailable.');design=row.data();tx.set(target,{...design,deletedAt:design.deletedAt||Date.now()});});
    // Sources can be shared by other editable designs, so only this copy's
    // uniquely named finished raster and thumbnail are removed.
    if(deps.deleteSavedDesignAsset)await Promise.all([design.asset,design.thumbnail].filter(Boolean).map(a=>deps.deleteSavedDesignAsset(a,input.id)));
    await target.set({id:design.id,name:design.name,createdAt:design.createdAt,deletedAt:Date.now()});return {ok:true,id:input.id};
  }
  function editorKey({productId,groupRef,device,artboard}){
    if(!['shared','desktop','mobile'].includes(device))throw new Error('Choose shared, desktop or mobile artwork.');
    if(!artboard||!token(artboard.key)||!Number.isInteger(artboard.width)||!Number.isInteger(artboard.height)||Math.min(artboard.width,artboard.height)<32||Math.max(artboard.width,artboard.height)>4096||artboard.width*artboard.height>16777216)throw new Error('Use an artboard between 32 and 4096 pixels per side.');
    return 'design_'+sha([productId,groupRef,device,artboard.key,artboard.width,artboard.height]).slice(0,32);
  }
  function editorDocument(value){
    if(!value||!Array.isArray(value.objects)||Buffer.byteLength(JSON.stringify(value))>650000)throw new Error('This design is too large to save. Use up to 120 layers with concise text.');
    const types=new Set(['Rect','Circle','Triangle','Line','Textbox','IText','Text','Image','Group','rect','circle','triangle','line','textbox','i-text','text','image','group','layoutManager','linear','radial','Brightness','Contrast','Saturation','Blur','Grayscale','Sepia','Invert','HueRotation','Gamma','Vibrance','Noise','Pixelate','Resize','Composed']);
    let count=0;const sourceIds=new Set();
    const walk=(v,depth=0)=>{if(depth>18)throw new Error('The design has too many nested groups.');if(Array.isArray(v))return v.map(x=>walk(x,depth+1));if(v&&typeof v==='object'){
      if(v.type&&!types.has(v.type))throw new Error('This design contains an unsupported layer or effect.');
      if(['Image','image'].includes(v.type)){if(!token(v.sourceKey))throw new Error('Each image needs its saved original source.');sourceIds.add(v.sourceKey);}
      if(v.type&&types.has(v.type)&&!['linear','radial'].includes(v.type)&&++count>240)throw new Error('Use up to 120 layers and their effects.');
      return Object.fromEntries(Object.entries(v).filter(([k])=>!['src','crossOrigin','__proto__','constructor','prototype','clipPath','backgroundImage','overlayImage'].includes(k)).map(([k,x])=>[k,walk(x,depth+1)]));
    }if(typeof v==='number'&&(!Number.isFinite(v)||Math.abs(v)>1000000))throw new Error('A layer contains an invalid size or position.');if(typeof v==='string'&&v.length>18000)throw new Error('A text layer is too long.');return v;};
    const document=walk(value);if(value.objects.length>120)throw new Error('Use up to 120 layers.');return {document,sourceIds:[...sourceIds]};
  }
  async function editorState(input={}){
    const {workspaceId,productId,groupRef,device='shared',artboard}=input,ref=refFor(workspaceId),w=await read(workspaceId);editorScope(w,input);
    const rows=await ref.collection('editorDesigns').get(),designs=rows.docs.map(d=>d.data()).filter(d=>d.productId===productId&&d.groupRef===groupRef),id=artboard?editorKey({...input,device}):null;
    if(!designs.some(d=>d.id===id))for(const legacyId of w.context.legacyEditorWorkspaceIds||[]){
      const legacy=await refFor(legacyId).collection('editorDesigns').get();
      for(const row of legacy.docs.map(d=>d.data()).filter(d=>productKey(d.productId)===productKey(productId)&&d.groupRef===groupRef))if(!designs.some(d=>d.id===row.id))designs.push({...row,revision:0,inheritedFrom:row.id,legacyWorkspaceId:legacyId});
      if(designs.some(d=>d.id===id))break;
    }
    const own=designs.find(d=>d.id===id),shared=artboard&&device!=='shared'?designs.find(d=>d.id===editorKey({...input,device:'shared'})):null,design=own||shared||null;
    const sources=[];if(design)for(const key of design.sourceIds||[]){const p=await (design.legacyWorkspaceId?refFor(design.legacyWorkspaceId):ref).collection('editorSources').doc(key).get();if(!p.exists)throw new Error('A saved design image is unavailable. Your layers are retained.');const photo=p.data();if(productKey(photo.productId)!==productKey(productId)||photo.groupRef!==groupRef)throw new Error('The saved source belongs to another product or group.');if(design.legacyWorkspaceId)await ref.collection('editorSources').doc(key).set({...photo,productId});sources.push({...photo,url:await deps.signAsset(photo.asset)});}
    const exportRows=await ref.collection('editorExports').get(),exports=[];
    for(const row of exportRows.docs.map(d=>d.data()).filter(e=>!e.saveDesignId&&e.designId===(design&&design.id||id)).sort((a,b)=>b.createdAt-a.createdAt).slice(0,12))exports.push({...row,url:await deps.signAsset(row.asset),width:row.asset.width,height:row.asset.height});
    return {ok:true,...await savedDesignGallery(w),design:design?{...design,...(!own?{id,revision:0,inheritedFrom:design.id,device}:{})}:null,sources,exports,designs:designs.map(({id,name,device,artboard,revision,updatedAt})=>({id,name,device,artboard,revision,updatedAt}))};
  }
  async function editorSave(input={}){
    const {workspaceId,productId,groupRef,device,artboard,expectedRevision=0}=input,ref=refFor(workspaceId),w=await read(workspaceId);editorScope(w,input);
    const id=editorKey(input),{document,sourceIds}=editorDocument(input.document),target=ref.collection('editorDesigns').doc(id);
    for(const key of sourceIds){const row=await ref.collection('editorSources').doc(key).get();if(!row.exists||row.data().groupRef!==groupRef||row.data().productId!==productId)throw new Error('An image belongs to another product’s design. Choose it again from this workspace.');}
    const data={id,productId,groupRef,device,artboard:{key:artboard.key,width:artboard.width,height:artboard.height},name:String(input.name||'Ad artwork').slice(0,120),document,sourceIds,revision:Number(expectedRevision)+1,updatedAt:Date.now()};
    await f().db.runTransaction(async tx=>{const current=await tx.get(target),latest=await tx.get(ref);editorScope(latest.data(),input);if(Number(current.exists?current.data().revision:0)!==Number(expectedRevision))throw new Error('This artwork was saved in another window. Your local edits are retained; reopen the saved version before replacing it.');
      if(current.exists)tx.set(target.collection('versions').doc(String(current.data().revision)),current.data());tx.set(target,clean(data));});
    return {ok:true,id,revision:data.revision,updatedAt:data.updatedAt};
  }
  // AI edits have their own immutable input and durable receipts. They never
  // replace saved artwork, change workspace messaging, or publish to Google.
  const editorAIRef=(workspaceId,id)=>{if(!/^eai_[a-f0-9]{40}$/.test(String(id||'')))throw new Error('Invalid AI design request.');return refFor(workspaceId).collection('editorAIJobs').doc(id);};
  async function editorAIStatus(input={}){
    const w=await read(input.workspaceId);editorScope(w,input);let target;
    if(input.jobId)target=editorAIRef(input.workspaceId,input.jobId);
    else{const key=editorKey(input),rows=await refFor(input.workspaceId).collection('editorAIJobs').where('designKey','==',key).get(),latest=rows.docs.map(d=>({ref:d.ref,...d.data()})).sort((a,b)=>b.createdAt-a.createdAt)[0];if(!latest)return {ok:true,jobId:null,phase:'idle',result:null};target=latest.ref;}
    const row=await target.get();if(!row.exists)throw new Error('The saved AI request was not found.');const job=row.data();editorScope(w,job.scope);
    if(job.resetAt||job.phase==='dismissed')return {ok:true,jobId:null,phase:'idle',reset:true,result:null};
    if(input.artboard&&job.designKey!==editorKey(input))throw new Error('This AI result belongs to another artboard.');
    const receipt=await target.collection('data').doc('response').get(),stale=['queued','running'].includes(job.phase)&&job.leaseUntil<Date.now()&&Date.now()-job.updatedAt>30000;
    const phase=stale?'needs_attention':job.phase,unknown=!!job.inFlight&&!receipt.exists;
    const request=input.includeOriginal?await target.collection('data').doc('request').get():null;
    const result=job.phase==='ready'?await target.collection('data').doc('result').get():null;
    return {ok:true,workspaceId:input.workspaceId,jobId:job.id,requestId:job.requestId,inputHash:job.inputHash,scope:job.scope,phase,
      progress:stale?{pct:job.progress.pct,label:unknown?'Provider completion is uncertain. The request will not be charged again.':'Saved work is available to resume.'}:job.progress,
      error:job.error||null,canRetry:phase==='needs_attention'&&!unknown,hasSavedResponse:receipt.exists,needsNewRequestApproval:false,
      usage:job.usage?[job.usage]:[],cost:{estimatedUsd:job.usage?.estimatedUsd??(job.inFlight?job.reservedUsd:0),costEstimated:job.usage?.costEstimated!==false,reservedUsd:job.reservedUsd||0},
      result:result?.exists?result.data():null,...(request?.exists?{mode:request.data().mode,selectedLayerId:request.data().selectedLayerId,originalDocument:request.data().document,sources:await Promise.all(request.data().sources.map(async source=>({...source,url:await deps.signAsset(source.asset)})))}:{}),imageAccess:deps.imageAccessStatus?deps.imageAccessStatus():null};
  }
  async function editorAIResume(input={}){
    const w=await read(input.workspaceId);editorScope(w,input);const target=editorAIRef(input.workspaceId,input.jobId);let queued=false;
    await f().db.runTransaction(async tx=>{const row=await tx.get(target),receipt=await tx.get(target.collection('data').doc('response'));if(!row.exists)throw new Error('The AI request was not found.');const job=row.data();editorScope(w,job.scope);
      if(job.resetAt||job.phase==='dismissed')throw new Error('This failed AI job was reset. Start a new design when ready.');
      if(job.phase==='ready'||job.phase==='running'&&job.leaseUntil>Date.now())return;
      if(job.inFlight&&!receipt.exists)throw new Error('The provider may have completed this paid request. Automatic replacement is blocked to prevent another charge. Its original request ID is retained.');
      queued=true;tx.update(target,{phase:'queued',owner:null,leaseUntil:0,error:null,updatedAt:Date.now(),progress:{pct:job.progress.pct,label:receipt.exists?'Reopening the already paid design response':'Resuming saved product research'}});
    });return {ok:true,workspaceId:input.workspaceId,jobId:input.jobId,queued};
  }
  async function editorAIStart(input={}){
    const {workspaceId,productId,groupRef,device,artboard}=input,ref=refFor(workspaceId),w=await read(workspaceId);editorScope(w,input);const designKey=editorKey(input);
    if(!deps.env.OPENAI_API_KEY)throw new Error('OpenAI access is not configured for the AI designer.');
    if(!token(input.requestId)||!['design','text'].includes(input.mode))throw new Error('Choose a valid AI design request.');
    const {document,sourceIds}=editorDocument(input.document),objects=document.objects;
    if(!objects.length||!sourceIds.length)throw new Error('Add a product photo to the artboard before asking AI to design it.');
    if(objects.some(o=>!token(o.id))||new Set(objects.map(o=>o.id)).size!==objects.length)throw new Error('Every artboard layer needs a unique editor ID. Reopen this design.');
    const selected=objects.find(o=>o.id===input.selectedLayerId),locked=o=>!!o.locked||(o.objects||[]).some(locked);
    if(input.mode==='text'&&(!selected||locked(selected)||!('text'in selected||selected.editorRole==='button')))throw new Error('Select an unlocked text or button layer beside Typography.');
    if(sourceIds.length>16)throw new Error('This AI design can visually inspect up to 16 original photo sources in one request.');
    if(typeof input.screenshotDataUrl!=='string'||!/^data:image\/(?:jpeg|png);base64,[A-Za-z0-9+/=]+$/.test(input.screenshotDataUrl)||input.screenshotDataUrl.length>4000000)throw new Error('A current JPEG or PNG artboard preview is required.');
    const sources=[];for(const id of sourceIds){const row=await ref.collection('editorSources').doc(id).get();if(!row.exists||row.data().groupRef!==groupRef||String(row.data().productId)!==String(productId))throw new Error('An original photo is outside this product and ad group.');sources.push(row.data());}
    const request={productId:String(productId),groupRef,device,artboard:{key:artboard.key,width:artboard.width,height:artboard.height},document,mode:input.mode,selectedLayerId:String(input.selectedLayerId||''),instruction:String(input.instruction||'').slice(0,2400)},inputHash=sha(request),id='eai_'+sha([designKey,input.requestId]).slice(0,40),target=editorAIRef(workspaceId,id),prior=await target.get();
    if(prior.exists){if(prior.data().inputHash!==inputHash)throw new Error('This AI request ID belongs to a different canvas. Resume its saved result or start a new request.');return editorAIResume({...input,jobId:id});}
    const image=Buffer.from(input.screenshotDataUrl.split(',')[1],'base64'),sharp=require('sharp'),meta=await sharp(image,{limitInputPixels:17000000}).metadata();
    if(!['jpeg','png'].includes(meta.format)||Number(meta.pages||1)>1||Math.abs(meta.width/meta.height-artboard.width/artboard.height)>.02)throw new Error('The preview does not match this artboard’s aspect ratio.');
    const preview=await sharp(image).resize({width:1280,height:1280,fit:'inside',withoutEnlargement:true}).flatten({background:'#ffffff'}).jpeg({quality:88}).toBuffer();
    const asset=await deps.saveAsset(workspaceId,preview,id+'_snapshot',{mimeType:'image/jpeg',kind:'AI editor context'}),scope={productId:String(productId),groupRef,device,artboard:request.artboard};let cached=false;
    await f().db.runTransaction(async tx=>{const latest=await tx.get(ref),exists=await tx.get(target);editorScope(latest.data(),input);if(latest.data().sourceSetId!==w.sourceSetId)throw new Error('The product sources changed. Reopen the editor.');if(exists.exists){if(exists.data().inputHash!==inputHash)throw new Error('AI request ID conflict.');cached=true;return;}
      const pointer=latest.data().editorAI;if(pointer?.id){const running=await tx.get(editorAIRef(workspaceId,pointer.id));if(running.exists){const j=running.data();if(j.phase!=='ready'&&(j.phase==='queued'||j.phase==='running'&&j.leaseUntil>Date.now()||j.inFlight))throw new Error('An earlier AI design request is unfinished. Reopen or resume its saved result before starting another paid request.');}}
      const now=Date.now();tx.set(target,{id,requestId:input.requestId,inputHash,designKey,scope,sourceSetId:w.sourceSetId,sourceVersion:w.sourceVersion||null,snapshotHash:w.snapshotHash||null,phase:'queued',owner:null,leaseUntil:0,createdAt:now,updatedAt:now,progress:{pct:4,label:'Current artwork and exact product context saved'},inFlight:null,reservedUsd:0});
      tx.set(target.collection('data').doc('request'),clean({...request,previewAsset:asset,sources}));tx.update(ref,{editorAI:{id,designKey,at:now}});
    });return {ok:true,workspaceId,jobId:id,queued:!cached,inputHash};
  }
  async function editorAIRun({workspaceId,jobId}={}){
    const ref=refFor(workspaceId),target=editorAIRef(workspaceId,jobId),owner=crypto.randomUUID();let job,request,w;
    await f().db.runTransaction(async tx=>{const row=await tx.get(target),workspace=await tx.get(ref),input=await tx.get(target.collection('data').doc('request'));if(!row.exists||!input.exists||!workspace.exists)throw new Error('AI design context is unavailable.');job=row.data();w=workspace.data();request=input.data();editorScope(w,job.scope);if(job.resetAt||job.phase==='dismissed'||job.phase==='ready'||job.phase==='running'&&job.leaseUntil>Date.now())return;job={...job,phase:'running',owner,leaseUntil:Date.now()+8*60000,updatedAt:Date.now()};tx.update(target,job);});
    if(job.owner!==owner)return {ok:true,cached:true};
    const save=async patch=>{Object.assign(job,patch,{updatedAt:Date.now()});await f().db.runTransaction(async tx=>{const row=await tx.get(target);if(row.data()?.owner!==owner)throw new Error('AI design worker ownership changed.');tx.update(target,clean(job));});};
    const saveData=async(key,data)=>f().db.runTransaction(async tx=>{const row=await tx.get(target);if(row.data()?.owner!==owner)throw new Error('AI design worker ownership changed before saving its output.');tx.set(target.collection('data').doc(key),clean(data));});
    const verify=async()=>{const current=await read(workspaceId);editorScope(current,job.scope);if(current.sourceSetId!==job.sourceSetId||(current.snapshotHash||null)!==job.snapshotHash||(current.sourceVersion||null)!==job.sourceVersion)throw new Error('Product or campaign evidence changed. The saved AI result cannot replace this draft.');};
    try{
      await verify();let responseRow=await target.collection('data').doc('response').get(),evidenceRow=await target.collection('data').doc('evidence').get(),evidence=evidenceRow.exists?evidenceRow.data():null;
      if(!responseRow.exists){
        if(job.inFlight)throw new Error('This paid request has no confirmed receipt. No replacement request was sent.');
        const products=await productsFor(ref,w),product=products.find(p=>String(p.id)===request.productId),group=(w.context.groups||[]).find(g=>g.ref===request.groupRef);if(!product||!group)throw new Error('The exact listing or ad group is unavailable.');
        await save({progress:{pct:12,label:'Researching this listing, keywords, brand and group outcomes'}});
        if(!evidence||Date.now()-evidence.researchCompletedAt>600000){
          const primaryImages=product.images||[];if(!primaryImages.length)throw new Error('The product listing has no verified source photograph.');
          evidence=await deps.research.collect({campaignId:w.context.campaignId||null,sourceVersion:w.sourceVersion,snapshot:w.sourceSnapshot,range:w.context.range,group,selectedProducts:[product],settings:{...w.settings,productId:product.id,sourceImageId:primaryImages.some(p=>p.id===w.settings.sourceImageId)?w.settings.sourceImageId:primaryImages[0].id},deadlineMs:90000});
          await saveData('evidence',evidence);
        }
        await save({progress:{pct:36,label:'Preparing the current canvas and its unchanged original photographs'}});
        const originals=[];for(const source of request.sources){const bytes=await deps.loadAsset(source.asset),normalized=await require('sharp')(bytes,{limitInputPixels:40000000}).resize({width:1280,height:1280,fit:'inside',withoutEnlargement:true}).flatten({background:'#ffffff'}).jpeg({quality:86}).toBuffer();originals.push({...source,dataUrl:'data:image/jpeg;base64,'+normalized.toString('base64')});}
        const screenshot=await deps.loadAsset(request.previewAsset),prepared=require('./googleAdsAdDesignResearch').buildEditorRequest({evidence,request,screenshotDataUrl:'data:image/jpeg;base64,'+screenshot.toString('base64'),sources:originals});
        const quote=await deps.reserveCost({key:'copy',workspace:w,job:{inputCoverage:{preparedReferenceCount:originals.length+1}}}),textBytes=prepared.input.reduce((n,row)=>n+Buffer.byteLength(typeof row.content==='string'?row.content:row.content.filter(c=>c.type==='input_text').map(c=>c.text).join('\n')),0);
        // UTF-8 bytes bound text-token count conservatively; use the adapter's
        // higher context rates, the actual output limit and a vision allowance.
        const boundedReserve=Math.ceil((textBytes*20/1000000+prepared.max_output_tokens*75/1000000+(originals.length+1)*.12)*100)/100,reserve=Math.max(Number(typeof quote==='object'?quote.reservedUsd:quote),boundedReserve),ctrl=await deps.control(),allowance=Math.max(1,Math.min(30,Number(ctrl.creativeBudgetUsd)||8));
        if(!Number.isFinite(reserve)||reserve<=0||reserve>allowance)throw new Error('The configured creative allowance cannot cover this bounded AI design request.');
        await verify();if(deps.verifyContext)await deps.verifyContext(w);
        const requestId=crypto.randomUUID();await save({reservedUsd:reserve,inFlight:{requestId,at:Date.now()},progress:{pct:52,label:'Astra is inspecting the ad and designing tailored copy, typography and layout'}});
        const response=await deps.responses(prepared,requestId);
        // Write the provider receipt before parsing or validation. A reload or
        // interrupted worker can use this response without a second API charge.
        await saveData('response',{response,requestId,receivedAt:Date.now()});responseRow=await target.collection('data').doc('response').get();
      }
      if(!evidence)throw new Error('The saved response is missing its original evidence.');
      const response=responseRow.data().response,actual=Number(response.estimatedUsd),usage={requestId:responseRow.data().requestId,providerModel:response.model||'gpt-6-astra',usage:response.usage||{},estimatedUsd:response.costEstimated===false&&Number.isFinite(actual)&&actual>=0?actual:job.reservedUsd,costEstimated:response.costEstimated!==false,at:Date.now()};
      await save({usage,inFlight:null,progress:{pct:86,label:'Checking product claims, editable layers, photo integrity and placement'}});
      const research=require('./googleAdsAdDesignResearch'),output=research.parseResponse(response),result=research.applyEditorPlan({output,request,evidence,sources:request.sources});editorDocument(result.document);await verify();
      await saveData('result',result);await save({phase:'ready',leaseUntil:0,completedAt:Date.now(),error:null,progress:{pct:100,label:'Tailored design is ready to apply to this artboard'}});
      return {ok:true,workspaceId,jobId};
    }catch(error){
      if(error.notDispatched||error.definiteResponse){job.inFlight=null;if(!job.usage)job.usage={estimatedUsd:0,costEstimated:!!error.definiteResponse,usage:{},at:Date.now()};}
      await save({phase:'needs_attention',leaseUntil:0,error:String(error.message||error).slice(0,700),progress:{pct:job.progress.pct,label:job.inFlight?'Paid request needs reconciliation; it will not run again automatically':'Saved design needs attention; completed work is retained'}});return {ok:false,workspaceId,jobId,error:job.error};
    }
  }
  async function editorExport(input={}){
    const {workspaceId,format='png',dataBase64,revision,displayOptimized=false,saveDesignId=null}=input,ref=refFor(workspaceId),w=await read(workspaceId);editorScope(w,input);
    const id=editorKey(input),saved=await ref.collection('editorDesigns').doc(id).get();if(!saved.exists||saved.data().revision!==revision)throw new Error('Save the latest editable artwork before exporting.');
    if(saveDesignId&&!/^saved_[a-f0-9]{32}$/.test(saveDesignId))throw new Error('Invalid saved design copy.');
    if(saveDesignId&&(format!=='png'||displayOptimized))throw new Error('Saved designs retain a lossless PNG master.');
    if(saveDesignId){const prior=await savedDesignsRef(w).doc(saveDesignId).get();if(prior.exists&&prior.data().deletedAt)throw new Error('This design copy was deleted. Save a new copy.');}
    if(!['png','jpeg'].includes(format))throw new Error('Export a PNG or JPEG.');
    const chunkBytes=1536*1024,maxBytes=80*1024*1024,uploads=ref.collection('editorExportUploads');let bytes,uploadRef,upload;
    const digest=b=>crypto.createHash('sha256').update(b).digest('hex');
    const decode=(value,limit)=>{if(typeof value!=='string'||!value.length||value.length>Math.ceil(limit/3)*4||value.length%4||!/^[A-Za-z0-9+/]*={0,2}$/.test(value))throw new Error('The artwork upload is incomplete or invalid. Retry the export.');const b=Buffer.from(value,'base64');if(b.length>limit||b.toString('base64')!==value)throw new Error('Invalid artwork upload size.');return b;};
    const clearParts=async target=>{const parts=await target.collection('parts').get();if(deps.deleteAsset)await Promise.all(parts.docs.map(p=>deps.deleteAsset(p.data().asset)));await f().db.runTransaction(async tx=>{for(const p of parts.docs)tx.delete(target.collection('parts').doc(p.id));});};
    if(input.phase==='start'){
      if(!Number.isInteger(input.bytes)||input.bytes<1||input.bytes>maxBytes||!/^[a-f0-9]{64}$/.test(input.sha256||''))throw new Error('Use a PNG or JPEG no larger than 80 MiB.');
      // A retry of the same saved raster resumes its immutable, hashed parts.
      const uploadId='export_'+sha([id,revision,format,!!displayOptimized,input.bytes,input.sha256,saveDesignId]).slice(0,40),target=uploads.doc(uploadId),previous=await target.get();
      if(previous.exists&&previous.data().expiresAt>Date.now()){
        if(previous.data().exportId){const done=await ref.collection('editorExports').doc(previous.data().exportId).get();if(done.exists)return {ok:true,...done.data(),url:await deps.signAsset(done.data().asset)};}
        const received=await target.collection('parts').get();return {ok:true,uploadId,chunkBytes,received:received.docs.map(d=>d.data().index)};
      }
      if(previous.exists)await clearParts(target);
      const old=await uploads.get();for(const row of old.docs.filter(d=>d.data().expiresAt<Date.now()&&!d.data().cleanedAt).slice(0,8)){try{await clearParts(uploads.doc(row.id));await uploads.doc(row.id).set({cleanedAt:Date.now()},{merge:true});}catch(_){/* Retry cleanup on the next export. */}}
      await target.set({id:uploadId,designId:id,revision,format,displayOptimized:!!displayOptimized,saveDesignId,bytes:input.bytes,sha256:input.sha256,chunkBytes,parts:Math.ceil(input.bytes/chunkBytes),expiresAt:Date.now()+24*60*60*1000});
      return {ok:true,uploadId,chunkBytes};
    }
    if(input.phase){
      if(!['chunk','complete'].includes(input.phase)||!/^export_[a-f0-9]{40}$/.test(input.uploadId||''))throw new Error('Invalid artwork upload session.');
      uploadRef=uploads.doc(input.uploadId);const row=await uploadRef.get();upload=row.exists&&row.data();
      if(!upload||upload.designId!==id||upload.revision!==revision||(upload.saveDesignId||null)!==saveDesignId||upload.format!==format||upload.displayOptimized!==!!displayOptimized||upload.expiresAt<Date.now())throw new Error('This artwork upload expired or belongs to a different design. Export again.');
      if(upload.exportId){const complete=await ref.collection('editorExports').doc(upload.exportId).get();if(complete.exists){const record=complete.data();return {ok:true,...record,url:await deps.signAsset(record.asset)};}}
      if(input.phase==='chunk'){
        if(!Number.isInteger(input.index)||input.index<0||input.index>=upload.parts)throw new Error('Invalid artwork part number.');
        const part=decode(dataBase64,chunkBytes),expected=Math.min(chunkBytes,upload.bytes-input.index*chunkBytes);if(part.length!==expected)throw new Error('The artwork part has an incorrect size. Retry this export.');
        const target=uploadRef.collection('parts').doc(String(input.index)),hash=digest(part),previous=await target.get();
        if(previous.exists&&previous.data().sha256===hash)return {ok:true,index:input.index,bytes:part.length};
        if(previous.exists&&!upload.cleanedAt)throw new Error('The artwork changed during upload. Export the saved design again.');
        const asset=await deps.saveAsset(workspaceId,part,'editor_part_'+input.uploadId+'_'+input.index,{mimeType:'application/octet-stream',kind:'temporary artwork upload'});
        await target.set({index:input.index,sha256:hash,asset});return {ok:true,index:input.index,bytes:part.length};
      }
      const rows=await uploadRef.collection('parts').get(),parts=rows.docs.map(d=>d.data()).sort((a,b)=>a.index-b.index);
      if(parts.length!==upload.parts||parts.some((p,i)=>p.index!==i))throw new Error('Some artwork parts have not arrived. Retry the export to resume the upload.');
      bytes=Buffer.concat(await Promise.all(parts.map(async p=>{const b=await deps.loadAsset(p.asset);if(digest(b)!==p.sha256)throw new Error('An artwork part failed verification. Export again.');return b;})));
      if(bytes.length!==upload.bytes||digest(bytes)!==upload.sha256)throw new Error('The artwork failed file verification. Your editable layers are saved. Export again.');
    }else bytes=decode(dataBase64,4*1024*1024);
    const sharp=require('sharp'),meta=await sharp(bytes,{limitInputPixels:16777216}).metadata(),board=saved.data().artboard;
    if(meta.width!==board.width||meta.height!==board.height||meta.format!==format||Number(meta.pages||1)>1)throw new Error('The export must match the saved artboard dimensions and file format exactly.');
    const maxDisplayBytes=150000;let output=bytes,quality=100;
    if(displayOptimized&&output.length>maxDisplayBytes){if(format!=='jpeg')throw new Error('Choose JPEG for a smaller Google Display file.');for(const q of [95,90,85,80,75,70,65,60]){output=await sharp(bytes).flatten({background:'#ffffff'}).jpeg({quality:q,chromaSubsampling:'4:4:4'}).toBuffer();quality=q;if(output.length<=maxDisplayBytes)break;}if(output.length>maxDisplayBytes)throw new Error('This design exceeds 150 KB at this size. Simplify the artwork or choose a smaller display size. Editable layers are saved. Uncheck optimization to export the full-quality master.');}
    const exportId=saveDesignId||'artwork_'+sha([id,revision,sha(output.toString('base64'))]).slice(0,32),asset=await deps.saveAsset(workspaceId,output,saveDesignId?'saved_design_'+saveDesignId:exportId,{width:meta.width,height:meta.height,mimeType:'image/'+format,kind:'finished display artwork'}),record={id:exportId,designId:id,revision,productId:input.productId,groupRef:input.groupRef,asset,format,bytes:output.length,quality,displayOptimized,saveDesignId,createdAt:Date.now()};
    let copyRecord=null;if(saveDesignId){const data=saved.data(),thumb=await sharp(bytes).resize({width:360,height:360,fit:"inside",withoutEnlargement:true}).png().toBuffer(),thumbnail=await deps.saveAsset(workspaceId,thumb,"saved_design_"+saveDesignId+"_thumb",{mimeType:"image/png",kind:"saved design thumbnail"});copyRecord={...data,id:saveDesignId,workspaceId,asset,thumbnail,appearance:appearanceSummary(data.document),createdAt:Date.now(),destination:(w.context.groups||[]).find(g=>g.ref===input.groupRef)?.url||""};}
    // Recheck the scope/revision after rendering or assembling the upload.
    await f().db.runTransaction(async tx=>{const latest=await tx.get(ref),design=await tx.get(ref.collection('editorDesigns').doc(id));editorScope(latest.data(),input);if(!design.exists||design.data().revision!==revision)throw new Error('The design changed during export. Export the latest saved artwork.');tx.set(ref.collection('editorExports').doc(exportId),record);if(copyRecord)tx.set(savedDesignsRef(w).doc(saveDesignId),clean(copyRecord));if(uploadRef)tx.set(uploadRef,{exportId},{merge:true});});
    if(uploadRef)try{await clearParts(uploadRef);await uploadRef.set({cleanedAt:Date.now()},{merge:true});}catch(_){/* Saved artwork is intact; expired-part cleanup will retry. */}
    return {ok:true,...record,url:await deps.signAsset(asset),message:'Exact artwork saved. This is a finished display creative; responsive Ads and Merchant photos keep their separate approval workflow.'};
  }
  async function crop({workspaceId,source,device,format,groupRef,rect=null,expectedImageId=null,remove=false}={}) {
    if(!['desktop','mobile'].includes(device)||!FORMATS.some(f=>f.key===format))throw new Error('Choose a desktop or mobile image format.');
    const ref=refFor(workspaceId),workspace=await read(workspaceId),group=(workspace.context.groups||[]).find(g=>g.ref===groupRef);
    if(!group)throw new Error('Choose an ad group in this workspace.');
    if(workspace.job&&(active(workspace.job)||workspace.job.inFlight||['queued','running'].includes(workspace.job.phase)))throw new Error('Wait for the current design or resolve its pending request before editing images.');
    let record=null,sourceDesignRef=null;
    if(!remove){
      if(!source||!['product','upload','current','library','savedDesign'].includes(source.kind))throw new Error('Choose a saved photo or design from this ad workspace.');
      const products=await productsFor(ref,workspace);let bytes,productIds=[],title='Ad image',originalAsset=null,rootSource=source,artwork=false;
      if(source.kind==='product'){
        const p=products.find(p=>String(p.id)===String(source.productId)),photo=p&&(p.images||[]).find(i=>i.id===source.imageId);
        if(!photo||(Array.isArray(p.creativeGroupRefs)?!p.creativeGroupRefs.includes(groupRef)&&!(p.eligibleGroupRefs||[]).includes(groupRef):Array.isArray(p.eligibleGroupRefs)&&!p.eligibleGroupRefs.includes(groupRef)))throw new Error('This photo is outside this ad’s product library.');
        bytes=await deps.fullSourceBytes(photo.url);productIds=[String(p.id)];title=p.title;
      }else if(source.kind==='upload'){
        const photo=(workspace.references||[]).find(i=>i.id===source.imageId);if(!photo)throw new Error('Uploaded photo was not found.');
        originalAsset=photo.originalAsset||photo.asset;bytes=await deps.loadAsset(originalAsset);productIds=photo.role==='product'?[String(photo.productId)]:[];title=photo.fileName;
      }else if(source.kind==='current'){
        const photo=(creativeFor(workspace).images||[]).find(i=>i.id===source.imageId&&i.groupRef===groupRef);if(!photo||/LOGO/.test(photo.fieldType||''))throw new Error('Choose a product image from this ad group.');
        bytes=await deps.fullSourceBytes(photo.url);title='Current Google ad';
      }else if(source.kind==='savedDesign'){
        editorScope(workspace,{productId:workspace.settings.productId,groupRef});if(!token(source.imageId))throw new Error('Choose a saved design.');
        const found=await savedDesignRecord(workspace,source.imageId),photo=found&&found.design;
        if(!photo||photo.deletedAt||photo.groupRef!==found.groupRef||productKey(photo.productId)!==productKey(workspace.settings.productId))throw new Error('This saved design is unavailable for the selected product and group.');
        sourceDesignRef=found.ref;bytes=await deps.loadAsset(photo.asset);title=photo.name;artwork=true;
        for(const id of photo.sourceIds||[]){const doc=await refFor(photo.workspaceId).collection('editorSources').doc(id).get();if(!doc.exists)throw new Error('A saved design source is unavailable.');productIds.push(...(doc.data().productIds||[]));}
        productIds=[...new Set(productIds)];
      }else{
        if(!token(source.imageId))throw new Error('Invalid saved image.');
        const saved=await ref.collection('imageLibrary').doc(source.imageId).get();if(!saved.exists||saved.data().groupRef!==groupRef)throw new Error('This saved image belongs to another ad group.');
        const photo=saved.data();originalAsset=photo.originalAsset||photo.asset;bytes=await deps.loadAsset(originalAsset);productIds=photo.productIds||[];title=photo.title;rootSource=photo.rootSource||source;artwork=photo.artwork===true;
      }
      const out=await deps.cropImage(bytes,format,rect),id='crop_'+sha([groupRef,workspace.settings.productId,format,sha(bytes.toString('base64')),out.crop,artwork]).slice(0,32);
      if(!originalAsset)originalAsset=await deps.saveAsset(workspaceId,bytes,'original_'+sha(bytes.toString('base64')).slice(0,24),{kind:'crop original',mimeType:out.mimeType});
      const asset=await deps.saveAsset(workspaceId,out.bytes,id,{width:out.width,height:out.height,kind:format});
      record={id,kind:'crop',groupRef,format,title:title||'Ad image',productIds,rootSource,artwork,asset,originalAsset,crop:out.crop,sourceWidth:out.sourceWidth,sourceHeight:out.sourceHeight,cropWidth:out.cropWidth,cropHeight:out.cropHeight,upscaled:out.upscaled,createdAt:Date.now()};
    }
    await f().db.runTransaction(async tx=>{
      const snapshot=await tx.get(ref);if(!snapshot.exists)throw new Error('Workspace was not found.');const latest=snapshot.data(),slot=chosenPlacements(latest).find(p=>p.groupRef===groupRef&&p.device===device&&p.format===format);
      if(sourceDesignRef){const sourceDesign=await tx.get(sourceDesignRef);if(!sourceDesign.exists||sourceDesign.data().deletedAt)throw new Error('This saved design was deleted while placing it.');}
      if((slot&&slot.imageId||null)!==expectedImageId||latest.sourceSetId!==workspace.sourceSetId||latest.job&&(active(latest.job)||latest.job.inFlight||['queued','running'].includes(latest.job.phase)))throw new Error('This image choice changed while saving. Reload the workspace and try again.');
      if(productKey(latest.settings.productId)!==productKey(workspace.settings.productId)||latest.settings.deviceLinked!==workspace.settings.deviceLinked)throw new Error('The selected product or device sharing changed while saving. Please retry.');
      const devices=latest.settings.deviceLinked===false?[device]:['desktop','mobile'];
      if(devices.length===2&&sha((latest.placements||[]).filter(p=>placementMatches(p,latest.settings)&&p.format===format))!==sha((workspace.placements||[]).filter(p=>placementMatches(p,workspace.settings)&&p.format===format)))throw new Error('The shared image changed while saving. Please retry.');
      const placements=(latest.placements||[]).filter(p=>!(placementMatches(p,latest.settings)&&devices.includes(p.device)&&p.format===format));
      if(record){tx.set(ref.collection('imageLibrary').doc(record.id),record);for(const target of devices)placements.push({groupRef,productId:latest.settings.productId,device:target,format,imageId:record.id,asset:record.asset,productIds:record.productIds,rootSource:record.rootSource,artwork:record.artwork});}
      if(latest.job)tx.set(ref.collection('history').doc(latest.job.id),latest.job);
      tx.update(ref,{placements,job:null,revision:Number(latest.revision||0)+1,updatedAt:Date.now()});
    });
    return status({workspaceId});
  }
  async function resetFailures({workspaceId,all=false,cursor,before}={}){
    const now=Date.now(),cutoff=before==null?now:Number(before);
    if(!Number.isFinite(cutoff)||cutoff<=0||cutoff>now+1000)throw new Error('Invalid reset time.');
    if(!workspaceId&&!all)throw new Error('Choose a workspace or all failed AI jobs.');
    let refs,nextCursor=null;
    if(workspaceId)refs=[refFor(workspaceId)];
    else{let query=f().db.collection(deps.COL.state).doc('adDesign').collection('workspaces').orderBy('__name__').limit(12);if(cursor){if(!token(cursor))throw new Error('Invalid reset cursor.');query=query.startAfter(cursor);}const page=await query.get();refs=page.docs.map(d=>d.ref);if(page.docs.length===12)nextCursor=page.docs[page.docs.length-1].id;}
    let resetJobs=0,resetEditorJobs=0,unconfirmed=0;const resetWorkspaceIds=[],resetEditorJobIds=[];
    for(const ref of refs){
      const outcome=await f().db.runTransaction(async tx=>{
        const row=await tx.get(ref);if(!row.exists||row.data().archivedAt)return 0;const w=row.data(),job=w.job;
        const stale=job&&['paused','queued','running'].includes(job.phase)&&!active(job)&&now-Number(job.updatedAt||job.createdAt||0)>120000;
        if(!job||job.phase!=='needs_attention'&&!stale||Number(job.updatedAt||job.createdAt||0)>cutoff||active(job)){tx.update(ref,{lastFailureResetAt:cutoff});return 0;}
        if(job.inFlight){const receipt=await tx.get(ref.collection('outputs').doc(job.id+'_'+job.inFlight.key)),r=receipt.exists&&receipt.data();if(!r||!r.rawResponse&&!r.stageResult&&!r.asset)return -1;}
        // Keep legacy paid images visible even if their job predates the gallery.
        const images=[];for(const [format,asset]of Object.entries(job.assets||{})){if((job.placements||[]).some(p=>p.asset.hash===asset.hash))continue;const id='generated_'+sha(asset.path).slice(0,32),target=ref.collection('imageLibrary').doc(id),existing=await tx.get(target);if(!existing.exists)images.push({target,data:{id,kind:'generated',groupRef:w.settings.groupRef,format,title:'Saved AI image',productIds:job.result&&job.result.productIds||[String(w.settings.productId)],asset,createdAt:job.completedAt||job.createdAt||now,jobId:job.id}});}
        tx.set(ref.collection('history').doc(job.id),clean({...job,resetAt:now,resetReason:'Operator reset failed AI state'}));for(const image of images)tx.set(image.target,clean(image.data));
        tx.update(ref,{job:null,revision:Number(w.revision||0)+1,updatedAt:now,lastFailureResetAt:now});return 1;
      });
      if(outcome===1){resetJobs++;resetWorkspaceIds.push(ref.id);}else if(outcome===-1)unconfirmed++;
      const failed=await ref.collection('editorAIJobs').where('phase','in',['needs_attention','queued','running']).get();
      for(const candidate of failed.docs){const result=await f().db.runTransaction(async tx=>{
        const row=await tx.get(candidate.ref),workspace=await tx.get(ref);if(!row.exists||!workspace.exists||workspace.data().archivedAt)return 0;const job=row.data();
        const stale=['queued','running'].includes(job.phase)&&Number(job.leaseUntil||0)<now&&now-Number(job.updatedAt||job.createdAt||0)>120000;
        if(job.phase!=='needs_attention'&&!stale||job.resetAt||Number(job.updatedAt||job.createdAt||0)>cutoff||active(job))return 0;
        if(job.inFlight){const receipt=await tx.get(candidate.ref.collection('data').doc('response'));if(!receipt.exists)return -1;}
        tx.set(candidate.ref.collection('data').doc('reset'),clean({job,resetAt:now,reason:'Operator reset failed AI state'}));
        tx.update(candidate.ref,{phase:'dismissed',resetAt:now,error:null,owner:null,leaseUntil:0,inFlight:null,updatedAt:now});
        if(workspace.data().editorAI?.id===job.id)tx.update(ref,{editorAI:null});return 1;
      });if(result===1){resetEditorJobs++;resetEditorJobIds.push(candidate.id);}else if(result===-1)unconfirmed++;}
    }
    return {ok:true,resetJobs,resetEditorJobs,unconfirmed,resetWorkspaceIds:[...new Set(resetWorkspaceIds)],resetEditorJobIds,before:cutoff,nextCursor};
  }
  async function start({ workspaceId, expectedVersion, snapshotHash, retry = false, mode='design', newRequest=false } = {}) {
    if(!['copy','design','image'].includes(mode))throw new Error('Choose image generation or messaging research.');
    const ref = refFor(workspaceId), value = await read(workspaceId), capability = provider();
    if(mode==='copy'&&value.job&&!['ready','draft'].includes(value.job.phase)&&value.job.mode!=='copy')throw new Error('Resume the saved image request before starting separate messaging research. Its paid work is preserved.');
    if (value.job && (value.job.phase === "ready"&&!newRequest || active(value.job))) return { ok: true, workspaceId, jobId: value.job.id, cached: true, queued: false };
    if (!capability.available) throw new Error(capability.reason || "Image generation is not configured.");
    if (value.context.generationAllowed === false) throw new Error("This source context is not ready for generation. Choose an eligible product and current ad or draft.");
    if (deps.verifyContext) await deps.verifyContext(value);
    if (value.context.campaignId) await deps.verifyBasis({ campaignId: value.context.campaignId, expectedVersion: expectedVersion == null ? value.sourceVersion : Number(expectedVersion), snapshotHash: snapshotHash || value.snapshotHash });
    if (!value.context.campaignId && !value.context.approvalId && !(value.context.itemIds || []).length) throw new Error("Choose a current product opportunity with exact eligible Merchant offers before generating a new campaign draft.");
    const checked = settingsFor(value.settings, await productsFor(ref, value), value.context.groups || [], value.references || [], formats(), creativeFor(value));
    const selectedPlacements=chosenPlacements(value),placementCount=new Set(selectedPlacements.map(p=>p.imageId)).size;
    if (!checked.selectedImages.length && !checked.referenceIds.length && !checked.currentAssetIds.length && !placementCount) throw new Error("Select at least one product photo or uploaded reference before generating.");
    if (!checked.selectedImages.length && !selectedPlacements.some(p=>(p.productIds||[]).length) && !checked.referenceIds.some(id=>(value.references||[]).some(r=>r.id===id&&r.role==="product"))) throw new Error("Select a listing photo or product-detail upload to preserve the jewelry's identity. Current ad images and style references guide the composition.");
    if (checked.selectedImages.length + checked.referenceIds.length + checked.currentAssetIds.length + placementCount > 144) throw new Error("Up to 144 photos can fit legibly in one composition. Reduce this selection or split it into designs; no generation was charged.");
    let jobId, cached = false;
    await f().db.runTransaction(async tx => { const s = await tx.get(ref); if (!s.exists) throw new Error("Workspace was not found."); const current = s.data();let previous = current.job;
      if (current.revision !== value.revision || sha(current.settings) !== sha(value.settings)) throw new Error("The selected product or design direction changed before generation began. Review it and start again.");
      if (active(previous)) { jobId = previous.id; cached = true; return; }
      if(previous&&previous.phase==='ready'&&newRequest){tx.set(ref.collection('history').doc(previous.id),previous);previous=null;}
      if (previous && previous.phase === "ready") { jobId = previous.id; cached = true; return; }
      let recovered = null, recordedOutput = false;
      if (previous && previous.inFlight) { const receipt = await tx.get(ref.collection("outputs").doc(previous.id + "_" + previous.inFlight.key)); if (receipt.exists) { recovered = receipt.data().stageResult || null; recordedOutput = !!(recovered || receipt.data().rawResponse || receipt.data().asset); } }
      if (previous && previous.inFlight && !recordedOutput && !retry) throw new Error("A previous paid request has an unknown result. Saved outputs are retained; explicitly allow a new request only if you want to retry that unfinished step.");
      const job = previous ? clean(previous) : { id: crypto.randomUUID(),mode,copyOverride:mode!=='copy'&&current.messaging&&current.messaging.edited?clean(current.messaging.copy):null, sourceVersion: current.sourceVersion, snapshotHash: current.snapshotHash, settingsHash: sha(current.settings), placements:clean(chosenPlacements(current)), phase: "queued", stages: {}, assets: {}, usage: [], requests: 0, createdAt: Date.now() };
      if (job.settingsHash !== sha(current.settings)) throw new Error("The direction changed. Save a new design revision first.");
      if (job.inFlight) { if (recovered) { job.stages[job.inFlight.key] = recovered; settle(job, job.inFlight.key, recovered); } else if (!recordedOutput) job.unknownRequests = (job.unknownRequests || []).concat({ ...job.inFlight, retriedAt: Date.now(), explicitRetry: true }); job.inFlight = null; }
      job.phase = "queued"; job.owner = null; job.error = null; job.leaseUntil = 0; job.progress = { pct: Number(job.progress && job.progress.pct) || 0, label: "Design queued; saved work will be reused" }; jobId = job.id;
      tx.update(ref, { job, updatedAt: Date.now() });
    }); return { ok: true, workspaceId, jobId, queued: !cached, cached };
  }
  async function run({ workspaceId, jobId } = {}) {
    const ref = refFor(workspaceId), owner = crypto.randomUUID(), started = Date.now(); let value, job;
    await f().db.runTransaction(async tx => { const s = await tx.get(ref); if (!s.exists) throw new Error("Workspace was not found."); value = s.data(); job = value.job;
      if (!job || job.id !== jobId) throw new Error("This generation job is no longer current.");
      if (job.phase === "ready") return; if (active(job)) throw new Error("This design is already running."); if (job.inFlight) throw new Error("The last paid request requires explicit resolution before another request.");
      job = { ...job, phase: "running", owner, leaseUntil: Date.now() + 850000 }; tx.update(ref, { job });
    });
    if (job.phase === "ready") return { ok: true, cached: true, workspaceId };
    const saveJob = async patch => { Object.assign(job, patch, { updatedAt: Date.now() }); await f().db.runTransaction(async tx => { const s = await tx.get(ref); if (!s.exists || !s.data().job || s.data().job.id !== jobId || s.data().job.owner !== owner) throw new Error("The design job lost its active lease."); tx.update(ref, { job: clean(job), updatedAt: Date.now() }); }); };
    const writeReceipt = async (key, output) => { await f().db.runTransaction(async tx => { const s = await tx.get(ref); if (!s.exists || !s.data().job || s.data().job.id !== jobId || s.data().job.owner !== owner) throw new Error("The design job lost its lease before its output could be attached."); tx.set(ref.collection("outputs").doc(jobId + "_" + key), clean({ ...output, requestId: output.requestId || job.inFlight && job.inFlight.requestId || null }), { merge: true }); }); };
    const progress = (pct, label) => saveJob({ progress: { pct: Math.max(Number(job.progress && job.progress.pct) || 0, pct), label } });
    const paid = async (key, operation) => {
      if (job.stages[key]) return job.stages[key];
      const stored = await ref.collection("outputs").doc(jobId + "_" + key).get();
      if (stored.exists && stored.data().stageResult) { job.stages[key] = stored.data().stageResult; settle(job, key, job.stages[key]); await saveJob({ inFlight: null }); return job.stages[key]; }
      if (stored.exists && (stored.data().rawResponse || stored.data().asset)) {
        const output = await operation(stored.data().requestId); output.requestId = stored.data().requestId;
        await writeReceipt(key, { stageResult: output, receivedAt: Date.now() }); job.stages[key] = clean(output); settle(job, key, output); await saveJob({ inFlight: null }); return output;
      }
      if (deps.verifyContext) await deps.verifyContext(value);
      if (typeof deps.reserveCost !== "function") throw new Error("The provider's request cost allowance is unavailable; no generation request was sent.");
      const quote = await deps.reserveCost({ key, provider: provider(), workspace: value, job }), reserve = Number(quote && typeof quote === "object" ? quote.reservedUsd : quote);
      if (!Number.isFinite(reserve) || reserve <= 0) throw new Error("The provider's request cost allowance could not be verified.");
      const ctrl = await deps.control(), allowance = Math.max(1, Math.min(30, Number(ctrl.creativeBudgetUsd) || 8)), spent = (job.reservations || []).reduce((sum, row) => sum + Number(row.actualUsd == null ? row.reservedUsd : row.actualUsd), 0);
      if (job.requests >= 24 || spent + reserve > allowance) throw new Error("The remaining creative allowance cannot cover this request. Saved outputs and uncertain-request reservations are retained; adjust the allowance to finish.");
      const requestId = crypto.randomUUID(); job.reservations = (job.reservations || []).concat({ key, requestId, reservedUsd: reserve, rationale: String(quote && quote.rationale || "Planning reservation; actual provider token usage is recorded separately.").slice(0, 400), at: Date.now(), settled: false });
      await saveJob({ requests: Number(job.requests || 0) + 1, inFlight: { key, at: Date.now(), requestId } });
      let output;
      try { output = await operation(job.inFlight.requestId); }
      catch (error) { if(error.notDispatched){const reservation=job.reservations.find(r=>r.requestId===requestId);reservation.settled=true;reservation.actualUsd=0;reservation.notDispatched=true;}if (error.definiteResponse||error.notDispatched) await saveJob({ inFlight: null }); throw error; }
      // Callers save raw images or Responses results before returning. A receipt
      // survives a crash between provider completion and the workspace update.
      output.requestId = requestId; await writeReceipt(key, { stageResult: output, receivedAt: Date.now() });
      job.stages[key] = clean(output); settle(job, key, output);
      await saveJob({ inFlight: null }); return output;
    };
    try {
      if (value.context.generationAllowed === false) throw new Error("This source context is not eligible for generation.");
      if (deps.verifyContext) await deps.verifyContext(value);
      if (value.context.campaignId) await deps.verifyBasis({ campaignId: value.context.campaignId, expectedVersion: job.sourceVersion, snapshotHash: job.snapshotHash });
      const products = await productsFor(ref), product = products.find(row => String(row.id) === value.settings.productId), group = (value.context.groups || []).find(row => row.ref === value.settings.groupRef);
      if (!product || !group) throw new Error("The selected product or ad group is no longer available.");
      settingsFor(value.settings, products, value.context.groups || [], value.references || [], formats(), creativeFor(value));
      const selected = settingsFor(value.settings, products, value.context.groups || [], value.references || [], formats(), creativeFor(value));
      let inputs = selected.selectedImages.map((choice,index) => {
        const p = products.find(p => String(p.id) === choice.productId), photo = p.images.find(i => i.id === choice.imageId);
        return { id: choice.productId + ":" + choice.imageId, label: "P" + (index + 1), productId: choice.productId, imageId: choice.imageId, role: "product", origin: (p.eligibleGroupRefs || []).includes(group.ref) ? "catalog" : "related", title: p.title, url: photo.url };
      }).concat(selected.referenceIds.map((id,index) => { const r = value.references.find(r => r.id === id); return { id: r.id, label: "U" + (index + 1), productId: r.role === "product" ? r.productId : null, role: r.role, origin: "upload", title: r.fileName, asset: r.asset }; }));
      inputs = inputs.concat(selected.currentAssetIds.map((id,index)=>{const asset=(creativeFor(value).images||[]).find(a=>a.id===id);return {id:asset.id,label:"A"+(index+1),productId:null,role:"inspiration",origin:"current",title:"Current Google "+asset.fieldType,url:asset.url};}));
      for(const [i,placement] of (job.placements||[]).entries()){
        if(placement.groupRef!==group.ref)throw new Error('A saved image choice belongs to another ad group.');
        if(!inputs.some(input=>input.id===placement.imageId))inputs.push({id:placement.imageId,label:'C'+(i+1),productId:(placement.productIds||[])[0]||null,productIds:placement.productIds||[],role:(placement.productIds||[]).length?'product':'inspiration',origin:'chosen crop',title:placement.device+' '+placement.format+' chosen image',asset:placement.asset});
      }
      const legacyPinned = !Array.isArray(value.settings.selectedImages) && job.inputCoverage && job.inputAssets && job.inputAssets.length;
      if (legacyPinned) {
        const ordered = [...(job.inputCoverage.sourceImageIds || []), ...(job.inputCoverage.referenceIds || [])];
        if (!ordered.length || ordered.some(id=>!job.inputAssets.some(a=>a.id===id))) throw new Error("The saved legacy source set is incomplete. Its paid outputs were preserved; save a new design revision to continue.");
        inputs = ordered.map((id,index)=>{const a=job.inputAssets.find(a=>a.id===id);return {id,label:"P"+(index+1),productId:a.role==="product"?String(product.id):null,role:a.role,origin:"saved",title:product.title,asset:a.asset};});
      }
      if (!inputs.length) throw new Error("Select at least one product photo or uploaded reference before generating.");
      if (!legacyPinned && inputs.length > 144) throw new Error("Up to 144 photos can fit legibly in one composition. Split this selection; no generation was charged.");
      const selectedProductIds = [...new Set(inputs.filter(i => i.role === "product").flatMap(i => i.productIds||i.productId&&[String(i.productId)]||[]))];
      const selectedProducts = selectedProductIds.map(id => products.find(p => String(p.id) === id));
      if (selectedProducts.some(p => !p)) throw new Error("A selected product is no longer in this workspace. Reload its listing before generating.");
      const researchProducts = [product, ...selectedProducts.filter(p => String(p.id) !== String(product.id))];
      // A single-product design may use genuine matching pieces, but unrelated
      // advertised listings must get their own design and destination.
      const unrelated=selectedProducts.filter(p=>productKey(p.id)!==productKey(product.id)&&!(p.relatedTo||[]).some(id=>productKey(id)===productKey(product.id))&&!(product.relatedTo||[]).some(id=>productKey(id)===productKey(p.id)));
      if(unrelated.length)throw new Error('These selected products need separate ads and destinations: '+unrelated.map(p=>p.title).join(', ')+'. Choose photos for '+product.title+' or its matching set.');
      const researchGroup=!value.context.campaignId?{...group,url:product.url}:group;
      await progress(4, "Saving every selected product photo and inspiration image");
      job.inputAssets = job.inputAssets || [];
      for (const [index, input] of inputs.entries()) {
        if (job.inputAssets.some(saved => saved.id === input.id)) continue;
        let asset = input.asset;
        if (!asset) { const original = await deps.sourceBytes(input.url), normalized = await deps.normalizeUpload(original); asset = await deps.saveAsset(workspaceId, normalized.bytes, jobId.replace(/-/g, "") + "_source_" + index, { width: normalized.width, height: normalized.height, kind: "source" }); }
        job.inputAssets.push({ id: input.id, label: input.label, productId: input.productId, role: input.role, asset }); await saveJob({});
      }
      const selectedSources = inputs.map(({asset,...input}) => input);
      let sourceFiles;
      if (legacyPinned) {
        sourceFiles = await Promise.all(inputs.map(input=>deps.loadAsset(input.asset)));
      } else if (job.preparedAssets && job.inputCoverage && sha(job.inputCoverage.selectedSourceIds) === sha(inputs.map(i=>i.id))) {
        sourceFiles = await Promise.all(job.preparedAssets.map(a=>deps.loadAsset(a)));
      } else {
        const rawFiles = await Promise.all(inputs.map(input => deps.loadAsset(job.inputAssets.find(saved => saved.id === input.id).asset)));
        const prepared = deps.prepareReferences ? await deps.prepareReferences({sources:inputs.map((input,index)=>({...input,bytes:rawFiles[index]}))}) : {
          references:rawFiles, referenceManifest:inputs.map((input,index)=>({index:index+1,kind:"single",cells:[{label:input.label,sourceId:input.id,productId:input.productId,role:input.role,title:input.title}]})), coverage:{complete:true,selectedSourceIds:inputs.map(i=>i.id),selectedSourceCount:inputs.length,preparedReferenceCount:inputs.length}
        };
        sourceFiles = prepared.references;
        if (sourceFiles.length > Number(provider().referenceLimit || 16) || !prepared.coverage.complete || sha(prepared.coverage.selectedSourceIds) !== sha(inputs.map(i=>i.id))) throw new Error("The selected photos could not all be prepared faithfully. Reduce this composition before generating; no AI request was sent.");
        job.preparedAssets = [];
        for (const [index, bytes] of sourceFiles.entries()) {
          const saved = await deps.saveAsset(workspaceId, bytes, jobId.replace(/-/g, "") + "_reference_" + index, {kind:"prepared reference"});
          job.preparedAssets.push(saved);
        }
        job.inputCoverage = {...prepared.coverage,referenceManifest:prepared.referenceManifest,selectedSources,sourceImageIds:selected.selectedImages.map(i=>i.imageId),referenceIds:selected.referenceIds,currentAssetIds:selected.currentAssetIds,
          usedProductImages:inputs.filter(i=>i.role==="product").length,usedInspirationImages:inputs.filter(i=>i.role==="inspiration").length,
          sourceAssets:inputs.map(input=>({id:input.id,hash:job.inputAssets.find(a=>a.id===input.id).asset.hash})),
          note:sourceFiles.length<inputs.length?"Every selection is included in labeled reference sheets; no photos were dropped.":"Every selected photo is included. No unselected photos were added."};
        await saveJob({});
      }
      await progress(6, "Researching this product, buyer intent and ad history");
      const savedCopy = await ref.collection("outputs").doc(jobId + "_copy").get();
      if (!job.evidence || !job.stages.copy && !savedCopy.exists && Date.now() - Number(job.evidence.researchCompletedAt || 0) > 600000) { job.evidence = await deps.research.collect({ campaignId: value.context.campaignId || null, sourceVersion: value.sourceVersion, snapshot: value.sourceSnapshot, range: value.context.range, group:researchGroup, selectedProducts: researchProducts, ...(legacyPinned?{}:{selectedSources}), settings: value.settings, deadlineMs: 90000 }); await saveJob({}); }
      // Construct and validate locally before reserving a paid request.
      const buildCopyRequest=recovering=>deps.research.buildRequest({evidence:job.evidence,mode:job.mode,recovering,feedback:value.settings.direction,currentCreative:group.original||{},style:value.settings.style,sourceImageDataUrl:'data:image/jpeg;base64,'+sourceFiles[0].toString('base64'),sourceReferences:sourceFiles.map((bytes,index)=>({dataUrl:'data:image/jpeg;base64,'+bytes.toString('base64'),manifest:job.inputCoverage.referenceManifest&&job.inputCoverage.referenceManifest[index]}))});
      const readCopy=async(key,recovering)=>{
        const saved=await ref.collection("outputs").doc(jobId+"_"+key).get();
        const request=job.stages[key]||saved.exists&&(saved.data().rawResponse||saved.data().stageResult)?null:buildCopyRequest(recovering);
        return paid(key,async requestId=>{
          const stored=await ref.collection("outputs").doc(jobId+"_"+key).get(),prior=stored.exists&&stored.data().rawResponse;
          const result=prior||await deps.responses(request,requestId);
          if(!prior)await writeReceipt(key,{rawResponse:result,receivedAt:Date.now()});
          let output;try{output=deps.research.validateResult({output:require('./googleAdsAdDesignResearch').parseResponse(result),evidence:job.evidence,channel:group.channel,group,mode:job.mode});}
          catch(error){settle(job,key,{requestId,usage:result.usage||{},providerModel:result.model||"gpt-6-astra",estimatedUsd:result.estimatedUsd,costEstimated:result.costEstimated!==false});await saveJob({inFlight:null});error.definiteResponse=true;throw error;}
          return {...output,usage:result.usage||{},responseId:result.id||null,providerModel:result.model||request&&request.model||"gpt-6-astra",estimatedUsd:result.estimatedUsd==null?1:result.estimatedUsd,costEstimated:result.costEstimated!==false};
        });
      };
      let copy;
      try{copy=await readCopy("copy",false);}
      catch(error){
        if(!['AI_OUTPUT_INCOMPLETE','AI_OUTPUT_INVALID'].includes(error.code))throw error;
        // Exactly one durable recovery stage, charged within the existing job
        // allowance. Resuming reuses its receipt; it never loops into new calls.
        await progress(12,"Completing an interrupted AI answer from the saved product research");
        copy=await readCopy("copy_repair",true);
      }
      if(job.copyOverride)copy={...copy,copy:job.copyOverride};
      if(job.mode==='copy'){
        job.result={copyOnly:true,copy:copy.copy,brief:copy.brief,evidence:job.evidence,keywords:group.keywords||[],sourceIds:copy.sourceIds,productIds:[String(product.id)]};
        await saveJob({phase:'ready',completedAt:Date.now(),leaseUntil:0,inFlight:null,error:null,progress:{pct:100,label:'Messaging researched and saved. Review it with each image, then approve the update here.'}});
        await ref.update({messaging:{copy:copy.copy,productId:value.settings.productId,groupRef:group.ref,researchedAt:Date.now(),evidenceHash:job.evidence.hash,edited:false}});
        return {ok:true,workspaceId,copyOnly:true};
      }
      await progress(25, "Copy and visual direction saved; composing each required format");
      const wanted = formats().filter(format => value.settings.formats.includes(format.key));
      job.placementAssets=job.placementAssets||{desktop:{},mobile:{}};
      for (let i = 0; i < wanted.length; i++) {
        const format = wanted[i];
        const desktop=job.mode==='image'?null:(job.placements||[]).find(p=>p.device==='desktop'&&p.format===format.key),mobile=job.mode==='image'?null:(job.placements||[]).find(p=>p.device==='mobile'&&p.format===format.key);
        if(desktop&&mobile){job.assets[format.key]=desktop.asset;job.placementAssets.desktop[format.key]=desktop.asset;job.placementAssets.mobile[format.key]=mobile.asset;await saveJob({});continue;}
        if (Date.now() - started > 540000) { await saveJob({ phase: "paused", progress: { pct: job.progress.pct, label: "Saved all completed work; continuing the remaining formats" }, leaseUntil: 0 }); return { ok: true, paused: true, dispatch: true, workspaceId, jobId }; }
        await progress(30 + Math.floor(i / wanted.length * 45), "Designing " + format.label.toLowerCase() + " — " + format.width + " × " + format.height);
        const output = await paid("image_" + format.key, async requestId => {
          const receipt = ref.collection("outputs").doc(jobId + "_image_" + format.key), stored = await receipt.get();
          if (stored.exists && stored.data().asset) return stored.data();
          const image = await deps.generateImage({ requestId, provider: provider(), format, references: sourceFiles, product, products: researchProducts, referenceManifest: job.inputCoverage.referenceManifest, brief: copy.brief, imageDirections: copy.imageDirections, settings: value.settings, inputCoverage: job.inputCoverage });
          const asset = await deps.saveAsset(workspaceId, image.bytes, jobId.replace(/-/g, "") + "_" + format.key, { width: format.width, height: format.height, kind: format.key, providerModel: provider().model });
          const saved = { asset, usage: image.usage || {}, estimatedUsd: image.estimatedUsd == null ? 1 : image.estimatedUsd, costEstimated: image.costEstimated !== false, providerModel: provider().model };
          await writeReceipt("image_" + format.key, saved); return saved;
        });
        const libraryId='generated_'+sha(output.asset.path).slice(0,32);
        const libraryImage={id:libraryId,kind:'generated',ownerProductId:value.settings.productId,workspaceId,groupRef:group.ref,format:format.key,title:product.title,productIds:selectedProductIds.length?selectedProductIds:[String(product.id)],asset:output.asset,createdAt:Date.now(),jobId};await ref.collection('imageLibrary').doc(libraryId).set(libraryImage);await archiveGenerated(value,libraryImage);
        job.assets[format.key] = desktop?desktop.asset:output.asset;
        job.placementAssets.desktop[format.key]=desktop?desktop.asset:output.asset;
        job.placementAssets.mobile[format.key]=mobile?mobile.asset:output.asset;
        await saveJob({});
      }
      if (Date.now() - started > 620000) { await saveJob({ phase: "paused", leaseUntil: 0, progress: { pct: job.progress.pct, label: "Every format is saved; continuing the quality review" } }); return { ok: true, paused: true, dispatch: true, workspaceId, jobId }; }
      await progress(82, "Checking product fidelity, framing and mobile readability");
      const readQuality = key => paid(key, async requestId => {
        const finalAssets=[...new Map(Object.values(job.placementAssets).flatMap(assets=>Object.values(assets)).map(a=>[a.hash,a])).values()];
        const stored=await ref.collection('outputs').doc(jobId+'_'+key).get();let response=stored.exists&&stored.data().rawResponse,output;
        const files = await Promise.all(finalAssets.map(asset => deps.loadAsset(asset)));
        try{output=await deps.reviewImages(sourceFiles[0], files, { ...copy.brief, copy: copy.copy, keywords: group.keywords || product.keywords || [], settings: value.settings, product: product.title, products: researchProducts, placementAssets:job.placementAssets, inputCoverage: job.inputCoverage }, sourceFiles, requestId,{rawResponse:response,onResponse:async raw=>{response=raw;await writeReceipt(key,{rawResponse:raw,receivedAt:Date.now()});}});}
        catch(error){if(response){settle(job,key,{requestId,usage:response.usage||{},providerModel:response.model||'gpt-6-astra',estimatedUsd:response.estimatedUsd,costEstimated:response.costEstimated!==false});await saveJob({inFlight:null});error.definiteResponse=true;}throw error;}
        await writeReceipt(key, { stageResult: output, receivedAt: Date.now() });
        return { ...output, estimatedUsd: output.estimatedUsd == null ? 1 : output.estimatedUsd, costEstimated: output.costEstimated !== false };
      });
      let quality;try{quality=await readQuality('quality');}catch(error){if(!['AI_OUTPUT_INCOMPLETE','AI_OUTPUT_INVALID'].includes(error.code))throw error;await progress(85,'Completing the saved artwork quality review');quality=await readQuality('quality_repair');}
      if (quality.pass !== true || quality.productFaithful !== true || quality.mobileReadable !== true || Number(quality.score) < 85) throw new Error("The generated design needs changes to product fidelity, framing or mobile clarity before it can enter Approvals.");
      const singleProductDestination=!value.context.campaignId&&!value.context.approvalId||/\/products\//.test(group.url||'');
      const outside = selectedProducts.filter(p => Array.isArray(p.eligibleGroupRefs) && !p.eligibleGroupRefs.includes(group.ref)||singleProductDestination&&String(p.id)!==String(product.id));
      group.requiresProductSplit=isSharedProductGroup(value,group);
      const publication = {ready:!outside.length&&!group.requiresProductSplit,productIds:selectedProductIds.length?selectedProductIds:[String(product.id)],reason:group.requiresProductSplit?"Design saved. Split the shared group into product groups before publishing this listing’s assets.":outside.length?"Design saved. These featured products are outside this ad's verified destination or product selection: "+outside.map(p=>p.title).join(", ")+". Open an ad that includes these products, or use photos from this ad's listings, before submitting it for publication.":null};
      job.result = { productIds: publication.productIds, publication, brief: copy.brief, copy: copy.copy, assets: job.assets, placementAssets:job.placementAssets, evidence: job.evidence, keywords: group.keywords || product.keywords || [], quality, learningApplications: copy.learningApplications || [], sourceIds: copy.sourceIds || [], inputCoverage: job.inputCoverage, designSettings: value.settings, previewOnlyFormats: group.channel === "search" ? ["portrait"] : [] }; await saveJob({});
      if (!publication.ready) {
        await saveJob({phase:"ready",completedAt:Date.now(),leaseUntil:0,inFlight:null,error:null,progress:{pct:100,label:"Design saved — review its product destination before publication"}});
        return {ok:true,workspaceId,approvalId:null,publication};
      }
      await progress(94, "Saving the exact copy and images for your approval");
      if (!job.approvalId) { const result = await deps.finish({ workspaceId, jobId, owner, workspace: value, group, product, selectedProducts: [product], result: job.result }); job.approvalId = result.approvalId; if (!job.approvalId) throw new Error("The finished design could not be added to Approvals."); await saveJob({}); }
      await ref.update({messaging:{copy:copy.copy,productId:value.settings.productId,groupRef:group.ref,researchedAt:Date.now(),evidenceHash:job.evidence.hash,edited:!!job.copyOverride}});
      await saveJob({ phase: "ready", completedAt: Date.now(), leaseUntil: 0, inFlight: null, error: null, progress: { pct: 100, label: "Ready in Approvals — review the exact copy and every image" } });
      return { ok: true, workspaceId, approvalId: job.approvalId };
    } catch (error) {
      await saveJob({ phase: "needs_attention", error: String(error.message || error).slice(0, 900), leaseUntil: 0, progress: { pct: Number(job.progress && job.progress.pct) || 0, label: "Saved work retained — review the unfinished step" } }); throw error;
    }
  }
  return { workspace, save, upload, crop, start, status, run, resetFailures, editorSource, editorState, editorSave, editorExport, editorSavedDesigns, editorOpenSavedDesign, editorDeleteSavedDesign, deleteGeneratedImage, linkPublishedDesignScopes, linkPublishedWorkspaceGallery, editorAIStart, editorAIStatus, editorAIResume, editorAIRun };
}
module.exports = { createAdDesignService, buildVersionDesignPayload, isSharedProductGroup, formatAssets, chosenPlacements, placementMatches, FORMATS, settingsFor, refreshedSettings, responseText, MAX_UPLOAD };
