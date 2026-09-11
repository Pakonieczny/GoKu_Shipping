// Ad Design workspaces: durable source selection, generation checkpoints and
// exact reviewed outputs. This module never publishes a Google Ads mutation.
const crypto = require("crypto");
const clean = v => JSON.parse(JSON.stringify(v));
const sha = v => crypto.createHash("sha256").update(typeof v === "string" ? v : JSON.stringify(v)).digest("hex");
const token = value => /^[a-zA-Z0-9_-]{1,100}$/.test(String(value || ""));
const active = job => job && Number(job.leaseUntil) > Date.now() && ["queued", "running"].includes(job.phase);
const MAX_UPLOAD = 4 * 1024 * 1024;
const productKey = id => String(id || '').split('/').pop();
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
    let hasReceipt = false;
    if (job.inFlight && !active(job)) { const receipt = await ref.collection("outputs").doc(job.id + "_" + job.inFlight.key).get(); hasReceipt = !!(receipt.exists && (receipt.data().stageResult || receipt.data().rawResponse || receipt.data().asset)); }
    const approvalId = job.result && job.result.publication && job.result.publication.ready === false ? null : job.approvalId || workspace.context.approvalId || null, review = approvalId && deps.reviewStatus ? await deps.reviewStatus(approvalId) : null;
    return { ok: true, workspaceId, revision:Number(workspace.revision||0), sourceVersion: workspace.sourceVersion || null, snapshotHash: workspace.snapshotHash || null,
      context: {...workspace.context,currentCreative:creativeFor(workspace)}, products, references, imageLibrary, placements:chosenPlacements(workspace), settings: workspace.settings, messaging, publication:workspace.publication||null, status: job.phase || "draft", phase: job.phase || "draft",
      progress: job.progress || { pct: 0, label: "Choose your product, reference images and direction" }, result,
      approvalId, review, error: job.error || null,
      canRetry: !active(job) && (!job.inFlight || hasReceipt) && ["paused", "needs_attention", "queued", "running"].includes(job.phase), needsNewRequestApproval: !!job.inFlight && !active(job) && !hasReceipt,
      provider: provider(), formats: formats(), usage: job.usage || [], generatedAt: job.completedAt || null,
      controlsMeaning: "Background and scale guide the photograph. Font and border preview the composition; Google chooses responsive typography and layout." };
  }
  async function workspace(input = {}) {
    if (input.workspaceId) return status({ workspaceId: input.workspaceId });
    const contextKey = { campaignId: String(input.campaignId || ""), approvalId: String(input.approvalId || ""), handle: String(input.handle || ""), groupRef: String(input.groupRef || ""),
      itemIds: input.campaignId || input.approvalId ? [] : [...new Set((input.itemIds || []).map(String))].sort(), feedLabel: input.campaignId || input.approvalId ? "" : String(input.feedLabel || "").toUpperCase() };
    if (!contextKey.campaignId && !contextKey.approvalId && !contextKey.handle) throw new Error("Open Ad Design from a campaign, draft or product opportunity.");
    let id = "design_" + sha(JSON.stringify(contextKey)).slice(0, 32), ref = refFor(id), saved = await ref.get();
    if (saved.exists && input.campaignId && !input.approvalId && !input.force) {
      const latest = await deps.verifyBasis({campaignId:String(input.campaignId)});
      if (latest && latest.snapshotHash && (Number(latest.version)!==Number(saved.data().sourceVersion)||latest.snapshotHash!==saved.data().snapshotHash)) {
        id = "design_" + sha({...contextKey,sourceVersion:latest.version,snapshotHash:latest.snapshotHash}).slice(0,32);ref=refFor(id);saved=await ref.get();
      }
    }
    if (saved.exists && !input.force) return status({ workspaceId: id });
    if (saved.exists && (active(saved.data().job) || saved.data().job && saved.data().job.inFlight)) throw new Error("Resolve the current or unconfirmed design request before refreshing its source context.");
    const loaded = await deps.loadContext(input), products = loaded.products || [], groups = loaded.context.groups || [];
    if (!products.length) throw new Error("No verified product photographs were found for this context.");
    const sourceSetId = crypto.randomUUID();
    await Promise.all(products.map((product, position) => ref.collection("sourceSets").doc(sourceSetId).collection("products").doc(sha(String(product.id)).slice(0, 32)).set(clean({ ...product, position }))));
    const prior = saved.exists ? saved.data() : null;
    const initialGroup = groups.find(group => group.ref === input.groupRef) || groups[0], initialProduct = products.find(product => (product.images || []).length && (!(initialGroup.productIds || []).length || initialGroup.productIds.map(String).includes(String(product.id).split("/").pop()))) || products.find(product => (product.images || []).length) || products[0];
    const references = prior && prior.references || [], settings = settingsFor({ productId: initialProduct.id, groupRef: input.groupRef, currentAssetIds:((loaded.context.currentCreative||{}).images||[]).filter(a=>a.groupRef===initialGroup.ref&&!/LOGO/.test(a.fieldType)).map(a=>a.id) }, products, groups, references, formats(),loaded.context.currentCreative);
    await f().db.runTransaction(async tx => { const latest = await tx.get(ref), current = latest.exists ? latest.data() : null;
      if (!!current !== !!prior || current && (Number(current.revision) !== Number(prior.revision) || (current.job && current.job.id || null) !== (prior.job && prior.job.id || null) || active(current.job) || current.job && current.job.inFlight)) throw new Error("The workspace changed while its sources were being refreshed. The current design was preserved.");
      if (current && current.job) tx.set(ref.collection("history").doc(current.job.id), current.job);
      tx.set(ref, clean({ schema: 1, workspaceId: id, context: loaded.context, sourceVersion: loaded.sourceVersion || null, snapshotHash: loaded.snapshotHash || null,
        sourceSnapshot: loaded.snapshot || null, sourceSetId, productsIds: products.map(row => row.id), settings, references, placements:prior&&prior.placements||[], job: null, revision: Number(prior && prior.revision || 0) + 1, createdAt: prior && prior.createdAt || Date.now(), updatedAt: Date.now() }));
    });
    return status({ workspaceId: id });
  }
  async function save(input = {}) {
    const ref = refFor(input.workspaceId), sourceWorkspace = await read(input.workspaceId), products = await productsFor(ref, sourceWorkspace);
    await f().db.runTransaction(async tx => { const s = await tx.get(ref); if (!s.exists) throw new Error("Workspace was not found."); const value = s.data();
      if (value.sourceSetId !== sourceWorkspace.sourceSetId) throw new Error("The gallery changed. Reload the workspace before saving these selections.");
      if (active(value.job)) throw new Error("Wait for the current design to finish before changing its direction.");
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
      const restored=priorJob&&priorJob.exists?priorJob.data():priorDesign&&priorDesign.job||null;
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
  async function crop({workspaceId,source,device,format,groupRef,rect=null,expectedImageId=null,remove=false}={}) {
    if(!['desktop','mobile'].includes(device)||!FORMATS.some(f=>f.key===format))throw new Error('Choose a desktop or mobile image format.');
    const ref=refFor(workspaceId),workspace=await read(workspaceId),group=(workspace.context.groups||[]).find(g=>g.ref===groupRef);
    if(!group)throw new Error('Choose an ad group in this workspace.');
    if(workspace.job&&(active(workspace.job)||workspace.job.inFlight||['queued','running'].includes(workspace.job.phase)))throw new Error('Wait for the current design or resolve its pending request before editing images.');
    let record=null;
    if(!remove){
      if(!source||!['product','upload','current','library'].includes(source.kind))throw new Error('Choose a saved photo from this ad workspace.');
      const products=await productsFor(ref,workspace);let bytes,productIds=[],title='Ad image',originalAsset=null,rootSource=source;
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
      }else{
        if(!token(source.imageId))throw new Error('Invalid saved image.');
        const saved=await ref.collection('imageLibrary').doc(source.imageId).get();if(!saved.exists||saved.data().groupRef!==groupRef)throw new Error('This saved image belongs to another ad group.');
        const photo=saved.data();originalAsset=photo.originalAsset||photo.asset;bytes=await deps.loadAsset(originalAsset);productIds=photo.productIds||[];title=photo.title;rootSource=photo.rootSource||source;
      }
      const out=await deps.cropImage(bytes,format,rect),id='crop_'+sha([groupRef,format,sha(bytes.toString('base64')),out.crop]).slice(0,32);
      if(!originalAsset)originalAsset=await deps.saveAsset(workspaceId,bytes,'original_'+sha(bytes.toString('base64')).slice(0,24),{kind:'crop original',mimeType:out.mimeType});
      const asset=await deps.saveAsset(workspaceId,out.bytes,id,{width:out.width,height:out.height,kind:format});
      record={id,kind:'crop',groupRef,format,title:title||'Ad image',productIds,rootSource,asset,originalAsset,crop:out.crop,sourceWidth:out.sourceWidth,sourceHeight:out.sourceHeight,cropWidth:out.cropWidth,cropHeight:out.cropHeight,upscaled:out.upscaled,createdAt:Date.now()};
    }
    await f().db.runTransaction(async tx=>{
      const snapshot=await tx.get(ref);if(!snapshot.exists)throw new Error('Workspace was not found.');const latest=snapshot.data(),slot=chosenPlacements(latest).find(p=>p.groupRef===groupRef&&p.device===device&&p.format===format);
      if((slot&&slot.imageId||null)!==expectedImageId||latest.sourceSetId!==workspace.sourceSetId||latest.job&&(active(latest.job)||latest.job.inFlight||['queued','running'].includes(latest.job.phase)))throw new Error('This image choice changed while saving. Reload the workspace and try again.');
      if(productKey(latest.settings.productId)!==productKey(workspace.settings.productId)||latest.settings.deviceLinked!==workspace.settings.deviceLinked)throw new Error('The selected product or device sharing changed while saving. Please retry.');
      const devices=latest.settings.deviceLinked===false?[device]:['desktop','mobile'];
      if(devices.length===2&&sha((latest.placements||[]).filter(p=>placementMatches(p,latest.settings)&&p.format===format))!==sha((workspace.placements||[]).filter(p=>placementMatches(p,workspace.settings)&&p.format===format)))throw new Error('The shared image changed while saving. Please retry.');
      const placements=(latest.placements||[]).filter(p=>!(placementMatches(p,latest.settings)&&devices.includes(p.device)&&p.format===format));
      if(record){tx.set(ref.collection('imageLibrary').doc(record.id),record);for(const target of devices)placements.push({groupRef,productId:latest.settings.productId,device:target,format,imageId:record.id,asset:record.asset,productIds:record.productIds,rootSource:record.rootSource});}
      if(latest.job)tx.set(ref.collection('history').doc(latest.job.id),latest.job);
      tx.update(ref,{placements,job:null,revision:Number(latest.revision||0)+1,updatedAt:Date.now()});
    });
    return status({workspaceId});
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
      const preparedCopyRequest=job.stages.copy||savedCopy.exists&&(savedCopy.data().rawResponse||savedCopy.data().stageResult)?null:deps.research.buildRequest({evidence:job.evidence,feedback:value.settings.direction,currentCreative:group.original||{},style:value.settings.style,sourceImageDataUrl:'data:image/jpeg;base64,'+sourceFiles[0].toString('base64'),sourceReferences:sourceFiles.map((bytes,index)=>({dataUrl:'data:image/jpeg;base64,'+bytes.toString('base64'),manifest:job.inputCoverage.referenceManifest&&job.inputCoverage.referenceManifest[index]}))});
      let copy = await paid("copy", async requestId => {
        const receipt = ref.collection("outputs").doc(jobId + "_copy"), stored = await receipt.get();
        const prior = stored.exists && stored.data().rawResponse;
        const request = prior ? null : preparedCopyRequest;
        const result = prior || await deps.responses(request, requestId);
        await writeReceipt("copy", { rawResponse: result, receivedAt: Date.now() });
        let output; try { output = deps.research.validateResult({ output: JSON.parse(responseText(result)), evidence: job.evidence, channel: group.channel, group }); }
        catch (error) { error.definiteResponse = true; throw error; }
        return { ...output, usage: result.usage || {}, responseId: result.id || null, providerModel: result.model || request && request.model || "gpt-6-astra", estimatedUsd: result.estimatedUsd == null ? 1 : result.estimatedUsd, costEstimated: result.costEstimated !== false };
      });
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
        await ref.collection('imageLibrary').doc(libraryId).set({id:libraryId,kind:'generated',groupRef:group.ref,format:format.key,title:product.title,productIds:selectedProductIds.length?selectedProductIds:[String(product.id)],asset:output.asset,createdAt:Date.now(),jobId});
        job.assets[format.key] = desktop?desktop.asset:output.asset;
        job.placementAssets.desktop[format.key]=desktop?desktop.asset:output.asset;
        job.placementAssets.mobile[format.key]=mobile?mobile.asset:output.asset;
        await saveJob({});
      }
      if (Date.now() - started > 620000) { await saveJob({ phase: "paused", leaseUntil: 0, progress: { pct: job.progress.pct, label: "Every format is saved; continuing the quality review" } }); return { ok: true, paused: true, dispatch: true, workspaceId, jobId }; }
      await progress(82, "Checking product fidelity, framing and mobile readability");
      const quality = await paid("quality", async requestId => {
        const finalAssets=[...new Map(Object.values(job.placementAssets).flatMap(assets=>Object.values(assets)).map(a=>[a.hash,a])).values()];
        const files = await Promise.all(finalAssets.map(asset => deps.loadAsset(asset))), output = await deps.reviewImages(sourceFiles[0], files, { ...copy.brief, copy: copy.copy, keywords: group.keywords || product.keywords || [], settings: value.settings, product: product.title, products: researchProducts, placementAssets:job.placementAssets, inputCoverage: job.inputCoverage }, sourceFiles, requestId);
        await writeReceipt("quality", { stageResult: output, receivedAt: Date.now() });
        return { ...output, estimatedUsd: output.estimatedUsd == null ? 1 : output.estimatedUsd, costEstimated: output.costEstimated !== false };
      });
      if (quality.pass !== true || quality.productFaithful !== true || quality.mobileReadable !== true || Number(quality.score) < 85) throw new Error("The generated design needs changes to product fidelity, framing or mobile clarity before it can enter Approvals.");
      const singleProductDestination=!value.context.campaignId&&!value.context.approvalId||/\/products\//.test(group.url||'');
      const outside = selectedProducts.filter(p => Array.isArray(p.eligibleGroupRefs) && !p.eligibleGroupRefs.includes(group.ref)||singleProductDestination&&String(p.id)!==String(product.id));
      const publication = {ready:!outside.length,productIds:selectedProductIds.length?selectedProductIds:[String(product.id)],reason:outside.length?"Design saved. These featured products are outside this ad's verified destination or product selection: "+outside.map(p=>p.title).join(", ")+". Open an ad that includes these products, or use photos from this ad's listings, before submitting it for publication.":null};
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
  return { workspace, save, upload, crop, start, status, run };
}
module.exports = { createAdDesignService, buildVersionDesignPayload, formatAssets, chosenPlacements, placementMatches, FORMATS, settingsFor, responseText, MAX_UPLOAD };
