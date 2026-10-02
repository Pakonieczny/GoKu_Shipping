'use strict';

// Read membership from the actual Google mutations, rather than treating every saved
// file as an included asset. Shared uploads can belong to more than one lane.
function campaignMedia(ops) {
  const assets = new Map(ops.filter(o=>o.assetOperation?.create).map(o=>[o.assetOperation.create.resourceName,o.assetOperation.create]));
  const images=[], videos=[], copy={headlines:[],longHeadlines:[],descriptions:[]}; let callToAction=null;
  for(const o of ops) {
    const link=o.assetGroupAssetOperation?.create, asset=link&&assets.get(link.asset);
    if(link) {
      const key={HEADLINE:'headlines',LONG_HEADLINE:'longHeadlines',DESCRIPTION:'descriptions'}[link.fieldType];
      if(key&&asset?.textAsset)copy[key].push(asset.textAsset.text);
      else if(link.fieldType==='CALL_TO_ACTION_SELECTION')callToAction=asset?.callToActionAsset?.callToAction;
      else if(link.fieldType==='YOUTUBE_VIDEO')videos.push(link.asset);
      else if(['MARKETING_IMAGE','SQUARE_MARKETING_IMAGE','PORTRAIT_MARKETING_IMAGE','LOGO','LANDSCAPE_LOGO'].includes(link.fieldType))images.push({resource:link.asset,role:link.fieldType});
    }
    const ad=o.adGroupAdOperation?.create?.ad, r=ad?.responsiveDisplayAd;
    if(ad?.imageAd)images.push({resource:ad.imageAd.imageAsset.asset,role:'FINISHED_ARTWORK'});
    if(r) {
      for(const [field,role] of Object.entries({marketingImages:'MARKETING_IMAGE',squareMarketingImages:'SQUARE_MARKETING_IMAGE',logoImages:'LANDSCAPE_LOGO',squareLogoImages:'LOGO'}))
        for(const i of r[field]||[])images.push({resource:i.asset,role});
      for(const v of r.youtubeVideos||[])videos.push(v.asset);
      copy.headlines=r.headlines.map(t=>t.text);copy.longHeadlines=[r.longHeadline.text];copy.descriptions=r.descriptions.map(t=>t.text);callToAction=r.callToActionText;
    }
  }
  return {images,videos,copy,callToAction};
}
function selectVideos(job,copy,D) {
  if(job&&(D.ready?D.ready(job):job.phase==='ready'&&D.hash(job.plan?.nativeCopy)===D.hash(copy)&&D.qualityPass(job.quality))&&job.publication?.phase==='attached'&&job.publication.reviewHash===D.reviewHash(job))
    return {jobId:job.id,reviewHash:D.reviewHash(job),attached:(job.publication.videos||[]).filter(v=>v.state==='PROCESSED'&&/^[a-zA-Z0-9_-]{11}$/.test(v.videoId||'')).map(v=>({key:v.key,videoId:v.videoId}))};
  const note=D.filmNote(job,copy);
  return {jobId:job?.id||null,reviewHash:!note?D.reviewHash(job):null,attached:[],pending:!note,note};
}

// Pending-review edits never modify the source workspace or an already published ad.
function createReview(D) {
  async function load(input, allowSourceChange=false) {
    if (!/^design-review-[a-f0-9]{32}$/.test(String(input.id || ''))) throw Error('Choose a saved complete-ad approval.');
    const ref = D.approval(input.id), snap = await ref.get(), item = snap.data();
    if (!snap.exists || item.type !== 'adDesignSubmission' || item.status !== 'PENDING' || input.hash !== item.reviewHash) throw Error('This review changed. Reload Approvals.');
    const context = await D.context(item.designReview.workspaceId);
    if (String(context.product.id) !== String(item.designReview.productId) || String(context.w.settings.groupRef) !== String(item.designReview.groupRef)) throw Error('This review belongs to a different product or ad group.');
    const stale=D.sourceHash(context.w)!==item.sourceHash||D.hash(context.w.context)!==D.hash(item.designReview.context);
    if(stale&&!allowSourceChange)throw Error('The saved source changed. Reopen this approval to refresh its saved images and plan.');
    return {ref, item, context, stale};
  }
  // Preparation can renew a pending review, but validation and publication never
  // renew it. Keep the exact package assets and operator copy when already pinned.
  async function refresh(input, persist=true) {
    const {ref,item,context,stale}=await load(input,true);
    if(!stale)return {item,refreshed:false};
    const r=item.designReview;
    if(String(context.w.context?.campaignId||'')!==String(r.context?.campaignId||''))throw Error('The campaign destination changed. Submit a separate review for that campaign.');
    if(item.applyAttempt||item.needsReconciliation)throw Error('This approval has a publication in progress. Refresh its publication status.');
    const basis=await D.snapshot(context,r),sourceHash=D.sourceHash(context.w);
    if(!D.copyValid(r.copy))throw Error('Complete the saved ad messaging before approval.');
    const designReview={...r,...basis,context:context.w.context};
    const reviewHash=D.hash({sourceHash,designReview});
    const patch={sourceHash,designReview,reviewHash,pipelinePlan:null,pipelineReview:null,vetted:false,updatedAt:Date.now(),sourceRefresh:{at:Date.now(),previousSourceHash:item.sourceHash,previousReviewHash:item.reviewHash},payload:{adDesign:{workspaceId:r.workspaceId,productId:r.productId,groupRef:r.groupRef},meta:{existingCampaignId:context.w.context?.campaignId||null}}};
    if(persist)await D.transaction(async tx=>{
      const live=await tx.get(ref),workspace=await tx.get(context.ref),v=live.data();
      if(!live.exists||v.status!=='PENDING'||v.reviewHash!==input.hash||v.applyAttempt||v.needsReconciliation||D.sourceHash(workspace.data())!==sourceHash||D.hash(workspace.data()?.context)!==D.hash(context.w.context))throw Error('The saved ad changed while refreshing. Reopen this approval.');
      tx.update(ref,patch);
    });
    return {item:{...item,...patch},patch,refreshed:true};
  }
  async function status(input) {
    const loaded=await load(input,true),context=loaded.context;
    const item=loaded.stale?{...loaded.item,designReview:{...loaded.item.designReview,...await D.snapshot(context,loaded.item.designReview)},pipelinePlan:null}:loaded.item;
    const r=item.designReview,{w,ref}=context;
    const placements = D.placements(w), images = [], warnings = [];
    async function image(asset, meta) {
      if (!asset) { warnings.push('The '+meta.label+' image is missing.'); return; }
      try { images.push({...meta, width:asset.width, height:asset.height, url:await D.sign(asset), hash:asset.hash}); }
      catch (_) { warnings.push('The '+meta.label+' preview could not be loaded.'); }
    }
    for (const format of ['landscape','square','portrait']) {
      const p = r.publicationImages?.find(p=>p.format===format)||placements.find(p => p.device === 'desktop' && p.format === format);
      await image(p?.asset || w.job?.result?.placementAssets?.desktop?.[format] || w.job?.result?.assets?.[format], {kind:'responsive', format, label:format[0].toUpperCase()+format.slice(1)});
    }
    if (r.layoutReview) {
      const proof = await (r.layoutReview.workspaceId&&D.workspace?D.workspace(r.layoutReview.workspaceId):ref).collection('editorAIJobs').doc(r.layoutReview.jobId).collection('data').doc('ad_proofs_v'+r.layoutReview.reviewVersion).get();
      if (proof.exists) {
        let rows = [];
        try { rows = D.fixedProofs(proof.data().images); } catch (_) { warnings.push('No Google-supported display layouts are saved.'); }
        await Promise.all(rows.map(p => image(p.asset, {kind:'fixed', label:p.width+' × '+p.height, width:p.width, height:p.height})));
      } else warnings.push('The saved display layouts are missing.');
    }
    // After preparation these are the exact compressed files destined for Google.
    const prepared = [];
    for (const g of item.pipelinePlan?.payload?.generatedAssets || []) {
      try { prepared.push({kind:g.fixed?'fixed':'responsive',resource:g.tempResourceName,label:g.fixed?'Display artwork':g.asset.kind || 'Included image',width:g.asset.width,height:g.asset.height,url:await D.sign(g.asset),hash:g.asset.hash}); }
      catch (_) { warnings.push('An included image preview could not be loaded.'); }
    }
    let motion = {variants:[], phase:'idle'};
    try { motion = await D.motion({workspaceId:r.workspaceId,productId:r.productId,groupRef:r.groupRef}); }
    catch (_) { warnings.push('Video previews could not be loaded. Retry to see the saved videos.'); }
    let selection={attached:[],pending:false,note:'Prepare the publishing plan to confirm video eligibility.'};
    if(r.includeVideos!==false&&D.videoSelection)try{selection=await D.videoSelection(r,context);}catch(_){warnings.push('Video eligibility could not be confirmed. Prepare the plan before approval.');}
    const variants=(motion.variants||[]).filter(v=>!v.previous);
    const eligible=String(motion.jobId)===String(selection.jobId)?variants:[];
    const roleFormat={MARKETING_IMAGE:'landscape',SQUARE_MARKETING_IMAGE:'square',PORTRAIT_MARKETING_IMAGE:'portrait'};
    const byResource=new Map(prepared.map(p=>[p.resource,p]));
    const styles={};
    for(const key of ['pmax','responsive_display','fixed_display']) {
      const campaign=item.pipelinePlan?.summary?.campaigns?.find(c=>c.style===key), media=campaign?.media;
      const fixed=key==='fixed_display', formats=key==='pmax'?['landscape','square','portrait']:['landscape','square'];
      const included=media?media.images.map(i=>byResource.has(i.resource)?{...byResource.get(i.resource),role:i.role,format:roleFormat[i.role],label:roleFormat[i.role]?roleFormat[i.role][0].toUpperCase()+roleFormat[i.role].slice(1):byResource.get(i.resource).label}:null).filter(Boolean):images.filter(i=>fixed?i.kind==='fixed':i.kind==='responsive'&&formats.includes(i.format));
      let videos=[];
      if(!fixed&&r.includeVideos!==false) {
        const links=media?media.videos.map(resource=>item.pipelinePlan.payload.mutateOperations.find(o=>o.assetOperation?.create?.resourceName===resource)?.assetOperation.create.youtubeVideoAsset?.youtubeVideoId).filter(Boolean):selection.attached.map(v=>v.videoId);
        videos=links.map(videoId=>{const pub=selection.attached.find(v=>v.videoId===videoId),v=pub&&eligible.find(v=>v.key===pub.key);return {...(v||{}),videoId,youtubeUrl:'https://www.youtube.com/watch?v='+videoId,label:v?.key||'Included YouTube video',inclusion:'Included YouTube video'};});
        const approved=item.pipelinePlan?.payload?.meta?.motion;
        const pending=key==='pmax'&&(media?approved&&approved.jobId===selection.jobId&&approved.reviewHash===selection.reviewHash:selection.pending);
        if(!videos.length&&pending)videos=eligible.map(v=>({...v,inclusion:'Attaches after campaign creation'}));
      }
      // Visibility is independent of upload eligibility. Completed paid clips stay
      // playable during review, copy changes, exclusion and individual redo runs.
      const matches=(a,b)=>!a.previous&&(!a.key||!b.key||a.key===b.key)&&((a.asset?.hash&&a.asset.hash===b.asset?.hash)||(a.url&&a.url===b.url));
      const savedVideos=fixed?[]:(motion.displayVariants||motion.variants||[]).map(v=>{
        const included=videos.find(i=>matches(v,i));
        const status=included?included.inclusion:v.previous?'Previous version · preview only':r.includeVideos===false?'Excluded from publishing':motion.publicationReady===true?'Saved clip · not uploaded to this ad type':motion.qualityTargetMet===false||['needs_attention','failed'].includes(motion.phase)?'Saved clip · upload not ready':['running','queued'].includes(motion.phase)?'Saved clip · set still processing':'Saved clip · not included in this ad type';
        return {...v,previewCopy:motion.copy||r.copy,included:!!included,inclusion:status};
      });
      for(const v of videos)if(!savedVideos.some(s=>matches(s,v)))savedVideos.push({...v,included:true,previewCopy:motion.copy||r.copy});
      styles[key]={prepared:!!media,selected:!!campaign,images:included,videos,savedVideos,copy:fixed?null:media?.copy||{headlines:(r.copy?.headlines||[]).slice(0,key==='pmax'?15:5),longHeadlines:(r.copy?.longHeadlines||[]).slice(0,key==='pmax'?5:1),descriptions:(r.copy?.descriptions||[]).slice(0,5)},callToAction:fixed?null:media?.callToAction||'Shop now',inheritedBrand:key==='pmax'&&item.pipelinePlan?.payload?.meta?.brandGuidelinesEnabled===true,videoNote:fixed?'Fixed Display uses finished image artwork; videos are not included.':videos.length?videos[0].inclusion:r.includeVideos===false?'Saved videos are excluded.':key==='responsive_display'&&selection.pending?'These films have not been uploaded to YouTube. Responsive Display starts without them.':selection.note||'No eligible videos are included.'};
    }
    return {ok:true,sourceStale:loaded.stale,reviewHash:item.reviewHash,destination:context.product.url||r.destination,videoScope:{workspaceId:motion.workspaceId||r.workspaceId,productId:r.productId,groupRef:motion.jobGroupRef||r.groupRef},videoReview:{advisory:true,publicationReady:motion.publicationReady===true,phase:motion.phase,score:Number.isFinite(motion.quality?.score)?motion.quality.score:null,target:motion.qualityTarget||null,issues:(motion.quality?.issues||[]).map(v=>String(typeof v==='string'?v:v.message||v.reason||'').slice(0,500)).filter(Boolean),error:motion.error||null},styles,images,prepared,videos:motion.displayVariants || motion.variants || [],videoPhase:motion.phase,videoNote:item.pipelinePlan?.summary?.videoStatus || 'Saved videos are shown for preview. The prepared plan confirms which videos can be included.',copyEdited:r.copyEdited===true,warnings};
  }
  async function update(input) {
    const {ref, item, context} = await load(input), r = item.designReview;
    const copy = Object.fromEntries(['headlines','longHeadlines','descriptions'].map(k => [k,Array.isArray(input.copy?.[k])?input.copy[k].map(v=>String(v).trim()):[]]));
    if (!D.copyValid(copy)) throw Error('Use 3–15 unique headlines (30 characters), 1–5 long headlines (90), and 2–5 descriptions (90). Include one headline of 15 characters or fewer and one description of 60 or fewer. Unsupported claims cannot be used.');
    if (typeof input.includeVideos !== 'boolean') throw Error('Choose whether to use saved videos.');
    const designReview = {...r,copy,includeVideos:input.includeVideos,copyEdited:D.hash(copy)!==D.hash(r.artworkCopy || context.w.messaging?.copy || context.w.job?.result?.copy || r.copy)};
    const reviewHash = D.hash({sourceHash:item.sourceHash,designReview});
    await D.transaction(async tx => {
      const live = await tx.get(ref), workspace = await tx.get(context.ref);
      if (live.data()?.status !== 'PENDING' || live.data()?.reviewHash !== input.hash || D.sourceHash(workspace.data()) !== item.sourceHash || D.hash(workspace.data()?.context) !== D.hash(r.context)) throw Error('This review changed while saving. Reload Approvals.');
      tx.update(ref,{designReview,reviewHash,pipelinePlan:null,pipelineReview:null,vetted:false,updatedAt:Date.now(),payload:{adDesign:{workspaceId:r.workspaceId,productId:r.productId,groupRef:r.groupRef},meta:{existingCampaignId:item.payload?.meta?.existingCampaignId || null}}});
    });
    return {ok:true,reviewHash,designReview};
  }
  return {status,update,refresh};
}
module.exports = {createReview,campaignMedia,selectVideos};
