'use strict';

// Pending-review edits never modify the source workspace or an already published ad.
function createReview(D) {
  async function load(input) {
    if (!/^design-review-[a-f0-9]{32}$/.test(String(input.id || ''))) throw Error('Choose a saved complete-ad approval.');
    const ref = D.approval(input.id), snap = await ref.get(), item = snap.data();
    if (!snap.exists || item.type !== 'adDesignSubmission' || item.status !== 'PENDING' || input.hash !== item.reviewHash) throw Error('This review changed. Reload Approvals.');
    const context = await D.context(item.designReview.workspaceId);
    if (D.sourceHash(context.w) !== item.sourceHash || D.hash(context.w.context) !== D.hash(item.designReview.context)) throw Error('The artwork changed. Send the current complete ad to Approvals again.');
    if (String(context.product.id) !== String(item.designReview.productId) || String(context.w.settings.groupRef) !== String(item.designReview.groupRef)) throw Error('This review belongs to a different product or ad group.');
    return {ref, item, context};
  }
  async function status(input) {
    const {item, context} = await load(input), r = item.designReview, {w, ref} = context;
    const placements = D.placements(w), images = [], warnings = [];
    async function image(asset, meta) {
      if (!asset) { warnings.push('The '+meta.label+' image is missing.'); return; }
      try { images.push({...meta, width:asset.width, height:asset.height, url:await D.sign(asset), hash:asset.hash}); }
      catch (_) { warnings.push('The '+meta.label+' preview could not be loaded.'); }
    }
    for (const format of ['landscape','square','portrait']) {
      const p = placements.find(p => p.device === 'desktop' && p.format === format);
      await image(p?.asset || w.job?.result?.placementAssets?.desktop?.[format] || w.job?.result?.assets?.[format], {kind:'responsive', format, label:format[0].toUpperCase()+format.slice(1)});
    }
    if (r.layoutReview) {
      const proof = await ref.collection('editorAIJobs').doc(r.layoutReview.jobId).collection('data').doc('ad_proofs_v'+r.layoutReview.reviewVersion).get();
      if (proof.exists) {
        let rows = [];
        try { rows = D.fixedProofs(proof.data().images); } catch (_) { warnings.push('No Google-supported display layouts are saved.'); }
        await Promise.all(rows.map(p => image(p.asset, {kind:'fixed', label:p.width+' × '+p.height, width:p.width, height:p.height})));
      } else warnings.push('The saved display layouts are missing.');
    }
    // After preparation these are the exact compressed files destined for Google.
    const prepared = [];
    for (const g of item.pipelinePlan?.payload?.generatedAssets || []) {
      try { prepared.push({kind:g.fixed?'fixed':'responsive',label:g.fixed?'Display artwork':g.asset.kind || 'Included image',width:g.asset.width,height:g.asset.height,url:await D.sign(g.asset),hash:g.asset.hash}); }
      catch (_) { warnings.push('An included image preview could not be loaded.'); }
    }
    let motion = {variants:[], phase:'idle'};
    try { motion = await D.motion({workspaceId:r.workspaceId,productId:r.productId,groupRef:r.groupRef}); }
    catch (_) { warnings.push('Video previews could not be loaded. Retry to see the saved videos.'); }
    return {ok:true,reviewHash:item.reviewHash,images,prepared,videos:motion.displayVariants || motion.variants || [],videoPhase:motion.phase,videoNote:item.pipelinePlan?.summary?.videoStatus || 'Saved videos are shown for preview. The prepared plan confirms which videos can be included.',copyEdited:r.copyEdited===true,warnings};
  }
  async function update(input) {
    const {ref, item, context} = await load(input), r = item.designReview;
    const copy = Object.fromEntries(['headlines','longHeadlines','descriptions'].map(k => [k,Array.isArray(input.copy?.[k])?input.copy[k].map(v=>String(v).trim()):[]]));
    if (!D.copyValid(copy)) throw Error('Use 3–15 unique headlines (30 characters), 1–5 long headlines (90), and 2–5 descriptions (90). Include one headline of 15 characters or fewer and one description of 60 or fewer. Unsupported claims cannot be used.');
    if (typeof input.includeVideos !== 'boolean') throw Error('Choose whether to use saved videos.');
    const designReview = {...r,copy,includeVideos:input.includeVideos,copyEdited:D.hash(copy)!==D.hash(context.w.messaging?.copy || context.w.job?.result?.copy || r.copy)};
    const reviewHash = D.hash({sourceHash:item.sourceHash,designReview});
    await D.transaction(async tx => {
      const live = await tx.get(ref), workspace = await tx.get(context.ref);
      if (live.data()?.status !== 'PENDING' || live.data()?.reviewHash !== input.hash || D.sourceHash(workspace.data()) !== item.sourceHash || D.hash(workspace.data()?.context) !== D.hash(r.context)) throw Error('This review changed while saving. Reload Approvals.');
      tx.update(ref,{designReview,reviewHash,pipelinePlan:null,pipelineReview:null,vetted:false,updatedAt:Date.now(),payload:{adDesign:{workspaceId:r.workspaceId,productId:r.productId,groupRef:r.groupRef},meta:{existingCampaignId:item.payload?.meta?.existingCampaignId || null}}});
    });
    return {ok:true,reviewHash,designReview};
  }
  return {status,update};
}
module.exports = {createReview};
